import { performance } from 'node:perf_hooks';
import fs from 'node:fs';
import path from 'node:path';
import { LOCKS_DIR, projectHash } from '../global.js';
import type { IndexingPipeline, IndexingResult } from '../indexer/pipeline.js';
import { beginReindex, isReindexing } from '../indexer/reindex-inflight.js';
import { shouldSkipRecentReindex } from '../indexer/recent-reindex-cache.js';
import { logger } from '../logger.js';
import { isHotChurnPath } from '../utils/hot-churn.js';
import { isSelfLock, LockError, withLock } from '../utils/pid-lock.js';
import { getReindexStats } from './reindex-stats.js';

// TRA-1763: the in-flight registry lives in `indexer/reindex-inflight.ts` so
// the pipeline can mark its own runs without an indexer→daemon import.
// Re-exported here so existing callers keep working unchanged.
export {
  beginReindex,
  countReindexingProjects,
  isReindexing,
} from '../indexer/reindex-inflight.js';

export interface ReindexFileRequest {
  project: string;
  path: string;
}

export type ReindexFileResult =
  | {
      ok: true;
      relPath: string;
      skippedRecent?: boolean;
      skippedChurn?: boolean;
      /** #1480: accepted into the per-project queue; indexing has not run yet. */
      queued?: boolean;
    }
  | { ok: false; status: 400 | 404 | 500; error: string }
  | { ok: false; status: 503; error: string; retryAfterSec: number };

/**
 * Projects with a single-file reindex in flight right now.
 *
 * (Registry moved to `indexer/reindex-inflight.ts` in TRA-1763 — see the
 * re-export above. The TRA-1125 history below still applies.)
 *
 * TRA-1125: `projects_indexing` in the vitals line only ever counted projects
 * in the initial-load path — `project-manager.ts` sets `status = 'indexing'`
 * there. This handler *requires* status `ready` to proceed and never changes
 * it, so by construction every incremental reindex was logged as idle. In the
 * measured window 264 of 264 vitals samples reported `projects_indexing: 0`
 * while the daemon burned 99.3% CPU on a reindex burst, which made every
 * "idle RSS" figure in docs/perf a silent mix of idle and busy.
 */
/** How long `stopProject()` waits for in-flight single-file reindexes before
 *  closing the project DB anyway (TRA-1553). Single-file runs are typically
 *  tens of ms, so this binds only pathological cases; the daemon-wide
 *  `DAEMON_SHUTDOWN_DEADLINE_MS` still caps the whole shutdown. */
export const REINDEX_DRAIN_TIMEOUT_MS = 5_000;

/** Normalize the in-flight/stopping key: the HTTP path keys by the raw client
 *  string while `stopProject()` keys by the registration string, and the two
 *  can differ in trailing slashes or relative segments for the same project. */
function keyOf(project: string): string {
  return path.resolve(project);
}

/**
 * Projects currently being torn down by `stopProject()` (TRA-1553). Set
 * synchronously before the first teardown await so no interleaving can start
 * new pipeline work against a closing DB; cleared when the project leaves the
 * map (and defensively on re-add, in case a stop threw midway). A stale mark
 * can only cause 503-with-retry, never data loss.
 */
const stopping = new Set<string>();

/** Mark a project as tearing down. Idempotent. */
export function markProjectStopping(project: string): void {
  stopping.add(keyOf(project));
}

/** Clear the teardown mark. Idempotent. */
export function clearProjectStopping(project: string): void {
  stopping.delete(keyOf(project));
}

/** Whether new reindex work must be refused for this project. */
export function isProjectStopping(project: string): boolean {
  return stopping.has(keyOf(project));
}

/**
 * Wait until no reindex is in flight for `project`, or `timeoutMs` elapses.
 * Returns true when drained, false on timeout (the caller must proceed to
 * close anyway — a hung reindex must not wedge shutdown past its deadline).
 */
export async function waitForReindexDrain(project: string, timeoutMs: number): Promise<boolean> {
  if (!isReindexing(project)) return true;
  const deadline = Date.now() + timeoutMs;
  while (isReindexing(project)) {
    if (Date.now() >= deadline) return false;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  return true;
}

export interface ReindexFileDeps {
  getProject: (root: string) =>
    | {
        pipeline: Pick<IndexingPipeline, 'indexFiles'>;
        /** Phase 5.1: when present and not 'ready', handler returns 503. */
        status?: 'starting' | 'indexing' | 'ready' | 'error';
      }
    | undefined;
  /** Override withLock for tests. */
  lock?: typeof withLock;
  /** Override the queued-reindex lock retry schedule for tests (#1480). */
  queueRetry?: { delayMs: number; deadlineMs: number };
}

type ManagedReindexTarget = NonNullable<ReturnType<ReindexFileDeps['getProject']>>;

/** Outcome of the synchronous, pre-lock half of a reindex-file request. */
type PreparedReindexFile =
  | { kind: 'done'; result: ReindexFileResult }
  | {
      kind: 'run';
      project: string;
      rel: string;
      managed: ManagedReindexTarget;
      startedAt: number;
    };

/**
 * Validate, resolve, and dispatch a single-file reindex against a managed
 * project, and answer only once the indexing has finished. Shares the
 * `<projectHash>-reindex` lock with `register_edit` so the HTTP path and the
 * MCP path serialize on the same SQLite writer.
 *
 * The HTTP route uses this only when the caller asks for the result
 * (`?wait=1`); the PostToolUse hook and `trace-mcp index-file` go through
 * {@link acceptReindexFile}, which answers before the work runs (#1480).
 */
export async function handleReindexFile(
  body: Partial<ReindexFileRequest> | undefined,
  deps: ReindexFileDeps,
): Promise<ReindexFileResult> {
  const prepared = prepareReindexFile(body, deps);
  if (prepared.kind === 'done') return prepared.result;
  return runReindexFile(prepared, deps);
}

/**
 * Validate a reindex-file request and queue the indexing instead of waiting
 * for it (#1480). Validation, the 404/503 answers and the churn/recent
 * fast paths are identical to {@link handleReindexFile}; a request that
 * passes them is added to the project's pending set and the call returns
 * `{ ok: true, queued: true }` synchronously.
 *
 * WHY: the response used to be written only after `indexFiles()` — including
 * project-wide edge resolution — had finished. On a ~27k-symbol PHP project
 * that is ~2.4 s per file, so nearly every edit outran the hook's 2 s curl
 * timeout on a healthy daemon, got recorded as `no-daemon`, and spawned a
 * cold `trace-mcp index-file` that indexed the same file again (sometimes
 * locally, as a second SQLite writer). Nothing on the hook path consumes the
 * result, so the wait bought nothing.
 *
 * The queue drains under the same `<projectHash>-reindex` lock, coalesces
 * paths that arrive while a run is in flight into the next batch, retries
 * lock contention instead of dropping the edit, and holds the in-flight mark
 * until it is empty so `stopProject()` still drains it before closing the DB.
 */
export function acceptReindexFile(
  body: Partial<ReindexFileRequest> | undefined,
  deps: ReindexFileDeps,
): ReindexFileResult {
  const prepared = prepareReindexFile(body, deps);
  if (prepared.kind === 'done') return prepared.result;
  enqueueReindex(prepared.project, prepared.rel, prepared.startedAt, deps);
  return { ok: true, relPath: prepared.rel, queued: true };
}

/**
 * The synchronous half shared by both entry points: request validation,
 * project lookup and readiness, path confinement, and the churn / recent
 * fast paths. Everything here runs before the reindex lock is touched.
 */
function prepareReindexFile(
  body: Partial<ReindexFileRequest> | undefined,
  deps: ReindexFileDeps,
): PreparedReindexFile {
  const startedAt = performance.now();
  const project = body?.project;
  const rawPath = body?.path;

  if (typeof project !== 'string' || project.length === 0) {
    return { kind: 'done', result: { ok: false, status: 400, error: 'project is required' } };
  }
  if (typeof rawPath !== 'string' || rawPath.length === 0) {
    return { kind: 'done', result: { ok: false, status: 400, error: 'path is required' } };
  }

  const managed = deps.getProject(project);
  if (!managed) {
    // TRA-2032: this 404 used to be silent — the daemon logged nothing and the
    // hook stats carry no root, so a whole class of "hook always falls back to
    // cold CLI" failures was undiagnosable. Log the missed root (at info, not
    // warn: a stuck config points every Edit here and warn would spam) so the
    // next QA correlation is one grep away.
    logger.info(
      { event: 'reindex-file', project, path: rawPath, pathSource: 'http' },
      'reindex-file: project not registered',
    );
    return {
      kind: 'done',
      result: { ok: false, status: 404, error: `project not registered: ${project}` },
    };
  }

  // Phase 5.1: if the project is still warming (cold daemon, indexAll in
  // progress), tell the client to fall back transiently. The hook then takes
  // the local CLI path until the daemon finishes warming.
  if (managed.status !== undefined && managed.status !== 'ready') {
    return {
      kind: 'done',
      result: {
        ok: false,
        status: 503,
        error: `project not ready: ${managed.status}`,
        retryAfterSec: 5,
      },
    };
  }

  // TRA-1553: the project is being torn down — its DB is about to close (or
  // already closing) while this handler would still start pipeline work
  // against it. Same 503 contract as a warming project: hook clients honour
  // Retry-After and fall back to the local CLI path transparently.
  if (isProjectStopping(project)) {
    return {
      kind: 'done',
      result: { ok: false, status: 503, error: 'project is stopping', retryAfterSec: 5 },
    };
  }

  const projectRoot = path.resolve(project);
  const absInput = path.isAbsolute(rawPath) ? rawPath : path.resolve(projectRoot, rawPath);
  const normalized = path.resolve(absInput);

  let relRaw = path.relative(projectRoot, normalized);
  if (relRaw.startsWith('..') || path.isAbsolute(relRaw)) {
    // TRA-2032: mixed symlink spellings — e.g. a pre-v0.6 hook posts an alias
    // file path while the route already normalized the project to the stored
    // spelling (or vice versa) — fail the lexical check for the same on-disk
    // file. Retry confinement on realpaths; the resulting rel is identical
    // under both spellings because symlinks resolve above the root.
    let realRoot: string;
    let realInput: string;
    try {
      realRoot = fs.realpathSync(projectRoot);
      realInput = fs.realpathSync(normalized);
    } catch {
      return {
        kind: 'done',
        result: { ok: false, status: 400, error: 'path is outside project root' },
      };
    }
    relRaw = path.relative(realRoot, realInput);
    if (relRaw.startsWith('..') || path.isAbsolute(relRaw)) {
      return {
        kind: 'done',
        result: { ok: false, status: 400, error: 'path is outside project root' },
      };
    }
  }
  const rel = path.sep === '\\' ? relRaw.split('\\').join('/') : relRaw;

  // TRA-2021: hot-churn runtime state (`gateway.heartbeat`, `cron/ticker_*`,
  // `cron/.tick.lock`) is rewritten every ~30 s with an unchanged content
  // hash — indexing it only ever yields `skippedHash=true, indexed=0` after
  // queueing behind the reindex lock (47 s worst case observed). Answer
  // before `withLock` so the hook path never contends the lock for it. The
  // HTTP layer still returns 204 — callers don't need to know the work was
  // dropped.
  if (isHotChurnPath(rel)) {
    const elapsedMs = Math.round(performance.now() - startedAt);
    logger.info(
      {
        event: 'reindex-file',
        project,
        path: rel,
        pathSource: 'http',
        skippedRecent: false,
        skippedHash: false,
        skippedChurn: true,
        indexed: 0,
        elapsedMs,
      },
      'reindex-file telemetry',
    );
    getReindexStats().record({
      pathSource: 'http',
      skippedRecent: false,
      skippedHash: false,
      skippedChurn: true,
      indexed: 0,
      elapsedMs,
    });
    return { kind: 'done', result: { ok: true, relPath: rel, skippedChurn: true } };
  }

  // Phase 1.3 dedup: when a single Edit causes both the PostToolUse hook
  // and Claude's register_edit MCP call to fire, the second arrival within
  // 500 ms is a no-op. The HTTP layer still returns 204 — callers don't need
  // to know the work was deduped.
  if (shouldSkipRecentReindex(project, rel)) {
    const elapsedMs = Math.round(performance.now() - startedAt);
    logger.info(
      {
        event: 'reindex-file',
        project,
        path: rel,
        pathSource: 'http',
        skippedRecent: true,
        skippedHash: false,
        indexed: 0,
        elapsedMs,
      },
      'reindex-file telemetry',
    );
    getReindexStats().record({
      pathSource: 'http',
      skippedRecent: true,
      skippedHash: false,
      indexed: 0,
      elapsedMs,
    });
    return { kind: 'done', result: { ok: true, relPath: rel, skippedRecent: true } };
  }

  return { kind: 'run', project, rel, managed, startedAt };
}

/** Run one prepared request under the reindex lock and report its telemetry. */
async function runReindexFile(
  prepared: Extract<PreparedReindexFile, { kind: 'run' }>,
  deps: ReindexFileDeps,
): Promise<ReindexFileResult> {
  const { project, rel, managed, startedAt } = prepared;
  const lock = deps.lock ?? withLock;

  const endReindex = beginReindex(project);
  try {
    const result = (await lock(
      { lockDir: LOCKS_DIR, name: `${projectHash(project)}-reindex`, op: 'reindex-file-http' },
      () => managed.pipeline.indexFiles([rel]),
    )) as IndexingResult | undefined;
    const indexed = result?.indexed ?? 0;
    const skipped = result?.skipped ?? 0;
    const skippedHash = indexed === 0 && skipped > 0;
    // TRA-935: report the indexing work and the wait for the reindex lock as
    // two numbers. Summed into one they made a 30 ms reindex that sat behind a
    // full project pass look like a 40-minute reindex.
    const totalMs = Math.round(performance.now() - startedAt);
    const elapsedMs = result?.durationMs ?? totalMs;
    const queuedMs = Math.max(0, totalMs - elapsedMs);
    logger.info(
      {
        event: 'reindex-file',
        project,
        path: rel,
        pathSource: 'http',
        skippedRecent: false,
        // Hash gate: the file was queued but indexFiles() returned a skipped
        // row instead of an indexed one — content hash matched the prior run.
        skippedHash,
        indexed,
        elapsedMs,
        queuedMs,
      },
      'reindex-file telemetry',
    );
    getReindexStats().record({
      pathSource: 'http',
      skippedRecent: false,
      skippedHash,
      indexed,
      elapsedMs,
      queuedMs,
    });
    return { ok: true, relPath: rel };
  } catch (err) {
    const elapsedMs = Math.round(performance.now() - startedAt);
    // TRA-2091: lock contention (including self-lock: this same daemon pid
    // already holding `<projectHash>-reindex` for an in-flight reindex) is
    // transient concurrency, not a crash. Answer 503 + Retry-After so hook
    // clients fall back to the local CLI path transparently (same contract
    // as warming/stopping), and log at warn with holder attribution instead
    // of L50.
    if (err instanceof LockError) {
      const holder = err.holder;
      const selfLock = isSelfLock(holder);
      logger.warn(
        {
          event: 'reindex-file',
          project,
          path: rel,
          pathSource: 'http',
          skippedRecent: false,
          skippedHash: false,
          indexed: 0,
          elapsedMs,
          lockBusy: true,
          selfLock,
          holder,
          holderOp: holder?.op,
          holderStartedAt: holder ? new Date(holder.started_at).toISOString() : undefined,
          holderStack: holder?.stack,
          err,
          error: String(err),
        },
        selfLock ? 'reindex-file lock busy (self-lock, retry)' : 'reindex-file lock busy (retry)',
      );
      getReindexStats().record({
        pathSource: 'http',
        skippedRecent: false,
        skippedHash: false,
        indexed: 0,
        elapsedMs,
        error: true,
      });
      return {
        ok: false,
        status: 503,
        error: `reindex_in_progress: ${err.message}`,
        retryAfterSec: 5,
      };
    }
    logger.error(
      {
        event: 'reindex-file',
        project,
        path: rel,
        pathSource: 'http',
        skippedRecent: false,
        skippedHash: false,
        indexed: 0,
        elapsedMs,
        err,
        error: String(err),
      },
      'reindex-file telemetry (error)',
    );
    getReindexStats().record({
      pathSource: 'http',
      skippedRecent: false,
      skippedHash: false,
      indexed: 0,
      elapsedMs,
      error: true,
    });
    return { ok: false, status: 500, error: String(err) };
  } finally {
    endReindex();
  }
}

/** How long a queued batch keeps retrying a busy reindex lock (or a project
 *  that is not `ready`) before it is dropped with an error record (#1480).
 *  The lock is held by `register_edit`, a `?wait=1` request or a foreign
 *  process; each of those finishes in seconds, so this only binds when the
 *  lock is wedged. */
export const QUEUED_REINDEX_RETRY_DEADLINE_MS = 30_000;
/** Pause between lock attempts for a queued batch. */
export const QUEUED_REINDEX_RETRY_DELAY_MS = 250;

interface PendingReindexQueue {
  /** Project spelling the requests used — the lock name and logs key off it. */
  project: string;
  /** Pending relative paths → when the first request for each was accepted. */
  paths: Map<string, number>;
  draining: boolean;
}

/** Per-project pending queues for {@link acceptReindexFile}. */
const pendingQueues = new Map<string, PendingReindexQueue>();

function enqueueReindex(
  project: string,
  rel: string,
  acceptedAt: number,
  deps: ReindexFileDeps,
): void {
  const key = keyOf(project);
  let queue = pendingQueues.get(key);
  if (!queue) {
    queue = { project, paths: new Map(), draining: false };
    pendingQueues.set(key, queue);
  }
  if (!queue.paths.has(rel)) queue.paths.set(rel, acceptedAt);
  if (queue.draining) return; // the running drain picks it up in its next batch
  queue.draining = true;
  void drainReindexQueue(queue, deps);
}

/**
 * Drain one project's queue batch by batch until it is empty. The in-flight
 * mark is taken synchronously (this function runs up to its first await
 * inside `enqueueReindex`), so a `stopProject()` that starts after the 202
 * was written still waits for the queued work before closing the DB.
 */
async function drainReindexQueue(queue: PendingReindexQueue, deps: ReindexFileDeps): Promise<void> {
  const endReindex = beginReindex(queue.project);
  try {
    while (queue.paths.size > 0) {
      const batch = [...queue.paths.entries()];
      queue.paths.clear();
      try {
        await runQueuedBatch(queue.project, batch, deps);
      } catch (err) {
        // runQueuedBatch reports its own failures; this only guards the loop.
        logger.error(
          { event: 'reindex-file', project: queue.project, err, error: String(err) },
          'reindex-file queue drain failed',
        );
      }
    }
  } finally {
    queue.draining = false;
    if (queue.paths.size === 0) pendingQueues.delete(keyOf(queue.project));
    endReindex();
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function runQueuedBatch(
  project: string,
  batch: Array<[string, number]>,
  deps: ReindexFileDeps,
): Promise<void> {
  const rels = batch.map(([rel]) => rel);
  const firstAcceptedAt = batch.reduce((min, [, at]) => (at < min ? at : min), Infinity);
  const lock = deps.lock ?? withLock;
  const delayMs = deps.queueRetry?.delayMs ?? QUEUED_REINDEX_RETRY_DELAY_MS;
  const deadline =
    performance.now() + (deps.queueRetry?.deadlineMs ?? QUEUED_REINDEX_RETRY_DEADLINE_MS);
  const base = {
    event: 'reindex-file',
    project,
    path: rels[0],
    ...(rels.length > 1 ? { paths: rels } : {}),
    batchSize: rels.length,
    pathSource: 'http',
    queued: true,
    skippedRecent: false,
  };
  const recordFailure = (): void => {
    getReindexStats().record({
      pathSource: 'http',
      skippedRecent: false,
      skippedHash: false,
      indexed: 0,
      elapsedMs: Math.round(performance.now() - firstAcceptedAt),
      error: true,
    });
  };

  for (;;) {
    // The project may have been stopped or unloaded since the 202 — its DB is
    // closing or gone, and a reload re-reads the file from disk anyway.
    const managed = isProjectStopping(project) ? undefined : deps.getProject(project);
    if (!managed) {
      logger.info(
        { ...base, skippedHash: false, indexed: 0 },
        'reindex-file queued batch dropped: project stopped or unloaded',
      );
      return;
    }
    let lockErr: LockError | undefined;
    if (managed.status === undefined || managed.status === 'ready') {
      try {
        const result = (await lock(
          { lockDir: LOCKS_DIR, name: `${projectHash(project)}-reindex`, op: 'reindex-file-http' },
          () => managed.pipeline.indexFiles(rels),
        )) as IndexingResult | undefined;
        const indexed = result?.indexed ?? 0;
        const skipped = result?.skipped ?? 0;
        const skippedHash = indexed === 0 && skipped > 0;
        // TRA-935: work and wait as two numbers. For a queued batch the wait
        // runs from the first accepted request, so it includes the time spent
        // behind the previous batch and any lock retries.
        const totalMs = Math.round(performance.now() - firstAcceptedAt);
        const elapsedMs = result?.durationMs ?? totalMs;
        const queuedMs = Math.max(0, totalMs - elapsedMs);
        logger.info(
          { ...base, skippedHash, indexed, elapsedMs, queuedMs },
          'reindex-file telemetry',
        );
        getReindexStats().record({
          pathSource: 'http',
          skippedRecent: false,
          skippedHash,
          indexed,
          elapsedMs,
          queuedMs,
        });
        return;
      } catch (err) {
        if (!(err instanceof LockError)) {
          logger.error(
            {
              ...base,
              skippedHash: false,
              indexed: 0,
              elapsedMs: Math.round(performance.now() - firstAcceptedAt),
              err,
              error: String(err),
            },
            'reindex-file telemetry (error)',
          );
          recordFailure();
          return;
        }
        lockErr = err;
      }
    }
    // Lock busy or project not ready: the sync path answers 503 and lets the
    // client retry, but nobody is waiting on a queued request — retry here
    // instead of silently losing the edit.
    if (performance.now() + delayMs > deadline) {
      const holder = lockErr?.holder ?? null;
      logger.warn(
        {
          ...base,
          skippedHash: false,
          indexed: 0,
          elapsedMs: Math.round(performance.now() - firstAcceptedAt),
          lockBusy: lockErr !== undefined,
          selfLock: isSelfLock(holder),
          holderOp: holder?.op,
          status: managed.status,
          error: lockErr ? String(lockErr) : `project not ready: ${managed.status}`,
        },
        'reindex-file queued batch dropped: retry deadline exceeded',
      );
      recordFailure();
      return;
    }
    await sleep(delayMs);
  }
}
