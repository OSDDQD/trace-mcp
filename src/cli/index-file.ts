/**
 * `trace-mcp index-file <file>` dispatch: hand the file to a running daemon,
 * and index it in-process only when no daemon will.
 *
 * #1480: the local fallback used to open the project DB and index straight
 * away whenever the daemon request failed — including when it only timed out
 * on a live daemon that was still going to process it, and without the
 * `<projectHash>-reindex` lock the daemon's own reindex-file handler and
 * `register_edit` take. That made it a second, unserialized SQLite writer on
 * exactly the projects whose reindex is slow enough to time out.
 */
import { LOCKS_DIR, projectHash } from '../global.js';
import { logger } from '../logger.js';
import { LockError, withLock } from '../utils/pid-lock.js';

export type IndexFileOutcome =
  /** The daemon accepted the file (2xx). */
  | 'daemon'
  /** The daemon took the request but did not answer in time; it still runs it. */
  | 'daemon-timeout'
  /** Indexed in-process under the reindex lock. */
  | 'local'
  /** Another process holds the reindex lock; nothing was written here. */
  | 'lock-busy';

export interface IndexFileDeps {
  /** Whether a daemon answers on its port right now. */
  daemonRunning: () => Promise<boolean>;
  /** POST the file to the daemon's reindex-file endpoint. May reject. */
  postToDaemon: () => Promise<{ ok: boolean; status: number }>;
  /** Index the file in-process. Only ever called while holding the reindex lock. */
  indexLocally: () => Promise<void>;
  /** Project root spelling the daemon registered — the lock name keys off it. */
  lockRoot: string;
  /** Override for tests. */
  lockDir?: string;
  /** Override for tests. */
  lock?: typeof withLock;
}

function isTimeout(err: unknown): boolean {
  const name = (err as { name?: unknown } | null)?.name;
  return name === 'TimeoutError' || name === 'AbortError';
}

export async function dispatchIndexFile(
  file: string,
  projectRoot: string,
  deps: IndexFileDeps,
): Promise<IndexFileOutcome> {
  // Daemon-first path: avoids a cold Node + WASM + plugin spawn (~300-500 ms)
  // when the long-running daemon already has everything warm. See plan-indexer-perf §1.1.
  if (await deps.daemonRunning().catch(() => false)) {
    try {
      const res = await deps.postToDaemon();
      if (res.ok) {
        logger.debug({ file, projectRoot, status: res.status }, 'index-file proxied to daemon');
        return 'daemon';
      }
      logger.warn(
        { file, projectRoot, status: res.status },
        'Daemon reindex-file rejected request — falling back to local indexing',
      );
    } catch (e) {
      // A live daemon that took the request but did not answer in time (its
      // event loop busy in a synchronous pass) still processes it. Indexing
      // the same file here as well would only add a second writer.
      if (isTimeout(e)) {
        logger.info(
          { file, projectRoot },
          'Daemon reindex-file did not answer in time — leaving the file to the daemon',
        );
        return 'daemon-timeout';
      }
      logger.warn(
        { file, projectRoot, err: (e as Error).message },
        'Daemon reindex-file failed — falling back to local indexing',
      );
    }
  }

  const lock = deps.lock ?? withLock;
  try {
    await lock(
      {
        lockDir: deps.lockDir ?? LOCKS_DIR,
        name: `${projectHash(deps.lockRoot)}-reindex`,
        op: 'index-file-cli',
      },
      deps.indexLocally,
    );
    return 'local';
  } catch (e) {
    if (e instanceof LockError) {
      logger.info(
        { file, projectRoot, holderOp: e.holder?.op, holderPid: e.holder?.pid },
        'index-file: reindex lock busy — skipping local indexing',
      );
      return 'lock-busy';
    }
    throw e;
  }
}
