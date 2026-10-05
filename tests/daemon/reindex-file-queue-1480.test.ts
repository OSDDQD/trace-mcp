import os from 'node:os';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

vi.mock('../../src/logger.js', () => ({
  logger: {
    info: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
    debug: vi.fn(),
    fatal: vi.fn(),
    trace: vi.fn(),
  },
}));

import {
  acceptReindexFile,
  clearProjectStopping,
  isReindexing,
  markProjectStopping,
} from '../../src/daemon/reindex-file-handler.js';
import { __resetReindexStatsForTests, getReindexStats } from '../../src/daemon/reindex-stats.js';
import { __resetRecentReindexCache } from '../../src/indexer/recent-reindex-cache.js';
import { logger } from '../../src/logger.js';
import { LockError } from '../../src/utils/pid-lock.js';

/**
 * #1480: the hook path must get its answer before the incremental reindex
 * runs. Answering only after `indexFiles()` made every edit on a large
 * project outrun the hook's 2 s curl timeout on a healthy daemon.
 */

function deferred(): { promise: Promise<void>; resolve: () => void } {
  let resolve!: () => void;
  const promise = new Promise<void>((r) => {
    resolve = r;
  });
  return { promise, resolve };
}

const busy = (): LockError =>
  new LockError('Lock held', {
    pid: process.pid,
    hostname: os.hostname(),
    op: 'register_edit',
    started_at: Date.now(),
  });

async function drained(project: string): Promise<void> {
  await vi.waitFor(() => expect(isReindexing(project)).toBe(false), { timeout: 5_000 });
}

describe('acceptReindexFile (#1480)', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    __resetRecentReindexCache();
    __resetReindexStatsForTests();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('answers before indexing finishes and holds the in-flight mark until it does', async () => {
    const project = '/tmp/proj-1480-ack';
    const gate = deferred();
    const indexFiles = vi.fn(async (_paths: string[]) => {
      await gate.promise;
      return { indexed: 1, skipped: 0, errors: 0, durationMs: 5 };
    });
    const getProject = vi.fn((root: string) =>
      root === project ? { pipeline: { indexFiles }, status: 'ready' as const } : undefined,
    );
    const lock = vi.fn(async (_opts: unknown, fn: () => Promise<unknown>) => fn());

    // Synchronous return: the route can write 202 without awaiting the work.
    const result = acceptReindexFile(
      { project, path: 'src/a.ts' },
      // biome-ignore lint/suspicious/noExplicitAny: test fake lock signature
      { getProject, lock: lock as any },
    );
    expect(result).toEqual({ ok: true, relPath: 'src/a.ts', queued: true });
    // stopProject() drains on this mark; it must be up before the 202 goes out.
    expect(isReindexing(project)).toBe(true);

    await vi.waitFor(() => expect(indexFiles).toHaveBeenCalledWith(['src/a.ts']));
    const lockOpts = lock.mock.calls[0][0] as { name: string };
    expect(lockOpts.name).toMatch(/-reindex$/);
    expect(isReindexing(project)).toBe(true);

    gate.resolve();
    await drained(project);
    expect(getReindexStats().summarize().indexed).toBe(1);
  });

  it('batches files that arrive while a run is in flight into one follow-up run', async () => {
    const project = '/tmp/proj-1480-batch';
    const gate = deferred();
    const indexFiles = vi
      .fn(async (_paths: string[]) => undefined)
      .mockImplementationOnce(async () => {
        await gate.promise;
      });
    const getProject = vi.fn((root: string) =>
      root === project ? { pipeline: { indexFiles } } : undefined,
    );
    const lock = vi.fn(async (_opts: unknown, fn: () => Promise<unknown>) => fn());
    // biome-ignore lint/suspicious/noExplicitAny: test fake lock signature
    const deps = { getProject, lock: lock as any };

    acceptReindexFile({ project, path: 'src/a.ts' }, deps);
    await vi.waitFor(() => expect(indexFiles).toHaveBeenCalledTimes(1));
    acceptReindexFile({ project, path: 'src/b.ts' }, deps);
    acceptReindexFile({ project, path: 'src/c.ts' }, deps);
    gate.resolve();
    await drained(project);

    expect(indexFiles.mock.calls.map((c) => c[0])).toEqual([
      ['src/a.ts'],
      ['src/b.ts', 'src/c.ts'],
    ]);
  });

  it('retries a busy reindex lock instead of dropping the edit', async () => {
    const project = '/tmp/proj-1480-retry';
    const indexFiles = vi.fn(async (_paths: string[]) => undefined);
    const getProject = vi.fn((root: string) =>
      root === project ? { pipeline: { indexFiles } } : undefined,
    );
    let attempts = 0;
    const lock = vi.fn(async (_opts: unknown, fn: () => Promise<unknown>) => {
      attempts++;
      if (attempts <= 2) throw busy();
      return fn();
    });

    acceptReindexFile(
      { project, path: 'src/a.ts' },
      // biome-ignore lint/suspicious/noExplicitAny: test fake lock signature
      { getProject, lock: lock as any, queueRetry: { delayMs: 1, deadlineMs: 5_000 } },
    );
    await drained(project);

    expect(lock).toHaveBeenCalledTimes(3);
    expect(indexFiles).toHaveBeenCalledWith(['src/a.ts']);
    expect(logger.warn).not.toHaveBeenCalled();
    expect(getReindexStats().summarize().errors).toBe(0);
  });

  it('drops the batch with a warn and an error record once the retry deadline passes', async () => {
    const project = '/tmp/proj-1480-deadline';
    const indexFiles = vi.fn(async (_paths: string[]) => undefined);
    const getProject = vi.fn((root: string) =>
      root === project ? { pipeline: { indexFiles } } : undefined,
    );
    const lock = vi.fn(async () => {
      throw busy();
    });

    acceptReindexFile(
      { project, path: 'src/a.ts' },
      // biome-ignore lint/suspicious/noExplicitAny: test fake lock signature
      { getProject, lock: lock as any, queueRetry: { delayMs: 1, deadlineMs: 20 } },
    );
    await drained(project);

    expect(indexFiles).not.toHaveBeenCalled();
    expect(logger.warn).toHaveBeenCalledTimes(1);
    const [meta, msg] = (logger.warn as ReturnType<typeof vi.fn>).mock.calls[0] as [
      Record<string, unknown>,
      string,
    ];
    expect(msg).toMatch(/retry deadline/);
    expect(meta.lockBusy).toBe(true);
    expect(getReindexStats().summarize().errors).toBe(1);
  });

  it('does not run a queued batch against a project that started stopping', async () => {
    const project = '/tmp/proj-1480-stopping';
    const indexFiles = vi.fn(async (_paths: string[]) => undefined);
    const getProject = vi.fn((root: string) =>
      root === project ? { pipeline: { indexFiles } } : undefined,
    );
    const lock = vi.fn(async () => {
      // First attempt finds the lock busy; the stop lands during the backoff.
      markProjectStopping(project);
      throw busy();
    });

    try {
      acceptReindexFile(
        { project, path: 'src/a.ts' },
        // biome-ignore lint/suspicious/noExplicitAny: test fake lock signature
        { getProject, lock: lock as any, queueRetry: { delayMs: 1, deadlineMs: 5_000 } },
      );
      await drained(project);
    } finally {
      clearProjectStopping(project);
    }

    expect(lock).toHaveBeenCalledTimes(1);
    expect(indexFiles).not.toHaveBeenCalled();
  });
});
