import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
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

import { dispatchIndexFile } from '../../src/cli/index-file.js';
import { projectHash } from '../../src/global.js';
import { acquireLock, releaseLock } from '../../src/utils/pid-lock.js';

/**
 * #1480: `trace-mcp index-file` must not become a second, unserialized writer
 * of a project DB the daemon is writing — neither after a timeout on a live
 * daemon, nor while the daemon (or anyone) holds the reindex lock.
 */
describe('dispatchIndexFile (#1480)', () => {
  const FILE = '/proj/src/a.ts';
  const ROOT = '/proj';
  let lockDir: string;

  beforeEach(() => {
    lockDir = fs.mkdtempSync(path.join(os.tmpdir(), 'trace-1480-index-file-'));
  });

  afterEach(() => {
    fs.rmSync(lockDir, { recursive: true, force: true });
  });

  function deps(over: {
    daemonRunning?: boolean;
    post?: () => Promise<{ ok: boolean; status: number }>;
  }) {
    const indexLocally = vi.fn(async () => {
      // The reindex lock must be held for the whole local write.
      expect(fs.existsSync(path.join(lockDir, `${projectHash(ROOT)}-reindex.pid`))).toBe(true);
    });
    return {
      indexLocally,
      deps: {
        daemonRunning: async () => over.daemonRunning ?? true,
        postToDaemon: over.post ?? (async () => ({ ok: true, status: 202 })),
        indexLocally,
        lockRoot: ROOT,
        lockDir,
      },
    };
  }

  it('leaves the file to the daemon when it accepted the request', async () => {
    const d = deps({ post: async () => ({ ok: true, status: 202 }) });
    expect(await dispatchIndexFile(FILE, ROOT, d.deps)).toBe('daemon');
    expect(d.indexLocally).not.toHaveBeenCalled();
  });

  it('does not index locally when a live daemon timed out on the request', async () => {
    const d = deps({
      post: async () => {
        throw new DOMException('The operation was aborted due to timeout', 'TimeoutError');
      },
    });
    expect(await dispatchIndexFile(FILE, ROOT, d.deps)).toBe('daemon-timeout');
    expect(d.indexLocally).not.toHaveBeenCalled();
  });

  it.each([
    ['the daemon is down', { daemonRunning: false }],
    ['the daemon answered 503', { post: async () => ({ ok: false, status: 503 }) }],
    [
      'the connection failed',
      {
        post: async () => {
          throw new TypeError('fetch failed');
        },
      },
    ],
  ])('indexes locally under the reindex lock when %s', async (_label, over) => {
    const d = deps(over);
    expect(await dispatchIndexFile(FILE, ROOT, d.deps)).toBe('local');
    expect(d.indexLocally).toHaveBeenCalledTimes(1);
    // Released afterwards.
    expect(fs.existsSync(path.join(lockDir, `${projectHash(ROOT)}-reindex.pid`))).toBe(false);
  });

  it('skips the local write while another holder has the reindex lock', async () => {
    const held = acquireLock({
      lockDir,
      name: `${projectHash(ROOT)}-reindex`,
      op: 'reindex-file-http',
    });
    try {
      const d = deps({ post: async () => ({ ok: false, status: 503 }) });
      expect(await dispatchIndexFile(FILE, ROOT, d.deps)).toBe('lock-busy');
      expect(d.indexLocally).not.toHaveBeenCalled();
    } finally {
      releaseLock(held);
    }
  });
});
