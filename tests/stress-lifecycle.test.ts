import { describe, it } from 'node:test';
import assert from 'node:assert';
import { Notifier } from '../src/services/notifier.service.js';
import { MockProvider } from '../src/providers/mock.provider.js';
import { NotificationPayload } from '../src/types/notification.js';

describe('Resource Lifecycle, Memory Stability & Stress Testing', () => {
  it('should process 1,000 notifications without unbounded memory growth', async () => {
    const mock = new MockProvider({ name: 'stress-push', channel: 'push' });
    const notifier = new Notifier({
      mode: 'memory',
      providers: [mock],
      rateLimit: { enabled: false },
      deduplication: { enabled: true, ttlSeconds: 300 },
      maxConcurrency: 20,
    });

    const initialMemory = process.memoryUsage().heapUsed;

    const payloads: NotificationPayload[] = Array.from({ length: 1000 }, (_, i) => ({
      id: `stress_item_${i}`,
      idempotencyKey: `idemp_stress_${i}`,
      recipient: { id: `user_stress_${i % 50}`, pushTokens: [`tok_${i}`] },
      channels: ['push'],
      title: `Stress test #${i}`,
    }));

    // Process all 1,000 items with bounded concurrency pool
    const results = await notifier.sendBulk(payloads, { concurrency: 25 });

    const finalMemory = process.memoryUsage().heapUsed;
    const memoryDiffMb = (finalMemory - initialMemory) / (1024 * 1024);

    assert.strictEqual(results.length, 1000);
    assert.strictEqual(mock.sentPayloads.length, 1000);
    // Heap growth should remain reasonably bounded (less than 25MB for 1000 payloads)
    assert.ok(memoryDiffMb < 25, `Expected heap growth < 25MB, measured ${memoryDiffMb.toFixed(2)}MB`);

    await notifier.close();
  });

  it('should be safe against repeated close() calls (idempotent shutdown)', async () => {
    const mock = new MockProvider({ name: 'mock-shutdown', channel: 'push' });
    const notifier = new Notifier({
      mode: 'memory',
      providers: [mock],
    });

    // Calling close multiple times must not throw or cause errors
    await notifier.close();
    await notifier.close();
    await notifier.close();

    // After shutdown, attempting to send should be rejected
    await assert.rejects(
      async () => {
        await notifier.sendSync({
          id: 'after_close',
          recipient: { id: 'u1' },
          channels: ['push'],
        });
      },
      { message: /closed/ }
    );
  });
});
