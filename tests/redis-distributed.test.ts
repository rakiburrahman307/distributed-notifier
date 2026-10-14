import { describe, it } from 'node:test';
import assert from 'node:assert';
import { RedisDeduplicationEngine } from '../src/engines/deduplication.js';
import { RedisTokenBucketRateLimiter } from '../src/engines/rate-limiter.js';
import { RedisDeadLetterQueue } from '../src/engines/dead-letter-queue.js';
import { Notifier } from '../src/services/notifier.service.js';
import { MockProvider } from '../src/providers/mock.provider.js';
import { NotificationPayload } from '../src/types/notification.js';

describe('Real Redis Distributed Engines & Concurrency', () => {
  const REDIS_URL = 'redis://localhost:6379';
  const testPrefix = `test:${Date.now()}:`;

  it('should guarantee atomic deduplication across multiple independent instances', async () => {
    // Simulate Instance A and Instance B (two microservices)
    const engineA = new RedisDeduplicationEngine(REDIS_URL, {
      ttlSeconds: 60,
      keyPrefix: `${testPrefix}dedup:`,
    });
    const engineB = new RedisDeduplicationEngine(REDIS_URL, {
      ttlSeconds: 60,
      keyPrefix: `${testPrefix}dedup:`,
    });

    const idempotencyKey = `order_charge_txn_9999_${Date.now()}`;

    // Both instances attempt to register the exact same key at the exact same moment
    const [resultA, resultB] = await Promise.all([
      engineA.checkAndRecord(idempotencyKey),
      engineB.checkAndRecord(idempotencyKey),
    ]);

    // Exactly one instance must accept (false = not duplicate)
    // and the other must reject (true = is duplicate)
    const acceptedCount = (resultA === false ? 1 : 0) + (resultB === false ? 1 : 0);
    const rejectedCount = (resultA === true ? 1 : 0) + (resultB === true ? 1 : 0);

    assert.strictEqual(acceptedCount, 1, 'Exactly one instance should acquire the deduplication key');
    assert.strictEqual(rejectedCount, 1, 'The other instance should detect duplicate key atomically');

    // Clean up
    await engineA.clear();
    await engineA.close();
    await engineB.close();
  });

  it('should enforce atomic token bucket rate limiting across multiple instances', async () => {
    // 5 tokens per 10 seconds
    const limiterA = new RedisTokenBucketRateLimiter(REDIS_URL, {
      maxPerWindow: 5,
      windowSeconds: 10,
      keyPrefix: `${testPrefix}rate:`,
    });
    const limiterB = new RedisTokenBucketRateLimiter(REDIS_URL, {
      maxPerWindow: 5,
      windowSeconds: 10,
      keyPrefix: `${testPrefix}rate:`,
    });

    const userKey = `user_vip_shared_${Date.now()}`;

    // Rapidly consume 7 tokens alternating across both instances
    const results = await Promise.all([
      limiterA.consume(userKey),
      limiterB.consume(userKey),
      limiterA.consume(userKey),
      limiterB.consume(userKey),
      limiterA.consume(userKey),
      limiterB.consume(userKey), // 6th request: should be blocked
      limiterA.consume(userKey), // 7th request: should be blocked
    ]);

    const allowedRequests = results.filter((r) => r.allowed);
    const blockedRequests = results.filter((r) => !r.allowed);

    assert.strictEqual(allowedRequests.length, 5, 'Exactly 5 requests should be allowed');
    assert.strictEqual(blockedRequests.length, 2, '2 requests should be rate limited');
    assert.ok(blockedRequests[0].retryAfterSeconds > 0);

    await limiterA.reset(userKey);
    await limiterA.close();
    await limiterB.close();
  });

  it('should durably store and trim failed notifications in RedisDeadLetterQueue', async () => {
    const dlq = new RedisDeadLetterQueue(REDIS_URL, {
      maxStored: 3, // Capacity limit of 3
      keyPrefix: `${testPrefix}dlq:`,
    });

    // Push 4 failed jobs
    for (let i = 1; i <= 4; i++) {
      await dlq.push(
        {
          id: `failed_job_${i}`,
          payload: {
            id: `payload_${i}`,
            recipient: { id: `u_${i}` },
            channels: ['push'],
            title: `Job ${i}`,
          },
          attemptsMade: 3,
          maxAttempts: 3,
          enqueuedAt: Date.now() - (5 - i) * 1000,
        },
        new Error(`Provider timeout on attempt ${i}`)
      );
    }

    // Since maxStored is 3, count must be clamped to 3 (oldest job 1 evicted)
    const count = await dlq.count();
    assert.strictEqual(count, 3);

    const all = await dlq.getAll();
    assert.strictEqual(all.length, 3);
    // Newest job 4 should be first in zrevrange
    assert.strictEqual(all[0].job.id, 'failed_job_4');

    const job2 = await dlq.get('failed_job_2');
    assert.ok(job2);
    assert.strictEqual(job2.error.message, 'Provider timeout on attempt 2');

    // Oldest job 1 should have been evicted
    const job1 = await dlq.get('failed_job_1');
    assert.strictEqual(job1, undefined);

    await dlq.clear();
    await dlq.close();
  });

  it('should dispatch and process a background notification via BullMQ + Redis', async () => {
    const mock = new MockProvider({ name: 'fcm-redis', channel: 'push' });
    const notifier = new Notifier({
      mode: 'redis',
      redis: REDIS_URL,
      providers: [mock],
      rateLimit: { enabled: false },
      deduplication: { enabled: false },
    });

    const payload: NotificationPayload = {
      id: `bullmq_notif_${Date.now()}`,
      recipient: { id: 'u_bullmq', pushTokens: ['tok_redis'] },
      channels: ['push'],
      title: 'Distributed BullMQ Test',
      body: 'Verified end-to-end with Redis cluster queue',
    };

    const dispatchRes = await notifier.dispatch(payload);
    assert.strictEqual(dispatchRes.status, 'QUEUED');
    assert.ok(dispatchRes.jobId);

    // Wait for BullMQ worker to process job
    let delivered = false;
    for (let i = 0; i < 30; i++) {
      await new Promise((r) => setTimeout(r, 100));
      if (mock.sentPayloads.some((p) => p.id === payload.id)) {
        delivered = true;
        break;
      }
    }

    assert.ok(delivered, 'BullMQ worker should process and deliver the background job');

    await notifier.close();
  });
});
