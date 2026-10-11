import { describe, it } from 'node:test';
import assert from 'node:assert';
import { InMemoryTokenBucketRateLimiter } from '../src/engines/rate-limiter.js';

describe('Token Bucket Rate Limiter', () => {
  it('should allow requests within capacity and block when exhausted', async () => {
    const limiter = new InMemoryTokenBucketRateLimiter(3, 60); // 3 tokens per 60s
    const userId = 'user_test_1';

    // 1st request -> allowed
    const res1 = await limiter.consume(userId);
    assert.strictEqual(res1.allowed, true);
    assert.strictEqual(res1.remainingTokens, 2);

    // 2nd request -> allowed
    const res2 = await limiter.consume(userId);
    assert.strictEqual(res2.allowed, true);
    assert.strictEqual(res2.remainingTokens, 1);

    // 3rd request -> allowed
    const res3 = await limiter.consume(userId);
    assert.strictEqual(res3.allowed, true);
    assert.strictEqual(res3.remainingTokens, 0);

    // 4th request -> blocked
    const res4 = await limiter.consume(userId);
    assert.strictEqual(res4.allowed, false);
    assert.ok(res4.retryAfterSeconds > 0);

    limiter.destroy();
  });
});
