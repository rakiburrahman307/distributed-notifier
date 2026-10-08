import { describe, it } from 'node:test';
import assert from 'node:assert';
import { ProviderTrafficShaper } from '../src/engines/traffic-shaper.js';

describe('Provider Traffic Shaper (Leaky Bucket)', () => {
  it('should space out rapid burst calls to obey maxRequestsPerSecond rate', async () => {
    // 10 req/s = 100ms interval between calls
    const shaper = new ProviderTrafficShaper({ maxRequestsPerSecond: 10 });
    const executedTimes: number[] = [];

    const p1 = shaper.schedule(async () => {
      executedTimes.push(Date.now());
      return 1;
    });

    const p2 = shaper.schedule(async () => {
      executedTimes.push(Date.now());
      return 2;
    });

    const [r1, r2] = await Promise.all([p1, p2]);
    assert.strictEqual(r1, 1);
    assert.strictEqual(r2, 2);
    assert.strictEqual(executedTimes.length, 2);

    // Verify time gap between burst requests
    const interval = executedTimes[1] - executedTimes[0];
    assert.ok(interval >= 50, `Expected interval >= 50ms, got ${interval}ms`);
  });
});
