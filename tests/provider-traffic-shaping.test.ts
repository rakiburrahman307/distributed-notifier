import { describe, it } from 'node:test';
import assert from 'node:assert';
import { MockProvider } from '../src/providers/mock.provider.js';
import { NotificationPayload } from '../src/types/notification.js';

describe('Provider Traffic Shaping Integration', () => {
  it('should pace rapid requests according to maxRequestsPerSecond', async () => {
    const provider = new MockProvider({
      name: 'rate-limited-mock',
      channel: 'push',
      trafficShaperOptions: { maxRequestsPerSecond: 10 }, // ~100ms interval
    });

    const payload: NotificationPayload = {
      id: 'p1',
      recipient: { id: 'u1', pushTokens: ['tok1'] },
      channels: ['push'],
      title: 'Hi',
    };

    const start = Date.now();
    const results = await Promise.all([
      provider.send(payload),
      provider.send({ ...payload, id: 'p2' }),
      provider.send({ ...payload, id: 'p3' }),
    ]);

    const elapsed = Date.now() - start;

    assert.strictEqual(results.length, 3);
    assert.ok(results.every((r) => r.success));
    // 3 calls at 10 rps should take at least ~180ms
    assert.ok(elapsed >= 180, `Expected elapsed >= 180ms, got ${elapsed}ms`);

    await provider.close();
  });

  it('should enforce backpressure when traffic shaper queue limit is exceeded', async () => {
    const provider = new MockProvider({
      name: 'tight-queue-mock',
      channel: 'sms',
      trafficShaperOptions: { maxRequestsPerSecond: 2, maxQueueSize: 2 },
    });

    const payload: NotificationPayload = {
      id: 'sms_p1',
      recipient: { id: 'u2', phone: '+1234567890' },
      channels: ['sms'],
      body: 'Code 123',
    };

    // Fire 5 rapid calls when maxQueueSize is 2
    const promises = [
      provider.send(payload),
      provider.send({ ...payload, id: 'sms_p2' }),
      provider.send({ ...payload, id: 'sms_p3' }),
      provider.send({ ...payload, id: 'sms_p4' }),
      provider.send({ ...payload, id: 'sms_p5' }),
    ];

    const results = await Promise.all(promises);

    // At least one or more of the late calls must be rejected due to queue overflow
    const overflowErrors = results.filter((r) => !r.success && r.error?.includes('queue capacity exceeded'));
    assert.ok(overflowErrors.length > 0, 'Expected at least one queue capacity overflow error');

    await provider.close();
  });
});
