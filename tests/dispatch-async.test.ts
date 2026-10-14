import { describe, it } from 'node:test';
import assert from 'node:assert';
import { Notifier } from '../src/services/notifier.service.js';
import { MockProvider } from '../src/providers/mock.provider.js';
import { NotificationPayload } from '../src/types/notification.js';

describe('Asynchronous Dispatch & Category Preferences', () => {
  it('should immediately return QUEUED status and stable jobId on dispatch()', async () => {
    const mock = new MockProvider({ name: 'push-fast', channel: 'push', delayMs: 50 });
    const notifier = new Notifier({
      mode: 'memory',
      providers: [mock],
      rateLimit: { enabled: false },
      deduplication: { enabled: false },
    });

    const payload: NotificationPayload = {
      id: 'async_notif_1',
      recipient: { id: 'user_async', pushTokens: ['tok_1'] },
      channels: ['push'],
      title: 'Async Job',
    };

    const start = Date.now();
    const result = await notifier.dispatch(payload);
    const elapsed = Date.now() - start;

    // Dispatch should return immediately without waiting for provider's 50ms delay
    assert.ok(elapsed < 40, `Dispatch took ${elapsed}ms, expected sub-40ms`);
    assert.strictEqual(result.status, 'QUEUED');
    assert.strictEqual(result.notificationId, 'async_notif_1');
    assert.ok(result.jobId);

    // Wait for the background worker to finish delivery
    await new Promise((resolve) => setTimeout(resolve, 80));
    assert.strictEqual(mock.sentPayloads.length, 1);
    assert.strictEqual(mock.sentPayloads[0].id, 'async_notif_1');

    await notifier.close();
  });

  it('should suppress notifications when recipient opted out of topic', async () => {
    const mock = new MockProvider({ name: 'marketing-push', channel: 'push' });
    const notifier = new Notifier({
      mode: 'memory',
      providers: [mock],
      rateLimit: { enabled: false },
      deduplication: { enabled: false },
    });

    const marketingPayload: NotificationPayload = {
      id: 'promo_99',
      topic: 'marketing',
      recipient: {
        id: 'user_optout',
        pushTokens: ['token_mkt'],
        preferences: {
          topics: {
            marketing: false, // Opted out of marketing
            security: true,
          },
        },
      },
      channels: ['push'],
      title: 'Flash Sale!',
    };

    const result = await notifier.sendSync(marketingPayload);
    assert.strictEqual(result.status, 'UNSUBSCRIBED');
    assert.strictEqual(mock.sentPayloads.length, 0);

    // But critical/HIGH priority or security alerts still go through!
    const securityPayload: NotificationPayload = {
      id: 'sec_100',
      topic: 'security',
      priority: 'HIGH',
      recipient: marketingPayload.recipient,
      channels: ['push'],
      title: 'New Login Detected',
    };

    const secResult = await notifier.sendSync(securityPayload);
    assert.strictEqual(secResult.status, 'SENT');
    assert.strictEqual(mock.sentPayloads.length, 1);

    await notifier.close();
  });

  it('should process sendBulk with bounded concurrency pool', async () => {
    const mock = new MockProvider({ name: 'bulk-mock', channel: 'push', delayMs: 10 });
    const notifier = new Notifier({
      mode: 'memory',
      providers: [mock],
      rateLimit: { enabled: false },
      deduplication: { enabled: false },
    });

    const payloads: NotificationPayload[] = Array.from({ length: 15 }, (_, i) => ({
      id: `bulk_item_${i}`,
      recipient: { id: `user_${i}`, pushTokens: [`tok_${i}`] },
      channels: ['push'],
      title: `Notice ${i}`,
    }));

    const results = await notifier.sendBulk(payloads, { concurrency: 5 });

    assert.strictEqual(results.length, 15);
    assert.ok(results.every((r) => r.status === 'SENT'));
    assert.strictEqual(mock.sentPayloads.length, 15);

    await notifier.close();
  });
});
