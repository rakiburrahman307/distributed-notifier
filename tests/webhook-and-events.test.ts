import { test, describe } from 'node:test';
import assert from 'node:assert';
import { Notifier } from '../src/services/notifier.service.js';
import { WebhookProvider } from '../src/providers/webhook.provider.js';
import { MockProvider } from '../src/providers/mock.provider.js';
import { NotificationPayload } from '../src/types/notification.js';

describe('WebhookProvider Delivery', () => {
  test('should deliver payload using custom webhook sender function', async () => {
    let capturedUrl = '';
    let capturedPayload: NotificationPayload | null = null;
    let capturedHeaders: Record<string, string> | undefined;

    const webhookProvider = new WebhookProvider({
      name: 'slack-webhook',
      headers: { Authorization: 'Bearer test-token' },
      sender: async (url, payload, headers) => {
        capturedUrl = url;
        capturedPayload = payload;
        capturedHeaders = headers;
        return { success: true, messageId: 'msg_webhook_123' };
      },
    });

    const payload: NotificationPayload = {
      id: 'webhook_test_1',
      recipient: { id: 'team_channel', email: 'team@company.com' },
      channels: ['webhook'],
      title: 'Deployment Completed',
      body: 'Release v1.1.0 is live in production',
      metadata: { webhookUrl: 'https://hooks.slack.com/services/T00/B00/XXXX' },
    };

    const result = await webhookProvider.send(payload);

    assert.strictEqual(result.success, true);
    assert.strictEqual(result.provider, 'slack-webhook');
    assert.strictEqual(result.channel, 'webhook');
    assert.strictEqual(result.messageId, 'msg_webhook_123');
    assert.strictEqual(capturedUrl, 'https://hooks.slack.com/services/T00/B00/XXXX');
    assert.strictEqual(capturedPayload?.id, 'webhook_test_1');
    assert.strictEqual(capturedHeaders?.Authorization, 'Bearer test-token');

    await webhookProvider.close();
  });

  test('should return clear failure if no webhook URL is specified', async () => {
    const webhookProvider = new WebhookProvider({ name: 'unconfigured-webhook' });

    const payload: NotificationPayload = {
      id: 'webhook_no_url',
      recipient: { id: 'user_without_url' },
      channels: ['webhook'],
      title: 'Test',
      body: 'No URL test',
    };

    const result = await webhookProvider.send(payload);

    assert.strictEqual(result.success, false);
    assert.match(result.error || '', /No webhook URL provided/);

    await webhookProvider.close();
  });
});

describe('Notifier Lifecycle EventEmitter Hooks', () => {
  test('should emit "dispatched" and "suppressed" on topic opt-out', async () => {
    const notifier = new Notifier();
    const dispatchedEvents: any[] = [];
    const suppressedEvents: any[] = [];

    notifier.on('dispatched', (result, payload) => {
      dispatchedEvents.push({ result, payload });
    });

    notifier.on('suppressed', (event) => {
      suppressedEvents.push(event);
    });

    const payload: NotificationPayload = {
      id: 'event_optout_1',
      recipient: {
        id: 'opted_out_user',
        preferences: { topics: { promo: false } },
      },
      channels: ['push'],
      topic: 'promo',
      title: '50% off today',
      body: 'Check out deals',
    };

    const dispatchResult = await notifier.dispatch(payload);

    assert.strictEqual(dispatchResult.status, 'UNSUBSCRIBED');
    assert.strictEqual(dispatchedEvents.length, 1);
    assert.strictEqual(dispatchedEvents[0].result.status, 'UNSUBSCRIBED');
    assert.strictEqual(suppressedEvents.length, 1);
    assert.strictEqual(suppressedEvents[0].reason, 'UNSUBSCRIBED');

    await notifier.close();
  });

  test('should emit "delivered" event when notification succeeds', async () => {
    const notifier = new Notifier();
    const mockProvider = new MockProvider({ name: 'mock-events', channel: 'push', shouldSucceed: true });
    notifier.registerProvider(mockProvider);

    let deliveredEvent: any = null;
    notifier.on('delivered', (result, payload) => {
      deliveredEvent = { result, payload };
    });

    const payload: NotificationPayload = {
      id: 'event_delivered_1',
      recipient: { id: 'user_active' },
      channels: ['push'],
      title: 'Order Shipped',
      body: 'Your package is on its way',
    };

    const result = await notifier.sendSync(payload);

    assert.strictEqual(result.status, 'SENT');
    assert.ok(deliveredEvent, 'delivered event should have fired');
    assert.strictEqual(deliveredEvent.result.status, 'SENT');
    assert.strictEqual(deliveredEvent.payload.id, 'event_delivered_1');

    await notifier.close();
  });

  test('should emit "failed" and "dead_letter" events when all providers fail', async () => {
    const notifier = new Notifier();
    const failingProvider = new MockProvider({
      name: 'failing-provider',
      channel: 'push',
      shouldFail: true,
    });
    notifier.registerProvider(failingProvider);

    let failedEvent: any = null;
    let deadLetterEvent: any = null;

    notifier.on('failed', (result, payload) => {
      failedEvent = { result, payload };
    });

    notifier.on('dead_letter', (entry) => {
      deadLetterEvent = entry;
    });

    const payload: NotificationPayload = {
      id: 'event_failure_1',
      recipient: { id: 'user_fail' },
      channels: ['push'],
      title: 'Alert',
      body: 'System warning',
    };

    const result = await notifier.sendSync(payload);

    assert.strictEqual(result.status, 'FAILED');
    assert.ok(failedEvent, 'failed event should have fired');
    assert.strictEqual(failedEvent.result.status, 'FAILED');
    assert.ok(deadLetterEvent, 'dead_letter event should have fired');
    assert.strictEqual(deadLetterEvent.job.id, 'event_failure_1');

    await notifier.close();
  });
});
