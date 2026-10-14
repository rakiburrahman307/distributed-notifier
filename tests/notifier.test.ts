import { describe, it } from 'node:test';
import assert from 'node:assert';
import { Notifier } from '../src/services/notifier.service.js';
import { MockProvider } from '../src/providers/mock.provider.js';
import { NotificationPayload } from '../src/types/notification.js';

describe('Notifier Integration Tests', () => {
  it('should deliver notification through primary mock provider', async () => {
    const pushMock = new MockProvider({ name: 'fcm-mock', channel: 'push' });
    const notifier = new Notifier({
      mode: 'memory',
      providers: [pushMock],
      rateLimit: { enabled: false },
      deduplication: { enabled: false },
    });

    const payload: NotificationPayload = {
      id: 'notif_1',
      recipient: { id: 'user_1', pushTokens: ['token_abc'] },
      channels: ['push'],
      title: 'Order Ready',
      body: 'Your food is ready for pickup!',
    };

    const result = await notifier.send(payload);

    assert.strictEqual(result.status, 'SENT');
    assert.strictEqual(result.channelsAttempted.length, 1);
    assert.strictEqual(result.channelResults[0].success, true);
    assert.strictEqual(pushMock.sentPayloads.length, 1);
  });

  it('should automatically use fallback channel when primary provider fails', async () => {
    const failingPushMock = new MockProvider({
      name: 'fcm-failing',
      channel: 'push',
      shouldFail: true,
      failError: 'APNs connection timed out',
    });
    const fallbackSmsMock = new MockProvider({
      name: 'sms-backup',
      channel: 'sms',
      shouldFail: false,
    });

    const notifier = new Notifier({
      mode: 'memory',
      providers: [failingPushMock, fallbackSmsMock],
      rateLimit: { enabled: false },
      deduplication: { enabled: false },
    });

    const payload: NotificationPayload = {
      id: 'notif_2',
      recipient: { id: 'user_2', pushTokens: ['token_123'], phone: '+1234567890' },
      channels: ['push'],
      fallbackChannels: ['sms'],
      title: 'Urgent OTP',
      body: 'Your code is 987654',
    };

    const result = await notifier.send(payload);

    assert.strictEqual(result.isFallbackUsed, true);
    assert.strictEqual(result.status, 'PARTIALLY_DELIVERED');
    const smsResult = result.channelResults.find((r) => r.channel === 'sms');
    assert.strictEqual(smsResult?.success, true);
    assert.strictEqual(fallbackSmsMock.sentPayloads.length, 1);
  });

  it('should suppress duplicate notification with identical idempotencyKey', async () => {
    const pushMock = new MockProvider({ name: 'fcm-mock', channel: 'push' });
    const notifier = new Notifier({
      mode: 'memory',
      providers: [pushMock],
      rateLimit: { enabled: false },
      deduplication: { enabled: true, ttlSeconds: 300 },
    });

    const payload: NotificationPayload = {
      id: 'notif_3',
      idempotencyKey: 'idemp_order_assigned_101',
      recipient: { id: 'user_3', pushTokens: ['token_xyz'] },
      channels: ['push'],
      title: 'Order Assigned',
      body: 'Rider is on the way',
    };

    // First send
    const firstResult = await notifier.send(payload);
    assert.strictEqual(firstResult.status, 'SENT');

    // Duplicate send with same idempotency key
    const secondResult = await notifier.send({ ...payload, id: 'notif_3_dupe' });
    assert.strictEqual(secondResult.status, 'SKIPPED_DUPLICATE');
    assert.strictEqual(pushMock.sentPayloads.length, 1);
  });

  it('should filter blocked channels according to recipient preferences', async () => {
    const pushMock = new MockProvider({ name: 'fcm-mock', channel: 'push' });
    const smsMock = new MockProvider({ name: 'sms-mock', channel: 'sms' });

    const notifier = new Notifier({
      mode: 'memory',
      providers: [pushMock, smsMock],
      rateLimit: { enabled: false },
      deduplication: { enabled: false },
    });

    const payload: NotificationPayload = {
      id: 'notif_4',
      recipient: {
        id: 'user_4',
        pushTokens: ['token_123'],
        phone: '+1234567890',
        preferences: { blockedChannels: ['sms'] }, // User opted out of SMS
      },
      channels: ['push', 'sms'],
      title: 'Promo',
      body: 'Get 20% off',
    };

    const result = await notifier.send(payload);

    assert.strictEqual(result.status, 'SENT');
    assert.strictEqual(result.channelsAttempted.length, 1);
    assert.strictEqual(result.channelsAttempted[0], 'push');
    assert.strictEqual(smsMock.sentPayloads.length, 0);
  });
});
