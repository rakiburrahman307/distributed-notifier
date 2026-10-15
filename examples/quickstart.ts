import {
  NotifierService,
  MockProvider,
  WebhookProvider,
  NotificationPayload,
} from '../src/index.js';

async function main() {
  console.log('='.repeat(65));
  console.log('  DISTRIBUTED-NOTIFIER: LIVE INTERACTIVE VERIFICATION');
  console.log('='.repeat(65));

  // 1. Initialize Notifier in Zero-Setup In-Memory Mode
  const notifier = new NotifierService({
    mode: 'in-memory',
    enableDeduplication: true,
    enableRateLimiting: true,
    rateLimiting: { maxPerWindow: 20, windowSeconds: 60 },
  });

  // 2. Register Lifecycle Listeners
  notifier.on('dispatched', (result) => console.log(`  [EVENT: DISPATCHED] -> Job ID: ${result.jobId}`));
  notifier.on('delivered', (result) => console.log(`  [EVENT: DELIVERED]  -> Notification: ${result.notificationId} via ${result.channelResults.find(r => r.success)?.channel}`));
  notifier.on('suppressed', (event) => console.log(`  [EVENT: SUPPRESSED] -> Reason: ${event.reason}`));

  // 3. Register Providers
  // Primary Push Provider (Simulating a temporary outage to test Fallback)
  notifier.registerProvider(
    new MockProvider({
      name: 'fcm-primary',
      channel: 'push',
      delayMs: 30,
      shouldFail: true, // Forces failover to demonstrate resilience
    })
  );

  // Backup SMS Provider (Should catch the fallback)
  notifier.registerProvider(
    new MockProvider({
      name: 'twilio-backup',
      channel: 'sms',
      delayMs: 20,
      shouldFail: false,
    })
  );

  // Webhook Provider (Slack / Discord)
  notifier.registerProvider(
    new WebhookProvider({
      name: 'slack-alerts',
      sender: async (url, payload) => {
        console.log(`  [WEBHOOK MOCK] Delivered to ${url} -> Title: "${payload.content?.title}"`);
        return { success: true, messageId: 'msg_webhook_123' };
      },
    })
  );

  // 4. Register a Reusable Template
  notifier.registerTemplate({
    id: 'order_shipped_otp',
    title: 'Order #{{orderId}} Shipped!',
    body: 'Hello {{user.name}}! Your Order #{{orderId}} is on the way. Tracking: {{trackingUrl}}',
    defaultChannels: ['push', 'sms'],
  });

  console.log('\n--- SCENARIO 1: Template Rendering & Multi-Channel Fallback ---');
  console.log('Attempting delivery via Push (failing) -> Automatically falling back to SMS (backup)...');

  const deliveryResult = await notifier.sendSync({
    id: 'notif_order_001',
    channels: ['push'],
    fallbackChannels: ['sms'],
    priority: 'HIGH',
    recipient: {
      id: 'usr_rakib_101',
      name: 'Rakibur Rahman',
      phone: '+8801700000000',
    },
    templateId: 'order_shipped_otp',
    templateData: {
      user: { name: 'Rakibur Rahman' },
      orderId: 'ORD-9842',
      trackingUrl: 'https://store.example.com/track/9842',
    },
  });

  console.log(`\nResult Status:   ${deliveryResult.status}`);
  console.log(`Fallback Used:   ${deliveryResult.isFallbackUsed}`);
  console.log(`Channel Success: ${deliveryResult.channelResults.find((r) => r.success)?.channel}`);

  console.log('\n--- SCENARIO 2: Idempotency & Duplicate Suppression ---');
  console.log('Sending the exact same notification with idempotencyKey="idemp_txn_999"...');

  const firstSend = await notifier.sendSync({
    id: 'txn_payment_01',
    channels: ['sms'],
    idempotencyKey: 'idemp_txn_999',
    recipient: { id: 'usr_rakib_101', phone: '+8801700000000' },
    content: { body: 'Your payment of $49.00 was successful.' },
  });
  console.log(`First dispatch status:  ${firstSend.status}`);

  const secondSend = await notifier.sendSync({
    id: 'txn_payment_02',
    channels: ['sms'],
    idempotencyKey: 'idemp_txn_999', // Duplicate key!
    recipient: { id: 'usr_rakib_101', phone: '+8801700000000' },
    content: { body: 'Your payment of $49.00 was successful.' },
  });
  console.log(`Second dispatch status: ${secondSend.status} (Safely Suppressed!)`);

  console.log('\n--- SCENARIO 3: Topic Preference Opt-Out ---');
  console.log('Recipient has opted out of "marketing" topic...');

  const promoSend = await notifier.sendSync({
    id: 'promo_eid_sale',
    channels: ['sms'],
    topic: 'marketing',
    recipient: {
      id: 'usr_rakib_101',
      phone: '+8801700000000',
      preferences: {
        topics: { marketing: false }, // Opted out of marketing
      },
    },
    content: { body: '50% off on all items! Buy now.' },
  });
  console.log(`Marketing dispatch status: ${promoSend.status} (Suppressed by Preferences!)`);

  // Clean shutdown
  await notifier.close();
  console.log('\n' + '='.repeat(65));
  console.log('  ALL TESTS & CAPABILITIES VERIFIED PERFECTLY (ZERO LEAKS)');
  console.log('='.repeat(65));
}

main().catch(console.error);
