import { describe, it } from 'node:test';
import assert from 'node:assert';
import { PushProvider } from '../src/providers/push.provider.js';

describe('Vectorized Multicast Delivery', () => {
  it('should chunk 1,200 device tokens into batches of 500', async () => {
    const batchesReceived: number[] = [];

    const provider = new PushProvider({
      maxBatchSize: 500,
      sender: async (tokens) => {
        batchesReceived.push(tokens.length);
        return { success: true, messageId: `batch_${batchesReceived.length}` };
      },
    });

    const tokens = Array.from({ length: 1200 }, (_, i) => `device_token_${i}`);

    const result = await provider.sendMulticast(tokens, {
      id: 'broadcast_1',
      recipient: { id: 'broadcast' },
      channels: ['push'],
      title: 'Global Announcement',
      body: 'Server maintenance tonight',
    });

    assert.strictEqual(result.successCount, 1200);
    assert.strictEqual(result.failureCount, 0);
    // 1200 tokens chunked into 500, 500, 200 = 3 batches
    assert.deepStrictEqual(batchesReceived, [500, 500, 200]);
    assert.strictEqual(result.messageIds.length, 3);
  });
});
