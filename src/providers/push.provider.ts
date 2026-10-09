import { BaseProvider, BaseProviderOptions } from './base.provider.js';
import { NotificationPayload, ChannelDeliveryResult } from '../types/notification.js';
import { MulticastResult } from '../types/provider.js';

export interface PushSenderFunction {
  (tokens: string[], notification: { title?: string; body?: string; data?: Record<string, unknown> }): Promise<{ success: boolean; messageId?: string; error?: string }>;
}

export interface PushProviderOptions extends BaseProviderOptions {
  name?: string;
  sender?: PushSenderFunction;
  maxBatchSize?: number; // Default 500 (standard FCM limit)
}

export class PushProvider extends BaseProvider {
  public readonly name: string;
  public readonly channel = 'push' as const;
  private readonly sender?: PushSenderFunction;
  private readonly maxBatchSize: number;

  constructor(options: PushProviderOptions = {}) {
    super(options);
    this.name = options.name || 'fcm-push';
    this.sender = options.sender;
    this.maxBatchSize = options.maxBatchSize || 500;
  }

  protected async deliver(payload: NotificationPayload): Promise<ChannelDeliveryResult> {
    const tokens = payload.recipient.pushTokens;
    if (!tokens || tokens.length === 0) {
      return {
        success: false,
        channel: 'push',
        provider: this.name,
        error: `No push tokens registered for recipient ${payload.recipient.id}`,
        timestamp: Date.now(),
      };
    }

    if (this.sender) {
      const result = await this.sender(tokens, {
        title: payload.title,
        body: payload.body,
        data: payload.data,
      });

      return {
        success: result.success,
        channel: 'push',
        provider: this.name,
        messageId: result.messageId,
        error: result.error,
        timestamp: Date.now(),
      };
    }

    // Default simulated push output for zero-setup development
    return {
      success: true,
      channel: 'push',
      provider: this.name,
      messageId: `push_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
      timestamp: Date.now(),
    };
  }

  /**
   * Vectorized multicast broadcast: splits tokens into batches of 500
   * and dispatches them efficiently.
   */
  public async sendMulticast(tokens: string[], payload: NotificationPayload): Promise<MulticastResult> {
    const messageIds: string[] = [];
    const failedTokens: string[] = [];
    let successCount = 0;
    let failureCount = 0;

    // Chunk into maxBatchSize (e.g. 500)
    for (let i = 0; i < tokens.length; i += this.maxBatchSize) {
      const chunk = tokens.slice(i, i + this.maxBatchSize);

      if (this.sender) {
        const res = await this.sender(chunk, {
          title: payload.title,
          body: payload.body,
          data: payload.data,
        });

        if (res.success) {
          successCount += chunk.length;
          if (res.messageId) messageIds.push(res.messageId);
        } else {
          failureCount += chunk.length;
          failedTokens.push(...chunk);
        }
      } else {
        successCount += chunk.length;
        messageIds.push(`batch_${Date.now()}_${i}`);
      }
    }

    return {
      successCount,
      failureCount,
      messageIds,
      failedTokens,
    };
  }
}
