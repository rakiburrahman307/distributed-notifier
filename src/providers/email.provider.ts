import { BaseProvider, BaseProviderOptions } from './base.provider.js';
import { NotificationPayload, ChannelDeliveryResult } from '../types/notification.js';

export interface EmailSenderFunction {
  (to: string, subject: string, body: string, data?: Record<string, unknown>): Promise<{ success: boolean; messageId?: string; error?: string }>;
}

export interface EmailProviderOptions extends BaseProviderOptions {
  name?: string;
  from?: string;
  sender?: EmailSenderFunction;
}

export class EmailProvider extends BaseProvider {
  public readonly name: string;
  public readonly channel = 'email' as const;
  public readonly from: string;
  private readonly sender?: EmailSenderFunction;

  constructor(options: EmailProviderOptions = {}) {
    super(options);
    this.name = options.name || 'email-service';
    this.from = options.from || 'notifications@example.com';
    this.sender = options.sender;
  }

  protected async deliver(payload: NotificationPayload): Promise<ChannelDeliveryResult> {
    const to = payload.recipient.email;
    if (!to) {
      return {
        success: false,
        channel: 'email',
        provider: this.name,
        error: `No email address provided for recipient ${payload.recipient.id}`,
        timestamp: Date.now(),
      };
    }

    if (this.sender) {
      const result = await this.sender(to, payload.title || 'Notification', payload.body || '', payload.data);
      return {
        success: result.success,
        channel: 'email',
        provider: this.name,
        messageId: result.messageId,
        error: result.error,
        timestamp: Date.now(),
      };
    }

    return {
      success: true,
      channel: 'email',
      provider: this.name,
      messageId: `email_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
      timestamp: Date.now(),
    };
  }
}
