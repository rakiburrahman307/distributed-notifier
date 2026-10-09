import { BaseProvider, BaseProviderOptions } from './base.provider.js';
import { NotificationPayload, ChannelDeliveryResult } from '../types/notification.js';

export interface SmsSenderFunction {
  (to: string, message: string, data?: Record<string, unknown>): Promise<{ success: boolean; messageId?: string; error?: string }>;
}

export interface SmsProviderOptions extends BaseProviderOptions {
  name?: string;
  sender?: SmsSenderFunction;
}

export class SmsProvider extends BaseProvider {
  public readonly name: string;
  public readonly channel = 'sms' as const;
  private readonly sender?: SmsSenderFunction;

  constructor(options: SmsProviderOptions = {}) {
    super(options);
    this.name = options.name || 'sms-gateway';
    this.sender = options.sender;
  }

  protected async deliver(payload: NotificationPayload): Promise<ChannelDeliveryResult> {
    const phone = payload.recipient.phone;
    if (!phone) {
      return {
        success: false,
        channel: 'sms',
        provider: this.name,
        error: `No phone number provided for recipient ${payload.recipient.id}`,
        timestamp: Date.now(),
      };
    }

    const message = payload.body || payload.title || '';

    if (this.sender) {
      const result = await this.sender(phone, message, payload.data);
      return {
        success: result.success,
        channel: 'sms',
        provider: this.name,
        messageId: result.messageId,
        error: result.error,
        timestamp: Date.now(),
      };
    }

    return {
      success: true,
      channel: 'sms',
      provider: this.name,
      messageId: `sms_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
      timestamp: Date.now(),
    };
  }
}
