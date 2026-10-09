import { BaseProvider, BaseProviderOptions } from './base.provider.js';
import { NotificationChannel, NotificationPayload, ChannelDeliveryResult } from '../types/notification.js';

export interface MockProviderOptions extends BaseProviderOptions {
  name?: string;
  channel?: NotificationChannel;
  shouldFail?: boolean;
  failError?: string;
  delayMs?: number;
}

export class MockProvider extends BaseProvider {
  public readonly name: string;
  public readonly channel: NotificationChannel;
  public shouldFail: boolean;
  public failError: string;
  public delayMs: number;
  public readonly sentPayloads: NotificationPayload[] = [];

  constructor(options: MockProviderOptions = {}) {
    super(options);
    this.name = options.name || 'mock-provider';
    this.channel = options.channel || 'push';
    this.shouldFail = options.shouldFail ?? false;
    this.failError = options.failError || 'Mock provider forced failure';
    this.delayMs = options.delayMs ?? 0;
  }

  protected async deliver(payload: NotificationPayload): Promise<ChannelDeliveryResult> {
    if (this.delayMs > 0) {
      await new Promise((resolve) => setTimeout(resolve, this.delayMs));
    }

    this.sentPayloads.push(payload);

    if (this.shouldFail) {
      return {
        success: false,
        channel: this.channel,
        provider: this.name,
        error: this.failError,
        timestamp: Date.now(),
      };
    }

    return {
      success: true,
      channel: this.channel,
      provider: this.name,
      messageId: `mock_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`,
      timestamp: Date.now(),
    };
  }

  public clear(): void {
    this.sentPayloads.length = 0;
  }
}
