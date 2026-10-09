import { BaseProvider, BaseProviderOptions } from './base.provider.js';
import { NotificationPayload, ChannelDeliveryResult } from '../types/notification.js';

export interface SocketEmitterFunction {
  (recipientId: string, event: string, data: Record<string, unknown>): Promise<void> | void;
}

export interface SocketProviderOptions extends BaseProviderOptions {
  name?: string;
  emitter?: SocketEmitterFunction;
  eventName?: string; // Default: 'notification'
}

export class SocketProvider extends BaseProvider {
  public readonly name: string;
  public readonly channel = 'in_app' as const;
  private readonly emitter?: SocketEmitterFunction;
  private readonly eventName: string;

  constructor(options: SocketProviderOptions = {}) {
    super(options);
    this.name = options.name || 'socket-in-app';
    this.emitter = options.emitter;
    this.eventName = options.eventName || 'notification';
  }

  protected async deliver(payload: NotificationPayload): Promise<ChannelDeliveryResult> {
    const recipientId = payload.recipient.id;

    if (this.emitter) {
      await this.emitter(recipientId, this.eventName, {
        id: payload.id,
        title: payload.title,
        body: payload.body,
        data: payload.data,
        priority: payload.priority,
        timestamp: Date.now(),
      });
    }

    return {
      success: true,
      channel: 'in_app',
      provider: this.name,
      messageId: `sock_${Date.now()}_${recipientId}`,
      timestamp: Date.now(),
    };
  }
}
