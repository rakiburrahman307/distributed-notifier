import { BaseProvider, BaseProviderOptions } from './base.provider.js';
import { NotificationPayload, ChannelDeliveryResult } from '../types/notification.js';

export interface WebhookSenderFunction {
  (
    url: string,
    payload: NotificationPayload,
    headers?: Record<string, string>
  ): Promise<{ success: boolean; messageId?: string; error?: string }>;
}

export interface WebhookProviderOptions extends BaseProviderOptions {
  name?: string;
  defaultUrl?: string;
  headers?: Record<string, string>;
  timeoutMs?: number;
  sender?: WebhookSenderFunction;
}

export class WebhookProvider extends BaseProvider {
  public readonly name: string;
  public readonly channel = 'webhook' as const;
  public readonly defaultUrl?: string;
  public readonly headers?: Record<string, string>;
  public readonly timeoutMs: number;
  private readonly sender?: WebhookSenderFunction;

  constructor(options: WebhookProviderOptions = {}) {
    super(options);
    this.name = options.name || 'webhook-service';
    this.defaultUrl = options.defaultUrl;
    this.headers = options.headers;
    this.timeoutMs = options.timeoutMs ?? 10000;
    this.sender = options.sender;
  }

  protected async deliver(payload: NotificationPayload): Promise<ChannelDeliveryResult> {
    const targetUrl =
      (payload.metadata?.webhookUrl as string | undefined) ||
      (payload.data?.webhookUrl as string | undefined) ||
      this.defaultUrl;

    if (!targetUrl) {
      return {
        success: false,
        channel: 'webhook',
        provider: this.name,
        error: `No webhook URL provided for notification ${payload.id} (recipient: ${payload.recipient.id})`,
        timestamp: Date.now(),
      };
    }

    if (this.sender) {
      const result = await this.sender(targetUrl, payload, this.headers);
      return {
        success: result.success,
        channel: 'webhook',
        provider: this.name,
        messageId: result.messageId,
        error: result.error,
        timestamp: Date.now(),
      };
    }

    // Default Node 18+ global fetch implementation with timeout
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), this.timeoutMs);

    try {
      const body = JSON.stringify({
        id: payload.id,
        recipient: {
          id: payload.recipient.id,
          phone: payload.recipient.phone,
          email: payload.recipient.email,
          locale: payload.recipient.locale,
          timezone: payload.recipient.timezone,
        },
        title: payload.title,
        body: payload.body,
        data: payload.data,
        metadata: payload.metadata,
        topic: payload.topic || payload.category,
        priority: payload.priority || 'NORMAL',
        timestamp: Date.now(),
      });

      const response = await fetch(targetUrl, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'User-Agent': 'distributed-notifier/1.0.0',
          ...this.headers,
        },
        body,
        signal: controller.signal,
      });

      if (!response.ok) {
        return {
          success: false,
          channel: 'webhook',
          provider: this.name,
          error: `HTTP ${response.status} ${response.statusText}`,
          timestamp: Date.now(),
        };
      }

      return {
        success: true,
        channel: 'webhook',
        provider: this.name,
        messageId: `wh_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
        timestamp: Date.now(),
      };
    } catch (err) {
      const errorMsg =
        err instanceof Error && err.name === 'AbortError'
          ? `Webhook request timed out after ${this.timeoutMs}ms`
          : err instanceof Error
          ? err.message
          : String(err);

      return {
        success: false,
        channel: 'webhook',
        provider: this.name,
        error: errorMsg,
        timestamp: Date.now(),
      };
    } finally {
      clearTimeout(timer);
    }
  }
}
