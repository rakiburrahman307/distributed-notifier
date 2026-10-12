import { NotificationPayload } from '../types/notification.js';

export interface DigestItem {
  payload: NotificationPayload;
  addedAt: number;
}

export type DigestFlushCallback = (recipientId: string, payloads: NotificationPayload[]) => Promise<void>;

export class DigestEngine {
  private readonly windowMs: number;
  private readonly enabled: boolean;
  private readonly buffers = new Map<string, DigestItem[]>(); // recipientId -> buffer
  private readonly timers = new Map<string, NodeJS.Timeout>();
  private flushCallback?: DigestFlushCallback;

  constructor(enabled = false, windowSeconds = 120) {
    this.enabled = enabled;
    this.windowMs = windowSeconds * 1000;
  }

  public setFlushCallback(callback: DigestFlushCallback): void {
    this.flushCallback = callback;
  }

  public shouldDigest(payload: NotificationPayload): boolean {
    if (!this.enabled) return false;
    // Don't digest urgent notifications or explicit individual alerts
    if (payload.priority === 'HIGH') return false;
    return Boolean(payload.metadata?.digestible);
  }

  public add(payload: NotificationPayload): void {
    const recipientId = payload.recipient.id;
    const existing = this.buffers.get(recipientId) || [];
    existing.push({ payload, addedAt: Date.now() });
    this.buffers.set(recipientId, existing);

    // Schedule flush if not already scheduled
    if (!this.timers.has(recipientId)) {
      const timer = setTimeout(() => {
        this.flush(recipientId).catch((err) => {
          console.error(`Error flushing digest for recipient ${recipientId}:`, err);
        });
      }, this.windowMs);

      if (timer.unref) {
        timer.unref();
      }
      this.timers.set(recipientId, timer);
    }
  }

  public async flush(recipientId: string): Promise<void> {
    const timer = this.timers.get(recipientId);
    if (timer) {
      clearTimeout(timer);
      this.timers.delete(recipientId);
    }

    const items = this.buffers.get(recipientId);
    if (!items || items.length === 0) return;

    this.buffers.delete(recipientId);
    const payloads = items.map((i) => i.payload);

    if (this.flushCallback) {
      await this.flushCallback(recipientId, payloads);
    }
  }

  public async flushAll(): Promise<void> {
    const recipientIds = Array.from(this.buffers.keys());
    for (const id of recipientIds) {
      await this.flush(id);
    }
  }

  public destroy(): void {
    for (const timer of this.timers.values()) {
      clearTimeout(timer);
    }
    this.timers.clear();
    this.buffers.clear();
  }
}
