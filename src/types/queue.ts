import { NotificationPayload } from './notification.js';

export interface RetryPolicy {
  maxAttempts: number;
  backoffMs: number;
  backoffFactor: number;
}

export interface JobEnqueueOptions {
  priority?: number; // 1 (Highest) to 10 (Lowest)
  delayMs?: number;
  retryPolicy?: RetryPolicy;
}

export interface NotificationJob {
  id: string;
  payload: NotificationPayload;
  attemptsMade: number;
  maxAttempts: number;
  enqueuedAt: number;
}

export interface IQueueEngine {
  readonly mode: 'memory' | 'bullmq';

  enqueue(payload: NotificationPayload, options?: JobEnqueueOptions): Promise<string>;

  process(handler: (payload: NotificationPayload) => Promise<void>): void;

  close(): Promise<void>;

  getPendingCount(): Promise<number>;
}
