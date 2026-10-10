import { IQueueEngine, JobEnqueueOptions, NotificationJob } from '../../types/queue.js';
import { NotificationPayload } from '../../types/notification.js';
import { RedisConnectionConfig } from '../../types/config.js';

export class BullMQQueueEngine implements IQueueEngine {
  public readonly mode = 'bullmq' as const;
  private readonly redisConfig: RedisConnectionConfig | string;
  private queueInstance?: unknown;
  private workerInstance?: unknown;
  private onJobFailed?: (job: NotificationJob, error: Error) => Promise<void>;

  constructor(redisConfig: RedisConnectionConfig | string) {
    this.redisConfig = redisConfig;
  }

  public setFailureHandler(handler: (job: NotificationJob, error: Error) => Promise<void>): void {
    this.onJobFailed = handler;
  }

  private async getBullMQ(): Promise<{ Queue: unknown; Worker: unknown }> {
    try {
      // Dynamic import to prevent crash when optional peer dependencies are not installed
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const bullmq = await (Function('return import("bullmq")')() as Promise<any>);
      return { Queue: bullmq.Queue, Worker: bullmq.Worker };
    } catch {
      throw new Error(
        'BullMQ is not installed. To use mode: "redis", please install optional dependencies:\n' +
        '  npm install bullmq ioredis\n' +
        'Or switch to mode: "memory" for zero-dependency execution.'
      );
    }
  }

  public async enqueue(payload: NotificationPayload, options: JobEnqueueOptions = {}): Promise<string> {
    const { Queue } = await this.getBullMQ();

    if (!this.queueInstance) {
      const redisOptions = typeof this.redisConfig === 'string'
        ? { connection: { url: this.redisConfig } }
        : { connection: this.redisConfig };
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      this.queueInstance = new (Queue as any)('notifications-queue', redisOptions);
    }

    const priority = options.priority ?? (payload.priority === 'HIGH' ? 1 : payload.priority === 'LOW' ? 10 : 5);
    const retryPolicy = options.retryPolicy || {
      maxAttempts: 3,
      backoffMs: 1000,
      backoffFactor: 2,
    };

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const job = await (this.queueInstance as any).add('notification-job', payload, {
      priority,
      delay: options.delayMs,
      attempts: retryPolicy.maxAttempts,
      backoff: {
        type: 'exponential',
        delay: retryPolicy.backoffMs,
      },
    });

    return job.id ? String(job.id) : payload.id;
  }

  private workerPromise?: Promise<void>;

  public process(handler: (payload: NotificationPayload) => Promise<void>): void {
    this.workerPromise = this.getBullMQ().then(({ Worker }) => {
      const redisOptions = typeof this.redisConfig === 'string'
        ? { connection: { url: this.redisConfig } }
        : { connection: this.redisConfig };

      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const worker = new (Worker as any)(
        'notifications-queue',
        async (job: { data: NotificationPayload }) => {
          await handler(job.data);
        },
        redisOptions
      );

      worker.on('failed', async (job: any, err: Error) => {
        if (job && job.attemptsMade >= (job.opts?.attempts || 1) && this.onJobFailed) {
          const notificationJob: NotificationJob = {
            id: String(job.id || job.data?.id),
            payload: job.data,
            attemptsMade: job.attemptsMade,
            maxAttempts: job.opts?.attempts || 1,
            enqueuedAt: job.timestamp || Date.now(),
          };
          try {
            await this.onJobFailed(notificationJob, err);
          } catch {
            // Safe guard
          }
        }
      });

      this.workerInstance = worker;
    }).catch((err) => {
      console.error('Failed to initialize BullMQ Worker:', err.message);
    });
  }

  public async getPendingCount(): Promise<number> {
    if (!this.queueInstance) return 0;
    try {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const count = await (this.queueInstance as any).getWaitingCount();
      return Number(count) || 0;
    } catch {
      return 0;
    }
  }

  public async close(): Promise<void> {
    if (this.workerPromise) {
      await this.workerPromise.catch(() => {});
    }
    if (this.workerInstance) {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const worker = this.workerInstance as any;
      this.workerInstance = undefined;
      try {
        await worker.close(true);
        if (typeof worker.disconnect === 'function') {
          await worker.disconnect();
        }
      } catch {}
    }
    if (this.queueInstance) {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const queue = this.queueInstance as any;
      this.queueInstance = undefined;
      try {
        await queue.close();
        if (typeof queue.disconnect === 'function') {
          await queue.disconnect();
        }
      } catch {}
    }
  }
}
