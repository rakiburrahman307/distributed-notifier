import { IQueueEngine, JobEnqueueOptions, NotificationJob, RetryPolicy } from '../../types/queue.js';
import { NotificationPayload } from '../../types/notification.js';
import { MinHeap } from '../../utils/min-heap.js';
import { calculateFullJitterBackoff } from '../../utils/backoff.js';

interface InternalJobItem {
  job: NotificationJob;
  priorityScore: number; // 1 (Highest) to 10 (Lowest)
  executeAt: number;     // Timestamp in ms
  retryPolicy: RetryPolicy;
}

export class InMemoryQueueEngine implements IQueueEngine {
  public readonly mode = 'memory' as const;

  // Binary Min-Heap: provides O(log N) scheduling operations
  private readonly heap = new MinHeap<InternalJobItem>();
  private isClosed = false;
  private handler?: (payload: NotificationPayload) => Promise<void>;
  private onJobFailed?: (job: NotificationJob, error: Error) => Promise<void>;
  private activeConcurrency = 0;
  private readonly maxConcurrency: number;
  private readonly maxQueueSize: number;
  private timer: NodeJS.Timeout | null = null;

  constructor(maxConcurrency = 10, maxQueueSize = 10000) {
    this.maxConcurrency = Math.max(1, maxConcurrency);
    this.maxQueueSize = Math.max(1, maxQueueSize);
  }

  public setFailureHandler(handler: (job: NotificationJob, error: Error) => Promise<void>): void {
    this.onJobFailed = handler;
  }

  public async enqueue(payload: NotificationPayload, options: JobEnqueueOptions = {}): Promise<string> {
    if (this.isClosed) {
      throw new Error('Queue engine is closed and cannot accept new jobs');
    }

    if (this.heap.size() >= this.maxQueueSize) {
      throw new Error(`In-memory queue capacity exceeded (limit: ${this.maxQueueSize})`);
    }

    const retryPolicy: RetryPolicy = options.retryPolicy || {
      maxAttempts: 3,
      backoffMs: 1000,
      backoffFactor: 2,
    };

    const jobId = payload.id || `job_${Date.now()}_${Math.random().toString(36).slice(2, 9)}`;
    const now = Date.now();
    const delayMs = Math.max(0, options.delayMs ?? 0);

    // Priority mapping: HIGH = 1, NORMAL = 5, LOW = 10
    const priorityScore = options.priority ?? (payload.priority === 'HIGH' ? 1 : payload.priority === 'LOW' ? 10 : 5);

    const jobItem: InternalJobItem = {
      job: {
        id: jobId,
        payload,
        attemptsMade: 0,
        maxAttempts: retryPolicy.maxAttempts,
        enqueuedAt: now,
      },
      priorityScore,
      executeAt: now + delayMs,
      retryPolicy,
    };

    // Priority for min-heap: primary key is executeAt, secondary key is priorityScore
    this.heap.insert(jobItem, this.getHeapKey(jobItem));
    this.scheduleNextTick();

    return jobId;
  }

  public process(handler: (payload: NotificationPayload) => Promise<void>): void {
    this.handler = handler;
    this.scheduleNextTick();
  }

  public async getPendingCount(): Promise<number> {
    return this.heap.size() + this.activeConcurrency;
  }

  public async close(): Promise<void> {
    this.isClosed = true;
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    this.heap.clear();
  }

  private getHeapKey(item: InternalJobItem): number {
    // Composite sort key: timestamp (coarse) + priorityScore (fine)
    return item.executeAt + (item.priorityScore * 0.001);
  }

  private scheduleNextTick(): void {
    if (this.isClosed || !this.handler) return;

    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }

    const nextItem = this.heap.peek();
    if (!nextItem) return;

    const now = Date.now();
    const delayMs = Math.max(0, nextItem.value.executeAt - now);

    if (delayMs === 0) {
      this.timer = setTimeout(() => this.processAvailableJobs(), 0);
    } else {
      this.timer = setTimeout(() => this.scheduleNextTick(), Math.min(delayMs, 30000));
    }

    if (this.timer?.unref) {
      this.timer.unref();
    }
  }

  private processAvailableJobs(): void {
    if (this.isClosed || !this.handler) return;

    const now = Date.now();

    while (this.activeConcurrency < this.maxConcurrency && !this.heap.isEmpty()) {
      const top = this.heap.peek();
      if (!top || top.value.executeAt > now) {
        break;
      }

      const item = this.heap.pop()!;
      this.activeConcurrency++;

      this.executeJob(item)
        .catch(() => {})
        .finally(() => {
          this.activeConcurrency--;
          this.scheduleNextTick();
        });
    }

    this.scheduleNextTick();
  }

  private async executeJob(item: InternalJobItem): Promise<void> {
    const { job } = item;
    job.attemptsMade++;

    try {
      if (this.handler) {
        await this.handler(job.payload);
      }
    } catch (err) {
      const error = err instanceof Error ? err : new Error(String(err));
      // Retry with AWS Standard Full Jitter Exponential Backoff
      if (job.attemptsMade < job.maxAttempts) {
        const jitteredBackoffMs = calculateFullJitterBackoff(
          job.attemptsMade,
          item.retryPolicy.backoffMs
        );
        item.executeAt = Date.now() + Math.max(200, jitteredBackoffMs);
        this.heap.insert(item, this.getHeapKey(item));
      } else {
        // Retries exhausted: forward to failure handler (DLQ)
        if (this.onJobFailed) {
          try {
            await this.onJobFailed(job, error);
          } catch {
            // Prevent unhandled rejection
          }
        }
      }
    }
  }
}
