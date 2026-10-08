export interface TrafficShaperOptions {
  maxRequestsPerSecond: number;
  maxQueueSize?: number; // Backpressure limit (default: 1000)
}

interface QueuedTask<T> {
  action: () => Promise<T>;
  resolve: (value: T | PromiseLike<T>) => void;
  reject: (reason?: unknown) => void;
}

/**
 * Leaky Bucket Provider Traffic Shaper.
 * Smooths out bursty requests to external provider APIs into a steady egress stream,
 * preventing HTTP 429 (Too Many Requests) throttling.
 * Features backpressure protection, timer cleanup, and lifecycle control.
 */
export class ProviderTrafficShaper {
  private readonly minIntervalMs: number;
  private readonly maxQueueSize: number;
  private lastExecutedTimestamp = 0;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private readonly queue: QueuedTask<any>[] = [];
  private isProcessing = false;
  private timer: NodeJS.Timeout | null = null;
  private isDestroyed = false;

  constructor(options: TrafficShaperOptions) {
    const rps = Math.max(1, options.maxRequestsPerSecond);
    this.minIntervalMs = 1000 / rps;
    this.maxQueueSize = options.maxQueueSize ?? 1000;
  }

  public schedule<T>(action: () => Promise<T>): Promise<T> {
    if (this.isDestroyed) {
      return Promise.reject(new Error('ProviderTrafficShaper is disposed and cannot accept tasks'));
    }

    if (this.queue.length >= this.maxQueueSize) {
      return Promise.reject(
        new Error(`ProviderTrafficShaper queue capacity exceeded (limit: ${this.maxQueueSize})`)
      );
    }

    return new Promise<T>((resolve, reject) => {
      this.queue.push({ action, resolve, reject });
      this.processQueue();
    });
  }

  private processQueue(): void {
    if (this.isDestroyed || this.isProcessing || this.queue.length === 0) return;
    this.isProcessing = true;

    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }

    const now = Date.now();
    const elapsed = now - this.lastExecutedTimestamp;
    const waitTime = Math.max(0, this.minIntervalMs - elapsed);

    this.timer = setTimeout(async () => {
      this.timer = null;
      if (this.isDestroyed) {
        this.isProcessing = false;
        return;
      }

      const task = this.queue.shift();
      if (!task) {
        this.isProcessing = false;
        return;
      }

      this.lastExecutedTimestamp = Date.now();

      try {
        const result = await task.action();
        task.resolve(result);
      } catch (err) {
        task.reject(err);
      } finally {
        this.isProcessing = false;
        this.processQueue();
      }
    }, waitTime);

    if (this.timer?.unref) {
      this.timer.unref();
    }
  }

  public getQueueLength(): number {
    return this.queue.length;
  }

  public clear(reason = 'Traffic shaper queue cleared'): void {
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }

    while (this.queue.length > 0) {
      const task = this.queue.shift();
      if (task) {
        task.reject(new Error(reason));
      }
    }

    this.isProcessing = false;
  }

  public destroy(): void {
    this.isDestroyed = true;
    this.clear('ProviderTrafficShaper has been destroyed');
  }
}
