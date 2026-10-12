import { NotificationJob } from '../types/queue.js';
import { RedisConnectionConfig } from '../types/config.js';

export interface DeadLetterEntry {
  job: NotificationJob;
  error: {
    message: string;
    stack?: string;
    name: string;
  };
  failedAt: number;
}

export type DeadLetterListener = (entry: DeadLetterEntry) => void | Promise<void>;

export interface IDeadLetterQueue {
  push(job: NotificationJob, error: Error): Promise<void>;
  get(jobId: string): Promise<DeadLetterEntry | undefined>;
  getAll(limit?: number): Promise<DeadLetterEntry[]>;
  count(): Promise<number>;
  remove(jobId: string): Promise<boolean>;
  clear(): Promise<void>;
  onDeadLetter(listener: DeadLetterListener): void;
  close?(): Promise<void>;
}

export class InMemoryDeadLetterQueue implements IDeadLetterQueue {
  private readonly entries = new Map<string, DeadLetterEntry>();
  private readonly maxStored: number;
  private readonly listeners: DeadLetterListener[] = [];

  constructor(maxStored = 1000) {
    this.maxStored = maxStored;
  }

  public async push(job: NotificationJob, error: Error): Promise<void> {
    const entry: DeadLetterEntry = {
      job,
      error: {
        message: error.message,
        stack: error.stack,
        name: error.name,
      },
      failedAt: Date.now(),
    };

    // FIFO eviction if storage limit reached
    if (this.entries.size >= this.maxStored) {
      const oldestKey = this.entries.keys().next().value;
      if (oldestKey) {
        this.entries.delete(oldestKey);
      }
    }

    this.entries.set(job.id, entry);

    // Notify listeners (Slack webhook, Sentry, logging)
    for (const listener of this.listeners) {
      try {
        await listener(entry);
      } catch (listenerErr) {
        console.error('Error in DeadLetterQueue listener:', listenerErr);
      }
    }
  }

  public async get(jobId: string): Promise<DeadLetterEntry | undefined> {
    return this.entries.get(jobId);
  }

  public async getAll(limit?: number): Promise<DeadLetterEntry[]> {
    const all = Array.from(this.entries.values());
    return limit ? all.slice(0, limit) : all;
  }

  public async count(): Promise<number> {
    return this.entries.size;
  }

  public async remove(jobId: string): Promise<boolean> {
    return this.entries.delete(jobId);
  }

  public async clear(): Promise<void> {
    this.entries.clear();
  }

  public onDeadLetter(listener: DeadLetterListener): void {
    this.listeners.push(listener);
  }

  public async close(): Promise<void> {
    this.entries.clear();
    this.listeners.length = 0;
  }
}

// Backward-compatible alias
export class DeadLetterQueue extends InMemoryDeadLetterQueue {}

export interface RedisDeadLetterQueueOptions {
  maxStored?: number;
  keyPrefix?: string;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  client?: any;
}

export class RedisDeadLetterQueue implements IDeadLetterQueue {
  private readonly maxStored: number;
  private readonly hashKey: string;
  private readonly zsetKey: string;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private redis: any;
  private isOwnedConnection = false;
  private readonly connectionConfig?: RedisConnectionConfig | string;
  private readonly listeners: DeadLetterListener[] = [];

  constructor(
    connectionConfig?: RedisConnectionConfig | string,
    options: RedisDeadLetterQueueOptions = {}
  ) {
    this.connectionConfig = connectionConfig;
    this.maxStored = options.maxStored ?? 1000;
    const prefix = options.keyPrefix ?? 'dn:dlq:';
    this.hashKey = `${prefix}data`;
    this.zsetKey = `${prefix}index`;
    if (options.client) {
      this.redis = options.client;
      this.isOwnedConnection = false;
    }
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private async getClient(): Promise<any> {
    if (this.redis) return this.redis;

    try {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const ioredis = await (Function('return import("ioredis")')() as Promise<any>);
      const RedisClass = ioredis.default || ioredis.Redis || ioredis;

      if (typeof this.connectionConfig === 'string') {
        this.redis = new RedisClass(this.connectionConfig, { maxRetriesPerRequest: null });
      } else if (this.connectionConfig && typeof this.connectionConfig === 'object') {
        const { url, ...opts } = this.connectionConfig;
        this.redis = url
          ? new RedisClass(url, { ...opts, maxRetriesPerRequest: null })
          : new RedisClass({ ...opts, maxRetriesPerRequest: null });
      } else {
        this.redis = new RedisClass({ maxRetriesPerRequest: null });
      }

      this.isOwnedConnection = true;
      return this.redis;
    } catch {
      throw new Error(
        'ioredis is required to use RedisDeadLetterQueue. Please install it:\n' +
        '  npm install ioredis'
      );
    }
  }

  public async push(job: NotificationJob, error: Error): Promise<void> {
    const client = await this.getClient();
    const now = Date.now();
    const entry: DeadLetterEntry = {
      job,
      error: {
        message: error.message,
        stack: error.stack,
        name: error.name,
      },
      failedAt: now,
    };

    const serialized = JSON.stringify(entry);

    const pipeline = client.pipeline();
    pipeline.hset(this.hashKey, job.id, serialized);
    pipeline.zadd(this.zsetKey, now, job.id);
    await pipeline.exec();

    // Evict oldest from both zset and hash if exceeding maxStored
    const count = await client.zcard(this.zsetKey);
    if (count > this.maxStored) {
      const excess = count - this.maxStored;
      const toRemove: string[] = await client.zrange(this.zsetKey, 0, excess - 1);
      if (toRemove && toRemove.length > 0) {
        const evictPipeline = client.pipeline();
        evictPipeline.zrem(this.zsetKey, ...toRemove);
        evictPipeline.hdel(this.hashKey, ...toRemove);
        await evictPipeline.exec();
      }
    }

    // Notify listeners
    for (const listener of this.listeners) {
      try {
        await listener(entry);
      } catch (listenerErr) {
        console.error('Error in RedisDeadLetterQueue listener:', listenerErr);
      }
    }
  }

  public async get(jobId: string): Promise<DeadLetterEntry | undefined> {
    const client = await this.getClient();
    const data = await client.hget(this.hashKey, jobId);
    if (!data) return undefined;
    try {
      return JSON.parse(data);
    } catch {
      return undefined;
    }
  }

  public async getAll(limit?: number): Promise<DeadLetterEntry[]> {
    const client = await this.getClient();
    const end = limit ? limit - 1 : -1;
    // Get jobIds ordered newest first
    const jobIds: string[] = await client.zrevrange(this.zsetKey, 0, end);
    if (!jobIds || jobIds.length === 0) return [];

    const rawEntries: (string | null)[] = await client.hmget(this.hashKey, ...jobIds);
    const results: DeadLetterEntry[] = [];
    for (const item of rawEntries) {
      if (item) {
        try {
          results.push(JSON.parse(item));
        } catch {
          // ignore corrupted items
        }
      }
    }
    return results;
  }

  public async count(): Promise<number> {
    const client = await this.getClient();
    return await client.zcard(this.zsetKey);
  }

  public async remove(jobId: string): Promise<boolean> {
    const client = await this.getClient();
    const pipeline = client.pipeline();
    pipeline.hdel(this.hashKey, jobId);
    pipeline.zrem(this.zsetKey, jobId);
    const results = await pipeline.exec();
    return (results?.[0]?.[1] as number) > 0;
  }

  public async clear(): Promise<void> {
    const client = await this.getClient();
    await client.del(this.hashKey, this.zsetKey);
  }

  public onDeadLetter(listener: DeadLetterListener): void {
    this.listeners.push(listener);
  }

  public async close(): Promise<void> {
    this.listeners.length = 0;
    if (this.isOwnedConnection && this.redis) {
      try {
        this.redis.disconnect(false);
      } catch {}
      this.redis = null;
    }
  }
}
