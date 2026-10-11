import { RedisConnectionConfig } from '../types/config.js';

export interface IDeduplicationEngine {
  isDuplicate(key: string): Promise<boolean>;
  record(key: string): Promise<void>;
  checkAndRecord?(key: string): Promise<boolean>; // Returns true if DUPLICATE, false if accepted
  clear(): Promise<void>;
  close?(): Promise<void>;
}

export interface InMemoryDeduplicationOptions {
  ttlSeconds?: number;
  maxEntries?: number;
}

export class InMemoryDeduplicationEngine implements IDeduplicationEngine {
  private readonly store = new Map<string, number>(); // key -> expiration timestamp
  private readonly ttlMs: number;
  private readonly maxEntries: number;
  private cleanupTimer: NodeJS.Timeout | null = null;
  private isDestroyed = false;

  constructor(options: number | InMemoryDeduplicationOptions = 300) {
    if (typeof options === 'number') {
      this.ttlMs = options * 1000;
      this.maxEntries = 50000;
    } else {
      this.ttlMs = (options.ttlSeconds ?? 300) * 1000;
      this.maxEntries = options.maxEntries ?? 50000;
    }

    // Periodic garbage collection every 60 seconds
    this.cleanupTimer = setInterval(() => this.purgeExpired(), 60000);
    if (this.cleanupTimer.unref) {
      this.cleanupTimer.unref();
    }
  }

  async isDuplicate(key: string): Promise<boolean> {
    if (this.isDestroyed) return false;
    const expiresAt = this.store.get(key);
    if (!expiresAt) return false;

    if (Date.now() > expiresAt) {
      this.store.delete(key);
      return false;
    }

    return true;
  }

  async record(key: string): Promise<void> {
    if (this.isDestroyed) return;

    // Evict oldest if max capacity reached
    if (this.store.size >= this.maxEntries) {
      const oldestKey = this.store.keys().next().value;
      if (oldestKey) {
        this.store.delete(oldestKey);
      }
    }

    this.store.set(key, Date.now() + this.ttlMs);
  }

  /**
   * Atomic check and record: returns true if already duplicate, false if newly recorded
   */
  async checkAndRecord(key: string): Promise<boolean> {
    const isDupe = await this.isDuplicate(key);
    if (isDupe) return true;
    await this.record(key);
    return false;
  }

  async clear(): Promise<void> {
    this.store.clear();
  }

  private purgeExpired(): void {
    if (this.isDestroyed) return;
    const now = Date.now();
    for (const [key, expiresAt] of this.store.entries()) {
      if (now > expiresAt) {
        this.store.delete(key);
      }
    }
  }

  public destroy(): void {
    this.isDestroyed = true;
    if (this.cleanupTimer) {
      clearInterval(this.cleanupTimer);
      this.cleanupTimer = null;
    }
    this.store.clear();
  }

  public async close(): Promise<void> {
    this.destroy();
  }
}

export interface RedisDeduplicationOptions {
  ttlSeconds?: number;
  keyPrefix?: string;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  client?: any;
}

export class RedisDeduplicationEngine implements IDeduplicationEngine {
  private readonly ttlSeconds: number;
  private readonly keyPrefix: string;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private redis: any;
  private isOwnedConnection = false;
  private readonly connectionConfig?: RedisConnectionConfig | string;

  constructor(
    connectionConfig?: RedisConnectionConfig | string,
    options: RedisDeduplicationOptions = {}
  ) {
    this.connectionConfig = connectionConfig;
    this.ttlSeconds = options.ttlSeconds ?? 300;
    this.keyPrefix = options.keyPrefix ?? 'dn:dedup:';
    if (options.client) {
      this.redis = options.client;
      this.isOwnedConnection = false;
    }
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private async getClient(): Promise<any> {
    if (this.redis) return this.redis;

    try {
      // Dynamic import of ioredis
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
        'ioredis is required to use RedisDeduplicationEngine. Please install it:\n' +
        '  npm install ioredis'
      );
    }
  }

  private getKey(key: string): string {
    return `${this.keyPrefix}${key}`;
  }

  async isDuplicate(key: string): Promise<boolean> {
    const client = await this.getClient();
    const exists = await client.exists(this.getKey(key));
    return exists === 1;
  }

  async record(key: string): Promise<void> {
    const client = await this.getClient();
    await client.set(this.getKey(key), '1', 'EX', this.ttlSeconds);
  }

  /**
   * Atomic check and record using Redis SET ... NX EX
   * Returns true if already exists (DUPLICATE), false if successfully registered
   */
  async checkAndRecord(key: string): Promise<boolean> {
    const client = await this.getClient();
    // SET key 1 EX ttl NX returns 'OK' if key did not exist, null if it already exists
    const result = await client.set(this.getKey(key), '1', 'EX', this.ttlSeconds, 'NX');
    return result !== 'OK';
  }

  async clear(): Promise<void> {
    const client = await this.getClient();
    let cursor = '0';
    do {
      const [nextCursor, keys] = await client.scan(cursor, 'MATCH', `${this.keyPrefix}*`, 'COUNT', 100);
      cursor = nextCursor;
      if (keys && keys.length > 0) {
        await client.del(...keys);
      }
    } while (cursor !== '0');
  }

  async close(): Promise<void> {
    if (this.isOwnedConnection && this.redis) {
      try {
        this.redis.disconnect(false);
      } catch {}
      this.redis = null;
    }
  }
}
