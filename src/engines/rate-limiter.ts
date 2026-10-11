import { RedisConnectionConfig } from '../types/config.js';

export interface RateLimitResult {
  allowed: boolean;
  remainingTokens: number;
  retryAfterSeconds: number;
}

export interface IRateLimiterEngine {
  consume(key: string): Promise<RateLimitResult>;
  reset(key: string): Promise<void>;
  close?(): Promise<void>;
}

interface Bucket {
  tokens: number;
  lastRefillTimestamp: number;
}

export interface InMemoryRateLimiterOptions {
  maxPerWindow?: number;
  windowSeconds?: number;
  maxBuckets?: number; // Capacity protection (default: 50,000)
}

export class InMemoryTokenBucketRateLimiter implements IRateLimiterEngine {
  private readonly buckets = new Map<string, Bucket>();
  private readonly capacity: number;
  private readonly refillRatePerMs: number;
  private readonly maxBuckets: number;
  private cleanupTimer: NodeJS.Timeout | null = null;
  private isDestroyed = false;

  constructor(maxPerWindowOrOptions: number | InMemoryRateLimiterOptions = 10, windowSeconds = 60) {
    if (typeof maxPerWindowOrOptions === 'number') {
      this.capacity = Math.max(1, maxPerWindowOrOptions);
      this.refillRatePerMs = this.capacity / (Math.max(1, windowSeconds) * 1000);
      this.maxBuckets = 50000;
    } else {
      this.capacity = Math.max(1, maxPerWindowOrOptions.maxPerWindow ?? 10);
      const winSec = Math.max(1, maxPerWindowOrOptions.windowSeconds ?? 60);
      this.refillRatePerMs = this.capacity / (winSec * 1000);
      this.maxBuckets = maxPerWindowOrOptions.maxBuckets ?? 50000;
    }

    // Periodic cleanup of stale buckets every 5 minutes
    this.cleanupTimer = setInterval(() => this.cleanup(), 300000);
    if (this.cleanupTimer.unref) {
      this.cleanupTimer.unref();
    }
  }

  async consume(key: string): Promise<RateLimitResult> {
    if (this.isDestroyed) {
      return { allowed: true, remainingTokens: this.capacity, retryAfterSeconds: 0 };
    }

    const now = Date.now();
    let bucket = this.buckets.get(key);

    if (!bucket) {
      // Memory bound check: evict oldest entry if capacity reached
      if (this.buckets.size >= this.maxBuckets) {
        const oldestKey = this.buckets.keys().next().value;
        if (oldestKey) {
          this.buckets.delete(oldestKey);
        }
      }

      bucket = { tokens: this.capacity, lastRefillTimestamp: now };
      this.buckets.set(key, bucket);
    } else {
      // Refill tokens based on elapsed time
      const elapsedMs = Math.max(0, now - bucket.lastRefillTimestamp);
      const tokensToAdd = elapsedMs * this.refillRatePerMs;
      bucket.tokens = Math.min(this.capacity, bucket.tokens + tokensToAdd);
      bucket.lastRefillTimestamp = now;
    }

    if (bucket.tokens >= 1) {
      bucket.tokens -= 1;
      return {
        allowed: true,
        remainingTokens: Math.floor(bucket.tokens),
        retryAfterSeconds: 0,
      };
    }

    // Rate limited - calculate time until 1 token is available
    const missingTokens = 1 - bucket.tokens;
    const waitMs = missingTokens / this.refillRatePerMs;
    const retryAfterSeconds = Math.ceil(waitMs / 1000);

    return {
      allowed: false,
      remainingTokens: 0,
      retryAfterSeconds: Math.max(1, retryAfterSeconds),
    };
  }

  async reset(key: string): Promise<void> {
    this.buckets.delete(key);
  }

  private cleanup(): void {
    if (this.isDestroyed) return;
    const now = Date.now();
    const staleThresholdMs = 300000; // 5 minutes inactivity
    for (const [key, bucket] of this.buckets.entries()) {
      if (now - bucket.lastRefillTimestamp > staleThresholdMs && bucket.tokens >= this.capacity) {
        this.buckets.delete(key);
      }
    }
  }

  public destroy(): void {
    this.isDestroyed = true;
    if (this.cleanupTimer) {
      clearInterval(this.cleanupTimer);
      this.cleanupTimer = null;
    }
    this.buckets.clear();
  }

  public async close(): Promise<void> {
    this.destroy();
  }
}

export interface RedisRateLimiterOptions {
  maxPerWindow?: number;
  windowSeconds?: number;
  keyPrefix?: string;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  client?: any;
}

const TOKEN_BUCKET_LUA_SCRIPT = `
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local window_ms = tonumber(ARGV[4])

local data = redis.call('HMGET', key, 'tokens', 'last_refill')
local tokens = tonumber(data[1])
local last_refill = tonumber(data[2])

if not tokens or not last_refill then
  tokens = capacity - 1
  last_refill = now
  redis.call('HMSET', key, 'tokens', tokens, 'last_refill', last_refill)
  redis.call('PEXPIRE', key, window_ms * 2)
  return {1, math.floor(tokens), 0}
else
  local elapsed = math.max(0, now - last_refill)
  tokens = math.min(capacity, tokens + (elapsed * refill_rate))
  last_refill = now
  if tokens >= 1 then
    tokens = tokens - 1
    redis.call('HMSET', key, 'tokens', tokens, 'last_refill', last_refill)
    redis.call('PEXPIRE', key, window_ms * 2)
    return {1, math.floor(tokens), 0}
  else
    local missing = 1 - tokens
    local wait_ms = math.ceil(missing / refill_rate)
    local retry_after = math.max(1, math.ceil(wait_ms / 1000))
    redis.call('HMSET', key, 'tokens', tokens, 'last_refill', last_refill)
    redis.call('PEXPIRE', key, window_ms * 2)
    return {0, 0, retry_after}
  end
end
`;

export class RedisTokenBucketRateLimiter implements IRateLimiterEngine {
  private readonly capacity: number;
  private readonly windowSeconds: number;
  private readonly refillRatePerMs: number;
  private readonly keyPrefix: string;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private redis: any;
  private isOwnedConnection = false;
  private readonly connectionConfig?: RedisConnectionConfig | string;

  constructor(
    connectionConfig?: RedisConnectionConfig | string,
    options: RedisRateLimiterOptions = {}
  ) {
    this.connectionConfig = connectionConfig;
    this.capacity = Math.max(1, options.maxPerWindow ?? 10);
    this.windowSeconds = Math.max(1, options.windowSeconds ?? 60);
    this.refillRatePerMs = this.capacity / (this.windowSeconds * 1000);
    this.keyPrefix = options.keyPrefix ?? 'dn:rate:';
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
        'ioredis is required to use RedisTokenBucketRateLimiter. Please install it:\n' +
        '  npm install ioredis'
      );
    }
  }

  private getKey(key: string): string {
    return `${this.keyPrefix}${key}`;
  }

  async consume(key: string): Promise<RateLimitResult> {
    const client = await this.getClient();
    const now = Date.now();
    const windowMs = this.windowSeconds * 1000;

    const res = await client.eval(
      TOKEN_BUCKET_LUA_SCRIPT,
      1,
      this.getKey(key),
      this.capacity,
      this.refillRatePerMs,
      now,
      windowMs
    );

    const [allowedNum, remainingTokens, retryAfterSeconds] = res as [number, number, number];

    return {
      allowed: allowedNum === 1,
      remainingTokens: Number(remainingTokens) || 0,
      retryAfterSeconds: Number(retryAfterSeconds) || 0,
    };
  }

  async reset(key: string): Promise<void> {
    const client = await this.getClient();
    await client.del(this.getKey(key));
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
