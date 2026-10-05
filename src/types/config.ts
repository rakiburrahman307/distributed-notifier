import { NotificationJob } from './queue.js';
import { IProvider } from './provider.js';
import { IDeduplicationEngine } from '../engines/deduplication.js';
import { IRateLimiterEngine } from '../engines/rate-limiter.js';
import { IDeadLetterQueue } from '../engines/dead-letter-queue.js';

export interface RedisConnectionConfig {
  host?: string;
  port?: number;
  password?: string;
  url?: string;
  db?: number;
  keyPrefix?: string;
}

export interface RateLimitConfig {
  enabled: boolean;
  maxPerWindow: number; // e.g. 10 requests
  windowSeconds: number; // e.g. 60 seconds
}

export interface DeduplicationConfig {
  enabled: boolean;
  ttlSeconds: number; // e.g. 300 seconds (5 min)
}

export interface QuietHoursConfig {
  enabled: boolean;
  startHour: number; // 0-23 (e.g. 23 = 11 PM)
  endHour: number; // 0-23 (e.g. 7 = 7 AM)
  defaultTimezone?: string; // Default: 'UTC'
  allowUrgentOverride?: boolean; // If priority === 'HIGH', bypass quiet hours
}

export interface DeadLetterConfig {
  enabled: boolean;
  maxRetries: number;
  maxStored?: number; // e.g. 1000 items limit
  onDeadLetter?: (job: NotificationJob, error: Error) => void | Promise<void>;
}

export interface DigestConfig {
  enabled: boolean;
  windowSeconds: number; // e.g. 120 seconds batch window
}

export interface NotifierConfig {
  mode?: 'memory' | 'redis';
  redis?: RedisConnectionConfig | string;
  providers?: IProvider[];
  rateLimit?: Partial<RateLimitConfig>;
  deduplication?: Partial<DeduplicationConfig>;
  quietHours?: Partial<QuietHoursConfig>;
  deadLetter?: Partial<DeadLetterConfig>;
  digest?: Partial<DigestConfig>;
  maxConcurrency?: number;
  maxQueueSize?: number;
  defaultRetryPolicy?: {
    maxAttempts: number;
    backoffMs: number;
    backoffFactor: number;
  };
  // Custom engine overrides if provided
  deduplicationEngine?: IDeduplicationEngine;
  rateLimiterEngine?: IRateLimiterEngine;
  deadLetterQueue?: IDeadLetterQueue;
}
