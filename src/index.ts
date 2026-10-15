// Main Orchestrator Service
export { Notifier, type NotifierEventMap } from './services/notifier.service.js';
export { FallbackRouter } from './services/fallback-router.js';

// Providers
export { BaseProvider, type BaseProviderOptions } from './providers/base.provider.js';
export { PushProvider, type PushProviderOptions, type PushSenderFunction } from './providers/push.provider.js';
export { SocketProvider, type SocketProviderOptions, type SocketEmitterFunction } from './providers/socket.provider.js';
export { EmailProvider, type EmailProviderOptions, type EmailSenderFunction } from './providers/email.provider.js';
export { SmsProvider, type SmsProviderOptions, type SmsSenderFunction } from './providers/sms.provider.js';
export { WebhookProvider, type WebhookProviderOptions, type WebhookSenderFunction } from './providers/webhook.provider.js';
export { MockProvider, type MockProviderOptions } from './providers/mock.provider.js';

// Engines & Algorithms
export { InMemoryQueueEngine } from './engines/queue/in-memory-queue.js';
export { BullMQQueueEngine } from './engines/queue/bullmq-queue.js';
export {
  InMemoryDeduplicationEngine,
  RedisDeduplicationEngine,
  type IDeduplicationEngine,
  type InMemoryDeduplicationOptions,
  type RedisDeduplicationOptions,
} from './engines/deduplication.js';
export {
  InMemoryTokenBucketRateLimiter,
  RedisTokenBucketRateLimiter,
  type IRateLimiterEngine,
  type RateLimitResult,
  type InMemoryRateLimiterOptions,
  type RedisRateLimiterOptions,
} from './engines/rate-limiter.js';
export { TemplateEngine, type NotificationTemplate, type TemplateContent } from './engines/template-engine.js';
export { QuietHoursEngine, type QuietHoursOptions, type QuietHoursCheckResult } from './engines/quiet-hours.js';
export { DigestEngine, type DigestFlushCallback, type DigestItem } from './engines/digest-engine.js';
export {
  InMemoryDeadLetterQueue,
  RedisDeadLetterQueue,
  DeadLetterQueue,
  type IDeadLetterQueue,
  type DeadLetterEntry,
  type DeadLetterListener,
  type RedisDeadLetterQueueOptions,
} from './engines/dead-letter-queue.js';
export { ProviderTrafficShaper, type TrafficShaperOptions } from './engines/traffic-shaper.js';

// Data Structures & Algorithmic Utilities
export { MinHeap, type HeapItem } from './utils/min-heap.js';
export { calculateFullJitterBackoff, calculateEqualJitterBackoff } from './utils/backoff.js';
export { CircuitBreaker, type CircuitBreakerOptions, type CircuitState } from './utils/circuit-breaker.js';
export { DefaultLogger, type Logger, type LogLevel } from './utils/logger.js';

// Validation Schemas
export {
  notificationChannelSchema,
  notificationPrioritySchema,
  recipientSchema,
  notificationPayloadSchema,
  notifierConfigSchema,
} from './validations/schemas.js';

// Core Types
export type {
  NotificationChannel,
  NotificationPriority,
  NotificationStatus,
  Recipient,
  RecipientPreferences,
  NotificationPayload,
  ChannelDeliveryResult,
  NotificationResult,
  DispatchResult,
} from './types/notification.js';

export type { IProvider, MulticastResult } from './types/provider.js';

export type {
  IQueueEngine,
  NotificationJob,
  JobEnqueueOptions,
  RetryPolicy,
} from './types/queue.js';

export type {
  NotifierConfig,
  RedisConnectionConfig,
  RateLimitConfig,
  DeduplicationConfig,
  QuietHoursConfig,
  DeadLetterConfig,
  DigestConfig,
} from './types/config.js';
