import { EventEmitter } from 'node:events';
import { NotifierConfig } from '../types/config.js';
import {
  NotificationPayload,
  NotificationResult,
  NotificationStatus,
  DispatchResult,
} from '../types/notification.js';
import { IProvider } from '../types/provider.js';
import { IQueueEngine, NotificationJob } from '../types/queue.js';
import { notificationPayloadSchema } from '../validations/schemas.js';
import { InMemoryQueueEngine } from '../engines/queue/in-memory-queue.js';
import { BullMQQueueEngine } from '../engines/queue/bullmq-queue.js';
import {
  InMemoryDeduplicationEngine,
  RedisDeduplicationEngine,
  IDeduplicationEngine,
} from '../engines/deduplication.js';
import {
  InMemoryTokenBucketRateLimiter,
  RedisTokenBucketRateLimiter,
  IRateLimiterEngine,
} from '../engines/rate-limiter.js';
import { TemplateEngine, NotificationTemplate } from '../engines/template-engine.js';
import { QuietHoursEngine } from '../engines/quiet-hours.js';
import { DigestEngine } from '../engines/digest-engine.js';
import {
  InMemoryDeadLetterQueue,
  RedisDeadLetterQueue,
  IDeadLetterQueue,
  DeadLetterEntry,
} from '../engines/dead-letter-queue.js';
import { FallbackRouter } from './fallback-router.js';
import { DefaultLogger, Logger } from '../utils/logger.js';

export interface NotifierEventMap {
  dispatched: (result: DispatchResult, payload: NotificationPayload) => void;
  delivered: (result: NotificationResult, payload: NotificationPayload) => void;
  failed: (result: NotificationResult, payload: NotificationPayload) => void;
  suppressed: (event: { reason: NotificationStatus; payload: NotificationPayload }) => void;
  dead_letter: (entry: DeadLetterEntry) => void;
}

export class Notifier extends EventEmitter {
  private readonly config: NotifierConfig;
  private readonly queueEngine: IQueueEngine;
  private readonly deduplication: IDeduplicationEngine;
  private readonly rateLimiter: IRateLimiterEngine;
  private readonly templateEngine: TemplateEngine;
  private readonly quietHours: QuietHoursEngine;
  private readonly digestEngine: DigestEngine;
  private readonly deadLetterQueue: IDeadLetterQueue;
  private readonly router: FallbackRouter;
  private readonly logger: Logger;
  private readonly registeredProviders: IProvider[] = [];
  private isClosed = false;

  constructor(config: NotifierConfig = {}) {
    super();
    this.config = config;
    this.logger = new DefaultLogger();

    const isRedis = config.mode === 'redis' && Boolean(config.redis);

    // 1. Initialize Queue Engine
    if (isRedis && config.redis) {
      this.queueEngine = new BullMQQueueEngine(config.redis);
    } else {
      this.queueEngine = new InMemoryQueueEngine(
        config.maxConcurrency ?? 10,
        config.maxQueueSize ?? 10000
      );
    }

    // 2. Initialize Distributed or In-Memory State Engines
    if (isRedis && config.redis) {
      this.deduplication =
        config.deduplicationEngine ??
        new RedisDeduplicationEngine(config.redis, {
          ttlSeconds: config.deduplication?.ttlSeconds,
        });

      this.rateLimiter =
        config.rateLimiterEngine ??
        new RedisTokenBucketRateLimiter(config.redis, {
          maxPerWindow: config.rateLimit?.maxPerWindow,
          windowSeconds: config.rateLimit?.windowSeconds,
        });

      this.deadLetterQueue =
        config.deadLetterQueue ??
        new RedisDeadLetterQueue(config.redis, {
          maxStored: config.deadLetter?.maxStored,
        });
    } else {
      this.deduplication =
        config.deduplicationEngine ??
        new InMemoryDeduplicationEngine({
          ttlSeconds: config.deduplication?.ttlSeconds ?? 300,
        });

      this.rateLimiter =
        config.rateLimiterEngine ??
        new InMemoryTokenBucketRateLimiter(
          config.rateLimit?.maxPerWindow ?? 10,
          config.rateLimit?.windowSeconds ?? 60
        );

      this.deadLetterQueue =
        config.deadLetterQueue ??
        new InMemoryDeadLetterQueue(config.deadLetter?.maxStored ?? 1000);
    }

    this.templateEngine = new TemplateEngine();
    this.quietHours = new QuietHoursEngine(config.quietHours);
    this.digestEngine = new DigestEngine(
      config.digest?.enabled ?? false,
      config.digest?.windowSeconds ?? 120
    );
    this.router = new FallbackRouter();

    // 3. Register custom dead letter listener and emit 'dead_letter' event
    this.deadLetterQueue.onDeadLetter((entry) => {
      this.emit('dead_letter', entry);
      if (config.deadLetter?.onDeadLetter) {
        const error = new Error(entry.error.message);
        error.stack = entry.error.stack;
        config.deadLetter.onDeadLetter(entry.job, error);
      }
    });

    // 4. Hook Queue failure handler into DLQ
    if ('setFailureHandler' in this.queueEngine) {
      (this.queueEngine as any).setFailureHandler(async (job: NotificationJob, err: Error) => {
        if (this.config.deadLetter?.enabled !== false) {
          await this.deadLetterQueue.push(job, err);
        }
      });
    }

    // 5. Register Initial Providers
    if (config.providers && config.providers.length > 0) {
      for (const provider of config.providers) {
        this.registerProvider(provider);
      }
    }

    // 6. Hook up Digest Flush Handler
    this.digestEngine.setFlushCallback(async (recipientId, payloads) => {
      this.logger.info(`Flushing digest batch of ${payloads.length} items for recipient ${recipientId}`);
      const combinedPayload: NotificationPayload = {
        id: `digest_${Date.now()}_${recipientId}`,
        recipient: payloads[0].recipient,
        channels: payloads[0].channels,
        priority: 'NORMAL',
        title: `You have ${payloads.length} updates`,
        body: payloads.map((p) => p.title || p.body).filter(Boolean).join('\n'),
        data: { count: payloads.length, itemIds: payloads.map((p) => p.id) },
      };
      await this.deliverInternal(combinedPayload);
    });

    // 7. Hook up Queue Worker Processor
    this.queueEngine.process(async (payload) => {
      await this.deliverInternal(payload);
    });
  }

  /**
   * Preflight checks: schema validation, template resolution, topic subscriptions,
   * idempotency/deduplication, and rate limiting.
   */
  private async preflight(payloadInput: NotificationPayload): Promise<{
    payload: NotificationPayload;
    isDuplicate?: boolean;
    isRateLimited?: boolean;
    isUnsubscribed?: boolean;
  }> {
    // 1. Zod schema validation
    const parsed = notificationPayloadSchema.safeParse(payloadInput);
    if (!parsed.success) {
      const errorMsg = parsed.error.issues
        .map((i) => `${i.path.join('.')}: ${i.message}`)
        .join(', ');
      throw new Error(`Invalid notification payload: ${errorMsg}`);
    }

    const payload = { ...parsed.data } as NotificationPayload;

    // 2. Template resolution if templateId is specified
    if (payload.templateId) {
      const targetLocale = payload.locale || payload.recipient.locale;
      const rendered = this.templateEngine.render(
        payload.templateId,
        payload.variables,
        targetLocale
      );
      payload.title = payload.title || rendered.title;
      payload.body = payload.body || rendered.body;
      if (rendered.defaultChannels && (!payload.channels || payload.channels.length === 0)) {
        payload.channels = rendered.defaultChannels;
      }
      if (rendered.data) {
        payload.data = { ...rendered.data, ...payload.data };
      }
    }

    // 3. User Topic / Category Preference check
    const topic = payload.topic || payload.category;
    const isCritical =
      payload.priority === 'HIGH' || topic === 'security' || topic === 'transactional';

    if (topic && payload.recipient.preferences) {
      const topicsMap =
        payload.recipient.preferences.topics || payload.recipient.preferences.categories;
      if (topicsMap && topicsMap[topic] === false && !isCritical) {
        this.logger.info(
          `Suppressed notification ${payload.id}: Recipient ${payload.recipient.id} opted out of topic "${topic}"`
        );
        return { payload, isUnsubscribed: true };
      }
    }

    // 4. Check Deduplication (Idempotency Key)
    if (this.config.deduplication?.enabled !== false && payload.idempotencyKey) {
      let isDupe = false;
      if (this.deduplication.checkAndRecord) {
        isDupe = await this.deduplication.checkAndRecord(payload.idempotencyKey);
      } else {
        isDupe = await this.deduplication.isDuplicate(payload.idempotencyKey);
        if (!isDupe) {
          await this.deduplication.record(payload.idempotencyKey);
        }
      }

      if (isDupe) {
        this.logger.warn(`Suppressed duplicate notification with key "${payload.idempotencyKey}"`);
        return { payload, isDuplicate: true };
      }
    }

    // 5. Check Rate Limiter
    if (this.config.rateLimit?.enabled !== false) {
      const rateCheck = await this.rateLimiter.consume(payload.recipient.id);
      if (!rateCheck.allowed) {
        this.logger.warn(
          `Rate limit exceeded for recipient "${payload.recipient.id}". Retry after ${rateCheck.retryAfterSeconds}s`
        );
        return { payload, isRateLimited: true };
      }
    }

    return { payload };
  }

  /**
   * Asynchronous non-blocking dispatch:
   * Validates pre-flight rules, pushes to the queue (BullMQ or InMemory),
   * and immediately returns a DispatchResult with stable jobId.
   */
  public async dispatch(payloadInput: NotificationPayload): Promise<DispatchResult> {
    if (this.isClosed) {
      throw new Error('Notifier is closed and cannot dispatch notifications');
    }

    const { payload, isDuplicate, isRateLimited, isUnsubscribed } = await this.preflight(
      payloadInput
    );

    if (isDuplicate) {
      const result: DispatchResult = {
        jobId: payload.id,
        notificationId: payload.id,
        status: 'SKIPPED_DUPLICATE',
        enqueuedAt: new Date(),
        idempotencyKey: payload.idempotencyKey,
      };
      this.emit('suppressed', { reason: 'SKIPPED_DUPLICATE', payload });
      this.emit('dispatched', result, payload);
      return result;
    }

    if (isRateLimited) {
      const result: DispatchResult = {
        jobId: payload.id,
        notificationId: payload.id,
        status: 'RATE_LIMITED',
        enqueuedAt: new Date(),
      };
      this.emit('suppressed', { reason: 'RATE_LIMITED', payload });
      this.emit('dispatched', result, payload);
      return result;
    }

    if (isUnsubscribed) {
      const result: DispatchResult = {
        jobId: payload.id,
        notificationId: payload.id,
        status: 'UNSUBSCRIBED',
        enqueuedAt: new Date(),
      };
      this.emit('suppressed', { reason: 'UNSUBSCRIBED', payload });
      this.emit('dispatched', result, payload);
      return result;
    }

    // Check Quiet Hours
    let delayMs = 0;
    let scheduledFor: Date | undefined;

    if (this.config.quietHours?.enabled) {
      const quietCheck = this.quietHours.shouldSuppress(
        payload.priority,
        payload.recipient.timezone,
        payload.recipient.preferences?.quietHoursEnabled
      );

      if (quietCheck.isQuietTime && quietCheck.resumeAt) {
        delayMs = Math.max(0, quietCheck.resumeAt.getTime() - Date.now());
        scheduledFor = quietCheck.resumeAt;
      }
    }

    // Enqueue for background worker processing
    const jobId = await this.queueEngine.enqueue(payload, { delayMs });

    const dispatchResult: DispatchResult = {
      jobId,
      notificationId: payload.id,
      status: delayMs > 0 ? 'SCHEDULED_QUIET_HOURS' : 'QUEUED',
      enqueuedAt: new Date(),
      scheduledFor,
      idempotencyKey: payload.idempotencyKey,
    };

    this.emit('dispatched', dispatchResult, payload);
    return dispatchResult;
  }

  /**
   * Direct synchronous send: waits for third-party providers to deliver
   * and returns full execution result immediately.
   */
  public async sendSync(payloadInput: NotificationPayload): Promise<NotificationResult> {
    if (this.isClosed) {
      throw new Error('Notifier is closed and cannot send notifications');
    }

    const { payload, isDuplicate, isRateLimited, isUnsubscribed } = await this.preflight(
      payloadInput
    );

    if (isDuplicate) {
      const result: NotificationResult = {
        notificationId: payload.id,
        status: 'SKIPPED_DUPLICATE',
        channelsAttempted: [],
        channelResults: [],
        isFallbackUsed: false,
        idempotencyKey: payload.idempotencyKey,
      };
      this.emit('suppressed', { reason: 'SKIPPED_DUPLICATE', payload });
      return result;
    }

    if (isRateLimited) {
      const result: NotificationResult = {
        notificationId: payload.id,
        status: 'RATE_LIMITED',
        channelsAttempted: [],
        channelResults: [],
        isFallbackUsed: false,
      };
      this.emit('suppressed', { reason: 'RATE_LIMITED', payload });
      return result;
    }

    if (isUnsubscribed) {
      const result: NotificationResult = {
        notificationId: payload.id,
        status: 'UNSUBSCRIBED',
        channelsAttempted: [],
        channelResults: [],
        isFallbackUsed: false,
      };
      this.emit('suppressed', { reason: 'UNSUBSCRIBED', payload });
      return result;
    }

    // Check Quiet Hours
    if (this.config.quietHours?.enabled) {
      const quietCheck = this.quietHours.shouldSuppress(
        payload.priority,
        payload.recipient.timezone,
        payload.recipient.preferences?.quietHoursEnabled
      );

      if (quietCheck.isQuietTime && quietCheck.resumeAt) {
        const delayMs = Math.max(0, quietCheck.resumeAt.getTime() - Date.now());
        this.logger.info(
          `Quiet hours active for recipient ${payload.recipient.id}. Scheduled for ${quietCheck.resumeAt.toISOString()}`
        );
        const jobId = await this.queueEngine.enqueue(payload, { delayMs });

        return {
          notificationId: payload.id,
          jobId,
          status: 'SCHEDULED_QUIET_HOURS',
          channelsAttempted: [],
          channelResults: [],
          isFallbackUsed: false,
          suppressedUntil: quietCheck.resumeAt,
        };
      }
    }

    // Check Digest Engine (batching)
    if (this.digestEngine.shouldDigest(payload)) {
      this.digestEngine.add(payload);
      return {
        notificationId: payload.id,
        status: 'QUEUED',
        channelsAttempted: [],
        channelResults: [],
        isFallbackUsed: false,
      };
    }

    // Filter blocked channels according to recipient preferences
    if (payload.recipient.preferences?.blockedChannels) {
      const blocked = new Set(payload.recipient.preferences.blockedChannels);
      payload.channels = payload.channels.filter((ch) => !blocked.has(ch));
      if (payload.channels.length === 0) {
        return {
          notificationId: payload.id,
          status: 'SENT',
          channelsAttempted: [],
          channelResults: [],
          isFallbackUsed: false,
        };
      }
    }

    // Execute synchronous delivery through Fallback Router
    return await this.deliverInternal(payload);
  }

  /**
   * Main send method (backward-compatible):
   * By default executes delivery, or if options.async is true, dispatches to queue.
   */
  public async send(
    payloadInput: NotificationPayload,
    options?: { async?: boolean }
  ): Promise<NotificationResult | DispatchResult> {
    if (options?.async) {
      return await this.dispatch(payloadInput);
    }
    return await this.sendSync(payloadInput);
  }

  /**
   * Internal delivery executor with fallback routing and Dead Letter Queue recording
   */
  private async deliverInternal(payload: NotificationPayload): Promise<NotificationResult> {
    try {
      const { results, isFallbackUsed } = await this.router.deliverWithFallback(
        payload,
        payload.channels,
        payload.fallbackChannels
      );

      const anySuccess = results.some((r) => r.success);
      const allSuccess = results.length > 0 && results.every((r) => r.success);

      const status: NotificationStatus = allSuccess
        ? 'SENT'
        : anySuccess
        ? 'PARTIALLY_DELIVERED'
        : 'FAILED';

      if (!anySuccess && this.config.deadLetter?.enabled !== false) {
        const firstError =
          results.find((r) => !r.success)?.error || 'All channel attempts failed';
        await this.deadLetterQueue.push(
          {
            id: payload.id,
            payload,
            attemptsMade: 1,
            maxAttempts: 1,
            enqueuedAt: Date.now(),
          },
          new Error(firstError)
        );
      }

      const notificationResult: NotificationResult = {
        notificationId: payload.id,
        status,
        channelsAttempted: results.map((r) => r.channel),
        channelResults: results,
        deliveredAt: anySuccess ? new Date() : undefined,
        isFallbackUsed,
        idempotencyKey: payload.idempotencyKey,
      };

      if (anySuccess) {
        this.emit('delivered', notificationResult, payload);
      } else {
        this.emit('failed', notificationResult, payload);
      }

      return notificationResult;
    } catch (err) {
      const error = err instanceof Error ? err : new Error(String(err));
      this.logger.error(`Fatal delivery error for notification ${payload.id}:`, error);

      if (this.config.deadLetter?.enabled !== false) {
        await this.deadLetterQueue.push(
          {
            id: payload.id,
            payload,
            attemptsMade: 1,
            maxAttempts: 1,
            enqueuedAt: Date.now(),
          },
          error
        );
      }

      const failedResult: NotificationResult = {
        notificationId: payload.id,
        status: 'FAILED',
        channelsAttempted: payload.channels,
        channelResults: [
          {
            success: false,
            channel: payload.channels[0] || 'push',
            provider: 'internal',
            error: error.message,
            timestamp: Date.now(),
          },
        ],
        isFallbackUsed: false,
      };

      this.emit('failed', failedResult, payload);
      return failedResult;
    }
  }

  /**
   * Send multiple notifications with controlled parallel concurrency pool
   */
  public async sendBulk(
    payloads: NotificationPayload[],
    options: { concurrency?: number; async?: boolean } = {}
  ): Promise<(NotificationResult | DispatchResult)[]> {
    const concurrency = Math.max(1, options.concurrency ?? 5);
    const results: (NotificationResult | DispatchResult)[] = new Array(payloads.length);
    let index = 0;

    const worker = async () => {
      while (index < payloads.length) {
        const currentIndex = index++;
        const payload = payloads[currentIndex];
        if (options.async) {
          results[currentIndex] = await this.dispatch(payload);
        } else {
          results[currentIndex] = await this.sendSync(payload);
        }
      }
    };

    const workers = Array.from(
      { length: Math.min(concurrency, payloads.length) },
      () => worker()
    );
    await Promise.all(workers);

    return results;
  }

  /**
   * Schedule a notification for future delivery
   */
  public async schedule(
    payload: NotificationPayload,
    dateOrDelayMs: Date | number
  ): Promise<string> {
    const delayMs =
      typeof dateOrDelayMs === 'number'
        ? dateOrDelayMs
        : Math.max(0, dateOrDelayMs.getTime() - Date.now());

    return await this.queueEngine.enqueue(payload, { delayMs });
  }

  public registerProvider(provider: IProvider): void {
    this.router.registerProvider(provider);
    this.registeredProviders.push(provider);
    this.logger.info(`Registered provider "${provider.name}" for channel "${provider.channel}"`);
  }

  public registerTemplate(template: NotificationTemplate): void {
    this.templateEngine.register(template);
    this.logger.info(`Registered notification template "${template.id}"`);
  }

  public registerTemplates(templates: NotificationTemplate[]): void {
    this.templateEngine.registerBatch(templates);
  }

  public getDeadLetterQueue(): IDeadLetterQueue {
    return this.deadLetterQueue;
  }

  public override on<K extends keyof NotifierEventMap>(event: K, listener: NotifierEventMap[K]): this;
  public override on(event: string | symbol, listener: (...args: any[]) => void): this {
    return super.on(event, listener);
  }

  public override once<K extends keyof NotifierEventMap>(event: K, listener: NotifierEventMap[K]): this;
  public override once(event: string | symbol, listener: (...args: any[]) => void): this {
    return super.once(event, listener);
  }

  public override emit<K extends keyof NotifierEventMap>(
    event: K,
    ...args: Parameters<NotifierEventMap[K]>
  ): boolean;
  public override emit(event: string | symbol, ...args: any[]): boolean {
    return super.emit(event, ...args);
  }

  public async close(): Promise<void> {
    if (this.isClosed) return;
    this.isClosed = true;

    this.removeAllListeners();
    this.digestEngine.destroy();
    await this.queueEngine.close();

    if (this.deduplication.close) {
      await this.deduplication.close();
    }
    if (this.rateLimiter.close) {
      await this.rateLimiter.close();
    }
    if (this.deadLetterQueue.close) {
      await this.deadLetterQueue.close();
    }

    // Close all registered providers
    for (const provider of this.registeredProviders) {
      if ('close' in provider && typeof (provider as any).close === 'function') {
        try {
          await (provider as any).close();
        } catch {
          // Safe guard during shutdown
        }
      }
    }
  }
}
