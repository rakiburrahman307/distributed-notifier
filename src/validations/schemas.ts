import { z } from 'zod';

export const notificationChannelSchema = z.enum([
  'push',
  'sms',
  'email',
  'in_app',
  'webhook',
]);

export const notificationPrioritySchema = z.enum(['HIGH', 'NORMAL', 'LOW']);

export const recipientSchema = z.object({
  id: z.string().min(1, 'Recipient ID is required'),
  phone: z.string().optional(),
  email: z.string().email().optional(),
  pushTokens: z.array(z.string()).optional(),
  timezone: z.string().optional(),
  locale: z.string().optional(),
  preferences: z
    .object({
      blockedChannels: z.array(notificationChannelSchema).optional(),
      quietHoursEnabled: z.boolean().optional(),
      topics: z.record(z.boolean()).optional(),
      categories: z.record(z.boolean()).optional(),
    })
    .optional(),
});

export const notificationPayloadSchema = z.object({
  id: z.string().min(1, 'Notification ID is required'),
  recipient: recipientSchema,
  channels: z
    .array(notificationChannelSchema)
    .min(1, 'At least one notification channel must be specified'),
  priority: notificationPrioritySchema.optional().default('NORMAL'),
  topic: z.string().optional(),
  category: z.string().optional(),
  templateId: z.string().optional(),
  locale: z.string().optional(),
  title: z.string().optional(),
  body: z.string().optional(),
  data: z.record(z.unknown()).optional(),
  variables: z.record(z.unknown()).optional(),
  idempotencyKey: z.string().optional(),
  fallbackChannels: z.array(notificationChannelSchema).optional(),
  scheduledAt: z.date().optional(),
  metadata: z.record(z.unknown()).optional(),
});

export const notifierConfigSchema = z.object({
  mode: z.enum(['memory', 'redis']).default('memory'),
  redis: z
    .union([
      z.string(),
      z.object({
        host: z.string().optional(),
        port: z.number().optional(),
        password: z.string().optional(),
        url: z.string().optional(),
        db: z.number().optional(),
        keyPrefix: z.string().optional(),
      }),
    ])
    .optional(),
  rateLimit: z
    .object({
      enabled: z.boolean().default(true),
      maxPerWindow: z.number().positive().default(10),
      windowSeconds: z.number().positive().default(60),
    })
    .default({ enabled: true, maxPerWindow: 10, windowSeconds: 60 }),
  deduplication: z
    .object({
      enabled: z.boolean().default(true),
      ttlSeconds: z.number().positive().default(300),
    })
    .default({ enabled: true, ttlSeconds: 300 }),
  quietHours: z
    .object({
      enabled: z.boolean().default(false),
      startHour: z.number().min(0).max(23).default(22),
      endHour: z.number().min(0).max(23).default(7),
      defaultTimezone: z.string().default('UTC'),
      allowUrgentOverride: z.boolean().default(true),
    })
    .default({
      enabled: false,
      startHour: 22,
      endHour: 7,
      defaultTimezone: 'UTC',
      allowUrgentOverride: true,
    }),
  deadLetter: z
    .object({
      enabled: z.boolean().default(true),
      maxRetries: z.number().min(0).default(3),
      maxStored: z.number().min(1).default(1000),
    })
    .default({ enabled: true, maxRetries: 3, maxStored: 1000 }),
  digest: z
    .object({
      enabled: z.boolean().default(false),
      windowSeconds: z.number().positive().default(120),
    })
    .default({ enabled: false, windowSeconds: 120 }),
});
