export type NotificationChannel = 'push' | 'sms' | 'email' | 'in_app' | 'webhook';

export type NotificationPriority = 'HIGH' | 'NORMAL' | 'LOW';

export type NotificationStatus =
  | 'QUEUED'
  | 'SENT'
  | 'FAILED'
  | 'PARTIALLY_DELIVERED'
  | 'SKIPPED_DUPLICATE'
  | 'RATE_LIMITED'
  | 'SCHEDULED_QUIET_HOURS'
  | 'UNSUBSCRIBED';

export interface RecipientPreferences {
  blockedChannels?: NotificationChannel[];
  quietHoursEnabled?: boolean;
  topics?: Record<string, boolean>; // e.g. { marketing: false, security: true, transactional: true }
  categories?: Record<string, boolean>; // alias for topics
}

export interface Recipient {
  id: string;
  phone?: string;
  email?: string;
  pushTokens?: string[];
  timezone?: string; // e.g. "Asia/Dhaka", "UTC"
  locale?: string;   // e.g. "en", "bn"
  preferences?: RecipientPreferences;
}

export interface NotificationPayload {
  id: string;
  recipient: Recipient;
  channels: NotificationChannel[];
  priority?: NotificationPriority;
  topic?: string;       // e.g. "marketing", "transactional", "security"
  category?: string;    // alias for topic
  templateId?: string;
  locale?: string;      // overrides recipient.locale
  title?: string;
  body?: string;
  data?: Record<string, unknown>;
  variables?: Record<string, unknown>;
  idempotencyKey?: string;
  fallbackChannels?: NotificationChannel[];
  scheduledAt?: Date;
  metadata?: Record<string, unknown>;
}

export interface ChannelDeliveryResult {
  success: boolean;
  channel: NotificationChannel;
  provider: string;
  messageId?: string;
  error?: string;
  timestamp: number;
}

export interface NotificationResult {
  notificationId: string;
  status: NotificationStatus;
  channelsAttempted: NotificationChannel[];
  channelResults: ChannelDeliveryResult[];
  deliveredAt?: Date;
  isFallbackUsed: boolean;
  suppressedUntil?: Date;
  idempotencyKey?: string;
  jobId?: string;
}

export interface DispatchResult {
  jobId: string;
  notificationId: string;
  status: NotificationStatus;
  enqueuedAt: Date;
  scheduledFor?: Date;
  idempotencyKey?: string;
}
