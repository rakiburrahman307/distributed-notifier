import { NotificationChannel, NotificationPayload, ChannelDeliveryResult } from './notification.js';

export interface MulticastResult {
  successCount: number;
  failureCount: number;
  messageIds: string[];
  failedTokens: string[];
}

export interface IProvider {
  /** Unique provider identifier (e.g. 'fcm-main', 'twilio-sms', 'resend-email') */
  readonly name: string;
  /** Primary channel handled by this provider */
  readonly channel: NotificationChannel;

  /** Execute single notification delivery */
  send(payload: NotificationPayload): Promise<ChannelDeliveryResult>;

  /**
   * Vectorized multicast delivery to multiple device tokens in a single request.
   * Significantly reduces network overhead when broadcasting to thousands of users.
   */
  sendMulticast?(tokens: string[], payload: NotificationPayload): Promise<MulticastResult>;

  /** Optional health probe */
  isHealthy?(): Promise<boolean>;
}
