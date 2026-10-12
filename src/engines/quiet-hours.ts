import { NotificationPriority } from '../types/notification.js';

export interface QuietHoursOptions {
  enabled: boolean;
  startHour: number; // 0 - 23 (e.g. 22 for 10 PM)
  endHour: number; // 0 - 23 (e.g. 7 for 7 AM)
  defaultTimezone?: string;
  allowUrgentOverride?: boolean;
}

export interface QuietHoursCheckResult {
  isQuietTime: boolean;
  resumeAt?: Date;
}

export class QuietHoursEngine {
  private readonly options: QuietHoursOptions;

  constructor(options: Partial<QuietHoursOptions> = {}) {
    this.options = {
      enabled: options.enabled ?? false,
      startHour: options.startHour ?? 22,
      endHour: options.endHour ?? 7,
      defaultTimezone: options.defaultTimezone ?? 'UTC',
      allowUrgentOverride: options.allowUrgentOverride ?? true,
    };
  }

  public shouldSuppress(
    priority: NotificationPriority = 'NORMAL',
    recipientTimezone?: string,
    recipientQuietHoursEnabled = true
  ): QuietHoursCheckResult {
    if (!this.options.enabled || !recipientQuietHoursEnabled) {
      return { isQuietTime: false };
    }

    // High priority OTP or critical alerts bypass quiet hours
    if (priority === 'HIGH' && this.options.allowUrgentOverride) {
      return { isQuietTime: false };
    }

    const tz = recipientTimezone || this.options.defaultTimezone || 'UTC';
    const now = new Date();

    const localHour = this.getLocalHour(now, tz);
    const isQuiet = this.isHourInQuietWindow(localHour, this.options.startHour, this.options.endHour);

    if (!isQuiet) {
      return { isQuietTime: false };
    }

    // Calculate resume timestamp (when endHour arrives in recipient's timezone)
    const resumeAt = this.calculateNextResumeTime(now, tz, this.options.endHour);
    return {
      isQuietTime: true,
      resumeAt,
    };
  }

  private isHourInQuietWindow(currentHour: number, startHour: number, endHour: number): boolean {
    if (startHour === endHour) return false;

    // Normal window: e.g. 1 PM (13) to 5 PM (17)
    if (startHour < endHour) {
      return currentHour >= startHour && currentHour < endHour;
    }

    // Overnight window: e.g. 10 PM (22) to 7 AM (7)
    return currentHour >= startHour || currentHour < endHour;
  }

  private getLocalHour(date: Date, timeZone: string): number {
    try {
      const formatter = new Intl.DateTimeFormat('en-US', {
        timeZone,
        hour: 'numeric',
        hourCycle: 'h23',
      });
      return parseInt(formatter.format(date), 10);
    } catch {
      return date.getUTCHours();
    }
  }

  private calculateNextResumeTime(now: Date, timeZone: string, endHour: number): Date {
    // Add 1 hour iteratively until local hour matches endHour
    for (let i = 1; i <= 24; i++) {
      const candidate = new Date(now.getTime() + i * 3600 * 1000);
      if (this.getLocalHour(candidate, timeZone) === endHour) {
        candidate.setMinutes(0, 0, 0);
        return candidate;
      }
    }
    return new Date(now.getTime() + 8 * 3600 * 1000); // Default 8 hours later fallback
  }
}
