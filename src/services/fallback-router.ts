import { IProvider } from '../types/provider.js';
import { NotificationChannel, NotificationPayload, ChannelDeliveryResult } from '../types/notification.js';

export class FallbackRouter {
  private readonly providers = new Map<NotificationChannel, IProvider[]>();

  public registerProvider(provider: IProvider): void {
    const list = this.providers.get(provider.channel) || [];
    list.push(provider);
    this.providers.set(provider.channel, list);
  }

  public getProvidersForChannel(channel: NotificationChannel): IProvider[] {
    return this.providers.get(channel) || [];
  }

  public async deliverWithFallback(
    payload: NotificationPayload,
    targetChannels: NotificationChannel[],
    fallbackChannels: NotificationChannel[] = []
  ): Promise<{ results: ChannelDeliveryResult[]; isFallbackUsed: boolean }> {
    const results: ChannelDeliveryResult[] = [];
    let isFallbackUsed = false;
    let anyPrimaryDelivered = false;

    // 1. Deliver to primary channels
    for (const channel of targetChannels) {
      const channelResult = await this.deliverToChannel(channel, payload);
      results.push(channelResult);
      if (channelResult.success) {
        anyPrimaryDelivered = true;
      }
    }

    // 2. If all primary attempts failed and fallback channels exist, execute fallback
    if (!anyPrimaryDelivered && fallbackChannels.length > 0) {
      isFallbackUsed = true;
      for (const fallbackChannel of fallbackChannels) {
        if (!targetChannels.includes(fallbackChannel)) {
          const fallbackResult = await this.deliverToChannel(fallbackChannel, payload);
          results.push(fallbackResult);
          if (fallbackResult.success) {
            break; // Stop at first successful fallback channel
          }
        }
      }
    }

    return { results, isFallbackUsed };
  }

  private async deliverToChannel(channel: NotificationChannel, payload: NotificationPayload): Promise<ChannelDeliveryResult> {
    const providers = this.providers.get(channel);

    if (!providers || providers.length === 0) {
      return {
        success: false,
        channel,
        provider: 'none',
        error: `No provider registered for channel: "${channel}"`,
        timestamp: Date.now(),
      };
    }

    // Attempt through available providers for this channel
    let lastError = 'All providers failed';
    for (const provider of providers) {
      try {
        const res = await provider.send(payload);
        if (res.success) {
          return res;
        }
        lastError = res.error || 'Provider returned unsuccessful result';
      } catch (err) {
        lastError = err instanceof Error ? err.message : String(err);
      }
    }

    return {
      success: false,
      channel,
      provider: providers[0].name,
      error: lastError,
      timestamp: Date.now(),
    };
  }
}
