import { IProvider } from '../types/provider.js';
import { NotificationChannel, NotificationPayload, ChannelDeliveryResult } from '../types/notification.js';
import { CircuitBreaker, CircuitBreakerOptions } from '../utils/circuit-breaker.js';
import { ProviderTrafficShaper, TrafficShaperOptions } from '../engines/traffic-shaper.js';

export interface BaseProviderOptions {
  circuitBreakerOptions?: CircuitBreakerOptions;
  trafficShaperOptions?: TrafficShaperOptions;
}

export abstract class BaseProvider implements IProvider {
  public abstract readonly name: string;
  public abstract readonly channel: NotificationChannel;
  protected readonly circuitBreaker: CircuitBreaker;
  protected readonly trafficShaper?: ProviderTrafficShaper;

  constructor(options?: CircuitBreakerOptions | BaseProviderOptions) {
    // Backward compatibility: options could be CircuitBreakerOptions or BaseProviderOptions
    let cbOptions: CircuitBreakerOptions | undefined;
    let tsOptions: TrafficShaperOptions | undefined;

    if (options) {
      if ('circuitBreakerOptions' in options || 'trafficShaperOptions' in options) {
        const fullOptions = options as BaseProviderOptions;
        cbOptions = fullOptions.circuitBreakerOptions;
        tsOptions = fullOptions.trafficShaperOptions;
      } else {
        cbOptions = options as CircuitBreakerOptions;
      }
    }

    this.circuitBreaker = new CircuitBreaker(cbOptions);
    if (tsOptions) {
      this.trafficShaper = new ProviderTrafficShaper(tsOptions);
    }
  }

  public async send(payload: NotificationPayload): Promise<ChannelDeliveryResult> {
    try {
      const executeDelivery = async () => {
        return await this.circuitBreaker.execute(async () => {
          return await this.deliver(payload);
        });
      };

      if (this.trafficShaper) {
        return await this.trafficShaper.schedule(executeDelivery);
      }

      return await executeDelivery();
    } catch (err) {
      const errorMessage = err instanceof Error ? err.message : String(err);
      return {
        success: false,
        channel: this.channel,
        provider: this.name,
        error: errorMessage,
        timestamp: Date.now(),
      };
    }
  }

  /**
   * Concrete channel delivery logic implemented by subclasses
   */
  protected abstract deliver(payload: NotificationPayload): Promise<ChannelDeliveryResult>;

  public async isHealthy(): Promise<boolean> {
    return this.circuitBreaker.getState() !== 'OPEN';
  }

  public async close(): Promise<void> {
    this.circuitBreaker.destroy();
    if (this.trafficShaper) {
      this.trafficShaper.destroy();
    }
  }
}
