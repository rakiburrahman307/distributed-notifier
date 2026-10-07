export type CircuitState = 'CLOSED' | 'OPEN' | 'HALF_OPEN';

export interface CircuitBreakerOptions {
  failureThreshold?: number; // Number of failures before opening circuit (default: 5)
  cooldownPeriodMs?: number; // Time to wait in OPEN state before testing HALF_OPEN (default: 30000ms)
  timeoutMs?: number; // Max execution timeout for call (default: 8000ms)
  halfOpenSuccessThreshold?: number; // Number of successes in HALF_OPEN to close (default: 1)
  onStateChange?: (from: CircuitState, to: CircuitState) => void;
}

export class CircuitBreaker {
  private state: CircuitState = 'CLOSED';
  private failureCount = 0;
  private halfOpenSuccessCount = 0;
  private lastFailureTime = 0;
  private activeHalfOpenProbes = 0;
  private isDestroyed = false;

  private readonly failureThreshold: number;
  private readonly cooldownPeriodMs: number;
  private readonly timeoutMs: number;
  private readonly halfOpenSuccessThreshold: number;
  private readonly onStateChange?: (from: CircuitState, to: CircuitState) => void;

  constructor(options: CircuitBreakerOptions = {}) {
    this.failureThreshold = Math.max(1, options.failureThreshold ?? 5);
    this.cooldownPeriodMs = Math.max(10, options.cooldownPeriodMs ?? 30000);
    this.timeoutMs = Math.max(10, options.timeoutMs ?? 8000);
    this.halfOpenSuccessThreshold = Math.max(1, options.halfOpenSuccessThreshold ?? 1);
    this.onStateChange = options.onStateChange;
  }

  public getState(): CircuitState {
    if (this.state === 'OPEN') {
      const now = Date.now();
      if (now - this.lastFailureTime > this.cooldownPeriodMs) {
        this.transitionTo('HALF_OPEN');
      }
    }
    return this.state;
  }

  public async execute<T>(action: () => Promise<T>): Promise<T> {
    if (this.isDestroyed) {
      throw new Error('CircuitBreaker is disposed and cannot execute operations');
    }

    const currentState = this.getState();

    if (currentState === 'OPEN') {
      throw new Error('Circuit breaker is OPEN. Fast failing request to protect downstream provider.');
    }

    if (currentState === 'HALF_OPEN') {
      // Allow only a limited number of test probes
      if (this.activeHalfOpenProbes >= 1) {
        throw new Error('Circuit breaker is HALF_OPEN. Probe in progress, fast failing additional requests.');
      }
      this.activeHalfOpenProbes++;
    }

    let timer: NodeJS.Timeout | undefined;
    let isSettled = false;

    try {
      const timeoutPromise = new Promise<never>((_, reject) => {
        timer = setTimeout(() => {
          if (!isSettled) {
            reject(new Error(`Provider call timed out after ${this.timeoutMs}ms`));
          }
        }, this.timeoutMs);
        if (timer.unref) {
          timer.unref();
        }
      });

      const result = await Promise.race([action(), timeoutPromise]);
      isSettled = true;

      this.onSuccess();
      return result;
    } catch (err) {
      isSettled = true;
      this.onFailure();
      throw err;
    } finally {
      if (timer) {
        clearTimeout(timer);
        timer = undefined;
      }
      if (currentState === 'HALF_OPEN') {
        this.activeHalfOpenProbes = Math.max(0, this.activeHalfOpenProbes - 1);
      }
    }
  }

  private onSuccess(): void {
    if (this.state === 'HALF_OPEN') {
      this.halfOpenSuccessCount++;
      if (this.halfOpenSuccessCount >= this.halfOpenSuccessThreshold) {
        this.reset();
      }
    } else {
      this.failureCount = 0;
    }
  }

  private onFailure(): void {
    this.failureCount++;
    this.lastFailureTime = Date.now();

    if (this.state === 'HALF_OPEN' || this.failureCount >= this.failureThreshold) {
      this.transitionTo('OPEN');
    }
  }

  private transitionTo(newState: CircuitState): void {
    if (this.state !== newState) {
      const oldState = this.state;
      this.state = newState;
      if (newState === 'OPEN') {
        this.halfOpenSuccessCount = 0;
      } else if (newState === 'HALF_OPEN') {
        this.halfOpenSuccessCount = 0;
      }
      if (this.onStateChange) {
        try {
          this.onStateChange(oldState, newState);
        } catch {
          // Prevent listener errors from corrupting state
        }
      }
    }
  }

  public reset(): void {
    const oldState = this.state;
    this.state = 'CLOSED';
    this.failureCount = 0;
    this.halfOpenSuccessCount = 0;
    this.activeHalfOpenProbes = 0;
    this.lastFailureTime = 0;

    if (oldState !== 'CLOSED' && this.onStateChange) {
      try {
        this.onStateChange(oldState, 'CLOSED');
      } catch {
        // Safe guard
      }
    }
  }

  public destroy(): void {
    this.isDestroyed = true;
    this.reset();
  }
}
