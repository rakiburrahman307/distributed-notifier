import { describe, it } from 'node:test';
import assert from 'node:assert';
import { CircuitBreaker } from '../src/utils/circuit-breaker.js';

describe('Circuit Breaker Resilience & Resource Lifecycle', () => {
  it('should execute fast actions and clear timeout timer without leaks', async () => {
    const cb = new CircuitBreaker({ timeoutMs: 5000 });
    const res = await cb.execute(async () => 'fast_result');
    assert.strictEqual(res, 'fast_result');
    assert.strictEqual(cb.getState(), 'CLOSED');
    cb.destroy();
  });

  it('should timeout slow operations and throw timeout error', async () => {
    const cb = new CircuitBreaker({ timeoutMs: 50 });
    await assert.rejects(
      async () => {
        await cb.execute(async () => {
          await new Promise((resolve) => setTimeout(resolve, 200));
          return 'slow_result';
        });
      },
      { message: /timed out after 50ms/ }
    );
    cb.destroy();
  });

  it('should trip to OPEN after reaching failureThreshold', async () => {
    let stateChanges: string[] = [];
    const cb = new CircuitBreaker({
      failureThreshold: 2,
      cooldownPeriodMs: 100,
      onStateChange: (from, to) => stateChanges.push(`${from}->${to}`),
    });

    // 1st failure
    await assert.rejects(async () => cb.execute(async () => { throw new Error('fail 1'); }));
    assert.strictEqual(cb.getState(), 'CLOSED');

    // 2nd failure -> trips OPEN
    await assert.rejects(async () => cb.execute(async () => { throw new Error('fail 2'); }));
    assert.strictEqual(cb.getState(), 'OPEN');
    assert.ok(stateChanges.includes('CLOSED->OPEN'));

    // Fast fails while OPEN
    await assert.rejects(
      async () => cb.execute(async () => 'should not run'),
      { message: /Circuit breaker is OPEN/ }
    );

    cb.destroy();
  });

  it('should transition to HALF_OPEN after cooldown and recover on success', async () => {
    const cb = new CircuitBreaker({
      failureThreshold: 1,
      cooldownPeriodMs: 50,
      halfOpenSuccessThreshold: 1,
    });

    await assert.rejects(async () => cb.execute(async () => { throw new Error('fail'); }));
    assert.strictEqual(cb.getState(), 'OPEN');

    // Wait for cooldown
    await new Promise((resolve) => setTimeout(resolve, 60));
    assert.strictEqual(cb.getState(), 'HALF_OPEN');

    // Probe success recovers breaker to CLOSED
    const res = await cb.execute(async () => 'recovered');
    assert.strictEqual(res, 'recovered');
    assert.strictEqual(cb.getState(), 'CLOSED');

    cb.destroy();
  });

  it('should guard against concurrent probes in HALF_OPEN state', async () => {
    const cb = new CircuitBreaker({
      failureThreshold: 1,
      cooldownPeriodMs: 40,
    });

    await assert.rejects(async () => cb.execute(async () => { throw new Error('err'); }));
    await new Promise((resolve) => setTimeout(resolve, 50));
    assert.strictEqual(cb.getState(), 'HALF_OPEN');

    // Start a probe that takes 50ms
    const probe1 = cb.execute(async () => {
      await new Promise((r) => setTimeout(r, 50));
      return 'probe1_success';
    });

    // Attempt second probe immediately while probe1 is running
    await assert.rejects(
      async () => cb.execute(async () => 'probe2'),
      { message: /Probe in progress/ }
    );

    const result1 = await probe1;
    assert.strictEqual(result1, 'probe1_success');
    assert.strictEqual(cb.getState(), 'CLOSED');

    cb.destroy();
  });
});
