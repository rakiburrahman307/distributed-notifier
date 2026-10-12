import { describe, it } from 'node:test';
import assert from 'node:assert';
import { QuietHoursEngine } from '../src/engines/quiet-hours.js';

describe('Quiet Hours Engine', () => {
  it('should allow urgent HIGH priority notification to bypass quiet hours', () => {
    // 0 to 24 quiet window (always active)
    const engine = new QuietHoursEngine({
      enabled: true,
      startHour: 0,
      endHour: 23,
      allowUrgentOverride: true,
    });

    const highPriorityCheck = engine.shouldSuppress('HIGH', 'UTC', true);
    assert.strictEqual(highPriorityCheck.isQuietTime, false);
  });

  it('should not suppress when quiet hours are disabled', () => {
    const engine = new QuietHoursEngine({ enabled: false });
    const check = engine.shouldSuppress('NORMAL', 'UTC', true);
    assert.strictEqual(check.isQuietTime, false);
  });
});
