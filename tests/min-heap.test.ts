import { describe, it } from 'node:test';
import assert from 'node:assert';
import { MinHeap } from '../src/utils/min-heap.js';

describe('Binary Min-Heap Priority Queue', () => {
  it('should always extract items in ascending priority order (lowest score first)', () => {
    const heap = new MinHeap<string>();

    heap.insert('Normal Task 1', 5);
    heap.insert('Critical OTP', 1);
    heap.insert('Background Promo', 10);
    heap.insert('Urgent Alert', 2);
    heap.insert('Normal Task 2', 5);

    assert.strictEqual(heap.pop(), 'Critical OTP');     // Priority 1
    assert.strictEqual(heap.pop(), 'Urgent Alert');      // Priority 2
    assert.strictEqual(heap.pop(), 'Normal Task 1');     // Priority 5
    assert.strictEqual(heap.pop(), 'Normal Task 2');     // Priority 5
    assert.strictEqual(heap.pop(), 'Background Promo');  // Priority 10
    assert.strictEqual(heap.isEmpty(), true);
  });

  it('should return undefined when popping an empty heap', () => {
    const heap = new MinHeap<number>();
    assert.strictEqual(heap.pop(), undefined);
    assert.strictEqual(heap.size(), 0);
  });
});
