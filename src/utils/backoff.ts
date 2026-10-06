/**
 * AWS Standard Full Jitter Exponential Backoff Algorithm.
 * Prevents the "Thundering Herd Problem" by evenly distributing retry spikes.
 *
 * @param attempt - The zero-indexed retry attempt count (0, 1, 2...)
 * @param baseMs - The base backoff interval in milliseconds (default: 1000ms)
 * @param capMs - The maximum ceiling backoff in milliseconds (default: 30000ms)
 * @returns Jittered backoff delay in milliseconds
 */
export function calculateFullJitterBackoff(attempt: number, baseMs = 1000, capMs = 30000): number {
  const exponentialCeiling = Math.min(capMs, baseMs * Math.pow(2, attempt));
  // Uniform random distribution between 0 and exponentialCeiling
  return Math.floor(Math.random() * exponentialCeiling);
}

/**
 * Equal Jitter variation: guarantees a minimum floor sleep time while still decorrelating peaks.
 */
export function calculateEqualJitterBackoff(attempt: number, baseMs = 1000, capMs = 30000): number {
  const exponentialCeiling = Math.min(capMs, baseMs * Math.pow(2, attempt));
  const half = exponentialCeiling / 2;
  return Math.floor(half + Math.random() * half);
}
