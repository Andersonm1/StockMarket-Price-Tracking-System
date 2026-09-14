import { randomUUID } from 'node:crypto';

export const DIRECTIONS = ['above', 'below'];
const SYMBOL_PATTERN = /^[A-Z][A-Z0-9.\-]{0,14}$/;

/**
 * Validates user input for an alert rule.
 * @returns {{ rule: object } | { error: string }}
 */
export function buildRule(input, { now = Date.now() } = {}) {
  if (!input || typeof input !== 'object') return { error: 'rule must be an object' };

  const symbol = typeof input.symbol === 'string' ? input.symbol.trim().toUpperCase() : '';
  if (!SYMBOL_PATTERN.test(symbol)) return { error: 'symbol must be a ticker such as AAPL' };

  if (!DIRECTIONS.includes(input.direction)) return { error: `direction must be one of: ${DIRECTIONS.join(', ')}` };

  const threshold = typeof input.threshold === 'string' ? Number(input.threshold) : input.threshold;
  if (typeof threshold !== 'number' || !Number.isFinite(threshold) || threshold <= 0) {
    return { error: 'threshold must be a positive number' };
  }

  const note = typeof input.note === 'string' ? input.note.trim().slice(0, 140) : '';

  return {
    rule: {
      id: randomUUID(),
      symbol,
      direction: input.direction,
      threshold: Math.round(threshold * 10000) / 10000,
      note,
      createdAt: now,
    },
  };
}
