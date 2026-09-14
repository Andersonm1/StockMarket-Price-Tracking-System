import { randomUUID } from 'node:crypto';

/**
 * Detects threshold crossings. A rule fires when the price moves from one side of the
 * threshold to the other (not merely while it stays beyond it), so it re-arms naturally
 * once the price comes back. `cooldownMs` suppresses repeat alerts for the same rule.
 */
export class AlertEvaluator {
  constructor({ cooldownMs = 0, now = () => Date.now() } = {}) {
    this.cooldownMs = cooldownMs;
    this.now = now;
    this.rulesBySymbol = new Map(); // symbol -> Map(ruleId -> rule)
    this.ruleIndex = new Map(); // ruleId -> rule
    this.lastPrice = new Map(); // symbol -> price
    this.lastFired = new Map(); // ruleId -> timestamp
  }

  get ruleCount() {
    return this.ruleIndex.size;
  }

  upsertRule(rule) {
    this.removeRule(rule.id);
    this.ruleIndex.set(rule.id, rule);
    if (!this.rulesBySymbol.has(rule.symbol)) this.rulesBySymbol.set(rule.symbol, new Map());
    this.rulesBySymbol.get(rule.symbol).set(rule.id, rule);
  }

  removeRule(id) {
    const existing = this.ruleIndex.get(id);
    if (!existing) return false;
    this.ruleIndex.delete(id);
    this.lastFired.delete(id);
    const bucket = this.rulesBySymbol.get(existing.symbol);
    bucket?.delete(id);
    if (bucket?.size === 0) this.rulesBySymbol.delete(existing.symbol);
    return true;
  }

  /** Records a price without triggering anything (used for stale ticks). */
  observe(tick) {
    this.lastPrice.set(tick.symbol, tick.price);
  }

  /** @returns {object[]} alert events triggered by this tick */
  evaluate(tick) {
    const previous = this.lastPrice.get(tick.symbol);
    this.lastPrice.set(tick.symbol, tick.price);
    if (previous === undefined) return [];

    const rules = this.rulesBySymbol.get(tick.symbol);
    if (!rules) return [];

    const now = this.now();
    const alerts = [];
    for (const rule of rules.values()) {
      if (!crossed(rule, previous, tick.price)) continue;

      const last = this.lastFired.get(rule.id);
      if (last !== undefined && now - last < this.cooldownMs) continue;
      this.lastFired.set(rule.id, now);

      alerts.push({
        id: randomUUID(),
        ruleId: rule.id,
        symbol: tick.symbol,
        name: tick.name || tick.symbol,
        direction: rule.direction,
        threshold: rule.threshold,
        price: tick.price,
        previousPrice: previous,
        note: rule.note || '',
        tickTimestamp: tick.timestamp,
        triggeredAt: now,
        message: `${tick.symbol} crossed ${rule.direction} ${formatPrice(rule.threshold)} (now ${formatPrice(tick.price)})`,
      });
    }
    return alerts;
  }
}

export function crossed(rule, previous, current) {
  if (rule.direction === 'above') return previous < rule.threshold && current >= rule.threshold;
  if (rule.direction === 'below') return previous > rule.threshold && current <= rule.threshold;
  return false;
}

function formatPrice(n) {
  return `$${n.toFixed(2)}`;
}
