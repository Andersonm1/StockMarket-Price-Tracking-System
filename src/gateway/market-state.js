import { ALL } from './subscriptions.js';

/** In-memory view of the streams, used to give newly connected clients an instant snapshot. */
export class MarketState {
  constructor({ companies, historySize = 120, recentAlertsSize = 50 }) {
    this.companies = new Map(companies.map((c) => [c.symbol, { symbol: c.symbol, name: c.name, sector: c.sector }]));
    this.historySize = historySize;
    this.recentAlertsSize = recentAlertsSize;
    this.latest = new Map(); // symbol -> tick
    this.history = new Map(); // symbol -> [[timestamp, price], ...]
    this.rules = new Map(); // id -> rule
    this.alerts = []; // newest first
  }

  /** @returns {boolean} true if this tick introduced a company we had not seen before */
  applyTick(tick) {
    this.latest.set(tick.symbol, tick);
    let points = this.history.get(tick.symbol);
    if (!points) {
      points = [];
      this.history.set(tick.symbol, points);
    }
    points.push([tick.timestamp, tick.price]);
    if (points.length > this.historySize) points.splice(0, points.length - this.historySize);

    if (this.companies.has(tick.symbol)) return false;
    this.companies.set(tick.symbol, { symbol: tick.symbol, name: tick.name || tick.symbol, sector: tick.sector || 'Unknown' });
    return true;
  }

  applyAlert(alert) {
    this.alerts.unshift(alert);
    if (this.alerts.length > this.recentAlertsSize) this.alerts.length = this.recentAlertsSize;
  }

  listCompanies() {
    return [...this.companies.values()].sort((a, b) => a.symbol.localeCompare(b.symbol));
  }

  listRules() {
    return [...this.rules.values()].sort((a, b) => a.symbol.localeCompare(b.symbol) || a.threshold - b.threshold);
  }

  snapshot(symbols) {
    const wanted = !symbols || symbols.includes(ALL) ? [...this.latest.keys()] : symbols;
    const prices = [];
    const history = {};
    for (const symbol of wanted) {
      const tick = this.latest.get(symbol);
      if (!tick) continue;
      prices.push(tick);
      history[symbol] = this.history.get(symbol) ?? [];
    }
    return { prices, history };
  }
}
