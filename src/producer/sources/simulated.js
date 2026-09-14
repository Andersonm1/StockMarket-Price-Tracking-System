const round2 = (n) => Math.round(n * 100) / 100;

/** Standard normal sample via Box-Muller. */
function gaussian(random) {
  let u = 0;
  while (u === 0) u = random();
  const v = random();
  return Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * v);
}

/**
 * Generates realistic-looking ticks with a geometric random walk
 * plus rare "news" jumps. Deterministic when given a seeded `random`.
 */
export class PriceSimulator {
  constructor(companies, { random = Math.random, now = () => Date.now() } = {}) {
    this.random = random;
    this.now = now;
    this.state = new Map(
      companies.map((c) => [c.symbol, {
        company: c,
        price: c.basePrice,
        open: c.basePrice,
        high: c.basePrice,
        low: c.basePrice,
        volume: 0,
      }]),
    );
  }

  tick() {
    const timestamp = this.now();
    return [...this.state.values()].map((s) => {
      const sigma = s.company.volatility;
      let shock = sigma * gaussian(this.random);
      if (this.random() < 0.002) shock += (this.random() < 0.5 ? -1 : 1) * sigma * 12; // occasional jump
      // Mild mean reversion toward the open keeps long-running demos in a sensible range.
      const drift = 0.002 * Math.log(s.open / s.price);
      s.price = Math.max(0.01, s.price * Math.exp(drift + shock));
      s.high = Math.max(s.high, s.price);
      s.low = Math.min(s.low, s.price);
      const tradeVolume = Math.floor(100 + this.random() * 5000);
      s.volume += tradeVolume;

      const price = round2(s.price);
      return {
        symbol: s.company.symbol,
        name: s.company.name,
        sector: s.company.sector,
        price,
        open: round2(s.open),
        high: round2(s.high),
        low: round2(s.low),
        change: round2(price - s.open),
        changePercent: round2(((price - s.open) / s.open) * 100),
        volume: s.volume,
        lastTradeVolume: tradeVolume,
        timestamp,
        source: 'simulated',
      };
    });
  }
}

export class SimulatedSource {
  constructor(companies, { intervalMs, logger }) {
    this.simulator = new PriceSimulator(companies);
    this.intervalMs = intervalMs;
    this.logger = logger;
    this.timer = null;
    this.running = false;
  }

  start(onTicks) {
    this.running = true;
    this.logger.info('simulated source started', { symbols: [...this.simulator.state.keys()], intervalMs: this.intervalMs });
    const loop = async () => {
      if (!this.running) return;
      const started = Date.now();
      try {
        await onTicks(this.simulator.tick());
      } catch (err) {
        this.logger.error('failed to publish ticks', err);
      }
      this.timer = setTimeout(loop, Math.max(0, this.intervalMs - (Date.now() - started)));
    };
    loop();
  }

  async stop() {
    this.running = false;
    clearTimeout(this.timer);
  }
}
