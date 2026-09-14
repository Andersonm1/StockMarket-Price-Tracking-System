import WebSocket from 'ws';

const round2 = (n) => Math.round(n * 100) / 100;

/**
 * Streams real trades from Finnhub's WebSocket API (https://finnhub.io/docs/api/websocket-trades).
 * Trades are batched and flushed every `intervalMs` so Kafka receives at most one tick
 * per symbol per interval. Note: US equities only trade during market hours.
 */
export class FinnhubSource {
  constructor(companies, { apiKey, intervalMs, logger }) {
    if (!apiKey) throw new Error('DATA_SOURCE=finnhub requires FINNHUB_API_KEY');
    this.companies = new Map(companies.map((c) => [c.symbol, c]));
    this.apiKey = apiKey;
    this.intervalMs = intervalMs;
    this.logger = logger;
    this.session = new Map(); // symbol -> { open, high, low, volume }
    this.latest = new Map(); // symbol -> tick waiting to be flushed
    this.ws = null;
    this.flushTimer = null;
    this.reconnectDelay = 1000;
    this.stopped = false;
  }

  start(onTicks) {
    this.onTicks = onTicks;
    this.connect();
    this.flushTimer = setInterval(() => this.flush(), this.intervalMs);
  }

  connect() {
    const ws = new WebSocket(`wss://ws.finnhub.io?token=${encodeURIComponent(this.apiKey)}`);
    this.ws = ws;

    ws.on('open', () => {
      this.reconnectDelay = 1000;
      for (const symbol of this.companies.keys()) ws.send(JSON.stringify({ type: 'subscribe', symbol }));
      this.logger.info('connected to finnhub', { symbols: [...this.companies.keys()] });
    });

    ws.on('message', (raw) => {
      let msg;
      try {
        msg = JSON.parse(raw.toString());
      } catch {
        return;
      }
      if (msg.type === 'trade' && Array.isArray(msg.data)) {
        for (const trade of msg.data) this.record(trade);
      } else if (msg.type === 'error') {
        this.logger.warn('finnhub error', { detail: msg.msg });
      }
    });

    ws.on('error', (err) => this.logger.warn('finnhub socket error', { error: err.message }));

    ws.on('close', () => {
      if (this.stopped) return;
      this.logger.warn('finnhub connection closed, reconnecting', { inMs: this.reconnectDelay });
      setTimeout(() => this.connect(), this.reconnectDelay);
      this.reconnectDelay = Math.min(this.reconnectDelay * 2, 30000);
    });
  }

  record(trade) {
    const symbol = trade.s;
    const company = this.companies.get(symbol);
    if (!company || typeof trade.p !== 'number') return;

    let s = this.session.get(symbol);
    if (!s) {
      s = { open: trade.p, high: trade.p, low: trade.p, volume: 0 };
      this.session.set(symbol, s);
    }
    s.high = Math.max(s.high, trade.p);
    s.low = Math.min(s.low, trade.p);
    s.volume += trade.v || 0;

    const price = round2(trade.p);
    this.latest.set(symbol, {
      symbol,
      name: company.name,
      sector: company.sector,
      price,
      open: round2(s.open),
      high: round2(s.high),
      low: round2(s.low),
      change: round2(price - s.open),
      changePercent: round2(((price - s.open) / s.open) * 100),
      volume: s.volume,
      lastTradeVolume: trade.v || 0,
      timestamp: trade.t || Date.now(),
      source: 'finnhub',
    });
  }

  async flush() {
    if (this.latest.size === 0) return;
    const ticks = [...this.latest.values()];
    this.latest.clear();
    try {
      await this.onTicks(ticks);
    } catch (err) {
      this.logger.error('failed to publish ticks', err);
    }
  }

  async stop() {
    this.stopped = true;
    clearInterval(this.flushTimer);
    this.ws?.close();
  }
}
