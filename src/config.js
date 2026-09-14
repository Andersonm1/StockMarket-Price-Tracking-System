import os from 'node:os';

function str(name, fallback) {
  const value = process.env[name];
  return value === undefined || value === '' ? fallback : value;
}

function int(name, fallback) {
  const raw = process.env[name];
  if (raw === undefined || raw === '') return fallback;
  const value = Number.parseInt(raw, 10);
  if (Number.isNaN(value)) throw new Error(`Environment variable ${name} must be an integer, got "${raw}"`);
  return value;
}

function list(name, fallback) {
  const raw = process.env[name];
  if (!raw) return fallback;
  return raw.split(',').map((s) => s.trim()).filter(Boolean);
}

export const config = {
  instanceId: str('INSTANCE_ID', os.hostname()),
  logLevel: str('LOG_LEVEL', 'info'),

  kafka: {
    brokers: list('KAFKA_BROKERS', ['localhost:29092']),
    partitions: int('KAFKA_PARTITIONS', 3),
    replicationFactor: int('KAFKA_REPLICATION_FACTOR', 1),
  },

  topics: {
    prices: str('TOPIC_PRICES', 'stock-prices'),
    rules: str('TOPIC_ALERT_RULES', 'alert-rules'),
    alerts: str('TOPIC_ALERTS', 'stock-alerts'),
  },

  producer: {
    // "simulated" (default, no API key needed) or "finnhub" (real trades, needs FINNHUB_API_KEY)
    source: str('DATA_SOURCE', 'simulated'),
    symbols: list('SYMBOLS', null),
    tickIntervalMs: int('TICK_INTERVAL_MS', 1000),
    finnhubApiKey: str('FINNHUB_API_KEY', ''),
  },

  alerts: {
    // Minimum time between two notifications for the same rule, so noisy prices
    // hovering around a threshold don't spam alerts.
    cooldownMs: int('ALERT_COOLDOWN_MS', 30000),
    // Ticks older than this are used to track price but never trigger alerts
    // (e.g. when the engine catches up on a backlog after downtime).
    maxTickAgeMs: int('ALERT_MAX_TICK_AGE_MS', 60000),
    // Optional Slack/Discord/generic webhook that receives every triggered alert.
    webhookUrl: str('ALERT_WEBHOOK_URL', ''),
  },

  gateway: {
    port: int('PORT', 8080),
    historySize: int('HISTORY_SIZE', 120),
    recentAlertsSize: int('RECENT_ALERTS_SIZE', 50),
  },
};
