import { config } from '../config.js';
import { createLogger } from '../lib/logger.js';
import { createKafka, createProducer, ensureTopics, onShutdown } from '../lib/kafka.js';
import { resolveCompanies } from '../shared/companies.js';
import { SimulatedSource } from './sources/simulated.js';
import { FinnhubSource } from './sources/finnhub.js';

const logger = createLogger('price-producer', config.logLevel);

async function main() {
  const kafka = createKafka('price-producer', logger);
  await ensureTopics(kafka, logger);

  const producer = createProducer(kafka);
  await producer.connect();

  const companies = resolveCompanies(config.producer.symbols);
  const { source: kind, tickIntervalMs: intervalMs, finnhubApiKey: apiKey } = config.producer;
  const source = kind === 'finnhub'
    ? new FinnhubSource(companies, { apiKey, intervalMs, logger })
    : new SimulatedSource(companies, { intervalMs, logger });

  let published = 0;
  source.start(async (ticks) => {
    await producer.send({
      topic: config.topics.prices,
      // Keyed by symbol: every tick for a company lands on the same partition, preserving order.
      messages: ticks.map((t) => ({ key: t.symbol, value: JSON.stringify(t), timestamp: String(t.timestamp) })),
    });
    published += ticks.length;
  });

  const stats = setInterval(() => {
    logger.info('publishing', { ticksLastMinute: published, topic: config.topics.prices, source: kind });
    published = 0;
  }, 60000);

  onShutdown(logger, async () => {
    clearInterval(stats);
    await source.stop();
    await producer.disconnect();
  });
}

main().catch((err) => {
  logger.error('producer failed to start', err);
  process.exit(1);
});
