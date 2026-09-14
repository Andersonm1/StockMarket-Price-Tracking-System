import { config } from '../config.js';
import { createLogger } from '../lib/logger.js';
import {
  consumeCompactedTopic, createKafka, createProducer, ensureTopics, onShutdown, parseJson,
} from '../lib/kafka.js';
import { AlertEvaluator } from './evaluator.js';

const logger = createLogger('alert-engine', config.logLevel);

async function notifyWebhook(alert) {
  if (!config.alerts.webhookUrl) return;
  try {
    const res = await fetch(config.alerts.webhookUrl, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      // `text` is understood by Slack, `content` by Discord; the full alert is included for custom receivers.
      body: JSON.stringify({ text: `🔔 ${alert.message}`, content: `🔔 ${alert.message}`, alert }),
      signal: AbortSignal.timeout(5000),
    });
    if (!res.ok) logger.warn('webhook responded with an error', { status: res.status });
  } catch (err) {
    logger.warn('webhook delivery failed', { error: err.message });
  }
}

async function main() {
  const kafka = createKafka('alert-engine', logger);
  await ensureTopics(kafka, logger);

  const evaluator = new AlertEvaluator({ cooldownMs: config.alerts.cooldownMs });
  const producer = createProducer(kafka);
  await producer.connect();

  // 1. Load every alert rule before looking at prices.
  const rules = await consumeCompactedTopic(kafka, {
    topic: config.topics.rules,
    groupPrefix: 'alert-engine-rules',
    logger,
    onMessage: (message) => {
      const id = message.key?.toString();
      if (!id) return;
      const rule = parseJson(message.value);
      if (rule) evaluator.upsertRule(rule);
      else evaluator.removeRule(id); // tombstone = deleted rule
    },
  });
  await rules.ready;
  logger.info('alert rules loaded', { count: evaluator.ruleCount });

  // 2. Evaluate prices. A shared group id lets several engines split the partitions;
  //    since ticks are keyed by symbol, each symbol's crossing state lives on one engine.
  const prices = kafka.consumer({ groupId: 'alert-engine' });
  await prices.connect();
  await prices.subscribe({ topic: config.topics.prices, fromBeginning: false });
  await prices.run({
    eachMessage: async ({ message }) => {
      let tick;
      try {
        tick = parseJson(message.value);
      } catch (err) {
        logger.warn('skipping malformed tick', { error: err.message });
        return;
      }
      if (!tick?.symbol || typeof tick.price !== 'number') return;

      if (Date.now() - tick.timestamp > config.alerts.maxTickAgeMs) {
        evaluator.observe(tick);
        return;
      }

      const alerts = evaluator.evaluate(tick);
      if (alerts.length === 0) return;

      await producer.send({
        topic: config.topics.alerts,
        messages: alerts.map((a) => ({ key: a.symbol, value: JSON.stringify(a) })),
      });
      for (const alert of alerts) {
        logger.info('alert triggered', { symbol: alert.symbol, detail: alert.message, ruleId: alert.ruleId });
        notifyWebhook(alert);
      }
    },
  });

  onShutdown(logger, async () => {
    await prices.disconnect();
    await rules.consumer.disconnect();
    await producer.disconnect();
  });
}

main().catch((err) => {
  logger.error('alert engine failed to start', err);
  process.exit(1);
});
