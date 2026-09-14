import { randomUUID } from 'node:crypto';
import kafkajs from 'kafkajs';
import { config } from '../config.js';

const { Kafka, logLevel, Partitioners } = kafkajs;

const KAFKA_TO_LOGGER = {
  [logLevel.ERROR]: 'error',
  [logLevel.WARN]: 'warn',
  [logLevel.INFO]: 'info',
  [logLevel.DEBUG]: 'debug',
};

export function createKafka(clientId, logger) {
  return new Kafka({
    clientId,
    brokers: config.kafka.brokers,
    logLevel: logLevel.WARN,
    retry: { initialRetryTime: 300, retries: 20 },
    logCreator: () => ({ level, log }) => {
      const { message, ...extra } = log;
      logger[KAFKA_TO_LOGGER[level] || 'info'](`kafka: ${message}`, { namespace: log.namespace, error: extra.error });
    },
  });
}

export function createProducer(kafka) {
  return kafka.producer({
    createPartitioner: Partitioners.DefaultPartitioner,
    allowAutoTopicCreation: false,
  });
}

/** Creates the topics the system needs if they don't exist yet. Safe to call from every service. */
export async function ensureTopics(kafka, logger) {
  const admin = kafka.admin();
  await admin.connect();
  try {
    const existing = new Set(await admin.listTopics());
    const { partitions, replicationFactor } = config.kafka;
    const topics = [
      { topic: config.topics.prices, numPartitions: partitions, replicationFactor },
      { topic: config.topics.alerts, numPartitions: partitions, replicationFactor },
      {
        // Compacted: Kafka keeps the latest value per rule id, so the topic is the rule store.
        topic: config.topics.rules,
        numPartitions: 1,
        replicationFactor,
        configEntries: [{ name: 'cleanup.policy', value: 'compact' }],
      },
    ].filter((t) => !existing.has(t.topic));

    if (topics.length === 0) return;
    try {
      const created = await admin.createTopics({ topics, waitForLeaders: true });
      if (created) logger.info('created kafka topics', { topics: topics.map((t) => t.topic) });
    } catch (err) {
      // Another service may have created them at the same moment.
      if (err.type !== 'TOPIC_ALREADY_EXISTS' && !/already exists/i.test(err.message)) throw err;
    }
  } finally {
    await admin.disconnect();
  }
}

/**
 * Reads a compacted topic from the beginning with a throwaway consumer group so every
 * instance sees the full state. `ready` resolves once the messages that existed at
 * startup have been replayed (or after `catchUpTimeoutMs` as a safety net).
 */
export async function consumeCompactedTopic(kafka, { topic, groupPrefix, onMessage, logger, catchUpTimeoutMs = 15000 }) {
  const admin = kafka.admin();
  await admin.connect();
  const offsets = await admin.fetchTopicOffsets(topic);
  await admin.disconnect();

  const pending = new Map(
    offsets.filter((o) => Number(o.high) > Number(o.low)).map((o) => [o.partition, Number(o.high)]),
  );

  let markReady;
  const ready = new Promise((resolve) => { markReady = resolve; });
  if (pending.size === 0) markReady();
  const timer = setTimeout(() => {
    if (pending.size > 0) logger.warn('timed out replaying compacted topic, continuing', { topic });
    markReady();
  }, catchUpTimeoutMs);
  ready.then(() => clearTimeout(timer));

  const consumer = kafka.consumer({ groupId: `${groupPrefix}-${randomUUID()}` });
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });
  await consumer.run({
    autoCommit: false,
    eachMessage: async ({ partition, message }) => {
      try {
        onMessage(message);
      } catch (err) {
        logger.error('failed to handle compacted topic message', err);
      }
      const target = pending.get(partition);
      if (target !== undefined && Number(message.offset) + 1 >= target) {
        pending.delete(partition);
        if (pending.size === 0) markReady();
      }
    },
  });

  return { consumer, ready };
}

export function parseJson(buffer) {
  if (!buffer) return null;
  return JSON.parse(buffer.toString('utf8'));
}

/** Runs `fn` once on SIGINT/SIGTERM, then exits. */
export function onShutdown(logger, fn) {
  let closing = false;
  const handler = async (signal) => {
    if (closing) return;
    closing = true;
    logger.info('shutting down', { signal });
    try {
      await fn();
    } catch (err) {
      logger.error('error during shutdown', err);
    } finally {
      process.exit(0);
    }
  };
  process.on('SIGINT', handler);
  process.on('SIGTERM', handler);
}
