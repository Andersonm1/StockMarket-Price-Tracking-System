import http from 'node:http';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import express from 'express';
import { WebSocketServer, WebSocket } from 'ws';
import { config } from '../config.js';
import { createLogger } from '../lib/logger.js';
import {
  consumeCompactedTopic, createKafka, createProducer, ensureTopics, onShutdown, parseJson,
} from '../lib/kafka.js';
import { resolveCompanies } from '../shared/companies.js';
import { buildRule } from '../shared/rules.js';
import { MarketState } from './market-state.js';
import { SubscriptionRegistry, normalizeSymbols, ALL } from './subscriptions.js';

const logger = createLogger('gateway', config.logLevel);
const PUBLIC_DIR = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../public');
const MAX_BUFFERED_BYTES = 1024 * 1024; // drop price updates for clients that can't keep up
const HEARTBEAT_MS = 30000;

async function main() {
  const state = new MarketState({
    companies: resolveCompanies(config.producer.symbols),
    historySize: config.gateway.historySize,
    recentAlertsSize: config.gateway.recentAlertsSize,
  });
  const registry = new SubscriptionRegistry();
  let kafkaReady = false;

  // ---------------------------------------------------------------- Kafka
  const kafka = createKafka(`gateway-${config.instanceId}`, logger);
  await ensureTopics(kafka, logger);

  const producer = createProducer(kafka);
  await producer.connect();

  const saveRule = (rule) => producer.send({
    topic: config.topics.rules,
    messages: [{ key: rule.id, value: JSON.stringify(rule) }],
  });
  const deleteRule = (id) => producer.send({
    topic: config.topics.rules,
    messages: [{ key: id, value: null }],
  });

  // ------------------------------------------------------------ WebSocket
  const send = (ws, type, data, { droppable = false } = {}) => {
    if (ws.readyState !== WebSocket.OPEN) return;
    if (droppable && ws.bufferedAmount > MAX_BUFFERED_BYTES) return;
    ws.send(JSON.stringify({ type, data }));
  };
  const broadcast = (type, data) => {
    for (const ws of registry.all()) send(ws, type, data);
  };

  const rules = await consumeCompactedTopic(kafka, {
    topic: config.topics.rules,
    groupPrefix: 'gateway-rules',
    logger,
    onMessage: (message) => {
      const id = message.key?.toString();
      if (!id) return;
      const rule = parseJson(message.value);
      if (rule) {
        state.rules.set(id, rule);
        broadcast('rule-saved', rule);
      } else if (state.rules.delete(id)) {
        broadcast('rule-deleted', { id });
      }
    },
  });
  await rules.ready;

  // A unique group per gateway instance: every instance must see every tick for its own clients.
  const streams = kafka.consumer({ groupId: `gateway-${config.instanceId}-${Date.now()}` });
  await streams.connect();
  await streams.subscribe({ topics: [config.topics.prices, config.topics.alerts], fromBeginning: false });
  await streams.run({
    autoCommit: false,
    eachMessage: async ({ topic, message }) => {
      let payload;
      try {
        payload = parseJson(message.value);
      } catch {
        return;
      }
      if (!payload) return;

      if (topic === config.topics.prices) {
        if (state.applyTick(payload)) broadcast('companies', state.listCompanies());
        for (const ws of registry.recipients(payload.symbol)) send(ws, 'price', payload, { droppable: true });
      } else if (topic === config.topics.alerts) {
        state.applyAlert(payload);
        // Alerts go to every client, regardless of the price filter: you want to hear about them.
        broadcast('alert', payload);
      }
    },
  });
  kafkaReady = true;

  // ----------------------------------------------------------------- HTTP
  const app = express();
  app.disable('x-powered-by');
  app.use(express.json({ limit: '16kb' }));
  app.use(express.static(PUBLIC_DIR));

  app.get('/api/health', (_req, res) => {
    res.status(kafkaReady ? 200 : 503).json({
      status: kafkaReady ? 'ok' : 'starting',
      instance: config.instanceId,
      clients: registry.size,
      symbolsStreaming: state.latest.size,
      rules: state.rules.size,
    });
  });

  app.get('/api/companies', (_req, res) => res.json(state.listCompanies()));

  app.get('/api/prices', (req, res) => {
    const symbols = req.query.symbols ? normalizeSymbols(String(req.query.symbols).split(',')) : null;
    if (req.query.symbols && !symbols) return res.status(400).json({ error: 'invalid symbols' });
    return res.json(state.snapshot(symbols ? [...symbols] : null));
  });

  app.get('/api/alerts/rules', (_req, res) => res.json(state.listRules()));

  app.post('/api/alerts/rules', async (req, res, next) => {
    const { rule, error } = buildRule(req.body);
    if (error) return res.status(400).json({ error });
    try {
      await saveRule(rule);
      return res.status(201).json(rule);
    } catch (err) {
      return next(err);
    }
  });

  app.delete('/api/alerts/rules/:id', async (req, res, next) => {
    if (!state.rules.has(req.params.id)) return res.status(404).json({ error: 'rule not found' });
    try {
      await deleteRule(req.params.id);
      return res.status(204).end();
    } catch (err) {
      return next(err);
    }
  });

  app.get('/api/alerts/history', (_req, res) => res.json(state.alerts));

  app.use((err, _req, res, _next) => {
    logger.error('request failed', err);
    res.status(500).json({ error: 'internal error' });
  });

  const server = http.createServer(app);
  const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 64 * 1024 });

  wss.on('connection', (ws, req) => {
    ws.isAlive = true;
    ws.on('pong', () => { ws.isAlive = true; });

    registry.add(ws, [ALL]);
    logger.debug('client connected', { ip: req.socket.remoteAddress, clients: registry.size });

    send(ws, 'welcome', {
      instance: config.instanceId,
      source: config.producer.source,
      companies: state.listCompanies(),
      subscriptions: registry.get(ws),
      rules: state.listRules(),
      alerts: state.alerts,
      ...state.snapshot(null),
    });

    ws.on('message', async (raw) => {
      let msg;
      try {
        msg = JSON.parse(raw.toString());
      } catch {
        return send(ws, 'error', { message: 'messages must be JSON' });
      }

      try {
        switch (msg?.type) {
          case 'subscribe':
          case 'unsubscribe':
          case 'set-subscriptions': {
            const symbols = normalizeSymbols(msg.symbols);
            if (!symbols) return send(ws, 'error', { message: 'symbols must be an array of tickers (or "*")' });
            const before = new Set(registry.get(ws));
            const action = { subscribe: 'subscribe', unsubscribe: 'unsubscribe', 'set-subscriptions': 'set' }[msg.type];
            const after = registry[action](ws, [...symbols]);
            send(ws, 'subscriptions', { symbols: after });
            // Send a snapshot for newly added companies so the client doesn't wait for the next tick.
            const added = after.includes(ALL) && !before.has(ALL) ? [ALL] : after.filter((s) => !before.has(s));
            if (added.length) send(ws, 'snapshot', state.snapshot(added));
            return undefined;
          }
          case 'create-rule': {
            const { rule, error } = buildRule(msg.rule);
            if (error) return send(ws, 'error', { message: error, requestId: msg.requestId });
            await saveRule(rule);
            return send(ws, 'rule-created', { rule, requestId: msg.requestId });
          }
          case 'delete-rule': {
            if (typeof msg.id !== 'string' || !state.rules.has(msg.id)) {
              return send(ws, 'error', { message: 'rule not found', requestId: msg.requestId });
            }
            await deleteRule(msg.id);
            return undefined;
          }
          case 'ping':
            return send(ws, 'pong', { time: Date.now() });
          default:
            return send(ws, 'error', { message: `unknown message type "${msg?.type}"` });
        }
      } catch (err) {
        logger.error('failed to handle client message', err);
        return send(ws, 'error', { message: 'internal error', requestId: msg.requestId });
      }
    });

    ws.on('close', () => registry.remove(ws));
    ws.on('error', () => registry.remove(ws));
  });

  const heartbeat = setInterval(() => {
    for (const ws of wss.clients) {
      if (!ws.isAlive) {
        registry.remove(ws);
        ws.terminate();
        continue;
      }
      ws.isAlive = false;
      ws.ping();
    }
  }, HEARTBEAT_MS);

  server.listen(config.gateway.port, () => {
    logger.info('gateway listening', { port: config.gateway.port, websocket: '/ws' });
  });

  onShutdown(logger, async () => {
    clearInterval(heartbeat);
    for (const ws of wss.clients) ws.close(1001, 'server shutting down');
    await new Promise((resolve) => server.close(resolve));
    await streams.disconnect();
    await rules.consumer.disconnect();
    await producer.disconnect();
  });
}

main().catch((err) => {
  logger.error('gateway failed to start', err);
  process.exit(1);
});
