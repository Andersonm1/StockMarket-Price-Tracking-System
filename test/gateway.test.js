import test from 'node:test';
import assert from 'node:assert/strict';
import { SubscriptionRegistry, normalizeSymbols, ALL } from '../src/gateway/subscriptions.js';
import { MarketState } from '../src/gateway/market-state.js';

test('normalizeSymbols uppercases, dedupes and rejects junk', () => {
  assert.deepEqual([...normalizeSymbols(['aapl', 'AAPL', ' msft ', '*'])], ['AAPL', 'MSFT', ALL]);
  assert.deepEqual([...normalizeSymbols(['BRK.B'])], ['BRK.B']);
  assert.equal(normalizeSymbols('AAPL'), null);
  assert.equal(normalizeSymbols(['<script>']), null);
  assert.equal(normalizeSymbols([42]), null);
});

test('registry routes prices only to interested clients', () => {
  const registry = new SubscriptionRegistry();
  const everything = {};
  const appleFan = {};
  const nobody = {};
  registry.add(everything);
  registry.add(appleFan, ['AAPL']);
  registry.add(nobody, []);

  assert.deepEqual([...registry.recipients('AAPL')], [everything, appleFan]);
  assert.deepEqual([...registry.recipients('MSFT')], [everything]);

  registry.subscribe(nobody, ['MSFT']);
  assert.deepEqual([...registry.recipients('MSFT')], [everything, nobody]);

  registry.unsubscribe(appleFan, ['AAPL']);
  assert.equal(registry.wants(appleFan, 'AAPL'), false);

  assert.deepEqual(registry.set(everything, ['TSLA', 'NVDA']), ['NVDA', 'TSLA']);
  assert.equal(registry.wants(everything, 'AAPL'), false);

  registry.remove(nobody);
  assert.equal(registry.size, 2);
});

test('market state keeps bounded history and snapshots by symbol', () => {
  const state = new MarketState({ companies: [{ symbol: 'AAPL', name: 'Apple Inc.', sector: 'Technology' }], historySize: 3 });
  for (let i = 1; i <= 5; i += 1) state.applyTick({ symbol: 'AAPL', price: i, timestamp: i });
  assert.deepEqual(state.history.get('AAPL'), [[3, 3], [4, 4], [5, 5]]);

  assert.equal(state.applyTick({ symbol: 'IBM', name: 'IBM', price: 200, timestamp: 1 }), true, 'new company reported');
  assert.deepEqual(state.listCompanies().map((c) => c.symbol), ['AAPL', 'IBM']);

  const snap = state.snapshot(['IBM', 'NOPE']);
  assert.deepEqual(snap.prices.map((p) => p.symbol), ['IBM']);
  assert.deepEqual(Object.keys(snap.history), ['IBM']);
  assert.equal(state.snapshot([ALL]).prices.length, 2);
});
