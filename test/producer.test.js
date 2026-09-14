import test from 'node:test';
import assert from 'node:assert/strict';
import { PriceSimulator } from '../src/producer/sources/simulated.js';
import { COMPANIES, resolveCompanies } from '../src/shared/companies.js';
import { buildRule } from '../src/shared/rules.js';

function seeded(seed) {
  // mulberry32
  let a = seed;
  return () => {
    a |= 0; a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

test('simulator emits one well-formed tick per company', () => {
  const sim = new PriceSimulator(COMPANIES, { random: seeded(1), now: () => 1234 });
  const ticks = sim.tick();
  assert.equal(ticks.length, COMPANIES.length);
  for (const t of ticks) {
    assert.ok(t.price > 0);
    assert.ok(t.low <= t.price && t.price <= t.high);
    assert.equal(t.timestamp, 1234);
    assert.equal(t.source, 'simulated');
    assert.equal(Math.round(t.price * 100) / 100, t.price, 'rounded to cents');
  }
});

test('simulated prices stay in a plausible band over a long run', () => {
  const sim = new PriceSimulator(COMPANIES, { random: seeded(42) });
  let last;
  for (let i = 0; i < 20000; i += 1) last = sim.tick();
  for (const t of last) {
    const base = COMPANIES.find((c) => c.symbol === t.symbol).basePrice;
    assert.ok(t.price > base * 0.5 && t.price < base * 1.5, `${t.symbol} drifted to ${t.price}`);
    assert.ok(t.volume > 0);
  }
});

test('resolveCompanies filters the watchlist and tolerates unknown tickers', () => {
  assert.equal(resolveCompanies(null).length, COMPANIES.length);
  const picked = resolveCompanies(['msft', 'ZZZZ']);
  assert.deepEqual(picked.map((c) => c.symbol), ['MSFT', 'ZZZZ']);
  assert.equal(picked[0].name, 'Microsoft Corporation');
});

test('buildRule validates and normalises alert rules', () => {
  const { rule } = buildRule({ symbol: ' aapl ', direction: 'below', threshold: '199.999999', note: 'buy the dip' }, { now: 5 });
  assert.equal(rule.symbol, 'AAPL');
  assert.equal(rule.threshold, 200);
  assert.equal(rule.createdAt, 5);
  assert.ok(rule.id);

  assert.match(buildRule({ symbol: 'AAPL', direction: 'sideways', threshold: 1 }).error, /direction/);
  assert.match(buildRule({ symbol: 'AAPL', direction: 'above', threshold: -3 }).error, /threshold/);
  assert.match(buildRule({ symbol: 'AAPL', direction: 'above', threshold: 'abc' }).error, /threshold/);
  assert.match(buildRule({ symbol: '', direction: 'above', threshold: 1 }).error, /symbol/);
  assert.match(buildRule(null).error, /object/);
});
