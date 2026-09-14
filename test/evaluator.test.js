import test from 'node:test';
import assert from 'node:assert/strict';
import { AlertEvaluator, crossed } from '../src/alerts/evaluator.js';

const tick = (price, symbol = 'AAPL') => ({ symbol, name: 'Apple Inc.', price, timestamp: 0 });

test('crossed detects movement through the threshold in the rule direction only', () => {
  const above = { direction: 'above', threshold: 100 };
  const below = { direction: 'below', threshold: 100 };
  assert.equal(crossed(above, 99.5, 100), true);
  assert.equal(crossed(above, 99, 101), true);
  assert.equal(crossed(above, 101, 102), false, 'already above');
  assert.equal(crossed(above, 101, 99), false, 'wrong direction');
  assert.equal(crossed(below, 100.5, 100), true);
  assert.equal(crossed(below, 99, 98), false, 'already below');
});

test('the first tick for a symbol only sets a baseline', () => {
  const evaluator = new AlertEvaluator();
  evaluator.upsertRule({ id: 'r1', symbol: 'AAPL', direction: 'above', threshold: 100 });
  assert.deepEqual(evaluator.evaluate(tick(150)), []);
});

test('fires once per crossing and re-arms after the price comes back', () => {
  let now = 0;
  const evaluator = new AlertEvaluator({ now: () => now });
  evaluator.upsertRule({ id: 'r1', symbol: 'AAPL', direction: 'above', threshold: 100, note: 'take profit' });

  evaluator.evaluate(tick(99));
  const [alert] = evaluator.evaluate(tick(100.25));
  assert.equal(alert.ruleId, 'r1');
  assert.equal(alert.price, 100.25);
  assert.equal(alert.previousPrice, 99);
  assert.equal(alert.note, 'take profit');
  assert.match(alert.message, /AAPL crossed above \$100\.00 \(now \$100\.25\)/);

  now += 1;
  assert.equal(evaluator.evaluate(tick(101)).length, 0, 'staying above does not re-fire');
  evaluator.evaluate(tick(98));
  assert.equal(evaluator.evaluate(tick(102)).length, 1, 're-armed after dropping back below');
});

test('cooldown suppresses repeated alerts for a noisy price', () => {
  let now = 1000;
  const evaluator = new AlertEvaluator({ cooldownMs: 30000, now: () => now });
  evaluator.upsertRule({ id: 'r1', symbol: 'AAPL', direction: 'above', threshold: 100 });

  evaluator.evaluate(tick(99));
  assert.equal(evaluator.evaluate(tick(101)).length, 1);
  evaluator.evaluate(tick(99));
  now += 10000;
  assert.equal(evaluator.evaluate(tick(101)).length, 0, 'inside cooldown');
  evaluator.evaluate(tick(99));
  now += 30000;
  assert.equal(evaluator.evaluate(tick(101)).length, 1, 'after cooldown');
});

test('rules only apply to their own symbol and can be updated or removed', () => {
  const evaluator = new AlertEvaluator();
  evaluator.upsertRule({ id: 'r1', symbol: 'AAPL', direction: 'below', threshold: 50 });
  evaluator.evaluate(tick(60, 'MSFT'));
  assert.equal(evaluator.evaluate(tick(40, 'MSFT')).length, 0);

  // Moving the rule to MSFT removes it from AAPL.
  evaluator.upsertRule({ id: 'r1', symbol: 'MSFT', direction: 'below', threshold: 50 });
  assert.equal(evaluator.rulesBySymbol.has('AAPL'), false);
  evaluator.evaluate(tick(60, 'MSFT'));
  assert.equal(evaluator.evaluate(tick(40, 'MSFT')).length, 1);

  assert.equal(evaluator.removeRule('r1'), true);
  assert.equal(evaluator.ruleCount, 0);
  evaluator.evaluate(tick(60, 'MSFT'));
  assert.equal(evaluator.evaluate(tick(40, 'MSFT')).length, 0);
});

test('observe updates the baseline without firing', () => {
  const evaluator = new AlertEvaluator();
  evaluator.upsertRule({ id: 'r1', symbol: 'AAPL', direction: 'above', threshold: 100 });
  evaluator.evaluate(tick(90));
  evaluator.observe(tick(110));
  assert.equal(evaluator.evaluate(tick(111)).length, 0);
});
