export const ALL = '*';

/** Normalises a client-supplied symbol list. Returns null if it is invalid. */
export function normalizeSymbols(symbols) {
  if (!Array.isArray(symbols) || symbols.length > 500) return null;
  const out = new Set();
  for (const s of symbols) {
    if (typeof s !== 'string') return null;
    const symbol = s.trim().toUpperCase();
    if (symbol === ALL) out.add(ALL);
    else if (/^[A-Z][A-Z0-9.\-]{0,14}$/.test(symbol)) out.add(symbol);
    else return null;
  }
  return out;
}

/** Tracks which companies each connected client wants to receive. */
export class SubscriptionRegistry {
  constructor() {
    this.clients = new Map(); // client -> Set(symbol | '*')
  }

  get size() {
    return this.clients.size;
  }

  add(client, symbols = [ALL]) {
    this.clients.set(client, new Set(symbols));
  }

  remove(client) {
    this.clients.delete(client);
  }

  /** Replaces the client's filter. */
  set(client, symbols) {
    this.clients.set(client, new Set(symbols));
    return this.get(client);
  }

  subscribe(client, symbols) {
    const current = this.clients.get(client) ?? new Set();
    for (const s of symbols) current.add(s);
    this.clients.set(client, current);
    return this.get(client);
  }

  unsubscribe(client, symbols) {
    const current = this.clients.get(client);
    if (!current) return [];
    for (const s of symbols) current.delete(s);
    return this.get(client);
  }

  get(client) {
    return [...(this.clients.get(client) ?? [])].sort();
  }

  wants(client, symbol) {
    const subs = this.clients.get(client);
    return !!subs && (subs.has(ALL) || subs.has(symbol));
  }

  *recipients(symbol) {
    for (const [client, subs] of this.clients) {
      if (subs.has(ALL) || subs.has(symbol)) yield client;
    }
  }

  *all() {
    yield* this.clients.keys();
  }
}
