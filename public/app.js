(() => {
  'use strict';

  const HISTORY_POINTS = 120;
  const STORAGE_KEY = 'tickerwatch.selection';

  const $ = (id) => document.getElementById(id);
  const el = {
    conn: $('conn'), connText: $('conn-text'), source: $('source-badge'),
    search: $('company-search'), companyList: $('company-list'), filterCount: $('filter-count'),
    selectAll: $('select-all'), selectNone: $('select-none'),
    grid: $('price-grid'), empty: $('empty-prices'), sort: $('sort'),
    ruleForm: $('rule-form'), ruleSymbol: $('rule-symbol'), ruleDirection: $('rule-direction'),
    ruleThreshold: $('rule-threshold'), ruleNote: $('rule-note'), ruleError: $('rule-error'),
    ruleList: $('rule-list'), ruleCount: $('rule-count'), alertFeed: $('alert-feed'),
    notifyBtn: $('enable-notifications'), toasts: $('toasts'), cardTemplate: $('card-template'),
  };

  const state = {
    companies: new Map(),
    selected: loadSelection(), // null = every company (including ones that appear later)
    prices: new Map(),
    history: new Map(),
    rules: new Map(),
    alerts: [],
    cards: new Map(),
  };

  const money = new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' });
  const compact = new Intl.NumberFormat('en-US', { notation: 'compact', maximumFractionDigits: 1 });
  const timeFmt = new Intl.DateTimeFormat(undefined, { hour: '2-digit', minute: '2-digit', second: '2-digit' });

  // ------------------------------------------------------------ selection
  function loadSelection() {
    try {
      const raw = localStorage.getItem(STORAGE_KEY);
      return raw ? new Set(JSON.parse(raw)) : null;
    } catch {
      return null;
    }
  }

  function saveSelection() {
    try {
      if (state.selected === null) localStorage.removeItem(STORAGE_KEY);
      else localStorage.setItem(STORAGE_KEY, JSON.stringify([...state.selected]));
    } catch { /* storage unavailable */ }
  }

  const isSelected = (symbol) => state.selected === null || state.selected.has(symbol);

  function setSelection(selected) {
    // Collapse "every known company ticked" back to "all" so newly listed companies show up too.
    if (selected && selected.size === state.companies.size && [...state.companies.keys()].every((s) => selected.has(s))) {
      selected = null;
    }
    state.selected = selected;
    saveSelection();
    sendSubscriptions();
    renderCompanies();
    renderGrid();
  }

  // ------------------------------------------------------------ websocket
  let ws;
  let reconnectDelay = 500;

  function connect() {
    setConnection('connecting', 'Connecting…');
    const scheme = location.protocol === 'https:' ? 'wss' : 'ws';
    ws = new WebSocket(`${scheme}://${location.host}/ws`);

    ws.addEventListener('open', () => {
      reconnectDelay = 500;
      setConnection('open', 'Live');
    });

    ws.addEventListener('message', (event) => {
      let msg;
      try { msg = JSON.parse(event.data); } catch { return; }
      const handler = handlers[msg.type];
      if (handler) handler(msg.data);
    });

    ws.addEventListener('close', () => {
      setConnection('closed', `Reconnecting in ${Math.round(reconnectDelay / 1000)}s…`);
      setTimeout(connect, reconnectDelay);
      reconnectDelay = Math.min(reconnectDelay * 2, 10000);
    });
  }

  function sendSubscriptions() {
    if (!ws || ws.readyState !== WebSocket.OPEN) return;
    const symbols = state.selected === null ? ['*'] : [...state.selected];
    ws.send(JSON.stringify({ type: 'set-subscriptions', symbols }));
  }

  function setConnection(kind, text) {
    el.conn.className = `pill pill--${kind}`;
    el.connText.textContent = text;
  }

  const handlers = {
    welcome(data) {
      state.companies = new Map(data.companies.map((c) => [c.symbol, c]));
      state.rules = new Map(data.rules.map((r) => [r.id, r]));
      state.alerts = data.alerts;
      applySnapshot(data);
      if (state.selected !== null) sendSubscriptions(); // server defaults to all companies
      renderCompanies();
      renderRuleSymbols();
      renderRules();
      renderAlerts();
      renderGrid();
    },
    snapshot(data) {
      applySnapshot(data);
      renderGrid();
    },
    price(tick) {
      const previous = state.prices.get(tick.symbol);
      state.prices.set(tick.symbol, tick);
      pushHistory(tick.symbol, tick.price);
      if (!state.companies.has(tick.symbol)) state.companies.set(tick.symbol, { symbol: tick.symbol, name: tick.name, sector: tick.sector });
      showSource(tick.source);
      if (!isSelected(tick.symbol)) return;
      if (!state.cards.has(tick.symbol)) renderGrid();
      else updateCard(tick.symbol, previous);
    },
    companies(list) {
      state.companies = new Map(list.map((c) => [c.symbol, c]));
      renderCompanies();
      renderRuleSymbols();
    },
    'rule-saved'(rule) {
      state.rules.set(rule.id, rule);
      renderRules();
    },
    'rule-deleted'({ id }) {
      state.rules.delete(id);
      renderRules();
    },
    alert(alert) {
      state.alerts.unshift(alert);
      state.alerts.length = Math.min(state.alerts.length, 50);
      renderAlerts();
      notify(alert);
    },
    error(data) {
      console.warn('server error:', data.message);
    },
  };

  function applySnapshot({ prices = [], history = {} }) {
    for (const tick of prices) {
      state.prices.set(tick.symbol, tick);
      showSource(tick.source);
    }
    for (const [symbol, points] of Object.entries(history)) {
      state.history.set(symbol, points.map(([, price]) => price).slice(-HISTORY_POINTS));
    }
  }

  function pushHistory(symbol, price) {
    const points = state.history.get(symbol) ?? [];
    points.push(price);
    if (points.length > HISTORY_POINTS) points.shift();
    state.history.set(symbol, points);
  }

  function showSource(source) {
    if (!source || el.source.dataset.value === source) return;
    el.source.dataset.value = source;
    el.source.textContent = source === 'simulated' ? 'Simulated data' : `Source: ${source}`;
    el.source.hidden = false;
  }

  // ------------------------------------------------------------ companies
  function renderCompanies() {
    const query = el.search.value.trim().toLowerCase();
    const companies = [...state.companies.values()].sort((a, b) => a.symbol.localeCompare(b.symbol));
    const fragment = document.createDocumentFragment();

    for (const c of companies) {
      if (query && !c.symbol.toLowerCase().includes(query) && !(c.name || '').toLowerCase().includes(query)) continue;
      const li = document.createElement('li');
      const label = document.createElement('label');
      const box = document.createElement('input');
      box.type = 'checkbox';
      box.checked = isSelected(c.symbol);
      box.value = c.symbol;
      const sym = document.createElement('span');
      sym.className = 'sym';
      sym.textContent = c.symbol;
      const nm = document.createElement('span');
      nm.className = 'nm';
      nm.textContent = c.name;
      nm.title = `${c.name} · ${c.sector}`;
      label.append(box, sym, nm);
      li.append(label);
      fragment.append(li);
    }
    el.companyList.replaceChildren(fragment);

    const total = state.companies.size;
    const count = state.selected === null ? total : state.selected.size;
    el.filterCount.textContent = `${count}/${total}`;
  }

  el.companyList.addEventListener('change', (event) => {
    const box = event.target;
    if (box.type !== 'checkbox') return;
    const selected = new Set(state.selected ?? state.companies.keys());
    if (box.checked) selected.add(box.value);
    else selected.delete(box.value);
    setSelection(selected);
  });
  el.search.addEventListener('input', renderCompanies);
  el.selectAll.addEventListener('click', () => setSelection(null));
  el.selectNone.addEventListener('click', () => setSelection(new Set()));

  // ------------------------------------------------------------ price grid
  function sortedSymbols() {
    const symbols = [...state.prices.keys()].filter(isSelected);
    const change = (s) => state.prices.get(s).changePercent;
    switch (el.sort.value) {
      case 'change-desc': return symbols.sort((a, b) => change(b) - change(a));
      case 'change-asc': return symbols.sort((a, b) => change(a) - change(b));
      default: return symbols.sort();
    }
  }

  function renderGrid() {
    const symbols = sortedSymbols();
    const wanted = new Set(symbols);
    for (const [symbol, card] of state.cards) {
      if (!wanted.has(symbol)) {
        card.root.remove();
        state.cards.delete(symbol);
      }
    }
    for (const symbol of symbols) {
      if (!state.cards.has(symbol)) {
        state.cards.set(symbol, createCard(symbol));
        updateCard(symbol);
      }
      el.grid.append(state.cards.get(symbol).root); // append moves existing nodes into sorted order
    }
    el.empty.hidden = symbols.length > 0;
    el.empty.textContent = state.selected !== null && state.selected.size === 0
      ? 'No companies selected. Pick some from the list.'
      : 'Waiting for market data…';
  }

  function createCard(symbol) {
    const root = el.cardTemplate.content.firstElementChild.cloneNode(true);
    const q = (sel) => root.querySelector(sel);
    const card = {
      root,
      symbol: q('.card__symbol'), name: q('.card__name'), price: q('.card__price'), change: q('.card__change'),
      spark: q('.card__spark'), open: q('.stat-open'), high: q('.stat-high'), low: q('.stat-low'), volume: q('.stat-volume'),
    };
    root.dataset.symbol = symbol;
    q('.card__alert').addEventListener('click', () => prefillRule(symbol));
    return card;
  }

  function updateCard(symbol, previous) {
    const card = state.cards.get(symbol);
    const tick = state.prices.get(symbol);
    if (!card || !tick) return;

    card.symbol.textContent = symbol;
    card.name.textContent = tick.name || state.companies.get(symbol)?.name || '';
    card.name.title = card.name.textContent;
    card.price.textContent = money.format(tick.price);
    const up = tick.change >= 0;
    card.change.textContent = `${up ? '+' : ''}${tick.change.toFixed(2)} (${up ? '+' : ''}${tick.changePercent.toFixed(2)}%)`;
    card.change.className = `card__change ${up ? 'up' : 'down'}`;
    card.open.textContent = tick.open.toFixed(2);
    card.high.textContent = tick.high.toFixed(2);
    card.low.textContent = tick.low.toFixed(2);
    card.volume.textContent = compact.format(tick.volume);

    if (previous && previous.price !== tick.price) {
      const cls = tick.price > previous.price ? 'flash-up' : 'flash-down';
      card.root.classList.remove('flash-up', 'flash-down');
      card.root.classList.add(cls);
      clearTimeout(card.flashTimer);
      card.flashTimer = setTimeout(() => card.root.classList.remove(cls), 450);
    }
    drawSparkline(card.spark, state.history.get(symbol) ?? []);
  }

  function drawSparkline(canvas, points) {
    const dpr = window.devicePixelRatio || 1;
    const width = canvas.clientWidth;
    const height = canvas.clientHeight;
    if (!width) return;
    if (canvas.width !== Math.round(width * dpr)) {
      canvas.width = Math.round(width * dpr);
      canvas.height = Math.round(height * dpr);
    }
    const ctx = canvas.getContext('2d');
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, width, height);
    if (points.length < 2) return;

    const min = Math.min(...points);
    const max = Math.max(...points);
    const range = max - min || 1;
    const x = (i) => (i / (points.length - 1)) * width;
    const y = (p) => height - 3 - ((p - min) / range) * (height - 6);
    const color = points[points.length - 1] >= points[0] ? '#22c55e' : '#f43f5e';

    ctx.beginPath();
    points.forEach((p, i) => (i ? ctx.lineTo(x(i), y(p)) : ctx.moveTo(x(i), y(p))));
    ctx.strokeStyle = color;
    ctx.lineWidth = 1.5;
    ctx.lineJoin = 'round';
    ctx.stroke();

    ctx.lineTo(width, height);
    ctx.lineTo(0, height);
    ctx.closePath();
    const gradient = ctx.createLinearGradient(0, 0, 0, height);
    gradient.addColorStop(0, `${color}40`);
    gradient.addColorStop(1, `${color}00`);
    ctx.fillStyle = gradient;
    ctx.fill();
  }

  el.sort.addEventListener('change', renderGrid);
  // Re-sort gainers/losers periodically rather than on every tick, so cards don't jump constantly.
  setInterval(() => { if (el.sort.value !== 'symbol') renderGrid(); }, 3000);
  window.addEventListener('resize', () => {
    for (const [symbol, card] of state.cards) drawSparkline(card.spark, state.history.get(symbol) ?? []);
  });

  // ------------------------------------------------------------ alert rules
  function renderRuleSymbols() {
    const current = el.ruleSymbol.value;
    const options = [...state.companies.values()]
      .sort((a, b) => a.symbol.localeCompare(b.symbol))
      .map((c) => new Option(`${c.symbol} · ${c.name}`, c.symbol));
    el.ruleSymbol.replaceChildren(...options);
    if (current && state.companies.has(current)) el.ruleSymbol.value = current;
  }

  function renderRules() {
    const rules = [...state.rules.values()].sort((a, b) => a.symbol.localeCompare(b.symbol) || a.threshold - b.threshold);
    el.ruleCount.textContent = rules.length ? `(${rules.length})` : '';
    if (rules.length === 0) {
      el.ruleList.replaceChildren(emptyItem('No alerts yet. Add one above or click 🔔 on a card.'));
      return;
    }
    el.ruleList.replaceChildren(...rules.map((rule) => {
      const li = document.createElement('li');
      li.dataset.id = rule.id;

      const text = document.createElement('span');
      text.className = 'rule-text';
      text.textContent = `${rule.symbol} ${rule.direction === 'above' ? '≥' : '≤'} ${money.format(rule.threshold)} `;
      const distance = document.createElement('span');
      distance.className = 'distance';
      text.append(distance);
      if (rule.note) {
        const note = document.createElement('span');
        note.className = 'rule-note';
        note.textContent = rule.note;
        text.append(note);
      }

      const del = document.createElement('button');
      del.type = 'button';
      del.className = 'icon-btn';
      del.title = 'Delete alert';
      del.textContent = '✕';
      del.addEventListener('click', () => deleteRule(rule.id));

      li.append(text, del);
      return li;
    }));
    updateRuleDistances();
  }

  function updateRuleDistances() {
    for (const li of el.ruleList.querySelectorAll('li[data-id]')) {
      const rule = state.rules.get(li.dataset.id);
      const tick = rule && state.prices.get(rule.symbol);
      const target = li.querySelector('.distance');
      if (!tick || !target) continue;
      const pct = ((rule.threshold - tick.price) / tick.price) * 100;
      target.textContent = `(${pct >= 0 ? '+' : ''}${pct.toFixed(2)}% away)`;
    }
  }
  setInterval(updateRuleDistances, 1000);

  function prefillRule(symbol) {
    el.ruleSymbol.value = symbol;
    const tick = state.prices.get(symbol);
    if (tick) {
      el.ruleDirection.value = 'above';
      el.ruleThreshold.value = (Math.ceil(tick.price * 1.005 * 100) / 100).toFixed(2);
    }
    el.ruleThreshold.focus();
    el.ruleThreshold.select();
  }

  el.ruleForm.addEventListener('submit', async (event) => {
    event.preventDefault();
    el.ruleError.hidden = true;
    const body = {
      symbol: el.ruleSymbol.value,
      direction: el.ruleDirection.value,
      threshold: Number(el.ruleThreshold.value),
      note: el.ruleNote.value,
    };
    try {
      const res = await fetch('/api/alerts/rules', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(body),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || 'Could not create alert');
      state.rules.set(data.id, data); // the Kafka round-trip will confirm it moments later
      renderRules();
      el.ruleThreshold.value = '';
      el.ruleNote.value = '';
      maybeShowNotificationButton();
    } catch (err) {
      el.ruleError.textContent = err.message;
      el.ruleError.hidden = false;
    }
  });

  async function deleteRule(id) {
    const res = await fetch(`/api/alerts/rules/${encodeURIComponent(id)}`, { method: 'DELETE' });
    if (res.ok || res.status === 404) {
      state.rules.delete(id);
      renderRules();
    }
  }

  // ------------------------------------------------------------ triggered alerts
  function renderAlerts() {
    if (state.alerts.length === 0) {
      el.alertFeed.replaceChildren(emptyItem('Nothing triggered yet.'));
      return;
    }
    el.alertFeed.replaceChildren(...state.alerts.map((alert) => {
      const li = document.createElement('li');
      const text = document.createElement('span');
      text.textContent = alert.message + (alert.note ? ` · ${alert.note}` : '');
      const time = document.createElement('time');
      time.dateTime = new Date(alert.triggeredAt).toISOString();
      time.textContent = timeFmt.format(alert.triggeredAt);
      li.append(text, time);
      return li;
    }));
  }

  function notify(alert) {
    const toast = document.createElement('div');
    toast.className = 'toast';
    const title = document.createElement('strong');
    title.textContent = `🔔 ${alert.symbol} alert`;
    const body = document.createElement('span');
    body.textContent = alert.message;
    toast.append(title, body);
    el.toasts.append(toast);
    setTimeout(() => toast.remove(), 6000);

    const card = state.cards.get(alert.symbol);
    if (card) {
      card.root.classList.remove('alerted');
      void card.root.offsetWidth; // restart the animation
      card.root.classList.add('alerted');
    }

    if ('Notification' in window && Notification.permission === 'granted' && document.hidden) {
      new Notification(`${alert.symbol} price alert`, { body: alert.message, tag: alert.ruleId });
    }
  }

  function maybeShowNotificationButton() {
    el.notifyBtn.hidden = !('Notification' in window) || Notification.permission !== 'default';
  }
  el.notifyBtn.addEventListener('click', async () => {
    await Notification.requestPermission();
    maybeShowNotificationButton();
  });

  function emptyItem(text) {
    const li = document.createElement('li');
    li.className = 'list-empty';
    li.textContent = text;
    return li;
  }

  maybeShowNotificationButton();
  renderRules();
  renderAlerts();
  connect();
})();
