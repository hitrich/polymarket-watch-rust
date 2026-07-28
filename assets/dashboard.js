(() => {
  'use strict';

  const token = document.querySelector('meta[name="command-token"]')?.content || '';
  const byId = (id) => document.getElementById(id);
  const missing = '--';
  const money = (value) => value == null ? missing : `$${formatNumber(value, 2)}`;
  const number = (value, digits = 3) => value == null ? missing : formatNumber(value, digits);
  const formatNumber = (value, digits) => {
    const parsed = Number(value);
    if (!Number.isFinite(parsed)) return missing;
    return new Intl.NumberFormat(undefined, {
      minimumFractionDigits: 0,
      maximumFractionDigits: digits,
    }).format(parsed);
  };
  const short = (value, leading = 7, trailing = 5) => {
    if (!value) return missing;
    const text = String(value);
    return text.length <= leading + trailing + 1
      ? text
      : `${text.slice(0, leading)}…${text.slice(-trailing)}`;
  };
  const time = (value) => {
    if (!Number.isFinite(Number(value)) || Number(value) <= 0) return missing;
    return new Date(Number(value)).toLocaleTimeString([], {
      hour: '2-digit', minute: '2-digit', second: '2-digit', fractionalSecondDigits: 3,
    });
  };
  const age = (timestamp, now = Date.now()) => {
    if (!Number.isFinite(Number(timestamp)) || Number(timestamp) <= 0) return 'never';
    const delta = Math.max(0, now - Number(timestamp));
    if (delta < 1000) return `${delta}ms ago`;
    if (delta < 60000) return `${(delta / 1000).toFixed(1)}s ago`;
    return `${Math.floor(delta / 60000)}m ago`;
  };

  const cell = (textValue, className = '') => {
    const node = document.createElement('td');
    node.textContent = textValue;
    if (className) node.className = className;
    return node;
  };
  const emptyRow = (columns, copy) => {
    const row = document.createElement('tr');
    const value = cell(copy, 'empty-cell');
    value.colSpan = columns;
    row.append(value);
    return row;
  };
  const replaceRows = (body, rows, columns, emptyCopy) => {
    body.replaceChildren(...(rows.length ? rows : [emptyRow(columns, emptyCopy)]));
  };

  function renderFeed(name, feed) {
    const card = document.querySelector(`[data-feed="${name}"]`);
    if (!card || !feed) return;
    card.dataset.status = feed.status;
    card.querySelector('b').textContent = String(feed.status || 'unknown').toUpperCase();
    const detail = feed.last_error
      ? feed.last_error
      : feed.status === 'disabled'
        ? 'Disabled by configuration'
        : `Last message ${age(feed.last_message_at_ms)}`;
    card.querySelector('small').textContent = detail;
  }

  function renderMarkets(runtime) {
    const markets = Object.entries(runtime.markets || {});
    byId('market-count').textContent = `${markets.length} MARKET${markets.length === 1 ? '' : 'S'}`;
    const rows = markets.map(([assetId, view]) => {
      const book = view.book || {};
      const row = document.createElement('tr');
      const marketCell = document.createElement('td');
      const title = document.createElement('strong');
      title.textContent = view.question || 'Untitled configured market';
      const condition = document.createElement('small');
      condition.textContent = short(view.condition_id, 10, 6);
      marketCell.append(title, condition);
      const spread = book.best_bid != null && book.best_ask != null
        ? Number(book.best_ask) - Number(book.best_bid)
        : null;
      const status = book.tradeable ? 'TRADEABLE' : 'LOCKED';
      row.append(
        marketCell,
        cell(short(assetId, 9, 6), 'mono'),
        cell(number(book.best_bid, 4), 'numeric'),
        cell(number(book.best_ask, 4), 'numeric'),
        cell(spread == null ? missing : number(spread, 4), 'numeric'),
        cell(number(book.last_trade_price, 4), 'numeric'),
        cell(age(book.local_received_at_ms)),
        cell(status, book.tradeable ? 'text-ok' : 'text-warn'),
      );
      return row;
    });
    replaceRows(
      byId('market-body'), rows, 8,
      'Waiting for verified market metadata and WebSocket snapshots.',
    );
  }

  function renderPortfolio(portfolio) {
    const accounting = byId('accounting-list');
    const values = portfolio ? [
      ['Starting cash', money(portfolio.starting_cash_usdc)],
      ['Cash', money(portfolio.cash_usdc)],
      ['Market value', money(portfolio.market_value_usdc)],
      ['Realized P&L', money(portfolio.realized_pnl_usdc)],
      ['Unrealized P&L', money(portfolio.unrealized_pnl_usdc)],
      ['Fees', money(portfolio.fees_paid_usdc)],
    ] : [
      ['Starting cash', missing], ['Cash', missing], ['Market value', missing],
      ['Realized P&L', missing], ['Unrealized P&L', missing], ['Fees', missing],
    ];
    accounting.replaceChildren(...values.map(([label, value]) => {
      const row = document.createElement('div');
      const term = document.createElement('dt');
      const detail = document.createElement('dd');
      term.textContent = label;
      detail.textContent = value;
      row.append(term, detail);
      return row;
    }));

    byId('metric-equity').textContent = portfolio ? money(portfolio.equity_usdc) : missing;
    byId('metric-equity-detail').textContent = portfolio
      ? `${money(portfolio.market_value_usdc)} marked value`
      : 'No portfolio snapshot';
    byId('metric-cash').textContent = portfolio ? money(portfolio.available_cash_usdc) : missing;
    byId('metric-cash-detail').textContent = portfolio
      ? `${money(portfolio.cash_usdc)} total cash`
      : 'No portfolio snapshot';
    const orders = portfolio?.open_orders || [];
    byId('metric-orders').textContent = portfolio ? String(orders.length) : missing;
    byId('metric-orders-detail').textContent = portfolio
      ? `${orders.length} resting paper order${orders.length === 1 ? '' : 's'}`
      : 'No execution snapshot';
    const loss = portfolio
      ? Math.max(0, Number(portfolio.starting_cash_usdc) - Number(portfolio.equity_usdc))
      : null;
    byId('metric-loss').textContent = loss == null ? missing : money(loss);
    byId('metric-loss-detail').textContent = portfolio
      ? 'Measured from paper starting equity'
      : 'No portfolio snapshot';

    const positions = (portfolio?.positions || []).filter((position) => Number(position.size) !== 0);
    byId('positions-empty').hidden = positions.length > 0;
    byId('position-list').replaceChildren(...positions.map((position) => {
      const row = document.createElement('article');
      const identity = document.createElement('div');
      const asset = document.createElement('strong');
      const basis = document.createElement('small');
      asset.textContent = short(position.asset_id, 10, 6);
      basis.textContent = `Average ${number(position.average_price, 4)}`;
      identity.append(asset, basis);
      const size = document.createElement('b');
      size.textContent = number(position.size, 3);
      row.append(identity, size);
      return row;
    }));

    const orderRows = orders.slice(0, 20).map((order) => {
      const row = document.createElement('tr');
      row.append(
        cell(short(order.state?.client_order_id, 10, 4), 'mono'),
        cell(String(order.state?.intent?.side || missing).toUpperCase()),
        cell(number(order.state?.intent?.limit_price, 4), 'numeric'),
        cell(number(order.remaining_size, 3), 'numeric'),
      );
      return row;
    });
    replaceRows(byId('order-body'), orderRows, 4, 'No open paper orders.');
  }

  function renderLiveAccount(account) {
    byId('portfolio-title').textContent = 'Live account';
    if (!account) {
      renderPortfolio(null);
      byId('portfolio-title').textContent = 'Live account';
      return;
    }
    const positionValue = Number(account.current_equity_usdc) - Number(account.collateral_balance_usdc);
    const cashPnl = (account.positions || []).reduce((sum, position) => sum + Number(position.cash_pnl_usdc || 0), 0);
    const values = [
      ['Snapshot time', time(account.updated_at_ms)],
      ['Collateral', money(account.collateral_balance_usdc)],
      ['Position value', money(positionValue)],
      ['Current equity', money(account.current_equity_usdc)],
      ['Position cash P&L', money(cashPnl)],
      ['Open orders', String(account.open_order_count ?? 0)],
    ];
    byId('accounting-list').replaceChildren(...values.map(([label, value]) => {
      const row = document.createElement('div');
      const term = document.createElement('dt');
      const detail = document.createElement('dd');
      term.textContent = label;
      detail.textContent = value;
      row.append(term, detail);
      return row;
    }));
    byId('metric-equity').textContent = money(account.current_equity_usdc);
    byId('metric-equity-detail').textContent = `Fresh account proof ${age(account.updated_at_ms)}`;
    byId('metric-cash').textContent = money(account.collateral_balance_usdc);
    byId('metric-cash-detail').textContent = 'Authenticated collateral balance';
    byId('metric-orders').textContent = String(account.open_order_count ?? 0);
    byId('metric-orders-detail').textContent = 'Authoritative CLOB open-order snapshot';
    byId('metric-loss').textContent = money(account.risk_state?.daily_loss_usdc || 0);
    byId('metric-loss-detail').textContent = 'Measured against persisted UTC-day baseline';

    const positions = (account.positions || []).filter((position) => Number(position.size) !== 0);
    byId('positions-empty').hidden = positions.length > 0;
    byId('positions-empty').textContent = 'No live positions.';
    byId('position-list').replaceChildren(...positions.map((position) => {
      const row = document.createElement('article');
      const identity = document.createElement('div');
      const asset = document.createElement('strong');
      const basis = document.createElement('small');
      asset.textContent = short(position.asset_id, 10, 6);
      basis.textContent = `Average ${number(position.average_price, 4)} · value ${money(position.current_value_usdc)}`;
      identity.append(asset, basis);
      const size = document.createElement('b');
      size.textContent = number(position.size, 3);
      row.append(identity, size);
      return row;
    }));
    const resting = account.risk_state?.own_resting_orders || [];
    const rows = resting.slice(0, 20).map((order) => {
      const row = document.createElement('tr');
      row.append(
        cell(short(order.asset_id, 9, 5), 'mono'),
        cell(String(order.side || missing).toUpperCase()),
        cell(number(order.price, 4), 'numeric'),
        cell(number(order.size, 3), 'numeric'),
      );
      return row;
    });
    replaceRows(byId('order-body'), rows, 4, 'No live open orders.');
  }

  function renderFills(fills, liveMode) {
    const rows = (fills || []).slice(0, 20).map((fill) => {
      const row = document.createElement('tr');
      row.append(
        cell(time(fill.filled_at_ms)),
        cell(String(fill.side || missing).toUpperCase()),
        cell(number(fill.price, 4), 'numeric'),
        cell(number(fill.size, 3), 'numeric'),
      );
      return row;
    });
    replaceRows(
      byId('fill-body'), rows, 4,
      liveMode ? 'No authenticated live fills observed.' : 'No simulated fills observed.',
    );
  }

  function renderSignals(runtime) {
    const quotes = Object.values(runtime.external_quotes || {});
    const quoteList = byId('quote-list');
    if (!quotes.length) {
      const empty = document.createElement('p');
      empty.className = 'empty-copy';
      empty.textContent = 'External signal feed is disabled or waiting.';
      quoteList.replaceChildren(empty);
    } else {
      quoteList.replaceChildren(...quotes.map((quote) => {
        const row = document.createElement('article');
        const identity = document.createElement('div');
        const name = document.createElement('strong');
        const venue = document.createElement('small');
        name.textContent = quote.symbol;
        venue.textContent = `${quote.venue} · ${age(quote.local_received_at_ms)}`;
        identity.append(name, venue);
        const price = document.createElement('b');
        price.textContent = money(quote.price);
        row.append(identity, price);
        if (quote.sequence_gap) row.dataset.warning = 'true';
        return row;
      }));
    }

    const walletTrades = runtime.recent_wallet_trades || [];
    const walletList = byId('wallet-list');
    if (!walletTrades.length) {
      const empty = document.createElement('p');
      empty.className = 'empty-copy';
      empty.textContent = 'No new watched-wallet trades. Historical trades are never replayed.';
      walletList.replaceChildren(empty);
    } else {
      walletList.replaceChildren(...walletTrades.slice(0, 12).map((trade) => {
        const row = document.createElement('article');
        const identity = document.createElement('div');
        const title = document.createElement('strong');
        const wallet = document.createElement('small');
        title.textContent = `${String(trade.side).toUpperCase()} ${trade.outcome || 'outcome'}`;
        wallet.textContent = `${short(trade.wallet, 8, 5)} · ${age(trade.timestamp_ms)}`;
        identity.append(title, wallet);
        const value = document.createElement('b');
        value.textContent = `${number(trade.size, 2)} @ ${number(trade.price, 4)}`;
        row.append(identity, value);
        return row;
      }));
    }
  }

  function renderLatency(latency) {
    const entries = Object.entries(latency || {});
    const list = byId('latency-list');
    if (!entries.length) {
      const empty = document.createElement('p');
      empty.className = 'empty-copy';
      empty.textContent = 'No latency samples yet.';
      list.replaceChildren(empty);
      return;
    }
    list.replaceChildren(...entries.map(([name, summary]) => {
      const row = document.createElement('article');
      const identity = document.createElement('div');
      const label = document.createElement('strong');
      const samples = document.createElement('small');
      label.textContent = name.replaceAll('_', ' ');
      samples.textContent = `${summary.samples} sample${summary.samples === 1 ? '' : 's'}`;
      identity.append(label, samples);
      const metrics = document.createElement('b');
      metrics.textContent = `p50 ${formatNumber(summary.p50_us / 1000, 2)}ms · p95 ${formatNumber(summary.p95_us / 1000, 2)}ms`;
      row.append(identity, metrics);
      return row;
    }));
  }

  function renderEvents(events) {
    const rows = (events || []).slice(0, 50).map((event) => {
      const row = document.createElement('tr');
      row.append(
        cell(time(event.timestamp_ms)),
        cell(String(event.level || 'info').toUpperCase(), `level-${event.level || 'info'}`),
        cell(event.component || 'runtime', 'mono'),
        cell(event.message || ''),
      );
      return row;
    });
    replaceRows(byId('event-body'), rows, 4, 'Waiting for runtime events.');
  }

  function renderReadiness(startup) {
    const readiness = startup.readiness || [];
    const passed = readiness.filter((gate) => gate.passed).length;
    const locked = readiness.length - passed;
    byId('gates-summary').textContent = `${passed} / ${readiness.length}`;
    byId('readiness-count').textContent = readiness.length
      ? `${locked} LOCKED`
      : 'VERIFYING';
    const list = byId('readiness-list');
    list.replaceChildren(...readiness.map((gate) => {
      const row = document.createElement('article');
      row.dataset.passed = String(Boolean(gate.passed));
      const identity = document.createElement('div');
      const name = document.createElement('strong');
      const detail = document.createElement('small');
      name.textContent = gate.name;
      detail.textContent = gate.detail;
      identity.append(name, detail);
      const result = document.createElement('b');
      result.textContent = gate.passed ? 'PASS' : 'LOCK';
      row.append(identity, result);
      return row;
    }));
    if (!readiness.length) {
      const empty = document.createElement('p');
      empty.className = 'empty-copy';
      empty.textContent = 'No startup readiness report.';
      list.replaceChildren(empty);
    }
    const reasons = startup.live_lock_reasons || [];
    byId('lock-reasons').replaceChildren(...(reasons.length ? reasons : ['No static launch locks reported.']).map((reason) => {
      const item = document.createElement('li');
      item.textContent = reason;
      return item;
    }));
  }

  function render(payload) {
    const runtime = payload.runtime || {};
    const startup = payload.startup || {};
    const mode = String(startup.mode || runtime.mode || 'unknown').toUpperCase();
    const phase = String(runtime.phase || 'starting').toUpperCase();
    byId('mode-pill').textContent = mode;
    byId('phase-pill').textContent = phase;
    byId('phase-pill').className = `status-chip phase-${runtime.phase || 'starting'}`;
    byId('updated-at').textContent = `${time(runtime.updated_at_ms)} · ${age(runtime.updated_at_ms)}`;
    byId('version-label').textContent = `v${startup.version || missing}`;
    byId('runtime-summary').textContent = runtime.paused
      ? 'The strategy is paused. Data ingestion, accounting, and safety verification continue.'
      : `The ${mode === 'LIVE' ? 'live' : 'paper'} strategy is active. Every intent still passes freshness, risk, compliance, rate, and journal gates.`;
    const liveLock = byId('live-lock');
    liveLock.querySelector('strong').textContent = runtime.live_submission_enabled ? 'ENABLED' : 'LOCKED';
    liveLock.querySelector('i').className = runtime.live_submission_enabled
      ? 'ti ti-circle-check'
      : 'ti ti-lock';
    liveLock.dataset.enabled = String(Boolean(runtime.live_submission_enabled));

    const notice = byId('notice');
    const noticeIcon = byId('notice-icon');
    if (runtime.phase === 'running') {
      notice.className = 'notice success';
      noticeIcon.className = 'ti ti-circle-check';
      byId('notice-title').textContent = `${mode === 'LIVE' ? 'Live' : 'Paper'} runtime active`;
      byId('notice-copy').textContent = mode === 'LIVE'
        ? 'Authenticated account state, verified books, and all readiness gates are active.'
        : 'Verified books and all configured gates are driving the simulation.';
    } else if (runtime.phase === 'paused') {
      notice.className = 'notice neutral';
      noticeIcon.className = 'ti ti-player-pause';
      byId('notice-title').textContent = 'Strategy paused';
      byId('notice-copy').textContent = 'Feeds and accounting remain active; no new strategy orders are created.';
    } else {
      notice.className = 'notice warning';
      noticeIcon.className = 'ti ti-alert-triangle';
      byId('notice-title').textContent = runtime.phase === 'degraded' ? 'Runtime degraded' : 'Starting safely';
      byId('notice-copy').textContent = runtime.phase === 'degraded'
        ? 'One or more authoritative inputs are unavailable. New orders remain blocked by downstream gates.'
        : 'Feeds, market metadata, and the journal are being verified.';
    }

    renderFeed('market_feed', runtime.market_feed);
    renderFeed('user_feed', runtime.user_feed);
    renderFeed('external_feed', runtime.external_feed);
    renderFeed('wallet_feed', runtime.wallet_feed);
    byId('compliance-status').textContent = String(runtime.compliance_status || 'unverified').toUpperCase();
    byId('compliance-location').textContent = runtime.compliance_location || 'Location unavailable';
    byId('catalog-status').textContent = runtime.catalog_ready ? 'VERIFIED' : 'VERIFYING';
    byId('journal-sequence').textContent = String(runtime.journal_next_sequence ?? missing);
    byId('journal-hash').textContent = runtime.journal_last_hash
      ? `BLAKE3 ${short(runtime.journal_last_hash, 12, 8)}`
      : 'hash unavailable';
    byId('metric-submitted').textContent = String(runtime.submitted_orders ?? 0);
    byId('metric-rejected').textContent = `${runtime.rejected_intents ?? 0} rejected intent${runtime.rejected_intents === 1 ? '' : 's'}`;

    byId('orders-title').textContent = mode === 'LIVE' ? 'Open live orders' : 'Open paper orders';
    byId('fills-kicker').textContent = mode === 'LIVE' ? 'AUTHENTICATED USER STREAM' : 'SIMULATED MATCHING';
    if (mode === 'LIVE') renderLiveAccount(runtime.live_account);
    else {
      byId('portfolio-title').textContent = 'Paper portfolio';
      renderPortfolio(runtime.portfolio);
    }
    renderMarkets(runtime);
    renderFills(runtime.recent_fills, mode === 'LIVE');
    renderSignals(runtime);
    renderLatency(runtime.latency);
    renderEvents(runtime.recent_events);
    renderReadiness(startup);

    document.querySelectorAll('[data-command]').forEach((button) => {
      const command = button.dataset.command;
      button.disabled =
        (command === 'resume_paper' && (mode !== 'PAPER' || !runtime.paused))
        || (command === 'resume_live' && (mode !== 'LIVE' || !runtime.paused || !runtime.live_submission_enabled))
        || (command === 'pause' && runtime.paused)
        || (command === 'flatten_paper' && mode !== 'PAPER');
    });
  }

  const toast = (message, tone = 'info') => {
    const node = byId('toast');
    node.textContent = message;
    node.dataset.tone = tone;
    node.classList.add('visible');
    clearTimeout(toast.timer);
    toast.timer = setTimeout(() => node.classList.remove('visible'), 5000);
  };

  async function refresh() {
    try {
      const response = await fetch('/api/status', {
        cache: 'no-store',
        headers: { 'X-Control-Token': token },
      });
      if (!response.ok) throw new Error(`status request failed (${response.status})`);
      render(await response.json());
      document.body.dataset.loading = 'false';
      document.body.dataset.connected = 'true';
      byId('connection-label').textContent = 'ONLINE';
    } catch (error) {
      document.body.dataset.loading = 'false';
      document.body.dataset.connected = 'false';
      byId('connection-label').textContent = 'OFFLINE';
      byId('phase-pill').textContent = 'STATUS UNAVAILABLE';
      byId('phase-pill').className = 'status-chip phase-degraded';
      toast(error.message || 'Runtime status unavailable', 'error');
    }
  }

  async function command(action, button) {
    button.disabled = true;
    button.setAttribute('aria-busy', 'true');
    let refreshed = false;
    try {
      const body = new URLSearchParams({ action, token });
      const response = await fetch('/api/command', { method: 'POST', body });
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.error || 'Command rejected');
      toast(`${payload.message} (${payload.affected_orders} affected)`, 'success');
      if (action === 'shutdown') {
        clearInterval(statusPoll);
        document.body.dataset.connected = 'false';
        byId('connection-label').textContent = 'STOPPING';
        byId('phase-pill').textContent = 'SHUTTING DOWN';
        byId('phase-pill').className = 'status-chip phase-shutting_down';
        byId('runtime-summary').textContent = 'Graceful shutdown accepted. Open orders are being cancelled and durable state is being flushed.';
        document.querySelectorAll('[data-command]').forEach((control) => {
          control.disabled = true;
        });
        return;
      }
      await refresh();
      refreshed = true;
    } catch (error) {
      toast(error.message || 'Command rejected', 'error');
    } finally {
      button.removeAttribute('aria-busy');
      if (action !== 'shutdown' && !refreshed) button.disabled = false;
    }
  }

  document.querySelectorAll('[data-command]').forEach((button) => {
    button.addEventListener('click', () => command(button.dataset.command, button));
  });

  const clockFormat = new Intl.DateTimeFormat(undefined, {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hourCycle: 'h23',
  });
  const utcClockFormat = new Intl.DateTimeFormat('en-GB', {
    timeZone: 'UTC',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hourCycle: 'h23',
  });
  const refreshClocks = () => {
    const now = new Date();
    byId('clock-local').textContent = clockFormat.format(now);
    byId('clock-utc').textContent = utcClockFormat.format(now);
  };

  const navLinks = Array.from(document.querySelectorAll('.rail a[href^="#"]'));
  const setActiveSection = (id) => {
    navLinks.forEach((link) => {
      const active = link.getAttribute('href') === `#${id}`;
      link.classList.toggle('active', active);
      if (active) link.setAttribute('aria-current', 'location');
      else link.removeAttribute('aria-current');
    });
  };
  if ('IntersectionObserver' in window) {
    const sectionObserver = new IntersectionObserver((entries) => {
      const visible = entries.find((entry) => entry.isIntersecting);
      if (visible) setActiveSection(visible.target.id);
    }, { rootMargin: '-18% 0px -70% 0px' });
    navLinks.forEach((link) => {
      const target = document.querySelector(link.getAttribute('href'));
      if (target) sectionObserver.observe(target);
    });
  }

  byId('runtime-origin').textContent = window.location.host;
  refreshClocks();
  setInterval(refreshClocks, 1000);
  refresh();
  const statusPoll = setInterval(refresh, 1000);
})();
