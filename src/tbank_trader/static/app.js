const appState = {
  dashboard: null,
  brokerStatus: null,
  simulation: { run: null, rebalances: [] },
};

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function numberOrNull(value) {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : null;
}

function formatNumber(value, digits = 2) {
  const numeric = numberOrNull(value);
  if (numeric === null) {
    return "n/a";
  }
  return numeric.toLocaleString("ru-RU", {
    minimumFractionDigits: 0,
    maximumFractionDigits: digits,
  });
}

function formatCurrency(value, digits = 2) {
  const numeric = numberOrNull(value);
  if (numeric === null) {
    return "n/a";
  }
  return `${formatNumber(numeric, digits)} ₽`;
}

function formatPercent(value, digits = 2) {
  const numeric = numberOrNull(value);
  if (numeric === null) {
    return "n/a";
  }
  return `${numeric >= 0 ? "+" : ""}${formatNumber(numeric, digits)}%`;
}

function parseDate(value) {
  if (!value || value === "n/a") {
    return null;
  }
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? null : date;
}

function formatDateTime(value) {
  const date = parseDate(value);
  if (!date) {
    return "n/a";
  }
  return new Intl.DateTimeFormat("ru-RU", {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  }).format(date);
}

function formatDateShort(value) {
  const date = parseDate(value);
  if (!date) {
    return "n/a";
  }
  return new Intl.DateTimeFormat("ru-RU", {
    day: "2-digit",
    month: "2-digit",
    year: "numeric",
  }).format(date);
}

function formatRelativeCountdown(value) {
  const date = parseDate(value);
  if (!date) {
    return "n/a";
  }

  let diff = Math.max(0, date.getTime() - Date.now());
  const days = Math.floor(diff / (24 * 60 * 60 * 1000));
  diff -= days * 24 * 60 * 60 * 1000;
  const hours = Math.floor(diff / (60 * 60 * 1000));
  diff -= hours * 60 * 60 * 1000;
  const minutes = Math.floor(diff / (60 * 1000));

  const parts = [];
  if (days > 0) {
    parts.push(`${days} д`);
  }
  if (hours > 0 || days > 0) {
    parts.push(`${hours} ч`);
  }
  parts.push(`${minutes} мин`);
  return parts.join(" ");
}

function formatSignedCurrency(value) {
  const numeric = numberOrNull(value);
  if (numeric === null) {
    return "n/a";
  }
  const prefix = numeric > 0 ? "+" : "";
  return `${prefix}${formatNumber(numeric, 2)} ₽`;
}

function toneFromNumeric(value) {
  const numeric = numberOrNull(value);
  if (numeric === null) {
    return "";
  }
  if (numeric > 0) {
    return "text-success";
  }
  if (numeric < 0) {
    return "text-danger";
  }
  return "";
}

function tag(label, tone = "info") {
  return `<span class="tag ${tone}">${escapeHtml(label)}</span>`;
}

function safeArray(value) {
  return Array.isArray(value) ? value : [];
}

function parseSymbolList(value) {
  return String(value || "")
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

function parsePositionsSummary(value) {
  return String(value || "")
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean)
    .map((item) => {
      const [symbol, quantityRaw] = item.split(":");
      return {
        symbol: symbol || "",
        quantity: Number(quantityRaw || 0),
      };
    })
    .filter((row) => row.symbol && Number.isFinite(row.quantity));
}

function parseTargetWeights(value) {
  const result = {};
  for (const item of String(value || "").split(",")) {
    const trimmed = item.trim();
    if (!trimmed) {
      continue;
    }
    const [symbol, weightRaw] = trimmed.split(":");
    const numericWeight = Number(weightRaw);
    if (symbol && Number.isFinite(numericWeight)) {
      result[symbol] = numericWeight;
    }
  }
  return result;
}

function findBrokerInstrument(symbol, brokerStatus) {
  return safeArray(brokerStatus?.symbols).find((row) => row.symbol === symbol) || null;
}

function findBrokerPosition(symbol, dashboard) {
  return safeArray(dashboard?.positions).find((row) => row.symbol === symbol) || null;
}

function currentPortfolioState(dashboard) {
  return dashboard?.portfolio_live || {};
}

function latestSimulationRebalances(simulation) {
  return safeArray(simulation?.rebalances)
    .slice()
    .sort((left, right) => new Date(left.rebalance_time).getTime() - new Date(right.rebalance_time).getTime());
}

function computeStrategyPortfolioView(dashboard, brokerStatus) {
  const portfolio = currentPortfolioState(dashboard);
  const equity = numberOrNull(portfolio.equity_rub) || 0;
  const cash = numberOrNull(portfolio.cash_rub) || 0;
  const selectedSymbols = new Set(parseSymbolList(portfolio.selected_symbols));
  const targetWeights = parseTargetWeights(portfolio.target_weights);
  const strategyPositions = parsePositionsSummary(portfolio.positions);

  const rows = strategyPositions.map((strategyRow) => {
    const brokerInstrument = findBrokerInstrument(strategyRow.symbol, brokerStatus);
    const brokerPosition = findBrokerPosition(strategyRow.symbol, dashboard);
    const lot = Math.max(Number(brokerInstrument?.lot || 1), 1);
    const avgPrice = numberOrNull(brokerPosition?.avg_price) || numberOrNull(brokerInstrument?.price) || 0;
    const currentPrice = numberOrNull(brokerPosition?.market_price) || numberOrNull(brokerInstrument?.price) || 0;
    const notional = strategyRow.quantity * lot * currentPrice;
    const pnlRub = strategyRow.quantity * lot * (currentPrice - avgPrice);
    const pnlPct = avgPrice > 0 ? ((currentPrice / avgPrice) - 1) * 100 : 0;

    let signal = "Держать";
    if (!selectedSymbols.has(strategyRow.symbol)) {
      signal = "Продать";
    } else if ((targetWeights[strategyRow.symbol] || 0) <= 0) {
      signal = "Проверить";
    }

    return {
      symbol: strategyRow.symbol,
      weightPct: equity > 0 ? (notional / equity) * 100 : 0,
      quantity: strategyRow.quantity,
      lot,
      avgPrice,
      currentPrice,
      pnlRub,
      pnlPct,
      signal,
      notional,
    };
  });

  const investedNotional = rows.reduce((sum, row) => sum + row.notional, 0);
  const totalPnl = rows.reduce((sum, row) => sum + row.pnlRub, 0);
  const investedWeightPct = equity > 0 ? (investedNotional / equity) * 100 : 0;
  const cashWeightPct = equity > 0 ? (cash / equity) * 100 : 0;

  return {
    rows,
    equity,
    cash,
    investedNotional,
    totalPnl,
    investedWeightPct,
    cashWeightPct,
    selectedSymbols,
  };
}

function computeRunDrawdownPct(dashboard) {
  const portfolio = currentPortfolioState(dashboard);
  const points = safeArray(dashboard?.portfolio_rebalances)
    .map((row) => numberOrNull(row.equity_rub))
    .filter((value) => value !== null);
  if (numberOrNull(portfolio.equity_rub) !== null) {
    points.push(Number(portfolio.equity_rub));
  }
  if (!points.length) {
    return null;
  }

  let peak = points[0];
  let currentDrawdownPct = 0;
  for (const point of points) {
    peak = Math.max(peak, point);
    currentDrawdownPct = peak > 0 ? ((point / peak) - 1) * 100 : 0;
  }
  return currentDrawdownPct;
}

function computeSimulationStatistics(simulation) {
  const points = latestSimulationRebalances(simulation)
    .map((row) => numberOrNull(row.equity_rub))
    .filter((value) => value !== null);
  if (points.length < 2) {
    return { volatilityPct: null, sharpe: null };
  }

  const returns = [];
  for (let index = 1; index < points.length; index += 1) {
    const previous = points[index - 1];
    const current = points[index];
    if (previous > 0) {
      returns.push((current / previous) - 1);
    }
  }
  if (!returns.length) {
    return { volatilityPct: null, sharpe: null };
  }

  const mean = returns.reduce((sum, value) => sum + value, 0) / returns.length;
  const variance =
    returns.reduce((sum, value) => sum + (value - mean) ** 2, 0) / Math.max(returns.length - 1, 1);
  const std = Math.sqrt(Math.max(variance, 0));
  const annualizationFactor = Math.sqrt(52);
  const volatilityPct = std * annualizationFactor * 100;
  const sharpe = std > 0 ? (mean / std) * annualizationFactor : null;
  return { volatilityPct, sharpe };
}

function simulationRegimeState(simulation) {
  const latestCompleted = latestSimulationRebalances(simulation)
    .slice()
    .reverse()
    .find((row) => row.regime_state && row.regime_state !== "waiting");
  return latestCompleted || null;
}

function rebalanceProgress(dashboard) {
  const portfolio = currentPortfolioState(dashboard);
  const last = parseDate(portfolio.last_rebalance_at);
  const next = parseDate(portfolio.next_rebalance_at);
  if (!last || !next) {
    return 0;
  }
  const total = next.getTime() - last.getTime();
  if (total <= 0) {
    return 0;
  }
  const progress = (Date.now() - last.getTime()) / total;
  return Math.max(0, Math.min(progress, 1));
}

function metricTile(label, value, note = "", tone = "") {
  return `
    <article class="mini-metric">
      <div class="mini-metric-label">${escapeHtml(label)}</div>
      <div class="mini-metric-value ${tone}">${escapeHtml(value)}</div>
      ${note ? `<div class="mini-metric-note">${escapeHtml(note)}</div>` : ""}
    </article>
  `;
}

function renderTable(targetId, columns, rows, emptyMessage = "Пока нет данных.") {
  const target = document.getElementById(targetId);
  if (!rows.length) {
    target.innerHTML = `<p class="empty-state">${escapeHtml(emptyMessage)}</p>`;
    return;
  }

  const head = columns.map((column) => `<th class="${column.numeric ? "numeric" : ""}">${escapeHtml(column.label)}</th>`).join("");
  const body = rows
    .map((row) => {
      const tds = columns
        .map((column) => {
          const rendered = column.render ? column.render(row[column.key], row) : escapeHtml(row[column.key]);
          return `<td class="${column.numeric ? "numeric" : ""}">${rendered}</td>`;
        })
        .join("");
      return `<tr>${tds}</tr>`;
    })
    .join("");

  target.innerHTML = `<div class="table-wrap"><table><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table></div>`;
}

function renderHeroSummary(dashboard, brokerStatus, simulation) {
  const system = dashboard.system || {};
  const portfolio = currentPortfolioState(dashboard);
  const simulationRun = simulation?.run || {};
  const target = document.getElementById("hero-summary");
  const environmentBadge = document.getElementById("environment-badge");
  const updatedAt = document.getElementById("updated-at");

  environmentBadge.textContent = system.broker_mode === "tbank" ? "Режим: Песочница" : "Режим: Симуляция";
  updatedAt.textContent = formatDateTime(system.engine_last_heartbeat || new Date().toISOString());

  target.innerHTML = [
    `<div class="summary-chip"><strong>Сервис</strong> ${escapeHtml(window.appName || "tbank-trader")}</div>`,
    `<div class="summary-chip"><strong>Брокер</strong> ${escapeHtml(system.broker_mode || "n/a")}</div>`,
    `<div class="summary-chip"><strong>Счёт</strong> ${escapeHtml(brokerStatus?.account_id || "n/a")}</div>`,
    `<div class="summary-chip"><strong>Universe</strong> ${escapeHtml(system.universe_eligible_size || 0)} инструментов</div>`,
    `<div class="summary-chip"><strong>History Coverage</strong> ${escapeHtml(system.historical_covered_instruments || 0)}</div>`,
    `<div class="summary-chip"><strong>Последний Sim Run</strong> ${escapeHtml(simulationRun.id || "n/a")}</div>`,
    `<div class="summary-chip"><strong>Статус Live</strong> ${portfolio.enabled ? "включен" : "выключен"}</div>`,
  ].join("");
}

function renderCurrentPortfolio(dashboard, brokerStatus) {
  const target = document.getElementById("current-portfolio");
  const portfolioView = computeStrategyPortfolioView(dashboard, brokerStatus);

  const rows = portfolioView.rows.map((row) => ({
    ...row,
    pnlClass: toneFromNumeric(row.pnlRub),
  }));

  const allocation = `
    <div class="allocation-row">
      <div class="allocation-label">
        Акции и позиции стратегии: ${formatNumber(portfolioView.investedWeightPct, 1)}% |
        Кэш: ${formatNumber(portfolioView.cashWeightPct, 1)}%
      </div>
      <div class="mini-badge">${portfolioView.rows.length} поз.</div>
    </div>
    <div class="allocation-bar">
      <span class="allocation-invested" style="width:${Math.max(0, Math.min(portfolioView.investedWeightPct, 100))}%"></span>
      <span class="allocation-cash" style="width:${Math.max(0, Math.min(portfolioView.cashWeightPct, 100))}%"></span>
    </div>
  `;

  target.innerHTML = `
    ${allocation}
    <div id="current-portfolio-table"></div>
    <div class="action-inline">
      <button class="danger" id="portfolio-stop-btn">Принудительный стоп</button>
    </div>
  `;

  const tableRows = rows.map((row) => ({
    ticker: row.symbol,
    weight: row.weightPct,
    lots: row.quantity,
    entry: row.avgPrice,
    current: row.currentPrice,
    pnlRub: row.pnlRub,
    pnlPct: row.pnlPct,
    signal: row.signal,
  }));

  if (portfolioView.cash > 0 || tableRows.length) {
    tableRows.push({
      ticker: "КЭШ",
      weight: portfolioView.cashWeightPct,
      lots: "-",
      entry: null,
      current: null,
      pnlRub: 0,
      pnlPct: 0,
      signal: "-",
    });
  }

  tableRows.push({
    ticker: "ИТОГО",
    weight: 100,
    lots: "—",
    entry: null,
    current: null,
    pnlRub: portfolioView.totalPnl,
    pnlPct: portfolioView.equity > 0 ? (portfolioView.totalPnl / portfolioView.equity) * 100 : 0,
    signal: "-",
  });

  renderTable(
    "current-portfolio-table",
    [
      { key: "ticker", label: "Тикер" },
      { key: "weight", label: "Вес", numeric: true, render: (value) => `${formatNumber(value, 1)}%` },
      { key: "lots", label: "Лоты", numeric: true },
      { key: "entry", label: "Цена входа", numeric: true, render: (value) => value == null ? "—" : formatNumber(value, 4) },
      { key: "current", label: "Тек. цена", numeric: true, render: (value) => value == null ? "—" : formatNumber(value, 4) },
      {
        key: "pnlRub",
        label: "PnL (руб)",
        numeric: true,
        render: (value) => `<span class="${toneFromNumeric(value)}">${escapeHtml(formatSignedCurrency(value))}</span>`,
      },
      {
        key: "pnlPct",
        label: "PnL (%)",
        numeric: true,
        render: (value) => `<span class="${toneFromNumeric(value)}">${escapeHtml(formatPercent(value, 2))}</span>`,
      },
      {
        key: "signal",
        label: "Сигнал",
        render: (value) => {
          const tone =
            value === "Держать" ? "success" : value === "Продать" ? "danger" : value === "Кэш" ? "warning" : "info";
          return tag(value, tone);
        },
      },
    ],
    tableRows,
    "Стратегия ещё не открыла позиции. Пока весь бюджет находится в кэше.",
  );
}

function renderLiveMetrics(dashboard, simulation, brokerStatus) {
  const target = document.getElementById("live-metrics");
  const system = dashboard.system || {};
  const portfolio = currentPortfolioState(dashboard);
  const portfolioView = computeStrategyPortfolioView(dashboard, brokerStatus);
  const simulationRun = simulation?.run || {};
  const { volatilityPct, sharpe } = computeSimulationStatistics(simulation);
  const currentDrawdownPct = computeRunDrawdownPct(dashboard);
  const startCapital = numberOrNull(simulationRun.initial_cash_rub) || 100000;
  const currentEquity = numberOrNull(portfolio.equity_rub) || 0;
  const liveReturnPct = startCapital > 0 ? ((currentEquity / startCapital) - 1) * 100 : 0;
  const avgTurnover =
    numberOrNull(simulationRun.turnover_rub) !== null && Number(simulationRun.completed_rebalances) > 0
      ? Number(simulationRun.turnover_rub) / Number(simulationRun.completed_rebalances)
      : null;
  const filterState = portfolio.regime_state === "on" ? "РЫНОК ОК" : "ЗАЩИТА / КЭШ";
  const filterTone = portfolio.regime_state === "on" ? "success" : "danger";

  target.innerHTML = `
    <div class="stack">
      <div class="status-flag">
        <span class="status-dot ${filterTone}"></span>
        <span>${escapeHtml(filterState)}</span>
      </div>
      <div class="mini-metrics">
        ${metricTile("Стартовый капитал", formatCurrency(startCapital, 0), "Базовый бюджет стратегии")}
        ${metricTile("Текущий эквити", formatCurrency(currentEquity, 2), "По live ledger", toneFromNumeric(liveReturnPct))}
        ${metricTile("Доходность LIVE", formatPercent(liveReturnPct, 2), "От стартового бюджета", toneFromNumeric(liveReturnPct))}
        ${metricTile("Просадка run", currentDrawdownPct == null ? "n/a" : formatPercent(currentDrawdownPct, 2), "От пика текущего tracked run", toneFromNumeric(currentDrawdownPct))}
        ${metricTile("Волатильность proxy", volatilityPct == null ? "n/a" : formatPercent(volatilityPct, 1), "Оценка по последнему historical run")}
        ${metricTile("Sharpe proxy", sharpe == null ? "n/a" : formatNumber(sharpe, 2), "На основе historical simulation")}
        ${metricTile("Сделок стратегии", String(safeArray(dashboard.portfolio_trades).length), "Записано в ledger")}
        ${metricTile("Средний оборот rebalance", avgTurnover == null ? "n/a" : formatCurrency(avgTurnover, 0), "По последнему historical simulation")}
      </div>
      <div class="muted-line">
        Текущий режим: ${escapeHtml(portfolio.regime_state || "n/a")}. Брокерский баланс RUB:
        ${formatCurrency(brokerStatus?.rub_balance, 2)}. Активных позиций стратегии:
        ${escapeHtml(portfolioView.rows.length)}.
      </div>
    </div>
  `;
}

function filterStatusTag(ok, waiting = false) {
  if (waiting) {
    return tag("ОЖИДАНИЕ", "warning");
  }
  return ok ? tag("ОК", "success") : tag("OFF", "danger");
}

function renderSignalsFilters(dashboard, simulation) {
  const target = document.getElementById("signals-filters");
  const system = dashboard.system || {};
  const portfolio = currentPortfolioState(dashboard);
  const progressPct = rebalanceProgress(dashboard) * 100;
  const currentDrawdownPct = computeRunDrawdownPct(dashboard);
  const simRegime = simulationRegimeState(simulation);
  const selected = parseSymbolList(portfolio.selected_symbols);
  const targetWeights = parseTargetWeights(portfolio.target_weights);

  target.innerHTML = `
    <div class="stack">
      <div class="countdown-card">
        <div class="countdown-title">Следующая ребалансировка</div>
        <div class="countdown-main">
          ${escapeHtml(formatDateTime(portfolio.next_rebalance_at || "n/a"))}
          <span class="subtle"> (через ${escapeHtml(formatRelativeCountdown(portfolio.next_rebalance_at || "n/a"))})</span>
        </div>
        <div class="progress-rail">
          <div class="progress-fill" style="width:${formatNumber(progressPct, 2)}%"></div>
        </div>
        <div class="muted-line">Прогресс текущего окна ребаланса: ${formatNumber(progressPct, 0)}%</div>
      </div>

      <div id="filters-table"></div>

      <div class="filter-summary">
        <div class="section-title">Предварительный состав на следующую ребалансировку</div>
        ${
          selected.length
            ? `<div class="selection-preview">
                ${selected
                  .map((symbol) => {
                    const weight = targetWeights[symbol];
                    return `<span class="selection-pill">${escapeHtml(symbol)} · ${formatNumber((weight || 0) * 100, 1)}%</span>`;
                  })
                  .join("")}
              </div>`
            : '<p class="empty-state">Новых кандидатов пока нет: стратегия либо ждёт достаточно истории, либо фильтр режима держит портфель в кэше.</p>'
        }
      </div>
    </div>
  `;

  const filterRows = [
    {
      filter: "Тренд live benchmark",
      status: portfolio.regime_state === "waiting" ? filterStatusTag(false, true) : filterStatusTag(portfolio.regime_state === "on"),
      current: portfolio.regime_reason || "n/a",
      threshold: system.portfolio_regime_symbol || "n/a",
    },
    {
      filter: "SPX/VIX parity filter",
      status: simRegime ? filterStatusTag(simRegime.regime_state === "on") : filterStatusTag(false, true),
      current: simRegime?.reason || "Нет завершенного sim-режима",
      threshold: "SPX > MA и VIX < порога",
    },
    {
      filter: "Просадка текущего run",
      status: currentDrawdownPct == null ? filterStatusTag(false, true) : filterStatusTag(currentDrawdownPct > -10),
      current: currentDrawdownPct == null ? "n/a" : formatPercent(currentDrawdownPct, 2),
      threshold: "-10%",
    },
    {
      filter: "Историческое покрытие",
      status: filterStatusTag(Number(system.history_ready_instruments || 0) > 0),
      current: `${escapeHtml(system.historical_covered_instruments || 0)} покрыто / ${escapeHtml(system.universe_eligible_size || 0)} eligible`,
      threshold: "history_ready > 0",
    },
    {
      filter: "Итоговый режим",
      status: portfolio.regime_state === "on" ? tag("ИНВЕСТИРОВАН", "success") : tag("ЗАЩИТА / КЭШ", "danger"),
      current: portfolio.selected_symbols || "корзина пустая",
      threshold: "live routing",
    },
  ];

  renderTable(
    "filters-table",
    [
      { key: "filter", label: "Фильтр" },
      { key: "status", label: "Статус", render: (value) => value },
      { key: "current", label: "Текущее значение" },
      { key: "threshold", label: "Порог / правило" },
    ],
    filterRows,
    "Фильтры ещё не рассчитаны.",
  );
}

function buildEquityChartSvg(simulation, dashboard) {
  const rebalances = latestSimulationRebalances(simulation);
  const portfolio = currentPortfolioState(dashboard);
  const points = rebalances
    .map((row) => ({
      time: parseDate(row.rebalance_time),
      equity: numberOrNull(row.equity_rub),
      status: row.status,
      regimeState: row.regime_state,
    }))
    .filter((row) => row.time && row.equity !== null);

  const livePointTime = parseDate(dashboard?.system?.engine_last_heartbeat || new Date().toISOString()) || new Date();
  const livePointEquity = numberOrNull(portfolio.equity_rub);
  if (livePointEquity !== null) {
    points.push({
      time: livePointTime,
      equity: livePointEquity,
      status: "live",
      regimeState: portfolio.regime_state || "n/a",
    });
  }

  if (points.length < 2) {
    return null;
  }

  const width = 920;
  const height = 320;
  const padX = 28;
  const padY = 22;
  const dataMinX = points[0].time.getTime();
  const dataMaxX = points[points.length - 1].time.getTime();
  const historicalOnly = points.slice(0, -1);
  const dataMinY = Math.min(...points.map((point) => point.equity));
  const dataMaxY = Math.max(...points.map((point) => point.equity));
  const paddedMinY = dataMinY * 0.998;
  const paddedMaxY = dataMaxY * 1.002;
  const rangeY = Math.max(paddedMaxY - paddedMinY, 1);
  const rangeX = Math.max(dataMaxX - dataMinX, 1);

  const xScale = (time) => padX + ((time - dataMinX) / rangeX) * (width - padX * 2);
  const yScale = (equity) => height - padY - ((equity - paddedMinY) / rangeY) * (height - padY * 2);

  const historicalPolyline = historicalOnly
    .map((point) => `${xScale(point.time.getTime())},${yScale(point.equity)}`)
    .join(" ");

  const liveLine =
    historicalOnly.length && livePointEquity !== null
      ? {
          x1: xScale(historicalOnly[historicalOnly.length - 1].time.getTime()),
          y1: yScale(historicalOnly[historicalOnly.length - 1].equity),
          x2: xScale(livePointTime.getTime()),
          y2: yScale(livePointEquity),
        }
      : null;

  const regimeRects = historicalOnly
    .filter((point) => point.regimeState === "off" || point.status === "regime_off")
    .map((point) => {
      const x = xScale(point.time.getTime());
      return `<rect x="${x - 8}" y="${padY}" width="16" height="${height - padY * 2}" fill="rgba(152, 54, 38, 0.14)" rx="8" />`;
    })
    .join("");

  const markers = historicalOnly
    .filter((point) => point.status === "executed")
    .map((point) => {
      const x = xScale(point.time.getTime());
      const y = yScale(point.equity);
      return `<circle cx="${x}" cy="${y}" r="4" fill="#2d5f8b" stroke="#ffffff" stroke-width="2" />`;
    })
    .join("");

  const liveMarker =
    livePointEquity !== null
      ? `<circle cx="${xScale(livePointTime.getTime())}" cy="${yScale(livePointEquity)}" r="5" fill="#13684e" stroke="#ffffff" stroke-width="2" />`
      : "";

  return `
    <svg class="chart-svg" viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" role="img" aria-label="Эквити-кривая стратегии">
      <rect x="0" y="0" width="${width}" height="${height}" rx="24" fill="rgba(255,255,255,0.42)" />
      ${regimeRects}
      <line x1="${padX}" y1="${height - padY}" x2="${width - padX}" y2="${height - padY}" stroke="rgba(30, 26, 20, 0.12)" stroke-width="1" />
      <line x1="${padX}" y1="${padY}" x2="${padX}" y2="${height - padY}" stroke="rgba(30, 26, 20, 0.12)" stroke-width="1" />
      <polyline fill="none" stroke="#2d5f8b" stroke-width="4" stroke-linecap="round" stroke-linejoin="round" points="${historicalPolyline}" />
      ${
        liveLine
          ? `<line x1="${liveLine.x1}" y1="${liveLine.y1}" x2="${liveLine.x2}" y2="${liveLine.y2}" stroke="#13684e" stroke-width="4" stroke-linecap="round" />`
          : ""
      }
      ${markers}
      ${liveMarker}
    </svg>
  `;
}

function renderEquityCurve(dashboard, simulation) {
  const target = document.getElementById("equity-curve");
  const chartSvg = buildEquityChartSvg(simulation, dashboard);
  const simulationRun = simulation?.run || {};
  const liveEquity = numberOrNull(currentPortfolioState(dashboard).equity_rub);
  const historicalEquity = numberOrNull(simulationRun.final_equity_rub);

  if (!chartSvg) {
    target.innerHTML = '<p class="empty-state">Недостаточно точек, чтобы построить эквити-кривую. Нужны исторические rebalance-записи и текущий live mark.</p>';
    return;
  }

  const rebalances = latestSimulationRebalances(simulation);
  target.innerHTML = `
    <div class="stack">
      <div class="chart-card">
        <div class="chart-shell">${chartSvg}</div>
        <div class="chart-axis-labels">
          <span>${escapeHtml(formatDateShort(rebalances[0]?.rebalance_time || "n/a"))}</span>
          <span>${escapeHtml(formatDateShort(dashboard?.system?.engine_last_heartbeat || "n/a"))}</span>
        </div>
      </div>
      <div class="legend-row">
        <div class="legend-item"><span class="legend-swatch history"></span> Исторический бэктест</div>
        <div class="legend-item"><span class="legend-swatch live"></span> Live PnL</div>
        <div class="legend-item"><span class="legend-swatch protective"></span> Режим защиты / cash</div>
      </div>
      <div class="chart-meta">
        Исторический финальный эквити: ${formatCurrency(historicalEquity, 2)}.
        Текущий live эквити: ${formatCurrency(liveEquity, 2)}.
        Завершенных historical rebalance-точек: ${escapeHtml(simulationRun.completed_rebalances || 0)}.
      </div>
    </div>
  `;
}

function buildBacktestRows(dashboard, simulation) {
  const system = dashboard.system || {};
  const simulationRun = simulation?.run;
  const rebalances = latestSimulationRebalances(simulation);
  const rows = [];

  if (simulationRun) {
    rows.push({
      experimentId: `sim_${simulationRun.id}`,
      params: `Top ${system.portfolio_top_percentile || "n/a"} · ${system.strategy_candle_interval || "n/a"} · weekly cadence`,
      period:
        rebalances.length > 0
          ? `${formatDateShort(rebalances[0].rebalance_time)} → ${formatDateShort(rebalances[rebalances.length - 1].rebalance_time)}`
          : "n/a",
      cagr: "n/a",
      mdd: simulationRun.max_drawdown_pct == null ? "n/a" : formatPercent(simulationRun.max_drawdown_pct, 2),
      sharpe: (() => {
        const stats = computeSimulationStatistics(simulation);
        return stats.sharpe == null ? "n/a" : formatNumber(stats.sharpe, 2);
      })(),
      turnover: formatCurrency(simulationRun.turnover_rub, 0),
      status: simulationRun.status === "completed" ? tag("LIVE BASELINE", "success") : tag(simulationRun.status || "n/a", "warning"),
    });
  }

  rows.push({
    experimentId: "live_runtime",
    params: `${system.broker_mode || "n/a"} · routing ${system.strategy_portfolio_live_enabled ? "on" : "off"}`,
    period: `${formatDateShort(system.run_started_at || "n/a")} → ${formatDateShort(system.engine_last_heartbeat || "n/a")}`,
    cagr: "n/a",
    mdd: (() => {
      const dd = computeRunDrawdownPct(dashboard);
      return dd == null ? "n/a" : formatPercent(dd, 2);
    })(),
    sharpe: "n/a",
    turnover: `${escapeHtml(safeArray(dashboard.portfolio_trades).length)} trade(s)`,
    status: tag("LIVE", "info"),
  });

  return rows;
}

function renderBacktests(dashboard, simulation) {
  const target = document.getElementById("backtests");
  target.innerHTML = `
    <div class="stack">
      <div id="backtests-table"></div>
      <div class="experiment-note">
        Сейчас в UI показывается последний сохранённый historical run и текущий live runtime.
        Когда в системе появится хранение нескольких experiment-конфигураций, этот блок автоматически превратится в полноценную сравнительную таблицу гипотез.
      </div>
    </div>
  `;

  renderTable(
    "backtests-table",
    [
      { key: "experimentId", label: "ID эксперимента" },
      { key: "params", label: "Параметры" },
      { key: "period", label: "Период" },
      { key: "cagr", label: "CAGR", numeric: true },
      { key: "mdd", label: "MDD", numeric: true },
      { key: "sharpe", label: "Sharpe", numeric: true },
      { key: "turnover", label: "Оборот", numeric: true },
      { key: "status", label: "Статус", render: (value) => value },
    ],
    buildBacktestRows(dashboard, simulation),
    "Бэктесты пока не сохранены.",
  );
}

function buildEventFeed(dashboard, simulation) {
  const events = [];

  for (const rebalance of safeArray(dashboard.portfolio_rebalances)) {
    events.push({
      time: rebalance.created_at,
      type: "REBALANCE",
      tone:
        rebalance.status === "executed"
          ? "success"
          : rebalance.status === "regime_off"
            ? "danger"
            : "warning",
      message: `Ребалансировка ${rebalance.status}. Корзина: ${rebalance.selected_symbols || "none"}. Planned ${rebalance.planned_actions}, executed ${rebalance.executed_actions}. Причина: ${rebalance.reason || "n/a"}.`,
    });
  }

  for (const trade of safeArray(dashboard.portfolio_trades)) {
    events.push({
      time: trade.created_at,
      type: "TRADE",
      tone: trade.side === "buy" ? "success" : trade.side === "sell" ? "warning" : "info",
      message: `${String(trade.side || "").toUpperCase()} ${trade.quantity} лот(ов) ${trade.symbol} по ${formatNumber(trade.price, 4)}. Номинал ${formatCurrency(trade.notional_rub, 2)}. Статус: ${trade.status}.`,
    });
  }

  for (const signal of safeArray(dashboard.signals)) {
    events.push({
      time: signal.created_at,
      type: "SIGNAL",
      tone:
        signal.status === "approved"
          ? "success"
          : signal.status === "rejected" || signal.status === "broker_error"
            ? "danger"
            : "info",
      message: `Сигнал ${signal.side} по ${signal.symbol}. Статус: ${signal.status}. Confidence: ${formatNumber(signal.confidence, 2)}. Причина: ${signal.reason}.`,
    });
  }

  for (const order of safeArray(dashboard.orders)) {
    events.push({
      time: order.created_at,
      type: "ORDER",
      tone: order.status === "filled" ? "success" : "warning",
      message: `Ордер ${order.side} ${order.quantity} ${order.symbol} по ${formatNumber(order.price, 4)}. Broker mode: ${order.broker_mode}. Status: ${order.status}.`,
    });
  }

  if (simulation?.run) {
    events.push({
      time: simulation.run.created_at,
      type: "SIM",
      tone: simulation.run.status === "completed" ? "info" : "warning",
      message: `Historical simulation run #${simulation.run.id}: ${simulation.run.status}. Rebalances: ${simulation.run.completed_rebalances}/${simulation.run.rebalance_points}. Return: ${formatPercent(simulation.run.total_return_pct, 2)}. Max DD: ${formatPercent(simulation.run.max_drawdown_pct, 2)}.`,
    });
  }

  const heartbeat = dashboard?.system?.engine_last_heartbeat;
  if (heartbeat) {
    events.push({
      time: heartbeat,
      type: "ENGINE",
      tone: dashboard.system.paused ? "warning" : "success",
      message: `Heartbeat итерации ${dashboard.system.engine_iteration}. Broker mode: ${dashboard.system.broker_mode}. Universe eligible: ${dashboard.system.universe_eligible_size || 0}.`,
    });
  }

  return events
    .filter((row) => parseDate(row.time))
    .sort((left, right) => parseDate(right.time).getTime() - parseDate(left.time).getTime())
    .slice(0, 14);
}

function renderEventFeed(dashboard, simulation) {
  const target = document.getElementById("event-feed");
  const rows = buildEventFeed(dashboard, simulation);
  if (!rows.length) {
    target.innerHTML = '<p class="empty-state">Лента событий пока пуста.</p>';
    return;
  }

  target.innerHTML = `
    <div class="log-feed">
      ${rows
        .map(
          (row) => `
            <div class="log-line ${row.tone}">
              <span class="log-time">[${escapeHtml(formatDateTime(row.time))}]</span>
              <span class="log-type">[${escapeHtml(row.type)}]</span>
              <span>${escapeHtml(row.message)}</span>
            </div>
          `,
        )
        .join("")}
    </div>
  `;
}

async function fetchDashboard() {
  const response = await fetch("/api/dashboard");
  return response.json();
}

async function fetchBrokerStatus() {
  const response = await fetch("/api/broker/status");
  return response.json();
}

async function fetchSimulation() {
  const response = await fetch("/api/simulation/latest");
  return response.json();
}

function renderAll() {
  if (!appState.dashboard) {
    return;
  }
  renderHeroSummary(appState.dashboard, appState.brokerStatus, appState.simulation);
  renderCurrentPortfolio(appState.dashboard, appState.brokerStatus);
  renderLiveMetrics(appState.dashboard, appState.simulation, appState.brokerStatus);
  renderSignalsFilters(appState.dashboard, appState.simulation);
  renderEquityCurve(appState.dashboard, appState.simulation);
  renderBacktests(appState.dashboard, appState.simulation);
  renderEventFeed(appState.dashboard, appState.simulation);

  const stopButton = document.getElementById("portfolio-stop-btn");
  if (stopButton) {
    stopButton.onclick = () => postAction("/api/system/pause");
  }
}

async function refreshDashboardLoop() {
  const [dashboard, simulation] = await Promise.all([fetchDashboard(), fetchSimulation()]);
  appState.dashboard = dashboard;
  appState.simulation = simulation || { run: null, rebalances: [] };
  renderAll();
}

async function refreshBrokerStatus() {
  appState.brokerStatus = await fetchBrokerStatus();
  renderAll();
}

async function postAction(url) {
  await fetch(url, { method: "POST" });
  await refreshDashboardLoop();
  await refreshBrokerStatus();
}

document.getElementById("pause-btn").addEventListener("click", () => postAction("/api/system/pause"));
document.getElementById("resume-btn").addEventListener("click", () => postAction("/api/system/resume"));

refreshDashboardLoop();
refreshBrokerStatus();
setInterval(refreshDashboardLoop, (window.dashboardRefreshSeconds || 5) * 1000);
setInterval(refreshBrokerStatus, (window.brokerStatusRefreshSeconds || 30) * 1000);
