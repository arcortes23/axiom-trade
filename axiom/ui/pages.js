/* Pure destination renderers for the beginner-friendly operator UI.
 *
 * This module deliberately does not read the DOM, fetch, mutate browser state,
 * or calculate financial totals. The shell owns navigation and transport;
 * these functions turn bounded server projections into accessible HTML.
 */

const VIEW_ALIASES = Object.freeze({
  overview: ["home", ""],
  canary: ["live", "polymarket"],
  "binance-canary": ["live", "binance"],
  paper: ["portfolio", "practice"],
  "rolling-portfolio": ["portfolio", "allocations"],
  datasets: ["research", "data"],
  candidates: ["research", "strategies"],
  crypto: ["research", "crypto"],
  "crypto-research": ["research", "crypto"],
  hermes: ["research", "automation"],
  shadow: ["research", "automation"],
});

const ROUTES = Object.freeze({
  home: { title: "Home", endpoint: "/api/v2/overview-summary" },
  live: {
    polymarket: { title: "Polymarket", endpoint: "/api/v2/canary" },
    binance: { title: "Binance", endpoint: "/api/v2/binance-canary" },
  },
  portfolio: {
    real: { title: "Real canary", endpoint: "/api/v2/canary" },
    practice: { title: "Practice", endpoint: "/api/v2/paper" },
    allocations: { title: "Allocations", endpoint: "/api/v2/rolling-portfolio" },
  },
  markets: { title: "Markets", endpoint: "/api/v2/polymarket" },
  research: {
    strategies: { title: "Strategies", endpoint: "/api/v2/candidates" },
    crypto: { title: "Crypto research", endpoint: "/api/v2/crypto-research" },
    automation: { title: "Automation", endpoint: "/api/v2/hermes" },
    data: { title: "Data", endpoint: "/api/v2/datasets" },
  },
  activity: { title: "Activity", endpoint: "/api/v2/activity" },
});


function isObject(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function list(value) {
  return Array.isArray(value) ? value : [];
}

function first(...values) {
  return values.find((value) => value !== undefined && value !== null && value !== "");
}

function text(value, fallback = "Not available") {
  return value === undefined || value === null || value === "" ? fallback : String(value);
}

function lower(value) {
  return String(value ?? "").trim().toLowerCase();
}

function humanize(value) {
  if (value === undefined || value === null || value === "") return "Not available";
  return String(value)
    .replace(/([a-z\d])([A-Z])/g, "$1 $2")
    .replace(/[._-]+/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .replace(/\b\w/g, (letter) => letter.toUpperCase());
}

function escLocal(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function sanitizeLocal(value, depth = 0, key = "") {
  if (/(token|secret|password|credential|authorization|cookie|private[_-]?key|api[_-]?key|csrf)/i.test(key)) return "[redacted]";
  if (depth > 5) return "[details omitted]";
  if (value === null || value === undefined || typeof value === "string" || typeof value === "number" || typeof value === "boolean") return value;
  if (Array.isArray(value)) return value.slice(0, 64).map((item) => sanitizeLocal(item, depth + 1, key));
  if (isObject(value)) return Object.fromEntries(Object.entries(value).slice(0, 128).map(([name, item]) => [name, sanitizeLocal(item, depth + 1, name)]));
  return String(value);
}

function technicalLocal(value, title = "Technical details") {
  return `<details class="technical"><summary>${escLocal(title)}</summary><pre>${escLocal(JSON.stringify(sanitizeLocal(value), null, 2))}</pre></details>`;
}

function uiFor(ctx) {
  const supplied = isObject(ctx?.ui) ? ctx.ui : {};
  return {
    esc: typeof supplied.esc === "function" ? supplied.esc : escLocal,
    money: typeof supplied.money === "function" ? supplied.money : (value) => text(value),
    number: typeof supplied.number === "function" ? supplied.number : (value) => text(value),
    time: typeof supplied.time === "function" ? supplied.time : (value) => text(value),
    human: typeof supplied.human === "function" ? supplied.human : humanize,
    badge: typeof supplied.badge === "function"
      ? supplied.badge
      : (label, tone = "neutral") => `<span class="badge ${escLocal(tone)}">${escLocal(label)}</span>`,
    empty: typeof supplied.empty === "function"
      ? supplied.empty
      : (title, body) => `<div class="empty-state"><h3>${escLocal(title)}</h3><p>${escLocal(body)}</p></div>`,
    notice: typeof supplied.notice === "function"
      ? supplied.notice
      : (title, body, tone = "info") => `<div class="notice ${escLocal(tone)}"><strong>${escLocal(title)}</strong><p>${escLocal(body)}</p></div>`,
    kv: typeof supplied.kv === "function"
      ? supplied.kv
      : (label, value) => `<div class="kv"><dt>${escLocal(label)}</dt><dd>${value}</dd></div>`,
    table: typeof supplied.table === "function" ? supplied.table : null,
    technical: typeof supplied.technical === "function"
      ? supplied.technical
      : technicalLocal,
    link: typeof supplied.link === "function" ? supplied.link : (label, patch = {}) => {
      const params = new URLSearchParams();
      Object.entries(patch || {}).forEach(([key, value]) => {
        if (value !== undefined && value !== null && value !== "") params.set(key, String(value));
      });
      return `<a href="?${escLocal(params.toString())}">${escLocal(label)}</a>`;
    },
    reason: typeof supplied.reason === "function" ? supplied.reason : humanize,
    readState: stateFor(ctx?.data, ctx),
    primaryRows: primaryRows(ctx?.data, ctx?.route, ctx),
  };
}

function normalizeRoute(input = {}) {
  const route = isObject(input) ? { ...input } : {};
  let view = String(route.view || "home").toLowerCase();
  let section = String(route.section || "").toLowerCase();
  const alias = VIEW_ALIASES[view];
  if (alias) {
    [view, section] = alias;
  }
  if (view === "markets") section = "";
  if (view === "activity") section = "";
  if (view === "home" || view === "settings") section = "";
  if (view === "live" && !["polymarket", "binance"].includes(section)) section = "polymarket";
  if (view === "portfolio" && !["real", "practice", "allocations"].includes(section)) section = "real";
  if (view === "research" && !["strategies", "crypto", "automation", "data"].includes(section)) section = "strategies";
  return { ...route, view, section };
}

function endpointFor(view, section) {
  const route = ROUTES[view];
  if (!route) return null;
  return route.endpoint || route[section]?.endpoint || null;
}

function optionsFor(key) {
  const options = {
    category: ["All categories", "Politics", "Sports", "Weather", "Crypto", "Other"],
    market_type: ["All market types", "crypto_spot", "prediction"],
    settlement: ["All settlement states", "open", "resolved_yes", "resolved_no", "void"],
    quality: ["All quality states", "OHLCV", "PRICE_PROXY", "ORDER_BOOK_SIMULATED"],
    source: ["All sources", "HISTORICAL", "FORWARD_COLLECTED"],
    timeframe: ["All timeframes", "1m", "1h", "1d", "live"],
    symbol: ["All symbols"],
    stage: ["All stages", "IDEA", "SCHEMA_VALIDATED", "BACKTESTED", "VALIDATED", "ROBUSTNESS_CHECKED", "FROZEN", "PAPER_FORWARD", "PAPER_PROMOTABLE", "REJECTED"],
    status: ["All statuses", "PENDING", "RUNNING", "SUCCEEDED", "COMPLETED", "REVIEW_REQUIRED", "REJECTED", "FAILED", "UNKNOWN", "INFO", "NOTICE", "WARNING", "ERROR"],
    kind: ["All kinds", "trading", "research", "data", "system", "dataset", "bootstrap", "collection", "lifecycle", "report", "collection_error", "operator"],
  };
  return options[key];
}
function spec(title, subtitle, endpoint, facets = [], sortOptions = [], detailRequests, pollMs) {
  const result = { title, subtitle, endpoint, facets, sortOptions };
  if (detailRequests) result.detailRequests = detailRequests;
  if (pollMs) result.pollMs = pollMs;
  return result;
}
function idOf(row, kindHint = "") {
  if (!isObject(row)) return "";
  const kind = String(row.record_kind || kindHint || "").toLowerCase();
  if (kind === "market") return text(first(row.market_id, row.id, row.identifier), "");
  if (kind === "submission") return text(first(row.attempt_id, row.id, row.identifier), "");
  if (kind === "order") return text(first(row.request_id, row.id, row.identifier), "");
  if (kind === "reservation") return text(first(row.reservation_id, row.id, row.identifier), "");
  if (kind === "fill" || kind === "risk-fill") return text(first(row.fill_id, row.id, row.identifier), "");
  if (kind === "position") return text(first(row.position_id, row.id, row.identifier), "");
  if (kind === "mark") return text(first(row.mark_id, row.id, row.identifier), "");
  if (kind === "cashflow") return text(first(row.flow_id, row.id, row.identifier), "");
  if (kind === "crypto" || kind === "crypto-research") return text(first(row.symbol, row.id, row.identifier, row.dataset_id), "");
  if (kind === "shadow") return text(first(row.job_id, row.id, row.identifier), "");
  if (kind === "hermes") return text(first(row.item_id, row.job_id, row.id, row.identifier), "");
  if (kind === "activity") return text(first(row.event_id, row.id, row.identifier), "");
  return text(first(row.id, row.identifier, row.market_id, row.candidate_id, row.dataset_id, row.job_id, row.item_id, row.attempt_id, row.request_id, row.reservation_id, row.fill_id, row.position_id, row.mark_id, row.flow_id, row.round_trip_id, row.symbol, row.event_id), "");
}

function selectedOf(route) {
  return first(route?.selected, route?.id, route?.symbol, route?.item_id, route?.candidate_id, route?.market_id, "");
}

function detailMap(ctx) {
  return isObject(ctx?.detail) ? ctx.detail : {};
}

function arrayFrom(data, keys = []) {
  if (!isObject(data)) return [];
  for (const key of keys) {
    if (Array.isArray(data[key])) return data[key];
  }
  if (Array.isArray(data.items)) return data.items;
  if (Array.isArray(data.rows)) return data.rows;
  if (Array.isArray(data.results)) return data.results;
  if (Array.isArray(data.records)) return data.records;
  return [];
}
function primaryRows(data, routeInput = {}, ctx = {}) {
  const source = envelope(data);
  const route = normalizeRoute(routeInput);
  let keys = ["items", "rows", "results", "records"];
  if (route.view === "markets") keys = ["items", "markets"];
  else if (route.view === "research" && route.section === "strategies") keys = ["items", "candidates"];
  else if (route.view === "research" && route.section === "crypto") keys = ["items", "catalog", "catalogs"];
  else if (route.view === "research" && route.section === "automation") keys = ["items", "queue", "jobs"];
  else if (route.view === "research" && route.section === "data") keys = ["items", "datasets", "catalog"];
  else if (route.view === "activity") keys = ["items", "events", "activity"];
  else if (route.view === "portfolio" && route.section === "practice") keys = ["items", "records", "observations", "paper_records"];
  else if (route.view === "portfolio" && route.section === "allocations") keys = ["active_rows", "members", "items"];
  else if (route.view === "live" && route.section === "binance") keys = ["items", "orders", "fills", "positions", "records"];
  else if ((route.view === "portfolio" && route.section === "real") || (route.view === "live" && route.section === "polymarket")) {
    keys = ["items", "orders", "fills", "records"];
  }
  const rows = arrayFrom(source, keys);
  if (route.view === "research" && route.section === "automation") {
    return rows.concat(rowsFromEnvelopes(source, ["shadow"]), arrayFrom(ctx?.detail?.shadow_list, ["items", "jobs", "records"]));
  }
  if ((route.view === "portfolio" && route.section === "real") || (route.view === "live" && route.section === "polymarket")) {
    return rows.concat(rowsFromEnvelopes(source, ["execution", "ledger"]));
  }
  return rows;
}
function hasStructuredContext(data, routeInput = {}) {
  const route = normalizeRoute(routeInput);
  if (route.view !== "live" || route.section !== "binance") return false;
  const source = envelope(data);
  return ["status", "state", "environment", "mode", "profile", "identity", "strict_testnet", "credentials", "transport", "title", "available"]
    .some((key) => Object.prototype.hasOwnProperty.call(source, key) && source[key] !== undefined && source[key] !== null);
}


function blockingState(state) {
  return ["loading", "error", "unavailable", "disconnected"].includes(state);
}

function nonEmpty(value) {
  if (Array.isArray(value)) return value.length > 0;
  if (isObject(value)) return Object.keys(value).length > 0;
  return value !== undefined && value !== null && String(value).trim() !== "";
}

function unavailableSource(source) {
  if (source.available === false) return true;
  const status = lower(source.status);
  return ["unavailable", "failed", "error", "timeout", "disconnected"].includes(status);
}

function stateFor(data, ctx) {
  const source = envelope(data);
  const errors = ctx?.errors;
  if (ctx?.loading || source.loading) return "loading";
  if (ctx?.error || nonEmpty(source.error) || nonEmpty(source.errors)) return "error";
  if (Array.isArray(errors) ? errors.length : isObject(errors) ? Object.keys(errors).length : Boolean(errors)) return "partial";
  if (unavailableSource(source)) return "unavailable";
  if (source.disconnected || source.connection === "DISCONNECTED") return "disconnected";
  if (source.stale || source.partial || source.status === "STALE" || source.status === "PARTIAL") return "partial";
  return "loaded";
}

function stateLabel(state) {
  return {
    loading: ["Loading", "The bounded request is in progress; no empty result is inferred."],
    error: ["This view is unavailable", "The server returned an error; no empty result is inferred. Try again without changing any controls."],
    unavailable: ["This view is unavailable", "The bounded source did not provide a readable result; no empty result is inferred."],
    disconnected: ["Account or venue disconnected", "Current values cannot be verified; no empty result is inferred."],
  }[state];
}


function stateBody(ui, data, ctx) {
  const state = stateFor(data, ctx);
  if (!blockingState(state)) return "";
  const rows = primaryRows(data, ctx?.route, ctx);
  if (rows.length) {
    if (state === "loading") return ui.notice("Refreshing bounded projection", "Showing last-known rows until this read completes; they are not presented as current.", "info");
    return ui.notice("Current projection unavailable", "Showing last-known rows from the bounded response; they are not presented as current.", "warn");
  }
  if (hasStructuredContext(data, ctx?.route)) {
    return ui.notice("Structured context retained", "Bounded venue, profile, or status context remains available; record totals are not inferred from this read.", "info");
  }
  const copy = stateLabel(state) || ["Data unavailable", "No bounded result is available; no empty result is inferred."];
  return ui.empty(copy[0], copy[1]);
}


function envelope(data) {
  return isObject(data) ? data : {};
}


function statusTone(status) {
  const value = lower(status);
  if (["failed", "error", "rejected", "revoked", "expired", "blocked", "unknown"].some((part) => value.includes(part))) return "bad";
  if (["pending", "testing", "partial", "stale", "parked", "unconfigured", "not configured", "review"].some((part) => value.includes(part))) return "warn";
  if (["active", "running", "ready", "complete", "completed", "filled", "fresh", "paper"].some((part) => value.includes(part))) return "good";
  return "neutral";
}

function rowsFromEnvelopes(source, keys) {
  const rows = [];
  keys.forEach((key) => {
    const nested = source?.[key];
    if (Array.isArray(nested)) rows.push(...nested);
    else if (isObject(nested)) rows.push(...arrayFrom(nested, ["items", "rows", "records"]));
  });
  return rows;
}

function labelFor(ui, value, fallback = "Not available") {
  const label = text(value, fallback);
  return typeof ui.human === "function" ? ui.human(label) : humanize(label);
}

function badge(ui, value, fallback = "Not available") {
  const label = text(value, fallback);
  return ui.badge(labelFor(ui, label), statusTone(label));
}

function metric(ui, label, value, detail = "") {
  return `<div class="metric"><span class="metric-label">${ui.esc(label)}</span><strong>${value}</strong>${detail ? `<small>${ui.esc(detail)}</small>` : ""}</div>`;
}

function metrics(items) {
  return `<div class="metrics">${items.join("")}</div>`;
}

function value(row, ...keys) {
  if (!isObject(row)) return undefined;
  for (const key of keys) {
    const bits = String(key).split(".");
    let current = row;
    for (const bit of bits) {
      if (Array.isArray(current) && bit === "length") current = current.length;
      else if (Array.isArray(current)) current = current[bit];
      else current = isObject(current) ? current[bit] : undefined;
    }
    if (current !== undefined && current !== null && current !== "") return current;
  }
  return undefined;
}

function shown(ui, row, keys, kind = "text") {
  const raw = value(row, ...keys);
  if (raw === undefined || raw === null || raw === "") return `<span class="muted">Not available</span>`;
  if (kind === "money") return ui.money(raw);
  if (kind === "number") return ui.number(raw);
  if (kind === "time") return ui.time(raw);
  if (kind === "status") return ui.esc(labelFor(ui, raw));
  return ui.esc(raw);
}
function fieldValue(ui, label, raw, kind = "text") {
  if (kind === "time") return ui.time(raw);
  if (/(status|phase|settlement|feasibility|quality|freshness|blocker)/i.test(label)) return ui.esc(labelFor(ui, raw));
  return ui.esc(text(raw));
}

function evidenceValue(ui, row, keys, title) {
  const raw = value(row, ...keys);
  if (raw === undefined || raw === null || raw === "") return `<span class="muted">Not available</span>`;
  return isObject(raw) || Array.isArray(raw) ? ui.technical(raw, title) : ui.esc(text(raw));
}

function hasAny(source, keys) {
  return isObject(source) && keys.some((key) => Object.prototype.hasOwnProperty.call(source, key));
}

function hrefFor(route, patch) {
  const merged = { ...route, ...patch };
  const params = new URLSearchParams();
  Object.entries(merged).forEach(([key, value]) => {
    if (["view", "section"].includes(key) || value === undefined || value === null || value === "") return;
    if (typeof value === "object") return;
    params.set(key, String(value));
  });
  const view = patch.view || route.view || "home";
  const section = patch.section || route.section;
  params.set("view", view);
  if (section) params.set("section", section);
  return `?${params.toString()}`;
}

function detailLink(ui, route, label, id, recordKind = "") {
  if (!id) return ui.esc(label);
  const patch = { selected: id, expanded: "1" };
  if (recordKind) patch.record_kind = recordKind;
  if (typeof ui.link === "function") {
    try {
      const link = ui.link(label, patch);
      if (link) return link;
    } catch (_error) {
      // A custom shell helper is optional; the deterministic fallback is below.
    }
  }
  return `<a href="${escLocal(hrefFor(route, patch))}">${ui.esc(label)}</a>`;
}

function table(ui, columns, rows, caption = "") {
  const values = list(rows);
  if (!values.length) {
    if (blockingState(ui.readState) && !list(ui.primaryRows).length) {
      const copy = stateLabel(ui.readState) || ["Data unavailable", "No bounded result is available; no empty result is inferred."];
      return ui.empty(copy[0], copy[1]);
    }
    const heading = caption || "Nothing to show";
    const explanation = caption ? `No ${caption.toLowerCase()} are present in this bounded projection; no value is inferred.` : "No records are available for this view.";
    return ui.empty(heading, explanation);
  }
  if (ui.table) {
    try {
      return ui.table(columns, values, caption ? { caption } : {});
    } catch (_error) {
      // Keep a safe local table if an integration helper has not landed yet.
    }
  }
  const head = columns.map((column) => `<th scope="col">${ui.esc(column.label)}</th>`).join("");
  const body = values.map((row) => `<tr>${columns.map((column) => {
    let rendered = "";
    try { rendered = column.render(row); } catch (_error) { rendered = `<span class="muted">Not available</span>`; }
    return `<td${column.className ? ` class="${ui.esc(column.className)}"` : ""}>${rendered}</td>`;
  }).join("")}</tr>`).join("");
  return `<div class="table-scroll"><table class="data-table">${caption ? `<caption>${ui.esc(caption)}</caption>` : ""}<thead><tr>${head}</tr></thead><tbody>${body}</tbody></table></div>`;
}

function pageFrame(ui, title, intro, body, state = "loaded") {
  const stateCopy = {
    loading: ["Loading", "Reading the bounded server projection; no empty result is inferred."],
    disconnected: ["Connection unavailable", "The last known values are not being presented as current; no empty result is inferred."],
    unavailable: ["Data unavailable", "The bounded source did not provide a readable result; no empty result is inferred."],
    partial: ["Partial or stale data", "Some supporting records need a refresh; unknown values remain unknown."],
    error: ["Could not load this view", "Retry the read-only request. No authority or control action was changed; no empty result is inferred."],
  }[state];
  return `<section class="section" data-page-state="${ui.esc(state)}"><header class="section-heading"><div><h2>${ui.esc(title)}</h2><p class="muted">${ui.esc(intro)}</p></div></header>${stateCopy ? ui.notice(stateCopy[0], stateCopy[1], ["error", "unavailable"].includes(state) ? "error" : "info") : ""}${body}</section>`;
}

function summary(ui, data, labels) {
  const source = envelope(data);
  return metrics(labels.map(([label, keys, kind]) => {
    const displayKind = kind || (/status|state|decision/i.test(label) ? "status" : "text");
    return metric(ui, label, shown(ui, source, keys, displayKind));
  }));
}

function renderOrders(ui, rows, route) {
  return table(ui, [
    { label: "Request / position", render: (row) => { const kind = value(row, "record_kind") || (value(row, "request_id") ? "order" : "submission"); return detailLink(ui, route, first(value(row, "request_id", "position_id", "attempt_id", "id", "question", "name", "market_name", "market"), "Unnamed request"), idOf(row, kind), kind); } },
    { label: "Phase", render: (row) => badge(ui, value(row, "phase", "stage", "status", "state", "settlement_status")) },
    { label: "Selected outcome", render: (row) => ui.esc(first(value(row, "selected_outcome", "outcome", "token", "outcome_name"), "Not available")) },
    { label: "Feasibility", render: (row) => badge(ui, value(row, "feasibility", "feasibility_status", "feasible", "affordability.status")) },
    { label: "Side", render: (row) => ui.esc(first(value(row, "side"), "Not available")) },
    { label: "Quantity", render: (row) => shown(ui, row, ["requested_quantity", "quantity", "size"], "number") },
    { label: "Submitted price", render: (row) => shown(ui, row, ["submitted_price", "requested_price", "price", "limit_price"], "number") },
    { label: "Fees", render: (row) => shown(ui, row, ["fees", "fee", "fee_amount", "commission"], "money") },
    { label: "Time", render: (row) => shown(ui, row, ["submitted_at", "timestamp", "created_at", "updated_at"], "time") },
  ], rows, "Orders and position requests");
}

function renderFills(ui, rows, route) {
  return table(ui, [
    { label: "Fill / position", render: (row) => { const kind = value(row, "record_kind") || (value(row, "fill_id") ? "fill" : "risk-fill"); return detailLink(ui, route, first(value(row, "fill_id", "position_id", "id", "question", "name", "market_name"), "Unnamed fill"), idOf(row, kind), kind); } },
    { label: "Phase", render: (row) => badge(ui, value(row, "phase", "stage", "status", "state")) },
    { label: "Selected outcome", render: (row) => ui.esc(first(value(row, "selected_outcome", "outcome", "token", "outcome_name"), "Not available")) },
    { label: "Feasibility", render: (row) => badge(ui, value(row, "feasibility", "feasibility_status", "feasible", "affordability.status")) },
    { label: "Side", render: (row) => ui.esc(first(value(row, "side"), "Not available")) },
    { label: "Filled quantity", render: (row) => shown(ui, row, ["quantity", "filled_quantity", "size"], "number") },
    { label: "Submitted price", render: (row) => shown(ui, row, ["submitted_price", "requested_price", "price", "fill_price"], "number") },
    { label: "Fees", render: (row) => shown(ui, row, ["fee", "fees", "fee_amount", "commission"], "money") },
    { label: "Time", render: (row) => shown(ui, row, ["filled_at", "timestamp", "created_at"], "time") },
  ], rows, "Position fills");
}

function renderRoundTrips(ui, rows, route = { view: "portfolio", section: "real" }) {
  return table(ui, [
    { label: "Position / market", render: (row) => {
      const label = first(value(row, "position_id", "market_id", "question", "name", "market_name"), "Unnamed position");
      return value(row, "position_id") ? detailLink(ui, route, label, value(row, "position_id"), "position") : ui.esc(label);
    } },
    { label: "Cost basis", render: (row) => shown(ui, row, ["cost_basis", "entry_price", "entry"], "money") },
    { label: "Gross proceeds", render: (row) => shown(ui, row, ["gross_proceeds", "exit_price", "exit"], "money") },
    { label: "Known result", render: (row) => shown(ui, row, ["realized_pnl", "known_result", "pnl", "result"], "money") },
    { label: "Status", render: (row) => badge(ui, value(row, "status", "state")) },
  ], rows, "Closed positions and settled round trips");
}

function recordsOrMissing(ui, source, keys, rows, renderer, label) {
  const availability = isObject(source?.availability) ? source.availability : {};
  const metadata = keys.map((key) => availability[key]).find((item) => item !== undefined && item !== null);
  const status = isObject(metadata) ? String(metadata.status || metadata.state || "").toUpperCase() : String(metadata || "").toUpperCase();
  if (["NOT_PERSISTED", "UNAVAILABLE", "ERROR", "UNKNOWN"].includes(status)) return ui.notice(`${label} unavailable`, first(isObject(metadata) ? metadata.reason : "", "The source did not persist this record class; no empty result is inferred."), "info");
  if (hasAny(source, keys)) return renderer();
  return ui.empty(`${label} not available`, `The bounded projection did not provide ${label.toLowerCase()} for this account or venue.`);
}

function renderPortfolio(ctx, route, ui, data) {
  const source = envelope(data);
  const state = stateFor(data, ctx);
  if (route.section === "practice") {
    const rows = arrayFrom(source, ["items", "records", "observations", "paper_records"]);
    const events = arrayFrom(source, ["events", "software_events"]);
    const body = `${stateBody(ui, data, ctx)}${summary(ui, source, [["Paper records", ["items.length", "count"]], ["Total records", ["total"]], ["Paper state", ["paper_only", "status"]], ["Last observation", ["updated_at", "latest_at"], "time"]])}<div class="notice info"><strong>Practice only</strong><p>These observations and provider outcomes stay separate from real canary accounting; payloads are shown as recorded, not interpreted as trades.</p></div>${table(ui, [{ label: "Record", render: (row) => ui.esc(first(value(row, "record_id", "id", "market_id"), "Unnamed record")) }, { label: "Type", render: (row) => badge(ui, value(row, "record_type", "status")) }, { label: "Outcome", render: (row) => ui.esc(first(value(row, "outcome", "resolution"), "Not available")) }, { label: "Known result", render: (row) => shown(ui, row, ["known_result", "realized_pnl", "pnl"], "money") }], rows, "Practice observations")}${events.length ? table(ui, [{ label: "Time", render: (row) => shown(ui, row, ["timestamp", "created_at"], "time") }, { label: "Message", render: (row) => ui.esc(first(value(row, "message", "title", "event"), "Unnamed event")) }], events, "Practice events") : ""}`;
    return pageFrame(ui, "Practice portfolio", "Paper observations and provider outcomes stay separate from real canary accounting.", body, state);
  }
  if (route.section === "allocations") {
    const review = isObject(source.policy_review) ? source.policy_review : {};
    const active = isObject(review.active) ? review.active : first(source.active, {}) || {};
    const proposed = isObject(review.proposed) ? review.proposed : first(source.proposed, {}) || {};
    const proposals = list(source.proposals).length ? source.proposals : list(review.proposals);
    const controls = isObject(ctx?.operator?.operator_controls) ? ctx.operator.operator_controls : {};
    const authoritativeReview = isObject(controls.exploratory_live_review) ? controls.exploratory_live_review : {};
    const members = arrayFrom(source, ["active_rows", "members", "items"]);
    const proposedMembers = list(value(authoritativeReview, "proposal.members")).length ? list(value(authoritativeReview, "proposal.members")) : list(authoritativeReview.members);
    const currentMembersMarkup = table(ui, [{ label: "Member", render: (row) => ui.esc(first(value(row, "name", "strategy_name", "candidate_id"), "Unnamed member")) }, { label: "Selection", render: (row) => badge(ui, value(row, "selection", "status", "state")) }, { label: "Reason", render: (row) => ui.esc(first(value(row, "reason", "decision_reason", "explanation"), "Not available")) }, { label: "Evidence", render: (row) => ui.esc(first(value(row, "evidence_status", "evidence"), "Not available")) }], members, "Current allocation members");
    const proposedMembersMarkup = table(ui, [{ label: "Candidate", render: (row) => ui.esc(first(value(row, "candidate_id", "name", "strategy_name"), "Unnamed candidate")) }, { label: "Proposal status", render: (row) => badge(ui, value(row, "status", "selection", "state")) }, { label: "Allocation", render: (row) => ui.money(first(value(row, "proposed_allocation", "allocation"), "")) }, { label: "Setup binding", render: (row) => evidenceValue(ui, row, ["setup_binding", "operational_setup_hash", "strategy_version_id"], "Canonical setup binding") }], proposedMembers, "Authoritative proposed members");
    const body = `${stateBody(ui, data, ctx)}${summary(ui, source, [["Allocation status", ["status", "controller_status"]], ["Active members", ["active_member_count", "member_count"]], ["Shared allocation", ["allocation.amount", "allocation.shared_usd", "shared_allocation"], "money"], ["Next review", ["next_scheduled_work", "next_review_at"], "time"]])}<div class="split"><article class="panel"><h3>Current authority</h3><dl class="detail-grid">${ui.kv("State", badge(ui, first(active.state, active.status, source.status)))}${ui.kv("Members", ui.esc(text(first(active.member_count, active.members, source.active_member_count))))}${ui.kv("Policy", ui.esc(text(first(active.policy_name, active.policy, active.policy_id))))}</dl></article><article class="panel"><h3>Proposed review</h3><dl class="detail-grid">${ui.kv("State", badge(ui, first(proposed.state, proposed.status, proposed.stage, authoritativeReview.status, "PROPOSED")))}${ui.kv("Decision", ui.esc(text(first(proposed.decision, proposed.reason, authoritativeReview.blockers, "Review required"))))}${ui.kv("Members", ui.esc(text(first(proposed.member_count, proposed.members, proposedMembers.length))))}${ui.kv("Selection", ui.esc(text(first(authoritativeReview.proposal?.selection_id, authoritativeReview.selection_id, "Not available"))))}</dl></article></div>${currentMembersMarkup}${proposedMembersMarkup}${table(ui, [{ label: "Proposal", render: (row) => ui.esc(first(value(row, "name", "proposal_id", "policy_id"), "Unnamed proposal")) }, { label: "Outcome", render: (row) => badge(ui, value(row, "outcome", "decision", "status")) }, { label: "Why", render: (row) => ui.esc(first(value(row, "reason", "explanation"), "Not available")) }], proposals, "Allocation proposals")}${source.cold_start_requirements ? ui.notice("Cold-start requirements", text(source.cold_start_requirements), "info") : ""}${source.raw ? ui.technical(source.raw) : ""}`;
    return pageFrame(ui, "Portfolio allocations", "Current authority and proposed policy review are separate records.", body, state);
  }
  const execution = isObject(source.execution) ? source.execution : {};
  const ledgerEnvelope = isObject(ctx?.ledger) ? ctx.ledger : {};
  const ledger = { ...source, ...execution, ...ledgerEnvelope };
  const orders = [...arrayFrom(ledger, ["canary_submission_attempts"]), ...arrayFrom(ledger, ["canary_position_requests"])];
  if (!orders.length) orders.push(...arrayFrom(ledger, ["orders", "submissions"]));
  const fills = [...arrayFrom(ledger, ["canary_position_fills"]), ...arrayFrom(ledger, ["canary_risk_fills"])];
  if (!fills.length) fills.push(...arrayFrom(ledger, ["fills"]));
  const roundTrips = arrayFrom(ledger, ["round_trips", "closed_round_trips", "closed"]);
  const payouts = arrayFrom(ledger, ["payouts", "resolution_payouts", "cashflows"]);
  const inventory = arrayFrom(ledger, ["inventory", "canary_position_lots", "open_inventory", "positions"]);
  const marks = arrayFrom(ledger, ["marks", "position_marks"]);
  const unknown = arrayFrom(ledger, ["unknown_obligations", "unknown_orders", "unknown"]);
  const reservations = arrayFrom(ledger, ["reservations", "canary_risk_reservations"]);
  const reservationSection = recordsOrMissing(ui, ledger, ["reservations", "canary_risk_reservations"], reservations, () => table(ui, [{ label: "Reservation", render: (row) => detailLink(ui, route, first(value(row, "reservation_id", "id"), "Unnamed reservation"), idOf(row), "reservation") }, { label: "Side", render: (row) => ui.esc(first(value(row, "side"), "Not available")) }, { label: "Quantity", render: (row) => shown(ui, row, ["reserved_quantity", "quantity"], "number") }, { label: "Reserved cost", render: (row) => shown(ui, row, ["reserved_cost", "amount", "notional"], "money") }, { label: "Status", render: (row) => badge(ui, value(row, "status", "state")) }, { label: "Held at", render: (row) => shown(ui, row, ["reserved_at", "created_at", "updated_at"], "time") }], reservations, "Risk reservations"), "Risk reservations");
  const authority = `<article class="panel"><h3>Execution authority</h3><dl class="detail-grid">${ui.kv("Authorization", badge(ui, value(source, "execution_authorization.status", "execution_authorization.state")))}${ui.kv("Mode", ui.esc(text(value(source, "execution_authorization.mode"))))}${ui.kv("Worker decision", badge(ui, value(source, "worker.next_decision", "autonomous.next_decision", "worker.blocker")))}${ui.kv("Worker tick", shown(ui, source.worker || {}, ["last_tick_at", "tick_at", "updated_at"], "time"))}${ui.kv("Readiness", badge(ui, value(source, "readiness.status")))}${ui.kv("Account", ui.esc(text(value(source, "readiness.account", "connectivity.account"))))}${ui.kv("Control state", badge(ui, value(source, "operator_controls.armed_state", "autonomous.control_state", "control_state")))}${ui.kv("Decision", badge(ui, value(source, "decision.status", "selection_status")))}</dl><p class="muted">Review is readable and non-activating. This page never grants permission or infers readiness.</p></article>`;
  const body = `${stateBody(ui, data, ctx)}${authority}${summary(ui, source, [["Canary status", ["control_state"]], ["Remaining daily capacity", ["risk_settings.remaining.gross_daily_buy_usd"], "money"], ["Realized result", ["execution.today_realized_pnl"], "money"], ["Execution events", ["execution.event_count", "execution.real_execution_events"]], ["Today orders", ["execution.today_orders"]], ["Total exposure", ["execution.total_exposure"], "money"], ["Open positions", ["execution.open_positions"]], ["Last request", ["execution.last_request_status"]]])}${recordsOrMissing(ui, ledger, ["orders", "canary_submission_attempts", "canary_position_requests", "submissions"], orders, () => renderOrders(ui, orders, route), "Orders")}${recordsOrMissing(ui, ledger, ["fills", "canary_position_fills", "canary_risk_fills"], fills, () => renderFills(ui, fills, route), "Fills")}${recordsOrMissing(ui, ledger, ["round_trips", "closed_round_trips"], roundTrips, () => renderRoundTrips(ui, roundTrips, route), "Closed positions and settled round trips")}${recordsOrMissing(ui, ledger, ["payouts", "resolution_payouts"], payouts, () => table(ui, [{ label: "Payout", render: (row) => detailLink(ui, route, first(value(row, "flow_id", "payout_id", "id"), "Unnamed cashflow"), idOf(row), "cashflow") }, { label: "Time", render: (row) => shown(ui, row, ["timestamp", "resolved_at", "occurred_at"], "time") }], payouts, "Resolution payouts"), "Resolution payouts")}${recordsOrMissing(ui, ledger, ["inventory", "canary_position_lots", "open_inventory", "positions"], inventory, () => table(ui, [{ label: "Position / market", render: (row) => detailLink(ui, route, first(value(row, "position_id", "market_id", "question", "name"), "Unnamed position"), idOf(row), value(row, "record_kind") || "position") }, { label: "Token", render: (row) => ui.esc(first(value(row, "token", "outcome", "side"), "Not available")) }, { label: "Quantity", render: (row) => shown(ui, row, ["quantity", "pending_exit_quantity"], "number") }, { label: "Cost basis", render: (row) => shown(ui, row, ["cost_basis", "open_cost"], "money") }, { label: "Status", render: (row) => badge(ui, value(row, "status", "state")) }], inventory, "Position inventory"), "Position inventory")}${recordsOrMissing(ui, ledger, ["marks", "position_marks"], marks, () => table(ui, [{ label: "Position / market", render: (row) => detailLink(ui, route, first(value(row, "position_id", "market_id"), "Unnamed position"), idOf(row), value(row, "record_kind") || "mark") }, { label: "Mark price", render: (row) => shown(ui, row, ["mark_price", "price"], "number") }, { label: "Marked value", render: (row) => shown(ui, row, ["marked_value", "value"], "money") }, { label: "Marked at", render: (row) => shown(ui, row, ["marked_at", "timestamp"], "time") }], marks, "Position marks"), "Position marks")}${recordsOrMissing(ui, ledger, ["unknown_obligations", "unknown_orders", "unknown"], unknown, () => table(ui, [{ label: "Record", render: (row) => { const linkId = first(value(row, "request_id", "attempt_id", "reservation_id", "fill_id")); const kind = value(row, "request_id") ? "order" : value(row, "attempt_id") ? "submission" : value(row, "reservation_id") ? "reservation" : value(row, "fill_id") ? "fill" : ""; const label = first(value(row, "position_id", "market_id", "question", "name", "record_id", "id"), "Unknown obligation"); return linkId && kind ? detailLink(ui, route, label, linkId, kind) : ui.esc(label); } }, { label: "Requested cost", render: (row) => shown(ui, row, ["requested_cost"], "money") }, { label: "Filled cost", render: (row) => shown(ui, row, ["filled_cost"], "money") }, { label: "Remaining cost", render: (row) => shown(ui, row, ["remaining_cost"], "money") }, { label: "Unknown amount", render: (row) => shown(ui, row, ["amount", "notional"], "money") }, { label: "Reason", render: (row) => ui.esc(first(value(row, "reason", "status"), "Outcome uncertain")) }], unknown, "Unknown obligations"), "Unknown obligations")}${ui.technical({ execution_authorization: source.execution_authorization, operator_controls: source.operator_controls, risk_settings: source.risk_settings })}`;
  return pageFrame(ui, "Real canary portfolio", "Execution records and accounting evidence from the canary only; practice results stay separate.", `${body}${reservationSection}`, state);
}

function renderMarkets(ctx, route, ui, data) {
  const source = envelope(data);
  const rows = arrayFrom(source, ["items", "markets"]);
  const body = `${stateBody(ui, data, ctx)}${summary(ui, source, [["Markets in bounded page", ["items.length", "count"]], ["Total available", ["total"]], ["Page", ["page"]], ["Updated", ["updated_at", "as_of"], "time"]])}${table(ui, [{ label: "Question / name", render: (row) => detailLink(ui, route, first(value(row, "question", "name", "title"), "Unnamed market"), idOf(row), "market") }, { label: "YES mid", render: (row) => shown(ui, row, ["snapshot.yes_mid"], "number") }, { label: "YES ask", render: (row) => shown(ui, row, ["snapshot.yes_ask"], "number") }, { label: "YES bid", render: (row) => shown(ui, row, ["snapshot.yes_bid"], "number") }, { label: "NO mid", render: (row) => shown(ui, row, ["snapshot.no_mid"], "number") }, { label: "NO ask", render: (row) => shown(ui, row, ["snapshot.no_ask"], "number") }, { label: "NO bid", render: (row) => shown(ui, row, ["snapshot.no_bid"], "number") }, { label: "Category", render: (row) => ui.esc(first(value(row, "category", "market_category"), "Not available")) }, { label: "Settlement", render: (row) => badge(ui, value(row, "settlement", "status", "state")) }], rows, "Bounded Polymarket market list")}${source.raw ? ui.technical(source.raw) : ""}`;
  return pageFrame(ui, "Markets", "Questions and order-book labels are reported as observed; no predicted chance is inferred.", body, stateFor(data, ctx));
}

function renderStrategyPage(ctx, route, ui, data) {
  const source = envelope(data);
  const rows = arrayFrom(source, ["items", "candidates"]);
  const body = `${stateBody(ui, data, ctx)}${summary(ui, source, [["Candidates in bounded page", ["items.length", "count"]], ["Total candidates", ["total"]], ["Research state", ["status", "research_status"]], ["Updated", ["updated_at", "as_of"], "time"]])}${table(ui, [{ label: "Candidate", render: (row) => detailLink(ui, route, first(value(row, "name", "candidate_name", "candidate_id", "strategy_id"), "Unnamed candidate"), idOf(row)) }, { label: "Family", render: (row) => ui.esc(first(value(row, "family"), "Not available")) }, { label: "Stage", render: (row) => badge(ui, value(row, "stage", "paper_status", "canary_status")) }, { label: "Operational setup", render: (row) => evidenceValue(ui, row, ["payload.operational_setup"], "Operational setup") }, { label: "Strategy document", render: (row) => evidenceValue(ui, row, ["payload.strategy_document"], "Strategy document") }, { label: "Canonical strategy", render: (row) => evidenceValue(ui, row, ["payload.canonical_strategy"], "Canonical strategy") }, { label: "Exit policy", render: (row) => evidenceValue(ui, row, ["payload.exit_policy"], "Exit policy") }, { label: "Provenance", render: (row) => evidenceValue(ui, row, ["provenance"], "Candidate provenance") }], rows, "Research candidates")}${source.raw ? ui.technical(source.raw) : ""}`;
  return pageFrame(ui, "Research strategies", "Research, paper, live evidence, and adverse outcomes remain visible as distinct stages.", body, stateFor(data, ctx));
}

function renderCryptoPage(ctx, route, ui, data) {
  const source = envelope(data);
  const rows = arrayFrom(source, ["items", "catalog", "catalogs"]);
  const sectionRows = (keys) => rowsFromEnvelopes(source, keys);
  source.reports = sectionRows(["reports"]);
  source.strategy_reports = sectionRows(["strategy_reports"]);
  source.bootstrap_reports = sectionRows(["bootstrap_reports"]);
  source.catalogs = sectionRows(["catalogs"]);
  source.universe_versions = sectionRows(["universe_versions"]);
  const catalogSections = [
    ["Reports", source.reports],
    ["Strategy reports", source.strategy_reports],
    ["Bootstrap reports", source.bootstrap_reports],
    ["Catalogs", source.catalogs],
    ["Universe versions", source.universe_versions],
  ];
  const sectionMarkup = catalogSections
    .map(([label, values]) => table(ui, [
      { label: "Identity", render: (row) => ui.esc(first(value(row, "report_id", "dataset_id", "symbol", "universe_version", "id"), "Not available")) },
      { label: "Status", render: (row) => badge(ui, value(row, "status", "quality", "validation.status")) },
      { label: "Source", render: (row) => ui.esc(first(value(row, "source_type", "provider", "source"), "Not available")) },
      { label: "Details", render: (row) => evidenceValue(ui, row, ["metadata", "payload", "validation"], `${label} details`) },
    ], values, label)).join("");
  const body = `${stateBody(ui, data, ctx)}${summary(ui, source, [["Catalogs in bounded page", ["items.length", "count"]], ["Universe versions", ["universe_versions.length", "universe_version_count"]], ["Symbols", ["symbol_count", "symbols"]], ["Reports", ["reports.length", "report_count"]], ["Bootstrap reports", ["bootstrap_reports.length", "bootstrap_report_count"]]])}<div class="notice info"><strong>Research only</strong><p>Crypto catalogs, reports, and bootstrap evidence are parked research records. This view has no activation or trading control.</p></div>${table(ui, [
    { label: "Dataset / symbol", render: (row) => detailLink(ui, route, first(value(row, "symbol", "dataset_id", "name"), "Unnamed catalog"), first(value(row, "symbol", "dataset_id", "id"), idOf(row))) },
    { label: "Version", render: (row) => ui.esc(first(value(row, "dataset_version", "version", "universe_version"), "Not available")) },
    { label: "Coverage", render: (row) => ui.esc(first(value(row, "coverage", "completeness"), "Not available")) },
    { label: "Source", render: (row) => ui.esc(first(value(row, "source_type", "provider"), "Not available")) },
    { label: "Validation", render: (row) => badge(ui, value(row, "validation.status", "quality")) },
    { label: "Updated", render: (row) => shown(ui, row, ["updated_at", "last_updated", "created_at"], "time") },
  ], rows, "Crypto research datasets and catalogs")}${sectionMarkup}${source.raw ? ui.technical(source.raw) : ""}`;
  return pageFrame(ui, "Crypto research", "Catalog, coverage, source, report, and bootstrap provenance without a trading implication.", body, stateFor(data, ctx));
}

function renderAutomationPage(ctx, route, ui, data) {
  const source = envelope(data);
  const queue = arrayFrom(source, ["items", "queue", "jobs"]);
  const shadowResponse = ctx?.detail?.shadow_list;
  const shadow = arrayFrom(shadowResponse || source.shadow, ["items", "jobs", "records"]);
  const shadowBody = shadowResponse !== undefined || source.shadow !== undefined
    ? table(ui, [
      { label: "Job", render: (row) => detailLink(ui, route, first(value(row, "job_id", "name", "id"), "Unnamed job"), idOf(row, "shadow"), "shadow") },
      { label: "Status", render: (row) => badge(ui, value(row, "status", "state", "outcome")) },
      { label: "Next evaluation", render: (row) => shown(ui, row, ["next_evaluation_at", "next_evaluation", "updated_at"], "time") },
      { label: "Blockers", render: (row) => evidenceValue(ui, row, ["blockers", "last_blocker"], "Shadow blockers") },
    ], shadow, "Shadow jobs") + detailPager(ui, route, shadowResponse || source.shadow, "shadow", "Shadow jobs")
    : ui.empty("Shadow jobs unavailable", "The bounded shadow-job projection was not returned; Hermes queue evidence remains separate.");
  const body = `${stateBody(ui, data, ctx)}${summary(ui, source, [["Queue items", ["total", "count"]], ["Pending", ["pending"]], ["Next schedule", ["next_schedule", "next_scheduled_work"], "time"], ["Hermes connection", ["connection", "hermes_connection"]], ["Heartbeat", ["heartbeat_at", "last_heartbeat"], "time"]])}<div class="split"><article class="panel"><h3>Research queue</h3>${table(ui, [{ label: "Item", render: (row) => detailLink(ui, route, first(value(row, "item_id", "title", "id"), "Unnamed item"), idOf(row, "hermes"), "hermes") }, { label: "Status", render: (row) => badge(ui, value(row, "status", "outcome_type", "outcome_label")) }, { label: "Human reason", render: (row) => ui.esc(first(value(row, "human_reason", "reason"), "Not available")) }, { label: "Available at", render: (row) => shown(ui, row, ["available_at", "time", "updated_at"], "time") }, { label: "Source", render: (row) => ui.esc(first(value(row, "source", "item_type"), "Not available")) }], queue, "Hermes research queue")}</article><article class="panel"><h3>Shadow jobs</h3>${shadowBody}</article></div><div class="notice info"><strong>Connection is not authority</strong><p>Hermes connection and worker heartbeat describe research automation only. Testing or completed jobs do not authorize live execution.</p></div>${source.raw ? ui.technical(source.raw) : ""}`;
  return pageFrame(ui, "Research automation", "Separate queue outcomes, external connection state, schedule, and heartbeat.", body, stateFor(data, ctx));
}

function renderDataPage(ctx, route, ui, data) {
  const source = envelope(data);
  const rows = arrayFrom(source, ["items", "datasets", "catalog"]);
  const body = `${stateBody(ui, data, ctx)}${summary(ui, source, [["Datasets in bounded page", ["items.length", "count"]], ["Total datasets", ["total"]], ["Page", ["page"]], ["Updated", ["updated_at", "as_of"], "time"]])}${table(ui, [{ label: "Dataset", render: (row) => detailLink(ui, route, first(value(row, "name", "dataset_name", "dataset_id"), "Unnamed dataset"), idOf(row)) }, { label: "Version", render: (row) => ui.esc(first(value(row, "dataset_version", "version"), "Not available")) }, { label: "Coverage", render: (row) => ui.esc(first(value(row, "coverage", "coverage_status"), "Not available")) }, { label: "Source", render: (row) => ui.esc(first(value(row, "source_type", "source"), "Not available")) }, { label: "Instrument", render: (row) => ui.esc(first(value(row, "instrument", "market_type"), "Not available")) }, { label: "Updated", render: (row) => shown(ui, row, ["updated_at", "created_at"], "time") }], rows, "Bounded dataset catalog")}${source.raw ? ui.technical(source.raw) : ""}`;
  return pageFrame(ui, "Data catalog", "Manifests, versions, coverage, and bounded missing ranges retain their source distinction.", body, stateFor(data, ctx));
}

function isFinancialEvent(row) {
  const category = lower(value(row, "category", "kind", "type", "domain"));
  return Boolean(value(row, "financial", "is_financial")) || ["trading", "order", "fill", "payout", "position", "inventory", "accounting", "money"].some((term) => category.includes(term));
}

function groupedActivity(rows) {
  const groups = [];
  const byKey = new Map();
  rows.forEach((row) => {
    const message = text(first(value(row, "message", "title", "description", "event")), "Activity event");
    const key = isFinancialEvent(row) ? `unique:${groups.length}` : `${lower(value(row, "category", "kind", "type"))}|${message}`;
    const existing = byKey.get(key);
    if (!existing) {
      const group = { ...row, __message: message, __count: 1, __first: value(row, "timestamp", "created_at", "at"), __last: value(row, "timestamp", "created_at", "at") };
      groups.push(group);
      byKey.set(key, group);
      return;
    }
    existing.__count += 1;
    existing.__last = value(row, "timestamp", "created_at", "at") || existing.__last;
  });
  return groups;
}

function renderActivity(ctx, route, ui, data) {
  const source = envelope(data);
  const rows = groupedActivity(arrayFrom(source, ["items", "events", "activity"]));
  const body = `${stateBody(ui, data, ctx)}${summary(ui, source, [["Events in loaded page", ["items.length", "count"]], ["Total events", ["total"]], ["Kinds", ["kind_count", "kinds"]], ["Latest", ["latest_at", "updated_at"], "time"]])}<div class="notice info"><strong>Page-local grouping</strong><p>Identical nonfinancial messages are grouped only within this bounded page. Trading and accounting events remain individual audit evidence.</p></div>${table(ui, [{ label: "Activity", render: (row) => detailLink(ui, route, first(value(row, "title", "message", "event_id", "id"), "Activity event"), idOf(row, "activity"), "activity") }, { label: "First time", render: (row) => shown(ui, row, ["__first", "timestamp", "created_at", "at"], "time") }, { label: "Last time", render: (row) => shown(ui, row, ["__last", "__first", "timestamp", "created_at", "at"], "time") }, { label: "Kind", render: (row) => ui.esc(first(value(row, "kind", "type", "category"), "Not available")) }, { label: "Source", render: (row) => ui.esc(first(value(row, "source", "source_type"), "Not available")) }, { label: "Status", render: (row) => badge(ui, value(row, "status", "severity", "level")) }, { label: "Count", render: (row) => ui.esc(row.__count > 1 ? `${row.__count} repeated` : "1") }], rows, "Activity evidence")}`;
  return pageFrame(ui, "Activity", "A chronological, bounded audit view with truthful kind, source, status, and grouped time span.", body, stateFor(data, ctx));
}

function renderBinance(ctx, route, ui, data) {
  const source = envelope(data);
  const identity = isObject(source.identity) ? source.identity : source;
  const status = labelFor(ui, first(value(source, "status.state", "status.status", "status", "state", "environment"), "Not available"));
  const strictTestnet = source.strict_testnet === true || value(source, "status.strict_testnet") === true;
  const rows = rowsFromEnvelopes(source, ["items", "orders", "fills", "positions", "unknown", "unknown_obligations", "records"]);
  const confirmation = first(value(source, "enable_phrase", "status.enable_phrase"), "");
  const button = (label, action, extra = "") => `<button class="binance-action${action === "KILL" ? " danger" : ""}" data-action="binance-control" data-binance-action="${ui.esc(action)}"${extra}>${ui.esc(label)}</button>`;
  const actionMarkup = strictTestnet
    ? `<article class="panel" data-binance-profile="strict-testnet"><div class="section-title"><h3>Autonomous Testnet controls</h3><span class="badge warn">STRICT TESTNET</span></div><p class="page-note">Price and quantity are computed automatically from Binance exchange filters and the frozen bounded Testnet envelope.</p><p class="page-note">${button("Check Testnet connectivity", "CONNECTIVITY_CHECK")} ${button("Validate an order", "ORDER_VALIDATION_TEST")} ${button("Pause autonomous Testnet", "PAUSE")} ${button("Disarm autonomous Testnet", "DISARM")} ${button("Kill autonomous Testnet", "KILL")}</p><p class="notice">Browser controls are limited to read-only connectivity, order validation, and risk-reducing pause, disarm, or kill. Autonomous enable/resume is CLI-only; window-seconds 30..900.</p></article>`
    : `<article class="panel" data-binance-profile="paper-testnet"><div class="section-title"><h3>Binance controls</h3><span class="badge warn">PAPER / TESTNET</span></div><p class="page-note">These actions use the existing isolated Binance control endpoint. They do not enable a Polymarket transport or change credentials.</p><div class="three-col"><label class="key-value"><span class="key">Order symbol</span><input data-binance-field="symbol" aria-label="Order validation symbol" autocomplete="off" placeholder="BTCUSDT"></label><label class="key-value"><span class="key">Order price</span><input data-binance-field="price" aria-label="Order validation price" inputmode="decimal" autocomplete="off" placeholder="price"></label><label class="key-value"><span class="key">Order quantity</span><input data-binance-field="quantity" aria-label="Order validation quantity" inputmode="decimal" autocomplete="off" placeholder="quantity"></label></div><p class="page-note">${button("Check connectivity", "CONNECTIVITY_CHECK")} ${button("Validate an order", "ORDER_VALIDATION_TEST")} ${button("Enable autonomous canary", "ENABLE", confirmation ? ` data-binance-confirmation="${ui.esc(text(confirmation))}"` : "")} ${button("Pause autonomous canary", "PAUSE")} ${button("Resume autonomous canary", "RESUME", confirmation ? ` data-binance-confirmation="${ui.esc(text(confirmation))}"` : "")} ${button("Disarm autonomous canary", "DISARM")} ${button("Kill autonomous canary", "KILL")}</p><p class="notice">The native control plane remains authoritative for confirmation, readiness, and action outcomes. No browser action is automatic.</p></article>`;
  const body = `${stateBody(ui, data, ctx)}${summary(ui, source, [["Venue state", ["status.state", "status.status", "status", "state", "environment"]], ["Mode", ["mode", "context", "profile"]], ["Orders observed", ["total", "count"]], ["Last heartbeat", ["heartbeat_at", "updated_at"], "time"]])}<div class="notice info"><strong>Binance view: ${ui.esc(text(status))}</strong><p>This projection does not read credentials or infer connectivity. Parked, not configured, paper, and strict Testnet contexts remain distinct from the Polymarket canary.</p></div>${actionMarkup}<article class="panel"><h3>Venue identity and restrictions</h3><dl class="detail-grid">${ui.kv("Environment", ui.esc(text(first(value(identity, "environment", "venue"), "Not available"))))}${ui.kv("Transport", ui.esc(text(first(value(identity, "transport", "transport_status"), "Not available"))))}${ui.kv("Account", ui.esc(text(first(value(identity, "account_status", "credentials.status"), "Not available"))))}${ui.kv("Execution", badge(ui, first(value(identity, "execution_status", "execution.state", "execution.status", "execution"), "PARKED")))}</dl></article>${table(ui, [{ label: "Symbol", render: (row) => detailLink(ui, route, first(value(row, "symbol", "name"), "Unnamed symbol"), idOf(row)) }, { label: "Stage", render: (row) => badge(ui, value(row, "status.state", "status.status", "status", "stage", "state")) }, { label: "Side", render: (row) => ui.esc(first(value(row, "side", "outcome"), "Not available")) }, { label: "Quantity", render: (row) => shown(ui, row, ["quantity", "size"], "number") }, { label: "Time", render: (row) => shown(ui, row, ["timestamp", "updated_at"], "time") }], rows, "Observed Binance records")}${source.probe ? ui.technical(source.probe, "Testnet probe evidence") : ""}${source.actions ? ui.technical(source.actions, "Existing advanced actions and restrictions") : ""}`;
  return pageFrame(ui, "Binance", "Parked or Testnet evidence is shown accurately without enabling a venue or changing the operating profile.", body, stateFor(data, ctx));
}

function detailTitle(route, detail) {
  if (route.section === "strategies") return first(value(detail, "name", "strategy_name", "candidate_id"), "Strategy details");
  if (route.section === "crypto") return first(value(detail, "symbol", "name", "report_id"), "Crypto research details");
  if (route.section === "automation") return first(value(detail, "item_id", "name", "title", "job_id", "id"), "Automation details");
  if (route.section === "data") return first(value(detail, "name", "dataset_name", "dataset_id"), "Dataset details");
  if (route.view === "markets") return first(value(detail, "question", "name", "title"), "Market details");
  return first(value(detail, "name", "title", "question", "id"), "Details");
}

function detailPager(ui, route, response, key, label) {
  if (!isObject(response)) return "";
  const nativePages = Number(response.pages);
  const hasNativePages = Number.isFinite(nativePages) && nativePages > 0;
  const hasMore = response.has_more === true;
  if (!hasNativePages && !hasMore) return "";
  const total = Number(response.total ?? response.count);
  const current = Math.max(1, Number(response.page) || Number(route[key === "shadow" ? "shadow_page" : "detail_page"]) || 1);
  const field = key === "shadow" ? "shadow_page" : "detail_page";
  const previous = current > 1 ? ui.link("Previous", { [field]: current - 1 }) : `<span class="button button-quiet" aria-disabled="true">Previous</span>`;
  const nextAvailable = hasMore || (hasNativePages && current < nativePages);
  const next = nextAvailable ? ui.link("Next", { [field]: current + 1 }) : `<span class="button button-quiet" aria-disabled="true">Next</span>`;
  const pageLabel = hasNativePages ? `Page ${Math.min(current, nativePages)} of ${nativePages}` : `Page ${current}`;
  const totalLabel = Number.isFinite(total) && total >= 0 ? ` · ${total} bounded records` : "";
  return `<nav class="detail-pagination" aria-label="${ui.esc(label)} pages"><span class="muted">${pageLabel}${totalLabel}</span><span class="detail-pagination-actions">${previous}${next}</span></nav>`;
}

function renderDetail(ctx) {
  const route = normalizeRoute(ctx?.route);
  const ui = uiFor(ctx);
  const selected = selectedOf(route);
  if (!selected) return ui.empty("Select an item", "Choose a named record to keep its detail view open across refreshes.");
  const map = detailMap(ctx);
  const detailKey = route.view === "markets"
    ? "market"
    : route.view === "research" && route.section === "strategies"
      ? "candidate"
      : route.view === "research" && route.section === "crypto"
        ? "crypto"
        : route.view === "research" && route.section === "automation"
          ? (route.record_kind === "shadow" ? "shadow" : "hermes")
          : route.view === "research" && route.section === "data"
            ? "dataset"
            : String(route.record_kind || "record").toLowerCase();
  const cached = map[selected] || map[detailKey] || map[route.record_kind] || map.record || map[route.section] || map[route.view];
  const cachedKind = isObject(cached) && cached.kind !== undefined ? String(cached.kind).toLowerCase() : "";
  const identityKind = cachedKind || detailKey;
  const selectedRow = arrayFrom(ctx?.data, ["items", "orders", "fills", "records", "active_rows"]).find((row) => idOf(row, detailKey) === String(selected));
  const hasCached = cached !== undefined && cached !== null;
  const cachedRecord = isObject(cached) && isObject(cached.record)
    ? cached.record
    : isObject(cached) && Array.isArray(cached.items)
      ? cached.items.find((row) => idOf(row, identityKind) === String(selected))
      : cached;
  const canonicalIdentity = ["crypto", "crypto-research", "shadow", "hermes", "activity"].includes(identityKind.toLowerCase());
  const cachedId = canonicalIdentity
    ? idOf(cachedRecord, identityKind)
    : (isObject(cached) && cached.id !== undefined ? String(cached.id) : idOf(cachedRecord, identityKind));
  const recordId = idOf(cachedRecord, identityKind);
  const financialKinds = ["order", "submission", "reservation", "fill", "risk-fill", "position", "mark", "cashflow"];
  const kindMatches = !cachedKind || cachedKind === detailKey || (detailKey === "record" && financialKinds.includes(cachedKind));
  const validCached = isObject(cachedRecord) && cachedId === String(selected) && (!recordId || recordId === String(selected)) && kindMatches ? cachedRecord : null;
  const direct = isObject(ctx?.data) && idOf(ctx.data, detailKey) === String(selected) ? ctx.data : null;
  const candidate = validCached || (!hasCached ? selectedRow || direct : null);
  const detail = isObject(candidate) && isObject(candidate.item) ? candidate.item : candidate;
  if (!isObject(detail)) return ui.empty("Details unavailable", "The selected record is no longer in the bounded response; refresh the list before opening it.");
  const embeddedEvents = list(detail.events || detail.evidence || detail.timeline);
  const embeddedRanges = list(detail.missing_ranges || detail.ranges || detail.gaps);
  const eventResponseFailed = (isObject(map.events) && (nonEmpty(map.events.error) || nonEmpty(map.events.errors))) || nonEmpty(ctx?.errors?.["detail:events"]);
  const gapResponseFailed = (isObject(map.gaps) && (nonEmpty(map.gaps.error) || nonEmpty(map.gaps.errors))) || nonEmpty(ctx?.errors?.["detail:gaps"]);
  const events = map.events !== undefined && !eventResponseFailed ? arrayFrom(map.events, ["items", "events", "records"]) : embeddedEvents;
  const ranges = map.gaps !== undefined && !gapResponseFailed ? arrayFrom(map.gaps, ["items", "ranges", "gaps"]) : embeddedRanges;
  const records = arrayFrom(detail, ["items", "orders", "fills", "records"]);
  const state = stateFor(detail, ctx);
  const fields = [
    ["Status", badge(ui, value(detail, "status", "state", "stage", ...(detailKey === "market" ? ["settlement"] : ["outcome"])))],
    ["Reason", ui.esc(text(first(value(detail, "reason", "explanation", "blocker"))))],
    ["Updated", shown(ui, detail, ["updated_at", "created_at", "timestamp"], "time")],
    ["Source", ui.esc(text(first(value(detail, "source", "source_type", "provenance.source"))))],
  ];
  const nativeFields = [
    ["Market", value(detail, "market_id", "market", "market_name")],
    ...(detailKey === "market" ? [] : [["Selected outcome", value(detail, "selected_outcome", "outcome", "token", "outcome_name")]]),
    ["Side", value(detail, "side")],
    ["Requested quantity", value(detail, "requested_quantity", "quantity", "size")],
    ["Filled quantity", value(detail, "filled_quantity")],
    ["Requested cost", value(detail, "requested_cost")],
    ["Remaining cost", value(detail, "remaining_cost")],
    ["Filled cost", value(detail, "filled_cost")],
    ["Marked value", value(detail, "marked_value")],
    ["Submitted price", value(detail, "submitted_price", "requested_price", "limit_price")],
    ["Fill price", value(detail, "fill_price", "price")],
    ["Fees", value(detail, "fees", "fee", "fee_amount", "commission")],
    ["Settlement", value(detail, "settlement_status", "settlement", "resolution")],
    ["Phase", value(detail, "phase", "stage")],
    ["Feasibility", value(detail, "feasibility", "feasibility_status", "feasible")],
    ["Last error", value(detail, "last_error", "error", "failure_reason")],
  ].filter(([, raw]) => raw !== undefined && raw !== null && raw !== "");
  const financialFields = detailKey === "mark"
    ? [
      ["Mark price", shown(ui, detail, ["mark_price"], "number")],
      ["Observed at", shown(ui, detail, ["observed_at", "marked_at"], "time")],
    ]
    : detailKey === "cashflow"
      ? [
        ["Amount", shown(ui, detail, ["amount"], "money")],
        ["Native flow kind", ui.esc(text(first(value(detail, "kind"), "Not available")))],
        ["Occurred at", shown(ui, detail, ["occurred_at"], "time")],
      ]
      : [];
  const visibleFinancialFields = financialFields.filter(([, rendered]) => rendered !== "");
  const domainFields = detailKey === "market"
    ? [
      ["Question", value(detail, "question", "name", "title")],
      ["Market ID", value(detail, "market_id", "id")],
      ["Market type", value(detail, "market_type", "market")],
      ["Category", value(detail, "category")],
      ["Source type", value(detail, "source_type", "source")],
      ["Timeframe", value(detail, "timeframe")],
      ["Settlement", value(detail, "settlement", "resolution")],
      ["Available outcomes", value(detail, "outcome", "outcomes")],
      ["Quality", value(detail, "quality", "research_quality")],
      ["Freshness", value(detail, "freshness", "freshness_status", "quality_context")],
      ["Observed at", value(detail, "observed_at", "timestamp"), "time"],
      ["YES mid", value(detail, "snapshot.yes_mid", "yes_mid")],
      ["YES bid", value(detail, "snapshot.yes_bid", "yes_bid")],
      ["YES ask", value(detail, "snapshot.yes_ask", "yes_ask")],
      ["NO mid", value(detail, "snapshot.no_mid", "no_mid")],
      ["NO bid", value(detail, "snapshot.no_bid", "no_bid")],
      ["NO ask", value(detail, "snapshot.no_ask", "no_ask")],
      ["Book depth", value(detail, "depth", "liquidity")],
      ["Minimum affordable", value(detail, "min_affordable", "minimum_affordable")],
    ]
    : detailKey === "crypto"
      ? [
        ["Symbol", value(detail, "symbol")],
        ["Dataset ID", value(detail, "dataset_id")],
        ["Dataset version", value(detail, "dataset_version", "version")],
        ["Market type", value(detail, "market_type", "market")],
        ["Instrument", value(detail, "instrument")],
        ["Universe version", value(detail, "universe_version")],
        ["Source type", value(detail, "source_type", "source")],
        ["Quality", value(detail, "quality", "research_quality")],
        ["Coverage", value(detail, "coverage")],
        ["Coverage start", value(detail, "coverage.start", "coverage.start_timestamp"), "time"],
        ["Coverage end", value(detail, "coverage.end", "coverage.end_timestamp"), "time"],
        ["Report status", value(detail, "report_status", "report.status")],
        ["Report updated", value(detail, "report.updated_at", "report.created_at", "report.timestamp"), "time"],
      ]
      : detailKey === "dataset"
        ? [
          ["Dataset ID", value(detail, "dataset_id", "id")],
          ["Dataset version", value(detail, "dataset_version", "version")],
          ["Provider", value(detail, "provider")],
          ["Instrument", value(detail, "instrument")],
          ["Market type", value(detail, "market_type", "market")],
          ["Timeframe", value(detail, "timeframe")],
          ["Quality", value(detail, "quality")],
          ["Source type", value(detail, "source_type", "source")],
          ["Rows", value(detail, "row_count", "rows")],
          ["Completeness", value(detail, "completeness", "coverage")],
          ["Start", value(detail, "start_timestamp", "start"), "time"],
          ["End", value(detail, "end_timestamp", "end"), "time"],
          ["Missing ranges", value(detail, "missing_range_count")],
          ["Aggregate health", detail.health === null ? "Not computed (stored catalog metadata only)" : value(detail, "health")],
        ]
        : detailKey === "hermes"
          ? [
            ["Item ID", value(detail, "item_id", "id")],
            ["Item type", value(detail, "item_type", "category")],
            ["Title", value(detail, "title", "name")],
            ["Human reason", value(detail, "human_reason", "reason")],
            ["Available at", value(detail, "available_at", "time"), "time"],
            ["Next schedule", value(detail, "next_schedule", "next_scheduled_work"), "time"],
            ["Submitted at", value(detail, "submitted_at"), "time"],
            ["Completed at", value(detail, "completed_at"), "time"],
            ["Outcome", value(detail, "outcome_label", "outcome_type", "outcome")],
            ["Source", value(detail, "source", "source_type")],
          ]
          : detailKey === "shadow"
            ? [
              ["Job ID", value(detail, "job_id", "id")],
              ["Venue", value(detail, "venue")],
              ["Heartbeat", value(detail, "heartbeat_at", "last_heartbeat"), "time"],
              ["Next evaluation", value(detail, "next_evaluation_at", "next_evaluation"), "time"],
              ["Blockers", value(detail, "blockers", "last_blocker")],
            ]
            : [];
  const visibleDomainFields = domainFields.filter(([, raw]) => raw !== undefined && raw !== null && raw !== "");
  const identifiers = [
    ["Record ID", value(detail, "record_id", "id")],
    ["Attempt ID", value(detail, "attempt_id")],
    ["Request ID", value(detail, "request_id")],
    ["Reservation ID", value(detail, "reservation_id")],
    ["Fill ID", value(detail, "fill_id")],
    ["Intent ID", value(detail, "intent_id")],
    ["Event ID", value(detail, "event_id")],
    ["Authorization ID", value(detail, "execution_authorization_id")],
    ["Position ID", value(detail, "position_id")],
    ["Mark ID", value(detail, "mark_id")],
    ["Flow ID", value(detail, "flow_id")],
  ].filter(([, raw]) => raw !== undefined && raw !== null && raw !== "");
  const fieldMarkup = fields.map(([label, rendered]) => ui.kv(label, rendered)).join("")
    + nativeFields.map(([label, raw]) => ui.kv(label, fieldValue(ui, label, raw))).join("")
    + visibleFinancialFields.map(([label, rendered]) => ui.kv(label, rendered)).join("");
  const domainTitle = detailKey === "market" ? "Observed market details" : detailKey === "crypto" ? "Native crypto catalog details" : detailKey === "dataset" ? "Native dataset catalog details" : detailKey === "hermes" ? "Hermes item details" : detailKey === "shadow" ? "Shadow job details" : "";
  const domainMarkup = visibleDomainFields.length
    ? `<section><h4>${ui.esc(domainTitle)}</h4><dl class="detail-grid">${visibleDomainFields.map(([label, raw, kind]) => ui.kv(label, fieldValue(ui, label, raw, kind))).join("")}</dl></section>`
    : "";
  const datasetCatalogMarkup = detailKey === "dataset"
    ? ui.notice("Stored catalog metadata", detail.health === null ? "This bounded dataset detail comes from the stored catalog; aggregate health was not computed." : "This bounded dataset detail comes from the stored catalog and retains its source provenance.", "info")
    : "";
  const identifierMarkup = identifiers.length ? `<section><h4>Exact identifiers</h4><dl class="detail-grid">${identifiers.map(([label, raw]) => ui.kv(label, `<span>${ui.esc(text(raw))}</span> <button class="button button-quiet copy-button" type="button" data-copy="${ui.esc(text(raw))}" data-copy-label="Copy">Copy</button>`)).join("")}</dl></section>` : "";
  const eventMarkup = detailKey === "candidate"
    ? table(ui, [{ label: "Time", render: (row) => shown(ui, row, ["timestamp", "created_at", "at"], "time") }, { label: "Stage", render: (row) => badge(ui, value(row, "stage", "status", "state")) }, { label: "Event", render: (row) => ui.esc(first(value(row, "title", "message", "name", "kind"), "Unnamed event")) }, { label: "Reason", render: (row) => ui.esc(first(value(row, "reason", "explanation"), "Not available")) }], events, "Evidence and events") + detailPager(ui, route, map.events, "detail", "Evidence and event")
    : "";
  const gapReadIncomplete = gapResponseFailed || (map.gaps === undefined && Boolean(ctx?.loading));
  const rangeRowsMarkup = gapReadIncomplete && !ranges.length
    ? ""
    : table(ui, [
      { label: "Range #", render: (row) => ui.esc(text(value(row, "range_index", "index"), "Not available")) },
      { label: "Dataset version", render: (row) => shown(ui, row, ["dataset_version", "range.dataset_version", "missing_range.dataset_version"]) },
      { label: "Start", render: (row) => shown(ui, row, ["range.start", "missing_range.start", "start", "start_timestamp"], "time") },
      { label: "End", render: (row) => shown(ui, row, ["range.end", "missing_range.end", "end", "end_timestamp"], "time") },
      { label: "Kind", render: (row) => ui.esc(text(value(row, "range.kind", "missing_range.kind", "kind"), "Not available")) },
      { label: "Reason", render: (row) => ui.esc(text(value(row, "range.reason", "missing_range.reason", "reason"), "Not available")) },
    ], ranges, "Missing ranges across saved dataset versions") + detailPager(ui, route, map.gaps, "detail", "Missing range");
  const rangeStatusMarkup = gapReadIncomplete
    ? ui.notice(ranges.length ? "Missing ranges refresh incomplete" : "Missing ranges unavailable", ranges.length ? "Showing ranges retained in the selected row; the native read did not complete." : "The native missing-ranges read did not complete; no empty result is inferred.", "info")
    : "";
  const rangeMarkup = detailKey === "dataset"
    ? `${datasetCatalogMarkup}${rangeStatusMarkup}${rangeRowsMarkup}`
    : "";
  const shadowMarkup = "";
  const recordMarkup = records.length ? table(ui, [{ label: "Record", render: (row) => ui.esc(first(value(row, "name", "title", "id"), "Unnamed record")) }, { label: "Status", render: (row) => badge(ui, value(row, "status", "state", "stage")) }, { label: "Time", render: (row) => shown(ui, row, ["timestamp", "created_at", "updated_at"], "time") }], records, "Related records") : "";
  const setupValues = [
    ["Operational setup", value(detail, "payload.operational_setup", "operational_setup")],
    ["Strategy document", value(detail, "payload.strategy_document", "strategy_document")],
    ["Entry setup", value(detail, "entry_setup")],
    ["Exit setup", value(detail, "payload.exit_policy", "exit_policy", "exit_setup")],
  ].filter(([, item]) => item !== undefined && item !== null && item !== "");
  const setupMarkup = setupValues.length ? `<section><h4>Canonical setup and strategy document</h4><dl class="detail-grid">${setupValues.map(([label, item]) => ui.kv(label, ui.esc(text(item)))).join("")}</dl></section>` : "";
  const detailFailure = isObject(ctx?.error) ? text(first(value(ctx.error, "message", "error", "detail")), "request failed") : text(ctx?.error || (isObject(ctx?.errors) ? ctx.errors.detail : ""), "");
  const detailStatusMarkup = detailFailure
    ? ui.notice("Detail request failed", `The bounded ${detailKey} detail resource could not be read (${detailFailure}); the loaded row is shown without treating the failed read as fresh.`, "warn")
    : state === "partial"
      ? ui.notice("Some detail is incomplete", "Ancillary evidence is incomplete; no missing value is treated as zero.", "info")
      : "";
  return `<article class="detail-panel" data-detail-id="${ui.esc(selected)}"><header class="section-heading"><div><h3>${ui.esc(detailTitle(route, detail))}</h3><p class="muted">Exact detail for the selected bounded record.</p></div>${badge(ui, value(detail, "status", "state", "stage", ...(detailKey === "market" ? ["settlement"] : ["outcome"])))}</header><dl class="detail-grid">${fieldMarkup}</dl>${domainMarkup}${identifierMarkup}${setupMarkup}${recordMarkup}${eventMarkup}${rangeMarkup}${shadowMarkup}${ui.technical(detail)}${detailStatusMarkup}</article>`;
}

function renderPage(ctx = {}) {
  const route = normalizeRoute(ctx.route);
  const ui = uiFor(ctx);
  const data = envelope(ctx.data);
  const state = stateFor(data, ctx);
  if (blockingState(state) && !primaryRows(data, route, ctx).length && !hasStructuredContext(data, route)) {
    const title = ROUTES[route.view]?.title || ROUTES[route.view]?.[route.section]?.title || humanize(route.view);
    return pageFrame(ui, title, "Read-only bounded server projection.", stateBody(ui, data, { ...ctx, route }), state);
  }
  if (route.view === "portfolio") return renderPortfolio(ctx, route, ui, data);
  if (route.view === "markets") return renderMarkets(ctx, route, ui, data);
  if (route.view === "research" && route.section === "strategies") return renderStrategyPage(ctx, route, ui, data);
  if (route.view === "research" && route.section === "crypto") return renderCryptoPage(ctx, route, ui, data);
  if (route.view === "research" && route.section === "automation") return renderAutomationPage(ctx, route, ui, data);
  if (route.view === "research" && route.section === "data") return renderDataPage(ctx, route, ui, data);
  if (route.view === "activity") return renderActivity(ctx, route, ui, data);
  if (route.view === "live" && route.section === "binance") return renderBinance(ctx, route, ui, data);
  if (route.view === "live" && route.section === "polymarket") return renderPortfolio({ ...ctx, data }, { ...route, section: "real" }, ui, data);
  return pageFrame(ui, "This destination is not available", "Choose a destination from the navigation.", ui.empty("No page renderer", "The requested destination is not part of the current route inventory."), "error");
}

function detailRequests(routeInput = {}) {
  const route = normalizeRoute(routeInput);
  const selected = selectedOf(route);
  const detailPage = Math.max(1, Number(route.detail_page) || 1);
  const shadowPage = Math.max(1, Number(route.shadow_page) || 1);
  const pageSize = encodeURIComponent(route.detail_page_size || 10);
  if (route.view === "research" && route.section === "automation") {
    const requests = { shadow_list: `/api/v2/shadow?page=${shadowPage}&page_size=${pageSize}` };
    if (!selected) return requests;
    const encoded = encodeURIComponent(selected);
    const marker = String(route.record_kind || route.source || "").toLowerCase();
    if (marker === "shadow") requests.shadow = `/api/v2/shadow/${encoded}`;
    else requests.hermes = `/api/v2/hermes/${encoded}`;
    return requests;
  }
  if (!selected) return {};
  const encoded = encodeURIComponent(selected);
  if (route.view === "markets") return { market: `/api/ui-record?kind=market&id=${encoded}` };
  if (route.view === "research" && route.section === "strategies") return { candidate: `/api/v2/candidates/${encoded}`, events: `/api/v2/candidates/${encoded}/events?page=${detailPage}&page_size=${pageSize}` };
  if (route.view === "research" && route.section === "crypto") return { crypto: `/api/v2/crypto-research/${encoded}` };
  if (route.view === "research" && route.section === "data") return { dataset: `/api/v2/datasets/${encoded}`, gaps: `/api/v2/datasets/${encoded}/missing-ranges?page=${detailPage}&page_size=${pageSize}` };
  if (route.view === "activity") return { activity: `/api/v2/activity?filter=${encoded}&page=1&page_size=${encodeURIComponent(route.detail_page_size || 10)}` };
  if ((route.view === "portfolio" && route.section === "real") || (route.view === "live" && route.section === "polymarket")) {
    const kind = encodeURIComponent(route.record_kind || "order");
    return { record: `/api/ui-record?kind=${kind}&id=${encoded}` };
  }
  return {};
}

function pageSpec(routeInput = {}) {
  const route = normalizeRoute(routeInput);
  const title = ROUTES[route.view]?.title || ROUTES[route.view]?.[route.section]?.title || humanize(route.view);
  if (route.view === "home" || route.view === "settings") return { title, subtitle: "Read-only operator overview", endpoint: endpointFor(route.view, route.section), facets: [], sortOptions: [] };
  if (route.view === "live" && route.section === "polymarket") return spec("Polymarket", "Current decisions, readiness, affordability, and canary records.", "/api/v2/canary", [], [], detailRequests, 15000);
  if (route.view === "live" && route.section === "binance") return spec("Binance", "Parked or strict Testnet evidence, kept separate from Polymarket.", "/api/v2/binance-canary", [], [], detailRequests, 20000);
  if (route.view === "portfolio" && route.section === "real") return spec("Real canary", "Execution and accounting evidence from the real canary only.", "/api/v2/canary", [], [], detailRequests, 15000);
  if (route.view === "markets") return spec("Markets", "Observed questions and order-book values.", "/api/v2/polymarket", [{ key: "category", label: "Category", options: optionsFor("category") }, { key: "market", label: "Market", options: optionsFor("market_type") }, { key: "quality", label: "Quality", options: ["All quality states", "PRICE_PROXY", "ORDER_BOOK_SIMULATED"] }, { key: "timeframe", label: "Timeframe", options: optionsFor("timeframe") }, { key: "settlement", label: "Settlement", options: optionsFor("settlement") }], [{ key: "observed_at", label: "Observed" }, { key: "source_timestamp", label: "Source time" }, { key: "market_id", label: "Market ID" }, { key: "quality", label: "Quality" }, { key: "category", label: "Category" }, { key: "timeframe", label: "Timeframe" }, { key: "settlement", label: "Settlement" }], detailRequests);
  if (route.view === "research" && route.section === "strategies") return spec("Strategies", "Research lifecycle and evidence.", "/api/v2/candidates", [{ key: "stage", label: "Stage", options: optionsFor("stage") }], [{ key: "updated_at", label: "Updated time" }, { key: "stage", label: "Stage" }, { key: "candidate_id", label: "Candidate ID" }], detailRequests);
  if (route.view === "research" && route.section === "crypto") return spec("Crypto research", "Catalogs and reports, parked research only.", "/api/v2/crypto-research", [{ key: "symbol", label: "Symbol", options: optionsFor("symbol") }, { key: "source_type", label: "Source", options: optionsFor("source") }], [{ key: "updated_at", label: "Updated time" }, { key: "dataset_id", label: "Dataset" }], detailRequests);
  if (route.view === "research" && route.section === "automation") return spec("Automation", "Hermes queue and shadow jobs.", "/api/v2/hermes", [{ key: "status", label: "Status", options: optionsFor("status") }], [{ key: "available_at", label: "Next schedule" }, { key: "status", label: "Status" }], detailRequests);
  if (route.view === "research" && route.section === "data") return spec("Data", "Catalogs, manifests, and bounded missing ranges.", "/api/v2/datasets", [{ key: "source_type", label: "Source", options: optionsFor("source") }, { key: "market", label: "Market", options: optionsFor("market_type") }, { key: "timeframe", label: "Timeframe", options: optionsFor("timeframe") }, { key: "quality", label: "Quality", options: optionsFor("quality") }], [{ key: "updated_at", label: "Updated time" }, { key: "dataset_id", label: "Dataset" }, { key: "dataset_version", label: "Version" }, { key: "provider", label: "Provider" }, { key: "instrument", label: "Instrument" }, { key: "market_type", label: "Market type" }, { key: "timeframe", label: "Timeframe" }, { key: "row_count", label: "Rows" }, { key: "completeness", label: "Completeness" }, { key: "quality", label: "Quality" }, { key: "source_type", label: "Source" }], detailRequests);
  if (route.view === "activity") return spec("Activity", "Chronological bounded audit evidence.", "/api/v2/activity", [{ key: "kind", label: "Kind", options: optionsFor("kind") }, { key: "status", label: "Status", options: optionsFor("status") }], [{ key: "timestamp", label: "Event time" }, { key: "status", label: "Status" }], detailRequests);
  return spec(title, "Read-only operator destination", endpointFor(route.view, route.section), [], [], detailRequests);

}
export { pageSpec, renderPage, renderDetail, stateFor, primaryRows };
export { normalizeRoute as normalizePageRoute };
