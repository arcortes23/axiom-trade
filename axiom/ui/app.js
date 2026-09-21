import { pageSpec, renderPage, renderDetail, stateFor, primaryRows } from "./pages.js";

const ROUTE_ALIASES = {
  overview: "home", home: "home", live: "live", canary: "live", "polymarket-canary": "live",
  portfolio: "portfolio", "paper-portfolio": "portfolio", "rolling-portfolio": "portfolio",
  markets: "markets", polymarket: "markets", "polymarket-research": "markets",
  research: "research", candidates: "research", crypto: "research", "crypto-research": "research", hermes: "research", automation: "research", shadow: "research", datasets: "research",
  activity: "activity", settings: "settings", system: "settings", risk: "settings", "risk-settings": "settings", "binance-canary": "live"
};
const SECTIONS = {
  live: ["polymarket", "binance"], portfolio: ["real", "practice", "allocations"], research: ["strategies", "crypto", "automation", "data"]
};
const VIEW_LABELS = { home: "Home", live: "Live trading", portfolio: "Portfolio", markets: "Markets", research: "Research", activity: "Activity", settings: "Settings & system" };
const PHT = "Asia/Manila";
const SECRET_KEY = /(?:token|secret|password|credential|authorization|cookie|private[_-]?key|api[_-]?key|csrf)/i;
const PUBLIC_IDENTIFIER_KEY = /^(?:token_id|authorization_id|execution_authorization_id|market_id|condition_id|candidate_id|strategy_id|strategy_version_id|setup_id|selection_id|policy_id|dataset_id|event_id|fill_id|request_id|reservation_id|position_id|attempt_id|order_id|job_id|scope_id|(?:[a-z0-9]+_)?(?:hash|version))$/i;
const PUBLIC_CONTAINER_KEY = /^(?:execution_authorization|authorization_bindings|draft_member_bindings|setup_bindings|provenance|lineage)$/i;

const CANDIDATE_TARGET_ACTIONS = new Set(["canary.eligibility.verify", "canary.eligibility.mark", "canary.generate_signal", "canary.arm"]);
export function money(value) {
  if (value === null || value === undefined || value === "" || (typeof value === "string" && !value.trim())) return "Not available";
  const numeric = typeof value === "number" ? value : Number(String(value).replace(/[$,]/g, ""));
  if (!Number.isFinite(numeric)) return "Not available";
  if (numeric === 0) return "$0.00";
  const raw = String(value).trim().replace(/^\$/, "");
  if (Math.abs(numeric) < 0.01 && /^-?\d+(?:\.\d+)?$/.test(raw)) return `$${raw}`;
  return new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", minimumFractionDigits: 2, maximumFractionDigits: 8 }).format(numeric);
}

export function formatTime(value) {
  if (!value || typeof value !== "string") return "Not available";
  const stamp = new Date(value);
  if (Number.isNaN(stamp.getTime())) return "Not available";
  const pht = new Intl.DateTimeFormat("en-PH-u-hc-h23", { timeZone: PHT, year: "numeric", month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit", second: "2-digit", hourCycle: "h23" }).format(stamp);
  return `${pht} PHT (UTC ${stamp.toISOString()})`;
}

export function parseRoute(search = "") {
  const params = new URLSearchParams(String(search || "").replace(/^\?/, ""));
  const requested = params.get("view") || params.get("tab") || "home";
  const view = ROUTE_ALIASES[requested.toLowerCase()] || "home";
  let section = params.get("section") || "";
  if (requested === "binance-canary") section = "binance";
  if (requested === "rolling-portfolio") section = "allocations";
  if (requested === "paper-portfolio" || (!params.has("view") && params.get("tab")?.toLowerCase() === "portfolio")) section = "practice";
  if (requested === "datasets") section = "data";
  if (requested === "candidates") section = "strategies";
  if (requested === "crypto" || requested === "crypto-research") section = "crypto";
  if (requested.toLowerCase() === "shadow" || requested.toLowerCase() === "hermes") section = "automation";
  if (view === "live" && !SECTIONS.live.includes(section)) section = "polymarket";
  if (view === "portfolio" && !SECTIONS.portfolio.includes(section)) section = "real";
  if (view === "research" && !SECTIONS.research.includes(section)) section = "strategies";
  const integer = (key, fallback) => { const n = Number(params.get(key)); return Number.isInteger(n) && n > 0 ? n : fallback; };
  const requestedSize = integer("page_size", 25);
  const pageSize = [10, 25, 50, 100].includes(requestedSize) ? requestedSize : 25;
  const detailPageSize = integer("detail_page_size", 10);
  const boundedDetailSize = [10, 25, 50, 100].includes(detailPageSize) ? detailPageSize : 10;
  const route = { view, section, page: integer("page", 1), page_size: pageSize, detail_page: integer("detail_page", 1), detail_page_size: boundedDetailSize, shadow_page: integer("shadow_page", 1), filter: params.get("filter") || "", sort: params.get("sort") || "", direction: params.get("direction") === "asc" ? "asc" : "desc", selected: params.get("selected") || params.get("detail") || "", record_kind: params.get("record_kind") || "", expanded: params.get("expanded") === "1" || params.get("expanded") === "true" };
  for (const key of ["category", "quality", "market", "symbol", "source_type", "timeframe", "settlement", "stage", "status", "severity", "kind", "environment"]) {
    const value = params.get(key);
    if (value) route[key] = value;
  }
  if (!route.source_type && params.get("source")) route.source_type = params.get("source");
  return route;
}

const pick = (sources, keys) => {
  for (const source of sources) {
    if (!source || typeof source !== "object") continue;
    for (const key of keys) if (source[key] !== undefined && source[key] !== null && source[key] !== "") return source[key];
  }
  return null;
};
const upper = value => String(value ?? "").trim().toUpperCase();
const present = value => value !== null && value !== undefined && value !== "";

export function reasonPresentation(code) {
  const key = upper(code);
  const known = {
    NO_SIGNAL: ["No signal", "The current strategy scan did not produce an actionable signal.", "Wait for the next supported scan."],
    INPUT_DEFICIT: ["Waiting for input", "Required market, account, or evidence input is not available.", "Refresh the supported checks."],
    UNAFFORDABLE: ["Budget not available", "The current authoritative allowance cannot support this action.", "Review the limits and current usage."],
    CONFIGURATION: ["Configuration needs attention", "A required setting is missing or invalid.", "Open Settings & system."],
    STOPPED_WORKER: ["Worker stopped", "The worker is not running, so no new decision can be submitted.", "Review worker status in Advanced."],
    UNKNOWN_ORDER: ["Outcome unknown", "An order outcome has not been reconciled by the authoritative status.", "Check Activity; do not submit again."],
    EXPIRED: ["Session expired", "The previous authorization is no longer active.", "Review the current authorization."],
    REVOKED: ["Authorization revoked", "There is no active exploratory authorization.", "Review the current terms before starting."],
    STALE: ["Checks need refreshing", "The last supported check is older than the freshness window.", "Refresh the existing preflight check."],
    ACCOUNT_DISCONNECTED: ["Account status unavailable", "Account-dependent readiness cannot be verified right now.", "Reconnect through the existing supported path."],
  };
  const value = known[key];
  return value ? { code: key, label: value[0], explanation: value[1], next: value[2] } : { code: key || "UNKNOWN", label: "Status needs review", explanation: present(code) ? `The system reported ${String(code)}.` : "The system did not provide a reason code.", next: "Open the relevant details and refresh supported checks." };
}

export function sessionPresentation({ operator = {}, canary = {}, connected = true, receivedAt = null, now = Date.now() } = {}) {
  const op = operator && typeof operator === "object" ? operator : {};
  const c = canary && typeof canary === "object" ? canary : {};
  const executionSource = [op.execution_authorization, c.execution_authorization].find(value => value && typeof value === "object") || null;
  const execution = executionSource || {};
  const authProjectionAvailable = Boolean(executionSource && Object.keys(executionSource).length);
  const activeRecord = execution.active && typeof execution.active === "object" ? execution.active : null;
  const authState = upper(pick([execution, activeRecord], ["status", "state", "authorization_state"]));
  const latestState = upper(pick([execution, op.authorization, c.authorization], ["latest_status", "latest_state", "status", "state", "control_state"]));
  const activeAuth = execution.active === true || authState === "ACTIVE" || upper(activeRecord?.status) === "ACTIVE";
  const worker = c.worker && typeof c.worker === "object" ? c.worker : {};
  const autonomous = [c.autonomous, c.status_report?.autonomous].find(value => value && typeof value === "object") || {};
  const control = [c.control, c.status_report?.authoritative_control, c.status_report?.control].find(value => value && typeof value === "object") || {};
  const controlRaw = pick([control, c], ["armed_state", "armed", "enabled", "control_state", "control_status"]) ?? pick([control, c.autonomous, c.status_report?.autonomous], ["state", "status"]);
  const controlState = controlRaw === true ? "ARMED" : controlRaw === false ? "DISARMED" : present(controlRaw) ? String(controlRaw) : "UNKNOWN";
  const controlStopped = ["DISARMED", "DISABLED", "STOPPED", "OFF"].includes(upper(controlState));
  const workerNext = pick([worker, autonomous], ["next_decision", "next_work"]);
  const workerBlocker = pick([worker, autonomous], ["blocker", "last_error_code", "reason_code", "last_cycle_blocker"]);
  const workerHeartbeat = pick([worker, autonomous], ["last_tick_at", "last_tick_completed_at", "last_successful_tick"]);
  const readinessSource = [c.readiness, c.status_report?.readiness].find(value => value && typeof value === "object") || {};
  const readinessState = upper(pick([readinessSource], ["status"])) || "UNKNOWN";
  const directSignal = [c.decision, c.signal, c.execution].find(value => typeof value === "string");
  const signalState = upper(directSignal || pick([c.decision, c.signal, autonomous, worker], ["status", "decision", "signal_status", "last_cycle_blocker", "reason_code", "next_decision"]));
  const researchState = upper(pick([op.research, op.research_feed], ["status", "state", "execution_state"])) || "RESEARCH ONLY";
  const observedCandidates = [
    pick([op.canary?.connectivity, op.connectivity, c.connectivity], ["checked_at", "checked_at_utc", "observed_at", "last_checked_at"]),
    pick([readinessSource], ["checked_at", "checked_at_utc", "observed_at", "last_checked_at"]),
    pick([c, c.status_report], ["readiness_snapshot_updated_at", "readiness_checked_at"]),
  ].filter(Boolean).map(value => ({ value, time: new Date(value).getTime() })).filter(item => Number.isFinite(item.time)).sort((a, b) => b.time - a.time);
  const observedAt = observedCandidates[0]?.value || null;
  const observed = observedAt ? new Date(observedAt).getTime() : NaN;
  const age = Number.isFinite(observed) ? Math.max(0, now - observed) : null;
  const freshness = age === null ? "UNKNOWN" : age <= 60000 ? "CURRENT" : "STALE";
  const connection = connected === false ? "DISCONNECTED" : connected === true ? "CONNECTED" : "UNKNOWN";
  const directReason = [c.decision, c.signal, c.execution].find(value => typeof value === "string");
  const noTradeCode = directReason || pick([c.decision, c.signal, c.execution, autonomous, worker], ["no_trade_reason", "blocker", "reason_code", "last_cycle_blocker", "next_decision", "decision", "signal_status"]);
  const reason = reasonPresentation(noTradeCode);
  let primaryAction;
  if (!activeAuth) primaryAction = { label: "Review and start", action: "review-live", reason: latestState === "REVOKED" ? "The latest authorization is revoked." : "No active authorization is available." };
  else if (controlStopped) primaryAction = { label: "Open Live trading", action: "live", reason: "Execution is stopped; no new entry or exit submissions will be sent." };
  else if (freshness !== "CURRENT") primaryAction = { label: "Refresh checks", action: "refresh", reason: freshness === "STALE" ? "The recorded check is older than 60 seconds." : "The current check timestamp is unavailable." };
  else if (["READY", "CURRENT", "ELIGIBLE"].includes(readinessState)) primaryAction = { label: "Open Live trading", action: "live", reason: "Current access/readiness checks report a supported ready state; they do not select a trade." };
  else primaryAction = { label: reason.next, action: "live", reason: reason.explanation };
  const workerValue = workerHeartbeat
    ? `Last heartbeat ${formatTime(workerHeartbeat)}${workerNext ? `; next ${human(workerNext)}` : ""}`
    : workerNext
      ? `Next ${human(workerNext)}`
      : workerBlocker
        ? human(workerBlocker)
        : "Not available";
  const authLabel = !authProjectionAvailable ? "Unavailable" : latestState === "DRAFT" ? "Review needed" : latestState === "REVOKED" ? "No active authorization" : latestState === "EXPIRED" ? "Authorization expired" : latestState ? human(latestState) : "Not active";
  const readinessLabel = readinessState !== "UNKNOWN" ? human(readinessState) : "Not available";
  const decisionLabel = signalState && signalState !== "UNKNOWN" ? (["NO_SIGNAL", "INPUT_DEFICIT", "UNAFFORDABLE", "CONFIGURATION", "STOPPED_WORKER", "UNKNOWN_ORDER", "EXPIRED", "REVOKED", "STALE", "ACCOUNT_DISCONNECTED"].includes(signalState) ? reasonPresentation(signalState).label : human(signalState)) : "Not available";
  const detail = `Authorization ${activeAuth ? "active" : authLabel.toLowerCase()}; control ${human(controlState).toLowerCase()}; worker ${workerValue.toLowerCase()}; access/readiness checks ${readinessLabel.toLowerCase()}; connection ${human(connection).toLowerCase()}.`;
  const tone = controlStopped || (activeAuth && freshness !== "CURRENT") || latestState === "REVOKED" || latestState === "EXPIRED" ? "warn" : activeAuth && freshness === "CURRENT" ? "info" : "neutral";
  return {
    label: activeAuth ? "Exploratory authorization active" : authLabel, detail, tone,
    permission: { label: activeAuth ? "Current authorization" : "Authorization status", value: activeAuth ? "Active" : authLabel },
    control: { label: "Control state", value: controlState === "UNKNOWN" ? "Unknown" : human(controlState) },
    worker: { label: "Worker observation", value: workerValue, nextDecision: workerNext, blocker: workerBlocker, heartbeatAt: workerHeartbeat },
    readiness: { label: "Access/readiness checks", value: readinessLabel },
    decision: { label: "Current decision", value: decisionLabel, reason },
    research: { label: "Research", value: researchState !== "RESEARCH ONLY" ? human(researchState) : "Research only" },
    freshness: { label: "Data freshness", value: freshness === "CURRENT" ? "Current" : freshness === "STALE" ? "Checks need refreshing" : "Unknown (check timestamp unavailable)", ageMs: age, observedAt, receivedAt },
    controlSource: control,
    primaryAction
  };
}
function esc(value) { return String(value ?? "").replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c])); }
function number(value) { if (value === null || value === undefined || value === "") return "Not available"; const n = Number(value); return Number.isFinite(n) ? new Intl.NumberFormat("en-US", { maximumFractionDigits: 8 }).format(n) : "Not available"; }
function time(value) { return formatTime(value); }
function human(value) { if (!present(value)) return "Not available"; return String(value).replace(/[_-]+/g, " ").toLowerCase().replace(/\b\w/g, c => c.toUpperCase()); }
function lifetimeBudgetValue(source) {
  const value = pick([source], ["lifetime_budget"]);
  return value && typeof value === "object" ? pick([value], ["max_notional_usd"]) : value;
}
function memberCount(sources) {
  for (const source of sources) {
    if (Array.isArray(source)) return source.length;
  }
  return null;
}
function badge(label, tone = "neutral") { return `<span class="badge badge-${esc(tone)}">${esc(label || "Unknown")}</span>`; }
function notice(title, body, tone = "info") { return `<div class="notice notice-${esc(tone)}" role="status"><strong>${esc(title)}</strong><span>${esc(body)}</span></div>`; }
function empty(title, body) { return `<section class="empty-state"><h3>${esc(title)}</h3><p>${esc(body)}</p></section>`; }
function kv(label, valueHtml) { return `<div class="kv"><dt>${esc(label)}</dt><dd>${valueHtml || "Not available"}</dd></div>`; }
function sanitize(value, depth = 0, key = "") {
  if (SECRET_KEY.test(key) && !PUBLIC_IDENTIFIER_KEY.test(key) && !PUBLIC_CONTAINER_KEY.test(key)) return "[redacted]";
  if (depth > 5) return "[details omitted]";
  if (value === null || value === undefined) return null;
  if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") return value;
  if (Array.isArray(value)) return value.slice(0, 64).map(item => sanitize(item, depth + 1, key));
  if (typeof value === "object") return Object.fromEntries(Object.entries(value).slice(0, 128).map(([k, v]) => [k, sanitize(v, depth + 1, k)]));
  return String(value);
}
function publicIdentifiers(value, output = [], depth = 0) {
  if (depth > 5 || value === null || value === undefined) return output;
  if (Array.isArray(value)) { value.slice(0, 64).forEach(item => publicIdentifiers(item, output, depth + 1)); return output; }
  if (typeof value !== "object") return output;
  Object.entries(value).slice(0, 128).forEach(([key, child]) => {
    if (PUBLIC_IDENTIFIER_KEY.test(key) && child !== null && child !== undefined && child !== "") output.push([key, String(child)]);
    else if (typeof child === "object") publicIdentifiers(child, output, depth + 1);
  });
  return output.slice(0, 32);
}
function technical(value, title = "Technical details") { const safe = sanitize(value); const encoded = typeof safe === "string" ? safe : JSON.stringify(safe, null, 2); const identifiers = publicIdentifiers(safe).map(([key, id]) => `<button class="button button-quiet copy-button" type="button" data-copy="${esc(id)}" data-copy-label="Copy ${esc(human(key))}">${esc(human(key))}</button>`).join(""); return `<details class="technical"><summary>${esc(title)}</summary><div class="technical-actions"><button class="button button-quiet copy-button" type="button" data-copy="${esc(encoded)}" data-copy-label="Copy">Copy bundle</button>${identifiers}</div><pre>${esc(encoded)}</pre></details>`; }
function link(label, patch = {}) { const next = { ...currentRoute, ...patch }; if (Object.prototype.hasOwnProperty.call(patch, "section") && patch.section !== currentRoute.section) { next.detail_page = 1; next.shadow_page = 1; } if (Object.prototype.hasOwnProperty.call(patch, "selected") && patch.selected !== currentRoute.selected) { next.detail_page = 1; next.shadow_page = 1; } if (Object.prototype.hasOwnProperty.call(patch, "record_kind") && patch.record_kind !== currentRoute.record_kind) { next.detail_page = 1; next.shadow_page = 1; } const q = new URLSearchParams(); for (const [key, val] of Object.entries(next)) if (val !== "" && val !== false && val !== null && val !== undefined) q.set(key === "view" ? "view" : key, String(val)); return `<a href="?${q.toString()}" data-route-link>${esc(label)}</a>`; }
function table(columns, rows, options = {}) { const list = Array.isArray(rows) ? rows : []; if (!list.length) return empty(options.emptyTitle || "No records", options.emptyBody || "No bounded records are available."); return `<div class="table-scroll" tabindex="0" role="region" aria-label="${esc(options.caption || "Data table")}"><table class="data-table"><caption>${esc(options.caption || "")}</caption><thead><tr>${columns.map(col => `<th class="${esc(col.className || "")}">${esc(col.label)}</th>`).join("")}</tr></thead><tbody>${list.map(row => `<tr>${columns.map(col => `<td class="${esc(col.className || "")}">${col.render ? col.render(row) : esc(row[col.key])}</td>`).join("")}</tr>`).join("")}</tbody></table></div>`; }
const ui = { esc, money, number, time, human, badge, empty, notice, kv, table, technical, link, reason: reasonPresentation };

function hrefForRoute(route) { const q = new URLSearchParams(); for (const [key, value] of Object.entries(route)) if (value !== "" && value !== false && value !== null && value !== undefined) q.set(key, String(value)); return `?${q}`; }
function routeTitle(route) { return route.view === "live" && route.section === "binance" ? "Live trading · Binance" : route.view === "live" ? "Live trading · Polymarket" : route.view === "portfolio" ? `Portfolio · ${human(route.section)}` : route.view === "research" ? `Research · ${human(route.section)}` : VIEW_LABELS[route.view] || "AXIOM"; }
function destinationLinks(route) {
  const groups = route.view === "live"
    ? [["Live trading", [["polymarket", "Polymarket"], ["binance", "Binance"]]]]
    : route.view === "portfolio"
    ? [["Portfolio", [["real", "Real canary"], ["practice", "Practice"], ["allocations", "Allocations"]]]]
    : route.view === "research"
    ? [["Research", [["strategies", "Strategies"], ["crypto", "Crypto research"], ["automation", "Automation"], ["data", "Data"]]]]
    : [];
  return groups.map(([title, entries]) => `<nav class="subnav" aria-label="${esc(title)} sections"><span>${esc(title)}</span>${entries.map(([section, label]) => `<a class="${route.section === section ? "active" : ""}" href="${hrefForRoute({ ...route, section, page: 1, detail_page: 1, shadow_page: 1, selected: "", expanded: false })}" data-route-link>${esc(label)}</a>`).join("")}</nav>`).join("");
}
function browseControls(route, spec, data = {}, state = {}) {
  const nativePages = Number(data.pages);
  const readState = stateFor(data, { loading: state.loading, error: state.errors?.page });
  const blockedWithoutRows = ["loading", "error", "unavailable", "disconnected"].includes(readState) && !primaryRows(data, route).length;
  const hasNativePager = !blockedWithoutRows && ((Number.isInteger(nativePages) && nativePages > 0) || typeof data.has_more === "boolean");
  if (!spec || (!spec.facets?.length && !spec.sortOptions?.length && !hasNativePager)) return "";
  const facetFields = (spec.facets || []).map(facet => {
    const options = facet.options || [];
    return `<label>${esc(facet.label)}<select name="${esc(facet.key)}" data-browse-control>${options.length ? options.map(option => { const value = /^All /.test(option) ? "" : option; return `<option value="${esc(value)}" ${String(route[facet.key] || "").toLowerCase() === String(value).toLowerCase() ? "selected" : ""}>${esc(human(option))}</option>`; }).join("") : `<option value="">All</option>`}</select></label>`;
  }).join("");
  const sort = (spec.sortOptions || []).map(option => `<option value="${esc(option.key)}" ${route.sort === option.key ? "selected" : ""}>${esc(option.label)}</option>`).join("");
  const direction = spec.sortOptions?.length ? `<label>Order<select name="direction" data-browse-control><option value="desc" ${route.direction === "desc" ? "selected" : ""}>Descending</option><option value="asc" ${route.direction === "asc" ? "selected" : ""}>Ascending</option></select></label>` : "";
  const page = Number(route.page) || 1;
  const pages = Number.isInteger(nativePages) && nativePages > 0 ? nativePages : 0;
  const hasMore = typeof data.has_more === "boolean" ? data.has_more : null;
  const hasPager = hasNativePager && (pages > 0 || hasMore !== null);
  const nextAvailable = hasMore !== null ? hasMore : pages > page;
  const pager = hasPager ? `<nav class="pager" aria-label="Pagination"><button class="button button-quiet" type="button" data-page="${Math.max(1, page - 1)}" ${page <= 1 ? "disabled" : ""}>Previous</button><span>Page ${page}${pages ? ` of ${pages}` : ""}</span><button class="button button-quiet" type="button" data-page="${pages ? Math.min(pages, page + 1) : page + 1}" ${nextAvailable ? "" : "disabled"}>Next</button></nav>` : "";
  return `<form class="browse-controls" id="browse-controls" role="search"><label class="search-field">Search<input name="filter" data-browse-control value="${esc(route.filter || "")}" placeholder="Search this view" autocomplete="off"></label>${facetFields}<label>Sort<select name="sort" data-browse-control><option value="">Default order</option>${sort}</select></label>${direction}<label>Rows<select name="page_size" data-browse-control>${[10, 25, 50, 100].map(size => `<option value="${size}" ${Number(route.page_size) === size ? "selected" : ""}>${size} per page</option>`).join("")}</select></label><button class="button button-secondary" type="submit">Apply filters</button>${pager}</form>`;
}
function metric(label, value, detail = "") { return `<div class="metric"><div class="metric-label">${esc(label)}</div><div class="metric-value">${value}</div>${detail ? `<small>${esc(detail)}</small>` : ""}</div>`; }
function valueFrom(data, keys) { return pick([data, data?.summary, data?.totals, data?.metrics, data?.operator_controls], keys); }
function sessionCard(operator, canary, connected, receivedAt, onReview = "", errors = {}) {
  const session = sessionPresentation({ operator, canary, connected, receivedAt });
  const p = session.primaryAction;
  const operatorUnavailable = Boolean(errors.operator);
  const execution = canary.execution_authorization || operator.execution_authorization || {};
  const auth = [execution.active, execution.draft, execution.authorization].find(value => value && typeof value === "object") || execution;
  const risk = operator.risk_settings || canary.risk_settings || {};
  const limits = risk.effective_limits || risk.active_limits || {};
  const review = operator.operator_controls?.exploratory_live_review || canary.operator_controls?.exploratory_live_review || {};
  const memberTotal = memberCount([review.proposal?.members, review.members, canary.rolling_portfolio?.members]);
  const action = operatorUnavailable
    ? `<span class="notice notice-warn">Current authority is unavailable; review actions are disabled until it is refreshed.</span>`
    : p.action === "review-live"
    ? `<button class="button button-primary" data-action="review-live">${esc(p.label)}</button>`
    : `<a class="button button-secondary" href="${hrefForRoute({ ...currentRoute, view: p.action === "live" ? "live" : "home" })}">${esc(p.label)}</a>`;
  const pendingRecovery = !operatorUnavailable && storedPendingAction() && p.action !== "review-live" ? `<button class="button button-primary" data-action="review-live">Check prior confirmation</button>` : "";
  const lines = [session.permission, session.control, session.worker, session.readiness, session.decision, session.research, session.freshness];
  const warning = Object.entries(errors || {}).map(([key, value]) => notice(`${human(key)} unavailable`, String(value), "warn")).join("");
  const stop = operatorUnavailable ? "" : `<button class="button button-danger" type="button" data-action="stop-canary">Stop canary</button>`;
  const authority = `<dl class="authority-grid">${kv("Session budget", money(lifetimeBudgetValue(auth)))}${kv("Max buy", money(pick([limits], ["max_all_in_buy_usd", "max_buy_usd"])))}${kv("Duration", esc(durationText(pick([auth], ["duration_seconds"]))))}${kv("Mode", esc(human(pick([auth], ["mode"]) ?? "Not available")))}${kv("Members", esc(memberTotal ?? "Not available"))}</dl>`;
  const badgeLabel = session.permission.value === "Active" ? "Authorization active" : session.tone === "warn" ? "Review needed" : "Not active";
  return `${warning}<section class="section session-section"><div class="section-heading"><div><p class="eyebrow">Current authority</p><h2>${esc(session.label)}</h2><p class="muted">${esc(session.detail)}</p></div>${badge(badgeLabel, session.tone)}</div>${authority}<dl class="session-grid">${lines.map(item => kv(item.label, `<strong>${esc(item.value)}</strong>`)).join("")}</dl><div class="session-next"><div><strong>Next supported action</strong><p>${esc(p.reason)}</p></div><div class="actions">${action}${pendingRecovery}${stop}<a class="button button-quiet" href="${hrefForRoute({ ...currentRoute, view: "settings" })}">Open settings</a></div></div></section>`;
}

function activityTimestamp(item) {
  const candidates = [item?.completed_at, item?.finished_at, item?.updated_at, item?.started_at, item?.timestamp, item?.created_at];
  return candidates.find(value => value && Number.isFinite(new Date(value).getTime())) || null;
}
function activityLabel(item) {
  const value = item?.title || item?.kind || item?.action || "Activity";
  return human(String(value).replace(/[._-]+/g, " "));
}
function renderHome(state) {
  const operator = state.data.operator || {};
  const canary = state.data.canary || {};
  const risk = operator.risk_settings || canary.risk_settings || {};
  const usage = risk.usage && typeof risk.usage === "object" ? risk.usage : {};
  const remaining = risk.remaining && typeof risk.remaining === "object" ? risk.remaining : {};
  const execution = canary.execution && typeof canary.execution === "object" ? canary.execution : {};
  const actions = Array.isArray(operator.actions) ? operator.actions : [];
  const orderedActions = actions.map(item => ({ item, time: activityTimestamp(item) })).filter(entry => entry.time).sort((a, b) => new Date(b.time).getTime() - new Date(a.time).getTime()).map(entry => entry.item);
  const latestAction = orderedActions[0] || {};
  const latestActionName = latestAction.action || latestAction.title || latestAction.status;
  const latestActionLabel = latestActionName ? `${human(String(latestActionName).replace(/[._-]+/g, " "))}${latestAction.status ? ` · ${human(latestAction.status)}` : ""}` : "Not available";
  const metrics = [
    metric("Remaining session budget", money(pick([remaining], ["exploratory_lifetime_usd"])), "Authoritative lifetime allowance only"),
    metric("Open-position cost", money(pick([usage], ["aggregate_open_cost_usd"])), "Persisted open-cost mark"),
    metric("Open-position value", money(pick([execution, canary], ["open_position_value_usd"])), "Server-reported marked value"),
    metric("Known net result", money(pick([execution, canary], ["known_net_pnl_usd", "realized_pnl_usd", "net_result_usd"])), "No totals reconstructed here"),
    metric("Submissions today", number(pick([usage], ["submitted_orders"]))),
    metric("Latest action", latestActionLabel)
  ].join("");
  const directReason = [canary.decision, canary.signal, canary.execution].find(value => typeof value === "string");
  const decisionSource = directReason || pick([canary.decision, canary.signal, canary.execution], ["no_trade_reason", "blocker", "reason_code", "last_cycle_blocker", "decision", "signal_status", "status"]) || pick([canary.autonomous, canary.worker, canary], ["no_trade_reason", "blocker", "reason_code", "last_cycle_blocker", "next_decision"]);
  const reason = reasonPresentation(decisionSource);
  return `<div class="stack">${sessionCard(operator, canary, state.connected, state.receivedAt, "", state.errors)}<section class="section"><div class="section-heading"><div><p class="eyebrow">At a glance</p><h2>What is known now</h2></div><span class="muted">Values are copied from bounded server projections.</span></div><div class="metrics">${metrics}</div></section><section class="split"><article class="section"><div class="section-heading"><div><h2>Why no trades</h2><p class="muted">A reason is shown only when the authoritative projection provides one.</p></div>${badge(reason.label, reason.code === "NO_SIGNAL" ? "neutral" : "warn")}</div><p>${esc(reason.explanation)}</p><p class="muted">Next: ${esc(reason.next)}</p></article><article class="section"><div class="section-heading"><div><h2>Recent activity</h2><p class="muted">Persisted actions only; no browser action is inferred from this list.</p></div><a class="button button-quiet" href="${hrefForRoute({ ...currentRoute, view: "activity" })}">Open activity</a></div>${renderActivityPreview(actions)}</article></section></div>`;
}
function renderActivityPreview(items) { const entries = (Array.isArray(items) ? items : []).map(item => ({ item, time: activityTimestamp(item) })).sort((a, b) => { if (!a.time && !b.time) return 0; if (!a.time) return 1; if (!b.time) return -1; return new Date(b.time).getTime() - new Date(a.time).getTime(); }).slice(0, 5); if (!entries.length) return empty("No recent activity", "No bounded activity entries are available."); return `<ul class="timeline">${entries.map(({ item }) => { const status = item.status ? ` · ${human(item.status)}` : ""; const detail = item.message || (item.reason ? reviewText(item.reason) : item.status ? `Status ${human(item.status)}` : "Not available"); return `<li><time>${esc(formatTime(activityTimestamp(item)))}</time><span>${esc(activityLabel(item))}${esc(status)}</span><p>${esc(detail)}</p></li>`; }).join("")}</ul>`; }

function renderLive(state) {
  const binance = state.route.section === "binance";
  const source = state.data.page || (binance ? state.data.binance : state.data.canary) || {};
  if (binance) {
    const pageContext = { ...state, data: source, detail: state.data.detail || {}, error: state.errors?.page, operator: state.data.operator || {}, canary: state.data.canary || {}, ui };
    return renderPage(pageContext);
  }
  const operator = state.data.operator || {};
  const statusReport = source.status_report && typeof source.status_report === "object" ? source.status_report : {};
  const decision = source.decision && typeof source.decision === "object" ? source.decision : source.autonomous && typeof source.autonomous === "object" ? source.autonomous : statusReport.autonomous && typeof statusReport.autonomous === "object" ? statusReport.autonomous : source.execution && typeof source.execution === "object" ? source.execution : {};
  const input = source.input && typeof source.input === "object" ? source.input : source.inputs && typeof source.inputs === "object" ? source.inputs : source.market_scope_funnel && typeof source.market_scope_funnel === "object" ? source.market_scope_funnel : {};
  const signal = source.signal && typeof source.signal === "object" ? source.signal : source.latest_signal && typeof source.latest_signal === "object" ? source.latest_signal : {};
  const readiness = source.readiness && typeof source.readiness === "object" ? source.readiness : statusReport.readiness && typeof statusReport.readiness === "object" ? statusReport.readiness : {};
  const affordability = source.per_outcome_affordability || source.affordability || source.venue_minimum_feasibility || {};
  const ledger = {
    ...(source.ledger && typeof source.ledger === "object" ? source.ledger : {}),
    ...(source.execution && typeof source.execution === "object" ? source.execution : {}),
    ...(operator.ledger && typeof operator.ledger === "object" ? operator.ledger : {}),
  };
  const orderRows = Array.isArray(ledger.orders) ? ledger.orders : [...(Array.isArray(ledger.canary_submission_attempts) ? ledger.canary_submission_attempts : []), ...(Array.isArray(ledger.canary_position_requests) ? ledger.canary_position_requests : [])];
  const fillRows = Array.isArray(ledger.fills) ? ledger.fills : [...(Array.isArray(ledger.canary_position_fills) ? ledger.canary_position_fills : []), ...(Array.isArray(ledger.canary_risk_fills) ? ledger.canary_risk_fills : [])];
  const rows = [...orderRows, ...fillRows];
  const countStatus = statuses => rows.filter(row => statuses.includes(upper(row?.status || row?.state))).length;
  const resting = rows.length ? String(countStatus(["RESTING", "OPEN", "PENDING"])) : "Not available";
  const partial = rows.length ? String(countStatus(["PARTIAL", "PARTIALLY_FILLED"])) : "Not available";
  const statusValue = (value, keys = ["status", "state", "result", "stage"]) => {
    if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") return value;
    return pick([value], keys) ?? "Not available";
  };
  const detailValue = (value, keys = ["reason", "blocker", "detail", "message", "explanation"]) => reviewText(pick([value], keys));
  const card = (title, value, detail, tone = "neutral") => `<article class="panel"><div class="section-heading"><h3>${esc(title)}</h3>${badge(reviewText(value), tone)}</div><p class="muted">${esc(detail)}</p></article>`;
  const decisionValue = statusValue(decision, ["status", "decision", "state", "reason_code"]);
  const inputValue = statusValue(input, ["status", "state", "result", "stage", "readiness"]);
  const signalValue = statusValue(signal, ["status", "signal", "decision", "state"]);
  const readinessValue = statusValue(readiness, ["status", "state", "result", "readiness"]);
  const feasibility = affordability && typeof affordability === "object" && Object.keys(affordability).length ? reviewObjectMarkup(affordability, "Per-outcome feasibility") : `<span class="muted">Not available</span>`;
  const errors = Object.entries(state.errors || {}).map(([key, value]) => notice(`${human(key)} unavailable`, String(value), "warn")).join("");
  const pageContext = { ...state, data: source, detail: state.data.detail || {}, error: state.errors?.page, operator, canary: state.data.canary || source, ledger: operator.ledger || state.data.ledger || {}, ui };
  const execution = renderPage(pageContext);
  const session = sessionCard(operator, source, state.connected, state.receivedAt, "", state.errors);
  return `${errors}<div class="stack">${session}<section class="section"><div class="section-heading"><div><p class="eyebrow">Live decision boundary</p><h2>Polymarket canary</h2><p class="muted">Decision, market input, signal, readiness, and persisted execution evidence remain separate.</p></div>${badge(reviewText(statusValue(source, ["status", "state", "control_state"])), upper(statusValue(source, ["status", "state", "control_state"])) === "READY" ? "good" : "warn")}</div><div class="three-col">${card("Decision", decisionValue, detailValue(decision), upper(String(decisionValue)) === "TRADE" ? "good" : "warn")}${card("Market input", inputValue, detailValue(input), "neutral")}${card("Signal", signalValue, detailValue(signal), "neutral")}${card("Readiness", readinessValue, detailValue(readiness), upper(String(readinessValue)) === "READY" ? "good" : "warn")}${card("Resting records", resting, "Persisted orders or fills currently marked resting/open/pending.", "neutral")}${card("Partial records", partial, "Persisted orders or fills currently marked partial.", "neutral")}</div></section><section class="section"><div class="section-heading"><div><p class="eyebrow">Affordability</p><h2>Per-outcome feasibility</h2></div><span class="muted">Server-provided values only</span></div>${feasibility}</section>${execution}</div>`;
}
function controlField(name, label, value = "", options = {}) {
  const type = options.type || "text";
  if (type === "select") return `<label class="field"><span>${esc(label)}</span><select data-control-field="${esc(name)}">${(options.options || []).map(option => `<option value="${esc(option)}" ${String(value) === String(option) ? "selected" : ""}>${esc(human(option))}</option>`).join("")}</select></label>`;
  if (type === "textarea") return `<label class="field"><span>${esc(label)}</span><textarea data-control-field="${esc(name)}" rows="4" placeholder="${esc(options.placeholder || "")}">${esc(value)}</textarea></label>`;
  return `<label class="field"><span>${esc(label)}${options.unit ? ` <small>(${esc(options.unit)})</small>` : ""}</span><input type="${type}" data-control-field="${esc(name)}" value="${esc(value)}" ${options.step ? `step="${esc(options.step)}"` : ""} ${options.inputmode ? `inputmode="${esc(options.inputmode)}"` : ""}></label>`;
}
function controlCard(action, title, description, fields = [], confirmation = "", status = "") {
  const fieldMarkup = fields.map(field => controlField(field.name, field.label, field.value, field)).join("").replace('data-control-field="values"', 'data-control-field="values" data-control-json="true"');
  const phrase = confirmation ? `<label class="field"><span>Type ${esc(confirmation)}</span><input data-control-confirmation autocomplete="off" aria-label="Type ${esc(confirmation)}"></label>` : "";
  return `<article class="panel control-card"><div class="section-heading"><div><h3>${esc(title)}</h3><p class="muted">${esc(description)}</p></div>${status ? badge(human(status)) : ""}</div><form class="control-form form-grid" data-control-action="${esc(action)}" data-control-confirm="${esc(confirmation)}">${fieldMarkup}${phrase}<div class="actions"><button class="button button-secondary" type="submit">${esc(title)}</button></div><p class="control-result" role="status"></p></form></article>`;
}
function runtimeStatusCard(title, record, description, detailKeys = []) {
  const value = record && typeof record === "object" ? record : {};
  const status = pick([value], ["status", "state"]) || "Not available";
  const details = detailKeys.map(([label, keys]) => {
    const item = pick([value], keys);
    return item === null || item === undefined || item === "" ? "" : kv(label, `<strong>${esc(reviewText(item))}</strong>`);
  }).join("");
  return `<article class="panel control-card"><div class="section-heading"><div><h3>${esc(title)}</h3><p class="muted">${esc(description)}</p></div>${badge(reviewText(status), "neutral")}</div>${details ? `<dl class="detail-grid">${details}</dl>` : ""}</article>`;
}
function renderAdvanced(operator) {
  const controls = operator.operator_controls || {};
  const actionCards = [
    controlCard("node.restart", "Restart node", "Restarts the existing local node boundary after its native identity and lock checks.", [], "", pick([operator.node, controls.node], ["status", "state"])),
    controlCard("bootstrap.start", "Start historical bootstrap", "Starts the existing resumable bootstrap job; it never publishes partial data as complete.", [], "", pick([operator.bootstrap, controls.bootstrap], ["status", "state"])),
    controlCard("bootstrap.resume", "Resume historical bootstrap", "Resumes the existing bounded bootstrap cursor without changing venue or execution authority.", [], "", pick([operator.bootstrap, controls.bootstrap], ["status", "state"])),
    controlCard("hermes.pause", "Pause Hermes queue", "Pauses the existing research queue only; external scheduler state is not inferred.", [], "", pick([operator.hermes, controls.hermes], ["status", "state"])),
    controlCard("hermes.resume", "Resume Hermes queue", "Resumes the existing research queue after its native scheduler checks.", [], "", pick([operator.hermes, controls.hermes], ["status", "state"])),
    controlCard("hermes.run_now", "Run Hermes now", "Requests one existing bounded queue run; it does not activate trading.", [], "", pick([operator.hermes, controls.hermes], ["status", "state"])),
    controlCard("canary.connectivity_check", "Refresh connectivity checks", "Read-only preflight for the configured canary boundary.", [], "REFRESH CHECKS", pick([operator.canary?.connectivity, controls.connectivity], ["status", "state"])),
    controlCard("canary.eligibility.verify", "Verify candidate eligibility", "Reads existing research gates for one candidate.", [{ name: "candidate_id", label: "Candidate ID" }]),
    controlCard("canary.eligibility.mark", "Mark candidate eligible", "Marks a candidate only after native eligibility validation.", [{ name: "candidate_id", label: "Candidate ID" }], "MARK CANARY ELIGIBLE"),
    controlCard("canary.generate_signal", "Generate candidate signal", "Generates the existing bounded signal record for one candidate.", [{ name: "candidate_id", label: "Candidate ID" }]),
    controlCard("canary.arm", "Arm canary", "Arms the existing canary only after credentials, settings, and native gates pass.", [{ name: "candidate_id", label: "Candidate ID" }], "ARM"),
    controlCard("canary.enable_auto", "Enable auto canary", "Enables the existing autonomous path only after fresh connectivity, credential binding, settings generation, and native confirmation checks.", [{ name: "venue", label: "Venue", value: "polymarket", type: "select", options: ["polymarket"] }, { name: "config_id", label: "Settings config ID" }, { name: "expected_generation", label: "Expected settings generation", type: "number", step: "1" }], "ENABLE AUTO CANARY POLYMARKET <config_id> <generation>"),
    controlCard("canary.kill", "Kill canary", "Emergency native kill boundary. This is separate from Stop and remains specialist-only.", [], "KILL"),
    controlCard("canary.recover_entry", "Recover unknown entry", "Production-only recovery requires exact persisted event, signal, and exchange order identifiers.", [{ name: "event_id", label: "Event ID" }, { name: "signal_id", label: "Signal ID" }, { name: "exchange_order_id", label: "Exchange order ID" }], "RECOVER UNKNOWN ENTRY"),
    controlCard("risk.settings.activate_draft", "Activate risk draft", "Advanced operation: activates one exact reviewed settings generation; unknown fields remain server-authoritative.", [{ name: "config_id", label: "Draft config ID" }, { name: "expected_generation", label: "Expected generation", type: "number", step: "1" }], "ACTIVATE RISK SETTINGS DRAFT"),
    controlCard("execution_authorization.activate", "Activate authorization", "Advanced legacy activation path; the normal two-stage review remains the supported path.", [{ name: "authorization_id", label: "Authorization ID" }, { name: "expected_generation", label: "Expected generation", type: "number", step: "1" }], "ACTIVATE EXPLORATORY AUTHORIZATION"),
    controlCard("execution_authorization.revoke", "Revoke authorization", "Revokes the exact active authorization after native generation checks.", [{ name: "authorization_id", label: "Authorization ID" }, { name: "expected_generation", label: "Expected generation", type: "number", step: "1" }, { name: "reason", label: "Reason" }], "REVOKE EXPLORATORY AUTHORIZATION"),
    controlCard("rolling.admission.review", "Review rolling policy", "Reviews a bounded rolling admission policy draft with exact risk binding fields.", [{ name: "values", label: "Policy values (JSON)", type: "textarea", placeholder: "{\"policy_id\":\"...\",\"policy_version\":\"...\"}" }], "REVIEW ROLLING ADMISSION POLICY"),
    controlCard("rolling.admission.activate", "Activate rolling policy", "Advanced policy activation requires exact policy or draft identity and risk generation.", [{ name: "policy_id", label: "Policy ID" }, { name: "policy_version", label: "Policy version" }, { name: "draft_id", label: "Draft ID" }, { name: "draft_version", label: "Draft version" }], "ACTIVATE ROLLING ADMISSION POLICY"),
  ];
  const statusCards = [
    runtimeStatusCard("AXIOM node", operator.node, "Start and stop are supervisor/CLI lifecycle operations. The supported browser action is Restart node above.", [["Heartbeat", ["heartbeat_at", "last_heartbeat_at"]]]),
    runtimeStatusCard("Paper engine", operator.paper, "Read-only paper status. Browser configuration and trading controls are intentionally not exposed.", [["Last run", ["last_run_at", "last_cycle_ended_at"]]]),
    runtimeStatusCard("Collector", operator.collector, "Independent collector restart is not exposed; use the supported Restart node action above.", [["Last cycle", ["last_cycle_ended_at", "last_successful_cycle"]], ["Next scheduled", ["next_scheduled_collection_at", "next_run_at"]]]),
  ].join("");
  const metadata = technical({ node: operator.node, bootstrap: operator.bootstrap, hermes: operator.hermes, collector: operator.collector, paper: operator.paper, controls }, "Advanced status provenance");
  return `${actionCards.join("")}${statusCards}${metadata}`;
}
function renderSettings(state) {
  const risk = state.data.risk || state.data.operator?.risk_settings || state.data.settings || {};
  const active = risk.active || risk.active_config || {};
  const draft = risk.draft || risk.proposed || {};
  const values = active.values || active.limits || risk.effective_limits || risk.active_limits || {};
  const draftValues = draft.values || draft.limits || {};
  const allKeys = [...new Set([...Object.keys(values), ...Object.keys(draftValues)])].filter(key => !SECRET_KEY.test(key)).slice(0, 64);
  const fields = allKeys.filter(key => !(REVIEW_LIMIT_ALIASES[key] && allKeys.includes(REVIEW_LIMIT_ALIASES[key])));
  const fieldErrors = risk.errors && typeof risk.errors === "object" ? risk.errors : {};
  const riskFields = fields.filter(key => REVIEW_LIMIT_LABELS[key]);
  const unsupportedFields = fields.filter(key => !REVIEW_LIMIT_LABELS[key]);
  const controls = riskFields.map(key => {
    const [label, unit] = REVIEW_LIMIT_LABELS[key];
    const value = values[key];
    const draftPresent = Object.prototype.hasOwnProperty.call(draftValues, key);
    const draftValue = draftValues[key];
    const current = draftPresent ? draftValue : value;
    const changed = draftPresent && String(draftValue) !== String(value);
    const optional = OPTIONAL_RISK_FIELDS.has(key);
    const hasValue = optional || (current !== null && current !== undefined && current !== "");
    const alias = Boolean(REVIEW_LIMIT_ALIASES[key]);
    const editable = hasValue && !alias && (optional || typeof current === "string" || typeof current === "number" || typeof current === "boolean");
    const error = fieldErrors[key];
    const activeText = value === null || value === undefined || value === "" ? (optional ? "Not configured" : "Unknown (not provided)") : reviewText(value);
    if (!editable) return `<div class="field"><span>${esc(label)}${unit ? ` <small>(${esc(unit)})</small>` : ""}</span><strong>${esc(activeText)}</strong><small>${changed ? `Draft: ${esc(reviewText(draftValue))} · ` : ""}Not editable until the authoritative projection provides a supported value.</small>${error ? `<span class="field-error">${esc(error)}</span>` : ""}</div>`;
    if (key === "max_submitted_orders_per_day") {
      const numeric = Number(current);
      const custom = ![5, 10, 20].includes(numeric);
      const options = [5, 10, 20].map(option => `<option value="${option}" ${!custom && numeric === option ? "selected" : ""}>${option}</option>`).join("");
      return `<label class="field"><span>${esc(label)}${unit ? ` <small>(${esc(unit)})</small>` : ""}</span><select data-risk-field="${esc(key)}" data-risk-custom-value="${custom ? esc(current) : ""}">${options}<option value="custom" ${custom ? "selected" : ""}>Custom${custom ? ` (${esc(current)})` : ""}</option></select><small>Active: ${esc(activeText)}${changed ? ` · Draft: ${esc(reviewText(draftValue))}` : ""}. Presets are 5, 10, and 20; custom preserves the current value.</small>${error ? `<span class="field-error">${esc(error)}</span>` : ""}</label>`;
    }
    const bool = typeof current === "boolean";
    const numeric = typeof current === "number";
    return `<label class="field"><span>${esc(label)}${unit ? ` <small>(${esc(unit)})</small>` : ""}</span><input data-risk-field="${esc(key)}" data-risk-optional="${optional ? "true" : "false"}" type="${bool ? "checkbox" : numeric ? "number" : "text"}" ${bool ? (current ? "checked" : "") : `value="${esc(current ?? "")}"`} ${numeric ? 'step="any" inputmode="decimal"' : ""} aria-label="${esc(label)}"><small>Active: ${esc(activeText)}${changed ? ` · Draft: ${esc(reviewText(draftValue))}` : ""}. Save creates a review draft only.</small>${error ? `<span class="field-error">${esc(error)}</span>` : ""}</label>`;
  }).join("");
  const unsupported = unsupportedFields.length ? `<section class="section"><div class="section-heading"><div><h2>Other persisted values</h2><p class="muted">These fields are retained for provenance but are not editable by this surface.</p></div></div><dl class="detail-grid">${unsupportedFields.map(key => kv(human(key), reviewValueMarkup(human(key), draftValues[key] ?? values[key]))).join("")}</dl></section>` : "";
  const operator = state.data.operator || {};
  const auth = operator.execution_authorization || state.data.canary?.execution_authorization || {};
  const stop = risk.stop_rules || active.stop_rules || draft.stop_rules || risk.stops || auth.active?.stop_rules || auth.draft?.stop_rules || auth.authorization?.stop_rules || auth.choices?.stop_rules || {};
  const errors = Object.entries(state.errors || {}).map(([key, value]) => notice(`${human(key)} unavailable`, String(value), "warn")).join("");
  const stopSummary = Object.keys(stop).length ? reviewValueMarkup("Stop rules", stop) : empty("Stop rules unavailable", "No authoritative stop-rule projection is available.");
  const changed = fields.filter(key => draftValues[key] !== undefined && String(draftValues[key]) !== String(values[key]));
  const diff = changed.length ? `<section class="section"><h2>Draft changes awaiting activation</h2><dl class="detail-grid">${changed.map(key => kv(REVIEW_LIMIT_LABELS[key]?.[0] || human(key), `<strong>${esc(reviewText(values[key]))}</strong><span class="muted"> → draft ${esc(reviewText(draftValues[key]))}</span>`)).join("")}</dl><p class="muted">No draft is active until the exact advanced activation action succeeds.</p></section>` : "";
  const connectivity = operator.canary?.connectivity || operator.connectivity || {};
  const system = state.data.system || {};
  const status = state.data.status || {};
  const metadata = `<section class="section"><h2>Connections and system</h2><dl class="detail-grid">${kv("Canary connection", esc(human(connectivity.status || connectivity.state || "Not available")))}${kv("Node", esc(human(operator.node?.status || operator.node?.state || "Not available")))}${kv("Storage", esc(system.storage?.status || system.storage_state || status.storage?.status || "Not available"))}${kv("Service status", esc(status.status || status.state || "Not available"))}</dl>${technical({ connectivity, system, status }, "Connection and storage provenance")}</section>`;
  const advanced = renderAdvanced(operator);
  return `${errors}<div class="stack"><section class="section"><div class="section-heading"><div><p class="eyebrow">Manage</p><h2>Settings & system</h2><p class="muted">Active values are authoritative. Save creates a draft for review; nothing activates automatically.</p></div>${badge(human(risk.status || "UNKNOWN"), risk.status === "CURRENT" ? "good" : "warn")}</div><h3>Risk limits</h3><form id="risk-form" class="form-grid">${controls || empty("Risk values unavailable", "The authoritative settings projection did not provide supported editable values.")}<div class="actions"><button class="button button-secondary" type="submit" data-action="save-risk" ${controls ? "" : "disabled"}>Save changes as draft</button><a class="button button-quiet" href="${hrefForRoute({ ...state.route, view: "live", section: "polymarket" })}">Review live boundary</a></div></form></section>${unsupported}${diff}<section class="section"><div class="section-heading"><div><p class="eyebrow">Safety boundary</p><h2>Supported stop rules</h2><p class="muted">These rules come from the authorization projection and are separate from risk limits.</p></div></div>${stopSummary}</section>${metadata}<section class="section"><div class="section-heading"><div><p class="eyebrow">Specialist</p><h2>Advanced operations</h2><p class="muted">Existing native controls only; unsupported actions are not synthesized here.</p></div></div><div class="control-stack">${advanced}</div></section></div>`;
}
let currentRoute = parseRoute(typeof location !== "undefined" ? location.search : "");
let appState = { route: currentRoute, data: {}, connected: true, receivedAt: null, generation: 0, controller: null, timer: null, actionPending: false, actionVersion: 0, reviewTrigger: null, controlToken: null, riskSaveNotice: null };
let controlTokenPromise = null;
async function ensureControlToken() {
  if (typeof appState.controlToken === "string") return appState.controlToken;
  if (!controlTokenPromise) {
    controlTokenPromise = requestJson("/api/control-token")
      .then(body => {
        if (typeof body.token !== "string" || !body.token) throw new Error(body.error || "CONTROL_TOKEN_UNAVAILABLE");
        appState.controlToken = body.token;
        return body.token;
      })
      .catch(() => { appState.controlToken = ""; return ""; });
  }
  return controlTokenPromise;
}

function writeRoute(route, push = true) { const next = { ...route }; if (next.view !== currentRoute.view || next.section !== currentRoute.section || next.selected !== currentRoute.selected || next.record_kind !== currentRoute.record_kind) { next.detail_page = 1; next.shadow_page = 1; } const url = `${location.pathname}${hrefForRoute(next)}`; if (push) history.pushState({}, "", url); else history.replaceState({}, "", url); currentRoute = next; appState.route = next; if (next.view !== "settings") appState.riskSaveNotice = null; }
function fieldKey(field) { return field?.dataset?.controlField || field?.dataset?.riskField || field?.name || field?.id || ""; }
function captureFormState(root) {
  const values = {};
  root.querySelectorAll("input,select,textarea").forEach(field => {
    const key = fieldKey(field);
    if (key) values[key] = field.type === "checkbox" ? field.checked : field.value;
  });
  const active = document.activeElement;
  const activeKey = active && root.contains(active) ? fieldKey(active) : "";
  const focus = activeKey ? { key: activeKey, start: active.selectionStart, end: active.selectionEnd } : null;
  const disclosures = Array.from(root.querySelectorAll("details[open]")).map(disclosureKey).filter(Boolean);
  const scroll = {
    root: root.scrollTop,
    window: typeof window !== "undefined" ? window.scrollY : 0,
    tables: Array.from(root.querySelectorAll(".table-scroll")).map(node => node.scrollTop),
  };
  return { values, focus, disclosures, scroll };
}
function disclosureKey(node) {
  const summary = node.querySelector(":scope > summary")?.textContent.trim() || "";
  const detail = node.closest("[data-detail-id]")?.getAttribute("data-detail-id") || "";
  if (detail) return `detail:${detail}:${summary}`;
  const row = node.closest("tr");
  const link = row?.querySelector("a[href]");
  if (link) {
    try {
      const url = new URL(link.href, typeof location !== "undefined" ? location.href : "http://localhost/");
      const selected = url.searchParams.get("selected") || url.searchParams.get("detail") || "";
      if (selected) return `row:${selected}:${url.searchParams.get("record_kind") || ""}:${summary}`;
    } catch {}
  }
  const container = node.closest(".panel,.section,article");
  const heading = container?.querySelector(":scope > h2,:scope > h3,:scope > h4")?.textContent.trim() || "";
  return `${container?.className || "content"}:${heading}:${summary}`;
}
function restoreFormState(root, previous) {
  const values = previous?.values || previous || {};
  for (const [key, value] of Object.entries(values)) {
    const field = Array.from(root.querySelectorAll("input,select,textarea")).find(candidate => fieldKey(candidate) === key);
    if (!field) continue;
    if (field.type === "checkbox") field.checked = Boolean(value);
    else field.value = value;
  }
  const disclosureSet = new Set(previous?.disclosures || []);
  root.querySelectorAll("details").forEach(disclosure => { disclosure.open = disclosureSet.has(disclosureKey(disclosure)); });
  const focus = previous?.focus;
  if (focus?.key) {
    const field = Array.from(root.querySelectorAll("input,select,textarea")).find(candidate => fieldKey(candidate) === focus.key);
    if (field) {
      field.focus({ preventScroll: true });
      if (typeof field.setSelectionRange === "function" && Number.isInteger(focus.start)) field.setSelectionRange(focus.start, focus.end ?? focus.start);
    }
  }
  const scroll = previous?.scroll;
  if (scroll) {
    root.scrollTop = scroll.root || 0;
    if (typeof window !== "undefined") window.scrollTo({ top: scroll.window || 0, behavior: "auto" });
    root.querySelectorAll(".table-scroll").forEach((node, index) => { node.scrollTop = scroll.tables?.[index] || 0; });
  }
}
function detailResourceKey(route = {}) {
  if (route.view === "markets") return "market";
  if (route.view === "research" && route.section === "strategies") return "candidate";
  if (route.view === "research" && route.section === "crypto") return "crypto";
  if (route.view === "research" && route.section === "automation") return route.record_kind === "shadow" ? "shadow" : "hermes";
  if (route.view === "research" && route.section === "data") return "dataset";
  if (route.view === "activity") return "activity";
  if ((route.view === "portfolio" && route.section === "real") || (route.view === "live" && route.section === "polymarket")) return "record";
  return "";
}
function renderRoute(state, previousForm = {}) {
  const root = document.querySelector("#content");
  if (!root) return;
  const pageData = state.data.page || (state.route.view === "live" ? state.data.canary || state.data.binance : {});
  const shared = {
    ...state,
    route: state.route,
    errors: state.errors || {},
    loading: Boolean(state.loading),
    operator: state.data.operator || {},
    canary: state.data.canary || pageData || {},
    ledger: state.data.operator?.ledger || {},
    execution_authorization: state.data.operator?.execution_authorization || {},
    risk_settings: state.data.operator?.risk_settings || {},
    actions: state.data.operator?.actions || [],
    ui,
  };
  const spec = pageSpec(state.route) || {};
  const detailErrorKey = detailResourceKey(state.route);
  const detailError = detailErrorKey ? state.errors?.[`detail:${detailErrorKey}`] : undefined;
  let html;
  if (state.route.view === "home") html = renderHome(state);
  else if (state.route.view === "settings") html = renderSettings(state);
  else if (state.route.view === "live") {
    html = renderLive(state);
    if (state.route.selected) {
      const pageContext = { ...shared, data: pageData, detail: state.data.detail || {}, error: shared.errors.page };
      html += renderDetail({ ...pageContext, error: detailError });
    }
  }
  else {
    const pageContext = { ...shared, data: pageData, detail: state.data.detail || {}, error: shared.errors.page };
    html = renderPage(pageContext);
    if (state.route.selected) html += renderDetail({ ...pageContext, error: detailError });
  }
  const chrome = `${destinationLinks(state.route)}${state.route.view === "home" || state.route.view === "settings" ? "" : browseControls(state.route, spec, pageData, state)}`;
  const loading = state.loading ? notice("Loading current projection", "Reading the bounded server records. Existing values remain last-known until this refresh completes.", "info") : "";
  const liveStop = "";
  root.innerHTML = `${chrome}${loading}${liveStop}${html}`;
  restoreFormState(root, previousForm);
  const riskNotice = appState.riskSaveNotice;
  if (state.route.view === "settings" && riskNotice) {
    const riskForm = root.querySelector("#risk-form");
    if (riskForm) {
      const node = document.createElement("p");
      node.className = `notice notice-${riskNotice.tone}`;
      node.setAttribute("role", "status");
      node.textContent = riskNotice.text;
      riskForm.appendChild(node);
    }
  }
  updateNav();
}
function updateNav() { document.querySelectorAll("[data-nav-view]").forEach(node => node.classList.toggle("active", node.dataset.navView === appState.route.view && (!node.dataset.navSection || node.dataset.navSection === appState.route.section))); const title = document.querySelector("#page-title"); if (title) title.textContent = routeTitle(appState.route); }

async function requestJson(url, signal) {
  const controller = new AbortController();
  let timer = null;
  let routeAborted = false;
  const abort = () => { routeAborted = true; controller.abort(); };
  if (signal) {
    if (signal.aborted) { controller.abort(); throw Object.assign(new Error("ROUTE_ABORTED"), { code: "ROUTE_ABORTED" }); }
    signal.addEventListener("abort", abort, { once: true });
  }
  timer = setTimeout(() => controller.abort(), 10000);
  try {
    const response = await fetch(url, { cache: "no-store", signal: controller.signal, headers: { Accept: "application/json" } });
    let body = {};
    try { body = await response.json(); } catch (error) { if (controller.signal.aborted) throw error; }
    if (!response.ok) throw Object.assign(new Error(body.error || `HTTP ${response.status}`), { status: response.status, body });
    return body;
  } catch (error) {
    if (routeAborted || signal?.aborted) throw Object.assign(new Error("ROUTE_ABORTED"), { code: "ROUTE_ABORTED" });
    if (controller.signal.aborted) throw Object.assign(new Error("REQUEST_TIMEOUT"), { code: "REQUEST_TIMEOUT" });
    throw error;
  } finally {
    clearTimeout(timer);
    signal?.removeEventListener("abort", abort);
  }
}
async function controlFetch(url, init, timeout = 12000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeout);
  try {
    const response = await fetch(url, { ...init, signal: controller.signal });
    let body = {};
    try { body = await response.json(); } catch (error) { if (controller.signal.aborted) throw error; }
    return { response, body };
  } catch (error) {
    if (controller.signal.aborted) throw Object.assign(new Error("CONTROL_TIMEOUT"), { code: "CONTROL_TIMEOUT" });
    throw error;
  } finally {
    clearTimeout(timer);
  }
}
function endpointUrl(spec, route) { if (!spec?.endpoint) return null; const endpoint = typeof spec.endpoint === "function" ? spec.endpoint(route) : spec.endpoint; if (!endpoint) return null; const q = new URLSearchParams({ page: String(route.page), page_size: String(route.page_size), direction: route.direction }); if (route.filter) q.set("filter", route.filter); if (route.sort) q.set("sort", route.sort); for (const key of ["category", "quality", "market", "symbol", "source", "source_type", "timeframe", "settlement", "stage", "status", "severity", "kind", "environment"]) if (route[key]) q.set(key, route[key]); return `${endpoint}${endpoint.includes("?") ? "&" : "?"}${q}`; }
async function loadRoute({ force = false } = {}) {
  clearTimeout(appState.timer);
  if (appState.controller) appState.controller.abort();
  const controller = new AbortController();
  const generation = ++appState.generation;
  appState.controller = controller;
  const route = appState.route;
  const routeKey = JSON.stringify(route);
  const sameRoute = appState.routeKey === routeKey;
  const form = sameRoute ? captureFormState(document.querySelector("#content") || document) : {};
  const spec = pageSpec(route) || {};
  const requestedKeys = route.view === "home"
    ? ["operator", "canary"]
    : route.view === "settings"
      ? ["operator", "risk", "system", "status"]
      : ["operator", "page", ...(route.view === "portfolio" ? ["canary"] : [])];
  if (!sameRoute) {
    for (const key of ["canary", "binance", "page", "risk", "system", "status"]) {
      if (!requestedKeys.includes(key)) delete appState.data[key];
    }
    appState.data.detail = {};
    appState.errors = {};
    appState.routeKey = routeKey;
  }
  appState.loading = true;
  renderRoute(appState, form);
  const jobs = {};
  const add = (key, url) => {
    if (!url) return;
    jobs[key] = requestJson(url, controller.signal).then(value => ({ key, value })).catch(error => {
      error.resource = key;
      throw error;
    });
  };
  if (route.view === "home") {
    add("operator", "/api/ui-state");
    add("canary", "/api/v2/canary");
  } else if (route.view === "settings") {
    add("risk", "/api/risk-settings");
    add("operator", "/api/ui-state");
    add("system", "/api/system");
    add("status", "/api/status");
  } else {
    add("page", endpointUrl(spec, route));
    add("operator", "/api/ui-state");
    if (route.view === "portfolio") add("canary", "/api/v2/canary");
    if (spec.detailRequests) {
      const details = spec.detailRequests(route) || {};
      for (const [key, url] of Object.entries(details)) add(`detail:${key}`, url);
    }
  }
  const results = await Promise.allSettled(Object.values(jobs));
  if (generation !== appState.generation) return;
  let successful = 0;
  appState.errors = {};
  for (const result of results) {
    if (result.status === "fulfilled") {
      successful++;
      const { key, value } = result.value;
      if (key.startsWith("detail:")) (appState.data.detail ||= {})[key.slice(7)] = value;
      else appState.data[key] = value;
    } else {
      const key = result.reason?.resource || "request";
      appState.errors[key] = result.reason?.message || "Unable to load this bounded projection.";
    }
  }
  if (route.view === "markets" && successful && !appState.errors.page && generation === appState.generation) {
    const page = appState.data.page;
    const nativePages = Number(page?.pages);
    const requestedPage = Number(route.page);
    const rows = Array.isArray(page?.items) ? page.items : Array.isArray(page?.markets) ? page.markets : [];
    if (Number.isInteger(nativePages) && nativePages > 0 && Number.isInteger(requestedPage) && requestedPage > nativePages && rows.length === 0) {
      const correctedRoute = { ...route, page: nativePages };
      try {
        const correctedPage = await requestJson(endpointUrl(spec, correctedRoute), controller.signal);
        if (generation !== appState.generation) return;
        const correctedRows = Array.isArray(correctedPage?.items) ? correctedPage.items : Array.isArray(correctedPage?.markets) ? correctedPage.markets : [];
        if (correctedRows.length) {
          writeRoute(correctedRoute, false);
          appState.routeKey = JSON.stringify(correctedRoute);
          appState.data.page = correctedPage;
        }
      } catch (error) {
        if (generation !== appState.generation) return;
      }
    }
  }
  appState.connected = successful > 0;
  if (successful) appState.receivedAt = new Date().toISOString();
  appState.loading = false;
  renderRoute(appState, form);
  appState.controller = null;
  const pollMs = spec.pollMs || 60000;
  appState.timer = setTimeout(() => loadRoute(), pollMs);
}

function reviewProjection(state = appState) {
  const operator = state.data?.operator || {};
  const canary = state.data?.canary || {};
  const controls = operator.operator_controls?.exploratory_live_review || canary.operator_controls?.exploratory_live_review || {};
  const execution = operator.execution_authorization || canary.execution_authorization || {};
  const authorization = execution.draft || execution.authorization || execution.active || {};
  const choices = controls.choices || controls.authorization_choices || authorization.choices || authorization || {};
  return { operator, canary, controls, execution, authorization, choices };
}
function reviewValues(source = reviewProjection()) {
  const { controls, authorization, choices } = source;
  const values = {};
  const bindings = {};
  for (const key of ["scope_hash", "scope_version", "scope_draft_id", "scope_draft_hash", "scope_draft_version", "selection_id", "selection_hash", "active_settings_hash", "active_settings_generation", "proposed_allocation_total", "proposed_allocation_risk_digest"]) {
    const value = pick([controls, authorization], [key]);
    if (value !== null) bindings[key] = value;
  }
  const purpose = pick([choices, authorization, controls], ["purpose"]);
  const sharedAllocation = pick([choices, authorization, controls], ["shared_allocation"]);
  const lifetimeBudget = pick([choices, authorization, controls], ["lifetime_budget"]);
  const stopRules = pick([choices, authorization, controls], ["stop_rules"]);
  const expiryAnchor = pick([choices, authorization, controls], ["expiry_anchor", "expiry_anchor_type"]);
  const duration = pick([choices, authorization, controls], ["duration_seconds"]);
  const members = reviewMembers(source);
  const missing = [];
  if (purpose === null || purpose === "") missing.push("purpose");
  if (sharedAllocation === null || sharedAllocation === "") missing.push("shared allocation");
  const lifetimeAmount = lifetimeBudget && typeof lifetimeBudget === "object" ? pick([lifetimeBudget], ["max_notional_usd", "amount"]) : lifetimeBudget;
  if (lifetimeAmount === null || lifetimeAmount === "") missing.push("lifetime budget");
  if (!stopRules || typeof stopRules !== "object" || !Object.keys(stopRules).length) missing.push("stop rules");
  if (expiryAnchor === null || expiryAnchor === "") missing.push("expiry anchor");
  const durationNumber = Number(duration);
  if (duration === null || duration === "" || !Number.isFinite(durationNumber) || durationNumber <= 0) missing.push("duration");
  if (!members.length) missing.push("proposed members");
  if (purpose !== null) values.purpose = purpose;
  if (sharedAllocation !== null) values.shared_allocation = sharedAllocation;
  if (lifetimeBudget !== null) values.lifetime_budget = lifetimeBudget;
  if (stopRules !== null) values.stop_rules = stopRules;
  if (expiryAnchor !== null) values.expiry_anchor = expiryAnchor;
  if (duration !== null) values.duration_seconds = duration;
  if (bindings && typeof bindings === "object") Object.assign(values, bindings);
  const required = Boolean(pick([authorization, controls, choices], ["adverse_evidence_ack_required"]) || authorization.adverse_evidence_ack?.required);
  if (required) values.adverse_evidence_ack = true;
  return { values, adverseRequired: required, missing };
}
const REVIEW_VOLATILE_FIELD = /(?:^|_)(?:created|updated|started|completed|observed|checked|heartbeat|timestamp|readiness|price|mid|ask|bid|now)(?:$|_)/i;
function reviewFingerprintValue(value, depth = 0) {
  if (depth > 5 || value === null || value === undefined || typeof value !== "object") return value;
  if (Array.isArray(value)) return value.slice(0, 32).map(item => reviewFingerprintValue(item, depth + 1));
  return Object.keys(value).sort().reduce((result, key) => {
    if (REVIEW_VOLATILE_FIELD.test(key)) return result;
    result[key] = reviewFingerprintValue(value[key], depth + 1);
    return result;
  }, {});
}
function reviewFingerprint(source = reviewProjection()) {
  const { execution, authorization, controls, choices } = source;
  const terms = {};
  for (const key of [
    "purpose",
    "shared_allocation",
    "lifetime_budget",
    "stop_rules",
    "expiry_anchor",
    "duration_seconds",
    "adverse_evidence_ack",
    "adverse_evidence_ack_required",
    "policy_id",
    "policy_version",
    "policy_hash",
    "active_settings_hash",
    "active_settings_generation",
    "proposed_allocation_total",
    "proposed_allocation_risk_digest",
    "scope_hash",
    "scope_version",
    "scope_draft_id",
    "scope_draft_hash",
    "scope_draft_version",
    "selection_id",
    "selection_hash",
    "exact_strategy_versions",
    "strategy_version_ids",
    "draft_member_bindings",
    "setup_bindings",
  ]) {
    const value = pick([choices, authorization, controls, execution], [key]);
    if (value !== null && value !== undefined) terms[key] = reviewFingerprintValue(value);
  }
  const members = reviewFingerprintValue(reviewMembers(source));
  const setups = reviewFingerprintValue(pick([controls, authorization], ["selected_setups", "setup_bindings"]));
  const adverse = reviewFingerprintValue(pick([controls, authorization], ["adverse_evidence", "evidence", "adverse_evidence_ack"]));
  return JSON.stringify({
    status: execution.status || authorization.status || controls.status,
    authorization_id: execution.authorization_id || authorization.authorization_id || authorization.id,
    generation: execution.generation || authorization.generation,
    authorization_bindings: reviewFingerprintValue(authorization.authorization_bindings || controls.authorization_bindings || terms.draft_member_bindings || terms.setup_bindings),
    terms,
    members,
    setups,
    adverse,
    choices: reviewFingerprintValue(choices),
  });
}
function reviewMembers(source = reviewProjection()) {
  const { controls, authorization, execution } = source;
  const candidates = [
    controls.members,
    controls.proposal?.members,
    authorization.proposal?.members,
    execution.proposal?.members,
  ];
  return candidates.find(value => Array.isArray(value) && value.length) || [];
}
const REVIEW_LIMIT_LABELS = Object.freeze({
  target_notional_usd: ["Maximum all-in buy", "USD per buy"],
  max_exposure_usd: ["Maximum aggregate exposure", "USD"],
  max_daily_loss_usd: ["Realized-loss entry stop", "USD"],
  max_open_positions: ["Maximum open positions", "positions"],
  max_orders_per_day: ["Maximum submitted orders", "orders per day"],
  max_all_in_buy_usd: ["Maximum all-in buy", "USD per buy"],
  max_fee_reserve_usd: ["Fee reserve", "USD"],
  max_gross_daily_buy_usd: ["Maximum gross daily buy", "USD per day"],
  max_aggregate_open_cost_usd: ["Maximum open-position cost", "USD"],
  max_aggregate_exposure_usd: ["Maximum aggregate exposure", "USD"],
  max_positions: ["Maximum open positions", "positions"],
  max_submitted_orders_per_day: ["Maximum submitted orders", "orders per day"],
  realized_loss_entry_stop_usd: ["Realized-loss entry stop", "USD"],
  equity_loss_entry_stop_usd: ["Equity-loss entry stop", "USD"],
  max_slippage_bps: ["Maximum slippage", "basis points"],
  per_market_buy_cap_usd: ["Per-market buy cap", "USD"],
  per_event_buy_cap_usd: ["Per-event buy cap", "USD"],
  cumulative_buy_cap_usd: ["Cumulative buy cap", "USD"],
});
const REVIEW_LIMIT_ALIASES = Object.freeze({
  target_notional_usd: "max_all_in_buy_usd",
  max_exposure_usd: "max_aggregate_exposure_usd",
  max_daily_loss_usd: "realized_loss_entry_stop_usd",
  max_open_positions: "max_positions",
  max_orders_per_day: "max_submitted_orders_per_day",
});
const OPTIONAL_RISK_FIELDS = new Set(["per_market_buy_cap_usd", "per_event_buy_cap_usd", "cumulative_buy_cap_usd"]);
function durationText(value) {
  if (value === null || value === undefined || (typeof value === "string" && !value.trim()) || typeof value === "boolean") return "Not available";
  const seconds = Number(value);
  if (!Number.isFinite(seconds) || seconds < 0) return "Not available";
  if (seconds % 86400 === 0 && seconds >= 86400) return `${number(seconds)} seconds (${number(seconds / 86400)} days)`;
  if (seconds % 3600 === 0 && seconds >= 3600) return `${number(seconds)} seconds (${number(seconds / 3600)} hours)`;
  if (seconds % 60 === 0 && seconds >= 60) return `${number(seconds)} seconds (${number(seconds / 60)} minutes)`;
  return `${number(seconds)} seconds`;
}
function reviewText(value) {
  if (value === null || value === undefined || value === "") return "Not available";
  const text = String(value);
  const knownCodes = ["ACTIVE", "BLOCKED", "CONFIRMED", "DRAFT", "EXPIRED", "FINAL_CONFIRMATION", "NONE", "REVIEWED_ONLY", "REVOKED", "STOP_AND_REVIEW", "UNACTIVATED", "UNAVAILABLE", "UNREVIEWED"];
  return knownCodes.includes(text) || /^[A-Z0-9]+(?:_[A-Z0-9]+)+$/.test(text) ? human(text) : text;
}
function reviewCopy(label, value) {
  if (value === null || value === undefined || value === "") return "";
  return `<span class="review-identifier"><span>${esc(label)}</span> <code>${esc(value)}</code><button class="button button-quiet copy-button" type="button" data-copy="${esc(value)}" data-copy-label="Copy ${esc(label)}">Copy</button></span>`;
}
function reviewScalarMarkup(label, value) {
  if (value === null || value === undefined || value === "") return `<span class="muted">Not configured</span>`;
  if (label === "Duration" || label === "Duration (seconds)") return `<strong>${esc(durationText(value))}</strong>`;
  const numeric = typeof value === "number" || (typeof value === "string" && Number.isFinite(Number(value.replace(/[$,]/g, ""))));
  const normalizedLabel = String(label).replace(/\s+/g, "_");
  if (typeof value === "string" && /(?:_at|timestamp|time)$/i.test(normalizedLabel)) return `<strong>${esc(formatTime(value))}</strong>`;
  if (/_usd$/i.test(normalizedLabel) || /budget|allocation|affordability|cost|exposure|limit/i.test(label)) {
    const rendered = numeric ? money(value) : reviewText(value);
    return `<strong>${esc(rendered)}</strong>${numeric && /_usd$/i.test(normalizedLabel) ? '<small class="muted"> USD</small>' : ""}`;
  }
  if (typeof value === "boolean") return `<strong>${value ? "Yes" : "No"}</strong>`;
  return `<strong>${esc(reviewText(value))}</strong>`;
}
function reviewObjectMarkup(value, title = "") {
  if (!value || typeof value !== "object") return reviewScalarMarkup(title, value);
  const entries = Object.entries(value).slice(0, 32);
  if (!entries.length) return `<span class="muted">Not configured</span>`;
  return `<dl class="detail-grid">${entries.map(([key, child]) => {
    const label = human(key);
    const rendered = /binding|hash|digest/i.test(key)
      ? technical(child, `${label} (technical)`)
      : Array.isArray(child)
        ? `<ul>${child.slice(0, 32).map(item => `<li>${esc(reviewText(typeof item === "object" ? pick([item], ["name", "id", "value"]) : item))}</li>`).join("")}</ul>`
        : child && typeof child === "object"
          ? reviewObjectMarkup(child, label)
          : reviewScalarMarkup(label, child);
    return kv(label, rendered);
  }).join("")}</dl>`;
}
function reviewMembersMarkup(value) {
  const members = Array.isArray(value) ? value : value && typeof value === "object" ? [value] : [];
  if (!members.length) return `<span class="muted">None recorded</span>`;
  return `<div class="review-cards">${members.slice(0, 32).map(member => {
    if (!member || typeof member !== "object") return `<article class="review-card"><strong>Unnamed proposed member</strong><p>${esc(reviewText(member))}</p></article>`;
    const name = pick([member], ["name", "candidate_name", "strategy_name"]) || "Unnamed proposed member";
    const ids = ["candidate_id", "setup_id"].filter(key => member[key] !== null && member[key] !== undefined && member[key] !== "");
    const details = ["status", "allocation", "proposed_allocation", "allocation_active"].filter(key => member[key] !== null && member[key] !== undefined && member[key] !== "");
    return `<article class="review-card"><strong>${esc(name)}</strong>${ids.length ? `<div class="review-identifiers">${ids.map(key => reviewCopy(human(key), member[key])).join("")}</div>` : `<p class="muted">No member identifier was provided.</p>`}${details.length ? `<dl class="detail-grid">${details.map(key => kv(human(key), reviewScalarMarkup(key, member[key]))).join("")}</dl>` : ""}${member.setup_binding ? technical(member.setup_binding, "Setup binding (technical)") : ""}</article>`;
  }).join("")}</div>`;
}
function reviewSetupsMarkup(value) {
  const setups = Array.isArray(value) ? value : value && typeof value === "object" ? [value] : [];
  if (!setups.length) return `<span class="muted">No proposed setup was provided.</span>`;
  return `<div class="review-cards">${setups.slice(0, 32).map(setup => {
    if (!setup || typeof setup !== "object") return `<article class="review-card"><strong>Unnamed proposed setup</strong><p>${esc(reviewText(setup))}</p></article>`;
    const name = pick([setup], ["name", "setup_name", "strategy_name"]) || "Unnamed proposed setup";
    const ids = ["setup_id", "setup_version", "strategy_version_id", "candidate_id"].filter(key => setup[key] !== null && setup[key] !== undefined && setup[key] !== "");
    const descriptions = [["Entry question", setup.entry_predicate || setup.entry], ["Direction", setup.direction], ["Outcome mapping", setup.outcome_mapping], ["Sizing", setup.sizing], ["Holding", setup.holding_semantics || setup.holding], ["Exit", setup.exit_semantics || setup.exit], ["Lookback", setup.lookback]].filter(([, item]) => item !== null && item !== undefined && item !== "");
    const terms = descriptions.length ? `<dl class="detail-grid">${descriptions.map(([label, item]) => kv(label, typeof item === "object" ? reviewObjectMarkup(item, label) : reviewScalarMarkup(label, item))).join("")}</dl>` : `<p class="muted">Readable frozen setup terms are unavailable; identifiers alone do not establish a complete reviewed setup.</p>`;
    return `<article class="review-card"><strong>${esc(name)}</strong>${ids.length ? `<div class="review-identifiers">${ids.map(key => reviewCopy(human(key), setup[key])).join("")}</div>` : `<p class="muted">No setup identifier was provided.</p>`}${terms}${setup.setup_hash || setup.operational_setup_hash ? technical({ setup_hash: setup.setup_hash, operational_setup_hash: setup.operational_setup_hash }, "Setup hash (technical)") : ""}</article>`;
  }).join("")}</div>`;
}
function reviewScopeMarkup(value) {
  if (!value || typeof value !== "object") return `<span class="muted">Unknown (not provided)</span>`;
  const draft = value.draft && typeof value.draft === "object" ? value.draft : value;
  const scope = draft.scope && typeof draft.scope === "object" ? draft.scope : value.scope && typeof value.scope === "object" ? value.scope : draft;
  const ids = pick([scope, draft, value], ["market_ids", "exact_market_ids"]);
  const markets = Array.isArray(ids) ? ids : [];
  const categories = pick([scope, draft], ["categories", "category_restriction"]);
  const marketTypes = pick([scope, draft], ["supported_market_types", "market_type", "instrument"]);
  const name = pick([scope, draft, value], ["name", "title", "description"]) || "Unnamed market scope";
  const restrictions = pick([scope, draft, value], ["scope_restrictions", "restrictions", "regime_restrictions"]);
  const exclusions = pick([scope, draft, value], ["exclusions", "excluded_markets"]);
  const rows = [
    ["Scope name", `<strong>${esc(name)}</strong>`],
    ["Markets", markets.length ? `<div class="review-identifiers">${markets.map(id => reviewCopy("Market ID", id)).join("")}</div>` : `<span class="muted">No exact market IDs recorded.</span>`],
    ["Categories", Array.isArray(categories) ? categories.map(item => reviewText(item)).join(", ") : categories && typeof categories === "object" ? reviewObjectMarkup(categories, "Categories") : reviewText(categories)],
    ["Market type", Array.isArray(marketTypes) ? marketTypes.map(item => reviewText(item)).join(", ") : reviewText(marketTypes)],
    ["Restrictions", restrictions && typeof restrictions === "object" ? reviewObjectMarkup(restrictions, "Restrictions") : reviewText(restrictions)],
    ["Exclusions", Array.isArray(exclusions) ? exclusions.map(item => reviewText(item)).join(", ") : reviewText(exclusions)],
  ];
  return `<div class="review-scope"><dl class="detail-grid">${rows.map(([label, rendered]) => kv(label, rendered.startsWith("<") ? rendered : `<strong>${esc(rendered)}</strong>`)).join("")}</dl>${technical(value, "Scope bindings (technical)")}</div>`;
}
function reviewRiskMarkup(value) {
  if (!value || typeof value !== "object") return `<span class="muted">Unknown (not provided)</span>`;
  const entries = Object.entries(value).filter(([key, item]) => {
    if (key === "max_aggregate_exposure_usd" && value.max_aggregate_open_cost_usd !== undefined && String(value.max_aggregate_open_cost_usd) === String(item)) return false;
    return !/^(?:target_notional_usd|max_exposure_usd|max_open_positions|max_orders_per_day|max_daily_loss_usd)$/.test(key);
  });
  if (!entries.length) return `<span class="muted">No limits recorded.</span>`;
  return `<dl class="detail-grid">${entries.map(([key, item]) => {
    const [label, unit] = REVIEW_LIMIT_LABELS[key] || [human(key), ""];
    const rendered = item === null || item === undefined || item === "" ? `<span class="muted">Not configured</span>` : `<strong>${esc(/usd$/i.test(key) ? money(item) : reviewText(item))}</strong>${unit ? `<small class="muted"> ${esc(unit)}</small>` : ""}`;
    return kv(label, rendered);
  }).join("")}</dl>`;
}
function reviewBlockersMarkup(value) {
  const items = Array.isArray(value) ? value : value && typeof value === "object" ? [value] : [];
  if (!items.length) return `<span class="muted">None recorded</span>`;
  return `<ul>${items.slice(0, 32).map(item => {
    const code = item && typeof item === "object" ? item.code || item.reason_code || item.status : item;
    const explanation = reasonPresentation(code).explanation;
    return `<li><strong>${esc(reviewText(code))}</strong><span class="muted"> — ${esc(explanation)}</span></li>`;
  }).join("")}</ul>`;
}
function reviewValueMarkup(label, value) {
  if (label === "Mode" && (typeof value === "string" || typeof value === "number")) return `<strong>${esc(reviewText(value))}</strong>${technical({ code: value }, "Mode code")}`;
  if (/^Duration/.test(label)) return reviewScalarMarkup(label, value);
  if (label === "Lifetime budget") {
    const amount = value && typeof value === "object" ? pick([value], ["max_notional_usd", "amount"]) : value;
    return amount === null || amount === undefined || amount === "" ? `<span class="muted">Unknown (not provided)</span>` : `<strong>${esc(money(amount))}</strong><span class="muted"> lifetime budget</span>`;
  }
  if (label === "Scope") return reviewScopeMarkup(value);
  if (/Proposed setups/i.test(label)) return reviewSetupsMarkup(value);
  if (/proposed members/i.test(label)) return reviewMembersMarkup(value);
  if (/Risk limits/i.test(label)) return reviewRiskMarkup(value);
  if (/affordability/i.test(label)) return reviewObjectMarkup(value, label);
  if (/Stop rules/i.test(label)) return reviewObjectMarkup(value, label);
  if (/Blockers/i.test(label)) return reviewBlockersMarkup(value);
  if (/binding|hash|digest/i.test(label)) return technical(value, `${label} (technical)`);
  if (Array.isArray(value)) {
    if (!value.length) return `<span class="muted">None recorded</span>`;
    return `<ul>${value.slice(0, 32).map(item => `<li>${item && typeof item === "object" ? reviewObjectMarkup(item, label) : reviewScalarMarkup(label, item)}</li>`).join("")}</ul>`;
  }
  if (!value || typeof value !== "object") return reviewScalarMarkup(label, value);
  return reviewObjectMarkup(value, label);
}
function reviewRows(source = reviewProjection()) {
  const { controls, execution, authorization, choices } = source;
  const { missing } = reviewValues(source);
  const rows = [
    ["Purpose", pick([choices, authorization, controls], ["purpose"])],
    ["Shared allocation", pick([choices, authorization, controls], ["shared_allocation"])],
    ["Lifetime budget", pick([choices, authorization, controls], ["lifetime_budget"])],
    ["Mode", pick([choices, authorization, controls], ["mode"])],
    ["Scope", pick([controls, authorization], ["scope", "market_scope"])],
    ["Proposed allocation total", pick([controls, authorization], ["proposed_allocation_total"])],
    ["Stop rules", pick([choices, authorization, controls], ["stop_rules"])],
    ["Duration", pick([choices, authorization, controls], ["duration_seconds"])],
    ["Expiry anchor", pick([choices, authorization, controls], ["expiry_anchor", "expiry_anchor_type"])],
    ["Expires at", pick([choices, authorization, controls], ["expires_at"])],
    ["Authorization status", pick([execution, authorization, controls], ["status", "permission"])],
    ["Exact current proposed members", reviewMembers(source)],
    ["Proposed setups", pick([controls, authorization], ["selected_setups", "setup_bindings"])],
    ["Risk limits", pick([controls, choices, authorization], ["limits", "risk_limits", "caps"])],
    ["Per-outcome affordability", pick([controls, choices, authorization], ["affordability", "outcome_limits", "per_outcome"])],
    ["Adverse evidence", pick([controls, authorization], ["adverse_evidence", "evidence", "adverse_evidence_ack"])],
    ["Readiness", pick([controls], ["readiness"])],
    ["Blockers", pick([controls], ["blockers", "no_member_reason"])],
    ["Bindings and hashes", pick([controls, authorization], ["authorization_bindings", "draft_member_bindings", "setup_bindings", "scope_hash", "selection_hash"])],
    ["Missing prerequisites", missing.length ? missing.join(", ") : null],
  ];
  const always = new Set(["Lifetime budget", "Scope", "Exact current proposed members", "Proposed setups", "Risk limits", "Per-outcome affordability"]);
  return rows.filter(([label, value]) => always.has(label) || (value !== null && value !== undefined && value !== "")).map(([label, value]) => kv(label, reviewValueMarkup(label, value))).join("");
}
function closeReviewDialog() {
  const dialog = document.querySelector("#review-dialog");
  const wasOpen = Boolean(dialog?.open);
  if (wasOpen) {
    appState.actionVersion++;
    appState.actionPending = false;
    dialog.close();
  }
  dialog?.remove();
  const trigger = appState.reviewTrigger;
  appState.reviewTrigger = null;
  if (!wasOpen) return;
  const selector = trigger?.selector || "";
  const label = trigger?.label || "";
  queueMicrotask(() => {
    const candidate = trigger?.node?.isConnected
      ? trigger.node
      : Array.from(document.querySelectorAll(selector || "[data-action='review-live']")).find(node => !label || node.textContent.trim() === label);
    candidate?.focus?.({ preventScroll: true });
  });
}
function reviewFormCurrent(form, version) {
  const dialog = form?.closest("#review-dialog");
  return Boolean(form?.isConnected && dialog?.open && version === appState.actionVersion);
}
function setReviewStage(form, stage) {
  const authorizationStage = stage === "authorization";
  const authorizationFields = form.querySelector("[data-review-authorization-fields]");
  const finalFields = form.querySelector("[data-review-final-fields]");
  if (authorizationFields) authorizationFields.hidden = !authorizationStage;
  if (finalFields) finalFields.hidden = authorizationStage;
  const authorizationInput = form.querySelector("#review-authorization-confirmation");
  const finalInput = form.querySelector("#review-confirmation");
  if (authorizationInput) {
    authorizationInput.disabled = !authorizationStage;
    authorizationInput.required = authorizationStage;
    if (!authorizationStage) authorizationInput.value = "";
  }
  if (finalInput) {
    finalInput.disabled = authorizationStage;
    finalInput.required = !authorizationStage;
    if (authorizationStage) finalInput.value = "";
  }
  const adverse = form.querySelector("#review-adverse");
  const adverseRequired = authorizationStage && form.dataset.adverseRequired === "true";
  const adverseContainer = form.querySelector("[data-review-adverse]");
  if (adverseContainer) adverseContainer.hidden = !adverseRequired;
  if (adverse) {
    adverse.disabled = !adverseRequired;
    adverse.required = adverseRequired;
    if (!adverseRequired) adverse.checked = false;
  }
  form.dataset.stage = stage;
}
function ensureReviewAdverseField(form, required) {
  const container = form.querySelector("[data-review-adverse]");
  if (!container || !required || container.querySelector("#review-adverse")) return;
  container.innerHTML = '<label class="checkbox-field"><input id="review-adverse" type="checkbox" required><span>I acknowledge the adverse evidence and limits shown above.</span></label>';
}
function setReviewFinalStage(form) {
  setReviewStage(form, "final");
  form.querySelector("#review-stage-label").textContent = "Final confirmation · Step 2";
  form.querySelector("#review-stage-copy").textContent = "Final confirmation grants the current exploratory live authorization and arms its reviewed worker. It may wait on fresh gates and submits no order itself.";
  form.querySelector("#review-stage-notice-title").textContent = "Final confirmation can activate the reviewed authorization.";
  form.querySelector("#review-stage-notice-copy").textContent = "It grants the current authorization, arms the worker, may wait on readiness, and does not guarantee an order. Stop prevents new entries and new exits; it does not liquidate positions or guarantee a loss limit. Expiry and spend limits remain binding.";
  const result = form.querySelector("#review-result");
  const finalFields = form.querySelector("[data-review-final-fields]");
  if (result && finalFields && result.parentElement !== finalFields) finalFields.prepend(result);
}
function markReviewCommitted(form) {
  form.querySelectorAll("[data-review-cancel]").forEach(button => { button.textContent = "Close"; button.setAttribute("aria-label", "Close"); });
}
function reviewDialog() {
  const existing = document.querySelector("#review-dialog");
  if (existing?.open) return;
  existing?.remove();
  const source = reviewProjection();
  const { adverseRequired, missing } = reviewValues(source);
  const dialog = document.createElement("dialog");
  dialog.id = "review-dialog";
  dialog.innerHTML = `<form method="dialog" class="dialog-card" id="review-form" data-stage="authorization"><div class="section-heading"><div><p class="eyebrow" id="review-stage-label">Readable review · Step 1</p><h2>Exploratory live authorization</h2><p class="muted" id="review-stage-copy">This first step records that the exact current terms were reviewed. It does not activate anything.</p></div><button class="button button-quiet" type="button" data-review-cancel>Cancel</button></div><p class="notice notice-warn" id="review-stage-notice"><strong id="review-stage-notice-title">No activation at this step.</strong><span id="review-stage-notice-copy">Read the purpose, scope, budget, duration, and stop rules before continuing.</span></p>${missing.length ? `<p class="notice notice-warn"><strong>Review blocked until terms are complete.</strong><span>Missing: ${esc(missing.join(", "))}.</span></p>` : ""}<dl class="detail-grid">${reviewRows(source) || kv("Current terms", "Not available")}</dl><div data-review-adverse>${adverseRequired ? `<label class="checkbox-field"><input id="review-adverse" type="checkbox" required><span>I acknowledge the adverse evidence and limits shown above.</span></label>` : ""}</div><div data-review-authorization-fields><label class="field"><span>Type REVIEW EXPLORATORY AUTHORIZATION</span><input id="review-authorization-confirmation" autocomplete="off" aria-label="Type REVIEW EXPLORATORY AUTHORIZATION" required></label><div id="review-result" role="status"></div><div class="actions"><button class="button button-primary" type="submit" data-review-submit>Start authorization review</button><button class="button button-quiet" type="button" data-review-cancel>Cancel</button></div></div><div data-review-final-fields hidden><label class="field"><span>Type CONFIRM EXPLORATORY LIVE</span><input id="review-confirmation" autocomplete="off" aria-label="Type CONFIRM EXPLORATORY LIVE"></label><div class="actions"><button class="button button-primary" type="submit" data-review-submit>Confirm Exploratory Live</button><button class="button button-quiet" type="button" data-review-cancel>Cancel</button></div></div></form>`;
  document.body.appendChild(dialog);
  dialog.dataset.reviewFingerprint = reviewFingerprint(source);
  const form = dialog.querySelector("#review-form");
  const finalPending = storedPendingAction();
  const reviewPending = storedReviewAction();
  if (form) {
    form.dataset.adverseRequired = String(adverseRequired);
    setReviewStage(form, "authorization");
    if (reviewPending) {
      form.dataset.reviewedFingerprint = "";
      markReviewCommitted(form);
    } else if (finalPending) {
      form.dataset.reviewedFingerprint = reviewFingerprint(source);
      setReviewFinalStage(form);
      markReviewCommitted(form);
    }
  }
  form?.addEventListener("submit", event => {
    event.preventDefault();
    if (event.submitter?.matches("[data-review-cancel]")) closeReviewDialog();
    else if (form.dataset.stage === "final") finalReview(form);
    else reviewAuthorization(form);
  });
  dialog.addEventListener("click", event => { if (event.target.closest("[data-review-cancel]")) { event.preventDefault(); closeReviewDialog(); } });
  dialog.addEventListener("cancel", event => { event.preventDefault(); closeReviewDialog(); });
  dialog.showModal();
  if (reviewPending && form) {
    showPendingOutcome(form, reviewPending, "A prior authorization review has no terminal outcome yet. Check its read-only status before starting another review.");
    form.querySelector("#review-result button")?.focus({ preventScroll: true });
  } else if (finalPending && form) {
    showPendingOutcome(form, finalPending, "A prior final confirmation has no terminal outcome yet. Check its read-only status before starting another review.");
    form.querySelector("#review-result button")?.focus({ preventScroll: true });
  } else {
    dialog.querySelector("#review-authorization-confirmation")?.focus({ preventScroll: true });
  }
}
function boundedActionIds(value) {
  return [...new Set((Array.isArray(value) ? value : []).filter(id => typeof id === "string" && id.length > 0 && id.length <= 256).slice(0, 64))];
}
function normalizedActionRecord(value, actionName) {
  const record = value && typeof value === "object" ? value : {};
  const startedAt = Number(record.started_at);
  return {
    action: typeof record.action === "string" && record.action ? record.action : actionName,
    started_at: Number.isFinite(startedAt) && startedAt >= 0 ? startedAt : 0,
    action_ids: boundedActionIds(record.action_ids),
    action_id: typeof record.action_id === "string" ? record.action_id : "",
  };
}
function mergedActionBaseline(actionName, baseline = null) {
  const persisted = normalizedActionRecord(storedActionBaseline(actionName), actionName);
  const current = normalizedActionRecord(baseline, actionName);
  return {
    action: actionName,
    started_at: current.started_at || persisted.started_at || 0,
    action_ids: boundedActionIds([...persisted.action_ids, ...current.action_ids]),
  };
}
export function resolveActionOutcome(rows, pending = {}, baseline = null) {
  const pendingRecord = normalizedActionRecord(pending, pending?.action || baseline?.action || "");
  const baselineRecord = normalizedActionRecord(baseline, pendingRecord.action);
  const actionName = pendingRecord.action;
  if (!actionName || !Array.isArray(rows)) return null;
  const actionIds = boundedActionIds([...pendingRecord.action_ids, ...baselineRecord.action_ids]);
  const startedAt = pendingRecord.started_at || baselineRecord.started_at || 0;
  const exactId = pendingRecord.action_id;
  if (exactId) {
    const exact = rows.find(row => row && row.action === actionName && String(row.action_id || "") === exactId);
    return terminalAction(exact) ? exact : null;
  }
  const candidates = rows.filter(row => {
    if (!row || row.action !== actionName || !row.action_id || actionIds.includes(String(row.action_id))) return false;
    const stamp = Date.parse(row.started_at || row.created_at || row.timestamp || row.updated_at || "");
    return Number.isFinite(stamp) && stamp >= startedAt;
  });
  return candidates.length === 1 && terminalAction(candidates[0]) ? candidates[0] : null;
}
const ACTION_BASELINE_KEY = "axiom.ui.action-baseline.v1";
function storedActionBaseline(actionName) {
  try {
    const value = JSON.parse(sessionStorage.getItem(ACTION_BASELINE_KEY) || "null");
    return value && value.action === actionName ? normalizedActionRecord(value, actionName) : null;
  } catch { return null; }
}
async function captureActionBaseline(actionName) {
  const baseline = { action: actionName, started_at: Date.now(), action_ids: [] };
  try {
    const snapshot = await requestJson("/api/ui-state");
    const rows = Array.isArray(snapshot.actions) ? snapshot.actions : Array.isArray(snapshot.operator?.actions) ? snapshot.operator.actions : [];
    baseline.action_ids = boundedActionIds(rows.filter(row => row && row.action === actionName).map(row => row.action_id).filter(Boolean));
  } catch {}
  try { sessionStorage.setItem(ACTION_BASELINE_KEY, JSON.stringify(baseline)); } catch {}
  return baseline;
}
const PENDING_ACTION_KEY = "axiom.ui.pending-action.v1";
function storedPendingAction() {
  try {
    const value = JSON.parse(sessionStorage.getItem(PENDING_ACTION_KEY) || "null");
    const normalized = normalizedActionRecord(value, "exploratory.live.review_confirm");
    return normalized.action === "exploratory.live.review_confirm" && value && typeof value === "object" ? normalized : null;
  } catch { return null; }
}
function savePendingAction(actionId, baseline) {
  try {
    const normalized = normalizedActionRecord(baseline, "exploratory.live.review_confirm");
    sessionStorage.setItem(PENDING_ACTION_KEY, JSON.stringify({ action: "exploratory.live.review_confirm", action_id: actionId ? String(actionId) : "", started_at: normalized.started_at || Date.now(), action_ids: boundedActionIds(normalized.action_ids) }));
  } catch {}
}
function clearPendingAction() {
  try { sessionStorage.removeItem(PENDING_ACTION_KEY); } catch {}
}
const REVIEW_PENDING_ACTION_KEY = "axiom.ui.pending-review-action.v1";
function storedReviewAction() {
  try {
    const value = JSON.parse(sessionStorage.getItem(REVIEW_PENDING_ACTION_KEY) || "null");
    const normalized = normalizedActionRecord(value, "execution_authorization.review");
    return normalized.action === "execution_authorization.review" && value && typeof value === "object" ? normalized : null;
  } catch { return null; }
}
function savePendingReview(actionId, baseline) {
  try {
    const normalized = normalizedActionRecord(baseline, "execution_authorization.review");
    sessionStorage.setItem(REVIEW_PENDING_ACTION_KEY, JSON.stringify({ action: "execution_authorization.review", action_id: actionId ? String(actionId) : "", started_at: normalized.started_at || Date.now(), action_ids: boundedActionIds(normalized.action_ids) }));
  } catch {}
}
function clearPendingReview() {
  try { sessionStorage.removeItem(REVIEW_PENDING_ACTION_KEY); } catch {}
}
function sameActionCorrelation(left, right) {
  const a = normalizedActionRecord(left, left?.action || right?.action || "");
  const b = normalizedActionRecord(right, right?.action || left?.action || "");
  if (!a.action || a.action !== b.action) return false;
  if (a.action_id && b.action_id && a.action_id !== b.action_id) return false;
  if (a.started_at && b.started_at && a.started_at !== b.started_at) return false;
  const aIds = boundedActionIds(a.action_ids);
  const bIds = boundedActionIds(b.action_ids);
  return aIds.length === bIds.length && aIds.every((id, index) => id === bIds[index]);
}
function savePendingActionIfSafe(actionId, baseline) {
  const candidate = normalizedActionRecord({ ...baseline, action: "exploratory.live.review_confirm", action_id: actionId ? String(actionId) : "" }, "exploratory.live.review_confirm");
  const current = storedPendingAction();
  if (current && !sameActionCorrelation(current, candidate)) return false;
  savePendingAction(actionId, baseline);
  return true;
}
function clearPendingActionIf(pending) {
  const current = storedPendingAction();
  if (!current || !sameActionCorrelation(current, pending)) return false;
  clearPendingAction();
  return true;
}
function savePendingReviewIfSafe(actionId, baseline) {
  const candidate = normalizedActionRecord({ ...baseline, action: "execution_authorization.review", action_id: actionId ? String(actionId) : "" }, "execution_authorization.review");
  const current = storedReviewAction();
  if (current && !sameActionCorrelation(current, candidate)) return false;
  savePendingReview(actionId, baseline);
  return true;
}
function clearPendingReviewIf(pending) {
  const current = storedReviewAction();
  if (!current || !sameActionCorrelation(current, pending)) return false;
  clearPendingReview();
  return true;
}
function terminalAction(status) {
  return ["COMPLETE", "FAILED"].includes(upper(status?.status));
}
function actionResponseStatus(value) {
  const source = value && typeof value === "object" ? value : {};
  return upper(source.status || source.action_status || source.result?.status || source.result?.action_status || source.action?.status || source.action?.action_status);
}
async function readFreshReviewTerms(form, result) {
  let fresh;
  try {
    const [operator, canary] = await Promise.all([requestJson("/api/ui-state"), requestJson("/api/v2/canary")]);
    fresh = reviewProjection({ data: { operator, canary } });
  } catch {
    result.textContent = "Authorization review completed, but current terms could not be read. Check again before continuing.";
    return false;
  }
  const detailGrid = form.querySelector("dl.detail-grid");
  if (detailGrid) {
    detailGrid.innerHTML = reviewRows(fresh) || kv("Current terms", "Not available");
    detailGrid.scrollTop = 0;
  }
  const dialogNode = form.closest("dialog");
  if (dialogNode) dialogNode.scrollTop = 0;
  const { missing } = reviewValues(fresh);
  if (missing.length) {
    form.dataset.reviewedFingerprint = "";
    result.textContent = `Authorization review completed, but current terms are incomplete: missing ${missing.join(", ")}.`;
    return false;
  }
  form.dataset.reviewedFingerprint = reviewFingerprint(fresh);
  setReviewFinalStage(form);
  result.textContent = "Current terms are in DRAFT. Read them again, then enter the final phrase to activate.";
  form.querySelector("#review-confirmation")?.focus({ preventScroll: true });
  return true;
}
function showPendingOutcome(form, pending, message, version = appState.actionVersion) {
  const authorizationPending = pending.action === "execution_authorization.review";
  const current = () => reviewFormCurrent(form, version);
  if (!current()) return;
  const result = form.querySelector("#review-result");
  const button = authorizationPending
    ? form.querySelector("[data-review-authorization-fields] [data-review-submit]")
    : form.querySelector("[data-review-final-fields] [data-review-submit]");
  const confirmation = authorizationPending ? form.querySelector("#review-authorization-confirmation") : form.querySelector("#review-confirmation");
  if (button) { button.disabled = true; button.textContent = "Outcome unresolved"; }
  if (confirmation) { confirmation.value = ""; confirmation.disabled = true; }
  if (authorizationPending) {
    const adverse = form.querySelector("#review-adverse");
    if (adverse) { adverse.checked = false; adverse.disabled = true; }
  }
  if (!result) return;
  result.replaceChildren(document.createTextNode(message));
  const check = document.createElement("button");
  check.type = "button";
  check.className = "button button-quiet";
  check.textContent = "Check read-only outcome";
  check.addEventListener("click", async () => {
    check.disabled = true;
    check.textContent = "Checking…";
    const status = await pollActionStatus(pending.action_id || null, pending.action, pending, 1);
    if (!status) {
      if (current()) {
        check.disabled = false;
        check.textContent = "Check read-only outcome";
      }
      return;
    }
    if (authorizationPending) {
      if (upper(status.status) === "FAILED") {
        clearPendingReviewIf(pending);
        if (!current()) return;
        if (confirmation) { confirmation.value = ""; confirmation.disabled = false; }
        const adverse = form.querySelector("#review-adverse");
        if (adverse) { adverse.checked = false; adverse.disabled = false; }
        if (button) { button.disabled = false; button.textContent = "Start authorization review"; }
        result.textContent = `Authorization review outcome: ${status.reason || status.status}. Enter the phrase again for an explicit fresh retry.`;
        return;
      }
      if (!current()) {
        clearPendingReviewIf(pending);
        return;
      }
      const refreshed = await readFreshReviewTerms(form, result);
      if (!current()) {
        clearPendingReviewIf(pending);
        return;
      }
      if (!refreshed) {
        check.disabled = false;
        check.textContent = "Check read-only outcome";
        result.append(document.createTextNode(" "), check);
        return;
      }
      clearPendingReviewIf(pending);
      return;
    }
    if (!current()) {
      clearPendingActionIf(pending);
      return;
    }
    clearPendingActionIf(pending);
    if (upper(status.status) === "FAILED") {
      if (confirmation) confirmation.disabled = false;
      result.textContent = `Final review outcome: ${status.reason || status.status}. No retry was sent.`;
      if (button) { button.disabled = false; button.textContent = "Confirm Exploratory Live"; }
      return;
    }
    result.textContent = `Final authorization completed with status ${human(status.status || "COMPLETE")} at ${formatTime(status.completed_at || status.finished_at || status.updated_at || status.timestamp || "")}. Current authority is refreshing; this confirmation submitted no order.`;
    if (button) { button.disabled = true; button.textContent = "Completed"; }
    setTimeout(() => { if (current()) loadRoute({ force: true }); }, 250);
  });
  result.append(document.createTextNode(" "), check);
}
async function reviewAuthorization(form) {
  if (appState.actionPending) return;
  const result = form.querySelector("#review-result");
  const priorPending = storedReviewAction() || storedPendingAction();
  if (priorPending) {
    showPendingOutcome(form, priorPending, priorPending.action === "execution_authorization.review"
      ? "A prior authorization review has no terminal outcome yet. Check its read-only status before starting another review."
      : "A prior final confirmation has no terminal outcome yet. Check its read-only status before starting another review.");
    return;
  }
  const button = form.querySelector("[data-review-authorization-fields] [data-review-submit]");
  const confirmation = form.querySelector("#review-authorization-confirmation");
  let adverseRequired = form.dataset.adverseRequired === "true";
  const retry = (message, pending = null) => {
    if (pending) clearPendingReviewIf(pending);
    if (!reviewFormCurrent(form, version)) return;
    ensureReviewAdverseField(form, adverseRequired);
    setReviewStage(form, "authorization");
    if (confirmation) { confirmation.value = ""; confirmation.disabled = false; }
    const adverse = form.querySelector("#review-adverse");
    if (adverse) { adverse.checked = false; adverse.disabled = !adverseRequired; }
    if (button) { button.disabled = false; button.textContent = "Start authorization review"; }
    result.textContent = message;
  };
  if (button) { button.disabled = true; button.textContent = "Checking current terms…"; }
  const version = ++appState.actionVersion;
  appState.actionPending = true;
  let dispatched = false;
  let baseline = null;
  try {
    const [operator, canary] = await Promise.all([requestJson("/api/ui-state"), requestJson("/api/v2/canary")]);
    if (!reviewFormCurrent(form, version)) return;
    const fresh = reviewProjection({ data: { operator, canary } });
    const dialogNode = form.closest("dialog");
    const freshFingerprint = reviewFingerprint(fresh);
    const initialFingerprint = dialogNode?.dataset.reviewFingerprint || "";
    const freshValues = reviewValues(fresh);
    const detailGrid = form.querySelector("dl.detail-grid");
    if (detailGrid) {
      detailGrid.innerHTML = reviewRows(fresh) || kv("Current terms", "Not available");
      detailGrid.scrollTop = 0;
    }
    if (dialogNode) dialogNode.scrollTop = 0;
    if (initialFingerprint && initialFingerprint !== freshFingerprint) {
      if (dialogNode) dialogNode.dataset.reviewFingerprint = freshFingerprint;
      adverseRequired = freshValues.adverseRequired;
      form.dataset.adverseRequired = String(adverseRequired);
      if (confirmation) confirmation.value = "";
      const adverse = form.querySelector("#review-adverse");
      if (adverse) adverse.checked = false;
      ensureReviewAdverseField(form, adverseRequired);
      setReviewStage(form, "authorization");
      if (button) { button.disabled = false; button.textContent = "Start authorization review"; }
      result.textContent = "Terms changed while this review was open. Current terms were refreshed; review them again before re-entering the phrase.";
      return;
    }
    if (dialogNode) dialogNode.dataset.reviewFingerprint = freshFingerprint;
    adverseRequired = freshValues.adverseRequired;
    form.dataset.adverseRequired = String(adverseRequired);
    if (freshValues.missing.length) {
      if (confirmation) confirmation.value = "";
      const adverse = form.querySelector("#review-adverse");
      if (adverse) adverse.checked = false;
      ensureReviewAdverseField(form, adverseRequired);
      setReviewStage(form, "authorization");
      if (button) { button.disabled = false; button.textContent = "Start authorization review"; }
      result.textContent = `Review blocked: missing ${freshValues.missing.join(", ")}. Re-read the current terms; no control call was sent.`;
      return;
    }
    const phrase = confirmation?.value.trim() || "";
    if (phrase !== "REVIEW EXPLORATORY AUTHORIZATION" || (adverseRequired && !form.querySelector("#review-adverse")?.checked)) {
      if (button) { button.disabled = false; button.textContent = "Start authorization review"; }
      result.textContent = adverseRequired ? "Type the exact phrase and acknowledge the adverse evidence before continuing." : "Type the exact phrase before continuing.";
      return;
    }
    if (!reviewFormCurrent(form, version)) return;
    baseline = await captureActionBaseline("execution_authorization.review");
    if (!reviewFormCurrent(form, version)) return;
    if (button) button.textContent = "Recording review…";
    const token = await ensureControlToken();
    if (!reviewFormCurrent(form, version)) return;
    if (!savePendingReviewIfSafe("", baseline)) return;
    dispatched = true;
    markReviewCommitted(form);
    const { response, body } = await controlFetch("/api/control", { method: "POST", headers: { "Content-Type": "application/json", "X-Axiom-Control-Token": token }, body: JSON.stringify({ action: "execution_authorization.review", confirm: "REVIEW EXPLORATORY AUTHORIZATION", payload: { values: freshValues.values } }), cache: "no-store" });
    const actionId = body.action_id || body.action?.action_id || body.result?.action_id || "";
    const status = actionResponseStatus(body);
    const pendingRecord = { action: "execution_authorization.review", action_id: actionId, started_at: baseline.started_at, action_ids: baseline.action_ids };
    if (version !== appState.actionVersion) {
      if (savePendingReviewIfSafe(actionId, baseline) && ["COMPLETE", "FAILED"].includes(status)) clearPendingReviewIf(pendingRecord);
      return;
    }
    if (status === "FAILED") {
      retry(`Authorization review outcome: ${body.reason || body.error || status}. Enter the phrase again for an explicit fresh retry.`, pendingRecord);
      return;
    }
    if (response.ok && body.ok === true && status === "COMPLETE") {
      if (!savePendingReviewIfSafe(actionId, baseline)) return;
      const refreshedComplete = await readFreshReviewTerms(form, result);
      if (!reviewFormCurrent(form, version)) {
        clearPendingReviewIf(pendingRecord);
        return;
      }
      if (refreshedComplete) clearPendingReviewIf(pendingRecord);
      else showPendingOutcome(form, pendingRecord, "Authorization review completed, but current terms are not yet readable. Check read-only status again before continuing.", version);
      return;
    }
    if (!response.ok && !actionId && !status && Number(response.status) < 500) {
      retry(`Review blocked: ${body.reason || body.error || "CONTROL_FAILED"}. No control action was recorded.`, pendingRecord);
      return;
    }
    if (!savePendingReviewIfSafe(actionId, baseline)) return;
    if (reviewFormCurrent(form, version)) {
      result.textContent = "Pending. The durable authorization review was recorded; checking its read-only status.";
    }
    const terminal = await pollActionStatus(actionId, "execution_authorization.review", baseline);
    if (!terminal) {
      showPendingOutcome(form, pendingRecord, "The authorization review outcome is still unresolved. Do not submit again.", version);
      return;
    }
    if (upper(terminal.status) === "FAILED") {
      retry(`Authorization review outcome: ${terminal.reason || terminal.status}. Enter the phrase again for an explicit fresh retry.`, pendingRecord);
      return;
    }
    if (!reviewFormCurrent(form, version)) {
      clearPendingReviewIf(pendingRecord);
      return;
    }
    const refreshed = await readFreshReviewTerms(form, result);
    if (!reviewFormCurrent(form, version)) {
      clearPendingReviewIf(pendingRecord);
      return;
    }
    if (refreshed) clearPendingReviewIf(pendingRecord);
    else showPendingOutcome(form, pendingRecord, "Authorization review completed, but current terms are not yet readable. Check read-only status again before continuing.", version);
  } catch (error) {
    if (!dispatched) {
      retry(`Review unavailable: ${error?.message || "current terms unavailable"}. No control action was sent.`);
      return;
    }
    const pendingFallback = { action: "execution_authorization.review", action_id: "", started_at: baseline?.started_at || Date.now(), action_ids: baseline?.action_ids || [] };
    const stored = storedReviewAction();
    const pendingAfterError = stored && sameActionCorrelation(stored, pendingFallback) ? stored : pendingFallback;
    if (!savePendingReviewIfSafe(pendingAfterError.action_id || "", pendingAfterError)) return;
    const terminal = await pollActionStatus(pendingAfterError.action_id || null, pendingAfterError.action, pendingAfterError, 1);
    if (!terminal) {
      showPendingOutcome(form, pendingAfterError, "The authorization review outcome is still unresolved. Do not submit again.", version);
      return;
    }
    if (upper(terminal.status) === "FAILED") {
      retry(`Authorization review outcome: ${terminal.reason || terminal.status}. Enter the phrase again for an explicit fresh retry.`, pendingAfterError);
      return;
    }
    if (!reviewFormCurrent(form, version)) {
      clearPendingReviewIf(pendingAfterError);
      return;
    }
    const refreshedAfterError = await readFreshReviewTerms(form, result);
    if (!reviewFormCurrent(form, version)) {
      clearPendingReviewIf(pendingAfterError);
      return;
    }
    if (refreshedAfterError) clearPendingReviewIf(pendingAfterError);
    else showPendingOutcome(form, pendingAfterError, "Authorization review completed, but current terms are not yet readable. Check read-only status again before continuing.", version);
  } finally {
    if (version === appState.actionVersion) appState.actionPending = false;
  }
}
async function pollActionStatus(actionId, actionName, baseline = null, attempts = 2) {
  const known = mergedActionBaseline(actionName, baseline);
  for (let attempt = 0; attempt < attempts; attempt++) {
    try {
      const snapshot = await requestJson("/api/ui-state");
      const rows = Array.isArray(snapshot.actions) ? snapshot.actions : Array.isArray(snapshot.operator?.actions) ? snapshot.operator.actions : [];
      const match = resolveActionOutcome(rows, { action: actionName, action_id: actionId || "", started_at: known.started_at, action_ids: known.action_ids });
      if (match) return match;
    } catch {}
    if (attempt + 1 < attempts) await new Promise(resolve => setTimeout(resolve, 600));
  }
  return null;
}
async function finalReview(form) {
  if (appState.actionPending) return;
  const finalFields = form.querySelector("[data-review-final-fields]");
  const reviewResult = form.querySelector("#review-result");
  if (reviewResult && finalFields && reviewResult.parentElement !== finalFields) finalFields.prepend(reviewResult);
  const confirmation = form.querySelector("#review-confirmation")?.value.trim() || "";
  const result = form.querySelector("#review-result");
  const reviewPending = storedReviewAction();
  if (reviewPending) {
    showPendingOutcome(form, reviewPending, "A prior authorization review has no terminal outcome yet. Check its read-only status before continuing.");
    return;
  }
  if (confirmation !== "CONFIRM EXPLORATORY LIVE") {
    result.textContent = "Type the exact phrase before continuing.";
    return;
  }
  const initialFingerprint = form.dataset.reviewedFingerprint || "";
  const button = form.querySelector("[data-review-final-fields] [data-review-submit]") || form.querySelector("[data-review-submit]");
  const pending = storedPendingAction();
  if (pending) {
    const pendingVersion = appState.actionVersion;
    appState.actionPending = true;
    if (button) { button.disabled = true; button.textContent = "Checking prior outcome…"; }
    const status = await pollActionStatus(pending.action_id || null, pending.action, pending, 1);
    if (pendingVersion === appState.actionVersion) appState.actionPending = false;
    if (!status) { showPendingOutcome(form, pending, "The prior confirmation outcome is still unresolved. Do not submit again.", pendingVersion); return; }
    if (!reviewFormCurrent(form, pendingVersion)) {
      clearPendingActionIf(pending);
      return;
    }
    clearPendingActionIf(pending);
    if (upper(status.status) === "FAILED") {
      const finalConfirmation = form.querySelector("#review-confirmation");
      if (finalConfirmation) { finalConfirmation.value = ""; finalConfirmation.disabled = false; }
      result.textContent = `Final review outcome: ${status.reason || status.status}. No retry was sent.`;
      if (button) { button.disabled = false; button.textContent = "Confirm Exploratory Live"; }
      return;
    }
    const completedAt = status.completed_at || status.finished_at || status.updated_at || status.timestamp || "";
    result.textContent = `Final authorization completed with status ${human(status.status || "COMPLETE")} at ${formatTime(completedAt)}. Current authority is refreshing; this confirmation submitted no order.`;
    if (button) { button.disabled = true; button.textContent = "Completed"; }
    setTimeout(() => { if (reviewFormCurrent(form, pendingVersion)) loadRoute({ force: true }); }, 250);
    return;
  }
  const version = ++appState.actionVersion;
  appState.actionPending = true;
  if (button) { button.disabled = true; button.textContent = "Checking current terms…"; }
  result.textContent = "Pending. Current terms are being checked; do not submit again.";
  let baseline = null;
  try {
    const [operator, canary] = await Promise.all([requestJson("/api/ui-state"), requestJson("/api/v2/canary")]);
    if (!reviewFormCurrent(form, version)) return;
    const fresh = reviewProjection({ data: { operator, canary } });
    const detailGrid = form.querySelector("dl.detail-grid");
    if (detailGrid) {
      detailGrid.innerHTML = reviewRows(fresh) || kv("Current terms", "Not available");
      detailGrid.scrollTop = 0;
    }
    const dialogNode = form.closest("dialog");
    if (dialogNode) dialogNode.scrollTop = 0;
    if (initialFingerprint && initialFingerprint !== reviewFingerprint(fresh)) {
      const finalConfirmation = form.querySelector("#review-confirmation");
      if (finalConfirmation) finalConfirmation.value = "";
      const adverse = form.querySelector("#review-adverse");
      if (adverse) adverse.checked = false;
      const freshValues = reviewValues(fresh);
      form.dataset.adverseRequired = String(freshValues.adverseRequired);
      ensureReviewAdverseField(form, freshValues.adverseRequired);
      form.dataset.reviewedFingerprint = "";
      if (dialogNode) dialogNode.dataset.reviewFingerprint = reviewFingerprint(fresh);
      setReviewStage(form, "authorization");
      form.querySelector("#review-stage-label").textContent = "Readable review · Step 1";
      form.querySelector("#review-stage-copy").textContent = "This first step records that the exact current terms were reviewed. It does not activate anything.";
      form.querySelector("#review-stage-notice-title").textContent = "No activation at this step.";
      form.querySelector("#review-stage-notice-copy").textContent = "Read the purpose, scope, budget, duration, and stop rules before continuing.";
      const authorizationFields = form.querySelector("[data-review-authorization-fields]");
      if (result && authorizationFields && result.parentElement !== authorizationFields) authorizationFields.prepend(result);
      const authorizationButton = form.querySelector("[data-review-authorization-fields] [data-review-submit]");
      if (authorizationButton) { authorizationButton.disabled = false; authorizationButton.textContent = "Start authorization review"; }
      result.textContent = "Terms changed while this review was open. Current terms were refreshed; complete the authorization review again before final confirmation.";
      return;
    }
    const { missing } = reviewValues(fresh);
    if (missing.length) {
      form.querySelector("#review-confirmation").value = "";
      result.textContent = `Final review blocked: missing ${missing.join(", ")}. Current terms are incomplete; no control call was sent.`;
      if (button) { button.disabled = false; button.textContent = "Confirm Exploratory Live"; }
      return;
    }
    if (!reviewFormCurrent(form, version)) return;
    baseline = await captureActionBaseline("exploratory.live.review_confirm");
    if (!reviewFormCurrent(form, version)) return;
    form.querySelector("#review-confirmation").value = "";
    markReviewCommitted(form);
    const token = await ensureControlToken();
    if (!reviewFormCurrent(form, version)) return;
    if (!savePendingActionIfSafe("", baseline)) return;
    const { response, body } = await controlFetch("/api/control", { method: "POST", headers: { "Content-Type": "application/json", "X-Axiom-Control-Token": token }, body: JSON.stringify({ action: "exploratory.live.review_confirm", confirm: confirmation }), cache: "no-store" });
    if (version !== appState.actionVersion) return;
    const actionId = body.action_id || body.action?.action_id || body.result?.action_id || "";
    const pendingRecord = { action: "exploratory.live.review_confirm", action_id: actionId, started_at: baseline.started_at, action_ids: baseline.action_ids };
    const responseStatus = upper(body.status || body.action_status || body.result?.status || body.result?.action_status || body.action?.status || body.action?.action_status);
    const responseReason = upper(body.reason || body.error || body.result?.reason || body.result?.error || body.action?.reason);
    const uncertain = Boolean(actionId) || (response.ok && !body.ok) || !response.ok && Number(response.status) >= 500 || ["RUNNING", "UNKNOWN", "OUTCOME_UNCERTAIN"].includes(responseStatus) || ["RUNNING", "UNKNOWN", "OUTCOME_UNCERTAIN"].includes(responseReason);
    if (uncertain || response.ok) {
      if (!savePendingActionIfSafe(actionId, baseline)) return;
      if (reviewFormCurrent(form, version)) {
        result.textContent = "Pending. The durable action was recorded; checking its status.";
      }
      const status = await pollActionStatus(actionId, "exploratory.live.review_confirm", baseline);
      if (!status) {
        showPendingOutcome(form, pendingRecord, "The confirmation outcome is still unresolved. Do not submit again.", version);
        return;
      }
      if (!reviewFormCurrent(form, version)) {
        clearPendingActionIf(pendingRecord);
        return;
      }
      clearPendingActionIf(pendingRecord);
      if (upper(status.status) === "FAILED") {
        result.textContent = `Final review outcome: ${status.reason || status.status}. No retry was sent.`;
        if (button) { button.disabled = false; button.textContent = "Close"; }
        return;
      }
      const completedAt = status.completed_at || status.finished_at || status.updated_at || status.timestamp || "";
      result.textContent = `Final authorization completed with status ${human(status.status || "COMPLETE")} at ${formatTime(completedAt)}. Current authority is refreshing; this confirmation submitted no order.`;
      if (button) { button.disabled = true; button.textContent = "Completed"; }
      setTimeout(() => { if (reviewFormCurrent(form, version)) loadRoute({ force: true }); }, 250);
    } else {
      clearPendingActionIf(pendingRecord);
      if (!reviewFormCurrent(form, version)) return;
      result.textContent = `Review blocked: ${body.reason || body.error || "CONTROL_FAILED"}. No control action was recorded.`;
      if (button) { button.disabled = false; button.textContent = "Confirm Exploratory Live"; }
    }
  } catch {
    if (version !== appState.actionVersion) return;
    const pendingAfterError = storedPendingAction() || { action: "exploratory.live.review_confirm", action_id: "", started_at: baseline?.started_at || Date.now(), action_ids: baseline?.action_ids || [] };
    if (!savePendingActionIfSafe(pendingAfterError.action_id || "", pendingAfterError)) return;
    const status = await pollActionStatus(pendingAfterError.action_id || null, pendingAfterError.action, pendingAfterError, 1);
    if (!status) {
      showPendingOutcome(form, pendingAfterError, "The confirmation outcome is still unresolved. Do not submit again.", version);
      return;
    }
    if (!reviewFormCurrent(form, version)) {
      clearPendingActionIf(pendingAfterError);
      return;
    }
    clearPendingActionIf(pendingAfterError);
    if (upper(status.status) === "FAILED") {
      result.textContent = `Final review outcome: ${status.reason || status.status}. No retry was sent.`;
      if (button) { button.disabled = false; button.textContent = "Confirm Exploratory Live"; }
    } else {
      const completedAt = status.completed_at || status.finished_at || status.updated_at || status.timestamp || "";
      result.textContent = `Final authorization completed with status ${human(status.status || "COMPLETE")} at ${formatTime(completedAt)}. Current authority is refreshing; this confirmation submitted no order.`;
      if (button) { button.disabled = true; button.textContent = "Completed"; }
    }
  } finally {
    if (version === appState.actionVersion) appState.actionPending = false;
  }
}
async function executeControlAction(actionName, confirmation, payload, resultNode, target = "") {
  if (appState.actionPending) return;
  appState.actionPending = true;
  const baseline = await captureActionBaseline(actionName);
  const setResult = text => { if (resultNode) resultNode.textContent = text; };
  try {
    const token = await ensureControlToken();
    const request = { action: actionName, confirm: confirmation, payload };
    if (target) request.target = target;
    const { response, body } = await controlFetch("/api/control", { method: "POST", headers: { "Content-Type": "application/json", "X-Axiom-Control-Token": token }, body: JSON.stringify(request), cache: "no-store" });
    if (!response.ok || !body.ok) throw new Error(body.reason || body.error || "CONTROL_FAILED");
    const actionId = body.action_id || body.action?.action_id || body.result?.action_id;
    setResult("Pending. The durable action was recorded; checking its status.");
    const status = await pollActionStatus(actionId, actionName, baseline);
    if (!status) {
      setResult("The outcome is uncertain. Check Activity and current status; no retry was sent.");
      return;
    }
    if (upper(status.status) === "FAILED") {
      setResult(`Action outcome: ${status.reason || status.status}. No retry was sent.`);
      return;
    }
    setResult("Action completed. Refreshing authoritative status.");
  } catch (error) {
    const status = await pollActionStatus(null, actionName, baseline);
    setResult(status ? `Action outcome: ${status.reason || status.status}.` : `Action outcome uncertain: ${error.message || "check Activity"}; no retry was sent.`);
  } finally {
    appState.actionPending = false;
    loadRoute({ force: true });
  }
}
async function runControl(form) {
  const actionName = form.dataset.controlAction || "";
  const fields = {};
  let invalid = "";
  form.querySelectorAll("[data-control-field]").forEach(field => {
    const key = field.dataset.controlField;
    if (!key) return;
    if (field.dataset.controlJson === "true") {
      try { fields[key] = field.value.trim() ? JSON.parse(field.value) : {}; } catch { invalid = `${key} must be valid JSON`; }
    } else if (field.type === "checkbox") fields[key] = field.checked;
    else if (field.value !== "") fields[key] = field.value;
  });
  const result = form.querySelector(".control-result");
  if (invalid) { if (result) result.textContent = invalid; return; }
  const candidateTargetAction = CANDIDATE_TARGET_ACTIONS.has(actionName);
  const candidateTarget = candidateTargetAction ? String(fields.candidate_id || "").trim() : "";
  if (candidateTargetAction && !candidateTarget) { if (result) result.textContent = "Candidate ID is required."; return; }
  let confirmation = form.dataset.controlConfirm || "";
  if (actionName === "canary.enable_auto") confirmation = `ENABLE AUTO CANARY ${fields.venue || "POLYMARKET"} ${fields.config_id || ""} ${fields.expected_generation || ""}`.trim();
  const typed = form.querySelector("[data-control-confirmation]")?.value.trim() || "";
  if (confirmation && typed !== confirmation) { if (result) result.textContent = `Type the exact phrase: ${confirmation}`; return; }
  await executeControlAction(actionName, confirmation, candidateTargetAction ? {} : fields, result, candidateTarget);
}
function stopCanary() {
  const root = document.querySelector("#content");
  const result = document.createElement("p");
  result.className = "notice notice-info";
  result.textContent = "Stop requested. Disarm prevents new canary submissions; it does not claim to close or cancel every position.";
  root?.prepend(result);
  executeControlAction("canary.disarm", "DISARM", {}, result);
}
async function saveRisk(form) {
  if (appState.actionPending) return;
  const values = {};
  form.querySelectorAll("[data-risk-field]").forEach(field => {
    const raw = field.type === "checkbox" ? field.checked : field.value;
    const optional = field.dataset.riskOptional === "true";
    const value = optional && raw === "" ? null : raw === "custom" && field.dataset.riskCustomValue !== "" ? field.dataset.riskCustomValue : raw;
    if (optional || field.type === "checkbox" || value !== "") values[field.dataset.riskField] = value;
  });
  if (!Object.keys(values).length) return;
  appState.actionPending = true;
  appState.riskSaveNotice = null;
  let refreshAfterSave = false;
  const result = document.createElement("p");
  result.className = "notice notice-info";
  result.textContent = "Saving a review draft; active settings remain authoritative.";
  form.appendChild(result);
  try {
    const token = await ensureControlToken();
    const { response, body } = await controlFetch("/api/control", { method: "POST", headers: { "Content-Type": "application/json", "X-Axiom-Control-Token": token }, body: JSON.stringify({ action: "risk.settings.save_draft", confirm: "SAVE RISK SETTINGS DRAFT", payload: { values } }), cache: "no-store" });
    refreshAfterSave = response.ok && body.ok;
    const nativeDetail = body?.detail ?? body?.message ?? body?.validation_error ?? body?.validation_message ?? body?.result?.detail ?? body?.result?.message ?? body?.result?.validation_error ?? body?.result?.validation_message ?? body?.result?.error;
    const nativeCode = body?.reason ?? body?.error ?? body?.result?.reason ?? body?.result?.code ?? body?.result?.error;
    const detailText = typeof nativeDetail === "string" && nativeDetail.trim()
      ? nativeDetail
      : nativeDetail && typeof nativeDetail === "object"
        ? String(nativeDetail.human_message || nativeDetail.validation_message || nativeDetail.message || nativeDetail.detail || nativeDetail.description || nativeDetail.reason || nativeDetail.error || JSON.stringify(nativeDetail))
        : Array.isArray(body?.errors || body?.result?.errors)
          ? (body.errors || body.result.errors).map(item => typeof item === "string" ? item : item?.message || item?.detail || item?.description || item?.reason).filter(Boolean).join("; ")
          : "";
    const errorText = detailText && nativeCode && String(nativeCode) !== detailText ? `${detailText} (${nativeCode})` : detailText || nativeCode || "CONTROL_FAILED";
    result.className = refreshAfterSave ? "notice notice-good" : "notice notice-warn";
    result.textContent = refreshAfterSave ? "Draft saved for review. No setting was activated." : `Draft was not saved: ${errorText}`;
    if (!refreshAfterSave) appState.riskSaveNotice = { tone: "warn", text: result.textContent };
  } catch {
    result.className = "notice notice-warn";
    result.textContent = "Draft outcome uncertain. Check Settings and Activity; no retry was sent.";
    appState.riskSaveNotice = { tone: "warn", text: result.textContent };
  } finally {
    appState.actionPending = false;
    if (refreshAfterSave) loadRoute({ force: true });
  }
}
async function refreshChecks() {
  if (appState.actionPending) return;
  appState.actionPending = true;
  const root = document.querySelector("#content");
  const pending = document.createElement("div");
  pending.className = "notice notice-info";
  pending.innerHTML = "<strong>Refreshing checks</strong><span>Running the existing read-only connectivity preflight. No authority changes.</span>";
  root?.prepend(pending);
  try {
    const token = await ensureControlToken();
    const { response, body } = await controlFetch("/api/control", { method: "POST", headers: { "Content-Type": "application/json", "X-Axiom-Control-Token": token }, body: JSON.stringify({ action: "canary.connectivity_check", confirm: "REFRESH CHECKS", payload: {} }), cache: "no-store" });
    pending.className = response.ok && body.ok ? "notice notice-good" : "notice notice-warn";
    pending.innerHTML = `<strong>${response.ok && body.ok ? "Checks refreshed" : "Checks could not be refreshed"}</strong><span>${esc(body.reason || body.error || "Current readiness remains unknown until the next successful preflight.")}</span>`;
  } catch {
    pending.className = "notice notice-warn";
    pending.innerHTML = "<strong>Checks could not be refreshed</strong><span>The preflight outcome is unavailable. No retry or authority change was sent.</span>";
  } finally {
    appState.actionPending = false;
    loadRoute({ force: true });
  }
}

function drawerFocusables(sidebar) {
  return Array.from(sidebar?.querySelectorAll("a[href],button:not([disabled]),input:not([disabled]),select:not([disabled]),textarea:not([disabled]),[tabindex]:not([tabindex='-1'])") || []).filter(node => !node.hidden && node.getClientRects().length);
}
function syncMenuAccessibility() {
  const app = document.querySelector("#app");
  const menu = document.querySelector(".menu-toggle");
  const sidebar = document.querySelector(".sidebar");
  const main = document.querySelector(".app-main");
  const scrim = document.querySelector(".nav-scrim");
  const mobile = typeof window.matchMedia === "function" && window.matchMedia("(max-width: 860px)").matches;
  const wasOpen = Boolean(app?.classList.contains("menu-open"));
  const open = mobile && wasOpen;
  if (app && !mobile) app.classList.remove("menu-open");
  menu?.setAttribute("aria-expanded", String(open));
  if (scrim) scrim.hidden = !open;
  if (sidebar) sidebar.inert = mobile && !open;
  if (main) main.inert = mobile && open;
  if (!mobile && wasOpen) menu?.focus({ preventScroll: true });
}
function toggleMenu(open) {
  const app = document.querySelector("#app");
  const menu = document.querySelector(".menu-toggle");
  const sidebar = document.querySelector(".sidebar");
  const main = document.querySelector(".app-main");
  const scrim = document.querySelector(".nav-scrim");
  const mobile = typeof window.matchMedia === "function" && window.matchMedia("(max-width: 860px)").matches;
  const wasOpen = Boolean(app?.classList.contains("menu-open"));
  const next = mobile && Boolean(open ?? !wasOpen);
  app?.classList.toggle("menu-open", next);
  menu?.setAttribute("aria-expanded", String(next));
  if (scrim) scrim.hidden = !next;
  if (next) {
    if (sidebar) sidebar.inert = false;
    if (main) main.inert = true;
    drawerFocusables(sidebar)[0]?.focus({ preventScroll: true });
  } else {
    if (main) main.inert = false;
    if (sidebar) sidebar.inert = mobile;
    if ((wasOpen || document.activeElement?.closest(".sidebar")) && mobile) {
      const restoreMenuFocus = () => menu?.focus({ preventScroll: true });
      restoreMenuFocus();
      queueMicrotask(restoreMenuFocus);
    }
  }
}
const BINANCE_GENERIC_ACTIONS = new Set(["CONNECTIVITY_CHECK", "ORDER_VALIDATION_TEST", "ENABLE", "PAUSE", "RESUME", "DISARM", "KILL"]);
const BINANCE_TESTNET_ACTIONS = new Set(["CONNECTIVITY_CHECK", "ORDER_VALIDATION_TEST", "PAUSE", "DISARM", "KILL"]);
function binanceSource() {
  const source = appState.data.page || appState.data.binance || {};
  return source && typeof source === "object" ? source : {};
}
function binanceStrictMode(source = binanceSource()) {
  const status = source.status && typeof source.status === "object" ? source.status : {};
  return source.strict_testnet === true || status.strict_testnet === true;
}
function binanceResultNode(button) {
  return button?.closest("[data-binance-scope]")?.querySelector("[data-binance-result]") || document.querySelector("[data-binance-result]") || document.querySelector("#binance-action-result");
}
function binanceField(button, name) {
  const scope = button?.closest("[data-binance-scope]") || button?.closest("section,article,form");
  const field = scope?.querySelector(`[data-binance-field="${name}"]`) || document.querySelector(`[data-binance-field="${name}"]`) || document.querySelector(`#binance-order-${name}`);
  return String(field?.value ?? button?.dataset?.[`binance${name[0].toUpperCase()}${name.slice(1)}`] ?? "");
}
function binanceEnablePhrase(source = binanceSource()) {
  const status = source.status && typeof source.status === "object" ? source.status : {};
  return String(source.enable_phrase || status.enable_phrase || "ENABLE BINANCE AUTO CANARY");
}
function binanceActionDialog(action, phrase = "") {
  return new Promise(resolve => {
    const dialog = document.createElement("dialog");
    dialog.className = "binance-control-dialog";
    const label = human(action.replaceAll("_", " "));
    const phraseMarkup = phrase ? `<label class="field"><span>Type ${esc(phrase)}</span><input id="binance-control-confirmation" autocomplete="off" aria-label="Type ${esc(phrase)}"></label>` : "";
    dialog.innerHTML = `<form method="dialog" class="dialog-card"><div class="section-heading"><div><p class="eyebrow">Binance control</p><h2>${esc(label)}</h2><p class="muted">This sends one guarded action to the isolated Binance control plane. No automatic retry will be sent.</p></div><button class="button button-quiet" type="button" data-binance-cancel>Cancel</button></div>${phraseMarkup}<p class="field-error" data-binance-dialog-error role="alert"></p><div class="actions"><button class="button button-primary" type="submit" data-binance-confirm>Confirm ${esc(label)}</button><button class="button button-quiet" type="button" data-binance-cancel>Cancel</button></div></form>`;
    document.body.appendChild(dialog);
    const form = dialog.querySelector("form");
    const finish = value => { dialog.close(); dialog.remove(); resolve(value); };
    form?.addEventListener("submit", event => {
      event.preventDefault();
      const typed = form.querySelector("#binance-control-confirmation")?.value.trim() || "";
      if (phrase && typed !== phrase) { const error = form.querySelector("[data-binance-dialog-error]"); if (error) error.textContent = "Type the exact phrase to continue."; form.querySelector("#binance-control-confirmation")?.focus(); return; }
      finish(true);
    });
    dialog.querySelectorAll("[data-binance-cancel]").forEach(node => node.addEventListener("click", () => finish(false)));
    dialog.addEventListener("cancel", event => { event.preventDefault(); finish(false); });
    dialog.showModal();
    (form.querySelector("#binance-control-confirmation") || form.querySelector("[data-binance-confirm]"))?.focus();
  });
}
async function runBinanceControl(button) {
  if (!button || appState.actionPending) return;
  const action = String(button.dataset.binanceAction || "").trim().toUpperCase();
  const source = binanceSource();
  const strict = binanceStrictMode(source);
  const allowed = strict ? BINANCE_TESTNET_ACTIONS : BINANCE_GENERIC_ACTIONS;
  const result = binanceResultNode(button);
  const label = human(action.replaceAll("_", " "));
  if (!allowed.has(action)) { if (result) result.textContent = `${label || "Binance action"} is not available in this runtime profile.`; return; }
  const phrase = !strict && (action === "ENABLE" || action === "RESUME") ? binanceEnablePhrase(source) : "";
  if (["ENABLE", "RESUME", "PAUSE", "DISARM", "KILL"].includes(action)) {
    if (!await binanceActionDialog(action, phrase)) return;
  }
  const payload = strict ? {} : action === "ORDER_VALIDATION_TEST" ? { symbol: binanceField(button, "symbol"), price: binanceField(button, "price"), quantity: binanceField(button, "quantity") } : action === "ENABLE" || action === "RESUME" ? { confirmation: phrase } : {};
  if (result) result.textContent = `${label} pending. The native outcome is being recorded; do not submit again.`;
  button.disabled = true;
  button.setAttribute("aria-busy", "true");
  appState.actionPending = true;
  try {
    const token = await ensureControlToken();
    const { response, body } = await controlFetch("/api/binance/control", { method: "POST", headers: { "Content-Type": "application/json", "X-Axiom-Control-Token": token }, body: JSON.stringify({ action, payload }), cache: "no-store" });
    const ok = response.ok && body.ok === true;
    const stamp = body.completed_at?.pht || body.completed_at?.utc || body.timestamp_pht || body.timestamp_utc || body.timestamp || "";
    const status = body.status || body.result?.status || body.result?.control?.status || (ok ? "COMPLETE" : "");
    if (result) result.textContent = ok ? `${label} completed · ${human(status || "COMPLETE")} · ${formatTime(stamp)}` : `${label} blocked: ${human(body.reason || body.error || "CONTROL_FAILED")}. No retry was sent.`;
    if (ok) setTimeout(() => loadRoute({ force: true }), 1200);
  } catch (error) {
    if (result) result.textContent = `${label} unavailable: ${error?.message || "network failure"}. No retry was sent.`;
  } finally {
    appState.actionPending = false;
    button.disabled = false;
    button.removeAttribute("aria-busy");
  }
}
function applyBrowseForm(form) {
  const patch = { ...currentRoute, page: 1 };
  form.querySelectorAll("[data-browse-control]").forEach(field => {
    if (field.name) patch[field.name] = field.value;
  });
  writeRoute(parseRoute(hrefForRoute(patch).slice(1)));
  loadRoute({ force: true });
}
function bindEvents() {
  const shell = document.querySelector("#app");
  if (!shell || shell.dataset.bound) return;
  shell.dataset.bound = "1";
  document.querySelector(".nav-scrim")?.addEventListener("click", () => toggleMenu(false));
  toggleMenu(false);
  window.addEventListener("resize", syncMenuAccessibility);
  shell.addEventListener("keydown", event => {
    if (event.key !== "Tab" || !event.target.closest(".sidebar") || !document.querySelector("#app")?.classList.contains("menu-open")) return;
    const focusables = drawerFocusables(document.querySelector(".sidebar"));
    if (!focusables.length) return;
    const first = focusables[0];
    const last = focusables[focusables.length - 1];
    if (event.shiftKey && document.activeElement === first) {
      event.preventDefault();
      last.focus({ preventScroll: true });
    } else if (!event.shiftKey && document.activeElement === last) {
      event.preventDefault();
      first.focus({ preventScroll: true });
    }
  });
  shell.addEventListener("click", event => {
    const nav = event.target.closest("[data-nav-view]");
    if (nav) { event.preventDefault(); toggleMenu(false); writeRoute(parseRoute(nav.getAttribute("href")?.split("?")[1] || "")); loadRoute({ force: true }); return; }
    const routeLink = event.target.closest("a[data-route-link]");
    if (routeLink) { event.preventDefault(); toggleMenu(false); writeRoute(parseRoute(routeLink.href.split("?")[1] || "")); loadRoute({ force: true }); return; }
    const pageButton = event.target.closest("[data-page]");
    if (pageButton && !pageButton.disabled) { event.preventDefault(); writeRoute({ ...currentRoute, page: Number(pageButton.dataset.page) || 1 }); loadRoute({ force: true }); return; }
    const action = event.target.closest("[data-action]")?.dataset.action;
    if (action === "binance-control") { event.preventDefault(); runBinanceControl(event.target.closest("[data-action='binance-control']")); return; }
    if (action === "toggle-menu") { event.preventDefault(); toggleMenu(); }
    if (action === "close-menu") { event.preventDefault(); toggleMenu(false); }
    if (action === "review-live") { event.preventDefault(); const node = event.target.closest("[data-action='review-live']"); appState.reviewTrigger = { node, selector: "[data-action='review-live']", label: node?.textContent.trim() || "" }; reviewDialog(); }
    if (action === "stop-canary") { event.preventDefault(); stopCanary(); }
    if (action === "refresh") { event.preventDefault(); refreshChecks(); }
    const copy = event.target.closest("[data-copy]");
    if (copy) { event.preventDefault(); if (!navigator.clipboard?.writeText) { copy.textContent = "Copy unavailable"; return; } navigator.clipboard.writeText(copy.dataset.copy || "").then(() => { copy.dataset.copyLabel ||= copy.textContent || "Copy"; copy.textContent = "Copied"; setTimeout(() => { copy.textContent = copy.dataset.copyLabel; }, 1200); }).catch(() => { copy.textContent = "Copy unavailable"; }); }
  });
  shell.addEventListener("change", event => { if (event.target.matches("#browse-controls select")) applyBrowseForm(event.target.form); });
  shell.addEventListener("submit", event => {
    if (event.target.id === "browse-controls") { event.preventDefault(); applyBrowseForm(event.target); return; }
    if (event.target.id === "review-form") return;
    if (event.target.matches("[data-control-action]")) { event.preventDefault(); runControl(event.target); return; }
    if (event.target.id === "risk-form") { event.preventDefault(); saveRisk(event.target); }
  });
  document.addEventListener("keydown", event => { if (event.key === "Escape") { closeReviewDialog(); toggleMenu(false); } });
  window.addEventListener("popstate", () => { currentRoute = parseRoute(location.search); appState.route = currentRoute; loadRoute({ force: true }); });
}

function boot() { if (typeof document === "undefined") return; if (location.protocol === "file:") return; bindEvents(); updateNav(); loadRoute({ force: true }); }
boot();

export { ui, renderHome, renderLive, renderSettings };
