import { expect, test } from "bun:test";
import { money, parseRoute, resolveActionOutcome, sessionPresentation } from "../axiom/ui/app.js";
import { pageSpec, renderDetail, renderPage } from "../axiom/ui/pages.js";

const ui = {
  esc: (value) => String(value ?? "").replace(/[&<>"']/g, (char) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[char])),
  money: (value) => value === null || value === undefined ? "Not available" : `$${value}`,
  number: (value) => String(value ?? "Not available"),
  time: (value) => String(value ?? "Not available"),
  human: (value) => String(value ?? "Not available"),
  badge: (label, tone = "neutral") => `<span class="badge badge-${tone}">${String(label ?? "Not available")}</span>`,
  empty: (title, body) => `<div class="empty-state"><h3>${title}</h3><p>${body}</p></div>`,
  notice: (title, body) => `<div class="notice"><strong>${title}</strong><p>${body}</p></div>`,
  kv: (label, value) => `<div class="kv"><dt>${label}</dt><dd>${value}</dd></div>`,
  technical: (value) => `<details class="technical"><summary>Technical details</summary><pre>${JSON.stringify(value)}</pre></details>`,
  link: (label, patch) => {
    const query = Object.entries(patch).map(([key, value]) => `${encodeURIComponent(key)}=${encodeURIComponent(value)}`).join("&");
    return `<a data-route-link href="?${query}">${label}</a>`;
  },
  table: (columns, rows, { caption = "" } = {}) => `<table><caption>${caption}</caption><tbody>${rows.map((row) => `<tr>${columns.map((column) => `<td>${column.render(row)}</td>`).join("")}</tr>`).join("")}</tbody></table>`,
};

const mixedAuthority = {
  production_live_trading: false,
  paper_only: true,
  execution_authorization: { status: "ACTIVE", mode: "EXPLORATORY_MICRO_CANARY" },
  operator_controls: { armed: false, armed_state: "DISARMED" },
  worker: { next_decision: "NO_SIGNAL", blocker: null, tick_count: 12 },
  autonomous: { enabled: false, control_state: "DISARMED", next_decision: "NO_SIGNAL" },
};
const records = {
  fixture: true,
  execution_authorization: { status: "ACTIVE", mode: "EXPLORATORY_MICRO_CANARY" },
  operator_controls: { armed: false, armed_state: "DISARMED" },
  worker: { next_decision: "NO_SIGNAL", blocker: null, tick_count: 12 },
  autonomous: { enabled: false, control_state: "DISARMED", next_decision: "NO_SIGNAL" },
  execution: {
    canary_submission_attempts: [{ attempt_id: "fixture-submission-1", status: "ATTEMPTED", attempted_at: "2034-02-03T04:00:00Z" }],
    canary_position_requests: [{ request_id: "fixture-request-1", position_id: "fixture-position-1", status: "PARTIAL", side: "BUY", requested_quantity: "1", filled_quantity: "0.25", average_price: "0.42" }],
    canary_position_fills: [{ fill_id: "fixture-fill-1", request_id: "fixture-request-1", quantity: "0.25", price: "0.42", fee: "0.0001", status: "SETTLED" }],
    canary_position_lots: [],
    canary_risk_fills: [],
    unknown_obligations: [{ id: "fixture-unknown-1", description: "Synthetic outcome is not reconciled", amount: "0.42", reason: "OUTCOME_UNCERTAIN" }],
    event_count: 1,
    today_orders: 1,
    real_execution_events: 1,
  },
};

test("money distinguishes missing, exact zero, and tiny fee values", () => {
  expect(money(null)).toBe("Not available");
  expect(money(0)).toBe("$0.00");
  expect(money("0.0001")).toContain("0.0001");
});
test("session presentation separates legacy flag from active micro-session authority", () => {
  const now = Date.parse("2034-02-03T04:05:00Z");
  const authority = { ...mixedAuthority, connectivity: { checked_at: "2034-02-03T04:05:00Z" } };
  const withoutLegacyFlag = sessionPresentation({ operator: { ...authority, production_live_trading: false }, canary: authority, now });
  const withLegacyFlag = sessionPresentation({ operator: { ...authority, production_live_trading: true }, canary: authority, now });
  expect(withLegacyFlag.permission.value).toBe(withoutLegacyFlag.permission.value);
  expect(withLegacyFlag.primaryAction.action).toBe(withoutLegacyFlag.primaryAction.action);
  expect(withLegacyFlag.tone).toBe(withoutLegacyFlag.tone);
  const withoutActiveAuthority = sessionPresentation({ operator: { production_live_trading: false }, canary: { connectivity: authority.connectivity }, now });
  expect(withoutActiveAuthority.primaryAction.action).toBe("review-live");
  expect(withoutActiveAuthority.permission.value).not.toBe(withoutLegacyFlag.permission.value);
});

test("revoked and stale states provide distinct truthful next actions", () => {
  const revoked = sessionPresentation({ operator: {}, canary: { execution_authorization: { status: "REVOKED" } }, connected: true, receivedAt: new Date().toISOString() });
  const stale = sessionPresentation({ operator: {}, canary: { execution_authorization: { status: "ACTIVE" }, readiness: { status: "READY" } }, connected: true, receivedAt: new Date(Date.now() - 61_000).toISOString() });
  expect(revoked.primaryAction.action).toBe("review-live");
  expect(stale.primaryAction.action).toBe("refresh");
});

test("route aliases preserve destination, section, pagination, filters, and selected detail", () => {
  const route = parseRoute("?tab=datasets&page=3&page_size=10&filter=aurora&sort=updated_at&direction=asc&selected=fixture-dataset-1&expanded=1&source=forward");
  expect(route.view).toBe("research");
  expect(route.section).toBe("data");
  expect(route.page).toBe(3);
  expect(route.page_size).toBe(10);
  expect(route.direction).toBe("asc");
  expect(route.selected).toBe("fixture-dataset-1");
  expect(route.source_type).toBe("forward");
});

test("every canonical destination exposes a bounded endpoint and useful facets", () => {
  const routes = [
    { view: "portfolio", section: "real" }, { view: "portfolio", section: "practice" }, { view: "portfolio", section: "allocations" },
    { view: "markets" }, { view: "research", section: "strategies" }, { view: "research", section: "crypto" },
    { view: "research", section: "automation" }, { view: "research", section: "data" }, { view: "activity" },
    { view: "live", section: "binance" },
  ];
  for (const route of routes) {
    const spec = pageSpec(route);
    expect(spec.endpoint).toMatch(/^\/api\//);
    expect(Array.isArray(spec.facets)).toBe(true);
    expect(Array.isArray(spec.sortOptions)).toBe(true);
  }
});

test("deep links use exact native record identity and keep shadow listing independent", () => {
  const markets = pageSpec({ view: "markets" });
  expect(markets.detailRequests({ view: "markets", selected: "fixture-market-1" }).market).toBe("/api/ui-record?kind=market&id=fixture-market-1");
  const automation = pageSpec({ view: "research", section: "automation" });
  expect(automation.detailRequests({ view: "research", section: "automation" }).shadow_list).toContain("/api/v2/shadow?page=1&page_size=10");
  expect(automation.detailRequests({ view: "research", section: "automation", selected: "fixture-hermes-1", record_kind: "hermes" }).hermes).toBe("/api/v2/hermes/fixture-hermes-1");
  expect(automation.detailRequests({ view: "research", section: "automation", selected: "fixture-shadow-1", record_kind: "shadow" }).shadow).toBe("/api/v2/shadow/fixture-shadow-1");
});

test("financial detail accepts the exact native order wrapper without a route kind", () => {
  const id = "fixture-position-request-aurora";
  const html = renderDetail({
    route: { view: "portfolio", section: "real", selected: id },
    detail: { record: { kind: "order", id, record: { request_id: id, position_id: "fixture-position-aurora", status: "PARTIAL", requested_quantity: "1" } } },
    ui,
  });
  expect(html).not.toContain("Details unavailable");
  expect(html).toContain(id);
});

test("portfolio renderer keeps real execution evidence separate from practice and unknown obligations", () => {
  const real = renderPage({ route: { view: "portfolio", section: "real" }, data: records, ui });
  const practice = renderPage({ route: { view: "portfolio", section: "practice" }, data: { items: [{ name: "Synthetic observation", simulated: true, known_result: "0.00" }] }, ui });
  expect(real).toContain("Unknown obligations");
  expect(real).toContain("0.0001");
  expect(real).not.toContain("Practice only");
  expect(practice).toContain("Practice only");
  expect(practice).not.toContain("Unknown obligations");
});

test("market and research pages use human names while retaining detail links", () => {
  const markets = renderPage({ route: { view: "markets" }, data: { items: [{ market_id: "fixture-market-1", question: "Synthetic question", snapshot: { yes_mid: "0.37", yes_ask: "0.38", yes_bid: "0.36", no_mid: "0.63", no_ask: "0.64", no_bid: "0.62" } }] }, ui });
  const research = renderPage({ route: { view: "research", section: "strategies" }, data: { items: [{ candidate_id: "fixture-candidate-1", name: "Synthetic strategy", stage: "REJECTED", payload: { operational_setup: "Bounded setup", strategy_document: "Fixture strategy", canonical_strategy: "FIXTURE_V1", exit_policy: "Review" }, provenance: { source: "fixture" } }] }, ui });
  expect(markets).toContain("Synthetic question");
  expect(markets).toContain("0.38");
  expect(markets).toContain("record_kind");
  expect(research).toContain("Synthetic strategy");
  expect(research).toContain("FIXTURE_V1");
});

test("activity groups only identical nonfinancial messages and retains financial audit rows", () => {
  const html = renderPage({ route: { view: "activity" }, data: { items: [
    { event_id: "fixture-event-1", message: "Same research note", kind: "research", source: "fixture", source_type: "catalog", status: "INFO", timestamp: "2034-02-03T04:00:00Z" },
    { event_id: "fixture-event-2", message: "Same research note", kind: "research", source: "fixture", source_type: "catalog", status: "INFO", timestamp: "2034-02-03T04:01:00Z" },
    { event_id: "fixture-event-3", message: "Same order", kind: "trading", source: "fixture", source_type: "canary", status: "NOTICE", timestamp: "2034-02-03T04:02:00Z" },
    { event_id: "fixture-event-4", message: "Same order", kind: "trading", source: "fixture", source_type: "canary", status: "NOTICE", timestamp: "2034-02-03T04:03:00Z" },
  ] }, ui });
  expect(html).toContain("2 repeated");
  expect((html.match(/Same order/g) || []).length).toBe(2);
});

test("detail renderer includes exact event and missing-range evidence without leaking controls", () => {
  const html = renderDetail({ route: { view: "research", section: "data", selected: "fixture-dataset-1" }, detail: {
    dataset: { dataset_id: "fixture-dataset-1", name: "Synthetic dataset", status: "PARTIAL", coverage: "PARTIAL" },
    gaps: { items: [{ start: "fixture-range", end: "fixture-range", status: "MISSING" }] },
  }, ui });
  expect(html).toContain("Synthetic dataset");
  expect(html).toContain("Missing ranges");
  expect(html).toContain("fixture-range");
  expect(html).not.toMatch(/csrf|token|private[_-]?key/i);
});

test("lost-ID pending resolves only one new terminal outcome beyond the persisted baseline", () => {
  const startedAt = Date.parse("2034-02-03T04:05:00Z");
  const baseline = { action: "exploratory.live.review_confirm", started_at: startedAt, action_ids: ["baseline-action"] };
  const rows = [
    { action: baseline.action, action_id: "older-action", status: "COMPLETE", started_at: "2034-02-03T04:04:59Z" },
    { action: baseline.action, action_id: "baseline-action", status: "COMPLETE", started_at: "2034-02-03T04:05:10Z" },
    { action: baseline.action, action_id: "new-action", status: "COMPLETE", started_at: "2034-02-03T04:05:11Z" },
  ];
  expect(resolveActionOutcome(rows, { action: baseline.action, started_at: startedAt }, baseline)).toEqual(rows[2]);
});

test("action outcome resolver ignores nonterminal and ambiguous candidates", () => {
  const action = "risk.settings.save_draft";
  const startedAt = Date.parse("2034-02-03T04:05:00Z");
  expect(resolveActionOutcome([
    { action, action_id: "running", status: "RUNNING", started_at: "2034-02-03T04:05:01Z" },
  ], { action, started_at: startedAt })).toBeNull();
  expect(resolveActionOutcome([
    { action, action_id: "unknown", status: "UNKNOWN", started_at: "2034-02-03T04:05:01Z" },
  ], { action, started_at: startedAt })).toBeNull();
  expect(resolveActionOutcome([
    { action, action_id: "one", status: "COMPLETE", started_at: "2034-02-03T04:05:01Z" },
    { action, action_id: "two", status: "FAILED", started_at: "2034-02-03T04:05:02Z" },
  ], { action, started_at: startedAt })).toBeNull();
  const exact = { action, action_id: "two", status: "FAILED", started_at: "2034-02-03T04:05:02Z" };
  expect(resolveActionOutcome([exact], { action, action_id: exact.action_id, started_at: startedAt })).toEqual(exact);
});

test("action outcome resolver accepts only native COMPLETE and FAILED terminal statuses", () => {
  const action = "canary.disarm";
  const pending = { action, started_at: Date.parse("2034-02-03T04:05:00Z") };
  const complete = { action, action_id: "complete", status: "COMPLETE", started_at: "2034-02-03T04:05:01Z" };
  const failed = { action, action_id: "failed", status: "FAILED", started_at: "2034-02-03T04:05:02Z" };
  expect(resolveActionOutcome([complete], pending)).toEqual(complete);
  expect(resolveActionOutcome([failed], pending)).toEqual(failed);
});
