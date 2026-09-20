# Polymarket one-click canary operator guide

This guide covers the **disarmed-by-default** Polymarket canary. A one-click
run is a bounded authorization of an already-qualified signal; it is not a
promise that an order will be sent or filled. The documented isolated runtime
must keep the local control plane `DISARMED`; it cannot authenticate or
activate a canary.

## Safety boundary

Use a dedicated development database and isolated runtime identity. Never copy
credentials, keyring records, databases, lock files, or order IDs between the
PAPER/development and any real venue runtime. Do not put a private key in a
command line, source file, `.env`, browser form, log, or this document. No
public-data, research, or paper result is execution evidence. A blocked or
unconfigured gate is **not** a passed gate.

The development profile must set `AXIOM_EXECUTION_PROFILE=isolated`. Isolated
mode denies real credential-store loads, configured credential probes, and real
venue transports (including read-only account/market calls). Injected fake
venues remain available for deterministic tests. If PowerShell lifecycle
scripts are used for development diagnostics, `-Isolated` and an explicit
dedicated development `-DbPath` are required on every invocation. Production
defaults are unchanged, and this release remains `DISARMED` pending the
required post-lifecycle-change gate and deployment review.

## Normal launch: isolated operator diagnostics

The normal development and diagnostic path is the control-wired operator
surface. It starts and supervises one paper node and serves the localhost
operator dashboard on the isolated port. Substitute the dedicated development
database path for `<development-db>`:

```powershell
python -m axiom.cli operator --isolated --db <development-db> --port 8187
```

Do not pair the node lifecycle start script with the control-less `dashboard`
surface for operator **Enable**/**Disable** actions. `dashboard` is
control-less; it is not the operator surface. Isolated mode denies production
credential loads, authenticated connectivity, account/order transports, and
canary activation. Deployment remains `DISARMED`.

1. In the operator dashboard, review the currently **ACTIVE** settings, any
   **DRAFT**, active/draft config ID and SHA-256 hash, settings generation,
   canary control generation, PHT reset, every independent remaining capacity,
   unresolved intents, and the candidate's current forward evidence.
2. Review the expected price, fee reserve, slippage, and original
   strategy/version metadata for any held lot.
3. The production `canary.connectivity_check` action derives the current
   selected member's persisted market/token binding through
   `OperatorControlPlane._selected_market_readiness` and passes those server
   values to `CanaryService.connectivity_check`; it never trusts caller-supplied
   market or token IDs. Without a selected binding it performs only the
   account-level connectivity check.
4. Do not click **Enable** or attempt activation from this isolated diagnostic
   surface. It cannot authenticate, reach account/order paths, or authorize a
   live canary; no live order or fill is implied.
5. At completion or on any emergency, use **Disable/Disarm** if a persisted
   control state must be cleared, reconcile UNKNOWN intents by exact client ID,
   and verify the persisted terminal state. Do not delete rows or manually
   release a reservation to make a panel look clear.

The backend commands below are optional storage-only diagnostics and emergency
controls for the same dedicated development database; they are not the normal
operator launch and do not perform authenticated connectivity:

```powershell
python -m axiom.cli canary-status --db <development-db>
python -m axiom.cli canary-disarm --db <development-db>
python -m axiom.cli canary-kill --db <development-db>
```

## Versioned settings and rollback

Settings live in the durable `canary_setting_configs` and
`canary_setting_audit` tables and are accessed through
`axiom.canary_settings.CanarySettingsService`:

These service calls manage persisted settings and audit history only; they do
not arm or activate the isolated runtime and do not perform authenticated
connectivity.

```python
service = CanarySettingsService(store, clock=utc_now)
snapshot = service.snapshot()
draft = service.save_draft({"max_submitted_orders_per_day": 10}, actor="operator")
active = service.activate_draft(
    draft["config_id"], actor="operator", expected_generation=snapshot["generation"]
)
```

`save_draft` persists a complete immutable DRAFT with exact values,
deterministic SHA-256 hash, actor, previous hash, and timestamp; it never
changes execution. Activation uses SQLite `BEGIN IMMEDIATE` compare-and-swap
fences for the observed settings generation, active hash, draft hash, and (when
supplied) canary control generation. A stale fence fails closed and leaves the
old ACTIVE row in force. Successful activation archives the prior ACTIVE row,
advances the generation, and records an audit event. Existing ACTIVE settings
are never rewritten by migration.

To roll back, create a new DRAFT containing the reviewed values from the
previous ACTIVE record, review its hash and generation in the dashboard, and
activate it with a fresh generation fence. Never edit/delete settings, audit,
fill, reservation, or UNKNOWN rows. A tighter active/candidate limit than an
existing commitment blocks **new entries only** and records the over-limit
reason; exits, liquidation, and reconciliation remain allowed.

The migration-created ACTIVE configuration is conservative: it takes the
minimum of known legacy `operator_config` and `canary_control` limits instead
of widening them, and does not revive authorization. Changed order-counting
semantics require explicit operator review.

`CanarySettingsService.snapshot()` exposes the full active/draft records,
config ID/hash/generation, control generation/state, effective limits,
authoritative usage and remaining capacity, PHT reset, engineering bounds, and
entry-only over-limit reasons. Decimal values are exact strings; engineering
bounds reject invalid values rather than clamping them.

When authoritative equity evidence is missing or stale, the snapshot reports
`equity_status` as `UNKNOWN`/`STALE` rather than treating it as zero and keeps
new entries blocked. The projection labels collateral as pUSD and reserves
venue base units separately from the human-readable decimal settings.

## Budget units and accounting

Configured monetary values are exact decimal strings in Polymarket collateral
units (pUSD); the historical `_usd` field names are compatibility labels only
and do not imply a fiat conversion. At the SDK boundary, collateral is
represented in integer base units (micro-pUSD, six decimal places) for the
Polygon chain (137), token
`0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB`. SQLite never aggregates money as
`REAL`. No funding, conversion, or approval action is part of this canary.
Fees are part of the BUY commitment, and a missing/UNKNOWN valuation is not
treated as zero.

The complete settings surface is:

| Setting | Meaning |
| --- | --- |
| `target_notional_usd` | Legacy name for the one-BUY target; kept equal to `max_all_in_buy_usd`. |
| `max_exposure_usd` | Legacy name for `max_aggregate_exposure_usd`. |
| `max_daily_loss_usd` | Legacy realized-loss entry stop; kept equal to `realized_loss_entry_stop_usd`. |
| `max_open_positions` | Legacy name for `max_positions`. |
| `max_orders_per_day` | Legacy total submission count; kept equal to `max_submitted_orders_per_day`. |
| `max_slippage_bps` | Maximum permitted execution deviation in basis points. |
| `max_all_in_buy_usd` | One BUY commitment, including fee reserve. |
| `max_fee_reserve_usd` | Fee reserve included in each BUY commitment. |
| `max_gross_daily_buy_usd` | Filled BUY cost for the PHT day plus pending/UNKNOWN remainder; SELLs never refill it. |
| `max_aggregate_open_cost_usd` | Filled cost of currently held lots. |
| `max_aggregate_exposure_usd` | Filled open cost plus unresolved BUY commitments. |
| `max_positions` | Distinct open market positions. |
| `max_submitted_orders_per_day` | Durable BUY and SELL submission attempts, counted once per attempt. |
| `realized_loss_entry_stop_usd` | Realized-loss stop for new entries. |
| `equity_loss_entry_stop_usd` | Equity-loss stop for new entries; external flows are separate. |
| `per_market_buy_cap_usd` | Optional market budget cap. |
| `per_event_buy_cap_usd` | Optional event budget cap. |
| `cumulative_buy_cap_usd` | Optional lifetime BUY commitment cap until an audited reset. |

`per_market_buy_cap_usd`, `per_event_buy_cap_usd`, and
`cumulative_buy_cap_usd` are optional (`null`). All other monetary values have
two-decimal configuration precision. Engineering bounds are explicit:
`0.01`–`1,000,000.00` pUSD for money, `1`–`10,000` for positions,
`1`–`100,000` for daily submissions, and `0`–`10,000` basis points for
slippage. Integer settings reject booleans, fractions, non-finite values, and
out-of-range values. Cross-budget violations are rejected, never silently
clamped.

The storage reservation helper commits capacity before an external venue call.
It counts filled cost plus remaining pending/UNKNOWN amount, rather than
counting a partial fill once as a full request and again as a fill. UNKNOWN is
held until authoritative reconciliation. SELL reservations consume base
inventory, not quote BUY budget, and risk-reducing exits remain available even
when a new entry limit is tighter. Submission attempts are durable and
idempotent; a crash or timeout never authorizes a blind retry.

PHT is `Asia/Manila` and is persisted/derived from UTC timestamps. Crossing
midnight does not erase unresolved orders, open lots, fills, or audit history.
A cumulative-cap reset requires an explicitly persisted `DISARMED` control,
generation fences, and a `CUMULATIVE_USAGE_RESET` audit event; it is never an
automatic midnight reset.

## Loss stops and external flows

Realized-loss and equity-loss settings are **entry stops**, not guarantees of a
maximum loss. They cannot prevent adverse fills, venue outages, stale marks,
fees, settlement changes, or losses already incurred between snapshots. An
external deposit/withdrawal/transfer is tracked as an external flow and must not
be misreported as trading performance or silently refill a BUY budget. An
unknown valuation is treated conservatively and remains reserved until
reconciliation.

## SDK and readiness boundary

The supported Polymarket SDK contract is pinned to 0.9 semantics. Credential,
balance, allowance, market, and order-book checks are read-only diagnostics only;
they do not establish live-order readiness. The isolated operator path does not
run authenticated connectivity checks and cannot reach account/order transports.
Historical research, forward evidence, paper signals, and simulated fills are
not live execution evidence. A live fill is never promised by this guide; every
order-capable path remains independently gated and fail-closed.

## EXPLORATORY_LIVE bounded canary

`EXPLORATORY_LIVE` is a bounded paper-to-live review policy, not an
authorization by itself. Its discovery pool is capped at **10** members, and
only **1–3 direct-valid members** may be enrolled. Every member must retain its
immutable strategy version, canonical operational setup and hash, capture
contract, evaluator contract, exact market scope, and current-market
observation lineage. Capture and evaluation must use that same binding; current
market data must never be backfilled into historical evidence.

The only reviewable exception is profitability evidence: a complete reviewer
disclosure may record profitability as unproven or not reached. This
profitability-only exception never waives setup, observation, calibration,
market, account, risk, or safety gates, and it never makes a paper result live
evidence.

## Unactivated broad scope draft and bounded trace

The operator exposes a pure read projection through
`OperatorControlPlane.rolling_exploratory_scope_draft()`. Normal production
startup and the existing explicit authorization-review path call the
idempotent `_prepare_rolling_exploratory_scope_draft()` callable to persist the
same record; neither callable activates allocation, a canary, or execution.
Dashboard/status GETs use the persisted projection only and never prepare it.
This is a **DRAFT** record, not active authority and not a member selection. Its
canonical normalized scope is:

- `mode=RULE_BASED_MARKETS`, `instrument=POLYMARKET`;
- no category restriction (`categories=[]`), so all supported categories are
  considered, including sports where the market is otherwise supported;
- `supported_market_types=["prediction"]`, meaning the existing standard
  binary prediction-market mechanics only; and
- canonical `scope_hash`, `scope_version`, and a separate `draft_hash`.

The draft excludes `COMBO`, unsupported or non-binary markets, closed or
non-accepting markets, markets without an order book, stale markets,
insufficient liquidity or depth, invalid token identity or strategy setup,
insufficient data, and failed evaluations. It records `paper_only=true`,
`live_execution=false`, `allocation_active=false`, and `canary_armed=false`.
The active operating scope and each selected member's frozen scope remain
separate records with their own hashes and versions; drafting never mutates
either one and there is no scope-activation action.

One bounded paper trace consumes this draft without granting authority:
`PolymarketCollector` discovery is capped at 10 markets, then each candidate
must materialize fresh market metadata, selected-token order-book data, and
token identity before evaluation. The evaluation evidence records the
`momentum` and `mean_reversion` templates plus strategy setup, data quality,
liquidity, depth, and sizing gates. A candidate that fails any gate is retained
as an explicit exclusion rather than silently becoming a live selection.

Each explicit draft tick keeps the request budget honest: at most **11** public
requests are available to draft discovery and scope preparation, reserving **5**
requests for at least one fresh market metadata/order-book capture. Fresh draft
work is scheduled ahead of unrelated frozen normal-cycle maintenance, while
that maintenance remains in its own continuation. Cache-only provider timestamp
and trade-provenance reads do not consume this public-request budget, but still
remain inside the provider and cycle-deadline guards; actual network operations
and their retries are charged normally. Suitable IDs that do not fit the
capture window are retained in the draft's binding-specific
`deferred_market_ids` continuation and retried on a later tick; they are not
reported as provider failures or suitability exclusions.

Official geoblock and account-readiness probes are independent read-only
evidence. Their result must not be inferred from discovery, the absence of a
selected member, or paper evaluation. The final review may show those
independent blockers, but the existing `exploratory.live.review_confirm`
confirmation remains the only final path and still coordinates the existing
authorization, selection, risk, and safety fences without submitting an order.

## Current canary settings and finite authorization

The active reviewed settings in this release are **$1.00 per all-in BUY**,
**$0.01 fee reserve**, **$5.00 gross daily BUY**, **$5.00 aggregate open cost
and exposure**, **3 open positions**, **5 submitted orders per day**, **100 bp
maximum slippage**, and **$2.00 realized/equity entry-loss stops**. The
frequently cited `$20` gross-daily / `$5` per-order values are not the active
settings here and must not be presented as current configuration.
The rolling policy budget and experimental enablement are disclosed separately
under the status `economic_policy.rolling_policy` projection. A zero global
rolling-policy budget or disabled experimental policy does not widen, replace,
or reinterpret the unchanged financial caps above; the dashboard also exposes
those caps under `economic_policy.financial_caps`.

Each authorization requires a finite lifetime budget, explicit stop rules, and
an expiry. Missing, stale, expired, or mismatched lifetime, settings
generation/hash, controller lease, scope, or selection bindings fail closed.
The lifetime budget and expiry are separate from the daily BUY budget; neither
is reset implicitly at midnight.

## Strict admission and capture checks

Before any order-capable action, the reviewed authorization must be bound to
the exact current market and selected token, with a current readiness proof.
The gate rechecks, fail-closed and independently:

- exact market identity, condition, selected YES/NO token identity, and
  strategy direction;
- current market status, accepting-orders state, order-book availability,
  selected-token depth, spread/slippage, and minimum depth;
- authenticated account identity, signer/funder/owner wallet, balance,
  allowance, and exchange spender identity;
- venue geoblock and jurisdiction policy; a fully blocked geoblock blocks
  both directions; and
- the active settings generation/hash, controller lease owner/generation,
  exact portfolio selection, risk reservation, and authorization expiry.

The final POST fence repeats account, geoblock, market, token, allowance, and
depth checks immediately before the irreversible request. Close-only mode
blocks BUY and permits only an otherwise valid managed SELL. Accepted,
partial, timeout, or unknown responses remain durable intents until
authoritative reconciliation.

## Restart, UNKNOWN, positions, and exits

`UNKNOWN` is reserved capacity, not success or failure. Reconcile it by exact
client order ID and authoritative venue state; never blindly retry after a
crash or timeout. On restart, reload and revalidate the persisted
authorization, settings hash/generation, controller lease, selection, scope,
positions, and reservations rather than reconstructing authority from memory.

Position duties are exact: track each managed market, token, strategy version,
expected price, fee reserve, slippage, and remaining quantity. Exit duties
remain available when a tighter entry budget blocks new BUYs; SELL reservations
consume owned base inventory, exit requests stay durable until reconciled, and
an exit never refills the BUY budget.

## Final review action and gate state

The single final action is `exploratory.live.review_confirm`. It must present
complete disclosure of the strategy/setup and direction, capture/evaluator
binding, 1–3 selected members, exact market and token, current readiness,
account/geoblock/depth checks, active settings, lifetime budget and expiry,
stop rules, positions, UNKNOWN usage, and exit capacity. It coordinates the
existing reviewed fences; it does not submit an order.

`exploratory.live.review_confirm` remains **unclicked**. Live submission is
disabled until explicit user confirmation through the control-wired operator
surface. This release makes no live-order or live-fill claim; paper,
historical, simulated, and fake-transport evidence are not execution
evidence. The isolated runtime remains `DISARMED`.

## Emergency rollback

On a stop or emergency, pause new entries first, reconcile each UNKNOWN intent
using its exact client order ID, then use the control-wired operator dashboard
**Disable/Disarm** and verify the persisted state is `DISARMED`. If a setting
must be reverted, create and review a DRAFT from the prior ACTIVE values and
activate it with the fresh generation/hash fence; this persists settings only
and does not activate the isolated canary. Never delete history or manually
release a reservation to make the dashboard appear clear; failed cleanup is an
incident.
