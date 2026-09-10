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
3. Do not click **Enable** or attempt activation from this isolated diagnostic
   surface. It cannot authenticate, reach account/order paths, or authorize a
   live canary; no live order or fill is implied.
4. At completion or on any emergency, use **Disable/Disarm** if a persisted
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

## Emergency rollback

On a stop or emergency, pause new entries first, reconcile each UNKNOWN intent
using its exact client order ID, then use the control-wired operator dashboard
**Disable/Disarm** and verify the persisted state is `DISARMED`. If a setting
must be reverted, create and review a DRAFT from the prior ACTIVE values and
activate it with the fresh generation/hash fence; this persists settings only
and does not activate the isolated canary. Never delete history or manually
release a reservation to make the dashboard appear clear; failed cleanup is an
incident.

## Release evidence and current gate state

The release report is intentionally `IN_PROGRESS`. The bounded R15 smoke
returned `NO_SUPPORTED_EDGE`: its supported-edge evaluator did not run because
the bounded historical projection lacked sufficient chronological observations.
That smoke used no credentials, submitted no orders, mutated no account, and
did not access the live database.

The isolated operator surface was served directly at port 8187. Its observed
state had live trading disabled, paper risk active, and the Polymarket
production transport disabled. Browser-daemon visual verification was
unavailable, so the HTTP surface was checked directly and the isolated
operator and child node were stopped. This is not live-order or live-fill
evidence.

The final financial review was clean for accepted-order accounting, exit
timeouts, transition cleanup, canonical fill recovery, and mixed provisional
surveillance fixes. The final isolation review was clean after execution
profile binding. These reviews do not authorize activation, orders, deployment,
or live-fill verification.

The complete R21 suite recorded 1040 passed, 3 skipped, 342 subtests, 0 failed,
and 221.985 seconds in
`reports/polymarket_release_r21_junit.xml`. R21 is explicitly
**pre-lifecycle-change** evidence. Because lifecycle code changed afterward,
another complete post-lifecycle-change gate remains required; the R21 result
must not be presented as the final gate. Until that gate passes, deployment
remains `DISARMED`, `live_fills_verified` remains false, and no live activation,
order, or fill claim is made.
