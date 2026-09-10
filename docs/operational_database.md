# AXIOM operational database

## Canonical runtime database

Normal AXIOM operation uses one SQLite database:

```text
runtime-data/axiom.sqlite
```

Commands and PowerShell lifecycle scripts use that path when no database
argument is supplied. An explicit `--db` or `-DbPath` remains an intentional
override. AXIOM never merges databases implicitly. For development diagnostics,
always provide a dedicated `--db`/`-DbPath` and the isolated profile switch
(`--isolated` for the CLI or `-Isolated` for PowerShell); omitting that switch
is not a supported development diagnostic profile.

Previous Phase 3, Phase 4, and Phase 4.2 databases, including names such as
`runtime-data/axiom_phase3.sqlite`, `runtime-data/axiom_phase4.sqlite`, and
`runtime-data/axiom_phase42.sqlite`, are development artifacts. They remain
separate for historical inspection; production-like paper operation should
use only `runtime-data/axiom.sqlite`. Merge or copy data only as an explicit,
reviewed migration outside the normal startup commands.

## Persisted Polymarket forward evidence

The read-only collector persists immutable forward records in
`polymarket_markets`, `polymarket_snapshots`, `polymarket_trades`, and
`collection_errors`.  `polymarket_markets` and `polymarket_snapshots` carry a
queryable `source_type` (`FORWARD_COLLECTED` or `HISTORICAL`); forward
snapshots also carry `quality`.  `collection_cycles` stores each bounded cycle
and `collector_state` stores the latest collector projection, including:
`candidate_bound_markets`, `candidate_bound_scheduled`,
`candidate_bound_fresh`, `candidate_bound_stale`,
`candidate_bound_missing`, `paper_forward_markets`,
`paper_forward_scheduled`, `discovery_scheduled`, `discovery_deferred`,
`candidate_references`, per-tier attempt/success/failure counts,
`request_latency_summary`, and `capacity_reason`.

Forward snapshot payload timestamps are intentionally distinct:

- `request_started_at` is when the order-book request began.
- `source_timestamp` is the canonical market/order-book timestamp persisted
  in the snapshot table and used for chronology.
- `provider_timestamp` is optional adapter timestamp evidence and may be
  `null` when the provider does not expose it; its absence does not replace or
  rewrite `source_timestamp`.
- `response_received_at` is when the response was received.
- `observed_at` is AXIOM's collection observation time.

Provider/source timestamps are rejected when they are too far in the future;
network or malformed-data failures become `collection_errors`, never synthetic
prices or settlements.

## Candidate authority and health

`candidate_forward_requirements` is the bounded, read-only authority for
current executable markets.  A candidate's declared targets remain visible,
but historical dataset constituents are recorded as
`historical_market_ids_ignored` and are never executable.  `market_ids` and
`permitted_market_ids` are the authorized current targets after frozen-filter
and target-instrument checks; they are not a global discovery list.

`polymarket_required_health` grades only those candidate-bound required
markets (`grade_scope: required_forward_markets`).  Its `fresh`, `stale`, and
`missing` sets, market diagnostics, candidate references, and source/observed
time bounds must not be read as health for every tracked or discovered market.
Global discovery is separately bounded by `discovery_budget_per_cycle` and
reported as scheduled or deferred; candidate-required work is prioritized
before paper-forward and discovery work.

Operator interpretation of authority and signal outcomes:

- `RESOLVED` / `CANDIDATE_FORWARD_MARKET_RESOLVED`: an executable current
  market was authorized.
- `UNRESOLVED` / `CANDIDATE_FORWARD_MARKET_UNRESOLVED`: no executable current
  market was resolved. `COLLECTOR_CAPACITY_INSUFFICIENT` is the separate
  capacity form of this outcome.
- `CLOSED` / `CANDIDATE_MARKET_CLOSED` and `MARKET_CLOSED`: the declared
  market is terminal, expired, inactive, or closed.
- `MARKET_FILTER_MISMATCH`: an active declared target is outside the frozen
  filters and remains diagnostic-only.
- `NO_FORWARD_SNAPSHOT`: an authorized market has no current persisted
  snapshot. A source or observed timestamp outside the canary's 60-second
  age limit is `STALE_FORWARD_EVIDENCE`.
- `COLLECTOR_CANDIDATE_HEALTH_BLOCKED`: required-health or binding data could
  not safely authorize evaluation. `NO_STRATEGY_SIGNAL` is non-actionable
  strategy output, not a collection failure.

`canary_signal_evaluations` is the durable audit row for each candidate
evaluation: `evaluation_id`, `candidate_id`, optional `cycle_id`,
`evaluated_at`, `reason_code`, optional `market_id`/`signal_id`, and the
bounded `signal`, `required_health`, and `evidence` JSON projections.
Reason counts include `READY_SIGNAL`, `NO_STRATEGY_SIGNAL`,
`NO_FORWARD_SNAPSHOT`, `STALE_FORWARD_EVIDENCE`, `MARKET_CLOSED`,
`MARKET_FILTER_MISMATCH`, `CANDIDATE_FORWARD_MARKET_UNRESOLVED`, and
`COLLECTOR_CANDIDATE_HEALTH_BLOCKED`.

The singleton `canary_autonomous_state` stores the latest signal-scan
projection (`signal_scan_*` cursor, ranking binding, cycle, coverage,
remaining count, status, skip reasons, and reason counts).  Each tick checks
at most 10 ranked candidates.  `canary_signal_scan_checked` retains the eight
most recent scan cycles.  `canary_signal_evaluations` retains the newest 4096
evaluations.  Unreferenced transient `canary_signals` are bounded to 4096;
signals referenced by evaluation/execution state and the newest `READY` signal
per candidate are protected.

## Normal operation examples

```powershell
python -m axiom.cli node-run --cycles 0 --disable-research --disable-mutations
python -m axiom.cli dataset-catalog --status
python -m axiom.cli research-summary
python -m axiom.cli submit-proposal --proposal '{"proposal_id":"proposal-example","statement":"<one falsifiable statement>","source":"<public source or immutable AXIOM result>","tests":["<bounded chronological test>"],"dataset_version":"<immutable version>","time_split":"train-validation-holdout","paper_only":true,"experiment_plan":{"schema_version":"1","market_type":"prediction","template":"probability_mispricing","dataset_version":"<immutable version>","max_variants":4,"min_samples":30,"paper_only":true}}'
```

## Micro-live Polymarket canary

Production live trading remains disabled by default. This release documents
only an isolated development and diagnostic path; deployment remains
`DISARMED`. Launch the control-wired operator surface, which supervises one
paper node, with a dedicated development database:

```powershell
python -m axiom.cli operator --isolated --db <development-db> --port 8187
```

Do not pair the node lifecycle start script with the control-less `dashboard`
surface for operator **Enable**/**Disable** actions. `dashboard` is not the
operator surface. Isolated mode denies production credential loads, configured
credential probes, authenticated connectivity, and account/order transports.
It cannot activate a canary. Do not configure or provide Polymarket
credentials for this diagnostic path; no credential or wallet command is part
of this procedure.

1. In the operator dashboard, review the **ACTIVE**/**DRAFT** settings, exact
   config ID/hash, settings and canary control generations, PHT reset, all
   independent remaining capacities, unresolved/UNKNOWN intents,
   candidate-bound forward evidence, and the expected price/fee/slippage.
2. Do not manually copy candidate, market, or token IDs into a normal run.
3. Do not click **Enable** or attempt activation from this isolated diagnostic
   surface. It cannot authenticate, reach account/order paths, or authorize a
   live canary; no live order or fill is implied.
4. On completion or emergency, pause entries, reconcile UNKNOWN intents by
   exact client ID, use **Disable/Disarm** if a persisted control state must be
   cleared, and verify the persisted state. Never delete rows or release a
   reservation to make a projection look clear.

Risk settings use exact pUSD decimal strings; `_usd` names are legacy labels
only and do not imply fiat conversion. Polygon chain 137 collateral uses
integer micro-pUSD base units (six decimals), token
`0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB`. The isolated canary does not
fund, convert, create credentials, repair allowances, or access authenticated
venue paths. The settings panel displays the complete active/draft surface and
engineering bounds (money `0.01`–`1,000,000.00` pUSD, positions `1`–`10,000`,
submissions `1`–`100,000`, slippage `0`–`10,000` bps); invalid, non-finite,
boolean, fractional, or cross-budget values are rejected rather than clamped.

The following backend commands are optional storage-only diagnostics and
emergency controls for the same dedicated development database, not the normal
operator launch:

```powershell
python -m axiom.cli canary-status --db <development-db>
python -m axiom.cli canary-disarm --db <development-db>
python -m axiom.cli canary-kill --db <development-db>
```

`canary-status` reads persisted projections. `canary-disarm` returns immediately
to paper-only and `canary-kill` prevents further submissions. The authenticated
`canary-check` path is not available in isolated mode and must not be used as a
substitute for this diagnostic procedure. An expired authorization returns to
paper-only automatically. SDK 0.9 checks are read-only, but isolated mode does
not run authenticated connectivity checks. Forward evidence, research, paper
signals, and simulated fills do not establish live-order readiness; no live
order or fill is claimed here.

Dashboard HTTP `GET` endpoints and `canary-status` read persisted projections.
They never probe a provider or the keyring; a credential cache miss is shown as
`NOT CHECKED`, not treated as a fresh credential result. The
`node-run --cycles 0 --disable-research --disable-mutations` example is
paper-only, but `--cycles 0` means run until stopped and collection still uses
configured public providers. It is therefore not a provider-free dry proof or
live-order readiness proof. Use `node-status`, `canary-status`,
`dataset-catalog --status`, or dashboard `GET` for storage-only inspection.

PowerShell lifecycle commands are node-only diagnostics, not the control-wired
operator surface. When they are used for development diagnostics,
`-Isolated` is REQUIRED on every invocation, and `-DbPath` must point to the
same dedicated development database. Never omit `-Isolated` or pair these
commands with the control-less dashboard for operator Enable/Disable:

```powershell
.\ops\start_axiom_node.ps1 -DbPath <development-db> -Isolated
.\ops\status_axiom_node.ps1 -DbPath <development-db> -Isolated
.\ops\restart_axiom_node.ps1 -DbPath <development-db> -Isolated
.\ops\stop_axiom_node.ps1 -DbPath <development-db> -Isolated
```

## Binance Spot canary

See [Binance Spot canary guide](binance_spot_canary.md) for the isolated strict Binance Spot `BINANCE_SPOT_TESTNET` runtime, operator controls, evidence status, and later reviewed migration procedure. Its fixed isolated database is `runtime-data/binance-testnet.sqlite`. The separate `PAPER` development runtime uses its own fixed isolated database, `runtime-data/binance-dev.sqlite`; both runtimes remain separate from the Polymarket canary and canonical `runtime-data/axiom.sqlite`.

## Historical bootstrap examples

```powershell
python -m axiom.cli bootstrap-history --all --resume
python -m axiom.cli bootstrap-history --crypto --resume
python -m axiom.cli bootstrap-history --polymarket --resume
python -m axiom.cli btc-research
```

To inspect another database without changing the canonical runtime database,
pass it explicitly:

```powershell
python -m axiom.cli dataset-catalog --db runtime-data/axiom_phase42.sqlite --status
.\ops\status_axiom_node.ps1 -DbPath runtime-data/axiom_phase42.sqlite -Isolated
```
