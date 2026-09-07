# Binance Spot autonomous micro-live canary

**Status:** isolated development vertical slice complete; production rollout is **not** complete.

This is the operator and reviewer runbook for the Binance Spot autonomous canary. It documents the implementation currently on `feature/binance-spot-canary`, the evidence that has actually been collected, and a later migration path. The implementation is intentionally isolated from the Polymarket canary and from the normal AXIOM node.

> **Safety boundary:** the supported CLI is PAPER-only. No Binance authenticated TESTNET or LIVE session was executed for this slice; no credentials were read; and no real orders or trades were sent.

## Start the isolated development runtime

Run from the Binance canary checkout with the repository virtual environment:

```powershell
.venv\Scripts\python.exe -m axiom.cli binance-dev
```

The command has no environment or database switches. It constructs `BinanceDevelopmentRuntime` with:

| Resource | Fixed value |
| --- | --- |
| Environment | `PAPER` |
| Dashboard | `http://127.0.0.1:8081` |
| SQLite database | `runtime-data/binance-dev.sqlite` |
| Runtime identity | `binance-dev` |
| Log / lock / stop / PID files | `runtime-data/binance-dev.log`, `binance-dev.lock`, `binance-dev.stop`, `binance-dev.pid` |
| Worker interval | 60 seconds |

`--once` is the bounded smoke mode: it starts the isolated server, runs one worker cycle, reports status, and stops. Without `--once`, the worker runs until the process is stopped. The runtime validates the canonical checkout and database path after path resolution, requires loopback `127.0.0.1`, requires port `8081`, and refuses a LIVE profile. Do not point it at `runtime-data/axiom.sqlite` or another checkout.

The generic `dashboard` and `operator` commands retain their normal default port `8080`; **do not use port 8080 for this Binance runtime**. Do not reuse a live runtime's database, keyring namespace, or process for this checkout.

### Environment and authorization defaults

There are three named Spot environments in the low-level contracts:

- `PAPER`: no authenticated REST origin and no authenticated Binance calls.
- `BINANCE_SPOT_TESTNET`: fixed authenticated origin `https://testnet.binance.vision`.
- `BINANCE_SPOT_LIVE`: fixed origin `https://api.binance.com`, but refused by the development profile/runtime.

The bare CLI always selects `PAPER`; it cannot select TESTNET or LIVE. The low-level `BinanceRuntimeProfile.development()` constructor defaults to TESTNET for embedders, which does **not** alter the CLI behavior. A PAPER runtime rejects credentials.

The exact TESTNET construction boundary is:

1. An embedder supplies explicit in-memory credentials as `BinanceSpotCredentials` (or an equivalent `{"api_key": ..., "api_secret": ...}` mapping). There is no ambient environment-variable fallback.
2. `BinanceDevelopmentRuntime(..., environment=BINANCE_SPOT_TESTNET, credentials=credentials, venue=None)` constructs exactly `BinanceSpotRESTClient(BINANCE_SPOT_TESTNET, credentials)`. An embedder may supply a venue instead, but it must be the exact, unwrapped `BinanceSpotRESTClient`; wrappers, subclasses, PAPER venues, and arbitrary objects are rejected.
3. The profile environment, runtime environment, venue environment, and fixed origin must agree exactly. The origin is not caller-configurable. `BinanceExecutionService` repeats the same identity check before opening its execution boundary.
4. The persisted control row binds the environment, immutable candidate binding hash, risk-envelope hash, and one-way fingerprint of the resolved credentials. A credential fingerprint mismatch on restart clears authorization, returns to `DISABLED`, and advances the control generation. Credential values are never persisted.

`BinanceCredentialStore` is an optional caller-side loader in the isolated `AXIOM-BINANCE-SPOT` keyring namespace (for example, the `binance-dev:BINANCE_SPOT_TESTNET` reference). Loading a keyring value does not weaken the boundary: the embedder must resolve it and pass the explicit credentials to the runtime, and environment fallback remains prohibited.

The independent Binance execution control row starts as `DISABLED` and `authorized=false`. This control plane is separate from the Polymarket canary control plane; the runtime has no Polymarket or Hermes transport.

To enable the independent control plane, the only accepted confirmation is exactly:

```text
ENABLE BINANCE AUTO CANARY
```

The phrase is case- and whitespace-sensitive. It is required for `ENABLE` and `RESUME`; a wrong phrase is rejected before service invocation. Enabling never bypasses the PAPER boundary or the development refusal of LIVE. There is no CLI command that configures Binance credentials or turns this runtime into authenticated TESTNET.

## Architecture and data flow

```text
persisted universe snapshot
        │
        ▼
BinanceAdapter (public market data only)
        │  exchangeInfo, closed klines, 24hr ticker, depth
        ▼
BoundedBinanceMarketCollector
        │  bounded freshness/depth/fill evidence + provenance
        ▼
BinanceAutonomousWorker (one serialized cycle)
        │
        ├─ reconcile durable execution state first
        ├─ evaluate exits for owned positions
        ├─ qualify and deterministically rank frozen candidates
        ├─ evaluate entry signals and risk
        └─ submit through BinanceExecutionService when independently ARMED
                         │
                         ├─ PAPER: deterministic local PaperBinanceSpotVenue
                         └─ future TESTNET embedder: exact BinanceSpotRESTClient bound to explicit credentials
AxiomStore (binance_* tables) ◄── execution ledger, reservations, fills, positions,
                                  reconciliation, worker cycles/events, operator audit
        │
        ▼
DashboardData → /api/v2/binance-canary and localhost /api/binance/control
```

The public market adapter and the authenticated execution client are deliberately separate. The market collector never exposes account, order, or cancellation methods. The worker never enables execution and does not construct a venue or network object. Every cycle is bounded and serialized; a concurrent cycle returns `BUSY` rather than overlapping decisions.

The default persisted crypto universe is `TOP_50_MARKET_CAP_BINANCE_USDT`: a daily (`1d`) point-in-time market-cap ranking intersected with Binance Spot `exchangeInfo`, quoted in `USDT`. The builder excludes configured stablecoins, duplicate/wrapped/staked/pegged assets, leveraged-token-style symbols, symbols that are not `TRADING`, and symbols without Spot permission. The runtime consumes the persisted selected snapshot; it does not silently rebuild membership during collection. The public smoke is a separate fixed three-symbol fixture: `BTCUSDT`, `ETHUSDT`, and `SOLUSDT`.

Default collector/worker boundaries are intentionally small: collector concurrency is at most four workers, depth is 20, market freshness is 120 seconds, historical request limit is 1,000 rows, decision interval is `1d`, and at most one entry is attempted per cycle. Selected symbols are collected by universe rank; an existing owned position is added as an explicit exit-only symbol so exits do not depend on current universe membership.

### One worker cycle

1. Reconcile account and unresolved intents before touching entry data, even while `DISABLED`, `DISARMED`, or `KILLED`.
2. Load the exact persisted universe version and bind provenance (`universe_id`, version, snapshot hash, dataset version).
3. Collect closed klines, ticker, order book, `exchangeInfo`, freshness, spread, depth, and conservative buy/sell fill evidence.
4. Evaluate owned-position exits before ranking or entry work.
5. Pass current execution feasibility to the qualification service, rank qualified candidates, and require a `CURRENT` valid selection.
6. Apply the control state, signal, exchange filters, account snapshot, and risk envelope again immediately before reserving and submitting an order.
7. Persist cycle/event state and a secret-free status projection.

Missing, stale, malformed, or incomplete evidence is a no-trade or pause condition; it is not permission to guess.

## Qualification and deterministic ranking

Qualification is for frozen crypto-Spot research candidates, not arbitrary dashboard input. A candidate must be lifecycle stage `FROZEN`, have `crypto_spot` market type, have an immutable symbol mapping/binding, and pass every gate below:

| Gate | Default requirement |
| --- | --- |
| Net expectancy after costs | strictly positive |
| Maximum drawdown | `<= 0.20` |
| Samples | at least 30 |
| Trades | at least 3 |
| Walk-forward consistency | `>= 0.60` |
| Neighbor stability | `>= 0.60` |
| Cost/slippage stress | positive/passed |
| Current execution feasibility | exactly `true` |
| Forward paper evidence | exactly `true` |
| Locked holdout | must be unused/declared false |

The qualification policy is `binance-crypto-execution-policy-v1`; the ranking formula is `binance-crypto-ranking-v1`; qualification records are `binance-crypto-execution-qualification-v1`. Each record persists binding, qualification, lifecycle-evidence, policy, and formula hashes. A changed immutable catalog/lifecycle record invalidates the current selection rather than being silently accepted.

The ranking score is the weighted mean of bounded components. The current weights are:

| Component | Weight |
| --- | ---: |
| Net expectancy | 0.24 |
| Drawdown | 0.12 |
| Sample count | 0.10 |
| Trade count | 0.08 |
| Walk-forward consistency | 0.14 |
| Neighbor stability | 0.12 |
| Cost/slippage stress | 0.10 |
| Execution feasibility | 0.05 |
| Forward paper evidence | 0.05 |

Materially equivalent candidates are clustered using family, root lineage, dataset, and universe provenance; only the cluster representative is rankable. Tie breaks are, in order: total score descending, net expectancy descending, walk-forward consistency descending, neighbor stability descending, candidate ID ascending, and symbol ascending. The ranker persists one `CURRENT` selected winner plus at most three bounded fallbacks. `STALE`, `NONE`, invalid, or missing selection blocks new entries and requires requalification; it does not authorize use of a previous winner.

The worker's feasibility check additionally requires `TRADING` status, Spot permission, `new_entry_allowed`, fresh ticker and book, at least one bid and ask level, a positive spread, and complete conservative buy and sell fill evidence. A stale universe, stale market, thin book, missing bars, or incomplete evidence produces no trade.

## Canonical risk envelope and exchange rules

The immutable default envelope is `binance-spot-risk-v1`, with all money in `USDT`:

| Limit | Default |
| --- | ---: |
| Entry notional, all-in cap | 10.00 USDT |
| Maximum aggregate exposure | 30.00 USDT |
| Maximum reserved exposure | 30.00 USDT |
| Realized-loss entry stop | 5.00 USDT |
| Equity-loss entry stop | 5.00 USDT |
| Maximum open positions | 5 |
| Maximum submissions per UTC day | 20 (entries and exits) |
| Maximum execution deviation | 100 bps |
| Exit-order reserve | 1 order per owned position |

`size_limit_order` performs pure Decimal sizing and filter validation; `assess_order` is the stateful final assessment. A prior approval is not an authorization token. The execution service re-assesses under its writer lock immediately before holding a reservation and calling the venue.

`exchangeInfo` is parsed into immutable `SymbolRules`. The decision boundary checks symbol identity, `status=TRADING`, `isSpotTradingAllowed`, declared order types, and declared time-in-force values. For LIMIT orders it applies the relevant positive bounds from:

- `PRICE_FILTER`: min/max price and tick size. Prices are rounded conservatively: BUY rounds up to a tick and SELL rounds down; enabled min/max bounds still reject violations.
- `LOT_SIZE`: min/max quantity and step size. Quantity is floored to the step and available inventory.
- `MIN_NOTIONAL` and `NOTIONAL`: positive minimum/maximum notional and their market-application flags.
- `PERCENT_PRICE` and `PERCENT_PRICE_BY_SIDE`: reference/average-price bands; an active band without a usable reference is blocked.
- `MAX_POSITION`: projected base-asset position cap.
- `MAX_NUM_ORDERS` and exchange order limits: current open orders plus the reserved exit capacity.

`MARKET_LOT_SIZE`, `MAX_NUM_ALGO_ORDERS`, `MAX_NUM_ICEBERG_ORDERS`, and newer/unknown filters are retained losslessly in the rules projection. This slice does not place MARKET, algo, iceberg, OCO, amend, or order-list orders; retained filters are not permission to add an unsupported order type. Unknown metadata is never treated as a wildcard.

The all-in BUY check is `notional + fee reserve <= 10.00 USDT`; it also checks quote availability, aggregate/reserved exposure, position count, daily loss, daily submissions, account pause, rate limit, and exchange capacity. A SELL requires AXIOM-owned base inventory, is floored to available inventory, and is blocked as `DUST`/`EXIT_BELOW_MINIMUM` when the result is zero or below the active quantity/notional minimum. No averaging into an owned position is performed.

## Order lifecycle, reconciliation, and crash semantics

Only `LIMIT` orders with `timeInForce=IOC` or `FOK` are accepted. Entry is `BUY`; exit is `SELL`. Client order IDs are deterministic `AXIOM-` identifiers derived from signal ID, symbol, and binding hash and are limited to 36 characters. The venue is called outside SQLite transactions.

The durable intent states are:

```text
INTENT → RESERVED → SUBMITTING → ACKNOWLEDGED / UNKNOWN / PARTIALLY_FILLED
                                      └→ FILLED / CANCELED / EXPIRED / REJECTED
```

Transitions are writer-transaction guarded and merged monotonically when concurrent workers, restart reconciliation, or a late venue response observe the same intent. A stale `ACKNOWLEDGED`/`UNKNOWN` observation cannot regress a terminal state or resurrect a canceled order. A `CANCELED` or `EXPIRED` row can be upgraded only when durable fill evidence proves that execution occurred; it is never upgraded merely by a stale status response.

`UNKNOWN` is an active unresolved state, not a failure that may be retried blindly. Network exceptions, HTTP 5xx responses, Binance ambiguous codes `-1000`, `-1006`, and `-1007`, a generation change during a venue call, or an ambiguous cancellation result preserve the intent as `UNKNOWN`. Its risk reservation remains held until reconciliation resolves it.

Exchange trade identities are scoped by symbol: a durable fill key is `<BINANCE_SYMBOL>:<exchange_trade_id>`, so equal raw trade IDs on different symbols remain distinct. Startup migration rewrites legacy unscoped IDs to this symbol-scoped form under the execution-store lock, drops only duplicate target keys, and is idempotent on every later initialization. It is not permission for an operator to edit, merge, or copy execution rows manually.

Reconciliation polls the account, queries each unresolved order by AXIOM client/exchange ID, and ingests `myTrades` rows by exact symbol-scoped exchange trade identity. The REST client canonicalizes `query_order`, `cancel_owned_order`, and `my_trades` inputs to Binance's `orderId`/`origClientOrderId` and symbol parameters; caller aliases cannot broaden ownership or endpoint access.

Crash boundaries are deliberate:

- A crash before the venue call leaves a committed `RESERVED` intent; restart queries its deterministic client ID. If the venue returns an authoritative absence (`-2013`/`authoritative_missing`), the result is a `RESET`, the control plane pauses with `TESTNET_RESET`, the reservation remains held, and no retry or second submission is allowed.
- A crash after the durable `SUBMITTING` transition but before the venue call has the same authoritative-absence behavior. The absence is authoritative only for reconciliation; it is not permission to infer a successful order or resubmit.
- A crash after submission but before the response commit leaves `SUBMITTING` or `UNKNOWN`; restart queries the existing client ID and never blindly submits again.
- A control-generation or kill change during an in-flight venue call converts the result to `UNKNOWN`; an already in-flight request is not retracted.
- Leases and owner IDs allow a restarted worker to reclaim stale work, while deterministic signal/client IDs prevent duplicate interval submissions.

`FILLED` is evidence-complete only when durable symbol-scoped fill rows cover the authoritative filled quantity. An exchange order status of `FILLED` without `myTrades` rows therefore keeps the reservation `HELD` and remains in reconciliation. Reservations release for `REJECTED`, `CANCELED`, and `EXPIRED`; they release for `FILLED` only after that evidence check. `PARTIALLY_FILLED` retains the unresolved remainder/worst-case reservation.

The local `PaperBinanceSpotVenue` has no transport. IOC fills up to configured liquidity; FOK fills only if the complete quantity is available, otherwise it expires. IDs and fills are deterministic and monotonically allocated within one venue instance, and query/cancel/myTrades operations accept only orders created by that instance. This is execution-boundary behavior, not exchange evidence.

### Paper forward evidence boundary

`CryptoPaperForwardEngine` is a separate deterministic evidence path. It consumes closed bars only, applies an explicit `now` as a close-time cutoff when supplied, evaluates on a completed bar, and executes the resulting LIMIT-style action only at the next bar's open. An absent next open, invalid timing, or zero available depth is a no-fill condition. Without an explicit cutoff it replays all supplied closed rows and does not hash ambient wall-clock time into the run identity.

Explicit side-book levels cap quantity and determine conservative VWAP. The engine recomputes VWAP for the final depth/cash-capped quantity, so unconsumed levels cannot change a partial fill's price; scalar depth caps are applied as additional limits. Each fill, observation, position event, and persisted result carries the run identity. A caller-supplied `run_id` is retained; otherwise the engine derives `binance-paper-<hash>` from immutable bindings, bars, universe/dataset provenance, interval, explicit cutoff, strategy, and simulation configuration. Reusing a run ID with a different immutable hash is rejected rather than silently overwritten.

## Positions, fees, dust, and breakers

Positions are recomputed from deduplicated fills and persist symbol, quantity, cost basis, average cost, mark, realized P/L, unrealized P/L, fees, valuation status, candidate, binding, and exit policy. A base-asset commission reduces received BUY quantity (and increases required SELL quantity); a quote-asset commission is a quote cost. A third-asset commission requires an explicit quote conversion field or fee mark. An unknown conversion is not treated as zero: valuation becomes `UNKNOWN`, unrealized P/L is withheld, and an armed service pauses with `UNKNOWN_FEE_VALUATION`.

Daily submission/loss counters and fees reset only at a UTC-day boundary. Inventory, open orders, aggregate exposure, unresolved/UNKNOWN reservations, and positions carry through rollover; a new UTC day never creates buying room. Unknown or unresolved orders remain risk-reserved across restart and rollover.

SELL reservations are symbol-scoped base-asset reservations, not quote buying-power reservations. The durable snapshot subtracts each held SELL's unfilled remainder from that same symbol's available inventory; the operator projection reports `base_reservations` by symbol and excludes those quantities from quote reservations. A SELL is floored to owned, unreserved base inventory and is blocked as `DUST`/`EXIT_BELOW_MINIMUM` when no valid remainder meets active filters. The worker excludes any symbol with a held SELL reservation from the next exit pass, suppressing duplicate exits until reconciliation releases or resolves the prior intent.

The following conditions fail closed or pause entries as applicable: `DISABLED`, `PAUSED`, `DISARMED`, `KILLED`; stale/missing market evidence; non-TRADING or Spot-disabled symbols; thin books/wide spreads; execution deviation; account pause; rate limit; crash marker; insufficient quote or owned inventory; order/filter violations; daily loss/submission limits; reconciliation reset/failure; and changed binding/envelope/credential identity.

Control actions have these meanings:

- `PAUSE`: blocks entries, allows risk-reducing exits, and keeps reconciliation running.
- `DISARM`: returns to a non-submitting `DISARMED` state; reconciliation continues and existing positions are only reported/risk-managed, not silently liquidated.
- `KILL`: sets `KILLED`, clears authorization, sets the kill request, and fences new submissions (including an in-flight result after the generation check). Treat it as an emergency stop. A later re-enable requires an explicit review and the exact confirmation phrase; `DISARM` is not a re-enable.

No action automatically closes positions, cancels an unknown order, or retries a request whose outcome is unknown.

## Dashboard and operator controls

The isolated server serves the Binance page at `http://127.0.0.1:8081` and exposes:

- `GET /api/v2/binance-canary`: bounded, secret-free status, qualification/selection, risk budgets, positions, orders, fills, UNKNOWN orders, worker heartbeat, and recent action audit. Default page size is 25; projections are capped at 100 records.
- `POST /api/binance/control`: the typed Binance-only action boundary. Allowed actions are `CONNECTIVITY_CHECK`, `ORDER_VALIDATION_TEST`, `ENABLE`, `PAUSE`, `RESUME`, `DISARM`, `KILL`, and `RESTART`.

Browser control requests require a loopback client, a same-loopback Origin when supplied, the server's generated `X-Axiom-Control-Token`, a JSON body no larger than 16 KiB, and an injected Binance control plane. Secret-shaped keys reject the whole nested value, including mappings and lists; persisted `*_json` columns are decoded before projection so nested secrets cannot survive as opaque strings. The credential projection is the sole allowlist exception and contains only `configured` and an opaque `reference_hash`; arbitrary credential mappings, hashes, and values are never returned. Configuration is not a dashboard operation; credential values are never returned.

The page shows development identity (environment, runtime, database, transport, safe credential status), connectivity/readiness, current-versus-stale selection, risk limits and remaining budgets, latest signal/no-trade reason, and bounded record tables. Status records contain both timezone-aware UTC and `Asia/Manila` (PHT) projections. Use UTC for audit ordering and PHT only as the operator display; do not infer a different execution day from the browser's local timezone.

`ORDER_VALIDATION_TEST` may invoke only the venue's test/validation method, never a place/submit/order method. It is a validation-only result and does not mean an order was sent. `CONNECTIVITY_CHECK` is read-only. `RESTART` requests worker restart only; it is not an automatic process or code rollout.

## REST/API baseline and transport decision

The implementation pins the Binance Spot API documentation baseline to commit [`041bba2d8a0bb8d26f77b88a0e2761743233fcf7`](https://github.com/binance/binance-spot-api-docs/commit/041bba2d8a0bb8d26f77b88a0e2761743233fcf7). The code records the production REST baseline date as `2026-09-02` and the Spot Testnet baseline date as `2026-09-04`; the latter is the Testnet changelog date at that pinned commit, not evidence that this checkout authenticated.

Relevant official, commit-pinned references:

- [Pinned Spot REST API](https://github.com/binance/binance-spot-api-docs/blob/041bba2d8a0bb8d26f77b88a0e2761743233fcf7/rest-api.md): request security, timing, HTTP/error semantics, exchange information, order, account, order query, open/all orders, trades, rate-limit, and cancel endpoints.
- [Pinned Spot Testnet REST API](https://github.com/binance/binance-spot-api-docs/blob/041bba2d8a0bb8d26f77b88a0e2761743233fcf7/testnet/rest-api.md): Testnet origin and security semantics.
- [Pinned symbol/exchange filters](https://github.com/binance/binance-spot-api-docs/blob/041bba2d8a0bb8d26f77b88a0e2761743233fcf7/filters.md): price, quantity, notional, percent-price, position, and order-count filters.
- [Pinned User Data Streams reference](https://github.com/binance/binance-spot-api-docs/blob/041bba2d8a0bb8d26f77b88a0e2761743233fcf7/user-data-stream.md): documented stream semantics; this slice does not implement or consume them.

No Binance SDK was selected. The authenticated client uses Python standard-library `urllib.request`, form encoding, and HMAC-SHA256 signing. Its fixed allowlist is:

```text
GET    /api/v3/account
POST   /api/v3/order
POST   /api/v3/order/test
GET    /api/v3/order
GET    /api/v3/openOrders
GET    /api/v3/allOrders
GET    /api/v3/myTrades
GET    /api/v3/rateLimit/order
DELETE /api/v3/order
```

The client fixes the origin from the named environment, accepts only explicit in-memory credentials, defaults `recvWindow` to 5,000 ms and request timeout to 10 seconds, and never accepts an arbitrary URL. The optional `BinanceCredentialStore` is the isolated keyring loader; it is not an environment-variable fallback and does not make credentials part of the client projection. Authenticated operation is **polling-based** (`account`, order queries, open/all orders, `myTrades`, and order-rate limits); there is no User Data Stream or websocket recovery path in this slice.

`query_order`, `cancel_owned_order`, and `my_trades` translate only canonical ownership parameters: `symbol` plus `orderId` or `origClientOrderId` (and `orderId` for `myTrades`). Caller spelling aliases are normalized or discarded before signing. The allowlist intentionally excludes withdrawals, transfers, deposits, account-management operations, arbitrary endpoints, and all unsupported order forms. API keys for any later TESTNET exercise must have only the required `USER_DATA` and `TRADE` permissions and must explicitly prohibit `WITHDRAWAL` and `TRANSFER`.

## Evidence and coverage status

### Isolated browser smoke (executed)

The isolated runtime was exercised at `http://127.0.0.1:8081`. The browser projection showed environment `PAPER`, execution `DISABLED`, no credentials, no horizontal overflow, and zero orders and fills. A read-only `CONNECTIVITY_CHECK` was submitted through the loopback control boundary and appeared as a persisted operator action; no order-validation or order-placement action was used. Ctrl+C then exited with code 0 and removed the runtime lock and PID files. This smoke did not authenticate to TESTNET or LIVE.

### Public real-crypto smoke (executed)

The only public-network evidence was a guarded, credential-free temporary smoke script and JSON output created under `runtime-data` for the verification run. Both temporary artifacts were removed during cleanup. They are recorded evidence only; a fresh checkout does not require these files, and no ignored artifact is a prerequisite for the runbook.

- Host was exactly `api.binance.com`; HTTPS and redirects were guarded.
- Exactly 12 requests were made: three each to `GET /api/v3/exchangeInfo`, `GET /api/v3/klines`, `GET /api/v3/ticker/24hr`, and `GET /api/v3/depth`.
- Symbols were exactly `BTCUSDT`, `ETHUSDT`, and `SOLUSDT`.
- The guard recorded zero violations, no credentials, no signatures, no account endpoints, and no order endpoints.
- Smoke limits were three symbols/workers, 2-second adapter timeout, 9-second per-symbol timeout, depth 5, three candles, and 0.001-unit fill evidence.
- All three records were observed as Binance-tradable, but the output status was `NO_TRADE` because ticker and book freshness were false at the smoke cutoff. This is market-data evidence only; the buy/sell prices are conservative book evidence, not fills.

### Tests and unexecuted authenticated coverage

The exact executed verification evidence is **127 Binance tests green**, **457 full isolated tests green**, and **19 focused legacy Polymarket regression tests green**. These tests cover injected/local boundaries, deterministic paper execution, risk/filter decisions, operator projections, and crash/reconciliation cases; they do not turn a fixture into network evidence.

**Authenticated Binance Spot TESTNET coverage is absent.** No authenticated TESTNET connectivity, account read, order-test, order submission, order query, trade-history reconciliation, or crash-after-submit exercise was run for this slice. No credentials were read, no keyring values were inspected, and no Binance LIVE request was attempted. LIVE coverage is prohibited by the development profile regardless of credentials, and no real orders or trades occurred.

## Review gate and later isolated migration

The following is a later, explicitly reviewed operation—not an automatic merge or rollout:

1. A reviewer verifies this document against the current code, pinned API baseline, risk envelope hash/version, endpoint allowlist, fill-ID migration behavior, redaction boundary, and branch diff. **Do not merge automatically.**
2. Preserve this checkout, `binance-dev` identity, port 8081, and `runtime-data/binance-dev.sqlite` as an isolated development artifact. Do not copy or merge it into `runtime-data/axiom.sqlite` as part of startup.
3. The symbol-scoped fill-ID migration is an implementation-owned, idempotent startup migration only. Do not run ad hoc SQL, hand-edit fill IDs, merge execution ledgers, or treat migration as permission to import orders/fills from another venue or database.
4. If authenticated validation is approved, create a separately owned TESTNET credential reference in the `AXIOM-BINANCE-SPOT` namespace for `binance-dev`/`BINANCE_SPOT_TESTNET`, with only `USER_DATA` and `TRADE`; verify `WITHDRAWAL` and `TRANSFER` are disabled. Never place a primary-wallet or unrelated venue secret in this namespace.
5. Run a new, explicitly approved TESTNET harness with injected credentials and an injected exact TESTNET venue. First perform read-only connectivity/account/order-query checks, then validation-only `POST /api/v3/order/test`; capture UTC/PHT audit records and confirm no arbitrary endpoint or permission path is reachable.
6. Only after a written go/no-go review may an operator exercise a bounded TESTNET order lifecycle. Verify LIMIT IOC and FOK behavior, partial/expired outcomes, exact symbol-scoped fill ingestion, fee conversion, cancellation ownership, reconciliation after restart, timeout/5xx/ambiguous `UNKNOWN`, account-epoch reset, crash-before-send authoritative absence reset/pause, and kill-generation fencing. Never blind-retry UNKNOWN.
7. Review evidence, logs, action audit, reservations (including symbol-scoped SELL base reservations), positions, and DB boundaries. Keep the TESTNET process, keyring reference, and database separate from all LIVE resources. A failed or incomplete gate returns to PAPER/DISABLED.
8. A future LIVE rollout requires a new production-specific design and separately reviewed profile/process/database/keyring policy. It is not achieved by changing this development environment, reusing its runtime identity, or selecting `BINANCE_SPOT_LIVE`; direct LIVE activation from this development slice is prohibited.

The migration is complete only when the review record, evidence, permission review, rollback/kill procedure, redaction review, and resource-isolation checks are approved. Until then, this document describes an isolated PAPER development vertical slice with public market-data evidence—not live trading.

## Explicit prohibitions

- No automatic merge, automatic rollout, or implicit database migration.
- No ad hoc execution-ledger edits, cross-symbol fill-ID reuse, or cross-database copy.
- No use of the main AXIOM port `8080` for `binance-dev`.
- No reuse of a LIVE runtime database, keyring, credentials, or process.
- No blind retry of `UNKNOWN`, timeout, 5xx, or ambiguous order/cancel outcomes.
- No withdrawal, transfer, deposit, account-management, or arbitrary endpoint permission.
- No direct LIVE activation or claim that this slice has authenticated TESTNET/LIVE coverage.
- No claim that public ticker/book observations are exchange fills or real trades.
