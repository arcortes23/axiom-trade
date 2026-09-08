# Binance Spot Testnet canary operator guide

**Status:** strict Binance Spot **TESTNET** cutover. This guide is the operator boundary for the isolated Testnet runtime; it is not a production/mainnet runbook.

The supported path is the `binance-testnet` CLI backed by `BinanceTestnetRuntime`. It owns a dedicated runtime identity, database, keyring entry, dashboard, gate, probe ledger, and audit records. The runtime fixes the Binance origin to `https://testnet.binance.vision`; an operator cannot select another origin or environment.

> **Current credential status:** **NOT CONFIGURED**. No authenticated connectivity, order validation, execution probe, reconciliation, or autonomous window has been executed for this checkout. Until the operator completes credential configuration and the status gate reports configured, the connectivity, validation, probe, and auto steps below remain unexecuted—not passed, skipped as successful, or implied by public market data. No credentials, orders, fills, or authenticated Testnet evidence are claimed here.

## Safety boundary and fixed resources

The Testnet runtime is intentionally separate from the PAPER runtime and from every other AXIOM process:

| Resource | Fixed Testnet value |
| --- | --- |
| Environment | `BINANCE_SPOT_TESTNET` |
| Binance REST origin | `https://testnet.binance.vision` (fixed; not operator-configurable) |
| Runtime identity | `binance-testnet` |
| Dashboard | `http://127.0.0.1:8082` |
| Database | `runtime-data/binance-testnet.sqlite` |
| Credential instance | `binance-testnet` |
| Credential namespace | `AXIOM-BINANCE-SPOT-TESTNET` |
| Credential storage | OS keyring; secret values are never printed or persisted in the runtime projection |
| Runtime tables | `binance_testnet_*` gate/probe tables, separate from research and autonomous execution tables |
| Browser auto enable | Not supported; autonomous enablement is CLI-only |

The process must run from this checkout with its Windows virtual environment. Do not copy its database, lock, PID, stop marker, keyring entry, or audit records into another runtime. Do not point it at another database. Do not place credentials in a command line, source file, `.env` file, log, screenshot, browser form, or shell history.

There is no arbitrary-origin option. There is no production/mainnet execution route in this operator flow. The Testnet allowlist does not include SAPI, withdrawals, transfers, deposits, account-management operations, or arbitrary endpoints.

### Bounded risk envelope

The Testnet execution boundary uses the immutable `binance-spot-risk-v1` envelope, in USDT:

| Limit | Default |
| --- | ---: |
| Entry all-in cap | `10.00 USDT` |
| Maximum aggregate exposure | `30.00 USDT` |
| Maximum reserved exposure | `30.00 USDT` |
| Realized-loss entry stop | `5.00 USDT` |
| Equity-loss entry stop | `5.00 USDT` |
| Maximum open positions | `5` |
| Maximum submissions per UTC day | `20` (entries and exits) |
| Maximum execution deviation | `100 bps` |

Only supported LIMIT IOC/FOK behavior is eligible. Exchange `exchangeInfo` filters are checked for symbol status, Spot permission, price/quantity steps and bounds, notional limits, percent-price bands, position limits, and order-count capacity. Missing, stale, malformed, or incomplete evidence fails closed.

## Required operator sequence

Run these steps in order. A gate that does not report the required result is a stop condition; do not continue to the next command.

### 1. Configure Testnet credentials (hidden prompts)

From PowerShell in the repository root:

```powershell
.\.venv\Scripts\python.exe -m axiom.cli binance-credentials configure --environment testnet
```

The command prompts for the Testnet API key and secret without echoing them and stores them only in the OS keyring entry identified by `AXIOM-BINANCE-SPOT-TESTNET` / `binance-testnet`. Use a separately owned Testnet key with only the required `USER_DATA` and `TRADE` permissions. The key must not have withdrawal or transfer permission. Never paste a secret into this document or into a command.

If configuration is canceled, incomplete, or rejected, stop. Do not attempt network commands.

### 2. Confirm safe credential status

```powershell
.\.venv\Scripts\python.exe -m axiom.cli binance-credentials status --environment testnet
```

The output is metadata only. It must identify `BINANCE_SPOT_TESTNET` and `AXIOM-BINANCE-SPOT-TESTNET`, report `configured: true`, and report `secret_values_exposed: false`. A safe unconfigured output has this shape:

```json
{
  "configured": false,
  "environment": "BINANCE_SPOT_TESTNET",
  "namespace": "AXIOM-BINANCE-SPOT-TESTNET",
  "secret_values_exposed": false
}
```

The current checkout is in that unconfigured state. If status is `configured: false`, or the environment/namespace is not exact, **connectivity, validation, probe, and auto remain unexecuted**. Do not treat a blocked result as a successful check and do not continue.

### 3. Run the explicit authenticated connectivity gate

Only after status reports `configured: true`:

```powershell
.\.venv\Scripts\python.exe -m axiom.cli binance-testnet connectivity
```

Require a secret-free `status: "PASS"` for the fixed Testnet environment, server-time check, and authenticated account-read check. `BLOCKED`, `UNKNOWN`, credential errors, time-skew/signature errors, permission errors, transport errors, or any environment/origin mismatch stop the sequence. This command is read-only; it does not submit an order.

With credentials unavailable, the command returns a blocked credential reason and does not construct the authenticated network/execution graph. That is the expected current outcome, not evidence of connectivity.

### 4. Run order validation (validation-only)

Only after connectivity is `PASS`:

```powershell
.\.venv\Scripts\python.exe -m axiom.cli binance-testnet validate
```

An optional symbol may be supplied only when deliberately reviewing one current USDT Spot symbol:

```powershell
.\.venv\Scripts\python.exe -m axiom.cli binance-testnet validate --symbol BTCUSDT
```

Require `status: "PASS"`, a current `TRADING` Spot symbol, compliant filters, a viable bounded quantity, and a viable planned exit. Validation uses Binance's test-order endpoint only; it is explicitly `validation_only` and is not proof that an order was placed. `REJECTED`, `BLOCKED`, `UNKNOWN`, stale market data, missing filters, or a non-viable planned exit stops the sequence.

### 5. Run the explicitly confirmed execution probe

Only after validation is `PASS`, and only after reviewing the computed symbol, price, quantity, notional, fee reserve, and risk projection, run the exact phrase below. The phrase is case- and whitespace-sensitive:

```powershell
.\.venv\Scripts\python.exe -m axiom.cli binance-testnet probe --confirmation "RUN BINANCE TESTNET EXECUTION PROBE"
```

The probe is the sole explicit bounded Testnet execution-probe step. It uses a deterministic `AXIOM-TESTNET-PROBE-...` client ID, a LIMIT order, the fixed risk envelope, and the fixed Testnet venue. It is not an autonomous enablement and it is not production/mainnet execution. A probe may create a Testnet order only when all preceding gates pass and the exact confirmation is supplied.

The command must report its resulting probe state. `ACKNOWLEDGED`, `FILLED`, `PARTIALLY_FILLED`, `CANCELED`, `EXPIRED`, `REJECTED`, or `UNKNOWN` are evidence states—not permission to submit a second probe. If the result is `UNKNOWN`, stop and reconcile by the persisted client ID; never rerun the probe to discover what happened.

### 6. Reconcile the persisted probe

After the probe returns, and before any later probe or autonomous action:

```powershell
.\.venv\Scripts\python.exe -m axiom.cli binance-testnet reconcile
```

Reconciliation queries the owned Testnet order using the exact persisted AXIOM client ID (and the exchange order ID once known), verifies symbol/client-order identity, retrieves authoritative `myTrades` evidence, and updates the isolated probe ledger. It is the required path for `UNKNOWN`, timeout, HTTP 5xx, rate-limit, and Binance ambiguous `-1000`, `-1006`, or `-1007` outcomes. Those outcomes are never blindly retried. A missing or contradictory authoritative response remains unresolved and keeps the reservation held.

If reconciliation detects a Testnet reset or cannot prove the order/fills, stop with the control plane paused. Do not infer a fill from an order status alone, cancel an unknown order blindly, or submit a replacement.

### 7. Serve the isolated dashboard

After the required gates and reconciliation review, serve the local, secret-free dashboard:

```powershell
.\.venv\Scripts\python.exe -m axiom.cli binance-testnet serve
```

Open only `http://127.0.0.1:8082` if a browser view is needed. The dashboard reports fixed Testnet identity, credential status (metadata only), connectivity/validation/probe status, bounded risk projections, orders/fills, unresolved orders, reconciliation, worker state, and audit actions. It does not display secrets. Browser controls cannot enable or resume autonomous Testnet execution; autonomous enablement is CLI-only.

For a readiness bind-and-stop smoke of the dashboard, without leaving a server running:

```powershell
.\.venv\Scripts\python.exe -m axiom.cli binance-testnet serve --once
```

`serve --once` is not a connectivity or order gate. It must not be used to infer authenticated success.

## Optional bounded autonomous window

Autonomous Testnet work is a separate, later gate. It is permitted only after:

1. credential status is configured and secret-free;
2. connectivity is `PASS`;
3. order validation is `PASS`;
4. any probe has been reconciled, with no unresolved `UNKNOWN` intent or held reservation requiring operator action;
5. the operator has reviewed the current selection, market evidence, risk envelope, and audit state; and
6. the operator has an explicit written go/no-go decision for this bounded window.

Run no browser enable action. Use the exact CLI confirmation and an integer window from 30 through 900 seconds inclusive:

```powershell
.\.venv\Scripts\python.exe -m axiom.cli binance-testnet auto --confirmation "ENABLE BINANCE TESTNET AUTO CANARY" --window-seconds <30-900>
```

Replace `<30-900>` with a concrete integer, for example `30` or `300`; do not pass the angle brackets literally. The exact confirmation is case- and whitespace-sensitive. The window is supervised, foreground, bounded, and not a background daemon. The runtime enforces the deadline and risk gates and returns structured evidence including `cycles_started`, `cycles_completed`, `deadline_at`, `finished_at`, lock ownership/release, and cleanup result. Missing credentials, failed gates, stale selection/evidence, a changed credential fingerprint, or any risk/reconciliation blocker leaves auto unexecuted or blocked.

When authorization was attempted, auto always attempts the stop sequence **PAUSE, then DISARM**, verifies terminal `DISARMED`, releases the Testnet lock, and reports the cleanup/terminal state. A failed cleanup is not success: treat it as an incident, keep the process stopped, and verify status before any further action.

## Stop sequence and expected outputs

Use this sequence for an operator stop or at the end of a bounded run:

1. For `serve`, press `Ctrl+C` in the same PowerShell window. The runtime sets its stop event, writes its owned stop marker, stops the worker and dashboard, closes resources, releases the lock, and records an internal `status: "STOPPED"` result. An interrupt may end the foreground command before it prints a JSON payload; do not infer success from the terminal exit alone. Confirm the next status invocation and do not delete another process's files.
2. For `auto`, wait for the bounded window to finish unless an emergency stop is required. Its structured result must include the attempted cleanup; after authorization the cleanup must show `PAUSE` followed by `DISARM`, `terminal_state: "DISARMED"`, and a released lock. If the process is interrupted or cleanup is not verified, stop using the same emergency procedure and inspect status.
3. For a safety event, first issue the operator **PAUSE** action through the Testnet control plane when available, then **DISARM**. PAUSE blocks new entries while reconciliation and risk-reducing handling continue; DISARM returns the plane to non-submitting `DISARMED`. Do not use DISARM as a re-enable and do not silently liquidate positions.
4. Re-check secret-free status:

```powershell
.\.venv\Scripts\python.exe -m axiom.cli binance-testnet status
```

A fresh `binance-testnet status` invocation creates an idle runtime and normally reports `status: "READY"` after the prior server has stopped; the stop finalizer itself records `status: "STOPPED"` and removes the owned lock/PID resources while retaining the stop marker. The status projection must continue to show `strict_testnet: true`, the fixed Testnet identity, `no_mainnet: true`, `no_sapi: true`, and no secret values. If an order is `UNKNOWN` or reconciliation is incomplete, leave the reservation and ledger intact and escalate for reconciliation rather than removing files or retrying.

Do not issue ad hoc SQL, edit execution rows, remove the lock/PID/stop files by hand, or copy records between PAPER and Testnet databases.

## Failure and reconciliation rules

The durable probe lifecycle is monotonic:

```text
INTENT -> RESERVED -> SUBMITTING -> ACKNOWLEDGED -> FILLED / CANCELED / EXPIRED / REJECTED
                              \-> UNKNOWN / PARTIALLY_FILLED
```

A timeout, HTTP 5xx, network exception, generation change, rate-limit result, ambiguous cancellation, or Binance `-1000`/`-1006`/`-1007` result is `UNKNOWN`. It is not permission to retry. Reconcile using the exact persisted client ID, then require authoritative order identity and symbol-scoped trade evidence. `FILLED` without durable trade rows remains unresolved. A crash before or after the venue call is handled by reconciliation; it never authorizes a second submission.

A changed keyring value or credential fingerprint fails closed. The runtime does not silently replace the venue/client with a new credential set. If authoritative order identity or symbol-scoped trade history needed to prove previously owned inventory is missing after a reset, the control plane pauses with a reset reason and keeps risk reservations held until review.

The probe and autonomous ledgers are isolated from research and PAPER evidence. Public market data, research rankings, forward-paper results, and dashboard observations are not exchange fills and never authorize a Testnet order. The Polymarket transport is not part of this runtime.

## Separate PAPER runtime (legacy development path)

PAPER remains available only as a separate, unauthenticated development runtime. It is not a preparation shortcut for Testnet and it cannot read Binance credentials or send authenticated requests:

```powershell
.\.venv\Scripts\python.exe -m axiom.cli binance-dev --once
```

Omit `--once` only when intentionally keeping the PAPER dashboard running:

```powershell
.\.venv\Scripts\python.exe -m axiom.cli binance-dev
```

PAPER uses runtime identity `binance-dev`, database `runtime-data/binance-dev.sqlite`, and dashboard `http://127.0.0.1:8081`. Its local `PaperBinanceSpotVenue` is deterministic and has no network transport. Keep all PAPER databases, logs, locks, and evidence separate from `binance-testnet`; never reinterpret PAPER fills as Testnet fills.

## API boundary and pinned official baseline

The implementation pins the Binance Spot API documentation baseline to commit [`041bba2d8a0bb8d26f77b88a0e2761743233fcf7`](https://github.com/binance/binance-spot-api-docs/commit/041bba2d8a0bb8d26f77b88a0e2761743233fcf7). The recorded baseline dates are Spot REST `2026-09-02` and Spot Testnet `2026-09-04`; dates are documentation metadata, not evidence of an authenticated run.

Pinned official references:

- [Spot REST API baseline](https://github.com/binance/binance-spot-api-docs/blob/041bba2d8a0bb8d26f77b88a0e2761743233fcf7/rest-api.md) — request security, timing, error semantics, account/order/query/trade/rate-limit/cancel behavior.
- [Spot Testnet REST API baseline](https://github.com/binance/binance-spot-api-docs/blob/041bba2d8a0bb8d26f77b88a0e2761743233fcf7/testnet/rest-api.md) — fixed Testnet origin and security semantics.
- [Spot REST errors baseline](https://github.com/binance/binance-spot-api-docs/blob/041bba2d8a0bb8d26f77b88a0e2761743233fcf7/errors.md) — pinned REST error codes and failure semantics.
- [Spot Testnet errors baseline](https://github.com/binance/binance-spot-api-docs/blob/041bba2d8a0bb8d26f77b88a0e2761743233fcf7/testnet/errors.md) — pinned Testnet error behavior.
- [Spot REST changelog baseline](https://github.com/binance/binance-spot-api-docs/blob/041bba2d8a0bb8d26f77b88a0e2761743233fcf7/CHANGELOG.md) — pinned root API changes.
- [Spot Testnet changelog baseline](https://github.com/binance/binance-spot-api-docs/blob/041bba2d8a0bb8d26f77b88a0e2761743233fcf7/testnet/CHANGELOG.md) — pinned Testnet changes and baseline date.
- [Symbol and exchange filters baseline](https://github.com/binance/binance-spot-api-docs/blob/041bba2d8a0bb8d26f77b88a0e2761743233fcf7/filters.md) — price, quantity, notional, percent-price, position, and order-count filters.
- [User Data Streams baseline](https://github.com/binance/binance-spot-api-docs/blob/041bba2d8a0bb8d26f77b88a0e2761743233fcf7/user-data-stream.md) — documented stream semantics; this runtime uses polling and does not treat a stream as a recovery path.

The authenticated allowlist is limited to account/readiness, order validation, owned order query, open/all orders, `myTrades`, order-rate limits, and owned-order cancellation. It excludes SAPI, transfers, withdrawals, deposits, account-management APIs, arbitrary URLs, and unsupported order forms. No browser action, public ticker/book observation, PAPER result, or research result changes that boundary.

## Operator prohibitions

- Do not continue past a failed or unconfigured gate.
- Do not claim authenticated connectivity, validation, probe, auto, orders, fills, or trades while credentials are unavailable; the current status is **NOT CONFIGURED**.
- Do not disclose, log, copy, or put API secrets in command lines, files, screenshots, browser fields, or reports.
- Do not use an arbitrary origin, alter the fixed Testnet profile, or reuse another runtime's database/keyring/process.
- Do not blind-retry `UNKNOWN`, HTTP 5xx, timeout, rate-limit, or Binance `-1000`/`-1006`/`-1007` outcomes.
- Do not cancel an unknown order or infer a fill without client-ID reconciliation and authoritative trade evidence.
- Do not use SAPI, transfer, withdrawal, deposit, account-management, or arbitrary endpoints.
- Do not enable autonomous execution from a browser; use the exact CLI phrase and a 30..900-second foreground window only after every gate passes.
- Do not treat PAPER or public market-data observations as Testnet execution evidence.
