# AXIOM operational database

## Canonical runtime database

Normal AXIOM operation uses one SQLite database:

```text
runtime-data/axiom.sqlite
```

Commands and PowerShell lifecycle scripts use that path when no database
argument is supplied. An explicit `--db` or `-DbPath` remains an intentional
override. AXIOM never merges databases implicitly.

Previous Phase 3, Phase 4, and Phase 4.2 databases, including names such as
`runtime-data/axiom_phase3.sqlite`, `runtime-data/axiom_phase4.sqlite`, and
`runtime-data/axiom_phase42.sqlite`, are development artifacts. They remain
separate for historical inspection; production-like paper operation should
use only `runtime-data/axiom.sqlite`. Merge or copy data only as an explicit,
reviewed migration outside the normal startup commands.

## Normal operation examples

```powershell
python -m axiom.cli node-run --cycles 0 --disable-research --disable-mutations
python -m axiom.cli dashboard
python -m axiom.cli dataset-catalog --status
python -m axiom.cli research-summary
python -m axiom.cli submit-proposal --proposal '{"proposal_id":"proposal-example","statement":"<one falsifiable statement>","source":"<public source or immutable AXIOM result>","tests":["<bounded chronological test>"],"dataset_version":"<immutable version>","time_split":"train-validation-holdout","paper_only":true,"experiment_plan":{"schema_version":"1","market_type":"prediction","template":"probability_mispricing","dataset_version":"<immutable version>","max_variants":4,"min_samples":30,"paper_only":true}}'
```

## Micro-live Polymarket canary

Production live trading remains disabled. The optional `LIVE_CANARY` path uses
the current official Polymarket unified Python SDK and CLOB V2 semantics only.
Use a dedicated Polymarket wallet containing only the small amount intended for
AXIOM canary testing. Never provide a primary-wallet private key.

Credentials are stored through Windows Credential Manager via the OS keyring:

```powershell
python -m axiom.cli credentials configure polymarket
python -m axiom.cli credentials status
```

Environment variables are supported only when `--allow-environment` is passed.
They are `POLYMARKET_PRIVATE_KEY`, `POLYMARKET_WALLET_ADDRESS`,
`POLYMARKET_RELAYER_API_KEY`, and `POLYMARKET_RELAYER_API_KEY_ADDRESS`.
Credential values never enter SQLite, dashboard JSON, reports, or logs.

```powershell
python -m axiom.cli canary-check --candidate <candidate-id> --market <market-id> --token <token-id>
python -m axiom.cli canary-arm --venue polymarket --candidate <candidate-id> --target-notional-usd 1.00 --expires-hours 24
python -m axiom.cli canary-status
python -m axiom.cli canary-disarm
python -m axiom.cli canary-kill
```

`canary-check` is no-order. `canary-kill` prevents further submissions
immediately. An expired arm returns to paper-only automatically.

PowerShell lifecycle commands use the same default:

```powershell
.\ops\start_axiom_node.ps1
.\ops\status_axiom_node.ps1
.\ops\restart_axiom_node.ps1
.\ops\stop_axiom_node.ps1
```

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
.\ops\status_axiom_node.ps1 -DbPath runtime-data/axiom_phase42.sqlite
```
