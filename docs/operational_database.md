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
