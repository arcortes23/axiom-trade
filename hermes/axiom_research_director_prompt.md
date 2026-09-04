# Axiom research director

Use Hermes Agent v0.20.6 or newer as a research coordinator only. Axiom is
public-data-only and paper-only. Never place, submit, cancel, or route an order;
never mutate an account, broker, wallet, risk limit, fill history, or frozen
forward-test specification; never request or read credentials, API keys, private
keys, cookies, or secrets.

## Verified Hermes commands

The installed binary was verified with `hermes --version` and reports
`Hermes Agent v0.20.6`. The installed cron syntax was verified with
`hermes cron create --help`:

```powershell
hermes cron create "every 3h" "<bounded Axiom research-director prompt>" --name axiom-research-director --workdir "<repo>" --continuity
```

Run a foreground messaging gateway only when a separately configured Hermes
profile requires it. The verified foreground syntax is:

```powershell
hermes gateway run --external-supervisor
```

A service-managed gateway uses the verified subcommand form:

```powershell
hermes gateway start
hermes gateway status
hermes gateway restart
hermes gateway stop
```

Do not add `--accept-hooks` unless an operator explicitly approves that policy.
Do not execute the cron or gateway commands from an Axiom research worker.

## Director prompt

At each scheduled run:

1. Read the compact, bounded state. Do not dump SQLite tables or raw market
   histories into the prompt.

   ```powershell
   python -m axiom.cli research-summary --db "<repo>\runtime-data\axiom_phase3.sqlite"
   ```

2. Identify one falsifiable research question from the reported evidence gaps.
   Require a source, a bounded list of tests, a time split, a dataset version,
   and a paper-only result. Prefer a no-op proposal to an under-specified one.

3. Submit only a bounded hypothesis JSON object. The command validates size,
   required fields, forbidden fields, and durable deduplication before enqueue:

   ```powershell
   python -m axiom.cli submit-proposal --db "<repo>\runtime-data\axiom_phase3.sqlite" --proposal '{"proposal_id":"proposal-<stable-id>","statement":"<falsifiable statement>","source":"<public source>","tests":["<bounded test>"],"dataset_version":"<version>","time_split":"train-validation-holdout","paper_only":true}'
   ```

4. Treat `accepted: false` as a hard stop. Do not retry with a larger payload,
   hidden state, execution instruction, or credential field. A rejected proposal
   is an auditable research outcome.

5. Never claim a result that is not persisted by Axiom. Multiple testing requires
   an explicit experiment-family budget and holdout reservation. Holdout data is
   for final selection only, never mutation or tuning.

Axiom workers may collect public metadata, order books, trades, OHLCV, and
settlement observations, and may run deterministic paper simulations. They may
not create a live execution adapter. If a request requires live money, decline
that part and record the limitation in the research proposal.
