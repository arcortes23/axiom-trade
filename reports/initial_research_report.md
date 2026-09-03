# AXIOM Initial Research Report

Generated from the public historical workflow at `2026-09-03T03:07:49.697329+00:00` UTC.

Command:

```text
python -m axiom.cli historical --markets 20 --timeout 10 --output reports/initial_research.json --db reports/axiom_final.sqlite
```

The machine-readable artifact is `reports/initial_research.json`; the SQLite provenance and experiment database is `reports/axiom_final.sqlite`.

## Executive conclusion

No strategy is promoted. All eight BTC/USDT candidates were rejected by the locked-holdout fitness gate. Positive raw holdout returns occurred for seven candidates, but every candidate had non-positive penalized locked-holdout fitness after train-to-validation and validation-to-holdout degradation penalties. These results are not evidence of executable profitability.

The prediction-market study is descriptive only. It evaluates the market's own last observed price as a calibration baseline, not an independent model or LLM opinion. The sample is low quality because this workflow has historical prices but no historical order-book depth, spread, or fill records. Live execution remains disabled.

## Crypto study

| Field | Result |
|---|---:|
| Provider | Binance |
| Instrument | BTC/USDT |
| Bars | 1,000 daily OHLCV bars |
| Dataset version | `sha256:11aeb1f2388dc407d91b61e5ad5e07d01b94aaa9e83cf875bbded8b3418d3636` |
| Simulation quality | MEDIUM |
| Chronological split | 60% train / 20% validation / 20% locked holdout |
| Initial cash | 10,000 |
| Fee assumption | 10 bps |
| Slippage assumption | 5 bps |
| Allocation assumption | 50% |

Execution uses next-bar OHLCV prices. Binance OHLCV does not provide historical order-book depth in this workflow, so the simulation quality is MEDIUM rather than HIGH.

| Strategy family | Holdout total return | Penalized fitness | Decision |
|---|---:|---:|---|
| dip | 5.7781% | -2.266094 | Rejected |
| momentum | 3.2258% | -2.421734 | Rejected |
| trend | -2.5464% | -2.700871 | Rejected |
| mean reversion | 0.9035% | -3.069743 | Rejected |
| breakout | 6.5311% | -2.410553 | Rejected |
| volatility | 1.1139% | -116.605934 | Rejected |
| rsi | 8.0963% | -2.299751 | Rejected |
| volume filter | 5.1472% | -2.890322 | Rejected |

All eight candidates were rejected for `non-positive locked-holdout fitness`. The highest raw holdout return was RSI at 8.0963%; its penalized fitness was still -2.299751. The lowest raw holdout return was trend at -2.5464%.

The large volatility-family penalty is an explicit consequence of its train/validation degradation, not a hidden failure or suppressed result.

## Prediction-market study

| Field | Result |
|---|---:|
| Provider | Polymarket |
| Markets requested | 20 |
| Markets seen | 20 |
| Markets with history | 20 |
| Resolved market observations | 20 |
| Simulation quality | LOW |
| Model version | `market-price-baseline-v1` |

### Calibration of last observed market price

| Metric | Value |
|---|---:|
| Observations | 20 |
| Brier score | 0.0005002375 |
| Log loss | 0.0057431446 |
| ECE | 0.0054750 |

These metrics measure the last observed market price against the recorded binary outcome. They do not measure an independent forecasting model.

### Historical price buckets

Bucket counts are price-history observations, not independent bets. The reported ROI is the unfilled price-proxy quantity `(outcome - price) / price`; it excludes the unavailable historical spread, depth, and actual fills.

| Price bucket | Observations | Wins | Win rate | Mean price | Mean price-proxy ROI |
|---|---:|---:|---:|---:|---:|
| 0–1c | 28 | 0 | 0.00% | 0.002250 | -100.00% |
| 1–2c | 0 | 0 | 0.00% | 0.000000 | 0.00% |
| 2–5c | 7 | 0 | 0.00% | 0.028714 | -100.00% |
| 5–10c | 3 | 0 | 0.00% | 0.068833 | -100.00% |
| 10–20c | 3 | 0 | 0.00% | 0.143667 | -100.00% |
| 20–50c | 17 | 0 | 0.00% | 0.382353 | -100.00% |
| 50–100c | 96 | 72 | 75.00% | 0.716333 | 7.3347% |

The bucket table is useful for exploratory diagnostics only. It cannot establish a tradable edge because it does not model executable quotes, historical liquidity, spread crossing, order-book impact, or selection dependence among adjacent price-history points.

The time-to-resolution diagnostic recorded 20 observations in the under-one-day bucket with mean price-proxy ROI -49.4219%; the source itself labels expiry timing as an approximation based on the last observed history point and market expiry. It is not used as a promotion decision.

## Risk and safety gates

- The locked holdout is evaluated only after strategy construction and train/validation comparison.
- Degradation and overfit penalties are included in the recorded fitness.
- Cost assumptions are explicit in the crypto experiment records.
- Prediction sizing and risk checks are uncertainty-aware and support drawdown, CVaR, exposure, correlation-group, and kill-switch gates.
- Hermes is schema-only and rejects unsafe execution permissions.
- Paper trading is the only trading mode; live execution raises `LiveExecutionDisabled`.

## Limitations and next data requirements

1. Binance OHLCV lacks historical order-book depth in this run; next-bar execution is an approximation.
2. Polymarket historical data available to this workflow is price-only; historical spread, depth, and fills are unavailable.
3. The prediction sample is selected from a provider catalog and may be small or non-independent.
4. Adjacent prediction-market history points are not independent bets.
5. The market-price calibration baseline is not an LLM or independent probability model.
6. The initial study does not justify capital deployment or a profitability claim.

The next evidence upgrade is a timestamped, immutable dataset containing historical executable order-book snapshots/trades and a pre-registered out-of-sample protocol before any candidate can be considered for paper promotion.
