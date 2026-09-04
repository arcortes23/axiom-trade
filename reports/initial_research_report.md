# Axiom Research Report

Generated: 2026-09-04T06:19:54.585904+00:00

## Crypto

- **bars:** 1000
- **benchmarks:** 3 entries
- **dataset_version:** sha256:e0ec5b714c172b69ac4b8d4fa8725db88d224c74408c80d70caa368dc315c9b9
- **errors:** 0 entries
- **experiments:** 8 entries
- **instrument:** BTC/USDT
- **instrument_metadata:** {"base_asset":"BTC","category":null,"contract_size":1.0,"currency":"USDT","expiry":null,"extra":{"isSpotTradingAllowed":true,"permissions":[],"quoteOrderQtyMarketAllowed":true,"status":"TRADING"},"lot_size":1e-05,"market_id":null,"market_type":"crypto_spot","provider":"binance","question":null,"quote_asset":"USDT","resolution_criteria":null,"symbol":"BTCUSDT","tags":[],"tick_size":0.01}
- **instrument_metadata_available:** True
- **limitations:** 3 entries
- **market_type:** crypto_spot
- **provider:** binance
- **simulation_quality:** MEDIUM

## Prediction markets

- **benchmarks:** 3 entries
- **calibration:** {"brier":2.499999999999697e-07,"ece":0.0004999999999999698,"log_loss":0.0005001250416822429,"observations":20}
- **errors:** 0 entries
- **historical_order_books_available:** False
- **independence_method:** unique event IDs or normalized question and expiry
- **independent_resolved_markets:** 20
- **limitations:** 5 entries
- **liquidity:** {"mean_absolute_error":0.0,"note":"liquidity association is descriptive; historical depth was unavailable","observations":0}
- **market_type:** prediction
- **markets_requested:** 20
- **markets_with_history:** 20
- **model_version:** market-price-baseline-v1
- **multi_horizon_calibration:** {"1d":{"brier":0.0,"count":0,"ece":0.0,"horizon_seconds":86400,"log_loss":0.0},"30d":{"brier":0.0,"count":0,"ece":0.0,"horizon_seconds":2592000,"log_loss":0.0},"7d":{"brier":0.0,"count":0,"ece":0.0,"horizon_seconds":604800,"log_loss":0.0}}
- **price_buckets:** {"0-1c":{"count":18,"lower":0.0,"mean_price":0.0007500000000000003,"mean_roi":-1.0,"resolved_count":18,"upper":0.01,"win_rate":0.0,"wins":0},"1-2c":{"count":0,"lower":0.01,"mean_price":0.0,"mean_roi":0.0,"resolved_count":0,"upper":0.02,"win_rate":0.0,"wins":0},"10-20c":{"count":0,"lower":0.1,"mean_price":0.0,"mean_roi":0.0,"resolved_count":0,"upper":0.2,"win_rate":0.0,"wins":0},"2-5c":{"count":41,"lower":0.02,"mean_price":0.02295121951219511,"mean_roi":-1.0,"resolved_count":41,"upper":0.05,"win_rate":0.0,"wins":0},"20-50c":{"count":4,"lower":0.2,"mean_price":0.34375,"mean_roi":-0.16666666666666663,"resolved_count":4,"upper":0.5,"win_rate":0.25,"wins":1},"5-10c":{"count":1,"lower":0.05,"mean_price":0.05,"mean_roi":-1.0,"resolved_count":1,"upper":0.1,"win_rate":0.0,"wins":0},"50-100c":{"count":91,"lower":0.5,"mean_price":0.7325274725274723,"mean_roi":0.3385024246835296,"resolved_count":91,"upper":1.0,"win_rate":0.9010989010989011,"wins":82}}
- **provider:** polymarket
- **repricing:** {"markets":7,"mean_reversion_fraction":0.0,"note":"adjacent price changes are not independent bets"}
- **research_quality:** PRICE_PROXY
- **resolved_market_groups:** 20
- **simulation_quality:** MEDIUM
- **time_to_resolution:** {"buckets":{"1d_7d":{"count":0.0,"mean_roi":0.0},"7d_30d":{"count":0.0,"mean_roi":0.0},"over_30d":{"count":0.0,"mean_roi":0.0},"under_1d":{"count":20.0,"mean_roi":-0.44972486243121557}},"mean_roi":-0.4497248624312156,"mean_seconds_to_expiry":0.0,"note":"last observed price and expiry are price-history approximations","observations":20}

## Limitations

- Binance OHLCV does not provide historical order-book depth in this workflow.
- Next-bar OHLCV execution is an approximation; results are not a live-profit claim.
- Candidate selection and rejection use validation only; locked holdout values are report-only.
- Public Polymarket histories are price-only unless a source supplies timestamped depth; this report labels such samples PRICE_PROXY.
- The independent-market count is a deduplicated event/question/expiry grouping proxy, not a statistical independence claim.
- Market price is used only as a calibration baseline; no LLM opinion is used as a probability model.
- Bucket ROI is descriptive and does not establish executable profitability.
- Historical order-book execution requires timestamped books; current books are never backfilled into history.
