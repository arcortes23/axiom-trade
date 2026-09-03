"""Reproducible historical research workflows for the initial Axiom study.

The workflows are intentionally conservative: they report data coverage and
simulation quality, keep train/validation/holdout separate, and never label a
small or price-only sample as proof of profitability.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
from statistics import mean
from typing import Any, Iterable, Mapping, Sequence

from .backtest import CryptoBacktester
from .data import BinanceAdapter, PolymarketAdapter
from .domain import MarketType, PredictionMarketSnapshot, SettlementState, SimulationQuality, to_record
from .evaluation import evaluate_scores, split_dataset
from .regime import RegimeEngine
from .storage import AxiomStore
from .strategy import validate_strategy
from .tracking import ExperimentTracker


_BUCKETS: tuple[tuple[str, float, float], ...] = (
    ("0-1c", 0.00, 0.01),
    ("1-2c", 0.01, 0.02),
    ("2-5c", 0.02, 0.05),
    ("5-10c", 0.05, 0.10),
    ("10-20c", 0.10, 0.20),
    ("20-50c", 0.20, 0.50),
    ("50-100c", 0.50, 1.00),
)


@dataclass(frozen=True, slots=True)
class ResearchReport:
    generated_at: datetime
    crypto: Mapping[str, Any]
    prediction: Mapping[str, Any]
    limitations: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "generated_at": self.generated_at.astimezone(timezone.utc).isoformat(),
            "crypto": _jsonable(self.crypto),
            "prediction": _jsonable(self.prediction),
            "limitations": list(self.limitations),
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True, indent=2)


def run_crypto_research(
    provider: Any | None = None,
    *,
    symbol: str = "BTC/USDT",
    start: datetime | None = None,
    end: datetime | None = None,
    initial_cash: float = 10_000.0,
    store: AxiomStore | None = None,
    timeout: float = 10.0,
) -> dict[str, Any]:
    """Backtest the requested deterministic crypto families on BTC/USDT."""
    provider = provider or BinanceAdapter(timeout=timeout)
    errors: list[str] = []
    try:
        bars = tuple(provider.historical_ohlcv(symbol, start=start, end=end, interval="1d"))
    except Exception as exc:  # network adapters must not abort the report
        bars = ()
        errors.append(f"crypto data error: {exc}")
    source_quality = _research_quality(provider, SimulationQuality.MEDIUM if bars else SimulationQuality.LOW)
    try:
        instrument_metadata = provider.metadata(symbol)
    except Exception as exc:
        instrument_metadata = None
        errors.append(f"crypto metadata error: {exc}")
    base: dict[str, Any] = {
        "market_type": MarketType.CRYPTO_SPOT.value,
        "provider": str(getattr(provider, "provider_name", provider.__class__.__name__)),
        "instrument": symbol,
        "instrument_metadata": to_record(instrument_metadata) if instrument_metadata is not None else None,
        "instrument_metadata_available": instrument_metadata is not None,
        "bars": len(bars),
        "dataset_version": "",
        "simulation_quality": source_quality.value,
        "experiments": [],
        "errors": errors,
    }
    if len(bars) < 6:
        base["limitations"] = ["fewer than six OHLCV bars; no train/validation/holdout result"]
        return base

    split, version = _chronological_split(bars)
    base["dataset_version"] = version
    if store is not None:
        try:
            with store.transaction():
                store.save_dataset(
                    f"crypto:{symbol.replace('/', '').replace('-', '').upper()}",
                    version,
                    bars,
                    metadata={
                        "provider": base["provider"],
                        "instrument": symbol,
                        "interval": "1d",
                        "instrument_metadata": base["instrument_metadata"],
                        "instrument_metadata_available": base["instrument_metadata_available"],
                    },
                    quality=_research_quality(provider, SimulationQuality.MEDIUM),
                )
                store.save_bars(symbol, bars, dataset_id=f"crypto:{symbol.replace('/', '').replace('-', '').upper()}", dataset_version=version)
        except ValueError as exc:
            errors.append(str(exc))

    families: tuple[tuple[str, dict[str, Any]], ...] = (
        ("dip", {"lookback": 14, "threshold": 0.03}),
        ("momentum", {"lookback": 14, "threshold": 0.03}),
        ("trend", {"fast": 10, "slow": 30, "threshold": 0.02}),
        ("mean_reversion", {"lookback": 20, "sigma": 2.0}),
        ("breakout", {"lookback": 20, "threshold": 0.02}),
        ("volatility", {"lookback": 20, "target": 0.02}),
        ("rsi", {"period": 14, "oversold": 30.0, "overbought": 70.0}),
        ("volume_filter", {"lookback": 20, "multiplier": 1.2}),
    )
    tracker = ExperimentTracker(store)
    experiments: list[dict[str, Any]] = []
    for family, parameters in families:
        definition = validate_strategy(
            {
                "version": 1,
                "market_type": MarketType.CRYPTO_SPOT.value,
                "family": family,
                "parameters": parameters,
                "strategy_id": f"btc-usdt-{family}",
            }
        )
        results = []
        for partition in (split.train, split.validation, split.holdout):
            result = CryptoBacktester(
                initial_cash=initial_cash,
                fee_bps=10.0,
                slippage_bps=5.0,
                allocation=0.50,
                symbol=symbol,
            ).run(partition, definition, symbol=symbol)
            results.append(result)
        scores = [result.metrics.get("total_return", 0.0) for result in results]
        evaluation = evaluate_scores(
            {"total_return": scores[0]},
            {"total_return": scores[1]},
            {"total_return": scores[2]},
            metric="total_return",
        )
        experiment_quality = _min_quality(source_quality, results[2].quality)
        record = tracker.track(
            definition.id,
            str(definition.version),
            provider=base["provider"],
            instrument=symbol,
            dataset_version=version,
            features=("open", "high", "low", "close", "volume"),
            model_version="deterministic-signal-v1",
            executable_prices={"fee_bps": 10.0, "slippage_bps": 5.0},
            regime=_regime_labels(bars),
            cost_assumptions={"fee_bps": 10.0, "slippage_bps": 5.0, "allocation": 0.50},
            metrics={
                "train_total_return": scores[0],
                "validation_total_return": scores[1],
                "holdout_total_return": scores[2],
                "train_fills": float(results[0].trades),
                "validation_fills": float(results[1].trades),
                "quality": experiment_quality.value,
            },
            fitness=evaluation.fitness,
            rejected=evaluation.fitness <= 0,
            rejection_reason="non-positive locked-holdout fitness" if evaluation.fitness <= 0 else None,
            created_at=bars[-1].timestamp,
        )
        experiments.append(
            {
                "strategy_id": definition.id,
                "strategy_version": definition.version,
                "quality": experiment_quality.value,
                "train": results[0].metrics,
                "validation": results[1].metrics,
                "holdout": results[2].metrics,
                "evaluation": {
                    "train_score": evaluation.train_score,
                    "validation_score": evaluation.validation_score,
                    "holdout_score": evaluation.holdout_score,
                    "degradation": evaluation.degradation,
                    "overfit_penalty": evaluation.overfit_penalty,
                    "fitness": evaluation.fitness,
                },
                "rejected": record.rejected,
                "rejection_reason": record.rejection_reason,
            }
        )
    base["experiments"] = experiments
    base["simulation_quality"] = _min_quality(
        source_quality,
        min((SimulationQuality(item["quality"]) for item in experiments), key=lambda item: _quality_rank(item.value), default=SimulationQuality.LOW),
    ).value
    base["limitations"] = [
        "Binance OHLCV does not provide historical order-book depth in this workflow.",
        "Next-bar OHLCV execution is an approximation; results are not a live-profit claim.",
    ]
    return base


def run_prediction_research(
    provider: Any | None = None,
    *,
    market_limit: int = 20,
    store: AxiomStore | None = None,
    timeout: float = 10.0,
) -> dict[str, Any]:
    """Study resolved Polymarket price buckets and calibration without LLM odds."""
    provider = provider or PolymarketAdapter(timeout=timeout)
    errors: list[str] = []
    try:
        markets = tuple(provider.markets(active=False))[: max(0, int(market_limit))]
    except Exception as exc:
        markets = ()
        errors.append(f"prediction catalog error: {exc}")
    base: dict[str, Any] = {
        "market_type": MarketType.PREDICTION.value,
        "provider": str(getattr(provider, "provider_name", provider.__class__.__name__)),
        "markets_requested": max(0, int(market_limit)),
        "markets_seen": len(markets),
        "markets_with_history": 0,
        "independent_resolved_markets": 0,
        "simulation_quality": SimulationQuality.LOW.value,
        "price_buckets": _empty_buckets(),
        "calibration": {"observations": 0, "brier": 0.0, "log_loss": 0.0, "ece": 0.0},
        "liquidity": {"observations": 0, "mean_absolute_error": 0.0},
        "time_to_resolution": {"observations": 0, "mean_roi": 0.0},
        "repricing": {"markets": 0, "mean_reversion_fraction": 0.0},
        "errors": errors,
        "model_version": "market-price-baseline-v1",
    }
    buckets = _empty_buckets()
    calibration: list[tuple[float, float]] = []
    liquidity_errors: list[float] = []
    time_rois: list[tuple[float, float]] = []
    repricing_scores: list[float] = []
    markets_with_history = 0
    resolved_count = 0
    for market in markets:
        if not isinstance(market, PredictionMarketSnapshot):
            continue
        try:
            history = tuple(provider.price_history(market.market_id))
        except Exception as exc:
            errors.append(f"{market.market_id}: history error: {exc}")
            continue
        if not history:
            continue
        markets_with_history += 1
        if store is not None:
            version = _content_version(history)
            try:
                store.save_dataset(
                    f"prediction:{market.market_id}",
                    version,
                    history,
                    metadata={
                        "provider": base["provider"],
                        "market_id": market.market_id,
                        "question": market.question,
                        "resolution_criteria": market.resolution_criteria,
                        "historical_order_book": False,
                    },
                    quality=SimulationQuality.LOW,
                )
            except ValueError as exc:
                errors.append(str(exc))
        outcome = _binary_outcome(market.settlement)
        prices = [_price(point) for point in history]
        prices = [price for price in prices if price is not None and 0.0 <= price <= 1.0]
        if not prices:
            continue
        if outcome is None:
            continue
        resolved_count += 1
        last_price = prices[-1]
        calibration.append((last_price, outcome))
        for price in prices:
            bucket = _bucket_for(price)
            if bucket is None:
                continue
            item = buckets[bucket]
            item["count"] += 1
            item["resolved_count"] += 1
            item["wins"] += 1 if outcome else 0
            item["mean_price_sum"] += price
            item["roi_sum"] += ((outcome - price) / price) if price > 0 else 0.0
        if market.liquidity is not None:
            liquidity_errors.append(abs(last_price - outcome))
        if market.expiry is not None:
            stamp = _point_timestamp(history[-1])
            if stamp is not None:
                seconds = max(0.0, (market.expiry - stamp).total_seconds())
                time_rois.append((seconds, ((outcome - last_price) / last_price) if last_price > 0 else 0.0))
        changes = [_price(point) for point in history]
        changes = [price for price in changes if price is not None]
        if len(changes) >= 3:
            jump = changes[-2] - changes[-3]
            follow = changes[-1] - changes[-2]
            if abs(jump) >= 0.05:
                repricing_scores.append(1.0 if jump * follow < 0 else 0.0)
    for item in buckets.values():
        count = item.pop("mean_price_sum")
        roi_sum = item.pop("roi_sum")
        item["mean_price"] = count / item["count"] if item["count"] else 0.0
        item["mean_roi"] = roi_sum / item["count"] if item["count"] else 0.0
        item["win_rate"] = item["wins"] / item["resolved_count"] if item["resolved_count"] else 0.0
    from .metrics import brier_score, expected_calibration_error, log_loss

    calibration_records = [{"probability": probability, "outcome": outcome} for probability, outcome in calibration]
    base["markets_with_history"] = markets_with_history
    base["independent_resolved_markets"] = resolved_count
    base["price_buckets"] = buckets
    base["calibration"] = {
        "observations": len(calibration_records),
        "brier": brier_score(calibration_records),
        "log_loss": log_loss(calibration_records),
        "ece": expected_calibration_error(calibration_records),
    }
    base["liquidity"] = {
        "observations": len(liquidity_errors),
        "mean_absolute_error": mean(liquidity_errors) if liquidity_errors else 0.0,
        "note": "liquidity association is descriptive; historical depth was unavailable",
    }
    time_buckets: dict[str, dict[str, float]] = {
        "under_1d": {"count": 0.0, "roi_sum": 0.0},
        "1d_7d": {"count": 0.0, "roi_sum": 0.0},
        "7d_30d": {"count": 0.0, "roi_sum": 0.0},
        "over_30d": {"count": 0.0, "roi_sum": 0.0},
    }
    for seconds, roi in time_rois:
        label = _time_bucket(seconds)
        time_buckets[label]["count"] += 1.0
        time_buckets[label]["roi_sum"] += roi
    base["time_to_resolution"] = {
        "observations": len(time_rois),
        "mean_roi": mean(roi for _, roi in time_rois) if time_rois else 0.0,
        "mean_seconds_to_expiry": mean(seconds for seconds, _ in time_rois) if time_rois else 0.0,
        "buckets": {
            label: {
                "count": values["count"],
                "mean_roi": values["roi_sum"] / values["count"] if values["count"] else 0.0,
            }
            for label, values in time_buckets.items()
        },
        "note": "last observed price and expiry are price-history approximations",
    }
    base["repricing"] = {
        "markets": len(repricing_scores),
        "mean_reversion_fraction": mean(repricing_scores) if repricing_scores else 0.0,
        "note": "adjacent price changes are not independent bets",
    }
    base["errors"] = errors
    base["limitations"] = [
        "Public Polymarket histories are price-only for this workflow; historical spread/depth and fills are unavailable.",
        "Resolved samples are selected from the provider catalog and may be small or non-independent.",
        "Market price is used only as a calibration baseline; no LLM opinion is used as a probability model.",
        "Bucket ROI is descriptive and does not establish executable profitability.",
    ]
    return base


def run_initial_research(
    *,
    crypto_provider: Any | None = None,
    prediction_provider: Any | None = None,
    market_limit: int = 20,
    store: AxiomStore | None = None,
    timeout: float = 10.0,
) -> ResearchReport:
    """Run both initial studies and return an honest, serializable report."""
    crypto = run_crypto_research(crypto_provider, store=store, timeout=timeout)
    prediction = run_prediction_research(prediction_provider, market_limit=market_limit, store=store, timeout=timeout)
    limitations = tuple(dict.fromkeys([*crypto.get("limitations", ()), *prediction.get("limitations", ())]))
    return ResearchReport(datetime.now(timezone.utc), crypto, prediction, limitations)


def write_report(report: ResearchReport | Mapping[str, Any], path: str) -> None:
    payload = report.to_json() if isinstance(report, ResearchReport) else json.dumps(_jsonable(report), sort_keys=True, indent=2)
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(payload + "\n")


def _chronological_split(rows: Sequence[Any]) -> tuple[Any, str]:
    ordered = sorted(rows, key=lambda item: item.timestamp)
    train_count = max(1, int(len(ordered) * 0.60))
    validation_count = max(1, int(len(ordered) * 0.20))
    if train_count + validation_count >= len(ordered):
        validation_count = 1
        train_count = len(ordered) - 2
    train_end = ordered[train_count].timestamp
    validation_end = ordered[train_count + validation_count].timestamp
    holdout_end = ordered[-1].timestamp + timedelta(microseconds=1)
    version = _content_version(ordered)
    return split_dataset(ordered, train_end, validation_end, holdout_end, dataset_version=version, require_nonempty=True), version


def _content_version(rows: Iterable[Any]) -> str:
    import hashlib

    encoded = json.dumps(_jsonable(list(rows)), sort_keys=True, separators=(",", ":"))
    return "sha256:" + hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _regime_labels(bars: Sequence[Any]) -> list[str]:
    return [regime.state.value for regime in RegimeEngine().detect_crypto(bars).regimes]


def _empty_buckets() -> dict[str, dict[str, Any]]:
    return {
        label: {"lower": lower, "upper": upper, "count": 0, "resolved_count": 0, "wins": 0, "mean_price_sum": 0.0, "roi_sum": 0.0}
        for label, lower, upper in _BUCKETS
    }


def _bucket_for(price: float) -> str | None:
    for label, lower, upper in _BUCKETS:
        if lower <= price < upper or (label == "50-100c" and price <= upper):
            return label
    return None


def _binary_outcome(state: SettlementState) -> float | None:
    if state is SettlementState.RESOLVED_YES:
        return 1.0
    if state is SettlementState.RESOLVED_NO:
        return 0.0
    return None
def _quality_rank(value: str) -> int:
    return {
        SimulationQuality.LOW.value: 0,
        SimulationQuality.MEDIUM.value: 1,
        SimulationQuality.HIGH.value: 2,
    }.get(str(value), 0)


def _quality_value(value: Any, fallback: SimulationQuality) -> SimulationQuality:
    if isinstance(value, SimulationQuality):
        return value
    try:
        return SimulationQuality(str(value)) if value is not None else fallback
    except ValueError:
        return fallback


def _research_quality(provider: Any, fallback: SimulationQuality) -> SimulationQuality:
    return _quality_value(getattr(provider, "simulation_quality", None), fallback)


def _min_quality(left: SimulationQuality, right: SimulationQuality) -> SimulationQuality:
    return left if _quality_rank(left.value) <= _quality_rank(right.value) else right


def _time_bucket(seconds: float) -> str:
    if seconds < 86_400.0:
        return "under_1d"
    if seconds < 7 * 86_400.0:
        return "1d_7d"
    if seconds < 30 * 86_400.0:
        return "7d_30d"
    return "over_30d"


def _price(point: Any) -> float | None:
    value = point.get("price", point.get("yes_mid")) if isinstance(point, Mapping) else None
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _point_timestamp(point: Any) -> datetime | None:
    value = point.get("timestamp") if isinstance(point, Mapping) else None
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return parsed.astimezone(timezone.utc) if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            return None
    return None


def _jsonable(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_jsonable(item) for item in value]
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat() if value.tzinfo else value.replace(tzinfo=timezone.utc).isoformat()
    if hasattr(value, "value") and not isinstance(value, (str, bytes)):
        return value.value
    if hasattr(value, "__dataclass_fields__"):
        return {name: _jsonable(getattr(value, name)) for name in value.__dataclass_fields__}
    return value


__all__ = ["ResearchReport", "run_crypto_research", "run_initial_research", "run_prediction_research", "write_report"]
