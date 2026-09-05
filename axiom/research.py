"""Reproducible historical research workflows for the initial Axiom study.

The workflows are intentionally conservative: they report data coverage and
simulation quality, keep train/validation/holdout separate, and never label a
small or price-only sample as proof of profitability.
"""
from __future__ import annotations

from dataclasses import dataclass
from itertools import islice
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
from statistics import mean, median
from typing import Any, Iterable, Mapping, Sequence

from .attribution import fitness_attribution
from .backtest import CryptoBacktester
from .benchmarks import crypto_benchmarks, prediction_benchmarks
from .bootstrap import crypto_universe_dataset_id
from .crypto_universe import SURVIVORSHIP_BIAS_PRESENT, UniverseSnapshot, load_crypto_universe
from .data import BinanceAdapter, PolymarketAdapter
from .domain import InstrumentMetadata, MarketType, OHLCVBar, PredictionMarketSnapshot, ResearchQuality, SettlementState, SimulationQuality, ensure_utc, parse_timestamp, to_record
from .evaluation import evaluate_scores, split_dataset
from .metrics import calibration_at_horizons
from .regime import RegimeEngine
from .robustness import bootstrap_confidence_interval, minimum_sample_check
from .storage import AxiomStore
from .strategy import validate_strategy
from .tracking import ExperimentTracker


_MAX_RESEARCH_RECORDS = 100_000
_CRYPTO_FAMILY_NAMES: tuple[str, ...] = (
    "dip",
    "momentum",
    "trend",
    "mean_reversion",
    "breakout",
    "volatility",
    "rsi",
    "volume_filter",
)


def _canonical_symbol(symbol: Any) -> str:
    return str(symbol).replace("/", "").replace("-", "").replace("_", "").strip().upper()


def _strategy_id_prefix(symbol: str) -> str:
    """Return the stable ``base-quote`` prefix used by deterministic strategies."""
    normalized = _canonical_symbol(symbol)
    for quote in ("USDT", "USDC", "BUSD", "USD", "BTC", "ETH"):
        if normalized.endswith(quote) and len(normalized) > len(quote):
            return f"{normalized[:-len(quote)].lower()}-{quote.lower()}"
    return normalized.lower()
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
    def to_markdown(self) -> str:
        """Render a concise human-readable report without embedding raw tables."""
        lines = [
            "# Axiom Research Report",
            "",
            f"Generated: {self.generated_at.astimezone(timezone.utc).isoformat()}",
            "",
            "## Crypto",
            "",
            _markdown_section(self.crypto),
            "",
            "## Prediction markets",
            "",
            _markdown_section(self.prediction),
            "",
            "## Limitations",
            "",
        ]
        lines.extend(f"- {item}" for item in self.limitations) if self.limitations else lines.append("- None recorded.")
        return "\n".join(lines).rstrip() + "\n"


def run_crypto_research(
    provider: Any | None = None,
    *,
    symbol: str = "BTC/USDT",
    start: datetime | None = None,
    end: datetime | None = None,
    initial_cash: float = 10_000.0,
    store: AxiomStore | None = None,
    timeout: float = 10.0,
    symbols: Sequence[str] | Mapping[str, Any] | None = None,
    providers: Mapping[str, Any] | Sequence[Any] | Any | None = None,
    symbol_providers: Mapping[str, Any] | None = None,
    universe: Mapping[str, Any] | UniverseSnapshot | None = None,
    universe_provenance: Mapping[str, Any] | None = None,
    dataset_id: str | Mapping[str, Any] | None = None,
    dataset_version: str | Mapping[str, Any] | None = None,
    timeframe: str | Mapping[str, Any] = "1d",
    source_type: str | Mapping[str, Any] = "HISTORICAL",
) -> dict[str, Any]:
    """Backtest deterministic crypto families for one symbol.

    The original single-symbol call remains the default.  Passing ``symbols``,
    ``providers``/``symbol_providers``, and universe provenance delegates to the
    additive multi-symbol orchestrator while retaining this stable entry point.
    """
    if symbols is not None or providers is not None or symbol_providers is not None:
        requested_symbols: Sequence[str] | Mapping[str, Any]
        if symbols is None:
            requested_symbols = symbol_providers if symbol_providers is not None else providers if isinstance(providers, Mapping) else (symbol,)
        else:
            requested_symbols = symbols
        return run_multi_symbol_crypto_research(
            requested_symbols,
            providers if symbol_providers is None else symbol_providers,
            provider=provider,
            universe=universe,
            universe_provenance=universe_provenance,
            start=start,
            end=end,
            initial_cash=initial_cash,
            store=store,
            timeout=timeout,
            dataset_id=dataset_id,
            dataset_version=dataset_version,
            timeframe=timeframe,
            source_type=source_type,
        )
    resolved_universe_provenance: dict[str, Any] | None = None
    if universe is not None or universe_provenance is not None:
        resolved_universe_provenance = _research_universe_provenance(
            universe,
            universe_provenance,
            store=store,
            requested_symbols=(symbol,),
        )
    dataset_binding = _dataset_binding_requested(dataset_id, dataset_version, timeframe, source_type)
    bound_dataset: dict[str, Any] | None = None
    if dataset_binding:
        bound_dataset = _load_bound_crypto_dataset(
            store,
            symbol=symbol,
            dataset_id=dataset_id,
            dataset_version=dataset_version,
            timeframe=timeframe,
            source_type=source_type,
        )
    errors: list[str] = []
    if bound_dataset is not None:
        bars = _bounded_crypto_bars(bound_dataset["bars"], start=start, end=end)
        source_quality = _quality_value(bound_dataset.get("quality"), SimulationQuality.MEDIUM)
        provider_name = str(bound_dataset.get("provider") or "persisted-dataset")
        instrument_metadata = bound_dataset.get("instrument_metadata")
    else:
        provider = provider or BinanceAdapter(timeout=timeout)
        try:
            bars = tuple(islice(iter(provider.historical_ohlcv(symbol, start=start, end=end, interval="1d")), _MAX_RESEARCH_RECORDS))
        except Exception as exc:  # network adapters must not abort the report
            bars = ()
            errors.append(f"crypto data error: {exc}")
        _consume_transport_errors(provider, errors, "crypto historical data")
        source_quality = _research_quality(provider, SimulationQuality.MEDIUM if bars else SimulationQuality.LOW)
        provider_name = str(getattr(provider, "provider_name", provider.__class__.__name__))
        try:
            instrument_metadata = provider.metadata(symbol)
        except Exception as exc:
            instrument_metadata = None
            errors.append(f"crypto metadata error: {exc}")
        _consume_transport_errors(provider, errors, "crypto metadata")
    coverage = {
        "bars": len(bars),
        "start": bars[0].timestamp.isoformat() if bars else None,
        "end": bars[-1].timestamp.isoformat() if bars else None,
    }
    base: dict[str, Any] = {
        "market_type": MarketType.CRYPTO_SPOT.value,
        "provider": provider_name,
        "instrument": symbol,
        "instrument_metadata": to_record(instrument_metadata) if instrument_metadata is not None else None,
        "instrument_metadata_available": instrument_metadata is not None,
        "bars": len(bars),
        "historical_coverage": coverage,
        "dataset_id": bound_dataset["dataset_id"] if bound_dataset is not None else "",
        "dataset_version": bound_dataset["dataset_version"] if bound_dataset is not None else "",
        "timeframe": bound_dataset["timeframe"] if bound_dataset is not None else timeframe,
        "source_type": bound_dataset["source_type"] if bound_dataset is not None else str(source_type).strip().upper(),
        "simulation_quality": source_quality.value,
        "experiments": [],
        "validation": {},
        "errors": errors,
    }
    if bound_dataset is not None:
        base["dataset_provenance"] = _jsonable(dict(bound_dataset["provenance"]))
    if resolved_universe_provenance is not None:
        base["universe_provenance"] = _jsonable(resolved_universe_provenance)
    if len(bars) < 6:
        base["limitations"] = ["fewer than six OHLCV bars; no train/validation/holdout result"]
        return base

    split, content_version = _chronological_split(bars)
    version = bound_dataset["dataset_version"] if bound_dataset is not None else content_version
    base["dataset_version"] = version
    base["benchmarks"] = [item.as_record() for item in crypto_benchmarks(bars, initial_cash=initial_cash, fee_bps=10.0, slippage_bps=5.0, symbol=symbol)]
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
                "strategy_id": f"{_strategy_id_prefix(symbol)}-{family}",
            }
        )
        results = []
        partitions = (
            (split.train, ()),
            (split.validation, split.train),
            (split.holdout, (*split.train, *split.validation)),
        )
        for partition, warmup in partitions:
            result = CryptoBacktester(
                initial_cash=initial_cash,
                fee_bps=10.0,
                slippage_bps=5.0,
                allocation=0.50,
                symbol=symbol,
            ).run(partition, definition, symbol=symbol, warmup=warmup)
            results.append(result)
        scores = [result.metrics.get("total_return", 0.0) for result in results]
        evaluation = evaluate_scores(
            {"total_return": scores[0]},
            {"total_return": scores[1]},
            {"total_return": scores[2]},
            metric="total_return",
        )
        experiment_quality = _min_quality(source_quality, results[2].quality)
        validation_sample = minimum_sample_check(
            len(split.validation),
            min_observations=10,
            trades=results[1].trades,
            min_trades=2,
        )
        rejection_reason = None
        if evaluation.fitness <= 0:
            rejection_reason = "non-positive validation selection fitness"
        elif not validation_sample["passed"]:
            rejection_reason = "insufficient validation sample"
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
                "holdout_fills": float(results[2].trades),
                "quality": experiment_quality.value,
                "selection_basis": "validation_only",
            },
            fitness=evaluation.fitness,
            rejected=rejection_reason is not None,
            rejection_reason=rejection_reason,
            created_at=bars[-1].timestamp,
        )
        experiments.append(
            {
                "family": family,
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
                    "holdout_degradation": evaluation.holdout_degradation,
                    "overfit_penalty": evaluation.overfit_penalty,
                    "fitness": evaluation.fitness,
                    "selection_basis": "validation_only",
                    "attribution": fitness_attribution(
                        {"total_return": scores[0]},
                        {"total_return": scores[1]},
                        {"total_return": scores[2]},
                        metric="total_return",
                    ),
                },
                "robustness": {
                    "minimum_sample": validation_sample,
                    "validation_score_ci": bootstrap_confidence_interval([scores[1]], resamples=256, seed=0),
                },
                "rejected": record.rejected,
                "rejection_reason": record.rejection_reason,
            }
        )
    base["experiments"] = experiments
    base["validation"] = {
        item["strategy_id"]: {
            "score": item["evaluation"]["validation_score"],
            "fitness": item["evaluation"]["fitness"],
            "minimum_sample": item["robustness"]["minimum_sample"],
            "rejected": item["rejected"],
            "rejection_reason": item["rejection_reason"],
        }
        for item in experiments
    }
    base["simulation_quality"] = _min_quality(
        source_quality,
        min(
            (SimulationQuality(item["quality"]) for item in experiments),
            key=lambda item: _quality_rank(item.value),
            default=SimulationQuality.LOW,
        ),
    ).value
    base["limitations"] = [
        "Binance OHLCV does not provide historical order-book depth in this workflow.",
        "Next-bar OHLCV execution is an approximation; results are not a live-profit claim.",
        "Candidate selection and rejection use validation only; locked holdout values are report-only.",
    ]
    return base
def run_multi_symbol_crypto_research(
    symbols: Sequence[str] | Mapping[str, Any] | None = None,
    providers: Mapping[str, Any] | Sequence[Any] | Any | None = None,
    *,
    start: datetime | None = None,
    end: datetime | None = None,
    provider: Any | None = None,
    symbol_providers: Mapping[str, Any] | None = None,
    universe: Mapping[str, Any] | UniverseSnapshot | None = None,
    universe_provenance: Mapping[str, Any] | None = None,
    universe_id: Any | None = None,
    universe_version: Any | None = None,
    methodology: Any | None = None,
    survivorship_bias: Any | None = None,
    initial_cash: float = 10_000.0,
    store: AxiomStore | None = None,
    timeout: float = 10.0,
    max_symbols: int = 50,
    dataset_id: str | Mapping[str, Any] | None = None,
    dataset_version: str | Mapping[str, Any] | None = None,
    timeframe: str | Mapping[str, Any] = "1d",
    source_type: str | Mapping[str, Any] = "HISTORICAL",
) -> dict[str, Any]:
    """Run deterministic OHLCV research independently for every universe symbol.

    ``symbols`` and ``providers`` are deliberately explicit.  A provider may be
    shared for all symbols, or supplied as a symbol-keyed mapping.  A missing or
    broken symbol is represented in the returned report rather than removed
    from the denominator or converted into an aggregate average.
    """
    requested, provider_inputs = _research_symbol_inputs(symbols, providers, symbol_providers)
    if isinstance(max_symbols, bool) or not isinstance(max_symbols, int) or not 1 <= max_symbols <= 50:
        raise ValueError("max_symbols must be an integer from 1 to 50")
    if len(requested) > max_symbols:
        raise ValueError(f"crypto research universe exceeds max_symbols={max_symbols}")
    if _multi_symbol_dataset_binding_requested(dataset_id, dataset_version, timeframe, source_type):
        _validate_multi_symbol_dataset_bindings(
            requested,
            dataset_id=dataset_id,
            dataset_version=dataset_version,
            timeframe=timeframe,
            source_type=source_type,
        )
    provenance = _research_universe_provenance(
        universe,
        universe_provenance,
        store=store,
        requested_symbols=requested,
        universe_id=universe_id,
        universe_version=universe_version,
        methodology=methodology,
        survivorship_bias=survivorship_bias,
    )
    declared_symbols = provenance.get("selected_symbols", provenance.get("symbols"))
    if declared_symbols is None:
        provenance["selected_symbols"] = list(requested)
        provenance["symbols"] = list(requested)
        declared_symbols = requested
    if isinstance(declared_symbols, Sequence) and not isinstance(declared_symbols, (str, bytes, bytearray)):
        declared = {_canonical_symbol(item) for item in declared_symbols}
        unexpected = sorted(symbol for symbol in requested if _canonical_symbol(symbol) not in declared)
        if unexpected:
            raise ValueError(f"symbols are not members of the declared universe: {unexpected}")
    per_symbol: dict[str, dict[str, Any]] = {}
    errors: list[str] = []
    for symbol in requested:
        symbol_provider = _provider_for_symbol(symbol, provider_inputs, provider)
        symbol_dataset_id = _symbol_binding_value(dataset_id, symbol)
        symbol_dataset_version = _symbol_binding_value(dataset_version, symbol)
        symbol_timeframe = _symbol_binding_value(
            timeframe,
            symbol,
            default=None if isinstance(timeframe, Mapping) else "1d",
        )
        symbol_source_type = _symbol_binding_value(
            source_type,
            symbol,
            default=None if isinstance(source_type, Mapping) else "HISTORICAL",
        )
        if symbol_provider is None and not _dataset_binding_requested(
            symbol_dataset_id,
            symbol_dataset_version,
            symbol_timeframe,
            symbol_source_type,
        ):
            result = _failed_crypto_symbol_result(symbol, "no provider supplied for symbol")
        else:
            try:
                result = run_crypto_research(
                    symbol_provider,
                    symbol=symbol,
                    start=start,
                    end=end,
                    initial_cash=initial_cash,
                    store=store,
                    timeout=timeout,
                    dataset_id=symbol_dataset_id,
                    dataset_version=symbol_dataset_version,
                    timeframe=symbol_timeframe,
                    source_type=symbol_source_type,
                )
            except Exception as exc:  # preserve one bad asset without aborting the universe
                result = _failed_crypto_symbol_result(symbol, f"crypto research error: {exc}")
        if symbol_dataset_id is not None:
            result["dataset_id"] = str(symbol_dataset_id).strip()
        else:
            result.setdefault("dataset_id", "")
        if symbol_dataset_version is not None:
            result["dataset_version"] = str(symbol_dataset_version).strip()
        else:
            result.setdefault("dataset_version", "")
        if symbol_timeframe is not None:
            result["timeframe"] = str(symbol_timeframe).strip()
        else:
            result.setdefault("timeframe", "")
        if symbol_source_type is not None:
            result["source_type"] = str(symbol_source_type).strip().upper()
        else:
            result.setdefault("source_type", "")
        result.setdefault("universe_provenance", _jsonable(provenance))
        experiments = result.get("experiments", ())
        result["status"] = "complete" if experiments else ("failed" if result.get("errors") else "insufficient_data")
        result["symbol"] = symbol
        result["metrics"] = {
            str(item.get("strategy_id", "")): {
                "train": item.get("train", {}),
                "validation": item.get("validation", {}),
                "holdout": item.get("holdout", {}),
                "evaluation": item.get("evaluation", {}),
            }
            for item in experiments
            if isinstance(item, Mapping)
        }
        result["surviving_families"] = sorted(
            {str(item.get("family")) for item in experiments if isinstance(item, Mapping) and not item.get("rejected") and item.get("family")}
        )
        result["rejected_families"] = sorted(
            {str(item.get("family")) for item in experiments if isinstance(item, Mapping) and item.get("rejected") and item.get("family")}
        )
        per_symbol[symbol] = result
        for detail in result.get("errors", ()):
            errors.append(f"{symbol}: {detail}")

    family_summaries = _cross_sectional_family_summaries(requested, per_symbol)
    coverage_by_symbol = {
        symbol: dict(result.get("historical_coverage", {}))
        for symbol, result in per_symbol.items()
    }
    starts = [item["start"] for item in coverage_by_symbol.values() if item.get("start")]
    ends = [item["end"] for item in coverage_by_symbol.values() if item.get("end")]
    symbols_with_data = [symbol for symbol, item in coverage_by_symbol.items() if item.get("bars", 0)]
    all_experiments = [
        {"symbol": symbol, **dict(experiment)}
        for symbol in requested
        for experiment in per_symbol[symbol].get("experiments", ())
        if isinstance(experiment, Mapping)
    ]
    family_names = tuple(_CRYPTO_FAMILY_NAMES)
    strategy_records = [
        {
            "family": family,
            "version": 1,
            "strategy_ids": [
                str(experiment["strategy_id"])
                for experiment in all_experiments
                if experiment.get("family") == family
            ],
        }
        for family in family_names
    ]
    failed_symbols = [symbol for symbol in requested if per_symbol[symbol].get("status") != "complete"]
    report: dict[str, Any] = {
        "market_type": MarketType.CRYPTO_SPOT.value,
        "universe_id": provenance["universe_id"],
        "universe_version": provenance["universe_version"],
        "methodology": provenance["methodology"],
        "survivorship_bias": provenance["survivorship_bias"],
        "universe_provenance": _jsonable(provenance),
        "universe": _jsonable(provenance),
        "assets": [_asset_from_symbol(symbol) for symbol in requested],
        "symbols": list(requested),
        "assets_covered": [_asset_from_symbol(symbol) for symbol in requested],
        "symbols_covered": list(requested),
        "historical_coverage": {
            "start": min(starts) if starts else None,
            "end": max(ends) if ends else None,
            "symbols_with_data": symbols_with_data,
            "by_symbol": coverage_by_symbol,
        },
        "strategies": strategy_records,
        "strategy_families": list(family_names),
        "experiments": all_experiments,
        "validation": {
            symbol: dict(per_symbol[symbol].get("validation", {}))
            for symbol in requested
        },
        "per_symbol": per_symbol,
        "symbol_results": per_symbol,
        "results": per_symbol,
        "metrics": {
            symbol: dict(per_symbol[symbol].get("metrics", {}))
            for symbol in requested
        },
        "family_summaries": family_summaries,
        "family_counts": family_summaries,
        "cross_sectional": {
            "family_summaries": family_summaries,
            "family_counts": family_summaries,
            "symbols_requested": len(requested),
            "symbols_evaluated": len(requested) - len(failed_symbols),
            "failed_symbols": failed_symbols,
        },
        "aggregate": {
            "family_summaries": family_summaries,
            "family_counts": family_summaries,
            "failed_symbols": failed_symbols,
        },
        "surviving_families": sorted(family for family, item in family_summaries.items() if item["survivors"]),
        "rejected_families": sorted(family for family, item in family_summaries.items() if item["rejects"]),
        "failed_symbols": failed_symbols,
        "bad_symbols": failed_symbols,
        "errors": errors,
        "live_execution": False,
        "status": "complete" if not failed_symbols else ("failed" if len(failed_symbols) == len(requested) else "partial"),
    }
    payload = _jsonable(report)
    if store is not None:
        report_id = "crypto-research:" + hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()[:24]
        _save_research_report_if_absent(
            store,
            report_id,
            payload,
            experiment_id=str(provenance["universe_id"]),
        )
    return payload


def _research_symbol_inputs(
    symbols: Sequence[str] | Mapping[str, Any] | None,
    providers: Mapping[str, Any] | Sequence[Any] | Any | None,
    symbol_providers: Mapping[str, Any] | None,
) -> tuple[tuple[str, ...], Mapping[str, Any] | Sequence[Any] | Any | None]:
    supplied_mapping = symbols if isinstance(symbols, Mapping) else None
    if supplied_mapping is not None:
        if symbol_providers is None and providers is None:
            providers = supplied_mapping
        symbols = tuple(str(key) for key in supplied_mapping)
    elif symbols is None:
        if symbol_providers is not None:
            symbols = tuple(str(key) for key in symbol_providers)
        elif isinstance(providers, Mapping):
            symbols = tuple(str(key) for key in providers)
        else:
            raise ValueError("symbols must be supplied for multi-symbol crypto research")
    elif isinstance(symbols, str):
        symbols = (symbols,)
    original = tuple(str(item).strip() for item in symbols)
    if not original or any(not item for item in original):
        raise ValueError("symbols must contain non-empty values")
    if symbol_providers is not None:
        providers = symbol_providers
    if isinstance(providers, Sequence) and not isinstance(providers, (str, bytes, bytearray)):
        if len(providers) != len(original):
            raise ValueError("provider sequence must match symbols")
        providers = {symbol: item for symbol, item in zip(original, providers)}
    requested = tuple(sorted(original, key=lambda item: (_canonical_symbol(item), item)))
    canonical = [_canonical_symbol(item) for item in requested]
    if len(set(canonical)) != len(canonical):
        raise ValueError("symbols must not contain duplicates")
    return requested, providers


def _provider_for_symbol(
    symbol: str,
    providers: Mapping[str, Any] | Sequence[Any] | Any | None,
    shared: Any | None,
) -> Any | None:
    if shared is not None:
        return shared
    if isinstance(providers, Mapping):
        if symbol in providers:
            return providers[symbol]
        key = _canonical_symbol(symbol)
        for candidate, value in providers.items():
            if _canonical_symbol(candidate) == key:
                return value
        return None
    if isinstance(providers, Sequence) and not isinstance(providers, (str, bytes, bytearray)):
        return providers[0] if len(providers) == 1 else None
    return providers


def _research_universe_provenance(
    universe: Mapping[str, Any] | UniverseSnapshot | None,
    supplied: Mapping[str, Any] | None,
    *,
    store: AxiomStore | None,
    requested_symbols: Sequence[str] | None = None,
    universe_id: Any | None = None,
    universe_version: Any | None = None,
    methodology: Any | None = None,
    survivorship_bias: Any | None = None,
) -> dict[str, Any]:
    """Normalize and verify one exact persisted universe binding.

    A caller-supplied mapping is only descriptive input.  The durable identity,
    version, hash, and selected membership are taken from the exact persisted
    snapshot and are never resolved through a latest/provider fallback.
    """
    result: dict[str, Any] = {}
    sources: list[dict[str, Any]] = []
    for source in (universe, supplied):
        if source is None:
            continue
        if isinstance(source, UniverseSnapshot):
            payload = source.as_dict()
        elif isinstance(source, Mapping):
            payload = dict(source)
        else:
            raise TypeError("universe provenance must be a mapping or UniverseSnapshot")
        nested = payload.get("provenance")
        if isinstance(nested, Mapping):
            merged = dict(nested)
            merged.update(payload)
            payload = merged
        sources.append(payload)
        result.update(payload)

    def _text(value: Any) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    def _claim(explicit: Any | None, keys: Sequence[str], label: str) -> str | None:
        values: list[str] = []
        explicit_value = _text(explicit)
        if explicit_value is not None:
            values.append(explicit_value)
        for source in sources:
            for key in keys:
                value = _text(source.get(key))
                if value is not None:
                    values.append(value)
        unique = list(dict.fromkeys(values))
        if len(unique) > 1:
            raise ValueError(f"conflicting {label} values in universe provenance")
        return unique[0] if unique else None

    resolved_id = _claim(universe_id, ("universe_id", "id"), "universe_id")
    resolved_version = _claim(
        universe_version,
        ("universe_version", "version", "snapshot_version"),
        "universe_version",
    )
    if resolved_id is None or resolved_version is None:
        raise ValueError("versioned universe provenance requires universe_id and universe_version")
    if resolved_version.casefold() in {"latest", "current", "default", "unversioned"}:
        raise ValueError("universe_version must be immutable and versioned")
    if store is None:
        raise ValueError("persisted universe store is required for versioned provenance")

    persisted = load_crypto_universe(store, universe_id=resolved_id, version=resolved_version)
    if persisted is None:
        raise ValueError(f"no persisted crypto universe found for {resolved_id}/{resolved_version}")
    if str(persisted.universe_id).strip() != resolved_id or str(persisted.version or "").strip() != resolved_version:
        raise ValueError("persisted universe identity does not match supplied universe")

    supplied_hash = _claim(
        None,
        ("snapshot_hash", "universe_snapshot_hash", "content_hash", "universe_hash"),
        "snapshot_hash",
    )
    persisted_hash = _text(persisted.snapshot_hash)
    if persisted_hash is None:
        raise ValueError("persisted universe snapshot_hash is required")
    if supplied_hash is not None and supplied_hash != persisted_hash:
        raise ValueError("universe snapshot_hash does not match persisted snapshot")

    def _members(value: Any, label: str) -> tuple[tuple[str, ...], frozenset[str]]:
        if isinstance(value, (str, bytes, bytearray)) or not isinstance(value, Sequence):
            raise ValueError(f"{label} must be a sequence")
        display = tuple(str(item).strip() for item in value)
        if any(not item for item in display):
            raise ValueError(f"{label} must not contain empty symbols")
        canonical = tuple(_canonical_symbol(item) for item in display)
        if any(not item for item in canonical) or len(set(canonical)) != len(canonical):
            raise ValueError(f"{label} must contain unique symbols")
        return display, frozenset(canonical)

    declared_members: tuple[str, ...] | None = None
    declared_set: frozenset[str] | None = None
    for source in sources:
        for key in ("selected_symbols", "symbols", "instruments"):
            if key not in source or source[key] is None:
                continue
            display, members = _members(source[key], f"universe {key}")
            if declared_set is not None and members != declared_set:
                raise ValueError("conflicting selected membership values in universe provenance")
            if declared_set is None:
                declared_members, declared_set = display, members

    persisted_members, persisted_set = _members(persisted.selected_symbols, "persisted selected_symbols")
    if declared_set is not None and declared_set != persisted_set:
        missing = sorted(persisted_set - declared_set)
        extra = sorted(declared_set - persisted_set)
        raise ValueError(
            "supplied universe selected membership does not match persisted snapshot"
            f" (missing={missing}, extra={extra})"
        )
    if requested_symbols is not None:
        _, requested_set = _members(requested_symbols, "requested symbols")
        if requested_set != persisted_set:
            missing = sorted(persisted_set - requested_set)
            extra = sorted(requested_set - persisted_set)
            raise ValueError(
                "persisted universe selected membership does not match requested symbols"
                f" (missing={missing}, extra={extra})"
            )

    result["universe_id"] = resolved_id
    result["universe_version"] = resolved_version
    if any("version" in source for source in sources):
        result["version"] = resolved_version
    result["snapshot_hash"] = persisted_hash
    output_members = list(declared_members if declared_members is not None else persisted_members)
    result["selected_symbols"] = output_members
    if declared_members is None or any("symbols" in source for source in sources):
        result["symbols"] = list(output_members)
    result["methodology"] = (
        _text(methodology)
        or _text(result.get("methodology"))
        or _text(result.get("method"))
        or _text(result.get("policy"))
        or "unspecified"
    )
    result["survivorship_bias"] = (
        _text(survivorship_bias)
        or _text(result.get("survivorship_bias"))
        or _text(result.get("survivorship"))
        or SURVIVORSHIP_BIAS_PRESENT
    )
    return result


def _failed_crypto_symbol_result(symbol: str, error: str) -> dict[str, Any]:
    return {
        "market_type": MarketType.CRYPTO_SPOT.value,
        "provider": None,
        "instrument": symbol,
        "instrument_metadata": None,
        "instrument_metadata_available": False,
        "bars": 0,
        "historical_coverage": {"bars": 0, "start": None, "end": None},
        "dataset_version": "",
        "simulation_quality": SimulationQuality.LOW.value,
        "experiments": [],
        "validation": {},
        "errors": [error],
        "limitations": [error],
    }


def _asset_from_symbol(symbol: str) -> str:
    normalized = _canonical_symbol(symbol)
    for quote in ("USDT", "USDC", "BUSD", "USD", "BTC", "ETH"):
        if normalized.endswith(quote) and len(normalized) > len(quote):
            return normalized[:-len(quote)]
    return normalized


def _distribution(values: Iterable[Any]) -> dict[str, Any]:
    finite: list[float] = []
    for value in values:
        try:
            number = float(value)
        except (TypeError, ValueError):
            continue
        if math.isfinite(number):
            finite.append(number)
    return {
        "count": len(finite),
        "values": finite,
        "min": min(finite) if finite else None,
        "max": max(finite) if finite else None,
        "median": median(finite) if finite else None,
    }


def _cross_sectional_family_summaries(
    symbols: Sequence[str],
    per_symbol: Mapping[str, Mapping[str, Any]],
) -> dict[str, dict[str, Any]]:
    output: dict[str, dict[str, Any]] = {}
    for family in _CRYPTO_FAMILY_NAMES:
        surviving_symbols: list[str] = []
        rejected_symbols: list[str] = []
        failed_symbols: list[str] = []
        fitness: list[Any] = []
        validation_returns: list[Any] = []
        holdout_returns: list[Any] = []
        evaluated = 0
        for symbol in symbols:
            experiments = [
                item for item in per_symbol[symbol].get("experiments", ())
                if isinstance(item, Mapping) and item.get("family") == family
            ]
            if not experiments:
                failed_symbols.append(symbol)
                continue
            evaluated += 1
            experiment = experiments[0]
            if experiment.get("rejected"):
                rejected_symbols.append(symbol)
            else:
                surviving_symbols.append(symbol)
            evaluation = experiment.get("evaluation", {})
            fitness.append(evaluation.get("fitness") if isinstance(evaluation, Mapping) else None)
            validation = experiment.get("validation", {})
            holdout = experiment.get("holdout", {})
            validation_returns.append(validation.get("total_return") if isinstance(validation, Mapping) else None)
            holdout_returns.append(holdout.get("total_return") if isinstance(holdout, Mapping) else None)
        distributions = {
            "fitness": _distribution(fitness),
            "validation_total_return": _distribution(validation_returns),
            "holdout_total_return": _distribution(holdout_returns),
        }
        output[family] = {
            "family": family,
            "symbols": list(symbols),
            "symbol_count": len(symbols),
            "evaluated": evaluated,
            "survivors": len(surviving_symbols),
            "rejects": len(rejected_symbols),
            "failures": len(failed_symbols),
            "survivor_count": len(surviving_symbols),
            "reject_count": len(rejected_symbols),
            "failed_count": len(failed_symbols),
            "surviving_symbols": surviving_symbols,
            "rejected_symbols": rejected_symbols,
            "failed_symbols": failed_symbols,
            "distribution": distributions,
            "distributions": distributions,
        }
    return output

# Explicit names used by integrations that describe the same additive API
# either as a universe run or as a multi-symbol run.
run_crypto_universe_research = run_multi_symbol_crypto_research
run_crypto_research_multi = run_multi_symbol_crypto_research
run_crypto_research_multi_symbol = run_multi_symbol_crypto_research



def run_prediction_research(
    provider: Any | None = None,
    *,
    market_limit: int = 20,
    store: AxiomStore | None = None,
    timeout: float = 10.0,
) -> dict[str, Any]:
    """Study resolved Polymarket price buckets and calibration without LLM odds."""
    provider = provider or PolymarketAdapter(timeout=timeout)
    if isinstance(market_limit, bool) or not isinstance(market_limit, int) or market_limit < 0:
        raise ValueError("market_limit must be a non-negative integer")
    requested = int(market_limit)
    errors: list[str] = []
    try:
        if requested == 0:
            markets = ()
        else:
            try:
                raw_markets = provider.markets(active=False, limit=requested)
            except TypeError as exc:
                if "limit" not in str(exc):
                    raise
                raw_markets = provider.markets(active=False)
            markets = tuple(islice(raw_markets, requested))
    except Exception as exc:
        markets = ()
        errors.append(f"prediction catalog error: {exc}")
    base: dict[str, Any] = {
        "market_type": MarketType.PREDICTION.value,
        "provider": str(getattr(provider, "provider_name", provider.__class__.__name__)),
        "markets_requested": requested,
        "markets_with_history": 0,
        "independent_resolved_markets": 0,
        "resolved_market_groups": 0,
        "independence_method": "unique event IDs or normalized question and expiry",
        "simulation_quality": SimulationQuality.LOW.value,
        "research_quality": ResearchQuality.PRICE_PROXY.value,
        "historical_order_books_available": False,
        "price_buckets": _empty_buckets(),
        "multi_horizon_calibration": {},
        "benchmarks": [],
        "liquidity": {"observations": 0, "mean_absolute_error": 0.0},
        "time_to_resolution": {"observations": 0, "mean_roi": 0.0},
        "repricing": {"markets": 0, "mean_reversion_fraction": 0.0},
        "errors": errors,
        "model_version": "market-price-baseline-v1",
    }
    buckets = _empty_buckets()
    calibration: list[tuple[float, float]] = []
    calibration_horizon_rows: list[dict[str, Any]] = []
    benchmark_rows: list[dict[str, Any]] = []
    liquidity_errors: list[float] = []
    time_rois: list[tuple[float, float]] = []
    repricing_scores: list[float] = []
    markets_with_history = 0
    resolved_market_groups: set[tuple[str, str]] = set()
    has_historical_books = False
    for market in markets:
        if not isinstance(market, PredictionMarketSnapshot):
            continue
        try:
            raw_history = tuple(islice(iter(provider.price_history(market.market_id)), _MAX_RESEARCH_RECORDS))
        except Exception as exc:
            errors.append(f"{market.market_id}: history error: {exc}")
            _consume_transport_errors(provider, errors, f"{market.market_id} history")
            continue
        _consume_transport_errors(provider, errors, f"{market.market_id} history")
        timed_history: list[tuple[datetime, Mapping[str, Any]]] = []
        for point in raw_history:
            if not isinstance(point, Mapping):
                continue
            stamp = _point_timestamp(point)
            if stamp is not None:
                timed_history.append((stamp, point))
        timed_history.sort(key=lambda item: item[0])
        history = tuple(point for _, point in timed_history)
        if not history:
            if raw_history:
                errors.append(f"{market.market_id}: no valid timestamped history")
            continue
        markets_with_history += 1
        market_has_books = any(_has_historical_book(point) for point in history)
        has_historical_books = has_historical_books or market_has_books
        if store is not None:
            version = _content_version(history)
            dataset_id = f"prediction:{market.market_id}"
            try:
                if store.load_dataset_record(dataset_id, version) is None:
                    store.save_dataset(
                        dataset_id,
                        version,
                        history,
                        metadata={
                            "provider": base["provider"],
                            "market_id": market.market_id,
                            "question": market.question,
                            "resolution_criteria": market.resolution_criteria,
                            "rules": market.resolution_criteria,
                            "expiry": market.expiry,
                            "settlement": market.settlement.value,
                            "yes_token_id": market.yes_token_id,
                            "no_token_id": market.no_token_id,
                            "historical_order_book": market_has_books,
                        },
                        quality=SimulationQuality.LOW,
                    )
            except ValueError as exc:
                errors.append(str(exc))
        outcome = _binary_outcome(market.settlement)
        price_rows = [
            (point, price)
            for point in history
            if (price := _price(point)) is not None and 0.0 <= price <= 1.0
        ]
        prices = [price for _, price in price_rows]
        if not prices:
            continue
        if outcome is None:
            continue
        resolved_market_groups.add(_resolved_market_group_key(market))
        benchmark_rows.extend(
            {
                "timestamp": _point_timestamp(point),
                "market_id": market.market_id,
                "yes_mid": price,
                "outcome": outcome,
                "settlement": market.settlement.value,
            }
            for point, price in price_rows
        )
        calibration_horizon_rows.extend(
            {
                "timestamp": _point_timestamp(point),
                "expiry": market.expiry,
                "market_id": market.market_id,
                "probability": price,
                "outcome": outcome,
            }
            for point, price in price_rows
            if _point_timestamp(point) is not None and market.expiry is not None
        )
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
        observed_liquidity = (
            history[-1].get("liquidity")
            if isinstance(history[-1], Mapping)
            else None
        )
        if observed_liquidity is not None:
            try:
                if math.isfinite(float(observed_liquidity)):
                    liquidity_errors.append(abs(last_price - outcome))
            except (TypeError, ValueError):
                pass
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
    base["independent_resolved_markets"] = len(resolved_market_groups)
    base["resolved_market_groups"] = len(resolved_market_groups)
    base["price_buckets"] = buckets
    base["calibration"] = {
        "observations": len(calibration_records),
        "brier": brier_score(calibration_records),
        "log_loss": log_loss(calibration_records),
        "ece": expected_calibration_error(calibration_records),
    }
    base["multi_horizon_calibration"] = calibration_at_horizons(calibration_horizon_rows)
    base["benchmarks"] = [item.as_record() for item in prediction_benchmarks(benchmark_rows)]
    base["historical_order_books_available"] = has_historical_books
    # This workflow computes price-history baselines only; merely storing a
    # book is not an executable simulation result.
    base["research_quality"] = ResearchQuality.PRICE_PROXY.value
    base["simulation_quality"] = (
        SimulationQuality.MEDIUM.value if markets_with_history else SimulationQuality.LOW.value
    )
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
        "Public Polymarket histories are price-only unless a source supplies timestamped depth; this report labels such samples PRICE_PROXY.",
        "The independent-market count is a deduplicated event/question/expiry grouping proxy, not a statistical independence claim.",
        "Market price is used only as a calibration baseline; no LLM opinion is used as a probability model.",
        "Bucket ROI is descriptive and does not establish executable profitability.",
        "Historical order-book execution requires timestamped books; current books are never backfilled into history.",
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
    """Write JSON by default and real Markdown for ``.md`` paths."""
    if str(path).lower().endswith((".md", ".markdown")):
        payload = report.to_markdown() if isinstance(report, ResearchReport) else _markdown_document(report)
        suffix = "" if payload.endswith("\n") else "\n"
    else:
        payload = report.to_json() if isinstance(report, ResearchReport) else json.dumps(_jsonable(report), sort_keys=True, indent=2)
        suffix = "\n"
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(payload + suffix)


def _markdown_document(report: Mapping[str, Any]) -> str:
    generated = report.get("generated_at", "unknown")
    crypto = report.get("crypto", {})
    prediction = report.get("prediction", {})
    limitations = report.get("limitations", ())
    lines = [
        "# Axiom Research Report",
        "",
        f"Generated: {generated}",
        "",
        "## Crypto",
        "",
        _markdown_section(crypto),
        "",
        "## Prediction markets",
        "",
        _markdown_section(prediction),
        "",
        "## Limitations",
        "",
    ]
    if isinstance(limitations, (list, tuple)) and limitations:
        lines.extend(f"- {item}" for item in limitations)
    else:
        lines.append("- None recorded.")
    return "\n".join(lines).rstrip() + "\n"
def _save_research_report_if_absent(
    store: Any,
    report_id: str,
    report: Mapping[str, Any],
    *,
    experiment_id: str | None = None,
) -> None:
    """Persist a deterministic report without turning retries into failures."""
    save_if_absent = getattr(store, "save_report_if_absent", None)
    if callable(save_if_absent):
        try:
            save_if_absent(report_id, report, experiment_id=experiment_id)
        except TypeError as first_error:
            try:
                save_if_absent(report_id, report)
            except TypeError:
                raise first_error
        return

    save_report = getattr(store, "save_report", None)
    if not callable(save_report):
        return
    try:
        save_report(report_id, report, experiment_id=experiment_id)
    except TypeError as first_error:
        try:
            save_report(report_id, report)
        except TypeError:
            raise first_error
    except ValueError:
        loader = getattr(store, "load_report", None)
        existing = loader(report_id) if callable(loader) else None
        if existing != report:
            raise


def _symbol_binding_value(value: Any, symbol: str, *, default: Any = None) -> Any:
    """Resolve a scalar or symbol-keyed binding without changing symbol spelling."""
    if not isinstance(value, Mapping):
        return value
    if symbol in value:
        return value[symbol]
    canonical = _canonical_symbol(symbol)
    for candidate, item in value.items():
        if _canonical_symbol(candidate) == canonical:
            return item
    return default


def _multi_symbol_dataset_binding_requested(
    dataset_id: str | Mapping[str, Any] | None,
    dataset_version: str | Mapping[str, Any] | None,
    timeframe: str | Mapping[str, Any],
    source_type: str | Mapping[str, Any],
) -> bool:
    """Return whether any selector makes this run require per-symbol binding."""
    return any(isinstance(value, Mapping) for value in (dataset_id, dataset_version, timeframe, source_type))


def _validate_multi_symbol_dataset_bindings(
    symbols: Sequence[str],
    *,
    dataset_id: str | Mapping[str, Any] | None,
    dataset_version: str | Mapping[str, Any] | None,
    timeframe: str | Mapping[str, Any],
    source_type: str | Mapping[str, Any],
) -> None:
    """Reject incomplete or invalid exact bindings before consulting any provider."""
    failures: list[str] = []
    for symbol in symbols:
        symbol_dataset_id = _symbol_binding_value(dataset_id, symbol)
        symbol_dataset_version = _symbol_binding_value(dataset_version, symbol)
        symbol_timeframe = _symbol_binding_value(
            timeframe,
            symbol,
            default=None if isinstance(timeframe, Mapping) else "1d",
        )
        symbol_source_type = _symbol_binding_value(
            source_type,
            symbol,
            default=None if isinstance(source_type, Mapping) else "HISTORICAL",
        )
        identifier = str(symbol_dataset_id or "").strip()
        version = str(symbol_dataset_version or "").strip()
        timeframe_value = str(symbol_timeframe or "").strip()
        source_value = str(symbol_source_type or "").strip().upper()
        reasons: list[str] = []
        if not identifier:
            reasons.append("dataset_id is missing or empty")
        elif identifier.casefold() in {"latest", "current", "unversioned"}:
            reasons.append("dataset_id must be an explicit persisted identifier")
        if not version:
            reasons.append("dataset_version is missing or empty")
        elif version.casefold() in {"latest", "current", "unversioned"}:
            reasons.append("dataset_version must be immutable and versioned")
        if not timeframe_value:
            reasons.append("timeframe is missing or empty")
        if source_value not in {"HISTORICAL", "FORWARD_COLLECTED"}:
            reasons.append("source_type must be HISTORICAL or FORWARD_COLLECTED")
        if reasons:
            failures.append(f"{symbol}: {', '.join(reasons)}")
    if failures:
        raise ValueError(
            "exact dataset binding requires every requested symbol to have valid selectors; "
            + "; ".join(failures)
        )


def _dataset_binding_requested(
    dataset_id: str | Mapping[str, Any] | None,
    dataset_version: str | Mapping[str, Any] | None,
    timeframe: str | Mapping[str, Any],
    source_type: str | Mapping[str, Any],
) -> bool:
    return (
        dataset_id is not None
        or dataset_version is not None
        or str(timeframe).strip() != "1d"
        or str(source_type).strip().upper() != "HISTORICAL"
    )


def _bounded_crypto_bars(
    rows: Sequence[OHLCVBar],
    *,
    start: datetime | None,
    end: datetime | None,
) -> tuple[OHLCVBar, ...]:
    """Apply the provider's inclusive UTC bounds before research truncation."""
    start_utc = ensure_utc(start) if start is not None else None
    end_utc = ensure_utc(end) if end is not None else None
    return tuple(
        islice(
            (
                bar
                for bar in rows
                if (start_utc is None or ensure_utc(bar.timestamp) >= start_utc)
                and (end_utc is None or ensure_utc(bar.timestamp) <= end_utc)
            ),
            _MAX_RESEARCH_RECORDS,
        )
    )


def _load_bound_crypto_dataset(
    store: AxiomStore | None,
    *,
    symbol: str,
    dataset_id: str | None,
    dataset_version: str | None,
    timeframe: str,
    source_type: str,
) -> dict[str, Any]:
    if store is None:
        raise ValueError("an AxiomStore is required for explicit dataset binding")
    identifier = str(dataset_id or "").strip()
    version = str(dataset_version or "").strip()
    timeframe_value = str(timeframe or "").strip()
    source_value = str(source_type or "").strip().upper()
    if not identifier or not version:
        raise ValueError("explicit dataset binding requires dataset_id and dataset_version")
    if identifier.casefold() in {"latest", "current", "unversioned"} or version.casefold() in {
        "latest",
        "current",
        "unversioned",
    }:
        raise ValueError("explicit dataset binding rejects latest or unversioned selectors")
    if not timeframe_value:
        raise ValueError("explicit dataset binding requires timeframe")
    if source_value not in {"HISTORICAL", "FORWARD_COLLECTED"}:
        raise ValueError("dataset source_type must be HISTORICAL or FORWARD_COLLECTED")

    catalog_loader = getattr(store, "load_dataset_catalog", None)
    if not callable(catalog_loader):
        raise ValueError("persisted dataset catalog is required")
    catalog = catalog_loader(identifier, version)
    if not isinstance(catalog, Mapping):
        raise ValueError("persisted dataset catalog is required")
    metadata = catalog.get("metadata")
    metadata = dict(metadata) if isinstance(metadata, Mapping) else {}

    def catalog_value(*names: str) -> Any:
        for name in names:
            value = catalog.get(name)
            if value is not None and (not isinstance(value, str) or value.strip()):
                return value
        for name in names:
            value = metadata.get(name)
            if value is not None and (not isinstance(value, str) or value.strip()):
                return value
        return None

    actual_id = str(catalog_value("dataset_id") or "").strip()
    actual_version = str(catalog_value("dataset_version", "version") or "").strip()
    if actual_id != identifier or actual_version != version:
        raise ValueError("persisted dataset identity does not match the requested selector")
    instrument = str(catalog_value("instrument") or "").strip()
    if not instrument:
        raise ValueError("persisted dataset catalog instrument is required")
    if _canonical_symbol(instrument) != _canonical_symbol(symbol):
        raise ValueError(f"persisted dataset instrument does not match symbol: {instrument} != {symbol}")
    market_type = str(catalog_value("market_type") or "").strip().lower()
    if market_type != MarketType.CRYPTO_SPOT.value:
        raise ValueError(f"persisted dataset is not crypto spot data: {market_type}")
    actual_timeframe = str(catalog_value("timeframe", "interval") or "").strip()
    if not actual_timeframe:
        raise ValueError("persisted dataset catalog timeframe is required")
    if actual_timeframe != timeframe_value:
        raise ValueError(f"persisted dataset timeframe does not match selector: {actual_timeframe} != {timeframe_value}")
    actual_source = str(catalog_value("source_type") or "").strip().upper()
    if not actual_source:
        raise ValueError("persisted dataset catalog source_type is required")
    if actual_source != source_value:
        raise ValueError(f"persisted dataset source_type does not match selector: {actual_source} != {source_value}")
    provenance = dict(catalog)

    # The dataset row is data only.  Its metadata cannot establish provenance
    # after the exact catalog record has been validated above.
    record = store.load_dataset_record(identifier, version)

    bars: list[OHLCVBar] = []
    try:
        bars = list(store.load_bars(instrument, dataset_id=identifier, dataset_version=version))
        if not bars and _canonical_symbol(instrument) != _canonical_symbol(symbol):
            bars = list(store.load_bars(symbol, dataset_id=identifier, dataset_version=version))
    except (AttributeError, TypeError, ValueError):
        bars = []
    if not bars and isinstance(record, Mapping):
        raw_records = record.get("records", ())
        if isinstance(raw_records, Sequence) and not isinstance(raw_records, (str, bytes, bytearray)):
            for raw in raw_records:
                if isinstance(raw, OHLCVBar):
                    bars.append(raw)
                elif isinstance(raw, Mapping):
                    timestamp = parse_timestamp(raw.get("timestamp"))
                    if timestamp is None:
                        raise ValueError("persisted crypto dataset contains a bar without timestamp")
                    bars.append(
                        OHLCVBar(
                            timestamp=timestamp,
                            open=float(raw["open"]),
                            high=float(raw["high"]),
                            low=float(raw["low"]),
                            close=float(raw["close"]),
                            volume=float(raw["volume"]),
                            spread=float(raw["spread"]) if raw.get("spread") is not None else None,
                            trades=int(raw["trades"]) if raw.get("trades") is not None else None,
                        )
                    )
    if not bars and int(provenance.get("row_count", 0) or 0) > 0:
        raise ValueError(f"persisted dataset has no bars for symbol: {symbol}")
    return {
        "dataset_id": identifier,
        "dataset_version": version,
        "timeframe": actual_timeframe,
        "source_type": actual_source,
        "provider": provenance.get("provider") or metadata.get("provider"),
        "quality": provenance.get("quality") or metadata.get("quality"),
        "instrument_metadata": metadata.get("instrument_metadata"),
        "bars": tuple(sorted(bars, key=lambda item: item.timestamp)),
        "provenance": provenance,
    }




def _markdown_section(value: Any) -> str:
    if not isinstance(value, Mapping):
        return f"- {value}"
    lines: list[str] = []
    for key, item in list(value.items())[:32]:
        if isinstance(item, Mapping):
            summary = json.dumps(_jsonable(item), sort_keys=True, separators=(",", ":"))
        elif isinstance(item, (list, tuple)):
            summary = f"{len(item)} entries"
        else:
            summary = str(_jsonable(item))
        lines.append(f"- **{key}:** {summary}")
    return "\n".join(lines) if lines else "- No result recorded."


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
    encoded = json.dumps(_jsonable(list(rows)), sort_keys=True, separators=(",", ":"))
    return "sha256:" + hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _regime_labels(bars: Sequence[Any]) -> list[str]:
    return [regime.state.value for regime in RegimeEngine().detect_crypto(bars).regimes]

def _empty_buckets() -> dict[str, dict[str, Any]]:
    return {
        label: {
            "lower": lower,
            "upper": upper,
            "count": 0,
            "resolved_count": 0,
            "wins": 0,
            "mean_price_sum": 0.0,
            "roi_sum": 0.0,
        }
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


def _resolved_market_group_key(market: PredictionMarketSnapshot) -> tuple[str, str]:
    event_id = getattr(market, "event_id", None)
    if event_id is not None and str(event_id).strip():
        return ("event:" + str(event_id).strip().casefold(), "")
    question = " ".join(str(market.question).casefold().split())
    question_hash = hashlib.sha256(question.encode("utf-8")).hexdigest()
    expiry = market.expiry.isoformat() if market.expiry is not None else ""
    return ("question:" + question_hash, expiry)
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
        number = float(value) if value is not None else None
    except (TypeError, ValueError):
        return None
    return number if number is not None and math.isfinite(number) and 0.0 <= number <= 1.0 else None


def _point_timestamp(point: Any) -> datetime | None:
    if not isinstance(point, Mapping):
        return None
    return parse_timestamp(point.get("timestamp", point.get("t")))


def _has_historical_book(point: Mapping[str, Any]) -> bool:
    raw = point.get("order_book", point.get("book"))
    if not isinstance(raw, Mapping):
        return hasattr(raw, "bids") and hasattr(raw, "asks") and bool(raw.bids) and bool(raw.asks)
    return bool(raw.get("bids")) and bool(raw.get("asks"))


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

def _consume_transport_errors(provider: Any, errors: list[str], context: str) -> None:
    consume = getattr(provider, "consume_transport_errors", None)
    if not callable(consume):
        return
    try:
        transport_errors = tuple(consume())
    except Exception as exc:
        errors.append(f"{context}: transport error collector failed: {exc}")
        return
    for error in transport_errors:
        status = getattr(error, "status", None)
        detail = f"HTTP {status}" if status is not None else str(error)
        errors.append(f"{context}: {detail}")


__all__ = [
    "ResearchReport",
    "run_crypto_research",
    "run_multi_symbol_crypto_research",
    "run_crypto_universe_research",
    "run_crypto_research_multi",
    "run_crypto_research_multi_symbol",
    "run_initial_research",
    "run_prediction_research",
    "write_report",
]
