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
from .data import BinanceAdapter, PolymarketAdapter
from .domain import MarketType, PredictionMarketSnapshot, ResearchQuality, SettlementState, SimulationQuality, parse_timestamp, to_record
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
    universe: Mapping[str, Any] | None = None,
    universe_provenance: Mapping[str, Any] | None = None,
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
        )
    provider = provider or BinanceAdapter(timeout=timeout)
    errors: list[str] = []
    try:
        bars = tuple(islice(iter(provider.historical_ohlcv(symbol, start=start, end=end, interval="1d")), _MAX_RESEARCH_RECORDS))
    except Exception as exc:  # network adapters must not abort the report
        bars = ()
        errors.append(f"crypto data error: {exc}")
    _consume_transport_errors(provider, errors, "crypto historical data")
    source_quality = _research_quality(provider, SimulationQuality.MEDIUM if bars else SimulationQuality.LOW)
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
        "provider": str(getattr(provider, "provider_name", provider.__class__.__name__)),
        "instrument": symbol,
        "instrument_metadata": to_record(instrument_metadata) if instrument_metadata is not None else None,
        "instrument_metadata_available": instrument_metadata is not None,
        "bars": len(bars),
        "historical_coverage": coverage,
        "dataset_version": "",
        "simulation_quality": source_quality.value,
        "experiments": [],
        "validation": {},
        "errors": errors,
    }
    if universe_provenance is not None:
        base["universe_provenance"] = _jsonable(dict(universe_provenance))
    if len(bars) < 6:
        base["limitations"] = ["fewer than six OHLCV bars; no train/validation/holdout result"]
        return base

    split, version = _chronological_split(bars)
    base["dataset_version"] = version
    base["benchmarks"] = [item.as_record() for item in crypto_benchmarks(bars, initial_cash=initial_cash, fee_bps=10.0, slippage_bps=5.0, symbol=symbol)]
    dataset_id = f"crypto:{symbol.replace('/', '').replace('-', '').upper()}"
    if store is not None:
        try:
            existing = store.load_dataset_record(dataset_id, version)
            with store.transaction():
                if existing is None:
                    store.save_dataset(
                        dataset_id,
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
                if not store.load_bars(symbol, dataset_id=dataset_id, dataset_version=version):
                    store.save_bars(symbol, bars, dataset_id=dataset_id, dataset_version=version)
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
    provider: Any | None = None,
    symbol_providers: Mapping[str, Any] | None = None,
    universe: Mapping[str, Any] | None = None,
    universe_provenance: Mapping[str, Any] | None = None,
    universe_id: Any | None = None,
    universe_version: Any | None = None,
    methodology: Any | None = None,
    survivorship_bias: Any | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
    initial_cash: float = 10_000.0,
    store: AxiomStore | None = None,
    timeout: float = 10.0,
    max_symbols: int = 50,
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
    provenance = _research_universe_provenance(
        universe,
        universe_provenance,
        universe_id=universe_id,
        universe_version=universe_version,
        methodology=methodology,
        survivorship_bias=survivorship_bias,
    )
    declared_symbols = provenance.get("selected_symbols", provenance.get("symbols"))
    if isinstance(declared_symbols, Sequence) and not isinstance(declared_symbols, (str, bytes, bytearray)):
        declared = {_canonical_symbol(item) for item in declared_symbols}
        unexpected = sorted(symbol for symbol in requested if _canonical_symbol(symbol) not in declared)
        if unexpected:
            raise ValueError(f"symbols are not members of the declared universe: {unexpected}")
    per_symbol: dict[str, dict[str, Any]] = {}
    errors: list[str] = []
    for symbol in requested:
        symbol_provider = _provider_for_symbol(symbol, provider_inputs, provider)
        if symbol_provider is None:
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
                    universe_provenance=provenance,
                )
            except Exception as exc:  # preserve one bad asset without aborting the universe
                result = _failed_crypto_symbol_result(symbol, f"crypto research error: {exc}")
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
    return _jsonable(report)


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
    universe: Mapping[str, Any] | None,
    supplied: Mapping[str, Any] | None,
    *,
    universe_id: Any | None,
    universe_version: Any | None,
    methodology: Any | None,
    survivorship_bias: Any | None,
) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for source in (universe, supplied):
        if source is not None:
            if not isinstance(source, Mapping):
                raise TypeError("universe provenance must be a mapping")
            result.update(dict(source))
    nested = result.get("provenance")
    if isinstance(nested, Mapping):
        merged = dict(nested)
        merged.update(result)
        result = merged
    def choose(explicit: Any | None, *keys: str, default: Any = None) -> Any:
        if explicit is not None:
            return explicit
        for key in keys:
            if key in result and result[key] not in (None, ""):
                return result[key]
        return default
    resolved_id = choose(universe_id, "universe_id", "id")
    resolved_version = choose(universe_version, "universe_version", "version", "snapshot_hash", "snapshot_version")
    if resolved_id in (None, "") or resolved_version in (None, ""):
        raise ValueError("versioned universe provenance requires universe_id and universe_version")
    result["universe_id"] = resolved_id
    result["universe_version"] = resolved_version
    result["methodology"] = choose(methodology, "methodology", "method", "policy", default="unspecified")
    result["survivorship_bias"] = choose(
        survivorship_bias,
        "survivorship_bias",
        "survivorship",
        default="unspecified",
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
