"""Deterministic built-in strategy signal evaluators.

All evaluators consume domain objects or JSON-compatible context.  They return a
bounded score in ``[-1, 1]`` (positive means YES/buy, negative means NO/sell).
No evaluator accepts or executes user-provided code.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import math
from statistics import mean, pstdev
from typing import Any, Iterable, Mapping, Sequence

from axiom.domain import MarketType, OHLCVBar, PredictionMarketSnapshot, SettlementState
from .dsl import StrategyDefinition, validate_strategy


def _number(value: Any, default: float = 0.0) -> float:
    try:
        value = float(value)
    except (TypeError, ValueError):
        return default
    return value if math.isfinite(value) else default


def _bars(data: Any) -> list[OHLCVBar | Mapping[str, Any]]:
    if isinstance(data, Mapping):
        for key in ("bars", "ohlcv", "history", "observations"):
            if key in data:
                data = data[key]
                break
    return list(data) if isinstance(data, Iterable) and not isinstance(data, (str, bytes, Mapping)) else []


def _field(item: Any, name: str, default: float = 0.0) -> float:
    if isinstance(item, Mapping):
        return _number(item.get(name), default)
    return _number(getattr(item, name, default), default)


def _closes(items: Sequence[Any]) -> list[float]:
    return [_field(item, "close") for item in items]


def _volumes(items: Sequence[Any]) -> list[float]:
    return [_field(item, "volume") for item in items]


def _sma(values: Sequence[float], window: int) -> float:
    values = values[-max(1, window):]
    return mean(values) if values else 0.0


def _rsi(values: Sequence[float], period: int = 14) -> float:
    values = values[-(period + 1):]
    if len(values) < 2:
        return 50.0
    gains = [max(0.0, b - a) for a, b in zip(values, values[1:])]
    losses = [max(0.0, a - b) for a, b in zip(values, values[1:])]
    avg_gain, avg_loss = mean(gains), mean(losses)
    if avg_loss == 0:
        return 100.0 if avg_gain else 50.0
    return 100.0 - (100.0 / (1.0 + avg_gain / avg_loss))


def _clip(value: float, low: float = -1.0, high: float = 1.0) -> float:
    return max(low, min(high, value)) if math.isfinite(value) else 0.0


def _crypto_signal(family: str, items: Sequence[Any], params: Mapping[str, Any]) -> float:
    closes = _closes(items)
    volumes = _volumes(items)
    if not closes:
        return 0.0
    current = closes[-1]
    lookback = max(1, int(params.get("lookback", params.get("window", 14))))
    threshold = _number(params.get("threshold", params.get("entry_threshold", 0.02)), 0.02)
    if family == "dip":
        recent = closes[-(lookback + 1):]
        peak = max(recent) if recent else current
        drawdown = current / peak - 1.0 if peak else 0.0
        if drawdown >= 0:
            return 0.0
        return _clip(-drawdown / max(threshold, 1e-12))
    if family == "momentum":
        if len(closes) <= lookback or closes[-lookback - 1] == 0:
            return 0.0
        return _clip((current / closes[-lookback - 1] - 1.0) / max(threshold, 1e-12))
    if family == "trend":
        fast = max(1, int(params.get("fast", min(lookback, 10))))
        slow = max(fast + 1, int(params.get("slow", max(lookback, 30))))
        if len(closes) < fast or len(closes) < slow:
            return 0.0
        baseline = _sma(closes, slow)
        return _clip(((_sma(closes, fast) / baseline) - 1.0) / max(threshold, 1e-12)) if baseline else 0.0
    if family == "mean_reversion":
        window = max(2, lookback)
        sample = closes[-window:]
        centre, deviation = mean(sample), pstdev(sample)
        if deviation <= 1e-12:
            return 0.0
        # Price below its mean creates a positive (long) signal.
        return _clip((centre - current) / (deviation * max(_number(params.get("sigma", 2.0), 2.0), 1e-12)))
    if family == "breakout":
        prior = closes[-(lookback + 1):-1]
        if not prior:
            return 0.0
        high, low = max(prior), min(prior)
        if current > high:
            return _clip((current / high - 1.0) / max(threshold, 1e-12)) if high else 1.0
        if current < low:
            return _clip(-(1.0 - current / low) / max(threshold, 1e-12)) if low else -1.0
        return 0.0
    if family == "volatility":
        window = max(2, lookback)
        sample = closes[-(window + 1):]
        returns = [b / a - 1.0 for a, b in zip(sample, sample[1:]) if a]
        observed = pstdev(returns) if returns else 0.0
        target = _number(params.get("target", params.get("volatility", threshold)), threshold)
        if target <= 0:
            return 0.0
        # A positive score means volatility is below the target (prefer risk-on).
        return _clip((target - observed) / target)
    if family == "rsi":
        value = _rsi(closes, max(2, int(params.get("period", lookback))))
        oversold = _number(params.get("oversold", 30.0), 30.0)
        overbought = _number(params.get("overbought", 70.0), 70.0)
        if value < oversold:
            return _clip((oversold - value) / max(oversold, 1.0))
        if value > overbought:
            return _clip(-(value - overbought) / max(100.0 - overbought, 1.0))
        return 0.0
    if family == "volume_filter":
        if not volumes:
            return 0.0
        baseline = _sma(volumes[:-1] if len(volumes) > 1 else volumes, lookback)
        multiplier = max(0.0, _number(params.get("multiplier", 1.0), 1.0))
        if baseline <= 0 or volumes[-1] < baseline * multiplier:
            return 0.0
        if len(closes) > 1:
            return _clip(1.0 if closes[-1] >= closes[-2] else -1.0)
        return 1.0
    return 0.0


def _snapshot_value(item: Any, key: str, default: Any = None) -> Any:
    if isinstance(item, Mapping):
        return item.get(key, default)
    return getattr(item, key, default)


def _snapshots(data: Any) -> list[Any]:
    if isinstance(data, Mapping):
        for key in ("snapshots", "markets", "history", "observations"):
            if key in data:
                data = data[key]
                break
    return list(data) if isinstance(data, Iterable) and not isinstance(data, (str, bytes, Mapping)) else []


def _market_probability(item: Any) -> float | None:
    value = _snapshot_value(item, "yes_mid")
    if value is None:
        value = _snapshot_value(item, "yes_ask")
    result = _number(value, math.nan)
    return result if math.isfinite(result) else None


def _model_probability(data: Any, item: Any, index: int = -1) -> float | None:
    for key in ("model_probability", "probability", "predicted_probability", "p"):
        value = _snapshot_value(item, key)
        if value is not None:
            result = _number(value, math.nan)
            return result if math.isfinite(result) else None
    if isinstance(data, Mapping):
        values = data.get("probabilities", data.get("model_probabilities"))
        if isinstance(values, Mapping):
            market_id = _snapshot_value(item, "market_id")
            value = values.get(market_id, values.get(str(index)))
        elif isinstance(values, Sequence) and not isinstance(values, (str, bytes)):
            try:
                value = values[index]
            except IndexError:
                value = None
        else:
            value = None
        result = _number(value, math.nan)
        if math.isfinite(result):
            return result
    return None


def _time_to_expiry(item: Any) -> float | None:
    value = _snapshot_value(item, "time_to_expiry_seconds")
    if value is not None and not callable(value):
        result = _number(value, math.nan)
        if math.isfinite(result):
            return result
    timestamp = _snapshot_value(item, "timestamp")
    expiry = _snapshot_value(item, "expiry")
    if isinstance(timestamp, datetime) and isinstance(expiry, datetime):
        return (expiry - timestamp).total_seconds()
    return None


def _prediction_signal(family: str, data: Any, params: Mapping[str, Any]) -> float:
    snapshots = _snapshots(data)
    if not snapshots:
        return 0.0
    current = snapshots[-1]
    market_p = _market_probability(current)
    model_p = _model_probability(data, current, -1)
    edge = (model_p - market_p) if model_p is not None and market_p is not None else 0.0
    threshold = max(_number(params.get("threshold", params.get("min_edge", 0.0)), 0.0), 1e-12)
    if family == "probability_mispricing":
        return _clip(edge / threshold) if threshold > 1e-12 else _clip(edge)
    if family == "tails":
        tail = _number(params.get("tail_probability", params.get("quantile", 0.1)), 0.1)
        if model_p is None or market_p is None:
            return 0.0
        if model_p <= tail and market_p > model_p:
            return _clip((market_p - model_p) / max(tail, 1e-12))
        if model_p >= 1.0 - tail and market_p < model_p:
            return _clip((market_p - model_p) / max(tail, 1e-12))
        return 0.0
    history_market = [p for p in (_market_probability(s) for s in snapshots) if p is not None]
    history_model = [_model_probability(data, s, i) for i, s in enumerate(snapshots)]
    history_model = [p for p in history_model if p is not None]
    if family == "mean_reversion":
        if len(history_market) < 2:
            return 0.0
        centre = mean(history_market[:-1]) if len(history_market) > 1 else history_market[-1]
        return _clip((centre - history_market[-1]) / max(threshold, 0.05))
    if family == "momentum":
        if len(history_market) < 2:
            return 0.0
        return _clip((history_market[-1] - history_market[0]) / max(threshold, 0.05))
    if family == "time_decay":
        seconds = _time_to_expiry(current)
        horizon = max(_number(params.get("horizon", 86400.0), 86400.0), 1.0)
        if seconds is None:
            return 0.0
        return _clip(edge * max(0.0, min(1.0, seconds / horizon)) / max(threshold, 0.05))
    if family == "consistency":
        if not history_model or not history_market:
            return 0.0
        paired = [(a, b) for a, b in zip(history_model, history_market)]
        average_edge = mean(a - b for a, b in paired)
        return _clip(average_edge / max(threshold, 0.05))
    if family == "cross_asset":
        peers = data.get("peer_probabilities", ()) if isinstance(data, Mapping) else ()
        peer_values = [_number(v, math.nan) for v in peers] if isinstance(peers, Iterable) else []
        peer_values = [v for v in peer_values if math.isfinite(v)]
        if market_p is None or not peer_values:
            return 0.0
        return _clip((mean(peer_values) - market_p) / max(threshold, 0.05))
    if family == "event_frequency":
        if isinstance(data, Mapping):
            observed = _number(data.get("event_count", data.get("events", 0.0)))
            horizon = max(_number(data.get("event_horizon", params.get("horizon", 1.0)), 1.0), 1e-12)
            expected = _number(data.get("expected_event_rate", params.get("expected_rate", 0.0))) * horizon
            if expected:
                return _clip((observed - expected) / max(abs(expected), 1.0))
        return 0.0
    if family == "liquidity":
        liquidity = _number(_snapshot_value(current, "liquidity"), 0.0)
        minimum = _number(params.get("min_liquidity", 0.0), 0.0)
        if minimum <= 0:
            return _clip(liquidity / (liquidity + 1.0))
        return _clip((liquidity - minimum) / minimum)
    if family == "correlation_aware":
        correlation = _number(data.get("correlation", 0.0), 0.0) if isinstance(data, Mapping) else 0.0
        penalty = max(0.0, min(1.0, abs(correlation)))
        return _clip(edge * (1.0 - penalty) / max(threshold, 0.05))
    return 0.0


def _operation_value(operation: Mapping[str, Any], values: Sequence[float]) -> float:
    op = operation.get("op")
    args = operation.get("args", values)
    if not isinstance(args, Sequence) or isinstance(args, (str, bytes)):
        args = [args]
    nums = [_number(item) for item in args]
    if op == "constant":
        return _clip(_number(operation.get("value", nums[0] if nums else 0.0)))
    if op in {"and", "or"}:
        truth = all(bool(item) for item in nums) if op == "and" else any(bool(item) for item in nums)
        return 1.0 if truth else 0.0
    if op == "not":
        return 0.0 if nums and bool(nums[0]) else 1.0
    if len(nums) < 2:
        return 0.0
    left, right = nums[0], nums[1]
    if op == "gt": return 1.0 if left > right else 0.0
    if op == "gte": return 1.0 if left >= right else 0.0
    if op == "lt": return 1.0 if left < right else 0.0
    if op == "lte": return 1.0 if left <= right else 0.0
    if op == "eq": return 1.0 if left == right else 0.0
    if op == "between":
        high = nums[2] if len(nums) > 2 else _number(operation.get("threshold"))
        return 1.0 if high >= left >= right else 0.0
    if op == "change": return _clip(left - right)
    if op in {"probability_edge", "zscore", "volume_ratio", "correlation", "liquidity", "event_frequency", "time_to_expiry", "sma", "ema", "rsi"}:
        return _clip(left)
    return 0.0


@dataclass(frozen=True, slots=True)
class Signal:
    family: str
    score: float
    side: str
    market_type: str

    @property
    def actionable(self) -> bool:
        return abs(self.score) > 1e-12


def evaluate_signal(strategy: StrategyDefinition | Mapping[str, Any] | str, data: Any) -> float:
    """Evaluate a validated strategy and return a deterministic score in ``[-1, 1]``."""
    definition = validate_strategy(strategy) if not isinstance(strategy, StrategyDefinition) else strategy
    if definition.market_type is MarketType.CRYPTO_SPOT:
        score = _crypto_signal(definition.family, _bars(data), definition.parameters)
    else:
        score = _prediction_signal(definition.family, data, definition.parameters)
    if definition.operations:
        operation_values = [score]
        for operation in definition.operations:
            operation_values.append(_operation_value(operation, operation_values))
        score = _clip(operation_values[-1])
    return _clip(score)


def evaluate_signal_record(strategy: StrategyDefinition | Mapping[str, Any] | str, data: Any) -> Signal:
    definition = validate_strategy(strategy) if not isinstance(strategy, StrategyDefinition) else strategy
    score = evaluate_signal(definition, data)
    side = "buy" if score > 0 else "sell" if score < 0 else "flat"
    return Signal(definition.family, score, side, definition.market_type.value)


def evaluate_crypto_family(family: str, bars: Sequence[OHLCVBar | Mapping[str, Any]], **parameters: Any) -> float:
    return _crypto_signal(family.strip().lower(), bars, parameters)


def evaluate_prediction_family(family: str, snapshots: Sequence[PredictionMarketSnapshot | Mapping[str, Any]], **parameters: Any) -> float:
    return _prediction_signal(family.strip().lower(), snapshots, parameters)


class BuiltinSignalEvaluator:
    """Registry-free evaluator facade, useful for deterministic offline callers."""

    def evaluate(self, strategy: StrategyDefinition | Mapping[str, Any] | str, data: Any) -> float:
        return evaluate_signal(strategy, data)

    __call__ = evaluate
SignalEvaluator = BuiltinSignalEvaluator


__all__ = [
    "BuiltinSignalEvaluator", "Signal", "SignalEvaluator", "evaluate_crypto_family",
    "evaluate_prediction_family", "evaluate_signal", "evaluate_signal_record",
]


