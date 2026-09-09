"""Deterministic built-in strategy signal evaluators.

All evaluators consume domain objects or JSON-compatible context.  They return a
bounded score in ``[-1, 1]`` (positive means YES/buy, negative means NO/sell).
No evaluator accepts or executes user-provided code.
"""
from __future__ import annotations
from dataclasses import dataclass, field

from collections.abc import Mapping as MappingABC
from datetime import datetime, timezone
import math
from statistics import mean, pstdev
from typing import Any, Iterable, Mapping, Sequence

from axiom.domain import MarketType, OHLCVBar, PredictionMarketSnapshot, SettlementState, parse_timestamp
from .dsl import StrategyDefinition, validate_strategy


MODEL_INPUT_MISSING = "MODEL_INPUT_MISSING"
WARMING_UP = "WARMING_UP"
INSUFFICIENT_LOOKBACK = "INSUFFICIENT_LOOKBACK"
STRATEGY_EVALUATED_DECLINED = "STRATEGY_EVALUATED_DECLINED"
SIGNAL_PRODUCED = "SIGNAL_PRODUCED"
MODEL_INPUT_PRESENT = "MODEL_INPUT_PRESENT"
CONSTANT_BASELINE = "CONSTANT_BASELINE"


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
        if len(closes) < window:
            return 0.0
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


def _snapshot_time(item: Any) -> datetime:
    for key in ("source_timestamp", "as_of_timestamp", "asof_timestamp", "as_of", "timestamp", "observed_at"):
        stamp = parse_timestamp(_snapshot_value(item, key))
        if stamp is not None:
            return stamp
    return datetime.min.replace(tzinfo=timezone.utc)


def _snapshots(data: Any) -> list[Any]:
    if isinstance(data, Mapping):
        for key in ("snapshots", "markets", "history", "observations"):
            if key in data:
                data = data[key]
                break
    return list(data) if isinstance(data, Iterable) and not isinstance(data, (str, bytes, Mapping)) else []

def _ordered_prediction_snapshots(data: Any) -> list[Any]:
    snapshots = _snapshots(data)
    if not snapshots:
        return []
    ordered = sorted(
        snapshots,
        key=lambda item: (
            _snapshot_time(item),
            str(_snapshot_value(item, "source_snapshot_id", "")),
            str(_snapshot_value(item, "market_id", "")),
        ),
    )
    # Signal histories are per market.  When a caller supplies a mixed batch,
    # retain only the current market rather than stitching unrelated events.
    current_market = _snapshot_value(ordered[-1], "market_id", None)
    if isinstance(data, Mapping):
        current_market = data.get("market_id", current_market)
    current_market = str(current_market or "").strip()
    if current_market:
        same_market = [
            item for item in ordered
            if str(_snapshot_value(item, "market_id", "")).strip() == current_market
        ]
        if same_market:
            ordered = same_market
    return ordered


def _market_probability(item: Any) -> float | None:
    value = _snapshot_value(item, "yes_mid")
    if value is None:
        value = _snapshot_value(item, "yes_ask")
    result = _number(value, math.nan)
    return result if math.isfinite(result) and 0.0 <= result <= 1.0 else None


def _model_probability(data: Any, item: Any, index: int = -1) -> float | None:
    for key in ("model_probability", "probability", "predicted_probability", "p"):
        value = _snapshot_value(item, key)
        if value is not None:
            result = _number(value, math.nan)
            return result if math.isfinite(result) and 0.0 <= result <= 1.0 else None
    if isinstance(data, Mapping):
        model_document = data.get("model_document", data.get("model"))
        if isinstance(model_document, Mapping):
            probability = evaluate_model_document_probability(model_document, item)
            if probability is not None:
                return probability
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
        if math.isfinite(result) and 0.0 <= result <= 1.0:
            return result


@dataclass(frozen=True, slots=True)
class ModelProbabilityEvaluation(MappingABC[str, Any]):
    """Canonical probability result shared by backtest and paper consumers."""

    probability: float | None
    reason_code: str
    evidence: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        probability = self.probability
        if probability is not None:
            try:
                probability = float(probability)
            except (TypeError, ValueError):
                probability = None
            if probability is None or not math.isfinite(probability) or not 0.0 <= probability <= 1.0:
                probability = None
        object.__setattr__(self, "probability", probability)
        object.__setattr__(self, "reason_code", str(self.reason_code).strip().upper() or MODEL_INPUT_MISSING)
        object.__setattr__(self, "evidence", dict(self.evidence) if isinstance(self.evidence, Mapping) else {})

    @property
    def available(self) -> bool:
        return self.probability is not None

    @property
    def reason(self) -> str:
        return self.reason_code

    def as_record(self) -> dict[str, Any]:
        return {
            "probability": self.probability,
            "reason_code": self.reason_code,
            "evidence": dict(self.evidence),
        }

    def __getitem__(self, key: str) -> Any:
        return self.as_record()[key]

    def get(self, key: str, default: Any = None) -> Any:
        return self.as_record().get(key, default)
    def __iter__(self):
        return iter(self.as_record())

    def __len__(self) -> int:
        return 3

def evaluate_model_document(
    model_document: Mapping[str, Any] | None,
    observation: Mapping[str, Any] | Any,
) -> ModelProbabilityEvaluation:
    """Evaluate one persisted model document and retain provenance.

    A constant document is executable, but is explicitly marked as a baseline
    rather than being mistaken for an observation-derived model.
    """
    if not isinstance(model_document, Mapping):
        return ModelProbabilityEvaluation(None, MODEL_INPUT_MISSING, {"model_source": "MISSING"})
    value: Any = None
    constant_key: str | None = None
    evidence: dict[str, Any] = {"model_source": "FIELD"}
    if "probability" in model_document:
        value = model_document["probability"]
        constant_key = "probability"
    elif "yes_probability" in model_document:
        value = model_document["yes_probability"]
        constant_key = "yes_probability"
    else:
        field_name = model_document.get("field")
        if isinstance(field_name, str) and field_name.strip():
            field_name = field_name.strip()
            value = (
                observation.get(field_name)
                if isinstance(observation, Mapping)
                else getattr(observation, field_name, None)
            )
            if value is None:
                return ModelProbabilityEvaluation(
                    None,
                    MODEL_INPUT_MISSING,
                    {"model_source": "FIELD", "field": field_name},
                )
            evidence = {"model_source": "FIELD", "field": field_name}
        else:
            return ModelProbabilityEvaluation(None, MODEL_INPUT_MISSING, {"model_source": "MISSING"})
    if isinstance(value, Mapping):
        value = value.get("probability", value.get("yes_probability", value.get("prediction")))
    try:
        probability = float(value)
    except (TypeError, ValueError):
        return ModelProbabilityEvaluation(
            None,
            MODEL_INPUT_MISSING,
            {"model_source": "CONSTANT_BASELINE" if constant_key else "FIELD", "field": constant_key},
        )
    if not math.isfinite(probability) or not 0.0 <= probability <= 1.0:
        return ModelProbabilityEvaluation(
            None,
            MODEL_INPUT_MISSING,
            {"model_source": "CONSTANT_BASELINE" if constant_key else "FIELD", "field": constant_key},
        )
    if constant_key is not None:
        evidence = {
            "model_source": CONSTANT_BASELINE,
            "model_document_type": CONSTANT_BASELINE,
            "field": constant_key,
        }
    return ModelProbabilityEvaluation(probability, MODEL_INPUT_PRESENT, evidence)


def evaluate_model_probability_evidence(
    model: Any | None,
    observation: Mapping[str, Any] | Any,
) -> ModelProbabilityEvaluation:
    """Evaluate a model object using the same semantics as persisted documents."""
    if isinstance(model, Mapping):
        return evaluate_model_document(model, observation)
    if model is None:
        return ModelProbabilityEvaluation(None, MODEL_INPUT_MISSING, {"model_source": "MISSING"})
    value: Any = None
    method_name: str | None = None
    for name in ("predict_probability", "probability", "predict", "estimate"):
        method = getattr(model, name, None)
        if not callable(method):
            continue
        try:
            value = method(observation)
        except (TypeError, AttributeError):
            continue
        method_name = name
        break
    if value is None and callable(model):
        try:
            value = model(observation)
            method_name = "__call__"
        except (TypeError, AttributeError):
            value = None
    if value is None:
        document = getattr(model, "document", None)
        if isinstance(document, Mapping):
            return evaluate_model_document(document, observation)
    if isinstance(value, Mapping):
        value = value.get("probability", value.get("yes_probability", value.get("prediction")))
    try:
        probability = float(value)
    except (TypeError, ValueError):
        return ModelProbabilityEvaluation(None, MODEL_INPUT_MISSING, {"model_source": method_name or "MISSING"})
    if not math.isfinite(probability) or not 0.0 <= probability <= 1.0:
        return ModelProbabilityEvaluation(None, MODEL_INPUT_MISSING, {"model_source": method_name or "MISSING"})
    return ModelProbabilityEvaluation(probability, MODEL_INPUT_PRESENT, {"model_source": method_name or "MODEL"})


def evaluate_model_document_probability(
    model_document: Mapping[str, Any] | None,
    observation: Mapping[str, Any] | Any,
) -> float | None:
    """Numeric compatibility API for persisted model documents."""
    return evaluate_model_document(model_document, observation).probability


evaluate_model_probability = evaluate_model_document_probability



def _time_to_expiry(item: Any) -> float | None:
    value = _snapshot_value(item, "time_to_expiry_seconds")
    if value is not None and not callable(value):
        result = _number(value, math.nan)
        if math.isfinite(result):
            return result
    timestamp = parse_timestamp(_snapshot_value(item, "timestamp"))
    expiry = parse_timestamp(_snapshot_value(item, "expiry"))
    if timestamp is not None and expiry is not None:
        return (expiry - timestamp).total_seconds()
    return None
def _declared_lookback(params: Mapping[str, Any], default: int = 1) -> int:
    raw = params.get("lookback", params.get("window", default))
    try:
        value = int(raw)
    except (TypeError, ValueError, OverflowError):
        return max(1, default)
    return max(1, value)


def _prediction_requires_model(family: str) -> bool:
    return family in {
        "probability_mispricing",
        "tails",
        "lottery_ticket",
        "time_decay",
        "consistency",
        "correlation_aware",
    }


def _prediction_signal(family: str, data: Any, params: Mapping[str, Any]) -> float:
    snapshots = _ordered_prediction_snapshots(data)
    if not snapshots:
        return 0.0
    current = snapshots[-1]
    market_p = _market_probability(current)
    model_p = _model_probability(data, current, len(snapshots) - 1)
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
    if family == "lottery_ticket":
        maximum_price = _number(params.get("max_probability", params.get("tail_probability", 0.10)), 0.10)
        minimum_edge = max(_number(params.get("min_edge", params.get("threshold", 0.0)), 0.0), 0.0)
        if (
            model_p is None
            or market_p is None
            or market_p > maximum_price
            or model_p <= market_p + minimum_edge
        ):
            return 0.0
        return _clip((model_p - market_p) / max(threshold, 0.05))
    history_market = [market for market in (_market_probability(s) for s in snapshots) if market is not None]
    paired = []
    for index, snapshot in enumerate(snapshots):
        market = _market_probability(snapshot)
        model = _model_probability(data, snapshot, index)
        if market is not None and model is not None:
            paired.append((model, market))
    history_model = [model for model, _ in paired]
    history_market_paired = [market for _, market in paired]
    lookback = _declared_lookback(params)
    if family == "mean_reversion":
        if len(history_market) < lookback + 1:
            return 0.0
        prior = history_market[:-1]
        centre = mean(prior[-lookback:])
        return _clip((centre - history_market[-1]) / max(threshold, 0.05))
    if family == "momentum":
        if len(history_market) < lookback + 1:
            return 0.0
        return _clip((history_market[-1] - history_market[-lookback - 1]) / max(threshold, 0.05))
    if family == "time_decay":
        seconds = _time_to_expiry(current)
        horizon = max(_number(params.get("horizon", 86400.0), 86400.0), 1.0)
        if seconds is None:
            return 0.0
        return _clip(edge * max(0.0, min(1.0, seconds / horizon)) / max(threshold, 0.05))
    if family == "consistency":
        if not history_model or not history_market_paired:
            return 0.0
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
class Signal(MappingABC[str, Any]):
    family: str
    score: float
    side: str
    market_type: str
    reason_code: str = STRATEGY_EVALUATED_DECLINED
    evidence: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "score", _clip(float(self.score)))
        object.__setattr__(self, "reason_code", str(self.reason_code).strip().upper() or STRATEGY_EVALUATED_DECLINED)
        object.__setattr__(self, "evidence", dict(self.evidence) if isinstance(self.evidence, Mapping) else {})

    @property
    def actionable(self) -> bool:
        return abs(self.score) > 1e-12
    @property
    def reason(self) -> str:
        return self.reason_code

    @property
    def evaluation_reason(self) -> str:
        return self.reason_code

    def as_record(self) -> dict[str, Any]:
        return {
            "family": self.family,
            "score": self.score,
            "side": self.side,
            "market_type": self.market_type,
            "reason_code": self.reason_code,
            "reason": self.reason_code,
            "evidence": dict(self.evidence),
        }

    def __getitem__(self, key: str) -> Any:
        return self.as_record()[key]

    def get(self, key: str, default: Any = None) -> Any:
        return self.as_record().get(key, default)
    def __iter__(self):
        return iter(self.as_record())

    def __len__(self) -> int:
        return 7


def _score_for_definition(definition: StrategyDefinition, data: Any) -> float:
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


def _model_evidence(data: Any, current: Any, index: int) -> ModelProbabilityEvaluation:
    prior_evidence = _snapshot_value(current, "model_evaluation")
    if isinstance(prior_evidence, Mapping) and prior_evidence.get("probability") is not None:
        return ModelProbabilityEvaluation(
            prior_evidence.get("probability"),
            str(prior_evidence.get("reason_code") or MODEL_INPUT_PRESENT),
            prior_evidence.get("evidence", {}),
        )
    if isinstance(data, Mapping):
        top_evidence = data.get("model_evaluation")
        if isinstance(top_evidence, Mapping) and top_evidence.get("probability") is not None:
            return ModelProbabilityEvaluation(
                top_evidence.get("probability"),
                str(top_evidence.get("reason_code") or MODEL_INPUT_PRESENT),
                top_evidence.get("evidence", {}),
            )
    for key in ("model_probability", "probability", "predicted_probability", "p"):
        value = _snapshot_value(current, key)
        if value is not None:
            try:
                probability = float(value)
            except (TypeError, ValueError):
                probability = None
            if probability is not None and math.isfinite(probability) and 0.0 <= probability <= 1.0:
                return ModelProbabilityEvaluation(
                    probability,
                    MODEL_INPUT_PRESENT,
                    {"model_source": "OBSERVATION", "field": key},
                )
            return ModelProbabilityEvaluation(
                None,
                MODEL_INPUT_MISSING,
                {"model_source": "OBSERVATION", "field": key},
            )
    if isinstance(data, Mapping):
        model = data.get("model_document", data.get("model"))
        if model is not None:
            return evaluate_model_probability_evidence(model, current)
        values = data.get("probabilities", data.get("model_probabilities"))
        if isinstance(values, Mapping):
            value = values.get(_snapshot_value(current, "market_id"), values.get(str(index)))
        elif isinstance(values, Sequence) and not isinstance(values, (str, bytes)):
            try:
                value = values[index]
            except IndexError:
                value = None
        else:
            value = None
        if value is not None:
            try:
                probability = float(value)
            except (TypeError, ValueError):
                probability = None
            if probability is not None and math.isfinite(probability) and 0.0 <= probability <= 1.0:
                return ModelProbabilityEvaluation(
                    probability,
                    MODEL_INPUT_PRESENT,
                    {"model_source": "INPUT_SEQUENCE"},
                )
    return ModelProbabilityEvaluation(None, MODEL_INPUT_MISSING, {"model_source": "MISSING"})


def evaluate_signal_evaluation(
    strategy: StrategyDefinition | Mapping[str, Any] | str,
    data: Any,
) -> Signal:
    """Evaluate a strategy and expose the state a consumer must act on."""
    definition = validate_strategy(strategy) if not isinstance(strategy, StrategyDefinition) else strategy
    evidence: dict[str, Any] = {}
    if definition.market_type is MarketType.CRYPTO_SPOT:
        bars = _bars(data)
        if not bars:
            return Signal(definition.family, 0.0, "flat", definition.market_type.value, WARMING_UP, {"history_count": 0})
        lookback = _declared_lookback(definition.parameters, 14)
        if definition.family == "mean_reversion":
            required = max(2, lookback)
        elif definition.family in {"dip", "momentum", "breakout", "volatility", "volume_filter"}:
            required = lookback + 1
        elif definition.family == "trend":
            try:
                fast = max(1, int(definition.parameters.get("fast", min(lookback, 10))))
                slow = max(fast + 1, int(definition.parameters.get("slow", max(lookback, 30))))
            except (TypeError, ValueError, OverflowError):
                fast, slow = min(lookback, 10), max(lookback, 30)
            required = slow
        elif definition.family == "rsi":
            required = max(2, _declared_lookback(definition.parameters, 14)) + 1
        else:
            required = 0
        if required:
            evidence.update({"lookback": lookback, "history_count": len(bars), "required": required})
            if len(bars) < required:
                return Signal(
                    definition.family, 0.0, "flat", definition.market_type.value,
                    INSUFFICIENT_LOOKBACK, evidence,
                )
    else:
        snapshots = _ordered_prediction_snapshots(data)
        if not snapshots:
            return Signal(definition.family, 0.0, "flat", definition.market_type.value, WARMING_UP, {"history_count": 0})
        lookback = _declared_lookback(definition.parameters)
        if definition.family in {"momentum", "mean_reversion"}:
            required = lookback + 1
            evidence.update({"lookback": lookback, "history_count": len(snapshots), "required": required})
            if len(snapshots) < required:
                return Signal(
                    definition.family, 0.0, "flat", definition.market_type.value,
                    INSUFFICIENT_LOOKBACK, evidence,
                )
        current = snapshots[-1]
        model_evaluation = _model_evidence(data, current, len(snapshots) - 1)
        evidence["model"] = model_evaluation.as_record()
        if _prediction_requires_model(definition.family) and not model_evaluation.available:
            return Signal(
                definition.family, 0.0, "flat", definition.market_type.value,
                MODEL_INPUT_MISSING, evidence,
            )
    score = _score_for_definition(definition, data)
    reason = SIGNAL_PRODUCED if abs(score) > 1e-12 else STRATEGY_EVALUATED_DECLINED
    side = "buy" if score > 0 else "sell" if score < 0 else "flat"
    return Signal(definition.family, score, side, definition.market_type.value, reason, evidence)


evaluate_signal_with_reason = evaluate_signal_evaluation
evaluate_signal_evidence = evaluate_signal_evaluation


def evaluate_signal(strategy: StrategyDefinition | Mapping[str, Any] | str, data: Any) -> float:
    """Numeric compatibility API returning a deterministic score in ``[-1, 1]``."""
    return evaluate_signal_evaluation(strategy, data).score


def evaluate_signal_record(strategy: StrategyDefinition | Mapping[str, Any] | str, data: Any) -> Signal:
    return evaluate_signal_evaluation(strategy, data)


def evaluate_crypto_family(family: str, bars: Sequence[OHLCVBar | Mapping[str, Any]], **parameters: Any) -> float:
    return _crypto_signal(family.strip().lower(), bars, parameters)


def evaluate_prediction_family(family: str, snapshots: Sequence[PredictionMarketSnapshot | Mapping[str, Any]], **parameters: Any) -> float:
    return _prediction_signal(family.strip().lower(), snapshots, parameters)


class BuiltinSignalEvaluator:
    """Registry-free evaluator facade, useful for deterministic offline callers."""

    def evaluate(self, strategy: StrategyDefinition | Mapping[str, Any] | str, data: Any) -> float:
        return evaluate_signal(strategy, data)

    def evaluate_record(self, strategy: StrategyDefinition | Mapping[str, Any] | str, data: Any) -> Signal:
        return evaluate_signal_evaluation(strategy, data)

    __call__ = evaluate


SignalEvaluator = BuiltinSignalEvaluator


__all__ = [
    "BuiltinSignalEvaluator", "CONSTANT_BASELINE", "INSUFFICIENT_LOOKBACK",
    "MODEL_INPUT_MISSING", "MODEL_INPUT_PRESENT", "ModelProbabilityEvaluation",
    "SIGNAL_PRODUCED", "STRATEGY_EVALUATED_DECLINED", "Signal", "SignalEvaluator",
    "WARMING_UP", "evaluate_crypto_family", "evaluate_model_document",
    "evaluate_model_document_probability", "evaluate_model_probability_evidence",
    "evaluate_prediction_family", "evaluate_signal", "evaluate_signal_evaluation",
    "evaluate_signal_evidence", "evaluate_signal_record", "evaluate_signal_with_reason",
]
