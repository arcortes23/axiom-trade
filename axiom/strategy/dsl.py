"""Versioned, data-only strategy definitions and validation.

The strategy DSL is intentionally declarative: a definition can select one of the
built-in evaluators and pass JSON-compatible parameters, but cannot contain Python
code, imports, callbacks, or expressions that are executed by the engine.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import json
import math
from types import MappingProxyType
from typing import Any, Mapping

from axiom.domain import MarketType


class StrategyValidationError(ValueError):
    """Raised when a strategy document does not satisfy the versioned DSL."""


CRYPTO_FAMILIES = frozenset({
    "dip", "momentum", "trend", "mean_reversion", "breakout", "volatility",
    "rsi", "volume_filter",
})
PREDICTION_FAMILIES = frozenset({
    "probability_mispricing", "tails", "mean_reversion", "momentum", "time_decay",
    "consistency", "cross_asset", "event_frequency", "liquidity", "correlation_aware",
})
FAMILIES = CRYPTO_FAMILIES | PREDICTION_FAMILIES
ALLOWED_OPERATIONS = frozenset({
    "and", "or", "not", "gt", "gte", "lt", "lte", "eq", "between", "change",
    "sma", "ema", "rsi", "volume_ratio", "zscore", "probability_edge",
    "time_to_expiry", "liquidity", "correlation", "event_frequency", "constant",
})
_TOP_LEVEL = frozenset({
    "version", "market_type", "family", "parameters", "operations", "strategy_id",
    "name", "probability_model", "resolution_aware", "resolution_inputs", "metadata",
})


@dataclass(frozen=True, slots=True)
class StrategyDefinition:
    """Validated immutable strategy document.

    ``parameters`` and ``operations`` contain only JSON primitives.  The object is
    safe to persist and can be passed to :func:`axiom.strategy.signals.evaluate_signal`.
    """

    version: int
    market_type: MarketType
    family: str
    parameters: Mapping[str, Any] = field(default_factory=dict)
    operations: tuple[Mapping[str, Any], ...] = ()
    strategy_id: str = ""
    name: str = ""
    probability_model: str | None = None
    resolution_aware: bool = False
    resolution_inputs: tuple[str, ...] = ()
    metadata: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, document: Mapping[str, Any]) -> "StrategyDefinition":
        return validate_strategy(document)

    @classmethod
    def from_json(cls, document: str | bytes) -> "StrategyDefinition":
        try:
            value = json.loads(document)
        except (TypeError, json.JSONDecodeError) as exc:
            raise StrategyValidationError(f"strategy JSON is invalid: {exc}") from exc
        if not isinstance(value, Mapping):
            raise StrategyValidationError("strategy JSON must contain an object")
        return cls.from_dict(value)

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "version": self.version,
            "market_type": self.market_type.value,
            "family": self.family,
            "parameters": _json_value(self.parameters),
            "operations": _json_value(list(self.operations)),
        }
        if self.strategy_id:
            result["strategy_id"] = self.strategy_id
        if self.name:
            result["name"] = self.name
        if self.probability_model is not None:
            result["probability_model"] = self.probability_model
        if self.market_type is MarketType.PREDICTION:
            result["resolution_aware"] = self.resolution_aware
            result["resolution_inputs"] = list(self.resolution_inputs)
        if self.metadata:
            result["metadata"] = _json_value(self.metadata)
        return result

    def to_json(self, *, sort_keys: bool = True) -> str:
        return json.dumps(self.to_dict(), sort_keys=sort_keys, separators=(",", ":"))

    @property
    def id(self) -> str:
        return self.strategy_id or f"{self.market_type.value}:{self.family}"


# Deliberately conservative bounds for values which have an unambiguous domain.
_NONNEGATIVE_KEYS = frozenset({
    "lookback", "fast", "slow", "window", "period", "max_positions", "min_volume",
    "min_liquidity", "max_spread", "max_loss", "drawdown", "decay", "horizon",
})
_PROBABILITY_KEYS = frozenset({
    "threshold", "entry_threshold", "exit_threshold", "min_probability", "max_probability",
    "probability", "confidence", "quantile", "tail_probability", "edge_threshold", "min_edge",
})


def _json_value(value: Any) -> Any:
    if value is None or isinstance(value, (str, bool, int, float)):
        if isinstance(value, float) and not math.isfinite(value):
            raise StrategyValidationError("strategy values must be finite JSON numbers")
        return value
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    raise StrategyValidationError(f"strategy value {type(value).__name__} is not JSON-compatible")


def _freeze_json(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({str(key): _freeze_json(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_json(item) for item in value)
    return value


def _validate_parameters(value: Any, path: str = "parameters") -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise StrategyValidationError(f"{path} must be an object")
    result = _json_value(value)
    assert isinstance(result, dict)
    for key, item in result.items():
        if isinstance(item, bool):
            continue
        if key in _NONNEGATIVE_KEYS and isinstance(item, (int, float)) and item < 0:
            raise StrategyValidationError(f"{path}.{key} must be non-negative")
        if key in {"lookback", "fast", "slow", "window", "period", "horizon"}:
            if not isinstance(item, int) or isinstance(item, bool) or item < 1:
                raise StrategyValidationError(f"{path}.{key} must be a positive integer")
        if key in _PROBABILITY_KEYS and isinstance(item, (int, float)) and not 0 <= item <= 1:
            raise StrategyValidationError(f"{path}.{key} must be between 0 and 1")
        if key in {"multiplier", "zscore", "stddev", "sigma", "fee_bps", "slippage_bps"}:
            if isinstance(item, (int, float)) and (not math.isfinite(float(item)) or item < 0):
                raise StrategyValidationError(f"{path}.{key} must be finite and non-negative")
    return result


def _validate_operations(value: Any) -> tuple[Mapping[str, Any], ...]:
    if value is None:
        return ()
    if not isinstance(value, (list, tuple)):
        raise StrategyValidationError("operations must be an array")
    operations: list[Mapping[str, Any]] = []
    for index, operation in enumerate(value):
        if not isinstance(operation, Mapping):
            raise StrategyValidationError(f"operations[{index}] must be an object")
        unknown = set(operation) - {"op", "args", "value", "threshold", "window", "field"}
        if unknown:
            raise StrategyValidationError(f"operations[{index}] has unknown fields: {sorted(unknown)}")
        op = operation.get("op")
        if not isinstance(op, str) or op not in ALLOWED_OPERATIONS:
            raise StrategyValidationError(f"operations[{index}] uses unknown operation {op!r}")
        _json_value(operation)
        operations.append(dict(operation))
    return tuple(operations)


def validate_strategy(document: Mapping[str, Any] | StrategyDefinition) -> StrategyDefinition:
    """Validate a JSON-compatible strategy mapping and return an immutable definition."""
    if isinstance(document, StrategyDefinition):
        return document
    if not isinstance(document, Mapping):
        raise StrategyValidationError("strategy must be an object")
    unknown = set(document) - _TOP_LEVEL
    if unknown:
        raise StrategyValidationError(f"strategy has unknown fields: {sorted(unknown)}")
    version = document.get("version")
    if not isinstance(version, int) or isinstance(version, bool) or version != 1:
        raise StrategyValidationError("strategy version must be the supported integer 1")
    market_value = document.get("market_type")
    if market_value is None:
        raise StrategyValidationError("market_type is required")
    try:
        market_type = market_value if isinstance(market_value, MarketType) else MarketType(str(market_value))
    except ValueError as exc:
        raise StrategyValidationError(f"unsupported market_type: {market_value!r}") from exc
    family = document.get("family")
    if not isinstance(family, str) or not family:
        raise StrategyValidationError("family is required")
    family = family.strip().lower()
    valid_families = CRYPTO_FAMILIES if market_type is MarketType.CRYPTO_SPOT else PREDICTION_FAMILIES
    if family not in valid_families:
        raise StrategyValidationError(f"family {family!r} is not valid for {market_type.value}")
    parameters = _validate_parameters(document.get("parameters", {}))
    operations = _validate_operations(document.get("operations"))
    for field_name in ("strategy_id", "name"):
        if field_name in document and not isinstance(document[field_name], str):
            raise StrategyValidationError(f"{field_name} must be a string")
    probability_model = document.get("probability_model")
    resolution_aware = document.get("resolution_aware", False)
    resolution_inputs_value = document.get("resolution_inputs", ())
    if market_type is MarketType.PREDICTION:
        if not isinstance(probability_model, str) or not probability_model.strip():
            raise StrategyValidationError("prediction strategies require probability_model")
        if resolution_aware is not True:
            raise StrategyValidationError("prediction strategies require resolution_aware=true")
        if not isinstance(resolution_inputs_value, (list, tuple)):
            raise StrategyValidationError("resolution_inputs must be an array")
        if not resolution_inputs_value:
            raise StrategyValidationError("prediction strategies require resolution-aware inputs")
        if any(not isinstance(item, str) or not item.strip() for item in resolution_inputs_value):
            raise StrategyValidationError("resolution_inputs must contain non-empty strings")
    elif any(field in document for field in ("probability_model", "resolution_aware", "resolution_inputs")):
        raise StrategyValidationError("probability and resolution fields are only valid for prediction strategies")
    metadata = document.get("metadata", {})
    if not isinstance(metadata, Mapping):
        raise StrategyValidationError("metadata must be an object")
    return StrategyDefinition(
        version=version,
        market_type=market_type,
        family=family,
        parameters=_freeze_json(parameters),
        operations=_freeze_json(operations),
        strategy_id=str(document.get("strategy_id", "")),
        name=str(document.get("name", "")),
        probability_model=probability_model.strip() if isinstance(probability_model, str) else None,
        resolution_aware=bool(resolution_aware),
        resolution_inputs=tuple(resolution_inputs_value) if isinstance(resolution_inputs_value, (list, tuple)) else (),
        metadata=_freeze_json(dict(_json_value(metadata))),
    )


class StrategyDSL:
    """Small facade for callers that prefer an object-oriented parser."""

    version = 1

    @staticmethod
    def parse(document: str | bytes | Mapping[str, Any]) -> StrategyDefinition:
        if isinstance(document, Mapping):
            return validate_strategy(document)
        return StrategyDefinition.from_json(document)

    @staticmethod
    def validate(document: str | bytes | Mapping[str, Any]) -> bool:
        StrategyDSL.parse(document)
        return True


def load_strategy(document: str | bytes | Mapping[str, Any]) -> StrategyDefinition:
    return StrategyDSL.parse(document)
parse_strategy = load_strategy
StrategySpec = StrategyDefinition
StrategySchemaError = StrategyValidationError


__all__ = [
    "ALLOWED_OPERATIONS", "CRYPTO_FAMILIES", "FAMILIES", "PREDICTION_FAMILIES",
    "StrategyDefinition", "StrategyDSL", "StrategySchemaError", "StrategySpec",
    "StrategyValidationError", "load_strategy", "parse_strategy", "validate_strategy",
]


