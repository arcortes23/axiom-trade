"""Versioned, bounded experiment plans for autonomous research.

Hermes supplies research intent and declarative bounds.  This module is the
only contract that converts that intent into executable deterministic strategy
variants; it never accepts Python, callbacks, credentials, or live controls.
"""
from __future__ import annotations
from dataclasses import dataclass
import hashlib
import json
import math
import re
from itertools import islice, product
from types import MappingProxyType
from typing import Any, Mapping

from .domain import MarketType
from .strategy import StrategyDefinition, load_strategy


PLAN_SCHEMA_VERSION = "1"
MAX_PLAN_BYTES = 16_384
MAX_PARAMETER_VALUES = 16
MAX_PLAN_VARIANTS = 64
MAX_FEATURES = 64
MAX_METRICS = 32
MAX_SAMPLES = 100_000
AUTONOMOUS_BUDGET_ID = "autonomous"


class ExperimentPlanError(ValueError):
    """Raised when a declarative plan cannot be safely executed."""

    def __init__(self, reason: str, detail: str) -> None:
        self.reason = str(reason).strip().upper() or "INVALID_EXPERIMENT_PLAN"
        self.code = self.reason
        self.detail = str(detail).strip() or self.reason
        super().__init__(f"{self.reason}: {self.detail}")


_TEMPLATE_FAMILIES = {
    "probability_edge": "probability_mispricing",
    "probability_mispricing": "probability_mispricing",
    "lottery_ticket": "lottery_ticket",
    "tails": "tails",
    "mean_reversion": "mean_reversion",
    "momentum": "momentum",
    "time_decay": "time_decay",
    "consistency": "consistency",
    "cross_asset": "cross_asset",
    "event_frequency": "event_frequency",
    "liquidity": "liquidity",
    "correlation_aware": "correlation_aware",
    "dip": "dip",
    "trend": "trend",
    "breakout": "breakout",
    "rsi": "rsi",
    "volume_filter": "volume_filter",
    "volatility": "volatility",
}

_SUPPORTED_FEATURES = {
    MarketType.PREDICTION: frozenset(
        {
            "timestamp",
            "market_id",
            "question",
            "expiry",
            "resolution_criteria",
            "settlement",
            "yes_bid",
            "yes_ask",
            "yes_mid",
            "no_bid",
            "no_ask",
            "no_mid",
            "model_probability",
            "liquidity",
            "spread",
            "volume",
            "time_to_expiry_seconds",
            "correlation",
            "event_count",
            "event_horizon",
            "expected_event_rate",
        }
    ),
    MarketType.CRYPTO_SPOT: frozenset(
        {"timestamp", "symbol", "open", "high", "low", "close", "volume", "trades", "spread"}
    ),
}

_DEFAULT_FEATURES = {
    MarketType.PREDICTION: ("timestamp", "market_id", "yes_mid", "model_probability", "expiry", "settlement"),
    MarketType.CRYPTO_SPOT: ("timestamp", "open", "high", "low", "close", "volume"),
}

_DEFAULT_PARAMETERS = {
    "probability_mispricing": {"threshold": (0.03, 0.05, 0.08)},
    "lottery_ticket": {"max_probability": (0.05, 0.10), "min_edge": (0.02, 0.05)},
    "tails": {"tail_probability": (0.05, 0.10), "threshold": (0.03, 0.05)},
    "time_decay": {"horizon": (86_400, 259_200), "threshold": (0.03, 0.05)},
    "mean_reversion": {"threshold": (0.03, 0.05)},
    "momentum": {"threshold": (0.03, 0.05)},
}

_ALLOWED_FIELDS = frozenset(
    {
        "schema_version",
        "plan_id",
        "hypothesis_id",
        "market_type",
        "template",
        "strategy_template",
        "strategy_family",
        "family",
        "strategy_document",
        "model_document",
        "allowed_features",
        "features",
        "parameters",
        "parameter_ranges",
        "filters",
        "regime_restrictions",
        "target",
        "target_instrument",
        "market_ids",
        "dataset_selector",
        "dataset_id",
        "dataset_version",
        "dataset_timeframe",
        "dataset_source",
        "dataset_source_type",
        "survivorship_bias",
        "universe",
        "universe_provenance",
        "universe_id",
        "universe_version",
        "universe_snapshot_hash",
        "snapshot_hash",
        "universe_methodology",
        "methodology",
        "train_validation_methodology",
        "time_split",
        "metrics",
        "experiment_family",
        "family_budget",
        "budget",
        "max_variants",
        "min_samples",
        "minimum_samples",
        "min_trades",
        "minimum_trades",
        "paper_only",
    }
)

_FORBIDDEN_KEY_TOKENS = frozenset(
    {
        "python",
        "code",
        "callback",
        "callable",
        "function",
        "lambda",
        "hook",
        "handler",
        "credential",
        "credentials",
        "private",
        "token",
        "tokens",
        "oauth",
        "jwt",
        "password",
        "passwords",
        "secret",
        "secrets",
        "risk",
        "position",
        "wallet",
        "wallets",
        "account",
        "accounts",
        "balance",
        "signature",
        "signing",
        "broker",
        "transaction",
        "live",
        "execute",
        "execution",
        "order",
        "orders",
        "holdout",
        "history",
        "historical",
        "withdraw",
        "withdrawal",
        "withdrawals",
        "authorization",
        "bearer",
    }
)
_FORBIDDEN_EXACT_FIELDS = frozenset(
    {
        "auth",
        "authentication",
        "api_key",
        "private_key",
        "access_token",
        "refresh_token",
        "client_secret",
        "order_id",
        "place_order",
        "submit_order",
        "execute_order",
        "live_execution",
    }
)


def _normal_key(value: Any) -> str:
    text = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", str(value))
    return re.sub(r"[^A-Za-z0-9]+", "_", text).strip("_").lower()


def _is_forbidden_key(key: Any) -> bool:
    normalized = _normal_key(key)
    tokens = frozenset(part for part in normalized.split("_") if part)
    return normalized in _FORBIDDEN_EXACT_FIELDS or bool(tokens & _FORBIDDEN_KEY_TOKENS)


def _reject_forbidden_key(key: Any, path: str) -> None:
    if _is_forbidden_key(key):
        raise ExperimentPlanError("UNSAFE_PLAN_FIELD", f"forbidden plan field {path}.{key}")


def _clean_json(value: Any, *, path: str = "plan", depth: int = 0) -> Any:
    if depth > 8:
        raise ExperimentPlanError("PLAN_TOO_DEEP", f"plan nesting exceeds eight levels at {path}")
    if isinstance(value, Mapping):
        if len(value) > 256:
            raise ExperimentPlanError("PLAN_TOO_LARGE", f"mapping is too large at {path}")
        result: dict[str, Any] = {}
        for key, child in value.items():
            if not isinstance(key, str):
                raise ExperimentPlanError("UNSAFE_PLAN_FIELD", f"plan field names must be strings at {path}")
            key_text = key
            if len(key_text) > 128:
                raise ExperimentPlanError("PLAN_TOO_LARGE", f"field name is too long at {path}")
            _reject_forbidden_key(key_text, path)
            result[key_text] = _clean_json(child, path=f"{path}.{key_text}", depth=depth + 1)
        return result
    if isinstance(value, (list, tuple)):
        if len(value) > 256:
            raise ExperimentPlanError("PLAN_TOO_LARGE", f"collection is too large at {path}")
        return [_clean_json(child, path=f"{path}[]", depth=depth + 1) for child in value]
    if isinstance(value, bool) or value is None or isinstance(value, str):
        if isinstance(value, str) and len(value) > 4_096:
            raise ExperimentPlanError("PLAN_TOO_LARGE", f"string is too long at {path}")
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ExperimentPlanError("INVALID_NUMBER", f"non-finite number at {path}")
        return value
    raise ExperimentPlanError("UNSAFE_PLAN_FIELD", f"unsupported value at {path}: {type(value).__name__}")


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False, default=str)
def _freeze_json(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({str(key): _freeze_json(child) for key, child in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_json(child) for child in value)
    return value


def _plain_json(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _plain_json(child) for key, child in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain_json(child) for child in value]
    return value



def _as_mapping(value: Any, *, name: str) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise ExperimentPlanError("INVALID_PLAN", f"{name} must be an object")
    clean = _clean_json(value, path=name)
    assert isinstance(clean, dict)
    return clean


def _string_list(value: Any, *, name: str, limit: int) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, (list, tuple)) or len(value) > limit:
        raise ExperimentPlanError("INVALID_PLAN", f"{name} must be a bounded list")
    result: list[str] = []
    for item in value:
        if not isinstance(item, str) or not item.strip():
            raise ExperimentPlanError("INVALID_PLAN", f"{name} must contain non-empty strings")
        result.append(item.strip())
    return tuple(dict.fromkeys(result))


def _time_split(value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ExperimentPlanError("INVALID_PLAN", "chronological time_split is required")
    tokens = tuple(token for token in re.split(r"[^a-z]+", value.lower()) if token)
    positions = [tokens.index(token) if token in tokens else -1 for token in ("train", "validation", "holdout")]
    if positions != sorted(positions) or any(position < 0 for position in positions) or any(
        token in tokens for token in ("random", "shuffle", "k", "fold")
    ):
        raise ExperimentPlanError("LOCKED_HOLDOUT_FORBIDDEN", "time_split must be chronological train-validation-holdout")
    return value.strip()


def _parameter_values(value: Any, *, name: str) -> tuple[Any, ...]:
    if isinstance(value, Mapping):
        keys = {str(key) for key in value}
        if keys != {"min", "max", "step"}:
            raise ExperimentPlanError(
                "UNBOUNDED_PARAMETER_RANGE",
                f"{name} ranges must contain exactly min, max, and step",
            )
        try:
            minimum = float(value["min"])
            maximum = float(value["max"])
            step = float(value["step"])
        except (TypeError, ValueError):
            raise ExperimentPlanError("UNBOUNDED_PARAMETER_RANGE", f"{name} range values must be numeric")
        if (
            not all(math.isfinite(number) for number in (minimum, maximum, step))
            or step <= 0
            or maximum < minimum
        ):
            raise ExperimentPlanError("UNBOUNDED_PARAMETER_RANGE", f"{name} range is invalid")
        count = int(math.floor((maximum - minimum) / step + 1e-12)) + 1
        if count < 1 or count > MAX_PARAMETER_VALUES:
            raise ExperimentPlanError("UNBOUNDED_PARAMETER_RANGE", f"{name} range has too many values")
        values = tuple(round(minimum + index * step, 12) for index in range(count))
        if all(isinstance(value[field], int) and not isinstance(value[field], bool) for field in ("min", "max", "step")):
            values = tuple(int(item) for item in values)
        return tuple(dict.fromkeys(values))
    values = value if isinstance(value, (list, tuple)) else (value,)
    if not values or len(values) > MAX_PARAMETER_VALUES:
        raise ExperimentPlanError("UNBOUNDED_PARAMETER_RANGE", f"{name} has too many values")
    cleaned = tuple(_clean_json(item, path=name) for item in values)
    if any(isinstance(item, (list, dict)) for item in cleaned):
        raise ExperimentPlanError("UNBOUNDED_PARAMETER_RANGE", f"{name} values must be scalar")
    return tuple(dict.fromkeys(cleaned))


def _target(value: Any, *, target_instrument: Any, market_ids: Any) -> dict[str, Any]:
    if value is None:
        result: dict[str, Any] = {}
    elif isinstance(value, str):
        result = {"instrument": value.strip()}
    elif isinstance(value, Mapping):
        result = _as_mapping(value, name="target")
    else:
        raise ExperimentPlanError("INVALID_PLAN", "target must be text or an object")
    if target_instrument is not None:
        if not isinstance(target_instrument, str) or not target_instrument.strip():
            raise ExperimentPlanError("INVALID_PLAN", "target_instrument must be non-empty text")
        result["instrument"] = target_instrument.strip()
    if market_ids is not None:
        result["market_ids"] = list(_string_list(market_ids, name="market_ids", limit=1000))
    return result
def _crypto_universe(
    raw: Mapping[str, Any],
    selector: Mapping[str, Any],
    methodology: Mapping[str, Any],
) -> dict[str, Any]:
    """Normalize explicit, immutable universe provenance for crypto plans.

    A plan may only refer to a snapshot that a worker can resolve exactly.
    Resolution against the store is deliberately done by the autonomous
    processor, but aliases are normalized here so the persisted plan has one
    canonical shape.
    """
    candidates = (
        raw.get("universe"),
        raw.get("universe_provenance"),
        methodology.get("universe"),
        methodology.get("universe_provenance"),
        selector.get("universe"),
        selector.get("universe_provenance"),
    )
    source = next((value for value in candidates if value is not None), None)
    if source is None and any(
        raw.get(name) is not None
        for name in ("universe_id", "universe_version", "universe_snapshot_hash", "snapshot_hash")
    ):
        source = {
            "universe_id": raw.get("universe_id"),
            "universe_version": raw.get("universe_version"),
            "snapshot_hash": raw.get("universe_snapshot_hash", raw.get("snapshot_hash")),
            "methodology": raw.get("universe_methodology"),
        }
    if not isinstance(source, Mapping):
        raise ExperimentPlanError(
            "INSUFFICIENT_DATA",
            "crypto_spot plans require versioned universe provenance and methodology",
        )
    value = _as_mapping(source, name="universe")
    universe_id = value.get("universe_id", value.get("id"))
    if not isinstance(universe_id, str) or not universe_id.strip():
        raise ExperimentPlanError("INSUFFICIENT_DATA", "crypto universe_id is required")
    version = value.get("universe_version", value.get("version"))
    if not isinstance(version, str) or not version.strip():
        raise ExperimentPlanError("INSUFFICIENT_DATA", "crypto universe_version is required")
    if version.strip().lower() in {"latest", "current", "default", "unversioned"}:
        raise ExperimentPlanError("INSUFFICIENT_DATA", "crypto universe_version must be immutable and versioned")
    method = value.get("methodology", value.get("method", value.get("selection_method")))
    if not isinstance(method, str) or not method.strip():
        raise ExperimentPlanError("INSUFFICIENT_DATA", "crypto universe methodology is required")
    instruments = value.get("instruments", value.get("symbols", value.get("assets")))
    normalized_instruments = _string_list(instruments, name="universe.instruments", limit=1000)
    if not normalized_instruments:
        raise ExperimentPlanError("INSUFFICIENT_DATA", "crypto universe instruments are required")
    snapshot_hash = value.get(
        "snapshot_hash",
        value.get("universe_snapshot_hash", value.get("content_hash", value.get("universe_hash"))),
    )
    if snapshot_hash is not None:
        if not isinstance(snapshot_hash, str) or not snapshot_hash.strip():
            raise ExperimentPlanError("INSUFFICIENT_DATA", "crypto universe snapshot_hash is required when supplied")
        if snapshot_hash.strip().lower() in {"latest", "current", "default", "unversioned"}:
            raise ExperimentPlanError("INSUFFICIENT_DATA", "crypto universe snapshot_hash must be immutable")
    allowed = {
        "universe_id",
        "id",
        "universe_version",
        "version",
        "methodology",
        "method",
        "selection_method",
        "instruments",
        "symbols",
        "assets",
        "source",
        "source_type",
        "content_hash",
        "snapshot_hash",
        "universe_snapshot_hash",
        "universe_hash",
        "dataset_id",
        "survivorship_bias",
        "survivorship",
    }
    unknown = sorted(set(value) - allowed)
    if unknown:
        raise ExperimentPlanError("UNSAFE_PLAN_FIELD", f"unsupported universe fields: {unknown}")
    result: dict[str, Any] = {
        "universe_id": universe_id.strip(),
        "universe_version": version.strip(),
        "methodology": method.strip(),
        "instruments": list(normalized_instruments),
    }
    if snapshot_hash is not None:
        result["snapshot_hash"] = snapshot_hash.strip()
    for key in ("source", "source_type", "dataset_id", "survivorship_bias", "survivorship"):
        if value.get(key) is not None:
            result[key] = str(value[key]).strip()
    if "survivorship" in result and "survivorship_bias" not in result:
        result["survivorship_bias"] = result.pop("survivorship")
    return result




@dataclass(frozen=True, slots=True)
class ExperimentPlan:
    """Immutable, data-only plan accepted by deterministic Axiom workers."""

    schema_version: str
    plan_id: str
    hypothesis_id: str
    market_type: MarketType
    template: str
    allowed_features: tuple[str, ...]
    parameters: Mapping[str, tuple[Any, ...]]
    filters: Mapping[str, Any]
    regime_restrictions: Mapping[str, Any]
    target: Mapping[str, Any]
    dataset_selector: Mapping[str, Any]
    methodology: Mapping[str, Any]
    metrics: tuple[str, ...]
    experiment_family: str
    family_budget: Mapping[str, Any]
    max_variants: int
    min_samples: int
    min_trades: int
    paper_only: bool
    strategy_document: Mapping[str, Any] | None = None
    model_document: Mapping[str, Any] | None = None
    universe: Mapping[str, Any] | None = None

    @classmethod
    def from_mapping(cls, document: Mapping[str, Any], *, hypothesis_id: str | None = None) -> "ExperimentPlan":
        if isinstance(document, ExperimentPlan):
            return document
        if not isinstance(document, Mapping):
            raise ExperimentPlanError("INVALID_PLAN", "experiment plan must be an object")
        unknown = set(str(key) for key in document) - _ALLOWED_FIELDS
        if unknown:
            forbidden_unknown = sorted(key for key in unknown if _is_forbidden_key(key))
            if forbidden_unknown:
                compact_unknown = " ".join(forbidden_unknown).lower()
                if "holdout" in compact_unknown:
                    raise ExperimentPlanError("LOCKED_HOLDOUT_FORBIDDEN", f"forbidden locked-data fields: {forbidden_unknown}")
                if any(token in compact_unknown for token in ("live", "execute", "execution")):
                    raise ExperimentPlanError("LIVE_EXECUTION_FORBIDDEN", f"forbidden execution fields: {forbidden_unknown}")
                raise ExperimentPlanError("UNSAFE_PLAN_FIELD", f"forbidden plan fields: {forbidden_unknown}")
            raise ExperimentPlanError("INVALID_PLAN", f"unknown plan fields: {sorted(unknown)}")
        raw = _clean_json(document)
        assert isinstance(raw, dict)
        schema_version = str(raw.get("schema_version", PLAN_SCHEMA_VERSION)).strip()
        if schema_version != PLAN_SCHEMA_VERSION:
            raise ExperimentPlanError("UNSUPPORTED_SCHEMA", f"unsupported experiment plan schema {schema_version!r}")

        strategy_document: Mapping[str, Any] | None = None
        raw_strategy = raw.get("strategy_document")
        if raw_strategy is not None:
            if not isinstance(raw_strategy, Mapping):
                raise ExperimentPlanError("UNSUPPORTED_STRATEGY_FAMILY", "strategy_document must be a declarative mapping")
            try:
                strategy_document = load_strategy(raw_strategy).to_dict()
            except Exception as exc:
                raise ExperimentPlanError("UNSUPPORTED_STRATEGY_FAMILY", str(exc)) from exc

        market_value = raw.get("market_type")
        if market_value is None and strategy_document is not None:
            market_value = strategy_document.get("market_type")
        try:
            market_type = market_value if isinstance(market_value, MarketType) else MarketType(str(market_value or "prediction"))
        except ValueError as exc:
            raise ExperimentPlanError("UNSUPPORTED_STRATEGY_FAMILY", f"unsupported market_type {market_value!r}") from exc
        if strategy_document is not None and strategy_document.get("market_type") != market_type.value:
            raise ExperimentPlanError("UNSUPPORTED_STRATEGY_FAMILY", "strategy market_type does not match plan")
        template_value = raw.get("template", raw.get("strategy_template", raw.get("strategy_family", raw.get("family"))))
        if template_value is None and strategy_document is not None:
            template_value = strategy_document.get("family")
        template_key = str(template_value or "probability_mispricing").strip().lower()
        family = _TEMPLATE_FAMILIES.get(template_key)
        if family is None or (market_type is MarketType.PREDICTION and family not in {
            "probability_mispricing", "lottery_ticket", "tails", "mean_reversion", "momentum", "time_decay",
            "consistency", "cross_asset", "event_frequency", "liquidity", "correlation_aware",
        }) or (market_type is MarketType.CRYPTO_SPOT and family not in {
            "dip", "momentum", "trend", "mean_reversion", "breakout", "rsi", "volume_filter", "volatility",
        }):
            raise ExperimentPlanError("UNSUPPORTED_STRATEGY_FAMILY", f"unsupported template {template_key!r} for {market_type.value}")
        if strategy_document is not None and strategy_document.get("family") != family:
            raise ExperimentPlanError("UNSUPPORTED_STRATEGY_FAMILY", "strategy family does not match template")

        features_value = raw.get("allowed_features", raw.get("features"))
        features = _string_list(features_value, name="allowed_features", limit=MAX_FEATURES) if features_value is not None else _DEFAULT_FEATURES[market_type]
        unsupported_features = sorted(set(features) - _SUPPORTED_FEATURES[market_type])
        if unsupported_features:
            raise ExperimentPlanError("UNSUPPORTED_FEATURE", f"unsupported features: {unsupported_features}")

        parameter_source = raw.get("parameters", raw.get("parameter_ranges"))
        if parameter_source is None and strategy_document is not None:
            parameter_source = strategy_document.get("parameters", {})
        if parameter_source is None:
            parameter_source = _DEFAULT_PARAMETERS.get(family, {"threshold": (0.05,)})
        if not isinstance(parameter_source, Mapping):
            raise ExperimentPlanError("UNBOUNDED_PARAMETER_RANGE", "parameters must be an object")
        parameters: dict[str, tuple[Any, ...]] = {}
        for key in sorted(parameter_source, key=str):
            key_text = str(key).strip().lower()
            if not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", key_text):
                raise ExperimentPlanError("UNBOUNDED_PARAMETER_RANGE", f"invalid parameter name {key!r}")
            _reject_forbidden_key(key_text, "parameters")
            parameters[key_text] = _parameter_values(parameter_source[key], name=f"parameters.{key_text}")
        if not parameters:
            parameters = {"threshold": (0.05,)}
        possible_variants = 1
        for values in parameters.values():
            possible_variants *= len(values)
            if possible_variants > MAX_PLAN_VARIANTS * MAX_PARAMETER_VALUES:
                raise ExperimentPlanError("EXPERIMENT_BUDGET_EXCEEDED", "parameter search space is too large")

        filters = _as_mapping(raw.get("filters"), name="filters")
        regime_restrictions = _as_mapping(raw.get("regime_restrictions"), name="regime_restrictions")
        target = _target(raw.get("target"), target_instrument=raw.get("target_instrument"), market_ids=raw.get("market_ids"))
        selector = _as_mapping(raw.get("dataset_selector"), name="dataset_selector")
        if raw.get("dataset_id") is not None:
            selector["dataset_id"] = str(raw["dataset_id"]).strip()
        if raw.get("dataset_version") is not None:
            selector["dataset_version"] = str(raw["dataset_version"]).strip()
        for raw_name, selector_name in (
            ("dataset_timeframe", "timeframe"),
            ("dataset_source", "source"),
            ("dataset_source_type", "source_type"),
            ("survivorship_bias", "survivorship_bias"),
        ):
            if raw.get(raw_name) is not None:
                selector.setdefault(selector_name, str(raw[raw_name]).strip())
        if selector.get("dataset_version") is None and selector.get("version") is not None:
            selector["dataset_version"] = str(selector["version"]).strip()
        if selector.get("source") is None and selector.get("provider") is not None:
            selector["source"] = str(selector["provider"]).strip()
        if selector.get("survivorship_bias") is None and selector.get("survivorship") is not None:
            selector["survivorship_bias"] = str(selector["survivorship"]).strip()
        if not str(selector.get("dataset_version", "")).strip():
            raise ExperimentPlanError("INSUFFICIENT_DATA", "dataset_version is required")
        selector["dataset_version"] = str(selector["dataset_version"]).strip()
        if not selector.get("dataset_id") and target.get("dataset_id"):
            selector["dataset_id"] = str(target["dataset_id"])
        if market_type is MarketType.CRYPTO_SPOT:
            if not selector.get("dataset_id"):
                raise ExperimentPlanError("INSUFFICIENT_DATA", "crypto_spot plans require an explicit dataset_id")
            if selector["dataset_version"].lower() in {"latest", "current", "default", "unversioned"}:
                raise ExperimentPlanError("INSUFFICIENT_DATA", "dataset_version must identify an immutable version")

        methodology = _as_mapping(raw.get("methodology", raw.get("train_validation_methodology")), name="methodology")
        universe: dict[str, Any] | None = None
        if market_type is MarketType.CRYPTO_SPOT:
            universe = _crypto_universe(raw, selector, methodology)
            methodology["universe"] = universe
        methodology["time_split"] = _time_split(raw.get("time_split", methodology.get("time_split", "train-validation-holdout")))
        metrics = _string_list(raw.get("metrics", ("expectancy", "drawdown", "trade_count", "sample_count")), name="metrics", limit=MAX_METRICS)
        if not metrics:
            raise ExperimentPlanError("INVALID_PLAN", "at least one metric is required")

        budget_value = raw.get("family_budget", raw.get("budget"))
        family_budget = _as_mapping(budget_value, name="family_budget")
        # Hermes may describe limits, but the worker owns the namespace.  Do
        # not let a proposal redirect reservations into another budget.
        family_budget["budget_id"] = AUTONOMOUS_BUDGET_ID
        for name, default in (("total_limit", 1000), ("per_family_limit", 250)):
            value = family_budget.get(name, default)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ExperimentPlanError("EXPERIMENT_BUDGET_EXCEEDED", f"{name} must be a non-negative integer")
            family_budget[name] = value
        max_variants = raw.get("max_variants", min(MAX_PLAN_VARIANTS, possible_variants))
        if isinstance(max_variants, bool) or not isinstance(max_variants, int) or not 1 <= max_variants <= MAX_PLAN_VARIANTS:
            raise ExperimentPlanError("EXPERIMENT_BUDGET_EXCEEDED", "max_variants must be between one and 64")
        min_samples = raw.get("min_samples", raw.get("minimum_samples", 30))
        min_trades = raw.get("min_trades", raw.get("minimum_trades", 0))
        for value, name in ((min_samples, "min_samples"), (min_trades, "min_trades")):
            if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= MAX_SAMPLES:
                raise ExperimentPlanError("INVALID_PLAN", f"{name} must be a bounded non-negative integer")
        if raw.get("paper_only") is not True:
            raise ExperimentPlanError("LIVE_EXECUTION_FORBIDDEN", "paper_only must be true")

        model_document: Mapping[str, Any] | None = None
        raw_model = raw.get("model_document")
        if raw_model is not None:
            if not isinstance(raw_model, Mapping):
                raise ExperimentPlanError("UNSUPPORTED_FEATURE", "model_document must be an object")
            model_document = _as_mapping(raw_model, name="model_document")
            unknown_model = set(model_document) - {"probability", "yes_probability", "field"}
            if unknown_model:
                raise ExperimentPlanError("UNSUPPORTED_FEATURE", f"unsupported model fields: {sorted(unknown_model)}")
            if "field" in model_document and (
                not isinstance(model_document["field"], str) or model_document["field"] not in _SUPPORTED_FEATURES[market_type]
            ):
                raise ExperimentPlanError("UNSUPPORTED_FEATURE", "model field is not supported")
            for field_name in ("probability", "yes_probability"):
                if field_name not in model_document:
                    continue
                value = model_document[field_name]
                if (
                    isinstance(value, bool)
                    or not isinstance(value, (int, float))
                    or not math.isfinite(float(value))
                    or not 0.0 <= float(value) <= 1.0
                ):
                    raise ExperimentPlanError(
                        "UNSUPPORTED_FEATURE",
                        f"model {field_name} must be a finite probability",
                    )

        resolved_hypothesis = str(raw.get("hypothesis_id", hypothesis_id or "")).strip()
        if not resolved_hypothesis:
            raise ExperimentPlanError("INVALID_PLAN", "hypothesis_id is required")
        experiment_family = str(raw.get("experiment_family", family)).strip().lower() or family
        if experiment_family != family:
            raise ExperimentPlanError(
                "UNSUPPORTED_STRATEGY_FAMILY",
                f"experiment_family {experiment_family!r} does not match supported family {family!r}",
            )
        normalized: dict[str, Any] = {
            "schema_version": schema_version,
            "hypothesis_id": resolved_hypothesis,
            "market_type": market_type.value,
            "template": template_key,
            "allowed_features": list(features),
            "parameters": {key: list(values) for key, values in sorted(parameters.items())},
            "filters": filters,
            "regime_restrictions": regime_restrictions,
            "target": target,
            "dataset_selector": selector,
            "methodology": methodology,
            "metrics": list(metrics),
            "experiment_family": experiment_family,
            "family_budget": family_budget,
            "max_variants": max_variants,
            "min_samples": min_samples,
            "min_trades": min_trades,
            "paper_only": True,
            "strategy_document": dict(strategy_document) if strategy_document is not None else None,
            "model_document": dict(model_document) if model_document is not None else None,
            "universe": universe,
        }
        if normalized["strategy_document"] is None:
            normalized.pop("strategy_document")
        if normalized["model_document"] is None:
            normalized.pop("model_document")
        if normalized["universe"] is None:
            normalized.pop("universe")
        plan_id = str(raw.get("plan_id", "")).strip()
        if not plan_id:
            plan_id = "plan-" + hashlib.sha256(_canonical(normalized).encode("utf-8")).hexdigest()[:24]
        if not re.fullmatch(r"[A-Za-z0-9._:-]{1,128}", plan_id):
            raise ExperimentPlanError("INVALID_PLAN", "plan_id contains unsupported characters")
        normalized["plan_id"] = plan_id
        if len(_canonical(normalized).encode("utf-8")) > MAX_PLAN_BYTES:
            raise ExperimentPlanError("PLAN_TOO_LARGE", "experiment plan exceeds bounded size")
        return cls(
            schema_version,
            plan_id,
            resolved_hypothesis,
            market_type,
            template_key,
            features,
            MappingProxyType({key: tuple(values) for key, values in sorted(parameters.items())}),
            _freeze_json(filters),
            _freeze_json(regime_restrictions),
            _freeze_json(target),
            _freeze_json(selector),
            _freeze_json(methodology),
            metrics,
            normalized["experiment_family"],
            _freeze_json(family_budget),
            max_variants,
            min_samples,
            min_trades,
            True,
            _freeze_json(strategy_document) if strategy_document is not None else None,
            _freeze_json(model_document) if model_document is not None else None,
            _freeze_json(universe) if universe is not None else None,
        )

    @classmethod
    def from_dict(cls, document: Mapping[str, Any], *, hypothesis_id: str | None = None) -> "ExperimentPlan":
        return cls.from_mapping(document, hypothesis_id=hypothesis_id)

    @classmethod
    def validate(cls, document: Mapping[str, Any], *, hypothesis_id: str | None = None) -> "ExperimentPlan":
        return cls.from_mapping(document, hypothesis_id=hypothesis_id)

    @classmethod
    def from_proposal(cls, proposal: Mapping[str, Any]) -> "ExperimentPlan":
        if not isinstance(proposal, Mapping):
            raise ExperimentPlanError("INVALID_PLAN", "proposal must be an object")
        unsafe_keys = sorted(str(key) for key in proposal if _is_forbidden_key(key))
        if unsafe_keys:
            compact = " ".join(unsafe_keys).lower()
            if "holdout" in compact:
                raise ExperimentPlanError("LOCKED_HOLDOUT_FORBIDDEN", f"forbidden proposal fields: {unsafe_keys}")
            if any(token in compact for token in ("live", "execute", "execution")):
                raise ExperimentPlanError("LIVE_EXECUTION_FORBIDDEN", f"forbidden proposal fields: {unsafe_keys}")
            raise ExperimentPlanError("UNSAFE_PLAN_FIELD", f"forbidden proposal fields: {unsafe_keys}")
        proposal_id = str(proposal.get("proposal_id", proposal.get("hypothesis_id", ""))).strip()
        raw_plan = proposal.get("experiment_plan")
        if raw_plan is None:
            raw_plan = {
                "hypothesis_id": proposal_id,
                "market_type": proposal.get("market_type", "prediction"),
                "template": proposal.get("template", proposal.get("strategy_family", "probability_mispricing")),
                "features": proposal.get("features"),
                "parameters": proposal.get("parameters", proposal.get("parameter_ranges")),
                "filters": proposal.get("filters", {}),
                "target": proposal.get("target", {}),
                "dataset_version": proposal.get("dataset_version"),
                "time_split": proposal.get("time_split", "train-validation-holdout"),
                "metrics": proposal.get("metrics") or ("expectancy", "drawdown", "trade_count", "sample_count"),
                "experiment_family": proposal.get("experiment_family"),
                "max_variants": proposal.get("max_variants", 4),
                "min_samples": proposal.get("min_samples", 30),
                "min_trades": proposal.get("min_trades", 0),
                "paper_only": proposal.get("paper_only") if proposal.get("paper_only") is not None else True,
            }
            if raw_plan.get("experiment_family") is None:
                raw_plan.pop("experiment_family", None)
            if isinstance(proposal.get("strategy_document"), Mapping):
                raw_plan["strategy_document"] = proposal["strategy_document"]
            if isinstance(proposal.get("model_document"), Mapping):
                raw_plan["model_document"] = proposal["model_document"]
        if not isinstance(raw_plan, Mapping):
            raise ExperimentPlanError("INVALID_PLAN", "experiment_plan must be an object")
        plan_document = dict(raw_plan)
        plan_document.setdefault("hypothesis_id", proposal_id)
        if proposal.get("dataset_version") is not None:
            plan_selector = plan_document.get("dataset_selector")
            plan_version = plan_document.get("dataset_version")
            if isinstance(plan_selector, Mapping):
                plan_version = plan_selector.get("dataset_version", plan_version)
            if plan_version is not None and str(plan_version).strip() != str(proposal["dataset_version"]).strip():
                raise ExperimentPlanError("INVALID_PLAN", "proposal and experiment plan dataset versions differ")
            plan_document.setdefault("dataset_version", proposal["dataset_version"])
        plan_document.setdefault("paper_only", True)
        return cls.from_mapping(plan_document, hypothesis_id=proposal_id)

    @property
    def dataset_version(self) -> str:
        return str(self.dataset_selector.get("dataset_version", ""))

    @property
    def dataset_id(self) -> str | None:
        value = self.dataset_selector.get("dataset_id")
        return str(value).strip() if value is not None and str(value).strip() else None

    @staticmethod
    def _selector_text(selector: Mapping[str, Any], *names: str) -> str | None:
        for name in names:
            value = selector.get(name)
            if value is not None and str(value).strip():
                return str(value).strip()
        return None

    @property
    def dataset_timeframe(self) -> str | None:
        return self._selector_text(self.dataset_selector, "timeframe", "interval")

    @property
    def dataset_source(self) -> str | None:
        return self._selector_text(self.dataset_selector, "source", "provider")

    @property
    def dataset_source_type(self) -> str | None:
        return self._selector_text(self.dataset_selector, "source_type")

    @property
    def dataset_survivorship(self) -> str | None:
        return self._selector_text(self.dataset_selector, "survivorship_bias", "survivorship")

    @property
    def universe_id(self) -> str | None:
        if self.universe is None:
            return None
        return self._selector_text(self.universe, "universe_id", "id")

    @property
    def universe_version(self) -> str | None:
        if self.universe is None:
            return None
        return self._selector_text(self.universe, "universe_version", "version")

    @property
    def universe_snapshot_hash(self) -> str | None:
        if self.universe is None:
            return None
        return self._selector_text(
            self.universe,
            "snapshot_hash",
            "universe_snapshot_hash",
            "content_hash",
            "universe_hash",
        )

    @property
    def target_instrument(self) -> str | None:
        value = self.target.get("instrument", self.target.get("symbol"))
        return str(value).strip() if value is not None and str(value).strip() else None

    @property
    def target_markets(self) -> tuple[str, ...]:
        return _string_list(self.target.get("market_ids"), name="target.market_ids", limit=1000)

    @property
    def budget_id(self) -> str:
        return AUTONOMOUS_BUDGET_ID

    def as_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "schema_version": self.schema_version,
            "plan_id": self.plan_id,
            "hypothesis_id": self.hypothesis_id,
            "market_type": self.market_type.value,
            "template": self.template,
            "allowed_features": list(self.allowed_features),
            "parameters": {key: list(values) for key, values in sorted(self.parameters.items())},
            "filters": _plain_json(self.filters),
            "regime_restrictions": _plain_json(self.regime_restrictions),
            "target": _plain_json(self.target),
            "dataset_selector": _plain_json(self.dataset_selector),
            "methodology": _plain_json(self.methodology),
            "metrics": list(self.metrics),
            "experiment_family": self.experiment_family,
            "family_budget": _plain_json(self.family_budget),
            "max_variants": self.max_variants,
            "min_samples": self.min_samples,
            "min_trades": self.min_trades,
            "paper_only": True,
        }
        if self.strategy_document is not None:
            result["strategy_document"] = _plain_json(self.strategy_document)
        if self.model_document is not None:
            result["model_document"] = _plain_json(self.model_document)
        if self.universe is not None:
            result["universe"] = _plain_json(self.universe)
        return result

    to_record = as_dict

    @property
    def plan_hash(self) -> str:
        return "sha256:" + hashlib.sha256(_canonical(self.as_dict()).encode("utf-8")).hexdigest()

    def variants(self) -> tuple[dict[str, Any], ...]:
        keys = tuple(sorted(self.parameters))
        combinations = product(*(self.parameters[key] for key in keys))
        result: list[dict[str, Any]] = []
        for values in islice(combinations, self.max_variants):
            result.append({key: value for key, value in zip(keys, values)})
        return tuple(result)
    def variant_documents(self) -> tuple[dict[str, Any], ...]:
        return tuple(
            {"variant_id": self.variant_id(parameters), "parameters": dict(parameters)}
            for parameters in self.variants()
        )

    generate_variants = variant_documents

    def variant_id(self, parameters: Mapping[str, Any]) -> str:
        token = _canonical({"plan_id": self.plan_id, "parameters": dict(parameters)})
        return "variant-" + hashlib.sha256(token.encode("utf-8")).hexdigest()[:24]

    def strategy_for(self, parameters: Mapping[str, Any], candidate_id: str) -> StrategyDefinition:
        if self.strategy_document is not None:
            document = dict(self.strategy_document)
        else:
            document = {
                "version": 1,
                "market_type": self.market_type.value,
                "family": _TEMPLATE_FAMILIES[self.template],
                "parameters": {},
            }
            if self.market_type is MarketType.PREDICTION:
                document.update(
                    {
                        "probability_model": "plan-model-probability",
                        "resolution_aware": True,
                        "resolution_inputs": ["expiry", "settlement"],
                    }
                )
        document["parameters"] = dict(parameters)
        document["strategy_id"] = str(candidate_id)
        try:
            return load_strategy(document)
        except Exception as exc:
            raise ExperimentPlanError("UNSUPPORTED_STRATEGY_FAMILY", str(exc)) from exc

    def model_for(self) -> Mapping[str, Any] | None:
        if self.market_type is not MarketType.PREDICTION:
            return None
        return dict(self.model_document or {"field": "model_probability"})


__all__ = [
    "AUTONOMOUS_BUDGET_ID",
    "ExperimentPlan",
    "ExperimentPlanError",
    "MAX_PLAN_VARIANTS",
    "PLAN_SCHEMA_VERSION",
]
