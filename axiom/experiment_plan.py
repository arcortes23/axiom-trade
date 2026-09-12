"""Versioned, bounded experiment plans for autonomous research.

Hermes supplies research intent and declarative bounds.  This module is the
only contract that converts that intent into executable deterministic strategy
variants; it never accepts Python, callbacks, credentials, or live controls.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum
import hashlib
import json
import math
import re
from itertools import islice, product
from types import MappingProxyType
from typing import Any, Mapping, Sequence

from .domain import MarketType, parse_timestamp
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
MARKET_SCOPE_SCHEMA_VERSION = "1"
MARKET_SCOPE_VERSION = MARKET_SCOPE_SCHEMA_VERSION


class MarketScopeMode(str, Enum):
    """The only supported market-scope policies."""

    RESEARCH_ONLY = "RESEARCH_ONLY"
    EXACT_MARKETS = "EXACT_MARKETS"
    RULE_BASED_MARKETS = "RULE_BASED_MARKETS"


_MARKET_SCOPE_FIELDS = frozenset(
    {
        "schema_version",
        "version",
        "mode",
        "instrument",
        "instrument_constraint",
        "target_instrument",
        "category",
        "categories",
        "category_constraints",
        "market_ids",
        "exact_market_ids",
        "markets",
        "filters",
        "regime_restrictions",
        "provenance",
        "source",
        "policy_hash",
        "scope_hash",
        "hash",
    }
)
_MARKET_SCOPE_PROVENANCE = frozenset({"canonical", "legacy-derived"})


def _scope_values(value: Any, *, name: str, limit: int, casefold: bool = False) -> tuple[str, ...]:
    if value is None:
        return ()
    values = (value,) if isinstance(value, str) else value
    if not isinstance(values, (list, tuple, set, frozenset)) or len(values) > limit:
        raise ExperimentPlanError("MALFORMED_MARKET_SCOPE", f"{name} must be a bounded list")
    result: list[str] = []
    for item in values:
        if not isinstance(item, str) or not item.strip():
            raise ExperimentPlanError("MALFORMED_MARKET_SCOPE", f"{name} must contain non-empty strings")
        text = item.strip().casefold() if casefold else item.strip()
        if text not in result:
            result.append(text)
    if isinstance(values, (set, frozenset)):
        result.sort()
    return tuple(result)


def _scope_alias(source: Mapping[str, Any], names: Sequence[str], *, name: str) -> Any:
    values = [(key, source[key]) for key in names if key in source]
    if not values:
        return None
    first = values[0][1]
    for key, value in values[1:]:
        if _canonical(value) != _canonical(first):
            raise ExperimentPlanError("CONFLICTING_MARKET_SCOPE", f"conflicting {name} values: {names[0]} and {key}")
    return first


def _scope_restrictions(value: Any) -> Mapping[str, Any]:
    if value is None:
        return MappingProxyType({})
    if not isinstance(value, Mapping):
        raise ExperimentPlanError("MALFORMED_MARKET_SCOPE", "market_scope.regime_restrictions must be an object")
    allowed = {"regime", "regimes", "allowed_regimes", "allowed_states"}
    unknown = sorted(set(str(key) for key in value) - allowed)
    if unknown:
        raise ExperimentPlanError("UNSUPPORTED_MARKET_SCOPE", f"unsupported market scope regime fields: {unknown}")
    supplied = _scope_alias(value, tuple(allowed), name="regime restrictions")
    if supplied is None:
        return MappingProxyType({})
    regimes = _scope_values(supplied, name="market_scope.regime_restrictions", limit=64, casefold=False)
    if not regimes:
        raise ExperimentPlanError("MALFORMED_MARKET_SCOPE", "market_scope.regime_restrictions must not be empty")
    return MappingProxyType({"regimes": tuple(regimes)})


def _scope_filter_values(value: Any, restrictions: Mapping[str, Any]) -> tuple[Mapping[str, Any], Mapping[str, Any]]:
    if value is None:
        source: Mapping[str, Any] = {}
    elif isinstance(value, Mapping):
        source = dict(value)
        if isinstance(source.get("category"), (set, frozenset)):
            source["category"] = sorted(source["category"])
    else:
        raise ExperimentPlanError("MALFORMED_MARKET_SCOPE", "market_scope.filters must be an object")
    try:
        normalized = normalize_forward_filters(source, restrictions)
    except ExperimentPlanError as exc:
        reason = "UNSUPPORTED_MARKET_SCOPE" if "unsupported" in exc.detail.lower() else "MALFORMED_MARKET_SCOPE"
        raise ExperimentPlanError(reason, exc.detail) from exc
    filters = dict(normalized)
    filter_category = filters.get("category")
    if filter_category is not None:
        categories = _scope_values(filter_category, name="market_scope.filters.category", limit=64, casefold=True)
        filters["category"] = categories[0] if isinstance(filter_category, str) else list(categories)
    regimes = filters.pop("regimes", None)
    normalized_restrictions = dict(restrictions)
    if regimes is not None:
        normalized_restrictions = {"regimes": tuple(sorted(str(item) for item in regimes))}
    return _freeze_json(filters), _freeze_json(normalized_restrictions)
def _scope_material(policy: "MarketScopePolicy") -> dict[str, Any]:
    material = policy.as_dict()
    material.pop("policy_hash", None)
    material.pop("scope_hash", None)
    material.pop("hash", None)
    material.pop("provenance", None)
    material["categories"] = sorted(material.get("categories", ()))
    material["market_ids"] = sorted(material.get("market_ids", ()))
    filters = dict(material.get("filters") or {})
    for key in ("category", "regimes"):
        value = filters.get(key)
        if isinstance(value, list):
            filters[key] = sorted(value)
    material["filters"] = filters
    restrictions = dict(material.get("regime_restrictions") or {})
    if isinstance(restrictions.get("regimes"), list):
        restrictions["regimes"] = sorted(restrictions["regimes"])
    material["regime_restrictions"] = restrictions
    return material


def _scope_equivalent(left: "MarketScopePolicy", right: "MarketScopePolicy") -> bool:
    """Compare authorities while ignoring provenance added by compatibility aliases."""
    left_material = _scope_material(left)
    right_material = _scope_material(right)
    left_material.pop("provenance", None)
    right_material.pop("provenance", None)
    return _canonical(left_material) == _canonical(right_material)
@dataclass(frozen=True, slots=True)
class MarketScopePolicy:
    """Immutable authority describing which prediction markets may be researched."""

    schema_version: str
    mode: str
    instrument: str | None = None
    categories: tuple[str, ...] = ()
    market_ids: tuple[str, ...] = ()
    filters: Mapping[str, Any] = field(default_factory=lambda: MappingProxyType({}))
    regime_restrictions: Mapping[str, Any] = field(
        default_factory=lambda: MappingProxyType({})
    )
    provenance: str = "canonical"

    def __post_init__(self) -> None:
        version = str(self.schema_version).strip()
        if version != MARKET_SCOPE_SCHEMA_VERSION:
            raise ValueError(f"unsupported market scope schema {version!r}")
        mode = self.mode.value if isinstance(self.mode, MarketScopeMode) else str(self.mode).strip().upper()
        if mode not in {item.value for item in MarketScopeMode}:
            raise ValueError(f"unsupported market scope mode {mode!r}")
        instrument = self.instrument
        if instrument is not None:
            if not isinstance(instrument, str) or not instrument.strip():
                raise ValueError("market scope instrument must be non-empty text")
            instrument = instrument.strip()
        categories = tuple(str(item).strip().casefold() for item in self.categories if str(item).strip())
        market_ids = tuple(str(item).strip() for item in self.market_ids if str(item).strip())
        if len(set(categories)) != len(categories) or len(set(market_ids)) != len(market_ids):
            raise ValueError("market scope values must not contain duplicates")
        filters = _freeze_json(dict(self.filters) if isinstance(self.filters, Mapping) else self.filters)
        restrictions = _freeze_json(
            dict(self.regime_restrictions) if isinstance(self.regime_restrictions, Mapping) else self.regime_restrictions
        )
        provenance = str(self.provenance).strip().lower()
        if provenance not in _MARKET_SCOPE_PROVENANCE:
            raise ValueError(f"unsupported market scope provenance {provenance!r}")
        if mode == MarketScopeMode.RESEARCH_ONLY.value and (instrument or categories or market_ids or filters or restrictions):
            raise ValueError("RESEARCH_ONLY market scope cannot contain market ids or rules")
        if mode == MarketScopeMode.EXACT_MARKETS.value and not market_ids:
            raise ValueError("EXACT_MARKETS market scope requires market ids")
        if mode == MarketScopeMode.RULE_BASED_MARKETS.value and market_ids:
            raise ValueError("RULE_BASED_MARKETS market scope cannot contain exact market ids")
        if mode == MarketScopeMode.RULE_BASED_MARKETS.value and not (instrument or categories or filters or restrictions):
            raise ValueError("RULE_BASED_MARKETS market scope requires at least one rule")
        object.__setattr__(self, "schema_version", version)
        object.__setattr__(self, "mode", mode)
        object.__setattr__(self, "instrument", instrument)
        object.__setattr__(self, "categories", categories)
        object.__setattr__(self, "market_ids", market_ids)
        object.__setattr__(self, "filters", filters)
        object.__setattr__(self, "regime_restrictions", restrictions)
        object.__setattr__(self, "provenance", provenance)

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "MarketScopePolicy":
        if not isinstance(value, Mapping):
            raise ExperimentPlanError("MALFORMED_MARKET_SCOPE", "market_scope must be an object")
        unknown = sorted(set(str(key) for key in value) - _MARKET_SCOPE_FIELDS)
        if unknown:
            raise ExperimentPlanError("UNSUPPORTED_MARKET_SCOPE", f"unsupported market scope fields: {unknown}")
        try:
            clean = _clean_json(value, path="market_scope")
        except ExperimentPlanError as exc:
            reason = "UNSUPPORTED_MARKET_SCOPE" if exc.reason == "UNSAFE_PLAN_FIELD" else "MALFORMED_MARKET_SCOPE"
            raise ExperimentPlanError(reason, exc.detail) from exc
        schema_version = _scope_alias(clean, ("schema_version", "version"), name="schema version")
        schema_version = MARKET_SCOPE_SCHEMA_VERSION if schema_version is None else str(schema_version).strip()
        if schema_version != MARKET_SCOPE_SCHEMA_VERSION:
            raise ExperimentPlanError("UNSUPPORTED_MARKET_SCOPE", f"unsupported market scope schema {schema_version!r}")
        mode_value = clean.get("mode")
        if not isinstance(mode_value, str) or not mode_value.strip():
            raise ExperimentPlanError("MALFORMED_MARKET_SCOPE", "market_scope.mode is required")
        mode = mode_value.strip().upper()
        if mode not in {item.value for item in MarketScopeMode}:
            raise ExperimentPlanError("UNSUPPORTED_MARKET_SCOPE", f"unsupported market scope mode {mode_value!r}")
        provenance = str(clean.get("provenance", "canonical")).strip().lower()
        if provenance not in _MARKET_SCOPE_PROVENANCE:
            raise ExperimentPlanError("MALFORMED_MARKET_SCOPE", "market_scope.provenance is invalid")
        instrument_value = _scope_alias(
            clean,
            ("instrument", "instrument_constraint", "target_instrument"),
            name="instrument",
        )
        if instrument_value is not None and (not isinstance(instrument_value, str) or not instrument_value.strip()):
            raise ExperimentPlanError("MALFORMED_MARKET_SCOPE", "market_scope.instrument must be non-empty text")
        categories_value = _scope_alias(
            clean,
            ("categories", "category", "category_constraints"),
            name="categories",
        )
        categories = _scope_values(categories_value, name="market_scope.categories", limit=64, casefold=True)
        ids_value = _scope_alias(clean, ("market_ids", "exact_market_ids", "markets"), name="market ids")
        market_ids = _scope_values(ids_value, name="market_scope.market_ids", limit=1000)
        restrictions = _scope_restrictions(clean.get("regime_restrictions"))
        filters, restrictions = _scope_filter_values(clean.get("filters"), restrictions)
        filter_category = filters.get("category")
        if filter_category is not None:
            filter_categories = _scope_values(filter_category, name="market_scope.filters.category", limit=64, casefold=True)
            if categories and set(categories) != set(filter_categories):
                raise ExperimentPlanError("CONFLICTING_MARKET_SCOPE", "market scope category constraints differ")
            categories = categories or filter_categories
        if categories and "category" not in filters:
            filters = _freeze_json({**dict(filters), "category": list(categories)})
        if mode == MarketScopeMode.RESEARCH_ONLY.value and (instrument_value or categories or market_ids or filters or restrictions):
            raise ExperimentPlanError("MALFORMED_MARKET_SCOPE", "RESEARCH_ONLY market scope cannot contain market ids or rules")
        if mode == MarketScopeMode.EXACT_MARKETS.value and not market_ids:
            raise ExperimentPlanError("MALFORMED_MARKET_SCOPE", "EXACT_MARKETS market scope requires market ids")
        if mode == MarketScopeMode.RULE_BASED_MARKETS.value and market_ids:
            raise ExperimentPlanError("CONFLICTING_MARKET_SCOPE", "RULE_BASED_MARKETS market scope conflicts with exact market ids")
        if mode == MarketScopeMode.RULE_BASED_MARKETS.value and not (instrument_value or categories or filters or restrictions):
            raise ExperimentPlanError("MALFORMED_MARKET_SCOPE", "RULE_BASED_MARKETS market scope requires at least one rule")
        supplied_hash = _scope_alias(clean, ("policy_hash", "scope_hash", "hash"), name="policy hash")
        policy = cls(
            schema_version,
            mode,
            instrument_value.strip() if isinstance(instrument_value, str) else None,
            categories,
            market_ids,
            filters,
            restrictions,
            provenance,
        )
        if supplied_hash is not None and str(supplied_hash).strip() != policy.policy_hash:
            raise ExperimentPlanError("CONFLICTING_MARKET_SCOPE", "market scope policy hash does not match canonical material")
        return policy


    @property
    def version(self) -> str:
        return self.schema_version

    @property
    def scope_version(self) -> str:
        return self.schema_version

    @property
    def policy_hash(self) -> str:
        return "sha256:" + hashlib.sha256(_canonical(_scope_material(self)).encode("utf-8")).hexdigest()

    @property
    def scope_hash(self) -> str:
        return self.policy_hash

    @property
    def hash(self) -> str:
        return self.policy_hash

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "mode": self.mode,
            "instrument": self.instrument,
            "categories": list(self.categories),
            "market_ids": list(self.market_ids),
            "filters": _plain_json(self.filters),
            "regime_restrictions": _plain_json(self.regime_restrictions),
            "provenance": self.provenance,
        }

    to_dict = as_dict
    as_record = as_dict


def normalize_market_scope(
    value: Mapping[str, Any] | None = None,
    *,
    target: Any = None,
    market_ids: Any = None,
    target_market_ids: Any = None,
    target_instrument: Any = None,
    instrument: Any = None,
    categories: Any = None,
    filters: Any = None,
    regime_restrictions: Any = None,
) -> MarketScopePolicy:
    """Normalize one explicit policy or legacy target/filter sources."""
    if value is not None:
        explicit = MarketScopePolicy.from_mapping(value)

        def has_material(item: Any) -> bool:
            if item is None:
                return False
            if isinstance(item, Mapping) and not item:
                return False
            if isinstance(item, (list, tuple, set, frozenset)) and not item:
                return False
            return True

        legacy_present = any(
            has_material(item)
            for item in (
                target,
                market_ids,
                target_market_ids,
                target_instrument,
                instrument,
                categories,
                filters,
                regime_restrictions,
            )
        )
        if not legacy_present:
            return explicit
        legacy = normalize_market_scope(
            None,
            target=target,
            market_ids=market_ids,
            target_market_ids=target_market_ids,
            target_instrument=target_instrument,
            instrument=instrument,
            categories=categories,
            filters=filters,
            regime_restrictions=regime_restrictions,
        )
        # Legacy aliases are partial constraints, not a second complete
        # policy.  Compare only dimensions supplied by those aliases so a
        # richer explicit scope (for example, target instrument plus nested
        # liquidity filters) remains valid while contradictions still fail.
        explicit_filters = dict(explicit.filters)
        legacy_filters = dict(legacy.filters)
        if explicit.mode == MarketScopeMode.RESEARCH_ONLY.value and (
            legacy.instrument
            or legacy.categories
            or legacy.market_ids
            or legacy_filters
            or legacy.regime_restrictions
        ):
            raise ExperimentPlanError("CONFLICTING_MARKET_SCOPE", "explicit market_scope conflicts with legacy target/filter sources")
        if legacy.market_ids and (
            explicit.mode != MarketScopeMode.EXACT_MARKETS.value
            or not explicit.market_ids
            or set(explicit.market_ids) != set(legacy.market_ids)
        ):
            raise ExperimentPlanError("CONFLICTING_MARKET_SCOPE", "explicit market_scope conflicts with legacy target/filter sources")
        if legacy.instrument and (
            explicit.instrument is None or explicit.instrument != legacy.instrument
        ):
            raise ExperimentPlanError("CONFLICTING_MARKET_SCOPE", "explicit market_scope conflicts with legacy target/filter sources")
        if legacy.categories:
            explicit_categories = explicit.categories
            if not explicit_categories:
                explicit_category = explicit_filters.get("category")
                explicit_categories = _scope_values(
                    explicit_category,
                    name="market_scope.filters.category",
                    limit=64,
                    casefold=True,
                )
            if not explicit_categories or set(explicit_categories) != set(legacy.categories):
                raise ExperimentPlanError("CONFLICTING_MARKET_SCOPE", "explicit market_scope conflicts with legacy target/filter sources")
        for key, value in legacy_filters.items():
            if key == "category":
                explicit_category = explicit_filters.get("category")
                if explicit_category is None:
                    explicit_category = explicit.categories
                if set(_scope_values(explicit_category, name="market_scope.filters.category", limit=64, casefold=True)) != set(
                    _scope_values(value, name="filters.category", limit=64, casefold=True)
                ):
                    raise ExperimentPlanError("CONFLICTING_MARKET_SCOPE", "explicit market_scope conflicts with legacy target/filter sources")
            elif key not in explicit_filters or _canonical(explicit_filters[key]) != _canonical(value):
                raise ExperimentPlanError("CONFLICTING_MARKET_SCOPE", "explicit market_scope conflicts with legacy target/filter sources")
        if legacy.regime_restrictions and (
            not explicit.regime_restrictions
            or _canonical(dict(explicit.regime_restrictions)) != _canonical(dict(legacy.regime_restrictions))
        ):
            raise ExperimentPlanError("CONFLICTING_MARKET_SCOPE", "explicit market_scope conflicts with legacy target/filter sources")
        return explicit

    target_instrument_value = instrument if instrument is not None else target_instrument
    target_ids = market_ids
    target_filter: Any = filters
    target_restrictions: Any = regime_restrictions
    target_categories = categories
    if target is not None:
        if isinstance(target, str):
            if target_instrument_value is not None and str(target_instrument_value).strip() != target.strip():
                raise ExperimentPlanError("CONFLICTING_MARKET_SCOPE", "target and target_instrument differ")
            target_instrument_value = target
        elif isinstance(target, Mapping):
            unknown_target = sorted(
                set(str(key) for key in target)
                - {"instrument", "symbol", "market_ids", "exact_market_ids", "markets", "category", "categories", "dataset_id"}
            )
            if unknown_target:
                raise ExperimentPlanError("UNSUPPORTED_MARKET_SCOPE", f"unsupported legacy target fields: {unknown_target}")
            target_instrument_value = _scope_alias(target, ("instrument", "symbol"), name="target instrument") or target_instrument_value
            target_ids = _scope_alias(target, ("market_ids", "exact_market_ids", "markets"), name="target market ids") or target_ids
            target_categories = _scope_alias(target, ("categories", "category"), name="target categories") or target_categories
        else:
            raise ExperimentPlanError("MALFORMED_MARKET_SCOPE", "legacy target must be text or an object")
    if target_market_ids is not None:
        if target_ids is not None and _canonical(target_ids) != _canonical(target_market_ids):
            raise ExperimentPlanError("CONFLICTING_MARKET_SCOPE", "market_ids and target_market_ids differ")
        target_ids = target_market_ids
    if target_categories is not None:
        if target_filter is None:
            target_filter = {"category": target_categories}
        elif isinstance(target_filter, Mapping) and "category" not in target_filter:
            target_filter = {**target_filter, "category": target_categories}
    supplied_any = any(
        item is not None
        for item in (target, market_ids, target_market_ids, target_instrument, instrument, categories, filters, regime_restrictions)
    )
    normalized_ids = _scope_values(target_ids, name="market_ids", limit=1000)
    normalized_categories = _scope_values(target_categories, name="categories", limit=64, casefold=True)
    normalized_restrictions = _scope_restrictions(target_restrictions)
    normalized_filters, normalized_restrictions = _scope_filter_values(target_filter, normalized_restrictions)
    filter_category = normalized_filters.get("category")
    if filter_category is not None:
        filter_categories = _scope_values(filter_category, name="filters.category", limit=64, casefold=True)
        if normalized_categories and set(normalized_categories) != set(filter_categories):
            raise ExperimentPlanError("CONFLICTING_MARKET_SCOPE", "legacy category constraints differ")
        normalized_categories = normalized_categories or filter_categories
    if normalized_categories and "category" not in normalized_filters:
        normalized_filters = _freeze_json({**dict(normalized_filters), "category": list(normalized_categories)})
    instrument_text = None
    if target_instrument_value is not None:
        if not isinstance(target_instrument_value, str) or not target_instrument_value.strip():
            raise ExperimentPlanError("MALFORMED_MARKET_SCOPE", "target_instrument must be non-empty text")
        instrument_text = target_instrument_value.strip()
    if normalized_ids:
        if not normalized_filters and normalized_restrictions:
            raise ExperimentPlanError("CONFLICTING_MARKET_SCOPE", "exact market ids conflict with regime restrictions")
        mode = MarketScopeMode.EXACT_MARKETS.value
    elif instrument_text or normalized_categories or normalized_filters or normalized_restrictions:
        mode = MarketScopeMode.RULE_BASED_MARKETS.value
    else:
        mode = MarketScopeMode.RESEARCH_ONLY.value
    return MarketScopePolicy(
        MARKET_SCOPE_SCHEMA_VERSION,
        mode,
        instrument_text,
        normalized_categories,
        normalized_ids,
        normalized_filters,
        normalized_restrictions,
        "legacy-derived" if supplied_any else "canonical",
    )


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
        "target_market_ids",
        "instrument",
        "categories",
        "market_scope",
        "market_scope_hash",
        "market_scope_version",
        "dataset_selector",
        "dataset_id",
        "dataset_version",
        "dataset_timeframe",
        "dataset_source",
        "dataset_source_type",
        "dataset_constituent_ids",
        "constituent_market_ids",
        "constituents",
        "market_versions",
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
        "trial_budget",
        "max_variants",
        "min_samples",
        "minimum_samples",
        "min_trades",
        "minimum_trades",
        "paper_only",
        "research_mode",
        "assumptions",
        "exit_policy",
        "exit",
        "campaign_id",
        "campaign_trial_id",
        "campaign_configuration_id",
        "campaign_protocol",
        "scientific_rationale",
        "dataset_boundary",
        "configuration_manifest",
        "observation_horizon",
        "qualification_gates",
        "validation_policy",
        "final_assessment_policy",
        "protected_row_identity_manifests",
        "selection_excluded",
})

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

def _normalize_dataset_selector(value: Any) -> dict[str, Any]:
    selector = _as_mapping(value, name="dataset_selector")
    for key in ("dataset_id", "dataset_version", "timeframe", "interval", "source", "provider", "source_type"):
        if key in selector and selector[key] is not None:
            if not isinstance(selector[key], str) or not selector[key].strip():
                raise ExperimentPlanError("MALFORMED_DATASET_SELECTOR", f"dataset_selector.{key} must be non-empty text")
            selector[key] = selector[key].strip()
    if "source_type" in selector:
        selector["source_type"] = selector["source_type"].upper()
    for key in ("constituent_market_ids", "constituent_ids"):
        if key in selector:
            selector[key] = list(_scope_values(selector[key], name=f"dataset_selector.{key}", limit=1000))
    if "constituents" in selector and isinstance(selector["constituents"], (list, tuple, set, frozenset)):
        values = selector["constituents"]
        if all(isinstance(item, str) for item in values):
            selector["constituents"] = list(
                _scope_values(values, name="dataset_selector.constituents", limit=1000)
            )
    return selector


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
def _research_mode(value: Any, market_type: MarketType) -> str:
    if market_type is not MarketType.PREDICTION:
        return "NOT_APPLICABLE"
    normalized = str(value or "PRICE_PROXY_RESEARCH").strip().upper().replace("-", "_").replace(" ", "_")
    aliases = {"PRICE_PROXY": "PRICE_PROXY_RESEARCH", "RECORDED_BOOK": "RECORDED_BOOK_REPLAY"}
    normalized = aliases.get(normalized, normalized)
    if normalized not in {"PRICE_PROXY_RESEARCH", "RECORDED_BOOK_REPLAY"}:
        raise ExperimentPlanError("UNSUPPORTED_RESEARCH_MODE", f"unsupported research_mode {value!r}")
    return normalized


def _research_assumptions(value: Any, mode: str) -> dict[str, Any]:
    source = _as_mapping(value, name="assumptions") if value is not None else {}
    version = str(source.get("version", "")).strip()
    expected = "price-proxy-v1" if mode == "PRICE_PROXY_RESEARCH" else "recorded-book-replay-v1"
    if mode != "NOT_APPLICABLE" and not version:
        version = expected
    if mode != "NOT_APPLICABLE" and version != expected:
        raise ExperimentPlanError("UNSUPPORTED_SCHEMA", f"assumptions.version must be {expected!r}")
    for name in ("fee_bps", "slippage_bps", "roundtrip_fee_bps", "roundtrip_slippage_bps"):
        if name in source:
            value_number = source[name]
            if isinstance(value_number, bool):
                raise ExperimentPlanError("INVALID_PLAN", f"assumptions.{name} must be non-negative")
            try:
                value_number = float(value_number)
            except (TypeError, ValueError):
                raise ExperimentPlanError("INVALID_PLAN", f"assumptions.{name} must be numeric") from None
            if not math.isfinite(value_number) or value_number < 0:
                raise ExperimentPlanError("INVALID_PLAN", f"assumptions.{name} must be non-negative")
            source[name] = value_number
    source["version"] = version or "not-applicable"
    source["mode"] = mode
    return source


def _exit_policy(value: Any, mode: str) -> dict[str, Any]:
    if value is None:
        value = {"type": "fixed_holding_period", "holding_period": 1}
    if isinstance(value, str):
        value = {"type": value}
    policy = _as_mapping(value, name="exit_policy")
    kind = str(policy.get("type", policy.get("kind", ""))).strip().lower()
    if kind != "fixed_holding_period":
        raise ExperimentPlanError("INVALID_PLAN", "prediction research requires fixed_holding_period exit_policy")
    period = policy.get("holding_period", policy.get("bars", policy.get("observations", 1)))
    if isinstance(period, bool) or not isinstance(period, int) or period < 1 or period > MAX_SAMPLES:
        raise ExperimentPlanError("INVALID_PLAN", "exit_policy.holding_period must be a bounded positive integer")
    return {"type": "fixed_holding_period", "holding_period": period}


def _trial_budget(value: Any, *, max_variants: int) -> dict[str, Any]:
    budget = _as_mapping(value, name="trial_budget")
    limit = budget.get("limit", budget.get("max_trials", max_variants))
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= MAX_PLAN_VARIANTS:
        raise ExperimentPlanError("EXPERIMENT_BUDGET_EXCEEDED", "trial_budget.limit must be between one and 64")
    budget["limit"] = limit
    budget["locked"] = True
    return budget


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
    market_scope: MarketScopePolicy
    filters: Mapping[str, Any]
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
    research_mode: str = "PRICE_PROXY_RESEARCH"
    assumptions: Mapping[str, Any] = field(
        default_factory=lambda: MappingProxyType({})
    )
    exit_policy: Mapping[str, Any] = field(
        default_factory=lambda: MappingProxyType(
            {"type": "fixed_holding_period", "holding_period": 1}
        )
    )
    trial_budget: Mapping[str, Any] = field(
        default_factory=lambda: MappingProxyType({"limit": 1, "locked": True})
    )
    campaign_id: str | None = None
    campaign_trial_id: str | None = None
    campaign_configuration_id: str | None = None
    campaign_protocol: Mapping[str, Any] | None = None
    scientific_rationale: str | None = None
    dataset_boundary: Mapping[str, Any] | None = None
    configuration_manifest: Mapping[str, Any] | None = None
    observation_horizon: Mapping[str, Any] | None = None
    qualification_gates: Mapping[str, Any] | None = None
    validation_policy: Mapping[str, Any] | None = None
    final_assessment_policy: Mapping[str, Any] | None = None
    protected_row_identity_manifests: Mapping[str, Any] | None = None
    selection_excluded: bool = False

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
        raw_market_scope = raw.get("market_scope")
        # A nested market scope owns current filters. Top-level filters are
        # historical qualification filters, except for legacy documents that
        # have no nested scope and therefore used that field for both roles.
        scope_filters = raw.get("filters") if not isinstance(raw_market_scope, Mapping) else None
        market_scope = normalize_market_scope(
            raw_market_scope,
            target=raw.get("target"),
            market_ids=raw.get("market_ids"),
            target_market_ids=raw.get("target_market_ids"),
            target_instrument=raw.get("target_instrument"),
            instrument=raw.get("instrument"),
            categories=raw.get("categories"),
            filters=scope_filters,
            regime_restrictions=raw.get("regime_restrictions"),
        )
        if "filters" in raw:
            try:
                historical_filters = normalize_forward_filters(raw.get("filters"))
            except ExperimentPlanError as exc:
                raise ExperimentPlanError("UNSUPPORTED_FEATURE", exc.detail) from exc
        else:
            # Legacy plans used market_scope.filters for historical selection.
            historical_filters = market_scope.filters
        provided_scope_hash = raw.get("market_scope_hash")
        if provided_scope_hash is not None and str(provided_scope_hash).strip() != market_scope.policy_hash:
            raise ExperimentPlanError("CONFLICTING_MARKET_SCOPE", "market_scope_hash does not match canonical market scope")
        provided_scope_version = raw.get("market_scope_version")
        if provided_scope_version is not None and str(provided_scope_version).strip() != market_scope.version:
            raise ExperimentPlanError("CONFLICTING_MARKET_SCOPE", "market_scope_version does not match canonical market scope")
        target = {
            **({"instrument": market_scope.instrument} if market_scope.instrument else {}),
            **({"market_ids": list(market_scope.market_ids)} if market_scope.market_ids else {}),
            **({"categories": list(market_scope.categories)} if market_scope.categories else {}),
        }
        legacy_target = raw.get("target")
        legacy_target_dataset_id = (
            legacy_target.get("dataset_id")
            if isinstance(legacy_target, Mapping)
            else None
        )
        selector = _normalize_dataset_selector(raw.get("dataset_selector"))
        for raw_name, selector_name in (
            ("dataset_id", "dataset_id"),
            ("dataset_version", "dataset_version"),
            ("dataset_timeframe", "timeframe"),
            ("dataset_source", "source"),
            ("dataset_source_type", "source_type"),
            ("dataset_constituent_ids", "constituent_market_ids"),
            ("constituent_market_ids", "constituent_market_ids"),
            ("constituents", "constituents"),
            ("market_versions", "market_versions"),
            ("survivorship_bias", "survivorship_bias"),
        ):
            supplied = raw.get(raw_name)
            if supplied is None:
                continue
            if selector_name in selector and _canonical(selector[selector_name]) != _canonical(supplied):
                raise ExperimentPlanError(
                    "CONFLICTING_DATASET_SELECTOR",
                    f"dataset selector {selector_name} conflicts with top-level {raw_name}",
                )
            selector[selector_name] = supplied
        if selector.get("dataset_version") is None and selector.get("version") is not None:
            selector["dataset_version"] = str(selector["version"]).strip()
        if selector.get("source") is None and selector.get("provider") is not None:
            selector["source"] = str(selector["provider"]).strip()
        if selector.get("survivorship_bias") is None and selector.get("survivorship") is not None:
            selector["survivorship_bias"] = str(selector["survivorship"]).strip()
        selector = _normalize_dataset_selector(selector)
        if not str(selector.get("dataset_version", "")).strip():
            raise ExperimentPlanError("INSUFFICIENT_DATA", "dataset_version is required")
        selector["dataset_version"] = str(selector["dataset_version"]).strip()
        if not selector.get("dataset_id") and legacy_target_dataset_id is not None:
            if not isinstance(legacy_target_dataset_id, str) or not legacy_target_dataset_id.strip():
                raise ExperimentPlanError("MALFORMED_DATASET_SELECTOR", "target.dataset_id must be non-empty text")
            selector["dataset_id"] = legacy_target_dataset_id.strip()
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
        family_budget["budget_id"] = AUTONOMOUS_BUDGET_ID
        for name, default in (("total_limit", 1000), ("per_family_limit", 250)):
            value = family_budget.get(name, default)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ExperimentPlanError("EXPERIMENT_BUDGET_EXCEEDED", f"{name} must be a non-negative integer")
            family_budget[name] = value
        max_variants = raw.get("max_variants", min(MAX_PLAN_VARIANTS, possible_variants))
        if isinstance(max_variants, bool) or not isinstance(max_variants, int) or not 1 <= max_variants <= MAX_PLAN_VARIANTS:
            raise ExperimentPlanError("EXPERIMENT_BUDGET_EXCEEDED", "max_variants must be between one and 64")
        research_mode = _research_mode(raw.get("research_mode"), market_type)
        assumptions = _research_assumptions(raw.get("assumptions"), research_mode)
        exit_policy = _exit_policy(raw.get("exit_policy", raw.get("exit")), research_mode) if market_type is MarketType.PREDICTION else {"type": "not_applicable"}
        trial_budget = _trial_budget(raw.get("trial_budget"), max_variants=max_variants)
        campaign_protocol = _as_mapping(raw.get("campaign_protocol"), name="campaign_protocol") if raw.get("campaign_protocol") is not None else {}
        protocol_budget = campaign_protocol.get("budget_limit", campaign_protocol.get("campaign_budget"))
        if protocol_budget is not None:
            if isinstance(protocol_budget, bool) or not isinstance(protocol_budget, int) or not 1 <= protocol_budget <= 24:
                raise ExperimentPlanError("EXPERIMENT_BUDGET_EXCEEDED", "campaign budget must be between one and 24")
        campaign_id = str(raw.get("campaign_id", "")).strip() or None
        campaign_trial_id = str(raw.get("campaign_trial_id", "")).strip() or None
        campaign_configuration_id = str(raw.get("campaign_configuration_id", "")).strip() or None
        scientific_rationale = str(raw.get("scientific_rationale", "")).strip() or None
        protocol_mapping_fields = (
            "dataset_boundary",
            "configuration_manifest",
            "observation_horizon",
            "qualification_gates",
            "validation_policy",
            "final_assessment_policy",
            "protected_row_identity_manifests",
        )
        normalized_protocol_fields: dict[str, Mapping[str, Any]] = {}
        for field_name in protocol_mapping_fields:
            supplied = raw.get(field_name)
            if supplied is not None:
                normalized_protocol_fields[field_name] = _as_mapping(supplied, name=field_name)
        for field_name, value in normalized_protocol_fields.items():
            campaign_protocol.setdefault(field_name, value)
        selection_excluded = raw.get("selection_excluded", False)
        if selection_excluded is None:
            selection_excluded = False
        if not isinstance(selection_excluded, bool):
            raise ExperimentPlanError("INVALID_PLAN", "selection_excluded must be boolean")
        methodology["research_mode"] = research_mode
        methodology["assumptions"] = assumptions
        methodology["exit_policy"] = exit_policy
        methodology["chronological_locked"] = True
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
            "market_scope": market_scope.as_dict(),
            "market_scope_hash": market_scope.policy_hash,
            "market_scope_version": market_scope.version,
            "filters": dict(historical_filters),
            "regime_restrictions": dict(market_scope.regime_restrictions),
            "target": target,
            "dataset_selector": selector,
            "methodology": methodology,
            "metrics": list(metrics),
            "experiment_family": experiment_family,
            "family_budget": family_budget,
            "trial_budget": trial_budget,
            "max_variants": max_variants,
            "min_samples": min_samples,
            "min_trades": min_trades,
            "paper_only": True,
            "research_mode": research_mode,
            "assumptions": assumptions,
            "exit_policy": exit_policy,
            "strategy_document": dict(strategy_document) if strategy_document is not None else None,
            "model_document": dict(model_document) if model_document is not None else None,
            "universe": universe,
            "campaign_id": campaign_id,
            "campaign_trial_id": campaign_trial_id,
            "campaign_configuration_id": campaign_configuration_id,
            "campaign_protocol": _plain_json(campaign_protocol) if campaign_protocol is not None else None,
            "scientific_rationale": scientific_rationale,
            **{key: _plain_json(value) for key, value in normalized_protocol_fields.items()},
            "selection_excluded": selection_excluded,
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
            schema_version=schema_version,
            plan_id=plan_id,
            hypothesis_id=resolved_hypothesis,
            market_type=market_type,
            template=template_key,
            allowed_features=features,
            parameters=MappingProxyType({key: tuple(values) for key, values in sorted(parameters.items())}),
            market_scope=market_scope,
            filters=_freeze_json(historical_filters),
            dataset_selector=_freeze_json(selector),
            methodology=_freeze_json(methodology),
            metrics=metrics,
            experiment_family=normalized["experiment_family"],
            family_budget=_freeze_json(family_budget),
            max_variants=max_variants,
            min_samples=min_samples,
            min_trades=min_trades,
            paper_only=True,
            strategy_document=_freeze_json(strategy_document) if strategy_document is not None else None,
            model_document=_freeze_json(model_document) if model_document is not None else None,
            universe=_freeze_json(universe) if universe is not None else None,
            research_mode=research_mode,
            assumptions=_freeze_json(assumptions),
            exit_policy=_freeze_json(exit_policy),
            trial_budget=_freeze_json(trial_budget),
            campaign_id=campaign_id,
            campaign_trial_id=campaign_trial_id,
            campaign_configuration_id=campaign_configuration_id,
            campaign_protocol=_freeze_json(campaign_protocol) if campaign_protocol is not None else None,
            scientific_rationale=scientific_rationale,
            dataset_boundary=_freeze_json(normalized_protocol_fields["dataset_boundary"]) if "dataset_boundary" in normalized_protocol_fields else None,
            configuration_manifest=_freeze_json(normalized_protocol_fields["configuration_manifest"]) if "configuration_manifest" in normalized_protocol_fields else None,
            observation_horizon=_freeze_json(normalized_protocol_fields["observation_horizon"]) if "observation_horizon" in normalized_protocol_fields else None,
            qualification_gates=_freeze_json(normalized_protocol_fields["qualification_gates"]) if "qualification_gates" in normalized_protocol_fields else None,
            validation_policy=_freeze_json(normalized_protocol_fields["validation_policy"]) if "validation_policy" in normalized_protocol_fields else None,
            final_assessment_policy=_freeze_json(normalized_protocol_fields["final_assessment_policy"]) if "final_assessment_policy" in normalized_protocol_fields else None,
            protected_row_identity_manifests=_freeze_json(normalized_protocol_fields["protected_row_identity_manifests"]) if "protected_row_identity_manifests" in normalized_protocol_fields else None,
            selection_excluded=selection_excluded,
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
                "filters": proposal.get("filters"),
                "target": proposal.get("target"),
                "dataset_version": proposal.get("dataset_version"),
                "time_split": proposal.get("time_split", "train-validation-holdout"),
                "metrics": proposal.get("metrics") or ("expectancy", "drawdown", "trade_count", "sample_count"),
                "experiment_family": proposal.get("experiment_family"),
                "max_variants": proposal.get("max_variants", 4),
                "min_samples": proposal.get("min_samples", 30),
                "min_trades": proposal.get("min_trades", 0),
                "research_mode": proposal.get("research_mode"),
                "assumptions": proposal.get("assumptions"),
                "exit_policy": proposal.get("exit_policy", proposal.get("exit")),
                "trial_budget": proposal.get("trial_budget"),
                "campaign_id": proposal.get("campaign_id"),
                "campaign_trial_id": proposal.get("campaign_trial_id"),
                "campaign_configuration_id": proposal.get("campaign_configuration_id"),
                "campaign_protocol": proposal.get("campaign_protocol"),
                "scientific_rationale": proposal.get("scientific_rationale"),
                "dataset_boundary": proposal.get("dataset_boundary"),
                "configuration_manifest": proposal.get("configuration_manifest"),
                "observation_horizon": proposal.get("observation_horizon"),
                "qualification_gates": proposal.get("qualification_gates"),
                "validation_policy": proposal.get("validation_policy"),
                "final_assessment_policy": proposal.get("final_assessment_policy"),
                "protected_row_identity_manifests": proposal.get("protected_row_identity_manifests"),
                "selection_excluded": proposal.get("selection_excluded"),
                "paper_only": proposal.get("paper_only") if proposal.get("paper_only") is not None else True,
            }
            if "filters" not in proposal:
                raw_plan.pop("filters", None)
            if raw_plan.get("experiment_family") is None:
                raw_plan.pop("experiment_family", None)
            if isinstance(proposal.get("strategy_document"), Mapping):
                raw_plan["strategy_document"] = proposal["strategy_document"]
            if isinstance(proposal.get("model_document"), Mapping):
                raw_plan["model_document"] = proposal["model_document"]
        if not isinstance(raw_plan, Mapping):
            raise ExperimentPlanError("INVALID_PLAN", "experiment_plan must be an object")
        plan_document = dict(raw_plan)
        # Preserve proposal-level compatibility aliases when an embedded plan
        # is supplied.  The plan parser remains the single conflict checker.
        for alias in (
            "market_scope",
            "market_scope_hash",
            "market_scope_version",
            "filters",
            "regime_restrictions",
            "target",
            "target_instrument",
            "market_ids",
            "target_market_ids",
            "instrument",
            "categories",
            "dataset_selector",
            "dataset_timeframe",
            "dataset_source",
            "dataset_source_type",
            "dataset_constituent_ids",
            "constituent_market_ids",
            "constituents",
            "market_versions",
            "survivorship_bias",
        ):
            if alias not in proposal:
                continue
            supplied = proposal[alias]
            scope_alias = alias in {
                "market_scope",
                "market_scope_hash",
                "market_scope_version",
                "regime_restrictions",
                "target",
                "target_instrument",
                "market_ids",
                "target_market_ids",
                "instrument",
                "categories",
            }
            if scope_alias and (
                supplied is None
                or supplied == {}
                or supplied == []
                or supplied == ()
            ):
                continue
            if alias == "filters" and supplied == {}:
                # An explicit empty proposal filter set is meaningful. It
                # intentionally replaces an embedded legacy copy instead of
                # falling back to that copy.
                plan_document[alias] = supplied
                continue
            if alias in plan_document and _canonical(plan_document[alias]) != _canonical(supplied):
                if alias == "market_scope":
                    try:
                        nested_scope = normalize_market_scope(plan_document[alias])
                        supplied_scope = normalize_market_scope(supplied)
                    except (ExperimentPlanError, TypeError, ValueError):
                        nested_scope = supplied_scope = None
                    if (
                        nested_scope is not None
                        and supplied_scope is not None
                        and _scope_equivalent(nested_scope, supplied_scope)
                    ):
                        continue
                reason = "CONFLICTING_MARKET_SCOPE" if scope_alias else "CONFLICTING_DATASET_SELECTOR"
                raise ExperimentPlanError(reason, f"proposal and experiment plan {alias} values differ")
            plan_document.setdefault(alias, supplied)
        if proposal.get("dataset_id") is not None:
            proposal_dataset_id = str(proposal["dataset_id"]).strip()
            plan_selector = plan_document.get("dataset_selector")
            if isinstance(plan_selector, Mapping):
                selector_dataset_id = plan_selector.get("dataset_id")
                if (
                    selector_dataset_id is not None
                    and str(selector_dataset_id).strip() != proposal_dataset_id
                ):
                    raise ExperimentPlanError("INVALID_PLAN", "proposal and experiment plan dataset ids differ")
                plan_selector = dict(plan_selector)
                plan_selector.setdefault("dataset_id", proposal_dataset_id)
                plan_document["dataset_selector"] = plan_selector
            else:
                plan_document.setdefault("dataset_id", proposal_dataset_id)
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
    def regime_restrictions(self) -> Mapping[str, Any]:
        """Compatibility view derived from the canonical market scope."""
        return self.market_scope.regime_restrictions

    @property
    def target(self) -> Mapping[str, Any]:
        """Compatibility target view derived from the canonical market scope."""
        result: dict[str, Any] = {}
        if self.market_scope.instrument is not None:
            result["instrument"] = self.market_scope.instrument
        if self.market_scope.market_ids:
            result["market_ids"] = list(self.market_scope.market_ids)
        if self.market_scope.categories:
            result["categories"] = list(self.market_scope.categories)
        return result

    @property
    def market_scope_hash(self) -> str:
        return self.market_scope.policy_hash

    @property
    def market_scope_version(self) -> str:
        return self.market_scope.version


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
            "market_scope": self.market_scope.as_dict(),
            "market_scope_hash": self.market_scope_hash,
            "market_scope_version": self.market_scope_version,
            "filters": _plain_json(self.filters),
            "regime_restrictions": _plain_json(self.regime_restrictions),
            "target": _plain_json(self.target),
            "dataset_selector": _plain_json(self.dataset_selector),
            "methodology": _plain_json(self.methodology),
            "metrics": list(self.metrics),
            "experiment_family": self.experiment_family,
            "family_budget": _plain_json(self.family_budget),
            "trial_budget": _plain_json(self.trial_budget),
            "max_variants": self.max_variants,
            "min_samples": self.min_samples,
            "min_trades": self.min_trades,
            "paper_only": True,
            "research_mode": self.research_mode,
            "assumptions": _plain_json(self.assumptions),
            "exit_policy": _plain_json(self.exit_policy),
        }
        if self.campaign_id is not None:
            result["campaign_id"] = self.campaign_id
        if self.campaign_trial_id is not None:
            result["campaign_trial_id"] = self.campaign_trial_id
        if self.campaign_configuration_id is not None:
            result["campaign_configuration_id"] = self.campaign_configuration_id
        if self.campaign_protocol is not None:
            result["campaign_protocol"] = _plain_json(self.campaign_protocol)
        if self.scientific_rationale is not None:
            result["scientific_rationale"] = self.scientific_rationale
        for name, value in (
            ("dataset_boundary", self.dataset_boundary),
            ("configuration_manifest", self.configuration_manifest),
            ("observation_horizon", self.observation_horizon),
            ("qualification_gates", self.qualification_gates),
            ("validation_policy", self.validation_policy),
            ("final_assessment_policy", self.final_assessment_policy),
            ("protected_row_identity_manifests", self.protected_row_identity_manifests),
        ):
            if value is not None:
                result[name] = _plain_json(value)
        if self.selection_excluded:
            result["selection_excluded"] = True
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


def normalize_forward_filters(
    filters: Mapping[str, Any] | None,
    regime_restrictions: Mapping[str, Any] | None = None,
) -> Mapping[str, Any]:
    """Return the small, immutable filter vocabulary used by forward authority.

    This intentionally mirrors :meth:`AutonomousResearchProcessor._apply_plan_filters`.
    The authority is a read-only consumer of a frozen plan, so unsupported keys
    are rejected instead of being silently ignored.
    """
    source = dict(filters or {})
    restrictions = dict(regime_restrictions or {})
    supported = {
        "entry_price",
        "minimum_hours_to_resolution",
        "maximum_hours_to_resolution",
        "min_liquidity",
        "max_spread",
        "regime",
        "regimes",
        "category",
    }
    restriction_fields = {"regime", "regimes", "allowed_regimes", "allowed_states"}
    unknown = sorted(set(source) - supported)
    unknown_restrictions = sorted(set(restrictions) - restriction_fields)
    if unknown or unknown_restrictions:
        fields = unknown + unknown_restrictions
        raise ExperimentPlanError("UNSUPPORTED_FEATURE", f"unsupported forward filters: {fields}")

    numeric_keys = {
        "entry_price",
        "minimum_hours_to_resolution",
        "maximum_hours_to_resolution",
        "min_liquidity",
        "max_spread",
    }
    result: dict[str, Any] = {}
    for key in (
        "entry_price",
        "minimum_hours_to_resolution",
        "maximum_hours_to_resolution",
        "min_liquidity",
        "max_spread",
        "category",
    ):
        if key not in source:
            continue
        value = source[key]
        if key == "category":
            if isinstance(value, (list, tuple)):
                values = tuple(str(item).strip().casefold() for item in value if str(item).strip())
                if not values:
                    raise ExperimentPlanError("UNSUPPORTED_FEATURE", "category filter must not be empty")
                result[key] = list(dict.fromkeys(values))
            elif isinstance(value, str) and value.strip():
                result[key] = value.strip().casefold()
            else:
                raise ExperimentPlanError("UNSUPPORTED_FEATURE", "category filter must be text")
        elif key in numeric_keys:
            result[key] = _forward_numeric_bound(value, path=f"filters.{key}")
        else:
            raise AssertionError(f"unhandled forward filter {key}")
    regime = source.get("regime", source.get("regimes"))
    restricted = restrictions.get(
        "allowed_regimes",
        restrictions.get("allowed_states", restrictions.get("regime", restrictions.get("regimes"))),
    )
    if regime is not None and restricted is not None:
        first = _forward_regime_values(regime)
        second = _forward_regime_values(restricted)
        values = sorted(first & second)
    elif regime is not None:
        values = sorted(_forward_regime_values(regime))
    elif restricted is not None:
        values = sorted(_forward_regime_values(restricted))
    else:
        values = []
    if regime is not None or restricted is not None:
        if not values:
            raise ExperimentPlanError("UNSUPPORTED_FEATURE", "regime filter must not be empty")
        result["regimes"] = values
    return _freeze_json(result)


def _forward_numeric(value: Any, *, path: str) -> float:
    """Normalize one forward numeric value without coercing text or booleans."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ExperimentPlanError("UNSUPPORTED_FEATURE", f"{path} must be a finite number")
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        raise ExperimentPlanError("UNSUPPORTED_FEATURE", f"{path} must be a finite number")
    if not math.isfinite(number):
        raise ExperimentPlanError("UNSUPPORTED_FEATURE", f"{path} must be a finite number")
    return number


def _forward_numeric_bound(value: Any, *, path: str) -> float | list[float]:
    if isinstance(value, (list, tuple)):
        if not value or len(value) > 256:
            raise ExperimentPlanError("UNSUPPORTED_FEATURE", f"{path} must be a bounded non-empty list")
        return [_forward_numeric(item, path=f"{path}[]") for item in value]
    if isinstance(value, Mapping):
        raise ExperimentPlanError("UNSUPPORTED_FEATURE", f"{path} must be a finite scalar or bounded list")
    return _forward_numeric(value, path=path)



def _forward_regime_values(value: Any) -> set[str]:
    if isinstance(value, str):
        values = {value.strip()}
    elif isinstance(value, (list, tuple)):
        values = {str(item).strip() for item in value if str(item).strip()}
    else:
        raise ExperimentPlanError("UNSUPPORTED_FEATURE", "regime filter must be text or a bounded list")
    if not values:
        raise ExperimentPlanError("UNSUPPORTED_FEATURE", "regime filter must not be empty")
    return values


def _forward_market_value(market: Mapping[str, Any], key: str) -> Any:
    if key in market:
        return market[key]
    for nested_name in ("payload", "snapshot", "metadata", "extra"):
        nested = market.get(nested_name)
        if isinstance(nested, Mapping):
            value = _forward_market_value(nested, key)
            if value is not None:
                return value
    return None


def _forward_market_instrument(market: Mapping[str, Any]) -> Any:
    """Return only persisted instrument identity fields, never market_id."""
    value = market.get("instrument")
    if value is not None:
        return value
    metadata = market.get("metadata")
    if isinstance(metadata, Mapping):
        value = metadata.get("symbol", metadata.get("instrument"))
        if value is not None:
            return value
    payload = market.get("payload")
    if isinstance(payload, Mapping):
        value = payload.get("instrument")
        if value is not None:
            return value
        metadata = payload.get("metadata")
        if isinstance(metadata, Mapping):
            return metadata.get("symbol", metadata.get("instrument"))
    return None


def _forward_identity(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip().casefold()
    return normalized or None


def forward_market_matches(
    market: Mapping[str, Any],
    filters: Mapping[str, Any] | None,
    *,
    now: Any | None = None,
    target_instrument: str | None = None,
) -> bool:
    """Match one current market against normalized forward filters.

    The function is deliberately pure: it performs no catalog lookup and
    treats malformed values as a non-match (fail closed).
    """
    if not isinstance(market, Mapping):
        return False
    if target_instrument is not None:
        expected_instrument = _forward_identity(target_instrument)
        actual_instrument = _forward_identity(_forward_market_instrument(market))
        if expected_instrument is None or actual_instrument != expected_instrument:
            return False
    try:
        normalized = normalize_forward_filters(filters)
    except (ExperimentPlanError, TypeError, ValueError):
        return False

    def finite(value: Any) -> float:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return math.nan
        try:
            number = float(value)
        except (TypeError, ValueError, OverflowError):
            return math.nan
        return number if math.isfinite(number) else math.nan

    def in_bound(value: float, bound: Any) -> bool:
        if not math.isfinite(value):
            return False
        if isinstance(bound, (list, tuple)):
            if len(bound) == 2:
                low, high = finite(bound[0]), finite(bound[1])
                return math.isfinite(low) and math.isfinite(high) and min(low, high) <= value <= max(low, high)
            return any(abs(value - finite(item)) <= 1e-12 for item in bound if math.isfinite(finite(item)))
        target = finite(bound)
        return math.isfinite(target) and abs(value - target) <= 1e-12

    if "entry_price" in normalized:
        price = finite(_forward_market_value(market, "yes_mid"))
        if not math.isfinite(price):
            price = finite(_forward_market_value(market, "yes_ask"))
        if not in_bound(price, normalized["entry_price"]):
            return False
    raw_expiry = _forward_market_value(market, "expiry")
    if raw_expiry is None:
        expiry_seconds = _forward_market_value(market, "time_to_expiry_seconds")
    else:
        expiry = parse_timestamp(raw_expiry)
        reference = parse_timestamp(now)
        if now is None:
            # Keep the predicate deterministic when callers omit ``now`` by
            # using the market's own observation timestamp as its reference.
            reference = parse_timestamp(_forward_market_value(market, "timestamp"))
        expiry_seconds = (expiry - reference).total_seconds() if expiry is not None and reference is not None else math.nan
    expiry_seconds = finite(expiry_seconds)
    if "minimum_hours_to_resolution" in normalized and (
        not math.isfinite(expiry_seconds)
        or expiry_seconds < finite(normalized["minimum_hours_to_resolution"]) * 3600.0
    ):
        return False
    if "maximum_hours_to_resolution" in normalized and (
        not math.isfinite(expiry_seconds)
        or expiry_seconds > finite(normalized["maximum_hours_to_resolution"]) * 3600.0
    ):
        return False
    if "min_liquidity" in normalized:
        liquidity = finite(_forward_market_value(market, "liquidity"))
        if not math.isfinite(liquidity) or liquidity < finite(normalized["min_liquidity"]):
            return False
    if "max_spread" in normalized:
        spread_value = _forward_market_value(market, "spread")
        if spread_value is None:
            quotes = _forward_market_value(market, "quotes")
            if isinstance(quotes, Mapping):
                # Prefer the explicitly collected YES spread, then NO spread.
                spread_value = quotes.get("yes_spread")
                if spread_value is None:
                    spread_value = quotes.get("no_spread")
                if spread_value is None:
                    yes_bid = finite(quotes.get("yes_bid"))
                    yes_ask = finite(quotes.get("yes_ask"))
                    if math.isfinite(yes_bid) and math.isfinite(yes_ask):
                        spread_value = yes_ask - yes_bid
        if spread_value is None:
            yes_bid = finite(_forward_market_value(market, "yes_bid"))
            yes_ask = finite(_forward_market_value(market, "yes_ask"))
            if math.isfinite(yes_bid) and math.isfinite(yes_ask):
                spread_value = yes_ask - yes_bid
        spread = finite(spread_value)
        if not math.isfinite(spread) or spread > finite(normalized["max_spread"]):
            return False
    if "category" in normalized:
        actual = str(_forward_market_value(market, "category") or "").strip().casefold()
        expected = normalized["category"]
        allowed = expected if isinstance(expected, (list, tuple)) else (expected,)
        if actual not in {str(item).casefold() for item in allowed}:
            return False
    if "regimes" in normalized:
        actual = str(
            _forward_market_value(market, "regime")
            or _forward_market_value(market, "regime_state")
            or ""
        )
        if actual not in set(str(item) for item in normalized["regimes"]):
            return False
    return True


def historical_market_ids(value: Any) -> tuple[str, ...]:
    """Extract explicitly provenance-bound historical constituent ids.

    Only fields that describe dataset constituents are considered.  Arbitrary
    target lists are never classified as historical merely because a dataset
    is historical, which preserves genuine exact current targets.
    """
    found: list[str] = []
    keys = {
        "historical_market_ids",
        "historical_constituent_market_ids",
        "constituent_market_ids",
        "market_versions",
        "constituents",
        "constituent_bindings",
    }

    def visit(item: Any, hinted: bool = False) -> None:
        if isinstance(item, Mapping):
            for key, child in item.items():
                normalized = _normal_key(key)
                child_hint = hinted or normalized in keys
                if normalized in {"market_id", "marketid"} and (hinted or child_hint):
                    text = str(child).strip()
                    if text:
                        found.append(text)
                visit(child, child_hint)
        elif hinted and isinstance(item, (list, tuple)):
            for child in item:
                if isinstance(child, str):
                    text = child.strip()
                    if text:
                        found.append(text)
                else:
                    visit(child, True)

    visit(value)
    return tuple(dict.fromkeys(found))


__all__ = [
    "AUTONOMOUS_BUDGET_ID",
    "ExperimentPlan",
    "ExperimentPlanError",
    "MARKET_SCOPE_SCHEMA_VERSION",
    "MARKET_SCOPE_VERSION",
    "MarketScopeMode",
    "MarketScopePolicy",
    "MAX_PLAN_VARIANTS",
    "PLAN_SCHEMA_VERSION",
    "forward_market_matches",
    "historical_market_ids",
    "normalize_forward_filters",
    "normalize_market_scope",
]
