"""Pure contracts and deterministic selection for a rolling strategy portfolio.

This module deliberately has no storage, clock, network, or execution dependencies.  A
caller supplies an immutable policy, evidence windows, the previous selection, and the
review timestamp; :func:`evaluate_rolling_selection` returns a new decision.  Decimal
values are retained throughout evaluation and rendered as strings only at the
serialization boundary.
"""
from __future__ import annotations

from dataclasses import dataclass, field, fields, is_dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
from types import MappingProxyType
from typing import Any, Mapping, Sequence


UTC = timezone.utc
ZERO = Decimal("0")
ONE = Decimal("1")
MAX_ID_LENGTH = 128
MAX_REASON_LENGTH = 512
MAX_K = 10
_MAX_REMOVED_MEMBER_EVIDENCE_HISTORY = 64
SECONDS_PER_DAY = Decimal("86400")


def _bounded_removed_member_evidence_history(
    value: Mapping[str, Sequence[str]] | Any,
) -> dict[str, tuple[str, ...]]:
    """Normalize and retain only the newest bounded removal evidence.

    Histories are serialized oldest-to-newest for each strategy.  Preserve that
    order while deduplicating, then retain the newest global suffix so the
    selection payload and its digest cannot grow without bound.
    """
    if not isinstance(value, Mapping):
        return {}
    entries: list[tuple[str, str]] = []
    seen: dict[str, set[str]] = {}
    for raw_strategy_id, raw_digests in value.items():
        strategy_id = str(raw_strategy_id).strip()
        if not strategy_id:
            continue
        if isinstance(raw_digests, str):
            digests: Any = (raw_digests,)
        elif isinstance(raw_digests, (set, frozenset)):
            digests = sorted(raw_digests, key=str)
        else:
            try:
                digests = iter(raw_digests)
            except TypeError:
                continue
        strategy_seen = seen.setdefault(strategy_id, set())
        for raw_digest in digests:
            digest = str(raw_digest).strip()
            if digest and digest not in strategy_seen:
                strategy_seen.add(digest)
                entries.append((strategy_id, digest))
    entries = entries[-_MAX_REMOVED_MEMBER_EVIDENCE_HISTORY:]
    bounded: dict[str, list[str]] = {}
    for strategy_id, digest in entries:
        bounded.setdefault(strategy_id, []).append(digest)
    return {strategy_id: tuple(digests) for strategy_id, digests in bounded.items()}


DEFAULT_SCORE_FORMULA = (
"net_return_rate * net_return_weight "
"- drawdown * drawdown_penalty_weight "
"+ reliability * reliability_weight "
"- overlap_penalty * overlap_penalty_weight; "
"net_return_rate = allocated_capital_net_return / allocated_capital; "
"allocated_capital_net_return is already net of persisted execution costs; "
"execution_cost_rate remains an informational cost breakdown"
)
SCORE_FORMULA_VERSION = "rolling-score-v1"
POLICY_VERSION = "rolling-admission-v1"
REASON_REDUCE = "REDUCE"
_MEMBER_ACTIONS = frozenset({"ADD", "HOLD", "REDUCE", "OBSERVE"})
_UNKNOWN_OVERLAP_KEY = "unknown"
_SUPPORTED_WINDOW_DAYS = frozenset({7, 30})
_SOURCE_CLASS_ALIASES = {
    "HISTORICAL": "HISTORICAL",
    "PRICE_PROXY": "HISTORICAL",
    "REPLAY": "REPLAY",
    "REPLAY_SIMULATED": "REPLAY",
    "ORDER_BOOK_SIMULATED": "REPLAY",
    "PAPER": "PAPER",
    "PAPER_FORWARD": "PAPER",
    "FORWARD_PAPER": "PAPER",
    "LIVE": "LIVE",
    "FORWARD_COLLECTED": "LIVE",
}
_CANONICAL_SOURCE_CLASSES = frozenset({"HISTORICAL", "REPLAY", "PAPER", "LIVE"})

# Machine-readable reasons are intentionally stable: they are persisted and are more
# useful to operators than a free-form explanation assembled by a caller.
REASON_ELIGIBLE = "ELIGIBLE_SCORE"
REASON_LOW_EVIDENCE = "LOW_EVIDENCE"
REASON_HARD_FAILURE = "HARD_FAILURE"
REASON_OVERLAP_DUPLICATE = "OVERLAP_DUPLICATE"
REASON_RETAINED_ORDINARY_LOSS = "RETAINED_ORDINARY_LOSS"
REASON_RETAINED_POSITION_MANAGEMENT = "RETAINED_POSITION_MANAGEMENT"
REASON_COOLDOWN = "REPLACEMENT_COOLDOWN"
REASON_REPLACEMENT_MARGIN = "REPLACEMENT_MARGIN_NOT_MET"
REASON_REPLACED = "REPLACED_BY_STRONGER_EVIDENCE"
REASON_NO_NEW_EVIDENCE = "NO_NEW_EVIDENCE"
REASON_FUTURE_EVIDENCE = "FUTURE_EVIDENCE"
REASON_CAPACITY = "CAPACITY_LIMIT"
REASON_EXPERIMENTAL_DISABLED = "EXPERIMENTAL_ALLOCATION_DISABLED"

_ACTIVE_STATUSES = frozenset({"ACTIVE", "PAPER", "REDUCE"})
_REMOVED_STATUSES = frozenset({"REMOVED", "RETIRED"})
_HARD_FAILURE_VALUES = frozenset(
    {
        "FAIL",
        "FAILED",
        "FALSE",
        "HARD_FAILURE",
        "INFEASIBLE",
        "PAUSE",
        "PAUSED",
        "UNSAFE",
        "0",
    }
)


def _text(value: Any, name: str, *, max_length: int = MAX_ID_LENGTH, required: bool = True) -> str:
    if value is None and not required:
        return ""
    result = str(value).strip()
    if required and not result:
        raise ValueError(f"{name} is required")
    if len(result) > max_length:
        raise ValueError(f"{name} exceeds {max_length} characters")
    return result

def _source_class(value: Any, *, required: bool = True) -> str:
    text = _text(value, "source_class", max_length=64, required=required)
    if not text:
        return ""
    canonical = _SOURCE_CLASS_ALIASES.get(text.upper().replace("-", "_").replace(" ", "_"))
    if canonical is None:
        raise ValueError(f"unsupported source_class: {text}")
    return canonical


def _decimal(value: Any, name: str, *, default: Decimal | None = None) -> Decimal:
    if value is None or value == "":
        if default is not None:
            return default
        raise ValueError(f"{name} is required")
    if isinstance(value, bool):
        raise ValueError(f"{name} must be a finite decimal")
    try:
        parsed = value if isinstance(value, Decimal) else Decimal(str(value).strip())
    except (InvalidOperation, TypeError, ValueError, ArithmeticError) as exc:
        raise ValueError(f"{name} must be a finite decimal") from exc
    if not parsed.is_finite():
        raise ValueError(f"{name} must be a finite decimal")
    return parsed


def _nonnegative(value: Any, name: str, *, default: Decimal | None = None) -> Decimal:
    parsed = _decimal(value, name, default=default)
    if parsed < ZERO:
        raise ValueError(f"{name} must be non-negative")
    return parsed


def _ratio(value: Any, name: str, *, default: Decimal | None = None) -> Decimal:
    parsed = _decimal(value, name, default=default)
    if parsed < ZERO or parsed > ONE:
        raise ValueError(f"{name} must be between 0 and 1")
    return parsed


def _integer(value: Any, name: str, *, minimum: int | None = None, maximum: int | None = None, default: int | None = None) -> int:
    if value is None or value == "":
        if default is not None:
            return default
        raise ValueError(f"{name} is required")
    if isinstance(value, bool):
        raise ValueError(f"{name} must be an integer")
    try:
        # Do not silently truncate fractional values.
        parsed_decimal = Decimal(str(value))
        if not parsed_decimal.is_finite() or parsed_decimal != parsed_decimal.to_integral_value():
            raise ValueError
        parsed = int(parsed_decimal)
    except (InvalidOperation, TypeError, ValueError, ArithmeticError) as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if minimum is not None and parsed < minimum:
        raise ValueError(f"{name} must be at least {minimum}")
    if maximum is not None and parsed > maximum:
        raise ValueError(f"{name} must be at most {maximum}")
    return parsed


def _utc(value: Any, name: str, *, required: bool = False) -> datetime | None:
    if value is None or value == "":
        if required:
            raise ValueError(f"{name} is required")
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{name} must be an ISO timestamp") from exc
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)


def _freeze(value: Any) -> Any:
    """Recursively freeze user mappings so frozen records are actually immutable."""
    if isinstance(value, Mapping):
        return MappingProxyType({str(key): _freeze(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(item) for item in value)
    if isinstance(value, (set, frozenset)):
        return frozenset(_freeze(item) for item in value)
    return value


def _plain(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _plain(item) for key, item in value.items()}
    if isinstance(value, (tuple, list, frozenset, set)):
        return [_plain(item) for item in value]
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, datetime):
        return _utc(value, "timestamp", required=True).isoformat()
    if is_dataclass(value) and not isinstance(value, type):
        return {
            item.name: _plain(getattr(value, item.name))
            for item in fields(value)
        }
    if hasattr(value, "value") and not isinstance(value, (str, bytes)):
        # Supports enum-like status values without importing an execution module.
        return _plain(value.value)
    return value


def _canonical(value: Any) -> str:
    return json.dumps(_plain(value), sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _hash(value: Any) -> str:
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _mapping(value: Any, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{name} must be a mapping")
    return value


@dataclass(frozen=True, slots=True)
class StrategyVersion:
    """Immutable strategy identity used by rolling evidence and selections."""

    strategy_version_id: str
    strategy_id: str = ""
    version: str = "1"
    created_at: datetime | None = None
    config_hash: str = ""
    payload: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "strategy_version_id", _text(self.strategy_version_id, "strategy_version_id"))
        object.__setattr__(self, "strategy_id", _text(self.strategy_id, "strategy_id", required=False))
        object.__setattr__(self, "version", _text(self.version, "version"))
        object.__setattr__(self, "created_at", _utc(self.created_at, "created_at"))
        object.__setattr__(self, "config_hash", _text(self.config_hash, "config_hash", max_length=256, required=False))
        object.__setattr__(self, "payload", _freeze(self.payload if isinstance(self.payload, Mapping) else {}))

    @property
    def immutable_id(self) -> str:
        return self.strategy_version_id

    def as_dict(self) -> dict[str, Any]:
        return _plain({
            "strategy_version_id": self.strategy_version_id,
            "strategy_id": self.strategy_id,
            "version": self.version,
            "created_at": self.created_at,
            "config_hash": self.config_hash,
            "payload": self.payload,
        })

    to_dict = as_dict
    serialize = as_dict

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "StrategyVersion":
        row = _mapping(value, "strategy version")
        return cls(
            strategy_version_id=row.get("strategy_version_id", row.get("id")),
            strategy_id=row.get("strategy_id", ""),
            version=row.get("version", "1"),
            created_at=row.get("created_at"),
            config_hash=row.get("config_hash", ""),
            payload=row.get("payload", row.get("metadata", {})),
        )


# A short alias is useful to callers that call the persisted object a record.
StrategyVersionRecord = StrategyVersion


@dataclass(frozen=True, slots=True)
class ResearchTrial:
    """Bounded research lineage; no lineage is fabricated for legacy rows."""

    research_trial_id: str
    strategy_version_id: str
    created_at: datetime | None = None
    status: str = "OPEN"
    parent_research_trial_id: str | None = None
    payload: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        trial_id = _text(self.research_trial_id, "research_trial_id")
        strategy_id = _text(self.strategy_version_id, "strategy_version_id")
        parent = _text(self.parent_research_trial_id, "parent_research_trial_id", required=False) or None
        if parent == trial_id:
            raise ValueError("research trial cannot parent itself")
        object.__setattr__(self, "research_trial_id", trial_id)
        object.__setattr__(self, "strategy_version_id", strategy_id)
        object.__setattr__(self, "created_at", _utc(self.created_at, "created_at"))
        object.__setattr__(self, "status", _text(self.status, "status", max_length=32).upper())
        object.__setattr__(self, "parent_research_trial_id", parent)
        object.__setattr__(self, "payload", _freeze(self.payload if isinstance(self.payload, Mapping) else {}))

    def as_dict(self) -> dict[str, Any]:
        return _plain({
            "research_trial_id": self.research_trial_id,
            "strategy_version_id": self.strategy_version_id,
            "created_at": self.created_at,
            "status": self.status,
            "parent_research_trial_id": self.parent_research_trial_id,
            "payload": self.payload,
        })

    to_dict = as_dict
    serialize = as_dict

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "ResearchTrial":
        row = _mapping(value, "research trial")
        return cls(
            research_trial_id=row.get("research_trial_id", row.get("id")),
            strategy_version_id=row.get("strategy_version_id"),
            created_at=row.get("created_at"),
            status=row.get("status", "OPEN"),
            parent_research_trial_id=row.get("parent_research_trial_id"),
            payload=row.get("payload", row.get("metadata", {})),
        )


ResearchTrialRecord = ResearchTrial


@dataclass(frozen=True, slots=True)
class RollingAdmissionPolicy:
    """Versioned, serializable admission policy.

    Every threshold and weight used by evaluation is a field on this object and is
    present in :meth:`as_dict`; the evaluator never consults an unversioned global
    threshold.  ``config_hash`` identifies this complete policy configuration.
    """

    policy_id: str
    version: str
    config_hash: str | None = None
    requested_window_days: tuple[int, ...] = (7, 30)
    review_interval_days: int = 1
    max_members: int = 5
    global_budget: Decimal = Decimal("0")
    min_actual_coverage_seconds: Decimal = ZERO
    minimum_coverage_ratio: Decimal = Decimal("0.80")
    min_completed_outcomes: int = 5
    min_reliability: Decimal = Decimal("0.50")
    min_score: Decimal = ZERO
    net_return_weight: Decimal = Decimal("1")
    drawdown_penalty_weight: Decimal = Decimal("1")
    execution_cost_weight: Decimal = Decimal("1")
    reliability_weight: Decimal = Decimal("0.25")
    overlap_penalty_weight: Decimal = Decimal("0.10")
    replacement_margin: Decimal = Decimal("0.05")
    cooldown_seconds: int = 86400
    experimental_allocation_enabled: bool = False
    score_formula: str = DEFAULT_SCORE_FORMULA
    formula_version: str = SCORE_FORMULA_VERSION
    minimum_evidence: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "policy_id", _text(self.policy_id, "policy_id"))
        object.__setattr__(self, "version", _text(self.version, "version"))
        windows = tuple(
            _integer(item, "requested_window_days", minimum=1, maximum=30)
            for item in self.requested_window_days
        )
        if not windows:
            raise ValueError("requested_window_days must not be empty")
        if any(item not in _SUPPORTED_WINDOW_DAYS for item in windows):
            raise ValueError("requested_window_days must contain only supported windows 7 or 30")
        if len(set(windows)) != len(windows):
            raise ValueError("requested_window_days must not contain duplicates")
        object.__setattr__(self, "requested_window_days", windows)
        object.__setattr__(self, "review_interval_days", _integer(self.review_interval_days, "review_interval_days", minimum=1))
        object.__setattr__(self, "max_members", _integer(self.max_members, "max_members", minimum=1, maximum=MAX_K))
        object.__setattr__(self, "global_budget", _nonnegative(self.global_budget, "global_budget", default=ZERO))
        object.__setattr__(self, "min_actual_coverage_seconds", _nonnegative(self.min_actual_coverage_seconds, "min_actual_coverage_seconds", default=ZERO))
        object.__setattr__(self, "minimum_coverage_ratio", _ratio(self.minimum_coverage_ratio, "minimum_coverage_ratio", default=Decimal("0.80")))
        object.__setattr__(self, "min_completed_outcomes", _integer(self.min_completed_outcomes, "min_completed_outcomes", minimum=0))
        object.__setattr__(self, "min_reliability", _ratio(self.min_reliability, "min_reliability", default=Decimal("0.50")))
        object.__setattr__(self, "min_score", _decimal(self.min_score, "min_score", default=ZERO))
        for name in (
            "net_return_weight",
            "drawdown_penalty_weight",
            "execution_cost_weight",
            "reliability_weight",
            "overlap_penalty_weight",
            "replacement_margin",
        ):
            parsed = _nonnegative(getattr(self, name), name)
            object.__setattr__(self, name, parsed)
        object.__setattr__(self, "cooldown_seconds", _integer(self.cooldown_seconds, "cooldown_seconds", minimum=0))
        if not isinstance(self.experimental_allocation_enabled, bool):
            raise TypeError("experimental_allocation_enabled must be a bool")
        formula = _text(self.score_formula, "score_formula", max_length=2048)
        formula_version = _text(self.formula_version, "formula_version", max_length=64)
        if formula != DEFAULT_SCORE_FORMULA:
            raise ValueError("unsupported score_formula")
        if formula_version != SCORE_FORMULA_VERSION:
            raise ValueError("unsupported formula_version")
        object.__setattr__(self, "score_formula", formula)
        object.__setattr__(self, "formula_version", formula_version)
        minimum = self.minimum_evidence if isinstance(self.minimum_evidence, Mapping) else {}
        object.__setattr__(self, "minimum_evidence", _freeze(minimum))
        if self.config_hash is None or self.config_hash == "":
            object.__setattr__(self, "config_hash", _hash(self._config_projection()))
        else:
            object.__setattr__(self, "config_hash", _text(self.config_hash, "config_hash", max_length=256))

    # Common aliases used by storage/admission callers.
    @property
    def policy_version(self) -> str:
        return self.version

    @property
    def max_k(self) -> int:
        return self.max_members

    @property
    def k(self) -> int:
        return self.max_members

    @property
    def review_daily(self) -> bool:
        return self.review_interval_days == 1

    def _config_projection(self) -> dict[str, Any]:
        return {
            "policy_id": self.policy_id,
            "version": self.version,
            "policy_version": self.version,
            "global_budget": self.global_budget,
            "min_score": self.min_score,
            "requested_window_days": self.requested_window_days,
            "review_interval_days": self.review_interval_days,
            "max_members": self.max_members,
            "global_budget": self.global_budget,
            "minimum_evidence": {
                "min_actual_coverage_seconds": self.min_actual_coverage_seconds,
                "minimum_coverage_ratio": self.minimum_coverage_ratio,
                "min_completed_outcomes": self.min_completed_outcomes,
                "min_reliability": self.min_reliability,
            },
            "weights": {
                "net_return": self.net_return_weight,
                "drawdown_penalty": self.drawdown_penalty_weight,
                "execution_cost": self.execution_cost_weight,
                "reliability": self.reliability_weight,
                "overlap_penalty": self.overlap_penalty_weight,
            },
            "replacement_margin": self.replacement_margin,
            "cooldown_seconds": self.cooldown_seconds,
            "experimental_allocation_enabled": self.experimental_allocation_enabled,
            "score_formula": self.score_formula,
            "formula_version": self.formula_version,
            "minimum_evidence_overrides": self.minimum_evidence,
        }

    def as_dict(self) -> dict[str, Any]:
        config = self._config_projection()
        result = {
            **config,
            "config_hash": self.config_hash,
            "policy_version": self.version,
            "windows_days": self.requested_window_days,
            "max_k": self.max_members,
            "minimum_evidence": {
                **_plain(self.minimum_evidence),
                "min_actual_coverage_seconds": self.min_actual_coverage_seconds,
                "minimum_coverage_ratio": self.minimum_coverage_ratio,
                "min_completed_outcomes": self.min_completed_outcomes,
                "min_reliability": self.min_reliability,
            },
            "score_formula_config": {
                "version": self.formula_version,
                "expression": self.score_formula,
                "weights": {
                    "net_return": self.net_return_weight,
                    "drawdown_penalty": self.drawdown_penalty_weight,
                    "execution_cost": self.execution_cost_weight,
                    "reliability": self.reliability_weight,
                    "overlap_penalty": self.overlap_penalty_weight,
                },
            },
        }
        return _plain(result)

    to_dict = as_dict
    serialize = as_dict
    @property
    def requested_windows_days(self) -> tuple[int, ...]:
        return self.requested_window_days

    @property
    def window_days(self) -> tuple[int, ...]:
        return self.requested_window_days

    @property
    def review_interval(self) -> timedelta:
        return timedelta(days=self.review_interval_days)

    @property
    def minimum_score(self) -> Decimal:
        return self.min_score

    @property
    def minimum_evidence_seconds(self) -> Decimal:
        return self.min_actual_coverage_seconds

    @property
    def replacement_cooldown_seconds(self) -> int:
        return self.cooldown_seconds

    @property
    def experimental_enabled(self) -> bool:
        return self.experimental_allocation_enabled

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "RollingAdmissionPolicy":
        row = _mapping(value, "admission policy")
        nested_raw = row.get("policy")
        nested_object = nested_raw if isinstance(nested_raw, cls) else None
        if nested_raw is not None and nested_object is None and not isinstance(nested_raw, Mapping):
            raise TypeError("admission policy policy must be a mapping or RollingAdmissionPolicy")
        if nested_raw is not None:
            nested = nested_object.as_dict() if nested_object is not None else dict(nested_raw)

            def first(source: Mapping[str, Any], *names: str) -> Any:
                for name in names:
                    if name in source and source[name] is not None:
                        return source[name]
                return None

            identity = (
                ("policy_id", ("policy_id", "id")),
                ("version", ("version", "policy_version")),
                ("config_hash", ("config_hash",)),
            )
            for label, names in identity:
                outer = first(row, *names)
                inner = first(nested, *names)
                if outer is not None and inner is not None and str(outer).strip() != str(inner).strip():
                    raise ValueError(f"admission policy {label} conflicts with nested policy")

            def component(source: Mapping[str, Any], name: str) -> Any:
                minimum = source.get("minimum_evidence")
                minimum = minimum if isinstance(minimum, Mapping) else {}
                weights = source.get("weights")
                weights = weights if isinstance(weights, Mapping) else {}
                formula = source.get("score_formula_config")
                formula = formula if isinstance(formula, Mapping) else {}
                aliases: dict[str, tuple[Any, ...]] = {
                    "requested_window_days": (
                        first(source, "requested_window_days", "windows_days"),
                    ),
                    "review_interval_days": (
                        first(source, "review_interval_days", "review_days"),
                    ),
                    "max_members": (
                        first(source, "max_members", "max_k", "k"),
                    ),
                    "global_budget": (first(source, "global_budget"),),
                    "min_actual_coverage_seconds": (
                        first(source, "min_actual_coverage_seconds")
                        if first(source, "min_actual_coverage_seconds") is not None
                        else first(minimum, "min_actual_coverage_seconds"),
                    ),
                    "minimum_coverage_ratio": (
                        first(source, "minimum_coverage_ratio")
                        if first(source, "minimum_coverage_ratio") is not None
                        else first(minimum, "minimum_coverage_ratio"),
                    ),
                    "min_completed_outcomes": (
                        first(source, "min_completed_outcomes")
                        if first(source, "min_completed_outcomes") is not None
                        else first(minimum, "min_completed_outcomes"),
                    ),
                    "min_reliability": (
                        first(source, "min_reliability")
                        if first(source, "min_reliability") is not None
                        else first(minimum, "min_reliability"),
                    ),
                    "min_score": (first(source, "min_score", "minimum_score"),),
                    "net_return_weight": (
                        first(source, "net_return_weight")
                        if first(source, "net_return_weight") is not None
                        else first(weights, "net_return"),
                    ),
                    "drawdown_penalty_weight": (
                        first(source, "drawdown_penalty_weight")
                        if first(source, "drawdown_penalty_weight") is not None
                        else first(weights, "drawdown_penalty"),
                    ),
                    "execution_cost_weight": (
                        first(source, "execution_cost_weight")
                        if first(source, "execution_cost_weight") is not None
                        else first(weights, "execution_cost"),
                    ),
                    "reliability_weight": (
                        first(source, "reliability_weight")
                        if first(source, "reliability_weight") is not None
                        else first(weights, "reliability"),
                    ),
                    "overlap_penalty_weight": (
                        first(source, "overlap_penalty_weight")
                        if first(source, "overlap_penalty_weight") is not None
                        else first(weights, "overlap_penalty"),
                    ),
                    "replacement_margin": (first(source, "replacement_margin"),),
                    "cooldown_seconds": (first(source, "cooldown_seconds"),),
                    "experimental_allocation_enabled": (
                        first(source, "experimental_allocation_enabled", "experimental_enabled"),
                    ),
                    "score_formula": (
                        first(source, "score_formula")
                        if first(source, "score_formula") is not None
                        else first(formula, "expression"),
                    ),
                    "formula_version": (
                        first(source, "formula_version")
                        if first(source, "formula_version") is not None
                        else first(formula, "version"),
                    ),
                    "minimum_evidence_overrides": (
                        first(source, "minimum_evidence_overrides"),
                    ),
                }
                return aliases[name][0]

            component_names = (
                "requested_window_days",
                "review_interval_days",
                "max_members",
                "global_budget",
                "min_actual_coverage_seconds",
                "minimum_coverage_ratio",
                "min_completed_outcomes",
                "min_reliability",
                "min_score",
                "net_return_weight",
                "drawdown_penalty_weight",
                "execution_cost_weight",
                "reliability_weight",
                "overlap_penalty_weight",
                "replacement_margin",
                "cooldown_seconds",
                "experimental_allocation_enabled",
                "score_formula",
                "formula_version",
                "minimum_evidence_overrides",
            )
            for name in component_names:
                outer = component(row, name)
                inner = component(nested, name)
                if outer is not None and inner is not None and _canonical(outer) != _canonical(inner):
                    raise ValueError(f"admission policy {name} conflicts with nested policy")

            if nested_object is not None:
                return nested_object
            merged = dict(nested)
            for key, item in row.items():
                if key not in {"policy", "created_at"} and key not in merged:
                    merged[key] = item
            for target, names, selected in (
                ("policy_id", ("policy_id", "id"), first(row, "policy_id", "id")),
                ("version", ("version", "policy_version"), first(row, "version", "policy_version")),
                ("config_hash", ("config_hash",), first(row, "config_hash")),
            ):
                if selected is not None and first(merged, *names) is None:
                    merged[target] = selected
            required_nested = (
                "requested_window_days",
                "max_members",
                "global_budget",
                "min_actual_coverage_seconds",
                "minimum_coverage_ratio",
                "min_completed_outcomes",
                "min_reliability",
                "min_score",
            )
            for name in required_nested:
                if component(merged, name) is None:
                    raise ValueError(f"nested admission policy missing {name}")
            row = merged
        minimum = row.get("minimum_evidence", {})
        minimum = minimum if isinstance(minimum, Mapping) else {}
        weights = row.get("weights", {})
        weights = weights if isinstance(weights, Mapping) else {}
        windows = row.get("requested_window_days", row.get("windows_days", (7, 30)))
        if isinstance(windows, int):
            windows = (windows,)
        return cls(
            policy_id=row.get("policy_id"),
            version=row.get("version", row.get("policy_version", POLICY_VERSION)),
            config_hash=row.get("config_hash"),
            requested_window_days=tuple(windows),
            review_interval_days=row.get("review_interval_days", row.get("review_days", 1)),
            max_members=row.get("max_members", row.get("max_k", row.get("k", 5))),
            global_budget=row.get("global_budget", ZERO),
            min_actual_coverage_seconds=row.get("min_actual_coverage_seconds", minimum.get("min_actual_coverage_seconds", ZERO)),
            minimum_coverage_ratio=row.get("minimum_coverage_ratio", minimum.get("minimum_coverage_ratio", Decimal("0.80"))),
            min_completed_outcomes=row.get("min_completed_outcomes", minimum.get("min_completed_outcomes", 5)),
            min_reliability=row.get("min_reliability", minimum.get("min_reliability", Decimal("0.50"))),
            min_score=row.get("min_score", row.get("minimum_score", ZERO)),
            net_return_weight=row.get("net_return_weight", weights.get("net_return", Decimal("1"))),
            drawdown_penalty_weight=row.get("drawdown_penalty_weight", weights.get("drawdown_penalty", Decimal("1"))),
            execution_cost_weight=row.get("execution_cost_weight", weights.get("execution_cost", Decimal("1"))),
            reliability_weight=row.get("reliability_weight", weights.get("reliability", Decimal("0.25"))),
            overlap_penalty_weight=row.get("overlap_penalty_weight", weights.get("overlap_penalty", Decimal("0.10"))),
            replacement_margin=row.get("replacement_margin", Decimal("0.05")),
            cooldown_seconds=row.get("cooldown_seconds", 86400),
            experimental_allocation_enabled=row.get("experimental_allocation_enabled", row.get("experimental_enabled", False)),
            score_formula=row.get("score_formula", row.get("score_formula_config", {}).get("expression", DEFAULT_SCORE_FORMULA) if isinstance(row.get("score_formula_config", {}), Mapping) else DEFAULT_SCORE_FORMULA),
            formula_version=row.get("formula_version", row.get("score_formula_config", {}).get("version", SCORE_FORMULA_VERSION) if isinstance(row.get("score_formula_config", {}), Mapping) else SCORE_FORMULA_VERSION),
            minimum_evidence=row.get("minimum_evidence_overrides", row.get("minimum_evidence", {})),
        )


def default_rolling_admission_policy() -> RollingAdmissionPolicy:
    """Return the conservative, explicitly paper-only default policy."""
    return RollingAdmissionPolicy(policy_id="rolling-default", version=POLICY_VERSION)


@dataclass(frozen=True, slots=True)
class RollingEvidence:
    """One exact, immutable evidence window for one strategy version."""
    strategy_version_id: str
    evidence_window_id: str
    candidate_id: str | None = None
    research_trial_id: str | None = None
    available_from: datetime | None = None
    available_through: datetime | None = None
    requested_days: int = 7
    actual_coverage_seconds: Decimal = ZERO
    observation_completeness: Decimal = ONE
    source_class: str = ""
    fee_assumption: Decimal = ZERO
    slippage_assumption: Decimal = ZERO
    allocated_capital_net_return: Decimal = ZERO
    realized_pnl: Decimal = ZERO
    unrealized_pnl: Decimal = ZERO
    fees: Decimal = ZERO
    costs: Decimal = ZERO
    drawdown: Decimal = ZERO
    completed_outcomes: int = 0
    reliability: Decimal = ZERO
    execution_feasibility: bool | str | None = None
    evidence_digest: str = ""
    overlap_key: str | None = None
    hard_failure: bool = False
    failure_reason: str = ""
    allocated_capital: Decimal | None = None
    paper_size: Decimal | None = None
    paper_sizing: Decimal | None = None
    fee_costs: Decimal | None = None
    slippage_costs: Decimal | None = None
    def __post_init__(self) -> None:
        object.__setattr__(self, "strategy_version_id", _text(self.strategy_version_id, "strategy_version_id"))
        object.__setattr__(self, "evidence_window_id", _text(self.evidence_window_id, "evidence_window_id"))
        object.__setattr__(self, "candidate_id", _text(self.candidate_id, "candidate_id", required=False) or None)
        object.__setattr__(self, "research_trial_id", _text(self.research_trial_id, "research_trial_id", required=False) or None)
        start = _utc(self.available_from, "available_from")
        through = _utc(self.available_through, "available_through")
        object.__setattr__(self, "available_from", start)
        object.__setattr__(self, "available_through", through)
        if start is not None and through is not None and through < start:
            raise ValueError("available_through must not precede available_from")
        requested_days = _integer(self.requested_days, "requested_days", minimum=1, maximum=30)
        if requested_days not in _SUPPORTED_WINDOW_DAYS:
            raise ValueError("requested_days must be 7 or 30")
        object.__setattr__(self, "requested_days", requested_days)
        completeness = _ratio(self.observation_completeness, "observation_completeness", default=ONE)
        object.__setattr__(self, "observation_completeness", completeness)
        coverage = _nonnegative(self.actual_coverage_seconds, "actual_coverage_seconds", default=ZERO)
        if start is not None and through is not None:
            span = through - start
            boundary = (
                Decimal(span.days) * SECONDS_PER_DAY
                + Decimal(span.seconds)
                + Decimal(span.microseconds) / Decimal("1000000")
            )
            if coverage > boundary:
                raise ValueError("actual_coverage_seconds must not exceed available range")
        object.__setattr__(self, "actual_coverage_seconds", coverage)
        object.__setattr__(self, "source_class", _source_class(self.source_class))
        sizing = self.paper_sizing
        if sizing is None:
            sizing = self.paper_size
        if sizing is None:
            sizing = self.allocated_capital
        sizing = _nonnegative(sizing, "paper_sizing", default=ZERO)
        object.__setattr__(self, "paper_sizing", sizing)
        object.__setattr__(self, "allocated_capital", sizing)
        object.__setattr__(self, "paper_size", sizing)
        for name in ("fee_assumption", "slippage_assumption"):
            parsed = _nonnegative(getattr(self, name), name, default=ZERO)
            object.__setattr__(self, name, parsed)
        fees = _nonnegative(self.fees, "fees", default=ZERO)
        if self.fee_costs is not None:
            fees += _nonnegative(self.fee_costs, "fee_costs")
        costs = _nonnegative(self.costs, "costs", default=ZERO)
        if self.slippage_costs is not None:
            costs += _nonnegative(self.slippage_costs, "slippage_costs")
        object.__setattr__(self, "fees", fees)
        object.__setattr__(self, "costs", costs)
        object.__setattr__(self, "fee_costs", fees)
        object.__setattr__(self, "slippage_costs", costs)
        object.__setattr__(self, "allocated_capital_net_return", _decimal(self.allocated_capital_net_return, "allocated_capital_net_return", default=ZERO))
        object.__setattr__(self, "realized_pnl", _decimal(self.realized_pnl, "realized_pnl", default=ZERO))
        object.__setattr__(self, "unrealized_pnl", _decimal(self.unrealized_pnl, "unrealized_pnl", default=ZERO))
        drawdown = _ratio(self.drawdown, "drawdown", default=ZERO)
        object.__setattr__(self, "drawdown", drawdown)
        object.__setattr__(self, "completed_outcomes", _integer(self.completed_outcomes, "completed_outcomes", minimum=0))
        object.__setattr__(self, "reliability", _ratio(self.reliability, "reliability", default=ZERO))
        feasibility = self.execution_feasibility
        if isinstance(feasibility, str):
            feasibility = feasibility.strip().upper()
        elif feasibility is not None and not isinstance(feasibility, bool):
            raise TypeError("execution_feasibility must be bool, string, or None")
        object.__setattr__(self, "execution_feasibility", feasibility)
        object.__setattr__(self, "overlap_key", _text(self.overlap_key, "overlap_key", required=False) or _UNKNOWN_OVERLAP_KEY)
        if not isinstance(self.hard_failure, bool):
            raise TypeError("hard_failure must be a bool")
        object.__setattr__(self, "hard_failure", self.hard_failure)
        object.__setattr__(self, "failure_reason", _text(self.failure_reason, "failure_reason", max_length=MAX_REASON_LENGTH, required=False))
        # Evidence identity is always derived from immutable content.  A caller
        # supplied digest is metadata only; never allow it to override the
        # canonical digest used for selection and persistence.
        _text(self.evidence_digest, "evidence_digest", max_length=256, required=False)
        object.__setattr__(self, "evidence_digest", _hash(self._digest_projection()))

    def _digest_projection(self) -> dict[str, Any]:
        return {
            "candidate_id": self.candidate_id,
            "research_trial_id": self.research_trial_id,
            "strategy_version_id": self.strategy_version_id,
            "evidence_window_id": self.evidence_window_id,
            "available_from": self.available_from,
            "available_through": self.available_through,
            "requested_days": self.requested_days,
            "actual_coverage_seconds": self.actual_coverage_seconds,
            "observation_completeness": self.observation_completeness,
            "source_class": self.source_class,
            "paper_sizing": self.paper_sizing,
            "fee_assumption": self.fee_assumption,
            "slippage_assumption": self.slippage_assumption,
            "allocated_capital_net_return": self.allocated_capital_net_return,
            "realized_pnl": self.realized_pnl,
            "unrealized_pnl": self.unrealized_pnl,
            "fees": self.fees,
            "costs": self.costs,
            "drawdown": self.drawdown,
            "completed_outcomes": self.completed_outcomes,
            "reliability": self.reliability,
            "execution_feasibility": self.execution_feasibility,
            "overlap_key": self.overlap_key,
        }

    @property
    def execution_cost(self) -> Decimal:
        return self.fees + self.costs + self.allocated_capital * (self.fee_assumption + self.slippage_assumption)

    @property
    def execution_cost_rate(self) -> Decimal:
        if self.allocated_capital <= ZERO:
            return ONE
        return self.execution_cost / self.allocated_capital

    @property
    def net_return_rate(self) -> Decimal:
        if self.allocated_capital <= ZERO:
            return ZERO
        return self.allocated_capital_net_return / self.allocated_capital

    @property
    def is_hard_failure(self) -> bool:
        feasibility = self.execution_feasibility
        return self.hard_failure or (
            isinstance(feasibility, str) and feasibility.upper() in _HARD_FAILURE_VALUES
        ) or feasibility is False

    @property
    def requested_window_days(self) -> int:
        return self.requested_days

    @property
    def actual_coverage(self) -> Decimal:
        return self.actual_coverage_seconds

    @property
    def net_return(self) -> Decimal:
        return self.allocated_capital_net_return

    @property
    def execution_costs(self) -> Decimal:
        return self.execution_cost

    @property
    def available_to(self) -> datetime | None:
        return self.available_through
    def minimum_evidence_failures(self, policy: RollingAdmissionPolicy) -> tuple[str, ...]:
        required_seconds = max(
            policy.min_actual_coverage_seconds,
            Decimal(self.requested_days) * SECONDS_PER_DAY * policy.minimum_coverage_ratio,
        )
        failures: list[str] = []
        if self.actual_coverage_seconds < required_seconds:
            failures.append("coverage_seconds")
        if self.observation_completeness < policy.minimum_coverage_ratio:
            failures.append("observation_completeness")
        if self.completed_outcomes < policy.min_completed_outcomes:
            failures.append("completed_outcomes")
        if self.reliability < policy.min_reliability:
            failures.append("reliability")
        if self.allocated_capital <= ZERO:
            failures.append("allocated_capital")
        if not self.source_class:
            failures.append("source_class")
        if self.available_from is None or self.available_through is None:
            failures.append("available_range")
        return tuple(failures)

    def score(self, policy: RollingAdmissionPolicy, *, overlap_penalty: Decimal = ZERO) -> Decimal:
        penalty = _nonnegative(overlap_penalty, "overlap_penalty", default=ZERO)
        # ``allocated_capital_net_return`` is already net of the persisted
        # fee/slippage costs.  Keep execution_cost_rate available for
        # reporting, but do not subtract it a second time from the score.
        return (
            self.net_return_rate * policy.net_return_weight
            - self.drawdown * policy.drawdown_penalty_weight
            + self.reliability * policy.reliability_weight
            - penalty * policy.overlap_penalty_weight
        )

    def as_dict(self) -> dict[str, Any]:
        return _plain({
            "candidate_id": self.candidate_id,
            "research_trial_id": self.research_trial_id,
            "strategy_version_id": self.strategy_version_id,
            "evidence_window_id": self.evidence_window_id,
            "available_from": self.available_from,
            "available_through": self.available_through,
            "requested_days": self.requested_days,
            "actual_coverage_seconds": self.actual_coverage_seconds,
            "observation_completeness": self.observation_completeness,
            "source_class": self.source_class,
            "paper_sizing": self.paper_sizing,
            "paper_size": self.paper_size,
            "allocated_capital": self.allocated_capital,
            "fee_assumption": self.fee_assumption,
            "slippage_assumption": self.slippage_assumption,
            "allocated_capital_net_return": self.allocated_capital_net_return,
            "realized_pnl": self.realized_pnl,
            "unrealized_pnl": self.unrealized_pnl,
            "fees": self.fees,
            "costs": self.costs,
            "drawdown": self.drawdown,
            "completed_outcomes": self.completed_outcomes,
            "reliability": self.reliability,
            "execution_feasibility": self.execution_feasibility,
            "hard_failure": self.hard_failure,
            "failure_reason": self.failure_reason,
            "evidence_digest": self.evidence_digest,
            "overlap_key": self.overlap_key,
        })

    to_dict = as_dict
    serialize = as_dict

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "RollingEvidence":
        row = _mapping(value, "rolling evidence")

        def assumption_value(raw: Any, *names: str) -> Any:
            if not isinstance(raw, Mapping):
                return None
            for name in names:
                candidate = raw.get(name)
                if candidate is not None:
                    return candidate
            return None

        def assumption_mapping(*names: str) -> Mapping[str, Any] | None:
            for name in names:
                candidate = row.get(name)
                if isinstance(candidate, Mapping):
                    return candidate
            return None

        sizing_assumptions = assumption_mapping("paper_sizing_assumptions", "paper_sizing")
        fee_assumptions = assumption_mapping("paper_fee_assumptions", "paper_fees", "fee_assumptions")
        slippage_assumptions = assumption_mapping(
            "paper_slippage_assumptions",
            "paper_slippage",
            "slippage_assumptions",
        )

        paper_sizing = row.get("paper_sizing", row.get("paper_size", row.get("allocated_capital", 0)))
        nested_sizing = assumption_value(
            sizing_assumptions,
            "paper_sizing",
            "paper_size",
            "allocated_capital",
            "size",
            "value",
        )
        if nested_sizing is not None:
            paper_sizing = nested_sizing
        allocated_capital = None if nested_sizing is not None else row.get("allocated_capital")
        paper_size = None if nested_sizing is not None else row.get("paper_size")
        fee_assumption = row.get("fee_assumption", row.get("fee_rate", 0))
        nested_fee = assumption_value(
            fee_assumptions,
            "fee_assumption",
            "fee_rate",
            "rate",
            "value",
        )
        if nested_fee is not None:
            fee_assumption = nested_fee
        slippage_assumption = row.get("slippage_assumption", row.get("slippage_rate", 0))
        nested_slippage = assumption_value(
            slippage_assumptions,
            "slippage_assumption",
            "slippage_rate",
            "rate",
            "value",
        )
        if nested_slippage is not None:
            slippage_assumption = nested_slippage
        costs = row.get("costs")
        if costs is None:
            costs = row.get("execution_costs", 0)
        slippage_costs = row.get("slippage_costs")
        return cls(
            strategy_version_id=row.get("strategy_version_id"),
            evidence_window_id=row.get("evidence_window_id", row.get("id")),
            candidate_id=row.get("candidate_id"),
            research_trial_id=row.get("research_trial_id", row.get("trial_id")),
            available_from=row.get("available_from"),
            available_through=row.get("available_through"),
            requested_days=row.get("requested_days", row.get("requested_window_days", 7)),
            actual_coverage_seconds=row.get("actual_coverage_seconds", 0),
            observation_completeness=row.get("observation_completeness", row.get("completeness", 1)),
            source_class=row.get("source_class", ""),
            paper_sizing=paper_sizing,
            fee_assumption=fee_assumption,
            slippage_assumption=slippage_assumption,
            allocated_capital_net_return=row.get("allocated_capital_net_return", row.get("net_return", 0)),
            realized_pnl=row.get("realized_pnl", 0),
            unrealized_pnl=row.get("unrealized_pnl", 0),
            fees=row.get("fees", 0),
            costs=costs,
            drawdown=row.get("drawdown", 0),
            completed_outcomes=row.get("completed_outcomes", row.get("outcomes", 0)),
            reliability=row.get("reliability", 0),
            execution_feasibility=row.get("execution_feasibility", row.get("execution_feasible")),
            evidence_digest=row.get("evidence_digest", row.get("digest", "")),
            overlap_key=row.get("overlap_key"),
            hard_failure=row.get("hard_failure", False),
            failure_reason=row.get("failure_reason", row.get("reason", "")),
            allocated_capital=allocated_capital,
            paper_size=paper_size,
            fee_costs=row.get("fee_costs"),
            slippage_costs=slippage_costs,
        )


StrategyEvidenceWindow = RollingEvidence


@dataclass(frozen=True, slots=True)
class RollingSelectionMember:
    """Immutable member row; position state is carried across removals."""

    strategy_version_id: str
    candidate_id: str | None = None
    research_trial_id: str | None = None
    allocation: Decimal = ZERO
    status: str = "OBSERVE"
    action: str = "HOLD"
    score: Decimal = ZERO
    reason: str = ""
    evidence_window_id: str | None = None
    overlap_key: str | None = None
    evidence_digest: str | None = None
    position_management_state: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "strategy_version_id", _text(self.strategy_version_id, "strategy_version_id"))
        object.__setattr__(self, "candidate_id", _text(self.candidate_id, "candidate_id", required=False) or None)
        object.__setattr__(self, "research_trial_id", _text(self.research_trial_id, "research_trial_id", required=False) or None)
        object.__setattr__(self, "allocation", _nonnegative(self.allocation, "allocation", default=ZERO))
        object.__setattr__(self, "status", _text(self.status, "status", max_length=32).upper())
        action = _text(self.action, "action", max_length=16, required=False).upper() or "HOLD"
        if action not in _MEMBER_ACTIONS:
            raise ValueError(f"unsupported member action: {action}")
        object.__setattr__(self, "action", action)
        object.__setattr__(self, "score", _decimal(self.score, "score", default=ZERO))
        object.__setattr__(self, "reason", _text(self.reason, "reason", max_length=MAX_REASON_LENGTH, required=False))
        object.__setattr__(self, "evidence_window_id", _text(self.evidence_window_id, "evidence_window_id", required=False) or None)
        object.__setattr__(self, "overlap_key", _text(self.overlap_key, "overlap_key", required=False) or _UNKNOWN_OVERLAP_KEY)
        digest = _text(self.evidence_digest, "evidence_digest", max_length=256, required=False)
        object.__setattr__(self, "evidence_digest", digest or None)
        state = self.position_management_state if isinstance(self.position_management_state, Mapping) else {}
        object.__setattr__(self, "position_management_state", _freeze(state))

    @property
    def position_state(self) -> Mapping[str, Any]:
        return self.position_management_state

    def with_status(
        self,
        status: str,
        reason: str,
        *,
        allocation: Decimal | None = None,
        action: str | None = None,
    ) -> "RollingSelectionMember":
        return RollingSelectionMember(
            strategy_version_id=self.strategy_version_id,
            candidate_id=self.candidate_id,
            research_trial_id=self.research_trial_id,
            allocation=self.allocation if allocation is None else allocation,
            status=status,
            action=action or ("REDUCE" if str(status).upper() in {"REMOVED", "PAUSED"} else self.action),
            score=self.score,
            reason=reason,
            evidence_window_id=self.evidence_window_id,
            overlap_key=self.overlap_key,
            evidence_digest=self.evidence_digest,
            position_management_state=self.position_management_state,
        )

    def as_dict(self) -> dict[str, Any]:
        return _plain({
            "strategy_version_id": self.strategy_version_id,
            "candidate_id": self.candidate_id,
            "research_trial_id": self.research_trial_id,
            "allocation": self.allocation,
            "status": self.status,
            "action": self.action,
            "score": self.score,
            "reason": self.reason,
            "evidence_window_id": self.evidence_window_id,
            "overlap_key": self.overlap_key,
            "evidence_digest": self.evidence_digest,
            "position_management_state": self.position_management_state,
            "position_state": self.position_management_state,
        })

    to_dict = as_dict
    serialize = as_dict

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "RollingSelectionMember":
        row = _mapping(value, "selection member")
        return cls(
            strategy_version_id=row.get("strategy_version_id"),
            candidate_id=row.get("candidate_id"),
            research_trial_id=row.get("research_trial_id", row.get("trial_id")),
            allocation=row.get("allocation", 0),
            status=row.get("status", "OBSERVE"),
            action=row.get("action", "HOLD"),
            score=row.get("score", 0),
            reason=row.get("reason", ""),
            evidence_window_id=row.get("evidence_window_id"),
            overlap_key=row.get("overlap_key"),
            evidence_digest=row.get("evidence_digest"),
            position_management_state=row.get("position_management_state", row.get("position_state", {})),
        )



PortfolioSelectionMember = RollingSelectionMember


@dataclass(frozen=True, slots=True)
class RollingSelection:
    """Append-only selection identity and risk binding."""

    portfolio_selection_id: str
    policy_id: str = ""
    policy_version: str = ""
    active_risk_config_id: str = ""
    active_risk_config_generation: int = 0
    active_risk_config_hash: str = ""
    selected_at: datetime | None = None
    review_due_at: datetime | None = None
    global_budget: Decimal = ZERO
    members: tuple[RollingSelectionMember, ...] = ()
    last_membership_change_at: datetime | None = None
    removed_member_evidence_history: Mapping[str, Sequence[str]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "portfolio_selection_id", _text(self.portfolio_selection_id, "portfolio_selection_id"))
        object.__setattr__(self, "policy_id", _text(self.policy_id, "policy_id", required=False))
        object.__setattr__(self, "policy_version", _text(self.policy_version, "policy_version", required=False))
        object.__setattr__(self, "active_risk_config_id", _text(self.active_risk_config_id, "active_risk_config_id", required=False))
        object.__setattr__(self, "active_risk_config_generation", _integer(self.active_risk_config_generation, "active_risk_config_generation", minimum=0))
        object.__setattr__(self, "active_risk_config_hash", _text(self.active_risk_config_hash, "active_risk_config_hash", max_length=256, required=False))
        object.__setattr__(self, "selected_at", _utc(self.selected_at, "selected_at"))
        object.__setattr__(
            self,
            "last_membership_change_at",
            _utc(self.last_membership_change_at, "last_membership_change_at") or self.selected_at,
        )
        due = _utc(self.review_due_at, "review_due_at")
        if self.selected_at is not None and due is not None and due < self.selected_at:
            raise ValueError("review_due_at must not precede selected_at")
        object.__setattr__(self, "review_due_at", due)
        budget = _nonnegative(self.global_budget, "global_budget", default=ZERO)
        object.__setattr__(self, "global_budget", budget)
        normalized: list[RollingSelectionMember] = []
        seen: set[str] = set()
        for item in self.members:
            member = item if isinstance(item, RollingSelectionMember) else RollingSelectionMember.from_mapping(item)
            if member.strategy_version_id in seen:
                raise ValueError("selection members must have unique strategy_version_id")
            seen.add(member.strategy_version_id)
            normalized.append(member)
        if len(normalized) > MAX_K:
            raise ValueError(f"selection cannot contain more than {MAX_K} members")
        if sum((member.allocation for member in normalized), ZERO) > budget:
            raise ValueError("member allocations exceed global_budget")
        history = _bounded_removed_member_evidence_history(
            self.removed_member_evidence_history
        )
        object.__setattr__(self, "removed_member_evidence_history", _freeze(history))
        object.__setattr__(self, "members", tuple(normalized))

    @property
    def selection_id(self) -> str:
        return self.portfolio_selection_id

    @property
    def risk_config_id(self) -> str:
        return self.active_risk_config_id

    @property
    def risk_config_generation(self) -> int:
        return self.active_risk_config_generation

    @property
    def risk_config_hash(self) -> str:
        return self.active_risk_config_hash

    def as_dict(self) -> dict[str, Any]:
        return _plain({
            "portfolio_selection_id": self.portfolio_selection_id,
            "selection_id": self.portfolio_selection_id,
            "policy_id": self.policy_id,
            "policy_version": self.policy_version,
            "active_risk_config_id": self.active_risk_config_id,
            "active_risk_config_generation": self.active_risk_config_generation,
            "active_risk_config_hash": self.active_risk_config_hash,
            "selected_at": self.selected_at,
            "review_due_at": self.review_due_at,
            "global_budget": self.global_budget,
            "removed_member_evidence_history": self.removed_member_evidence_history,
            "last_membership_change_at": self.last_membership_change_at,
            "members": self.members,
        })

    to_dict = as_dict
    serialize = as_dict

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "RollingSelection":
        row = _mapping(value, "portfolio selection")
        members = row.get("members", row.get("selected_members", ()))
        return cls(
            portfolio_selection_id=row.get("portfolio_selection_id", row.get("selection_id", row.get("id"))),
            policy_id=row.get("policy_id", ""),
            policy_version=row.get("policy_version", row.get("version", "")),
            active_risk_config_id=row.get("active_risk_config_id", row.get("risk_config_id", "")),
            active_risk_config_generation=row.get("active_risk_config_generation", row.get("risk_config_generation", 0)),
            active_risk_config_hash=row.get("active_risk_config_hash", row.get("risk_config_hash", "")),
            selected_at=row.get("selected_at"),
            review_due_at=row.get("review_due_at"),
            global_budget=row.get("global_budget", 0),
            last_membership_change_at=row.get("last_membership_change_at", row.get("selected_at")),
            removed_member_evidence_history=row.get("removed_member_evidence_history", {}),
            members=tuple(item if isinstance(item, RollingSelectionMember) else RollingSelectionMember.from_mapping(item) for item in members),
        )


PortfolioSelection = RollingSelection


@dataclass(frozen=True, slots=True)
class RollingSelectionDecision:
    """Pure evaluation output with complete policy and formula provenance."""

    status: str
    members: tuple[RollingSelectionMember, ...] = ()
    reasons: tuple[str, ...] = ()
    selected_at: datetime | None = None
    review_due_at: datetime | None = None
    policy_id: str = ""
    policy_version: str = ""
    config_hash: str = ""
    score_formula: str = DEFAULT_SCORE_FORMULA
    formula_version: str = SCORE_FORMULA_VERSION
    policy_config: Mapping[str, Any] = field(default_factory=dict)
    global_budget: Decimal = ZERO
    removed_members: tuple[RollingSelectionMember, ...] = ()
    last_membership_change_at: datetime | None = None
    removed_member_evidence_history: Mapping[str, Sequence[str]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "status", _text(self.status, "status", max_length=32).upper())
        normalized_members = tuple(item if isinstance(item, RollingSelectionMember) else RollingSelectionMember.from_mapping(item) for item in self.members)
        normalized_removed = tuple(item if isinstance(item, RollingSelectionMember) else RollingSelectionMember.from_mapping(item) for item in self.removed_members)
        if len(normalized_members) > MAX_K:
            raise ValueError(f"decision cannot contain more than {MAX_K} members")
        ids = [member.strategy_version_id for member in normalized_members]
        if len(ids) != len(set(ids)):
            raise ValueError("decision members must have unique strategy_version_id")
        object.__setattr__(self, "members", normalized_members)
        object.__setattr__(self, "removed_members", normalized_removed)
        object.__setattr__(self, "reasons", tuple(_text(reason, "reason", max_length=MAX_REASON_LENGTH) for reason in self.reasons))
        object.__setattr__(self, "selected_at", _utc(self.selected_at, "selected_at"))
        object.__setattr__(
            self,
            "last_membership_change_at",
            _utc(self.last_membership_change_at, "last_membership_change_at") or self.selected_at,
        )
        object.__setattr__(self, "review_due_at", _utc(self.review_due_at, "review_due_at"))
        object.__setattr__(self, "policy_id", _text(self.policy_id, "policy_id", required=False))
        object.__setattr__(self, "policy_version", _text(self.policy_version, "policy_version", required=False))
        object.__setattr__(self, "config_hash", _text(self.config_hash, "config_hash", max_length=256, required=False))
        object.__setattr__(self, "score_formula", _text(self.score_formula, "score_formula", max_length=2048))
        object.__setattr__(self, "formula_version", _text(self.formula_version, "formula_version", max_length=64))
        object.__setattr__(self, "policy_config", _freeze(self.policy_config if isinstance(self.policy_config, Mapping) else {}))
        budget = _nonnegative(self.global_budget, "global_budget", default=ZERO)
        object.__setattr__(self, "global_budget", budget)
        if sum((member.allocation for member in normalized_members), ZERO) > budget:
            raise ValueError("member allocations exceed global_budget")
        history = _bounded_removed_member_evidence_history(
            self.removed_member_evidence_history
        )
        object.__setattr__(self, "removed_member_evidence_history", _freeze(history))

    @property
    def selection_status(self) -> str:
        return self.status

    @property
    def selected_members(self) -> tuple[RollingSelectionMember, ...]:
        return self.members

    @property
    def formula(self) -> str:
        return self.score_formula

    @property
    def config(self) -> Mapping[str, Any]:
        return self.policy_config

    def as_dict(self) -> dict[str, Any]:
        return _plain({
            "status": self.status,
            "selection_status": self.status,
            "members": self.members,
            "selected_members": self.members,
            "removed_members": self.removed_members,
            "reasons": self.reasons,
            "selected_at": self.selected_at,
            "review_due_at": self.review_due_at,
            "last_membership_change_at": self.last_membership_change_at,
            "policy_id": self.policy_id,
            "policy_version": self.policy_version,
            "config_hash": self.config_hash,
            "score_formula": self.score_formula,
            "formula_version": self.formula_version,
            "policy_config": self.policy_config,
            "removed_member_evidence_history": self.removed_member_evidence_history,
            "global_budget": self.global_budget,
        })

    to_dict = as_dict
    serialize = as_dict


SelectionDecision = RollingSelectionDecision


def _coerce_policy(policy: RollingAdmissionPolicy | Mapping[str, Any]) -> RollingAdmissionPolicy:
    return policy if isinstance(policy, RollingAdmissionPolicy) else RollingAdmissionPolicy.from_mapping(policy)


def _coerce_evidence(evidence: Any) -> tuple[RollingEvidence, ...]:
    if isinstance(evidence, RollingEvidence):
        return (evidence,)
    if isinstance(evidence, Mapping):
        # A single evidence record is distinguishable by its identity fields.  A
        # mapping keyed by strategy id is also accepted by storage adapters.
        if "strategy_version_id" in evidence or "evidence_window_id" in evidence:
            return (RollingEvidence.from_mapping(evidence),)
        values = evidence.values()
    else:
        values = evidence or ()
    result = tuple(item if isinstance(item, RollingEvidence) else RollingEvidence.from_mapping(item) for item in values)
    return result


def _coerce_selection(selection: RollingSelection | RollingSelectionDecision | Mapping[str, Any] | None) -> RollingSelection | None:
    if selection is None:
        return None
    if isinstance(selection, RollingSelection):
        return selection
    if isinstance(selection, RollingSelectionDecision):
        return RollingSelection(
            portfolio_selection_id="decision-current",
            policy_id=selection.policy_id,
            policy_version=selection.policy_version,
            selected_at=selection.selected_at,
            review_due_at=selection.review_due_at,
            global_budget=selection.global_budget,
            last_membership_change_at=selection.last_membership_change_at or selection.selected_at,
            removed_member_evidence_history=selection.removed_member_evidence_history,
            members=selection.members,
        )
    return RollingSelection.from_mapping(selection)


def _membership_allocation_signature(
    members: Sequence[RollingSelectionMember],
) -> tuple[tuple[str, Decimal], ...]:
    return tuple(
        sorted(
            ((member.strategy_version_id, member.allocation) for member in members),
            key=lambda item: item[0],
        )
    )


def _latest_windows(
    evidence: Sequence[RollingEvidence],
) -> dict[str, dict[str, dict[int, RollingEvidence]]]:
    """Keep the newest window per strategy, source class, and duration.

    Source classes are part of the evidence identity.  In particular, a 7-day
    HISTORICAL row must never be paired with a 30-day LIVE row merely because
    they share a strategy version.
    """
    grouped: dict[str, dict[str, dict[int, RollingEvidence]]] = {}
    for item in evidence:
        by_source = grouped.setdefault(item.strategy_version_id, {})
        by_window = by_source.setdefault(item.source_class, {})
        prior = by_window.get(item.requested_days)
        if prior is None or (
            (item.available_through or datetime.min.replace(tzinfo=UTC)),
            item.evidence_digest,
            item.evidence_window_id,
        ) > (
            (prior.available_through or datetime.min.replace(tzinfo=UTC)),
            prior.evidence_digest,
            prior.evidence_window_id,
        ):
            by_window[item.requested_days] = item
    return grouped


def _aggregate_evidence(
    policy: RollingAdmissionPolicy,
    windows: Mapping[str, Mapping[int, RollingEvidence]],
    *,
    overlap_penalty: Decimal = ZERO,
) -> tuple[Decimal, RollingEvidence | None, tuple[str, ...], tuple[RollingEvidence, ...]]:
    # Select exactly one source class.  Prefer a source with every required
    # duration, then choose the newest deterministic source rather than mixing
    # durations from different source classes.
    source_candidates = [
        (str(source), dict(source_windows))
        for source, source_windows in windows.items()
        if isinstance(source_windows, Mapping)
    ]
    if not source_candidates:
        return ZERO, None, ("windows",), ()
    complete_sources = [
        item
        for item in source_candidates
        if all(days in item[1] for days in policy.requested_window_days)
    ]
    candidates = complete_sources or source_candidates

    def source_key(item: tuple[str, Mapping[int, RollingEvidence]]) -> tuple[Any, ...]:
        source, source_windows = item
        selected = tuple(
            source_windows[days]
            for days in policy.requested_window_days
            if days in source_windows
        )
        through = max(
            (row.available_through or datetime.min.replace(tzinfo=UTC))
            for row in selected
        ) if selected else datetime.min.replace(tzinfo=UTC)
        return (len(selected), through, source)

    source, selected_windows = max(candidates, key=source_key)
    selected = tuple(
        selected_windows[days]
        for days in policy.requested_window_days
        if days in selected_windows
    )
    missing = tuple(
        f"window_{days}d"
        for days in policy.requested_window_days
        if days not in selected_windows
    )
    if not selected:
        return ZERO, None, missing or ("windows",), ()
    failures: list[str] = list(missing)
    if missing:
        failures.append("required_windows")
    candidate_ids = {item.candidate_id for item in selected}
    trial_ids = {item.research_trial_id for item in selected}
    if len(candidate_ids) > 1:
        failures.append("candidate_id_mismatch")
    if len(trial_ids) > 1:
        failures.append("research_trial_id_mismatch")
    for item in selected:
        if not item.candidate_id:
            failures.append(f"{item.requested_days}d:candidate_id")
        if not item.research_trial_id:
            failures.append(f"{item.requested_days}d:research_trial_id")
        failures.extend(f"{item.requested_days}d:{failure}" for failure in item.minimum_evidence_failures(policy))
    scores = tuple(item.score(policy, overlap_penalty=overlap_penalty) for item in selected)
    score = sum(scores, ZERO) / Decimal(len(scores))
    anchor = max(
        selected,
        key=lambda item: (
            item.available_through or datetime.min.replace(tzinfo=UTC),
            item.evidence_digest,
            item.evidence_window_id,
        ),
    )
    return score, anchor, tuple(dict.fromkeys(failures)), selected

def _reduced_allocation(allocation: Decimal) -> Decimal:
    """Reduce an existing obligation deterministically without closing it."""
    value = _nonnegative(allocation, "allocation", default=ZERO)
    return value / Decimal("2") if value > ZERO else ZERO


def _incumbent_status(member: RollingSelectionMember) -> str:
    """Return the single status contract for a maintained incumbent row."""
    if member.allocation > ZERO:
        return "REDUCE" if member.status == "REDUCE" else "ACTIVE"
    if member.status == "PAUSED":
        return "PAUSED"
    if member.status == "PAPER":
        return "PAPER"
    return "OBSERVE"




def _member_from_evidence(
    evidence: RollingEvidence,
    *,
    score: Decimal,
    status: str,
    reason: str,
    allocation: Decimal = ZERO,
) -> RollingSelectionMember:
    if not evidence.candidate_id or not evidence.research_trial_id:
        status = "OBSERVE"
        allocation = ZERO
        reason = reason or REASON_LOW_EVIDENCE
    return RollingSelectionMember(
        strategy_version_id=evidence.strategy_version_id,
        candidate_id=evidence.candidate_id,
        research_trial_id=evidence.research_trial_id,
        allocation=allocation,
        status=status,
        action=(
            "REDUCE"
            if status == "REDUCE"
            else (
                "ADD"
                if status in {"ACTIVE", "PAPER"} and allocation > ZERO
                else ("REDUCE" if status in {"REMOVED", "PAUSED"} else "OBSERVE")
            )
        ),
        score=score,
        reason=reason,
        evidence_window_id=evidence.evidence_window_id,
        overlap_key=evidence.overlap_key,
        evidence_digest=evidence.evidence_digest,
        position_management_state={"evidence_digest": evidence.evidence_digest},
    )


def _allocate(members: Sequence[RollingSelectionMember], policy: RollingAdmissionPolicy) -> tuple[RollingSelectionMember, ...]:
    if not members:
        return ()
    budget = policy.global_budget
    reserved_total = sum(
        (member.allocation for member in members if member.allocation > ZERO),
        ZERO,
    )
    if reserved_total > budget:
        raise ValueError("CURRENT_ALLOCATION_EXCEEDS_GLOBAL_BUDGET")
    # Experimental allocation is a deliberate opt-in.  In the default paper-only
    # mode newly selected rows already have zero allocation; existing allocations
    # remain untouched so ordinary-loss retention does not close positions.
    if not policy.experimental_allocation_enabled or budget <= ZERO:
        return tuple(member for member in members)
    # Every existing positive allocation is reserved, including ACTIVE and REDUCE
    # rows carrying an open position.  Only zero-allocation ACTIVE/PAPER rows are
    # new fundable entries; this prevents averaging up incumbents.
    fundable = tuple(
        member
        for member in members
        if member.status in {"ACTIVE", "PAPER"} and member.allocation <= ZERO
    )
    remaining = budget - reserved_total
    allocations: dict[str, Decimal] = {}
    if fundable and remaining > ZERO:
        share = remaining / Decimal(len(fundable))
        running = ZERO
        for index, member in enumerate(fundable):
            allocation = remaining - running if index == len(fundable) - 1 else share
            running += allocation
            allocations[member.strategy_version_id] = allocation
    return tuple(
        RollingSelectionMember(
            strategy_version_id=member.strategy_version_id,
            candidate_id=member.candidate_id,
            research_trial_id=member.research_trial_id,
            allocation=allocations.get(member.strategy_version_id, member.allocation),
            status=(
                "ACTIVE"
                if member.status == "PAPER" and allocations.get(member.strategy_version_id, member.allocation) > ZERO
                else member.status
            ),
            action=(
                "ADD"
                if allocations.get(member.strategy_version_id, member.allocation) > ZERO
                and member.allocation <= ZERO
                else member.action
            ),
            score=member.score,
            reason=member.reason,
            evidence_window_id=member.evidence_window_id,
            overlap_key=member.overlap_key,
            evidence_digest=member.evidence_digest,
            position_management_state=member.position_management_state,
        )
        for member in members
    )




def _decision(
    policy: RollingAdmissionPolicy,
    *,
    status: str,
    members: Sequence[RollingSelectionMember],
    reasons: Sequence[str],
    now: datetime,
    removed_members: Sequence[RollingSelectionMember] = (),
    last_membership_change_at: datetime | None = None,
    removed_member_evidence_history: Mapping[str, Sequence[str]] | None = None,
) -> RollingSelectionDecision:
    current = _utc(now, "now", required=True)
    assert current is not None
    due = current + timedelta(days=policy.review_interval_days)
    return RollingSelectionDecision(
        status=status,
        policy_id=policy.policy_id,
        members=tuple(members),
        removed_members=tuple(removed_members),
        reasons=tuple(dict.fromkeys(str(reason) for reason in reasons if str(reason))),
        selected_at=current,
        review_due_at=due,
        last_membership_change_at=last_membership_change_at,
        policy_version=policy.version,
        config_hash=str(policy.config_hash),
        removed_member_evidence_history=removed_member_evidence_history or {},
        score_formula=policy.score_formula,
        formula_version=policy.formula_version,
        policy_config=policy.as_dict(),
        global_budget=policy.global_budget,
    )


def evaluate_rolling_selection(
    policy: RollingAdmissionPolicy | Mapping[str, Any],
    evidence: Sequence[RollingEvidence | Mapping[str, Any]] | Mapping[str, Any],
    current_selection: RollingSelection | RollingSelectionDecision | Mapping[str, Any] | None,
    now: datetime,
) -> RollingSelectionDecision:
    """Evaluate a rolling selection without mutating any input.

    Candidate ordering is score descending then immutable strategy-version id.  The
    evaluator admits only candidates with all policy windows and minimum evidence;
    overlap groups are funded once; and a current ordinary loss is retained unless
    a stronger, genuinely new candidate clears both replacement margin and cooldown.
    """
    admission = _coerce_policy(policy)
    review_time = _utc(now, "now", required=True)
    assert review_time is not None
    records = _coerce_evidence(evidence)
    previous = _coerce_selection(current_selection)
    future_records = tuple(
        item
        for item in records
        if item.available_through is not None and item.available_through > review_time
    )
    usable_records = tuple(
        item
        for item in records
        if item.available_through is None or item.available_through <= review_time
    )
    grouped = _latest_windows(usable_records)
    future_grouped = _latest_windows(future_records)
    future_only_rows = {
        strategy_id: max(
            (
                item
                for source_windows in windows.values()
                for item in source_windows.values()
            ),
            key=lambda item: (
                item.available_through or datetime.min.replace(tzinfo=UTC),
                item.evidence_digest,
                item.evidence_window_id,
            ),
        )
        for strategy_id, windows in future_grouped.items()
        if strategy_id not in grouped and windows
    }

    latest_records = tuple(
        item
        for source_windows in grouped.values()
        for windows in source_windows.values()
        for item in windows.values()
    )
    hard_failures = tuple(
        sorted(
            (item for item in latest_records if item.is_hard_failure),
            key=lambda item: (
                item.strategy_version_id,
                item.requested_days,
                item.available_through or datetime.min.replace(tzinfo=UTC),
                item.evidence_digest,
                item.evidence_window_id,
            ),
        )
    )
    hard_failure_strategy_ids = frozenset(item.strategy_version_id for item in hard_failures)

    old_by_id: dict[str, RollingSelectionMember] = {}
    removed_history: dict[str, RollingSelectionMember] = {}
    removed_digests: dict[str, list[str]] = {}
    if previous is not None:
        for strategy_id, digests in previous.removed_member_evidence_history.items():
            history = removed_digests.setdefault(strategy_id, [])
            for digest in digests:
                value = str(digest).strip()
                if value and value not in history:
                    history.append(value)
        for member in previous.members:
            if member.status in _REMOVED_STATUSES:
                removed_history[member.strategy_version_id] = member
            else:
                old_by_id[member.strategy_version_id] = member
            if member.status in _REMOVED_STATUSES and member.evidence_digest:
                history = removed_digests.setdefault(member.strategy_version_id, [])
                if member.evidence_digest not in history:
                    history.append(member.evidence_digest)

    # Keep a due date as a transparent review cadence.  A hard failure above always
    # wins; otherwise an early review cannot silently churn a portfolio.
    if (
        previous is not None
        and previous.review_due_at is not None
        and review_time < previous.review_due_at
        and not hard_failure_strategy_ids
    ):
        retained = tuple(
            member
            if (status := _incumbent_status(member)) == member.status
            else member.with_status(status, member.reason)
            for member in previous.members[: admission.max_members]
        )
        return _decision(
            admission,
            status="PAPER" if retained else "OBSERVE",
            members=retained,
            reasons=("REVIEW_NOT_DUE",),
            now=review_time,
            last_membership_change_at=previous.last_membership_change_at or previous.selected_at,
            removed_member_evidence_history=previous.removed_member_evidence_history,
        )

    # First pass computes score and evidence quality.  All ties are deterministic.
    candidate_rows: dict[str, tuple[RollingEvidence, Decimal, tuple[str, ...], tuple[RollingEvidence, ...]]] = {}
    for strategy_id, windows in grouped.items():
        # A provisional score identifies overlap winners.  The penalty is applied in
        # the second pass, so a unique member is never penalized for having a key.
        score, anchor, failures, selected = _aggregate_evidence(admission, windows)
        if anchor is not None:
            candidate_rows[strategy_id] = (anchor, score, failures, selected)

    overlap_groups: dict[str, list[str]] = {}
    for strategy_id, (anchor, score_value, failures, _selected) in candidate_rows.items():
        if anchor.overlap_key:
            overlap_groups.setdefault(anchor.overlap_key, []).append(strategy_id)
    overlap_winner: dict[str, str] = {}
    for overlap_key, strategy_ids in overlap_groups.items():
        admissible = [
            strategy_id
            for strategy_id in strategy_ids
            if strategy_id not in hard_failure_strategy_ids
            and not candidate_rows[strategy_id][2]
            and candidate_rows[strategy_id][1] >= admission.min_score
        ]
        healthy_contenders = [
            strategy_id for strategy_id in strategy_ids
            if strategy_id not in hard_failure_strategy_ids
        ]
        contenders = admissible or healthy_contenders or strategy_ids
        overlap_winner[overlap_key] = min(
            contenders,
            key=lambda strategy_id: (-candidate_rows[strategy_id][1], strategy_id),
        )

    evaluated: dict[str, tuple[RollingEvidence, Decimal, tuple[str, ...], bool]] = {}
    for strategy_id, (anchor, base_score, failures, selected) in candidate_rows.items():
        duplicate = bool(anchor.overlap_key and overlap_winner.get(anchor.overlap_key) != strategy_id)
        penalty = ONE if duplicate else ZERO
        score = base_score - penalty * admission.overlap_penalty_weight
        evaluated[strategy_id] = (anchor, score, failures, duplicate)

    retained: dict[str, RollingSelectionMember] = {}
    removed: list[RollingSelectionMember] = []
    reasons: list[str] = [REASON_HARD_FAILURE] if hard_failures else []
    reasons.extend(
        f"{item.strategy_version_id}:{item.failure_reason or REASON_HARD_FAILURE}"
        for item in hard_failures
    )
    # Existing members are retained before considering new candidates.  This is the
    # ordinary-loss rule and also preserves position-management state.
    for strategy_id, old in old_by_id.items():
        if strategy_id in hard_failure_strategy_ids:
            retained[strategy_id] = old.with_status(
                "PAUSED",
                REASON_HARD_FAILURE,
                allocation=ZERO,
                action="REDUCE",
            )
            reasons.append(f"{strategy_id}:{REASON_HARD_FAILURE}")
            continue
        row = evaluated.get(strategy_id)
        if row is None:
            reason = (
                REASON_FUTURE_EVIDENCE
                if strategy_id in future_only_rows
                else REASON_RETAINED_POSITION_MANAGEMENT
            )
            retained[strategy_id] = old.with_status(
                _incumbent_status(old),
                reason,
            )
            reasons.append(f"{strategy_id}:{reason}")
            continue
        anchor, score, failures, duplicate = row
        if duplicate:
            retained[strategy_id] = old.with_status(
                "OBSERVE",
                REASON_OVERLAP_DUPLICATE,
                allocation=ZERO,
                action="REDUCE",
            )
            reasons.append(f"{strategy_id}:{REASON_OVERLAP_DUPLICATE}")
        elif failures:
            retained[strategy_id] = RollingSelectionMember(
                strategy_version_id=old.strategy_version_id,
                candidate_id=old.candidate_id,
                research_trial_id=old.research_trial_id,
                allocation=ZERO,
                status="OBSERVE",
                action="OBSERVE",
                score=score,
                reason=f"{REASON_LOW_EVIDENCE}:{','.join(failures)}",
                evidence_window_id=anchor.evidence_window_id,
                overlap_key=anchor.overlap_key,
                evidence_digest=anchor.evidence_digest,
                position_management_state=old.position_management_state,
            )
            reasons.append(f"{strategy_id}:{REASON_LOW_EVIDENCE}:{','.join(failures)}")
        elif score < admission.min_score:
            reduced = old.allocation > ZERO
            retained[strategy_id] = RollingSelectionMember(
                strategy_version_id=old.strategy_version_id,
                candidate_id=old.candidate_id,
                research_trial_id=old.research_trial_id,
                allocation=_reduced_allocation(old.allocation) if reduced else old.allocation,
                status="REDUCE" if reduced else _incumbent_status(old),
                action="REDUCE" if reduced else old.action,
                score=score,
                reason=REASON_REDUCE if reduced else REASON_RETAINED_ORDINARY_LOSS,
                evidence_window_id=anchor.evidence_window_id,
                overlap_key=anchor.overlap_key,
                evidence_digest=anchor.evidence_digest,
                position_management_state=old.position_management_state,
            )
            reasons.append(
                f"{strategy_id}:{REASON_REDUCE if reduced else REASON_RETAINED_ORDINARY_LOSS}"
            )
        else:
            retained[strategy_id] = RollingSelectionMember(
                strategy_version_id=old.strategy_version_id,
                candidate_id=old.candidate_id,
                research_trial_id=old.research_trial_id,
                allocation=old.allocation,
                status=_incumbent_status(old),
                action="REDUCE" if old.status == "REDUCE" else old.action,
                score=score,
                reason=REASON_ELIGIBLE,
                evidence_window_id=anchor.evidence_window_id,
                overlap_key=anchor.overlap_key,
                evidence_digest=anchor.evidence_digest,
                position_management_state=old.position_management_state,
            )

    eligible: list[tuple[str, RollingEvidence, Decimal]] = []
    observed: list[RollingSelectionMember] = []
    for strategy_id, (anchor, score, failures, duplicate) in evaluated.items():
        if strategy_id in old_by_id:
            continue
        if strategy_id in hard_failure_strategy_ids:
            observed.append(
                _member_from_evidence(
                    anchor,
                    score=score,
                    status="PAUSED",
                    reason=REASON_HARD_FAILURE,
                )
            )
            reasons.append(f"{strategy_id}:{REASON_HARD_FAILURE}")
            continue
        if strategy_id in removed_history or strategy_id in removed_digests:
            old_removed = removed_history.get(strategy_id)
            old_digest = (
                old_removed.evidence_digest
                if old_removed is not None and old_removed.evidence_digest
                else ""
            )
            historical = removed_digests.get(strategy_id, set())
            if (
                (old_removed is not None and old_removed.evidence_window_id == anchor.evidence_window_id)
                or old_digest == anchor.evidence_digest
                or anchor.evidence_digest in historical
            ):
                observed.append(_member_from_evidence(anchor, score=score, status="OBSERVE", reason=REASON_NO_NEW_EVIDENCE))
                reasons.append(f"{strategy_id}:{REASON_NO_NEW_EVIDENCE}")
                continue
        if failures:
            failure_reason = f"{REASON_LOW_EVIDENCE}:{','.join(failures)}"
            observed.append(_member_from_evidence(anchor, score=score, status="OBSERVE", reason=failure_reason))
            reasons.append(f"{strategy_id}:{failure_reason}")
            continue
        if score < admission.min_score:
            observed.append(_member_from_evidence(anchor, score=score, status="OBSERVE", reason=REASON_LOW_EVIDENCE))
            reasons.append(f"{strategy_id}:{REASON_LOW_EVIDENCE}")
            continue
        if duplicate:
            observed.append(_member_from_evidence(anchor, score=score, status="OBSERVE", reason=REASON_OVERLAP_DUPLICATE))
            reasons.append(f"{strategy_id}:{REASON_OVERLAP_DUPLICATE}")
            continue
        eligible.append((strategy_id, anchor, score))
    for strategy_id in sorted(future_only_rows):
        observed.append(
            _member_from_evidence(
                future_only_rows[strategy_id],
                score=ZERO,
                status="OBSERVE",
                reason=REASON_FUTURE_EVIDENCE,
            )
        )
        if strategy_id not in old_by_id:
            reasons.append(f"{strategy_id}:{REASON_FUTURE_EVIDENCE}")


    eligible.sort(key=lambda item: (-item[2], item[0]))
    previous_membership_change_at = (
        previous.last_membership_change_at or previous.selected_at
        if previous is not None
        else None
    )
    cooldown_active = (
        previous_membership_change_at is not None
        and review_time < previous_membership_change_at + timedelta(seconds=admission.cooldown_seconds)
    )
    for strategy_id, anchor, score in eligible:
        candidate = _member_from_evidence(
            anchor,
            score=score,
            status="PAPER" if not admission.experimental_allocation_enabled else "ACTIVE",
            reason=REASON_ELIGIBLE if admission.experimental_allocation_enabled else f"{REASON_ELIGIBLE}:{REASON_EXPERIMENTAL_DISABLED}",
        )
        if len(retained) < admission.max_members:
            retained[strategy_id] = candidate
            continue
        incumbent_id, incumbent = min(retained.items(), key=lambda item: (item[1].score, item[0]))
        if cooldown_active:
            observed.append(candidate.with_status("OBSERVE", REASON_COOLDOWN))
            reasons.append(f"{strategy_id}:{REASON_COOLDOWN}")
            continue
        if score <= incumbent.score + admission.replacement_margin:
            observed.append(candidate.with_status("OBSERVE", REASON_REPLACEMENT_MARGIN))
            reasons.append(f"{strategy_id}:{REASON_REPLACEMENT_MARGIN}")
            continue
        removed.append(incumbent.with_status("REMOVED", REASON_REPLACED, allocation=ZERO))
        del retained[incumbent_id]
        retained[strategy_id] = candidate
        reasons.append(f"{incumbent_id}:{REASON_REPLACED}")

    # The persisted selection contains only its bounded K members.  Low-evidence
    # observations remain visible when there are free slots; otherwise they belong
    # in the transparent reason list and are not accidentally funded.
    ordered_members = sorted(retained.values(), key=lambda member: (-member.score, member.strategy_version_id))[: admission.max_members]
    if len(ordered_members) < admission.max_members:
        for item in sorted(observed, key=lambda member: (-member.score, member.strategy_version_id)):
            if len(ordered_members) >= admission.max_members:
                break
            if item.strategy_version_id not in {member.strategy_version_id for member in ordered_members}:
                ordered_members.append(item)
    final_members = _allocate(ordered_members, admission)
    if final_members and any(member.status in {"PAPER", "ACTIVE", "REDUCE"} for member in final_members):
        status = "ACTIVE" if admission.experimental_allocation_enabled else "PAPER"
    elif final_members and any(member.status == "PAUSED" for member in final_members):
        status = "PAUSE"
    elif final_members:
        status = "OBSERVE"
    else:
        status = "OBSERVE"
    previous_signature = _membership_allocation_signature(previous.members) if previous is not None else ()
    current_signature = _membership_allocation_signature(final_members)
    if previous is None:
        last_membership_change_at = review_time if current_signature else None
    elif current_signature != previous_signature:
        last_membership_change_at = review_time
    else:
        last_membership_change_at = previous_membership_change_at
    if not final_members and not reasons:
        reasons.append("NO_CANDIDATES")
    history_map: dict[str, list[str]] = {
        strategy_id: list(digests)
        for strategy_id, digests in removed_digests.items()
        if digests
    }
    for member in removed:
        if member.evidence_digest:
            digests = history_map.setdefault(member.strategy_version_id, [])
            if member.evidence_digest not in digests:
                digests.append(member.evidence_digest)
    history_map = _bounded_removed_member_evidence_history(history_map)
    return _decision(
        admission,
        status=status,
        members=final_members,
        reasons=reasons,
        now=review_time,
        removed_members=removed,
        last_membership_change_at=last_membership_change_at,
        removed_member_evidence_history=history_map,
    )


__all__ = [
    "DEFAULT_SCORE_FORMULA",
    "POLICY_VERSION",
    "REASON_CAPACITY",
    "REASON_COOLDOWN",
    "REASON_ELIGIBLE",
    "REASON_EXPERIMENTAL_DISABLED",
    "REASON_HARD_FAILURE",
    "REASON_LOW_EVIDENCE",
    "REASON_FUTURE_EVIDENCE",
    "REASON_OVERLAP_DUPLICATE",
    "REASON_REPLACED",
    "REASON_REPLACEMENT_MARGIN",
    "REASON_RETAINED_ORDINARY_LOSS",
    "REASON_NO_NEW_EVIDENCE",
    "REASON_REDUCE",
    "REASON_RETAINED_POSITION_MANAGEMENT",
    "ResearchTrial",
    "ResearchTrialRecord",
    "RollingAdmissionPolicy",
    "RollingEvidence",
    "RollingSelection",
    "RollingSelectionDecision",
    "RollingSelectionMember",
    "SelectionDecision",
    "StrategyEvidenceWindow",
    "StrategyVersion",
    "StrategyVersionRecord",
    "PortfolioSelection",
    "PortfolioSelectionMember",
    "default_rolling_admission_policy",
    "evaluate_rolling_selection",
]
