"""Shadow-only paired assessment for terminal rejected prediction candidates.

This module intentionally owns no lifecycle, rolling-admission, or venue route.  It
materializes a frozen, paper-only assessment from two already-rejected candidates,
then drives one shared :class:`ForwardPaperEngine` and :class:`Portfolio`.
"""
from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
import hashlib
import inspect
import json
import math
from decimal import Decimal, InvalidOperation
from types import MappingProxyType
from typing import Any

from .canary_settings import CanarySettingsService
from .domain import PredictionMarketSnapshot, ensure_utc, parse_timestamp, to_record, utc_now
from .forward import (
    ForwardTestRegistry,
    ForwardTestSpec,
    _content_hash,
    _operational_setup_for_strategy,
    _operational_setup_hash,
)
from .experiment_plan import normalize_market_scope
from .market_scope import CurrentMarket, MarketScopeResolutionStatus, resolve_market_scope
from .paper_engine import ForwardPaperEngine, OperationalPaperPolicy
from .portfolio import Portfolio
from .strategy.signals import (
    MODEL_INPUT_MISSING,
    evaluate_model_probability_evidence,
    evaluate_signal_evaluation,
)


SHADOW_SCHEMA = "axiom-shadow-assessment-v1"
SHADOW_STATUS_REGISTERED = "REGISTERED"
SHADOW_STATUS_RUNNING = "RUNNING"
SHADOW_STATUS_WAITING_FOR_DATA = "WAITING_FOR_DATA"
SHADOW_STATUS_COMPLETED = "COMPLETED"
SHADOW_STATUS_BLOCKED = "BLOCKED"
SHADOW_STATUS_STOPPED = "STOPPED"
SHADOW_STATES = frozenset(
    {
        SHADOW_STATUS_REGISTERED,
        SHADOW_STATUS_RUNNING,
        SHADOW_STATUS_WAITING_FOR_DATA,
        SHADOW_STATUS_COMPLETED,
        SHADOW_STATUS_BLOCKED,
        SHADOW_STATUS_STOPPED,
    }
)
_TERMINAL_STATUSES = frozenset({SHADOW_STATUS_COMPLETED, SHADOW_STATUS_BLOCKED, SHADOW_STATUS_STOPPED})
_MEMBER_ORDER = {"momentum": 0, "mean_reversion": 1}
_MAX_MARKETS = 1_000
_MAX_PROVIDER_ROWS = 10_000
_MAX_RECONCILE_ROWS = 20_000
_MAX_LOOKBACK = _MAX_PROVIDER_ROWS
_MAX_CYCLES = 100_000
_SHARED_CAP_NAMES = (
    "shared_cap",
    "shared_cap_usd",
    "max_shared_cap",
    "max_shared_cap_usd",
    "shared_budget",
    "shared_budget_usd",
    "max_shared_budget",
    "max_shared_budget_usd",
    "bankroll_cap",
    "bankroll_cap_usd",
    "max_bankroll",
    "max_bankroll_usd",
    "budget_cap",
    "budget_cap_usd",
    "max_budget",
    "max_budget_usd",
    "global_budget",
    "global_budget_usd",
    "max_global_budget",
    "max_global_budget_usd",
)
_MAX_STATE_BLOCKERS = 64
# Aggregate-group identities are bounded by the registration cycle limit.  A
# smaller rolling cache makes an old durable group look new after restart and
# inflates accounting when reconciliation is repeated.
_MAX_AGGREGATE_GROUPS = _MAX_CYCLES
_MAX_BANKROLL = 1_000_000_000.0


class ShadowAssessmentError(ValueError):
    """Malformed or incompatible shadow assessment input."""


class ShadowBlocked(RuntimeError):
    """A shadow assessment cannot proceed without unsafe inference."""


def _canonical(value: Any) -> str:
    def plain(item: Any) -> Any:
        if isinstance(item, Mapping):
            return {str(key): plain(child) for key, child in item.items()}
        if isinstance(item, (list, tuple)):
            return [plain(child) for child in item]
        if isinstance(item, datetime):
            return ensure_utc(item).isoformat()
        value_attr = getattr(item, "value", None)
        if value_attr is not None and item.__class__.__name__ != "type":
            return plain(value_attr)
        return item

    return json.dumps(plain(value), sort_keys=True, separators=(",", ":"), allow_nan=False, default=repr)


def _json_copy(value: Any) -> Any:
    return json.loads(_canonical(value))


def _text(value: Any) -> str:
    return str(value).strip() if value is not None else ""


def _mapping(value: Any, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ShadowAssessmentError(f"{name} must be a mapping")
    return value


def _first(mapping: Mapping[str, Any], *names: str) -> Any:
    for name in names:
        if name in mapping and mapping[name] not in (None, ""):
            return mapping[name]
    return None


def _member_id(family: str, candidate_id: str) -> str:
    return f"{family}:{candidate_id}"


def _canonical_member_id(item: Mapping[str, Any]) -> str:
    """Return the deterministic shadow identity, rejecting divergent aliases."""
    family = _text(item.get("family")).lower()
    candidate_id = _text(item.get("candidate_id"))
    if family not in _MEMBER_ORDER or not candidate_id:
        raise ShadowAssessmentError("shadow member identity is invalid")
    expected = _member_id(family, candidate_id)
    shadow_member_id = _text(item.get("shadow_member_id"))
    member_id = _text(item.get("member_id"))
    if shadow_member_id and shadow_member_id != expected:
        raise ShadowAssessmentError("shadow member identity does not match family and candidate")
    if member_id and member_id != expected:
        raise ShadowAssessmentError("member identity does not match family and candidate")
    if shadow_member_id and member_id and shadow_member_id != member_id:
        raise ShadowAssessmentError("shadow member identity aliases conflict")
    return expected

def _declared_values(payload: Mapping[str, Any], names: Sequence[str]) -> list[Any]:
    """Collect immutable declaration values from the supported frozen paths."""
    found: list[Any] = []
    queue: list[tuple[Mapping[str, Any], int]] = [(payload, 0)]
    seen: set[int] = set()
    while queue:
        current, depth = queue.pop(0)
        marker = id(current)
        if marker in seen or depth > 4:
            continue
        seen.add(marker)
        for name in names:
            if name in current and current[name] not in (None, ""):
                found.append(current[name])
        for key in (
            "config",
            "forward_config",
            "frozen_document",
            "frozen_documents",
            "forward_test",
            "frozen",
            "forward_evidence",
            "provenance",
            "setup",
            "operational_setup",
            "strategy",
            "strategy_document",
            "model",
            "model_document",
        ):
            child = current.get(key)
            if isinstance(child, Mapping):
                queue.append((child, depth + 1))
    return found


def _one_declared_mapping(payload: Mapping[str, Any], names: Sequence[str], label: str) -> Mapping[str, Any] | None:
    values = _declared_values(payload, names)
    mappings = [value for value in values if isinstance(value, Mapping)]
    if values and len(mappings) != len(values):
        raise ShadowAssessmentError(f"frozen {label} provenance is invalid")
    if not mappings:
        return None
    canonical = _canonical(mappings[0])
    if any(_canonical(value) != canonical for value in mappings[1:]):
        raise ShadowAssessmentError(f"conflicting frozen {label} provenance")
    return mappings[0]


def _candidate_config(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Return the one canonical forward config, including nested frozen paths."""
    values = _declared_values(payload, ("forward_config", "config"))
    mappings = [value for value in values if isinstance(value, Mapping)]
    if values and len(mappings) != len(values):
        raise ShadowAssessmentError("frozen candidate config provenance is invalid")
    if not mappings:
        raise ShadowAssessmentError("missing frozen candidate config")
    try:
        from .forward import _canonical_forward_config
        canonical = [_canonical_forward_config(value) for value in mappings]
    except Exception as exc:
        raise ShadowAssessmentError("frozen candidate config is not canonical") from exc
    first = _canonical(canonical[0])
    if any(_canonical(value) != first for value in canonical[1:]):
        raise ShadowAssessmentError("conflicting frozen candidate config provenance")
    return dict(canonical[0])
_HISTORICAL_REJECTED_PROJECTION = "HISTORICAL_REJECTED_SETUP_CURRENT_SETTINGS"
_FORWARD_EVIDENCE_STAGES = frozenset(
    {"ROBUSTNESS_CHECKED", "FROZEN", "PAPER_FORWARD", "PAPER_PROMOTABLE"}
)
_ABSOLUTE_MOVE_PREDICATE = {
    "version": "absolute-move-v1",
    "minimum_move": 0.05,
    "units": "probability",
    "boundary": "inclusive",
}


def _candidate_fallback_value(payload: Mapping[str, Any], names: Sequence[str]) -> Any:
    """Return one direct candidate declaration, rejecting conflicting aliases."""
    values = [
        payload[name]
        for name in names
        if name in payload and payload[name] not in (None, "")
    ]
    if not values:
        return None
    first = values[0]
    if any(_canonical(value) != _canonical(first) for value in values[1:]):
        raise ShadowAssessmentError(
            f"conflicting rejected candidate declaration: {names[0]}"
        )
    return first


def _rejected_strategy(
    payload: Mapping[str, Any],
    setup: Mapping[str, Any],
    family: str,
) -> tuple[dict[str, Any], str]:
    values = _declared_values(
        payload,
        ("strategy_document", "strategy", "strategy_definition"),
    )
    mappings = [value for value in values if isinstance(value, Mapping)]
    if not mappings:
        raise ShadowAssessmentError("missing rejected strategy document")
    for value in values:
        if not isinstance(value, Mapping):
            raise ShadowAssessmentError("rejected strategy provenance is invalid")
    if len(mappings) > 1 and any(
        _canonical(value) != _canonical(mappings[0]) for value in mappings[1:]
    ):
        raise ShadowAssessmentError("conflicting rejected strategy provenance")
    source = dict(mappings[0])

    required_fields = (
        "version",
        "market_type",
        "family",
        "operations",
        "probability_model",
        "resolution_aware",
        "resolution_inputs",
        "parameters",
    )
    missing_fields = [name for name in required_fields if name not in source]
    if missing_fields:
        if "parameters" in missing_fields:
            raise ShadowAssessmentError("missing rejected strategy parameters")
        raise ShadowAssessmentError(
            "missing rejected strategy fields: " + ", ".join(missing_fields)
        )

    source_family = source["family"]
    if not isinstance(source_family, str) or source_family != family:
        raise ShadowAssessmentError("rejected strategy family disagrees with root family")
    if isinstance(source["version"], bool) or source["version"] != 1:
        raise ShadowAssessmentError("rejected strategy version is unsupported")
    if source["market_type"] != "prediction":
        raise ShadowAssessmentError("rejected strategy market type is invalid")
    operations = source["operations"]
    if not isinstance(operations, (list, tuple)):
        raise ShadowAssessmentError("rejected strategy operations are invalid")
    if operations:
        raise ShadowAssessmentError(
            "rejected strategy operations are incompatible with the directional setup"
        )
    probability_model = source["probability_model"]
    if not isinstance(probability_model, str) or not probability_model.strip():
        raise ShadowAssessmentError("rejected strategy probability model is invalid")
    if source["resolution_aware"] is not True:
        raise ShadowAssessmentError("rejected strategy resolution awareness is invalid")
    resolution_inputs = source["resolution_inputs"]
    if (
        not isinstance(resolution_inputs, (list, tuple))
        or not resolution_inputs
        or any(not isinstance(item, str) or not item.strip() for item in resolution_inputs)
    ):
        raise ShadowAssessmentError("rejected strategy resolution inputs are invalid")

    source_parameters = source["parameters"]
    if not isinstance(source_parameters, Mapping):
        raise ShadowAssessmentError("rejected strategy parameters are invalid")
    top_parameters = payload.get("parameters")
    if top_parameters is not None:
        top_parameters = _mapping(top_parameters, "strategy parameters")
        unknown_parameters = [
            name for name in top_parameters if name not in source_parameters
        ]
        if unknown_parameters:
            raise ShadowAssessmentError(
                "rejected root strategy parameters are not explicit: "
                + ", ".join(str(name) for name in unknown_parameters)
            )
        if any(
            _canonical(top_parameters[name]) != _canonical(source_parameters[name])
            for name in top_parameters
        ):
            raise ShadowAssessmentError("conflicting rejected strategy parameters")
    parameters = dict(source_parameters)

    parameter_names = ("lookback", "threshold")

    def scalar_declaration(names: Sequence[str], label: str) -> Any:
        found: list[Any] = []
        for name in names:
            if name in source_parameters and source_parameters[name] not in (None, ""):
                found.append(source_parameters[name])
            if name in payload and payload[name] not in (None, ""):
                found.append(payload[name])
        if not found:
            raise ShadowAssessmentError(f"missing rejected strategy {label}")
        first = found[0]
        if any(_canonical(value) != _canonical(first) for value in found[1:]):
            raise ShadowAssessmentError(f"conflicting rejected strategy {label}")
        return first

    missing_parameters = [name for name in (*parameter_names, "entry_predicate") if name not in parameters]
    if missing_parameters:
        if "entry_predicate" in missing_parameters:
            raise ShadowAssessmentError("missing rejected strategy entry predicate")
        raise ShadowAssessmentError(
            "missing rejected strategy parameters: " + ", ".join(missing_parameters)
        )

    raw_lookback = scalar_declaration(("lookback", "window"), "lookback")
    if isinstance(raw_lookback, bool) or type(raw_lookback) is not int:
        raise ShadowAssessmentError("rejected strategy lookback must be an integer")
    if raw_lookback < 1 or raw_lookback > _MAX_LOOKBACK:
        raise ShadowAssessmentError("rejected strategy lookback is out of bounds")
    raw_threshold = scalar_declaration(("threshold",), "threshold")
    if isinstance(raw_threshold, bool):
        raise ShadowAssessmentError("rejected strategy threshold is invalid")
    try:
        threshold = float(raw_threshold)
    except (TypeError, ValueError, OverflowError):
        threshold = math.nan
    if not math.isfinite(threshold) or not 0.0 <= threshold <= 1.0:
        raise ShadowAssessmentError("rejected strategy threshold is invalid")

    predicate = _mapping(parameters["entry_predicate"], "entry predicate")
    if payload.get("entry_predicate") not in (None, ""):
        if _canonical(payload["entry_predicate"]) != _canonical(predicate):
            raise ShadowAssessmentError("conflicting rejected strategy entry predicate")
    if _canonical(predicate) != _canonical(_ABSOLUTE_MOVE_PREDICATE):
        raise ShadowAssessmentError("rejected strategy entry predicate is not canonical")
    try:
        predicate_minimum = float(predicate["minimum_move"])
    except (KeyError, TypeError, ValueError, OverflowError):
        raise ShadowAssessmentError("rejected strategy entry predicate is invalid") from None
    if abs(threshold - predicate_minimum) > 1e-12:
        raise ShadowAssessmentError("rejected strategy threshold and entry predicate disagree")

    # The strategy mapping is the immutable declaration.  Do not complete it
    # from wrapper fields or overwrite any required dimension before parsing.
    document = dict(source)
    try:
        from .strategy import load_strategy
        canonical = load_strategy(document).to_dict()
    except Exception as exc:
        raise ShadowAssessmentError("rejected strategy document is invalid") from exc
    for field_name in required_fields:
        if _canonical(canonical.get(field_name)) != _canonical(source[field_name]):
            raise ShadowAssessmentError(
                f"rejected strategy {field_name} is not canonical"
            )
    canonical_family = _text(canonical.get("family")).lower()
    if canonical_family != family or canonical.get("market_type") != "prediction":
        raise ShadowAssessmentError("rejected strategy family or market type is invalid")
    canonical_parameters = canonical.get("parameters")
    if not isinstance(canonical_parameters, Mapping):
        raise ShadowAssessmentError("rejected strategy parameters are invalid")
    if canonical_parameters.get("lookback") != raw_lookback:
        raise ShadowAssessmentError("rejected strategy lookback is not canonical")
    if abs(float(canonical_parameters.get("threshold", math.nan)) - threshold) > 1e-12:
        raise ShadowAssessmentError("rejected strategy threshold is not canonical")
    if _canonical(canonical_parameters.get("entry_predicate")) != _canonical(predicate):
        raise ShadowAssessmentError("rejected strategy entry predicate is not canonical")

    # The setup is candidate-declared and immutable.  Validate the directional
    # fields it carries, but do not regenerate or write it as historical config.
    setup_family = _text(setup.get("family")).lower()
    if setup_family != family:
        raise ShadowAssessmentError("rejected operational setup family disagrees")
    setup_market_type = setup.get("market_type")
    if setup_market_type not in (None, "", "prediction"):
        raise ShadowAssessmentError("rejected operational setup market type disagrees")
    setup_lookback = setup.get("lookback")
    if setup_lookback not in (None, "") and setup_lookback != raw_lookback:
        raise ShadowAssessmentError("rejected operational setup lookback disagrees")
    setup_predicate = setup.get("entry_predicate")
    if setup_predicate not in (None, "") and _canonical(setup_predicate) != _canonical(predicate):
        raise ShadowAssessmentError("rejected operational setup entry predicate disagrees")
    return canonical, _content_hash(canonical)


def _rejected_model(payload: Mapping[str, Any]) -> tuple[dict[str, Any], str, bool]:
    values = _declared_values(payload, ("model_document", "model"))
    mappings = [value for value in values if isinstance(value, Mapping)]
    if values and len(mappings) != len(values):
        raise ShadowAssessmentError("rejected model provenance is invalid")
    if len(mappings) > 1 and any(
        _canonical(value) != _canonical(mappings[0]) for value in mappings[1:]
    ):
        raise ShadowAssessmentError("conflicting rejected model provenance")
    model = _json_copy(mappings[0]) if mappings else {}
    return model, _content_hash(model), bool(mappings)
def _rejected_flags(payload: Mapping[str, Any]) -> None:
    for name in ("paper_only", "research_only"):
        values = _declared_values(payload, (name,))
        if not values or any(not isinstance(value, bool) or value is not True for value in values):
            raise ShadowAssessmentError(f"rejected candidate must declare {name}=true")
    values = _declared_values(payload, ("live_execution",))
    if any(not isinstance(value, bool) or value is not False for value in values):
        raise ShadowAssessmentError("rejected candidate live_execution must be false")


_COST_FEE_ALIAS_NAMES = frozenset({"fee_bps", "fees_bps", "fee_rate"})
_COST_SLIPPAGE_ALIAS_NAMES = frozenset(
    {"slippage_bps", "max_slippage_bps", "slippage"}
)


def _strict_cost_number(value: Any, path: str) -> float:
    if isinstance(value, bool) or value in (None, ""):
        raise ShadowAssessmentError(f"{path} must be finite and non-negative")
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        raise ShadowAssessmentError(f"{path} must be finite and non-negative") from None
    if not math.isfinite(number) or number < 0:
        raise ShadowAssessmentError(f"{path} must be finite and non-negative")
    return number

def _validate_raw_cost_aliases(value: Any, path: str = "costs", *, depth: int = 0) -> None:
    if depth > 6:
        raise ShadowAssessmentError("cost provenance nesting is invalid")
    if isinstance(value, Mapping):
        for raw_key, raw_value in value.items():
            key = _text(raw_key).lower().replace("-", "_")
            child_path = f"{path}.{key}"
            if (
                key in _COST_FEE_ALIAS_NAMES
                or key in _COST_SLIPPAGE_ALIAS_NAMES
            ) and not (
                key == "slippage" and isinstance(raw_value, Mapping)
            ):
                candidate = raw_value
                if isinstance(candidate, Mapping):
                    candidate = _first(candidate, key, "value", "amount", "bps")
                _strict_cost_number(candidate, child_path)
            _validate_raw_cost_aliases(raw_value, child_path, depth=depth + 1)
    elif isinstance(value, (list, tuple)):
        for index, child in enumerate(value[:32]):
            _validate_raw_cost_aliases(child, f"{path}[{index}]", depth=depth + 1)

def _reject_candidate_allocation_aliases(
    value: Any,
    path: str = "candidate",
    *,
    depth: int = 0,
) -> None:
    """Do not let historical candidate sizing influence the shared wallet."""
    if depth > 6:
        raise ShadowAssessmentError("candidate allocation provenance is invalid")
    if isinstance(value, Mapping):
        for raw_key, raw_value in value.items():
            key = _text(raw_key).lower().replace("-", "_")
            child_path = f"{path}.{key}"
            if key in _ALLOCATION_ALIAS_NAMES:
                raise ShadowAssessmentError(
                    f"candidate allocation declaration is not permitted: {child_path}"
                )
            _reject_candidate_allocation_aliases(
                raw_value, child_path, depth=depth + 1
            )
    elif isinstance(value, (list, tuple)):
        for index, child in enumerate(value[:32]):
            _reject_candidate_allocation_aliases(
                child, f"{path}[{index}]", depth=depth + 1
            )


def _rejected_costs(
    payload: Mapping[str, Any],
    setup: Mapping[str, Any],
) -> dict[str, Any]:
    raw = _candidate_fallback_value(
        payload,
        ("cost_assumptions", "cost_provenance", "book_assumptions"),
    )
    if raw is None:
        raw = setup.get("book_assumptions")
    if not isinstance(raw, Mapping):
        raise ShadowAssessmentError("missing rejected recorded-book cost assumptions")
    _validate_raw_cost_aliases(raw, "rejected.costs")
    setup_book = setup.get("book_assumptions")
    if setup_book is not None:
        _validate_raw_cost_aliases(setup_book, "rejected.setup.book_assumptions")
    source = _text(raw.get("source", raw.get("book_source"))).lower()
    setup_source = (
        _text(setup_book.get("source", setup_book.get("book_source"))).lower()
        if isinstance(setup_book, Mapping)
        else ""
    )
    if source and source != "recorded_book":
        raise ShadowAssessmentError("rejected cost assumptions must use recorded_book")
    if setup_source and setup_source != "recorded_book":
        raise ShadowAssessmentError("operational setup book source is invalid")
    if source != "recorded_book" and setup_source != "recorded_book":
        raise ShadowAssessmentError("rejected recorded-book cost source is missing")
    normalized = dict(raw)
    fees = normalized.get("fees")
    slippage = normalized.get("slippage")

    def strict_alias(
        mapping: Mapping[str, Any],
        aliases: Sequence[str],
        path: str,
    ) -> float | None:
        for name in aliases:
            if name in mapping:
                return _strict_cost_number(mapping[name], f"{path}.{name}")
        return None

    fee_bps = strict_alias(normalized, tuple(_COST_FEE_ALIAS_NAMES), "rejected.costs")
    if fee_bps is None and isinstance(fees, Mapping):
        fee_bps = strict_alias(
            fees, tuple(_COST_FEE_ALIAS_NAMES), "rejected.costs.fees"
        )
    if fee_bps is not None:
        normalized["fee_bps"] = fee_bps

    slippage_bps = strict_alias(
        normalized, tuple(_COST_SLIPPAGE_ALIAS_NAMES - {"slippage"}), "rejected.costs"
    )
    if slippage_bps is None and isinstance(slippage, Mapping):
        slippage_bps = strict_alias(
            slippage,
            tuple(_COST_SLIPPAGE_ALIAS_NAMES - {"slippage"}),
            "rejected.costs.slippage",
        )
    if slippage_bps is not None:
        normalized["slippage_bps"] = slippage_bps
    elif not isinstance(slippage, Mapping):
        if "slippage" in normalized:
            normalized["slippage_bps"] = _strict_cost_number(
                normalized["slippage"], "rejected.costs.slippage"
            )
    if "fee_bps" not in normalized or "slippage_bps" not in normalized:
        raise ShadowAssessmentError(
            "rejected recorded-book costs must include fee_bps and slippage_bps"
        )
    costs = _cost_provenance(
        {},
        setup,
        {"paper_assumptions": normalized},
    )
    if source or setup_source:
        costs["source"] = "recorded_book"
    # Historical rejected candidates did not freeze paper sizing.  Never let a
    # stale candidate allocation become a shared-wallet budget fence.
    costs.pop("sizing", None)
    return costs


def _validate_rejected_without_config(
    candidate_id: str,
    record: Mapping[str, Any],
    payload: Mapping[str, Any],
    settings: Mapping[str, Any],
) -> dict[str, Any]:
    """Project a terminal rejection into a new shadow registration config."""
    _reject_candidate_allocation_aliases(payload)
    for index, value in enumerate(
        _declared_values(
            payload,
            ("cost_provenance", "cost_assumptions", "paper_assumptions", "book_assumptions"),
        )
    ):
        _validate_raw_cost_aliases(value, f"rejected.declared_costs[{index}]")
    _rejected_flags(payload)
    setup_value = _one_declared_mapping(payload, ("operational_setup", "setup"), "operational setup")
    setup = dict(_mapping(setup_value, "operational setup"))
    setup_hash_values = _declared_values(payload, ("operational_setup_hash", "setup_hash"))
    if not setup_hash_values:
        raise ShadowAssessmentError("missing rejected operational setup hash")
    setup_hash = _text(setup_hash_values[0])
    if any(_text(value) != setup_hash for value in setup_hash_values):
        raise ShadowAssessmentError("conflicting rejected operational setup hash")
    try:
        if setup_hash != _operational_setup_hash(setup):
            raise ShadowAssessmentError("rejected operational setup hash mismatch")
    except ShadowAssessmentError:
        raise
    except Exception as exc:
        raise ShadowAssessmentError("rejected operational setup cannot be hashed") from exc
    setup_id_values = _declared_values(payload, ("setup_id", "operational_setup_id"))
    if setup_id_values:
        if any(not isinstance(value, str) or not value.strip() for value in setup_id_values):
            raise ShadowAssessmentError("rejected operational setup id must be a non-empty string")
        setup_id = setup_id_values[0].strip()
        if any(value.strip() != setup_id for value in setup_id_values[1:]):
            raise ShadowAssessmentError("missing or conflicting rejected operational setup id")
    else:
        setup_id = setup.get("setup_id")
        if not isinstance(setup_id, str) or not setup_id.strip():
            raise ShadowAssessmentError("rejected operational setup id must be a non-empty string")
        setup_id = setup_id.strip()

    family_values = _declared_values(payload, ("family", "strategy_family"))
    if setup.get("family") not in (None, ""):
        family_values.append(setup["family"])
    family = {_text(value).lower() for value in family_values if _text(value)}
    if family != {"momentum"} and family != {"mean_reversion"}:
        raise ShadowAssessmentError("rejected candidate family is ambiguous or invalid")
    family_name = next(iter(family))
    strategy, strategy_hash = _rejected_strategy(payload, setup, family_name)
    model, model_hash, model_declared = _rejected_model(payload)
    declared_strategy_hash = _declared_values(payload, ("strategy_hash",))
    if declared_strategy_hash and any(_text(value) != strategy_hash for value in declared_strategy_hash):
        raise ShadowAssessmentError("rejected strategy hash mismatch")
    declared_model_hash = _declared_values(payload, ("model_hash",))
    if declared_model_hash and any(_text(value) != model_hash for value in declared_model_hash):
        raise ShadowAssessmentError("rejected model hash mismatch")

    scope = _candidate_scope(payload, setup, {})
    setup_policy = setup.get("market_scope_policy")
    if isinstance(setup_policy, Mapping):
        try:
            if normalize_market_scope(setup_policy).scope_hash != normalize_market_scope(scope).scope_hash:
                raise ShadowAssessmentError("rejected setup and market scope disagree")
        except ShadowAssessmentError:
            raise
        except Exception as exc:
            raise ShadowAssessmentError("rejected setup market scope is invalid") from exc
    scope_policy = normalize_market_scope(scope)
    scope_hash_values = _declared_values(payload, ("market_scope_hash", "scope_hash"))
    scope_version_values = _declared_values(payload, ("market_scope_version", "scope_version"))
    if not scope_hash_values or any(_text(value) != scope_policy.scope_hash for value in scope_hash_values):
        raise ShadowAssessmentError("rejected market scope hash is missing or invalid")
    if not scope_version_values or any(_text(value) != scope_policy.scope_version for value in scope_version_values):
        raise ShadowAssessmentError("rejected market scope version is missing or invalid")

    exit_policy = _candidate_exit(payload, setup, {})
    costs = _rejected_costs(payload, setup)
    risk_limits = _risk_limits(settings, {})
    rejection_evidence = _rejection_evidence(candidate_id, payload)
    if not rejection_evidence.get("has_rejection_provenance"):
        raise ShadowAssessmentError("missing rejection provenance")
    rejection_evidence.pop("has_rejection_provenance", None)
    historical_values = _declared_values(
        payload,
        ("historical_evidence", "historical_provenance", "historical_support"),
    )
    historical_evidence: Mapping[str, Any] = {}
    if historical_values:
        if any(not isinstance(value, Mapping) for value in historical_values):
            raise ShadowAssessmentError("historical provenance must be a mapping")
        historical_evidence = _json_copy(historical_values[0])
        if any(_canonical(value) != _canonical(historical_evidence) for value in historical_values[1:]):
            raise ShadowAssessmentError("conflicting historical provenance")

    settings_identity = {
        key: settings.get(key) for key in ("config_id", "generation", "config_hash")
    }
    projection_config = {
        "schema": SHADOW_SCHEMA,
        "shadow_assessment": True,
        "observation_intent": True,
        "paper_only": True,
        "research_only": True,
        "live_execution": False,
        "execution": "paper_only",
        "market_authority_required": bool(scope_policy.market_ids),
        "market_scope": scope_policy.as_dict(),
        "market_scope_hash": scope_policy.scope_hash,
        "market_scope_version": scope_policy.scope_version,
        "strategy_document": _json_copy(strategy),
        "model_document": _json_copy(model),
        "exit_policy": _json_copy(exit_policy),
        "paper_assumptions_explicit": True,
        "paper_assumptions": _json_copy(costs),
        "risk_limits": _json_copy(risk_limits),
        "settings_identity": _json_copy(settings_identity),
    }
    projection_hash = _content_hash(
        {"config": projection_config, "risk_limits": risk_limits}
    )
    candidate_declared = {
        "strategy": _json_copy(strategy),
        "operational_setup": _json_copy(setup),
        "market_scope": scope_policy.as_dict(),
        "exit_policy": _json_copy(exit_policy),
        "recorded_book_cost_assumptions": _json_copy(costs),
        "paper_only": True,
        "research_only": True,
        "rejection_evidence": _json_copy(rejection_evidence),
    }
    if model_declared:
        candidate_declared["model"] = _json_copy(model)
    projection_provenance = {
        "kind": _HISTORICAL_REJECTED_PROJECTION,
        "candidate_declared": candidate_declared,
        "current_derived": {
            "risk_limits": _json_copy(risk_limits),
            "paper_sizing": {
                "rule": "shared_wallet_half_at_registration",
                "settings_identity": _json_copy(settings_identity),
            },
            "settings_identity": _json_copy(settings_identity),
        },
    }
    return {
        "candidate_id": candidate_id,
        "shadow_member_id": _member_id(family_name, candidate_id),
        "family": family_name,
        "setup": _json_copy(setup),
        "setup_id": setup_id,
        "setup_hash": setup_hash,
        "strategy": _json_copy(strategy),
        "strategy_hash": strategy_hash,
        "model": _json_copy(model),
        "model_hash": model_hash,
        "config": projection_config,
        "config_hash": projection_hash,
        "scope": scope_policy.as_dict(),
        "scope_hash": scope_policy.scope_hash,
        "scope_version": scope_policy.scope_version,
        "exit_policy": _json_copy(exit_policy),
        "cost_provenance": _json_copy(costs),
        "risk_limits": _json_copy(risk_limits),
        "rejection_evidence": rejection_evidence,
        "historical_evidence": historical_evidence,
        "projection_provenance": projection_provenance,
        "historical_frozen_config": False,
    }
def _candidate_scope(
    payload: Mapping[str, Any],
    setup: Mapping[str, Any],
    config: Mapping[str, Any],
) -> Mapping[str, Any]:
    """Resolve scope aliases without allowing one frozen document to win silently."""
    values = _declared_values(payload, ("market_scope", "scope"))
    if isinstance(setup, Mapping):
        values.extend(
            setup[name]
            for name in ("market_scope", "scope")
            if setup.get(name) is not None
        )
    if isinstance(config, Mapping):
        values.extend(
            config[name]
            for name in ("market_scope", "scope")
            if config.get(name) is not None
        )
    policies: list[Any] = []
    for value in values:
        if not isinstance(value, Mapping):
            raise ShadowAssessmentError("frozen market scope provenance is invalid")
        try:
            policies.append(normalize_market_scope(value))
        except Exception as exc:
            raise ShadowAssessmentError("frozen market scope is not canonical") from exc
    if not policies:
        raise ShadowAssessmentError("missing frozen market scope")
    first = policies[0]
    if any(policy.scope_hash != first.scope_hash for policy in policies[1:]):
        raise ShadowAssessmentError("conflicting frozen market scope provenance")
    return first.as_dict()


def _candidate_risk_limits(payload: Mapping[str, Any], config: Mapping[str, Any]) -> dict[str, Any]:
    values = _declared_values(
        payload,
        ("risk_limits", "risk_snapshot", "candidate_risk_limits"),
    )
    if not values and isinstance(config, Mapping):
        values = [
            config[name]
            for name in ("risk_limits", "risk_snapshot", "candidate_risk_limits")
            if name in config and config[name] not in (None, "")
        ]
    mappings = [value for value in values if isinstance(value, Mapping)]
    if values and len(mappings) != len(values):
        raise ShadowAssessmentError("candidate risk provenance is invalid")
    if not mappings:
        raise ShadowAssessmentError("candidate risk provenance is required")
    first = mappings[0]
    if any(_canonical(value) != _canonical(first) for value in mappings[1:]):
        raise ShadowAssessmentError("conflicting candidate risk provenance")
    return _json_copy(first)


def _source_values(value: Any, *, depth: int = 0) -> Iterable[Any]:
    if depth > 4:
        return ()
    if isinstance(value, Mapping):
        values: list[Any] = []
        for key, child in value.items():
            normalized = str(key).strip().lower().replace("-", "_")
            if normalized in {
                "source", "source_type", "historical_source_type",
                "historical_split_source_type", "data_source",
            }:
                values.append(child)
            if normalized in {"provenance", "metadata", "metadata_provenance", "snapshot"}:
                values.extend(_source_values(child, depth=depth + 1))
        return values
    if isinstance(value, (list, tuple)):
        result: list[Any] = []
        for child in value[:32]:
            result.extend(_source_values(child, depth=depth + 1))
        return result
    return ()


def _validate_input_provenance(row: Mapping[str, Any]) -> None:
    for value in _source_values(row):
        source = _text(value).upper()
        if (
            source in {"HISTORICAL", "PAPER_FORWARD", "PRIVATE", "LIVE", "TESTNET", "UNKNOWN"}
            or any(marker in source for marker in ("HISTORICAL", "PAPER_FORWARD", "PRIVATE", "LIVE", "TESTNET", "UNKNOWN"))
        ):
            raise ShadowAssessmentError(f"shadow input source is not current: {source}")

    def synthetic(value: Any, depth: int = 0) -> bool:
        if depth > 4:
            return False
        if isinstance(value, Mapping):
            for key, child in value.items():
                normalized = str(key).strip().lower().replace("-", "_")
                if normalized in {"synthetic", "synthetic_fixture", "is_synthetic", "fixture"}:
                    if (isinstance(child, bool) and child) or (
                        isinstance(child, str) and child.strip().lower() in {"1", "true", "yes", "on"}
                    ):
                        return True
                if isinstance(child, (Mapping, list, tuple)) and synthetic(child, depth + 1):
                    return True
        elif isinstance(value, (list, tuple)):
            return any(synthetic(child, depth + 1) for child in value[:32])
        return False

    if synthetic(row):
        raise ShadowAssessmentError("shadow input synthetic provenance is not permitted")


def _declared_lookback(strategy: Mapping[str, Any]) -> int:
    params = strategy.get("parameters", {})
    params = params if isinstance(params, Mapping) else {}
    raw = params.get("lookback", params.get("window", 1))
    if isinstance(raw, bool) or not isinstance(raw, int) or raw < 1 or raw > _MAX_LOOKBACK:
        raise ShadowAssessmentError("strategy lookback is out of bounds")
    return raw


def _candidate_documents(payload: Mapping[str, Any], config: Mapping[str, Any] | None = None) -> tuple[Mapping[str, Any], Mapping[str, Any]]:
    strategy_names = ("strategy_document", "strategy", "strategy_definition")
    model_names = ("model_document", "model")
    strategy_values = _declared_values(payload, strategy_names)
    model_values = _declared_values(payload, model_names)
    if isinstance(config, Mapping):
        strategy_values.extend(value for name in strategy_names if (value := config.get(name)) not in (None, ""))
        model_values.extend(value for name in model_names if (value := config.get(name)) not in (None, ""))
    strategy_mappings = [value for value in strategy_values if isinstance(value, Mapping)]
    model_mappings = [value for value in model_values if isinstance(value, Mapping)]
    if strategy_values and len(strategy_mappings) != len(strategy_values):
        raise ShadowAssessmentError("frozen strategy provenance is invalid")
    if model_values and len(model_mappings) != len(model_values):
        raise ShadowAssessmentError("frozen model provenance is invalid")
    if not strategy_mappings or not model_mappings:
        return (
            _mapping(strategy_mappings[0] if strategy_mappings else None, "frozen strategy document"),
            _mapping(model_mappings[0] if model_mappings else None, "frozen model document"),
        )
    if any(_canonical(value) != _canonical(strategy_mappings[0]) for value in strategy_mappings[1:]):
        raise ShadowAssessmentError("conflicting frozen strategy provenance")
    if any(_canonical(value) != _canonical(model_mappings[0]) for value in model_mappings[1:]):
        raise ShadowAssessmentError("conflicting frozen model provenance")
    return strategy_mappings[0], model_mappings[0]
def _candidate_exit(payload: Mapping[str, Any], setup: Mapping[str, Any], config: Mapping[str, Any]) -> dict[str, Any]:
    values = _declared_values(payload, ("exit_policy", "exit"))
    if not values and isinstance(config, Mapping):
        values = [
            config[name]
            for name in ("exit_policy", "exit")
            if config.get(name) not in (None, "")
        ]
    if not values:
        raise ShadowAssessmentError("frozen exit policy is required")
    policies: list[dict[str, Any]] = []
    for value in values:
        if isinstance(value, str):
            value = {"type": value}
        policy = dict(_mapping(value, "frozen exit policy"))
        kind = _text(policy.get("type", policy.get("kind"))).lower()
        if kind != "fixed_holding_period":
            raise ShadowAssessmentError("shadow assessment requires fixed_holding_period exit policy")
        raw_period = policy.get("holding_period", policy.get("observations", policy.get("bars")))
        if isinstance(raw_period, bool) or type(raw_period) is not int:
            raise ShadowAssessmentError("exit holding period must be a positive integer")
        if raw_period < 1 or raw_period > 100_000:
            raise ShadowAssessmentError("exit holding period is out of bounds")
        policy["type"] = "fixed_holding_period"
        policy["holding_period"] = raw_period
        policy.pop("kind", None)
        policy.pop("observations", None)
        policy.pop("bars", None)
        policies.append(policy)
    first = policies[0]
    if any(_canonical(value) != _canonical(first) for value in policies[1:]):
        raise ShadowAssessmentError("conflicting frozen exit policy provenance")
    return first


def _cost_provenance(payload: Mapping[str, Any], setup: Mapping[str, Any], config: Mapping[str, Any]) -> dict[str, Any]:
    values = _declared_values(
        payload,
        ("cost_provenance", "cost_assumptions", "paper_assumptions", "book_assumptions"),
    )
    if not values and isinstance(config, Mapping):
        values = [
            config[name]
            for name in ("cost_provenance", "cost_assumptions", "paper_assumptions", "book_assumptions")
            if config.get(name) not in (None, "")
        ]
    mappings = [value for value in values if isinstance(value, Mapping)]
    if values and len(mappings) != len(values):
        raise ShadowAssessmentError("frozen cost provenance is invalid")
    if not mappings:
        raise ShadowAssessmentError("frozen cost provenance is required")
    for index, value in enumerate(values):
        _validate_raw_cost_aliases(value, f"frozen.costs[{index}]")
    result = _json_copy(mappings[0])
    setup_costs = setup.get("book_assumptions")
    if setup_costs is not None and not isinstance(setup_costs, Mapping):
        raise ShadowAssessmentError("operational setup book assumptions are invalid")
    if isinstance(setup_costs, Mapping):
        _validate_raw_cost_aliases(setup_costs, "frozen.setup.book_assumptions")
        # Recorded provider assumptions are authoritative.  A candidate may
        # repeat them, but may not replace them with a different value.
        for key in ("fee_bps", "fees_bps", "fee_rate", "slippage_bps", "max_slippage_bps", "slippage"):
            if key in result and key in setup_costs and _canonical(result[key]) != _canonical(setup_costs[key]):
                raise ShadowAssessmentError("frozen cost provenance conflicts with operational setup")
        result = {**dict(result), **dict(setup_costs)}

    def number(*names: str) -> float | None:
        for name in names:
            current = result.get(name)
            if isinstance(current, Mapping):
                current = _first(current, name, "value")
            if current in (None, "") or isinstance(current, bool):
                continue
            try:
                parsed = float(current)
            except (TypeError, ValueError, OverflowError):
                continue
            if math.isfinite(parsed) and parsed >= 0:
                return parsed
        return None

    fee = number("fee_bps", "fees_bps", "fee_rate")
    slippage = number("slippage_bps", "max_slippage_bps", "slippage")
    if fee is None or slippage is None:
        raise ShadowAssessmentError("frozen cost provenance must include finite fee_bps and slippage_bps")
    result["fee_bps"] = fee
    result["slippage_bps"] = slippage
    result.setdefault("fees", {"model": "proportional", "fee_bps": str(fee)})
    result.setdefault("slippage", {"model": "proportional", "slippage_bps": str(slippage)})
    if not isinstance(result.get("fees"), Mapping) or not isinstance(result.get("slippage"), Mapping):
        raise ShadowAssessmentError("frozen cost provenance fees and slippage must be mappings")
    result.setdefault("sizing", {"model": "fixed_allocated_capital", "allocated_capital": "100"})
    result["version"] = _text(result.get("version")) or "paper-assumptions-v1"
    return result


def _risk_number(value: Any, name: str) -> float:
    if isinstance(value, bool):
        raise ShadowAssessmentError(f"invalid candidate risk limit: {name}")
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        raise ShadowAssessmentError(f"invalid candidate risk limit: {name}") from None
    if not math.isfinite(number) or number < 0:
        raise ShadowAssessmentError(f"invalid candidate risk limit: {name}")
    return number


def _risk_limits(snapshot: Mapping[str, Any], fallback: Mapping[str, Any] | Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    if not isinstance(snapshot, Mapping):
        raise ShadowAssessmentError("CURRENT risk settings are unavailable")
    if isinstance(fallback, Mapping):
        fallbacks: tuple[Mapping[str, Any], ...] = (fallback,)
    elif isinstance(fallback, Sequence) and not isinstance(fallback, (str, bytes)):
        fallbacks = tuple(fallback)
    else:
        raise ShadowAssessmentError("candidate risk provenance is invalid")
    if not fallbacks or any(not isinstance(item, Mapping) for item in fallbacks):
        raise ShadowAssessmentError("candidate risk provenance is invalid")
    active = snapshot.get("effective_limits")
    if not isinstance(active, Mapping):
        raise ShadowAssessmentError("CURRENT risk settings limits are invalid")
    known = {
        "max_order_notional", "max_account_exposure", "max_market_exposure",
        "max_strategy_exposure", "max_group_exposure", "max_loss", "max_expected_loss",
        "max_drawdown", "max_daily_loss", "min_liquidity", "max_spread", "crash_threshold",
        "cooldown_seconds", "max_position_fraction", "kelly_fraction", "max_cvar",
    }
    aliases: dict[str, tuple[str, ...]] = {
        "max_order_notional": ("max_order_notional", "max_all_in_buy_usd", "target_notional_usd"),
        "max_account_exposure": ("max_account_exposure", "max_aggregate_exposure_usd", "max_exposure_usd"),
        "max_loss": ("max_loss", "max_daily_loss_usd", "realized_loss_entry_stop_usd", "equity_loss_entry_stop_usd"),
        "max_spread": ("max_spread",),
        "max_daily_loss": ("max_daily_loss",),
    }
    active_values: dict[str, float] = {}
    for target, names in aliases.items():
        candidates = [active[name] for name in names if name in active and active[name] not in (None, "")]
        if target == "max_spread":
            bps = [active[name] for name in ("max_slippage_bps", "slippage_bps") if name in active and active[name] not in (None, "")]
            candidates.extend(float(_risk_number(value, "max_slippage_bps")) / 10_000.0 for value in bps)
        parsed = [_risk_number(value, target) for value in candidates]
        if parsed:
            active_values[target] = min(parsed)
    for key in known:
        if key in active and active[key] not in (None, "") and key not in active_values:
            active_values[key] = _risk_number(active[key], key)

    candidate_values: dict[str, float] = {}
    candidate_aliases = {
        **{key: (key,) for key in known},
        "max_order_notional": ("max_order_notional", "max_all_in_buy_usd", "max_all_in_buy", "target_notional_usd"),
        "max_account_exposure": ("max_account_exposure", "max_aggregate_exposure_usd", "max_exposure_usd"),
        "max_loss": ("max_loss", "max_daily_loss_usd", "realized_loss_entry_stop_usd", "equity_loss_entry_stop_usd"),
        "max_spread": ("max_spread", "max_spread_bps", "max_slippage_bps", "slippage_bps"),
    }
    reverse: dict[str, str] = {
        name: target for target, names in candidate_aliases.items() for name in names
    }
    for fallback_item in fallbacks:
        for name, value in fallback_item.items():
            target = reverse.get(str(name).strip().lower())
            if target is None or value in (None, ""):
                continue
            parsed = _risk_number(value, str(name))
            if target == "max_spread" and str(name).strip().lower().endswith("_bps"):
                parsed /= 10_000.0
            candidate_values[target] = min(candidate_values.get(target, parsed), parsed)

    result: dict[str, Any] = {}
    # Candidate limits can only tighten an active fence.  Both rejected
    # members participate in the intersection; member order is irrelevant.
    for key, value in active_values.items():
        result[key] = min(value, candidate_values[key]) if key in candidate_values else value
    return result


def _declared_hash(payload: Mapping[str, Any], name: str) -> str:
    values = _declared_values(payload, (name,))
    if not values:
        raise ShadowAssessmentError(f"missing frozen {name}")
    text_values: list[str] = []
    for value in values:
        if not isinstance(value, str) or not value.strip():
            raise ShadowAssessmentError(f"frozen {name} must be a non-empty string")
        text_values.append(value.strip())
    expected = text_values[0]
    if any(value != expected for value in text_values[1:]):
        raise ShadowAssessmentError(f"conflicting frozen {name} provenance")
    return expected


def _rejection_evidence(candidate_id: str, payload: Mapping[str, Any]) -> dict[str, Any]:
    keys = (
        "rejection_reason", "rejected_from", "reason_code", "rejection_evidence",
        "forward_evidence", "validation_evidence", "historical_evidence",
    )
    evidence: dict[str, Any] = {}
    has_provenance = False
    for key in keys:
        values = _declared_values(payload, (key,))
        if not values:
            continue
        has_provenance = True
        first = values[0]
        if any(_canonical(value) != _canonical(first) for value in values[1:]):
            raise ShadowAssessmentError(f"conflicting rejection provenance: {key}")
        evidence[key] = _json_copy(first)
    evidence["candidate_id"] = candidate_id
    evidence["stage"] = "REJECTED"
    evidence["has_rejection_provenance"] = has_provenance
    return evidence


def _event_stage(event: Mapping[str, Any], name: str) -> str:
    return _text(event.get(name)).upper()


def _event_mapping(event: Mapping[str, Any]) -> Mapping[str, Any]:
    payload = event.get("payload")
    return payload if isinstance(payload, Mapping) else {}


def _authoritative_frozen_forward_proof(
    store: Any,
    candidate_id: str,
    payload: Mapping[str, Any],
    events: Sequence[Mapping[str, Any]],
) -> Mapping[str, Any]:
    """Require a lifecycle freeze event bound to the persisted forward row."""
    frozen_events = [
        event
        for event in events
        if _event_stage(event, "to_stage") == "FROZEN"
    ]
    if not frozen_events:
        raise ShadowAssessmentError("authoritative frozen lifecycle proof is missing")
    frozen_event = frozen_events[-1]
    if _event_stage(frozen_event, "from_stage") != "ROBUSTNESS_CHECKED":
        raise ShadowAssessmentError("frozen lifecycle chain is invalid")
    frozen_payload = _event_mapping(frozen_event)
    if frozen_payload.get("frozen") is not True:
        raise ShadowAssessmentError("frozen lifecycle evidence is invalid")

    def optional_hash(name: str) -> str | None:
        values = _declared_values(payload, (name,))
        if not values:
            return None
        if any(not isinstance(value, str) or not value.strip() for value in values):
            raise ShadowAssessmentError(f"frozen {name} must be a non-empty string")
        first = values[0].strip()
        if any(value.strip() != first for value in values[1:]):
            raise ShadowAssessmentError(f"conflicting frozen {name} provenance")
        return first

    event_hashes = {
        name: _declared_hash(frozen_payload, name)
        for name in ("strategy_hash", "model_hash", "config_hash")
    }
    strategy_hash = optional_hash("strategy_hash")
    model_hash = optional_hash("model_hash")
    config_hash = optional_hash("config_hash")
    if (
        strategy_hash is not None
        and strategy_hash != event_hashes["strategy_hash"]
    ) or (
        model_hash is not None
        and model_hash != event_hashes["model_hash"]
    ) or (
        config_hash is not None
        and config_hash != event_hashes["config_hash"]
    ):
        raise ShadowAssessmentError("frozen lifecycle hashes do not match candidate")
    strategy_hash = strategy_hash or event_hashes["strategy_hash"]
    model_hash = model_hash or event_hashes["model_hash"]
    config_hash = config_hash or event_hashes["config_hash"]
    expected_frozen_hash = hashlib.sha256(
        "|".join((strategy_hash, model_hash, config_hash)).encode()
    ).hexdigest()
    frozen_hash_values = _declared_values(frozen_payload, ("frozen_hash",))
    if frozen_hash_values and any(
        not isinstance(value, str) or value.strip() != expected_frozen_hash
        for value in frozen_hash_values
    ):
        raise ShadowAssessmentError("frozen lifecycle hash is invalid")

    candidate_config_values = _declared_values(payload, ("forward_config", "config"))
    candidate_config = (
        _candidate_config(payload) if candidate_config_values else None
    )
    candidate_risk_values = _declared_values(
        payload, ("risk_limits", "risk_snapshot", "candidate_risk_limits")
    )
    candidate_risk = (
        _candidate_risk_limits(payload, candidate_config or {})
        if candidate_risk_values
        else None
    )
    risk_values = _declared_values(frozen_payload, ("risk_snapshot",))
    if not risk_values or any(not isinstance(value, Mapping) for value in risk_values):
        raise ShadowAssessmentError("frozen lifecycle risk snapshot is missing")
    frozen_risk = risk_values[0]
    if any(_canonical(value) != _canonical(frozen_risk) for value in risk_values[1:]):
        raise ShadowAssessmentError("conflicting frozen lifecycle risk snapshot")
    if candidate_risk is not None and _canonical(candidate_risk) != _canonical(frozen_risk):
        raise ShadowAssessmentError("frozen lifecycle risk snapshot does not match candidate")

    forward_ids = _declared_values(
        payload,
        ("forward_test_id", "forward_id", "experiment_id"),
    )
    if any(not isinstance(value, str) or not value.strip() for value in forward_ids):
        raise ShadowAssessmentError("frozen forward identifier must be a non-empty string")
    try:
        loader = getattr(store, "load_forward_tests")
        records = loader(limit=10_000)
    except Exception as exc:
        raise ShadowAssessmentError("authoritative frozen forward proof is unavailable") from exc
    if not isinstance(records, Sequence) or isinstance(records, (str, bytes)):
        raise ShadowAssessmentError("authoritative frozen forward proof is unavailable")

    matches: list[Mapping[str, Any]] = []
    for record in records:
        if not isinstance(record, Mapping):
            continue
        experiment_id = record.get("experiment_id")
        if not isinstance(experiment_id, str) or not experiment_id.strip():
            continue
        if forward_ids and experiment_id.strip() not in {
            value.strip() for value in forward_ids
        }:
            continue
        record_config = record.get("config")
        record_risk = record.get("risk_limits")
        if not isinstance(record_config, Mapping) or not isinstance(record_risk, Mapping):
            continue
        record_candidate_id = record_config.get("candidate_id")
        if (
            not isinstance(record_candidate_id, str)
            or record_candidate_id.strip() != candidate_id
        ):
            continue
        if _text(record.get("quality")).upper() != "PAPER_FORWARD":
            continue
        record_strategy_hash = record.get("strategy_hash")
        record_model_hash = record.get("model_hash")
        if (
            not isinstance(record_strategy_hash, str)
            or not isinstance(record_model_hash, str)
            or record_strategy_hash.strip() != strategy_hash
            or record_model_hash.strip() != model_hash
        ):
            continue
        if candidate_config is not None and _canonical(record_config) != _canonical(candidate_config):
            continue
        if _canonical(record_risk) != _canonical(frozen_risk):
            continue
        if _content_hash({"config": record_config, "risk_limits": record_risk}) != config_hash:
            continue
        matches.append(record)
    if len(matches) != 1:
        raise ShadowAssessmentError("authoritative frozen forward proof is ambiguous")
    return matches[0]

def _projection_mode(
    store: Any,
    candidate_id: str,
    record: Mapping[str, Any],
    payload: Mapping[str, Any],
    events: Sequence[Mapping[str, Any]] | None,
) -> tuple[str, Mapping[str, Any] | None]:
    """Classify only from the persisted lifecycle chain, never payload reasons."""
    if not isinstance(events, Sequence) or isinstance(events, (str, bytes)) or not events:
        raise ShadowAssessmentError("candidate lifecycle event chain is unavailable")
    normalized_events = [
        event for event in events if isinstance(event, Mapping)
    ]
    if len(normalized_events) != len(events):
        raise ShadowAssessmentError("candidate lifecycle event chain is invalid")
    # Lifecycle rows are authoritative only when they belong to this candidate.
    for event in normalized_events:
        event_candidate_id = event.get("candidate_id")
        if (
            not isinstance(event_candidate_id, str)
            or event_candidate_id.strip() != candidate_id
        ):
            raise ShadowAssessmentError("candidate lifecycle event identity is invalid")
    rejected_events = [
        event
        for event in normalized_events
        if _event_stage(event, "to_stage") == "REJECTED"
    ]
    if len(rejected_events) != 1:
        raise ShadowAssessmentError("candidate lifecycle rejection chain is invalid")
    has_forward_evidence = any(
        _event_stage(event, "to_stage") in _FORWARD_EVIDENCE_STAGES
        for event in normalized_events
    )
    if any(
        _event_stage(event, "to_stage") == "FROZEN"
        for event in normalized_events
    ):
        proof = _authoritative_frozen_forward_proof(
            store, candidate_id, payload, normalized_events
        )
        return "frozen", proof
    if has_forward_evidence:
        raise ShadowAssessmentError("candidate has incomplete frozen lifecycle evidence")
    rejection = rejected_events[0]
    if (
        _event_stage(rejection, "from_stage") != "BACKTESTED"
        or _text(rejection.get("reason")).lower() != "negative_validation_expectancy"
    ):
        raise ShadowAssessmentError(
            "candidate rejection is not an authoritative negative validation expectancy"
        )
    if _declared_values(payload, ("forward_config", "config")):
        raise ShadowAssessmentError("frozen candidate config provenance is ambiguous")
    return "projection", None


def _validate_candidate_unchecked(
    candidate_id: str,
    record: Mapping[str, Any],
    settings: Mapping[str, Any] | None = None,
    *,
    events: Sequence[Mapping[str, Any]] | None = None,
    store: Any | None = None,
) -> dict[str, Any]:
    if not isinstance(candidate_id, str) or not candidate_id.strip():
        raise ShadowAssessmentError("candidate_id must be a non-empty string")
    if not isinstance(record, Mapping):
        raise ShadowAssessmentError("candidate lifecycle record must be a mapping")
    if _text(record.get("stage")).upper() != "REJECTED":
        raise ShadowAssessmentError("shadow assessment accepts terminal REJECTED candidates only")
    payload = _mapping(record.get("payload", {}), "candidate payload")

    declared_ids = _declared_values(payload, ("candidate_id",))
    record_id = record.get("candidate_id")
    if record_id not in (None, ""):
        declared_ids.append(record_id)
    if declared_ids:
        if any(not isinstance(value, str) or not value.strip() for value in declared_ids):
            raise ShadowAssessmentError("candidate identity must be a non-empty string")
        if any(value.strip() != candidate_id.strip() for value in declared_ids):
            raise ShadowAssessmentError("candidate payload identity does not match candidate_id")
    candidate_id = candidate_id.strip()
    mode, frozen_proof = _projection_mode(store, candidate_id, record, payload, events)
    if mode == "projection":
        if not isinstance(settings, Mapping):
            raise ShadowAssessmentError("CURRENT risk settings are required for rejected projection")
        return _validate_rejected_without_config(candidate_id, record, payload, settings)

    def declared_text(names: Sequence[str], label: str) -> str:
        values = _declared_values(payload, names)
        if not values:
            raise ShadowAssessmentError(f"missing frozen {label}")
        normalized: list[str] = []
        for value in values:
            if not isinstance(value, str) or not value.strip():
                raise ShadowAssessmentError(f"frozen {label} must be a non-empty string")
            normalized.append(value.strip())
        first = normalized[0]
        if any(value != first for value in normalized[1:]):
            raise ShadowAssessmentError(f"conflicting frozen {label} provenance")
        return first
    def frozen_declared_text(names: Sequence[str], label: str) -> str:
        values = _declared_values(payload, names)
        if values:
            return declared_text(names, label)
        if frozen_proof is None:
            raise ShadowAssessmentError(f"missing frozen {label}")
        value = frozen_proof.get(names[0])
        if not isinstance(value, str) or not value.strip():
            raise ShadowAssessmentError(f"missing frozen {label}")
        return value.strip()

    def frozen_declared_hash(name: str) -> str:
        values = _declared_values(payload, (name,))
        if values:
            return _declared_hash(payload, name)
        if frozen_proof is None:
            raise ShadowAssessmentError(f"missing frozen {name}")
        value = frozen_proof.get(name)
        if not isinstance(value, str) or not value.strip():
            raise ShadowAssessmentError(f"missing frozen {name}")
        return value.strip()

    if frozen_proof is None:
        raise ShadowAssessmentError("authoritative frozen forward proof is missing")

    setup_value = _one_declared_mapping(payload, ("operational_setup", "setup"), "operational setup")
    setup = dict(_mapping(setup_value, "operational setup"))
    setup_hash = declared_text(("operational_setup_hash", "setup_hash"), "operational setup hash")
    try:
        expected_setup_hash = _operational_setup_hash(setup)
    except Exception as exc:
        raise ShadowAssessmentError("operational setup cannot be hashed") from exc
    if setup_hash != expected_setup_hash:
        raise ShadowAssessmentError("operational setup hash mismatch")
    setup_id = declared_text(("setup_id", "operational_setup_id"), "operational setup id")
    family_values = _declared_values(payload, ("family",))
    family_values.extend(
        value
        for value in (setup.get("family"),)
        if value not in (None, "")
    )
    if not family_values:
        raise ShadowAssessmentError("missing frozen member family")
    if any(not isinstance(value, str) or not value.strip() for value in family_values):
        raise ShadowAssessmentError("member family must be a non-empty string")
    families = {value.strip().lower() for value in family_values}
    if len(families) != 1 or next(iter(families), "") not in _MEMBER_ORDER:
        raise ShadowAssessmentError("operational setup must identify momentum or mean_reversion")
    family = next(iter(families))

    config_values = _declared_values(payload, ("forward_config", "config"))
    config = (
        _candidate_config(payload)
        if config_values
        else _mapping(frozen_proof.get("config"), "authoritative frozen candidate config")
    )
    if not isinstance(config, Mapping):
        raise ShadowAssessmentError("frozen candidate config must be a mapping")
    strategy, model = _candidate_documents(payload, config)
    strategy = dict(_mapping(strategy, "frozen strategy document"))
    model = dict(_mapping(model, "frozen model document"))
    strategy_hash = frozen_declared_hash("strategy_hash")
    model_hash = frozen_declared_hash("model_hash")
    if _content_hash(strategy) != strategy_hash:
        from .forward import _normalized_strategy_document
        try:
            normalized_strategy = _normalized_strategy_document(strategy)
        except Exception as exc:
            raise ShadowAssessmentError("frozen strategy document is invalid") from exc
        if _content_hash(normalized_strategy) != strategy_hash:
            raise ShadowAssessmentError("frozen strategy hash mismatch")
        strategy = dict(normalized_strategy)
    if _content_hash(model) != model_hash:
        raise ShadowAssessmentError("frozen model hash mismatch")
    strategy_family = strategy.get("family")
    if not isinstance(strategy_family, str) or strategy_family.strip().lower() != family:
        raise ShadowAssessmentError("operational setup family and strategy family disagree")

    scope = _candidate_scope(payload, setup, config)
    try:
        scope_policy = normalize_market_scope(scope)
    except (TypeError, ValueError) as exc:
        raise ShadowAssessmentError("frozen market scope is not canonical") from exc
    scope_hash = declared_text(("market_scope_hash", "scope_hash"), "market scope hash")
    if scope_hash != scope_policy.scope_hash:
        raise ShadowAssessmentError("frozen market scope hash mismatch")
    scope_version = declared_text(("market_scope_version", "scope_version"), "market scope version")
    if scope_version != scope_policy.scope_version:
        raise ShadowAssessmentError("frozen market scope version mismatch")

    exit_policy = _candidate_exit(payload, setup, config)
    costs = _cost_provenance(payload, setup, config)
    risk_values = _declared_values(
        payload, ("risk_limits", "risk_snapshot", "candidate_risk_limits")
    )
    risk_limits = (
        _candidate_risk_limits(payload, config)
        if risk_values
        else dict(_mapping(frozen_proof.get("risk_limits"), "authoritative frozen risk limits"))
    )
    config_hash = frozen_declared_hash("config_hash")
    expected_config_hash = _content_hash({"config": config, "risk_limits": risk_limits})
    if config_hash != expected_config_hash:
        raise ShadowAssessmentError("frozen candidate config hash mismatch")
    frozen_hash_values = _declared_values(payload, ("frozen_hash",))
    frozen_hash = (
        _declared_hash(payload, "frozen_hash")
        if frozen_hash_values
        else hashlib.sha256(
            "|".join((strategy_hash, model_hash, config_hash)).encode()
        ).hexdigest()
    )
    expected_frozen_hash = hashlib.sha256(
        "|".join((strategy_hash, model_hash, config_hash)).encode()
    ).hexdigest()
    if frozen_hash != expected_frozen_hash:
        raise ShadowAssessmentError("frozen candidate hash mismatch")

    # Ensure the operational setup is not merely a copied setup from the other family.
    try:
        expected_setup = _operational_setup_for_strategy(
            strategy,
            {**dict(config), "market_scope": scope_policy.as_dict()},
        )
    except Exception:
        raise ShadowAssessmentError("operational setup provenance is invalid") from None
    if not isinstance(expected_setup, Mapping):
        raise ShadowAssessmentError("operational setup provenance is invalid")
    expected_family = expected_setup.get("family")
    expected_id = expected_setup.get("setup_id")
    if (
        not isinstance(expected_family, str)
        or expected_family.strip().lower() != family
        or not isinstance(expected_id, str)
        or expected_id.strip() != setup_id
    ):
        raise ShadowAssessmentError("operational setup does not match persisted strategy")

    rejection_evidence = _rejection_evidence(candidate_id, payload)
    if not rejection_evidence.get("has_rejection_provenance"):
        raise ShadowAssessmentError("missing rejection provenance")
    rejection_evidence.pop("has_rejection_provenance", None)
    historical_values = _declared_values(
        payload,
        ("historical_evidence", "historical_provenance", "historical_support"),
    )
    historical_evidence: Mapping[str, Any] = {}
    if historical_values:
        if any(not isinstance(value, Mapping) for value in historical_values):
            raise ShadowAssessmentError("historical provenance must be a mapping")
        historical_evidence = _json_copy(historical_values[0])
        if any(_canonical(value) != _canonical(historical_evidence) for value in historical_values[1:]):
            raise ShadowAssessmentError("conflicting historical provenance")

    member_identity = _member_id(family, candidate_id)
    return {
        "candidate_id": candidate_id,
        "shadow_member_id": member_identity,
        "family": family,
        "setup": _json_copy(setup),
        "setup_id": setup_id,
        "setup_hash": setup_hash,
        "strategy": _json_copy(strategy),
        "strategy_hash": strategy_hash,
        "model": _json_copy(model),
        "model_hash": model_hash,
        "config": _json_copy(config),
        "config_hash": config_hash,
        "frozen_hash": frozen_hash,
        "scope": scope_policy.as_dict(),
        "scope_hash": scope_policy.scope_hash,
        "scope_version": scope_policy.scope_version,
        "exit_policy": _json_copy(exit_policy),
        "cost_provenance": _json_copy(costs),
        "risk_limits": _json_copy(risk_limits),
        "rejection_evidence": rejection_evidence,
        "historical_evidence": historical_evidence,
    }


def _validate_candidate(
    candidate_id: str,
    record: Mapping[str, Any],
    settings: Mapping[str, Any] | None = None,
    *,
    events: Sequence[Mapping[str, Any]] | None = None,
    store: Any | None = None,
) -> dict[str, Any]:
    try:
        return _validate_candidate_unchecked(
            candidate_id,
            record,
            settings,
            events=events,
            store=store,
        )
    except ShadowAssessmentError:
        raise
    except Exception:
        raise ShadowAssessmentError("candidate provenance is invalid") from None


def _verify_family_semantics(members: Sequence[Mapping[str, Any]]) -> None:
    families = {_text(item.get("family")).lower() for item in members}
    if families != set(_MEMBER_ORDER):
        raise ShadowAssessmentError("shadow assessment requires one momentum and one mean_reversion candidate")
    try:
        rows = max(_declared_lookback(item["strategy"]) for item in members) + 1
    except (KeyError, TypeError):
        raise ShadowAssessmentError("family direction provenance is invalid") from None
    data = {
        "observations": tuple(
            {"market_id": "semantic-check", "yes_mid": 0.35 + (0.01 * index), "timestamp": index}
            for index in range(rows)
        )
    }
    scores: dict[str, float] = {}
    for member in members:
        try:
            result = evaluate_signal_evaluation(member["strategy"], data)
            scores[_text(member.get("family")).lower()] = float(result.score)
        except Exception:
            raise ShadowAssessmentError("family direction provenance is invalid") from None
    if not scores.get("momentum") or not scores.get("mean_reversion") or scores["momentum"] * scores["mean_reversion"] >= 0:
        raise ShadowAssessmentError("momentum and mean_reversion family signs are not opposite")


@dataclass(frozen=True, slots=True)
class _Member:
    member_id: str
    candidate_id: str
    family: str
    setup_id: str
    setup_hash: str
    strategy: Mapping[str, Any]
    strategy_hash: str
    model: Mapping[str, Any]
    model_hash: str
    scope: Mapping[str, Any]
    scope_hash: str
    scope_version: str
    exit_policy: Mapping[str, Any]
    costs: Mapping[str, Any]
    rejection_evidence: Mapping[str, Any]


def _composite_member_input(value: Mapping[str, Any]) -> dict[str, Any]:
    """Normalize legacy aliases at the runtime boundary only."""
    item = dict(_mapping(value, "composite member"))
    aliases = (
        ("shadow_member_id", "member_id"),
        ("setup", "operational_setup"),
        ("strategy", "strategy_document"),
        ("model", "model_document"),
        ("config", "forward_config"),
        ("cost_provenance", "costs"),
    )
    for canonical, legacy in aliases:
        if item.get(canonical) in (None, "") and item.get(legacy) not in (None, ""):
            item[canonical] = item[legacy]
    return item


class ShadowCompositeStrategy:
    """Dispatch persisted evaluators while preserving member identity."""

    def __init__(self, members: Sequence[Mapping[str, Any]], *, portfolio: Portfolio | None = None) -> None:
        normalized_members = tuple(_composite_member_input(item) for item in members)
        self.members = tuple(
            _Member(
                _canonical_member_id(item), _text(item["candidate_id"]), _text(item["family"]).lower(),
                _text(item["setup_id"]), _text(item["setup_hash"]), MappingProxyType(_json_copy(item["strategy"])),
                _text(item["strategy_hash"]), MappingProxyType(_json_copy(item["model"])), _text(item["model_hash"]),
                MappingProxyType(_json_copy(item["scope"])), _text(item["scope_hash"]), _text(item["scope_version"]),
                MappingProxyType(_json_copy(item["exit_policy"])), MappingProxyType(_json_copy(item["cost_provenance"])),
                MappingProxyType(_json_copy(item["rejection_evidence"])),
            )
            for item in normalized_members
        )
        if len(self.members) != 2 or {member.family for member in self.members} != set(_MEMBER_ORDER):
            raise ShadowAssessmentError("composite strategy requires exactly two family members")
        self.portfolio = portfolio
        self.definition = {
            "schema": "axiom-shadow-composite-v1",
            "market_type": "prediction",
            "family": "shadow_composite",
            "members": [
                {
                    "shadow_member_id": member.member_id,
                    "candidate_id": member.candidate_id,
                    "family": member.family,
                    "strategy_hash": member.strategy_hash,
                }
                for member in self.members
            ],
        }
        self.evaluations: list[dict[str, Any]] = []

    def _member(self, observation: Mapping[str, Any]) -> _Member | None:
        identifier = _text(observation.get("shadow_member_id"))
        return next((member for member in self.members if member.member_id == identifier), None)

    @staticmethod
    def _history(context: Mapping[str, Any], member_id: str, market_id: str) -> tuple[Mapping[str, Any], ...]:
        history = context.get("history", ())
        if not isinstance(history, Iterable) or isinstance(history, (str, bytes, Mapping)):
            history = ()
        result = []
        for item in history:
            if not isinstance(item, Mapping):
                continue
            if _text(item.get("shadow_member_id")) != member_id or _text(item.get("market_id")) != market_id:
                continue
            result.append(item)
        return tuple(result)

    def _attributable_inventory(self, member: _Member, market_id: str) -> list[Any]:
        if self.portfolio is None:
            return []
        lots: list[list[Any]] = []
        fills = getattr(self.portfolio, "fills", ())
        for fill in fills or ():
            metadata = getattr(fill, "metadata", {})
            if not isinstance(metadata, Mapping) or _text(metadata.get("shadow_member_id")) != member.member_id:
                continue
            if _text(getattr(fill, "market_id", None) or getattr(fill, "symbol", "")) != market_id:
                continue
            side = _text(getattr(getattr(fill, "side", None), "value", getattr(fill, "side", ""))).lower()
            outcome = _text(metadata.get("outcome", "yes")).lower() or "yes"
            try:
                quantity = float(getattr(fill, "quantity", 0.0))
            except (TypeError, ValueError):
                continue
            if not math.isfinite(quantity) or quantity <= 0:
                continue
            if side == "buy":
                lots.append([fill, quantity, outcome])
            elif side == "sell":
                remaining = quantity
                for lot in lots:
                    if remaining <= 1e-12 or lot[2] != outcome:
                        continue
                    consumed = min(remaining, lot[1])
                    lot[1] -= consumed
                    remaining -= consumed
        result: list[Any] = []
        for fill, quantity, _outcome in lots:
            if quantity <= 1e-12:
                continue
            try:
                result.append(replace(fill, quantity=quantity))
            except (TypeError, ValueError):
                result.append(fill)
        return result

    def signal(self, context: Mapping[str, Any]) -> Mapping[str, Any] | None:
        observation = context.get("observation")
        if not isinstance(observation, Mapping):
            return None
        member = self._member(observation)
        if member is None:
            return None
        market_id = _text(observation.get("market_id", context.get("symbol")))
        history = self._history(context, member.member_id, market_id)
        signal_input = (*history, observation)
        data: dict[str, Any] = {
            "observations": signal_input,
            "snapshots": signal_input,
            "history": signal_input,
            "market_id": market_id,
            "model_document": dict(member.model),
        }
        evaluation = evaluate_signal_evaluation(member.strategy, data)
        record = evaluation.as_record()
        self.evaluations.append({
            "shadow_member_id": member.member_id,
            "shadow_member_family": member.family,
            "market_id": market_id,
            "timestamp": observation.get("timestamp", observation.get("observed_at")),
            "evaluation": record,
        })
        # A managed SELL is permitted only when a prior, attributable BUY fill
        # exists and the frozen holding-period count has elapsed.
        inventory = self._attributable_inventory(member, market_id)
        if inventory:
            period = int(member.exit_policy.get("holding_period", 1))
            current_time = parse_timestamp(observation.get("timestamp", observation.get("observed_at")))
            if current_time is not None:
                due = [
                    fill for fill in inventory
                    if sum(
                        1 for item in (*history, observation)
                        if parse_timestamp(item.get("timestamp", item.get("observed_at"))) is not None
                        and parse_timestamp(item.get("timestamp", item.get("observed_at"))) > ensure_utc(fill.timestamp)
                    ) >= period
                ]
                if due:
                    fill = due[0]
                    outcome = _text(getattr(fill, "metadata", {}).get("outcome", "yes")).lower() or "yes"
                    return {"side": "sell", "quantity": float(getattr(fill, "quantity", 0.0)), "outcome": outcome}
        if not evaluation.actionable or evaluation.evidence.get("entry_eligible") is False:
            return None
        outcome = _text(evaluation.evidence.get("outcome")).lower()
        if outcome not in {"yes", "no"}:
            outcome = "yes" if evaluation.score > 0 else "no"
        return {"side": f"buy_{outcome}", "quantity": abs(float(evaluation.score)), "outcome": outcome}


class ShadowCompositeModel:
    """Model facade selecting the persisted model by shadow member tag."""

    def __init__(self, members: Sequence[_Member]) -> None:
        self.members = tuple(members)
        self.document = {
            "schema": "axiom-shadow-composite-model-v1",
            "members": [{"shadow_member_id": member.member_id, "model_hash": member.model_hash} for member in members],
        }

    def predict_probability(self, observation: Mapping[str, Any]) -> Mapping[str, Any] | None:
        identifier = _text(observation.get("shadow_member_id")) if isinstance(observation, Mapping) else ""
        member = next((item for item in self.members if item.member_id == identifier), None)
        if member is None:
            return None
        result = evaluate_model_probability_evidence(member.model, observation)
        return result.as_record() if result.probability is not None else None


def _provider_is_public_polymarket(provider: Any) -> bool:
    name = _text(getattr(provider, "provider_name", "")).lower()
    cls = provider.__class__
    return name == "polymarket" or cls.__name__ == "PolymarketAdapter" or (
        cls.__module__.endswith(".polymarket") and "polymarket" in cls.__name__.lower()
    )


def _provider_is_explicit_fixture(provider: Any) -> bool:
    return getattr(provider, "synthetic_fixture", False) is True


def _provider_read_only(provider: Any) -> None:
    if provider is None:
        raise ShadowAssessmentError("public prediction provider is required")
    forbidden = ("submit_order", "place_order", "create_order", "cancel_order", "execute", "send_order", "trade")
    for name in forbidden:
        if callable(getattr(provider, name, None)):
            raise ShadowAssessmentError("shadow provider must expose public read-only methods only")
    if not (_provider_is_public_polymarket(provider) or _provider_is_explicit_fixture(provider)):
        raise ShadowAssessmentError("shadow provider provenance is not a public Polymarket adapter or explicit fixture")
    if not any(callable(getattr(provider, name, None)) for name in ("markets", "market", "price_history")):
        raise ShadowAssessmentError("provider has no public prediction read method")


def _invoke(method: Any, *args: Any, **kwargs: Any) -> Any:
    try:
        return method(*args, **kwargs)
    except TypeError:
        # Test doubles and older adapters often omit optional time bounds.
        if kwargs:
            try:
                return method(*args)
            except TypeError:
                return method()
        raise


def _market_mapping(value: Any) -> dict[str, Any] | None:
    if isinstance(value, PredictionMarketSnapshot):
        record = dict(to_record(value))
        # PredictionMarketSnapshot is the domain's Polymarket record.  The
        # venue identity is not a dataclass field, so retain it at the mapping
        # boundary for canonical market-scope resolution.
        record.setdefault("instrument", "POLYMARKET")
        return record
    if isinstance(value, Mapping):
        return dict(value)
    try:
        record = to_record(value)
    except Exception:
        return None
    return record if isinstance(record, Mapping) else None


def _books(provider: Any, market_id: str) -> Mapping[str, Any]:
    try:
        method = getattr(provider, "order_books", None)
        if callable(method):
            value = _invoke(method, market_id, depth=20)
            if isinstance(value, Mapping):
                return value
        method = getattr(provider, "order_book", None)
        if callable(method):
            value = _invoke(method, market_id, depth=20)
            return {"yes": value} if value is not None else {}
    except Exception:
        return {}
    return {}


def _validate_row_token_aliases(row: Mapping[str, Any], tokens: Mapping[str, str]) -> None:
    """Reject row token declarations that disagree with selected market tokens."""
    expected_yes = _text(tokens.get("yes"))
    expected_no = _text(tokens.get("no"))
    expected = {expected_yes, expected_no}
    if not expected_yes or not expected_no or len(expected) != 2:
        return

    def check(value: Any, expected_value: str) -> None:
        if value not in (None, "") and _text(value) != expected_value:
            raise ShadowAssessmentError("provider row token identity does not match selected market")

    check(row.get("yes_token_id", row.get("yesTokenId")), expected_yes)
    check(row.get("no_token_id", row.get("noTokenId")), expected_no)
    for name in ("token_ids", "clob_token_ids", "clobTokenIds"):
        if name not in row or row[name] in (None, ""):
            continue
        raw = row[name]
        if isinstance(raw, Mapping):
            values = [_text(value) for value in raw.values()]
            for key, value in raw.items():
                normalized_key = _text(key).lower().replace("-", "_")
                if normalized_key in {"yes", "yes_token_id", "yestokenid"}:
                    check(value, expected_yes)
                elif normalized_key in {"no", "no_token_id", "notokenid"}:
                    check(value, expected_no)
        elif isinstance(raw, (list, tuple, set, frozenset)):
            values = [_text(value) for value in raw]
        else:
            raise ShadowAssessmentError("provider row token aliases are invalid")
        if not values or set(values) != expected:
            raise ShadowAssessmentError("provider row token aliases do not match selected market")


def _history_token_id(row: Mapping[str, Any]) -> str:
    return _text(
        _first(
            row,
            "token_id", "tokenId", "tokenID", "asset_id", "assetId", "asset",
            "clob_token_id", "clobTokenId",
        )
    )


def _history_price(row: Mapping[str, Any]) -> Any:
    return _first(row, "yes_mid", "mid", "midpoint", "price", "close", "last")

def _normalize_provider_row(
    value: Any,
    market_id: str,
    tokens: Mapping[str, str] | None,
    *,
    current: bool = False,
    provider_public: bool = False,
    explicit_fixture: bool = False,
) -> dict[str, Any] | None:
    row = _market_mapping(value)
    if row is None:
        return None
    provider_source = _first(row, "source_type", "source")
    try:
        _validate_input_provenance(row)
        if tokens:
            _validate_row_token_aliases(row, tokens)
            for outcome in ("yes", "no"):
                nested = row.get(f"{outcome}_order_book")
                if isinstance(nested, Mapping):
                    _validate_row_token_aliases(nested, tokens)
    except ShadowAssessmentError:
        return None
    if provider_source in (None, ""):
        if not (provider_public or explicit_fixture):
            return None
    elif (
        not explicit_fixture
        and _text(provider_source).upper() not in {"CURRENT", "FORWARD_COLLECTED", "POLYMARKET", "PUBLIC"}
    ):
        return None
    if provider_public:
        for field in ("provider", "venue", "instrument"):
            declared = _text(row.get(field)).lower()
            if declared and declared not in {"polymarket", "polymarket-public", "public", "prediction"}:
                return None
    row["market_id"] = _text(row.get("market_id")) or market_id
    if row["market_id"] != market_id:
        return None
    stamp = parse_timestamp(
        _first(row, "timestamp", "observed_at", "provider_timestamp", "source_timestamp")
    )
    if stamp is None:
        return None
    row["timestamp"] = stamp
    token_id = _history_token_id(row)
    expected_yes = _text((tokens or {}).get("yes"))
    expected_no = _text((tokens or {}).get("no"))
    if token_id and tokens and token_id not in {expected_yes, expected_no}:
        return None
    for outcome, expected in (("yes", expected_yes), ("no", expected_no)):
        nested = row.get(f"{outcome}_order_book")
        nested_token = _history_token_id(nested) if isinstance(nested, Mapping) else ""
        if nested_token and tokens and nested_token != expected:
            return None
    has_yes_mid = row.get("yes_mid") not in (None, "")
    if not has_yes_mid and token_id:
        raw_price = _history_price(row)
        try:
            price = float(raw_price)
        except (TypeError, ValueError, OverflowError):
            return None
        if not math.isfinite(price) or not 0 <= price <= 1:
            return None
        row["yes_mid"] = price if token_id == expected_yes else 1.0 - price
        row["source_token_id"] = token_id
    try:
        yes_mid = float(row.get("yes_mid"))
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(yes_mid) or not 0 <= yes_mid <= 1:
        return None
    row["yes_mid"] = yes_mid
    if provider_source not in (None, ""):
        row["provider_source_type"] = _text(provider_source)
    if provider_public:
        row.setdefault("provider", "polymarket")
        row.setdefault("instrument", "POLYMARKET")
        row.setdefault("venue", "POLYMARKET")
    row["source_type"] = "FORWARD_COLLECTED"
    row["paper_only"] = True
    if not row.get("size_increment"):
        row.setdefault("size_increment", "0.000001")
    if current:
        row["current_snapshot"] = True
    return row


def _rows_for_market(
    provider: Any,
    market_id: str,
    now: datetime,
    cursor: datetime | None,
    tokens: Mapping[str, str] | None = None,
    *,
    max_rows: int = _MAX_PROVIDER_ROWS,
) -> list[dict[str, Any]]:
    max_rows = max(0, min(int(max_rows), _MAX_PROVIDER_ROWS))
    if max_rows == 0:
        return []
    provider_public = _provider_is_public_polymarket(provider)
    explicit_fixture = _provider_is_explicit_fixture(provider)
    rows: list[dict[str, Any]] = []

    def sort_key(item: Mapping[str, Any]) -> tuple[Any, str, Any, str]:
        stamp = parse_timestamp(item.get("timestamp", item.get("observed_at")))
        source_cursor = _source_cursor_value(item)
        boundary_stamp = source_cursor[0] if source_cursor is not None else stamp
        snapshot_id = source_cursor[1] if source_cursor is not None else ""
        return (
            boundary_stamp or datetime.min.replace(tzinfo=now.tzinfo),
            snapshot_id,
            stamp or datetime.min.replace(tzinfo=now.tzinfo),
            _canonical(item),
        )

    def retain(item: dict[str, Any]) -> None:
        # Inclusive providers commonly return the oldest row first.  Keep a
        # bounded newest window rather than stopping at the first page: the
        # caller must be able to discover an unseen snapshot at a timestamp
        # already represented by the durable cursor.
        rows.append(item)
        if len(rows) > max_rows * 2:
            rows.sort(key=sort_key)
            del rows[:-max_rows]

    history_method = getattr(provider, "price_history", None)
    if callable(history_method):
        try:
            value = _invoke(history_method, market_id, start=cursor, end=now)
            if isinstance(value, Iterable) and not isinstance(value, (str, bytes, Mapping)):
                retained_count = 0
                source_row_limit = min(_MAX_PROVIDER_ROWS * 2, max_rows * 2)
                for item in value:
                    normalized = _normalize_provider_row(
                        item, market_id, tokens,
                        provider_public=provider_public,
                        explicit_fixture=explicit_fixture,
                    )
                    if normalized is None:
                        continue
                    boundary = _source_cursor_value(normalized)
                    boundary_stamp = (
                        boundary[0]
                        if boundary is not None
                        else parse_timestamp(normalized.get("timestamp"))
                    )
                    if cursor is not None and boundary_stamp is not None and boundary_stamp < cursor:
                        continue
                    retain(normalized)
                    retained_count += 1
                    if retained_count >= source_row_limit:
                        break
        except Exception:
            # Current market read remains usable; no synthetic history is made.
            pass
    current = None
    market_method = getattr(provider, "market", None)
    if callable(market_method):
        try:
            current = _market_mapping(_invoke(market_method, market_id))
        except Exception:
            current = None
    if current is not None:
        books = _books(provider, market_id)
        if books:
            yes = books.get("yes")
            no = books.get("no")
            if yes is not None:
                current["yes_order_book"] = to_record(yes) if not isinstance(yes, Mapping) else dict(yes)
            if no is not None:
                current["no_order_book"] = to_record(no) if not isinstance(no, Mapping) else dict(no)
        normalized = _normalize_provider_row(
            current, market_id, tokens, current=True,
            provider_public=provider_public,
            explicit_fixture=explicit_fixture,
        )
        if normalized is not None:
            retain(normalized)

    # A token-level history has one yes and one no row for a timestamp.  Merge
    # those rows before the engine sees them so one public observation is counted
    # per market/time, not once per token.
    merged: dict[tuple[str, ...], dict[str, Any]] = {}
    for row in rows:
        stamp = parse_timestamp(row.get("timestamp"))
        if stamp is None:
            continue
        token_id = _text(row.get("source_token_id"))
        source_cursor = _source_cursor_value(row)
        if source_cursor is not None:
            key = (
                stamp.isoformat(),
                source_cursor[0].isoformat(),
                source_cursor[1],
            )
        elif token_id:
            # Token-level rows without a snapshot id still need to merge into
            # one market observation.
            key = (stamp.isoformat(),)
        else:
            # Full market rows without a source id are distinguishable only by
            # their stable value projection; do not collapse unseen snapshots
            # merely because their timestamps match.
            key = (stamp.isoformat(), _source_identity_key(market_id, row))
        previous = merged.get(key)
        if previous is None:
            merged[key] = row
            continue
        if token_id:
            if token_id == _text((tokens or {}).get("yes")):
                previous["yes_mid"] = row["yes_mid"]
            elif token_id == _text((tokens or {}).get("no")):
                previous["no_mid"] = 1.0 - row["yes_mid"]
        for name, value in row.items():
            if value not in (None, "") and previous.get(name) in (None, ""):
                previous[name] = value
    ordered = sorted(merged.values(), key=sort_key)
    return ordered[-max_rows:]

def _tag_observation(row: Mapping[str, Any], *, job_id: str, member_id: str, group_id: str) -> dict[str, Any]:
    value = _json_copy(dict(row))
    value["shadow_job_id"] = job_id
    value["shadow_member_id"] = member_id
    value["shadow_group_id"] = group_id
    value["shadow_assessment"] = True
    value["synthetic_fixture"] = False
    value["paper_only"] = True
    value["live_execution"] = False
    value["source_type"] = "FORWARD_COLLECTED"
    return value


def _source_cursor_value(row: Mapping[str, Any]) -> tuple[datetime, str] | None:
    """Return the durable source boundary carried by a provider observation."""
    stamp = parse_timestamp(
        _first(
            row,
            "source_timestamp",
            "provider_timestamp",
            "timestamp",
            "observed_at",
        )
    )
    snapshot_id = _text(
        _first(
            row,
            "source_snapshot_id",
            "snapshot_id",
            "observation_id",
            "row_id",
            "id",
        )
    )
    if stamp is None or not snapshot_id:
        return None
    return ensure_utc(stamp), snapshot_id


def _source_identity_key(market_id: str, row: Mapping[str, Any]) -> str:
    """Build an identity that excludes cycle-varying shadow metadata."""
    source_cursor = _source_cursor_value(row)
    if source_cursor is not None:
        stamp, snapshot_id = source_cursor
        return _canonical(
            {
                "market_id": market_id,
                "source_timestamp": stamp.isoformat(),
                "source_snapshot_id": snapshot_id,
            }
        )
    stamp = parse_timestamp(
        _first(row, "timestamp", "observed_at", "source_timestamp")
    )
    return _canonical(
        {
            "market_id": market_id,
            "timestamp": stamp.isoformat() if stamp is not None else None,
            "source_timestamp": _first(row, "source_timestamp", "provider_timestamp"),
            "yes_mid": row.get("yes_mid"),
            "no_mid": row.get("no_mid"),
            "settlement": row.get("settlement"),
        }
    )


def _source_cursor_state(value: Any) -> tuple[datetime, str] | None:
    if not isinstance(value, Mapping):
        return None
    snapshot_id = _text(value.get("snapshot_id"))
    stamp = parse_timestamp(value.get("timestamp"))
    if not snapshot_id or stamp is None:
        return None
    return ensure_utc(stamp), snapshot_id

def _safe_stop_value(value: Any, name: str, *, allow_none: bool = True) -> int | datetime | None:
    if value is None and allow_none:
        return None
    if name == "stop_at":
        stamp = parse_timestamp(value)
        if stamp is None:
            raise ShadowAssessmentError("stop_at must be a timestamp")
        return stamp
    if isinstance(value, bool) or type(value) is not int:
        raise ShadowAssessmentError(f"{name} must be a positive bounded integer")
    if value <= 0:
        raise ShadowAssessmentError(f"{name} must be a positive bounded integer")
    maximum = _MAX_CYCLES if name == "max_cycles" else _MAX_PROVIDER_ROWS
    if value > maximum:
        raise ShadowAssessmentError(f"{name} must be a positive bounded integer")
    return value


def _positive_decimal(value: Any, name: str) -> Decimal:
    if isinstance(value, bool) or value is None:
        raise ShadowAssessmentError(f"{name} must be finite and positive")
    try:
        number = value if isinstance(value, Decimal) else Decimal(str(value).strip())
    except (InvalidOperation, TypeError, ValueError):
        raise ShadowAssessmentError(f"{name} must be finite and positive") from None
    if not number.is_finite() or number <= 0:
        raise ShadowAssessmentError(f"{name} must be finite and positive")
    if number > Decimal(str(_MAX_BANKROLL)):
        raise ShadowAssessmentError(f"{name} is out of bounds")
    return number


def _shared_cap_values(source: Any, path: str = "") -> list[tuple[str, Decimal]]:
    if not isinstance(source, Mapping):
        return []
    values: list[tuple[str, Decimal]] = []
    for raw_key, raw_value in source.items():
        key = _text(raw_key)
        normalized = key.lower()
        child_path = f"{path}.{key}" if path else key
        if normalized in _SHARED_CAP_NAMES:
            if raw_value is None:
                continue
            candidate = raw_value
            if isinstance(candidate, Mapping):
                candidate = _first(candidate, "value", "amount", "cap", "limit", "budget", "bankroll")
                if candidate is None:
                    raise ShadowAssessmentError(f"{child_path} must declare a positive cap")
            values.append((child_path, _positive_decimal(candidate, child_path)))
            continue
        if normalized in {"shared", "budget", "shared_budget", "allocation"} and isinstance(raw_value, Mapping):
            values.extend(_shared_cap_values(raw_value, child_path))
    return values

_MEMBER_SHARED_RISK_NAMES = (
    "max_account_exposure",
    "max_account_exposure_usd",
    "aggregate_exposure_usd",
    "max_aggregate_exposure_usd",
    "max_aggregate_exposure",
    "max_exposure_usd",
    "aggregate_open_cost_usd",
    "max_aggregate_open_cost_usd",
    "max_aggregate_open_cost",
    "max_open_cost_usd",
)
_MEMBER_ALLOCATION_NAMES = (
    "allocated_capital",
    "allocated_capital_usd",
    "member_allocated_capital",
    "member_allocated_capital_usd",
    "max_allocated_capital",
    "max_allocated_capital_usd",
)

_ALLOCATION_ALIAS_NAMES = frozenset(
    {
        *(_name.lower() for _name in _SHARED_CAP_NAMES),
        "allocation",
        "allocation_usd",
        *(_name.lower() for _name in _MEMBER_ALLOCATION_NAMES),
    }
)


def _named_positive_values(
    source: Mapping[str, Any],
    names: Sequence[str],
    path: str,
) -> list[tuple[str, Decimal]]:
    result: list[tuple[str, Decimal]] = []
    wanted = {name.lower() for name in names}
    for raw_key, raw_value in source.items():
        key = _text(raw_key)
        if key.lower() not in wanted:
            continue
        candidate = raw_value
        if isinstance(candidate, Mapping):
            candidate = _first(candidate, "value", "amount", "cap", "limit", "budget", "bankroll")
        result.append((f"{path}.{key}", _positive_decimal(candidate, f"{path}.{key}")))
    return result


def _member_budget_values(
    member: Mapping[str, Any],
    index: int,
) -> tuple[list[tuple[str, Decimal]], list[tuple[str, Decimal]]]:
    risk_values: list[tuple[str, Decimal]] = []
    allocation_values: list[tuple[str, Decimal]] = []
    risk = member.get("risk_limits")
    if isinstance(risk, Mapping):
        risk_values.extend(
            _named_positive_values(
                risk,
                _MEMBER_SHARED_RISK_NAMES,
                f"members[{index}].risk_limits",
            )
        )
    costs = member.get("cost_provenance")
    if isinstance(costs, Mapping):
        allocation_values.extend(
            _named_positive_values(
                costs,
                _MEMBER_ALLOCATION_NAMES,
                f"members[{index}].cost_provenance",
            )
        )
        risk_values.extend(
            _named_positive_values(
                costs,
                _MEMBER_SHARED_RISK_NAMES,
                f"members[{index}].cost_provenance",
            )
        )
        for section in ("exposure", "risk", "limits"):
            nested = costs.get(section)
            if isinstance(nested, Mapping):
                risk_values.extend(
                    _named_positive_values(
                        nested,
                        _MEMBER_SHARED_RISK_NAMES,
                        f"members[{index}].cost_provenance.{section}",
                    )
                )
        sizing = costs.get("sizing")
        if isinstance(sizing, Mapping):
            allocation_values.extend(
                _named_positive_values(
                    sizing,
                    _MEMBER_ALLOCATION_NAMES,
                    f"members[{index}].cost_provenance.sizing",
                )
            )
    return risk_values, allocation_values


def _shadow_budget_cap(settings: Mapping[str, Any], members: Sequence[Mapping[str, Any]]) -> tuple[Decimal, list[dict[str, str]]]:
    limits = settings.get("effective_limits")
    if not isinstance(limits, Mapping):
        raise ShadowAssessmentError("CURRENT risk settings limits are unavailable")

    def required(names: Sequence[str], label: str) -> list[tuple[str, Decimal]]:
        found = [
            (name, _positive_decimal(limits[name], label))
            for name in names
            if name in limits
        ]
        if not found:
            raise ShadowAssessmentError(f"CURRENT risk settings must declare {label}")
        return found

    def append_required(names: Sequence[str], label: str) -> None:
        cap_values.extend(required(names, label))


    sources: list[dict[str, str]] = []
    cap_values: list[tuple[str, Decimal]] = []
    for names, label in (
        (
            ("max_aggregate_open_cost_usd", "aggregate_open_cost_usd", "max_aggregate_open_cost", "max_open_cost_usd"),
            "max_aggregate_open_cost",
        ),
        (
            (
                "max_aggregate_exposure_usd",
                "aggregate_exposure_usd",
                "max_aggregate_exposure",
                "max_exposure_usd",
                "max_account_exposure",
                "max_account_exposure_usd",
            ),
            "max_aggregate_exposure",
        ),
    ):
        append_required(names, label)

    # Daily and cumulative fences are shared budgets when declared.  Every
    # populated alias participates so a looser spelling cannot bypass a
    # tighter CURRENT setting.
    for names in (
        ("max_gross_daily_buy_usd", "daily_buy_cap_usd", "gross_daily_buy_usd"),
        ("cumulative_buy_cap_usd", "max_cumulative_buy_usd", "cumulative_buy_usd"),
    ):
        for name in names:
            if name in limits and limits[name] not in (None, ""):
                cap_values.append((name, _positive_decimal(limits[name], name)))

    declared_sources: list[tuple[str, Decimal]] = []
    declared_sources.extend(_shared_cap_values(limits, "effective_limits"))
    declared_sources.extend(_shared_cap_values(settings, "settings"))
    for name in ("active", "config", "settings", "values"):
        value = settings.get(name)
        if isinstance(value, Mapping):
            declared_sources.extend(_shared_cap_values(value, f"settings.{name}"))
    for index, member in enumerate(members):
        risk_values, allocation_values = _member_budget_values(member, index)
        # max_account_exposure is a shared RiskEngine fence even when the
        # rejected candidate declares it in member-local risk provenance.
        cap_values.extend(risk_values)
        # Each member receives exactly half of the one shared wallet.  Preserve
        # every frozen allocation ceiling without summing a smaller member away.
        cap_values.extend(
            (f"{name}*2", value * Decimal("2"))
            for name, value in allocation_values
        )
        declared_sources.extend(
            _shared_cap_values(member.get("risk_limits"), f"members[{index}].risk_limits")
        )
        declared_sources.extend(
            _shared_cap_values(member.get("cost_provenance"), f"members[{index}].cost_provenance")
        )

    # Frozen shared_budget/shared_cap declarations are additional bankroll
    # fences, not merely provenance.  Include every declared alias so no
    # alternate declaration can bypass the tightest shared ceiling.
    cap_values.extend(declared_sources)
    cap = min(value for _, value in cap_values)
    for name, value in cap_values:
        sources.append({"name": name, "value": format(value, "f")})
    return cap, sources


def _status_report(store: Any, clock: Any, now: datetime) -> tuple[str, Mapping[str, Any]]:
    try:
        from .canary import CanaryService
        report = CanaryService(store, clock=clock, initialize=False).status_report()
    except Exception as exc:
        return "UNKNOWN", {"status": "UNKNOWN", "blocker": f"CANARY_STATUS_UNAVAILABLE:{exc}"}
    control = report.get("control") if isinstance(report, Mapping) else None
    state = _text(control.get("state")) if isinstance(control, Mapping) else _text(report.get("control_state"))
    return state.upper() or "UNKNOWN", report if isinstance(report, Mapping) else {}


class ShadowAssessmentService:
    """Immutable two-member paper assessment coordinator."""

    def __init__(self, store: Any, *, clock: Any = utc_now, max_markets: int = 100, max_observations: int = 1_000) -> None:
        if store is None:
            raise TypeError("store is required")
        if isinstance(max_markets, bool) or not isinstance(max_markets, int) or not 0 < max_markets <= _MAX_MARKETS:
            raise ValueError("max_markets must be in [1,1000]")
        if isinstance(max_observations, bool) or not isinstance(max_observations, int) or not 0 < max_observations <= _MAX_PROVIDER_ROWS:
            raise ValueError("max_observations is out of bounds")
        self.store = store
        self.clock = clock
        self.max_markets = max_markets
        self.max_observations = max_observations

    def _load_candidates(
        self,
        candidate_ids: Sequence[str],
        settings: Mapping[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        result = []
        event_loader = getattr(self.store, "list_candidate_lifecycle_events", None)
        if not callable(event_loader):
            raise ShadowAssessmentError("candidate lifecycle event chain is unavailable")
        for candidate_id in candidate_ids:
            record = self.store.load_candidate_lifecycle(candidate_id)
            if not isinstance(record, Mapping):
                raise ShadowAssessmentError(f"candidate does not exist: {candidate_id}")
            try:
                events = event_loader(candidate_id, limit=256)
            except Exception as exc:
                raise ShadowAssessmentError("candidate lifecycle event chain is unavailable") from exc
            result.append(
                _validate_candidate(
                    candidate_id,
                    record,
                    settings,
                    events=events,
                    store=self.store,
                )
            )
        return result

    def register(
        self,
        candidate_ids: Sequence[str],
        *,
        bankroll: float | None = None,
        max_cycles: int | None = None,
        max_observations: int | None = None,
        stop_at: datetime | None = None,
        now: datetime | None = None,
    ) -> Mapping[str, Any]:
        if isinstance(candidate_ids, (str, bytes)) or not isinstance(candidate_ids, Sequence) or len(candidate_ids) != 2:
            raise ShadowAssessmentError("shadow registration requires exactly two candidate IDs")
        identifiers = tuple(_text(item) for item in candidate_ids)
        if any(not item for item in identifiers) or len(set(identifiers)) != 2:
            raise ShadowAssessmentError("shadow registration requires two distinct candidate IDs")
        if max_cycles is None and max_observations is None and stop_at is None:
            raise ShadowAssessmentError(
                "shadow registration requires at least one stop bound "
                "(max_cycles, max_observations, or stop_at)"
            )
        current = ensure_utc(now or self.clock())
        cycle_limit = _safe_stop_value(max_cycles, "max_cycles")
        observation_limit = _safe_stop_value(max_observations, "max_observations")
        stop_stamp = _safe_stop_value(stop_at, "stop_at")
        if isinstance(stop_stamp, datetime) and ensure_utc(stop_stamp) <= current:
            raise ShadowAssessmentError("stop_at must be in the future")
        settings = CanarySettingsService(self.store, clock=self.clock, initialize=False).snapshot(now=current)
        if _text(settings.get("status")).upper() != "CURRENT" or not settings.get("settings_available"):
            raise ShadowAssessmentError("CURRENT risk settings are unavailable")
        members = self._load_candidates(identifiers, settings)
        members.sort(key=lambda item: (_MEMBER_ORDER.get(item["family"], 99), item["candidate_id"]))
        _verify_family_semantics(members)
        if _canonical(members[0]["scope"]) != _canonical(members[1]["scope"]):
            raise ShadowAssessmentError("shadow members must share the exact frozen market scope")
        if _canonical(members[0]["exit_policy"]) != _canonical(members[1]["exit_policy"]):
            raise ShadowAssessmentError("shadow members must share the exact frozen exit policy")
        if _canonical(members[0]["cost_provenance"]) != _canonical(members[1]["cost_provenance"]):
            raise ShadowAssessmentError("shadow members must share the exact frozen cost provenance")
        settings_identity = {key: settings.get(key) for key in ("config_id", "generation", "config_hash")}
        budget_cap, cap_sources = _shadow_budget_cap(settings, members)
        if bankroll is None:
            bankroll_decimal = budget_cap
            budget_source = "derived"
        else:
            bankroll_decimal = _positive_decimal(bankroll, "bankroll")
            if bankroll_decimal > budget_cap:
                raise ShadowAssessmentError("bankroll exceeds CURRENT shared risk cap")
            budget_source = "explicit"
        bankroll_value = float(bankroll_decimal)
        if not math.isfinite(bankroll_value) or bankroll_value <= 0 or bankroll_value > _MAX_BANKROLL:
            raise ShadowAssessmentError("bankroll must be finite, positive, and bounded")
        bankroll_exact = format(bankroll_decimal, "f")
        shared_budget = {
            "bankroll": bankroll_value,
            "bankroll_exact": bankroll_exact,
            "source": budget_source,
            "bankroll_source": budget_source,
            "cap": format(budget_cap, "f"),
            "cap_sources": cap_sources,
            "settings_identity": _json_copy(settings_identity),
        }
        # The job identity excludes wall-clock time but includes the exact
        # immutable budget and CURRENT settings fence.
        identity_members: list[dict[str, Any]] = []
        for item in members:
            member_identity = {
                key: item[key]
                for key in (
                    "family",
                    "setup_id",
                    "setup_hash",
                    "strategy_hash",
                    "model_hash",
                    "scope_hash",
                    "scope_version",
                    "exit_policy",
                    "cost_provenance",
                )
            }
            if "projection_provenance" in item:
                member_identity["projection_provenance"] = item["projection_provenance"]
            identity_members.append(member_identity)
        identity = {
            "schema": SHADOW_SCHEMA,
            "candidate_ids": [item["candidate_id"] for item in members],
            "members": identity_members,
            "shared_budget": shared_budget,
            "max_cycles": cycle_limit,
            "max_observations": observation_limit,
            "stop_at": stop_stamp.isoformat() if isinstance(stop_stamp, datetime) else None,
            "settings_identity": settings_identity,
        }
        job_id = "shadow-" + hashlib.sha256(_canonical(identity).encode()).hexdigest()[:32]
        existing = self.store.load_shadow_job(job_id)
        # Registration identity intentionally omits wall-clock time.  Reuse
        # the persisted immutable start and settings snapshot while rebuilding
        # the manifest so a restart can validate the duplicate without
        # touching the running row or its forward-test record.
        registration_start = current
        persisted_settings: Mapping[str, Any] | None = None
        if isinstance(existing, Mapping):
            existing_manifest = existing.get("manifest")
            existing_shared = (
                existing_manifest.get("shared")
                if isinstance(existing_manifest, Mapping)
                else None
            )
            existing_spec = (
                existing_shared.get("spec")
                if isinstance(existing_shared, Mapping)
                else None
            )
            persisted_start = (
                parse_timestamp(existing_spec.get("start_timestamp"))
                if isinstance(existing_spec, Mapping)
                else None
            )
            if persisted_start is not None:
                registration_start = persisted_start
            candidate_settings = (
                existing_shared.get("risk_settings")
                if isinstance(existing_shared, Mapping)
                else None
            )
            if isinstance(candidate_settings, Mapping):
                persisted_settings = _json_copy(candidate_settings)
        member_records = []
        for item in members:
            member_record = {
                "shadow_member_id": item["shadow_member_id"],
                "candidate_id": item["candidate_id"],
                "family": item["family"],
                "setup": item["setup"],
                "setup_id": item["setup_id"],
                "setup_hash": item["setup_hash"],
                "strategy": item["strategy"],
                "strategy_hash": item["strategy_hash"],
                "model": item["model"],
                "model_hash": item["model_hash"],
                "config": item["config"],
                "config_hash": item["config_hash"],
                "scope": item["scope"],
                "scope_hash": item["scope_hash"],
                "scope_version": item["scope_version"],
                "exit_policy": item["exit_policy"],
                "cost_provenance": item["cost_provenance"],
                "risk_limits": item["risk_limits"],
                "rejection_evidence": item["rejection_evidence"],
                "historical_evidence": item["historical_evidence"],
            }
            if "frozen_hash" in item:
                member_record["frozen_hash"] = item["frozen_hash"]
            if "projection_provenance" in item:
                member_record["projection_provenance"] = item["projection_provenance"]
                member_record["historical_frozen_config"] = False
            member_records.append(member_record)
        composite = ShadowCompositeStrategy(member_records)
        composite_model = ShadowCompositeModel(composite.members)
        # Freeze the declarative composite documents once.  The registry hashes
        # its runtime arguments, so passing the custom runtime facades here would
        # hash their object representations instead of the persisted documents.
        composite_strategy_document = _json_copy(composite.definition)
        composite_model_document = _json_copy(composite_model.document)
        composite.definition = composite_strategy_document
        composite_model.document = composite_model_document
        composite_strategy_hash = _content_hash(composite_strategy_document)
        composite_model_hash = _content_hash(composite_model_document)
        try:
            scope_policy = normalize_market_scope(members[0]["scope"])
        except Exception:
            raise ShadowAssessmentError("frozen market scope is not canonical") from None
        canonical_scope = scope_policy.as_dict()
        if any(_text(item["scope_hash"]) != scope_policy.scope_hash for item in members):
            raise ShadowAssessmentError("shadow members must share the exact frozen market scope hash")
        if any(_text(item["scope_version"]) != scope_policy.scope_version for item in members):
            raise ShadowAssessmentError("shadow members must share the exact frozen market scope version")
        allowed_markets = tuple(scope_policy.market_ids)
        authority_required = bool(allowed_markets)
        try:
            policy_values = dict(_mapping(settings.get("effective_limits"), "CURRENT risk settings limits"))
            # Shadow is paper-only and public snapshots may not carry venue
            # rule metadata; monetary/order/position fences still remain fully
            # active from the frozen CURRENT settings.
            policy_values["require_market_rules"] = False
            policy_values["allow_event_overlap"] = True
            policy_values["allow_strategy_overlap"] = True
            operational_policy = OperationalPaperPolicy.from_value(policy_values)
        except Exception as exc:
            raise ShadowAssessmentError("CURRENT operational paper policy is invalid") from exc
        spec_config = {
            "schema": SHADOW_SCHEMA, "paper_only": True, "live_execution": False, "execution": "paper_only",
            "shadow_assessment": True, "observation_intent": authority_required,
            "market_authority_required": authority_required,
            "market_scope": canonical_scope, "market_scope_hash": scope_policy.scope_hash,
            "market_scope_version": scope_policy.scope_version,
            "strategy_document": composite_strategy_document,
            "model_document": composite_model_document,
            "paper_assumptions_explicit": True,
            "paper_assumptions": _json_copy(members[0]["cost_provenance"]),
            "member_allocated_capital": format(bankroll_decimal / Decimal("2"), "f"),
        }
        # Keep sizing bounded and shared: the one wallet is the only capital
        # source, and each member can consume no more than half at one entry.
        assumptions = spec_config["paper_assumptions"]
        sizing = assumptions.get("sizing") if isinstance(assumptions, Mapping) else None
        projection = any("projection_provenance" in item for item in members)
        if not isinstance(sizing, Mapping):
            if not projection:
                raise ShadowAssessmentError("frozen cost provenance sizing is invalid")
            sizing = {
                "model": "fixed_allocated_capital",
                "source": "CURRENT_SETTINGS",
                "settings_identity": _json_copy(settings_identity),
            }
        assumptions = dict(assumptions)
        assumptions["sizing"] = {
            **dict(sizing),
            "allocated_capital": format(bankroll_decimal / Decimal("2"), "f"),
        }
        spec_config["paper_assumptions"] = assumptions
        spec = ForwardTestRegistry(self.store).freeze(
            strategy=composite_strategy_document, model=composite_model_document, config=spec_config, start_timestamp=registration_start,
            bankroll=bankroll_value, allowed_markets=allowed_markets,
            risk_limits=_risk_limits(settings, [item["risk_limits"] for item in members]),
            experiment_id=f"{job_id}:run",
        )
        if spec.strategy_hash != composite_strategy_hash or spec.model_hash != composite_model_hash:
            raise ShadowAssessmentError("composite frozen document hash mismatch")
        for member in member_records:
            member["strategy"] = _json_copy(member["strategy"])
            member["model"] = _json_copy(member["model"])
        member_documents = [
            {
                "shadow_member_id": member["shadow_member_id"],
                "candidate_id": member["candidate_id"],
                "strategy_document": _json_copy(member["strategy"]),
                "model_document": _json_copy(member["model"]),
            }
            for member in member_records
        ]
        member_hashes = [
            {
                "shadow_member_id": member["shadow_member_id"],
                "candidate_id": member["candidate_id"],
                "strategy_hash": member["strategy_hash"],
                "model_hash": member["model_hash"],
            }
            for member in member_records
        ]
        frozen_documents = {
            "strategy_document": composite_strategy_document,
            "model_document": composite_model_document,
            "strategy_hash": composite_strategy_hash,
            "model_hash": composite_model_hash,
        }
        shared_record = {
            "spec": spec.as_record(),
            "run_id": spec.experiment_id,
            "bankroll": bankroll_value,
            "budget": shared_budget,
            "shared_budget": shared_budget,
            "bankroll_source": budget_source,
            "bankroll_cap": format(budget_cap, "f"),
            "risk_settings": persisted_settings if persisted_settings is not None else _json_copy(settings),
            "risk_settings_identity": settings_identity,
            "operational_policy": operational_policy.as_record(),
            "scope": canonical_scope,
            "scope_hash": scope_policy.scope_hash,
            "scope_version": scope_policy.scope_version,
            "cost_provenance": _json_copy(spec_config["paper_assumptions"]),
            "member_allocated_capital": format(bankroll_decimal / Decimal("2"), "f"),
            "frozen_documents": frozen_documents,
            "member_documents": member_documents,
            "member_hashes": member_hashes,
        }
        if projection:
            shared_record["projection_provenance"] = {
                "kind": _HISTORICAL_REJECTED_PROJECTION,
                "candidate_declared_fields": [
                    "strategy",
                    "model",
                    "operational_setup",
                    "market_scope",
                    "exit_policy",
                    "recorded_book_cost_assumptions",
                    "paper_only",
                    "research_only",
                    "rejection_evidence",
                ],
                "current_derived_fields": [
                    "risk_limits",
                    "paper_assumptions.sizing",
                    "settings_identity",
                ],
                "current_derived": {
                    "risk_limits": _json_copy(spec.risk_limits),
                    "paper_sizing": _json_copy(
                        spec.config["paper_assumptions"]["sizing"]
                    ),
                    "settings_identity": _json_copy(settings_identity),
                },
                "settings_identity": _json_copy(settings_identity),
            }
        manifest = {
            "schema": SHADOW_SCHEMA,
            "job_id": job_id,
            "paper_only": True,
            "live_execution": False,
            "members": member_records,
            "shared": shared_record,
            "stop_conditions": {
                "max_cycles": cycle_limit,
                "max_observations": observation_limit,
                "stop_at": stop_stamp.isoformat() if isinstance(stop_stamp, datetime) else None,
            },
        }
        state = self._initial_state(job_id, spec, member_records, cycle_limit, observation_limit, stop_stamp)
        return self.store.register_shadow_job(job_id, manifest, state, SHADOW_STATUS_REGISTERED, current, current)
    @staticmethod
    def _initial_state(job_id: str, spec: ForwardTestSpec, members: Sequence[Mapping[str, Any]], max_cycles: int | None, max_observations: int | None, stop_at: datetime | None) -> dict[str, Any]:
        return {
            "schema": SHADOW_SCHEMA, "job_id": job_id, "run_id": spec.experiment_id,
            "cycles": 0, "public_observations": 0, "member_observations": 0,
            "selected_markets": [], "selected_tokens": {}, "cursor_by_market": {},
            "source_cursor_by_market": {},
            # Durable source identities at the current cursor boundary.  The
            # provider is inclusive, so this compact index prevents a
            # restart from assigning a new group to a same-timestamp row that
            # has fallen out of the bounded observation query.
            "source_identity_groups": {},
            "aggregate_groups": [], "incomplete_groups": [],
            "last_cycle": None, "next_evaluation_at": None, "blockers": [],
            "stop": {"max_cycles": max_cycles, "max_observations": max_observations, "stop_at": stop_at.isoformat() if stop_at else None, "reached": False, "reason": None},
            "members": {
                _text(item["shadow_member_id"]): {
                    "candidate_id": item["candidate_id"], "family": item["family"], "signals": 0,
                    "declines": 0, "risk_rejections": 0, "fills": 0, "exits": 0,
                    "accounting": {"buy_fills": 0, "sell_fills": 0, "filled_quantity": 0.0, "fees": 0.0},
                } for item in members
            },
            "paper_only": True, "live_execution": False,
        }

    def load(self, job_id: str) -> Mapping[str, Any] | None:
        return self.store.load_shadow_job(job_id)

    def list(self, *, status: str | None = None, limit: int = 100) -> list[Mapping[str, Any]]:
        return self.store.list_shadow_jobs(status=status, limit=limit)

    def _persist(self, row: Mapping[str, Any], state: Mapping[str, Any], status: str, next_at: datetime | None, now: datetime) -> Mapping[str, Any]:
        self.store.update_shadow_job(row["job_id"], int(row["version"]), state, status, next_at, now)
        # The store's CAS result is authoritative, but reload it before
        # returning so callers cannot observe a stale/intermediate version
        # while later state transitions have already been persisted.
        latest = self.store.load_shadow_job(row["job_id"])
        if not isinstance(latest, Mapping):
            raise RuntimeError("shadow job disappeared after CAS update")
        return latest

    @staticmethod
    def _append_blocker(state: dict[str, Any], blocker: str) -> None:
        blockers = state.setdefault("blockers", [])
        if not isinstance(blockers, list):
            blockers = []
            state["blockers"] = blockers
        if blocker not in blockers:
            blockers.append(blocker)
        del blockers[:-_MAX_STATE_BLOCKERS]

    def _blocked(
        self,
        row: Mapping[str, Any],
        state: dict[str, Any],
        status: str,
        blocker: str,
        now: datetime,
        *,
        retryable: bool = True,
        interval_seconds: float = 60.0,
    ) -> Mapping[str, Any]:
        self._append_blocker(state, blocker)
        state["last_blocker"] = blocker
        state["retryable"] = retryable
        next_at = now + timedelta(seconds=interval_seconds) if retryable else None
        state["next_evaluation_at"] = next_at.isoformat() if next_at else None
        return self._persist(row, state, status, next_at, now)

    def _events_for_group(self, experiment_id: str, group_id: str) -> list[dict[str, Any]]:
        if not experiment_id or not group_id:
            return []
        connection = getattr(self.store, "connection", None)
        if connection is not None:
            try:
                rows = connection.execute(
                    "SELECT event_id,experiment_id,observation_id,market_id,timestamp,status,payload_json "
                    "FROM paper_execution_events "
                    "WHERE experiment_id=? AND json_extract(payload_json,'$.shadow_group_id')=? "
                    "ORDER BY timestamp,observation_id LIMIT ?",
                    (str(experiment_id), str(group_id), _MAX_RECONCILE_ROWS),
                ).fetchall()
                result: list[dict[str, Any]] = []
                for row in rows:
                    try:
                        payload = json.loads(str(row["payload_json"]))
                    except (TypeError, ValueError, json.JSONDecodeError):
                        continue
                    result.append({
                        "event_id": row["event_id"],
                        "experiment_id": row["experiment_id"],
                        "observation_id": row["observation_id"],
                        "market_id": row["market_id"],
                        "timestamp": parse_timestamp(row["timestamp"]),
                        "status": row["status"],
                        "payload": payload if isinstance(payload, Mapping) else {},
                    })
                return result
            except Exception:
                pass
        try:
            events = self.store.list_paper_execution_events(
                experiment_id, limit=_MAX_RECONCILE_ROWS
            )
        except Exception:
            return []
        return [
            event for event in events
            if isinstance(event, Mapping)
            and isinstance(event.get("payload"), Mapping)
            and _text(event["payload"].get("shadow_group_id")) == group_id
        ]

    def _durable_source_index(
        self,
        run_id: str,
    ) -> dict[str, dict[str, str]] | None:
        """Index persisted source identities by member and original group."""
        if not run_id:
            return {}
        try:
            rows = self.store.list_latest_paper_observations(
                run_id,
                per_market_limit=_MAX_RECONCILE_ROWS,
                per_scope_limit=_MAX_RECONCILE_ROWS,
            )
        except Exception:
            return None
        if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes)):
            return None
        result: dict[str, dict[str, str]] = {}
        for item in rows:
            if not isinstance(item, Mapping):
                continue
            payload = item.get("payload")
            if not isinstance(payload, Mapping):
                continue
            market_id = _text(item.get("market_id", payload.get("market_id")))
            member_id = _text(payload.get("shadow_member_id"))
            if not market_id or not member_id:
                continue
            # An existing source identity without a durable group is
            # ambiguous.  Mark it as a conflict rather than allowing this
            # cycle to silently retag the observation.
            group_id = _text(payload.get("shadow_group_id"))
            source_key = _source_identity_key(market_id, payload)
            members = result.setdefault(source_key, {})
            previous = members.get(member_id)
            if previous is None:
                members[member_id] = group_id
            elif previous != group_id:
                # A source identity cannot safely belong to two groups.  Keep
                # the conflict visible to the caller instead of retagging it.
                members[member_id] = ""
        return result

    @staticmethod
    def _remember_source_identity(
        state: dict[str, Any],
        *,
        source_key: str,
        market_id: str,
        timestamp: datetime,
        member_id: str,
        group_id: str,
    ) -> bool:
        """Persist boundary identities needed for inclusive-source replay."""
        raw = state.get("source_identity_groups")
        identities = dict(raw) if isinstance(raw, Mapping) else {}
        raw_entry = identities.get(source_key)
        entry = dict(raw_entry) if isinstance(raw_entry, Mapping) else {}
        entry["market_id"] = market_id
        entry["timestamp"] = ensure_utc(timestamp).isoformat()
        raw_members = entry.get("members")
        members = dict(raw_members) if isinstance(raw_members, Mapping) else {}
        previous = _text(members.get(member_id))
        conflict = member_id in members and previous != group_id
        if member_id not in members:
            members[member_id] = group_id
        elif conflict:
            # A source identity cannot safely be assigned to two groups.
            members[member_id] = ""
        entry["members"] = members
        identities[source_key] = entry
        state["source_identity_groups"] = identities
        return conflict

    def _reconcile_fills(
        self,
        state: dict[str, Any],
        *,
        run_id: str,
        strategy_id: str,
        job_id: str,
    ) -> None:
        """Rebuild exact fill accounting from a bounded, matching run."""
        fills: list[Any] = []
        blocker: str | None = None
        expected_members = {
            _text(member_id)
            for member_id in (state.get("members") or {})
            if _text(member_id)
        } if isinstance(state.get("members"), Mapping) else set()
        if not run_id or not strategy_id or not expected_members:
            blocker = "SHADOW_FILL_RECONCILIATION_MISMATCH"
        else:
            try:
                count = self.store.count_paper_fills(
                    run_id,
                    strategy_id=strategy_id,
                )
                # Do not coerce malformed counts: truncating a float or
                # accepting a string would make the bounded proof ambiguous.
                if type(count) is not int:
                    raise ValueError("invalid paper fill count")
                if count < 0 or count > _MAX_RECONCILE_ROWS:
                    blocker = "SHADOW_FILL_RECONCILIATION_OVERFLOW"
                else:
                    stored = self.store.list_paper_fills(
                        run_id,
                        strategy_id=strategy_id,
                        limit=count,
                    )
                    if (
                        not isinstance(stored, Sequence)
                        or isinstance(stored, (str, bytes))
                        or len(stored) != count
                    ):
                        blocker = "SHADOW_FILL_RECONCILIATION_MISMATCH"
                    else:
                        for fill in stored:
                            order_id = _text(getattr(fill, "order_id", ""))
                            metadata = getattr(fill, "metadata", {})
                            if not order_id:
                                blocker = "SHADOW_FILL_RECONCILIATION_MISMATCH"
                                break
                            if not isinstance(metadata, Mapping):
                                blocker = "SHADOW_FILL_RECONCILIATION_MISMATCH"
                                break
                            if _text(metadata.get("paper_experiment_id")) != run_id:
                                blocker = "SHADOW_FILL_RECONCILIATION_MISMATCH"
                                break
                            if _text(getattr(fill, "strategy_id", "")) != strategy_id:
                                blocker = "SHADOW_FILL_RECONCILIATION_MISMATCH"
                                break
                            fill_job = _text(metadata.get("shadow_job_id"))
                            if fill_job and fill_job != job_id:
                                blocker = "SHADOW_FILL_RECONCILIATION_MISMATCH"
                                break
                            member_id = _text(metadata.get("shadow_member_id"))
                            if member_id not in expected_members:
                                blocker = "SHADOW_FILL_RECONCILIATION_MISMATCH"
                                break
                            side = _text(
                                getattr(
                                    getattr(fill, "side", None),
                                    "value",
                                    getattr(fill, "side", ""),
                                )
                            ).lower()
                            try:
                                quantity = float(getattr(fill, "quantity"))
                                price = float(getattr(fill, "price"))
                                fees = float(getattr(fill, "fees"))
                                slippage = float(getattr(fill, "slippage"))
                            except (AttributeError, TypeError, ValueError, OverflowError):
                                blocker = "SHADOW_FILL_RECONCILIATION_MISMATCH"
                                break
                            if (
                                side not in {"buy", "sell"}
                                or not math.isfinite(quantity)
                                or quantity <= 0
                                or not math.isfinite(price)
                                or price <= 0
                                or not math.isfinite(fees)
                                or fees < 0
                                or not math.isfinite(slippage)
                                or slippage < 0
                            ):
                                blocker = "SHADOW_FILL_RECONCILIATION_MISMATCH"
                                break
                            fills.append(fill)
            except Exception:
                blocker = blocker or "SHADOW_FILL_RECONCILIATION_MISMATCH"
        if blocker is not None:
            self._append_blocker(state, blocker)
            state["last_fill_reconciliation_blocker"] = blocker
            fills = []
        else:
            state.pop("last_fill_reconciliation_blocker", None)
        self._aggregate(state, (), fills)

    @staticmethod
    def _remember_aggregate_group(state: dict[str, Any], group_id: str) -> bool:
        if not group_id:
            return False
        groups = state.get("aggregate_groups")
        if not isinstance(groups, list):
            groups = []
            state["aggregate_groups"] = groups
        if group_id in groups:
            return False
        if len(groups) >= _MAX_AGGREGATE_GROUPS:
            # The registration bound is the durable lifetime.  Never evict an
            # old identity: doing so permits repeated reconciliation to count
            # its events again.
            return False
        groups.append(group_id)
        return True

    def _reconcile_durable(self, state: dict[str, Any], manifest: Mapping[str, Any]) -> None:
        shared = manifest.get("shared")
        spec = shared.get("spec") if isinstance(shared, Mapping) else None
        run_id = _text(spec.get("experiment_id")) if isinstance(spec, Mapping) else ""
        if not run_id:
            return
        try:
            counts = self.store.paper_history_counts(run_id)
            durable_members = int(counts.get("observations", 0))
        except Exception:
            durable_members = 0
        if durable_members > 0:
            state["member_observations"] = max(
                int(state.get("member_observations", 0)), durable_members
            )
        members = manifest.get("members")
        expected_members = {
            _text(member.get("shadow_member_id"))
            for member in members
            if isinstance(member, Mapping)
        } if isinstance(members, Sequence) else set()
        try:
            # This is the bounded durable reconciliation window.  Previously
            # only a small recent slice was loaded, allowing old groups to be
            # forgotten and re-aggregated after a restart.
            latest = self.store.list_latest_paper_observations(
                run_id,
                per_market_limit=_MAX_RECONCILE_ROWS,
                per_scope_limit=_MAX_RECONCILE_ROWS,
            )
        except Exception:
            latest = []
        if not isinstance(latest, Sequence) or isinstance(latest, (str, bytes)):
            latest = []

        groups: dict[str, dict[str, Any]] = {}
        source_identity_conflicts: set[str] = set()
        for item in latest:
            if not isinstance(item, Mapping):
                continue
            payload = item.get("payload")
            if not isinstance(payload, Mapping):
                continue
            member_id = _text(payload.get("shadow_member_id"))
            stamp = parse_timestamp(item.get("timestamp", payload.get("timestamp")))
            market_id = _text(item.get("market_id", payload.get("market_id")))
            if not member_id or member_id not in expected_members or not market_id or stamp is None:
                continue
            source_cursor = _source_cursor_value(payload)
            boundary_stamp = source_cursor[0] if source_cursor is not None else stamp
            source_key = _source_identity_key(market_id, payload)
            group_id = _text(payload.get("shadow_group_id"))
            # Remember even malformed durable rows.  A missing group is not
            # safe to repair by assigning a fresh cycle identity.
            if self._remember_source_identity(
                state,
                source_key=source_key,
                market_id=market_id,
                timestamp=boundary_stamp,
                member_id=member_id,
                group_id=group_id,
            ) or not group_id:
                source_identity_conflicts.add(source_key)
            if not group_id:
                continue
            row_key = source_key
            group = groups.setdefault(group_id, {"rows": {}})
            details = group["rows"].get(row_key)
            if details is None:
                details = {
                    "members": set(),
                    "market_id": market_id,
                    "timestamp": stamp,
                    "source_cursor": source_cursor,
                }
                group["rows"][row_key] = details
            elif (
                details.get("market_id") != market_id
                or details.get("timestamp") != stamp
            ):
                source_identity_conflicts.add(source_key)
            details["members"].add(member_id)

        complete: list[tuple[str, dict[str, Any]]] = []
        complete_rows = 0
        for group_id, details in groups.items():
            rows = details["rows"]
            if not rows or not expected_members:
                continue
            if (
                not all(expected_members.issubset(row["members"]) for row in rows.values())
                or any(row_key in source_identity_conflicts for row_key in rows)
            ):
                continue
            complete.append((group_id, details))
            complete_rows += len(rows)
        complete_ids = {group_id for group_id, _details in complete}
        pending_groups = {
            _text(group_id)
            for group_id in state.get("incomplete_groups", ())
            if _text(group_id)
        }
        pending_groups.update(
            group_id
            for group_id, details in groups.items()
            if details["rows"]
            and (
                not all(expected_members.issubset(row["members"]) for row in details["rows"].values())
                or any(row_key in source_identity_conflicts for row_key in details["rows"])
            )
        )
        pending_groups.difference_update(complete_ids)
        state["incomplete_groups"] = sorted(pending_groups)[-_MAX_AGGREGATE_GROUPS:]
        if source_identity_conflicts:
            self._append_blocker(state, "SHADOW_SOURCE_IDENTITY_MISMATCH")
            state["last_source_identity_blocker"] = "SHADOW_SOURCE_IDENTITY_MISMATCH"

        raw_cursors = state.get("cursor_by_market")
        cursors = dict(raw_cursors) if isinstance(raw_cursors, Mapping) else {}
        raw_source_cursors = state.get("source_cursor_by_market")
        source_cursors = (
            dict(raw_source_cursors)
            if isinstance(raw_source_cursors, Mapping)
            else {}
        )
        for group_id, details in complete:
            for row in details["rows"].values():
                market_id = row["market_id"]
                stamp = row["timestamp"]
                prior = parse_timestamp(cursors.get(market_id))
                if prior is None or stamp > prior:
                    cursors[market_id] = stamp.isoformat()
                source_cursor = row.get("source_cursor")
                if source_cursor is not None:
                    prior_source = _source_cursor_state(source_cursors.get(market_id))
                    if prior_source is None or source_cursor > prior_source:
                        source_cursors[market_id] = {
                            "timestamp": source_cursor[0].isoformat(),
                            "snapshot_id": source_cursor[1],
                        }
            if self._remember_aggregate_group(state, group_id):
                self._aggregate(state, self._events_for_group(run_id, group_id), None)
        state["cursor_by_market"] = dict(sorted(cursors.items()))
        state["source_cursor_by_market"] = dict(sorted(source_cursors.items()))
        raw_identities = state.get("source_identity_groups")
        if isinstance(raw_identities, Mapping):
            identities: dict[str, dict[str, Any]] = {}
            for source_key, raw_entry in raw_identities.items():
                if not isinstance(raw_entry, Mapping):
                    continue
                market_id = _text(raw_entry.get("market_id"))
                stamp = parse_timestamp(raw_entry.get("timestamp"))
                raw_members = raw_entry.get("members")
                if (
                    not market_id
                    or stamp is None
                    or not isinstance(raw_members, Mapping)
                    or not raw_members
                ):
                    continue
                boundary = _source_cursor_state(source_cursors.get(market_id))
                boundary_stamp = (
                    boundary[0]
                    if boundary is not None
                    else parse_timestamp(cursors.get(market_id))
                )
                if boundary_stamp is not None and stamp < boundary_stamp:
                    continue
                identities[_text(source_key)] = {
                    "market_id": market_id,
                    "timestamp": ensure_utc(stamp).isoformat(),
                    "members": {
                        _text(member_id): _text(group_id)
                        for member_id, group_id in raw_members.items()
                        if _text(member_id)
                    },
                }
            state["source_identity_groups"] = identities
        else:
            state["source_identity_groups"] = {}
        if complete_rows:
            state["public_observations"] = max(
                int(state.get("public_observations", 0)), complete_rows
            )
        if complete:
            state["cycles"] = max(
                int(state.get("cycles", 0)),
                len(state.get("aggregate_groups", ())),
            )

        # Fills are durable independently of the shadow CAS.  Rebuild fill
        # accounting from the matching run on every reconciliation so a
        # crash-completion path reports the committed bankroll activity.
        strategy_id = _text(spec.get("strategy_hash")) if isinstance(spec, Mapping) else ""
        job_id = _text(manifest.get("job_id"))
        self._reconcile_fills(
            state,
            run_id=run_id,
            strategy_id=strategy_id,
            job_id=job_id,
        )

    def _select_markets(self, state: dict[str, Any], manifest: Mapping[str, Any], provider: Any, now: datetime) -> tuple[list[str], dict[str, Mapping[str, str]], str | None]:
        members = manifest.get("members")
        if not isinstance(members, Sequence) or len(members) != 2:
            return [], {}, "MANIFEST_MEMBERS_INVALID"
        scope = members[0].get("scope") if isinstance(members[0], Mapping) else None
        if not isinstance(scope, Mapping):
            return [], {}, "SCOPE_MISSING"

        previous_ids: list[str] = []
        previous_tokens: dict[str, Mapping[str, str]] = {}
        raw_selected = state.get("selected_markets")
        raw_tokens = state.get("selected_tokens")
        if isinstance(raw_selected, list) and isinstance(raw_tokens, Mapping):
            for value in raw_selected:
                market_id = _text(value)
                token_value = raw_tokens.get(market_id)
                if market_id and isinstance(token_value, Mapping) and token_value.get("yes") and token_value.get("no"):
                    previous_ids.append(market_id)
                    previous_tokens[market_id] = {
                        "yes": _text(token_value["yes"]),
                        "no": _text(token_value["no"]),
                    }

        market_reader = getattr(provider, "markets", None)
        if not callable(market_reader):
            if previous_ids:
                return previous_ids, previous_tokens, None
            return [], {}, "PUBLIC_MARKETS_UNAVAILABLE"
        try:
            raw = _invoke(market_reader, active=True)
            if isinstance(raw, Mapping):
                raw = raw.values()
            if not isinstance(raw, Iterable) or isinstance(raw, (str, bytes, Mapping)):
                return (previous_ids, previous_tokens, None) if previous_ids else ([], {}, "PUBLIC_MARKETS_INVALID")
            records: list[dict[str, Any]] = []
            explicit_fixture = _provider_is_explicit_fixture(provider)
            public_adapter = _provider_is_public_polymarket(provider)
            for item in raw:
                record = _market_mapping(item)
                if record is None:
                    continue
                requested_market = _text(record.get("market_id", record.get("condition_id", record.get("id"))))
                yes_token = _text(record.get("yes_token_id", record.get("yesTokenId")))
                no_token = _text(record.get("no_token_id", record.get("noTokenId")))
                if not requested_market or not yes_token or not no_token:
                    continue
                if not (public_adapter or explicit_fixture):
                    continue
                if public_adapter and any(
                    _text(record.get(field)).lower()
                    not in {"", "polymarket", "polymarket-public", "public", "prediction"}
                    for field in ("provider", "venue", "instrument")
                ):
                    continue
                try:
                    _validate_input_provenance(record)
                    _validate_row_token_aliases(record, {"yes": yes_token, "no": no_token})
                except ShadowAssessmentError:
                    continue
                record["market_id"] = requested_market
                record["yes_token_id"] = yes_token
                record["no_token_id"] = no_token
                records.append(record)
                if len(records) >= _MAX_PROVIDER_ROWS:
                    break
        except Exception:
            return (previous_ids, previous_tokens, None) if previous_ids else ([], {}, "PUBLIC_MARKETS_UNAVAILABLE")
        try:
            resolution = resolve_market_scope(
                _text(members[0].get("candidate_id")),
                {"market_scope": scope},
                records,
                resolved_at=now,
                max_matches=min(self.max_markets, _MAX_MARKETS),
                max_markets=_MAX_MARKETS,
            )
        except Exception:
            return (previous_ids, previous_tokens, None) if previous_ids else ([], {}, "SCOPE_INVALID")
        if resolution.status not in {
            MarketScopeResolutionStatus.MATCHED.value,
            MarketScopeResolutionStatus.PARTIAL.value,
        }:
            if previous_ids:
                state["scope_resolution"] = resolution.as_dict()
                return previous_ids, previous_tokens, None
            return [], {}, f"SCOPE_{_text(resolution.reason).upper() or 'UNRESOLVED'}"

        selected_tokens = dict(previous_tokens)
        for market in sorted(resolution.matched_markets, key=lambda item: item.market_id):
            if len(selected_tokens) >= self.max_markets and market.market_id not in selected_tokens:
                continue
            if market.yes_token_id and market.no_token_id:
                selected_tokens.setdefault(
                    market.market_id,
                    {"yes": _text(market.yes_token_id), "no": _text(market.no_token_id)},
                )
        selected_ids = sorted(selected_tokens)[: self.max_markets]
        selected_tokens = {market_id: selected_tokens[market_id] for market_id in selected_ids}
        if not selected_ids:
            return [], {}, "SCOPE_NO_TOKENIZED_MARKETS"
        state["selected_markets"] = selected_ids
        state["selected_tokens"] = selected_tokens
        state["scope_resolution"] = resolution.as_dict()
        return selected_ids, selected_tokens, None

    @staticmethod
    def _aggregate(
        state: dict[str, Any],
        events: Sequence[Mapping[str, Any]],
        fills: Sequence[Any] | None = None,
    ) -> None:
        members = state.get("members")
        if not isinstance(members, Mapping):
            return
        if fills is not None:
            for stats in members.values():
                if isinstance(stats, Mapping):
                    stats["fills"] = 0
                    stats["exits"] = 0
                    stats["accounting"] = {
                        "buy_fills": 0, "sell_fills": 0,
                        "filled_quantity": 0.0, "fees": 0.0,
                    }
        for event in events:
            payload = event.get("payload")
            if not isinstance(payload, Mapping):
                continue
            member_id = _text(payload.get("shadow_member_id"))
            stats = members.get(member_id)
            if not isinstance(stats, Mapping):
                continue
            status = _text(event.get("status", payload.get("status"))).upper()
            evaluation = payload.get("evaluation")
            evidence = evaluation.get("evidence") if isinstance(evaluation, Mapping) else None
            outcome = payload.get("outcome")
            evaluation_evidence = payload.get("evaluation_evidence")
            if outcome in (None, "") and isinstance(evaluation_evidence, Mapping):
                outcome = evaluation_evidence.get("outcome")
            if outcome in (None, "") and isinstance(evidence, Mapping):
                outcome = evidence.get("outcome")
            signal = (
                status in {"SIGNAL", "FULL_FILL", "PARTIAL_FILL", "FILLED", "PARTIALLY_FILLED"}
                or bool(payload.get("evaluation_actionable"))
                or _text(outcome).lower() in {"yes", "no"}
            )
            if signal:
                stats["signals"] = int(stats.get("signals", 0)) + 1
            if status in {"NO_SIGNAL", "STRATEGY_EVALUATED_DECLINED", "DECLINED", "WARMING_UP"}:
                stats["declines"] = int(stats.get("declines", 0)) + 1
            if "RISK" in status or payload.get("risk_rejected"):
                stats["risk_rejections"] = int(stats.get("risk_rejections", 0)) + 1
        if fills is None:
            return
        for fill in fills:
            metadata = getattr(fill, "metadata", {})
            if not isinstance(metadata, Mapping):
                continue
            member_id = _text(metadata.get("shadow_member_id"))
            stats = members.get(member_id)
            if not isinstance(stats, Mapping):
                continue
            side = _text(getattr(getattr(fill, "side", None), "value", getattr(fill, "side", ""))).lower()
            accounting = stats.setdefault("accounting", {})
            stats["fills"] = int(stats.get("fills", 0)) + 1
            if side == "sell":
                stats["exits"] = int(stats.get("exits", 0)) + 1
                accounting["sell_fills"] = int(accounting.get("sell_fills", 0)) + 1
            else:
                accounting["buy_fills"] = int(accounting.get("buy_fills", 0)) + 1
            accounting["filled_quantity"] = float(accounting.get("filled_quantity", 0.0)) + float(getattr(fill, "quantity", 0.0))
            accounting["fees"] = float(accounting.get("fees", 0.0)) + float(getattr(fill, "fees", 0.0))
    def tick(
        self,
        job_id: str,
        provider: Any,
        *,
        now: datetime | None = None,
        max_observations: int | None = None,
        interval_seconds: float | None = None,
    ) -> Mapping[str, Any]:
        if isinstance(interval_seconds, bool):
            raise ValueError("interval_seconds must be finite and positive")
        interval = 60.0 if interval_seconds is None else float(interval_seconds)
        if not math.isfinite(interval) or interval <= 0:
            raise ValueError("interval_seconds must be finite and positive")
        row = self.store.load_shadow_job(job_id)
        if not isinstance(row, Mapping):
            raise KeyError(job_id)
        status = _text(row.get("status")).upper()
        original_state = _json_copy(row.get("state", {}))
        state = _json_copy(original_state)
        manifest = _mapping(row.get("manifest"), "shadow manifest")
        current = ensure_utc(now or self.clock())
        # Durable paper rows may have committed immediately before a process
        # died.  Reconcile even terminal rows before returning so completion
        # reports the durable fills and accounting.
        self._reconcile_durable(state, manifest)
        if status in _TERMINAL_STATUSES:
            if state != original_state:
                return self._persist(
                    row,
                    state,
                    status,
                    parse_timestamp(row.get("next_evaluation_at")),
                    current,
                )
            return row
        _provider_read_only(provider)
        stop = state.get("stop") if isinstance(state.get("stop"), Mapping) else {}
        cycles = int(state.get("cycles", 0))
        public_count = int(state.get("public_observations", 0))
        pending_groups = bool(state.get("incomplete_groups"))
        limit = max_observations if max_observations is not None else self.max_observations
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 0:
            raise ValueError("max_observations must be non-negative")
        if (
            not pending_groups
            and stop.get("max_cycles") is not None
            and cycles >= int(stop["max_cycles"])
        ):
            stop = dict(stop); stop.update({"reached": True, "reason": "max_cycles"}); state["stop"] = stop
            state["next_evaluation_at"] = None
            return self._persist(row, state, SHADOW_STATUS_COMPLETED, None, current)
        if (
            not pending_groups
            and stop.get("max_observations") is not None
            and public_count >= int(stop["max_observations"])
        ):
            stop = dict(stop); stop.update({"reached": True, "reason": "max_observations"}); state["stop"] = stop
            state["next_evaluation_at"] = None
            return self._persist(row, state, SHADOW_STATUS_COMPLETED, None, current)
        stop_at = parse_timestamp(stop.get("stop_at"))
        if stop_at is not None and current >= stop_at and not pending_groups:
            stop = dict(stop); stop.update({"reached": True, "reason": "stop_at"}); state["stop"] = stop
            state["next_evaluation_at"] = None
            return self._persist(row, state, SHADOW_STATUS_COMPLETED, None, current)
        canary_state, _ = _status_report(self.store, self.clock, current)
        if canary_state != "DISARMED":
            return self._blocked(
                row, state, SHADOW_STATUS_WAITING_FOR_DATA, "CANARY_NOT_DISARMED",
                current, interval_seconds=interval,
            )
        selected, tokens, blocker = self._select_markets(state, manifest, provider, current)
        if blocker:
            return self._blocked(
                row, state, SHADOW_STATUS_WAITING_FOR_DATA, blocker,
                current, interval_seconds=interval,
            )
        members = manifest.get("members")
        if not isinstance(members, Sequence) or len(members) != 2:
            return self._blocked(
                row, state, SHADOW_STATUS_BLOCKED, "MANIFEST_MEMBERS_INVALID",
                current, retryable=False, interval_seconds=interval,
            )
        shared = manifest.get("shared")
        spec_record = shared.get("spec") if isinstance(shared, Mapping) else None
        if not isinstance(spec_record, Mapping):
            return self._blocked(
                row, state, SHADOW_STATUS_BLOCKED, "SHARED_SPEC_MISSING",
                current, retryable=False, interval_seconds=interval,
            )
        spec_start = parse_timestamp(spec_record.get("start_timestamp"))
        if spec_start is None:
            return self._blocked(
                row, state, SHADOW_STATUS_BLOCKED, "SHARED_SPEC_INVALID",
                current, retryable=False, interval_seconds=interval,
            )
        raw_cursors = state.get("cursor_by_market")
        cursors = dict(raw_cursors) if isinstance(raw_cursors, Mapping) else {}
        raw_source_cursors = state.get("source_cursor_by_market")
        source_cursors = (
            dict(raw_source_cursors)
            if isinstance(raw_source_cursors, Mapping)
            else {}
        )
        run_id = _text(spec_record.get("experiment_id"))
        durable_source_index = self._durable_source_index(run_id)
        if durable_source_index is None:
            return self._blocked(
                row,
                state,
                SHADOW_STATUS_WAITING_FOR_DATA,
                "SHADOW_SOURCE_IDENTITY_UNAVAILABLE",
                current,
                interval_seconds=interval,
            )
        # Merge the persisted boundary cache with the bounded store query.
        # Store projections can only return a recent window; identities
        # retained in state remain authoritative for inclusive replays.
        raw_identity_cache = state.get("source_identity_groups")
        if isinstance(raw_identity_cache, Mapping):
            for source_key, raw_entry in raw_identity_cache.items():
                normalized_source_key = _text(source_key)
                if not normalized_source_key or not isinstance(raw_entry, Mapping):
                    continue
                raw_members = raw_entry.get("members")
                if not isinstance(raw_members, Mapping):
                    continue
                indexed = durable_source_index.setdefault(normalized_source_key, {})
                for member_id, group_id in raw_members.items():
                    member = _text(member_id)
                    if not member:
                        continue
                    group = _text(group_id)
                    previous = indexed.get(member)
                    if previous is None:
                        indexed[member] = group
                    elif previous != group:
                        indexed[member] = ""
        expected_member_ids = {
            _text(member.get("shadow_member_id"))
            for member in members
            if isinstance(member, Mapping)
        }
        remaining = (
            max(0, int(stop.get("max_observations")) - public_count)
            if stop.get("max_observations") is not None
            else limit
        )
        row_budget = min(limit, remaining, self.max_observations)
        cycle_group_id = f"{job_id}:cycle:{cycles + 1}"
        public_rows: list[dict[str, Any]] = []
        row_keys: dict[int, str] = {}
        row_groups: dict[str, str] = {}
        row_durable_members: dict[str, set[str]] = {}
        seen: set[str] = set()
        source_identity_conflict = False
        for market_id in selected:
            if len(public_rows) >= row_budget:
                break
            source_boundary = _source_cursor_state(source_cursors.get(market_id))
            cursor = (
                source_boundary[0]
                if source_boundary is not None
                else parse_timestamp(cursors.get(market_id))
            )
            rows = _rows_for_market(
                provider,
                market_id,
                current,
                cursor,
                tokens.get(market_id),
                # Fetch a bounded source window independently of the number
                # of rows this cycle may consume.  With an inclusive history
                # endpoint, limiting this to row_budget can return only
                # already-durable boundary rows and hide unseen snapshots.
                max_rows=_MAX_PROVIDER_ROWS,
            )
            for row_value in rows:
                if len(public_rows) >= row_budget:
                    break
                if not isinstance(row_value, Mapping):
                    continue
                try:
                    _validate_input_provenance(row_value)
                except ShadowAssessmentError:
                    continue
                stamp = parse_timestamp(row_value.get("timestamp", row_value.get("observed_at")))
                if stamp is None or stamp < spec_start or stamp > current:
                    continue
                future_invalid = False
                for field in (
                    "source_timestamp", "as_of_timestamp", "asof_timestamp", "as_of",
                    "provider_timestamp", "observed_timestamp", "response_received_at",
                ):
                    parsed = parse_timestamp(row_value.get(field))
                    if parsed is not None and parsed > stamp:
                        future_invalid = True
                        break
                if future_invalid:
                    continue
                observed_at = parse_timestamp(row_value.get("observed_at"))
                if observed_at is not None and observed_at > current:
                    continue
                available_at = parse_timestamp(row_value.get("available_at"))
                if available_at is not None and available_at > (observed_at or stamp):
                    continue
                boundary = _source_cursor_value(row_value)
                boundary_stamp = boundary[0] if boundary is not None else stamp
                if cursor is not None and boundary_stamp < cursor:
                    continue
                item = dict(row_value)
                source_key = _source_identity_key(market_id, item)
                if source_key in seen:
                    continue
                seen.add(source_key)
                durable_members = durable_source_index.get(source_key, {})
                if any(not group_id for group_id in durable_members.values()):
                    source_identity_conflict = True
                    continue
                existing_members = set(durable_members).intersection(expected_member_ids)
                existing_groups = {
                    group_id for group_id in durable_members.values() if group_id
                }
                if len(existing_groups) > 1:
                    source_identity_conflict = True
                    continue
                if expected_member_ids and expected_member_ids.issubset(existing_members):
                    continue
                row_groups[source_key] = next(iter(existing_groups), cycle_group_id)
                row_durable_members[source_key] = existing_members
                row_keys[id(item)] = source_key
                public_rows.append(item)
        if source_identity_conflict:
            return self._blocked(
                row,
                state,
                SHADOW_STATUS_WAITING_FOR_DATA,
                "SHADOW_SOURCE_IDENTITY_MISMATCH",
                current,
                interval_seconds=interval,
            )
        public_rows.sort(
            key=lambda item: (
                _text(item.get("market_id")),
                parse_timestamp(item.get("timestamp", item.get("observed_at")))
                or datetime.min.replace(tzinfo=current.tzinfo),
                _canonical(item),
            )
        )
        unique_rows = public_rows
        if not unique_rows:
            return self._blocked(
                row, state, SHADOW_STATUS_WAITING_FOR_DATA,
                "PUBLIC_OBSERVATIONS_UNAVAILABLE", current,
                interval_seconds=interval,
            )
        expanded: list[dict[str, Any]] = []
        active_group_ids: set[str] = set()
        for item in unique_rows:
            source_key = row_keys[id(item)]
            item_group_id = row_groups[source_key]
            active_group_ids.add(item_group_id)
            durable_members = row_durable_members[source_key]
            for member in sorted(
                members,
                key=lambda value: (
                    _MEMBER_ORDER.get(_text(value.get("family")).lower(), 99),
                    _text(value.get("candidate_id")),
                ),
            ):
                member_id = _text(member.get("shadow_member_id"))
                if member_id in durable_members:
                    continue
                expanded.append(
                    _tag_observation(
                        item,
                        job_id=job_id,
                        member_id=member_id,
                        group_id=item_group_id,
                    )
                )
        if not expanded:
            return self._blocked(
                row,
                state,
                SHADOW_STATUS_WAITING_FOR_DATA,
                "PUBLIC_OBSERVATIONS_UNAVAILABLE",
                current,
                interval_seconds=interval,
            )
        try:
            spec = ForwardTestSpec(
                spec_record["experiment_id"], spec_record["strategy_hash"], spec_record["model_hash"], spec_record.get("config", {}),
                spec_start, spec_record["bankroll"], tuple(spec_record.get("allowed_markets", ())), spec_record.get("risk_limits", {}),
            )
            composite = ShadowCompositeStrategy(members)
            composite_model = ShadowCompositeModel(composite.members)
            portfolio = Portfolio(spec.bankroll)
            engine = ForwardPaperEngine(
                spec, store=self.store, strategy=composite, model=composite_model, portfolio=portfolio,
                storage_namespace=spec.experiment_id, execution_mode="forward",
                operational_policy=(shared.get("operational_policy") if isinstance(shared, Mapping) else None),
                operational_settings=(shared.get("risk_settings_identity") if isinstance(shared, Mapping) else None),
            )
            composite.portfolio = engine.portfolio
            cycle = engine.run(expanded, now=current, max_observations=len(expanded))
        except Exception as exc:
            return self._blocked(
                row, state, SHADOW_STATUS_BLOCKED,
                f"PAPER_ENGINE:{type(exc).__name__}", current,
                retryable=False, interval_seconds=interval,
            )
        # Durable reconciliation, rather than the in-memory engine result,
        # decides whether any source group consumed a cycle.
        state["last_cycle"] = cycle.as_record()
        state["last_cycle"]["shadow_group_id"] = (
            next(iter(active_group_ids))
            if len(active_group_ids) == 1
            else cycle_group_id
        )
        self._reconcile_durable(state, manifest)
        aggregate_groups = {
            _text(group_id) for group_id in state.get("aggregate_groups", ())
        }
        group_complete = (
            bool(active_group_ids)
            and active_group_ids.issubset(aggregate_groups)
        )
        if cycle.blocker:
            self._append_blocker(state, cycle.blocker)
            next_status = (
                SHADOW_STATUS_BLOCKED
                if cycle.retryable is False
                else SHADOW_STATUS_WAITING_FOR_DATA
            )
        elif not group_complete:
            self._append_blocker(state, "SHADOW_MEMBER_PAIR_INCOMPLETE")
            state["last_blocker"] = "SHADOW_MEMBER_PAIR_INCOMPLETE"
            state["retryable"] = True
            next_status = SHADOW_STATUS_WAITING_FOR_DATA
        else:
            next_status = (
                SHADOW_STATUS_RUNNING
                if cycle.observations_processed
                else SHADOW_STATUS_WAITING_FOR_DATA
            )
        if (
            next_status != SHADOW_STATUS_BLOCKED
            and group_complete
            and stop.get("max_cycles") is not None
            and state["cycles"] >= int(stop["max_cycles"])
        ):
            stop = dict(stop)
            stop.update({"reached": True, "reason": "max_cycles"})
            state["stop"] = stop
            next_status = SHADOW_STATUS_COMPLETED
        if (
            next_status != SHADOW_STATUS_BLOCKED
            and group_complete
            and stop.get("max_observations") is not None
            and state["public_observations"] >= int(stop["max_observations"])
        ):
            stop = dict(stop)
            stop.update({"reached": True, "reason": "max_observations"})
            state["stop"] = stop
            next_status = SHADOW_STATUS_COMPLETED
        if (
            next_status != SHADOW_STATUS_BLOCKED
            and stop_at is not None
            and current >= stop_at
            and group_complete
        ):
            stop = dict(stop)
            stop.update({"reached": True, "reason": "stop_at"})
            state["stop"] = stop
            next_status = SHADOW_STATUS_COMPLETED
        next_at = None if next_status in _TERMINAL_STATUSES else current + timedelta(seconds=interval)
        state["next_evaluation_at"] = next_at.isoformat() if next_at else None
        return self._persist(row, state, next_status, next_at, current)

    run_tick = tick

    def stop(self, job_id: str, *, now: datetime | None = None, reason: str = "operator_stop") -> Mapping[str, Any]:
        row = self.store.load_shadow_job(job_id)
        if not isinstance(row, Mapping):
            raise KeyError(job_id)
        if _text(row.get("status")).upper() in _TERMINAL_STATUSES:
            return row
        state = _json_copy(row.get("state", {}))
        stop = dict(state.get("stop") or {})
        stop.update({"reached": True, "reason": _text(reason) or "operator_stop"})
        state["stop"] = stop
        state["next_evaluation_at"] = None
        return self._persist(row, state, SHADOW_STATUS_STOPPED, None, ensure_utc(now or self.clock()))


CompositeShadowStrategy = ShadowCompositeStrategy
ShadowStrategy = ShadowCompositeStrategy

__all__ = [
    "CompositeShadowStrategy", "SHADOW_SCHEMA", "SHADOW_STATES", "ShadowAssessmentError",
    "ShadowAssessmentService", "ShadowBlocked", "ShadowCompositeModel", "ShadowCompositeStrategy",
]
