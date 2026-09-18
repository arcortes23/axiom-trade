"""Frozen forward-testing specifications with paper-only semantics."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
import hashlib
import json
import math
import re
from types import MappingProxyType
from typing import Any, Mapping, Sequence

from .domain import ResearchQuality, ensure_utc, parse_timestamp, utc_now
from .research_bus import ResearchBusPermissionError, _validate_payload
from .storage import AxiomStore
from .risk import RiskLimits
from .strategy.dsl import PREDICTION_FAMILIES
from .strategy.signals import _prediction_requires_model

_PRIVATE_FORWARD_TOKENS = frozenset(
    {
        "credential",
        "private",
        "secret",
        "api_key",
        "password",
        "token",
        "cookie",
        "session",
        "authorization",
        "bearer",
        "oauth",
        "jwt",
        "broker",
        "wallet",
        "execute",
        "execution",
        "live",
        "withdraw",
    }
)

COMMON_PAPER_ASSUMPTIONS: Mapping[str, Any] = MappingProxyType(
    {
        "version": "paper-assumptions-v1",
        "currency": "USD",
        "sizing": MappingProxyType(
            {"model": "fixed_allocated_capital", "allocated_capital": "100"}
        ),
        "fees": MappingProxyType({"model": "proportional", "fee_bps": "10"}),
        "slippage": MappingProxyType({"model": "proportional", "slippage_bps": "5"}),
    }
)

_SUPPORTED_PAPER_SIZING_MODELS = frozenset({"fixed_allocated_capital"})


def _paper_assumptions_explicit(config: Mapping[str, Any]) -> bool:
    """Return whether a caller explicitly froze paper assumptions.

    Older persisted forward records contain the backfilled ``paper_assumptions``
    document but no marker.  They remain legacy-compatible; only a marker
    written while freezing a new intent makes the assumptions authoritative.
    """
    marker = config.get("paper_assumptions_explicit")
    if marker is not None and not isinstance(marker, bool):
        raise ValueError("paper_assumptions_explicit must be a boolean")
    return bool(marker)


def _paper_assumption_costs(config: Mapping[str, Any]) -> tuple[float, float] | None:
    if not _paper_assumptions_explicit(config):
        return None
    assumptions = config.get("paper_assumptions")
    if not isinstance(assumptions, Mapping):
        raise ValueError("explicit paper assumptions must be a mapping")
    fees = assumptions.get("fees")
    slippage = assumptions.get("slippage")
    if not isinstance(fees, Mapping) or not isinstance(slippage, Mapping):
        raise ValueError("explicit paper assumptions require fees and slippage")
    try:
        fee_bps = float(fees.get("fee_bps"))
        slippage_bps = float(slippage.get("slippage_bps"))
    except (TypeError, ValueError):
        raise ValueError("explicit paper assumption costs must be numeric") from None
    if (
        not math.isfinite(fee_bps)
        or not math.isfinite(slippage_bps)
        or fee_bps < 0
        or slippage_bps < 0
    ):
        raise ValueError("explicit paper assumption costs must be finite and non-negative")
    return fee_bps / 10_000.0, slippage_bps


def _validate_private_fields(value: Any, *, path: str = "value", depth: int = 0) -> None:
    if depth > 8:
        raise ValueError(f"forward input nesting exceeds 8 levels: {path}")
    if isinstance(value, Mapping):
        if len(value) > 256:
            raise ValueError(f"forward input mapping is too large: {path}")
        for key, child in value.items():
            normalized = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", str(key))
            normalized = re.sub(r"[^A-Za-z0-9]+", "_", normalized).strip("_").lower()
            compact = normalized.replace("_", "")
            if any(token in normalized or token in compact for token in _PRIVATE_FORWARD_TOKENS):
                raise ValueError(f"forward input contains forbidden private or execution field: {path}.{key}")
            _validate_private_fields(child, path=f"{path}.{key}", depth=depth + 1)
    elif isinstance(value, (list, tuple)):
        if len(value) > 256:
            raise ValueError(f"forward input collection is too large: {path}")
        for index, child in enumerate(value):
            _validate_private_fields(child, path=f"{path}[{index}]", depth=depth + 1)


def _validate_forward_config(config: Mapping[str, Any]) -> None:
    if not isinstance(config, Mapping):
        raise ValueError("forward test config must be a mapping")
    public: dict[str, Any] = {}
    for key, value in config.items():
        normalized = str(key).replace("-", "_").lower()
        if normalized in {
            "strategy_version_id",
            "research_trial_id",
            "portfolio_selection_id",
            "admission_policy_id",
            "admission_policy_version",
            "risk_config_id",
            "risk_config_generation",
            "risk_config_hash",
        }:
            continue
        if normalized in {"live", "live_execution"}:
            if value is not None and (not isinstance(value, bool) or value):
                raise ValueError("forward tests are paper-only")
            continue
        if normalized == "execution":
            if value not in (None, "paper_only"):
                raise ValueError("forward tests are paper-only")
            continue
        public[str(key)] = value
    # Observation intents carry typed safety/provenance markers so the worker
    # can fail closed.  They are control metadata, not public strategy input.
    if config.get("observation_intent") is True:
        for key in (
            "observation_only_lineage",
            "observation_intent",
            "observation_capture_only",
            "execution_scope",
            "research_only",
            "paper_only",
            "selection_excluded",
            "allocation_active",
            "canary_armed",
            "market_authority_required",
        ):
            public.pop(key, None)
    try:
        _validate_payload(public)
    except (ResearchBusPermissionError, TypeError, ValueError) as exc:
        raise ValueError("forward test config contains forbidden private or execution fields") from exc

def _normalize_inventory_binding(config: dict[str, Any]) -> None:
    """Normalize the one legacy inventory binding path before bus validation."""
    resolution = config.get("scope_resolution")
    if not isinstance(resolution, Mapping):
        return
    provenance = resolution.get("provenance")
    if not isinstance(provenance, Mapping):
        return
    current_set = provenance.get("current_market_set")
    if not isinstance(current_set, Mapping):
        return
    normalized_set = dict(current_set)
    legacy = normalized_set.get("order_token")
    inventory = normalized_set.get("inventory_digest")
    if (
        legacy not in (None, "")
        and inventory not in (None, "")
        and str(legacy).strip() != str(inventory).strip()
    ):
        raise ValueError("current_market_set order_token and inventory_digest conflict")
    if inventory in (None, "") and legacy not in (None, ""):
        normalized_set["inventory_digest"] = legacy
    normalized_set.pop("order_token", None)
    normalized_provenance = dict(provenance)
    normalized_provenance["current_market_set"] = normalized_set
    normalized_resolution = dict(resolution)
    normalized_resolution["provenance"] = normalized_provenance
    config["scope_resolution"] = normalized_resolution


def _canonical_scope_config(config: Mapping[str, Any]) -> dict[str, Any]:
    """Validate and canonicalize both accepted scope aliases."""
    result = dict(config)
    _normalize_inventory_binding(result)
    supplied_scope = result.get("scope")
    supplied_market_scope = result.get("market_scope")
    if supplied_scope is not None and supplied_market_scope is not None:
        try:
            from .experiment_plan import normalize_market_scope

            scope_policy = normalize_market_scope(supplied_scope)
            market_scope_policy = normalize_market_scope(supplied_market_scope)
        except (TypeError, ValueError) as exc:
            raise ValueError("forward test scope is not canonical") from exc
        if scope_policy.scope_hash != market_scope_policy.scope_hash:
            raise ValueError("forward test scope and market_scope conflict")
        supplied_market_scope = market_scope_policy.as_dict()
    elif supplied_market_scope is None and supplied_scope is not None:
        supplied_market_scope = supplied_scope
    if supplied_market_scope is None:
        return result
    try:
        from .experiment_plan import normalize_market_scope

        policy = normalize_market_scope(supplied_market_scope)
    except (TypeError, ValueError) as exc:
        raise ValueError("forward test market_scope is not canonical") from exc
    for field, expected in (
        ("market_scope_hash", policy.scope_hash),
        ("scope_hash", policy.scope_hash),
        ("market_scope_version", policy.scope_version),
        ("scope_version", policy.scope_version),
    ):
        supplied = result.get(field)
        if supplied is not None and str(supplied).strip() != expected:
            raise ValueError(f"forward test {field} does not match canonical market scope")
        result[field] = expected
    canonical_scope = policy.as_dict()
    result["market_scope"] = canonical_scope
    if "scope" in result:
        result["scope"] = canonical_scope
    return result


def _plain_json(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _plain_json(child) for key, child in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain_json(child) for child in value]
    return value


def _canonical_forward_config(config: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """Return the immutable paper config carried by a new forward test."""
    result = _plain_json(dict(config or {}))
    supplied = "paper_assumptions" in result
    supplied_assumptions = result.get("paper_assumptions")
    if supplied and not isinstance(supplied_assumptions, Mapping):
        raise ValueError("paper_assumptions must be a mapping")
    merged_assumptions = _plain_json(COMMON_PAPER_ASSUMPTIONS)
    if isinstance(supplied_assumptions, Mapping):
        for key, value in supplied_assumptions.items():
            if isinstance(value, Mapping) and isinstance(merged_assumptions.get(key), Mapping):
                merged_assumptions[key] = {**dict(merged_assumptions[key]), **dict(value)}
            else:
                merged_assumptions[key] = value
    sizing = merged_assumptions.get("sizing")
    sizing = sizing if isinstance(sizing, Mapping) else {}
    sizing_model = sizing.get("model", merged_assumptions.get("sizing_model"))
    if str(sizing_model).strip() not in _SUPPORTED_PAPER_SIZING_MODELS:
        raise ValueError(f"unsupported paper sizing model: {sizing_model!r}")
    result["paper_assumptions"] = merged_assumptions
    if supplied:
        result["paper_assumptions_explicit"] = True
    elif "paper_assumptions_explicit" in result:
        _paper_assumptions_explicit(result)
    return _plain_json(_canonical_scope_config(result))



_MODEL_FREE_DOCUMENT = {"model_required": False}


def _model_free_marker(value: Any) -> bool:
    return (
        isinstance(value, Mapping)
        and value.get("model_required") is False
        and set(value) == {"model_required"}
    )


def _prediction_strategy_model_required(strategy: Any) -> bool | None:
    """Return the existing built-in model requirement for prediction families."""
    document = strategy
    if not isinstance(document, Mapping):
        to_dict = getattr(document, "to_dict", None)
        if callable(to_dict):
            try:
                document = to_dict()
            except Exception:
                return None
        elif isinstance(getattr(document, "definition", None), Mapping):
            document = getattr(document, "definition")
    if isinstance(document, Mapping):
        nested = document.get("strategy_document", document.get("canonical_strategy"))
        if isinstance(nested, Mapping):
            document = nested
    if not isinstance(document, Mapping):
        return None
    market_type = str(document.get("market_type", "")).strip().lower()
    if market_type not in {"prediction", "prediction_market"}:
        return None
    family = str(document.get("family", document.get("template", ""))).strip().lower()
    if not family or family not in PREDICTION_FAMILIES:
        return None
    if not _prediction_requires_model(family):
        return False
    if str(document.get("probability_model", "")).strip().lower() == "market":
        return False
    return True


def _normalize_model_document(
    strategy: Any,
    model: Any,
    configured: Any = None,
) -> tuple[Any, Any]:
    """Canonicalize model input for model-independent prediction work."""
    required = _prediction_strategy_model_required(strategy)
    model_source = getattr(model, "document", model)
    if required is False:
        marker = dict(_MODEL_FREE_DOCUMENT)
        return marker, marker
    return model_source, configured
def _merge_exact_observation_bindings(
    config: Mapping[str, Any] | None,
    *,
    candidate_id: str | None = None,
    strategy_version_id: str | None = None,
    research_trial_id: str | None = None,
    source_strategy_hash: str | None = None,
    rolling_strategy_hash: str | None = None,
    dataset_selector: Mapping[str, Any] | None = None,
    scope: Mapping[str, Any] | None = None,
    market_scope: Mapping[str, Any] | None = None,
) -> tuple[dict[str, Any], bool]:
    """Merge caller-provided rolling bindings without weakening immutability."""
    result = _canonical_scope_config(dict(config or {}))
    rolling = False

    def text(name: str, value: Any) -> str | None:
        if value is None:
            return None
        normalized = str(value).strip()
        if not normalized:
            raise ValueError(f"{name} must be non-empty")
        return normalized

    def bind_text(name: str, value: Any) -> None:
        nonlocal rolling
        normalized = text(name, value)
        if normalized is None:
            return
        existing = result.get(name)
        if existing not in (None, "") and str(existing).strip() != normalized:
            raise ValueError(f"observation intent binding conflicts for {name}")
        result[name] = normalized
        rolling = True

    bind_text("candidate_id", candidate_id)
    bind_text("strategy_version_id", strategy_version_id)
    bind_text("research_trial_id", research_trial_id)
    bind_text("source_strategy_hash", source_strategy_hash)
    bind_text("rolling_strategy_hash", rolling_strategy_hash)

    if dataset_selector is not None:
        if not isinstance(dataset_selector, Mapping):
            raise ValueError("dataset_selector must be a mapping")
        supplied = _plain(dataset_selector)
        current = result.get("dataset_selector")
        if current not in (None, {}) and _canonical(current) != _canonical(supplied):
            raise ValueError("observation intent binding conflicts for dataset_selector")
        result["dataset_selector"] = supplied
        rolling = True
    if scope is not None and market_scope is not None:
        try:
            from .experiment_plan import normalize_market_scope

            scope_compare = normalize_market_scope(scope).as_dict()
            market_scope_compare = normalize_market_scope(market_scope).as_dict()
        except (TypeError, ValueError) as exc:
            raise ValueError("scope is not canonical") from exc
        if _canonical(scope_compare) != _canonical(market_scope_compare):
            raise ValueError("observation intent binding conflicts for scope")
    scope = scope if scope is not None else market_scope
    if scope is not None:
        if not isinstance(scope, Mapping):
            raise ValueError("scope must be a mapping")
        supplied = _plain(scope)
        current = result.get("market_scope", result.get("scope"))
        if current not in (None, {}):
            try:
                from .experiment_plan import normalize_market_scope

                current_compare = normalize_market_scope(current).as_dict()
                supplied_compare = normalize_market_scope(supplied).as_dict()
            except (TypeError, ValueError) as exc:
                raise ValueError("scope is not canonical") from exc
            if _canonical(current_compare) != _canonical(supplied_compare):
                raise ValueError("observation intent binding conflicts for scope")
        result["scope"] = supplied
        result["market_scope"] = supplied
        rolling = True
    elif "scope" in result and "market_scope" not in result:
        result["market_scope"] = result["scope"]

    rolling = any(
        value is not None
        for value in (
            strategy_version_id,
            research_trial_id,
            source_strategy_hash,
            rolling_strategy_hash,
            dataset_selector,
            scope,
        )
    )
    return result, rolling


def _scope_allowed_markets(config: Mapping[str, Any]) -> tuple[str, ...] | None:
    scope = config.get("market_scope")
    if not isinstance(scope, Mapping):
        return None
    mode = str(scope.get("mode", "")).strip().upper()
    if mode != "EXACT_MARKETS":
        return None
    values = scope.get("market_ids", ())
    if not isinstance(values, (list, tuple, set, frozenset)):
        return ()
    return tuple(dict.fromkeys(str(item).strip() for item in values if str(item).strip()))


def _scope_resolution_mapping(value: Any) -> Mapping[str, Any] | None:
    """Return a bounded immutable scope-resolution proof mapping."""
    if isinstance(value, Mapping):
        return value
    as_dict = getattr(value, "as_dict", None)
    if callable(as_dict):
        try:
            resolved = as_dict()
        except Exception:
            return None
        return resolved if isinstance(resolved, Mapping) else None
    return None
_SCOPE_PROOF_VOLATILE_KEYS = frozenset(
    {
        "resolved_at",
        "resolution_id",
        "resolution_timestamp",
        "request_id",
        "query_id",
        "observed_at",
        "created_at",
        "updated_at",
    }
)


def _semantic_scope_resolution(value: Mapping[str, Any]) -> dict[str, Any]:
    """Keep authority/disposition identity while dropping refresh metadata."""
    def clean(item: Any) -> Any:
        if isinstance(item, Mapping):
            return {
                str(key): clean(child)
                for key, child in item.items()
                if str(key).strip().lower() not in _SCOPE_PROOF_VOLATILE_KEYS
            }
        if isinstance(item, (list, tuple)):
            return [clean(child) for child in item]
        return item

    return clean(value)


def _scope_resolution_market_ids(value: Mapping[str, Any]) -> tuple[str, ...]:
    """Extract only resolver-authoritative matched market identities."""
    matched = value.get("matched_markets", value.get("resolved_markets", ()))
    if not isinstance(matched, (list, tuple, set, frozenset)):
        matched = value.get("matched_market_ids", value.get("resolved_market_ids", ()))
    if isinstance(matched, str):
        matched = (matched,)
    if not isinstance(matched, (list, tuple, set, frozenset)):
        return ()
    result: list[str] = []
    for item in matched:
        if isinstance(item, Mapping):
            item = item.get("market_id", item.get("id"))
        else:
            item = getattr(item, "market_id", item)
        text = str(item).strip() if item is not None else ""
        if text and text not in result:
            result.append(text)
    return tuple(result)


def _require_rule_scope_resolution(
    source_config: Mapping[str, Any],
    source_candidate: str,
    allowed_markets: Sequence[str],
    supplied: Any,
) -> None:
    """Reject rule-scope materialization without matching complete authority."""
    try:
        from .experiment_plan import normalize_market_scope

        scope = source_config.get("market_scope", source_config.get("scope"))
        policy = normalize_market_scope(scope)
    except (TypeError, ValueError) as exc:
        raise ValueError("RULE_BASED_MARKET_SCOPE_POLICY_INVALID") from exc
    if str(policy.mode).strip().upper() != "RULE_BASED_MARKETS":
        return
    proof = _scope_resolution_mapping(supplied)
    if proof is None:
        raise ValueError("RULE_BASED_MARKET_SCOPE_RESOLUTION_REQUIRED")
    status = str(proof.get("status", "")).strip().upper()
    if status not in {"MATCHED", "COMPLETE"} or proof.get("complete") is False:
        raise ValueError("RULE_BASED_MARKET_SCOPE_RESOLUTION_INCOMPLETE")
    if str(proof.get("candidate_id", "")).strip() != source_candidate:
        raise ValueError("RULE_BASED_MARKET_SCOPE_RESOLUTION_CANDIDATE_MISMATCH")
    if str(proof.get("scope_hash", proof.get("market_scope_hash", ""))).strip() != policy.scope_hash:
        raise ValueError("RULE_BASED_MARKET_SCOPE_RESOLUTION_HASH_MISMATCH")
    if str(proof.get("scope_version", proof.get("market_scope_version", proof.get("version", "")))).strip() != policy.scope_version:
        raise ValueError("RULE_BASED_MARKET_SCOPE_RESOLUTION_VERSION_MISMATCH")
    resolved_ids = set(_scope_resolution_market_ids(proof))
    if not resolved_ids or any(str(item).strip() not in resolved_ids for item in allowed_markets):
        raise ValueError("RULE_BASED_MARKET_SCOPE_RESOLUTION_MARKET_MISMATCH")

@dataclass(frozen=True, slots=True)
class ForwardTestSpec:
    experiment_id: str
    strategy_hash: str
    model_hash: str
    config: Mapping[str, Any] = field(default_factory=dict)
    start_timestamp: datetime = field(default_factory=utc_now)
    bankroll: float = 10_000.0
    allowed_markets: tuple[str, ...] = ()
    risk_limits: Mapping[str, Any] = field(default_factory=dict)
    quality: ResearchQuality = ResearchQuality.PAPER_FORWARD
    registration_timestamp: datetime | None = None


    def __post_init__(self) -> None:
        bankroll = float(self.bankroll)
        if (
            not str(self.experiment_id).strip()
            or not str(self.strategy_hash).strip()
            or not str(self.model_hash).strip()
        ):
            raise ValueError("forward specs require experiment, strategy, and model identifiers")
        if not math.isfinite(bankroll) or bankroll <= 0:
            raise ValueError("forward bankroll must be finite and positive")
        if not isinstance(self.quality, ResearchQuality):
            object.__setattr__(self, "quality", ResearchQuality(str(self.quality)))
        if self.quality is not ResearchQuality.PAPER_FORWARD:
            raise ValueError("forward registry accepts PAPER_FORWARD specs only")
        start = ensure_utc(self.start_timestamp)
        registration = ensure_utc(self.registration_timestamp or start)
        if registration != start:
            raise ValueError("registration_timestamp must equal start_timestamp for a frozen test")
        object.__setattr__(self, "start_timestamp", start)
        object.__setattr__(self, "registration_timestamp", registration)
        normalized_config = _canonical_scope_config(self.config)
        _validate_forward_config(normalized_config)
        try:
            RiskLimits(**dict(self.risk_limits))
        except (TypeError, ValueError) as exc:
            raise ValueError("forward risk_limits are invalid") from exc
        _validate_private_fields(self.risk_limits, path="risk_limits")
        normalized_markets = tuple(
            dict.fromkeys(str(item).strip() for item in self.allowed_markets if str(item).strip())
        )
        if bool(normalized_config.get("market_authority_required", False)) and not normalized_markets:
            raise ValueError("forward test requires a non-empty frozen market authority")
        allowed_by_scope = _scope_allowed_markets(normalized_config)
        if allowed_by_scope is not None and any(item not in allowed_by_scope for item in normalized_markets):
            raise ValueError("forward test allowed_markets exceed the exact canonical market scope")
        if len(normalized_markets) > 1000:
            raise ValueError("forward test allowed_markets exceeds 1000 entries")
        object.__setattr__(self, "allowed_markets", normalized_markets)
        object.__setattr__(self, "config", _freeze_json(normalized_config))
        object.__setattr__(self, "risk_limits", _freeze_json(self.risk_limits))
    def as_record(self) -> dict[str, Any]:
        return {
            "experiment_id": self.experiment_id,
            "strategy_hash": self.strategy_hash,
            "model_hash": self.model_hash,
            "config": _plain(self.config),
            "start_timestamp": self.start_timestamp.isoformat(),
            "registration_timestamp": self.registration_timestamp.isoformat(),
            "bankroll": self.bankroll,
            "allowed_markets": list(self.allowed_markets),
            "risk_limits": _plain(self.risk_limits),
            "quality": self.quality.value,
        }


class ForwardTestRegistry:
    """Append-only registry; it never starts a trader or submits an order."""

    def __init__(self, store: AxiomStore | None = None) -> None:
        self.store = store
        self._specs: dict[str, ForwardTestSpec] = {}

    def freeze(
        self,
        *,
        strategy: Any,
        model: Any,
        config: Mapping[str, Any] | None = None,
        start_timestamp: datetime | None = None,
        bankroll: float = 10_000.0,
        allowed_markets: Sequence[str] = (),
        risk_limits: Mapping[str, Any] | None = None,
        experiment_id: str | None = None,
        strategy_version_id: str | None = None,
        research_trial_id: str | None = None,
        portfolio_selection_id: str | None = None,
        admission_policy_id: str | None = None,
        admission_policy_version: str | None = None,
        risk_config_id: str | None = None,
        risk_config_generation: int | None = None,
        risk_config_hash: str | None = None,
    ) -> ForwardTestSpec:
        strategy_value, model_value = _frozen_runtime_documents(strategy, model, config)
        strategy_hash = _content_hash(_normalized_strategy_document(strategy_value))
        model_hash = _content_hash(model_value)
        config_record = dict(config or {})
        lineage = {
            "strategy_version_id": strategy_version_id,
            "research_trial_id": research_trial_id,
            "portfolio_selection_id": portfolio_selection_id,
            "admission_policy_id": admission_policy_id,
            "admission_policy_version": admission_policy_version,
            "risk_config_id": risk_config_id,
            "risk_config_generation": risk_config_generation,
            "risk_config_hash": risk_config_hash,
        }
        for key, value in lineage.items():
            if value is None:
                continue
            existing = config_record.get(key)
            if existing not in (None, "") and _canonical(existing) != _canonical(value):
                raise ValueError(f"forward test lineage conflicts for {key}")
            config_record[key] = value
        config_record = _bind_operational_setup(
            strategy_value,
            _canonical_forward_config(config_record),
        )
        _validate_forward_config(config_record)
        start = ensure_utc(start_timestamp or utc_now())
        normalized_markets = tuple(dict.fromkeys(str(item).strip() for item in allowed_markets if str(item).strip()))
        payload = {
            "strategy_hash": strategy_hash,
            "model_hash": model_hash,
            "config": config_record,
            "start_timestamp": start.isoformat(),
            "registration_timestamp": start.isoformat(),
            "bankroll": float(bankroll),
            "allowed_markets": list(normalized_markets),
            "risk_limits": dict(risk_limits or {}),
            "quality": ResearchQuality.PAPER_FORWARD.value,
        }
        identifier = experiment_id or "forward-" + hashlib.sha256(_canonical(payload).encode("utf-8")).hexdigest()[:24]
        spec = ForwardTestSpec(identifier, strategy_hash, model_hash, payload["config"], start, bankroll, normalized_markets, payload["risk_limits"])
        existing = self._specs.get(identifier)
        if existing is not None:
            if existing.as_record() != spec.as_record():
                raise ValueError(f"forward test is frozen: {identifier}")
            return existing
        if self.store is not None:
            self.store.save_forward_test(identifier, spec.as_record())
        self._specs[identifier] = spec
        return spec

    def register_forward_test(
        self,
        *,
        strategy: Any,
        model: Any,
        registration_timestamp: datetime | None = None,
        now: datetime | None = None,
        config: Mapping[str, Any] | None = None,
        bankroll: float = 10_000.0,
        allowed_markets: Sequence[str] = (),
        risk_limits: Mapping[str, Any] | None = None,
        experiment_id: str | None = None,
        strategy_version_id: str | None = None,
        research_trial_id: str | None = None,
        portfolio_selection_id: str | None = None,
        admission_policy_id: str | None = None,
        admission_policy_version: str | None = None,
        risk_config_id: str | None = None,
        risk_config_generation: int | None = None,
        risk_config_hash: str | None = None,
    ) -> ForwardTestSpec:
        """Register a genuinely forward test; historical starts are rejected."""
        current = ensure_utc(now or utc_now())
        registration = ensure_utc(registration_timestamp or current)
        if registration < current:
            raise ValueError("forward registration_timestamp cannot be in the past")
        return self.freeze(
            strategy=strategy,
            model=model,
            config=config,
            start_timestamp=registration,
            bankroll=bankroll,
            allowed_markets=allowed_markets,
            risk_limits=risk_limits,
            experiment_id=experiment_id,
            strategy_version_id=strategy_version_id,
            research_trial_id=research_trial_id,
            portfolio_selection_id=portfolio_selection_id,
            admission_policy_id=admission_policy_id,
            admission_policy_version=admission_policy_version,
            risk_config_id=risk_config_id,
            risk_config_generation=risk_config_generation,
            risk_config_hash=risk_config_hash,
        )

    def register_observation_intent(
        self,
        *,
        strategy: Any,
        model: Any,
        config: Mapping[str, Any],
        registration_timestamp: datetime | None = None,
        bankroll: float = 10_000.0,
        risk_limits: Mapping[str, Any] | None = None,
        candidate_id: str,
        strategy_version_id: str | None = None,
        research_trial_id: str | None = None,
        source_strategy_hash: str | None = None,
        rolling_strategy_hash: str | None = None,
        dataset_selector: Mapping[str, Any] | None = None,
        scope: Mapping[str, Any] | None = None,
        market_scope: Mapping[str, Any] | None = None,
        scope_resolution: Mapping[str, Any] | Any | None = None,
    ) -> ForwardTestSpec:
        """Persist one immutable paper/research observation intent.

        Rolling bindings are part of the frozen config.  The intent identity
        is derived from those bindings rather than accepted from a caller.
        Legacy callers that provide only ``candidate_id`` retain their
        candidate-derived identifier.
        """
        identifier = str(candidate_id).strip()
        if not identifier:
            raise ValueError("candidate_id is required for an observation intent")
        intent_config, rolling = _merge_exact_observation_bindings(
            config,
            candidate_id=identifier,
            strategy_version_id=strategy_version_id,
            research_trial_id=research_trial_id,
            source_strategy_hash=source_strategy_hash,
            rolling_strategy_hash=rolling_strategy_hash,
            dataset_selector=dataset_selector,
            scope=scope,
            market_scope=market_scope,
        )
        if scope_resolution is not None:
            proof = _scope_resolution_mapping(scope_resolution)
            if proof is None:
                raise ValueError("scope_resolution must be a mapping or immutable resolution")
            intent_config.setdefault(
                "scope_resolution",
                _semantic_scope_resolution(proof),
            )
        # Legacy intents historically carried candidate identity only in
        # their deterministic experiment id; do not rewrite their frozen
        # config on an idempotent retry.
        if not rolling and "candidate_id" not in config:
            intent_config.pop("candidate_id", None)
        intent_config["observation_intent"] = True
        intent_config["market_authority_required"] = False
        # Observation intents must retain the immutable source documents.  A
        # later materialization may not index caller configuration for them.
        intent_config.setdefault(
            "strategy_document", _normalized_strategy_document(strategy)
        )
        strategy_for_model = intent_config.get("strategy_document", strategy)
        source_model_document = getattr(model, "document", model)
        normalized_model, configured_model = _normalize_model_document(
            strategy_for_model,
            source_model_document,
            intent_config.get("model_document"),
        )
        if isinstance(configured_model, Mapping):
            intent_config["model_document"] = dict(configured_model)
        elif isinstance(normalized_model, Mapping):
            intent_config["model_document"] = dict(normalized_model)
        _frozen_runtime_documents(strategy, model, intent_config)
        # Hash the supplied runtime objects after validating them against the
        # immutable documents carried by the intent config.  The strategy
        # hash must be computed before identity material is assembled; older
        # callers rely on this exact content hash and may omit setup fields.
        computed_strategy_hash = _content_hash(_normalized_strategy_document(strategy))
        if rolling_strategy_hash is not None and str(rolling_strategy_hash).strip() != computed_strategy_hash:
            raise ValueError("rolling_strategy_hash does not match strategy")
        normalized_config = _bind_operational_setup(
            strategy,
            _canonical_forward_config(intent_config),
        )
        identity_config = dict(normalized_config)
        # The generated intent ID is persisted as provenance but must not
        # participate in its own rolling digest.
        identity_config.pop("observation_intent_id", None)
        identity_config.pop("paper_observation_intent_id", None)
        identity_config.pop("superseded_observation_intent_ids", None)
        computed_model_hash = _content_hash(normalized_model)
        normalized_risk_limits = dict(risk_limits or {})
        identity_material = {
            "candidate_id": identifier,
            "strategy_hash": computed_strategy_hash,
            "model_hash": computed_model_hash,
            "config": identity_config,
            "bankroll": float(bankroll),
            "risk_limits": normalized_risk_limits,
        }
        experiment_id = (
            "observation-intent-" + identifier
            if not rolling
            else "observation-intent-"
            + hashlib.sha256(_canonical(identity_material).encode("utf-8")).hexdigest()[:24]
        )
        intent_config["observation_intent_id"] = experiment_id
        intent_config["paper_observation_intent_id"] = experiment_id
        normalized_config["observation_intent_id"] = experiment_id
        normalized_config["paper_observation_intent_id"] = experiment_id
        existing = self.get(experiment_id)
        if existing is not None:
            existing_config = dict(existing.config)
            if (
                "paper_assumptions_explicit" not in normalized_config
                and existing_config.get("paper_assumptions_explicit") is True
            ):
                # Older observation intents were canonicalized twice during
                # persistence, which added this marker after identity hashing.
                existing_config.pop("paper_assumptions_explicit", None)
            exact_identity = (
                str(existing.strategy_hash).strip() == computed_strategy_hash
                and str(existing.model_hash).strip() == computed_model_hash
                and _canonical(existing_config) == _canonical(normalized_config)
                and float(existing.bankroll) == identity_material["bankroll"]
                and _canonical(existing.risk_limits) == _canonical(normalized_risk_limits)
                and not existing.allowed_markets
            )
            if exact_identity:
                # Registration/start timestamps are operational metadata, not
                # part of an observation intent's deterministic identity.
                return existing
            raise ValueError(f"forward test is frozen: {experiment_id}")
        # The deterministic id changes when provenance changes; exact
        # identities for one candidate intentionally coexist under their digest
        # IDs.  Any actual identifier collision remains rejected by ``freeze``
        # and the immutable store persistence layer.
        return self.freeze(
            strategy=strategy,
            model=model,
            config=intent_config,
            start_timestamp=registration_timestamp or utc_now(),
            bankroll=bankroll,
            allowed_markets=(),
            risk_limits=normalized_risk_limits,
            experiment_id=experiment_id,
        )

    def list_observation_intents(
        self,
        *,
        limit: int | None = None,
        after_experiment_id: str | None = None,
    ) -> tuple[ForwardTestSpec, ...]:
        def convert(record: Mapping[str, Any]) -> ForwardTestSpec:
            timestamp = parse_timestamp(record["start_timestamp"])
            if timestamp is None:
                raise ValueError(f"invalid persisted forward-test timestamp: {record['experiment_id']}")
            return ForwardTestSpec(
                record["experiment_id"],
                record["strategy_hash"],
                record["model_hash"],
                record["config"],
                timestamp,
                record["bankroll"],
                tuple(record["allowed_markets"]),
                record["risk_limits"],
                ResearchQuality(record["quality"]),
                timestamp,
            )
        if (
            self.store is not None
            and callable(getattr(self.store, "load_observation_intents", None))
            and (limit is not None or after_experiment_id is not None)
        ):
            records = self.store.load_observation_intents(
                limit=512 if limit is None else int(limit),
                after_experiment_id=after_experiment_id,
            )
            return tuple(convert(record) for record in records)
        if self.store is not None and not callable(
            getattr(self.store, "load_observation_intents", None)
        ) and limit is not None:
            # A persistent migration scan must never fall back to loading all
            # historical rows from an unbounded compatibility API.
            return ()
        specs = tuple(
            spec
            for spec in self.list()
            if bool((spec.config if isinstance(spec.config, Mapping) else {}).get("observation_intent"))
            and spec.experiment_id.startswith("observation-intent-")
            and (
                not after_experiment_id
                or spec.experiment_id > str(after_experiment_id)
            )
        )
        return specs if limit is None else specs[: int(limit)]

    def materialize_observation_intent(
        self,
        intent: ForwardTestSpec | str,
        *,
        allowed_markets: Sequence[str],
        registration_timestamp: datetime | None = None,
        now: datetime | None = None,
        candidate_id: str | None = None,
        strategy_version_id: str | None = None,
        research_trial_id: str | None = None,
        source_strategy_hash: str | None = None,
        rolling_strategy_hash: str | None = None,
        dataset_selector: Mapping[str, Any] | None = None,
        scope: Mapping[str, Any] | None = None,
        market_scope: Mapping[str, Any] | None = None,
        scope_resolution: Mapping[str, Any] | Any | None = None,
    ) -> ForwardTestSpec:
        """Create the immutable bounded forward spec after authority resolution."""
        source = intent if isinstance(intent, ForwardTestSpec) else self.get(str(intent))
        if source is None:
            raise ValueError("observation intent is missing")
        source_config = dict(source.config) if isinstance(source.config, Mapping) else {}
        if not bool(source_config.get("observation_intent")):
            raise ValueError("forward spec is not an observation intent")
        strategy_document = source_config.get("strategy_document")
        if not isinstance(strategy_document, Mapping):
            strategy_document = source_config.get("strategy", source_config.get("canonical_strategy"))
        if not isinstance(strategy_document, Mapping):
            raise ValueError("observation intent strategy document is missing")
        model_document = source_config.get("model_document")
        if not isinstance(model_document, Mapping):
            model_document = source_config.get("model")
        model_required = _prediction_strategy_model_required(strategy_document)
        if model_required is False:
            # Materialized specs never carry an executable model for a
            # model-independent family, even when a caller supplied one.
            model_document = dict(_MODEL_FREE_DOCUMENT)
            source_config["model_document"] = dict(model_document)
        elif not isinstance(model_document, Mapping):
            if model_required is True:
                raise ValueError("MODEL_INPUT_MISSING")
            raise ValueError("observation intent model document is missing")
        elif _model_free_marker(model_document):
            raise ValueError("MODEL_INPUT_MISSING")
        source_candidate = str(source_config.get("candidate_id", "")).strip()
        if not source_candidate:
            source_candidate = source.experiment_id.removeprefix("observation-intent-").strip()
        if not source_candidate:
            raise ValueError("observation intent has no candidate identity")
        if candidate_id is not None and str(candidate_id).strip() != source_candidate:
            raise ValueError("observation intent candidate_id conflicts with frozen binding")
        markets = tuple(dict.fromkeys(str(item).strip() for item in allowed_markets if str(item).strip()))
        if not markets or len(markets) > 100:
            raise ValueError("collector must materialize a non-empty bounded market set")
        source_config, rolling_binding = _merge_exact_observation_bindings(
            source_config,
            candidate_id=source_candidate,
            strategy_version_id=strategy_version_id,
            research_trial_id=research_trial_id,
            source_strategy_hash=source_strategy_hash,
            rolling_strategy_hash=rolling_strategy_hash,
            dataset_selector=dataset_selector,
            scope=scope,
            market_scope=market_scope,
        )
        scope_document = source_config.get("market_scope", source_config.get("scope"))
        if isinstance(scope_document, Mapping):
            _require_rule_scope_resolution(
                source_config,
                source_candidate,
                markets,
                scope_resolution
                if scope_resolution is not None
                else source_config.get("scope_resolution"),
            )
        source_config["market_authority_required"] = True
        current = ensure_utc(now or utc_now())
        registration = ensure_utc(registration_timestamp or current)
        complete_exact_identity = all(
            str(source_config.get(field) or "").strip()
            for field in (
                "strategy_version_id",
                "research_trial_id",
                "source_strategy_hash",
                "rolling_strategy_hash",
            )
        )
        rolling_only = bool(
            rolling_binding
            or (
                source_config.get("rolling_research") is True
                and complete_exact_identity
            )
        )
        if rolling_only:
            binding_digest = hashlib.sha256(
                _canonical(
                    {
                        "source_experiment_id": source.experiment_id,
                        "config": source_config,
                        "allowed_markets": markets,
                    }
                ).encode("utf-8")
            ).hexdigest()[:24]
            source_config["materialized_binding_hash"] = "sha256:" + binding_digest
            experiment_id = "forward-" + source_candidate + "-" + binding_digest
        else:
            # Keep the original collector/legacy identity unchanged.
            experiment_id = "forward-" + source_candidate

        strategy_value, model_value = _frozen_runtime_documents(
            strategy_document,
            model_document,
            source_config,
        )
        strategy_hash = _content_hash(_normalized_strategy_document(strategy_value))
        model_hash = _content_hash(model_value)
        canonical_config = _canonical_forward_config(source_config)
        expected_bankroll = float(source.bankroll)
        expected_risk_limits = dict(source.risk_limits)
        existing = self.get(experiment_id)
        if existing is not None:
            exact_identity = (
                str(existing.strategy_hash).strip() == strategy_hash
                and str(existing.model_hash).strip() == model_hash
                and _canonical(existing.config) == _canonical(canonical_config)
                and float(existing.bankroll) == expected_bankroll
                and _canonical(existing.risk_limits) == _canonical(expected_risk_limits)
                and tuple(existing.allowed_markets) == markets
            )
            if exact_identity:
                # Registration/start timestamps are operational metadata, not
                # part of a materialized intent's deterministic identity.
                return existing
            raise ValueError(f"forward test is frozen: {experiment_id}")

        return self.register_forward_test(
            strategy=dict(strategy_document),
            model=dict(model_document),
            registration_timestamp=registration,
            now=current,
            config=source_config,
            bankroll=source.bankroll,
            allowed_markets=markets,
            risk_limits=expected_risk_limits,
            experiment_id=experiment_id,
        )



    def list(self) -> tuple[ForwardTestSpec, ...]:
        if self.store is not None:
            records = self.store.load_forward_tests()
            specs = []
            for record in records:
                start_timestamp = parse_timestamp(record["start_timestamp"])
                if start_timestamp is None:
                    raise ValueError(f"invalid persisted forward-test timestamp: {record['experiment_id']}")
                specs.append(
                    ForwardTestSpec(
                        record["experiment_id"],
                        record["strategy_hash"],
                        record["model_hash"],
                        record["config"],
                        start_timestamp,
                        record["bankroll"],
                        tuple(record["allowed_markets"]),
                        record["risk_limits"],
                        ResearchQuality(record["quality"]),
                        start_timestamp,
                    )
                )
            return tuple(specs)
        return tuple(sorted(self._specs.values(), key=lambda spec: (spec.start_timestamp, spec.experiment_id)))
    def get(self, experiment_id: str) -> ForwardTestSpec | None:
        identifier = str(experiment_id).strip()
        if not identifier:
            return None
        if self.store is None:
            return self._specs.get(identifier)
        record = self.store.load_forward_test(identifier)
        if record is None:
            return None
        start_timestamp = parse_timestamp(record["start_timestamp"])
        if start_timestamp is None:
            raise ValueError(f"invalid persisted forward-test timestamp: {identifier}")
        return ForwardTestSpec(
            record["experiment_id"],
            record["strategy_hash"],
            record["model_hash"],
            record["config"],
            start_timestamp,
            record["bankroll"],
            tuple(record["allowed_markets"]),
            record["risk_limits"],
            ResearchQuality(record["quality"]),
            start_timestamp,
        )
def _frozen_runtime_documents(
    strategy: Any,
    model: Any,
    config: Mapping[str, Any] | None,
) -> tuple[Any, Any]:
    """Validate runtime documents against configured frozen documents."""
    config_document = config.get("strategy_document") if isinstance(config, Mapping) else None
    config_model_document = config.get("model_document") if isinstance(config, Mapping) else None
    if (
        isinstance(config_document, Mapping)
        and _content_hash(_normalized_strategy_document(config_document))
        != _content_hash(_normalized_strategy_document(strategy))
    ):
        raise ValueError("config strategy_document does not match frozen strategy")
    model_source = getattr(model, "document", model)
    configured_model = config_model_document
    if _model_free_marker(configured_model):
        if _prediction_strategy_model_required(strategy) is not False:
            raise ValueError("MODEL_INPUT_MISSING")
        model_source = dict(_MODEL_FREE_DOCUMENT)
        configured_model = dict(_MODEL_FREE_DOCUMENT)
    if (
        isinstance(configured_model, Mapping)
        and _canonical(configured_model) != _canonical(model_source)
    ):
        raise ValueError("config model_document does not match frozen model")
    return (
        config_document if isinstance(config_document, Mapping) else strategy,
        configured_model if isinstance(configured_model, Mapping) else model_source,
    )


def _normalized_strategy_document(value: Any) -> Any:
    if isinstance(value, Mapping):
        try:
            from .strategy import load_strategy

            return load_strategy(value).to_dict()
        except Exception:
            return value
    return value


# These are deliberately data-only.  The operational setup is the contract
# carried by new directional prediction records; legacy records without it
# remain readable and are never backfilled by ``ForwardTestSpec``.
OPERATIONAL_SETUP_SCHEMA = "axiom-operational-setup"
OPERATIONAL_SETUP_VERSION = "1"
_DIRECTIONAL_SETUP_FAMILIES = frozenset({"momentum", "mean_reversion"})
_ABSOLUTE_MOVE_PREDICATE = {
    "version": "absolute-move-v1",
    "minimum_move": 0.05,
    "units": "probability",
    "boundary": "inclusive",
}
def _operational_dataset_identity(
    config: Mapping[str, Any],
) -> tuple[Any, Any]:
    """Require one dataset identity across every bounded setup authority."""
    containers: list[tuple[str, Mapping[str, Any]]] = [("config", config)]
    for name in (
        "dataset_selector",
        "dataset_boundary",
        "dataset_attestation",
        "assessment_manifest_ref",
        "experiment_plan",
        "plan",
        "forward_config",
        "config",
    ):
        value = config.get(name)
        if isinstance(value, Mapping):
            containers.append((name, value))
    values: dict[str, list[Any]] = {"dataset_id": [], "dataset_version": []}

    def add(field: str, value: Any) -> None:
        if value not in (None, ""):
            values[field].append(value)

    for name, container in containers:
        add("dataset_id", container.get("dataset_id"))
        add("dataset_version", container.get("dataset_version"))
        if name in {"dataset_selector", "dataset_boundary", "dataset_attestation"}:
            add("dataset_version", container.get("version"))
        selector = container.get("dataset_selector")
        if isinstance(selector, Mapping):
            add("dataset_id", selector.get("dataset_id"))
            add(
                "dataset_version",
                selector.get("dataset_version", selector.get("version")),
            )
        for binding_name in ("dataset_boundary", "dataset_attestation"):
            binding = container.get(binding_name)
            if isinstance(binding, Mapping):
                add("dataset_id", binding.get("dataset_id"))
                add(
                    "dataset_version",
                    binding.get("dataset_version", binding.get("version")),
                )
    result: list[Any] = []
    for field in ("dataset_id", "dataset_version"):
        distinct = {_canonical(value) for value in values[field]}
        if len(distinct) > 1:
            raise ValueError(f"conflicting operational dataset identity: {field}")
        result.append(values[field][0] if values[field] else None)
    return result[0], result[1]




def _operational_setup_for_strategy(
    strategy: Any,
    config: Mapping[str, Any] | None = None,
) -> dict[str, Any] | None:
    """Return the canonical setup for one exact directional strategy.

    Setup derivation is deliberately stricter than the general strategy
    compatibility helpers.  A malformed or non-canonical document must never
    acquire the directional paper contract through fallback parsing or scalar
    coercion.
    """
    try:
        from .strategy import load_strategy

        document = _plain_json(load_strategy(strategy).to_dict())
    except Exception:
        return None
    if not isinstance(document, Mapping):
        return None
    if document.get("market_type") != "prediction":
        return None
    family = document.get("family")
    if not isinstance(family, str) or family not in _DIRECTIONAL_SETUP_FAMILIES:
        return None
    parameters = document.get("parameters")
    if not isinstance(parameters, Mapping):
        return None
    lookback = parameters.get("lookback")
    threshold = parameters.get("threshold")
    if (
        type(lookback) is not int
        or lookback != 1
        or type(threshold) is not float
        or not math.isfinite(threshold)
        or threshold != 0.05
    ):
        return None
    predicate = parameters.get("entry_predicate")
    if (
        not isinstance(predicate, Mapping)
        or _canonical(predicate) != _canonical(_ABSOLUTE_MOVE_PREDICATE)
    ):
        return None

    source = _plain_json(dict(config or {}))
    dataset_id, dataset_version = _operational_dataset_identity(source)
    raw_manifest = source.get("assessment_manifest_ref")
    boundary = source.get("dataset_boundary")
    attestation = source.get("dataset_attestation")
    if boundary is not None and not isinstance(boundary, Mapping):
        return None
    if attestation is not None and not isinstance(attestation, Mapping):
        return None
    attestation_hash = (
        str(attestation.get("attestation_hash", "")).strip()
        if isinstance(attestation, Mapping)
        else ""
    )
    if isinstance(raw_manifest, Mapping):
        manifest = _plain_json(raw_manifest)
        if not isinstance(manifest, Mapping):
            return None
        manifest = dict(manifest)
    else:
        manifest = {
            "kind": "dataset_boundary" if isinstance(boundary, Mapping) else "dataset_attestation",
        }
    if dataset_id not in (None, ""):
        manifest["dataset_id"] = dataset_id
    if dataset_version not in (None, ""):
        manifest["dataset_version"] = dataset_version
    if isinstance(boundary, Mapping):
        manifest["dataset_boundary"] = dict(boundary)
        manifest["manifest_digest"] = boundary.get(
            "ordered_row_manifest_digest",
            boundary.get("content_hash"),
        )
    if attestation_hash:
        manifest["attestation_hash"] = attestation_hash
    if source.get("plan_hash") is not None:
        manifest["plan_hash"] = source.get("plan_hash")
    manifest = {
        key: value
        for key, value in manifest.items()
        if value is not None and value != ""
    }

    scope_source = source.get("market_scope", source.get("scope"))
    if isinstance(scope_source, Mapping):
        try:
            from .experiment_plan import normalize_market_scope

            scope = normalize_market_scope(scope_source).as_dict()
        except (TypeError, ValueError):
            return None
    else:
        scope = {
            "schema_version": "1",
            "mode": "RULE_BASED_MARKETS",
            "instrument": "POLYMARKET",
            "categories": [],
            "market_ids": [],
            "filters": {},
            "regime_restrictions": [],
            "provenance": "canonical",
        }
    setup_id = f"{family}:absolute-move-v1:L1:H1"
    return {
        "contract_schema": OPERATIONAL_SETUP_SCHEMA,
        "contract_version": OPERATIONAL_SETUP_VERSION,
        "setup_id": setup_id,
        "family": family,
        "market_type": "prediction",
        "market_scope_policy": scope,
        "assessment_manifest_ref": manifest,
        "required_observations": {
            "path_length": 3,
            "lookback": 1,
            "holding": 1,
            "unit": "observations",
            "same_market": True,
        },
        "lookback": 1,
        "entry_predicate": dict(_ABSOLUTE_MOVE_PREDICATE),
        "entry_predicate_raw": "abs(delta_probability) >= 0.05",
        "outcome_mapping": {
            "positive_delta": "BUY YES",
            "negative_delta": "BUY NO",
            "buy_interpretation": "BUY",
        },
        "signal_strength": {
            "formula": "delta_probability / 0.05",
            "raw_measure": "delta_probability",
            "scale": "threshold",
            "threshold": 0.05,
            "eligibility_separate": True,
        },
        "sizing": {
            "rule": "active_settings",
            "settings_reference": "active_runtime_settings",
            "allocation_reference": "paper_assumptions.sizing.allocated_capital",
            "limit_reference": "active_settings",
            "hard_coded_cap": False,
        },
        "invalidation": {
            "data_blockers": [
                "INSUFFICIENT_LOOKBACK",
                "MISSING_MARKET_PROBABILITY",
                "MARKET_ID_MISSING",
                "INCOMPLETE_SAME_MARKET_PATH",
                "UNRESOLVED_MARKET",
            ],
            "invalid_if": "any required observation is absent, non-finite, out of order, or from another market",
        },
        "holding_semantics": {
            "count": 1,
            "unit": "same_market_observation",
            "same_market": True,
            "pending": "retain until the next same-market observation",
            "unresolved": "do not mark an outcome settled",
            "terminal": "settle only a terminal outcome; do not extend the path",
        },
        "book_assumptions": {
            "source": "recorded_book",
            "fee_bps": 10.0,
            "slippage_bps": 5.0,
            "quote_policy": "recorded_book_only",
        },
        "model_required": False,
        "paper_only": True,
        "paper_only_statement": "Paper-only research; no live capability or submission path.",
    }


def _operational_setup_hash(setup: Mapping[str, Any]) -> str:
    return _content_hash(_plain_json(setup))


def _is_directional_setup_candidate(strategy: Any) -> bool:
    try:
        from .strategy import load_strategy

        document = _plain_json(load_strategy(strategy).to_dict())
    except Exception:
        return False
    parameters = document.get("parameters") if isinstance(document, Mapping) else None
    predicate = parameters.get("entry_predicate") if isinstance(parameters, Mapping) else None
    return (
        isinstance(document, Mapping)
        and document.get("market_type") == "prediction"
        and document.get("family") in _DIRECTIONAL_SETUP_FAMILIES
        and isinstance(predicate, Mapping)
        and _canonical(predicate) == _canonical(_ABSOLUTE_MOVE_PREDICATE)
    )


def _bind_operational_setup(
    strategy: Any,
    config: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Canonicalize a new config's setup while preserving supplied identity."""
    result = _plain_json(dict(config or {}))
    handoff = result.get("observation_handoff")
    strict_observation = result.get("canonical_operational_setup_required") is True
    setup = _operational_setup_for_strategy(strategy, result)
    if setup is None:
        if strict_observation:
            raise ValueError("OPERATIONAL_SETUP_UNSUPPORTED")
        return result
    supplied = result.get("operational_setup")
    if supplied is not None and _canonical(supplied) != _canonical(setup):
        raise ValueError("operational_setup does not match the exact strategy contract")
    supplied_hash = result.get("operational_setup_hash")
    setup_hash = _operational_setup_hash(setup)
    if supplied_hash not in (None, "") and str(supplied_hash).strip() != setup_hash:
        raise ValueError("operational_setup_hash does not match operational_setup")
    result["operational_setup"] = setup
    result["operational_setup_hash"] = setup_hash
    return result



def _content_hash(value: Any) -> str:
    return "sha256:" + hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _canonical(value: Any) -> str:
    def convert(item: Any) -> Any:
        if isinstance(item, Mapping):
            return {str(key): convert(child) for key, child in sorted(item.items(), key=lambda pair: str(pair[0]))}
        if isinstance(item, (list, tuple)):
            return [convert(child) for child in item]
        if isinstance(item, datetime):
            return ensure_utc(item).isoformat()
        if hasattr(item, "value") and not isinstance(item, (str, bytes)):
            return item.value
        if hasattr(item, "to_dict") and callable(item.to_dict):
            return convert(item.to_dict())
        return item
    return json.dumps(convert(value), sort_keys=True, separators=(",", ":"), allow_nan=False, default=repr)


class _FrozenList(list):
    """Immutable list representation that preserves canonical JSON equality."""

    __slots__ = ()

    @staticmethod
    def _immutable(*_args: Any, **_kwargs: Any) -> None:
        raise TypeError("frozen forward configuration is immutable")

    __setitem__ = _immutable
    __delitem__ = _immutable
    __iadd__ = _immutable
    __imul__ = _immutable
    append = _immutable
    clear = _immutable
    extend = _immutable
    insert = _immutable
    pop = _immutable
    remove = _immutable
    reverse = _immutable
    sort = _immutable


def _freeze_json(value: Mapping[str, Any]) -> Mapping[str, Any]:
    def freeze(item: Any, *, preserve_lists: bool = False) -> Any:
        if isinstance(item, Mapping):
            return MappingProxyType(
                {
                    str(key): freeze(
                        child,
                        preserve_lists=preserve_lists or str(key) == "operational_setup",
                    )
                    for key, child in item.items()
                }
            )
        if isinstance(item, list):
            frozen = [freeze(child, preserve_lists=preserve_lists) for child in item]
            return _FrozenList(frozen) if preserve_lists else tuple(frozen)
        return item

    return freeze(json.loads(_canonical(value)))

def _plain(value: Any) -> Any:
    return json.loads(_canonical(value))


__all__ = [
    "COMMON_PAPER_ASSUMPTIONS",
    "ForwardTestRegistry",
    "ForwardTestSpec",
    "OPERATIONAL_SETUP_SCHEMA",
    "OPERATIONAL_SETUP_VERSION",
    "_ABSOLUTE_MOVE_PREDICATE",
    "_bind_operational_setup",
    "_operational_setup_for_strategy",
    "_operational_setup_hash",
]
