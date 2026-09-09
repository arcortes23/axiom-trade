"""Bounded, auditable handling for pre-canonical Polymarket scopes.

Legacy scope fields are interpreted once through :func:`normalize_market_scope`.
Only a single unambiguous legacy document may be converted, and conversion
always creates a new proposal.  The source document is never written back or
used as an executable alias.
"""
from __future__ import annotations

from dataclasses import dataclass, replace
from enum import Enum
import hashlib
import json
import math
from types import MappingProxyType
from typing import Any, Mapping

from .experiment_plan import (
    ExperimentPlan,
    ExperimentPlanError,
    MarketScopePolicy,
    normalize_market_scope,
)
from .research_bus import DurableResearchBus, ResearchBusPermissionError, ResearchQueueItem
from .storage import AxiomStore


class LegacyScopeClassification(str, Enum):
    """The bounded outcome of inspecting one frozen candidate document."""

    CANONICAL_VALID = "CANONICAL_VALID"
    LEGACY_UNAMBIGUOUS = "LEGACY_UNAMBIGUOUS"
    LEGACY_AMBIGUOUS = "LEGACY_AMBIGUOUS"
    INVALID = "INVALID"


# String constants mirror the existing scope APIs and make JSON consumers
# independent of Enum implementation details.
CANONICAL_VALID = LegacyScopeClassification.CANONICAL_VALID.value
LEGACY_UNAMBIGUOUS = LegacyScopeClassification.LEGACY_UNAMBIGUOUS.value
LEGACY_AMBIGUOUS = LegacyScopeClassification.LEGACY_AMBIGUOUS.value
INVALID = LegacyScopeClassification.INVALID.value


class LegacyScopeError(ValueError):
    """Raised when a legacy scope cannot safely become a successor proposal."""

    def __init__(self, reason: str, detail: str) -> None:
        self.reason = str(reason).strip().upper() or "INVALID_LEGACY_SCOPE"
        self.detail = str(detail).strip() or self.reason
        super().__init__(f"{self.reason}: {self.detail}")


@dataclass(frozen=True, slots=True)
class LegacyScopeAssessment:
    """Immutable result of classifying one source candidate/document."""

    classification: str
    reason: str
    candidate_id: str | None = None
    frozen_hash: str | None = None
    scope: MarketScopePolicy | None = None
    scope_hash: str | None = None
    scope_version: str | None = None

    def __post_init__(self) -> None:
        classification = (
            self.classification.value
            if isinstance(self.classification, LegacyScopeClassification)
            else str(self.classification).strip().upper()
        )
        if classification not in {item.value for item in LegacyScopeClassification}:
            raise ValueError(f"unsupported legacy scope classification {classification!r}")
        object.__setattr__(self, "classification", classification)
        object.__setattr__(self, "reason", str(self.reason).strip().upper() or classification)
        for field_name in ("candidate_id", "frozen_hash", "scope_hash", "scope_version"):
            value = getattr(self, field_name)
            if value is not None:
                text = str(value).strip()
                object.__setattr__(self, field_name, text or None)

    @property
    def canonical_scope(self) -> MarketScopePolicy | None:
        return self.scope

    @property
    def normalized_scope(self) -> MarketScopePolicy | None:
        return self.scope

    @property
    def predecessor_candidate_id(self) -> str | None:
        return self.candidate_id

    @property
    def predecessor_frozen_hash(self) -> str | None:
        return self.frozen_hash

    @property
    def normalized(self) -> Mapping[str, Any] | None:
        return self.scope.as_dict() if self.scope is not None else None

    @property
    def enqueueable(self) -> bool:
        return self.classification == LEGACY_UNAMBIGUOUS and self.scope is not None
    @property
    def executable(self) -> bool:
        """Whether this result may be converted (never eligibility itself)."""
        return self.enqueueable

    def as_record(self) -> dict[str, Any]:
        return {
            "classification": self.classification,
            "reason": self.reason,
            "candidate_id": self.candidate_id,
            "frozen_hash": self.frozen_hash,
            "scope": self.scope.as_dict() if self.scope is not None else None,
            "scope_hash": self.scope_hash,
            "scope_version": self.scope_version,
            "enqueueable": self.enqueueable,
        }


@dataclass(frozen=True, slots=True)
class LegacyScopeSuccessor:
    """A new canonical proposal related to one immutable predecessor."""

    successor_id: str
    predecessor_candidate_id: str
    predecessor_frozen_hash: str
    proposal: Mapping[str, Any]
    plan: ExperimentPlan
    assessment: LegacyScopeAssessment
    queue_item: ResearchQueueItem | None = None

    @property
    def identity(self) -> str:
        return self.successor_id

    @property
    def successor_candidate_id(self) -> str:
        return self.successor_id

    @property
    def predecessor_hash(self) -> str:
        return self.predecessor_frozen_hash
    @property
    def proposal_id(self) -> str:
        return self.successor_id

    @property
    def queue_item_id(self) -> str | None:
        return self.queue_item.item_id if self.queue_item is not None else None

    @property
    def successor_document(self) -> Mapping[str, Any]:
        return self.proposal

    @property
    def candidate_document(self) -> Mapping[str, Any]:
        return self.proposal

    def as_record(self) -> dict[str, Any]:
        return {
            "successor_id": self.successor_id,
            "proposal_id": self.successor_id,
            "predecessor_candidate_id": self.predecessor_candidate_id,
            "predecessor_frozen_hash": self.predecessor_frozen_hash,
            "proposal": dict(self.proposal),
            "plan": self.plan.as_dict(),
            "assessment": self.assessment.as_record(),
            "queue_item_id": self.queue_item_id,
            "queue_status": self.queue_item.status.value if self.queue_item is not None else None,
        }


_SCOPE_ALIASES = (
    "target",
    "market_ids",
    "target_market_ids",
    "target_instrument",
    "instrument",
    "categories",
    "filters",
    "regime_restrictions",
)
_SCOPE_SOURCE_FIELDS = (*_SCOPE_ALIASES, "frozen_filters")
_PLAN_FIELDS = frozenset(
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
        "max_variants",
        "min_samples",
        "minimum_samples",
        "min_trades",
        "minimum_trades",
        "paper_only",
    }
)
_SCOPE_PLAN_COMPATIBILITY_FIELDS = frozenset(
    {
        "target",
        "target_instrument",
        "market_ids",
        "target_market_ids",
        "instrument",
        "categories",
        "filters",
        "regime_restrictions",
        "market_scope_hash",
        "market_scope_version",
    }
)
_MAX_SCOPE_INPUT_BYTES = 16_384
_MAX_SCOPE_INPUT_DEPTH = 8
_MAX_SCOPE_INPUT_ITEMS = 256
_MAX_SCOPE_INPUT_STRING = 4_096


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def _bounded(value: Any, *, depth: int = 0, path: str = "document") -> Any:
    """Copy only JSON values while imposing a small inspection budget."""
    if depth > _MAX_SCOPE_INPUT_DEPTH:
        raise LegacyScopeError("INVALID", f"legacy scope document nesting exceeds {_MAX_SCOPE_INPUT_DEPTH}: {path}")
    if isinstance(value, Mapping):
        if len(value) > _MAX_SCOPE_INPUT_ITEMS:
            raise LegacyScopeError("INVALID", f"legacy scope document mapping is too large: {path}")
        result: dict[str, Any] = {}
        for key, child in value.items():
            key_text = str(key)
            if len(key_text) > _MAX_SCOPE_INPUT_STRING:
                raise LegacyScopeError("INVALID", f"legacy scope document key is too long: {path}")
            result[key_text] = _bounded(child, depth=depth + 1, path=f"{path}.{key_text}")
        return result
    if isinstance(value, (list, tuple)):
        if len(value) > _MAX_SCOPE_INPUT_ITEMS:
            raise LegacyScopeError("INVALID", f"legacy scope document collection is too large: {path}")
        return [_bounded(child, depth=depth + 1, path=f"{path}[]") for child in value]
    if isinstance(value, (set, frozenset)):
        if len(value) > _MAX_SCOPE_INPUT_ITEMS:
            raise LegacyScopeError("INVALID", f"legacy scope document collection is too large: {path}")
        copied = [_bounded(child, depth=depth + 1, path=f"{path}[]") for child in value]
        return sorted(copied, key=lambda item: _canonical(item))
    if isinstance(value, (str, int, bool)) or value is None:
        if isinstance(value, str) and len(value) > _MAX_SCOPE_INPUT_STRING:
            raise LegacyScopeError("INVALID", f"legacy scope document string is too long: {path}")
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise LegacyScopeError("INVALID", f"legacy scope document contains a non-finite value: {path}")
        return value
    raise LegacyScopeError("INVALID", f"unsupported legacy scope document value at {path}: {type(value).__name__}")


def _text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _first_text(source: Mapping[str, Any], names: tuple[str, ...]) -> str | None:
    for name in names:
        value = _text(source.get(name))
        if value:
            return value
    return None

def _resolve_document(
    document: Mapping[str, Any] | None,
    source_document: Mapping[str, Any] | None,
) -> Mapping[str, Any] | None:
    if document is not None and source_document is not None:
        raise TypeError("pass either document or source_document, not both")
    return source_document if document is None else document



def _references(document: Mapping[str, Any], source: Mapping[str, Any]) -> tuple[str | None, str | None]:
    candidate_id = _first_text(document, ("candidate_id", "predecessor_candidate_id"))
    candidate_id = candidate_id or _first_text(source, ("candidate_id", "predecessor_candidate_id"))
    frozen_hash = _first_text(document, ("frozen_hash", "predecessor_frozen_hash", "source_frozen_hash"))
    frozen_hash = frozen_hash or _first_text(source, ("frozen_hash", "predecessor_frozen_hash", "source_frozen_hash"))
    for parent in (document.get("binding"), source.get("binding"), document.get("immutable_hashes"), source.get("immutable_hashes"), source.get("source_binding")):
        if not isinstance(parent, Mapping):
            continue
        candidate_id = candidate_id or _first_text(parent, ("candidate_id", "predecessor_candidate_id"))
        frozen_hash = frozen_hash or _first_text(parent, ("frozen_hash", "predecessor_frozen_hash", "source_frozen_hash"))
    return candidate_id, frozen_hash


def _scope_source(document: Mapping[str, Any]) -> tuple[Mapping[str, Any], Mapping[str, Any]]:
    payload = document.get("payload")
    root = payload if isinstance(payload, Mapping) else document
    plan = root.get("experiment_plan") if isinstance(root, Mapping) else None
    source = plan if isinstance(plan, Mapping) else root
    # A row may persist a scope next to an embedded plan.  The embedded plan
    # remains authoritative when present; top-level aliases are only a
    # fallback when it has no scope material.
    if isinstance(plan, Mapping) and not any(key in plan for key in (*_SCOPE_SOURCE_FIELDS, "market_scope")):
        if any(key in root for key in (*_SCOPE_SOURCE_FIELDS, "market_scope")):
            source = root
    return root, source


def _scope_arguments(source: Mapping[str, Any]) -> dict[str, Any]:
    result = {name: source.get(name) for name in _SCOPE_ALIASES if name in source}
    if "filters" not in result and "frozen_filters" in source:
        result["filters"] = source["frozen_filters"]
    return result
def _legacy_conflict(source: Mapping[str, Any]) -> bool:
    """Catch cross-alias conflicts not represented by normalize's precedence."""
    target = source.get("target")
    if not isinstance(target, Mapping):
        return False

    def equivalent(left: Any, right: Any, *, field: str) -> bool:
        try:
            left_policy = normalize_market_scope(None, **{field: left})
            right_policy = normalize_market_scope(None, **{field: right})
        except (ExperimentPlanError, TypeError, ValueError):
            return True
        return _canonical(_canonical_policy(left_policy).as_dict()) == _canonical(
            _canonical_policy(right_policy).as_dict()
        )

    nested_ids = next(
        (target[name] for name in ("market_ids", "exact_market_ids", "markets") if name in target),
        None,
    )
    outer_ids = next(
        (source[name] for name in ("market_ids", "target_market_ids") if name in source),
        None,
    )
    if nested_ids is not None and outer_ids is not None and not equivalent(nested_ids, outer_ids, field="market_ids"):
        return True
    nested_instrument = next(
        (target[name] for name in ("instrument", "symbol") if name in target),
        None,
    )
    outer_instrument = next(
        (source[name] for name in ("instrument", "target_instrument") if name in source),
        None,
    )
    if nested_instrument is not None and outer_instrument is not None and not equivalent(
        nested_instrument,
        outer_instrument,
        field="target_instrument",
    ):
        return True
    nested_categories = next(
        (target[name] for name in ("categories", "category") if name in target),
        None,
    )
    outer_categories = source.get("categories")
    if nested_categories is not None and outer_categories is not None and not equivalent(
        nested_categories,
        outer_categories,
        field="categories",
    ):
        return True
    return False


def _scope_source(document: Mapping[str, Any]) -> tuple[Mapping[str, Any], Mapping[str, Any]]:
    payload = document.get("payload")
    root = payload if isinstance(payload, Mapping) else document
    plan = root.get("experiment_plan") if isinstance(root, Mapping) else None
    source = plan if isinstance(plan, Mapping) else root
    # A row may persist a scope next to an embedded plan.  The embedded plan
    # remains authoritative when present; top-level aliases are only a
    # fallback when it has no scope material.
    if isinstance(plan, Mapping) and not any(key in plan for key in (*_SCOPE_SOURCE_FIELDS, "market_scope")):
        if any(key in root for key in (*_SCOPE_SOURCE_FIELDS, "market_scope")):
            source = root
    return root, source


def classify_legacy_scope(
    document: Mapping[str, Any] | None = None,
    *,
    source_document: Mapping[str, Any] | None = None,
) -> LegacyScopeAssessment:
    """Classify one candidate/document without mutating or persisting it."""
    document = _resolve_document(document, source_document)
    if not isinstance(document, Mapping):
        return LegacyScopeAssessment(INVALID, "MALFORMED_DOCUMENT")
    try:
        bounded_document = _bounded(document)
        encoded = _canonical(bounded_document).encode("utf-8")
        if len(encoded) > _MAX_SCOPE_INPUT_BYTES:
            return LegacyScopeAssessment(INVALID, "DOCUMENT_TOO_LARGE")
    except LegacyScopeError as exc:
        return LegacyScopeAssessment(INVALID, exc.reason)
    root, source = _scope_source(bounded_document)
    candidate_id, frozen_hash = _references(bounded_document, root)
    if source is not root:
        for name in _SCOPE_SOURCE_FIELDS + ("market_scope",):
            if name in source and name in root and _canonical(source[name]) != _canonical(root[name]):
                return LegacyScopeAssessment(LEGACY_AMBIGUOUS, "CONFLICTING_MARKET_SCOPE", candidate_id, frozen_hash)
        merged_source = dict(root)
        merged_source.update(source)
        source = merged_source
    explicit = source.get("market_scope")
    if explicit is None and source is not root:
        explicit = root.get("market_scope")
    arguments = _scope_arguments(source)
    if _legacy_conflict(source):
        return LegacyScopeAssessment(LEGACY_AMBIGUOUS, "CONFLICTING_MARKET_SCOPE", candidate_id, frozen_hash)
    if explicit is not None:
        try:
            policy = normalize_market_scope(explicit, **arguments)
        except ExperimentPlanError as exc:
            classification = LEGACY_AMBIGUOUS if exc.reason == "CONFLICTING_MARKET_SCOPE" else INVALID
            return LegacyScopeAssessment(classification, exc.reason, candidate_id, frozen_hash)
        except (TypeError, ValueError):
            return LegacyScopeAssessment(INVALID, "MALFORMED_MARKET_SCOPE", candidate_id, frozen_hash)
        return LegacyScopeAssessment(
            CANONICAL_VALID,
            "CANONICAL_SCOPE",
            candidate_id,
            frozen_hash,
            policy,
            policy.scope_hash,
            policy.scope_version,
        )
    if not any(name in source for name in _SCOPE_SOURCE_FIELDS):
        return LegacyScopeAssessment(INVALID, "MISSING_MARKET_SCOPE", candidate_id, frozen_hash)
    try:
        policy = normalize_market_scope(None, **arguments)
    except ExperimentPlanError as exc:
        classification = LEGACY_AMBIGUOUS if exc.reason == "CONFLICTING_MARKET_SCOPE" else INVALID
        return LegacyScopeAssessment(classification, exc.reason, candidate_id, frozen_hash)
    except (TypeError, ValueError):
        return LegacyScopeAssessment(INVALID, "MALFORMED_MARKET_SCOPE", candidate_id, frozen_hash)
    if policy.mode == "RESEARCH_ONLY":
        return LegacyScopeAssessment(INVALID, "MISSING_MARKET_SCOPE", candidate_id, frozen_hash)
    return LegacyScopeAssessment(
        LEGACY_UNAMBIGUOUS,
        "LEGACY_SCOPE_NORMALIZED",
        candidate_id,
        frozen_hash,
        policy,
        policy.scope_hash,
        policy.scope_version,
    )


def _canonical_policy(policy: MarketScopePolicy) -> MarketScopePolicy:
    material = policy.as_dict()
    material["provenance"] = "canonical"
    material["categories"] = sorted(policy.categories)
    material["market_ids"] = sorted(policy.market_ids)
    filters = dict(material.get("filters") or {})
    if isinstance(filters.get("category"), list):
        filters["category"] = sorted(filters["category"])
    material["filters"] = filters
    restrictions = dict(material.get("regime_restrictions") or {})
    if isinstance(restrictions.get("regimes"), list):
        restrictions["regimes"] = sorted(restrictions["regimes"])
    material["regime_restrictions"] = restrictions
    return MarketScopePolicy.from_mapping(material)


def _plan_document(root: Mapping[str, Any]) -> dict[str, Any]:
    nested = root.get("experiment_plan")
    if isinstance(nested, Mapping):
        result = {str(key): value for key, value in nested.items()}
    else:
        result = {str(key): root[key] for key in _PLAN_FIELDS if key in root}
    for field_name in _SCOPE_PLAN_COMPATIBILITY_FIELDS:
        result.pop(field_name, None)
    return result


def _source_statement(root: Mapping[str, Any], candidate_id: str) -> str:
    value = root.get("statement", root.get("hypothesis"))
    text = _text(value)
    return text or f"Revalidate legacy market scope predecessor {candidate_id}"


def _source_tests(root: Mapping[str, Any]) -> list[str]:
    value = root.get("tests", root.get("validation_plan"))
    if isinstance(value, (list, tuple)) and value and all(isinstance(item, str) and item.strip() for item in value):
        return [str(item).strip() for item in value[:16]]
    return ["bounded chronological backtest and validation of canonical successor scope"]


def create_legacy_successor(
    document: Mapping[str, Any] | None = None,
    *,
    source_document: Mapping[str, Any] | None = None,
    source_candidate_id: str | None = None,
    source_frozen_hash: str | None = None,
) -> LegacyScopeSuccessor:
    """Create one deterministic canonical successor without queue/lifecycle writes."""
    document = _resolve_document(document, source_document)
    assessment = classify_legacy_scope(document)
    if assessment.classification != LEGACY_UNAMBIGUOUS or assessment.scope is None:
        raise LegacyScopeError(assessment.reason, f"legacy scope classification is {assessment.classification}")
    supplied_candidate_id = _text(source_candidate_id)
    supplied_frozen_hash = _text(source_frozen_hash)
    if supplied_candidate_id and assessment.candidate_id and supplied_candidate_id != assessment.candidate_id:
        raise LegacyScopeError("PREDECESSOR_MISMATCH", "source_candidate_id does not match the frozen document")
    if supplied_frozen_hash and assessment.frozen_hash and supplied_frozen_hash != assessment.frozen_hash:
        raise LegacyScopeError("PREDECESSOR_MISMATCH", "source_frozen_hash does not match the frozen document")
    candidate_id = supplied_candidate_id or assessment.candidate_id
    frozen_hash = supplied_frozen_hash or assessment.frozen_hash
    if not candidate_id:
        raise LegacyScopeError("MISSING_PREDECESSOR_CANDIDATE", "predecessor candidate_id is required")
    if not frozen_hash:
        raise LegacyScopeError("MISSING_PREDECESSOR_FROZEN_HASH", "predecessor frozen hash is required")
    root, _ = _scope_source(_bounded(document))
    canonical_scope = _canonical_policy(assessment.scope)
    plan_document = _plan_document(root)
    plan_document["market_scope"] = canonical_scope.as_dict()
    try:
        provisional = ExperimentPlan.from_mapping(plan_document, hypothesis_id=candidate_id)
    except ExperimentPlanError as exc:
        raise LegacyScopeError(exc.reason, exc.detail) from exc
    identity_material = {
        "schema": "legacy-scope-successor-v1",
        "predecessor_candidate_id": candidate_id,
        "predecessor_frozen_hash": frozen_hash,
        # The canonical scope hash is order-independent by contract, unlike
        # legacy list presentation and the derived plan compatibility views.
        "canonical_scope_hash": canonical_scope.scope_hash,
        "canonical_scope_version": canonical_scope.scope_version,
    }
    successor_id = "legacy-successor-" + hashlib.sha256(_canonical(identity_material).encode("utf-8")).hexdigest()[:24]
    final_document = provisional.as_dict()
    final_document["hypothesis_id"] = successor_id
    final_document["plan_id"] = "plan-legacy-" + hashlib.sha256(successor_id.encode("utf-8")).hexdigest()[:24]
    final_document["market_scope"] = canonical_scope.as_dict()
    try:
        plan = ExperimentPlan.from_mapping(final_document)
    except ExperimentPlanError as exc:
        raise LegacyScopeError(exc.reason, exc.detail) from exc
    proposal: dict[str, Any] = {
        "proposal_id": successor_id,
        "statement": _source_statement(root, candidate_id),
        "source": _text(root.get("source")) or "legacy-scope-migration",
        "tests": _source_tests(root),
        "dataset_version": plan.dataset_version,
        "time_split": str(plan.methodology.get("time_split", "train-validation-holdout")),
        "paper_only": True,
        "experiment_plan": plan.as_dict(),
        "successor_candidate_id": successor_id,
        "predecessor_candidate_id": candidate_id,
        "predecessor_frozen_hash": frozen_hash,
        "successor_scope_hash": plan.market_scope_hash,
        "successor_scope_version": plan.market_scope_version,
        "legacy_scope_classification": LEGACY_UNAMBIGUOUS,
        "successor_relation": "LEGACY_SCOPE_SUCCESSOR",
        "provenance": {
            "type": "legacy_scope_successor",
            "predecessor_candidate_id": candidate_id,
            "predecessor_frozen_hash": frozen_hash,
            "legacy_scope_hash": assessment.scope_hash,
            "legacy_scope_version": assessment.scope_version,
            "canonical_scope_hash": plan.market_scope_hash,
            "canonical_scope_version": plan.market_scope_version,
        },
    }
    return LegacyScopeSuccessor(
        successor_id,
        candidate_id,
        frozen_hash,
        MappingProxyType(proposal),
        plan,
        assessment,
    )


def enqueue_legacy_successor(
    document: Mapping[str, Any] | None = None,
    *,
    source_document: Mapping[str, Any] | None = None,
    store: AxiomStore | None = None,
    bus: DurableResearchBus | None = None,
    source_candidate_id: str | None = None,
    source_frozen_hash: str | None = None,
    priority: int = 0,
) -> LegacyScopeSuccessor:
    """Validate and enqueue exactly one successor through the ordinary bus."""
    document = _resolve_document(document, source_document)
    successor = create_legacy_successor(
        document,
        source_candidate_id=source_candidate_id,
        source_frozen_hash=source_frozen_hash,
    )
    if bus is None:
        if store is None:
            raise ValueError("store or bus is required")
        bus = DurableResearchBus(store, source="legacy-scope", author="legacy-scope")
    if store is None:
        store = getattr(bus, "_store", None)
    from .director import validate_hermes_proposal

    validation = validate_hermes_proposal(successor.proposal, store=store)
    if not validation.accepted:
        detail = "; ".join(validation.reasons) or "proposal rejected"
        raise LegacyScopeError("PROPOSAL_REJECTED", detail)
    try:
        item = bus.submit_hypothesis(
            validation.normalized or successor.proposal,
            dedupe_key=f"legacy-successor:{successor.successor_id}",
            lineage=(successor.predecessor_candidate_id, successor.predecessor_frozen_hash),
            priority=priority,
        )
    except ResearchBusPermissionError as exc:
        raise LegacyScopeError("PROPOSAL_REJECTED", str(exc)) from exc
    return replace(successor, queue_item=item)


# Explicitly named aliases for callers that describe this action as proposing
# rather than creating.  Both names resolve to the same implementation and do
# not establish a second scope authority.
propose_legacy_successor = create_legacy_successor


__all__ = [
    "CANONICAL_VALID",
    "INVALID",
    "LEGACY_AMBIGUOUS",
    "LEGACY_UNAMBIGUOUS",
    "LegacyScopeAssessment",
    "LegacyScopeClassification",
    "LegacyScopeError",
    "LegacyScopeSuccessor",
    "classify_legacy_scope",
    "create_legacy_successor",
    "enqueue_legacy_successor",
    "propose_legacy_successor",
]
