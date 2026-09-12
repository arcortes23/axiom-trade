"""Durable closed-loop autonomous research processing.

The processor is deliberately boring: queue leases, declarative plans,
deterministic backtests, ordered lifecycle writes, and paper-forward evidence.
Every external boundary is persisted or rejected; no live execution path exists.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
import re
from itertools import product
from statistics import mean
from typing import Any, Callable, Iterable, Mapping, Sequence

from .backtest import CryptoBacktester
from .backtest.prediction import run_prediction_research_mode
from .forward import ForwardTestRegistry, _content_hash
from .director import compact_report, validate_hermes_proposal
from .domain import Fill, MarketType, ResearchQuality, SettlementState, ensure_utc, parse_timestamp, utc_now
from .evaluation import split_dataset
from .paper_engine import build_resolved_bet
from .lifecycle import CandidateLifecycle, CandidateLifecycleManager, CandidateStage, PromotionCriteria
from .metrics import expected_calibration_error
from .mutations import DeterministicMutationEngine, ExperimentBudget
from .research_bus import DurableResearchBus, ResearchBusPermissionError, ResearchQueueItem, ResearchQueueStatus
from .robustness import bootstrap_confidence_interval, minimum_sample_check, neighboring_parameter_stability

from .storage import AxiomStore
from .data_quality import evaluate_prediction_data_quality, persisted_quality_fields
from .strategy import StrategyDefinition, load_strategy
from .strategy.signals import evaluate_model_document_probability
from .experiment_plan import AUTONOMOUS_BUDGET_ID, ExperimentPlan, ExperimentPlanError, MAX_PLAN_VARIANTS


_MAX_QUEUE_RESULT_ITEMS = 64
_MAX_DATASET_ROWS = 100_000
_MAX_FORWARD_ROWS = 100_000
_MAX_LEGACY_RECOVERY_ITEMS = 64
_LEGACY_RECOVERY_STATE_NAME = "autonomous-legacy-recovery"
_MAX_AUTOMATIC_REASSESSMENTS = 3
_MUTABLE_DATASET_VERSION_ALIASES = frozenset({"latest", "current", "default", "unversioned"})
_NEXT_DATASET_SCHEDULER_STATE_NAME = "autonomous-next-dataset-version"
_NEXT_DATASET_JOB_PREFIX = "polymarket-dataset-successor:"
_NEXT_DATASET_JOB_STATUS_WAITING = "WAITING_FOR_NEW_DATASET_VERSION"
_NEXT_DATASET_JOB_STATUS_ENQUEUED = "ENQUEUED"
_NEXT_DATASET_JOB_STATUS_COMPLETED = "COMPLETED"
_NEXT_DATASET_JOB_STATUS_FAILED = "FAILED"
_NEXT_DATASET_JOB_CADENCE = "collection_cycle"
_NEXT_DATASET_JOB_TRIGGER = "AUTOMATIC_RESEARCH_TICK"
_NEXT_DATASET_GENERATED_KIND = "dataset_version_successor"

PAPER_MARKET_AUTHORITY_CAP = 100

PREDECLARED_STRATEGY_STARTING_SET: tuple[Mapping[str, Any], ...] = (
    {
        "template": "momentum",
        "parameters": {"lookback": (1,), "threshold": (0.05,)},
        "metadata": {"research_role": "PRICE_MOMENTUM_ASSESSMENT"},
    },
    {
        "template": "mean_reversion",
        "parameters": {"lookback": (1,), "threshold": (0.05,)},
        "metadata": {"research_role": "PRICE_MEAN_REVERSION_ASSESSMENT"},
    },
    {
        "template": "probability_mispricing",
        "parameters": {"threshold": (0.05,)},
        "model_document": {"field": "yes_mid"},
        "metadata": {
            "research_role": "ZERO_EDGE_CONTROL",
            "selection_excluded": True,
            "proven_zero_edge": True,
        },
    },
)

CAMPAIGN_BUDGET_LIMIT = 24
CAMPAIGN_MAX_FINALISTS = 1
CAMPAIGN_STATUSES = frozenset(
    {
        "PLANNED",
        "RUNNING",
        "WAITING_FOR_DATA",
        "FINAL_ASSESSMENT",
        "COMPLETED_QUALIFIED",
        "CAMPAIGN_EXHAUSTED_NO_QUALIFIED_STRATEGY",
        "SOFTWARE_OR_INPUT_ERROR",
    }
)
CAMPAIGN_TRIAL_TERMINAL = frozenset(
    {
        "ECONOMIC_REJECTION",
        "DATA_INSUFFICIENT",
        "SOFTWARE_OR_INPUT_ERROR",
        "VALIDATION_QUALIFIED",
        "FINAL_ASSESSMENT",
    }
)
POLYMARKET_CAMPAIGN_GRID: tuple[Mapping[str, Any], ...] = tuple(
    {
        "configuration_id": f"{family}:lookback-{lookback}:threshold-{threshold:.2f}",
        "template": family,
        "parameters": {"lookback": lookback, "threshold": threshold},
    }
    for family in ("momentum", "mean_reversion")
    for lookback in (1, 3, 5)
    for threshold in (0.02, 0.05)
)


def _campaign_configuration_key(value: Mapping[str, Any]) -> str:
    template = str(value.get("template", value.get("family", ""))).strip().lower()
    parameters = value.get("parameters")
    if not isinstance(parameters, Mapping):
        parameters = {
            key: value[key]
            for key in ("lookback", "threshold")
            if key in value
        }
    normalized_parameters: dict[str, Any] = {}
    for key, parameter in parameters.items():
        if isinstance(parameter, (list, tuple)) and len(parameter) == 1:
            parameter = parameter[0]
        normalized_parameters[str(key)] = parameter
    return _canonical_binding({"template": template, "parameters": normalized_parameters})
def _campaign_row_identity(row: Mapping[str, Any], index: int) -> str:
    for name in ("row_identity", "source_snapshot_id", "snapshot_id", "observation_id"):
        value = row.get(name)
        if value is not None and str(value).strip():
            return str(value).strip()
    market = str(row.get("market_id", row.get("symbol", ""))).strip()
    timestamp = str(row.get("timestamp", row.get("source_timestamp", ""))).strip()
    if market or timestamp:
        return f"{market}@{timestamp}"
    return f"row:{index}"


def _campaign_row_manifest(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, str]]:
    return [
        {
            "row_identity": _campaign_row_identity(row, index),
            "content_hash": _hash_document(row),
        }
        for index, row in enumerate(rows)
    ]


def _campaign_split_ranges(count: int) -> dict[str, tuple[int, int]]:
    train_end = max(0, int(count * 0.60))
    validation_end = max(train_end, int(count * 0.80))
    if count:
        train_end = min(count, max(1, train_end))
        validation_end = min(count, max(train_end, validation_end))
    return {
        "development": (0, train_end),
        "validation": (train_end, validation_end),
        "final": (validation_end, count),
    }


def _campaign_compact_row_provenance(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Return bounded evidence that authenticates the complete ordered rows.

    The manifest is materialized only while computing its digest.  Persisted
    campaign/proposal payloads carry the digest and split boundary identities,
    never the per-row manifest itself.
    """
    manifest = _campaign_row_manifest(rows)
    timestamps = [
        parse_timestamp(row.get("timestamp", row.get("source_timestamp")))
        for row in rows
    ]
    timestamps = [value for value in timestamps if value is not None]
    split_boundaries: dict[str, dict[str, Any]] = {}
    for name, (start, end) in _campaign_split_ranges(len(rows)).items():
        part = manifest[start:end]
        split: dict[str, Any] = {
            "start_index": start,
            "end_index": end,
            "row_count": len(part),
        }
        if part:
            split.update(
                {
                    "first_row_identity": part[0]["row_identity"],
                    "last_row_identity": part[-1]["row_identity"],
                    "first_content_hash": part[0]["content_hash"],
                    "last_content_hash": part[-1]["content_hash"],
                }
            )
        split_boundaries[name] = split
    digest = _hash_document(manifest)
    return {
        "schema_version": "axiom-compact-row-provenance-v1",
        "row_count": len(rows),
        "exact_cutoff": max(timestamps).isoformat() if timestamps else None,
        "ordered_row_manifest_digest": digest,
        # ``root`` is an explicit alias for consumers that call the ordered
        # manifest commitment a Merkle/root digest; both names are one value.
        "ordered_row_manifest_root": digest,
        "content_hash": digest,
        "split_boundaries": split_boundaries,
    }


def _campaign_rows_for_split(
    rows: Sequence[Mapping[str, Any]],
    descriptor: Any,
) -> list[Mapping[str, Any]]:
    """Resolve a compact split descriptor while checking its row identities."""
    if isinstance(descriptor, (list, tuple)):
        # Read-only compatibility for campaigns persisted before compact
        # provenance. New payloads never take this path.
        identities = {
            str(item.get("row_identity"))
            for item in descriptor
            if isinstance(item, Mapping)
        }
        return [
            row
            for index, row in enumerate(rows)
            if _campaign_row_identity(row, index) in identities
        ]
    if not isinstance(descriptor, Mapping):
        raise AutonomousResearchError(
            "DATASET_PROVENANCE_INVALID",
            "campaign split provenance is missing",
        )
    try:
        start = int(descriptor.get("start_index"))
        end = int(descriptor.get("end_index"))
        expected_count = int(descriptor.get("row_count"))
    except (TypeError, ValueError):
        raise AutonomousResearchError(
            "DATASET_PROVENANCE_INVALID",
            "campaign split provenance indexes are invalid",
        ) from None
    if start < 0 or end < start or end > len(rows):
        raise AutonomousResearchError(
            "DATASET_PROVENANCE_INVALID",
            "campaign split provenance indexes exceed the dataset boundary",
        )
    manifest = _campaign_row_manifest(rows)
    part = list(rows[start:end])
    part_manifest = manifest[start:end]
    if expected_count != len(part):
        raise AutonomousResearchError(
            "DATASET_PROVENANCE_INVALID",
            "campaign split row count changed after enqueue",
        )
    if part_manifest:
        if (
            str(descriptor.get("first_row_identity", "")).strip()
            != part_manifest[0]["row_identity"]
            or str(descriptor.get("last_row_identity", "")).strip()
            != part_manifest[-1]["row_identity"]
            or str(descriptor.get("first_content_hash", "")).strip()
            != part_manifest[0]["content_hash"]
            or str(descriptor.get("last_content_hash", "")).strip()
            != part_manifest[-1]["content_hash"]
        ):
            raise AutonomousResearchError(
                "DATASET_PROVENANCE_INVALID",
                "campaign split boundary identity changed after enqueue",
            )
    return part


def _variant_count(plan: ExperimentPlan) -> int:
    count = 1
    for values in plan.parameters.values():
        count *= len(values)
    return count






def _binding_value(value: Any) -> str | None:
    """Normalize a persisted binding value without turning ``None`` into text."""
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _scope_binding(plan: ExperimentPlan) -> dict[str, Any]:
    """Return the canonical, recomputed authority carried by every worker artifact."""
    return {
        "market_scope": plan.market_scope.as_dict(),
        "market_scope_hash": plan.market_scope_hash,
        "market_scope_version": plan.market_scope_version,
        "scope_hash": plan.market_scope_hash,
        "scope_version": plan.market_scope_version,
        "dataset_selector": dict(plan.as_dict()["dataset_selector"]),
    }
def _strategy_metadata(strategy: StrategyDefinition) -> dict[str, Any]:
    metadata = strategy.metadata if isinstance(strategy.metadata, Mapping) else {}
    role = str(metadata.get("research_role", "")).strip()
    return {
        "research_role": role or None,
        "selection_excluded": bool(metadata.get("selection_excluded", False)),
        "proven_zero_edge": bool(metadata.get("proven_zero_edge", False)),
    }




def _canonical_binding(value: Any) -> str:
    def plain(item: Any) -> Any:
        if isinstance(item, datetime):
            # Persisted JSON bindings carry ISO strings while store readers
            # may return datetime objects.  Canonicalize both forms without
            # weakening any value comparison.
            return ensure_utc(item).isoformat()
        if isinstance(item, Mapping):
            return {str(key): plain(child) for key, child in item.items()}
        if isinstance(item, (list, tuple)):
            return [plain(child) for child in item]
        if isinstance(item, (set, frozenset)):
            return [plain(child) for child in sorted(item, key=str)]
        return item
    return json.dumps(plain(value), sort_keys=True, separators=(",", ":"), allow_nan=False, default=str)
_GENERATED_QUEUE_PROVENANCE_SCHEMA = "axiom-generated-queue-v1"
_GENERATED_QUEUE_KINDS = frozenset({
    "predeclared_starting_set",
    "legacy_scope_successor",
    "mutation_child",
    _NEXT_DATASET_GENERATED_KIND,
})


def _payload_without_generated_provenance(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Return the proposal identity material without its internal marker."""
    result = dict(payload)
    provenance = result.get("provenance")
    if isinstance(provenance, Mapping) and isinstance(provenance.get("internal"), Mapping):
        clean_provenance = dict(provenance)
        clean_provenance.pop("internal", None)
        if clean_provenance:
            result["provenance"] = clean_provenance
        else:
            # A marker is added to otherwise unprovenanced proposals.  Do not
            # leave behind the empty container created solely by removing it;
            # the pre-marker and post-marker identities must be identical.
            result.pop("provenance", None)
    return result


def _proposal_identity(payload: Mapping[str, Any]) -> str:
    return "sha256:" + hashlib.sha256(
        _canonical_binding(_payload_without_generated_provenance(payload)).encode("utf-8")
    ).hexdigest()


def _mark_generated_queue_payload(
    payload: Mapping[str, Any],
    *,
    kind: str,
    dataset_id: str | None,
    dataset_version: str | None,
    attestation_hash: str | None,
) -> dict[str, Any]:
    """Attach a self-authenticating marker to worker-generated proposals."""
    clean = dict(payload)
    provenance = dict(clean.get("provenance")) if isinstance(clean.get("provenance"), Mapping) else {}
    provenance["internal"] = {
        "schema": _GENERATED_QUEUE_PROVENANCE_SCHEMA,
        "generated": True,
        "kind": str(kind).strip(),
        "proposal_identity": _proposal_identity(clean),
        "dataset_id": str(dataset_id or "").strip() or None,
        "dataset_version": str(dataset_version or "").strip() or None,
        "attestation_hash": str(attestation_hash or "").strip() or None,
    }
    clean["provenance"] = provenance
    return clean


def _generated_queue_provenance(payload: Mapping[str, Any]) -> Mapping[str, Any] | None:
    provenance = payload.get("provenance")
    if not isinstance(provenance, Mapping):
        return None
    internal = provenance.get("internal")
    if not isinstance(internal, Mapping):
        return None
    if not (
        internal.get("schema") == _GENERATED_QUEUE_PROVENANCE_SCHEMA
        or internal.get("generated") is True
        or str(internal.get("kind", "")).strip() in _GENERATED_QUEUE_KINDS
    ):
        return None
    return internal


def _legacy_dataset_selector_conflict(document: Mapping[str, Any]) -> str | None:
    """Reject contradictory outer/nested dataset selectors before handoff."""
    nested = document.get("experiment_plan")
    nested = nested if isinstance(nested, Mapping) else None
    contexts = (("outer", document), ("nested", nested)) if nested is not None else (("outer", document),)
    values: dict[str, list[tuple[str, str]]] = {"dataset_id": [], "dataset_version": []}
    for label, source in contexts:
        if not isinstance(source, Mapping):
            continue
        selector = source.get("dataset_selector")
        selector = selector if isinstance(selector, Mapping) else {}
        for name, aliases in (
            ("dataset_id", ("dataset_id",)),
            ("dataset_version", ("dataset_version", "version")),
        ):
            for alias in aliases:
                value = selector.get(alias)
                if value is not None and str(value).strip():
                    values[name].append((f"{label}.dataset_selector.{alias}", str(value).strip()))
                value = source.get(alias)
                if value is not None and str(value).strip():
                    values[name].append((f"{label}.{alias}", str(value).strip()))
    for name, entries in values.items():
        distinct = {value for _, value in entries}
        if len(distinct) > 1:
            return "CONFLICTING_DATASET_SELECTOR"
        if name == "dataset_version" and any(
            value.casefold() in _MUTABLE_DATASET_VERSION_ALIASES for value in distinct
        ):
            return "DATASET_VERSION_ALIAS"
    return None
class AutonomousResearchError(ValueError):
    """A deterministic, auditable queue rejection or unsupported operation."""

    def __init__(self, reason: str, detail: str) -> None:
        self.reason = str(reason).strip().upper() or "AUTONOMOUS_RESEARCH_ERROR"
        self.code = self.reason
        self.detail = str(detail).strip() or self.reason
        super().__init__(f"{self.reason}: {self.detail}")


@dataclass(frozen=True, slots=True)
class AutonomousResearchConfig:
    """Boundaries applied to every autonomous queue cycle and mutation."""

    max_items_per_cycle: int = 1
    lease_seconds: float = 300.0
    total_limit: int = 1000
    family_limit: int = 250
    max_plan_variants: int = 8
    max_children_per_parent: int = 2
    max_generation_depth: int = 2
    max_experiments_per_day: int = 250
    mutation_enabled: bool = True
    promotion_criteria: PromotionCriteria = field(default_factory=PromotionCriteria)

    def __post_init__(self) -> None:
        for name in (
            "max_items_per_cycle",
            "total_limit",
            "family_limit",
            "max_plan_variants",
            "max_children_per_parent",
            "max_generation_depth",
            "max_experiments_per_day",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")
        if self.max_items_per_cycle < 1:
            raise ValueError("max_items_per_cycle must be positive")
        if self.max_plan_variants < 1 or self.max_plan_variants > MAX_PLAN_VARIANTS:
            raise ValueError(f"max_plan_variants must be between one and {MAX_PLAN_VARIANTS}")
        lease = float(self.lease_seconds)
        if not math.isfinite(lease) or lease <= 0:
            raise ValueError("lease_seconds must be finite and positive")
        object.__setattr__(self, "lease_seconds", lease)
        if not isinstance(self.mutation_enabled, bool):
            raise ValueError("mutation_enabled must be boolean")
        if not isinstance(self.promotion_criteria, PromotionCriteria):
            raise ValueError("promotion_criteria must be PromotionCriteria")


@dataclass(frozen=True, slots=True)
class AutonomousQueueCycle:
    released: int
    claimed: int
    completed: int
    rejected: int
    failed: int
    results: tuple[Mapping[str, Any], ...] = ()
    legacy_recovery: tuple[Mapping[str, Any], ...] = ()

    def as_record(self) -> dict[str, Any]:
        return {
            "released": self.released,
            "claimed": self.claimed,
            "completed": self.completed,
            "rejected": self.rejected,
            "failed": self.failed,
            "results": [dict(item) for item in self.results],
            "legacy_recovery": [dict(item) for item in self.legacy_recovery],
            "paper_only": True,
        }


class AutonomousResearchProcessor:
    """Claim and execute bounded paper research with a locked holdout.

    Ordinary autonomous trials receive only train and validation partitions.
    The holdout remains locked until an explicitly authorized post-selection
    assessment; it is never used to select variants, mutate strategies, or
    qualify a canary.
    """

    @staticmethod
    def predeclared_starting_set() -> tuple[Mapping[str, Any], ...]:
        """Return the small deterministic strategy set owned by Axiom."""
        return tuple(
            {
                "template": str(item["template"]),
                "parameters": {
                    str(name): tuple(values)
                    for name, values in dict(item["parameters"]).items()
                },
                **(
                    {"model_document": dict(item["model_document"])}
                    if isinstance(item.get("model_document"), Mapping)
                    else {}
                ),
                **(
                    {"metadata": dict(item["metadata"])}
                    if isinstance(item.get("metadata"), Mapping)
                    else {}
                ),
            }
            for item in PREDECLARED_STRATEGY_STARTING_SET
        )

    def enqueue_predeclared_starting_set(
        self,
        proposal: Mapping[str, Any],
        *,
        strategies: Sequence[Mapping[str, Any]] | None = None,
        priority: int = 0,
        available_at: datetime | None = None,
    ) -> tuple[ResearchQueueItem, ...]:
        """Durably enqueue Axiom's deterministic starter trials.

        The caller provides research data selectors and assumptions, not
        candidate/proposal ids.  Each family gets a deterministic identity and
        bounded one-variant plan; queue dedupe makes retries idempotent.
        """
        if not isinstance(proposal, Mapping):
            raise TypeError("proposal must be a mapping")
        selected = tuple(strategies) if strategies is not None else self.predeclared_starting_set()
        if not selected or len(selected) > self.config.max_plan_variants:
            raise AutonomousResearchError(
                "EXPERIMENT_BUDGET_EXCEEDED",
                "predeclared strategy set is empty or exceeds the node bound",
            )
        base = dict(proposal)
        base.pop("proposal_id", None)
        base.pop("hypothesis_id", None)
        items: list[ResearchQueueItem] = []
        for index, strategy in enumerate(selected):
            if not isinstance(strategy, Mapping):
                raise AutonomousResearchError("UNSUPPORTED_STRATEGY_FAMILY", "predeclared strategy must be a mapping")
            template = str(strategy.get("template", strategy.get("family", ""))).strip().lower()
            parameters = strategy.get("parameters", {})
            if not template or not isinstance(parameters, Mapping):
                raise AutonomousResearchError("UNSUPPORTED_STRATEGY_FAMILY", "predeclared strategy requires template and parameters")
            raw_metadata = strategy.get("metadata")
            metadata = dict(raw_metadata) if isinstance(raw_metadata, Mapping) else {}
            role = str(strategy.get("research_role", metadata.get("research_role", ""))).strip()
            if role:
                metadata["research_role"] = role
            if "selection_excluded" in strategy:
                metadata["selection_excluded"] = bool(strategy["selection_excluded"])
            if "proven_zero_edge" in strategy:
                metadata["proven_zero_edge"] = bool(strategy["proven_zero_edge"])
            plan = base.get("experiment_plan")
            plan_document = dict(plan) if isinstance(plan, Mapping) else {}
            plan_document.update(
                {
                    "template": template,
                    "parameters": dict(parameters),
                    "max_variants": 1,
                    "paper_only": True,
                }
            )
            model_document = strategy.get("model_document")
            if isinstance(model_document, Mapping):
                plan_document["model_document"] = dict(model_document)
            strategy_document = strategy.get("strategy_document")
            if metadata:
                strategy_document = dict(strategy_document) if isinstance(strategy_document, Mapping) else {
                    "version": 1,
                    "market_type": MarketType.PREDICTION.value,
                    "family": template,
                    "probability_model": "plan-model-probability",
                    "resolution_aware": True,
                    "resolution_inputs": ["expiry", "settlement"],
                }
                strategy_document["metadata"] = metadata
                plan_document["strategy_document"] = strategy_document
            material = {
                "schema": "axiom-predeclared-starting-set-v1",
                "base": base,
                "template": template,
                "parameters": dict(parameters),
                "model_document": dict(model_document) if isinstance(model_document, Mapping) else None,
                "metadata": metadata,
                "index": index,
            }
            proposal_id = "predeclared-" + hashlib.sha256(
                _canonical_binding(material).encode("utf-8")
            ).hexdigest()[:24]
            item_payload = {
                **base,
                "proposal_id": proposal_id,
                "experiment_plan": plan_document,
                "paper_only": True,
                "predeclared_starting_set": True,
            }
            # Keep the attestation binding sourced from the plan before
            # compatibility defaults are added to the stored payload.
            selector = item_payload.get("dataset_selector")
            selector = selector if isinstance(selector, Mapping) else plan_document.get("dataset_selector")
            selector = selector if isinstance(selector, Mapping) else plan_document
            dataset_id = str(
                selector.get("dataset_id") or item_payload.get("dataset_id") or plan_document.get("dataset_id") or ""
            ).strip() or None
            dataset_version = str(
                selector.get("dataset_version")
                or selector.get("version")
                or item_payload.get("dataset_version")
                or plan_document.get("dataset_version")
                or ""
            ).strip() or None
            attestation_hash = None
            attestation_loader = getattr(self.store, "load_dataset_integrity_attestation", None)
            if callable(attestation_loader) and dataset_id and dataset_version:
                try:
                    attestation = attestation_loader(dataset_id, dataset_version)
                except Exception:
                    attestation = None
                if isinstance(attestation, Mapping):
                    attestation_hash = str(attestation.get("attestation_hash", "")).strip() or None
            # Processing normalizes worker payloads before checking the
            # self-authenticating marker. Store that same canonical shape so a
            # sparse predeclared proposal does not reject itself at claim time.
            item_payload = _normalize_hypothesis_payload(
                item_payload,
                source_fallback=str(
                    getattr(self.bus, "author", "")
                    or getattr(self.bus, "source", "")
                    or "hermes"
                ).strip()
                or "hermes",
            )
            item_payload = _mark_generated_queue_payload(
                item_payload,
                kind="predeclared_starting_set",
                dataset_id=dataset_id,
                dataset_version=dataset_version,
                attestation_hash=attestation_hash,
            )
            item = self.bus.submit_hypothesis(
                item_payload,
                dedupe_key=f"predeclared:{proposal_id}:{attestation_hash or 'unattested'}",
                priority=priority,
                available_at=available_at,
            )
            items.append(item)
        return tuple(items)

    submit_predeclared_starting_set = enqueue_predeclared_starting_set

    def _persist_predeclared_seed_evidence(
        self,
        evidence: Mapping[str, Any],
        *,
        dataset_id: str,
        dataset_version: str | None,
    ) -> bool:
        """Persist one blocked seed decision as immutable research evidence.

        A dataset binding can produce different blocked decisions as its
        persisted provenance changes.  The binding is the experiment
        namespace, while the canonical evidence digest identifies one
        append-only decision attempt within that namespace.  This keeps
        retries of an identical observation idempotent without allowing a
        later blocker to collide with, or overwrite, its predecessor.
        """
        saver = getattr(self.store, "save_report_if_absent", None)
        if not callable(saver):
            return False
        version = str(dataset_version or "").strip()
        binding_id = "autonomous-predeclared-seed:" + str(dataset_id).strip()
        if version:
            binding_id += ":" + version
        evidence_id = hashlib.sha256(
            _canonical_binding(dict(evidence)).encode("utf-8")
        ).hexdigest()[:24]
        report_id = f"{binding_id}:{evidence_id}"
        try:
            return bool(
                saver(
                    report_id,
                    dict(evidence),
                    experiment_id=binding_id,
                )
            )
        except Exception:
            # A failed evidence write must never turn a fail-closed seed gate
            # into an untrusted queue submission.
            return False

    def _predeclared_seed_blocker(
        self,
        *,
        blocker: str,
        detail: str,
        dataset_id: str,
        dataset_version: str | None,
        catalog: Mapping[str, Any] | None = None,
        attestation: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Build the exact prerequisite projection for a blocked seed."""
        normalized_blocker = str(blocker).strip().upper() or "DATASET_PROVENANCE_INVALID"
        normalized_detail = str(detail).strip() or normalized_blocker
        if normalized_blocker == "DATASET_ATTESTATION_MISSING":
            next_action = "PERSIST_CURRENT_HISTORICAL_DATASET_ATTESTATION"
        elif normalized_blocker == "DATASET_ATTESTATION_STALE":
            next_action = "REFRESH_CURRENT_HISTORICAL_DATASET_ATTESTATION"
        elif "CONTAMIN" in normalized_detail.upper():
            next_action = "REMOVE_FORWARD_CONTAMINATION_AND_REATTEST_HISTORICAL_DATASET"
        else:
            next_action = "REPAIR_HISTORICAL_DATASET_PROVENANCE_AND_REATTEST"
        catalog_map = catalog if isinstance(catalog, Mapping) else {}
        attestation_map = attestation if isinstance(attestation, Mapping) else {}
        return {
            "report_type": "autonomous_predeclared_seed_blocked",
            "progress": "BLOCKED",
            "blocker": normalized_blocker,
            "reason": normalized_detail,
            "next_action": next_action,
            "required_dataset": {
                "dataset_id": str(dataset_id).strip() or None,
                "dataset_version": str(dataset_version or "").strip() or None,
                "source_type": "HISTORICAL",
                "market_type": MarketType.PREDICTION.value,
                "attestation_status": "CURRENT",
                "contamination_result": "PASS",
            },
            "catalog_identity": {
                "dataset_id": str(catalog_map.get("dataset_id", "")).strip() or None,
                "dataset_version": str(
                    catalog_map.get("dataset_version", catalog_map.get("version", ""))
                ).strip()
                or None,
                "instrument": str(catalog_map.get("instrument", "")).strip() or None,
                "row_count": catalog_map.get("row_count"),
                "completeness": catalog_map.get("completeness"),
            },
            "attestation": {
                "status": str(attestation_map.get("status", "")).strip().upper() or None,
                "contamination_result": attestation_map.get("contamination_result"),
                "attestation_hash": attestation_map.get("attestation_hash"),
                "reason": attestation_map.get("reason"),
            },
            "queue_items_enqueued": 0,
            "variants_tested": 0,
            "paper_only": True,
            "research_only": True,
        }

    def _enqueue_predeclared_from_persisted_scope(self, now: datetime) -> tuple[ResearchQueueItem, ...]:
        """Seed bounded prediction research from the persisted historical catalog.

        The seed is deterministic and policy-bound: the aggregate historical
        dataset and canonical POLYMARKET forward rules are selected before any
        candidate performance is inspected.  The exact dataset catalog and
        current integrity attestation are validated through the same
        provenance contract used by ordinary hypothesis processing before a
        queue row can be created. Queue deduplication makes this safe to invoke
        on every normal worker tick.
        """
        expected_dataset_id = "Polymarket-historical"
        list_catalog = getattr(self.store, "list_dataset_catalog", None)
        if not callable(list_catalog):
            evidence = self._predeclared_seed_blocker(
                blocker="DATASET_CATALOG_MISSING",
                detail="store has no persisted historical dataset catalog",
                dataset_id=expected_dataset_id,
                dataset_version=None,
            )
            self._persist_predeclared_seed_evidence(
                evidence,
                dataset_id=expected_dataset_id,
                dataset_version=None,
            )
            return ()
        try:
            catalogs = list_catalog(market_type="prediction", limit=128)
        except Exception as exc:
            evidence = self._predeclared_seed_blocker(
                blocker="DATASET_CATALOG_UNAVAILABLE",
                detail=f"historical dataset catalog could not be loaded: {exc}",
                dataset_id=expected_dataset_id,
                dataset_version=None,
            )
            self._persist_predeclared_seed_evidence(
                evidence,
                dataset_id=expected_dataset_id,
                dataset_version=None,
            )
            return ()
        if not isinstance(catalogs, Sequence):
            evidence = self._predeclared_seed_blocker(
                blocker="DATASET_CATALOG_INVALID",
                detail="historical dataset catalog result is not a sequence",
                dataset_id=expected_dataset_id,
                dataset_version=None,
            )
            self._persist_predeclared_seed_evidence(
                evidence,
                dataset_id=expected_dataset_id,
                dataset_version=None,
            )
            return ()
        def catalog_instrument(item: Mapping[str, Any]) -> str:
            return str(item.get("instrument", "")).strip().upper()

        def catalog_version(item: Mapping[str, Any]) -> str:
            return str(item.get("dataset_version") or item.get("version") or "").strip()

        def complete(item: Any) -> bool:
            if not isinstance(item, Mapping):
                return False
            try:
                metadata = item.get("metadata")
                metadata = metadata if isinstance(metadata, Mapping) else {}
                metadata_instrument = str(metadata.get("instrument", "")).strip().upper()
                return (
                    bool(str(item.get("dataset_id", "")).strip())
                    and bool(catalog_version(item))
                    and catalog_version(item).casefold() not in _MUTABLE_DATASET_VERSION_ALIASES
                    and str(item.get("source_type", "")).strip().upper() == "HISTORICAL"
                    and str(item.get("market_type", "")).strip().lower() == MarketType.PREDICTION.value
                    and catalog_instrument(item) == "POLYMARKET"
                    and (not metadata_instrument or metadata_instrument == catalog_instrument(item))
                    and _finite(item.get("completeness"), 0.0) >= 1.0
                    and int(item.get("row_count", 0) or 0) > 0
                    and not item.get("missing_ranges")
                )
            except (TypeError, ValueError, OverflowError):
                return False
        catalog = next(
            (
                item
                for item in catalogs
                if isinstance(item, Mapping)
                and str(item.get("dataset_id", "")).strip() == expected_dataset_id
            ),
            None,
        )
        if catalog is None:
            evidence = self._predeclared_seed_blocker(
                blocker="DATASET_CATALOG_MISSING",
                detail=f"no historical Polymarket catalog for {expected_dataset_id}",
                dataset_id=expected_dataset_id,
                dataset_version=None,
            )
            self._persist_predeclared_seed_evidence(
                evidence,
                dataset_id=expected_dataset_id,
                dataset_version=None,
            )
            return ()
        dataset_version = catalog_version(catalog)
        instrument = catalog_instrument(catalog)
        if not complete(catalog):
            source_type = str(catalog.get("source_type", "")).strip().upper()
            market_type = str(catalog.get("market_type", "")).strip().lower()
            if instrument != "POLYMARKET":
                detail = (
                    f"historical dataset catalog instrument {instrument or '<missing>'} "
                    f"does not normalize to POLYMARKET for {expected_dataset_id}"
                )
            elif dataset_version.casefold() in _MUTABLE_DATASET_VERSION_ALIASES:
                detail = (
                    f"historical dataset catalog version {dataset_version!r} is a mutable alias; "
                    "an immutable dataset version is required"
                )
            elif source_type != "HISTORICAL":
                detail = (
                    f"dataset catalog source_type {source_type or '<missing>'} "
                    f"is not HISTORICAL for {expected_dataset_id}"
                )
            elif market_type != MarketType.PREDICTION.value:
                detail = (
                    f"dataset catalog market_type {market_type or '<missing>'} "
                    f"is not prediction for {expected_dataset_id}"
                )
            else:
                detail = f"historical dataset catalog is incomplete for {expected_dataset_id}"
            evidence = self._predeclared_seed_blocker(
                blocker="DATASET_CATALOG_INVALID",
                detail=detail,
                dataset_id=expected_dataset_id,
                dataset_version=dataset_version or None,
                catalog=catalog,
            )
            self._persist_predeclared_seed_evidence(
                evidence,
                dataset_id=expected_dataset_id,
                dataset_version=dataset_version or None,
            )
            return ()
        dataset_id = str(catalog.get("dataset_id", "")).strip()
        instrument = catalog_instrument(catalog)
        market_scope = {
            "schema_version": "1",
            "mode": "RULE_BASED_MARKETS",
            # PRICE_PROXY rows are intentionally compact.  The bounded scope
            # uses the catalog's attested instrument identity; liquidity and
            # spread are current-book evidence, not historical price-path
            # inputs.
            "instrument": instrument,
            "filters": {},
            "regime_restrictions": {},
            "provenance": "canonical",
        }
        methodology = {
            "research_mode": "PRICE_PROXY_RESEARCH",
            "initial_cash": 10_000.0,
            "allocation": 0.25,
            "predeclared_starting_set": True,
        }
        assumptions = {
            "version": "price-proxy-v1",
            "fee_bps": 10.0,
            "slippage_bps": 5.0,
            "roundtrip_fee_bps": 20.0,
            "roundtrip_slippage_bps": 10.0,
            "cost_sensitivity": {
                "fee_bps": [10.0, 20.0],
                "slippage_bps": [5.0, 10.0],
            },
        }
        exit_policy = {"type": "fixed_holding_period", "holding_period": 1}
        allowed_features = ["timestamp", "market_id", "yes_mid"]
        proposal = {
            "proposal_id": f"predeclared-seed:{dataset_id}:{dataset_version}",
            "statement": "Evaluate bounded Polymarket price-path momentum and mean reversion.",
            "dataset_selector": {
                "dataset_id": dataset_id,
                "dataset_version": dataset_version,
                "source_type": "HISTORICAL",
            },
            "source": "axiom-autonomous-predeclared",
            "tests": ["bounded chronological price-proxy backtest and validation"],
            "market_type": MarketType.PREDICTION.value,
            "dataset_id": dataset_id,
            "dataset_version": dataset_version,
            "market_scope": market_scope,
            "allowed_features": allowed_features,
            "time_split": "train-validation-holdout",
            "min_samples": 30,
            "min_trades": 20,
            "max_variants": 1,
            "paper_only": True,
            "assumptions": assumptions,
            "exit_policy": exit_policy,
            "methodology": methodology,
            "experiment_plan": {
                "market_type": MarketType.PREDICTION.value,
                "market_scope": market_scope,
                "dataset_id": dataset_id,
                "dataset_version": dataset_version,
                "allowed_features": allowed_features,
                "time_split": "train-validation-holdout",
                "min_samples": 30,
                "min_trades": 20,
                "assumptions": assumptions,
                "exit_policy": exit_policy,
                "methodology": methodology,
            },
        }
        try:
            plan = ExperimentPlan.from_proposal(proposal)
            self._validate_persisted_dataset_provenance(plan)
        except (AutonomousResearchError, ExperimentPlanError, TypeError, ValueError, RuntimeError) as exc:
            attestation_loader = getattr(self.store, "load_dataset_integrity_attestation", None)
            try:
                attestation = (
                    attestation_loader(dataset_id, dataset_version)
                    if callable(attestation_loader)
                    else None
                )
            except Exception:
                attestation = None
            blocker = str(getattr(exc, "reason", "")).strip().upper() or "DATASET_PROVENANCE_INVALID"
            detail = str(getattr(exc, "detail", "")).strip() or str(exc)
            evidence = self._predeclared_seed_blocker(
                blocker=blocker,
                detail=detail,
                dataset_id=dataset_id,
                dataset_version=dataset_version,
                catalog=catalog,
                attestation=attestation if isinstance(attestation, Mapping) else None,
            )
            self._persist_predeclared_seed_evidence(
                evidence,
                dataset_id=dataset_id,
                dataset_version=dataset_version,
            )
            return ()
        try:
            return self.enqueue_predeclared_starting_set(proposal, priority=0, available_at=now)
        except (AutonomousResearchError, ResearchBusPermissionError, TypeError, ValueError, RuntimeError) as exc:
            blocker = str(getattr(exc, "reason", "")).strip().upper() or "PREDECLARED_SEED_ENQUEUE_FAILED"
            evidence = self._predeclared_seed_blocker(
                blocker=blocker,
                detail=str(getattr(exc, "detail", "")).strip() or str(exc),
                dataset_id=dataset_id,
                dataset_version=dataset_version,
                catalog=catalog,
            )
            self._persist_predeclared_seed_evidence(
                evidence,
                dataset_id=dataset_id,
                dataset_version=dataset_version,
            )
            return ()

    def __init__(
        self,
        store: AxiomStore,
        *,
        bus: DurableResearchBus | None = None,
        config: AutonomousResearchConfig | None = None,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self.store = store
        self.bus = bus or DurableResearchBus(store)
        self.config = config or AutonomousResearchConfig()
        self.clock = clock
        self.lifecycle = CandidateLifecycleManager(store, criteria=self.config.promotion_criteria)
        self._campaign_active_state: dict[str, Any] = {}
    @staticmethod
    def campaign_job_name(campaign_id: str) -> str:
        value = str(campaign_id).strip()
        if not value or len(value) > 128:
            raise ValueError("campaign_id must be bounded non-empty text")
        return f"polymarket-research-campaign:{value}"

    @staticmethod
    def campaign_configurations() -> tuple[Mapping[str, Any], ...]:
        """Return the fixed, one-variant Polymarket generator grid."""
        return tuple(dict(item) for item in POLYMARKET_CAMPAIGN_GRID)

    @staticmethod
    def _campaign_dataset_rows(store: AxiomStore, dataset_id: str, dataset_version: str) -> list[Mapping[str, Any]]:
        loaded = store.load_dataset(dataset_id, dataset_version)
        if isinstance(loaded, Mapping) and isinstance(loaded.get("records"), Sequence):
            loaded = loaded["records"]
        if not isinstance(loaded, Sequence) or isinstance(loaded, (str, bytes)):
            return []
        rows = [_normalize_row(row) for row in loaded if isinstance(row, Mapping)]
        rows = [row for row in rows if row is not None]
        rows.sort(
            key=lambda row: (
                parse_timestamp(row.get("timestamp", row.get("source_timestamp"))) or datetime.min.replace(tzinfo=timezone.utc),
                _campaign_row_identity(row, 0),
                _hash_document(row),
            )
        )
        return rows[:_MAX_DATASET_ROWS]

    def _campaign_prior_configuration_keys(self) -> set[str]:
        keys: set[str] = set()
        lister = getattr(self.store, "list_experiment_plans", None)
        if callable(lister):
            try:
                records = lister(limit=10_000, newest_first=False)
            except TypeError:
                records = lister(limit=10_000)
            except Exception:
                records = ()
            for record in records or ():
                if not isinstance(record, Mapping):
                    continue
                plan = record.get("plan")
                if not isinstance(plan, Mapping):
                    continue
                template = str(plan.get("template", plan.get("experiment_family", ""))).strip().lower()
                parameters = plan.get("parameters")
                if template in {"momentum", "mean_reversion"} and isinstance(parameters, Mapping):
                    names = tuple(sorted(parameters))
                    value_sets = tuple(
                        tuple(value) if isinstance(value, (list, tuple)) else (value,)
                        for value in (parameters[name] for name in names)
                    )
                    for values in product(*value_sets):
                        keys.add(
                            _campaign_configuration_key(
                                {
                                    "template": template,
                                    "parameters": dict(zip(names, values)),
                                }
                            )
                        )
        # Queue rows and immutable reports are evidence too.  A rejected
        # candidate is never reopened merely because its queue row is still
        # visible.
        try:
            queued = self.store.list_research_items(limit=10_000)
        except Exception:
            queued = ()
        for record in queued or ():
            payload = record.get("payload") if isinstance(record, Mapping) else None
            plan = payload.get("experiment_plan") if isinstance(payload, Mapping) else None
            if isinstance(plan, Mapping):
                template = str(plan.get("template", "")).strip().lower()
                parameters = plan.get("parameters")
                if template in {"momentum", "mean_reversion"} and isinstance(parameters, Mapping):
                    names = tuple(sorted(parameters))
                    value_sets = tuple(
                        tuple(value) if isinstance(value, (list, tuple)) else (value,)
                        for value in (parameters[name] for name in names)
                    )
                    for values in product(*value_sets):
                        keys.add(
                            _campaign_configuration_key(
                                {
                                    "template": template,
                                    "parameters": dict(zip(names, values)),
                                }
                            )
                        )
        return keys
    @staticmethod
    def _campaign_split_manifests(rows: Sequence[Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
        """Return compact split commitments, not the historical row lists."""
        return dict(_campaign_compact_row_provenance(rows)["split_boundaries"])

    @staticmethod
    def _campaign_default_protocol(
        *,
        campaign_id: str,
        dataset_id: str,
        dataset_version: str,
        rows: Sequence[Mapping[str, Any]],
        configurations: Sequence[Mapping[str, Any]],
        qualification_gates: Mapping[str, Any] | None,
        finalist_count: int,
        observation_horizon: int,
    ) -> dict[str, Any]:
        provenance = _campaign_compact_row_provenance(rows)
        manifests = dict(provenance["split_boundaries"])
        boundary = {
            "schema_version": provenance["schema_version"],
            "dataset_id": dataset_id,
            "dataset_version": dataset_version,
            "source_type": "HISTORICAL",
            "exact_cutoff": provenance["exact_cutoff"],
            "row_count": provenance["row_count"],
            "ordered_row_manifest_digest": provenance["ordered_row_manifest_digest"],
            "ordered_row_manifest_root": provenance["ordered_row_manifest_root"],
            "content_hash": provenance["content_hash"],
            "split_boundaries": dict(manifests),
        }
        return {
            "schema_version": "polymarket-finite-campaign-v1",
            "campaign_id": campaign_id,
            "scientific_rationale": (
                "Test whether short price-path momentum or mean-reversion "
                "signals survive a fixed chronological paper evaluation after costs."
            ),
            "dataset_boundary": boundary,
            "configuration_manifest": [dict(item) for item in configurations],
            "fixed_generator": {
                "families": ["momentum", "mean_reversion"],
                "lookbacks": [1, 3, 5],
                "thresholds": [0.02, 0.05],
                "automatic_mutations": False,
            },
            "entry_rules": {
                "signal": "strategy score exceeds absolute threshold",
                "price_path": "same-market observed quote only",
                "selection_partition": "validation",
            },
            "exit_rules": {
                "type": "fixed_holding_period",
                "unit": "observations",
                "count": int(observation_horizon),
            },
            "required_features": [
                "timestamp",
                "market_id",
                "yes_mid",
                "yes_bid",
                "yes_ask",
                "settlement",
            ],
            "scope": {
                "market_type": "prediction",
                "instrument": "POLYMARKET",
                "paper_only": True,
            },
            "observation_horizon": {
                "unit": "observations",
                "count": int(observation_horizon),
                "semantics": "per_market_observation_count",
            },
            "costs": {"fee_bps": 10.0, "slippage_bps": 5.0},
            "qualification_gates": dict(
                qualification_gates
                or {"min_expectancy": 0.0, "min_samples": 3, "min_trades": 0}
            ),
            "budget_limit": CAMPAIGN_BUDGET_LIMIT,
            "budget_version": "v1",
            "validation_policy": {
                "chronological": True,
                "selection_metric": "validation_expectancy",
                "automatic_mutations": False,
                "one_variant_per_trial": True,
            },
            "final_assessment_policy": {
                "evaluate_once": True,
                "max_finalists": int(finalist_count),
                "select_from": "validation_only",
                "untouched_until_validation_complete": True,
            },
            "protected_row_identity_manifests": manifests,
            "reassessment_limit": 1,
            "reassessment_version": "v1",
        }

    @staticmethod
    def _campaign_protocol_state(payload: Mapping[str, Any]) -> tuple[Mapping[str, Any], str]:
        protocol = payload.get("protocol")
        if not isinstance(protocol, Mapping):
            raise AutonomousResearchError(
                "CAMPAIGN_PROTOCOL_INVALID",
                "campaign operator job has no durable protocol",
            )
        protocol_hash = _hash_document(protocol)
        declared_hash = str(payload.get("protocol_hash", "")).strip()
        if not declared_hash or declared_hash != protocol_hash:
            raise AutonomousResearchError(
                "CAMPAIGN_PROTOCOL_INVALID",
                "campaign operator job protocol identity does not match its canonical protocol",
            )
        return protocol, protocol_hash

    @staticmethod
    def _campaign_trial_boundary(
        payload: Mapping[str, Any],
        protocol: Mapping[str, Any],
        trial: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        trial_id = str(trial.get("trial_id", "")).strip()
        reassessment_of = str(trial.get("reassessment_of", "")).strip()
        if reassessment_of:
            boundaries = payload.get("reassessment_boundaries")
            boundary = boundaries.get(trial_id) if isinstance(boundaries, Mapping) else None
        else:
            boundary = protocol.get("dataset_boundary")
        if not isinstance(boundary, Mapping):
            raise AutonomousResearchError(
                "CAMPAIGN_PROTOCOL_INVALID",
                f"durable campaign boundary is missing for trial {trial_id}",
            )
        return boundary

    def _validate_campaign_plan_binding(
        self,
        payload: Mapping[str, Any],
        trial: Mapping[str, Any],
        plan: ExperimentPlan,
        *,
        queue_payload: Mapping[str, Any] | None = None,
    ) -> tuple[Mapping[str, Any], Mapping[str, Any]]:
        protocol, protocol_hash = self._campaign_protocol_state(payload)
        campaign_id = str(payload.get("campaign_id", "")).strip()
        reference = plan.campaign_protocol
        if not isinstance(reference, Mapping):
            raise AutonomousResearchError(
                "CAMPAIGN_PROTOCOL_INVALID",
                "campaign plan has no compact durable protocol reference",
            )
        expected_reference = {
            "schema_version": protocol.get("schema_version"),
            "campaign_id": campaign_id,
            "protocol_hash": protocol_hash,
            "budget_version": protocol.get("budget_version"),
            "reassessment_version": protocol.get("reassessment_version"),
        }
        # ExperimentPlan normalizes the compact reference by copying the
        # separately persisted trial fields into ``campaign_protocol``.  Bind
        # the protocol identity itself exactly, while leaving those bounded
        # projections to the field-level checks below.
        reference_identity = {
            key: reference.get(key)
            for key in expected_reference
            if key in reference
        }
        if reference_identity != expected_reference:
            raise AutonomousResearchError(
                "CAMPAIGN_PROTOCOL_INVALID",
                "campaign plan protocol reference is not bound to the durable protocol",
            )
        unexpected_reference_fields = set(reference) - set(expected_reference) - {
            "dataset_boundary",
            "configuration_manifest",
            "observation_horizon",
            "qualification_gates",
            "validation_policy",
            "final_assessment_policy",
        }
        if unexpected_reference_fields:
            raise AutonomousResearchError(
                "CAMPAIGN_PROTOCOL_INVALID",
                "campaign plan protocol reference contains unapproved duplicated protocol fields",
            )
        if plan.campaign_id != campaign_id:
            raise AutonomousResearchError(
                "CAMPAIGN_PROTOCOL_INVALID",
                "campaign plan campaign_id does not match the durable campaign",
            )
        trial_id = str(trial.get("trial_id", "")).strip()
        if plan.campaign_trial_id != trial_id:
            raise AutonomousResearchError(
                "CAMPAIGN_PROTOCOL_INVALID",
                "campaign plan trial_id does not match the durable campaign trial",
            )
        configuration_id = str(trial.get("configuration_id", "")).strip()
        if plan.campaign_configuration_id != configuration_id:
            raise AutonomousResearchError(
                "CAMPAIGN_PROTOCOL_INVALID",
                "campaign plan configuration does not match the durable campaign trial",
            )
        if queue_payload is not None and str(queue_payload.get("campaign_protocol_hash", "")).strip() != protocol_hash:
            raise AutonomousResearchError(
                "CAMPAIGN_PROTOCOL_INVALID",
                "campaign queue protocol hash does not match the durable protocol",
            )
        boundary = self._campaign_trial_boundary(payload, protocol, trial)
        expected_digest = str(
            boundary.get("ordered_row_manifest_digest", boundary.get("content_hash", ""))
        ).strip()
        plan_boundary = plan.dataset_boundary
        plan_digest = (
            str(plan_boundary.get("ordered_row_manifest_digest", plan_boundary.get("content_hash", ""))).strip()
            if isinstance(plan_boundary, Mapping)
            else ""
        )
        if not expected_digest or plan_digest != expected_digest:
            raise AutonomousResearchError(
                "CAMPAIGN_PROTOCOL_INVALID",
                "campaign plan boundary digest does not match the durable trial boundary",
            )
        selector_id = str(plan.dataset_id or "").strip()
        selector_version = str(plan.dataset_version or "").strip()
        if (
            selector_id != str(boundary.get("dataset_id", "")).strip()
            or selector_version != str(boundary.get("dataset_version", "")).strip()
        ):
            raise AutonomousResearchError(
                "CAMPAIGN_PROTOCOL_INVALID",
                "campaign plan dataset selector does not match the durable trial boundary",
            )
        durable_configuration = trial.get("configuration")
        manifest = plan.configuration_manifest
        if not isinstance(durable_configuration, Mapping) or not isinstance(manifest, Mapping):
            raise AutonomousResearchError(
                "CAMPAIGN_PROTOCOL_INVALID",
                "campaign trial configuration is not durably bound",
            )
        expected_manifest = {
            "configuration_id": configuration_id,
            "template": durable_configuration.get("template"),
            "parameters": dict(durable_configuration.get("parameters", {})),
        }
        if _canonical_binding(manifest) != _canonical_binding(expected_manifest):
            raise AutonomousResearchError(
                "CAMPAIGN_PROTOCOL_INVALID",
                "campaign plan configuration manifest is not durably bound",
            )
        return protocol, boundary

    def _campaign_trial_from_payload(
        self,
        payload: Mapping[str, Any],
        *,
        campaign_id: str,
        trial_id: str,
    ) -> tuple[Mapping[str, Any], Mapping[str, Any]]:
        record = self.store.get_operator_job(self.campaign_job_name(campaign_id))
        state_payload = record.get("payload") if isinstance(record, Mapping) else None
        if not isinstance(state_payload, Mapping):
            raise AutonomousResearchError(
                "CAMPAIGN_PROTOCOL_INVALID",
                "campaign operator job is missing",
            )
        trials = state_payload.get("trials")
        trial = next(
            (
                item
                for item in trials
                if isinstance(item, Mapping) and str(item.get("trial_id", "")).strip() == trial_id
            ),
            None,
        ) if isinstance(trials, Sequence) and not isinstance(trials, (str, bytes)) else None
        if trial is None:
            raise AutonomousResearchError(
                "CAMPAIGN_PROTOCOL_INVALID",
                f"campaign trial {trial_id} is missing from the durable operator job",
            )
        return state_payload, trial

    def _campaign_queue_plan(
        self,
        state: Mapping[str, Any],
        trial: Mapping[str, Any],
    ) -> ExperimentPlan:
        protocol, _ = self._campaign_protocol_state(state)
        boundary = self._campaign_trial_boundary(state, protocol, trial)
        config = trial.get("configuration") if isinstance(trial.get("configuration"), Mapping) else {}
        campaign_id = str(state.get("campaign_id", "")).strip()
        trial_id = str(trial.get("trial_id", "")).strip()
        configuration_id = str(trial.get("configuration_id", "")).strip()
        base = state.get("base_proposal") if isinstance(state.get("base_proposal"), Mapping) else {}
        raw_base_plan = base.get("experiment_plan") if isinstance(base.get("experiment_plan"), Mapping) else base
        plan_document = dict(raw_base_plan)
        for redundant in ("exact_dataset_row_boundary", "protected_rows", "protected_row_identity_manifests"):
            plan_document.pop(redundant, None)
        protocol_reference = {
            "schema_version": protocol.get("schema_version"),
            "campaign_id": campaign_id,
            "protocol_hash": _hash_document(protocol),
            "budget_version": protocol.get("budget_version"),
            "reassessment_version": protocol.get("reassessment_version"),
        }
        plan_document.update(
            {
                "plan_id": f"campaign-plan:{campaign_id}:{trial_id}",
                "hypothesis_id": f"campaign-hypothesis:{campaign_id}:{trial_id}",
                "campaign_id": campaign_id,
                "campaign_trial_id": trial_id,
                "campaign_configuration_id": configuration_id,
                "campaign_protocol": protocol_reference,
                "dataset_boundary": {
                    "ordered_row_manifest_digest": str(
                        boundary.get("ordered_row_manifest_digest", boundary.get("content_hash", ""))
                    ).strip()
                },
                "configuration_manifest": {
                    "configuration_id": configuration_id,
                    "template": config.get("template"),
                    "parameters": dict(config.get("parameters", {})),
                },
                "observation_horizon": protocol.get("observation_horizon", {"unit": "observations", "count": 1}),
                "qualification_gates": protocol.get("qualification_gates", {}),
                "validation_policy": protocol.get("validation_policy", {}),
                "final_assessment_policy": protocol.get("final_assessment_policy", {}),
                "template": config.get("template"),
                "parameters": {
                    str(name): [value]
                    for name, value in dict(config.get("parameters", {})).items()
                },
                "max_variants": 1,
                "trial_budget": {"limit": 1, "locked": True},
                "paper_only": True,
                "dataset_selector": {
                    **(
                        dict(plan_document.get("dataset_selector", {}))
                        if isinstance(plan_document.get("dataset_selector"), Mapping)
                        else {}
                    ),
                    "dataset_id": boundary.get("dataset_id"),
                    "dataset_version": boundary.get("dataset_version"),
                },
            }
        )
        plan_document.pop("scientific_rationale", None)
        plan_document.setdefault("market_type", "prediction")
        plan_document.setdefault("market_scope", {
            "mode": "RULE_BASED_MARKETS",
            "instrument": "POLYMARKET",
            "provenance": "canonical",
        })
        plan_document.setdefault("allowed_features", protocol.get("required_features", ()))
        plan_document.setdefault("time_split", "train-validation-holdout")
        plan_document.setdefault("metrics", ("expectancy", "drawdown", "trade_count", "sample_count"))
        plan_document.setdefault("min_samples", int(protocol.get("qualification_gates", {}).get("min_samples", 3)))
        plan_document.setdefault("min_trades", int(protocol.get("qualification_gates", {}).get("min_trades", 0)))
        plan_document.setdefault("research_mode", "PRICE_PROXY_RESEARCH")
        plan_document.setdefault("assumptions", protocol.get("costs", {}))
        plan_document.setdefault("exit_policy", {
            "type": "fixed_holding_period",
            "holding_period": int(protocol.get("observation_horizon", {}).get("count", 1)),
        })
        plan_document.setdefault("filters", {})
        plan_document.setdefault("experiment_family", config.get("template"))
        return ExperimentPlan.from_mapping(plan_document, hypothesis_id=plan_document["hypothesis_id"])



    def _campaign_queue_next(self, now: datetime) -> Mapping[str, Any] | None:
        campaign_id = str(self._campaign_active_state.get("campaign_id", "")).strip()
        job_name = self.campaign_job_name(campaign_id)
        # Queue binding, plan persistence, and the campaign cursor advance are
        # one durable operation.  An immediate transaction also serializes
        # concurrent/restarted workers before either can spend the same slot.
        with self.store.transaction(immediate=True):
            state = dict(self.store.get_operator_job(job_name) or {})
            payload = state.get("payload") if isinstance(state.get("payload"), Mapping) else {}
            payload = dict(payload)
            trials = [dict(item) for item in payload.get("trials", ()) if isinstance(item, Mapping)]
            next_trial = next(
                (
                    item
                    for item in trials
                    if item.get("status") == "PLANNED"
                    and str(item.get("reassessment_of", "")).strip()
                ),
                None,
            )
            if next_trial is None:
                next_trial = next((item for item in trials if item.get("status") == "PLANNED"), None)
            if next_trial is None:
                return None
            protocol, protocol_hash = self._campaign_protocol_state(payload)
            plan = self._campaign_queue_plan(payload, next_trial)
            campaign_trial_id = str(next_trial.get("trial_id", "")).strip()
            queue_payload = {
                "proposal_id": plan.hypothesis_id,
                "hypothesis_id": plan.hypothesis_id,
                "statement": str(protocol.get("scientific_rationale", "Finite Polymarket campaign")),
                "source": "axiom-finite-campaign",
                "tests": ["chronological validation on the protected dataset boundary"],
                "dataset_id": plan.dataset_id,
                "dataset_version": plan.dataset_version,
                "time_split": plan.methodology.get("time_split", "train-validation-holdout"),
                "experiment_plan": plan.as_dict(),
                "campaign_id": campaign_id,
                "campaign_trial_id": campaign_trial_id,
                "campaign_configuration_id": next_trial.get("configuration_id"),
                "campaign_protocol_hash": protocol_hash,
                "paper_only": True,
                "predeclared_starting_set": False,
                "automatic_mutations": False,
            }
            dedupe_key = f"campaign:{campaign_id}:{campaign_trial_id}"
            queued = next(
                (
                    item
                    for item in self.bus.list_campaign_trials(campaign_id, limit=10_000)
                    if str(item.payload.get("campaign_trial_id", "")).strip() == campaign_trial_id
                ),
                None,
            )
            if queued is not None:
                existing_payload = queued.payload
                existing_plan = existing_payload.get("experiment_plan")
                existing_plan_matches = False
                if isinstance(existing_plan, Mapping):
                    try:
                        existing_bound_plan = ExperimentPlan.from_mapping(
                            existing_plan,
                            hypothesis_id=str(existing_payload.get("hypothesis_id", "")).strip() or None,
                        )
                    except (ExperimentPlanError, TypeError, ValueError):
                        existing_bound_plan = None
                    existing_plan_matches = (
                        existing_bound_plan is not None
                        and existing_bound_plan.plan_id == plan.plan_id
                        and existing_bound_plan.plan_hash == plan.plan_hash
                    )
                if (
                    str(existing_payload.get("campaign_id", "")).strip() != campaign_id
                    or str(existing_payload.get("proposal_id", "")).strip() != plan.hypothesis_id
                    or str(existing_payload.get("hypothesis_id", "")).strip() != plan.hypothesis_id
                    or not existing_plan_matches
                    or str(existing_payload.get("campaign_protocol_hash", "")).strip() != protocol_hash
                ):
                    raise AutonomousResearchError(
                        "CAMPAIGN_QUEUE_BINDING_MISMATCH",
                        f"existing queue binding does not match planned trial {campaign_trial_id}",
                    )
            else:
                # Keep the plan PENDING until the queue write succeeds.  If
                # payload validation rejects the trial, this transaction rolls
                # back and the trial remains PLANNED for operator diagnosis.
                self.store.save_experiment_plan(
                    plan.plan_id,
                    plan.as_dict(),
                    hypothesis_id=plan.hypothesis_id,
                    plan_hash=plan.plan_hash,
                    status="PENDING",
                    timestamp=now,
                )
                queued = self.bus.submit_campaign_trial(
                    queue_payload,
                    campaign_id=campaign_id,
                    trial_id=campaign_trial_id,
                    dedupe_key=dedupe_key,
                    available_at=now,
                )
            for item in trials:
                if item.get("trial_id") == campaign_trial_id:
                    item.update(
                        {
                            "status": "RUNNING",
                            "proposal_id": plan.hypothesis_id,
                            "hypothesis_id": plan.hypothesis_id,
                            "plan_id": plan.plan_id,
                            "plan_hash": plan.plan_hash,
                            "queue_item_id": queued.item_id,
                            "queued_at": ensure_utc(now).isoformat(),
                        }
                    )
            payload["trials"] = trials
            counts = dict(payload.get("counts") or {})
            counts["planned"] = max(0, int(counts.get("planned", 0)) - 1)
            counts["running"] = int(counts.get("running", 0)) + 1
            payload["counts"] = counts
            payload["budget_used"] = int(payload.get("budget_used", 0)) + 1
            payload["budget_remaining"] = max(0, CAMPAIGN_BUDGET_LIMIT - payload["budget_used"])
            payload["status"] = "RUNNING"
            payload["next_real_job"] = None
            payload["last_updated_at"] = ensure_utc(now).isoformat()
            self.store.set_operator_job(job_name, "RUNNING", payload, resumable=True, timestamp=now)
            self._campaign_active_state = payload
            return {
                "trial_id": campaign_trial_id,
                "queue_item_id": queued.item_id,
                "proposal_id": plan.hypothesis_id,
                "hypothesis_id": plan.hypothesis_id,
                "plan_id": plan.plan_id,
                "plan_hash": plan.plan_hash,
            }

    def start_polymarket_campaign(
        self,
        campaign_id: str,
        proposal: Mapping[str, Any] | None = None,
        *,
        dataset_id: str | None = None,
        dataset_version: str | None = None,
        observation_horizon: int = 1,
        finalist_count: int = CAMPAIGN_MAX_FINALISTS,
        qualification_gates: Mapping[str, Any] | None = None,
        now: datetime | None = None,
    ) -> Mapping[str, Any]:
        """Persist a finite protocol and queue exactly its next trial."""
        current = ensure_utc(now or self.clock())
        campaign = str(campaign_id).strip()
        if not campaign:
            raise ValueError("campaign_id is required")
        if isinstance(observation_horizon, bool) or not isinstance(observation_horizon, int) or observation_horizon < 1:
            raise ValueError("observation_horizon must be a positive observation count")
        if isinstance(finalist_count, bool) or not 1 <= int(finalist_count) <= CAMPAIGN_MAX_FINALISTS:
            raise ValueError("finalist_count exceeds the campaign safety bound")
        existing = self.store.get_operator_job(self.campaign_job_name(campaign))
        if isinstance(existing, Mapping):
            existing_payload = dict(existing.get("payload") or {})
            self._campaign_protocol_state(existing_payload)
            self._campaign_active_state = existing_payload
            return existing_payload
        source = dict(proposal or {})
        resolved_dataset_id = str(
            dataset_id
            or source.get("dataset_id")
            or (
                source.get("dataset_selector", {}).get("dataset_id")
                if isinstance(source.get("dataset_selector"), Mapping)
                else ""
            )
            or "Polymarket-historical"
        ).strip()
        resolved_dataset_version = str(
            dataset_version
            or source.get("dataset_version")
            or (
                source.get("dataset_selector", {}).get("dataset_version")
                if isinstance(source.get("dataset_selector"), Mapping)
                else ""
            )
            or ""
        ).strip()
        if not resolved_dataset_version:
            raise ValueError("dataset_version is required")
        rows = self._campaign_dataset_rows(self.store, resolved_dataset_id, resolved_dataset_version)
        prior = self._campaign_prior_configuration_keys()
        configurations = [
            dict(item)
            for item in self.campaign_configurations()
            if _campaign_configuration_key(item) not in prior
        ]
        protocol = self._campaign_default_protocol(
            campaign_id=campaign,
            dataset_id=resolved_dataset_id,
            dataset_version=resolved_dataset_version,
            rows=rows,
            configurations=configurations,
            qualification_gates=qualification_gates,
            finalist_count=int(finalist_count),
            observation_horizon=int(observation_horizon),
        )
        protocol_hash = _hash_document(protocol)
        trials: list[dict[str, Any]] = [
            {
                "trial_id": f"trial:{campaign}:{index:02d}",
                "configuration_id": item["configuration_id"],
                "configuration": dict(item),
                "status": "PLANNED",
                "result": None,
            }
            for index, item in enumerate(configurations)
        ]
        # The legacy zero-edge control is evidence, not a new trial.
        control = {
            "configuration_id": "control:zero_edge",
            "template": "probability_mispricing",
            "parameters": {"threshold": 0.05},
            "status": "SELECTION_EXCLUDED",
            "reason": "existing zero-edge control retained; never rerun",
        }
        payload: dict[str, Any] = {
            "schema_version": "polymarket-finite-campaign-v1",
            "campaign_id": campaign,
            "status": "PLANNED" if trials else "CAMPAIGN_EXHAUSTED_NO_QUALIFIED_STRATEGY",
            "protocol": protocol,
            "protocol_hash": protocol_hash,
            "reassessment_boundaries": {},
            "base_proposal": source,
            "dataset_id": resolved_dataset_id,
            "dataset_version": resolved_dataset_version,
            "budget_limit": CAMPAIGN_BUDGET_LIMIT,
            "budget_used": 0,
            "budget_remaining": CAMPAIGN_BUDGET_LIMIT,
            "fixed_configuration_count": len(configurations),
            "counts": {
                "planned": len(trials),
                "running": 0,
                "economic_rejection": 0,
                "data_insufficient": 0,
                "software_or_input_error": 0,
                "validation_qualified": 0,
                "final_assessment": 0,
                "qualified": 0,
            },
            "trials": trials,
            "selection_excluded_evidence": [control],
            "qualified_candidate_ids": [],
            "finalist_candidate_ids": [],
            "final_assessment_evaluated": False,
            "reassessment_count": 0,
            "last_result": None,
            "next_real_job": None,
            "created_at": ensure_utc(current).isoformat(),
            "last_updated_at": ensure_utc(current).isoformat(),
            "paper_only": True,
            "research_only": True,
        }
        job_name = self.campaign_job_name(campaign)
        # This write is deliberately before the first bus write.  A crash
        # between the two leaves a durable protocol that can be resumed.
        self.store.set_operator_job(job_name, payload["status"], payload, resumable=True, timestamp=current)
        self._campaign_active_state = payload
        if trials:
            with self.store.transaction():
                self._campaign_queue_next(current)
        return dict(self.store.get_operator_job(job_name).get("payload", payload))

    start_campaign = start_polymarket_campaign

    def campaign_state(self, campaign_id: str) -> Mapping[str, Any] | None:
        record = self.store.get_operator_job(self.campaign_job_name(campaign_id))
        payload = record.get("payload") if isinstance(record, Mapping) else None
        if isinstance(payload, Mapping):
            self._campaign_active_state = dict(payload)
            return dict(payload)
        return None

    get_campaign_state = campaign_state

    def _campaign_schedule_data_job(
        self,
        payload: dict[str, Any],
        trial: Mapping[str, Any],
        *,
        now: datetime,
        reason: str,
    ) -> str:
        campaign_id = str(payload.get("campaign_id", "")).strip()
        trial_id = str(trial.get("trial_id", "")).strip()
        job_name = f"polymarket-research-data:{campaign_id}:{trial_id}"
        producer_name = f"polymarket-dataset-producer:{campaign_id}:{trial_id}"
        data_payload = {
            "schema_version": "polymarket-research-data-v1",
            "job_kind": "POLYMARKET_RESEARCH_DATA_PREREQUISITE",
            "campaign_id": campaign_id,
            "campaign_trial_id": trial_id,
            "dataset_id": payload.get("dataset_id"),
            "dataset_version": payload.get("dataset_version"),
            "reason": reason,
            "producer_job": producer_name,
            "status_taxonomy": ["SCHEDULED", "RUNNING", "NO_NEW_DATA", "COMPLETE", "FAILED", "EXHAUSTED"],
            "cursor": trial.get("data_cursor"),
            "request_budget": 1,
            "backoff_seconds": 60,
            "next_attempt_at": ensure_utc(now).isoformat(),
            "paper_only": True,
        }
        self.store.set_operator_job(job_name, "SCHEDULED", data_payload, resumable=True, timestamp=now)
        payload["next_real_job"] = job_name
        return job_name

    @staticmethod
    def _campaign_result_classification(result: Mapping[str, Any]) -> str:
        code = str(result.get("reason_code", "")).strip().upper()
        if code in {"INSUFFICIENT_DATA", "DATA_INSUFFICIENT", "DATASET_PROVENANCE_INVALID", "DATASET_ATTESTATION_MISSING"}:
            return "DATA_INSUFFICIENT"
        if code in {"PROCESSING_FAILED", "INVALID_PLAN", "INVALID_DATASET", "SOFTWARE_OR_INPUT_ERROR"}:
            return "SOFTWARE_OR_INPUT_ERROR"
        if result.get("accepted") is not False and str(result.get("stage", "")).upper() != CandidateStage.REJECTED.value:
            return "VALIDATION_QUALIFIED"
        return "ECONOMIC_REJECTION"

    def _campaign_final_assessment(self, payload: dict[str, Any], *, now: datetime) -> None:
        if bool(payload.get("final_assessment_evaluated")):
            return
        protocol, _ = self._campaign_protocol_state(payload)
        payload["status"] = "FINAL_ASSESSMENT"
        trials = [dict(item) for item in payload.get("trials", ()) if isinstance(item, Mapping)]
        qualified = [
            item
            for item in trials
            if item.get("status") == "VALIDATION_QUALIFIED"
            and not bool(item.get("selection_excluded"))
        ][: int(protocol.get("final_assessment_policy", {}).get("max_finalists", 1))]
        protected = protocol.get("protected_row_identity_manifests")
        protected = protected if isinstance(protected, Mapping) else {}
        final_manifest = protected.get("final", ())
        boundary = protocol.get("dataset_boundary") if isinstance(protocol, Mapping) else {}
        boundary = boundary if isinstance(boundary, Mapping) else {}
        final_boundary = boundary.get("split_boundaries", {}).get("final", ())
        if not final_boundary:
            final_boundary = final_manifest
        final_ids = {
            str(item.get("row_identity"))
            for item in final_manifest
            if isinstance(item, Mapping)
        } if isinstance(final_manifest, (list, tuple)) else set()
        final_row_count = (
            int(final_manifest.get("row_count", 0))
            if isinstance(final_manifest, Mapping)
            else len(final_ids)
        )
        final_digest = (
            str(boundary.get("ordered_row_manifest_digest", boundary.get("content_hash", ""))).strip()
            if isinstance(boundary, Mapping)
            else ""
        )
        assessments: list[dict[str, Any]] = []
        for trial in qualified:
            plan_id = str(trial.get("plan_id", "")).strip()
            plan_record = self.store.load_experiment_plan(plan_id) if plan_id else None
            raw_plan = plan_record.get("plan") if isinstance(plan_record, Mapping) else None
            if not isinstance(raw_plan, Mapping):
                continue
            try:
                plan = ExperimentPlan.from_mapping(
                    raw_plan,
                    hypothesis_id=str(plan_record.get("hypothesis_id", "")).strip() or None,
                )
                expected_plan_hash = str(trial.get("plan_hash", "")).strip()
                if not expected_plan_hash or expected_plan_hash != plan.plan_hash:
                    raise AutonomousResearchError(
                        "CAMPAIGN_PROTOCOL_INVALID",
                        "campaign final-assessment plan hash is not durably bound",
                    )
                _, trial_boundary = self._validate_campaign_plan_binding(payload, trial, plan)
                trial_protected = protected if not str(trial.get("reassessment_of", "")).strip() else {}
                trial_manifest = trial_protected.get("final", ())
                trial_boundary_splits = trial_boundary.get("split_boundaries", {})
                trial_descriptor = (
                    trial_boundary_splits.get("final", ())
                    if isinstance(trial_boundary_splits, Mapping)
                    else ()
                ) or trial_manifest
                expected_digest = str(
                    trial_boundary.get(
                        "ordered_row_manifest_digest",
                        trial_boundary.get("content_hash", ""),
                    )
                ).strip()
                rows = self._campaign_dataset_rows(
                    self.store,
                    str(trial_boundary.get("dataset_id", "")).strip(),
                    str(trial_boundary.get("dataset_version", "")).strip(),
                )
                actual_provenance = _campaign_compact_row_provenance(rows)
                if (
                    actual_provenance["row_count"] != int(trial_boundary.get("row_count", -1))
                    or actual_provenance["exact_cutoff"] != trial_boundary.get("exact_cutoff")
                    or actual_provenance["ordered_row_manifest_digest"] != expected_digest
                ):
                    raise AutonomousResearchError(
                        "DATASET_PROVENANCE_INVALID",
                        "campaign final-assessment dataset boundary changed",
                    )
                final_rows = _campaign_rows_for_split(rows, trial_descriptor)
                trial_digest = expected_digest
                strategy = plan.strategy_for(plan.variants()[0], str(trial.get("candidate_id", trial.get("trial_id"))))
                metrics = dict(self._run_backtest(plan, strategy, final_rows))
                gates = protocol.get("qualification_gates", {})
                sample_check = minimum_sample_check(
                    int(metrics.get("sample_count", 0)),
                    trades=int(metrics.get("filled_trades", 0)),
                    min_observations=int(gates.get("min_samples", 0)),
                    min_trades=int(gates.get("min_trades", 0)),
                )
                passed = (
                    _finite(metrics.get("expectancy"), -math.inf) >= _finite(gates.get("min_expectancy"), 0.0)
                    and bool(sample_check["passed"])
                )
                assessments.append(
                    {
                        "trial_id": trial.get("trial_id"),
                        "candidate_id": trial.get("candidate_id"),
                        "status": "QUALIFIED" if passed else "REJECTED",
                        "passed": passed,
                        "minimum_sample_check": sample_check,
                        "metrics": _compact_evidence(metrics),
                        "protected_row_identity_manifest": dict(trial_descriptor) if isinstance(trial_descriptor, Mapping) else list(trial_manifest),
                        "final_rows_digest": trial_digest,
                    }
                )
            except (AutonomousResearchError, ExperimentPlanError, TypeError, ValueError) as exc:
                assessments.append(
                    {
                        "trial_id": trial.get("trial_id"),
                        "candidate_id": trial.get("candidate_id"),
                        "status": "SOFTWARE_OR_INPUT_ERROR",
                        "reason": str(exc),
                    }
                )
        qualified_ids = [
            str(item.get("candidate_id"))
            for item in assessments
            if item.get("status") == "QUALIFIED" and str(item.get("candidate_id", "")).strip()
        ]
        persisted_final_manifest = (
            dict(final_manifest)
            if isinstance(final_manifest, Mapping)
            else dict(final_boundary)
            if isinstance(final_boundary, Mapping)
            else list(final_manifest)
        )
        payload["final_assessment"] = {
            "evaluated_once": True,
            "assessments": assessments,
            "protected_row_identity_manifest": persisted_final_manifest,
            "protected_row_identity_digest": final_digest,
            "row_count": final_row_count,
        }
        payload["final_assessment_evaluated"] = True
        payload["qualified_candidate_ids"] = qualified_ids
        payload["counts"]["final_assessment"] = len(assessments)
        payload["counts"]["qualified"] = len(qualified_ids)
        payload["status"] = "COMPLETED_QUALIFIED" if qualified_ids else "CAMPAIGN_EXHAUSTED_NO_QUALIFIED_STRATEGY"
        payload["next_real_job"] = None
        payload["last_updated_at"] = ensure_utc(now).isoformat()

    def _advance_campaign_after_result(
        self,
        item: ResearchQueueItem,
        result: Mapping[str, Any],
        now: datetime,
    ) -> None:
        campaign_id = str(item.payload.get("campaign_id", "")).strip()
        if not campaign_id:
            return
        job_name = self.campaign_job_name(campaign_id)
        record = self.store.get_operator_job(job_name)
        if not isinstance(record, Mapping):
            return
        payload = dict(record.get("payload") or {})
        trial_id = str(item.payload.get("campaign_trial_id", "")).strip()
        trials = [dict(entry) for entry in payload.get("trials", ()) if isinstance(entry, Mapping)]
        trial = next((entry for entry in trials if str(entry.get("trial_id", "")) == trial_id), None)
        if trial is None or str(trial.get("status", "")).upper() in CAMPAIGN_TRIAL_TERMINAL:
            return
        classification = self._campaign_result_classification(result)
        trial.update(
            {
                "status": classification,
                "candidate_id": result.get("candidate_id"),
                "result": dict(result),
                "completed_at": ensure_utc(now).isoformat(),
            }
        )
        payload["trials"] = trials
        counts = dict(payload.get("counts") or {})
        counts["running"] = max(0, int(counts.get("running", 0)) - 1)
        count_key = classification.lower()
        counts[count_key] = int(counts.get(count_key, 0)) + 1
        payload["counts"] = counts
        payload["last_result"] = {
            "trial_id": trial_id,
            "status": classification,
            "reason_code": result.get("reason_code"),
            "candidate_id": result.get("candidate_id"),
        }
        self._campaign_active_state = payload
        if classification == "DATA_INSUFFICIENT":
            payload["status"] = "WAITING_FOR_DATA"
            self._campaign_schedule_data_job(
                payload,
                trial,
                now=now,
                reason=str(result.get("reason", "insufficient observations")),
            )
        elif classification == "SOFTWARE_OR_INPUT_ERROR":
            payload["status"] = "SOFTWARE_OR_INPUT_ERROR"
            payload["next_real_job"] = None
        else:
            payload["status"] = "RUNNING"
            self.store.set_operator_job(job_name, "RUNNING", payload, resumable=True, timestamp=now)
            queued_next = (
                self._campaign_queue_next(now)
                if payload["budget_remaining"] > 0
                else None
            )
            if queued_next is None:
                self._campaign_final_assessment(payload, now=now)
        if classification in {"DATA_INSUFFICIENT", "SOFTWARE_OR_INPUT_ERROR"}:
            self.store.set_operator_job(job_name, payload["status"], payload, resumable=True, timestamp=now)
        elif payload.get("status") in {"CAMPAIGN_EXHAUSTED_NO_QUALIFIED_STRATEGY", "COMPLETED_QUALIFIED"}:
            self.store.set_operator_job(job_name, payload["status"], payload, resumable=False, timestamp=now)

    def _campaign_reassessment_dataset_identity(
        self,
        payload: Mapping[str, Any],
        evidence_identity: str,
        *,
        dataset_id: str | None,
        dataset_version: str | None,
    ) -> tuple[str, str]:
        current_id = str(payload.get("dataset_id", "")).strip()
        current_version = str(payload.get("dataset_version", "")).strip()
        resolved_id = str(dataset_id or current_id).strip()
        resolved_version = str(dataset_version or "").strip()
        if resolved_version:
            return resolved_id, resolved_version
        if not resolved_id:
            return resolved_id, current_version
        # Attestation hashes are the normal evidence identity. Resolve the
        # exact immutable version instead of guessing from version ordering.
        catalog_lister = getattr(self.store, "list_dataset_catalog", None)
        catalog_loader = getattr(self.store, "load_dataset_catalog", None)
        attestation_loader = getattr(self.store, "load_dataset_integrity_attestation", None)
        candidates: list[str] = []
        if callable(catalog_lister):
            try:
                catalogs = catalog_lister(
                    source_type="HISTORICAL",
                    market_type=MarketType.PREDICTION.value,
                    limit=256,
                )
            except TypeError:
                try:
                    catalogs = catalog_lister(limit=256)
                except Exception:
                    catalogs = ()
            except Exception:
                catalogs = ()
            for catalog in catalogs if isinstance(catalogs, Sequence) else ():
                if not isinstance(catalog, Mapping):
                    continue
                listed_id = str(catalog.get("dataset_id", "")).strip()
                version = str(catalog.get("dataset_version", catalog.get("version", ""))).strip()
                if listed_id == resolved_id and version:
                    candidates.append(version)
        versions = getattr(self.store, "dataset_versions", None)
        if callable(versions):
            try:
                candidates.extend(str(item).strip() for item in versions(resolved_id))
            except Exception:
                pass
        seen: set[str] = set()
        for version in candidates:
            if not version or version in seen or version == current_version:
                continue
            seen.add(version)
            if callable(catalog_loader):
                try:
                    catalog = catalog_loader(resolved_id, version)
                except Exception:
                    continue
                if not isinstance(catalog, Mapping):
                    continue
            if not callable(attestation_loader):
                continue
            try:
                attestation = attestation_loader(resolved_id, version)
            except Exception:
                continue
            if (
                isinstance(attestation, Mapping)
                and str(attestation.get("attestation_hash", "")).strip() == evidence_identity
                and str(attestation.get("status", "")).strip().upper() == "CURRENT"
            ):
                return resolved_id, version
        return resolved_id, current_version

    @staticmethod
    def _campaign_boundary_for_dataset(
        dataset_id: str,
        dataset_version: str,
        rows: Sequence[Mapping[str, Any]],
    ) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
        provenance = _campaign_compact_row_provenance(rows)
        boundary = {
            "schema_version": provenance["schema_version"],
            "dataset_id": dataset_id,
            "dataset_version": dataset_version,
            "source_type": "HISTORICAL",
            "exact_cutoff": provenance["exact_cutoff"],
            "row_count": provenance["row_count"],
            "ordered_row_manifest_digest": provenance["ordered_row_manifest_digest"],
            "ordered_row_manifest_root": provenance["ordered_row_manifest_root"],
            "content_hash": provenance["content_hash"],
            "split_boundaries": dict(provenance["split_boundaries"]),
        }
        return boundary, dict(provenance["split_boundaries"])

    def reassess_campaign(
        self,
        campaign_id: str,
        *,
        evidence_identity: str,
        dataset_id: str | None = None,
        dataset_version: str | None = None,
        now: datetime | None = None,
    ) -> Mapping[str, Any]:
        """Allow one auditable changed-evidence reassessment only."""
        current = ensure_utc(now or self.clock())
        job_name = self.campaign_job_name(campaign_id)
        record = self.store.get_operator_job(job_name)
        if not isinstance(record, Mapping):
            raise ValueError("campaign does not exist")
        payload = dict(record.get("payload") or {})
        protocol, _ = self._campaign_protocol_state(payload)
        identity = str(evidence_identity).strip()
        if not identity:
            raise ValueError("evidence_identity is required")
        previous_identity = str(payload.get("last_evidence_identity", "")).strip() or None
        if identity == previous_identity or int(payload.get("reassessment_count", 0)) >= 1:
            return payload
        waiting = [
            dict(item)
            for item in payload.get("trials", ())
            if isinstance(item, Mapping) and item.get("status") == "DATA_INSUFFICIENT"
        ]
        if not waiting:
            return payload
        resolved_dataset_id, resolved_dataset_version = self._campaign_reassessment_dataset_identity(
            payload,
            identity,
            dataset_id=dataset_id,
            dataset_version=dataset_version,
        )
        original_dataset_id = str(payload.get("dataset_id", "")).strip()
        original_dataset_version = str(payload.get("dataset_version", "")).strip()
        reassessment_boundary: Mapping[str, Any] = protocol.get("dataset_boundary", {})
        if not isinstance(reassessment_boundary, Mapping):
            reassessment_boundary = {}
        if (
            resolved_dataset_id != original_dataset_id
            or resolved_dataset_version != original_dataset_version
        ):
            new_rows = self._campaign_dataset_rows(
                self.store,
                resolved_dataset_id,
                resolved_dataset_version,
            )
            reassessment_boundary, reassessment_manifests = self._campaign_boundary_for_dataset(
                resolved_dataset_id,
                resolved_dataset_version,
                new_rows,
            )
        payload["reassessment_count"] = 1
        payload["last_evidence_identity"] = identity
        payload["reassessment_evidence"] = {
            "identity": identity,
            "changed_from": previous_identity,
            "dataset_id": resolved_dataset_id,
            "dataset_version": resolved_dataset_version,
            "recorded_at": ensure_utc(current).isoformat(),
            "auditable": True,
        }
        reassessment_boundaries = dict(payload.get("reassessment_boundaries") or {})
        trials = [dict(item) for item in payload.get("trials", ()) if isinstance(item, Mapping)]
        existing_trial_ids = {
            str(item.get("trial_id", "")).strip()
            for item in trials
        }
        appended = 0
        for old in waiting:
            reassessment_id = f"{old.get('trial_id')}:reassessment-1"
            if reassessment_id in existing_trial_ids:
                continue
            trials.append(
                {
                    "trial_id": reassessment_id,
                    "configuration_id": old.get("configuration_id"),
                    "configuration": dict(old.get("configuration", {})),
                    "status": "PLANNED",
                    "reassessment_of": old.get("trial_id"),
                    "dataset_id": resolved_dataset_id,
                    "dataset_version": resolved_dataset_version,
                    "result": None,
                }
            )
            reassessment_boundaries[reassessment_id] = dict(reassessment_boundary)
            existing_trial_ids.add(reassessment_id)
            appended += 1
        payload["reassessment_boundaries"] = reassessment_boundaries
        payload["trials"] = trials
        counts = dict(payload.get("counts") or {})
        counts["planned"] = int(counts.get("planned", 0)) + appended
        payload["counts"] = counts
        payload["status"] = "RUNNING"
        payload["next_real_job"] = None
        self.store.set_operator_job(job_name, "RUNNING", payload, resumable=True, timestamp=current)
        self.store.save_report_if_absent(
            f"campaign-reassessment:{campaign_id}:1",
            {
                "report_type": "polymarket_campaign_reassessment",
                "campaign_id": campaign_id,
                "evidence_identity": identity,
                "changed_from": previous_identity,
                "reassessment_count": 1,
                "paper_only": True,
            },
            experiment_id=job_name,
        )
        self._campaign_active_state = payload
        with self.store.transaction():
            self._campaign_queue_next(current)
        return dict(self.store.get_operator_job(job_name).get("payload", payload))
    def advance_campaign(
        self,
        campaign_id: str,
        *,
        evidence_identity: str | None = None,
        dataset_id: str | None = None,
        dataset_version: str | None = None,
        now: datetime | None = None,
    ) -> Mapping[str, Any]:
        if evidence_identity is not None:
            return self.reassess_campaign(
                campaign_id,
                evidence_identity=evidence_identity,
                dataset_id=dataset_id,
                dataset_version=dataset_version,
                now=now,
            )
        state = self.campaign_state(campaign_id)
        return dict(state or {})

    def _legacy_provenance_state(
        self,
        plan: ExperimentPlan | None,
        document: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Return a bounded, canonical snapshot of successor provenance state."""
        if plan is not None:
            dataset_id = str(plan.dataset_id or "").strip()
            dataset_version = str(plan.dataset_version or "").strip()
            selector = plan.dataset_selector
        else:
            source = document.get("experiment_plan")
            source = source if isinstance(source, Mapping) else document
            raw_selector = source.get("dataset_selector")
            selector = raw_selector if isinstance(raw_selector, Mapping) else source
            dataset_id = str(selector.get("dataset_id") or source.get("dataset_id") or "").strip()
            dataset_version = str(
                selector.get("dataset_version")
                or selector.get("version")
                or source.get("dataset_version")
                or source.get("version")
                or ""
            ).strip()
        declared: dict[str, Any] = {}
        for name in ("source_type", "source", "provider", "timeframe", "interval"):
            value = selector.get(name)
            if value is not None and str(value).strip():
                declared[name] = str(value).strip()
        state: dict[str, Any] = {
            "dataset_id": dataset_id or None,
            "dataset_version": dataset_version or None,
            "declared_selectors": declared,
            "catalog": None,
            "attestation": None,
        }
        if not dataset_id or not dataset_version:
            return state
        catalog_loader = getattr(self.store, "load_dataset_catalog", None)
        if callable(catalog_loader):
            try:
                catalog = catalog_loader(dataset_id, dataset_version)
            except Exception:
                catalog = None
            if isinstance(catalog, Mapping):
                metadata = catalog.get("metadata")
                metadata = metadata if isinstance(metadata, Mapping) else {}
                state["catalog"] = {
                    name: catalog.get(name)
                    for name in (
                        "dataset_id",
                        "dataset_version",
                        "version",
                        "provider",
                        "source",
                        "instrument",
                        "market_type",
                        "timeframe",
                        "source_type",
                        "snapshot_id",
                        "row_count",
                        "completeness",
                        "missing_ranges",
                    )
                    if catalog.get(name) is not None
                }
                state["catalog"]["metadata"] = {
                    name: metadata.get(name)
                    for name in (
                        "provider",
                        "source",
                        "instrument",
                        "market_type",
                        "timeframe",
                        "source_type",
                        "research_quality",
                        "historical_order_book_available",
                        "contamination_result",
                        "provenance_version",
                        "policy_version",
                    )
                    if metadata.get(name) is not None
                }
        attestation_loader = getattr(self.store, "load_dataset_integrity_attestation", None)
        if callable(attestation_loader):
            try:
                attestation = attestation_loader(dataset_id, dataset_version)
            except Exception:
                attestation = None
            if isinstance(attestation, Mapping):
                state["attestation"] = {
                    name: attestation.get(name)
                    for name in (
                        "dataset_id",
                        "dataset_version",
                        "status",
                        "reason",
                        "reasons",
                        "contamination_result",
                        "attestation_hash",
                        "row_count",
                        "completeness",
                        "source_type",
                        "market_type",
                        "execution_fidelity",
                    )
                    if attestation.get(name) is not None
                }
        return state

    def _persist_legacy_recovery_evidence(
        self,
        evidence: Mapping[str, Any],
        *,
        queue_item: ResearchQueueItem | None = None,
        now: datetime,
    ) -> bool:
        """Persist one recovery decision without mutating its predecessor.

        A queued successor gets a queue event so the ordinary activity
        projection can show the handoff before the item is claimed.  A blocked
        predecessor has no queue item to attach to, so it gets one stable
        report per canonical blocker/provenance state.  Both paths remain
        idempotent on the recovery key.
        """
        recovery_key = str(evidence.get("recovery_key", "")).strip()
        detail = dict(evidence)
        state_digest = _legacy_recovery_state_digest(detail)
        detail["provenance_state_digest"] = state_digest
        if isinstance(evidence, dict):
            evidence["provenance_state_digest"] = state_digest
        if queue_item is not None:
            item_id = str(queue_item.item_id).strip()
            if not item_id:
                return False
            try:
                events = self.store.list_research_queue_events(item_id, limit=256)
                if any(
                    isinstance(event, Mapping)
                    and isinstance(event.get("detail"), Mapping)
                    and str(event["detail"].get("recovery_key", "")).strip() == recovery_key
                    for event in events
                ):
                    return True
                self.store.record_research_queue_event(
                    item_id,
                    "LEGACY_SCOPE_RECOVERY",
                    detail,
                    timestamp=now,
                )
                return True
            except (KeyError, RuntimeError, TypeError, ValueError):
                return False

        report_id = (
            "legacy-recovery-"
            + hashlib.sha256(recovery_key.encode("utf-8")).hexdigest()[:24]
            + "-"
            + state_digest.removeprefix("sha256:")
        )
        try:
            if self.store.load_report(report_id) is not None:
                return True
            self.store.save_report_if_absent(
                report_id,
                {"report_type": "legacy_scope_recovery", **detail},
                experiment_id=detail.get("predecessor_candidate_id"),
            )
            return True
        except (RuntimeError, TypeError, ValueError):
            return False

    def _recover_legacy_predecessors(self, now: datetime) -> tuple[Mapping[str, Any], ...]:
        """Run a bounded, deterministic legacy-scope handoff before claiming.

        Only persisted prediction predecessors with a nested target market
        list are eligible for conversion.  Classification and failure
        evidence are durable, while the old lifecycle row is deliberately
        never rewritten.
        """
        page_loader = getattr(self.store, "load_candidate_lifecycle_page", None)
        loader = getattr(self.store, "load_candidate_lifecycle", None)
        if not callable(page_loader) and not callable(loader):
            return ()

        # The cursor is worker state, not processor state.  A processor
        # restart therefore resumes the same keyset page instead of silently
        # returning to the oldest lifecycle rows.
        state_loader = getattr(self.store, "get_scheduler_state", None)
        state_setter = getattr(self.store, "set_scheduler_state", None)
        durable_pagination = (
            callable(page_loader)
            and callable(state_loader)
            and callable(state_setter)
        )
        state: Mapping[str, Any] = {}
        cursor_updated_at: str | None = None
        cursor_candidate_id: str | None = None
        if durable_pagination:
            try:
                loaded_state = state_loader(_LEGACY_RECOVERY_STATE_NAME)
            except (RuntimeError, TypeError, ValueError):
                return ()
            if isinstance(loaded_state, Mapping):
                state = loaded_state
                cursor = loaded_state.get("cursor")
                if isinstance(cursor, Mapping):
                    timestamp = str(cursor.get("updated_at", "")).strip()
                    candidate_id = str(cursor.get("candidate_id", "")).strip()
                    if timestamp and candidate_id:
                        cursor_updated_at = timestamp
                        cursor_candidate_id = candidate_id
        try:
            if callable(page_loader):
                if cursor_updated_at is not None and cursor_candidate_id is not None:
                    records = page_loader(
                        limit=_MAX_LEGACY_RECOVERY_ITEMS,
                        after_updated_at=cursor_updated_at,
                        after_candidate_id=cursor_candidate_id,
                    )
                    # Reaching the end starts a new deterministic pass.  The
                    # first page is read in this same cycle so a lone row (or
                    # a short tail) cannot make the worker idle for a cycle.
                    if not records:
                        cursor_updated_at = None
                        cursor_candidate_id = None
                        records = page_loader(limit=_MAX_LEGACY_RECOVERY_ITEMS)
                else:
                    records = page_loader(limit=_MAX_LEGACY_RECOVERY_ITEMS)
            else:
                records = loader(limit=_MAX_LEGACY_RECOVERY_ITEMS)
        except (RuntimeError, TypeError, ValueError):
            return ()
        if not isinstance(records, list):
            return ()

        candidates: list[Mapping[str, Any]] = []
        for record in records:
            if not isinstance(record, Mapping):
                continue
            payload = record.get("payload")
            if not isinstance(payload, Mapping):
                continue
            plan = payload.get("experiment_plan")
            plan = plan if isinstance(plan, Mapping) else {}
            market_type = plan.get("market_type", payload.get("market_type"))
            if str(market_type or "").strip().lower() != MarketType.PREDICTION.value:
                continue
            if str(payload.get("successor_relation", "")).strip().upper() == "LEGACY_SCOPE_SUCCESSOR":
                continue
            candidates.append(record)

        output: list[Mapping[str, Any]] = []
        from .legacy_scope import (
            CANONICAL_VALID,
            LEGACY_UNAMBIGUOUS,
            LegacyScopeError,
            classify_legacy_scope,
            create_legacy_successor,
        )

        inspected = 0
        cursor_record: Mapping[str, Any] | None = None
        scan_aborted = False
        recovery_limit = min(_MAX_LEGACY_RECOVERY_ITEMS, self.config.max_items_per_cycle)
        for record in candidates:
            if inspected >= recovery_limit:
                break
            inspected += 1
            prior_cursor_record = cursor_record
            cursor_record = record
            payload = record.get("payload")
            assert isinstance(payload, Mapping)
            document = _legacy_recovery_document(record)
            if document is None:
                continue
            selector_reason = _legacy_dataset_selector_conflict(document)
            assessment = classify_legacy_scope(document)
            if assessment.classification == CANONICAL_VALID and selector_reason is None:
                continue
            candidate_id = assessment.candidate_id or str(
                document.get("candidate_id") or record.get("candidate_id") or ""
            ).strip() or None
            nested_target = _nested_legacy_target_market_ids(document)
            plan_document = document.get("experiment_plan")
            target_document = (
                plan_document.get("target")
                if isinstance(plan_document, Mapping)
                else None
            )
            nested_target_unrecognized = nested_target is None and not (
                isinstance(target_document, Mapping) and not target_document
            )
            if selector_reason is not None:
                classification = "INVALID"
                reason = selector_reason
            elif nested_target_unrecognized:
                classification = "INVALID"
                reason = "MISSING_NESTED_TARGET_MARKET_IDS"
            else:
                classification = assessment.classification
                reason = assessment.reason
            frozen_hash = assessment.frozen_hash
            recovery_key = _legacy_recovery_key(record, candidate_id, frozen_hash)
            evidence: dict[str, Any] = {
                "recovery_key": recovery_key,
                "attempt": "LEGACY_SCOPE_RECOVERY",
                "attempted": True,
                "attempted_at": ensure_utc(now).isoformat(),
                "predecessor_candidate_id": candidate_id,
                "predecessor_frozen_hash": frozen_hash,
                "classification": classification,
                "reason_code": reason,
                "scope_hash": assessment.scope_hash,
                "scope_version": assessment.scope_version,
                "progress": "BLOCKED",
                "blocker": reason,
                "next_action": "RETAIN_LEGACY_PREDECESSOR",
                "successor_id": None,
                "queue_item_id": None,
                "queue_status": None,
                "provenance_state": self._legacy_provenance_state(None, document),
                "paper_only": True,
            }
            successor = None
            if classification == LEGACY_UNAMBIGUOUS and assessment.scope is not None:
                try:
                    # The successor is immutable and plan-validated, but its
                    # exact historical dataset binding must also be present in
                    # the store with a current attestation before enqueue.
                    successor = create_legacy_successor(
                        document,
                        source_candidate_id=candidate_id,
                        source_frozen_hash=frozen_hash,
                    )
                    evidence["provenance_state"] = self._legacy_provenance_state(successor.plan, document)
                    validation = validate_hermes_proposal(successor.proposal)
                    if not validation.accepted:
                        detail = "; ".join(validation.reasons) or "proposal rejected"
                        raise LegacyScopeError("PROPOSAL_REJECTED", detail)
                    self._validate_persisted_dataset_provenance(
                        successor.plan,
                        legacy_document=document,
                    )
                    queued_payload = _normalize_hypothesis_payload(
                        dict(validation.normalized or successor.proposal),
                        source_fallback=str(
                            getattr(self.bus, "author", "")
                            or getattr(self.bus, "source", "")
                            or "hermes"
                        ).strip()
                        or "hermes",
                    )
                    attestation_loader = getattr(self.store, "load_dataset_integrity_attestation", None)
                    attestation_hash = None
                    if callable(attestation_loader) and successor.plan.dataset_id and successor.plan.dataset_version:
                        try:
                            attestation = attestation_loader(
                                successor.plan.dataset_id,
                                successor.plan.dataset_version,
                            )
                        except Exception:
                            attestation = None
                        if isinstance(attestation, Mapping):
                            attestation_hash = str(attestation.get("attestation_hash", "")).strip() or None
                    queued_payload = _mark_generated_queue_payload(
                        queued_payload,
                        kind="legacy_scope_successor",
                        dataset_id=successor.plan.dataset_id,
                        dataset_version=successor.plan.dataset_version,
                        attestation_hash=attestation_hash,
                    )
                    dedupe_key = (
                        f"legacy-successor:{successor.successor_id}:"
                        f"{attestation_hash or 'unattested'}"
                    )
                    # Storage derives queue ids from the versioned dedupe key.
                    # Reuse an existing row so identical attestation retries
                    # cannot replace its immutable payload or queue history.
                    queue_item = self.bus.get(
                        "queue-" + hashlib.sha256(dedupe_key.encode("utf-8")).hexdigest()
                    )
                    if queue_item is None:
                        queue_item = self.bus.submit_hypothesis(
                            queued_payload,
                            dedupe_key=dedupe_key,
                            lineage=(successor.predecessor_candidate_id, successor.predecessor_frozen_hash),
                            available_at=now,
                        )
                    evidence.update(
                        {
                            "classification": successor.assessment.classification,
                            "reason_code": "LEGACY_SCOPE_NORMALIZED",
                            "scope_hash": successor.plan.market_scope_hash,
                            "scope_version": successor.plan.market_scope_version,
                            "progress": "ENQUEUED",
                            "blocker": None,
                            "next_action": "CLAIM_AND_PROCESS_CANONICAL_SUCCESSOR",
                            "successor_id": successor.successor_id,
                            "queue_item_id": queue_item.item_id if queue_item is not None else None,
                            "queue_status": queue_item.status.value if queue_item is not None else None,
                        }
                    )
                    persisted = self._persist_legacy_recovery_evidence(
                        evidence,
                        queue_item=queue_item,
                        now=now,
                    )
                except (AutonomousResearchError, LegacyScopeError, ResearchBusPermissionError, RuntimeError, TypeError, ValueError) as exc:
                    blocker = str(getattr(exc, "reason", "")).strip().upper() or "LEGACY_SCOPE_RECOVERY_FAILED"
                    evidence.update(
                        {
                            "reason_code": blocker,
                            "progress": "BLOCKED",
                            "blocker": blocker,
                            "next_action": "RETAIN_LEGACY_PREDECESSOR",
                            "provenance_state": self._legacy_provenance_state(
                                getattr(successor, "plan", None),
                                document,
                            ),
                        }
                    )
                    persisted = self._persist_legacy_recovery_evidence(evidence, now=now)
            else:
                persisted = self._persist_legacy_recovery_evidence(evidence, now=now)
            evidence["evidence_persisted"] = persisted
            output.append(evidence)
            if not persisted:
                # Do not advance beyond an evidence write that failed.  The
                # next cycle retries this candidate from the durable boundary.
                cursor_record = prior_cursor_record
                scan_aborted = True
                break
        if durable_pagination:
            # Once every prediction row in a fetched page was scanned, the
            # final lifecycle row is the safe boundary—even when intervening
            # rows belong to another market type.  If the page had more than
            # the per-cycle cap, retain the last scanned prediction row so
            # unscanned candidates in this page are not skipped.  An evidence
            # failure similarly retains the prior boundary.
            if not scan_aborted and records and len(candidates) <= recovery_limit:
                progress_record = records[-1]
            else:
                progress_record = cursor_record
            next_cursor: dict[str, str] | None = None
            if isinstance(progress_record, Mapping):
                raw_timestamp = progress_record.get("updated_at")
                if isinstance(raw_timestamp, datetime):
                    timestamp = ensure_utc(raw_timestamp).isoformat()
                else:
                    timestamp = str(raw_timestamp or "").strip()
                candidate_id = str(progress_record.get("candidate_id") or "").strip()
                if timestamp and candidate_id:
                    next_cursor = {
                        "updated_at": timestamp,
                        "candidate_id": candidate_id,
                    }
            next_state = dict(state)
            next_state["schema_version"] = "autonomous-legacy-recovery-v1"
            next_state["cursor"] = next_cursor
            try:
                state_setter(_LEGACY_RECOVERY_STATE_NAME, next_state)
            except (RuntimeError, TypeError, ValueError):
                # Evidence remains durable even if a transient state write
                # fails; retrying the page is safe because report keys dedupe.
                pass
        return tuple(output)
    @staticmethod
    def _next_dataset_job_name(plan_id: str) -> str:
        return _NEXT_DATASET_JOB_PREFIX + str(plan_id).strip()

    @staticmethod
    def _bounded_dataset_catalog(
        catalog: Mapping[str, Any] | None,
        *,
        dataset_id: str = "",
        dataset_version: str = "",
    ) -> dict[str, Any] | None:
        """Project catalog identity and aggregate facts without nested metadata."""
        if not isinstance(catalog, Mapping):
            return None
        summary: dict[str, Any] = {}
        for key in (
            "dataset_id",
            "dataset_version",
            "version",
            "provider",
            "instrument",
            "market_type",
            "timeframe",
            "start_timestamp",
            "end_timestamp",
            "row_count",
            "completeness",
            "quality",
            "source_type",
            "snapshot_id",
        ):
            value = catalog.get(key)
            if value is None:
                continue
            summary[key] = value.isoformat() if isinstance(value, datetime) else _compact_value(value)
        if not str(summary.get("dataset_id", "")).strip() and dataset_id:
            summary["dataset_id"] = dataset_id
        if not str(summary.get("dataset_version", summary.get("version", ""))).strip() and dataset_version:
            summary["dataset_version"] = dataset_version
        missing_ranges = catalog.get("missing_ranges")
        if missing_ranges is not None:
            if isinstance(missing_ranges, (list, tuple)):
                summary["missing_ranges_count"] = len(missing_ranges)
                summary["missing_ranges_digest"] = "sha256:" + hashlib.sha256(
                    _canonical_binding(missing_ranges).encode("utf-8")
                ).hexdigest()
            else:
                summary["missing_ranges"] = bool(missing_ranges)
        return summary

    @staticmethod
    def _bounded_dataset_attestation(
        attestation: Mapping[str, Any] | None,
        *,
        dataset_id: str = "",
        dataset_version: str = "",
    ) -> dict[str, Any] | None:
        """Project immutable attestation facts without constituent bindings."""
        if not isinstance(attestation, Mapping):
            return None
        summary: dict[str, Any] = {}
        for key in (
            "dataset_id",
            "dataset_version",
            "source_type",
            "market_type",
            "row_count",
            "observed_row_count",
            "completeness",
            "start_timestamp",
            "end_timestamp",
            "contamination_result",
            "provenance_version",
            "policy_version",
            "reason",
            "attestation_hash",
            "verified_at",
            "status",
        ):
            value = attestation.get(key)
            if value is None:
                continue
            summary[key] = value.isoformat() if isinstance(value, datetime) else _compact_value(value)
        if not str(summary.get("dataset_id", "")).strip() and dataset_id:
            summary["dataset_id"] = dataset_id
        if not str(summary.get("dataset_version", "")).strip() and dataset_version:
            summary["dataset_version"] = dataset_version

        bindings = attestation.get("constituent_bindings", attestation.get("constituents"))
        count: int | None = None
        for key in ("constituent_count", "constituents_count", "market_count"):
            value = attestation.get(key)
            if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
                count = value
                break
        if count is None and isinstance(bindings, (list, tuple)):
            count = len(bindings)
        if count is not None:
            summary["constituent_count"] = count

        digest: str | None = None
        for key in (
            "canonical_digest",
            "constituent_bindings_digest",
            "constituent_digest",
            "bindings_digest",
        ):
            value = attestation.get(key)
            if value is not None and str(value).strip():
                digest = str(value).strip()
                break
        if digest is None and isinstance(bindings, (list, tuple)):
            digest = "sha256:" + hashlib.sha256(
                _canonical_binding(bindings).encode("utf-8")
            ).hexdigest()
        if digest is not None:
            summary["canonical_digest"] = digest
            summary["constituent_bindings_digest"] = digest
        return summary

    def _report_next_dataset_scheduler_error(
        self,
        exc: BaseException | str,
        now: datetime,
        *,
        plan_id: str | None = None,
        candidate_id: str | None = None,
    ) -> None:
        """Expose successor scheduling failures while allowing queue work to continue."""
        message = str(exc).strip() or type(exc).__name__
        code = type(exc).__name__.upper() if not isinstance(exc, str) else "NEXT_DATASET_JOB_SCHEDULER_ERROR"
        state_loader = getattr(self.store, "get_scheduler_state", None)
        state_setter = getattr(self.store, "set_scheduler_state", None)
        if callable(state_setter):
            try:
                current = state_loader(_NEXT_DATASET_SCHEDULER_STATE_NAME) if callable(state_loader) else None
                state = dict(current) if isinstance(current, Mapping) else {}
                state.update(
                    {
                        "status": "DEGRADED",
                        "last_error": message[:2_000],
                        "last_error_code": code,
                        "last_error_at": ensure_utc(now).isoformat(),
                    }
                )
                if plan_id:
                    state["plan_id"] = plan_id
                if candidate_id:
                    state["candidate_id"] = candidate_id
                state_setter(_NEXT_DATASET_SCHEDULER_STATE_NAME, state)
            except Exception:
                pass
        saver = getattr(self.store, "save_report_if_absent", None)
        if not callable(saver):
            return
        report_key = "|".join((code, message, str(plan_id or ""), str(candidate_id or "")))
        report_id = "autonomous-next-dataset-error-" + hashlib.sha256(report_key.encode("utf-8")).hexdigest()[:24]
        try:
            saver(
                report_id,
                {
                    "report_type": "autonomous_next_dataset_scheduler_error",
                    "blocker": "NEXT_DATASET_JOB_SCHEDULER_ERROR",
                    "reason_code": code,
                    "reason": message[:2_000],
                    "plan_id": plan_id,
                    "candidate_id": candidate_id,
                    "timestamp": ensure_utc(now).isoformat(),
                    "paper_only": True,
                },
                experiment_id=plan_id,
            )
        except Exception:
            pass

    @staticmethod
    def _strategy_contract(plan: ExperimentPlan) -> dict[str, Any]:
        """Return the immutable strategy material independent of data version."""
        document = dict(plan.as_dict())
        document.pop("plan_id", None)
        document.pop("hypothesis_id", None)
        selector = document.pop("dataset_selector", {})
        if isinstance(selector, Mapping):
            document["dataset_selector_contract"] = {
                str(key): value
                for key, value in selector.items()
                if str(key) not in {"dataset_id", "dataset_version", "version"}
            }
        return document

    @classmethod
    def _strategy_contract_digest(cls, plan: ExperimentPlan) -> str:
        return "sha256:" + hashlib.sha256(
            _canonical_binding(cls._strategy_contract(plan)).encode("utf-8")
        ).hexdigest()
    @staticmethod
    def _complete_polymarket_catalog(catalog: Any, *, dataset_id: str, version: str) -> bool:
        if not isinstance(catalog, Mapping):
            return False
        catalog_id = str(catalog.get("dataset_id", "")).strip()
        catalog_version = str(catalog.get("dataset_version", catalog.get("version", ""))).strip()
        if (
            catalog_id != dataset_id
            or catalog_version != version
            or not version
            or version.casefold() in _MUTABLE_DATASET_VERSION_ALIASES
            or str(catalog.get("source_type", "")).strip().upper() != "HISTORICAL"
            or str(catalog.get("market_type", "")).strip().lower() != MarketType.PREDICTION.value
            or str(catalog.get("instrument", "")).strip().upper() != "POLYMARKET"
        ):
            return False
        try:
            completeness = float(catalog.get("completeness", 0.0))
            row_count = catalog.get("row_count", 0)
        except (TypeError, ValueError, OverflowError):
            return False
        return (
            math.isfinite(completeness)
            and completeness >= 1.0
            and isinstance(row_count, int)
            and not isinstance(row_count, bool)
            and row_count > 0
            and not catalog.get("missing_ranges")
        )

    def _load_next_dataset_version(
        self,
        *,
        dataset_id: str,
        rejected_version: str,
    ) -> tuple[Mapping[str, Any], Mapping[str, Any]] | None:
        """Find one newer, exact, attested aggregate without touching crypto."""
        catalog_loader = getattr(self.store, "load_dataset_catalog", None)
        attestation_loader = getattr(self.store, "load_dataset_integrity_attestation", None)
        catalog_lister = getattr(self.store, "list_dataset_catalog", None)
        if not callable(catalog_loader) or not callable(attestation_loader) or not callable(catalog_lister):
            return None
        try:
            catalogs = catalog_lister(
                source_type="HISTORICAL",
                market_type=MarketType.PREDICTION.value,
                limit=256,
            )
        except TypeError:
            try:
                catalogs = catalog_lister(limit=256)
            except Exception:
                return None
        except Exception:
            return None
        if not isinstance(catalogs, Sequence):
            return None
        for listed in catalogs:
            if not isinstance(listed, Mapping):
                continue
            version = str(listed.get("dataset_version", listed.get("version", ""))).strip()
            if version == rejected_version or not version:
                continue
            # Re-load by exact identity rather than trusting a broad catalog
            # projection.  Forward/rolling rows never pass this source gate.
            try:
                catalog = catalog_loader(dataset_id, version)
            except Exception:
                continue
            if not self._complete_polymarket_catalog(catalog, dataset_id=dataset_id, version=version):
                continue
            try:
                attestation = attestation_loader(dataset_id, version)
            except Exception:
                # A crypto provider failure or an attestation read failure is
                # evidence for this job only; it must not stop the scheduler.
                continue
            if not isinstance(attestation, Mapping):
                continue
            if (
                str(attestation.get("dataset_id", "")).strip() != dataset_id
                or str(attestation.get("dataset_version", "")).strip() != version
                or str(attestation.get("status", "")).strip().upper() != "CURRENT"
                or str(attestation.get("contamination_result", "")).strip().upper() != "PASS"
                or not str(attestation.get("attestation_hash", "")).strip()
            ):
                continue
            return dict(catalog), dict(attestation)
        return None

    def _predecessor_proposal(self, plan_id: str) -> Mapping[str, Any] | None:
        lister = getattr(self.store, "list_research_items", None)
        if not callable(lister):
            return None
        try:
            items = lister(limit=4096)
        except Exception:
            return None
        if not isinstance(items, Sequence):
            return None
        for item in items:
            if not isinstance(item, Mapping):
                continue
            payload = item.get("payload")
            if not isinstance(payload, Mapping):
                continue
            item_plan_id = str(payload.get("plan_id", "")).strip()
            nested = payload.get("experiment_plan")
            if not item_plan_id and isinstance(nested, Mapping):
                item_plan_id = str(nested.get("plan_id", "")).strip()
            if item_plan_id == plan_id:
                return dict(payload)
        return None

    def _rejection_plan(
        self,
        item: ResearchQueueItem | Mapping[str, Any] | None,
        result: Mapping[str, Any],
    ) -> tuple[ExperimentPlan, str] | None:
        plan_id = str(result.get("plan_id", "")).strip()
        item_payload = (
            item.payload
            if isinstance(item, ResearchQueueItem)
            else item
            if isinstance(item, Mapping)
            else None
        )
        if not plan_id and isinstance(item_payload, Mapping):
            plan_id = str(item_payload.get("plan_id", "")).strip()
        plan_record = self.store.load_experiment_plan(plan_id) if plan_id else None
        raw_plan = plan_record.get("plan") if isinstance(plan_record, Mapping) else None
        if not isinstance(raw_plan, Mapping) and isinstance(item_payload, Mapping):
            raw_plan = item_payload.get("experiment_plan")
        if not isinstance(raw_plan, Mapping):
            return None
        try:
            plan = ExperimentPlan.from_mapping(
                raw_plan,
                hypothesis_id=(
                    str(plan_record.get("hypothesis_id", "")).strip()
                    if isinstance(plan_record, Mapping)
                    else str(result.get("hypothesis_id", "")).strip()
                ) or None,
            )
        except (ExperimentPlanError, TypeError, ValueError):
            return None
        if plan_id and plan.plan_id != plan_id:
            return None
        expected_hash = str(
            plan_record.get("plan_hash", "")
            if isinstance(plan_record, Mapping)
            else result.get("plan_hash", "")
        ).strip()
        if expected_hash and expected_hash != plan.plan_hash:
            return None
        candidates = result.get("candidate_results")
        candidate_ids = [
            str(candidate.get("candidate_id", "")).strip()
            for candidate in candidates
            if isinstance(candidate, Mapping) and str(candidate.get("candidate_id", "")).strip()
        ] if isinstance(candidates, Sequence) and not isinstance(candidates, (str, bytes)) else []
        if not candidate_ids and isinstance(item_payload, Mapping):
            candidate_id = str(item_payload.get("candidate_id", "")).strip()
            if candidate_id:
                candidate_ids.append(candidate_id)
        candidate_id = candidate_ids[0] if candidate_ids else ""
        if not candidate_id:
            return None
        lifecycle_loader = getattr(self.store, "load_candidate_lifecycle", None)
        if callable(lifecycle_loader):
            lifecycle = lifecycle_loader(candidate_id)
            if not isinstance(lifecycle, Mapping):
                return None
            lifecycle_payload = lifecycle.get("payload")
            if not isinstance(lifecycle_payload, Mapping):
                return None
            if str(lifecycle.get("stage", "")).strip().upper() != CandidateStage.REJECTED.value:
                return None
            relation = str(lifecycle_payload.get("successor_relation", "")).strip().upper()
            generated = _generated_queue_provenance(lifecycle_payload)
            if relation == "LEGACY_SCOPE_SUCCESSOR" or (
                isinstance(generated, Mapping)
                and str(generated.get("kind", "")).strip() == "legacy_scope_successor"
            ):
                return None
        if isinstance(item_payload, Mapping):
            relation = str(item_payload.get("successor_relation", "")).strip().upper()
            generated = _generated_queue_provenance(item_payload)
            if relation == "LEGACY_SCOPE_SUCCESSOR" or (
                isinstance(generated, Mapping)
                and str(generated.get("kind", "")).strip() == "legacy_scope_successor"
            ):
                return None
        if (
            plan.market_type is not MarketType.PREDICTION
            or str(plan.dataset_id or "").strip() != "Polymarket-historical"
            or not str(plan.dataset_version).strip()
            or str(plan.dataset_version).casefold() in _MUTABLE_DATASET_VERSION_ALIASES
        ):
            return None
        return plan, candidate_id


    def _waiting_job_payload(
        self,
        plan: ExperimentPlan,
        *,
        candidate_id: str,
        result: Mapping[str, Any],
        now: datetime,
    ) -> dict[str, Any]:
        dataset_id = str(plan.dataset_id or "").strip()
        rejected_version = str(plan.dataset_version).strip()
        catalog_loader = getattr(self.store, "load_dataset_catalog", None)
        attestation_loader = getattr(self.store, "load_dataset_integrity_attestation", None)
        try:
            catalog = catalog_loader(dataset_id, rejected_version) if callable(catalog_loader) else None
        except Exception:
            catalog = None
        try:
            attestation = attestation_loader(dataset_id, rejected_version) if callable(attestation_loader) else None
        except Exception:
            attestation = None
        contract = self._strategy_contract(plan)
        contract_digest = self._strategy_contract_digest(plan)
        next_check = ensure_utc(now) + timedelta(seconds=60)
        return {
            "schema_version": "autonomous-next-dataset-version-v1",
            "job_kind": "POLYMARKET_DATASET_VERSION_SUCCESSOR",
            "progress": "WAITING_FOR_NEW_DATASET_VERSION",
            "predecessor_plan_id": plan.plan_id,
            "predecessor_plan_hash": plan.plan_hash,
            "predecessor_hypothesis_id": plan.hypothesis_id,
            "predecessor_candidate_id": candidate_id,
            "dataset_id": dataset_id,
            "old_dataset_version": rejected_version,
            "rejected_dataset_version": rejected_version,
            "rejected_result": {
                "reason_code": result.get("reason_code"),
                "blocker": result.get("blocker"),
                "accepted": result.get("accepted"),
            },
            "rejected_dataset_catalog": self._bounded_dataset_catalog(
                catalog,
                dataset_id=dataset_id,
                dataset_version=rejected_version,
            ),
            "rejected_dataset_attestation": self._bounded_dataset_attestation(
                attestation,
                dataset_id=dataset_id,
                dataset_version=rejected_version,
            ),
            "required_predicate": {
                "dataset_id": dataset_id,
                "version_distinct_from": rejected_version,
                "source_type": "HISTORICAL",
                "market_type": MarketType.PREDICTION.value,
                "instrument": "POLYMARKET",
                "catalog": {
                    "exact_identity": True,
                    "completeness_min": 1.0,
                    "row_count_min": 1,
                    "missing_ranges": False,
                },
                "attestation": {
                    "status": "CURRENT",
                    "contamination_result": "PASS",
                },
                "rolling_collection_satisfies": False,
            },
            "strategy_contract": contract,
            "strategy_contract_digest": contract_digest,
            "schedule_trigger": _NEXT_DATASET_JOB_TRIGGER,
            "schedule_cadence": _NEXT_DATASET_JOB_CADENCE,
            "schedule_cadence_seconds": 60,
            "next_check_at": next_check.isoformat(),
            "next_run_at": next_check.isoformat(),
            "last_checked_at": ensure_utc(now).isoformat(),
            "dedupe_namespace": "successor",
            "successor_dedupe_template": "successor:{predecessor_plan_id}:{new_dataset_version}",
            "paper_only": True,
            "research_only": True,
        }

    def _create_waiting_next_dataset_job(
        self,
        plan: ExperimentPlan,
        *,
        candidate_id: str,
        result: Mapping[str, Any],
        now: datetime,
    ) -> Mapping[str, Any] | None:
        """Create one durable waiting job; caller may already hold a tx."""
        job_name = self._next_dataset_job_name(plan.plan_id)
        existing = self.store.get_operator_job(job_name)
        if existing is not None:
            return existing
        payload = self._waiting_job_payload(plan, candidate_id=candidate_id, result=result, now=now)
        self.store.set_operator_job(
            job_name,
            _NEXT_DATASET_JOB_STATUS_WAITING,
            payload,
            resumable=True,
            timestamp=now,
        )
        return self.store.get_operator_job(job_name)

    def _maybe_create_waiting_next_dataset_job(
        self,
        item: ResearchQueueItem,
        result: Mapping[str, Any],
        now: datetime,
    ) -> None:
        if (
            result.get("accepted") is not False
            or str(result.get("blocker", "")).strip().upper() != "NO_SUPPORTED_EDGE"
        ):
            return
        try:
            resolved = self._rejection_plan(item, result)
            if resolved is None:
                return
            plan, candidate_id = resolved
            self._create_waiting_next_dataset_job(
                plan,
                candidate_id=candidate_id,
                result=result,
                now=now,
            )
        except Exception as exc:
            # The queue result and rejection remain authoritative if job
            # creation cannot be persisted in this tick, but the scheduler
            # failure must remain visible to operators.
            self._report_next_dataset_scheduler_error(
                exc,
                now,
                plan_id=str(result.get("plan_id", "")).strip() or None,
                candidate_id=str(result.get("candidate_id", "")).strip() or None,
            )

    def _build_dataset_successor(
        self,
        plan: ExperimentPlan,
        *,
        candidate_id: str,
        catalog: Mapping[str, Any],
        attestation: Mapping[str, Any],
        job_payload: Mapping[str, Any],
    ) -> tuple[ExperimentPlan, dict[str, Any], str]:
        version = str(catalog.get("dataset_version", catalog.get("version", ""))).strip()
        old_hash = str(job_payload.get("predecessor_plan_hash", plan.plan_hash)).strip()
        identity = {
            "schema": "axiom-polymarket-dataset-successor-v1",
            "predecessor_plan_id": plan.plan_id,
            "predecessor_plan_hash": old_hash,
            "predecessor_hypothesis_id": plan.hypothesis_id,
            "predecessor_candidate_id": candidate_id,
            "dataset_id": str(plan.dataset_id or "").strip(),
            "dataset_version": version,
        }
        token = hashlib.sha256(_canonical_binding(identity).encode("utf-8")).hexdigest()[:24]
        hypothesis_id = "hypothesis-successor-" + token
        plan_id = "plan-successor-" + token
        plan_document = plan.as_dict()
        plan_document["plan_id"] = plan_id
        plan_document["hypothesis_id"] = hypothesis_id
        selector = dict(plan_document.get("dataset_selector", {}))
        selector["dataset_id"] = str(plan.dataset_id or "").strip()
        selector["dataset_version"] = version
        plan_document["dataset_selector"] = selector
        successor = ExperimentPlan.from_mapping(plan_document, hypothesis_id=hypothesis_id)
        contract_digest = self._strategy_contract_digest(successor)
        if contract_digest != str(job_payload.get("strategy_contract_digest", "")).strip():
            raise AutonomousResearchError(
                "STRATEGY_CONTRACT_CHANGED",
                "successor dataset version changed the immutable strategy contract",
            )
        source = self._predecessor_proposal(plan.plan_id) or {}
        attestation_payload = self._bounded_dataset_attestation(
            attestation,
            dataset_id=str(plan.dataset_id or "").strip(),
            dataset_version=version,
        ) or {}
        proposal = dict(successor.as_dict())
        for name, fallback in (
            ("statement", "Re-evaluate the immutable Polymarket strategy on a new historical dataset version."),
            ("source", "axiom-autonomous-dataset-successor"),
            ("tests", ["bounded chronological backtest and validation"]),
            ("time_split", plan.methodology.get("time_split", "train-validation-holdout")),
        ):
            value = source.get(name, fallback)
            if name == "tests" and (
                not isinstance(value, (list, tuple))
                or not value
                or any(not isinstance(test, str) or not test.strip() for test in value)
            ):
                value = fallback
            proposal[name] = value
        proposal.update(
            {
                "proposal_id": hypothesis_id,
                "hypothesis_id": hypothesis_id,
                "dataset_id": successor.dataset_id,
                "dataset_version": successor.dataset_version,
                "market_type": successor.market_type.value,
                "experiment_plan": successor.as_dict(),
                "dataset_attestation": dict(attestation_payload),
                "strategy_contract_digest": contract_digest,
                "successor_relation": "POLYMARKET_DATASET_VERSION_SUCCESSOR",
                "dataset_successor_predecessor": {
                    "plan_id": plan.plan_id,
                    "plan_hash": plan.plan_hash,
                    "candidate_id": candidate_id,
                },
                "paper_only": True,
            }
        )
        validation = validate_hermes_proposal(proposal, store=self.store)
        if not validation.accepted:
            detail = "; ".join(validation.reasons) or "successor proposal rejected"
            raise AutonomousResearchError("SUCCESSOR_PROPOSAL_INVALID", detail)
        normalized = dict(validation.normalized or proposal)
        validated_plan = ExperimentPlan.from_proposal(normalized)
        if (
            validated_plan.plan_id != plan_id
            or validated_plan.hypothesis_id != hypothesis_id
            or validated_plan.dataset_version != version
            or validated_plan.plan_hash != successor.plan_hash
        ):
            raise AutonomousResearchError(
                "SUCCESSOR_PROPOSAL_INVALID",
                "validated successor identity or dataset binding changed",
            )
        queued_payload = _normalize_hypothesis_payload(
            normalized,
            source_fallback=str(getattr(self.bus, "author", "") or "hermes"),
        )
        queued_payload = _mark_generated_queue_payload(
            queued_payload,
            kind=_NEXT_DATASET_GENERATED_KIND,
            dataset_id=validated_plan.dataset_id,
            dataset_version=validated_plan.dataset_version,
            attestation_hash=str(attestation.get("attestation_hash", "")).strip() or None,
        )
        dedupe_key = f"successor:{plan.plan_id}:{version}"
        return validated_plan, queued_payload, dedupe_key

    @staticmethod
    def _next_dataset_job_is_due(job: Mapping[str, Any], now: datetime) -> bool:
        """Return whether a persisted successor job may be polled now."""
        if str(job.get("status", "")).strip().upper() != _NEXT_DATASET_JOB_STATUS_WAITING:
            return True
        payload = job.get("payload")
        if not isinstance(payload, Mapping):
            return True
        next_check = parse_timestamp(payload.get("next_check_at"))
        # Jobs written before the cadence field existed (or with a malformed
        # value) remain immediately eligible rather than becoming stranded.
        return next_check is None or ensure_utc(now) >= next_check

    def _advance_waiting_next_dataset_job(self, job: Mapping[str, Any], now: datetime) -> None:
        if not self._next_dataset_job_is_due(job, now):
            return
        job_name = str(job.get("job_name", "")).strip()
        if not job_name:
            return
        payload = job.get("payload")
        payload = dict(payload) if isinstance(payload, Mapping) else {}
        status = str(job.get("status", "")).strip().upper()
        if status == _NEXT_DATASET_JOB_STATUS_ENQUEUED:
            queue_id = str(payload.get("queue_item_id", "")).strip()
            queued = self.bus.get(queue_id) if queue_id else None
            if queued is None or queued.status.value not in {"COMPLETED", "ACCEPTED", "REJECTED", "FAILED"}:
                return
            payload.update(
                {
                    "progress": "COMPLETED" if queued.status.value in {"COMPLETED", "ACCEPTED"} else "FAILED",
                    "queue_status": queued.status.value,
                    "successor_result": dict(queued.result) if isinstance(queued.result, Mapping) else None,
                    "last_checked_at": ensure_utc(now).isoformat(),
                }
            )
            self.store.set_operator_job(
                job_name,
                _NEXT_DATASET_JOB_STATUS_COMPLETED
                if queued.status.value in {"COMPLETED", "ACCEPTED"}
                else _NEXT_DATASET_JOB_STATUS_FAILED,
                payload,
                resumable=False,
                timestamp=now,
            )
            return
        if status != _NEXT_DATASET_JOB_STATUS_WAITING:
            return
        dataset_id = str(payload.get("dataset_id", "Polymarket-historical")).strip()
        old_version = str(
            payload.get("rejected_dataset_version", payload.get("old_dataset_version", ""))
        ).strip()
        plan_id = str(payload.get("predecessor_plan_id", "")).strip()
        plan_record = self.store.load_experiment_plan(plan_id) if plan_id else None
        raw_plan = plan_record.get("plan") if isinstance(plan_record, Mapping) else payload.get("predecessor_plan")
        if not isinstance(raw_plan, Mapping):
            return
        try:
            plan = ExperimentPlan.from_mapping(
                raw_plan,
                hypothesis_id=str(
                    (plan_record or {}).get("hypothesis_id", payload.get("predecessor_hypothesis_id", ""))
                ).strip() or None,
            )
        except (ExperimentPlanError, TypeError, ValueError):
            return
        if (
            plan.plan_id != plan_id
            or str(plan.dataset_id or "").strip() != dataset_id
            or str(plan.dataset_version).strip() != old_version
        ):
            return
        found = self._load_next_dataset_version(dataset_id=dataset_id, rejected_version=old_version)
        next_check = ensure_utc(now) + timedelta(seconds=60)
        if found is None:
            payload.update(
                {
                    "progress": "WAITING_FOR_NEW_DATASET_VERSION",
                    "last_checked_at": ensure_utc(now).isoformat(),
                    "next_check_at": next_check.isoformat(),
                    "next_run_at": next_check.isoformat(),
                }
            )
            self.store.set_operator_job(
                job_name,
                _NEXT_DATASET_JOB_STATUS_WAITING,
                payload,
                resumable=True,
                timestamp=now,
            )
            return
        catalog, attestation = found
        candidate_id = str(payload.get("predecessor_candidate_id", "")).strip()
        try:
            successor, queued_payload, dedupe_key = self._build_dataset_successor(
                plan,
                candidate_id=candidate_id,
                catalog=catalog,
                attestation=attestation,
                job_payload=payload,
            )
            self.store.save_experiment_plan(
                successor.plan_id,
                successor.as_dict(),
                hypothesis_id=successor.hypothesis_id,
                plan_hash=successor.plan_hash,
                status="PENDING",
                timestamp=now,
            )
            queue_id = "queue-" + hashlib.sha256(dedupe_key.encode("utf-8")).hexdigest()
            queued = self.bus.get(queue_id)
            if queued is None:
                queued = self.bus.submit_proposal(
                    queued_payload,
                    dedupe_key=dedupe_key,
                    lineage=(plan.plan_id, plan.plan_hash, candidate_id),
                    available_at=now,
                )
            payload.update(
                {
                    "progress": "ENQUEUED",
                    "successor_plan_id": successor.plan_id,
                    "successor_plan_hash": successor.plan_hash,
                    "successor_hypothesis_id": successor.hypothesis_id,
                    "successor_dataset_version": successor.dataset_version,
                    "successor_attestation_hash": attestation.get("attestation_hash"),
                    "queue_item_id": queued.item_id,
                    "queue_status": queued.status.value,
                    "dedupe_key": dedupe_key,
                    "next_check_at": None,
                    "next_run_at": now.isoformat(),
                    "last_checked_at": ensure_utc(now).isoformat(),
                }
            )
            self.store.set_operator_job(
                job_name,
                _NEXT_DATASET_JOB_STATUS_ENQUEUED,
                payload,
                resumable=True,
                timestamp=now,
            )
        except (AutonomousResearchError, ResearchBusPermissionError, ExperimentPlanError, RuntimeError, TypeError, ValueError) as exc:
            payload.update(
                {
                    "progress": "WAITING_FOR_NEW_DATASET_VERSION",
                    "last_error": str(exc),
                    "last_checked_at": ensure_utc(now).isoformat(),
                    "next_check_at": next_check.isoformat(),
                    "next_run_at": next_check.isoformat(),
                }
            )
            self.store.set_operator_job(
                job_name,
                _NEXT_DATASET_JOB_STATUS_WAITING,
                payload,
                last_error=str(exc),
                resumable=True,
                timestamp=now,
            )

    def _poll_waiting_next_dataset_jobs(self, now: datetime) -> None:
        lister = getattr(self.store, "list_operator_jobs", None)
        if callable(lister):
            try:
                jobs = lister()
            except Exception as exc:
                self._report_next_dataset_scheduler_error(exc, now)
                jobs = ()
            if isinstance(jobs, Sequence):
                for job in jobs:
                    if not isinstance(job, Mapping):
                        continue
                    name = str(job.get("job_name", "")).strip()
                    if not name.startswith(_NEXT_DATASET_JOB_PREFIX):
                        continue
                    try:
                        with self.store.transaction():
                            current = self.store.get_operator_job(name)
                            if isinstance(current, Mapping) and self._next_dataset_job_is_due(current, now):
                                self._advance_waiting_next_dataset_job(current, now)
                    except Exception as exc:
                        self._report_next_dataset_scheduler_error(exc, now)

    def _backfill_next_dataset_job(self, now: datetime) -> None:
        lister = getattr(self.store, "list_experiment_plans", None)
        if not callable(lister):
            return
        try:
            plans = lister(limit=256, newest_first=True)
        except TypeError:
            try:
                plans = lister(limit=256)
            except Exception as exc:
                self._report_next_dataset_scheduler_error(exc, now)
                return
        except Exception as exc:
            self._report_next_dataset_scheduler_error(exc, now)
            return
        if not isinstance(plans, Sequence):
            return
        for record in plans:
            if not isinstance(record, Mapping):
                continue
            result = record.get("result")
            if not isinstance(result, Mapping):
                continue
            if (
                result.get("accepted") is not False
                or str(result.get("blocker", "")).strip().upper() != "NO_SUPPORTED_EDGE"
            ):
                continue
            plan_id = str(record.get("plan_id", "")).strip()
            raw_plan = record.get("plan")
            if not plan_id or not isinstance(raw_plan, Mapping):
                continue
            item: ResearchQueueItem | Mapping[str, Any] | None = self._predecessor_proposal(plan_id)
            try:
                resolved = self._rejection_plan(item, result)
            except Exception as exc:
                self._report_next_dataset_scheduler_error(exc, now, plan_id=plan_id)
                continue
            if resolved is None:
                continue
            plan, candidate_id = resolved
            job_name = self._next_dataset_job_name(plan.plan_id)
            try:
                with self.store.transaction():
                    if self.store.get_operator_job(job_name) is not None:
                        continue
                    self._create_waiting_next_dataset_job(
                        plan,
                        candidate_id=candidate_id,
                        result=result,
                        now=now,
                    )
                return
            except Exception as exc:
                self._report_next_dataset_scheduler_error(
                    exc,
                    now,
                    plan_id=plan.plan_id,
                    candidate_id=candidate_id,
                )
                continue

    def _schedule_next_dataset_jobs(self, now: datetime) -> None:
        """Poll resumable successors, then backfill only the latest rejection."""
        self._poll_waiting_next_dataset_jobs(now)
        self._backfill_next_dataset_job(now)

    def process_pending(self, *, worker: str = "research-queue", now: datetime | None = None) -> AutonomousQueueCycle:
        """Claim at most the configured bounded number of items.

        Candidate, plan, budget, and queue completion writes share one SQLite
        transaction.  A crash before commit therefore leaves the lease to
        expire and retries the same deterministic identifiers without creating
        duplicate experiments or lifecycle transitions.
        """
        current = ensure_utc(now or self.clock())
        legacy_recovery = self._recover_legacy_predecessors(current)
        self._enqueue_predeclared_from_persisted_scope(current)
        self._schedule_next_dataset_jobs(current)
        released = self.bus.resume_expired(now=current)
        results: list[Mapping[str, Any]] = []
        claimed = 0
        completed = 0
        rejected = 0
        failed = 0
        for _ in range(self.config.max_items_per_cycle):
            item = self.bus.claim(worker, lease_seconds=self.config.lease_seconds, now=current)
            if item is None:
                break
            claimed += 1
            phase_index = 0

            def phase_event(stage: str, detail: Any | None = None) -> None:
                nonlocal phase_index
                phase_index += 1
                self.store.record_research_queue_event(
                    item.item_id,
                    stage,
                    detail,
                    timestamp=current + timedelta(microseconds=phase_index),
                )
            try:
                with self.store.transaction():
                    phase_event("CLAIM", {"worker": worker, "item_type": item.item_type})
                    phase_event("VALIDATE", {"item_type": item.item_type})
                    result = self._process_item(item, current)
                    result = _bounded_queue_result(result)
                    try:
                        self._advance_campaign_after_result(item, result, current)
                    except (RuntimeError, TypeError, ValueError, KeyError) as campaign_error:
                        campaign_id = str(item.payload.get("campaign_id", "")).strip()
                        if campaign_id:
                            campaign_job = self.campaign_job_name(campaign_id)
                            campaign_record = self.store.get_operator_job(campaign_job)
                            campaign_payload = dict(campaign_record.get("payload") or {}) if isinstance(campaign_record, Mapping) else {}
                            campaign_payload["status"] = "SOFTWARE_OR_INPUT_ERROR"
                            campaign_payload["last_result"] = {
                                "trial_id": item.payload.get("campaign_trial_id"),
                                "status": "SOFTWARE_OR_INPUT_ERROR",
                                "reason": str(campaign_error),
                            }
                            self.store.set_operator_job(
                                campaign_job,
                                "SOFTWARE_OR_INPUT_ERROR",
                                campaign_payload,
                                resumable=False,
                                timestamp=current,
                            )
                    phase_event(
                        "ACCEPT" if result.get("accepted") is not False else "REJECT",
                        {"reason_code": result.get("reason_code")},
                    )
                    for stage in ("BOUNDED_EXPERIMENT", "TEST", "RESULT", "LIFECYCLE"):
                        phase_event(stage, {"item_type": item.item_type})
                    accepted = result.get("accepted") is not False
                    if not accepted:
                        self._maybe_create_waiting_next_dataset_job(item, result, current)
                    phase_event(
                        "COMPLETE",
                        {"item_type": item.item_type, "accepted": accepted},
                    )
                    self.bus.complete(
                        item.item_id,
                        status=ResearchQueueStatus.COMPLETED if accepted else ResearchQueueStatus.REJECTED,
                        result=result,
                        error=None if accepted else str(result.get("reason") or result.get("status") or "rejected"),
                        worker=worker,
                        now=current + timedelta(microseconds=phase_index + 1),
                    )
                if accepted:
                    completed += 1
                else:
                    rejected += 1
                results.append(result)
            except AutonomousResearchError as exc:
                result = _bounded_queue_result(
                    {
                        "accepted": False,
                        "reason_code": exc.reason,
                        "reason": exc.detail,
                        "item_type": item.item_type,
                        "item_id": item.item_id,
                        "paper_only": True,
                    }
                )
                try:
                    self._advance_campaign_after_result(item, result, current)
                except (RuntimeError, TypeError, ValueError, KeyError):
                    pass
                try:
                    with self.store.transaction():
                        phase_event(
                            "REJECT",
                            {"reason_code": exc.reason, "item_type": item.item_type},
                        )
                        self.bus.complete(
                            item.item_id,
                            status=ResearchQueueStatus.REJECTED,
                            result=result,
                            error=str(exc),
                            worker=worker,
                            now=current + timedelta(microseconds=phase_index + 1),
                        )
                    rejected += 1
                    results.append(result)
                except RuntimeError:
                    # The lease may have expired while an operator paused the
                    # process.  The storage lease owner remains authoritative;
                    # a later cycle will release and reclaim it safely.
                    continue
            except Exception as exc:
                result = _bounded_queue_result(
                    {
                        "accepted": False,
                        "reason_code": "PROCESSING_FAILED",
                        "reason": str(exc),
                        "item_type": item.item_type,
                        "item_id": item.item_id,
                        "paper_only": True,
                    }
                )
                try:
                    self._advance_campaign_after_result(item, result, current)
                except (RuntimeError, TypeError, ValueError, KeyError):
                    pass
                try:
                    with self.store.transaction():
                        phase_event(
                            "FAILED",
                            {"reason_code": "PROCESSING_FAILED", "item_type": item.item_type},
                        )
                        self.bus.complete(
                            item.item_id,
                            status=ResearchQueueStatus.FAILED,
                            result=result,
                            error=str(exc),
                            worker=worker,
                            now=current + timedelta(microseconds=phase_index + 1),
                        )
                    failed += 1
                    results.append(result)
                except RuntimeError:
                    continue
        return AutonomousQueueCycle(
            released,
            claimed,
            completed,
            rejected,
            failed,
            tuple(results),
            tuple(legacy_recovery),
        )

    def _materialized_observation_binding(
        self,
        candidate_id: str,
        payload: Mapping[str, Any],
    ) -> tuple[Any, ExperimentPlan, Mapping[str, Any]] | None:
        """Resolve a schema intent only when every immutable binding agrees."""
        if not bool(payload.get("paper_observation_intent")):
            return None
        intent_id = str(payload.get("paper_observation_intent_id", "")).strip()
        if not intent_id:
            return None
        plan_id = str(payload.get("plan_id", "")).strip()
        plan_record = self.store.load_experiment_plan(plan_id) if plan_id else None
        raw_plan = plan_record.get("plan") if isinstance(plan_record, Mapping) else None
        if not isinstance(raw_plan, Mapping):
            return None
        try:
            plan = ExperimentPlan.from_mapping(
                raw_plan,
                hypothesis_id=str(payload.get("hypothesis_id", "")).strip() or None,
            )
            if plan.market_type is not MarketType.PREDICTION:
                return None
            if str(payload.get("plan_hash", "")).strip() != plan.plan_hash:
                return None
            self._validate_persisted_dataset_provenance(plan)
            attestation_loader = getattr(self.store, "load_dataset_integrity_attestation", None)
            attestation = attestation_loader(plan.dataset_id, plan.dataset_version) if callable(attestation_loader) else None
            if not isinstance(attestation, Mapping):
                return None
        except (AutonomousResearchError, ExperimentPlanError, TypeError, ValueError, RuntimeError):
            return None

        registry = ForwardTestRegistry(self.store)
        expected_intent_id = "observation-intent-" + candidate_id
        if intent_id != expected_intent_id:
            return None
        intent = registry.get(intent_id)
        if intent is None or str(intent.experiment_id) != expected_intent_id:
            return None
        intent_config = intent.config if isinstance(intent.config, Mapping) else {}
        if (
            intent_config.get("observation_intent") is not True
            or intent_config.get("market_authority_required") is not False
            or str(intent_config.get("candidate_id", "")).strip() != candidate_id
            or str(intent_config.get("plan_id", "")).strip() != plan.plan_id
            or str(intent_config.get("plan_hash", "")).strip() != plan.plan_hash
            or intent.allowed_markets
        ):
            return None
        spec = registry.get("forward-" + candidate_id)
        if spec is None or str(spec.experiment_id) != "forward-" + candidate_id:
            return None
        config = spec.config if isinstance(spec.config, Mapping) else {}
        if (
            str(config.get("candidate_id", "")).strip() != candidate_id
            or str(config.get("plan_id", "")).strip() != plan.plan_id
            or str(config.get("plan_hash", "")).strip() != plan.plan_hash
            or config.get("observation_intent") is not True
            or config.get("market_authority_required") is not True
            or not spec.allowed_markets
            or len(spec.allowed_markets) > PAPER_MARKET_AUTHORITY_CAP
        ):
            return None
        expected_scope = _scope_binding(plan)
        for name, expected in expected_scope.items():
            if _canonical_binding(intent_config.get(name)) != _canonical_binding(expected):
                return None
            if _canonical_binding(config.get(name)) != _canonical_binding(expected):
                return None
            if _canonical_binding(payload.get(name)) != _canonical_binding(expected):
                return None
        for name, expected in (
            ("dataset_id", plan.dataset_id),
            ("dataset_version", plan.dataset_version),
        ):
            if _canonical_binding(intent_config.get(name)) != _canonical_binding(expected):
                return None
            if _canonical_binding(config.get(name)) != _canonical_binding(expected):
                return None
            if _canonical_binding(payload.get(name)) != _canonical_binding(expected):
                return None
        candidate_strategy = payload.get("strategy")
        forward_strategy = config.get("strategy_document")
        candidate_model = plan.model_for() or {"type": "deterministic"}
        forward_model = config.get("model_document")
        if not isinstance(candidate_strategy, Mapping) or not isinstance(forward_strategy, Mapping):
            return None
        candidate_strategy = dict(candidate_strategy)
        forward_strategy = dict(forward_strategy)
        candidate_strategy.pop("strategy_id", None)
        forward_strategy.pop("strategy_id", None)
        if _canonical_binding(candidate_strategy) != _canonical_binding(forward_strategy):
            return None
        intent_strategy = intent_config.get("strategy_document")
        intent_model = intent_config.get("model_document")
        if not isinstance(intent_strategy, Mapping) or not isinstance(intent_model, Mapping):
            return None
        intent_strategy = dict(intent_strategy)
        intent_strategy.pop("strategy_id", None)
        if _canonical_binding(candidate_strategy) != _canonical_binding(intent_strategy):
            return None
        if not isinstance(forward_model, Mapping) or _canonical_binding(candidate_model) != _canonical_binding(forward_model):
            return None
        if _canonical_binding(candidate_model) != _canonical_binding(intent_model):
            return None
        resolution_loader = getattr(self.store, "load_market_scope_resolution", None)
        resolution = resolution_loader(candidate_id) if callable(resolution_loader) else None
        if resolution is None:
            return None
        resolution_document = resolution if isinstance(resolution, Mapping) else {}
        status = str(
            resolution_document.get("status", getattr(resolution, "status", ""))
        ).strip().upper()
        resolved_hash = str(
            resolution_document.get("scope_hash", getattr(resolution, "scope_hash", ""))
        ).strip()
        resolved_version = str(
            resolution_document.get("scope_version", getattr(resolution, "scope_version", ""))
        ).strip()
        matched = resolution_document.get("matched_markets", getattr(resolution, "matched_markets", ()))
        matched_ids = tuple(
            str(
                item.get("market_id", "")
                if isinstance(item, Mapping)
                else getattr(item, "market_id", "")
            ).strip()
            for item in matched
            if str(
                item.get("market_id", "")
                if isinstance(item, Mapping)
                else getattr(item, "market_id", "")
            ).strip()
        )
        if (
            status != "MATCHED"
            or resolved_hash != plan.market_scope_hash
            or resolved_version != plan.market_scope_version
            or matched_ids != tuple(spec.allowed_markets)
        ):
            return None
        existing_attestation = payload.get("dataset_attestation")
        if existing_attestation is not None and _canonical_binding(existing_attestation) != _canonical_binding(attestation):
            return None
        config_attestation = config.get("dataset_attestation")
        if config_attestation is not None and _canonical_binding(config_attestation) != _canonical_binding(attestation):
            return None
        return spec, plan, dict(attestation)

    def _reassess_schema_observation_candidate(
        self,
        record: Mapping[str, Any],
        now: datetime,
    ) -> Mapping[str, Any] | None:
        candidate_id = str(record.get("candidate_id", "")).strip()
        payload = record.get("payload")
        if not candidate_id or not isinstance(payload, Mapping):
            return None
        bound = self._materialized_observation_binding(candidate_id, payload)
        if bound is None:
            return None
        spec, _, attestation = bound
        observations = self.store.list_paper_observations(
            spec.experiment_id,
            limit=_MAX_FORWARD_ROWS,
        )
        execution_events = self.store.list_paper_execution_events(
            spec.experiment_id,
            limit=_MAX_FORWARD_ROWS,
        )
        ledgers = self.store.list_paper_bet_ledger(spec.experiment_id, limit=_MAX_FORWARD_ROWS)
        if not observations and not execution_events and not ledgers:
            return None
        reassessment_record = {
            "candidate_id": candidate_id,
            "stage": CandidateStage.PAPER_FORWARD.value,
            "payload": {**dict(payload), "forward_test_id": spec.experiment_id},
        }
        # Build evidence before comparing identities.  Forward evidence
        # materialization may persist a resolved-bet ledger, and that ledger
        # is part of the digest.  Hashing the pre-materialization rows would
        # make the first reassessment look changed again on the next cycle.
        evidence = self._forward_evidence(reassessment_record, now)
        evidence_identity = str(evidence.get("forward_evidence_identity", "")).strip()
        if not evidence_identity:
            return None
        if str(payload.get("forward_evidence_identity", "")).strip() == evidence_identity:
            return None
        attempts = payload.get("automatic_reassessment_attempts", 0)
        if isinstance(attempts, bool) or not isinstance(attempts, int) or attempts < 0:
            attempts = 0
        if attempts >= _MAX_AUTOMATIC_REASSESSMENTS:
            return None
        body = dict(payload)
        body.update(
            {
                "paper_forward_started": True,
                "forward_test_id": spec.experiment_id,
                "forward_config": dict(spec.config),
                "registration_timestamp": spec.registration_timestamp.isoformat(),
                "forward_evidence": _compact_evidence(evidence),
                "forward_evidence_identity": evidence_identity,
                "dataset_attestation": dict(attestation),
                "historical_qualification": "PENDING",
                "automatic_reassessment_attempts": attempts + 1,
                "last_automatic_reassessment_at": ensure_utc(now).isoformat(),
                "paper_only": True,
                "research_only": True,
            }
        )
        committed = self.store.save_candidate_lifecycle(
            candidate_id,
            CandidateStage.PAPER_FORWARD.value,
            body,
            from_stage=CandidateStage.SCHEMA_VALIDATED.value,
            reason="materialized forward evidence reassessed through canonical paper pipeline",
            timestamp=now,
        )
        if not committed:
            return None
        result = self.lifecycle.get(candidate_id)
        if result is None:
            return None
        self.lifecycle._schedule_readiness_snapshot_stale("LIFECYCLE_ADVANCED")
        result = self._evaluate_forward_candidate(candidate_id, now) or result
        return {
            "candidate_id": candidate_id,
            "stage": result.stage.value,
            "reassessed": True,
            "forward_evidence": _compact_evidence(evidence),
            "evidence_identity": evidence_identity,
            "promotion_reasons": list(self.config.promotion_criteria.evaluate({**body, **evidence})),
        }

    def reevaluate_forward_candidates(self, *, now: datetime | None = None) -> tuple[Mapping[str, Any], ...]:
        """Update active forward evidence and apply configured promotion gates."""
        current = ensure_utc(now or self.clock())
        output: list[Mapping[str, Any]] = []
        records = self.store.load_candidate_lifecycle(limit=10_000)
        if not isinstance(records, list):
            return ()
        for record in records:
            if not isinstance(record, Mapping):
                continue
            stage = str(record.get("stage", "")).strip()
            if stage == CandidateStage.SCHEMA_VALIDATED.value:
                candidate_id = str(record.get("candidate_id", "")).strip()
                if not candidate_id:
                    continue
                try:
                    with self.store.transaction():
                        reassessed = self._reassess_schema_observation_candidate(record, current)
                    if reassessed is not None:
                        output.append(reassessed)
                except (AttributeError, KeyError, RuntimeError, TypeError, ValueError):
                    continue
                continue
            if stage != CandidateStage.PAPER_FORWARD.value:
                continue
            candidate_id = str(record.get("candidate_id", "")).strip()
            if not candidate_id:
                continue
            try:
                with self.store.transaction():
                    evidence = self._forward_evidence(record, current)
                    candidate = self.lifecycle.get(candidate_id)
                    if candidate is None:
                        continue
                    existing = dict(candidate.payload)
                    previous_identity = str(existing.get("forward_evidence_identity", "")).strip()
                    current_identity = str(evidence.get("forward_evidence_identity", "")).strip()
                    changed = (
                        previous_identity != current_identity
                        if previous_identity and current_identity
                        else any(existing.get(key) != value for key, value in evidence.items())
                    )
                    if changed:
                        candidate = self.lifecycle.record_evidence(
                            candidate_id,
                            evidence,
                            expected_stage=CandidateStage.PAPER_FORWARD,
                            reason="forward paper evidence update",
                        )
                    reasons = self.config.promotion_criteria.evaluate({**existing, **evidence})
                    hard_reasons = self.config.promotion_criteria.hard_rejection_reasons(evidence)
                    if hard_reasons:
                        candidate = self.lifecycle.reject(
                            candidate_id,
                            hard_reasons[0],
                            evidence=evidence,
                            expected_stage=CandidateStage.PAPER_FORWARD,
                            expected_payload=candidate.payload,
                        )
                    elif not reasons:
                        candidate = self.lifecycle.advance(
                            candidate_id,
                            CandidateStage.PAPER_PROMOTABLE,
                            {**evidence, "holdout_used": False},
                            reason="paper-forward criteria passed; human review required",
                        )
                    output.append(
                        {
                            "candidate_id": candidate_id,
                            "stage": candidate.stage.value,
                            "forward_evidence": _compact_evidence(evidence),
                            "promotion_reasons": list(dict.fromkeys((*reasons, *hard_reasons))),
                        }
                    )
            except (KeyError, RuntimeError, ValueError) as exc:
                output.append({"candidate_id": candidate_id, "stage": "PAPER_FORWARD", "error": str(exc)})
        return tuple(output)

    def _process_item(self, item: ResearchQueueItem, now: datetime) -> Mapping[str, Any]:
        handlers = {
            "hypothesis": self._process_hypothesis,
            "candidate": self._process_candidate,
            "report": self._process_report,
            "review_request": self._process_review_request,
            "experiment_result": self._process_experiment_result,
        }
        handler = handlers.get(str(item.item_type).strip().lower())
        if handler is None:
            raise AutonomousResearchError("UNSUPPORTED_ITEM_TYPE", f"unsupported research item type {item.item_type!r}")
        return handler(item, now)

    def _process_hypothesis(self, item: ResearchQueueItem, now: datetime) -> Mapping[str, Any]:
        proposal = _normalize_hypothesis_payload(item.payload, item)
        generated_scope_binding: tuple[str | None, str | None, str | None, str | None, str | None, str | None] | None = None
        generated_provenance = _generated_queue_provenance(proposal)
        generated_kind_hint = bool(proposal.get("predeclared_starting_set")) or (
            str(proposal.get("successor_relation", "")).strip().upper()
            in {"LEGACY_SCOPE_SUCCESSOR", "POLYMARKET_DATASET_VERSION_SUCCESSOR"}
        )
        if generated_provenance is None and generated_kind_hint:
            raise AutonomousResearchError(
                "GENERATED_PROVENANCE_INVALID",
                "generated queue item is missing its internal provenance marker",
            )
        if generated_provenance is not None:
            try:
                original_plan = ExperimentPlan.from_proposal(proposal)
            except ExperimentPlanError as exc:
                raise AutonomousResearchError(exc.reason, exc.detail) from exc
            # This is deliberately the first scope-dependent operation.  A
            # generated successor must reject a rotated attestation before
            # legacy scope classification, freezing, or lifecycle resolution
            # can produce a less authoritative forward-market reason.
            self._revalidate_generated_queue_item(item, original_plan, proposal)
            if any(
                key in proposal
                for key in ("predecessor_candidate_id", "predecessor_frozen_hash", "frozen_hash")
            ):
                generated_scope_binding = (
                    _binding_value(proposal.get("predecessor_candidate_id")),
                    _binding_value(
                        proposal.get("predecessor_frozen_hash", proposal.get("frozen_hash"))
                    ),
                    _binding_value(original_plan.market_scope_hash),
                    _binding_value(original_plan.market_scope_version),
                    _binding_value(original_plan.dataset_id),
                    _binding_value(original_plan.dataset_version),
                )
        scope_declared = "market_scope" in proposal or any(
            key in proposal
            for key in ("predecessor_candidate_id", "predecessor_frozen_hash", "frozen_hash")
        )
        if scope_declared:
            from .legacy_scope import (
                CANONICAL_VALID,
                LEGACY_UNAMBIGUOUS,
                LegacyScopeError,
                freeze_canonical_scope_proposal,
                handoff_current_scope_resolution,
                validate_frozen_scope_proposal,
            )

            scope_assessment = validate_frozen_scope_proposal(proposal)
            if scope_assessment.classification not in {CANONICAL_VALID, LEGACY_UNAMBIGUOUS}:
                raise AutonomousResearchError(
                    scope_assessment.reason,
                    f"scope proposal classification is {scope_assessment.classification}",
                )
            has_predecessor = any(
                key in proposal
                for key in ("predecessor_candidate_id", "predecessor_frozen_hash", "frozen_hash")
            )
            try:
                if has_predecessor:
                    proposal = freeze_canonical_scope_proposal(
                        proposal,
                        assumptions=proposal.get("assumptions"),
                        current_resolution=proposal.get("current_resolution"),
                        source_candidate_id=proposal.get("predecessor_candidate_id"),
                        source_frozen_hash=proposal.get("predecessor_frozen_hash", proposal.get("frozen_hash")),
                    )
                resolution = proposal.get("current_resolution")
                if isinstance(resolution, Mapping):
                    proposal = handoff_current_scope_resolution(proposal, resolution)
            except (LegacyScopeError, TypeError, ValueError) as exc:
                raise AutonomousResearchError(
                    getattr(exc, "reason", "INVALID_MARKET_SCOPE"),
                    str(exc),
                ) from exc
        validation = validate_hermes_proposal(proposal)
        if not validation.accepted:
            reason_code = next(
                (reason for reason in validation.reasons if re.fullmatch(r"[A-Z][A-Z0-9_]{2,63}", reason)),
                "INVALID_PROPOSAL",
            )
            raise AutonomousResearchError(reason_code, "; ".join(validation.reasons))
        plan = ExperimentPlan.from_proposal(validation.normalized or proposal)
        campaign_boundary: Mapping[str, Any] | None = None
        campaign_id = str(item.payload.get("campaign_id", "")).strip()
        if campaign_id:
            campaign_trial_id = str(item.payload.get("campaign_trial_id", "")).strip()
            campaign_state, campaign_trial = self._campaign_trial_from_payload(
                item.payload,
                campaign_id=campaign_id,
                trial_id=campaign_trial_id,
            )
            _, campaign_boundary = self._validate_campaign_plan_binding(
                campaign_state,
                campaign_trial,
                plan,
                queue_payload=item.payload,
            )
        elif plan.campaign_id:
            raise AutonomousResearchError(
                "CAMPAIGN_PROTOCOL_INVALID",
                "campaign plan is missing its queue campaign binding",
            )
        if generated_scope_binding is None:
            self._revalidate_generated_queue_item(item, plan, proposal)
        else:
            # The frozen successor may not carry the internal marker, but it
            # must remain bound to the exact authenticated predecessor scope
            # and dataset that were checked above.
            frozen_scope_binding = (
                _binding_value(proposal.get("predecessor_candidate_id")),
                _binding_value(
                    proposal.get("predecessor_frozen_hash", proposal.get("frozen_hash"))
                ),
                _binding_value(plan.market_scope_hash),
                _binding_value(plan.market_scope_version),
                _binding_value(plan.dataset_id),
                _binding_value(plan.dataset_version),
            )
            if frozen_scope_binding != generated_scope_binding:
                raise AutonomousResearchError(
                    "GENERATED_PROVENANCE_INVALID",
                    "generated legacy scope freeze changed its authenticated binding",
                )

        if plan.max_variants > self.config.max_plan_variants:
            raise AutonomousResearchError(
                "EXPERIMENT_BUDGET_EXCEEDED",
                f"plan requests {plan.max_variants} variants; node limit is {self.config.max_plan_variants}",
            )
        if _variant_count(plan) > self.config.max_plan_variants:
            raise AutonomousResearchError(
                "EXPERIMENT_BUDGET_EXCEEDED",
                "plan parameter space exceeds the node bound; all declared trials must run",
            )
        variants = plan.variants()
        if len(variants) != _variant_count(plan):
            raise AutonomousResearchError(
                "EXPERIMENT_BUDGET_EXCEEDED",
                "plan variant generation truncated; refusing incomplete trial set",
            )
        if not variants:
            raise AutonomousResearchError("EXPERIMENT_BUDGET_EXCEEDED", "plan produced no bounded variants")
        # Record the immutable plan before attempting historical evaluation so
        # schema-valid paper observation intents survive an insufficient split.
        self.store.save_experiment_plan(
            plan.plan_id,
            plan.as_dict(),
            hypothesis_id=plan.hypothesis_id,
            plan_hash=plan.plan_hash,
            status="ACCEPTED",
            timestamp=now,
        )
        prepared: list[dict[str, Any]] = []
        validation_scores: dict[str, float] = {}
        # Freeze every schema-valid bounded strategy and persist its paper
        # observation intent before loading or judging historical evidence.
        for parameters in variants:
            candidate_id = _candidate_id(plan, parameters, generation=0)
            strategy = plan.strategy_for(parameters, candidate_id)
            self._initialize_candidate(
                plan,
                candidate_id,
                strategy,
                parameters,
                trial_index=len(prepared),
                trial_count=len(variants),
                now=now,
            )
            prepared.append(
                {
                    "trial_index": len(prepared),
                    "candidate_id": candidate_id,
                    "strategy": strategy,
                    "parameters": dict(parameters),
                    "variant_id": plan.variant_id(parameters),
                    "evaluation": None,
                }
            )
        rows, split = self._load_split(plan, boundary_override=campaign_boundary)
        split_counts = {
            "dataset_row_count": len(rows),
            "train_sample_count": len(split.train),
            "validation_sample_count": len(split.validation),
            "holdout_sample_count": len(split.holdout),
        }
        historical_insufficient = len(split.validation) < plan.min_samples
        if not historical_insufficient:
            for candidate in prepared:
                strategy: StrategyDefinition = candidate["strategy"]
                try:
                    evaluation = self._evaluate_datasets(
                        plan,
                        strategy,
                        split.train,
                        split.validation,
                        (),
                    )
                except AutonomousResearchError:
                    raise
                except (KeyError, TypeError, ValueError) as exc:
                    raise AutonomousResearchError("INVALID_DATASET", str(exc)) from exc
                candidate["evaluation"] = evaluation
                validation_scores[str(candidate["candidate_id"])] = _finite(
                    evaluation["validation"].get("expectancy"), 0.0
                )
        self.store.save_experiment_plan(
            plan.plan_id,
            plan.as_dict(),
            hypothesis_id=plan.hypothesis_id,
            plan_hash=plan.plan_hash,
            status="ACCEPTED",
            timestamp=now,
        )
        if historical_insufficient:
            results: list[dict[str, Any]] = []
            historical_sample_count = len(split.validation)
            historical_filled_trades = 0
            historical_sample_check = minimum_sample_check(
                historical_sample_count,
                trades=historical_filled_trades,
                min_observations=plan.min_samples,
                min_trades=plan.min_trades,
            )
            for candidate in prepared:
                candidate_id = str(candidate["candidate_id"])
                strategy: StrategyDefinition = candidate["strategy"]
                observation_reason = (
                    "at least three chronological observations are required"
                    if len(rows) < 3
                    else "INSUFFICIENT_DATA"
                )
                current = self.lifecycle.get(candidate_id)
                if current is not None and current.stage is CandidateStage.SCHEMA_VALIDATED:
                    self.lifecycle.record_evidence(
                        candidate_id,
                        {
                            "historical_qualification": "INSUFFICIENT_DATA",
                            "historical_blocker": "INSUFFICIENT_DATA",
                            "validation": {
                                "sample_count": historical_sample_count,
                                "filled_trades": historical_filled_trades,
                            },
                            "validation_sample_count": historical_sample_count,
                            "validation_filled_trades": historical_filled_trades,
                            "required_validation_samples": plan.min_samples,
                            "required_validation_trades": plan.min_trades,
                            "minimum_sample_check": historical_sample_check,
                            "paper_observation_intent": True,
                            "paper_only": True,
                            "research_only": True,
                        },
                        expected_stage=CandidateStage.SCHEMA_VALIDATED,
                        reason="historical validation remains below required sample minimum",
                    )
                results.append(
                    {
                        "candidate_id": candidate_id,
                        "stage": CandidateStage.SCHEMA_VALIDATED.value,
                        "reason": observation_reason,
                        "reason_code": "INSUFFICIENT_DATA",
                        "forward_test_id": None,
                        "paper_observation_intent": True,
                        "research_only": True,
                        "paper_only": True,
                        "historical_blocker": (
                            f"validation sample count {historical_sample_count} is below {plan.min_samples}"
                        ),
                        "validation_sample_count": historical_sample_count,
                        "validation_filled_trades": historical_filled_trades,
                        "required_validation_samples": plan.min_samples,
                        "required_validation_trades": plan.min_trades,
                        "minimum_sample_check": historical_sample_check,
                        "validation": {
                            "filled_trades": historical_filled_trades,
                        },
                        **split_counts,
                        **_strategy_metadata(strategy),
                    }
                )
            summary = self._hypothesis_result(plan, results, (), split_counts=split_counts)
            self.store.save_experiment_plan(
                plan.plan_id,
                plan.as_dict(),
                hypothesis_id=plan.hypothesis_id,
                plan_hash=plan.plan_hash,
                status="COMPLETED",
                result=summary,
                timestamp=now,
            )
            self.store.save_report_if_absent(
                "autonomous-" + plan.plan_id,
                {"report_type": "autonomous_experiment_result", **summary},
                experiment_id=plan.plan_id,
            )
            return summary
        results: list[dict[str, Any]] = []
        for candidate in prepared:
            candidate_id = str(candidate["candidate_id"])
            strategy: StrategyDefinition = candidate["strategy"]
            evaluation = candidate["evaluation"]
            validation = (
                evaluation.get("validation", {})
                if isinstance(evaluation, Mapping)
                else {}
            )
            sample_check = minimum_sample_check(
                int(validation.get("sample_count", 0)),
                trades=int(validation.get("filled_trades", 0)),
                min_observations=plan.min_samples,
                min_trades=plan.min_trades,
            )
            if not sample_check["passed"]:
                # Historical qualification is a gate on promotion, not a
                # reason to destroy a canonical paper observation intent.
                insufficient_evidence = {
                    "historical_qualification": "INSUFFICIENT_DATA",
                    "historical_blocker": "INSUFFICIENT_DATA",
                    "validation": _compact_evidence(validation),
                    "validation_sample_count": int(validation.get("sample_count", 0)),
                    "validation_filled_trades": int(validation.get("filled_trades", 0)),
                    "required_validation_samples": plan.min_samples,
                    "required_validation_trades": plan.min_trades,
                    "minimum_sample_check": sample_check,
                    "paper_observation_intent": True,
                    "paper_only": True,
                    "research_only": True,
                }
                self.lifecycle.record_evidence(
                    candidate_id,
                    insufficient_evidence,
                    expected_stage=CandidateStage.SCHEMA_VALIDATED,
                    reason="historical validation remains below required sample or trade minimum",
                )
                result = {
                    "candidate_id": candidate_id,
                    "stage": CandidateStage.SCHEMA_VALIDATED.value,
                    "reason": "INSUFFICIENT_DATA",
                    "reason_code": "INSUFFICIENT_DATA",
                    "forward_test_id": None,
                    **insufficient_evidence,
                }
            else:
                try:
                    result = self._advance_candidate(
                        plan,
                        candidate_id,
                        strategy,
                        evaluation,
                        variant_count=len(prepared),
                        validation_scores=validation_scores,
                        now=now,
                        generation=0,
                        lineage=(),
                    )
                except AutonomousResearchError as exc:
                    current = self.lifecycle.get(candidate_id)
                    if current is not None and current.stage is not CandidateStage.REJECTED:
                        self.lifecycle.reject(candidate_id, exc.reason, evidence={"reason_detail": exc.detail})
                    result = {
                        "candidate_id": candidate_id,
                        "stage": CandidateStage.REJECTED.value,
                        "reason": exc.detail,
                        "reason_code": exc.reason,
                    }
            result = {
                **dict(result),
                **_strategy_metadata(strategy),
                "dataset_row_count": split_counts["dataset_row_count"],
                "train_sample_count": split_counts["train_sample_count"],
                "validation_sample_count": int(validation.get("sample_count", 0)),
                "holdout_sample_count": split_counts["holdout_sample_count"],
                "validation_filled_trades": int(validation.get("filled_trades", 0)),
                "required_validation_samples": plan.min_samples,
                "required_validation_trades": plan.min_trades,
            }
            holdout = evaluation.get("holdout") if isinstance(evaluation, Mapping) else None
            if isinstance(holdout, Mapping):
                result = {
                    **dict(result),
                    "holdout_evaluated": True,
                    "holdout_used_for_selection": False,
                    "holdout": _compact_evidence(holdout),
                }
            results.append(result)
        insufficient_data = any(
            str(item.get("reason_code", "")).strip().upper() == "INSUFFICIENT_DATA"
            for item in results
        )
        mutations = (
            ()
            if bool(item.payload.get("predeclared_starting_set"))
            or bool(item.payload.get("campaign_id"))
            or insufficient_data
            else self._generate_mutations(plan, prepared, validation_scores, now, lineage=())
        )
        summary = self._hypothesis_result(plan, results, mutations, split_counts=split_counts)
        self.store.save_experiment_plan(
            plan.plan_id,
            plan.as_dict(),
            hypothesis_id=plan.hypothesis_id,
            plan_hash=plan.plan_hash,
            status="COMPLETED",
            result=summary,
            timestamp=now,
        )
        self.store.save_report_if_absent(
            "autonomous-" + plan.plan_id,
            {"report_type": "autonomous_experiment_result", **summary},
            experiment_id=plan.plan_id,
        )
        return summary

    def _initialize_candidate(
        self,
        plan: ExperimentPlan,
        candidate_id: str,
        strategy: StrategyDefinition,
        parameters: Mapping[str, Any],
        *,
        trial_index: int,
        trial_count: int,
        now: datetime,
    ) -> Mapping[str, Any]:
        payload = self._candidate_payload(
            plan,
            candidate_id,
            strategy,
            parameters,
            variant_id=plan.variant_id(parameters),
            generation=0,
            lineage=(),
        )
        # Register the worker-generated identity before any lifecycle
        # transition or observation intent is persisted.  The surrounding
        # queue transaction makes this registration durable with the plan,
        # run, and completion record, while a crash rolls all of them back.
        self.lifecycle.register_idea(candidate_id, payload)
        self._reserve_candidate(plan, candidate_id, now)
        self.store.save_strategy_if_absent(strategy.id, strategy.to_dict())
        self.store.save_experiment_if_absent(
            candidate_id,
            {
                "status": "IDEA",
                "run_id": candidate_id,
                "candidate_id": candidate_id,
                "hypothesis_id": plan.hypothesis_id,
                "plan_id": plan.plan_id,
                "plan_hash": plan.plan_hash,
                **_scope_binding(plan),
                "dataset_id": plan.dataset_id,
                "dataset_version": plan.dataset_version,
                "variant": dict(parameters),
                "trial_index": trial_index,
                "trial_count": trial_count,
                "holdout_evaluated": False,
                "holdout_used_for_selection": False,
                "paper_only": True,
            },
            strategy_id=strategy.id,
        )
        intent = (
            self._register_schema_observation_intent(plan, candidate_id, strategy, now)
            if plan.market_type is MarketType.PREDICTION
            else None
        )
        result = {**dict(payload), "paper_observation_intent": intent is not None}
        if intent is not None:
            result.update(
                {
                    "paper_observation_intent_id": intent.experiment_id,
                    "paper_observation_intent": True,
                }
            )
        return result

    def _register_schema_observation_intent(
        self,
        plan: ExperimentPlan,
        candidate_id: str,
        strategy: StrategyDefinition,
        now: datetime,
    ) -> Any:
        model_document = plan.model_for() or {"type": "deterministic"}
        if plan.market_type is MarketType.CRYPTO_SPOT:
            config: dict[str, Any] = {"paper_only": True}
            risk_limits: Mapping[str, Any] = {"max_position_fraction": 0.0}
            dataset_attestation: Mapping[str, Any] | None = None
        else:
            dataset_attestation = self._dataset_attestation(plan, strict=False)
            config = {
                "execution": "paper_only",
                "market_authority_required": False,
            }
            risk_limits = {"max_position_fraction": 0.05}
        config.update(
            {
                "candidate_id": candidate_id,
                "plan_id": plan.plan_id,
                "plan_hash": plan.plan_hash,
                "dataset_id": plan.dataset_id,
                "dataset_version": plan.dataset_version,
                "strategy_document": strategy.to_dict(),
                "model_document": dict(model_document),
                **_scope_binding(plan),
                "assumptions": dict(plan.assumptions),
                "cost_assumptions": {
                    "fee_bps": plan.assumptions.get("fee_bps"),
                    "slippage_bps": plan.assumptions.get("slippage_bps"),
                    "roundtrip_fee_bps": plan.assumptions.get("roundtrip_fee_bps"),
                    "roundtrip_slippage_bps": plan.assumptions.get("roundtrip_slippage_bps"),
                    "sensitivity": plan.assumptions.get("cost_sensitivity"),
                },
                "exit_policy": dict(plan.exit_policy),
                "research_mode": plan.research_mode,
            }
        )
        intent = ForwardTestRegistry(self.store).register_observation_intent(
            strategy=strategy.to_dict(),
            model=dict(model_document),
            config=config,
            registration_timestamp=now,
            risk_limits=risk_limits,
            candidate_id=candidate_id,
        )
        current = self.lifecycle.get(candidate_id)
        if current is not None and current.stage is CandidateStage.IDEA:
            self.lifecycle.advance(
                candidate_id,
                CandidateStage.SCHEMA_VALIDATED,
                {
                    **dict(current.payload),
                    "schema_valid": True,
                    "referenced_features": list(plan.allowed_features),
                    "parameter_ranges_bounded": True,
                    "paper_observation_intent_id": intent.experiment_id,
                    "paper_observation_intent": True,
                    "paper_only": True,
                    "research_only": True,
                    "historical_qualification": "PENDING",
                    **(
                        {"dataset_attestation": dict(dataset_attestation)}
                        if dataset_attestation is not None
                        else {}
                    ),
                },
                reason="canonical paper observation intent registered before historical qualification",
            )
        return intent

    def _process_candidate(self, item: ResearchQueueItem, now: datetime) -> Mapping[str, Any]:
        payload = dict(item.payload)
        candidate_id = str(payload.get("candidate_id", "")).strip()
        if not candidate_id:
            raise AutonomousResearchError("INVALID_CANDIDATE", "candidate_id is required")
        raw_generation = payload.get("generation", 0)
        if isinstance(raw_generation, bool) or not isinstance(raw_generation, int) or raw_generation < 0:
            raise AutonomousResearchError("INVALID_CANDIDATE", "generation must be a non-negative integer")
        generation = int(raw_generation)
        if generation > self.config.max_generation_depth:
            raise AutonomousResearchError(
                "GENERATION_DEPTH_EXCEEDED",
                f"candidate generation {generation} exceeds node limit {self.config.max_generation_depth}",
            )
        run_record = self.store.load_experiment(candidate_id)
        if not isinstance(run_record, Mapping):
            raise AutonomousResearchError(
                "UNAUTHENTICATED_CANDIDATE",
                "candidate queue items require a persisted worker-generated run",
            )
        persisted_run_id = _binding_value(run_record.get("run_id"))
        persisted_candidate_id = _binding_value(run_record.get("candidate_id"))
        if persisted_run_id != candidate_id or persisted_candidate_id != candidate_id:
            raise AutonomousResearchError(
                "CANDIDATE_BINDING_MISMATCH",
                "persisted worker run identity does not match candidate_id",
            )
        raw_lineage = payload.get("lineage", ())
        if not isinstance(raw_lineage, (list, tuple)) or len(raw_lineage) > 256:
            raise AutonomousResearchError("INVALID_CANDIDATE", "lineage must be a bounded list")
        lineage = tuple(str(value).strip() for value in raw_lineage if str(value).strip())
        plan_id = str(payload.get("plan_id", "")).strip()
        plan_record = self.store.load_experiment_plan(plan_id) if plan_id else None
        if not isinstance(plan_record, Mapping):
            raise AutonomousResearchError(
                "UNAUTHENTICATED_CANDIDATE",
                "candidate must reference a persisted experiment plan",
            )
        raw_plan = plan_record.get("plan")
        if not isinstance(raw_plan, Mapping):
            raise AutonomousResearchError("INVALID_PLAN", "persisted candidate experiment plan is invalid")
        try:
            plan = ExperimentPlan.from_mapping(raw_plan, hypothesis_id=str(payload.get("hypothesis_id", "")).strip() or None)
        except ExperimentPlanError as exc:
            raise AutonomousResearchError(exc.reason, exc.detail) from exc
        if plan.max_variants > self.config.max_plan_variants:
            raise AutonomousResearchError(
                "EXPERIMENT_BUDGET_EXCEEDED",
                f"plan requests {plan.max_variants} variants; node limit is {self.config.max_plan_variants}",
            )
        raw_max_variants = payload.get("max_variants")
        if raw_max_variants is not None and (
            isinstance(raw_max_variants, bool)
            or not isinstance(raw_max_variants, int)
            or raw_max_variants < 1
            or raw_max_variants > self.config.max_plan_variants
            or raw_max_variants > plan.max_variants
        ):
            raise AutonomousResearchError(
                "EXPERIMENT_BUDGET_EXCEEDED",
                "candidate max_variants exceeds the persisted plan or node limit",
            )
        if generation > 0 and plan.market_type is MarketType.PREDICTION:
            if _generated_queue_provenance(payload) is None:
                raise AutonomousResearchError(
                    "GENERATED_PROVENANCE_INVALID",
                    "generated mutation candidate is missing its internal provenance marker",
                )
            self._revalidate_generated_queue_item(item, plan, payload)
        self._validate_worker_candidate_binding(
            payload,
            candidate_id=candidate_id,
            generation=generation,
            lineage=lineage,
            plan=plan,
            plan_record=plan_record,
            run_record=run_record,
        )
        crypto_binding = self._crypto_binding(plan)
        strategy_value = payload.get("strategy", payload.get("strategy_document"))
        if not isinstance(strategy_value, Mapping):
            raise AutonomousResearchError("UNSUPPORTED_STRATEGY_FAMILY", "candidate strategy document is missing")
        try:
            supplied_strategy = load_strategy(strategy_value)
            raw_parameters = payload.get("parameters")
            if not isinstance(raw_parameters, Mapping):
                raise AutonomousResearchError("CANDIDATE_BINDING_MISMATCH", "candidate parameters are required")
            parameters = dict(raw_parameters)
            if dict(supplied_strategy.parameters) != parameters:
                raise AutonomousResearchError(
                    "CANDIDATE_BINDING_MISMATCH",
                    "candidate strategy parameters do not match candidate parameters",
                )
            strategy = plan.strategy_for(parameters, candidate_id)
            supplied_document = supplied_strategy.to_dict()
            expected_document = strategy.to_dict()
            supplied_document.pop("strategy_id", None)
            expected_document.pop("strategy_id", None)
            if supplied_document != expected_document:
                raise AutonomousResearchError(
                    "UNSUPPORTED_STRATEGY_FAMILY",
                    "candidate strategy does not match its declarative experiment plan",
                )
        except AutonomousResearchError:
            raise
        except Exception as exc:
            raise AutonomousResearchError("UNSUPPORTED_STRATEGY_FAMILY", str(exc)) from exc
        if generation == 0:
            raise AutonomousResearchError(
                "UNAUTHENTICATED_CANDIDATE",
                "generation-zero candidates are produced inline, not accepted from the queue",
            )
        else:
            parent_id = _binding_value(payload.get("parent_id"))
            if _binding_value(run_record.get("strategy_id")) != candidate_id:
                raise AutonomousResearchError(
                    "CANDIDATE_BINDING_MISMATCH",
                    "mutation strategy identity is not the persisted candidate identity",
                )
            persisted_variant = run_record.get("variant")
            if not isinstance(persisted_variant, Mapping) or dict(persisted_variant) != parameters:
                raise AutonomousResearchError(
                    "CANDIDATE_BINDING_MISMATCH",
                    "mutation parameters do not match the persisted worker variant",
                )
            persisted_record_strategy = run_record.get("strategy")
            if not isinstance(persisted_record_strategy, Mapping):
                raise AutonomousResearchError(
                    "CANDIDATE_BINDING_MISMATCH",
                    "mutation strategy document is missing from the persisted worker run",
                )
            stored_document = self.store.load_strategy(candidate_id)
            if not parent_id or not isinstance(stored_document, Mapping):
                raise AutonomousResearchError(
                    "CANDIDATE_BINDING_MISMATCH",
                    "mutation candidate lacks persisted parent strategy",
                )
            try:
                stored_strategy = load_strategy(stored_document)
                recorded_strategy = load_strategy(persisted_record_strategy)
            except Exception as exc:
                raise AutonomousResearchError("CANDIDATE_BINDING_MISMATCH", "persisted mutation strategy is invalid") from exc
            supplied_document = supplied_strategy.to_dict()
            persisted_document = stored_strategy.to_dict()
            recorded_document = recorded_strategy.to_dict()
            supplied_document.pop("strategy_id", None)
            persisted_document.pop("strategy_id", None)
            recorded_document.pop("strategy_id", None)
            if supplied_document != persisted_document or supplied_document != recorded_document:
                raise AutonomousResearchError(
                    "CANDIDATE_BINDING_MISMATCH",
                    "mutation strategy does not match the persisted worker strategy",
                )
            expected_candidate_id = _mutation_candidate_id(parent_id, generation, stored_strategy)
            if candidate_id != expected_candidate_id:
                raise AutonomousResearchError(
                    "CANDIDATE_BINDING_MISMATCH",
                    "mutation candidate_id is not deterministic for its parent lineage",
                )
        rows, split = self._load_split(plan)
        try:
            evaluation = self._evaluate_datasets(plan, strategy, split.train, split.validation)
        except AutonomousResearchError:
            raise
        except (KeyError, TypeError, ValueError) as exc:
            raise AutonomousResearchError("INVALID_DATASET", str(exc)) from exc
        # Direct candidate submissions do not pass through hypothesis
        # expansion; reserve their unit inside the queue transaction too.
        self._reserve_candidate(plan, candidate_id, now)
        current = self.lifecycle.get(candidate_id)
        if current is None:
            candidate_record = {
                **payload,
                "candidate_id": candidate_id,
                "plan_id": plan.plan_id,
                "plan_hash": plan.plan_hash,
                **_scope_binding(plan),
                "holdout_used": False,
            }
            if crypto_binding is not None:
                candidate_record["crypto_provenance"] = crypto_binding
            self.lifecycle.register_idea(candidate_id, candidate_record)
        result = self._advance_candidate(
            plan,
            candidate_id,
            strategy,
            evaluation,
            variant_count=max(1, len(plan.variants())),
            validation_scores={candidate_id: _finite(evaluation["validation"].get("expectancy"), 0.0)},
            now=now,
            generation=generation,
            lineage=lineage,
            crypto_binding=crypto_binding,
        )
        if (
            result.get("stage") in {
                CandidateStage.ROBUSTNESS_CHECKED.value,
                CandidateStage.FROZEN.value,
                CandidateStage.PAPER_FORWARD.value,
                CandidateStage.PAPER_PROMOTABLE.value,
            }
            and generation < self.config.max_generation_depth
        ):
            mutation_ids = self._generate_mutations(
                plan,
                (
                    {
                        "candidate_id": candidate_id,
                        "strategy": strategy,
                        "evaluation": evaluation,
                    },
                ),
                {candidate_id: _finite(evaluation["validation"].get("expectancy"), 0.0)},
                now,
                lineage=lineage,
            )
            if mutation_ids:
                result = {**dict(result), "mutation_candidates": list(mutation_ids)}
        return {**dict(result), **_scope_binding(plan)}

    def _validate_worker_candidate_binding(
        self,
        payload: Mapping[str, Any],
        *,
        candidate_id: str,
        generation: int,
        lineage: Sequence[str],
        plan: ExperimentPlan,
        plan_record: Mapping[str, Any],
        run_record: Mapping[str, Any],
    ) -> None:
        """Accept only candidate payloads created by the durable worker path."""
        for field, expected in (
            ("plan_id", plan.plan_id),
            ("plan_hash", plan.plan_hash),
            ("dataset_id", plan.dataset_id),
            ("dataset_version", plan.dataset_version),
        ):
            supplied = _binding_value(payload.get(field))
            persisted = _binding_value(run_record.get(field))
            if supplied != expected or persisted != expected:
                raise AutonomousResearchError(
                    "CANDIDATE_BINDING_MISMATCH",
                    f"candidate {field} does not match the persisted experiment plan",
                )
        expected_scope = _scope_binding(plan)
        for field, expected in expected_scope.items():
            supplied = payload.get(field)
            persisted = run_record.get(field)
            if _canonical_binding(supplied) != _canonical_binding(expected) or _canonical_binding(persisted) != _canonical_binding(expected):
                raise AutonomousResearchError(
                    "CANDIDATE_BINDING_MISMATCH",
                    f"candidate {field} does not match the persisted experiment plan",
                )
        if _binding_value(plan_record.get("plan_id")) != plan.plan_id or _binding_value(plan_record.get("plan_hash")) != plan.plan_hash:
            raise AutonomousResearchError("CANDIDATE_BINDING_MISMATCH", "candidate plan identity is not exact")

        stored_generation = run_record.get("generation")
        if isinstance(stored_generation, bool) or not isinstance(stored_generation, int) or stored_generation != generation:
            raise AutonomousResearchError("CANDIDATE_BINDING_MISMATCH", "candidate generation is not the persisted worker generation")
        stored_lineage = run_record.get("lineage", ())
        if not isinstance(stored_lineage, (list, tuple)):
            raise AutonomousResearchError("CANDIDATE_BINDING_MISMATCH", "persisted candidate lineage is invalid")
        normalized_stored_lineage = tuple(str(value).strip() for value in stored_lineage if str(value).strip())
        if normalized_stored_lineage != tuple(lineage):
            raise AutonomousResearchError("CANDIDATE_BINDING_MISMATCH", "candidate lineage is not the persisted worker lineage")
        if generation == 0:
            if _binding_value(payload.get("parent_id")) or lineage:
                raise AutonomousResearchError("CANDIDATE_BINDING_MISMATCH", "generation-zero candidates cannot declare lineage")
        else:
            parent_id = _binding_value(payload.get("parent_id"))
            persisted_parent_id = _binding_value(run_record.get("parent_id"))
            if not parent_id or parent_id != persisted_parent_id or parent_id not in lineage:
                raise AutonomousResearchError("CANDIDATE_BINDING_MISMATCH", "mutation parent lineage is not exact")
            if self.lifecycle.get(parent_id) is None:
                raise AutonomousResearchError("CANDIDATE_BINDING_MISMATCH", "mutation parent is not persisted")

    def _process_report(self, item: ResearchQueueItem, now: datetime) -> Mapping[str, Any]:
        payload = compact_report(item.payload)
        report_id = str(payload.get("report_id", item.item_id)).strip() if isinstance(payload, Mapping) else item.item_id
        if not report_id:
            report_id = item.item_id
        self.store.save_report_if_absent(report_id, {"report_type": "informational", "payload": payload})
        return {"accepted": True, "kind": "report", "report_id": report_id, "persisted": True, "paper_only": True}

    def _process_review_request(self, item: ResearchQueueItem, now: datetime) -> Mapping[str, Any]:
        payload = item.payload
        candidate_id = str(payload.get("candidate_id", "")).strip()
        hypothesis_id = str(payload.get("hypothesis_id", "")).strip()
        candidate = self.lifecycle.get(candidate_id) if candidate_id else None
        plan_rows = self.store.list_experiment_plans(hypothesis_id=hypothesis_id, limit=32) if hypothesis_id else []
        response = {
            "accepted": True,
            "kind": "review_request",
            "candidate_id": candidate_id or None,
            "hypothesis_id": hypothesis_id or None,
            "candidate_stage": candidate.stage.value if candidate is not None else None,
            "plans": len(plan_rows),
            "status": "review_output_ready",
            "paper_only": True,
        }
        self.store.save_report_if_absent("review-" + item.item_id, {"report_type": "review", **response})
        return response

    def _process_experiment_result(self, item: ResearchQueueItem, now: datetime) -> Mapping[str, Any]:
        """Attach only evidence derived from a persisted, bound paper run.

        Queue payloads are untrusted input.  In particular, ``result`` and
        ``metrics`` are never copied into lifecycle evidence: a caller can
        submit either field without having run an experiment.  The queue item
        must identify an immutable persisted experiment and the exact plan and
        dataset recorded for the candidate.  Forward metrics are then
        recomputed from the paper observations, execution events, and ledger.
        """
        payload = item.payload
        candidate_id = str(payload.get("candidate_id", "")).strip()
        if not candidate_id:
            raise AutonomousResearchError("INVALID_RESULT", "experiment_result requires candidate_id")
        candidate = self.lifecycle.get(candidate_id)
        if candidate is None:
            raise AutonomousResearchError("INVALID_RESULT", f"unknown candidate {candidate_id}")

        experiment = self.store.load_experiment(candidate_id)
        if not isinstance(experiment, Mapping):
            raise AutonomousResearchError(
                "UNAUTHENTICATED_RESULT",
                "experiment_result requires a persisted immutable worker run",
            )
        self._validate_result_binding(candidate_id, candidate.payload, payload, experiment)

        forward_id = str(candidate.payload.get("forward_test_id", "")).strip()
        if str(payload.get("forward_test_id", "")).strip() != forward_id:
            raise AutonomousResearchError(
                "UNAUTHENTICATED_RESULT",
                "experiment_result forward_test_id does not match the candidate run",
            )
        if candidate.stage is not CandidateStage.PAPER_FORWARD or not forward_id:
            raise AutonomousResearchError(
                "UNTRUSTED_RESULT",
                "experiment_result has no persisted paper forward run to evaluate",
            )
        registry = ForwardTestRegistry(self.store)
        spec = registry.get(forward_id)
        if spec is None:
            raise AutonomousResearchError("UNAUTHENTICATED_RESULT", f"missing persisted forward test {forward_id}")
        self._validate_forward_result_binding(candidate.payload, spec)

        observations = self.store.list_paper_observations(forward_id, limit=_MAX_FORWARD_ROWS)
        execution_events = self.store.list_paper_execution_events(forward_id, limit=_MAX_FORWARD_ROWS)
        ledgers = self.store.list_paper_bet_ledger(forward_id, limit=_MAX_FORWARD_ROWS)
        if not observations and not execution_events and not ledgers:
            raise AutonomousResearchError(
                "UNTRUSTED_RESULT",
                "experiment_result has no persisted paper observations or outcomes",
            )

        # The caller-supplied result/metrics is intentionally ignored.  This
        # is the sole evidence path for a queued experiment result.
        evidence = self._forward_evidence(candidate.as_record(), now)
        evidence["result_attached"] = True
        evidence["holdout_used"] = False
        candidate = self.lifecycle.record_evidence(
            candidate_id,
            evidence,
            expected_stage=CandidateStage.PAPER_FORWARD,
            reason="bound paper result evaluated from persisted observations",
        )
        self._evaluate_forward_candidate(candidate_id, now)
        response = {
            "accepted": True,
            "kind": "experiment_result",
            "candidate_id": candidate_id,
            "stage": (self.lifecycle.get(candidate_id) or candidate).stage.value,
            "attached": True,
            "paper_only": True,
            "evidence_source": "persisted_paper_observations",
        }
        self.store.save_report_if_absent("result-" + item.item_id, {"report_type": "experiment_result", **response})
        return response

    def _validate_result_binding(
        self,
        candidate_id: str,
        candidate_payload: Mapping[str, Any],
        result_payload: Mapping[str, Any],
        experiment: Mapping[str, Any],
    ) -> None:
        """Require queue identity and immutable plan/dataset bindings to agree."""
        run_id = str(result_payload.get("run_id", result_payload.get("experiment_id", ""))).strip()
        if run_id != candidate_id:
            raise AutonomousResearchError(
                "UNAUTHENTICATED_RESULT",
                "experiment_result run_id does not match the persisted worker run",
            )
        for identity_field in ("run_id", "experiment_id"):
            supplied_identity = result_payload.get(identity_field)
            if supplied_identity is not None and str(supplied_identity).strip() != candidate_id:
                raise AutonomousResearchError(
                    "UNAUTHENTICATED_RESULT",
                    f"experiment_result {identity_field} does not match the persisted worker run",
                )
        persisted_run_id = str(experiment.get("run_id", candidate_id)).strip()
        if persisted_run_id != candidate_id:
            raise AutonomousResearchError("RESULT_BINDING_MISMATCH", "persisted worker run identity does not match")
        experiment_candidate = str(experiment.get("candidate_id", candidate_id)).strip()
        if experiment_candidate != candidate_id:
            raise AutonomousResearchError("RESULT_BINDING_MISMATCH", "persisted worker run candidate_id does not match")

        plan_id = _binding_value(candidate_payload.get("plan_id"))
        plan_hash = _binding_value(candidate_payload.get("plan_hash"))
        dataset_id = _binding_value(candidate_payload.get("dataset_id"))
        dataset_version = _binding_value(candidate_payload.get("dataset_version"))
        if not plan_id or not plan_hash or not dataset_version:
            raise AutonomousResearchError(
                "RESULT_BINDING_MISMATCH",
                "candidate lacks exact persisted plan and dataset binding",
            )
        for field, expected in (
            ("plan_id", plan_id),
            ("plan_hash", plan_hash),
            ("dataset_id", dataset_id),
            ("dataset_version", dataset_version),
        ):
            supplied = _binding_value(result_payload.get(field))
            if supplied != expected:
                raise AutonomousResearchError(
                    "RESULT_BINDING_MISMATCH",
                    f"experiment_result {field} does not match the candidate binding",
                )
            persisted = _binding_value(experiment.get(field))
            if persisted != expected:
                raise AutonomousResearchError(
                    "RESULT_BINDING_MISMATCH",
                    f"persisted worker run {field} does not match the candidate binding",
                )

        plan_record = self.store.load_experiment_plan(plan_id)
        if not isinstance(plan_record, Mapping):
            raise AutonomousResearchError("RESULT_BINDING_MISMATCH", f"missing persisted experiment plan {plan_id}")
        if _binding_value(plan_record.get("plan_hash")) != plan_hash:
            raise AutonomousResearchError("RESULT_BINDING_MISMATCH", "persisted experiment plan hash does not match")
        raw_plan = plan_record.get("plan")
        if not isinstance(raw_plan, Mapping):
            raise AutonomousResearchError("RESULT_BINDING_MISMATCH", "persisted experiment plan is invalid")
        try:
            plan = ExperimentPlan.from_mapping(raw_plan)
        except ExperimentPlanError as exc:
            raise AutonomousResearchError("RESULT_BINDING_MISMATCH", str(exc)) from exc
        if plan.plan_id != plan_id or plan.plan_hash != plan_hash:
            raise AutonomousResearchError("RESULT_BINDING_MISMATCH", "experiment plan identity is not exact")
        if _binding_value(plan.dataset_id) != dataset_id or plan.dataset_version != dataset_version:
            raise AutonomousResearchError("RESULT_BINDING_MISMATCH", "experiment plan dataset binding is not exact")
        expected_scope = _scope_binding(plan)
        for field, expected in expected_scope.items():
            if _canonical_binding(candidate_payload.get(field)) != _canonical_binding(expected):
                raise AutonomousResearchError("RESULT_BINDING_MISMATCH", f"candidate {field} is not canonical")
            if field in result_payload and _canonical_binding(result_payload.get(field)) != _canonical_binding(expected):
                raise AutonomousResearchError("RESULT_BINDING_MISMATCH", f"experiment_result {field} does not match the candidate scope")
            if _canonical_binding(experiment.get(field)) != _canonical_binding(expected):
                raise AutonomousResearchError("RESULT_BINDING_MISMATCH", f"persisted worker run {field} does not match the candidate scope")

    @staticmethod
    def _validate_forward_result_binding(candidate_payload: Mapping[str, Any], spec: Any) -> None:
        forward_id = str(candidate_payload.get("forward_test_id", "")).strip()
        if str(getattr(spec, "experiment_id", "")).strip() != forward_id:
            raise AutonomousResearchError("RESULT_BINDING_MISMATCH", "forward-test identity does not match candidate")
        config = getattr(spec, "config", {})
        if not isinstance(config, Mapping):
            raise AutonomousResearchError("RESULT_BINDING_MISMATCH", "persisted forward-test config is invalid")
        for field in ("plan_id", "dataset_id", "dataset_version"):
            expected = _binding_value(candidate_payload.get(field))
            if _binding_value(config.get(field)) != expected:
                raise AutonomousResearchError(
                    "RESULT_BINDING_MISMATCH",
                    f"forward-test {field} does not match the candidate binding",
                )
        for field in ("market_scope_hash", "market_scope_version", "scope_hash", "scope_version", "market_scope", "dataset_selector"):
            expected = candidate_payload.get(field)
            if _canonical_binding(config.get(field)) != _canonical_binding(expected):
                raise AutonomousResearchError(
                    "RESULT_BINDING_MISMATCH",
                    f"forward-test {field} does not match the candidate scope",
                )

    def _crypto_binding(self, plan: ExperimentPlan) -> dict[str, Any] | None:
        """Resolve and validate immutable crypto inputs from the persisted store."""
        if plan.market_type is not MarketType.CRYPTO_SPOT:
            return None
        dataset_id = plan.dataset_id
        dataset_version = plan.dataset_version
        timeframe = plan.dataset_timeframe
        source = plan.dataset_source
        survivorship = plan.dataset_survivorship
        if not dataset_id or not dataset_version or not timeframe or not source or not survivorship:
            raise AutonomousResearchError(
                "INSUFFICIENT_DATA",
                "crypto plans require exact dataset id, version, timeframe, source, and survivorship provenance",
            )
        if dataset_version.lower() in {"latest", "current", "default", "unversioned"}:
            raise AutonomousResearchError("INSUFFICIENT_DATA", "crypto dataset_version must be immutable and versioned")
        catalog_loader = getattr(self.store, "load_dataset_catalog", None)
        if not callable(catalog_loader):
            raise AutonomousResearchError("INSUFFICIENT_DATA", "crypto research requires a persisted dataset catalog")
        catalog = catalog_loader(dataset_id, dataset_version)
        if not isinstance(catalog, Mapping):
            raise AutonomousResearchError(
                "INSUFFICIENT_DATA",
                f"no exact dataset catalog {dataset_id}/{dataset_version}",
            )
        catalog_id = str(catalog.get("dataset_id", "")).strip()
        catalog_version = str(catalog.get("dataset_version", catalog.get("version", ""))).strip()
        if catalog_id != dataset_id or catalog_version != dataset_version:
            raise AutonomousResearchError("CRYPTO_PROVENANCE_MISMATCH", "dataset catalog identity does not match the plan")
        if str(catalog.get("market_type", "")).strip().lower() != MarketType.CRYPTO_SPOT.value:
            raise AutonomousResearchError("CRYPTO_PROVENANCE_MISMATCH", "dataset catalog market_type is not crypto_spot")
        catalog_timeframe = str(catalog.get("timeframe", "")).strip()
        if catalog_timeframe != timeframe:
            raise AutonomousResearchError("CRYPTO_PROVENANCE_MISMATCH", "dataset timeframe does not match the plan")
        catalog_source = str(catalog.get("provider", catalog.get("source", ""))).strip()
        if catalog_source != source:
            raise AutonomousResearchError("CRYPTO_PROVENANCE_MISMATCH", "dataset source does not match the plan")
        catalog_source_type = str(catalog.get("source_type", "")).strip().upper()
        declared_source_type = plan.dataset_source_type
        if declared_source_type and catalog_source_type != declared_source_type.strip().upper():
            raise AutonomousResearchError("CRYPTO_PROVENANCE_MISMATCH", "dataset source_type does not match the plan")
        if catalog_source_type != "HISTORICAL":
            raise AutonomousResearchError(
                "CRYPTO_PROVENANCE_MISMATCH",
                "crypto autonomous research accepts historical datasets only",
            )
        catalog_metadata = catalog.get("metadata")
        catalog_metadata = dict(catalog_metadata) if isinstance(catalog_metadata, Mapping) else {}
        catalog_survivorship = catalog_metadata.get(
            "survivorship_bias",
            catalog_metadata.get("survivorship", catalog_metadata.get("survivorship_label")),
        )
        if catalog_survivorship is None or str(catalog_survivorship).strip() != survivorship:
            raise AutonomousResearchError("CRYPTO_PROVENANCE_MISMATCH", "dataset survivorship provenance does not match the plan")

        universe_id = plan.universe_id
        universe_version = plan.universe_version
        universe_hash = plan.universe_snapshot_hash
        if not universe_id or not universe_version or not universe_hash:
            raise AutonomousResearchError(
                "INSUFFICIENT_DATA",
                "crypto plans require exact persisted universe id, version, and snapshot_hash",
            )
        if any(
            value.lower() in {"latest", "current", "default", "unversioned"}
            for value in (universe_version, universe_hash)
        ):
            raise AutonomousResearchError("INSUFFICIENT_DATA", "crypto universe binding must be immutable and versioned")
        universe_document = dict(plan.universe or {})
        universe_dataset_id = str(universe_document.get("dataset_id") or f"universe:{universe_id}").strip()
        universe_catalog = catalog_loader(universe_dataset_id, universe_version)
        if not isinstance(universe_catalog, Mapping):
            raise AutonomousResearchError(
                "INSUFFICIENT_DATA",
                f"no exact universe snapshot {universe_dataset_id}/{universe_version}",
            )
        if str(universe_catalog.get("dataset_id", "")).strip() != universe_dataset_id:
            raise AutonomousResearchError("CRYPTO_PROVENANCE_MISMATCH", "universe catalog identity does not match the plan")
        if str(universe_catalog.get("dataset_version", universe_catalog.get("version", ""))).strip() != universe_version:
            raise AutonomousResearchError("CRYPTO_PROVENANCE_MISMATCH", "universe version does not match the plan")
        universe_metadata = universe_catalog.get("metadata")
        universe_metadata = dict(universe_metadata) if isinstance(universe_metadata, Mapping) else {}
        persisted_universe_id = str(universe_metadata.get("universe_id", "")).strip()
        if persisted_universe_id and persisted_universe_id != universe_id:
            raise AutonomousResearchError("CRYPTO_PROVENANCE_MISMATCH", "universe id does not match the persisted snapshot")
        persisted_hash = str(
            universe_metadata.get("snapshot_hash")
            or universe_metadata.get("content_hash")
            or universe_catalog.get("snapshot_hash")
            or ""
        ).strip()
        if persisted_hash != universe_hash:
            raise AutonomousResearchError("CRYPTO_PROVENANCE_MISMATCH", "universe snapshot_hash does not match the persisted snapshot")
        if universe_metadata.get("point_in_time") is not True:
            raise AutonomousResearchError("CRYPTO_PROVENANCE_MISMATCH", "universe snapshot is not point-in-time")

        universe_record_loader = getattr(self.store, "load_dataset_record", None)
        universe_record = (
            universe_record_loader(universe_dataset_id, universe_version)
            if callable(universe_record_loader)
            else None
        )
        universe_rows = universe_record.get("records") if isinstance(universe_record, Mapping) else None
        if isinstance(universe_rows, Mapping):
            universe_rows = universe_rows.get("records", universe_rows.get("rows", ()))
        if not isinstance(universe_rows, (list, tuple)):
            universe_rows = self.store.load_dataset(universe_dataset_id, universe_version)
        if not isinstance(universe_rows, (list, tuple)):
            raise AutonomousResearchError("INSUFFICIENT_DATA", "persisted universe snapshot has no bounded records")
        selected: dict[str, str] = {}
        for row in universe_rows:
            if not isinstance(row, Mapping) or not bool(row.get("selected")):
                continue
            for key in ("binance_symbol", "symbol", "instrument"):
                value = row.get(key)
                if value is not None and _normal_symbol(value):
                    selected.setdefault(_normal_symbol(value), str(value).strip())
        declared_values = universe_document.get("instruments")
        if declared_values is None:
            declared_values = universe_document.get("symbols")
        if declared_values is None:
            declared_values = ()
        if isinstance(declared_values, str) or not isinstance(declared_values, (list, tuple)):
            raise AutonomousResearchError(
                "CRYPTO_PROVENANCE_MISMATCH",
                "plan universe instruments/symbols must be a bounded list",
            )
        if not declared_values:
            raise AutonomousResearchError(
                "CRYPTO_PROVENANCE_MISMATCH",
                "plan universe instruments/symbols must contain at least one symbol",
            )
        declared: set[str] = set()
        for index, value in enumerate(declared_values):
            normalized = _normal_symbol(value)
            if value is None or not str(value).strip() or not normalized:
                raise AutonomousResearchError(
                    "CRYPTO_PROVENANCE_MISMATCH",
                    f"plan universe instruments/symbols contains an empty symbol at index {index}",
                )
            if normalized in declared:
                raise AutonomousResearchError(
                    "CRYPTO_PROVENANCE_MISMATCH",
                    f"plan universe instruments/symbols contains duplicate normalized symbol {normalized!r}",
                )
            declared.add(normalized)
        persisted = set(selected)
        if declared != persisted:
            missing = sorted(persisted - declared)
            unexpected = sorted(declared - persisted)
            raise AutonomousResearchError(
                "CRYPTO_PROVENANCE_MISMATCH",
                "plan universe instruments do not exactly match persisted selected rows: "
                f"declared={sorted(declared)!r}; persisted={sorted(persisted)!r}; "
                f"missing={missing!r}; unexpected={unexpected!r}",
            )

        target = plan.target_instrument
        target_key = _normal_symbol(target)
        if not target_key or target_key not in selected:
            raise AutonomousResearchError(
                "CRYPTO_UNIVERSE_MEMBERSHIP_MISMATCH",
                f"selected instrument {target or '<missing>'} is not a member of the persisted universe",
            )
        return {
            "dataset": {
                "dataset_id": dataset_id,
                "dataset_version": dataset_version,
                "timeframe": catalog_timeframe,
                "source": catalog_source,
                "source_type": catalog_source_type,
                "survivorship_bias": survivorship,
                "snapshot_id": str(catalog.get("snapshot_id", "")).strip(),
            },
            "universe": {
                "universe_id": universe_id,
                "universe_version": universe_version,
                "snapshot_hash": universe_hash,
                "dataset_id": universe_dataset_id,
                "snapshot_id": str(universe_catalog.get("snapshot_id", "")).strip(),
                "methodology": str(universe_document.get("methodology", "")).strip(),
                "point_in_time": True,
            },
            "selected_symbol": selected[target_key],
        }
    def _dataset_attestation(
        self,
        plan: ExperimentPlan,
        *,
        strict: bool = True,
    ) -> Mapping[str, Any]:
        """Load dataset attestation used by frozen scope evidence.

        Automatic queue items and legacy-scope reassessments call this in the
        default strict mode. Ordinary manually submitted proposals retain the
        historical compatibility path: their dataset rows remain the source
        of historical evaluation, while attestation is optional metadata.
        """
        dataset_id = plan.dataset_id
        dataset_version = plan.dataset_version
        if not dataset_id and dataset_version:
            finder = getattr(self.store, "load_dataset_by_version", None)
            try:
                resolved = finder(dataset_version) if callable(finder) else None
            except Exception:
                if strict:
                    raise
                resolved = None
            if isinstance(resolved, Mapping):
                candidate_id = resolved.get("dataset_id")
                if candidate_id is not None and str(candidate_id).strip():
                    dataset_id = str(candidate_id).strip()
        if not dataset_id or not dataset_version:
            if not strict:
                return {}
            raise AutonomousResearchError(
                "DATASET_ATTESTATION_MISSING",
                "frozen scope evidence requires an exact dataset selector",
            )
        loader = getattr(self.store, "load_dataset_integrity_attestation", None)
        verifier = getattr(self.store, "verify_dataset_integrity_attestation", None)
        try:
            attestation = loader(dataset_id, dataset_version) if callable(loader) else None
            if not isinstance(attestation, Mapping) and callable(verifier):
                attestation = verifier(dataset_id, dataset_version)
        except Exception:
            if not strict:
                return {}
            raise
        if not isinstance(attestation, Mapping):
            if not strict:
                return {}
            raise AutonomousResearchError(
                "DATASET_ATTESTATION_MISSING",
                f"no immutable dataset attestation for {dataset_id}/{dataset_version}",
            )
        return dict(attestation)

    def _validate_persisted_dataset_provenance(
        self,
        plan: ExperimentPlan,
        *,
        legacy_document: Mapping[str, Any] | None = None,
    ) -> Mapping[str, Any]:
        """Require an exact, current store attestation before prediction research."""
        if plan.market_type is not MarketType.PREDICTION:
            return {}
        dataset_id = str(plan.dataset_id or "").strip()
        dataset_version = str(plan.dataset_version or "").strip()
        if not dataset_id or not dataset_version:
            raise AutonomousResearchError(
                "DATASET_PROVENANCE_INVALID",
                "prediction research requires an exact persisted dataset id and version",
            )
        if dataset_version.casefold() in _MUTABLE_DATASET_VERSION_ALIASES:
            raise AutonomousResearchError(
                "DATASET_PROVENANCE_INVALID",
                f"dataset version {dataset_version!r} is a mutable alias; an immutable dataset version is required",
            )

        catalog_loader = getattr(self.store, "load_dataset_catalog", None)
        if not callable(catalog_loader):
            raise AutonomousResearchError(
                "DATASET_PROVENANCE_INVALID",
                f"store has no exact persisted dataset catalog for {dataset_id}/{dataset_version}",
            )
        try:
            catalog = catalog_loader(dataset_id, dataset_version)
        except Exception as exc:
            raise AutonomousResearchError(
                "DATASET_PROVENANCE_INVALID",
                f"persisted dataset catalog could not be loaded for {dataset_id}/{dataset_version}",
            ) from exc
        if not isinstance(catalog, Mapping):
            raise AutonomousResearchError(
                "DATASET_PROVENANCE_INVALID",
                f"no exact persisted dataset catalog for {dataset_id}/{dataset_version}",
            )
        catalog_id = str(catalog.get("dataset_id", "")).strip()
        catalog_version = str(catalog.get("dataset_version", catalog.get("version", ""))).strip()
        if (
            catalog_id != dataset_id
            or catalog_version != dataset_version
            or catalog_version.casefold() in _MUTABLE_DATASET_VERSION_ALIASES
        ):
            raise AutonomousResearchError(
                "DATASET_PROVENANCE_INVALID",
                "dataset catalog identity does not match the exact plan binding",
            )
        if str(catalog.get("market_type", "")).strip().lower() != MarketType.PREDICTION.value:
            raise AutonomousResearchError(
                "DATASET_PROVENANCE_INVALID",
                "dataset catalog market_type is not prediction",
            )
        catalog_source_type = str(catalog.get("source_type", "")).strip().upper()
        if catalog_source_type != "HISTORICAL":
            raise AutonomousResearchError(
                "DATASET_PROVENANCE_INVALID",
                f"dataset catalog source_type {catalog_source_type or '<missing>'} is not HISTORICAL",
            )
        catalog_instrument = str(catalog.get("instrument", "")).strip()
        scope_instrument = str(plan.market_scope.instrument or "").strip()
        if scope_instrument and catalog_instrument != scope_instrument:
            raise AutonomousResearchError(
                "DATASET_PROVENANCE_INVALID",
                "dataset catalog instrument does not match the exact plan market-scope instrument",
            )
        catalog_metadata = catalog.get("metadata")
        if isinstance(catalog_metadata, Mapping):
            metadata_instrument = str(catalog_metadata.get("instrument", "")).strip()
            if metadata_instrument and metadata_instrument != catalog_instrument:
                raise AutonomousResearchError(
                    "DATASET_PROVENANCE_INVALID",
                    "dataset catalog instrument metadata conflicts with its canonical instrument",
                )

        selector = plan.dataset_selector
        declared_selectors: dict[str, list[Any]] = {
            name: [selector.get(name)]
            for name in ("provider", "source", "timeframe", "interval", "source_type")
            if selector.get(name) is not None and str(selector.get(name)).strip()
        }
        if legacy_document is not None:
            nested = legacy_document.get("experiment_plan")
            nested = nested if isinstance(nested, Mapping) else {}
            for source in (legacy_document, nested):
                raw_selector = source.get("dataset_selector")
                if isinstance(raw_selector, Mapping):
                    for name in ("provider", "source", "timeframe", "interval", "source_type"):
                        value = raw_selector.get(name)
                        if value is not None and str(value).strip():
                            declared_selectors.setdefault(name, []).append(value)
                for raw_name, name in (
                    ("dataset_source", "source"),
                    ("dataset_source_type", "source_type"),
                    ("dataset_timeframe", "timeframe"),
                ):
                    value = source.get(raw_name)
                    if value is not None and str(value).strip():
                        declared_selectors.setdefault(name, []).append(value)
        catalog_sources = [
            str(catalog.get(name)).strip()
            for name in ("provider", "source")
            if catalog.get(name) is not None and str(catalog.get(name)).strip()
        ]
        if len(set(catalog_sources)) > 1:
            raise AutonomousResearchError(
                "DATASET_PROVENANCE_INVALID",
                "dataset catalog provider and source selectors disagree",
            )
        catalog_source = catalog_sources[0] if catalog_sources else ""
        for name in ("provider", "source"):
            for declared in declared_selectors.get(name, ()):
                if str(declared).strip() != catalog_source:
                    raise AutonomousResearchError(
                        "DATASET_PROVENANCE_INVALID",
                        f"dataset selector {name} does not match the exact catalog provider/source",
                    )
        catalog_timeframe = str(catalog.get("timeframe", "")).strip()
        for name in ("timeframe", "interval"):
            for declared in declared_selectors.get(name, ()):
                if str(declared).strip() != catalog_timeframe:
                    raise AutonomousResearchError(
                        "DATASET_PROVENANCE_INVALID",
                        f"dataset selector {name} does not match the exact catalog timeframe",
                    )
        for declared_source_type in declared_selectors.get("source_type", ()):
            if str(declared_source_type).strip().upper() != catalog_source_type:
                raise AutonomousResearchError(
                    "DATASET_PROVENANCE_INVALID",
                    "dataset selector source_type does not match the exact catalog",
                )

        attestation_loader = getattr(self.store, "load_dataset_integrity_attestation", None)
        if not callable(attestation_loader):
            raise AutonomousResearchError(
                "DATASET_ATTESTATION_MISSING",
                f"store has no persisted dataset attestation for {dataset_id}/{dataset_version}",
            )
        try:
            attestation = attestation_loader(dataset_id, dataset_version)
        except Exception as exc:
            raise AutonomousResearchError(
                "DATASET_ATTESTATION_MISSING",
                f"persisted dataset attestation could not be loaded for {dataset_id}/{dataset_version}",
            ) from exc
        if not isinstance(attestation, Mapping):
            raise AutonomousResearchError(
                "DATASET_ATTESTATION_MISSING",
                f"no persisted dataset attestation for {dataset_id}/{dataset_version}",
            )
        if (
            str(attestation.get("dataset_id", "")).strip() != dataset_id
            or str(attestation.get("dataset_version", "")).strip() != dataset_version
            or not str(attestation.get("attestation_hash", "")).strip()
        ):
            raise AutonomousResearchError(
                "DATASET_PROVENANCE_INVALID",
                "persisted dataset attestation identity or hash does not match the exact plan binding",
            )
        status = str(attestation.get("status", "")).strip().upper()
        if status != "CURRENT":
            reason = str(attestation.get("reason") or status or "UNKNOWN").strip().upper()
            raise AutonomousResearchError(
                "DATASET_ATTESTATION_STALE",
                f"persisted dataset attestation is not CURRENT ({reason})",
            )
        provenance_payload = {
            "market_type": MarketType.PREDICTION.value,
            "dataset_id": dataset_id,
            "dataset_version": dataset_version,
            "dataset_provenance": {
                "dataset_id": dataset_id,
                "dataset_version": dataset_version,
                "source_type": catalog_source_type,
                "time_split": plan.methodology.get("time_split"),
            },
        }
        quality = evaluate_prediction_data_quality(
            self.store,
            provenance_payload,
            verify_attestation=False,
        )

        reasons = quality.get("reasons", ())
        if (
            quality.get("historical_data_integrity_passed") is not True
            or quality.get("historical_provenance_complete") is not True
            or quality.get("historical_rows_nonempty") is not True
            or quality.get("historical_no_forward_contamination") is not True
        ):
            detail = "; ".join(str(reason) for reason in reasons if str(reason).strip())
            raise AutonomousResearchError(
                "DATASET_PROVENANCE_INVALID",
                detail or f"persisted dataset provenance is invalid for {dataset_id}/{dataset_version}",
            )
        return quality
    def _revalidate_generated_queue_item(
        self,
        item: ResearchQueueItem,
        plan: ExperimentPlan,
        payload: Mapping[str, Any],
    ) -> None:
        """Revalidate attestation only for self-authenticating worker items."""
        internal = _generated_queue_provenance(payload)
        if internal is None:
            return
        if (
            internal.get("schema") != _GENERATED_QUEUE_PROVENANCE_SCHEMA
            or internal.get("generated") is not True
            or str(internal.get("kind", "")).strip() not in _GENERATED_QUEUE_KINDS
        ):
            raise AutonomousResearchError(
                "GENERATED_PROVENANCE_INVALID",
                "generated queue provenance marker is malformed",
            )
        expected_identity = str(internal.get("proposal_identity", "")).strip()
        if not expected_identity or expected_identity != _proposal_identity(payload):
            raise AutonomousResearchError(
                "GENERATED_PROVENANCE_INVALID",
                "generated queue provenance does not match the canonical proposal identity",
            )
        dataset_id = str(plan.dataset_id or "").strip()
        dataset_version = str(plan.dataset_version or "").strip()
        if (
            str(internal.get("dataset_id", "")).strip() != dataset_id
            or str(internal.get("dataset_version", "")).strip() != dataset_version
        ):
            raise AutonomousResearchError(
                "GENERATED_PROVENANCE_INVALID",
                "generated queue provenance dataset binding does not match the canonical plan",
            )
        self._validate_persisted_dataset_provenance(plan)
        attestation_loader = getattr(self.store, "load_dataset_integrity_attestation", None)
        if not callable(attestation_loader):
            raise AutonomousResearchError(
                "DATASET_ATTESTATION_MISSING",
                f"generated queue item has no persisted dataset attestation for {dataset_id}/{dataset_version}",
            )
        try:
            attestation = attestation_loader(dataset_id, dataset_version)
        except Exception as exc:
            raise AutonomousResearchError(
                "DATASET_ATTESTATION_MISSING",
                f"generated queue item attestation could not be loaded for {dataset_id}/{dataset_version}",
            ) from exc
        current_hash = (
            str(attestation.get("attestation_hash", "")).strip()
            if isinstance(attestation, Mapping)
            else ""
        )
        expected_hash = str(internal.get("attestation_hash", "")).strip()
        if not expected_hash or current_hash != expected_hash:
            raise AutonomousResearchError(
                "DATASET_ATTESTATION_CHANGED",
                "generated queue item attestation changed after enqueue",
            )

    def _load_prediction_dataset_by_version(self, version: str) -> Any | None:
        """Load a datasetless prediction plan only when its version is unambiguous."""
        connection = getattr(self.store, "connection", None)
        execute = getattr(connection, "execute", None)
        if callable(execute):
            try:
                matches = execute(
                    "SELECT dataset_id, 'datasets' AS source FROM datasets WHERE version=? "
                    "UNION ALL "
                    "SELECT dataset_id, 'dataset_catalog' AS source "
                    "FROM dataset_catalog WHERE dataset_version=? "
                    "ORDER BY dataset_id, source",
                    (str(version), str(version)),
                ).fetchall()
            except Exception as exc:
                raise AutonomousResearchError(
                    "INSUFFICIENT_DATA",
                    f"immutable dataset lookup failed for version {version}",
                ) from exc
            if len(matches) > 1:
                identities = ", ".join(f"{row[1]}:{row[0]}" for row in matches)
                raise AutonomousResearchError(
                    "INSUFFICIENT_DATA",
                    f"ambiguous immutable datasets at version {version}: {identities}",
                )
            if not matches:
                return None
            dataset_id = str(matches[0][0]).strip()
            loader = getattr(self.store, "load_dataset", None)
            if callable(loader):
                return loader(dataset_id, version)
            return None

        finder = getattr(self.store, "load_dataset_by_version", None)
        if callable(finder):
            found = finder(version)
            if isinstance(found, Mapping):
                found_version = str(found.get("version", found.get("dataset_version", ""))).strip()
                if found_version == str(version):
                    return found.get("records")
        return None

    def _load_split(
        self,
        plan: ExperimentPlan,
        *,
        boundary_override: Mapping[str, Any] | None = None,
    ) -> tuple[list[Mapping[str, Any]], Any]:
        records: Any = None
        if plan.dataset_version.lower() in {"latest", "current", "default", "unversioned"}:
            if plan.market_type is MarketType.CRYPTO_SPOT:
                raise AutonomousResearchError(
                    "INSUFFICIENT_DATA",
                    "crypto dataset_version must be immutable and versioned",
                )
            raise AutonomousResearchError(
                "INSUFFICIENT_DATA",
                "dataset_version must identify an immutable version",
            )
        if plan.market_type is MarketType.CRYPTO_SPOT:
            self._crypto_binding(plan)
            if not plan.dataset_id:
                raise AutonomousResearchError(
                    "INSUFFICIENT_DATA",
                    "crypto_spot research requires a versioned dataset_id",
                )
            loader = getattr(self.store, "load_dataset_record", None)
            if callable(loader):
                record = loader(plan.dataset_id, plan.dataset_version)
                if isinstance(record, Mapping):
                    record_version = str(record.get("version", record.get("dataset_version", ""))).strip()
                    if record_version and record_version != plan.dataset_version:
                        raise AutonomousResearchError(
                            "CRYPTO_PROVENANCE_MISMATCH",
                            "dataset record version does not match the plan",
                        )
                    records = record.get("records")
            if records is None:
                records = self.store.load_dataset(plan.dataset_id, plan.dataset_version)
        elif plan.dataset_id:
            records = self.store.load_dataset(plan.dataset_id, plan.dataset_version)
        else:
            records = self._load_prediction_dataset_by_version(plan.dataset_version)
        if records is None:
            raise AutonomousResearchError(
                "INSUFFICIENT_DATA",
                f"no immutable dataset {plan.dataset_id or '*'} at version {plan.dataset_version}",
            )
        if isinstance(records, Mapping):
            records = records.get("records", records.get("rows", records.get("observations", ())))
        if not isinstance(records, (list, tuple)):
            raise AutonomousResearchError("INSUFFICIENT_DATA", "dataset records are not a bounded sequence")
        rows = [_normalize_row(item) for item in list(records)[:_MAX_DATASET_ROWS]]
        rows = [row for row in rows if row is not None]
        boundary = boundary_override if boundary_override is not None else plan.dataset_boundary
        expected_digest = (
            str(boundary.get("ordered_row_manifest_digest", boundary.get("content_hash", ""))).strip()
            if isinstance(boundary, Mapping)
            else ""
        )
        if expected_digest:
            boundary_ordered = sorted(
                rows,
                key=lambda row: (
                    parse_timestamp(row.get("timestamp", row.get("source_timestamp")))
                    or datetime.min.replace(tzinfo=timezone.utc),
                    _campaign_row_identity(row, 0),
                    _hash_document(row),
                ),
            )
            actual_provenance = _campaign_compact_row_provenance(boundary_ordered)
            if (
                actual_provenance["row_count"] != int(boundary.get("row_count", -1))
                or actual_provenance["exact_cutoff"] != boundary.get("exact_cutoff")
                or actual_provenance["ordered_row_manifest_digest"] != expected_digest
            ):
                raise AutonomousResearchError(
                    "DATASET_PROVENANCE_INVALID",
                    "dataset rows changed after the campaign boundary was locked",
                )
        rows = self._apply_plan_filters(plan, rows)
        rows = self._apply_model_document(plan, rows)
        if not rows:
            # A schema-valid paper intent must remain durable when the immutable
            # dataset is empty after scope/model filters.  Keep the historical
            # partition empty; no observations or qualification are inferred.
            empty_start = datetime(1970, 1, 1, tzinfo=timezone.utc)
            return [], split_dataset(
                (),
                empty_start,
                empty_start + timedelta(microseconds=1),
                empty_start + timedelta(microseconds=2),
                dataset_version=plan.dataset_version,
                require_nonempty=False,
            )
        for feature in plan.allowed_features:
            if feature in {"timestamp", "market_id", "symbol", "expiry", "settlement", "question", "resolution_criteria"}:
                continue
            if not any(_value(row, feature) is not None for row in rows):
                raise AutonomousResearchError("INSUFFICIENT_DATA", f"dataset has no values for feature {feature}")
        stamps = [parse_timestamp(_value(row, "timestamp")) for row in rows]
        if any(stamp is None for stamp in stamps):
            raise AutonomousResearchError("INSUFFICIENT_DATA", "dataset contains rows without timestamps")
        ordered = [
            row
            for _, row in sorted(
                zip(stamps, rows),
                key=lambda pair: (pair[0], _value(pair[1], "market_id", "")),
            )
        ]

        def sparse_split() -> Any:
            # Preserve the immutable rows even when there are too few
            # distinct time boundaries to form three non-empty partitions.
            first_stamp = parse_timestamp(_value(ordered[0], "timestamp"))
            if first_stamp is None:
                raise AutonomousResearchError("INSUFFICIENT_DATA", "dataset timestamps are invalid")
            return split_dataset(
                ordered,
                first_stamp + timedelta(microseconds=1),
                first_stamp + timedelta(microseconds=2),
                first_stamp + timedelta(microseconds=3),
                dataset_version=plan.dataset_version,
                require_nonempty=False,
            )

        if len(ordered) < 3:
            # Keep the immutable rows and their chronological partition even
            # when historical evidence is too small to backtest.  The caller
            # records the already-registered paper intent as SCHEMA_VALIDATED;
            # no historical metrics or forward authority are inferred here.
            return ordered, sparse_split()
        train_count = max(1, int(len(ordered) * 0.60))
        validation_count = max(1, int(len(ordered) * 0.20))
        if train_count + validation_count >= len(ordered):
            train_count = max(1, len(ordered) - 2)
            validation_count = 1
        train_end = parse_timestamp(_value(ordered[train_count], "timestamp"))
        validation_end = parse_timestamp(_value(ordered[train_count + validation_count], "timestamp"))
        holdout_end = parse_timestamp(_value(ordered[-1], "timestamp"))
        if train_end is None or validation_end is None or holdout_end is None:
            raise AutonomousResearchError("INSUFFICIENT_DATA", "dataset timestamps are invalid")
        split = split_dataset(
            ordered,
            train_end,
            validation_end,
            holdout_end + timedelta(microseconds=1),
            dataset_version=plan.dataset_version,
            require_nonempty=False,
        )
        if not split.train or not split.validation or not split.holdout:
            return ordered, sparse_split()
        # The locked holdout is evaluated once after the complete trial set is
        # fixed. It never feeds selection, mutation, Hermes, or qualification.
        return ordered, split

    def _apply_plan_filters(self, plan: ExperimentPlan, rows: Sequence[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
        # Historical qualification is bound only to the plan's explicit
        # top-level filters; current market-scope filters belong to forward
        # resolution and must not remove PRICE_PROXY history rows.
        filters = dict(plan.filters)
        restrictions = dict(plan.regime_restrictions)
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
        unknown = sorted(set(filters) - supported)
        if unknown:
            raise AutonomousResearchError("UNSUPPORTED_FEATURE", f"unsupported plan filters: {unknown}")
        supported_restrictions = {"regime", "regimes", "allowed_regimes", "allowed_states"}
        unknown_restrictions = sorted(set(restrictions) - supported_restrictions)
        if unknown_restrictions:
            raise AutonomousResearchError(
                "UNSUPPORTED_FEATURE",
                f"unsupported regime restrictions: {unknown_restrictions}",
            )
        allowed_regimes = filters.get("regime", filters.get("regimes"))
        restricted_regimes = restrictions.get(
            "allowed_regimes",
            restrictions.get("allowed_states", restrictions.get("regime", restrictions.get("regimes"))),
        )
        if allowed_regimes is not None and restricted_regimes is not None:
            allowed_regimes = _as_regime_set(allowed_regimes, "regime filter") & _as_regime_set(
                restricted_regimes,
                "regime restrictions",
            )
        elif allowed_regimes is None:
            allowed_regimes = restricted_regimes
        elif allowed_regimes is not None:
            allowed_regimes = _as_regime_set(allowed_regimes, "regime filter")
        target_markets = set(plan.target_markets)
        target_instrument = plan.target_instrument
        target_instrument_key = _normal_symbol(target_instrument) if target_instrument else ""
        historical_polymarket_scope = (
            plan.market_type is MarketType.PREDICTION and target_instrument_key == "POLYMARKET"
        )
        result: list[Mapping[str, Any]] = []
        for row in rows:
            market_id = str(_value(row, "market_id", "")).strip()
            if target_markets and market_id not in target_markets:
                continue
            if historical_polymarket_scope:
                # Historical price-history rows may omit venue identity.  The
                # exact dataset selector and its immutable attestation bind
                # those rows; an explicitly supplied identity still has to
                # agree with the canonical Polymarket scope.
                row_symbol = _binding_value(_value(row, "symbol"))
                row_instrument = _binding_value(_value(row, "instrument"))
                if row_symbol is not None and _normal_symbol(row_symbol) != target_instrument_key:
                    continue
                if row_instrument is not None and _normal_symbol(row_instrument) != target_instrument_key:
                    continue
            elif target_instrument:
                symbol = str(_value(row, "symbol", _value(row, "instrument", ""))).strip()
                if target_instrument not in {market_id, symbol}:
                    continue
            if "category" in filters:
                actual_category = str(_value(row, "category", "") or "").strip().casefold()
                expected_category = filters["category"]
                expected_categories = (
                    expected_category
                    if isinstance(expected_category, (list, tuple, set))
                    else (expected_category,)
                )
                if actual_category not in {str(item).strip().casefold() for item in expected_categories if str(item).strip()}:
                    continue
            price = _finite(_value(row, "yes_mid", _value(row, "yes_ask")), math.nan)
            if "entry_price" in filters and not _in_bound(price, filters["entry_price"]):
                continue
            seconds = _time_to_expiry(row)
            if "minimum_hours_to_resolution" in filters:
                minimum = _finite(filters["minimum_hours_to_resolution"], math.nan) * 3600.0
                if not math.isfinite(seconds) or seconds < minimum:
                    continue
            if "maximum_hours_to_resolution" in filters:
                maximum = _finite(filters["maximum_hours_to_resolution"], math.nan) * 3600.0
                if not math.isfinite(seconds) or seconds > maximum:
                    continue
            liquidity = _finite(_value(row, "liquidity"), math.nan)
            if "min_liquidity" in filters and (not math.isfinite(liquidity) or liquidity < _finite(filters["min_liquidity"], math.inf)):
                continue
            spread = _finite(_value(row, "spread"), math.nan)
            if "max_spread" in filters and (not math.isfinite(spread) or spread > _finite(filters["max_spread"], -math.inf)):
                continue
            if allowed_regimes is not None:
                regime = _value(row, "regime", _value(row, "regime_state"))
                if str(regime) not in allowed_regimes:
                    continue
            result.append(row)
        return result

    @staticmethod
    def _apply_model_document(plan: ExperimentPlan, rows: Sequence[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
        model = plan.model_for()
        if model is None:
            return list(rows)
        result: list[Mapping[str, Any]] = []
        for row in rows:
            clean = dict(row)
            probability = evaluate_model_document_probability(model, clean)
            if probability is not None:
                clean["model_probability"] = probability
            result.append(clean)
        return result

    def _evaluate_datasets(
        self,
        plan: ExperimentPlan,
        strategy: StrategyDefinition,
        train: Sequence[Mapping[str, Any]],
        validation: Sequence[Mapping[str, Any]],
        holdout: Sequence[Mapping[str, Any]] = (),
    ) -> dict[str, Mapping[str, Any]]:
        result: dict[str, Mapping[str, Any]] = {
            "train": self._run_backtest(plan, strategy, train),
            "validation": self._run_backtest(plan, strategy, validation),
        }
        if holdout:
            # This is an attested, chronological review only.  It is not part
            # of variant selection, mutation, or canary qualification.
            result["holdout"] = self._run_backtest(plan, strategy, holdout)
        return result

    @staticmethod
    def _run_backtest(plan: ExperimentPlan, strategy: StrategyDefinition, rows: Sequence[Mapping[str, Any]]) -> Mapping[str, Any]:
        methodology = plan.methodology
        assumptions = plan.assumptions if isinstance(plan.assumptions, Mapping) else {}
        initial_cash = _finite(methodology.get("initial_cash"), 10_000.0)
        fee_bps = _finite(assumptions.get("fee_bps"), _finite(methodology.get("fee_bps"), 0.0))
        slippage_bps = _finite(assumptions.get("slippage_bps"), _finite(methodology.get("slippage_bps"), 0.0))
        allocation = _finite(methodology.get("allocation"), 0.25)
        if (
            not math.isfinite(initial_cash)
            or initial_cash <= 0
            or not math.isfinite(fee_bps)
            or fee_bps < 0
            or not math.isfinite(slippage_bps)
            or slippage_bps < 0
        ):
            raise AutonomousResearchError("INVALID_PLAN", "cost assumptions must be finite and non-negative")
        holding_period = 1
        research_mode: str | None = None
        exit_policy: Mapping[str, Any] | None = None
        if plan.market_type is MarketType.PREDICTION:
            research_mode = str(plan.research_mode or methodology.get("research_mode") or "PRICE_PROXY_RESEARCH").strip().upper()
            exit_policy = dict(plan.exit_policy) if isinstance(plan.exit_policy, Mapping) else {
                "type": "fixed_holding_period",
                "holding_period": 1,
            }
            holding_period = exit_policy.get("holding_period")
            if isinstance(holding_period, bool) or not isinstance(holding_period, int) or holding_period < 1:
                raise AutonomousResearchError("INVALID_PLAN", "exit_policy.holding_period must be a bounded positive integer")
            result = run_prediction_research_mode(
                rows,
                strategy,
                mode=research_mode,
                initial_cash=initial_cash,
                fee_bps=fee_bps,
                slippage_bps=slippage_bps,
                allocation=max(0.0, min(1.0, allocation)),
                resolutions=None,
                model_document=plan.model_document,
                holding_period=holding_period,
                observation_horizon=(
                    dict(plan.observation_horizon)
                    if isinstance(plan.observation_horizon, Mapping)
                    else {"unit": "observations", "count": holding_period}
                ),
                exit_policy=exit_policy,
            )
        else:
            result = CryptoBacktester(
                initial_cash=initial_cash,
                fee_bps=fee_bps,
                slippage_bps=slippage_bps,
                allocation=max(0.0, min(1.0, allocation)),
                symbol=plan.target_instrument or "ASSET",
            ).run(rows, strategy, symbol=plan.target_instrument or "ASSET")
        metrics = {str(key): _finite(value, 0.0) for key, value in result.metrics.items() if _is_finite_number(value)}
        settled = len(result.outcomes)
        if plan.market_type is MarketType.PREDICTION:
            expectancy = metrics.get("roi", 0.0) if settled else metrics.get("expected_value", metrics.get("roi", 0.0))
        else:
            expectancy = metrics.get("expectancy", metrics.get("total_return", 0.0))
        curve_values = [_finite(item.get("equity"), 0.0) for item in result.equity_curve if isinstance(item, Mapping)]
        returns = [current / previous - 1.0 for previous, current in zip(curve_values, curve_values[1:]) if previous > 0]
        confidence = bootstrap_confidence_interval(
            returns or [expectancy],
            resamples=min(256, max(32, len(returns or [expectancy]) * 8)),
            seed=0,
        )
        sample_count = len(rows)
        independent = len({str(_value(row, "market_id", _value(row, "symbol", index))) for index, row in enumerate(rows)})
        liquidity_values = [_finite(_value(row, "liquidity"), math.nan) for row in rows]
        liquidity_values = [value for value in liquidity_values if math.isfinite(value)]
        regimes = {
            str(_value(row, "regime", _value(row, "regime_state", "unknown")))
            for row in rows
            if _value(row, "regime", _value(row, "regime_state")) is not None
        }
        result_quality = result.quality.value if hasattr(result.quality, "value") else str(result.quality)
        summary = {
            "sample_count": sample_count,
            "independent_samples": independent,
            "filled_trades": len(result.fills),
            "settled_markets": settled,
            "expectancy": expectancy,
            "roi": metrics.get("roi", metrics.get("total_return", 0.0)),
            "max_drawdown": metrics.get("max_drawdown", 0.0),
            "calibration": max(0.0, min(1.0, 1.0 - metrics.get("ece", 0.0))) if settled else 0.0,
            "liquidity": mean(liquidity_values) if liquidity_values else 0.0,
            "regime_count": len(regimes) if regimes else (1 if rows else 0),
            "quality": result_quality,
            "assumption_version": assumptions.get("version"),
            "cost_assumptions": {
                "fee_bps": fee_bps,
                "slippage_bps": slippage_bps,
            },
            "exit_policy": dict(exit_policy) if exit_policy is not None else None,
            "observation_horizon": (
                dict(plan.observation_horizon)
                if isinstance(plan.observation_horizon, Mapping)
                else {"unit": "observations", "count": holding_period}
            ),
            "research_mode": research_mode,
            "execution_simulation": False,
            "costs": metrics.get("fees", 0.0) + metrics.get("slippage", 0.0),
            "confidence_lower_bound": _finite(confidence.get("lower"), 0.0),
            "confidence_interval": {
                "lower": _finite(confidence.get("lower"), 0.0),
                "upper": _finite(confidence.get("upper"), 0.0),
                "count": int(confidence.get("count", 0)),
            },
            "metrics": metrics,
            "outcomes": settled,
        }
        return summary

    def _advance_candidate(
        self,
        plan: ExperimentPlan,
        candidate_id: str,
        strategy: StrategyDefinition,
        evaluation: Mapping[str, Mapping[str, Any]],
        *,
        variant_count: int,
        validation_scores: Mapping[str, float],
        now: datetime,
        generation: int,
        lineage: Sequence[str],
        crypto_binding: Mapping[str, Any] | None = None,
    ) -> Mapping[str, Any]:
        if plan.market_type is MarketType.CRYPTO_SPOT and crypto_binding is None:
            crypto_binding = self._crypto_binding(plan)
        candidate = self.lifecycle.get(candidate_id)
        if candidate is None:
            raise AutonomousResearchError("INVALID_CANDIDATE", f"candidate {candidate_id} is not registered")
        if candidate.stage is CandidateStage.REJECTED:
            return {"candidate_id": candidate_id, "stage": candidate.stage.value, "reason": candidate.rejection_reason}
        if candidate.stage in {CandidateStage.PAPER_FORWARD, CandidateStage.PAPER_PROMOTABLE}:
            return {"candidate_id": candidate_id, "stage": candidate.stage.value}
        base = {
            "candidate_id": candidate_id,
            "hypothesis_id": plan.hypothesis_id,
            "plan_id": plan.plan_id,
            **_scope_binding(plan),
            **_strategy_metadata(strategy),
            "plan_hash": plan.plan_hash,
            "dataset_id": plan.dataset_id,
            "dataset_version": plan.dataset_version,
            "dataset_provenance": {
                "dataset_id": plan.dataset_id,
                "dataset_version": plan.dataset_version,
                "time_split": plan.methodology.get("time_split"),
                "universe": dict(plan.universe or {}),
            },
            "assumptions": dict(plan.assumptions),
            "cost_assumptions": {
                "fee_bps": plan.assumptions.get("fee_bps"),
                "slippage_bps": plan.assumptions.get("slippage_bps"),
                "roundtrip_fee_bps": plan.assumptions.get("roundtrip_fee_bps"),
                "roundtrip_slippage_bps": plan.assumptions.get("roundtrip_slippage_bps"),
                "sensitivity": plan.assumptions.get("cost_sensitivity"),
            },
            "exit_policy": dict(plan.exit_policy),
            "research_mode": plan.research_mode,
            "experiment_family": plan.experiment_family,
            "generation": generation,
            "lineage": list(dict.fromkeys([*(str(value) for value in lineage), *([candidate_id] if generation else [])])),
            "market_type": plan.market_type.value,
            "paper_only": True,
            "holdout_used": False,
        }
        if crypto_binding is not None:
            base["crypto_provenance"] = dict(crypto_binding)
        if candidate.stage is CandidateStage.IDEA:
            candidate = self.lifecycle.advance(
                candidate_id,
                CandidateStage.SCHEMA_VALIDATED,
                {
                    **base,
                    "schema_valid": True,
                    "referenced_features": list(plan.allowed_features),
                    "parameter_ranges_bounded": True,
                },
                reason="declarative experiment schema validated",
            )
        train = dict(evaluation["train"])
        validation = dict(evaluation["validation"])
        if candidate.stage is CandidateStage.SCHEMA_VALIDATED:
            candidate = self.lifecycle.advance(
                candidate_id,
                CandidateStage.BACKTESTED,
                {
                    **base,
                    "backtest_complete": True,
                    "train": _compact_evidence(train),
                    "costs_included": True,
                    "raw_observations": train.get("sample_count", 0),
                    "data_quality": train.get("quality"),
                },
                reason="bounded historical simulation completed with costs",
            )
        validation_expectancy = _finite(validation.get("expectancy"), 0.0)
        if validation_expectancy < 0.0:
            self.lifecycle.reject(
                candidate_id,
                "negative_validation_expectancy",
                evidence={
                    **base,
                    "validation_complete": True,
                    "validation": _compact_evidence(validation),
                    "benchmark_comparison": {"baseline_expectancy": 0.0, "delta": validation_expectancy},
                },
                expected_stage=candidate.stage,
                expected_payload=candidate.payload,
            )
            return {
                "candidate_id": candidate_id,
                "reason": "negative_validation_expectancy",
                "reason_code": "NEGATIVE_VALIDATION_EXPECTANCY",
            }
        if candidate.stage is CandidateStage.BACKTESTED:
            candidate = self.lifecycle.advance(
                candidate_id,
                CandidateStage.VALIDATED,
                {
                    **base,
                    "validation_complete": True,
                    "validation": _compact_evidence(validation),
                    "validation_expectancy": validation_expectancy,
                    "benchmark_comparison": {"baseline_expectancy": 0.0, "delta": validation_expectancy},
                },
                reason="chronological validation completed without locked feedback",
            )
        stability = neighboring_parameter_stability(validation_scores)
        stability_value = _finite(stability.get("stable_fraction"), 0.0)
        sample_check = minimum_sample_check(
            int(validation.get("sample_count", 0)),
            trades=int(validation.get("filled_trades", 0)),
            min_observations=plan.min_samples,
            min_trades=plan.min_trades,
        )
        data_quality = train.get("quality", validation.get("quality"))
        policy_quality = evaluate_prediction_data_quality(
            self.store,
            {
                **base,
                "data_quality": data_quality,
                "validation_execution_quality": validation.get("quality"),
            },
        )
        if policy_quality.get("applicable"):
            quality_passed = bool(policy_quality.get("canary_data_quality_acceptable"))
        else:
            if isinstance(data_quality, Mapping):
                quality_passed = data_quality.get("passed")
                if not isinstance(quality_passed, bool):
                    quality_passed = str(
                        data_quality.get("label", data_quality.get("quality", ""))
                    ).strip().upper() in {"HIGH", "MEDIUM", "GOOD", "PASS", "PASSED"}
            else:
                quality_passed = str(data_quality or "").strip().upper() in {
                    "HIGH", "MEDIUM", "GOOD", "PASS", "PASSED",
                }
        robust_evidence = {
            **base,
            "data_quality": data_quality,
            "data_quality_passed": bool(quality_passed),
            **persisted_quality_fields(policy_quality),
            "robustness_passed": bool(sample_check["passed"] and stability_value >= 0.60 and validation_expectancy >= 0.0),
            "minimum_sample_check": sample_check,
            "validation_stability": stability_value,
            "validation_stability_summary": stability,
            "multiple_testing": {
                "variants_tested": variant_count,
                "selected_from_variants": variant_count,
                "selection_metric": "validation_expectancy",
                "locked_partition_used_for_selection": False,
            },
            "validation_regime_behavior": {"regimes": validation.get("regime_count", 0)},
            "validation_execution_quality": validation.get("quality"),
            "validation_confidence_interval": validation.get("confidence_interval"),
            "validation_expectancy": validation_expectancy,
            "validation_confidence_lower_bound": _finite(validation.get("confidence_lower_bound"), 0.0),
            "validation_calibration": _finite(validation.get("calibration"), 0.0),
        }
        if candidate.stage is CandidateStage.VALIDATED:
            if not sample_check["passed"]:
                self.lifecycle.reject(
                    candidate_id,
                    "INSUFFICIENT_DATA",
                    evidence=robust_evidence,
                    expected_stage=candidate.stage,
                    expected_payload=candidate.payload,
                )
                return {
                    "candidate_id": candidate_id,
                    "reason": "INSUFFICIENT_DATA",
                    "reason_code": "INSUFFICIENT_DATA",
                }
            if stability_value < 0.60:
                self.lifecycle.reject(
                    candidate_id,
                    "unstable_neighbor_parameters",
                    evidence=robust_evidence,
                    expected_stage=candidate.stage,
                    expected_payload=candidate.payload,
                )
                return {
                    "candidate_id": candidate_id,
                    "reason": "unstable_neighbor_parameters",
                    "reason_code": "UNSTABLE_NEIGHBOR_PARAMETERS",
                }
            candidate = self.lifecycle.advance(
                candidate_id,
                CandidateStage.ROBUSTNESS_CHECKED,
                robust_evidence,
                reason="bounded validation robustness checks completed",
            )
        if candidate.stage is CandidateStage.ROBUSTNESS_CHECKED and bool(base.get("selection_excluded")):
            return {
                "candidate_id": candidate_id,
                "stage": candidate.stage.value,
                "validation_expectancy": validation_expectancy,
                "variant_count": variant_count,
                "forward_test_id": None,
                "research_only": True,
                "paper_only": True,
                **_strategy_metadata(strategy),
            }
        if candidate.stage is CandidateStage.ROBUSTNESS_CHECKED:
            model_document = plan.model_for() or {"type": "deterministic"}
            dataset_attestation = self._dataset_attestation(
                plan,
                strict=generation > 0 and plan.market_type is MarketType.PREDICTION,
            )
            if plan.market_type is MarketType.CRYPTO_SPOT:
                # Crypto autonomous candidates are historical research only.
                # They are deliberately never registered with ForwardTestRegistry.
                risk_snapshot = {"execution_capability": "none", "max_position_fraction": 0.0}
                forward_config = {
                    "paper_only": True,
                    "execution_capability": "none",
                    "plan_id": plan.plan_id,
                    "dataset_id": plan.dataset_id,
                    "dataset_version": plan.dataset_version,
                    "strategy_document": strategy.to_dict(),
                    "model_document": dict(model_document),
                }
            else:
                risk_snapshot = {"max_position_fraction": 0.05}
                forward_config = {
                    "execution": "paper_only",
                    "market_authority_required": True,
                    "plan_id": plan.plan_id,
                    "dataset_id": plan.dataset_id,
                    "dataset_version": plan.dataset_version,
                    "strategy_document": strategy.to_dict(),
                    "model_document": dict(model_document),
                }
            forward_config["candidate_id"] = candidate_id
            forward_config["observation_intent"] = True
            forward_config.update(_scope_binding(plan))
            forward_config["assumptions"] = dict(plan.assumptions)
            forward_config["cost_assumptions"] = {
                "fee_bps": plan.assumptions.get("fee_bps"),
                "slippage_bps": plan.assumptions.get("slippage_bps"),
                "roundtrip_fee_bps": plan.assumptions.get("roundtrip_fee_bps"),
                "roundtrip_slippage_bps": plan.assumptions.get("roundtrip_slippage_bps"),
                "sensitivity": plan.assumptions.get("cost_sensitivity"),
            }
            forward_config["exit_policy"] = dict(plan.exit_policy)
            forward_config["research_mode"] = plan.research_mode
            config_hash = _hash_document({"config": forward_config, "risk_limits": risk_snapshot})
            strategy_hash = _content_hash(strategy.to_dict())
            model_hash = _content_hash(model_document)
            frozen_hash = hashlib.sha256("|".join((strategy_hash, model_hash, config_hash)).encode("utf-8")).hexdigest()
            candidate = self.lifecycle.advance(
                candidate_id,
                CandidateStage.FROZEN,
                {
                    **robust_evidence,
                    **base,
                    "frozen": True,
                    "strategy_hash": strategy_hash,
                    "model_hash": model_hash,
                    "config_hash": config_hash,
                    "frozen_hash": frozen_hash,
                    "risk_snapshot": risk_snapshot,
                    "dataset_provenance": {
                        "dataset_id": plan.dataset_id,
                        "dataset_version": plan.dataset_version,
                        "time_split": plan.methodology.get("time_split"),
                        "universe": dict(plan.universe or {}),
                    },
                    "dataset_attestation": dict(dataset_attestation),
                    "experiment_budget_lineage": {
                        "budget_id": AUTONOMOUS_BUDGET_ID,
                        "family": plan.experiment_family,
                        "variants_tested": variant_count,
                    },
                },
                reason="exact strategy, model, configuration, risk, and provenance frozen",
            )
            if plan.market_type is MarketType.CRYPTO_SPOT:
                return {
                    "candidate_id": candidate_id,
                    "stage": candidate.stage.value,
                    "validation_expectancy": validation_expectancy,
                    "variant_count": variant_count,
                    "forward_test_id": None,
                    "research_only": True,
                    "execution_capability": "none",
                    "paper_only": True,
                    "crypto_provenance": dict(crypto_binding or {}),
                }
            required_independent_samples = self.config.promotion_criteria.min_independent_samples
            # A zero explicit threshold means no independent-sample
            # qualification bound; it must not collapse current authority to
            # one market.  Runtime defaults still bind authority to their
            # required sample count, bounded by the global safety cap.
            authority_market_cap = min(
                PAPER_MARKET_AUTHORITY_CAP,
                required_independent_samples or PAPER_MARKET_AUTHORITY_CAP,
            )
            authority = self.store.candidate_forward_requirements(
                candidate_ids=(candidate_id,),
                now=now,
                max_candidates=1,
                max_markets_per_candidate=authority_market_cap,
                max_total_markets=authority_market_cap,
            )
            authority_candidate = (
                authority.get("candidates", [])[0]
                if isinstance(authority.get("candidates"), list) and authority.get("candidates")
                else {}
            )
            allowed_markets = tuple(
                str(item).strip()
                for item in authority_candidate.get("permitted_market_ids", ())
                if str(item).strip()
            )
            if (
                authority_candidate.get("resolution") != "RESOLVED"
                or not allowed_markets
            ):
                return {
                    "candidate_id": candidate_id,
                    "stage": candidate.stage.value,
                    "validation_expectancy": validation_expectancy,
                    "variant_count": variant_count,
                    "forward_test_id": None,
                    "reason_code": authority_candidate.get(
                        "reason_code", "CANDIDATE_FORWARD_MARKET_UNRESOLVED"
                    ),
                    "research_only": True,
                    "paper_only": True,
                }

            registry = ForwardTestRegistry(self.store)
            spec = registry.register_forward_test(
                strategy=strategy.to_dict(),
                model=dict(model_document),
                registration_timestamp=now,
                now=now,
                config=forward_config,
                bankroll=10_000.0,
                allowed_markets=allowed_markets,
                risk_limits=risk_snapshot,
                experiment_id="forward-" + candidate_id,
            )
            candidate = self.lifecycle.advance(
                candidate_id,
                CandidateStage.PAPER_FORWARD,
                {
                    **base,
                    "paper_forward_started": True,
                    "forward_test_id": spec.experiment_id,
                    # Preserve the registry's real current-time provenance on
                    # the lifecycle record; never reconstruct it from history.
                    "registration_timestamp": spec.registration_timestamp.isoformat(),
                    "forward_evidence": {
                        "evidence_scope": "forward_only",
                        "forward_order_attempts": 0,
                        "forward_successful_order_attempts": 0,
                        "forward_failed_order_attempts": 0,
                        "fills": 0,
                        "partial_fills": 0,
                        "no_fill_orders": 0,
                        "forward_independent_resolved_bets": 0,
                        "forward_expectancy": None,
                        "forward_confidence_lower_bound": None,
                        "forward_stability": None,
                        "forward_calibration": None,
                        "forward_max_drawdown": 0.0,
                        "paper_only": True,
                    },
                },
                reason="genuine forward registration created at current time",
            )
        return {
            "candidate_id": candidate_id,
            "stage": candidate.stage.value,
            "validation_expectancy": validation_expectancy,
            "variant_count": variant_count,
            "forward_test_id": candidate.payload.get("forward_test_id"),
        }

    def _generate_mutations(
        self,
        plan: ExperimentPlan,
        prepared: Sequence[Mapping[str, Any]],
        validation_scores: Mapping[str, float],
        now: datetime,
        *,
        lineage: Sequence[str] = (),
    ) -> tuple[str, ...]:
        if not self.config.mutation_enabled or self.config.max_children_per_parent <= 0:
            return ()
        if not prepared:
            return ()
        current = ensure_utc(now)
        daily_limit = self.config.max_experiments_per_day
        daily_since: datetime | None = None
        daily_until: datetime | None = None
        if daily_limit is not None:
            daily_since = datetime(current.year, current.month, current.day, tzinfo=timezone.utc)
            daily_until = daily_since + timedelta(days=1)
        parent = max(prepared, key=lambda item: (validation_scores.get(str(item["candidate_id"]), float("-inf")), str(item["candidate_id"])))
        parent_id = str(parent["candidate_id"])
        lifecycle = self.lifecycle.get(parent_id)
        if lifecycle is None or lifecycle.stage not in {CandidateStage.ROBUSTNESS_CHECKED, CandidateStage.FROZEN, CandidateStage.PAPER_FORWARD, CandidateStage.PAPER_PROMOTABLE}:
            return ()
        generation = int(lifecycle.payload.get("generation", 0))
        if generation >= self.config.max_generation_depth:
            return ()
        existing = self.store.load_candidate_lifecycle(limit=10_000)
        children = 0
        if isinstance(existing, list):
            children = sum(
                1
                for record in existing
                if isinstance(record, Mapping)
                and isinstance(record.get("payload"), Mapping)
                and str(record["payload"].get("parent_id", "")) == parent_id
            )
        remaining = max(0, self.config.max_children_per_parent - children)
        if daily_limit is not None:
            assert daily_since is not None and daily_until is not None
            daily_count = self.store.count_experiment_budget_reservations(
                AUTONOMOUS_BUDGET_ID,
                since=daily_since,
                until=daily_until,
            )
            remaining = min(remaining, max(0, daily_limit - daily_count))
        if remaining <= 0:
            return ()
        strategy = parent["strategy"]
        # Experiment plans are untrusted input.  The node owns the shared
        # autonomous budget namespace; never let plan-declared limits poison
        # its persisted immutable limits.
        budget = ExperimentBudget(
            budget_id=AUTONOMOUS_BUDGET_ID,
            total_limit=self.config.total_limit,
            per_family_limit=self.config.family_limit,
        )
        engine = DeterministicMutationEngine(store=self.store, lifecycle=self.lifecycle, budget=budget, seed=0)
        try:
            generated = engine.mutate(
                strategy,
                parent_id=parent_id,
                generation=generation + 1,
                max_variants=remaining,
                provenance={
                    "plan_id": plan.plan_id,
                    "hypothesis_id": plan.hypothesis_id,
                    "validation_only": True,
                    "market_scope_hash": plan.market_scope_hash,
                    "market_scope": plan.market_scope.as_dict(),
                    "market_scope_version": plan.market_scope_version,
                    "dataset_selector": dict(plan.as_dict()["dataset_selector"]),
                    "locked_partition_used": False,
                    "crypto_provenance": dict(self._crypto_binding(plan) or {}),
                },
                lineage=lineage,
                timestamp=now,
                daily_limit=daily_limit,
                daily_since=daily_since,
                daily_until=daily_until,
            )
        except (RuntimeError, ValueError):
            return ()
        child_ids: list[str] = []
        attestation_hash: str | None = None
        if plan.market_type is MarketType.PREDICTION:
            attestation_loader = getattr(self.store, "load_dataset_integrity_attestation", None)
            if callable(attestation_loader) and plan.dataset_id and plan.dataset_version:
                try:
                    attestation = attestation_loader(plan.dataset_id, plan.dataset_version)
                except Exception:
                    attestation = None
                if isinstance(attestation, Mapping):
                    attestation_hash = str(attestation.get("attestation_hash", "")).strip() or None
        for child in generated:
            child_document = child.strategy.to_dict()
            child_document["strategy_id"] = child.candidate_id
            child_strategy = load_strategy(child_document)
            child_payload = {
                "candidate_id": child.candidate_id,
                "hypothesis_id": plan.hypothesis_id,
                "plan_id": plan.plan_id,
                "plan_hash": plan.plan_hash,
                **_scope_binding(plan),
                "experiment_plan": plan.as_dict(),
                "strategy": child_strategy.to_dict(),
                "parameters": dict(child_strategy.parameters),
                "variant_id": "mutation-" + child.candidate_id,
                "generation": child.generation,
                "parent_id": child.parent_id,
                "lineage": list(child.lineage),
                "dataset_version": plan.dataset_version,
                "dataset_id": plan.dataset_id,
                "paper_only": True,
                "holdout_used": False,
                "crypto_provenance": dict(self._crypto_binding(plan) or {}),
            }
            child_payload = _mark_generated_queue_payload(
                child_payload,
                kind="mutation_child",
                dataset_id=plan.dataset_id,
                dataset_version=plan.dataset_version,
                attestation_hash=attestation_hash,
            )
            self.store.save_strategy_if_absent(child_strategy.id, child_strategy.to_dict())
            self.store.save_experiment_if_absent(
                child.candidate_id,
                {
                    "run_id": child.candidate_id,
                    "status": "IDEA",
                    "strategy_id": child_strategy.id,
                    "strategy": child_strategy.to_dict(),
                    "variant": dict(child_strategy.parameters),
                    "candidate_id": child.candidate_id,
                    "hypothesis_id": plan.hypothesis_id,
                    "plan_id": plan.plan_id,
                    "plan_hash": plan.plan_hash,
                    **_scope_binding(plan),
                    "dataset_id": plan.dataset_id,
                    "dataset_version": plan.dataset_version,
                    "generation": child.generation,
                    "parent_id": child.parent_id,
                    "lineage": list(child.lineage),
                    "paper_only": True,
                    "holdout_used": False,
                },
                strategy_id=child_strategy.id,
            )
            self.bus.submit_candidate(
                child_payload,
                dedupe_key="candidate:" + child.candidate_id,
                lineage=child.lineage,
                available_at=now,
            )
            child_ids.append(child.candidate_id)
        return tuple(child_ids)

    def _reserve_candidate(self, plan: ExperimentPlan, candidate_id: str, now: datetime) -> None:
        current = ensure_utc(now)
        daily_since: datetime | None = None
        daily_until: datetime | None = None
        if self.config.max_experiments_per_day is not None:
            daily_since = datetime(current.year, current.month, current.day, tzinfo=timezone.utc)
            daily_until = daily_since + timedelta(days=1)
        try:
            self.store.reserve_experiment_budget(
                AUTONOMOUS_BUDGET_ID,
                total_limit=self.config.total_limit,
                per_family_limit=self.config.family_limit,
                family=plan.experiment_family,
                reservation_key=candidate_id,
                timestamp=current,
                daily_limit=self.config.max_experiments_per_day,
                daily_since=daily_since,
                daily_until=daily_until,
            )
        except RuntimeError as exc:
            reason = (
                "EXPERIMENT_DAILY_LIMIT_EXCEEDED"
                if str(exc) == "experiment daily budget exhausted"
                else "EXPERIMENT_BUDGET_EXCEEDED"
            )
            raise AutonomousResearchError(reason, str(exc)) from exc

    def _candidate_payload(
        self,
        plan: ExperimentPlan,
        candidate_id: str,
        strategy: StrategyDefinition,
        parameters: Mapping[str, Any],
        *,
        variant_id: str,
        generation: int,
        lineage: Sequence[str],
    ) -> dict[str, Any]:
        return {
            "candidate_id": candidate_id,
            "hypothesis_id": plan.hypothesis_id,
            "plan_id": plan.plan_id,
            "plan_hash": plan.plan_hash,
            "experiment_plan": plan.as_dict(),
            **_scope_binding(plan),
            "strategy": strategy.to_dict(),
            **_strategy_metadata(strategy),
            "parameters": dict(parameters),
            "variant_id": variant_id,
            "generation": generation,
            "lineage": list(lineage),
            "dataset_id": plan.dataset_id,
            "dataset_version": plan.dataset_version,
            "experiment_family": plan.experiment_family,
            "paper_only": True,
            "holdout_used": False,
        }

    def _hypothesis_result(
        self,
        plan: ExperimentPlan,
        results: Sequence[Mapping[str, Any]],
        mutations: Sequence[str],
        *,
        split_counts: Mapping[str, int] | None = None,
    ) -> dict[str, Any]:
        rejected = [item for item in results if item.get("stage") == CandidateStage.REJECTED.value]
        selected_stages = (
            {CandidateStage.FROZEN.value}
            if plan.market_type is MarketType.CRYPTO_SPOT
            else {CandidateStage.PAPER_FORWARD.value, CandidateStage.PAPER_PROMOTABLE.value}
        )
        selected = [
            item
            for item in results
            if item.get("stage") in selected_stages
            and not bool(item.get("selection_excluded"))
            and str(item.get("research_role", "")).strip().upper() != "ZERO_EDGE_CONTROL"
        ]
        reason_codes = [
            str(value).strip()
            for item in results
            for value in (item.get("reason_code"),)
            if value is not None and str(value).strip()
        ]
        if not reason_codes:
            reason_codes = [
                str(item.get("reason")).strip()
                for item in results
                if re.fullmatch(r"[A-Z][A-Z0-9_]{2,63}", str(item.get("reason", "")).strip())
            ]
        reason_code = next(iter(dict.fromkeys(reason_codes)), None)
        summary_reason = next(
            (
                str(item.get("reason")).strip()
                for item in results
                if item.get("reason") is not None and str(item.get("reason")).strip()
            ),
            None,
        )
        supported_edge = bool(selected)
        insufficient_data = not supported_edge and any(
            str(item.get("reason_code", "")).strip().upper() == "INSUFFICIENT_DATA"
            for item in results
        )
        if supported_edge:
            summary_reason = None
        reasons: dict[str, int] = {}
        for item in rejected:
            reason = str(item.get("reason", "rejected"))
            reasons[reason] = reasons.get(reason, 0) + 1
        trial_manifest = [
            {
                "trial_index": index,
                "candidate_id": item.get("candidate_id"),
                "stage": item.get("stage"),
                "dataset_row_count": item.get("dataset_row_count"),
                "train_sample_count": item.get("train_sample_count"),
                "validation_sample_count": item.get("validation_sample_count"),
                "holdout_sample_count": item.get("holdout_sample_count"),
                "validation_expectancy": item.get("validation_expectancy"),
                "validation_filled_trades": item.get("validation_filled_trades"),
                "required_validation_samples": item.get("required_validation_samples"),
                "required_validation_trades": item.get("required_validation_trades"),
                "minimum_sample_check": item.get("minimum_sample_check"),
                "research_role": item.get("research_role"),
                "selection_excluded": bool(item.get("selection_excluded")),
                "proven_zero_edge": bool(item.get("proven_zero_edge")),
                "holdout_evaluated": bool(item.get("holdout_evaluated")),
                "holdout_used_for_selection": bool(item.get("holdout_used_for_selection")),
            }
            for index, item in enumerate(results)
        ]
        split = {
            str(name): int(value)
            for name, value in dict(split_counts or {}).items()
            if not isinstance(value, bool)
        }
        split.setdefault("dataset_row_count", 0)
        split.setdefault("train_sample_count", 0)
        split.setdefault("validation_sample_count", 0)
        split.setdefault("holdout_sample_count", 0)
        return {
            "accepted": supported_edge,
            "reason_code": None if supported_edge else reason_code,
            "reason": summary_reason,
            "kind": "hypothesis",
            "hypothesis_id": plan.hypothesis_id,
            "plan_id": plan.plan_id,
            "plan_hash": plan.plan_hash,
            **_scope_binding(plan),
            "status": (
                "accepted_research_only"
                if plan.market_type is MarketType.CRYPTO_SPOT and selected
                else "accepted" if selected
                else "insufficient_data" if insufficient_data
                else "unsupported_by_validation"
            ),
            "dataset_row_count": split["dataset_row_count"],
            "train_sample_count": split["train_sample_count"],
            "validation_sample_count": split["validation_sample_count"],
            "holdout_sample_count": split["holdout_sample_count"],
            "split": {
                "train": split["train_sample_count"],
                "validation": split["validation_sample_count"],
                "holdout": split["holdout_sample_count"],
            },
            "supported_edge": supported_edge,
            "blocker": None if supported_edge else "INSUFFICIENT_DATA" if insufficient_data else "NO_SUPPORTED_EDGE",
            "next_action": "AWAIT_PAPER_EVIDENCE" if supported_edge else "CONTINUE_RESEARCH_AND_PAPER",
            "variants_tested": len(results),
            "selected_from_variants": len(selected),
            "selected_candidate_ids": [
                str(item.get("candidate_id"))
                for item in selected
                if str(item.get("candidate_id", "")).strip()
            ],
            "trial_manifest": trial_manifest,
            "candidate_results": [
                {
                    "candidate_id": item.get("candidate_id"),
                    "stage": item.get("stage"),
                    "reason": item.get("reason"),
                    "reason_code": item.get("reason_code"),
                    "forward_test_id": item.get("forward_test_id"),
                    "dataset_row_count": item.get("dataset_row_count"),
                    "train_sample_count": item.get("train_sample_count"),
                    "validation_sample_count": item.get("validation_sample_count"),
                    "holdout_sample_count": item.get("holdout_sample_count"),
                    "validation_filled_trades": item.get("validation_filled_trades"),
                    "required_validation_samples": item.get("required_validation_samples"),
                    "required_validation_trades": item.get("required_validation_trades"),
                    "minimum_sample_check": item.get("minimum_sample_check"),
                    "validation": item.get("validation"),
                    "research_role": item.get("research_role"),
                    "selection_excluded": bool(item.get("selection_excluded")),
                    "proven_zero_edge": bool(item.get("proven_zero_edge")),
                    "holdout_evaluated": bool(item.get("holdout_evaluated")),
                    "holdout_used_for_selection": bool(item.get("holdout_used_for_selection")),
                    "holdout": item.get("holdout"),
                }
                for item in results
            ],
            "rejected_reasons": dict(sorted(reasons.items())),
            "mutation_candidates": list(mutations[:_MAX_QUEUE_RESULT_ITEMS]),
            "experiment_family": plan.experiment_family,
            "dataset_version": plan.dataset_version,
            "data_quality": "PRICE_PROXY_RESEARCH" if plan.market_type is MarketType.PREDICTION else "OHLCV_SIMULATED",
            "paper_only": True,
            "research_only": plan.market_type is MarketType.CRYPTO_SPOT,
            "holdout_used": False,
            "holdout_evaluated": any(
                bool(item.get("holdout_evaluated")) for item in results
            ),
            "holdout_locked": not any(
                bool(item.get("holdout_evaluated")) for item in results
            ),
            "holdout_selection_fence": "validation_only",
        }

    @staticmethod
    def _forward_evidence_identity(
        spec: Any,
        observations: Sequence[Mapping[str, Any]],
        execution_events: Sequence[Mapping[str, Any]],
        ledgers: Sequence[Mapping[str, Any]],
        fills: Sequence[Any] = (),
    ) -> str:
        """Hash immutable paper records so unchanged evidence is idempotent."""
        def rows(items: Sequence[Mapping[str, Any]], identifier: str) -> list[dict[str, Any]]:
            result: list[dict[str, Any]] = []
            for item in items:
                value = item if isinstance(item, Mapping) else {}
                record_id = str(value.get(identifier, "")).strip()
                if not record_id:
                    record_id = _hash_document(value)
                result.append(
                    {
                        "id": record_id,
                        "market_id": str(value.get("market_id", "")).strip(),
                        "timestamp": str(value.get("timestamp", value.get("resolved_at", ""))),
                        "payload_hash": _hash_document(value.get("payload", value)),
                        "status": str(value.get("status", "")).strip().upper(),
                    }
                )
            return sorted(result, key=lambda row: (row["id"], row["market_id"], row["timestamp"], row["payload_hash"]))

        expected_experiment_id = str(getattr(spec, "experiment_id", "")).strip()
        expected_strategy_hash = str(getattr(spec, "strategy_hash", "")).strip()
        fill_identity_fields = (
            "order_id",
            "market_id",
            "symbol",
            "timestamp",
            "side",
            "quantity",
            "price",
            "fees",
            "slippage",
            "strategy_id",
            "expected_probability",
            "executable_probability",
            "paper_experiment_id",
            "execution_status",
            "requested_quantity",
            "partial",
            "outcome",
        )

        def fill_rows(items: Sequence[Any]) -> list[dict[str, Any]]:
            result: list[dict[str, Any]] = []
            for index, item in enumerate(items):
                if index >= _MAX_FORWARD_ROWS:
                    break
                if isinstance(item, Fill):
                    metadata = item.metadata if isinstance(item.metadata, Mapping) else {}
                    paper_experiment_id = str(metadata.get("paper_experiment_id", "")).strip()
                    strategy_id = str(item.strategy_id).strip()
                    if paper_experiment_id != expected_experiment_id or strategy_id != expected_strategy_hash:
                        continue
                    canonical = {
                        "order_id": str(item.order_id).strip(),
                        "market_id": str(item.market_id or item.symbol).strip(),
                        "symbol": str(item.symbol).strip(),
                        "timestamp": ensure_utc(item.timestamp).isoformat(),
                        "side": getattr(item.side, "value", item.side),
                        "quantity": item.quantity,
                        "price": item.price,
                        "fees": item.fees,
                        "slippage": item.slippage,
                        "strategy_id": strategy_id,
                        "expected_probability": item.expected_probability,
                        "executable_probability": item.executable_probability,
                        "paper_experiment_id": paper_experiment_id,
                        "execution_status": metadata.get("execution_status"),
                        "requested_quantity": metadata.get("requested_quantity"),
                        "partial": metadata.get("partial"),
                        "outcome": metadata.get("outcome"),
                    }
                elif isinstance(item, Mapping):
                    metadata = item.get("metadata")
                    metadata = metadata if isinstance(metadata, Mapping) else {}
                    paper_experiment_id = str(
                        item.get("paper_experiment_id", metadata.get("paper_experiment_id", ""))
                    ).strip()
                    strategy_id = str(item.get("strategy_id", "")).strip()
                    if paper_experiment_id != expected_experiment_id or strategy_id != expected_strategy_hash:
                        continue
                    canonical = {
                        key: item.get(key, metadata.get(key))
                        for key in fill_identity_fields
                    }
                    canonical["order_id"] = str(canonical.get("order_id") or "").strip()
                    canonical["market_id"] = str(
                        canonical.get("market_id") or canonical.get("symbol") or ""
                    ).strip()
                    canonical["symbol"] = str(canonical.get("symbol") or "").strip()
                    canonical["timestamp"] = str(canonical.get("timestamp") or "")
                    canonical["side"] = getattr(canonical.get("side"), "value", canonical.get("side"))
                    canonical["strategy_id"] = strategy_id
                    canonical["paper_experiment_id"] = paper_experiment_id
                else:
                    continue
                result.append(
                    {
                        "id": canonical["order_id"] or _hash_document(canonical),
                        **canonical,
                    }
                )
            return sorted(
                result,
                key=lambda row: (
                    row["id"],
                    row["market_id"],
                    row["timestamp"],
                    _canonical_binding(row),
                ),
            )

        config = spec.config if isinstance(getattr(spec, "config", None), Mapping) else {}
        scope = {
            name: config.get(name)
            for name in (
                "plan_id",
                "plan_hash",
                "market_scope_hash",
                "market_scope_version",
                "dataset_selector",
                "dataset_id",
                "dataset_version",
            )
        }
        return _hash_document(
            {
                "forward_test_id": str(getattr(spec, "experiment_id", "")),
                "strategy_hash": str(getattr(spec, "strategy_hash", "")),
                "model_hash": str(getattr(spec, "model_hash", "")),
                "allowed_markets": list(getattr(spec, "allowed_markets", ()) or ()),
                "scope": scope,
                "observations": rows(observations, "observation_id"),
                "execution_events": rows(execution_events, "event_id"),
                "ledgers": rows(ledgers, "bet_id"),
                "fills": fill_rows(fills),
            }
        )

    def _forward_evidence(self, record: Mapping[str, Any], now: datetime) -> dict[str, Any]:
        payload = record.get("payload", {})
        payload = dict(payload) if isinstance(payload, Mapping) else {}
        forward_id = str(payload.get("forward_test_id", "")).strip()
        if not forward_id:
            raise ValueError("candidate has no forward test")
        spec = ForwardTestRegistry(self.store).get(forward_id)
        if spec is None:
            raise ValueError(f"missing forward test {forward_id}")
        observations = self.store.list_paper_observations(forward_id, limit=_MAX_FORWARD_ROWS)
        fills: list[Fill] = []
        try:
            stored_fills = self.store.load_fills(strategy_id=spec.strategy_hash)
        except Exception:
            stored_fills = ()
        for fill in stored_fills:
            if str(fill.metadata.get("paper_experiment_id", "")) != forward_id:
                continue
            fills.append(fill)
            if len(fills) >= _MAX_FORWARD_ROWS:
                break
        event_records = self.store.list_paper_execution_events(forward_id, limit=_MAX_FORWARD_ROWS)
        ledger_records = self.store.list_paper_bet_ledger(forward_id, limit=_MAX_FORWARD_ROWS)
        terminal_by_market: dict[str, str] = {}
        observed_markets: set[str] = set()
        for row in observations:
            item = row.get("payload", {}) if isinstance(row, Mapping) else {}
            item = item if isinstance(item, Mapping) else {}
            market_id = str(row.get("market_id", item.get("market_id", ""))).strip()
            if market_id:
                observed_markets.add(market_id)
            settlement = _settlement_name(item.get("settlement"))
            if settlement in {"resolved_yes", "resolved_no", "void"} and market_id:
                terminal_by_market[market_id] = settlement

        fills_by_market: dict[str, list[Fill]] = {}
        for fill in fills:
            fills_by_market.setdefault(str(fill.market_id or fill.symbol), []).append(fill)
        ledger_by_market: dict[str, Mapping[str, Any]] = {}
        for row in ledger_records:
            market_id = str(row.get("market_id", "")).strip()
            ledger = row.get("payload")
            if market_id and isinstance(ledger, Mapping):
                ledger_by_market[market_id] = ledger
        for market_id, resolution in terminal_by_market.items():
            if market_id in ledger_by_market:
                continue
            ledger = build_resolved_bet(
                experiment_id=forward_id,
                market_id=market_id,
                strategy_id=spec.strategy_hash,
                settlement=resolution,
                resolved_at=next(
                    (
                        row["timestamp"]
                        for row in observations
                        if str(row.get("market_id", "")) == market_id
                        and isinstance(row.get("payload"), Mapping)
                        and _settlement_name(row["payload"].get("settlement")) == resolution
                    ),
                    now,
                ),
                fills=fills_by_market.get(market_id, ()),
            )
            if ledger is not None:
                self.store.save_paper_bet_ledger(
                    ledger["bet_id"],
                    forward_id,
                    market_id,
                    spec.strategy_hash,
                    ledger["outcome"],
                    ledger["resolution"],
                    parse_timestamp(ledger["resolved_at"]) or now,
                    ledger,
                )
                ledger_by_market[market_id] = ledger
        ledgers = list(ledger_by_market.values())

        if not event_records:
            event_records = [
                {
                    "market_id": str(fill.market_id or fill.symbol),
                    "status": str(fill.metadata.get("execution_status", "FULL_FILL")).upper(),
                    "payload": {
                        "market_id": str(fill.market_id or fill.symbol),
                        "status": str(fill.metadata.get("execution_status", "FULL_FILL")).upper(),
                        "outcomes": ["SIGNAL", "ORDER_ATTEMPT", str(fill.metadata.get("execution_status", "FULL_FILL")).upper()],
                        "order_attempted": True,
                        "requested_quantity": _finite(fill.metadata.get("requested_quantity"), fill.quantity),
                        "filled_quantity": fill.quantity,
                        "liquidity": fill.metadata.get("liquidity"),
                        "spread_paid": fill.metadata.get("spread_paid", 0.0),
                        "depth_consumed": fill.metadata.get("depth_consumed", 0.0),
                    },
                }
                for fill in fills
            ]
        event_payloads = [
            row.get("payload", {})
            for row in event_records
            if isinstance(row, Mapping) and isinstance(row.get("payload"), Mapping)
        ]
        attempt_events = [
            event
            for event in event_payloads
            if bool(event.get("order_attempted"))
            or "ORDER_ATTEMPT" in tuple(event.get("outcomes", ()))
        ]
        successful_events = [
            event
            for event in attempt_events
            if str(event.get("status", "")).upper() in {"FULL_FILL", "PARTIAL_FILL"}
            and _finite(event.get("filled_quantity"), 0.0) > 0
        ]
        no_fill_events = [event for event in attempt_events if str(event.get("status", "")).upper() == "NO_FILL"]
        risk_rejected_events = [
            event for event in attempt_events if str(event.get("status", "")).upper() == "RISK_REJECTED"
        ]
        requested_quantity = sum(max(0.0, _finite(event.get("requested_quantity"), 0.0)) for event in attempt_events)
        filled_quantity = sum(max(0.0, _finite(event.get("filled_quantity"), 0.0)) for event in attempt_events)
        partial_fills = sum(
            1
            for fill in fills
            if bool(fill.metadata.get("partial"))
            or str(fill.metadata.get("execution_status", "")).upper() == "PARTIAL_FILL"
        )
        liquidity_rejections = sum(
            1
            for event in no_fill_events
            if bool(event.get("liquidity_rejected"))
            or str(event.get("reason", "")).lower() in {"insufficient_liquidity", "invalid_order_book"}
        )
        order_attempts = len(attempt_events)
        successful_order_attempts = len(successful_events)
        failed_order_attempts = sum(
            1
            for event in attempt_events
            if str(event.get("status", "")).upper() in {"NO_FILL", "RISK_REJECTED"}
        )
        markets_signaled = {
            str(event.get("market_id", "")).strip()
            for event in event_payloads
            if str(event.get("market_id", "")).strip()
            and "SIGNAL" in tuple(event.get("outcomes", ()))
            and str(event.get("status", "")).upper() != "NO_SIGNAL"
        }
        markets_traded = {str(fill.market_id or fill.symbol).strip() for fill in fills}
        markets_resolved = set(terminal_by_market)
        resolved_positions = sum(int(_finite(ledger.get("positions"), 0.0)) for ledger in ledgers)
        resolved_pnls = [_finite(ledger.get("net_pnl"), math.nan) for ledger in ledgers]
        resolved_pnls = [value for value in resolved_pnls if math.isfinite(value)]
        resolved_rois = [_finite(ledger.get("roi"), math.nan) for ledger in ledgers]
        resolved_rois = [value for value in resolved_rois if math.isfinite(value)]
        forward_expectancy = mean(resolved_pnls) if resolved_pnls else None
        forward_roi = mean(resolved_rois) if resolved_rois else None
        confidence_interval = (
            bootstrap_confidence_interval(
                resolved_pnls,
                resamples=min(256, max(32, len(resolved_pnls) * 8)),
                seed=0,
            )
            if resolved_pnls
            else None
        )
        raw_confidence_lower_bound = (
            _finite(confidence_interval.get("lower"), math.nan)
            if isinstance(confidence_interval, Mapping)
            else math.nan
        )
        forward_confidence_lower_bound = (
            raw_confidence_lower_bound
            if math.isfinite(raw_confidence_lower_bound)
            else None
        )
        forward_stability = (
            sum(1 for value in resolved_rois if value >= 0.0) / len(resolved_rois)
            if len(resolved_rois) >= 2
            else None
        )
        calibration_rows: list[dict[str, float]] = []
        for ledger in ledgers:
            probability = _finite(ledger.get("expected_probability_at_entry"), math.nan)
            resolution = _settlement_name(ledger.get("resolution"))
            if math.isfinite(probability) and 0.0 <= probability <= 1.0 and resolution in {"resolved_yes", "resolved_no"}:
                calibration_rows.append(
                    {
                        "probability": probability,
                        "outcome": 1.0 if resolution == "resolved_yes" else 0.0,
                    }
                )
        forward_calibration = (
            max(0.0, min(1.0, 1.0 - expected_calibration_error(calibration_rows)))
            if calibration_rows
            else None
        )
        state_record = self.store.load_paper_state(forward_id) or {}
        state = state_record.get("state", {}) if isinstance(state_record, Mapping) else {}
        portfolio = state.get("portfolio", {}) if isinstance(state, Mapping) else {}
        risk_state = state.get("risk", {}) if isinstance(state, Mapping) else {}
        duration = max(0.0, (ensure_utc(now) - spec.registration_timestamp).total_seconds())
        risk_equity = _finite(risk_state.get("equity"), spec.bankroll)
        equity = _finite(portfolio.get("equity"), risk_equity)
        prior_peak = max(
            _finite(payload.get("forward_peak_equity"), spec.bankroll),
            _finite(state.get("forward_peak_equity"), spec.bankroll),
        )
        peak = max(prior_peak, equity)
        calculated_drawdown = max(0.0, 1.0 - equity / peak) if peak > 0 else 0.0
        drawdown = max(
            calculated_drawdown,
            _finite(state.get("forward_max_drawdown"), 0.0),
            _finite(risk_state.get("drawdown"), 0.0),
        )
        event_liquidity = [
            _finite(event.get("liquidity"), math.nan)
            for event in attempt_events
            if math.isfinite(_finite(event.get("liquidity"), math.nan))
        ]
        regime_values: set[str] = set()
        for event in attempt_events:
            regime = event.get("regime", event.get("regime_state"))
            if regime is not None and str(regime).strip():
                regime_values.add(str(regime).strip())
        average_slippage = (
            sum(float(fill.slippage) * float(fill.quantity) for fill in fills) / filled_quantity
            if filled_quantity > 0
            else None
        )
        spread_paid = (
            sum(
                max(0.0, _finite(event.get("spread_paid"), 0.0))
                * max(0.0, _finite(event.get("filled_quantity"), 0.0))
                for event in attempt_events
            )
            / filled_quantity
            if filled_quantity > 0
            else None
        )
        risk_reasons = risk_state.get("reasons", ()) if isinstance(risk_state, Mapping) else ()
        risk_reasons = tuple(str(reason) for reason in risk_reasons) if isinstance(risk_reasons, (list, tuple, set)) else ()
        hard_risk_reasons = {
            "max_loss",
            "max_drawdown",
            "max_daily_loss",
            "max_cvar",
            "emergency_kill_switch",
        }
        execution_impossible = (
            order_attempts > 0
            and order_attempts >= self.config.promotion_criteria.min_order_attempts_for_execution_rejection
            and successful_order_attempts == 0
        )
        forward_liquidity = mean(event_liquidity) if event_liquidity else None
        independent_resolved_bets = len(ledgers)
        unresolved_markets = markets_traded - markets_resolved
        total_gross = sum(_finite(ledger.get("gross_pnl"), 0.0) for ledger in ledgers)
        total_fees = sum(_finite(ledger.get("fees"), 0.0) for ledger in ledgers)
        total_slippage = sum(_finite(ledger.get("slippage"), 0.0) for ledger in ledgers)
        total_net = sum(_finite(ledger.get("net_pnl"), 0.0) for ledger in ledgers)
        return {
            "forward_test_id": forward_id,
            "evidence_scope": "forward_only",
            "forward_duration_seconds": duration,
            "markets_observed": len(observed_markets),
            "markets_signaled": len(markets_signaled),
            "markets_traded": len(markets_traded),
            "markets_resolved": len(markets_resolved),
            "forward_trades": successful_order_attempts,
            "trades": successful_order_attempts,
            "forward_order_attempts": order_attempts,
            "order_attempts": order_attempts,
            "forward_successful_order_attempts": successful_order_attempts,
            "successful_order_attempts": successful_order_attempts,
            "forward_failed_order_attempts": failed_order_attempts,
            "failed_order_attempts": failed_order_attempts,
            "risk_rejected_orders": len(risk_rejected_events),
            "fills": len(fills),
            "partial_fills": partial_fills,
            "no_fill_orders": len(no_fill_events),
            "no_fills": len(no_fill_events),
            "observations_without_signal": sum(
                1 for event in event_payloads if str(event.get("status", "")).upper() == "NO_SIGNAL"
            ),
            "requested_quantity": requested_quantity,
            "filled_quantity": filled_quantity,
            "fill_ratio": min(1.0, filled_quantity / requested_quantity) if requested_quantity > 0 else None,
            "depth_consumed": sum(max(0.0, _finite(event.get("depth_consumed"), 0.0)) for event in attempt_events),
            "average_slippage": average_slippage,
            "spread_paid": spread_paid,
            "orders_rejected_for_liquidity": liquidity_rejections,
            "forward_liquidity": forward_liquidity,
            "positions_opened": len({(str(fill.market_id or fill.symbol), str(fill.metadata.get("outcome", "yes"))) for fill in fills}),
            "resolved_positions": resolved_positions,
            "independent_markets_traded": len(markets_traded),
            "forward_independent_resolved_bets": independent_resolved_bets,
            "independent_resolved_bets": independent_resolved_bets,
            "unresolved_markets": len(unresolved_markets),
            "unresolved_fills": sum(
                1
                for fill in fills
                if str(fill.market_id or fill.symbol) not in ledger_by_market
            ),
            "forward_gross_pnl": total_gross if ledgers else None,
            "forward_fees": total_fees if ledgers else None,
            "forward_slippage": total_slippage if ledgers else None,
            "forward_net_pnl": total_net if ledgers else None,
            "forward_expectancy": forward_expectancy,
            "forward_roi": forward_roi,
            "forward_confidence_interval": (
                dict(confidence_interval) if isinstance(confidence_interval, Mapping) else None
            ),
            "forward_confidence_lower_bound": forward_confidence_lower_bound,
            "forward_stability": forward_stability,
            "forward_calibration": forward_calibration,
            "forward_max_drawdown": drawdown,
            "forward_peak_equity": peak,
            "forward_regime_count": len(regime_values),
            "risk_breach": bool(set(risk_reasons) & hard_risk_reasons),
            "model_invalid": False,
            "invariant_violation": False,
            "execution_impossible": execution_impossible,
            "resolved_bet_ids": [str(ledger.get("bet_id", "")) for ledger in ledgers],
            "forward_evidence_identity": self._forward_evidence_identity(
                spec,
                observations,
                event_records,
                ledgers,
                fills,
            ),
            "paper_only": True,
            "forward_benchmark_comparison": (
                {"baseline_expectancy": 0.0, "delta": forward_expectancy}
                if forward_expectancy is not None
                else None
            ),
        }

    def _evaluate_forward_candidate(self, candidate_id: str, now: datetime) -> CandidateLifecycle | None:
        record = self.lifecycle.get(candidate_id)
        if record is None or record.stage is not CandidateStage.PAPER_FORWARD:
            return record
        evidence = self._forward_evidence(record.as_record(), now)
        updated = self.lifecycle.record_evidence(candidate_id, evidence, expected_stage=CandidateStage.PAPER_FORWARD, reason="forward result evaluation")
        reasons = self.config.promotion_criteria.evaluate({**updated.payload, **evidence})
        hard_reasons = self.config.promotion_criteria.hard_rejection_reasons(evidence)
        if hard_reasons:
            return self.lifecycle.reject(
                candidate_id,
                hard_reasons[0],
                evidence=evidence,
                expected_stage=CandidateStage.PAPER_FORWARD,
                expected_payload=updated.payload,
            )
        if not reasons:
            return self.lifecycle.advance(
                candidate_id,
                CandidateStage.PAPER_PROMOTABLE,
                {**evidence, "holdout_used": False},
                reason="paper-forward criteria passed; human review required",
            )
        return updated
def _legacy_recovery_document(record: Mapping[str, Any]) -> Mapping[str, Any] | None:
    """Extract JSON evidence before handing a lifecycle row to legacy logic.

    ``load_candidate_lifecycle`` adds metadata such as ``updated_at`` to the
    outer row.  That metadata is useful for ordering but is not part of the
    persisted candidate document and cannot be inspected by the bounded legacy
    classifier.  Keep the document authoritative, using row metadata only
    when the payload omitted its candidate identity.
    """
    payload = record.get("payload")
    if not isinstance(payload, Mapping):
        return None
    document = dict(payload)
    for name in (
        "candidate_id",
        "frozen_hash",
        "predecessor_candidate_id",
        "predecessor_frozen_hash",
        "stage",
    ):
        if name not in document and record.get(name) is not None:
            document[name] = record[name]
    return document


def _nested_legacy_target_market_ids(payload: Mapping[str, Any]) -> tuple[Any, ...] | str | None:
    """Return the nested legacy target binding, if one was persisted."""
    plan = payload.get("experiment_plan")
    if not isinstance(plan, Mapping):
        return None
    target = plan.get("target")
    if not isinstance(target, Mapping):
        return None
    for name in ("market_ids", "exact_market_ids", "markets"):
        if name in target:
            value = target[name]
            if isinstance(value, str):
                return value
            if isinstance(value, (list, tuple, set, frozenset)):
                return tuple(value)
            return (value,)
    return None


def _legacy_recovery_state_digest(evidence: Mapping[str, Any]) -> str:
    """Digest only stable blocker/provenance state, not retry telemetry."""
    material = {
        name: evidence.get(name)
        for name in (
            "predecessor_candidate_id",
            "predecessor_frozen_hash",
            "classification",
            "reason_code",
            "progress",
            "blocker",
            "next_action",
            "scope_hash",
            "scope_version",
            "successor_id",
            "provenance_state",
        )
    }
    return "sha256:" + hashlib.sha256(_canonical_binding(material).encode("utf-8")).hexdigest()


def _legacy_recovery_key(
    record: Mapping[str, Any],
    candidate_id: str | None,
    frozen_hash: str | None,
) -> str:
    """Build a stable identity for one predecessor recovery attempt."""
    material = {
        "candidate_id": candidate_id,
        "frozen_hash": frozen_hash,
        "record": record,
    }
    return "legacy-scope-recovery:" + hashlib.sha256(
        _canonical_binding(material).encode("utf-8")
    ).hexdigest()[:32]


def _candidate_id(plan: ExperimentPlan, parameters: Mapping[str, Any], *, generation: int) -> str:
    token = json.dumps(
        {"plan_id": plan.plan_id, "plan_hash": plan.plan_hash, "parameters": dict(parameters), "generation": generation},
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    return "candidate-" + hashlib.sha256(token.encode("utf-8")).hexdigest()[:24]

def _mutation_candidate_id(parent_id: str, generation: int, strategy: StrategyDefinition) -> str:
    document = strategy.to_dict()
    document.pop("strategy_id", None)
    token = json.dumps(
        {"parent": str(parent_id), "generation": generation, "strategy": document, "seed": 0},
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    return "mutation-" + hashlib.sha256(token.encode("utf-8")).hexdigest()[:24]

def _normalize_hypothesis_payload(
    payload: Mapping[str, Any],
    item: ResearchQueueItem | None = None,
    *,
    source_fallback: str | None = None,
) -> dict[str, Any]:
    result = dict(payload)
    item_id = item.item_id if item is not None else ""
    hypothesis_id = str(result.get("proposal_id", result.get("hypothesis_id", ""))).strip() or item_id
    assumptions = result.get("assumptions", {})
    assumptions = assumptions if isinstance(assumptions, Mapping) else {}
    if hypothesis_id or "proposal_id" in result:
        result.setdefault("proposal_id", hypothesis_id)
    if item is not None:
        default_source = item.author or item.source or "hermes"
    else:
        default_source = source_fallback or "hermes"
    result.setdefault("source", str(result.get("author", default_source)) or "hermes")
    result.setdefault("tests", ["bounded chronological backtest and validation"])
    market_type = result.get("market_type")
    plan_value = result.get("experiment_plan")
    if isinstance(plan_value, Mapping):
        market_type = plan_value.get("market_type", market_type)
    # Prediction proposals retain their historical compatibility default. A
    # crypto proposal must state its immutable dataset version explicitly.
    if str(market_type or "prediction").strip().lower() != MarketType.CRYPTO_SPOT.value:
        result.setdefault("dataset_version", assumptions.get("dataset_version", "axiom-persisted-v1"))
    result.setdefault("time_split", "train-validation-holdout")
    result.setdefault("paper_only", True)
    if "experiment_plan" not in result and isinstance(assumptions.get("experiment_plan"), Mapping):
        result["experiment_plan"] = assumptions["experiment_plan"]
    return result


def _normalize_row(value: Any) -> dict[str, Any] | None:
    if isinstance(value, Mapping):
        row = dict(value)
        nested = row.get("snapshot")
        if isinstance(nested, Mapping):
            merged = dict(nested)
            merged.update({key: child for key, child in row.items() if key != "snapshot"})
            row = merged
        payload = row.get("payload")
        if isinstance(payload, Mapping) and isinstance(payload.get("snapshot"), Mapping):
            merged = dict(payload["snapshot"])
            merged.update({key: child for key, child in row.items() if key != "payload"})
            row = merged
        return row
    return None


def _value(row: Mapping[str, Any], name: str, default: Any = None) -> Any:
    return row.get(name, default)

def _normal_symbol(value: Any) -> str:
    return str(value).replace("/", "").replace("-", "").replace("_", "").strip().upper()


def _time_to_expiry(row: Mapping[str, Any]) -> float:
    direct = _finite(row.get("time_to_expiry_seconds"), math.nan)
    if math.isfinite(direct):
        return direct
    stamp = parse_timestamp(row.get("timestamp"))
    expiry = parse_timestamp(row.get("expiry"))
    if stamp is None or expiry is None:
        return math.nan
    return (expiry - stamp).total_seconds()


def _as_regime_set(value: Any, name: str) -> set[str]:
    if isinstance(value, str):
        values = {value}
    elif isinstance(value, (list, tuple, set)):
        values = {str(item) for item in value if str(item).strip()}
    else:
        raise AutonomousResearchError("UNSUPPORTED_FEATURE", f"{name} must be text or a bounded list")
    if not values:
        raise AutonomousResearchError("UNSUPPORTED_FEATURE", f"{name} must not be empty")
    return values


def _in_bound(value: float, bound: Any) -> bool:
    if not math.isfinite(value):
        return False
    if isinstance(bound, (list, tuple)):
        if len(bound) == 2:
            low, high = _finite(bound[0], math.nan), _finite(bound[1], math.nan)
            return math.isfinite(low) and math.isfinite(high) and min(low, high) <= value <= max(low, high)
        return any(abs(value - _finite(item, math.nan)) <= 1e-12 for item in bound if math.isfinite(_finite(item, math.nan)))
    target = _finite(bound, math.nan)
    return math.isfinite(target) and abs(value - target) <= 1e-12


def _has_book(rows: Sequence[Mapping[str, Any]]) -> bool:
    return any(isinstance(row.get("order_book"), Mapping) or isinstance(row.get("yes_order_book"), Mapping) for row in rows)


def _finite(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return float(default)
    return number if math.isfinite(number) else float(default)


def _settlement_name(value: Any) -> str:
    if isinstance(value, SettlementState):
        return value.value
    return str(value or "").strip().lower()


def _is_finite_number(value: Any) -> bool:
    try:
        return math.isfinite(float(value))
    except (TypeError, ValueError):
        return False


def _hash_document(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False, default=str)
    return "sha256:" + hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _compact_evidence(value: Mapping[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, item in value.items():
        if key in {
            "metrics",
            "confidence_interval",
            "forward_confidence_interval",
            "validation_confidence_interval",
            "minimum_sample_check",
            "validation_stability",
            "validation_stability_summary",
            "validation_expectancy",
            "validation_confidence_lower_bound",
            "validation_calibration",
            "benchmark_comparison",
            "forward_benchmark_comparison",
            "validation_regime_behavior",
            "validation_execution_quality",
            "forward_test_id",
            "evidence_scope",
            "forward_duration_seconds",
            "markets_observed",
            "markets_signaled",
            "markets_traded",
            "markets_resolved",
            "forward_order_attempts",
            "order_attempts",
            "forward_trades",
            "trades",
            "forward_successful_order_attempts",
            "successful_order_attempts",
            "forward_failed_order_attempts",
            "failed_order_attempts",
            "risk_rejected_orders",
            "fills",
            "partial_fills",
            "no_fill_orders",
            "no_fills",
            "observations_without_signal",
            "requested_quantity",
            "filled_quantity",
            "fill_ratio",
            "depth_consumed",
            "average_slippage",
            "spread_paid",
            "orders_rejected_for_liquidity",
            "forward_liquidity",
            "positions_opened",
            "resolved_positions",
            "independent_markets_traded",
            "forward_independent_resolved_bets",
            "independent_resolved_bets",
            "unresolved_markets",
            "unresolved_fills",
            "forward_gross_pnl",
            "forward_fees",
            "forward_slippage",
            "forward_net_pnl",
            "forward_expectancy",
            "forward_roi",
            "forward_confidence_lower_bound",
            "forward_stability",
            "forward_calibration",
            "forward_max_drawdown",
            "forward_peak_equity",
            "forward_regime_count",
            "risk_breach",
            "model_invalid",
            "invariant_violation",
            "execution_impossible",
            "resolved_bet_ids",
            "independent_samples",
            "filled_trades",
            "trades",
            "trade_count",
            "expectancy",
            "confidence_lower_bound",
            "stability",
            "calibration",
            "max_drawdown",
            "regime_count",
            "liquidity",
            "costs",
            "paper_only",
        }:
            result[str(key)] = _compact_value(item)
    return result


def _compact_value(value: Any, depth: int = 0) -> Any:
    if depth > 3:
        return "<truncated>"
    if isinstance(value, Mapping):
        return {str(key): _compact_value(child, depth + 1) for key, child in list(value.items())[:32]}
    if isinstance(value, (list, tuple)):
        return [_compact_value(child, depth + 1) for child in list(value)[:32]]
    if isinstance(value, float):
        return value if math.isfinite(value) else 0.0
    if isinstance(value, (str, int, bool)) or value is None:
        return value
    return str(value)


def _bounded_queue_result(value: Mapping[str, Any]) -> dict[str, Any]:
    result = _compact_value(value)
    if not isinstance(result, dict):
        result = {"result": result}
    # Keep the long-standing public summary fields even when newer bounded
    # split/accounting fields fill the first 32 compact slots.
    for key in (
        "data_quality",
        "paper_only",
        "research_only",
        "holdout_used",
        "holdout_evaluated",
        "holdout_locked",
        "holdout_selection_fence",
    ):
        if key in value:
            result[key] = _compact_value(value[key])
    return result


__all__ = [
    "PREDECLARED_STRATEGY_STARTING_SET",
    "POLYMARKET_CAMPAIGN_GRID",
    "CAMPAIGN_BUDGET_LIMIT",
    "CAMPAIGN_STATUSES",
    "CAMPAIGN_TRIAL_TERMINAL",
    "AutonomousQueueCycle",
    "AutonomousResearchConfig",
    "AutonomousResearchError",
    "AutonomousResearchProcessor",
]
