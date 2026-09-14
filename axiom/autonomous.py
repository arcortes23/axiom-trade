"""Durable closed-loop autonomous research processing.

The processor is deliberately boring: queue leases, declarative plans,
deterministic backtests, ordered lifecycle writes, and paper-forward evidence.
Every external boundary is persisted or rejected; no live execution path exists.
"""
from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import math
import re
from itertools import islice, product
from statistics import mean
from typing import Any, Callable, Iterable, Mapping, NoReturn, Sequence

from .backtest import CryptoBacktester
from .backtest.prediction import run_prediction_research_mode
from .forward import COMMON_PAPER_ASSUMPTIONS, ForwardTestRegistry, _canonical_forward_config, _content_hash
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
from .rolling_portfolio import (
    RollingAdmissionPolicy,
    RollingEvidence,
    RollingSelection,
    default_rolling_admission_policy,
    evaluate_rolling_selection,
)
from .strategy.signals import evaluate_model_document_probability
from .experiment_plan import AUTONOMOUS_BUDGET_ID, ExperimentPlan, ExperimentPlanError, MAX_PLAN_VARIANTS, normalize_market_scope


_MAX_QUEUE_RESULT_ITEMS = 64
_MAX_DATASET_ROWS = 100_000
_MAX_FORWARD_ROWS = 100_000
_MAX_LEGACY_RECOVERY_ITEMS = 64
_MAX_ROLLING_STRATEGIES = 128
_MAX_ROLLING_DISCOVERY_SCAN = 2_048
_ROLLING_DISCOVERY_PAGE = 64
_ROLLING_MATURE_STAGES = frozenset({"FROZEN", "PAPER_FORWARD", "PAPER_PROMOTABLE"})
_ROLLING_ENROLLMENT_MODES = frozenset({"RESEARCH", "OBSERVATION"})
_ROLLING_ENROLLMENT_VALIDATION_VERSION = "rolling-enrollment-v2"
_ROLLING_MAX_PROVENANCE_BYTES = 8_192
_ROLLING_MAX_PROVENANCE_DEPTH = 5
# These fields are observations/manifests, not authoritative candidate
# identity.  In particular, a frozen dataset attestation may contain a large
# constituent binding array whose candidate ids are unrelated to its owner.
_ROLLING_NON_AUTHORITATIVE_ARRAY_KEYS = frozenset(
    {
        "attestation",
        "attestations",
        "dataset_attestation",
        "dataset_attestations",
        "constituent_bindings",
        "constituents",
        "observation",
        "observations",
        "observation_rows",
        "rows",
        "snapshots",
        "bindings",
        "market_bindings",
    }
)
# Historical rolling evidence is preflighted before JSON decoding.  The fixed
# 16 MiB/25,000-row boundary keeps immutable source material bounded while
# the source-row projection below remains deterministic.
_MAX_ROLLING_DATASET_PAYLOAD_BYTES = 16 * 1024 * 1024
_MAX_ROLLING_SOURCE_ROWS = 25_000
_MAX_ROLLING_EXIT_LINEAGE = 20
_MAX_ROLLING_TOTAL_ROWS = 100_000
_MAX_ROLLING_QUEUE_RESULTS = 64
_MAX_ROLLING_HERMES_IDS = 32
_MAX_ROLLING_HERMES_PAYLOAD_BYTES = 16_383
_MAX_ROLLING_HERMES_ID_LENGTH = 4_096
_MAX_ROLLING_STATE_ITEMS = 32
_MAX_ROLLING_STATE_ID_LENGTH = 256
_MAX_ROLLING_STATE_PAYLOAD_BYTES = 60_000
_ROLLING_STATE_RESULT_KEYS = (
    "strategy_version_id",
    "research_trial_id",
    "candidate_id",
    "evidence_window_id",
    "source_class",
    "requested_days",
    "status",
    "reason",
    "next_job",
    "actual_coverage_seconds",
    "observation_completeness",
    "realized_pnl",
    "unrealized_pnl",
    "drawdown",
    "completed_outcomes",
    "reliability",
    "evidence_digest",
    "overlap_key",
)
_ROLLING_HERMES_ID_FIELDS = (
    "strategy_version_ids",
    "research_trial_ids",
    "candidate_ids",
)
_LEGACY_RECOVERY_STATE_NAME = "autonomous-legacy-recovery"
_MAX_AUTOMATIC_REASSESSMENTS = 3
_MUTABLE_DATASET_VERSION_ALIASES = frozenset({"latest", "current", "default", "unversioned"})
CAMPAIGN_PROTOCOL_V1_ID = "polymarket-paper-campaign-v1"
CAMPAIGN_PROTOCOL_V2_ID = "polymarket-paper-campaign-v2"
CAMPAIGN_SCHEMA_V1 = "polymarket-finite-campaign-v1"
CAMPAIGN_SCHEMA_V2 = "polymarket-finite-campaign-v2"

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
CAMPAIGN_TERMINAL_STATUSES = frozenset(
    {
        "COMPLETED_QUALIFIED",
        "CAMPAIGN_EXHAUSTED_NO_QUALIFIED_STRATEGY",
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


def _rolling_identity_values(
    value: Any,
    *names: str,
    max_depth: int = _ROLLING_MAX_PROVENANCE_DEPTH,
) -> tuple[set[str], bool]:
    """Collect candidate identity from bounded authoritative containers.

    Lifecycle payloads carry both identity/provenance and large observation
    manifests.  Walking every value makes an attestation's constituent list
    look like candidate identity and exhausts the traversal budget.  Mapping
    containers remain recursively inspectable for strict conflict detection;
    known bulk arrays are deliberately opaque because they are not identity
    authority.
    """
    values: set[str] = set()
    pending: list[tuple[Any, int, str | None]] = [(value, 0, None)]
    seen: set[int] = set()
    truncated = False
    visited = 0
    wanted = frozenset(names)
    while pending:
        item, depth, parent_key = pending.pop()
        if isinstance(item, Mapping):
            marker = id(item)
            if marker in seen:
                truncated = True
                continue
            seen.add(marker)
            visited += 1
            if visited > 512:
                truncated = True
                break
            for name in wanted:
                raw = item.get(name)
                # Structured identity fields are ambiguous, not absent.  A
                # mapping/list must never be silently ignored and allow the
                # remaining scalar candidate id to pass validation.
                if isinstance(raw, (Mapping, list, tuple)):
                    truncated = True
                    continue
                candidate = _binding_value(raw)
                if candidate:
                    values.add(candidate)
            if depth >= max_depth:
                # Only traversable mappings/sequences make this node
                # truncated; opaque bulk arrays do not consume the budget.
                if any(
                    isinstance(child, Mapping)
                    or (
                        isinstance(child, (list, tuple))
                        and str(key).strip().lower() not in _ROLLING_NON_AUTHORITATIVE_ARRAY_KEYS
                    )
                    for key, child in item.items()
                ):
                    truncated = True
                continue
            children = list(item.items())
            if len(children) > 512:
                truncated = True
                children = children[:512]
            for key, child in reversed(children):
                key_name = str(key).strip().lower()
                if isinstance(child, (list, tuple)):
                    if key_name in _ROLLING_NON_AUTHORITATIVE_ARRAY_KEYS:
                        continue
                    if len(child) > 128:
                        truncated = True
                    pending.append((child, depth + 1, key_name))
                elif isinstance(child, Mapping):
                    pending.append((child, depth + 1, key_name))
        elif isinstance(item, (list, tuple)):
            if parent_key in _ROLLING_NON_AUTHORITATIVE_ARRAY_KEYS:
                continue
            if depth > max_depth:
                if item:
                    truncated = True
                continue
            for child in reversed(item[:128]):
                pending.append((child, depth + 1, parent_key))
            if len(item) > 128:
                truncated = True
    return values, truncated


def _rolling_candidate_values(value: Any) -> tuple[set[str], bool]:
    return _rolling_identity_values(value, "candidate_id", "candidate", "strategy_candidate_id")


def _rolling_payload_without_created_at(value: Mapping[str, Any]) -> dict[str, Any]:
    """Return the complete immutable payload identity for retry comparisons."""
    return {
        str(key): child
        for key, child in value.items()
        if str(key) != "created_at"
    }

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

def _rolling_number(value: Any, default: Decimal = Decimal("0")) -> Decimal:
    if isinstance(value, bool) or value is None:
        return default
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return default
    return parsed if parsed.is_finite() else default


def _rolling_timestamp(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return ensure_utc(value)
    if value is None:
        return None
    try:
        return parse_timestamp(value)
    except (TypeError, ValueError, OverflowError):
        return None


def _rolling_hash(value: Any) -> str:
    return "sha256:" + hashlib.sha256(_canonical_binding(value).encode("utf-8")).hexdigest()
def _rolling_provenance_hash(value: Any) -> str:
    try:
        encoded = _canonical_binding(value)
    except (TypeError, ValueError, OverflowError, RecursionError):
        encoded = repr(value)
    return "sha256:" + hashlib.sha256(encoded.encode("utf-8", "replace")).hexdigest()


def _rolling_provenance_compact(value: Any, depth: int = 0) -> Any:
    if depth >= _ROLLING_MAX_PROVENANCE_DEPTH:
        return {
            "omitted": True,
            "reason": "DEPTH_LIMIT",
            "sha256": _rolling_provenance_hash(value),
        }
    if isinstance(value, Mapping):
        items = sorted(value.items(), key=lambda pair: str(pair[0]))
        result = {
            str(key): _rolling_provenance_compact(child, depth + 1)
            for key, child in items[:64]
        }
        if len(items) > 64:
            result["_omitted_fields"] = [
                str(key) for key, _ in items[64:128]
            ]
            result["_truncated"] = True
        return result
    if isinstance(value, (list, tuple)):
        result = [_rolling_provenance_compact(child, depth + 1) for child in value[:64]]
        if len(value) > 64:
            result.append(
                {
                    "omitted": True,
                    "reason": "ITEM_LIMIT",
                    "sha256": _rolling_provenance_hash(value[64:]),
                }
            )
        return result
    if isinstance(value, str):
        if len(value) <= 1024:
            return value
        return {
            "omitted": True,
            "reason": "STRING_LIMIT",
            "sha256": _rolling_provenance_hash(value),
            "preview": value[:256],
        }
    if isinstance(value, (int, float, bool)) or value is None:
        return value
    return str(value)[:1024]


def _rolling_provenance_bound(value: Mapping[str, Any]) -> dict[str, Any]:
    compacted = _rolling_provenance_compact(value)
    encoded = _canonical_binding(compacted)
    if len(encoded.encode("utf-8")) <= _ROLLING_MAX_PROVENANCE_BYTES:
        return dict(compacted) if isinstance(compacted, Mapping) else {"value": compacted}
    source_hash = _rolling_provenance_hash(value)
    output: dict[str, Any] = {
        "provenance_schema": "axiom-rolling-provenance-v1",
        "provenance_truncated": True,
        "provenance_sha256": source_hash,
        "provenance_original_bytes": len(encoded.encode("utf-8")),
        "provenance_omitted_fields": [],
    }
    priority = (
        "candidate_id",
        "rolling_research",
        "research_mode",
        "mode",
        "validation_version",
        "predecessor_enrollment_id",
        "predecessor_candidate_id",
        "source_candidate_id",
        "source_trial_id",
        "research_trial_id",
        "predecessor_strategy_id",
        "predecessor_strategy_version",
        "source_config_hash",
        "market_scope",
        "market_scope_hash",
        "market_scope_version",
        "scope",
        "dataset_selector",
        "dataset_id",
        "dataset_version",
        "predecessor_provenance",
        "original_rejection",
        "original_economic_outcome",
        "entry_policy",
        "exit_policy",
    )
    keys = list(compacted) if isinstance(compacted, Mapping) else []
    ordered_keys = list(dict.fromkeys([*priority, *sorted(keys)]))
    for key in ordered_keys:
        if key not in compacted:
            continue
        candidate = dict(output)
        candidate[key] = compacted[key]
        if len(_canonical_binding(candidate).encode("utf-8")) <= _ROLLING_MAX_PROVENANCE_BYTES:
            output[key] = compacted[key]
            continue
        output["provenance_omitted_fields"].append(str(key))
    if len(_canonical_binding(output).encode("utf-8")) > _ROLLING_MAX_PROVENANCE_BYTES:
        output["provenance_omitted_fields"] = output["provenance_omitted_fields"][:32]
    while len(_canonical_binding(output).encode("utf-8")) > _ROLLING_MAX_PROVENANCE_BYTES:
        removable = next(
            (
                key
                for key in reversed(list(output))
                if key not in {
                    "provenance_schema",
                    "provenance_truncated",
                    "provenance_sha256",
                    "provenance_original_bytes",
                }
            ),
            None,
        )
        if removable is None:
            break
        output.pop(removable, None)
    return output


def _rolling_state_ids(values: Iterable[Any]) -> list[str]:
    """Project identifiers for durable state without retaining the full lineage."""
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        identifier = str(value or "").strip()
        if not identifier:
            continue
        identifier = identifier[:_MAX_ROLLING_STATE_ID_LENGTH]
        if identifier in seen:
            continue
        seen.add(identifier)
        result.append(identifier)
        if len(result) >= _MAX_ROLLING_STATE_ITEMS:
            break
    return result


def _rolling_state_result(value: Any) -> dict[str, Any]:
    """Keep only compact, scalar evidence fields in restart-facing state."""
    if not isinstance(value, Mapping):
        return {"value": str(value)[:_MAX_ROLLING_STATE_ID_LENGTH]}
    result: dict[str, Any] = {}
    for key in _ROLLING_STATE_RESULT_KEYS:
        if key not in value:
            continue
        item = value[key]
        if key == "requested_days" and isinstance(item, (list, tuple)):
            result[key] = [
                int(day)
                for day in item[:2]
                if isinstance(day, int) and not isinstance(day, bool)
            ]
        elif isinstance(item, (str, int, float, bool)) or item is None:
            result[key] = item if not isinstance(item, str) else item[:_MAX_ROLLING_STATE_ID_LENGTH]
        else:
            result[key] = str(item)[:_MAX_ROLLING_STATE_ID_LENGTH]
    return result
def _rolling_refresh_identity(
    strategies: Sequence[Mapping[str, Any]],
    evidence_rows: Sequence[Mapping[str, Any]],
    pending: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Build an exact, bounded identity for one rolling refresh.

    The operator payload intentionally keeps only samples of the refresh
    material.  The identity is computed from the complete bounded inputs
    before those samples are projected, so a restart can distinguish a real
    enrollment/evidence/shortage transition from the same daily state.
    Timestamps used only for scheduling are excluded from shortage identity.
    """

    def strategy_identity(item: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "strategy_version_id": str(item.get("strategy_version_id", "")).strip(),
            "research_trial_id": str(item.get("research_trial_id", "")).strip() or None,
            "candidate_id": str(item.get("candidate_id", "")).strip() or None,
            "strategy_hash": str(item.get("strategy_hash", "")).strip() or None,
            "config_hash": str(item.get("config_hash", "")).strip() or None,
            "version": str(item.get("version", "")).strip() or None,
        }

    def evidence_identity(item: Mapping[str, Any]) -> dict[str, Any]:
        digest = str(item.get("evidence_digest", "")).strip()
        if not digest:
            digest = _rolling_hash(
                {
                    str(key): value
                    for key, value in item.items()
                    if str(key) not in {"created_at", "measured_at"}
                }
            )
        return {
            "strategy_version_id": str(item.get("strategy_version_id", "")).strip(),
            "research_trial_id": str(item.get("research_trial_id", "")).strip() or None,
            "candidate_id": str(item.get("candidate_id", "")).strip() or None,
            "evidence_window_id": str(item.get("evidence_window_id", "")).strip(),
            "source_class": str(item.get("source_class", "")).strip().upper(),
            "requested_days": item.get("requested_days"),
            "evidence_digest": digest,
        }

    def shortage_identity(item: Mapping[str, Any]) -> dict[str, Any]:
        # Queue scheduling timestamps are not evidence or shortage identity.
        # Keep every other field so changing a retry reason or its next work is
        # observed exactly rather than relying on a lossy sample projection.
        return {
            str(key): value
            for key, value in item.items()
            if str(key) not in {"available_at", "scheduled_at", "created_at"}
        }

    strategy_values = sorted(
        (strategy_identity(item) for item in strategies if isinstance(item, Mapping)),
        key=_canonical_binding,
    )
    evidence_values = sorted(
        (evidence_identity(item) for item in evidence_rows if isinstance(item, Mapping)),
        key=_canonical_binding,
    )
    shortage_values = sorted(
        (shortage_identity(item) for item in pending if isinstance(item, Mapping)),
        key=_canonical_binding,
    )
    material = {
        "schema_version": "rolling-refresh-identity-v1",
        "strategies": strategy_values,
        "evidence": evidence_values,
        "shortages": shortage_values,
    }
    strategy_digest = _rolling_hash(strategy_values)
    evidence_digest = _rolling_hash(evidence_values)
    shortage_digest = _rolling_hash(shortage_values)
    return {
        "schema_version": material["schema_version"],
        "digest": _rolling_hash(material),
        "strategy_digest": strategy_digest,
        "evidence_digest": evidence_digest,
        "shortage_digest": shortage_digest,
        "strategy_versions_total": len(strategy_values),
        "research_trials_total": sum(
            1 for item in strategy_values if item.get("research_trial_id")
        ),
        "evidence_windows_total": len(evidence_values),
        "pending_total": len(shortage_values),
        "strategy_versions": _rolling_state_ids(
            item.get("strategy_version_id") for item in strategy_values
        ),
        "research_trials": _rolling_state_ids(
            item.get("research_trial_id") for item in strategy_values
        ),
        "evidence_windows": _rolling_state_ids(
            item.get("evidence_window_id") for item in evidence_values
        ),
    }


def _rolling_prior_initialization_review(
    prior_state: Mapping[str, Any] | None,
    previous_selection: Mapping[str, Any] | None,
) -> bool:
    """Recognize a persisted no-definition review without guessing from emptiness."""

    records: tuple[Mapping[str, Any], ...] = tuple(
        item for item in (prior_state, previous_selection) if isinstance(item, Mapping)
    )
    for record in records:
        identity = record.get("refresh_identity")
        if isinstance(identity, Mapping):
            try:
                if int(identity.get("strategy_versions_total", -1)) == 0:
                    return True
            except (TypeError, ValueError):
                pass
        for raw_reason in record.get("reasons", ()):
            if str(raw_reason).strip().upper() == "NO_STRATEGY_DEFINITIONS":
                return True
        pending = record.get("pending", ())
        if isinstance(pending, (list, tuple)):
            if any(
                isinstance(item, Mapping)
                and str(item.get("reason", "")).strip().upper()
                == "NO_STRATEGY_DEFINITIONS"
                for item in pending
            ):
                return True
        history = record.get("event_history", ())
        if isinstance(history, (list, tuple)) and history:
            latest = history[-1]
            if isinstance(latest, Mapping):
                if str(latest.get("reason", "")).strip().upper() == "NO_STRATEGY_DEFINITIONS":
                    return True
                if any(
                    str(reason).strip().upper() == "NO_STRATEGY_DEFINITIONS"
                    for reason in latest.get("reasons", ())
                ):
                    return True
    return False


def _rolling_initialization_observe(
    decision: Any,
    refreshed: Mapping[str, Any],
) -> Any:
    """Turn an initialization transition into a paper-only OBSERVE outcome."""

    strategy_ids = {
        str(value).strip()
        for value in refreshed.get("strategy_versions", ())
        if str(value).strip()
    }
    pending_reasons: dict[str, list[str]] = {}
    pending = refreshed.get("pending", ())
    if isinstance(pending, (list, tuple)):
        for item in pending:
            if not isinstance(item, Mapping):
                continue
            strategy_id = str(item.get("strategy_version_id", "")).strip()
            reason = str(item.get("reason", "")).strip() or str(item.get("status", "")).strip()
            if strategy_id and reason:
                pending_reasons.setdefault(strategy_id, []).append(reason)
                strategy_ids.add(strategy_id)

    member_reasons: dict[str, str] = {}
    for member in decision.members:
        strategy_id = str(member.strategy_version_id).strip()
        if strategy_id:
            member_reasons[strategy_id] = (
                str(member.reason).strip() or "INITIALIZATION_OBSERVE"
            )

    reasons = ["INITIALIZATION_OBSERVE"]
    reasons.extend(str(reason) for reason in decision.reasons)
    for strategy_id in sorted(strategy_ids):
        shortage = pending_reasons.get(strategy_id)
        reason = (
            member_reasons.get(strategy_id)
            or (shortage[0] if shortage else None)
            or "INITIALIZATION_OBSERVE"
        )
        reasons.append(f"{strategy_id}:{reason}")
    return replace(
        decision,
        status="OBSERVE",
        members=(),
        reasons=tuple(dict.fromkeys(reasons)),
    )




def _rolling_state_payload(
    *,
    status: str,
    scheduled_at: str,
    queue_item_id: Any,
    queue_status: Any,
    strategies: Sequence[Mapping[str, Any]],
    evidence_rows: Sequence[Mapping[str, Any]],
    pending: Sequence[Mapping[str, Any]],
    source_classes: Sequence[str],
    requested_window_days: Sequence[int],
    source_rows_total: int,
) -> dict[str, Any]:
    """Build a bounded operator-job payload for rolling refresh state."""
    strategy_total = len(strategies)
    evidence_total = len(evidence_rows)
    pending_total = len(pending)
    candidate_values = [item.get("candidate_id") for item in strategies if item.get("candidate_id")]
    candidate_total = len(candidate_values)
    strategy_ids = _rolling_state_ids(item.get("strategy_version_id") for item in strategies)
    trial_ids = _rolling_state_ids(item.get("research_trial_id") for item in strategies)
    candidate_ids = _rolling_state_ids(candidate_values)
    evidence_sample = [_rolling_state_result(item) for item in evidence_rows[:_MAX_ROLLING_STATE_ITEMS]]
    pending_sample = [_rolling_state_result(item) for item in pending[:_MAX_ROLLING_STATE_ITEMS]]
    truncated = {
        "strategy_versions": strategy_total > len(strategy_ids),
        "research_trials": strategy_total > len(trial_ids),
        "candidate_ids": candidate_total > len(candidate_ids),
        "evidence_windows": evidence_total > len(evidence_sample),
        "pending": pending_total > len(pending_sample),
        "source_rows": source_rows_total >= _MAX_ROLLING_TOTAL_ROWS,
    }
    refresh_identity = _rolling_refresh_identity(strategies, evidence_rows, pending)
    state: dict[str, Any] = {
        "status": status,
        "scheduled_at": scheduled_at,
        "queue_item_id": str(queue_item_id or "")[:_MAX_ROLLING_STATE_ID_LENGTH],
        "queue_status": str(queue_status or "")[:_MAX_ROLLING_STATE_ID_LENGTH],
        "strategy_versions": strategy_ids,
        "strategy_versions_total": strategy_total,
        "strategy_versions_truncated": truncated["strategy_versions"],
        "research_trials": trial_ids,
        "research_trials_total": strategy_total,
        "research_trials_truncated": truncated["research_trials"],
        "candidate_ids": candidate_ids,
        "candidate_ids_total": candidate_total,
        "candidate_ids_truncated": truncated["candidate_ids"],
        "evidence_windows": evidence_sample,
        "evidence_windows_total": evidence_total,
        "evidence_windows_truncated": truncated["evidence_windows"],
        "evidence_window_count": evidence_total,
        "pending": pending_sample,
        "next_jobs": sorted(
            {
                str(item.get("next_job")).strip()
                for item in pending
                if str(item.get("next_job", "")).strip()
            }
        ),
        "pending_total": pending_total,
        "pending_truncated": truncated["pending"],
        "source_rows_total": source_rows_total,
        "source_rows_truncated": truncated["source_rows"],
        "source_classes": list(source_classes),
        "requested_window_days": list(requested_window_days),
        "paper_only": True,
        "truncated": any(truncated.values()),
        "refresh_identity": refresh_identity,
        "rolling_review_identity": str(refresh_identity["digest"]),
    }
    while len(_canonical_binding(state).encode("utf-8")) > _MAX_ROLLING_STATE_PAYLOAD_BYTES:
        removable = next(
            (
                name
                for name in ("pending", "evidence_windows", "strategy_versions", "research_trials", "candidate_ids")
                if state[name]
            ),
            None,
        )
        if removable is None:
            break
        state[removable].pop()
        state[f"{removable}_truncated"] = True
        state["truncated"] = True
    return state




def _rolling_unique_ids(values: Iterable[Any], *, limit: int = _MAX_ROLLING_HERMES_IDS) -> list[str]:
    """Return deterministic, bounded identifiers for one Hermes request field."""
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        identifier = str(value or "").strip()
        if (
            not identifier
            or len(identifier) > _MAX_ROLLING_HERMES_ID_LENGTH
            or identifier in seen
        ):
            continue
        seen.add(identifier)
        result.append(identifier)
        if len(result) >= limit:
            break
    return result


def _rolling_hermes_payload(
    strategies: Sequence[Mapping[str, Any]],
    sources: Sequence[str],
    current: datetime,
) -> dict[str, Any]:
    """Build a deduplicated rolling request below ResearchBus's byte limit."""
    payload: dict[str, Any] = {
        "rolling_research": True,
        "strategy_version_ids": _rolling_unique_ids(
            item.get("strategy_version_id") for item in strategies
        ),
        "research_trial_ids": _rolling_unique_ids(
            item.get("research_trial_id") for item in strategies
        ),
        "candidate_ids": _rolling_unique_ids(
            item.get("candidate_id") for item in strategies if item.get("candidate_id")
        ),
        "source_classes": list(sources),
        "requested_window_days": [7, 30],
        "paper_only": True,
    }
    while len(_canonical_binding(payload).encode("utf-8")) >= _MAX_ROLLING_HERMES_PAYLOAD_BYTES:
        fields = [field for field in _ROLLING_HERMES_ID_FIELDS if payload[field]]
        if not fields:
            # The fixed metadata above is intentionally tiny; this fallback
            # keeps the byte-bound invariant true even for pathological input.
            payload = {
                "rolling_research": True,
                "source_classes": list(sources),
                "requested_window_days": [7, 30],
                "paper_only": True,
            }
            break
        field = max(fields, key=lambda name: (len(payload[name]), name))
        payload[field].pop()
    return payload




def _rolling_row_time(row: Mapping[str, Any]) -> datetime | None:
    for key in ("source_timestamp", "timestamp", "observed_at", "event_timestamp", "resolved_at", "time", "created_at"):
        stamp = _rolling_timestamp(row.get(key))
        if stamp is not None:
            return stamp
    return None


def _rolling_source_name(value: Any) -> str:
    text = str(value or "").strip().upper().replace("-", "_").replace(" ", "_")
    aliases = {
        "FORWARD_COLLECTED": "LIVE",
        "ORDER_BOOK_SIMULATED": "REPLAY",
        "REPLAY_SIMULATED": "REPLAY",
        "PRICE_PROXY": "HISTORICAL",
        "PAPER_FORWARD": "PAPER",
        "FORWARD_PAPER": "PAPER",
    }
    return aliases.get(text, text or "HISTORICAL")


_ROLLING_PERSISTED_SOURCE_CLASSES: Mapping[str, str] = {
    "HISTORICAL": "HISTORICAL",
    "REPLAY": "HISTORICAL",
    "PAPER": "PAPER",
    "LIVE": "FORWARD_COLLECTED",
}


def _rolling_persisted_source_class(value: Any) -> str:
    """Map a requested rolling source to the storage source class."""
    source = _rolling_source_name(value)
    return _ROLLING_PERSISTED_SOURCE_CLASSES.get(source, source)


_ROLLING_SOURCE_TYPES: Mapping[str, frozenset[str]] = {
    "HISTORICAL": frozenset({"HISTORICAL", "PRICE_PROXY"}),
    "REPLAY": frozenset({"REPLAY", "REPLAY_SIMULATED", "ORDER_BOOK_SIMULATED", "HISTORICAL"}),
    "PAPER": frozenset({"PAPER", "PAPER_FORWARD", "FORWARD_PAPER"}),
    "LIVE": frozenset({"LIVE", "FORWARD_COLLECTED"}),
}
_ROLLING_SOURCE_QUERY_TYPES: Mapping[str, tuple[str, ...]] = {
    # Storage persists only these source types.  Keep the public rolling class
    # in evidence while translating only the query boundary.
    "REPLAY": ("HISTORICAL",),
    "LIVE": ("FORWARD_COLLECTED",),
}




def _rolling_source_binding(record: Mapping[str, Any]) -> dict[str, Any]:
    """Extract one exact selector/scope binding and reject contradictions."""
    containers: list[Mapping[str, Any]] = [record]
    for name in (
        "payload",
        "provenance",
        "strategy_document",
        "strategy",
        "accounting",
        "canonical_accounting",
        "strategy_accounting",
        "resolved_bet",
    ):
        child = record.get(name)
        if isinstance(child, Mapping):
            containers.append(child)
            nested_provenance = child.get("provenance")
            if isinstance(nested_provenance, Mapping):
                containers.append(nested_provenance)
    selectors: list[Mapping[str, Any]] = []
    for container in containers:
        raw = container.get("dataset_selector")
        if isinstance(raw, Mapping):
            selectors.append(raw)
    fields = (
        "dataset_id",
        "dataset_version",
        "version",
        "market_scope",
        "market_scope_hash",
        "market_scope_version",
        "scope",
        "scope_hash",
        "scope_version",
        "market_id",
        "market_ids",
        "candidate_id",
        "research_trial_id",
        "trial_id",
        "strategy_hash",
        "source_strategy_hash",
        "rolling_strategy_hash",
        "strategy_version_id",
        "experiment_id",
    )
    binding: dict[str, Any] = {}
    direct_fields = set(fields)
    for field in fields:
        values: list[Any] = []
        for selector in selectors:
            value = selector.get(field)
            if value is not None and value != "":
                values.append(value)
        for container in containers:
            if field in direct_fields:
                # Dataset aliases are read from dataset_selector above.  A
                # nested strategy document's DSL ``version`` is not a
                # dataset version.
                if field == "version" and (
                    container is not record
                    or "strategy_version_id" in container
                    or "strategy_hash" in container
                ):
                    continue
                value = container.get(field)
                if value is not None and value != "":
                    values.append(value)
        distinct = {_canonical_binding(value) for value in values}
        if len(distinct) > 1:
            raise ValueError(f"conflicting rolling dataset/scope selector: {field}")
        if values:
            binding[field] = values[0]
    if "trial_id" in binding:
        if (
            "research_trial_id" in binding
            and _canonical_binding(binding["research_trial_id"])
            != _canonical_binding(binding["trial_id"])
        ):
            raise ValueError("conflicting rolling dataset/scope selector: research_trial_id")
        binding.setdefault("research_trial_id", binding["trial_id"])
    return binding


_ROLLING_SOURCE_BINDING_FIELDS = (
    "dataset_id",
    "dataset_version",
    "version",
    "strategy_hash",
    "strategy_version_id",
    "research_trial_id",
    "candidate_id",
)


def _rolling_source_job_binding(
    record: Mapping[str, Any],
    source: str,
    *,
    experiment_id: str | None = None,
) -> dict[str, Any]:
    """Require the immutable identity carried by a standard source loader."""
    binding = _rolling_source_binding(record)
    expected: dict[str, Any] = {}
    for field in ("strategy_hash", "strategy_version_id", "research_trial_id", "candidate_id"):
        value = _binding_value(binding.get(field))
        if value is None:
            value = _binding_value(record.get(field))
        if value is None:
            raise ValueError(f"{source}_SOURCE_BINDING_REQUIRED")
        expected[field] = value
    if source in {"HISTORICAL", "REPLAY"}:
        dataset_id = _binding_value(binding.get("dataset_id"))
        dataset_version = _binding_value(binding.get("dataset_version", binding.get("version")))
        if dataset_id is None or dataset_version is None:
            raise ValueError(f"{source}_DATASET_BINDING_REQUIRED")
        expected["dataset_id"] = dataset_id
        expected["dataset_version"] = dataset_version
    if experiment_id is not None:
        expected["experiment_id"] = experiment_id
    return expected


def _rolling_inject_source_binding(
    raw: Mapping[str, Any],
    expected: Mapping[str, Any],
) -> dict[str, Any]:
    """Inject absent row duplicates while rejecting contradictory lineage."""
    row = dict(raw)
    try:
        current = _rolling_source_binding(row)
    except (TypeError, ValueError):
        row["_rolling_accounting_rejection"] = "SOURCE_BINDING_CONFLICT"
        return row
    for field in _ROLLING_SOURCE_BINDING_FIELDS:
        expected_value = _binding_value(expected.get(field))
        if expected_value is None:
            continue
        actual_value = _binding_value(current.get(field))
        if actual_value is not None and actual_value != expected_value:
            row["_rolling_accounting_rejection"] = "SOURCE_BINDING_CONFLICT"
            return row
        row.setdefault(field, expected_value)
    expected_experiment = _binding_value(expected.get("experiment_id"))
    if expected_experiment is not None:
        actual_experiment = _binding_value(current.get("experiment_id"))
        if actual_experiment is not None and actual_experiment != expected_experiment:
            row["_rolling_accounting_rejection"] = "PAPER_EXPERIMENT_BINDING_CONFLICT"
            return row
        row.setdefault("experiment_id", expected_experiment)
    return row



def _rolling_rule_scope_market_ids(
    store: Any,
    record: Mapping[str, Any],
    binding: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
    now: datetime,
) -> set[str] | None:
    """Return the exact market set authorized by a RULE_BASED_MARKETS scope."""
    scope = binding.get("market_scope", binding.get("scope"))
    if not isinstance(scope, Mapping):
        return None
    mode = str(scope.get("mode", "")).strip().upper()
    if mode != "RULE_BASED_MARKETS":
        return None
    candidate_id = _binding_value(binding.get("candidate_id"))
    scope_hash = _binding_value(
        binding.get("market_scope_hash", binding.get("scope_hash"))
    )
    scope_version = _binding_value(
        binding.get("market_scope_version", binding.get("scope_version"))
    )
    if not candidate_id or not scope_hash or not scope_version:
        raise ValueError("RULE_BASED_MARKET_SCOPE_BINDING_REQUIRED")
    loader = getattr(store, "load_market_scope_resolution", None)
    resolution: Any = None
    if callable(loader):
        try:
            resolution = loader(
                candidate_id,
                scope_hash=scope_hash,
                scope_version=scope_version,
            )
        except TypeError:
            try:
                resolution = loader(candidate_id)
            except Exception:
                resolution = None
        except Exception:
            resolution = None
    if resolution is not None:
        if hasattr(resolution, "as_dict") and callable(resolution.as_dict):
            resolution = resolution.as_dict()
        if not isinstance(resolution, Mapping):
            raise ValueError("RULE_BASED_MARKET_SCOPE_RESOLUTION_INVALID")
        if (
            _binding_value(resolution.get("candidate_id")) != candidate_id
            or _binding_value(resolution.get("scope_hash")) != scope_hash
            or _binding_value(resolution.get("scope_version")) != scope_version
            or str(resolution.get("status", "")).strip().upper() != "MATCHED"
        ):
            raise ValueError("RULE_BASED_MARKET_SCOPE_RESOLUTION_INVALID")
        matched = resolution.get("matched_markets", ())
        if not isinstance(matched, (list, tuple)):
            raise ValueError("RULE_BASED_MARKET_SCOPE_RESOLUTION_INVALID")
        result: set[str] = set()
        for market in matched:
            if isinstance(market, Mapping):
                market_id = _binding_value(
                    market.get("market_id", market.get("id"))
                )
            else:
                market_id = _binding_value(market)
            if market_id:
                result.add(market_id)
        return result
    # Custom stores may not persist resolutions.  Reuse the canonical resolver
    # against their bounded inventory rather than implementing a second rule
    # matcher here; unresolved or partial results fail closed.
    try:
        from .market_scope import resolve_market_scope
    except (ImportError, AttributeError) as exc:
        raise ValueError("RULE_BASED_MARKET_SCOPE_RESOLVER_UNAVAILABLE") from exc
    inventory: dict[str, Mapping[str, Any]] = {}
    for raw in rows:
        if not isinstance(raw, Mapping):
            continue
        view = _rolling_snapshot_view(raw)
        market_id = _binding_value(
            view.get("market_id", view.get("condition_id", view.get("id")))
        )
        if market_id and market_id not in inventory:
            inventory[market_id] = view
    try:
        resolved = resolve_market_scope(
            candidate_id,
            record,
            tuple(inventory.values()),
            resolved_at=now,
        )
    except Exception as exc:
        raise ValueError("RULE_BASED_MARKET_SCOPE_RESOLUTION_FAILED") from exc
    resolved_status = str(getattr(resolved, "status", "")).strip().upper()
    if resolved_status != "MATCHED":
        raise ValueError("RULE_BASED_MARKET_SCOPE_RESOLUTION_INCOMPLETE")
    matched = getattr(resolved, "matched_markets", ())
    return {
        market_id
        for market in matched
        if (market_id := _binding_value(getattr(market, "market_id", None)))
    }
def _rolling_document_is_marked(value: Mapping[str, Any], provenance: Mapping[str, Any] | None = None) -> bool:
    containers = [value]
    if isinstance(provenance, Mapping):
        containers.append(provenance)
    for field_name in ("provenance", "strategy_document", "strategy", "canonical_strategy", "payload"):
        nested = value.get(field_name)
        if isinstance(nested, Mapping):
            containers.append(nested)
            if isinstance(nested.get("provenance"), Mapping):
                containers.append(nested["provenance"])
    for item in containers:
        if item.get("rolling_research") is True or item.get("rolling") is True:
            return True
        if str(item.get("research_mode", "")).strip().upper() in {"ROLLING", "ROLLING_RESEARCH"}:
            return True
        if str(item.get("trial_kind", "")).strip().upper() == "ROLLING_RESEARCH":
            return True
        if str(item.get("source", "")).strip().lower() == "rolling":
            return True
    return False
def _rolling_campaign_bound(value: Mapping[str, Any]) -> bool:
    """Reject campaign lineage while bounding unrelated bulk observations.

    ``experiment_plan`` is an authoritative campaign container and is checked
    before any general traversal.  Large attestation/observation arrays are
    not campaign authority; treating their length as a campaign marker would
    incorrectly reject otherwise valid frozen candidates.
    """
    seen: set[int] = set()
    visited = 0

    def direct_marker(item: Mapping[str, Any]) -> bool:
        if any(
            _binding_value(item.get(name))
            for name in ("campaign_id", "campaign_trial_id", "campaign_configuration_id")
        ):
            return True
        protocol = item.get("campaign_protocol")
        if (
            isinstance(protocol, Mapping)
            or (protocol is not None and str(protocol).strip())
            or str(item.get("source", "")).strip().lower()
            in {"axiom-finite-campaign", "finite-campaign"}
        ):
            return True
        return False

    def visit(item: Any, depth: int = 0, parent_key: str | None = None) -> bool:
        nonlocal visited
        if isinstance(item, (list, tuple)):
            if parent_key in _ROLLING_NON_AUTHORITATIVE_ARRAY_KEYS:
                return False
            if depth > _ROLLING_MAX_PROVENANCE_DEPTH:
                return True
            if len(item) > 128:
                # Unknown bulk arrays remain fail-closed.  Known observation
                # arrays are handled above and are intentionally ignored.
                return True
            return any(visit(child, depth + 1, parent_key) for child in item)
        if not isinstance(item, Mapping):
            return False
        marker = id(item)
        if marker in seen:
            return True
        seen.add(marker)
        visited += 1
        if visited > 512 or depth > _ROLLING_MAX_PROVENANCE_DEPTH:
            return True
        if direct_marker(item):
            return True
        # Check this authoritative container first.  This prevents a large
        # unrelated field encountered earlier in insertion order from hiding
        # a campaign marker nested in the plan.
        plan = item.get("experiment_plan")
        if isinstance(plan, Mapping) and visit(plan, depth + 1, "experiment_plan"):
            return True
        children = list(item.items())
        if len(children) > 512:
            return True
        for key, child in children:
            key_name = str(key).strip().lower()
            if key_name == "experiment_plan":
                continue
            if isinstance(child, (list, tuple)) and key_name in _ROLLING_NON_AUTHORITATIVE_ARRAY_KEYS:
                continue
            if visit(child, depth + 1, key_name):
                return True
        return False

    return visit(value)


def _rolling_snapshot_view(row: Mapping[str, Any]) -> dict[str, Any]:
    """Merge persisted snapshot/result payloads before terminal metric reads."""
    result: dict[str, Any] = dict(row)
    pending = [row.get(name) for name in ("payload", "snapshot", "data", "observation", "result", "outcome")]
    seen: set[int] = set()
    while pending:
        value = pending.pop(0)
        if not isinstance(value, Mapping) or id(value) in seen:
            continue
        seen.add(id(value))
        for key, child in value.items():
            if key not in result or result.get(key) in (None, ""):
                result[str(key)] = child
        pending.extend(value.get(name) for name in ("payload", "snapshot", "data", "observation", "result", "outcome"))
    return result




def _rolling_overlap_key(rows: Sequence[Mapping[str, Any]]) -> str:
    exposures: set[str] = set()
    for raw in rows:
        row = _rolling_snapshot_view(raw)
        for key in ("market_id", "market_ids", "universe_id", "universe_version", "market_scope_hash", "scope_hash"):
            value = row.get(key)
            if isinstance(value, (list, tuple, set, frozenset)):
                exposures.update(str(item).strip() for item in value if str(item).strip())
            elif value is not None and str(value).strip():
                exposures.add(str(value).strip())
    if not exposures:
        return "unknown"
    return "exposure:" + _rolling_hash(sorted(exposures)).removeprefix("sha256:")[:40]

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

    # Rolling portfolio -------------------------------------------------
    def _rolling_strategy_documents(self) -> tuple[dict[str, Any], ...]:
        """Discover mature legacy candidates and validate them for rolling research.

        Legacy strategy rows are never promoted in place.  They are joined to
        their current lifecycle row by the canonical candidate id, then copied
        into an application-owned rolling identity.  Every rejected join is
        recorded as an immutable enrollment decision.
        """
        found: dict[str, dict[str, Any]] = {}
        enrollment_saver = getattr(self.store, "save_rolling_enrollment", None)

        def decode(value: Any) -> Mapping[str, Any]:
            if isinstance(value, Mapping):
                return value
            if isinstance(value, str):
                try:
                    parsed = json.loads(value)
                except (TypeError, ValueError, json.JSONDecodeError):
                    return {}
                return parsed if isinstance(parsed, Mapping) else {}
            return {}

        def compact(value: Any, depth: int = 0) -> Any:
            return _rolling_provenance_compact(value, depth)

        def candidate_id(row: Mapping[str, Any], payload: Mapping[str, Any]) -> str | None:
            values: set[str] = set()
            truncated = False
            for source in (row, payload):
                found_values, source_truncated = _rolling_candidate_values(source)
                values.update(found_values)
                truncated = truncated or source_truncated
            if truncated or len(values) != 1:
                return None
            return sorted(values)[0]

        def source_value(payload: Mapping[str, Any], *names: str) -> str | None:
            for name in names:
                value = _binding_value(payload.get(name))
                if value:
                    return value
            for name in ("provenance", "forward_evidence", "result", "outcome", "config"):
                child = payload.get(name)
                if isinstance(child, Mapping):
                    value = source_value(child, *names)
                    if value:
                        return value
            return None

        def provenance(
            *,
            candidate: str,
            payload: Mapping[str, Any],
            strategy_row: Mapping[str, Any],
            scope: Mapping[str, Any],
            entry: Mapping[str, Any],
            exit_policy: Mapping[str, Any],
        ) -> dict[str, Any]:
            original_rejection = {
                key: compact(payload[key])
                for key in (
                    "rejection_reason",
                    "reason",
                    "reason_code",
                    "status",
                )
                if key in payload
            }
            original_economic = {
                key: compact(payload[key])
                for key in (
                    "economic_result",
                    "economic_outcome",
                    "result",
                    "outcome",
                    "validation",
                    "forward_evidence",
                    "metrics",
                    "net_return",
                    "realized_pnl",
                    "unrealized_pnl",
                )
                if key in payload
            }
            result = {
                "rolling_research": True,
                "research_mode": "RESEARCH",
                "predecessor_candidate_id": candidate,
                "source_candidate_id": candidate,
                "source_trial_id": source_value(
                    payload,
                    "research_trial_id",
                    "trial_id",
                    "source_trial_id",
                    "forward_test_id",
                    "experiment_id",
                    "plan_id",
                ),
                "source_config_hash": source_value(
                    payload,
                    "config_hash",
                    "source_config_hash",
                ),
                "predecessor_strategy_id": _binding_value(strategy_row.get("strategy_id")),
                "predecessor_strategy_version": _binding_value(strategy_row.get("version")),
                "scope": compact(scope),
                "entry_policy": compact(entry),
                "exit_policy": compact(exit_policy),
                "original_economic_outcome": original_economic,
                "original_rejection": original_rejection,
            }
            return _rolling_provenance_bound(result)
        def origin_base(
            *,
            candidate: str,
            payload: Mapping[str, Any],
            strategy_row: Mapping[str, Any],
        ) -> dict[str, Any]:
            original_provenance = (
                compact(payload["provenance"])
                if "provenance" in payload
                else {}
            )
            return {
                "rolling_research": True,
                "predecessor_candidate_id": candidate,
                "source_candidate_id": candidate,
                "source_trial_id": source_value(
                    payload,
                    "research_trial_id",
                    "trial_id",
                    "source_trial_id",
                    "forward_test_id",
                    "experiment_id",
                    "plan_id",
                ),
                "source_config_hash": source_value(
                    payload,
                    "config_hash",
                    "source_config_hash",
                ),
                "source_strategy_hash": source_value(
                    payload,
                    "source_strategy_hash",
                    "strategy_hash",
                ),
                "rolling_strategy_hash": source_value(
                    payload,
                    "rolling_strategy_hash",
                ),
                "predecessor_strategy_id": _binding_value(strategy_row.get("strategy_id")),
                "predecessor_strategy_version": _binding_value(strategy_row.get("version")),
                "original_provenance": original_provenance,
                "original_economic_outcome": {
                    key: compact(payload[key])
                    for key in (
                        "economic_result",
                        "economic_outcome",
                        "result",
                        "outcome",
                        "validation",
                        "forward_evidence",
                        "metrics",
                        "net_return",
                        "realized_pnl",
                        "unrealized_pnl",
                    )
                    if key in payload
                },
                "original_rejection": {
                    key: compact(payload[key])
                    for key in ("rejection_reason", "reason", "reason_code", "status")
                    if key in payload
                },
            }

        def campaign_markers(*sources: Mapping[str, Any]) -> dict[str, Any]:
            marker_names = (
                "campaign_id",
                "campaign_trial_id",
                "campaign_configuration_id",
                "campaign_protocol",
            )
            markers: dict[str, Any] = {}
            seen: set[int] = set()
            visited = 0

            def visit(item: Any, depth: int = 0, parent_key: str | None = None) -> None:
                nonlocal visited
                if isinstance(item, (list, tuple)):
                    if parent_key in _ROLLING_NON_AUTHORITATIVE_ARRAY_KEYS:
                        return
                    if depth > _ROLLING_MAX_PROVENANCE_DEPTH or len(item) > 128:
                        return
                    for child in item:
                        visit(child, depth + 1, parent_key)
                    return
                if not isinstance(item, Mapping):
                    return
                marker = id(item)
                if marker in seen:
                    return
                seen.add(marker)
                visited += 1
                if visited > 512 or depth > _ROLLING_MAX_PROVENANCE_DEPTH:
                    return
                plan = item.get("experiment_plan")
                if isinstance(plan, Mapping):
                    visit(plan, depth + 1, "experiment_plan")
                for name in marker_names:
                    if name in item and name not in markers:
                        markers[name] = compact(item[name])
                source = str(item.get("source", "")).strip().lower()
                if source in {"axiom-finite-campaign", "finite-campaign"}:
                    markers.setdefault("source", item.get("source"))
                for key, child in item.items():
                    key_name = str(key).strip().lower()
                    if key_name == "experiment_plan":
                        continue
                    if (
                        isinstance(child, (list, tuple))
                        and key_name in _ROLLING_NON_AUTHORITATIVE_ARRAY_KEYS
                    ):
                        continue
                    visit(child, depth + 1, key_name)

            for source in sources:
                visit(source)
            return markers

        enrollment_lister = getattr(self.store, "list_rolling_enrollments", None)

        def predecessor_for(candidate: str) -> tuple[str | None, Mapping[str, Any] | None]:
            if not callable(enrollment_lister):
                return None, None
            try:
                records = enrollment_lister(candidate_id=candidate, limit=256)
            except (TypeError, ValueError, RuntimeError):
                return None, None
            if not isinstance(records, (list, tuple)):
                return None, None
            for prior in records:
                if not isinstance(prior, Mapping):
                    continue
                prior_id = _binding_value(prior.get("enrollment_id"))
                prior_version = _binding_value(prior.get("validation_version"))
                if (
                    prior_id
                    and str(prior.get("status", "")).strip().upper() == "EXCLUDED"
                    and prior_version != _ROLLING_ENROLLMENT_VALIDATION_VERSION
                ):
                    return prior_id, prior
            return None, None

        def enrollment_identity(
            *,
            candidate: str,
            status: str,
            reason: str,
            strategy_version_id: str | None,
            research_trial_id: str | None,
        ) -> str:
            material = {
                "candidate_id": candidate,
                "validation_version": _ROLLING_ENROLLMENT_VALIDATION_VERSION,
                "strategy_version_id": strategy_version_id,
                "research_trial_id": research_trial_id,
                "status": status,
                "reason": reason,
            }
            return "rolling-enrollment-" + _rolling_hash(material).removeprefix("sha256:")[:48]

        def decision(
            *,
            candidate: str,
            status: str,
            reason: str,
            origin: Mapping[str, Any],
            strategy_version_id: str | None = None,
            research_trial_id: str | None = None,
        ) -> None:
            if not callable(enrollment_saver):
                return
            normalized_status = str(status).strip().upper()
            predecessor_id, predecessor = predecessor_for(candidate)
            bounded_origin = dict(origin)
            bounded_origin["validation_version"] = _ROLLING_ENROLLMENT_VALIDATION_VERSION
            if predecessor_id:
                bounded_origin["predecessor_enrollment_id"] = predecessor_id
                prior_provenance = predecessor.get("provenance") if isinstance(predecessor, Mapping) else None
                if isinstance(prior_provenance, Mapping):
                    bounded_origin["predecessor_provenance"] = _rolling_provenance_bound(prior_provenance)
            record = {
                "enrollment_id": enrollment_identity(
                    candidate=candidate,
                    status=normalized_status,
                    reason=reason,
                    strategy_version_id=strategy_version_id,
                    research_trial_id=research_trial_id,
                ),
                "candidate_id": candidate,
                "strategy_version_id": strategy_version_id,
                "research_trial_id": research_trial_id,
                "status": normalized_status,
                "reason": reason,
                "validation_version": _ROLLING_ENROLLMENT_VALIDATION_VERSION,
                "predecessor_enrollment_id": predecessor_id,
                "provenance": _rolling_provenance_bound(bounded_origin),
            }
            try:
                enrollment_saver(record)
            except (TypeError, ValueError, RuntimeError):
                # A corrupt legacy row must not prevent other candidates from
                # being discovered.  The immutable source remains untouched.
                return

        def validate_join(
            strategy_row: Mapping[str, Any],
            lifecycle_row: Mapping[str, Any],
        ) -> dict[str, Any] | None:
            strategy_payload = decode(strategy_row.get("strategy_payload"))
            lifecycle_payload = decode(lifecycle_row.get("candidate_payload"))
            row_candidate = _binding_value(lifecycle_row.get("candidate_id"))
            # Campaign provenance is authoritative and must win before the
            # general identity walk.  Otherwise a large unrelated attestation
            # can mask the required CAMPAIGN_BOUND exclusion.
            if _rolling_campaign_bound(strategy_payload) or _rolling_campaign_bound(lifecycle_payload):
                campaign_candidates, _ = _rolling_candidate_values(
                    {"candidate_id": row_candidate, "payload": lifecycle_payload}
                )
                campaign_candidate = row_candidate or (
                    sorted(campaign_candidates)[0] if len(campaign_candidates) == 1 else None
                )
                if campaign_candidate:
                    campaign_origin = origin_base(
                        candidate=campaign_candidate,
                        payload=lifecycle_payload,
                        strategy_row=strategy_row,
                    )
                    markers = campaign_markers(strategy_payload, lifecycle_payload)
                    campaign_origin.update(markers)
                    campaign_origin["campaign_markers"] = dict(markers)
                    campaign_origin["campaign_excluded"] = True
                    decision(
                        candidate=campaign_candidate,
                        status="EXCLUDED",
                        reason="CAMPAIGN_BOUND",
                        origin=campaign_origin,
                    )
                return None
            lifecycle_candidates, lifecycle_truncated = _rolling_candidate_values(
                {"candidate_id": row_candidate, "payload": lifecycle_payload}
            )
            if lifecycle_truncated or len(lifecycle_candidates) != 1:
                if row_candidate:
                    decision(
                        candidate=row_candidate,
                        status="EXCLUDED",
                        reason="CANDIDATE_IDENTITY_MISMATCH",
                        origin={
                            "rolling_research": True,
                            "predecessor_candidate_id": row_candidate,
                            "source_candidate_id": sorted(lifecycle_candidates),
                        },
                    )
                return None
            candidate = sorted(lifecycle_candidates)[0]
            strategy_candidates, strategy_truncated = _rolling_candidate_values(strategy_payload)
            if strategy_truncated or (strategy_candidates and strategy_candidates != {candidate}):
                decision(
                    candidate=candidate,
                    status="EXCLUDED",
                    reason="CANDIDATE_IDENTITY_MISMATCH",
                    origin={
                        "rolling_research": True,
                        "predecessor_candidate_id": candidate,
                        "source_candidate_id": sorted(strategy_candidates),
                        "source_trial_id": source_value(
                            lifecycle_payload,
                            "research_trial_id",
                            "trial_id",
                            "source_trial_id",
                            "forward_test_id",
                            "experiment_id",
                            "plan_id",
                        ),
                        "original_economic_outcome": {
                            key: compact(lifecycle_payload[key])
                            for key in ("result", "outcome", "economic_result", "economic_outcome")
                            if key in lifecycle_payload
                        },
                        "original_rejection": {
                            key: compact(lifecycle_payload[key])
                            for key in ("rejection_reason", "reason", "reason_code", "status")
                            if key in lifecycle_payload
                        },
                    },
                )
                return None
            base_origin = origin_base(
                candidate=candidate,
                payload=lifecycle_payload,
                strategy_row=strategy_row,
            )
            stage = str(lifecycle_row.get("stage", "")).strip().upper()
            if stage not in _ROLLING_MATURE_STAGES:
                decision(
                    candidate=candidate,
                    status="EXCLUDED",
                    reason="LIFECYCLE_NOT_MATURE",
                    origin={**base_origin, "lifecycle_stage": stage},
                )
                return None
            source_candidates = (
                strategy_payload.get("strategy_document"),
                strategy_payload.get("strategy"),
                strategy_payload.get("payload"),
                lifecycle_payload.get("strategy_document"),
                lifecycle_payload.get("strategy"),
            )
            source = next(
                (
                    value
                    for value in source_candidates
                    if isinstance(value, Mapping)
                ),
                None,
            )
            if not isinstance(source, Mapping) and any(
                key in strategy_payload
                for key in ("family", "template", "market_type", "parameters")
            ):
                # ``save_strategy`` persists the DSL document directly,
                # whereas rolling-generated rows carry it under
                # ``strategy_document``.  Both are immutable definitions.
                source = strategy_payload
            if not isinstance(source, Mapping):
                decision(
                    candidate=candidate,
                    status="EXCLUDED",
                    reason="STRATEGY_DEFINITION_MISSING",
                    origin=base_origin,
                )
                return None
            try:
                definition = load_strategy(source)
            except (TypeError, ValueError, RuntimeError) as exc:
                decision(
                    candidate=candidate,
                    status="EXCLUDED",
                    reason="STRATEGY_DSL_INVALID",
                    origin={**base_origin, "error": str(exc)[:256]},
                )
                return None
            scope_value = lifecycle_payload.get("market_scope", lifecycle_payload.get("scope"))
            if not isinstance(scope_value, Mapping):
                plan = lifecycle_payload.get("experiment_plan")
                scope_value = plan.get("market_scope") if isinstance(plan, Mapping) else None
            if not isinstance(scope_value, Mapping):
                decision(
                    candidate=candidate,
                    status="EXCLUDED",
                    reason="SCOPE_INVALID",
                    origin={
                        **base_origin,
                        "next_action": "PERSIST_EXPLICIT_MARKET_SCOPE_AND_SCOPE_IDENTITY",
                        "next_work": "PERSIST_EXPLICIT_MARKET_SCOPE_AND_SCOPE_IDENTITY",
                    },
                )
                return None
            try:
                scope_policy = normalize_market_scope(scope_value)
            except (TypeError, ValueError, ExperimentPlanError) as exc:
                decision(
                    candidate=candidate,
                    status="EXCLUDED",
                    reason="SCOPE_INVALID",
                    origin={
                        **base_origin,
                        "error": str(exc)[:256],
                        "next_action": "REPAIR_EXPLICIT_MARKET_SCOPE_AND_SCOPE_IDENTITY",
                        "next_work": "REPAIR_EXPLICIT_MARKET_SCOPE_AND_SCOPE_IDENTITY",
                    },
                )
                return None
            supplied_hash = source_value(lifecycle_payload, "market_scope_hash", "scope_hash")
            supplied_version = source_value(lifecycle_payload, "market_scope_version", "scope_version")
            if supplied_hash and supplied_hash != scope_policy.scope_hash:
                decision(
                    candidate=candidate,
                    status="EXCLUDED",
                    reason="SCOPE_IDENTITY_MISMATCH",
                    origin={
                        **base_origin,
                        "next_action": "REPAIR_MARKET_SCOPE_IDENTITY",
                        "next_work": "REPAIR_MARKET_SCOPE_IDENTITY",
                    },
                )
                return None
            if supplied_version and supplied_version != scope_policy.scope_version:
                decision(
                    candidate=candidate,
                    status="EXCLUDED",
                    reason="SCOPE_IDENTITY_MISMATCH",
                    origin={
                        **base_origin,
                        "next_action": "REPAIR_MARKET_SCOPE_IDENTITY",
                        "next_work": "REPAIR_MARKET_SCOPE_IDENTITY",
                    },
                )
                return None
            plan = lifecycle_payload.get("experiment_plan")
            plan = plan if isinstance(plan, Mapping) else {}
            dataset_selector_value = lifecycle_payload.get("dataset_selector")
            if not isinstance(dataset_selector_value, Mapping):
                dataset_selector_value = plan.get("dataset_selector")
            dataset_selector = (
                dict(dataset_selector_value)
                if isinstance(dataset_selector_value, Mapping)
                else {}
            )
            for field_name in ("dataset_id", "dataset_version"):
                selector_names = (
                    ("dataset_version", "version")
                    if field_name == "dataset_version"
                    else ("dataset_id",)
                )
                direct_value = lifecycle_payload.get(field_name)
                plan_value = plan.get(field_name)
                selector_values = [
                    dataset_selector.get(name)
                    for name in selector_names
                    if dataset_selector.get(name) is not None
                    and str(dataset_selector.get(name)).strip()
                ]
                values = [
                    value
                    for value in (direct_value, plan_value, *selector_values)
                    if value is not None and str(value).strip()
                ]
                if len({_canonical_binding(value) for value in values}) > 1:
                    decision(
                        candidate=candidate,
                        status="EXCLUDED",
                        reason="DATASET_SELECTOR_IDENTITY_MISMATCH",
                        origin=base_origin,
                    )
                    return None
                if values:
                    dataset_selector[field_name] = values[0]
            entry_value = lifecycle_payload.get("entry_policy", lifecycle_payload.get("entry"))
            if entry_value is None:
                entry_value = plan.get("entry_policy", plan.get("entry"))
            if entry_value is None:
                entry_value = {"type": "strategy_signal", "family": definition.family}
            if isinstance(entry_value, str):
                entry_value = {"type": entry_value}
            if not isinstance(entry_value, Mapping) or not str(
                entry_value.get("type", entry_value.get("kind", ""))
            ).strip():
                decision(candidate=candidate, status="EXCLUDED", reason="ENTRY_POLICY_INVALID", origin=base_origin)
                return None
            exit_value = lifecycle_payload.get("exit_policy", lifecycle_payload.get("exit"))
            if exit_value is None:
                exit_value = plan.get("exit_policy", plan.get("exit"))
            if exit_value is None:
                exit_value = {"type": "fixed_holding_period", "holding_period": 1}
            if isinstance(exit_value, str):
                exit_value = {"type": exit_value}
            if not isinstance(exit_value, Mapping):
                decision(candidate=candidate, status="EXCLUDED", reason="EXIT_POLICY_INVALID", origin=base_origin)
                return None
            exit_kind = str(exit_value.get("type", exit_value.get("kind", ""))).strip().lower()
            if exit_kind != "fixed_holding_period":
                decision(candidate=candidate, status="EXCLUDED", reason="EXIT_POLICY_INVALID", origin=base_origin)
                return None
            raw_period = exit_value.get(
                "holding_period",
                exit_value.get("bars", exit_value.get("observations", 1)),
            )
            try:
                holding_period = int(raw_period)
            except (TypeError, ValueError, OverflowError):
                holding_period = 0
            if isinstance(raw_period, bool) or holding_period < 1 or holding_period > 10_000:
                decision(candidate=candidate, status="EXCLUDED", reason="EXIT_POLICY_INVALID", origin=base_origin)
                return None
            mode_value = source_value(lifecycle_payload, "enrollment_mode", "research_mode", "mode")
            mode = str(mode_value or "").strip().upper().replace("-", "_")
            if mode in {"LIVE", "CANARY", "EXECUTION", "TRADING"}:
                decision(candidate=candidate, status="EXCLUDED", reason="ENROLLMENT_MODE_NOT_ALLOWED", origin=base_origin)
                return None
            origin = provenance(
                candidate=candidate,
                payload=lifecycle_payload,
                strategy_row=strategy_row,
                scope=scope_policy.as_dict(),
                entry=dict(entry_value),
                exit_policy={**dict(exit_value), "holding_period": holding_period},
            )
            origin["market_scope"] = scope_policy.as_dict()
            origin["market_scope_hash"] = scope_policy.scope_hash
            origin["market_scope_version"] = scope_policy.scope_version
            if dataset_selector:
                origin["dataset_selector"] = compact(dataset_selector)
                for field_name in ("dataset_id", "dataset_version"):
                    origin[field_name] = dataset_selector[field_name]
            document = definition.to_dict()
            document.pop("strategy_id", None)
            strategy_hash = _rolling_hash(document)
            config_hash = source_value(lifecycle_payload, "config_hash", "source_config_hash")
            computed_strategy_version_id = "strategy-version-" + _rolling_hash(
                {"strategy_hash": strategy_hash, "candidate_id": candidate}
            ).removeprefix("sha256:")[:40]
            declared_strategy_version_id = _binding_value(
                lifecycle_payload.get("strategy_version_id")
            )
            if (
                declared_strategy_version_id is not None
                and declared_strategy_version_id != computed_strategy_version_id
            ):
                decision(
                    candidate=candidate,
                    status="EXCLUDED",
                    reason="STRATEGY_VERSION_IDENTITY_MISMATCH",
                    origin=base_origin,
                    strategy_version_id=declared_strategy_version_id,
                )
                return None
            strategy_version_id = declared_strategy_version_id or computed_strategy_version_id
            trial_identity = {
                "candidate_id": candidate,
                "strategy_version_id": strategy_version_id,
                "source_trial_id": origin.get("source_trial_id"),
                "source_config_hash": config_hash,
            }
            computed_research_trial_id = "research-trial-" + _rolling_hash(
                trial_identity
            ).removeprefix("sha256:")[:40]
            research_trial_id = (
                _binding_value(lifecycle_payload.get("research_trial_id"))
                or _binding_value(lifecycle_payload.get("trial_id"))
                or computed_research_trial_id
            )
            origin["source_config_hash"] = config_hash
            origin["source_strategy_hash"] = (
                base_origin.get("source_strategy_hash") or strategy_hash
            )
            origin["rolling_strategy_hash"] = strategy_hash
            origin["research_trial_id"] = research_trial_id
            origin["validation_version"] = _ROLLING_ENROLLMENT_VALIDATION_VERSION
            origin["mode"] = "OBSERVATION" if stage == "PAPER_FORWARD" else "RESEARCH"
            origin = _rolling_provenance_bound(origin)
            return {
                "strategy_document": document,
                "strategy_id": _binding_value(strategy_row.get("strategy_id")) or definition.id,
                "version": str(strategy_row.get("version") or definition.version),
                "strategy_hash": strategy_hash,
                "config_hash": config_hash,
                "candidate_id": candidate,
                "strategy_version_id": strategy_version_id,
                "research_trial_id": research_trial_id,
                "source_strategy_hash": origin.get("source_strategy_hash"),
                "rolling_strategy_hash": strategy_hash,
                "validation_version": _ROLLING_ENROLLMENT_VALIDATION_VERSION,
                "provenance": origin,
                "enrollment_id": enrollment_identity(
                    candidate=candidate,
                    status="ACCEPTED",
                    reason="MATURE_NONCAMPAIGN_RESEARCH",
                    strategy_version_id=strategy_version_id,
                    research_trial_id=research_trial_id,
                ),
            }

        connection = getattr(self.store, "connection", None)
        execute = getattr(connection, "execute", None)
        rows: list[Mapping[str, Any]] = []
        legacy_query_completed = False
        if callable(execute):
            scan_remaining = _MAX_ROLLING_DISCOVERY_SCAN
            cursor: tuple[Any, ...] | None = None
            mature_stages = tuple(sorted(_ROLLING_MATURE_STAGES))
            while scan_remaining > 0:
                placeholders = ",".join("?" for _ in mature_stages)
                where = f" WHERE UPPER(COALESCE(c.stage,'')) IN ({placeholders})"
                params: list[Any] = list(mature_stages)
                if cursor is not None:
                    where += (
                        " AND (c.updated_at > ? OR "
                        "(c.updated_at = ? AND c.candidate_id > ?) OR "
                        "(c.updated_at = ? AND c.candidate_id = ? AND s.strategy_id > ?) OR "
                        "(c.updated_at = ? AND c.candidate_id = ? AND s.strategy_id = ? AND s.version > ?))"
                    )
                    params.extend(
                        [
                            cursor[0],
                            cursor[0],
                            cursor[1],
                            cursor[0],
                            cursor[1],
                            cursor[2],
                            cursor[0],
                            cursor[1],
                            cursor[2],
                            cursor[3],
                        ]
                    )
                params.append(min(_ROLLING_DISCOVERY_PAGE, scan_remaining))
                try:
                    queried = execute(
                        "SELECT s.strategy_id,s.version,s.payload_json AS strategy_payload,"
                        "c.candidate_id,c.stage,c.payload_json AS candidate_payload,c.updated_at "
                        "FROM strategies AS s JOIN candidate_lifecycle AS c "
                        "ON c.candidate_id=s.strategy_id OR "
                        "c.candidate_id=CASE WHEN json_valid(s.payload_json) "
                        "THEN json_extract(s.payload_json,'$.candidate_id') ELSE NULL END "
                        f"{where} "
                        "ORDER BY c.updated_at,c.candidate_id,s.strategy_id,s.version LIMIT ?",
                        tuple(params),
                    ).fetchall()
                except Exception:
                    rows = []
                    break
                legacy_query_completed = True
                batch = [dict(row) for row in queried or ()]
                if not batch:
                    break
                rows.extend(batch)
                scan_remaining -= len(batch)
                last = batch[-1]
                next_cursor = (
                    last.get("updated_at"),
                    str(last.get("candidate_id", "")),
                    str(last.get("strategy_id", "")),
                    str(last.get("version", "")),
                )
                if cursor is not None and next_cursor <= cursor:
                    break
                cursor = next_cursor
                if len(batch) < _ROLLING_DISCOVERY_PAGE:
                    break
        if not rows and not legacy_query_completed:
            strategy_lister = getattr(self.store, "list_strategies", None)
            lifecycle_lister = getattr(self.store, "load_candidate_lifecycle", None)
            if callable(strategy_lister) and callable(lifecycle_lister):
                def bounded_legacy_rows(loader: Callable[..., Any]) -> tuple[Any, ...]:
                    """Call only adapters that expose an explicit bounded limit."""
                    try:
                        values = loader(limit=_MAX_ROLLING_DISCOVERY_SCAN)
                    except TypeError as exc:
                        raise ValueError(
                            "ROLLING_DISCOVERY_BOUNDED_SOURCE_UNAVAILABLE"
                        ) from exc
                    if isinstance(values, Mapping):
                        values = values.get("records", values.get("rows", (values,)))
                    if values is None or isinstance(values, (str, bytes)):
                        return ()
                    try:
                        return tuple(islice(values, _MAX_ROLLING_DISCOVERY_SCAN))
                    except TypeError:
                        return ()

                try:
                    strategy_rows = bounded_legacy_rows(strategy_lister)
                    lifecycle_rows = bounded_legacy_rows(lifecycle_lister)
                except ValueError as exc:
                    if str(exc) == "ROLLING_DISCOVERY_BOUNDED_SOURCE_UNAVAILABLE":
                        raise
                    strategy_rows, lifecycle_rows = (), ()
                except (TypeError, RuntimeError):
                    strategy_rows, lifecycle_rows = (), ()
                lifecycle_by_id = {
                    str(item.get("candidate_id")).strip(): item
                    for item in lifecycle_rows
                    if (
                        isinstance(item, Mapping)
                        and str(item.get("candidate_id", "")).strip()
                        and str(item.get("stage", "")).strip().upper() in _ROLLING_MATURE_STAGES
                    )
                }
                for item in strategy_rows:
                    if len(rows) >= _MAX_ROLLING_DISCOVERY_SCAN:
                        break
                    if not isinstance(item, Mapping):
                        continue
                    strategy_payload = item.get("strategy", item.get("payload", {}))
                    strategy_id = str(item.get("strategy_id", "")).strip()
                    payload_candidates, _ = _rolling_candidate_values(decode(strategy_payload))
                    candidate_keys = [strategy_id, str(item.get("candidate_id", "")).strip()]
                    candidate_keys.extend(sorted(payload_candidates))
                    lifecycle = next(
                        (lifecycle_by_id.get(key) for key in candidate_keys if key and lifecycle_by_id.get(key)),
                        None,
                    )
                    if lifecycle is None:
                        continue
                    rows.append(
                        {
                            "strategy_id": strategy_id,
                            "version": item.get("version"),
                            "strategy_payload": strategy_payload,
                            "candidate_id": lifecycle.get("candidate_id"),
                            "stage": lifecycle.get("stage"),
                            "candidate_payload": lifecycle.get("payload"),
                        }
                    )
        for row in rows[:_MAX_ROLLING_DISCOVERY_SCAN]:
            item = validate_join(row, row)
            if item is not None:
                found[item["strategy_hash"] + ":" + str(item["candidate_id"])] = item

        # Existing application-owned rolling rows survive a restart even when
        # the legacy source has been compacted or removed.
        if callable(execute):
            try:
                modern_rows = execute(
                    "SELECT payload_json,strategy_version_id FROM strategy_versions "
                    "ORDER BY created_at,strategy_version_id LIMIT ?",
                    (_MAX_ROLLING_STRATEGIES,),
                ).fetchall()
            except Exception:
                modern_rows = ()
            for row in modern_rows or ():
                payload = decode(row["payload_json"])
                provenance = payload.get("provenance")
                provenance = dict(provenance) if isinstance(provenance, Mapping) else {}
                if not _rolling_document_is_marked(payload, provenance):
                    continue
                source = payload.get("strategy_document", payload.get("canonical_strategy"))
                if not isinstance(source, Mapping) or _rolling_campaign_bound(payload):
                    continue
                canonical_candidates, canonical_truncated = _rolling_candidate_values(provenance)
                payload_candidates, payload_truncated = _rolling_candidate_values(payload)
                if (
                    canonical_truncated
                    or payload_truncated
                    or (
                        payload_candidates
                        and canonical_candidates
                        and payload_candidates != canonical_candidates
                    )
                ):
                    continue
                candidates = canonical_candidates or payload_candidates
                if len(candidates) != 1:
                    continue
                try:
                    definition = load_strategy(source)
                except (TypeError, ValueError, RuntimeError):
                    continue
                item = dict(payload)
                item["strategy_document"] = definition.to_dict()
                item["strategy_version_id"] = row["strategy_version_id"]
                item["provenance"] = _rolling_provenance_bound(provenance)
                item["candidate_id"] = sorted(candidates)[0]
                item.setdefault("research_trial_id", provenance.get("research_trial_id"))
                item.setdefault("strategy_hash", _rolling_hash(item["strategy_document"]))
                item["_strategy_version_existing"] = True
                if item.get("research_trial_id"):
                    found.setdefault(
                        str(item["strategy_hash"]) + ":" + str(item["candidate_id"]),
                        item,
                    )
        ordered = sorted(
            found.values(),
            key=lambda item: (
                str(item.get("strategy_hash", "")),
                str(item.get("candidate_id", "")),
            ),
        )
        return tuple(ordered[:_MAX_ROLLING_STRATEGIES])

    def _rolling_persist_strategy_lineage(
        self,
        documents: Sequence[Mapping[str, Any]],
        now: datetime,
    ) -> tuple[dict[str, Any], ...]:
        saver = getattr(self.store, "save_strategy_version", None)
        trial_saver = getattr(self.store, "save_research_trial", None)
        enrollment_saver = getattr(self.store, "save_rolling_enrollment", None)
        enrollment_lister = getattr(self.store, "list_rolling_enrollments", None)

        def predecessor_for(candidate: str) -> tuple[str | None, Mapping[str, Any] | None]:
            if not callable(enrollment_lister):
                return None, None
            try:
                records = enrollment_lister(candidate_id=candidate, limit=256)
            except (TypeError, ValueError, RuntimeError):
                return None, None
            if not isinstance(records, (list, tuple)):
                return None, None
            for prior in records:
                if not isinstance(prior, Mapping):
                    continue
                prior_id = _binding_value(prior.get("enrollment_id"))
                prior_version = _binding_value(prior.get("validation_version"))
                if (
                    prior_id
                    and str(prior.get("status", "")).strip().upper() == "EXCLUDED"
                    and prior_version != _ROLLING_ENROLLMENT_VALIDATION_VERSION
                ):
                    return prior_id, prior
            return None, None
        persisted: list[dict[str, Any]] = []
        for item in documents:
            document = dict(item["strategy_document"])
            strategy_hash = str(item["strategy_hash"])
            candidate_id = str(item.get("candidate_id") or "").strip() or None
            strategy_version_id = str(item.get("strategy_version_id") or "").strip()
            if not strategy_version_id:
                identity = {"strategy_hash": strategy_hash, "candidate_id": candidate_id}
                strategy_version_id = "strategy-version-" + _rolling_hash(identity).removeprefix("sha256:")[:40]
            provenance = _rolling_provenance_bound(
                {
                    **dict(item.get("provenance") or {}),
                    "rolling_research": True,
                    "research_mode": "ROLLING_RESEARCH",
                    "candidate_id": candidate_id,
                    "source_candidate_id": candidate_id,
                }
            )
            enrollment_mode = str(provenance.get("mode", "RESEARCH")).strip().upper()
            if enrollment_mode not in _ROLLING_ENROLLMENT_MODES:
                enrollment_mode = "RESEARCH"
            record = {
                "strategy_version_id": strategy_version_id,
                "strategy_id": str(item.get("strategy_id", "")).strip() or str(document.get("family", "strategy")),
                "version": str(item.get("version", "1")),
                "code_hash": strategy_hash,
                "strategy_hash": strategy_hash,
                "config_hash": str(item.get("config_hash", strategy_hash)),
                "created_at": now.isoformat(),
                "strategy_document": document,
                "canonical_strategy": document,
                "enrollment_mode": enrollment_mode,
                "provenance": provenance,
                "candidate_id": candidate_id,
                "execution_scope": "OBSERVATION",
                "research_only": True,
                "paper_only": True,
            }
            trial_id = str(item.get("research_trial_id") or provenance.get("research_trial_id") or "").strip()
            if not trial_id:
                trial_identity = {
                    "strategy_version_id": strategy_version_id,
                    "candidate_id": candidate_id,
                    "source_trial_id": provenance.get("source_trial_id"),
                    "source_config_hash": provenance.get("source_config_hash"),
                }
                trial_id = "research-trial-" + _rolling_hash(trial_identity).removeprefix("sha256:")[:40]
            trial = {
                "research_trial_id": trial_id,
                "trial_id": trial_id,
                "strategy_version_id": strategy_version_id,
                "candidate_id": candidate_id,
                "status": "SCHEDULED",
                "created_at": now.isoformat(),
                "trial_kind": "ROLLING_RESEARCH",
                "strategy_hash": strategy_hash,
                "enrollment_mode": enrollment_mode,
                "requested_window_days": [7, 30],
                "rolling_research": True,
                "execution_scope": "OBSERVATION",
                "research_only": True,
                "paper_only": True,
                "payload": {"strategy_document": document, "provenance": provenance},
            }
            def immutable_match(existing: Any, expected: Mapping[str, Any]) -> bool:
                if not isinstance(existing, Mapping):
                    return False
                return _canonical_binding(
                    _rolling_payload_without_created_at(existing)
                ) == _canonical_binding(
                    _rolling_payload_without_created_at(expected)
                )

            try:
                if callable(saver) and not item.get("_strategy_version_existing"):
                    try:
                        saver(record)
                    except ValueError as exc:
                        loader = getattr(self.store, "load_strategy_version", None)
                        existing = loader(strategy_version_id) if callable(loader) else None
                        if not immutable_match(existing, record):
                            raise exc
                if callable(trial_saver):
                    try:
                        trial_saver(trial)
                    except ValueError as exc:
                        loader = getattr(self.store, "load_research_trial", None)
                        existing = loader(trial_id) if callable(loader) else None
                        if not immutable_match(existing, trial):
                            raise exc
            except (TypeError, ValueError, RuntimeError):
                # Persistence failures are retryable.  Do not create a
                # terminal EXCLUDED enrollment while lineage may be partial:
                # the next pass can reconcile exact immutable identities and
                # insert ACCEPTED under the same deterministic enrollment id.
                continue
            if callable(enrollment_saver) and candidate_id:
                predecessor_id, predecessor = predecessor_for(candidate_id)
                accepted_provenance = dict(provenance)
                accepted_provenance["validation_version"] = _ROLLING_ENROLLMENT_VALIDATION_VERSION
                if predecessor_id:
                    accepted_provenance["predecessor_enrollment_id"] = predecessor_id
                    prior_provenance = predecessor.get("provenance") if isinstance(predecessor, Mapping) else None
                    if isinstance(prior_provenance, Mapping):
                        accepted_provenance["predecessor_provenance"] = _rolling_provenance_bound(prior_provenance)
                try:
                    enrollment_saver(
                        {
                            "enrollment_id": item.get("enrollment_id"),
                            "candidate_id": candidate_id,
                            "strategy_version_id": strategy_version_id,
                            "research_trial_id": trial_id,
                            "status": "ACCEPTED",
                            "reason": "MATURE_NONCAMPAIGN_RESEARCH",
                            "validation_version": _ROLLING_ENROLLMENT_VALIDATION_VERSION,
                            "predecessor_enrollment_id": predecessor_id,
                            "provenance": _rolling_provenance_bound(accepted_provenance),
                        }
                    )
                except (TypeError, ValueError, RuntimeError):
                    # The strategy and trial are still immutable and linked;
                    # a retry can repair only the enrollment projection.
                    pass
            persisted.append({**record, "research_trial_id": trial_id, "candidate_id": candidate_id})
        return tuple(persisted)

    def _load_rolling_historical_dataset(
        self,
        dataset_id: str,
        dataset_version: str,
    ) -> Any | None:
        """Preflight immutable JSON size/row metadata before ``load_dataset``.

        The regular storage loader intentionally serves all dataset consumers.
        Rolling evidence has a stricter source boundary, so inspect SQLite
        metadata first and refuse oversized payloads before storage decodes the
        JSON string or reconstructs catalog-backed records.
        """
        connection = getattr(self.store, "connection", None)
        execute = getattr(connection, "execute", None)
        if not callable(execute):
            raise ValueError("HISTORICAL_DATASET_PREFLIGHT_UNAVAILABLE")
        try:
            payload_row = execute(
                "SELECT LENGTH(CAST(payload_json AS BLOB)) AS payload_bytes,"
                "CASE WHEN json_valid(payload_json) THEN json_type(payload_json) END AS payload_type,"
                "json_extract(CASE WHEN json_valid(metadata_json) THEN metadata_json ELSE '{}' END,"
                "'$.row_count') AS metadata_row_count,"
                "json_array_length(CASE WHEN json_valid(payload_json) THEN payload_json ELSE '[]' END) "
                "AS payload_row_count,"
                "CASE WHEN json_valid(payload_json) "
                "AND json_type(payload_json,'$.records')='array' "
                "THEN json_array_length(json_extract(payload_json,'$.records')) END "
                "AS records_row_count,"
                "CASE WHEN json_valid(payload_json) "
                "AND json_type(payload_json,'$.rows')='array' "
                "THEN json_array_length(json_extract(payload_json,'$.rows')) END "
                "AS rows_row_count,"
                "json_array_length(CASE WHEN json_valid(metadata_json) "
                "AND json_type(metadata_json,'$.records')='array' "
                "THEN json_extract(metadata_json,'$.records') ELSE '[]' END) "
                "AS metadata_records_row_count,"
                "json_array_length(CASE WHEN json_valid(metadata_json) "
                "AND json_type(metadata_json,'$.rows')='array' "
                "THEN json_extract(metadata_json,'$.rows') ELSE '[]' END) "
                "AS metadata_rows_row_count "
                "FROM datasets WHERE dataset_id=? AND version=? LIMIT 1",
                (str(dataset_id), str(dataset_version)),
            ).fetchone()
            catalog_row = execute(
                "SELECT row_count,"
                "json_array_length(CASE WHEN json_valid(metadata_json) "
                "AND json_type(metadata_json,'$.market_versions')='array' "
                "THEN json_extract(metadata_json,'$.market_versions') ELSE '[]' END) "
                "AS market_versions_count "
                "FROM dataset_catalog "
                "WHERE dataset_id=? AND dataset_version=? LIMIT 1",
                (str(dataset_id), str(dataset_version)),
            ).fetchone()
        except Exception as exc:
            raise ValueError("HISTORICAL_DATASET_PREFLIGHT_FAILED") from exc

        def integer(value: Any, field: str) -> int | None:
            if value is None:
                return None
            if isinstance(value, bool):
                raise ValueError(f"{field} is invalid")
            try:
                parsed = int(value)
            except (TypeError, ValueError, OverflowError) as exc:
                raise ValueError(f"{field} is invalid") from exc
            if parsed < 0:
                raise ValueError(f"{field} is invalid")
            return parsed

        if payload_row is None and catalog_row is None:
            raise ValueError("HISTORICAL_DATASET_METADATA_MISSING")
        payload_rows: int | None = None
        metadata_rows: int | None = None
        if payload_row is not None:
            payload_bytes = integer(payload_row["payload_bytes"], "payload_bytes")
            payload_type = str(payload_row["payload_type"] or "").strip().lower()
            metadata_rows = integer(
                payload_row["metadata_row_count"],
                "metadata_row_count",
            )
            root_rows = integer(
                payload_row["payload_row_count"],
                "payload_row_count",
            )
            records_rows = integer(
                payload_row["records_row_count"],
                "records_row_count",
            )
            rows_rows = integer(
                payload_row["rows_row_count"],
                "rows_row_count",
            )
            metadata_records_rows = integer(
                payload_row["metadata_records_row_count"],
                "metadata_records_row_count",
            )
            metadata_rows_rows = integer(
                payload_row["metadata_rows_row_count"],
                "metadata_rows_row_count",
            )
            if payload_bytes is None or payload_bytes > _MAX_ROLLING_DATASET_PAYLOAD_BYTES:
                raise ValueError("HISTORICAL_DATASET_PAYLOAD_TOO_LARGE")
            for row_count in (
                metadata_rows,
                root_rows,
                records_rows,
                rows_rows,
                metadata_records_rows,
                metadata_rows_rows,
            ):
                if row_count is not None and row_count > _MAX_ROLLING_SOURCE_ROWS:
                    raise ValueError("HISTORICAL_DATASET_ROW_COUNT_TOO_LARGE")
            if payload_type == "array":
                payload_rows = root_rows
            elif payload_type == "object":
                shaped = [
                    value
                    for value in (records_rows, rows_rows)
                    if value is not None
                ]
                if records_rows is not None and rows_rows is not None and records_rows != rows_rows:
                    raise ValueError("HISTORICAL_DATASET_ROW_COUNT_MISMATCH")
                payload_rows = shaped[0] if shaped else None
            if (
                payload_rows is None
                and metadata_rows is None
                and catalog_row is None
            ):
                raise ValueError("HISTORICAL_DATASET_ROW_COUNT_UNAVAILABLE")
            if (
                payload_rows is not None
                and metadata_rows is not None
                and payload_rows != metadata_rows
            ):
                raise ValueError("HISTORICAL_DATASET_ROW_COUNT_MISMATCH")

        if catalog_row is not None:
            catalog_rows = integer(catalog_row["row_count"], "catalog_row_count")
            market_versions = integer(
                catalog_row["market_versions_count"],
                "market_versions_count",
            )
            if catalog_rows is None:
                raise ValueError("HISTORICAL_DATASET_ROW_COUNT_UNAVAILABLE")
            if (
                catalog_rows > _MAX_ROLLING_SOURCE_ROWS
                or market_versions is not None
                and market_versions > _MAX_ROLLING_SOURCE_ROWS
            ):
                raise ValueError("HISTORICAL_DATASET_ROW_COUNT_TOO_LARGE")
            if payload_rows is not None and catalog_rows != payload_rows:
                raise ValueError("HISTORICAL_DATASET_ROW_COUNT_MISMATCH")
        return self.store.load_dataset(dataset_id, dataset_version)

    def _rolling_accounting_projection(
        self,
        row: Mapping[str, Any],
        strategy: Mapping[str, Any],
    ) -> Mapping[str, Any] | None:
        """Return validated explicit strategy accounting, never infer it from prices."""
        view = _rolling_snapshot_view(row)
        quality = str(
            view.get("research_quality", view.get("quality", view.get("source_type", "")))
        ).strip().upper().replace("-", "_")

        def reject(reason: str, metric: str | None = None) -> None:
            row["_rolling_accounting_rejection"] = reason
            if metric:
                row["_rolling_accounting_metric"] = metric

        if quality in {"PRICE_PROXY", "PRICE_PROXY_RESEARCH", "PRICE_REPLAY"}:
            reject("PRICE_PROXY_ACCOUNTING_UNAVAILABLE")
            return None

        accounting: Mapping[str, Any] | None = None
        for name in ("canonical_accounting", "strategy_accounting", "accounting"):
            value = view.get(name)
            if isinstance(value, Mapping):
                accounting = value
                break

        # ForwardPaperEngine persists one canonical resolved-bet payload rather
        # than a second accounting wrapper.  It is accepted only after the
        # PAPER loader has proved the immutable experiment/spec binding.
        paper_experiment = _binding_value(row.get("_paper_experiment_id"))
        if accounting is None and paper_experiment:
            resolved = view.get("resolved_bet")
            if not isinstance(resolved, Mapping) and {
                "net_pnl",
                "capital_at_risk",
                "resolution",
            }.issubset(view):
                resolved = view
            if isinstance(resolved, Mapping):
                resolved_experiment = _binding_value(
                    resolved.get("experiment_id", view.get("experiment_id"))
                )
                resolution = str(resolved.get("resolution", "")).strip().lower()
                if (
                    resolved_experiment != paper_experiment
                    or resolved.get("closed") is not True
                    or resolution
                    not in {"resolved_yes", "resolved_no", "void"}
                ):
                    reject("PAPER_RESOLVED_BET_BINDING_INVALID")
                    return None

                def decimal_value(value: Any) -> Decimal | None:
                    if value is None or isinstance(value, bool):
                        return None
                    try:
                        parsed = Decimal(str(value))
                    except (InvalidOperation, TypeError, ValueError, OverflowError):
                        return None
                    return parsed if parsed.is_finite() else None

                def first_timestamp(*names: str) -> datetime | None:
                    for name in names:
                        timestamp = _rolling_timestamp(resolved.get(name))
                        if timestamp is None:
                            timestamp = _rolling_timestamp(view.get(name))
                        if timestamp is not None:
                            return timestamp
                    return None

                shortage = resolved.get("accounting_shortage")
                if shortage:
                    shortage_fields = (
                        shortage.get("fields")
                        if isinstance(shortage, Mapping)
                        else ()
                    )
                    reject(
                        "PAPER_RESOLVED_BET_ACCOUNTING_INCOMPLETE",
                        str(next(iter(shortage_fields), "accounting")),
                    )
                    return None
                net_pnl = decimal_value(resolved.get("net_pnl"))
                capital = decimal_value(resolved.get("capital_at_risk"))
                fees_value = decimal_value(resolved.get("fees"))
                slippage_value = decimal_value(resolved.get("slippage"))
                if net_pnl is None:
                    reject("ACCOUNTING_METRIC_MISSING", "realized_pnl")
                    return None
                if capital is None:
                    reject("ACCOUNTING_METRIC_MISSING", "allocated_capital")
                    return None
                if fees_value is None:
                    reject("ACCOUNTING_METRIC_MISSING", "fees")
                    return None
                if slippage_value is None:
                    reject("ACCOUNTING_METRIC_MISSING", "costs")
                    return None
                if capital <= Decimal("0"):
                    reject("ACCOUNTING_METRIC_NONPOSITIVE", "allocated_capital")
                    return None
                drawdown = min(
                    Decimal("1"),
                    max(Decimal("0"), -net_pnl / capital),
                )
                if "drawdown" in resolved:
                    supplied_drawdown = decimal_value(resolved.get("drawdown"))
                    if supplied_drawdown is None:
                        reject("ACCOUNTING_METRIC_INVALID", "drawdown")
                        return None
                    if supplied_drawdown != drawdown:
                        reject("PAPER_RESOLVED_BET_DRAWDOWN_CONFLICT", "drawdown")
                        return None
                if "unrealized_pnl" in resolved:
                    supplied_unrealized = decimal_value(resolved.get("unrealized_pnl"))
                    if supplied_unrealized is None:
                        reject("ACCOUNTING_METRIC_INVALID", "unrealized_pnl")
                        return None
                    if supplied_unrealized != Decimal("0"):
                        reject(
                            "PAPER_RESOLVED_BET_UNREALIZED_NONZERO",
                            "unrealized_pnl",
                        )
                        return None
                for name, expected in (
                    ("allocated_capital", capital),
                    ("allocated_capital_net_return", net_pnl),
                    ("realized_pnl", net_pnl),
                    ("fees", fees_value),
                    ("costs", slippage_value),
                ):
                    if name not in resolved:
                        continue
                    supplied = decimal_value(resolved.get(name))
                    if supplied is None:
                        reject("ACCOUNTING_METRIC_INVALID", name)
                        return None
                    if supplied != expected:
                        reject("PAPER_RESOLVED_BET_ACCOUNTING_CONFLICT", name)
                        return None
                for name in ("completed_outcomes", "reliability"):
                    if name not in resolved:
                        continue
                    supplied = decimal_value(resolved.get(name))
                    if supplied is None:
                        reject("ACCOUNTING_METRIC_INVALID", name)
                        return None
                    if supplied != Decimal("1"):
                        reject("PAPER_RESOLVED_BET_CONTRACT_CONFLICT", name)
                        return None
                expected_outcome = (
                    "yes"
                    if resolution == "resolved_yes"
                    else "no"
                    if resolution == "resolved_no"
                    else None
                )
                supplied_winner = _binding_value(resolved.get("winning_outcome"))
                if (
                    expected_outcome is not None
                    and supplied_winner is not None
                    and supplied_winner.lower() != expected_outcome
                ):
                    reject("PAPER_RESOLVED_BET_CONTRACT_CONFLICT", "winning_outcome")
                    return None
                available_from = first_timestamp(
                    "observation_open_timestamp",
                    "observation_opened_at",
                    "open_timestamp",
                    "opened_at",
                    "observation_timestamp",
                    "observation_at",
                    "available_from",
                    "coverage_from",
                    "window_start",
                    "timestamp",
                )
                available_through = first_timestamp(
                    "resolved_at",
                    "available_through",
                    "coverage_through",
                    "window_end",
                )
                if available_from is None:
                    reject("ACCOUNTING_COVERAGE_MISSING", "observation_open_timestamp")
                    return None
                if available_through is None:
                    reject("ACCOUNTING_COVERAGE_MISSING", "resolved_at")
                    return None
                if available_from > available_through:
                    reject("ACCOUNTING_COVERAGE_INVALID", "observation_open_timestamp")
                    return None
                accounting = dict(resolved)
                accounting.update(
                    {
                        "allocated_capital": capital,
                        "allocated_capital_net_return": net_pnl,
                        "realized_pnl": net_pnl,
                        "unrealized_pnl": Decimal("0"),
                        "fees": fees_value,
                        "costs": slippage_value,
                        "completed_outcomes": Decimal("1"),
                        "reliability": Decimal("1"),
                        "drawdown": drawdown,
                        "available_from": available_from.isoformat(),
                        "available_through": available_through.isoformat(),
                    }
                )

        if accounting is None:
            reject("ACCOUNTING_MISSING")
            return None

        try:
            binding = _rolling_source_binding(view)
        except (TypeError, ValueError):
            reject("SOURCE_BINDING_CONFLICT")
            return None
        expected_binding = _rolling_source_binding(strategy)
        expected_candidate = _binding_value(
            strategy.get("candidate_id")
            or expected_binding.get("candidate_id")
            or expected_binding.get("source_candidate_id")
        )
        expected_trial = _binding_value(
            strategy.get("research_trial_id")
            or expected_binding.get("research_trial_id")
            or expected_binding.get("trial_id")
        )
        expected_strategy_version = _binding_value(
            strategy.get("strategy_version_id")
            or expected_binding.get("strategy_version_id")
        )
        expected_strategy_hash = _binding_value(
            strategy.get("strategy_hash")
            or expected_binding.get("strategy_hash")
        )
        row_candidate = _binding_value(binding.get("candidate_id"))
        row_trial = _binding_value(binding.get("research_trial_id", binding.get("trial_id")))
        row_strategy_version = _binding_value(binding.get("strategy_version_id"))
        row_strategy_hash = _binding_value(binding.get("strategy_hash"))
        if (
            not expected_candidate
            or not expected_trial
            or not expected_strategy_version
            or not expected_strategy_hash
            or row_candidate != expected_candidate
            or row_trial != expected_trial
            or row_strategy_version != expected_strategy_version
            or row_strategy_hash != expected_strategy_hash
        ):
            reject("SOURCE_BINDING_MISMATCH")
            return None
        if paper_experiment:
            if _binding_value(binding.get("experiment_id")) != paper_experiment:
                reject("PAPER_EXPERIMENT_BINDING_MISMATCH")
                return None
            row_strategy = _binding_value(view.get("strategy_id"))
            if row_strategy and row_strategy != expected_strategy_hash:
                reject("PAPER_STRATEGY_BINDING_MISMATCH")
                return None

        def value_for(*names: str) -> tuple[str | None, Any]:
            for name in names:
                if name in accounting:
                    return name, accounting[name]
            return None, None

        def parse_metric(name: str, raw: Any, *, nonnegative: bool = False, ratio: bool = False) -> Any:
            if raw is None or isinstance(raw, bool) or isinstance(raw, (Mapping, list, tuple, set, frozenset)):
                reject("ACCOUNTING_METRIC_TYPE_INVALID", name)
                return None
            try:
                parsed = Decimal(str(raw))
            except (InvalidOperation, TypeError, ValueError, OverflowError):
                reject("ACCOUNTING_METRIC_MALFORMED", name)
                return None
            if not parsed.is_finite():
                reject("ACCOUNTING_METRIC_NONFINITE", name)
                return None
            if nonnegative and parsed < 0:
                reject("ACCOUNTING_METRIC_NEGATIVE_IMPOSSIBLE", name)
                return None
            if ratio and (parsed < 0 or parsed > 1):
                reject("ACCOUNTING_METRIC_OUT_OF_RANGE", name)
                return None
            if name == "completed_outcomes" and parsed != parsed.to_integral_value():
                reject("ACCOUNTING_METRIC_MALFORMED", name)
                return None
            return parsed

        metric_aliases: dict[str, tuple[str, ...]] = {
            "allocated_capital": ("allocated_capital", "allocated_capital_usd", "paper_size", "paper_sizing"),
            "net_return": ("allocated_capital_net_return", "net_return"),
            "realized_pnl": ("realized_pnl",),
            "unrealized_pnl": ("unrealized_pnl",),
            "fees": ("fees", "fee_costs"),
            "costs": ("costs", "slippage_costs"),
            "drawdown": ("drawdown",),
            "completed_outcomes": ("completed_outcomes", "outcomes"),
            "reliability": ("reliability",),
        }
        parsed_metrics: dict[str, Decimal] = {}
        for metric_name, aliases in metric_aliases.items():
            alias, raw = value_for(*aliases)
            if alias is None:
                reject("ACCOUNTING_METRIC_MISSING", metric_name)
                return None
            parsed = parse_metric(
                metric_name,
                raw,
                nonnegative=metric_name
                in {"allocated_capital", "fees", "costs", "drawdown", "completed_outcomes", "reliability"},
                ratio=metric_name in {"drawdown", "reliability"},
            )
            if parsed is None:
                return None
            parsed_metrics[metric_name] = parsed
        if parsed_metrics["completed_outcomes"] < 0:
            reject("ACCOUNTING_METRIC_NEGATIVE_IMPOSSIBLE", "completed_outcomes")
            return None
        result = dict(accounting)
        for metric_name, parsed in parsed_metrics.items():
            result[metric_name] = parsed
        result["_timestamp"] = _rolling_row_time(view)
        result["_available_from"] = _rolling_timestamp(
            view.get("available_from", view.get("coverage_from", view.get("window_start")))
            or accounting.get("available_from", accounting.get("coverage_from"))
        )
        result["_available_through"] = _rolling_timestamp(
            view.get("available_through", view.get("coverage_through", view.get("window_end")))
            or accounting.get("available_through", accounting.get("coverage_through"))
        )
        if result["_available_from"] is None or result["_available_through"] is None:
            reject("ACCOUNTING_COVERAGE_MISSING")
            return None
        if result["_available_through"] < result["_available_from"]:
            reject("ACCOUNTING_COVERAGE_INVALID")
            return None
        return result

    def _rolling_source_rows(
        self,
        record: Mapping[str, Any],
        source_class: str,
        now: datetime,
    ) -> list[dict[str, Any]]:
        source = _rolling_source_name(source_class)
        if source not in _ROLLING_SOURCE_TYPES:
            raise ValueError(f"unsupported rolling source class: {source_class}")
        persisted_source = _rolling_persisted_source_class(source)
        try:
            source_binding = _rolling_source_job_binding(record, source)
        except ValueError as exc:
            if str(exc) == "HISTORICAL_DATASET_BINDING_REQUIRED":
                raise ValueError("HISTORICAL_SELECTOR_REQUIRED") from exc
            raise
        binding = _rolling_source_binding(record)
        dataset_id = str(source_binding.get("dataset_id", "")).strip()
        dataset_version = str(source_binding.get("dataset_version", "")).strip()
        strategy_hash = _binding_value(source_binding.get("strategy_hash"))
        expected_candidate = _binding_value(source_binding.get("candidate_id"))
        expected_trial = _binding_value(source_binding.get("research_trial_id"))
        expected_strategy_version = _binding_value(
            source_binding.get("strategy_version_id")
        )
        rows: list[dict[str, Any]] = []
        if source == "HISTORICAL":
            # Historical evidence is valid only for an explicit immutable
            # selector.  Never substitute the newest or first catalog entry.
            loaded = self._load_rolling_historical_dataset(dataset_id, dataset_version)
            if isinstance(loaded, Mapping):
                loaded = loaded.get("records", loaded.get("rows", ()))
            if isinstance(loaded, Sequence) and not isinstance(loaded, (str, bytes)):
                rows = [
                    _rolling_inject_source_binding(item, source_binding)
                    for item in loaded
                    if isinstance(item, Mapping)
                ]
        elif source in {"REPLAY", "LIVE"}:
            loader = getattr(self.store, "load_polymarket_snapshots", None)
            if callable(loader):
                persisted_types = _ROLLING_SOURCE_QUERY_TYPES[source]
                for source_value in persisted_types:
                    # The storage boundary accepts only persisted source types;
                    # the requested rolling class remains canonical below.
                    # The storage boundary accepts only persisted source
                    # types; the requested rolling class remains canonical.
                    loader_binding_proven = True
                    try:
                        values = loader(
                            source_type=source_value,
                            dataset_id=dataset_id or None,
                            dataset_version=dataset_version or None,
                            strategy_version_id=source_binding["strategy_version_id"],
                            research_trial_id=source_binding["research_trial_id"],
                            candidate_id=source_binding["candidate_id"],
                            limit=_MAX_FORWARD_ROWS,
                        )
                    except TypeError:
                        loader_binding_proven = False
                        values = loader(source_type=source_value, limit=_MAX_FORWARD_ROWS)
                    if isinstance(values, Mapping):
                        values = values.get("records", values.get("rows", ()))
                    for item_index, item in enumerate(values or ()):
                        if item_index >= _MAX_FORWARD_ROWS:
                            break
                        if not isinstance(item, Mapping):
                            continue
                        if not loader_binding_proven and source in {"REPLAY", "LIVE"}:
                            try:
                                row_binding = _rolling_source_binding(item)
                            except (TypeError, ValueError):
                                row_binding = {}
                                rejection = "SOURCE_BINDING_CONFLICT"
                            else:
                                required_fields = (
                                    "strategy_hash",
                                    "strategy_version_id",
                                    "research_trial_id",
                                    "candidate_id",
                                )
                                missing = any(
                                    _binding_value(row_binding.get(field)) is None
                                    for field in required_fields
                                )
                                conflict = any(
                                    (
                                        _binding_value(row_binding.get(field)) is not None
                                        and _binding_value(row_binding.get(field))
                                        != _binding_value(source_binding.get(field))
                                    )
                                    for field in required_fields
                                )
                                rejection = (
                                    "SOURCE_BINDING_CONFLICT"
                                    if conflict
                                    else "SOURCE_BINDING_UNPROVEN"
                                    if missing
                                    else None
                                )
                            if rejection is not None:
                                row = dict(item)
                                row["_rolling_accounting_rejection"] = rejection
                                rows.append(row)
                                continue
                            if source == "REPLAY":
                                row_dataset = _binding_value(
                                    row_binding.get("dataset_id")
                                )
                                row_version = _binding_value(
                                    row_binding.get(
                                        "dataset_version",
                                        row_binding.get("version"),
                                    )
                                )
                                if row_dataset != dataset_id or row_version != dataset_version:
                                    row = dict(item)
                                    row["_rolling_accounting_rejection"] = (
                                        "SOURCE_BINDING_CONFLICT"
                                    )
                                    rows.append(row)
                                    continue
                        rows.append(_rolling_inject_source_binding(item, source_binding))
        elif source == "PAPER":
            registry_loader = getattr(self.store, "load_forward_tests", None)
            if not callable(registry_loader):
                raise ValueError("PAPER_REGISTRY_UNAVAILABLE")
            try:
                try:
                    spec_values = registry_loader(limit=_MAX_ROLLING_SOURCE_ROWS)
                    if isinstance(spec_values, Mapping):
                        spec_values = spec_values.get("records", spec_values.get("rows", ()))
                    specs: list[Any] = []
                    for spec in spec_values or ():
                        if len(specs) >= _MAX_ROLLING_SOURCE_ROWS:
                            break
                        specs.append(spec)
                except TypeError:
                    # An adapter without an explicit bounded parameter cannot
                    # safely be used for rolling discovery.
                    specs = []
            except Exception as exc:
                raise ValueError("PAPER_REGISTRY_UNAVAILABLE") from exc
            matched_spec = False
            for spec in specs:
                if not isinstance(spec, Mapping):
                    continue
                spec_strategy_hash = _binding_value(spec.get("strategy_hash"))
                experiment_id = _binding_value(spec.get("experiment_id"))
                config = spec.get("config")
                config = config if isinstance(config, Mapping) else {}
                source_strategy_hash = _binding_value(
                    config.get("source_strategy_hash")
                ) or spec_strategy_hash
                rolling_strategy_hash = _binding_value(
                    config.get("rolling_strategy_hash")
                ) or spec_strategy_hash
                if (
                    source_strategy_hash != spec_strategy_hash
                    or rolling_strategy_hash != strategy_hash
                ):
                    continue
                try:
                    spec_binding = _rolling_source_binding(
                        {"payload": config, "strategy_hash": spec_strategy_hash}
                    )
                except (TypeError, ValueError):
                    continue
                spec_candidate = _binding_value(spec_binding.get("candidate_id"))
                spec_trial = _binding_value(
                    spec_binding.get("research_trial_id", spec_binding.get("trial_id"))
                )
                spec_strategy_version = _binding_value(
                    spec_binding.get("strategy_version_id")
                )
                if (
                    not experiment_id
                    or spec_candidate != expected_candidate
                    or spec_trial != expected_trial
                    or spec_strategy_version != expected_strategy_version
                ):
                    continue
                matched_spec = True
                loader = getattr(self.store, "list_paper_bet_ledger", None)
                if not callable(loader):
                    continue
                try:
                    values = loader(experiment_id, limit=_MAX_ROLLING_SOURCE_ROWS)
                except Exception as exc:
                    raise ValueError("PAPER_SOURCE_UNAVAILABLE") from exc
                row_binding = {
                    **source_binding,
                    "experiment_id": experiment_id,
                }
                for item_index, item in enumerate(values or ()):
                    if item_index >= _MAX_ROLLING_SOURCE_ROWS:
                        break
                    if not isinstance(item, Mapping):
                        continue
                    raw_view = _rolling_snapshot_view(item)
                    raw_strategy_values = (
                        item.get("strategy_hash"),
                        item.get("strategy_id"),
                        raw_view.get("strategy_hash"),
                        raw_view.get("strategy_id"),
                    )
                    allowed_hashes = {strategy_hash, spec_strategy_hash}
                    if any(
                        _binding_value(value) is not None
                        and _binding_value(value) not in allowed_hashes
                        for value in raw_strategy_values
                    ):
                        rejected = dict(item)
                        rejected["_rolling_accounting_rejection"] = "PAPER_STRATEGY_BINDING_CONFLICT"
                        rows.append(rejected)
                        continue
                    raw_source_values = (
                        item.get("source_strategy_hash"),
                        raw_view.get("source_strategy_hash"),
                    )
                    if any(
                        _binding_value(value) is not None
                        and _binding_value(value) != spec_strategy_hash
                        for value in raw_source_values
                    ):
                        rejected = dict(item)
                        rejected["_rolling_accounting_rejection"] = "PAPER_SOURCE_STRATEGY_BINDING_CONFLICT"
                        rows.append(rejected)
                        continue
                    translated_item = dict(item)
                    translated_item["strategy_hash"] = strategy_hash
                    translated_item["source_strategy_hash"] = spec_strategy_hash
                    translated_item["rolling_strategy_hash"] = strategy_hash
                    raw_payload = translated_item.get("payload")
                    if isinstance(raw_payload, Mapping):
                        translated_payload = dict(raw_payload)
                        translated_payload["strategy_hash"] = strategy_hash
                        translated_payload["source_strategy_hash"] = spec_strategy_hash
                        translated_payload["rolling_strategy_hash"] = strategy_hash
                        translated_item["payload"] = translated_payload
                    row = _rolling_inject_source_binding(translated_item, row_binding)
                    if row.get("_rolling_accounting_rejection"):
                        rows.append(row)
                        continue
                    row_view = _rolling_snapshot_view(row)
                    if (
                        _binding_value(item.get("experiment_id")) is not None
                        and _binding_value(item.get("experiment_id")) != experiment_id
                    ):
                        row["_rolling_accounting_rejection"] = "PAPER_EXPERIMENT_BINDING_CONFLICT"
                        rows.append(row)
                        continue
                    row_strategy = _binding_value(
                        row_view.get("strategy_hash", row_view.get("strategy_id"))
                    )
                    if row_strategy and row_strategy != strategy_hash:
                        row["_rolling_accounting_rejection"] = "PAPER_STRATEGY_BINDING_CONFLICT"
                        rows.append(row)
                        continue
                    row["_paper_experiment_id"] = experiment_id
                    rows.append(row)
            if not matched_spec:
                raise ValueError("PAPER_SPEC_BINDING_MISMATCH")
        resolved_scope_market_ids = _rolling_rule_scope_market_ids(
            self.store,
            record,
            binding,
            rows,
            now,
        )
        normalized: list[dict[str, Any]] = []
        allowed_source_types = _ROLLING_SOURCE_TYPES[source]
        for raw in rows:
            if not isinstance(raw, Mapping):
                continue
            row = _rolling_snapshot_view(raw)
            declared_source = str(
                row.get("source_type", row.get("source_class", ""))
            ).strip().upper().replace("-", "_")
            if source == "PAPER" and declared_source == "FORWARD_COLLECTED":
                # ForwardPaperEngine persists its observation rows with the
                # collection source type; this loader's canonical class is
                # still PAPER and remains experiment-bound above.
                declared_source = "PAPER"
            if declared_source and declared_source not in allowed_source_types:
                continue
            if _rolling_campaign_bound(row):
                continue
            if row.get("_rolling_accounting_rejection"):
                row["source_class"] = persisted_source
                row["source_type"] = persisted_source
                row["requested_source_class"] = source
                row["requested_source_type"] = source
                normalized.append(row)
                continue
            accounting = self._rolling_accounting_projection(row, record)
            if accounting is None:
                if row.get("_rolling_accounting_rejection"):
                    row["source_class"] = persisted_source
                    row["source_type"] = persisted_source
                    row["requested_source_class"] = source
                    row["requested_source_type"] = source
                    normalized.append(row)
                continue
            row["_rolling_accounting"] = dict(accounting)
            binding_conflict = False
            for field_name in ("dataset_id", "dataset_version", "version"):
                expected = source_binding.get(field_name)
                if expected is None or expected == "":
                    continue
                actual = row.get(field_name)
                if actual is not None and actual != "" and _canonical_binding(actual) != _canonical_binding(expected):
                    binding_conflict = True
                    break
            if binding_conflict:
                row["_rolling_accounting_rejection"] = "SOURCE_BINDING_CONFLICT"
                row["source_class"] = persisted_source
                row["source_type"] = persisted_source
                row["requested_source_class"] = source
                row["requested_source_type"] = source
                normalized.append(row)
                continue
            scope = binding.get("market_scope", binding.get("scope"))
            scope = scope if isinstance(scope, Mapping) else {}
            if resolved_scope_market_ids is not None:
                expected_markets = resolved_scope_market_ids
            else:
                expected_markets = scope.get(
                    "market_ids",
                    scope.get(
                        "markets",
                        binding.get("market_ids", binding.get("market_id", ())),
                    ),
                )
                if isinstance(expected_markets, str):
                    expected_markets = (expected_markets,)
                expected_markets = {
                    str(item).strip()
                    for item in (expected_markets or ())
                    if str(item).strip()
                }
            if resolved_scope_market_ids is not None and not expected_markets:
                continue
            if expected_markets:
                actual_market = str(row.get("market_id", "")).strip()
                if actual_market not in expected_markets:
                    continue
            row["source_class"] = persisted_source
            row["source_type"] = persisted_source
            row["requested_source_class"] = source
            row["requested_source_type"] = source
            if _rolling_row_time(row) is not None:
                normalized.append(row)
        normalized.sort(
            key=lambda row: (
                _rolling_row_time(row) or datetime.min.replace(tzinfo=timezone.utc),
                str(row.get("market_id", "")),
                str(row.get("snapshot_id", row.get("observation_id", ""))),
            )
        )
        return normalized[-_MAX_ROLLING_SOURCE_ROWS:]

    def _rolling_evidence_record(
        self,
        strategy: Mapping[str, Any],
        rows: Sequence[Mapping[str, Any]],
        source_class: str,
        days: int,
        now: datetime,
    ) -> dict[str, Any] | None:
        """Aggregate explicit strategy accounting; prices are never PnL input."""
        if days not in {7, 30} or not rows:
            return None
        requested_source = _rolling_source_name(source_class)
        persisted_source = _rolling_persisted_source_class(requested_source)
        projected: list[tuple[Mapping[str, Any], Mapping[str, Any]]] = []
        for row in rows:
            if not isinstance(row, Mapping):
                continue
            accounting = row.get("_rolling_accounting")
            if not isinstance(accounting, Mapping):
                accounting = self._rolling_accounting_projection(row, strategy)
            if isinstance(accounting, Mapping):
                projected.append((row, accounting))
        if not projected:
            return None
        stream_through = max(
            accounting["_available_through"]
            for _row, accounting in projected
            if isinstance(accounting.get("_available_through"), datetime)
        )
        requested_start = stream_through - timedelta(days=days)
        selected = [
            (row, accounting)
            for row, accounting in projected
            if accounting["_available_through"] >= requested_start
            and accounting["_available_from"] <= stream_through
        ]
        if not selected:
            return None
        intervals: list[tuple[datetime, datetime]] = []
        for _row, accounting in selected:
            start = max(requested_start, accounting["_available_from"])
            end = min(stream_through, accounting["_available_through"])
            if end >= start:
                intervals.append((start, end))
        if not intervals:
            return None
        intervals.sort()
        merged: list[tuple[datetime, datetime]] = []
        for start, end in intervals:
            if not merged or start > merged[-1][1]:
                merged.append((start, end))
            elif end > merged[-1][1]:
                merged[-1] = (merged[-1][0], end)
        actual_coverage = sum(int((end - start).total_seconds()) for start, end in merged)
        available_from = min(start for start, _end in intervals)
        available_through = max(end for _start, end in intervals)

        def metric(accounting: Mapping[str, Any], *names: str) -> Decimal:
            for name in names:
                if accounting.get(name) is not None:
                    return Decimal(str(accounting[name]))
            raise ValueError("validated accounting metric is missing")

        net_return = sum(
            (
                metric(accounting, "allocated_capital_net_return", "net_return")
                for _row, accounting in selected
            ),
            Decimal("0"),
        )
        realized_pnl = sum((metric(accounting, "realized_pnl") for _row, accounting in selected), Decimal("0"))
        unrealized_pnl = sum((metric(accounting, "unrealized_pnl") for _row, accounting in selected), Decimal("0"))
        fees = sum((metric(accounting, "fees", "fee_costs") for _row, accounting in selected), Decimal("0"))
        costs = sum((metric(accounting, "costs", "slippage_costs") for _row, accounting in selected), Decimal("0"))
        if min(fees, costs) < Decimal("0"):
            return None
        if not any(
            accounting.get("allocated_capital_net_return") is not None
            or accounting.get("net_return") is not None
            for _row, accounting in selected
        ):
            net_return = realized_pnl + unrealized_pnl - fees - costs
        completed_values = [
            metric(accounting, "completed_outcomes")
            for _row, accounting in selected
            if accounting.get("completed_outcomes") is not None
        ]
        completed = sum(int(value) for value in completed_values if value == value.to_integral_value())
        if any(value != value.to_integral_value() or value < 0 for value in completed_values):
            return None
        reliability_values = [
            metric(accounting, "reliability")
            for _row, accounting in selected
            if accounting.get("reliability") is not None
        ]
        if any(value < Decimal("0") or value > Decimal("1") for value in reliability_values):
            return None
        reliability = (
            sum(reliability_values, Decimal("0")) / Decimal(len(reliability_values))
            if reliability_values
            else Decimal("0")
        )
        drawdowns = [
            metric(accounting, "drawdown")
            for _row, accounting in selected
            if accounting.get("drawdown") is not None
        ]
        if any(value < Decimal("0") or value > Decimal("1") for value in drawdowns):
            return None
        drawdown = max(drawdowns, default=Decimal("0"))
        allocated_capital = sum(
            (metric(accounting, "allocated_capital", "paper_sizing", "paper_size") for _row, accounting in selected),
            Decimal("0"),
        )
        feasibility: Any = ""
        for _row, accounting in selected:
            if accounting.get("execution_feasibility") is not None:
                feasibility = accounting["execution_feasibility"]
                if isinstance(feasibility, bool):
                    feasibility = str(feasibility)
                if str(feasibility).strip().upper() in {"FALSE", "NO", "INFEASIBLE"} or feasibility is False:
                    feasibility = "False"
                    break
        provenance = strategy.get("provenance") if isinstance(strategy.get("provenance"), Mapping) else {}
        candidate_id = str(strategy.get("candidate_id") or provenance.get("candidate_id") or "").strip() or None
        trial_id = str(strategy.get("research_trial_id") or provenance.get("research_trial_id") or "").strip() or None
        source_binding = _rolling_source_binding(strategy)
        accounting_digest = [
            {
                "identity": str(
                    row.get("accounting_id", row.get("ledger_id", row.get("observation_id", "")))
                ),
                "from": accounting["_available_from"],
                "through": accounting["_available_through"],
                "metrics": {
                    key: accounting.get(key)
                    for key in (
                        "allocated_capital_net_return",
                        "net_return",
                        "realized_pnl",
                        "unrealized_pnl",
                        "fees",
                        "costs",
                        "completed_outcomes",
                        "reliability",
                        "drawdown",
                    )
                    if accounting.get(key) is not None
                },
            }
            for row, accounting in selected
        ]
        digest = _rolling_hash(
            {
                "strategy_version_id": strategy["strategy_version_id"],
                "research_trial_id": trial_id,
                "candidate_id": candidate_id,
                "source_class": persisted_source,
                "requested_source_class": requested_source,
                "source_binding": source_binding,
                "requested_days": days,
                "available_from": available_from,
                "available_through": available_through,
                "actual_coverage_seconds": actual_coverage,
                "accounting": accounting_digest,
            }
        )
        window_id = "rolling-window-" + digest.removeprefix("sha256:")[:40]
        requested_seconds = Decimal(days) * Decimal(86400)
        completeness = (
            min(Decimal("1"), Decimal(actual_coverage) / requested_seconds)
            if requested_seconds > 0
            else Decimal("0")
        )
        record = {
            "strategy_version_id": strategy["strategy_version_id"],
            "candidate_id": candidate_id,
            "research_trial_id": trial_id,
            "evidence_window_id": window_id,
            "available_from": available_from.isoformat(),
            "available_through": available_through.isoformat(),
            "requested_days": days,
            "actual_coverage_seconds": actual_coverage,
            "observation_completeness": str(completeness),
            "source_class": persisted_source,
            "paper_sizing_assumptions": {
                "currency": "USD",
                "allocated_capital": str(allocated_capital),
                "sizing_model": "canonical_accounting",
            },
            "paper_fee_assumptions": {"fee_rate": "0", "fee_bps": "0"},
            "paper_slippage_assumptions": {"slippage_rate": "0", "slippage_bps": "0"},
            "allocated_capital_net_return": str(net_return),
            "realized_pnl": str(realized_pnl),
            "unrealized_pnl": str(unrealized_pnl),
            "fees": str(fees),
            "costs": str(costs),
            "drawdown": str(drawdown),
            "completed_outcomes": completed,
            "reliability": str(reliability),
            "execution_feasibility": feasibility,
            "evidence_digest": "",
            "overlap_key": _rolling_overlap_key([row for row, _accounting in selected]),
            "market_path_count": len(
                {
                    str(row.get("market_id")).strip()
                    for row, _accounting in selected
                    if str(row.get("market_id", "")).strip()
                }
            ),
            "market_path_keys": sorted(
                {
                    str(row.get("market_id")).strip()
                    for row, _accounting in selected
                    if str(row.get("market_id", "")).strip()
                }
            ),
            "paper_only": True,
            "rolling_research": True,
            "measured_at": available_through.isoformat(),
        }
        record["evidence_digest"] = RollingEvidence.from_mapping(record).evidence_digest
        return record

    def refresh_rolling_evidence(self, now: datetime | None = None) -> Mapping[str, Any]:
        """Materialize bounded, source-separated rolling evidence and schedule retries."""
        current = ensure_utc(now or self.clock())
        documents = self._rolling_strategy_documents()
        strategies = self._rolling_persist_strategy_lineage(documents, current)
        sources = ("HISTORICAL", "REPLAY", "PAPER", "LIVE")
        queue_payload = _rolling_hermes_payload(strategies, sources, current)
        queue_identity = dict(queue_payload)
        queue_identity.pop("available_at", None)
        queue_item = self.bus.submit_review_request(
            queue_payload,
            dedupe_key="rolling-research:" + _rolling_hash(queue_identity),
            available_at=current,
        )
        evidence_rows: list[dict[str, Any]] = []
        pending: list[dict[str, Any]] = []
        saver = getattr(self.store, "save_strategy_evidence_window", None)
        source_cache: dict[str, list[dict[str, Any]]] = {}
        source_cache_errors: dict[str, str] = {}
        total_rows = 0

        def add_pending(item: Mapping[str, Any]) -> None:
            if len(pending) < _MAX_ROLLING_QUEUE_RESULTS:
                entry = dict(item)
                entry.setdefault("next_job", "rolling-research-evidence")
                pending.append(entry)

        for strategy in strategies:
            source_payload = dict(strategy)
            source_payload["payload"] = {
                **dict(strategy.get("provenance") or {}),
                "strategy_document": strategy.get("strategy_document"),
            }
            for source in sources:
                canonical_source = _rolling_source_name(source)
                try:
                    binding = _rolling_source_binding(source_payload)
                    cache_key = _rolling_hash(
                        {
                            "source_class": canonical_source,
                            "dataset_selector": binding,
                            "strategy_hash": (
                                str(strategy.get("strategy_hash", ""))
                                if canonical_source == "PAPER"
                                else None
                            ),
                        }
                    )
                except (TypeError, ValueError) as exc:
                    add_pending(
                        {
                            "strategy_version_id": strategy["strategy_version_id"],
                            "research_trial_id": strategy["research_trial_id"],
                            "candidate_id": strategy.get("candidate_id"),
                            "source_class": canonical_source,
                            "requested_days": [7, 30],
                            "status": "ERROR",
                            "reason": "SOURCE_SELECTOR_INVALID",
                            "error_type": type(exc).__name__,
                            "error": str(exc)[:256],
                        }
                    )
                    continue
                rows: list[dict[str, Any]]
                if cache_key in source_cache_errors:
                    add_pending(
                        {
                            "strategy_version_id": strategy["strategy_version_id"],
                            "research_trial_id": strategy["research_trial_id"],
                            "candidate_id": strategy.get("candidate_id"),
                            "source_class": canonical_source,
                            "requested_days": [7, 30],
                            "status": "ERROR",
                            "reason": "SOURCE_LOAD_FAILED",
                            "error": source_cache_errors[cache_key],
                        }
                    )
                    continue
                if cache_key not in source_cache:
                    try:
                        source_cache[cache_key] = self._rolling_source_rows(
                            source_payload, canonical_source, current
                        )
                    except (TypeError, ValueError) as exc:
                        source_cache_errors[cache_key] = (
                            f"{type(exc).__name__}: {str(exc)[:256]}"
                        )
                        add_pending(
                            {
                                "strategy_version_id": strategy["strategy_version_id"],
                                "research_trial_id": strategy["research_trial_id"],
                                "candidate_id": strategy.get("candidate_id"),
                                "source_class": canonical_source,
                                "requested_days": [7, 30],
                                "status": "ERROR",
                                "reason": "SOURCE_LOAD_FAILED",
                                "error": source_cache_errors[cache_key],
                            }
                        )
                        continue
                rows = source_cache[cache_key]
                remaining = max(0, _MAX_ROLLING_TOTAL_ROWS - total_rows)
                bounded_rows = list(rows[:remaining])
                accounting_rejections = sorted(
                    {
                        str(row.get("_rolling_accounting_rejection"))
                        for row in bounded_rows
                        if row.get("_rolling_accounting_rejection")
                    }
                )
                for rejection in accounting_rejections:
                    add_pending(
                        {
                            "strategy_version_id": strategy["strategy_version_id"],
                            "research_trial_id": strategy["research_trial_id"],
                            "candidate_id": strategy.get("candidate_id"),
                            "source_class": canonical_source,
                            "requested_days": [7, 30],
                            "status": "ERROR",
                            "reason": rejection,
                        }
                    )
                total_rows += len(bounded_rows)
                for days in (7, 30):
                    evidence = self._rolling_evidence_record(
                        strategy, bounded_rows, canonical_source, days, current
                    )
                    if evidence is None:
                        add_pending(
                            {
                                "strategy_version_id": strategy["strategy_version_id"],
                                "research_trial_id": strategy["research_trial_id"],
                                "candidate_id": strategy.get("candidate_id"),
                                "source_class": canonical_source,
                                "requested_days": days,
                                "status": "SCHEDULED",
                                "reason": (
                                    "HISTORICAL_DATASET_EMPTY"
                                    if canonical_source == "HISTORICAL"
                                    else "INSUFFICIENT_EVIDENCE"
                                ),
                            }
                        )
                        continue
                    if not callable(saver):
                        add_pending(
                            {
                                "strategy_version_id": strategy["strategy_version_id"],
                                "research_trial_id": strategy["research_trial_id"],
                                "candidate_id": strategy.get("candidate_id"),
                                "source_class": canonical_source,
                                "requested_days": days,
                                "status": "ERROR",
                                "reason": "EVIDENCE_PERSISTENCE_UNAVAILABLE",
                            }
                        )
                        continue
                    try:
                        saver(evidence)
                    except (TypeError, ValueError) as exc:
                        add_pending(
                            {
                                "strategy_version_id": strategy["strategy_version_id"],
                                "research_trial_id": strategy["research_trial_id"],
                                "candidate_id": strategy.get("candidate_id"),
                                "source_class": canonical_source,
                                "requested_days": days,
                                "status": "ERROR",
                                "reason": "EVIDENCE_PERSISTENCE_FAILED",
                                "error_type": type(exc).__name__,
                                "error": str(exc)[:256],
                            }
                        )
                        continue
                    evidence_rows.append(evidence)
                if total_rows >= _MAX_ROLLING_TOTAL_ROWS:
                    break
            if total_rows >= _MAX_ROLLING_TOTAL_ROWS:
                break
        if not strategies:
            proposal_items: tuple[Any, ...] = ()
            proposal_error: str | None = None
            try:
                proposal_items = tuple(self._enqueue_predeclared_from_persisted_scope(current) or ())
            except (AutonomousResearchError, ResearchBusPermissionError, TypeError, ValueError, RuntimeError) as exc:
                proposal_error = f"{type(exc).__name__}: {str(exc)[:256]}"
            fallback = {
                "strategy_version_id": None,
                "source_class": "ALL",
                "requested_days": [7, 30],
                "status": "SCHEDULED" if proposal_error is None else "ERROR",
                "reason": "NO_STRATEGY_DEFINITIONS" if proposal_error is None else "PROPOSAL_PATH_FAILED",
                "next_job": "predeclared_starting_set",
                "proposal_queue_item_ids": [
                    str(item.item_id)
                    for item in proposal_items
                    if getattr(item, "item_id", None)
                ],
            }
            if proposal_error is not None:
                fallback["error"] = proposal_error
            add_pending(fallback)
        state = _rolling_state_payload(
            status="SCHEDULED" if pending else "READY",
            scheduled_at=current.isoformat(),
            queue_item_id=queue_item.item_id,
            queue_status=queue_item.status.value,
            strategies=strategies,
            evidence_rows=evidence_rows,
            pending=pending,
            source_classes=sources,
            requested_window_days=(7, 30),
            source_rows_total=total_rows,
        )
        setter = getattr(self.store, "set_operator_job", None)
        if callable(setter):
            setter("rolling-research-evidence", "SCHEDULED" if pending else "COMPLETED", state, resumable=True)
        return state

    def _rolling_risk_binding(self, policy: RollingAdmissionPolicy) -> dict[str, Any]:
        active_loader = getattr(self.store, "load_canary_setting_config", None)
        if callable(active_loader):
            try:
                active = active_loader(state="ACTIVE")
            except TypeError:
                try:
                    active = active_loader()
                except Exception:
                    active = None
            except Exception:
                active = None
            if isinstance(active, Mapping):
                values = active.get("values", active.get("settings", {}))
                values = values if isinstance(values, Mapping) else {}
                identifier = str(active.get("config_id", "")).strip()
                generation = int(active.get("generation", 0) or 0)
                digest = str(active.get("config_hash", "")).strip()
                if identifier and generation > 0 and digest:
                    budget = values.get("global_budget", values.get("max_notional", values.get("budget", policy.global_budget)))
                    return {
                        "risk_config_id": identifier,
                        "risk_config_generation": generation,
                        "risk_config_hash": digest,
                        "global_budget": str(budget),
                    }
        for name in ("load_active_risk_config", "active_risk_config", "get_active_risk_config"):
            loader = getattr(self.store, name, None)
            if callable(loader):
                try:
                    value = loader()
                except Exception:
                    value = None
                if isinstance(value, Mapping):
                    identifier = str(value.get("risk_config_id", value.get("config_id", ""))).strip()
                    if identifier:
                        generation = int(value.get("risk_config_generation", value.get("generation", 0)) or 0)
                        digest = str(value.get("risk_config_hash", value.get("config_hash", ""))).strip() or _rolling_hash(value)
                        budget = value.get("global_budget", value.get("budget", policy.global_budget))
                        return {
                            "risk_config_id": identifier,
                            "risk_config_generation": generation,
                            "risk_config_hash": digest,
                            "global_budget": str(budget),
                        }
        return {
            "risk_config_id": "rolling-risk-default",
            "risk_config_generation": 0,
            "risk_config_hash": _rolling_hash({"risk_config_id": "rolling-risk-default", "global_budget": str(policy.global_budget)}),
            "global_budget": str(policy.global_budget),
        }

    def _rolling_policy(self) -> RollingAdmissionPolicy:
        default = default_rolling_admission_policy()
        active_payload = None
        config_loader = getattr(self.store, "get_operator_config", None)
        if callable(config_loader):
            try:
                configured = config_loader("rolling_admission_policy_active", None)
            except TypeError:
                configured = config_loader("rolling_admission_policy_active")
            except Exception:
                configured = None
            if isinstance(configured, Mapping) and any(
                key in configured for key in ("policy_id", "version", "policy_version", "config_hash")
            ):
                active_payload = configured
        if active_payload is None:
            job_loader = getattr(self.store, "get_operator_job", None)
            active_record = job_loader("rolling_admission_policy_active") if callable(job_loader) else None
            active_payload = active_record.get("payload") if isinstance(active_record, Mapping) else None
            if (
                not isinstance(active_payload, Mapping)
                and isinstance(active_record, Mapping)
                and any(key in active_record for key in ("policy_id", "version", "policy_version", "config_hash"))
            ):
                active_payload = active_record
        if isinstance(active_payload, Mapping) and isinstance(active_payload.get("policy"), Mapping):
            active_payload = active_payload["policy"]
        if active_payload is not None:
            if not isinstance(active_payload, Mapping):
                raise ValueError("active rolling admission policy payload is invalid")
            policy_id = str(active_payload.get("policy_id", "")).strip()
            version = str(active_payload.get("version", active_payload.get("policy_version", ""))).strip()
            expected_hash = str(active_payload.get("config_hash", "")).strip()
            if not policy_id or not version or not expected_hash:
                raise ValueError("active rolling admission policy identity is incomplete")
            loader = getattr(self.store, "load_admission_policy", None)
            if not callable(loader):
                raise ValueError("active rolling admission policy loader is unavailable")
            stored = loader(policy_id, version)
            if not isinstance(stored, Mapping):
                raise ValueError("active rolling admission policy is missing")
            policy = RollingAdmissionPolicy.from_mapping(stored)
            if policy.policy_id != policy_id or policy.version != version or str(policy.config_hash) != expected_hash:
                raise ValueError("active rolling admission policy identity mismatch")
            return policy
        loader = getattr(self.store, "load_admission_policy", None)
        if callable(loader):
            stored = loader(default.policy_id, default.version)
            if isinstance(stored, Mapping):
                return RollingAdmissionPolicy.from_mapping(stored)
        saver = getattr(self.store, "save_admission_policy", None)
        if callable(saver):
            saver(default.as_dict())
        return default

    def review_rolling_portfolio(self, now: datetime | None = None, force: bool = False) -> Mapping[str, Any]:
        """Review only persisted rolling evidence and commit an append-only selection."""
        requested_now = ensure_utc(now or self.clock())
        refreshed = self.refresh_rolling_evidence(requested_now)
        policy = self._rolling_policy()
        previous = self.active_portfolio_selection()
        review_state_loader = getattr(self.store, "load_portfolio_review_state", None)
        prior_state = review_state_loader() if callable(review_state_loader) else None
        effective_now = requested_now
        if isinstance(previous, Mapping):
            previous_selected_at = _rolling_timestamp(previous.get("selected_at"))
            if previous_selected_at is not None and previous_selected_at > effective_now:
                effective_now = previous_selected_at
        current = effective_now
        current_identity = (
            refreshed.get("refresh_identity")
            if isinstance(refreshed.get("refresh_identity"), Mapping)
            else {}
        )
        prior_identity = (
            prior_state.get("refresh_identity")
            if isinstance(prior_state, Mapping)
            and isinstance(prior_state.get("refresh_identity"), Mapping)
            else {}
        )
        prior_initialization = _rolling_prior_initialization_review(prior_state, previous)
        try:
            current_strategy_total = int(current_identity.get("strategy_versions_total", -1))
        except (TypeError, ValueError):
            current_strategy_total = -1
        prior_digest = str(
            prior_identity.get("digest")
            if prior_identity
            else (prior_state.get("rolling_review_identity") if isinstance(prior_state, Mapping) else "")
        ).strip()
        current_digest = str(current_identity.get("digest", "")).strip()
        refresh_changed = bool(prior_digest and current_digest and prior_digest != current_digest)
        if not prior_identity and isinstance(prior_state, Mapping):
            prior_pending = prior_state.get("pending")
            current_pending = refreshed.get("pending")
            if isinstance(prior_pending, (list, tuple)) and isinstance(current_pending, (list, tuple)):
                refresh_changed = _rolling_hash(prior_pending) != _rolling_hash(current_pending)
        initialization_transition = prior_initialization and (
            current_strategy_total > 0 or refresh_changed
        )
        due = _rolling_timestamp(prior_state.get("review_due_at")) if isinstance(prior_state, Mapping) else None
        bypass_due = bool(force or initialization_transition)
        if not bypass_due and due is not None and current < due and previous is not None:
            return self.rolling_portfolio_state()
        lister = getattr(self.store, "list_strategy_evidence_windows", None)
        if callable(lister):
            try:
                evidence_rows = lister(limit=10_000)
            except TypeError:
                try:
                    evidence_rows = lister()
                except Exception:
                    evidence_rows = ()
            except Exception:
                evidence_rows = ()
        else:
            evidence_rows = ()
        evidence_rows = tuple(
            row
            for row in (evidence_rows or ())
            if isinstance(row, Mapping)
            and _rolling_document_is_marked(
                row,
                row.get("provenance") if isinstance(row.get("provenance"), Mapping) else None,
            )
            and not _rolling_campaign_bound(row)
        )
        risk = self._rolling_risk_binding(policy)
        # subtract every historical allocation on each rotation.
        active_obligations = Decimal("0")
        risk["active_obligations"] = str(active_obligations)
        available_budget = max(Decimal("0"), _rolling_number(risk.get("global_budget")))
        risk["available_budget"] = str(available_budget)
        effective_policy = replace(policy, global_budget=available_budget, config_hash=policy.config_hash)
        evaluation_previous = previous
        if bypass_due and isinstance(previous, Mapping):
            # The pure evaluator has its own cadence guard.  Adjust only the
            # in-memory input for this one material/explicit review, while
            # preserving RollingSelection's selected_at/review_due_at ordering.
            evaluation_previous = dict(previous)
            selected_at = _rolling_timestamp(evaluation_previous.get("selected_at"))
            evaluation_previous["review_due_at"] = max(
                current,
                selected_at or current,
            ).isoformat()
        decision = evaluate_rolling_selection(
            effective_policy,
            evidence_rows,
            evaluation_previous,
            current,
        )
        if initialization_transition:
            decision = _rolling_initialization_observe(decision, refreshed)
        prior_event_history = (
            list(prior_state.get("event_history", []))
            if isinstance(prior_state, Mapping) and isinstance(prior_state.get("event_history"), list)
            else []
        )
        event_history = (
            prior_event_history
            + [{"at": current.isoformat(), "status": decision.status, "reasons": list(decision.reasons)}]
        )[-128:]
        members = [
            member.as_dict()
            for member in decision.members
            if member.candidate_id and member.research_trial_id
        ]
        # Removed members are not funded selection rows, but their immutable
        # identities must remain in the committed payload until the runtime
        # observes and completes the corresponding exit obligation.
        removed_members: list[dict[str, Any]] = []
        removed_ids: set[str] = set()

        def preserve_removed(raw: Any) -> None:
            if len(removed_members) >= _MAX_ROLLING_EXIT_LINEAGE or not isinstance(raw, Mapping):
                return
            strategy_id = str(raw.get("strategy_version_id", "")).strip()
            if not strategy_id or strategy_id in removed_ids:
                return
            item = dict(raw)
            item["status"] = "PAUSED"
            item["action"] = "REDUCE"
            item["allocation"] = "0"
            removed_members.append(item)
            removed_ids.add(strategy_id)

        for member in decision.removed_members:
            preserve_removed(member.as_dict())
        if isinstance(previous, Mapping):
            prior_removed = previous.get("removed_members", ())
            if isinstance(prior_removed, (list, tuple)):
                for raw in prior_removed:
                    preserve_removed(raw)
            prior_members = previous.get("members", previous.get("selected_members", ()))
            if isinstance(prior_members, (list, tuple)):
                for raw in prior_members:
                    if isinstance(raw, Mapping) and (
                        str(raw.get("status", "")).strip().upper() in {"REMOVED", "PAUSED"}
                        or str(raw.get("action", "")).strip().upper() == "REDUCE"
                    ):
                        preserve_removed(raw)
        selection_id = "rolling-selection-" + _rolling_hash({
            "policy": policy.config_hash,
            "risk": risk,
            "at": current,
            "refresh_identity": current_identity.get("digest"),
            "members": members,
            "removed_members": removed_members,
            "status": decision.status,
        }).removeprefix("sha256:")[:40]
        selection = {
            "portfolio_selection_id": selection_id,
            "selection_id": selection_id,
            "policy_id": policy.policy_id,
            "policy_version": policy.version,
            "risk_config_id": risk["risk_config_id"],
            "active_risk_config_id": risk["risk_config_id"],
            "risk_config_generation": risk["risk_config_generation"],
            "active_risk_config_generation": risk["risk_config_generation"],
            "risk_config_hash": risk["risk_config_hash"],
            "active_risk_config_hash": risk["risk_config_hash"],
            "global_budget": risk["global_budget"],
            "selected_at": current.isoformat(),
            "review_due_at": decision.review_due_at.isoformat() if decision.review_due_at else current.isoformat(),
            "status": decision.status,
            "reasons": list(decision.reasons),
            "score_formula": decision.score_formula,
            "formula_version": decision.formula_version,
            "policy_config": decision.policy_config,
            "removed_members": removed_members,
            "removed_member_evidence_history": dict(decision.removed_member_evidence_history),
            "last_membership_change_at": (
                decision.last_membership_change_at.isoformat()
                if decision.last_membership_change_at is not None
                else None
            ),
            "paper_only": True,
        }
        members = [
            {
                **member,
                "portfolio_selection_id": selection_id,
                "admission_policy_id": policy.policy_id,
                "admission_policy_version": policy.version,
                "risk_config_id": risk["risk_config_id"],
                "risk_config_generation": risk["risk_config_generation"],
                "risk_config_hash": risk["risk_config_hash"],
            }
            for member in members
        ]
        committer = getattr(self.store, "commit_portfolio_selection", None)
        state_saver = getattr(self.store, "save_portfolio_review_state", None)
        review_payload = {
            "portfolio_selection_id": selection_id,
            "review_due_at": selection["review_due_at"],
            "reviewed_at": current.isoformat(),
            "status": decision.status,
            "reasons": list(decision.reasons),
            "event_history": event_history,
            "removed_member_evidence_history": dict(decision.removed_member_evidence_history),
            "last_membership_change_at": selection["last_membership_change_at"],
            "pending": refreshed.get("pending", []),
            "next_jobs": refreshed.get("next_jobs", []),
            "refresh_identity": current_identity,
            "rolling_review_identity": current_identity.get("digest"),
            "initialization_transition": initialization_transition,
            "updated_at": current.isoformat(),
        }
        def persist_selection_and_review() -> Mapping[str, Any] | None:
            committed_value: Mapping[str, Any] | None = None
            if callable(committer):
                committed_result = committer(selection, members)
                if isinstance(committed_result, Mapping):
                    committed_value = committed_result
            if callable(state_saver):
                state_saver(review_payload)
            return committed_value

        transaction_factory = getattr(self.store, "transaction", None)
        if callable(transaction_factory):
            try:
                transaction_context = transaction_factory(immediate=True)
            except TypeError:
                transaction_context = transaction_factory()
            with transaction_context:
                committed = persist_selection_and_review()
        else:
            committed = persist_selection_and_review()
        committed_selection = (
            dict(committed)
            if isinstance(committed, Mapping)
            else {**selection, "members": members}
        )
        committed_selection.setdefault(
            "last_membership_change_at",
            selection["last_membership_change_at"],
        )
        committed_selection.setdefault("removed_members", removed_members)
        state = {
            "status": decision.status,
            "controller_status": "SCHEDULED" if refreshed.get("pending") else "READY",
            "portfolio_selection_id": selection_id,
            "selection": committed_selection,
            "event_history": event_history,
            "removed_member_evidence_history": dict(decision.removed_member_evidence_history),
            "last_membership_change_at": selection["last_membership_change_at"],
            "k": len(members),
            "actual": len(members),
            "actionable": sum(
                1
                for member in members
                if str(member.get("status", "")).upper() in {"ACTIVE", "PAPER", "REDUCE"}
                and _rolling_number(member.get("allocation")) > 0
            ),
            "policy": policy.as_dict(),
            "risk_binding": risk,
            "reasons": list(decision.reasons),
            "pending": refreshed.get("pending", []),
            "cold_start_requirements": refreshed.get("pending", []),
            "next_jobs": refreshed.get("next_jobs", []),
            "refresh_identity": current_identity,
            "rolling_review_identity": current_identity.get("digest"),
            "initialization_transition": initialization_transition,
            "paper_only": True,
        }
        return state

    def rolling_portfolio_state(self) -> Mapping[str, Any]:
        current = self.active_portfolio_selection()
        review_loader = getattr(self.store, "load_portfolio_review_state", None)
        review = review_loader() if callable(review_loader) else None
        policy = self._rolling_policy()
        risk = self._rolling_risk_binding(policy)
        members = list(current.get("members", ())) if isinstance(current, Mapping) else []
        pending = review.get("pending", []) if isinstance(review, Mapping) else []
        return {
            "controller_status": str(review.get("status", "COLD_START") if isinstance(review, Mapping) else "COLD_START"),
            "portfolio_selection_id": current.get("portfolio_selection_id") if isinstance(current, Mapping) else None,
            "last_membership_change_at": (
                (
                    current.get("last_membership_change_at")
                    if isinstance(current, Mapping)
                    else None
                )
                or (
                    review.get("last_membership_change_at")
                    if isinstance(review, Mapping)
                    else None
                )
            ),
            "k": int(current.get("k", len(members))) if isinstance(current, Mapping) else 0,
            "actual": len(members),
            "actionable": sum(
                1
                for member in members
                if str(member.get("status", "")).upper() in {"ACTIVE", "PAPER", "REDUCE"}
                and _rolling_number(member.get("allocation")) > 0
            ),
            "policy": policy.as_dict(),
            "risk_binding": risk,
            "active_rows": members,
            "global_limits_usage": {
                "global_budget": risk["global_budget"],
                "allocated": str(sum((_rolling_number(member.get("allocation")) for member in members), Decimal("0"))),
                "active_obligations": risk.get("active_obligations", "0"),
                "available_budget": risk.get("available_budget", risk["global_budget"]),
            },
            "event_history": review.get("event_history", []) if isinstance(review, Mapping) else [],
            "removed_member_evidence_history": review.get("removed_member_evidence_history", {}) if isinstance(review, Mapping) else {},
            "next_jobs": (
                review.get("next_jobs")
                if isinstance(review, Mapping) and isinstance(review.get("next_jobs"), list)
                else [{"job_name": "rolling-research-evidence", "status": "SCHEDULED" if pending else "COMPLETED"}]
            ),
            "cold_start_requirements": pending,
            "refresh_identity": review.get("refresh_identity", {}) if isinstance(review, Mapping) else {},
            "rolling_review_identity": review.get("rolling_review_identity") if isinstance(review, Mapping) else None,
            "paper_only": True,
        }

    def active_portfolio_selection(self) -> Mapping[str, Any] | None:
        loader = getattr(self.store, "load_current_portfolio_selection", None)
        if not callable(loader):
            return None
        try:
            value = loader()
        except Exception:
            return None
        return dict(value) if isinstance(value, Mapping) else None

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
        if any(
            str(row.get("source_type", "HISTORICAL")).strip().upper()
            != "HISTORICAL"
            for row in rows
        ):
            raise AutonomousResearchError(
                "SOFTWARE_OR_INPUT_ERROR",
                "campaign dataset rows contain FORWARD_COLLECTED evidence",
            )
        rows.sort(
            key=lambda row: (
                parse_timestamp(row.get("timestamp", row.get("source_timestamp"))) or datetime.min.replace(tzinfo=timezone.utc),
                _campaign_row_identity(row, 0),
                _hash_document(row),
            )
        )
        return rows[:_MAX_DATASET_ROWS]

    def _campaign_prior_configuration_keys(
        self,
        protocol_id: str | None = None,
    ) -> set[str]:
        target_protocol = str(protocol_id or "").strip()
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
                if target_protocol == CAMPAIGN_PROTOCOL_V2_ID:
                    protocol = plan.get("campaign_protocol")
                    if (
                        isinstance(protocol, Mapping)
                        and str(protocol.get("schema_version") or "").strip()
                        == CAMPAIGN_SCHEMA_V1
                    ):
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
                if target_protocol == CAMPAIGN_PROTOCOL_V2_ID:
                    protocol = plan.get("campaign_protocol")
                    if (
                        isinstance(protocol, Mapping)
                        and str(protocol.get("schema_version") or "").strip()
                        == CAMPAIGN_SCHEMA_V1
                    ):
                        continue
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
        protocol_id: str = CAMPAIGN_PROTOCOL_V1_ID,
        attestation_hash: str | None = None,
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
        if str(attestation_hash or "").strip():
            boundary["attestation_hash"] = str(attestation_hash).strip()
        protocol_identity = str(protocol_id).strip()
        if protocol_identity not in {CAMPAIGN_PROTOCOL_V1_ID, CAMPAIGN_PROTOCOL_V2_ID}:
            raise ValueError("unsupported campaign protocol")
        schema_version = (
            CAMPAIGN_SCHEMA_V2
            if protocol_identity == CAMPAIGN_PROTOCOL_V2_ID
            else CAMPAIGN_SCHEMA_V1
        )
        required_features = (
            ["timestamp", "market_id", "yes_mid"]
            if protocol_identity == CAMPAIGN_PROTOCOL_V2_ID
            else ["timestamp", "market_id", "yes_mid", "yes_bid", "yes_ask", "settlement"]
        )
        return {
            "schema_version": schema_version,
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
            "required_features": required_features,
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
    def _campaign_state_defaults(
        payload: Mapping[str, Any],
        protocol: Mapping[str, Any],
        *,
        campaign_id: str,
        now: datetime,
    ) -> dict[str, Any]:
        """Fill fields introduced after the first durable campaign writer."""
        state = dict(payload)
        boundary = protocol.get("dataset_boundary")
        boundary = boundary if isinstance(boundary, Mapping) else {}
        raw_trials = state.get("trials")
        trials = [
            item
            for item in (raw_trials if isinstance(raw_trials, Sequence) and not isinstance(raw_trials, (str, bytes)) else ())
            if isinstance(item, Mapping)
        ]
        if "schema_version" not in state:
            state["schema_version"] = protocol.get(
                "schema_version",
                "polymarket-finite-campaign-v1",
            )
        if "campaign_id" not in state:
            state["campaign_id"] = campaign_id
        if "status" not in state:
            state["status"] = "PLANNED"
        if "reassessment_boundaries" not in state:
            state["reassessment_boundaries"] = {}
        if "base_proposal" not in state:
            state["base_proposal"] = {}
        if "dataset_id" not in state:
            state["dataset_id"] = boundary.get("dataset_id")
        if "dataset_version" not in state:
            state["dataset_version"] = boundary.get("dataset_version")
        if "last_evidence_identity" not in state:
            initial_identity = str(boundary.get("attestation_hash", "")).strip()
            state["last_evidence_identity"] = initial_identity or None
        if "budget_limit" not in state:
            state["budget_limit"] = protocol.get("budget_limit", CAMPAIGN_BUDGET_LIMIT)
        if "budget_used" not in state:
            state["budget_used"] = 0
        if "budget_remaining" not in state:
            try:
                budget_limit = int(state["budget_limit"])
            except (TypeError, ValueError, OverflowError):
                budget_limit = CAMPAIGN_BUDGET_LIMIT
                state["budget_limit"] = budget_limit
            try:
                budget_used = int(state["budget_used"])
            except (TypeError, ValueError, OverflowError):
                budget_used = 0
                state["budget_used"] = budget_used
            state["budget_remaining"] = max(0, budget_limit - budget_used)
        if "fixed_configuration_count" not in state:
            manifest = protocol.get("configuration_manifest")
            state["fixed_configuration_count"] = (
                len(manifest)
                if isinstance(manifest, Sequence) and not isinstance(manifest, (str, bytes))
                else len(trials)
            )
        if "trials" not in state or not isinstance(state.get("trials"), Sequence) or isinstance(
            state.get("trials"),
            (str, bytes),
        ):
            state["trials"] = []
            trials = []
        if "counts" not in state or not isinstance(state.get("counts"), Mapping):
            state["counts"] = {}
        counts = dict(state["counts"])
        status_counts = {
            "planned": sum(str(item.get("status", "")).upper() == "PLANNED" for item in trials),
            "running": sum(str(item.get("status", "")).upper() == "RUNNING" for item in trials),
            "economic_rejection": sum(
                str(item.get("status", "")).upper() == "ECONOMIC_REJECTION"
                for item in trials
            ),
            "data_insufficient": sum(
                str(item.get("status", "")).upper() == "DATA_INSUFFICIENT"
                for item in trials
            ),
            "software_or_input_error": sum(
                str(item.get("status", "")).upper() == "SOFTWARE_OR_INPUT_ERROR"
                for item in trials
            ),
            "validation_qualified": sum(
                str(item.get("status", "")).upper() == "VALIDATION_QUALIFIED"
                for item in trials
            ),
            "final_assessment": sum(
                str(item.get("status", "")).upper() == "FINAL_ASSESSMENT"
                for item in trials
            ),
            "qualified": 0,
        }
        for key, default in status_counts.items():
            counts.setdefault(key, default)
        state["counts"] = counts
        for key, default in (
            ("selection_excluded_evidence", []),
            ("qualified_candidate_ids", []),
            ("finalist_candidate_ids", []),
            ("final_assessment_evaluated", False),
            ("reassessment_count", 0),
            ("last_result", None),
            ("next_real_job", None),
            ("created_at", ensure_utc(now).isoformat()),
            ("last_updated_at", ensure_utc(now).isoformat()),
            ("paper_only", True),
            ("research_only", True),
        ):
            state.setdefault(key, default)
        return state

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
        if boundary.get("missing_ranges"):
            raise AutonomousResearchError(
                "CAMPAIGN_PROTOCOL_INVALID",
                "campaign trial boundary contains explicit missing dataset ranges",
            )
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
        plan_document.setdefault("time_split", "train-validation-holdout")
        plan_document.setdefault("metrics", ("expectancy", "drawdown", "trade_count", "sample_count"))
        plan_document.setdefault("min_samples", int(protocol.get("qualification_gates", {}).get("min_samples", 3)))
        plan_document.setdefault("min_trades", int(protocol.get("qualification_gates", {}).get("min_trades", 0)))
        plan_document["allowed_features"] = list(protocol.get("required_features", ()))
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
            boundary = self._campaign_trial_boundary(payload, protocol, next_trial)
            self._validate_campaign_dataset_provenance(
                str(boundary.get("dataset_id", "")).strip(),
                str(boundary.get("dataset_version", "")).strip(),
                boundary=boundary,
                plan=plan,
            )
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
        protocol_id: str | None = None,
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
        source = dict(proposal or {})
        inferred_protocol_id = (
            CAMPAIGN_PROTOCOL_V2_ID
            if campaign.startswith(f"{CAMPAIGN_PROTOCOL_V2_ID}:")
            else CAMPAIGN_PROTOCOL_V1_ID
        )
        resolved_protocol_id = str(
            protocol_id or source.get("protocol_id") or inferred_protocol_id
        ).strip()
        if resolved_protocol_id not in {CAMPAIGN_PROTOCOL_V1_ID, CAMPAIGN_PROTOCOL_V2_ID}:
            raise ValueError("unsupported campaign protocol")
        job_name = self.campaign_job_name(campaign)
        existing = self.store.get_operator_job(job_name)
        if isinstance(existing, Mapping):
            # A scheduler restart may observe a protocol written by the
            # pre-compact writer after it crashed before its first queue
            # commit.  Re-read and repair under the writer lock so two
            # restarts cannot each spend the same first campaign slot.
            with self.store.transaction(immediate=True):
                durable = self.store.get_operator_job(job_name)
                if not isinstance(durable, Mapping):
                    return {}
                existing_payload = dict(durable.get("payload") or {})
                protocol = existing_payload.get("protocol")
                if not isinstance(protocol, Mapping):
                    self._campaign_protocol_state(existing_payload)
                protocol_hash = _hash_document(protocol)
                declared_hash = str(existing_payload.get("protocol_hash", "")).strip()
                if declared_hash and declared_hash != protocol_hash:
                    raise AutonomousResearchError(
                        "CAMPAIGN_PROTOCOL_INVALID",
                        "campaign operator job protocol identity does not match its canonical protocol",
                    )
                payload_status = str(existing_payload.get("status", "")).strip().upper()
                record_status = str(durable.get("status", "")).strip().upper()
                if (
                    payload_status in CAMPAIGN_TERMINAL_STATUSES
                    or record_status in CAMPAIGN_TERMINAL_STATUSES
                    or str(existing_payload.get("supersession_reason", "")).strip().upper()
                    == "SUPERSEDED_PROTOCOL"
                ):
                    # Terminal campaigns are immutable evidence.  Validate an
                    # already-declared hash, but do not rewrite legacy rows or
                    # enqueue a successor.
                    if declared_hash:
                        self._campaign_protocol_state(existing_payload)
                    self._campaign_active_state = existing_payload
                    return existing_payload
                if not str(existing_payload.get("status", "")).strip():
                    existing_payload["status"] = record_status or "PLANNED"
                if "protocol_hash" not in existing_payload or not declared_hash:
                    existing_payload["protocol_hash"] = protocol_hash
                resumed_payload = self._campaign_state_defaults(
                    existing_payload,
                    protocol,
                    campaign_id=campaign,
                    now=current,
                )
                status = str(resumed_payload.get("status", "PLANNED")).strip().upper()
                if resumed_payload != dict(durable.get("payload") or {}):
                    self.store.set_operator_job(
                        job_name,
                        status,
                        resumed_payload,
                        resumable=True,
                        timestamp=current,
                    )
                self._campaign_active_state = resumed_payload
                if status == "PLANNED":
                    self._campaign_queue_next(current)
                resumed_record = self.store.get_operator_job(job_name)
                result_payload = (
                    resumed_record.get("payload")
                    if isinstance(resumed_record, Mapping)
                    and isinstance(resumed_record.get("payload"), Mapping)
                    else resumed_payload
                )
            return dict(result_payload)
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
        attestation = self._validate_campaign_dataset_provenance(
            resolved_dataset_id,
            resolved_dataset_version,
            source=source,
        )
        rows = self._campaign_dataset_rows(self.store, resolved_dataset_id, resolved_dataset_version)
        if len(rows) != int(attestation.get("row_count", -1)):
            raise AutonomousResearchError(
                "SOFTWARE_OR_INPUT_ERROR",
                "campaign dataset rows do not match the attested catalog count",
            )
        prior = self._campaign_prior_configuration_keys(resolved_protocol_id)
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
            protocol_id=resolved_protocol_id,
            attestation_hash=str(attestation.get("attestation_hash", "")).strip(),
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
            "schema_version": protocol.get("schema_version", CAMPAIGN_SCHEMA_V1),
            "protocol_id": resolved_protocol_id,
            "campaign_id": campaign,
            "status": "PLANNED" if trials else "CAMPAIGN_EXHAUSTED_NO_QUALIFIED_STRATEGY",
            "protocol": protocol,
            "protocol_hash": protocol_hash,
            "reassessment_boundaries": {},
            "last_evidence_identity": str(attestation.get("attestation_hash", "")).strip() or None,
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
        if code in {"INSUFFICIENT_DATA", "DATA_INSUFFICIENT"}:
            return "DATA_INSUFFICIENT"
        if code in {
            "PROCESSING_FAILED",
            "INVALID_PLAN",
            "INVALID_DATASET",
            "SOFTWARE_OR_INPUT_ERROR",
            "DATASET_PROVENANCE_INVALID",
            "DATASET_ATTESTATION_MISSING",
            "DATASET_ATTESTATION_STALE",
            "DATASET_ATTESTATION_CHANGED",
        }:
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
                self._validate_campaign_dataset_provenance(
                    str(trial_boundary.get("dataset_id", "")).strip(),
                    str(trial_boundary.get("dataset_version", "")).strip(),
                    boundary=trial_boundary,
                    plan=plan,
                )
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
                        "dataset_boundary": dict(trial_boundary),
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
        finalist_assessment = next(
            (
                item
                for item in assessments
                if item.get("status") == "QUALIFIED"
                and isinstance(item.get("dataset_boundary"), Mapping)
            ),
            None,
        )
        envelope_boundary = (
            dict(finalist_assessment["dataset_boundary"])
            if isinstance(finalist_assessment, Mapping)
            else None
        )
        if envelope_boundary is not None:
            finalist_manifest = finalist_assessment.get("protected_row_identity_manifest")
            persisted_final_manifest = (
                dict(finalist_manifest)
                if isinstance(finalist_manifest, Mapping)
                else list(finalist_manifest)
                if isinstance(finalist_manifest, (list, tuple))
                else {}
            )
            final_digest = str(
                envelope_boundary.get(
                    "ordered_row_manifest_digest",
                    envelope_boundary.get("content_hash", ""),
                )
            ).strip()
            final_row_count = int(envelope_boundary.get("row_count", 0))
        else:
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
            "dataset_boundary": envelope_boundary,
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
        if (
            str(record.get("status", "")).strip().upper() in CAMPAIGN_TERMINAL_STATUSES
            or str(payload.get("status", "")).strip().upper() in CAMPAIGN_TERMINAL_STATUSES
            or str(payload.get("supersession_reason", "")).strip().upper()
            == "SUPERSEDED_PROTOCOL"
        ):
            return
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
        *,
        attestation_hash: str | None = None,
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
        if str(attestation_hash or "").strip():
            boundary["attestation_hash"] = str(attestation_hash).strip()
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
        job_name = self.campaign_job_name(campaign_id)
        record = self.store.get_operator_job(job_name)
        if not isinstance(record, Mapping):
            raise ValueError("campaign does not exist")
        payload = dict(record.get("payload") or {})
        # Terminal campaign outcomes are immutable evidence.  Guard before
        # protocol, identity, provenance, or clock handling so legacy terminal
        # rows cannot be reopened by changed evidence (including v1
        # non-resumable input-error and superseded-protocol rows).
        record_status = str(record.get("status", "")).strip().upper()
        payload_status = str(payload.get("status", "")).strip().upper()
        supersession_reason = (
            str(payload.get("supersession_reason", "")).strip().upper()
            or str(record.get("supersession_reason", "")).strip().upper()
        )
        resumable_marker = payload.get("resumable")
        if resumable_marker is None:
            resumable_marker = record.get("resumable")
        legacy_nonresumable_error = (
            (
                record_status == "SOFTWARE_OR_INPUT_ERROR"
                or payload_status == "SOFTWARE_OR_INPUT_ERROR"
            )
            and (
                resumable_marker is False
                or (
                    isinstance(resumable_marker, str)
                    and resumable_marker.strip().upper() in {"0", "FALSE"}
                )
                or (
                    isinstance(resumable_marker, (int, float))
                    and not isinstance(resumable_marker, bool)
                    and resumable_marker == 0
                )
            )
        )
        if (
            record_status in CAMPAIGN_TERMINAL_STATUSES
            or payload_status in CAMPAIGN_TERMINAL_STATUSES
            or supersession_reason == "SUPERSEDED_PROTOCOL"
            or legacy_nonresumable_error
        ):
            return payload
        current = ensure_utc(now or self.clock())
        protocol, _ = self._campaign_protocol_state(payload)
        identity = str(evidence_identity).strip()
        if not identity:
            raise ValueError("evidence_identity is required")
        boundary = protocol.get("dataset_boundary")
        boundary_identity = (
            str(boundary.get("attestation_hash")).strip()
            if isinstance(boundary, Mapping)
            and boundary.get("attestation_hash") is not None
            else ""
        )
        raw_previous_identity = payload.get("last_evidence_identity")
        previous_identity = (
            str(raw_previous_identity).strip()
            if raw_previous_identity is not None
            else ""
        ) or boundary_identity or None
        if previous_identity is None:
            # Legacy rows without an authenticated prior identity cannot
            # establish that supplied evidence is changed.
            return payload
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
        reassessment_attestation = self._validate_campaign_dataset_provenance(
            resolved_dataset_id,
            resolved_dataset_version,
            expected_attestation_hash=identity,
        )
        new_rows = self._campaign_dataset_rows(
            self.store,
            resolved_dataset_id,
            resolved_dataset_version,
        )
        if len(new_rows) != int(reassessment_attestation.get("row_count", -1)):
            raise AutonomousResearchError(
                "SOFTWARE_OR_INPUT_ERROR",
                "campaign reassessment rows do not match the attested catalog count",
            )
        reassessment_boundary, _ = self._campaign_boundary_for_dataset(
            resolved_dataset_id,
            resolved_dataset_version,
            new_rows,
            attestation_hash=str(
                reassessment_attestation.get("attestation_hash", "")
            ).strip(),
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
            self._validate_campaign_dataset_provenance(
                str(campaign_boundary.get("dataset_id", "")).strip(),
                str(campaign_boundary.get("dataset_version", "")).strip(),
                boundary=campaign_boundary,
                plan=plan,
            )
            self._validate_persisted_dataset_provenance(plan)
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

    def _validate_campaign_dataset_provenance(
        self,
        dataset_id: str,
        dataset_version: str,
        *,
        boundary: Mapping[str, Any] | None = None,
        source: Mapping[str, Any] | None = None,
        plan: ExperimentPlan | None = None,
        expected_attestation_hash: str | None = None,
    ) -> Mapping[str, Any]:
        """Validate a campaign dataset fence before loading any trial rows."""
        identifier = str(dataset_id).strip()
        version = str(dataset_version).strip()

        def invalid(detail: str) -> NoReturn:
            raise AutonomousResearchError("SOFTWARE_OR_INPUT_ERROR", detail)

        if not identifier or not version or version.casefold() in _MUTABLE_DATASET_VERSION_ALIASES:
            invalid("campaign requires an exact immutable dataset id and version")
        catalog_loader = getattr(self.store, "load_dataset_catalog", None)
        if not callable(catalog_loader):
            invalid(f"no persisted dataset catalog for {identifier}/{version}")
        try:
            catalog = catalog_loader(identifier, version)
        except Exception as exc:
            invalid(f"persisted dataset catalog could not be loaded for {identifier}/{version}: {exc}")
        if not isinstance(catalog, Mapping):
            invalid(f"no persisted dataset catalog for {identifier}/{version}")
        catalog_id = str(catalog.get("dataset_id", "")).strip()
        catalog_version = str(catalog.get("dataset_version", catalog.get("version", ""))).strip()
        if catalog_id != identifier or catalog_version != version:
            invalid("campaign dataset catalog identity does not match its frozen boundary")
        if catalog.get("missing_ranges"):
            invalid("campaign dataset catalog contains explicit missing ranges")
        if str(catalog.get("market_type", "")).strip().lower() != MarketType.PREDICTION.value:
            invalid("campaign dataset catalog market_type is not prediction")
        catalog_source_type = str(catalog.get("source_type", "")).strip().upper()
        if catalog_source_type != "HISTORICAL":
            invalid(
                "campaign dataset catalog source_type "
                f"{catalog_source_type or '<missing>'} is not HISTORICAL"
            )
        catalog_instrument = str(catalog.get("instrument", "")).strip().upper()
        if catalog_instrument != "POLYMARKET":
            invalid("campaign dataset catalog instrument is not POLYMARKET")
        if catalog.get("missing_ranges"):
            raise AutonomousResearchError(
                "DATASET_PROVENANCE_INVALID",
                "persisted dataset catalog contains explicit missing ranges",
            )
        metadata = catalog.get("metadata")
        metadata = metadata if isinstance(metadata, Mapping) else {}
        metadata_source_type = str(metadata.get("source_type", "")).strip().upper()
        if metadata_source_type and metadata_source_type != "HISTORICAL":
            invalid("campaign dataset metadata source_type is not HISTORICAL")
        if catalog.get("discovery_complete") is False:
            invalid("campaign dataset catalog discovery coverage is incomplete")
        coverage_status = str(metadata.get("coverage_status", "")).strip().upper()
        if coverage_status and coverage_status != "COMPLETE":
            invalid("campaign dataset catalog coverage status is not COMPLETE")
        requested_coverage = metadata.get("requested_coverage")
        if (
            isinstance(requested_coverage, Mapping)
            and requested_coverage.get("discovery_complete") is not True
        ):
            invalid("campaign dataset requested coverage is incomplete")

        declared = source if isinstance(source, Mapping) else {}
        selector = declared.get("dataset_selector")
        selector = dict(selector) if isinstance(selector, Mapping) else {}
        for raw_name, field in (
            ("dataset_source_type", "source_type"),
            ("dataset_source", "source"),
            ("dataset_timeframe", "timeframe"),
        ):
            if raw_name in declared and declared.get(raw_name) is not None:
                selector.setdefault(field, declared.get(raw_name))
        if plan is not None:
            selector = plan.dataset_selector
        for field in ("provider", "source", "timeframe", "interval", "source_type"):
            value = selector.get(field)
            if value is None or not str(value).strip():
                continue
            if field == "source_type":
                if str(value).strip().upper() != catalog_source_type:
                    invalid(f"campaign dataset selector {field} does not match HISTORICAL")
            elif field in {"timeframe", "interval"}:
                if str(value).strip() != str(catalog.get("timeframe", "")).strip():
                    invalid(f"campaign dataset selector {field} does not match its catalog")
            else:
                catalog_source = str(
                    catalog.get("provider", catalog.get("source", ""))
                ).strip()
                if str(value).strip() != catalog_source:
                    invalid(f"campaign dataset selector {field} does not match its catalog")

        try:
            catalog_count = catalog.get("row_count")
            catalog_complete = float(catalog.get("completeness", 0.0))
        except (TypeError, ValueError, OverflowError):
            invalid("campaign dataset catalog row count or completeness is malformed")
        if (
            isinstance(catalog_count, bool)
            or not isinstance(catalog_count, int)
            or catalog_count <= 0
            or not math.isfinite(catalog_complete)
            or catalog_complete < 1.0
        ):
            invalid("campaign dataset catalog is empty or incomplete")

        if isinstance(boundary, Mapping):
            boundary_id = str(boundary.get("dataset_id", "")).strip()
            boundary_version = str(boundary.get("dataset_version", "")).strip()
            boundary_source = str(boundary.get("source_type", "")).strip().upper()
            if (
                boundary_id != identifier
                or boundary_version != version
                or boundary_source != "HISTORICAL"
            ):
                invalid("campaign dataset provenance does not match its frozen boundary")
            boundary_count = boundary.get("row_count")
            if (
                isinstance(boundary_count, bool)
                or not isinstance(boundary_count, int)
                or boundary_count != catalog_count
            ):
                invalid("campaign dataset row count does not match its frozen boundary")

        attestation_loader = getattr(self.store, "load_dataset_integrity_attestation", None)
        if not callable(attestation_loader):
            invalid(f"no persisted dataset attestation for {identifier}/{version}")
        try:
            attestation = attestation_loader(identifier, version)
        except Exception as exc:
            invalid(f"persisted dataset attestation could not be loaded for {identifier}/{version}: {exc}")
        if not isinstance(attestation, Mapping):
            invalid(f"no persisted dataset attestation for {identifier}/{version}")
        attestation_id = str(attestation.get("dataset_id", "")).strip()
        attestation_version = str(attestation.get("dataset_version", "")).strip()
        attestation_hash = str(attestation.get("attestation_hash", "")).strip()
        if (
            attestation_id != identifier
            or attestation_version != version
            or not attestation_hash
        ):
            invalid("campaign dataset attestation identity or hash is mismatched")
        if expected_attestation_hash and attestation_hash != str(expected_attestation_hash).strip():
            invalid("campaign reassessment evidence does not match the current attestation")
        if isinstance(boundary, Mapping):
            frozen_hash = str(boundary.get("attestation_hash", "")).strip()
            if frozen_hash and frozen_hash != attestation_hash:
                invalid("campaign dataset attestation changed after the boundary was frozen")
        if str(attestation.get("status", "")).strip().upper() != "CURRENT":
            invalid("campaign dataset attestation is not CURRENT")
        if str(attestation.get("source_type", "")).strip().upper() != "HISTORICAL":
            invalid("campaign dataset attestation source_type is not HISTORICAL")
        if str(attestation.get("market_type", "")).strip().lower() != MarketType.PREDICTION.value:
            invalid("campaign dataset attestation market_type is not prediction")
        if str(attestation.get("contamination_result", "")).strip().upper() != "PASS":
            invalid("campaign dataset attestation reports forward contamination")
        attestation_count = attestation.get("row_count")
        try:
            attestation_complete = float(attestation.get("completeness", 0.0))
        except (TypeError, ValueError, OverflowError):
            invalid("campaign dataset attestation completeness is malformed")
        if (
            isinstance(attestation_count, bool)
            or not isinstance(attestation_count, int)
            or attestation_count != catalog_count
            or not math.isfinite(attestation_complete)
            or attestation_complete < 1.0
        ):
            invalid("campaign dataset attestation does not match its catalog")
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
            strategy_hash = _content_hash(strategy.to_dict())
            rolling_strategy_document = dict(strategy.to_dict())
            rolling_strategy_document.pop("strategy_id", None)
            rolling_strategy_hash = _rolling_hash(rolling_strategy_document)
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
            strategy_version_id = "strategy-version-" + _rolling_hash(
                {"strategy_hash": rolling_strategy_hash, "candidate_id": candidate_id}
            ).removeprefix("sha256:")[:40]
            research_trial_id = "research-trial-" + _rolling_hash(
                {
                    "candidate_id": candidate_id,
                    "strategy_version_id": strategy_version_id,
                    "source_config_hash": plan.plan_hash,
                }
            ).removeprefix("sha256:")[:40]
            forward_config["strategy_version_id"] = strategy_version_id
            forward_config["research_trial_id"] = research_trial_id
            forward_config["candidate_id"] = candidate_id
            forward_config["plan_id"] = plan.plan_id
            forward_config["strategy_hash"] = strategy_hash
            forward_config["source_strategy_hash"] = strategy_hash
            forward_config["rolling_strategy_hash"] = rolling_strategy_hash
            if plan.market_type is not MarketType.CRYPTO_SPOT:
                forward_config = _canonical_forward_config(forward_config)
            config_hash = _hash_document({"config": forward_config, "risk_limits": risk_snapshot})
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
                    "source_strategy_hash": strategy_hash,
                    "rolling_strategy_hash": rolling_strategy_hash,
                    "strategy_version_id": strategy_version_id,
                    "research_trial_id": research_trial_id,
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
                strategy_version_id=strategy_version_id,
                research_trial_id=research_trial_id,
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
        spec_config = spec.config if isinstance(spec.config, Mapping) else {}
        try:
            paper_lineage = _rolling_source_binding(
                {"payload": spec_config, "strategy_hash": spec.strategy_hash}
            )
        except (TypeError, ValueError):
            paper_lineage = {}
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
        observation_open_by_market: dict[str, datetime] = {}
        for observation in observations:
            if not isinstance(observation, Mapping):
                continue
            observation_payload = observation.get("payload")
            observation_payload = (
                observation_payload
                if isinstance(observation_payload, Mapping)
                else {}
            )
            observation_market = str(
                observation.get(
                    "market_id",
                    observation_payload.get("market_id", ""),
                )
            ).strip()
            observation_time = _rolling_timestamp(
                observation.get("timestamp")
                or observation_payload.get("timestamp")
                or observation.get("observed_at")
                or observation_payload.get("observed_at")
            )
            if observation_market and observation_time is not None:
                previous = observation_open_by_market.get(observation_market)
                if previous is None or observation_time < previous:
                    observation_open_by_market[observation_market] = observation_time

        def complete_resolved_ledger(
            source_ledger: Mapping[str, Any],
            market: str,
        ) -> dict[str, Any]:
            ledger = dict(source_ledger)
            rolling_hash = _binding_value(paper_lineage.get("rolling_strategy_hash"))
            source_hash = _binding_value(
                paper_lineage.get("source_strategy_hash")
            ) or _binding_value(spec.strategy_hash)
            if rolling_hash is not None:
                ledger["strategy_hash"] = rolling_hash
                ledger["rolling_strategy_hash"] = rolling_hash
            elif source_hash is not None:
                ledger["strategy_hash"] = source_hash
            if source_hash is not None:
                ledger["source_strategy_hash"] = source_hash
            for field in (
                "strategy_version_id",
                "research_trial_id",
                "candidate_id",
                "plan_id",
            ):
                value = paper_lineage.get(field)
                if value is not None:
                    ledger[field] = value
            shortages: list[str] = []
            for field in ("net_pnl", "fees", "slippage", "capital_at_risk"):
                value = ledger.get(field)
                if value is None or not _is_finite_number(value):
                    shortages.append(field)
            resolved_time = _rolling_timestamp(ledger.get("resolved_at"))
            if resolved_time is None:
                shortages.append("resolved_at")
            source_open_time = next(
                (
                    stamp
                    for name in (
                        "observation_open_timestamp",
                        "observation_opened_at",
                        "open_timestamp",
                        "opened_at",
                        "observation_timestamp",
                        "observation_at",
                        "available_from",
                        "coverage_from",
                        "window_start",
                    )
                    if (stamp := _rolling_timestamp(ledger.get(name))) is not None
                ),
                None,
            )
            if (
                source_open_time is not None
                and resolved_time is not None
                and source_open_time > resolved_time
            ):
                shortages.append("observation_open_timestamp_after_resolved_at")
            open_time = observation_open_by_market.get(market) or source_open_time
            if open_time is None:
                market_fills = fills_by_market.get(market, ())
                if market_fills:
                    open_time = min(ensure_utc(fill.timestamp) for fill in market_fills)
            if open_time is None:
                shortages.append("observation_open_timestamp")
            elif resolved_time is not None and open_time > resolved_time:
                shortages.append("observation_open_timestamp_after_resolved_at")
            if shortages:
                ledger["accounting_shortage"] = {
                    "code": "PAPER_RESOLVED_BET_ACCOUNTING_INCOMPLETE",
                    "fields": sorted(set(shortages)),
                }
                return ledger
            capital = Decimal(str(ledger["capital_at_risk"]))
            net_pnl = Decimal(str(ledger["net_pnl"]))
            if not capital.is_finite() or capital <= Decimal("0"):
                ledger["accounting_shortage"] = {
                    "code": "PAPER_RESOLVED_BET_ACCOUNTING_INCOMPLETE",
                    "fields": ["capital_at_risk_nonpositive"],
                }
                return ledger
            drawdown = min(Decimal("1"), max(Decimal("0"), -net_pnl / capital))
            ledger.pop("accounting_shortage", None)
            ledger.update(
                {
                    "allocated_capital": ledger["capital_at_risk"],
                    "allocated_capital_net_return": ledger["net_pnl"],
                    "realized_pnl": ledger["net_pnl"],
                    "unrealized_pnl": 0.0,
                    "costs": ledger["slippage"],
                    "completed_outcomes": 1,
                    # Reliability means the settlement/accounting contract
                    # completed, never that this bet was profitable.
                    "reliability": 1.0,
                    "drawdown": float(drawdown),
                    "observation_open_timestamp": open_time.isoformat(),
                    "available_from": open_time.isoformat(),
                    "coverage_from": open_time.isoformat(),
                    "available_through": resolved_time.isoformat(),
                    "coverage_through": resolved_time.isoformat(),
                }
            )
            return ledger
        ledger_by_market: dict[str, Mapping[str, Any]] = {}
        for row in ledger_records:
            if not isinstance(row, Mapping):
                continue
            market_id = str(row.get("market_id", "")).strip()
            source_ledger = row.get("payload")
            if not market_id or not isinstance(source_ledger, Mapping):
                continue
            ledger = dict(source_ledger)
            for field in ("bet_id", "outcome", "resolution", "resolved_at"):
                if field not in ledger and row.get(field) is not None:
                    value = row.get(field)
                    ledger[field] = value.isoformat() if isinstance(value, datetime) else value
            ledger = complete_resolved_ledger(ledger, market_id)
            ledger_by_market[market_id] = ledger
            resolved_time = _rolling_timestamp(ledger.get("resolved_at"))
            if (
                ledger.get("bet_id")
                and ledger.get("outcome")
                and ledger.get("resolution")
                and resolved_time is not None
            ):
                self.store.save_paper_bet_ledger(
                    str(ledger["bet_id"]),
                    forward_id,
                    market_id,
                    spec.strategy_hash,
                    str(ledger["outcome"]),
                    str(ledger["resolution"]),
                    resolved_time,
                    ledger,
                )
        for market_id, resolution in terminal_by_market.items():
            if market_id in ledger_by_market:
                continue
            resolved_at = next(
                (
                    stamp
                    for row in observations
                    if isinstance(row, Mapping)
                    and isinstance(row.get("payload"), Mapping)
                    and str(
                        row.get("market_id", row["payload"].get("market_id", ""))
                    ).strip()
                    == market_id
                    and _settlement_name(row["payload"].get("settlement")) == resolution
                    if (stamp := _rolling_timestamp(
                        row.get("timestamp")
                        or row["payload"].get("timestamp")
                        or row.get("observed_at")
                        or row["payload"].get("observed_at")
                    )) is not None
                ),
                None,
            )
            if resolved_at is None:
                continue
            ledger = build_resolved_bet(
                experiment_id=forward_id,
                market_id=market_id,
                strategy_id=spec.strategy_hash,
                settlement=resolution,
                resolved_at=resolved_at,
                fills=fills_by_market.get(market_id, ()),
            )
            if ledger is not None:
                ledger = complete_resolved_ledger(ledger, market_id)
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
        # Historical PRICE_PROXY records may persist the observed probability
        # under ``price``.  This is a representation normalization, not a
        # quote synthesis: bid/ask fields remain absent when they were absent
        # in the source dataset.
        if row.get("yes_mid") is None and row.get("price") is not None:
            row["yes_mid"] = row["price"]
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
