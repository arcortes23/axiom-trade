"""Local operator dashboard HTTP server.

The server is dependency-free and every JSON endpoint remains available for
automation. ``/`` serves the dark research console with paper-first controls.
"""
from __future__ import annotations

from dataclasses import asdict, is_dataclass
from itertools import islice
from datetime import date, datetime
import hmac
import ipaddress
import json
import logging
import math
from decimal import Decimal
import re
import secrets
import sqlite3
from . import canary as canary_module
from .canary import CanaryService, _canary_eligibility_is_bound, _canary_has_last_good
from .canary_settings import CanarySettingsService
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from typing import Any, Callable, Mapping, Sequence
from urllib.parse import parse_qs, unquote, urlparse
from .operator import CANARY_CONNECTIVITY_CONFIG_KEY, OperatorControlPlane, _stored_connectivity_projection

_CLIENT_DISCONNECT_ERRORS = (BrokenPipeError, ConnectionAbortedError, ConnectionResetError)
_LOGGER = logging.getLogger(__name__)

from .director import research_summary
from .domain import ensure_utc, parse_timestamp, to_record

def _reject_json_constant(value: str) -> Any:
    raise ValueError(f"non-finite JSON constant: {value}")



def _loopback_host(value: str) -> bool:
    if str(value).strip().lower() == "localhost":
        return True
    try:
        return ipaddress.ip_address(str(value).strip()).is_loopback
    except ValueError:
        return False

_ENDPOINTS = (
    "overview",
    "operator",
    "datasets",
    "research",
    "research-summary",
    "crypto",
    "crypto-research",
    "btc-research",
    "prediction",
    "polymarket-research",
    "evolution",
    "risk",
    "risk-settings",
    "paper-portfolio",
    "opportunities",
    "queue",
    "autonomous-research",
    "hermes",
    "system",
    "status",
    "dataset-health",
    "evidence-maturity",
    "strategy",
)
_V2_ENDPOINTS = ("overview-summary", "canary", "binance-canary", "datasets", "activity", "candidates", "polymarket", "hermes", "crypto-research", "crypto", "paper")

_DEFAULT_PAGE_SIZE = 25
_PAGE_SIZE_OPTIONS = (10, 25, 50, 100)
_MAX_PAGE_SIZE = 100
_CANARY_ELIGIBLE_STAGES = frozenset({"FROZEN", "PAPER_FORWARD", "PAPER_PROMOTABLE"})
_PAPER_FORWARD_STAGES = frozenset({"PAPER_FORWARD", "PAPER_PROMOTABLE"})
_BINANCE_HTTP_FORBIDDEN_ACTIONS = frozenset({"EXECUTION_PROBE", "RECONCILE_PROBE"})
_MARKET_SCOPE_FUNNEL_STAGES = (
    "historically_qualified",
    "valid_frozen_scope",
    "matching_current_markets",
    "fresh_complete_inputs",
    "strategy_evaluated",
    "ready_signal",
    "execution_feasible",
    "submitted",
    "filled",
)


def _empty_market_scope_funnel() -> dict[str, Any]:
    stages = {
        name: {
            "count": 0,
            "blocker_counts": {},
            "timestamps": {"latest": None, "earliest": None},
        }
        for name in _MARKET_SCOPE_FUNNEL_STAGES
    }
    return {
        "available": False,
        "stages": stages,
        "stage_counts": {name: 0 for name in _MARKET_SCOPE_FUNNEL_STAGES},
        "blocker_counts": {},
        "timestamps": {"as_of": None, "latest": None, "earliest": None},
        "as_of": None,
        "live_execution": False,
    }


def _pagination_error(query: Mapping[str, Any]) -> str | None:
    """Return a client-facing validation message for v2 query parameters."""
    def first(name: str) -> str:
        value = query.get(name, "")
        if isinstance(value, (list, tuple)):
            value = value[0] if value else ""
        return str(value).strip()

    raw_page = first("page")
    if raw_page:
        try:
            if int(raw_page) < 1:
                return "page must be a positive integer"
        except ValueError:
            return "page must be a positive integer"
    raw_size = first("page_size")
    if raw_size:
        try:
            size = int(raw_size)
        except ValueError:
            return "page_size must be one of 10, 25, 50, or 100"
        if size not in _PAGE_SIZE_OPTIONS:
            return "page_size must be one of 10, 25, 50, or 100"
    return None
_MAX_SIZE_FALLBACK = 1000
_LATEST_CANDIDATE_LIMIT = 50

_RESEARCH_PROGRESS_LIMIT = 50
_RESEARCH_FORWARD_TEST_LIMIT = 100
_CAMPAIGN_JOB_PREFIX = "polymarket-research-campaign:"
_CAMPAIGN_JOB_LIMIT = 32
_CAMPAIGN_TRIAL_TERMINAL = frozenset(
    {
        "ECONOMIC_REJECTION",
        "DATA_INSUFFICIENT",
        "SOFTWARE_OR_INPUT_ERROR",
        "VALIDATION_QUALIFIED",
        "FINAL_ASSESSMENT",
    }
)


def _pagination_params(query: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """Normalize dashboard pagination controls without allowing large reads."""
    query = query or {}

    def first(name: str, default: str = "") -> str:
        value = query.get(name, default)
        if isinstance(value, (list, tuple)):
            value = value[0] if value else default
        return str(value).strip()

    try:
        page = int(first("page", "1"))
    except (TypeError, ValueError):
        page = 1
    page = max(1, page)
    try:
        page_size = int(first("page_size", str(_DEFAULT_PAGE_SIZE)))
    except (TypeError, ValueError):
        page_size = _DEFAULT_PAGE_SIZE
    if page_size not in _PAGE_SIZE_OPTIONS:
        page_size = _DEFAULT_PAGE_SIZE
    page_size = min(_MAX_PAGE_SIZE, max(10, page_size))
    direction = first("direction", "desc").lower()
    if direction not in {"asc", "desc"}:
        direction = "desc"
    return {
        "page": page,
        "page_size": page_size,
        "sort": first("sort", ""),
        "direction": direction,
        "filter": first("filter", "") or None,
        "stage": first("stage", "") or None,
        "source_type": first("source_type", "") or None,
        "market": first("market", "") or None,
        "timeframe": first("timeframe", "") or None,
        "quality": first("quality", "") or None,
        "category": first("category", "") or None,
        "settlement": first("settlement", "") or None,
        "status": first("status", "") or None,
        "kind": first("kind", "") or None,
        "item_id": first("item_id", "") or None,
        "record_type": first("record_type", "") or None,
        "symbol": first("symbol", "") or None,
        "universe_version": first("universe_version", "") or None,
        "dataset_version": first("dataset_version", "") or None,
    }


def _page_result(
    items: Any,
    *,
    page: int,
    page_size: int,
    total: int | None = None,
) -> dict[str, Any]:
    """Return the stable common response shape used by every v2 collection."""
    values = list(items) if isinstance(items, (list, tuple)) else []
    total_value = max(len(values), int(total if total is not None else len(values)))
    pages = max(1, math.ceil(total_value / page_size)) if total_value else 0
    return {
        "items": values,
        "page": int(page),
        "page_size": int(page_size),
        "total": total_value,
        "pages": pages,
    }


def _jsonable(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (tuple, list, set, frozenset)):
        return [_jsonable(v) for v in value]
    if isinstance(value, (datetime, date)):
        return ensure_utc(value).isoformat() if isinstance(value, datetime) else value.isoformat()
    if is_dataclass(value):
        try:
            return _jsonable(to_record(value))
        except (TypeError, ValueError):
            return _jsonable(asdict(value))
    if hasattr(value, "value") and not isinstance(value, (str, bytes)):
        return value.value
    return value


def _bounded_value(value: Any, *, depth: int = 0) -> Any:
    """Compact nested persisted values before putting them on a dashboard row."""
    if depth >= 4:
        return "<truncated>"
    if isinstance(value, Mapping):
        return {
            str(key): _bounded_value(child, depth=depth + 1)
            for key, child in islice(value.items(), 32)
        }
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_bounded_value(child, depth=depth + 1) for child in islice(value, 32)]
    if isinstance(value, (datetime, date)):
        return _jsonable(value)
    if isinstance(value, str):
        return value if len(value) <= 1024 else value[:1021] + "..."
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, float):
        return value
    return _jsonable(value)
_BINANCE_SECRET_KEY = re.compile(
    r"(?:secret|password|passwd|token|api[_-]?key|apikey|private[_-]?key|private|mnemonic|passphrase|authorization|bearer|credential)",
    re.IGNORECASE,
)
_BINANCE_CREDENTIAL_HASH_RE = re.compile(r"\A[0-9a-fA-F]{64}\Z")


def _binance_safe_value(value: Any, *, depth: int = 0, key: str | None = None) -> Any:
    """Bound and redact an untrusted Binance projection before JSON output.

    Secret-shaped keys own their entire subtree.  Credential status is the
    only allowlisted exception and is deliberately reconstructed at this
    projection boundary.
    """
    if depth >= 6:
        return "<truncated>"
    key_text = str(key) if key is not None else ""
    lowered = key_text.lower()

    if lowered == "credentials":
        if isinstance(value, Mapping) and "configured" in value:
            reference = value.get("reference_hash")
            reference_text = (
                reference.casefold()
                if type(reference) is str and _BINANCE_CREDENTIAL_HASH_RE.fullmatch(reference)
                else None
            )
            return {
                "configured": value.get("configured") is True,
                "reference_hash": reference_text,
            }
        return "<redacted>"
    if key_text and _BINANCE_SECRET_KEY.search(key_text):
        return "<redacted>"

    # Decode JSON columns before walking them.  A raw JSON string can hide an
    # entire nested secret subtree even when the column name itself is safe.
    if lowered.endswith("_json") and isinstance(value, str):
        try:
            decoded = json.loads(value)
        except (TypeError, ValueError, json.JSONDecodeError):
            decoded = None
        if decoded is not None:
            return _binance_safe_value(decoded, depth=depth, key=key_text[:-5] or None)

    if isinstance(value, Mapping):
        return {
            str(name): _binance_safe_value(child, depth=depth + 1, key=str(name))
            for name, child in list(value.items())[:64]
        }
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_binance_safe_value(child, depth=depth + 1) for child in list(value)[:100]]
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, (datetime, date)):
        return _jsonable(value)
    if isinstance(value, str):
        return value if len(value) <= 4096 else value[:4093] + "..."
    if isinstance(value, float) and not math.isfinite(value):
        return None
    projected = _jsonable(value)
    if projected is value and not isinstance(value, (str, int, float, bool, type(None))):
        return str(value)[:1024]
    return projected


def _nested_value(*sources: Mapping[str, Any], keys: tuple[str, ...]) -> Any:
    for source in sources:
        for key in keys:
            value = source.get(key)
            if value is not None and value != "":
                return value
    return None
def _display_value_missing(value: Any) -> bool:
    """Return whether a persisted display value is absent rather than falsy."""
    return value is None or (isinstance(value, str) and not value.strip())
def _signal_projection(value: Any) -> dict[str, Any] | None:
    """Return a non-empty public signal mapping, or the explicit missing value."""
    if not isinstance(value, Mapping) or not value:
        return None
    projected = dict(value)
    return projected or None
def _signal_scan_reason_counts(*sources: Mapping[str, Any] | None) -> dict[str, int]:
    """Decode only the bounded reason counts for the persisted scan cycle."""
    flattened: list[Mapping[str, Any]] = []
    for source in sources:
        if not isinstance(source, Mapping):
            continue
        flattened.append(source)
        for nested_name in ("autonomous", "autonomous_canary", "worker", "readiness", "status_report"):
            nested = source.get(nested_name)
            if isinstance(nested, Mapping):
                flattened.append(nested)
    for source in flattened:
        raw = source.get("signal_scan_reason_counts")
        if _display_value_missing(raw):
            raw = source.get("signal_scan_reason_counts_json")
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except (TypeError, ValueError, json.JSONDecodeError):
                raw = None
        if not isinstance(raw, Mapping):
            continue
        counts: dict[str, int] = {}
        for key, value in list(raw.items())[:64]:
            try:
                count = int(value)
            except (TypeError, ValueError):
                continue
            if count < 0:
                continue
            reason = str(key).strip()[:128]
            if reason:
                counts[reason] = count
        return counts
    return {}




def _canonical_blocker(*sources: Mapping[str, Any] | None) -> Any:
    """Return the first present blocker across bounded report sections."""
    for source in sources:
        if not isinstance(source, Mapping):
            continue
        value = source.get("blocker")
        if not _display_value_missing(value):
            return value
    return None
def _canary_control_state(*sources: Mapping[str, Any] | None) -> str:
    """Return the first authoritative, non-unknown control state."""
    for source in sources:
        if not isinstance(source, Mapping):
            continue
        for key in ("control_state", "micro_live_canary", "state"):
            value = source.get(key)
            if _display_value_missing(value):
                continue
            state = str(value).strip().upper()
            if state and state != "UNKNOWN":
                return state
    return "UNKNOWN"


def _normalized_canary_blocker(
    blocker: Any,
    *control_sources: Mapping[str, Any] | None,
) -> Any:
    """Fill absent blockers from control state and hide stale disabled state."""
    control_state = _canary_control_state(*control_sources)
    normalized = str(blocker).strip().upper() if not _display_value_missing(blocker) else ""
    if normalized == "AUTONOMOUS_CANARY_DISABLED" and control_state in {
        "ARMED",
        "AUTONOMOUS_MICRO_LIVE",
        "LIVE",
    }:
        return None
    if not _display_value_missing(blocker):
        return blocker
    if control_state in {"DISABLED", "DISARMED"}:
        return "AUTONOMOUS_CANARY_DISABLED"
    if control_state == "UNKNOWN":
        return "AUTONOMOUS_CONTROL_UNKNOWN"
    return blocker






def _merge_persisted_values(
    persisted: Mapping[str, Any] | None,
    *sections: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Merge report sections without replacing non-null persisted values."""
    merged = dict(persisted) if isinstance(persisted, Mapping) else {}
    for section in sections:
        if not isinstance(section, Mapping):
            continue
        for key, value in section.items():
            if key not in merged or _display_value_missing(merged[key]):
                merged[key] = value
    return merged


def _patch_non_missing_values(
    target: Mapping[str, Any] | None,
    *sections: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Patch a bounded projection only with present values from later sections."""
    merged = dict(target) if isinstance(target, Mapping) else {}
    for section in sections:
        if not isinstance(section, Mapping):
            continue
        for key, value in section.items():
            if not _display_value_missing(value):
                merged[key] = value
    return merged



def _hermes_reason_code(row: Mapping[str, Any], result: Mapping[str, Any], payload: Mapping[str, Any]) -> str | None:
    value = _nested_value(result, payload, row, keys=("reason_code", "rejection_code", "error_code"))
    if value is None:
        return None
    return str(value).strip() or None


def _hermes_human_reason(row: Mapping[str, Any], result: Mapping[str, Any], payload: Mapping[str, Any]) -> str | None:
    value = _nested_value(
        result,
        payload,
        row,
        keys=("human_reason", "reason", "rejection_reason", "detail", "last_error", "error"),
    )
    if isinstance(value, Mapping):
        value = value.get("message") or value.get("detail") or value.get("reason")
    if value is None:
        return None
    return str(value).strip() or None


def _polymarket_quality_display(item: Mapping[str, Any]) -> tuple[str, str]:
    payload = item.get("payload")
    payload = payload if isinstance(payload, Mapping) else {}
    source_type = str(
        payload.get("source_type")
        or item.get("source_type")
        or ("HISTORICAL" if str(item.get("snapshot_id", "")).startswith("pmhist:") else "FORWARD_COLLECTED")
    ).upper()
    quality = str(item.get("quality") or payload.get("quality") or payload.get("research_quality") or "UNKNOWN").upper()
    if source_type == "HISTORICAL":
        if quality in {"HISTORICAL_ORDER_BOOK", "ORDER_BOOK"} or bool(item.get("historical_order_book_available")):
            return "HISTORICAL TIMESTAMPED DEPTH", "Historical depth is timestamped; it is not a current book."
        return "HISTORICAL PRICE PROXY", "Historical price history; no historical depth is asserted."
    if quality == "ORDER_BOOK_SIMULATED":
        return "CURRENT ORDER BOOK · SIMULATED EXECUTION", "Forward order-book observation; fills remain simulated."
    if quality in {"PRICE_PROXY", "UNKNOWN"}:
        return "CURRENT PRICE PROXY", "Forward price observation; no order-book depth is asserted."
    return f"CURRENT {quality.replace('_', ' ')}", "Forward observation; execution remains simulated."

def _hermes_row(item: Mapping[str, Any]) -> dict[str, Any]:
    """Expose queue provenance without requiring callers to decode JSON payloads."""
    payload = item.get("payload")
    payload = payload if isinstance(payload, Mapping) else {}
    result = item.get("result")
    result = result if isinstance(result, Mapping) else {}
    dataset_id = _nested_value(result, payload, keys=("dataset_id", "dataset", "data_id"))
    dataset_version = _nested_value(
        result,
        payload,
        keys=("dataset_version", "data_version", "version"),
    )
    family = _nested_value(
        result,
        payload,
        keys=("family", "experiment_family", "strategy_family"),
    )
    reason_code = _hermes_reason_code(item, result, payload)
    human_reason = _hermes_human_reason(item, result, payload)
    outcome_type, outcome_label = _hermes_outcome_type(item.get("status"), reason_code)
    timestamp = item.get("updated_at") or item.get("created_at")
    row = {
        "time": timestamp,
        "created_at": item.get("created_at"),
        "updated_at": item.get("updated_at"),
        "item_id": item.get("item_id"),
        "source": item.get("source"),
        "status": item.get("status"),
        "dataset_id": dataset_id,
        "dataset_version": dataset_version,
        "version": dataset_version,
        "family": family,
        "attempts": item.get("attempts", 0),
        "reason_code": reason_code,
        "outcome_type": outcome_type,
        "outcome_label": outcome_label,
        "last_error": item.get("last_error"),
        "human_reason": human_reason,
        "reason": human_reason,
        "item_type": item.get("item_type"),
        "payload": _bounded_value(payload),
        "result": _bounded_value(result) if result else None,
    }
    # Preserve queue columns used by existing automation while replacing only
    # unbounded nested values with compact representations.
    for key, value in item.items():
        if key not in row:
            row[key] = _bounded_value(value)
    return row


def _hermes_outcome_type(status: Any, reason_code: Any) -> tuple[str | None, str | None]:
    normalized_status = str(status or "").strip().upper()
    normalized_reason_code = str(reason_code or "").strip().upper()
    if normalized_reason_code == "DATASET_NOT_FOUND" and normalized_status in {"REJECTED", "FAILED"}:
        return "PROPOSAL_REJECTED", "PROPOSAL REJECTED"
    if normalized_status in {"REJECTED", "FAILED"}:
        return "EXPERIMENT_REJECTED", "EXPERIMENT REJECTED"
    if normalized_status in {"ACCEPTED", "COMPLETED"}:
        return "EXPERIMENT_COMPLETED", "EXPERIMENT COMPLETED"
    return None, None


def _is_terminal_hermes_status(value: Any) -> bool:
    return str(value or "").upper() in {"ACCEPTED", "COMPLETED", "REJECTED", "FAILED"}


def _candidate_record(candidate: Any) -> dict[str, Any]:
    strategy = getattr(candidate, "strategy", None)
    strategy_record = strategy.to_dict() if callable(getattr(strategy, "to_dict", None)) else strategy
    return {
        "candidate_id": getattr(candidate, "candidate_id", ""),
        "generation": getattr(candidate, "generation", 0),
        "lineage": list(getattr(candidate, "lineage", ())),
        "score": getattr(candidate, "score", None),
        "train_score": getattr(candidate, "train_score", None),
        "validation_score": getattr(candidate, "validation_score", None),
        "holdout_score": getattr(candidate, "holdout_score", None),
        "rejected": getattr(candidate, "rejected", False),
        "rejection_reason": getattr(candidate, "rejection_reason", None),
        "strategy": strategy_record,
    }


def _number_or_zero(value: Any) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 0.0
    return number if math.isfinite(number) else 0.0


_CANARY_SELECTION_STATUSES = frozenset({"CURRENT", "STALE", "NONE", "UNKNOWN"})
_CANARY_AUTONOMOUS_FIELDS = (
    "last_signal_id",
    "candidates_ranked",
    "candidates_signal_checked",
    "candidates_no_signal",
    "actionable_candidates_found",
    "selected_actionable_candidate",
    "selected_actionable_rank",
    "selected_actionable_score",
    "signal_scan_cursor",
    "signal_scan_ranking_run_id",
    "next_signal_scan_start_rank",
    "next_signal_scan_end_rank",
    "signal_scan_cycle_id",
    "signal_scan_candidate_universe_hash",
    "signal_scan_cycle_started_at",
    "signal_scan_cycle_completed_at",
    "signal_scan_cycle_complete",
    "signal_scan_checked_this_cycle",
    "signal_scan_remaining_this_cycle",
    "signal_scan_coverage_percentage",
    "signal_scan_skip_reasons_json",
    "signal_scan_reason_counts_json",
    "signal_scan_status",
)
_CANARY_STATUS_FIELDS = (
    "eligibility_raw_count",
    "eligible_count",
    "rankable_raw_count",
    "rankable_count",
    "ranking_run_id",
    "ranking_timestamp",
    "selection_status",
    "selection_valid",
    "selection_invalidation_reason",
    "selection_reason",
    "selected_candidate",
    "last_selected_candidate",
    *_CANARY_AUTONOMOUS_FIELDS,
)
_CANARY_RESEARCH_WINNER_FIELDS = ("winner_id", "winner_rank", "winner_score")
_CANARY_READINESS_FIELDS = (
    "readiness_snapshot_status",
    "readiness_snapshot_stale",
    "readiness_snapshot_reason",
    "readiness_snapshot_updated_at",
)


def _canary_autonomous_projection(
    *sources: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Project bounded autonomous scan fields without inventing values."""
    flattened: list[Mapping[str, Any]] = []
    for source in sources:
        if not isinstance(source, Mapping):
            continue
        flattened.append(source)
        for nested_name in (
            "autonomous",
            "autonomous_canary",
            "status",
            "status_report",
            "readiness",
            "worker",
            "execution",
        ):
            nested = source.get(nested_name)
            if isinstance(nested, Mapping):
                flattened.append(nested)
    projection: dict[str, Any] = {}
    for name in _CANARY_AUTONOMOUS_FIELDS:
        value: Any = None
        for source in flattened:
            candidate = source.get(name)
            if not _display_value_missing(candidate):
                value = _bounded_value(candidate)
                break
        projection[name] = value
    return projection


def _canary_status_projection(status: Mapping[str, Any] | None) -> dict[str, Any]:
    """Normalize the immutable selection status contract for dashboard consumers."""
    source = dict(status) if isinstance(status, Mapping) else {}
    nested = source.get("autonomous")
    nested = nested if isinstance(nested, Mapping) else {}
    # Keep the persisted top-level display payload (including readiness
    projection = dict(source)
    def value(name: str, default: Any = None) -> Any:
        # Report sections can contain sparse aliases.  Keep a non-missing
        # nested/persisted value rather than letting a section-level null erase
        # it during normalization.
        candidate = source.get(name) if name in source else None
        if not _display_value_missing(candidate):
            return candidate
        candidate = nested.get(name)
        return candidate if not _display_value_missing(candidate) else default
    projection.update(
        {
            "readiness_snapshot_status": value(
                "readiness_snapshot_status", "STALE"
            ),
            "readiness_snapshot_stale": value("readiness_snapshot_stale", True),
            "readiness_snapshot_reason": value(
                "readiness_snapshot_reason", "READINESS_SNAPSHOT_MISSING"
            ),
            "readiness_snapshot_updated_at": value(
                "readiness_snapshot_updated_at", None
            ),
        }
    )

    readiness_status = str(
        projection.get("readiness_snapshot_status") or "STALE"
    ).strip().upper()
    readiness_reason = str(
        projection.get("readiness_snapshot_reason") or "READINESS_SNAPSHOT_MISSING"
    ).strip().upper()
    readiness_stale = projection.get("readiness_snapshot_stale", True)
    readiness_current = (
        readiness_status == "CURRENT"
        and readiness_stale in (False, 0, "0", "false", "FALSE")
    )

    raw_version = value("readiness_snapshot_version")
    try:
        projection_version = (
            0 if isinstance(raw_version, bool) else int(raw_version or 0)
        )
    except (TypeError, ValueError):
        projection_version = 0
    has_last_good = _canary_has_last_good(
        projection,
        reason=readiness_reason,
        projection_version=projection_version,
        readiness_status=readiness_status,
        readiness_stale=readiness_stale,
    )

    def count(name: str) -> int | None:
        if not has_last_good:
            return None
        raw = value(name)
        if raw is None or isinstance(raw, bool):
            return None
        try:
            parsed = int(raw)
        except (TypeError, ValueError):
            return None
        return parsed if parsed >= 0 else None
    selection_status = str(
        value("selection_status", "UNKNOWN") or "UNKNOWN"
    ).strip().upper()
    if not has_last_good and selection_status in {"CURRENT", "STALE", "NONE"}:
        selection_status = "UNKNOWN"
    if selection_status not in _CANARY_SELECTION_STATUSES:
        selection_status = "UNKNOWN"
    raw_selection_valid = value("selection_valid")
    if selection_status == "UNKNOWN":
        selection_valid: bool | None = None
    elif (
        raw_selection_valid is True
        and has_last_good
        and readiness_current
        and selection_status == "CURRENT"
    ):
        selection_valid = True
    else:
        selection_valid = False
    if has_last_good and not readiness_current and selection_status == "CURRENT":
        selection_status = "STALE"
        selection_valid = False
    selected_candidate = value("selected_candidate")
    if selected_candidate is not None:
        selected_candidate = str(selected_candidate).strip() or None
    last_selected_candidate = value("last_selected_candidate")
    if last_selected_candidate is not None:
        last_selected_candidate = str(last_selected_candidate).strip() or None
    if not (selection_valid is True and selection_status == "CURRENT"):
        selected_candidate = None
    projection.update(
        {
            "eligibility_raw_count": count("eligibility_raw_count"),
            "eligible_count": count("eligible_count"),
            "rankable_raw_count": count("rankable_raw_count"),
            "rankable_count": count("rankable_count"),
            "ranking_run_id": value("ranking_run_id"),
            "ranking_timestamp": value("ranking_timestamp"),
            "selection_status": selection_status,
            "selection_valid": selection_valid,
            "selection_invalidation_reason": value("selection_invalidation_reason"),
            "selection_reason": value("selection_reason"),
            "selected_candidate": selected_candidate,
            "last_selected_candidate": last_selected_candidate,
            "latest_signal": _signal_projection(value("latest_signal")),
            "next_decision": value("next_decision"),
            "blocker": value("blocker"),
            "signal_scan_reason_counts": _signal_scan_reason_counts(source),
            **_canary_autonomous_projection(source),
        }
    )
    # Research winner fields describe the persisted ranking selection.  Keep
    # them distinct from the actionable scan's selected candidate/rank/score.
    current_selection = selection_valid is True and selection_status == "CURRENT"
    projection["winner_id"] = selected_candidate if current_selection else None
    selected_winner = projection.get("selected_winner")
    for name, aliases in (
        ("winner_rank", ("rank",)),
        ("winner_score", ("score", "total_score")),
    ):
        candidate = value(name)
        if _display_value_missing(candidate):
            for alias in aliases:
                candidate = value(alias)
                if not _display_value_missing(candidate):
                    break
        if _display_value_missing(candidate) and isinstance(selected_winner, Mapping):
            for alias in (name, *aliases):
                candidate = selected_winner.get(alias)
                if not _display_value_missing(candidate):
                    break
        projection[name] = candidate if current_selection else None
    selected_winner = projection.get("selected_winner")
    if isinstance(selected_winner, Mapping):
        selected_winner = dict(selected_winner)
        selected_winner["candidate_id"] = (
            selected_candidate if current_selection else last_selected_candidate
        )
        selected_winner["selection_status"] = selection_status
        selected_winner["selection_valid"] = selection_valid
        selected_winner["selection_invalidation_reason"] = projection[
            "selection_invalidation_reason"
        ]
        selected_winner["selected_candidate"] = selected_candidate
        selected_winner["last_selected_candidate"] = last_selected_candidate
        selected_winner["winner_id"] = projection["winner_id"]
        selected_winner["winner_rank"] = projection.get("winner_rank")
        selected_winner["winner_score"] = projection.get("winner_score")
        projection["selected_winner"] = selected_winner
    autonomous = projection.get("autonomous")
    autonomous = dict(autonomous) if isinstance(autonomous, Mapping) else {}
    autonomous.update({name: projection[name] for name in _CANARY_STATUS_FIELDS})
    autonomous.update(
        {
            name: projection[name]
            for name in _CANARY_READINESS_FIELDS
            if name in projection
        }
    )
    for name in _CANARY_RESEARCH_WINNER_FIELDS:
        autonomous[name] = projection[name]
    autonomous["signal_scan_reason_counts"] = _signal_scan_reason_counts(
        projection,
        autonomous,
        source,
    )
    for name, fallback in (
        ("rank", "winner_rank"),
        ("score", "winner_score"),
        ("next_decision", "next_decision"),
        ("blocker", "blocker"),
    ):
        if _display_value_missing(autonomous.get(name)):
            candidate = projection.get(fallback)
            if _display_value_missing(candidate) and name in source:
                candidate = source.get(name)
            autonomous[name] = candidate
    if not (selection_valid and selection_status == "CURRENT"):
        autonomous["rank"] = None
        autonomous["score"] = None
    projection["autonomous"] = autonomous
    return projection

def _canary_status_report(service: Any) -> tuple[dict[str, Any], dict[str, Any]]:
    """Read one bounded report plus its cheap legacy readiness projection."""
    preserve_keys = (
        *_CANARY_STATUS_FIELDS,
        *_CANARY_READINESS_FIELDS,
        "latest_signal",
        "rank",
        "score",
        "next_decision",
        "blocker",
        "winner_rank",
        "winner_score",
        "autonomous",
    )

    def bounded_mapping(value: Any) -> dict[str, Any]:
        projected = _bounded_value(value)
        result = dict(projected) if isinstance(projected, Mapping) else {}
        if isinstance(value, Mapping):
            for key in preserve_keys:
                if key in value:
                    result[str(key)] = _bounded_value(value[key])
        return result

    legacy: Mapping[str, Any] = {}
    readiness_method = getattr(service, "readiness_snapshot", None)
    if callable(readiness_method):
        try:
            candidate = readiness_method()
            if isinstance(candidate, Mapping):
                legacy = bounded_mapping(candidate)
        except Exception:
            legacy = {}
    report_method = getattr(service, "status_report", None)
    if callable(report_method):
        try:
            report = report_method()
        except Exception:
            report = {}
        if isinstance(report, Mapping):
            projected_report = bounded_mapping(report)
            bounded = dict(projected_report)
            for section_name in (
                "latest_signal",
                "control",
                "authoritative_control",
                "readiness",
                "authoritative_readiness",
                "worker",
                "execution",
            ):
                if section_name in report:
                    bounded[section_name] = bounded_mapping(report[section_name])
            raw_readiness = bounded.get("readiness") or bounded.get(
                "authoritative_readiness", {}
            )
            readiness = _merge_persisted_values(
                legacy,
                raw_readiness if isinstance(raw_readiness, Mapping) else None,
            )
            bounded["readiness"] = readiness
            authoritative_control = bounded.get("control") or bounded.get(
                "authoritative_control", {}
            )
            if isinstance(authoritative_control, Mapping):
                control_state = str(
                    authoritative_control.get("state") or "UNKNOWN"
                ).strip().upper()
                expires_at = authoritative_control.get("expires_at")
                expiry = parse_timestamp(expires_at)
                now = None
                clock = getattr(service, "clock", None)
                if callable(clock):
                    try:
                        now = ensure_utc(clock())
                    except Exception:
                        now = None
                control_expired = (
                    control_state in {"ARMED", "AUTONOMOUS_MICRO_LIVE"}
                    and expiry is not None
                    and now is not None
                    and expiry <= now
                )
                effective_state = "DISARMED" if control_expired else control_state
                if effective_state in {"ARMED", "AUTONOMOUS_MICRO_LIVE"}:
                    display_state = "ENABLED"
                elif effective_state == "KILLED":
                    display_state = "KILLED"
                elif effective_state in {"DISABLED", "DISARMED"}:
                    display_state = "DISABLED"
                else:
                    display_state = "UNKNOWN"
                # Project the copied status report too, so an expired
                # authoritative control cannot remain enabled in a nested
                # dashboard surface.
                for section_name in ("control", "authoritative_control"):
                    section = bounded.get(section_name)
                    if isinstance(section, Mapping):
                        projected_control = dict(section)
                        projected_control.update(
                            {
                                "state": effective_state,
                                "control_state": effective_state,
                                "micro_live_canary": effective_state,
                                "display_state": display_state,
                                "expired": control_expired,
                            }
                        )
                        bounded[section_name] = projected_control
                bounded.update(
                    {
                        "micro_live_canary": effective_state,
                        "control_state": effective_state,
                        "display_state": display_state,
                        "production_live_trading": (
                            "ENABLED" if display_state == "ENABLED" else "DISABLED"
                        ),
                        "expired": control_expired,
                    }
                )
                bounded_autonomous = bounded.get("autonomous")
                if isinstance(bounded_autonomous, Mapping):
                    bounded_autonomous = dict(bounded_autonomous)
                    bounded_autonomous.update(
                        {
                            "enabled": effective_state
                            in {"ARMED", "AUTONOMOUS_MICRO_LIVE"},
                            "control_state": effective_state,
                            "micro_live_canary": effective_state,
                            "display_state": display_state,
                            "expired": control_expired,
                        }
                    )
                    bounded["autonomous"] = bounded_autonomous
                # Control state is authoritative even when the qualification
                # projection is stale and still carries an older control view.
                merged_control = {
                    "micro_live_canary": effective_state,
                    "control_state": effective_state,
                    "display_state": display_state,
                    "production_live_trading": (
                        "ENABLED" if display_state == "ENABLED" else "DISABLED"
                    ),
                    "expired": control_expired,
                    "candidate": authoritative_control.get("candidate"),
                    "venue": authoritative_control.get("venue"),
                    "expiry": expires_at,
                    "control_generation": authoritative_control.get("generation"),
                }
                readiness.update(merged_control)
                readiness_autonomous = readiness.get("autonomous")
                if isinstance(readiness_autonomous, Mapping):
                    readiness_autonomous = dict(readiness_autonomous)
                    readiness_autonomous.update(
                        {
                            "enabled": effective_state
                            in {"ARMED", "AUTONOMOUS_MICRO_LIVE"},
                            "control_state": effective_state,
                            "micro_live_canary": effective_state,
                            "display_state": display_state,
                            "expired": control_expired,
                        }
                    )
                    readiness["autonomous"] = readiness_autonomous
                merged = dict(readiness)
            sections = [
                section
                for section in (
                    bounded.get("control") or bounded.get("authoritative_control") or {},
                    readiness,
                    bounded.get("worker") or {},
                    bounded.get("execution") or {},
                )
                if isinstance(section, Mapping)
            ]
            autonomous_sections = [
                section.get("autonomous")
                for section in sections
                if isinstance(section.get("autonomous"), Mapping)
            ]
            sources = [readiness, bounded]
            sources.extend(sections)
            sources.extend(autonomous_sections)
            merged = dict(readiness)

            def set_alias(name: str, *aliases: str) -> None:
                if name in merged and not _display_value_missing(merged[name]):
                    return
                for source in sources:
                    for alias in aliases:
                        value = source.get(alias)
                        if not _display_value_missing(value):
                            merged[name] = value
                            return

            # Preserve legacy flat fields even when a status report moves them
            # into bounded control/worker/execution sections.
            set_alias("connectivity", "connectivity")
            merged["blocker"] = _canonical_blocker(*sources)
            signal = None
            for source in (bounded, *sections, *autonomous_sections):
                for alias in ("latest_signal", "signal"):
                    candidate = _signal_projection(source.get(alias))
                    if candidate is not None:
                        signal = candidate
                        break
                if signal is not None:
                    break
            merged["latest_signal"] = signal
            set_alias("risk_envelope", "risk_envelope", "risk", "limits")
            set_alias("risk_limits", "risk_limits", "risk_envelope", "limits")
            set_alias("trades", "trades")
            set_alias(
                "execution_event_count",
                "execution_event_count",
                "event_count",
                "real_execution_events",
            )
            set_alias(
                "real_execution_events",
                "real_execution_events",
                "execution_event_count",
                "event_count",
            )
            selected_winner = next(
                (
                    source.get("selected_winner")
                    for source in sources
                    if isinstance(source.get("selected_winner"), Mapping)
                ),
                None,
            )
            if isinstance(selected_winner, Mapping):
                sources.append(selected_winner)
                if "selected_winner" not in merged:
                    merged["selected_winner"] = dict(selected_winner)
            set_alias("selected_candidate", "selected_candidate")
            set_alias("last_selected_candidate", "last_selected_candidate")
            set_alias("winner_id", "winner_id", "candidate_id")
            set_alias("winner_rank", "winner_rank", "rank")
            set_alias("winner_score", "winner_score", "score", "total_score")
            set_alias("selection_reason", "selection_reason")
            return merged, bounded
    return (dict(legacy) if isinstance(legacy, Mapping) else {}, {})


class DashboardData:
    """Dashboard data facade plus the local control-plane projection."""

    def __init__(
        self,
        *,
        data: Mapping[str, Any] | None = None,
        tracker: Any | None = None,
        crypto_provider: Any | None = None,
        prediction_provider: Any | None = None,
        evolution: Any | None = None,
        risk: Any | None = None,
        store: Any | None = None,
        control: OperatorControlPlane | None = None,
        binance_canary: Any | None = None,
        settings_service: CanarySettingsService | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._data = dict(data or {})
        self.clock = clock or (lambda: datetime.now().astimezone())
        self.tracker = tracker
        # Compatibility-only inputs: worker-owned providers are never consulted
        # by dashboard reads, which project persisted storage below.
        self.crypto_provider = crypto_provider
        self.prediction_provider = prediction_provider
        self.evolution = evolution
        self.risk = risk
        self.store = store
        self.control = control
        self.binance_canary = binance_canary
        control_settings = getattr(control, "settings", None) if control is not None else None
        self.settings = (
            settings_service
            if settings_service is not None
            else control_settings
            if isinstance(control_settings, CanarySettingsService)
            else CanarySettingsService(store, initialize=False)
            if store is not None and hasattr(store, "connection")
            else None
        )
        self._canary_service = None
        if self.store is not None and hasattr(self.store, "connection"):
            try:
                self._canary_service = CanaryService(
                    self.store,
                    initialize=False,
                    settings=self.settings,
                    clock=self.clock,
                )
            except Exception:
                self._canary_service = None

    def risk_settings_data(self) -> dict[str, Any]:
        """Return the persisted risk settings snapshot without recomputation."""
        configured = self._configured("risk-settings")
        if configured is not None:
            projected = _bounded_value(configured)
            return dict(projected) if isinstance(projected, Mapping) else {
                "status": "ERROR",
                "error": "RISK_SETTINGS_INVALID",
                "live_execution": False,
            }
        source = self.settings
        if source is None:
            return {
                "status": "UNKNOWN",
                "error": "RISK_SETTINGS_UNAVAILABLE",
                "live_execution": False,
            }
        try:
            snapshot = source.snapshot()
        except Exception as exc:
            return {
                "status": "ERROR",
                "error": type(exc).__name__,
                "detail": str(exc)[:160],
                "live_execution": False,
            }
        projected = _bounded_value(snapshot)
        if not isinstance(projected, Mapping):
            return {
                "status": "ERROR",
                "error": "RISK_SETTINGS_INVALID",
                "live_execution": False,
            }
        result = dict(projected)
        if result.get("active") is not None or result.get("effective_limits") is not None:
            result["status"] = "CURRENT"
            result["settings_available"] = True
        result.setdefault("live_execution", False)
        return result
    def _configured(self, name: str) -> Any:
        if name in self._data:
            return self._data[name]
        return None

    def research(self) -> Any:
        configured = self._configured("research")
        if configured is not None:
            return configured
        if self.tracker is not None:
            if hasattr(self.tracker, "as_records"):
                return {"experiments": self.tracker.as_records(), "reports": []}
            records = getattr(self.tracker, "records", ())
            return {"experiments": [_jsonable(item) for item in records], "reports": []}
        if self.store is not None:
            summary = research_summary(self.store, limit=20)
            experiments = []
            for item in self.store.list_experiments(limit=100):
                experiment = item.get("experiment", {})
                experiment = experiment if isinstance(experiment, Mapping) else {}
                experiments.append(
                    {
                        "experiment_id": item.get("experiment_id"),
                        "strategy_id": item.get("strategy_id"),
                        "created_at": item.get("created_at"),
                        "status": experiment.get("status"),
                        "rejected": bool(experiment.get("rejected", False)),
                    }
                )
            return {
                "experiments": experiments,
                "reports": summary.get("reports", []),
                "candidates": summary.get("candidates", []),
                "autonomous": summary.get("autonomous", {}),
                "hermes": summary.get("hermes", {}),
                "live_execution": False,
            }
        return {"experiments": [], "reports": [], "live_execution": False}

    def crypto(self) -> Any:
        configured = self._configured("crypto")
        if configured is not None:
            return configured
        summary = self.store.dashboard_summary() if self.store is not None else {}
        return {
            "available": bool(summary.get("bars", 0)),
            "provider": "persisted",
            "symbols": [],
            "bars": summary.get("bars", 0),
            "datasets": summary.get("datasets", 0),
            "live_execution": False,
        }

    def prediction(self) -> Any:
        configured = self._configured("prediction")
        if configured is not None:
            return configured
        markets: list[dict[str, Any]] = []
        if self.store is not None:
            try:
                tracked = self.store.tracked_polymarket_markets(active_only=True, include_payload=True)
                active_ids = {
                    str(item.get("market_id"))
                    for item in tracked
                    if isinstance(item, Mapping) and item.get("market_id")
                }
                snapshots = self.store.load_latest_polymarket_snapshots(active_ids, limit=1000)
            except AttributeError:
                snapshots = self.store.load_polymarket_snapshots(limit=1000, latest=True)
                active_ids = {
                    str(item.get("market_id"))
                    for item in snapshots
                    if isinstance(item, Mapping) and item.get("market_id")
                }
            latest_by_market: dict[str, Mapping[str, Any]] = {}
            for item in snapshots:
                market_id = str(item.get("market_id", "")).strip()
                if not market_id or market_id not in active_ids:
                    continue
                payload = item.get("payload", {})
                if isinstance(payload, Mapping) and str(payload.get("source_type", "")).upper() == "HISTORICAL":
                    continue
                latest_by_market.setdefault(market_id, item)
            for item in latest_by_market.values():
                payload = item.get("payload", {})
                if not isinstance(payload, Mapping):
                    payload = {}
                snapshot = payload.get("snapshot")
                record = dict(snapshot) if isinstance(snapshot, Mapping) else dict(payload)
                record.setdefault("market_id", item.get("market_id"))
                record["research_quality"] = item.get("quality", payload.get("research_quality"))
                record["source_type"] = payload.get("source_type", "FORWARD_COLLECTED")
                markets.append(record)
        return {
            "available": bool(markets),
            "provider": "persisted",
            "markets": markets,
            "source_type": "FORWARD_COLLECTED",
            "live_execution": False,
        }

    def evolution_data(self) -> Any:
        configured = self._configured("evolution")
        if configured is not None:
            return configured
        source = self.evolution
        if source is None and self.store is not None:
            candidates = self.store.load_candidate_lifecycle(limit=100)
            return {"available": bool(candidates), "candidates": candidates}
        if source is None:
            return {"available": False, "candidates": []}
        if callable(source):
            return source()
        snapshot = getattr(source, "snapshot", None)
        if callable(snapshot):
            return snapshot()
        population = getattr(source, "population", None)
        if population is not None and hasattr(source, "generation"):
            return {
                "available": True,
                "generation": int(source.generation),
                "population": [_candidate_record(candidate) for candidate in population],
            }
        return source

    def risk_data(self) -> Any:
        configured = self._configured("risk")
        if configured is not None:
            return configured
        source = self.risk
        if source is None and self.settings is not None:
            return self.risk_settings_data()
        if source is None:
            return {"available": False, "live_execution": False}
        if callable(source):
            return source()
        snapshot = getattr(source, "snapshot", None)
        if callable(snapshot):
            return snapshot()
        status = getattr(source, "status", None)
        if callable(status):
            return status()
        return source

    def dataset_health(self) -> Any:
        configured = self._configured("dataset-health")
        if configured is not None:
            return configured
        if self.store is None:
            return {
                "grade": "F",
                "grade_scope": "collector_health",
                "reason_code": "NO_FORWARD_SNAPSHOTS",
                "reasons": [{"code": "NO_FORWARD_SNAPSHOTS", "reason": "No FORWARD_COLLECTED snapshot is available."}],
                "window_start": None,
                "window_end": None,
                "source_type": "FORWARD_COLLECTED",
                "historical_maturity_grade": "F",
                "historical_error_count": 0,
                "markets": 0,
                "snapshots": 0,
                "trades": 0,
                "collection_errors": 0,
                "stale_markets": [],
                "gaps": [],
                "live_execution": False,
            }
        health = getattr(self.store, "polymarket_health", None)
        if not callable(health):
            return {"grade": "F", "reason_code": "HEALTH_UNAVAILABLE", "error": "store has no polymarket health method", "live_execution": False}
        return health()
    @staticmethod
    def _normalize_page_response(value: Any, params: Mapping[str, Any]) -> dict[str, Any]:
        if not isinstance(value, Mapping):
            return _page_result([], page=int(params["page"]), page_size=int(params["page_size"]))
        return _page_result(
            value.get("items", []),
            page=int(value.get("page", params["page"])),
            page_size=int(value.get("page_size", params["page_size"])),
            total=int(value.get("total", 0)),
        )

    def _store_page(self, method_name: str, params: Mapping[str, Any], **kwargs: Any) -> dict[str, Any]:
        """Call a storage paginator, retaining a strict bounded compatibility path."""
        page = int(params["page"])
        page_size = int(params["page_size"])
        method = getattr(self.store, method_name, None) if self.store is not None else None
        if callable(method):
            try:
                return self._normalize_page_response(
                    method(page=page, page_size=page_size, sort=params.get("sort") or None, direction=params.get("direction", "desc"), **kwargs),
                    params,
                )
            except TypeError:
                try:
                    return self._normalize_page_response(method(page=page, page_size=page_size, **kwargs), params)
                except (AttributeError, TypeError, ValueError):
                    pass
            except (AttributeError, ValueError):
                pass
        return _page_result([], page=page, page_size=page_size)

    def paginate_dataset_catalog(self, params: Mapping[str, Any] | None = None) -> dict[str, Any]:
        values = _pagination_params(params)
        values["sort"] = values.get("sort") or "updated_at"
        result = self._store_page(
            "paginate_dataset_catalog",
            values,
            source_type=values.get("source_type"),
            market_type=values.get("market"),
            timeframe=values.get("timeframe"),
            quality=values.get("quality"),
            filter=values.get("filter"),
        )
        if result["items"] or callable(getattr(self.store, "paginate_dataset_catalog", None)):
            return result
        if self.store is None or not callable(getattr(self.store, "list_dataset_catalog", None)):
            configured = self._configured("datasets")
            if isinstance(configured, Mapping):
                records = list(configured.get("historical", [])) + list(configured.get("forward", []))
                needle = str(values.get("filter") or "").lower()
                if values.get("source_type"):
                    records = [item for item in records if str(item.get("source_type", "")).upper() == str(values["source_type"]).upper()]
                if needle:
                    records = [item for item in records if needle in json.dumps(_jsonable(item), sort_keys=True).lower()]
                offset = (values["page"] - 1) * values["page_size"]
                return _page_result(records[offset : offset + values["page_size"]], page=values["page"], page_size=values["page_size"], total=len(records))
            return result
        limit = min(_MAX_SIZE_FALLBACK, values["page"] * values["page_size"])
        try:
            records = self.store.list_dataset_catalog(
                source_type=values.get("source_type"),
                market_type=values.get("market"),
                limit=limit,
            )
        except TypeError:
            records = self.store.list_dataset_catalog(limit=limit)
        needle = str(values.get("filter") or "").lower()
        if needle:
            records = [item for item in records if needle in json.dumps(_jsonable(item), sort_keys=True).lower()]
        if values.get("timeframe"):
            records = [item for item in records if str(item.get("timeframe", "")) == str(values["timeframe"])]
        if values.get("quality"):
            records = [item for item in records if str(item.get("quality", "")) == str(values["quality"])]
        offset = (values["page"] - 1) * values["page_size"]
        return _page_result(records[offset : offset + values["page_size"]], page=values["page"], page_size=values["page_size"], total=len(records))

    def paginate_candidate_lifecycle(self, params: Mapping[str, Any] | None = None) -> dict[str, Any]:
        values = _pagination_params(params)
        values["sort"] = values.get("sort") or "updated_at"
        result = self._store_page(
            "paginate_candidate_lifecycle",
            values,
            stage=values.get("stage"),
            quality=values.get("quality"),
            market=values.get("market"),
            source_type=values.get("source_type"),
            filter=values.get("filter"),
        )
        if result["items"] or callable(getattr(self.store, "paginate_candidate_lifecycle", None)):
            result["items"] = [
                {**dict(item), **self._candidate_row(item)}
                for item in result.get("items", [])
                if isinstance(item, Mapping)
            ]
            return result
        if self.store is None or not callable(getattr(self.store, "load_candidate_lifecycle", None)):
            return result
        limit = min(_MAX_PAGE_SIZE * _MAX_PAGE_SIZE, values["page"] * values["page_size"])
        records = self.store.load_candidate_lifecycle(limit=limit)
        records = records if isinstance(records, list) else []
        needle = str(values.get("filter") or "").lower()
        if values.get("stage"):
            records = [item for item in records if str(item.get("stage", "")) == str(values["stage"])]
        if needle:
            records = [item for item in records if needle in json.dumps(_jsonable(item), sort_keys=True).lower()]
        rows = [self._candidate_row(item) for item in records]
        offset = (values["page"] - 1) * values["page_size"]
        return _page_result(rows[offset : offset + values["page_size"]], page=values["page"], page_size=values["page_size"], total=len(rows))

    def paginate_research_activity(self, params: Mapping[str, Any] | None = None) -> dict[str, Any]:
        values = _pagination_params(params)
        values["sort"] = values.get("sort") or "created_at"
        result = self._store_page(
            "paginate_research_activity",
            values,
            source=values.get("source_type"),
            source_type=values.get("source_type"),
            kind=values.get("kind"),
            status=values.get("status"),
            market=values.get("market"),
            filter=values.get("filter"),
        )
        if result["items"] or callable(getattr(self.store, "paginate_research_activity", None)):
            return result
        items = self._activity_feed(limit=min(_MAX_SIZE_FALLBACK, values["page"] * values["page_size"]))
        needle = str(values.get("filter") or "").lower()
        if needle:
            items = [item for item in items if needle in json.dumps(_jsonable(item), sort_keys=True).lower()]
        offset = (values["page"] - 1) * values["page_size"]
        return _page_result(items[offset : offset + values["page_size"]], page=values["page"], page_size=values["page_size"], total=len(items))

    def _enrich_polymarket_rows(self, result: dict[str, Any]) -> dict[str, Any]:
        evidence = self._required_forward_evidence()
        diagnostics = evidence.get("market_diagnostics", [])
        by_market = {
            str(item.get("market_id")): item
            for item in diagnostics
            if isinstance(item, Mapping) and item.get("market_id")
        }
        required_markets = {
            str(value).strip()
            for value in evidence.get("candidate_bound_markets", ())
            if str(value).strip()
        }
        references = evidence.get("candidate_references", {})
        references = references if isinstance(references, Mapping) else {}
        for item in result.get("items", []):
            if not isinstance(item, Mapping):
                continue
            market_id = str(item.get("market_id") or "").strip()
            diagnostic = by_market.get(market_id)
            diagnostic_bound = (
                diagnostic is not None
                and diagnostic.get("candidate_bound") is True
            )
            candidate_bound = market_id in required_markets or diagnostic_bound
            item["candidate_bound"] = candidate_bound
            item["candidate_bound_priority"] = candidate_bound
            item["required_priority"] = "CANDIDATE_BOUND" if candidate_bound else None
            item["forward_required"] = candidate_bound
            item["candidate_references"] = (
                list(diagnostic.get("candidate_references", ()))[:32]
                if diagnostic is not None
                else list(references.get(market_id, ()))[:32]
                if isinstance(references.get(market_id), (list, tuple, set, frozenset))
                else []
            )
            # Health diagnostics are only authoritative when persisted by a
            # worker/evaluation.  Never recompute them from provider state.
            if candidate_bound:
                if diagnostic is None:
                    diagnostic = {
                        "source_timestamp": item.get("source_timestamp"),
                        "observed_at": item.get("observed_at"),
                        "freshness_age_seconds": None,
                        "collection_state": "unknown",
                        "reason_code": "NO_PERSISTED_MARKET_HEALTH",
                        "reason_display": "NO_PERSISTED_MARKET_HEALTH",
                    }
                else:
                    diagnostic = dict(diagnostic)
                    diagnostic.setdefault("reason_display", diagnostic.get("reason_code"))
                for key in (
                    "source_timestamp",
                    "observed_at",
                    "freshness_age_seconds",
                    "collection_state",
                    "reason_code",
                    "reason_display",
                ):
                    item[key] = diagnostic.get(key)
            quality_label, quality_context = _polymarket_quality_display(item)
            item["quality_label"] = quality_label
            item["quality_context"] = quality_context
        result["forward_evidence"] = evidence
        return result


    def paginate_polymarket_markets(self, params: Mapping[str, Any] | None = None) -> dict[str, Any]:
        values = _pagination_params(params)
        values["sort"] = values.get("sort") or "observed_at"
        result = self._store_page(
            "paginate_polymarket_markets",
            values,
            market=values.get("market"),
            timeframe=values.get("timeframe"),
            quality=values.get("quality"),
            category=values.get("category"),
            settlement=values.get("settlement"),
            filter=values.get("filter"),
            include_snapshots=True,
        )
        if result["items"] or callable(getattr(self.store, "paginate_polymarket_markets", None)):
            for item in result.get("items", []):
                if not isinstance(item, Mapping):
                    continue
                snapshot = item.get("snapshot")
                if not isinstance(snapshot, Mapping):
                    payload = item.get("payload")
                    snapshot = payload.get("snapshot") if isinstance(payload, Mapping) else {}
                if isinstance(snapshot, Mapping):
                    for key in ("question", "yes_mid", "liquidity", "category", "settlement", "timeframe"):
                        if key not in item and key in snapshot:
                            item[key] = snapshot[key]
            return self._enrich_polymarket_rows(result)
        data = self.prediction()
        items = data.get("markets", []) if isinstance(data, Mapping) else []
        items = items if isinstance(items, list) else []
        needle = str(values.get("filter") or "").lower()
        if values.get("category"):
            items = [item for item in items if str(item.get("category", "")) == str(values["category"])]
        if values.get("settlement"):
            items = [item for item in items if str(item.get("settlement", "")) == str(values["settlement"])]
        if needle:
            items = [item for item in items if needle in json.dumps(_jsonable(item), sort_keys=True).lower()]
        for item in items:
            if isinstance(item, Mapping):
                label, context = _polymarket_quality_display(item)
                item["quality_label"] = label
                item["quality_context"] = context
        offset = (values["page"] - 1) * values["page_size"]
        return self._enrich_polymarket_rows(
            _page_result(
                items[offset : offset + values["page_size"]],
                page=values["page"],
                page_size=values["page_size"],
                total=len(items),
            )
        )

    def paginate_research_queue(self, params: Mapping[str, Any] | None = None) -> dict[str, Any]:
        values = _pagination_params(params)
        requested_sort = values.get("sort") or "priority"
        # The storage paginator sorts on persisted columns.  Derived display
        # columns use their closest stable provenance key, then expose their
        # real values in each bounded row.
        values["sort"] = {
            "time": "updated_at",
            "dataset_id": "item_id",
            "dataset_version": "item_id",
            "version": "item_id",
            "family": "item_type",
            "reason_code": "status",
            "human_reason": "updated_at",
        }.get(str(requested_sort).lower(), requested_sort)
        result = self._store_page(
            "paginate_research_queue",
            values,
            status=values.get("status"),
            source=values.get("source_type"),
            item_type=values.get("category"),
            item_id=values.get("item_id"),
            filter=values.get("filter"),
        )
        if result["items"] or callable(getattr(self.store, "paginate_research_queue", None)):
            result["items"] = [
                _hermes_row(item)
                for item in result.get("items", [])
                if isinstance(item, Mapping)
            ]
            return result
        if self.store is None or not callable(getattr(self.store, "list_research_items", None)):
            configured = self._configured("hermes")
            if isinstance(configured, Mapping):
                records = configured.get("items", configured.get("queue", []))
                records = [item for item in records if isinstance(item, Mapping)] if isinstance(records, list) else []
                needle = str(values.get("filter") or "").lower()
                if values.get("status"):
                    records = [item for item in records if str(item.get("status", "")).upper() == str(values["status"]).upper()]
                if values.get("item_id"):
                    records = [item for item in records if str(item.get("item_id")) == str(values["item_id"])]
                if needle:
                    records = [item for item in records if needle in json.dumps(_jsonable(item), sort_keys=True).lower()]
                records = [_hermes_row(item) for item in records]
                records.sort(key=lambda item: str(item.get("item_id", "")))
                if values.get("direction") == "desc":
                    records.reverse()
                start = (values["page"] - 1) * values["page_size"]
                return _page_result(records[start : start + values["page_size"]], page=values["page"], page_size=values["page_size"], total=len(records))
            return result
        limit = min(_MAX_SIZE_FALLBACK, values["page"] * values["page_size"])
        records = self.store.list_research_items(status=values.get("status"), limit=limit)
        needle = str(values.get("filter") or "").lower()
        if needle:
            records = [item for item in records if needle in json.dumps(_jsonable(item), sort_keys=True).lower()]
        rows = [_hermes_row(item) for item in records if isinstance(item, Mapping)]
        offset = (values["page"] - 1) * values["page_size"]
        return _page_result(rows[offset : offset + values["page_size"]], page=values["page"], page_size=values["page_size"], total=len(rows))
    def hermes_detail(self, item_id: str) -> dict[str, Any]:
        """Return one queue item with human-readable, bounded evidence."""
        identifier = str(item_id).strip()
        item: Mapping[str, Any] | None = None
        if self.store is not None and callable(getattr(self.store, "get_research_item", None)):
            item = self.store.get_research_item(identifier)
        if item is None:
            page = self.paginate_research_queue(
                {"page": 1, "page_size": 10, "item_id": identifier, "sort": "item_id", "direction": "asc"}
            )
            candidates = page.get("items", []) if isinstance(page, Mapping) else []
            item = candidates[0] if candidates and isinstance(candidates[0], Mapping) else None
        if item is None:
            return {
                "available": False,
                "item_id": identifier,
                "error": "Hermes item not found",
                "live_execution": False,
            }
        row = _hermes_row(item)
        payload = item.get("payload")
        payload = payload if isinstance(payload, Mapping) else {}
        result = item.get("result")
        result = result if isinstance(result, Mapping) else {}
        events: list[Any] = []
        if self.store is not None and callable(getattr(self.store, "list_research_queue_events", None)):
            events = self.store.list_research_queue_events(identifier, limit=64)
        statement = _nested_value(result, payload, keys=("statement", "hypothesis", "thesis"))
        tests = _nested_value(result, payload, keys=("tests", "validation_plan", "test_plan"))
        plan = _nested_value(result, payload, keys=("plan", "experiment_plan", "plan_id"))
        plan_payload = plan if isinstance(plan, Mapping) else {}
        dataset_selector = plan_payload.get("dataset_selector")
        dataset_selector = dataset_selector if isinstance(dataset_selector, Mapping) else {}
        dataset_id = row.get("dataset_id") or dataset_selector.get("dataset_id")
        dataset_version = row.get("dataset_version") or dataset_selector.get("dataset_version")
        family = row.get("family") or payload.get("experiment_family") or payload.get("family") or plan_payload.get("experiment_family")
        return {
            "available": True,
            "item_id": identifier,
            "item": row,
            # Keep the common page shape for clients that used the old detail
            # endpoint, while making all evidence available by named fields.
            "items": [row],
            "page": 1,
            "page_size": 1,
            "total": 1,
            "pages": 1,
            "time": row.get("time"),
            "source": row.get("source"),
            "status": row.get("status"),
            "dataset_id": dataset_id,
            "dataset_version": dataset_version,
            "family": family,
            "attempts": row.get("attempts", 0),
            "reason_code": row.get("reason_code"),
            "outcome_type": row.get("outcome_type"),
            "outcome_label": row.get("outcome_label"),
            "outcome": (
                {
                    "type": row.get("outcome_type"),
                    "label": row.get("outcome_label"),
                    "reason_code": row.get("reason_code"),
                    "dataset_id": dataset_id,
                    "dataset_version": dataset_version,
                }
                if row.get("outcome_type")
                else None
            ),
            "last_error": row.get("last_error"),
            "human_reason": row.get("human_reason"),
            "statement": _bounded_value(statement),
            "tests": _bounded_value(tests),
            "plan": _bounded_value(plan),
            "lifecycle_events": [_bounded_value(event) for event in events[:64]],
            "final_result": _bounded_value(result) if result else None,
            "rejection": (
                {
                    "reason_code": row.get("reason_code"),
                    "reason": row.get("human_reason"),
                    "exact": row.get("human_reason") or row.get("last_error"),
                }
                if str(row.get("status", "")).upper() in {"REJECTED", "FAILED"}
                else None
            ),
            "rejection_reason": row.get("human_reason") if str(row.get("status", "")).upper() in {"REJECTED", "FAILED"} else None,
            "paper_only": True,
            "live_execution": False,
        }

    @staticmethod
    def _crypto_catalog_row(item: Mapping[str, Any]) -> dict[str, Any]:
        metadata = item.get("metadata")
        metadata = metadata if isinstance(metadata, Mapping) else {}
        symbol = str(item.get("instrument") or item.get("symbol") or "").strip()
        symbols = _nested_value(item, metadata, keys=("symbols", "symbol"))
        if isinstance(symbols, str):
            symbols = [symbols]
        elif not isinstance(symbols, (list, tuple, set, frozenset)):
            symbols = [symbol] if symbol else []
        assets = _nested_value(item, metadata, keys=("assets", "asset"))
        if isinstance(assets, str):
            assets = [assets]
        elif not isinstance(assets, (list, tuple, set, frozenset)):
            assets = list(symbols)
        universe_version = _nested_value(
            item,
            metadata,
            keys=("universe_version", "universeVersion", "catalog_version"),
        )
        coverage = _nested_value(item, metadata, keys=("coverage", "coverage_summary"))
        if not isinstance(coverage, Mapping):
            coverage = {
                "start": item.get("start_timestamp"),
                "end": item.get("end_timestamp"),
                "rows": item.get("row_count", 0),
                "completeness": item.get("completeness", 0.0),
                "missing_ranges": item.get("missing_ranges", []),
            }
        return {
            **{str(key): _bounded_value(value) for key, value in item.items() if key != "metadata"},
            "dataset_id": item.get("dataset_id"),
            "dataset_version": item.get("dataset_version"),
            "symbol": symbol,
            "symbols": [_bounded_value(value) for value in list(symbols)[:32]],
            "assets": [_bounded_value(value) for value in list(assets)[:32]],
            "universe_version": universe_version,
            "coverage": _bounded_value(coverage),
            "strategies": _bounded_value(_nested_value(item, metadata, keys=("strategies", "strategy")) or []),
            "experiments": _bounded_value(_nested_value(item, metadata, keys=("experiments", "experiment")) or []),
            "validation": _bounded_value(_nested_value(item, metadata, keys=("validation", "validation_summary")) or {}),
            "families": _bounded_value(_nested_value(item, metadata, keys=("families", "family")) or []),
            "metadata": _bounded_value(metadata),
        }

    @staticmethod
    def _crypto_bootstrap_report_row(item: Mapping[str, Any]) -> dict[str, Any] | None:
        report = item.get("report")
        report = report if isinstance(report, Mapping) else {}
        report_id = str(item.get("report_id") or "").lower()
        experiment_id = str(item.get("experiment_id") or "").lower()
        is_bootstrap = (
            report_id.startswith(("historical-bootstrap:", "bootstrap:"))
            or experiment_id.startswith("crypto-universe:")
            or str(report.get("kind") or "").lower() in {"historical_bootstrap", "bootstrap"}
        )
        if not is_bootstrap:
            return None
        return {
            "report_id": item.get("report_id"),
            "experiment_id": item.get("experiment_id"),
            "created_at": item.get("created_at"),
            "kind": report.get("kind", report.get("report_type", "bootstrap")),
            "report_type": report.get("report_type", report.get("kind", "bootstrap")),
            "universe_version": _nested_value(report, keys=("universe_version", "catalog_version")),
            "assets": _bounded_value(report.get("assets", [])),
            "symbols": _bounded_value(report.get("symbols", report.get("instruments", []))),
            "coverage": _bounded_value(report.get("coverage", report.get("coverage_summary", {}))),
            "validation": _bounded_value(report.get("validation", report.get("validation_summary", {}))),
            "report": _bounded_value(report),
        }

    @staticmethod
    def _crypto_report_row(item: Mapping[str, Any]) -> dict[str, Any] | None:
        report = item.get("report")
        report = report if isinstance(report, Mapping) else {}
        if DashboardData._crypto_bootstrap_report_row(item) is not None:
            return None
        experiment_id = str(item.get("experiment_id") or "").lower()
        kind = str(report.get("kind") or report.get("report_type") or "").lower()
        is_strategy = bool(
            experiment_id.startswith(("crypto-research:", "crypto-paper:", "btcusdt"))
            or kind in {"btc_historical_walk_forward", "crypto_strategy_research", "crypto_walk_forward"}
            or any(key in report for key in ("strategies", "experiments", "validation", "families", "strategy"))
        )
        if not is_strategy:
            return None
        return {
            "report_id": item.get("report_id"),
            "experiment_id": item.get("experiment_id"),
            "created_at": item.get("created_at"),
            "kind": report.get("kind", report.get("report_type")),
            "report_type": report.get("report_type", report.get("kind")),
            "universe_version": _nested_value(report, keys=("universe_version", "catalog_version")),
            "assets": _bounded_value(report.get("assets", [])),
            "symbols": _bounded_value(report.get("symbols", report.get("instruments", []))),
            "coverage": _bounded_value(report.get("coverage", report.get("coverage_summary", {}))),
            "strategies": _bounded_value(report.get("strategies", report.get("strategy", []))),
            "experiments": _bounded_value(report.get("experiments", report.get("experiment", []))),
            "validation": _bounded_value(report.get("validation", report.get("validation_summary", {}))),
            "families": _bounded_value(report.get("families", report.get("family", []))),
            "report": _bounded_value(report),
        }

    def paginate_crypto_research(self, params: Mapping[str, Any] | None = None) -> dict[str, Any]:
        """Page every persisted crypto catalog, with compact report context."""
        values = _pagination_params(params)
        values["sort"] = values.get("sort") or "updated_at"
        requested_symbol = str(values.get("symbol") or "").strip()
        configured = self._configured("crypto-research")
        if isinstance(configured, Mapping) and self.store is None:
            raw_catalogs = configured.get("catalogs", configured.get("catalog", []))
            if not raw_catalogs:
                raw_catalogs = list(configured.get("historical", [])) + list(configured.get("forward", []))
            records = [
                item for item in raw_catalogs
                if isinstance(item, Mapping)
                and str(item.get("market_type", "crypto_spot")).lower() == "crypto_spot"
            ]
            if requested_symbol:
                needle = requested_symbol.replace("/", "").replace("-", "").upper()
                records = [
                    item for item in records
                    if needle in str(item.get("instrument", item.get("symbol", ""))).replace("/", "").replace("-", "").upper()
                ]
            if values.get("filter"):
                needle = str(values["filter"]).lower()
                records = [item for item in records if needle in json.dumps(_jsonable(item), sort_keys=True).lower()]
            records = [self._crypto_catalog_row(item) for item in records]
            reverse = values["direction"] == "desc"
            records.sort(key=lambda item: str(item.get(values["sort"], item.get("updated_at", ""))), reverse=reverse)
            total = len(records)
            start = (values["page"] - 1) * values["page_size"]
            page_rows = records[start : start + values["page_size"]]
            reports_raw = configured.get("reports", [])
            reports: list[dict[str, Any]] = []
            bootstrap_reports: list[dict[str, Any]] = []
            for item in reports_raw if isinstance(reports_raw, list) else []:
                if not isinstance(item, Mapping):
                    continue
                report_row = self._crypto_report_row(item)
                if report_row is not None and len(reports) < 20:
                    reports.append(report_row)
                bootstrap_row = self._crypto_bootstrap_report_row(item)
                if bootstrap_row is not None and len(bootstrap_reports) < 20:
                    bootstrap_reports.append(bootstrap_row)
            return self._crypto_research_result(page_rows, reports, values, total=total, bootstrap_reports=bootstrap_reports)
        result = self._store_page(
            "paginate_dataset_catalog",
            values,
            source_type=values.get("source_type"),
            market_type="crypto_spot",
            instrument=requested_symbol or None,
            filter=values.get("filter"),
        )
        rows = [
            self._crypto_catalog_row(item)
            for item in result.get("items", [])
            if isinstance(item, Mapping)
        ]
        reports: list[dict[str, Any]] = []
        bootstrap_reports: list[dict[str, Any]] = []
        if self.store is not None and callable(getattr(self.store, "list_reports", None)):
            for item in self.store.list_reports(limit=256, newest_first=True):
                if not isinstance(item, Mapping):
                    continue
                report_row = self._crypto_report_row(item)
                if report_row is not None and len(reports) < 20:
                    reports.append(report_row)
                bootstrap_row = self._crypto_bootstrap_report_row(item)
                if bootstrap_row is not None and len(bootstrap_reports) < 20:
                    bootstrap_reports.append(bootstrap_row)
        return self._crypto_research_result(
            rows,
            reports,
            values,
            total=int(result.get("total", len(rows))),
            bootstrap_reports=bootstrap_reports,
        )

    def _crypto_research_result(
        self,
        rows: list[dict[str, Any]],
        reports: list[dict[str, Any]],
        values: Mapping[str, Any],
        *,
        total: int,
        bootstrap_reports: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        bootstrap_reports = bootstrap_reports or []
        symbols = sorted({str(symbol) for row in rows for symbol in (row.get("symbols") or []) if str(symbol).strip()})
        assets = sorted({str(asset) for row in rows for asset in (row.get("assets") or []) if str(asset).strip()})
        universe_versions = [row.get("universe_version") for row in rows if row.get("universe_version")]
        universe_versions.extend(report.get("universe_version") for report in reports + bootstrap_reports if report.get("universe_version"))
        def collect(key: str) -> list[Any]:
            values_out: list[Any] = []
            for row in rows + reports:
                value = row.get(key)
                if isinstance(value, (list, tuple, set, frozenset)):
                    values_out.extend(value)
                elif value not in (None, "", {}):
                    values_out.append(value)
            unique: list[Any] = []
            seen: set[str] = set()
            for value in values_out:
                marker = json.dumps(_jsonable(value), sort_keys=True, default=str)
                if marker not in seen:
                    seen.add(marker)
                    unique.append(_bounded_value(value))
            return unique[:64]
        bootstrap_progress = self._bootstrap_progress_rows()
        bootstrap_universe = self._bootstrap_universe_summary()
        return {
            **_page_result(rows, page=int(values["page"]), page_size=int(values["page_size"]), total=total),
            "available": bool(total or reports or bootstrap_reports),
            "catalogs": rows,
            "catalog": rows,
            "reports": reports,
            "strategy_reports": reports,
            "bootstrap_reports": bootstrap_reports,
            "bootstrap_report_count": len(bootstrap_reports),
            "bootstrap_progress": bootstrap_progress,
            "bootstrap_states": bootstrap_progress,
            "bootstrap_universe": bootstrap_universe,
            "universe_versions": list(dict.fromkeys(str(value) for value in universe_versions))[:32],
            "assets": assets[:64],
            "asset_count": len(assets),
            "symbols": symbols[:64],
            "symbol_count": len(symbols),
            "coverage": collect("coverage"),
            "strategies": collect("strategies"),
            "experiments": collect("experiments"),
            "validation": collect("validation"),
            "families": collect("families"),
            "paper_only": True,
            "live_execution": False,
        }

    def crypto_research_detail(self, symbol: str) -> dict[str, Any]:
        result = self.paginate_crypto_research(
            {"page": 1, "page_size": _MAX_PAGE_SIZE, "symbol": unquote(str(symbol)), "sort": "dataset_id", "direction": "asc"}
        )
        result["selected_symbol"] = unquote(str(symbol))
        return result


    def _paper_view_result(self, result: dict[str, Any]) -> dict[str, Any]:
        counts_method = getattr(self.store, "paper_record_counts", None) if self.store is not None else None
        counts = (
            counts_method()
            if callable(counts_method)
            else self.store.dashboard_summary()
            if self.store is not None
            else {}
        )
        candidate = self._paper_portfolio(candidate_only=True)
        result = dict(result)
        result["paper_telemetry"] = {
            "observation_records": int(counts.get("paper_observations", 0) or 0),
            "execution_events": int(counts.get("paper_execution_events", 0) or 0),
            "resolved_bets": int(counts.get("paper_bet_ledger", 0) or 0),
            "record_count": sum(
                int(counts.get(key, 0) or 0)
                for key in ("paper_observations", "paper_execution_events", "paper_bet_ledger")
            ),
        }
        result["candidate_portfolios"] = candidate.get("states", [])
        result["candidate_portfolio_summary"] = {
            key: candidate.get(key, 0)
            for key in ("total_equity", "total_pnl", "resolved_bets", "win_rate", "expectancy")
        }
        result["paper_only"] = True
        result["live_execution"] = False
        return result

    def paginate_paper_records(self, params: Mapping[str, Any] | None = None) -> dict[str, Any]:
        values = _pagination_params(params)
        values["sort"] = values.get("sort") or "timestamp"
        result = self._store_page(
            "paginate_paper_records",
            values,
            market=values.get("market"),
            status=values.get("status"),
            filter=values.get("filter"),
        )
        if result["items"] or callable(getattr(self.store, "paginate_paper_records", None)):
            return self._paper_view_result(result)
        paper = self._paper_portfolio()
        items = paper.get("states", []) if isinstance(paper, Mapping) else []
        items = items if isinstance(items, list) else []
        needle = str(values.get("filter") or "").lower()
        if needle:
            items = [item for item in items if needle in json.dumps(_jsonable(item), sort_keys=True).lower()]
        offset = (values["page"] - 1) * values["page_size"]
        return self._paper_view_result(
            _page_result(items[offset : offset + values["page_size"]], page=values["page"], page_size=values["page_size"], total=len(items))
        )

    def dataset_detail(self, dataset_id: str) -> dict[str, Any]:
        identifier = str(dataset_id)
        record = None
        if self.store is not None and callable(getattr(self.store, "load_dataset_catalog", None)):
            record = self.store.load_dataset_catalog(identifier)
        if record is None:
            catalogs = self._configured("datasets")
            if isinstance(catalogs, Mapping):
                for item in catalogs.get("historical", []) + catalogs.get("forward", []):
                    if isinstance(item, Mapping) and str(item.get("dataset_id")) == identifier:
                        record = item
                        break
        if record is None:
            return {"available": False, "dataset_id": identifier, "error": "dataset not found", "live_execution": False}
        result: dict[str, Any] = {"available": True, "dataset_id": identifier, "dataset_version": record.get("dataset_version"), "catalog": record, "live_execution": False}
        if self.store is not None and callable(getattr(self.store, "data_health", None)):
            try:
                result["health"] = self.store.data_health(identifier)
            except (AttributeError, TypeError, ValueError):
                pass
        return result

    @staticmethod
    def _binance_testnet_status(status: Mapping[str, Any] | None, result: Mapping[str, Any] | None = None) -> bool:
        """Recognize strict TESTNET only from its explicit boolean marker."""
        status = status if isinstance(status, Mapping) else {}
        result = result if isinstance(result, Mapping) else {}
        return status.get("strict_testnet") is True or result.get("strict_testnet") is True

    def binance_nav_label(self) -> str:
        """Return the initial Binance nav label from the facade's strict marker."""
        return (
            "BINANCE SPOT TESTNET"
            if getattr(self.binance_canary, "strict_testnet", False) is True
            else "BINANCE SPOT CANARY"
        )

    @classmethod
    def _binance_testnet_projection(
        cls,
        result: dict[str, Any],
        status: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Expose stable named TESTNET sections while retaining duck typing."""
        projected_status = dict(status)
        projected_status["strict_testnet"] = True
        projected_status["title"] = "BINANCE SPOT TESTNET"
        projected_status.setdefault("environment", "BINANCE_SPOT_TESTNET")
        profile = projected_status.get("profile")
        if not isinstance(profile, Mapping):
            profile = result.get("profile") if isinstance(result.get("profile"), Mapping) else {}
        projected_status["profile"] = dict(profile)
        projected_status["profile"].setdefault("environment", "TESTNET")
        projected_status.pop("enable_phrase", None)
        projected_status.pop("probe_confirmation", None)
        for name in (
            "credentials",
            "connectivity",
            "validation",
            "probe",
            "isolation",
            "autonomous",
        ):
            value = result.get(name, projected_status.get(name))
            if value is not None:
                projected_status[name] = value
                result.setdefault(name, value)
        result.pop("enable_phrase", None)
        result.pop("probe_confirmation", None)
        result["strict_testnet"] = True
        result["title"] = "BINANCE SPOT TESTNET"
        result["environment"] = projected_status["environment"]
        result["profile"] = projected_status["profile"]
        result["status"] = projected_status
        return result

    def binance_canary_data(self, params: Mapping[str, Any] | None = None) -> dict[str, Any]:
        """Return a bounded, secret-free projection from an optional Binance facade.

        The dashboard deliberately does not import or construct the Binance
        control plane.  Callers inject an already configured facade and this
        method only uses its public duck-typed methods.
        """
        values = _pagination_params(params)
        page = int(values["page"])
        page_size = min(int(values["page_size"]), 100)
        facade = self.binance_canary
        if facade is None:
            return {
                "available": False,
                "configured": False,
                "page": page,
                "page_size": page_size,
                "positions": _page_result([], page=page, page_size=page_size),
                "orders": _page_result([], page=page, page_size=page_size),
                "fills": _page_result([], page=page, page_size=page_size),
                "unknown": _page_result([], page=page, page_size=page_size),
                "actions": [],
                "status": {"state": "NOT_CONFIGURED", "transport": {"polymarket": "DISABLED"}},
            }

        snapshot_method = getattr(facade, "snapshot", None)
        raw: Any = None
        if callable(snapshot_method):
            try:
                raw = snapshot_method(page=page, page_size=page_size)
            except TypeError:
                try:
                    raw = snapshot_method(page, page_size)
                except TypeError:
                    try:
                        raw = snapshot_method(page_size, page)
                    except TypeError:
                        raw = snapshot_method()
        status_method = getattr(facade, "status", None)
        status_raw: Any = None
        if isinstance(raw, Mapping) and isinstance(raw.get("status"), Mapping):
            status_raw = raw.get("status")
        elif isinstance(raw, Mapping):
            status_raw = raw
        if status_raw is None and callable(status_method):
            status_raw = status_method()

        result = dict(raw) if isinstance(raw, Mapping) else {}
        if isinstance(status_raw, Mapping):
            # Keep status fields available at the top level for simple fakes,
            # while preserving the explicit nested status contract.
            for key, value in status_raw.items():
                result.setdefault(str(key), value)
            result["status"] = status_raw
        result.setdefault("available", True)
        result["configured"] = True
        result["page"] = page
        result["page_size"] = page_size

        history_method = getattr(facade, "action_history", None)
        if not callable(history_method):
            history_method = getattr(facade, "list_actions", None)
        if callable(history_method) and "actions" not in result:
            try:
                history = history_method(limit=page_size)
            except TypeError:
                history = history_method()
            result["actions"] = history
        if isinstance(status_raw, Mapping) and self._binance_testnet_status(status_raw, result):
            result = self._binance_testnet_projection(result, status_raw)
        return _binance_safe_value(result)

    def v2_snapshot(self, endpoint: str, params: Mapping[str, Any] | None = None) -> dict[str, Any]:
        name = endpoint.strip("/")
        if name.lower().startswith("datasets/"):
            parts = name.split("/")
            identifier = unquote(parts[1])
            detail = self.dataset_detail(identifier)
            if len(parts) > 2 and parts[2].lower() == "missing-ranges":
                values = _pagination_params(params)
                method = getattr(self.store, "paginate_dataset_missing_ranges", None) if self.store is not None else None
                if callable(method):
                    try:
                        return method(identifier, dataset_version=values.get("dataset_version"), page=values["page"], page_size=values["page_size"], sort=values["sort"] or "range_index", direction=values["direction"], filter=values["filter"])
                    except (AttributeError, TypeError, ValueError):
                        pass
                catalog = detail.get("catalog", {}) if isinstance(detail, Mapping) else {}
                ranges = catalog.get("missing_ranges", []) if isinstance(catalog, Mapping) else []
                start = (values["page"] - 1) * values["page_size"]
                return _page_result(ranges[start : start + values["page_size"]], page=values["page"], page_size=values["page_size"], total=len(ranges))
            return detail
        if name.lower().startswith("candidates/") and name.lower().endswith("/events"):
            parts = name.split("/")
            identifier = unquote(parts[1])
            values = _pagination_params(params)
            method = getattr(self.store, "paginate_candidate_lifecycle_events", None) if self.store is not None else None
            if callable(method):
                return method(candidate_id=identifier, page=values["page"], page_size=values["page_size"], sort=values["sort"] or "created_at", direction=values["direction"], filter=values["filter"])
            events = self.store.list_candidate_lifecycle_events(identifier, limit=_MAX_SIZE_FALLBACK) if self.store is not None and callable(getattr(self.store, "list_candidate_lifecycle_events", None)) else []
            start = (values["page"] - 1) * values["page_size"]
            return _page_result(events[start : start + values["page_size"]], page=values["page"], page_size=values["page_size"], total=len(events))
        if name.lower().startswith("candidates/"):
            identifier = unquote(name.split("/", 1)[1])
            return self.strategy_detail(identifier)
        if name.lower().startswith("hermes/"):
            identifier = unquote(name.split("/", 1)[1])
            return self.hermes_detail(identifier)
        if name.lower().startswith("crypto-research/"):
            identifier = unquote(name.split("/", 1)[1])
            return self.crypto_research_detail(identifier)
        handlers = {
            "overview-summary": lambda _params: self.overview_summary(),
            "canary": lambda _params: self.canary_data(),
            "binance-canary": self.binance_canary_data,
            "datasets": self.paginate_dataset_catalog,
            "activity": self.paginate_research_activity,
            "candidates": self.paginate_candidate_lifecycle,
            "polymarket": self.paginate_polymarket_markets,
            "hermes": self.paginate_research_queue,
            "crypto-research": self.paginate_crypto_research,
            "crypto": self.paginate_crypto_research,
            "paper": self.paginate_paper_records,
        }
        handler = handlers.get(name.lower())
        if handler is None:
            raise KeyError(endpoint)
        return handler(params)
    def evidence_maturity(self) -> Any:
        configured = self._configured("evidence-maturity")
        if configured is not None:
            return configured
        if self.store is None or not callable(getattr(self.store, "polymarket_evidence_maturity", None)):
            return {"grade": "F", "grade_scope": "research_evidence_maturity", "live_execution": False}
        return self.store.polymarket_evidence_maturity()

    def research_summary_data(self) -> Any:
        configured = self._configured("research-summary")
        if configured is not None:
            return configured
        return research_summary(self.store) if self.store is not None else {"live_execution": False, "gaps": ["no store"]}
    def autonomous_research_data(self) -> Any:
        configured = self._configured("autonomous-research")
        if configured is not None:
            return configured
        if self.store is None:
            return {
                "hermes": {"submitted": 0, "accepted": 0, "rejected": 0, "pending": 0},
                "plans": [],
                "queue": {},
                "lifecycle_funnel": {},
                "rejection_reasons": {},
                "accounting": {},
                "budgets": None,
                "live_execution": False,
            }
        summary = research_summary(self.store, limit=50)
        autonomous = summary.get("autonomous", {}) if isinstance(summary, Mapping) else {}
        return {
            "hermes": summary.get("hermes", {}),
            "plans": autonomous.get("plans", []),
            "queue": autonomous.get("queue_items", []),
            "lifecycle_funnel": autonomous.get("lifecycle_funnel", {}),
            "rejection_reasons": autonomous.get("rejection_reasons", {}),
            "accounting": autonomous.get("accounting", {}),
            "budgets": autonomous.get("budget"),
            "live_execution": False,
        }

    def paper_data(self) -> Any:
        configured = self._configured("paper")
        if configured is not None:
            return configured
        states = self.store.list_paper_states() if self.store is not None and callable(getattr(self.store, "list_paper_states", None)) else []
        return {"available": bool(states), "states": states, "live_execution": False}

    def opportunities_data(self) -> Any:
        configured = self._configured("opportunities")
        if configured is not None:
            return configured
        records = self.store.list_opportunity_snapshots(limit=100) if self.store is not None else []
        return {"available": bool(records), "opportunities": records, "live_execution": False}

    def queue_data(self) -> Any:
        if self.store is None:
            return {"total": 0, "live_execution": False}
        return {"stats": self.store.research_queue_stats(), "items": self.store.list_research_items(limit=50), "live_execution": False}

    def status_data(self) -> Any:
        if self.store is None:
            return {"status": "offline", "live_execution": False}
        # This endpoint is a storage projection.  Process identity and lock
        # ownership are verified by the supervisor/action paths, never by a
        # dashboard refresh.
        workers = self.store.list_worker_states(limit=2048)
        now = ensure_utc(self.clock())
        statuses: list[str] = []
        normalized_workers: list[dict[str, Any]] = []
        crypto_error = False
        for row in workers:
            if not isinstance(row, Mapping):
                continue
            item = dict(row)
            worker_name = str(item.get("worker_name", ""))
            worker_payload = item.get("payload")
            worker_payload = worker_payload if isinstance(worker_payload, Mapping) else {}
            state = str(item.get("status", "unknown")).lower()
            if (
                isinstance(worker_payload.get("crypto_paper"), Mapping)
                and worker_payload["crypto_paper"].get("enabled")
                and worker_payload["crypto_paper"].get("last_error")
            ):
                crypto_error = True
            heartbeat = parse_timestamp(item.get("heartbeat_at"))
            age = (
                max(0.0, (now - heartbeat).total_seconds())
                if heartbeat is not None
                else None
            )
            try:
                stale_after = float(worker_payload.get("stale_after_seconds", 300.0))
            except (TypeError, ValueError):
                stale_after = 300.0
            if not math.isfinite(stale_after) or stale_after <= 0:
                stale_after = 300.0
            if state in {"running", "degraded"} and (age is None or age > stale_after):
                state = "stale"
                item["status"] = state
            item["worker_alive"] = None
            item["worker_identity_valid"] = (
                worker_payload.get("worker_identity_valid")
                if isinstance(worker_payload.get("worker_identity_valid"), bool)
                else None
            )
            item["worker_lock_owner_valid"] = None
            item["heartbeat_age_seconds"] = age
            item["stale_after_seconds"] = stale_after
            item["liveness"] = "PERSISTED_ONLY"
            statuses.append(state)
            normalized_workers.append(item)
        health_rows = [
            row
            for row in normalized_workers
            if str(row.get("worker_name", "")) == "health-monitor"
        ]
        health_payload = health_rows[0].get("payload", {}) if health_rows else {}
        health_grade = (
            str(health_payload.get("grade", "")).upper()
            if isinstance(health_payload, Mapping)
            else ""
        )
        if "stale" in statuses:
            status = "stale"
        elif (
            crypto_error
            or (health_grade and health_grade not in {"A", "OK", "HEALTHY"})
            or "degraded" in statuses
        ):
            status = "degraded"
        elif "running" in statuses:
            status = "running"
        elif "unknown" in statuses:
            status = "unknown"
        elif "stopped" in statuses:
            status = "stopped"
        elif statuses:
            status = "idle"
        else:
            status = "not_started"
        summary = research_summary(self.store, limit=20)
        try:
            current_health = self.dataset_health()
        except Exception as exc:
            current_health = {
                "grade": "F",
                "reason_code": "HEALTH_UNAVAILABLE",
                "error": str(exc),
            }
        health_fields = self._health_status_fields(
            normalized_workers,
            current_health if isinstance(current_health, Mapping) else {},
        )
        return {
            "status": status,
            "summary": self.store.dashboard_summary(),
            "cycles": self.store.list_collection_cycles(limit=20),
            "queue": self.store.research_queue_stats(),
            "workers": normalized_workers,
            "normalized_workers": normalized_workers,
            "autonomous": summary.get("autonomous", {}),
            "hermes": summary.get("hermes", {}),
            "health_grade": health_grade or health_fields["health_grade"],
            **health_fields,
            "live_execution": False,
        }
    def _health_status_fields(self, workers: Sequence[Mapping[str, Any]], health: Mapping[str, Any]) -> dict[str, Any]:
        grade = str(health.get("grade", "")).upper() or None
        reasons = health.get("reasons", [])
        first_reason = reasons[0] if isinstance(reasons, (list, tuple)) and reasons else {}
        if not isinstance(first_reason, Mapping):
            first_reason = {}
        unhealthy = next(
            (
                row for row in workers
                if str(row.get("status", "")).lower() in {"degraded", "stale", "error"}
            ),
            None,
        )
        worker_name = "health-monitor" if grade and grade not in {"A", "OK", "HEALTHY"} else (
            str(unhealthy.get("worker_name")) if isinstance(unhealthy, Mapping) else None
        )
        worker_payload = unhealthy.get("payload") if isinstance(unhealthy, Mapping) else {}
        if worker_name == "health-monitor":
            worker_payload = next(
                (row.get("payload") for row in workers if str(row.get("worker_name")) == worker_name),
                {},
            )
        payload = worker_payload if isinstance(worker_payload, Mapping) else {}
        reason = payload.get("degrading_reason") or first_reason.get("reason") or payload.get("last_error") or health.get("error")
        code = payload.get("reason_code") or first_reason.get("code") or health.get("reason_code")
        return {
            "health_grade": grade,
            "health_reason_code": str(code) if code else None,
            "health_reasons": list(reasons) if isinstance(reasons, (list, tuple)) else [],
            "degrading_worker": worker_name,
            "degrading_reason": str(reason) if reason else None,
            "historical_maturity_grade": health.get("historical_maturity_grade"),
            "historical_error_count": health.get("historical_error_count", 0),
            "health_window": {
                "start": health.get("window_start"),
                "end": health.get("window_end"),
                "seconds": health.get("window_seconds"),
            },
        }

    def system(self) -> dict[str, Any]:

        configured = self._configured("system")
        if configured is not None:
            result = dict(configured) if isinstance(configured, Mapping) else {"value": configured}
        else:
            result = {"service": "axiom-dashboard", "status": "ok", "offline": True, "live_execution": False}
        result["dataset_health"] = self.dataset_health()
        result["endpoints"] = list(_ENDPOINTS)
        return result
    @staticmethod
    def _bounded_operator_health(value: Any) -> dict[str, Any]:
        """Project persisted health without reintroducing unbounded failure rows."""
        if not isinstance(value, Mapping):
            return {}
        fields = (
            "grade",
            "grade_scope",
            "reason_code",
            "reason_display",
            "reasons",
            "window_start",
            "window_end",
            "window_seconds",
            "source_type",
            "historical_maturity_grade",
            "historical_error_count",
            "evidence_grade",
            "markets",
            "markets_with_snapshots",
            "metadata_records",
            "snapshots",
            "collection_errors",
            "top_failure_codes",
            "stale_market_count",
            "reason",
            "degrading_reason",
            "configured_interval_seconds",
            "expected_interval_seconds",
            "stale_after_seconds",
            "last_cycle_started_at",
            "last_cycle_ended_at",
            "last_cycle_duration_seconds",
            "last_successful_cycle",
            "last_cycle_markets_attempted",
            "last_cycle_markets_successful",
            "last_cycle_markets_failed",
            "latest_observed_at",
            "next_scheduled_collection_at",
            "worker_heartbeat_at",
            "stale_markets",
            "gap_count",
            "gaps",
            # Candidate-bound health is persisted alongside collector health.
            "candidate_bound_markets",
            "scheduled",
            "fresh",
            "stale",
            "missing",
            "newest_required_source_timestamp",
            "oldest_required_source_timestamp",
            "newest_required_observed_at",
            "oldest_required_observed_at",
            "newest_required_snapshot",
            "oldest_required_snapshot",
            "newest_required_source",
            "oldest_required_source",
            "newest_required_observed",
            "oldest_required_observed",
            "candidate_references",
            "market_diagnostics",
            "diagnostics",
            "required_market_count",
            "unresolved_candidates",
            "closed_candidates",
        )
        return {
            key: _bounded_value(value[key])
            for key in fields
            if key in value
        }


    def _health_for_operator(
        self,
        persisted_worker_health: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Project only already-persisted/configured health evidence.

        A normal overview read must not reconstruct collector health from the
        full snapshot/error tables.  Missing evidence is represented as stale
        and unknown by ``overview_summary``.
        """
        configured = self._configured("dataset-health")
        if isinstance(configured, Mapping):
            return self._bounded_operator_health(configured)
        if isinstance(persisted_worker_health, Mapping) and persisted_worker_health:
            projected = self._bounded_operator_health(persisted_worker_health)
            if projected:
                return projected
        return {}


    @staticmethod
    def _operator_health_fields(
        health: Mapping[str, Any],
    ) -> tuple[str | None, str, Any, list[Any], str | None, str | None, str | None, Any]:
        grade = str(health.get("grade") or "").upper() or None
        scope = health.get("grade_scope") or "collector_health"
        reasons = health.get("reasons", [])
        reasons = list(reasons)[:32] if isinstance(reasons, (list, tuple)) else []
        first_reason = reasons[0] if reasons else {}
        if not isinstance(first_reason, Mapping):
            first_reason = {}
        reason_code = _nested_value(
            health,
            first_reason,
            keys=("reason_code", "code", "error_code"),
        )
        reason = _nested_value(
            health,
            first_reason,
            keys=("degrading_reason", "reason", "detail", "message", "human_reason", "last_error", "error"),
        )
        source_type = str(health.get("source_type") or "FORWARD_COLLECTED")
        maturity_grade = health.get("historical_maturity_grade") or health.get("evidence_grade")
        return (
            grade,
            str(scope),
            reason_code,
            reasons,
            str(reason) if reason else None,
            source_type,
            maturity_grade,
            health.get("historical_error_count", 0),
        )


    def _operator_bootstrap_progress(self) -> list[dict[str, Any]]:
        """Read only the small bootstrap page needed by operator clients."""
        try:
            return self._bootstrap_progress_rows(limit=20)
        except (AttributeError, TypeError, ValueError, sqlite3.Error):
            return []

    def overview(self) -> dict[str, Any]:
        configured = self._configured("overview")
        if configured is not None:
            return dict(configured) if isinstance(configured, Mapping) else configured
        research = self.research()
        experiments = research.get("experiments", ()) if isinstance(research, Mapping) else ()
        experiment_count = len(experiments) if hasattr(experiments, "__len__") else 0
        if self.store is not None:
            try:
                experiment_count = int(self.store.dashboard_summary().get("experiments", experiment_count))
            except Exception:
                pass
        return {
            "service": "axiom-dashboard",
            "status": "ok",
            "offline": True,
            "live_execution": False,
            "experiment_count": experiment_count,
            "endpoints": list(_ENDPOINTS),
        }

    def dataset_catalog_data(self) -> dict[str, Any]:
        configured = self._configured("datasets")
        if configured is not None:
            return dict(configured) if isinstance(configured, Mapping) else {"value": configured}
        if self.store is None or not callable(getattr(self.store, "list_dataset_catalog", None)):
            return {
                "historical": [],
                "forward": [],
                "historical_count": 0,
                "forward_count": 0,
                "live_execution": False,
            }
        records = self.store.list_dataset_catalog(limit=_MAX_SIZE_FALLBACK)
        historical = [item for item in records if str(item.get("source_type", "")).upper() == "HISTORICAL"]
        forward = [item for item in records if str(item.get("source_type", "")).upper() == "FORWARD_COLLECTED"]
        return {
            "historical": historical,
            "forward": forward,
            "historical_count": len(historical),
            "forward_count": len(forward),
            "historical_rows": sum(int(item.get("row_count", 0)) for item in historical),
            "forward_rows": sum(int(item.get("row_count", 0)) for item in forward),
            "historical_coverage": [
                {
                    "dataset_id": item.get("dataset_id"),
                    "instrument": item.get("instrument"),
                    "timeframe": item.get("timeframe"),
                    "start": item.get("start_timestamp"),
                    "end": item.get("end_timestamp"),
                    "rows": item.get("row_count", 0),
                    "completeness": item.get("completeness", 0.0),
                    "quality": item.get("quality"),
                    "missing_ranges": item.get("missing_ranges", []),
                }
                for item in historical
            ],
            "forward_coverage": [
                {
                    "dataset_id": item.get("dataset_id"),
                    "instrument": item.get("instrument"),
                    "timeframe": item.get("timeframe"),
                    "start": item.get("start_timestamp"),
                    "end": item.get("end_timestamp"),
                    "rows": item.get("row_count", 0),
                    "quality": item.get("quality"),
                }
                for item in forward
            ],
            "live_execution": False,
        }

    def _bounded_candidate_lifecycle(self, *, limit: int = _LATEST_CANDIDATE_LIMIT) -> list[Mapping[str, Any]]:
        """Read the globally newest bounded candidate history for dashboard cards."""
        if self.store is None:
            return []
        try:
            bounded_limit = max(0, min(int(limit), _LATEST_CANDIDATE_LIMIT))
        except (TypeError, ValueError, OverflowError):
            return []
        if bounded_limit == 0:
            return []

        def bounded_records(records: Any) -> list[Mapping[str, Any]]:
            source = records if isinstance(records, (list, tuple)) else ()
            return list(
                islice(
                    (item for item in source if isinstance(item, Mapping)),
                    bounded_limit,
                )
            )

        paginator = getattr(self.store, "paginate_candidate_lifecycle", None)
        if callable(paginator):
            try:
                page = paginator(
                    page=1,
                    page_size=min(_LATEST_CANDIDATE_LIMIT, max(10, bounded_limit)),
                    sort="updated_at",
                    direction="desc",
                )
            except (AttributeError, TypeError, ValueError, sqlite3.Error):
                page = None
            if isinstance(page, Mapping):
                return bounded_records(page.get("items"))

        loader = getattr(self.store, "load_candidate_lifecycle", None)
        if not callable(loader):
            return []
        try:
            records = loader(limit=bounded_limit)
        except (AttributeError, TypeError, ValueError, sqlite3.Error):
            return []
        return bounded_records(records)

    def _candidate_rows(self) -> list[dict[str, Any]]:
        """Return the bounded legacy candidate table without authority scans."""
        return [
            self._candidate_row(item)
            for item in self._bounded_candidate_lifecycle()
        ]

    @staticmethod
    def _latest_candidate_projection(item: Mapping[str, Any]) -> dict[str, Any]:
        payload = item.get("payload")
        payload = payload if isinstance(payload, Mapping) else {}
        candidate_id = str(item.get("candidate_id") or "").strip()
        persisted_provenance = payload.get("dataset_provenance", payload.get("provenance"))
        provenance = (
            _bounded_value(persisted_provenance)
            if isinstance(persisted_provenance, Mapping)
            else {}
        )
        return {
            "candidate_id": candidate_id,
            "strategy_id": payload.get("strategy_id", payload.get("experiment_id", candidate_id)),
            "family": payload.get("experiment_family", payload.get("family", "unknown")),
            "market": payload.get("market_type", payload.get("market")),
            "market_type": payload.get("market_type", payload.get("market")),
            "stage": item.get("stage"),
            "updated_at": item.get("updated_at"),
            "provenance": provenance,
        }

    def _persisted_forward_health(
        self,
        *sources: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        """Return one bounded persisted required-market health projection."""
        health_keys = {
            "candidate_bound_markets",
            "scheduled",
            "fresh",
            "stale",
            "missing",
            "market_diagnostics",
            "diagnostics",
            "required_market_count",
        }
        for source in sources:
            if isinstance(source, Mapping) and health_keys.intersection(source):
                return dict(source)
        if self.store is None:
            return {}
        workers = getattr(self.store, "list_worker_states", None)
        if callable(workers):
            try:
                rows = workers(limit=32)
            except (AttributeError, TypeError, ValueError, sqlite3.Error):
                rows = []
            for row in rows if isinstance(rows, (list, tuple)) else ():
                if not isinstance(row, Mapping):
                    continue
                payload = row.get("payload")
                if not isinstance(payload, Mapping):
                    continue
                for candidate in (
                    payload,
                    payload.get("required_health"),
                    payload.get("market_health"),
                    payload.get("forward_health"),
                ):
                    if isinstance(candidate, Mapping) and health_keys.intersection(candidate):
                        return dict(candidate)
        evaluations = getattr(self.store, "list_signal_evaluations", None)
        if not callable(evaluations) and self._canary_service is not None:
            evaluations = getattr(self._canary_service, "list_signal_evaluations", None)
        if callable(evaluations):
            try:
                rows = evaluations(limit=64)
            except (AttributeError, TypeError, ValueError, sqlite3.Error):
                rows = []
            for row in rows if isinstance(rows, (list, tuple)) else ():
                if not isinstance(row, Mapping):
                    continue
                candidate = row.get("required_health")
                if isinstance(candidate, Mapping) and health_keys.intersection(candidate):
                    return dict(candidate)
        return {}


    def _candidate_canary_eligibility(self, candidate_id: str) -> Mapping[str, Any] | None:
        """Read and verify the persisted eligibility binding."""
        if self.store is None or not candidate_id:
            return None
        connection = getattr(self.store, "connection", None)
        if connection is None:
            return None
        try:
            lock = getattr(self.store, "_lock", None)
            if lock is None:
                row = connection.execute(
                    "SELECT candidate_id,eligible_at,frozen_hash,evidence_json "
                    "FROM canary_eligibility WHERE candidate_id=?",
                    (candidate_id,),
                ).fetchone()
            else:
                with lock:
                    row = connection.execute(
                        "SELECT candidate_id,eligible_at,frozen_hash,evidence_json "
                        "FROM canary_eligibility WHERE candidate_id=?",
                        (candidate_id,),
                    ).fetchone()
        except (AttributeError, sqlite3.Error):
            # The canary schema is optional for read-only dashboard consumers.
            return None
        if row is None:
            return None
        eligibility = dict(row)
        return {
            "candidate_id": eligibility.get("candidate_id"),
            "eligible_at": eligibility.get("eligible_at"),
        }
    def _candidate_canary_eligibility_count(self) -> int:
        """Count persisted bindings that still validate against lifecycle."""
        if self.store is None:
            return 0
        connection = getattr(self.store, "connection", None)
        if connection is None:
            return 0
        query = (
            "SELECT e.candidate_id,e.frozen_hash,e.evidence_json "
            "FROM canary_eligibility AS e "
            "JOIN candidate_lifecycle AS c ON c.candidate_id=e.candidate_id "
            "WHERE c.stage IN ('FROZEN','PAPER_FORWARD','PAPER_PROMOTABLE')"
        )
        try:
            lock = getattr(self.store, "_lock", None)
            if lock is None:
                rows = connection.execute(query).fetchall()
            else:
                with lock:
                    rows = connection.execute(query).fetchall()
        except (AttributeError, sqlite3.Error):
            return 0
        return sum(
            1
            for row in rows
            if _canary_eligibility_is_bound(
                self.store,
                str(row["candidate_id"]),
                row,
            )
        )


    def _candidate_status_fields(self, item: Mapping[str, Any]) -> dict[str, Any]:
        """Project persisted lifecycle/eligibility state without qualification work.

        Dashboard reads must never re-run canary qualification.  The lifecycle
        stage and the presence of a persisted eligibility row are deliberately
        only a display projection; authoritative decisions remain in the
        canary execution paths.
        """
        candidate_id = str(item.get("candidate_id") or "").strip()
        stage = str(item.get("stage") or "").strip().upper()
        payload = item.get("payload")
        payload = payload if isinstance(payload, Mapping) else {}
        eligibility = self._candidate_canary_eligibility(candidate_id)
        canary_eligible = bool(eligibility) and stage in _CANARY_ELIGIBLE_STAGES
        persisted_gates = _nested_value(
            item,
            payload,
            keys=("historical_gates", "historical_gate_status", "qualification_status"),
        )
        historical_gates = (
            str(persisted_gates).strip().upper()
            if persisted_gates is not None and str(persisted_gates).strip()
            else "NOT_PASSED"
        )
        persisted_quality = _nested_value(
            item,
            payload,
            keys=("canary_data_quality_gate", "quality_gate"),
        )
        quality_gate = (
            str(persisted_quality).strip()
            if persisted_quality is not None and str(persisted_quality).strip()
            else "NOT PASSED"
        )
        paper_forward = stage in _PAPER_FORWARD_STAGES
        if stage == "PAPER_FORWARD":
            paper_forward_status = "ACTIVE"
            paper_status = "PAPER_FORWARD"
        elif stage == "PAPER_PROMOTABLE":
            paper_forward_status = "COMPLETE"
            paper_status = "PAPER_PROMOTABLE"
        else:
            paper_forward_status = "NOT_STARTED"
            paper_status = "NOT_STARTED"
        return {
            "historical_gates": historical_gates,
            "historical_gate_reason": _nested_value(
                item,
                payload,
                keys=("historical_gate_reason", "qualification_reason", "reason"),
            ),
            "historical_data_integrity": str(
                _nested_value(item, payload, keys=("historical_data_integrity",))
                or "UNKNOWN"
            ),
            "historical_execution_fidelity": str(
                _nested_value(item, payload, keys=("historical_execution_fidelity",))
                or "UNKNOWN"
            ),
            "canary_data_quality_gate": quality_gate,
            "production_evidence": str(
                _nested_value(item, payload, keys=("production_evidence",))
                or "INSUFFICIENT"
            ),
            "canary_eligible": canary_eligible,
            "canary_eligible_at": eligibility.get("eligible_at") if canary_eligible and eligibility else None,
            "canary_status": "ELIGIBLE" if canary_eligible else "NOT_ELIGIBLE",
            "paper_forward": paper_forward,
            "paper_forward_status": paper_forward_status,
            "paper_promotable": stage == "PAPER_PROMOTABLE",
            "paper_promotable_status": "PROMOTABLE" if stage == "PAPER_PROMOTABLE" else "NOT_YET",
            "paper_status": paper_status,
        }

    def _candidate_provenance(self, payload: Mapping[str, Any], candidate_id: str) -> dict[str, Any]:
        plan = payload.get("experiment_plan")
        plan = plan if isinstance(plan, Mapping) else {}
        config = payload.get("forward_config")
        config = config if isinstance(config, Mapping) else {}
        forward: Mapping[str, Any] = {}
        forward_id = str(payload.get("forward_test_id") or "").strip()
        if forward_id and self.store is not None and callable(getattr(self.store, "load_forward_test", None)):
            try:
                loaded = self.store.load_forward_test(forward_id)
                if isinstance(loaded, Mapping):
                    forward = loaded
            except (AttributeError, TypeError, ValueError):
                forward = {}
        sources = (payload, plan, config, forward)
        values = {
            "market_type": _nested_value(*sources, keys=("market_type", "market")),
            "instrument": _nested_value(*sources, keys=("instrument", "symbol", "asset")),
            "dataset_id": _nested_value(*sources, keys=("dataset_id", "dataset", "data_id")),
            "dataset_version": _nested_value(*sources, keys=("dataset_version", "data_version", "version")),
            "source_type": _nested_value(*sources, keys=("source_type", "data_source")),
            "timeframe": _nested_value(*sources, keys=("timeframe", "interval")),
        }
        dataset_id = str(values["dataset_id"] or "").strip()
        dataset_version = str(values["dataset_version"] or "").strip()
        missing = "MISSING PROVENANCE"
        normalized = {
            key: str(value).strip() if value is not None and str(value).strip() else missing
            for key, value in values.items()
        }
        normalized["forward_test_id"] = forward_id or missing
        normalized["status"] = "COMPLETE" if all(value != missing for value in normalized.values()) else missing
        normalized["candidate_id"] = candidate_id
        return normalized

    def _candidate_row(self, item: Mapping[str, Any]) -> dict[str, Any]:
        payload = item.get("payload", {})
        payload = payload if isinstance(payload, Mapping) else {}
        candidate_id = str(item.get("candidate_id", ""))
        provenance = self._candidate_provenance(payload, candidate_id)
        return {
            "candidate_id": candidate_id,
            "strategy_id": payload.get("strategy_id", payload.get("experiment_id", candidate_id)),
            "family": payload.get("experiment_family", payload.get("family", "unknown")),
            "market": provenance["market_type"],
            "market_type": provenance["market_type"],
            "generation": payload.get("generation", 0),
            "parent_id": payload.get("parent_id"),
            "stage": item.get("stage"),
            "validation_expectancy": payload.get("validation_expectancy", payload.get("validation_score")),
            "validation_max_drawdown": payload.get("validation_max_drawdown"),
            "validation_stability": payload.get("validation_stability"),
            "forward_bets": payload.get("forward_independent_resolved_bets", payload.get("markets_resolved")),
            "forward_pnl": payload.get("forward_pnl", payload.get("forward_net_pnl")),
            "data_quality": payload.get("data_quality", payload.get("quality", payload.get("research_quality"))),
            "provenance": provenance,
            "rejection_reason": payload.get("rejection_reason"),
            "updated_at": item.get("updated_at"),
            **self._candidate_status_fields(item),
        }
    @staticmethod
    def _compact_candidate_value(value: Any) -> Any:
        """Keep overview display values scalar and bounded."""
        if isinstance(value, (Mapping, list, tuple, set, frozenset)):
            return "<complex>"
        normalized = _jsonable(value)
        if isinstance(normalized, (Mapping, list, tuple, set, frozenset)):
            return "<complex>"
        if isinstance(normalized, str):
            return normalized[:256]
        return normalized

    def _compact_candidate_display_row(self, item: Mapping[str, Any]) -> dict[str, Any]:
        """Project only overview columns, reusing an already projected row."""
        # ``paginate_candidate_lifecycle`` normally returns the public
        # candidate projection.  Re-projecting it here used to repeat
        # provenance loading and eligibility reads for every overview row.
        row = (
            item
            if "historical_gates" in item and "canary_status" in item
            else self._candidate_row(item)
        )
        return {
            key: self._compact_candidate_value(row.get(key))
            for key in (
                "candidate_id",
                "strategy_id",
                "family",
                "market",
                "stage",
                "historical_gates",
                "canary_status",
                "paper_forward_status",
                "paper_promotable_status",
                "updated_at",
            )
        }

    def _activity_feed(self, *, limit: int = 50) -> list[dict[str, Any]]:
        if self.store is None:
            return []
        events: list[dict[str, Any]] = []

        def add(kind: str, timestamp: Any, message: str, details: Mapping[str, Any] | None = None) -> None:
            stamp = parse_timestamp(timestamp)
            events.append(
                {
                    "kind": kind,
                    "timestamp": stamp,
                    "message": message,
                    "details": dict(details or {}),
                }
            )

        for item in self.store.list_dataset_catalog(limit=100):
            add(
                "dataset",
                item.get("updated_at"),
                f"Dataset {item.get('dataset_id')} published ({item.get('row_count', 0)} rows)",
                {
                    "source_type": item.get("source_type"),
                    "timeframe": item.get("timeframe"),
                    "quality": item.get("quality"),
                },
            )
        for item in self.store.list_dataset_bootstrap_states(limit=100):
            add(
                "bootstrap",
                item.get("updated_at"),
                f"{item.get('dataset_id')} bootstrap {str(item.get('status', 'unknown')).lower()}",
                {"errors": item.get("errors", []), "next_timestamp": item.get("next_timestamp")},
            )
        for item in self.store.list_collection_cycles(limit=100):
            payload = item.get("payload", {}) if isinstance(item, Mapping) else {}
            payload = payload if isinstance(payload, Mapping) else {}
            add(
                "collection",
                item.get("ended_at") or item.get("started_at"),
                f"Polymarket collection cycle completed ({payload.get('markets_seen', 0)} markets)",
                {"errors": payload.get("errors", 0), "cycle_id": item.get("cycle_id")},
            )
        for item in self.store.list_candidate_lifecycle_events(limit=100):
            add(
                "lifecycle",
                item.get("created_at"),
                f"Candidate {item.get('candidate_id')} moved to {item.get('to_stage')}",
                {"from_stage": item.get("from_stage"), "reason": item.get("reason")},
            )
        for item in self.store.list_research_items(limit=100):
            add(
                "research",
                item.get("updated_at"),
                f"Research item {item.get('item_type')} is {str(item.get('status', 'unknown')).lower()}",
                {"item_id": item.get("item_id"), "last_error": item.get("last_error")},
            )
        for item in self.store.list_reports(limit=100, newest_first=True):
            add(
                "report",
                item.get("created_at"),
                f"Research report {item.get('report_id')} saved",
                {"experiment_id": item.get("experiment_id")},
            )
        events.extend(self._campaign_activity_rows())
        events.sort(key=lambda item: parse_timestamp(item.get("timestamp")) or datetime.min.replace(tzinfo=datetime.now().astimezone().tzinfo), reverse=True)
        return events[:limit]

    def _paper_portfolio(self, *, candidate_only: bool = False) -> dict[str, Any]:
        if self.store is None:
            return {
                "paper_money": True,
                "live_execution": False,
                "states": [],
                "total_equity": 0.0,
                "total_pnl": 0.0,
            }
        portfolio_loader = getattr(self.store, "list_paper_portfolio_states", None)
        if candidate_only:
            candidate_ids = self._candidate_paper_experiment_ids()
            if not candidate_ids:
                states = []
            elif callable(portfolio_loader):
                states = portfolio_loader(experiment_ids=candidate_ids, limit=1000)
            else:
                scoped_loader = getattr(self.store, "list_paper_states_for_experiments", None)
                states = (
                    scoped_loader(candidate_ids, limit=1000)
                    if callable(scoped_loader)
                    else self.store.list_paper_states(limit=1000)
                )
                states = [
                    item
                    for item in states
                    if isinstance(item, Mapping)
                    and (
                        str(item.get("experiment_id") or "").strip() in candidate_ids
                        or str((item.get("state") or {}).get("candidate_id") if isinstance(item.get("state"), Mapping) else "").strip() in candidate_ids
                    )
                ]
        elif callable(portfolio_loader):
            states = portfolio_loader(limit=1000)
        else:
            states = self.store.list_paper_states(limit=1000)
        rows: list[dict[str, Any]] = []
        total_equity = 0.0
        total_pnl = 0.0
        total_bets = 0
        winning_bets = 0
        for item in states:
            state = item.get("state", {}) if isinstance(item, Mapping) else {}
            state = state if isinstance(state, Mapping) else {}
            portfolio = state.get("portfolio", {})
            portfolio = portfolio if isinstance(portfolio, Mapping) else {}
            risk = state.get("risk", {})
            risk = risk if isinstance(risk, Mapping) else {}
            equity = _number_or_zero(portfolio.get("equity", state.get("equity", 0.0)))
            initial = _number_or_zero(portfolio.get("initial_cash", state.get("initial_cash", 0.0)))
            pnl = equity - initial if initial else _number_or_zero(state.get("forward_pnl"))
            ledger = []
            if callable(getattr(self.store, "list_paper_bet_ledger", None)):
                ledger = self.store.list_paper_bet_ledger(str(item.get("experiment_id", "")), limit=1000)
            for bet in ledger:
                payload = bet.get("payload", {}) if isinstance(bet, Mapping) else {}
                pnl_value = _number_or_zero(payload.get("net_pnl")) if isinstance(payload, Mapping) else 0.0
                total_pnl += pnl_value
                total_bets += 1
                if pnl_value > 0:
                    winning_bets += 1
            total_equity += equity
            if not ledger:
                total_pnl += pnl
            rows.append(
                {
                    "experiment_id": item.get("experiment_id"),
                    "updated_at": item.get("updated_at"),
                    "equity": equity,
                    "initial_cash": initial,
                    "pnl": pnl,
                    "drawdown": _number_or_zero(state.get("forward_max_drawdown", risk.get("max_drawdown"))),
                    "fills": state.get("fill_count", len(portfolio.get("fills", [])) if isinstance(portfolio.get("fills"), list) else 0),
                    "open_positions": portfolio.get("positions", {}),
                    "resolved_bets": len(ledger),
                    "paper_only": True,

                }
            )
        return {
            "paper_money": True,
            "live_execution": False,
            "states": rows,
            "total_equity": total_equity,
            "total_pnl": total_pnl,
            "resolved_bets": total_bets,
            "win_rate": winning_bets / total_bets if total_bets else 0.0,
            "expectancy": total_pnl / total_bets if total_bets else 0.0,
        }
    def _candidate_paper_experiment_ids(self) -> set[str]:
        if self.store is None:
            return set()
        lifecycle_loader = getattr(self.store, "list_candidate_lifecycle_for_dashboard", None)
        if not callable(lifecycle_loader):
            lifecycle_loader = getattr(self.store, "load_candidate_lifecycle", None)
        if not callable(lifecycle_loader):
            return set()
        records = lifecycle_loader(limit=1000)
        result: set[str] = set()
        for item in records if isinstance(records, list) else []:
            if not isinstance(item, Mapping) or str(item.get("stage") or "").upper() not in _PAPER_FORWARD_STAGES:
                continue
            candidate_id = str(item.get("candidate_id") or "").strip()
            payload = item.get("payload")
            payload = payload if isinstance(payload, Mapping) else {}
            for value in (candidate_id, payload.get("experiment_id"), payload.get("strategy_id"), payload.get("paper_experiment_id")):
                if str(value or "").strip():
                    result.add(str(value).strip())
        return result

    def _bootstrap_universe_summary(self) -> dict[str, Any]:
        states = (
            self.store.list_dataset_bootstrap_states(limit=1000)
            if self.store is not None and callable(getattr(self.store, "list_dataset_bootstrap_states", None))
            else []
        )
        rows = self._bootstrap_progress_rows(states, limit=1000)
        selected_symbols = sorted({str(item["selected_symbol"]).strip() for item in rows if str(item.get("selected_symbol") or "").strip()})
        progress_values = [float(item["progress"]) for item in rows if item.get("progress") is not None]
        versions = sorted({str(item.get("universe_version")).strip() for item in rows if str(item.get("universe_version") or "").strip()})
        return {
            "selected_count": len(selected_symbols),
            "selected_symbols": selected_symbols[:64],
            "dataset_count": len(rows),
            "universe_version": versions[0] if versions else None,
            "progress": sum(progress_values) / len(progress_values) if progress_values else None,
            "completed_datasets": sum(1 for item in rows if str(item.get("status")).upper() in {"COMPLETE", "EMPTY"}),
        }

    def _bootstrap_progress_rows(self, states: Any | None = None, *, limit: int = 20) -> list[dict[str, Any]]:
        """Expose bounded, per-dataset bootstrap cursors for operator clients."""
        if states is None:
            states = (
                self.store.list_dataset_bootstrap_states(limit=20)
                if self.store is not None and callable(getattr(self.store, "list_dataset_bootstrap_states", None))
                else []
            )
        if not isinstance(states, (list, tuple)):
            return []
        rows: list[dict[str, Any]] = []
        for item in states[: max(0, int(limit))]:
            if not isinstance(item, Mapping):
                continue
            payload = item.get("payload")
            payload = payload if isinstance(payload, Mapping) else {}

            def value(*keys: str) -> Any:
                return _nested_value(item, payload, keys=keys)

            selected_symbol = value("selected_symbol", "symbol")
            instrument = value("instrument")
            symbol = str(selected_symbol or instrument or "").strip()
            if not symbol:
                continue
            errors = value("errors")
            errors = list(errors) if isinstance(errors, (list, tuple)) else []
            requested_start = value("requested_start")
            requested_end = value("requested_end")
            next_timestamp = value("next_timestamp")
            status = str(value("status") or "UNKNOWN").upper()
            progress = value("progress", "progress_fraction")
            try:
                progress = float(progress) if progress is not None else None
            except (TypeError, ValueError):
                progress = None
            if progress is None:
                start = parse_timestamp(requested_start)
                end = parse_timestamp(requested_end)
                cursor = parse_timestamp(next_timestamp)
                if status in {"COMPLETE", "EMPTY"}:
                    progress = 1.0
                elif start is not None and end is not None and cursor is not None and end > start:
                    progress = max(0.0, min(1.0, (cursor - start).total_seconds() / (end - start).total_seconds()))
            rows.append(
                {
                    "dataset_id": value("dataset_id"),
                    "symbol": symbol,
                    "selected_symbol": selected_symbol or None,
                    "instrument": instrument or symbol,
                    "timeframe": value("timeframe"),
                    "status": status,
                    "requested_start": requested_start,
                    "requested_end": requested_end,
                    "next_timestamp": next_timestamp,
                    "progress": progress,
                    "progress_fraction": progress,
                    "records": value("records"),
                    "records_staged": value("records_staged"),
                    "retries": value("retries") or 0,
                    "errors": list(errors)[:32],
                    "error_count": len(errors),
                    "updated_at": value("updated_at"),
                }
            )
        return rows

    def btc_research_data(
        self,
        *,
        catalog_data: Mapping[str, Any] | None = None,
        bootstrap_progress: list[Mapping[str, Any]] | None = None,
    ) -> dict[str, Any]:
        catalogs_data = catalog_data if isinstance(catalog_data, Mapping) else self.dataset_catalog_data()
        catalogs = catalogs_data.get("historical", [])
        btc_catalogs = [
            item
            for item in catalogs
            if str(item.get("market_type", "")).lower() == "crypto_spot"
            and str(item.get("instrument", "")).replace("/", "").replace("-", "").upper() == "BTCUSDT"
        ]
        coverage = catalogs_data.get("coverage_summary", {})
        btc_summary = coverage.get("btc", {}) if isinstance(coverage, Mapping) else {}
        reports: list[dict[str, Any]] = []
        if self.store is not None:
            for item in self.store.list_reports(limit=100, newest_first=True):
                report = item.get("report", {})
                if isinstance(report, Mapping) and report.get("kind") == "btc_historical_walk_forward":
                    reports.append({"report_id": item.get("report_id"), "report": report, "created_at": item.get("created_at")})
        latest = reports[0] if reports else None
        progress = bootstrap_progress if bootstrap_progress is not None else self._bootstrap_progress_rows()
        return {
            "available": bool(btc_catalogs or btc_summary),
            "catalog": btc_catalogs,
            "catalog_summary": btc_summary,
            "latest_report": latest,
            "reports": reports[:20],
            "bootstrap_progress": progress,
            "bootstrap_states": progress,
            "live_execution": False,
        }

    def polymarket_research_data(
        self,
        *,
        catalog_data: Mapping[str, Any] | None = None,
        current_data: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        catalogs = catalog_data if isinstance(catalog_data, Mapping) else self.dataset_catalog_data()
        historical = [
            item
            for item in catalogs.get("historical", [])
            if str(item.get("market_type", "")).lower() == "prediction"
        ]
        current = current_data if isinstance(current_data, Mapping) else self.prediction()
        aggregate = next((item for item in historical if item.get("dataset_id") == "Polymarket-historical"), None)
        coverage = catalogs.get("coverage_summary", {})
        poly_summary = coverage.get("polymarket", {}) if isinstance(coverage, Mapping) else {}
        historical_markets = int(poly_summary.get("historical_distinct_prediction_datasets", 0) or 0)
        historical_price_points = int(poly_summary.get("historical_price_points", 0) or 0)
        quality = poly_summary.get("research_quality") or poly_summary.get("quality") or (aggregate or {}).get("quality") or "PRICE_PROXY"
        order_book_available = bool(poly_summary.get("historical_order_book_available", False) or (aggregate or {}).get("metadata", {}).get("historical_order_book_available", False))
        return {
            "available": bool(historical or historical_markets or (isinstance(current, Mapping) and current.get("markets"))),
            "current": current,
            "historical_catalog": historical,
            "historical_aggregate": aggregate,
            "historical_markets": historical_markets or sum(1 for item in historical if str(item.get("dataset_id", "")).startswith("prediction:")),
            "historical_price_points": historical_price_points or int((aggregate or {}).get("row_count", 0)),
            "research_quality": str(quality),
            "historical_order_book_available": order_book_available,
            "coverage_summary": poly_summary,
            "live_execution": False,
        }

    def strategy_detail(self, candidate_id: str) -> dict[str, Any]:
        identifier = str(candidate_id).strip()
        if self.store is None:
            return {"candidate_id": identifier, "available": False, "error": "no persisted store", "live_execution": False}
        lifecycle = self.store.load_candidate_lifecycle(identifier)
        if not isinstance(lifecycle, Mapping):
            return {"candidate_id": identifier, "available": False, "error": "candidate not found", "live_execution": False}
        payload = lifecycle.get("payload", {})
        payload = dict(payload) if isinstance(payload, Mapping) else {}
        events = self.store.list_candidate_lifecycle_events(identifier, limit=100)
        report_rows: list[dict[str, Any]] = []
        for item in self.store.list_reports(limit=1000, newest_first=True):
            report = item.get("report", {})
            if not isinstance(report, Mapping):
                continue
            if identifier in json.dumps(_jsonable(report), sort_keys=True):
                report_rows.append(item)
        provenance = self._candidate_provenance(payload, identifier)
        return {
            "available": True,
            "candidate_id": identifier,
            "stage": lifecycle.get("stage"),
            **self._candidate_status_fields(lifecycle),
            "strategy": {
                "id": payload.get("strategy_id", payload.get("experiment_id", identifier)),
                "family": payload.get("experiment_family", payload.get("family")),
                "market_type": provenance["market_type"],
                "parameters": payload.get("parameters", payload.get("strategy_parameters", {})),
                "generation": payload.get("generation", 0),
                "parent_id": payload.get("parent_id"),
            },
            "provenance": provenance,
            "hypothesis": payload.get("hypothesis", payload.get("statement")),
            "historical": {key: payload.get(key) for key in payload if str(key).startswith(("historical_", "validation_", "holdout_", "walk_forward", "regime"))},
            "forward": {key: payload.get(key) for key in payload if str(key).startswith("forward_") or key in {"fills", "markets_observed", "markets_resolved"}},
            "rejection_reason": payload.get("rejection_reason"),
            "lineage": payload.get("lineage", payload.get("parent_id")),
            "lifecycle_events": events,
            "reports": report_rows[:20],
            "raw": lifecycle,
            "paper_only": True,
            "live_execution": False,
        }

    def _operator_catalog_summary(self) -> dict[str, Any]:
        """Build overview coverage from SQL aggregates plus two small pages."""
        aggregate_method = getattr(self.store, "dashboard_coverage_summary", None) if self.store is not None else None
        aggregate = aggregate_method() if callable(aggregate_method) else {}
        aggregate = dict(aggregate) if isinstance(aggregate, Mapping) else {}
        historical_page = self.paginate_dataset_catalog({"page": 1, "page_size": 10, "source_type": "HISTORICAL"})
        forward_page = self.paginate_dataset_catalog({"page": 1, "page_size": 10, "source_type": "FORWARD_COLLECTED"})
        historical = [item for item in historical_page.get("items", []) if isinstance(item, Mapping)]
        forward = [item for item in forward_page.get("items", []) if isinstance(item, Mapping)]
        historical_coverage = [
            {
                "dataset_id": item.get("dataset_id"),
                "instrument": item.get("instrument"),
                "timeframe": item.get("timeframe"),
                "start": item.get("start_timestamp"),
                "end": item.get("end_timestamp"),
                "rows": item.get("row_count", 0),
                "completeness": item.get("completeness", 0.0),
                "quality": item.get("quality"),
            }
            for item in historical
            if not str(item.get("dataset_id", "")).startswith("prediction:")
        ]
        forward_coverage = [
            {
                "dataset_id": item.get("dataset_id"),
                "instrument": item.get("instrument"),
                "timeframe": item.get("timeframe"),
                "start": item.get("start_timestamp"),
                "end": item.get("end_timestamp"),
                "rows": item.get("row_count", 0),
                "quality": item.get("quality"),
            }
            for item in forward
        ]
        return {
            "historical": historical,
            "forward": forward,
            "historical_count": int(aggregate.get("historical_count", historical_page.get("total", len(historical)))),
            "forward_count": int(aggregate.get("forward_count", forward_page.get("total", len(forward)))),
            "historical_rows": int(aggregate.get("historical_rows", sum(int(item.get("row_count", 0)) for item in historical))),
            "forward_rows": int(aggregate.get("forward_rows", sum(int(item.get("row_count", 0)) for item in forward))),
            "historical_coverage": historical_coverage,
            "forward_coverage": forward_coverage,
            "coverage_summary": aggregate,
            "live_execution": False,
        }

    def _operator_paper_summary(self) -> dict[str, Any]:
        """Return a small paper page and aggregate counts for the overview."""
        page = self.paginate_paper_records({"page": 1, "page_size": 10})
        counts = self.store.dashboard_summary() if self.store is not None else {}
        records = [item for item in page.get("items", []) if isinstance(item, Mapping)]
        states = [item for item in records if str(item.get("record_type", "")).lower() == "state"]
        portfolio: Mapping[str, Any] = {}
        if self.store is not None and callable(getattr(self.store, "list_paper_states", None)):
            try:
                candidate = self._paper_portfolio()
                if isinstance(candidate, Mapping):
                    portfolio = candidate
            except (AttributeError, TypeError, ValueError):
                portfolio = {}
        return {
            "paper_money": True,
            "live_execution": False,
            "states": states,
            "record_count": page.get("total", 0),
            "state_count": int(counts.get("paper_state", len(states))),
            "resolved_bets": int(portfolio.get("resolved_bets", counts.get("paper_bet_ledger", 0)) or 0),
            "total_equity": _number_or_zero(portfolio.get("total_equity", 0.0)),
            "total_pnl": _number_or_zero(portfolio.get("total_pnl", 0.0)),
            "win_rate": _number_or_zero(portfolio.get("win_rate", 0.0)),
            "expectancy": _number_or_zero(portfolio.get("expectancy", 0.0)),
        }

    def _latest_hermes_outcome(self) -> dict[str, Any] | None:
        page = self.paginate_research_queue(
            {"page": 1, "page_size": _MAX_PAGE_SIZE, "sort": "updated_at", "direction": "desc"}
        )
        rows = [
            item for item in page.get("items", [])
            if isinstance(item, Mapping) and _is_terminal_hermes_status(item.get("status"))
        ]
        if not rows:
            return None
        rows.sort(
            key=lambda item: parse_timestamp(item.get("time"))
            or datetime.min.replace(tzinfo=datetime.now().astimezone().tzinfo),
            reverse=True,
        )
        row = rows[0]
        return {
            "time": row.get("time"),
            "item_id": row.get("item_id"),
            "status": row.get("status"),
            "reason_code": row.get("reason_code"),
            "outcome_type": row.get("outcome_type"),
            "outcome_label": row.get("outcome_label"),
            "human_reason": row.get("human_reason"),
            "dataset_id": row.get("dataset_id"),
            "dataset_version": row.get("dataset_version"),
            "family": row.get("family"),
            "attempts": row.get("attempts", 0),
            "live_execution": False,
        }

    @staticmethod
    def _campaign_id_from_job(record: Mapping[str, Any]) -> str | None:
        payload = record.get("payload")
        payload = payload if isinstance(payload, Mapping) else {}
        value = str(payload.get("campaign_id") or "").strip()
        if value:
            return value
        job_name = str(record.get("job_name") or "").strip()
        if job_name.startswith(_CAMPAIGN_JOB_PREFIX):
            value = job_name[len(_CAMPAIGN_JOB_PREFIX) :].strip()
        return value or None

    def _campaign_job_records(self) -> list[Mapping[str, Any]]:
        if self.store is None:
            return []
        lister = getattr(self.store, "list_operator_jobs", None)
        if not callable(lister):
            return []
        try:
            raw = lister()
        except (AttributeError, TypeError, ValueError, sqlite3.Error):
            return []
        records = [
            item
            for item in (raw if isinstance(raw, (list, tuple)) else ())
            if isinstance(item, Mapping)
            and str(item.get("job_name") or "").startswith(_CAMPAIGN_JOB_PREFIX)
            and self._campaign_id_from_job(item) is not None
        ]
        records.sort(
            key=lambda item: (
                parse_timestamp(item.get("updated_at"))
                or datetime.min.replace(tzinfo=datetime.now().astimezone().tzinfo),
                str(item.get("job_name") or ""),
            ),
            reverse=True,
        )
        return records[:_CAMPAIGN_JOB_LIMIT]

    @staticmethod
    def _campaign_integer(value: Any, default: int = 0) -> int:
        if isinstance(value, bool):
            return default
        try:
            return max(0, int(value))
        except (TypeError, ValueError, OverflowError):
            return default

    def _campaign_progress_projection(self) -> dict[str, Any]:
        """Expose one bounded synthetic campaign state from durable jobs.

        Campaign qualification is intentionally kept in this research-only
        projection.  It is never folded into canary eligibility or any other
        real-readiness count.
        """
        empty = {
            "available": False,
            "campaign_id": None,
            "status": "NOT_INITIALIZED",
            "budget": {"limit": 0, "used": 0, "remaining": 0},
            "budget_limit": 0,
            "budget_used": 0,
            "budget_remaining": 0,
            "completed": 0,
            "remaining": 0,
            "completed_trials": 0,
            "remaining_trials": 0,
            "last_result": None,
            "qualified": [],
            "qualified_candidate_ids": [],
            "qualified_count": 0,
            "next_real_job": None,
            "waiting_prerequisite": None,
            "synthetic": True,
            "paper_only": True,
            "research_only": True,
            "real_readiness": False,
            "live_execution": False,
        }
        configured = self._configured("campaign_progress")
        if configured is None:
            configured = self._configured("campaign-progress")
        records = (
            [configured]
            if isinstance(configured, Mapping)
            and (
                configured.get("campaign_id") is not None
                or configured.get("campaign") is not None
            )
            else self._campaign_job_records()
        )
        if not records:
            return empty
        record = records[0]
        payload = record.get("payload")
        payload = dict(payload) if isinstance(payload, Mapping) else {}
        campaign_id = self._campaign_id_from_job(record)
        if campaign_id is None:
            campaign_value = payload.get("campaign")
            campaign_id = (
                str(campaign_value.get("campaign_id") or "").strip()
                if isinstance(campaign_value, Mapping)
                else None
            ) or None
        raw_status = payload.get("status", record.get("status"))
        status = str(raw_status or "UNKNOWN").strip().upper() or "UNKNOWN"
        budget_limit = self._campaign_integer(payload.get("budget_limit"), 0)
        budget_used = self._campaign_integer(payload.get("budget_used"), 0)
        budget_remaining = self._campaign_integer(
            payload.get("budget_remaining"),
            max(0, budget_limit - budget_used),
        )
        if budget_limit:
            budget_used = min(budget_used, budget_limit)
            budget_remaining = min(budget_remaining, max(0, budget_limit - budget_used))
        raw_trials = payload.get("trials")
        trials = [
            item
            for item in (raw_trials if isinstance(raw_trials, (list, tuple)) else ())
            if isinstance(item, Mapping)
        ][:64]
        completed = sum(
            1
            for item in trials
            if str(item.get("status") or "").strip().upper() in _CAMPAIGN_TRIAL_TERMINAL
        )
        if not trials:
            counts = payload.get("counts")
            counts = counts if isinstance(counts, Mapping) else {}
            completed = sum(
                self._campaign_integer(counts.get(name), 0)
                for name in (
                    "economic_rejection",
                    "data_insufficient",
                    "software_or_input_error",
                    "validation_qualified",
                    "final_assessment",
                )
            )
        remaining = max(0, len(trials) - completed) if trials else 0
        raw_qualified = payload.get("qualified_candidate_ids", payload.get("qualified", ()))
        if isinstance(raw_qualified, Mapping):
            raw_qualified = raw_qualified.get("candidate_ids", ())
        qualified = [
            str(item).strip()
            for item in (raw_qualified if isinstance(raw_qualified, (list, tuple, set, frozenset)) else ())
            if str(item).strip()
        ][:32]
        next_real_job = str(payload.get("next_real_job") or "").strip() or None
        waiting: dict[str, Any] | None = None
        if next_real_job and self.store is not None:
            getter = getattr(self.store, "get_operator_job", None)
            prerequisite = None
            if callable(getter):
                try:
                    prerequisite = getter(next_real_job)
                except (AttributeError, TypeError, ValueError, sqlite3.Error):
                    prerequisite = None
            prerequisite_payload = (
                prerequisite.get("payload")
                if isinstance(prerequisite, Mapping)
                and isinstance(prerequisite.get("payload"), Mapping)
                else {}
            )
            waiting = {
                "job_name": next_real_job,
                "status": str(
                    prerequisite.get("status", "UNKNOWN")
                    if isinstance(prerequisite, Mapping)
                    else "UNKNOWN"
                ).strip().upper()
                or "UNKNOWN",
                "producer_job": str(prerequisite_payload.get("producer_job") or "").strip() or None,
                "dataset_id": prerequisite_payload.get("dataset_id", payload.get("dataset_id")),
                "dataset_version": prerequisite_payload.get("dataset_version", payload.get("dataset_version")),
                "reason": prerequisite_payload.get("reason"),
                "next_attempt_at": prerequisite_payload.get("next_attempt_at"),
            }
        last_result = payload.get("last_result")
        if last_result is not None and not isinstance(last_result, Mapping):
            last_result = {"value": last_result}
        return {
            "available": True,
            "campaign_id": campaign_id,
            "status": status,
            "budget": {
                "limit": budget_limit,
                "used": budget_used,
                "remaining": budget_remaining,
            },
            "budget_limit": budget_limit,
            "budget_used": budget_used,
            "budget_remaining": budget_remaining,
            "completed": completed,
            "remaining": remaining,
            "completed_trials": completed,
            "remaining_trials": remaining,
            "last_result": _bounded_value(last_result),
            "qualified": qualified,
            "qualified_candidate_ids": qualified,
            "qualified_count": len(qualified),
            "next_real_job": next_real_job,
            "waiting_prerequisite": _bounded_value(waiting),
            "synthetic": True,
            "paper_only": True,
            "research_only": True,
            "real_readiness": False,
            "live_execution": False,
        }

    def _campaign_activity_rows(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for record in self._campaign_job_records():
            payload = record.get("payload")
            payload = payload if isinstance(payload, Mapping) else {}
            campaign_id = self._campaign_id_from_job(record)
            if not campaign_id:
                continue
            status = str(payload.get("status") or record.get("status") or "UNKNOWN").strip().upper()
            rows.append(
                {
                    "kind": "campaign",
                    "timestamp": parse_timestamp(
                        record.get("updated_at")
                        or payload.get("last_updated_at")
                        or payload.get("created_at")
                    ),
                    "message": f"Campaign {campaign_id} is {status.lower()}",
                    "details": {
                        "campaign_id": campaign_id,
                        "status": status,
                        "budget_used": self._campaign_integer(payload.get("budget_used"), 0),
                        "budget_remaining": self._campaign_integer(payload.get("budget_remaining"), 0),
                        "next_real_job": str(payload.get("next_real_job") or "").strip() or None,
                        "synthetic": True,
                        "paper_only": True,
                    },
                }
            )
        return rows

    def _research_progress_projection(
        self,
        *,
        aggregate: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Project the bounded automatic-research handoff from persisted rows.

        Queue results intentionally contain a compact candidate summary.  The
        lifecycle row remains authoritative for the current stage, so a
        compact ``null`` stage never erases a persisted ``SCHEMA_VALIDATED``
        stage.  This method only reads bounded queue/lifecycle/worker pages and
        persisted forward-test and paper-history projections.
        """
        empty: dict[str, Any] = {
            "available": False,
            "candidate_count": 0,
            "dataset_id": None,
            "dataset_version": None,
            "job_status": "NOT_INITIALIZED",
            "last_completion_at": None,
            "next_run_at": None,
            "candidate_stage": None,
            "samples_available": None,
            "samples_required": None,
            "trades_available": None,
            "trades_required": None,
            "forward_observations": 0,
            "blocker": None,
            "available_samples": None,
            "required_samples": None,
            "available_trades": None,
            "required_trades": None,
            "forward_observation_count": 0,
            "automatic_research_job": {
                "status": "NOT_INITIALIZED",
                "last_completion_at": None,
                "next_run_at": None,
                "schedule": None,
            },
            "dataset": {"id": None, "version": None},
            "status": None,
            "job": {
                "status": "NOT_INITIALIZED",
                "last_completion_at": None,
                "next_run_at": None,
                "schedule": None,
            },
            "candidate": {
                "stage": None,
                "samples": {"available": None, "required": None},
                "trades": {"available": None, "required": None},
                "forward_observations": 0,
                "blocker": None,
            },
            "collector": {
                "status": None,
                "last_completion_at": None,
                "next_run_at": None,
            },
            "live_execution": False,
        }
        if self.store is None:
            return empty

        aggregate = aggregate if isinstance(aggregate, Mapping) else {}

        def records(method_name: str, **kwargs: Any) -> list[Mapping[str, Any]]:
            method = getattr(self.store, method_name, None)
            if not callable(method):
                return []
            try:
                raw = method(**kwargs)
            except (AttributeError, TypeError, ValueError, sqlite3.Error):
                return []
            return [
                item
                for item in (raw if isinstance(raw, (list, tuple)) else ())
                if isinstance(item, Mapping)
            ]

        def mapping_call(method_name: str, *args: Any, **kwargs: Any) -> Mapping[str, Any]:
            method = getattr(self.store, method_name, None)
            if not callable(method):
                return {}
            try:
                raw = method(*args, **kwargs)
            except (AttributeError, TypeError, ValueError, sqlite3.Error):
                return {}
            return dict(raw) if isinstance(raw, Mapping) else {}

        def text(value: Any) -> str | None:
            if value is None:
                return None
            result = str(value).strip()
            return result or None

        def integer(value: Any) -> int | None:
            if isinstance(value, bool):
                return None
            try:
                result = int(value)
            except (TypeError, ValueError, OverflowError):
                return None
            return max(0, result)

        def first_text(sources: Sequence[Mapping[str, Any]], keys: Sequence[str]) -> str | None:
            for source in sources:
                for key in keys:
                    value = text(source.get(key))
                    if value is not None:
                        return value
            return None

        def first_integer(sources: Sequence[Mapping[str, Any]], keys: Sequence[str]) -> int | None:
            for source in sources:
                for key in keys:
                    value = integer(source.get(key))
                    if value is not None:
                        return value
            return None

        queue_items = records("list_research_items", limit=_RESEARCH_PROGRESS_LIMIT)
        latest_queue = aggregate.get("latest_queue_item")
        if isinstance(latest_queue, Mapping):
            latest_id = text(latest_queue.get("item_id"))
            if latest_id and not any(text(item.get("item_id")) == latest_id for item in queue_items):
                queue_items.append(latest_queue)
        queue_items.sort(
            key=lambda item: parse_timestamp(item.get("updated_at") or item.get("created_at"))
            or datetime.min.replace(tzinfo=datetime.now().astimezone().tzinfo),
            reverse=True,
        )
        lifecycle_records = self._bounded_candidate_lifecycle(limit=_RESEARCH_PROGRESS_LIMIT)
        lifecycle_records.sort(key=lambda item: text(item.get("candidate_id")) or "")
        lifecycle_records.sort(
            key=lambda item: parse_timestamp(item.get("updated_at"))
            or datetime.min.replace(tzinfo=datetime.now().astimezone().tzinfo),
            reverse=True,
        )
        lifecycle_by_id: dict[str, Mapping[str, Any]] = {}
        for item in lifecycle_records:
            identifier = text(item.get("candidate_id"))
            if identifier and identifier not in lifecycle_by_id:
                lifecycle_by_id[identifier] = item

        queue_candidate_rows: list[
            tuple[Mapping[str, Any], Mapping[str, Any], Mapping[str, Any]]
        ] = []
        queue_candidate_ids: set[str] = set()
        candidate_work = 0
        for queue_item in queue_items:
            queue_result = queue_item.get("result")
            queue_result = queue_result if isinstance(queue_result, Mapping) else {}
            compact = queue_result.get("candidate_results")
            compact_rows = compact if isinstance(compact, (list, tuple)) else ()
            if not compact_rows and text(queue_result.get("candidate_id")):
                compact_rows = (queue_result,)
            remaining = _RESEARCH_PROGRESS_LIMIT - candidate_work
            if remaining <= 0:
                break
            for compact_row in islice(compact_rows, remaining):
                candidate_work += 1
                if not isinstance(compact_row, Mapping):
                    continue
                candidate_id = text(compact_row.get("candidate_id"))
                if not candidate_id:
                    continue
                queue_candidate_ids.add(candidate_id)
                queue_candidate_rows.append((queue_item, queue_result, compact_row))

        # A queue result can contain several candidates, while a lifecycle
        # record is the durable current stage for one candidate.  Keep one
        # newest queue row per candidate, then rank the resulting identities
        # by authenticated current worker provenance before recency.
        queue_candidates_by_id: dict[
            str, tuple[Mapping[str, Any], Mapping[str, Any], Mapping[str, Any]]
        ] = {}
        for candidate_row in queue_candidate_rows:
            queue_item = candidate_row[0]
            candidate_id = text(candidate_row[2].get("candidate_id"))
            if not candidate_id:
                continue
            previous = queue_candidates_by_id.get(candidate_id)
            if previous is None:
                queue_candidates_by_id[candidate_id] = candidate_row
                continue
            previous_stamp = parse_timestamp(
                previous[0].get("updated_at") or previous[0].get("created_at")
            )
            current_stamp = parse_timestamp(
                queue_item.get("updated_at") or queue_item.get("created_at")
            )
            if current_stamp is not None and (
                previous_stamp is None
                or current_stamp > previous_stamp
                or (
                    current_stamp == previous_stamp
                    and (text(queue_item.get("item_id")) or "")
                    < (text(previous[0].get("item_id")) or "")
                )
            ):
                queue_candidates_by_id[candidate_id] = candidate_row

        candidate_entries: dict[str, dict[str, Any]] = {}
        for ordinal, (candidate_id, candidate_row) in enumerate(
            queue_candidates_by_id.items()
        ):
            candidate_entries[candidate_id] = {
                "candidate_id": candidate_id,
                "queue_item": candidate_row[0],
                "queue_result": candidate_row[1],
                "compact": candidate_row[2],
                "lifecycle": lifecycle_by_id.get(candidate_id, {}),
                "_ordinal": ordinal,
            }
        for candidate_id, lifecycle in lifecycle_by_id.items():
            candidate_entries.setdefault(
                candidate_id,
                {
                    "candidate_id": candidate_id,
                    "queue_item": {},
                    "queue_result": {},
                    "compact": {},
                    "lifecycle": lifecycle,
                    "_ordinal": len(candidate_entries),
                },
            )

        generated_kinds = frozenset(
            {"predeclared_starting_set", "legacy_scope_successor", "mutation_child"}
        )
        generated_schema = "axiom-generated-queue-v1"
        canonical_dataset_id = "Polymarket-historical"

        def generated_marker(source: Mapping[str, Any]) -> Mapping[str, Any] | None:
            provenance = source.get("provenance")
            if not isinstance(provenance, Mapping):
                return None
            internal = provenance.get("internal")
            if not isinstance(internal, Mapping):
                return None
            kind = text(internal.get("kind"))
            identity = text(internal.get("proposal_identity"))
            if (
                internal.get("schema") != generated_schema
                or internal.get("generated") is not True
                or kind not in generated_kinds
                or not identity
                or not identity.startswith("sha256:")
            ):
                return None
            return internal

        def candidate_sources(entry: Mapping[str, Any]) -> list[Mapping[str, Any]]:
            queue_item = entry.get("queue_item")
            queue_item = queue_item if isinstance(queue_item, Mapping) else {}
            queue_payload = queue_item.get("payload")
            queue_payload = queue_payload if isinstance(queue_payload, Mapping) else {}
            queue_result = entry.get("queue_result")
            queue_result = queue_result if isinstance(queue_result, Mapping) else {}
            compact = entry.get("compact")
            compact = compact if isinstance(compact, Mapping) else {}
            lifecycle = entry.get("lifecycle")
            lifecycle = lifecycle if isinstance(lifecycle, Mapping) else {}
            lifecycle_payload = lifecycle.get("payload")
            lifecycle_payload = (
                lifecycle_payload if isinstance(lifecycle_payload, Mapping) else {}
            )
            return [compact, lifecycle_payload, queue_payload, queue_result]

        def candidate_priority(entry: Mapping[str, Any]) -> int:
            sources = candidate_sources(entry)
            for source in sources:
                marker = generated_marker(source)
                if marker is None:
                    continue
                marker_dataset = text(marker.get("dataset_id"))
                dataset_id = marker_dataset
                if dataset_id is None:
                    dataset_id = first_text(
                        sources,
                        ("dataset_id", "dataset", "data_id"),
                    )
                if dataset_id and dataset_id.casefold() == canonical_dataset_id.casefold():
                    return 2
                return 1
            return 0

        def candidate_recency(entry: Mapping[str, Any]) -> datetime:
            values: list[datetime] = []
            queue_item = entry.get("queue_item")
            if isinstance(queue_item, Mapping):
                for key in ("updated_at", "created_at"):
                    stamp = parse_timestamp(queue_item.get(key))
                    if stamp is not None:
                        values.append(stamp)
            lifecycle = entry.get("lifecycle")
            if isinstance(lifecycle, Mapping):
                stamp = parse_timestamp(lifecycle.get("updated_at"))
                if stamp is not None:
                    values.append(stamp)
            return max(
                values,
                default=datetime.min.replace(tzinfo=datetime.now().astimezone().tzinfo),
            )

        ranked_candidates = sorted(
            candidate_entries.values(),
            key=lambda entry: (
                candidate_priority(entry),
                candidate_recency(entry),
                -int(entry.get("_ordinal", 0)),
            ),
            reverse=True,
        )
        selected_entry = ranked_candidates[0] if ranked_candidates else None
        candidate_id: str | None = (
            text(selected_entry.get("candidate_id")) if selected_entry else None
        )
        queue_item: Mapping[str, Any] = (
            selected_entry.get("queue_item", {}) if selected_entry else {}
        )
        queue_item = queue_item if isinstance(queue_item, Mapping) else {}
        queue_result: Mapping[str, Any] = (
            selected_entry.get("queue_result", {}) if selected_entry else {}
        )
        queue_result = queue_result if isinstance(queue_result, Mapping) else {}
        compact_candidate: Mapping[str, Any] = (
            selected_entry.get("compact", {}) if selected_entry else {}
        )
        compact_candidate = (
            compact_candidate if isinstance(compact_candidate, Mapping) else {}
        )
        lifecycle = lifecycle_by_id.get(candidate_id or "", {})
        lifecycle_payload = lifecycle.get("payload")
        lifecycle_payload = (
            lifecycle_payload if isinstance(lifecycle_payload, Mapping) else {}
        )
        queue_payload = queue_item.get("payload")
        queue_payload = queue_payload if isinstance(queue_payload, Mapping) else {}
        compact_result = compact_candidate
        selected_priority = candidate_priority(selected_entry) if selected_entry else 0
        selected_sources = (
            [lifecycle_payload, queue_payload, compact_result]
            if selected_priority >= 2
            else [compact_result, lifecycle_payload, queue_payload]
        )
        queue_result_candidate_id = text(queue_result.get("candidate_id"))
        queue_result_has_nested = isinstance(queue_result.get("candidate_results"), (list, tuple))
        if not queue_result_has_nested or queue_result_candidate_id == candidate_id:
            selected_sources.append(queue_result)
        result_sources = [source for source in selected_sources if isinstance(source, Mapping)]
        plans = [
            source.get("experiment_plan")
            for source in result_sources
            if isinstance(source.get("experiment_plan"), Mapping)
        ]
        plan = plans[0] if plans else {}
        minimum_checks: list[Mapping[str, Any]] = []
        evidence_sources: list[Mapping[str, Any]] = []
        for source in (*result_sources, plan):
            if not isinstance(source, Mapping):
                continue
            evidence_sources.append(source)
            for key in (
                "minimum_sample_check",
                "minimum_samples",
                "validation",
                "forward_validation",
                "forward_evidence",
                "historical_evidence",
            ):
                nested = source.get(key)
                if isinstance(nested, Mapping):
                    minimum_checks.append(nested)
                    evidence_sources.append(nested)
        evidence_sources.extend(minimum_checks)
        samples_available = first_integer(
            evidence_sources,
            (
                "count",
                "sample_count",
                "samples",
                "independent_samples",
                "validation_sample_count",
                "forward_sample_count",
                "raw_observations",
            ),
        )
        samples_required = first_integer(
            [plan, *minimum_checks, *result_sources],
            (
                "min_observations",
                "min_independent_samples",
                "required_samples",
                "min_samples",
                "minimum_samples",
                "required_validation_samples",
            ),
        )
        trades_available = first_integer(
            evidence_sources,
            (
                "trades",
                "trade_count",
                "filled_trades",
                "validation_filled_trades",
                "validation_trade_count",
                "validation_trades",
                "forward_trade_count",
            ),
        )
        trades_required = first_integer(
            [plan, *minimum_checks, *result_sources],
            (
                "min_trades",
                "required_trades",
                "minimum_trades",
                "required_validation_trades",
            ),
        )
        forward_tests = records("load_forward_tests", limit=_RESEARCH_FORWARD_TEST_LIMIT)
        forward_ids: list[str] = []
        for source in result_sources:
            for key in ("forward_test_id", "forward_test", "paper_observation_intent_id"):
                value = source.get(key)
                if isinstance(value, Mapping):
                    value = value.get("experiment_id") or value.get("id")
                value = text(value)
                if value and value not in forward_ids:
                    forward_ids.append(value)
        if candidate_id:
            for item in forward_tests:
                config = item.get("config")
                config = config if isinstance(config, Mapping) else {}
                if text(config.get("candidate_id")) == candidate_id:
                    identifier = text(item.get("experiment_id"))
                    if identifier and identifier not in forward_ids:
                        forward_ids.append(identifier)
            for identifier in (
                f"forward-candidate-{candidate_id}",
                f"observation-intent-{candidate_id}",
            ):
                if identifier not in forward_ids:
                    forward_ids.append(identifier)
        forward_observations: int | None = None
        history_method = getattr(self.store, "paper_history_counts", None)
        if callable(history_method):
            for experiment_id in forward_ids[:8]:
                history = mapping_call("paper_history_counts", experiment_id)
                if history:
                    count = integer(history.get("observations"))
                    if count is not None:
                        forward_observations = (
                            (forward_observations or 0) + count
                        )
        if forward_observations is None:
            observation_method = getattr(self.store, "list_paper_observations", None)
            if callable(observation_method):
                for experiment_id in forward_ids[:8]:
                    rows = records(
                        "list_paper_observations",
                        experiment_id=experiment_id,
                        limit=1000,
                    )
                    forward_observations = (forward_observations or 0) + len(rows)
        if forward_observations is None:
            forward_observations = first_integer(
                evidence_sources,
                ("forward_observations", "forward_observation_count", "observations"),
            )
        forward_observations = max(0, int(forward_observations or 0))

        lifecycle_stage = text(lifecycle.get("stage"))
        candidate_stage = lifecycle_stage or first_text(
            [compact_result, queue_result],
            ("stage", "candidate_stage"),
        )
        dataset_sources: list[Mapping[str, Any]] = []
        for source in result_sources:
            dataset_selector = source.get("dataset_selector")
            if isinstance(dataset_selector, Mapping):
                dataset_sources.append(dataset_selector)
            dataset_sources.append(source)
        dataset_id = first_text(dataset_sources, ("dataset_id", "dataset", "data_id"))
        dataset_version = first_text(
            dataset_sources,
            ("dataset_version", "data_version", "version"),
        )

        scheduler = mapping_call("get_scheduler_state", "hermes-control")
        exact_worker_method = getattr(self.store, "get_worker_state", None)
        research_worker: Mapping[str, Any] = {}
        if callable(exact_worker_method):
            # Prefer the durable queue boundary, retaining the local engine
            # name for stores written before the queue worker was introduced.
            research_worker = mapping_call("get_worker_state", "research-queue")
            if not research_worker:
                research_worker = mapping_call("get_worker_state", "research-engine")

        # This bounded page remains necessary for the other worker projections
        # (notably the collector) and for stores that predate get_worker_state.
        worker_rows = records("list_worker_states", limit=32)
        worker_map = {
            text(item.get("worker_name")): item
            for item in worker_rows
            if text(item.get("worker_name"))
        }
        if not research_worker:
            research_worker = worker_map.get("research-queue", {})
            if not research_worker:
                research_worker = worker_map.get("research-engine", {})
        research_payload = research_worker.get("payload")
        research_payload = research_payload if isinstance(research_payload, Mapping) else {}
        worker_cycle = research_payload.get("last_cycle")
        if not isinstance(worker_cycle, Mapping):
            worker_cycle = research_payload.get("cycle")
        worker_cycle = worker_cycle if isinstance(worker_cycle, Mapping) else {}

        def latest_timestamp(
            sources: Sequence[Mapping[str, Any]],
            keys: Sequence[str],
        ) -> str | None:
            values: list[tuple[datetime, str]] = []
            for source in sources:
                for key in keys:
                    value = source.get(key)
                    stamp = parse_timestamp(value)
                    rendered = text(value)
                    if stamp is not None and rendered is not None:
                        values.append((stamp, rendered))
            if not values:
                return None
            return max(values, key=lambda item: (item[0], item[1]))[1]

        scheduled_status = first_text([scheduler], ("status", "state"))
        worker_status = first_text(
            [research_worker, research_payload, worker_cycle],
            ("worker_status", "status", "state"),
        )
        job_status = (scheduled_status or worker_status or "NOT_INITIALIZED").upper()
        completion_keys = (
            "last_completion_at",
            "last_completed_at",
            "last_tick_completed_at",
            "completed_at",
            "completion_at",
            "cycle_ended_at",
            "ended_at",
        )
        worker_sources = [worker_cycle, research_payload, research_worker]
        last_completion = latest_timestamp(worker_sources, completion_keys)
        if last_completion is None:
            # A worker heartbeat is the only safe fallback when the worker
            # persisted a completed idle cycle without a dedicated completion
            # field.  It remains evidence of the queue boundary, including a
            # cycle whose claimed count is zero.
            last_completion = latest_timestamp(
                [research_payload, research_worker],
                ("heartbeat_at", "worker_heartbeat_at"),
            )
        if last_completion is None:
            last_completion = latest_timestamp(
                [scheduler],
                (*completion_keys, "last_run_at"),
            )
        if last_completion is None:
            terminal_queue_items = [
                item
                for item in queue_items
                if _is_terminal_hermes_status(item.get("status"))
            ]
            terminal_queue_items.sort(
                key=lambda item: (
                    parse_timestamp(item.get("updated_at") or item.get("created_at"))
                    or datetime.min.replace(tzinfo=datetime.now().astimezone().tzinfo),
                    text(item.get("item_id")) or "",
                ),
                reverse=True,
            )
            if terminal_queue_items:
                last_completion = first_text(
                    terminal_queue_items[0:1],
                    ("updated_at", "created_at"),
                )
        if last_completion is None:
            # Keep the old queue-item fallback only for stores that expose no
            # worker/scheduler completion evidence at all.
            last_completion = first_text(
                [queue_item] if queue_item else (),
                ("last_completion_at", "last_completed_at"),
            )
        collector_state = mapping_call("get_collector_state", "polymarket")
        collector_worker = worker_map.get("polymarket-collector", {})
        collector_payload = collector_worker.get("payload")
        collector_payload = collector_payload if isinstance(collector_payload, Mapping) else {}
        cycles = records("list_collection_cycles", collector_name="polymarket", limit=4)
        latest_cycle = cycles[0] if cycles else {}
        latest_cycle_payload = latest_cycle.get("payload")
        latest_cycle_payload = latest_cycle_payload if isinstance(latest_cycle_payload, Mapping) else {}
        collector_completion = first_text(
            [
                collector_state,
                collector_payload,
                latest_cycle,
                latest_cycle_payload,
            ],
            ("last_cycle_ended_at", "last_successful_cycle", "ended_at"),
        )
        collector_next = first_text(
            [collector_state, collector_payload],
            ("next_scheduled_collection_at", "next_run_at"),
        )
        next_run = first_text(
            [scheduler, research_payload, collector_state, collector_payload],
            ("next_run_at", "next_scheduled_collection_at"),
        )
        schedule = first_text([scheduler], ("trigger", "schedule"))
        if schedule is None:
            schedule = "after_each_collection" if scheduler else None
        candidate_count = min(
            _RESEARCH_PROGRESS_LIMIT,
            len(queue_candidate_ids | set(lifecycle_by_id)),
        )
        blocker = first_text(
            (
                [lifecycle_payload, compact_result, queue_result]
                if selected_priority >= 2
                else [compact_result, queue_result, lifecycle_payload]
            ),
            ("reason_code", "blocker"),
        )
        if blocker and blocker.upper() == "NO_SUPPORTED_EDGE":
            blocker = None
        has_candidate = candidate_id is not None
        candidate_blocker = blocker if has_candidate else None
        candidate_status = candidate_blocker if candidate_blocker else None
        result = {
            "available": has_candidate,
            "candidate_count": candidate_count,
            "dataset_id": dataset_id,
            "dataset_version": dataset_version,
            "job_status": job_status,
            "last_completion_at": last_completion,
            "next_run_at": next_run,
            "candidate_stage": candidate_stage,
            "samples_available": samples_available,
            "samples_required": samples_required,
            "trades_available": trades_available,
            "trades_required": trades_required,
            "forward_observations": forward_observations,
            "blocker": candidate_blocker,
            "available_samples": samples_available,
            "required_samples": samples_required,
            "available_trades": trades_available,
            "required_trades": trades_required,
            "forward_observation_count": forward_observations,
            "automatic_research_job": {
                "status": job_status,
                "last_completion_at": last_completion,
                "next_run_at": next_run,
                "schedule": schedule,
            },
            "dataset": {
                "id": dataset_id,
                "version": dataset_version,
            },
            "status": candidate_status,
            "job": {
                "status": job_status,
                "last_completion_at": last_completion,
                "next_run_at": next_run,
                "schedule": schedule,
            },
            "candidate": {
                "stage": candidate_stage,
                "samples": {
                    "available": samples_available,
                    "required": samples_required,
                },
                "trades": {
                    "available": trades_available,
                    "required": trades_required,
                },
                "forward_observations": forward_observations,
                "blocker": candidate_blocker,
            },
            "collector": {
                "status": text(collector_worker.get("status")) if collector_worker else None,
                "last_completion_at": collector_completion,
                "next_run_at": collector_next,
            },
            "live_execution": False,
        }
        return result

    def market_scope_funnel_data(self) -> dict[str, Any]:
        """Project the persisted market-scope handoff without recomputation.

        The aggregate is written by the qualification/resolution workers.  A
        dashboard GET may only read that bounded aggregate; it must not scan
        candidate lifecycle rows or resolve current markets itself.
        """
        empty = _empty_market_scope_funnel()
        if self.store is None:
            return empty
        method = getattr(self.store, "market_scope_resolution_funnel", None)
        if not callable(method):
            return empty
        try:
            raw = method(limit=1000)
        except TypeError:
            try:
                raw = method()
            except (AttributeError, TypeError, ValueError, sqlite3.Error):
                return empty
        except (AttributeError, TypeError, ValueError, sqlite3.Error):
            return empty
        if not isinstance(raw, Mapping):
            return empty
        def scope_bound(value: Any, depth: int = 0) -> Any:
            if depth >= 8:
                return "<truncated>"
            if isinstance(value, Mapping):
                return {
                    str(key): scope_bound(child, depth + 1)
                    for key, child in list(value.items())[:128]
                }
            if isinstance(value, (list, tuple, set, frozenset)):
                return [scope_bound(child, depth + 1) for child in list(value)[:1000]]
            return _jsonable(value)

        bounded = scope_bound(raw)
        bounded = bounded if isinstance(bounded, Mapping) else {}
        result = dict(bounded)
        raw_stage_values = bounded.get("stages", bounded.get("funnel", {}))
        raw_stages = raw_stage_values if isinstance(raw_stage_values, Mapping) else {}
        raw_counts = bounded.get("stage_counts", {})
        raw_counts = raw_counts if isinstance(raw_counts, Mapping) else {}
        raw_blockers = bounded.get("blocker_counts", {})
        raw_blockers = raw_blockers if isinstance(raw_blockers, Mapping) else {}
        raw_timestamps = bounded.get("timestamps", {})
        raw_timestamps = raw_timestamps if isinstance(raw_timestamps, Mapping) else {}
        exact_blockers: dict[str, int] = {}
        raw_blocker_values = bounded.get("blockers", [])
        if isinstance(raw_blocker_values, (list, tuple)):
            result["resolution_blockers"] = list(raw_blocker_values)[:1000]
            for item in raw_blocker_values[:1000]:
                if not isinstance(item, Mapping):
                    continue
                reason = str(item.get("reason") or "").strip()
                if reason:
                    try:
                        exact_blockers[reason] = max(0, int(item.get("count", 0) or 0))
                    except (TypeError, ValueError):
                        exact_blockers[reason] = 0
        if isinstance(raw_stage_values, (list, tuple)):
            result["resolution_stages"] = list(raw_stage_values)[:1000]
        if isinstance(bounded.get("items"), (list, tuple)):
            result["resolution_items"] = list(bounded["items"])[:1000]
        result["resolution_status_counts"] = dict(
            bounded.get("status_counts", bounded.get("statuses", {}))
            if isinstance(bounded.get("status_counts", bounded.get("statuses", {})), Mapping)
            else {}
        )
        result["resolution_reason_counts"] = dict(
            bounded.get("reason_counts", bounded.get("reasons", {}))
            if isinstance(bounded.get("reason_counts", bounded.get("reasons", {})), Mapping)
            else {}
        )

        def aliases(stage: str) -> tuple[str, ...]:
            return (stage, stage.upper(), stage.replace("_", "-"), stage.replace("_", " "))

        def lookup(source: Mapping[str, Any], stage: str) -> Any:
            for key in aliases(stage):
                if key in source:
                    return source[key]
            return None

        def count_value(value: Any) -> int:
            try:
                return max(0, int(value or 0))
            except (TypeError, ValueError):
                return 0

        stages: dict[str, dict[str, Any]] = {}
        stage_counts: dict[str, int] = {}
        for stage in _MARKET_SCOPE_FUNNEL_STAGES:
            entry = lookup(raw_stages, stage)
            if isinstance(entry, Mapping):
                count = count_value(entry.get("count", entry.get("total", lookup(raw_counts, stage))))
                blockers = entry.get("blocker_counts", entry.get("blockers", {}))
                blockers = blockers if isinstance(blockers, Mapping) else {}
                timestamps = entry.get("timestamps", {})
                timestamps = timestamps if isinstance(timestamps, Mapping) else {}
                latest = entry.get("latest_at", entry.get("updated_at", timestamps.get("latest")))
                earliest = entry.get("earliest_at", timestamps.get("earliest"))
            else:
                count = count_value(entry if entry is not None else lookup(raw_counts, stage))
                blockers = lookup(raw_blockers, stage)
                blockers = blockers if isinstance(blockers, Mapping) else {}
                latest = None
                earliest = None
            stage_counts[stage] = count
            stages[stage] = {
                "count": count,
                "blocker_counts": dict(blockers),
                "timestamps": {"latest": latest, "earliest": earliest},
            }
        result["stages"] = stages
        result["stage_counts"] = stage_counts
        result["blocker_counts"] = dict(raw_blockers or exact_blockers)
        result["timestamps"] = {
            **dict(raw_timestamps),
            "latest": raw_timestamps.get("latest") or bounded.get("latest_resolved_at"),
            "as_of": raw_timestamps.get("as_of") or bounded.get("latest_resolved_at"),
        }
        result.setdefault("as_of", bounded.get("latest_resolved_at"))
        result["available"] = bool(
            bounded.get("available", False)
            or bounded.get("total", 0)
            or bounded.get("resolution_count", 0)
            or any(stage_counts.values())
        )
        result["live_execution"] = False
        result["storage_backed"] = True
        return result
    def _research_order_funnel(
        self,
        scope_funnel: Mapping[str, Any],
        aggregate: Mapping[str, Any] | None,
        canary_status: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        """Join bounded persisted evidence into the nine-stage handoff.

        This only joins already-persisted summaries.  It never turns a
        missing readiness, order, or fill record into a successful stage.
        """
        result = dict(scope_funnel)
        stage_values = scope_funnel.get("stage_counts", {})
        stage_values = stage_values if isinstance(stage_values, Mapping) else {}
        direct_stages = scope_funnel.get("stages", {})
        direct_stages = direct_stages if isinstance(direct_stages, Mapping) else {}
        if any(stage_values.get(name, 0) for name in _MARKET_SCOPE_FUNNEL_STAGES):
            return result
        aggregate = aggregate if isinstance(aggregate, Mapping) else {}
        candidate_stages = aggregate.get("candidate_stages", {})
        candidate_stages = candidate_stages if isinstance(candidate_stages, Mapping) else {}
        def count_value(value: Any) -> int:
            try:
                return max(0, int(value or 0))
            except (TypeError, ValueError):
                return 0

        normalized_candidates = {
            str(key).upper(): count_value(value)
            for key, value in candidate_stages.items()
            if str(key).strip()
        }
        historical = sum(
            normalized_candidates.get(key, 0)
            for key in ("FROZEN", "PAPER_FORWARD", "PAPER_PROMOTABLE")
        )
        evaluated = sum(
            normalized_candidates.get(key, 0)
            for key in ("BACKTESTED", "VALIDATED", "ROBUSTNESS_CHECKED", "FROZEN", "PAPER_FORWARD", "PAPER_PROMOTABLE")
        )
        resolution_statuses = scope_funnel.get("resolution_status_counts", {})
        resolution_statuses = resolution_statuses if isinstance(resolution_statuses, Mapping) else {}
        statuses = {
            str(key).upper(): count_value(value)
            for key, value in resolution_statuses.items()
        }
        resolution_reasons = scope_funnel.get("resolution_reason_counts", {})
        resolution_reasons = resolution_reasons if isinstance(resolution_reasons, Mapping) else {}
        valid_scope = min(historical, sum(value for key, value in statuses.items() if key != "INVALID_POLICY"))
        matching = min(historical, statuses.get("MATCHED", 0) + statuses.get("PARTIAL", 0))
        prior_scope = min(historical, valid_scope)
        reason_counts = {
            str(key): count_value(value)
            for key, value in resolution_reasons.items()
        }
        canary_status = canary_status if isinstance(canary_status, Mapping) else {}
        fresh_source = next(
            (
                source.get(key)
                for source in (canary_status, aggregate)
                for key in ("fresh_complete_inputs_count", "complete_inputs_count", "fresh_inputs")
                if source.get(key) is not None
            ),
            None,
        )
        fresh = min(matching, count_value(fresh_source)) if fresh_source is not None else 0
        evaluated_source = next(
            (
                aggregate.get(key)
                for key in ("strategy_evaluated", "strategies_evaluated", "evaluated")
                if aggregate.get(key) is not None
            ),
            None,
        )
        if evaluated_source is not None:
            evaluated = count_value(evaluated_source)
        strategy = min(fresh, evaluated)
        autonomous = canary_status.get("autonomous", {})
        autonomous = autonomous if isinstance(autonomous, Mapping) else {}
        ready_signal = min(
            strategy,
            count_value(autonomous.get("signals_generated", canary_status.get("signals_generated", 0))),
        )
        readiness_status = str(
            canary_status.get("readiness_snapshot_status")
            or canary_status.get("readiness_status")
            or ""
        ).upper()
        execution_feasible = ready_signal if readiness_status == "CURRENT" else 0
        aggregate_counts = aggregate.get("counts", {})
        aggregate_counts = aggregate_counts if isinstance(aggregate_counts, Mapping) else {}

        def aggregate_value(keys: tuple[str, ...]) -> Any:
            for source in (aggregate, aggregate_counts):
                for key in keys:
                    if source.get(key) is not None:
                        return source.get(key)
            return 0

        submitted_source = aggregate_value(("orders_submitted", "submitted", "paper_submitted", "paper_execution_events"))
        filled_source = aggregate_value(("orders_filled", "filled", "paper_filled", "fills", "paper_bet_ledger"))
        submitted = min(execution_feasible, count_value(submitted_source))
        filled = min(submitted, count_value(filled_source))
        timestamps = scope_funnel.get("timestamps", {})
        timestamps = dict(timestamps) if isinstance(timestamps, Mapping) else {}
        latest_scope = scope_funnel.get("latest_resolved_at") or timestamps.get("latest")

        def stage(name: str, count: int, blockers: Mapping[str, Any], latest: Any = None) -> dict[str, Any]:
            existing = direct_stages.get(name)
            if isinstance(existing, Mapping) and existing.get("blocker_counts"):
                blockers = existing["blocker_counts"]
            return {
                "count": max(0, int(count)),
                "blocker_counts": {
                    str(key): max(0, int(value or 0))
                    for key, value in blockers.items()
                },
                "timestamps": {"latest": latest, "earliest": latest},
            }

        stage_rows = {
            "historically_qualified": stage(
                "historically_qualified",
                historical,
                {} if historical else {"NO_PERSISTED_HISTORICAL_QUALIFICATION": 0},
            ),
            "valid_frozen_scope": stage(
                "valid_frozen_scope",
                prior_scope,
                {**({"INVALID_POLICY": statuses.get("INVALID_POLICY", 0)} if statuses.get("INVALID_POLICY") else {}),
                 "NO_PERSISTED_FROZEN_SCOPE": historical - prior_scope},
                latest_scope,
            ),
            "matching_current_markets": stage(
                "matching_current_markets",
                matching,
                {**{key: value for key, value in reason_counts.items()},
                 "NO_MATCHING_CURRENT_MARKETS": prior_scope - matching},
                latest_scope,
            ),
            "fresh_complete_inputs": stage(
                "fresh_complete_inputs",
                fresh,
                {"NO_PERSISTED_FRESH_COMPLETE_INPUTS": matching - fresh},
            ),
            "strategy_evaluated": stage(
                "strategy_evaluated",
                strategy,
                {"NO_PERSISTED_STRATEGY_EVALUATION": historical - strategy},
            ),
            "ready_signal": stage(
                "ready_signal",
                ready_signal,
                {"NO_PERSISTED_READY_SIGNAL": strategy - ready_signal},
                canary_status.get("ranking_timestamp"),
            ),
            "execution_feasible": stage(
                "execution_feasible",
                execution_feasible,
                {"READINESS_SNAPSHOT_NOT_CURRENT": ready_signal - execution_feasible},
                canary_status.get("readiness_snapshot_updated_at"),
            ),
            "submitted": stage(
                "submitted",
                submitted,
                {"NO_PERSISTED_SUBMISSION": execution_feasible - submitted},
            ),
            "filled": stage(
                "filled",
                filled,
                {"NO_PERSISTED_FILL": submitted - filled},
            ),
        }
        result["stages"] = stage_rows
        result["stage_counts"] = {name: row["count"] for name, row in stage_rows.items()}
        result["timestamps"] = {
            **timestamps,
            "latest": timestamps.get("latest") or latest_scope,
            "as_of": timestamps.get("as_of") or scope_funnel.get("as_of") or latest_scope,
        }
        result["available"] = bool(
            scope_funnel.get("available")
            or any(row["count"] for row in stage_rows.values())
        )
        result["storage_backed"] = True
        return result

    def _required_forward_evidence(
        self,
        market_scope_funnel: Mapping[str, Any] | None = None,
        *,
        health_sources: tuple[Mapping[str, Any] | None, ...] = (),
        candidate_ids: list[str] | tuple[str, ...] | None = None,
    ) -> dict[str, Any]:
        """Expose persisted scope/health state without authority work."""
        funnel = (
            dict(market_scope_funnel)
            if isinstance(market_scope_funnel, Mapping)
            else self.market_scope_funnel_data()
        )
        health = self._persisted_forward_health(*health_sources)
        stage_counts = funnel.get("stage_counts", {})
        stage_counts = stage_counts if isinstance(stage_counts, Mapping) else {}
        funnel_blockers = funnel.get("blocker_counts", {})
        funnel_blockers = funnel_blockers if isinstance(funnel_blockers, Mapping) else {}
        timestamps = funnel.get("timestamps", {})
        timestamps = timestamps if isinstance(timestamps, Mapping) else {}

        def bounded_list(value: Any, *, limit: int = 1000) -> list[Any]:
            if not isinstance(value, (list, tuple, set, frozenset)):
                return []
            return list(value)[:limit]

        def health_list(name: str) -> list[Any]:
            return bounded_list(health.get(name))

        candidate_bound_markets = health_list("candidate_bound_markets")
        scheduled = health_list("scheduled")
        fresh = health_list("fresh")
        stale = health_list("stale")
        missing = health_list("missing")
        diagnostics = health.get("market_diagnostics", health.get("diagnostics", []))
        diagnostics = bounded_list(diagnostics, limit=100)
        diagnostics = [
            dict(_bounded_value(item))
            for item in diagnostics
            if isinstance(item, Mapping) and str(item.get("market_id") or "").strip()
        ]
        references = health.get("candidate_references", {})
        references = references if isinstance(references, Mapping) else {}
        candidate_references = {
            str(market_id): bounded_list(values, limit=32)
            for market_id, values in list(references.items())[:1000]
            if isinstance(values, (list, tuple, set, frozenset))
        }
        for diagnostic in diagnostics:
            market_id = str(diagnostic["market_id"])
            refs = diagnostic.get("candidate_references")
            if market_id not in candidate_references and isinstance(
                refs, (list, tuple, set, frozenset)
            ):
                candidate_references[market_id] = bounded_list(refs, limit=32)
            if diagnostic.get("candidate_bound") and market_id not in candidate_bound_markets:
                candidate_bound_markets.append(market_id)

        def first_health(*names: str) -> Any:
            for name in names:
                value = health.get(name)
                if value is not None and value != "":
                    return value
            return None

        required_count = first_health("required_market_count")
        if required_count is None:
            required_count = len(candidate_bound_markets)
        try:
            required_market_count = max(0, int(required_count or 0))
        except (TypeError, ValueError):
            required_market_count = len(candidate_bound_markets)
        latest_source = first_health("newest_required_source_timestamp", "newest_required_source")
        earliest_source = first_health("oldest_required_source_timestamp", "oldest_required_source")
        latest_observed = first_health("newest_required_observed_at", "newest_required_observed")
        earliest_observed = first_health("oldest_required_observed_at", "oldest_required_observed")
        latest_source = latest_source or timestamps.get("latest")
        earliest_source = earliest_source or timestamps.get("earliest")
        latest_observed = latest_observed or timestamps.get("latest")
        earliest_observed = earliest_observed or timestamps.get("earliest")
        unresolved = bounded_list(health.get("unresolved_candidates"))
        if "unresolved_candidates" not in health:
            unresolved = bounded_list(funnel.get("unresolved_candidates"))
        closed = bounded_list(health.get("closed_candidates"))
        if "closed_candidates" not in health:
            closed = bounded_list(funnel.get("closed_candidates"))
        reason_code = first_health("reason_code")
        if reason_code is None:
            reason_code = (
                "CANDIDATE_FORWARD_MARKET_UNRESOLVED"
                if unresolved
                else None if funnel.get("available") else "NO_PERSISTED_SCOPE_RESOLUTION"
            )
        reason_display = reason_code or first_health("reason_display")
        grade = first_health("grade")
        if grade is None:
            grade = "CURRENT" if funnel.get("available") else "UNKNOWN"
        blocker_counts = health.get("blocker_counts", funnel_blockers)
        if not isinstance(blocker_counts, Mapping):
            blocker_counts = funnel_blockers
        blocker_counts = dict(blocker_counts or {})
        result = {
            "candidate_bound_markets": candidate_bound_markets,
            "scheduled": scheduled,
            "fresh": fresh,
            "stale": stale,
            "missing": missing,
            "newest_required_source_timestamp": latest_source,
            "oldest_required_source_timestamp": earliest_source,
            "newest_required_observed_at": latest_observed,
            "oldest_required_observed_at": earliest_observed,
            "newest_required_source": latest_source,
            "oldest_required_source": earliest_source,
            "newest_required_observed": latest_observed,
            "oldest_required_observed": earliest_observed,
            "grade": grade,
            "grade_scope": first_health("grade_scope") or "persisted_market_scope_resolution",
            "reason_code": reason_code,
            "reason_display": reason_display,
            "candidate_references": candidate_references,
            "market_diagnostics": diagnostics,
            "diagnostics": diagnostics,
            "required_market_count": required_market_count,
            "unresolved_candidates": unresolved,
            "closed_candidates": closed,
            "as_of": first_health("as_of") or funnel.get("as_of") or timestamps.get("as_of"),
            "market_scope_funnel": funnel,
            "blocker_counts": blocker_counts,
        }
        return result

    def overview_summary(
        self,
        *,
        canary_snapshot: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Return the bounded overview payload; list views stay lazy."""
        configured = self._configured("overview-summary")
        if configured is not None:
            result = dict(configured) if isinstance(configured, Mapping) else {"value": configured}
            if isinstance(result, dict):
                if "market_scope_funnel" not in result:
                    result["market_scope_funnel"] = self.market_scope_funnel_data()
                if "campaign_progress" not in result:
                    result["campaign_progress"] = self._campaign_progress_projection()
            return result
        market_scope_funnel = self.market_scope_funnel_data()
        if self.store is None or not callable(getattr(self.store, "dashboard_overview_summary", None)):
            candidate_records = self._bounded_candidate_lifecycle()
            candidate_ids = [
                str(item.get("candidate_id") or "").strip()
                for item in candidate_records
                if str(item.get("candidate_id") or "").strip()
            ]
            market_scope_funnel = self._research_order_funnel(
                market_scope_funnel,
                {},
                {},
            )
            research_progress = self._research_progress_projection(aggregate={})
            campaign_progress = self._campaign_progress_projection()
            latest_candidates = [
                self._latest_candidate_projection(item)
                for item in candidate_records[:_LATEST_CANDIDATE_LIMIT]
            ]
            return {
                "available": False,
                "components": [],
                "research_cards": {},
                "coverage": {},
                "latest_activity": [],
                "latest_candidates": latest_candidates,
                "candidates": latest_candidates,
                "forward_evidence": self._required_forward_evidence(
                    market_scope_funnel,
                    candidate_ids=candidate_ids,
                ),
                "research_progress": research_progress,
                "market_scope_funnel": market_scope_funnel,
                "campaign_progress": campaign_progress,
                "signal_scan_reason_counts": {},
                "live_execution": False,
            }
        aggregate = self.store.dashboard_overview_summary(activity_limit=8)
        research_progress = self._research_progress_projection(aggregate=aggregate)
        campaign_progress = self._campaign_progress_projection()
        campaign_activity = self._campaign_activity_rows()
        latest_activity = list(aggregate.get("latest_activity", []))
        if campaign_activity:
            latest_activity = sorted(
                latest_activity + campaign_activity,
                key=lambda item: parse_timestamp(item.get("timestamp")) or datetime.min.replace(
                    tzinfo=datetime.now().astimezone().tzinfo
                ),
                reverse=True,
            )[:8]
        research_feed = (
            self.store.research_feed_status()
            if callable(getattr(self.store, "research_feed_status", None))
            else {}
        )
        counts = aggregate.get("counts", {}) if isinstance(aggregate, Mapping) else {}
        catalog = aggregate.get("catalog", {}) if isinstance(aggregate, Mapping) else {}
        stages = {
            str(key): int(value)
            for key, value in (aggregate.get("candidate_stages", {}) if isinstance(aggregate, Mapping) else {}).items()
        }
        queue_statuses = {
            str(key).upper(): int(value)
            for key, value in (aggregate.get("queue_statuses", {}) if isinstance(aggregate, Mapping) else {}).items()
        }
        bootstrap_statuses = {
            str(key).upper(): int(value)
            for key, value in (aggregate.get("bootstrap_statuses", {}) if isinstance(aggregate, Mapping) else {}).items()
        }
        workers = [
            item for item in (aggregate.get("workers", []) if isinstance(aggregate, Mapping) else [])
            if isinstance(item, Mapping)
        ]
        worker_map = {str(item.get("worker_name")): item for item in workers}
        collector_worker = worker_map.get("polymarket-collector", {})
        collector_worker_payload = collector_worker.get("payload", {}) if isinstance(collector_worker, Mapping) else {}
        collector_worker_payload = collector_worker_payload if isinstance(collector_worker_payload, Mapping) else {}
        paper_worker = worker_map.get("paper-engine", {})
        paper_worker_payload = paper_worker.get("payload", {}) if isinstance(paper_worker, Mapping) else {}
        paper_worker_payload = paper_worker_payload if isinstance(paper_worker_payload, Mapping) else {}
        research_worker = worker_map.get("research-engine", {})
        research_worker_payload = research_worker.get("payload", {}) if isinstance(research_worker, Mapping) else {}
        research_worker_payload = research_worker_payload if isinstance(research_worker_payload, Mapping) else {}
        health_worker = worker_map.get("health-monitor", {})
        health_payload = health_worker.get("payload", {}) if isinstance(health_worker, Mapping) else {}
        health_payload = health_payload if isinstance(health_payload, Mapping) else {}
        health = self._health_for_operator(health_payload)
        if not health:
            # No persisted health monitor row is evidence of unknown/stale
            # readiness, not permission to rebuild health synchronously.
            health = {
                "status": "STALE",
                "grade": "UNKNOWN",
                "grade_scope": "collector_health",
                "reason_code": "HEALTH_UNAVAILABLE",
                "reasons": [],
                "source_type": "FORWARD_COLLECTED",
                "historical_maturity_grade": "UNKNOWN",
                "historical_error_count": 0,
            }
        (
            health_grade,
            health_grade_scope,
            health_reason_code,
            health_reasons,
            health_reason,
            health_source_type,
            historical_maturity_grade,
            historical_error_count,
        ) = self._operator_health_fields(health)
        health = {
            **health,
            "grade": health_grade,
            "grade_scope": health_grade_scope,
            "reason_code": health_reason_code,
            "reasons": health_reasons,
            "source_type": health_source_type,
            "historical_maturity_grade": historical_maturity_grade,
            "historical_error_count": historical_error_count,
        }
        collector_state = self.store.get_collector_state("polymarket") or {}
        collector_state = collector_state if isinstance(collector_state, Mapping) else {}
        forward_catalog_count = (
            int(catalog.get("forward_collected", {}).get("datasets", 0) or 0)
            if isinstance(catalog.get("forward_collected"), Mapping)
            else 0
        )

        def health_value(*keys: str) -> Any:
            return _nested_value(
                health,
                health_payload,
                collector_worker_payload,
                collector_state,
                keys=keys,
            )

        collector_detail = {
            "grade": health_grade,
            "forward_catalogs": forward_catalog_count,
            "grade_scope": health_grade_scope,
            "reason_code": health_reason_code,
            "reason": health_reason,
            "reasons": _bounded_value(health_reasons),
            "source_type": health_source_type,
            "historical_maturity_grade": historical_maturity_grade,
            "historical_error_count": historical_error_count,
            "collection_errors": health_value("collection_errors") or 0,
            "top_failure_codes": _bounded_value(health_value("top_failure_codes") or []),
            "stale_market_count": health_value("stale_market_count")
            or len(health_value("stale_markets") or []),
            "gap_count": health_value("gap_count") or len(health_value("gaps") or []),
            "configured_interval_seconds": health_value("configured_interval_seconds") or 60.0,
            "effective_collection_cadence_seconds": health_value("effective_collection_cadence_seconds"),
            "stale_after_seconds": health_value("stale_after_seconds") or 180.0,
            "last_cycle_duration_seconds": health_value("last_cycle_duration_seconds"),
            "last_cycle_started_at": health_value("last_cycle_started_at"),
            "last_cycle_ended_at": health_value("last_cycle_ended_at"),
            "markets_attempted": health_value(
                "last_cycle_markets_attempted", "markets_attempted"
            )
            or 0,
            "markets_successful": health_value(
                "last_cycle_markets_successful", "markets_successful"
            )
            or 0,
            "markets_failed": health_value(
                "last_cycle_markets_failed", "markets_failed"
            )
            or 0,
            "last_cycle_markets_attempted": health_value(
                "last_cycle_markets_attempted", "markets_attempted"
            )
            or 0,
            "last_cycle_markets_successful": health_value(
                "last_cycle_markets_successful", "markets_successful"
            )
            or 0,
            "last_cycle_markets_failed": health_value(
                "last_cycle_markets_failed", "markets_failed"
            )
            or 0,
            "scheduled_market_count": (
                len(collector_state.get("scheduled_market_ids", []))
                if isinstance(collector_state.get("scheduled_market_ids"), (list, tuple))
                else health_value("scheduled_market_count") or 0
            ),
            "last_successful_cycle": health_value(
                "last_successful_cycle", "last_successful_collection_at"
            ),
            "next_scheduled_collection_at": health_value("next_scheduled_collection_at"),
            "worker_heartbeat_at": health_value("worker_heartbeat_at"),
        }
        crypto_ready = (
            int(catalog.get("historical", {}).get("datasets", 0) or 0) > 0
            if isinstance(catalog.get("historical"), Mapping)
            else False
        )
        if canary_snapshot is None and self._canary_service is not None:
            raw_canary_status, canary_report = _canary_status_report(
                self._canary_service
            )
            canary_status = _canary_status_projection(raw_canary_status)
            if canary_report:
                canary_status["status_report"] = canary_report
        elif canary_snapshot is None:
            canary_report = {}
            canary_status = _canary_status_projection({})
            if canary_report:
                canary_status["status_report"] = canary_report
        else:
            canary_report = {}
            canary_status = _canary_status_projection(canary_snapshot)
        worker_section = (
            canary_report.get("worker", {})
            if isinstance(canary_report, Mapping)
            else {}
        )
        autonomous = dict(canary_status.get("autonomous") or {})
        if canary_report:
            worker_section = canary_report.get("worker", {})
            autonomous = dict(canary_status.get("autonomous") or {})
            if isinstance(worker_section, Mapping):
                worker_auto = worker_section.get("autonomous")
                autonomous = _patch_non_missing_values(autonomous, worker_auto)
                worker_fields = {
                    key: worker_section.get(key)
                    for key in (
                        "last_tick_at",
                        "last_tick_started_at",
                        "last_tick_completed_at",
                        "last_successful_tick",
                        "last_error_code",
                        "consecutive_failures",
                        "next_retry_at",
                        "candidates_evaluated",
                        "signals_generated",
                        "orders_attempted",
                        "next_decision",
                        "blocker",
                        "last_signal_id",
                        "worker_status",
                        *_CANARY_AUTONOMOUS_FIELDS,
                    )
                    if key in worker_section
                }
                autonomous = _patch_non_missing_values(autonomous, worker_fields)
        canary_status = _patch_non_missing_values(
            canary_status,
            _canary_autonomous_projection(autonomous),
        )
        raw_blocker = _canonical_blocker(
            worker_section if isinstance(worker_section, Mapping) else None,
            autonomous,
            canary_status,
        )
        blocker = _normalized_canary_blocker(
            raw_blocker,
            canary_report.get("control") if isinstance(canary_report, Mapping) else None,
            canary_report.get("authoritative_control") if isinstance(canary_report, Mapping) else None,
            canary_report.get("readiness") if isinstance(canary_report, Mapping) else None,
            canary_status,
        )
        signal_scan_reason_counts = _signal_scan_reason_counts(
            canary_status,
            autonomous,
            worker_section if isinstance(worker_section, Mapping) else None,
            canary_report,
        )
        canary_status["blocker"] = blocker
        canary_status["last_cycle_blocker"] = raw_blocker
        canary_status["signal_scan_reason_counts"] = signal_scan_reason_counts
        autonomous["blocker"] = blocker
        autonomous["last_cycle_blocker"] = raw_blocker
        autonomous["signal_scan_reason_counts"] = signal_scan_reason_counts
        canary_status["autonomous"] = autonomous
        latest_signal = _signal_projection(canary_status.get("latest_signal"))
        canary_status["latest_signal"] = latest_signal
        candidate_records = self._bounded_candidate_lifecycle()
        candidate_ids = [
            str(item.get("candidate_id") or "").strip()
            for item in candidate_records
            if str(item.get("candidate_id") or "").strip()
        ]
        market_scope_funnel = self._research_order_funnel(
            market_scope_funnel,
            aggregate,
            canary_status,
        )
        forward_evidence = self._required_forward_evidence(
            market_scope_funnel,
            health_sources=(
                health,
                health_payload,
                collector_worker_payload,
                collector_state,
            ),
            candidate_ids=candidate_ids,
        )
        overview_now = ensure_utc(self.clock())

        def worker_state(name: str, default: str = "NOT INITIALIZED") -> str:
            item = worker_map.get(name, {})
            status = str(item.get("status") or "").upper()
            if status in {"RUNNING", "DEGRADED"}:
                heartbeat = parse_timestamp(item.get("heartbeat_at"))
                payload = item.get("payload") if isinstance(item.get("payload"), Mapping) else {}
                try:
                    stale_after = float(payload.get("stale_after_seconds", 300.0))
                except (TypeError, ValueError):
                    stale_after = 300.0
                if (
                    not math.isfinite(stale_after)
                    or stale_after <= 0
                    or heartbeat is None
                    or max(0.0, (overview_now - ensure_utc(heartbeat)).total_seconds()) > stale_after
                ):
                    status = "STALE"
            return status or default

        latest_queue = aggregate.get("latest_queue_item") if isinstance(aggregate, Mapping) else None
        latest_outcome = None
        if isinstance(latest_queue, Mapping) and _is_terminal_hermes_status(
            latest_queue.get("status")
        ):
            outcome = _hermes_row(latest_queue)
            latest_outcome = {
                "time": outcome.get("time"),
                "item_id": outcome.get("item_id"),
                "status": outcome.get("status"),
                "reason_code": outcome.get("reason_code"),
                "outcome_type": outcome.get("outcome_type"),
                "outcome_label": outcome.get("outcome_label"),
                "human_reason": outcome.get("human_reason"),
                "dataset_id": outcome.get("dataset_id"),
                "dataset_version": outcome.get("dataset_version"),
                "family": outcome.get("family"),
            }

        aggregate_candidates = (
            aggregate.get("latest_candidates")
            if isinstance(aggregate, Mapping)
            else None
        )
        if isinstance(aggregate_candidates, (list, tuple)) and aggregate_candidates:
            latest_candidates = [
                _bounded_value(item)
                for item in aggregate_candidates[:_LATEST_CANDIDATE_LIMIT]
                if isinstance(item, Mapping)
            ]
        else:
            latest_candidates = [
                self._latest_candidate_projection(item)
                for item in candidate_records[:_LATEST_CANDIDATE_LIMIT]
            ]
        historical = catalog.get("historical", {}) if isinstance(catalog, Mapping) else {}
        forward = catalog.get("forward_collected", {}) if isinstance(catalog, Mapping) else {}
        bootstrap_progress = self._operator_bootstrap_progress()
        candidate_canary_count = (
            canary_status.get("eligible_count")
            if isinstance(canary_status, Mapping)
            else None
        )
        paper_detail = dict(paper_worker_payload)
        paper_detail["paper_forward_candidates"] = stages.get("PAPER_FORWARD", 0) + stages.get("PAPER_PROMOTABLE", 0)
        paper_detail["status"] = (
            f"{paper_detail['paper_forward_candidates']} PAPER_FORWARD candidate(s); "
            f"{paper_worker_payload.get('processed_candidates', 0)} processed this pass; "
            f"{paper_worker_payload.get('remaining_candidates', 0)} remaining"
        )
        research_detail = dict(research_worker_payload)
        research_detail["status"] = (
            f"{research_worker_payload.get('passes', 0)} pass(es); "
            f"{research_worker_payload.get('queue_items_processed', 0)} research item(s) processed"
        )
        if health_grade in {"A", "OK", "HEALTHY"} or (
            not health_grade and forward_catalog_count
        ):
            collector_default_state = "READY"
        elif health_grade == "UNKNOWN" and not forward_catalog_count:
            collector_default_state = "NOT INITIALIZED"
        elif health_grade and health_grade != "F" or forward_catalog_count:
            collector_default_state = "DEGRADED"
        else:
            collector_default_state = "NOT INITIALIZED"
        components = [
            {
                "name": "AXIOM NODE",
                "state": worker_state("axiom-node"),
                "detail": {"reason": health_reason if worker_state("axiom-node") in {"DEGRADED", "STALE"} else None},
            },
            {
                "name": "POLYMARKET COLLECTOR",
                "state": worker_state("polymarket-collector", collector_default_state),
                "detail": collector_detail,
            },
            {
                "name": "CRYPTO DATA",
                "state": "READY" if crypto_ready else ("UPDATING" if bootstrap_statuses.get("RUNNING") else "NOT INITIALIZED"),
                "detail": {"bootstrap_statuses": bootstrap_statuses},
            },
            {
                "name": "INTERNAL RESEARCH QUEUE",
                "state": worker_state("research-queue", "READY" if queue_statuses else "NOT INITIALIZED"),
                "detail": {"queue_statuses": queue_statuses, "latest_outcome": latest_outcome},
            },
            {
                "name": "PAPER ENGINE",
                "state": worker_state("paper-engine", "ACTIVE" if counts.get("paper_state", 0) else "NOT INITIALIZED"),
                "detail": paper_detail,
            },
            {
                "name": "RESEARCH ENGINE",
                "state": worker_state("research-engine", worker_state("research-queue", "NOT INITIALIZED")),
                "detail": research_detail,
            },
        ]
        return {
            "available": True,
            "title": "AXIOM / operator research console",
            "live_trading": {"status": "Disabled", "enabled": False},
            "paper_risk_engine": {"status": "Active", "enabled": True},
            "dataset_health": health,
            "health_grade": health_grade,
            "health_status": str(health.get("status") or ("STALE" if health_grade == "UNKNOWN" else "CURRENT")).upper(),
            "readiness_status": str(canary_status.get("readiness_snapshot_status") or "STALE").upper(),
            "grade_scope": health_grade_scope,
            "reason_code": health_reason_code,
            "reasons": health_reasons,
            "window_start": health.get("window_start"),
            "window_end": health.get("window_end"),
            "source_type": health_source_type,
            "historical_maturity_grade": historical_maturity_grade,
            "historical_error_count": historical_error_count,
            "health_reason_code": health_reason_code,
            "health_reasons": health_reasons,
            "degrading_reason": health_reason,
            "health_window": {
                "start": health.get("window_start"),
                "end": health.get("window_end"),
                "seconds": health.get("window_seconds"),
            },
            "components": components,
            "research_cards": {
                "experiments_run": counts.get("experiments", 0),
                "active_hypotheses": queue_statuses.get("PENDING", 0) + queue_statuses.get("TESTING", 0),
                "candidates_alive": sum(value for key, value in stages.items() if key != "REJECTED"),
                "candidate_rejected": stages.get("REJECTED", 0),
                "research_rejected": queue_statuses.get("REJECTED", 0) + queue_statuses.get("FAILED", 0),
                "canary_eligible": candidate_canary_count,
                "paper_forward": stages.get("PAPER_FORWARD", 0) + stages.get("PAPER_PROMOTABLE", 0),
                "paper_promotable": stages.get("PAPER_PROMOTABLE", 0),
            },
            "coverage": {
                "historical_count": historical.get("datasets", 0) if isinstance(historical, Mapping) else 0,
                "historical_rows": historical.get("rows", 0) if isinstance(historical, Mapping) else 0,
                "forward_count": forward.get("datasets", 0) if isinstance(forward, Mapping) else 0,
                "forward_rows": forward.get("rows", 0) if isinstance(forward, Mapping) else 0,
                "logical_rows": aggregate.get("logical_rows", {}),
            },
            "btc": {
                "available": bool(crypto_ready or bootstrap_progress),
                "catalog": [],
                "catalog_summary": {},
                "latest_report": None,
                "reports": [],
                "bootstrap_progress": bootstrap_progress,
                "bootstrap_states": bootstrap_progress,
            },
            "lifecycle_funnel": stages,
            "activity": latest_activity,
            "latest_activity": latest_activity,
            "latest_candidates": latest_candidates,
            "candidates": latest_candidates,
            "canary": canary_status,
            "canary_signal": latest_signal,
            "forward_evidence": forward_evidence,
            "research_progress": research_progress,
            "market_scope_funnel": market_scope_funnel,
            "campaign_progress": campaign_progress,
            "signal_scan_reason_counts": signal_scan_reason_counts,
            "candidate_status": {
                "canary_eligible": candidate_canary_count,
                "rankable": canary_status.get("rankable_count") if isinstance(canary_status, Mapping) else None,
                "paper_forward": stages.get("PAPER_FORWARD", 0) + stages.get("PAPER_PROMOTABLE", 0),
                "paper_promotable": stages.get("PAPER_PROMOTABLE", 0),
            },
            "hermes": {"statuses": queue_statuses, "latest_outcome": latest_outcome},
            "hermes_latest_outcome": latest_outcome,
            "collector_health": collector_detail,
            "research_feed": research_feed,
            "counts": counts,
            "paper_summary": {
                "telemetry_records": counts.get("paper_observations", 0)
                + counts.get("paper_execution_events", 0)
                + counts.get("paper_bet_ledger", 0),
                "candidate_portfolios": 0,
            },
            "paper_only": True,
            "live_execution": False,
        }

    def canary_data(self) -> dict[str, Any]:
        settings_snapshot = self.risk_settings_data()
        canary_report: dict[str, Any] = {}
        if self.store is None:
            canary: Mapping[str, Any] = {
                "production_live_trading": "UNKNOWN",
                "micro_live_canary": "UNKNOWN",
                "display_state": "UNKNOWN",
                "control_state": "UNKNOWN",
                "candidate": None,
                "winner_id": None,
                "winner_rank": None,
                "winner_score": None,
                "selection_reason": None,
                "eligibility_raw_count": None,
                "eligible_count": None,
                "rankable_raw_count": None,
                "rankable_count": None,
                "ranking_run_id": None,
                "ranking_timestamp": None,
                "selection_status": "UNKNOWN",
                "selection_valid": None,
                "selection_invalidation_reason": None,
                "selected_candidate": None,
                "last_selected_candidate": None,
                "readiness_snapshot_status": "STALE",
                "readiness_snapshot_stale": True,
                "readiness_snapshot_reason": "NO_PERSISTED_SNAPSHOT",
                "readiness_snapshot_updated_at": None,
                "historical_data_integrity": "UNKNOWN",
                "historical_execution_fidelity": "UNKNOWN",
                "current_execution_evidence": "UNKNOWN",
                "risk_envelope": {},
                "risk_limits": {},
                "execution_event_count": None,
                "autonomous": {
                    "enabled": False,
                    "next_decision": "UNKNOWN",
                    "blocker": "AUTONOMOUS_CONTROL_UNKNOWN",
                },
                "trades": [],
                "latest_signal": None,
                "live_execution": False,
            }
        else:
            service = self._canary_service
            if service is None:
                canary = {}
            else:
                raw_canary, canary_report = _canary_status_report(service)
                canary = raw_canary
        effective_settings = (
            settings_snapshot.get("effective_limits")
            if isinstance(settings_snapshot, Mapping)
            else None
        )
        if isinstance(effective_settings, Mapping):
            canary = dict(canary)
            canary["risk_limits"] = dict(effective_settings)
            canary["risk_envelope"] = dict(effective_settings)
        canary = _canary_status_projection(canary)
        if canary_report:
            canary["status_report"] = canary_report
            for name in ("control", "readiness", "worker", "execution"):
                value = canary_report.get(name)
                if isinstance(value, Mapping):
                    canary[name] = dict(value)
        else:
            canary_report = {}
        if canary_report:
            readiness = canary_report.get("readiness", {})
            execution = canary_report.get("execution", {})
            control = canary_report.get("control", canary_report.get("authoritative_control", {}))
            readiness = readiness if isinstance(readiness, Mapping) else {}
            execution = execution if isinstance(execution, Mapping) else {}
            persisted_risk = (
                readiness.get("risk_envelope")
                or control.get("risk_envelope")
                or {}
            )
            canary.setdefault("risk_envelope", persisted_risk)
            canary.setdefault(
                "risk_limits",
                readiness.get("risk_limits")
                or control.get("risk_limits")
                or canary.get("risk_envelope", {}),
            )
            canary.setdefault("trades", execution.get("trades", []))
            canary.setdefault(
                "execution_event_count",
                execution.get("event_count", execution.get("real_execution_events", 0)),
            )
            canary.setdefault(
                "real_execution_events",
                execution.get("real_execution_events", execution.get("event_count", 0)),
            )
        signal = _signal_projection(canary.get("latest_signal"))
        autonomous = dict(canary.get("autonomous") or {})
        worker_section = (
            canary_report.get("worker", {}) if isinstance(canary_report, Mapping) else {}
        )
        if isinstance(worker_section, Mapping):
            autonomous = _patch_non_missing_values(
                autonomous,
                worker_section.get("autonomous"),
            )
            worker_fields = {
                key: worker_section.get(key)
                for key in (
                    "last_tick_at",
                    "last_tick_started_at",
                    "last_tick_completed_at",
                    "last_successful_tick",
                    "last_error_code",
                    "consecutive_failures",
                    "next_retry_at",
                    "candidates_evaluated",
                    "signals_generated",
                    "orders_attempted",
                    "next_decision",
                    "blocker",
                    "last_signal_id",
                    "worker_status",
                    *_CANARY_AUTONOMOUS_FIELDS,
                )
                if key in worker_section
            }
            autonomous = _patch_non_missing_values(autonomous, worker_fields)
        autonomous = _patch_non_missing_values(
            autonomous,
            _canary_autonomous_projection(worker_section, canary),
        )
        autonomous.setdefault("rank", canary.get("winner_rank"))
        autonomous.setdefault("score", canary.get("winner_score"))
        autonomous.setdefault("selection_reason", canary.get("selection_reason"))
        autonomous.setdefault("next_decision", canary.get("next_decision"))
        canary["autonomous"] = autonomous
        canary = _patch_non_missing_values(
            canary,
            _canary_autonomous_projection(autonomous),
        )
        raw_blocker = _canonical_blocker(
            worker_section if isinstance(worker_section, Mapping) else None,
            autonomous,
            canary,
        )
        blocker = _normalized_canary_blocker(
            raw_blocker,
            canary_report.get("control") if isinstance(canary_report, Mapping) else None,
            canary_report.get("authoritative_control") if isinstance(canary_report, Mapping) else None,
            canary_report.get("readiness") if isinstance(canary_report, Mapping) else None,
            canary,
        )
        control_state = _canary_control_state(
            canary_report.get("control") if isinstance(canary_report, Mapping) else None,
            canary_report.get("authoritative_control") if isinstance(canary_report, Mapping) else None,
            canary_report.get("readiness") if isinstance(canary_report, Mapping) else None,
            canary,
        )
        signal_scan_reason_counts = _signal_scan_reason_counts(
            canary,
            autonomous,
            worker_section if isinstance(worker_section, Mapping) else None,
            canary_report,
        )
        market_scope_funnel = self.market_scope_funnel_data()
        forward_evidence = self._required_forward_evidence(market_scope_funnel)
        canary["blocker"] = blocker
        canary["last_cycle_blocker"] = raw_blocker
        canary["signal_scan_reason_counts"] = signal_scan_reason_counts
        autonomous["blocker"] = blocker
        autonomous["last_cycle_blocker"] = raw_blocker
        autonomous["signal_scan_reason_counts"] = signal_scan_reason_counts
        canary["autonomous"] = autonomous
        canary["latest_signal"] = signal
        canary["control_state"] = control_state
        canary.setdefault("display_state", canary.get("control_state"))
        canary.setdefault(
            "production_live_trading",
            "ENABLED" if str(canary.get("control_state")).upper() in {"ARMED", "LIVE"} else "DISABLED",
        )
        if "selected_winner" not in canary:
            selected = canary.get("selected_candidate")
            historical = canary.get("last_selected_candidate")
            if selected is not None or historical is not None:
                canary["selected_winner"] = {
                    "candidate_id": selected or historical,
                    "selected_candidate": selected,
                    "last_selected_candidate": historical,
                    "selection_status": canary.get("selection_status", "NONE"),
                    "selection_valid": canary.get("selection_valid", False),
                    "selection_invalidation_reason": canary.get("selection_invalidation_reason"),
                }
        if str(canary.get("display_state") or "").upper() == "DISARMED":
            canary["display_state"] = "DISABLED"
        eligible_count = canary["eligible_count"]
        rankable_count = canary["rankable_count"]
        raw_execution_events = canary.get("real_execution_events")
        try:
            execution_events = (
                None
                if raw_execution_events is None
                else int(raw_execution_events)
            )
        except (TypeError, ValueError):
            execution_events = None
        candidate_status = {
            "eligibility_raw_count": canary["eligibility_raw_count"],
            "canary_eligible": eligible_count,
            "eligible_count": eligible_count,
            "rankable_raw_count": canary["rankable_raw_count"],
            "rankable": rankable_count,
            "rankable_count": rankable_count,
        }
        persisted_connectivity: Any = None
        if self.store is not None:
            try:
                persisted_connectivity = self.store.get_operator_config(
                    CANARY_CONNECTIVITY_CONFIG_KEY,
                    None,
                )
            except (AttributeError, TypeError, ValueError, sqlite3.Error):
                persisted_connectivity = None
        connectivity = _stored_connectivity_projection(persisted_connectivity)
        credential_type = canary_module.CredentialStore
        try:
            credentials = credential_type.cached_projection(
                allow_environment=False,
                persisted={
                    "canary": canary,
                    "status_report": canary_report,
                    "connectivity": connectivity,
                },
            )
        except BaseException:
            credentials = None
        if not isinstance(credentials, Mapping):
            credentials = {
                "configured": None,
                "status": "NOT CHECKED",
                "secret_values_exposed": False,
            }
        else:
            credentials = dict(credentials)
        credentials["secret_values_exposed"] = False
        canary["credentials"] = dict(credentials)
        canary["risk_settings"] = settings_snapshot
        projection = {
            "canary": canary,
            "risk_settings": settings_snapshot,
            "status_report": canary_report,
            "control": (
                canary_report.get("control", canary_report.get("authoritative_control", {}))
                if isinstance(canary_report, Mapping)
                else {}
            ),
            "readiness": (
                canary_report.get("readiness", {})
                if isinstance(canary_report, Mapping)
                else {}
            ),
            "worker": (
                canary_report.get("worker", {})
                if isinstance(canary_report, Mapping)
                else {}
            ),
            "execution": (
                canary_report.get("execution", {})
                if isinstance(canary_report, Mapping)
                else {}
            ),
            "autonomous_canary": autonomous,
            "canary_signal": signal,
            "connectivity": connectivity,
            "research_cards": {
                "canary_eligible": eligible_count,
                "eligible_count": eligible_count,
                "eligibility_raw_count": canary["eligibility_raw_count"],
                "rankable_count": rankable_count,
                "rankable_raw_count": canary["rankable_raw_count"],
            },
            "candidate_status": candidate_status,
            "credentials": credentials,
            "real_execution_events": execution_events,
            "live_execution": False,
            "forward_evidence": forward_evidence,
            "market_scope_funnel": market_scope_funnel,
            "signal_scan_reason_counts": signal_scan_reason_counts,
        }
        projection.update({name: canary[name] for name in _CANARY_STATUS_FIELDS})
        return projection

    def _operator_control_data(self) -> dict[str, Any]:
        """Merge responsive controls into the bounded persisted overview."""
        controls = self.control.status() if self.control is not None else {}
        controls = dict(controls) if isinstance(controls, Mapping) else {}
        control_canary = controls.get("canary")
        control_snapshot = (
            control_canary.get("status", control_canary)
            if isinstance(control_canary, Mapping)
            else None
        )
        try:
            persisted = self.overview_summary(
                canary_snapshot=control_snapshot
                if isinstance(control_snapshot, Mapping)
                else None
            )
        except (AttributeError, TypeError, ValueError, sqlite3.Error):
            persisted = {}
        result = dict(persisted) if isinstance(persisted, Mapping) else {}

        # ``overview_summary`` is authoritative for every research surface.
        # Controls are additive and must never replace persisted cards, rows,
        # candidate selections, lifecycle values, or canary evidence.
        result["operator_controls"] = controls
        result["autonomous_canary_worker"] = controls.get(
            "autonomous_canary_worker", {}
        )
        canary = result.get("canary")
        if isinstance(canary, Mapping):
            result.setdefault(
                "real_execution_events",
                canary.get("real_execution_events", canary.get("execution_event_count", 0)),
            )
            result.setdefault("autonomous_canary", canary.get("autonomous", {}))
            result["autonomous_canary"] = _patch_non_missing_values(
                result.get("autonomous_canary"),
                _canary_autonomous_projection(
                    canary,
                    control_canary if isinstance(control_canary, Mapping) else None,
                    controls.get("autonomous_canary_worker"),
                ),
            )
        result.update(
            _canary_autonomous_projection(
                result,
                result.get("autonomous_canary"),
                result.get("autonomous_canary_worker"),
            )
        )
        # ``OperatorControlPlane.status`` already returns the bounded
        # credential metadata projection.  Preserve unknown/not-checked
        # values instead of coercing a missing value to ``False``.
        raw_credentials = controls.get("credentials")
        configured = (
            raw_credentials.get("configured")
            if isinstance(raw_credentials, Mapping)
            and isinstance(raw_credentials.get("configured"), bool)
            else None
        )
        raw_status = (
            str(raw_credentials.get("status") or "").strip().upper()
            if isinstance(raw_credentials, Mapping)
            else ""
        )
        if raw_status not in {"CONFIGURED", "NOT CONFIGURED", "UNKNOWN", "NOT CHECKED"}:
            raw_status = (
                "CONFIGURED"
                if configured is True
                else "NOT CONFIGURED"
                if configured is False
                else "NOT CHECKED"
            )
        result["credentials"] = {
            "configured": configured,
            "status": raw_status,
            "secret_values_exposed": False,
        }
        # Keep control-only status available without colliding with the
        # persisted ``raw`` research payload.
        result.setdefault("control_status", controls)
        return result

    def operator_data(self) -> dict[str, Any]:
        configured = self._configured("operator")
        if configured is not None:
            result = dict(configured) if isinstance(configured, Mapping) else {"value": configured}
            result["autonomous_canary"] = _patch_non_missing_values(
                result.get("autonomous_canary"),
                _canary_autonomous_projection(result),
            )
            result.update(_canary_autonomous_projection(result.get("autonomous_canary")))
            if self.control is not None:
                result["operator_controls"] = self.control.status()
            return result
        if self.store is not None:
            return self._operator_control_data()
        operator_controls: Mapping[str, Any] = {}
        catalogs = self._operator_catalog_summary()
        overview_coverage = catalogs
        summary = self.research_summary_data()
        status = self.status_data() if self.store is not None else {"status": "not_started", "workers": []}
        workers = status.get("workers", []) if isinstance(status, Mapping) else []
        worker_map = {
            str(item.get("worker_name")): item
            for item in workers
            if isinstance(item, Mapping)
        }
        bootstrap_states = self.store.list_dataset_bootstrap_states(limit=20) if self.store is not None else []
        btc_states = [item for item in bootstrap_states if str(item.get("dataset_id", "")).startswith("BTCUSDT-")]
        forward_states = [item for item in bootstrap_states if str(item.get("dataset_id", "")).startswith("Polymarket")]
        stages = {
            "IDEA": 0,
            "SCHEMA_VALIDATED": 0,
            "BACKTESTED": 0,
            "VALIDATED": 0,
            "ROBUSTNESS_CHECKED": 0,
            "FROZEN": 0,
            "PAPER_FORWARD": 0,
            "PAPER_PROMOTABLE": 0,
            "REJECTED": 0,
        }
        crypto_research = self.paginate_crypto_research({"page": 1, "page_size": 50})
        latest_hermes_outcome = self._latest_hermes_outcome()
        funnel = self.store.candidate_lifecycle_funnel() if self.store is not None else {}
        for key, value in funnel.items():
            stages[str(key)] = int(value)
        candidate_page = self.paginate_candidate_lifecycle({"page": 1, "page_size": 10})
        candidate_rows = [
            {**dict(item), **self._candidate_row(item)}
            for item in candidate_page.get("items", [])
            if isinstance(item, Mapping)
        ]
        candidate_status = {
            "canary_eligible": None,
            # Paper status is lifecycle-derived and intentionally remains
            # separate from the persisted canary eligibility binding.
            "paper_forward": stages["PAPER_FORWARD"] + stages["PAPER_PROMOTABLE"],
            "paper_promotable": stages["PAPER_PROMOTABLE"],
        }
        activity_page = self.paginate_research_activity({"page": 1, "page_size": 10})
        activity_rows = activity_page.get("items", []) if isinstance(activity_page, Mapping) else []
        count = self.store.dashboard_summary() if self.store is not None else {}
        hermes = summary.get("hermes", {}) if isinstance(summary, Mapping) else {}
        paper = self._operator_paper_summary()
        polymarket_page = self.paginate_polymarket_markets({"page": 1, "page_size": 10})
        polymarket_current = {
            "markets": list(polymarket_page.get("items", [])),
            "available": bool(polymarket_page.get("total", 0)),
            "live_execution": False,
        }

        def component(name: str, value: str, detail: Any = None) -> dict[str, Any]:
            return {"name": name, "state": value, "detail": detail}

        node_state = str(status.get("status", "not_started")).lower() if isinstance(status, Mapping) else "not_started"
        node_label = {
            "running": "RUNNING",
            "idle": "READY",
            "stopped": "STOPPED",
            "not_started": "NOT INITIALIZED",
            "stale": "DEGRADED",
            "degraded": "DEGRADED",
        }.get(node_state, node_state.upper())
        node_reason = status.get("detail") if isinstance(status, Mapping) else None
        if not node_reason and isinstance(status, Mapping):
            unhealthy = [
                item for item in workers
                if str(item.get("status", "")).lower() in {"degraded", "stale", "error"}
            ]
            for item in unhealthy:
                payload = item.get("payload", {})
                if isinstance(payload, Mapping):
                    node_reason = _nested_value(
                        payload,
                        keys=("last_error", "error", "reason", "degraded_reason", "latest_reason", "failure_reason"),
                    )
                if node_reason:
                    break
            if not node_reason and node_state in {"stale", "degraded"}:
                node_reason = "Worker heartbeat, identity, lock ownership, or health grade is degraded."
            if not node_reason and status.get("health_grade"):
                node_reason = f"Health monitor grade is {status['health_grade']}."
        current_health = self.dataset_health()
        current_health = dict(current_health) if isinstance(current_health, Mapping) else {}
        current_grade = str(current_health.get("grade", "")).upper()
        health_grade_scope = current_health.get("grade_scope") or "collector_health"
        health_reasons = (
            list(current_health.get("reasons", []))
            if isinstance(current_health.get("reasons", []), (list, tuple))
            else []
        )
        health_reason_item = health_reasons[0] if health_reasons else {}
        health_reason_code = (
            _nested_value(health_reason_item, keys=("code", "reason_code", "error_code"))
            if isinstance(health_reason_item, Mapping)
            else None
        ) or current_health.get("reason_code")
        if isinstance(health_reason_item, Mapping):
            health_reason = _nested_value(
                health_reason_item,
                keys=("reason", "detail", "message", "human_reason"),
            )
        else:
            health_reason = str(health_reason_item).strip() if health_reason_item else None
        health_reason = health_reason or current_health.get("error")
        health_source_type = current_health.get("source_type") or "FORWARD_COLLECTED"
        health_window_start = current_health.get("window_start")
        health_window_end = current_health.get("window_end")
        historical_maturity_grade = current_health.get("historical_maturity_grade")
        historical_error_count = current_health.get("historical_error_count", 0)
        dataset_health = {
            **current_health,
            "grade": current_grade or current_health.get("grade"),
            "grade_scope": health_grade_scope,
            "reason_code": health_reason_code,
            "reasons": health_reasons,
            "window_start": health_window_start,
            "window_end": health_window_end,
            "source_type": health_source_type,
            "historical_maturity_grade": historical_maturity_grade,
            "historical_error_count": historical_error_count,
        }
        crypto_catalog_count = int(crypto_research.get("total", 0) or 0) if isinstance(crypto_research, Mapping) else 0
        crypto_label = "READY" if crypto_catalog_count else ("UPDATING" if any(str(item.get("status")).upper() == "RUNNING" for item in btc_states) else "NOT INITIALIZED")
        polymarket_label = (
            "DEGRADED" if current_grade and current_grade not in {"A", "OK", "HEALTHY"} and catalogs.get("forward_count", 0)
            else ("READY" if catalogs.get("forward_count", 0) else ("UPDATING" if any(str(item.get("status")).upper() == "RUNNING" for item in forward_states) else "NOT INITIALIZED"))
        )
        polymarket_reason = health_reason or (
            "No forward catalog is persisted." if polymarket_label == "NOT INITIALIZED" else None
        )
        polymarket_detail = {
            "forward_catalogs": catalogs.get("forward_count", 0),
            "grade": current_grade or None,
            "grade_scope": health_grade_scope,
            "reason_code": health_reason_code,
            "reasons": health_reasons,
            "window_start": health_window_start,
            "window_end": health_window_end,
            "source_type": health_source_type,
            "historical_maturity_grade": historical_maturity_grade,
            "historical_error_count": historical_error_count,
            "reason": polymarket_reason,
        }
        hermes_workers = [
            str(item.get("status", "")).lower()
            for name, item in worker_map.items()
            if name in {"hermes", "research-queue", "autonomous-research"}
        ]
        if "running" in hermes_workers:
            hermes_label, hermes_reason = "RUNNING", "Internal research queue worker is executing."
        elif "degraded" in hermes_workers or "stale" in hermes_workers:
            hermes_label, hermes_reason = "DEGRADED", "Internal research queue worker heartbeat or identity is stale."
        elif "stopped" in hermes_workers:
            hermes_label, hermes_reason = "STOPPED", "Internal research queue worker is stopped."
        elif hermes_workers:
            hermes_label, hermes_reason = "READY", "Internal research queue worker is idle."
        elif hermes.get("submitted", 0) or hermes.get("pending", 0):
            hermes_label, hermes_reason = "STOPPED", "Internal research queue work is persisted but no queue worker is executing."
        else:
            hermes_label, hermes_reason = "NOT INITIALIZED", "No internal research queue execution state is persisted."
        paper_state_count = int(paper.get("state_count", 0) or 0)
        paper_label = "ACTIVE" if paper_state_count > 0 else "NOT INITIALIZED"
        canary_service = self._canary_service
        if canary_service is not None:
            raw_canary, canary_report = _canary_status_report(canary_service)
            canary_status = _canary_status_projection(raw_canary)
            worker_section = canary_report.get("worker", {})
            autonomous = dict(canary_status.get("autonomous") or {})
            if isinstance(worker_section, Mapping):
                autonomous = _patch_non_missing_values(
                    autonomous,
                    worker_section.get("autonomous"),
                )
                autonomous = _patch_non_missing_values(
                    autonomous,
                    {
                        key: worker_section.get(key)
                        for key in (
                            "next_decision",
                            "blocker",
                            "last_signal_id",
                            "worker_status",
                        )
                        if key in worker_section
                    },
                )
        else:
            canary_report = {}
            canary_status = _canary_status_projection(self.canary_data()["canary"])
        latest_canary_signal = canary_status.get("latest_signal")
        autonomous_canary = (
            canary_status.get("autonomous", {})
            if isinstance(canary_status, Mapping)
            else {}
        )
        raw_credentials = (
            operator_controls.get("credentials")
            if isinstance(operator_controls, Mapping)
            else None
        )
        if isinstance(raw_credentials, Mapping):
            configured = (
                raw_credentials.get("configured")
                if isinstance(raw_credentials.get("configured"), bool)
                else None
            )
            raw_status = str(raw_credentials.get("status") or "").strip().upper()
            if raw_status not in {
                "CONFIGURED",
                "NOT CONFIGURED",
                "UNKNOWN",
                "NOT CHECKED",
            }:
                raw_status = (
                    "CONFIGURED"
                    if configured is True
                    else "NOT CONFIGURED"
                    if configured is False
                    else "NOT CHECKED"
                )
            credentials = {
                "configured": configured,
                "status": raw_status,
                "secret_values_exposed": False,
            }
        else:
            try:
                credentials = canary_module.CredentialStore.cached_projection(
                    allow_environment=False,
                    persisted={
                        "canary": canary_status,
                        "status_report": canary_report,
                    },
                )
            except BaseException:
                credentials = None
            if not isinstance(credentials, Mapping):
                credentials = {
                    "configured": None,
                    "status": "NOT CHECKED",
                    "secret_values_exposed": False,
                }
            else:
                credentials = dict(credentials)
            credentials["secret_values_exposed"] = False
        return {
            "title": "AXIOM / operator research console",
            "live_trading": {"status": "Disabled", "enabled": False},
            "canary": canary_status,
            "canary_signal": latest_canary_signal,
            "autonomous_canary": autonomous_canary,
            "real_execution_events": int(
                canary_status.get("real_execution_events", 0) or 0
            ),
            "paper_risk_engine": {"status": "Active", "enabled": True},
            "dataset_health": dataset_health,
            "health_grade": current_grade or None,
            "grade_scope": health_grade_scope,
            "reason_code": health_reason_code,
            "reasons": health_reasons,
            "window_start": health_window_start,
            "window_end": health_window_end,
            "source_type": health_source_type,
            "historical_maturity_grade": historical_maturity_grade,
            "historical_error_count": historical_error_count,
            "components": [
                component("AXIOM NODE", node_label, {"status": node_state, "reason": node_reason}),
                component("POLYMARKET COLLECTOR", polymarket_label, polymarket_detail),
                component("CRYPTO DATA", crypto_label, {"historical_catalogs": catalogs.get("historical_count", 0), "reason": "No historical catalog is persisted." if crypto_label == "NOT INITIALIZED" else None}),
                component("INTERNAL RESEARCH QUEUE", hermes_label, {**dict(hermes), "execution_state": hermes_label, "reason": hermes_reason}),
                component("PAPER ENGINE", paper_label, {"states": paper_state_count, "reason": "Waiting for PAPER_FORWARD." if paper_label == "NOT INITIALIZED" else None}),
            ],
            "research_cards": {
                "experiments_run": int(count.get("experiments", 0)),
                "active_hypotheses": int(hermes.get("pending", 0)),
                "candidates_alive": sum(value for key, value in stages.items() if key != "REJECTED"),
                "candidate_rejected": stages["REJECTED"],
                "research_rejected": int(hermes.get("rejected", 0) or 0),
                # Kept as an additive compatibility alias for existing clients.
                "rejected": stages["REJECTED"],
                "canary_eligible": candidate_status["canary_eligible"],
                "paper_forward": stages["PAPER_FORWARD"],
                "paper_promotable": stages["PAPER_PROMOTABLE"],
                "newest_hermes_outcome": latest_hermes_outcome,
            },
            "coverage": catalogs,
            "activity": activity_rows,
            "lifecycle_funnel": stages,
            "candidate_status": candidate_status,
            "candidates": candidate_rows,
            "latest_candidates": candidate_rows,
            "btc": self.btc_research_data(catalog_data=catalogs),
            "crypto_research": crypto_research,
            "hermes_latest_outcome": latest_hermes_outcome,
            "polymarket": self.polymarket_research_data(catalog_data=catalogs, current_data=polymarket_current),
            "paper_portfolio": paper,
            "operator_controls": operator_controls,
            "credentials": credentials,
            "hermes": hermes,
            "raw": {
                "summary": summary,
                "status": status,
                "dataset_catalog": catalogs,
                "dashboard_summary": count,
            },
            "paper_only": True,
            "live_execution": False,
        }
    def snapshot(self, endpoint: str, params: Mapping[str, Any] | None = None) -> Any:
        raw_endpoint = endpoint.strip("/")
        endpoint = raw_endpoint.lower()
        if endpoint == "binance-canary":
            return self.binance_canary_data(params)
        if endpoint.startswith("strategy/"):
            return self.strategy_detail(raw_endpoint.split("/", 1)[1])
        if endpoint == "overview":
            return self.overview()
        if endpoint == "operator":
            return self.operator_data()
        if endpoint == "datasets":
            return self.dataset_catalog_data()
        if endpoint == "research":
            return self.research()
        if endpoint == "research-summary":
            return self.research_summary_data()
        if endpoint == "autonomous-research":
            return self.autonomous_research_data()
        if endpoint == "crypto-research":
            return self.paginate_crypto_research(params)
        if endpoint == "crypto":
            return self.crypto()
        if endpoint == "btc-research":
            return self.btc_research_data()
        if endpoint == "prediction":
            return self.prediction()
        if endpoint == "polymarket-research":
            return self.polymarket_research_data()
        if endpoint == "evolution":
            return self.evolution_data()
        if endpoint == "risk":
            return self.risk_data()
        if endpoint == "risk-settings":
            return self.risk_settings_data()
        if endpoint == "paper":
            return self.paper_data()
        if endpoint == "paper-portfolio":
            return self._paper_portfolio()
        if endpoint == "opportunities":
            return self.opportunities_data()
        if endpoint == "queue":
            return self.queue_data()
        if endpoint == "hermes":
            summary = self.research_summary_data()
            return summary.get("hermes", {}) if isinstance(summary, Mapping) else {}
        if endpoint == "system":
            return self.system()
        if endpoint == "status":
            return self.status_data()
        if endpoint == "dataset-health":
            return self.dataset_health()
        if endpoint == "evidence-maturity":
            return self.evidence_maturity()
        if endpoint == "strategy":
            return {"candidates": self._candidate_rows(), "live_execution": False}
        raise KeyError(endpoint)


def _dashboard_html(
    control_token: str | None = None,
    *,
    binance_nav_label: str = "BINANCE SPOT CANARY",
) -> str:
    """Return the bounded operator dashboard surface."""
    return """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta name="axiom-control-token" content="__AXIOM_CONTROL_TOKEN__">
  <title>AXIOM / Operator Research Console</title>
  <style>
    :root { color-scheme: dark; --bg:#080d17; --panel:#101827; --panel2:#0c1422; --line:#213047; --text:#e8eef8; --muted:#8b9ab0; --blue:#67b7ff; --cyan:#58e0d0; --green:#65d39b; --amber:#f4bf64; --red:#f27d8d; font-family:Inter,ui-sans-serif,system-ui,-apple-system,Segoe UI,sans-serif; }
    * { box-sizing: border-box; }
    body { width:100%; max-width:100vw; margin:0; overflow-x:hidden; background:radial-gradient(circle at 85% 0%,#13233d 0,var(--bg) 36rem); color:var(--text); }
    header { border-bottom:1px solid var(--line); background:rgba(8,13,23,.94); position:sticky; top:0; z-index:2; backdrop-filter:blur(12px); }
    .topbar, main { width:min(100% - 32px, 1400px); margin:0 auto; }
    .topbar { padding:20px 0 12px; display:flex; align-items:flex-start; justify-content:space-between; gap:20px; }
    h1,h2,h3,p { margin:0; } h1 { font-size:1.3rem; letter-spacing:.08em; text-transform:uppercase; } h2 { font-size:.95rem; letter-spacing:.04em; text-transform:uppercase; } h3 { font-size:.8rem; color:var(--muted); text-transform:uppercase; letter-spacing:.08em; }
    .eyebrow { color:var(--blue); font-size:.68rem; letter-spacing:.15em; text-transform:uppercase; margin-bottom:7px; } .subtitle,.muted,.page-note { color:var(--muted); } .subtitle { margin-top:6px; font-size:.86rem; }
    .live-lock { border:1px solid #276a62; background:#0c2b2c; color:#9bf1d3; border-radius:6px; padding:9px 11px; font-size:.7rem; text-transform:uppercase; letter-spacing:.08em; white-space:nowrap; }
    nav { width:min(100% - 32px, 1400px); margin:0 auto; display:flex; gap:4px; padding:0 0 11px; overflow-x:auto; } nav button,button.link { border:1px solid transparent; color:var(--muted); background:transparent; cursor:pointer; } nav button { padding:7px 10px; border-radius:5px; font-size:.7rem; letter-spacing:.06em; text-transform:uppercase; } nav button.tab:hover:not(.active) { color:var(--text); border-color:var(--line); background:#122039; } nav button.tab.active { color:var(--text); border-color:var(--line); background:#122039; } nav button.tab:focus-visible { outline:2px solid var(--blue); outline-offset:2px; }
    main { padding:22px 0 55px; } .view { display:none; } .view.active { display:block; }
    .grid,.two-col,.three-col,.status-grid,.card-grid { display:grid; gap:12px; } .status-grid { grid-template-columns:repeat(5,minmax(0,1fr)); margin-bottom:14px; } .card-grid { grid-template-columns:repeat(6,minmax(0,1fr)); margin-bottom:14px; } .two-col { grid-template-columns:minmax(0,1.45fr) minmax(260px,.8fr); } .three-col { grid-template-columns:repeat(3,minmax(0,1fr)); }
    .panel { min-width:0; border:1px solid var(--line); background:linear-gradient(145deg,rgba(16,24,39,.96),rgba(10,17,29,.96)); border-radius:8px; padding:15px; box-shadow:0 10px 34px rgba(0,0,0,.14); } .panel + .panel { margin-top:12px; } .status-card { padding:12px 13px; }
    .status-head,.section-title,.pager { display:flex; align-items:center; justify-content:space-between; gap:10px; } .status-name { font-size:.68rem; color:var(--muted); letter-spacing:.07em; text-transform:uppercase; } .status-value { margin-top:12px; font-size:.88rem; font-weight:650; }
    .badge { display:inline-block; border-radius:999px; padding:3px 7px; font-size:.6rem; letter-spacing:.05em; text-transform:uppercase; border:1px solid var(--line); color:var(--muted); } .badge.good { color:#9bf1d3; border-color:#276a62; background:#102d2d; } .badge.warn { color:#ffd99a; border-color:#765424; background:#2d2414; } .badge.bad { color:#ffb4bd; border-color:#713844; background:#2d161d; }
    .metric { font-size:1.45rem; font-variant-numeric:tabular-nums; margin-top:8px; } .metric-label { color:var(--muted); font-size:.69rem; margin-top:3px; } .empty { border:1px dashed #33445d; border-radius:6px; color:var(--muted); padding:17px; font-size:.8rem; line-height:1.5; background:var(--panel2); } .empty strong { color:var(--text); display:block; margin-bottom:5px; }
    .scroll { width:100%; overflow-x:auto; } table { width:100%; border-collapse:collapse; font-size:.75rem; } th,td { text-align:left; padding:8px; border-bottom:1px solid #1c2a3f; white-space:nowrap; } th { position:sticky; top:0; z-index:1; background:#101827; color:var(--muted); font-weight:600; text-transform:uppercase; letter-spacing:.05em; font-size:.62rem; } tbody tr:hover { background:#142139; }
    button.link { color:var(--blue); padding:0; font:inherit; text-align:left; } button.link:hover { text-decoration:underline; } input,select { min-width:0; border:1px solid var(--line); border-radius:5px; background:#0a1220; color:var(--text); padding:7px 9px; font-size:.73rem; } .filters { display:flex; gap:7px; flex-wrap:wrap; margin:10px 0; } .filters input { flex:1 1 180px; }
    .timeline { display:grid; gap:2px; } .timeline-item { display:grid; grid-template-columns:95px 75px minmax(0,1fr); gap:9px; padding:8px 0; border-bottom:1px solid #1c2a3f; align-items:baseline; } .timeline-time { color:var(--muted); font-size:.65rem; } .timeline-kind { color:var(--cyan); text-transform:uppercase; letter-spacing:.05em; font-size:.61rem; }
    .funnel { display:grid; gap:7px; } .funnel-row { display:grid; grid-template-columns:145px 1fr 35px; gap:8px; align-items:center; font-size:.69rem; } .funnel-track { height:8px; background:#172238; border-radius:5px; overflow:hidden; } .funnel-bar { height:100%; background:linear-gradient(90deg,var(--blue),var(--cyan)); border-radius:5px; }
    .key-value { display:grid; grid-template-columns:145px minmax(0,1fr); gap:7px; font-size:.76rem; } .key { color:var(--muted); } .key-value + .key-value { margin-top:7px; } details { margin-top:12px; } summary { color:var(--muted); cursor:pointer; font-size:.72rem; } pre { margin:9px 0 0; max-height:350px; overflow:auto; white-space:pre-wrap; word-break:break-word; color:#b7c7dc; font-size:.68rem; line-height:1.4; } .page-note { font-size:.74rem; line-height:1.5; margin-top:9px; } .notice { border-left:3px solid var(--amber); padding:8px 11px; color:#e7d4a8; background:#211b10; font-size:.73rem; line-height:1.4; } .right { text-align:right; } .pager { margin-top:11px; color:var(--muted); font-size:.72rem; } .pager button { border:1px solid var(--line); border-radius:4px; color:var(--text); background:#0a1220; padding:5px 8px; cursor:pointer; } .pager button:disabled { opacity:.4; cursor:default; }
    @media (max-width:1050px) { .status-grid { grid-template-columns:repeat(3,1fr); } .card-grid { grid-template-columns:repeat(3,1fr); } .two-col,.three-col { grid-template-columns:1fr; } } @media (max-width:620px) { .topbar,main,nav { width:min(100% - 24px,1400px); } .status-grid,.card-grid { grid-template-columns:repeat(2,1fr); } .timeline-item { grid-template-columns:72px 60px minmax(0,1fr); } }
    .identity { display:flex; flex-direction:column; gap:2px; min-width:160px; } .identity-main { color:var(--text); font-weight:650; } .identity-sub { color:var(--muted); font-size:.65rem; } .copy { border:1px solid var(--line); border-radius:4px; background:transparent; color:var(--blue); cursor:pointer; font-size:.62rem; padding:2px 5px; margin-left:5px; } .copy:hover { background:#122039; } .refresh-note { min-height:1.1em; color:var(--muted); font-size:.68rem; } .refresh-note.slow { color:var(--amber); } .quality-context { display:block; color:var(--muted); font-size:.62rem; white-space:normal; max-width:260px; } .activity-compact { border-bottom:1px solid #1c2a3f; padding:8px 0; } .activity-compact + .activity-compact { margin-top:2px; } .detail-grid { display:grid; gap:10px; } .detail-section { border-top:1px solid #1c2a3f; padding-top:9px; } .detail-section h3 { margin-bottom:6px; } .progress { height:7px; background:#172238; border-radius:5px; overflow:hidden; } .progress > i { display:block; height:100%; background:linear-gradient(90deg,var(--blue),var(--cyan)); }
    .binance-view .identity,.binance-view td,.binance-view strong { overflow-wrap:anywhere; word-break:break-word; white-space:normal; } .binance-view table { table-layout:fixed; } .binance-view details pre { max-height:420px; } .binance-action { border:1px solid var(--line); border-radius:5px; background:#0a1220; color:var(--text); cursor:pointer; padding:7px 9px; font-size:.7rem; } .binance-action.danger { color:#ffb4bd; border-color:#713844; }
  </style>
</head>
<body>
  <header><div class="topbar"><div><div class="eyebrow">Paper-first research operations</div><h1>AXIOM / operator console</h1><p class="subtitle">Historical evidence, forward observation, and paper lifecycle in one view.</p></div><div class="live-lock">Live trading <strong>Disabled</strong><br>Paper risk engine <strong>Active</strong></div></div>
    <nav aria-label="Research sections">
      <button class="tab active" data-view="overview">Overview</button><button class="tab" data-view="datasets">DATASETS</button><button class="tab" data-view="activity">ACTIVITY</button><button class="tab" data-view="crypto">CRYPTO RESEARCH</button><button class="tab" data-view="polymarket">Polymarket</button><button class="tab" data-view="candidates">Candidates</button><button class="tab" data-view="hermes">Hermes</button><button class="tab" data-view="portfolio">Paper Portfolio</button><button class="tab" data-view="canary">Polymarket Canary</button><button class="tab" data-view="binance-canary">__BINANCE_NAV_LABEL__</button>
    </nav>
  </header>
  <main>
    <section id="view-overview" class="view active"><div id="component-grid" class="status-grid"></div><div id="overview-readiness-snapshot"></div><div id="research-cards" class="card-grid"></div>
      <article id="research-progress" class="panel"><div class="section-title"><h2>AUTOMATIC RESEARCH</h2><span class="badge warn">persisted · paper-only</span></div><div id="research-progress-content" class="three-col"></div></article>
      <article id="research-feed" class="panel"><div class="section-title"><h2>RESEARCH FEED</h2><span class="badge warn">observability · paper-only</span></div><div id="research-feed-content"><section class="panel"><div class="section-title"><h3>External Hermes feed</h3><span class="badge warn">External status UNKNOWN</span></div><p class="page-note">Waiting for live external feed evidence.</p></section></div></article>
      <article class="panel"><div class="section-title"><h2>SYSTEM CONTROL</h2><span class="badge good">localhost + token</span></div><div id="operator-controls" class="three-col"></div><div id="control-result" class="page-note"></div></article>
      <div class="two-col"><div><article class="panel"><div class="section-title"><h2>Historical / forward coverage</h2><a class="link" href="#datasets" data-link="datasets">View all</a></div><div id="coverage"></div></article>
        <article class="panel"><div class="section-title"><h2>Candidate lifecycle funnel</h2><a class="link" href="#candidates" data-link="candidates">View all</a></div><div id="funnel" class="funnel"></div></article>
        <article class="panel"><div class="section-title"><h2>Research → order funnel</h2><span class="badge">persisted handoff</span></div><div id="market-scope-funnel" class="funnel"></div><p class="page-note">Qualification is historical evidence only. Readiness and execution feasibility are persisted separately.</p></article>
        <article class="panel"><div class="section-title"><h2>Latest candidates</h2><a class="link" href="#candidates" data-link="candidates">View all</a></div><div id="overview-candidates" class="scroll"></div></article></div>
        <div><article class="panel"><div class="section-title"><h2>Latest activity</h2><a class="link" href="#activity" data-link="activity">View all</a></div><div id="overview-activity" class="timeline"></div></article>
        <article class="panel"><div class="section-title"><h2>Selected detail</h2><span class="muted">preserved on refresh</span></div><div id="detail" class="empty"><strong>Select an item</strong>Dataset and candidate evidence appears here.</div></article></div></div>
      <details><summary>Technical details · raw APIs and retained debug surfaces</summary><p class="page-note">Existing JSON APIs remain available for automation. Research maturity, Paper forward, and Research queue and node status are retained below as raw endpoint links.</p><div id="api-links"><a href="/api/v2/datasets">datasets</a> · <a href="/api/v2/activity">activity</a> · <a href="/api/v2/candidates">candidates</a> · <a href="/api/v2/polymarket">polymarket</a> · <a href="/api/v2/binance-canary">binance-canary</a> · <a href="/api/v2/hermes">hermes</a> · <a href="/api/v2/paper">paper</a> · <a href="/api/autonomous-research">autonomous-research</a></div><pre id="raw-overview"></pre></details>
    </section>
    <section id="view-datasets" class="view"><article class="panel"><div class="section-title"><h2>DATASETS</h2><span id="dataset-total" class="muted"></span></div><div class="filters"><input id="datasets-filter" placeholder="Filter dataset, instrument, source" aria-label="Filter datasets"><select id="datasets-source"><option value="">All sources</option><option>HISTORICAL</option><option>FORWARD_COLLECTED</option></select><select id="datasets-size"><option>25</option><option>50</option><option>100</option></select></div><div id="datasets-table" class="scroll"></div><div id="datasets-pager" class="pager"></div></article><article id="dataset-detail" class="panel"></article></section>
    <section id="view-activity" class="view"><article class="panel"><div class="section-title"><h2>ACTIVITY</h2><span id="activity-total" class="muted"></span></div><div class="filters"><input id="activity-filter" placeholder="Filter activity" aria-label="Filter activity"><select id="activity-status"><option value="">All statuses</option><option>PENDING</option><option>RUNNING</option><option>COMPLETE</option><option>COMPLETED</option><option>ACCEPTED</option><option>FAILED</option><option>ERROR</option><option>REJECTED</option></select><select id="activity-size"><option>25</option><option>50</option><option>100</option></select></div><div id="activity-table" class="scroll"></div><div id="activity-pager" class="pager"></div></article></section>
    <section id="view-crypto" class="view"><article class="panel"><div class="section-title"><h2>CRYPTO RESEARCH</h2><span class="badge good">historical and forward · paper-only</span></div><div class="filters"><input id="crypto-filter" placeholder="Filter crypto catalog, report, symbol" aria-label="Filter crypto research"><input id="crypto-symbol" placeholder="Symbol" aria-label="Filter crypto symbol"><select id="crypto-size"><option>25</option><option>50</option><option>100</option></select></div><div id="crypto-summary"></div><div id="crypto-table" class="scroll"></div><div id="crypto-pager" class="pager"></div><div id="crypto-detail"></div><div class="notice">All crypto catalogs and reports are bounded, versioned, and paper-only; no live execution path exists.</div></article></section>
    <section id="view-polymarket" class="view"><article class="panel"><div class="section-title"><h2>Polymarket opportunities</h2><span class="badge warn">price proxy unless timestamped depth exists</span></div><div class="filters"><input id="polymarket-filter" placeholder="Filter questions or markets" aria-label="Filter Polymarket"><select id="polymarket-category"><option value="">All categories</option></select><select id="polymarket-size"><option>25</option><option>50</option><option>100</option></select></div><div id="pm-summary"></div><div id="pm-markets" class="scroll"></div><div id="polymarket-pager" class="pager"></div><p class="page-note">Historical price history is separate from forward order-book observations. No historical depth, spread, fills, or executable quotes are fabricated.</p></article></section>
    <section id="view-candidates" class="view"><article class="panel"><div class="section-title"><h2>CANDIDATES</h2><span id="candidate-total" class="muted"></span></div><div class="filters"><input id="candidates-filter" placeholder="Filter strategy, family, market" aria-label="Filter candidates"><select id="candidates-stage"><option value="">All stages</option></select><select id="candidates-size"><option>25</option><option>50</option><option>100</option></select></div><div id="candidates-table" class="scroll"></div><div id="candidates-pager" class="pager"></div></article></section>
    <article id="candidate-detail" class="panel"><div class="section-title"><h2>Candidate detail</h2><span class="muted">historical → forward → lifecycle</span></div><div class="empty">Select a candidate to inspect evidence.</div></article>
    <section id="view-hermes" class="view"><article class="panel"><div class="section-title"><h2>Hermes / research loop</h2><span class="badge">research only · no canary control</span></div><div id="hermes-summary"></div><div class="filters"><input id="hermes-filter" placeholder="Filter queue" aria-label="Filter Hermes queue"><select id="hermes-status"><option value="">All statuses</option><option>PENDING</option><option>TESTING</option><option>COMPLETED</option><option>ACCEPTED</option><option>REJECTED</option><option>FAILED</option><option>ERROR</option></select><select id="hermes-size"><option>25</option><option>50</option><option>100</option></select></div><div id="hermes-table" class="scroll"></div><div id="hermes-pager" class="pager"></div><div id="hermes-detail"></div></article></section>
    <section id="view-portfolio" class="view"><article class="panel"><div class="section-title"><h2>Paper Portfolio</h2><span class="badge warn">paper-only · no live execution</span></div><div class="filters"><input id="paper-filter" placeholder="Filter paper records" aria-label="Filter paper records"><select id="paper-status"><option value="">All statuses</option><option>OPEN</option><option>CLOSED</option><option>RESOLVED</option><option>UNKNOWN</option></select><select id="paper-size"><option>25</option><option>50</option><option>100</option></select></div><div id="portfolio-summary"></div><div id="portfolio-states" class="scroll"></div><div id="paper-pager" class="pager"></div></article></section>
    <section id="view-canary" class="view"><article class="panel" style="border-color:var(--red)"><div class="section-title"><h2>REAL CANARY MONEY</h2><span class="badge bad">PRODUCTION LIVE TRADING: DISABLED</span></div><div id="canary-action-result" class="page-note"></div><div id="canary-readiness-snapshot"></div><div id="canary-controls"></div><div id="risk-settings"></div><div id="canary-connectivity"></div><div id="canary-summary"></div><div id="canary-trades" class="scroll"></div><p class="notice">Autonomous canary is independent from paper research. No secrets are stored or displayed. It remains prediction-only, bounded by active settings, and killable from this console.</p></article></section>
    <article id="canary-recovery-form" class="panel"><div class="section-title"><h2>UNKNOWN ENTRY RECOVERY</h2><span class="badge warn">READ-ONLY · PRODUCTION PROFILE</span></div><p class="page-note">Attach only an operator-supplied canonical exchange order ID. This does not post, retry, activate, or release an entry.</p><div class="three-col"><label>Event ID<input id="canary-recovery-event" autocomplete="off"></label><label>Signal ID<input id="canary-recovery-signal" autocomplete="off"></label><label>Canonical exchange order ID<input id="canary-recovery-order" autocomplete="off"></label></div><label>Exact confirmation<input id="canary-recovery-confirm" placeholder="RECOVER UNKNOWN ENTRY" autocomplete="off"></label><p class="page-note"><button id="canary-recovery-submit" class="link">Recover and reconcile</button> <span id="canary-recovery-result"></span></p></article>
    <section id="view-binance-canary" class="view binance-view"><article class="panel" style="border-color:var(--amber)"><div class="section-title"><h2>BINANCE SPOT CANARY</h2><span class="badge warn">DEVELOPMENT / PAPER|TESTNET</span></div><p class="page-note">Separate from the Polymarket canary. <strong>POLYMARKET TRANSPORT: DISABLED</strong> · Binance Spot only · no implicit control-plane construction.</p><div id="binance-action-result" class="page-note"></div><div id="binance-identity"></div><div id="binance-connectivity"></div><div id="binance-qualification"></div><div id="binance-risk"></div><div id="binance-controls"></div><div id="binance-records" class="scroll"></div><details><summary>Full Binance projection and identifiers</summary><pre id="binance-raw"></pre></details><p class="notice">Credentials are never displayed. Connectivity checks are read-only; order validation is an explicit test action. No browser action can place an order.</p></article></section>
    <div id="binance-testnet-static-labels" hidden>BINANCE SPOT TESTNET · TESTNET CONNECTIVITY · ORDER VALIDATION · TESTNET EXECUTION PROBE · AUTONOMOUS TESTNET · localhost</div>
  </main>
  <script>
    const $ = (id) => document.getElementById(id), safe = (v) => String(v ?? "—").replace(/[&<>"']/g, c => ({ "&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;" }[c])), json = (v) => JSON.stringify(v ?? {}, null, 2);
    const count = (v) => v == null ? "UNKNOWN" : Number.isFinite(Number(v)) ? String(v) : "UNKNOWN", phtDateFormatter = new Intl.DateTimeFormat("en-PH-u-hc-h23", { timeZone:"Asia/Manila", year:"numeric", month:"2-digit", day:"2-digit", hour:"2-digit", minute:"2-digit", second:"2-digit", hourCycle:"h23" }), dateText = (v) => { if(!v || typeof v !== "string" || !/(?:Z|[+-][0-9]{2}:[0-9]{2})$/.test(v)) return "—"; const date = new Date(v); if(Number.isNaN(date.getTime())) return "—"; const parts = Object.fromEntries(phtDateFormatter.formatToParts(date).filter(i => i.type !== "literal").map(i => [i.type, i.value])); return `${parts.year}-${parts.month}-${parts.day} ${parts.hour}:${parts.minute}:${parts.second} PHT`; }, usd = (v) => { const number=Number(v); return Number.isFinite(number)?`$${number.toFixed(2)}`:"—"; }, arr = (v) => Array.isArray(v) ? v : [];
    const empty = (title,body) => `<div class="empty"><strong>${safe(title)}</strong>${safe(body)}</div>`, statusClass = (v) => { const s=String(v||"").toUpperCase(); return ["READY","RUNNING","ACTIVE","COMPLETE","COMPLETED","HEALTHY","ELIGIBLE","PASSED","PROMOTABLE","A","B"].includes(s)?"good":["DEGRADED","STOPPED","UPDATING","SUBMITTING","UNKNOWN","C"].includes(s)?"warn":["ERROR","STALE","REJECTED","KILLED","BLOCKED","FAIL","INSUFFICIENT","UNAVAILABLE","D","F"].includes(s)?"bad":""; };
    function readinessSnapshotMarkup(data) { const snapshot=data?.canary&&typeof data.canary==="object"?data.canary:(data||{}),status=String(snapshot.readiness_snapshot_status||"STALE").toUpperCase(),stale=snapshot.readiness_snapshot_stale===true||status==="STALE",label=stale?"READINESS SNAPSHOT STALE":"READINESS SNAPSHOT CURRENT",updated=snapshot.readiness_snapshot_updated_at||data?.readiness_snapshot_updated_at; return `<p class="page-note readiness-snapshot"><span class="badge ${statusClass(stale?"STALE":"CURRENT")}">${label}</span> · Updated ${safe(dateText(updated))}</p>`; }
    let params = new URLSearchParams(location.search); const state = { tab: params.get("tab") || "overview", page: Math.max(1,Number(params.get("page")||1)), page_size: [10,25,50,100].includes(Number(params.get("page_size"))) ? Number(params.get("page_size")) : 25, filter: params.get("filter") || "", sort: params.get("sort") || "", direction: params.get("direction") === "asc" ? "asc" : "desc", selected: params.get("selected") || "", expanded: params.get("expanded") === "1" };
    let operator = {}, current = {}, loadInFlight = false, operatorControlsRendered = false, binanceTestnetMode = false;
    let riskReview = {active:null,draft:null};
    const controlToken = document.querySelector('meta[name="axiom-control-token"]')?.content || "";
    function controlButton(action,label,target="",confirmation="",payload=null) { const encodedPayload=payload&&typeof payload==="object"&&!Array.isArray(payload)?JSON.stringify(payload):""; return `<button class="link control-action" data-control-action="${safe(action)}" data-control-target="${safe(target)}" data-control-confirm="${safe(confirmation)}" data-control-payload="${safe(encodedPayload)}">${safe(label)}</button>`; }
    function isCanaryAction(action) { return String(action||"").startsWith("canary."); }
    function actionResultNode(action) { return $(isCanaryAction(action)?"canary-action-result":"control-result"); }
    function actionResultMessage(action,message) { const node=actionResultNode(action); if(node)node.textContent=message||""; }
    async function controlPost(action,target="",confirmation="",extra={}) {
      const payload={action,target,...(extra&&typeof extra==="object"?{payload:extra}: {})}; if(confirmation)payload.confirm=confirmation;
      try {
        const response=await fetch("/api/control",{method:"POST",headers:{"Content-Type":"application/json","X-Axiom-Control-Token":controlToken},body:JSON.stringify(payload),cache:"no-store"});
        const result=await response.json();
        const connectivity=result?.result?.connectivity||result?.connectivity;
        if(isCanaryAction(action)&&connectivity) {
          if(lastGood.canary&&typeof lastGood.canary==="object"&&!Array.isArray(lastGood.canary)) lastGood.canary={...lastGood.canary,connectivity};
          renderCanaryConnectivity(connectivity);
        }
        const actionIdentity=result?.action_id?` · ${result.action_id}`:"";
        actionResultMessage(action,result.ok?`${action} completed${actionIdentity}`:`${action} blocked: ${result.reason||"CONTROL_FAILED"}${actionIdentity}`);
        if(activeController)activeController.abort();
        refreshGeneration++;
        activeController=null;
        loadInFlight=false;
        clearTimeout(slowRefreshTimer);
        slowRefreshTimer=null;
        nextRefreshAt=0; await loadPage(state.tab,true);
        return result;
      } catch(error) {
        actionResultMessage(action,`${action} unavailable: ${error?.message||"network failure"}`);
        return {ok:false,reason:"CONTROL_UNAVAILABLE"};
      }
    }
    function renderOperatorControls(data) {
      const controls=data.operator_controls||{};
      if(!Object.keys(controls).length){
        if(!operatorControlsRendered)$("operator-controls").innerHTML=empty("Operator controls unavailable","Launch with python -m axiom.cli operator to enable typed localhost controls.");
        return;
      }
      operatorControlsRendered=true;
      const n=controls.node||{},b=controls.bootstrap||{},h=controls.hermes||{},p=controls.paper||{},c=controls.collector||{},cred=controls.credentials||{};
      const progress=b.total_datasets?`${count(b.completed_datasets)} / ${count(b.total_datasets)} datasets`:"—";
      $("operator-controls").innerHTML=[
        `<article><h3>AXIOM NODE</h3><div class="key-value"><span class="key">Status</span><strong>${safe(n.status)}</strong></div><div class="key-value"><span class="key">PID / heartbeat</span><strong>${safe(n.pid)} · ${safe(dateText(n.heartbeat_at))}</strong></div><p class="page-note">${controlButton("node.restart","Restart node","node")}</p></article>`,
        `<article><h3>CRYPTO BOOTSTRAP</h3><div class="key-value"><span class="key">Status</span><strong>${safe(b.status)}</strong></div><div class="key-value"><span class="key">Current</span><strong>${safe(b.current_symbol)} · ${safe(b.current_timeframe)}</strong></div><div class="key-value"><span class="key">Progress</span><strong>${safe(progress)}</strong></div><p class="page-note">${b.status==="FAILED"||b.resumable?controlButton("bootstrap.resume","Resume bootstrap","crypto-universe"):controlButton("bootstrap.start","Start bootstrap","crypto-universe")}</p></article>`,
        `<article><h3>INTERNAL RESEARCH QUEUE</h3><div class="key-value"><span class="key">Internal queue status</span><strong>${safe(h.status||"UNKNOWN")}</strong></div><div class="key-value"><span class="key">Last / next</span><strong>${safe(dateText(h.last_run_at))} · ${safe(dateText(h.next_run_at))}</strong></div><p class="page-note">${String(h.status||"").toUpperCase()==="PAUSED"?controlButton("hermes.resume","Resume processing"):controlButton("hermes.pause","Pause processing")} · ${controlButton("hermes.run_now","Process next pending item now")}</p></article>`,
        `<article><h3>PAPER ENGINE</h3><div class="key-value"><span class="key">Status</span><strong>${safe(p.status)}</strong></div><p class="page-note">Read-only paper status. No browser configuration or trading controls.</p></article>`,
        `<article><h3>COLLECTOR</h3><div class="key-value"><span class="key">Status</span><strong>${safe(c.status)}</strong></div><p class="page-note">Safe independent restart is unavailable; restart the node instead.</p></article>`,
        `<article><h3>CREDENTIALS</h3><div class="key-value"><span class="key">Configured</span><strong>${cred.configured?"YES":"NO"}</strong></div><p class="page-note">Configuration is CLI-only. Secret values are never returned.</p></article>`
      ].join("");
    }
    function saveState(push=false) { const q=new URLSearchParams(); q.set("tab",state.tab); q.set("page",state.page); q.set("page_size",state.page_size); if(state.filter)q.set("filter",state.filter); if(state.sort)q.set("sort",state.sort); if(state.direction!=="desc")q.set("direction",state.direction); if(state.selected)q.set("selected",state.selected); if(state.expanded)q.set("expanded","1"); document.querySelectorAll("select.facet").forEach(el=>{if(el.value)q.set(el.dataset.param||el.id,el.value)}); (push?history.pushState:history.replaceState).call(history,{}, "", `${location.pathname}?${q}`); }
    function activate(tab,push=true) { if(tab!==state.tab){state.selected="";state.expanded=false;state.filter="";state.sort="";state.direction="desc";state.page=1;} state.tab=tab; document.querySelectorAll(".tab").forEach(b=>b.classList.toggle("active",b.dataset.view===tab)); document.querySelectorAll(".view").forEach(v=>v.classList.toggle("active",v.id===`view-${tab}`)); saveState(push); if(tab!=="overview"&&tab!=="canary") loadPage(tab); }
    function sortButton(key,label) { const active=state.sort===key, arrow=active?(state.direction==="asc"?" ▲":" ▼"):""; return `<button class="link sort" data-sort="${safe(key)}">${safe(label)}${arrow}</button>`; }
    function restoreFacets() { document.querySelectorAll("select.facet").forEach(el=>{const value=params.get(el.dataset.param||el.id);if(value!==null&&Array.from(el.options).some(o=>o.value===value))el.value=value;}); document.querySelectorAll('select[id$="-size"]').forEach(el=>{el.value=String(state.page_size);}); document.querySelectorAll(".filters input").forEach(el=>{el.value=state.filter;}); }
    function ensureFacets() { const specs={datasets:[["datasets-market","Market","market",["crypto_spot","prediction"]],["datasets-timeframe","Timeframe","timeframe",["1m","1h","1d","live"]],["datasets-quality","Quality","quality",["OHLCV","PRICE_PROXY","ORDER_BOOK_SIMULATED"]]],polymarket:[["polymarket-settlement","Settlement","settlement",["open","resolved_yes","resolved_no","void"]],["polymarket-quality","Quality","quality",["PRICE_PROXY","ORDER_BOOK_SIMULATED"]]]}; Object.entries(specs).forEach(([view,entries])=>{const host=document.querySelector(`#view-${view} .filters`);if(!host)return;entries.forEach(([id,label,param,options])=>{if($(id))return;const s=document.createElement("select");s.id=id;s.className="facet";s.dataset.param=param;s.innerHTML=`<option value="">All ${label.toLowerCase()}</option>${options.map(o=>`<option value="${safe(o)}">${safe(o)}</option>`).join("")}`;host.appendChild(s);});}); document.querySelectorAll(".filters select").forEach(el=>{el.classList.add("facet");if(el.id.endsWith("-size")){el.dataset.param="page_size";if(!Array.from(el.options).some(o=>o.value==="10")){const option=document.createElement("option");option.value="10";option.textContent="10";el.insertBefore(option,el.firstChild);}}else if(!el.dataset.param)el.dataset.param=el.id.includes("source")?"source_type":el.id.includes("stage")?"stage":el.id.includes("status")?"status":el.id.includes("category")?"category":el.id;}); restoreFacets(); }
    function pager(name,data) { const total=Number(data?.total)||0,page=Number(data?.page)||1,size=Number(data?.page_size)||25,pages=Number(data?.pages)||0,start=total?(page-1)*size+1:0,end=Math.min(page*size,total),windowStart=Math.min(Math.max(1,page-3),Math.max(1,pages-6)); const numbers=pages?Array.from({length:Math.min(pages,7)},(_,i)=>windowStart+i):[]; $(`${name}-pager`).innerHTML=`<span>Showing ${start}–${end} of ${total} · Page ${page} of ${pages||1}</span><span><button data-page="${page-1}" ${page<=1?"disabled":""}>Previous</button> ${numbers.map(n=>`<button data-page="${n}" ${n===page?"disabled":""}>${n}</button>`).join(" ")} <button data-page="${page+1}" ${!pages||page>=pages?"disabled":""}>Next</button></span>`; $(`${name}-pager`).querySelectorAll("button").forEach(b=>b.addEventListener("click",()=>{const next=Number(b.dataset.page);if(name==="dataset-ranges")loadDataset(state.selected,next,false);else if(name==="candidate-events")loadCandidate(state.selected,next,false);else{state.page=next;saveState(true);loadPage(name==="paper"?"portfolio":name);}})); }
    async function fetchV2(name) { const q=new URLSearchParams({page:String(state.page),page_size:String(state.page_size),direction:state.direction}); if(state.filter)q.set("filter",state.filter); if(state.sort)q.set("sort",state.sort); const controls={datasets:[["datasets-source","source_type"],["datasets-market","market"],["datasets-timeframe","timeframe"],["datasets-quality","quality"]],activity:[["activity-status","status"]],candidates:[["candidates-stage","stage"]],polymarket:[["polymarket-category","category"],["polymarket-settlement","settlement"],["polymarket-quality","quality"]],hermes:[["hermes-status","status"]],paper:[["paper-status","status"]]}; for(const [id,key] of (controls[state.tab]||[])){const el=$(id);if(el&&el.value)q.set(key,el.value);} const response=await fetch(`/api/v2/${name}?${q}`,{cache:"no-store"}); if(!response.ok)throw new Error(`${name} HTTP ${response.status}`); return response.json(); }
    function renderComponents(data) { $("component-grid").innerHTML=arr(data.components).map(i=>{const s=String(i.state||"NOT INITIALIZED"),reason=i.detail?.reason||i.detail?.error||"";return `<article class="panel status-card"><div class="status-head"><span class="status-name">${safe(i.name)}</span><span class="badge ${statusClass(s)}">${safe(s)}</span></div><div class="status-value">${safe(i.detail?.status||i.detail?.symbol||"read-only")}</div>${reason?`<p class="page-note">${safe(reason)}</p>`:""}</article>`}).join("")||empty("System not initialized","Start the normal AXIOM node to populate worker status."); }
    function renderOverview(data) { renderComponents(data); const cards=data.research_cards||{}; $("research-cards").innerHTML=[["experiments_run","Experiments run"],["active_hypotheses","Active hypotheses"],["candidates_alive","Candidates alive"],["rejected","Rejected"],["paper_forward","Paper forward"],["paper_promotable","Paper promotable"]].map(([k,l])=>`<article class="panel"><div class="metric">${count(cards[k])}</div><div class="metric-label">${l}</div></article>`).join(""); const c=data.coverage||{}; $("coverage").innerHTML=`<div class="three-col"><div class="key-value"><span class="key">Historical datasets</span><strong>${count(c.historical_count)}</strong></div><div class="key-value"><span class="key">Forward datasets</span><strong>${count(c.forward_count)}</strong></div><div class="key-value"><span class="key">Rows observed</span><strong>${count((c.historical_rows||0)+(c.forward_rows||0))}</strong></div></div><p class="page-note">Prediction market datasets are available in DATASETS; overview intentionally shows summaries only.</p>`; const funnel=data.lifecycle_funnel||{}; const max=Math.max(1,...Object.values(funnel).map(Number)); $("funnel").innerHTML=Object.entries(funnel).map(([k,v])=>`<div class="funnel-row"><span>${safe(k)}</span><span class="funnel-track"><span class="funnel-bar" style="width:${Math.min(100,Number(v)/max*100)}%"></span></span><span class="right">${count(v)}</span></div>`).join("")||empty("No candidate lifecycle","Hermes hypotheses appear after a durable queue item is processed."); $("overview-candidates").innerHTML=tableCandidates(arr(data.candidates).slice(0,10),false); $("overview-activity").innerHTML=arr(data.activity).slice(0,10).map(i=>`<div class="timeline-item"><span class="timeline-time">${safe(dateText(i.timestamp))}</span><span class="timeline-kind">${safe(i.kind)}</span><span>${safe(i.message)}</span></div>`).join("")||empty("No research activity yet","Durable bootstrap, collection, and Hermes activity will appear here."); $("raw-overview").textContent=json(data.raw||{}); }
    function tableCandidates(items,interactive=true) { if(!items.length)return empty("No candidates yet","Submit a bounded paper-only hypothesis through Hermes."); const sortable={strategy_id:"candidate_id",stage:"stage",updated_at:"updated_at"}; return `<table><thead><tr>${[["strategy_id","Strategy"],["family","Family"],["market","Market"],["stage","Stage"],["historical_gates","Historical gates"],["canary_status","Micro-live canary"],["paper_forward_status","Paper forward status"],["paper_promotable_status","Paper promotable"],["updated_at","Updated"]].map(([k,l])=>`<th>${interactive&&sortable[k]?sortButton(sortable[k],l):safe(l)}</th>`).join("")}</tr></thead><tbody>${items.map(i=>`<tr><td>${interactive?`<button class="link candidate" data-id="${encodeURIComponent(i.candidate_id||"")}">${safe(i.strategy_id||i.candidate_id)}</button>`:safe(i.strategy_id||i.candidate_id)}</td><td>${safe(i.family)}</td><td>${safe(i.market)}</td><td><span class="badge ${statusClass(i.stage)}">${safe(i.stage)}</span></td><td><span class="badge ${statusClass(i.historical_gates)}">${safe(i.historical_gates||"NOT_PASSED")}</span></td><td><span class="badge ${statusClass(i.canary_status)}">${safe(i.canary_status||"NOT_ELIGIBLE")}</span></td><td><span class="badge ${statusClass(i.paper_forward_status)}">${safe(i.paper_forward_status||"NOT_STARTED")}</span></td><td><span class="badge ${statusClass(i.paper_promotable_status)}">${safe(i.paper_promotable_status||"NOT_YET")}</span></td><td>${safe(dateText(i.updated_at))}</td></tr>`).join("")}</tbody></table>`; }
    function bindTable() { document.querySelectorAll(".sort").forEach(b=>b.addEventListener("click",()=>{const k=b.dataset.sort;state.direction=state.sort===k&&state.direction==="desc"?"asc":"desc";state.sort=k;state.page=1;saveState(true);loadPage(state.tab)})); document.querySelectorAll(".candidate").forEach(b=>b.addEventListener("click",()=>{const id=decodeURIComponent(b.dataset.id);loadCandidate(id).then(()=>renderCandidateOperations(id));})); document.querySelectorAll(".dataset").forEach(b=>b.addEventListener("click",()=>loadDataset(decodeURIComponent(b.dataset.id)))); }
    async function renderCandidateOperations(id) {
      try {
        const response=await fetch(`/api/v2/candidates/${encodeURIComponent(id)}`,{cache:"no-store"});
        const candidate=await response.json(), provenance=candidate.provenance||{};
        const host=$("candidate-detail"); if(!host)return;
        host.insertAdjacentHTML("afterbegin",`<article id="candidate-operator-actions" class="panel"><div class="section-title"><h2>AUTHORITATIVE ELIGIBILITY</h2><span class="badge ${statusClass(candidate.canary_status)}">${safe(candidate.canary_status||"NOT_ELIGIBLE")}</span></div><div class="key-value"><span class="key">Market / dataset</span><strong>${safe(provenance.market_type)} · ${safe(provenance.dataset_id)} / ${safe(provenance.dataset_version)}</strong></div><p class="page-note">Eligibility is evaluated and persisted by the autonomous ranker. The dashboard cannot mark, arm, or submit a candidate.</p></article>`);
      } catch(error) {}
    }
    async function loadDataset(id,rangePage=1,persist=true) { state.selected=id; state.expanded=true; if(persist)saveState(true); try { const detailResponse=await fetch(`/api/v2/datasets/${encodeURIComponent(id)}`,{cache:"no-store"}),d=await detailResponse.json(),version=d.dataset_version||d.catalog?.dataset_version||"",rangeQuery=new URLSearchParams({page:String(rangePage),page_size:String(state.page_size)}); if(version)rangeQuery.set("dataset_version",version); const rangesResponse=await fetch(`/api/v2/datasets/${encodeURIComponent(id)}/missing-ranges?${rangeQuery}`,{cache:"no-store"}),rangesData=rangesResponse.ok?await rangesResponse.json():{}; const markup=d.available?`<div class="key-value"><span class="key">Dataset</span><strong>${safe(d.dataset_id||id)}</strong></div><div class="key-value"><span class="key">Version</span><strong>${safe(d.dataset_version||d.catalog?.dataset_version)}</strong></div><div class="key-value"><span class="key">Quality</span><span class="badge">${safe(d.catalog?.quality)}</span></div><details open><summary>Health and missing ranges</summary><pre>${safe(json({health:d.health,missing_ranges:arr(rangesData.items)}))}</pre><div id="dataset-ranges-pager" class="pager"></div></details>`:empty("Dataset unavailable",d.error||"Dataset not found"); $("detail").innerHTML=markup; if($("dataset-detail"))$("dataset-detail").innerHTML=markup; if(d.available&&$("dataset-ranges-pager"))pager("dataset-ranges",rangesData); } catch(e) { const markup=empty("Dataset detail unavailable",e.message); $("detail").innerHTML=markup; if($("dataset-detail"))$("dataset-detail").innerHTML=markup; } }
    async function loadHermes(id,persist=true) { state.selected=id; state.expanded=true; if(persist)saveState(true); try { const r=await fetch(`/api/v2/hermes/${encodeURIComponent(id)}`,{cache:"no-store"}),d=await r.json(); const item=arr(d.items)[0],outcome=d.outcome||{},label=outcome.label||item?.outcome_label||"",datasetId=d.dataset_id||item?.dataset_id||"",datasetVersion=d.dataset_version||item?.dataset_version||"",outcomeMarkup=label?`<div class="three-col"><div class="key-value"><span class="key">Outcome</span><strong>${safe(label)}</strong></div><div class="key-value"><span class="key">Dataset ID</span><strong>${safe(datasetId||"—")}</strong></div><div class="key-value"><span class="key">Dataset version</span><strong>${safe(datasetVersion||"—")}</strong></div></div>`:""; $("hermes-detail").innerHTML=item?`${outcomeMarkup}<details open><summary>Hermes item ${safe(id)}</summary><pre>${safe(json(item))}</pre></details>`:empty("Hermes item unavailable","The queue item no longer exists."); } catch(e) { $("hermes-detail").innerHTML=empty("Hermes detail unavailable",e.message); } }
    function renderDatasets(data) { $("dataset-total").textContent=`${count(data.total)} datasets`; const rows=arr(data.items); $("datasets-table").innerHTML=rows.length?`<table><thead><tr>${[["dataset_id","Dataset"],["source_type","Source"],["market_type","Market"],["instrument","Instrument"],["timeframe","Timeframe"],["quality","Quality"],["row_count","Rows"],["updated_at","Updated"]].map(([k,l])=>`<th>${sortButton(k,l)}</th>`).join("")}</tr></thead><tbody>${rows.map(i=>`<tr><td><button class="link dataset" data-id="${encodeURIComponent(i.dataset_id||"")}">${safe(i.dataset_id)}</button></td><td>${safe(i.source_type)}</td><td>${safe(i.market_type)}</td><td>${safe(i.instrument)}</td><td>${safe(i.timeframe)}</td><td>${safe(i.quality)}</td><td>${count(i.row_count)}</td><td>${safe(dateText(i.updated_at))}</td></tr>`).join("")}</tbody></table>`:empty("No datasets","Catalog history has not been initialized."); pager("datasets",data); bindTable(); if(state.tab==="datasets"&&state.selected)loadDataset(state.selected,1,false); }
    function renderActivity(data) { $("activity-total").textContent=`${count(data.total)} events`; $("activity-table").innerHTML=arr(data.items).length?`<table><thead><tr>${[["timestamp","Time"],["kind","Kind"],["message","Activity"]].map(([k,l])=>`<th>${k==="message"?safe(l):sortButton(k,l)}</th>`).join("")}</tr></thead><tbody>${arr(data.items).map(i=>`<tr><td>${safe(dateText(i.timestamp))}</td><td>${safe(i.kind)}</td><td>${safe(i.message)}${i.details&&Object.keys(i.details).length?` <details><summary>details</summary><pre>${safe(json(i.details))}</pre></details>`:""}</td></tr>`).join("")}</tbody></table>`:empty("No research activity","Durable activity will appear after workers run."); pager("activity",data); bindTable(); }
    function renderCandidates(data) { $("candidate-total").textContent=`${count(data.total)} candidates`; const stages=[...new Set(arr(data.items).map(i=>i.stage).filter(Boolean))].sort(),select=$("candidates-stage"),selected=select.value||params.get("stage")||""; select.innerHTML=`<option value="">All stages</option>${stages.map(s=>`<option value="${safe(s)}">${safe(s)}</option>`).join("")}`; if(selected&&!stages.includes(selected))select.insertAdjacentHTML("beforeend",`<option value="${safe(selected)}">${safe(selected)}</option>`); select.value=selected; $("candidates-table").innerHTML=tableCandidates(arr(data.items)); pager("candidates",data); bindTable(); if(state.tab==="candidates"&&state.selected)loadCandidate(state.selected,1,false); }
    function renderPolymarket(data) { const items=arr(data.items),categories=[...new Set(items.map(i=>i.category).filter(Boolean))].sort(),cat=$("polymarket-category"),old=cat.value||params.get("category")||""; cat.innerHTML=`<option value="">All categories</option>${categories.map(c=>`<option value="${safe(c)}">${safe(c)}</option>`).join("")}`; if(old&&!categories.includes(old))cat.insertAdjacentHTML("beforeend",`<option value="${safe(old)}">${safe(old)}</option>`); cat.value=old; const quality=items.map(i=>i.quality||i.research_quality).find(Boolean)||"—"; $("pm-summary").innerHTML=`<div class="three-col"><div class="key-value"><span class="key">Markets</span><strong>${count(data.total)}</strong></div><div class="key-value"><span class="key">Page</span><strong>${count(data.page)}</strong></div><div class="key-value"><span class="key">Quality</span><strong>${safe(quality)}</strong></div></div>`; const sortable={market_id:"market_id",category:"category",settlement:"settlement",quality:"quality"}; $("pm-markets").innerHTML=items.length?`<table><thead><tr>${[["market_id","Market"],["question","Question"],["category","Category"],["yes_mid","YES"],["liquidity","Liquidity"],["settlement","Settlement"],["quality","Quality"]].map(([k,l])=>`<th>${sortable[k]?sortButton(sortable[k],l):safe(l)}</th>`).join("")}</tr></thead><tbody>${items.map(i=>`<tr><td>${safe(i.market_id)}</td><td><details><summary>${safe(String(i.question||i.snapshot?.question||i.market_id).slice(0,90))}</summary><p class="page-note">${safe(i.question||i.snapshot?.question||i.market_id)}</p></details></td><td>${safe(i.category)}</td><td>${safe(i.yes_mid??i.snapshot?.yes_mid??i.payload?.snapshot?.yes_mid)}</td><td>${safe(i.liquidity??i.snapshot?.liquidity??i.payload?.snapshot?.liquidity)}</td><td>${safe(i.settlement??i.snapshot?.settlement??i.payload?.snapshot?.settlement)}</td><td>${safe(i.quality||i.research_quality||"—")}</td></tr>`).join("")}</tbody></table>`:empty("No forward market observations","Run the normal node or collect-data for forward-only quotes."); pager("polymarket",data); bindTable(); }
    function renderHermes(data) { const h=operator.hermes||{},latest=operator.hermes_latest_outcome||{}; $("hermes-summary").innerHTML=`<div class="three-col"><div class="key-value"><span class="key">Submitted</span><strong>${count(h.submitted)}</strong></div><div class="key-value"><span class="key">Accepted</span><strong>${count(h.accepted)}</strong></div><div class="key-value"><span class="key">Pending</span><strong>${count(h.pending)}</strong></div><div class="key-value"><span class="key">Latest outcome</span><strong>${safe(latest.outcome_label||latest.status||"—")}</strong></div><div class="key-value"><span class="key">Selected dataset</span><strong>${safe(latest.dataset_id||"—")} / ${safe(latest.dataset_version||"—")}</strong></div></div><p class="page-note">Hermes status reflects queue execution state, not integration availability. ${safe(h.reason||"")}</p>`; $("hermes-table").innerHTML=arr(data.items).length?`<table><thead><tr>${[["item_id","Item"],["item_type","Type"],["status","Status"],["created_at","Created"]].map(([k,l])=>`<th>${sortButton(k,l)}</th>`).join("")}</tr></thead><tbody>${arr(data.items).map(i=>`<tr><td><button class="link hermes-item" data-id="${encodeURIComponent(i.item_id||"")}">${safe(i.item_id)}</button></td><td>${safe(i.item_type)}</td><td><span class="badge ${statusClass(i.status)}">${safe(i.status)}</span></td><td>${safe(dateText(i.created_at||i.updated_at))}</td></tr>`).join("")}</tbody></table>`:empty("Hermes not initialized","Start the research node or submit a paper-only proposal."); pager("hermes",data); bindTable(); document.querySelectorAll(".hermes-item").forEach(b=>b.addEventListener("click",()=>loadHermes(decodeURIComponent(b.dataset.id)))); if(state.tab==="hermes"&&state.selected)loadHermes(state.selected,false); }
    async function loadCandidate(id,eventPage=1,persist=true) { state.selected=id; state.expanded=true; if(persist)saveState(true); try { const q=new URLSearchParams({page:String(eventPage),page_size:String(state.page_size)}),candidateResponse=await fetch(`/api/v2/candidates/${encodeURIComponent(id)}`,{cache:"no-store"}),r=await fetch(`/api/v2/candidates/${encodeURIComponent(id)}/events?${q}`,{cache:"no-store"}),candidate=candidateResponse.ok?await candidateResponse.json():{},d=await r.json(); const checks=[["Historical gates",candidate.historical_gates||"NOT_PASSED"],["Historical data integrity",candidate.historical_data_integrity||"FAIL"],["Historical execution fidelity",candidate.historical_execution_fidelity||"UNKNOWN"],["Canary data quality",candidate.canary_data_quality_gate||"NOT PASSED"],["Production evidence",candidate.production_evidence||"INSUFFICIENT"],["Micro-live canary",candidate.canary_status||"NOT_ELIGIBLE"],["Paper forward status",candidate.paper_forward_status||"NOT_STARTED"],["Paper promotable",candidate.paper_promotable_status||"NOT_YET"]]; const markup=`<div class="key-value"><span class="key">Candidate</span><strong>${safe(candidate.candidate_id||id)}</strong></div><div class="three-col">${checks.map(([label,value])=>`<div class="key-value"><span class="key">${safe(label)}</span><strong><span class="badge ${statusClass(value)}">${safe(value)}</span></strong></div>`).join("")}</div>${arr(d.items).length?`<table><thead><tr><th>Time</th><th>Stage</th><th>Reason</th></tr></thead><tbody>${arr(d.items).map(i=>`<tr><td>${safe(dateText(i.created_at||i.timestamp))}</td><td><span class="badge">${safe(i.stage||i.to_stage)}</span></td><td>${safe(i.reason||i.message)}</td></tr>`).join("")}</tbody></table>`:empty("No lifecycle events","No persisted lifecycle evidence exists for this candidate.")}<div id="candidate-events-pager" class="pager"></div>`; $("detail").innerHTML=markup; if(state.tab==="candidates")$("dataset-detail").innerHTML=markup; if($("candidate-events-pager")){const total=Number(d.total)||0,page=Number(d.page)||1,size=Number(d.page_size)||state.page_size,pages=Number(d.pages)||0,start=total?(page-1)*size+1:0,end=Math.min(page*size,total); $("candidate-events-pager").innerHTML=`<span>Showing ${start}–${end} of ${total}</span><span><button data-page="${page-1}" ${page<=1?"disabled":""}>Previous</button> <button data-page="${page+1}" ${!pages||page>=pages?"disabled":""}>Next</button></span>`; $("candidate-events-pager").querySelectorAll("button").forEach(b=>b.addEventListener("click",()=>loadCandidate(id,Number(b.dataset.page),false)));} } catch(e) { $("detail").innerHTML=empty("Candidate detail unavailable",e.message); } }
    function renderPaper(data) { const p=operator.paper_portfolio||{}; $("portfolio-summary").innerHTML=`<div class="card-grid"><div class="panel"><div class="metric">${p.state_count?Number(p.total_equity||0).toFixed(2):"—"}</div><div class="metric-label">paper equity</div></div><div class="panel"><div class="metric">${p.state_count?Number(p.total_pnl||0).toFixed(2):"—"}</div><div class="metric-label">paper P/L</div></div><div class="panel"><div class="metric">${count(data.total)}</div><div class="metric-label">paper records</div></div><div class="panel"><div class="metric">${p.state_count?`${(Number(p.win_rate||0)*100).toFixed(1)}%`:"—"}</div><div class="metric-label">win rate</div></div></div>`; $("portfolio-states").innerHTML=arr(data.items).length?`<table><thead><tr><th>${sortButton("timestamp","Time")}</th><th>${sortButton("record_type","Type")}</th><th>Experiment</th><th>Market</th><th>Status</th><th>Details</th></tr></thead><tbody>${arr(data.items).map(i=>`<tr><td>${safe(dateText(i.timestamp||i.created_at||i.updated_at))}</td><td>${safe(i.record_type)}</td><td>${safe(i.experiment_id)}</td><td>${safe(i.market_id||i.symbol)}</td><td><span class="badge ${statusClass(i.status)}">${safe(i.status)}</span></td><td><details><summary>view</summary><pre>${safe(json(i))}</pre></details></td></tr>`).join("")}</tbody></table>`:empty("Waiting for PAPER_FORWARD","Paper portfolio initializes only after a candidate enters PAPER_FORWARD and observations are persisted."); pager("paper",data); bindTable(); }
    function renderCanaryConnectivity(value) {
      const c=value||{}, checkedAt=c.checked_at, checkedPht=dateText(checkedAt), checkedMs=checkedAt?Date.parse(checkedAt):NaN, ageMs=Number.isFinite(checkedMs)?Date.now()-checkedMs:NaN, fresh=Number.isFinite(ageMs)&&ageMs>=0&&ageMs<=60000, displayedStatus=c.status==="READY"&&!fresh?"STALE":c.status, sdk=c.sdk||{}, credentials=c.credentials||{}, authentication=c.authentication||{}, account=c.account||{}, geo=c.geoblock||{}, balance=c.balance||{}, allowance=c.allowance||{}, market=c.market||{}, book=c.order_book||{};
      if(!value){ $("canary-connectivity").innerHTML=empty("No connectivity check persisted","Run Connectivity check to perform a read-only pre-arming check."); return; }
      const failures=arr(c.failure_reasons), failureMarkup=failures.length?`<div class="key-value"><span class="key">Failure codes</span><strong>${safe(arr(c.failure_codes).join(", ")||"—")}</strong></div><div class="key-value"><span class="key">Failure reasons</span><strong>${failures.map(item=>`${safe(item.code)}: ${safe(item.reason)}`).join("<br>")}</strong></div>`:"";
      const marketMarkup=String(market.status||"SKIPPED").toUpperCase()!=="SKIPPED"?`<div class="key-value"><span class="key">Market</span><strong>${safe(market.status)}</strong></div>`:"";
      const bookMarkup=String(book.status||"SKIPPED").toUpperCase()!=="SKIPPED"?`<div class="key-value"><span class="key">Order book</span><strong>${safe(book.status)}</strong></div>`:"";
      $("canary-connectivity").innerHTML=`<article class="panel"><div class="section-title"><h2>CONNECTIVITY</h2><span class="badge ${statusClass(displayedStatus)}">${safe(displayedStatus||"BLOCKED")}</span></div><div class="three-col"><div class="key-value"><span class="key">SDK</span><strong>${safe(sdk.status)} · ${safe(sdk.name)} · ${safe(sdk.version)}</strong></div><div class="key-value"><span class="key">Credentials</span><strong>${safe(credentials.status)}</strong></div><div class="key-value"><span class="key">Authentication</span><strong>${safe(authentication.status)}</strong></div><div class="key-value"><span class="key">Account</span><strong>${safe(account.status)}${account.wallet_type?` · ${safe(account.wallet_type)}`:""}</strong></div><div class="key-value"><span class="key">Geoblock</span><strong>${safe(geo.status)}${geo.country?` · ${safe(geo.country)}`:""}${geo.region?` / ${safe(geo.region)}`:""}</strong></div><div class="key-value"><span class="key">Balance</span><strong>${safe(balance.status)}${balance.available_usd!=null?` · ${usd(balance.available_usd)}`:""}</strong></div><div class="key-value"><span class="key">Allowance</span><strong>${safe(allowance.status)}</strong></div>${marketMarkup}${bookMarkup}<div class="key-value"><span class="key">Checked</span><strong>${safe(dateText(c.checked_at))}</strong></div></div>${failureMarkup?`<p class="page-note">${failureMarkup}</p>`:""}</article>`;
    }
    async function binanceControlPost(action,payload={}) {
      const node=$("binance-action-result");
      try {
        const response=await fetch("/api/binance/control",{method:"POST",headers:{"Content-Type":"application/json","X-Axiom-Control-Token":controlToken},body:JSON.stringify({action,payload}),cache:"no-store"});
        const result=await response.json();
        if(node)node.textContent=result.ok?`${action} completed · ${result.action_id||"persisted"}`:`${action} blocked: ${result.reason||"CONTROL_FAILED"}`;
        if(activeController)activeController.abort();
        refreshGeneration++; activeController=null; loadInFlight=false; clearTimeout(slowRefreshTimer); slowRefreshTimer=null; nextRefreshAt=0;
        if(state.tab==="binance-canary")await loadPage("binance-canary",true);
        return result;
      } catch(error) {
        if(node)node.textContent=`${action} unavailable: ${error?.message||"network failure"}`;
        return {ok:false,reason:"BINANCE_CONTROL_UNAVAILABLE"};
      }
    }
    function binanceRecordTable(title,rows) {
      const values=arr(rows), keys=[...new Set(values.flatMap(item=>Object.keys(item||{})).filter(key=>!/(?:secret|token|password|credential|authorization)/i.test(key)))].slice(0,8);
      if(!values.length)return empty(`No ${title.toLowerCase()}`,"No bounded records are available.");
      const visible=values.slice(0,100);
      return `<article class="panel"><div class="section-title"><h3>${safe(title)}</h3><span class="badge">${visible.length} shown</span></div><table><thead><tr>${keys.map(key=>`<th>${safe(key.replaceAll("_"," "))}</th>`).join("")}</tr></thead><tbody>${visible.map(item=>`<tr>${keys.map(key=>`<td>${safe(typeof item[key]==="object"?JSON.stringify(item[key]):item[key])}</td>`).join("")}</tr>`).join("")}</tbody></table><details><summary>Full IDs and bounded detail records</summary><pre>${safe(json(visible))}</pre></details></article>`;
    }
    function setBinanceNavLabel(label) { const tab=document.querySelector('nav button.tab[data-view="binance-canary"]'); if(tab)tab.textContent=label; }
    function renderBinanceCanary(data) {
      setBinanceNavLabel("BINANCE SPOT CANARY");
      const status=data?.status&&typeof data.status==="object"?data.status:data||{}, profile=data.profile||data.development_profile||status.profile||status.development_profile||{}, connectivity=data.connectivity||status.connectivity||{}, readiness=data.readiness||status.readiness||{}, qualification=data.qualification||status.qualification||{}, risk=data.risk||data.budgets||status.risk||status.budgets||{}, heartbeat=data.heartbeat||status.heartbeat||{}, signal=data.latest_signal||status.latest_signal||null, actions=arr(data.actions||status.actions);
      const identityRows=[["Environment",profile.environment||"PAPER / TESTNET"],["Instance",profile.feature_instance||profile.runtime_identity||profile.runtime||"binance-dev"],["DB path",profile.db_path||"—"],["Schema revision",profile.schema_revision||profile.revision||"unknown"],["Transport","BINANCE SPOT ENABLED · POLYMARKET DISABLED"],["Credentials",data.credentials?.configured?"CONFIGURED (safe status only)":"NOT CONFIGURED"]];
      $("binance-identity").innerHTML=`<article class="panel"><div class="section-title"><h2>DEVELOPMENT / PAPER|TESTNET IDENTITY</h2><span class="badge warn">${safe(profile.environment||"PAPER|TESTNET")}</span></div><div class="three-col">${identityRows.map(([key,value])=>`<div class="key-value"><span class="key">${safe(key)}</span><strong>${safe(value)}</strong></div>`).join("")}</div><p class="page-note">Status ${safe(dateText(data.timestamp||status.timestamp))} · UTC→PHT display enabled</p></article>`;
      $("binance-connectivity").innerHTML=`<article class="panel"><div class="section-title"><h2>READ-ONLY CONNECTIVITY / READINESS</h2><span class="badge ${statusClass(readiness.status||connectivity.readiness||connectivity.status)}">${safe(readiness.status||connectivity.readiness||connectivity.status||"UNKNOWN")}</span></div><div class="three-col"><div class="key-value"><span class="key">Connectivity</span><strong>${safe(connectivity.status||"UNKNOWN")} · ${connectivity.stale?"STALE":"CURRENT"}</strong></div><div class="key-value"><span class="key">Checked UTC / PHT</span><strong>${safe(connectivity.checked_at||"—")} · ${safe(dateText(connectivity.checked_at))}</strong></div><div class="key-value"><span class="key">Heartbeat</span><strong>${safe(heartbeat.status||"UNKNOWN")} · ${safe(dateText(heartbeat.timestamp||heartbeat.heartbeat_at))}</strong></div></div><p class="page-note"><button class="binance-action" data-binance-action="CONNECTIVITY_CHECK">Connectivity check (read-only)</button> · no order placement</p></article>`;
      const selected=qualification.selection||status.selection||null, selectedId=selected?.candidate_id||selected?.strategy_id||selected?.id||"—";
      $("binance-qualification").innerHTML=`<article class="panel"><div class="section-title"><h2>QUALIFICATION / RANKING</h2><span class="badge ${statusClass(qualification.current_vs_stale||qualification.selection_status)}">${safe(qualification.current_vs_stale||qualification.selection_status||"NONE")}</span></div><div class="three-col"><div class="key-value"><span class="key">Eligibility</span><strong>${safe(qualification.eligible_count??qualification.eligibility??"—")}</strong></div><div class="key-value"><span class="key">Rankable</span><strong>${safe(qualification.rankable_count??arr(qualification.rankable).length)}</strong></div><div class="key-value"><span class="key">Selection / family</span><strong>${safe(selectedId)} · ${safe(qualification.family||selected?.family||"—")}</strong></div><div class="key-value"><span class="key">Reason</span><strong>${safe(qualification.reason||status.reason||"—")}</strong></div></div><details><summary>Ranking selection and reasons</summary><pre>${safe(json({qualification,selection:selected}))}</pre></details></article>`;
      const limits=risk.limits||risk.envelope||{}, remaining=risk.remaining||{}, riskKeys=[...new Set([...Object.keys(limits),...Object.keys(remaining)])].slice(0,32);
      $("binance-risk").innerHTML=`<article class="panel"><div class="section-title"><h2>RISK ENVELOPE / BUDGETS</h2><span class="badge">bounded</span></div><div class="three-col">${riskKeys.map(key=>`<div class="key-value"><span class="key">${safe(key.replaceAll("_"," "))}</span><strong>${safe(limits[key]??"—")} / remaining ${safe(remaining[key]??"—")}</strong></div>`).join("")}<div class="key-value"><span class="key">Net PnL / fees</span><strong>${safe(risk.net_pnl||"—")} / ${safe(risk.fees||"—")}</strong></div><div class="key-value"><span class="key">Exposure / reservations</span><strong>${safe(risk.exposure||"—")} / ${safe(risk.reservations||"—")}</strong></div></div></article>`;
      const currentState=String(status.control?.state||status.state||"UNKNOWN"), confirm=String(status.enable_phrase||"ENABLE BINANCE AUTO CANARY");
      $("binance-controls").innerHTML=`<article class="panel"><div class="section-title"><h2>BINANCE CONTROL</h2><span class="badge ${statusClass(currentState)}">${safe(currentState)}</span></div><div class="filters"><input id="binance-confirm" aria-label="Exact enable phrase" placeholder="${safe(confirm)}"><input id="binance-order-symbol" aria-label="Order validation symbol" placeholder="BTCUSDT"><input id="binance-order-price" aria-label="Order validation price" placeholder="price"><input id="binance-order-quantity" aria-label="Order validation quantity" placeholder="quantity"></div><p class="page-note"><button class="binance-action" data-binance-action="ORDER_VALIDATION_TEST">Order validation test</button> <button class="binance-action" data-binance-action="ENABLE">ENABLE</button> <button class="binance-action" data-binance-action="PAUSE">PAUSE</button> <button class="binance-action" data-binance-action="RESUME">RESUME</button> <button class="binance-action" data-binance-action="DISARM">DISARM</button> <button class="binance-action danger" data-binance-action="KILL">KILL</button></p><p class="page-note">Enable/resume require the exact phrase: <code>${safe(confirm)}</code>. Actions are persisted per action.</p></article>`;
      const records=[binanceRecordTable("Positions",data.positions?.items||status.positions?.items||data.positions),binanceRecordTable("Orders",data.orders?.items||status.orders?.items||data.orders),binanceRecordTable("Fills",data.fills?.items||status.fills?.items||data.fills),binanceRecordTable("UNKNOWN orders",data.unknown?.items||status.unknown?.items||data.unknown)].join("");
      $("binance-records").innerHTML=`<div class="three-col">${records}</div><article class="panel"><div class="section-title"><h3>LATEST SIGNAL / NO-TRADE</h3><span class="badge ${statusClass(signal?.status||"UNKNOWN")}">${safe(signal?.status||"UNKNOWN")}</span></div><div class="key-value"><span class="key">Signal</span><strong>${safe(signal?.signal_id||signal?.id||"—")}</strong></div><div class="key-value"><span class="key">No-trade reason</span><strong>${safe(data.no_trade_reason||status.no_trade_reason||signal?.no_trade_reason||"—")}</strong></div><div class="key-value"><span class="key">Pause / disarm / kill</span><strong>${status.pause?"PAUSED":"RUNNING"} / ${status.disarmed?"DISARMED":"ARMED"} / ${status.killed?"KILLED":"NOT KILLED"}</strong></div></article>`;
      $("binance-raw").textContent=json({status,actions});
    }
    const _renderBinanceCanaryPaper = renderBinanceCanary;
    function _binanceTestnetData(data) {
      const status=data?.status&&typeof data.status==="object"?data.status:{};
      return status.strict_testnet===true||data?.strict_testnet===true;
    }
    function _binanceTestnetValue(value) {
      return value&&typeof value==="object"?safe(json(value)):safe(value);
    }
    function _binanceTestnetField(object, keys) {
      const source=object&&typeof object==="object"?object:{};
      for(const key of keys) {
        const value=source[key];
        if(value!==undefined&&value!==null&&value!=="")return _binanceTestnetValue(value);
      }
      return "—";
    }
    function renderBinanceTestnet(data) {
      setBinanceNavLabel("BINANCE SPOT TESTNET");
      const status=data?.status&&typeof data.status==="object"?data.status:{}, profile=data?.profile||status.profile||{}, credentials=data?.credentials||status.credentials||{}, connectivity=data?.connectivity||status.connectivity||{}, validation=data?.validation||status.validation||{}, probe=data?.probe||status.probe||{}, isolation=data?.isolation||status.isolation||{}, autonomous=data?.autonomous||status.autonomous||{}, account=connectivity.account||status.account||{}, entry=probe.intent||probe.entry||probe.buy||{}, exit=probe.exit||probe.exit_order||probe.sell||{}, orders=probe.orders||probe.order_records||[], fills=probe.fills||probe.trade_fills||probe.fill_records||[], reconciliation=probe.reconciliation||probe.reconcile||status.reconciliation||{}, actions=arr(data?.actions||status.actions);
      const checked=connectivity.checked_at||connectivity.timestamp||status.checked_at||status.timestamp||{}, checkedPht=typeof checked==="object"?checked.pht:checked, balances=account.balances||connectivity.balances||[], state=autonomous.state||status.control?.state||status.state||"DISARMED";
      document.querySelector("#view-binance-canary > article > .section-title h2")?.replaceChildren(document.createTextNode("BINANCE SPOT TESTNET"));
      const badge=document.querySelector("#view-binance-canary > article > .section-title .badge"); if(badge)badge.textContent="TESTNET / LOCALHOST ONLY";
      $("binance-identity").innerHTML=`<article class="panel"><div class="section-title"><h2>BINANCE SPOT TESTNET</h2><span class="badge warn">${safe(profile.environment||"TESTNET")}</span></div><div class="three-col"><div class="key-value"><span class="key">Environment</span><strong>TESTNET</strong></div><div class="key-value"><span class="key">Profile</span><strong>${_binanceTestnetField(profile,["identity","name","runtime_identity","profile"])}</strong></div><div class="key-value"><span class="key">Database</span><strong>${_binanceTestnetField(profile,["db_path","database","database_path"])}</strong></div><div class="key-value"><span class="key">Configured</span><strong>${credentials.configured===true?"CONFIGURED":"NOT CONFIGURED"}</strong></div><div class="key-value"><span class="key">Isolation</span><strong>${_binanceTestnetField(isolation,["status","reason","boundary"])}</strong></div><div class="key-value"><span class="key">Credentials</span><strong>STATUS ONLY · VALUES NEVER RENDERED</strong></div></div><p class="page-note">Status ${safe(typeof checked==="object"?(checked.utc||"—"):checked)} · localhost control token required.</p></article>`;
      $("binance-connectivity").innerHTML=`<article class="panel"><div class="section-title"><h2>TESTNET CONNECTIVITY</h2><span class="badge ${statusClass(connectivity.status||"BLOCKED")}">${safe(connectivity.status||"BLOCKED")}</span></div><div class="three-col"><div class="key-value"><span class="key">Configured status</span><strong>${credentials.configured===true?"CONFIGURED":"NOT CONFIGURED"}</strong></div><div class="key-value"><span class="key">Authentication</span><strong>${_binanceTestnetField(connectivity,["authentication","auth_status","reason"])}</strong></div><div class="key-value"><span class="key">Account / Spot</span><strong>${_binanceTestnetField(account,["account_type","type"])} · ${account.can_trade===true?"CAN TRADE":"BLOCKED"}</strong></div><div class="key-value"><span class="key">Server time</span><strong>${_binanceTestnetField(connectivity,["server_time_ms","server_time","serverTime"])}</strong></div><div class="key-value"><span class="key">Bounded balances</span><strong>${Array.isArray(balances)?`${balances.length} shown`:_binanceTestnetValue(balances)}</strong></div><div class="key-value"><span class="key">Check PHT</span><strong>${safe(checkedPht||"—")}</strong></div><div class="key-value"><span class="key">Reason</span><strong>${_binanceTestnetField(connectivity,["reason","error"])}</strong></div></div>${Array.isArray(balances)&&balances.length?`<details><summary>Bounded balances</summary><pre>${safe(json(balances.slice(0,64)))}</pre></details>`:""}</article>`;
      $("binance-qualification").innerHTML=`<article class="panel"><div class="section-title"><h2>ORDER VALIDATION</h2><span class="badge ${statusClass(validation.status||"BLOCKED")}">${safe(validation.status||"BLOCKED")}</span></div><div class="three-col"><div class="key-value"><span class="key">Symbol</span><strong>${_binanceTestnetField(validation,["symbol"])}</strong></div><div class="key-value"><span class="key">Side</span><strong>${_binanceTestnetField(validation,["side","order_side"])}</strong></div><div class="key-value"><span class="key">Price</span><strong>${_binanceTestnetField(validation,["price"])}</strong></div><div class="key-value"><span class="key">Quantity</span><strong>${_binanceTestnetField(validation,["quantity"])}</strong></div><div class="key-value"><span class="key">Fee reserve</span><strong>${_binanceTestnetField(validation,["fee_reserve","fee","fee_reservation"])}</strong></div><div class="key-value"><span class="key">Reservation</span><strong>${_binanceTestnetField(validation,["reservation","risk_reservation","planned_exit"])}</strong></div><div class="key-value"><span class="key">Status</span><strong>${_binanceTestnetField(validation,["status","reason"])}</strong></div></div></article>`;
      $("binance-risk").innerHTML=`<article class="panel"><div class="section-title"><h2>TESTNET EXECUTION PROBE</h2><span class="badge ${statusClass(probe.status||"BLOCKED")}">${safe(probe.status||"BLOCKED")}</span></div><div class="three-col"><div class="key-value"><span class="key">Probe label</span><strong>${_binanceTestnetField(probe,["label","probe_kind","name"])}</strong></div><div class="key-value"><span class="key">Entry exchange ID</span><strong>${_binanceTestnetField(entry,["exchange_order_id","exchangeOrderId","order_id"])}</strong></div><div class="key-value"><span class="key">Entry client ID</span><strong>${_binanceTestnetField(entry,["client_order_id","clientOrderId","newClientOrderId"])}</strong></div><div class="key-value"><span class="key">Fills / fees</span><strong>${Array.isArray(fills)?`${fills.length} fills · ${_binanceTestnetField(probe,["fee_paid","fees","commission"])}`:_binanceTestnetField(probe,["fills","fees"])}</strong></div><div class="key-value"><span class="key">Owned quantity</span><strong>${_binanceTestnetField(probe,["owned_quantity","owned_qty","quantity_owned"])}</strong></div><div class="key-value"><span class="key">Exit</span><strong>${_binanceTestnetField(exit,["state","status","reason"])}</strong></div><div class="key-value"><span class="key">Exit exchange / client IDs</span><strong>${_binanceTestnetField(exit,["exchange_order_id","client_order_id","order_id"])}</strong></div><div class="key-value"><span class="key">Realized PnL</span><strong>${_binanceTestnetField(probe,["realized_pnl","realizedPnL"])}</strong></div><div class="key-value"><span class="key">Reconciliation</span><strong>${_binanceTestnetField(reconciliation,["status","reason","state"])}</strong></div><div class="key-value"><span class="key">DUST</span><strong>${String(probe.status||exit.state||exit.status||"").toUpperCase()==="DUST"?"DUST":"—"}</strong></div></div>${orders.length||fills.length?`<details><summary>Probe orders and fills</summary><pre>${safe(json({orders:orders.slice(0,100),fills:fills.slice(0,100)}))}</pre></details>`:""}</article>`;
      $("binance-controls").innerHTML=`<article class="panel"><div class="section-title"><h2>AUTONOMOUS TESTNET</h2><span class="badge ${statusClass(state)}">${safe(state)}</span></div><p class="page-note">Price and quantity are computed automatically from Binance exchange filters and the frozen bounded Testnet envelope.</p><p class="page-note"><button class="binance-action" data-binance-action="CONNECTIVITY_CHECK">Connectivity check</button> <button class="binance-action" data-binance-action="ORDER_VALIDATION_TEST">Validate order</button> <button class="binance-action" data-binance-action="PAUSE">Pause</button> <button class="binance-action" data-binance-action="DISARM">Disarm</button> <button class="binance-action danger" data-binance-action="KILL">Kill</button></p><p class="notice">Execution probe and reconciliation actions are CLI/runtime-only. Browser controls are limited to read-only connectivity, order validation, and risk-reducing pause, disarm, or kill.</p></article><article class="panel"><div class="section-title"><h2>AUTONOMOUS TESTNET STATUS</h2><span class="badge ${statusClass(autonomous.state||state)}">${safe(autonomous.state||state)}</span></div><div class="three-col"><div class="key-value"><span class="key">Enabled</span><strong>${_binanceTestnetField(autonomous,["enabled"])}</strong></div><div class="key-value"><span class="key">State</span><strong>${_binanceTestnetField(autonomous,["state"])}</strong></div><div class="key-value"><span class="key">Blocked reason</span><strong>${_binanceTestnetField(autonomous,["blocked_reason","blocker","reason"])}</strong></div><div class="key-value"><span class="key">Selected candidate</span><strong>${_binanceTestnetField(autonomous,["selected_candidate","candidate"])}</strong></div><div class="key-value"><span class="key">Current signal</span><strong>${_binanceTestnetField(autonomous,["current_signal","signal"])}</strong></div><div class="key-value"><span class="key">No-trade reason</span><strong>${_binanceTestnetField(autonomous,["no_trade_reason"])}</strong></div><div class="key-value"><span class="key">Frozen envelope</span><strong>${_binanceTestnetField(autonomous,["risk_envelope","frozen_envelope","risk"])}</strong></div><div class="key-value"><span class="key">Bounded window</span><strong>${_binanceTestnetField(autonomous,["bounded_window","window","window_seconds"])}</strong></div></div></article>`;
      $("binance-controls").firstElementChild?.insertAdjacentHTML("beforeend",'<p class="page-note">Autonomous enable/resume is CLI-only; window-seconds 30..900.</p>');
      const records=[binanceRecordTable("TESTNET PROBE ORDERS",orders),binanceRecordTable("TESTNET PROBE FILLS",fills)].join("");
      $("binance-records").innerHTML=`<div class="three-col">${records}</div><article class="panel"><div class="section-title"><h3>TESTNET ISOLATION</h3><span class="badge">${safe(_binanceTestnetField(isolation,["status","reason","boundary"]))}</span></div><p class="page-note">Probe evidence is isolated from strategy signals and execution ledgers. No Polymarket transport is available.</p><pre>${safe(json({isolation,actions:actions.slice(0,100)}))}</pre></article>`;
      $("binance-raw").textContent=json({status,connectivity,validation,probe,isolation,autonomous,actions});
    }
    renderBinanceCanary = function(data) {
      binanceTestnetMode = _binanceTestnetData(data);
      if(binanceTestnetMode) { renderBinanceTestnet(data); return; }
      _renderBinanceCanaryPaper(data);
    };

    function renderCanarySetup(data) {
      const payload=data&&typeof data==="object"?data:{}, canary=payload.canary&&typeof payload.canary==="object"?payload.canary:{};
      // Keep this setup boundary explicit: the canary view owns the
      // connectivity/settings sub-surfaces, while the autonomous renderer
      // below owns the bounded readiness and execution summary.
      renderCanaryConnectivity(payload.connectivity??canary.connectivity??null);
      renderRiskSettings(payload);
      const readiness=payload.readiness&&typeof payload.readiness==="object"?payload.readiness:{};
      const updated=payload.readiness_snapshot_updated_at??canary.readiness_snapshot_updated_at??readiness.updated_at;
      if($("canary-readiness-snapshot"))$("canary-readiness-snapshot").innerHTML=readinessSnapshotMarkup({...payload,canary:{...canary,readiness_snapshot_updated_at:updated}});
    }
    function renderRiskSettings(data) {
      const snapshot=data?.risk_settings||data?.canary?.risk_settings||{},
        active=snapshot.active||snapshot.active_config||{},
        rawDraft=snapshot.draft||snapshot.draft_config||null,
        draft=rawDraft&&typeof rawDraft==="object"&&rawDraft.config_id?rawDraft:null,
        source=(draft?.values&&typeof draft.values==="object"?draft.values:(draft?.limits&&typeof draft.limits==="object"?draft.limits:(active.values&&typeof active.values==="object"?active.values:(active.limits&&typeof active.limits==="object"?active.limits:(snapshot.effective_limits||snapshot.active_limits||{})))));
      const value=(name,fallback="")=>source?.[name]??active?.[name]??fallback;
      const submissionValue=Number(value("max_submitted_orders_per_day",value("max_orders_per_day","5")));
      const submissionPreset=[5,10,20].includes(submissionValue)?String(submissionValue):"custom";
      const fields=[
        ["max_all_in_buy_usd","Maximum all-in buy","decimal"],
        ["max_gross_daily_buy_usd","Gross daily buy budget","decimal"],
        ["max_aggregate_exposure_usd","Open exposure","decimal"],
        ["max_positions","Maximum positions","number"],
        ["realized_loss_entry_stop_usd","Realized loss stop","decimal"],
        ["equity_loss_entry_stop_usd","Equity loss stop","decimal"],
        ["max_slippage_bps","Slippage (bps)","number"]
      ];
      const advanced=[
        ["max_fee_reserve_usd","Fee reserve","decimal"],
        ["per_market_buy_cap_usd","Per-market buy limit","decimal"],
        ["per_event_buy_cap_usd","Per-event buy limit","decimal"],
        ["cumulative_buy_cap_usd","Cumulative buy limit","decimal"]
      ];
      riskReview={
        active:{
          values:active.values||active.settings||snapshot.effective_limits||snapshot.active_limits||{},
          configId:active.config_id??active.id??snapshot.config_id??snapshot.active_config_id??null,
          generation:active.generation??snapshot.generation??null,
          hash:active.config_hash??snapshot.config_hash??null,
          controlGeneration:snapshot.control_generation??active.control_generation??null
        },
        draft:draft&&draft.config_id?{
          values:draft.values||draft.settings||{},
          configId:draft.config_id??draft.id??null,
          generation:draft.generation??null,
          hash:draft.config_hash??null
        }:null
      };
      const optionalAdvanced=new Set(["per_market_buy_cap_usd","per_event_buy_cap_usd","cumulative_buy_cap_usd"]);
      const input=(name,label,type="decimal",current="")=>{const display=current==null?"":String(current),optional=optionalAdvanced.has(name),marker=optional?' data-risk-optional="clearable"':"";return `<label class="key-value"><span class="key">${safe(label)}</span><input data-risk-field="${safe(name)}"${marker} aria-label="${safe(label)}" value="${safe(display)}" inputmode="${type==="number"?"numeric":"decimal"}"></label>`;};
      const reviewFields=[["max_submitted_orders_per_day","Submissions/day","number"],...fields,...advanced];
      riskReview.labels=Object.fromEntries(reviewFields.map(([name,label])=>[name,label]));
      const reviewValues=riskReview.draft?.values||{};
      const activeValues=riskReview.active.values||{};
      const diff=riskReview.draft?reviewFields.map(([name,label])=>({name,label,before:activeValues[name],after:reviewValues[name]})).filter(item=>JSON.stringify(item.before??null)!==JSON.stringify(item.after??null)):[];
      const diffMarkup=diff.length
        ? `<ul>${diff.map(item=>`<li>${safe(item.label)}: ${safe(item.before??"")} → <strong>${safe(item.after??"")}</strong></li>`).join("")}</ul>`
        : `<p class="page-note">No saved changes are waiting for activation.</p>`;
      const status=String(snapshot.status||"CURRENT").toUpperCase();
      $("risk-settings").innerHTML=`<article class="panel"><div class="section-title"><h2>POLYMARKET RISK SETTINGS</h2><span class="badge ${statusClass(status)}">${safe(status)}</span></div><p class="page-note">Edit one bounded setting at a time, review the exact changes, then confirm activation. Active limits remain authoritative until activation succeeds.</p><div class="three-col"><label class="key-value"><span class="key">Submissions/day</span><select data-risk-field="max_submitted_orders_per_day" aria-label="Submissions per day"><option value="5"${submissionPreset==="5"?" selected":""}>5</option><option value="10"${submissionPreset==="10"?" selected":""}>10</option><option value="20"${submissionPreset==="20"?" selected":""}>20</option><option value="custom"${submissionPreset==="custom"?" selected":""}>Custom</option></select><input data-risk-submissions-custom aria-label="Custom submissions per day" value="${submissionPreset==="custom"?safe(submissionValue):""}" inputmode="numeric"${submissionPreset==="custom"?"":" hidden"}></label>${fields.map(([name,label,type])=>input(name,label,type,value(name))).join("")}</div><details><summary>Optional advanced limits</summary><div class="three-col">${advanced.map(([name,label,type])=>input(name,label,type,value(name))).join("")}</div></details><div class="filters"><label class="key-value"><span class="key">Operator</span><strong>Authenticated operator</strong></label><button class="risk-settings-action" data-risk-action="save">Review changes</button></div><div id="risk-review" class="page-note"><strong>Activation review</strong>${diffMarkup}</div>${riskReview.draft?`<p class="page-note"><button class="risk-settings-action" data-risk-action="activate">Confirm activation</button></p>`:""}</article>`;
      const submissions=$("[data-risk-field='max_submitted_orders_per_day']"),custom=$("[data-risk-submissions-custom]");
      submissions?.addEventListener("change",()=>{if(custom){custom.hidden=submissions.value!=="custom";if(submissions.value!=="custom")custom.value="";}});
    }
    function renderCanary(data) {
      renderCanarySetup(data);
      const payload=data&&typeof data==="object"?data:{}, c=payload.canary&&typeof payload.canary==="object"?payload.canary:{}, auto=payload.autonomous_canary&&typeof payload.autonomous_canary==="object"?payload.autonomous_canary:(c.autonomous&&typeof c.autonomous==="object"?c.autonomous:{});
      const control=payload.control&&typeof payload.control==="object"?payload.control:(c.control&&typeof c.control==="object"?c.control:{});
      const connectivity=payload.connectivity&&typeof payload.connectivity==="object"?payload.connectivity:(c.connectivity&&typeof c.connectivity==="object"?c.connectivity:null);
      const risk=c.risk_envelope&&typeof c.risk_envelope==="object"?c.risk_envelope:(c.risk_limits&&typeof c.risk_limits==="object"?c.risk_limits:{});
      const stateValue=String(c.control_state??control.state??c.micro_live_canary??"UNKNOWN").toUpperCase();
      const backendState=stateValue, enabled=stateValue==="AUTONOMOUS_MICRO_LIVE"||stateValue==="ENABLED";
      const selectionStatus=String(c.selection_status??control.selection_status??"UNKNOWN").toUpperCase();
      const selectionValid=c.selection_valid===true&&selectionStatus==="CURRENT";
      const signal=payload.canary_signal??c.latest_signal??null;
      const currentCandidate=selectionValid?c.selected_candidate||"": "";
      const currentWinnerId=selectionValid?c.winner_id||"": "";
      const historicalCandidate=c.last_selected_candidate||"";
      const selectionLabel=selectionStatus==="STALE"?"STALE · REEVALUATION REQUIRED":selectionStatus==="CURRENT"?"CURRENT":c.selection_invalidation_reason==="NO_ELIGIBLE_CANDIDATES"?"NO_ELIGIBLE_CANDIDATES":"UNKNOWN";
      const rawEligible=c.eligibility_raw_count==null?null:Number(c.eligibility_raw_count);
      const eligible=c.eligible_count==null?null:Number(c.eligible_count);
      const rawRankable=c.rankable_raw_count==null?null:Number(c.rankable_raw_count);
      const rankable=c.rankable_count==null?null:Number(c.rankable_count);
      const events=data.real_execution_events??c.real_execution_events??c.execution_event_count??null;
      const manualCandidate=backendState==="ARMED"&&c.candidate?`<div class="panel"><div class="metric">${safe(c.candidate)}</div><div class="metric-label">Manual armed candidate</div></div>`:"";
      const connectivityCheckedAt=connectivity?.checked_at?Date.parse(connectivity.checked_at):NaN;
      const connectivityAgeMs=Number.isFinite(connectivityCheckedAt)?Date.now()-connectivityCheckedAt:NaN;
      const connectivityFresh=Number.isFinite(connectivityAgeMs)&&connectivityAgeMs>=0&&connectivityAgeMs<=60000;
      const connectivityReady=connectivity?.ready===true&&connectivity?.status==="READY"&&connectivityFresh;
      const connectivityBlocker=connectivityReady?"":connectivity?.ready===true&&!connectivityFresh?"CONNECTIVITY_CHECK_STALE":arr(connectivity?.failure_codes)[0]||"CONNECTIVITY_BLOCKED";
      const selectionReason=c.selection_invalidation_reason||"", selectionBlocker=selectionValid&&currentCandidate?"":(selectionReason||(selectionStatus==="STALE"?"REEVALUATION_REQUIRED":selectionStatus==="UNKNOWN"?"READINESS_UNKNOWN":selectionStatus==="NONE"?"NO_CURRENT_SELECTION":"SELECTION_INVALID"));
      const currentRank=selectionValid&&selectionStatus==="CURRENT"?auto.rank:"—", currentScore=selectionValid&&selectionStatus==="CURRENT"?auto.score:"—", selectionReasonLabel=c.selection_reason||"—", historicalMarkup=historicalCandidate?`<div class="panel"><div class="metric">${safe(historicalCandidate)}</div><div class="metric-label">Selected winner · Historical selected ID</div><p class="page-note"><span class="badge ${statusClass(selectionStatus)}">${safe(selectionLabel)}</span></p></div>`:"";
      const autonomousBlocker=enabled?"ENABLED":!connectivity?"CONNECTIVITY_CHECK_REQUIRED":!connectivityReady?connectivityBlocker:backendState==="KILLED"?"CANARY_KILLED":selectionBlocker||String(auto.blocker||"AUTONOMOUS_CANARY_DISABLED");
      const autoReady=connectivityReady&&backendState!=="KILLED"&&!enabled&&selectionValid&&Boolean(currentCandidate);
      const activeBinding=riskReview.active||{};
      const bindingReady=Boolean(activeBinding.configId)&&Number.isInteger(Number(activeBinding.generation))&&Number(activeBinding.generation)>0;
      const enable=enabled?controlButton("canary.disarm","DISARM","","DISARM"):autoReady&&bindingReady?`<button class="risk-settings-action" data-risk-action="enable">Review active limits and enable</button>`:"";
      const riskMarkup=Object.entries(risk).map(([key,value])=>`<div class="key-value"><span class="key">${safe(key.replaceAll("_"," "))}</span><strong>${safe(value)}</strong></div>`).join("")||empty("Risk envelope unavailable","No frozen risk limits are persisted.");
      $("canary-controls").innerHTML=`<article class="panel"><div class="section-title"><h2>AUTONOMOUS CANARY CONTROL</h2><span class="badge ${statusClass(stateValue)}">${safe(stateValue)}</span></div><p class="page-note"><strong>${safe(enabled?"AUTO CANARY ENABLED":autoReady?"AUTO CANARY READY TO ENABLE":`AUTO CANARY BLOCKED: ${autonomousBlocker}`)}</strong></p><div class="page-note">${controlButton("canary.connectivity_check","Connectivity check")} · ${enable} · ${controlButton("canary.kill","KILL","","KILL")}</div><p class="page-note">One confirmation enables the prediction-only envelope using the active venue settings. Research, eligibility, ranking, and submission decisions run in the node worker; Hermes cannot change this envelope.</p></article>`;
      $("canary-summary").innerHTML=`<div class="card-grid"><div class="panel"><div class="metric">${safe(stateValue)}</div><div class="metric-label">Autonomous canary state</div></div>${currentCandidate?`<div class="panel"><div class="metric">${safe(currentCandidate)}</div><div class="metric-label">Selected winner · Current selection</div><p class="page-note"><span class="badge ${statusClass(selectionStatus)}">${safe(selectionLabel)}</span></p></div>`:historicalMarkup||`<div class="panel"><div class="metric">—</div><div class="metric-label">Current selection</div><p class="page-note"><span class="badge ${statusClass(selectionStatus)}">${safe(selectionLabel)}</span></p></div>`}${manualCandidate}<div class="panel"><div class="metric">${safe(currentRank)} · ${safe(currentScore)}</div><div class="metric-label">Current rank / score</div></div><div class="panel"><div class="metric">${count(rawEligible)}</div><div class="metric-label">Eligible candidates (raw)</div></div><div class="panel"><div class="metric">${count(eligible)}</div><div class="metric-label">Eligible candidates (validated)</div></div><div class="panel"><div class="metric">${count(rawRankable)}</div><div class="metric-label">Rankable candidates (raw)</div></div><div class="panel"><div class="metric">${count(rankable)}</div><div class="metric-label">Rankable candidates (validated)</div></div><div class="panel"><div class="metric">${count(events)}</div><div class="metric-label">Real execution events</div></div></div><article class="panel"><div class="section-title"><h2>Autonomous readiness</h2><span class="badge ${statusClass(autonomousBlocker)}">${safe(autoReady?"READY":autonomousBlocker)}</span></div><div class="three-col"><div class="key-value"><span class="key">Selection status</span><strong>${safe(selectionLabel)}</strong></div><div class="key-value"><span class="key">Selection valid</span><strong>${safe(selectionValid)}</strong></div><div class="key-value"><span class="key">Current selection</span><strong>${safe(currentCandidate||"—")}</strong></div><div class="key-value"><span class="key">Historical selection</span><strong>${safe(historicalCandidate||"—")}</strong></div><div class="key-value"><span class="key">Selection reason</span><strong>${safe(selectionReasonLabel)}</strong></div><div class="key-value"><span class="key">Invalidation reason</span><strong>${safe(selectionReason||"—")}</strong></div><div class="key-value"><span class="key">Last ranking run ID</span><strong>${safe(c.ranking_run_id||"—")}</strong></div><div class="key-value"><span class="key">Last ranking timestamp</span><strong>${safe(dateText(c.ranking_timestamp))}</strong></div><div class="key-value"><span class="key">Historical data integrity</span><strong>${safe(c.historical_data_integrity||"UNKNOWN")}</strong></div><div class="key-value"><span class="key">Historical execution fidelity</span><strong>${safe(c.historical_execution_fidelity||"UNKNOWN")}</strong></div><div class="key-value"><span class="key">Current execution evidence</span><strong>${safe(c.current_execution_evidence||"CURRENT_ORDER_BOOK_REQUIRED")}</strong></div><div class="key-value"><span class="key">Next decision</span><strong>${safe(auto.next_decision||"—")}</strong></div><div class="key-value"><span class="key">Blocker</span><strong>${safe(selectionReason||auto.blocker||"—")}</strong></div></div></article><article class="panel"><div class="section-title"><h2>Risk envelope</h2><span class="badge warn">active settings</span></div>${riskMarkup}</article>`;
      const readiness=signal?String(signal.status||"READY"):"NO SIGNAL", detail=signal?`<div class="three-col"><div class="key-value"><span class="key">Signal readiness</span><strong>${safe(readiness)}</strong></div><div class="key-value"><span class="key">Market / outcome</span><strong>${safe(signal.market_id)} / ${safe(signal.outcome)}</strong></div><div class="key-value"><span class="key">Expected price</span><strong>${safe(signal.paper_expected_price)}</strong></div><div class="key-value"><span class="key">Generated</span><strong>${safe(dateText(signal.generated_at))}</strong></div><div class="key-value"><span class="key">Order result</span><strong>${safe(c.last_request_status||"NO ORDER")}</strong></div></div>`:empty("No latest signal","No persisted signal is available.");
      $("canary-summary").insertAdjacentHTML("beforeend",`<article class="panel"><div class="section-title"><h2>Latest signal</h2><span class="badge ${statusClass(readiness)}">${safe(readiness)}</span></div>${detail}<p class="page-note">Kill prevents new submissions; an in-flight request is recorded, in-flight not retracted, and never retried automatically.</p></article>`);
      $("canary-trades").innerHTML=arr(c.trades).length?`<table><thead><tr><th>Time</th><th>Candidate</th><th>Market</th><th>Side</th><th>Status</th><th>Price Δ</th></tr></thead><tbody>${arr(c.trades).map(t=>`<tr><td>${safe(dateText(t.timestamp))}</td><td>${safe(t.candidate_id)}</td><td>${safe(t.market_id)}</td><td>${safe(t.side)}</td><td><span class="badge ${statusClass(t.status)}">${safe(t.status)}</span></td><td>${safe(t.price_difference)}</td></tr>`).join("")}</tbody></table>`:empty("No canary execution evidence","No order has been submitted by the autonomous worker.");
    }
    function renderCanaryAutonomousState(data) {
      const c=data?.canary||{}, auto=data?.autonomous_canary||c.autonomous||{};
      const read=(name)=>auto[name]??c[name]??null;
      const selectionStatus=String(c.selection_status??auto.selection_status??"UNKNOWN").toUpperCase();
      const selectionValid=(c.selection_valid??auto.selection_valid)===true;
      const boundResearchCandidate=c.selected_candidate??auto.selected_candidate??null;
      const currentSelectionBinding=selectionValid&&selectionStatus==="CURRENT"&&boundResearchCandidate!=null&&String(boundResearchCandidate).trim()!=="";
      const researchCandidate=currentSelectionBinding?boundResearchCandidate:null;
      const researchRank=currentSelectionBinding?(c.winner_rank??auto.winner_rank??auto.rank??null):null;
      const actionableCandidate=read("selected_actionable_candidate");
      const actionableFound=read("actionable_candidates_found");
      const signalChecked=read("candidates_signal_checked");
      const noSignal=read("candidates_no_signal");
      const observed=actionableFound!=null||signalChecked!=null||noSignal!=null;
      const hasActionable=actionableCandidate!=null&&String(actionableCandidate).trim()!=="";
      const eligibleCount=c.eligible_count??auto.eligible_count,rankableCount=c.rankable_count??auto.rankable_count;
      const noEligible=eligibleCount!=null&&rankableCount!=null&&Number(eligibleCount)===0&&Number(rankableCount)===0;
      const noAction=!hasActionable&&observed&&(
        (actionableFound!=null&&Number(actionableFound)===0)||
        (actionableFound==null&&noSignal!=null&&Number(noSignal)>0)
      );
      const scanStatus=String(read("signal_scan_status")||"").toUpperCase();
      const status=noEligible?"NO_ELIGIBLE_CANDIDATES":scanStatus==="IN_PROGRESS"?"IN_PROGRESS":noAction?"NO ACTIONABLE SIGNAL":hasActionable?"ACTIONABLE SIGNAL":scanStatus==="UNKNOWN"?"DATA MISSING":"UNKNOWN";
      const currentCandidate=hasActionable?actionableCandidate:noAction?"NONE":null;
      const scanWindow=`${safe(read("next_signal_scan_start_rank"))}–${safe(read("next_signal_scan_end_rank"))}`;
      const existing=$("canary-actionable-opportunity");
      if(existing)existing.remove();
      $("canary-summary")?.insertAdjacentHTML("afterbegin",`<article id="canary-actionable-opportunity" class="panel"><div class="section-title"><h2>ACTIONABLE SIGNAL SCAN</h2><span class="badge ${statusClass(status)}">${safe(status)}</span></div><div class="three-col"><div class="key-value"><span class="key">Research winner · candidate</span><strong>${safe(researchCandidate)}</strong></div><div class="key-value"><span class="key">Research rank</span><strong>${safe(researchRank)}</strong></div><div class="key-value"><span class="key">Current actionable candidate</span><strong>${safe(currentCandidate)}</strong></div><div class="key-value"><span class="key">Candidates ranked</span><strong>${count(read("candidates_ranked"))}</strong></div><div class="key-value"><span class="key">Signal checked this tick</span><strong>${count(signalChecked)}</strong></div><div class="key-value"><span class="key">No signal</span><strong>${count(noSignal)}</strong></div><div class="key-value"><span class="key">Actionable</span><strong>${count(actionableFound)}</strong></div><div class="key-value"><span class="key">Chosen actionable rank</span><strong>${safe(read("selected_actionable_rank"))}</strong></div><div class="key-value"><span class="key">Chosen score</span><strong>${safe(read("selected_actionable_score"))}</strong></div></div>${noAction?`<p class="page-note"><strong>NO ACTIONABLE SIGNAL</strong> · checked ${count(signalChecked)} candidate(s) · next scan window ranks ${scanWindow}</p>`:""}<p class="page-note">Signal scan ranking run ${safe(read("signal_scan_ranking_run_id"))} · cursor ${safe(read("signal_scan_cursor"))}</p></article>`);
      const forwardEvidence=data?.forward_evidence||c.forward_evidence||{}, reasonCounts=data?.signal_scan_reason_counts||auto.signal_scan_reason_counts||{};
      const displayReason=(value)=>String(value||"").toUpperCase()==="CANDIDATE_FORWARD_MARKET_UNRESOLVED"?"UNRESOLVED_MARKET":value;
      const evidenceRows=[["Candidate-bound markets",arr(forwardEvidence.candidate_bound_markets).length],["Scheduled",arr(forwardEvidence.scheduled).length],["Fresh",arr(forwardEvidence.fresh).length],["Stale",arr(forwardEvidence.stale).length],["Missing",arr(forwardEvidence.missing).length],["Grade",forwardEvidence.grade],["Reason",displayReason(forwardEvidence.reason_display||forwardEvidence.reason_code)],["Newest source",dateText(forwardEvidence.newest_required_source_timestamp)],["Oldest source",dateText(forwardEvidence.oldest_required_source_timestamp)],["Newest observed",dateText(forwardEvidence.newest_required_observed_at)],["Oldest observed",dateText(forwardEvidence.oldest_required_observed_at)]];
      $("canary-summary")?.insertAdjacentHTML("beforeend",`<article id="canary-forward-evidence" class="panel"><div class="section-title"><h2>FORWARD EVIDENCE</h2><span class="badge ${statusClass(forwardEvidence.grade)}">${safe(forwardEvidence.grade||"UNKNOWN")}</span></div><div class="three-col">${evidenceRows.map(([label,value])=>`<div class="key-value"><span class="key">${safe(label)}</span><strong>${safe(value??"—")}</strong></div>`).join("")}</div><p class="page-note">Signal scan reason counts: ${safe(Object.entries(reasonCounts).map(([key,value])=>`${key}=${value}`).join(", ")||"—")}</p>${c.last_cycle_blocker?`<p class="page-note">Last-cycle blocker: ${safe(c.last_cycle_blocker)} (control state ${safe(c.control_state||"UNKNOWN")})</p>`:""}</article>`);
      const universeHash=read("signal_scan_candidate_universe_hash");
      const coverage=noEligible?0:universeHash==null||String(universeHash).trim()===""?0:Number(read("signal_scan_coverage_percentage")??0);
      $("canary-summary")?.insertAdjacentHTML("beforeend",`<p class="page-note signal-scan-coverage">Signal scan coverage: ${safe(Number.isFinite(coverage)?coverage:0)}% · checked ${safe(read("signal_scan_checked_this_cycle")??0)} · remaining ${safe(read("signal_scan_remaining_this_cycle")??0)} · cycle ${safe(read("signal_scan_cycle_id"))} · status ${safe(read("signal_scan_status")||"UNKNOWN")}</p>`);
    }
    const _renderCanaryResearchAndAction = renderCanary;
    renderCanary = (data) => { _renderCanaryResearchAndAction(data); renderCanaryAutonomousState(data); };
    function renderBtc(data) { const b=operator.btc||{},summary=b.catalog_summary||{},rows=arr(summary.latest_by_timeframe||summary.timeframes),fallback=arr(b.catalog),catalogRows=rows.length?rows:fallback; $("btc-summary").innerHTML=catalogRows.length?`<div class="three-col"><div class="key-value"><span class="key">Catalog timeframes</span><strong>${count(catalogRows.length)}</strong></div><div class="key-value"><span class="key">Rows observed</span><strong>${count(catalogRows.reduce((total,item)=>total+Number(item.row_count||0),0))}</strong></div><div class="key-value"><span class="key">Latest report</span><strong>${safe(dateText(b.latest_report?.created_at))}</strong></div></div>`:empty("BTC history not initialized","Run bootstrap-history --crypto, then btc-research."); $("btc-experiments").innerHTML=""; }
    async function loadCrypto(symbol,persist=true) { state.selected=symbol; state.expanded=true; if(persist)saveState(true); try { const response=await fetch(`/api/v2/crypto-research/${encodeURIComponent(symbol)}`,{cache:"no-store"}),data=await response.json(); $("crypto-detail").innerHTML=arr(data.items).length?`<details open><summary>Crypto detail · ${safe(symbol)}</summary><div class="three-col"><div class="key-value"><span class="key">Universe version</span><strong>${safe(data.universe_version)}</strong></div><div class="key-value"><span class="key">Strategies</span><strong>${count(arr(data.strategies).length)}</strong></div><div class="key-value"><span class="key">Families</span><strong>${count(arr(data.families).length)}</strong></div></div><pre>${safe(json({catalogs:data.items,reports:data.reports,validation:data.validation,coverage:data.coverage}))}</pre></details>`:empty("Crypto symbol unavailable","No catalog is persisted for this symbol."); } catch(e) { $("crypto-detail").innerHTML=empty("Crypto detail unavailable",e.message); } }
    function renderCrypto(data) { const rows=arr(data.items),symbols=arr(data.symbols),summary={universe_version:data.universe_version,symbols:data.symbol_count??symbols.length,assets:data.asset_count??arr(data.assets).length,catalogs:data.total,reports:arr(data.reports).length}; $("crypto-summary").innerHTML=`<div class="three-col">${[["Universe version",summary.universe_version],["Symbols",summary.symbols],["Assets",summary.assets],["Catalogs",summary.catalogs],["Reports",summary.reports],["Families",arr(data.families).length]].map(([label,value])=>`<div class="key-value"><span class="key">${safe(label)}</span><strong>${safe(value)}</strong></div>`).join("")}</div>`; $("crypto-table").innerHTML=rows.length?`<table><thead><tr><th>Symbol</th><th>Dataset</th><th>Version</th><th>Source</th><th>Coverage</th><th>Strategies</th><th>Experiments</th><th>Validation</th><th>Families</th></tr></thead><tbody>${rows.map(i=>`<tr><td><button class="link crypto-symbol-row" data-symbol="${encodeURIComponent(i.symbol||"")}">${safe(i.symbol)}</button></td><td>${safe(i.dataset_id)}</td><td>${safe(i.dataset_version)}</td><td>${safe(i.source_type)}</td><td>${safe(json(i.coverage))}</td><td>${safe(json(i.strategies))}</td><td>${safe(json(i.experiments))}</td><td>${safe(json(i.validation))}</td><td>${safe(json(i.families))}</td></tr>`).join("")}</tbody></table>`:empty("No crypto catalogs","No crypto catalog or report has been persisted."); pager("crypto",data); document.querySelectorAll(".crypto-symbol-row").forEach(b=>b.addEventListener("click",()=>loadCrypto(decodeURIComponent(b.dataset.symbol)))); }
    function renderOutcomeCards(data) { const cards=data.research_cards||{}, latest=data.hermes_latest_outcome||cards.newest_hermes_outcome||{}, latestLabel=latest.outcome_label||latest.status||"—"; $("research-cards").innerHTML=[["experiments_run","Experiments run"],["active_hypotheses","Active hypotheses"],["candidates_alive","Candidates alive"],["candidate_rejected","Candidate Rejected"],["research_rejected","Research Rejected"],["canary_eligible","Canary eligible"],["paper_forward","Paper forward"],["paper_promotable","Paper promotable"]].map(([k,l])=>`<article class="panel"><div class="metric">${count(cards[k])}</div><div class="metric-label">${l}</div></article>`).join("")+`<article class="panel"><div class="metric">${safe(latestLabel)}</div><div class="metric-label">Newest Hermes outcome · ${safe(latest.item_id||"none")}</div><div class="page-note">Dataset: ${safe(latest.dataset_id||"—")} / ${safe(latest.dataset_version||"—")}</div>${latest.human_reason?`<p class="page-note">${safe(latest.human_reason)}</p>`:""}</article>`; }
    const _renderOverview=renderOverview; renderOverview=(data)=>{_renderOverview(data);renderOutcomeCards(data);};
    const VIEW_ENDPOINT = {overview:"overview-summary",canary:"canary","binance-canary":"binance-canary",datasets:"datasets",activity:"activity",candidates:"candidates",polymarket:"polymarket",hermes:"hermes",crypto:"crypto-research",portfolio:"paper"};
    const VIEW_TARGET = {datasets:"datasets-table",activity:"activity-table",candidates:"candidates-table",polymarket:"pm-markets",hermes:"hermes-table",crypto:"crypto-table",portfolio:"portfolio-states", "binance-canary":"binance-records"};
    const VIEW_CADENCE = {overview:10000,canary:15000,"binance-canary":15000,datasets:30000,activity:15000,candidates:30000,polymarket:30000,hermes:30000,crypto:30000,portfolio:30000};
    let activeController = null, detailController = null, refreshGeneration = 0, nextRefreshAt = 0, slowRefreshTimer = null, startupPending = true;
    const REFRESH_TIMEOUT_MS = 8000;
    const lastGood = {overview:null,canary:null,"binance-canary":null,controls:null};
    let lastSuccessful = 0;
    function refreshError(error) {
      if(error?.name==="AbortError") return "request cancelled";
      if(error?.name==="TimeoutError") return "request timed out";
      return "request unavailable";
    }
    async function fetchWithTimeout(url, options={}) {
      const controller = new AbortController();
      const parent = options.signal;
      let timedOut = false;
      const abort = () => controller.abort();
      if(parent) {
        if(parent.aborted) controller.abort();
        else parent.addEventListener("abort", abort, {once:true});
      }
      const timeout = setTimeout(() => {timedOut=true;controller.abort();}, REFRESH_TIMEOUT_MS);
      try {
        const response = await fetch(url, {...options, signal:controller.signal});
        if(!response.ok) throw new Error(`${url} HTTP ${response.status}`);
        return await response.json();
      } catch(error) {
        if(timedOut) {
          const timeoutError = new Error("refresh request timed out");
          timeoutError.name = "TimeoutError";
          throw timeoutError;
        }
        throw error;
      } finally {
        clearTimeout(timeout);
        if(parent) parent.removeEventListener("abort", abort);
      }
    }
    // Shared status classifier covers readiness, degradation, and terminal grades.
    function refreshCadence(tab) { return VIEW_CADENCE[tab] || 30000; }
    function refreshNote(tab) { const view=$(`view-${tab}`); if(!view)return null; let note=view.querySelector(".refresh-note"); if(!note){ const title=view.querySelector(".section-title"); if(!title)return null; note=document.createElement("span"); note.className="refresh-note"; title.appendChild(note); } return note; }
    function refreshMessage(tab,message,slow=false) { const note=refreshNote(tab); if(note){note.textContent=message||"";note.classList.toggle("slow",slow);} }
    function clearRefreshing(tab) {
      const note=refreshNote(tab);
      if(note && note.textContent==="Refreshing…")refreshMessage(tab,"");
    }
    function shortId(value) { const text=String(value??""); return text.length>30?`${text.slice(0,13)}…${text.slice(-11)}`:text||"—"; }
    function copyButton(value) { const text=String(value??""); return text?`<span role="button" tabindex="0" class="copy" data-copy="${safe(text)}" title="Copy full value">copy</span>`:""; }
    function identity(primary,full,secondary="") { return `<span class="identity" title="${safe(full||primary)}"><span class="identity-main">${safe(primary||"—")}${copyButton(full||primary)}</span>${secondary?`<span class="identity-sub">${safe(secondary)}</span>`:""}</span>`; }
    function datasetPrimary(item) { const instrument=String(item.instrument||"").trim(),timeframe=String(item.timeframe||"").trim(); return String(item.market_type||"").toLowerCase()==="crypto_spot"&&instrument?`${instrument}${timeframe?` · ${timeframe}`:""}`:shortId(item.dataset_id); }
    function activityKey(item) { const d=item.details||{}; return [String(d.selected_symbol||d.symbol||d.instrument||d.dataset_id||item.market_id||""),String(d.timeframe||"")].join("|"); }
    function compactActivityRows(rows) { const compact=[]; for(const item of arr(rows)){ const previous=compact[compact.length-1]; if(previous&&item.kind==="bootstrap"&&previous.kind==="bootstrap"&&activityKey(previous)===activityKey(item)){previous.count++;previous.events.push(item);continue;} compact.push({...item,count:1,events:[item]}); } return compact; }
    function activityMarkup(rows) { return compactActivityRows(rows).map(item=>{const suffix=item.count>1?` ×${item.count}`:"";const detail=item.events.length>1?` <details><summary>${item.events.length} adjacent bootstrap events</summary><pre>${safe(json(item.events))}</pre></details>`:(item.details&&Object.keys(item.details).length?` <details><summary>details</summary><pre>${safe(json(item.details))}</pre></details>`:"");return `<div class="activity-compact"><div class="timeline-item"><span class="timeline-time">${safe(dateText(item.timestamp))}</span><span class="timeline-kind">${safe(item.kind)}${suffix}</span><span>${safe(item.message)}${detail}</span></div></div>`; }).join("")||empty("No research activity","Durable activity will appear after workers run."); }
    function ensureActivityKind() { const status=$("activity-status"); if(!status||$("activity-kind"))return; const select=document.createElement("select"); select.id="activity-kind"; select.className="facet"; select.dataset.param="kind"; select.setAttribute("aria-label","Filter activity type"); select.innerHTML='<option value="">All activity types</option>'+["bootstrap","dataset","research","lifecycle","collection","collection_error","report","operator"].map(v=>`<option value="${v}">${v.replace("_"," ")}</option>`).join(""); status.parentNode.insertBefore(select,status); }
    fetchV2 = async function(name,signal) { const q=new URLSearchParams(); if(!["overview-summary","canary"].includes(name)){q.set("page",String(state.page));q.set("page_size",String(state.page_size));q.set("direction",state.direction);if(state.filter)q.set("filter",state.filter);if(state.sort)q.set("sort",state.sort);} const controls={datasets:[["datasets-source","source_type"],["datasets-market","market"],["datasets-timeframe","timeframe"],["datasets-quality","quality"]],activity:[["activity-status","status"],["activity-kind","kind"]],candidates:[["candidates-stage","stage"]],polymarket:[["polymarket-category","category"],["polymarket-settlement","settlement"],["polymarket-quality","quality"]],hermes:[["hermes-status","status"]],paper:[["paper-status","status"]]}; for(const [id,key] of (controls[state.tab]||[])){const el=$(id);if(el&&el.value)q.set(key,el.value);} if(state.tab==="crypto"&&$("crypto-symbol")?.value.trim())q.set("symbol",$("crypto-symbol").value.trim()); const url=`/api/v2/${name}${q.toString()?`?${q}`:""}`,response=await fetch(url,{cache:"no-store",signal}); if(!response.ok)throw new Error(`${name} HTTP ${response.status}`); return response.json(); };
    const _fetchV2 = fetchV2;
    async function fetchV2Bounded(name, parentSignal) {
      const controller = new AbortController();
      let timedOut = false;
      const abort = () => controller.abort();
      if(parentSignal) {
        if(parentSignal.aborted) controller.abort();
        else parentSignal.addEventListener("abort", abort, {once:true});
      }
      const timeout = setTimeout(() => {timedOut=true;controller.abort();}, REFRESH_TIMEOUT_MS);
      try {
        return await _fetchV2(name, controller.signal);
      } catch(error) {
        if(timedOut) {
          const timeoutError = new Error("refresh request timed out");
          timeoutError.name = "TimeoutError";
          throw timeoutError;
        }
        throw error;
      } finally {
        clearTimeout(timeout);
        if(parentSignal) parentSignal.removeEventListener("abort", abort);
      }
    }
    function researchFeedValue(value) { if(value==null||value==="")return "UNKNOWN"; return typeof value==="object"?json(value):String(value); }
    function researchFeedTimestamp(value) { if(value==null||value==="")return "UNKNOWN"; const formatted=dateText(value); return formatted==="—"?String(value):formatted; }
    function researchFeedField(key,label,value,timestamp=false) { return `<div class="key-value" data-field="${safe(key)}"><span class="key">${safe(label)}</span><strong>${safe(timestamp?researchFeedTimestamp(value):researchFeedValue(value))}</strong></div>`; }
    function renderResearchProgress(data) {
      const progress=data?.research_progress&&typeof data.research_progress==="object"?data.research_progress:{};
      const campaign=data?.campaign_progress&&typeof data.campaign_progress==="object"?data.campaign_progress:{};
      const candidate=progress.candidate&&typeof progress.candidate==="object"?progress.candidate:{};
      const job=progress.job&&typeof progress.job==="object"?progress.job:{};
      const samples=candidate.samples&&typeof candidate.samples==="object"?candidate.samples:{};
      const trades=candidate.trades&&typeof candidate.trades==="object"?candidate.trades:{};
      const pair=(value,required)=>value==null&&required==null?"UNKNOWN":`${value==null?"UNKNOWN":value} / ${required==null?"UNKNOWN":required}`;
      const dataset=progress.dataset_id||"UNKNOWN",version=progress.dataset_version||"UNKNOWN";
      const blocker=progress.blocker||"—",status=String(progress.job_status||job.status||"NOT_INITIALIZED").toUpperCase();
      const candidateStatus=progress.status||"—";
      const budget=campaign.budget&&typeof campaign.budget==="object"?campaign.budget:{};
      $("research-progress-content").innerHTML=[
        researchFeedField("dataset","Dataset",dataset),
        researchFeedField("dataset_version","Dataset version",version),
        researchFeedField("job_status","Automatic research job",status),
        researchFeedField("last_completion_at","Last completion",job.last_completion_at||progress.last_completion_at,true),
        researchFeedField("next_run_at","Next run",job.next_run_at||progress.next_run_at,true),
        researchFeedField("candidate_stage","Candidate stage",candidate.stage||progress.candidate_stage),
        researchFeedField("samples","Samples available / required",pair(samples.available??progress.samples_available,samples.required??progress.samples_required)),
        researchFeedField("trades","Trades available / required",pair(trades.available??progress.trades_available,trades.required??progress.trades_required)),
        researchFeedField("forward_observations","Forward observations",candidate.forward_observations??progress.forward_observations),
        researchFeedField("blocker","Blocker",blocker),
        researchFeedField("status","Validation status",candidateStatus),
        researchFeedField("campaign_id","Synthetic campaign",campaign.campaign_id),
        researchFeedField("campaign_status","Campaign status",campaign.status),
        researchFeedField("campaign_budget","Campaign budget",`${budget.used??campaign.budget_used??0} / ${budget.limit??campaign.budget_limit??0} (${budget.remaining??campaign.budget_remaining??0} remaining)`),
        researchFeedField("campaign_completed","Campaign trials completed",campaign.completed??campaign.completed_trials??0),
        researchFeedField("campaign_remaining","Campaign trials remaining",campaign.remaining??campaign.remaining_trials??0),
        researchFeedField("campaign_last_result","Campaign last result",campaign.last_result),
        researchFeedField("campaign_qualified","Campaign qualified (synthetic)",campaign.qualified??campaign.qualified_candidate_ids),
        researchFeedField("campaign_next_real_job","Next real job",campaign.next_real_job),
        researchFeedField("campaign_waiting_prerequisite","Waiting prerequisite",campaign.waiting_prerequisite)
      ].join("");
    }
    function renderResearchFeed(data) {
      const feed=data?.research_feed&&typeof data.research_feed==="object"?data.research_feed:{},external=feed.external_hermes||{},internal=feed.internal_queue||{},proposals=feed.proposals||{},candidates=feed.candidates||{},budgets=feed.budgets||{};
      const externalStatus=String(external.status??"UNKNOWN").toUpperCase(),internalStatus=String(internal.status??"UNKNOWN").toUpperCase();
      const fields=(source,specs)=>specs.map(([key,label,timestamp])=>researchFeedField(key,label,source[key],timestamp)).join("");
      const externalEvidence=researchFeedValue(external.evidence);
      $("research-feed-content").innerHTML=`<div class="two-col">
        <section class="panel"><div class="section-title"><h3>External Hermes feed</h3><span class="badge ${statusClass(externalStatus)}">External status ${safe(externalStatus)}</span></div>${researchFeedField("job_id","Job ID",external.job_id)}${researchFeedField("status","External status",externalStatus)}${researchFeedField("evidence","Evidence",externalEvidence)}</section>
        <section class="panel"><div class="section-title"><h3>Internal research queue processing</h3><span class="badge ${statusClass(internalStatus)}">Internal queue ${safe(internalStatus)}</span></div>${researchFeedField("status","Status",internal.status)}${researchFeedField("trigger","Trigger",internal.trigger)}${researchFeedField("last_cycle_at","Last cycle at",internal.last_cycle_at,true)}</section>
      </div>
      <div class="two-col">
        <section class="panel"><h3>Proposals</h3>${fields(proposals,[["latest_submitted_at","Latest submitted at",true],["latest_accepted_at","Latest accepted at",true],["submitted_24h","Submitted 24h"],["accepted_24h","Accepted 24h"],["rejected_24h","Rejected 24h"],["failed_24h","Failed 24h"],["pending","Pending"],["processing","Processing"],["completed","Completed"],["rejected","Rejected"]])}</section>
        <section class="panel"><h3>Candidates</h3>${fields(candidates,[["latest_created_at","Latest created at",true],["created_24h","Created 24h"],["mutations_24h","Mutations 24h"],["total","Total"],["new","New"],["eligible","Eligible"],["rejected","Rejected"]])}</section>
      </div>
      <div class="two-col">
        <section class="panel"><h3>Budgets</h3>${fields(budgets,[["total_limit","Total limit"],["total_used","Total used"],["total_remaining","Total remaining"],["families","Families"]])}</section>
        <section class="panel"><h3>Candidate admission</h3>${researchFeedField("no_new_candidates_reason","No new candidates reason",feed.no_new_candidates_reason)}</section>
      </div>`;
    }
    function renderMarketScopeFunnel(data) {
      const funnel=data.market_scope_funnel||{}, stages=funnel.stages||{}, counts=funnel.stage_counts||{};
      const names=["historically_qualified","valid_frozen_scope","matching_current_markets","fresh_complete_inputs","strategy_evaluated","ready_signal","execution_feasible","submitted","filled"];
      const max=Math.max(1,...names.map(name=>Number(stages[name]?.count??counts[name]??0)));
      const rows=names.map(name=>{
        const stage=stages[name]||{}, value=Number(stage.count??counts[name]??0)||0, blockers=stage.blocker_counts||stage.blockers||{}, ts=stage.timestamps||{};
        const blockerText=Object.entries(blockers).map(([key,count])=>`${safe(key)}=${safe(count)}`).join(", ");
        const when=ts.latest||stage.latest_at||"";
        return `<div class="funnel-row" title="${safe(blockerText)}"><span>${safe(name.replaceAll("_"," "))}</span><span class="funnel-track"><span class="funnel-bar" style="width:${Math.min(100,Math.round(value/max*100))}%"></span></span><strong>${count(value)}</strong></div><p class="page-note">${blockerText?`Blockers: ${blockerText} · `:""}${when?`latest ${safe(dateText(when))}`:"No persisted timestamp"}</p>`;
      }).join("");
      $("market-scope-funnel").innerHTML=rows||empty("No persisted market-scope handoff","Resolution workers have not persisted a bounded funnel yet.");
    }
    renderOverview = (data) => { renderComponents(data); renderOutcomeCards(data); const c=data.coverage||{},h=data.collector_health||{}; $("coverage").innerHTML=`<div class="three-col"><div class="key-value"><span class="key">Historical datasets</span><strong>${count(c.historical_count)}</strong></div><div class="key-value"><span class="key">Historical rows</span><strong>${count(c.historical_rows)}</strong></div><div class="key-value"><span class="key">Forward datasets</span><strong>${count(c.forward_count)}</strong></div><div class="key-value"><span class="key">Forward rows</span><strong>${count(c.forward_rows)}</strong></div><div class="key-value"><span class="key">Logical observations</span><strong>${count((c.logical_rows||{}).bars)}</strong></div><div class="key-value"><span class="key">Collector errors</span><strong>${count(h.collection_errors)}</strong></div><div class="key-value"><span class="key">Last cycle duration</span><strong>${h.last_cycle_duration_seconds==null?"—":`${Number(h.last_cycle_duration_seconds).toFixed(1)}s`}</strong></div><div class="key-value"><span class="key">Effective cadence</span><strong>${h.effective_collection_cadence_seconds==null?"—":`${Number(h.effective_collection_cadence_seconds).toFixed(1)}s`}</strong></div><div class="key-value"><span class="key">Markets A / S / F</span><strong>${count(h.last_cycle_markets_attempted)} / ${count(h.last_cycle_markets_successful)} / ${count(h.last_cycle_markets_failed)}</strong></div></div><p class="page-note">Configured interval ${safe(h.configured_interval_seconds??"—")}s · stale threshold ${safe(h.stale_after_seconds??"—")}s · last successful cycle ${safe(dateText(h.last_successful_cycle))}</p>`; $("overview-activity").innerHTML=activityMarkup(arr(data.latest_activity||data.activity)); $("overview-candidates").innerHTML=empty("Candidate list is lazy","Open Candidates to load the bounded lifecycle page."); $("raw-overview").textContent=json({counts:data.counts,collector_health:h,latest_outcome:data.hermes_latest_outcome}); };
    const _renderOverviewScheduling = renderOverview;
    renderOverview = (data) => {
      _renderOverviewScheduling(data);
      renderResearchProgress(data);
      renderResearchFeed(data);
      renderMarketScopeFunnel(data);
      $("overview-readiness-snapshot").innerHTML=readinessSnapshotMarkup(data);
      if(Object.prototype.hasOwnProperty.call(data,"operator_controls")||!operatorControlsRendered)renderOperatorControls(data);
      const h = data.collector_health || {};
      const components = Object.fromEntries(arr(data.components).map(item => [item.name, item]));
      const collector = components["POLYMARKET COLLECTOR"]?.detail || {};
      const paper = components["PAPER ENGINE"]?.detail || {};
      const research = components["RESEARCH ENGINE"]?.detail || {};
      $("coverage").insertAdjacentHTML("beforeend", `<p class="page-note">Next collection ${safe(dateText(h.next_scheduled_collection_at || collector.next_scheduled_collection_at))} · collector heartbeat ${safe(dateText(h.worker_heartbeat_at || collector.worker_heartbeat_at))} · PAPER_FORWARD pass ${safe(paper.status || "—")} · research pass ${safe(research.status || "—")}</p>`);
      const forwardEvidence=data.forward_evidence||{}, reasonCounts=data.signal_scan_reason_counts||{};
      $("coverage").insertAdjacentHTML("beforeend",`<article class="panel"><div class="section-title"><h3>Candidate-bound forward evidence</h3><span class="badge ${statusClass(forwardEvidence.grade)}">${safe(forwardEvidence.grade||"UNKNOWN")}</span></div><div class="three-col"><div class="key-value"><span class="key">Required / scheduled</span><strong>${count(arr(forwardEvidence.candidate_bound_markets).length)} / ${count(arr(forwardEvidence.scheduled).length)}</strong></div><div class="key-value"><span class="key">Fresh / stale / missing</span><strong>${count(arr(forwardEvidence.fresh).length)} / ${count(arr(forwardEvidence.stale).length)} / ${count(arr(forwardEvidence.missing).length)}</strong></div><div class="key-value"><span class="key">Reason</span><strong>${safe(forwardEvidence.reason_display||forwardEvidence.reason_code||"—")}</strong></div><div class="key-value"><span class="key">Newest source</span><strong>${safe(dateText(forwardEvidence.newest_required_source_timestamp))}</strong></div><div class="key-value"><span class="key">Oldest source</span><strong>${safe(dateText(forwardEvidence.oldest_required_source_timestamp))}</strong></div><div class="key-value"><span class="key">Newest observed</span><strong>${safe(dateText(forwardEvidence.newest_required_observed_at))}</strong></div></div><p class="page-note">Signal scan reason counts: ${safe(Object.entries(reasonCounts).map(([key,value])=>`${key}=${value}`).join(", ")||"—")}</p></article>`);
      const latestCandidates = arr(data.latest_candidates || data.candidates);
      $("overview-candidates").innerHTML = latestCandidates.length
        ? tableCandidates(latestCandidates, false)
        : empty("No candidates yet", "No persisted lifecycle candidates are available.");
      const funnel = data.lifecycle_funnel || {};
      const max = Math.max(1, ...Object.values(funnel).map(Number));
    renderPolymarket = (data) => { const items=arr(data.items),categories=[...new Set(items.map(i=>i.category).filter(Boolean))].sort(),cat=$("polymarket-category"),old=cat.value||params.get("category")||""; cat.innerHTML=`<option value="">All categories</option>${categories.map(c=>`<option value="${safe(c)}">${safe(c)}</option>`).join("")}`;if(old&&!categories.includes(old))cat.insertAdjacentHTML("beforeend",`<option value="${safe(old)}">${safe(old)}</option>`);cat.value=old;const quality=items.map(i=>i.quality_label||i.quality).find(Boolean)||"—";$("pm-summary").innerHTML=`<div class="three-col"><div class="key-value"><span class="key">Markets</span><strong>${count(data.total)}</strong></div><div class="key-value"><span class="key">Evidence quality</span><strong>${safe(quality)}</strong></div><div class="key-value"><span class="key">Execution</span><strong>SIMULATED ONLY</strong></div></div>`;$("pm-markets").innerHTML=items.length?`<table><thead><tr><th>Priority</th><th>Market</th><th>Question</th><th>Category</th><th>Quality</th><th>Source</th><th>Observed</th><th>Age</th><th>State / reason</th><th>Candidate references</th></tr></thead><tbody>${items.map(i=>{const priority=i.candidate_bound_priority||i.candidate_bound,reason=i.reason_display||i.reason_code;return `<tr><td>${priority?`<span class="badge warn">REQUIRED</span>`:"—"}</td><td>${identity(shortId(i.market_id),i.market_id)}</td><td title="${safe(i.question||"")}">${safe(i.question||"—")}</td><td>${safe(i.category)}</td><td><strong>${safe(i.quality_label||i.quality||"UNKNOWN")}</strong><span class="quality-context">${safe(i.quality_context||"")}</span></td><td>${safe(dateText(i.source_timestamp))}</td><td>${safe(dateText(i.observed_at))}</td><td>${i.freshness_age_seconds==null?"—":safe(Number(i.freshness_age_seconds).toFixed(1)+"s")}</td><td>${safe(i.collection_state||"—")}${reason?` · ${safe(reason)}`:""}</td><td>${safe(arr(i.candidate_references).join(", ")||"—")}</td></tr>`}).join("")}</tbody></table>`:empty("No Polymarket observations","No persisted market page matches the current filters.");pager("polymarket",data);bindTable(); };
    };
    renderDatasets = (data) => { $("dataset-total").textContent=`${count(data.total)} datasets`; const rows=arr(data.items); $("datasets-table").innerHTML=rows.length?`<table><thead><tr>${[["dataset_id","Dataset"],["source_type","Source"],["market_type","Market"],["instrument","Instrument"],["timeframe","Timeframe"],["quality","Quality"],["row_count","Rows"],["updated_at","Updated"]].map(([k,l])=>`<th>${sortButton(k,l)}</th>`).join("")}</tr></thead><tbody>${rows.map(i=>`<tr><td><button class="link dataset" data-id="${encodeURIComponent(i.dataset_id||"")}">${identity(datasetPrimary(i),i.dataset_id,i.dataset_id===datasetPrimary(i)?"":`full ${shortId(i.dataset_id)}`)}</button></td><td>${safe(i.source_type)}</td><td>${safe(i.market_type)}</td><td>${safe(i.instrument)}</td><td>${safe(i.timeframe)}</td><td>${safe(i.quality)}</td><td>${count(i.row_count)}</td><td>${safe(dateText(i.updated_at))}</td></tr>`).join("")}</tbody></table>`:empty("No datasets","No catalog records match the current filters."); pager("datasets",data); bindTable(); };
    renderActivity = (data) => { ensureActivityKind(); $("activity-total").textContent=`${count(data.total)} events`; $("activity-table").innerHTML=arr(data.items).length?`<div class="timeline">${activityMarkup(data.items)}</div>`:empty("No research activity","Durable activity will appear after workers run."); pager("activity",data); bindTable(); };
    renderCrypto = (data) => { const rows=arr(data.items),u=data.bootstrap_universe||{}; $("crypto-summary").innerHTML=`<div class="three-col"><div class="key-value"><span class="key">Universe version</span><strong title="${safe(data.universe_version)}">${safe(shortId(data.universe_version))}${copyButton(data.universe_version)}</strong></div><div class="key-value"><span class="key">Selected universe</span><strong>${count(u.selected_count)}</strong></div><div class="key-value"><span class="key">Bootstrap progress</span><strong>${u.progress==null?"—":(Number(u.progress)*100).toFixed(1)+"%"}</strong></div><div class="key-value"><span class="key">Bootstrap datasets</span><strong>${count(u.dataset_count)}</strong></div><div class="key-value"><span class="key">Bootstrap reports</span><strong>${count(data.bootstrap_report_count??arr(data.bootstrap_reports).length)}</strong></div><div class="key-value"><span class="key">Strategy reports</span><strong>${count(arr(data.strategy_reports||data.reports).length)}</strong></div></div>`; $("crypto-table").innerHTML=rows.length?`<table><thead><tr><th>Symbol</th><th>Dataset</th><th>Source</th><th>Rows</th><th>Quality</th><th>Updated</th></tr></thead><tbody>${rows.map(i=>`<tr><td>${safe(i.symbol||arr(i.symbols)[0])}</td><td>${identity(shortId(i.dataset_id),i.dataset_id,shortId(i.dataset_version))}</td><td>${safe(i.source_type)}</td><td>${count(i.row_count)}</td><td>${safe(i.quality)}</td><td>${safe(dateText(i.updated_at))}</td></tr>`).join("")}</tbody></table>`:empty("No crypto catalogs","Crypto data is separate from strategy research; run the bounded bootstrap or select another symbol."); if(arr(data.bootstrap_progress).length){$("crypto-detail").innerHTML=`<article class="panel"><div class="section-title"><h2>Bootstrap cursors</h2><span class="muted">${count(u.completed_datasets)} complete / ${count(u.dataset_count)} datasets</span></div><div class="scroll"><table><thead><tr><th>Selected symbol</th><th>Timeframe</th><th>Status</th><th>Progress</th><th>Records</th><th>Errors</th></tr></thead><tbody>${arr(data.bootstrap_progress).map(i=>`<tr><td>${safe(i.selected_symbol||i.symbol)}</td><td>${safe(i.timeframe)}</td><td>${safe(i.status)}</td><td>${i.progress==null?"—":(Number(i.progress)*100).toFixed(1)+"%"}</td><td>${count(i.records)}</td><td>${count(i.error_count)}</td></tr>`).join("")}</tbody></table></div></article>`;} else $("crypto-detail").innerHTML=""; pager("crypto",data); bindTable(); };
    renderPolymarket = (data) => { const items=arr(data.items),categories=[...new Set(items.map(i=>i.category).filter(Boolean))].sort(),cat=$("polymarket-category"),old=cat.value||params.get("category")||""; cat.innerHTML=`<option value="">All categories</option>${categories.map(c=>`<option value="${safe(c)}">${safe(c)}</option>`).join("")}`;if(old&&!categories.includes(old))cat.insertAdjacentHTML("beforeend",`<option value="${safe(old)}">${safe(old)}</option>`);cat.value=old;const quality=items.map(i=>i.quality_label||i.quality).find(Boolean)||"—";$("pm-summary").innerHTML=`<div class="three-col"><div class="key-value"><span class="key">Markets</span><strong>${count(data.total)}</strong></div><div class="key-value"><span class="key">Evidence quality</span><strong>${safe(quality)}</strong></div><div class="key-value"><span class="key">Execution</span><strong>SIMULATED ONLY</strong></div></div>`;$("pm-markets").innerHTML=items.length?`<table><thead><tr><th>Market</th><th>Question</th><th>Category</th><th>Quality</th><th>Settlement</th><th>Observed</th></tr></thead><tbody>${items.map(i=>`<tr><td>${identity(shortId(i.market_id),i.market_id)}</td><td title="${safe(i.question||"")}">${safe(i.question||"—")}</td><td>${safe(i.category)}</td><td><strong>${safe(i.quality_label||i.quality||"UNKNOWN")}</strong><span class="quality-context">${safe(i.quality_context||"")}</span></td><td>${safe(i.settlement)}</td><td>${safe(dateText(i.observed_at))}</td></tr>`).join("")}</tbody></table>`:empty("No Polymarket observations","No persisted market page matches the current filters.");pager("polymarket",data);bindTable(); };
    function detailSection(title,value) { if(value==null||value===""||(Array.isArray(value)&&!value.length))return ""; return `<section class="detail-section"><h3>${safe(title)}</h3>${typeof value==="string"||typeof value==="number"?`<div>${safe(value)}</div>`:`<pre>${safe(json(value))}</pre>`}</section>`; }
    loadHermes = async function(id,persist=true) { state.selected=id;state.expanded=true;if(persist)saveState(true);if(detailController)detailController.abort();detailController=new AbortController();try{const response=await fetch(`/api/v2/hermes/${encodeURIComponent(id)}`,{cache:"no-store",signal:detailController.signal});if(!response.ok)throw new Error(`Hermes HTTP ${response.status}`);const d=await response.json(),item=arr(d.items)[0]||d.item||{};$("hermes-detail").innerHTML=d.available?`<article class="panel"><div class="section-title"><h2>Proposal detail</h2><span class="badge ${statusClass(d.status||item.status)}">${safe(d.status||item.status||"UNKNOWN")}</span></div><div class="detail-grid">${detailSection("Statement",d.statement)}<div class="detail-section"><h3>Exact dataset and family</h3><div class="key-value"><span class="key">Dataset ID</span><strong>${identity(shortId(d.dataset_id||item.dataset_id),d.dataset_id||item.dataset_id)}</strong></div><div class="key-value"><span class="key">Dataset version</span><strong>${identity(shortId(d.dataset_version||item.dataset_version),d.dataset_version||item.dataset_version)}</strong></div><div class="key-value"><span class="key">Family</span><strong>${safe(d.family||item.family)}</strong></div></div>${detailSection("Parameters / experiment plan",d.plan)}${detailSection("Submission validation and tests",d.tests)}${detailSection("Queue lifecycle",d.lifecycle_events)}${detailSection("Terminal result",d.final_result)}${d.rejection?detailSection("Rejection code and reason",d.rejection):""}</div><details><summary>raw proposal evidence</summary><pre>${safe(json(d))}</pre></details></article>`:empty("Hermes item unavailable",d.error||"The queue item was not found.");}catch(error){if(error.name!=="AbortError")$("hermes-detail").innerHTML=empty("Hermes detail unavailable",error.message);} };
    renderPaper = (data) => { const s=data.candidate_portfolio_summary||{},portfolios=arr(data.candidate_portfolios),t=data.paper_telemetry||{}; $("portfolio-summary").innerHTML=`<div class="card-grid"><div class="panel"><div class="metric">${portfolios.length?Number(s.total_equity||0).toFixed(2):"—"}</div><div class="metric-label">candidate paper equity</div></div><div class="panel"><div class="metric">${portfolios.length?Number(s.total_pnl||0).toFixed(2):"—"}</div><div class="metric-label">candidate paper P/L</div></div><div class="panel"><div class="metric">${count(portfolios.length)}</div><div class="metric-label">candidate portfolios</div></div><div class="panel"><div class="metric">${count(t.observation_records)}</div><div class="metric-label">paper telemetry observations</div></div><div class="panel"><div class="metric">${count(t.execution_events)}</div><div class="metric-label">paper execution events</div></div><div class="panel"><div class="metric">${count(t.resolved_bets)}</div><div class="metric-label">paper ledger bets</div></div></div><p class="page-note">Telemetry is persisted observation/execution history. Candidate portfolios are lifecycle-linked PAPER_FORWARD/PAPER_PROMOTABLE states only.</p>`; $("portfolio-states").innerHTML=arr(data.items).length?`<table><thead><tr><th>Record</th><th>Experiment</th><th>Market</th><th>Status</th><th>Timestamp</th></tr></thead><tbody>${arr(data.items).map(i=>`<tr><td>${safe(i.record_type)} · ${identity(shortId(i.record_id),i.record_id)}</td><td>${safe(i.experiment_id)}</td><td>${safe(i.market_id)}</td><td>${safe(i.status||i.resolution||i.outcome)}</td><td>${safe(dateText(i.timestamp))}</td></tr>`).join("")}</tbody></table>`:empty("No paper telemetry","No paper observations, execution events, or ledger records are persisted.");pager("paper",data);bindTable(); };
    loadPage = async function(tab,force=false) {
      if(tab!==state.tab||!VIEW_ENDPOINT[tab]||document.hidden||loadInFlight||(!force&&Date.now()<nextRefreshAt))return;
      const generation=++refreshGeneration,controller=new AbortController();
      activeController=controller; loadInFlight=true; refreshMessage(tab,"");
      slowRefreshTimer=setTimeout(()=>{if(generation===refreshGeneration)refreshMessage(tab,"Refreshing…",true);},2000);
      const renderPersisted=async(kind,url,render,target)=>{
        try {
          const data=await fetchWithTimeout(url,{cache:"no-store",signal:controller.signal});
          if(generation!==refreshGeneration)return;
          render(data);
          lastGood[kind]=data;
          lastSuccessful=Date.now();
          refreshMessage(target,`Updated · ${new Date().toLocaleTimeString()}`);
        } catch(error) {
          if(generation!==refreshGeneration||error?.name==="AbortError")return;
          if(lastGood[kind]) {
            render(lastGood[kind]);
            refreshMessage(target,`Refresh failed (${refreshError(error)}) · showing last successful content`,true);
          } else {
            refreshMessage(target,`Refresh failed (${refreshError(error)}) · no cached dashboard snapshot available`,true);
          }
        }
      };
      try {
        if(tab==="overview") {
          await Promise.all([
            renderPersisted("overview","/api/v2/overview-summary",renderOverview,"overview"),
            (async()=>{
              try {
                const controls=await fetchWithTimeout("/api/operator",{cache:"no-store",signal:controller.signal});
                if(generation!==refreshGeneration)return;
                renderOperatorControls({operator_controls:controls.operator_controls||controls});
                lastGood.controls=controls;
                operator=controls;
              } catch(error) {
                if(generation!==refreshGeneration||error?.name==="AbortError")return;
                if(lastGood.controls) {
                  operator=lastGood.controls;
                  renderOperatorControls({operator_controls:operator.operator_controls||operator});
                  refreshMessage(tab,`Refresh failed (${refreshError(error)}) · showing last successful content`,true);
                } else {
                  refreshMessage(tab,`Refresh failed (${refreshError(error)}) · no cached dashboard snapshot available`,true);
                }
              }
            })()
          ]);
        } else if(tab==="canary") {
          await renderPersisted("canary","/api/v2/canary",renderCanary,"canary");
        } else {
          const data=await fetchV2Bounded(VIEW_ENDPOINT[tab],controller.signal);
          if(generation!==refreshGeneration)return;
          lastGood[tab]=data; lastSuccessful=Date.now(); current=data;
          ({datasets:renderDatasets,activity:renderActivity,candidates:renderCandidates,polymarket:renderPolymarket,hermes:renderHermes,crypto:renderCrypto,portfolio:renderPaper,"binance-canary":renderBinanceCanary}[tab])(data);
          refreshMessage(tab,`Updated · ${new Date().toLocaleTimeString()}`);
        }
      } catch(error) {
        if(generation===refreshGeneration&&error?.name!=="AbortError") {
          const cached=lastGood[tab];
          if(cached) {
            ({overview:renderOverview,canary:renderCanary,datasets:renderDatasets,activity:renderActivity,candidates:renderCandidates,polymarket:renderPolymarket,hermes:renderHermes,crypto:renderCrypto,portfolio:renderPaper,"binance-canary":renderBinanceCanary}[tab])(cached);
            refreshMessage(tab,`Refresh failed (${refreshError(error)}) · showing last successful content`,true);
          } else {
            refreshMessage(tab,`Refresh failed (${refreshError(error)}) · no cached dashboard snapshot available`,true);
          }
        }
      } finally {
        if(activeController===controller) {
          clearRefreshing(tab);
          clearTimeout(slowRefreshTimer);
          slowRefreshTimer=null;
          activeController=null;
          loadInFlight=false;
          if(generation===refreshGeneration)nextRefreshAt=Date.now()+refreshCadence(tab);
        }
      }
    };
    activate = function(tab,push=true) {
      if(!VIEW_ENDPOINT[tab])tab="overview";
      if(tab!==state.tab){state.selected="";state.expanded=false;state.filter="";state.sort="";state.direction="desc";state.page=1;}
      state.tab=tab;
      document.querySelectorAll(".tab").forEach(b=>b.classList.toggle("active",b.dataset.view===tab));
      document.querySelectorAll(".view").forEach(v=>v.classList.toggle("active",v.id===`view-${tab}`));
      if($("candidate-detail"))$("candidate-detail").style.display=tab==="candidates"?"block":"none";
      if($("detail"))$("detail").style.display=tab==="overview"?"block":"none";
      if(activeController)activeController.abort();
      if(detailController)detailController.abort();
      refreshGeneration++;
      nextRefreshAt=0;
      saveState(push);
      const schedule=()=>{if(state.tab!==tab)return;if(loadInFlight){setTimeout(schedule,25);return;}loadPage(tab,true);};
      setTimeout(schedule,0);
    };
    load = async function() {
      if(startupPending){startupPending=false;return;}
      if(document.hidden||loadInFlight)return;
      return loadPage(state.tab,false);
    };
    document.addEventListener("click",async event=>{const button=event.target.closest?.(".binance-action");if(!button)return;const action=button.dataset.binanceAction||"",payload={};if(!binanceTestnetMode&&(action==="ENABLE"||action==="RESUME"))payload.confirmation=$("binance-confirm")?.value||"";if(action==="ORDER_VALIDATION_TEST"&&!binanceTestnetMode){payload.symbol=$("binance-order-symbol")?.value||"";payload.price=$("binance-order-price")?.value||"";payload.quantity=$("binance-order-quantity")?.value||"";}await binanceControlPost(action,payload);});
    document.addEventListener("click",async event=>{
      const button=event.target.closest?.(".control-action");
      if(!button||button.disabled)return;
      button.disabled=true;
      try {
        const action=button.dataset.controlAction||"",target=button.dataset.controlTarget||"",expected=button.dataset.controlConfirm||"",encodedPayload=button.dataset.controlPayload||"";
        let actionPayload={};
        if(encodedPayload){
          const parsed=JSON.parse(encodedPayload);
          if(parsed&&typeof parsed==="object"&&!Array.isArray(parsed))actionPayload=parsed;
        }
        if(expected){
          const typed=window.prompt(`Type ${expected} to continue`);
          if(typed!==expected){
            actionResultMessage(action,`${action} cancelled: exact confirmation required`);
            return;
          }
        }
        const result=await controlPost(action,target,expected,actionPayload);
        const local=$("candidate-control-result");
        if(local&&target===state.selected&&!isCanaryAction(action))local.textContent=result.ok?`${action} completed`:`${action} blocked: ${result.reason||"CONTROL_FAILED"}`;
      } finally {
        button.disabled=false;
      }
    });
    document.addEventListener("click",async event=>{
      const button=event.target.closest?.(".risk-settings-action");
      if(!button||button.disabled)return;
      button.disabled=true;
      try {
        const action=button.dataset.riskAction||"",values={};
        document.querySelectorAll("[data-risk-field]").forEach(input=>{
          if(input.dataset.riskField==="max_submitted_orders_per_day"){
            if(input.value==="custom"){
              const custom=$("[data-risk-submissions-custom]");
              if(!custom||custom.value==="")throw new Error("CUSTOM_ORDER_SUBMISSIONS_REQUIRED");
              values.max_submitted_orders_per_day=custom.value;
            } else values.max_submitted_orders_per_day=input.value;
          } else if(input.dataset.riskField==="max_aggregate_exposure_usd"){
            if(input.value!==""){
              values.max_aggregate_exposure_usd=input.value;
              values.max_aggregate_open_cost_usd=input.value;
            }
          } else if(input.value!=="" || input.dataset.riskOptional==="clearable") values[input.dataset.riskField]=input.value===""?null:input.value;
        });
        let result;
        let confirmation="";
        if(action==="save"){
          confirmation="SAVE RISK SETTINGS DRAFT";
          const typed=window.prompt("Type SAVE RISK SETTINGS DRAFT to review the changes");
          if(typed!==confirmation){
            actionResultMessage("canary.settings","Risk settings save cancelled: exact confirmation required");
            return;
          }
          result=await controlPost("canary.settings.save_draft","",confirmation,{values});
          if(result?.ok&&result?.result&&typeof result.result==="object"){
            const saved=result.result.risk_settings||result.result;
            const draft=saved.draft||saved;
            if(draft.config_id)riskReview.draft={values:draft.values||draft.settings||{},configId:draft.config_id,generation:draft.generation,hash:draft.config_hash};
          }
        } else if(action==="activate"){
          const draft=riskReview.draft,active=riskReview.active;
          if(!draft?.configId||!active?.generation){
            actionResultMessage("canary.settings","Activation blocked: review the changes again after refreshing active settings");
            return;
          }
          confirmation="ACTIVATE RISK SETTINGS DRAFT";
          const typed=window.prompt("Type ACTIVATE RISK SETTINGS DRAFT to confirm the reviewed changes");
          if(typed!==confirmation){
            actionResultMessage("canary.settings","Risk settings activation cancelled: exact confirmation required");
            return;
          }
          result=await controlPost("canary.settings.activate_draft","",confirmation,{config_id:draft.configId,expected_generation:Number(active.generation)});
        } else if(action==="enable"){
          const active=riskReview.active;
          if(!active?.configId||!Number.isInteger(Number(active.generation))||Number(active.generation)<1){
            actionResultMessage("canary.settings","Enable blocked: active settings review is unavailable; refresh and review again");
            return;
          }
          const summary=Object.entries(active.values||{}).filter(([name])=>riskReview.labels?.[name]).map(([name,value])=>`${riskReview.labels[name]}=${value}`).join(", ");
          if(!window.confirm(`Review active limits before enabling:\n${summary||"No active limits available"}`)){
            actionResultMessage("canary.settings","Enable cancelled: active limits were not confirmed");
            return;
          }
          confirmation=`ENABLE AUTO CANARY POLYMARKET ${active.configId} ${Number(active.generation)}`;
          result=await controlPost("canary.enable_auto","",confirmation,{venue:"polymarket",config_id:active.configId,expected_generation:Number(active.generation)});
        } else result={ok:false,reason:"UNKNOWN_RISK_SETTINGS_ACTION"};
        if(!result?.ok){
          const reason=String(result?.reason||"CONTROL_FAILED"),detail=[result?.detail,result?.details,result?.error,result?.message,result?.result?.detail,result?.result?.reason].filter(value=>typeof value==="string").join(" ");
          const conflict=reason.trim().toUpperCase()==="CANARYSETTINGSCONFLICT"||/settings (?:hash|generation)|control generation|draft review|reviewed settings/i.test(detail);
          const stale=conflict||/generation|hash|stale|review/i.test(reason);
          actionResultMessage("canary.settings",conflict?"Risk settings activation blocked: the reviewed settings or control generation changed. Refresh and review the settings again before activating.":`Risk settings action blocked: ${reason}${stale?" · refreshed active settings and review differences":""}`);
        }
      } finally {
        button.disabled=false;
      }
    });
    const recoveryForm=$("canary-recovery-form"),canaryView=$("view-canary"); if(recoveryForm&&canaryView){const wrapper=document.createElement("details");wrapper.className="panel";wrapper.innerHTML="<summary>Advanced entry recovery (operator-required only)</summary>";canaryView.appendChild(wrapper);wrapper.appendChild(recoveryForm);recoveryForm.classList.remove("panel");}
    document.addEventListener("click",async event=>{const button=event.target.closest?.("#canary-recovery-submit");if(!button)return;const eventId=$("canary-recovery-event")?.value.trim()||"",signalId=$("canary-recovery-signal")?.value.trim()||"",orderId=$("canary-recovery-order")?.value.trim()||"",confirmation=$("canary-recovery-confirm")?.value||"",node=$("canary-recovery-result");if(!eventId||!signalId||!orderId||confirmation!=="RECOVER UNKNOWN ENTRY"){if(node)node.textContent="Recovery blocked: exact event, signal, order, and confirmation are required";return;}const result=await controlPost("canary.recover_entry",eventId,confirmation,{event_id:eventId,signal_id:signalId,exchange_order_id:orderId});if(node)node.textContent=result.ok?"Recovery attached and reconciled":`Recovery blocked: ${result.reason||"CONTROL_FAILED"}`;});
    ensureActivityKind(); if($("crypto-symbol")){const oldSymbol=$("crypto-symbol"),newSymbol=oldSymbol.cloneNode(true);oldSymbol.replaceWith(newSymbol);newSymbol.addEventListener("input",()=>{state.page=1;saveState(true);loadPage("crypto",true);});} document.addEventListener("click",event=>{const button=event.target.closest?.(".copy");if(!button)return;navigator.clipboard?.writeText(button.dataset.copy||"").then(()=>{button.textContent="copied";setTimeout(()=>button.textContent="copy",1200);}).catch(()=>{});}); document.addEventListener("visibilitychange",()=>{if(document.hidden){if(activeController)activeController.abort();}else{nextRefreshAt=0;load();}});
    ensureFacets(); document.querySelectorAll(".tab").forEach(b=>b.addEventListener("click",()=>activate(b.dataset.view))); document.querySelectorAll("[data-link]").forEach(b=>b.addEventListener("click",e=>{e.preventDefault();activate(b.dataset.link)})); document.querySelectorAll(".filters input,.filters select").forEach(el=>el.addEventListener(el.tagName==="INPUT"?"input":"change",()=>{if(el.id.endsWith("-size")){const n=Number(el.value);if([10,25,50,100].includes(n)){state.page_size=n;document.querySelectorAll('select[id$="-size"]').forEach(s=>s.value=String(n));}} else if(el.id.includes("-filter"))state.filter=el.value;state.page=1;saveState(true);loadPage(state.tab)})); window.addEventListener("popstate",()=>{const q=new URLSearchParams(location.search),nextTab=q.get("tab")||"overview",changed=nextTab!==state.tab;params=q;state.tab=nextTab;state.page=Math.max(1,Number(q.get("page")||1));state.page_size=[10,25,50,100].includes(Number(q.get("page_size")))?Number(q.get("page_size")):25;state.filter=changed?"":q.get("filter")||"";state.sort=changed?"":q.get("sort")||"";state.direction=changed?"desc":q.get("direction")==="asc"?"asc":"desc";state.selected=changed?"":q.get("selected")||"";state.expanded=changed?false:q.get("expanded")==="1";restoreFacets();activate(state.tab,false)}); load(); activate(state.tab,false); const refreshHandle=setInterval(load,10000); window.addEventListener("beforeunload",()=>clearInterval(refreshHandle));
    if($("crypto-symbol"))$("crypto-symbol").addEventListener("input",async()=>{const symbol=$("crypto-symbol").value.trim(),q=new URLSearchParams({page:"1",page_size:String(state.page_size),direction:state.direction});if(symbol)q.set("symbol",symbol);const response=await fetch(`/api/v2/crypto-research?${q}`,{cache:"no-store"});if(response.ok)renderCrypto(await response.json());});
    // setInterval(load, 10000) is the ten-second refresh contract.
  </script>
</html>""".replace("__AXIOM_CONTROL_TOKEN__", str(control_token or "")).replace(
        "__BINANCE_NAV_LABEL__", binance_nav_label
    )

class _DashboardHandler(BaseHTTPRequestHandler):
    server: "_BoundDashboardServer"

    def log_message(self, format: str, *args: Any) -> None:
        return

    def _send(self, status: int, payload: Any, content_type: str = "application/json; charset=utf-8") -> None:
        try:
            body = payload.encode("utf-8") if isinstance(payload, str) else json.dumps(_jsonable(payload), sort_keys=True, indent=2, allow_nan=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.end_headers()
            self.wfile.write(body)
        except _CLIENT_DISCONNECT_ERRORS:
            _LOGGER.debug("dashboard client disconnected while sending response")

    def _loopback_client(self) -> bool:
        try:
            address = ipaddress.ip_address(str(self.client_address[0]))
            mapped = getattr(address, "ipv4_mapped", None)
            return bool(mapped.is_loopback if mapped is not None else address.is_loopback)
        except ValueError:
            return False

    def _server_authority(self) -> tuple[str, int] | None:
        address = getattr(self.server, "server_address", None)
        if not isinstance(address, tuple) or len(address) < 2:
            return None
        host = str(address[0]).strip().lower()
        try:
            port = int(address[1])
        except (TypeError, ValueError):
            return None
        return (host, port) if host and 0 < port <= 65535 else None

    def _host_allowed(self) -> bool:
        expected = self._server_authority()
        supplied = str(self.headers.get("Host", "") or "").strip()
        if expected is None or not supplied or any(char in supplied for char in "\r\n"):
            return False
        try:
            parsed = urlparse("//" + supplied)
            supplied_host = parsed.hostname
            supplied_port = parsed.port
        except ValueError:
            return False
        if (
            supplied_host is None
            or parsed.username is not None
            or parsed.password is not None
            or parsed.path
            or parsed.query
            or parsed.fragment
            or supplied_port is None
        ):
            return False
        return (
            supplied_host.strip().lower() == expected[0]
            and supplied_port == expected[1]
        )

    def _same_origin(self, origin: str) -> bool:
        expected = self._server_authority()
        if expected is None:
            return False
        try:
            parsed = urlparse(origin)
            supplied_host = parsed.hostname
            supplied_port = parsed.port
        except ValueError:
            return False
        if (
            parsed.scheme.lower() != "http"
            or supplied_host is None
            or parsed.username is not None
            or parsed.password is not None
            or parsed.path
            or parsed.params
            or parsed.query
            or parsed.fragment
            or supplied_port is None
        ):
            return False
        return (
            supplied_host.strip().lower() == expected[0]
            and supplied_port == expected[1]
        )

    def _read_request_allowed(self) -> bool:
        return self._loopback_client() and self._host_allowed()

    def _control_request_allowed(self) -> bool:
        expected = str(getattr(self.server, "control_token", "") or "")
        supplied = str(self.headers.get("X-Axiom-Control-Token", "") or "")
        if (
            not expected
            or not self._read_request_allowed()
            or not hmac.compare_digest(supplied, expected)
        ):
            return False
        origin = self.headers.get("Origin")
        if origin and not self._same_origin(str(origin)):
            return False
        return True


    def do_POST(self) -> None:  # noqa: N802 - stdlib handler API
        parsed = urlparse(self.path)
        route = parsed.path.rstrip("/")
        is_binance = route == "/api/binance/control"
        if route != "/api/control" and not is_binance:
            self._send(405 if parsed.path.startswith("/api/control") or parsed.path.startswith("/api/binance/control") else 404, {"error": "control endpoint required"})
            return
        if not is_binance and self.server.dashboard_data.control is None:
            self._send(503, {"error": "operator controls unavailable"})
            return
        if not self._control_request_allowed():
            self._send(403, {"error": "localhost control token required"})
            return
        content_length = self.headers.get("Content-Length")
        try:
            length = int(content_length or "0")
        except ValueError:
            length = -1
        if length < 0 or length > 16_384:
            self._send(413, {"error": "control request is too large"})
            return
        content_type = str(self.headers.get("Content-Type", "")).split(";", 1)[0].strip().lower()
        if content_type != "application/json":
            self._send(415, {"error": "application/json required"})
            return
        try:
            body = json.loads(
                self.rfile.read(length).decode("utf-8"),
                parse_constant=_reject_json_constant,
            )
        except (UnicodeDecodeError, ValueError, json.JSONDecodeError):
            self._send(400, {"error": "invalid JSON"})
            return
        if not isinstance(body, Mapping):
            self._send(400, {"error": "control request must be an object"})
            return
        if is_binance:
            action = body.get("action")
            if not isinstance(action, str) or not action.strip():
                self._send(400, {"error": "Binance action must be a non-empty string"})
                return
            normalized_action = action.strip().upper()
            if normalized_action in _BINANCE_HTTP_FORBIDDEN_ACTIONS:
                self._send(
                    403,
                    {
                        "ok": False,
                        "action": normalized_action,
                        "reason": "BROWSER_ACTION_FORBIDDEN",
                    },
                )
                return
            if set(body) - {"action", "payload"}:
                self._send(400, {"error": "unsupported Binance control fields"})
                return
            payload = body.get("payload", {})
            if not isinstance(payload, Mapping):
                self._send(400, {"error": "Binance payload must be an object"})
                return
            if self.server.dashboard_data.binance_canary is None:
                self._send(503, {"error": "Binance canary controls unavailable"})
                return
            try:
                result = self.server.dashboard_data.binance_canary.action(action, payload)
            except Exception as exc:
                self._send(503, {"ok": False, "action": action.strip().upper(), "reason": "BINANCE_ACTION_FAILED", "error": type(exc).__name__})
                return
            result = _binance_safe_value(result)
            if not isinstance(result, Mapping):
                result = {"ok": True, "action": action.strip().upper(), "result": result}
            reason = str(result.get("reason", "")) if isinstance(result, Mapping) else ""
            status = 200 if result.get("ok") is not False else (503 if reason.endswith(("UNAVAILABLE", "TIMEOUT", "FAILED")) else 400)
            self._send(status, result)
            return
        allowed_fields = {
            "action",
            "target",
            "confirm",
            "payload",
            "values",
            "actor",
            "config_id",
            "expected_generation",
            "venue",
        }
        if set(body) - allowed_fields:
            self._send(400, {"error": "unsupported control fields"})
            return
        payload = body.get("payload")
        flat_payload = {
            name: body[name]
            for name in ("values", "actor", "config_id", "expected_generation", "venue")
            if name in body
        }
        if payload is not None and not isinstance(payload, Mapping):
            self._send(400, {"error": "control payload must be an object"})
            return
        if payload is not None and flat_payload:
            self._send(400, {"error": "control payload must be nested"})
            return
        action_payload = dict(payload) if isinstance(payload, Mapping) else flat_payload
        action_name = str(body.get("action") or "").strip()
        payload_actions = {
            "canary.settings.save_draft",
            "risk.settings.save_draft",
            "canary.settings.activate_draft",
            "risk.settings.activate_draft",
            "canary.enable_auto",
            "canary.recover_entry",
        }
        if action_payload and action_name not in payload_actions:
            self._send(400, {"error": "action does not accept a payload"})
            return
        result = self.server.dashboard_data.control.execute(
            body.get("action", ""),
            body.get("target", ""),
            confirm=body.get("confirm", ""),
            payload=action_payload,
        )
        reason = str(result.get("reason", "")) if isinstance(result, Mapping) else ""
        if result.get("ok") if isinstance(result, Mapping) else False:
            status = 200
        elif reason in {"BOOTSTRAP_ALREADY_RUNNING", "NODE_ALREADY_RUNNING", "NODE_STOP_TIMEOUT", "BOOTSTRAP_NOT_RESUMABLE"} or "generation" in reason.lower() or "settings conflict" in reason.lower():
            status = 409
        elif reason.endswith("UNAVAILABLE") or reason.endswith("TIMEOUT") or reason.endswith("FAILED"):
            status = 503
        else:
            status = 400
        self._send(status, result)

    def do_GET(self) -> None:  # noqa: N802 - stdlib handler API
        if not self._read_request_allowed():
            self._send(403, {"error": "localhost dashboard host required"})
            return
        parsed = urlparse(self.path)
        path = parsed.path.strip("/")
        if path in {"api/control", "api/binance/control"}:
            self._send(405, {"error": "POST required"})
            return
        if path in {"", "index.html"}:
            try:
                self._send(
                    200,
                    _dashboard_html(
                        self.server.control_token,
                        binance_nav_label=self.server.dashboard_data.binance_nav_label(),
                    ),
                    "text/html; charset=utf-8",
                )
            except _CLIENT_DISCONNECT_ERRORS:
                return
            return
        query = parse_qs(parsed.query, keep_blank_values=True)
        if path.startswith("api/v2/"):
            endpoint = path[len("api/v2/") :]
            allowed = endpoint.lower() in _V2_ENDPOINTS or endpoint.lower().startswith(("datasets/", "candidates/", "hermes/", "crypto-research/"))
            if allowed:
                validation_error = _pagination_error(query)
                if validation_error:
                    self._send(400, {"error": "invalid pagination", "detail": validation_error})
                    return
                try:
                    self._send(200, self.server.dashboard_data.v2_snapshot(endpoint, query))
                except _CLIENT_DISCONNECT_ERRORS:
                    return
                except ValueError as exc:
                    self._send(400, {"error": "invalid request", "detail": type(exc).__name__ if endpoint.lower() == "binance-canary" else str(exc)})
                except Exception as exc:
                    self._send(503, {"error": "data unavailable", "detail": type(exc).__name__ if endpoint.lower() == "binance-canary" else str(exc)})
                return
        endpoint = path[4:] if path.startswith("api/") else path
        dynamic_strategy = endpoint.lower().startswith("strategy/") and len(endpoint.split("/", 1)[1]) > 0
        if endpoint in _ENDPOINTS or dynamic_strategy:
            try:
                if dynamic_strategy:
                    endpoint = "strategy/" + unquote(endpoint.split("/", 1)[1])
                self._send(200, self.server.dashboard_data.snapshot(endpoint, query))
            except _CLIENT_DISCONNECT_ERRORS:
                return
            except Exception as exc:
                self._send(503, {"error": "data unavailable", "detail": str(exc)})
            return
        self._send(404, {"error": "not found", "endpoints": ["/", *[f"/api/{name}" for name in _ENDPOINTS], *[f"/api/v2/{name}" for name in _V2_ENDPOINTS]]})

class _BoundDashboardServer(ThreadingHTTPServer):
    def __init__(self, address: tuple[str, int], data: DashboardData) -> None:
        if not _loopback_host(str(address[0])):
            raise ValueError("dashboard server must bind to a loopback address")
        super().__init__(address, _DashboardHandler)
        self.dashboard_data = data
        self.control_token = secrets.token_urlsafe(32)
        self.daemon_threads = True
        self.allow_reuse_address = True


class DashboardServer:
    """Threaded local dashboard server; ``start`` is non-blocking."""

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 0,
        *,
        data: DashboardData | None = None,
        **data_kwargs: Any,
    ) -> None:
        if not _loopback_host(str(host)):
            raise ValueError("dashboard server must bind to a loopback address")
        self.host = host
        self.port = int(port)
        self.data = data or DashboardData(**data_kwargs)
        self._server: _BoundDashboardServer | None = None
        self._thread: Thread | None = None

    @property
    def address(self) -> tuple[str, int] | None:
        return None if self._server is None else (str(self._server.server_address[0]), int(self._server.server_address[1]))

    @property
    def url(self) -> str | None:
        address = self.address
        return None if address is None else f"http://{address[0]}:{address[1]}"

    def start(self) -> "DashboardServer":
        if self._server is not None:
            return self
        self._server = _BoundDashboardServer((self.host, self.port), self.data)
        self._thread = Thread(target=self._server.serve_forever, name="axiom-dashboard", daemon=True)
        self._thread.start()
        return self

    def serve_forever(self) -> None:
        if self._server is None:
            self._server = _BoundDashboardServer((self.host, self.port), self.data)
        self._server.serve_forever()

    def stop(self) -> None:
        if self._server is None:
            return
        self._server.shutdown()
        self._server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=2)
        self._server = None
        self._thread = None

    close = stop

    def __enter__(self) -> "DashboardServer":
        return self.start()

    def __exit__(self, *_: Any) -> None:
        self.stop()


Dashboard = DashboardServer


def create_dashboard_server(host: str = "127.0.0.1", port: int = 0, **kwargs: Any) -> DashboardServer:
    return DashboardServer(host, port, **kwargs)


def serve_dashboard(host: str = "127.0.0.1", port: int = 8080, **kwargs: Any) -> None:
    server = DashboardServer(host, port, **kwargs)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.stop()


__all__ = ["DashboardData", "DashboardServer", "Dashboard", "create_dashboard_server", "serve_dashboard"]
