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
import re
from decimal import Decimal
from html import escape as _html_escape
from pathlib import Path
import secrets
import sqlite3
from . import canary as canary_module
from .canary import CanaryService, _canary_eligibility_is_bound, _canary_has_last_good
from .canary_settings import CanarySettingsService
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from typing import Any, Callable, Mapping, Sequence
from urllib.parse import parse_qs, unquote, urlparse
from .operator import (
    CANARY_CONNECTIVITY_CONFIG_KEY,
    OperatorControlError,
    OperatorControlPlane,
    _connectivity_safe_diagnostics,
    _authorization_public_projection,
    _public_setup_bindings,
    _public_draft_member_bindings,
    _rolling_policy_identity,
    _safe_value,
    _stored_connectivity_projection,
)

_CLIENT_DISCONNECT_ERRORS = (BrokenPipeError, ConnectionAbortedError, ConnectionResetError)
_LOGGER = logging.getLogger(__name__)

from .director import research_summary
from .domain import ensure_utc, parse_timestamp, to_record
from .rolling_portfolio import _source_class
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
    "ui-state",
    "ui-record",
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
_UI_RECORD_KINDS = frozenset(
    {
        "market",
        "order",
        "submission",
        "reservation",
        "fill",
        "risk-fill",
        "position",
        "mark",
        "cashflow",
    }
)
_UI_ASSETS = {
    "/assets/app.js": ("app.js", "text/javascript; charset=utf-8"),
    "/assets/pages.js": ("pages.js", "text/javascript; charset=utf-8"),
    "/assets/styles.css": ("styles.css", "text/css; charset=utf-8"),
}
_V2_ENDPOINTS = ("overview-summary", "canary", "rolling-portfolio", "binance-canary", "datasets", "activity", "candidates", "polymarket", "hermes", "crypto-research", "crypto", "paper", "shadow")

_DEFAULT_PAGE_SIZE = 25
_PAGE_SIZE_OPTIONS = (10, 25, 50, 100)
_MAX_PAGE_SIZE = 100
_CANARY_ELIGIBLE_STAGES = frozenset({"FROZEN", "PAPER_FORWARD", "PAPER_PROMOTABLE"})
_ROLLING_RISK_USAGE_FIELDS = (
    "rolling_global_reserved_usd",
    "rolling_global_budget_usd",
    "rolling_strategy_reserved_usd",
    "rolling_strategy_allocations",
)
_CANARY_USAGE_FIELDS = (
    "submitted_orders",
    "buy_filled_usd",
    "buy_pending_usd",
    "buy_unknown_usd",
    "gross_daily_buy_usd",
    "all_in_buy_reserved_usd",
    "aggregate_open_cost_usd",
    "aggregate_exposure_usd",
    "open_positions",
    "realized_loss_usd",
    "today_realized_pnl_usd",
    "equity_loss_usd",
    "equity_status",
    "risk_breaker",
    "per_market_buy_usd",
    "per_event_buy_usd",
    "cumulative_buy_usd",
    "external_flow_usd",
)
_PAPER_FORWARD_STAGES = frozenset({"PAPER_FORWARD", "PAPER_PROMOTABLE"})
_ROLLING_REVIEW_HTTP_FIELDS = frozenset(
    {
        "policy",
        "values",
        "actor",
        "expected_risk_config_id",
        "expected_risk_config_generation",
        "expected_risk_config_hash",
    }
)
_ROLLING_ACTIVATE_HTTP_FIELDS = frozenset(
    {
        "policy_id",
        "policy_version",
        "draft_id",
        "draft_version",
        "actor",
        "expected_risk_config_id",
        "expected_risk_config_generation",
        "expected_risk_config_hash",
    }
)
_EXECUTION_AUTHORIZATION_HTTP_FIELDS = frozenset(
    {
        "authorization_id",
        "actor",
        "expected_generation",
        "reason",
    }
)
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
_MARKET_SCOPE_PUBLIC_ITEM_FIELDS = (
    "candidate_id",
    "market_id",
    "condition_id",
    "token_id",
    "token_ids",
    "outcome",
    "category",
    "market_type",
    "status",
    "stage",
    "count",
    "reason",
    "reason_code",
    "scope_hash",
    "scope_version",
    "policy_hash",
    "provenance_hash",
    "setup_hash",
    "strategy_version_id",
    "research_trial_id",
    "evidence_digest",
    "source_class",
    "resolved_at",
    "updated_at",
)
_MARKET_SCOPE_DETAIL_LIST_KEYS = frozenset(
    {"items", "resolution_items", "resolution_blockers", "blockers", "stages", "resolution_stages"}
)


def _market_scope_public_item(value: Any) -> Any:
    """Retain scope identity/status while excluding raw policy/provenance bodies."""
    if not isinstance(value, Mapping):
        return _jsonable(value)
    projected: dict[str, Any] = {}
    for key in _MARKET_SCOPE_PUBLIC_ITEM_FIELDS:
        if key not in value or value[key] in (None, ""):
            continue
        child = value[key]
        if key in {"token_ids"}:
            if isinstance(child, (list, tuple, set, frozenset)):
                projected[key] = [
                    _jsonable(item) for item in list(child)[:32] if item not in (None, "")
                ]
            continue
        if isinstance(child, Mapping):
            continue
        if isinstance(child, (list, tuple, set, frozenset)):
            projected[key] = [_jsonable(item) for item in list(child)[:32]]
        else:
            projected[key] = _jsonable(child)
    return projected


def _public_market_scope_funnel(value: Any) -> dict[str, Any]:
    """Cap persisted scope detail lists after internal candidate matching."""
    result = dict(value) if isinstance(value, Mapping) else {}
    for key in _MARKET_SCOPE_DETAIL_LIST_KEYS:
        child = result.get(key)
        if isinstance(child, (list, tuple)):
            result[key] = list(child)[:64]
    return result


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
        "MARKET_RESOLUTION_FAILURE",
        "DATA_INSUFFICIENT",
        "SOFTWARE_OR_INPUT_ERROR",
        "VALIDATION_QUALIFIED",
        "FINAL_ASSESSMENT",
    }
)
_ROLLING_DIAGNOSTIC_LIMIT = 64
_ROLLING_DIAGNOSTIC_STRING_LENGTH = 512



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
    if depth >= 4 and isinstance(value, (Mapping, list, tuple, set, frozenset)):
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
_HTTP_JSON_MAX_DEPTH = 8
_HTTP_JSON_MAX_ITEMS = 64
_HTTP_JSON_MAX_STRING = 4_096
_HTTP_JSON_MAX_KEYS = 128
_HTTP_JSON_MAX_BYTES = 1_048_576


def _http_bound_value(
    value: Any,
    *,
    depth: int = 0,
    stats: dict[str, int | bool],
    key: str | None = None,
) -> Any:
    """Bound arbitrary GET output without recursively materializing raw JSON."""
    if depth >= _HTTP_JSON_MAX_DEPTH or key in {
        "market_bindings",
        "current_market_bindings",
    }:
        market_fields = _HTTP_PUBLIC_SECTION_FIELDS.get("market_bindings", ())

        def bounded_market_bindings(raw: Any) -> list[dict[str, Any]]:
            if not isinstance(raw, (list, tuple, set, frozenset)):
                return []
            result: list[dict[str, Any]] = []
            for binding in list(raw)[:_HTTP_JSON_MAX_ITEMS]:
                if not isinstance(binding, Mapping):
                    continue
                projected: dict[str, Any] = {}
                for field in market_fields:
                    if field not in binding:
                        continue
                    child = binding[field]
                    if field == "outcome_token_ids":
                        if isinstance(child, (list, tuple, set, frozenset)):
                            projected[field] = [
                                _http_bound_value(
                                    item,
                                    depth=0,
                                    stats=stats,
                                    key=field,
                                )
                                for item in list(child)[:_HTTP_JSON_MAX_ITEMS]
                            ]
                        continue
                    if isinstance(child, (str, int, float, bool)) or child is None:
                        projected[field] = _http_bound_value(
                            child,
                            depth=0,
                            stats=stats,
                            key=field,
                        )
                result.append(projected)
            return result

        if key in {"market_bindings", "current_market_bindings"}:
            if isinstance(value, Mapping):
                projected: dict[str, Any] = {}
                for field in market_fields:
                    if field not in value:
                        continue
                    child = value[field]
                    if field == "outcome_token_ids":
                        if isinstance(child, (list, tuple, set, frozenset)):
                            projected[field] = [
                                _http_bound_value(
                                    item,
                                    depth=0,
                                    stats=stats,
                                    key=field,
                                )
                                for item in list(child)[:_HTTP_JSON_MAX_ITEMS]
                            ]
                        continue
                    if isinstance(child, (str, int, float, bool)) or child is None:
                        projected[field] = _http_bound_value(
                            child,
                            depth=0,
                            stats=stats,
                            key=field,
                        )
                return projected
            return bounded_market_bindings(value)
        if key in {"setup_bindings", "draft_member_bindings"}:
            if not isinstance(value, (list, tuple, set, frozenset)):
                return []
            binding_fields = _HTTP_PUBLIC_SECTION_FIELDS.get(key, ())
            projected_bindings: list[dict[str, Any]] = []
            for binding in list(value)[:_HTTP_JSON_MAX_ITEMS]:
                if not isinstance(binding, Mapping):
                    continue
                projected: dict[str, Any] = {}
                for field in binding_fields:
                    child = binding.get(field)
                    if field == "market_bindings":
                        projected[field] = bounded_market_bindings(child)
                    elif isinstance(child, (str, int, float, bool)) or child is None:
                        projected[field] = _http_bound_value(
                            child,
                            depth=0,
                            stats=stats,
                            key=field,
                        )
                projected_bindings.append(projected)
            return projected_bindings
        stats["truncated"] = True
        stats["omitted_items"] = int(stats.get("omitted_items", 0)) + 1
        return "<truncated>"
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for index, (child_key, child) in enumerate(value.items()):
            if index >= _HTTP_JSON_MAX_KEYS:
                stats["truncated"] = True
                stats["omitted_items"] = int(stats.get("omitted_items", 0)) + len(value) - index
                break
            child_depth = depth + 1
            if depth == 0 and str(child_key) in {"operator_controls", "control_status"}:
                # These are already bounded public components; do not spend
                # the root envelope depth before their typed leaf projection.
                child_depth = depth
            if str(child_key) in {"readiness", "connectivity", "token_readiness"}:
                # These are bounded typed readiness projections; restart their
                # finite envelope so exact leg quote leaves remain visible.
                child_depth = 0
            if (
                key in {"readiness", "connectivity"}
                and str(child_key) == "diagnostics"
            ):
                child_depth = 0
            result[str(child_key)] = _http_bound_value(
                child,
                depth=child_depth,
                stats=stats,
                key=str(child_key),
            )
        return result
    if isinstance(value, (list, tuple, set, frozenset)):
        source = list(value)
        result = [
            _http_bound_value(
                child,
                depth=depth + 1,
                stats=stats,
                key=key,
            )
            for child in source[:_HTTP_JSON_MAX_ITEMS]
        ]
        if len(source) > _HTTP_JSON_MAX_ITEMS:
            stats["truncated"] = True
            stats["omitted_items"] = int(stats.get("omitted_items", 0)) + len(source) - _HTTP_JSON_MAX_ITEMS
        return result
    if isinstance(value, str):
        if len(value) <= _HTTP_JSON_MAX_STRING:
            return value
        stats["truncated"] = True
        stats["omitted_items"] = int(stats.get("omitted_items", 0)) + 1
        return "<truncated>"
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, bool) or value is None or isinstance(value, int):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, Decimal):
        return format(value, "f")[:_HTTP_JSON_MAX_STRING]
    if is_dataclass(value):
        try:
            return _http_bound_value(
                to_record(value),
                depth=depth,
                stats=stats,
                key=key,
            )
        except (TypeError, ValueError):
            return _http_bound_value(
                asdict(value),
                depth=depth,
                stats=stats,
                key=key,
            )
    try:
        converted = value.value if hasattr(value, "value") else str(value)
    except Exception:
        converted = "<unserializable>"
    return _http_bound_value(converted, depth=depth, stats=stats, key=key)

_HTTP_PUBLIC_SECTION_FIELDS: dict[str, tuple[str, ...]] = {
    "risk_settings": (
        "status", "config_id", "generation", "config_hash", "control_generation",
        "active", "draft", "active_config", "draft_config", "effective_limits",
        "active_limits", "usage", "remaining", "cumulative_buy_cap_usd",
        "remaining_cumulative_buy_usd", "cumulative_buy_cap_state",
        "cumulative_buy_over_limit_reason",
    ),
    "active": ("config_id", "id", "generation", "config_hash", "status", "values", "settings", "limits"),
    "active_config": ("config_id", "id", "generation", "config_hash", "status", "values", "settings", "limits"),
    "draft": ("config_id", "id", "generation", "config_hash", "status", "values", "settings", "limits"),
    "draft_config": ("config_id", "id", "generation", "config_hash", "status", "values", "settings", "limits"),
    "policy": (
        "policy_id", "id", "version", "policy_version", "config_hash", "policy_hash",
        "draft_id", "draft_version", "draft_hash", "status", "review_status",
        "global_budget", "max_members", "risk_config_id", "risk_config_generation",
        "risk_config_hash", "paper_only", "live_execution",
    ),
    "active_policy": (
        "policy_id", "id", "version", "policy_version", "config_hash", "policy_hash",
        "draft_id", "draft_version", "draft_hash", "status", "review_status",
        "global_budget", "max_members", "risk_config_id", "risk_config_generation",
        "risk_config_hash", "paper_only", "live_execution",
    ),
    "reviewed_policy": (
        "policy_id", "id", "version", "policy_version", "config_hash", "policy_hash",
        "draft_id", "draft_version", "draft_hash", "status", "review_status",
        "global_budget", "max_members", "risk_config_id", "risk_config_generation",
        "risk_config_hash", "paper_only", "live_execution",
    ),
    "proposed_policy": (
        "policy_id", "id", "version", "policy_version", "config_hash", "policy_hash",
        "draft_id", "draft_version", "draft_hash", "status", "review_status",
        "global_budget", "max_members", "risk_config_id", "risk_config_generation",
        "risk_config_hash", "paper_only", "live_execution",
    ),
    "execution_authorization": (
        "status", "authorization_id", "id", "generation", "mode", "purpose",
        "exact_strategy_versions", "strategy_version_ids", "reviewed_selection_policy_hash",
        "selection_policy_hash", "selection_id", "selection_hash", "adverse_evidence_ack",
        "lifetime_budget", "shared_allocation", "expiry_anchor", "duration_seconds",
        "expires_at", "stop_rules", "admission_mode", "proposal_only", "controller_lease",
        "scope_hash", "scope_version", "scope_draft_id", "scope_draft_hash",
        "scope_draft_version", "active_scope_hash", "active_scope_version",
        "frozen_scope_hash", "frozen_scope_version",
        "active_settings_hash",
        "active_settings_generation", "policy_id", "policy_version", "policy_hash",
        "setup_bindings", "draft_member_bindings", "proposed_allocation_total",
        "proposed_allocation_risk_digest", "active", "draft", "identity",
        "rolling_exploratory_scope_draft", "blockers", "paper_only", "live_execution",
    ),
    "exploratory_live_review": (
        "profitability", "status", "proposal_status", "proposal", "scope",
        "scope_binding", "blockers", "no_member_reason", "allocation",
        "policy", "review", "selected_setups", "entry_predicate", "direction", "sizing",
        "exit", "lookback", "adverse_evidence", "members", "limits", "limit_blockers",
        "readiness", "authorization", "authorization_choices", "authorization_bindings",
        "shared_allocation", "lifetime_budget", "expiry_anchor", "duration_seconds",
        "expires_at", "stop_rules", "accounting", "paper_only", "live_execution",
    ),
    "proposal": (
        "status", "selection_id", "selection_hash", "policy_id", "policy_version",
        "policy_hash", "scope_draft_id", "scope_draft_version", "scope_draft_hash",
        "proposed_allocation_total", "proposed_allocation_risk_digest", "members",
        "proposal_only",
    ),
    "no_member_reason": (
        "code", "selection_status", "selection_id", "k", "global_budget",
        "selection_reasons", "actionable_reasons",
    ),
    "authorization_choices": (
        "purpose", "shared_allocation", "lifetime_budget", "expiry_anchor",
        "duration_seconds", "expires_at", "stop_rules", "status", "approved",
    ),
    "selected_setups": (
        "name", "strategy_name", "setup_name", "family", "experiment_family",
        "strategy_id", "strategy_version_id", "strategy_version", "strategy_hash",
        "candidate_id", "setup_id", "setup_version", "setup_hash", "operational_setup_hash",
        "entry_predicate", "entry", "outcome_mapping", "direction", "sizing",
        "holding", "holding_semantics", "exit", "exit_semantics", "lookback", "parameters",
        "strategy", "strategy_document", "operational_setup", "setup_policy",
        "scope", "market_scope", "scope_restrictions", "restrictions", "exclusions",
        "excluded_markets", "frozen_scope",
    ),
    "scope_binding": (
        "draft_id", "draft_hash", "draft_version", "scope_hash", "scope_version",
        "active_scope_hash", "active_scope_version", "frozen_scope_hash",
        "frozen_scope_version",
    ),
    "scope_state": (
        "draft_id", "draft_hash", "draft_version", "scope_hash", "scope_version",
        "status", "scope", "market_ids", "market_type", "supported_market_types",
        "category_restriction", "exclusions",
    ),
    "scope_document": (
        "schema_version", "version", "mode", "name", "type", "instrument", "categories",
        "market_ids", "exact_market_ids", "filters", "regime_restrictions", "provenance",
    ),
    "authorization_bindings": (
        "selection_id", "selection_hash", "policy_id", "policy_version", "policy_hash",
        "setup_bindings", "draft_member_bindings", "proposed_allocation_total",
        "proposed_allocation_risk_digest",
    ),
    "setup_bindings": (
        "strategy_version_id", "candidate_id", "setup_id", "setup_version", "setup_hash",
        "operational_setup_hash", "draft_bound", "draft_id", "draft_hash", "scope_hash",
        "scope_version", "market_bindings",
    ),
    "draft_member_bindings": (
        "strategy_version_id", "candidate_id", "draft_bound", "draft_id", "draft_hash",
        "scope_hash", "scope_version", "operational_setup_hash", "market_bindings",
    ),
    "market_bindings": (
        "market_id", "condition_id", "yes_token_id", "no_token_id", "outcome_token_id",
        "outcome_token_ids",
    ),
    "readiness": (
        "status", "fresh", "checked_at", "market_id", "token_id", "diagnostics", "blockers",
    ),
    "diagnostics": (
        "account", "geoblock", "balance", "allowance", "market", "book",
        "token_readiness",
    ),
    "accounting": (
        "buy_pending_usd", "buy_unknown_usd", "all_in_buy_reserved_usd",
        "reserved_exit_capacity", "proposed_allocation_total",
        "proposed_allocation_risk_digest", "equity_status",
    ),
    "rolling_portfolio": (
        "status", "controller_status", "k", "actual", "actual_k", "actionable",
        "policy", "active_policy", "reviewed_policy", "proposed_policy", "policy_review",
        "allocation_review", "risk", "evidence", "selection", "signal", "execution",
        "active_rows", "global_limits", "global_limits_usage", "rolling_usage", "canary_usage",
        "events", "event_history", "reason_history", "admission_reason_history",
        "replacement_reason_history", "next_jobs", "cold_start_requirements", "blockers",
        "actionable_blockers", "paper_only", "live_execution",
    ),
    "scope_draft": (
        "draft_id", "scope_id", "status", "scope_hash", "scope_version", "draft_hash",
        "draft_version", "market_type", "supported_market_types", "category_restriction",
        "exclusions", "members", "policy", "scope", "paper_only", "live_execution",
    ),
    "rolling_exploratory_scope_draft": (
        "draft_id", "scope_id", "status", "scope_hash", "scope_version", "draft_hash",
        "draft_version", "market_type", "supported_market_types", "category_restriction",
        "exclusions", "members", "policy", "scope", "paper_only", "live_execution",
    ),
    "authorization": (
        "status", "authorization_id", "id", "generation", "mode", "purpose",
        "exact_strategy_versions", "strategy_version_ids", "reviewed_selection_policy_hash",
        "selection_policy_hash", "selection_id", "selection_hash", "adverse_evidence_ack",
        "lifetime_budget", "stop_rules", "expires_at", "controller_lease", "active", "draft",
        "identity", "rolling_exploratory_scope_draft", "blockers", "paper_only", "live_execution",
    ),
    "controller_lease": (
        "status", "owner_id", "generation", "lease_id", "acquired_at", "expires_at",
        "heartbeat_at", "updated_at", "reason", "blocker", "paper_only", "live_execution",
    ),
    "market_scope_funnel": (
        "available", "total", "resolution_count", "stage_counts", "stages", "blocker_counts",
        "timestamps", "resolution_status_counts", "resolution_reason_counts",
        "resolution_items", "resolution_blockers", "resolution_stages", "as_of",
        "storage_backed", "live_execution",
    ),
    "resolution_items": (
        "candidate_id", "market_id", "condition_id", "token_id", "token_ids", "outcome",
        "category", "market_type", "status", "stage", "count", "reason", "reason_code", "scope_hash",
        "scope_version", "policy_hash", "provenance_hash", "setup_hash",
        "strategy_version_id", "research_trial_id", "evidence_digest", "source_class",
        "resolved_at", "updated_at",
    ),
    "resolution_blockers": ("reason", "reason_code", "count", "status", "candidate_id", "resolved_at"),
    "resolution_stages": ("status", "stage", "count", "blocker_counts", "timestamps"),
    "canary": (
        "status", "control_state", "micro_live_canary", "display_state", "production_live_trading",
        "selection_status", "selection_valid", "selection_invalidation_reason", "selected_candidate",
        "last_selected_candidate", "winner_id", "winner_rank", "winner_score", "risk_envelope",
        "risk_limits", "risk_settings", "autonomous", "latest_signal", "status_report",
        "control", "readiness", "worker", "execution", "blocker", "last_cycle_blocker",
        "signal_scan_reason_counts", "trades", "execution_event_count", "real_execution_events",
        "credentials", "live_execution",
    ),
    "autonomous_canary": (
        "enabled", "control_state", "micro_live_canary", "display_state", "selection_status",
        "selection_valid", "selected_candidate", "last_selected_candidate", "rank", "score",
        "blocker", "next_decision", "worker_status", "last_signal_id", "candidates_ranked",
        "candidates_signal_checked", "candidates_no_signal", "actionable_candidates_found",
        "selected_actionable_candidate", "selected_actionable_rank", "selected_actionable_score",
        "signal_scan_candidate_universe_hash", "signal_scan_cycle_id", "signal_scan_status",
        "signal_scan_checked_this_cycle", "signal_scan_remaining_this_cycle",
        "signal_scan_coverage_percentage", "signal_scan_reason_counts_json", "signal_scan_checked_keys",
    ),
    "status_report": (
        "control_state", "micro_live_canary", "display_state", "selection_status",
        "selection_valid", "selected_candidate", "last_selected_candidate", "winner_id",
        "winner_rank", "winner_score", "latest_signal", "autonomous", "control",
        "authoritative_control", "readiness", "authoritative_readiness", "worker", "execution",
        "risk_envelope", "risk_limits", "blocker", "live_execution",
    ),
    "operator_controls": (
        "node", "bootstrap", "hermes", "paper", "collector", "credentials", "canary",
        "risk_settings", "controller_lease", "execution_authorization", "exploratory_live_review",
        "scope_draft", "rolling_exploratory_scope_draft", "blocker", "blockers", "paper_only",
        "live_execution",
    ),
}
_HTTP_PUBLIC_SECTION_PRIORITY = (
    # Controls contain the operator's actionable proposal, blockers, and
    # execution-authorization bindings.  Reserve this bounded surface before
    # verbose rolling history can consume the hard response byte cap.
    "operator_controls",
    "execution_authorization",
    "exploratory_live_review",
    "rolling_exploratory_scope_draft",
    "scope_draft",
    "controller_lease",
    "canary",
    "status_report",
    "control",
    "readiness",
    "worker",
    "execution",
    "market_scope_funnel",
    "risk_settings",
    "rolling_portfolio",
)


def _http_public_section(
    value: Any,
    key: str,
    *,
    depth: int = 0,
    parent_key: str | None = None,
) -> Any:
    """Project known dashboard sections before the hard byte-cap fallback."""
    if depth >= _HTTP_JSON_MAX_DEPTH:
        return "<truncated>"
    if isinstance(value, Mapping):
        fields_key = key
        if parent_key == "scope":
            if key == "draft":
                fields_key = "scope_draft"
            elif key in {"active", "frozen"}:
                fields_key = "scope_state"
        elif parent_key in {
            "draft", "active", "frozen", "scope_state", "scope_draft",
            "rolling_exploratory_scope_draft",
        } and key == "scope":
            fields_key = "scope_document"
        elif parent_key == "policy" and key == "scope":
            fields_key = "scope_document"
        elif parent_key in {"execution_authorization", "authorization"} and key in {
            "active", "draft", "authorization"
        }:
            fields_key = "execution_authorization"
        fields = _HTTP_PUBLIC_SECTION_FIELDS.get(fields_key)
        if fields is None:
            return value
        return {
            field: _http_public_section(
                value[field],
                field,
                depth=depth + 1,
                parent_key=fields_key,
            )
            for field in fields
            if field in value
        }
    if isinstance(value, (list, tuple, set, frozenset)):
        return [
            _http_public_section(
                child,
                key,
                depth=depth + 1,
                parent_key=parent_key,
            )
            for child in list(value)[:_HTTP_JSON_MAX_ITEMS]
        ]
    return value



def _http_json_bytes(payload: Any) -> bytes:
    stats: dict[str, int | bool] = {"truncated": False, "omitted_items": 0}
    bounded = _http_bound_value(payload, stats=stats)
    metadata = {
        "truncated": bool(stats["truncated"]),
        "omitted_items": int(stats["omitted_items"]),
        "max_depth": _HTTP_JSON_MAX_DEPTH,
        "max_items": _HTTP_JSON_MAX_ITEMS,
        "max_string": _HTTP_JSON_MAX_STRING,
        "max_bytes": _HTTP_JSON_MAX_BYTES,
    }
    if isinstance(bounded, Mapping):
        bounded = dict(bounded)
    else:
        bounded = {"value": bounded}
    body = json.dumps(bounded, sort_keys=True, indent=2, allow_nan=False).encode("utf-8")
    if len(body) <= _HTTP_JSON_MAX_BYTES and not stats["truncated"]:
        return body
    if len(body) <= _HTTP_JSON_MAX_BYTES:
        bounded["_response_projection"] = metadata
        return json.dumps(bounded, sort_keys=True, indent=2, allow_nan=False).encode("utf-8")
    # A bounded top-level scalar fallback preserves IDs/counts/status while
    # dropping nested sections that would otherwise exceed the hard byte cap.
    compact: dict[str, Any] = {}
    byte_metadata = {
        **metadata,
        "truncated": True,
        "byte_cap_applied": True,
    }
    if isinstance(bounded, Mapping):
        def add_candidate(key: str, value: Any) -> None:
            if key == "_response_projection" or key in compact:
                return
            projected = _http_public_section(value, key)
            candidate = dict(compact)
            candidate[key] = projected
            candidate["_response_projection"] = byte_metadata
            candidate_body = json.dumps(
                candidate,
                sort_keys=True,
                indent=2,
                allow_nan=False,
            ).encode("utf-8")
            if len(candidate_body) > _HTTP_JSON_MAX_BYTES:
                stats["omitted_items"] = int(stats.get("omitted_items", 0)) + 1
                return
            compact[key] = projected

        for key in _HTTP_PUBLIC_SECTION_PRIORITY:
            if key in bounded:
                add_candidate(key, bounded[key])
        for key, value in bounded.items():
            key_text = str(key)
            if key_text in compact or key_text == "_response_projection":
                continue
            if isinstance(value, (str, int, float, bool)) or value is None:
                add_candidate(key_text, value)
    compact["_response_projection"] = byte_metadata
    body = json.dumps(compact, sort_keys=True, indent=2, allow_nan=False).encode("utf-8")
    if len(body) <= _HTTP_JSON_MAX_BYTES:
        return body
    # The metadata-only fallback is intentionally tiny and always truthful.
    return json.dumps(
        {"value": "<truncated>", "_response_projection": byte_metadata},
        sort_keys=True,
        indent=2,
        allow_nan=False,
    ).encode("utf-8")


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
_SHADOW_STATUSES = frozenset(
    {"REGISTERED", "RUNNING", "WAITING_FOR_DATA", "COMPLETED", "BLOCKED", "STOPPED"}
)
_SHADOW_ACTIVE_STATUSES = frozenset({"REGISTERED", "RUNNING", "WAITING_FOR_DATA"})
_SHADOW_MEMBER_PUBLIC_FIELDS = (
    "shadow_member_id",
    "candidate_id",
    "family",
    "setup_id",
    "setup_hash",
    "strategy_hash",
    "model_hash",
    "scope_hash",
    "scope_version",
)
_SHADOW_ACCOUNTING_FIELDS = (
    "signals",
    "declines",
    "risk_rejections",
    "fills",
    "exits",
    "buy_fills",
    "sell_fills",
    "filled_quantity",
    "fees",
)
_SHADOW_BUDGET_FIELDS = (
    "bankroll",
    "allocated",
    "allocated_capital",
    "reserved",
    "spent",
    "used",
    "remaining",
    "available",
    "limit",
    "currency",
)


def _shadow_public_text(value: Any, *, limit: int = 256) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text[:limit] if text else None


def _shadow_public_number(value: Any, *, integer: bool = False) -> int | float | None:
    if isinstance(value, bool) or value is None:
        return None
    if integer:
        try:
            number = int(value)
        except (TypeError, ValueError, OverflowError):
            return None
        return number if number >= 0 else None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(number) or number < 0:
        return None
    return number


def _shadow_public_timestamp(value: Any) -> str | None:
    if value is None:
        return None
    stamp = parse_timestamp(value)
    return stamp.isoformat() if stamp is not None else None


def _shadow_public_accounting(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        return {}
    result: dict[str, Any] = {}
    for field in _SHADOW_ACCOUNTING_FIELDS:
        if field not in value:
            continue
        projected = _shadow_public_number(value.get(field), integer=field in {
            "signals", "declines", "risk_rejections", "fills", "exits", "buy_fills", "sell_fills",
        })
        if projected is not None:
            result[field] = projected
    return result


def _shadow_public_budget(
    manifest_shared: Mapping[str, Any],
    state: Mapping[str, Any],
) -> dict[str, Any]:
    sources: list[Mapping[str, Any]] = []
    for source_name in ("shared_budget", "budget", "accounting"):
        source = state.get(source_name)
        if isinstance(source, Mapping):
            sources.append(source)
    sources.append(manifest_shared)
    result: dict[str, Any] = {}
    for field in _SHADOW_BUDGET_FIELDS:
        for source in sources:
            if field not in source:
                continue
            value = source.get(field)
            projected = (
                _shadow_public_text(value)
                if field == "currency"
                else _shadow_public_number(value)
            )
            if projected is not None:
                result[field] = projected
                break
    bankroll = result.get("bankroll")
    if bankroll is not None and "allocated_capital" not in result:
        result["allocated_capital"] = bankroll / 2.0 if isinstance(bankroll, (int, float)) else None
    return result


def _shadow_public_members(
    manifest: Mapping[str, Any],
    state: Mapping[str, Any],
) -> list[dict[str, Any]]:
    raw_members = manifest.get("members")
    state_members = state.get("members")
    state_members = state_members if isinstance(state_members, Mapping) else {}
    if not isinstance(raw_members, (list, tuple)):
        return []
    members: list[dict[str, Any]] = []
    for raw in raw_members[:2]:
        if not isinstance(raw, Mapping):
            continue
        member: dict[str, Any] = {}
        for field in _SHADOW_MEMBER_PUBLIC_FIELDS:
            value = raw.get(field)
            if field in {"shadow_member_id", "candidate_id", "family", "setup_id", "setup_hash", "strategy_hash", "model_hash", "scope_hash", "scope_version"}:
                member[field] = _shadow_public_text(value)
        member_id = member.get("shadow_member_id")
        state_member = state_members.get(member_id) if member_id else None
        if not isinstance(state_member, Mapping) and member_id:
            state_member = state_members.get(str(member_id))
        state_member = state_member if isinstance(state_member, Mapping) else {}
        accounting = _shadow_public_accounting(state_member.get("accounting"))
        for field in ("signals", "declines", "risk_rejections", "fills", "exits"):
            projected = _shadow_public_number(state_member.get(field), integer=True)
            if projected is not None:
                accounting[field] = projected
        member["accounting"] = accounting
        members.append(member)
    return members


def _shadow_public_blockers(state: Mapping[str, Any]) -> list[str]:
    values: list[Any] = []
    raw = state.get("blockers")
    if isinstance(raw, (list, tuple)):
        values.extend(raw)
    values.append(state.get("last_blocker"))
    result: list[str] = []
    for value in values:
        text = _shadow_public_text(value, limit=256)
        if not text:
            continue
        # Persisted blocker details can originate from provider exceptions.
        # Keep only the stable reason code at the public boundary.
        text = text.split(":", 1)[0].strip()[:128]
        if text and text not in result:
            result.append(text)
    return result[:32]


def _shadow_public_stop_conditions(
    manifest: Mapping[str, Any],
    state: Mapping[str, Any],
) -> dict[str, Any]:
    declared = manifest.get("stop_conditions")
    declared = declared if isinstance(declared, Mapping) else {}
    state_stop = state.get("stop")
    state_stop = state_stop if isinstance(state_stop, Mapping) else {}
    result: dict[str, Any] = {}
    for field in ("max_cycles", "max_observations"):
        value = declared.get(field, state_stop.get(field))
        projected = _shadow_public_number(value, integer=True)
        result[field] = projected
    result["stop_at"] = _shadow_public_timestamp(
        declared.get("stop_at", state_stop.get("stop_at"))
    )
    result["reached"] = state_stop.get("reached") is True
    reason = _shadow_public_text(state_stop.get("reason"))
    result["reason"] = reason
    return result


def _shadow_public_progress(
    state: Mapping[str, Any],
    stop_conditions: Mapping[str, Any],
    status: str,
) -> dict[str, Any]:
    cycles = _shadow_public_number(state.get("cycles"), integer=True) or 0
    observations = _shadow_public_number(state.get("public_observations"), integer=True) or 0
    member_observations = _shadow_public_number(state.get("member_observations"), integer=True) or 0
    cycle_limit = stop_conditions.get("max_cycles")
    observation_limit = stop_conditions.get("max_observations")
    cycle_fraction = (
        min(1.0, cycles / cycle_limit)
        if isinstance(cycle_limit, int) and cycle_limit > 0
        else None
    )
    observation_fraction = (
        min(1.0, observations / observation_limit)
        if isinstance(observation_limit, int) and observation_limit > 0
        else None
    )
    fractions = [value for value in (cycle_fraction, observation_fraction) if value is not None]
    fraction = max(fractions) if fractions else (1.0 if status in {"COMPLETED", "STOPPED"} else None)
    return {
        "cycles": cycles,
        "public_observations": observations,
        "member_observations": member_observations,
        "cycle_limit": cycle_limit,
        "observation_limit": observation_limit,
        "cycle_fraction": cycle_fraction,
        "observation_fraction": observation_fraction,
        "fraction": fraction,
    }


def _shadow_next_action(status: str, blockers: Sequence[str]) -> str:
    if status == "REGISTERED":
        return "WAIT_FOR_SHADOW_WORKER"
    if status == "RUNNING":
        return "WAIT_FOR_NEXT_EVALUATION"
    if status == "WAITING_FOR_DATA":
        return "WAIT_FOR_DATA"
    if status == "BLOCKED":
        return "REVIEW_BLOCKER" if blockers else "REVIEW_SHADOW_JOB"
    if status == "COMPLETED":
        return "NO_ACTION_COMPLETED"
    if status == "STOPPED":
        return "NO_ACTION_STOPPED"
    return "REVIEW_SHADOW_JOB"


def _shadow_public_job(row: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(row, Mapping):
        return None
    manifest = row.get("manifest")
    manifest = manifest if isinstance(manifest, Mapping) else {}
    state = row.get("state")
    state = state if isinstance(state, Mapping) else {}
    status = str(row.get("status") or "UNKNOWN").strip().upper()
    if status not in _SHADOW_STATUSES:
        status = "UNKNOWN"
    stop_conditions = _shadow_public_stop_conditions(manifest, state)
    blockers = _shadow_public_blockers(state)
    next_evaluation_at = _shadow_public_timestamp(
        row.get("next_evaluation_at") or state.get("next_evaluation_at")
    )
    members = _shadow_public_members(manifest, state)
    shared = manifest.get("shared")
    shared = shared if isinstance(shared, Mapping) else {}
    progress = _shadow_public_progress(state, stop_conditions, status)
    last_cycle = state.get("last_cycle")
    last_cycle = last_cycle if isinstance(last_cycle, Mapping) else {}
    cycle_summary = {
        field: _shadow_public_number(last_cycle.get(field), integer=field in {
            "observations_processed", "events_written", "fills",
        })
        for field in ("observations_processed", "events_written", "fills")
        if _shadow_public_number(last_cycle.get(field), integer=field in {
            "observations_processed", "events_written", "fills",
        }) is not None
    }
    next_action = _shadow_next_action(status, blockers)
    return {
        "job_id": _shadow_public_text(row.get("job_id")),
        "status": status,
        "schema": _shadow_public_text(manifest.get("schema") or state.get("schema")),
        "paper_only": True,
        "live_execution": False,
        "created_at": _shadow_public_timestamp(row.get("created_at")),
        "updated_at": _shadow_public_timestamp(row.get("updated_at")),
        "next_evaluation_at": next_evaluation_at,
        "next_evaluation": next_evaluation_at,
        "version": _shadow_public_number(row.get("version"), integer=True),
        "members": members,
        "shared": {
            "run_id": _shadow_public_text(shared.get("run_id") or state.get("run_id")),
            "scope_hash": _shadow_public_text(shared.get("scope_hash")),
            "scope_version": _shadow_public_text(shared.get("scope_version")),
        },
        "shared_budget": _shadow_public_budget(shared, state),
        "accounting": {
            "members": [
                {
                    "shadow_member_id": member.get("shadow_member_id"),
                    **member.get("accounting", {}),
                }
                for member in members
            ],
        },
        "progress": progress,
        "progress_fraction": progress.get("fraction"),
        "stop_conditions": stop_conditions,
        "blockers": blockers,
        "last_blocker": blockers[-1] if blockers else None,
        "last_cycle": cycle_summary or None,
        "next_action": next_action,
        "read_only": True,
    }
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
_OPERATOR_COVERAGE_COMPAT_KEYS = (
    "historical_count",
    "historical_datasets",
    "historical_rows",
    "forward_count",
    "forward_datasets",
    "forward_rows",
    "logical_rows",
)


def _merge_operator_coverage(
    persisted: Mapping[str, Any] | None,
    controls: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Join qualification and dataset coverage without dropping either."""
    durable = dict(persisted) if isinstance(persisted, Mapping) else {}
    live = dict(controls) if isinstance(controls, Mapping) else {}
    merged = {**durable, **live}
    for key in _OPERATOR_COVERAGE_COMPAT_KEYS:
        if key in durable and not _display_value_missing(durable[key]):
            merged[key] = durable[key]
    for key, aliases in (
        ("historical_count", ("historical_datasets",)),
        ("forward_count", ("forward_datasets",)),
    ):
        if _display_value_missing(merged.get(key)):
            for alias in aliases:
                if not _display_value_missing(merged.get(alias)):
                    merged[key] = merged[alias]
                    break
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

def _scan_count_is_zero(value: Any) -> bool:
    if value is None or isinstance(value, bool):
        return False
    try:
        return Decimal(str(value)) == Decimal("0")
    except (TypeError, ValueError, ArithmeticError):
        return False


def _clear_stale_actionable_projection(projection: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(projection)
    if _scan_count_is_zero(result.get("actionable_candidates_found")):
        result["selected_actionable_candidate"] = None
        result["selected_actionable_rank"] = None
        result["selected_actionable_score"] = None
    return result



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
    return _clear_stale_actionable_projection(projection)


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
                tracked = self.store.tracked_polymarket_markets(active_only=True, include_payload=False, limit=1000)
                active_ids = {
                    str(item.get("market_id") if isinstance(item, Mapping) else item)
                    for item in tracked
                    if (
                        isinstance(item, Mapping)
                        and item.get("market_id")
                    )
                    or (not isinstance(item, Mapping) and str(item).strip())
                }
                latest_loader = getattr(self.store, "load_latest_polymarket_snapshots_dashboard", None)
                if callable(latest_loader):
                    snapshots = latest_loader(active_ids, limit=1000)
                else:
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
                market_id = item.get("market_id")
                if market_id not in (None, ""):
                    item["record_kind"] = "market"
                    item["record_id"] = market_id
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
                market_id = item.get("market_id")
                if market_id not in (None, ""):
                    item["record_kind"] = "market"
                    item["record_id"] = market_id
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
        dashboard_loader = (
            getattr(self.store, "load_dataset_catalog_dashboard", None)
            if self.store is not None
            else None
        )
        legacy_loader = (
            getattr(self.store, "load_dataset_catalog", None)
            if self.store is not None
            else None
        )
        if callable(dashboard_loader):
            record = dashboard_loader(identifier)
        elif callable(legacy_loader):
            record = legacy_loader(identifier)
        if record is None:
            catalogs = self._configured("datasets")
            if isinstance(catalogs, Mapping):
                for item in catalogs.get("historical", []) + catalogs.get("forward", []):
                    if isinstance(item, Mapping) and str(item.get("dataset_id")) == identifier:
                        record = item
                        break
        if record is None:
            return {"available": False, "dataset_id": identifier, "error": "dataset not found", "live_execution": False}
        return {
            "available": True,
            "dataset_id": identifier,
            "dataset_version": record.get("dataset_version"),
            "catalog": record,
            "health": None,
            "live_execution": False,
        }

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

    def rolling_portfolio_data(self) -> dict[str, Any]:
        """Project bounded rolling controller/evidence/selection state."""
        empty = {
            "status": "UNKNOWN",
            "controller_status": "UNKNOWN",
            "k": 0,
            "actual": 0,
            "actual_k": 0,
            "actionable": 0,
            "policy": {},
            "active_policy": {},
            "reviewed_policy": {},
            "proposed_policy": {},
            "policy_review": {
                "status": "NOT_REVIEWED",
                "active": {},
                "proposed": {},
                "caps": {},
                "allocation": {},
            },
            "allocation_review": {},
            "risk": {},
            "evidence": {
                "status": "UNKNOWN",
                "requested_windows": 0,
                "available_windows": 0,
                "admitted_windows": 0,
                "measured": {},
                "monetary": {},
                "reasons": [],
                "attribution": [],
                "cursor": {},
                "blockers": [],
            },
            "selection": {"status": "UNKNOWN", "k": 0, "actual_k": 0},
            "signal": {"status": "UNKNOWN"},
            "execution": {"status": "UNKNOWN"},
            "active_rows": [],
            "global_limits": {},
            "global_limits_usage": {},
            "rolling_usage": {},
            "canary_usage": {},
            "events": [],
            "event_history": [],
            "reason_history": [],
            "admission_reason_history": [],
            "replacement_reason_history": [],
            "next_jobs": [],
            "cold_start_requirements": [],
            "paper_only": True,
            "live_execution": False,
        }
        if self.store is None:
            return empty

        def load(method_name: str, *args: Any, **kwargs: Any) -> Any:
            method = getattr(self.store, method_name, None)
            if not callable(method):
                return None
            try:
                return method(*args, **kwargs)
            except Exception:
                return None

        selection = load("load_current_portfolio_selection")
        selection = selection if isinstance(selection, Mapping) else {}
        review_state = load("load_portfolio_review_state")
        review_state = review_state if isinstance(review_state, Mapping) else {}
        rolling_worker = load("get_worker_state", "rolling-portfolio")
        rolling_worker = (
            dict(rolling_worker) if isinstance(rolling_worker, Mapping) else {}
        )
        worker_payload = (
            rolling_worker.get("payload")
            if isinstance(rolling_worker.get("payload"), Mapping)
            else {}
        )
        worker_status = str(
            worker_payload.get("worker_status")
            or rolling_worker.get("status")
            or "NOT_INITIALIZED"
        ).upper()
        worker_scheduled = worker_payload.get("scheduled")
        if worker_scheduled is None:
            worker_scheduled = bool(rolling_worker) and worker_status in {
                "SCHEDULED",
                "RUNNING",
                "IDLE",
                "DEGRADED",
            }

        def member_value(member: Mapping[str, Any], payload: Mapping[str, Any], key: str) -> Any:
            value = member.get(key)
            return value if value is not None else payload.get(key)

        def text(value: Any) -> str | None:
            result = str(value).strip() if value is not None else ""
            return result or None

        def plural(value: Any, fallback: Any = None) -> list[str]:
            source = value
            if source is None or source == "":
                source = fallback
            if isinstance(source, (list, tuple, set, frozenset)):
                values = source
            elif source is None or source == "":
                values = ()
            else:
                values = (source,)
            return [str(item).strip()[:512] for item in values if str(item).strip()][:16]

        members = selection.get("members", selection.get("selected_members", []))
        members = members if isinstance(members, (list, tuple)) else []
        evidence_loader = getattr(self.store, "list_strategy_evidence_windows", None)
        all_evidence_rows: list[Mapping[str, Any]] = []
        if callable(evidence_loader):
            try:
                loaded_evidence = evidence_loader(limit=10_000)
            except TypeError:
                try:
                    loaded_evidence = evidence_loader()
                except Exception:
                    loaded_evidence = ()
            except Exception:
                loaded_evidence = ()
            all_evidence_rows = [
                item for item in (loaded_evidence or ())
                if isinstance(item, Mapping)
            ]
        rolling_cursor = load("load_rolling_evidence_cursor")
        rolling_cursor = rolling_cursor if isinstance(rolling_cursor, Mapping) else {}
        rolling_blockers = load("list_rolling_evidence_blockers", limit=32)
        rolling_blockers = [
            item for item in (rolling_blockers or ())
            if isinstance(item, Mapping)
        ]
        rolling_worker_state = (
            worker_payload.get("rolling_portfolio")
            if isinstance(worker_payload.get("rolling_portfolio"), Mapping)
            else {}
        )
        active_rows: list[dict[str, Any]] = []
        evidence_count = 0
        active_member_count = 0

        def recent_chronological(
            values: Sequence[Any],
            *,
            limit: int,
            timestamp_keys: tuple[str, ...],
        ) -> list[Mapping[str, Any]]:
            source_values: Sequence[Any] = values
            if len(values) > 128:
                def boundary_stamp(item: Any) -> Any:
                    if not isinstance(item, Mapping):
                        return None
                    for key in timestamp_keys:
                        stamp = parse_timestamp(item.get(key))
                        if stamp is not None:
                            return stamp
                    return None

                first_stamp = boundary_stamp(values[0])
                last_stamp = boundary_stamp(values[-1])
                source_values = (
                    values[:128]
                    if first_stamp is not None
                    and last_stamp is not None
                    and first_stamp > last_stamp
                    else values[-128:]
                )
            rows = [item for item in source_values if isinstance(item, Mapping)]
            stamped: list[tuple[Any, int, Mapping[str, Any]]] = []
            for index, item in enumerate(rows):
                stamp = next(
                    (
                        parse_timestamp(item.get(key))
                        for key in timestamp_keys
                        if parse_timestamp(item.get(key)) is not None
                    ),
                    None,
                )
                if stamp is None:
                    stamped = []
                    break
                stamped.append((stamp, index, item))
            if stamped:
                rows = [
                    item
                    for _, _, item in sorted(
                        stamped,
                        key=lambda value: (value[0], value[1]),
                    )
                ]
            return rows[-max(0, limit):]

        def hydrate_exact_evidence_row(row: Any) -> Mapping[str, Any]:
            """Project a raw SQL row like ``list_strategy_evidence_windows``.

            The exact lookup deliberately bypasses the bounded catalogue
            loader.  Keep its JSON columns on the same parsed/flattened shape
            as storage rows before the identity checks below, while retaining
            a fail-closed marker for malformed documents.
            """
            if isinstance(row, Mapping):
                raw = dict(row)
            else:
                try:
                    keys = row.keys()
                    raw = {key: row[key] for key in keys}
                except Exception:
                    return {}

            json_columns = frozenset(
                {
                    "payload_json",
                    "evaluation_json",
                    "portfolio_accounting_json",
                    "paper_sizing_assumptions_json",
                    "paper_fee_assumptions_json",
                    "paper_slippage_assumptions_json",
                }
            )

            def decode_json(value: Any) -> tuple[Any, bool, bool]:
                if value is None or value == "":
                    return None, True, False
                if isinstance(value, (Mapping, list, tuple, bool, int, float)):
                    return value, True, True
                try:
                    return json.loads(value), True, True
                except (TypeError, ValueError, json.JSONDecodeError):
                    return None, False, True

            payload, payload_valid, payload_present = decode_json(
                raw.get("payload_json")
            )
            item: dict[str, Any] = (
                dict(payload) if payload_valid and isinstance(payload, Mapping) else {}
            )
            for key, value in raw.items():
                if key not in json_columns:
                    item[key] = value
            malformed_json = not payload_valid

            evaluation, evaluation_valid, evaluation_present = decode_json(
                raw.get("evaluation_json")
            )
            if evaluation_present and not evaluation_valid:
                malformed_json = True
                evaluation = {}
            if not isinstance(evaluation, Mapping):
                candidate = item.get("evaluation")
                if not isinstance(candidate, Mapping):
                    metrics = item.get("metrics")
                    candidate = (
                        metrics.get("evaluation")
                        if isinstance(metrics, Mapping)
                        else {}
                    )
                evaluation = candidate if isinstance(candidate, Mapping) else {}
            if evaluation_present or isinstance(evaluation, Mapping) and evaluation:
                item["evaluation"] = dict(evaluation)

            portfolio, portfolio_valid, portfolio_present = decode_json(
                raw.get("portfolio_accounting_json")
            )
            if portfolio_present and not portfolio_valid:
                malformed_json = True
                portfolio = {}
            if not isinstance(portfolio, Mapping):
                candidate = item.get("portfolio_accounting")
                if not isinstance(candidate, Mapping):
                    metrics = item.get("metrics")
                    candidate = (
                        metrics.get("portfolio_accounting")
                        if isinstance(metrics, Mapping)
                        else {}
                    )
                portfolio = candidate if isinstance(candidate, Mapping) else {}
            if portfolio_present or isinstance(portfolio, Mapping) and portfolio:
                item["portfolio_accounting"] = dict(portfolio)

            for column_name, field_name in (
                ("paper_sizing_assumptions_json", "paper_sizing_assumptions"),
                ("paper_fee_assumptions_json", "paper_fee_assumptions"),
                ("paper_slippage_assumptions_json", "paper_slippage_assumptions"),
            ):
                decoded, valid, present = decode_json(raw.get(column_name))
                if not valid:
                    malformed_json = True
                elif present:
                    item[field_name] = decoded

            evaluation_field_names = (
                "evaluation_run_id",
                "evaluation_version",
                "supersedes_evidence_id",
                "loaded_rows",
                "valid_input_rows",
                "evaluator_invoked",
                "evaluator_completed",
                "evaluated_observations",
                "signal_count",
                "diagnostic_summary_count",
                "evaluator_name",
                "evaluator_error",
                "evaluator_prerequisite",
            )
            evaluation_count_fields = frozenset(
                {
                    "loaded_rows",
                    "valid_input_rows",
                    "evaluated_observations",
                    "signal_count",
                    "diagnostic_summary_count",
                }
            )
            for field_name in evaluation_field_names:
                column_value = raw.get(field_name)
                if column_value is None:
                    continue
                try:
                    normalized = (
                        bool(column_value)
                        if field_name in {"evaluator_invoked", "evaluator_completed"}
                        else int(column_value)
                        if field_name in evaluation_count_fields
                        else column_value
                    )
                except (TypeError, ValueError, OverflowError):
                    malformed_json = True
                    continue
                evaluation[field_name] = normalized
                item[field_name] = normalized
            if evaluation:
                item["evaluation"] = dict(evaluation)

            accounting_available = raw.get("accounting_available")
            if accounting_available is not None:
                try:
                    accounting_available = bool(accounting_available)
                except (TypeError, ValueError):
                    malformed_json = True
                else:
                    item["accounting_available"] = accounting_available
                    portfolio["accounting_available"] = accounting_available

            for field_name in ("accounting_complete", "accounting_partial"):
                value = item.get(field_name)
                if value is None:
                    value = portfolio.get(field_name)
                if value is None:
                    continue
                if isinstance(value, bool):
                    normalized = value
                elif isinstance(value, int) and value in (0, 1):
                    normalized = bool(value)
                elif isinstance(value, str) and value.strip().upper() in {
                    "TRUE",
                    "1",
                    "YES",
                }:
                    normalized = True
                elif isinstance(value, str) and value.strip().upper() in {
                    "FALSE",
                    "0",
                    "NO",
                }:
                    normalized = False
                else:
                    malformed_json = True
                    continue
                item[field_name] = normalized
                portfolio[field_name] = normalized

            for field_name in (
                "initial_cash",
                "cash",
                "equity",
                "realized_pnl",
                "unrealized_pnl",
                "net_pnl",
                "fees",
                "costs",
                "open_positions",
                "opening_fills",
                "closing_fills",
                "partial_closing_fills",
                "completed_round_trips",
            ):
                if field_name in portfolio:
                    item[field_name] = portfolio[field_name]
            if portfolio_present or portfolio:
                item["portfolio_accounting"] = dict(portfolio)
            if malformed_json:
                item["_evidence_json_invalid"] = True
            return item


        def exact_evidence(
            strategy_id: str | None,
            evidence_window_id: str | None,
        ) -> Mapping[str, Any]:
            """Load one immutable window by its id, never by newest-window order."""
            if not strategy_id or not evidence_window_id:
                return {}
            for accessor_name in (
                "get_strategy_evidence_window",
                "load_strategy_evidence_window",
                "get_evidence_window",
                "load_evidence_window",
            ):
                accessor = getattr(self.store, accessor_name, None)
                if not callable(accessor):
                    continue
                for args, kwargs in (
                    ((strategy_id, evidence_window_id), {}),
                    ((evidence_window_id,), {}),
                    ((), {"strategy_version_id": strategy_id, "evidence_window_id": evidence_window_id}),
                    ((), {"evidence_window_id": evidence_window_id}),
                ):
                    try:
                        result = accessor(*args, **kwargs)
                    except TypeError:
                        continue
                    except Exception:
                        result = None
                    if (
                        isinstance(result, Mapping)
                        and text(result.get("evidence_window_id")) == evidence_window_id
                    ):
                        return result
                    if isinstance(result, (list, tuple)):
                        for item in result:
                            if (
                                isinstance(item, Mapping)
                                and text(item.get("evidence_window_id")) == evidence_window_id
                            ):
                                return item
            connection = getattr(self.store, "connection", None)
            execute = getattr(connection, "execute", None)
            if callable(execute):
                try:
                    row = execute(
                        "SELECT * FROM strategy_evidence_windows "
                        "WHERE evidence_window_id=? LIMIT 1",
                        (evidence_window_id,),
                    ).fetchone()
                except Exception:
                    row = None
                if row is not None:
                    return hydrate_exact_evidence_row(row)
            if callable(evidence_loader):
                try:
                    rows = evidence_loader(strategy_id, limit=4096)
                except TypeError:
                    try:
                        rows = evidence_loader(
                            strategy_version_id=strategy_id,
                            limit=4096,
                        )
                    except Exception:
                        rows = []
                except Exception:
                    rows = []
                if isinstance(rows, (list, tuple)):
                    return next(
                        (
                            item
                            for item in rows
                            if isinstance(item, Mapping)
                            and text(item.get("evidence_window_id")) == evidence_window_id
                        ),
                        {},
                    )
            return {}
        def evidence_field(row: Mapping[str, Any], key: str, default: Any = None) -> Any:
            value = row.get(key)
            if value is not None:
                return value
            payload = row.get("payload")
            if isinstance(payload, Mapping) and payload.get(key) is not None:
                return payload.get(key)
            return default
        evaluation_field_names = (
            "evaluation_run_id",
            "evaluation_version",
            "supersedes_evidence_id",
            "loaded_rows",
            "valid_input_rows",
            "evaluator_invoked",
            "evaluator_completed",
            "evaluated_observations",
            "signal_count",
            "diagnostic_summary_count",
            "evaluator_name",
            "evaluator_error",
            "evaluator_prerequisite",
        )
        accounting_field_names = (
            "accounting_available",
            "initial_cash",
            "cash",
            "equity",
            "realized_pnl",
            "unrealized_pnl",
            "net_pnl",
            "fees",
            "costs",
            "allocated_capital",
            "capital_at_risk",
            "open_positions",
            "opening_fills",
            "closing_fills",
            "partial_closing_fills",
            "fills",
            "completed_round_trips",
        )
        monetary_field_names = (
            "initial_cash",
            "cash",
            "equity",
            "realized_pnl",
            "unrealized_pnl",
            "net_pnl",
            "fees",
            "costs",
            "allocated_capital",
            "capital_at_risk",
        )

        def evaluation_kind_projection(
            row: Mapping[str, Any],
            payload: Mapping[str, Any],
        ) -> tuple[str | None, bool, bool]:
            """Project the explicit v2 evaluator/accounting provenance.

            ``ACTUAL_LEDGER`` is intentionally never inferred from accounting
            values.  A v2 row without an explicit kind remains canonical for
            legacy non-PAPER sources; PAPER rows remain unknown and fail closed.
            """
            metrics = row.get("metrics")
            metrics = metrics if isinstance(metrics, Mapping) else {}
            nested = row.get("evaluation")
            nested = nested if isinstance(nested, Mapping) else {}
            metrics_evaluation = metrics.get("evaluation")
            metrics_evaluation = (
                metrics_evaluation
                if isinstance(metrics_evaluation, Mapping)
                else {}
            )
            sources = (row, payload, nested, metrics_evaluation)
            explicit: list[str] = []
            invalid = False
            for source in sources:
                if "evaluation_kind" not in source:
                    continue
                value = str(source.get("evaluation_kind") or "").strip().upper()
                value = value.replace("-", "_").replace(" ", "_")
                if value in {"CANONICAL_SIMULATION", "ACTUAL_LEDGER"}:
                    explicit.append(value)
                else:
                    invalid = True
            # Match storage/domain's v2 provenance discriminator exactly.
            # Accounting/status fields may be present on legacy rows and
            # must not upgrade them into canonical v2 evidence.
            v2_fields = (
                "evaluation_kind",
                "evaluation_run_id",
                "evaluation_version",
                "supersedes_evidence_id",
            )
            is_v2 = any(
                any(field_name in source for field_name in v2_fields)
                for source in sources
            )
            kind_values = set(explicit)
            conflict = invalid or len(kind_values) > 1
            if conflict:
                return None, True, is_v2
            if explicit:
                return explicit[0], False, is_v2
            source_value = next(
                (
                    source.get("source_class")
                    for source in sources
                    if source.get("source_class") not in (None, "")
                ),
                None,
            )
            try:
                normalized_source = _source_class(source_value, required=False)
            except (TypeError, ValueError):
                normalized_source = str(source_value or "").strip().upper()
            if is_v2 and normalized_source != "PAPER":
                return "CANONICAL_SIMULATION", False, True
            return None, False, is_v2

        def evaluation_projection(row: Mapping[str, Any]) -> dict[str, Any]:
            payload = row.get("payload")
            payload = payload if isinstance(payload, Mapping) else {}
            nested = row.get("evaluation")
            if not isinstance(nested, Mapping):
                metrics = row.get("metrics")
                nested = metrics.get("evaluation") if isinstance(metrics, Mapping) else {}
            result: dict[str, Any] = dict(nested) if isinstance(nested, Mapping) else {}
            for field_name in evaluation_field_names:
                value = row.get(field_name)
                if value is None:
                    value = payload.get(field_name)
                if value is not None:
                    result[field_name] = value
            evaluation_kind, kind_conflict, is_v2 = evaluation_kind_projection(
                row,
                payload,
            )
            result["evaluation_kind"] = evaluation_kind
            result["evaluation_kind_conflict"] = kind_conflict
            result["evaluation_v2"] = is_v2
            for field_name in ("evaluator_error", "evaluator_prerequisite"):
                if field_name in result:
                    detail = text(result[field_name])
                    result[field_name] = (
                        detail[:_ROLLING_DIAGNOSTIC_STRING_LENGTH]
                        if detail
                        else None
                    )
            return result

        def _accounting_projection_key(value: Any) -> Any:
            """Build a deterministic, type-aware key for duplicate fields."""
            if value is None:
                return ("none",)
            if isinstance(value, bool):
                return ("bool", value)
            if isinstance(value, Mapping):
                return (
                    "mapping",
                    tuple(
                        sorted(
                            [
                                (str(key), _accounting_projection_key(child))
                                for key, child in value.items()
                            ],
                            key=lambda item: item[0],
                        )
                    ),
                )
            if isinstance(value, (set, frozenset)):
                return (
                    "set",
                    tuple(
                        sorted(
                            (_accounting_projection_key(child) for child in value),
                            key=repr,
                        )
                    ),
                )
            if isinstance(value, (list, tuple)):
                return (
                    "sequence",
                    tuple(_accounting_projection_key(child) for child in value),
                )
            if isinstance(value, Decimal):
                return ("decimal", str(value))
            return (type(value).__module__, type(value).__qualname__, value)

        def _accounting_projection_value(value: Any) -> Any:
            """Canonicalize set-like nested values before public bounding."""
            if isinstance(value, Mapping):
                return {
                    str(key): _accounting_projection_value(child)
                    for key, child in value.items()
                }
            if isinstance(value, (set, frozenset)):
                values = [_accounting_projection_value(child) for child in value]
                return sorted(values, key=lambda child: repr(_accounting_projection_key(child)))
            if isinstance(value, tuple):
                return [_accounting_projection_value(child) for child in value]
            if isinstance(value, list):
                return [_accounting_projection_value(child) for child in value]
            return value

        def accounting_projection(row: Mapping[str, Any]) -> dict[str, Any]:
            payload = row.get("payload")
            payload = payload if isinstance(payload, Mapping) else {}
            metrics = row.get("metrics")
            nested_sources = tuple(
                (source_name, source)
                for source_name, source in (
                    ("row.portfolio_accounting", row.get("portfolio_accounting")),
                    (
                        "metrics.portfolio_accounting",
                        metrics.get("portfolio_accounting")
                        if isinstance(metrics, Mapping)
                        else None,
                    ),
                    ("payload.portfolio_accounting", payload.get("portfolio_accounting")),
                )
                if isinstance(source, Mapping)
            )
            result: dict[str, Any] = {}
            conflicting_fields: set[str] = set()
            field_names = {
                key
                for _source_name, source in nested_sources
                for key in source
            }
            for field_name in sorted(field_names, key=str):
                projections = [
                    (source_name, source[field_name])
                    for source_name, source in nested_sources
                    if field_name in source
                ]
                first_value = projections[0][1]
                first_key = _accounting_projection_key(first_value)
                if any(
                    _accounting_projection_key(value) != first_key
                    for _source_name, value in projections[1:]
                ):
                    conflicting_fields.add(field_name)
                    result[field_name] = None
                else:
                    result[field_name] = _accounting_projection_value(first_value)
            for field_name in accounting_field_names:
                if field_name in result:
                    continue
                value = row.get(field_name)
                if value is None:
                    value = payload.get(field_name)
                if value is not None:
                    result[field_name] = value
            if conflicting_fields:
                result["accounting_projection_mismatch"] = True
                result["accounting_projection_conflicts"] = sorted(
                    conflicting_fields,
                    key=str,
                )[:32]
                result["accounting_available"] = False
                result["accounting_complete"] = False
                result["accounting_partial"] = True
            elif "accounting_available" not in result:
                result["accounting_available"] = None
            availability = result.get("accounting_available")
            if availability is not True:
                for field_name in monetary_field_names:
                    result[field_name] = None
            return result

        def evidence_number(row: Mapping[str, Any], *keys: str) -> Decimal:
            for key in keys:
                value = evidence_field(row, key)
                if value in (None, ""):
                    continue
                try:
                    parsed = Decimal(str(value))
                except (TypeError, ValueError, ArithmeticError):
                    continue
                if parsed.is_finite():
                    return parsed
            return Decimal("0")

        def optional_count(value: Any) -> int | None:
            if value in (None, "") or isinstance(value, bool):
                return None
            if isinstance(value, (Mapping, list, tuple, set, frozenset)):
                return len(value)
            try:
                parsed = Decimal(str(value))
            except (TypeError, ValueError, ArithmeticError):
                return None
            if (
                not parsed.is_finite()
                or parsed < 0
                or parsed != parsed.to_integral_value()
            ):
                return None
            return int(parsed)

        def bounded_diagnostic(value: Any) -> str | None:
            detail = text(value)
            return (
                detail[:_ROLLING_DIAGNOSTIC_STRING_LENGTH]
                if detail
                else None
            )

        projected_evidence = [
            (row, evaluation_projection(row), accounting_projection(row))
            for row in all_evidence_rows
        ]


        def aggregate_count(
            field_name: str,
            *,
            legacy_names: tuple[str, ...] = (),
        ) -> int | None:
            values: list[int] = []
            for row, evaluation, accounting in projected_evidence:
                value = accounting.get(field_name)
                if value is None:
                    value = evaluation.get(field_name)
                if value is None:
                    value = next(
                        (
                            evidence_field(row, legacy_name)
                            for legacy_name in legacy_names
                            if evidence_field(row, legacy_name) is not None
                        ),
                        None,
                    )
                parsed = optional_count(value)
                if parsed is not None:
                    values.append(parsed)
            return sum(values) if values else None

        opening_fills = aggregate_count(
            "opening_fills",
            legacy_names=("openings",),
        )
        closing_fills = aggregate_count(
            "closing_fills",
            legacy_names=("closings", "exits"),
        )
        partial_closing_fills = aggregate_count(
            "partial_closing_fills",
            legacy_names=("partial_closings", "partial_exits"),
        )
        canonical_fill_counts = (
            opening_fills,
            closing_fills,
            partial_closing_fills,
        )
        measured = {
            "loaded_rows": aggregate_count("loaded_rows", legacy_names=("requested_rows",)),
            "valid_input_rows": aggregate_count(
                "valid_input_rows",
                legacy_names=("available_rows",),
            ),
            "evaluated_observations": aggregate_count(
                "evaluated_observations",
                legacy_names=("evaluated_rows",),
            ),
            "signal_count": aggregate_count("signal_count", legacy_names=("signals",)),
            "diagnostic_summary_count": aggregate_count("diagnostic_summary_count"),
            "fills": (
                sum(value or 0 for value in canonical_fill_counts)
                if any(value is not None for value in canonical_fill_counts)
                else aggregate_count("fills", legacy_names=("fills",))
            ),
            "opening_fills": opening_fills,
            "closing_fills": closing_fills,
            "partial_closing_fills": partial_closing_fills,
            "completed_round_trips": aggregate_count(
                "completed_round_trips",
                legacy_names=("completed_outcomes",),
            ),
            "positions": aggregate_count("positions"),
        }
        measured["exits"] = sum(
            value or 0
            for value in (
                measured.get("closing_fills"),
                measured.get("partial_closing_fills"),
            )
        )
        evaluator_invoked = [
            evaluation.get("evaluator_invoked")
            for _row, evaluation, _accounting in projected_evidence
            if evaluation.get("evaluator_invoked") is not None
        ]
        evaluator_completed = [
            evaluation.get("evaluator_completed")
            for _row, evaluation, _accounting in projected_evidence
            if evaluation.get("evaluator_completed") is not None
        ]
        evaluation_kinds = [
            evaluation.get("evaluation_kind")
            for _row, evaluation, _accounting in projected_evidence
            if evaluation.get("evaluation_kind") is not None
        ]
        accounting_states = [
            accounting.get("accounting_available")
            for _row, _evaluation, accounting in projected_evidence
        ]
        monetary_fields = monetary_field_names
        monetary: dict[str, Any] = {}
        if projected_evidence and all(value is True for value in accounting_states):
            for field_name in monetary_fields:
                values = [
                    Decimal(str(accounting[field_name]))
                    for _row, _evaluation, accounting in projected_evidence
                    if accounting.get(field_name) not in (None, "")
                ]
                monetary[field_name] = sum(values, Decimal("0")) if len(values) == len(projected_evidence) else None
        else:
            monetary = {field_name: None for field_name in monetary_fields}
        evidence_reasons: list[str] = []
        for row, evaluation, accounting in projected_evidence:
            reasons = plural(
                evidence_field(row, "admission_reasons"),
                evidence_field(row, "reasons", evidence_field(row, "reason")),
            )
            unavailable = bounded_diagnostic(
                evidence_field(row, "accounting_unavailable_reason")
            )
            if unavailable:
                reasons.append(unavailable)
            for field_name in ("evaluator_error", "evaluator_prerequisite"):
                detail = bounded_diagnostic(evaluation.get(field_name))
                if detail:
                    reasons.append(detail)
            if accounting.get("accounting_available") is False:
                reasons.append("ACCOUNTING_UNAVAILABLE")
            for reason in reasons:
                if reason not in evidence_reasons:
                    evidence_reasons.append(reason)
        evidence_attribution: list[dict[str, Any]] = []
        for row in recent_chronological(
            all_evidence_rows,
            limit=64,
            timestamp_keys=("available_through", "available_from", "created_at"),
        ):
            evaluation = evaluation_projection(row)
            accounting = accounting_projection(row)
            attribution = {
                key: evidence_field(row, key)
                for key in (
                    "evidence_window_id",
                    "strategy_version_id",
                    "research_trial_id",
                    "candidate_id",
                    "requested_days",
                    "source_class",
                    "evaluation_kind",
                    "evidence_digest",
                    "source_digest",
                    "accounting_digest",
                    "admitted",
                    "admission_reasons",
                    "positions",
                    "open_positions",
                    "openings",
                    "completed_outcomes",
                )
                if evidence_field(row, key) is not None
            }
            attribution["evaluation"] = evaluation
            attribution["evaluation_kind"] = evaluation.get("evaluation_kind")
            attribution["portfolio_accounting"] = accounting
            attribution["accounting_available"] = accounting.get("accounting_available")
            evidence_attribution.append(attribution)
        rolling_evidence_summary = {
            # A historical/partial admitted row is informative, but cannot
            # make the portfolio ready before every active member's exact
            # evidence window has been verified below.
            "status": "PARTIAL" if all_evidence_rows else "UNKNOWN",
            "requested_windows": int(
                rolling_worker_state.get("requested_work_items", len(all_evidence_rows))
                or len(all_evidence_rows)
            ),
            "available_windows": len(all_evidence_rows),
            "admitted_windows": sum(
                1 for row in all_evidence_rows if evidence_field(row, "admitted") is True
            ),
            "requested_days": [
                evidence_field(row, "requested_days")
                for row in all_evidence_rows
                if evidence_field(row, "requested_days") is not None
            ][:128],
            "available_coverage_seconds": sum(
                int(evidence_number(row, "actual_coverage_seconds"))
                for row in all_evidence_rows
            ),
            "measured": measured,
            "monetary": monetary,
            "reasons": evidence_reasons[:64],
            "attribution": evidence_attribution,
            "cursor": dict(rolling_cursor),
            "blockers": rolling_blockers[:32],
            "evaluation": {
                "loaded_rows": measured.get("loaded_rows"),
                "valid_input_rows": measured.get("valid_input_rows"),
                "evaluator_invoked": evaluator_invoked,
                "evaluator_completed": evaluator_completed,
                "evaluation_kinds": evaluation_kinds,
                "evaluated_observations": measured.get("evaluated_observations"),
                "signal_count": measured.get("signal_count"),
                "diagnostic_summary_count": measured.get("diagnostic_summary_count"),
                "errors": [
                    bounded_diagnostic(evaluation.get("evaluator_error"))
                    for _row, evaluation, _accounting in projected_evidence
                    if bounded_diagnostic(evaluation.get("evaluator_error"))
                ][:_ROLLING_DIAGNOSTIC_LIMIT],
                "prerequisites": [
                    bounded_diagnostic(evaluation.get("evaluator_prerequisite"))
                    for _row, evaluation, _accounting in projected_evidence
                    if bounded_diagnostic(evaluation.get("evaluator_prerequisite"))
                ][:_ROLLING_DIAGNOSTIC_LIMIT],
            },
            "accounting": {
                "availability": accounting_states,
                "available": (
                    True
                    if accounting_states and all(value is True for value in accounting_states)
                    else False
                    if accounting_states and all(value is False for value in accounting_states)
                    else None
                ),
                "fills": measured.get("fills"),
                "exits": measured.get("exits"),
                "completed_round_trips": measured.get("completed_round_trips"),
                "monetary": monetary,
            },
        }

        def evidence_payload(
            row: Mapping[str, Any],
        ) -> tuple[Mapping[str, Any], str | None]:
            if row.get("_evidence_json_invalid") is True:
                return {}, "evidence_payload_invalid"
            payload = row.get("payload")
            if isinstance(payload, Mapping):
                return payload, None
            raw_payload = row.get("payload_json")
            if raw_payload in (None, ""):
                return {}, None
            try:
                decoded = json.loads(str(raw_payload))
            except (TypeError, ValueError, json.JSONDecodeError):
                return {}, "evidence_payload_invalid"
            return (
                decoded if isinstance(decoded, Mapping) else {},
                None,
            )

        def canonical_source(value: Any) -> str | None:
            raw = text(value)
            if not raw:
                return None
            try:
                return _source_class(raw, required=False)
            except (TypeError, ValueError):
                return None

        for member in members[:10]:
            if not isinstance(member, Mapping):
                continue
            if (
                str(member.get("status") or "").upper() != "ACTIVE"
                or _number_or_zero(member.get("allocation")) <= 0
            ):
                continue
            active_member_count += 1

            payload = member.get("payload")
            payload = payload if isinstance(payload, Mapping) else {}
            strategy_id = text(member_value(member, payload, "strategy_version_id"))
            research_trial_id = text(member_value(member, payload, "research_trial_id"))
            candidate_id = text(member_value(member, payload, "candidate_id"))
            evidence_window_id = text(member_value(member, payload, "evidence_window_id"))
            member_digest = text(member_value(member, payload, "evidence_digest"))
            # Keep the member payload digest as the lineage anchor; a persisted
            # row digest must match it rather than silently replacing it.
            member_source_class = text(member_value(member, payload, "source_class"))
            normalized_member_source = canonical_source(member_source_class)
            evidence = exact_evidence(strategy_id, evidence_window_id)
            persisted_window_id = text(evidence.get("evidence_window_id"))
            persisted_payload, payload_error = evidence_payload(evidence)
            persisted_strategy = text(evidence.get("strategy_version_id"))
            persisted_trial = text(
                evidence.get("research_trial_id")
                or persisted_payload.get("research_trial_id")
                or persisted_payload.get("trial_id")
            )
            persisted_candidate = text(
                evidence.get("candidate_id")
                or persisted_payload.get("candidate_id")
            )
            persisted_digest = text(
                evidence.get("evidence_digest")
                or persisted_payload.get("evidence_digest")
            )
            persisted_source_class = text(
                evidence.get("source_class")
                or persisted_payload.get("source_class")
            )
            normalized_persisted_source = canonical_source(persisted_source_class)
            def evidence_field(name: str) -> Any:
                for source in (evidence, persisted_payload, payload, member):
                    if isinstance(source, Mapping) and name in source:
                        return source.get(name)
                return None

            row_evaluation = evaluation_projection(evidence)
            row_accounting = accounting_projection(evidence)
            accounting_flags = {
                "accounting_available": row_accounting.get(
                    "accounting_available",
                    evidence_field("accounting_available"),
                ),
                "accounting_complete": evidence_field("accounting_complete"),
                "accounting_partial": evidence_field("accounting_partial"),
                "admitted": evidence_field("admitted"),
            }
            reasons = plural(
                member_value(member, payload, "reasons"),
                member_value(member, payload, "reason"),
            )
            admission_reasons = plural(
                member_value(member, payload, "admission_reasons"),
                reasons,
            )
            replacement_reasons = plural(
                member_value(member, payload, "replacement_reasons"),
                member_value(member, payload, "replacement_reason"),
            )
            blockers = [
                key
                for key, value in (
                    ("strategy_version_id", strategy_id),
                    ("research_trial_id", research_trial_id),
                    ("candidate_id", candidate_id),
                    ("evidence_window_id", evidence_window_id),
                    ("evidence_digest", member_digest),
                )
                if not value
            ]
            if member_source_class and normalized_member_source is None:
                blockers.append("source_class_unsupported")
            if not evidence:
                blockers.append(
                    "evidence_window_not_found"
                    if evidence_window_id
                    else "evidence_window_id_required"
                )
            else:
                if not persisted_window_id:
                    blockers.append("evidence_window_id_missing")
                elif persisted_window_id != evidence_window_id:
                    blockers.append("evidence_window_id_mismatch")
                if payload_error:
                    blockers.append(payload_error)
                if not persisted_strategy:
                    blockers.append("evidence_strategy_version_id_missing")
                elif strategy_id and persisted_strategy != strategy_id:
                    blockers.append("evidence_strategy_version_id_mismatch")
                if not persisted_trial:
                    blockers.append("evidence_research_trial_id_missing")
                elif research_trial_id and persisted_trial != research_trial_id:
                    blockers.append("evidence_research_trial_id_mismatch")
                if not persisted_candidate:
                    blockers.append("evidence_candidate_id_missing")
                elif candidate_id and persisted_candidate != candidate_id:
                    blockers.append("evidence_candidate_id_mismatch")
                if not persisted_digest:
                    blockers.append("evidence_digest_missing")
                elif member_digest and persisted_digest != member_digest:
                    blockers.append("evidence_digest_mismatch")
                if not persisted_source_class:
                    blockers.append("evidence_source_class_missing")
                elif normalized_persisted_source is None:
                    blockers.append("evidence_source_class_unsupported")
                elif (
                    normalized_member_source is not None
                    and normalized_persisted_source != normalized_member_source
                ):
                    blockers.append("evidence_source_class_mismatch")
            if row_accounting.get("accounting_projection_mismatch") is True:
                blockers.append("accounting_projection_mismatch")
            if row_evaluation.get("evaluation_kind_conflict"):
                blockers.append("evaluation_kind_conflict")
            evaluation_kind = row_evaluation.get("evaluation_kind")
            if row_evaluation.get("evaluation_v2") and evaluation_kind is None:
                blockers.append("evaluation_kind_required")
            if evaluation_kind == "CANONICAL_SIMULATION":
                if row_evaluation.get("evaluator_invoked") is not True:
                    blockers.append("evaluator_invoked_required")
                if row_evaluation.get("evaluator_completed") is not True:
                    blockers.append("evaluator_completed_required")
            for field_name, blocker_name in (
                ("evaluator_error", "evaluator_error"),
                ("evaluator_prerequisite", "evaluator_prerequisite"),
            ):
                if text(row_evaluation.get(field_name)):
                    blockers.append(blocker_name)
            for flag_name, expected_value, blocker_name in (
                ("accounting_available", True, "accounting_unavailable"),
                ("accounting_complete", True, "accounting_incomplete"),
                ("accounting_partial", False, "accounting_partial"),
                ("admitted", True, "member_not_admitted"),
            ):
                if accounting_flags[flag_name] is not expected_value:
                    blockers.append(blocker_name)
            evidence_valid = bool(evidence) and not blockers
            if evidence_valid:
                evidence_count += 1
            active_rows.append(
                {
                    "evaluation_run_id": row_evaluation.get("evaluation_run_id"),
                    "evaluation_version": row_evaluation.get("evaluation_version"),
                    "evaluation_kind": row_evaluation.get("evaluation_kind"),
                    "supersedes_evidence_id": row_evaluation.get("supersedes_evidence_id"),
                    "evaluation": row_evaluation,
                    "portfolio_accounting": row_accounting,
                    "evaluation_error": row_evaluation.get("evaluator_error"),
                    "evaluation_prerequisite": row_evaluation.get("evaluator_prerequisite"),
                    "accounting_status": (
                        "AVAILABLE"
                        if accounting_flags["accounting_available"] is True
                        else "UNAVAILABLE"
                        if accounting_flags["accounting_available"] is False
                        else "UNKNOWN"
                    ),
                    "accounting_available": accounting_flags["accounting_available"],
                    "accounting_complete": accounting_flags["accounting_complete"],
                    "accounting_partial": accounting_flags["accounting_partial"],
                    "admitted": accounting_flags["admitted"],
                    "strategy_version_id": strategy_id,
                    "research_trial_id": research_trial_id,
                    "candidate_id": candidate_id,
                    "evidence_window_id": evidence_window_id,
                    "evidence_digest": member_digest or persisted_digest,
                    "status": str(member.get("status") or "ACTIVE").upper(),
                    "allocation": member.get("allocation"),
                    "executable": evidence_valid,
                    "blockers": blockers[:16],
                    "evidence_status": (
                        "AVAILABLE"
                        if evidence_valid
                        else "MISMATCH"
                        if evidence
                        else "MISSING_EXACT_WINDOW"
                    ),
                    "actual_coverage_seconds": evidence.get(
                        "actual_coverage_seconds",
                        member_value(member, payload, "actual_coverage_seconds"),
                    ),
                    "source_class": persisted_source_class or member_source_class,
                    "net_return": (
                        evidence.get(
                            "allocated_capital_net_return",
                            evidence.get(
                                "net_return",
                                member_value(member, payload, "net_return"),
                            ),
                        )
                        if accounting_flags["accounting_available"] is True
                        else None
                    ),
                    "drawdown": (
                        evidence.get(
                            "drawdown",
                            member_value(member, payload, "drawdown"),
                        )
                        if accounting_flags["accounting_available"] is True
                        else None
                    ),
                    "exposure": (
                        member_value(member, payload, "exposure")
                        or member_value(member, payload, "position_management_state")
                        or {}
                    )
                    if accounting_flags["accounting_available"] is True
                    else {},
                    "reason": member_value(member, payload, "reason"),
                    "reasons": reasons,
                    "admission_reasons": admission_reasons,
                    "replacement_reasons": replacement_reasons,
                    "next_review": selection.get(
                        "review_due_at",
                        review_state.get("review_due_at"),
                    ),
                }
            )
        active_rows = active_rows[:10]

        selections_loader = getattr(self.store, "list_portfolio_selections", None)
        events_raw = []
        if callable(selections_loader):
            try:
                events_raw = selections_loader(limit=32)
            except Exception:
                events_raw = []
        events: list[dict[str, Any]] = []
        for event in recent_chronological(
            events_raw if isinstance(events_raw, (list, tuple)) else [],
            limit=32,
            timestamp_keys=("committed_at", "selected_at"),
        ):
            if not isinstance(event, Mapping):
                continue
            event_reasons = plural(event.get("reasons"), event.get("reason"))
            admission_reasons = plural(event.get("admission_reasons"), event_reasons)
            replacement_reasons = plural(
                event.get("replacement_reasons"),
                event.get("replacement_reason"),
            )
            events.append(
                {
                    "portfolio_selection_id": event.get("portfolio_selection_id"),
                    "selected_at": event.get("selected_at"),
                    "committed_at": event.get("committed_at"),
                    "k": event.get("k", len(event.get("members", ()))),
                    "status": event.get("status", review_state.get("status")),
                    "reason": event.get("reason"),
                    "reasons": event_reasons,
                    "admission_reasons": admission_reasons,
                    "replacement_reasons": replacement_reasons,
                }
            )
        events = events[-32:]

        state = dict(review_state)
        policy = state.get("policy") if isinstance(state.get("policy"), Mapping) else {}
        active_policy = load("get_operator_config", "rolling_admission_policy_active", {})
        active_policy = dict(active_policy) if isinstance(active_policy, Mapping) else {}
        reviewed_policy = load("get_operator_config", "rolling_admission_policy_review", {})
        reviewed_policy = dict(reviewed_policy) if isinstance(reviewed_policy, Mapping) else {}

        def immutable_policy_document(
            envelope: Mapping[str, Any],
        ) -> tuple[dict[str, Any], str | None]:
            if not envelope:
                return {}, None
            try:
                expected_identity = _rolling_policy_identity(envelope)
            except OperatorControlError as exc:
                return {}, (
                    "immutable_policy_identity_missing"
                    if exc.code == "ROLLING_POLICY_IDENTITY_REQUIRED"
                    else "immutable_policy_identity_invalid"
                )
            loaded = load(
                "load_admission_policy",
                expected_identity["policy_id"],
                expected_identity["version"],
            )
            if not isinstance(loaded, Mapping):
                return {}, "immutable_policy_unavailable"
            try:
                loaded_identity = _rolling_policy_identity(loaded)
            except OperatorControlError:
                return {}, "immutable_policy_invalid"
            if loaded_identity != expected_identity:
                return {}, "immutable_policy_mismatch"
            return dict(loaded), None

        active_policy_document, active_policy_error = immutable_policy_document(active_policy)
        reviewed_policy_document, reviewed_policy_error = immutable_policy_document(reviewed_policy)
        if active_policy_document:
            active_policy = {**active_policy, "policy": dict(active_policy_document)}
        if reviewed_policy_document:
            reviewed_policy = {**reviewed_policy, "policy": dict(reviewed_policy_document)}
        risk = self.risk_settings_data()
        effective_limits = (
            risk.get("effective_limits")
            if isinstance(risk, Mapping)
            else {}
        )
        if not isinstance(effective_limits, Mapping):
            effective_limits = risk.get("limits", {}) if isinstance(risk, Mapping) else {}
        effective_limits = effective_limits if isinstance(effective_limits, Mapping) else {}
        risk_usage = risk.get("usage") if isinstance(risk, Mapping) else {}
        risk_usage = risk_usage if isinstance(risk_usage, Mapping) else {}

        # CanarySettingsService exposes generic canary counters in ``usage``.
        # Rolling budget usage is a separate risk-accounting dimension and must
        # never be inferred from those counters or from a worker's stale
        # display-only allocation summary.
        accounting: Mapping[str, Any] = {}
        accounting_sources = (
            risk_usage,
            risk.get("rolling_usage") if isinstance(risk, Mapping) else None,
            risk,
        )
        has_persisted_rolling_usage = any(
            isinstance(source, Mapping)
            and any(name in source for name in _ROLLING_RISK_USAGE_FIELDS)
            for source in accounting_sources
        )
        accounting_method = getattr(self.store, "canary_risk_accounting", None)
        if not has_persisted_rolling_usage and callable(accounting_method):
            try:
                loaded_accounting = accounting_method(now=self.clock())
            except TypeError:
                try:
                    loaded_accounting = accounting_method(self.clock())
                except Exception:
                    loaded_accounting = {}
            except Exception:
                loaded_accounting = {}
            if isinstance(loaded_accounting, Mapping):
                accounting = loaded_accounting
        rolling_sources: tuple[Mapping[str, Any], ...] = tuple(
            source
            for source in (
                accounting,
                risk.get("rolling_usage") if isinstance(risk, Mapping) else None,
                risk_usage,
                risk,
            )
            if isinstance(source, Mapping)
        )
        rolling_usage: dict[str, Any] = {}
        for name in _ROLLING_RISK_USAGE_FIELDS:
            for source in rolling_sources:
                if name in source and source.get(name) is not None:
                    rolling_usage[name] = source.get(name)
                    break
        if "rolling_global_reserved_usd" in rolling_usage:
            rolling_usage.setdefault(
                "rolling_global_open_capital_usd",
                rolling_usage["rolling_global_reserved_usd"],
            )
        canary_sources = tuple(
            source
            for source in (risk_usage, accounting, risk)
            if isinstance(source, Mapping)
        )
        canary_usage: dict[str, Any] = {}
        for name in _CANARY_USAGE_FIELDS:
            for source in canary_sources:
                if name in source and source.get(name) is not None:
                    canary_usage[name] = source.get(name)
                    break
        def rolling_identity(source: Mapping[str, Any], *names: str) -> str | None:
            for name in names:
                value = source.get(name)
                if value in (None, ""):
                    continue
                normalized = str(value).strip()
                if normalized:
                    return normalized
            return None

        active_policy_document = dict(active_policy_document)
        if active_policy_error:
            active_policy_id = None
            active_policy_version = None
            active_policy_hash = None
        elif not active_policy:
            # No active pointer is a valid pre-activation state.  Keep the
            # active projection empty and let selection fences describe the
            # resulting lack of active authority; it must not stale an
            # otherwise valid, explicitly reviewed proposal.
            active_policy_id = None
            active_policy_version = None
            active_policy_hash = None
        else:
            try:
                active_identity = _rolling_policy_identity(active_policy)
            except OperatorControlError:
                active_policy_id = None
                active_policy_version = None
                active_policy_hash = None
                active_policy_error = active_policy_error or "immutable_policy_identity_invalid"
            else:
                active_policy_id = active_identity["policy_id"]
                active_policy_version = active_identity["version"]
                active_policy_hash = active_identity["config_hash"]
        selection_policy_id = rolling_identity(
            selection, "policy_id", "admission_policy_id"
        )
        selection_policy_version = rolling_identity(
            selection, "policy_version", "admission_policy_version"
        )
        selection_policy_hash = rolling_identity(
            selection, "policy_hash", "config_hash"
        )
        selection_policy_config = selection.get("policy_config")
        if (
            not selection_policy_hash
            and isinstance(selection_policy_config, Mapping)
        ):
            selection_policy_hash = rolling_identity(
                selection_policy_config, "policy_hash", "config_hash"
            )
        selection_fence_blockers: list[str] = []

        def fence_blocker(reason: str) -> None:
            if reason not in selection_fence_blockers:
                selection_fence_blockers.append(reason)
        if active_policy_error:
            fence_blocker(f"active_{active_policy_error}")

        if not active_policy_id:
            fence_blocker("active_policy_id_unavailable")
        if not active_policy_version:
            fence_blocker("active_policy_version_unavailable")
        if not active_policy_hash:
            fence_blocker("active_policy_hash_unavailable")
        if not selection_policy_id:
            fence_blocker("selection_policy_id_missing")
        elif active_policy_id and selection_policy_id != active_policy_id:
            fence_blocker("selection_policy_id_mismatch")
        if not selection_policy_version:
            fence_blocker("selection_policy_version_missing")
        elif active_policy_version and selection_policy_version != active_policy_version:
            fence_blocker("selection_policy_version_mismatch")
        persisted_policy: Mapping[str, Any] = {}
        persisted_policy_loader = getattr(self.store, "load_admission_policy", None)
        if (
            callable(persisted_policy_loader)
            and selection_policy_id
            and selection_policy_version
        ):
            try:
                loaded_policy = persisted_policy_loader(
                    selection_policy_id,
                    selection_policy_version,
                )
            except Exception:
                loaded_policy = None
            if isinstance(loaded_policy, Mapping):
                persisted_policy = loaded_policy
            else:
                fence_blocker("persisted_policy_unavailable")
        persisted_policy_identity: dict[str, str] = {}
        if persisted_policy:
            try:
                persisted_policy_identity = _rolling_policy_identity(persisted_policy)
            except OperatorControlError:
                fence_blocker("persisted_policy_identity_invalid")
        persisted_policy_hash = persisted_policy_identity.get("config_hash")
        if not selection_policy_hash and persisted_policy_hash:
            selection_policy_hash = persisted_policy_hash
        if not selection_policy_hash:
            fence_blocker("selection_policy_hash_missing")
        elif active_policy_hash and selection_policy_hash != active_policy_hash:
            fence_blocker("selection_policy_hash_mismatch")
        if (
            persisted_policy_hash
            and active_policy_hash
            and persisted_policy_hash != active_policy_hash
        ):
            fence_blocker("persisted_policy_hash_mismatch")

        active_risk_source = (
            active_policy.get("risk", active_policy.get("risk_config"))
            if isinstance(active_policy, Mapping)
            else {}
        )
        active_risk_source = (
            active_risk_source
            if isinstance(active_risk_source, Mapping)
            else active_policy
        )
        risk_names = (
            ("risk_config_id", "active_risk_config_id"),
            ("risk_config_generation", "active_risk_config_generation"),
            ("risk_config_hash", "active_risk_config_hash"),
        )
        current_risk_names = {
            "risk_config_id": ("config_id", "active_config_id", "settings_config_id"),
            "risk_config_generation": (
                "generation",
                "config_generation",
                "settings_generation",
            ),
            "risk_config_hash": ("config_hash", "active_config_hash", "settings_hash"),
        }
        expected_risk: dict[str, str | None] = {}
        selected_risk: dict[str, str | None] = {}
        current_risk: dict[str, str | None] = {}
        for field, alias in risk_names:
            expected_risk[field] = rolling_identity(active_risk_source, field, alias)
            selected_risk[field] = rolling_identity(selection, field, alias)
            current_risk[field] = rolling_identity(
                risk,
                *current_risk_names[field],
            )
            if not expected_risk[field]:
                expected_risk[field] = current_risk[field]
            if not expected_risk[field]:
                fence_blocker(f"active_{field}_unavailable")
            if not selected_risk[field]:
                fence_blocker(f"selection_{field}_missing")
            elif expected_risk[field] and field != "risk_config_generation":
                if selected_risk[field] != expected_risk[field]:
                    fence_blocker(f"selection_{field}_mismatch")
            if (
                current_risk[field]
                and expected_risk[field]
                and field != "risk_config_generation"
                and current_risk[field] != expected_risk[field]
            ):
                fence_blocker(f"active_{field}_stale")
        for source_name, source in (
            ("selection", selected_risk),
            ("active", expected_risk),
            ("current", current_risk),
        ):
            generation = source.get("risk_config_generation")
            try:
                parsed_generation = int(generation) if generation is not None else 0
            except (TypeError, ValueError, OverflowError):
                parsed_generation = 0
            if parsed_generation <= 0:
                fence_blocker(f"{source_name}_risk_config_generation_invalid")
            elif (
                source_name != "current"
                and current_risk["risk_config_generation"]
            ):
                try:
                    current_generation = int(current_risk["risk_config_generation"])
                except (TypeError, ValueError, OverflowError):
                    current_generation = 0
                if parsed_generation != current_generation:
                    fence_blocker(
                        f"{source_name}_risk_config_generation_mismatch"
                    )
        if selection_fence_blockers:
            if "rolling_selection_stale" not in selection_fence_blockers:
                selection_fence_blockers.insert(0, "rolling_selection_stale")
            for row in active_rows:
                row_blockers: list[str] = []
                for blocker in (
                    *selection_fence_blockers,
                    *(row.get("blockers") or ()),
                ):
                    if blocker not in row_blockers:
                        row_blockers.append(blocker)
                row["blockers"] = row_blockers[:16]
                row["executable"] = False

        selection_id = text(
            selection.get("portfolio_selection_id")
            or selection.get("selection_id")
        )
        actual_k = len(active_rows)
        try:
            selected_k = min(10, max(0, int(selection.get("k", len(members)) or 0)))
        except (TypeError, ValueError):
            selected_k = min(10, max(0, len(members)))
        evidence_ready = (
            active_member_count > 0
            and evidence_count == active_member_count
            and not selection_fence_blockers
        )
        rolling_evidence_summary["status"] = (
            "READY"
            if evidence_ready
            else ("PARTIAL" if all_evidence_rows else "UNKNOWN")
        )
        cold_start = []
        if not selection_id or not evidence_ready:
            cold_start = [
                "persist_strategy_versions",
                "persist_research_trials",
                "persist_actual_evidence_windows",
                "bind_exact_selection_member_evidence_window_ids",
                "review_and_activate_admission_policy",
                "bind_active_risk_config",
            ]
            if selection_fence_blockers:
                cold_start.append("selection_policy_or_risk_stale")
            if active_member_count and any(
                not row.get("candidate_id") or not row.get("research_trial_id")
                for row in active_rows
            ):
                cold_start.append("bind_exact_candidate_and_research_trial_ids")
            for source in (review_state, state):
                persisted = source.get("cold_start_requirements") if isinstance(source, Mapping) else None
                if isinstance(persisted, (list, tuple)):
                    for requirement in persisted[:16]:
                        if requirement not in cold_start:
                            cold_start.append(requirement)
                pending = source.get("pending") if isinstance(source, Mapping) else None
                if isinstance(pending, (list, tuple)):
                    for requirement in pending[:16]:
                        if requirement not in cold_start:
                            cold_start.append(requirement)
            cold_start = cold_start[:32]
        status = (
            "COLD_START"
            if not selection_id or not evidence_ready
            else str(state.get("status") or "CURRENT").upper()
        )


        risk_binding: dict[str, Any] = {}
        for source in (selection, active_policy):
            if not isinstance(source, Mapping):
                continue
            for key in (
                "risk_config_id",
                "active_risk_config_id",
                "risk_config_generation",
                "active_risk_config_generation",
                "risk_config_hash",
                "active_risk_config_hash",
            ):
                if key in source and source.get(key) is not None:
                    risk_binding.setdefault(key, source.get(key))
        policy_document = dict(active_policy_document)
        policy_id = selection_policy_id or active_policy_id
        policy_version = selection_policy_version or active_policy_version
        policy_hash = selection_policy_hash or active_policy_hash

        reason_history_raw = review_state.get("event_history", [])
        history_rows = recent_chronological(
            reason_history_raw if isinstance(reason_history_raw, (list, tuple)) else [],
            limit=128,
            timestamp_keys=("at", "timestamp", "created_at"),
        )
        history_projection: list[dict[str, Any]] = []
        for event in history_rows:
            reasons = plural(event.get("reasons"), event.get("reason"))
            admission_reasons = plural(event.get("admission_reasons"), reasons)
            replacement_reasons = plural(
                event.get("replacement_reasons"),
                event.get("replacement_reason"),
            )
            history_projection.append(
                {
                    "at": event.get("at"),
                    "status": event.get("status"),
                    "reason": event.get("reason"),
                    "reasons": reasons,
                    "admission_reasons": admission_reasons,
                    "replacement_reasons": replacement_reasons,
                }
            )
        reason_history = history_projection[-32:]
        admission_reason_history = [
            item for item in history_projection if item.get("admission_reasons")
        ][-32:]
        replacement_reason_history = [
            item for item in history_projection if item.get("replacement_reasons")
        ][-32:]

        next_jobs: Any = None
        for source in (
            state,
            review_state,
            worker_payload,
            worker_payload.get("rolling_portfolio")
            if isinstance(worker_payload.get("rolling_portfolio"), Mapping)
            else {},
        ):
            if isinstance(source, Mapping) and source.get("next_jobs") is not None:
                next_jobs = source.get("next_jobs")
                break
        if next_jobs is None:
            next_jobs = []

        def policy_summary(
            source: Mapping[str, Any],
            document: Mapping[str, Any],
        ) -> dict[str, Any]:
            document = document if isinstance(document, Mapping) else {}
            return {
                "policy_id": rolling_identity(document, "policy_id", "id"),
                "version": rolling_identity(document, "version", "policy_version"),
                "config_hash": rolling_identity(document, "config_hash", "policy_hash"),
                "global_budget": document.get("global_budget"),
                "max_active_strategies": document.get(
                    "max_members",
                    document.get("max_k"),
                ),
                "status": source.get("status") or source.get("review_status"),
            }

        active_summary = policy_summary(active_policy, active_policy_document)
        proposed_summary = policy_summary(reviewed_policy, reviewed_policy_document)

        current_binding = {
            "risk_config_id": current_risk.get("risk_config_id"),
            "risk_config_generation": current_risk.get("risk_config_generation"),
            "risk_config_hash": current_risk.get("risk_config_hash"),
        }
        proposed_binding = {
            "risk_config_id": reviewed_policy.get("risk_config_id"),
            "risk_config_generation": reviewed_policy.get("risk_config_generation"),
            "risk_config_hash": reviewed_policy.get("risk_config_hash"),
        }
        binding_present = all(value not in (None, "") for value in proposed_binding.values())
        binding_matches = binding_present and all(
            str(proposed_binding[name]) == str(current_binding[name])
            for name in proposed_binding
        )
        proposed_status = str(
            reviewed_policy.get("status")
            or reviewed_policy.get("review_status")
            or "NOT_REVIEWED"
        ).upper()
        if active_policy_error or reviewed_policy_error:
            proposed_status = "STALE"
        if proposed_status == "REVIEWED" and not binding_matches:
            proposed_status = "STALE"
        allocation_review = reviewed_policy.get("allocation_review")
        allocation_review = (
            dict(allocation_review) if isinstance(allocation_review, Mapping) else {}
        )
        caps = {
            "submissions_per_day": effective_limits.get("max_submitted_orders_per_day"),
            "per_buy_usd": effective_limits.get("max_all_in_buy_usd"),
            "daily_buy_usd": effective_limits.get("max_gross_daily_buy_usd"),
            "open_exposure_usd": effective_limits.get("max_aggregate_exposure_usd"),
            "max_open_positions": effective_limits.get("max_positions"),
            "per_market_buy_usd": effective_limits.get("per_market_buy_cap_usd"),
            "per_event_buy_usd": effective_limits.get("per_event_buy_cap_usd"),
            "cumulative_buy_usd": effective_limits.get("cumulative_buy_cap_usd"),
        }
        policy_review = {
            "status": proposed_status,
            "active": active_summary,
            "proposed": {
                **proposed_summary,
                "draft_id": reviewed_policy.get("draft_id"),
                "draft_version": reviewed_policy.get("draft_version"),
                "draft_hash": reviewed_policy.get("draft_hash"),
            },
            "caps": caps,
            "allocation": allocation_review,
            "canary_binding": current_binding,
            "proposed_canary_binding": proposed_binding,
            "active_vs_proposed": {
                "active_policy_id": active_summary.get("policy_id"),
                "active_policy_version": active_summary.get("version"),
                "active_global_budget": active_summary.get("global_budget"),
                "proposed_policy_id": proposed_summary.get("policy_id"),
                "proposed_policy_version": proposed_summary.get("version"),
                "proposed_global_budget": proposed_summary.get("global_budget"),
                "requires_explicit_activation": True,
            },
            "paper_only": True,
            "live_execution": False,
        }

        return _bounded_value(
            {
                "status": status,
                "paper_only": True,
                "live_execution": False,
                "controller_status": worker_status,
                "k": selected_k,
                "actual": actual_k,
                "actual_k": actual_k,
                "actionable": sum(
                    1 for row in active_rows if row.get("executable")
                ),
                "rolling_evidence": rolling_evidence_summary,
                "policy": policy_document,
                "active_policy": dict(active_policy),
                "reviewed_policy": dict(reviewed_policy),
                "proposed_policy": dict(reviewed_policy),
                "policy_review": policy_review,
                "allocation_review": allocation_review,
                "next_jobs": next_jobs,
                "next_work": worker_payload.get("next_work")
                or rolling_worker_state.get("next_work"),
                "cold_start_requirements": cold_start,
                "policy_identity": {
                    "policy_id": policy_id,
                    "version": policy_version,
                    "config_hash": policy_hash,
                    "active_policy_id": active_policy_id,
                    "active_policy_version": active_policy_version,
                    "active_policy_hash": active_policy_hash,
                },
                "risk": risk_binding,
                "controller": {
                    "worker_name": rolling_worker.get("worker_name"),
                    "status": worker_status,
                    "worker_status": worker_status,
                    "scheduled": bool(worker_scheduled),
                    "started_at": rolling_worker.get("started_at"),
                    "heartbeat_at": rolling_worker.get("heartbeat_at"),
                    "worker_heartbeat_at": rolling_worker.get("heartbeat_at"),
                    "updated_at": rolling_worker.get("updated_at"),
                    "last_error": worker_payload.get("last_error"),
                    "last_error_code": worker_payload.get("last_error_code"),
                    "last_review_at": state.get("reviewed_at"),
                    "next_review_at": state.get(
                        "review_due_at",
                        selection.get("review_due_at"),
                    ),
                    "next_work": worker_payload.get("next_work")
                    or rolling_worker_state.get("next_work"),
                    "interval_seconds": worker_payload.get(
                        "configured_interval_seconds"
                    ),
                    "cadence_seconds": worker_payload.get(
                        "configured_interval_seconds"
                    ),
                    "configured_interval_seconds": worker_payload.get(
                        "configured_interval_seconds"
                    ),
                    "evidence_interval_seconds": worker_payload.get(
                        "evidence_interval_seconds",
                        worker_payload.get("configured_interval_seconds"),
                    ),
                    "review_interval_seconds": worker_payload.get(
                        "review_interval_seconds"
                    ),
                    "blocker": state.get("blocker"),
                },
                "evidence": {
                    **rolling_evidence_summary,
                    "status": (
                        "READY"
                        if evidence_ready
                        else rolling_evidence_summary.get("status", "COLD_START")
                    ),
                    "actual_coverage": state.get("actual_coverage"),
                    "available_windows": rolling_evidence_summary.get(
                        "available_windows",
                        evidence_count,
                    ),
                    "required_windows": active_member_count,
                    "source_classes": sorted(
                        {
                            str(row.get("source_class"))
                            for row in active_rows
                            if row.get("source_class")
                        }
                    )[:10],
                },
                "selection": {
                    "status": state.get("selection_status", status),
                    "portfolio_selection_id": selection_id,
                    "k": selected_k,
                    "actual_k": actual_k,
                    "policy_id": policy_id,
                    "version": policy_version,
                    "policy_version": policy_version,
                    "config_hash": policy_hash,
                    "risk_config_id": selected_risk["risk_config_id"],
                    "risk_config_generation": selected_risk[
                        "risk_config_generation"
                    ],
                    "risk_config_hash": selected_risk["risk_config_hash"],
                    "blockers": selection_fence_blockers[:32],
                },
                "signal": {
                    "status": state.get(
                        "signal_status",
                        worker_payload.get("signal_status", "UNKNOWN"),
                    ),
                    "evaluated_members": state.get(
                        "evaluated_members",
                        worker_payload.get("evaluated_members", 0),
                    ),
                    "ready_members": state.get(
                        "ready_members",
                        worker_payload.get("ready_members", 0),
                    ),
                    "cursor": dict(rolling_cursor),
                },
                "execution": {
                    "status": state.get(
                        "execution_status",
                        worker_payload.get("execution_status", "PAPER_ONLY"),
                    ),
                    "cursor": dict(rolling_cursor),
                    "live_execution": False,
                    "submissions": worker_payload.get("submissions", []),
                },
                "active_rows": active_rows,
                "global_limits": dict(effective_limits),
                "global_limits_usage": dict(rolling_usage),
                "rolling_usage": dict(rolling_usage),
                "canary_usage": dict(canary_usage),
                "events": events,
                "event_history": reason_history,
                "reason_history": reason_history,
                "admission_reason_history": admission_reason_history,
                "replacement_reason_history": replacement_reason_history,
                "selection_blockers": selection_fence_blockers[:32],
                "risk_binding": risk_binding,
            }
        )

    def v2_snapshot(self, endpoint: str, params: Mapping[str, Any] | None = None) -> dict[str, Any]:
        name = endpoint.strip("/")
        if name.lower() == "shadow":
            return self.shadow_data(params)
        if name.lower() == "shadow/latest":
            latest = self.shadow_data({"page": 1, "page_size": 1})
            return latest.get("latest") if isinstance(latest.get("latest"), Mapping) else {
                "available": False,
                "read_only": True,
                "paper_only": True,
                "live_execution": False,
            }
        if name.lower().startswith("shadow/"):
            identifier = unquote(name.split("/", 1)[1])
            if identifier.lower() == "latest":
                latest = self.shadow_data({"page": 1, "page_size": 1})
                return latest.get("latest") if isinstance(latest.get("latest"), Mapping) else {
                    "available": False,
                    "read_only": True,
                    "paper_only": True,
                    "live_execution": False,
                }
            return self.shadow_detail(identifier)
        if name.lower().startswith("datasets/"):
            parts = name.split("/")
            identifier = unquote(parts[1])
            if len(parts) > 2 and parts[2].lower() == "missing-ranges":
                values = _pagination_params(params)
                method = getattr(self.store, "paginate_dataset_missing_ranges", None) if self.store is not None else None
                if callable(method):
                    try:
                        return method(identifier, dataset_version=values.get("dataset_version"), page=values["page"], page_size=values["page_size"], sort=values["sort"] or "range_index", direction=values["direction"], filter=values["filter"])
                    except (AttributeError, TypeError, ValueError):
                        pass
                detail = self.dataset_detail(identifier)
                catalog = detail.get("catalog", {}) if isinstance(detail, Mapping) else {}
                ranges = catalog.get("missing_ranges", []) if isinstance(catalog, Mapping) else []
                start = (values["page"] - 1) * values["page_size"]
                return _page_result(ranges[start : start + values["page_size"]], page=values["page"], page_size=values["page_size"], total=len(ranges))
            return self.dataset_detail(identifier)
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
            "rolling-portfolio": lambda _params: self.rolling_portfolio_data(),
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
        states = (
            self.store.list_paper_states(limit=100)
            if self.store is not None and callable(getattr(self.store, "list_paper_states", None))
            else []
        )
        return {"available": bool(states), "states": states, "live_execution": False}

    def shadow_detail(self, job_id: str) -> dict[str, Any]:
        identifier = unquote(str(job_id).strip())
        loader = getattr(self.store, "load_shadow_job", None) if self.store is not None else None
        row: Mapping[str, Any] | None = None
        if callable(loader) and identifier:
            try:
                loaded = loader(identifier)
                row = loaded if isinstance(loaded, Mapping) else None
            except (AttributeError, TypeError, ValueError, sqlite3.Error):
                row = None
        projected = _shadow_public_job(row)
        if projected is None:
            return {
                "available": False,
                "job_id": _shadow_public_text(identifier),
                "read_only": True,
                "paper_only": True,
                "live_execution": False,
            }
        return {"available": True, **projected}

    def shadow_data(self, params: Mapping[str, Any] | None = None) -> dict[str, Any]:
        values = _pagination_params(params)
        raw_status = values.get("status")
        status = str(raw_status).strip().upper() if raw_status else None
        if status and status not in _SHADOW_STATUSES:
            raise ValueError("invalid shadow status")
        page = values["page"]
        page_size = values["page_size"]
        native_result: Mapping[str, Any] | None = None
        paginator = getattr(self.store, "paginate_shadow_jobs", None) if self.store is not None else None
        if callable(paginator):
            try:
                result = paginator(
                    page=page,
                    page_size=page_size,
                    status=status,
                    sort=values["sort"] or "updated_at",
                    direction=values["direction"],
                    filter=values["filter"],
                )
                native_result = result if isinstance(result, Mapping) else None
            except (AttributeError, TypeError, ValueError, sqlite3.Error):
                native_result = None
        if native_result is None:
            rows: list[dict[str, Any]] = []
            total = 0
            latest = None
            current = None
            status_counts = {status_name: 0 for status_name in sorted(_SHADOW_STATUSES)}
        else:
            raw_items = native_result.get("items", [])
            rows = [
                projected
                for row in raw_items if isinstance(row, Mapping)
                if (projected := _shadow_public_job(row)) is not None
            ] if isinstance(raw_items, (list, tuple)) else []
            page = int(native_result.get("page", page) or page)
            page_size = int(native_result.get("page_size", page_size) or page_size)
            total = max(0, int(native_result.get("total", 0) or 0))
            latest = _shadow_public_job(native_result.get("latest"))
            current = _shadow_public_job(native_result.get("current_job"))
            raw_counts = native_result.get("status_counts")
            status_counts = {
                status_name: int(raw_counts.get(status_name, 0) or 0)
                for status_name in sorted(_SHADOW_STATUSES)
            } if isinstance(raw_counts, Mapping) else {
                status_name: sum(1 for row in rows if row.get("status") == status_name)
                for status_name in sorted(_SHADOW_STATUSES)
            }
            if latest is None and rows:
                latest = rows[0]
            if current is None:
                current = next(
                    (row for row in rows if str(row.get("status") or "").upper() in _SHADOW_ACTIVE_STATUSES),
                    None,
                ) or latest
        return {
            **_page_result(rows, page=page, page_size=page_size, total=total),
            "available": bool(total),
            "latest": latest,
            "current_job": current,
            "current_job_id": current.get("job_id") if isinstance(current, Mapping) else None,
            "next_action": current.get("next_action") if isinstance(current, Mapping) else None,
            "status_counts": status_counts,
            "active_count": sum(status_counts[name] for name in _SHADOW_ACTIVE_STATUSES),
            "read_only": True,
            "paper_only": True,
            "live_execution": False,
        }

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
        worker_count_method = getattr(self.store, "worker_state_count", None)
        worker_total = (
            int(worker_count_method())
            if callable(worker_count_method)
            else None
        )
        worker_limit = 128
        worker_dashboard_loader = getattr(self.store, "list_worker_states_dashboard", None)
        workers = (
            worker_dashboard_loader(limit=worker_limit)
            if callable(worker_dashboard_loader)
            else self.store.list_worker_states(limit=worker_limit)
        )
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
        # Do not rebuild collector health or research accounting from large
        # history tables during a status refresh.  Persisted worker health is
        # the truthful local read projection; detailed health remains on its
        # dedicated endpoint.
        summary = {"autonomous": {}, "hermes": {}}
        current_health = dict(health_payload) if isinstance(health_payload, Mapping) else {}
        health_fields = self._health_status_fields(normalized_workers, current_health)
        cycles_loader = getattr(self.store, "list_collection_cycles_dashboard", None)
        cycles = (
            cycles_loader(limit=20)
            if callable(cycles_loader)
            else self.store.list_collection_cycles(limit=20)
        )
        queue = self.store.research_queue_stats()
        return {
            "status": status,
            "summary": self.store.dashboard_summary(),
            "cycles": cycles,
            "queue": queue,
            "workers": normalized_workers,
            "normalized_workers": normalized_workers,
            "workers_total": worker_total if worker_total is not None else len(normalized_workers),
            "workers_returned": len(normalized_workers),
            "workers_truncated": (
                worker_total is not None and worker_total > len(normalized_workers)
            ),
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
    def _bounded_health_from_activity(aggregate: Mapping[str, Any] | None) -> dict[str, Any]:
        """Preserve current collector errors when no persisted health projection exists."""
        if not isinstance(aggregate, Mapping):
            return {}
        activity = aggregate.get("latest_activity")
        if not isinstance(activity, (list, tuple)):
            return {}
        failures: list[dict[str, Any]] = []
        for item in activity[:32]:
            if not isinstance(item, Mapping) or str(item.get("kind") or "").lower() != "collection_error":
                continue
            details = item.get("details")
            details = details if isinstance(details, Mapping) else {}
            failure = {
                "error_id": item.get("event_id"),
                "market_id": item.get("market_id"),
                "observed_at": item.get("timestamp"),
                "kind": item.get("status") or details.get("kind") or "COLLECTION_ERROR",
                "detail": details.get("detail") or item.get("message"),
            }
            failure["reason_code"] = str(failure["kind"] or "COLLECTION_ERROR").upper()
            failure["reason"] = str(failure["detail"] or "Current collector failure retained.")
            failures.append(failure)
        if not failures:
            return {}
        reason = {
            "code": "CURRENT_COLLECTION_FAILURES",
            "reason": f"{len(failures)} current collector failure(s) are retained.",
        }
        return {
            "status": "DEGRADED",
            "grade": "B",
            "grade_scope": "collector_health",
            "reason_code": reason["code"],
            "reasons": [reason],
            "current_failures": failures,
            "collection_errors": len(failures),
            "historical_error_count": 0,
            "source_type": "FORWARD_COLLECTED",
        }


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
        dashboard_lister = (
            getattr(self.store, "list_dataset_catalog_dashboard", None)
            if self.store is not None
            else None
        )
        legacy_lister = (
            getattr(self.store, "list_dataset_catalog", None)
            if self.store is not None
            else None
        )
        if self.store is None or not (callable(dashboard_lister) or callable(legacy_lister)):
            return {
                "historical": [],
                "forward": [],
                "historical_count": 0,
                "forward_count": 0,
                "live_execution": False,
            }
        records = (
            dashboard_lister(limit=_MAX_SIZE_FALLBACK)
            if callable(dashboard_lister)
            else legacy_lister(limit=_MAX_SIZE_FALLBACK)
        )
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

        dashboard_lister = getattr(self.store, "list_candidate_lifecycle_dashboard", None)
        if callable(dashboard_lister):
            try:
                return bounded_records(dashboard_lister(limit=bounded_limit))
            except (AttributeError, TypeError, ValueError, sqlite3.Error):
                pass

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
            "unresolved_candidates",
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
        """Read the persisted eligibility binding evidence."""
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
            "frozen_hash": eligibility.get("frozen_hash"),
            "evidence_json": eligibility.get("evidence_json"),
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

        Dashboard reads must never re-run canary qualification.  Eligibility is
        displayed only when its persisted evidence remains authoritatively bound
        to the current candidate lifecycle.
        """
        candidate_id = str(item.get("candidate_id") or "").strip()
        stage = str(item.get("stage") or "").strip().upper()
        payload = item.get("payload")
        payload = payload if isinstance(payload, Mapping) else {}
        eligibility = self._candidate_canary_eligibility(candidate_id)
        canary_eligible = (
            eligibility is not None
            and _canary_eligibility_is_bound(self.store, candidate_id, eligibility)
        )
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
            positions = portfolio.get("positions", {})
            positions = positions if isinstance(positions, Mapping) else {}
            position_count = _number_or_zero(portfolio.get("position_count", len(positions)))
            position_count = max(0, int(position_count))
            position_returned = _number_or_zero(portfolio.get("positions_returned", len(positions)))
            position_returned = max(0, int(position_returned))
            positions_truncated = bool(portfolio.get("positions_truncated", position_returned < position_count))
            equity = _number_or_zero(portfolio.get("equity", state.get("equity", 0.0)))
            initial = _number_or_zero(portfolio.get("initial_cash", state.get("initial_cash", 0.0)))
            pnl = equity - initial if initial else _number_or_zero(state.get("forward_pnl"))
            ledger = []
            ledger_count = 0
            ledger_summary_loader = getattr(self.store, "paper_bet_ledger_summary", None)
            if callable(ledger_summary_loader):
                ledger_summary = ledger_summary_loader(str(item.get("experiment_id", "")))
                if isinstance(ledger_summary, Mapping):
                    ledger_count = max(0, int(ledger_summary.get("count", 0) or 0))
                    total_pnl += _number_or_zero(ledger_summary.get("net_pnl"))
                    winning_bets += max(0, int(ledger_summary.get("wins", 0) or 0))
                    total_bets += ledger_count
            else:
                ledger_loader = getattr(self.store, "list_paper_bet_ledger", None)
                if callable(ledger_loader):
                    ledger = ledger_loader(str(item.get("experiment_id", "")), limit=1000)
                for bet in ledger:
                    payload = bet.get("payload", {}) if isinstance(bet, Mapping) else {}
                    pnl_value = _number_or_zero(payload.get("net_pnl")) if isinstance(payload, Mapping) else 0.0
                    total_pnl += pnl_value
                    total_bets += 1
                    ledger_count += 1
                    if pnl_value > 0:
                        winning_bets += 1
            total_equity += equity
            if ledger_count == 0:
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
                    "open_positions": dict(positions),
                    "open_position_count": position_count,
                    "open_positions_returned": position_returned,
                    "open_positions_truncated": positions_truncated,
                    "resolved_bets": ledger_count,
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
            dashboard_lister = getattr(self.store, "list_dataset_bootstrap_states_dashboard", None)
            if callable(dashboard_lister):
                try:
                    states = dashboard_lister(limit=limit)
                except (AttributeError, TypeError, ValueError, sqlite3.Error):
                    states = []
            else:
                states = (
                    self.store.list_dataset_bootstrap_states(limit=limit)
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
        progress_lister = getattr(self.store, "list_operator_job_progress", None)
        raw: Any = None
        if callable(progress_lister):
            try:
                # This projection extracts only progress scalars and capped
                # arrays in SQLite; the protocol/job payload never crosses
                # into Python.
                raw = progress_lister(
                    job_prefix=_CAMPAIGN_JOB_PREFIX,
                    limit=_CAMPAIGN_JOB_LIMIT,
                )
            except (AttributeError, TypeError, ValueError, sqlite3.Error):
                raw = None
        if raw is None:
            lister = getattr(self.store, "list_operator_jobs", None)
            if not callable(lister):
                return []
            try:
                # Compatibility with stores predating the progress projection.
                raw = lister(
                    job_prefix=_CAMPAIGN_JOB_PREFIX,
                    limit=_CAMPAIGN_JOB_LIMIT,
                )
            except TypeError:
                try:
                    raw = lister(limit=_CAMPAIGN_JOB_LIMIT)
                except TypeError:
                    try:
                        raw = lister()
                    except (AttributeError, TypeError, ValueError, sqlite3.Error):
                        return []
            except (AttributeError, ValueError, sqlite3.Error):
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
            "counts": {
                "economic_rejection": 0,
                "market_resolution_failure": 0,
                "data_insufficient": 0,
                "software_or_input_error": 0,
                "validation_qualified": 0,
                "final_assessment": 0,
            },
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
        projected_trial_count = payload.get("_trial_count")
        projected_completed_count = payload.get("_completed_trial_count")
        if projected_trial_count is not None:
            trial_count = self._campaign_integer(projected_trial_count, 0)
            completed = min(
                trial_count,
                self._campaign_integer(projected_completed_count, 0),
            )
            trials: list[Mapping[str, Any]] = []
        else:
            raw_trials = payload.get("trials")
            trials = [
                item
                for item in (raw_trials if isinstance(raw_trials, (list, tuple)) else ())
                if isinstance(item, Mapping)
            ][:64]
            payload_counts = payload.get("counts")
            payload_counts = payload_counts if isinstance(payload_counts, Mapping) else {}
            count_names = (
                "economic_rejection",
                "market_resolution_failure",
                "data_insufficient",
                "software_or_input_error",
                "validation_qualified",
                "final_assessment",
            )
            campaign_counts = {
                name: self._campaign_integer(payload_counts.get(name), 0)
                for name in count_names
            }
            if trials:
                for item in trials:
                    trial_status = str(item.get("status") or "").strip().upper().lower()
                    if trial_status in campaign_counts and trial_status not in payload_counts:
                        campaign_counts[trial_status] += 1
                completed = sum(
                    1
                    for item in trials
                    if str(item.get("status") or "").strip().upper()
                    in _CAMPAIGN_TRIAL_TERMINAL
                )
            else:
                completed = sum(campaign_counts.values())
            trial_count = max(len(trials), completed)
        if projected_trial_count is not None:
            payload_counts = payload.get("counts")
            payload_counts = payload_counts if isinstance(payload_counts, Mapping) else {}
            campaign_counts = {
                name: self._campaign_integer(payload_counts.get(name), 0)
                for name in (
                    "economic_rejection",
                    "market_resolution_failure",
                    "data_insufficient",
                    "software_or_input_error",
                    "validation_qualified",
                    "final_assessment",
                )
            }
            if projected_completed_count is None:
                completed = (
                    min(trial_count, sum(campaign_counts.values()))
                    if payload_counts
                    else min(
                        trial_count,
                        self._campaign_integer(projected_completed_count, 0),
                    )
                )
        remaining = max(0, trial_count - completed)
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
            "counts": dict(campaign_counts),
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

        queue_method = getattr(self.store, "list_research_items_dashboard", None)
        queue_items = (
            records("list_research_items_dashboard", limit=_RESEARCH_PROGRESS_LIMIT)
            if callable(queue_method)
            else records("list_research_items", limit=_RESEARCH_PROGRESS_LIMIT)
        )
        latest_loader = getattr(self.store, "get_latest_research_item_dashboard", None)
        if callable(latest_loader):
            try:
                latest_candidate = latest_loader()
            except (AttributeError, TypeError, ValueError, sqlite3.Error):
                latest_candidate = None
            latest_queue = (
                latest_candidate
                if isinstance(latest_candidate, Mapping)
                else None
            )
        else:
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

        # Keep dashboard worker reads on the bounded SQL projection.  The
        # generic single-worker accessor decodes the persisted payload in full.
        worker_method = getattr(self.store, "list_worker_states_dashboard", None)
        worker_rows = (
            records("list_worker_states_dashboard", limit=32)
            if callable(worker_method)
            else records("list_worker_states", limit=32)
        )
        worker_map = {
            text(item.get("worker_name")): item
            for item in worker_rows
            if text(item.get("worker_name"))
        }
        research_worker: Mapping[str, Any] = worker_map.get("research-queue", {})
        if not research_worker:
            research_worker = worker_map.get("research-engine", {})
        if not research_worker:
            exact_worker_method = getattr(self.store, "get_worker_state", None)
            if callable(exact_worker_method):
                # Retain compatibility with stores predating the bounded page.
                research_worker = mapping_call("get_worker_state", "research-queue")
                if not research_worker:
                    research_worker = mapping_call("get_worker_state", "research-engine")
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

    def market_scope_funnel_data(self, *, public: bool = True) -> dict[str, Any]:
        """Project persisted scope aggregates without retaining raw documents.

        Internal callers may request the larger identity-only candidate index
        while they perform exact candidate matching; HTTP-facing responses use
        the smaller public list.
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
                result: dict[str, Any] = {}
                for key, child in list(value.items())[:128]:
                    key_text = str(key)
                    if (
                        key_text in _MARKET_SCOPE_DETAIL_LIST_KEYS
                        and isinstance(child, (list, tuple, set, frozenset))
                    ):
                        limit = 64 if public else 1000
                        result[key_text] = [
                            _market_scope_public_item(item)
                            for item in list(child)[:limit]
                        ]
                    else:
                        result[key_text] = scope_bound(child, depth + 1)
                return result
            if isinstance(value, (list, tuple, set, frozenset)):
                limit = 64 if public else 1000
                return [scope_bound(child, depth + 1) for child in list(value)[:limit]]
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
        detail_limit = 64 if public else 1000
        raw_blocker_values = bounded.get("blockers", [])
        if isinstance(raw_blocker_values, (list, tuple)):
            result["resolution_blockers"] = list(raw_blocker_values)[:detail_limit]
            for item in raw_blocker_values[:detail_limit]:
                if not isinstance(item, Mapping):
                    continue
                reason = str(item.get("reason") or "").strip()
                if reason:
                    try:
                        exact_blockers[reason] = max(0, int(item.get("count", 0) or 0))
                    except (TypeError, ValueError):
                        exact_blockers[reason] = 0
        if isinstance(raw_stage_values, (list, tuple)):
            result["resolution_stages"] = list(raw_stage_values)[:detail_limit]
        if isinstance(bounded.get("items"), (list, tuple)):
            result["resolution_items"] = list(bounded["items"])[:detail_limit]
        result.pop("items", None)
        result.pop("blockers", None)
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
            else self.market_scope_funnel_data(public=False)
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
        candidate_filter = (
            {
                str(identifier).strip()
                for identifier in candidate_ids
                if str(identifier).strip()
            }
            if candidate_ids is not None
            else None
        )

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
        if candidate_filter is not None:
            def current_candidate_refs(values: Any) -> list[str]:
                return [
                    str(value).strip()
                    for value in bounded_list(values, limit=32)
                    if str(value).strip() in candidate_filter
                ]

            filtered_references: dict[str, list[str]] = {}
            for market_id, values in candidate_references.items():
                refs = current_candidate_refs(values)
                if refs:
                    filtered_references[market_id] = refs
            filtered_diagnostics: list[dict[str, Any]] = []
            for diagnostic in diagnostics:
                market_id = str(diagnostic["market_id"])
                refs = current_candidate_refs(diagnostic.get("candidate_references"))
                if not refs:
                    refs = filtered_references.get(market_id, [])
                if not refs:
                    continue
                bounded_diagnostic = dict(diagnostic)
                bounded_diagnostic["candidate_references"] = refs
                filtered_diagnostics.append(bounded_diagnostic)
                filtered_references.setdefault(market_id, refs)
            candidate_references = filtered_references
            allowed_markets = set(candidate_references)

            def current_markets(values: list[Any]) -> list[Any]:
                return [
                    value
                    for value in values
                    if str(value).strip() in allowed_markets
                ]

            candidate_bound_markets = current_markets(candidate_bound_markets)
            scheduled = current_markets(scheduled)
            fresh = current_markets(fresh)
            stale = current_markets(stale)
            missing = current_markets(missing)
            diagnostics = filtered_diagnostics

        def current_candidates(values: Any) -> list[str]:
            return [
                str(value).strip()
                for value in bounded_list(values)
                if candidate_filter is None or str(value).strip() in candidate_filter
            ]

        def ordered_candidate_ids(values: Any, *, limit: int | None = None) -> list[str]:
            """Normalize candidate IDs while keeping persisted order deterministic."""
            if not isinstance(values, (list, tuple, set, frozenset)):
                return []
            iterable = (
                sorted(values, key=lambda value: str(value))
                if isinstance(values, (set, frozenset))
                else values
            )
            result: list[str] = []
            seen: set[str] = set()
            for value in iterable:
                identifier = str(value).strip()
                if not identifier or identifier in seen:
                    continue
                seen.add(identifier)
                result.append(identifier)
                if limit is not None and len(result) >= limit:
                    break
            return result

        persisted_scope_candidates: set[str] = set()
        if candidate_filter is not None:
            # ``resolution_items`` is the bounded persisted canonical scope
            # index.  The aliases keep direct/fake funnel projections
            # compatible without inferring scope from aggregate counts.
            for field in ("resolution_items", "resolutions", "items"):
                for item in bounded_list(funnel.get(field)):
                    if not isinstance(item, Mapping):
                        continue
                    identifier = str(item.get("candidate_id") or "").strip()
                    if identifier in candidate_filter:
                        persisted_scope_candidates.add(identifier)
            # Candidate references are the persisted market-health binding
            # for the canonical scope; only references surviving the exact
            # current-candidate filter may satisfy this check.
            for values in candidate_references.values():
                persisted_scope_candidates.update(
                    identifier
                    for identifier in ordered_candidate_ids(values)
                    if identifier in candidate_filter
                )

        candidate_ids_in_order = ordered_candidate_ids(candidate_ids)

        def first_health(*names: str) -> Any:
            for name in names:
                value = health.get(name)
                if value is not None and value != "":
                    return value
            return None

        required_count = first_health("required_market_count")
        if candidate_filter is not None:
            required_count = len(candidate_bound_markets)
        elif required_count is None:
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
        if candidate_filter is not None:
            unresolved_source = (
                health.get("unresolved_candidates")
                if "unresolved_candidates" in health
                else funnel.get("unresolved_candidates")
            )
            persisted_unresolved = [
                identifier
                for identifier in ordered_candidate_ids(unresolved_source, limit=1000)
                if identifier in candidate_filter
            ]
            missing_scope = [
                identifier
                for identifier in candidate_ids_in_order
                if identifier not in persisted_scope_candidates
            ]
            # Prioritize current candidates so the existing 1000-item bound
            # cannot hide an unresolved current scope behind stale health.
            unresolved = ordered_candidate_ids(
                [*missing_scope, *persisted_unresolved],
                limit=1000,
            )
            closed = current_candidates(
                health.get("closed_candidates")
                if "closed_candidates" in health
                else funnel.get("closed_candidates")
            )
        capacity_excluded = current_candidates(
            health.get("capacity_excluded_candidates")
            if "capacity_excluded_candidates" in health
            else funnel.get("capacity_excluded_candidates")
        )
        reason_code = first_health("reason_code")
        grade = first_health("grade")
        if candidate_filter is not None:
            if capacity_excluded:
                grade, reason_code = "D", "COLLECTOR_CAPACITY_INSUFFICIENT"
            elif unresolved:
                grade, reason_code = "D", "CANDIDATE_FORWARD_MARKET_UNRESOLVED"
            elif closed:
                grade, reason_code = "D", "CANDIDATE_MARKET_CLOSED"
            elif missing:
                grade, reason_code = "D", "REQUIRED_MARKETS_MISSING"
            elif stale:
                grade, reason_code = "C", "REQUIRED_MARKETS_STALE"
            elif required_market_count:
                grade, reason_code = "A", "REQUIRED_MARKETS_FRESH"
            else:
                grade, reason_code = "D", "CANDIDATE_FORWARD_MARKET_UNRESOLVED"
        else:
            if reason_code is None:
                reason_code = (
                    "CANDIDATE_FORWARD_MARKET_UNRESOLVED"
                    if unresolved
                    else None if funnel.get("available") else "NO_PERSISTED_SCOPE_RESOLUTION"
                )
            if grade is None:
                grade = "CURRENT" if funnel.get("available") else "UNKNOWN"
        reason_display = reason_code or first_health("reason_display")
        if candidate_filter is not None:
            reason_display = reason_code
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
            "capacity_excluded_candidates": capacity_excluded,
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
                if "rolling_portfolio" not in result:
                    result["rolling_portfolio"] = self.rolling_portfolio_data()
            return result
        market_scope_funnel: Mapping[str, Any] = {}
        if self.store is None or not callable(getattr(self.store, "dashboard_overview_summary", None)):
            market_scope_funnel = self.market_scope_funnel_data()
            candidate_records = self._bounded_candidate_lifecycle()
            candidate_ids = [
                str(item.get("candidate_id") or "").strip()
                for item in candidate_records
                if str(item.get("candidate_id") or "").strip()
                and str(item.get("stage") or "").strip().upper() in _CANARY_ELIGIBLE_STAGES
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
                "rolling_portfolio": self.rolling_portfolio_data(),
                "live_execution": False,
            }
        aggregate = self.store.dashboard_overview_summary(activity_limit=8)
        market_scope_funnel = self.market_scope_funnel_data(public=False)
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
        activity_fallback = self._bounded_health_from_activity(aggregate)
        if activity_fallback:
            if not health:
                health = activity_fallback
            else:
                health = {
                    **activity_fallback,
                    **health,
                    "reason_code": health.get("reason_code") or activity_fallback["reason_code"],
                    "reasons": health.get("reasons") or activity_fallback["reasons"],
                    "current_failures": health.get("current_failures") or activity_fallback["current_failures"],
                }
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
        if _scan_count_is_zero(canary_status.get("actionable_candidates_found")) or _scan_count_is_zero(
            autonomous.get("actionable_candidates_found")
        ):
            for target in (canary_status, autonomous):
                target["selected_actionable_candidate"] = None
                target["selected_actionable_rank"] = None
                target["selected_actionable_score"] = None
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
            and str(item.get("stage") or "").strip().upper() in _CANARY_ELIGIBLE_STAGES
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
            "rolling_portfolio": self.rolling_portfolio_data(),
            "canary_signal": latest_signal,
            "forward_evidence": forward_evidence,
            "research_progress": research_progress,
            "market_scope_funnel": _public_market_scope_funnel(market_scope_funnel),
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
        market_scope_funnel = self.market_scope_funnel_data(public=False)
        candidate_records = self._bounded_candidate_lifecycle()
        candidate_ids = [
            str(item.get("candidate_id") or "").strip()
            for item in candidate_records
            if str(item.get("candidate_id") or "").strip()
            and str(item.get("stage") or "").strip().upper() in _CANARY_ELIGIBLE_STAGES
        ]
        forward_evidence = self._required_forward_evidence(
            market_scope_funnel,
            candidate_ids=candidate_ids,
        )
        canary["blocker"] = blocker
        canary["last_cycle_blocker"] = raw_blocker
        canary["signal_scan_reason_counts"] = signal_scan_reason_counts
        autonomous["blocker"] = blocker
        autonomous["last_cycle_blocker"] = raw_blocker
        autonomous["signal_scan_reason_counts"] = signal_scan_reason_counts
        canary["autonomous"] = autonomous
        if _scan_count_is_zero(canary.get("actionable_candidates_found")) or _scan_count_is_zero(
            autonomous.get("actionable_candidates_found")
        ):
            for target in (canary, autonomous):
                target["selected_actionable_candidate"] = None
                target["selected_actionable_rank"] = None
                target["selected_actionable_score"] = None
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
            "rolling_portfolio": self.rolling_portfolio_data(),
            "execution_authorization": self.execution_authorization_data(),
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
            "market_scope_funnel": _public_market_scope_funnel(market_scope_funnel),
            "signal_scan_reason_counts": signal_scan_reason_counts,
        }
        projection.update({name: canary[name] for name in _CANARY_STATUS_FIELDS})
        return projection
    def execution_authorization_data(self) -> dict[str, Any]:
        """Return bounded authorization/identity metadata from shared storage."""
        active: Mapping[str, Any] | None = None
        latest: Mapping[str, Any] | None = None
        loader = getattr(self.store, "load_active_execution_authorization", None)
        if callable(loader):
            try:
                value = loader(mode="EXPLORATORY_MICRO_CANARY", now=self.clock())
            except TypeError:
                value = loader()
            except Exception:
                value = None
            if isinstance(value, Mapping):
                active = value
        list_authorizations = getattr(
            self.store, "list_execution_authorizations", None
        )
        if callable(list_authorizations):
            try:
                records = list_authorizations(
                    mode="EXPLORATORY_MICRO_CANARY",
                    limit=1,
                    now=self.clock(),
                )
            except TypeError:
                records = list_authorizations(
                    mode="EXPLORATORY_MICRO_CANARY",
                    limit=1,
                )
            except Exception:
                records = ()
            if isinstance(records, (list, tuple)) and records:
                candidate = records[0]
                if isinstance(candidate, Mapping):
                    latest = candidate
                    if active is None and str(
                        candidate.get("status") or ""
                    ).upper() == "ACTIVE":
                        active = candidate
        get_config = getattr(self.store, "get_operator_config", None)
        try:
            draft = (
                get_config("execution_authorization_review", None)
                if callable(get_config)
                else None
            )
        except Exception:
            draft = None
        draft = dict(draft) if isinstance(draft, Mapping) else None
        try:
            rolling_scope_draft = (
                get_config("rolling_exploratory_scope_draft", None)
                if callable(get_config)
                else None
            )
        except Exception:
            rolling_scope_draft = None
        rolling_scope_draft = (
            dict(rolling_scope_draft)
            if isinstance(rolling_scope_draft, Mapping)
            else None
        )
        try:
            workers = (
                getattr(self.store, "list_worker_states", lambda **_: [])(limit=32)
            )
        except Exception:
            workers = []
        workers = workers if isinstance(workers, (list, tuple)) else []
        root = next(
            (
                row for row in workers
                if isinstance(row, Mapping)
                and str(row.get("worker_name") or "") == "axiom-node"
            ),
            {},
        )
        worker_payload = root.get("payload") if isinstance(root, Mapping) else {}
        worker_payload = worker_payload if isinstance(worker_payload, Mapping) else {}
        db_path = str(
            getattr(self.store, "path", None)
            or worker_payload.get("db_path")
            or ""
        )
        process_identity = worker_payload.get("process_identity")
        pid = worker_payload.get("pid")
        identity = {
            "instance_id": str(process_identity or f"axiom-node:{pid or 'unknown'}"),
            "database": db_path,
            "db_path": db_path,
            "revision": worker_payload.get("revision") or "unknown",
            "service": "axiom.canary.CanaryService",
            "service_identity": "axiom.canary.CanaryService",
            "pid": pid,
            "process_identity": process_identity,
        }
        risk = self.risk_settings_data()
        limits = risk.get("effective_limits", risk.get("active_limits", {}))
        limits = dict(limits) if isinstance(limits, Mapping) else {}
        usage = risk.get("usage", {}) if isinstance(risk, Mapping) else {}
        usage = dict(usage) if isinstance(usage, Mapping) else {}
        remaining = risk.get("remaining", {}) if isinstance(risk, Mapping) else {}
        remaining = dict(remaining) if isinstance(remaining, Mapping) else {}
        latest_status = (
            str(latest.get("status") or "").strip().upper()
            if isinstance(latest, Mapping)
            else ""
        )
        if (
            latest is not None
            and latest_status in {"DRAFT", "EXPIRED", "REVOKED"}
            and (
                draft is None
                or str(draft.get("status") or "").strip().upper() == "ACTIVE"
            )
        ):
            draft = dict(latest)
        draft_status = (
            str(draft.get("status") or "").strip().upper()
            if isinstance(draft, Mapping)
            else ""
        )
        if isinstance(active, Mapping):
            status = str(active.get("status") or "ACTIVE").upper()
        elif latest_status in {"DRAFT", "EXPIRED", "REVOKED", "UNKNOWN"}:
            status = latest_status
        elif draft_status in {"DRAFT", "EXPIRED", "REVOKED", "UNKNOWN"}:
            status = draft_status
        elif draft is not None and "status" in draft:
            status = "UNKNOWN"
        else:
            status = "DISABLED"
        authorization = active or latest or draft
        authorization_id = (
            authorization.get("authorization_id") or authorization.get("id")
            if isinstance(authorization, Mapping)
            else None
        )
        mode = (
            str(authorization.get("mode") or "").strip().upper()
            if isinstance(authorization, Mapping)
            else ""
        ) or "EXPLORATORY_MICRO_CANARY"
        generation_value = (
            authorization.get("generation")
            if isinstance(authorization, Mapping)
            else None
        )
        try:
            generation = (
                int(generation_value)
                if generation_value is not None
                and not isinstance(generation_value, bool)
                else None
            )
        except (TypeError, ValueError, OverflowError):
            generation = None
        if generation is not None and generation <= 0:
            generation = None
        lease: Mapping[str, Any] = {}
        lease_loader = getattr(self.store, "load_canary_controller_lease", None)
        if callable(lease_loader):
            try:
                value = lease_loader(now=self.clock())
            except TypeError:
                value = lease_loader()
            except Exception:
                value = None
            if isinstance(value, Mapping):
                lease = dict(value)
        return _bounded_value(
            {
                "status": status,
                "mode": mode,
                "generation": generation,
                "active": (
                    _authorization_public_projection(active)
                    if active is not None
                    else None
                ),
                "authorization": (
                    _authorization_public_projection(authorization)
                    if isinstance(authorization, Mapping)
                    else None
                ),
                "authorization_id": str(authorization_id).strip() if authorization_id else None,
                "rolling_exploratory_scope_draft": (
                    _safe_value(rolling_scope_draft)
                    if rolling_scope_draft is not None
                    else None
                ),
                "scope_draft": (
                    _safe_value(rolling_scope_draft)
                    if rolling_scope_draft is not None
                    else None
                ),
                "controller_lease": _safe_value(lease),
                "draft": (
                    _authorization_public_projection(draft)
                    if draft is not None
                    else None
                ),
                "identity": _safe_value(identity),
                "instance": _safe_value(identity),
                "economic_policy": {
                    "limits": limits,
                    "usage": usage,
                    "remaining": remaining,
                    "daily": {
                        "submissions": {
                            "limit": limits.get("max_submitted_orders_per_day"),
                            "used": usage.get("submitted_orders"),
                            "remaining": remaining.get("submitted_orders"),
                        },
                        "buy_usd": {
                            "limit": limits.get("max_gross_daily_buy_usd"),
                            "used": usage.get("gross_daily_buy_usd"),
                            "remaining": remaining.get("gross_daily_buy_usd"),
                        },
                    },
                    "lifetime": {
                        "buy_usd": {
                            "limit": risk.get("cumulative_buy_cap_usd"),
                            "used": usage.get("cumulative_buy_usd"),
                            "remaining": risk.get("remaining_cumulative_buy_usd"),
                        }
                    },
                },
                "paper_only": True,
                "live_execution": False,
            }
        )

    def ui_record_data(self, kind: str, record_id: str) -> dict[str, Any] | None:
        """Return one exact, bounded persisted record for a UI deep link."""
        identifier = str(record_id or "").strip()
        kind_value = str(kind or "").strip().lower()
        if not identifier or len(identifier) > 256 or any(ord(char) < 32 for char in identifier):
            return None
        specs: dict[str, tuple[str, str, str]] = {
            "order": (
                "canary_position_requests",
                "request_id",
                "SELECT request_id,position_id,reservation_id,event_id,venue,market_id,token_id,side,order_id,requested_quantity,requested_price,filled_quantity,average_price,fees,status,submitted_at,updated_at,last_error,settlement_status FROM canary_position_requests WHERE request_id=?",
            ),
            "submission": (
                "canary_submission_attempts",
                "attempt_id",
                "SELECT attempt_id,intent_id,side,attempted_at,status,candidate_id,execution_authorization_id FROM canary_submission_attempts WHERE attempt_id=?",
            ),
            "reservation": (
                "canary_risk_reservations",
                "reservation_id",
                "SELECT reservation_id,intent_id,side,market_id,event_id,requested_cost,filled_cost,remaining_cost,quantity,filled_quantity,status,created_at,updated_at,released_at,candidate_id,execution_authorization_id FROM canary_risk_reservations WHERE reservation_id=?",
            ),
            "fill": (
                "canary_position_fills",
                "fill_id",
                "SELECT fill_id,request_id,position_id,quantity,price,fee,status,filled_at FROM canary_position_fills WHERE fill_id=?",
            ),
            "risk-fill": (
                "canary_risk_fills",
                "fill_id",
                "SELECT fill_id,reservation_id,quantity,price,cost,fee,filled_at,candidate_id,execution_authorization_id FROM canary_risk_fills WHERE fill_id=?",
            ),
            "position": (
                "canary_position_lots",
                "position_id",
                "SELECT position_id,reservation_id,event_id,venue,market_id,token_id,candidate_id,strategy_version_id,portfolio_selection_id,quantity,sold_quantity,cost_basis,fees,gross_proceeds,exit_fees,realized_pnl,pending_exit_quantity,status,opened_at,updated_at FROM canary_position_lots WHERE position_id=?",
            ),
            "mark": (
                "canary_equity_marks",
                "mark_id",
                "SELECT mark_id,market_id,token_id,side,quantity,mark_price,cost_basis_usd,mark_fee,observed_at,source,candidate_id FROM canary_equity_marks WHERE mark_id=?",
            ),
            "cashflow": (
                "canary_risk_cashflows",
                "flow_id",
                "SELECT flow_id,kind,amount,occurred_at,candidate_id FROM canary_risk_cashflows WHERE flow_id=?",
            ),
        }
        connection = getattr(self.store, "connection", None) if self.store is not None else None
        if connection is None:
            return None
        lock = getattr(self.store, "_lock", None)
        try:
            if kind_value == "market":
                market_query = (
                    "SELECT market_id,observed_at,metadata_hash,payload_json,source_type,created_at "
                    "FROM polymarket_markets WHERE market_id=? "
                    "ORDER BY observed_at DESC,metadata_hash DESC LIMIT 1"
                )
                snapshot_query = (
                    "SELECT snapshot_id,market_id,source_timestamp,observed_at,payload_json,quality,source_type,created_at "
                    "FROM polymarket_snapshots WHERE market_id=? "
                    "ORDER BY observed_at DESC,source_timestamp DESC,snapshot_id DESC LIMIT 1"
                )
                def exact_market() -> Any:
                    row = connection.execute(market_query, (identifier,)).fetchone()
                    source = "polymarket_markets"
                    if row is None:
                        row = connection.execute(snapshot_query, (identifier,)).fetchone()
                        source = "polymarket_snapshots"
                    return row, source
                if lock is None:
                    row, source = exact_market()
                else:
                    with lock:
                        row, source = exact_market()
                if row is None:
                    return None
                record = dict(row)
                payload = record.pop("payload_json", None)
                try:
                    decoded = json.loads(payload) if isinstance(payload, str) else payload
                except (TypeError, ValueError, json.JSONDecodeError):
                    decoded = None
                if isinstance(decoded, Mapping):
                    record["payload"] = _bounded_value(decoded)
                return {
                    "kind": kind_value,
                    "id": identifier,
                    "record": _bounded_value(record),
                    "provenance": {"source": source, "lookup": "exact market_id"},
                }
            spec = specs.get(kind_value)
            if spec is None:
                return None
            table, primary_key, query = spec
            def exact_row() -> Any:
                return connection.execute(query, (identifier,)).fetchone()
            if lock is None:
                row = exact_row()
            else:
                with lock:
                    row = exact_row()
        except (AttributeError, sqlite3.Error, TypeError, ValueError):
            return None
        if row is None:
            return None
        return {
            "kind": kind_value,
            "id": identifier,
            "record": _bounded_value(dict(row)),
            "provenance": {"source": table, "primary_key": primary_key, "lookup": "exact primary key"},
        }

    def _ui_ledger_data(self, limit: int = 64) -> dict[str, Any]:
        """Return bounded, read-only canary ledger evidence for the UI."""
        def unavailable(reason: str) -> dict[str, Any]:
            return {"status": "unavailable", "reason": reason}

        empty = {
            "source": "persisted canary ledger",
            "orders": [],
            "reservations": [],
            "fills": [],
            "round_trips": [],
            "settled_positions": [],
            "cashflows": [],
            "payouts": [],
            "inventory": [],
            "marks": [],
            "unknown_obligations": [],
            "availability": {
                "orders": unavailable("STORE_UNAVAILABLE"),
                "reservations": unavailable("STORE_UNAVAILABLE"),
                "fills": unavailable("STORE_UNAVAILABLE"),
                "round_trips": unavailable("STORE_UNAVAILABLE"),
                "settled_positions": unavailable("STORE_UNAVAILABLE"),
                "cashflows": unavailable("STORE_UNAVAILABLE"),
                "inventory": unavailable("STORE_UNAVAILABLE"),
                "marks": unavailable("STORE_UNAVAILABLE"),
                "unknown_obligations": unavailable("STORE_UNAVAILABLE"),
            },
        }
        if self.store is None:
            return empty
        connection = getattr(self.store, "connection", None)
        if connection is None:
            return empty
        try:
            bounded_limit = max(1, min(int(limit), 100))
        except (TypeError, ValueError):
            bounded_limit = 64
        lock = getattr(self.store, "_lock", None)
        fetch_errors: dict[str, str] = {}

        def fetch(query: str, source: str) -> list[dict[str, Any]]:
            try:
                if lock is None:
                    rows = connection.execute(query, (bounded_limit,)).fetchall()
                else:
                    with lock:
                        rows = connection.execute(query, (bounded_limit,)).fetchall()
            except (AttributeError, sqlite3.Error) as exc:
                fetch_errors[source] = type(exc).__name__
                return []
            return [dict(row) for row in rows]

        submissions = fetch(
            """
            SELECT attempt.attempt_id,attempt.intent_id,attempt.side,
                   attempt.attempted_at,attempt.status,
                   reservation.market_id,reservation.event_id,
                   attempt.candidate_id,attempt.execution_authorization_id
            FROM canary_submission_attempts AS attempt
            LEFT JOIN canary_risk_reservations AS reservation
              ON reservation.intent_id=attempt.intent_id
            ORDER BY attempt.attempted_at DESC,attempt.attempt_id DESC
            LIMIT ?
            """,
            "submissions",
        )
        reservations = fetch(
            """
            SELECT reservation_id,intent_id,side,market_id,event_id,
                   requested_cost,filled_cost,remaining_cost,quantity,
                   filled_quantity,status,created_at,updated_at,released_at,
                   candidate_id,execution_authorization_id
            FROM canary_risk_reservations
            ORDER BY updated_at DESC,reservation_id DESC
            LIMIT ?
            """,
            "reservations",
        )
        risk_fills = fetch(
            """
            SELECT fill.fill_id,fill.reservation_id,fill.quantity,fill.price,
                   fill.cost,fill.fee,fill.filled_at,fill.candidate_id,
                   fill.execution_authorization_id,
                   reservation.market_id,reservation.event_id
            FROM canary_risk_fills AS fill
            LEFT JOIN canary_risk_reservations AS reservation
              ON reservation.reservation_id=fill.reservation_id
            ORDER BY fill.filled_at DESC,fill.fill_id DESC
            LIMIT ?
            """,
            "risk_fills",
        )
        marks = fetch(
            """
            SELECT mark_id,market_id,token_id,side,quantity,mark_price,
                   cost_basis_usd,mark_fee,observed_at,source,candidate_id
            FROM canary_equity_marks
            ORDER BY observed_at DESC,mark_id DESC
            LIMIT ?
            """,
            "marks",
        )
        position_lots = fetch(
            """
            SELECT position_id,reservation_id,event_id,venue,market_id,token_id,
                   candidate_id,strategy_version_id,portfolio_selection_id,
                   quantity,sold_quantity,cost_basis,fees,gross_proceeds,
                   exit_fees,realized_pnl,pending_exit_quantity,status,
                   opened_at,updated_at
            FROM canary_position_lots
            ORDER BY updated_at DESC,position_id DESC
            LIMIT ?
            """,
            "position_lots",
        )
        position_requests = fetch(
            """
            SELECT request_id,position_id,reservation_id,event_id,venue,market_id,
                   token_id,side,order_id,requested_quantity,requested_price,
                   filled_quantity,average_price,fees,status,submitted_at,
                   updated_at,last_error,settlement_status
            FROM canary_position_requests
            ORDER BY updated_at DESC,request_id DESC
            LIMIT ?
            """,
            "position_requests",
        )
        position_fills = fetch(
            """
            SELECT fill_id,request_id,position_id,quantity,price,fee,status,
                   filled_at
            FROM canary_position_fills
            ORDER BY filled_at DESC,fill_id DESC
            LIMIT ?
            """,
            "position_fills",
        )
        cashflows = fetch(
            """
            SELECT flow_id,kind,amount,occurred_at,candidate_id
            FROM canary_risk_cashflows
            ORDER BY occurred_at DESC,flow_id DESC
            LIMIT ?
            """,
            "cashflows",
        )

        def with_link(rows: list[dict[str, Any]], kind: str, key: str) -> list[dict[str, Any]]:
            return [
                {**row, "record_kind": kind, "record_id": row.get(key)}
                for row in rows
                if row.get(key) not in (None, "")
            ]

        submissions = with_link(submissions, "submission", "attempt_id")
        reservations = with_link(reservations, "reservation", "reservation_id")
        risk_fills = with_link(risk_fills, "risk-fill", "fill_id")
        position_requests = with_link(position_requests, "order", "request_id")
        position_fills = with_link(position_fills, "fill", "fill_id")
        position_lots = with_link(position_lots, "position", "position_id")
        marks = with_link(marks, "mark", "mark_id")
        cashflows = with_link(cashflows, "cashflow", "flow_id")
        orders = submissions + position_requests
        fills = risk_fills + position_fills
        closed_lots = [
            row for row in position_lots
            if str(row.get("status") or "").upper() == "CLOSED"
        ]
        settled_positions = [
            row for row in position_lots
            if str(row.get("status") or "").upper() == "SETTLED"
        ]
        open_lots = [
            row for row in position_lots
            if str(row.get("status") or "").upper() in {"OPEN", "EXIT_PENDING", "DUST", "MANAGEMENT_BLOCKED"}
        ]
        unknown = [
            row
            for row in reservations + orders + fills
            if str(row.get("status") or "").strip().upper() == "UNKNOWN"
        ]
        unknown = unknown[:bounded_limit]

        def availability(name: str, sources: Sequence[str], rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
            failed = [f"{source}:{fetch_errors[source]}" for source in sources if source in fetch_errors]
            if failed:
                return {"status": "unavailable", "reason": "QUERY_FAILED", "sources": failed}
            return {"status": "persisted", "count": len(rows)}

        empty.update(
            {
                "orders": orders,
                "reservations": reservations,
                "fills": fills,
                "round_trips": closed_lots,
                "settled_positions": settled_positions,
                "cashflows": cashflows,
                "payouts": [
                    row
                    for row in cashflows
                    if str(row.get("kind") or "").strip().upper()
                    in {"PAYOUT", "SETTLEMENT", "RESOLUTION"}
                ],
                "inventory": open_lots,
                "marks": marks,
                "unknown_obligations": unknown,
                "availability": {
                    "orders": availability("orders", ("submissions", "position_requests"), orders),
                    "reservations": availability("reservations", ("reservations",), reservations),
                    "fills": availability("fills", ("risk_fills", "position_fills"), fills),
                    "round_trips": availability("round_trips", ("position_lots",), closed_lots),
                    "settled_positions": availability("settled_positions", ("position_lots",), settled_positions),
                    "cashflows": availability("cashflows", ("cashflows",), cashflows),
                    "inventory": availability("inventory", ("position_lots",), open_lots),
                    "marks": availability("marks", ("marks",), marks),
                    "unknown_obligations": availability("unknown_obligations", ("reservations", "submissions", "position_requests", "risk_fills", "position_fills"), unknown),
                },
            }
        )
        return empty

    def ui_state_data(self) -> dict[str, Any]:
        """Return the safe read-only projection consumed by the UI shell."""
        actions: list[dict[str, Any]] = []
        get_config = getattr(self.store, "get_operator_config", None)
        try:
            raw_actions = get_config("operator_action_state", {}) if callable(get_config) else {}
        except Exception:
            raw_actions = {}
        raw_actions = raw_actions if isinstance(raw_actions, Mapping) else {}
        entries = raw_actions.get("actions", [])
        for entry in reversed(entries[-32:]) if isinstance(entries, list) else []:
            if not isinstance(entry, Mapping):
                continue
            result = entry.get("result")
            result = result if isinstance(result, Mapping) else {}
            def safe_text(value: Any) -> str | None:
                if value is None or isinstance(value, (dict, list, tuple, set)):
                    return None
                text = str(value).strip()
                return text[:240] if text else None
            pid = entry.get("pid")
            if isinstance(pid, bool) or not isinstance(pid, int):
                pid = None
            actions.append(
                {
                    "action_id": safe_text(entry.get("action_id")),
                    "action": safe_text(entry.get("action")),
                    "target": safe_text(entry.get("target")),
                    "status": safe_text(entry.get("status")),
                    "started_at": safe_text(entry.get("started_at")),
                    "completed_at": safe_text(entry.get("completed_at")),
                    "pid": pid,
                    "reason": safe_text(entry.get("reason")),
                    "result": {
                        key: result.get(key) if key in {"ok", "generation"} else safe_text(result.get(key))
                        for key in ("ok", "reason", "status", "action_id", "authorization_id", "generation")
                        if key in result
                    },
                }
            )
        node: dict[str, Any] = {}
        try:
            workers = self.store.list_worker_states(limit=32)
        except Exception:
            workers = []
        for worker in workers if isinstance(workers, (list, tuple)) else ():
            if not isinstance(worker, Mapping) or str(worker.get("worker_name") or "") != "axiom-node":
                continue
            payload = worker.get("payload")
            payload = payload if isinstance(payload, Mapping) else {}
            node = {
                "worker_name": "axiom-node",
                "status": worker.get("status") or payload.get("status"),
                "state": worker.get("state") or payload.get("state"),
                "heartbeat_at": worker.get("heartbeat_at") or payload.get("heartbeat_at"),
                "reason": worker.get("reason") or payload.get("reason"),
            }
            break
        configured_operator = self._configured("operator")
        if not isinstance(configured_operator, Mapping):
            configured_operator = {}
        configured_controls = configured_operator.get("operator_controls")
        if not isinstance(configured_controls, Mapping):
            configured_controls = self._configured("operator_controls")
        configured_controls = (
            self._operator_controls_projection(configured_controls)
            if isinstance(configured_controls, Mapping)
            else {}
        )
        authorization = _bounded_value(self.execution_authorization_data())
        risk_settings = _bounded_value(self.risk_settings_data())
        canary = _bounded_value(self.canary_data())
        if isinstance(configured_operator.get("execution_authorization"), Mapping):
            authorization = _bounded_value(configured_operator["execution_authorization"])
        if isinstance(configured_operator.get("risk_settings"), Mapping):
            risk_settings = _bounded_value(configured_operator["risk_settings"])
        if isinstance(configured_operator.get("canary"), Mapping):
            canary = _bounded_value(configured_operator["canary"])
        rolling = canary.get("rolling_portfolio") if isinstance(canary, Mapping) else None
        if isinstance(rolling, Mapping):
            selection = rolling.get("selection")
            selection = selection if isinstance(selection, Mapping) else {}
            rows = rolling.get("active_rows")
            rows = rows if isinstance(rows, list) else []
            native_members: list[dict[str, Any]] = []
            for raw in rows[:16]:
                if not isinstance(raw, Mapping):
                    continue
                member = {
                    key: raw.get(key)
                    for key in (
                        "candidate_id",
                        "strategy_version_id",
                        "research_trial_id",
                        "setup_id",
                        "setup_hash",
                        "scope_hash",
                        "scope_version",
                        "allocation",
                        "proposed_allocation",
                        "allocation_active",
                        "status",
                    )
                    if raw.get(key) not in (None, "")
                }
                candidate_id = member.get("candidate_id")
                if candidate_id and self.store is not None:
                    try:
                        lifecycle = self.store.load_candidate_lifecycle(str(candidate_id))
                    except Exception:
                        lifecycle = None
                    payload = lifecycle.get("payload") if isinstance(lifecycle, Mapping) else {}
                    if isinstance(payload, Mapping):
                        for key in ("strategy_name", "setup_name", "family", "experiment_family"):
                            if payload.get(key) not in (None, ""):
                                member[key] = payload[key]
                if member:
                    native_members.append(member)
            current_review = {
                "status": str(rolling.get("status") or selection.get("status") or "UNKNOWN").upper(),
                "proposal_status": str(selection.get("status") or rolling.get("status") or "UNKNOWN").upper(),
                "members": native_members,
                "proposal": {
                    "status": str(selection.get("status") or "UNKNOWN").upper(),
                    "selection_id": selection.get("portfolio_selection_id") or selection.get("selection_id"),
                    "selection_hash": selection.get("selection_hash"),
                    "members": native_members,
                    "proposed_allocation_total": rolling.get("allocation_review", {}).get("proposed_total") if isinstance(rolling.get("allocation_review"), Mapping) else None,
                },
                "authorization_bindings": {
                    key: selection.get(key)
                    for key in ("portfolio_selection_id", "selection_id", "selection_hash", "policy_id", "policy_version", "config_hash")
                    if selection.get(key) not in (None, "")
                },
                "provenance": "DashboardData.canary_data.rolling_portfolio persisted selection",
            }
            existing_review = configured_controls.get("exploratory_live_review") if isinstance(configured_controls, Mapping) else {}
            existing_review = existing_review if isinstance(existing_review, Mapping) else {}
            merged_review = dict(existing_review)
            merged_review.update(current_review)
            configured_controls = dict(configured_controls) if isinstance(configured_controls, Mapping) else {}
            configured_controls["exploratory_live_review"] = merged_review
        # The canonical exploratory proposal is the immutable selection pointed
        # to by rolling_exploratory_proposal, not rolling_portfolio.active_rows.
        proposal_pointer = get_config("rolling_exploratory_proposal", None) if callable(get_config) else None
        proposal_pointer = proposal_pointer if isinstance(proposal_pointer, Mapping) else {}
        proposal_selection_id = str(
            proposal_pointer.get("selection_id")
            or proposal_pointer.get("portfolio_selection_id")
            or ""
        ).strip()
        proposal_selection = None
        proposal_hash_valid = False
        if proposal_selection_id and self.store is not None:
            try:
                candidate_selection = self.store.load_portfolio_selection(proposal_selection_id)
            except Exception:
                candidate_selection = None
            if isinstance(candidate_selection, Mapping):
                expected_hash = str(proposal_pointer.get("selection_hash") or "").strip()
                actual_hash = str(candidate_selection.get("selection_hash") or "").strip()
                proposal_hash_valid = bool(expected_hash and actual_hash and expected_hash == actual_hash)
                if proposal_hash_valid:
                    proposal_selection = candidate_selection
            setup_fields = (
                "name", "strategy_name", "setup_name", "family", "experiment_family",
                "candidate_id", "research_trial_id", "allocation", "proposed_allocation",
                "allocation_active", "status",
                "strategy_id", "strategy_version", "strategy_version_id", "strategy_hash",
                "setup_id", "setup_version", "setup_hash", "operational_setup_hash",
                "entry_predicate", "entry", "outcome_mapping", "direction", "sizing", "description",
                "holding", "holding_semantics",
                "exit", "exit_semantics", "lookback", "parameters", "strategy", "strategy_document",
                "operational_setup", "setup_policy", "scope", "market_scope", "scope_restrictions",
                "restrictions", "exclusions", "excluded_markets", "frozen_scope",
            )
            def enrich_review_setup(raw: Mapping[str, Any]) -> dict[str, Any]:
                result = {
                    key: raw.get(key)
                    for key in setup_fields
                    if raw.get(key) not in (None, "")
                }
                nested_sources: list[Mapping[str, Any]] = []
                for key in ("setup", "operational_setup", "strategy", "strategy_document", "setup_policy"):
                    value = raw.get(key)
                    if isinstance(value, Mapping):
                        nested_sources.append(value)
                for source in tuple(nested_sources):
                    parameters = source.get("parameters")
                    if isinstance(parameters, Mapping):
                        nested_sources.append(parameters)
                strategy_version_id = str(result.get("strategy_version_id") or "").strip()
                expected_version = str(result.get("strategy_version") or "").strip()
                expected_hash = str(result.get("strategy_hash") or "").strip()
                resolved_strategy: Mapping[str, Any] | None = None
                if self.store is not None:
                    try:
                        if strategy_version_id:
                            candidate = self.store.load_strategy_version(strategy_version_id)
                        else:
                            strategy_id = str(result.get("strategy_id") or "").strip()
                            candidate = (
                                self.store.load_strategy(strategy_id, expected_version)
                                if strategy_id and expected_version
                                else None
                            )
                    except Exception:
                        candidate = None
                    if isinstance(candidate, Mapping):
                        actual_version = str(candidate.get("version") or candidate.get("strategy_version") or "").strip()
                        candidate_hashes = {
                            str(candidate.get(key) or "").strip()
                            for key in ("strategy_hash", "code_hash", "config_hash", "hash")
                        }
                        if (
                            (expected_version and expected_version != actual_version)
                            or (expected_hash and expected_hash not in candidate_hashes)
                        ):
                            candidate = None
                    if isinstance(candidate, Mapping):
                        resolved_strategy = candidate
                        nested_sources.insert(0, candidate)
                        parameters = candidate.get("parameters")
                        if isinstance(parameters, Mapping):
                            nested_sources.insert(1, parameters)
                for source in nested_sources:
                    for key in setup_fields:
                        if result.get(key) in (None, "") and source.get(key) not in (None, ""):
                            result[key] = source[key]
                if result.get("entry_predicate") in (None, "") and result.get("entry") not in (None, ""):
                    result["entry_predicate"] = result["entry"]
                if result.get("exit_semantics") in (None, "") and result.get("exit") not in (None, ""):
                    result["exit_semantics"] = result["exit"]
                if resolved_strategy:
                    setup_source = resolved_strategy.get("operational_setup") or resolved_strategy.get("setup")
                    if isinstance(setup_source, Mapping):
                        expected_setup_version = str(result.get("setup_version") or "").strip()
                        actual_setup_version = str(setup_source.get("setup_version") or setup_source.get("version") or "").strip()
                        expected_setup_hash = str(result.get("setup_hash") or result.get("operational_setup_hash") or "").strip()
                        actual_setup_hash = str(setup_source.get("setup_hash") or setup_source.get("hash") or setup_source.get("operational_setup_hash") or "").strip()
                        if (
                            (expected_setup_version and expected_setup_version != actual_setup_version)
                            or (expected_setup_hash and expected_setup_hash != actual_setup_hash)
                        ):
                            setup_source = None
                        if isinstance(setup_source, Mapping):
                            for key in setup_fields:
                                if result.get(key) in (None, "") and setup_source.get(key) not in (None, ""):
                                    result[key] = setup_source[key]
                return result

        if isinstance(proposal_selection, Mapping):
            raw_members = proposal_selection.get("members", proposal_selection.get("selected_members", []))
            raw_members = raw_members if isinstance(raw_members, (list, tuple)) else []
            proposal_members: list[dict[str, Any]] = []
            for raw in raw_members[:16]:
                member = enrich_review_setup(raw)
                proposal_members.append(member)
            proposal_setups = proposal_selection.get("selected_setups") or proposal_selection.get("setups") or []
            if not isinstance(proposal_setups, (list, tuple)):
                proposal_setups = []
            proposal_setups = [
                enrich_review_setup(raw) if isinstance(raw, Mapping) else raw
                for raw in proposal_setups[:16]
            ]
            if not proposal_setups:
                proposal_setups = [
                    enrich_review_setup(raw)
                    for raw in raw_members[:16]
                    if isinstance(raw, Mapping)
                ]
            proposal_limits = risk_settings.get("effective_limits", risk_settings.get("active_limits", {}))
            proposal_limits = proposal_limits if isinstance(proposal_limits, Mapping) else {}
            proposal_readiness = proposal_selection.get("readiness") or proposal_selection.get("blockers") or {}
            proposal_adverse = proposal_selection.get("adverse_evidence") or proposal_selection.get("adverse") or []
            if not isinstance(proposal_adverse, (list, tuple, Mapping)):
                proposal_adverse = []
            proposal_review = {
                "status": str(proposal_selection.get("status") or "UNACTIVATED").upper(),
                "proposal_status": str(proposal_selection.get("status") or "UNACTIVATED").upper(),
                "members": proposal_members,
                "scope": _bounded_value(proposal_selection.get("scope") or proposal_selection.get("market_scope") or {}),
                "scope_binding": {
                    key: proposal_selection.get(key) or proposal_pointer.get(key)
                    for key in (
                        "scope_draft_id", "scope_draft_hash", "scope_draft_version",
                        "scope_hash", "scope_version", "active_scope_hash",
                        "active_scope_version", "frozen_scope_hash", "frozen_scope_version",
                    )
                    if proposal_selection.get(key) is not None or proposal_pointer.get(key) is not None
                },
                "selected_setups": _bounded_value(list(proposal_setups)[:16]),
                "adverse_evidence": _bounded_value(proposal_adverse),
                "limits": _bounded_value(proposal_limits),
                "affordability": _bounded_value(proposal_selection.get("affordability") or proposal_selection.get("outcome_limits") or risk_settings.get("remaining") or {}),
                "readiness": _bounded_value(proposal_readiness),
                "blockers": _bounded_value(proposal_selection.get("blockers") or []),
                "setup_bindings": _bounded_value(proposal_selection.get("setup_bindings") or proposal_selection.get("draft_member_bindings") or []),
                "proposal": {
                    "status": str(proposal_selection.get("status") or "UNACTIVATED").upper(),
                    "selection_id": proposal_selection.get("selection_id") or proposal_selection.get("portfolio_selection_id") or proposal_selection_id,
                    "selection_hash": proposal_selection.get("selection_hash") or proposal_pointer.get("selection_hash"),
                    "policy_id": proposal_selection.get("policy_id") or proposal_pointer.get("policy_id"),
                    "policy_version": proposal_selection.get("policy_version") or proposal_pointer.get("policy_version"),
                    "policy_hash": proposal_selection.get("policy_hash") or proposal_pointer.get("policy_hash"),
                    "scope_draft_id": proposal_selection.get("scope_draft_id") or proposal_pointer.get("scope_draft_id"),
                    "scope_draft_version": proposal_selection.get("scope_draft_version") or proposal_pointer.get("scope_draft_version"),
                    "scope_draft_hash": proposal_selection.get("scope_draft_hash") or proposal_pointer.get("scope_draft_hash"),
                    "proposed_allocation_total": proposal_selection.get("proposed_allocation_total"),
                    "proposed_allocation_risk_digest": proposal_selection.get("proposed_allocation_risk_digest"),
                    "members": proposal_members,
                },
                "authorization_bindings": {
                    key: proposal_selection.get(key) or proposal_pointer.get(key)
                    for key in (
                        "selection_id", "selection_hash", "policy_id", "policy_version",
                        "policy_hash", "scope_draft_id", "scope_draft_hash", "scope_draft_version",
                        "active_settings_hash", "active_settings_generation",
                    )
                    if proposal_selection.get(key) is not None or proposal_pointer.get(key) is not None
                },
                "provenance": "store.load_portfolio_selection via rolling_exploratory_proposal pointer",
            }
            existing_review = configured_controls.get("exploratory_live_review") if isinstance(configured_controls, Mapping) else {}
            existing_review = existing_review if isinstance(existing_review, Mapping) else {}
            for field in ("selected_setups", "adverse_evidence", "limits", "affordability", "readiness", "blockers", "scope", "setup_bindings"):
                if proposal_review.get(field) in (None, "", [], {}):
                    proposal_review[field] = _bounded_value(existing_review.get(field))
            configured_controls = dict(configured_controls) if isinstance(configured_controls, Mapping) else {}
            configured_controls["exploratory_live_review"] = {**dict(existing_review), **proposal_review}
        elif proposal_selection_id:
            configured_controls = dict(configured_controls) if isinstance(configured_controls, Mapping) else {}
            configured_controls["exploratory_live_review"] = {
                "status": "UNAVAILABLE",
                "proposal_status": "UNAVAILABLE",
                "members": [],
                "proposal": {
                    "status": "UNAVAILABLE",
                    "selection_id": proposal_selection_id,
                    "selection_hash": proposal_pointer.get("selection_hash"),
                    "members": [],
                },
                "blockers": ["EXPLORATORY_LIVE_PROPOSAL_BINDING_STALE"],
                "provenance": "rolling_exploratory_proposal pointer unresolved or selection hash mismatch",
            }
        if callable(get_config):
            try:
                review_draft = get_config("execution_authorization_review", None)
            except Exception:
                review_draft = None
            if isinstance(review_draft, Mapping):
                choices = {
                    key: _safe_value(review_draft.get(key))
                    for key in (
                        "purpose",
                        "shared_allocation",
                        "lifetime_budget",
                        "expiry_anchor",
                        "duration_seconds",
                        "expires_at",
                        "stop_rules",
                        "adverse_evidence_ack_required",
                    )
                    if key in review_draft
                }
                review = configured_controls.get("exploratory_live_review") if isinstance(configured_controls, Mapping) else {}
                review = dict(review) if isinstance(review, Mapping) else {}
                review.setdefault("status", review_draft.get("status"))
                review.setdefault("authorization", _authorization_public_projection(review_draft))
                review.setdefault("choices", choices)
                configured_controls = dict(configured_controls) if isinstance(configured_controls, Mapping) else {}
                configured_controls["exploratory_live_review"] = review
        return {
            "schema": "ui-state.v1",
            "operator_controls": configured_controls,
            "execution_authorization": authorization,
            "risk_settings": risk_settings,
            "canary": canary,
            "actions": actions,
            "node": node,
            "ledger": self._ui_ledger_data(),
            "provenance": {
                "authorization": "DashboardData.execution_authorization_data",
                "risk": "DashboardData.risk_settings_data",
                "current_review": "DashboardData.canary_data.rolling_portfolio and store.load_candidate_lifecycle",
                "canary": "DashboardData.canary_data",
                "actions": "operator_action_state",
                "ledger": "read-only canary_* tables",
            },
        }


    def _operator_controls_projection(self, value: Any) -> Any:
        """Bound controls while preserving the already-public nested review scope."""
        projected = _connectivity_safe_diagnostics(value)
        if not isinstance(value, Mapping) or not isinstance(projected, Mapping):
            return projected
        raw_review = value.get("exploratory_live_review")
        if not isinstance(raw_review, Mapping):
            return projected
        review = _connectivity_safe_diagnostics(raw_review)
        raw_scope = raw_review.get("scope")
        if isinstance(review, Mapping) and isinstance(raw_scope, Mapping):
            scope = _safe_value(raw_scope)
            if isinstance(scope, Mapping):
                scope = dict(scope)
                for section_name in ("draft", "active", "frozen"):
                    section = raw_scope.get(section_name)
                    if not isinstance(section, Mapping):
                        continue
                    section_projection = _safe_value(section)
                    if isinstance(section_projection, Mapping):
                        scope[section_name] = section_projection
                review = dict(review)
                review["scope"] = scope
        raw_readiness = raw_review.get("readiness")
        if isinstance(review, Mapping) and isinstance(raw_readiness, Mapping):
            review = dict(review)
            review["readiness"] = _connectivity_safe_diagnostics(raw_readiness)
        raw_choices = raw_review.get("choices")
        if isinstance(raw_choices, Mapping) and isinstance(review, Mapping):
            review = dict(review)
            review["choices"] = {
                key: _safe_value(raw_choices.get(key))
                for key in (
                    "purpose",
                    "shared_allocation",
                    "lifetime_budget",
                    "expiry_anchor",
                    "duration_seconds",
                    "expires_at",
                    "stop_rules",
                    "adverse_evidence_ack_required",
                )
                if key in raw_choices
            }
        if isinstance(review, Mapping) and isinstance(raw_review.get("authorization_bindings"), Mapping):
            raw_bindings = raw_review["authorization_bindings"]
            bindings: dict[str, Any] = {
                key: _connectivity_safe_diagnostics(raw_bindings[key])
                for key in (
                    "selection_id",
                    "selection_hash",
                    "policy_id",
                    "policy_version",
                    "policy_hash",
                    "proposed_allocation_total",
                    "proposed_allocation_risk_digest",
                )
                if key in raw_bindings
            }
            if "setup_bindings" in raw_bindings:
                bindings["setup_bindings"] = _public_setup_bindings(
                    raw_bindings.get("setup_bindings")
                )
            if "draft_member_bindings" in raw_bindings:
                bindings["draft_member_bindings"] = _public_draft_member_bindings(
                    raw_bindings.get("draft_member_bindings")
                )
            review = dict(review)
            review["authorization_bindings"] = bindings
        raw_authorization = raw_review.get("authorization")
        if isinstance(review, Mapping) and isinstance(raw_authorization, Mapping):
            authorization = _authorization_public_projection(raw_authorization)
            authorization_fields = (
                "status",
                "authorization_id",
                "id",
                "generation",
                "mode",
                "purpose",
                "exact_strategy_versions",
                "strategy_version_ids",
                "reviewed_selection_policy_hash",
                "selection_policy_hash",
                "selection_id",
                "selection_hash",
                "adverse_evidence_ack",
                "lifetime_budget",
                "stop_rules",
                "expires_at",
                "scope_hash",
                "scope_version",
                "scope_draft_id",
                "scope_draft_hash",
                "scope_draft_version",
                "scope",
                "active_scope_hash",
                "active_scope_version",
                "frozen_scope_hash",
                "frozen_scope_version",
                "active_settings_hash",
                "active_settings_generation",
                "proposed_allocation_total",
                "proposed_allocation_risk_digest",
                "policy_id",
                "policy_version",
                "policy_hash",
            )
            for key in authorization_fields:
                if key in raw_authorization:
                    authorization[key] = _connectivity_safe_diagnostics(raw_authorization[key])
            for nested_key in ("active", "draft", "authorization"):
                nested = raw_authorization.get(nested_key)
                if not isinstance(nested, Mapping):
                    continue
                nested_projection = _authorization_public_projection(nested)
                if "setup_bindings" in nested:
                    nested_projection["setup_bindings"] = _public_setup_bindings(
                        nested.get("setup_bindings")
                    )
                if "draft_member_bindings" in nested:
                    nested_projection["draft_member_bindings"] = _public_draft_member_bindings(
                        nested.get("draft_member_bindings")
                    )
                authorization[nested_key] = nested_projection
            review = dict(review)
            review["authorization"] = authorization
        projected = dict(projected)
        if isinstance(review, Mapping):
            projected["exploratory_live_review"] = review
        return projected


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
        control_projection = self._operator_controls_projection(controls)
        result["operator_controls"] = control_projection
        authorization = controls.get("execution_authorization")
        if not isinstance(authorization, Mapping):
            authorization = self.execution_authorization_data()
        result["execution_authorization"] = _bounded_value(authorization)
        scope_draft = controls.get(
            "rolling_exploratory_scope_draft",
            controls.get("scope_draft"),
        )
        if not isinstance(scope_draft, Mapping):
            auth_scope = (
                authorization.get("rolling_exploratory_scope_draft")
                if isinstance(authorization, Mapping)
                else None
            )
            scope_draft = auth_scope if isinstance(auth_scope, Mapping) else {}
        result["rolling_exploratory_scope_draft"] = _bounded_value(scope_draft)
        result["scope_draft"] = result["rolling_exploratory_scope_draft"]
        result["active_scope"] = _bounded_value(
            controls.get("active_scope") or controls.get("market_scope_funnel") or {}
        )
        result["identity"] = _bounded_value(
            controls.get("identity")
            or authorization.get("identity")
            if isinstance(authorization, Mapping)
            else None
        )
        result["instance"] = result["identity"]
        result["mode"] = str(
            controls.get("mode")
            or (
                "exploratory_reviewed"
                if str(authorization.get("status") or "").upper() == "ACTIVE"
                else "observing"
            )
        )
        result["revision"] = (
            result["identity"].get("revision")
            if isinstance(result.get("identity"), Mapping)
            else None
        )
        result["armed"] = (
            controls.get("armed")
            if isinstance(controls.get("armed"), bool)
            else str(controls.get("armed_state") or "").upper()
            in {"ARMED", "AUTONOMOUS_MICRO_LIVE", "LIVE"}
        )
        result["armed_state"] = controls.get(
            "armed_state",
            controls.get("mode") if str(controls.get("mode") or "").upper() in {
                "ARMED", "AUTONOMOUS_MICRO_LIVE", "LIVE"
            } else None,
        )
        result["economic_policy"] = _bounded_value(
            controls.get("economic_policy")
            or authorization.get("economic_policy", {})
            if isinstance(authorization, Mapping)
            else {}
        )
        result["policy"] = _bounded_value(
            controls.get("policy")
            or (
                result["economic_policy"].get("policy", {})
                if isinstance(result.get("economic_policy"), Mapping)
                else {}
            )
        )
        result["budgets"] = result["economic_policy"]
        result["daily_budget"] = _bounded_value(
            result["economic_policy"].get("daily", {})
            if isinstance(result.get("economic_policy"), Mapping)
            else {}
        )
        result["lifetime_budget"] = _bounded_value(
            result["economic_policy"].get("lifetime", {})
            if isinstance(result.get("economic_policy"), Mapping)
            else {}
        )
        result["strategies"] = _bounded_value(
            controls.get("strategies") or result.get("strategies") or {}
        )
        strategy_projection = result["strategies"]
        result["active_strategies"] = _bounded_value(
            strategy_projection.get("active", [])
            if isinstance(strategy_projection, Mapping)
            else []
        )
        result["suspended_strategies"] = _bounded_value(
            strategy_projection.get("suspended", [])
            if isinstance(strategy_projection, Mapping)
            else []
        )
        result["signals"] = _bounded_value(
            controls.get("signals") or result.get("signals") or {}
        )
        signal_projection = result["signals"]
        result["current_signals"] = _bounded_value(
            signal_projection.get("current", signal_projection.get("latest"))
            if isinstance(signal_projection, Mapping)
            else None
        )
        result["last_work"] = controls.get(
            "last_work", controls.get("last_evaluation", result.get("last_work"))
        )
        result["next_work"] = controls.get("next_work", result.get("next_work"))
        result["work"] = {
            "last": result.get("last_work"),
            "next": controls.get("next_work", result.get("next_work")),
        }
        result["last_review"] = controls.get(
            "last_review",
            controls.get("last_review_at", result.get("last_review")),
        )
        result["last_review_at"] = result["last_review"]
        result["next_review"] = controls.get(
            "next_review",
            controls.get("next_review_at", result.get("next_review")),
        )
        result["next_review_at"] = result["next_review"]
        result["controller_lease"] = _bounded_value(
            controls.get("controller_lease")
            or (
                authorization.get("controller_lease", {})
                if isinstance(authorization, Mapping)
                else {}
            )
        )
        result["autonomous_canary_worker"] = controls.get(
            "autonomous_canary_worker", {}
        )
        canary = result.get("canary")
        persisted_coverage = (
            result.get("coverage") if isinstance(result.get("coverage"), Mapping) else {}
        )
        control_coverage = (
            controls.get("coverage") if isinstance(controls.get("coverage"), Mapping) else {}
        )
        result["coverage"] = _bounded_value(
            _merge_operator_coverage(persisted_coverage, control_coverage)
        )
        result["qualification_coverage"] = _bounded_value(
            control_coverage or result["coverage"]
        )
        coverage_projection = result["coverage"]
        for coverage_key in (
            "historical_count",
            "historical_rows",
            "forward_count",
            "forward_rows",
        ):
            result[coverage_key] = (
                coverage_projection.get(coverage_key, 0)
                if isinstance(coverage_projection, Mapping)
                else 0
            )
        result["exclusions"] = _bounded_value(
            controls.get("exclusions")
            or (
                result["coverage"].get("exclusions", [])
                if isinstance(result.get("coverage"), Mapping)
                else []
            )
        )
        result["remaining_budgets"] = _bounded_value(
            controls.get("remaining_budgets") or controls.get("remaining") or {}
        )
        result["execution"] = _bounded_value(
            controls.get("execution") or controls.get("execution_summary") or {}
        )
        result["execution_state"] = _bounded_value(
            controls.get("execution_state")
            or (
                {"state": canary.get("control_state")}
                if isinstance(canary, Mapping)
                else {}
            )
        )
        result["blockers"] = _bounded_value(
            controls.get("blockers")
            or (
                [controls.get("blocker")]
                if controls.get("blocker") not in (None, "", "NONE")
                else []
            )
        )
        result["execution_authorization_id"] = (
            authorization.get("authorization_id")
            or authorization.get("id")
            if isinstance(authorization, Mapping)
            else None
        )
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
        result["control_status"] = control_projection
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
            persisted_coverage = (
                result.get("coverage")
                if isinstance(result.get("coverage"), Mapping)
                else {}
            )
            result.setdefault("qualification_coverage", _bounded_value(persisted_coverage))
            for coverage_key in (
                "historical_count",
                "historical_rows",
                "forward_count",
                "forward_rows",
            ):
                if coverage_key not in result:
                    result[coverage_key] = persisted_coverage.get(coverage_key, 0)
            if self.control is not None:
                result["operator_controls"] = self._operator_controls_projection(
                    self.control.status()
                )
                controls = result["operator_controls"]
                authorization = controls.get("execution_authorization")
                if not isinstance(authorization, Mapping):
                    authorization = self.execution_authorization_data()
                result["execution_authorization"] = _bounded_value(authorization)
                result["execution_authorization_id"] = (
                    authorization.get("authorization_id")
                    or authorization.get("id")
                    if isinstance(authorization, Mapping)
                    else None
                )
                result["identity"] = _bounded_value(
                    controls.get("identity")
                    or authorization.get("identity")
                    if isinstance(authorization, Mapping)
                    else None
                )
                result["instance"] = result["identity"]
                result["revision"] = (
                    result["identity"].get("revision")
                    if isinstance(result.get("identity"), Mapping)
                    else None
                )
                result["mode"] = str(
                    controls.get("mode")
                    or (
                        "exploratory_reviewed"
                        if str(authorization.get("status") or "").upper() == "ACTIVE"
                        else "observing"
                    )
                )
                result["armed"] = (
                    controls.get("armed")
                    if isinstance(controls.get("armed"), bool)
                    else str(controls.get("armed_state") or "").upper()
                    in {"ARMED", "AUTONOMOUS_MICRO_LIVE", "LIVE"}
                )
                result["armed_state"] = controls.get("armed_state")
                result["economic_policy"] = _bounded_value(
                    controls.get("economic_policy")
                    or authorization.get("economic_policy", {})
                    if isinstance(authorization, Mapping)
                    else {}
                )
                result["controller_lease"] = _bounded_value(
                    controls.get("controller_lease")
                    or (
                        authorization.get("controller_lease", {})
                        if isinstance(authorization, Mapping)
                        else {}
                    )
                )
                persisted_coverage = (
                    result.get("coverage")
                    if isinstance(result.get("coverage"), Mapping)
                    else {}
                )
                control_coverage = (
                    controls.get("coverage")
                    if isinstance(controls.get("coverage"), Mapping)
                    else {}
                )
                result["coverage"] = _bounded_value(
                    _merge_operator_coverage(persisted_coverage, control_coverage)
                )
                result["qualification_coverage"] = _bounded_value(
                    control_coverage or result["coverage"]
                )
                for coverage_key in (
                    "historical_count",
                    "historical_rows",
                    "forward_count",
                    "forward_rows",
                ):
                    result[coverage_key] = result["coverage"].get(coverage_key, 0)
                result["exclusions"] = _bounded_value(
                    controls.get("exclusions")
                    or (
                        result["coverage"].get("exclusions", [])
                        if isinstance(result.get("coverage"), Mapping)
                        else []
                    )
                )
                result["remaining_budgets"] = _bounded_value(
                    controls.get("remaining_budgets") or controls.get("remaining") or {}
                )
                result["execution"] = _bounded_value(
                    controls.get("execution") or controls.get("execution_summary") or {}
                )
                result["policy"] = _bounded_value(
                    controls.get("policy")
                    or (
                        result["economic_policy"].get("policy", {})
                        if isinstance(result.get("economic_policy"), Mapping)
                        else {}
                    )
                )
                result["budgets"] = result["economic_policy"]
                result["daily_budget"] = _bounded_value(
                    result["economic_policy"].get("daily", {})
                    if isinstance(result.get("economic_policy"), Mapping)
                    else {}
                )
                result["lifetime_budget"] = _bounded_value(
                    result["economic_policy"].get("lifetime", {})
                    if isinstance(result.get("economic_policy"), Mapping)
                    else {}
                )
                result["strategies"] = _bounded_value(
                    controls.get("strategies") or result.get("strategies") or {}
                )
                strategy_projection = result["strategies"]
                result["active_strategies"] = _bounded_value(
                    strategy_projection.get("active", [])
                    if isinstance(strategy_projection, Mapping)
                    else []
                )
                result["suspended_strategies"] = _bounded_value(
                    strategy_projection.get("suspended", [])
                    if isinstance(strategy_projection, Mapping)
                    else []
                )
                result["signals"] = _bounded_value(
                    controls.get("signals") or result.get("signals") or {}
                )
                signal_projection = result["signals"]
                result["current_signals"] = _bounded_value(
                    signal_projection.get("current", signal_projection.get("latest"))
                    if isinstance(signal_projection, Mapping)
                    else None
                )
                result["last_work"] = controls.get(
                    "last_work", controls.get("last_evaluation", result.get("last_work"))
                )
                result["next_work"] = controls.get("next_work", result.get("next_work"))
                result["work"] = {
                    "last": result.get("last_work"),
                    "next": controls.get("next_work", result.get("next_work")),
                }
                result["last_review"] = controls.get(
                    "last_review",
                    controls.get("last_review_at", result.get("last_review")),
                )
                result["last_review_at"] = result["last_review"]
                result["next_review"] = controls.get(
                    "next_review",
                    controls.get("next_review_at", result.get("next_review")),
                )
                result["next_review_at"] = result["next_review"]
                result["execution_state"] = _bounded_value(
                    controls.get("execution_state")
                    or result.get("execution_state")
                    or {}
                )
                result["blockers"] = _bounded_value(
                    controls.get("blockers")
                    or (
                        [controls.get("blocker")]
                        if controls.get("blocker") not in (None, "", "NONE")
                        else result.get("blockers", [])
                    )
                )
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
        if endpoint == "ui-state":
            return self.ui_state_data()
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
    *,
    binance_nav_label: str = "BINANCE SPOT CANARY",
) -> str:
    """Return the framework-free shell; controls fetch their token in memory."""
    binance = _html_escape(str(binance_nav_label or "BINANCE SPOT CANARY"), quote=True)
    offline_css = """<style id="offline-reference-style">
html,body{margin:0;min-height:100%;background:#f7f8fb;color:#172033;font:16px/1.5 system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}
.app-shell{display:flex;min-height:100vh}.sidebar{box-sizing:border-box;width:16rem;padding:1.5rem;background:#101827;color:#f4f7fb}.brand{display:block;color:inherit;text-decoration:none}.brand small{display:block;color:#b7c1d3}.nav-groups{display:grid;gap:1.25rem;margin-top:2rem}.nav-group p{margin:.25rem 0;color:#b7c1d3;font-size:.75rem;text-transform:uppercase;letter-spacing:.08em}.nav-group a{display:block;padding:.6rem .7rem;color:#f4f7fb;border-radius:.4rem;text-decoration:none}.nav-group a[aria-disabled=true],.header-destination[aria-disabled=true]{opacity:.55;cursor:not-allowed}.app-main{flex:1;min-width:0}.shell-header{display:flex;align-items:center;gap:1rem;padding:1.25rem 2rem;border-bottom:1px solid #dce1ea;background:#fff}.shell-header h1{margin:.15rem 0 0}.menu-toggle{display:none;padding:.6rem .8rem}.shell-main{max-width:72rem;margin:0 auto;padding:2.25rem}.section{padding:1.5rem;border:1px solid #dce1ea;border-radius:.7rem;background:#fff;box-shadow:0 8px 24px #17203312}.section h2{margin-top:0}.muted{color:#5f6b7d}.offline-note{max-width:72rem;margin:1rem auto;padding:0 2.25rem;color:#5f6b7d}@media(max-width:48rem){.sidebar{position:fixed;inset:0 auto 0 0;z-index:2;transform:translateX(-100%)}.app-shell.menu-open .sidebar{transform:translateX(0)}.menu-toggle{display:inline-block}.shell-header,.shell-main{padding-left:1rem;padding-right:1rem}.offline-note{padding:0 1rem}}
</style>"""
    offline_guard = """<script>
(function () {
  function renderOfflineReference() {
    var root = document.getElementById("app");
    var content = document.getElementById("content");
    if (!root || !content) return;
    document.documentElement.classList.add("offline-reference");
    root.classList.add("offline-reference");
    content.innerHTML = '<section class="section" aria-labelledby="offline-reference-title"><p class="eyebrow">Offline reference</p><h2 id="offline-reference-title">Read-only workspace copy</h2><p>This saved page is a safe reference only. It does not load live data, request an operator token, or send controls.</p><p class="muted">Start the local Axiom dashboard to inspect current bounded projections. Navigation and control actions are intentionally disabled in this file copy.</p></section>';
    root.querySelectorAll("a[data-nav-view],a[data-route-link],a.header-destination").forEach(function (link) {
      link.setAttribute("aria-disabled", "true");
      link.setAttribute("tabindex", "-1");
      link.addEventListener("click", function (event) { event.preventDefault(); event.stopPropagation(); });
    });
    root.querySelectorAll("[data-action]").forEach(function (control) {
      control.setAttribute("aria-disabled", "true");
      if ("disabled" in control) control.disabled = true;
      control.hidden = true;
      control.addEventListener("click", function (event) { event.preventDefault(); event.stopPropagation(); });
    });
  }
  function loadLiveAssets() {
    if (!/^https?:$/.test(window.location.protocol)) { renderOfflineReference(); return; }
    var stylesheet = document.createElement("link");
    stylesheet.rel = "stylesheet";
    stylesheet.href = "/assets/styles.css";
    document.head.appendChild(stylesheet);
    var module = document.createElement("script");
    module.type = "module";
    module.src = "/assets/app.js";
    document.body.appendChild(module);
  }
  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", loadLiveAssets, { once: true });
  else loadLiveAssets();
})();
</script>"""
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  {offline_css}
  <title>AXIOM / operator console</title>
</head>
<body>
  <div id="app" class="app-shell">
    <aside class="sidebar" id="site-navigation" aria-label="Main navigation">
      <a class="brand" href="?view=home" data-nav-view="home">
        <span class="brand-mark" aria-hidden="true">A</span>
        <span><strong>Axiom</strong><small>Research and canary workspace</small></span>
      </a>
      <nav class="nav-groups">
        <div class="nav-group"><p>Operate</p>
          <a href="?view=home" data-nav-view="home">Home</a>
          <a href="?view=live&amp;section=polymarket" data-nav-view="live" data-nav-section="polymarket">Live trading</a>
          <a href="?view=portfolio&amp;section=real" data-nav-view="portfolio" data-nav-section="real">Portfolio</a>
          <a href="?view=markets" data-nav-view="markets">Markets</a>
        </div>
        <div class="nav-group"><p>Explore</p>
          <a href="?view=research&amp;section=strategies" data-nav-view="research" data-nav-section="strategies">Research</a>
          <a href="?view=activity" data-nav-view="activity">Activity</a>
        </div>
        <div class="nav-group"><p>Manage</p>
          <a href="?view=settings" data-nav-view="settings">Settings &amp; system</a>
        </div>
      </nav>
      <div class="sidebar-foot"><span class="status-dot"></span><span>Local operator UI · guarded actions</span></div>
    </aside>
    <div class="app-main">
      <header class="shell-header">
        <button class="menu-toggle button button-quiet" type="button" aria-controls="site-navigation" aria-expanded="false" data-action="toggle-menu">Menu</button>
        <div><p class="eyebrow">Axiom workspace</p><h1 id="page-title">Home</h1></div>
        <a class="header-destination" href="?view=live&amp;section=binance" data-nav-view="live" data-nav-section="binance">Binance</a>
      </header>
      <main class="shell-main">
        <div id="content"><section class="loading-state" role="status"><strong>Loading workspace…</strong><span>Reading the bounded local projections.</span></section></div>
      </main>
    </div>
  </div>
  <div class="nav-scrim" data-action="close-menu" hidden></div>
</body>
{offline_guard}
</html>"""

class _DashboardHandler(BaseHTTPRequestHandler):
    server: "_BoundDashboardServer"

    def log_message(self, format: str, *args: Any) -> None:
        return

    def _send(self, status: int, payload: Any, content_type: str = "application/json; charset=utf-8") -> None:
        try:
            body = (
                bytes(payload)
                if isinstance(payload, (bytes, bytearray))
                else payload.encode("utf-8")
                if isinstance(payload, str)
                else _http_json_bytes(payload)
            )
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
        action_name = str(body.get("action") or "").strip()
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
            "market_id",
            "token_id",
            "candidate_id",
            *_ROLLING_REVIEW_HTTP_FIELDS,
            *_ROLLING_ACTIVATE_HTTP_FIELDS,
            *_EXECUTION_AUTHORIZATION_HTTP_FIELDS,
        }
        if set(body) - allowed_fields:
            self._send(400, {"error": "unsupported control fields"})
            return
        payload = body.get("payload")
        if payload is not None and not isinstance(payload, Mapping):
            self._send(400, {"error": "control payload must be an object"})
            return
        rolling_review_actions = {
            "rolling.admission.review",
            "rolling.policy.review",
            "admission_policy.review",
        }
        rolling_activate_actions = {
            "rolling.admission.activate",
            "rolling.policy.activate",
            "admission_policy.activate",
        }
        rolling_fields = (
            _ROLLING_REVIEW_HTTP_FIELDS | _ROLLING_ACTIVATE_HTTP_FIELDS
        ) - {"values", "actor"}
        if action_name not in rolling_review_actions | rolling_activate_actions:
            if set(body) & rolling_fields:
                self._send(400, {"error": "rolling fields require a rolling action"})
                return
        execution_authorization_actions = {
            "execution_authorization.review",
            "execution_authorization.activate",
            "execution_authorization.revoke",
            "exploratory.authorization.review",
            "exploratory.authorization.activate",
            "exploratory.authorization.revoke",
            "authorization.review",
            "authorization.activate",
            "authorization.revoke",
        }
        exploratory_live_actions = {
            "exploratory.live.review_confirm",
            "exploratory_live.review_confirm",
            "canary.exploratory_live.review_confirm",
        }
        flat_fields = {
            "values",
            "actor",
            "config_id",
            "expected_generation",
            "venue",
        }
        if action_name in exploratory_live_actions:
            flat_fields |= {"actor", "market_id", "token_id", "candidate_id"}
        elif action_name in rolling_review_actions:
            flat_fields |= _ROLLING_REVIEW_HTTP_FIELDS
        elif action_name in rolling_activate_actions:
            flat_fields |= _ROLLING_ACTIVATE_HTTP_FIELDS
        elif action_name in execution_authorization_actions:
            flat_fields |= _EXECUTION_AUTHORIZATION_HTTP_FIELDS
        flat_payload = {
            name: body[name]
            for name in flat_fields
            if name in body
        }
        if payload is not None and flat_payload:
            self._send(400, {"error": "control payload must be nested"})
            return
        action_payload = dict(payload) if isinstance(payload, Mapping) else flat_payload
        payload_actions = {
            "canary.settings.save_draft",
            "risk.settings.save_draft",
            "canary.settings.activate_draft",
            "risk.settings.activate_draft",
            "canary.enable_auto",
            "canary.recover_entry",
            *rolling_review_actions,
            *rolling_activate_actions,
            *execution_authorization_actions,
            *exploratory_live_actions,
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
        asset = _UI_ASSETS.get(f"/{path}")
        if asset is not None:
            filename, content_type = asset
            try:
                body = (Path(__file__).with_name("ui") / filename).read_bytes()
            except (OSError, ValueError):
                self._send(404, {"error": "asset not found"})
                return
            self._send(200, body, content_type)
            return
        if path == "api/control-token":
            origin = self.headers.get("Origin")
            if origin and not self._same_origin(str(origin)):
                self._send(403, {"error": "same-origin dashboard required"})
                return
            self._send(200, {"token": str(self.server.control_token or "")})
            return
        if path in {"api/control", "api/binance/control"}:
            self._send(405, {"error": "POST required"})
            return
        if path in {"", "index.html"}:
            try:
                self._send(
                    200,
                    _dashboard_html(
                        binance_nav_label=self.server.dashboard_data.binance_nav_label(),
                    ),
                    "text/html; charset=utf-8",
                )
            except _CLIENT_DISCONNECT_ERRORS:
                return
            return
        query = parse_qs(parsed.query, keep_blank_values=True)
        if path == "api/ui-record":
            kinds = query.get("kind", [])
            identifiers = query.get("id", [])
            kind = kinds[0].strip().lower() if len(kinds) == 1 else ""
            identifier = identifiers[0].strip() if len(identifiers) == 1 else ""
            invalid_identifier = (
                not identifier
                or len(identifier) > 256
                or any(ord(char) < 32 for char in identifier)
            )
            if kind not in _UI_RECORD_KINDS or invalid_identifier or len(kinds) != 1 or len(identifiers) != 1:
                self._send(400, {"error": "invalid record kind or id"})
                return
            record = self.server.dashboard_data.ui_record_data(kind, identifier)
            if record is None:
                self._send(404, {"error": "record unavailable", "kind": kind, "id": identifier})
                return
            self._send(200, record)
            return
        if path.startswith("api/v2/"):
            endpoint = path[len("api/v2/") :]
            allowed = endpoint.lower() in _V2_ENDPOINTS or endpoint.lower().startswith(("datasets/", "candidates/", "hermes/", "crypto-research/", "shadow/"))
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
