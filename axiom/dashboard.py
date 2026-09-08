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
import os
from decimal import Decimal
from pathlib import Path
import re
import secrets
import sqlite3
from . import canary as canary_module
from .canary import CanaryService, _canary_eligibility_is_bound
import subprocess
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from typing import Any, Mapping
from urllib.parse import parse_qs, unquote, urlparse
from .operator import CANARY_CONNECTIVITY_CONFIG_KEY, OperatorControlPlane, _stored_connectivity_projection


_CLIENT_DISCONNECT_ERRORS = (BrokenPipeError, ConnectionAbortedError, ConnectionResetError)
_LOGGER = logging.getLogger(__name__)

from .director import research_summary
from .domain import ensure_utc, parse_timestamp, to_record


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name == "nt":
        try:
            import ctypes

            handle = ctypes.windll.kernel32.OpenProcess(0x1000, False, pid)
            if handle:
                ctypes.windll.kernel32.CloseHandle(handle)
                return True
        except (AttributeError, OSError):
            pass
    try:
        os.kill(pid, 0)
        return True
    except PermissionError:
        return True
    except OSError:
        return False
def _pid_command_line(pid: int) -> str:
    if pid <= 0:
        return ""
    if os.name == "nt":
        try:
            result = subprocess.run(
                [
                    "powershell",
                    "-NoProfile",
                    "-NonInteractive",
                    "-Command",
                    f'(Get-CimInstance Win32_Process -Filter "ProcessId={int(pid)}").CommandLine',
                ],
                capture_output=True,
                text=True,
                timeout=2.0,
                check=False,
            )
            return result.stdout.strip()
        except (OSError, subprocess.SubprocessError):
            return ""
    try:
        return Path(f"/proc/{pid}/cmdline").read_bytes().replace(b"\x00", " ").decode("utf-8", "replace").strip()
    except (OSError, UnicodeError):
        return ""


def _node_command_db(raw_command: str) -> str | None:
    tokens = [
        match.group(1) or match.group(2) or match.group(3)
        for match in re.finditer(r'"([^"]*)"|\'([^\']*)\'|([^\s]+)', raw_command)
    ]
    if not tokens:
        return None
    executable = tokens[0].replace("\\", "/").rsplit("/", 1)[-1].lower()
    if executable in {"axiom", "axiom.exe"}:
        command_index = 1
    elif executable in {"py", "py.exe"} or re.fullmatch(r"pythonw?(?:\d+(?:\.\d+)?)?(?:\.exe)?", executable):
        if len(tokens) < 4 or tokens[1].lower() != "-m" or tokens[2].lower() != "axiom.cli":
            return None
        command_index = 3
    else:
        return None
    if len(tokens) <= command_index or tokens[command_index].lower() not in {"node-run", "run-research-node"}:
        return None
    for index in range(command_index + 1, len(tokens)):
        token = tokens[index]
        lowered = token.lower()
        if lowered == "--db" and index + 1 < len(tokens):
            return tokens[index + 1]
        if lowered.startswith("--db="):
            return token[5:]
    return None


def _pid_matches_node(pid: int, db_path: str) -> bool:
    if not _pid_alive(pid):
        return False
    raw_command = _pid_command_line(pid)
    actual = _node_command_db(raw_command)
    if not actual:
        return False
    try:
        expected = os.path.normcase(os.path.abspath(db_path))
        observed = os.path.normcase(os.path.abspath(actual))
    except (OSError, TypeError, ValueError):
        return False
    return expected == observed


def _lock_owner_matches(lock_path: str, pid: int) -> bool:
    if not lock_path or pid <= 0:
        return False
    try:
        return int(Path(lock_path).read_text(encoding="utf-8").splitlines()[0].strip()) == pid
    except (OSError, TypeError, ValueError):
        return False


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
    "paper",
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
        return [_bounded_value(child, depth=depth + 1) for child in list(value)[:32]]
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
    normalized_reason = str(reason_code or "").strip().upper()
    if normalized_status not in {"ACCEPTED", "COMPLETED", "REJECTED", "FAILED"}:
        return None, None
    if normalized_reason == "DATASET_NOT_FOUND":
        return "PROPOSAL_REJECTED", "PROPOSAL REJECTED"
    if normalized_status in {"REJECTED", "FAILED"}:
        return "EXPERIMENT_REJECTED", "EXPERIMENT REJECTED"
    return "EXPERIMENT_COMPLETED", "EXPERIMENT COMPLETED"

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

_CANARY_SELECTION_STATUSES = frozenset({"CURRENT", "STALE", "NONE"})
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
    "selected_candidate",
    "last_selected_candidate",
)


def _canary_status_projection(status: Mapping[str, Any] | None) -> dict[str, Any]:
    """Normalize the immutable selection status contract for dashboard consumers."""
    source = dict(status) if isinstance(status, Mapping) else {}
    nested = source.get("autonomous")
    nested = nested if isinstance(nested, Mapping) else {}

    def value(name: str, default: Any = None) -> Any:
        # An explicit top-level ``None`` is authoritative and must not be
        # replaced by historical nested data.
        if name in source:
            return source[name]
        return nested.get(name, default)

    def count(name: str) -> int:
        try:
            return max(0, int(value(name, 0) or 0))
        except (TypeError, ValueError):
            return 0

    selection_status = str(value("selection_status", "NONE") or "NONE").strip().upper()
    if selection_status not in _CANARY_SELECTION_STATUSES:
        selection_status = "NONE"
    selection_valid = value("selection_valid", False) is True
    selected_candidate = value("selected_candidate")
    if selected_candidate is not None:
        selected_candidate = str(selected_candidate).strip() or None
    if not (selection_valid and selection_status == "CURRENT"):
        selected_candidate = None
    last_selected_candidate = value("last_selected_candidate")
    if last_selected_candidate is not None:
        last_selected_candidate = str(last_selected_candidate).strip() or None
    projection = dict(source)
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
            "selected_candidate": selected_candidate,
            "last_selected_candidate": last_selected_candidate,
        }
    )
    # ``winner_id`` remains current-only. Historical selections are represented
    # by ``last_selected_candidate`` and never become executable again.
    projection["winner_id"] = (
        selected_candidate if selection_valid and selection_status == "CURRENT" else None
    )
    if not (selection_valid and selection_status == "CURRENT"):
        projection["winner_rank"] = None
        projection["winner_score"] = None
    selected_winner = projection.get("selected_winner")
    if isinstance(selected_winner, Mapping):
        selected_winner = dict(selected_winner)
        selected_winner["selection_status"] = selection_status
        selected_winner["selection_valid"] = selection_valid
        selected_winner["selection_invalidation_reason"] = projection[
            "selection_invalidation_reason"
        ]
        projection["selected_winner"] = selected_winner
    autonomous = projection.get("autonomous")
    autonomous = dict(autonomous) if isinstance(autonomous, Mapping) else {}
    autonomous.update({name: projection[name] for name in _CANARY_STATUS_FIELDS})
    autonomous["winner_id"] = projection["winner_id"]
    if not (selection_valid and selection_status == "CURRENT"):
        autonomous["rank"] = None
        autonomous["score"] = None
    projection["autonomous"] = autonomous
    return projection


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
    ) -> None:
        self._data = dict(data or {})
        self.tracker = tracker
        self.crypto_provider = crypto_provider
        self.prediction_provider = prediction_provider
        self.evolution = evolution
        self.risk = risk
        self.store = store
        self.control = control
        self.binance_canary = binance_canary
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
        provider = self.crypto_provider
        if provider is None:
            summary = self.store.dashboard_summary() if self.store is not None else {}
            return {
                "available": bool(summary.get("bars", 0)),
                "provider": "persisted",
                "symbols": [],
                "bars": summary.get("bars", 0),
                "datasets": summary.get("datasets", 0),
                "live_execution": False,
            }
        symbols = self._data.get("crypto_symbols", ())
        tickers = {}
        provider_errors = 0
        for symbol in symbols:
            try:
                ticker = provider.ticker(symbol)
            except Exception:
                provider_errors += 1
                ticker = None
            if ticker is not None:
                tickers[symbol] = ticker
        result = {
            "available": bool(tickers),
            "provider": provider.__class__.__name__,
            "tickers": tickers,
            "provider_errors": provider_errors,
            "live_execution": False,
        }
        if provider_errors and not tickers:
            result["error"] = "crypto provider unavailable"
        return result

    def prediction(self) -> Any:
        configured = self._configured("prediction")
        if configured is not None:
            return configured
        provider = self.prediction_provider
        if provider is None:
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
        try:
            try:
                markets = provider.markets(active=True, limit=1000)
            except TypeError:
                markets = provider.markets(active=True)
            markets = list(islice(markets, 1000))
        except Exception:
            return {
                "available": False,
                "provider": provider.__class__.__name__,
                "markets": [],
                "error": "prediction provider unavailable",
                "live_execution": False,
            }
        return {
            "available": bool(markets),
            "provider": provider.__class__.__name__,
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
                quality_label, quality_context = _polymarket_quality_display(item)
                item["quality_label"] = quality_label
                item["quality_context"] = quality_context
            return result
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
        return _page_result(items[offset : offset + values["page_size"]], page=values["page"], page_size=values["page_size"], total=len(items))

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
        counts = self.store.dashboard_summary() if self.store is not None else {}
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
                    return method(identifier, dataset_version=values.get("dataset_version"), page=values["page"], page_size=values["page_size"], sort=values["sort"] or "range_index", direction=values["direction"], filter=values["filter"])
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
        workers = self.store.list_worker_states(limit=2048)
        now = ensure_utc(datetime.now().astimezone())
        db_path = str(getattr(self.store, "path", ""))
        default_lock_path = db_path + ".lock"
        identity_cache: dict[tuple[int, str], bool] = {}
        lock_cache: dict[tuple[str, int], bool] = {}
        statuses: list[str] = []
        normalized_workers: list[dict[str, Any]] = []
        crypto_error = False
        root_lock_path = default_lock_path
        stale_after_seconds = 300.0
        for row in workers:
            payload = row.get("payload") if isinstance(row, Mapping) else None
            if not isinstance(payload, Mapping):
                continue
            if isinstance(payload.get("lock_path"), str) and payload["lock_path"]:
                root_lock_path = str(payload["lock_path"])
            try:
                configured_stale_after = float(payload.get("stale_after_seconds"))
            except (TypeError, ValueError):
                configured_stale_after = stale_after_seconds
            if math.isfinite(configured_stale_after) and configured_stale_after > 0:
                stale_after_seconds = configured_stale_after
            if root_lock_path != default_lock_path or stale_after_seconds != 300.0:
                break
        worker_rows = {
            str(row.get("worker_name", "")): row
            for row in workers
            if isinstance(row, Mapping)
        }
        for row in workers:
            item = dict(row)
            worker_name = str(item.get("worker_name", ""))
            state = str(item.get("status", "unknown")).lower()
            worker_payload = item.get("payload")
            if (
                isinstance(worker_payload, Mapping)
                and isinstance(worker_payload.get("crypto_paper"), Mapping)
                and worker_payload["crypto_paper"].get("enabled")
                and worker_payload["crypto_paper"].get("last_error")
            ):
                crypto_error = True
            liveness_candidate = state == "running" or (
                state == "degraded"
                and isinstance(worker_payload, Mapping)
                and bool(worker_payload.get("lock_path"))
            )
            if liveness_candidate:
                pid = worker_payload.get("pid") if isinstance(worker_payload, Mapping) else None
                try:
                    worker_pid = int(pid)
                except (TypeError, ValueError):
                    worker_pid = 0
                identity_key = (worker_pid, db_path)
                if identity_key not in identity_cache:
                    identity_cache[identity_key] = _pid_matches_node(worker_pid, db_path)
                identity_valid = identity_cache[identity_key]
                lock_path = (
                    str(worker_payload.get("lock_path"))
                    if isinstance(worker_payload, Mapping) and worker_payload.get("lock_path")
                    else root_lock_path
                )
                lock_key = (lock_path, worker_pid)
                if lock_key not in lock_cache:
                    lock_cache[lock_key] = _lock_owner_matches(lock_path, worker_pid)
                lock_owner_valid = lock_cache[lock_key]
                alive = _pid_alive(worker_pid)
                heartbeat = parse_timestamp(item.get("heartbeat_at"))
                age = (now - heartbeat).total_seconds() if heartbeat is not None else None
                item["worker_alive"] = alive
                item["worker_identity_valid"] = identity_valid
                item["worker_lock_owner_valid"] = lock_owner_valid
                item["heartbeat_age_seconds"] = age
                liveness_failure = not alive or not identity_valid or not lock_owner_valid or age is None or age > stale_after_seconds
                watchdog_fresh = False
                if liveness_failure and (age is None or age > stale_after_seconds):
                    watchdog_row = worker_rows.get(f"{worker_name}:watchdog")
                    watchdog_payload = watchdog_row.get("payload") if isinstance(watchdog_row, Mapping) else None
                    watchdog_status = str(watchdog_row.get("status", "")).lower() if isinstance(watchdog_row, Mapping) else ""
                    watchdog_pid_value = watchdog_payload.get("pid") if isinstance(watchdog_payload, Mapping) else None
                    try:
                        watchdog_pid = int(watchdog_pid_value)
                    except (TypeError, ValueError):
                        watchdog_pid = 0
                    watchdog_heartbeat = parse_timestamp(watchdog_row.get("heartbeat_at")) if isinstance(watchdog_row, Mapping) else None
                    watchdog_age = (now - watchdog_heartbeat).total_seconds() if watchdog_heartbeat is not None else None
                    watchdog_lock_path = (
                        str(watchdog_payload.get("lock_path"))
                        if isinstance(watchdog_payload, Mapping) and watchdog_payload.get("lock_path")
                        else lock_path
                    )
                    watchdog_identity_key = (watchdog_pid, db_path)
                    if watchdog_identity_key not in identity_cache:
                        identity_cache[watchdog_identity_key] = _pid_matches_node(watchdog_pid, db_path)
                    watchdog_lock_key = (watchdog_lock_path, watchdog_pid)
                    if watchdog_lock_key not in lock_cache:
                        lock_cache[watchdog_lock_key] = _lock_owner_matches(watchdog_lock_path, watchdog_pid)
                    watchdog_fresh = (
                        watchdog_status == "running"
                        and watchdog_pid == worker_pid
                        and _pid_alive(watchdog_pid)
                        and identity_cache[watchdog_identity_key]
                        and lock_cache[watchdog_lock_key]
                        and watchdog_age is not None
                        and watchdog_age <= stale_after_seconds
                    )
                if liveness_failure:
                    state = "degraded" if watchdog_fresh and alive and identity_valid and lock_owner_valid else "stale"
                    item["status"] = state
            statuses.append(state)
            normalized_workers.append(item)
        health_rows = [
            row for row in normalized_workers
            if str(row.get("worker_name", "")) == "health-monitor"
        ]
        health_payload = health_rows[0].get("payload", {}) if health_rows else {}
        health_grade = str(health_payload.get("grade", "")).upper() if isinstance(health_payload, Mapping) else ""
        if "stale" in statuses:
            status = "stale"
        elif crypto_error or (health_grade and health_grade not in {"A", "OK", "HEALTHY"}) or "degraded" in statuses:
            status = "degraded"
        elif "running" in statuses:
            status = "running"
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
            current_health = {"grade": "F", "reason_code": "HEALTH_UNAVAILABLE", "error": str(exc)}
        health_fields = self._health_status_fields(normalized_workers, current_health if isinstance(current_health, Mapping) else {})
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

    def _candidate_rows(self) -> list[dict[str, Any]]:
        if self.store is None:
            return []
        records = self.store.load_candidate_lifecycle(limit=_MAX_SIZE_FALLBACK)
        records = records if isinstance(records, list) else []
        return [self._candidate_row(item) for item in records if isinstance(item, Mapping)]

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
        if not _canary_eligibility_is_bound(self.store, candidate_id, eligibility):
            return None
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
        candidate_id = str(item.get("candidate_id") or "").strip()
        stage = str(item.get("stage") or "").strip().upper()
        eligibility = self._candidate_canary_eligibility(candidate_id)
        try:
            validation = CanaryService(self.store, initialize=False).validate_eligibility(candidate_id)
        except Exception:
            validation = {"eligible": False, "reason_code": "CANARY_VALIDATION_UNAVAILABLE", "checks": []}
        binding_present = False
        if self.store is not None and getattr(self.store, "connection", None) is not None:
            try:
                binding_present = self.store.connection.execute(
                    "SELECT 1 FROM canary_eligibility WHERE candidate_id=?",
                    (candidate_id,),
                ).fetchone() is not None
            except sqlite3.Error:
                binding_present = False
        canary_eligible = bool(validation.get("eligible")) and (
            eligibility is not None or not binding_present
        ) and stage in _CANARY_ELIGIBLE_STAGES
        if stage == "PAPER_FORWARD":
            paper_forward_status = "ACTIVE"
            paper_status = "PAPER_FORWARD"
        elif stage == "PAPER_PROMOTABLE":
            paper_forward_status = "COMPLETE"
            paper_status = "PAPER_PROMOTABLE"
        else:
            paper_forward_status = "NOT_STARTED"
            paper_status = "NOT_STARTED"
        quality = validation.get("data_quality")
        quality = quality if isinstance(quality, Mapping) else {}
        fidelity = str(quality.get("historical_execution_fidelity") or "UNKNOWN")
        integrity = str(quality.get("historical_data_integrity") or "FAIL")
        quality_gate = (
            "PASS FOR $1 MICRO-LIVE"
            if quality.get("canary_data_quality_acceptable")
            else "NOT PASSED"
        )
        return {
            "historical_gates": "PASSED" if validation.get("eligible") else "NOT_PASSED",
            "historical_gate_reason": validation.get("reason_code"),
            "historical_data_integrity": integrity,
            "historical_execution_fidelity": (
                f"{fidelity} · LIMITED" if fidelity == "PRICE_PROXY" else fidelity
            ),
            "canary_data_quality_gate": quality_gate,
            "production_evidence": str(quality.get("production_evidence_status") or "INSUFFICIENT"),
            "canary_eligible": canary_eligible,
            "canary_eligible_at": eligibility.get("eligible_at") if canary_eligible and eligibility else None,
            "canary_status": "ELIGIBLE" if canary_eligible else "NOT_ELIGIBLE",
            "paper_forward": stage in _PAPER_FORWARD_STAGES,
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
        if self.store is not None and dataset_id and callable(getattr(self.store, "load_dataset_catalog", None)):
            try:
                catalog = self.store.load_dataset_catalog(dataset_id, dataset_version or None)
            except (AttributeError, TypeError, ValueError):
                catalog = None
            if isinstance(catalog, Mapping):
                for key in ("market_type", "instrument", "dataset_version", "source_type", "timeframe"):
                    if not values.get(key) and catalog.get(key):
                        values[key] = catalog.get(key)
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
        """Project only the columns rendered by the overview latest-candidates table."""
        row = self._candidate_row(item)
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
        states = self.store.list_paper_states(limit=1000)
        if candidate_only:
            candidate_ids = self._candidate_paper_experiment_ids()
            states = [
                item
                for item in states
                if isinstance(item, Mapping)
                and (
                    str(item.get("experiment_id") or "").strip() in candidate_ids
                    or str((item.get("state") or {}).get("candidate_id") if isinstance(item.get("state"), Mapping) else "").strip() in candidate_ids
                )
            ]
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
            key=lambda item: parse_timestamp(item.get("time") or item.get("updated_at") or item.get("created_at"))
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

    def overview_summary(self) -> dict[str, Any]:
        """Return the bounded overview payload; list views stay lazy."""
        configured = self._configured("overview-summary")
        if configured is not None:
            return dict(configured) if isinstance(configured, Mapping) else {"value": configured}
        if self.store is None or not callable(getattr(self.store, "dashboard_overview_summary", None)):
            return {
                "available": False,
                "components": [],
                "research_cards": {},
                "coverage": {},
                "latest_activity": [],
                "live_execution": False,
            }
        aggregate = self.store.dashboard_overview_summary(activity_limit=8)
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
        health_grade = str(health_payload.get("grade") or "").upper() or None
        collector_state = self.store.get_collector_state("polymarket") or {}
        collector_state = collector_state if isinstance(collector_state, Mapping) else {}
        collector_detail = {
            "grade": health_payload.get("grade") or collector_worker_payload.get("grade"),
            "reason_code": health_payload.get("reason_code"),
            "reasons": _bounded_value(health_payload.get("reasons", [])),
            "collection_errors": health_payload.get("collection_errors", 0),
            "top_failure_codes": _bounded_value(health_payload.get("top_failure_codes", [])),
            "stale_market_count": health_payload.get("stale_market_count", len(health_payload.get("stale_markets", []))),
            "gap_count": health_payload.get("gap_count", len(health_payload.get("gaps", []))),
            "configured_interval_seconds": (
                health_payload.get("configured_interval_seconds")
                or collector_worker_payload.get("configured_interval_seconds")
                or collector_state.get("configured_interval_seconds")
                or 60.0
            ),
            "effective_collection_cadence_seconds": health_payload.get("effective_collection_cadence_seconds"),
            "stale_after_seconds": (
                health_payload.get("stale_after_seconds")
                or collector_worker_payload.get("stale_after_seconds")
                or collector_state.get("stale_after_seconds")
                or 180.0
            ),
            "last_cycle_duration_seconds": (
                collector_worker_payload.get("last_cycle_duration_seconds")
                or collector_state.get("last_cycle_duration_seconds")
                or health_payload.get("last_cycle_duration_seconds")
            ),
            "last_cycle_started_at": (
                collector_worker_payload.get("last_cycle_started_at")
                or collector_state.get("last_cycle_started_at")
                or health_payload.get("last_cycle_started_at")
            ),
            "last_cycle_ended_at": (
                collector_worker_payload.get("last_cycle_ended_at")
                or collector_state.get("last_cycle_ended_at")
                or health_payload.get("last_cycle_ended_at")
            ),
            "markets_attempted": collector_worker_payload.get("last_cycle_markets_attempted", collector_state.get("markets_attempted", 0)),
            "markets_successful": collector_worker_payload.get("last_cycle_markets_successful", collector_state.get("markets_successful", 0)),
            "markets_failed": collector_worker_payload.get("last_cycle_markets_failed", collector_state.get("markets_failed", 0)),
            "last_cycle_markets_attempted": collector_worker_payload.get("last_cycle_markets_attempted", collector_state.get("markets_attempted", 0)),
            "last_cycle_markets_successful": collector_worker_payload.get("last_cycle_markets_successful", collector_state.get("markets_successful", 0)),
            "last_cycle_markets_failed": collector_worker_payload.get("last_cycle_markets_failed", collector_state.get("markets_failed", 0)),
            "scheduled_market_count": (
                len(collector_state.get("scheduled_market_ids", []))
                if isinstance(collector_state.get("scheduled_market_ids"), (list, tuple))
                else collector_worker_payload.get("scheduled_market_count", health_payload.get("scheduled_market_count", 0))
            ),
            "last_successful_cycle": health_payload.get("last_successful_cycle") or collector_worker_payload.get("last_successful_collection_at"),
            "next_scheduled_collection_at": (
                collector_worker_payload.get("next_scheduled_collection_at")
                or collector_state.get("next_scheduled_collection_at")
            ),
            "worker_heartbeat_at": collector_worker_payload.get("worker_heartbeat_at") or collector_state.get("worker_heartbeat_at"),
        }
        health_reason = (
            health_payload.get("degrading_reason")
            or health_payload.get("reason")
            or health_payload.get("last_error")
            or health_payload.get("reason_code")
        )

        def worker_state(name: str, default: str = "NOT INITIALIZED") -> str:
            item = worker_map.get(name, {})
            status = str(item.get("status") or "").upper()
            return status or default

        crypto_ready = int(catalog.get("historical", {}).get("datasets", 0) or 0) > 0 if isinstance(catalog.get("historical"), Mapping) else False
        canary_service = CanaryService(self.store, initialize=False)
        canary_status = _canary_status_projection(canary_service.status())
        latest_signal = canary_service.latest_signal()
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
        historical = catalog.get("historical", {}) if isinstance(catalog, Mapping) else {}
        forward = catalog.get("forward_collected", {}) if isinstance(catalog, Mapping) else {}
        candidate_canary_count = self._candidate_canary_eligibility_count()
        latest_candidates: list[dict[str, Any]] = []
        try:
            candidate_page = self.paginate_candidate_lifecycle(
                {"page": 1, "page_size": 10, "sort": "updated_at", "direction": "desc"}
            )
            latest_candidates = [
                self._compact_candidate_display_row(item)
                for item in candidate_page.get("items", [])
                if isinstance(item, Mapping)
            ][:10]
        except (AttributeError, TypeError, ValueError, sqlite3.Error):
            latest_candidates = []
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
        collector_default_state = "READY" if health_grade in {"A", "OK", "HEALTHY"} else (health_grade or "NOT INITIALIZED")
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
                "name": "HERMES",
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
            "activity": aggregate.get("latest_activity", []),
            "latest_activity": aggregate.get("latest_activity", []),
            "latest_candidates": latest_candidates,
            "candidates": latest_candidates,
            "lifecycle_funnel": stages,
            "candidate_status": {
                "canary_eligible": candidate_canary_count,
                "rankable": canary_status.get("rankable_count", 0) if isinstance(canary_status, Mapping) else 0,
                "paper_forward": stages.get("PAPER_FORWARD", 0) + stages.get("PAPER_PROMOTABLE", 0),
                "paper_promotable": stages.get("PAPER_PROMOTABLE", 0),
            },
            "hermes": {"statuses": queue_statuses, "latest_outcome": latest_outcome},
            "hermes_latest_outcome": latest_outcome,
            "canary": canary_status,
            "canary_signal": latest_signal,
            "collector_health": collector_detail,
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
        credentials = canary_module.CredentialStore().safe_projection(allow_environment=False)
        if self.store is None:
            canary: Mapping[str, Any] = {
                "production_live_trading": "DISABLED",
                "micro_live_canary": "DISABLED",
                "display_state": "DISABLED",
                "control_state": "DISABLED",
                "candidate": None,
                "winner_id": None,
                "winner_rank": None,
                "winner_score": None,
                "selection_reason": None,
                "eligibility_raw_count": 0,
                "eligible_count": 0,
                "rankable_raw_count": 0,
                "rankable_count": 0,
                "ranking_run_id": None,
                "ranking_timestamp": None,
                "selection_status": "NONE",
                "selection_valid": False,
                "selection_invalidation_reason": None,
                "selected_candidate": None,
                "last_selected_candidate": None,
                "historical_data_integrity": "UNKNOWN",
                "historical_execution_fidelity": "UNKNOWN",
                "current_execution_evidence": "CURRENT_ORDER_BOOK_REQUIRED",
                "risk_envelope": {},
                "risk_limits": {},
                "real_execution_events": 0,
                "execution_event_count": 0,
                "autonomous": {
                    "enabled": False,
                    "next_decision": "ENABLE AUTO CANARY",
                    "blocker": "AUTONOMOUS_CANARY_DISABLED",
                },
                "trades": [],
                "live_execution": False,
            }
            signal = None
        else:
            service = CanaryService(self.store, initialize=False)
            canary = service.status()
            signal = service.latest_signal()
        canary = _canary_status_projection(canary)
        autonomous = canary["autonomous"]
        eligible_count = canary["eligible_count"]
        rankable_count = canary["rankable_count"]
        execution_events = int(canary.get("real_execution_events", 0) or 0)
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
        projection = {
            "canary": canary,
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
        }
        projection.update({name: canary[name] for name in _CANARY_STATUS_FIELDS})
        return projection

    def _operator_control_data(self) -> dict[str, Any]:
        """Merge responsive controls into the bounded persisted overview."""
        controls = self.control.status() if self.control is not None else {}
        controls = dict(controls) if isinstance(controls, Mapping) else {}
        try:
            persisted = self.overview_summary()
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
        credentials = controls.get("credentials")
        if isinstance(credentials, Mapping):
            configured = bool(credentials.get("configured"))
            result["credentials"] = {
                "configured": configured,
                "status": (
                    "CONFIGURED" if configured else "NOT CONFIGURED"
                ),
                "secret_values_exposed": False,
            }
        else:
            result["credentials"] = canary_module.CredentialStore().safe_projection(
                allow_environment=False
            )
        # Keep control-only status available without colliding with the
        # persisted ``raw`` research payload.
        result.setdefault("control_status", controls)
        return result

    def operator_data(self) -> dict[str, Any]:
        configured = self._configured("operator")
        if configured is not None:
            result = dict(configured) if isinstance(configured, Mapping) else {"value": configured}
            if self.control is not None:
                result["operator_controls"] = self.control.status()
            return result
        if self.control is not None and self.store is not None:
            return self._operator_control_data()
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
            "canary_eligible": self._candidate_canary_eligibility_count(),
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
            hermes_label, hermes_reason = "RUNNING", "Hermes queue worker is executing."
        elif "degraded" in hermes_workers or "stale" in hermes_workers:
            hermes_label, hermes_reason = "DEGRADED", "Hermes queue worker heartbeat or identity is stale."
        elif "stopped" in hermes_workers:
            hermes_label, hermes_reason = "STOPPED", "Hermes queue worker is stopped."
        elif hermes_workers:
            hermes_label, hermes_reason = "READY", "Hermes queue worker is idle."
        elif hermes.get("submitted", 0) or hermes.get("pending", 0):
            hermes_label, hermes_reason = "STOPPED", "Hermes work is persisted but no queue worker is executing."
        else:
            hermes_label, hermes_reason = "NOT INITIALIZED", "No Hermes execution state is persisted."
        paper_state_count = int(paper.get("state_count", 0) or 0)
        paper_label = "ACTIVE" if paper_state_count > 0 else "NOT INITIALIZED"
        canary_service = (
            CanaryService(self.store, initialize=False) if self.store is not None else None
        )
        canary_status = _canary_status_projection(
            canary_service.status()
            if canary_service is not None
            else self.canary_data()["canary"]
        )
        latest_canary_signal = (
            canary_service.latest_signal() if canary_service is not None else None
        )
        operator_controls = self.control.status() if self.control is not None else {}
        candidate_status["rankable"] = int(canary_status.get("rankable_count", 0) or 0)
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
            configured = bool(raw_credentials.get("configured"))
            credentials = {
                "configured": configured,
                "status": "CONFIGURED" if configured else "NOT CONFIGURED",
                "secret_values_exposed": False,
            }
        else:
            credentials = canary_module.CredentialStore().safe_projection(
                allow_environment=False
            )
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
                component("HERMES", hermes_label, {**dict(hermes), "execution_state": hermes_label, "reason": hermes_reason}),
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
    <section id="view-overview" class="view active"><div id="component-grid" class="status-grid"></div><div id="research-cards" class="card-grid"></div>
      <article class="panel"><div class="section-title"><h2>SYSTEM CONTROL</h2><span class="badge good">localhost + token</span></div><div id="operator-controls" class="three-col"></div><div id="control-result" class="page-note"></div></article>
      <div class="two-col"><div><article class="panel"><div class="section-title"><h2>Historical / forward coverage</h2><a class="link" href="#datasets" data-link="datasets">View all</a></div><div id="coverage"></div></article>
        <article class="panel"><div class="section-title"><h2>Candidate lifecycle funnel</h2><a class="link" href="#candidates" data-link="candidates">View all</a></div><div id="funnel" class="funnel"></div></article>
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
    <section id="view-canary" class="view"><article class="panel" style="border-color:var(--red)"><div class="section-title"><h2>REAL CANARY MONEY</h2><span class="badge bad">PRODUCTION LIVE TRADING: DISABLED</span></div><div id="canary-action-result" class="page-note"></div><div id="canary-controls"></div><div id="canary-connectivity"></div><div id="canary-summary"></div><div id="canary-trades" class="scroll"></div><p class="notice">Autonomous canary is independent from paper research. No secrets are stored or displayed. It remains prediction-only, bounded at $1 per order, and killable from this console.</p></article></section>
    <section id="view-binance-canary" class="view binance-view"><article class="panel" style="border-color:var(--amber)"><div class="section-title"><h2>BINANCE SPOT CANARY</h2><span class="badge warn">DEVELOPMENT / PAPER|TESTNET</span></div><p class="page-note">Separate from the Polymarket canary. <strong>POLYMARKET TRANSPORT: DISABLED</strong> · Binance Spot only · no implicit control-plane construction.</p><div id="binance-action-result" class="page-note"></div><div id="binance-identity"></div><div id="binance-connectivity"></div><div id="binance-qualification"></div><div id="binance-risk"></div><div id="binance-controls"></div><div id="binance-records" class="scroll"></div><details><summary>Full Binance projection and identifiers</summary><pre id="binance-raw"></pre></details><p class="notice">Credentials are never displayed. Connectivity checks are read-only; order validation is an explicit test action. No browser action can place an order.</p></article></section>
    <div id="binance-testnet-static-labels" hidden>BINANCE SPOT TESTNET · TESTNET CONNECTIVITY · ORDER VALIDATION · TESTNET EXECUTION PROBE · AUTONOMOUS TESTNET · localhost</div>
  </main>
  <script>
    const $ = (id) => document.getElementById(id), safe = (v) => String(v ?? "—").replace(/[&<>"']/g, c => ({ "&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;" }[c])), json = (v) => JSON.stringify(v ?? {}, null, 2);
    const count = (v) => Number.isFinite(Number(v)) ? String(v) : "0", phtDateFormatter = new Intl.DateTimeFormat("en-PH-u-hc-h23", { timeZone:"Asia/Manila", year:"numeric", month:"2-digit", day:"2-digit", hour:"2-digit", minute:"2-digit", second:"2-digit", hourCycle:"h23" }), dateText = (v) => { if(!v || typeof v !== "string" || !/(?:Z|[+-][0-9]{2}:[0-9]{2})$/.test(v)) return "—"; const date = new Date(v); if(Number.isNaN(date.getTime())) return "—"; const parts = Object.fromEntries(phtDateFormatter.formatToParts(date).filter(i => i.type !== "literal").map(i => [i.type, i.value])); return `${parts.year}-${parts.month}-${parts.day} ${parts.hour}:${parts.minute}:${parts.second} PHT`; }, usd = (v) => { const number=Number(v); return Number.isFinite(number)?`$${number.toFixed(2)}`:"—"; }, arr = (v) => Array.isArray(v) ? v : [];
    const empty = (title,body) => `<div class="empty"><strong>${safe(title)}</strong>${safe(body)}</div>`, statusClass = (v) => { const s=String(v||"").toUpperCase(); return ["READY","RUNNING","ACTIVE","COMPLETE","COMPLETED","HEALTHY","ELIGIBLE","PASSED","PROMOTABLE","A","B"].includes(s)?"good":["DEGRADED","STOPPED","UPDATING","SUBMITTING","UNKNOWN","C"].includes(s)?"warn":["ERROR","STALE","REJECTED","KILLED","BLOCKED","FAIL","INSUFFICIENT","UNAVAILABLE","D","F"].includes(s)?"bad":""; };
    let params = new URLSearchParams(location.search); const state = { tab: params.get("tab") || "overview", page: Math.max(1,Number(params.get("page")||1)), page_size: [10,25,50,100].includes(Number(params.get("page_size"))) ? Number(params.get("page_size")) : 25, filter: params.get("filter") || "", sort: params.get("sort") || "", direction: params.get("direction") === "asc" ? "asc" : "desc", selected: params.get("selected") || "", expanded: params.get("expanded") === "1" };
    let operator = {}, current = {}, loadInFlight = false, operatorControlsRendered = false, binanceTestnetMode = false;
    const controlToken = document.querySelector('meta[name="axiom-control-token"]')?.content || "";
    function controlButton(action,label,target="",confirmation="") { return `<button class="link control-action" data-control-action="${safe(action)}" data-control-target="${safe(target)}" data-control-confirm="${safe(confirmation)}">${safe(label)}</button>`; }
    function isCanaryAction(action) { return String(action||"").startsWith("canary."); }
    function actionResultNode(action) { return $(isCanaryAction(action)?"canary-action-result":"control-result"); }
    function actionResultMessage(action,message) { const node=actionResultNode(action); if(node)node.textContent=message||""; }
    async function controlPost(action,target="",confirmation="") {
      const payload={action,target}; if(confirmation)payload.confirm=confirmation;
      try {
        const response=await fetch("/api/control",{method:"POST",headers:{"Content-Type":"application/json","X-Axiom-Control-Token":controlToken},body:JSON.stringify(payload),cache:"no-store"});
        const result=await response.json();
        const connectivity=result?.result?.connectivity||result?.connectivity;
        if(isCanaryAction(action)&&connectivity) {
          if(lastGood.canary&&typeof lastGood.canary==="object"&&!Array.isArray(lastGood.canary)) lastGood.canary={...lastGood.canary,connectivity};
          renderCanaryConnectivity(connectivity);
        }
        actionResultMessage(action,result.ok?`${action} completed`:`${action} blocked: ${result.reason||"CONTROL_FAILED"}`);
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
        `<article><h3>HERMES</h3><div class="key-value"><span class="key">Status / job</span><strong>${safe(h.status)} · ${safe(h.job_id)}</strong></div><div class="key-value"><span class="key">Last / next</span><strong>${safe(dateText(h.last_run_at))} · ${safe(dateText(h.next_run_at))}</strong></div><p class="page-note">${h.status==="PAUSED"?controlButton("hermes.resume","Resume Hermes"):controlButton("hermes.pause","Pause Hermes")} · ${controlButton("hermes.run_now","Run now")}</p></article>`,
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
      const c=value||{}, checkedAt=c.checked_at, checkedPht=dateText(checkedAt), sdk=c.sdk||{}, credentials=c.credentials||{}, authentication=c.authentication||{}, account=c.account||{}, geo=c.geoblock||{}, balance=c.balance||{}, allowance=c.allowance||{}, market=c.market||{}, book=c.order_book||{};
      if(!value){ $("canary-connectivity").innerHTML=empty("No connectivity check persisted","Run Connectivity check to perform a read-only pre-arming check."); return; }
      const failures=arr(c.failure_reasons), failureMarkup=failures.length?`<div class="key-value"><span class="key">Failure codes</span><strong>${safe(arr(c.failure_codes).join(", ")||"—")}</strong></div><div class="key-value"><span class="key">Failure reasons</span><strong>${failures.map(item=>`${safe(item.code)}: ${safe(item.reason)}`).join("<br>")}</strong></div>`:"";
      const marketMarkup=String(market.status||"SKIPPED").toUpperCase()!=="SKIPPED"?`<div class="key-value"><span class="key">Market</span><strong>${safe(market.status)}</strong></div>`:"";
      const bookMarkup=String(book.status||"SKIPPED").toUpperCase()!=="SKIPPED"?`<div class="key-value"><span class="key">Order book</span><strong>${safe(book.status)}</strong></div>`:"";
      $("canary-connectivity").innerHTML=`<article class="panel"><div class="section-title"><h2>CONNECTIVITY</h2><span class="badge ${statusClass(c.status)}">${safe(c.status||"BLOCKED")}</span></div><div class="three-col"><div class="key-value"><span class="key">SDK</span><strong>${safe(sdk.status)} · ${safe(sdk.name)} · ${safe(sdk.version)}</strong></div><div class="key-value"><span class="key">Credentials</span><strong>${safe(credentials.status)}</strong></div><div class="key-value"><span class="key">Authentication</span><strong>${safe(authentication.status)}</strong></div><div class="key-value"><span class="key">Account</span><strong>${safe(account.status)}${account.wallet_type?` · ${safe(account.wallet_type)}`:""}</strong></div><div class="key-value"><span class="key">Geoblock</span><strong>${safe(geo.status)}${geo.country?` · ${safe(geo.country)}`:""}${geo.region?` / ${safe(geo.region)}`:""}</strong></div><div class="key-value"><span class="key">Balance</span><strong>${safe(balance.status)}${balance.available_usd!=null?` · ${usd(balance.available_usd)}`:""}</strong></div><div class="key-value"><span class="key">Allowance</span><strong>${safe(allowance.status)}</strong></div>${marketMarkup}${bookMarkup}<div class="key-value"><span class="key">Checked</span><strong>${safe(dateText(c.checked_at))}</strong></div></div>${failureMarkup?`<p class="page-note">${failureMarkup}</p>`:""}</article>`;
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

    function renderCanary(data) {
      const c=data.canary||{}, auto=data.autonomous_canary||c.autonomous||{}, risk=c.risk_envelope||c.risk_limits||{}, signal=data.canary_signal||null, connectivity=data.connectivity??c.connectivity??null;
      renderCanaryConnectivity(connectivity);
      const backendState=String(c.micro_live_canary||"DISABLED"), stateValue=backendState==="KILLED"?"KILLED":Boolean(auto.enabled)?"ENABLED":"DISABLED";
      const enabled=Boolean(auto.enabled), selectionStatus=String(c.selection_status||"NONE").toUpperCase(), selectionValid=c.selection_valid===true, currentCandidate=selectionValid&&selectionStatus==="CURRENT"?c.selected_candidate||"": "", currentWinnerId=c.winner_id||"", historicalCandidate=c.last_selected_candidate||"", selectionLabel=selectionStatus==="STALE"?"STALE · REEVALUATION REQUIRED":selectionStatus==="CURRENT"?"CURRENT":"NONE", rawEligible=Number(c.eligibility_raw_count)||0, eligible=Number(c.eligible_count)||0, rawRankable=Number(c.rankable_raw_count)||0, rankable=Number(c.rankable_count)||0, events=data.real_execution_events??c.real_execution_events??c.execution_event_count??0, manualCandidate=backendState==="ARMED"&&c.candidate?`<div class="panel"><div class="metric">${safe(c.candidate)}</div><div class="metric-label">Manual armed candidate</div></div>`:"";
      const connectivityReady=connectivity?.ready===true, connectivityBlocker=connectivityReady?"":arr(connectivity?.failure_codes)[0]||"CONNECTIVITY_BLOCKED";
      const selectionReason=c.selection_invalidation_reason||"", selectionBlocker=selectionValid&&currentCandidate?"":(selectionReason||(selectionStatus==="STALE"?"REEVALUATION_REQUIRED":selectionStatus==="NONE"?"NO_CURRENT_SELECTION":"SELECTION_INVALID"));
      const autonomousBlocker=!connectivity?"CONNECTIVITY_CHECK_REQUIRED":!connectivityReady?connectivityBlocker:backendState==="KILLED"?"CANARY_KILLED":selectionBlocker||String(auto.blocker||"AUTONOMOUS_CANARY_DISABLED");
      const autoReady=connectivityReady&&backendState!=="KILLED"&&!enabled&&selectionValid&&Boolean(currentCandidate);
      const enable=stateValue==="KILLED"?"":enabled?controlButton("canary.disarm","DISARM","","DISARM"):controlButton("canary.enable_auto","ENABLE AUTO CANARY","","ENABLE AUTO CANARY");
      const riskMarkup=Object.entries(risk).map(([key,value])=>`<div class="key-value"><span class="key">${safe(key.replaceAll("_"," "))}</span><strong>${safe(value)}</strong></div>`).join("")||empty("Risk envelope unavailable","No frozen risk limits are persisted.");
      const currentRank=selectionValid&&selectionStatus==="CURRENT"?auto.rank:"—", currentScore=selectionValid&&selectionStatus==="CURRENT"?auto.score:"—", selectionReasonLabel=c.selection_reason||"—", historicalMarkup=historicalCandidate?`<div class="panel"><div class="metric">${safe(historicalCandidate)}</div><div class="metric-label">Selected winner · Historical selected ID</div><p class="page-note"><span class="badge ${statusClass(selectionStatus)}">${safe(selectionLabel)}</span></p></div>`:"";
      $("canary-controls").innerHTML=`<article class="panel"><div class="section-title"><h2>AUTONOMOUS CANARY CONTROL</h2><span class="badge ${statusClass(stateValue)}">${safe(stateValue)}</span></div><p class="page-note"><strong>${safe(autoReady?"AUTO CANARY READY TO ENABLE":`AUTO CANARY BLOCKED: ${autonomousBlocker}`)}</strong></p><div class="page-note">${controlButton("canary.connectivity_check","Connectivity check")} · ${enable} · ${controlButton("canary.kill","KILL","","KILL")}</div><p class="page-note">One confirmation enables the frozen prediction-only $1 envelope. Research, eligibility, ranking, and submission decisions run in the node worker; Hermes cannot change this envelope.</p></article>`;
      $("canary-summary").innerHTML=`<div class="card-grid"><div class="panel"><div class="metric">${safe(stateValue)}</div><div class="metric-label">Autonomous canary state</div></div>${currentCandidate?`<div class="panel"><div class="metric">${safe(currentCandidate)}</div><div class="metric-label">Selected winner · Current selection</div><p class="page-note"><span class="badge ${statusClass(selectionStatus)}">${safe(selectionLabel)}</span></p></div>`:historicalMarkup||`<div class="panel"><div class="metric">—</div><div class="metric-label">Current selection</div><p class="page-note"><span class="badge ${statusClass(selectionStatus)}">${safe(selectionLabel)}</span></p></div>`}${manualCandidate}<div class="panel"><div class="metric">${safe(currentRank)} · ${safe(currentScore)}</div><div class="metric-label">Current rank / score</div></div><div class="panel"><div class="metric">${count(rawEligible)}</div><div class="metric-label">Eligible candidates (raw)</div></div><div class="panel"><div class="metric">${count(eligible)}</div><div class="metric-label">Eligible candidates (validated)</div></div><div class="panel"><div class="metric">${count(rawRankable)}</div><div class="metric-label">Rankable candidates (raw)</div></div><div class="panel"><div class="metric">${count(rankable)}</div><div class="metric-label">Rankable candidates (validated)</div></div><div class="panel"><div class="metric">${count(events)}</div><div class="metric-label">Real execution events</div></div></div><article class="panel"><div class="section-title"><h2>Autonomous readiness</h2><span class="badge ${statusClass(autonomousBlocker)}">${safe(autoReady?"READY":autonomousBlocker)}</span></div><div class="three-col"><div class="key-value"><span class="key">Selection status</span><strong>${safe(selectionLabel)}</strong></div><div class="key-value"><span class="key">Selection valid</span><strong>${safe(selectionValid)}</strong></div><div class="key-value"><span class="key">Current selection</span><strong>${safe(currentCandidate||"—")}</strong></div><div class="key-value"><span class="key">Historical selection</span><strong>${safe(historicalCandidate||"—")}</strong></div><div class="key-value"><span class="key">Selection reason</span><strong>${safe(selectionReasonLabel)}</strong></div><div class="key-value"><span class="key">Invalidation reason</span><strong>${safe(selectionReason||"—")}</strong></div><div class="key-value"><span class="key">Last ranking run ID</span><strong>${safe(c.ranking_run_id||"—")}</strong></div><div class="key-value"><span class="key">Last ranking timestamp</span><strong>${safe(dateText(c.ranking_timestamp))}</strong></div><div class="key-value"><span class="key">Historical data integrity</span><strong>${safe(c.historical_data_integrity||"UNKNOWN")}</strong></div><div class="key-value"><span class="key">Historical execution fidelity</span><strong>${safe(c.historical_execution_fidelity||"UNKNOWN")}</strong></div><div class="key-value"><span class="key">Current execution evidence</span><strong>${safe(c.current_execution_evidence||"CURRENT_ORDER_BOOK_REQUIRED")}</strong></div><div class="key-value"><span class="key">Next decision</span><strong>${safe(auto.next_decision||"—")}</strong></div><div class="key-value"><span class="key">Blocker</span><strong>${safe(selectionReason||auto.blocker||"—")}</strong></div></div></article><article class="panel"><div class="section-title"><h2>Risk envelope</h2><span class="badge warn">bounded $1</span></div>${riskMarkup}</article>`;
      const readiness=signal?String(signal.status||"READY"):"NO SIGNAL", detail=signal?`<div class="three-col"><div class="key-value"><span class="key">Signal readiness</span><strong>${safe(readiness)}</strong></div><div class="key-value"><span class="key">Market / outcome</span><strong>${safe(signal.market_id)} / ${safe(signal.outcome)}</strong></div><div class="key-value"><span class="key">Expected price</span><strong>${safe(signal.paper_expected_price)}</strong></div><div class="key-value"><span class="key">Generated</span><strong>${safe(dateText(signal.generated_at))}</strong></div><div class="key-value"><span class="key">Order result</span><strong>${safe(c.last_request_status||"NO ORDER")}</strong></div></div>`:empty("No latest signal","No persisted signal is available.");
      $("canary-summary").insertAdjacentHTML("beforeend",`<article class="panel"><div class="section-title"><h2>Latest signal</h2><span class="badge ${statusClass(readiness)}">${safe(readiness)}</span></div>${detail}<p class="page-note">Kill prevents new submissions; an in-flight request is recorded, in-flight not retracted, and never retried automatically.</p></article>`);
      $("canary-trades").innerHTML=arr(c.trades).length?`<table><thead><tr><th>Time</th><th>Candidate</th><th>Market</th><th>Side</th><th>Status</th><th>Price Δ</th></tr></thead><tbody>${arr(c.trades).map(t=>`<tr><td>${safe(dateText(t.timestamp))}</td><td>${safe(t.candidate_id)}</td><td>${safe(t.market_id)}</td><td>${safe(t.side)}</td><td><span class="badge ${statusClass(t.status)}">${safe(t.status)}</span></td><td>${safe(t.price_difference)}</td></tr>`).join("")}</tbody></table>`:empty("No canary execution evidence","No order has been submitted by the autonomous worker.");
    }
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
    renderOverview = (data) => { renderComponents(data); renderOutcomeCards(data); const c=data.coverage||{},h=data.collector_health||{}; $("coverage").innerHTML=`<div class="three-col"><div class="key-value"><span class="key">Historical datasets</span><strong>${count(c.historical_count)}</strong></div><div class="key-value"><span class="key">Historical rows</span><strong>${count(c.historical_rows)}</strong></div><div class="key-value"><span class="key">Forward datasets</span><strong>${count(c.forward_count)}</strong></div><div class="key-value"><span class="key">Forward rows</span><strong>${count(c.forward_rows)}</strong></div><div class="key-value"><span class="key">Logical observations</span><strong>${count((c.logical_rows||{}).bars)}</strong></div><div class="key-value"><span class="key">Collector errors</span><strong>${count(h.collection_errors)}</strong></div><div class="key-value"><span class="key">Last cycle duration</span><strong>${h.last_cycle_duration_seconds==null?"—":`${Number(h.last_cycle_duration_seconds).toFixed(1)}s`}</strong></div><div class="key-value"><span class="key">Effective cadence</span><strong>${h.effective_collection_cadence_seconds==null?"—":`${Number(h.effective_collection_cadence_seconds).toFixed(1)}s`}</strong></div><div class="key-value"><span class="key">Markets A / S / F</span><strong>${count(h.last_cycle_markets_attempted)} / ${count(h.last_cycle_markets_successful)} / ${count(h.last_cycle_markets_failed)}</strong></div></div><p class="page-note">Configured interval ${safe(h.configured_interval_seconds??"—")}s · stale threshold ${safe(h.stale_after_seconds??"—")}s · last successful cycle ${safe(dateText(h.last_successful_cycle))}</p>`; $("overview-activity").innerHTML=activityMarkup(arr(data.latest_activity||data.activity)); $("overview-candidates").innerHTML=empty("Candidate list is lazy","Open Candidates to load the bounded lifecycle page."); $("raw-overview").textContent=json({counts:data.counts,collector_health:h,latest_outcome:data.hermes_latest_outcome}); };
    const _renderOverviewScheduling = renderOverview;
    renderOverview = (data) => {
      _renderOverviewScheduling(data);
      if(Object.prototype.hasOwnProperty.call(data,"operator_controls")||!operatorControlsRendered)renderOperatorControls(data);
      const h = data.collector_health || {};
      const components = Object.fromEntries(arr(data.components).map(item => [item.name, item]));
      const collector = components["POLYMARKET COLLECTOR"]?.detail || {};
      const paper = components["PAPER ENGINE"]?.detail || {};
      const research = components["RESEARCH ENGINE"]?.detail || {};
      $("coverage").insertAdjacentHTML("beforeend", `<p class="page-note">Next collection ${safe(dateText(h.next_scheduled_collection_at || collector.next_scheduled_collection_at))} · collector heartbeat ${safe(dateText(h.worker_heartbeat_at || collector.worker_heartbeat_at))} · PAPER_FORWARD pass ${safe(paper.status || "—")} · research pass ${safe(research.status || "—")}</p>`);
      const latestCandidates = arr(data.latest_candidates || data.candidates);
      $("overview-candidates").innerHTML = latestCandidates.length
        ? tableCandidates(latestCandidates, false)
        : empty("No candidates yet", "No persisted lifecycle candidates are available.");
      const funnel = data.lifecycle_funnel || {};
      const max = Math.max(1, ...Object.values(funnel).map(Number));
      $("funnel").innerHTML = Object.entries(funnel).map(([k, v]) => `<div class="funnel-row"><span>${safe(k)}</span><span class="funnel-track"><span class="funnel-bar" style="width:${Math.min(100, Number(v) / max * 100)}%"></span></span><span class="right">${count(v)}</span></div>`).join("") || empty("No candidate lifecycle", "Hermes hypotheses appear after a durable queue item is processed.");
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
          lastGood[kind]=data; lastSuccessful=Date.now();
          render(data);
          refreshMessage(target,`Updated · ${new Date().toLocaleTimeString()}`);
        } catch(error) {
          if(generation!==refreshGeneration||error?.name==="AbortError")return;
          if(lastGood[kind])render(lastGood[kind]);
          refreshMessage(target,`Refresh failed (${refreshError(error)}) · showing last successful content`,true);
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
                lastGood.controls=controls;
                operator=controls;
                renderOperatorControls({operator_controls:controls.operator_controls||controls});
              } catch(error) {
                if(generation!==refreshGeneration||error?.name==="AbortError")return;
                if(lastGood.controls) {
                  operator=lastGood.controls;
                  renderOperatorControls({operator_controls:operator.operator_controls||operator});
                }
                refreshMessage(tab,`Refresh failed (${refreshError(error)}) · showing last successful content`,true);
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
          if(lastGood[tab])({overview:renderOverview,canary:renderCanary,datasets:renderDatasets,activity:renderActivity,candidates:renderCandidates,polymarket:renderPolymarket,hermes:renderHermes,crypto:renderCrypto,portfolio:renderPaper,"binance-canary":renderBinanceCanary}[tab])(lastGood[tab]);
          refreshMessage(tab,`Refresh failed (${refreshError(error)}) · showing last successful content`,true);
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
    document.addEventListener("click",async event=>{const button=event.target.closest?.(".control-action");if(!button)return;const action=button.dataset.controlAction||"",target=button.dataset.controlTarget||"",expected=button.dataset.controlConfirm||"";if(expected){const typed=window.prompt(`Type ${expected} to continue`);if(typed!==expected){actionResultMessage(action,`${action} cancelled: exact confirmation required`);return;}}const result=await controlPost(action,target,expected);const local=$("candidate-control-result");if(local&&target===state.selected&&!isCanaryAction(action))local.textContent=result.ok?`${action} completed`:`${action} blocked: ${result.reason||"CONTROL_FAILED"}`;});
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

    def _control_request_allowed(self) -> bool:
        expected = str(getattr(self.server, "control_token", "") or "")
        supplied = str(self.headers.get("X-Axiom-Control-Token", "") or "")
        if not expected or not self._loopback_client() or not hmac.compare_digest(supplied, expected):
            return False
        origin = self.headers.get("Origin")
        if origin:
            parsed_origin = urlparse(origin)
            if parsed_origin.scheme not in {"http", "https"} or not _loopback_host(parsed_origin.hostname or ""):
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
            body = json.loads(self.rfile.read(length).decode("utf-8"))
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
        allowed_fields = {"action", "target", "confirm"}
        if set(body) - allowed_fields:
            self._send(400, {"error": "unsupported control fields"})
            return
        result = self.server.dashboard_data.control.execute(
            body.get("action", ""),
            body.get("target", ""),
            confirm=body.get("confirm", ""),
        )
        reason = str(result.get("reason", "")) if isinstance(result, Mapping) else ""
        if result.get("ok") if isinstance(result, Mapping) else False:
            status = 200
        elif reason in {"BOOTSTRAP_ALREADY_RUNNING", "NODE_ALREADY_RUNNING", "NODE_STOP_TIMEOUT", "BOOTSTRAP_NOT_RESUMABLE"}:
            status = 409
        elif reason.endswith("UNAVAILABLE") or reason.endswith("TIMEOUT") or reason.endswith("FAILED"):
            status = 503
        else:
            status = 400
        self._send(status, result)

    def do_GET(self) -> None:  # noqa: N802 - stdlib handler API
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
        super().__init__(address, _DashboardHandler)
        self.dashboard_data = data
        self.control_token = secrets.token_urlsafe(32)
        self.daemon_threads = True
        self.allow_reuse_address = True


class DashboardServer:
    """Threaded local dashboard server; ``start`` is non-blocking."""

    def __init__(self, host: str = "127.0.0.1", port: int = 0, *, data: DashboardData | None = None, **data_kwargs: Any) -> None:
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
