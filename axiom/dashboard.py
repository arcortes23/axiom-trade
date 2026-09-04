"""Local read-only operator dashboard HTTP server.

The server is dependency-free and every JSON endpoint remains available for
automation.  ``/`` serves the dark research console without live execution.
"""
from __future__ import annotations

from dataclasses import asdict, is_dataclass
from itertools import islice
from datetime import date, datetime
import json
import math
import os
from pathlib import Path
import re
from .canary import CanaryService
import subprocess
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from types import MappingProxyType
from typing import Any, Mapping
from urllib.parse import parse_qs, unquote, urlparse

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

_ENDPOINTS = (
    "overview",
    "operator",
    "datasets",
    "research",
    "research-summary",
    "crypto",
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
_V2_ENDPOINTS = ("datasets", "activity", "candidates", "polymarket", "hermes", "paper")

_DEFAULT_PAGE_SIZE = 25
_PAGE_SIZE_OPTIONS = (10, 25, 50, 100)
_MAX_PAGE_SIZE = 100


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
        "item_id": first("item_id", "") or None,
        "record_type": first("record_type", "") or None,
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


class DashboardData:
    """Read-only data facade used by HTTP handlers and embedders."""

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
    ) -> None:
        self._data = dict(data or {})
        self.tracker = tracker
        self.crypto_provider = crypto_provider
        self.prediction_provider = prediction_provider
        self.evolution = evolution
        self.risk = risk
        self.store = store

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
            return {"grade": "F", "error": "store has no polymarket health method", "live_execution": False}
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
        offset = (values["page"] - 1) * values["page_size"]
        return _page_result(items[offset : offset + values["page_size"]], page=values["page"], page_size=values["page_size"], total=len(items))

    def paginate_research_queue(self, params: Mapping[str, Any] | None = None) -> dict[str, Any]:
        values = _pagination_params(params)
        values["sort"] = values.get("sort") or "priority"
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
            return result
        if self.store is None or not callable(getattr(self.store, "list_research_items", None)):
            return result
        limit = min(_MAX_SIZE_FALLBACK, values["page"] * values["page_size"])
        records = self.store.list_research_items(status=values.get("status"), limit=limit)
        needle = str(values.get("filter") or "").lower()
        if needle:
            records = [item for item in records if needle in json.dumps(_jsonable(item), sort_keys=True).lower()]
        offset = (values["page"] - 1) * values["page_size"]
        return _page_result(records[offset : offset + values["page_size"]], page=values["page"], page_size=values["page_size"], total=len(records))

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
            return result
        paper = self._paper_portfolio()
        items = paper.get("states", []) if isinstance(paper, Mapping) else []
        items = items if isinstance(items, list) else []
        needle = str(values.get("filter") or "").lower()
        if needle:
            items = [item for item in items if needle in json.dumps(_jsonable(item), sort_keys=True).lower()]
        offset = (values["page"] - 1) * values["page_size"]
        return _page_result(items[offset : offset + values["page_size"]], page=values["page"], page_size=values["page_size"], total=len(items))

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
            detail_params = dict(params or {})
            detail_params["item_id"] = identifier
            return self.paginate_research_queue(detail_params)
        handlers = {
            "datasets": self.paginate_dataset_catalog,
            "activity": self.paginate_research_activity,
            "candidates": self.paginate_candidate_lifecycle,
            "polymarket": self.paginate_polymarket_markets,
            "hermes": self.paginate_research_queue,
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
        return {
            "status": status,
            "summary": self.store.dashboard_summary(),
            "cycles": self.store.list_collection_cycles(limit=20),
            "queue": self.store.research_queue_stats(),
            "workers": normalized_workers,
            "normalized_workers": normalized_workers,
            "autonomous": summary.get("autonomous", {}),
            "hermes": summary.get("hermes", {}),
            "health_grade": health_grade or None,
            "live_execution": False,
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
    @staticmethod
    def _candidate_row(item: Mapping[str, Any]) -> dict[str, Any]:
        payload = item.get("payload", {})
        payload = payload if isinstance(payload, Mapping) else {}
        candidate_id = str(item.get("candidate_id", ""))
        return {
            "candidate_id": candidate_id,
            "strategy_id": payload.get("strategy_id", payload.get("experiment_id", candidate_id)),
            "family": payload.get("experiment_family", payload.get("family", "unknown")),
            "market": payload.get("market_type", payload.get("market", "unknown")),
            "generation": payload.get("generation", 0),
            "parent_id": payload.get("parent_id"),
            "stage": item.get("stage"),
            "validation_expectancy": payload.get("validation_expectancy", payload.get("validation_score")),
            "validation_max_drawdown": payload.get("validation_max_drawdown"),
            "validation_stability": payload.get("validation_stability"),
            "forward_bets": payload.get("forward_independent_resolved_bets", payload.get("markets_resolved")),
            "forward_pnl": payload.get("forward_pnl", payload.get("forward_net_pnl")),
            "data_quality": payload.get("data_quality", payload.get("quality", payload.get("research_quality"))),
            "rejection_reason": payload.get("rejection_reason"),
            "updated_at": item.get("updated_at"),
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

    def _paper_portfolio(self) -> dict[str, Any]:
        if self.store is None:
            return {
                "paper_money": True,
                "live_execution": False,
                "states": [],
                "total_equity": 0.0,
                "total_pnl": 0.0,
            }
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
    def btc_research_data(self, *, catalog_data: Mapping[str, Any] | None = None) -> dict[str, Any]:
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
        return {
            "available": bool(btc_catalogs or btc_summary),
            "catalog": btc_catalogs,
            "catalog_summary": btc_summary,
            "latest_report": latest,
            "reports": reports[:20],
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
        return {
            "available": True,
            "candidate_id": identifier,
            "stage": lifecycle.get("stage"),
            "strategy": {
                "id": payload.get("strategy_id", payload.get("experiment_id", identifier)),
                "family": payload.get("experiment_family", payload.get("family")),
                "market_type": payload.get("market_type"),
                "parameters": payload.get("parameters", payload.get("strategy_parameters", {})),
                "generation": payload.get("generation", 0),
                "parent_id": payload.get("parent_id"),
            },
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

    def operator_data(self) -> dict[str, Any]:
        configured = self._configured("operator")
        if configured is not None:
            return dict(configured) if isinstance(configured, Mapping) else {"value": configured}
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
        funnel = self.store.candidate_lifecycle_funnel() if self.store is not None else {}
        for key, value in funnel.items():
            stages[str(key)] = int(value)
        candidate_page = self.paginate_candidate_lifecycle({"page": 1, "page_size": 10})
        candidate_rows = [
            {**dict(item), **self._candidate_row(item)}
            for item in candidate_page.get("items", [])
            if isinstance(item, Mapping)
        ]
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
                    node_reason = payload.get("last_error") or payload.get("error")
                if node_reason:
                    break
            if not node_reason and node_state in {"stale", "degraded"}:
                node_reason = "Worker heartbeat, identity, lock ownership, or health grade is degraded."
            if not node_reason and status.get("health_grade"):
                node_reason = f"Health monitor grade is {status['health_grade']}."
        crypto_label = "READY" if catalogs.get("historical_count", 0) else ("UPDATING" if any(str(item.get("status")).upper() == "RUNNING" for item in btc_states) else "NOT INITIALIZED")
        polymarket_label = "READY" if catalogs.get("forward_count", 0) else ("UPDATING" if any(str(item.get("status")).upper() == "RUNNING" for item in forward_states) else "NOT INITIALIZED")
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
        return {
            "title": "AXIOM / operator research console",
            "live_trading": {"status": "Disabled", "enabled": False},
            "canary": CanaryService(self.store).status() if self.store is not None else {"production_live_trading": "DISABLED", "micro_live_canary": "DISARMED", "live_execution": False, "trades": []},
            "paper_risk_engine": {"status": "Active", "enabled": True},
            "components": [
                component("AXIOM NODE", node_label, {"status": node_state, "reason": node_reason}),
                component("POLYMARKET COLLECTOR", polymarket_label, {"forward_catalogs": catalogs.get("forward_count", 0), "reason": "No forward catalog is persisted." if polymarket_label == "NOT INITIALIZED" else None}),
                component("CRYPTO DATA", crypto_label, {"historical_catalogs": catalogs.get("historical_count", 0), "reason": "No historical catalog is persisted." if crypto_label == "NOT INITIALIZED" else None}),
                component("HERMES", hermes_label, {**dict(hermes), "execution_state": hermes_label, "reason": hermes_reason}),
                component("PAPER ENGINE", paper_label, {"states": paper_state_count, "reason": "Waiting for PAPER_FORWARD." if paper_label == "NOT INITIALIZED" else None}),
            ],
            "research_cards": {
                "experiments_run": int(count.get("experiments", 0)),
                "active_hypotheses": int(hermes.get("pending", 0)),
                "candidates_alive": sum(value for key, value in stages.items() if key != "REJECTED"),
                "rejected": stages["REJECTED"],
                "paper_forward": stages["PAPER_FORWARD"],
                "paper_promotable": stages["PAPER_PROMOTABLE"],
            },
            "coverage": overview_coverage,
            "activity": activity_rows,
            "lifecycle_funnel": stages,
            "candidates": candidate_rows,
            "btc": self.btc_research_data(catalog_data=catalogs),
            "polymarket": self.polymarket_research_data(catalog_data=catalogs, current_data=polymarket_current),
            "paper_portfolio": paper,
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

    def snapshot(self, endpoint: str) -> Any:
        raw_endpoint = endpoint.strip("/")
        endpoint = raw_endpoint.lower()
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


def _dashboard_html() -> str:
    """Return the bounded, read-only operator dashboard surface."""
    return """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
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
    nav { width:min(100% - 32px, 1400px); margin:0 auto; display:flex; gap:4px; padding:0 0 11px; overflow-x:auto; } nav button,button.link { border:1px solid transparent; color:var(--muted); background:transparent; cursor:pointer; } nav button { padding:7px 10px; border-radius:5px; font-size:.7rem; letter-spacing:.06em; text-transform:uppercase; } nav button:hover,nav button.active { color:var(--text); border-color:var(--line); background:#122039; }
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
  </style>
</head>
<body>
  <header><div class="topbar"><div><div class="eyebrow">Read-only research operations</div><h1>AXIOM / operator console</h1><p class="subtitle">Historical evidence, forward observation, and paper lifecycle in one view.</p></div><div class="live-lock">Live trading <strong>Disabled</strong><br>Paper risk engine <strong>Active</strong></div></div>
    <nav aria-label="Research sections">
      <button class="tab active" data-view="overview">Overview</button><button class="tab" data-view="datasets">DATASETS</button><button class="tab" data-view="activity">ACTIVITY</button><button class="tab" data-view="btc">BTC Research</button><button class="tab" data-view="polymarket">Polymarket</button><button class="tab" data-view="candidates">Candidates</button><button class="tab" data-view="hermes">Hermes</button><button class="tab" data-view="portfolio">Paper Portfolio</button><button class="tab" data-view="canary">REAL CANARY</button>
    </nav>
  </header>
  <main>
    <section id="view-overview" class="view active"><div id="component-grid" class="status-grid"></div><div id="research-cards" class="card-grid"></div>
      <div class="two-col"><div><article class="panel"><div class="section-title"><h2>Historical / forward coverage</h2><a class="link" href="#datasets" data-link="datasets">View all</a></div><div id="coverage"></div></article>
        <article class="panel"><div class="section-title"><h2>Candidate lifecycle funnel</h2><a class="link" href="#candidates" data-link="candidates">View all</a></div><div id="funnel" class="funnel"></div></article>
        <article class="panel"><div class="section-title"><h2>Latest candidates</h2><a class="link" href="#candidates" data-link="candidates">View all</a></div><div id="overview-candidates" class="scroll"></div></article></div>
        <div><article class="panel"><div class="section-title"><h2>Latest activity</h2><a class="link" href="#activity" data-link="activity">View all</a></div><div id="overview-activity" class="timeline"></div></article>
        <article class="panel"><div class="section-title"><h2>Selected detail</h2><span class="muted">preserved on refresh</span></div><div id="detail" class="empty"><strong>Select an item</strong>Dataset and candidate evidence appears here.</div></article></div></div>
      <details><summary>Technical details · raw APIs and retained debug surfaces</summary><p class="page-note">Existing JSON APIs remain available for automation. Research maturity, Paper forward, and Research queue and node status are retained below as raw endpoint links.</p><div id="api-links"><a href="/api/v2/datasets">datasets</a> · <a href="/api/v2/activity">activity</a> · <a href="/api/v2/candidates">candidates</a> · <a href="/api/v2/polymarket">polymarket</a> · <a href="/api/v2/hermes">hermes</a> · <a href="/api/v2/paper">paper</a> · <a href="/api/autonomous-research">autonomous-research</a></div><pre id="raw-overview"></pre></details>
    </section>
    <section id="view-datasets" class="view"><article class="panel"><div class="section-title"><h2>DATASETS</h2><span id="dataset-total" class="muted"></span></div><div class="filters"><input id="datasets-filter" placeholder="Filter dataset, instrument, source" aria-label="Filter datasets"><select id="datasets-source"><option value="">All sources</option><option>HISTORICAL</option><option>FORWARD_COLLECTED</option></select><select id="datasets-size"><option>25</option><option>50</option><option>100</option></select></div><div id="datasets-table" class="scroll"></div><div id="datasets-pager" class="pager"></div></article><article id="dataset-detail" class="panel"></article></section>
    <section id="view-activity" class="view"><article class="panel"><div class="section-title"><h2>ACTIVITY</h2><span id="activity-total" class="muted"></span></div><div class="filters"><input id="activity-filter" placeholder="Filter activity" aria-label="Filter activity"><select id="activity-status"><option value="">All statuses</option><option>PENDING</option><option>RUNNING</option><option>COMPLETE</option><option>COMPLETED</option><option>ACCEPTED</option><option>FAILED</option><option>ERROR</option><option>REJECTED</option></select><select id="activity-size"><option>25</option><option>50</option><option>100</option></select></div><div id="activity-table" class="scroll"></div><div id="activity-pager" class="pager"></div></article></section>
    <section id="view-btc" class="view"><article class="panel"><div class="section-title"><h2>BTC / USDT historical research</h2><span class="badge good">historical only</span></div><div id="btc-summary"></div><div id="btc-experiments" class="scroll"></div><div class="notice">Walk-forward results are deterministic and paper-only; no live execution path exists.</div></article></section>
    <section id="view-polymarket" class="view"><article class="panel"><div class="section-title"><h2>Polymarket opportunities</h2><span class="badge warn">price proxy unless timestamped depth exists</span></div><div class="filters"><input id="polymarket-filter" placeholder="Filter questions or markets" aria-label="Filter Polymarket"><select id="polymarket-category"><option value="">All categories</option></select><select id="polymarket-size"><option>25</option><option>50</option><option>100</option></select></div><div id="pm-summary"></div><div id="pm-markets" class="scroll"></div><div id="polymarket-pager" class="pager"></div><p class="page-note">Historical price history is separate from forward order-book observations. No historical depth, spread, fills, or executable quotes are fabricated.</p></article></section>
    <section id="view-candidates" class="view"><article class="panel"><div class="section-title"><h2>CANDIDATES</h2><span id="candidate-total" class="muted"></span></div><div class="filters"><input id="candidates-filter" placeholder="Filter strategy, family, market" aria-label="Filter candidates"><select id="candidates-stage"><option value="">All stages</option></select><select id="candidates-size"><option>25</option><option>50</option><option>100</option></select></div><div id="candidates-table" class="scroll"></div><div id="candidates-pager" class="pager"></div></article></section>
    <article id="candidate-detail" class="panel"><div class="section-title"><h2>Candidate detail</h2><span class="muted">historical → forward → lifecycle</span></div><div class="empty">Select a candidate to inspect evidence.</div></article>
    <section id="view-hermes" class="view"><article class="panel"><div class="section-title"><h2>Hermes / research loop</h2><span class="badge">no autonomous trading</span></div><div id="hermes-summary"></div><div class="filters"><input id="hermes-filter" placeholder="Filter queue" aria-label="Filter Hermes queue"><select id="hermes-status"><option value="">All statuses</option><option>PENDING</option><option>TESTING</option><option>COMPLETED</option><option>ACCEPTED</option><option>REJECTED</option><option>FAILED</option><option>ERROR</option></select><select id="hermes-size"><option>25</option><option>50</option><option>100</option></select></div><div id="hermes-table" class="scroll"></div><div id="hermes-pager" class="pager"></div><div id="hermes-detail"></div></article></section>
    <section id="view-portfolio" class="view"><article class="panel"><div class="section-title"><h2>Paper portfolio and histories</h2><span class="badge good">paper money</span></div><div id="portfolio-summary"></div><div class="filters"><input id="paper-filter" placeholder="Filter experiment, market" aria-label="Filter paper records"><select id="paper-status"><option value="">All statuses</option><option>FILLED</option><option>RESOLVED</option><option>OPEN</option><option>CLOSED</option><option>ACCEPTED</option><option>FAILED</option></select><select id="paper-size"><option>25</option><option>50</option><option>100</option></select></div><div id="portfolio-states" class="scroll"></div><div id="paper-pager" class="pager"></div></article></section>
    <section id="view-canary" class="view"><article class="panel" style="border-color:var(--red)"><div class="section-title"><h2>REAL CANARY MONEY</h2><span class="badge bad">PRODUCTION LIVE TRADING: DISABLED</span></div><div id="canary-summary"></div><div id="canary-trades" class="scroll"></div><p class="notice">Micro-live canary is independent from paper research. No secrets are stored or displayed.</p></article></section>
  </main>
  <script>
    const $ = (id) => document.getElementById(id), safe = (v) => String(v ?? "—").replace(/[&<>"']/g, c => ({ "&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;" }[c])), json = (v) => JSON.stringify(v ?? {}, null, 2);
    const count = (v) => Number.isFinite(Number(v)) ? String(v) : "0", dateText = (v) => v ? new Date(v).toISOString().replace(".000Z","Z") : "—", arr = (v) => Array.isArray(v) ? v : [];
    const empty = (title,body) => `<div class="empty"><strong>${safe(title)}</strong>${safe(body)}</div>`, statusClass = (v) => { const s=String(v||"").toUpperCase(); return ["READY","RUNNING","ACTIVE","COMPLETE","COMPLETED","HEALTHY"].includes(s)?"good":["DEGRADED","STOPPED","UPDATING"].includes(s)?"warn":["ERROR","STALE","REJECTED"].includes(s)?"bad":""; };
    let params = new URLSearchParams(location.search); const state = { tab: params.get("tab") || "overview", page: Math.max(1,Number(params.get("page")||1)), page_size: [10,25,50,100].includes(Number(params.get("page_size"))) ? Number(params.get("page_size")) : 25, filter: params.get("filter") || "", sort: params.get("sort") || "", direction: params.get("direction") === "asc" ? "asc" : "desc", selected: params.get("selected") || "", expanded: params.get("expanded") === "1" };
    let operator = {}, current = {}, loadInFlight = false;
    function saveState(push=false) { const q=new URLSearchParams(); q.set("tab",state.tab); q.set("page",state.page); q.set("page_size",state.page_size); if(state.filter)q.set("filter",state.filter); if(state.sort)q.set("sort",state.sort); if(state.direction!=="desc")q.set("direction",state.direction); if(state.selected)q.set("selected",state.selected); if(state.expanded)q.set("expanded","1"); document.querySelectorAll("select.facet").forEach(el=>{if(el.value)q.set(el.dataset.param||el.id,el.value)}); (push?history.pushState:history.replaceState).call(history,{}, "", `${location.pathname}?${q}`); }
    function activate(tab,push=true) { if(tab!==state.tab){state.selected="";state.expanded=false;state.filter="";state.sort="";state.direction="desc";state.page=1;} state.tab=tab; document.querySelectorAll(".tab").forEach(b=>b.classList.toggle("active",b.dataset.view===tab)); document.querySelectorAll(".view").forEach(v=>v.classList.toggle("active",v.id===`view-${tab}`)); saveState(push); if(tab!=="overview"&&tab!=="btc") loadPage(tab); }
    function sortButton(key,label) { const active=state.sort===key, arrow=active?(state.direction==="asc"?" ▲":" ▼"):""; return `<button class="link sort" data-sort="${safe(key)}">${safe(label)}${arrow}</button>`; }
    function restoreFacets() { document.querySelectorAll("select.facet").forEach(el=>{const value=params.get(el.dataset.param||el.id);if(value!==null&&Array.from(el.options).some(o=>o.value===value))el.value=value;}); document.querySelectorAll('select[id$="-size"]').forEach(el=>{el.value=String(state.page_size);}); document.querySelectorAll(".filters input").forEach(el=>{el.value=state.filter;}); }
    function ensureFacets() { const specs={datasets:[["datasets-market","Market","market",["crypto_spot","prediction"]],["datasets-timeframe","Timeframe","timeframe",["1m","1h","1d","live"]],["datasets-quality","Quality","quality",["OHLCV","PRICE_PROXY","ORDER_BOOK_SIMULATED"]]],polymarket:[["polymarket-settlement","Settlement","settlement",["open","resolved_yes","resolved_no","void"]],["polymarket-quality","Quality","quality",["PRICE_PROXY","ORDER_BOOK_SIMULATED"]]]}; Object.entries(specs).forEach(([view,entries])=>{const host=document.querySelector(`#view-${view} .filters`);if(!host)return;entries.forEach(([id,label,param,options])=>{if($(id))return;const s=document.createElement("select");s.id=id;s.className="facet";s.dataset.param=param;s.innerHTML=`<option value="">All ${label.toLowerCase()}</option>${options.map(o=>`<option value="${safe(o)}">${safe(o)}</option>`).join("")}`;host.appendChild(s);});}); document.querySelectorAll(".filters select").forEach(el=>{el.classList.add("facet");if(el.id.endsWith("-size")){el.dataset.param="page_size";if(!Array.from(el.options).some(o=>o.value==="10")){const option=document.createElement("option");option.value="10";option.textContent="10";el.insertBefore(option,el.firstChild);}}else if(!el.dataset.param)el.dataset.param=el.id.includes("source")?"source_type":el.id.includes("stage")?"stage":el.id.includes("status")?"status":el.id.includes("category")?"category":el.id;}); restoreFacets(); }
    function pager(name,data) { const total=Number(data?.total)||0,page=Number(data?.page)||1,size=Number(data?.page_size)||25,pages=Number(data?.pages)||0,start=total?(page-1)*size+1:0,end=Math.min(page*size,total),windowStart=Math.min(Math.max(1,page-3),Math.max(1,pages-6)); const numbers=pages?Array.from({length:Math.min(pages,7)},(_,i)=>windowStart+i):[]; $(`${name}-pager`).innerHTML=`<span>Showing ${start}–${end} of ${total} · Page ${page} of ${pages||1}</span><span><button data-page="${page-1}" ${page<=1?"disabled":""}>Previous</button> ${numbers.map(n=>`<button data-page="${n}" ${n===page?"disabled":""}>${n}</button>`).join(" ")} <button data-page="${page+1}" ${!pages||page>=pages?"disabled":""}>Next</button></span>`; $(`${name}-pager`).querySelectorAll("button").forEach(b=>b.addEventListener("click",()=>{const next=Number(b.dataset.page);if(name==="dataset-ranges")loadDataset(state.selected,next,false);else if(name==="candidate-events")loadCandidate(state.selected,next,false);else{state.page=next;saveState(true);loadPage(name==="paper"?"portfolio":name);}})); }
    async function fetchV2(name) { const q=new URLSearchParams({page:String(state.page),page_size:String(state.page_size),direction:state.direction}); if(state.filter)q.set("filter",state.filter); if(state.sort)q.set("sort",state.sort); const controls={datasets:[["datasets-source","source_type"],["datasets-market","market"],["datasets-timeframe","timeframe"],["datasets-quality","quality"]],activity:[["activity-status","status"]],candidates:[["candidates-stage","stage"]],polymarket:[["polymarket-category","category"],["polymarket-settlement","settlement"],["polymarket-quality","quality"]],hermes:[["hermes-status","status"]],paper:[["paper-status","status"]]}; for(const [id,key] of (controls[state.tab]||[])){const el=$(id);if(el&&el.value)q.set(key,el.value);} const response=await fetch(`/api/v2/${name}?${q}`,{cache:"no-store"}); if(!response.ok)throw new Error(`${name} HTTP ${response.status}`); return response.json(); }
    function renderComponents(data) { $("component-grid").innerHTML=arr(data.components).map(i=>{const s=String(i.state||"NOT INITIALIZED"),reason=i.detail?.reason||i.detail?.error||"";return `<article class="panel status-card"><div class="status-head"><span class="status-name">${safe(i.name)}</span><span class="badge ${statusClass(s)}">${safe(s)}</span></div><div class="status-value">${safe(i.detail?.status||i.detail?.symbol||"read-only")}</div>${reason?`<p class="page-note">${safe(reason)}</p>`:""}</article>`}).join("")||empty("System not initialized","Start the normal AXIOM node to populate worker status."); }
    function renderOverview(data) { renderComponents(data); const cards=data.research_cards||{}; $("research-cards").innerHTML=[["experiments_run","Experiments run"],["active_hypotheses","Active hypotheses"],["candidates_alive","Candidates alive"],["rejected","Rejected"],["paper_forward","Paper forward"],["paper_promotable","Paper promotable"]].map(([k,l])=>`<article class="panel"><div class="metric">${count(cards[k])}</div><div class="metric-label">${l}</div></article>`).join(""); const c=data.coverage||{}; $("coverage").innerHTML=`<div class="three-col"><div class="key-value"><span class="key">Historical datasets</span><strong>${count(c.historical_count)}</strong></div><div class="key-value"><span class="key">Forward datasets</span><strong>${count(c.forward_count)}</strong></div><div class="key-value"><span class="key">Rows observed</span><strong>${count((c.historical_rows||0)+(c.forward_rows||0))}</strong></div></div><p class="page-note">Prediction market datasets are available in DATASETS; overview intentionally shows summaries only.</p>`; const funnel=data.lifecycle_funnel||{}; const max=Math.max(1,...Object.values(funnel).map(Number)); $("funnel").innerHTML=Object.entries(funnel).map(([k,v])=>`<div class="funnel-row"><span>${safe(k)}</span><span class="funnel-track"><span class="funnel-bar" style="width:${Math.min(100,Number(v)/max*100)}%"></span></span><span class="right">${count(v)}</span></div>`).join("")||empty("No candidate lifecycle","Hermes hypotheses appear after a durable queue item is processed."); $("overview-candidates").innerHTML=tableCandidates(arr(data.candidates).slice(0,10),false); $("overview-activity").innerHTML=arr(data.activity).slice(0,10).map(i=>`<div class="timeline-item"><span class="timeline-time">${safe(dateText(i.timestamp))}</span><span class="timeline-kind">${safe(i.kind)}</span><span>${safe(i.message)}</span></div>`).join("")||empty("No research activity yet","Durable bootstrap, collection, and Hermes activity will appear here."); $("raw-overview").textContent=json(data.raw||{}); }
    function tableCandidates(items,interactive=true) { if(!items.length)return empty("No candidates yet","Submit a bounded paper-only hypothesis through Hermes."); const sortable={strategy_id:"candidate_id",stage:"stage",updated_at:"updated_at"}; return `<table><thead><tr>${[["strategy_id","Strategy"],["family","Family"],["market","Market"],["stage","Stage"],["updated_at","Updated"]].map(([k,l])=>`<th>${interactive&&sortable[k]?sortButton(sortable[k],l):safe(l)}</th>`).join("")}</tr></thead><tbody>${items.map(i=>`<tr><td>${interactive?`<button class="link candidate" data-id="${encodeURIComponent(i.candidate_id||"")}">${safe(i.strategy_id||i.candidate_id)}</button>`:safe(i.strategy_id||i.candidate_id)}</td><td>${safe(i.family)}</td><td>${safe(i.market)}</td><td><span class="badge ${statusClass(i.stage)}">${safe(i.stage)}</span></td><td>${safe(dateText(i.updated_at))}</td></tr>`).join("")}</tbody></table>`; }
    function bindTable() { document.querySelectorAll(".sort").forEach(b=>b.addEventListener("click",()=>{const k=b.dataset.sort;state.direction=state.sort===k&&state.direction==="desc"?"asc":"desc";state.sort=k;state.page=1;saveState(true);loadPage(state.tab)})); document.querySelectorAll(".candidate").forEach(b=>b.addEventListener("click",()=>loadCandidate(decodeURIComponent(b.dataset.id)))); document.querySelectorAll(".dataset").forEach(b=>b.addEventListener("click",()=>loadDataset(decodeURIComponent(b.dataset.id)))); }
    async function loadDataset(id,rangePage=1,persist=true) { state.selected=id; state.expanded=true; if(persist)saveState(true); try { const detailResponse=await fetch(`/api/v2/datasets/${encodeURIComponent(id)}`,{cache:"no-store"}),d=await detailResponse.json(),version=d.dataset_version||d.catalog?.dataset_version||"",rangeQuery=new URLSearchParams({page:String(rangePage),page_size:String(state.page_size)}); if(version)rangeQuery.set("dataset_version",version); const rangesResponse=await fetch(`/api/v2/datasets/${encodeURIComponent(id)}/missing-ranges?${rangeQuery}`,{cache:"no-store"}),rangesData=rangesResponse.ok?await rangesResponse.json():{}; const markup=d.available?`<div class="key-value"><span class="key">Dataset</span><strong>${safe(d.dataset_id||id)}</strong></div><div class="key-value"><span class="key">Version</span><strong>${safe(d.dataset_version||d.catalog?.dataset_version)}</strong></div><div class="key-value"><span class="key">Quality</span><span class="badge">${safe(d.catalog?.quality)}</span></div><details open><summary>Health and missing ranges</summary><pre>${safe(json({health:d.health,missing_ranges:arr(rangesData.items)}))}</pre><div id="dataset-ranges-pager" class="pager"></div></details>`:empty("Dataset unavailable",d.error||"Dataset not found"); $("detail").innerHTML=markup; if($("dataset-detail"))$("dataset-detail").innerHTML=markup; if(d.available&&$("dataset-ranges-pager"))pager("dataset-ranges",rangesData); } catch(e) { const markup=empty("Dataset detail unavailable",e.message); $("detail").innerHTML=markup; if($("dataset-detail"))$("dataset-detail").innerHTML=markup; } }
    async function loadHermes(id,persist=true) { state.selected=id; state.expanded=true; if(persist)saveState(true); try { const r=await fetch(`/api/v2/hermes/${encodeURIComponent(id)}`,{cache:"no-store"}),d=await r.json(); const item=arr(d.items)[0]; $("hermes-detail").innerHTML=item?`<details open><summary>Hermes item ${safe(id)}</summary><pre>${safe(json(item))}</pre></details>`:empty("Hermes item unavailable","The queue item no longer exists."); } catch(e) { $("hermes-detail").innerHTML=empty("Hermes detail unavailable",e.message); } }
    function renderDatasets(data) { $("dataset-total").textContent=`${count(data.total)} datasets`; const rows=arr(data.items); $("datasets-table").innerHTML=rows.length?`<table><thead><tr>${[["dataset_id","Dataset"],["source_type","Source"],["market_type","Market"],["instrument","Instrument"],["timeframe","Timeframe"],["quality","Quality"],["row_count","Rows"],["updated_at","Updated"]].map(([k,l])=>`<th>${sortButton(k,l)}</th>`).join("")}</tr></thead><tbody>${rows.map(i=>`<tr><td><button class="link dataset" data-id="${encodeURIComponent(i.dataset_id||"")}">${safe(i.dataset_id)}</button></td><td>${safe(i.source_type)}</td><td>${safe(i.market_type)}</td><td>${safe(i.instrument)}</td><td>${safe(i.timeframe)}</td><td>${safe(i.quality)}</td><td>${count(i.row_count)}</td><td>${safe(dateText(i.updated_at))}</td></tr>`).join("")}</tbody></table>`:empty("No datasets","Catalog history has not been initialized."); pager("datasets",data); bindTable(); if(state.tab==="datasets"&&state.selected)loadDataset(state.selected,1,false); }
    function renderActivity(data) { $("activity-total").textContent=`${count(data.total)} events`; $("activity-table").innerHTML=arr(data.items).length?`<table><thead><tr>${[["timestamp","Time"],["kind","Kind"],["message","Activity"]].map(([k,l])=>`<th>${k==="message"?safe(l):sortButton(k,l)}</th>`).join("")}</tr></thead><tbody>${arr(data.items).map(i=>`<tr><td>${safe(dateText(i.timestamp))}</td><td>${safe(i.kind)}</td><td>${safe(i.message)}${i.details&&Object.keys(i.details).length?` <details><summary>details</summary><pre>${safe(json(i.details))}</pre></details>`:""}</td></tr>`).join("")}</tbody></table>`:empty("No research activity","Durable activity will appear after workers run."); pager("activity",data); bindTable(); }
    function renderCandidates(data) { $("candidate-total").textContent=`${count(data.total)} candidates`; const stages=[...new Set(arr(data.items).map(i=>i.stage).filter(Boolean))].sort(),select=$("candidates-stage"),selected=select.value||params.get("stage")||""; select.innerHTML=`<option value="">All stages</option>${stages.map(s=>`<option value="${safe(s)}">${safe(s)}</option>`).join("")}`; if(selected&&!stages.includes(selected))select.insertAdjacentHTML("beforeend",`<option value="${safe(selected)}">${safe(selected)}</option>`); select.value=selected; $("candidates-table").innerHTML=tableCandidates(arr(data.items)); pager("candidates",data); bindTable(); if(state.tab==="candidates"&&state.selected)loadCandidate(state.selected,1,false); }
    function renderPolymarket(data) { const items=arr(data.items),categories=[...new Set(items.map(i=>i.category).filter(Boolean))].sort(),cat=$("polymarket-category"),old=cat.value||params.get("category")||""; cat.innerHTML=`<option value="">All categories</option>${categories.map(c=>`<option value="${safe(c)}">${safe(c)}</option>`).join("")}`; if(old&&!categories.includes(old))cat.insertAdjacentHTML("beforeend",`<option value="${safe(old)}">${safe(old)}</option>`); cat.value=old; const quality=items.map(i=>i.quality||i.research_quality).find(Boolean)||"—"; $("pm-summary").innerHTML=`<div class="three-col"><div class="key-value"><span class="key">Markets</span><strong>${count(data.total)}</strong></div><div class="key-value"><span class="key">Page</span><strong>${count(data.page)}</strong></div><div class="key-value"><span class="key">Quality</span><strong>${safe(quality)}</strong></div></div>`; const sortable={market_id:"market_id",category:"category",settlement:"settlement",quality:"quality"}; $("pm-markets").innerHTML=items.length?`<table><thead><tr>${[["market_id","Market"],["question","Question"],["category","Category"],["yes_mid","YES"],["liquidity","Liquidity"],["settlement","Settlement"],["quality","Quality"]].map(([k,l])=>`<th>${sortable[k]?sortButton(sortable[k],l):safe(l)}</th>`).join("")}</tr></thead><tbody>${items.map(i=>`<tr><td>${safe(i.market_id)}</td><td><details><summary>${safe(String(i.question||i.snapshot?.question||i.market_id).slice(0,90))}</summary><p class="page-note">${safe(i.question||i.snapshot?.question||i.market_id)}</p></details></td><td>${safe(i.category)}</td><td>${safe(i.yes_mid??i.snapshot?.yes_mid??i.payload?.snapshot?.yes_mid)}</td><td>${safe(i.liquidity??i.snapshot?.liquidity??i.payload?.snapshot?.liquidity)}</td><td>${safe(i.settlement??i.snapshot?.settlement??i.payload?.snapshot?.settlement)}</td><td>${safe(i.quality||i.research_quality||"—")}</td></tr>`).join("")}</tbody></table>`:empty("No forward market observations","Run the normal node or collect-data for forward-only quotes."); pager("polymarket",data); bindTable(); }
    function renderHermes(data) { const h=operator.hermes||{}; $("hermes-summary").innerHTML=`<div class="three-col"><div class="key-value"><span class="key">Submitted</span><strong>${count(h.submitted)}</strong></div><div class="key-value"><span class="key">Accepted</span><strong>${count(h.accepted)}</strong></div><div class="key-value"><span class="key">Pending</span><strong>${count(h.pending)}</strong></div></div><p class="page-note">Hermes status reflects queue execution state, not integration availability. ${safe(h.reason||"")}</p>`; $("hermes-table").innerHTML=arr(data.items).length?`<table><thead><tr>${[["item_id","Item"],["item_type","Type"],["status","Status"],["created_at","Created"]].map(([k,l])=>`<th>${sortButton(k,l)}</th>`).join("")}</tr></thead><tbody>${arr(data.items).map(i=>`<tr><td><button class="link hermes-item" data-id="${encodeURIComponent(i.item_id||"")}">${safe(i.item_id)}</button></td><td>${safe(i.item_type)}</td><td><span class="badge ${statusClass(i.status)}">${safe(i.status)}</span></td><td>${safe(dateText(i.created_at||i.updated_at))}</td></tr>`).join("")}</tbody></table>`:empty("Hermes not initialized","Start the research node or submit a paper-only proposal."); pager("hermes",data); bindTable(); document.querySelectorAll(".hermes-item").forEach(b=>b.addEventListener("click",()=>loadHermes(decodeURIComponent(b.dataset.id)))); if(state.tab==="hermes"&&state.selected)loadHermes(state.selected,false); }
    async function loadCandidate(id,eventPage=1,persist=true) { state.selected=id; state.expanded=true; if(persist)saveState(true); try { const q=new URLSearchParams({page:String(eventPage),page_size:String(state.page_size)}),r=await fetch(`/api/v2/candidates/${encodeURIComponent(id)}/events?${q}`,{cache:"no-store"}),d=await r.json(); const markup=`<div class="key-value"><span class="key">Candidate</span><strong>${safe(id)}</strong></div>${arr(d.items).length?`<table><thead><tr><th>Time</th><th>Stage</th><th>Reason</th></tr></thead><tbody>${arr(d.items).map(i=>`<tr><td>${safe(dateText(i.created_at||i.timestamp))}</td><td><span class="badge">${safe(i.stage||i.to_stage)}</span></td><td>${safe(i.reason||i.message)}</td></tr>`).join("")}</tbody></table>`:empty("No lifecycle events","No persisted lifecycle evidence exists for this candidate.")}<div id="candidate-events-pager" class="pager"></div>`; $("detail").innerHTML=markup; if(state.tab==="candidates")$("dataset-detail").innerHTML=markup; if($("candidate-events-pager")){const total=Number(d.total)||0,page=Number(d.page)||1,size=Number(d.page_size)||state.page_size,pages=Number(d.pages)||0,start=total?(page-1)*size+1:0,end=Math.min(page*size,total); $("candidate-events-pager").innerHTML=`<span>Showing ${start}–${end} of ${total}</span><span><button data-page="${page-1}" ${page<=1?"disabled":""}>Previous</button> <button data-page="${page+1}" ${!pages||page>=pages?"disabled":""}>Next</button></span>`; $("candidate-events-pager").querySelectorAll("button").forEach(b=>b.addEventListener("click",()=>loadCandidate(id,Number(b.dataset.page),false)));} } catch(e) { $("detail").innerHTML=empty("Candidate detail unavailable",e.message); } }
    function renderPaper(data) { const p=operator.paper_portfolio||{}; $("portfolio-summary").innerHTML=`<div class="card-grid"><div class="panel"><div class="metric">${p.state_count?Number(p.total_equity||0).toFixed(2):"—"}</div><div class="metric-label">paper equity</div></div><div class="panel"><div class="metric">${p.state_count?Number(p.total_pnl||0).toFixed(2):"—"}</div><div class="metric-label">paper P/L</div></div><div class="panel"><div class="metric">${count(data.total)}</div><div class="metric-label">paper records</div></div><div class="panel"><div class="metric">${p.state_count?`${(Number(p.win_rate||0)*100).toFixed(1)}%`:"—"}</div><div class="metric-label">win rate</div></div></div>`; $("portfolio-states").innerHTML=arr(data.items).length?`<table><thead><tr><th>${sortButton("timestamp","Time")}</th><th>${sortButton("record_type","Type")}</th><th>Experiment</th><th>Market</th><th>Status</th><th>Details</th></tr></thead><tbody>${arr(data.items).map(i=>`<tr><td>${safe(dateText(i.timestamp||i.created_at||i.updated_at))}</td><td>${safe(i.record_type)}</td><td>${safe(i.experiment_id)}</td><td>${safe(i.market_id||i.symbol)}</td><td><span class="badge ${statusClass(i.status)}">${safe(i.status)}</span></td><td><details><summary>view</summary><pre>${safe(json(i))}</pre></details></td></tr>`).join("")}</tbody></table>`:empty("Waiting for PAPER_FORWARD","Paper portfolio initializes only after a candidate enters PAPER_FORWARD and observations are persisted."); pager("paper",data); bindTable(); }
    async function loadPage(tab) { const endpoint={datasets:"datasets",activity:"activity",candidates:"candidates",polymarket:"polymarket",hermes:"hermes",portfolio:"paper"}[tab]; if(!endpoint)return; try { current=await fetchV2(endpoint); ({datasets:renderDatasets,activity:renderActivity,candidates:renderCandidates,polymarket:renderPolymarket,hermes:renderHermes,portfolio:renderPaper}[tab])(current); } catch(e) { const target={datasets:"datasets-table",activity:"activity-table",candidates:"candidates-table",polymarket:"pm-markets",hermes:"hermes-table",portfolio:"portfolio-states"}[tab]; if($(target))$(target).innerHTML=empty("Dashboard data unavailable",e.message); } }
    function renderCanary(data) { const c=data.canary||{}; $("canary-summary").innerHTML=`<div class="card-grid"><div class="panel"><div class="metric">${safe(c.micro_live_canary||"DISARMED")}</div><div class="metric-label">Micro live canary</div></div><div class="panel"><div class="metric">${safe(c.candidate)}</div><div class="metric-label">Candidate · expires ${safe(c.expiry)}</div></div><div class="panel"><div class="metric">${count(c.today_orders)}</div><div class="metric-label">Today's orders</div></div><div class="panel"><div class="metric">$${Number(c.today_realized_pnl||0).toFixed(2)}</div><div class="metric-label">Today's realized P/L</div></div><div class="panel"><div class="metric">$${Number(c.total_exposure||0).toFixed(2)}</div><div class="metric-label">Total exposure · ${count(c.open_positions)} positions</div></div><div class="panel"><div class="metric">$${Number(c.daily_loss_budget_remaining||0).toFixed(2)}</div><div class="metric-label">Daily loss budget remaining</div></div></div>`; $("canary-trades").innerHTML=arr(c.trades).length?`<table><thead><tr><th>Time</th><th>Candidate</th><th>Market</th><th>Side</th><th>Notional</th><th>Paper expected price</th><th>Actual price</th><th>Difference</th><th>Status</th><th>P/L</th></tr></thead><tbody>${arr(c.trades).map(i=>`<tr><td>${safe(dateText(i.timestamp))}</td><td>${safe(i.candidate_id)}</td><td>${safe(i.market_id)}</td><td>${safe(i.side)}</td><td>${safe(i.requested_notional)}</td><td>${safe(i.paper_expected_price)}</td><td>${safe(i.actual_average_price)}</td><td>${safe(i.price_difference)}</td><td>${safe(i.status)}</td><td>${safe(i.realized_pnl)}</td></tr>`).join("")}</tbody></table>`:empty("No real canary trades","Arm an eligible candidate explicitly; paper research continues independently."); }
    function renderBtc(data) { const b=operator.btc||{},summary=b.catalog_summary||{},rows=arr(summary.latest_by_timeframe||summary.timeframes),fallback=arr(b.catalog),catalogRows=rows.length?rows:fallback; $("btc-summary").innerHTML=catalogRows.length?`<div class="three-col"><div class="key-value"><span class="key">Catalog timeframes</span><strong>${count(catalogRows.length)}</strong></div><div class="key-value"><span class="key">Rows observed</span><strong>${count(catalogRows.reduce((total,item)=>total+Number(item.row_count||0),0))}</strong></div><div class="key-value"><span class="key">Latest report</span><strong>${safe(dateText(b.latest_report?.created_at))}</strong></div></div>`:empty("BTC history not initialized","Run bootstrap-history --crypto, then btc-research."); $("btc-experiments").innerHTML=""; }
    async function load() { if(loadInFlight)return; loadInFlight=true; try { const response=await fetch("/api/operator",{cache:"no-store"}); if(!response.ok)throw new Error(`operator HTTP ${response.status}`); operator=await response.json(); renderOverview(operator); renderBtc(operator); renderCanary(operator); if(!["overview","btc","canary"].includes(state.tab))await loadPage(state.tab); } catch(e) { $("component-grid").innerHTML=empty("Dashboard unavailable",e.message); } finally { loadInFlight=false; } }
    ensureFacets(); document.querySelectorAll(".tab").forEach(b=>b.addEventListener("click",()=>activate(b.dataset.view))); document.querySelectorAll("[data-link]").forEach(b=>b.addEventListener("click",e=>{e.preventDefault();activate(b.dataset.link)})); document.querySelectorAll(".filters input,.filters select").forEach(el=>el.addEventListener(el.tagName==="INPUT"?"input":"change",()=>{if(el.id.endsWith("-size")){const n=Number(el.value);if([10,25,50,100].includes(n)){state.page_size=n;document.querySelectorAll('select[id$="-size"]').forEach(s=>s.value=String(n));}} else if(el.id.includes("-filter"))state.filter=el.value;state.page=1;saveState(true);loadPage(state.tab)})); window.addEventListener("popstate",()=>{const q=new URLSearchParams(location.search),nextTab=q.get("tab")||"overview",changed=nextTab!==state.tab;params=q;state.tab=nextTab;state.page=Math.max(1,Number(q.get("page")||1));state.page_size=[10,25,50,100].includes(Number(q.get("page_size")))?Number(q.get("page_size")):25;state.filter=changed?"":q.get("filter")||"";state.sort=changed?"":q.get("sort")||"";state.direction=changed?"desc":q.get("direction")==="asc"?"asc":"desc";state.selected=changed?"":q.get("selected")||"";state.expanded=changed?false:q.get("expanded")==="1";restoreFacets();activate(state.tab,false)}); load(); activate(state.tab,false); const refreshHandle=setInterval(load,10000); window.addEventListener("beforeunload",()=>clearInterval(refreshHandle));
    // setInterval(load, 10000) is the ten-second refresh contract.
  </script>
</body>
</html>"""

class _DashboardHandler(BaseHTTPRequestHandler):
    server: "_BoundDashboardServer"

    def log_message(self, format: str, *args: Any) -> None:
        return

    def _send(self, status: int, payload: Any, content_type: str = "application/json; charset=utf-8") -> None:
        body = payload.encode("utf-8") if isinstance(payload, str) else json.dumps(_jsonable(payload), sort_keys=True, indent=2, allow_nan=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802 - stdlib handler API
        parsed = urlparse(self.path)
        path = parsed.path.strip("/")
        if path in {"", "index.html"}:
            self._send(200, _dashboard_html(), "text/html; charset=utf-8")
            return
        query = parse_qs(parsed.query, keep_blank_values=True)
        if path.startswith("api/v2/"):
            endpoint = path[len("api/v2/") :]
            allowed = endpoint.lower() in _V2_ENDPOINTS or endpoint.lower().startswith(("datasets/", "candidates/", "hermes/"))
            if allowed:
                validation_error = _pagination_error(query)
                if validation_error:
                    self._send(400, {"error": "invalid pagination", "detail": validation_error})
                    return
                try:
                    self._send(200, self.server.dashboard_data.v2_snapshot(endpoint, query))
                except ValueError as exc:
                    self._send(400, {"error": "invalid request", "detail": str(exc)})
                except Exception as exc:
                    self._send(503, {"error": "data unavailable", "detail": str(exc)})
                return
        endpoint = path[4:] if path.startswith("api/") else path
        dynamic_strategy = endpoint.lower().startswith("strategy/") and len(endpoint.split("/", 1)[1]) > 0
        if endpoint in _ENDPOINTS or dynamic_strategy:
            try:
                if dynamic_strategy:
                    endpoint = "strategy/" + unquote(endpoint.split("/", 1)[1])
                self._send(200, self.server.dashboard_data.snapshot(endpoint))
            except Exception as exc:
                self._send(503, {"error": "data unavailable", "detail": str(exc)})
            return
        self._send(404, {"error": "not found", "endpoints": ["/", *[f"/api/{name}" for name in _ENDPOINTS], *[f"/api/v2/{name}" for name in _V2_ENDPOINTS]]})

class _BoundDashboardServer(ThreadingHTTPServer):
    def __init__(self, address: tuple[str, int], data: DashboardData) -> None:
        super().__init__(address, _DashboardHandler)
        self.dashboard_data = data
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
