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
import subprocess
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from types import MappingProxyType
from typing import Any, Mapping
from urllib.parse import urlparse

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
            "workers": normalized_workers,
            "cycles": self.store.list_collection_cycles(limit=20),
            "queue": self.store.research_queue_stats(),
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
        records = self.store.list_dataset_catalog(limit=10_000)
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
        records = self.store.load_candidate_lifecycle(limit=10_000)
        records = records if isinstance(records, list) else []
        rows: list[dict[str, Any]] = []
        for item in records:
            payload = item.get("payload", {}) if isinstance(item, Mapping) else {}
            payload = payload if isinstance(payload, Mapping) else {}
            candidate_id = str(item.get("candidate_id", ""))
            rows.append(
                {
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
            )
        return rows

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

    def btc_research_data(self) -> dict[str, Any]:
        catalogs = self.dataset_catalog_data().get("historical", [])
        btc_catalogs = [
            item
            for item in catalogs
            if str(item.get("market_type", "")).lower() == "crypto_spot"
            and str(item.get("instrument", "")).replace("/", "").replace("-", "").upper() == "BTCUSDT"
        ]
        reports: list[dict[str, Any]] = []
        if self.store is not None:
            for item in self.store.list_reports(limit=100, newest_first=True):
                report = item.get("report", {})
                if isinstance(report, Mapping) and report.get("kind") == "btc_historical_walk_forward":
                    reports.append({"report_id": item.get("report_id"), "report": report, "created_at": item.get("created_at")})
        latest = reports[0] if reports else None
        return {
            "available": bool(btc_catalogs),
            "catalog": btc_catalogs,
            "latest_report": latest,
            "reports": reports[:20],
            "live_execution": False,
        }

    def polymarket_research_data(self) -> dict[str, Any]:
        catalogs = self.dataset_catalog_data()
        historical = [
            item
            for item in catalogs.get("historical", [])
            if str(item.get("market_type", "")).lower() == "prediction"
        ]
        current = self.prediction()
        aggregate = next((item for item in historical if item.get("dataset_id") == "Polymarket-historical"), None)
        return {
            "available": bool(historical or (isinstance(current, Mapping) and current.get("markets"))),
            "current": current,
            "historical_catalog": historical,
            "historical_aggregate": aggregate,
            "historical_markets": sum(1 for item in historical if str(item.get("dataset_id", "")).startswith("prediction:")),
            "historical_price_points": int((aggregate or {}).get("row_count", 0)),
            "research_quality": str((aggregate or {}).get("quality") or "PRICE_PROXY"),
            "historical_order_book_available": bool((aggregate or {}).get("metadata", {}).get("historical_order_book_available", False)),
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

    def operator_data(self) -> dict[str, Any]:
        configured = self._configured("operator")
        if configured is not None:
            return dict(configured) if isinstance(configured, Mapping) else {"value": configured}
        catalogs = self.dataset_catalog_data()
        summary = self.research_summary_data()
        status = self.status_data() if self.store is not None else {"status": "not_started", "workers": []}
        workers = status.get("workers", []) if isinstance(status, Mapping) else []
        worker_map = {
            str(item.get("worker_name")): item
            for item in workers
            if isinstance(item, Mapping)
        }
        bootstrap_states = self.store.list_dataset_bootstrap_states(limit=1000) if self.store is not None else []
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
        candidate_rows = self._candidate_rows()
        count = self.store.dashboard_summary() if self.store is not None else {}
        hermes = summary.get("hermes", {}) if isinstance(summary, Mapping) else {}
        paper = self._paper_portfolio()

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
        crypto_label = "READY" if catalogs.get("historical_count", 0) else ("UPDATING" if any(str(item.get("status")) == "RUNNING" for item in btc_states) else "NOT INITIALIZED")
        polymarket_label = "READY" if catalogs.get("forward_count", 0) else ("UPDATING" if any(str(item.get("status")) == "RUNNING" for item in forward_states) else "NOT INITIALIZED")
        hermes_label = "READY" if hermes.get("submitted", 0) or "research-queue" in worker_map else "NOT INITIALIZED"
        paper_label = "ACTIVE" if paper.get("states") else "NOT INITIALIZED"
        return {
            "title": "AXIOM / operator research console",
            "live_trading": {"status": "Disabled", "enabled": False},
            "paper_risk_engine": {"status": "Active", "enabled": True},
            "components": [
                component("AXIOM NODE", node_label, status.get("status") if isinstance(status, Mapping) else None),
                component("POLYMARKET COLLECTOR", polymarket_label, {"forward_catalogs": catalogs.get("forward_count", 0)}),
                component("CRYPTO DATA", crypto_label, {"historical_catalogs": catalogs.get("historical_count", 0)}),
                component("HERMES", hermes_label, hermes),
                component("PAPER ENGINE", paper_label, {"states": len(paper.get("states", []))}),
            ],
            "research_cards": {
                "experiments_run": int(count.get("experiments", 0)),
                "active_hypotheses": int(hermes.get("pending", 0)),
                "candidates_alive": sum(value for key, value in stages.items() if key != "REJECTED"),
                "rejected": stages["REJECTED"],
                "paper_forward": stages["PAPER_FORWARD"],
                "paper_promotable": stages["PAPER_PROMOTABLE"],
            },
            "coverage": catalogs,
            "activity": self._activity_feed(),
            "lifecycle_funnel": stages,
            "candidates": candidate_rows,
            "btc": self.btc_research_data(),
            "polymarket": self.polymarket_research_data(),
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
    """Return the read-only operator dashboard surface."""
    return """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>AXIOM / Operator Research Console</title>
  <style>
    :root {
      color-scheme: dark;
      --bg: #080d17;
      --panel: #101827;
      --panel-2: #0c1422;
      --line: #213047;
      --text: #e8eef8;
      --muted: #8b9ab0;
      --blue: #67b7ff;
      --cyan: #58e0d0;
      --green: #65d39b;
      --amber: #f4bf64;
      --red: #f27d8d;
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, Segoe UI, sans-serif;
    }
    * { box-sizing: border-box; }
    body { margin: 0; background: radial-gradient(circle at 85% 0%, #13233d 0, var(--bg) 36rem); color: var(--text); }
    header { border-bottom: 1px solid var(--line); background: rgba(8, 13, 23, .92); position: sticky; top: 0; z-index: 2; backdrop-filter: blur(12px); }
    .topbar, main { max-width: 1500px; margin: 0 auto; padding-left: 28px; padding-right: 28px; }
    .topbar { padding-top: 22px; padding-bottom: 16px; display: flex; align-items: flex-start; justify-content: space-between; gap: 20px; }
    h1, h2, h3, p { margin: 0; }
    h1 { font-size: 1.35rem; letter-spacing: .08em; text-transform: uppercase; }
    h2 { font-size: .97rem; letter-spacing: .04em; text-transform: uppercase; color: #d5e2f4; }
    h3 { font-size: .85rem; color: var(--muted); text-transform: uppercase; letter-spacing: .08em; }
    .eyebrow { color: var(--blue); font-size: .7rem; letter-spacing: .15em; text-transform: uppercase; margin-bottom: 8px; }
    .subtitle, .muted { color: var(--muted); }
    .subtitle { margin-top: 6px; font-size: .88rem; }
    .live-lock { border: 1px solid #276a62; background: #0c2b2c; color: #9bf1d3; border-radius: 6px; padding: 10px 12px; font-size: .72rem; text-transform: uppercase; letter-spacing: .09em; white-space: nowrap; }
    nav { display: flex; gap: 4px; padding: 0 28px 12px; max-width: 1500px; margin: 0 auto; overflow-x: auto; }
    nav button, button.link { border: 1px solid transparent; color: var(--muted); background: transparent; cursor: pointer; }
    nav button { padding: 8px 12px; border-radius: 5px; font-size: .75rem; letter-spacing: .06em; text-transform: uppercase; }
    nav button:hover, nav button.active { color: var(--text); border-color: var(--line); background: #122039; }
    main { padding-top: 24px; padding-bottom: 60px; }
    .view { display: none; }
    .view.active { display: block; }
    .status-grid, .card-grid, .two-col, .three-col { display: grid; gap: 12px; }
    .status-grid { grid-template-columns: repeat(5, minmax(150px, 1fr)); margin-bottom: 16px; }
    .card-grid { grid-template-columns: repeat(6, minmax(120px, 1fr)); margin-bottom: 16px; }
    .two-col { grid-template-columns: minmax(0, 1.4fr) minmax(300px, .8fr); }
    .three-col { grid-template-columns: repeat(3, minmax(0, 1fr)); }
    .panel { border: 1px solid var(--line); background: linear-gradient(145deg, rgba(16, 24, 39, .96), rgba(10, 17, 29, .96)); border-radius: 8px; padding: 16px; min-width: 0; box-shadow: 0 10px 34px rgba(0, 0, 0, .14); }
    .panel + .panel { margin-top: 12px; }
    .status-card { padding: 13px 14px; }
    .status-head { display: flex; align-items: center; justify-content: space-between; gap: 8px; }
    .status-name { font-size: .72rem; color: var(--muted); letter-spacing: .07em; text-transform: uppercase; }
    .status-value { margin-top: 13px; font-size: .93rem; font-weight: 650; }
    .badge { display: inline-block; border-radius: 999px; padding: 3px 7px; font-size: .63rem; letter-spacing: .05em; text-transform: uppercase; border: 1px solid var(--line); color: var(--muted); }
    .badge.good { color: #9bf1d3; border-color: #276a62; background: #102d2d; }
    .badge.warn { color: #ffd99a; border-color: #765424; background: #2d2414; }
    .badge.bad { color: #ffb4bd; border-color: #713844; background: #2d161d; }
    .metric { font-size: 1.55rem; font-variant-numeric: tabular-nums; margin-top: 10px; }
    .metric-label { color: var(--muted); font-size: .72rem; margin-top: 4px; }
    .section-title { display: flex; align-items: baseline; justify-content: space-between; gap: 12px; margin-bottom: 12px; }
    .empty { border: 1px dashed #33445d; border-radius: 6px; color: var(--muted); padding: 18px; font-size: .83rem; line-height: 1.5; background: var(--panel-2); }
    .empty strong { color: var(--text); display: block; margin-bottom: 5px; }
    table { width: 100%; border-collapse: collapse; font-size: .78rem; }
    th, td { text-align: left; padding: 9px 8px; border-bottom: 1px solid #1c2a3f; white-space: nowrap; }
    th { color: var(--muted); font-weight: 600; text-transform: uppercase; letter-spacing: .06em; font-size: .64rem; }
    tbody tr:hover { background: #142139; }
    .scroll { overflow-x: auto; }
    button.link { color: var(--blue); padding: 0; font: inherit; text-align: left; }
    button.link:hover { text-decoration: underline; }
    input, select { border: 1px solid var(--line); border-radius: 5px; background: #0a1220; color: var(--text); padding: 7px 9px; font-size: .75rem; }
    .filters { display: flex; gap: 8px; flex-wrap: wrap; margin-bottom: 10px; }
    .timeline { display: grid; gap: 2px; }
    .timeline-item { display: grid; grid-template-columns: 92px 76px minmax(0, 1fr); gap: 10px; padding: 9px 0; border-bottom: 1px solid #1c2a3f; align-items: baseline; }
    .timeline-time { color: var(--muted); font-size: .67rem; font-variant-numeric: tabular-nums; }
    .timeline-kind { color: var(--cyan); text-transform: uppercase; letter-spacing: .06em; font-size: .63rem; }
    .timeline-message { font-size: .78rem; }
    .funnel { display: grid; gap: 7px; }
    .funnel-row { display: grid; grid-template-columns: 145px 1fr 36px; gap: 8px; align-items: center; font-size: .71rem; }
    .funnel-track { height: 9px; background: #172238; border-radius: 5px; overflow: hidden; }
    .funnel-bar { height: 100%; background: linear-gradient(90deg, var(--blue), var(--cyan)); border-radius: 5px; min-width: 0; }
    .chart { width: 100%; height: 170px; display: block; border-radius: 6px; background: #0a1220; }
    .chart polyline { fill: none; stroke: var(--blue); stroke-width: 2.5; }
    .chart line { stroke: #22334d; stroke-width: 1; }
    .chart text { fill: var(--muted); font-size: 8px; }
    .key-value { display: grid; grid-template-columns: 150px minmax(0, 1fr); gap: 8px; font-size: .78rem; }
    .key-value + .key-value { margin-top: 8px; }
    .key { color: var(--muted); }
    details { margin-top: 16px; }
    summary { color: var(--muted); cursor: pointer; font-size: .75rem; }
    pre { margin: 10px 0 0; max-height: 430px; overflow: auto; white-space: pre-wrap; word-break: break-word; color: #b7c7dc; font-size: .7rem; line-height: 1.45; }
    .page-note { color: var(--muted); font-size: .78rem; line-height: 1.55; margin-top: 10px; }
    .notice { border-left: 3px solid var(--amber); padding: 9px 12px; color: #e7d4a8; background: #211b10; font-size: .76rem; line-height: 1.45; }
    .right { text-align: right; }
    @media (max-width: 1050px) { .status-grid { grid-template-columns: repeat(3, 1fr); } .card-grid { grid-template-columns: repeat(3, 1fr); } .two-col, .three-col { grid-template-columns: 1fr; } }
    @media (max-width: 620px) { .topbar, main { padding-left: 15px; padding-right: 15px; } nav { padding-left: 15px; padding-right: 15px; } .status-grid, .card-grid { grid-template-columns: repeat(2, 1fr); } .timeline-item { grid-template-columns: 72px 60px minmax(0, 1fr); } }
  </style>
</head>
<body>
  <header>
    <div class="topbar">
      <div><div class="eyebrow">Read-only research operations</div><h1>AXIOM / operator console</h1><p class="subtitle">Historical evidence, forward observation, and paper lifecycle in one view.</p></div>
      <div class="live-lock">Live trading <strong>Disabled</strong><br>Paper risk engine <strong>Active</strong></div>
    </div>
    <nav aria-label="Research sections">
      <button class="tab active" data-view="overview">Overview</button>
      <button class="tab" data-view="btc">BTC Research</button>
      <button class="tab" data-view="polymarket">Polymarket</button>
      <button class="tab" data-view="portfolio">Paper Portfolio</button>
      <button class="tab" data-view="hermes">Hermes</button>
    </nav>
  </header>
  <main>
    <section id="view-overview" class="view active">
      <div id="component-grid" class="status-grid"></div>
      <div id="research-cards" class="card-grid"></div>
      <div class="two-col">
        <div>
          <article class="panel"><div class="section-title"><h2>Historical / forward coverage</h2><span id="coverage-summary" class="muted"></span></div><div id="coverage"></div></article>
          <article class="panel"><div class="section-title"><h2>Candidate lifecycle funnel</h2><span class="muted">current durable stage</span></div><div id="funnel" class="funnel"></div></article>
          <article class="panel"><div class="section-title"><h2>Candidate strategies</h2><span class="muted">select a row for evidence detail</span></div><div class="filters"><input id="candidate-filter" placeholder="Filter strategy, family, market" aria-label="Filter candidates"><select id="candidate-stage" aria-label="Filter candidate stage"><option value="">All stages</option></select></div><div id="candidates" class="scroll"></div></article>
        </div>
        <article class="panel"><div class="section-title"><h2>Research activity</h2><span id="last-refreshed" class="muted">waiting</span></div><div id="activity" class="timeline"></div></article>
        <article class="panel"><div class="section-title"><h2>Strategy detail</h2><span class="muted">historical → forward → lifecycle</span></div><div id="strategy-detail" class="empty"><strong>Select a candidate</strong>Its hypothesis, parameters, evidence, lineage, rejection reason, and paper-only forward state will appear here.</div></article>
      </div>
      <details><summary>Technical details · raw APIs and retained debug surfaces</summary><p class="page-note">Existing JSON APIs remain available for automation. The older labels “Research maturity”, “Paper forward”, and “Research queue and node status” are retained below as raw endpoint links.</p><pre id="raw-overview"></pre></details>
    </section>
    <section id="view-btc" class="view">
      <article class="panel"><div class="section-title"><h2>BTC / USDT historical research</h2><span class="badge good">historical only</span></div><div id="btc-summary"></div><div id="btc-chart"></div><div id="btc-experiments" class="scroll"></div><div class="notice">Walk-forward results use deterministic existing strategy families. Fees, slippage, turnover, drawdown, expectancy, Sharpe, Sortino, and stability are reported; no live execution path exists.</div></article>
    </section>
    <section id="view-polymarket" class="view">
      <article class="panel"><div class="section-title"><h2>Polymarket opportunities</h2><span class="badge warn">price proxy unless timestamped depth exists</span></div><div id="pm-summary"></div><div id="pm-markets" class="scroll"></div><p class="page-note">Historical price history is kept separate from forward order-book observations. No historical depth, spread, fills, or executable quotes are fabricated.</p></article>
    </section>
    <section id="view-portfolio" class="view">
      <article class="panel"><div class="section-title"><h2>Paper portfolio</h2><span class="badge good">paper money</span></div><div id="portfolio-summary"></div><div id="portfolio-states" class="scroll"></div></article>
    </section>
    <section id="view-hermes" class="view">
      <article class="panel"><div class="section-title"><h2>Hermes / research loop</h2><span class="badge">no autonomous trading</span></div><div id="hermes-summary"></div><div id="hermes-raw"></div></article>
    </section>
    <details><summary>Raw JSON API links</summary><p id="api-links" class="page-note"></p><pre id="raw-api"></pre></details>
    <p class="muted page-note">Last refresh is read-only and cache-bypassed. Auto-refresh interval: 10 seconds.</p>
  </main>
  <script>
    const $ = (id) => document.getElementById(id);
    const safe = (value) => String(value ?? "—").replace(/[&<>"']/g, (char) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[char]));
    const json = (value) => JSON.stringify(value ?? {}, null, 2);
    const number = (value, digits = 3) => { const n = Number(value); return Number.isFinite(n) ? n.toFixed(digits) : "—"; };
    const count = (value) => { const n = Number(value); return Number.isFinite(n) ? String(n) : "0"; };
    const dateText = (value) => value ? new Date(value).toISOString().replace(".000Z", "Z") : "—";
    const empty = (title, body) => `<div class="empty"><strong>${safe(title)}</strong>${safe(body)}</div>`;
    const statusClass = (value) => { const text = String(value || "").toUpperCase(); return ["READY", "RUNNING", "ACTIVE", "COMPLETE", "HEALTHY"].includes(text) ? "good" : ["DEGRADED", "STOPPED", "UPDATING"].includes(text) ? "warn" : ""; };
    const asArray = (value) => Array.isArray(value) ? value : [];
    let operator = {};
    let candidateSort = { key: "updated_at", direction: -1 };

    function chart(values, label) {
      const points = values.map(Number).filter(Number.isFinite);
      if (!points.length) return empty("No chart yet", "Run BTC historical research after a catalog dataset is available.");
      const low = Math.min(...points), high = Math.max(...points), span = high - low || 1;
      const polyline = points.map((value, index) => `${10 + index * (380 / Math.max(1, points.length - 1))},${145 - ((value - low) / span) * 120}`).join(" ");
      return `<svg class="chart" viewBox="0 0 400 170" role="img" aria-label="${safe(label)}"><line x1="10" y1="25" x2="390" y2="25"></line><line x1="10" y1="145" x2="390" y2="145"></line><polyline points="${polyline}"></polyline><text x="12" y="18">${safe(label)}</text><text x="12" y="163">${number(low, 4)}</text><text x="350" y="163">${number(high, 4)}</text></svg>`;
    }

    function renderComponents(data) {
      $("component-grid").innerHTML = asArray(data.components).map((item) => {
        const state = String(item.state || "NOT INITIALIZED");
        return `<article class="panel status-card"><div class="status-head"><span class="status-name">${safe(item.name)}</span><span class="badge ${statusClass(state)}">${safe(state)}</span></div><div class="status-value">${safe(item.detail?.symbol || item.detail?.status || "read-only")}</div></article>`;
      }).join("") || empty("System not initialized", "Start the normal AXIOM node to populate worker status.");
    }

    function renderCards(cards) {
      const definitions = [["experiments_run", "Experiments run"], ["active_hypotheses", "Active hypotheses"], ["candidates_alive", "Candidates alive"], ["rejected", "Rejected"], ["paper_forward", "Paper forward"], ["paper_promotable", "Paper promotable"]];
      $("research-cards").innerHTML = definitions.map(([key, label]) => `<article class="panel"><div class="metric">${count(cards?.[key])}</div><div class="metric-label">${label}</div></article>`).join("");
    }

    function renderCoverage(coverage) {
      const historical = asArray(coverage?.historical_coverage);
      const forward = asArray(coverage?.forward_coverage);
      $("coverage-summary").textContent = `${count(coverage?.historical_rows)} historical rows · ${count(coverage?.forward_rows)} forward rows`;
      const rows = (items, source) => items.length ? `<h3>${source}</h3><table><thead><tr><th>Dataset</th><th>Instrument</th><th>Timeframe</th><th>Range</th><th>Rows</th><th>Quality</th></tr></thead><tbody>${items.map((item) => `<tr><td>${safe(item.dataset_id)}</td><td>${safe(item.instrument)}</td><td>${safe(item.timeframe)}</td><td>${safe(dateText(item.start))} → ${safe(dateText(item.end))}</td><td>${count(item.rows)}${source === "Historical" ? ` (${number(Number(item.completeness) * 100, 1)}%)` : ""}</td><td>${safe(item.quality)}</td></tr>`).join("")}</tbody></table>` : empty(`${source} coverage not initialized`, source === "Historical" ? "Run bootstrap-history --crypto, then btc-research." : "Run the normal node or collect-data command for forward observations.");
      $("coverage").innerHTML = rows(historical, "Historical") + rows(forward, "Forward collected");
    }

    function renderFunnel(funnel) {
      const entries = Object.entries(funnel || {});
      const maximum = Math.max(1, ...entries.map(([, value]) => Number(value) || 0));
      $("funnel").innerHTML = entries.length ? entries.map(([stage, value]) => `<div class="funnel-row"><span>${safe(stage)}</span><span class="funnel-track"><span class="funnel-bar" style="width:${Math.min(100, ((Number(value) || 0) / maximum) * 100)}%"></span></span><span class="right">${count(value)}</span></div>`).join("") : empty("No candidate lifecycle", "Hermes hypotheses become visible here only after a durable research item is processed.");
    }

    function sortedCandidates(rows) {
      const filter = String($("candidate-filter")?.value || "").toLowerCase();
      const stage = String($("candidate-stage")?.value || "");
      return asArray(rows).filter((row) => (!stage || row.stage === stage) && (!filter || JSON.stringify(row).toLowerCase().includes(filter))).sort((left, right) => {
        const a = left[candidateSort.key] ?? "", b = right[candidateSort.key] ?? "";
        return (a < b ? -1 : a > b ? 1 : 0) * candidateSort.direction;
      });
    }

    function renderCandidates(rows) {
      const allStages = [...new Set(asArray(rows).map((row) => row.stage).filter(Boolean))].sort();
      const select = $("candidate-stage");
      const current = select.value;
      select.innerHTML = `<option value="">All stages</option>${allStages.map((stage) => `<option value="${safe(stage)}">${safe(stage)}</option>`).join("")}`;
      select.value = allStages.includes(current) ? current : "";
      const values = sortedCandidates(rows);
      if (!values.length) { $("candidates").innerHTML = empty("No candidates yet", "Submit a bounded paper-only hypothesis through Hermes; lifecycle evidence appears after the queue worker runs."); return; }
      $("candidates").innerHTML = `<table><thead><tr>${[["strategy_id", "Strategy"], ["family", "Family"], ["market", "Market"], ["stage", "Stage"], ["validation_expectancy", "Val exp."], ["validation_max_drawdown", "Val DD"], ["validation_stability", "Stability"], ["forward_bets", "Fwd bets"], ["data_quality", "Data"], ["updated_at", "Updated"]].map(([key, label]) => `<th><button class="link sort" data-sort="${key}">${label}</button></th>`).join("")}</tr></thead><tbody>${values.map((row) => `<tr><td><button class="link candidate" data-candidate="${encodeURIComponent(row.candidate_id)}">${safe(row.strategy_id)}</button></td><td>${safe(row.family)}</td><td>${safe(row.market)}</td><td><span class="badge">${safe(row.stage)}</span></td><td>${number(row.validation_expectancy)}</td><td>${number(row.validation_max_drawdown)}</td><td>${number(row.validation_stability)}</td><td>${count(row.forward_bets)}</td><td>${safe(row.data_quality)}</td><td>${safe(dateText(row.updated_at))}</td></tr>`).join("")}</tbody></table>`;
      document.querySelectorAll(".sort").forEach((button) => button.addEventListener("click", () => { const key = button.dataset.sort; candidateSort = { key, direction: candidateSort.key === key ? -candidateSort.direction : 1 }; renderCandidates(operator.candidates); }));
      document.querySelectorAll(".candidate").forEach((button) => button.addEventListener("click", () => loadDetail(decodeURIComponent(button.dataset.candidate))));
    }

    function renderDetail(detail) {
      if (!detail?.available) {
        $("strategy-detail").className = "empty";
        $("strategy-detail").innerHTML = empty("Candidate not found", "Choose a row from the sortable candidate table.");
        return;
      }
      const strategy = detail.strategy || {};
      $("strategy-detail").className = "";
      $("strategy-detail").innerHTML = `<div class="key-value"><span class="key">Strategy</span><strong>${safe(strategy.id || detail.candidate_id)}</strong></div><div class="key-value"><span class="key">Family</span><span>${safe(strategy.family)}</span></div><div class="key-value"><span class="key">Stage</span><span class="badge">${safe(detail.stage)}</span></div><div class="key-value"><span class="key">Generation / parent</span><span>${safe(strategy.generation)} / ${safe(strategy.parent_id || "root")}</span></div><div class="key-value"><span class="key">Hypothesis</span><span>${safe(detail.hypothesis || "not recorded")}</span></div><h3 style="margin-top:14px">Historical evidence</h3><pre>${safe(json(detail.historical))}</pre><h3 style="margin-top:14px">Forward paper evidence</h3><pre>${safe(json(detail.forward))}</pre>${detail.rejection_reason ? `<p class="page-note">Rejection: ${safe(detail.rejection_reason)}</p>` : ""}<details><summary>Lineage and lifecycle events</summary><pre>${safe(json({ lineage: detail.lineage, events: detail.lifecycle_events }))}</pre></details>`;
    }

    function renderActivity(items) {
      const values = asArray(items);
      $("activity").innerHTML = values.length ? values.slice(0, 30).map((item) => `<div class="timeline-item"><span class="timeline-time">${safe(dateText(item.timestamp))}</span><span class="timeline-kind">${safe(item.kind)}</span><span class="timeline-message">${safe(item.message)}</span></div>`).join("") : empty("No research activity yet", "Bootstrap, collection, or Hermes activity will appear here with its durable timestamp.");
    }

    function renderBtc(data) {
      const report = data?.latest_report?.report || {};
      const coverage = asArray(data?.catalog);
      const metrics = asArray(report.experiments).map((item) => Number(item.aggregate?.mean_total_return)).filter(Number.isFinite);
      $("btc-summary").innerHTML = coverage.length ? `<div class="three-col"><div class="key-value"><span class="key">Catalog snapshots</span><strong>${count(coverage.length)}</strong></div><div class="key-value"><span class="key">Latest rows</span><strong>${count(coverage[0]?.row_count)}</strong></div><div class="key-value"><span class="key">Research windows</span><strong>${count(report.walk_forward?.windows)}</strong></div></div><p class="page-note">${safe(dateText(coverage[0]?.start_timestamp))} → ${safe(dateText(coverage[0]?.end_timestamp))} · completeness ${number(Number(coverage[0]?.completeness) * 100, 1)}%</p>` : empty("BTC history not initialized", "Run python -m axiom.cli bootstrap-history --crypto --resume, then python -m axiom.cli btc-research.");
      $("btc-chart").innerHTML = chart(metrics, "Mean locked-holdout return by family");
      const experiments = asArray(report.experiments);
      $("btc-experiments").innerHTML = experiments.length ? `<table><thead><tr><th>Family</th><th>Mean return</th><th>Max DD</th><th>Expectancy</th><th>Sharpe</th><th>Sortino</th><th>Turnover</th><th>Stability</th></tr></thead><tbody>${experiments.map((item) => `<tr><td>${safe(item.family)}</td><td>${number(item.aggregate?.mean_total_return)}</td><td>${number(item.aggregate?.mean_max_drawdown)}</td><td>${number(item.aggregate?.mean_expectancy)}</td><td>${number(item.aggregate?.mean_sharpe)}</td><td>${number(item.aggregate?.mean_sortino)}</td><td>${number(item.aggregate?.mean_turnover)}</td><td>${number(item.parameter_stability?.score)}</td></tr>`).join("")}</tbody></table>` : "";
    }

    function renderPolymarket(data) {
      const aggregate = data?.historical_aggregate;
      const current = asArray(data?.current?.markets);
      $("pm-summary").innerHTML = `<div class="three-col"><div class="key-value"><span class="key">Historical markets</span><strong>${count(data?.historical_markets)}</strong></div><div class="key-value"><span class="key">Price points</span><strong>${count(data?.historical_price_points)}</strong></div><div class="key-value"><span class="key">Research quality</span><strong>${safe(data?.research_quality || "PRICE_PROXY")}</strong></div></div><p class="page-note">${aggregate ? `Aggregate range ${safe(dateText(aggregate.start_timestamp))} → ${safe(dateText(aggregate.end_timestamp))}.` : "No historical Polymarket catalog yet."}</p>`;
      $("pm-markets").innerHTML = current.length ? `<table><thead><tr><th>Question</th><th>Category</th><th>YES</th><th>Liquidity</th><th>Settlement</th><th>Quality</th></tr></thead><tbody>${current.slice(0, 50).map((item) => `<tr><td>${safe(item.question || item.market_id)}</td><td>${safe(item.category)}</td><td>${number(item.yes_mid, 4)}</td><td>${number(item.liquidity)}</td><td>${safe(item.settlement)}</td><td>${safe(item.research_quality || "PRICE_PROXY")}</td></tr>`).join("")}</tbody></table>` : empty("No forward market observations", "Run the normal AXIOM node or collect-data to populate forward-only quotes. Historical price-proxy data is shown separately above.");
    }

    function renderPortfolio(data) {
      $("portfolio-summary").innerHTML = `<div class="card-grid"><div class="panel"><div class="metric">${number(data?.total_equity, 2)}</div><div class="metric-label">paper equity</div></div><div class="panel"><div class="metric">${number(data?.total_pnl, 2)}</div><div class="metric-label">paper P/L</div></div><div class="panel"><div class="metric">${count(data?.resolved_bets)}</div><div class="metric-label">resolved bets</div></div><div class="panel"><div class="metric">${number(data?.win_rate * 100, 1)}%</div><div class="metric-label">win rate</div></div></div>`;
      const states = asArray(data?.states);
      $("portfolio-states").innerHTML = states.length ? `<table><thead><tr><th>Experiment</th><th>Equity</th><th>P/L</th><th>Drawdown</th><th>Fills</th><th>Open positions</th><th>Updated</th></tr></thead><tbody>${states.map((item) => `<tr><td>${safe(item.experiment_id)}</td><td>${number(item.equity, 2)}</td><td>${number(item.pnl, 2)}</td><td>${number(item.drawdown * 100, 1)}%</td><td>${count(item.fills)}</td><td>${safe(Object.keys(item.open_positions || {}).length)}</td><td>${safe(dateText(item.updated_at))}</td></tr>`).join("")}</tbody></table>` : empty("Paper portfolio is empty", "Candidates must reach PAPER_FORWARD before paper observations and portfolio state appear.");
    }

    function renderHermes(data) {
      $("hermes-summary").innerHTML = `<div class="three-col"><div class="key-value"><span class="key">Submitted</span><strong>${count(data?.submitted)}</strong></div><div class="key-value"><span class="key">Accepted</span><strong>${count(data?.accepted)}</strong></div><div class="key-value"><span class="key">Pending</span><strong>${count(data?.pending)}</strong></div></div><p class="page-note">Hermes validates bounded hypotheses and time splits; it cannot submit orders or enable live trading.</p>`;
      $("hermes-raw").innerHTML = data && Object.keys(data).length ? `<details open><summary>Hermes evidence</summary><pre>${safe(json(data))}</pre></details>` : empty("Hermes not initialized", "Start the research node or submit a paper-only proposal.");
    }

    async function loadDetail(candidateId) {
      try {
        const response = await fetch(`/api/strategy/${encodeURIComponent(candidateId)}`, { cache: "no-store" });
        const detail = await response.json();
        renderDetail(detail);
        const panel = $("raw-overview");
        panel.textContent = json(detail);
        panel.closest("details").open = true;
      } catch (error) { $("raw-overview").textContent = `Strategy detail unavailable: ${error.message}`; }
    }

    async function load() {
      try {
        const response = await fetch("/api/operator", { cache: "no-store" });
        if (!response.ok) throw new Error(`operator HTTP ${response.status}`);
        operator = await response.json();
        renderComponents(operator);
        renderCards(operator.research_cards);
        renderCoverage(operator.coverage);
        renderFunnel(operator.lifecycle_funnel);
        renderCandidates(operator.candidates);
        renderActivity(operator.activity);
        renderBtc(operator.btc);
        renderPolymarket(operator.polymarket);
        renderPortfolio(operator.paper_portfolio);
        renderHermes(operator.hermes);
        $("last-refreshed").textContent = `refreshed ${new Date().toISOString().replace(".000Z", "Z")}`;
        $("raw-overview").textContent = json(operator.raw);
        $("raw-api").textContent = json(operator);
        $("api-links").innerHTML = ["operator", "datasets", "crypto", "btc-research", "prediction", "polymarket-research", "paper", "paper-portfolio", "hermes", "research-summary", "autonomous-research", "status", "dataset-health"].map((name) => `<a href="/api/${name}">${name}</a>`).join(" · ");
      } catch (error) {
        $("component-grid").innerHTML = empty("Dashboard data unavailable", error.message);
      }
    }

    document.querySelectorAll(".tab").forEach((button) => button.addEventListener("click", () => {
      document.querySelectorAll(".tab").forEach((item) => item.classList.toggle("active", item === button));
      document.querySelectorAll(".view").forEach((view) => view.classList.toggle("active", view.id === `view-${button.dataset.view}`));
    }));
    $("candidate-filter").addEventListener("input", () => renderCandidates(operator.candidates));
    $("candidate-stage").addEventListener("change", () => renderCandidates(operator.candidates));
    load();
    const refreshHandle = setInterval(load, 10000);
    window.addEventListener("beforeunload", () => clearInterval(refreshHandle));
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
        path = urlparse(self.path).path.strip("/")
        if path in {"", "index.html"}:
            self._send(200, _dashboard_html(), "text/html; charset=utf-8")
            return
        endpoint = path[4:] if path.startswith("api/") else path
        dynamic_strategy = endpoint.lower().startswith("strategy/") and len(endpoint.split("/", 1)[1]) > 0
        if endpoint in _ENDPOINTS or dynamic_strategy:
            try:
                self._send(200, self.server.dashboard_data.snapshot(endpoint))
            except Exception as exc:
                self._send(503, {"error": "data unavailable", "detail": str(exc)})
            return
        self._send(404, {"error": "not found", "endpoints": ["/", *[f"/api/{name}" for name in _ENDPOINTS]]})


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
