"""Typed, localhost-only operator controls for the paper research node.

This module deliberately has no shell or browser-provided command execution. The
control surface maps a small allowlist to existing in-process service APIs and
persists every requested action as a bounded audit record.
"""
from __future__ import annotations

from datetime import datetime, timezone
import ipaddress
import os
from pathlib import Path
import re
import subprocess
import sys
import threading
import time
from typing import Any, Callable, Mapping

from .autonomous import AutonomousResearchProcessor
from .bootstrap import BTC_HISTORY_START, HistoricalBootstrapper
from .canary import CanaryBlocked, CanaryService, CredentialStore, PolymarketClobV2Venue
from .crypto_universe import load_crypto_universe
from .data import BinanceAdapter
from .domain import utc_now
from .node import _pid_alive, _pid_matches_node
from .storage import AxiomStore


DEFAULT_HERMES_JOB_ID = "f1d27bf8c27a"
BOOTSTRAP_JOB_NAME = "crypto-universe-bootstrap"
HERMES_STATE_NAME = "hermes-control"
_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,255}$")
_SECRET_KEY = re.compile(r"(?:secret|private|password|token|api[_-]?key|credential|mnemonic)", re.I)
_ALLOWED_ACTIONS = frozenset(
    {
        "node.restart",
        "bootstrap.start",
        "bootstrap.resume",
        "hermes.pause",
        "hermes.resume",
        "hermes.run_now",
        "canary.connectivity_check",
        "canary.eligibility.verify",
        "canary.eligibility.mark",
        "canary.generate_signal",
        "canary.arm",
        "canary.enable_auto",
        "canary.disarm",
        "canary.kill",
    }
)
_CONFIRMATIONS = {
    "canary.eligibility.mark": "MARK CANARY ELIGIBLE",
    "canary.arm": "ARM",
    "canary.enable_auto": "ENABLE AUTO CANARY",
    "canary.disarm": "DISARM",
    "canary.kill": "KILL",
}


class OperatorControlError(RuntimeError):
    """Safe, bounded error returned by an operator action."""

    def __init__(self, code: str, detail: str | None = None) -> None:
        self.code = str(code).strip() or "OPERATOR_ACTION_FAILED"
        super().__init__(detail or self.code)


def _safe_value(value: Any, *, depth: int = 0) -> Any:
    if depth >= 4:
        return "<truncated>"
    if isinstance(value, Mapping):
        return {
            str(key): _safe_value(child, depth=depth + 1)
            for key, child in list(value.items())[:32]
            if not _SECRET_KEY.search(str(key))
        }
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_safe_value(child, depth=depth + 1) for child in list(value)[:32]]
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value if not isinstance(value, str) or len(value) <= 1024 else value[:1021] + "..."
    return str(value)[:1024]


def _safe_identifier(value: Any, field: str) -> str:
    identifier = str(value or "").strip()
    if not identifier or _SAFE_IDENTIFIER.fullmatch(identifier) is None:
        raise OperatorControlError("INVALID_IDENTIFIER", f"invalid {field}")
    return identifier


def _loopback_host(value: str) -> bool:
    if str(value).strip().lower() == "localhost":
        return True
    try:
        return ipaddress.ip_address(str(value).strip()).is_loopback
    except ValueError:
        return False


def _operator_job_payload(job: Mapping[str, Any] | None) -> dict[str, Any]:
    if not isinstance(job, Mapping):
        return {
            "job_name": BOOTSTRAP_JOB_NAME,
            "status": "NOT_STARTED",
            "current_symbol": None,
            "current_timeframe": None,
            "completed_symbols": 0,
            "total_symbols": 0,
            "completed_datasets": 0,
            "total_datasets": 0,
            "started_at": None,
            "updated_at": None,
            "last_error": None,
            "resumable": False,
        }
    payload = job.get("payload") if isinstance(job.get("payload"), Mapping) else {}
    return {
        "job_name": BOOTSTRAP_JOB_NAME,
        "status": str(job.get("status") or "NOT_STARTED").upper(),
        "current_symbol": payload.get("current_symbol"),
        "current_timeframe": payload.get("current_timeframe"),
        "completed_symbols": int(payload.get("completed_symbols", 0) or 0),
        "total_symbols": int(payload.get("total_symbols", 0) or 0),
        "completed_datasets": int(payload.get("completed_datasets", 0) or 0),
        "total_datasets": int(payload.get("total_datasets", 0) or 0),
        "started_at": job.get("started_at"),
        "updated_at": job.get("updated_at"),
        "last_error": job.get("last_error"),
        "resumable": bool(job.get("resumable")),
    }


class HermesOperatorAdapter:
    """Fixed in-process Hermes adapter; it accepts no executable or argv."""

    def __init__(self, store: AxiomStore, job_id: str) -> None:
        self.store = store
        self.job_id = _safe_identifier(job_id, "Hermes job ID")

    def state(self) -> dict[str, Any]:
        raw = self.store.get_scheduler_state(HERMES_STATE_NAME) or {}
        state = dict(raw) if isinstance(raw, Mapping) else {}
        status = str(state.get("status") or "ACTIVE").upper()
        if status not in {"ACTIVE", "PAUSED"}:
            status = "ACTIVE"
        return {
            "job_id": self.job_id,
            "status": status,
            "schedule": state.get("schedule") or "after_each_collection",
            "last_run_at": state.get("last_run_at"),
            "next_run_at": state.get("next_run_at"),
            "last_result": _safe_value(state.get("last_result")),
            "run_requested_at": state.get("run_requested_at"),
        }

    def set_status(self, status: str) -> dict[str, Any]:
        requested = str(status).upper()
        if requested not in {"ACTIVE", "PAUSED"}:
            raise OperatorControlError("INVALID_HERMES_STATUS")
        current = self.store.get_scheduler_state(HERMES_STATE_NAME) or {}
        body = dict(current) if isinstance(current, Mapping) else {}
        body.update({"status": requested, "job_id": self.job_id, "updated_at": utc_now().isoformat()})
        self.store.set_scheduler_state(HERMES_STATE_NAME, body)
        return self.state()

    def run_now(self) -> dict[str, Any]:
        current = self.state()
        if current["status"] == "PAUSED":
            raise OperatorControlError("HERMES_PAUSED", "resume Hermes before running it")
        processor = AutonomousResearchProcessor(self.store)
        cycle = processor.process_pending(worker="operator-hermes")
        now = utc_now().isoformat()
        result = {
            "claimed": int(getattr(cycle, "claimed", 0)),
            "completed": int(getattr(cycle, "completed", 0)),
            "rejected": int(getattr(cycle, "rejected", 0)),
            "failed": int(getattr(cycle, "failed", 0)),
        }
        self.store.set_scheduler_state(
            HERMES_STATE_NAME,
            {
                **(self.store.get_scheduler_state(HERMES_STATE_NAME) or {}),
                "status": "ACTIVE",
                "job_id": self.job_id,
                "last_run_at": now,
                "last_result": result,
                "run_requested_at": None,
                "updated_at": now,
            },
        )
        return {**self.state(), "last_result": result}


class OperatorControlPlane:
    """Authoritative action boundary used by the dashboard and launcher."""

    def __init__(
        self,
        store: AxiomStore,
        *,
        db_path: str | os.PathLike[str] | None = None,
        hermes_job_id: str | None = None,
        node_launcher: Callable[[list[str]], Any] | None = None,
    ) -> None:
        self.store = store
        raw_db = db_path if db_path is not None else getattr(store, "path", "")
        self.db_path = os.path.abspath(os.path.expanduser(str(raw_db))) if str(raw_db) not in {"", ":memory:"} else str(raw_db)
        self._node_launcher = node_launcher or self._spawn_node
        self._lock = threading.RLock()
        self._bootstrap_threads: dict[str, threading.Thread] = {}
        configured = self.store.get_operator_config("hermes_research_job_id", None)
        selected = hermes_job_id or configured or DEFAULT_HERMES_JOB_ID
        self.hermes_job_id = _safe_identifier(selected, "Hermes job ID")
        if hermes_job_id is not None:
            self.configure_hermes_job_id(hermes_job_id)

    def configure_hermes_job_id(self, job_id: str) -> str:
        value = _safe_identifier(job_id, "Hermes job ID")
        self.store.set_operator_config("hermes_research_job_id", value)
        self.hermes_job_id = value
        return value

    def _node_lock_path(self) -> Path:
        return Path(str(self.db_path) + ".lock")

    def _node_status(self) -> dict[str, Any]:
        lock_path = self._node_lock_path()
        lock_pid = 0
        lock_text = ""
        try:
            lock_text = lock_path.read_text(encoding="ascii")
            lock_pid = int(lock_text.splitlines()[0].strip())
        except (FileNotFoundError, OSError, ValueError):
            pass
        workers = self.store.list_worker_states(limit=2048)
        root = next((item for item in workers if str(item.get("worker_name")) == "axiom-node"), {})
        worker_payload = root.get("payload") if isinstance(root.get("payload"), Mapping) else {}
        persisted_pid = worker_payload.get("pid")
        try:
            pid = int(lock_pid or persisted_pid or 0)
        except (TypeError, ValueError):
            pid = 0
        identity_valid = bool(pid and _pid_matches_node(pid, self.db_path))
        alive = bool(pid and _pid_alive(pid))
        persisted_status = str(root.get("status") or "").lower()
        if alive and identity_valid:
            state = "RUNNING"
        elif persisted_status in {"running", "degraded"} and (pid or lock_path.exists()):
            state = "STALE"
        else:
            state = "STOPPED"
        return {
            "status": state,
            "pid": pid or None,
            "started_at": root.get("started_at"),
            "heartbeat_at": root.get("heartbeat_at"),
            "lock_path": str(lock_path),
            "lock_exists": lock_path.exists(),
            "worker_status": root.get("status"),
            "worker_alive": alive,
            "worker_identity_valid": identity_valid,
        }

    def _clear_stale_lock(self) -> None:
        path = self._node_lock_path()
        try:
            text = path.read_text(encoding="ascii")
            pid = int(text.splitlines()[0].strip())
        except (FileNotFoundError, OSError, ValueError):
            return
        if _pid_alive(pid) and _pid_matches_node(pid, self.db_path):
            raise OperatorControlError("NODE_ALREADY_RUNNING")
        try:
            if path.read_text(encoding="ascii") == text:
                path.unlink()
        except (FileNotFoundError, OSError):
            pass

    def _spawn_node(self, command: list[str]) -> subprocess.Popen[Any]:
        kwargs: dict[str, Any] = {
            "stdin": subprocess.DEVNULL,
            "stdout": subprocess.DEVNULL,
            "stderr": subprocess.DEVNULL,
            "shell": False,
        }
        if os.name == "nt":
            kwargs["creationflags"] = int(getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)) | int(getattr(subprocess, "DETACHED_PROCESS", 0))
        else:
            kwargs["start_new_session"] = True
        return subprocess.Popen(command, **kwargs)

    def ensure_node(self, *, wait_seconds: float = 5.0) -> dict[str, Any]:
        with self._lock:
            current = self._node_status()
            if current["status"] == "RUNNING":
                return current
            self._clear_stale_lock()
            command = [sys.executable, "-m", "axiom.cli", "node-run", "--db", self.db_path, "--cycles", "0"]
            try:
                process = self._node_launcher(command)
            except (OSError, RuntimeError) as exc:
                raise OperatorControlError("NODE_START_FAILED") from exc
            deadline = time.monotonic() + max(0.1, float(wait_seconds))
            while time.monotonic() < deadline:
                current = self._node_status()
                if current["status"] == "RUNNING":
                    return current
                if hasattr(process, "poll") and process.poll() is not None:
                    break
                time.sleep(0.1)
            raise OperatorControlError("NODE_START_TIMEOUT")

    def restart_node(self) -> dict[str, Any]:
        with self._lock:
            current = self._node_status()
            if current["status"] == "RUNNING":
                pid = int(current.get("pid") or 0)
                path = self._node_lock_path()
                try:
                    marker = path.read_text(encoding="ascii")
                except (FileNotFoundError, OSError) as exc:
                    raise OperatorControlError("NODE_STOP_UNSAFE") from exc
                if not marker or int(marker.splitlines()[0].strip()) != pid:
                    raise OperatorControlError("NODE_STOP_UNSAFE")
                stop_path = Path(str(self.db_path) + ".stop")
                try:
                    stop_path.write_text(marker, encoding="ascii")
                except OSError as exc:
                    raise OperatorControlError("NODE_STOP_REQUEST_FAILED") from exc
                deadline = time.monotonic() + 15.0
                while time.monotonic() < deadline:
                    if self._node_status()["status"] != "RUNNING":
                        break
                    time.sleep(0.1)
                if self._node_status()["status"] == "RUNNING":
                    raise OperatorControlError("NODE_STOP_TIMEOUT")
            return self.ensure_node()

    def _bootstrap_status(self) -> dict[str, Any]:
        job = self.store.get_operator_job(BOOTSTRAP_JOB_NAME)
        result = _operator_job_payload(job)
        try:
            snapshot = load_crypto_universe(self.store)
            selected = list(snapshot.selected_symbols) if snapshot is not None else []
        except (AttributeError, TypeError, ValueError):
            selected = []
        states = self.store.list_dataset_bootstrap_states(limit=10_000)
        running = [item for item in states if str(item.get("status", "")).upper() == "RUNNING"]
        complete = [item for item in states if str(item.get("status", "")).upper() in {"COMPLETE", "EMPTY"}]
        result.update(
            {
                "current_symbol": (running[0].get("instrument") if running else result.get("current_symbol")),
                "current_timeframe": (running[0].get("timeframe") if running else result.get("current_timeframe")),
                "completed_datasets": len(complete),
                "total_datasets": len(selected) * 4,
                "total_symbols": len(selected),
            }
        )
        if result["status"] == "RUNNING" and not running and complete and result["completed_datasets"] >= result["total_datasets"]:
            result["status"] = "COMPLETE"
        elif result["status"] == "NOT_STARTED" and 0 < result["completed_datasets"] < result["total_datasets"]:
            result["status"] = "PARTIAL"
            result["resumable"] = True
        return result

    def _bootstrap_worker(self, *, resume: bool, started_at: datetime, total_symbols: int) -> None:
        status = "COMPLETE"
        error: str | None = None
        try:
            snapshot = load_crypto_universe(self.store)
            if snapshot is None:
                raise OperatorControlError("CRYPTO_UNIVERSE_NOT_INITIALIZED")
            bootstrapper = HistoricalBootstrapper(self.store, crypto_provider=BinanceAdapter(timeout=10.0))
            reports = bootstrapper.bootstrap_crypto_universe(
                snapshot,
                start=BTC_HISTORY_START,
                resume=bool(resume),
                max_symbols=50,
            )
            failed = [item for item in reports if str(getattr(item, "status", "")).upper() in {"FAILED", "ERROR"}]
            if failed:
                status = "FAILED"
                error = "BOOTSTRAP_DATASET_FAILED"
        except Exception as exc:
            status = "FAILED"
            error = exc.code if isinstance(exc, OperatorControlError) else type(exc).__name__.upper()
        finally:
            progress = self._bootstrap_status()
            progress.update({"status": status, "total_symbols": total_symbols, "last_error": error, "resumable": status != "COMPLETE"})
            self.store.set_operator_job(
                BOOTSTRAP_JOB_NAME,
                status,
                {key: progress.get(key) for key in ("current_symbol", "current_timeframe", "completed_symbols", "total_symbols", "completed_datasets", "total_datasets")},
                pid=os.getpid(),
                started_at=started_at,
                last_error=error,
                resumable=status != "COMPLETE",
            )
            with self._lock:
                self._bootstrap_threads.pop(BOOTSTRAP_JOB_NAME, None)

    def start_bootstrap(self, *, resume: bool) -> dict[str, Any]:
        with self._lock:
            current = self._bootstrap_status()
            thread = self._bootstrap_threads.get(BOOTSTRAP_JOB_NAME)
            if current["status"] == "RUNNING" and thread is not None and thread.is_alive():
                raise OperatorControlError("BOOTSTRAP_ALREADY_RUNNING")
            if resume and not current["resumable"] and current["status"] not in {"FAILED", "STALE"}:
                raise OperatorControlError("BOOTSTRAP_NOT_RESUMABLE")
            try:
                snapshot = load_crypto_universe(self.store)
            except (AttributeError, TypeError, ValueError):
                snapshot = None
            if snapshot is None:
                raise OperatorControlError("CRYPTO_UNIVERSE_NOT_INITIALIZED")
            selected = list(snapshot.selected_symbols)
            now = utc_now()
            self.store.set_operator_job(
                BOOTSTRAP_JOB_NAME,
                "RUNNING",
                {
                    "current_symbol": current.get("current_symbol"),
                    "current_timeframe": current.get("current_timeframe"),
                    "completed_symbols": current.get("completed_symbols", 0),
                    "total_symbols": len(selected),
                    "completed_datasets": current.get("completed_datasets", 0),
                    "total_datasets": len(selected) * 4,
                    "resume": bool(resume),
                },
                pid=os.getpid(),
                started_at=now,
                resumable=True,
            )
            thread = threading.Thread(
                target=self._bootstrap_worker,
                kwargs={"resume": bool(resume), "started_at": now, "total_symbols": len(selected)},
                name="axiom-crypto-bootstrap",
                daemon=True,
            )
            self._bootstrap_threads[BOOTSTRAP_JOB_NAME] = thread
            thread.start()
            return self._bootstrap_status()

    def _hermes(self) -> HermesOperatorAdapter:
        configured = self.store.get_operator_config("hermes_research_job_id", self.hermes_job_id)
        selected = configured if isinstance(configured, str) and configured else self.hermes_job_id
        return HermesOperatorAdapter(self.store, selected)

    def status(self) -> dict[str, Any]:
        hermes = self._hermes()
        workers = self.store.list_worker_states(limit=2048)
        worker_map = {str(item.get("worker_name")): item for item in workers if isinstance(item, Mapping)}
        def worker(name: str) -> dict[str, Any]:
            row = worker_map.get(name, {})
            payload = row.get("payload") if isinstance(row.get("payload"), Mapping) else {}
            return {
                "status": str(row.get("status") or "NOT_STARTED").upper(),
                "pid": payload.get("pid"),
                "heartbeat_at": row.get("heartbeat_at"),
                "started_at": row.get("started_at"),
            }
        try:
            projected_credentials = CredentialStore().safe_projection(
                allow_environment=False
            )
            if isinstance(projected_credentials, Mapping):
                configured = bool(projected_credentials.get("configured"))
            else:
                configured = False
        except BaseException:
            configured = False
        credentials = {
            "configured": configured,
            "status": "CONFIGURED" if configured else "NOT CONFIGURED",
            "secret_values_exposed": False,
        }
        canary = CanaryService(self.store, initialize=False)
        try:
            canary_status = _safe_value(canary.status())
            latest_signal = _safe_value(canary.latest_signal())
        except Exception:
            canary_status, latest_signal = {"micro_live_canary": "DISABLED"}, None
        worker_status = worker("autonomous-canary")
        autonomous_state = canary_status.get("autonomous") if isinstance(canary_status, Mapping) else {}
        return {
            "node": self._node_status(),
            "bootstrap": self._bootstrap_status(),
            "hermes": hermes.state(),
            "collector": worker("polymarket-collector"),
            "paper": {**worker("paper-engine"), "read_only": True, "live_execution": False},
            "research": worker("research-engine"),
            "autonomous_canary_worker": worker_status,
            "credentials": credentials,
            "canary": {
                "status": canary_status,
                "latest_signal": latest_signal,
                "autonomous": autonomous_state,
                "submit": "AUTONOMOUS_WORKER" if autonomous_state.get("enabled") else "DISABLED_UNTIL_OPERATOR_ENABLE",
            },
            "live_execution": False,
            "paper_only": True,
        }

    def _audit(self, action: str, target: str, *, success: bool, reason: str = "", result: Mapping[str, Any] | None = None) -> None:
        try:
            self.store.record_operator_action(
                action,
                target,
                success=success,
                reason=reason,
                result=_safe_value(result or {}),
            )
        except Exception:
            pass

    def execute(self, action: str, target: str = "", *, confirm: str = "") -> dict[str, Any]:
        action_value = str(action or "").strip()
        target_value = str(target or "").strip()
        try:
            if action_value not in _ALLOWED_ACTIONS:
                raise OperatorControlError("ACTION_NOT_ALLOWED")
            expected = _CONFIRMATIONS.get(action_value)
            if expected is not None and confirm != expected:
                raise OperatorControlError("EXACT_CONFIRMATION_REQUIRED")
            if action_value in {"canary.eligibility.verify", "canary.eligibility.mark", "canary.generate_signal", "canary.arm"}:
                target_value = _safe_identifier(target_value, "candidate ID")
            elif action_value == "canary.enable_auto" and target_value:
                raise OperatorControlError("AUTONOMOUS_CANARY_ACCEPTS_NO_TARGET")
            if action_value == "node.restart":
                result = {"node": self.restart_node()}
            elif action_value == "bootstrap.start":
                result = {"bootstrap": self.start_bootstrap(resume=False)}
            elif action_value == "bootstrap.resume":
                result = {"bootstrap": self.start_bootstrap(resume=True)}
            elif action_value == "hermes.pause":
                result = {"hermes": self._hermes().set_status("PAUSED")}
            elif action_value == "hermes.resume":
                result = {"hermes": self._hermes().set_status("ACTIVE")}
            elif action_value == "hermes.run_now":
                result = {"hermes": self._hermes().run_now()}
            elif action_value == "canary.connectivity_check":
                credentials = CredentialStore()
                configured = credentials.configured(allow_environment=False)
                venue = PolymarketClobV2Venue(allow_environment=False) if configured else None
                service = CanaryService(self.store, credentials=credentials, initialize=False)
                result = {
                    "connectivity": service.connectivity_check(
                        venue=venue,
                        credentials_configured=configured,
                        allow_environment=False,
                    )
                }
            elif action_value == "canary.eligibility.verify":
                service = CanaryService(self.store, initialize=False)
                result = {"eligibility": service.validate_eligibility(target_value)}
            elif action_value == "canary.eligibility.mark":
                service = CanaryService(self.store, initialize=True)
                validation = service.validate_eligibility(target_value)
                if not validation.get("eligible"):
                    raise OperatorControlError(str(validation.get("reason_code") or "CANDIDATE_RESEARCH_GATES_INCOMPLETE"))
                service.mark_eligible(target_value)
                result = {"eligibility": validation, "marked": True}
            elif action_value == "canary.generate_signal":
                service = CanaryService(self.store, initialize=True)
                result = {"signal": service.generate_signal(target_value)}
            elif action_value == "canary.arm":
                credentials = CredentialStore()
                if not credentials.configured(allow_environment=False):
                    raise OperatorControlError("CREDENTIALS_NOT_CONFIGURED")
                service = CanaryService(self.store, credentials=credentials, initialize=True)
                venue = PolymarketClobV2Venue(allow_environment=False)
                result = {
                    "canary": service.arm(
                        target_value,
                        venue=venue,
                    )
                }
            elif action_value == "canary.enable_auto":
                service = CanaryService(self.store, initialize=True)
                result = {
                    "canary": service.enable_autonomous_micro_live(),
                    "confirmation": "ENABLE AUTO CANARY",
                }
            elif action_value == "canary.disarm":
                service = CanaryService(self.store, initialize=True)
                service.disarm()
                result = {"canary": service.status()}
            elif action_value == "canary.kill":
                service = CanaryService(self.store, initialize=True)
                service.kill()
                result = {"canary": service.status()}
            else:
                raise OperatorControlError("ACTION_NOT_ALLOWED")
            public = _safe_value(result)
            self._audit(action_value, target_value, success=True, result=public)
            return {"ok": True, "action": action_value, "target": target_value, "result": public, "paper_only": True, "live_execution": False}
        except OperatorControlError as exc:
            reason = exc.code
        except CanaryBlocked as exc:
            reason = str(exc)[:160] or "CANARY_BLOCKED"
        except (OSError, RuntimeError, TypeError, ValueError) as exc:
            reason = type(exc).__name__.upper()
        except Exception as exc:
            reason = type(exc).__name__.upper()
        self._audit(action_value or "invalid", target_value, success=False, reason=reason, result={"ok": False, "reason": reason})
        return {"ok": False, "action": action_value, "target": target_value, "reason": reason, "paper_only": True, "live_execution": False}


__all__ = [
    "BOOTSTRAP_JOB_NAME",
    "DEFAULT_HERMES_JOB_ID",
    "HermesOperatorAdapter",
    "OperatorControlError",
    "OperatorControlPlane",
]
