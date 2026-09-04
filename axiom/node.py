"""Always-on, public-data-only Windows research node.

The node owns a durable SQLite state store, a single-process lock, bounded worker
cycles, and paper-only processing. It has no credentials, broker, or order route.
"""
from __future__ import annotations

from dataclasses import dataclass
from itertools import islice
import logging
from logging.handlers import RotatingFileHandler
import math
import os
from pathlib import Path
import re
import subprocess
import threading
import time
from typing import Any, Callable, Mapping

from .collector import CollectionCycle, CollectorConfig, PolymarketCollector
from .data import PolymarketAdapter
from .domain import OrderBookSnapshot, ensure_utc, parse_timestamp, to_record, utc_now
from .forward import ForwardTestRegistry, _content_hash
from .opportunity import scan_opportunities
from .paper import CryptoPaperTrader
from .paper_engine import run_forward_paper
from .research_bus import DurableResearchBus
from .storage import AxiomStore
from .strategy import evaluate_signal_record, load_strategy


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
        return Path(f"/proc/{pid}/cmdline").read_bytes().replace(b"\x00", b" ").decode("utf-8", "replace").strip()
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
def _consume_provider_errors(provider: Any, context: str) -> tuple[list[str], bool]:
    consumer = getattr(provider, "consume_transport_errors", None)
    if not callable(consumer):
        return [], False
    try:
        errors = tuple(consumer())
    except Exception as exc:
        return [f"{context}: transport error collector failed: {exc}"], False
    details: list[str] = []
    retryable = False
    for error in errors:
        status = getattr(error, "status", None)
        details.append(f"{context}: HTTP {status}" if status is not None else f"{context}: {error}")
        retryable = retryable or bool(getattr(error, "retryable", False))
    return details, retryable



@dataclass(frozen=True, slots=True)
class NodeConfig:
    db_path: str
    lock_path: str | None = None
    log_path: str | None = None
    interval_seconds: float = 60.0
    depth: int = 20
    max_markets: int = 100
    max_attempts: int = 3
    failure_cooldown_seconds: float = 30.0
    retain_cycles: int = 5
    worker_name: str = "axiom-node"
    max_log_bytes: int = 5_000_000
    backup_count: int = 3
    crypto_symbol: str = "BTC/USDT"
    crypto_enabled: bool = True

    def __post_init__(self) -> None:
        db_text = str(self.db_path).strip()
        if db_text not in {":memory:", ""} and not db_text.startswith("file:"):
            object.__setattr__(self, "db_path", os.path.abspath(os.path.expanduser(db_text)))
        if not str(self.db_path).strip():
            raise ValueError("db_path is required")
        for field_name in ("lock_path", "log_path"):
            configured = getattr(self, field_name)
            if configured is None:
                continue
            text = str(configured).strip()
            object.__setattr__(
                self,
                field_name,
                os.path.abspath(os.path.expanduser(text)) if text else None,
            )
        def path_identity(value: Any) -> str:
            text = str(value)
            if text.startswith("file:") or text == ":memory:":
                return text.casefold()
            return os.path.normcase(os.path.abspath(os.path.expanduser(text)))

        db_identity = path_identity(self.db_path)
        lock_identity = path_identity(self.lock_path or f"{self.db_path}.lock")
        log_identity = path_identity(self.log_path or f"{self.db_path}.log")
        if len({db_identity, lock_identity, log_identity}) != 3:
            raise ValueError("db_path, lock_path, and log_path must be distinct")
        if not str(self.worker_name).strip():
            raise ValueError("worker_name is required")
        if not str(self.crypto_symbol).strip():
            raise ValueError("crypto_symbol is required")
        if not isinstance(self.crypto_enabled, bool):
            raise ValueError("crypto_enabled must be boolean")
        interval = float(self.interval_seconds)
        cooldown = float(self.failure_cooldown_seconds)
        if not math.isfinite(interval) or interval <= 0:
            raise ValueError("interval_seconds must be finite and positive")
        if isinstance(self.depth, bool) or not isinstance(self.depth, int) or self.depth <= 0:
            raise ValueError("depth must be a positive integer")
        if isinstance(self.max_markets, bool) or not isinstance(self.max_markets, int) or self.max_markets <= 0:
            raise ValueError("max_markets must be a positive integer")
        if isinstance(self.max_attempts, bool) or not isinstance(self.max_attempts, int) or self.max_attempts <= 0:
            raise ValueError("max_attempts must be a positive integer")
        if not math.isfinite(cooldown) or cooldown < 0:
            raise ValueError("failure_cooldown_seconds must be finite and non-negative")
        if isinstance(self.retain_cycles, bool) or not isinstance(self.retain_cycles, int) or self.retain_cycles <= 0:
            raise ValueError("retain_cycles must be a positive integer")
        if isinstance(self.max_log_bytes, bool) or not isinstance(self.max_log_bytes, int) or self.max_log_bytes <= 0:
            raise ValueError("max_log_bytes must be a positive integer")
        if isinstance(self.backup_count, bool) or not isinstance(self.backup_count, int) or self.backup_count < 0:
            raise ValueError("backup_count must be a non-negative integer")


class _NoopStrategy:
    """Explicit no-op strategy used when only stored strategy hashes are available."""

    def signal(self, observation: Any, context: Mapping[str, Any] | None = None) -> None:
        return None


class _PersistedStrategy:
    def __init__(self, definition: Any) -> None:
        self.definition = definition
        self.strategy_id = definition.id
        self._history: dict[str, list[Any]] = {}

    def signal(self, context: Mapping[str, Any]) -> Mapping[str, Any] | None:
        symbol = str(context.get("symbol", ""))
        persisted_history = context.get("history")
        if isinstance(persisted_history, (list, tuple)):
            history = list(persisted_history)
            history.append(context.get("market", context.get("observation")))
            history = history[-512:]
            self._history[symbol] = history
        else:
            history = self._history.setdefault(symbol, [])
            history.append(context.get("market", context.get("observation")))
            if len(history) > 512:
                del history[:-512]
        signal = evaluate_signal_record(self.definition, {"observations": tuple(history)})
        if not signal.actionable:
            return None
        score = float(signal.score)
        outcome = "yes" if score > 0 else "no"
        return {"side": f"buy_{outcome}", "quantity": abs(score), "outcome": outcome}


class _PersistedProbabilityModel:
    def __init__(self, document: Mapping[str, Any]) -> None:
        self.document = dict(document)

    def predict_probability(self, observation: Mapping[str, Any]) -> float | None:
        try:
            if "probability" in self.document:
                return float(self.document["probability"])
            if "yes_probability" in self.document:
                return float(self.document["yes_probability"])
            field = self.document.get("field")
            if isinstance(field, str) and field.strip():
                value = observation.get(field)
                return float(value) if value is not None else None
        except (TypeError, ValueError):
            return None
        return None


class ResearchNode:
    """Run collection, paper workers, and durable research recovery."""

    def __init__(
        self,
        config: NodeConfig,
        *,
        provider: Any | None = None,
        crypto_provider: Any | None = None,
        opportunity_model: Any | None = None,
        store: AxiomStore | None = None,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self.config = config
        self.store = store if store is not None else AxiomStore(config.db_path)
        self._owns_store = store is None
        self.provider = provider if provider is not None else PolymarketAdapter()
        self.opportunity_model = opportunity_model
        self.sleep = sleep
        self.clock = clock
        self.stop_event = threading.Event()
        self._heartbeat_stop = threading.Event()
        self._heartbeat_thread: threading.Thread | None = None
        self.started_at: datetime | None = None
        self._lock_fd: int | None = None
        self.crypto_provider = crypto_provider if config.crypto_enabled else None
        normalized_symbol = str(config.crypto_symbol).replace("/", "").replace("-", "").upper()
        self._crypto_experiment_id = f"crypto-paper-{normalized_symbol}"
        self._crypto_trader = (
            CryptoPaperTrader(self.crypto_provider, _NoopStrategy())
            if self.crypto_provider is not None
            else None
        )
        try:
            crypto_history = self.store.paper_history_counts(self._crypto_experiment_id)
        except Exception:
            crypto_history = {"observations": 0, "fills": 0}
        self._crypto_status: dict[str, Any] = {
            "enabled": self._crypto_trader is not None,
            "symbol": config.crypto_symbol,
            "observations": int(crypto_history.get("observations", 0)),
            "fills": int(crypto_history.get("fills", 0)),
        }
        self._logger: logging.Logger | None = None
        self._handler: RotatingFileHandler | None = None
        self._cycles: list[CollectionCycle] = []
        self._restart_count = 0
        self._last_status: dict[str, Any] | None = None
        self.collector = PolymarketCollector(
            self.provider,
            self.store,
            CollectorConfig(
                interval_seconds=config.interval_seconds,
                depth=config.depth,
                max_markets=config.max_markets,
                max_attempts=config.max_attempts,
                failure_cooldown_seconds=config.failure_cooldown_seconds,
                retain_cycles=config.retain_cycles,
            ),
            clock=clock,
            sleep=sleep,
        )
        self.bus = DurableResearchBus(self.store)

    @property
    def lock_path(self) -> Path:
        return Path(self.config.lock_path or (str(self.config.db_path) + ".lock"))

    @property
    def log_path(self) -> Path:
        return Path(self.config.log_path or (str(self.config.db_path) + ".log"))
    @property
    def stop_path(self) -> Path:
        return Path(str(self.config.db_path) + ".stop")

    def _external_stop_requested(self) -> bool:
        if self.stop_event.is_set():
            return True
        try:
            marker = self.stop_path.read_text(encoding="utf-8").strip()
        except FileNotFoundError:
            return False
        except OSError:
            marker = "stop"
        if not marker:
            return False
        marker_lines = marker.splitlines()
        try:
            marker_pid = int(marker_lines[0].strip())
        except (IndexError, ValueError):
            marker_pid = 0
        if marker_pid > 0:
            if marker_pid != os.getpid():
                if not _pid_matches_node(marker_pid, str(self.config.db_path)):
                    try:
                        self.stop_path.unlink()
                    except OSError:
                        pass
                return False
            try:
                owner_marker = self.lock_path.read_text(encoding="ascii").strip()
            except (FileNotFoundError, OSError):
                owner_marker = ""
            if len(marker_lines) < 2 or owner_marker != marker:
                try:
                    self.stop_path.unlink()
                except OSError:
                    pass
                return False
        self.stop_event.set()
        return True

    def run(self, *, max_cycles: int | None = None) -> list[CollectionCycle]:
        if max_cycles is not None and (isinstance(max_cycles, bool) or max_cycles < 0):
            raise ValueError("max_cycles must be non-negative or None")
        self._acquire_lock()
        try:
            self._configure_logging()
        except BaseException:
            self._release_lock()
            if self._owns_store:
                self.store.close()
            raise
        self.started_at = ensure_utc(self.clock())
        self.stop_event.clear()
        completed = 0
        cycle_failure = False
        attempted = 0
        try:
            self.store.save_worker_state(
                self.config.worker_name,
                "running",
                {
                    "pid": os.getpid(),
                    "paper_only": True,
                    "live_execution": False,
                    "crypto_paper": dict(self._crypto_status),
                },
                started_at=self.started_at,
                heartbeat_at=self.started_at,
            )
            self._start_heartbeat_watchdog()
            while max_cycles is None or attempted < max_cycles:
                if self._external_stop_requested():
                    break
                attempted += 1
                self._heartbeat(
                    "running",
                    {
                        "cycle": completed,
                        "attempt": attempted,
                        "restart_count": self._restart_count,
                        "crypto_paper": dict(self._crypto_status),
                    },
                )
                try:
                    self._run_crypto_paper()
                    cycle = self.collector.collect_once(now=ensure_utc(self.clock()))
                    self._run_opportunity_pipeline()
                    self._cycles.append(cycle)
                    if len(self._cycles) > self.config.retain_cycles:
                        del self._cycles[:-self.config.retain_cycles]
                    self._run_paper_workers()
                    self._run_research_queue()
                    self.bus.resume_expired(now=ensure_utc(self.clock()))
                    self._run_health_monitor()
                    completed += 1
                    cycle_failure = False
                    self._heartbeat(
                        "running",
                        {
                            "cycle": completed,
                            "last_collection": cycle.as_record(),
                            "restart_count": self._restart_count,
                            "paper_only": True,
                            "live_execution": False,
                            "crypto_paper": dict(self._crypto_status),
                        },
                    )
                except Exception as exc:  # worker restart boundary
                    cycle_failure = True
                    self._restart_count += 1
                    self._log(logging.ERROR, "worker cycle failed; restarting: %s", exc)
                    self._heartbeat(
                        "degraded",
                        {
                            "error": str(exc),
                            "restart_count": self._restart_count,
                            "crypto_paper": dict(self._crypto_status),
                        },
                    )
                if max_cycles is not None and attempted >= max_cycles:
                    self._external_stop_requested()
                    break
                deadline = time.monotonic() + float(self.config.interval_seconds)
                while not self.stop_event.is_set():
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    if self.stop_event.wait(min(remaining, 0.5)):
                        break
                    if self._external_stop_requested():
                        break
        except KeyboardInterrupt:
            self.stop_event.set()
            raise
        finally:
            self._stop_heartbeat_watchdog()
            status = "stopped" if self.stop_event.is_set() else ("degraded" if cycle_failure else "idle")
            try:
                self._heartbeat(
                    status,
                    {
                        "cycles": completed,
                        "attempts": attempted,
                        "restart_count": self._restart_count,
                        "crypto_paper": dict(self._crypto_status),
                    },
                )
            except Exception:
                pass
            try:
                self.stop_path.unlink()
            except FileNotFoundError:
                pass
            except OSError:
                pass
            self._release_lock()
            try:
                self._last_status = self._status_payload()
            except Exception:
                self._last_status = {
                    "worker_name": self.config.worker_name,
                    "status": status,
                    "lock_path": str(self.lock_path),
                    "lock_exists": self.lock_path.exists(),
                    "log_path": str(self.log_path),
                    "restart_count": self._restart_count,
                    "paper_only": True,
                    "live_execution": False,
                    "crypto_paper": dict(self._crypto_status),
                }
            self._close_logging()
            if self._owns_store:
                self.store.close()
        return list(self._cycles)

    def _start_heartbeat_watchdog(self) -> None:
        self._heartbeat_stop.clear()
        interval = max(0.5, min(10.0, max(float(self.config.interval_seconds), 0.5)))
        watchdog_name = f"{self.config.worker_name}:watchdog"
        try:
            self.store.save_worker_state(
                watchdog_name,
                "running",
                {
                    "pid": os.getpid(),
                    "parent_worker": self.config.worker_name,
                    "lock_path": str(self.lock_path),
                    "paper_only": True,
                    "live_execution": False,
                },
                started_at=self.started_at,
                heartbeat_at=ensure_utc(self.clock()),
            )
        except Exception as exc:
            self._log(logging.WARNING, "initial heartbeat watchdog update failed: %s", exc)

        def beat() -> None:
            while not self._heartbeat_stop.wait(interval):
                try:
                    self.store.save_worker_state(
                        watchdog_name,
                        "running",
                        {
                            "pid": os.getpid(),
                            "parent_worker": self.config.worker_name,
                            "lock_path": str(self.lock_path),
                            "paper_only": True,
                            "live_execution": False,
                        },
                        started_at=self.started_at,
                        heartbeat_at=ensure_utc(self.clock()),
                    )
                except Exception as exc:
                    self._log(logging.WARNING, "heartbeat watchdog update failed: %s", exc)

        self._heartbeat_thread = threading.Thread(
            target=beat,
            name=f"{self.config.worker_name}-heartbeat",
            daemon=True,
        )
        self._heartbeat_thread.start()

    def _stop_heartbeat_watchdog(self) -> None:
        self._heartbeat_stop.set()
        thread = self._heartbeat_thread
        self._heartbeat_thread = None
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=2.0)
        try:
            self.store.save_worker_state(
                f"{self.config.worker_name}:watchdog",
                "stopped",
                {
                    "pid": os.getpid(),
                    "parent_worker": self.config.worker_name,
                    "lock_path": str(self.lock_path),
                    "paper_only": True,
                    "live_execution": False,
                },
                started_at=self.started_at,
                heartbeat_at=ensure_utc(self.clock()),
            )
        except Exception:
            pass

    run_forever = run

    def stop(self) -> None:
        self.stop_event.set()

    def status(self) -> dict[str, Any]:
        try:
            return self._status_payload()
        except Exception:
            if self._last_status is not None:
                return dict(self._last_status)
            return {
                "worker_name": self.config.worker_name,
                "status": "closed",
                "lock_path": str(self.lock_path),
                "lock_exists": self.lock_path.exists(),
                "log_path": str(self.log_path),
                "restart_count": self._restart_count,
                "paper_only": True,
                "live_execution": False,
                "crypto_paper": dict(self._crypto_status),
            }

    def _status_payload(self) -> dict[str, Any]:
        rows = {row["worker_name"]: row for row in self.store.list_worker_states(limit=2048)}
        state = rows.get(self.config.worker_name)
        worker_payload = state.get("payload") if state else None
        status_lock_path = self.lock_path
        status_log_path = self.log_path
        if self.config.lock_path is None and isinstance(worker_payload, Mapping):
            persisted_lock_path = str(worker_payload.get("lock_path", "")).strip()
            if persisted_lock_path:
                status_lock_path = Path(persisted_lock_path)
        if self.config.log_path is None and isinstance(worker_payload, Mapping):
            persisted_log_path = str(worker_payload.get("log_path", "")).strip()
            if persisted_log_path:
                status_log_path = Path(persisted_log_path)
        lock_exists = status_lock_path.exists()
        lock_owner_pid: int | None = None
        if lock_exists:
            try:
                lock_owner_pid = int(status_lock_path.read_text(encoding="ascii").splitlines()[0].strip())
            except (OSError, ValueError):
                lock_owner_pid = None
        persisted_pid = worker_payload.get("pid") if isinstance(worker_payload, Mapping) else None
        try:
            worker_pid = int(persisted_pid)
        except (TypeError, ValueError):
            worker_pid = lock_owner_pid or 0
        worker_alive = _pid_alive(worker_pid)
        worker_identity_valid = (
            self._lock_fd is not None
            or _pid_matches_node(worker_pid, str(self.config.db_path))
        ) if worker_alive else False
        heartbeat = state.get("heartbeat_at") if state else None
        heartbeat_value = parse_timestamp(heartbeat)
        try:
            heartbeat_age = (
                max(0.0, (ensure_utc(self.clock()) - heartbeat_value).total_seconds())
                if heartbeat_value is not None
                else None
            )
        except Exception:
            heartbeat_age = None
        status = str(state.get("status", "not_started")).lower() if state else "not_started"
        child_workers: dict[str, Any] = {}
        child_degraded = False
        child_running = False
        for worker_name, worker_state in rows.items():
            if worker_name == self.config.worker_name:
                continue
            child_status = str(worker_state.get("status", "not_started")).lower()
            child_workers[worker_name] = {
                "status": child_status,
                "heartbeat_at": worker_state.get("heartbeat_at"),
                "payload": worker_state.get("payload"),
            }
            child_degraded = child_degraded or child_status in {"degraded", "stale"}
            child_running = child_running or child_status == "running"
        persisted_crypto = worker_payload.get("crypto_paper") if isinstance(worker_payload, Mapping) else None
        crypto_error = bool(
            (self._crypto_status.get("enabled") and self._crypto_status.get("last_error"))
            or (
                isinstance(persisted_crypto, Mapping)
                and persisted_crypto.get("enabled")
                and persisted_crypto.get("last_error")
            )
        )
        persisted_stale_after = worker_payload.get("stale_after_seconds") if isinstance(worker_payload, Mapping) else None
        try:
            stale_after = float(persisted_stale_after)
        except (TypeError, ValueError):
            stale_after = max(float(self.config.interval_seconds) * 3.0, float(self.config.failure_cooldown_seconds))
        if not math.isfinite(stale_after) or stale_after < 0:
            stale_after = max(float(self.config.interval_seconds) * 3.0, float(self.config.failure_cooldown_seconds))
        pid_marker_exists = Path(str(self.config.db_path) + ".node.pid").exists()
        health_degraded = child_degraded or crypto_error
        watchdog_state = rows.get(f"{self.config.worker_name}:watchdog")
        watchdog_payload = watchdog_state.get("payload") if watchdog_state else None
        watchdog_pid_value = watchdog_payload.get("pid") if isinstance(watchdog_payload, Mapping) else None
        try:
            watchdog_pid = int(watchdog_pid_value)
        except (TypeError, ValueError):
            watchdog_pid = 0
        watchdog_heartbeat = parse_timestamp(watchdog_state.get("heartbeat_at")) if watchdog_state else None
        try:
            watchdog_age = (
                max(0.0, (ensure_utc(self.clock()) - watchdog_heartbeat).total_seconds())
                if watchdog_heartbeat is not None
                else None
            )
        except Exception:
            watchdog_age = None
        watchdog_fresh = bool(
            watchdog_state
            and str(watchdog_state.get("status", "")).lower() == "running"
            and watchdog_pid == worker_pid
            and _pid_alive(watchdog_pid)
            and _pid_matches_node(watchdog_pid, str(self.config.db_path))
            and lock_owner_pid == watchdog_pid
            and watchdog_age is not None
            and watchdog_age <= stale_after
        )
        if status not in {"stopped", "closed", "stale", "degraded"} and child_running:
            status = "running"
        liveness_candidate = status == "running" or (status == "degraded" and (lock_exists or pid_marker_exists))
        if liveness_candidate and (
            not lock_exists
            or lock_owner_pid != worker_pid
            or not worker_alive
            or not worker_identity_valid
            or heartbeat_age is None
            or heartbeat_age > stale_after
        ):
            if watchdog_fresh and lock_exists and worker_alive and worker_identity_valid:
                status = "degraded"
            else:
                status = "stale" if lock_exists or pid_marker_exists else "stopped"
        elif status not in {"stopped", "closed", "stale"} and health_degraded:
            status = "degraded"
        payload = {
            "worker_name": self.config.worker_name,
            "status": status,
            "pid": os.getpid() if self._lock_fd is not None else (worker_pid or None),
            "lock_path": str(status_lock_path),
            "lock_exists": lock_exists,
            "lock_owner_pid": lock_owner_pid,
            "worker_alive": worker_alive,
            "worker_identity_valid": worker_identity_valid,
            "heartbeat_age_seconds": heartbeat_age,
            "log_path": str(status_log_path),
            "restart_count": self._restart_count,
            "paper_only": True,
            "live_execution": False,
            "crypto_paper": dict(self._crypto_status),
            "workers": child_workers,
        }
        if state:
            payload.update({"heartbeat_at": state.get("heartbeat_at"), "worker": worker_payload})
            if isinstance(worker_payload, Mapping) and isinstance(worker_payload.get("crypto_paper"), Mapping):
                payload["crypto_paper"] = dict(worker_payload["crypto_paper"])
        return payload

    def _run_crypto_paper(self) -> None:
        if self._crypto_trader is None:
            self._crypto_status = {
                **self._crypto_status,
                "enabled": False,
                "last_run_at": ensure_utc(self.clock()).isoformat(),
            }
            return
        started = ensure_utc(self.clock())
        try:
            ticker = None
            ticker_retrieved_at: datetime | None = None
            last_error = None
            for attempt in range(self.config.max_attempts):
                try:
                    ticker = self.crypto_provider.ticker(self.config.crypto_symbol)
                    if ticker is not None:
                        ticker_retrieved_at = ensure_utc(self.clock())
                except Exception as exc:
                    last_error = str(exc)
                    ticker = None
                transport_errors = getattr(self.crypto_provider, "consume_transport_errors", lambda: ())()
                if ticker is not None:
                    break
                if transport_errors:
                    last_error = "; ".join(
                        f"HTTP {error.status}" if error.status is not None else str(error)
                        for error in transport_errors
                    )
                retryable = any(bool(getattr(error, "retryable", False)) for error in transport_errors)
                if not retryable or attempt + 1 >= self.config.max_attempts:
                    break
                delay = max(0.0, min(float(self.config.failure_cooldown_seconds), 2.0 ** attempt))
                self.sleep(delay)
            if ticker is None:
                self._crypto_status = {
                    **self._crypto_status,
                    "last_run_at": ensure_utc(self.clock()).isoformat(),
                    "last_error": last_error or "ticker unavailable",
                }
                return
            ticker_timestamp = ensure_utc(ticker.timestamp)
            if ticker_retrieved_at is None:
                ticker_retrieved_at = started
            if ticker_timestamp > ticker_retrieved_at:
                self.store.save_collection_error(
                    None,
                    started,
                    "future_crypto_ticker",
                    "ticker timestamp is in the future",
                    {"symbol": self.config.crypto_symbol, "timestamp": ticker_timestamp.isoformat()},
                )
                self._crypto_status = {
                    **self._crypto_status,
                    "last_run_at": started.isoformat(),
                    "last_error": "ticker timestamp is in the future",
                }
                return
            crypto_book: OrderBookSnapshot | None = None
            book_retrieved_at = ensure_utc(self.clock())
            try:
                candidate_book = self.crypto_provider.order_book(
                    self.config.crypto_symbol,
                    depth=self.config.depth,
                )
                book_retrieved_at = ensure_utc(self.clock())
                if isinstance(candidate_book, OrderBookSnapshot):
                    crypto_book = candidate_book
                elif candidate_book is not None:
                    last_error = "invalid crypto order-book response"
            except Exception as exc:
                last_error = str(exc)
            book_errors = getattr(self.crypto_provider, "consume_transport_errors", lambda: ())()
            if book_errors:
                last_error = "; ".join(
                    f"HTTP {error.status}" if error.status is not None else str(error)
                    for error in book_errors
                )
            provider_error = last_error
            execution_timestamp = ticker_timestamp
            if crypto_book is not None:
                book_timestamp = ensure_utc(crypto_book.timestamp)
                if book_timestamp > book_retrieved_at:
                    self.store.save_collection_error(
                        None,
                        started,
                        "future_crypto_order_book",
                        "order-book timestamp is in the future",
                        {"symbol": self.config.crypto_symbol, "timestamp": book_timestamp.isoformat()},
                    )
                    self._crypto_status = {
                        **self._crypto_status,
                        "last_run_at": started.isoformat(),
                        "last_error": "order-book timestamp is in the future",
                    }
                    return
                execution_timestamp = max(execution_timestamp, book_timestamp)
            observation_fingerprint = _content_hash(
                {
                    "ticker": to_record(ticker),
                    "order_book": to_record(crypto_book) if crypto_book is not None else None,
                    "execution_timestamp": execution_timestamp.isoformat(),
                }
            ).split(":", 1)[-1]
            observation_id = f"{self._crypto_experiment_id}-{observation_fingerprint}"
            if self.store.paper_observation_exists(observation_id):
                self._crypto_status = {
                    **self._crypto_status,
                    "last_run_at": ensure_utc(self.clock()).isoformat(),
                    "last_observed_at": ticker_timestamp.isoformat(),
                    "deduplicated": int(self._crypto_status.get("deduplicated", 0)) + 1,
                    "last_error": provider_error,
                }
                return
            observation_payload = {
                "ticker": to_record(ticker),
                "order_book": to_record(crypto_book) if crypto_book is not None else None,
                "execution_timestamp": execution_timestamp.isoformat(),
                "paper_only": True,
                "live_execution": False,
                "provider_error": provider_error,
            }
            before_fill_count = len(self._crypto_trader.fills)
            before_sequence = self._crypto_trader._sequence
            try:
                fill = self._crypto_trader.run_once(
                    self.config.crypto_symbol,
                    timestamp=execution_timestamp,
                    ticker=ticker,
                    book=crypto_book,
                    book_observed_at=book_retrieved_at,
                )
                if fill is None:
                    inserted = self.store.save_paper_observation(
                        observation_id,
                        self._crypto_experiment_id,
                        self.config.crypto_symbol,
                        ticker_timestamp,
                        observation_payload,
                    )
                else:
                    inserted = self.store.save_paper_execution(
                        observation_id,
                        self._crypto_experiment_id,
                        self.config.crypto_symbol,
                        ticker_timestamp,
                        observation_payload,
                        fill,
                        fill_id=f"{self._crypto_experiment_id}-{fill.order_id}",
                    )
            except Exception:
                del self._crypto_trader._fills[before_fill_count:]
                self._crypto_trader._sequence = before_sequence
                raise
            if not inserted:
                del self._crypto_trader._fills[before_fill_count:]
                self._crypto_trader._sequence = before_sequence
                self._crypto_status = {
                    **self._crypto_status,
                    "last_run_at": ensure_utc(self.clock()).isoformat(),
                    "last_observed_at": ticker_timestamp.isoformat(),
                    "deduplicated": int(self._crypto_status.get("deduplicated", 0)) + 1,
                    "last_error": provider_error,
                }
                return
            try:
                crypto_history = self.store.paper_history_counts(self._crypto_experiment_id)
            except Exception:
                crypto_history = {
                    "observations": int(self._crypto_status.get("observations", 0)) + int(inserted),
                    "fills": int(self._crypto_status.get("fills", 0)) + int(fill is not None),
                }
            self._crypto_status = {
                **self._crypto_status,
                "last_run_at": ensure_utc(self.clock()).isoformat(),
                "last_observed_at": ensure_utc(ticker.timestamp).isoformat(),
                "observations": int(crypto_history.get("observations", 0)),
                "fills": int(crypto_history.get("fills", 0)),
                "last_error": provider_error,
            }
        except Exception as exc:
            self._crypto_status = {
                **self._crypto_status,
                "last_run_at": ensure_utc(self.clock()).isoformat(),
                "last_error": str(exc),
            }
            self._log(logging.ERROR, "crypto paper cycle failed: %s", exc)
    def _run_opportunity_pipeline(self) -> None:
        worker_name = "opportunity-pipeline"
        started = ensure_utc(self.clock())
        self.store.save_worker_state(
            worker_name,
            "running",
            {"pid": os.getpid(), "paper_only": True, "live_execution": False},
            started_at=started,
            heartbeat_at=started,
        )
        try:
            provider_errors: list[str] = []
            markets: tuple[Any, ...] = ()
            for attempt in range(self.config.max_attempts):
                call_error: str | None = None
                call_retryable = False
                try:
                    try:
                        markets = tuple(islice(iter(self.provider.markets(active=True, limit=self.config.max_markets)), self.config.max_markets))
                    except TypeError:
                        markets = tuple(islice(self.provider.markets(active=True), self.config.max_markets))
                except Exception as exc:
                    call_error = str(exc)
                    call_retryable = isinstance(exc, (OSError, TimeoutError))
                    markets = ()
                transport_details, transport_retryable = _consume_provider_errors(self.provider, "market discovery")
                provider_errors.extend(transport_details)
                if call_error:
                    provider_errors.append(f"market discovery: {call_error}")
                retryable = call_retryable or transport_retryable
                if not retryable or attempt + 1 >= self.config.max_attempts:
                    break
                self.sleep(max(0.0, min(float(self.config.failure_cooldown_seconds), 2.0 ** attempt)))
            markets_retrieved_at = ensure_utc(self.clock())
            opportunities_observed_at = markets_retrieved_at
            records: list[dict[str, Any]] = []
            probabilities: dict[str, float] = {}
            uncertainties: dict[str, float] = {}
            model_versions: set[str] = set()
            for market in markets:
                market_id = str(getattr(market, "market_id", "")).strip()
                if not market_id:
                    continue
                record = to_record(market)
                if not isinstance(record, Mapping):
                    continue
                record = dict(record)
                market_timestamp = parse_timestamp(record.get("timestamp"))
                if market_timestamp is not None and market_timestamp > markets_retrieved_at:
                    self.store.save_collection_error(
                        market_id,
                        started,
                        "future_observation",
                        "market timestamp is in the future",
                        {"timestamp": market_timestamp.isoformat()},
                    )
                    continue
                books: Any = {}
                for attempt in range(self.config.max_attempts):
                    call_error: str | None = None
                    call_retryable = False
                    try:
                        books = self.provider.order_books(market_id, depth=self.config.depth)
                    except Exception as exc:
                        call_error = str(exc)
                        call_retryable = isinstance(exc, (OSError, TimeoutError))
                        books = {}
                    transport_details, transport_retryable = _consume_provider_errors(
                        self.provider,
                        f"order books {market_id}",
                    )
                    provider_errors.extend(transport_details)
                    if call_error:
                        provider_errors.append(f"order books {market_id}: {call_error}")
                    retryable = call_retryable or transport_retryable
                    if not retryable or attempt + 1 >= self.config.max_attempts:
                        break
                    self.sleep(max(0.0, min(float(self.config.failure_cooldown_seconds), 2.0 ** attempt)))
                books_retrieved_at = ensure_utc(self.clock())
                opportunities_observed_at = max(opportunities_observed_at, books_retrieved_at)
                if isinstance(books, Mapping):
                    for outcome in ("yes", "no"):
                        book = books.get(outcome)
                        if book is None:
                            continue
                        book_record = to_record(book)
                        book_timestamp = (
                            parse_timestamp(book_record.get("timestamp"))
                            if isinstance(book_record, Mapping)
                            else None
                        )
                        if book_timestamp is not None and book_timestamp > books_retrieved_at:
                            self.store.save_collection_error(
                                market_id,
                                started,
                                "future_order_book",
                                "order-book timestamp is in the future",
                                {"outcome": outcome, "timestamp": book_timestamp.isoformat()},
                            )
                            continue
                        record[f"{outcome}_order_book"] = book_record
                if self.opportunity_model is None:
                    estimate = record.get("yes_mid")
                    if estimate is None:
                        estimate = record.get("yes_ask")
                    version = "market-price-baseline-v1"
                    quality = "PRICE_PROXY"
                    uncertainty = 1.0
                    features: Mapping[str, Any] = {}
                elif isinstance(self.opportunity_model, Mapping):
                    estimate = self.opportunity_model.get(market_id)
                    version = "configured-model"
                    quality = "MODEL_ESTIMATE"
                    uncertainty = 0.0
                    features = {}
                else:
                    estimate = self.opportunity_model(market)
                    version = str(getattr(estimate, "model_version", "configured-model"))
                    quality = str(getattr(estimate, "research_quality", "MODEL_ESTIMATE"))
                    uncertainty = float(getattr(estimate, "uncertainty", 0.0) or 0.0)
                    features_value = getattr(estimate, "features", {})
                    features = features_value if isinstance(features_value, Mapping) else {}
                if isinstance(estimate, Mapping):
                    probability_value = estimate.get("probability", estimate.get("yes_probability", estimate.get("prediction")))
                    version = str(estimate.get("model_version", version))
                    quality = str(estimate.get("research_quality", quality))
                    uncertainty = float(estimate.get("uncertainty", uncertainty) or 0.0)
                    features_value = estimate.get("features", {})
                    features = features_value if isinstance(features_value, Mapping) else features
                else:
                    probability_value = getattr(estimate, "probability", estimate)
                try:
                    probability = float(probability_value)
                    uncertainty = float(uncertainty)
                except (TypeError, ValueError):
                    continue
                if not math.isfinite(probability) or not 0.0 <= probability <= 1.0:
                    continue
                if not math.isfinite(uncertainty) or uncertainty < 0:
                    continue
                probabilities[market_id] = probability
                uncertainties[market_id] = uncertainty
                model_versions.add(version)
                record.update(
                    {
                        "model_probability": probability,
                        "uncertainty": uncertainty,
                        "model_version": version,
                        "research_quality": quality,
                        "model_features": dict(features),
                    }
                )
                records.append(record)
            opportunities = scan_opportunities(
                records,
                probabilities,
                model_version=sorted(model_versions)[0] if model_versions else "market-price-baseline-v1",
            )
            inserted = self.store.save_opportunity_snapshots(opportunities_observed_at, (item.as_record() for item in opportunities))
            self.store.save_worker_state(
                worker_name,
                "degraded" if provider_errors else "idle",
                {
                    "markets": len(records),
                    "opportunities": len(opportunities),
                    "inserted": inserted,
                    "model_versions": sorted(model_versions),
                    "provider_errors": provider_errors[-32:],
                    "paper_only": True,
                    "live_execution": False,
                },
                started_at=started,
                heartbeat_at=ensure_utc(self.clock()),
            )
        except Exception as exc:
            self.store.save_worker_state(
                worker_name,
                "degraded",
                {"error": str(exc), "paper_only": True, "live_execution": False},
                started_at=started,
                heartbeat_at=ensure_utc(self.clock()),
            )
            self._log(logging.ERROR, "opportunity pipeline failed: %s", exc)

    def _run_paper_workers(self) -> None:
        registry = ForwardTestRegistry(self.store)
        for spec in registry.list():
            config = spec.config if isinstance(spec.config, Mapping) else {}
            if bool(config.get("historical_replay")):
                continue
            worker_name = f"paper:{spec.experiment_id}"
            started = ensure_utc(self.clock())
            self.store.save_worker_state(
                worker_name,
                "running",
                {"pid": os.getpid(), "experiment_id": spec.experiment_id, "paper_only": True, "live_execution": False},
                started_at=started,
                heartbeat_at=started,
            )
            try:
                config = spec.config if isinstance(spec.config, Mapping) else {}
                strategy_document = config.get("strategy_document")
                model_document = config.get("model_document")
                if not isinstance(strategy_document, Mapping) or not isinstance(model_document, Mapping):
                    raise ValueError("forward test has no persisted executable strategy/model documents")
                strategy_definition = load_strategy(strategy_document)
                if _content_hash(strategy_definition) != spec.strategy_hash or _content_hash(model_document) != spec.model_hash:
                    raise ValueError("persisted executable documents do not match frozen forward-test hashes")
                if strategy_definition.market_type.value != "prediction":
                    raise ValueError("node Polymarket workers require a prediction strategy")
                if "probability" not in model_document and "yes_probability" not in model_document and not (
                    isinstance(model_document.get("field"), str) and model_document["field"].strip()
                ):
                    raise ValueError("persisted model document is not executable")
                strategy = _PersistedStrategy(strategy_definition)
                model = _PersistedProbabilityModel(model_document)
                market_limit = self.config.max_markets if self.config.max_markets is not None else 1000
                market_ids = tuple(spec.allowed_markets)[:market_limit]
                if not market_ids:
                    market_ids = tuple(self.store.tracked_polymarket_markets(active_only=False, limit=market_limit))
                state_record = self.store.load_paper_state(spec.experiment_id) or {}
                state_payload = state_record.get("state", {})
                raw_cursors = state_payload.get("cursor_by_market", {}) if isinstance(state_payload, Mapping) else {}
                cursors = {
                    str(key): parsed
                    for key, value in raw_cursors.items()
                    if (parsed := parse_timestamp(value)) is not None
                } if isinstance(raw_cursors, Mapping) else {}
                raw_source_cursors = state_payload.get("source_cursor_by_market", {}) if isinstance(state_payload, Mapping) else {}
                source_cursors = {
                    str(key): (parsed, str(value.get("snapshot_id")).strip())
                    for key, value in raw_source_cursors.items()
                    if isinstance(value, Mapping)
                    and str(value.get("snapshot_id", "")).strip()
                    and (parsed := parse_timestamp(value.get("timestamp"))) is not None
                } if isinstance(raw_source_cursors, Mapping) else {}
                opportunity_by_market: dict[str, list[dict[str, Any]]] = {}
                opportunity_rows = self.store.list_opportunity_snapshots(limit=min(4096, max(32, market_limit * 4)))
                for opportunity_row in opportunity_rows:
                    observed_at = parse_timestamp(opportunity_row.get("observed_at"))
                    if observed_at is not None and observed_at > started:
                        continue
                    opportunity = opportunity_row.get("opportunity")
                    if not isinstance(opportunity, Mapping):
                        continue
                    opportunity_market = str(opportunity.get("market_id", "")).strip()
                    if opportunity_market not in market_ids:
                        continue
                    records_for_market = opportunity_by_market.setdefault(opportunity_market, [])
                    if len(records_for_market) < 2:
                        records_for_market.append(dict(opportunity))
                observations: list[dict[str, Any]] = []
                for market_id in market_ids:
                    rows = self.store.load_polymarket_snapshots(
                        market_id,
                        source_start=cursors.get(str(market_id), spec.registration_timestamp),
                        source_end=started,
                        source_after=source_cursors.get(str(market_id)),
                        limit=512,
                    )
                    for row in rows:
                        payload = row.get("payload")
                        if not isinstance(payload, Mapping):
                            continue
                        observation = dict(payload.get("snapshot", payload))
                        observation.setdefault("market_id", market_id)
                        observation.setdefault("timestamp", row.get("source_timestamp") or row.get("observed_at"))
                        observation["source_snapshot_id"] = row.get("snapshot_id")
                        observation["source_timestamp"] = row.get("source_timestamp")
                        for key in ("yes_order_book", "no_order_book", "available_at"):
                            if key in payload:
                                observation[key] = payload[key]
                        opportunity_records = opportunity_by_market.get(str(market_id), [])
                        if opportunity_records:
                            observation["opportunities"] = opportunity_records
                            yes_opportunity = next(
                                (item for item in opportunity_records if str(item.get("outcome", "")).lower() == "yes"),
                                opportunity_records[0],
                            )
                            for key in (
                                "model_probability",
                                "uncertainty",
                                "executable_price",
                                "executable_edge",
                                "executable_ev",
                                "research_quality",
                                "model_version",
                                "family",
                                "correlation_group",
                                "liquidity",
                            ):
                                if key in yes_opportunity:
                                    observation.setdefault(f"opportunity_{key}", yes_opportunity[key])
                        observations.append(observation)
                cycle = run_forward_paper(
                    spec,
                    store=self.store,
                    strategy=strategy,
                    model=model,
                    observations=observations,
                    now=started,
                )
                self.store.save_worker_state(
                    worker_name,
                    "idle",
                    {"pid": os.getpid(), "experiment_id": spec.experiment_id, "cycle": cycle.as_record(), "paper_only": True, "live_execution": False},
                    started_at=started,
                    heartbeat_at=ensure_utc(self.clock()),
                )
            except Exception as exc:
                self.store.save_worker_state(
                    worker_name,
                    "degraded",
                    {"pid": os.getpid(), "experiment_id": spec.experiment_id, "error": str(exc), "paper_only": True, "live_execution": False},
                    started_at=started,
                    heartbeat_at=ensure_utc(self.clock()),
                )
                self._log(logging.ERROR, "paper worker failed for %s: %s", spec.experiment_id, exc)



    def _run_research_queue(self) -> None:
        worker_name = "research-queue"
        started = ensure_utc(self.clock())
        self.store.save_worker_state(
            worker_name,
            "running",
            {"pid": os.getpid(), "paper_only": True, "live_execution": False},
            started_at=started,
            heartbeat_at=started,
        )
        try:
            stats = self.bus.stats()
            self.store.save_worker_state(
                worker_name,
                "idle",
                {
                    "dispatcher": "hermes-safe-interface",
                    "pending": int(stats.get("PENDING", 0)),
                    "stats": dict(stats),
                    "paper_only": True,
                    "live_execution": False,
                },
                started_at=started,
                heartbeat_at=ensure_utc(self.clock()),
            )
        except Exception as exc:
            self.store.save_worker_state(
                worker_name,
                "degraded",
                {"error": str(exc), "paper_only": True, "live_execution": False},
                started_at=started,
                heartbeat_at=ensure_utc(self.clock()),
            )
            self._log(logging.ERROR, "research queue monitor failed: %s", exc)

    def _run_health_monitor(self) -> None:
        worker_name = "health-monitor"
        started = ensure_utc(self.clock())
        self.store.save_worker_state(
            worker_name,
            "running",
            {"pid": os.getpid(), "paper_only": True, "live_execution": False},
            started_at=started,
            heartbeat_at=started,
        )
        try:
            health = self.store.polymarket_health(
                expected_interval_seconds=self.config.interval_seconds,
                stale_after_seconds=max(self.config.interval_seconds * 3.0, self.config.failure_cooldown_seconds),
                now=started,
            )
            health_grade = str(health.get("grade", "")).upper()
            stale_values = health.get("stale_markets", ())
            stale_markets = tuple(islice(iter(stale_values), 32)) if isinstance(stale_values, (list, tuple)) else ()
            stale_market_count = len(stale_values) if isinstance(stale_values, (list, tuple)) else 0
            self.store.save_worker_state(
                worker_name,
                "idle" if health_grade in {"A", "OK", "HEALTHY"} else "degraded",
                {
                    "grade": health.get("grade"),
                    "evidence_grade": health.get("evidence_maturity", {}).get("grade"),
                    "markets": health.get("markets", 0),
                    "stale_markets": list(stale_markets),
                    "stale_market_count": stale_market_count,
                    "trades": health.get("trades", 0),
                    "collection_errors": health.get("collection_errors", 0),
                    "paper_only": True,
                    "live_execution": False,
                },
                started_at=started,
                heartbeat_at=ensure_utc(self.clock()),
            )
        except Exception as exc:
            self.store.save_worker_state(
                worker_name,
                "degraded",
                {"error": str(exc), "paper_only": True, "live_execution": False},
                started_at=started,
                heartbeat_at=ensure_utc(self.clock()),
            )
            self._log(logging.ERROR, "health monitor failed: %s", exc)
    def _acquire_lock(self) -> None:
        path = self.lock_path
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            self._lock_fd = os.open(str(path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError as exc:
            raise RuntimeError(f"another Axiom node holds {path}") from exc
        os.write(self._lock_fd, f"{os.getpid()}\n{time.time_ns()}\n".encode("ascii"))

    def _release_lock(self) -> None:
        fd = self._lock_fd
        if fd is None:
            return
        self._lock_fd = None
        should_unlink = False
        try:
            try:
                owner = int(self.lock_path.read_text(encoding="ascii").splitlines()[0].strip())
            except (FileNotFoundError, OSError, ValueError):
                owner = None
            if owner == os.getpid():
                try:
                    path_stat = os.stat(self.lock_path)
                    fd_stat = os.fstat(fd)
                    path_inode = getattr(path_stat, "st_ino", 0)
                    fd_inode = getattr(fd_stat, "st_ino", 0)
                    inode_available = path_inode not in (None, 0) and fd_inode not in (None, 0)
                    should_unlink = (
                        path_stat.st_dev == fd_stat.st_dev and path_inode == fd_inode
                        if inode_available
                        else True
                    )
                except OSError:
                    should_unlink = False
        finally:
            try:
                os.close(fd)
            except OSError:
                pass
        if should_unlink:
            try:
                self.lock_path.unlink()
            except FileNotFoundError:
                pass

    def _configure_logging(self) -> None:
        logger = logging.getLogger(f"axiom.node.{self.config.worker_name}")
        logger.setLevel(logging.INFO)
        logger.propagate = False
        path = self.log_path
        path.parent.mkdir(parents=True, exist_ok=True)
        handler = RotatingFileHandler(path, maxBytes=self.config.max_log_bytes, backupCount=self.config.backup_count, encoding="utf-8")
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        logger.addHandler(handler)
        self._logger, self._handler = logger, handler
        self._log(logging.INFO, "node started pid=%s db=%s", os.getpid(), self.config.db_path)

    def _close_logging(self) -> None:
        if self._logger is not None and self._handler is not None:
            self._logger.removeHandler(self._handler)
            self._handler.close()
        self._logger, self._handler = None, None

    def _log(self, level: int, message: str, *args: Any) -> None:
        if self._logger is not None:
            self._logger.log(level, message, *args)

    def _heartbeat(self, status: str, payload: Mapping[str, Any]) -> None:
        body = {
            "pid": os.getpid(),
            "lock_path": str(self.lock_path),
            "log_path": str(self.log_path),
            "stale_after_seconds": max(float(self.config.interval_seconds) * 3.0, float(self.config.failure_cooldown_seconds)),
            "paper_only": True,
            "live_execution": False,
            **dict(payload),
        }
        self.store.save_worker_state(
            self.config.worker_name,
            status,
            body,
            started_at=self.started_at,
            heartbeat_at=ensure_utc(self.clock()),
        )


__all__ = ["NodeConfig", "ResearchNode"]
