"""Always-on Windows research and autonomous canary node.

The node owns a durable SQLite state store, a single-process lock, bounded
worker cycles, paper-first research, and one isolated autonomous canary worker.
The worker cannot access research or collector thread state and never changes
the production-live execution flag.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from itertools import islice
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler
import logging
import math
import os
from pathlib import Path
import re
import subprocess
import threading
from typing import Any, Callable, Mapping
import time
from .collector import CollectionCycle, CollectorConfig, PolymarketCollector
from .data import PolymarketAdapter
from .domain import OrderBookSnapshot, ensure_utc, parse_timestamp, to_record, utc_now
from .forward import ForwardTestRegistry, _content_hash
from .opportunity import scan_opportunities
from .paper import CryptoPaperTrader
from .paper_engine import run_forward_paper
from .storage import AxiomStore
from .autonomous import AutonomousResearchConfig, AutonomousResearchProcessor
from .research_bus import DurableResearchBus
from .lifecycle import PromotionCriteria
from .strategy import evaluate_signal_record, load_strategy
from .auto_canary import AutonomousCanaryWorker


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
    max_provider_clock_skew_seconds: float = 5.0
    failure_cooldown_seconds: float = 30.0
    retain_cycles: int = 5
    worker_name: str = "axiom-node"
    max_log_bytes: int = 5_000_000
    backup_count: int = 3
    research_enabled: bool = True
    research_max_items_per_cycle: int = 1
    paper_candidates_per_cycle: int = 4
    paper_observations_per_candidate: int = 64
    research_lease_seconds: float = 300.0
    experiment_total_limit: int = 1000
    experiment_family_limit: int = 250
    max_plan_variants: int = 8
    max_children_per_parent: int = 2
    max_generation_depth: int = 2
    max_experiments_per_day: int = 250
    mutation_enabled: bool = True
    promotion_criteria: PromotionCriteria = field(default_factory=PromotionCriteria)
    crypto_symbol: str = "BTC/USDT"
    crypto_enabled: bool = True
    auto_canary_interval_seconds: float = 60.0

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
        auto_interval = float(self.auto_canary_interval_seconds)
        if not math.isfinite(auto_interval) or auto_interval <= 0:
            raise ValueError("auto_canary_interval_seconds must be finite and positive")
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
        provider_clock_skew = float(self.max_provider_clock_skew_seconds)
        if not math.isfinite(provider_clock_skew) or provider_clock_skew < 0:
            raise ValueError("max_provider_clock_skew_seconds must be finite and non-negative")
        if isinstance(self.retain_cycles, bool) or not isinstance(self.retain_cycles, int) or self.retain_cycles <= 0:
            raise ValueError("retain_cycles must be a positive integer")
        if isinstance(self.max_log_bytes, bool) or not isinstance(self.max_log_bytes, int) or self.max_log_bytes <= 0:
            raise ValueError("max_log_bytes must be a positive integer")
        if isinstance(self.backup_count, bool) or not isinstance(self.backup_count, int) or self.backup_count < 0:
            raise ValueError("backup_count must be a non-negative integer")
        if not isinstance(self.research_enabled, bool):
            raise ValueError("research_enabled must be boolean")
        if isinstance(self.paper_candidates_per_cycle, bool) or not isinstance(self.paper_candidates_per_cycle, int) or self.paper_candidates_per_cycle <= 0:
            raise ValueError("paper_candidates_per_cycle must be a positive integer")
        if isinstance(self.paper_observations_per_candidate, bool) or not isinstance(self.paper_observations_per_candidate, int) or self.paper_observations_per_candidate <= 0:
            raise ValueError("paper_observations_per_candidate must be a positive integer")
        AutonomousResearchConfig(
            max_items_per_cycle=self.research_max_items_per_cycle,
            lease_seconds=self.research_lease_seconds,
            total_limit=self.experiment_total_limit,
            family_limit=self.experiment_family_limit,
            max_plan_variants=self.max_plan_variants,
            max_children_per_parent=self.max_children_per_parent,
            max_generation_depth=self.max_generation_depth,
            max_experiments_per_day=self.max_experiments_per_day,
            mutation_enabled=self.mutation_enabled,
            promotion_criteria=self.promotion_criteria,
        )


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
        self._logger: logging.Logger | None = None
        self._handler: RotatingFileHandler | None = None
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
        self._worker_condition = threading.Condition()
        self._paper_scheduler_lock = threading.Lock()
        self._collector_thread: threading.Thread | None = None
        self._research_thread: threading.Thread | None = None
        self._health_thread: threading.Thread | None = None
        self._auto_canary_thread: threading.Thread | None = None
        self._auto_canary_worker = AutonomousCanaryWorker(
            self.store,
            interval_seconds=config.auto_canary_interval_seconds,
            clock=clock,
        )
        self._collector_error: str | None = None
        self._research_error: str | None = None
        self._run_cycle_base = 0
        self._research_passes = 0
        self._cycles: list[CollectionCycle] = []
        self._restart_count = 0
        self._last_status: dict[str, Any] | None = None
        self._paper_store: AxiomStore | None = None
        self.collector = PolymarketCollector(
            self.provider,
            self.store,
            CollectorConfig(
                interval_seconds=config.interval_seconds,
                depth=config.depth,
                max_markets=config.max_markets,
                max_attempts=config.max_attempts,
                failure_cooldown_seconds=config.failure_cooldown_seconds,
                max_provider_clock_skew_seconds=config.max_provider_clock_skew_seconds,
                retain_cycles=config.retain_cycles,
            ),
            clock=clock,
            sleep=sleep,
        )
        self.bus = DurableResearchBus(self.store)
        self.research_processor = AutonomousResearchProcessor(
            self.store,
            bus=self.bus,
            config=AutonomousResearchConfig(
                max_items_per_cycle=config.research_max_items_per_cycle,
                lease_seconds=config.research_lease_seconds,
                total_limit=config.experiment_total_limit,
                family_limit=config.experiment_family_limit,
                max_plan_variants=config.max_plan_variants,
                max_children_per_parent=config.max_children_per_parent,
                max_generation_depth=config.max_generation_depth,
                max_experiments_per_day=config.max_experiments_per_day,
                mutation_enabled=config.mutation_enabled,
                promotion_criteria=config.promotion_criteria,
            ),
            clock=clock,
        )

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
        self._collector_error = None
        self._research_error = None
        self._cycles.clear()
        self._run_cycle_base = 0
        self._research_passes = 0
        cycle_failure = False
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
            worker_start_states = {
                "polymarket-collector": {
                    "pid": os.getpid(),
                    "configured_interval_seconds": float(self.config.interval_seconds),
                    "paper_only": True,
                    "live_execution": False,
                },
                "paper-engine": {
                    "pid": os.getpid(),
                    "candidate_count": 0,
                    "processed_candidates": 0,
                    "remaining_candidates": 0,
                    "paper_only": True,
                    "live_execution": False,
                },
                "research-engine": {"pid": os.getpid(), "paper_only": True, "live_execution": False},
                "health-monitor": {
                    "pid": os.getpid(),
                    "configured_interval_seconds": float(self.config.interval_seconds),
                    "paper_only": True,
                    "live_execution": False,
                },
                "autonomous-canary": {
                    "pid": os.getpid(),
                    "configured_interval_seconds": float(self.config.auto_canary_interval_seconds),
                    "autonomous": True,
                    "production_live_execution": False,
                },
            }
            for worker_name, payload in worker_start_states.items():
                self.store.save_worker_state(
                    worker_name,
                    "running",
                    payload,
                    started_at=self.started_at,
                    heartbeat_at=self.started_at,
                )
            self._start_heartbeat_watchdog()
            if max_cycles != 0:
                self._start_worker_threads(max_cycles)
                while not self.stop_event.is_set():
                    with self._worker_condition:
                        cycle_count = len(self._cycles) - self._run_cycle_base
                        research_passes = self._research_passes
                        collector = self._collector_thread
                        if max_cycles is not None and cycle_count >= max_cycles and research_passes >= 1:
                            break
                        if collector is not None and not collector.is_alive() and (
                            max_cycles is None or cycle_count < max_cycles
                        ):
                            cycle_failure = True
                            break
                        self._worker_condition.wait(timeout=0.5)
            cycle_failure = cycle_failure or bool(self._collector_error or self._research_error)
            status = (
                "degraded"
                if cycle_failure
                else ("stopped" if self.stop_event.is_set() and max_cycles is None else "idle")
            )
        except KeyboardInterrupt:
            self.stop_event.set()
            raise
        finally:
            self.stop_event.set()
            with self._worker_condition:
                self._worker_condition.notify_all()
            for worker in (
                self._collector_thread,
                self._research_thread,
                self._health_thread,
                self._auto_canary_thread,
            ):
                if worker is not None:
                    worker.join()
            self._stop_heartbeat_watchdog()
            if max_cycles is not None:
                try:
                    self.store.save_worker_state(
                        "autonomous-canary",
                        "idle",
                        {
                            "pid": os.getpid(),
                            "autonomous": True,
                            "production_live_execution": False,
                        },
                        started_at=self.started_at,
                        heartbeat_at=ensure_utc(self.clock()),
                    )
                except Exception:
                    pass
            self._collector_thread = None
            self._research_thread = None
            self._health_thread = None
            self._auto_canary_thread = None
            try:
                self._heartbeat(
                    status,
                    {
                        "cycles": len(self._cycles) - self._run_cycle_base,
                        "attempts": len(self._cycles) - self._run_cycle_base,
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

    def _start_worker_threads(self, max_cycles: int | None) -> None:
        self._collector_thread = threading.Thread(
            target=self._collector_worker_loop,
            args=(max_cycles,),
            name=f"{self.config.worker_name}-collector",
            daemon=True,
        )
        self._research_thread = threading.Thread(
            target=self._research_worker_loop,
            name=f"{self.config.worker_name}-research",
            daemon=True,
        )
        self._health_thread = threading.Thread(
            target=self._health_worker_loop,
            name=f"{self.config.worker_name}-health",
            daemon=True,
        )
        self._auto_canary_thread = threading.Thread(
            target=self._auto_canary_worker_loop,
            name=f"{self.config.worker_name}-autonomous-canary",
            daemon=True,
        )
        self._collector_thread.start()
        self._research_thread.start()
        self._health_thread.start()
        self._auto_canary_thread.start()
    def _auto_canary_worker_loop(self) -> None:
        status = "running"
        try:
            while not self.stop_event.is_set():
                result = self._auto_canary_worker.tick()
                payload = {
                    "pid": os.getpid(),
                    "configured_interval_seconds": float(self.config.auto_canary_interval_seconds),
                    "autonomous": True,
                    "production_live_execution": False,
                    "decision": result.get("decision"),
                    "blocker": result.get("blocker"),
                    "candidate_id": result.get("candidate_id"),
                    "signal_id": result.get("signal_id"),
                }
                status = "degraded" if result.get("status") == "ERROR" else "running"
                self.store.save_worker_state(
                    "autonomous-canary",
                    status,
                    payload,
                    started_at=ensure_utc(self.clock()),
                    heartbeat_at=ensure_utc(self.clock()),
                )
                if self.stop_event.wait(self.config.auto_canary_interval_seconds):
                    break
        except Exception as exc:
            status = "degraded"
            try:
                self.store.save_worker_state(
                    "autonomous-canary",
                    status,
                    {
                        "pid": os.getpid(),
                        "autonomous": True,
                        "production_live_execution": False,
                        "decision": "AUTONOMOUS_WORKER_EXCEPTION",
                        "blocker": "AUTONOMOUS_WORKER_EXCEPTION",
                        "error_type": type(exc).__name__,
                    },
                    started_at=ensure_utc(self.clock()),
                    heartbeat_at=ensure_utc(self.clock()),
                )
            except Exception:
                pass
        finally:
            try:
                self.store.save_worker_state(
                    "autonomous-canary",
                    "stopped" if self.stop_event.is_set() else status,
                    {
                        "pid": os.getpid(),
                        "autonomous": True,
                        "production_live_execution": False,
                    },
                    started_at=ensure_utc(self.clock()),
                    heartbeat_at=ensure_utc(self.clock()),
                )
            except Exception:
                pass

    def _collector_for_worker(self) -> tuple[PolymarketCollector, AxiomStore | None]:
        """Run collection on the node store while preserving independent cadence."""
        if not isinstance(self.collector, PolymarketCollector):
            return self.collector, None  # type: ignore[return-value]
        return self.collector, None

    def _update_collector_schedule(self, store: AxiomStore, next_scheduled: datetime) -> None:
        state = store.get_collector_state("polymarket") or {}
        state.update(
            {
                "configured_interval_seconds": float(self.config.interval_seconds),
                "next_scheduled_collection_at": ensure_utc(next_scheduled).isoformat(),
                "worker_heartbeat_at": ensure_utc(self.clock()).isoformat(),
            }
        )
        store.set_collector_state("polymarket", state)

    def _save_collector_worker_state(
        self,
        status: str,
        *,
        next_scheduled: datetime | None = None,
        cycle: CollectionCycle | None = None,
        error: str | None = None,
    ) -> None:
        payload: dict[str, Any] = {
            "pid": os.getpid(),
            "configured_interval_seconds": float(self.config.interval_seconds),
            "next_scheduled_collection_at": (
                ensure_utc(next_scheduled).isoformat() if next_scheduled is not None else None
            ),
            "worker_heartbeat_at": ensure_utc(self.clock()).isoformat(),
            "paper_only": True,
            "live_execution": False,
        }
        if cycle is not None:
            payload.update(
                {
                    "last_cycle": cycle.as_record(),
                    "last_cycle_started_at": cycle.started_at.isoformat(),
                    "last_cycle_ended_at": cycle.ended_at.isoformat(),
                    "last_cycle_duration_seconds": cycle.duration_seconds,
                    "last_cycle_markets_attempted": cycle.markets_attempted,
                    "last_cycle_markets_successful": cycle.markets_successful,
                    "last_cycle_markets_failed": cycle.markets_failed,
                    "last_successful_collection_at": cycle.ended_at.isoformat()
                    if cycle.markets_successful > 0
                    else None,
                }
            )
        if error:
            payload["error"] = error
        self.store.save_worker_state(
            "polymarket-collector",
            status,
            payload,
            started_at=cycle.started_at if cycle is not None else ensure_utc(self.clock()),
            heartbeat_at=ensure_utc(self.clock()),
        )

    def _collector_worker_loop(self, max_cycles: int | None) -> None:
        collector, owned_store = self._collector_for_worker()
        completed = 0
        failed_attempts = 0
        next_due = time.monotonic()
        next_scheduled = ensure_utc(self.clock())
        try:
            schedule_store = owned_store or self.store
            self._save_collector_worker_state("running", next_scheduled=next_scheduled)
            self._update_collector_schedule(schedule_store, next_scheduled)
            while not self.stop_event.is_set() and (max_cycles is None or completed < max_cycles):
                if self._external_stop_requested():
                    break
                scheduled_for = next_scheduled
                try:
                    cycle = collector.collect_once()
                except Exception as exc:
                    failed_attempts += 1
                    self._collector_error = str(exc)
                    self._log(logging.ERROR, "collector worker cycle failed: %s", exc)
                    self._save_collector_worker_state(
                        "degraded",
                        next_scheduled=scheduled_for,
                        error=str(exc),
                    )
                    if max_cycles is not None and failed_attempts >= max_cycles:
                        break
                else:
                    failed_attempts = 0
                    self._collector_error = None
                    with self._worker_condition:
                        self._cycles.append(cycle)
                        if len(self._cycles) > self._run_cycle_base + self.config.retain_cycles:
                            del self._cycles[: -(self.config.retain_cycles)]
                        completed += 1
                        self._worker_condition.notify_all()
                    next_scheduled = cycle.started_at + timedelta(seconds=float(self.config.interval_seconds))
                    self._update_collector_schedule(schedule_store, next_scheduled)
                    self._save_collector_worker_state(
                        "running",
                        next_scheduled=next_scheduled,
                        cycle=cycle,
                    )
                if max_cycles is not None and completed >= max_cycles:
                    break
                next_due += float(self.config.interval_seconds)
                delay = max(0.0, next_due - time.monotonic())
                if delay <= 0:
                    next_due = time.monotonic()
                    next_scheduled = ensure_utc(self.clock())
                else:
                    next_scheduled = ensure_utc(self.clock()) + timedelta(seconds=delay)
                self._update_collector_schedule(schedule_store, next_scheduled)
                if self.stop_event.wait(delay):
                    break
        finally:
            final_status = (
                "stopped"
                if self.stop_event.is_set() and max_cycles is None
                else ("degraded" if self._collector_error else "idle")
            )
            try:
                self._save_collector_worker_state(
                    final_status,
                    next_scheduled=next_scheduled,
                    error=self._collector_error,
                )
            except Exception:
                pass
            if owned_store is not None:
                owned_store.close()
            with self._worker_condition:
                self._worker_condition.notify_all()

    def _research_worker_loop(self) -> None:
        first_cycle = self._run_cycle_base
        try:
            self.store.save_worker_state(
                "research-engine",
                "running",
                {"pid": os.getpid(), "paper_only": True, "live_execution": False},
                started_at=ensure_utc(self.clock()),
                heartbeat_at=ensure_utc(self.clock()),
            )
            while not self.stop_event.is_set():
                with self._worker_condition:
                    while (
                        not self.stop_event.is_set()
                        and len(self._cycles) <= first_cycle
                        and (self._collector_thread is None or self._collector_thread.is_alive())
                    ):
                        self._worker_condition.wait(timeout=0.5)
                    if self.stop_event.is_set():
                        break
                    if len(self._cycles) <= first_cycle and self._collector_thread is not None and not self._collector_thread.is_alive():
                        self._research_error = "collector produced no completed cycle"
                        break
                started = ensure_utc(self.clock())
                try:
                    cycle_stats = self._run_research_cycle()
                    self._research_error = None
                    status = "idle"
                except Exception as exc:
                    self._research_error = str(exc)
                    self._log(logging.ERROR, "research worker cycle failed: %s", exc)
                    cycle_stats = {"error": str(exc)}
                    status = "degraded"
                with self._worker_condition:
                    self._research_passes += 1
                    research_passes = self._research_passes
                    self._worker_condition.notify_all()
                queue_cycle = cycle_stats.get("research_queue") if isinstance(cycle_stats, Mapping) else None
                queue_items_processed = (
                    int(queue_cycle.get("claimed", 0))
                    if isinstance(queue_cycle, Mapping)
                    else 0
                )
                self.store.save_worker_state(
                    "research-engine",
                    status,
                    {
                        "pid": os.getpid(),
                        "cycle": cycle_stats,
                        "passes": research_passes,
                        "queue_items_processed": queue_items_processed,
                        "cycle_started_at": started.isoformat(),
                        "cycle_ended_at": ensure_utc(self.clock()).isoformat(),
                        "paper_only": True,
                        "live_execution": False,
                    },
                    started_at=started,
                    heartbeat_at=ensure_utc(self.clock()),
                )
                if self.stop_event.wait(1.0):
                    break
        finally:
            try:
                self.store.save_worker_state(
                    "research-engine",
                    "stopped" if self.stop_event.is_set() else "degraded",
                    {
                        "pid": os.getpid(),
                        "error": self._research_error,
                        "passes": self._research_passes,
                        "paper_only": True,
                        "live_execution": False,
                    },
                    started_at=ensure_utc(self.clock()),
                    heartbeat_at=ensure_utc(self.clock()),
                )
            except Exception:
                pass
            with self._worker_condition:
                self._worker_condition.notify_all()

    def _health_worker_loop(self) -> None:
        seen = self._run_cycle_base
        try:
            while not self.stop_event.is_set():
                with self._worker_condition:
                    while (
                        not self.stop_event.is_set()
                        and len(self._cycles) <= seen
                        and (self._collector_thread is None or self._collector_thread.is_alive())
                    ):
                        self._worker_condition.wait(timeout=0.5)
                    if self.stop_event.is_set():
                        break
                    if len(self._cycles) <= seen and self._collector_thread is not None and not self._collector_thread.is_alive():
                        break
                    seen = len(self._cycles)
                try:
                    self._run_health_monitor()
                except Exception as exc:
                    self._log(logging.ERROR, "health worker cycle failed: %s", exc)
        finally:
            try:
                health_payload: dict[str, Any] = {
                    "pid": os.getpid(),
                    "paper_only": True,
                    "live_execution": False,
                }
                for item in self.store.list_worker_states(limit=2048):
                    if item.get("worker_name") == "health-monitor" and isinstance(item.get("payload"), Mapping):
                        health_payload = dict(item["payload"])
                        health_payload.update({"pid": os.getpid(), "paper_only": True, "live_execution": False})
                        break
                self.store.save_worker_state(
                    "health-monitor",
                    "stopped" if self.stop_event.is_set() or self._collector_error is None else "degraded",
                    health_payload,
                    started_at=ensure_utc(self.clock()),
                    heartbeat_at=ensure_utc(self.clock()),
                )
            except Exception:
                pass
            with self._worker_condition:
                self._worker_condition.notify_all()

    def _run_research_cycle(self) -> dict[str, Any]:
        self._run_crypto_paper()
        self._run_opportunity_pipeline()
        self.bus.resume_expired(now=ensure_utc(self.clock()))
        paper_stats = self._run_paper_workers()
        self.research_processor.reevaluate_forward_candidates(now=ensure_utc(self.clock()))
        queue_stats = self._run_research_queue()
        return {"paper": paper_stats, "research_queue": queue_stats}

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
                    heartbeat = ensure_utc(self.clock())
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
                        heartbeat_at=heartbeat,
                    )
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
                        heartbeat_at=heartbeat,
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
        with self._worker_condition:
            self._worker_condition.notify_all()

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
            child_payload = worker_state.get("payload")
            child_pid = child_payload.get("pid") if isinstance(child_payload, Mapping) else None
            try:
                if worker_pid and child_pid is not None and int(child_pid) != worker_pid:
                    continue
            except (TypeError, ValueError):
                pass
            child_status = str(worker_state.get("status", "not_started")).lower()
            child_workers[worker_name] = {
                "status": child_status,
                "heartbeat_at": worker_state.get("heartbeat_at"),
                "payload": child_payload,
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
            "autonomous_research": {
                "enabled": self.config.research_enabled,
                "queue": self.store.research_queue_stats(),
                "lifecycle_funnel": self.store.candidate_lifecycle_funnel(),
                "rejection_reasons": self.store.candidate_rejection_reasons(limit=64),
                "budget": self.store.load_experiment_budget("autonomous"),
                "limits": {
                    "items_per_cycle": self.config.research_max_items_per_cycle,
                    "paper_candidates_per_cycle": self.config.paper_candidates_per_cycle,
                    "paper_observations_per_candidate": self.config.paper_observations_per_candidate,
                    "total_experiments": self.config.experiment_total_limit,
                    "family_experiments": self.config.experiment_family_limit,
                    "daily_experiments": self.config.max_experiments_per_day,
                    "plan_variants": self.config.max_plan_variants,
                    "children_per_parent": self.config.max_children_per_parent,
                    "generation_depth": self.config.max_generation_depth,
                    "mutations_enabled": self.config.mutation_enabled,
                },
            },
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
    def _run_persisted_opportunity_pipeline(self, started: datetime) -> bool:
        """Scan the collector's newest evidence without issuing a second sweep."""
        if self.opportunity_model is not None and not isinstance(self.opportunity_model, Mapping):
            return False
        collector_state = self.store.get_collector_state("polymarket") or {}
        has_completed_collection = bool(collector_state.get("last_cycle_ended_at"))
        latest_rows = self.store.load_latest_polymarket_snapshots(
            source_type="FORWARD_COLLECTED",
            limit=self.config.max_markets,
        )
        freshness_seconds = max(120.0, float(self.config.interval_seconds) * 2.0)
        cutoff = started - timedelta(seconds=freshness_seconds)
        records: list[dict[str, Any]] = []
        evidence_by_market: dict[str, dict[str, Any]] = {}
        observed_values: list[datetime] = []
        for row in latest_rows:
            observed_at = parse_timestamp(row.get("observed_at"))
            if observed_at is None or observed_at > started or observed_at < cutoff:
                continue
            payload = row.get("payload")
            if not isinstance(payload, Mapping):
                continue
            snapshot = payload.get("snapshot", payload)
            if not isinstance(snapshot, Mapping):
                continue
            market_id = str(row.get("market_id") or snapshot.get("market_id") or "").strip()
            if not market_id:
                continue
            record = dict(snapshot)
            record["market_id"] = market_id
            source_timestamp = parse_timestamp(row.get("source_timestamp"))
            record["timestamp"] = (source_timestamp or observed_at).isoformat()
            for key in ("yes_order_book", "no_order_book", "available_at"):
                if key in payload:
                    record[key] = payload[key]
            record.setdefault("research_quality", payload.get("research_quality") or row.get("quality"))
            records.append(record)
            evidence = payload.get("evidence")
            evidence_by_market[market_id] = dict(evidence) if isinstance(evidence, Mapping) else {
                key: payload.get(key)
                for key in ("request_started_at", "source_timestamp", "provider_timestamp", "response_received_at")
                if payload.get(key) is not None
            }
            observed_values.append(observed_at)
        if not has_completed_collection and not records:
            return False
        worker_name = "opportunity-pipeline"
        if not records:
            reason = "no fresh persisted forward evidence is available"
            self.store.save_worker_state(
                worker_name,
                "degraded",
                {
                    "markets": 0,
                    "opportunities": 0,
                    "inserted": 0,
                    "degrading_reason": reason,
                    "last_degrading_reason": reason,
                    "last_error": reason,
                    "paper_only": True,
                    "live_execution": False,
                },
                started_at=started,
                heartbeat_at=ensure_utc(self.clock()),
            )
            return True
        probabilities: dict[str, float] = {}
        uncertainties: dict[str, float] = {}
        model_versions: set[str] = set()
        for record in records:
            market_id = str(record.get("market_id", "")).strip()
            if self.opportunity_model is None:
                estimate = record.get("yes_mid")
                if estimate is None:
                    estimate = record.get("yes_ask")
                version = "market-price-baseline-v1"
                quality = str(record.get("research_quality") or "PRICE_PROXY")
                uncertainty = 1.0
            else:
                estimate = self.opportunity_model.get(market_id)
                version = "configured-model"
                quality = "MODEL_ESTIMATE"
                uncertainty = 0.0
            if isinstance(estimate, Mapping):
                probability_value = estimate.get("probability", estimate.get("yes_probability", estimate.get("prediction")))
                version = str(estimate.get("model_version", version))
                quality = str(estimate.get("research_quality", quality))
                uncertainty = float(estimate.get("uncertainty", uncertainty) or 0.0)
            else:
                probability_value = estimate
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
                }
            )
        opportunities = scan_opportunities(
            records,
            probabilities,
            uncertainties=uncertainties,
            model_version=sorted(model_versions)[0] if model_versions else "market-price-baseline-v1",
        )
        opportunity_records: list[dict[str, Any]] = []
        for item in opportunities:
            item_record = item.as_record()
            evidence = evidence_by_market.get(item.market_id)
            if evidence:
                item_record["evidence"] = dict(evidence)
                for key in ("request_started_at", "source_timestamp", "provider_timestamp", "response_received_at"):
                    item_record[key] = evidence.get(key)
            opportunity_records.append(item_record)
        observed_at = max(observed_values) if observed_values else started
        inserted = self.store.save_opportunity_snapshots(observed_at, opportunity_records)
        self.store.save_worker_state(
            worker_name,
            "idle",
            {
                "markets": len(records),
                "opportunities": len(opportunities),
                "inserted": inserted,
                "model_versions": sorted(model_versions),
                "source": "persisted_forward_collection",
                "source_observed_at": observed_at.isoformat(),
                "paper_only": True,
                "live_execution": False,
            },
            started_at=started,
            heartbeat_at=ensure_utc(self.clock()),
        )
        return True

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
        if self._run_persisted_opportunity_pipeline(started):
            return
        try:
            provider_errors: list[str] = []
            degrading_reasons: list[str] = []
            markets: tuple[Any, ...] = ()
            market_request_started_at = started
            market_response_received_at = started
            for attempt in range(self.config.max_attempts):
                call_error: str | None = None
                call_retryable = False
                market_request_started_at = ensure_utc(self.clock())
                try:
                    try:
                        markets = tuple(
                            islice(
                                iter(self.provider.markets(active=True, limit=self.config.max_markets)),
                                self.config.max_markets,
                            )
                        )
                    except TypeError:
                        market_request_started_at = ensure_utc(self.clock())
                        markets = tuple(islice(self.provider.markets(active=True), self.config.max_markets))
                except Exception as exc:
                    call_error = str(exc)
                    call_retryable = isinstance(exc, (OSError, TimeoutError))
                    markets = ()
                finally:
                    market_response_received_at = ensure_utc(self.clock())
                transport_details, transport_retryable = _consume_provider_errors(self.provider, "market discovery")
                provider_errors.extend(transport_details)
                degrading_reasons.extend(transport_details)
                if call_error:
                    detail = f"market discovery: {call_error}"
                    provider_errors.append(detail)
                    degrading_reasons.append(detail)
                retryable = call_retryable or transport_retryable
                if not retryable or attempt + 1 >= self.config.max_attempts:
                    break
                self.sleep(max(0.0, min(float(self.config.failure_cooldown_seconds), 2.0**attempt)))
            markets_retrieved_at = market_response_received_at
            opportunities_observed_at = markets_retrieved_at
            records: list[dict[str, Any]] = []
            evidence_by_market: dict[str, dict[str, Any]] = {}
            probabilities: dict[str, float] = {}
            uncertainties: dict[str, float] = {}
            model_versions: set[str] = set()
            max_skew = timedelta(seconds=float(self.config.max_provider_clock_skew_seconds))

            def retrieval_evidence(
                request_started_at: datetime,
                source_timestamp: datetime | None,
                response_received_at: datetime,
            ) -> dict[str, Any]:
                request_started = request_started_at.isoformat()
                response_received = response_received_at.isoformat()
                source = source_timestamp.isoformat() if source_timestamp is not None else None
                return {
                    "request_started_at": request_started,
                    "source_timestamp": source,
                    "provider_timestamp": source,
                    "response_received_at": response_received,
                    "request_window": {
                        "started_at": request_started,
                        "received_at": response_received,
                    },
                }

            for market in markets:
                market_id = str(getattr(market, "market_id", "")).strip()
                if not market_id:
                    continue
                record = to_record(market)
                if not isinstance(record, Mapping):
                    continue
                record = dict(record)
                market_timestamp = parse_timestamp(record.get("timestamp"))
                market_evidence = retrieval_evidence(
                    market_request_started_at,
                    market_timestamp,
                    market_response_received_at,
                )
                market_allowed_until = market_response_received_at + max_skew
                if market_timestamp is not None and market_timestamp > market_allowed_until:
                    detail = "market timestamp is in the future"
                    reason = f"{market_id}: {detail}"
                    self.store.save_collection_error(
                        market_id,
                        market_response_received_at,
                        "future_observation",
                        detail,
                        {
                            **market_evidence,
                            "timestamp": market_timestamp.isoformat(),
                            "allowed_until": market_allowed_until.isoformat(),
                        },
                    )
                    degrading_reasons.append(reason)
                    continue

                books: Any = {}
                books_request_started_at = ensure_utc(self.clock())
                books_response_received_at = books_request_started_at
                for attempt in range(self.config.max_attempts):
                    call_error = None
                    call_retryable = False
                    books_request_started_at = ensure_utc(self.clock())
                    try:
                        books = self.provider.order_books(market_id, depth=self.config.depth)
                    except Exception as exc:
                        call_error = str(exc)
                        call_retryable = isinstance(exc, (OSError, TimeoutError))
                        books = {}
                    finally:
                        books_response_received_at = ensure_utc(self.clock())
                    transport_details, transport_retryable = _consume_provider_errors(
                        self.provider,
                        f"order books {market_id}",
                    )
                    provider_errors.extend(transport_details)
                    degrading_reasons.extend(transport_details)
                    if call_error:
                        detail = f"order books {market_id}: {call_error}"
                        provider_errors.append(detail)
                        degrading_reasons.append(detail)
                    retryable = call_retryable or transport_retryable
                    if not retryable or attempt + 1 >= self.config.max_attempts:
                        break
                    self.sleep(max(0.0, min(float(self.config.failure_cooldown_seconds), 2.0**attempt)))
                opportunities_observed_at = max(opportunities_observed_at, books_response_received_at)
                order_book_evidence: dict[str, dict[str, Any]] = {}
                source_timestamps = [stamp for stamp in (market_timestamp,) if stamp is not None]
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
                        evidence = retrieval_evidence(
                            books_request_started_at,
                            book_timestamp,
                            books_response_received_at,
                        )
                        order_book_evidence[outcome] = evidence
                        book_allowed_until = books_response_received_at + max_skew
                        if book_timestamp is not None and book_timestamp > book_allowed_until:
                            detail = "order-book timestamp is in the future"
                            reason = f"{market_id} {outcome}: {detail}"
                            self.store.save_collection_error(
                                market_id,
                                books_response_received_at,
                                "future_order_book",
                                detail,
                                {
                                    **evidence,
                                    "outcome": outcome,
                                    "timestamp": book_timestamp.isoformat(),
                                    "allowed_until": book_allowed_until.isoformat(),
                                },
                            )
                            degrading_reasons.append(reason)
                            continue
                        if isinstance(book_record, Mapping):
                            record[f"{outcome}_order_book"] = dict(book_record)
                        if book_timestamp is not None:
                            source_timestamps.append(book_timestamp)

                source_timestamp = max(source_timestamps) if source_timestamps else None
                if source_timestamp is not None:
                    # The scanner must see the canonical latest provider time;
                    # otherwise it intentionally discards a newer order book.
                    record["timestamp"] = source_timestamp.isoformat()
                evidence = {
                    "market": market_evidence,
                    "order_books": order_book_evidence,
                    **retrieval_evidence(
                        market_request_started_at,
                        source_timestamp,
                        books_response_received_at,
                    ),
                }
                evidence_by_market[market_id] = evidence
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
            opportunity_records: list[dict[str, Any]] = []
            for item in opportunities:
                item_record = item.as_record()
                evidence = evidence_by_market.get(item.market_id)
                if isinstance(evidence, Mapping):
                    item_record["evidence"] = dict(evidence)
                    for key in ("request_started_at", "source_timestamp", "provider_timestamp", "response_received_at"):
                        item_record[key] = evidence.get(key)
                opportunity_records.append(item_record)
            inserted = self.store.save_opportunity_snapshots(opportunities_observed_at, opportunity_records)
            latest_reason = degrading_reasons[-1] if degrading_reasons else None
            self.store.save_worker_state(
                worker_name,
                "degraded" if degrading_reasons else "idle",
                {
                    "markets": len(records),
                    "opportunities": len(opportunities),
                    "inserted": inserted,
                    "model_versions": sorted(model_versions),
                    "provider_errors": provider_errors[-32:],
                    "degrading_reason": latest_reason,
                    "last_degrading_reason": latest_reason,
                    "last_error": latest_reason,
                    "paper_only": True,
                    "live_execution": False,
                },
                started_at=started,
                heartbeat_at=ensure_utc(self.clock()),
            )
        except Exception as exc:
            reason = str(exc)
            self.store.save_worker_state(
                worker_name,
                "degraded",
                {
                    "error": reason,
                    "last_error": reason,
                    "degrading_reason": reason,
                    "last_degrading_reason": reason,
                    "paper_only": True,
                    "live_execution": False,
                },
                started_at=started,
                heartbeat_at=ensure_utc(self.clock()),
            )
            self._log(logging.ERROR, "opportunity pipeline failed: %s", exc)

    def _run_paper_workers(self) -> dict[str, Any]:
        if not self._paper_scheduler_lock.acquire(blocking=False):
            return {
                "candidate_count": 0,
                "processed_candidates": 0,
                "successful_candidates": 0,
                "observations_processed": 0,
                "fills_inserted": 0,
                "remaining_candidates": 0,
                "next_candidate_id": None,
                "skipped": "scheduler already running",
            }
        try:
            db_path = str(self.config.db_path).strip()
            if self._paper_store is None and db_path.lower() != ":memory:":
                with AxiomStore(db_path) as paper_store:
                    self._paper_store = paper_store
                    try:
                        return self._run_paper_workers_locked()
                    finally:
                        self._paper_store = None
            return self._run_paper_workers_locked()
        finally:
            self._paper_scheduler_lock.release()

    def _run_paper_workers_locked(self) -> dict[str, Any]:
        paper_store = self._paper_store or self.store
        registry = ForwardTestRegistry(paper_store)
        specs = sorted(
            (
                spec
                for spec in registry.list()
                if not bool((spec.config if isinstance(spec.config, Mapping) else {}).get("historical_replay"))
            ),
            key=lambda spec: (spec.start_timestamp, spec.experiment_id),
        )
        scheduler_name = "paper-engine"
        state = self.store.get_scheduler_state(scheduler_name) or {}
        if not specs:
            stats = {
                "candidate_count": 0,
                "processed_candidates": 0,
                "successful_candidates": 0,
                "observations_processed": 0,
                "fills_inserted": 0,
                "remaining_candidates": 0,
                "next_candidate_id": None,
            }
            self.store.set_scheduler_state(scheduler_name, {**state, **stats, "cursor": 0, "last_candidate_id": None})
            self.store.save_worker_state(
                scheduler_name,
                "idle",
                {"pid": os.getpid(), **stats, "paper_only": True, "live_execution": False},
                started_at=ensure_utc(self.clock()),
                heartbeat_at=ensure_utc(self.clock()),
            )
            return stats
        try:
            cursor = int(state.get("cursor", 0))
        except (TypeError, ValueError):
            cursor = 0
        cursor %= len(specs)
        last_candidate_id = str(state.get("last_candidate_id", "")).strip()
        if last_candidate_id:
            matching = next((index for index, spec in enumerate(specs) if spec.experiment_id == last_candidate_id), None)
            if matching is not None:
                cursor = (matching + 1) % len(specs)
        limit = min(self.config.paper_candidates_per_cycle, len(specs))
        selected = tuple(specs[(cursor + offset) % len(specs)] for offset in range(limit))
        next_cursor = (cursor + limit) % len(specs)
        stats: dict[str, Any] = {
            "candidate_count": len(specs),
            "processed_candidates": len(selected),
            "successful_candidates": 0,
            "observations_processed": 0,
            "fills_inserted": 0,
            "remaining_candidates": max(0, len(specs) - len(selected)),
            "next_candidate_id": specs[next_cursor].experiment_id if specs else None,
            "processed_candidate_ids": [spec.experiment_id for spec in selected],
        }
        self.store.set_scheduler_state(
            scheduler_name,
            {
                **state,
                "cursor": next_cursor,
                "last_candidate_id": selected[-1].experiment_id,
                "candidate_count": len(specs),
                "next_candidate_id": stats["next_candidate_id"],
                "last_reserved_candidate_ids": stats["processed_candidate_ids"],
                "last_cycle_started_at": ensure_utc(self.clock()).isoformat(),
            },
        )
        started = ensure_utc(self.clock())
        self.store.save_worker_state(
            scheduler_name,
            "running",
            {
                "pid": os.getpid(),
                "candidate_count": len(specs),
                "processed_candidates": 0,
                "remaining_candidates": stats["remaining_candidates"],
                "next_candidate_id": stats["next_candidate_id"],
                "paper_only": True,
                "live_execution": False,
            },
            started_at=started,
            heartbeat_at=started,
        )
        for spec in selected:
            result = self._run_single_paper_worker(spec)
            if result is None:
                continue
            if "error" not in result:
                stats["successful_candidates"] += 1
                stats["observations_processed"] += int(result.get("observations_processed", 0))
                stats["fills_inserted"] += int(result.get("fills_inserted", 0))
            self.sleep(0)
        stats["cycle_ended_at"] = ensure_utc(self.clock()).isoformat()
        self.store.set_scheduler_state(
            scheduler_name,
            {
                **(self.store.get_scheduler_state(scheduler_name) or state),
                **stats,
                "last_cycle_ended_at": stats["cycle_ended_at"],
            },
        )
        self.store.save_worker_state(
            scheduler_name,
            "idle",
            {"pid": os.getpid(), **stats, "paper_only": True, "live_execution": False},
            started_at=started,
            heartbeat_at=ensure_utc(self.clock()),
        )
        return stats

    def _run_single_paper_worker(self, spec: Any) -> dict[str, Any] | None:
        worker_name = f"paper:{spec.experiment_id}"
        started = ensure_utc(self.clock())
        paper_store = self._paper_store or self.store
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
            strategy_hash = _content_hash(strategy_definition.to_dict())
            if strategy_hash != spec.strategy_hash and _content_hash(strategy_definition) != spec.strategy_hash:
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
                market_ids = tuple(paper_store.tracked_polymarket_markets(active_only=False, limit=market_limit))
            state_record = paper_store.load_paper_state(spec.experiment_id) or {}
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
            opportunity_rows = paper_store.list_opportunity_snapshots(limit=min(4096, max(32, market_limit * 4)))
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
            observation_limit = self.config.paper_observations_per_candidate
            for market_id in market_ids:
                if len(observations) >= observation_limit:
                    break
                rows = paper_store.load_polymarket_snapshots(
                    market_id,
                    source_start=cursors.get(str(market_id), spec.registration_timestamp),
                    source_end=started,
                    source_after=source_cursors.get(str(market_id)),
                    limit=min(512, observation_limit - len(observations)),
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
                    if len(observations) >= observation_limit:
                        break
            cycle = run_forward_paper(
                spec,
                store=paper_store,
                strategy=strategy,
                model=model,
                observations=observations,
                now=started,
            )
            cycle_payload = cycle.as_record()
            self.store.save_worker_state(
                worker_name,
                "idle",
                {"pid": os.getpid(), "experiment_id": spec.experiment_id, "cycle": cycle_payload, "paper_only": True, "live_execution": False},
                started_at=started,
                heartbeat_at=ensure_utc(self.clock()),
            )
            return cycle_payload
        except Exception as exc:
            self.store.save_worker_state(
                worker_name,
                "degraded",
                {"pid": os.getpid(), "experiment_id": spec.experiment_id, "error": str(exc), "paper_only": True, "live_execution": False},
                started_at=started,
                heartbeat_at=ensure_utc(self.clock()),
            )
            self._log(logging.ERROR, "paper worker failed for %s: %s", spec.experiment_id, exc)
            return {"error": str(exc)}



    def _run_research_queue(self) -> dict[str, Any]:
        worker_name = "research-queue"
        started = ensure_utc(self.clock())
        self.store.save_worker_state(
            worker_name,
            "running",
            {"pid": os.getpid(), "paper_only": True, "live_execution": False},
            started_at=started,
            heartbeat_at=started,
        )
        control = self.store.get_scheduler_state("hermes-control") or {}
        control = control if isinstance(control, Mapping) else {}
        hermes_paused = str(control.get("status", "ACTIVE")).upper() == "PAUSED"
        try:
            if not self.config.research_enabled:
                cycle_record = {
                    "released": 0,
                    "claimed": 0,
                    "completed": 0,
                    "rejected": 0,
                    "failed": 0,
                    "results": [],
                    "disabled": True,
                    "paper_only": True,
                }
            elif hermes_paused:
                cycle_record = {
                    "released": 0,
                    "claimed": 0,
                    "completed": 0,
                    "rejected": 0,
                    "failed": 0,
                    "results": [],
                    "paused": True,
                    "job_id": control.get("job_id"),
                    "paper_only": True,
                }
            else:
                cycle = self.research_processor.process_pending(worker=worker_name, now=started)
                cycle_record = cycle.as_record()
            stats = self.bus.stats()
            self.store.save_worker_state(
                worker_name,
                "paused" if hermes_paused else ("disabled" if not self.config.research_enabled else "idle"),
                {
                    "dispatcher": "autonomous-research-processor",
                    "pending": int(stats.get("PENDING", 0)),
                    "stats": dict(stats),
                    "last_cycle": cycle_record,
                    "paper_only": True,
                    "live_execution": False,
                },
                started_at=started,
                heartbeat_at=ensure_utc(self.clock()),
            )
            return dict(cycle_record)
        except Exception as exc:
            self.store.save_worker_state(
                worker_name,
                "degraded",
                {"error": str(exc), "paper_only": True, "live_execution": False},
                started_at=started,
                heartbeat_at=ensure_utc(self.clock()),
            )
            self._log(logging.ERROR, "research queue processor failed: %s", exc)
            return {"error": str(exc)}

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
            reasons = health.get("reasons", ())
            current_failures = health.get("current_failures", ())
            self.store.save_worker_state(
                worker_name,
                "idle" if health_grade in {"A", "OK", "HEALTHY"} else "degraded",
                {
                    "grade": health.get("grade"),
                    "reason_code": health.get("reason_code"),
                    "reasons": list(reasons) if isinstance(reasons, (list, tuple)) else [],
                    "degrading_reason": (
                        reasons[0].get("reason") if reasons and isinstance(reasons[0], Mapping) else None
                    ),
                    "evidence_grade": health.get("historical_maturity_grade", health.get("evidence_maturity", {}).get("grade")),
                    "historical_error_count": health.get("historical_error_count", 0),
                    "markets": health.get("markets", 0),
                    "scheduled_market_count": health.get("scheduled_market_count", 0),
                    "stale_markets": list(stale_markets),
                    "stale_market_count": stale_market_count,
                    "gaps": health.get("gaps", []),
                    "current_failures": list(current_failures) if isinstance(current_failures, (list, tuple)) else [],
                    "top_failure_codes": health.get("top_failure_codes", []),
                    "trades": health.get("trades", 0),
                    "collection_errors": health.get("collection_errors", 0),
                    "configured_interval_seconds": health.get("configured_interval_seconds"),
                    "effective_collection_cadence_seconds": health.get("effective_collection_cadence_seconds"),
                    "last_cycle_duration_seconds": health.get("last_cycle_duration_seconds"),
                    "last_successful_cycle": health.get("last_successful_cycle"),
                    "last_cycle_markets_attempted": health.get("last_cycle_markets_attempted", 0),
                    "last_cycle_markets_successful": health.get("last_cycle_markets_successful", 0),
                    "last_cycle_markets_failed": health.get("last_cycle_markets_failed", 0),
                    "last_cycle_started_at": health.get("last_cycle_started_at"),
                    "last_cycle_ended_at": health.get("last_cycle_ended_at"),
                    "next_scheduled_collection_at": health.get("next_scheduled_collection_at"),
                    "worker_heartbeat_at": health.get("worker_heartbeat_at"),
                    "window_start": health.get("window_start"),
                    "window_end": health.get("window_end"),
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
