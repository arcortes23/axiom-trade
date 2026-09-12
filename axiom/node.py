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
import errno
import os
from pathlib import Path
import re
import subprocess
import threading
from typing import Any, Callable, Mapping
import time
import uuid
from .collector import CollectionCycle, CollectorConfig, PolymarketCollector
from .bootstrap import (
    HistoricalBootstrapper,
    POLYMARKET_DATASET_ID,
    POLYMARKET_HISTORICAL_JOB_NAME,
)
from .data import PolymarketAdapter, SyntheticPredictionProvider
from .domain import OrderBookSnapshot, ensure_utc, parse_timestamp, to_record, utc_now
from .forward import ForwardTestRegistry, _content_hash
from .opportunity import scan_opportunities
from .paper import CryptoPaperTrader
from .paper_engine import (
    PAPER_STATE_EXECUTION_BINDING_MISMATCH,
    paper_execution_binding,
    paper_state_binding_blocker,
    run_forward_paper,
)
from .storage import AxiomStore
from .autonomous import AutonomousResearchConfig, AutonomousResearchProcessor
from .research_bus import DurableResearchBus
from .lifecycle import PromotionCriteria
from .strategy import evaluate_signal_record, load_strategy
from .auto_canary import AutonomousCanaryWorker
from .canary import CanaryBlocked, CanaryService

POLYMARKET_HISTORICAL_JOB_NAME = "polymarket-historical-refresh"
POLYMARKET_AUTONOMY_JOB_NAME = "polymarket-autonomy"
POLYMARKET_AUTONOMY_PROTOCOL_ID = "polymarket-paper-campaign-v1"
POLYMARKET_REPLAY_DATASET_ID = "Polymarket-recorded-book-replay"
POLYMARKET_REPLAY_MAX_ROWS = 10_000


class _HistoricalRequestBudget:
    """Read-only provider facade that enforces one tick's request budget."""

    _NETWORK_METHODS = frozenset({"markets", "market", "market_page", "metadata", "price_history"})

    def __init__(self, provider: Any, budget: int) -> None:
        self._provider = provider
        self._budget = int(budget)
        self.requests = 0
        self.provider_name = getattr(provider, "provider_name", "polymarket")

    def __getattr__(self, name: str) -> Any:
        value = getattr(self._provider, name)
        if name not in self._NETWORK_METHODS or not callable(value):
            return value

        def call(*args: Any, **kwargs: Any) -> Any:
            if self.requests >= self._budget:
                raise RuntimeError("historical request budget exhausted")
            self.requests += 1
            return value(*args, **kwargs)

        return call


EXECUTION_PROFILE_ENV = "AXIOM_EXECUTION_PROFILE"
ISOLATED_EXECUTION_PROFILE = "isolated"
PRODUCTION_EXECUTION_PROFILE = "production"
_VALID_EXECUTION_PROFILES = frozenset(
    {ISOLATED_EXECUTION_PROFILE, PRODUCTION_EXECUTION_PROFILE}
)
_DOTNET_EPOCH_TICKS = 621355968000000000
_FILETIME_EPOCH_TICKS = 504911232000000000
NODE_REVISION = os.environ.get("AXIOM_REVISION", "axiom-node-v1")


def normalized_execution_profile(
    value: Any = None,
    *,
    default: str | None = None,
) -> str | None:
    """Return an exact execution profile, or the explicit missing default.

    ``None`` and the empty string represent an omitted setting.  Every other
    value must be exactly one of the two supported lower-case profiles;
    malformed values raise instead of silently becoming production.  An
    explicit value does not bypass a malformed ambient profile.
    """
    ambient = os.environ.get(EXECUTION_PROFILE_ENV)
    if value is None:
        selected = ambient
    else:
        if ambient not in {None, ""} and ambient not in _VALID_EXECUTION_PROFILES:
            raise ValueError(
                "execution profile must be exactly 'isolated' or 'production'"
            )
        selected = value
    if selected is None or selected == "":
        selected = default
    if selected is None:
        return None
    if not isinstance(selected, str) or selected not in _VALID_EXECUTION_PROFILES:
        raise ValueError(
            "execution profile must be exactly 'isolated' or 'production'"
        )
    return selected


def _process_start_time_ticks(pid: int | None = None) -> int:
    """Return a process creation timestamp in .NET ``DateTime.Ticks``."""
    process_id = int(os.getpid() if pid is None else pid)
    if process_id <= 0:
        raise ValueError("process ID must be positive")
    if os.name == "nt":
        try:
            import ctypes
            from ctypes import wintypes

            kernel32 = ctypes.windll.kernel32
            handle = kernel32.OpenProcess(0x1000, False, process_id)
            if not handle:
                raise OSError(ctypes.get_last_error(), "OpenProcess failed")
            creation = wintypes.FILETIME()
            exit_time = wintypes.FILETIME()
            kernel_time = wintypes.FILETIME()
            user_time = wintypes.FILETIME()
            try:
                if not kernel32.GetProcessTimes(
                    handle,
                    ctypes.byref(creation),
                    ctypes.byref(exit_time),
                    ctypes.byref(kernel_time),
                    ctypes.byref(user_time),
                ):
                    raise OSError(ctypes.get_last_error(), "GetProcessTimes failed")
            finally:
                kernel32.CloseHandle(handle)
            filetime_ticks = (
                (int(creation.dwHighDateTime) << 32)
                | int(creation.dwLowDateTime)
            )
            result = _FILETIME_EPOCH_TICKS + filetime_ticks
            if result <= 0:
                raise OSError("invalid process creation timestamp")
            return result
        except (AttributeError, OSError, TypeError, ValueError) as exc:
            raise RuntimeError("process start time unavailable") from exc

    # Linux provides the process start time as clock ticks since boot.  This
    # keeps local tests and non-Windows deployments truthful without falling
    # back to the current wall clock.
    try:
        stat_text = Path(f"/proc/{process_id}/stat").read_text(encoding="ascii")
        closing_comm = stat_text.rfind(")")
        fields = stat_text[closing_comm + 2 :].split()
        start_ticks = int(fields[19])
        clock_ticks = int(os.sysconf("SC_CLK_TCK"))
        boot_seconds = None
        for line in Path("/proc/stat").read_text(encoding="ascii").splitlines():
            if line.startswith("btime "):
                boot_seconds = int(line.split()[1])
                break
        if boot_seconds is None or clock_ticks <= 0 or start_ticks < 0:
            raise ValueError("missing process start metadata")
        return (
            _DOTNET_EPOCH_TICKS
            + boot_seconds * 10_000_000
            + (start_ticks * 10_000_000) // clock_ticks
        )
    except (OSError, UnicodeError, IndexError, TypeError, ValueError) as exc:
        raise RuntimeError("process start time unavailable") from exc



def _is_real_transport(value: Any) -> bool:
    """Identify repository-owned network adapters while allowing test doubles."""
    if value is None:
        return False
    if isinstance(value, PolymarketAdapter):
        return True
    cls = type(value)
    module = str(getattr(cls, "__module__", "")).casefold()
    name = str(getattr(cls, "__name__", "")).casefold()
    return (
        module in {"axiom.data.polymarket", "axiom.data.binance"}
        or (module.startswith("axiom.") and name in {"binanceadapter", "polymarketadapter"})
    )


def _isolated_venue_factory() -> Any:
    raise CanaryBlocked("ISOLATED_EXECUTION_PROFILE")


def _pid_alive(pid: int) -> bool | None:
    """Return process liveness, preserving inaccessible as an unknown state."""
    if pid <= 0:
        return False
    if os.name == "nt":
        try:
            import ctypes

            kernel32 = ctypes.windll.kernel32
            handle = kernel32.OpenProcess(0x1000, False, pid)
            if handle:
                kernel32.CloseHandle(handle)
                return True
            error_code = int(kernel32.GetLastError())
            if error_code in {6, 87, 1168}:  # invalid handle/parameter/not found
                return False
            return None
        except (AttributeError, OSError):
            return None
    try:
        os.kill(pid, 0)
        return True
    except PermissionError:
        return None
    except OSError as exc:
        if exc.errno == errno.ESRCH:
            return False
        if exc.errno in {errno.EACCES, errno.EPERM}:
            return None
        return None


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


def _pid_matches_node(
    pid: int,
    db_path: str,
    *,
    expected_start_ticks: int | None = None,
) -> bool:
    if _pid_alive(pid) is not True:
        return False
    if expected_start_ticks is not None:
        try:
            if expected_start_ticks <= 0 or _process_start_time_ticks(pid) != expected_start_ticks:
                return False
        except (RuntimeError, TypeError, ValueError):
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
    worker_name: str = "axiom-node"
    lock_path: str | None = None
    pid_path: str | None = None
    log_path: str | None = None
    interval_seconds: float = 60.0
    depth: int = 20
    max_markets: int = 100
    discovery_budget_per_cycle: int = 20
    max_concurrency: int = 1
    freshness_sla_seconds: float | None = None
    max_attempts: int = 3
    max_provider_clock_skew_seconds: float = 5.0
    failure_cooldown_seconds: float = 30.0
    retain_cycles: int = 5
    research_enabled: bool = True
    research_max_items_per_cycle: int = 1
    historical_refresh_enabled: bool = False
    historical_refresh_interval_seconds: float = 3600.0
    historical_refresh_request_budget: int = 25
    historical_refresh_market_budget: int = 4
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
    execution_profile: str | None = None
    max_log_bytes: int = 5_000_000
    backup_count: int = 3
    revision: str | None = None

    def __post_init__(self) -> None:
        db_text = str(self.db_path).strip()
        if db_text not in {":memory:", ""} and not db_text.startswith("file:"):
            object.__setattr__(self, "db_path", os.path.abspath(os.path.expanduser(db_text)))
        if not str(self.db_path).strip():
            raise ValueError("db_path is required")
        profile = normalized_execution_profile(
            self.execution_profile,
            default=PRODUCTION_EXECUTION_PROFILE,
        )
        object.__setattr__(self, "execution_profile", profile)
        revision = str(self.revision or os.environ.get("AXIOM_REVISION") or NODE_REVISION).strip()
        object.__setattr__(self, "revision", revision or NODE_REVISION)
        for field_name in ("lock_path", "log_path", "pid_path"):
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
        pid_identity = path_identity(self.pid_path or f"{self.db_path}.node.pid")
        if len({db_identity, lock_identity, log_identity, pid_identity}) != 4:
            raise ValueError("db_path, lock_path, log_path, and pid_path must be distinct")
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
        if not math.isfinite(cooldown) or cooldown < 0:
            raise ValueError("failure_cooldown_seconds must be finite and non-negative")
        if isinstance(self.max_markets, bool) or not isinstance(self.max_markets, int) or self.max_markets <= 0:
            raise ValueError("max_markets must be a positive integer")
        if isinstance(self.discovery_budget_per_cycle, bool) or not isinstance(self.discovery_budget_per_cycle, int) or self.discovery_budget_per_cycle < 0:
            raise ValueError("discovery_budget_per_cycle must be a non-negative integer")
        if isinstance(self.max_concurrency, bool) or not isinstance(self.max_concurrency, int) or self.max_concurrency not in {1, 2}:
            raise ValueError("max_concurrency must be one or two")
        if self.freshness_sla_seconds is not None:
            freshness = float(self.freshness_sla_seconds)
            if not math.isfinite(freshness) or freshness <= 0:
                raise ValueError("freshness_sla_seconds must be finite and positive")
        if isinstance(self.max_attempts, bool) or not isinstance(self.max_attempts, int) or self.max_attempts <= 0:
            raise ValueError("max_attempts must be a positive integer")
        provider_clock_skew = float(self.max_provider_clock_skew_seconds)
        if not math.isfinite(provider_clock_skew) or provider_clock_skew < 0:
            raise ValueError("max_provider_clock_skew_seconds must be finite and non-negative")
        if isinstance(self.retain_cycles, bool) or not isinstance(self.retain_cycles, int) or self.retain_cycles <= 0:
            raise ValueError("retain_cycles must be a positive integer")
        if isinstance(self.max_log_bytes, bool) or not isinstance(self.max_log_bytes, int) or self.max_log_bytes <= 0:
            raise ValueError("max_log_bytes must be a positive integer")
        if isinstance(self.backup_count, bool) or not isinstance(self.backup_count, int) or self.backup_count < 0:
            raise ValueError("backup_count must be a non-negative integer")
        if not isinstance(self.historical_refresh_enabled, bool):
            raise ValueError("historical_refresh_enabled must be boolean")
        historical_interval = float(self.historical_refresh_interval_seconds)
        if not math.isfinite(historical_interval) or historical_interval <= 0:
            raise ValueError("historical_refresh_interval_seconds must be finite and positive")
        for field_name in (
            "historical_refresh_request_budget",
            "historical_refresh_market_budget",
        ):
            value = getattr(self, field_name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{field_name} must be a non-negative integer")
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
        historical_provider: Any | None = None,
        crypto_provider: Any | None = None,
        opportunity_model: Any | None = None,
        store: AxiomStore | None = None,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self.config = config
        self.execution_profile = normalized_execution_profile(
            config.execution_profile,
            default=PRODUCTION_EXECUTION_PROFILE,
        )
        self.revision = str(config.revision or NODE_REVISION)
        if self.execution_profile == ISOLATED_EXECUTION_PROFILE:
            # The profile is an inherited process boundary, not a dashboard
            # preference.  Reassert it before constructing any provider.
            os.environ[EXECUTION_PROFILE_ENV] = ISOLATED_EXECUTION_PROFILE
        self.store = store if store is not None else AxiomStore(config.db_path)
        self._owns_store = store is None
        selected_provider = provider
        if selected_provider is None:
            selected_provider = (
                SyntheticPredictionProvider()
                if self.execution_profile == ISOLATED_EXECUTION_PROFILE
                else PolymarketAdapter()
            )
        if (
            self.execution_profile == ISOLATED_EXECUTION_PROFILE
            and _is_real_transport(selected_provider)
        ):
            if self._owns_store:
                self.store.close()
            raise RuntimeError(
                "ISOLATED_EXECUTION_PROFILE rejects real prediction transports"
            )
        self.historical_provider: Any | None = None
        if config.historical_refresh_enabled:
            selected_historical_provider = historical_provider
            if selected_historical_provider is None:
                factory = getattr(selected_provider, "isolated_worker_factory", None)
                if callable(factory):
                    candidate = factory()
                    selected_historical_provider = (
                        candidate() if callable(candidate) else candidate
                    )
                elif self.execution_profile == ISOLATED_EXECUTION_PROFILE:
                    selected_historical_provider = SyntheticPredictionProvider()
                else:
                    selected_historical_provider = PolymarketAdapter()
            if (
                self.execution_profile == ISOLATED_EXECUTION_PROFILE
                and _is_real_transport(selected_historical_provider)
            ):
                if self._owns_store:
                    self.store.close()
                raise RuntimeError(
                    "ISOLATED_EXECUTION_PROFILE rejects real historical prediction transports"
                )
            self.historical_provider = selected_historical_provider
        self.provider = selected_provider
        self.opportunity_model = opportunity_model
        self.sleep = sleep
        self.clock = clock
        self._logger: logging.Logger | None = None
        self._handler: RotatingFileHandler | None = None
        self.stop_event = threading.Event()
        self._heartbeat_stop = threading.Event()
        self._heartbeat_thread: threading.Thread | None = None
        self.started_at: datetime | None = None
        self._run_identity = ""
        self._lock_fd: int | None = None
        selected_crypto_provider = crypto_provider if config.crypto_enabled else None
        if self.execution_profile == ISOLATED_EXECUTION_PROFILE and _is_real_transport(selected_crypto_provider):
            if self._owns_store:
                self.store.close()
            raise RuntimeError("ISOLATED_EXECUTION_PROFILE rejects real crypto transports")
        self.crypto_provider = selected_crypto_provider
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
        self._historical_refresh_thread: threading.Thread | None = None
        self._auto_canary_thread: threading.Thread | None = None
        self._historical_thread: threading.Thread | None = None
        self._worker_runtime_lock = threading.RLock()
        self._worker_runtime: dict[str, dict[str, Any]] = {}
        self._worker_restart_counts: dict[str, int] = {}
        self._worker_fatal: set[str] = set()
        self._health_passes = 0
        self._worker_restart_limit = max(1, int(config.max_attempts))
        self._auto_canary_worker = AutonomousCanaryWorker(
            self.store,
            interval_seconds=config.auto_canary_interval_seconds,
            clock=clock,
            venue_factory=(
                _isolated_venue_factory
                if self.execution_profile == ISOLATED_EXECUTION_PROFILE
                else None
            ),
        )
        self._collector_error: str | None = None
        self._research_error: str | None = None
        self._historical_error: str | None = None
        self._historical_passes = 0
        self._historical_fatal = False
        self._run_cycle_base = 0
        self._research_passes = 0
        self._cycles: list[CollectionCycle] = []
        self._collection_count = 0
        self._auto_canary_fatal = False
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
                discovery_budget_per_cycle=config.discovery_budget_per_cycle,
                max_concurrency=config.max_concurrency,
                freshness_sla_seconds=config.freshness_sla_seconds,
                max_attempts=config.max_attempts,
                failure_cooldown_seconds=config.failure_cooldown_seconds,
                max_provider_clock_skew_seconds=config.max_provider_clock_skew_seconds,
                retain_cycles=config.retain_cycles,
            ),
            clock=clock,
            sleep=sleep,
        )
        self.historical_bootstrapper = HistoricalBootstrapper(
            self.store,
            prediction_provider=self.provider,
            clock=clock,
            sleep=sleep,
            max_attempts=config.max_attempts,
            backoff=config.failure_cooldown_seconds,
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
    def pid_path(self) -> Path:
        return Path(self.config.pid_path or (str(self.config.db_path) + ".node.pid"))

    @property
    def process_identity(self) -> str:
        return f"{self.config.worker_name}:{os.getpid()}:{self.revision}:{self._run_identity or 'not-started'}"
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
            # An unreadable marker cannot authorize this process to stop.
            return False
        if not marker:
            return False
        marker_lines = marker.splitlines()
        try:
            marker_pid = int(marker_lines[0].strip())
        except (IndexError, ValueError):
            return False
        if marker_pid <= 0:
            return False
        if marker_pid > 0:
            if marker_pid != os.getpid():
                # A stop marker from another process is stale only when its
                # owner is proven dead.  Access-denied liveness must not erase
                # a marker or permit an unsafe ownership decision.
                if _pid_alive(marker_pid) is False:
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
                return False
        self.stop_event.set()
        return True
    def _worker_payload(self, worker_name: str) -> dict[str, Any]:
        with self._worker_runtime_lock:
            runtime = dict(self._worker_runtime.get(worker_name, {}))
        runtime.update(
            {
                "pid": os.getpid(),
                "process_identity": self.process_identity,
                "revision": self.revision,
                "execution_profile": self.execution_profile,
                "paper_only": True,
                "live_execution": False,
            }
        )
        return runtime

    def _persist_worker_runtime(
        self,
        worker_name: str,
        status: str,
        *,
        extra: Mapping[str, Any] | None = None,
    ) -> None:
        payload = self._worker_payload(worker_name)
        if extra:
            payload.update(dict(extra))
        try:
            self.store.save_worker_state(
                worker_name,
                status,
                payload,
                started_at=self.started_at or ensure_utc(self.clock()),
                heartbeat_at=ensure_utc(self.clock()),
            )
        except Exception as exc:
            self._log(logging.WARNING, "worker state update failed for %s: %s", worker_name, exc)

    def _worker_tick_started(
        self,
        worker_name: str,
        *,
        next_work: Any = None,
        timestamp: datetime | None = None,
    ) -> datetime:
        timestamp_value = ensure_utc(timestamp or self.clock())
        timestamp = timestamp_value.isoformat()
        with self._worker_runtime_lock:
            runtime = self._worker_runtime.setdefault(
                worker_name,
                {
                    "started_at": self.started_at.isoformat() if self.started_at else timestamp,
                    "errors": [],
                    "successful_decision_candidates": [],
                    "successful_markets": [],
                },
            )
            runtime.update(
                {
                    "last_tick_started_at": timestamp,
                    "worker_status": "RUNNING",
                    "next_work": next_work,
                }
            )
        self._persist_worker_runtime(worker_name, "running")
        return timestamp_value

    def _worker_tick_completed(
        self,
        worker_name: str,
        *,
        successful: bool = True,
        successful_candidates: Any = (),
        successful_markets: Any = (),
        decision: str | None = None,
        next_work: Any = None,
        error: BaseException | str | None = None,
        extra: Mapping[str, Any] | None = None,
    ) -> None:
        """Persist a completed tick without turning partial work into success.

        A worker can complete its bounded operation while producing only
        partial/failed output.  ``last_tick_completed_at`` records that fact;
        ``last_successful_tick`` changes only for genuinely successful work,
        while failure counters remain visible for bounded backoff.
        """
        timestamp_value = ensure_utc(self.clock())
        timestamp = timestamp_value.isoformat()

        def bounded_texts(value: Any) -> list[str]:
            if isinstance(value, str):
                values = [value]
            elif isinstance(value, (list, tuple, set, frozenset)):
                values = list(value)
            else:
                values = []
            return [str(item).strip() for item in values if str(item).strip()][:64]

        candidates = bounded_texts(successful_candidates)
        markets = bounded_texts(successful_markets)
        with self._worker_runtime_lock:
            runtime = self._worker_runtime.setdefault(worker_name, {"errors": []})
            runtime["last_tick_completed_at"] = timestamp
            runtime["next_work"] = next_work
            if decision is not None:
                runtime["next_decision"] = decision
            for key, values in (
                ("successful_decision_candidates", candidates),
                ("successful_markets", markets),
            ):
                if values:
                    existing = [*runtime.get(key, []), *values]
                    runtime[key] = list(dict.fromkeys(existing))[-64:]
            if successful:
                runtime.update(
                    {
                        "last_successful_tick": timestamp,
                        "worker_status": "IDLE",
                        "last_error": None,
                        "last_error_code": None,
                        "consecutive_failures": 0,
                        "next_retry_at": None,
                    }
                )
                persisted_status = "idle"
            else:
                failures = int(runtime.get("consecutive_failures", 0) or 0) + 1
                delay = min(30.0, max(0.1, 2.0 ** max(0, failures - 1)))
                if error is not None:
                    message = str(error)
                    runtime["last_error"] = message
                    runtime["last_error_code"] = (
                        type(error).__name__.upper()
                        if not isinstance(error, str)
                        else "WORKER_TICK_PARTIAL"
                    )
                    errors = [*runtime.get("errors", []), message]
                    runtime["errors"] = errors[-32:]
                runtime.update(
                    {
                        "consecutive_failures": failures,
                        "next_retry_at": (
                            timestamp_value + timedelta(seconds=delay)
                        ).isoformat(),
                        "worker_status": "DEGRADED",
                    }
                )
                persisted_status = "degraded"
        self._persist_worker_runtime(worker_name, persisted_status, extra=extra)

    def _worker_tick_failed(
        self,
        worker_name: str,
        exc: BaseException | str,
        *,
        fatal: bool = False,
        next_work: Any = None,
    ) -> None:
        timestamp = ensure_utc(self.clock())
        message = str(exc)
        with self._worker_runtime_lock:
            runtime = self._worker_runtime.setdefault(worker_name, {"errors": []})
            failures = int(runtime.get("consecutive_failures", 0) or 0) + 1
            delay = min(
                30.0,
                max(0.1, 2.0 ** max(0, failures - 1)),
            )
            errors = [*runtime.get("errors", []), message]
            runtime.update(
                {
                    "last_tick_completed_at": timestamp.isoformat(),
                    "last_error": message,
                    "last_error_code": type(exc).__name__.upper()
                    if not isinstance(exc, str)
                    else "WORKER_TICK_FAILED",
                    "errors": errors[-32:],
                    "consecutive_failures": failures,
                    "next_retry_at": (
                        None
                        if fatal
                        else (timestamp + timedelta(seconds=delay)).isoformat()
                    ),
                    "next_work": next_work,
                    "worker_status": "FATAL" if fatal else "DEGRADED",
                }
            )
        self._persist_worker_runtime(worker_name, "fatal" if fatal else "degraded")
    def _publish_autonomous_initializing(self, timestamp: datetime) -> None:
        """Publish a minimal startup state before the first autonomous tick."""
        try:
            service = CanaryService(self.store, clock=self.clock)
            service.upsert_initializing_readiness_snapshot(timestamp=timestamp)
            if not self.config.mutation_enabled:
                service.record_autonomous_decision(
                    next_decision="AUTONOMOUS_CANARY_DISABLED",
                    blocker="AUTONOMOUS_CANARY_DISABLED",
                    signal_id=None,
                    worker_status="DISABLED",
                    timestamp=timestamp,
                    publish=False,
                )
                return
            service.record_autonomous_decision(
                next_decision="INITIALIZING",
                blocker=None,
                signal_id=None,
                worker_status="INITIALIZING",
                timestamp=timestamp,
                publish=False,
            )
        except Exception as exc:
            self._log(logging.WARNING, "autonomous canary initialization state failed: %s", exc)

    def _save_autonomous_worker_state(
        self,
        status: str,
        *,
        decision: str | None = None,
        blocker: str | None = None,
        error_type: str | None = None,
        candidate_id: str | None = None,
        signal_id: str | None = None,
        restart_attempt: int | None = None,
    ) -> None:
        """Persist node-owned autonomous liveness without touching control."""
        if restart_attempt is not None:
            with self._worker_runtime_lock:
                runtime = self._worker_runtime.setdefault(
                    "autonomous-canary",
                    {"errors": []},
                )
                runtime.update(
                    {
                        "restart_count": self._restart_count,
                        "restart_attempt": restart_attempt,
                        "restart_reason": "AUTONOMOUS_THREAD_EXITED",
                    }
                )
        heartbeat = ensure_utc(self.clock())
        payload: dict[str, Any] = {
            **self._worker_payload("autonomous-canary"),
            "pid": os.getpid(),
            "configured_interval_seconds": float(self.config.auto_canary_interval_seconds),
            "autonomous": True,
            "production_live_execution": False,
            "restart_count": self._restart_count,
        }
        if decision is not None:
            payload["decision"] = decision
        if blocker is not None:
            payload["blocker"] = blocker
        if error_type is not None:
            payload["error_type"] = error_type
        if candidate_id is not None:
            payload["candidate_id"] = candidate_id
        if signal_id is not None:
            payload["signal_id"] = signal_id
        if restart_attempt is not None:
            payload["restart_attempt"] = restart_attempt
        if status.lower() == "fatal":
            payload.update({"fatal": True, "requires_attention": True})
        try:
            self.store.save_worker_state(
                "autonomous-canary",
                status,
                payload,
                started_at=self.started_at or heartbeat,
                heartbeat_at=heartbeat,
            )
        except Exception:
            return

    def _persist_autonomous_fatal(self, reason: str) -> None:
        self._auto_canary_fatal = True
        self._save_autonomous_worker_state(
            "fatal",
            decision="AUTONOMOUS_WORKER_FATAL_REVIEW_REQUIRED",
            blocker=reason,
            error_type="AUTONOMOUS_THREAD_EXITED",
        )
        try:
            service = CanaryService(self.store, clock=self.clock)
            service.record_autonomous_decision(
                next_decision="AUTONOMOUS_WORKER_FATAL_REVIEW_REQUIRED",
                blocker=reason,
                worker_status="FATAL",
                timestamp=ensure_utc(self.clock()),
                last_error_code="AUTONOMOUS_THREAD_EXITED",
            )
        except Exception as exc:
            self._log(logging.WARNING, "autonomous fatal state publication failed: %s", exc)

    def run(self, *, max_cycles: int | None = None) -> list[CollectionCycle]:
        if max_cycles is not None and (isinstance(max_cycles, bool) or max_cycles < 0):
            raise ValueError("max_cycles must be non-negative or None")
        self._run_identity = uuid.uuid4().hex
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
        self._historical_error = None
        with self._worker_runtime_lock:
            self._worker_runtime = {}
        self._worker_restart_counts = {}
        self._worker_fatal.clear()
        self._health_passes = 0
        self._cycles.clear()
        self._collection_count = 0
        self._run_cycle_base = 0
        self._research_passes = 0
        self._historical_passes = 0
        self._historical_fatal = False
        self._auto_canary_fatal = False
        cycle_failure = False
        status = "degraded"
        try:
            self.store.save_worker_state(
                self.config.worker_name,
                "running",
                {
                    "pid": os.getpid(),
                    "process_identity": self.process_identity,
                    "revision": self.revision,
                    "execution_profile": self.execution_profile,
                    "db_path": str(self.config.db_path),
                    "lock_path": str(self.lock_path),
                    "log_path": str(self.log_path),
                    "pid_path": str(self.pid_path),
                    "started_at": self.started_at.isoformat(),
                    "paper_only": True,
                    "live_execution": False,
                    "crypto_paper": dict(self._crypto_status),
                },
                started_at=self.started_at,
                heartbeat_at=self.started_at,
            )
            self._publish_autonomous_initializing(self.started_at)
            worker_start_states = {
                "polymarket-collector": {
                    "configured_interval_seconds": float(self.config.interval_seconds),
                    "next_work": "collect_market_cycle",
                },
                "paper-engine": {
                    "candidate_count": 0,
                    "processed_candidates": 0,
                    "remaining_candidates": 0,
                    "next_work": "run_paper_candidates",
                },
                "research-engine": {"next_work": "wait_for_collection"},
                "health-monitor": {
                    "configured_interval_seconds": float(self.config.interval_seconds),
                    "next_work": "wait_for_collection",
                },
                "autonomous-canary": {
                    "configured_interval_seconds": float(self.config.auto_canary_interval_seconds),
                    "autonomous": True,
                    "next_work": (
                        "evaluate_candidates"
                        if self.config.mutation_enabled
                        else "disabled"
                    ),
                    "decision": (
                        None
                        if self.config.mutation_enabled
                        else "AUTONOMOUS_CANARY_DISABLED"
                    ),
                    "blocker": (
                        None
                        if self.config.mutation_enabled
                        else "AUTONOMOUS_CANARY_DISABLED"
                    ),
                    "production_live_execution": False,
                },
            }
            if self.config.historical_refresh_enabled:
                worker_start_states[POLYMARKET_HISTORICAL_JOB_NAME] = {
                    "configured_interval_seconds": float(
                        self.config.historical_refresh_interval_seconds
                    ),
                    "request_budget": self.config.historical_refresh_request_budget,
                    "market_budget": self.config.historical_refresh_market_budget,
                    "replay_row_limit": POLYMARKET_REPLAY_MAX_ROWS,
                    "next_work": "refresh_historical_catalog",
                    "producer_job": POLYMARKET_HISTORICAL_JOB_NAME,
                    "resumable": True,
                }
            for worker_name, payload in worker_start_states.items():
                with self._worker_runtime_lock:
                    self._worker_runtime[worker_name] = {
                        "started_at": self.started_at.isoformat(),
                        "last_tick_started_at": None,
                        "last_tick_completed_at": None,
                        "last_successful_tick": None,
                        "errors": [],
                        "successful_decision_candidates": [],
                        "successful_markets": [],
                        "consecutive_failures": 0,
                        **payload,
                    }
                self._persist_worker_runtime(
                    worker_name,
                    (
                        "disabled"
                        if (
                            worker_name == "autonomous-canary"
                            and not self.config.mutation_enabled
                        )
                        else ("idle" if max_cycles == 0 else "running")
                    ),
                )
            self._start_heartbeat_watchdog()
            if max_cycles != 0:
                self._start_worker_threads(max_cycles)
                while not self.stop_event.is_set():
                    if self._external_stop_requested():
                        break
                    with self._worker_condition:
                        cycle_count = self._collection_count - self._run_cycle_base
                        research_passes = self._research_passes
                        health_passes = self._health_passes
                        complete = bool(
                            max_cycles is not None
                            and cycle_count >= max_cycles
                            and research_passes >= 1
                            and health_passes >= 1
                        )
                        if complete:
                            break
                        self._worker_condition.wait(timeout=0.25)
                    # Poll the owned marker from the responsive coordinator,
                    # not only from a collector cycle whose cadence may be 60s.
                    if self._external_stop_requested():
                        break
                    if not self._supervise_workers(max_cycles=max_cycles):
                        cycle_failure = True
                        break
            elif max_cycles == 0:
                # No child was launched; immediately settle the startup
                # marker so direct bounded invocations cannot leave RUNNING.
                if self.config.mutation_enabled:
                    try:
                        service = CanaryService(self.store, clock=self.clock)
                        service.record_autonomous_decision(
                            next_decision="WAIT_FOR_NEXT_DECISION",
                            blocker=None,
                            worker_status="IDLE",
                            timestamp=ensure_utc(self.clock()),
                            publish=False,
                        )
                    except Exception:
                        pass
            with self._worker_runtime_lock:
                worker_degraded = any(
                    str(runtime.get("worker_status", "")).upper() in {"DEGRADED", "FATAL"}
                    for runtime in self._worker_runtime.values()
                )
            cycle_failure = cycle_failure or self._auto_canary_fatal or self._historical_fatal or worker_degraded or bool(
                self._collector_error or self._research_error or self._historical_error
            )
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
                self._historical_refresh_thread,
                self._auto_canary_thread,
                self._historical_thread,
            ):
                if worker is not None:
                    worker.join()
            self._stop_heartbeat_watchdog()
            self._collector_thread = None
            self._research_thread = None
            self._health_thread = None
            self._historical_refresh_thread = None
            self._auto_canary_thread = None
            self._historical_thread = None
            try:
                self._heartbeat(
                    status,
                    {
                        "cycles": self._collection_count - self._run_cycle_base,
                        "attempts": self._collection_count - self._run_cycle_base,
                        "restart_count": self._restart_count,
                        "crypto_paper": dict(self._crypto_status),
                    },
                )
            except Exception:
                pass
            try:
                marker = self.stop_path.read_text(encoding="ascii").strip()
                owner_marker = self.lock_path.read_text(encoding="ascii").strip()
                if marker and marker == owner_marker:
                    self.stop_path.unlink()
            except (FileNotFoundError, OSError):
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
        self._historical_refresh_thread = None
        if self.config.historical_refresh_enabled:
            self._historical_refresh_thread = threading.Thread(
                target=self._historical_refresh_worker_loop,
                name=f"{self.config.worker_name}-historical-refresh",
                daemon=True,
            )
        self._auto_canary_thread = None
        if self.config.mutation_enabled:
            self._auto_canary_thread = threading.Thread(
                target=self._auto_canary_worker_loop,
                name=f"{self.config.worker_name}-autonomous-canary",
                daemon=True,
            )
        self._historical_thread = self._historical_refresh_thread
        self._collector_thread.start()
        self._research_thread.start()
        self._health_thread.start()
        if self._historical_refresh_thread is not None:
            self._historical_refresh_thread.start()
        if self._auto_canary_thread is not None:
            self._auto_canary_thread.start()
    def _worker_thread_specs(self, max_cycles: int | None) -> dict[str, tuple[str, Callable[[], None]]]:
        specs: dict[str, tuple[str, Callable[[], None]]] = {
            "polymarket-collector": (
                "_collector_thread",
                lambda: self._collector_worker_loop(max_cycles),
            ),
            "research-engine": ("_research_thread", self._research_worker_loop),
            "health-monitor": ("_health_thread", self._health_worker_loop),
        }
        if self.config.historical_refresh_enabled:
            specs[POLYMARKET_HISTORICAL_JOB_NAME] = (
                "_historical_refresh_thread",
                self._historical_refresh_worker_loop,
            )
        if self.config.mutation_enabled:
            specs["autonomous-canary"] = (
                "_auto_canary_thread",
                self._auto_canary_worker_loop,
            )
        return specs
    def _supervise_workers(self, *, max_cycles: int | None) -> bool:
        """Restart unexpectedly exited workers, then fence after bounded retries."""
        for worker_name, (attribute, target) in self._worker_thread_specs(max_cycles).items():
            thread = getattr(self, attribute)
            if thread is not None and thread.is_alive():
                continue
            # A bounded collector exits normally once its requested cycles are
            # complete; it must not be restarted while research/health settle.
            if (
                worker_name == "polymarket-collector"
                and max_cycles is not None
                and self._collection_count - self._run_cycle_base >= max_cycles
            ):
                continue
            if (
                worker_name == POLYMARKET_HISTORICAL_JOB_NAME
                and max_cycles is not None
                and self._historical_passes >= max_cycles
            ):
                continue
            if worker_name == "autonomous-canary" and self._auto_canary_fatal:
                self._worker_fatal.add(worker_name)
                return False
            if worker_name in self._worker_fatal:
                return False
            attempts = int(self._worker_restart_counts.get(worker_name, 0))
            if attempts >= self._worker_restart_limit:
                self._worker_fatal.add(worker_name)
                self._worker_tick_failed(
                    worker_name,
                    "worker thread exited after bounded restart attempts",
                    fatal=True,
                    next_work="operator_review_required",
                )
                if worker_name == "autonomous-canary":
                    self._auto_canary_fatal = True
                if worker_name == POLYMARKET_HISTORICAL_JOB_NAME:
                    self._historical_fatal = True
                self._log(logging.ERROR, "%s exhausted restart attempts", worker_name)
                return False
            attempts += 1
            self._worker_restart_counts[worker_name] = attempts
            self._restart_count += 1
            delay = min(30.0, max(0.1, 2.0 ** (attempts - 1) * 0.1))
            self._worker_tick_failed(
                worker_name,
                "worker thread exited unexpectedly",
                next_work=f"restart_in_{delay:g}s",
            )
            if worker_name == "autonomous-canary":
                self._save_autonomous_worker_state(
                    "degraded",
                    decision="AUTONOMOUS_THREAD_RESTARTING",
                    blocker="AUTONOMOUS_THREAD_EXITED",
                    error_type="AUTONOMOUS_THREAD_EXITED",
                    restart_attempt=attempts,
                )
            if self.stop_event.wait(delay):
                return True
            replacement = threading.Thread(
                target=target,
                name=f"{self.config.worker_name}-{worker_name}",
                daemon=True,
            )
            setattr(self, attribute, replacement)
            replacement.start()
        return True
    def _auto_canary_worker_loop(self) -> None:
        """Run isolated ticks with bounded recovery and truthful boundaries."""
        if not self.config.mutation_enabled:
            self._save_autonomous_worker_state(
                "disabled",
                decision="AUTONOMOUS_CANARY_DISABLED",
                blocker="AUTONOMOUS_CANARY_DISABLED",
            )
            try:
                CanaryService(self.store, clock=self.clock).record_autonomous_decision(
                    next_decision="AUTONOMOUS_CANARY_DISABLED",
                    blocker="AUTONOMOUS_CANARY_DISABLED",
                    worker_status="DISABLED",
                    timestamp=ensure_utc(self.clock()),
                    publish=False,
                )
            except Exception:
                pass
            return

        status = "idle"
        try:
            while not self.stop_event.is_set():
                result: Mapping[str, Any] | None = None
                for attempt in range(self.config.max_attempts):
                    self._worker_tick_started(
                        "autonomous-canary",
                        next_work="evaluate_candidates",
                    )
                    self._save_autonomous_worker_state(
                        "running",
                        decision="EVALUATING_CANDIDATES",
                    )
                    try:
                        candidate_result = self._auto_canary_worker.tick()
                        result = (
                            candidate_result
                            if isinstance(candidate_result, Mapping)
                            else {"status": "ERROR", "error_type": "INVALID_TICK_RESULT"}
                        )
                        if str(result.get("status") or "").upper() == "ERROR":
                            raise RuntimeError(
                                str(result.get("error_type") or result.get("blocker") or "tick returned ERROR")
                            )
                        break
                    except BaseException as exc:
                        self._worker_tick_failed(
                            "autonomous-canary",
                            exc,
                            fatal=attempt + 1 >= self.config.max_attempts,
                            next_work=(
                                "operator_review_required"
                                if attempt + 1 >= self.config.max_attempts
                                else f"retry_{attempt + 1}"
                            ),
                        )
                        self._log(logging.ERROR, "autonomous worker tick failed: %s", exc)
                        if attempt + 1 >= self.config.max_attempts:
                            self._auto_canary_fatal = True
                            self._persist_autonomous_fatal("AUTONOMOUS_WORKER_TICK_EXHAUSTED")
                            status = "fatal"
                            break
                        delay = min(
                            float(self.config.auto_canary_interval_seconds),
                            max(0.1, 2.0**attempt * 0.1),
                        )
                        if self.stop_event.wait(delay):
                            break
                if self._auto_canary_fatal or self.stop_event.is_set():
                    break
                result = result or {}
                result_status = str(result.get("status") or "").upper()
                result_decision = str(result.get("decision") or "").upper()
                result_blocker = str(result.get("blocker") or "").strip()
                tick_failed = (
                    result_decision == "UNKNOWN_NO_RETRY"
                    or result_blocker == "UNKNOWN_NO_RETRY"
                    or result_status == "ERROR"
                    or (
                        result_status == "BLOCKED"
                        and (
                            result_decision == "POSITION_RECONCILIATION_BLOCKED"
                            or result_blocker
                            in {
                                "CANARY_RECONCILIATION_PROVIDER_ERROR",
                                "CANARY_POSITION_RECONCILIATION_FAILED",
                                "CANARY_SUBMISSION_UNKNOWN",
                                "AUTONOMOUS_WORKER_EXCEPTION",
                            }
                        )
                    )
                )
                status = "degraded" if tick_failed else "idle"
                self._worker_tick_completed(
                    "autonomous-canary",
                    successful=not tick_failed,
                    successful_candidates=result.get("candidate_id"),
                    successful_markets=result.get("market_ids", result.get("markets")),
                    decision=str(result.get("decision") or "WAIT_FOR_NEXT_DECISION"),
                    next_work=result.get("next_work"),
                    error=(
                        result_blocker
                        or str(result.get("error_type") or "")
                        if tick_failed
                        else None
                    ),
                    extra={
                        "candidate_id": result.get("candidate_id"),
                        "signal_id": result.get("signal_id"),
                        "decision": result.get("decision"),
                    },
                )
                self._save_autonomous_worker_state(
                    status,
                    decision=str(result.get("decision") or ""),
                    blocker=result.get("blocker"),
                    error_type=result.get("error_type"),
                    candidate_id=result.get("candidate_id"),
                    signal_id=result.get("signal_id"),
                )
                if self.stop_event.wait(self.config.auto_canary_interval_seconds):
                    break
        except BaseException as exc:
            status = "fatal"
            self._auto_canary_fatal = True
            self._worker_tick_failed(
                "autonomous-canary",
                exc,
                fatal=True,
                next_work="operator_review_required",
            )
            self._log(logging.ERROR, "autonomous worker loop failed: %s", exc)
            self._persist_autonomous_fatal("AUTONOMOUS_WORKER_LOOP_EXCEPTION")
        finally:
            if not self._auto_canary_fatal:
                self._save_autonomous_worker_state(
                    "degraded" if status == "degraded" else "idle",
                    decision=(
                        "AUTONOMOUS_WORKER_EXCEPTION"
                        if status == "degraded"
                        else "WAIT_FOR_NEXT_DECISION"
                    ),
                    blocker=(
                        "AUTONOMOUS_WORKER_EXCEPTION"
                        if status == "degraded"
                        else None
                    ),
                )

    def _latest_eligible_historical_catalog(self) -> tuple[dict[str, Any] | None, Mapping[str, Any] | None, str]:
        """Return the newest non-empty, attested immutable Polymarket catalog."""
        try:
            catalogs = self.store.list_dataset_catalog(
                source_type="HISTORICAL",
                market_type="prediction",
                limit=None,
            )
        except TypeError:
            catalogs = self.store.list_dataset_catalog(limit=None)
        if not isinstance(catalogs, (list, tuple)):
            return None, None, "HISTORICAL_CATALOG_UNAVAILABLE"
        for raw in catalogs:
            if not isinstance(raw, Mapping):
                continue
            if str(raw.get("dataset_id", "")).strip() != POLYMARKET_DATASET_ID:
                continue
            version = str(raw.get("dataset_version") or raw.get("version") or "").strip()
            metadata = raw.get("metadata")
            metadata = metadata if isinstance(metadata, Mapping) else {}
            try:
                complete = (
                    bool(version)
                    and version.casefold() not in {"latest", "current", "default", "unversioned"}
                    and str(raw.get("source_type", "")).strip().upper() == "HISTORICAL"
                    and str(raw.get("market_type", "")).strip().lower() == "prediction"
                    and str(raw.get("instrument", "")).strip().upper() == "POLYMARKET"
                    and (
                        not metadata.get("instrument")
                        or str(metadata.get("instrument")).strip().upper() == "POLYMARKET"
                    )
                    and int(raw.get("row_count", 0) or 0) > 0
                    and float(raw.get("completeness", 0.0) or 0.0) >= 1.0
                    and not raw.get("missing_ranges")
                )
            except (TypeError, ValueError, OverflowError):
                complete = False
            if not complete:
                continue
            attestation = self.store.load_dataset_integrity_attestation(
                POLYMARKET_DATASET_ID,
                version,
            )
            if not isinstance(attestation, Mapping):
                verifier = getattr(self.store, "verify_dataset_integrity_attestation", None)
                if callable(verifier):
                    try:
                        attestation = verifier(POLYMARKET_DATASET_ID, version)
                    except Exception:
                        attestation = None
            if (
                isinstance(attestation, Mapping)
                and str(attestation.get("status", "")).upper() == "CURRENT"
                and str(attestation.get("contamination_result", "")).upper() == "PASS"
            ):
                return dict(raw), dict(attestation), "READY"
        return None, None, "WAITING_FOR_CURRENT_HISTORICAL_DATASET"

    @staticmethod
    def _campaign_reassessment_snapshot(
        payload: Mapping[str, Any] | None,
    ) -> tuple[int, frozenset[str], Any, str | None]:
        """Read durable reassessment evidence without inferring an action."""
        if not isinstance(payload, Mapping):
            return 0, frozenset(), None, None
        raw_count = payload.get("reassessment_count", 0)
        try:
            count = (
                0
                if isinstance(raw_count, bool)
                else max(0, int(raw_count))
            )
        except (TypeError, ValueError, OverflowError):
            count = 0
        raw_trials = payload.get("trials")
        trials = raw_trials if isinstance(raw_trials, (list, tuple)) else ()
        trial_ids = frozenset(
            str(item.get("trial_id", "")).strip()
            for item in trials
            if isinstance(item, Mapping) and str(item.get("trial_id", "")).strip()
        )
        evidence = payload.get("reassessment_evidence")
        last_identity = str(payload.get("last_evidence_identity", "")).strip() or None
        return count, trial_ids, evidence, last_identity

    def _start_polymarket_campaign(
        self,
        catalog: Mapping[str, Any],
        attestation: Mapping[str, Any],
        now: datetime,
    ) -> dict[str, Any]:
        """Start one protocol campaign and account only real reassessments."""
        state = self.store.get_scheduler_state(POLYMARKET_AUTONOMY_JOB_NAME) or {}
        state = dict(state) if isinstance(state, Mapping) else {}
        prior_state = dict(state)
        version = str(catalog.get("dataset_version") or catalog.get("version") or "").strip()
        existing_campaign = str(state.get("campaign_id") or "").strip()
        if existing_campaign:
            locked_version = str(state.get("dataset_version") or "").strip()
            last_reassessed_version = str(
                state.get("last_reassessed_dataset_version") or ""
            ).strip()
            attempted_version = str(
                state.get("last_reassessment_attempted_dataset_version") or ""
            ).strip()
            campaign_job_name = self.research_processor.campaign_job_name(existing_campaign)
            campaign_record = self.store.get_operator_job(campaign_job_name)
            campaign_before = (
                campaign_record.get("payload")
                if isinstance(campaign_record, Mapping)
                and isinstance(campaign_record.get("payload"), Mapping)
                else None
            )
            before_count, before_trial_ids, before_evidence, before_identity = (
                self._campaign_reassessment_snapshot(campaign_before)
            )
            reassessment_changed = False
            if (
                version
                and locked_version
                and version != locked_version
                and version != last_reassessed_version
                and version != attempted_version
            ):
                reassess = getattr(self.research_processor, "reassess_campaign", None)
                evidence_identity = str(
                    attestation.get("attestation_hash") or ""
                ).strip()
                if callable(reassess) and evidence_identity and campaign_before is not None:
                    reassess(
                        existing_campaign,
                        evidence_identity=evidence_identity,
                        dataset_id=POLYMARKET_DATASET_ID,
                        dataset_version=version,
                        now=now,
                    )
                    campaign_after_record = self.store.get_operator_job(campaign_job_name)
                    campaign_after = (
                        campaign_after_record.get("payload")
                        if isinstance(campaign_after_record, Mapping)
                        and isinstance(campaign_after_record.get("payload"), Mapping)
                        else None
                    )
                    (
                        after_count,
                        after_trial_ids,
                        after_evidence,
                        after_identity,
                    ) = self._campaign_reassessment_snapshot(campaign_after)
                    new_reassessment_trial_ids = {
                        trial_id
                        for trial_id in after_trial_ids - before_trial_ids
                        if trial_id.endswith(":reassessment-1")
                    }
                    evidence_changed = (
                        after_evidence != before_evidence
                        or after_identity != before_identity
                    )
                    reassessment_changed = (
                        after_count == before_count + 1
                        and after_count <= 1
                        and bool(new_reassessment_trial_ids)
                        and (
                            evidence_changed
                            or (
                                before_evidence is None
                                and after_evidence is None
                                and before_identity is None
                                and after_identity is None
                            )
                        )
                    )
                    if reassessment_changed:
                        state["last_reassessed_dataset_version"] = version
                        state["reassessment_count"] = after_count
                        state["reassessment"] = dict(campaign_after or {})
                    else:
                        # A processor no-op is not a consumed reassessment.
                        # Remember the attempted dataset only to suppress the
                        # same durable no-op on the next scheduler tick.
                        state["last_reassessment_attempted_dataset_version"] = version
            campaign_after_record = self.store.get_operator_job(campaign_job_name)
            campaign_payload = (
                campaign_after_record.get("payload")
                if isinstance(campaign_after_record, Mapping)
                and isinstance(campaign_after_record.get("payload"), Mapping)
                else campaign_before
            )
            campaign_payload = (
                dict(campaign_payload)
                if isinstance(campaign_payload, Mapping)
                else {}
            )
            campaign_status = (
                str(campaign_payload.get("status") or state.get("status") or "RUNNING")
                .strip()
                .upper()
            )
            state.update(
                {
                    "protocol_id": POLYMARKET_AUTONOMY_PROTOCOL_ID,
                    "dataset_id": POLYMARKET_DATASET_ID,
                    "latest_dataset_version": version,
                    "latest_attestation_hash": attestation.get("attestation_hash"),
                    "campaign": campaign_payload,
                    "campaign_job_status": campaign_status,
                    "status": campaign_status,
                    "next_work": (
                        (
                            "process_finite_campaign"
                            if not (
                                str(
                                    state.get(
                                        "last_reassessment_attempted_dataset_version",
                                        "",
                                    )
                                    or ""
                                ).strip()
                                == version
                            )
                            else (
                                "wait_for_campaign_data"
                                if campaign_status == "WAITING_FOR_DATA"
                                else "wait_for_campaign_state"
                            )
                        )
                        if campaign_status == "RUNNING"
                        else (
                            "wait_for_campaign_data"
                            if campaign_status == "WAITING_FOR_DATA"
                            else "wait_for_campaign_state"
                        )
                    ),
                    "paper_only": True,
                    "live_execution": False,
                }
            )
            if state != prior_state:
                state["updated_at"] = now.isoformat()
                self.store.set_scheduler_state(POLYMARKET_AUTONOMY_JOB_NAME, state)
            return state
        stable_campaign_id = f"{POLYMARKET_AUTONOMY_PROTOCOL_ID}:campaign"
        campaign = self.research_processor.start_polymarket_campaign(
            stable_campaign_id,
            dataset_id=POLYMARKET_DATASET_ID,
            dataset_version=version,
            now=now,
        )
        campaign_payload = dict(campaign) if isinstance(campaign, Mapping) else {}
        campaign_status = str(campaign_payload.get("status") or "UNKNOWN").upper()
        campaign_reassessment_count, _, _, _ = self._campaign_reassessment_snapshot(
            campaign_payload
        )
        campaign_job_name = self.research_processor.campaign_job_name(stable_campaign_id)
        queued_items = len(
            self.research_processor.bus.list_campaign_trials(
                stable_campaign_id,
                limit=10_000,
            )
        )
        state.update(
            {
                "protocol_id": POLYMARKET_AUTONOMY_PROTOCOL_ID,
                "campaign_id": stable_campaign_id,
                "campaign_job_name": campaign_job_name,
                "campaign_job_status": campaign_status,
                "campaign": campaign_payload,
                "dataset_id": POLYMARKET_DATASET_ID,
                "dataset_version": version,
                "latest_dataset_version": version,
                "attestation_hash": attestation.get("attestation_hash"),
                "reassessment_count": min(1, campaign_reassessment_count),
                "status": campaign_status,
                "queued_items": queued_items,
                "next_work": (
                    "process_finite_campaign"
                    if campaign_status == "RUNNING"
                    else "wait_for_campaign_state"
                ),
                "updated_at": now.isoformat(),
                "paper_only": True,
                "live_execution": False,
            }
        )
        self.store.set_scheduler_state(POLYMARKET_AUTONOMY_JOB_NAME, state)
        return state

    def _run_historical_refresh(
        self,
        *,
        cutoff: datetime | None = None,
    ) -> dict[str, Any]:
        """Refresh public history and then seed only an eligible finite campaign."""
        now = ensure_utc(cutoff or self.clock())
        if (
            self.config.historical_refresh_request_budget <= 0
            or self.config.historical_refresh_market_budget <= 0
            or self.historical_provider is None
        ):
            state = self.store.get_scheduler_state(POLYMARKET_AUTONOMY_JOB_NAME) or {}
            state = dict(state) if isinstance(state, Mapping) else {}
            state.update(
                {
                    "protocol_id": POLYMARKET_AUTONOMY_PROTOCOL_ID,
                    "status": "WAITING_FOR_BUDGET",
                    "next_work": "wait_for_positive_refresh_budget",
                    "updated_at": now.isoformat(),
                    "paper_only": True,
                    "live_execution": False,
                }
            )
            self.store.set_scheduler_state(POLYMARKET_AUTONOMY_JOB_NAME, state)
            return {"status": "WAITING", "reason": "WAITING_FOR_BUDGET"}
        provider = _HistoricalRequestBudget(
            self.historical_provider,
            self.config.historical_refresh_request_budget,
        )
        bootstrapper = HistoricalBootstrapper(
            self.store,
            prediction_provider=provider,
            sleep=self.sleep,
            clock=self.clock,
            max_attempts=self.config.max_attempts,
            backoff=0.0,
        )
        report = bootstrapper.bootstrap_polymarket(
            max_markets=self.config.historical_refresh_market_budget,
            request_budget=self.config.historical_refresh_request_budget,
            resume=True,
        )
        report_record = report.as_record() if hasattr(report, "as_record") else dict(report)
        catalog, attestation, reason = self._latest_eligible_historical_catalog()
        if catalog is not None and attestation is not None:
            state = self._start_polymarket_campaign(catalog, attestation, now)
        else:
            state = self.store.get_scheduler_state(POLYMARKET_AUTONOMY_JOB_NAME) or {}
            state = dict(state) if isinstance(state, Mapping) else {}
            state.update(
                {
                    "protocol_id": POLYMARKET_AUTONOMY_PROTOCOL_ID,
                    "status": "WAITING_FOR_ELIGIBLE_CATALOG",
                    "next_work": "wait_for_current_pass_historical_catalog",
                    "reason": reason,
                    "updated_at": now.isoformat(),
                    "paper_only": True,
                    "live_execution": False,
                }
            )
            self.store.set_scheduler_state(POLYMARKET_AUTONOMY_JOB_NAME, state)
        return {
            "status": str(report_record.get("status") or "UNKNOWN"),
            "report": report_record,
            "requests": provider.requests,
            "campaign": state,
        }

    def _historical_worker_loop(self, max_cycles: int | None) -> None:
        completed = 0
        next_due = time.monotonic()
        next_scheduled = ensure_utc(self.clock())
        try:
            self._persist_worker_runtime(
                POLYMARKET_HISTORICAL_JOB_NAME,
                "running",
                extra={
                    "configured_interval_seconds": float(
                        self.config.historical_refresh_interval_seconds
                    ),
                    "request_budget": self.config.historical_refresh_request_budget,
                    "market_budget": self.config.historical_refresh_market_budget,
                    "next_work": "refresh_historical_catalog",
                },
            )
            while not self.stop_event.is_set() and (
                max_cycles is None or completed < max_cycles
            ):
                self._worker_tick_started(
                    POLYMARKET_HISTORICAL_JOB_NAME,
                    next_work=next_scheduled.isoformat(),
                )
                try:
                    result = self._run_historical_refresh()
                    self._historical_error = None
                    self._worker_tick_completed(
                        POLYMARKET_HISTORICAL_JOB_NAME,
                        successful=True,
                        decision=str(result.get("status") or "HISTORICAL_REFRESH_COMPLETE"),
                        next_work="wait_for_next_historical_refresh",
                        extra=result,
                    )
                except BaseException as exc:
                    self._historical_error = str(exc)
                    self._worker_tick_failed(
                        POLYMARKET_HISTORICAL_JOB_NAME,
                        exc,
                        fatal=False,
                        next_work="retry_historical_refresh",
                    )
                completed += 1
                self._historical_passes += 1
                if max_cycles is not None and completed >= max_cycles:
                    break
                next_due += float(self.config.historical_refresh_interval_seconds)
                delay = max(0.0, next_due - time.monotonic())
                if self.stop_event.wait(delay):
                    break
                next_scheduled = ensure_utc(self.clock()) + timedelta(seconds=delay)
        finally:
            try:
                self._persist_worker_runtime(
                    POLYMARKET_HISTORICAL_JOB_NAME,
                    "stopped" if self.stop_event.is_set() else (
                        "degraded" if self._historical_error else "idle"
                    ),
                    extra={"next_work": "stopped"},
                )
            except Exception:
                pass
            with self._worker_condition:
                self._worker_condition.notify_all()

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
            **self._worker_payload("polymarket-collector"),
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
            payload["last_error"] = error
        self.store.save_worker_state(
            "polymarket-collector",
            status,
            payload,
            started_at=cycle.started_at if cycle is not None else ensure_utc(self.clock()),
            heartbeat_at=ensure_utc(self.clock()),
        )

    def _persist_polymarket_replay_state(
        self,
        replay: Mapping[str, Any],
        *,
        cutoff: datetime,
    ) -> None:
        state = self.store.get_scheduler_state(POLYMARKET_AUTONOMY_JOB_NAME) or {}
        state = dict(state) if isinstance(state, Mapping) else {}
        replay_payload = dict(replay)
        state.update(
            {
                "protocol_id": state.get(
                    "protocol_id",
                    POLYMARKET_AUTONOMY_PROTOCOL_ID,
                ),
                "replay": replay_payload,
                "replay_dataset_id": replay_payload.get("dataset_id"),
                "replay_dataset_version": replay_payload.get("dataset_version"),
                "replay_row_count": replay_payload.get("row_count", 0),
                "replay_cutoff": replay_payload.get("cutoff"),
                "replay_manifest": replay_payload.get("snapshot_manifest", []),
                "replay_status": replay_payload.get("status"),
                "replay_error": replay_payload.get("error"),
                "updated_at": cutoff.isoformat(),
                "paper_only": True,
                "live_execution": False,
            }
        )
        self.store.set_scheduler_state(POLYMARKET_AUTONOMY_JOB_NAME, state)

    def _publish_polymarket_forward_replay(self, cutoff: datetime) -> dict[str, Any]:
        """Publish a bounded, immutable replay without touching the network."""
        prior_state = self.store.get_scheduler_state(POLYMARKET_AUTONOMY_JOB_NAME) or {}
        prior_state = dict(prior_state) if isinstance(prior_state, Mapping) else {}
        prior_replay = prior_state.get("replay")
        cutoff_value = ensure_utc(cutoff)
        cutoff_text = cutoff_value.isoformat()
        existing_versions: set[str] = set()
        try:
            existing_catalogs = self.store.list_dataset_catalog(
                source_type="FORWARD_COLLECTED",
                limit=256,
            )
            for catalog in existing_catalogs:
                if not isinstance(catalog, Mapping):
                    continue
                if str(catalog.get("dataset_id") or "").strip() != POLYMARKET_REPLAY_DATASET_ID:
                    continue
                metadata = catalog.get("metadata")
                if (
                    isinstance(metadata, Mapping)
                    and str(metadata.get("exact_cutoff") or "").strip() == cutoff_text
                ):
                    version = str(catalog.get("dataset_version") or "").strip()
                    if version:
                        existing_versions.add(version)
        except Exception:
            existing_versions = set()
        try:
            raw_result = self.historical_bootstrapper.publish_polymarket_forward_replay(
                cutoff=cutoff_value,
                dataset_id=POLYMARKET_REPLAY_DATASET_ID,
                max_rows=POLYMARKET_REPLAY_MAX_ROWS,
            )
            if not isinstance(raw_result, Mapping):
                raise TypeError("forward replay publisher returned a non-mapping result")
            replay = dict(raw_result)
            replay["dataset_id"] = str(
                replay.get("dataset_id") or POLYMARKET_REPLAY_DATASET_ID
            )
            replay["cutoff"] = cutoff_text
            replay["exact_cutoff"] = cutoff_text
            replay["research_mode"] = "RECORDED_BOOK_REPLAY"
            replay["source_type"] = "FORWARD_COLLECTED"
            replay["query_limit"] = POLYMARKET_REPLAY_MAX_ROWS
            try:
                row_count = int(replay.get("row_count", 0) or 0)
            except (TypeError, ValueError, OverflowError) as exc:
                raise ValueError("forward replay publisher returned an invalid row count") from exc
            if row_count < 0:
                raise ValueError("forward replay publisher returned a negative row count")
            replay["row_count"] = row_count
            manifest = replay.get("snapshot_manifest", [])
            if isinstance(manifest, (list, tuple)):
                normalized_manifest: list[Any] = []
                for item in manifest:
                    if not isinstance(item, Mapping):
                        normalized_manifest.append(item)
                        continue
                    entry = dict(item)
                    source_stamp = parse_timestamp(entry.get("source_timestamp"))
                    if source_stamp is not None:
                        entry["source_timestamp"] = source_stamp.isoformat()
                    normalized_manifest.append(entry)
                replay["snapshot_manifest"] = normalized_manifest
            else:
                replay["snapshot_manifest"] = []
            prior_version = (
                str(prior_replay.get("dataset_version") or "").strip()
                if isinstance(prior_replay, Mapping)
                else ""
            )
            prior_cutoff = (
                str(prior_replay.get("cutoff") or "").strip()
                if isinstance(prior_replay, Mapping)
                else ""
            )
            prior_manifest = (
                prior_replay.get("snapshot_manifest", [])
                if isinstance(prior_replay, Mapping)
                else []
            )
            current_version = str(replay.get("dataset_version") or "").strip()
            same_content = (
                row_count > 0
                and bool(current_version)
                and (
                    current_version in existing_versions
                    or (
                        isinstance(prior_replay, Mapping)
                        and prior_version == current_version
                        and prior_cutoff == cutoff_text
                        and prior_replay.get("row_count") == row_count
                        and prior_manifest == replay["snapshot_manifest"]
                    )
                )
            )
            replay["status"] = (
                "EMPTY" if row_count == 0 else ("NO_NEW_DATA" if same_content else "PUBLISHED")
            )
        except BaseException as exc:
            replay = {
                "status": "FAILED",
                "dataset_id": POLYMARKET_REPLAY_DATASET_ID,
                "dataset_version": None,
                "row_count": 0,
                "cutoff": cutoff_text,
                "exact_cutoff": cutoff_text,
                "snapshot_manifest": [],
                "research_mode": "RECORDED_BOOK_REPLAY",
                "source_type": "FORWARD_COLLECTED",
                "query_limit": POLYMARKET_REPLAY_MAX_ROWS,
                "error": str(exc),
                "error_type": type(exc).__name__,
            }
        try:
            self._persist_polymarket_replay_state(replay, cutoff=cutoff_value)
        except BaseException as exc:
            replay = {
                **replay,
                "status": "FAILED",
                "error": str(exc),
                "error_type": type(exc).__name__,
            }
        return replay

    def _run_historical_refresh_tick(
        self,
        *,
        cutoff: datetime | None = None,
    ) -> None:
        """Run one bounded historical tick and its persisted replay publication."""
        if not self.config.historical_refresh_enabled:
            return
        if cutoff is None:
            with self._worker_runtime_lock:
                started_text = (
                    self._worker_runtime.get(POLYMARKET_HISTORICAL_JOB_NAME, {})
                    .get("last_tick_started_at")
                )
            cutoff = parse_timestamp(started_text) if started_text else None
        tick_cutoff = ensure_utc(cutoff or self.clock())
        historical_error: BaseException | None = None
        try:
            if self.historical_provider is not None:
                autonomy_result = self._run_historical_refresh(cutoff=tick_cutoff)
                report_record = dict(autonomy_result.get("report") or {})
                report_record.setdefault("status", autonomy_result.get("status"))
                report_record["requests"] = autonomy_result.get("requests", 0)
                report_record["campaign"] = autonomy_result.get("campaign")
            else:
                report = self.historical_bootstrapper.bootstrap_polymarket(
                    max_markets=self.config.max_markets,
                    market_budget=self.config.historical_refresh_market_budget,
                    request_budget=self.config.historical_refresh_request_budget,
                    resume=True,
                )
                report_record = report.as_record()
        except BaseException as exc:
            historical_error = exc
            report_record = {
                "status": "FAILED",
                "errors": [str(exc)],
                "error_type": type(exc).__name__,
            }
        producer_status = str(report_record.get("status") or "").upper()
        if historical_error is not None:
            producer_status = "FAILED"
        if producer_status in {"SCHEDULED", "RUNNING"}:
            worker_status = "waiting"
            wait_classification = {
                "status": "WAITING_FOR_DATA",
                "producer_job": POLYMARKET_HISTORICAL_JOB_NAME,
                "producer_status": producer_status,
            }
        elif producer_status == "EXHAUSTED":
            worker_status = "degraded"
            wait_classification = {
                "status": "WAITING_FOR_DATA",
                "producer_job": POLYMARKET_HISTORICAL_JOB_NAME,
                "producer_status": producer_status,
                "exhausted": True,
            }
        elif producer_status == "FAILED":
            worker_status = "degraded"
            wait_classification = {
                "status": "SOFTWARE_OR_INPUT_ERROR",
                "producer_job": POLYMARKET_HISTORICAL_JOB_NAME,
                "producer_status": producer_status,
            }
        else:
            worker_status = "idle"
            wait_classification = {
                "status": producer_status or "NO_NEW_DATA",
                "producer_job": POLYMARKET_HISTORICAL_JOB_NAME,
            }
        replay = self._publish_polymarket_forward_replay(tick_cutoff)
        replay_status = str(replay.get("status") or "").upper()
        if replay_status == "FAILED":
            worker_status = "degraded"
        manifest = replay.get("snapshot_manifest", [])
        if isinstance(manifest, (list, tuple)):
            normalized_manifest: list[Any] = []
            for item in manifest:
                if not isinstance(item, Mapping):
                    normalized_manifest.append(item)
                    continue
                entry = dict(item)
                source_stamp = parse_timestamp(entry.get("source_timestamp"))
                if source_stamp is not None:
                    entry["source_timestamp"] = source_stamp.isoformat()
                normalized_manifest.append(entry)
            replay["snapshot_manifest"] = normalized_manifest
        else:
            replay["snapshot_manifest"] = []
        report_record["replay"] = replay
        historical_error_text = (
            str(historical_error)
            if historical_error is not None
            else (
                str(report_record.get("error"))
                if report_record.get("error")
                else None
            )
        )
        replay_error = str(replay.get("error")) if replay.get("error") else None
        last_report_errors = report_record.get("errors", [])
        last_error = (
            list(last_report_errors)[:1]
            if isinstance(last_report_errors, (list, tuple))
            else ([historical_error_text] if historical_error_text else [])
        )
        next_work = (
            "retry_replay_publication"
            if replay_status == "FAILED"
            else "wait_for_next_historical_refresh"
        )
        with self._worker_runtime_lock:
            runtime = self._worker_runtime.setdefault(
                POLYMARKET_HISTORICAL_JOB_NAME,
                {"errors": []},
            )
            runtime.update(
                {
                    "job_name": POLYMARKET_HISTORICAL_JOB_NAME,
                    "job_status": producer_status,
                    "wait_classification": wait_classification,
                    "last_report": report_record,
                    "last_error": last_error,
                    "historical_error": historical_error_text,
                    "replay": replay,
                    "replay_status": replay_status,
                    "replay_error": replay_error,
                    "replay_dataset_id": replay.get("dataset_id"),
                    "replay_dataset_version": replay.get("dataset_version"),
                    "replay_row_count": replay.get("row_count", 0),
                    "replay_cutoff": replay.get("cutoff"),
                    "replay_manifest": replay.get("snapshot_manifest", []),
                }
            )
        self._persist_worker_runtime(
            POLYMARKET_HISTORICAL_JOB_NAME,
            worker_status,
            extra={
                "job_name": POLYMARKET_HISTORICAL_JOB_NAME,
                "job_status": producer_status,
                "wait_classification": wait_classification,
                "last_report": report_record,
                "last_error": last_error,
                "historical_error": historical_error_text,
                "replay": replay,
                "replay_status": replay_status,
                "replay_error": replay_error,
                "replay_dataset_id": replay.get("dataset_id"),
                "replay_dataset_version": replay.get("dataset_version"),
                "replay_row_count": replay.get("row_count", 0),
                "replay_cutoff": replay.get("cutoff"),
                "replay_manifest": replay.get("snapshot_manifest", []),
                "next_work": next_work,
            },
        )

    def _historical_refresh_worker_loop(self) -> None:
        """Schedule bounded historical refresh ticks independently."""
        interval_seconds = max(0.1, float(self.config.historical_refresh_interval_seconds))
        status = "idle"
        try:
            self._persist_worker_runtime(
                POLYMARKET_HISTORICAL_JOB_NAME,
                "running",
                extra={
                    "configured_interval_seconds": interval_seconds,
                    "replay_row_limit": POLYMARKET_REPLAY_MAX_ROWS,
                    "next_work": "refresh_historical_data",
                    "producer_job": POLYMARKET_HISTORICAL_JOB_NAME,
                    "resumable": True,
                },
            )
            while not self.stop_event.is_set():
                if self._external_stop_requested():
                    break
                self._worker_tick_started(
                    POLYMARKET_HISTORICAL_JOB_NAME,
                    next_work="refresh_historical_data",
                )
                self._run_historical_refresh_tick()
                if self.stop_event.is_set():
                    break
                # Schedule from completion, rather than catching up missed
                # slots, so a slow producer can never spin or starve peers.
                if self.stop_event.wait(interval_seconds):
                    break
        except BaseException as exc:
            status = "degraded"
            self._worker_tick_failed(
                POLYMARKET_HISTORICAL_JOB_NAME,
                exc,
                fatal=False,
                next_work="operator_review_required",
            )
            self._log(logging.ERROR, "historical refresh worker failed: %s", exc)
        finally:
            try:
                self._persist_worker_runtime(
                    POLYMARKET_HISTORICAL_JOB_NAME,
                    "stopped" if self.stop_event.is_set() else status,
                    extra={"next_work": "stopped"},
                )
            except Exception:
                pass
            with self._worker_condition:
                self._worker_condition.notify_all()

    def _collector_worker_loop(self, max_cycles: int | None) -> None:
        collector, owned_store = self._collector_for_worker()
        completed = 0
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
                cycle: CollectionCycle | None = None
                for attempt in range(self.config.max_attempts):
                    self._worker_tick_started(
                        "polymarket-collector",
                        next_work=scheduled_for.isoformat(),
                    )
                    try:
                        candidate_cycle = collector.collect_once()
                        if not isinstance(candidate_cycle, CollectionCycle):
                            raise RuntimeError("collector returned an invalid cycle")
                        cycle = candidate_cycle
                        break
                    except BaseException as exc:
                        self._collector_error = str(exc)
                        self._worker_tick_failed(
                            "polymarket-collector",
                            exc,
                            fatal=attempt + 1 >= self.config.max_attempts,
                            next_work=(
                                "operator_review_required"
                                if attempt + 1 >= self.config.max_attempts
                                else f"retry_{attempt + 1}"
                            ),
                        )
                        self._log(logging.ERROR, "collector worker cycle failed: %s", exc)
                        if attempt + 1 >= self.config.max_attempts:
                            break
                        delay = min(
                            float(self.config.failure_cooldown_seconds),
                            max(0.1, 2.0**attempt * 0.1),
                        )
                        if self.stop_event.wait(delay):
                            break
                if cycle is None:
                    break
                cycle_record = cycle.as_record()
                cycle_failures = sum(
                    int(cycle_record.get(field, 0) or 0)
                    for field in (
                        "markets_failed",
                        "errors",
                        "provider_failures",
                        "metadata_failures",
                        "order_book_failures",
                        "trade_failures",
                    )
                )
                cycle_degraded = cycle_failures > 0
                cycle_error = (
                    f"collection cycle completed with {cycle_failures} recorded failures"
                    if cycle_degraded
                    else None
                )
                self._collector_error = cycle_error
                with self._worker_condition:
                    self._cycles.append(cycle)
                    self._collection_count += 1
                    if len(self._cycles) > self._run_cycle_base + self.config.retain_cycles:
                        del self._cycles[: -(self.config.retain_cycles)]
                    completed += 1
                    self._worker_condition.notify_all()
                next_scheduled = cycle.started_at + timedelta(seconds=float(self.config.interval_seconds))
                self._update_collector_schedule(schedule_store, next_scheduled)
                self._worker_tick_completed(
                    "polymarket-collector",
                    successful=not cycle_degraded,
                    successful_markets=cycle_record.get("candidate_bound_scheduled", ()),
                    next_work=next_scheduled.isoformat(),
                    error=cycle_error,
                    extra={
                        "last_cycle": cycle_record,
                        "last_cycle_started_at": cycle.started_at.isoformat(),
                        "last_cycle_ended_at": cycle.ended_at.isoformat(),
                        "last_cycle_duration_seconds": cycle.duration_seconds,
                        "last_cycle_markets_attempted": cycle.markets_attempted,
                        "last_cycle_markets_successful": cycle.markets_successful,
                        "last_cycle_markets_failed": cycle.markets_failed,
                        "last_successful_collection_at": (
                            cycle.ended_at.isoformat()
                            if cycle.markets_successful > 0
                            else None
                        ),
                    },
                )
                self._save_collector_worker_state(
                    "degraded" if cycle_degraded else "running",
                    next_scheduled=next_scheduled,
                    cycle=cycle,
                    error=cycle_error,
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
        first_cycle = self._collection_count
        try:
            self._persist_worker_runtime(
                "research-engine",
                "running",
                extra={"next_work": "wait_for_collection"},
            )
            while not self.stop_event.is_set():
                with self._worker_condition:
                    while (
                        not self.stop_event.is_set()
                        and self._collection_count <= first_cycle
                        and (self._collector_thread is None or self._collector_thread.is_alive())
                    ):
                        self._worker_condition.wait(timeout=0.5)
                    if self.stop_event.is_set():
                        break
                    if (
                        self._collection_count <= first_cycle
                        and self._collector_thread is not None
                        and not self._collector_thread.is_alive()
                    ):
                        self._research_error = "collector produced no completed cycle"
                        self._worker_tick_failed(
                            "research-engine",
                            self._research_error,
                            fatal=True,
                            next_work="operator_review_required",
                        )
                        break
                started = ensure_utc(self.clock())
                cycle_stats: Mapping[str, Any] | None = None
                for attempt in range(self.config.max_attempts):
                    self._worker_tick_started(
                        "research-engine",
                        next_work="run_deterministic_research",
                    )
                    try:
                        candidate_stats = self._run_research_cycle()
                        cycle_stats = (
                            candidate_stats
                            if isinstance(candidate_stats, Mapping)
                            else {"result": candidate_stats}
                        )
                        break
                    except BaseException as exc:
                        self._research_error = str(exc)
                        self._worker_tick_failed(
                            "research-engine",
                            exc,
                            fatal=attempt + 1 >= self.config.max_attempts,
                            next_work=(
                                "operator_review_required"
                                if attempt + 1 >= self.config.max_attempts
                                else f"retry_{attempt + 1}"
                            ),
                        )
                        self._log(logging.ERROR, "research worker cycle failed: %s", exc)
                        if attempt + 1 >= self.config.max_attempts:
                            break
                        delay = min(
                            float(self.config.failure_cooldown_seconds),
                            max(0.1, 2.0**attempt * 0.1),
                        )
                        if self.stop_event.wait(delay):
                            break
                if cycle_stats is None:
                    break
                research_degraded = bool(cycle_stats.get("degraded"))
                research_error = (
                    "; ".join(str(item) for item in cycle_stats.get("errors", ()) if str(item).strip())
                    if research_degraded and isinstance(cycle_stats.get("errors"), (list, tuple))
                    else None
                )
                self._research_error = research_error if research_degraded else None
                with self._worker_condition:
                    self._research_passes += 1
                    research_passes = self._research_passes
                    self._worker_condition.notify_all()
                queue_cycle = cycle_stats.get("research_queue")
                queue_items_processed = (
                    int(queue_cycle.get("claimed", 0))
                    if isinstance(queue_cycle, Mapping)
                    else 0
                )
                paper_cycle = cycle_stats.get("paper")
                processed_candidates = (
                    paper_cycle.get("processed_candidate_ids", ())
                    if isinstance(paper_cycle, Mapping)
                    else ()
                )
                self._worker_tick_completed(
                    "research-engine",
                    successful=not research_degraded,
                    successful_candidates=processed_candidates,
                    decision="RESEARCH_CYCLE_COMPLETE",
                    next_work="wait_for_collection",
                    error=research_error,
                    extra={
                        "cycle": dict(cycle_stats),
                        "passes": research_passes,
                        "queue_items_processed": queue_items_processed,
                        "cycle_started_at": started.isoformat(),
                        "cycle_ended_at": ensure_utc(self.clock()).isoformat(),
                    },
                )
                if self.stop_event.wait(1.0):
                    break
                first_cycle = self._collection_count
        finally:
            try:
                self._persist_worker_runtime(
                    "research-engine",
                    "stopped" if self.stop_event.is_set() else "degraded",
                    extra={
                        "error": self._research_error,
                        "passes": self._research_passes,
                        "next_work": "stopped",
                    },
                )
            except Exception:
                pass
            with self._worker_condition:
                self._worker_condition.notify_all()

    def _health_worker_loop(self) -> None:
        seen = self._collection_count
        try:
            self._persist_worker_runtime(
                "health-monitor",
                "running",
                extra={"next_work": "wait_for_collection"},
            )
            while not self.stop_event.is_set():
                with self._worker_condition:
                    while (
                        not self.stop_event.is_set()
                        and self._collection_count <= seen
                        and (self._collector_thread is None or self._collector_thread.is_alive())
                    ):
                        self._worker_condition.wait(timeout=0.5)
                    if self.stop_event.is_set():
                        break
                    if (
                        self._collection_count <= seen
                        and self._collector_thread is not None
                        and not self._collector_thread.is_alive()
                    ):
                        break
                    seen = self._collection_count
                success = False
                for attempt in range(self.config.max_attempts):
                    self._worker_tick_started(
                        "health-monitor",
                        next_work="evaluate_collection_health",
                    )
                    try:
                        success = self._run_health_monitor() is not False
                        if success:
                            break
                        raise RuntimeError("health monitor returned failure")
                    except BaseException as exc:
                        self._worker_tick_failed(
                            "health-monitor",
                            exc,
                            fatal=attempt + 1 >= self.config.max_attempts,
                            next_work=(
                                "operator_review_required"
                                if attempt + 1 >= self.config.max_attempts
                                else f"retry_{attempt + 1}"
                            ),
                        )
                        self._log(logging.ERROR, "health worker cycle failed: %s", exc)
                        if attempt + 1 >= self.config.max_attempts:
                            break
                        delay = min(
                            float(self.config.failure_cooldown_seconds),
                            max(0.1, 2.0**attempt * 0.1),
                        )
                        if self.stop_event.wait(delay):
                            break
                if not success:
                    break
                with self._worker_condition:
                    self._health_passes += 1
                    self._worker_condition.notify_all()
                self._worker_tick_completed(
                    "health-monitor",
                    decision="HEALTH_CHECK_COMPLETE",
                    next_work="wait_for_collection",
                )
        finally:
            try:
                self._persist_worker_runtime(
                    "health-monitor",
                    "stopped" if self.stop_event.is_set() else "degraded",
                    extra={"next_work": "stopped"},
                )
            except Exception:
                pass
            with self._worker_condition:
                self._worker_condition.notify_all()

    def _run_research_cycle(self) -> dict[str, Any]:
        errors: list[str] = []
        self._run_crypto_paper()
        if self._crypto_status.get("last_error"):
            errors.append(f"crypto paper: {self._crypto_status['last_error']}")
        opportunity_result = self._run_opportunity_pipeline()
        if opportunity_result is False:
            errors.append("opportunity pipeline completed with degraded output")
        self.bus.resume_expired(now=ensure_utc(self.clock()))
        paper_stats = self._run_paper_workers()
        if isinstance(paper_stats, Mapping):
            processed = int(paper_stats.get("processed_candidates", 0) or 0)
            successful = int(paper_stats.get("successful_candidates", processed) or 0)
            blocked = int(paper_stats.get("blocked_candidates", 0) or 0)
            if "error" in paper_stats or successful + blocked < processed:
                errors.append("paper engine completed with degraded candidate output")
        self.research_processor.reevaluate_forward_candidates(now=ensure_utc(self.clock()))
        queue_stats = self._run_research_queue()
        if isinstance(queue_stats, Mapping) and "error" in queue_stats:
            errors.append("research queue completed with degraded output")
        return {
            "paper": paper_stats,
            "research_queue": queue_stats,
            "degraded": bool(errors),
            "errors": errors,
        }

    def _start_heartbeat_watchdog(self) -> None:
        self._heartbeat_stop.clear()
        interval = max(0.5, min(10.0, max(float(self.config.interval_seconds), 0.5)))
        watchdog_name = f"{self.config.worker_name}:watchdog"
        try:
            self.store.save_worker_state(
                watchdog_name,
                "running",
                {
                    **self._worker_payload(watchdog_name),
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
                            **self._worker_payload(watchdog_name),
                            "pid": os.getpid(),
                            "parent_worker": self.config.worker_name,
                            "lock_path": str(self.lock_path),
                            "paper_only": True,
                            "live_execution": False,
                        },
                        started_at=self.started_at,
                        heartbeat_at=heartbeat,
                    )
                    self._heartbeat(
                        "running",
                        {"crypto_paper": dict(self._crypto_status)},
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
                    **self._worker_payload(f"{self.config.worker_name}:watchdog"),
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
                "pid": None,
                "process_identity": self.process_identity,
                "revision": self.revision,
                "execution_profile": self.execution_profile if self._lock_fd is not None else None,
                "db_path": str(self.config.db_path),
                "pid_path": str(self.pid_path),
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
        persisted_profile = (
            str(worker_payload.get("execution_profile", "")).strip().casefold()
            if isinstance(worker_payload, Mapping)
            else ""
        )
        if persisted_profile in {ISOLATED_EXECUTION_PROFILE, PRODUCTION_EXECUTION_PROFILE}:
            status_execution_profile: str | None = persisted_profile
        elif state is None:
            status_execution_profile = self.execution_profile
        else:
            # A running row without a canonical profile is not attributable
            # to this invocation.  Do not infer production from a missing or
            # malformed persisted value.
            status_execution_profile = None
        status_lock_path = self.lock_path
        status_log_path = self.log_path
        status_pid_path = self.pid_path
        if self.config.lock_path is None and isinstance(worker_payload, Mapping):
            persisted_lock_path = str(worker_payload.get("lock_path", "")).strip()
            if persisted_lock_path:
                status_lock_path = Path(persisted_lock_path)
        if self.config.log_path is None and isinstance(worker_payload, Mapping):
            persisted_log_path = str(worker_payload.get("log_path", "")).strip()
            if persisted_log_path:
                status_log_path = Path(persisted_log_path)
        if self.config.pid_path is None and isinstance(worker_payload, Mapping):
            persisted_pid_path = str(worker_payload.get("pid_path", "")).strip()
            if persisted_pid_path:
                status_pid_path = Path(persisted_pid_path)
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
        marker_pid: int | None = None
        marker_start_ticks: int | None = None
        try:
            marker_lines = status_pid_path.read_text(encoding="ascii").splitlines()
            if len(marker_lines) >= 2:
                marker_pid = int(marker_lines[0].strip())
                marker_start_ticks = int(marker_lines[1].strip())
        except (FileNotFoundError, OSError, UnicodeError, ValueError):
            marker_pid = None
            marker_start_ticks = None
        worker_alive = _pid_alive(worker_pid)
        worker_identity_valid = (
            self._lock_fd is not None
            or (
                marker_pid == worker_pid
                and _pid_matches_node(
                    worker_pid,
                    str(self.config.db_path),
                    expected_start_ticks=marker_start_ticks,
                )
            )
        ) if worker_alive is True else (None if worker_alive is None else False)
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
        persisted_stale_after = worker_payload.get("stale_after_seconds") if isinstance(worker_payload, Mapping) else None
        try:
            stale_after = float(persisted_stale_after)
        except (TypeError, ValueError):
            stale_after = max(float(self.config.interval_seconds) * 3.0, float(self.config.failure_cooldown_seconds))
        if not math.isfinite(stale_after) or stale_after < 0:
            stale_after = max(float(self.config.interval_seconds) * 3.0, float(self.config.failure_cooldown_seconds))
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
            child_heartbeat = parse_timestamp(worker_state.get("heartbeat_at"))
            try:
                child_age = (
                    max(0.0, (ensure_utc(self.clock()) - child_heartbeat).total_seconds())
                    if child_heartbeat is not None
                    else None
                )
            except Exception:
                child_age = None
            child_stale = bool(
                child_status == "running"
                and (child_age is None or child_age > stale_after)
            )
            if child_stale:
                child_status = "stale"
            child_workers[worker_name] = {
                "status": child_status,
                "heartbeat_at": worker_state.get("heartbeat_at"),
                "heartbeat_age_seconds": child_age,
                "stale": child_stale,
                "payload": child_payload,
            }
            child_degraded = child_degraded or child_status in {"degraded", "stale", "fatal"}
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
        pid_marker_exists = status_pid_path.exists()
        health_degraded = child_degraded or crypto_error
        cycles_completed = self._collection_count - self._run_cycle_base
        watchdog_state = rows.get(f"{self.config.worker_name}:watchdog")
        watchdog_payload = watchdog_state.get("payload") if isinstance(watchdog_state, Mapping) else None
        watchdog_pid_value = (
            watchdog_payload.get("pid")
            if isinstance(watchdog_payload, Mapping)
            else None
        )
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
            and _pid_alive(watchdog_pid) is True
            and _pid_matches_node(watchdog_pid, str(self.config.db_path))
            and lock_owner_pid == watchdog_pid
            and watchdog_age is not None
            and watchdog_age <= stale_after
        )
        if status not in {"stopped", "closed", "stale", "degraded"} and child_running:
            status = "running"
        liveness_candidate = status == "running" or (
            status == "degraded" and (lock_exists or pid_marker_exists)
        )
        liveness_unknown = worker_alive is None or worker_identity_valid is None
        if liveness_candidate and (
            not lock_exists
            or lock_owner_pid != worker_pid
            or worker_alive is not True
            or worker_identity_valid is not True
            or heartbeat_age is None
            or heartbeat_age > stale_after
        ):
            if liveness_unknown:
                status = "unknown"
            elif watchdog_fresh and lock_exists and worker_alive and worker_identity_valid:
                status = "degraded"
            else:
                status = "stale" if lock_exists or pid_marker_exists else "stopped"
        elif status not in {"stopped", "closed", "stale"} and health_degraded:
            status = "degraded"
        payload = {
            "worker_name": self.config.worker_name,
            "status": status,
            "pid": os.getpid() if self._lock_fd is not None else (worker_pid or None),
            "process_identity": (
                worker_payload.get("process_identity")
                if isinstance(worker_payload, Mapping)
                else self.process_identity
            ),
            "revision": (
                worker_payload.get("revision")
                if isinstance(worker_payload, Mapping)
                else self.revision
            ),
            "execution_profile": status_execution_profile,
            "db_path": str(self.config.db_path),
            "pid_path": str(status_pid_path),
            "lock_path": str(status_lock_path),
            "lock_exists": lock_exists,
            "lock_owner_pid": lock_owner_pid,
            "worker_alive": worker_alive,
            "worker_identity_valid": worker_identity_valid,
            "heartbeat_age_seconds": heartbeat_age,
            "log_path": str(status_log_path),
            "restart_count": self._restart_count,
            "cycles_completed": cycles_completed,
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
            "polymarket_autonomy": {
                "enabled": self.config.historical_refresh_enabled,
                "historical_refresh_interval_seconds": self.config.historical_refresh_interval_seconds,
                "request_budget": self.config.historical_refresh_request_budget,
                "market_budget": self.config.historical_refresh_market_budget,
                "state": self.store.get_scheduler_state(POLYMARKET_AUTONOMY_JOB_NAME),
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
            return False
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

    def _run_opportunity_pipeline(self) -> bool:
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
            if self._run_persisted_opportunity_pipeline(started):
                return True
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
            return not bool(degrading_reasons)
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
            return False

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
                and not (
                    bool((spec.config if isinstance(spec.config, Mapping) else {}).get("observation_intent"))
                    and not spec.allowed_markets
                )
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
                "blocked_candidates": 0,
                "blocked_candidate_ids": [],
                "blockers": [],
                "failed_candidates": 0,
                "failed_candidate_ids": [],
                "errors": [],
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
            "blocked_candidates": 0,
            "blocked_candidate_ids": [],
            "blockers": [],
            "failed_candidates": 0,
            "failed_candidate_ids": [],
            "errors": [],
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
                stats["failed_candidates"] += 1
                stats["failed_candidate_ids"].append(spec.experiment_id)
                stats["errors"].append("paper worker returned no result")
            elif (
                str(result.get("status") or "").upper() == "BLOCKED"
                and result.get("retryable") is False
                and str(result.get("blocker") or "").strip()
            ):
                stats["blocked_candidates"] += 1
                stats["blocked_candidate_ids"].append(spec.experiment_id)
                stats["blockers"].append(
                    {
                        "candidate_id": spec.experiment_id,
                        "blocker": str(result["blocker"]),
                        "reason_code": str(result.get("reason_code") or result["blocker"]),
                        "retryable": False,
                    }
                )
            elif "error" not in result:
                stats["successful_candidates"] += 1
                stats["observations_processed"] += int(result.get("observations_processed", 0))
                stats["fills_inserted"] += int(result.get("fills_inserted", 0))
            else:
                stats["failed_candidates"] += 1
                stats["failed_candidate_ids"].append(spec.experiment_id)
                stats["errors"].append(str(result.get("error") or "paper worker failed"))
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
            "degraded" if stats["failed_candidates"] else "idle",
            {
                "pid": os.getpid(),
                **stats,
                "last_error": stats["errors"][-1] if stats["errors"] else None,
                "paper_only": True,
                "live_execution": False,
            },
            started_at=started,
            heartbeat_at=ensure_utc(self.clock()),
        )
        return stats
    def _paper_binding_blocked_result(
        self,
        spec: Any,
        *,
        worker_name: str,
        started: datetime,
        paper_store: AxiomStore,
    ) -> dict[str, Any] | None:
        state_record = paper_store.load_paper_state(spec.experiment_id)
        if state_record is None:
            return None
        state_payload = state_record.get("state")
        persisted_binding = (
            state_payload.get("execution_binding")
            if isinstance(state_payload, Mapping)
            else None
        )
        blocker = paper_state_binding_blocker(
            persisted_binding,
            paper_execution_binding(spec),
        )
        if blocker is None:
            return None
        worker_payload = {
            "pid": os.getpid(),
            "experiment_id": spec.experiment_id,
            **blocker,
            "error": blocker["reason"],
            "error_code": PAPER_STATE_EXECUTION_BINDING_MISMATCH,
            "last_error": None,
            "last_error_code": PAPER_STATE_EXECUTION_BINDING_MISMATCH,
            "next_retry_at": None,
            "paper_only": True,
            "live_execution": False,
        }
        self.store.save_worker_state(
            worker_name,
            "blocked",
            worker_payload,
            started_at=started,
            heartbeat_at=ensure_utc(self.clock()),
        )
        return {
            "status": "BLOCKED",
            "experiment_id": spec.experiment_id,
            "observations_seen": 0,
            "observations_processed": 0,
            "observations_skipped": 0,
            "fills_inserted": 0,
            "settlements": 0,
            "execution_events": 0,
            "errors": [blocker["blocker"]],
            **blocker,
        }


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
            blocked_result = self._paper_binding_blocked_result(
                spec,
                worker_name=worker_name,
                started=started,
                paper_store=paper_store,
            )
            if blocked_result is not None:
                return blocked_result
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

    def _run_health_monitor(self) -> bool:
        worker_name = "health-monitor"
        started = ensure_utc(self.clock())
        self.store.save_worker_state(
            worker_name,
            "running",
            {
                **self._worker_payload(worker_name),
                "pid": os.getpid(),
                "paper_only": True,
                "live_execution": False,
            },
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
                    **self._worker_payload(worker_name),
                    "grade": health_grade or "UNKNOWN",
                    "grade_scope": health.get("grade_scope", "collector_health"),
                    "source_type": health.get("source_type", "FORWARD_COLLECTED"),
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
            return True
        except Exception as exc:
            self.store.save_worker_state(
                worker_name,
                "degraded",
                {
                    **self._worker_payload(worker_name),
                    "error": str(exc),
                    "paper_only": True,
                    "live_execution": False,
                },
                started_at=started,
                heartbeat_at=ensure_utc(self.clock()),
            )
            self._log(logging.ERROR, "health monitor failed: %s", exc)
            return False
    def _acquire_lock(self) -> None:
        path = self.lock_path
        path.parent.mkdir(parents=True, exist_ok=True)
        self.pid_path.parent.mkdir(parents=True, exist_ok=True)
        marker_ticks = _process_start_time_ticks()
        marker = f"{os.getpid()}\n{marker_ticks}\n"
        try:
            fd = os.open(str(path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError as exc:
            raise RuntimeError(f"another Axiom node holds {path}") from exc
        self._lock_fd = fd
        try:
            encoded_marker = marker.encode("ascii")
            offset = 0
            while offset < len(encoded_marker):
                written = os.write(fd, encoded_marker[offset:])
                if written <= 0:
                    raise OSError("lock marker write made no progress")
                offset += written
            self.pid_path.write_text(
                f"{os.getpid()}\n{marker_ticks}\n{self.process_identity}\n"
                f"{self.execution_profile}\n",
                encoding="ascii",
            )
        except BaseException:
            self._discard_acquired_lock(fd)
            raise

    def _discard_acquired_lock(self, fd: int) -> None:
        """Close and remove only the lock inode acquired by this invocation."""
        should_unlink = False
        try:
            try:
                path_stat = os.stat(self.lock_path)
                fd_stat = os.fstat(fd)
                path_inode = getattr(path_stat, "st_ino", 0)
                fd_inode = getattr(fd_stat, "st_ino", 0)
                inode_available = path_inode not in (None, 0) and fd_inode not in (None, 0)
                should_unlink = (
                    path_stat.st_dev == fd_stat.st_dev and path_inode == fd_inode
                    if inode_available
                    else False
                )
            except OSError:
                should_unlink = False
        finally:
            if self._lock_fd == fd:
                self._lock_fd = None
            try:
                os.close(fd)
            except OSError:
                pass
        if should_unlink:
            try:
                self.lock_path.unlink()
            except FileNotFoundError:
                pass

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
            try:
                marker_lines = self.pid_path.read_text(encoding="ascii").splitlines()
                if marker_lines and int(marker_lines[0].strip()) == os.getpid():
                    self.pid_path.unlink()
            except (FileNotFoundError, OSError, ValueError):
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

    def _log(self, level: int, message: str, *args: Any) -> None:
        if self._logger is not None:
            self._logger.log(level, message, *args)

    def _close_logging(self) -> None:
        if self._logger is not None and self._handler is not None:
            self._logger.removeHandler(self._handler)
            self._handler.close()
        self._logger, self._handler = None, None

    def _heartbeat(self, status: str, payload: Mapping[str, Any]) -> None:
        body = {
            **dict(payload),
            "pid": os.getpid(),
            "process_identity": self.process_identity,
            "revision": self.revision,
            "execution_profile": self.execution_profile,
            "db_path": str(self.config.db_path),
            "pid_path": str(self.pid_path),
            "lock_path": str(self.lock_path),
            "log_path": str(self.log_path),
            "stale_after_seconds": max(float(self.config.interval_seconds) * 3.0, float(self.config.failure_cooldown_seconds)),
            "paper_only": True,
            "live_execution": False,
        }
        self.store.save_worker_state(
            self.config.worker_name,
            status,
            body,
            started_at=self.started_at,
            heartbeat_at=ensure_utc(self.clock()),
        )


__all__ = ["NodeConfig", "ResearchNode"]
