"""Continuous, resumable, read-only Polymarket collection.

The collector stores every source payload with an observation timestamp and a
content-derived identity.  Repeating a cycle is therefore safe: identical
metadata, snapshots, and trades are ignored, while changed metadata remains an
immutable record.  Network failures become collection-error records and never
become synthetic prices or settlements.
"""
from __future__ import annotations

from contextlib import nullcontext
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from concurrent.futures import FIRST_COMPLETED, Future, TimeoutError as FutureTimeout, wait
import hashlib
import inspect
import json
import math
import queue
import threading
import time
from typing import Any, Callable, Mapping, Sequence

from .data._http import HTTPFetchError
from .data.interfaces import PredictionMarketDataProvider
from .domain import (
    MarketType,
    OrderBookSnapshot,
    PredictionMarketSnapshot,
    ResearchQuality,
    SettlementState,
    TradePrint,
    ensure_utc,
    parse_timestamp,
    to_record,
    utc_now,
)
from .storage import AxiomStore
from .forward import ForwardTestRegistry, ForwardTestSpec
from .lifecycle import CandidateLifecycleManager, CandidateStage
_UNSET = object()
_MAX_SCOPE_REQUEST_PATH_LENGTH = 512
_MAX_SCOPE_QUERY_DEPTH = 8
_MAX_SCOPE_QUERY_ITEMS = 128
_MAX_SCOPE_QUERY_STRING_LENGTH = 1024
_MAX_CYCLE_CONTINUATION_IDS = 256


class _ScopePersistenceValueError(ValueError):
    """A provider pagination value cannot be persisted as plain JSON."""


class _ProviderDeadlineExceeded(TimeoutError):
    """A provider operation did not finish before the collector deadline."""

    deadline_expired = True

    def __init__(
        self,
        endpoint: str,
        timeout_seconds: float,
        *,
        in_flight: bool = False,
        cycle_expired: bool = False,
    ) -> None:
        self.endpoint = str(endpoint)
        self.timeout_seconds = float(timeout_seconds)
        self.in_flight = bool(in_flight)
        self.cycle_expired = bool(cycle_expired)
        reason = (
            "COLLECTOR_CYCLE_DEADLINE_EXCEEDED"
            if self.cycle_expired
            else ("PROVIDER_CALL_IN_FLIGHT" if self.in_flight else "PROVIDER_CALL_TIMEOUT")
        )
        super().__init__(
            f"{reason}: {self.endpoint} exceeded {self.timeout_seconds:g}s deadline"
        )


class _BoundedProviderExecutor:
    """A fixed-size daemon executor for provider calls.

    Python cannot forcibly stop a thread blocked in an arbitrary provider.
    Keeping the executor fixed-size and daemonized prevents a timed-out call
    from wedging the collector or creating one permanent thread per retry.
    Callers cancel queued work and key their active operations so a later tick
    cannot duplicate an in-flight public request.
    """

    def __init__(self, max_workers: int) -> None:
        workers = max(1, int(max_workers))
        self._queue: queue.Queue[tuple[Future[Any], Callable[[], Any]] | None] = queue.Queue(
            maxsize=workers * 2
        )
        self._closed = False
        self._lock = threading.Lock()
        self._threads: list[threading.Thread] = []
        for index in range(workers):
            ready = threading.Event()
            thread = threading.Thread(
                target=self._run,
                args=(ready,),
                name=f"axiom-provider-{index}",
                daemon=True,
            )
            thread.start()
            ready.wait(timeout=0.5)
            self._threads.append(thread)

    def submit(self, operation: Callable[[], Any]) -> Future[Any]:
        future: Future[Any] = Future()
        with self._lock:
            if self._closed:
                raise RuntimeError("provider executor is closed")
            try:
                self._queue.put_nowait((future, operation))
            except queue.Full as exc:
                raise RuntimeError("provider executor queue is full") from exc
        return future

    def shutdown(
        self,
        *,
        wait: bool = True,
        timeout: float | None = 0.5,
    ) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            # Cancel queued work before placing sentinels so every worker can
            # observe shutdown even when a caller queued its full bounded set.
            while True:
                try:
                    item = self._queue.get_nowait()
                except queue.Empty:
                    break
                self._queue.task_done()
                if item is not None:
                    item[0].cancel()
            for _ in self._threads:
                self._queue.put_nowait(None)
        if not wait:
            return
        deadline = None if timeout is None else time.monotonic() + max(0.0, float(timeout))
        current = threading.current_thread()
        for thread in self._threads:
            if thread is current:
                continue
            remaining = None if deadline is None else max(0.0, deadline - time.monotonic())
            thread.join(timeout=remaining)
    def _run(self, ready: threading.Event | None = None) -> None:
        if ready is not None:
            ready.set()
        while True:
            item = self._queue.get()
            try:
                if item is None:
                    return
                future, operation = item
                if not future.set_running_or_notify_cancel():
                    continue
                try:
                    result = operation()
                except BaseException as exc:
                    future.set_exception(exc)
                else:
                    future.set_result(result)
            finally:
                self._queue.task_done()


_MAX_SCOPE_CURSOR_HISTORY = 256
# Keep the complete bounded inventory across keyset pages small enough for
# durable continuation while preventing page-local scope authority.
_MAX_SCOPE_INVENTORY = 10_000
# The pure scope resolver accepts at most this many records per candidate.  A
# collector tick resolves at most this many candidate documents, matching the
# existing ranking/lifecycle query caps.
_MAX_SCOPE_RESOLUTION_MARKETS = 1_000
_MAX_SCOPE_RESOLUTION_CANDIDATES = 1_000
_MAX_CANDIDATE_BOUND_MARKETS = 100

_MAX_SCOPE_DIRECT_LOOKUPS = 256
_MAX_OBSERVATION_HANDOFF_FAST_CYCLES = 3
_MAX_OBSERVATION_HANDOFF_BATCH = 16

# Discovery may assess one shared set of assumptions across the complete
# durable inventory before candidate-specific resolution begins.  The product
# therefore adds one inventory-sized discovery set to the resolver matrix.
# Entries are never evicted: this ceiling covers every assessment key that can
# be needed by one bounded resolution pass.
_MAX_SCOPE_SUITABILITY_CACHE = _MAX_SCOPE_INVENTORY + (
    min(
        _MAX_SCOPE_INVENTORY,
        _MAX_SCOPE_RESOLUTION_MARKETS,
    ) * _MAX_SCOPE_RESOLUTION_CANDIDATES
)


@dataclass(frozen=True, slots=True)
class CollectorConfig:
    """Operational settings for the public-data collector."""

    interval_seconds: float = 60.0
    depth: int = 20
    stale_after_seconds: float | None = None
    max_markets: int = 100
    active: bool = True
    market_ids: tuple[str, ...] = ()
    collector_name: str = "polymarket"
    max_attempts: int = 3
    backoff_initial_seconds: float = 1.0
    backoff_multiplier: float = 2.0
    backoff_max_seconds: float = 30.0
    jitter_seconds: float = 0.25
    failure_cooldown_seconds: float = 30.0
    provider_timeout_seconds: float = 10.0
    max_trade_pages: int = 100
    max_provider_clock_skew_seconds: float = 5.0
    retain_cycles: int = 1
    discovery_budget_per_cycle: int = 20
    # Suitable-market production is deliberately bounded and read-only.  The
    # defaults describe the smallest observable paper probe; policies may
    # tighten them through their own frozen scope filters.
    intended_token: str = "yes"
    required_capital: float = 1.0
    min_entry_depth: float = 0.0
    min_exit_depth: float = 0.0
    min_activity: float = 0.0
    max_suitable_pages_per_cycle: int = 20
    max_concurrency: int = 1
    freshness_sla_seconds: float | None = None
    poll_plan: Callable[..., Any] | None = None

    def __post_init__(self) -> None:
        interval = float(self.interval_seconds)
        if not math.isfinite(interval) or interval <= 0:
            raise ValueError("collector interval must be finite and positive")
        if isinstance(self.depth, bool) or not isinstance(self.depth, int) or self.depth <= 0:
            raise ValueError("collector depth must be a positive integer")
        if self.stale_after_seconds is not None:
            stale_after = float(self.stale_after_seconds)
            if not math.isfinite(stale_after) or stale_after <= 0:
                raise ValueError("stale_after_seconds must be finite and positive")
        if self.freshness_sla_seconds is not None:
            freshness = float(self.freshness_sla_seconds)
            if not math.isfinite(freshness) or freshness <= 0:
                raise ValueError("freshness_sla_seconds must be finite and positive")
        if isinstance(self.max_markets, bool) or not isinstance(self.max_markets, int) or self.max_markets <= 0:
            raise ValueError("max_markets must be a positive integer")
        if isinstance(self.discovery_budget_per_cycle, bool) or not isinstance(self.discovery_budget_per_cycle, int) or self.discovery_budget_per_cycle < 0:
            raise ValueError("discovery_budget_per_cycle must be a non-negative integer")
        token = str(self.intended_token).strip().lower()
        if token not in {"yes", "no"}:
            raise ValueError("intended_token must be yes or no")
        object.__setattr__(self, "intended_token", token)
        for name in ("required_capital", "min_entry_depth", "min_exit_depth", "min_activity"):
            value = getattr(self, name)
            if isinstance(value, bool):
                raise ValueError(f"{name} must be finite and non-negative")
            number = float(value)
            if not math.isfinite(number) or number < 0:
                raise ValueError(f"{name} must be finite and non-negative")
            object.__setattr__(self, name, number)
        if (
            isinstance(self.max_suitable_pages_per_cycle, bool)
            or not isinstance(self.max_suitable_pages_per_cycle, int)
            or self.max_suitable_pages_per_cycle <= 0
        ):
            raise ValueError("max_suitable_pages_per_cycle must be a positive integer")
        if isinstance(self.max_concurrency, bool) or not isinstance(self.max_concurrency, int) or self.max_concurrency not in {1, 2}:
            raise ValueError("max_concurrency must be one or two")
        if not str(self.collector_name).strip():
            raise ValueError("collector_name is required")
        timeout_seconds = float(self.provider_timeout_seconds)
        if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
            raise ValueError("provider_timeout_seconds must be finite and positive")
        for name in (
            "backoff_initial_seconds",
            "backoff_multiplier",
            "backoff_max_seconds",
            "jitter_seconds",
            "failure_cooldown_seconds",
            "max_provider_clock_skew_seconds",
        ):
            value = float(getattr(self, name))
            if not math.isfinite(value) or value < 0:
                raise ValueError(f"{name} must be finite and non-negative")
        if self.backoff_multiplier < 1:
            raise ValueError("backoff_multiplier must be at least one")
        if not isinstance(self.max_attempts, int) or isinstance(self.max_attempts, bool) or self.max_attempts <= 0:
            raise ValueError("max_attempts must be a positive integer")
        if not isinstance(self.max_trade_pages, int) or isinstance(self.max_trade_pages, bool) or self.max_trade_pages <= 0:
            raise ValueError("max_trade_pages must be a positive integer")
        if not isinstance(self.retain_cycles, int) or isinstance(self.retain_cycles, bool) or self.retain_cycles <= 0:
            raise ValueError("retain_cycles must be a positive integer")
        normalized = tuple(dict.fromkeys(str(item).strip() for item in self.market_ids if str(item).strip()))
        object.__setattr__(self, "market_ids", normalized)
    @property
    def cycle_budget_seconds(self) -> float:
        """Maximum provider-work window for one collection tick."""
        # Scope resolution can need one bounded inventory window, one direct
        # exact-id window, and one downstream collection window.
        return 3.0 * float(self.provider_timeout_seconds)



@dataclass(frozen=True, slots=True)
class CollectionCycle:
    started_at: datetime
    ended_at: datetime
    markets_seen: int
    markets_attempted: int
    markets_successful: int
    markets_failed: int
    metadata_inserted: int = 0
    snapshots_inserted: int = 0
    snapshot_duplicates: int = 0
    trades_inserted: int = 0
    trade_duplicates: int = 0
    errors: int = 0
    requests: int = 0
    rate_limits: int = 0
    retries: int = 0
    provider_failures: int = 0
    provider_timeouts: int = 0
    cooldowns: int = 0
    provider_timeout_evidence: Sequence[Mapping[str, Any]] = ()
    skipped_markets: int = 0
    metadata_failures: int = 0
    order_book_failures: int = 0
    trade_failures: int = 0
    elapsed_seconds: float | None = None
    candidate_bound_markets: tuple[str, ...] = ()
    candidate_bound_scheduled: tuple[str, ...] = ()
    candidate_bound_fresh: tuple[str, ...] = ()
    candidate_bound_stale: tuple[str, ...] = ()
    candidate_bound_missing: tuple[str, ...] = ()
    paper_forward_markets: tuple[str, ...] = ()
    paper_forward_scheduled: tuple[str, ...] = ()
    discovery_scheduled: tuple[str, ...] = ()
    discovery_deferred: tuple[str, ...] = ()
    candidate_references: Mapping[str, Sequence[str]] | None = None
    suitable_market_scheduled: tuple[str, ...] = ()
    suitable_market_deferred: tuple[str, ...] = ()
    discovery_exclusions: Sequence[Mapping[str, Any]] = ()
    inventory_coverage: str | None = None
    market_authorization: Mapping[str, Any] | None = None
    tier_attempts: Mapping[str, int] | None = None
    tier_successes: Mapping[str, int] | None = None
    tier_failures: Mapping[str, int] | None = None
    request_latency_summary: Mapping[str, Any] | None = None
    capacity_reason: str | None = None
    discovery_coverage_status: str | None = None
    discovery_cursor: str | None = None
    discovery_complete: bool | None = None
    current_stage: str | None = None
    current_endpoint: str | None = None

    @property
    def duration_seconds(self) -> float:
        if self.elapsed_seconds is not None:
            return max(0.0, float(self.elapsed_seconds))
        return max(0.0, (self.ended_at - self.started_at).total_seconds())

    def as_record(self) -> dict[str, Any]:
        return {
            "started_at": self.started_at.isoformat(),
            "ended_at": self.ended_at.isoformat(),
            "duration_seconds": self.duration_seconds,
            "markets_seen": self.markets_seen,
            "markets_attempted": self.markets_attempted,
            "markets_successful": self.markets_successful,
            "markets_failed": self.markets_failed,
            "metadata_inserted": self.metadata_inserted,
            "snapshots_inserted": self.snapshots_inserted,
            "snapshot_duplicates": self.snapshot_duplicates,
            "trades_inserted": self.trades_inserted,
            "trade_duplicates": self.trade_duplicates,
            "errors": self.errors,
            "requests": self.requests,
            "rate_limits": self.rate_limits,
            "provider_timeouts": self.provider_timeouts,
            "retries": self.retries,
            "provider_failures": self.provider_failures,
            "cooldowns": self.cooldowns,
            "skipped_markets": self.skipped_markets,
            "metadata_failures": self.metadata_failures,
            "order_book_failures": self.order_book_failures,
            "trade_failures": self.trade_failures,
            "candidate_bound_markets": list(self.candidate_bound_markets),
            "candidate_bound_scheduled": list(self.candidate_bound_scheduled),
            "candidate_bound_fresh": list(self.candidate_bound_fresh),
            "suitable_market_scheduled": list(self.suitable_market_scheduled),
            "suitable_market_deferred": list(self.suitable_market_deferred),
            "discovery_exclusions": [dict(item) for item in self.discovery_exclusions],
            "inventory_coverage": self.inventory_coverage,
            "market_authorization": dict(self.market_authorization or {}),
            "candidate_bound_stale": list(self.candidate_bound_stale),
            "candidate_bound_missing": list(self.candidate_bound_missing),
            "paper_forward_markets": list(self.paper_forward_markets),
            "paper_forward_scheduled": list(self.paper_forward_scheduled),
            "discovery_scheduled": list(self.discovery_scheduled),
            "discovery_deferred": list(self.discovery_deferred),
            "candidate_references": {
                str(key): list(value)
                for key, value in (self.candidate_references or {}).items()
            },
            "tier_attempts": dict(self.tier_attempts or {}),
            "tier_successes": dict(self.tier_successes or {}),
            "request_latency_summary": dict(self.request_latency_summary or {}),
            "tier_failures": dict(self.tier_failures or {}),
            "capacity_reason": self.capacity_reason,
            "discovery_coverage_status": self.discovery_coverage_status,
            "discovery_cursor": self.discovery_cursor,
            "discovery_complete": self.discovery_complete,
            "current_stage": self.current_stage,
            "current_endpoint": self.current_endpoint,
            "provider_timeout_evidence": [dict(item) for item in self.provider_timeout_evidence],
        }






class PolymarketCollector:
    """Collect Gamma metadata and CLOB observations without order submission."""

    def __init__(
        self,
        provider: PredictionMarketDataProvider,
        store: AxiomStore,
        config: CollectorConfig | None = None,
        *,
        clock: Callable[[], datetime] = utc_now,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.provider = provider
        self._suitable_market_evidence: list[dict[str, Any]] = []
        self._scope_suitability_cache: dict[tuple[str, str], dict[str, Any]] = {}
        self.store = store
        self.config = config or CollectorConfig()
        self.clock = clock
        self.sleep = sleep
        self._discovery_continuation: Mapping[str, Any] | None = None
        # Set for the duration of a tick and intentionally left in place for
        # daemon workers that finish after the caller has returned.
        self._cycle_deadline_monotonic: float | None = None
        self._cycle_deadline_exhausted = False
        self._scope_phase_active = False
        self._cycle_last_remaining_market_ids: tuple[str, ...] = ()
        # A present rolling selection is a closed collection authority.  If a
        # selected member has no exact scope resolution, discovery must not
        # widen the cycle back to the public catalog.
        self._rolling_scope_blocked = False
        self._rolling_scope_candidate_ids: tuple[str, ...] = ()
        self._rolling_scope_documents: dict[str, Mapping[str, Any]] = {}
        self._scope_authority_market_ids: set[str] = set()

        self._scope_direct_lookup_cursor = 0
        self._scope_direct_priority_lookup_cursor = 0
        self._scope_direct_protected_lookup_cursor = 0
        self._scope_resolution_candidate_cursor = 0
        self._scope_resolution_deferred_cursor = 0
        self._scope_inventory_budget_remaining: float | None = None
        self._scope_direct_budget_remaining: float | None = None

        # The nested continuation is assembled during scope discovery and
        # persisted with the root collector state at the end of the cycle.
        self._scope_inventory_continuation: Mapping[str, Any] | None = None
        # Scope suitability refreshes are shared by inventory resolution and
        # candidate projection.  Remember attempts and successful snapshots
        # for this tick so a provider call is issued only once.
        self._scope_refresh_attempted: set[str] = set()
        self._scope_refreshed_snapshots: dict[str, PredictionMarketSnapshot] = {}
        self._scope_resolution_deferred_candidate_ids: tuple[str, ...] = ()
        self._observation_materialization_deferred_candidate_ids: tuple[str, ...] = ()
        self._observation_materialization_cursor = 0
        self._observation_materialization_turn = 0
        self._scope_resolutions: dict[str, Any] = {}
        self._scope_handoff_fast_streak = 0
        # Resolutions produced for this cycle are the only authority passed to
        # observation-intent materialization; callers cannot supply market ids
        self._provider_executor_lock = threading.Lock()
        self._provider_executor: _BoundedProviderExecutor | None = None
        self._scope_provider_executor: _BoundedProviderExecutor | None = None
        # Exact selected ids use a provider clone as well as a separate
        # executor.  A provider may carry a session/throttle lock that a
        # broad inventory request can hold until its transport deadline.
        # Fresh clones isolate each exact operation; completed callers release
        # them after diagnostics while timed-out workers remain bounded here.
        self._scope_direct_provider_executor: _BoundedProviderExecutor | None = None
        self._scope_direct_provider: Any | None = None
        self._scope_direct_provider_ids: set[int] = set()
        self._scope_direct_provider_refs: dict[int, Any] = {}
        self._scope_broad_provider_unavailable = False
        self._active_provider_calls: set[tuple[int, str]] = set()
        self._provider_call_pools: dict[tuple[int, str], str] = {}
        self._provider_timeout_evidence: list[dict[str, Any]] = []
        self._current_stage = "idle"
        self._provider_futures: dict[tuple[int, str], Future[Any]] = {}
        self._current_endpoint: str | None = None
        self._collection_executor: _BoundedProviderExecutor | None = None
        self._closed = False

    def close(self) -> None:
        """Stop all provider workers owned by this collector, idempotently."""
        with self._provider_executor_lock:
            if self._closed:
                return
            self._closed = True
            executor_values = (
                getattr(self, "_provider_executor", None),
                getattr(self, "_scope_provider_executor", None),
                getattr(self, "_scope_direct_provider_executor", None),
                getattr(self, "_collection_provider_executor", None),
                getattr(self, "_collection_executor", None),
            )
            executors: list[_BoundedProviderExecutor] = []
            seen: set[int] = set()
            for executor in executor_values:
                if executor is not None and id(executor) not in seen:
                    executors.append(executor)
                    seen.add(id(executor))
        for executor in executors:
            executor.shutdown()
        with self._provider_executor_lock:
            active_direct_provider_ids = {
                provider_id
                for provider_id, endpoint in self._active_provider_calls
                if self._provider_call_pools.get((provider_id, endpoint)) == "scope_direct"
            }
            direct_providers = [
                provider
                for provider in self._scope_direct_provider_refs.values()
                if id(provider) not in active_direct_provider_ids
            ]
        seen_providers: set[int] = set()
        for direct_provider in direct_providers:
            if direct_provider is self.provider or id(direct_provider) in seen_providers:
                continue
            seen_providers.add(id(direct_provider))
            close = getattr(direct_provider, "close", None)
            if callable(close):
                try:
                    close()
                except Exception:
                    pass
    def _set_current_stage(
        self,
        stage: str,
        endpoint: str | None,
        observed_at: datetime | None = None,
        *,
        persist: bool = False,
    ) -> None:
        """Publish the operation currently owning the collector tick.

        This intentionally uses the collector root state, rather than only
        cycle output: a provider call can be blocked before a cycle exists.
        The endpoint is bounded plain text so a malformed provider label
        cannot make durable state unbounded.
        """
        self._current_stage = str(stage).strip()[:128] or "unknown"
        endpoint_text = str(endpoint).strip()[:512] if endpoint is not None else ""
        self._current_endpoint = endpoint_text or None
        if not persist:
            return
        try:
            state = self.store.get_collector_state(self.config.collector_name) or {}
            state = dict(state) if isinstance(state, Mapping) else {}
            state.update(
                {
                    "current_stage": self._current_stage,
                    "current_endpoint": self._current_endpoint,
                    "current_stage_at": ensure_utc(observed_at or self.clock()).isoformat(),
                }
            )
            self.store.set_collector_state(self.config.collector_name, state)
        except Exception:
            # Stage evidence must never turn a provider timeout into a second
            # collector failure.
            pass
    def _cycle_remaining_seconds(self) -> float | None:
        deadline = self._cycle_deadline_monotonic
        if deadline is None:
            return None
        return deadline - time.monotonic()

    def _cycle_budget_error(
        self,
        endpoint: str,
        observed_at: datetime,
        counters: dict[str, Any],
    ) -> _ProviderDeadlineExceeded:
        self._cycle_deadline_exhausted = True
        return self._record_provider_timeout(
            endpoint,
            observed_at,
            counters,
            cycle_expired=True,
        )

    def _cycle_budget_available(
        self,
        endpoint: str,
        observed_at: datetime,
        counters: dict[str, Any],
    ) -> bool:
        remaining = self._cycle_remaining_seconds()
        if remaining is None or remaining > 0:
            return True
        raise self._cycle_budget_error(endpoint, observed_at, counters)



    def _mark_cycle_exhaustion(self, endpoint: str | None = None) -> None:
        """Record a local deadline boundary without waiting on provider work."""
        self._cycle_deadline_exhausted = True
        if endpoint:
            self._current_endpoint = str(endpoint).strip()[:512] or self._current_endpoint
        self._current_stage = "cycle_deadline"

    def collect_once(
        self,
        market_ids: Sequence[str] | None = None,
        *,
        now: datetime | None = None,
    ) -> CollectionCycle:
        started = ensure_utc(now or self.clock())
        monotonic_started = time.monotonic()
        self._cycle_deadline_monotonic = monotonic_started + self.config.cycle_budget_seconds
        self._cycle_deadline_exhausted = False
        self._scope_inventory_budget_remaining = None
        self._scope_direct_budget_remaining = None
        self._cycle_last_remaining_market_ids = ()
        root_state = self.store.get_collector_state(self.config.collector_name) or {}
        previous_cycle_continuation = (
            root_state.get("cycle_continuation")
            if isinstance(root_state.get("cycle_continuation"), Mapping)
            else {}
        )
        resume_ids = [
            str(item).strip()
            for item in previous_cycle_continuation.get("remaining_market_ids", ())
            if str(item).strip()
        ][:_MAX_CYCLE_CONTINUATION_IDS]
        persisted_scope_continuation = root_state.get("scope_inventory_continuation")
        self._scope_inventory_continuation = (
            self._bound_scope_continuation(persisted_scope_continuation)
            if isinstance(persisted_scope_continuation, Mapping)
            else None
        )
        self._scope_direct_lookup_cursor = self._scope_direct_cursor(root_state)
        self._scope_direct_priority_lookup_cursor = self._scope_direct_priority_cursor(root_state)
        self._scope_direct_protected_lookup_cursor = self._scope_direct_protected_cursor(root_state)
        self._scope_resolution_candidate_cursor = self._scope_resolution_candidate_cursor_value(root_state)
        try:
            self._scope_resolution_deferred_cursor = max(
                0,
                int(root_state.get("scope_resolution_deferred_cursor", 0)),
            )
        except (TypeError, ValueError, OverflowError):
            self._scope_resolution_deferred_cursor = 0

        self._discovery_continuation = (
            root_state.get("discovery_continuation")
            if isinstance(root_state.get("discovery_continuation"), Mapping)
            else None
        )
        requested = tuple(dict.fromkeys(str(item).strip() for item in (market_ids or ()) if str(item).strip()))
        superseded_candidate_ids, superseded_intent_ids = (
            self._superseded_observation_ids()
        )
        rolling_scope_ids = self._rolling_scope_market_ids()
        rolling_exact_scope_ids = {
            market_id
            for document in getattr(self, "_rolling_scope_documents", {}).values()
            if isinstance(document, Mapping)
            for market_id in self._scope_exact_market_ids((document,))
        }
        rolling_non_exact_ids = tuple(
            item for item in rolling_scope_ids if item not in rolling_exact_scope_ids
        )

        configured = requested or self.config.market_ids
        self._scope_suitability_cache = {}
        self._scope_refreshed_snapshots = {}
        self._scope_broad_provider_unavailable = False
        self._scope_refresh_attempted = set()
        self._scope_resolution_deferred_candidate_ids = tuple(
            str(item).strip()
            for item in root_state.get("scope_resolution_deferred_candidate_ids", ())
            if str(item).strip()
        )[:_MAX_SCOPE_RESOLUTION_CANDIDATES]
        self._observation_materialization_deferred_candidate_ids = tuple(
            str(item).strip()
            for item in root_state.get("observation_materialization_deferred_candidate_ids", ())
            if str(item).strip()
        )[:_MAX_SCOPE_RESOLUTION_CANDIDATES]
        try:
            self._observation_materialization_cursor = max(
                0,
                int(root_state.get("observation_materialization_cursor", 0)),
            )
        except (TypeError, ValueError, OverflowError):
            self._observation_materialization_cursor = 0
        try:
            self._observation_materialization_turn = int(
                root_state.get("observation_materialization_turn", 0)
            ) % 2
        except (TypeError, ValueError, OverflowError):
            self._observation_materialization_turn = 0
        try:
            self._scope_handoff_fast_streak = max(
                0,
                int(root_state.get("scope_handoff_fast_streak", 0)),
            )
        except (TypeError, ValueError, OverflowError):
            self._scope_handoff_fast_streak = 0
        configured_values = tuple(dict.fromkeys([*configured, *rolling_non_exact_ids]))

        primary_candidate_ids = list(dict.fromkeys([
            *(
                candidate_id
                for candidate_id in (self._active_primary_candidate_ids() or ())
                if candidate_id not in superseded_candidate_ids
            ),
            *(
                candidate_id
                for candidate_id in getattr(self, "_rolling_scope_candidate_ids", ())
                if candidate_id not in superseded_candidate_ids
            ),
        ]))
        paper_ids = [
            candidate_id
            for candidate_id in self._active_paper_forward_ids()
            if candidate_id not in superseded_candidate_ids
        ]
        observation_intent_ids = [
            candidate_id
            for candidate_id in self._active_observation_intent_ids()
            if candidate_id not in superseded_candidate_ids
        ]

        primary_candidate_set = set(primary_candidate_ids)
        paper_set = set(paper_ids)
        observation_set = set(observation_intent_ids)
        self._provider_timeout_evidence = []
        self._current_stage = "collection_start"
        self._current_endpoint = None
        counters = self._new_counters()
        discovery_exclusions: list[Mapping[str, Any]] = []
        self._scope_resolutions = {}
        self._scope_phase_active = True
        try:
            scope_candidate_ids, scope_candidate_markets, scope_discovered, scope_cursor = (
                self._resolve_market_scopes(
                    started,
                    tuple(
                        dict.fromkeys(
                            [*(primary_candidate_ids or ()), *paper_ids, *observation_intent_ids]
                        )
                    ),
                    root_state,
                    counters,
                )
            )
            scope_authorized_market_ids = {
                market_id
                for values in scope_candidate_markets.values()
                for market_id in values
            }
            resume_ids = [
                market_id
                for market_id in resume_ids
                if (
                    market_id not in self._scope_authority_market_ids
                    or market_id in scope_authorized_market_ids
                )
            ]


        finally:
            self._scope_phase_active = False
        if self._scope_persisted_proof_restore_allowed():
            self._restore_current_observation_scope_proofs(
                started,
                observation_intent_ids,
                scope_candidate_ids,
                scope_candidate_markets,
                self._scope_resolutions,
            )
        if isinstance(self._scope_inventory_continuation, Mapping):
            discovery_exclusions = [
                *(
                    dict(item)
                    for item in self._scope_inventory_continuation.get("suitability_exclusions", ())
                    if isinstance(item, Mapping)
                ),
            ]
        materialized_observation_ids = self._materialize_observation_intents(
            started,
            observation_intent_ids,
            scope_candidate_markets,
            counters,
            scope_resolutions=self._scope_resolutions,
        )
        validated_observation_handoff_ids = self._validated_observation_handoff_ids(
            tuple(paper_set & observation_set),
            scope_candidate_markets,
            self._scope_resolutions,
        )

        scope_candidate_set = set(scope_candidate_ids)
        scope_primary_ids = [
            identifier
            for identifier in scope_candidate_ids
            if identifier in primary_candidate_set
        ]
        scope_paper_ids = [
            identifier
            for identifier in scope_candidate_ids
            if identifier not in primary_candidate_set
            and (
                identifier in (paper_set - observation_set)
                or identifier in set(materialized_observation_ids)
                or identifier in validated_observation_handoff_ids
            )
        ]
        legacy_primary_ids = [
            identifier for identifier in primary_candidate_ids
            if identifier not in scope_candidate_set
        ]
        candidate_requirements = self._candidate_requirements(
            started, candidate_ids=legacy_primary_ids
        )
        candidate_bound = self._requirement_markets(candidate_requirements)
        candidate_references = self._requirement_references(candidate_requirements)
        # Scope resolution is the sole authority for candidates carrying a
        # frozen market policy.  The older requirements projection is retained
        # only for candidates without one, preserving collection priority for
        # legacy stores while preventing a second scope authority.
        for candidate_id in scope_primary_ids:
            for market_id in scope_candidate_markets.get(candidate_id, ()):
                if market_id not in candidate_bound:
                    candidate_bound.append(market_id)
                candidate_references.setdefault(market_id, [])
                if candidate_id not in candidate_references[market_id]:
                    candidate_references[market_id].append(candidate_id)
        configured_paper_values: tuple[str, ...] = ()
        if scope_candidate_set:
            allowed_scope_ids = {
                market_id
                for values in scope_candidate_markets.values()
                for market_id in values
            }
            configured_values = tuple(
                item
                for item in configured
                if item in {
                    market_id
                    for candidate_id in scope_primary_ids
                    for market_id in scope_candidate_markets.get(candidate_id, ())
                }
            )
            configured_paper_values = tuple(
                item
                for item in configured
                if item in allowed_scope_ids and item not in configured_values
            )
            # Rolling rule-scope members remain an independent market
            # authority; exact ids are scheduled only from their resolver
            # matches above.
            configured_values = tuple(dict.fromkeys([
                *configured_values,
                *rolling_non_exact_ids,
            ]))
        if configured_values:
            candidate_bound = list(dict.fromkeys([*configured_values, *candidate_bound]))
            for identifier in configured_values:
                candidate_references.setdefault(identifier, [])
        candidate_bound = list(dict.fromkeys(candidate_bound))[:_MAX_CANDIDATE_BOUND_MARKETS]
        candidate_bound_set = set(candidate_bound)
        candidate_references = {
            market_id: references
            for market_id, references in candidate_references.items()
            if market_id in candidate_bound_set
        }
        configured_values = tuple(
            market_id for market_id in configured_values if market_id in candidate_bound_set
        )


        # The storage health projection consumes the legacy authority shape.
        # Project resolved scope ids into that shape without asking storage to
        # rediscover or reinterpret the canonical policy.
        health_requirements: Mapping[str, Any] = candidate_requirements
        if scope_primary_ids:
            health_requirements = {
                **dict(candidate_requirements),
                "market_ids": list(candidate_bound),
                "candidate_references": candidate_references,
            }
        candidate_health = self._required_health(health_requirements, started, candidate_bound)
        candidate_fresh = list(candidate_health.get("fresh", ()))
        candidate_stale = list(candidate_health.get("stale", ()))
        candidate_missing = list(candidate_health.get("missing", ()))
        if configured_values:
            represented = set(candidate_fresh) | set(candidate_stale) | set(candidate_missing)
            candidate_missing.extend(
                identifier for identifier in configured_values if identifier not in represented
            )
            candidate_missing = list(dict.fromkeys(candidate_missing))
        known_candidate = set(candidate_bound)
        due_set = set(candidate_stale) | set(candidate_missing)
        due_candidates = [identifier for identifier in candidate_bound if identifier in due_set]
        # Explicit/configured ids are authoritative even when no lifecycle
        # authority exists in a lightweight fake store.
        if configured_values:
            due_candidates = list(dict.fromkeys([*configured_values, *due_candidates]))
            known_candidate.update(configured_values)

        legacy_paper_ids = [
            identifier
            for identifier in paper_ids
            if identifier not in primary_candidate_set
            and identifier not in scope_candidate_set
        ]
        paper_requirements = self._candidate_requirements(started, candidate_ids=legacy_paper_ids)
        paper_markets = [
            market_id
            for market_id in configured_paper_values
            if market_id not in known_candidate
        ]
        paper_markets.extend(
            market_id
            for market_id in self._requirement_markets(paper_requirements)
            if market_id not in known_candidate and market_id not in paper_markets
        )
        for candidate_id in scope_paper_ids:
            for market_id in scope_candidate_markets.get(candidate_id, ()):
                if market_id not in known_candidate and market_id not in paper_markets:
                    paper_markets.append(market_id)

        capacity = self.config.max_markets
        candidate_scheduled = due_candidates[:capacity]
        remaining = max(0, capacity - len(candidate_scheduled))
        paper_scheduled = paper_markets[:remaining]
        remaining = max(0, remaining - len(paper_scheduled))

        discovery_cursor = scope_cursor if scope_candidate_set else root_state.get(
            "discovery_carry_cursor", 0
        )
        discovery_scheduled: list[str] = []
        discovered: dict[str, PredictionMarketSnapshot] = {}
        discovery_deferred: list[str] = []
        discovery_coverage_status = (
            str((self._discovery_continuation or {}).get("coverage_status", "")).upper()
            or None
        )
        discovery_exclusions.extend(
            dict(item)
            for item in (
                (self._discovery_continuation or {}).get("suitability_exclusions", ())
                if isinstance(self._discovery_continuation, Mapping)
                else ()
            )
            if isinstance(item, Mapping)
        )
        # A scope-bearing candidate has already consumed the one shared public
        # inventory pass above.  Never append unqualified inventory to its
        # schedule; this is what prevents research-only/invalid scopes from
        # widening into live collection authority.
        if (
            not configured
            and not scope_candidate_set
            and not self._rolling_scope_blocked
            and remaining > 0
            and self.config.discovery_budget_per_cycle > 0
        ):
            try:
                discovered_values, next_cursor, deferred = self._discover_markets(
                    started,
                    counters,
                    budget=min(remaining, self.config.discovery_budget_per_cycle),
                    carry_cursor=discovery_cursor,
                    exclude=set(known_candidate) | set(paper_markets),
                )
                for snapshot in discovered_values:
                    if (
                        isinstance(snapshot, PredictionMarketSnapshot)
                        and snapshot.market_id not in known_candidate
                        and snapshot.market_id not in paper_markets
                    ):
                        discovered[snapshot.market_id] = snapshot
                discovery_scheduled = list(discovered)
                discovery_deferred = [
                    identifier
                    for identifier in deferred
                    if identifier not in known_candidate and identifier not in paper_markets
                ]
                discovery_coverage_status = (
                    str((self._discovery_continuation or {}).get("coverage_status", "")).upper()
                    or ("PARTIAL" if next_cursor is not None else "COMPLETE")
                )
                discovery_cursor = next_cursor
            except Exception as exc:
                counters["errors"] += 1
                discovery_coverage_status = "ERROR"
                self.store.save_collection_error(None, started, "discovery", str(exc))
                discovery_exclusions.extend(
                    dict(item)
                    for item in self._suitable_market_evidence
                    if isinstance(item, Mapping)
                    and str(item.get("action", "")).upper() == "UNSUITABLE"
                )
                discovery_exclusions = list(
                    {
                        str(item.get("market_id", "")): item
                        for item in discovery_exclusions
                        if item.get("market_id")
                    }.values()
                )[-256:]
        discovery_exclusions.extend(
            dict(item)
            for item in self._suitable_market_evidence
            if isinstance(item, Mapping)
            and str(item.get("action", "")).upper() == "UNSUITABLE"
        )
        discovery_exclusions = list(
            {
                str(item.get("market_id", "")): item
                for item in discovery_exclusions
                if item.get("market_id")
            }.values()
        )[-256:]
        if (
            not configured
            and not scope_candidate_set
            and not self._rolling_scope_blocked
            and remaining > len(discovery_scheduled)
        ):
            try:
                tracked = self.store.tracked_polymarket_markets(
                    active_only=self.config.active,
                    now=started,
                    include_payload=True,
                )
                tracked_ids = [
                    str(item.get("market_id"))
                    for item in tracked
                    if isinstance(item, Mapping) and item.get("market_id")
                ]
                for identifier in tracked_ids:
                    if (
                        identifier not in discovery_scheduled
                        and identifier not in known_candidate
                        and identifier not in paper_markets
                        and len(discovery_scheduled) < min(
                            remaining, self.config.discovery_budget_per_cycle
                        )
                    ):
                        discovery_scheduled.append(identifier)
            except (AttributeError, TypeError, ValueError):
                pass

        tier_by_market: dict[str, str] = {
            market_id: "candidate" for market_id in candidate_scheduled
        }
        tier_by_market.update({market_id: "paper_forward" for market_id in paper_scheduled})
        tier_by_market.update({market_id: "discovery" for market_id in discovery_scheduled})
        planned_ids = list(dict.fromkeys([
            *resume_ids,
            *candidate_scheduled,
            *paper_scheduled,
            *discovery_scheduled,
        ]))[:capacity]
        for market_id in resume_ids:
            tier_by_market.setdefault(market_id, "discovery")

        workers = self._isolated_worker_providers()

        def run_one(identifier: str, worker_provider: Any | None = None) -> tuple[str, dict[str, Any]]:
            local = self._collect_market(
                identifier,
                discovered.get(identifier),
                started,
                point_in_time=now is not None,
                provider=worker_provider if worker_provider is not None else self.provider,
                force=identifier in due_candidates,
            )
            return identifier, local

        def failed_task(identifier: str, exc: Exception) -> tuple[str, dict[str, Any]]:
            local_counters = self._new_counters()
            local_counters["markets_attempted"] = 1
            local_counters["markets_failed"] = 1
            local_counters["errors"] = 1
            try:
                self.store.save_collection_error(identifier, started, "collector", str(exc))
            except Exception:
                # The task's accounting must remain observable even if error
                # persistence itself is unavailable.
                pass
            return identifier, {"counters": local_counters, "attempted": 1}

        results: list[tuple[str, dict[str, Any]]] = []
        completed_identifiers: set[str] = set()
        remaining_after_deadline: list[str] = []
        if workers and len(planned_ids) > 1:
            # Use the collector's fixed daemon pool instead of
            # ``ThreadPoolExecutor``'s context manager.  That context manager
            # unconditionally joins workers during cleanup, which would make
            # a missed provider bypass turn a bounded tick into a permanent
            # hang.
            worker_count = min(self.config.max_concurrency, len(workers), len(planned_ids))
            with self._provider_executor_lock:
                executor = self._collection_executor
                if executor is None:
                    executor = _BoundedProviderExecutor(worker_count)
                    self._collection_executor = executor
            future_tasks: dict[Future[Any], tuple[str, Any]] = {}
            next_index = 0

            def cancel_and_record_remaining() -> None:
                nonlocal remaining_after_deadline
                for future in tuple(future_tasks):
                    future.cancel()
                queued = [
                    identifier
                    for _, (identifier, _provider) in future_tasks.items()
                ]
                remaining_after_deadline = list(dict.fromkeys([
                    *queued,
                    *planned_ids[next_index:],
                ]))

            while next_index < len(planned_ids) or future_tasks:
                remaining = self._cycle_remaining_seconds()
                if remaining is not None and remaining <= 0:
                    self._mark_cycle_exhaustion(self._current_endpoint)
                    cancel_and_record_remaining()
                    break
                while next_index < len(planned_ids) and len(future_tasks) < worker_count:
                    remaining = self._cycle_remaining_seconds()
                    if remaining is not None and remaining <= 0:
                        self._mark_cycle_exhaustion(self._current_endpoint)
                        cancel_and_record_remaining()
                        break
                    identifier = planned_ids[next_index]
                    next_index += 1
                    worker_provider = workers[len(future_tasks) % worker_count]
                    future = executor.submit(
                        lambda i=identifier, p=worker_provider: run_one(i, p)
                    )
                    future_tasks[future] = (identifier, worker_provider)
                if not future_tasks:
                    break
                remaining = self._cycle_remaining_seconds()
                wait_timeout = None if remaining is None else max(0.000001, remaining)
                done, _ = wait(
                    tuple(future_tasks),
                    timeout=wait_timeout,
                    return_when=FIRST_COMPLETED,
                )
                if not done:
                    self._mark_cycle_exhaustion(self._current_endpoint)
                    cancel_and_record_remaining()
                    break
                for future in done:
                    identifier, _worker_provider = future_tasks.pop(future)
                    completed_identifiers.add(identifier)
                    try:
                        results.append(future.result())
                    except Exception as exc:
                        results.append(failed_task(identifier, exc))
                if self._cycle_deadline_exhausted:
                    cancel_and_record_remaining()
                    break
        else:
            for index, identifier in enumerate(planned_ids):
                remaining = self._cycle_remaining_seconds()
                if remaining is not None and remaining <= 0:
                    self._mark_cycle_exhaustion(self._current_endpoint)
                    remaining_after_deadline = list(planned_ids[index:])
                    break
                try:
                    results.append(run_one(identifier))
                    completed_identifiers.add(identifier)
                except Exception as exc:
                    results.append(failed_task(identifier, exc))
                if self._cycle_deadline_exhausted:
                    remaining_after_deadline = list(planned_ids[index + 1:])
                    break

        tier_attempts = {"candidate": 0, "paper_forward": 0, "discovery": 0}
        tier_successes = {"candidate": 0, "paper_forward": 0, "discovery": 0}
        tier_failures = {"candidate": 0, "paper_forward": 0, "discovery": 0}
        for identifier, local in results:
            local_counters = local.get("counters", {})
            self._merge_counters(counters, local_counters)
            tier = tier_by_market.get(identifier, "discovery")
            tier_attempts[tier] += int(local.get("attempted", 0))
            if int(local.get("attempted", 0)):
                if int(local_counters.get("errors", 0)) == 0:
                    tier_successes[tier] += 1
                else:
                    tier_failures[tier] += 1
        ended = ensure_utc(now or self.clock())
        if ended < started:
            ended = started
        final_health = self._required_health(health_requirements, ended, candidate_bound)
        candidate_fresh = list(final_health.get("fresh", candidate_fresh))
        candidate_stale = list(final_health.get("stale", candidate_stale))
        candidate_missing = list(final_health.get("missing", candidate_missing))
        excluded_candidates: list[str] = []
        for requirements in (candidate_requirements, paper_requirements):
            values = requirements.get("capacity_excluded_candidates", ())
            if isinstance(values, (list, tuple, set, frozenset)):
                excluded_candidates.extend(
                    str(candidate).strip()
                    for candidate in values
                    if str(candidate).strip()
                )
        capacity_reason = (
            "COLLECTOR_CAPACITY_INSUFFICIENT"
            if excluded_candidates
            else None
        )
        if capacity_reason is None and due_candidates and len(candidate_scheduled) < len(due_candidates):
            capacity_reason = "COLLECTOR_CAPACITY_INSUFFICIENT"
        discovery_complete = (
            None
            if configured or scope_candidate_set
            else (
                discovery_coverage_status == "COMPLETE"
                if discovery_coverage_status is not None
                else None
            )
        )
        scope_continuation = (
            self._scope_inventory_continuation
            if isinstance(self._scope_inventory_continuation, Mapping)
            else {}
        )
        inventory_coverage = str(
            scope_continuation.get("coverage_status")
            or discovery_coverage_status
            or ""
        ).upper() or None
        scope_bindings = [
            {
                "candidate_id": str(candidate_id),
                "scope_hash": str(proof.get("scope_hash")),
                "scope_version": str(proof.get("scope_version")),
            }
            for candidate_id, proof in self._scope_resolutions.items()
            if isinstance(proof, Mapping)
            and proof.get("scope_hash")
            and proof.get("scope_version")
        ]
        market_authorization = {
            "status": (
                "VERIFIED_MARKET_AUTHORIZED"
                if scope_continuation.get("verified_market_ids")
                else "OBSERVATION_ONLY"
            ),
            "verified_market_ids": list(scope_continuation.get("verified_market_ids", ())),
            "coverage_status": inventory_coverage,
            "scope_bindings": scope_bindings,
        }
        suitable_deferred = tuple(
            item for item in discovery_deferred
            if item not in discovery_scheduled
        )
        deadline_stage = self._current_stage
        deadline_endpoint = self._current_endpoint
        if self._cycle_deadline_exhausted:
            self._cycle_last_remaining_market_ids = tuple(dict.fromkeys(
                str(item).strip()
                for item in remaining_after_deadline
                if str(item).strip()
            ))[:_MAX_CYCLE_CONTINUATION_IDS]
        cycle_continuation = (
            {
                "status": "DEGRADED",
                "retryable": True,
                "resolver": "retry_provider_call",
                "next_action": "retry_next_collection_tick",
                "timeout_reason": "COLLECTOR_CYCLE_DEADLINE_EXCEEDED",
                "stage": deadline_stage,
                "endpoint": deadline_endpoint,
                "timeout_endpoint": deadline_endpoint,
                "remaining_market_ids": list(self._cycle_last_remaining_market_ids),
                "scope_cursor": scope_cursor,
                "discovery_cursor": discovery_cursor,
                "updated_at": ended.isoformat(),
            }
            if self._cycle_deadline_exhausted
            else None
        )
        final_stage = (
            "degraded"
            if self._provider_timeout_evidence or self._cycle_deadline_exhausted
            else "cycle_complete"
        )
        self._set_current_stage(final_stage, self._current_endpoint, ended)
        cycle = CollectionCycle(
            started,
            ended,
            len(planned_ids),
            elapsed_seconds=max(0.0, time.monotonic() - monotonic_started),
            candidate_bound_markets=tuple(candidate_bound),
            candidate_bound_scheduled=tuple(candidate_scheduled),
            candidate_bound_fresh=tuple(candidate_fresh),
            candidate_bound_stale=tuple(candidate_stale),
            candidate_bound_missing=tuple(candidate_missing),
            paper_forward_markets=tuple(paper_markets),
            paper_forward_scheduled=tuple(paper_scheduled),
            discovery_scheduled=tuple(discovery_scheduled),
            discovery_deferred=tuple(discovery_deferred),
            candidate_references=candidate_references,
            tier_attempts=tier_attempts,
            tier_successes=tier_successes,
            tier_failures=tier_failures,
            request_latency_summary=self._latency_summary(counters.pop("_request_latencies", [])),
            suitable_market_scheduled=tuple(discovery_scheduled),
            suitable_market_deferred=suitable_deferred,
            discovery_exclusions=tuple(discovery_exclusions),
            inventory_coverage=inventory_coverage,
            market_authorization=market_authorization,
            capacity_reason=capacity_reason,
            discovery_coverage_status=discovery_coverage_status,
            discovery_cursor=(
                str(discovery_cursor)
                if isinstance(discovery_cursor, str) and discovery_cursor.strip()
                else None
            ),
            discovery_complete=discovery_complete,
            current_stage=self._current_stage,
            current_endpoint=self._current_endpoint,
            provider_timeout_evidence=tuple(
                counters.pop("_provider_timeout_evidence", ())
            ),
            **counters,
        )
        cycle_payload = cycle.as_record()
        cycle_id = "cycle-" + hashlib.sha256(
            f"{self.config.collector_name}|{started.isoformat()}|{ended.isoformat()}|{_stable_payload(cycle_payload)}".encode("utf-8")
        ).hexdigest()
        self.store.save_collection_cycle(
            cycle_id,
            self.config.collector_name,
            cycle_payload,
            started_at=started,
            ended_at=ended,
        )
        try:
            forward_snapshots = self.store.load_polymarket_snapshots(start=started, end=ended)
            source_times = [
                item.get("source_timestamp")
                for item in forward_snapshots
                if isinstance(item, Mapping) and item.get("source_timestamp") is not None
            ]
            source_times = [ensure_utc(item) for item in source_times]
            forward_quality = (
                ResearchQuality.ORDER_BOOK_SIMULATED.value
                if any(
                    str(item.get("quality", "")) == ResearchQuality.ORDER_BOOK_SIMULATED.value
                    for item in forward_snapshots if isinstance(item, Mapping)
                )
                else ResearchQuality.PRICE_PROXY.value
            )
            self.store.save_dataset_catalog(
                "Polymarket-forward-orderbook",
                cycle_id,
                provider=str(getattr(self.provider, "provider_name", self.provider.__class__.__name__)),
                instrument="POLYMARKET",
                market_type=MarketType.PREDICTION,
                timeframe="live",
                start_timestamp=min(source_times) if source_times else None,
                row_count=len(forward_snapshots),
                completeness=(
                    1.0
                    if counters["errors"] == 0 and discovery_complete is not False
                    else (
                        0.0
                        if discovery_complete is False
                        else max(
                            0.0,
                            1.0 - counters["errors"] / max(1, len(planned_ids)),
                        )
                    )
                ),
                missing_ranges=(),
                quality=forward_quality,
                source_type="FORWARD_COLLECTED",
                snapshot_id=cycle_id,
                metadata={
                    "collector": self.config.collector_name,
                    "cycle_id": cycle_id,
                    "markets_seen": len(planned_ids),
                    "collection_cycle": cycle_payload,
                    "discovery_coverage_status": discovery_coverage_status,
                    "discovery_complete": discovery_complete,
                    "live_execution": False,
                },
            )
        except (AttributeError, TypeError, ValueError):
            pass
        self.store.set_collector_state(
            self.config.collector_name,
            {
                **dict(root_state),
                "last_cycle_started_at": started.isoformat(),
                "last_cycle_ended_at": ended.isoformat(),
                "last_cycle_duration_seconds": cycle.duration_seconds,
                "configured_interval_seconds": self.config.interval_seconds,
                "scheduled_market_ids": planned_ids,
                "markets_seen": len(planned_ids),
                "stale_after_seconds": self.config.stale_after_seconds,
                "discovery_carry_cursor": discovery_cursor,
                "discovery_continuation": self._discovery_continuation,
                "discovery_coverage_status": discovery_coverage_status,
                "discovery_complete": discovery_complete,
                "scope_discovery_carry_cursor": scope_cursor,
                "scope_inventory_continuation": self._scope_inventory_continuation,
                "scope_direct_lookup_cursor": max(0, min(
                    _MAX_SCOPE_INVENTORY,
                    int(self._scope_direct_lookup_cursor),
                )),
                "scope_direct_priority_lookup_cursor": max(
                    0,
                    int(self._scope_direct_priority_lookup_cursor),
                ),
                "scope_direct_protected_lookup_cursor": max(
                    0,
                    int(self._scope_direct_protected_lookup_cursor),
                ),
                "scope_resolution_candidate_cursor": max(
                    0,
                    int(self._scope_resolution_candidate_cursor),
                ),
                "scope_resolution_deferred_cursor": max(
                    0,
                    int(self._scope_resolution_deferred_cursor),
                ),

                "scope_resolution_deferred_candidate_ids": list(
                    self._scope_resolution_deferred_candidate_ids
                )[:_MAX_SCOPE_RESOLUTION_CANDIDATES],
                "observation_materialization_deferred_candidate_ids": list(
                    self._observation_materialization_deferred_candidate_ids
                )[:_MAX_SCOPE_RESOLUTION_CANDIDATES],
                "observation_materialization_cursor": max(
                    0,
                    int(self._observation_materialization_cursor),
                ),
                "observation_materialization_turn": int(
                    self._observation_materialization_turn
                ) % 2,
                "scope_handoff_fast_streak": max(
                    0,
                    int(self._scope_handoff_fast_streak),
                ),
                "suitable_market_scheduled": list(discovery_scheduled),
                "suitable_market_deferred": list(suitable_deferred),
                "discovery_exclusions": [dict(item) for item in discovery_exclusions],
                "suitable_market_evidence": [dict(item) for item in self._suitable_market_evidence[-256:]],
                "inventory_coverage": inventory_coverage,
                "market_authorization": market_authorization,
                "candidate_bound_markets": list(candidate_bound),
                "candidate_bound_scheduled": list(candidate_scheduled),
                "candidate_bound_fresh": list(candidate_fresh),
                "candidate_bound_stale": list(candidate_stale),
                "candidate_bound_missing": list(candidate_missing),
                "paper_forward_markets": list(paper_markets),
                "paper_forward_scheduled": list(paper_scheduled),
                "discovery_scheduled": list(discovery_scheduled),
                "discovery_deferred": list(discovery_deferred),
                "candidate_references": candidate_references,
                "current_stage": cycle.current_stage,
                "current_endpoint": cycle.current_endpoint,
                "current_stage_at": ended.isoformat(),
                "tier_attempts": tier_attempts,
                "tier_successes": tier_successes,
                "tier_failures": tier_failures,
                "request_latency_summary": cycle.request_latency_summary,
                "provider_timeout_seconds": self.config.provider_timeout_seconds,
                "provider_timeout_evidence": [
                    dict(item) for item in cycle.provider_timeout_evidence
                ],
                "capacity_reason": capacity_reason,
                "cycle_deadline_seconds": self.config.cycle_budget_seconds,
                "cycle_continuation": cycle_continuation,
                **counters,
            },
        )
        return cycle
    @staticmethod
    def _new_counters() -> dict[str, Any]:
        return {
            "markets_attempted": 0,
            "markets_successful": 0,
            "markets_failed": 0,
            "metadata_inserted": 0,
            "snapshots_inserted": 0,
            "snapshot_duplicates": 0,
            "trades_inserted": 0,
            "trade_duplicates": 0,
            "errors": 0,
            "requests": 0,
            "rate_limits": 0,
            "retries": 0,
            "provider_failures": 0,
            "provider_timeouts": 0,
            "cooldowns": 0,
            "skipped_markets": 0,
            "metadata_failures": 0,
            "order_book_failures": 0,
            "trade_failures": 0,
            "_request_latencies": [],
            "_provider_timeout_evidence": [],

        }
    @staticmethod
    def _merge_counters(target: dict[str, Any], source: Mapping[str, Any]) -> None:
        for key in (
            "markets_attempted", "markets_successful", "markets_failed",
            "metadata_inserted", "snapshots_inserted", "snapshot_duplicates",
            "trades_inserted", "trade_duplicates", "errors", "requests",
            "rate_limits", "retries", "provider_failures", "provider_timeouts",
            "cooldowns", "skipped_markets", "metadata_failures",
            "order_book_failures", "trade_failures",
        ):
            target[key] = int(target.get(key, 0)) + int(source.get(key, 0))
        target.setdefault("_request_latencies", []).extend(
            float(value) for value in source.get("_request_latencies", ())
        )
        target.setdefault("_provider_timeout_evidence", []).extend(
            dict(item)
            for item in source.get("_provider_timeout_evidence", ())
            if isinstance(item, Mapping)
        )

    @staticmethod
    def _latency_summary(values: Sequence[float]) -> dict[str, Any]:
        samples = sorted(float(value) for value in values if math.isfinite(float(value)))
        if not samples:
            return {"count": 0, "min_seconds": None, "max_seconds": None, "mean_seconds": None, "p50_seconds": None}
        middle = samples[len(samples) // 2] if len(samples) % 2 else (samples[len(samples) // 2 - 1] + samples[len(samples) // 2]) / 2.0
        return {
            "count": len(samples),
            "min_seconds": samples[0],
            "max_seconds": samples[-1],
            "mean_seconds": sum(samples) / len(samples),
            "p50_seconds": middle,
        }

    def _candidate_requirements(
        self,
        now: datetime,
        *,
        candidate_ids: Sequence[str] | None = None,
    ) -> Mapping[str, Any]:
        method = getattr(self.store, "candidate_forward_requirements", None)
        if not callable(method):
            return {}
        kwargs = {
            "now": now,
            "max_total_markets": max(100, self.config.max_markets),
        }
        if candidate_ids is not None:
            kwargs["candidate_ids"] = tuple(candidate_ids)
        try:
            result = method(**kwargs)
        except TypeError:
            if candidate_ids is not None:
                return {}
            # Preserve compatibility with small test stores exposing only the
            # original no-argument authority method.
            try:
                result = method(now=now)
            except (TypeError, ValueError):
                result = method()
        return result if isinstance(result, Mapping) else {}

    def _rolling_scope_market_ids(self) -> list[str]:
        """Return bounded market ids from the current rolling scope authority.

        Exact scopes are already explicit.  Rule scopes are never interpreted
        by the collector: their immutable, current-market resolution is loaded
        using the frozen candidate binding persisted with the research
        artifact.  A selected member whose scope cannot be proven therefore
        blocks rolling discovery rather than widening into the public catalog.
        """
        self._rolling_scope_blocked = False
        self._rolling_scope_candidate_ids = ()
        self._rolling_scope_documents = {}
        superseded_candidate_ids, _ = self._superseded_observation_ids()
        selection_loader = getattr(self.store, "load_current_portfolio_selection", None)

        if not callable(selection_loader):
            return []
        try:
            selection = selection_loader()
        except Exception:
            self._rolling_scope_blocked = True
            return []
        if selection is None:
            return []
        if not isinstance(selection, Mapping):
            self._rolling_scope_blocked = True
            return []
        members = selection.get("members", selection.get("selected_members", ()))
        if not isinstance(members, (list, tuple)):
            self._rolling_scope_blocked = True
            return []
        if not members:
            self._rolling_scope_blocked = True
            return []

        def normalized_ids(value: Any) -> list[str]:
            if isinstance(value, str):
                value = (value,)
            if not isinstance(value, (list, tuple, set, frozenset)):
                return []
            return list(dict.fromkeys(str(item).strip() for item in value if str(item).strip()))


        def consistent_text(documents: Sequence[Mapping[str, Any]], names: Sequence[str]) -> tuple[str | None, bool]:
            values: list[str] = []
            for document in documents:
                for name in names:
                    value = document.get(name)
                    if value is not None and str(value).strip():
                        values.append(str(value).strip())
            unique = list(dict.fromkeys(values))
            return (unique[0] if unique else None), len(unique) > 1

        def resolve_rule(
            candidate_id: str | None,
            scope_hash: str | None,
            scope_version: str | None,
        ) -> list[str] | None:
            if not candidate_id or not scope_hash or not scope_version:
                return None
            loader = getattr(self.store, "load_market_scope_resolution", None)
            if not callable(loader):
                return None
            try:
                try:
                    resolution = loader(
                        candidate_id,
                        scope_hash=scope_hash,
                        scope_version=scope_version,
                    )
                except TypeError:
                    resolution = loader(candidate_id)
            except Exception:
                return None
            try:
                if hasattr(resolution, "as_dict") and callable(resolution.as_dict):
                    resolution = resolution.as_dict()
            except Exception:
                return None
            if not isinstance(resolution, Mapping):
                return None
            if (
                str(resolution.get("candidate_id", "")).strip() != candidate_id
                or str(resolution.get("scope_hash", "")).strip() != scope_hash
                or str(
                    resolution.get("scope_version", resolution.get("version", ""))
                ).strip()
                != scope_version
                or str(resolution.get("status", "")).strip().upper() != "MATCHED"
            ):
                return None
            matched = resolution.get("matched_markets", _UNSET)
            if matched is _UNSET:
                return None
            ids: list[str] = []
            if isinstance(matched, (list, tuple, set, frozenset)):
                for market in matched:
                    if isinstance(market, Mapping):
                        value = market.get("market_id", market.get("id"))
                    else:
                        value = getattr(market, "market_id", market)
                    text = str(value).strip() if value is not None else ""
                    if text:
                        ids.append(text)
            return list(dict.fromkeys(ids)) if ids else None

        result: list[str] = []
        connection = getattr(self.store, "connection", None)
        execute = getattr(connection, "execute", None)
        if not callable(execute):
            self._rolling_scope_blocked = True

        for member in members:
            if not isinstance(member, Mapping):
                self._rolling_scope_blocked = True
                continue
            strategy_version_id = str(member.get("strategy_version_id", "")).strip()
            if not strategy_version_id:
                self._rolling_scope_blocked = True
                continue
            payload: Any = None
            if callable(execute):
                try:
                    row = execute(
                        "SELECT payload_json FROM strategy_versions WHERE strategy_version_id=?",
                        (strategy_version_id,),
                    ).fetchone()
                except Exception:
                    row = None
                if row is not None:
                    try:
                        payload = json.loads(row[0]) if isinstance(row[0], str) else row[0]
                    except Exception:
                        payload = None
            else:
                for name in (
                    "payload",
                    "frozen_document",
                    "strategy_document",
                    "experiment_plan",
                    "market_scope",
                ):
                    value = member.get(name)
                    if isinstance(value, Mapping):
                        payload = member if name in {"experiment_plan", "market_scope"} else value
                        break
                if payload is None and self._has_scope_material(member):
                    payload = member
            if not isinstance(payload, Mapping):
                self._rolling_scope_blocked = True
                continue


            documents: list[Mapping[str, Any]] = [payload]
            for name in (
                "payload",
                "frozen_document",
                "strategy_document",
                "experiment_plan",
                "market_scope",
                "provenance",
                "strategy",
                "canonical_strategy",
            ):
                value = payload.get(name)
                if isinstance(value, Mapping):
                    documents.append(value)
            candidate_values = [
                str(value).strip()
                for value in (member.get("candidate_id"), *[
                    document.get("candidate_id") for document in documents
                ])
                if value is not None and str(value).strip()
            ]
            candidate_ids = list(dict.fromkeys(candidate_values))
            if len(candidate_ids) > 1:
                self._rolling_scope_blocked = True
                continue
            candidate_id = candidate_ids[0] if candidate_ids else None
            if candidate_id and candidate_id in superseded_candidate_ids:
                continue
            scope_documents: list[Mapping[str, Any]] = []
            legacy_ids: list[str] = []
            canonical_scope_seen = False
            member_ids: list[str] = []
            member_scope_failed = False
            for document in documents:
                scopes: list[Mapping[str, Any]] = []
                direct_scope = document.get("market_scope")
                if isinstance(direct_scope, Mapping):
                    scopes.append(direct_scope)
                alias_scope = document.get("scope")
                if isinstance(alias_scope, Mapping):
                    scopes.append(alias_scope)
                if str(document.get("mode", "")).strip():
                    scopes.append(document)
                plan = document.get("experiment_plan")
                if isinstance(plan, Mapping) and isinstance(plan.get("market_scope"), Mapping):
                    scopes.append(plan["market_scope"])
                if scopes:
                    scope_documents.extend(scopes)
                else:
                    values = document.get(
                        "market_ids",
                        document.get("markets", document.get("target_market_ids", ())),
                    )
                    legacy_ids.extend(normalized_ids(values))
            if not scope_documents and legacy_ids:
                member_ids.extend(legacy_ids)
            for scope in scope_documents:
                mode = str(scope.get("mode", "")).strip().upper()
                values = scope.get(
                    "market_ids",
                    scope.get("markets", scope.get("target_market_ids", ())),
                )
                if mode == "RULE_BASED_MARKETS":
                    canonical_scope_seen = True
                    scope_hash, hash_conflict = consistent_text(
                        [*documents, scope],
                        ("market_scope_hash", "scope_hash"),
                    )
                    scope_version_values = [
                        document.get(name)
                        for document in [*documents, scope]
                        for name in ("market_scope_version", "scope_version")
                        if document.get(name) is not None and str(document.get(name)).strip()
                    ]
                    scope_version_values.extend(
                        str(scope.get("version")).strip()
                        for _ in (0,)
                        if scope.get("version") is not None and str(scope.get("version")).strip()
                    )
                    scope_version_unique = list(dict.fromkeys(str(value).strip() for value in scope_version_values))
                    scope_version = scope_version_unique[0] if scope_version_unique else None
                    version_conflict = len(scope_version_unique) > 1
                    if hash_conflict or version_conflict:
                        member_scope_failed = True
                        continue
                    resolved = resolve_rule(candidate_id, scope_hash, scope_version)
                    if resolved is None:
                        member_scope_failed = True
                    else:
                        member_ids.extend(resolved)
                elif mode == "EXACT_MARKETS":
                    canonical_scope_seen = True
                    exact_ids = normalized_ids(values)
                    if exact_ids:
                        member_ids.extend(exact_ids)
                    else:
                        member_scope_failed = True
                elif mode == "RESEARCH_ONLY":
                    canonical_scope_seen = True
                    member_scope_failed = True
                elif not mode:
                    legacy_ids.extend(normalized_ids(values))
                else:
                    member_scope_failed = True
            if member_scope_failed or (canonical_scope_seen and not member_ids):
                self._rolling_scope_blocked = True
                continue
            if not canonical_scope_seen:
                member_ids.extend(legacy_ids)
            exact_scope_seen = any(
                str(scope.get("mode", "")).strip().upper() == "EXACT_MARKETS"
                for scope in scope_documents
            )
            if exact_scope_seen and candidate_id:
                self._rolling_scope_candidate_ids = tuple(dict.fromkeys([
                    *self._rolling_scope_candidate_ids,
                    candidate_id,
                ]))
                self._rolling_scope_documents[candidate_id] = self._scope_document(payload)
            result.extend(member_ids)
        return list(dict.fromkeys(result))[: self.config.max_markets]


    def _active_primary_candidate_ids(
        self,
        superseded_candidate_ids: set[str] | None = None,
    ) -> list[str] | None:
        """Return the currently selected/eligible ranking universe.

        PAPER_FORWARD is not a disqualifier here: a paper candidate can be
        ranked and therefore remain in tier one.  Unranked paper candidates
        are discovered separately as tier two.
        """
        if superseded_candidate_ids is None:
            superseded_candidate_ids, _ = self._superseded_observation_ids()
        lock = getattr(self.store, "_lock", None)
        with lock if lock is not None else nullcontext():
            try:
                connection = self.store.connection
                rows = connection.execute(
                    "SELECT candidate_id FROM canary_rankings "
                    "WHERE selected=1 ORDER BY rank,candidate_id LIMIT 1000"
                ).fetchall()
                ranked = [
                    str(row[0]).strip()
                    for row in rows
                    if str(row[0]).strip()
                    and str(row[0]).strip() not in superseded_candidate_ids
                ]
                if ranked:
                    return list(dict.fromkeys(ranked))
            except Exception:
                pass
            try:
                connection = self.store.connection
                rows = connection.execute(
                    "SELECT candidate_id FROM canary_eligibility ORDER BY eligible_at,candidate_id LIMIT 1000"
                ).fetchall()
                eligible = [
                    str(row[0]).strip()
                    for row in rows
                    if str(row[0]).strip()
                    and str(row[0]).strip() not in superseded_candidate_ids
                ]
                if eligible:
                    return list(dict.fromkeys(eligible))
            except Exception:
                pass
        return []
    @staticmethod
    def _requirement_markets(requirements: Mapping[str, Any]) -> list[str]:
        values = requirements.get("market_ids", ())
        if not isinstance(values, (list, tuple, set, frozenset)):
            bound = requirements.get("candidate_bound_markets", ())
            if isinstance(bound, Mapping):
                values = [
                    item
                    for group in bound.values()
                    for item in group
                    if isinstance(group, (list, tuple, set, frozenset))
                ]
            else:
                values = bound
        return list(dict.fromkeys(str(item).strip() for item in values if str(item).strip()))

    @staticmethod
    def _requirement_references(requirements: Mapping[str, Any]) -> dict[str, list[str]]:
        refs = requirements.get("candidate_references", {})
        if not isinstance(refs, Mapping):
            return {}
        return {
            str(market_id): list(dict.fromkeys(str(candidate).strip() for candidate in values if str(candidate).strip()))
            for market_id, values in refs.items()
            if isinstance(values, (list, tuple, set, frozenset))
        }

    def _required_health(
        self,
        requirements: Mapping[str, Any],
        now: datetime,
        fallback_markets: Sequence[str],
    ) -> Mapping[str, Any]:
        method = getattr(self.store, "polymarket_required_health", None)
        if callable(method) and requirements:
            stale_after = self.config.freshness_sla_seconds or self.config.stale_after_seconds or self.config.interval_seconds
            try:
                try:
                    result = method(
                        requirements=requirements,
                        scheduled_market_ids=(),
                        now=now,
                        stale_after_seconds=stale_after,
                        max_markets=self.config.max_markets,
                    )
                except TypeError:
                    # Preserve compatibility with lightweight stores exposing
                    # the pre-cap health signature.
                    result = method(
                        requirements=requirements,
                        scheduled_market_ids=(),
                        now=now,
                        stale_after_seconds=stale_after,
                    )
                if isinstance(result, Mapping):
                    return result
            except (AttributeError, TypeError, ValueError):
                pass
        return {
            "fresh": [],
            "stale": [],
            "missing": list(fallback_markets),
        }

    def _superseded_observation_ids(self) -> tuple[set[str], set[str]]:
        try:
            intents = ForwardTestRegistry(self.store).list_observation_intents()
        except (AttributeError, TypeError, ValueError, RuntimeError):
            return set(), set()
        return self._observation_superseded_ids(intents)

    def _active_paper_forward_ids(
        self,
        superseded_candidate_ids: set[str] | None = None,
    ) -> list[str]:
        if superseded_candidate_ids is None:
            superseded_candidate_ids, _ = self._superseded_observation_ids()
        method = getattr(self.store, "load_candidate_lifecycle", None)
        if not callable(method):
            return []
        try:
            records = method(limit=1000)
        except (TypeError, ValueError):
            records = method()
        if not isinstance(records, list):
            return []
        return list(dict.fromkeys(
            str(row.get("candidate_id")).strip()
            for row in records
            if isinstance(row, Mapping)
            and str(row.get("stage", "")).strip().upper() in {"PAPER_FORWARD", "PAPER_PROMOTABLE"}
            and str(row.get("candidate_id", "")).strip()
            and str(row.get("candidate_id", "")).strip() not in superseded_candidate_ids
        ))

    @staticmethod
    def _observation_superseded_ids(
        intents: Sequence[Any],
    ) -> tuple[set[str], set[str]]:
        superseded_candidates: set[str] = set()
        superseded_intents: set[str] = set()
        for spec in intents:
            config = spec.config if isinstance(spec.config, Mapping) else {}
            handoff = config.get("observation_handoff")
            if not isinstance(handoff, Mapping):
                continue
            predecessor_candidate_id = str(
                handoff.get("predecessor_candidate_id", "")
            ).strip()
            predecessor_intent_id = str(
                handoff.get("predecessor_observation_intent_id", "")
            ).strip()
            if predecessor_candidate_id:
                superseded_candidates.add(predecessor_candidate_id)
            if predecessor_intent_id:
                superseded_intents.add(predecessor_intent_id)
        return superseded_candidates, superseded_intents
    def _active_observation_intent_ids(
        self,
        superseded_candidate_ids: set[str] | None = None,
        superseded_intent_ids: set[str] | None = None,
    ) -> list[str]:
        intents = ForwardTestRegistry(self.store).list_observation_intents()
        discovered_candidates, discovered_intents = self._observation_superseded_ids(intents)
        superseded_candidate_ids = (
            discovered_candidates
            if superseded_candidate_ids is None
            else superseded_candidate_ids
        )
        superseded_intent_ids = (
            discovered_intents
            if superseded_intent_ids is None
            else superseded_intent_ids
        )
        result: list[str] = []
        for spec in intents:
            config = spec.config if isinstance(spec.config, Mapping) else {}
            candidate_id = str(config.get("candidate_id", "")).strip()
            if (
                candidate_id
                and candidate_id not in superseded_candidate_ids
                and str(spec.experiment_id).strip() not in superseded_intent_ids
            ):
                result.append(candidate_id)
        return list(dict.fromkeys(result))
    @staticmethod
    def _observation_intent_requires_handoff(intent: Any) -> bool:
        config = getattr(intent, "config", None)
        if not isinstance(config, Mapping):
            return False
        return (
            config.get("observation_only_lineage") is True
            or isinstance(config.get("observation_handoff"), Mapping)
        )
    def _observation_lifecycle_requires_reconcile(self, candidate_id: str) -> bool:
        loader = getattr(self.store, "load_candidate_lifecycle", None)
        if not callable(loader):
            return False
        try:
            record = loader(candidate_id)
        except (AttributeError, KeyError, TypeError, ValueError, RuntimeError):
            return False
        if not isinstance(record, Mapping):
            return False
        stage = str(record.get("stage", "")).strip().upper()
        if stage in {CandidateStage.IDEA.value, CandidateStage.SCHEMA_VALIDATED.value}:
            return True
        if stage not in {
            CandidateStage.PAPER_FORWARD.value,
            CandidateStage.PAPER_PROMOTABLE.value,
        }:
            return True
        return True

    def _reconcile_materialized_observation_lifecycle(
        self,
        candidate_id: str,
        intent: Any,
        spec: ForwardTestSpec,
        markets: Sequence[str],
        scope_resolution: Any,
        observed_at: datetime,
    ) -> bool:
        proof = (
            scope_resolution
            if isinstance(scope_resolution, Mapping)
            else (
                scope_resolution.as_dict()
                if hasattr(scope_resolution, "as_dict")
                and callable(scope_resolution.as_dict)
                else None
            )
        )
        if not isinstance(proof, Mapping):
            raise ValueError("observation materialization requires persisted scope proof")
        matched_ids = tuple(self._scope_result_market_ids(proof))
        allowed_ids = tuple(str(item).strip() for item in markets if str(item).strip())
        if (
            str(proof.get("status", "")).strip().upper() != "MATCHED"
            or matched_ids != allowed_ids
            or tuple(spec.allowed_markets) != allowed_ids
        ):
            raise ValueError("observation materialization scope is not the verified current match")
        manager = CandidateLifecycleManager(self.store)
        current = manager.get(candidate_id)
        if current is None:
            raise ValueError("observation materialization lifecycle is missing")
        config = dict(spec.config) if isinstance(spec.config, Mapping) else {}
        evidence = dict(current.payload)
        for field in (
            "experiment_plan",
            "plan_id",
            "plan_hash",
            "market_scope",
            "market_scope_hash",
            "market_scope_version",
            "dataset_selector",
            "dataset_attestation",
        ):
            value = config.get(field)
            if value in (None, "", {}, []):
                continue
            existing = evidence.get(field)
            if existing not in (None, "", {}, []) and _stable_payload(existing) != _stable_payload(value):
                raise ValueError(f"observation lifecycle binding conflicts for {field}")
            evidence[field] = value
        intent_id = str(getattr(intent, "experiment_id", "") or "").strip()
        if not intent_id:
            raise ValueError("observation materialization intent identity is missing")
        evidence.update(
            {
                "schema_valid": True,
                "paper_observation_intent_id": intent_id,
                "paper_observation_intent": True,
                "paper_only": True,
                "research_only": True,
                "execution_scope": "OBSERVATION",
                "observation_only_lineage": True,
                "selection_excluded": True,
                "allocation_active": False,
                "canary_armed": False,
                "scope_resolution": dict(proof),
                "market_scope_resolution": dict(proof),
            }
        )
        if current.stage is CandidateStage.IDEA:
            manager.advance(
                candidate_id,
                CandidateStage.SCHEMA_VALIDATED,
                evidence,
                reason="current observation scope verified after schema intent registration",
            )
            return False
        if current.stage is CandidateStage.SCHEMA_VALIDATED:
            forward_evidence = dict(current.payload)
            forward_evidence.update(
                {
                    "paper_forward_started": True,
                    "holdout_used": False,
                    "forward_test_id": spec.experiment_id,
                    "forward_config": config,
                    "allowed_markets": list(allowed_ids),
                    "current_market_ids": list(allowed_ids),
                    "resolved_market_ids": list(allowed_ids),
                    "scope_resolution": dict(proof),
                    "market_scope_resolution": dict(proof),
                    "paper_only": True,
                    "research_only": True,
                    "execution_scope": "OBSERVATION",
                    "observation_only_lineage": True,
                    "selection_excluded": True,
                    "allocation_active": False,
                    "canary_armed": False,
                }
            )
            manager.advance(
                candidate_id,
                CandidateStage.PAPER_FORWARD,
                forward_evidence,
                reason="current observation scope materialized as paper forward",
                observation_only=True,
            )
            return True
        if current.stage in {CandidateStage.PAPER_FORWARD, CandidateStage.PAPER_PROMOTABLE}:
            required_safety = {
                "paper_observation_intent": True,
                "paper_only": True,
                "research_only": True,
                "execution_scope": "OBSERVATION",
                "observation_only_lineage": True,
                "selection_excluded": True,
                "allocation_active": False,
                "canary_armed": False,
            }
            if any(
                type(current.payload.get(key)) is not type(expected)
                or current.payload.get(key) != expected
                for key, expected in required_safety.items()
            ):
                raise ValueError("observation lifecycle paper authority safety is not persisted")
            existing_id = str(current.payload.get("forward_test_id", "")).strip()
            existing_markets = tuple(
                str(item).strip()
                for item in current.payload.get("allowed_markets", ())
                if str(item).strip()
            )
            if (
                existing_id != str(spec.experiment_id).strip()
                or existing_markets != allowed_ids
                or current.payload.get("scope_resolution") != dict(proof)
                or current.payload.get("market_scope_resolution") != dict(proof)
            ):
                evidence = {
                    **dict(current.payload),
                    "forward_test_id": spec.experiment_id,
                    "forward_config": config,
                    "allowed_markets": list(allowed_ids),
                    "current_market_ids": list(allowed_ids),
                    "resolved_market_ids": list(allowed_ids),
                    "scope_resolution": dict(proof),
                    "market_scope_resolution": dict(proof),
                    "paper_forward_started": True,
                    "holdout_used": False,
                    "paper_only": True,
                    "research_only": True,
                    "execution_scope": "OBSERVATION",
                    "observation_only_lineage": True,
                    "selection_excluded": True,
                    "allocation_active": False,
                    "canary_armed": False,
                }
                manager.record_evidence(
                    candidate_id,
                    evidence,
                    expected_stage=current.stage,
                    reason="observation materialization lifecycle reconciliation",
                )
            return True
        raise ValueError(f"observation lifecycle cannot advance from {current.stage.value}")
    def _scope_persisted_proof_restore_allowed(self) -> bool:
        continuation = self._scope_inventory_continuation
        if not isinstance(continuation, Mapping):
            return True
        if continuation.get("query_reset") or continuation.get("rebase_required"):
            return False
        if continuation.get("scope_resolution_integrity_error"):
            return False
        request_fingerprint = str(continuation.get("request_fingerprint", "")).strip()
        expected_fingerprint = str(continuation.get("expected_query_fingerprint", "")).strip()
        return not (
            request_fingerprint
            and expected_fingerprint
            and request_fingerprint != expected_fingerprint
        )


    def _restore_current_observation_scope_proofs(
        self,
        observed_at: datetime,
        candidate_ids: Sequence[str],
        scope_candidate_ids: list[str],
        candidate_markets: dict[str, list[str]],
        scope_resolutions: dict[str, Any],
    ) -> None:
        loader = getattr(self.store, "load_market_scope_resolution", None)
        lifecycle_loader = getattr(self.store, "load_candidate_lifecycle", None)
        if not callable(loader):
            return
        try:
            intents = ForwardTestRegistry(self.store).list_observation_intents()
        except (AttributeError, KeyError, RuntimeError, TypeError, ValueError):
            return
        intents_by_candidate = {
            str((intent.config if isinstance(intent.config, Mapping) else {}).get("candidate_id", "")).strip(): intent
            for intent in intents
        }
        observed = ensure_utc(observed_at)
        freshness = self.config.freshness_sla_seconds or self.config.interval_seconds
        try:
            max_age = max(1.0, float(freshness))
        except (TypeError, ValueError):
            max_age = 60.0
        for candidate_id in candidate_ids:
            candidate_id = str(candidate_id).strip()
            if not candidate_id or candidate_id in candidate_markets:
                continue
            intent = intents_by_candidate.get(candidate_id)
            config = intent.config if intent is not None and isinstance(intent.config, Mapping) else {}
            scope_hash = str(config.get("market_scope_hash", "")).strip()
            scope_version = str(config.get("market_scope_version", "")).strip()
            if (not scope_hash or not scope_version) and callable(lifecycle_loader):
                try:
                    lifecycle = lifecycle_loader(candidate_id)
                except (AttributeError, KeyError, RuntimeError, TypeError, ValueError):
                    lifecycle = None
                payload = lifecycle.get("payload") if isinstance(lifecycle, Mapping) else None
                if isinstance(payload, Mapping):
                    scope_hash = scope_hash or str(payload.get("market_scope_hash", "")).strip()
                    scope_version = scope_version or str(payload.get("market_scope_version", "")).strip()
            if not scope_hash or not scope_version:
                continue
            try:
                proof = loader(
                    candidate_id,
                    scope_hash=scope_hash,
                    scope_version=scope_version,
                )
            except (AttributeError, KeyError, RuntimeError, TypeError, ValueError):
                continue
            if hasattr(proof, "as_dict") and callable(proof.as_dict):
                try:
                    proof = proof.as_dict()
                except (AttributeError, TypeError, ValueError):
                    continue
            if not isinstance(proof, Mapping):
                continue
            if (
                str(proof.get("candidate_id", "")).strip() != candidate_id
                or str(proof.get("scope_hash", "")).strip() != scope_hash
                or str(proof.get("scope_version", proof.get("version", ""))).strip() != scope_version
                or str(proof.get("status", "")).strip().upper() != "MATCHED"
            ):
                continue
            resolved_at = parse_timestamp(proof.get("resolved_at"))
            if resolved_at is None:
                continue
            age = (observed - ensure_utc(resolved_at)).total_seconds()
            if age < 0 or age > max_age:
                continue
            def disposition_ids(
                value: Any,
                *,
                require_reason: bool,
            ) -> list[str] | None:
                if not isinstance(value, (list, tuple)):
                    return None
                result: list[str] = []
                for row in value:
                    if not isinstance(row, Mapping):
                        return None
                    market_id = str(row.get("market_id", row.get("id", ""))).strip()
                    reason = str(row.get("reason", "")).strip()
                    if not market_id or (require_reason and not reason) or market_id in result:
                        return None
                    result.append(market_id)
                return result

            matched_ids = disposition_ids(proof.get("matched_markets"), require_reason=False)
            excluded_ids = disposition_ids(proof.get("excluded_markets"), require_reason=True)
            deferred_ids = disposition_ids(proof.get("deferred_markets"), require_reason=True)
            if (
                not matched_ids
                or excluded_ids is None
                or deferred_ids is None
                or deferred_ids
                or set(matched_ids).intersection(excluded_ids)
            ):
                continue
            candidate_markets[candidate_id] = matched_ids
            scope_resolutions[candidate_id] = proof
            if candidate_id not in scope_candidate_ids:
                scope_candidate_ids.append(candidate_id)


    def _observation_handoff_proof_current(
        self,
        candidate_id: str,
        markets: Sequence[str],
        proof: Any,
        observed_at: datetime,
    ) -> bool:
        if not isinstance(proof, Mapping):
            return False
        if str(proof.get("candidate_id", "")).strip() != str(candidate_id).strip():
            return False
        if str(proof.get("status", "")).strip().upper() != "MATCHED":
            return False
        matched_ids = tuple(self._scope_result_market_ids(proof))
        allowed_ids = tuple(str(item).strip() for item in markets if str(item).strip())
        if not matched_ids or matched_ids != allowed_ids:
            return False
        resolved_at = parse_timestamp(proof.get("resolved_at"))
        if resolved_at is None:
            return False
        freshness = self.config.freshness_sla_seconds or self.config.interval_seconds
        try:
            max_age = max(1.0, float(freshness))
        except (TypeError, ValueError):
            max_age = 60.0
        age = (ensure_utc(observed_at) - ensure_utc(resolved_at)).total_seconds()
        return 0 <= age <= max_age

    def _materialize_observation_intents(
        self,
        observed_at: datetime,
        candidate_ids: Sequence[str],
        candidate_markets: Mapping[str, Sequence[str]],
        counters: dict[str, Any],
        *,
        scope_resolutions: Mapping[str, Any] | None = None,

    ) -> set[str]:
        registry = ForwardTestRegistry(self.store)
        intents = registry.list_observation_intents()
        superseded_candidates, superseded_intents = self._observation_superseded_ids(intents)
        prior_deferred = list(dict.fromkeys(
            candidate_id
            for candidate_id in self._observation_materialization_deferred_candidate_ids
            if candidate_id not in superseded_candidates
        ))
        current_ids = list(dict.fromkeys(
            str(item).strip()
            for item in candidate_ids
            if str(item).strip() and str(item).strip() not in superseded_candidates
        ))
        by_candidate = {
            str(spec.config.get("candidate_id", "")).strip(): spec
            for spec in intents
            if isinstance(spec.config, Mapping)
            and str(spec.config.get("candidate_id", "")).strip()
            and str(spec.config.get("candidate_id", "")).strip()
            not in superseded_candidates
            and str(spec.experiment_id).strip() not in superseded_intents
        }
        ready_ids: set[str] = set()
        if not prior_deferred and not current_ids:
            self._observation_materialization_deferred_candidate_ids = ()
            self._observation_materialization_cursor = 0
            self._observation_materialization_turn = 0
            return ready_ids

        if prior_deferred:
            cursor = self._observation_materialization_cursor % len(prior_deferred)
            deferred_order = prior_deferred[cursor:] + prior_deferred[:cursor]
        else:
            deferred_order = []
        deferred_set = set(prior_deferred)
        pending_current_ids = [
            candidate_id
            for candidate_id in current_ids
            if (
                candidate_id not in deferred_set
                and candidate_id in by_candidate
                and (
                    registry.get("forward-" + candidate_id) is None
                    or self._observation_lifecycle_requires_reconcile(candidate_id)
                )
                and any(
                    str(item).strip()
                    for item in candidate_markets.get(candidate_id, ())
                )
            )
        ]
        turn = self._observation_materialization_turn % 2
        ordered: list[tuple[int, str]] = []
        for offset in range(max(len(deferred_order), len(pending_current_ids))):
            for group in (turn, 1 - turn):
                values = deferred_order if group == 0 else pending_current_ids
                if offset < len(values):
                    ordered.append((group, values[offset]))

        deferred_consumed: set[str] = set()
        deferred_retries: list[str] = []
        current_retries: list[str] = []
        blocked = False
        blocked_group: int | None = None
        blocked_index = 0
        handoff_batch_used = 0
        for index, (group, candidate_id) in enumerate(ordered):
            markets = tuple(
                str(item).strip()
                for item in candidate_markets.get(candidate_id, ())
                if str(item).strip()
            )
            intent = by_candidate.get(candidate_id)
            handoff_fast_path = bool(
                intent is not None
                and markets
                and self._observation_intent_requires_handoff(intent)
                and self._observation_handoff_proof_current(
                    candidate_id,
                    markets,
                    (scope_resolutions or {}).get(candidate_id),
                    observed_at,
                )
            )
            remaining = self._cycle_remaining_seconds()
            if remaining is not None and remaining <= 0:
                self._mark_cycle_exhaustion("observation_materialization")
                blocked = True
                blocked_group = group
                blocked_index = index
                break
            if not self._downstream_collection_budget_available() or (
                not self._scope_pipeline_budget_available()
                and (
                    not handoff_fast_path
                    or handoff_batch_used >= _MAX_OBSERVATION_HANDOFF_BATCH
                )
            ):
                blocked = True
                blocked_group = group
                blocked_index = index
                break
            if handoff_fast_path:
                handoff_batch_used += 1
            intent = by_candidate.get(candidate_id)
            if not markets or intent is None:
                if group == 0:
                    deferred_consumed.add(candidate_id)
                continue
            existing_spec = registry.get("forward-" + candidate_id)
            if (
                existing_spec is not None
                and not self._observation_lifecycle_requires_reconcile(candidate_id)
            ):
                try:
                    lifecycle_ready = True
                    if (
                        isinstance(existing_spec, ForwardTestSpec)
                        and self._observation_intent_requires_handoff(intent)
                    ):
                        lifecycle_ready = self._reconcile_materialized_observation_lifecycle(
                            candidate_id,
                            intent,
                            existing_spec,
                            markets,
                            (scope_resolutions or {}).get(candidate_id),
                            observed_at,
                        )
                except (AttributeError, KeyError, RuntimeError, TypeError, ValueError):
                    counters["errors"] += 1
                    if group == 0:
                        deferred_retries.append(candidate_id)
                    else:
                        current_retries.append(candidate_id)
                    continue
                if lifecycle_ready:
                    ready_ids.add(candidate_id)
                    if group == 0:
                        deferred_consumed.add(candidate_id)
                elif group == 0:
                    deferred_retries.append(candidate_id)
                else:
                    current_retries.append(candidate_id)
                continue
            try:
                transaction_factory = getattr(self.store, "transaction", None)
                transaction = (
                    transaction_factory()
                    if callable(transaction_factory)
                    else nullcontext()
                )
                with transaction:
                    materialized = existing_spec or registry.materialize_observation_intent(
                        intent,
                        allowed_markets=markets[:100],
                        registration_timestamp=observed_at,
                        now=observed_at,
                        candidate_id=candidate_id,
                        scope_resolution=(scope_resolutions or {}).get(candidate_id),
                    )
                    if (
                        isinstance(materialized, ForwardTestSpec)
                        and self._observation_intent_requires_handoff(intent)
                    ):
                        lifecycle_ready = self._reconcile_materialized_observation_lifecycle(
                            candidate_id,
                            intent,
                            materialized,
                            markets,
                            (scope_resolutions or {}).get(candidate_id),
                            observed_at,
                        )
                    else:
                        lifecycle_ready = True
                if lifecycle_ready:
                    ready_ids.add(candidate_id)
                    if group == 0:
                        deferred_consumed.add(candidate_id)
                elif group == 0:
                    deferred_retries.append(candidate_id)
            except (AttributeError, KeyError, RuntimeError, TypeError, ValueError):
                counters["errors"] += 1
                if group == 0:
                    deferred_retries.append(candidate_id)
                else:
                    current_retries.append(candidate_id)
        deferred_next = [
            candidate_id
            for candidate_id in deferred_order
            if candidate_id not in deferred_consumed
            and candidate_id not in deferred_retries
        ]
        self._observation_materialization_deferred_candidate_ids = tuple(
            dict.fromkeys([
                *deferred_next,
                *deferred_retries,
                *current_retries,
            ])
        )[:_MAX_SCOPE_RESOLUTION_CANDIDATES]
        self._observation_materialization_cursor = 0
        if blocked:
            if blocked_index == 0:
                other_group = 1 - (blocked_group or 0)
                has_other_group = bool(
                    deferred_order if other_group == 0 else pending_current_ids
                )
                self._observation_materialization_turn = (
                    other_group if has_other_group else (blocked_group or 0)
                )
            elif blocked_index < len(ordered):
                self._observation_materialization_turn = ordered[blocked_index][0]
            else:
                self._observation_materialization_turn = 1 - (blocked_group or 0)
        elif self._observation_materialization_deferred_candidate_ids:
            self._observation_materialization_turn = 0
        else:
            self._observation_materialization_turn = 1
        return ready_ids

    @staticmethod
    def _scope_proof_semantic_payload(value: Mapping[str, Any]) -> Mapping[str, Any]:
        return {
            key: item
            for key, item in value.items()
            if key not in {"resolved_at", "resolution_id"}
        }

    def _observation_handoff_payload_matches(
        self,
        candidate_id: str,
        intent: ForwardTestSpec,
        spec: ForwardTestSpec,
        payload: Mapping[str, Any],
        proof: Mapping[str, Any],
        current_ids: tuple[str, ...],
    ) -> bool:
        required_safety = {
            "schema_valid": True,
            "paper_observation_intent": True,
            "paper_only": True,
            "research_only": True,
            "paper_forward_started": True,
            "holdout_used": False,
            "execution_scope": "OBSERVATION",
            "observation_only_lineage": True,
            "selection_excluded": True,
            "allocation_active": False,
            "canary_armed": False,
        }
        if any(
            type(payload.get(key)) is not type(expected)
            or payload.get(key) != expected
            for key, expected in required_safety.items()
        ):
            return False
        config = spec.config if isinstance(spec.config, Mapping) else {}
        if str(config.get("candidate_id", "")).strip() != candidate_id:
            return False
        for intent_field in ("observation_intent_id", "paper_observation_intent_id"):
            bound_intent_id = str(config.get(intent_field, "")).strip()
            if bound_intent_id and bound_intent_id != str(intent.experiment_id).strip():
                return False
        if (
            str(payload.get("candidate_id", "")).strip() != candidate_id
            or str(payload.get("paper_observation_intent_id", "")).strip()
            != str(intent.experiment_id).strip()
            or str(payload.get("forward_test_id", "")).strip()
            != str(spec.experiment_id).strip()
        ):
            return False
        forward_config = payload.get("forward_config")
        if (
            not isinstance(forward_config, Mapping)
            or _stable_payload(forward_config) != _stable_payload(config)
        ):
            return False
        for field in (
            "experiment_plan",
            "plan_id",
            "plan_hash",
            "market_scope",
            "market_scope_hash",
            "market_scope_version",
            "dataset_selector",
            "dataset_attestation",
        ):
            expected = config.get(field)
            if expected in (None, "", {}, []):
                continue
            if field not in payload or _stable_payload(payload.get(field)) != _stable_payload(expected):
                return False
        for field in ("strategy_hash", "model_hash"):
            if field in payload:
                expected = getattr(spec, field, None)
                if expected is None or str(payload.get(field)).strip() != str(expected).strip():
                    return False
        payload_ids = tuple(
            str(item).strip()
            for item in payload.get("allowed_markets", ())
            if str(item).strip()
        )
        current_market_ids = tuple(
            str(item).strip()
            for item in payload.get("current_market_ids", ())
            if str(item).strip()
        )
        resolved_market_ids = tuple(
            str(item).strip()
            for item in payload.get("resolved_market_ids", ())
            if str(item).strip()
        )
        if payload_ids != current_ids or current_market_ids != current_ids or resolved_market_ids != current_ids:
            return False
        lifecycle_scope_resolution = payload.get("scope_resolution")
        lifecycle_market_scope_resolution = payload.get("market_scope_resolution")
        if (
            not isinstance(lifecycle_scope_resolution, Mapping)
            or not isinstance(lifecycle_market_scope_resolution, Mapping)
        ):
            return False
        lifecycle_authority = self._scope_proof_semantic_payload(lifecycle_scope_resolution)
        current_authority = self._scope_proof_semantic_payload(proof)
        return (
            _stable_payload(lifecycle_authority) == _stable_payload(current_authority)
            and _stable_payload(
                self._scope_proof_semantic_payload(lifecycle_market_scope_resolution)
            ) == _stable_payload(current_authority)
        )

    def _validated_observation_handoff_ids(
        self,
        candidate_ids: Sequence[str],
        candidate_markets: Mapping[str, Sequence[str]],
        scope_resolutions: Mapping[str, Any],
        *,
        allow_schema: bool = False,
    ) -> set[str]:
        loader = getattr(self.store, "load_candidate_lifecycle", None)
        if not callable(loader):
            return set()
        try:
            intents = ForwardTestRegistry(self.store).list_observation_intents()
        except (AttributeError, KeyError, RuntimeError, TypeError, ValueError):
            return set()
        intents_by_candidate = {
            str(
                (intent.config if isinstance(intent.config, Mapping) else {}).get(
                    "candidate_id", ""
                )
            ).strip(): intent
            for intent in intents
        }
        registry = ForwardTestRegistry(self.store)
        ready: set[str] = set()
        for raw_candidate_id in candidate_ids:
            candidate_id = str(raw_candidate_id).strip()
            intent = intents_by_candidate.get(candidate_id)
            if (
                not candidate_id
                or intent is None
                or not self._observation_intent_requires_handoff(intent)
            ):
                continue
            try:
                lifecycle = loader(candidate_id)
            except (AttributeError, KeyError, RuntimeError, TypeError, ValueError):
                continue
            if not isinstance(lifecycle, Mapping):
                continue
            payload = lifecycle.get("payload")
            stage = str(lifecycle.get("stage", "")).strip().upper()
            proof = scope_resolutions.get(candidate_id)
            if allow_schema and stage == CandidateStage.SCHEMA_VALIDATED.value:
                config = intent.config if isinstance(intent.config, Mapping) else {}
                current_ids = tuple(
                    str(item).strip()
                    for item in candidate_markets.get(candidate_id, ())
                    if str(item).strip()
                )
                matched_ids = self._scope_result_market_ids(proof)
                if (
                    isinstance(payload, Mapping)
                    and isinstance(proof, Mapping)
                    and str(config.get("market_scope_hash", "")).strip()
                    == str(proof.get("scope_hash", "")).strip()
                    and str(config.get("market_scope_version", "")).strip()
                    == str(proof.get("scope_version", "")).strip()
                    and str(proof.get("candidate_id", "")).strip() == candidate_id
                    and str(proof.get("status", "")).strip().upper() == "MATCHED"
                    and tuple(matched_ids) == current_ids
                ):
                    ready.add(candidate_id)
                continue
            forward_test_id = (
                str(payload.get("forward_test_id", "")).strip()
                if isinstance(payload, Mapping)
                else ""
            )
            if not forward_test_id:
                continue
            try:
                spec = registry.get(forward_test_id)
            except (AttributeError, KeyError, RuntimeError, TypeError, ValueError):
                continue
            if spec is None:
                continue
            if str(lifecycle.get("stage", "")).strip().upper() not in {
                CandidateStage.PAPER_FORWARD.value,
                CandidateStage.PAPER_PROMOTABLE.value,
            }:
                continue
            payload = lifecycle.get("payload")
            proof = scope_resolutions.get(candidate_id)
            if not isinstance(payload, Mapping) or not isinstance(proof, Mapping):
                continue
            matched_ids = self._scope_result_market_ids(proof)
            current_ids = tuple(
                str(item).strip()
                for item in candidate_markets.get(candidate_id, ())
                if str(item).strip()
            )
            if (
                str(proof.get("candidate_id", "")).strip() != candidate_id
                or str(proof.get("status", "")).strip().upper() != "MATCHED"
                or tuple(matched_ids) != current_ids
                or tuple(spec.allowed_markets) != current_ids
                or str(payload.get("market_scope_hash", "")).strip()
                != str(proof.get("scope_hash", "")).strip()
                or str(payload.get("market_scope_version", "")).strip()
                != str(proof.get("scope_version", "")).strip()
                or not self._observation_handoff_payload_matches(
                    candidate_id,
                    intent,
                    spec,
                    payload,
                    proof,
                    current_ids,
                )
            ):
                continue

            ready.add(candidate_id)
        return ready

    def _isolated_worker_providers(self) -> list[Any]:
        if self.config.max_concurrency <= 1:
            return []
        factory = getattr(self.provider, "isolated_worker_factory", None)
        if not callable(factory):
            return []
        providers: list[Any] = []
        try:
            for _ in range(min(2, self.config.max_concurrency)):
                worker = factory()
                if callable(worker) and not isinstance(worker, PredictionMarketDataProvider):
                    worker = worker()
                if worker is None or any(worker is existing for existing in providers):
                    return []
                providers.append(worker)
        except Exception:
            return []
        return providers if len(providers) >= 2 else []
    def _scope_direct_lookup_provider(self) -> Any:
        """Create one fresh isolated provider for an exact operation."""
        factory = getattr(self.provider, "isolated_worker_factory", None)
        if callable(factory):
            try:
                worker = factory()
                if callable(worker) and not isinstance(worker, PredictionMarketDataProvider):
                    worker = worker()
                if worker is not None and worker is not self.provider:
                    self._scope_direct_provider = worker
                    self._scope_direct_provider_ids.add(id(worker))
                    self._scope_direct_provider_refs[id(worker)] = worker
                    return worker
            except Exception:
                pass
        return self.provider

    def _scope_provider_pool(self, provider: Any) -> str | None:
        """Use the isolated exact pool for fresh provider clones."""
        if id(provider) in self._scope_direct_provider_ids:
            return "scope_direct"
        return None
    @staticmethod
    def _scope_market_operation(provider: Any, market_id: str) -> Any:
        fetcher = getattr(provider, "scope_market", None)
        if not callable(fetcher):
            fetcher = getattr(provider, "market", None)
        if not callable(fetcher):
            raise RuntimeError("provider has no exact market operation")
        return fetcher(market_id)
    def _close_scope_direct_provider(self, provider: Any) -> None:
        """Drain diagnostics, then close one exact clone exactly once."""
        provider_id = id(provider)
        with self._provider_executor_lock:
            owned = self._scope_direct_provider_refs.get(provider_id)
            if owned is not provider:
                return
            self._scope_direct_provider_refs.pop(provider_id, None)
            self._scope_direct_provider_ids.discard(provider_id)
            if self._scope_direct_provider is provider:
                self._scope_direct_provider = None
        # Providers may invalidate their advisory buffers during close, so
        # diagnostics must be consumed first on both normal and orphan paths.
        self._drain_provider_advisories_now(provider)
        close = getattr(provider, "close", None)
        if callable(close):
            try:
                close()
            except Exception:
                pass
    def _finish_scope_direct_orphan(
        self,
        provider: Any,
        key: tuple[int, str],
    ) -> None:
        self._close_scope_direct_provider(provider)
        self._release_provider_call(key)

    def _resolve_market_scopes(
        self,
        observed_at: datetime,
        candidate_ids: Sequence[str],
        root_state: Mapping[str, Any],
        counters: dict[str, Any],
    ) -> tuple[list[str], dict[str, list[str]], dict[str, PredictionMarketSnapshot], Any]:
        """Resolve all frozen market policies against one shared inventory.

        Scope resolution deliberately happens before the normal candidate and
        paper tiers.  The resolver owns policy parsing, exclusion/defer
        taxonomy, exact token identity, and provenance persistence; this
        method only supplies bounded current records and projects matched
        market ids into the existing fair scheduler.
        """
        self._scope_authority_market_ids = set()

        try:
            from .market_scope import resolve_market_scope
        except (ImportError, AttributeError):
            resolve_market_scope = None
        saver = getattr(self.store, "save_market_scope_resolution", None)
        loader = getattr(self.store, "load_candidate_lifecycle", None)
        rolling_documents = getattr(self, "_rolling_scope_documents", {})
        if (
            not callable(resolve_market_scope)
            or (not callable(loader) and not rolling_documents)
        ):
            return [], {}, {}, self._scope_cursor(root_state)


        documents: list[tuple[str, Mapping[str, Any]]] = []
        scope_candidate_ids: list[str] = []
        truncated_scope_candidate_ids: list[str] = []
        deferred_candidates = [
            str(item).strip()
            for item in root_state.get("scope_resolution_deferred_candidate_ids", ())
            if str(item).strip()
        ]
        current_candidate_ids = list(dict.fromkeys(
            str(item).strip()
            for item in candidate_ids
            if str(item).strip()
        ))
        # A current, already validated observation handoff is allowed to
        # bypass broad inventory/suitability work.  The persisted proof is
        # exact and freshness-bounded; using it here preserves a downstream
        # collection window when an unrelated provider call is slow.
        preloaded_scope_ids: list[str] = []
        preloaded_scope_markets: dict[str, list[str]] = {}
        preloaded_scope_resolutions: dict[str, Any] = {}
        if self._scope_persisted_proof_restore_allowed():
            self._restore_current_observation_scope_proofs(
                observed_at,
                current_candidate_ids,
                preloaded_scope_ids,
                preloaded_scope_markets,
                preloaded_scope_resolutions,
            )
            fast_path_loader_failed = False
            preloaded_handoff_ids = self._validated_observation_handoff_ids(
                preloaded_scope_ids,
                preloaded_scope_markets,
                preloaded_scope_resolutions,
                allow_schema=True,
            )
            if (
                preloaded_handoff_ids
                and self._scope_handoff_fast_streak < _MAX_OBSERVATION_HANDOFF_FAST_CYCLES
            ):
                ordered_handoff_ids = [
                    candidate_id
                    for candidate_id in current_candidate_ids
                    if candidate_id in preloaded_handoff_ids
                ]
                fast_scope_candidate_ids: list[str] = []
                authority_market_ids: set[str] = set()
                for candidate_id in current_candidate_ids:
                    try:
                        record = loader(candidate_id)
                    except (AttributeError, KeyError, RuntimeError, TypeError, ValueError):
                        fast_path_loader_failed = True
                        record = None
                    payload = record.get("payload") if isinstance(record, Mapping) else None
                    rolling_payload = (
                        rolling_documents.get(candidate_id)
                        if isinstance(rolling_documents, Mapping)
                        else None
                    )
                    if not isinstance(payload, Mapping):
                        if isinstance(rolling_payload, Mapping):
                            payload = rolling_payload
                        else:
                            fast_path_loader_failed = True
                            continue
                    if not self._has_scope_material(payload):
                        continue
                    fast_scope_candidate_ids.append(candidate_id)
                    try:
                        authority_market_ids.update(
                            self._scope_exact_market_ids((self._scope_document(payload),))
                        )
                    except (TypeError, ValueError):
                        continue
                for payload in (
                    rolling_documents.values()
                    if isinstance(rolling_documents, Mapping)
                    else ()
                ):
                    if isinstance(payload, Mapping) and self._has_scope_material(payload):
                        try:
                            authority_market_ids.update(
                                self._scope_exact_market_ids((self._scope_document(payload),))
                            )
                        except (TypeError, ValueError):
                            continue
                if not fast_path_loader_failed:
                    self._scope_authority_market_ids = authority_market_ids
                    self._scope_resolution_deferred_candidate_ids = tuple(
                        candidate_id
                        for candidate_id in fast_scope_candidate_ids
                        if candidate_id not in preloaded_handoff_ids
                    )[:_MAX_SCOPE_RESOLUTION_CANDIDATES]
                    self._scope_resolutions.update(
                        {
                            candidate_id: preloaded_scope_resolutions[candidate_id]
                            for candidate_id in ordered_handoff_ids
                        }
                    )
                    self._scope_handoff_fast_streak += 1
                    return (
                        fast_scope_candidate_ids,
                        {
                            candidate_id: preloaded_scope_markets[candidate_id]
                            for candidate_id in ordered_handoff_ids
                        },
                        {},
                        self._scope_cursor(root_state),
                    )
        self._scope_handoff_fast_streak = 0
        current_rolling_ids = {
            str(item).strip()
            for item in getattr(self, "_rolling_scope_candidate_ids", ())
            if str(item).strip()
        }
        protected_current_ids = [
            candidate_id
            for candidate_id in current_candidate_ids
            if candidate_id in current_rolling_ids
        ]
        rotating_current_ids = [
            candidate_id
            for candidate_id in current_candidate_ids
            if candidate_id not in current_rolling_ids
        ]
        try:
            candidate_cursor = int(self._scope_resolution_candidate_cursor)
        except (TypeError, ValueError, OverflowError):
            candidate_cursor = 0
        candidate_cursor %= len(rotating_current_ids) if rotating_current_ids else 1
        rotated_current_ids = (
            rotating_current_ids[candidate_cursor:]
            + rotating_current_ids[:candidate_cursor]
        )
        try:
            deferred_cursor = int(self._scope_resolution_deferred_cursor)
        except (TypeError, ValueError, OverflowError):
            deferred_cursor = 0
        deferred_cursor %= len(deferred_candidates) if deferred_candidates else 1
        rotated_deferred_candidates = (
            deferred_candidates[deferred_cursor:]
            + deferred_candidates[:deferred_cursor]
        )
        deferred_candidate_set = set(deferred_candidates)
        current_candidate_set = set(current_candidate_ids)
        deferred_reserve_id = next(
            (
                candidate_id
                for candidate_id in rotated_deferred_candidates
                if candidate_id not in current_candidate_set
            ),
            rotated_deferred_candidates[0] if rotated_deferred_candidates else None,
        )
        ordered_deferred_candidates = (
            [deferred_reserve_id]
            + [
                candidate_id
                for candidate_id in rotated_deferred_candidates
                if candidate_id != deferred_reserve_id
            ]
            if deferred_reserve_id is not None
            else []
        )
        # Reserve one resolver-document slot for persisted deferred work when
        # noncritical current rotation could otherwise fill the cap forever.
        reserve_deferred = (
            1
            if ordered_deferred_candidates
            and len(protected_current_ids) < _MAX_SCOPE_RESOLUTION_CANDIDATES
            else 0
        )
        rotating_capacity = max(
            0,
            _MAX_SCOPE_RESOLUTION_CANDIDATES
            - len(protected_current_ids)
            - reserve_deferred,
        )
        rotating_prefix_count = min(len(rotated_current_ids), rotating_capacity)
        materialized_rotating_ids: set[str] = set()
        materialized_deferred_ids: set[str] = set()
        # Protected rolling selections remain first.  One persisted deferred
        # candidate is admitted before the remainder of current rotation.
        candidate_order = list(dict.fromkeys([
            *protected_current_ids,
            *rotated_current_ids[:rotating_prefix_count],
            *ordered_deferred_candidates[:reserve_deferred],
            *rotated_current_ids[rotating_prefix_count:],
            *ordered_deferred_candidates[reserve_deferred:],
        ]))
        for candidate_id in candidate_order:
            try:
                record = loader(candidate_id) if callable(loader) else None
            except (AttributeError, KeyError, TypeError, ValueError):
                record = None
            if not isinstance(record, Mapping):
                rolling_document = (
                    rolling_documents.get(candidate_id)
                    if isinstance(rolling_documents, Mapping)
                    else None
                )
                if isinstance(rolling_document, Mapping):
                    record = {
                        "candidate_id": candidate_id,
                        "stage": "FROZEN",
                        "payload": dict(rolling_document),
                    }
            if not isinstance(record, Mapping):
                continue

            stage = str(record.get("stage", "")).strip().upper()
            payload = record.get("payload")
            if not isinstance(payload, Mapping):
                continue
            if stage not in {"FROZEN", "PAPER_FORWARD", "PAPER_PROMOTABLE"}:
                if not (
                    stage in {"SCHEMA_VALIDATED", "REJECTED"}
                    and str(payload.get("paper_observation_intent_id", "")).strip()
                ):
                    continue
            if self._has_scope_material(payload):
                scope_candidate_ids.append(candidate_id)
                if len(documents) < _MAX_SCOPE_RESOLUTION_CANDIDATES:
                    documents.append((candidate_id, self._scope_document(payload)))
                    if candidate_id in rotating_current_ids:
                        materialized_rotating_ids.add(candidate_id)
                    if (
                        candidate_id in deferred_candidate_set
                        and candidate_id not in current_candidate_set
                    ):
                        materialized_deferred_ids.add(candidate_id)
                else:
                    truncated_scope_candidate_ids.append(candidate_id)
        materialized_rotating_count = len(materialized_rotating_ids)
        self._scope_resolution_candidate_cursor = (
            candidate_cursor + materialized_rotating_count
        ) % len(rotating_current_ids) if rotating_current_ids else 0
        materialized_deferred_count = len(materialized_deferred_ids)
        self._scope_resolution_deferred_cursor = (
            deferred_cursor + materialized_deferred_count
        ) % len(deferred_candidates) if deferred_candidates else 0
        materialized_candidate_ids = {candidate_id for candidate_id, _ in documents}
        unmaterialized_deferred_candidates = [
            candidate_id
            for candidate_id in deferred_candidates
            if candidate_id not in materialized_candidate_ids
        ]
        self._scope_authority_market_ids = set(
            list(dict.fromkeys(
                market_id
                for _, document in documents
                for market_id in self._scope_exact_market_ids((document,))
            ))[:_MAX_SCOPE_INVENTORY]
        )

        if not documents:
            return scope_candidate_ids, {}, {}, self._scope_cursor(root_state)
        if not callable(saver):
            # A canonical scope without a persistence path cannot become
            # legacy collection authority or silently schedule its ids.
            return scope_candidate_ids, {}, {}, self._scope_cursor(root_state)

        exact_ids = list(dict.fromkeys(
            market_id
            for _, document in documents
            for market_id in self._scope_exact_market_ids((document,))
        ))[:_MAX_SCOPE_INVENTORY]
        protected_priority_exact_ids = list(dict.fromkeys(
            market_id
            for candidate_id, document in documents
            if candidate_id in current_rolling_ids
            for market_id in self._scope_exact_market_ids((document,))
        ))
        protected_priority_exact_id_set = set(protected_priority_exact_ids)
        pre_direct_attempted_ids: set[str] = set()
        pre_direct_snapshots: dict[str, PredictionMarketSnapshot] = {}
        pre_direct_provider: Any | None = None
        try:
            protected_cursor = int(self._scope_direct_protected_lookup_cursor)
        except (TypeError, ValueError, OverflowError):
            protected_cursor = 0
        protected_cursor %= len(protected_priority_exact_ids) if protected_priority_exact_ids else 1
        protected_cursor_start = protected_cursor
        if (
            protected_priority_exact_ids
            and self._cycle_remaining_seconds() is not None
            and self._cycle_remaining_seconds() <= 0
        ):
            self._mark_cycle_exhaustion("scope_exact_lookup")
        if protected_priority_exact_ids and self._scope_direct_budget_available():
            protected_probe_ids = [
                protected_priority_exact_ids[
                    (protected_cursor + offset) % len(protected_priority_exact_ids)
                ]
                for offset in range(
                    min(_MAX_SCOPE_DIRECT_LOOKUPS, len(protected_priority_exact_ids))
                )
            ]
            for market_id in protected_probe_ids:
                remaining = self._cycle_remaining_seconds()
                if remaining is not None and remaining <= 0:
                    self._mark_cycle_exhaustion("scope_exact_lookup")
                    break
                if not self._scope_direct_budget_available():
                    break
                pre_direct_attempted_ids.add(market_id)
                try:
                    direct_snapshot = self._call_scope_direct(
                        f"scope_exact:/markets/{market_id}",
                        lambda operation_provider, identifier=market_id: self._scope_market_operation(
                            operation_provider,
                            identifier,
                        ),
                        observed_at,
                        counters,
                    )
                except _ProviderDeadlineExceeded as exc:
                    if exc.cycle_expired:
                        self._mark_cycle_exhaustion("scope_exact_lookup")
                    break
                except Exception as exc:
                    counters["errors"] += 1
                    try:
                        self.store.save_collection_error(
                            market_id,
                            observed_at,
                            "scope_exact_lookup",
                            str(exc),
                        )
                    except Exception:
                        pass
                    continue
                if not isinstance(direct_snapshot, PredictionMarketSnapshot):
                    continue
                if str(direct_snapshot.market_id).strip() != market_id:
                    continue
                pre_direct_snapshots[market_id] = direct_snapshot
            if pre_direct_attempted_ids:
                self._scope_direct_protected_lookup_cursor = (
                    protected_cursor + len(pre_direct_attempted_ids)
                ) % len(protected_priority_exact_ids)
        carry_cursor = self._scope_cursor(root_state)
        needs_inventory = any(
            self._scope_document_needs_inventory(document)
            for _, document in documents
        )
        scope_candidates = scope_candidate_ids
        if needs_inventory:
            current_records, snapshots, next_cursor = self._discover_scope_inventory(
                observed_at,
                counters,
                carry_cursor=carry_cursor,
                documents=tuple(document for _, document in documents),
            )
            self._scope_resolution_deferred_candidate_ids = tuple(
                dict.fromkeys([
                    *unmaterialized_deferred_candidates,
                    *truncated_scope_candidate_ids,
                ])
            )[:_MAX_SCOPE_RESOLUTION_CANDIDATES]
            if (
                isinstance(self._scope_inventory_continuation, Mapping)
                and self._scope_inventory_continuation.get("query_reset")
                and not pre_direct_attempted_ids
            ):
                # A reset/rebase response is intentionally not a resolver
                # input.  Keep the candidate scoped so legacy authority cannot
                # leak into scheduling while the next cycle rebases.
                return scope_candidates, {}, {}, next_cursor
        else:
            current_records, snapshots, next_cursor = [], {}, carry_cursor
        if pre_direct_attempted_ids:
            current_records = [
                record
                for record in current_records
                if not (
                    isinstance(record, Mapping)
                    and str(record.get("market_id", "")).strip()
                    in pre_direct_attempted_ids
                )
            ]
            pre_direct_records = [
                self._scope_market_record(
                    snapshot,
                    observed_at,
                    pre_direct_provider or self.provider,
                )
                for snapshot in pre_direct_snapshots.values()
            ]
            current_records = [*pre_direct_records, *current_records]
            snapshots.update(pre_direct_snapshots)
        direct_loop_attempted_ids: set[str] = set()
        direct_attempted_ids: set[str] = set(pre_direct_attempted_ids)
        direct_snapshots: dict[str, PredictionMarketSnapshot] = dict(pre_direct_snapshots)

        known_inventory_ids = {
            str(market_id).strip()
            for market_id in snapshots
            if str(market_id).strip()
        }
        priority_candidates = {
            str(item).strip()
            for item in candidate_ids
            if str(item).strip()
        }
        priority_exact_ids = list(dict.fromkeys(
            market_id
            for candidate_id, document in documents
            if candidate_id in priority_candidates
            for market_id in self._scope_exact_market_ids((document,))
        ))
        priority_exact_id_set = set(priority_exact_ids)
        rotating_priority_exact_ids = [
            market_id
            for market_id in priority_exact_ids
            if market_id not in protected_priority_exact_id_set
        ]
        documents_by_candidate = {
            candidate_id: document
            for candidate_id, document in documents
        }
        deferred_exact_ids = list(dict.fromkeys(
            market_id
            for candidate_id in ordered_deferred_candidates
            for document in (documents_by_candidate.get(candidate_id),)
            if isinstance(document, Mapping)
            for market_id in self._scope_exact_market_ids((document,))
            if market_id not in priority_exact_id_set
        ))
        deferred_exact_ids.extend(
            market_id
            for market_id in exact_ids
            if market_id not in priority_exact_id_set
            and market_id not in deferred_exact_ids
        )
        missing_protected_priority_ids = [
            market_id
            for market_id in protected_priority_exact_ids
            if market_id not in known_inventory_ids
            and market_id not in pre_direct_attempted_ids
        ]
        missing_priority_ids = [
            market_id
            for market_id in rotating_priority_exact_ids
            if market_id not in known_inventory_ids
        ]
        missing_deferred_ids = [
            market_id
            for market_id in deferred_exact_ids
            if market_id not in known_inventory_ids
        ]
        missing_exact_ids = [
            *missing_protected_priority_ids,
            *missing_priority_ids,
            *missing_deferred_ids,
        ]
        direct_provider = pre_direct_provider or self.provider
        if missing_exact_ids:
            try:
                direct_cursor = int(self._scope_direct_lookup_cursor)
            except (TypeError, ValueError):
                direct_cursor = 0
            try:
                priority_cursor = int(self._scope_direct_priority_lookup_cursor)
            except (TypeError, ValueError):
                priority_cursor = 0
            try:
                protected_cursor = int(self._scope_direct_protected_lookup_cursor)
            except (TypeError, ValueError):
                protected_cursor = 0
            priority_cursor %= len(missing_priority_ids) if missing_priority_ids else 1
            protected_cursor %= (
                len(missing_protected_priority_ids)
                if missing_protected_priority_ids
                else 1
            )
            direct_cursor %= len(missing_deferred_ids) if missing_deferred_ids else 1
            direct_capacity_remaining = max(
                0,
                _MAX_SCOPE_DIRECT_LOOKUPS - len(pre_direct_attempted_ids),
            )
            protected_priority_ids = [
                missing_protected_priority_ids[
                    (protected_cursor + offset) % len(missing_protected_priority_ids)
                ]
                for offset in range(
                    min(direct_capacity_remaining, len(missing_protected_priority_ids))
                )
            ] if missing_protected_priority_ids else []
            remaining_direct = max(
                0,
                direct_capacity_remaining - len(protected_priority_ids),
            )
            deferred_reserve = 1 if missing_deferred_ids and remaining_direct else 0
            priority_ids = [
                missing_priority_ids[(priority_cursor + offset) % len(missing_priority_ids)]
                for offset in range(
                    min(
                        max(0, remaining_direct - deferred_reserve),
                        len(missing_priority_ids),
                    )
                )
            ] if missing_priority_ids else []
            remaining_direct = max(
                0,
                direct_capacity_remaining
                - len(protected_priority_ids)
                - len(priority_ids),
            )
            deferred_ids = [
                missing_deferred_ids[(direct_cursor + offset) % len(missing_deferred_ids)]
                for offset in range(
                    min(remaining_direct, len(missing_deferred_ids))
                )
            ] if missing_deferred_ids else []
            direct_ids = [*protected_priority_ids, *priority_ids, *deferred_ids]
            priority_id_set = set(missing_priority_ids)
            protected_priority_id_set = set(protected_priority_ids)
            direct_loop_attempted_ids: set[str] = set()
            attempted_direct = 0
            attempted_protected = 0
            attempted_priority = 0
            attempted_deferred = 0
            for market_id in direct_ids:
                remaining = self._cycle_remaining_seconds()
                if remaining is not None and remaining <= 0:
                    self._mark_cycle_exhaustion("scope_exact_lookup")
                    break
                if not self._scope_direct_budget_available():
                    break
                direct_attempted_ids.add(market_id)
                direct_loop_attempted_ids.add(market_id)
                attempted_direct += 1
                if market_id in protected_priority_id_set:
                    attempted_protected += 1
                elif market_id in priority_id_set:
                    attempted_priority += 1
                else:
                    attempted_deferred += 1

                try:
                    direct_snapshot = self._call_scope_direct(
                        f"scope_exact:/markets/{market_id}",
                        lambda operation_provider, identifier=market_id: self._scope_market_operation(
                            operation_provider,
                            identifier,
                        ),
                        observed_at,
                        counters,
                    )
                except _ProviderDeadlineExceeded as exc:
                    if exc.cycle_expired:
                        self._mark_cycle_exhaustion("scope_exact_lookup")
                    break
                except Exception as exc:
                    counters["errors"] += 1
                    try:
                        self.store.save_collection_error(
                            market_id,
                            observed_at,
                            "scope_exact_lookup",
                            str(exc),
                        )
                    except Exception:
                        pass
                    continue
                if not isinstance(direct_snapshot, PredictionMarketSnapshot):
                    continue
                if str(direct_snapshot.market_id).strip() != market_id:
                    continue
                direct_snapshots[market_id] = direct_snapshot
                snapshots[market_id] = direct_snapshot

        if direct_loop_attempted_ids:
            carried_records = [
                record
                for record in current_records
                if not (
                    isinstance(record, Mapping)
                    and str(record.get("market_id", "")).strip()
                    in direct_loop_attempted_ids
                )
            ]
            direct_records = [
                self._scope_market_record(snapshot, observed_at, direct_provider)
                for snapshot in direct_snapshots.values()
            ]
            current_records = [*direct_records, *carried_records]

            self._scope_direct_protected_lookup_cursor = (
                protected_cursor_start
                + len(pre_direct_attempted_ids)
                + attempted_protected
            ) % len(protected_priority_exact_ids) if protected_priority_exact_ids else 0
            self._scope_direct_priority_lookup_cursor = (
                priority_cursor + attempted_priority
            ) % len(missing_priority_ids) if missing_priority_ids else 0
            self._scope_direct_lookup_cursor = (
                direct_cursor + attempted_deferred
            ) % len(missing_deferred_ids) if missing_deferred_ids else 0
        elif not missing_exact_ids and not pre_direct_attempted_ids:
            self._scope_direct_protected_lookup_cursor = 0
            self._scope_direct_priority_lookup_cursor = 0
            self._scope_direct_lookup_cursor = 0

        coverage = (
            str(self._scope_inventory_continuation.get("coverage_status", "")).upper()
            if isinstance(self._scope_inventory_continuation, Mapping)
            else ""
        )
        if not coverage and current_records and self._scope_inventory_continuation is None:
            # The legacy markets(active=True) endpoint returns a complete
            # bounded inventory and has no continuation metadata.
            coverage = "COMPLETE"
        max_scope_markets = min(
            _MAX_SCOPE_RESOLUTION_MARKETS,
            max(0, len(current_records)),
        )

        candidate_markets: dict[str, list[str]] = {}
        deferred_resolution_ids: list[str] = []
        for document_index, (candidate_id, document) in enumerate(documents):
            if (
                not self._scope_pipeline_budget_available()
                and not self._scope_candidate_has_cached_evidence(
                    document,
                    current_records,
                    snapshots,
                )
            ):
                deferred_resolution_ids = [
                    deferred_id
                    for deferred_id, _ in documents[document_index:]
                ][: _MAX_SCOPE_RESOLUTION_CANDIDATES]
                break
            candidate_records = list(current_records)
            candidate_snapshots = dict(self._scope_refreshed_snapshots)
            candidate_snapshots.update(snapshots)
            policy = self._scope_policy(document)
            exact_scope = (
                policy is not None
                and str(getattr(policy, "mode", "")).upper() == "EXACT_MARKETS"
            )
            suitability_configured = self._suitability_is_configured(document)
            suitability_provider = self.provider
            kwargs = self._suitability_kwargs(document) if suitability_configured else {}

            direct_exact_scope = exact_scope and bool(
                direct_attempted_ids.intersection(
                    self._scope_exact_market_ids((document,))
                )
            )
            if (
                coverage != "COMPLETE"
                and not suitability_configured
                and not direct_exact_scope
            ):
                # A page-local match is not authority without a current
                # selected-token suitability proof.  Exact ids are resolved
                # early only after their bounded direct lookup has run.
                deferred_resolution_ids.append(candidate_id)
                continue



            # unsuitable records wait behind unresolved records so they cannot
            # consume the resolver cap before a later market is assessed.
            if suitability_configured and candidate_snapshots:
                def suitability_priority(record: Mapping[str, Any]) -> int:
                    market_id = str(record.get("market_id", "")).strip()
                    assessment = self._scope_suitability_cache.get(
                        (market_id, _stable_payload(kwargs))
                    )
                    if not isinstance(assessment, Mapping):
                        return 1
                    return (
                        0
                        if str(assessment.get("action", "")).upper() == "SUITABLE"
                        else 2
                    )

                candidate_records.sort(
                    key=lambda record: suitability_priority(record)
                    if isinstance(record, Mapping)
                    else 1
                )
            fresh_direct_records = [
                record
                for record in candidate_records
                if isinstance(record, Mapping)
                and str(record.get("market_id", "")).strip() in direct_snapshots
            ]
            if fresh_direct_records:
                fresh_ids = {
                    str(record.get("market_id", "")).strip()
                    for record in fresh_direct_records
                }
                candidate_records = [
                    *fresh_direct_records,
                    *(
                        record
                        for record in candidate_records
                        if (
                            not isinstance(record, Mapping)
                            or str(record.get("market_id", "")).strip() not in fresh_ids
                        )
                    ),
                ]
            candidate_records = candidate_records[:max_scope_markets]


            if suitability_configured:
                parameters = self._suitability_parameters(document)
                refreshed_records: list[Mapping[str, Any]] = []
                refreshed_snapshots: dict[str, PredictionMarketSnapshot] = {}
                for raw_record in candidate_records:
                    if not isinstance(raw_record, Mapping):
                        continue
                    market_id = str(raw_record.get("market_id", "")).strip()
                    snapshot = candidate_snapshots.get(market_id)
                    terminal_settlement = snapshot is not None and snapshot.settlement in {
                        SettlementState.RESOLVED_YES,
                        SettlementState.RESOLVED_NO,
                        SettlementState.VOID,
                    }
                    if snapshot is not None and (
                        snapshot.active is False
                        or snapshot.closed is True
                        or snapshot.archived is True
                        or terminal_settlement
                    ):
                        lifecycle_reason = (
                            "MARKET_CLOSED"
                            if snapshot.closed is True or terminal_settlement
                            else "INACTIVE_MARKET"
                        )
                        refreshed = dict(
                            self._scope_market_record(
                                snapshot,
                                observed_at,
                                suitability_provider,
                            )
                        )
                        refreshed["suitability_evidence"] = {
                            "market_id": market_id,
                            "action": "UNSUITABLE",
                            "category": "MARKET_LIFECYCLE",
                            "reason": lifecycle_reason,
                            "resolver": "scope_lifecycle",
                            "next_action": "no_refresh_terminal",
                            "observed_at": observed_at.isoformat(),
                        }
                        refreshed_records.append(refreshed)
                        refreshed_snapshots[market_id] = snapshot
                        self._scope_refreshed_snapshots[market_id] = snapshot
                        continue
                    # Once the cycle deadline is exhausted, cached successful
                    # snapshots remain safe to assess; only a new provider
                    # refresh is forbidden.  The same reservation applies to
                    # an uncached selected-token book probe.
                    if (
                        snapshot is None
                        and self._cycle_remaining_seconds() is not None
                        and self._cycle_remaining_seconds() <= 0
                    ):
                        self._mark_cycle_exhaustion("scope_refresh")
                        break
                    if (
                        snapshot is None
                        and market_id not in self._scope_refresh_attempted
                        and not self._scope_pipeline_budget_available()
                    ):
                        snapshot = None
                    elif snapshot is None and market_id not in self._scope_refresh_attempted:
                        snapshot = self._scope_snapshot_from_record(raw_record, observed_at)
                        if snapshot is not None:
                            refresh_kwargs: dict[str, Any] = {}
                            if "pool_name" in inspect.signature(self._refresh_scope_snapshot).parameters:
                                refresh_kwargs["pool_name"] = "scope_direct" if exact_scope else "scope"
                            snapshot = self._refresh_scope_snapshot(
                                snapshot,
                                suitability_provider,
                                observed_at,
                                counters,
                                **refresh_kwargs,
                            )
                            if snapshot is not None:
                                self._scope_refreshed_snapshots[market_id] = snapshot
                    assessment_key = (market_id, _stable_payload(kwargs))
                    if (
                        snapshot is not None
                        and assessment_key not in self._scope_suitability_cache
                        and not self._scope_pipeline_budget_available()
                    ):
                        snapshot = None
                    if snapshot is None:
                        refreshed = dict(raw_record)
                        refreshed["suitability_evidence"] = {
                            "market_id": market_id,
                            "action": "UNSUITABLE",
                            "category": "RULES_UNKNOWN",
                            "reason": "SUITABILITY_UNKNOWN",
                            "resolver": "refresh_market_suitability",
                            "next_action": "recheck_next_discovery_tick",
                            "observed_at": observed_at.isoformat(),
                        }
                        refreshed_records.append(refreshed)
                        continue
                    assessment = self._cached_scope_suitability_assessment(
                        snapshot,
                        observed_at,
                        suitability_provider,
                        counters=counters,
                        pool_name="scope_direct" if exact_scope else "scope",
                        **kwargs,
                    )
                    refreshed = dict(self._scope_market_record(snapshot, observed_at, suitability_provider))
                    refreshed["suitability_evidence"] = dict(assessment)
                    refreshed["suitable_market"] = assessment.get("action") == "SUITABLE"
                    refreshed_records.append(refreshed)
                    refreshed_snapshots[market_id] = snapshot
                    self._scope_refreshed_snapshots[market_id] = snapshot
                candidate_records = refreshed_records
                candidate_snapshots = refreshed_snapshots
                if coverage != "COMPLETE":
                    exact_ids_for_candidate = set(
                        self._scope_exact_market_ids((document,))
                    ) if exact_scope else set()
                    candidate_records = [
                        record
                        for record in candidate_records
                        if (
                            exact_scope
                            and str(record.get("market_id", "")).strip()
                            in exact_ids_for_candidate
                        )
                        or (
                            isinstance(record.get("suitability_evidence"), Mapping)
                            and str(record["suitability_evidence"].get("action", "")).upper()
                            == "SUITABLE"
                        )
                    ]
                    if not candidate_records:
                        continue

            candidate_limit = min(
                _MAX_SCOPE_RESOLUTION_MARKETS,
                max(0, len(candidate_records)),
            )
            try:
                result = resolve_market_scope(
                    candidate_id,
                    document,
                    candidate_records,
                    resolved_at=observed_at,
                    max_matches=100,
                    max_markets=candidate_limit,
                )
            except Exception as exc:
                counters["errors"] += 1
                self.store.save_collection_error(candidate_id, observed_at, "market_scope", str(exc))
                continue
            try:
                parameters = inspect.signature(saver).parameters
                accepts_if_absent = (
                    "if_absent" in parameters
                    or any(
                        parameter.kind is inspect.Parameter.VAR_KEYWORD
                        for parameter in parameters.values()
                    )
                )
            except (TypeError, ValueError):
                accepts_if_absent = True
            try:
                if accepts_if_absent:
                    saver(result, if_absent=True)
                else:
                    saver(result)
            except Exception as exc:
                counters["errors"] += 1
                self.store.save_collection_error(
                    candidate_id, observed_at, "market_scope_persistence", str(exc)
                )
                continue
            # A persisted resolution with unresolved current-market
            # evidence must remain in the bounded deferred queue.  This
            # applies even when inventory itself is terminal COMPLETE:
            # direct lookup windows can only verify a subset per cycle.
            if getattr(result, "deferred_markets", ()):
                deferred_resolution_ids.append(candidate_id)
            matched = self._scope_result_market_ids(result)
            candidate_markets[candidate_id] = matched
            proof = result.as_dict() if hasattr(result, "as_dict") and callable(result.as_dict) else result
            if isinstance(proof, Mapping):
                self._scope_resolutions[candidate_id] = proof
        deferred_priority_resolution_ids = [
            candidate_id
            for candidate_id in deferred_resolution_ids
            if candidate_id in deferred_candidate_set
        ]
        other_resolution_ids = [
            candidate_id
            for candidate_id in deferred_resolution_ids
            if candidate_id not in deferred_candidate_set
        ]
        self._scope_resolution_deferred_candidate_ids = tuple(
            dict.fromkeys([
                *unmaterialized_deferred_candidates,
                *deferred_priority_resolution_ids,
                *other_resolution_ids,
                *truncated_scope_candidate_ids,
            ])
        )[:_MAX_SCOPE_RESOLUTION_CANDIDATES]
        return scope_candidates, candidate_markets, snapshots, next_cursor

    @staticmethod
    def _scope_cursor(root_state: Mapping[str, Any]) -> Any:
        continuation = root_state.get("scope_inventory_continuation")
        if isinstance(continuation, Mapping):
            if str(continuation.get("coverage_status", "")).upper() == "COMPLETE":
                return None
            value = continuation.get("after_cursor", continuation.get("cursor"))
            if isinstance(value, str) and value.strip():
                return value
        value = root_state.get("scope_discovery_carry_cursor", root_state.get("discovery_carry_cursor", 0))
        if isinstance(value, str):
            return value.strip() or None
        try:
            return max(0, int(value))
        except (TypeError, ValueError):
            return 0
    @staticmethod
    def _scope_direct_cursor(root_state: Mapping[str, Any]) -> int:
        value = root_state.get("scope_direct_lookup_cursor", 0)
        try:
            cursor = int(value)
        except (TypeError, ValueError, OverflowError):
            return 0
        return max(0, min(_MAX_SCOPE_INVENTORY, cursor))

    @staticmethod
    def _scope_direct_priority_cursor(root_state: Mapping[str, Any]) -> int:
        value = root_state.get("scope_direct_priority_lookup_cursor", 0)
        try:
            cursor = int(value)
        except (TypeError, ValueError, OverflowError):
            return 0
        return max(0, cursor)

    @staticmethod
    def _scope_resolution_candidate_cursor_value(root_state: Mapping[str, Any]) -> int:
        value = root_state.get("scope_resolution_candidate_cursor", 0)
        try:
            cursor = int(value)
        except (TypeError, ValueError, OverflowError):
            return 0
        return max(0, cursor)


    @staticmethod
    def _scope_direct_protected_cursor(root_state: Mapping[str, Any]) -> int:
        value = root_state.get("scope_direct_protected_lookup_cursor", 0)
        try:
            cursor = int(value)
        except (TypeError, ValueError, OverflowError):
            return 0
        return max(0, cursor)
    @staticmethod
    def _scope_inventory_records(value: Any) -> list[dict[str, Any]]:
        """Normalize persisted inventory records to the durable inventory cap."""
        if not isinstance(value, (list, tuple)):
            return []
        records: list[dict[str, Any]] = []
        seen_ids: set[str] = set()

        for raw_record in value:
            if not isinstance(raw_record, Mapping):
                continue
            market_id = str(raw_record.get("market_id", "")).strip()
            if not market_id or market_id in seen_ids:
                continue
            seen_ids.add(market_id)
            records.append(dict(raw_record))
            if len(records) >= _MAX_SCOPE_INVENTORY:
                break
        return records

    @staticmethod
    def _scope_market_ids(value: Any, *, limit: int = _MAX_SCOPE_INVENTORY) -> list[str]:
        """Normalize a persisted market-id sequence without growing it."""
        if not isinstance(value, (list, tuple, set, frozenset)):
            return []
        bounded_limit = max(0, min(_MAX_SCOPE_INVENTORY, int(limit)))
        result: list[str] = []
        seen: set[str] = set()
        for item in value:
            market_id = str(item).strip()
            if not market_id or market_id in seen:
                continue
            seen.add(market_id)
            result.append(market_id)
            if len(result) >= bounded_limit:
                break
        return result

    @classmethod
    def _bound_scope_continuation(cls, value: Mapping[str, Any]) -> dict[str, Any]:
        """Bound every inventory-shaped collection before it can be persisted."""
        bounded = dict(value)
        for name in ("inventory_records",):
            if name in bounded:
                bounded[name] = cls._scope_inventory_records(bounded.get(name))
        for name in (
            "seen_market_ids",
            "verified_market_ids",
            "suitability_refresh_queue",
        ):
            if name in bounded:
                bounded[name] = cls._scope_market_ids(bounded.get(name))
        return bounded

    @staticmethod
    def _scope_cursor_history(value: Any) -> list[str]:
        """Normalize the bounded opaque-cursor history persisted in state."""
        if not isinstance(value, (list, tuple)):
            return []
        history: list[str] = []
        for item in value:
            if not isinstance(item, str):
                continue
            cursor = item.strip()
            if cursor and cursor not in history:
                history.append(cursor)
        return history[-_MAX_SCOPE_CURSOR_HISTORY:]



    @staticmethod
    def _has_scope_material(payload: Mapping[str, Any]) -> bool:
        """Recognize an explicitly bound canonical scope.

        Bare ``market_ids``/filter fields are the pre-scope candidate
        contract.  They must continue through ``candidate_forward_requirements``
        so existing frozen candidates retain their legacy authority.  A
        canonical policy (or its immutable hash/version binding), including
        one nested in a frozen plan/document, is instead owned exclusively by
        persisted scope resolution.
        """
        bindings = {"market_scope", "market_scope_hash", "market_scope_version", "scope_hash", "scope_version"}

        def declared(document: Mapping[str, Any], depth: int = 0) -> bool:
            if "market_scope" in document and document.get("market_scope") is not None:
                return True
            if any(
                key in document
                and document.get(key) is not None
                and bool(str(document.get(key)).strip())
                for key in bindings - {"market_scope"}
            ):
                return True
            if depth >= 3:
                return False
            return any(
                isinstance(nested := document.get(key), Mapping)
                and declared(nested, depth + 1)
                for key in ("experiment_plan", "frozen_document", "frozen_documents")
            )

        return declared(payload)

    @staticmethod
    def _scope_document(payload: Mapping[str, Any]) -> Mapping[str, Any]:
        """Copy the immutable lifecycle payload without changing authority."""
        document = dict(payload)
        frozen = payload.get("frozen_document")
        if isinstance(frozen, Mapping):
            document = {**document, **dict(frozen)}
        frozen_documents = payload.get("frozen_documents")
        if isinstance(frozen_documents, Mapping):
            document = {**document, **dict(frozen_documents)}
        return document
    @staticmethod
    def _scope_document_needs_inventory(document: Mapping[str, Any]) -> bool:
        source = document.get("experiment_plan")
        source = source if isinstance(source, Mapping) else document
        policy = source.get("market_scope")
        if isinstance(policy, Mapping):
            mode = str(policy.get("mode", "")).strip().upper()
            return mode in {"EXACT_MARKETS", "RULE_BASED_MARKETS"}
        # Hash/version-only or malformed bindings cannot authorize current
        # markets, so resolving them against an inventory is unnecessary.
        return False
    @staticmethod
    def _scope_policy(document: Mapping[str, Any]) -> Any | None:
        source = document.get("experiment_plan")
        source = source if isinstance(source, Mapping) else document
        value = source.get("market_scope")
        if value is None:
            return None
        try:
            from .experiment_plan import MarketScopePolicy, normalize_market_scope
            if isinstance(value, MarketScopePolicy):
                return value
            if isinstance(value, Mapping):
                return normalize_market_scope(value)
        except (TypeError, ValueError, ImportError):
            return None
        return None

    def _scope_inventory_query(
        self,
        documents: Sequence[Mapping[str, Any]],
        observed_at: datetime,
        provider: Any,
        counters: dict[str, Any],
        *,
        limit: int,
        after_cursor: str | None,
    ) -> dict[str, Any]:
        """Build an OR-safe Gamma envelope for all active scope policies."""
        policies = [
            policy
            for document in documents
            if (policy := self._scope_policy(document)) is not None
            and str(getattr(policy, "mode", "")).upper()
            in {"EXACT_MARKETS", "RULE_BASED_MARKETS"}
        ]
        query: dict[str, Any] = {
            "limit": limit,
            "after_cursor": after_cursor,
            "closed": False,
            "tag_ids": (),
            "include_tag": True,
            "liquidity_num_min": None,
            "end_date_min": None,
            "end_date_max": None,
        }
        if not policies:
            return query

        # A category is safe only when every policy has exactly one identical
        # category.  IDs are provider-owned; a slug is never sent as an ID.
        category_values: list[str] = []
        for policy in policies:
            categories = tuple(
                str(item).strip().casefold()
                for item in getattr(policy, "categories", ())
                if str(item).strip()
            )
            if len(categories) != 1:
                category_values = []
                break
            category_values.append(categories[0])
        if category_values and len(set(category_values)) == 1:
            resolver = getattr(provider, "resolve_tag_slug", None)
            if callable(resolver):
                lookup_failed = False
                try:
                    resolved = self._call_provider(
                        "scope_taxonomy:/tags/slug",
                        lambda: resolver(category_values[0]),
                        observed_at,
                        counters,
                        provider=provider,
                    )
                    if isinstance(resolved, Mapping):
                        resolved = (
                            resolved.get("id")
                            or resolved.get("tag_id")
                            or resolved.get("tagId")
                        )
                    if isinstance(resolved, (str, int)) and not isinstance(resolved, bool):
                        query["tag_ids"] = (resolved,)
                    else:
                        lookup_failed = True
                except _ProviderDeadlineExceeded:
                    raise
                except Exception:
                    # Taxonomy lookup is advisory.  Falling back to the
                    # unfiltered page is safer than inventing an identifier.
                    lookup_failed = True
                if lookup_failed:
                    # Optional lookup adapters may retain transport or
                    # validation failures for the next provider operation.
                    # Drain only after this failed advisory call so a
                    # successful broader discovery page is not misclassified.
                    self._consume_advisory_provider_errors(provider, observed_at, counters)


        def scalar_bound(policy: Any, name: str) -> float | None:
            filters = getattr(policy, "filters", {})
            value = filters.get(name) if isinstance(filters, Mapping) else None
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                return None
            try:
                number = float(value)
            except (TypeError, ValueError, OverflowError):
                return None
            return number if math.isfinite(number) else None

        # For the union of policies, a lower/upper envelope is safe only when
        # every policy supplies that bound.  Otherwise an unconstrained policy
        # could be silently filtered out by the server.
        liquidity = [scalar_bound(policy, "min_liquidity") for policy in policies]
        if all(value is not None for value in liquidity):
            query["liquidity_num_min"] = min(value for value in liquidity if value is not None)

        minimum_hours = [
            scalar_bound(policy, "minimum_hours_to_resolution")
            for policy in policies
        ]
        if all(value is not None for value in minimum_hours):
            lower = observed_at + timedelta(hours=min(value for value in minimum_hours if value is not None))
            query["end_date_min"] = lower.isoformat()

        maximum_hours = [
            scalar_bound(policy, "maximum_hours_to_resolution")
            for policy in policies
        ]
        if all(value is not None for value in maximum_hours):
            upper = observed_at + timedelta(hours=max(value for value in maximum_hours if value is not None))
            query["end_date_max"] = upper.isoformat()
        return query

    def _consume_advisory_provider_errors(
        self,
        provider: Any,
        observed_at: datetime,
        counters: dict[str, Any],
    ) -> None:
        for name in ("consume_transport_errors", "consume_validation_errors"):
            consumer = getattr(provider, name, None)
            if not callable(consumer):
                continue
            try:
                self._call_provider(
                    f"scope_error_drain:{name}",
                    consumer,
                    observed_at,
                    counters,
                    provider=provider,
                )
            except Exception:
                # A best-effort advisory drain must never become a discovery
                # failure or mask the broader page request.
                pass

    @staticmethod
    def _drain_provider_advisories_now(provider: Any) -> None:
        """Discard bounded stale provider advisories outside collector calls.

        Provider adapters retain validation failures separately from transport
        failures.  Exact-scope calls may run on an isolated adapter clone, so
        draining only ``self.provider`` leaves late or malformed clone results
        queued for the next request.  This helper is called only while the
        clone has no in-flight operation and never attributes clone errors to
        the root provider.
        """
        for name in ("consume_transport_errors", "consume_validation_errors"):
            consumer = getattr(provider, name, None)
            if not callable(consumer):
                continue
            try:
                consumer()
            except Exception:
                pass

    @staticmethod
    def _scope_query_without_cursor(query: Mapping[str, Any]) -> dict[str, Any]:
        return {
            str(key): value
            for key, value in query.items()
            if str(key) != "after_cursor"
        }

    @staticmethod
    def _scope_count(value: Any, default: int = 0) -> int:
        if isinstance(value, bool):
            return default
        try:
            number = int(value)
        except (TypeError, ValueError, OverflowError):
            return default
        return max(0, number)
    @staticmethod
    def _scope_persistable(value: Any, *, depth: int = 0) -> Any:
        """Project provider request metadata to bounded plain JSON values."""
        if depth > _MAX_SCOPE_QUERY_DEPTH:
            raise _ScopePersistenceValueError("pagination metadata nesting is too deep")
        if value is None or isinstance(value, (bool, int)):
            return value
        if isinstance(value, float):
            if not math.isfinite(value):
                raise _ScopePersistenceValueError("pagination metadata contains a non-finite number")
            return value
        if isinstance(value, str):
            return value[:_MAX_SCOPE_QUERY_STRING_LENGTH]
        if isinstance(value, Mapping):
            result: dict[str, Any] = {}
            try:
                items = value.items()
                for index, (key, child) in enumerate(items):
                    if index >= _MAX_SCOPE_QUERY_ITEMS:
                        break
                    if not isinstance(key, str):
                        raise _ScopePersistenceValueError("pagination metadata keys must be strings")
                    result[key[:_MAX_SCOPE_QUERY_STRING_LENGTH]] = PolymarketCollector._scope_persistable(
                        child,
                        depth=depth + 1,
                    )
            except _ScopePersistenceValueError:
                raise
            except Exception as exc:
                raise _ScopePersistenceValueError("pagination metadata mapping is not readable") from exc
            return result
        if isinstance(value, (list, tuple)):
            result = []
            try:
                for index, child in enumerate(value):
                    if index >= _MAX_SCOPE_QUERY_ITEMS:
                        break
                    result.append(
                        PolymarketCollector._scope_persistable(
                            child,
                            depth=depth + 1,
                        )
                    )
            except _ScopePersistenceValueError:
                raise
            except Exception as exc:
                raise _ScopePersistenceValueError("pagination metadata sequence is not readable") from exc
            return result
        raise _ScopePersistenceValueError(
            f"unsupported pagination metadata value: {type(value).__name__}"
        )

    @classmethod
    def _scope_persistable_query(
        cls,
        value: Any,
        fallback: Mapping[str, Any],
    ) -> dict[str, Any]:
        try:
            projected = cls._scope_persistable(value)
        except _ScopePersistenceValueError:
            projected = None
        if isinstance(projected, Mapping):
            return dict(projected)
        return dict(fallback)

    @staticmethod
    def _scope_persistable_path(value: Any, fallback: str) -> str:
        if isinstance(value, str) and value.strip():
            return value.strip()[:_MAX_SCOPE_REQUEST_PATH_LENGTH]
        return fallback[:_MAX_SCOPE_REQUEST_PATH_LENGTH]
    @staticmethod
    def _scope_page_value(page: Any, name: str, default: Any = _UNSET) -> Any:
        if isinstance(page, Mapping):
            value = page.get(name, default)
        else:
            value = getattr(page, name, default)
        return default if value is _UNSET else value



    def _downstream_collection_reserve_seconds(self) -> float:
        """Reserve one bounded provider window for scheduled collection."""
        cycle_budget = max(0.001, float(self.config.cycle_budget_seconds))
        provider_window = max(0.001, float(self.config.provider_timeout_seconds))
        return min(provider_window, cycle_budget * 0.5)

    def _scope_resolution_reserve_seconds(self) -> float:
        """Reserve bounded storage time for one scope proof before collection."""
        cycle_budget = max(0.001, float(self.config.cycle_budget_seconds))
        return max(0.001, min(float(self.config.provider_timeout_seconds) * 0.1, cycle_budget * 0.1))

    def _scope_phase_reserve_seconds(self) -> float:
        return (
            self._downstream_collection_reserve_seconds()
            + self._scope_resolution_reserve_seconds()
        )

    def _downstream_collection_budget_available(self) -> bool:
        """Whether scope work may consume time without starving collection."""
        remaining = self._cycle_remaining_seconds()
        if remaining is None:
            return True
        return remaining > self._downstream_collection_reserve_seconds() + 1e-6

    def _scope_pipeline_budget_available(self) -> bool:
        """Keep proof persistence and downstream collection behind scope probes."""
        remaining = self._cycle_remaining_seconds()
        if remaining is None:
            return True
        return remaining > self._scope_phase_reserve_seconds() + 1e-6

    def _scope_direct_budget_available(self) -> bool:
        """Allow exact lookups their independent phase share without overrunning collection."""
        remaining = self._cycle_remaining_seconds()
        if remaining is None:
            return True
        if remaining <= 0:
            return False
        if self._scope_direct_budget_remaining is not None and self._scope_direct_budget_remaining <= 0:
            return False
        phase_timeout = self._provider_phase_timeout("scope_direct", remaining)
        return phase_timeout > 0 and remaining > self._downstream_collection_reserve_seconds()

    @staticmethod
    def _scope_exact_market_ids(
        documents: Sequence[Mapping[str, Any]],
    ) -> tuple[str, ...]:
        """Return immutable exact-scope members for refresh prioritization."""
        result: list[str] = []
        for document in documents:
            if not isinstance(document, Mapping):
                continue
            try:
                policy = PolymarketCollector._scope_policy(document)
            except (TypeError, ValueError):
                policy = None
            if policy is None or str(getattr(policy, "mode", "")).upper() != "EXACT_MARKETS":
                continue
            values = getattr(policy, "market_ids", ())
            if isinstance(values, str):
                values = (values,)
            if not isinstance(values, (list, tuple, set, frozenset)):
                continue
            result.extend(
                str(item).strip()
                for item in values
                if str(item).strip()
            )
        return tuple(dict.fromkeys(result))
    def _scope_candidate_has_cached_evidence(
        self,
        document: Mapping[str, Any],
        records: Sequence[Mapping[str, Any]],
        snapshots: Mapping[str, PredictionMarketSnapshot],
    ) -> bool:
        """Allow pure resolution from this tick's snapshots after the reserve."""
        if not self._suitability_is_configured(document):
            return True
        kwargs = self._suitability_kwargs(document)
        policy = self._scope_policy(document)
        required_ids: set[str] = set()
        if policy is not None and str(getattr(policy, "mode", "")).upper() == "EXACT_MARKETS":
            required_ids.update(self._scope_exact_market_ids((document,)))
        if not required_ids:
            required_ids.update(
                str(record.get("market_id", "")).strip()
                for record in records
                if isinstance(record, Mapping) and str(record.get("market_id", "")).strip()
            )
        available_snapshots = dict(self._scope_refreshed_snapshots)
        available_snapshots.update(snapshots)
        cache_key = _stable_payload(kwargs)
        return all(
            market_id in available_snapshots
            and (market_id, cache_key) in self._scope_suitability_cache
            for market_id in required_ids
        )

    def _refresh_scope_snapshot(
        self,
        snapshot: PredictionMarketSnapshot,
        provider: Any,
        observed_at: datetime,
        counters: dict[str, Any],
        *,
        pool_name: str | None = None,
    ) -> PredictionMarketSnapshot | None:
        """Refresh carried inventory metadata before using its evidence."""
        market_id = str(snapshot.market_id).strip()
        if market_id:
            self._scope_refresh_attempted.add(market_id)
        provider_pool = pool_name or self._scope_provider_pool(provider)
        broad_scope = provider_pool == "scope" or (
            provider_pool is None and self._scope_phase_active
        )
        if broad_scope and self._scope_broad_provider_unavailable:
            return None
        try:
            if provider_pool == "scope_direct":
                self._close_scope_direct_provider(provider)
                current = self._call_scope_direct(
                    f"scope_refresh:/markets/{snapshot.market_id}",
                    lambda operation_provider: operation_provider.market(snapshot.market_id),
                    observed_at,
                    counters,
                )
            else:
                fetcher = getattr(provider, "market", None)
                if not callable(fetcher):
                    return None
                current = self._call_provider(
                    f"scope_refresh:/markets/{snapshot.market_id}",
                    lambda: fetcher(snapshot.market_id),
                    observed_at,
                    counters,
                    provider=provider,
                    market_id=snapshot.market_id,
                )
        except _ProviderDeadlineExceeded:
            if broad_scope:
                self._scope_broad_provider_unavailable = True
            return None
        except Exception:
            return None
        return current if isinstance(current, PredictionMarketSnapshot) else None


    @staticmethod
    def _scope_snapshot_from_record(
        record: Mapping[str, Any],
        observed_at: datetime,
    ) -> PredictionMarketSnapshot | None:
        """Rehydrate a bounded discovery record after a process restart."""
        market_id = str(record.get("market_id", "")).strip()
        if not market_id:
            return None
        timestamp = parse_timestamp(
            record.get("provider_timestamp", record.get("observed_at"))
        ) or ensure_utc(observed_at)
        settlement_raw = record.get("settlement", record.get("outcome", "open"))
        try:
            settlement = settlement_raw if isinstance(settlement_raw, SettlementState) else SettlementState(
                str(settlement_raw).strip().lower()
            )
        except (TypeError, ValueError):
            settlement = SettlementState.UNKNOWN
        tags = record.get("tags", record.get("tag", ()))
        if isinstance(tags, str):
            tags = (tags,)
        elif not isinstance(tags, (list, tuple)):
            tags = ()
        try:
            return PredictionMarketSnapshot(
                timestamp=timestamp,
                market_id=market_id,
                question=str(record.get("question") or market_id),
                yes_bid=record.get("yes_bid"),
                yes_ask=record.get("yes_ask"),
                yes_mid=record.get("yes_mid", record.get("price")),
                no_bid=record.get("no_bid"),
                no_ask=record.get("no_ask"),
                no_mid=record.get("no_mid"),
                volume=record.get("volume"),
                liquidity=record.get("liquidity"),
                expiry=parse_timestamp(record.get("expiry")),
                settlement=settlement,
                category=record.get("category"),
                tags=tuple(str(tag) for tag in tags),
                source=str(record.get("source") or ""),
                yes_token_id=record.get("yes_token_id"),
                no_token_id=record.get("no_token_id"),
                condition_id=record.get("condition_id") or None,
                provider_timestamp=parse_timestamp(record.get("provider_timestamp")),
                active=record.get("active"),
                closed=record.get("closed"),
                archived=record.get("archived"),
                accepting_orders=record.get("accepting_orders"),
                enable_order_book=record.get("enable_order_book"),
            )
        except (TypeError, ValueError):
            return None

    def _discover_scope_inventory(
        self,
        observed_at: datetime,
        counters: dict[str, Any],
        *,
        carry_cursor: Any,
        provider: Any | None = None,
        documents: Sequence[Mapping[str, Any]] = (),
    ) -> tuple[list[Mapping[str, Any]], dict[str, PredictionMarketSnapshot], Any]:
        """Fetch exactly one bounded scope page, preserving opaque continuation."""
        provider = provider or self.provider
        suitability_specs: list[
            tuple[int, int, Mapping[str, Any], dict[str, Any], bool]
        ] = []
        for index, document in enumerate(documents):
            if not isinstance(document, Mapping):
                continue
            parameters = self._suitability_parameters(document)
            if not parameters:
                continue
            kwargs = self._suitability_kwargs(document)
            assumption_error = bool(parameters.get("_assumption_error"))
            is_observation = bool(
                str(document.get("paper_observation_intent_id", "")).strip()
                or document.get("observation_intent") is True
                or str(document.get("quality", "")).strip().upper() == "PAPER_OBSERVATION"
            )
            suitability_specs.append((
                1 if is_observation else 0,
                index,
                document,
                kwargs,
                assumption_error,
            ))
        suitability_specs.sort(
            key=lambda item: (item[4], item[0], item[1])
        )
        suitability_enabled = bool(suitability_specs)
        exact_market_ids = set(self._scope_exact_market_ids(documents))

        def suitability_provider_for(market_id: str) -> Any:
            return provider

        def suitability_pool_for(market_id: str) -> str | None:
            if str(market_id).strip() in exact_market_ids:
                return "scope_direct"
            return None
        suitability_kwargs: dict[str, Any] = (
            dict(suitability_specs[0][3]) if suitability_specs else {}
        )
        suitability_kwargs_by_market: dict[str, dict[str, Any]] = {}
        suitability_kwargs_validity: dict[str, bool] = {}
        for _, _, document, kwargs, assumption_error in suitability_specs:
            for market_id in self._scope_exact_market_ids((document,)):
                previous_valid = suitability_kwargs_validity.get(market_id)
                if previous_valid is False and not assumption_error:
                    suitability_kwargs_by_market[market_id] = dict(kwargs)
                    suitability_kwargs_validity[market_id] = True
                elif market_id not in suitability_kwargs_by_market:
                    suitability_kwargs_by_market[market_id] = dict(kwargs)
                    suitability_kwargs_validity[market_id] = not assumption_error
        method_page = getattr(provider, "market_page", None)
        if callable(method_page) and self.config.discovery_budget_per_cycle > 0:
            limit = min(100, max(1, int(self.config.discovery_budget_per_cycle)))
            previous = (
                dict(self._scope_inventory_continuation)
                if isinstance(self._scope_inventory_continuation, Mapping)
                else {}
            )
            try:
                query = self._scope_inventory_query(
                    documents,
                    observed_at,
                    provider,
                    counters,
                    limit=limit,
                    after_cursor=None,
                )
            except Exception as exc:
                counters["errors"] += 1
                timeout_fields = (
                    {
                        "timeout_reason": str(exc).split(":", 1)[0],
                        "timeout_endpoint": getattr(exc, "endpoint", self._current_endpoint),
                        "timeout_seconds": float(self.config.provider_timeout_seconds),
                        "resolver": "retry_provider_call",
                        "next_action": "retry_next_collection_tick",
                    }
                    if isinstance(exc, _ProviderDeadlineExceeded)
                    else {}
                )
                self._scope_inventory_continuation = {
                    **previous,
                    "after_cursor": previous.get("after_cursor", carry_cursor),
                    "coverage_status": "ERROR",
                    "error_reason": str(exc),
                    **timeout_fields,
                    "updated_at": observed_at.isoformat(),
                }
                try:
                    self.store.save_collection_error(
                        None,
                        observed_at,
                        "scope_discovery",
                        str(exc),
                    )
                except Exception:
                    pass
                return [], {}, carry_cursor
            previous_request = previous.get("request_query")
            if not isinstance(previous_request, Mapping):
                previous_request = previous.get("query")
            query_changed = (
                isinstance(previous_request, Mapping)
                and _stable_payload(self._scope_query_without_cursor(previous_request))
                != _stable_payload(self._scope_query_without_cursor(query))
            )
            fingerprint_material = {
                "request_path": "/markets/keyset",
                "query": self._scope_query_without_cursor(query),
            }
            expected_fingerprint = "sha256:" + hashlib.sha256(
                _stable_payload(fingerprint_material).encode("utf-8")
            ).hexdigest()
            saved_cursor = previous.get("after_cursor", previous.get("cursor"))
            has_saved_cursor = isinstance(saved_cursor, str) and bool(saved_cursor.strip())
            prior_terminal = bool(previous) and (
                str(previous.get("coverage_status", "")).upper() == "COMPLETE"
                or not has_saved_cursor
            )
            prior_rebase = bool(
                previous.get("query_reset")
                or previous.get("rebase_required")
            )
            stored_request_fingerprint = previous.get("request_fingerprint")
            stored_request_fingerprint = (
                str(stored_request_fingerprint).strip()
                if (
                    stored_request_fingerprint is not None
                    and str(stored_request_fingerprint).strip()
                )
                else None
            )
            # Older continuations used query_fingerprint for the collector's
            # request hash.  Only use it as a compatibility fallback when it
            # actually matches the newly computed request fingerprint; a
            # provider-owned fingerprint must not be mistaken for this hash.
            if (
                stored_request_fingerprint is None
                and str(previous.get("query_fingerprint", "")).strip()
                == expected_fingerprint
            ):
                stored_request_fingerprint = expected_fingerprint
            stored_fingerprint_changed = (
                stored_request_fingerprint is not None
                and stored_request_fingerprint != expected_fingerprint
            )
            base_state = (
                {}
                if (
                    query_changed
                    or prior_terminal
                    or prior_rebase
                    or stored_fingerprint_changed
                )
                else previous
            )
            base_state = self._bound_scope_continuation(base_state)
            current_cursor: str | None = None
            if base_state and str(base_state.get("coverage_status", "")).upper() != "COMPLETE":
                saved_cursor = base_state.get("after_cursor", base_state.get("cursor"))
                if isinstance(saved_cursor, str) and saved_cursor.strip():
                    current_cursor = saved_cursor
            cursor_history = self._scope_cursor_history(
                base_state.get(
                    "seen_cursor_history",
                    base_state.get("seen_cursors", ()),
                )
            )
            inventory_records_by_id: dict[str, dict[str, Any]] = {}
            prior_inventory = base_state.get("inventory_records", ())
            if isinstance(prior_inventory, (list, tuple)):
                for raw_record in prior_inventory:
                    if not isinstance(raw_record, Mapping):
                        continue
                    market_id = str(raw_record.get("market_id", "")).strip()
                    if market_id and market_id not in inventory_records_by_id:
                        inventory_records_by_id[market_id] = dict(raw_record)
            cursor_history_exhausted = False
            if current_cursor is not None and current_cursor not in cursor_history:
                if len(cursor_history) >= _MAX_SCOPE_CURSOR_HISTORY:
                    cursor_history_exhausted = True
                else:
                    cursor_history.append(current_cursor)
            stored_provider_fingerprint = base_state.get("provider_query_fingerprint")
            stored_provider_fingerprint = (
                str(stored_provider_fingerprint).strip()
                if (
                    stored_provider_fingerprint is not None
                    and str(stored_provider_fingerprint).strip()
                )
                else None
            )
            if (
                stored_provider_fingerprint is None
                and base_state
                and str(base_state.get("query_fingerprint", "")).strip()
                not in {"", expected_fingerprint}
            ):
                # Compatibility with continuations written before the
                # request/provider fingerprints were split.
                stored_provider_fingerprint = str(base_state["query_fingerprint"]).strip()
            # A provider fingerprint is optional.  Once a provider supplies
            # one, it is adopted and must remain stable; the collector's
            # request hash is never used as a provider-fingerprint surrogate.
            expected_provider_fingerprint = stored_provider_fingerprint
            query["after_cursor"] = current_cursor
            request_path = "/markets/keyset"
            request_query = dict(query)
            safe_request_query = self._scope_persistable_query(request_query, {})
            try:
                parameters = inspect.signature(method_page).parameters
            except (TypeError, ValueError):
                parameters = {}
            accepts_kwargs = any(
                parameter.kind is inspect.Parameter.VAR_KEYWORD
                for parameter in parameters.values()
            )
            call_kwargs = dict(request_query)
            if parameters and not accepts_kwargs:
                call_kwargs = {
                    key: value for key, value in call_kwargs.items() if key in parameters
                }
            def persist_query_reset(
                reason: str,
                *,
                observed_fingerprint: str | None = None,
            ) -> None:
                counters["errors"] += 1
                zero_counts = {
                    "raw_count": 0,
                    "unique_count": 0,
                    "duplicate_count": 0,
                    "malformed_count": 0,
                }
                continuation = {
                    "request_path": request_path,
                    "request_query": safe_request_query,
                    "request": {
                        "method": "GET",
                        "path": request_path,
                        "query": safe_request_query,
                    },
                    "query": dict(safe_request_query),
                    "request_fingerprint": expected_fingerprint,
                    "query_fingerprint": (
                        expected_provider_fingerprint or expected_fingerprint
                    ),
                    "provider_query_fingerprint": expected_provider_fingerprint,
                    "expected_query_fingerprint": expected_fingerprint,
                    "after_cursor": None,
                    "opaque_cursor": None,
                    "seen_market_ids": [],
                    "seen_cursor_history": [],
                    "cursor": None,
                    "coverage_status": "ERROR",
                    "raw_count": 0,
                    "unique_count": 0,
                    "duplicate_count": 0,
                    "malformed_count": 0,
                    "cumulative": dict(zero_counts),
                    "cumulative_raw_count": 0,
                    "cumulative_unique_count": 0,
                    "cumulative_malformed_count": 0,
                    "query_reset": True,
                    "rebase_required": True,
                    "query_reset_reason": reason,
                    "requested_at": observed_at.isoformat(),
                    "last_request_at": observed_at.isoformat(),
                    "response_received_at": observed_at.isoformat(),
                    "last_response_at": observed_at.isoformat(),
                    "updated_at": observed_at.isoformat(),
                    "last_page_at": observed_at.isoformat(),
                    "first_page_at": observed_at.isoformat(),
                }
                continuation["error_reason"] = (
                    "QUERY_RESET"
                    if reason == "QUERY_FINGERPRINT_MISMATCH"
                    else reason
                )
                if observed_fingerprint is not None:
                    continuation["observed_query_fingerprint"] = observed_fingerprint
                self._scope_inventory_continuation = continuation
            if cursor_history_exhausted:
                persist_query_reset("CURSOR_HISTORY_EXHAUSTED")
                return [], {}, None
            if current_cursor is not None and len(inventory_records_by_id) >= _MAX_SCOPE_INVENTORY:
                # The existing continuation is retained for restart, but a
                # cursor still outstanding means this is not complete scope
                # authority and must never be resolved page-locally.
                self._scope_inventory_continuation = {
                    **base_state,
                    "after_cursor": current_cursor,
                    "opaque_cursor": current_cursor,
                    "cursor": current_cursor,
                    "coverage_status": "BUDGET_EXHAUSTED",
                    "verified_market_ids": [],
                    "authorization_status": "UNAUTHORIZED" if suitability_enabled else base_state.get("authorization_status"),
                    "inventory_records": self._scope_inventory_records(
                        list(inventory_records_by_id.values())
                    ),
                    "seen_market_ids": self._scope_market_ids(sorted(inventory_records_by_id)),
                    "updated_at": observed_at.isoformat(),
                }
                snapshot_by_id: dict[str, PredictionMarketSnapshot] = {}
                prior_refresh_queue = [
                    market_id
                    for market_id in self._scope_market_ids(
                        base_state.get("suitability_refresh_queue", ())
                    )
                    if market_id in inventory_records_by_id
                ]
                prior_verified = [
                    market_id
                    for market_id in self._scope_market_ids(
                        base_state.get("verified_market_ids", ())
                    )
                    if market_id in inventory_records_by_id
                ]
                exact_ids = exact_market_ids
                refresh_order = list(dict.fromkeys([
                    *(
                        market_id
                        for market_id in prior_verified
                        if market_id in exact_ids
                    ),
                    *prior_refresh_queue,
                    *prior_verified,
                    *inventory_records_by_id,
                ]))[:_MAX_SCOPE_INVENTORY]
                refresh_index = 0
                failed_refresh = False
                for market_id in refresh_order:
                    if self._scope_broad_provider_unavailable and market_id not in exact_ids:
                        refresh_index += 1
                        failed_refresh = True
                        continue
                    if self._cycle_deadline_exhausted:
                        refresh_index += 1
                        failed_refresh = True
                        break
                    if self._cycle_remaining_seconds() is not None and self._cycle_remaining_seconds() <= 0:
                        self._mark_cycle_exhaustion("scope_refresh")
                        refresh_index += 1
                        failed_refresh = True
                        break
                    if suitability_enabled and not self._scope_pipeline_budget_available():
                        # Leave one provider window for the resolver's
                        # selected-token book probe on the next tick.
                        break
                    if market_id in self._scope_refresh_attempted:
                        refresh_index += 1
                        continue
                    snapshot = self._scope_snapshot_from_record(
                        inventory_records_by_id[market_id],
                        observed_at,
                    )
                    if suitability_enabled and snapshot is not None:
                        refresh_kwargs: dict[str, Any] = {}
                        if "pool_name" in inspect.signature(self._refresh_scope_snapshot).parameters:
                            refresh_kwargs["pool_name"] = suitability_pool_for(market_id)
                        snapshot = self._refresh_scope_snapshot(
                            snapshot,
                            suitability_provider_for(market_id),
                            observed_at,
                            counters,
                            **refresh_kwargs,
                        )
                    if suitability_enabled and snapshot is None:
                        refresh_index += 1
                        failed_refresh = True
                        if self._cycle_deadline_exhausted:
                            break
                        continue
                    if market_id and snapshot is not None:
                        snapshot_by_id[market_id] = snapshot
                        self._scope_refreshed_snapshots[market_id] = snapshot
                    refresh_index += 1
                rotation = refresh_index % len(refresh_order) if refresh_order else 0
                if failed_refresh and refresh_order and rotation == 0:
                    rotation = 1
                self._scope_inventory_continuation["suitability_refresh_queue"] = self._scope_market_ids(
                    refresh_order[rotation:] + refresh_order[:rotation]
                )
                return self._scope_inventory_records(
                    list(inventory_records_by_id.values())
                ), snapshot_by_id, current_cursor


            try:
                page = self._call_provider(
                    "scope_keyset:/markets/keyset",
                    lambda: method_page(**call_kwargs),
                    observed_at,
                    counters,
                    provider=provider,
                )
            except Exception as exc:
                counters["errors"] += 1
                try:
                    self.store.save_collection_error(None, observed_at, "scope_discovery", str(exc))
                except Exception:
                    pass
                timeout_fields = (
                    {
                        "timeout_reason": str(exc).split(":", 1)[0],
                        "timeout_endpoint": getattr(exc, "endpoint", self._current_endpoint),
                        "timeout_seconds": float(self.config.provider_timeout_seconds),
                        "resolver": "retry_provider_call",
                        "next_action": "retry_next_collection_tick",
                    }
                    if isinstance(exc, _ProviderDeadlineExceeded)
                    else {}
                )
                cumulative = {
                    "raw_count": self._scope_count(base_state.get("cumulative_raw_count")),
                    "unique_count": self._scope_count(base_state.get("cumulative_unique_count")),
                    "duplicate_count": self._scope_count(base_state.get("cumulative_duplicate_count")),
                    "malformed_count": self._scope_count(base_state.get("cumulative_malformed_count")),
                }
                continuation = {
                    **base_state,
                    **timeout_fields,
                    "request_path": request_path,
                    "request_query": safe_request_query,
                    "request": {
                        "method": "GET",
                        "path": request_path,
                        "query": safe_request_query,
                    },
                    "query": dict(safe_request_query),
                    "request_fingerprint": expected_fingerprint,
                    "query_fingerprint": (
                        stored_provider_fingerprint or expected_fingerprint
                    ),
                    "provider_query_fingerprint": stored_provider_fingerprint,
                    "expected_query_fingerprint": expected_fingerprint,
                    "after_cursor": current_cursor,
                    "opaque_cursor": current_cursor,
                    "verified_market_ids": [] if suitability_enabled else list(base_state.get("verified_market_ids", ())),
                    "authorization_status": "UNAUTHORIZED" if suitability_enabled else base_state.get("authorization_status"),
                    "coverage_status": "ERROR",
                    "raw_count": 0,
                    "unique_count": 0,
                    "duplicate_count": 0,
                    "malformed_count": 0,
                    "cumulative": cumulative,
                    "cumulative_raw_count": cumulative["raw_count"],
                    "cumulative_unique_count": cumulative["unique_count"],
                    "cumulative_duplicate_count": cumulative["duplicate_count"],
                    "cumulative_malformed_count": cumulative["malformed_count"],
                    "seen_cursor_history": list(cursor_history),
                    "requested_at": observed_at.isoformat(),
                    "last_request_at": observed_at.isoformat(),
                    "response_received_at": observed_at.isoformat(),
                    "last_response_at": observed_at.isoformat(),
                    "updated_at": observed_at.isoformat(),
                    "last_page_at": observed_at.isoformat(),
                    "first_page_at": base_state.get("first_page_at", observed_at.isoformat()),
                }
                self._scope_inventory_continuation = continuation
                return [], {}, current_cursor
            page_was_none = page is None
            page_was_invalid = (
                not page_was_none
                and not isinstance(page, (Mapping, list, tuple))
                and not hasattr(page, "snapshots")
            )
            if page_was_none or page_was_invalid:
                persist_query_reset("INVALID_PAGE")
                return [], {}, None
            provided_fingerprint = self._scope_page_value(page, "query_fingerprint", _UNSET)
            provided_fingerprint = (
                str(provided_fingerprint).strip()
                if provided_fingerprint not in (_UNSET, None)
                and str(provided_fingerprint).strip()
                else None
            )
            if (
                provided_fingerprint is not None
                and expected_provider_fingerprint is not None
                and provided_fingerprint != expected_provider_fingerprint
            ):
                persist_query_reset(
                    "QUERY_FINGERPRINT_MISMATCH",
                    observed_fingerprint=provided_fingerprint,
                )
                return [], {}, None
            raw_items = self._scope_page_value(page, "snapshots", _UNSET)
            if raw_items is _UNSET and isinstance(page, Mapping):
                raw_items = page.get("markets", page.get("data", ()))
            if raw_items is _UNSET:
                raw_items = page if isinstance(page, (list, tuple)) else ()
            try:
                raw_items = list(raw_items or ())
            except TypeError:
                raw_items = []
            supplied_raw = self._scope_page_value(page, "raw_count", _UNSET)
            supplied_unique = self._scope_page_value(page, "unique_count", _UNSET)
            supplied_duplicate = self._scope_page_value(page, "duplicate_count", _UNSET)
            supplied_malformed = self._scope_page_value(page, "malformed_count", _UNSET)
            raw_count = self._scope_count(
                supplied_raw,
                len(raw_items) if supplied_raw is _UNSET else 0,
            )
            valid: list[PredictionMarketSnapshot] = []
            malformed_computed = 0
            page_ids: set[str] = set()
            duplicate_computed = 0
            for item in raw_items:
                if not isinstance(item, PredictionMarketSnapshot):
                    malformed_computed += 1
                    continue
                market_id = str(item.market_id).strip()
                if not market_id:
                    malformed_computed += 1
                    continue
                if market_id in page_ids:
                    duplicate_computed += 1
                    continue
                page_ids.add(market_id)
                valid.append(item)
            malformed_count = max(
                malformed_computed,
                self._scope_count(supplied_malformed, 0) if supplied_malformed is not _UNSET else 0,
            )
            duplicate_count = max(
                duplicate_computed,
                self._scope_count(supplied_duplicate, 0) if supplied_duplicate is not _UNSET else 0,
            )
            suitability_enabled = any(self._suitability_is_configured(document) for document in documents)
            suitability_evidence: list[dict[str, Any]] = []
            if not suitability_enabled:
                suitability_evidence = [
                    dict(item)
                    for item in base_state.get("suitable_market_evidence", ())
                    if isinstance(item, Mapping)
                ]
            suitability_exclusions: list[dict[str, Any]] = [
                dict(item)
                for item in base_state.get("suitability_exclusions", ())
                if isinstance(item, Mapping)
            ]
            # A verified id from an earlier cycle is not current authority.
            # It is revalidated below against the present selected-token book.
            verified_market_ids: set[str] = set()
            page_error_reason = self._scope_page_value(page, "error_reason", None)
            page_error_reason = (
                str(page_error_reason).strip().upper()
                if page_error_reason is not None and str(page_error_reason).strip()
                else None
            )
            if page_error_reason == "REPEATED_CURSOR":
                persist_query_reset(
                    "REPEATED_CURSOR",
                    observed_fingerprint=provided_fingerprint,
                )
                return [], {}, None
            explicit_status = str(
                self._scope_page_value(page, "coverage_status", "")
            ).strip().upper()
            if explicit_status == "ERROR":
                # Error pages are discarded before their IDs can enter the
                # cross-page seen set or be scheduled by scope resolution.
                persist_query_reset(
                    page_error_reason or "PAGE_ERROR",
                    observed_fingerprint=provided_fingerprint,
                )
                return [], {}, None
            next_cursor = self._scope_page_value(page, "next_cursor", None)
            if next_cursor is not None:
                if not isinstance(next_cursor, str) or not next_cursor.strip():
                    persist_query_reset(
                        "INVALID_NEXT_CURSOR",
                        observed_fingerprint=provided_fingerprint,
                    )
                    return [], {}, None
            if next_cursor is not None and next_cursor in cursor_history:
                persist_query_reset(
                    "REPEATED_CURSOR",
                    observed_fingerprint=provided_fingerprint,
                )
                return [], {}, None
            if next_cursor is not None:
                if len(cursor_history) >= _MAX_SCOPE_CURSOR_HISTORY:
                    persist_query_reset(
                        "CURSOR_HISTORY_EXHAUSTED",
                        observed_fingerprint=provided_fingerprint,
                    )
                    return [], {}, None
                cursor_history.append(next_cursor)
            provider_fingerprint = provided_fingerprint or stored_provider_fingerprint
            page_fingerprint = provider_fingerprint or expected_fingerprint
            seen_ids = set(inventory_records_by_id)
            seen_ids.update(
                str(item).strip()
                for item in base_state.get("seen_market_ids", ())
                if str(item).strip()
            )
            new_snapshots: list[PredictionMarketSnapshot] = []
            cross_page_duplicates = 0
            cap_reached = False
            for item in valid:
                market_id = str(item.market_id).strip()
                if market_id in seen_ids:
                    cross_page_duplicates += 1
                    continue
                if len(seen_ids) >= _MAX_SCOPE_INVENTORY:
                    cap_reached = True
                    break
                seen_ids.add(market_id)
                new_snapshots.append(item)
            duplicate_count += cross_page_duplicates
            unique_count = len(new_snapshots)
            if supplied_unique is not _UNSET and not seen_ids:
                unique_count = self._scope_count(supplied_unique, unique_count)
            if next_cursor is None:
                coverage_status = (
                    "PARTIAL"
                    if explicit_status in {"PARTIAL", "BUDGET_EXHAUSTED"} or malformed_count
                    else "COMPLETE"
                )
            elif cap_reached or explicit_status in {"PARTIAL", "BUDGET_EXHAUSTED"}:
                coverage_status = explicit_status if explicit_status in {"PARTIAL", "BUDGET_EXHAUSTED"} else "BUDGET_EXHAUSTED"
            else:
                coverage_status = "BUDGET_EXHAUSTED"
            suitability_evidence: list[dict[str, Any]] = []
            if not suitability_enabled:
                suitability_evidence = [
                    dict(item)
                    for item in base_state.get("suitable_market_evidence", ())
                    if isinstance(item, Mapping)
                ]
            page_records: list[dict[str, Any]] = []
            refreshed_snapshots: dict[str, PredictionMarketSnapshot] = {}
            refresh_queue: list[str] = []
            if suitability_enabled:
                # Carried inventory is a persistent work queue.  Refresh one
                # market end-to-end before spending the cycle on broad page
                # suitability, and retain a provider window for its
                # selected-token book/rules probe before starting metadata.
                prior_refresh_queue = [
                    market_id
                    for market_id in self._scope_market_ids(
                        base_state.get("suitability_refresh_queue", ())
                    )
                    if market_id in inventory_records_by_id
                ]
                prior_verified = [
                    market_id
                    for market_id in self._scope_market_ids(
                        base_state.get("verified_market_ids", ())
                    )
                    if market_id in inventory_records_by_id
                ]
                exact_ids = exact_market_ids
                refresh_order = list(dict.fromkeys([
                    *(
                        market_id
                        for market_id in prior_verified
                        if market_id in exact_ids
                    ),
                    *prior_refresh_queue,
                    *prior_verified,
                    *inventory_records_by_id,
                ]))[:_MAX_SCOPE_INVENTORY]
                refresh_index = 0
                failed_refresh = False
                failed_market_id: str | None = None
                for market_id in refresh_order:
                    if self._scope_broad_provider_unavailable and market_id not in exact_ids:
                        refresh_index += 1
                        failed_refresh = True
                        failed_market_id = failed_market_id or market_id
                        continue
                    if self._cycle_deadline_exhausted:
                        refresh_index += 1
                        failed_refresh = True
                        failed_market_id = failed_market_id or market_id
                        break
                    if self._cycle_remaining_seconds() is not None and self._cycle_remaining_seconds() <= 0:
                        self._mark_cycle_exhaustion("scope_suitability")
                        refresh_index += 1
                        failed_refresh = True
                        failed_market_id = failed_market_id or market_id
                        break
                    if not self._scope_pipeline_budget_available():
                        # Preserve a full downstream metadata/rules + book
                        # pipeline for the next queued market.
                        break
                    if market_id in self._scope_refresh_attempted:
                        refresh_index += 1
                        continue
                    record = dict(inventory_records_by_id[market_id])
                    # Evidence from a prior tick is diagnostic history only.
                    record.pop("suitability_evidence", None)
                    record.pop("suitable_market", None)
                    inventory_records_by_id[market_id] = record
                    snapshot = self._scope_snapshot_from_record(record, observed_at)
                    if snapshot is None:
                        refresh_index += 1
                        failed_refresh = True
                        failed_market_id = failed_market_id or market_id
                        continue
                    refresh_kwargs: dict[str, Any] = {}
                    if "pool_name" in inspect.signature(self._refresh_scope_snapshot).parameters:
                        refresh_kwargs["pool_name"] = suitability_pool_for(market_id)
                    snapshot = self._refresh_scope_snapshot(
                        snapshot,
                        suitability_provider_for(market_id),
                        observed_at,
                        counters,
                        **refresh_kwargs,
                    )
                    if snapshot is None:
                        refresh_index += 1
                        failed_refresh = True
                        failed_market_id = failed_market_id or market_id
                        if self._cycle_deadline_exhausted:
                            break
                        continue
                    if not self._scope_pipeline_budget_available():
                        # Metadata succeeded, but starting a book request now
                        # would consume the reserved suitability window.
                        refresh_index += 1
                        failed_refresh = True
                        failed_market_id = failed_market_id or market_id
                        break
                    probe_kwargs = suitability_kwargs_by_market.get(
                        market_id,
                        suitability_kwargs,
                    )
                    assessment = self._cached_scope_suitability_assessment(
                        snapshot,
                        observed_at,
                        suitability_provider_for(market_id),
                        counters=counters,
                        pool_name=suitability_pool_for(market_id),
                        **probe_kwargs,
                    )
                    self._suitable_market_evidence.append(dict(assessment))
                    suitability_evidence.append(dict(assessment))
                    record = dict(self._scope_market_record(
                        snapshot,
                        observed_at,
                        suitability_provider_for(market_id),
                    ))
                    record["suitability_evidence"] = dict(assessment)
                    record["suitable_market"] = assessment.get("action") == "SUITABLE"
                    refreshed_snapshots[market_id] = snapshot
                    self._scope_refreshed_snapshots[market_id] = snapshot
                    if record["suitable_market"]:
                        verified_market_ids.add(market_id)
                    else:
                        failed_refresh = True
                        failed_market_id = failed_market_id or market_id
                        suitability_exclusions.append(dict(assessment))
                    inventory_records_by_id[market_id] = record
                    refresh_index += 1
                    if self._cycle_deadline_exhausted:
                        break
                if failed_refresh and failed_market_id and prior_refresh_queue:
                    queue_rotation = prior_refresh_queue.index(failed_market_id) + 1 if failed_market_id in prior_refresh_queue else None
                    rotation = (
                        queue_rotation % len(prior_refresh_queue)
                        if queue_rotation is not None and prior_refresh_queue
                        else refresh_index % len(refresh_order) if refresh_order else 0
                    )
                else:
                    rotation = refresh_index % len(refresh_order) if refresh_order else 0
                if failed_refresh and refresh_order and rotation == 0 and not prior_refresh_queue:
                    # A complete pass otherwise returns the same head.  Move
                    # it even when every bounded entry was visited so a
                    # failed/unsuitable member cannot monopolize position 0.
                    rotation = 1
                queue_source = refresh_order
                refresh_queue = self._scope_market_ids(
                    queue_source[rotation:] + queue_source[:rotation]
                )
            for item in new_snapshots:
                if self._cycle_remaining_seconds() is not None and self._cycle_remaining_seconds() <= 0:
                    self._mark_cycle_exhaustion("scope_suitability")
                    break
                if suitability_enabled and not self._scope_pipeline_budget_available():
                    # Keep an explicit collection window after scope
                    # authorization; page rows can continue on the next tick.
                    break
                market_id = str(item.market_id).strip()
                record = dict(self._scope_market_record(
                    item,
                    observed_at,
                    suitability_provider_for(market_id),
                ))
                refreshed_snapshots[market_id] = item
                self._scope_refreshed_snapshots[market_id] = item
                if suitability_enabled:
                    probe_kwargs = suitability_kwargs_by_market.get(
                        market_id,
                        suitability_kwargs,
                    )
                    assessment = self._cached_scope_suitability_assessment(
                        item,
                        observed_at,
                        suitability_provider_for(market_id),
                        counters=counters,
                        pool_name=suitability_pool_for(market_id),
                        **probe_kwargs,
                    )
                    self._suitable_market_evidence.append(dict(assessment))
                    suitability_evidence.append(dict(assessment))
                    record["suitability_evidence"] = dict(assessment)
                    record["suitable_market"] = assessment.get("action") == "SUITABLE"
                    if record["suitable_market"]:
                        verified_market_ids.add(market_id)
                    else:
                        suitability_exclusions.append(dict(assessment))
                page_records.append(record)
            for record in page_records:
                market_id = str(record.get("market_id", "")).strip()
                if market_id:
                    inventory_records_by_id[market_id] = record
            cumulative_raw = self._scope_count(base_state.get("cumulative_raw_count")) + raw_count
            cumulative_unique = self._scope_count(base_state.get("cumulative_unique_count")) + unique_count
            cumulative_duplicate = self._scope_count(base_state.get("cumulative_duplicate_count")) + duplicate_count
            cumulative_malformed = self._scope_count(base_state.get("cumulative_malformed_count")) + malformed_count
            cumulative = {
                "raw_count": cumulative_raw,
                "unique_count": cumulative_unique,
                "duplicate_count": cumulative_duplicate,
                "malformed_count": cumulative_malformed,
            }
            page_query = self._scope_page_value(page, "query", _UNSET)
            safe_page_query = self._scope_persistable_query(
                page_query if isinstance(page_query, Mapping) else safe_request_query,
                safe_request_query,
            )
            page_path = self._scope_persistable_path(
                self._scope_page_value(page, "request_path", request_path),
                request_path,
            )
            continuation = {
                "request_path": page_path,
                "request_query": safe_request_query,
                "request": {
                    "method": "GET",
                    "path": page_path,
                    "query": safe_page_query,
                },
                "query": dict(safe_page_query),
                "request_fingerprint": expected_fingerprint,
                "query_fingerprint": page_fingerprint,
                "provider_query_fingerprint": provider_fingerprint,
                "expected_query_fingerprint": expected_fingerprint,
                "after_cursor": next_cursor,
                "opaque_cursor": next_cursor,
                "cursor": next_cursor,
                "coverage_status": coverage_status,
                "raw_count": raw_count,
                "unique_count": unique_count,
                "duplicate_count": duplicate_count,
                "malformed_count": malformed_count,
                "cumulative": cumulative,
                "cumulative_raw_count": cumulative_raw,
                "cumulative_unique_count": cumulative_unique,
                "cumulative_duplicate_count": cumulative_duplicate,
                "seen_market_ids": sorted(seen_ids)[:_MAX_SCOPE_INVENTORY],
                "seen_cursor_history": (
                    list(cursor_history) if next_cursor is not None else []
                ),
                "requested_at": observed_at.isoformat(),
                "last_request_at": observed_at.isoformat(),
                "response_received_at": observed_at.isoformat(),
                "last_response_at": observed_at.isoformat(),
                "updated_at": observed_at.isoformat(),
                "last_page_at": observed_at.isoformat(),
                "first_page_at": base_state.get("first_page_at", observed_at.isoformat()),
            }
            if suitability_enabled:
                continuation["suitability_enabled"] = True
                continuation["suitable_market_evidence"] = suitability_evidence[-256:]
                continuation["suitability_exclusions"] = suitability_exclusions[-256:]
                continuation["verified_market_ids"] = self._scope_market_ids(sorted(verified_market_ids))
                continuation["suitability_refresh_queue"] = self._scope_market_ids(refresh_queue)
                continuation["authorization_status"] = (
                    "VERIFIED_MARKET_AUTHORIZED" if verified_market_ids else "UNAUTHORIZED"
                )
            if coverage_status != "COMPLETE" or suitability_enabled:
                continuation["inventory_records"] = self._scope_inventory_records(
                    list(inventory_records_by_id.values())
                )
            if page_error_reason is not None:
                continuation["error_reason"] = page_error_reason
            self._scope_inventory_continuation = self._bound_scope_continuation(continuation)
            record_by_id = dict(refreshed_snapshots)
            records = self._scope_inventory_records(list(inventory_records_by_id.values()))
            return records, record_by_id, next_cursor

        if callable(method_page) and self.config.discovery_budget_per_cycle <= 0:
            previous = self._bound_scope_continuation(
                dict(self._scope_inventory_continuation)
                if isinstance(self._scope_inventory_continuation, Mapping)
                else {}
            )
            previous["coverage_status"] = "BUDGET_EXHAUSTED"
            previous["seen_cursor_history"] = self._scope_cursor_history(
                previous.get(
                    "seen_cursor_history",
                    previous.get("seen_cursors", ()),
                )
            )
            previous["updated_at"] = observed_at.isoformat()
            self._scope_inventory_continuation = previous
            return [], {}, carry_cursor

        # Legacy/fake providers retain the original bounded offset behavior.
        method = getattr(provider, "markets", None)
        legacy_previous = self._bound_scope_continuation(
            dict(self._scope_inventory_continuation)
            if isinstance(self._scope_inventory_continuation, Mapping)
            else {}
        )
        # Keep the legacy no-continuation sentinel intact: it proves that the
        # bounded markets(active=True) response is a complete inventory.
        self._scope_inventory_continuation = legacy_previous or None
        # A prior cycle's suitability proof is expired before this page is
        # assessed; only current selected-token evidence may authorize.
        verified_market_ids: set[str] = set()
        suitability_exclusions = [
            dict(item)
            for item in legacy_previous.get("suitability_exclusions", ())
            if isinstance(item, Mapping)
        ]
        if not callable(method):
            return [], {}, carry_cursor
        kwargs: dict[str, Any] = {"active": True}
        try:
            parameters = inspect.signature(method).parameters
        except (TypeError, ValueError):
            parameters = {}
        accepts_kwargs = any(
            parameter.kind is inspect.Parameter.VAR_KEYWORD
            for parameter in parameters.values()
        )
        scan_budget = min(
            100,
            max(1, self.config.max_markets, self.config.discovery_budget_per_cycle),
        )
        if "limit" in parameters or accepts_kwargs or not parameters:
            kwargs["limit"] = scan_budget
        try:
            values = self._call_provider(
                "scope_markets:/markets",
                lambda: method(**kwargs),
                observed_at,
                counters,
                provider=provider,
            ) or ()
        except Exception as exc:
            counters["errors"] += 1
            timeout_fields = (
                {
                    "timeout_reason": str(exc).split(":", 1)[0],
                    "timeout_endpoint": getattr(exc, "endpoint", self._current_endpoint),
                    "timeout_seconds": float(self.config.provider_timeout_seconds),
                    "resolver": "retry_provider_call",
                    "next_action": "retry_next_collection_tick",
                }
                if isinstance(exc, _ProviderDeadlineExceeded)
                else {}
            )
            self._scope_inventory_continuation = {
                **legacy_previous,
                "request_path": "/markets",
                "request_query": dict(kwargs),
                "after_cursor": carry_cursor,
                "coverage_status": "ERROR",
                "error_reason": str(exc),
                **timeout_fields,
                "updated_at": observed_at.isoformat(),
            }
            self.store.save_collection_error(None, observed_at, "scope_discovery", str(exc))
            return [], {}, carry_cursor
        snapshots: list[PredictionMarketSnapshot] = []
        seen_ids: set[str] = set()
        for item in values:
            if not isinstance(item, PredictionMarketSnapshot):
                continue
            market_id = str(item.market_id).strip()
            if not market_id or market_id in seen_ids:
                continue
            seen_ids.add(market_id)
            snapshots.append(item)
            if len(snapshots) >= 100:
                break
        if not snapshots:
            self._scope_inventory_continuation = legacy_previous
            return [], {}, carry_cursor
        try:
            offset = int(carry_cursor) % len(snapshots)
        except (TypeError, ValueError):
            offset = 0
        rotated = snapshots[offset:] + snapshots[:offset]
        page = rotated[:scan_budget]
        records: list[Mapping[str, Any]] = []
        record_by_id: dict[str, PredictionMarketSnapshot] = {}
        suitability_evidence: list[dict[str, Any]] = [
            dict(item)
            for item in legacy_previous.get("suitable_market_evidence", ())
            if isinstance(item, Mapping)
        ]
        for item in page:
            market_id = str(item.market_id).strip()
            suitability_provider = suitability_provider_for(market_id)
            record = dict(self._scope_market_record(item, observed_at, suitability_provider))
            if suitability_enabled:
                probe_kwargs = suitability_kwargs_by_market.get(
                    str(item.market_id).strip(),
                    suitability_kwargs,
                )
                assessment = self._cached_scope_suitability_assessment(
                    item,
                    observed_at,
                    suitability_provider_for(market_id),
                    counters=counters,
                    pool_name=suitability_pool_for(market_id),
                    **probe_kwargs,
                )
                suitability_evidence.append(dict(assessment))
                record["suitability_evidence"] = dict(assessment)
                record["suitable_market"] = assessment.get("action") == "SUITABLE"
                if record["suitable_market"]:
                    verified_market_ids.add(str(item.market_id).strip())
                else:
                    suitability_exclusions.append(dict(assessment))
            records.append(record)
            record_by_id[str(item.market_id).strip()] = item
            self._scope_refreshed_snapshots[str(item.market_id).strip()] = item
        next_cursor = (offset + len(page)) % len(snapshots)
        if suitability_enabled:
            self._scope_inventory_continuation = {
                **legacy_previous,
                "request_path": "/markets",
                "request_query": dict(kwargs),
                "after_cursor": next_cursor,
                "coverage_status": "BUDGET_EXHAUSTED",
                "suitability_enabled": True,
                "suitable_market_evidence": suitability_evidence[-256:],
                "suitability_exclusions": suitability_exclusions[-256:],
                "verified_market_ids": sorted(verified_market_ids)[:_MAX_SCOPE_INVENTORY],
                "authorization_status": (
                    "VERIFIED_MARKET_AUTHORIZED"
                    if verified_market_ids
                    else "UNAUTHORIZED"
                ),
                "updated_at": observed_at.isoformat(),
            }
        return records, record_by_id, next_cursor




    @staticmethod
    def _scope_market_record(
        snapshot: PredictionMarketSnapshot,
        observed_at: datetime,
        provider: Any,
    ) -> Mapping[str, Any]:
        settlement = getattr(snapshot.settlement, "value", snapshot.settlement)
        # Gamma lifecycle fields are authoritative only when explicitly
        # present.  Keep missing values unknown so the resolver can defer
        # rather than infer lifecycle or order-book state from unrelated
        # fields such as settlement or an attached book.
        active = snapshot.active
        closed = snapshot.closed
        book_available = snapshot.enable_order_book
        provider_name = getattr(provider, "provider_name", type(provider).__name__)
        return {
            "market_id": snapshot.market_id,
            "condition_id": snapshot.condition_id or "",
            "yes_token_id": snapshot.yes_token_id or "",
            "no_token_id": snapshot.no_token_id or "",
            "instrument": "POLYMARKET",
            "instrument_type": "POLYMARKET",
            "market_type": "prediction",
            "venue": "POLYMARKET",
            "category": snapshot.category,
            "categories": [snapshot.category] if snapshot.category else [],
            "tag": list(snapshot.tags),
            "tags": list(snapshot.tags),
            "question": snapshot.question,
            "yes_bid": snapshot.yes_bid,
            "yes_ask": snapshot.yes_ask,
            "yes_mid": snapshot.yes_mid,
            "price": snapshot.yes_mid,
            "expiry": snapshot.expiry.isoformat() if snapshot.expiry is not None else None,
            "liquidity": snapshot.liquidity,
            "volume": snapshot.volume,
            "spread": snapshot.yes_spread,
            "yes_spread": snapshot.yes_spread,
            "no_spread": snapshot.no_spread,
            "source": provider_name,
            "provider": "POLYMARKET",
            "provider_name": provider_name,
            "active": active,
            "open": active,
            "closed": closed,
            "archived": snapshot.archived,
            "settlement": settlement,
            "outcome": settlement,
            "accepting_orders": snapshot.accepting_orders,
            "acceptingOrders": snapshot.accepting_orders,
            "accepting-orders": snapshot.accepting_orders,
            "enable_order_book": snapshot.enable_order_book,
            "enableOrderBook": snapshot.enable_order_book,
            "book": book_available,
            "book_available": book_available,
            "order_book_available": book_available,
            "source_type": "CURRENT",
            "observed_at": observed_at.isoformat(),
            "provider_timestamp": (
                snapshot.provider_timestamp.isoformat()
                if snapshot.provider_timestamp is not None
                else None
            ),
            "metadata_provenance": {
                "source_type": "CURRENT",
                "provider": provider_name,
                "instrument": "POLYMARKET",
                "venue": "POLYMARKET",
                "observed_at": observed_at.isoformat(),
            },
        }
    @staticmethod
    def _book_levels(book: Any, side: str) -> list[tuple[float, float]]:
        """Return finite ``(price, size)`` rows without treating missing depth as malformed."""
        values = getattr(book, "asks" if side == "BUY" else "bids", None)
        if values is None and isinstance(book, Mapping):
            values = book.get("asks" if side == "BUY" else "bids", ())
        if not isinstance(values, (list, tuple)):
            return []
        levels: list[tuple[float, float]] = []
        for value in values:
            price = getattr(value, "price", None)
            size = getattr(value, "size", None)
            if isinstance(value, Mapping):
                price = value.get("price", value.get("px", price))
                size = value.get("size", value.get("quantity", value.get("qty", size)))
            try:
                price_number = float(price)
                size_number = float(size)
            except (TypeError, ValueError, OverflowError):
                continue
            if math.isfinite(price_number) and math.isfinite(size_number) and price_number > 0 and size_number > 0:
                levels.append((price_number, size_number))
        levels.sort(key=lambda item: item[0], reverse=side != "BUY")
        return levels
    @staticmethod
    def _book_token_id(book: Any) -> str | None:
        value = getattr(book, "token_id", None)
        if isinstance(book, Mapping):
            value = book.get(
                "token_id",
                book.get("tokenId", book.get("asset_id", book.get("assetId", value))),
            )
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    def _cached_scope_suitability_assessment(
        self,
        snapshot: PredictionMarketSnapshot,
        observed_at: datetime,
        provider: Any,
        *,
        counters: dict[str, Any] | None = None,
        pool_name: str | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Reuse one scope suitability probe for the duration of a tick."""
        key = (str(snapshot.market_id).strip(), _stable_payload(kwargs))
        cached = self._scope_suitability_cache.get(key)
        if cached is not None:
            return dict(cached)
        provider_pool = pool_name or self._scope_provider_pool(provider)
        assessment = self._suitable_market_assessment(
            snapshot,
            observed_at,
            provider,
            counters=counters,
            pool_name=provider_pool,
            **kwargs,
        )
        # Do not evict: callers may revisit an earlier market later in the
        # same resolution pass, and eviction would repeat its provider probe.
        if len(self._scope_suitability_cache) < _MAX_SCOPE_SUITABILITY_CACHE:
            self._scope_suitability_cache[key] = dict(assessment)
        return dict(assessment)


    def _suitable_market_assessment(
        self,
        snapshot: PredictionMarketSnapshot,
        observed_at: datetime,
        provider: Any,
        *,
        pool_name: str | None = None,
        counters: dict[str, Any] | None = None,
        intended_token: str | None = None,
        required_capital: float | None = None,
        min_entry_depth: float | None = None,
        min_exit_depth: float | None = None,
        min_activity: float | None = None,
        venue_fee_rate: float | None = None,
    ) -> dict[str, Any]:
        """Assess one public market for a bounded paper-observation probe.

        This is intentionally a producer-side assessment, not execution
        authority.  A later execution controller must re-read canonical rules,
        token identity, and depth immediately before any order decision.
        """
        operation_pool = pool_name or self._scope_provider_pool(provider)
        token = str(intended_token if intended_token is not None else self.config.intended_token).strip().lower()
        capital = float(self.config.required_capital if required_capital is None else required_capital)
        entry_floor = float(self.config.min_entry_depth if min_entry_depth is None else min_entry_depth)
        exit_floor = float(self.config.min_exit_depth if min_exit_depth is None else min_exit_depth)
        fee_rate = float(0.0 if venue_fee_rate is None else venue_fee_rate)
        activity_floor = float(self.config.min_activity if min_activity is None else min_activity)
        if (
            token not in {"yes", "no"}
            or not all(math.isfinite(value) and value >= 0 for value in (capital, entry_floor, exit_floor, fee_rate, activity_floor))
        ):
            return {
                "market_id": str(snapshot.market_id),
                "category": "RULES_UNKNOWN",
                "action": "UNSUITABLE",
                "reason": "SUITABILITY_ASSUMPTIONS_UNKNOWN",
                "resolver": "refresh_frozen_scope_assumptions",
                "next_action": "recheck_next_discovery_tick",
                "intended_token": token,
                "required_capital": capital if math.isfinite(capital) and capital >= 0 else None,
                "observed_required_capital": None,
                "observed_required_amount": None,
                "venue_fee_rate": fee_rate if math.isfinite(fee_rate) and fee_rate >= 0 else None,
                "observed_at": ensure_utc(observed_at).isoformat(),
            }
        evidence: dict[str, Any] = {
            "market_id": str(snapshot.market_id),
            "category": "MARKET_CONSTRAINT",
            "action": "UNSUITABLE",
            "reason": "",
            "resolver": "inspect_market_depth",
            "next_action": "recheck_next_discovery_tick",
            "intended_token": token,
            "required_capital": capital,
            "observed_required_capital": None,
            "observed_required_amount": None,
            "venue_fee_rate": fee_rate,
            "observed_at": ensure_utc(observed_at).isoformat(),
        }
        broad_scope = operation_pool == "scope" or (
            operation_pool is None and self._scope_phase_active
        )
        if broad_scope and self._scope_broad_provider_unavailable:
            evidence.update(
                category="PROVIDER_UNAVAILABLE",
                reason="SCOPE_PROVIDER_UNAVAILABLE",
                resolver="retry_provider_call",
                next_action="retry_next_collection_tick",
            )
            return evidence
        try:
            snapshot_timestamp = ensure_utc(snapshot.timestamp)
        except (AttributeError, TypeError, ValueError, OverflowError):
            evidence.update(
                category="DATA_FRESHNESS",
                reason="FRESHNESS_UNKNOWN",
                resolver="refresh_market_snapshot",
                next_action="recheck_next_discovery_tick",
            )
            return evidence
        if snapshot.active is None:
            evidence.update(
                category="MARKET_LIFECYCLE",
                reason="ACTIVE_UNKNOWN",
                resolver="refresh_market_metadata",
                next_action="recheck_next_discovery_tick",
            )
            return evidence
        if snapshot.closed is None:
            evidence.update(
                category="MARKET_LIFECYCLE",
                reason="CLOSED_UNKNOWN",
                resolver="refresh_market_metadata",
                next_action="recheck_next_discovery_tick",
            )
            return evidence
        if snapshot.active is False or snapshot.closed is True or snapshot.archived is True:
            evidence.update(
                category="MARKET_LIFECYCLE",
                reason="INACTIVE_MARKET",
                resolver="wait_for_market_reopen",
                next_action="recheck_next_discovery_tick",
            )
            return evidence
        if snapshot.accepting_orders is None:
            evidence.update(
                category="MARKET_LIFECYCLE",
                reason="ACCEPTING_ORDERS_UNKNOWN",
                resolver="refresh_market_metadata",
                next_action="recheck_next_discovery_tick",
            )
            return evidence
        if snapshot.accepting_orders is False:
            evidence.update(
                category="MARKET_LIFECYCLE",
                reason="ACCEPTING_ORDERS_FALSE",
                resolver="wait_for_accepting_orders",
                next_action="recheck_next_discovery_tick",
            )
            return evidence
        if snapshot.enable_order_book is None:
            evidence.update(
                category="MARKET_LIFECYCLE",
                reason="ORDER_BOOK_UNKNOWN",
                resolver="refresh_market_metadata",
                next_action="recheck_next_discovery_tick",
            )
            return evidence
        if snapshot.enable_order_book is False:
            evidence.update(
                category="CAPITAL_OR_MARKET_CONSTRAINT",
                reason="ORDER_BOOK_UNAVAILABLE",
                resolver="wait_for_order_book",
                next_action="recheck_next_discovery_tick",
            )
            return evidence
        token_id = getattr(snapshot, f"{token}_token_id", None)
        evidence["token_id"] = str(token_id) if token_id else None
        if not token_id:
            evidence.update(
                category="CAPITAL_OR_MARKET_CONSTRAINT",
                reason="TOKEN_ID_MISSING",
                resolver="refresh_market_metadata",
                next_action="recheck_next_discovery_tick",
            )
            return evidence
        book: Any = snapshot.order_book
        attached_token = self._book_token_id(book)
        if attached_token is not None and attached_token != str(token_id).strip():
            book = None
        def call_public(
            endpoint: str,
            operation_factory: Callable[[Any], Any],
        ) -> Any:
            if operation_pool == "scope_direct":
                if counters is None:
                    return operation_factory(provider)
                return self._call_scope_direct(
                    endpoint,
                    operation_factory,
                    observed_at,
                    counters,
                )
            if counters is None:
                return operation_factory(provider)
            return self._call_provider(
                endpoint,
                lambda: operation_factory(provider),
                observed_at,
                counters,
                provider=provider,
                pool_name=operation_pool,
            )

        if book is None:
            fetch_for_token = getattr(provider, "order_book_for_token", None)
            if callable(fetch_for_token):
                try:
                    book = call_public(
                        f"suitability_order_book:{snapshot.market_id}",
                        lambda operation_provider: operation_provider.order_book_for_token(
                            str(token_id),
                            depth=self.config.depth,
                        ),
                    )
                except _ProviderDeadlineExceeded as exc:
                    evidence.update(
                        category="PROVIDER_TIMEOUT",
                        reason=str(exc).split(":", 1)[0],
                        resolver="retry_provider_call",
                        next_action="retry_next_collection_tick",
                    )
                    return evidence
                except Exception:
                    book = None
        if book is None:
            fetcher = getattr(provider, "order_books", None)
            if callable(fetcher):
                try:
                    books = call_public(
                        f"suitability_order_books:{snapshot.market_id}",
                        lambda operation_provider: operation_provider.order_books(
                            snapshot.market_id,
                            depth=self.config.depth,
                        ),
                    )
                    if isinstance(books, Mapping):
                        # The provider's map key is only a lookup hint.  The
                        # returned book must carry the exact selected token
                        # identity; never guess from a singleton response.
                        for key in (token, str(token_id).strip()):
                            candidate = books.get(key)
                            if candidate is not None and self._book_token_id(candidate) == str(token_id).strip():
                                book = candidate
                                break
                except _ProviderDeadlineExceeded as exc:
                    evidence.update(
                        category="PROVIDER_TIMEOUT",
                        reason=str(exc).split(":", 1)[0],
                        resolver="retry_provider_call",
                        next_action="retry_next_collection_tick",
                    )
                    return evidence
                except Exception:
                    book = None
        if getattr(book, "available", True) is False or (
            isinstance(book, Mapping) and book.get("available") is False
        ):
            book = None
        if book is None:
            evidence.update(
                category="CAPITAL_OR_MARKET_CONSTRAINT",
                reason="NO_DEPTH",
                resolver="inspect_selected_token_depth",
                next_action="recheck_next_discovery_tick",
            )
            return evidence
        attached_token = self._book_token_id(book)
        if attached_token != str(token_id).strip():
            evidence.update(
                category="CAPITAL_OR_MARKET_CONSTRAINT",
                reason="NO_DEPTH",
                resolver="inspect_selected_token_depth",
                next_action="recheck_next_discovery_tick",
            )
            return evidence
        try:
            from .polymarket_rules import assess_selected_token_depth, parse_polymarket_rules

            rules = parse_polymarket_rules(book)
        except Exception as exc:
            reason = str(exc).strip() or "RULES_UNKNOWN"
            evidence.update(
                category="RULES_UNKNOWN",
                reason=reason,
                resolver="refresh_market_rules",
                next_action="recheck_next_discovery_tick",
                rules_error=reason,
            )
            return evidence
        evidence["rules_version"] = str(
            getattr(rules, "rules_version", getattr(rules, "sdk_version", ""))
        )
        asks = self._book_levels(book, "BUY")
        bids = self._book_levels(book, "SELL")
        if not asks or not bids:
            evidence.update(
                category="CAPITAL_OR_MARKET_CONSTRAINT",
                reason="NO_DEPTH",
                resolver="inspect_selected_token_depth",
                next_action="recheck_next_discovery_tick",
            )
            return evidence
        best_ask = asks[0][0]
        minimum_order = float(rules.min_order_size)
        # Polymarket quantities are two-decimal orders.  Round the requested
        # quantity up to a valid two-decimal representation when an unusual
        # venue minimum has finer precision; displayed level sizes remain
        # untouched and are assessed at their original precision.
        minimum_order = math.ceil(minimum_order * 100.0 - 1e-12) / 100.0
        quantity = max(
            minimum_order,
            0.01,
            math.floor(
                (
                    capital / (best_ask * (1.0 + fee_rate))
                    if capital > 0 and fee_rate >= 0
                    else minimum_order
                )
                * 100.0
                + 1e-12
            )
            / 100.0,
        )
        observed_required = quantity * best_ask * (1.0 + fee_rate)
        evidence["observed_required_capital"] = observed_required
        entry_depth = sum(size for _, size in asks)
        exit_depth = sum(size for _, size in bids)
        age_seconds = max(
            0.0,
            (ensure_utc(observed_at) - snapshot_timestamp).total_seconds(),
        )
        evidence.update({
            "entry_depth": entry_depth,
            "exit_depth": exit_depth,
            "depth_score": min(entry_depth, exit_depth),
            "entry_activity": float(snapshot.volume or 0.0),
            "exit_activity": float(snapshot.volume or 0.0),
            "activity_score": float(snapshot.volume or 0.0),
            "freshness_score": 1.0 / (1.0 + age_seconds),
            "required_quantity": quantity,
            "entry_price": best_ask,
        })
        canonical_action: str | None = None
        canonical_reason: str | None = None
        try:
            assessments = (
                assess_selected_token_depth(
                    book,
                    rules,
                    side="BUY",
                    quantity=quantity,
                    cap_usd=capital,
                    venue_fee_rate=fee_rate,
                ),
                assess_selected_token_depth(
                    book,
                    rules,
                    side="SELL",
                    quantity=quantity,
                    venue_fee_rate=fee_rate,
                ),
            )
            assessment = assessments[0]
            action_value = getattr(assessment, "action", None)
            if action_value is None and isinstance(assessment, Mapping):
                action_value = assessment.get("action")
            canonical_action = str(getattr(action_value, "value", action_value) or "").upper() or None
            reason_value = getattr(assessment, "reason", None)
            if reason_value is None and isinstance(assessment, Mapping):
                reason_value = assessment.get("reason")
            canonical_reason = str(getattr(reason_value, "value", reason_value) or "").upper() or None
            for candidate_assessment in assessments[1:]:
                candidate_action = getattr(candidate_assessment, "action", None)
                if candidate_action is None and isinstance(candidate_assessment, Mapping):
                    candidate_action = candidate_assessment.get("action")
                normalized_action = str(getattr(candidate_action, "value", candidate_action) or "").upper()
                if normalized_action != "SUITABLE":
                    canonical_action = normalized_action or "UNKNOWN"
                    candidate_reason = getattr(candidate_assessment, "reason", None)
                    if candidate_reason is None and isinstance(candidate_assessment, Mapping):
                        candidate_reason = candidate_assessment.get("reason")
                    canonical_reason = (
                        str(getattr(candidate_reason, "value", candidate_reason) or "").upper()
                        or canonical_reason
                    )
            required_value = getattr(assessment, "required_cost", None)
            if required_value is None and isinstance(assessment, Mapping):
                required_value = assessment.get("required_cost", assessment.get("required_amount"))
            try:
                if required_value is not None and math.isfinite(float(required_value)):
                    evidence["observed_required_capital"] = float(required_value)
            except (TypeError, ValueError, OverflowError):
                pass
            evidence["observed_required_amount"] = evidence.get("observed_required_capital")
        except Exception as exc:
            reason = str(exc).strip() or "RULES_UNKNOWN"
            evidence.update(
                category="RULES_UNKNOWN",
                reason=reason,
                resolver="refresh_market_rules",
                next_action="recheck_next_discovery_tick",
                rules_error=reason,
            )
            return evidence
        if canonical_action in {None, "UNKNOWN"}:
            evidence.update(
                category="RULES_UNKNOWN",
                reason="RULES_UNKNOWN",
                resolver="refresh_market_rules",
                next_action="recheck_next_discovery_tick",
            )
            return evidence
        if canonical_action is not None and canonical_action != "SUITABLE":
            canonical_category = (
                "DATA_QUALITY"
                if canonical_action == "MALFORMED" or canonical_reason == "MALFORMED_BOOK"
                else "CAPITAL_OR_MARKET_CONSTRAINT"
            )
            canonical_reason = canonical_reason or canonical_action
            if canonical_reason == "INSUFFICIENT_DEPTH":
                canonical_reason = "NO_DEPTH"
            evidence.update(
                category=canonical_category,
                reason=canonical_reason,
                resolver=(
                    "refresh_selected_token_order_book"
                    if canonical_category == "DATA_QUALITY"
                    else "inspect_selected_token_depth"
                ),
                next_action="recheck_next_discovery_tick",
            )
            return evidence
        if entry_depth < max(entry_floor, quantity) or exit_depth < max(exit_floor, quantity):
            evidence.update(
                category="CAPITAL_OR_MARKET_CONSTRAINT",
                reason="NO_DEPTH",
                resolver="inspect_selected_token_depth",
                next_action="recheck_next_discovery_tick",
            )
            return evidence
        if activity_floor > 0 and float(snapshot.volume or 0.0) < activity_floor:
            evidence.update(
                category="CAPITAL_OR_MARKET_CONSTRAINT",
                reason="INSUFFICIENT_ACTIVITY",
                resolver="wait_for_market_activity",
                next_action="recheck_next_discovery_tick",
            )
            return evidence
        fresh = True
        if self.config.freshness_sla_seconds is not None:
            age = (ensure_utc(observed_at) - snapshot_timestamp).total_seconds()
            fresh = age <= float(self.config.freshness_sla_seconds)
            if not fresh:
                evidence.update(
                    category="DATA_FRESHNESS",
                    reason="STALE_MARKET",
                    resolver="refresh_market_snapshot",
                    next_action="recheck_next_discovery_tick",
                )
                return evidence
        evidence.update(
            category="SUITABLE_MARKET",
            action="SUITABLE",
            reason="SUITABLE",
            fresh=fresh,
            freshness_rank=0 if fresh else 1,
        )
        return evidence

    @staticmethod
    def _suitability_parameters(document: Mapping[str, Any]) -> dict[str, Any]:
        """Read immutable probe assumptions without silently changing them.

        Scope assumptions are commonly carried beside the canonical policy,
        but older frozen artifacts place them under the plan or an assumptions
        container.  All recognized locations are read in deterministic order;
        conflicting aliases are marked unknown instead of selecting one.
        """
        sources: list[Mapping[str, Any]] = []
        pending: list[Mapping[str, Any]] = [document]
        seen: set[int] = set()
        while pending and len(sources) < 32:
            source = pending.pop(0)
            marker = id(source)
            if marker in seen:
                continue
            seen.add(marker)
            sources.append(source)
            for key in (
                "experiment_plan",
                "market_scope",
                "suitability",
                "suitability_assumptions",
                "assumptions",
                "cost_assumptions",
                "fees",
                "paper_fee_assumptions",
            ):
                child = source.get(key)
                if isinstance(child, Mapping):
                    pending.append(child)

        result: dict[str, Any] = {}
        errors: list[str] = []

        def add_number(name: str, value: Any, *, scale: float = 1.0) -> None:
            try:
                if isinstance(value, bool):
                    raise ValueError
                number = float(value) / scale
            except (TypeError, ValueError, OverflowError):
                errors.append(f"{name}_INVALID")
                return
            if not math.isfinite(number) or number < 0:
                errors.append(f"{name}_INVALID")
                return
            previous = result.get(name)
            if previous is not None and not math.isclose(float(previous), number, rel_tol=0.0, abs_tol=1e-12):
                errors.append(f"{name}_CONFLICT")
                return
            result[name] = number

        for source in sources:
            for name, aliases in (
                ("required_capital", ("required_capital", "min_required_capital")),
                ("min_entry_depth", ("min_entry_depth",)),
                ("min_exit_depth", ("min_exit_depth",)),
                ("min_activity", ("min_activity",)),
            ):
                for alias in aliases:
                    if alias in source:
                        add_number(name, source[alias])
                        break
            if "intended_token" in source:
                token = str(source["intended_token"]).strip().lower()
                if token not in {"yes", "no"}:
                    errors.append("INTENDED_TOKEN_INVALID")
                elif "intended_token" in result and result["intended_token"] != token:
                    errors.append("INTENDED_TOKEN_CONFLICT")
                else:
                    result["intended_token"] = token
            for alias in ("venue_fee_rate", "fee_rate", "feeRate"):
                if alias in source:
                    add_number("venue_fee_rate", source[alias])
                    break
            for alias in ("venue_fee_bps", "fee_bps", "feeRateBps"):
                if alias in source:
                    add_number("venue_fee_rate", source[alias], scale=10_000.0)
                    break
        if errors:
            result["_assumption_error"] = ",".join(dict.fromkeys(errors))
        return result

    @classmethod
    def _suitability_kwargs(cls, document: Mapping[str, Any]) -> dict[str, Any]:
        values = cls._suitability_parameters(document)
        if values.get("_assumption_error"):
            # An invalid or conflicting frozen assumption is never replaced
            # with a process default; force a truthful unknown assessment.
            return {"intended_token": "__unknown__"}
        return {
            key: value
            for key, value in values.items()
            if key in {
                "intended_token",
                "required_capital",
                "min_entry_depth",
                "min_exit_depth",
                "min_activity",
                "venue_fee_rate",
            }
        }

    @classmethod
    def _suitability_is_configured(cls, document: Mapping[str, Any]) -> bool:
        return bool(cls._suitability_parameters(document))

    def _discover_markets_keyset_suitable(
        self,
        observed_at: datetime,
        counters: dict[str, Any],
        *,
        budget: int,
        carry_cursor: Any,
        provider: Any,
        exclude: set[str],
    ) -> tuple[Sequence[PredictionMarketSnapshot], Any, Sequence[str]]:
        """Page through unsuitable rows until a bounded suitable batch appears."""
        previous = (
            dict(self._discovery_continuation)
            if isinstance(self._discovery_continuation, Mapping)
            else {}
        )
        cursor = carry_cursor if isinstance(carry_cursor, str) and carry_cursor.strip() else None
        limit = min(100, max(1, int(budget)))
        # The market budget limits returned candidates, not the number of
        # pages inspected.  A small budget must still be able to advance past
        # an arbitrary bounded run of unsuitable pages.
        max_pages = max(1, int(self.config.max_suitable_pages_per_cycle))
        selected: list[PredictionMarketSnapshot] = []
        suitable_evidence: list[dict[str, Any]] = []
        deferred: list[str] = []
        exclusions: list[dict[str, Any]] = [
            dict(item)
            for item in previous.get("suitability_exclusions", ())
            if isinstance(item, Mapping)
        ]
        page_count = 0
        seen_cursors: list[str] = []
        raw_total = unique_total = duplicate_total = malformed_total = 0
        while page_count < max_pages:
            kwargs: dict[str, Any] = {
                "limit": limit,
                "after_cursor": cursor,
                "closed": not self.config.active,
                "include_tag": True,
            }
            method_page = getattr(provider, "market_page")
            try:
                parameters = inspect.signature(method_page).parameters
            except (TypeError, ValueError):
                parameters = {}
            if parameters and not any(p.kind is inspect.Parameter.VAR_KEYWORD for p in parameters.values()):
                kwargs = {key: value for key, value in kwargs.items() if key in parameters}
            try:
                page = self._call_provider(
                    "discovery_keyset:/markets/keyset",
                    lambda: method_page(**kwargs),
                    observed_at,
                    counters,
                    provider=provider,
                )
            except Exception as exc:
                counters["errors"] += 1
                timeout_fields = (
                    {
                        "timeout_reason": str(exc).split(":", 1)[0],
                        "timeout_endpoint": getattr(exc, "endpoint", self._current_endpoint),
                        "timeout_seconds": float(self.config.provider_timeout_seconds),
                        "resolver": "retry_provider_call",
                        "next_action": "retry_next_collection_tick",
                    }
                    if isinstance(exc, _ProviderDeadlineExceeded)
                    else {}
                )
                self._discovery_continuation = {
                    **dict(self._discovery_continuation or {}),
                    "coverage_status": "ERROR",
                    "after_cursor": cursor,
                    "error_reason": str(exc),
                    **timeout_fields,
                    "updated_at": observed_at.isoformat(),
                }
                self.store.save_collection_error(None, observed_at, "discovery", str(exc))
                return tuple(selected), cursor, tuple(dict.fromkeys(deferred))
            if page is None or (
                not isinstance(page, (Mapping, list, tuple))
                and not hasattr(page, "snapshots")
            ):
                counters["errors"] += 1
                self._discovery_continuation = {
                    **previous,
                    "request_path": "/markets/keyset",
                    "request_query": {**kwargs, "after_cursor": cursor},
                    "after_cursor": None,
                    "opaque_cursor": None,
                    "cursor": None,
                    "coverage_status": "ERROR",
                    "error_reason": "INVALID_PAGE",
                    "query_reset": True,
                    "rebase_required": True,
                    "seen_cursor_history": [],
                    "updated_at": observed_at.isoformat(),
                }
                self.store.save_collection_error(None, observed_at, "discovery", "invalid discovery page")
                return tuple(selected), None, tuple(dict.fromkeys(deferred))
            page_status = str(self._scope_page_value(page, "coverage_status", "") or "").upper()
            if page_status == "ERROR":
                counters["errors"] += 1
                self._discovery_continuation = {
                    **dict(self._discovery_continuation or {}),
                    "coverage_status": "ERROR",
                    "after_cursor": cursor,
                    "error_reason": str(
                        self._scope_page_value(page, "error_reason", "provider page error")
                    ),
                    "updated_at": observed_at.isoformat(),
                }
                self.store.save_collection_error(
                    None,
                    observed_at,
                    "discovery",
                    str(self._scope_page_value(page, "error_reason", "provider page error")),
                )
                return tuple(selected), cursor, tuple(dict.fromkeys(deferred))
            raw_items = self._scope_page_value(page, "snapshots", _UNSET)
            if raw_items is _UNSET and isinstance(page, Mapping):
                raw_items = page.get("markets", page.get("data", ()))
            if raw_items is _UNSET:
                raw_items = page if isinstance(page, (list, tuple)) else ()
            try:
                raw_values = list(raw_items or ())
            except TypeError:
                raw_values = []
            snapshots = [item for item in raw_values if isinstance(item, PredictionMarketSnapshot)]
            malformed = len(raw_values) - len(snapshots)
            raw_total += self._scope_count(self._scope_page_value(page, "raw_count", len(raw_values)), len(raw_values))
            malformed_total += max(malformed, self._scope_count(self._scope_page_value(page, "malformed_count", 0)))
            page_ids: set[str] = set()
            unique_page: list[PredictionMarketSnapshot] = []
            for snapshot in snapshots:
                if snapshot.market_id in page_ids:
                    duplicate_total += 1
                    continue
                page_ids.add(snapshot.market_id)
                unique_page.append(snapshot)
            unique_total += len(unique_page)
            next_cursor = self._scope_page_value(page, "next_cursor", None)
            if next_cursor is not None and (
                not isinstance(next_cursor, str)
                or not next_cursor.strip()
                or next_cursor == cursor
                or next_cursor in seen_cursors
            ):
                counters["errors"] += 1
                reason = (
                    "REPEATED_CURSOR"
                    if isinstance(next_cursor, str)
                    and (next_cursor == cursor or next_cursor in seen_cursors)
                    else "INVALID_NEXT_CURSOR"
                )
                self._discovery_continuation = {
                    **previous,
                    "request_path": "/markets/keyset",
                    "request_query": {**kwargs, "after_cursor": cursor},
                    "after_cursor": None,
                    "opaque_cursor": None,
                    "cursor": None,
                    "coverage_status": "ERROR",
                    "error_reason": reason,
                    "query_reset": True,
                    "rebase_required": True,
                    "seen_cursor_history": [],
                    "suitable_market_ids": [item.market_id for item in selected],
                    "updated_at": observed_at.isoformat(),
                }
                self.store.save_collection_error(None, observed_at, "discovery", reason)
                return tuple(selected), None, tuple(dict.fromkeys(deferred))
            candidates: list[tuple[PredictionMarketSnapshot, dict[str, Any]]] = []
            for snapshot in unique_page:
                if snapshot.market_id in exclude:
                    continue
                assessment = self._suitable_market_assessment(
                    snapshot,
                    observed_at,
                    provider,
                    counters=counters,
                )
                self._suitable_market_evidence.append(assessment)
                if assessment.get("action") == "SUITABLE":
                    candidates.append((snapshot, assessment))
                else:
                    deferred.append(snapshot.market_id)
                    exclusions.append(assessment)
            candidates.sort(
                key=lambda item: (
                    int(item[1].get("freshness_rank", 1)),
                    -float(item[1].get("freshness_score", 0.0)),
                    -float(item[1].get("activity_score", 0.0)),
                    -float(item[1].get("depth_score", 0.0)),
                    -float(item[1].get("entry_depth", 0.0)),
                    -float(item[1].get("exit_depth", 0.0)),
                    str(item[0].market_id),
                )
            )
            for snapshot, assessment in candidates:
                if len(selected) >= budget:
                    deferred.append(snapshot.market_id)
                    continue
                selected.append(snapshot)
                suitable_evidence.append(dict(assessment))
            page_count += 1
            cursor = next_cursor if isinstance(next_cursor, str) and next_cursor.strip() else None
            if selected or cursor is None:
                break
            seen_cursors.append(cursor)
        status = (
            page_status
            if cursor is None and page_status in {"PARTIAL", "BUDGET_EXHAUSTED"}
            else ("COMPLETE" if cursor is None else "BUDGET_EXHAUSTED")
        )
        if malformed_total:
            counters["errors"] += malformed_total
            status = "PARTIAL"
            self.store.save_collection_error(
                None,
                observed_at,
                "discovery_malformed_rows",
                f"{malformed_total} malformed public market rows",
            )
        self._discovery_continuation = {
            "request_path": "/markets/keyset",
            "request_query": {"limit": limit, "after_cursor": cursor, "closed": not self.config.active, "include_tag": True},
            "after_cursor": cursor,
            "coverage_status": status,
            "raw_count": raw_total,
            "unique_count": unique_total,
            "duplicate_count": duplicate_total,
            "malformed_count": malformed_total,
            "suitability_exclusions": exclusions[-256:],
            "suitable_market_ids": [item.market_id for item in selected],
            "suitable_market_evidence": suitable_evidence[-256:],
            "updated_at": observed_at.isoformat(),
        }
        return tuple(selected), cursor, tuple(dict.fromkeys(deferred))

    @staticmethod
    def _scope_result_market_ids(result: Any) -> list[str]:
        raw = getattr(result, "matched_markets", _UNSET)
        if raw is _UNSET and isinstance(result, Mapping):
            raw = result.get("matched_markets", result.get("matched", ()))
        if raw is _UNSET or raw is None:
            return []
        if isinstance(raw, Mapping):
            raw = raw.values()
        if isinstance(raw, (str, bytes)):
            raw = (raw,)
        try:
            values = list(raw)
        except TypeError:
            values = [raw]
        result_ids: list[str] = []
        for item in values:
            if isinstance(item, str):
                identifier = item.strip()
            elif isinstance(item, Mapping):
                identifier = str(item.get("market_id", item.get("id", ""))).strip()
            else:
                identifier = str(getattr(item, "market_id", "")).strip()
            if identifier and identifier not in result_ids:
                result_ids.append(identifier)
        return result_ids

    def run_forever(
        self,
        *,
        cycles: int | None = None,
        stop_event: Any | None = None,
        on_cycle: Callable[[CollectionCycle], Any] | None = None,
        retain_cycles: int | None = None,
    ) -> list[CollectionCycle]:
        """Run finite cycles or continue until ``stop_event`` with bounded memory."""
        if cycles is not None and (isinstance(cycles, bool) or cycles < 0):
            raise ValueError("cycles must be non-negative or None")
        retained = self.config.retain_cycles if retain_cycles is None else int(retain_cycles)
        if retained <= 0:
            raise ValueError("retain_cycles must be positive")
        results: list[CollectionCycle] = []
        completed = 0
        try:
            while cycles is None or completed < cycles:
                if stop_event is not None and stop_event.is_set():
                    break
                cycle = self.collect_once()
                results.append(cycle)
                if len(results) > retained:
                    del results[:-retained]
                if on_cycle is not None:
                    on_cycle(cycle)
                completed += 1
                if cycles is not None and completed >= cycles:
                    break
                if stop_event is not None and stop_event.is_set():
                    break
                self.sleep(self.config.interval_seconds)
        finally:
            self.close()
        return results

    def _discover_markets(
        self,
        observed_at: datetime,
        counters: dict[str, Any],
        *,
        budget: int,
        carry_cursor: Any = None,
        provider: Any | None = None,
        exclude: set[str] | None = None,
    ) -> tuple[Sequence[PredictionMarketSnapshot], Any, Sequence[str]]:
        provider = provider or self.provider
        excluded = exclude or set()
        budget = max(0, int(budget))
        if budget <= 0:
            self._discovery_continuation = {
                **dict(self._discovery_continuation or {}),
                "coverage_status": "BUDGET_EXHAUSTED",
                "updated_at": observed_at.isoformat(),
            }
            return (), carry_cursor, ()

        # Real Gamma adapters expose the documented keyset endpoint.  Keep the
        # legacy offset path only for in-memory/test providers without it.
        method_page = getattr(provider, "market_page", None)
        if callable(method_page):
            return self._discover_markets_keyset_suitable(
                observed_at,
                counters,
                budget=budget,
                carry_cursor=carry_cursor,
                provider=provider,
                exclude=excluded,
            )

        method = getattr(provider, "markets", None)
        if not callable(method):
            self._discovery_continuation = {
                **dict(self._discovery_continuation or {}),
                "coverage_status": "ERROR",
                "error_reason": "provider has no market discovery method",
                "updated_at": observed_at.isoformat(),
            }
            return (), carry_cursor, ()
        kwargs = {"active": self.config.active}
        try:
            parameters = inspect.signature(method).parameters
        except (TypeError, ValueError):
            parameters = {}
        accepts_kwargs = any(
            parameter.kind is inspect.Parameter.VAR_KEYWORD
            for parameter in parameters.values()
        )
        try:
            legacy_cursor = int(carry_cursor or 0)
        except (TypeError, ValueError):
            legacy_cursor = 0
        if "limit" in parameters or accepts_kwargs or not parameters:
            kwargs["limit"] = min(
                100,
                max(self.config.max_markets, self.config.discovery_budget_per_cycle, legacy_cursor + budget + 1),
            )
        values = self._call_provider(
            "discovery",
            lambda: method(**kwargs),
            observed_at,
            counters,
            provider=provider,
        ) or ()
        snapshots = [item for item in values if isinstance(item, PredictionMarketSnapshot)]
        if not snapshots:
            self._discovery_continuation = {
                "coverage_status": "NO_MATCHING_MARKETS",
                "after_cursor": legacy_cursor,
                "updated_at": observed_at.isoformat(),
            }
            return (), legacy_cursor, ()
        offset = legacy_cursor % len(snapshots)
        rotated = snapshots[offset:] + snapshots[:offset]
        selected: list[PredictionMarketSnapshot] = []
        deferred: list[str] = []
        suitability_exclusions: list[dict[str, Any]] = [
            dict(item)
            for item in (
                (self._discovery_continuation or {}).get("suitability_exclusions", ())
                if isinstance(self._discovery_continuation, Mapping)
                else ()
            )
            if isinstance(item, Mapping)
        ]
        unsuitable_count = 0
        for item in rotated:
            if item.market_id in excluded:
                continue
            assessment = self._suitable_market_assessment(
                item,
                observed_at,
                provider,
                counters=counters,
            )
            self._suitable_market_evidence.append(assessment)
            if assessment.get("action") != "SUITABLE":
                deferred.append(item.market_id)
                suitability_exclusions.append(assessment)
                unsuitable_count += 1
                continue
            if len(selected) < budget:
                selected.append(item)
            else:
                deferred.append(item.market_id)
        scanned = len(selected) + unsuitable_count + sum(
            1 for item in rotated if item.market_id in excluded
        )
        next_cursor = (offset + scanned) % len(snapshots)
        self._discovery_continuation = {
            "coverage_status": "PARTIAL",
            "after_cursor": next_cursor,
            "suitability_exclusions": suitability_exclusions[-256:],
            "suitable_market_ids": [item.market_id for item in selected],
            "updated_at": observed_at.isoformat(),
        }
        return selected, next_cursor, deferred

    def _collect_market(
        self,
        market_id: str,
        discovered: PredictionMarketSnapshot | None,
        observed_at: datetime,
        *,
        point_in_time: bool = False,
        provider: Any | None = None,
        force: bool = False,
    ) -> dict[str, Any]:
        provider = provider or self.provider
        counters = self._new_counters()
        state_key = f"{self.config.collector_name}:{market_id}"
        state = self.store.get_collector_state(state_key) or {}

        def finish(failed: bool = False) -> dict[str, Any]:
            counters["markets_attempted"] = 1
            counters["markets_failed"] = 1 if failed else 0
            counters["markets_successful"] = 0 if failed else 1
            return {"counters": counters, "attempted": 1}

        if not force and not self._poll_due(market_id, discovered, state, observed_at):
            counters["skipped_markets"] += 1
            return {"counters": counters, "attempted": 0}
        cooldown_until = _parse_iso(state.get("cooldown_until"))
        if cooldown_until is not None and observed_at < cooldown_until:
            counters["cooldowns"] += 1
            counters["skipped_markets"] += 1
            return {"counters": counters, "attempted": 0}

        market_request_started_at = observed_at if point_in_time else ensure_utc(self.clock())
        snapshot: PredictionMarketSnapshot | None = None
        market_error = False
        try:
            snapshot = self._call_provider(
                f"market:{market_id}",
                lambda: provider.market(market_id),
                observed_at,
                counters,
                provider=provider,
                market_id=market_id,
            )
        except Exception as exc:
            market_error = True
            counters["errors"] += 1
            self.store.save_collection_error(market_id, observed_at, "market", str(exc))
            snapshot = discovered
        market_response_received_at = observed_at if point_in_time else ensure_utc(self.clock())
        if snapshot is None and discovered is not None:
            snapshot = discovered
        if snapshot is None:
            if not market_error:
                counters["errors"] += 1
                self.store.save_collection_error(market_id, observed_at, "market_unavailable", "market metadata unavailable")
            self.store.set_collector_state(
                state_key,
                {
                    **state,
                    "market_id": market_id,
                    "last_attempt_at": observed_at.isoformat(),
                    "cooldown_until": self._cooldown_until(observed_at).isoformat(),
                    "errors": int(state.get("errors", 0)) + int(counters["errors"]),
                },
            )
            return finish(True)

        market_provider_timestamp = self._provider_timestamp(
            provider,
            market_id,
            "market",
            snapshot,
            observed_at=observed_at,
            counters=counters,
        )
        market_future_data: list[str] = []
        if not self._valid_provider_timestamp(
            snapshot.timestamp,
            market_request_started_at,
            market_response_received_at,
            skew_seconds=self.config.max_provider_clock_skew_seconds,
        ):
            market_future_data.append("market")
        elif market_provider_timestamp is not None and not self._valid_provider_timestamp(
            market_provider_timestamp,
            market_request_started_at,
            market_response_received_at,
            skew_seconds=self.config.max_provider_clock_skew_seconds,
        ):
            market_future_data.append("market provider")
        if market_future_data:
            counters["errors"] += len(market_future_data)
            self.store.save_collection_error(
                market_id,
                observed_at,
                "future_observation",
                ", ".join(market_future_data) + " timestamp is after response plus allowed skew",
            )
            self._save_market_state(
                state_key, state, market_id, observed_at, counters, 0, cooldown=True
            )
            return finish(True)

        metadata_request_started_at = observed_at if point_in_time else ensure_utc(self.clock())
        metadata_error = False
        try:
            metadata = self._call_provider(
                f"metadata:{market_id}",
                lambda: provider.metadata(market_id),
                observed_at,
                counters,
                provider=provider,
                market_id=market_id,
            )
        except Exception as exc:
            metadata_error = True
            metadata = None
            counters["errors"] += 1
            self.store.save_collection_error(market_id, observed_at, "metadata", str(exc))
        metadata_response_received_at = observed_at if point_in_time else ensure_utc(self.clock())
        if metadata is None:
            counters["metadata_failures"] += 1
            if not metadata_error:
                counters["errors"] += 1
                self.store.save_collection_error(market_id, observed_at, "metadata_unavailable", "provider returned no metadata")
        terminal_states = {
            SettlementState.RESOLVED_YES,
            SettlementState.RESOLVED_NO,
            SettlementState.VOID,
        }
        closed = snapshot.settlement in terminal_states or (
            snapshot.expiry is not None and snapshot.expiry <= metadata_response_received_at
        )
        metadata_payload: dict[str, Any] = {
            "source_type": "FORWARD_COLLECTED",
            "snapshot": to_record(snapshot),
            "metadata": to_record(metadata) if metadata is not None else None,
            "token_ids": {"yes": snapshot.yes_token_id, "no": snapshot.no_token_id},
            "source": getattr(provider, "provider_name", type(provider).__name__),
            "metadata_available": metadata is not None,
            "active": not closed,
            "closed": closed,
        }
        metadata_identity = {key: value for key, value in metadata_payload.items()}
        metadata_hash = hashlib.sha256(_stable_payload(metadata_identity).encode("utf-8")).hexdigest()
        if self.store.save_polymarket_market_metadata(
            market_id,
            metadata_payload,
            observed_at=metadata_response_received_at,
            metadata_hash=metadata_hash,
            source_type="FORWARD_COLLECTED",
        ):
            counters["metadata_inserted"] += 1

        books_request_started_at = observed_at if point_in_time else ensure_utc(self.clock())
        try:
            books = dict(
                self._call_provider(
                    f"order_books:{market_id}",
                    lambda: provider.order_books(market_id, depth=self.config.depth),
                    observed_at,
                    counters,
                    provider=provider,
                    market_id=market_id,
                ) or {}
            )
        except Exception as exc:
            counters["errors"] += 1
            counters["order_book_failures"] += 1
            self.store.save_collection_error(market_id, observed_at, "order_book", str(exc))
            self._save_market_state(
                state_key, state, market_id, observed_at, counters, 0, cooldown=True
            )
            return finish(True)
        books_retrieved_at = observed_at if point_in_time else ensure_utc(self.clock())
        collection_observed_at = max(observed_at, metadata_response_received_at, books_retrieved_at)
        yes_book = books.get("yes")
        no_book = books.get("no")
        if not isinstance(yes_book, OrderBookSnapshot):
            yes_book = None
        if not isinstance(no_book, OrderBookSnapshot):
            no_book = None
        yes_provider_timestamp = self._provider_timestamp(
            provider,
            market_id,
            "yes_order_book",
            yes_book,
            observed_at=observed_at,
            counters=counters,
        )
        no_provider_timestamp = self._provider_timestamp(
            provider,
            market_id,
            "no_order_book",
            no_book,
            observed_at=observed_at,
            counters=counters,
        )
        future_data: list[str] = []
        for label, book, provider_stamp in (
            ("yes order book", yes_book, yes_provider_timestamp),
            ("no order book", no_book, no_provider_timestamp),
        ):
            if book is not None and not self._valid_provider_timestamp(
                book.timestamp,
                books_request_started_at,
                books_retrieved_at,
                skew_seconds=self.config.max_provider_clock_skew_seconds,
            ):
                future_data.append(label)
            elif provider_stamp is not None and not self._valid_provider_timestamp(
                provider_stamp,
                books_request_started_at,
                books_retrieved_at,
                skew_seconds=self.config.max_provider_clock_skew_seconds,
            ):
                future_data.append(label + " provider")
        if future_data:
            counters["errors"] += len(future_data)
            self.store.save_collection_error(
                market_id,
                collection_observed_at,
                "future_observation",
                ", ".join(future_data) + " timestamp is after collection observation",
            )
            self._save_market_state(
                state_key, state, market_id, collection_observed_at, counters, 0, cooldown=True
            )
            return finish(True)
        provider_stamps = [
            stamp for stamp in (market_provider_timestamp, yes_provider_timestamp, no_provider_timestamp)
            if stamp is not None
        ]
        provider_timestamp = max(provider_stamps) if provider_stamps else None
        canonical, canonical_source_timestamp = _canonical_snapshot(snapshot, yes_book, no_book)
        quality = ResearchQuality.ORDER_BOOK_SIMULATED if yes_book is not None or no_book is not None else ResearchQuality.PRICE_PROXY
        # ``source_timestamp`` is the exact timestamp persisted in the
        # canonical snapshot.  Provider timestamps remain separate evidence
        # and may legitimately be unavailable.
        source_timestamp = canonical_source_timestamp
        payload = {
            "source_type": "FORWARD_COLLECTED",
            "snapshot": to_record(canonical),
            "yes_order_book": to_record(yes_book) if yes_book is not None else None,
            "no_order_book": to_record(no_book) if no_book is not None else None,
            "quotes": {
                "yes_bid": canonical.yes_bid,
                "yes_ask": canonical.yes_ask,
                "yes_spread": canonical.yes_spread,
                "no_bid": canonical.no_bid,
                "no_ask": canonical.no_ask,
                "no_spread": canonical.no_spread,
            },
            "depth": {"yes": _book_depth(yes_book), "no": _book_depth(no_book)},
            "volume": canonical.volume,
            "liquidity": canonical.liquidity,
            "request_started_at": books_request_started_at.isoformat(),
            "provider_timestamp": provider_timestamp.isoformat() if provider_timestamp else None,
            "response_received_at": books_retrieved_at.isoformat(),
            "observed_at": collection_observed_at.isoformat(),
            "source_timestamp": source_timestamp.isoformat(),
            "time_to_resolution_seconds": (
                max(0.0, (canonical.expiry - source_timestamp).total_seconds()) if canonical.expiry else None
            ),
            "yes_token_id": canonical.yes_token_id,
            "no_token_id": canonical.no_token_id,
            "settlement": canonical.settlement.value,
            "research_quality": quality.value,
            "metadata_available": metadata is not None,
        }
        snapshot_id = hashlib.sha256(_stable_payload(payload).encode("utf-8")).hexdigest()
        if self.store.save_polymarket_snapshot(
            snapshot_id,
            market_id,
            source_timestamp,
            collection_observed_at,
            payload,
            quality=quality,
            source_type="FORWARD_COLLECTED",
        ):
            counters["snapshots_inserted"] += 1
        else:
            counters["snapshot_duplicates"] += 1
        last_trade = _parse_iso(state.get("last_trade_timestamp"))
        raw_trade_cursor = state.get("last_trade_cursor")
        last_trade_cursor = (
            str(raw_trade_cursor).strip()
            if raw_trade_cursor not in (None, "")
            else None
        )
        trade_fetch_failed = False
        try:
            trades = self._fetch_trades(
                market_id,
                last_trade,
                collection_observed_at,
                counters,
                cursor=last_trade_cursor,
                provider=provider,
            )
        except Exception as exc:
            trades = ()
            counters["errors"] += 1
            counters["trade_failures"] += 1
            self.store.save_collection_error(market_id, collection_observed_at, "trades", str(exc))
            trade_fetch_failed = True
        latest_trade: datetime | None = last_trade
        for trade in trades or ():
            if not isinstance(trade, TradePrint):
                counters["errors"] += 1
                self.store.save_collection_error(market_id, collection_observed_at, "malformed_trade", "provider returned non-TradePrint")
                continue
            if trade.timestamp > collection_observed_at:
                counters["errors"] += 1
                self.store.save_collection_error(market_id, collection_observed_at, "future_trade", "trade timestamp is after collection observation")
                continue
            provenance: Mapping[str, Any] = {}
            provenance_getter = getattr(provider, "trade_provenance", None)
            if callable(provenance_getter):
                try:
                    candidate_provenance = self._call_provider(
                        f"trade_provenance:{market_id}",
                        lambda: provenance_getter(trade),
                        collection_observed_at,
                        counters,
                        provider=provider,
                        market_id=market_id,
                    )
                except _ProviderDeadlineExceeded as exc:
                    counters["errors"] += 1
                    counters["trade_failures"] += 1
                    self.store.save_collection_error(
                        market_id,
                        collection_observed_at,
                        "trade_provenance_timeout",
                        str(exc),
                    )
                    trade_fetch_failed = True
                    break
                except (TypeError, ValueError):
                    candidate_provenance = {}
                if isinstance(candidate_provenance, Mapping):
                    # Query and collection time describe this observation, not
                    # the immutable print.  Keeping them out of the stored
                    # evidence prevents a repeated poll from conflicting with
                    # the original trade payload.
                    provenance = {
                        key: candidate_provenance[key]
                        for key in (
                            "source_type",
                            "provider",
                            "endpoint",
                            "condition_id",
                            "source_identity",
                            "response_timestamp",
                        )
                        if key in candidate_provenance
                    }
            trade_payload: Mapping[str, Any] = {
                **to_record(trade),
                "source_type": "FORWARD_COLLECTED",
                "source_timestamp": trade.timestamp.isoformat(),
                "provenance": dict(provenance),
            }
            if self.store.save_polymarket_trade(market_id, trade_payload):
                counters["trades_inserted"] += 1
            else:
                counters["trade_duplicates"] += 1
            latest_trade = max(latest_trade, trade.timestamp) if latest_trade else trade.timestamp
        trade_cursor: str | None = None
        if trade_fetch_failed:
            latest_trade = last_trade
            trade_cursor = last_trade_cursor
        elif not getattr(provider, "last_trades_complete", True):
            latest_trade = last_trade
            provider_cursor = str(getattr(provider, "last_trade_cursor", "")).strip() or None
            trade_cursor = provider_cursor or last_trade_cursor
        self._save_market_state(
            state_key,
            state,
            market_id,
            collection_observed_at,
            counters,
            0,
            source_timestamp=canonical_source_timestamp,
            latest_trade=latest_trade,
            trade_cursor=trade_cursor,
        )
        return finish(bool(counters["errors"]))

    @staticmethod
    def _valid_provider_timestamp(
        timestamp: datetime | None,
        request_started_at: datetime,
        response_received_at: datetime,
        *,
        skew_seconds: float,
    ) -> bool:
        if timestamp is None:
            return True
        try:
            stamp = ensure_utc(timestamp)
            received = ensure_utc(response_received_at)
            skew = max(0.0, float(skew_seconds))
        except (TypeError, ValueError, OverflowError):
            return False
        return stamp <= received + timedelta(seconds=skew)

    def _provider_timestamp(
        self,
        provider: Any,
        market_id: str,
        kind: str,
        value: Any,
        *,
        observed_at: datetime | None = None,
        counters: dict[str, Any] | None = None,
    ) -> datetime | None:
        getter = getattr(provider, "provider_timestamp_for", None)
        if callable(getter):
            def read_stamp() -> Any:
                try:
                    return getter(market_id, kind=kind)
                except (TypeError, ValueError):
                    try:
                        return getter(market_id, kind)
                    except (TypeError, ValueError):
                        return None

            if observed_at is not None and counters is not None:
                stamp = self._call_provider(
                    f"provider_timestamp:{kind}:{market_id}",
                    read_stamp,
                    observed_at,
                    counters,
                    provider=provider,
                    market_id=market_id,
                )
            else:
                stamp = read_stamp()
            if stamp is not None:
                try:
                    return ensure_utc(stamp)
                except (TypeError, ValueError):
                    return None
            return None
        stamp = getattr(value, "timestamp", None)
        try:
            return ensure_utc(stamp) if stamp is not None else None
        except (TypeError, ValueError):
            return None
    def _fetch_trades(
        self,
        market_id: str,
        last_trade: datetime | None,
        observed_at: datetime,
        counters: dict[str, Any],
        *,
        cursor: str | None = None,
        provider: Any | None = None,
    ) -> Sequence[TradePrint]:
        provider = provider or self.provider

        def call() -> Sequence[TradePrint]:
            method = provider.trades
            kwargs: dict[str, Any] = {"start": last_trade, "end": observed_at}
            try:
                parameters = inspect.signature(method).parameters
            except (TypeError, ValueError):
                parameters = {}
            accepts_kwargs = any(
                parameter.kind is inspect.Parameter.VAR_KEYWORD
                for parameter in parameters.values()
            )
            if not parameters or "max_pages" in parameters or accepts_kwargs:
                kwargs["max_pages"] = self.config.max_trade_pages
            if cursor is not None and (not parameters or "cursor" in parameters or accepts_kwargs):
                kwargs["cursor"] = cursor
            try:
                return method(market_id, **kwargs)
            except TypeError as exc:
                message = str(exc)
                if "max_pages" in kwargs and "max_pages" in message:
                    kwargs.pop("max_pages")
                    return method(market_id, **kwargs)
                if "cursor" in kwargs and "cursor" in message:
                    kwargs.pop("cursor")
                    return method(market_id, **kwargs)
                raise

        return self._call_provider(
            f"trades:{market_id}",
            call,
            observed_at,
            counters,
            provider=provider,
            market_id=market_id,
        ) or ()
    def _provider_phase_timeout(
        self,
        pool_name: str,
        remaining: float | None,
    ) -> float:
        """Allocate bounded provider windows for each scope phase."""
        timeout = float(self.config.provider_timeout_seconds)
        if remaining is None or pool_name not in {"scope", "scope_direct"}:
            return timeout if remaining is None else max(
                0.000001,
                min(timeout, remaining),
            )
        downstream_reserve = self._downstream_collection_reserve_seconds()
        if (
            self._scope_inventory_budget_remaining is None
            or self._scope_direct_budget_remaining is None
        ):
            available = max(0.000002, remaining - downstream_reserve)
            share = available * 0.5
            self._scope_inventory_budget_remaining = share
            self._scope_direct_budget_remaining = share
        budget = (
            self._scope_inventory_budget_remaining
            if pool_name == "scope"
            else self._scope_direct_budget_remaining
        )
        return max(
            0.000001,
            min(timeout, budget or 0.0, remaining - downstream_reserve),
        )

    def _consume_provider_phase_budget(self, pool_name: str, elapsed: float) -> None:
        if pool_name == "scope":
            if self._scope_inventory_budget_remaining is not None:
                self._scope_inventory_budget_remaining = max(
                    0.0,
                    self._scope_inventory_budget_remaining - max(0.0, elapsed),
                )
        elif pool_name == "scope_direct":
            if self._scope_direct_budget_remaining is not None:
                self._scope_direct_budget_remaining = max(
                    0.0,
                    self._scope_direct_budget_remaining - max(0.0, elapsed),
                )

    def _provider_in_flight_wait_timeout(self, pool_name: str) -> float:
        """Bound retries on prior work without consuming downstream reserve."""
        return self._provider_phase_timeout(pool_name, self._cycle_remaining_seconds())


    def _submit_provider_call(
        self,
        provider: Any,
        endpoint: str,
        operation: Callable[[], Any],
        *,
        pool_name: str | None = None,
    ) -> tuple[Future[Any], tuple[int, str]]:
        pool_name = pool_name or ("scope" if self._scope_phase_active else "collection")
        key = (id(provider), f"{pool_name}:{endpoint}")
        with self._provider_executor_lock:
            if self._closed:
                raise RuntimeError("collector is closed")
            if key in self._active_provider_calls:
                existing = self._provider_futures.get(key)
                if existing is not None and not existing.done():
                    wait_timeout = self._provider_in_flight_wait_timeout(pool_name)
                    try:
                        existing.result(timeout=wait_timeout)
                    except FutureTimeout:
                        pass
                    except BaseException:
                        pass
                if existing is not None and existing.done():
                    self._active_provider_calls.discard(key)
                    self._provider_futures.pop(key, None)
                    self._provider_call_pools.pop(key, None)
                else:
                    # Never wait beyond this tick on an operation owned by an
                    # earlier tick; its provider thread is daemonized and the
                    # next tick must remain bounded while preserving dedup.
                    raise _ProviderDeadlineExceeded(
                        endpoint,
                        float(self.config.provider_timeout_seconds),
                        in_flight=True,
                    )
            for active_key in tuple(self._active_provider_calls):
                existing = self._provider_futures.get(active_key)
                if existing is not None and existing.done():
                    self._active_provider_calls.discard(active_key)
                    self._provider_futures.pop(active_key, None)
                    self._provider_call_pools.pop(active_key, None)
            active_direct_keys = [
                active_key
                for active_key in self._active_provider_calls
                if self._provider_call_pools.get(active_key) == "scope_direct"
            ]
            if pool_name == "scope_direct" and len(active_direct_keys) >= 2:
                raise _ProviderDeadlineExceeded(
                    endpoint,
                    float(self.config.provider_timeout_seconds),
                    in_flight=True,
                )
            active_limit = 1 if pool_name == "scope_direct" else self.config.max_concurrency
            active_for_provider = [
                active_endpoint
                for active_provider_id, active_endpoint in self._active_provider_calls
                if (
                    active_provider_id == id(provider)
                    and self._provider_call_pools.get(
                        (active_provider_id, active_endpoint)
                    ) == pool_name
                )
            ]
            if len(active_for_provider) >= active_limit:
                active_endpoint = active_for_provider[0]
                existing = self._provider_futures.get((id(provider), active_endpoint))
                if existing is not None and not existing.done():
                    wait_timeout = self._provider_in_flight_wait_timeout(pool_name)
                    try:
                        existing.result(timeout=wait_timeout)
                    except FutureTimeout:
                        pass
                    except BaseException:
                        pass
                if existing is not None and existing.done():
                    self._active_provider_calls.discard((id(provider), active_endpoint))
                    self._provider_futures.pop((id(provider), active_endpoint), None)
                    self._provider_call_pools.pop((id(provider), active_endpoint), None)
                else:
                    raise _ProviderDeadlineExceeded(
                        active_endpoint,
                        float(self.config.provider_timeout_seconds),
                        in_flight=True,
                    )
            if pool_name == "scope_direct" and not any(
                active_provider_id == id(provider)
                for active_provider_id, _active_endpoint in self._active_provider_calls
            ):
                # A timed-out daemon call can finish after its caller leaves
                # _call_provider.  Clear advisories left by that prior clone
                # operation before this new exact request starts.
                self._drain_provider_advisories_now(provider)
            if pool_name == "scope":
                executor = self._scope_provider_executor
                if executor is None:
                    executor = _BoundedProviderExecutor(self.config.max_concurrency)
                    self._scope_provider_executor = executor
            elif pool_name == "scope_direct":
                executor = self._scope_direct_provider_executor
                if executor is None:
                    executor = _BoundedProviderExecutor(2)
                    self._scope_direct_provider_executor = executor
            else:
                executor = getattr(self, "_collection_provider_executor", None)
                if executor is None:
                    executor = _BoundedProviderExecutor(self.config.max_concurrency)
                    self._collection_provider_executor = executor
            self._active_provider_calls.add(key)
            self._provider_call_pools[key] = pool_name
            try:
                future = executor.submit(operation)
            except BaseException:
                self._active_provider_calls.discard(key)
                self._provider_futures.pop(key, None)
                self._provider_call_pools.pop(key, None)
                raise
            self._provider_futures[key] = future
        future.add_done_callback(lambda _future: self._release_provider_call(key))
        return future, key

    def _release_provider_call(self, key: tuple[int, str]) -> None:
        with self._provider_executor_lock:
            self._provider_call_pools.pop(key, None)
            self._provider_futures.pop(key, None)
            self._active_provider_calls.discard(key)

    def _record_provider_timeout(
        self,
        endpoint: str,
        observed_at: datetime,
        counters: dict[str, Any],
        *,
        in_flight: bool = False,
        cycle_expired: bool = False,
    ) -> _ProviderDeadlineExceeded:
        timeout = float(self.config.provider_timeout_seconds)
        error = _ProviderDeadlineExceeded(
            endpoint,
            timeout,
            in_flight=in_flight,
            cycle_expired=cycle_expired,
        )
        reason = (
            "COLLECTOR_CYCLE_DEADLINE_EXCEEDED"
            if cycle_expired
            else ("PROVIDER_CALL_IN_FLIGHT" if in_flight else "PROVIDER_CALL_TIMEOUT")
        )
        self._set_current_stage("provider_timeout", endpoint, observed_at, persist=True)
        evidence = {
            "endpoint": str(endpoint),
            "reason": reason,
            "resolver": "retry_provider_call",
            "next_action": "retry_next_collection_tick",
            "retryable": True,
            "timeout_seconds": timeout,
            "observed_at": ensure_utc(observed_at).isoformat(),
        }
        evidence["current_stage"] = self._current_stage
        evidence["current_endpoint"] = self._current_endpoint
        counters["provider_timeouts"] = int(counters.get("provider_timeouts", 0)) + 1
        counters.setdefault("_provider_timeout_evidence", []).append(evidence)
        self._provider_timeout_evidence.append(dict(evidence))
        try:
            self.store.save_collection_error(
                None,
                observed_at,
                "provider_timeout",
                (
                    f"{reason} endpoint={endpoint} timeout_seconds={timeout:g}; "
                    "resolver=retry_provider_call; "
                    "next_action=retry_next_collection_tick"
                ),
            )
        except Exception:
            pass
        return error
    def _call_scope_direct(
        self,
        endpoint: str,
        operation_factory: Callable[[Any], Any],
        observed_at: datetime,
        counters: dict[str, Any],
    ) -> Any:
        """Run one exact operation on a fresh clone for every attempt."""
        last_error: Exception | None = None
        timeout = float(self.config.provider_timeout_seconds)
        for attempt in range(self.config.max_attempts):
            self._cycle_budget_available(endpoint, observed_at, counters)
            counters["requests"] += 1
            request_started = time.monotonic()
            provider = self._scope_direct_lookup_provider()
            owned = provider is not self.provider
            try:
                try:
                    future, key = self._submit_provider_call(
                        provider,
                        endpoint,
                        lambda: operation_factory(provider),
                        pool_name="scope_direct",
                    )
                except _ProviderDeadlineExceeded as exc:
                    if owned:
                        self._close_scope_direct_provider(provider)
                    deadline_error = self._record_provider_timeout(
                        exc.endpoint,
                        observed_at,
                        counters,
                        in_flight=exc.in_flight,
                        cycle_expired=exc.cycle_expired,
                    )
                    if exc.cycle_expired:
                        self._cycle_deadline_exhausted = True
                    counters["provider_failures"] += 1
                    raise deadline_error
                remaining = self._cycle_remaining_seconds()
                effective_timeout = (
                    self._provider_phase_timeout("scope_direct", remaining)
                    if remaining is not None
                    else timeout
                )
                wait_timeout = (
                    effective_timeout
                    if remaining is None
                    else min(effective_timeout, max(0.000001, remaining))
                )
                try:
                    result = future.result(timeout=wait_timeout)
                except FutureTimeout:
                    # ``FutureTimeout`` aliases ``TimeoutError`` on Python
                    # 3.11.  A completed worker exception is therefore a
                    # normal provider failure and must retain retry/backoff;
                    # only a successful late result is stale.
                    worker_exception: BaseException | None = None
                    if future.done():
                        try:
                            worker_exception = future.exception()
                        except BaseException as exc:
                            worker_exception = exc
                        if isinstance(worker_exception, Exception):
                            self._release_provider_call(key)
                            self._consume_transport_errors(provider)
                            self._drain_provider_advisories_now(provider)
                            if owned:
                                self._close_scope_direct_provider(provider)
                            raise worker_exception
                        self._release_provider_call(key)
                        self._consume_transport_errors(provider)
                        self._drain_provider_advisories_now(provider)
                        if owned:
                            self._close_scope_direct_provider(provider)
                    else:
                        future.cancel()
                        future.add_done_callback(
                            lambda _future, clone=provider, call_key=key: self._finish_scope_direct_orphan(
                                clone,
                                call_key,
                            )
                        )
                    cycle_expired = remaining is not None and remaining <= effective_timeout
                    deadline_error = self._record_provider_timeout(
                        endpoint,
                        observed_at,
                        counters,
                        cycle_expired=cycle_expired,
                    )
                    if cycle_expired:
                        self._cycle_deadline_exhausted = True
                    counters["provider_failures"] += 1
                    raise deadline_error
                except BaseException:
                    self._release_provider_call(key)
                    raise
                self._release_provider_call(key)
                transport_errors = self._consume_transport_errors(provider)
                self._drain_provider_advisories_now(provider)
                if owned:
                    self._close_scope_direct_provider(provider)
                retryable_error = next(
                    (
                        error
                        for error in reversed(transport_errors)
                        if getattr(error, "retryable", False)
                    ),
                    None,
                )
                if transport_errors:
                    counters["rate_limits"] += sum(
                        1 for error in transport_errors
                        if getattr(error, "status", None) == 429
                    )
                    if retryable_error is not None and attempt + 1 < self.config.max_attempts:
                        delay = self._backoff_delay(
                            endpoint,
                            attempt,
                            getattr(retryable_error, "retry_after", None),
                        )
                        if delay is not None:
                            remaining = self._cycle_remaining_seconds()
                            if remaining is not None and remaining <= delay:
                                raise self._cycle_budget_error(endpoint, observed_at, counters)
                            counters["retries"] += 1
                            self.sleep(delay)
                            continue
                    detail = "; ".join(str(error) for error in transport_errors)
                    raise RuntimeError(f"{endpoint} provider failure: {detail}")
                return result
            except _ProviderDeadlineExceeded:
                raise
            except Exception as exc:
                last_error = exc
                transport_errors = self._consume_transport_errors(provider)
                self._drain_provider_advisories_now(provider)
                if owned:
                    self._close_scope_direct_provider(provider)
                counters["rate_limits"] += sum(
                    1 for error in transport_errors
                    if getattr(error, "status", None) == 429
                )
                if isinstance(exc, HTTPFetchError) and exc.status == 429:
                    counters["rate_limits"] += 1
                retryable = isinstance(exc, (OSError, TimeoutError)) or (
                    isinstance(exc, HTTPFetchError) and exc.retryable
                )
                if retryable and attempt + 1 < self.config.max_attempts:
                    retry_after = exc.retry_after if isinstance(exc, HTTPFetchError) else (
                        getattr(transport_errors[-1], "retry_after", None)
                        if transport_errors else None
                    )
                    delay = self._backoff_delay(endpoint, attempt, retry_after)
                    if delay is not None:
                        remaining = self._cycle_remaining_seconds()
                        if remaining is not None and remaining <= delay:
                            raise self._cycle_budget_error(endpoint, observed_at, counters)
                        counters["retries"] += 1
                        self.sleep(delay)
                        continue
                counters["provider_failures"] += 1
                raise
            finally:
                elapsed = max(0.0, time.monotonic() - request_started)
                self._consume_provider_phase_budget("scope_direct", elapsed)
                counters.setdefault("_request_latencies", []).append(elapsed)
        raise RuntimeError(f"{endpoint} failed after retries: {last_error}")

    def _call_provider(
        self,
        endpoint: str,
        operation: Callable[[], Any],
        observed_at: datetime,
        counters: dict[str, Any],
        *,
        provider: Any | None = None,
        market_id: str | None = None,
        pool_name: str | None = None,
    ) -> Any:
        provider = provider or self.provider
        provider_pool = pool_name or ("scope" if self._scope_phase_active else "collection")
        scope_failfast = provider_pool == "scope" and endpoint.startswith(
            (
                "scope_refresh:",
                "scope_keyset:",
                "scope_markets:",
                "suitability_order_book:",
                "suitability_order_books:",
            )
        )
        ephemeral_scope_provider = (
            provider_pool == "scope_direct" and provider is not self.provider
        )
        last_error: Exception | None = None
        timeout = float(self.config.provider_timeout_seconds)
        for attempt in range(self.config.max_attempts):
            self._cycle_budget_available(endpoint, observed_at, counters)
            counters["requests"] += 1
            request_started = time.monotonic()
            try:
                try:
                    future, _key = self._submit_provider_call(
                        provider,
                        endpoint,
                        operation,
                        pool_name=provider_pool,
                    )
                except _ProviderDeadlineExceeded as exc:
                    if scope_failfast:
                        self._scope_broad_provider_unavailable = True
                    if ephemeral_scope_provider:
                        self._close_scope_direct_provider(provider)
                    deadline_error = self._record_provider_timeout(
                        exc.endpoint,
                        observed_at,
                        counters,
                        in_flight=exc.in_flight,
                        cycle_expired=exc.cycle_expired,
                    )
                    if exc.cycle_expired:
                        self._cycle_deadline_exhausted = True
                    counters["provider_failures"] += 1
                    raise deadline_error
                except BaseException:
                    if ephemeral_scope_provider:
                        self._close_scope_direct_provider(provider)
                    raise
                remaining = self._cycle_remaining_seconds()
                effective_timeout = timeout
                if (
                    remaining is not None
                    and provider_pool in {"scope", "scope_direct"}
                ):
                    effective_timeout = self._provider_phase_timeout(
                        provider_pool,
                        remaining,
                    )
                wait_timeout = (
                    effective_timeout
                    if remaining is None
                    else min(effective_timeout, max(0.000001, remaining))
                )
                try:
                    result = future.result(timeout=wait_timeout)
                except FutureTimeout:
                    # ``FutureTimeout`` aliases ``TimeoutError`` on Python
                    # 3.11.  Completed worker exceptions use normal retry;
                    # only successful late results are stale.
                    worker_exception: BaseException | None = None
                    if future.done():
                        try:
                            worker_exception = future.exception()
                        except BaseException as exc:
                            worker_exception = exc
                        if isinstance(worker_exception, Exception):
                            self._release_provider_call(_key)
                            self._consume_transport_errors(provider)
                            if provider_pool == "scope_direct":
                                self._drain_provider_advisories_now(provider)
                            if ephemeral_scope_provider:
                                self._close_scope_direct_provider(provider)
                            raise worker_exception
                        self._release_provider_call(_key)
                        self._consume_transport_errors(provider)
                        if provider_pool == "scope_direct":
                            self._drain_provider_advisories_now(provider)
                    else:
                        future.cancel()
                        if ephemeral_scope_provider:
                            future.add_done_callback(
                                lambda _future, clone=provider: self._close_scope_direct_provider(clone)
                            )
                    if scope_failfast:
                        self._scope_broad_provider_unavailable = True
                    cycle_expired = remaining is not None and remaining <= effective_timeout
                    deadline_error = self._record_provider_timeout(
                        endpoint,
                        observed_at,
                        counters,
                        cycle_expired=cycle_expired,
                    )
                    if cycle_expired:
                        self._cycle_deadline_exhausted = True
                    counters["provider_failures"] += 1
                    raise deadline_error
                except BaseException:
                    self._release_provider_call(_key)
                    raise
                self._release_provider_call(_key)
                transport_errors = self._consume_transport_errors(provider)
                if provider_pool == "scope_direct":
                    # Consume validation/advisory failures generated by this
                    # completed clone operation; the root provider is untouched.
                    self._drain_provider_advisories_now(provider)
                retryable_error = next(
                    (error for error in reversed(transport_errors) if getattr(error, "retryable", False)),
                    None,
                )
                if transport_errors:
                    counters["rate_limits"] += sum(
                        1 for error in transport_errors if getattr(error, "status", None) == 429
                    )
                    if retryable_error is not None and attempt + 1 < self.config.max_attempts:
                        delay = self._backoff_delay(endpoint, attempt, getattr(retryable_error, "retry_after", None))
                        if delay is not None:
                            remaining = self._cycle_remaining_seconds()
                            if remaining is not None and remaining <= delay:
                                raise self._cycle_budget_error(endpoint, observed_at, counters)
                            counters["retries"] += 1
                            self.sleep(delay)
                            continue
                    detail = "; ".join(str(error) for error in transport_errors)
                    raise RuntimeError(f"{endpoint} provider failure: {detail}")
                if ephemeral_scope_provider:
                    self._close_scope_direct_provider(provider)
                return result
            except _ProviderDeadlineExceeded:
                if scope_failfast:
                    self._scope_broad_provider_unavailable = True
                raise
            except Exception as exc:
                last_error = exc
                transport_errors = self._consume_transport_errors(provider)
                if provider_pool == "scope_direct":
                    self._drain_provider_advisories_now(provider)
                if ephemeral_scope_provider:
                    self._close_scope_direct_provider(provider)
                counters["rate_limits"] += sum(
                    1 for error in transport_errors if getattr(error, "status", None) == 429
                )
                if isinstance(exc, HTTPFetchError) and exc.status == 429:
                    counters["rate_limits"] += 1
                retryable = isinstance(exc, (OSError, TimeoutError)) or (
                    isinstance(exc, HTTPFetchError) and exc.retryable
                )
                if retryable and attempt + 1 < self.config.max_attempts:
                    retry_after = exc.retry_after if isinstance(exc, HTTPFetchError) else (
                        getattr(transport_errors[-1], "retry_after", None) if transport_errors else None
                    )
                    delay = self._backoff_delay(endpoint, attempt, retry_after)
                    if delay is not None:
                        remaining = self._cycle_remaining_seconds()
                        if remaining is not None and remaining <= delay:
                            raise self._cycle_budget_error(endpoint, observed_at, counters)
                        counters["retries"] += 1
                        self.sleep(delay)
                        continue
                counters["provider_failures"] += 1
                raise
            finally:
                elapsed = max(0.0, time.monotonic() - request_started)
                self._consume_provider_phase_budget(provider_pool, elapsed)
                counters.setdefault("_request_latencies", []).append(elapsed)
        raise RuntimeError(f"{endpoint} failed after retries: {last_error}")

    def _consume_transport_errors(self, provider: Any | None = None) -> tuple[Any, ...]:
        provider = provider or self.provider
        consumer = getattr(provider, "consume_transport_errors", None)
        if not callable(consumer):
            return ()
        return tuple(consumer())

    def _backoff_delay(
        self,
        endpoint: str,
        attempt: int,
        retry_after: float | None,
    ) -> float | None:
        base = min(
            self.config.backoff_max_seconds,
            self.config.backoff_initial_seconds * (self.config.backoff_multiplier**attempt),
        )
        if retry_after is not None:
            retry_after = float(retry_after)
            if not math.isfinite(retry_after) or retry_after < 0:
                retry_after = None
            elif retry_after > self.config.backoff_max_seconds:
                # A retry delay larger than the bounded retry budget must not
                # be clipped and retried early; fail into market cooldown.
                return None
            else:
                base = max(base, retry_after)
        if self.config.jitter_seconds <= 0:
            return base
        digest = hashlib.sha256(f"{self.config.collector_name}|{endpoint}|{attempt}".encode("utf-8")).digest()
        fraction = int.from_bytes(digest[:8], "big") / float(2**64)
        return min(self.config.backoff_max_seconds, base + fraction * self.config.jitter_seconds)

    def _cooldown_until(self, observed_at: datetime) -> datetime:
        return observed_at + timedelta(seconds=self.config.failure_cooldown_seconds)

    def _poll_due(
        self,
        market_id: str,
        snapshot: PredictionMarketSnapshot | None,
        state: Mapping[str, Any],
        observed_at: datetime,
    ) -> bool:
        last = _parse_iso(state.get("last_observed_at"))
        interval = self.config.interval_seconds
        if self.config.poll_plan is not None:
            try:
                planned = self.config.poll_plan(market_id, snapshot, state, observed_at)
            except TypeError:
                planned = self.config.poll_plan(market_id, snapshot, state)
            if isinstance(planned, Mapping):
                planned = planned.get("interval_seconds", planned.get("next_due"))
            if isinstance(planned, datetime):
                return observed_at >= ensure_utc(planned)
            if planned is not None:
                interval = float(planned)
                if not math.isfinite(interval) or interval <= 0:
                    raise ValueError("poll_plan interval must be finite and positive")
        return last is None or (observed_at - last).total_seconds() >= interval

    def _save_market_state(
        self,
        state_key: str,
        state: Mapping[str, Any],
        market_id: str,
        observed_at: datetime,
        counters: Mapping[str, int],
        cycle_errors_before: int,
        *,
        source_timestamp: datetime | None = None,
        latest_trade: datetime | None = None,
        trade_cursor: str | None | object = _UNSET,
        cooldown: bool = False,
    ) -> None:
        payload = {
            **dict(state),
            "market_id": market_id,
            "source_type": "FORWARD_COLLECTED",
            "last_attempt_at": observed_at.isoformat(),
            "last_observed_at": observed_at.isoformat(),
            "last_source_timestamp": source_timestamp.isoformat() if source_timestamp else state.get("last_source_timestamp"),
            "last_trade_timestamp": latest_trade.isoformat() if latest_trade else state.get("last_trade_timestamp"),
            "last_trade_cursor": state.get("last_trade_cursor") if trade_cursor is _UNSET else trade_cursor,
            "polls": int(state.get("polls", 0)) + (0 if cooldown else 1),
            "errors": int(state.get("errors", 0)) + int(counters["errors"]) - cycle_errors_before,
            "cooldown_until": self._cooldown_until(observed_at).isoformat() if cooldown else None,
            "stale_after_seconds": self.config.stale_after_seconds,
        }
        self.store.set_collector_state(state_key, payload)


def _book_depth(book: OrderBookSnapshot | None) -> dict[str, float | int]:
    if book is None:
        return {"bid_levels": 0, "ask_levels": 0, "bid_quantity": 0.0, "ask_quantity": 0.0}
    return {
        "bid_levels": len(book.bids),
        "ask_levels": len(book.asks),
        "bid_quantity": sum(level.size for level in book.bids),
        "ask_quantity": sum(level.size for level in book.asks),
    }


def _canonical_snapshot(
    snapshot: PredictionMarketSnapshot,
    yes_book: OrderBookSnapshot | None,
    no_book: OrderBookSnapshot | None,
) -> tuple[PredictionMarketSnapshot, datetime]:
    timestamps = [snapshot.timestamp]
    if yes_book is not None:
        timestamps.append(yes_book.timestamp)
    if no_book is not None:
        timestamps.append(no_book.timestamp)
    source_timestamp = max(timestamps)
    return (
        replace(
            snapshot,
            timestamp=source_timestamp,
            yes_bid=yes_book.best_bid if yes_book is not None and yes_book.best_bid is not None else snapshot.yes_bid,
            yes_ask=yes_book.best_ask if yes_book is not None and yes_book.best_ask is not None else snapshot.yes_ask,
            yes_mid=yes_book.midpoint if yes_book is not None and yes_book.midpoint is not None else snapshot.yes_mid,
            no_bid=no_book.best_bid if no_book is not None and no_book.best_bid is not None else snapshot.no_bid,
            no_ask=no_book.best_ask if no_book is not None and no_book.best_ask is not None else snapshot.no_ask,
            no_mid=no_book.midpoint if no_book is not None and no_book.midpoint is not None else snapshot.no_mid,
            order_book=yes_book,
        ),
        source_timestamp,
    )


def _stable_payload(payload: Any) -> str:
    return _jsonable(payload)


def _jsonable(value: Any) -> str:
    import json

    def convert(item: Any) -> Any:
        if isinstance(item, Mapping):
            return {str(key): convert(child) for key, child in item.items()}
        if isinstance(item, (list, tuple)):
            return [convert(child) for child in item]
        if isinstance(item, datetime):
            return ensure_utc(item).isoformat()
        return item

    return json.dumps(convert(value), sort_keys=True, separators=(",", ":"), allow_nan=False)


def _parse_iso(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value).replace("Z", "+00:00")
    try:
        return ensure_utc(datetime.fromisoformat(text))
    except ValueError:
        return None


__all__ = ["CollectionCycle", "CollectorConfig", "PolymarketCollector"]
