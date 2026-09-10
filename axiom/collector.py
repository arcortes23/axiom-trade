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
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import inspect
import math
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
    to_record,
    utc_now,
)
from .storage import AxiomStore
from .forward import ForwardTestRegistry
_UNSET = object()
_MAX_SCOPE_REQUEST_PATH_LENGTH = 512
_MAX_SCOPE_QUERY_DEPTH = 8
_MAX_SCOPE_QUERY_ITEMS = 128
_MAX_SCOPE_QUERY_STRING_LENGTH = 1024


class _ScopePersistenceValueError(ValueError):
    """A provider pagination value cannot be persisted as plain JSON."""


_MAX_SCOPE_CURSOR_HISTORY = 256


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
    max_trade_pages: int = 100
    max_provider_clock_skew_seconds: float = 5.0
    retain_cycles: int = 1
    discovery_budget_per_cycle: int = 20
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
        if isinstance(self.max_concurrency, bool) or not isinstance(self.max_concurrency, int) or self.max_concurrency not in {1, 2}:
            raise ValueError("max_concurrency must be one or two")
        if not str(self.collector_name).strip():
            raise ValueError("collector_name is required")
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
    cooldowns: int = 0
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
    tier_attempts: Mapping[str, int] | None = None
    tier_successes: Mapping[str, int] | None = None
    tier_failures: Mapping[str, int] | None = None
    request_latency_summary: Mapping[str, Any] | None = None
    capacity_reason: str | None = None
    discovery_coverage_status: str | None = None
    discovery_cursor: str | None = None

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
            "tier_failures": dict(self.tier_failures or {}),
            "request_latency_summary": dict(self.request_latency_summary or {}),
            "capacity_reason": self.capacity_reason,
            "discovery_coverage_status": self.discovery_coverage_status,
            "discovery_cursor": self.discovery_cursor,
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
        self.store = store
        self.config = config or CollectorConfig()
        self.clock = clock
        self.sleep = sleep
        self._discovery_continuation: Mapping[str, Any] | None = None
        # The nested continuation is assembled during scope discovery and
        # persisted with the root collector state at the end of the cycle.
        self._scope_inventory_continuation: Mapping[str, Any] | None = None

    def collect_once(
        self,
        market_ids: Sequence[str] | None = None,
        *,
        now: datetime | None = None,
    ) -> CollectionCycle:
        started = ensure_utc(now or self.clock())
        monotonic_started = time.monotonic()
        root_state = self.store.get_collector_state(self.config.collector_name) or {}
        self._scope_inventory_continuation = (
            root_state.get("scope_inventory_continuation")
            if isinstance(root_state.get("scope_inventory_continuation"), Mapping)
            else None
        )
        self._discovery_continuation = (
            root_state.get("discovery_continuation")
            if isinstance(root_state.get("discovery_continuation"), Mapping)
            else None
        )
        requested = tuple(dict.fromkeys(str(item).strip() for item in (market_ids or ()) if str(item).strip()))
        configured = requested or self.config.market_ids
        configured_values = tuple(configured)
        primary_candidate_ids = self._active_primary_candidate_ids() or []
        paper_ids = self._active_paper_forward_ids()
        observation_intent_ids = self._active_observation_intent_ids()
        primary_candidate_set = set(primary_candidate_ids)
        paper_set = set(paper_ids)
        observation_set = set(observation_intent_ids)
        counters = self._new_counters()
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
        self._materialize_observation_intents(
            started,
            observation_intent_ids,
            scope_candidate_markets,
            counters,
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
            and (identifier in paper_set or identifier in observation_set)
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
        if configured_values:
            candidate_bound = list(dict.fromkeys([*configured_values, *candidate_bound]))
            for identifier in configured_values:
                candidate_references.setdefault(identifier, [])

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
        # A scope-bearing candidate has already consumed the one shared public
        # inventory pass above.  Never append unqualified inventory to its
        # schedule; this is what prevents research-only/invalid scopes from
        # widening into live collection authority.
        if (
            not configured
            and not scope_candidate_set
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
                self.store.save_collection_error(None, started, "discovery", str(exc))
        if not configured and not scope_candidate_set and remaining > len(discovery_scheduled):
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
        planned_ids = list(dict.fromkeys([*candidate_scheduled, *paper_scheduled, *discovery_scheduled]))
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
        if workers and len(planned_ids) > 1:
            # Keep each isolated provider assigned to only one future at a
            # time.  Completed providers are recycled for the next task,
            # rather than assigned by modulo to overlapping futures.
            worker_count = min(self.config.max_concurrency, len(workers), len(planned_ids))
            with ThreadPoolExecutor(max_workers=worker_count) as executor:
                future_tasks: dict[Any, tuple[str, Any]] = {}
                planned = iter(planned_ids)
                for worker_provider in workers[:worker_count]:
                    try:
                        identifier = next(planned)
                    except StopIteration:
                        break
                    future = executor.submit(run_one, identifier, worker_provider)
                    future_tasks[future] = (identifier, worker_provider)
                while future_tasks:
                    for future in as_completed(tuple(future_tasks)):
                        identifier, worker_provider = future_tasks.pop(future)
                        try:
                            results.append(future.result())
                        except Exception as exc:
                            results.append(failed_task(identifier, exc))
                        try:
                            next_identifier = next(planned)
                        except StopIteration:
                            continue
                        next_future = executor.submit(run_one, next_identifier, worker_provider)
                        future_tasks[next_future] = (next_identifier, worker_provider)
        else:
            for identifier in planned_ids:
                try:
                    results.append(run_one(identifier))
                except Exception as exc:
                    results.append(failed_task(identifier, exc))

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
            capacity_reason=capacity_reason,
            discovery_coverage_status=discovery_coverage_status,
            discovery_cursor=(
                str(discovery_cursor)
                if isinstance(discovery_cursor, str) and discovery_cursor.strip()
                else None
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
                end_timestamp=max(source_times) if source_times else None,
                row_count=len(forward_snapshots),
                completeness=1.0 if counters["errors"] == 0 else max(0.0, 1.0 - counters["errors"] / max(1, len(planned_ids))),
                missing_ranges=(),
                quality=forward_quality,
                source_type="FORWARD_COLLECTED",
                snapshot_id=cycle_id,
                metadata={
                    "collector": self.config.collector_name,
                    "cycle_id": cycle_id,
                    "markets_seen": len(planned_ids),
                    "collection_cycle": cycle_payload,
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
                "scope_discovery_carry_cursor": scope_cursor,
                "scope_inventory_continuation": self._scope_inventory_continuation,
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
                "tier_attempts": tier_attempts,
                "tier_successes": tier_successes,
                "tier_failures": tier_failures,
                "request_latency_summary": cycle.request_latency_summary,
                "capacity_reason": capacity_reason,
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
            "cooldowns": 0,
            "skipped_markets": 0,
            "metadata_failures": 0,
            "order_book_failures": 0,
            "trade_failures": 0,
            "_request_latencies": [],
        }

    @staticmethod
    def _merge_counters(target: dict[str, Any], source: Mapping[str, Any]) -> None:
        for key in (
            "markets_attempted", "markets_successful", "markets_failed",
            "metadata_inserted", "snapshots_inserted", "snapshot_duplicates",
            "trades_inserted", "trade_duplicates", "errors", "requests",
            "rate_limits", "retries", "provider_failures", "cooldowns",
            "skipped_markets", "metadata_failures", "order_book_failures",
            "trade_failures",
        ):
            target[key] = int(target.get(key, 0)) + int(source.get(key, 0))
        target.setdefault("_request_latencies", []).extend(
            float(value) for value in source.get("_request_latencies", ())
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

    def _active_primary_candidate_ids(self) -> list[str] | None:
        """Return the currently selected/eligible ranking universe.

        PAPER_FORWARD is not a disqualifier here: a paper candidate can be
        ranked and therefore remain in tier one.  Unranked paper candidates
        are discovered separately as tier two.
        """
        lock = getattr(self.store, "_lock", None)
        with lock if lock is not None else nullcontext():
            try:
                connection = self.store.connection
                rows = connection.execute(
                    "SELECT candidate_id FROM canary_rankings "
                    "WHERE selected=1 ORDER BY rank,candidate_id LIMIT 1000"
                ).fetchall()
                ranked = [str(row[0]).strip() for row in rows if str(row[0]).strip()]
                if ranked:
                    return list(dict.fromkeys(ranked))
            except Exception:
                pass
            try:
                connection = self.store.connection
                rows = connection.execute(
                    "SELECT candidate_id FROM canary_eligibility ORDER BY eligible_at,candidate_id LIMIT 1000"
                ).fetchall()
                eligible = [str(row[0]).strip() for row in rows if str(row[0]).strip()]
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

    def _active_paper_forward_ids(self) -> list[str]:
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
        ))

    def _active_observation_intent_ids(self) -> list[str]:
        intents = ForwardTestRegistry(self.store).list_observation_intents()
        result: list[str] = []
        for spec in intents:
            config = spec.config if isinstance(spec.config, Mapping) else {}
            candidate_id = str(config.get("candidate_id", "")).strip()
            if candidate_id:
                result.append(candidate_id)
        return list(dict.fromkeys(result))

    def _materialize_observation_intents(
        self,
        observed_at: datetime,
        candidate_ids: Sequence[str],
        candidate_markets: Mapping[str, Sequence[str]],
        counters: dict[str, Any],
    ) -> None:
        if not candidate_ids:
            return
        registry = ForwardTestRegistry(self.store)
        by_candidate = {
            str(spec.config.get("candidate_id", "")).strip(): spec
            for spec in registry.list_observation_intents()
            if isinstance(spec.config, Mapping)
            and str(spec.config.get("candidate_id", "")).strip()
        }
        for candidate_id in candidate_ids:
            markets = tuple(
                str(item).strip()
                for item in candidate_markets.get(candidate_id, ())
                if str(item).strip()
            )
            if not markets:
                continue
            intent = by_candidate.get(str(candidate_id).strip())
            if intent is None:
                continue
            # Later ticks refresh current scope authority, not the immutable
            # experiment or its original registration timestamp.
            if registry.get("forward-" + candidate_id) is not None:
                continue
            try:
                registry.materialize_observation_intent(
                    intent,
                    allowed_markets=markets[:100],
                    registration_timestamp=observed_at,
                    now=observed_at,
                    candidate_id=candidate_id,
                )
            except (TypeError, ValueError):
                counters["errors"] += 1

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
        try:
            from .market_scope import resolve_market_scope
        except (ImportError, AttributeError):
            resolve_market_scope = None
        saver = getattr(self.store, "save_market_scope_resolution", None)
        loader = getattr(self.store, "load_candidate_lifecycle", None)
        if not callable(resolve_market_scope) or not callable(loader):
            return [], {}, {}, self._scope_cursor(root_state)

        documents: list[tuple[str, Mapping[str, Any]]] = []
        for candidate_id in dict.fromkeys(str(item).strip() for item in candidate_ids if str(item).strip()):
            try:
                record = loader(candidate_id)
            except (AttributeError, KeyError, TypeError, ValueError):
                continue
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
                documents.append((candidate_id, self._scope_document(payload)))
        if not documents:
            return [], {}, {}, self._scope_cursor(root_state)
        carry_cursor = self._scope_cursor(root_state)
        needs_inventory = any(
            self._scope_document_needs_inventory(document)
            for _, document in documents
        )
        scope_candidates = [candidate_id for candidate_id, _ in documents]
        if needs_inventory:
            current_records, snapshots, next_cursor = self._discover_scope_inventory(
                observed_at,
                counters,
                carry_cursor=carry_cursor,
                documents=tuple(document for _, document in documents),
            )
            if (
                isinstance(self._scope_inventory_continuation, Mapping)
                and self._scope_inventory_continuation.get("query_reset")
            ):
                # A reset/rebase response is intentionally not a resolver
                # input.  Keep the candidate scoped so legacy authority cannot
                # leak into scheduling while the next cycle rebases.
                return scope_candidates, {}, {}, next_cursor
        else:
            current_records, snapshots, next_cursor = [], {}, carry_cursor
        # Resolver limits are independently bounded from the scheduler's
        # global market cap.  A candidate may have up to the canonical 100
        # matches, while this cycle still schedules at most max_markets.
        max_scope_markets = min(1000, max(0, len(current_records)))
        candidate_markets: dict[str, list[str]] = {}
        for candidate_id, document in documents:
            try:
                result = resolve_market_scope(
                    candidate_id,
                    document,
                    current_records,
                    resolved_at=observed_at,
                    max_matches=100,
                    max_markets=max_scope_markets,
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
            matched = self._scope_result_market_ids(result)
            candidate_markets[candidate_id] = matched
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
                    resolved = resolver(category_values[0])
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
                except Exception:
                    # Taxonomy lookup is advisory.  Falling back to the
                    # unfiltered page is safer than inventing an identifier.
                    lookup_failed = True
                if lookup_failed:
                    # Optional lookup adapters may retain transport or
                    # validation failures for the next provider operation.
                    # Drain only after this failed advisory call so a
                    # successful broader discovery page is not misclassified.
                    self._consume_advisory_provider_errors(provider)


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

    @staticmethod
    def _consume_advisory_provider_errors(provider: Any) -> None:
        for name in ("consume_transport_errors", "consume_validation_errors"):
            consumer = getattr(provider, name, None)
            if not callable(consumer):
                continue
            try:
                consumer()
            except Exception:
                # A best-effort advisory drain must never become a discovery
                # failure or mask the broader page request.
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
        method_page = getattr(provider, "market_page", None)
        if callable(method_page) and self.config.discovery_budget_per_cycle > 0:
            limit = min(100, max(1, int(self.config.discovery_budget_per_cycle)))
            previous = (
                dict(self._scope_inventory_continuation)
                if isinstance(self._scope_inventory_continuation, Mapping)
                else {}
            )
            query = self._scope_inventory_query(
                documents,
                observed_at,
                provider,
                limit=limit,
                after_cursor=None,
            )
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
                    "cumulative_duplicate_count": 0,
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


            try:
                page = self._call_provider(
                    "scope_discovery",
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
                cumulative = {
                    "raw_count": self._scope_count(base_state.get("cumulative_raw_count")),
                    "unique_count": self._scope_count(base_state.get("cumulative_unique_count")),
                    "duplicate_count": self._scope_count(base_state.get("cumulative_duplicate_count")),
                    "malformed_count": self._scope_count(base_state.get("cumulative_malformed_count")),
                }
                continuation = {
                    **base_state,
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
            seen_ids = {
                str(item).strip()
                for item in base_state.get("seen_market_ids", ())
                if str(item).strip()
            }
            new_snapshots: list[PredictionMarketSnapshot] = []
            cross_page_duplicates = 0
            for item in valid:
                market_id = str(item.market_id).strip()
                if market_id in seen_ids:
                    cross_page_duplicates += 1
                    continue
                seen_ids.add(market_id)
                new_snapshots.append(item)
            duplicate_count += cross_page_duplicates
            unique_count = len(new_snapshots)
            if supplied_unique is not _UNSET and not seen_ids:
                unique_count = self._scope_count(supplied_unique, unique_count)
            if next_cursor is None:
                coverage_status = "PARTIAL" if explicit_status == "PARTIAL" or malformed_count else "COMPLETE"
            elif explicit_status in {"PARTIAL", "BUDGET_EXHAUSTED"}:
                coverage_status = explicit_status
            else:
                coverage_status = "BUDGET_EXHAUSTED"
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
                "seen_market_ids": sorted(seen_ids)[:10000],
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
            if page_error_reason is not None:
                continuation["error_reason"] = page_error_reason
            self._scope_inventory_continuation = continuation
            record_by_id = {
                str(item.market_id).strip(): item
                for item in new_snapshots
                if str(item.market_id).strip()
            }
            records = [
                self._scope_market_record(item, observed_at, provider)
                for item in new_snapshots
            ]
            return records, record_by_id, next_cursor

        if callable(method_page) and self.config.discovery_budget_per_cycle <= 0:
            previous = (
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
                "scope_discovery",
                lambda: method(**kwargs),
                observed_at,
                counters,
                provider=provider,
            ) or ()
        except Exception as exc:
            counters["errors"] += 1
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
            return [], {}, carry_cursor
        try:
            offset = int(carry_cursor) % len(snapshots)
        except (TypeError, ValueError):
            offset = 0
        rotated = snapshots[offset:] + snapshots[:offset]
        page = rotated[:scan_budget]
        records = [self._scope_market_record(item, observed_at, provider) for item in page]
        record_by_id = {
            str(item.market_id).strip(): item
            for item in page
            if str(item.market_id).strip()
        }
        next_cursor = (offset + len(page)) % len(snapshots)
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
            cursor = carry_cursor if isinstance(carry_cursor, str) and carry_cursor.strip() else None
            limit = min(100, max(1, budget))
            kwargs: dict[str, Any] = {
                "limit": limit,
                "after_cursor": cursor,
                "closed": not self.config.active,
                "include_tag": True,
            }
            try:
                parameters = inspect.signature(method_page).parameters
            except (TypeError, ValueError):
                parameters = {}
            if parameters and not any(
                parameter.kind is inspect.Parameter.VAR_KEYWORD
                for parameter in parameters.values()
            ):
                kwargs = {key: value for key, value in kwargs.items() if key in parameters}
            try:
                page = self._call_provider(
                    "discovery",
                    lambda: method_page(**kwargs),
                    observed_at,
                    counters,
                    provider=provider,
                )
            except Exception as exc:
                counters["errors"] += 1
                self._discovery_continuation = {
                    **dict(self._discovery_continuation or {}),
                    "coverage_status": "ERROR",
                    "after_cursor": cursor,
                    "error_reason": str(exc),
                    "updated_at": observed_at.isoformat(),
                }
                self.store.save_collection_error(None, observed_at, "discovery", str(exc))
                return (), cursor, ()
            page_invalid = (
                page is None
                or (
                    not isinstance(page, (Mapping, list, tuple))
                    and not hasattr(page, "snapshots")
                )
            )
            if page_invalid:
                counters["errors"] += 1
                self._discovery_continuation = {
                    **dict(self._discovery_continuation or {}),
                    "coverage_status": "ERROR",
                    "after_cursor": cursor,
                    "error_reason": "invalid discovery page",
                    "updated_at": observed_at.isoformat(),
                }
                self.store.save_collection_error(None, observed_at, "discovery", "invalid discovery page")
                return (), cursor, ()
            status = str(self._scope_page_value(page, "coverage_status", "")).strip().upper()
            next_cursor = self._scope_page_value(page, "next_cursor", None)
            if next_cursor is not None and (
                not isinstance(next_cursor, str) or not next_cursor.strip() or next_cursor == cursor
            ):
                counters["errors"] += 1
                status = "ERROR"
                next_cursor = cursor
                self.store.save_collection_error(None, observed_at, "discovery", "invalid or repeated keyset cursor")
            raw_items = self._scope_page_value(page, "snapshots", _UNSET)
            if raw_items is _UNSET and isinstance(page, Mapping):
                raw_items = page.get("markets", page.get("data", ()))
            try:
                snapshots = [
                    item for item in list(raw_items or ())
                    if isinstance(item, PredictionMarketSnapshot)
                ]
            except TypeError:
                snapshots = []
            malformed_count = self._scope_count(
                self._scope_page_value(page, "malformed_count", 0)
            )
            if malformed_count:
                counters["errors"] += malformed_count
                self.store.save_collection_error(
                    None,
                    observed_at,
                    "discovery_malformed_rows",
                    f"{malformed_count} malformed public market rows",
                )
            if status == "ERROR":
                counters["errors"] += 1
                self._discovery_continuation = {
                    **dict(self._discovery_continuation or {}),
                    "coverage_status": status,
                    "after_cursor": next_cursor,
                    "updated_at": observed_at.isoformat(),
                }
                return (), next_cursor, ()
            selected = [item for item in snapshots if item.market_id not in excluded][:budget]
            selected_ids = {item.market_id for item in selected}
            deferred = [
                item.market_id
                for item in snapshots
                if item.market_id not in excluded and item.market_id not in selected_ids
            ]
            if not selected and not deferred and next_cursor is None:
                status = "NO_MATCHING_MARKETS"
            elif next_cursor is not None:
                status = status if status in {"PARTIAL", "BUDGET_EXHAUSTED"} else "PARTIAL"
            else:
                status = status if status in {"PARTIAL", "COMPLETE"} else "COMPLETE"
            self._discovery_continuation = {
                "request_path": str(self._scope_page_value(page, "request_path", "/markets/keyset")),
                "request_query": dict(kwargs),
                "query_fingerprint": self._scope_page_value(page, "query_fingerprint", None),
                "after_cursor": next_cursor,
                "coverage_status": status,
                "raw_count": self._scope_count(self._scope_page_value(page, "raw_count", len(snapshots))),
                "unique_count": self._scope_count(self._scope_page_value(page, "unique_count", len(snapshots))),
                "duplicate_count": self._scope_count(self._scope_page_value(page, "duplicate_count", 0)),
                "malformed_count": self._scope_count(self._scope_page_value(page, "malformed_count", 0)),
                "requested_at": observed_at.isoformat(),
                "updated_at": observed_at.isoformat(),
            }
            return tuple(selected), next_cursor, tuple(deferred)

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
        selected = [item for item in rotated if item.market_id not in excluded][:budget]
        selected_ids = {item.market_id for item in selected}
        deferred = [
            item.market_id
            for item in rotated
            if item.market_id not in excluded and item.market_id not in selected_ids
        ]
        scanned = len(selected) + sum(1 for item in rotated if item.market_id in excluded)
        next_cursor = (offset + scanned) % len(snapshots)
        self._discovery_continuation = {
            "coverage_status": "PARTIAL",
            "after_cursor": next_cursor,
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
            provider, market_id, "market", snapshot
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
        yes_provider_timestamp = self._provider_timestamp(provider, market_id, "yes_order_book", yes_book)
        no_provider_timestamp = self._provider_timestamp(provider, market_id, "no_order_book", no_book)
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
                    candidate_provenance = provenance_getter(trade)
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
        skew_seconds: float = 5.0,
    ) -> bool:
        if timestamp is None:
            return True
        # Provider timestamps may legitimately predate a request (a quote can
        # be old), but may not be from the future beyond configured skew.
        return ensure_utc(timestamp) <= ensure_utc(response_received_at) + timedelta(seconds=skew_seconds)

    @staticmethod
    def _provider_timestamp(
        provider: Any,
        market_id: str,
        kind: str,
        value: Any,
    ) -> datetime | None:
        getter = getattr(provider, "provider_timestamp_for", None)
        if callable(getter):
            try:
                stamp = getter(market_id, kind=kind)
            except (TypeError, ValueError):
                try:
                    stamp = getter(market_id, kind)
                except (TypeError, ValueError):
                    stamp = None
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

    def _call_provider(
        self,
        endpoint: str,
        operation: Callable[[], Any],
        observed_at: datetime,
        counters: dict[str, Any],
        *,
        provider: Any | None = None,
        market_id: str | None = None,
    ) -> Any:
        provider = provider or self.provider
        last_error: Exception | None = None
        for attempt in range(self.config.max_attempts):
            counters["requests"] += 1
            request_started = time.monotonic()
            try:
                result = operation()
                transport_errors = self._consume_transport_errors(provider)
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
                            counters["retries"] += 1
                            self.sleep(delay)
                            continue
                    detail = "; ".join(str(error) for error in transport_errors)
                    raise RuntimeError(f"{endpoint} provider failure: {detail}")
                return result
            except Exception as exc:
                last_error = exc
                transport_errors = self._consume_transport_errors(provider)
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
                        counters["retries"] += 1
                        self.sleep(delay)
                        continue
                counters["provider_failures"] += 1
                raise
            finally:
                counters.setdefault("_request_latencies", []).append(
                    max(0.0, time.monotonic() - request_started)
                )
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
