"""Continuous, resumable, read-only Polymarket collection.

The collector stores every source payload with an observation timestamp and a
content-derived identity.  Repeating a cycle is therefore safe: identical
metadata, snapshots, and trades are ignored, while changed metadata remains an
immutable record.  Network failures become collection-error records and never
become synthetic prices or settlements.
"""
from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timedelta
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
_UNSET = object()


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
    poll_plan: Callable[..., Any] | None = None

    def __post_init__(self) -> None:
        interval = float(self.interval_seconds)
        if not math.isfinite(interval) or interval <= 0:
            raise ValueError("collector interval must be finite and positive")
        if not isinstance(self.depth, int) or isinstance(self.depth, bool) or self.depth <= 0:
            raise ValueError("collector depth must be a positive integer")
        if self.stale_after_seconds is not None:
            stale_after = float(self.stale_after_seconds)
            if not math.isfinite(stale_after) or stale_after <= 0:
                raise ValueError("stale_after_seconds must be finite and positive")
        if isinstance(self.max_markets, bool) or not isinstance(self.max_markets, int) or self.max_markets <= 0:
            raise ValueError("max_markets must be a positive integer")
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
    metadata_inserted: int
    snapshots_inserted: int
    snapshot_duplicates: int
    trades_inserted: int
    trade_duplicates: int
    errors: int
    requests: int = 0
    rate_limits: int = 0
    retries: int = 0
    provider_failures: int = 0
    cooldowns: int = 0
    skipped_markets: int = 0
    metadata_failures: int = 0
    order_book_failures: int = 0
    trade_failures: int = 0

    @property
    def duration_seconds(self) -> float:
        return max(0.0, (self.ended_at - self.started_at).total_seconds())

    def as_record(self) -> dict[str, Any]:
        return {
            "started_at": self.started_at.isoformat(),
            "ended_at": self.ended_at.isoformat(),
            "duration_seconds": self.duration_seconds,
            "markets_seen": self.markets_seen,
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

    def collect_once(
        self,
        market_ids: Sequence[str] | None = None,
        *,
        now: datetime | None = None,
    ) -> CollectionCycle:
        started = ensure_utc(now or self.clock())
        counters: dict[str, int] = {
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
        }
        discovered: dict[str, PredictionMarketSnapshot] = {}
        requested = tuple(dict.fromkeys(str(item).strip() for item in (market_ids or ()) if str(item).strip()))
        configured = requested or self.config.market_ids
        if configured:
            ids = list(configured)
        else:
            try:
                discovered_values = self._discover_markets(started, counters)
                for snapshot in discovered_values or ():
                    if isinstance(snapshot, PredictionMarketSnapshot):
                        discovered[snapshot.market_id] = snapshot
            except Exception as exc:
                counters["errors"] += 1
                self.store.save_collection_error(None, started, "discovery", str(exc))
            tracked = self.store.tracked_polymarket_markets(active_only=self.config.active, now=started, include_payload=True)
            tracked_ids = [str(item.get("market_id")) for item in tracked if isinstance(item, Mapping) and item.get("market_id")]
            ids = list(dict.fromkeys([*discovered.keys(), *tracked_ids]))
        if self.config.max_markets is not None:
            ids = ids[: self.config.max_markets]
        for market_id in ids:
            try:
                self._collect_market(
                    market_id,
                    discovered.get(market_id),
                    started,
                    counters,
                    point_in_time=now is not None,
                )
            except Exception as exc:
                counters["errors"] += 1
                self.store.save_collection_error(market_id, started, "collector", str(exc))
        ended = ensure_utc(now or self.clock())
        if ended < started:
            ended = started
        cycle = CollectionCycle(started, ended, len(ids), **counters)
        cycle_payload = cycle.as_record()
        cycle_id = "cycle-" + hashlib.sha256(
            f"{self.config.collector_name}|{started.isoformat()}|{ended.isoformat()}|{_stable_payload(cycle_payload)}".encode("utf-8")
        ).hexdigest()
        self.store.save_collection_cycle(cycle_id, self.config.collector_name, cycle_payload, started_at=started, ended_at=ended)
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
                if any(str(item.get("quality", "")) == ResearchQuality.ORDER_BOOK_SIMULATED.value for item in forward_snapshots if isinstance(item, Mapping))
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
                completeness=1.0 if counters["errors"] == 0 else max(0.0, 1.0 - counters["errors"] / max(1, len(ids))),
                missing_ranges=(),
                quality=forward_quality,
                source_type="FORWARD_COLLECTED",
                snapshot_id=cycle_id,
                metadata={
                    "collector": self.config.collector_name,
                    "cycle_id": cycle_id,
                    "markets_seen": len(ids),
                    "collection_cycle": cycle_payload,
                    "live_execution": False,
                },
            )
        except (AttributeError, TypeError, ValueError):
            # Catalog publication must not turn a successfully persisted
            # forward collection cycle into a failed collection.
            pass
        self.store.set_collector_state(
            self.config.collector_name,
            {
                "last_cycle_started_at": started.isoformat(),
                "last_cycle_ended_at": ended.isoformat(),
                "markets_seen": len(ids),
                "stale_after_seconds": self.config.stale_after_seconds,
                **counters,
            },
        )
        return cycle

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

    def _discover_markets(self, observed_at: datetime, counters: dict[str, int]) -> Sequence[PredictionMarketSnapshot]:
        method = self.provider.markets
        kwargs: dict[str, Any] = {"active": self.config.active}
        try:
            parameters = inspect.signature(method).parameters
        except (TypeError, ValueError):
            parameters = {}
        if self.config.max_markets is not None and (
            "limit" in parameters or any(parameter.kind is inspect.Parameter.VAR_KEYWORD for parameter in parameters.values())
        ):
            kwargs["limit"] = self.config.max_markets
        return self._call_provider("discovery", lambda: method(**kwargs), observed_at, counters) or ()

    def _collect_market(
        self,
        market_id: str,
        discovered: PredictionMarketSnapshot | None,
        observed_at: datetime,
        counters: dict[str, int],
        *,
        point_in_time: bool = False,
    ) -> None:
        state_key = f"{self.config.collector_name}:{market_id}"
        state = self.store.get_collector_state(state_key) or {}
        if not self._poll_due(market_id, discovered, state, observed_at):
            counters["skipped_markets"] += 1
            return
        cooldown_until = _parse_iso(state.get("cooldown_until"))
        if cooldown_until is not None and observed_at < cooldown_until:
            counters["cooldowns"] += 1
            counters["skipped_markets"] += 1
            return
        cycle_errors_before = counters["errors"]
        snapshot: PredictionMarketSnapshot | None = None
        market_error = False
        try:
            snapshot = self._call_provider(
                f"market:{market_id}",
                lambda: self.provider.market(market_id),
                observed_at,
                counters,
                market_id=market_id,
            )
        except Exception as exc:
            market_error = True
            counters["errors"] += 1
            self.store.save_collection_error(market_id, observed_at, "market", str(exc))
            snapshot = discovered
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
                    "errors": int(state.get("errors", 0)) + counters["errors"] - cycle_errors_before,
                },
            )
            return
        market_retrieved_at = observed_at if point_in_time else ensure_utc(self.clock())
        if ensure_utc(snapshot.timestamp) > market_retrieved_at:
            counters["errors"] += 1
            self.store.save_collection_error(
                market_id,
                observed_at,
                "future_observation",
                "market timestamp is after local retrieval",
            )
            self._save_market_state(state_key, state, market_id, observed_at, counters, cycle_errors_before, cooldown=True)
            return
        metadata_error = False
        try:
            metadata = self._call_provider(
                f"metadata:{market_id}",
                lambda: self.provider.metadata(market_id),
                observed_at,
                counters,
                market_id=market_id,
            )
        except Exception as exc:
            metadata_error = True
            metadata = None
            self.store.save_collection_error(market_id, observed_at, "metadata", str(exc))
        metadata_observed_at = observed_at if point_in_time else ensure_utc(self.clock())
        if metadata is None:
            counters["errors"] += 1
            counters["metadata_failures"] += 1
            if not metadata_error:
                self.store.save_collection_error(market_id, observed_at, "metadata_unavailable", "provider returned no metadata")
        terminal_states = {
            SettlementState.RESOLVED_YES,
            SettlementState.RESOLVED_NO,
            SettlementState.VOID,
        }
        closed = snapshot.settlement in terminal_states or (
            snapshot.expiry is not None and snapshot.expiry <= metadata_observed_at
        )
        metadata_payload: dict[str, Any] = {
            "source_type": "FORWARD_COLLECTED",
            "snapshot": to_record(snapshot),
            "metadata": to_record(metadata) if metadata is not None else None,
            "token_ids": {"yes": snapshot.yes_token_id, "no": snapshot.no_token_id},
            "source": getattr(self.provider, "provider_name", type(self.provider).__name__),
            "metadata_available": metadata is not None,
            "active": not closed,
            "closed": closed,
            "observed_at": metadata_observed_at.isoformat(),
        }
        metadata_identity = {
            key: value for key, value in metadata_payload.items() if key != "observed_at"
        }
        metadata_hash = hashlib.sha256(_stable_payload(metadata_identity).encode("utf-8")).hexdigest()
        if self.store.save_polymarket_market_metadata(
            market_id,
            metadata_payload,
            observed_at=metadata_observed_at,
            metadata_hash=metadata_hash,
            source_type="FORWARD_COLLECTED",
        ):
            counters["metadata_inserted"] += 1
        books_request_started_at = observed_at if point_in_time else ensure_utc(self.clock())
        try:
            books = dict(
                self._call_provider(
                    f"order_books:{market_id}",
                    lambda: self.provider.order_books(market_id, depth=self.config.depth),
                    observed_at,
                    counters,
                    market_id=market_id,
                )
                or {}
            )
        except Exception as exc:
            counters["errors"] += 1
            counters["order_book_failures"] += 1
            self.store.save_collection_error(market_id, observed_at, "order_book", str(exc))
            self._save_market_state(state_key, state, market_id, observed_at, counters, cycle_errors_before, cooldown=True)
            return
        books_retrieved_at = observed_at if point_in_time else ensure_utc(self.clock())
        collection_observed_at = max(observed_at, metadata_observed_at, books_retrieved_at)
        yes_book = books.get("yes")
        no_book = books.get("no")
        if not isinstance(yes_book, OrderBookSnapshot):
            yes_book = None
        if not isinstance(no_book, OrderBookSnapshot):
            no_book = None
        latest_allowed_source_time = books_retrieved_at + timedelta(seconds=self.config.max_provider_clock_skew_seconds)
        future_data = [
            label
            for label, item in (("market", snapshot), ("yes order book", yes_book), ("no order book", no_book))
            if item is not None and ensure_utc(item.timestamp) > latest_allowed_source_time
        ]
        if future_data:
            counters["errors"] += len(future_data)
            self.store.save_collection_error(
                market_id,
                collection_observed_at,
                "future_observation",
                ", ".join(future_data) + " timestamp is after collection observation",
            )
            self._save_market_state(state_key, state, market_id, collection_observed_at, counters, cycle_errors_before, cooldown=True)
            return
        canonical, source_timestamp = _canonical_snapshot(snapshot, yes_book, no_book)
        quality = ResearchQuality.ORDER_BOOK_SIMULATED if yes_book is not None or no_book is not None else ResearchQuality.PRICE_PROXY
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
            "observed_at": collection_observed_at.isoformat(),
            "source_timestamp": source_timestamp.isoformat(),
            "request_started_at": books_request_started_at.isoformat(),
            "response_received_at": books_retrieved_at.isoformat(),
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
        last_trade_cursor = str(state.get("last_trade_cursor", "")).strip() or None
        trade_fetch_failed = False
        try:
            trades = self._fetch_trades(market_id, last_trade, collection_observed_at, counters, cursor=last_trade_cursor)
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
            if self.store.save_polymarket_trade(market_id, trade):
                counters["trades_inserted"] += 1
            else:
                counters["trade_duplicates"] += 1
            latest_trade = max(latest_trade, trade.timestamp) if latest_trade else trade.timestamp
        trade_cursor: str | None = None
        if trade_fetch_failed:
            latest_trade = last_trade
            trade_cursor = last_trade_cursor
        elif not getattr(self.provider, "last_trades_complete", True):
            latest_trade = last_trade
            provider_cursor = str(getattr(self.provider, "last_trade_cursor", "")).strip() or None
            trade_cursor = provider_cursor or last_trade_cursor
        self._save_market_state(
            state_key,
            state,
            market_id,
            collection_observed_at,
            counters,
            cycle_errors_before,
            source_timestamp=source_timestamp,
            latest_trade=latest_trade,
            trade_cursor=trade_cursor,
        )

    def _fetch_trades(
        self,
        market_id: str,
        last_trade: datetime | None,
        observed_at: datetime,
        counters: dict[str, int],
        *,
        cursor: str | None = None,
    ) -> Sequence[TradePrint]:
        def call() -> Sequence[TradePrint]:
            method = self.provider.trades
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
            market_id=market_id,
        ) or ()

    def _call_provider(
        self,
        endpoint: str,
        operation: Callable[[], Any],
        observed_at: datetime,
        counters: dict[str, int],
        *,
        market_id: str | None = None,
    ) -> Any:
        last_error: Exception | None = None
        for attempt in range(self.config.max_attempts):
            counters["requests"] += 1
            try:
                result = operation()
                transport_errors = self._consume_transport_errors()
                retryable_error = next((error for error in reversed(transport_errors) if error.retryable), None)
                if transport_errors:
                    for error in transport_errors:
                        if error.status == 429:
                            counters["rate_limits"] += 1
                    if retryable_error is not None and attempt + 1 < self.config.max_attempts:
                        counters["retries"] += 1
                        self.sleep(self._backoff_delay(endpoint, attempt, retryable_error.retry_after))
                        continue
                    detail = "; ".join(str(error) for error in transport_errors)
                    raise RuntimeError(f"{endpoint} provider failure: {detail}")
                return result
            except Exception as exc:
                last_error = exc
                self._consume_transport_errors()
                retryable = isinstance(exc, (OSError, TimeoutError)) or (
                    isinstance(exc, HTTPFetchError) and exc.retryable
                )
                if retryable and attempt + 1 < self.config.max_attempts:
                    counters["retries"] += 1
                    retry_after = exc.retry_after if isinstance(exc, HTTPFetchError) else None
                    self.sleep(self._backoff_delay(endpoint, attempt, retry_after))
                    continue
                counters["provider_failures"] += 1
                raise
        raise RuntimeError(f"{endpoint} failed after retries: {last_error}")

    def _consume_transport_errors(self) -> tuple[Any, ...]:
        consumer = getattr(self.provider, "consume_transport_errors", None)
        if not callable(consumer):
            return ()
        return tuple(consumer())

    def _backoff_delay(self, endpoint: str, attempt: int, retry_after: float | None) -> float:
        base = min(
            self.config.backoff_max_seconds,
            self.config.backoff_initial_seconds * (self.config.backoff_multiplier**attempt),
        )
        if retry_after is not None:
            base = max(base, min(self.config.backoff_max_seconds, float(retry_after)))
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
