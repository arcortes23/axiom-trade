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
import math
import time
from typing import Any, Callable, Mapping, Sequence

from .data.interfaces import PredictionMarketDataProvider
from .domain import (
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


@dataclass(frozen=True, slots=True)
class CollectorConfig:
    """Operational settings for the public-data collector."""

    interval_seconds: float = 60.0
    depth: int = 20
    stale_after_seconds: float | None = None
    max_markets: int | None = None
    active: bool = True
    market_ids: tuple[str, ...] = ()
    collector_name: str = "polymarket"
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
        if self.max_markets is not None and (
            not isinstance(self.max_markets, int) or isinstance(self.max_markets, bool) or self.max_markets <= 0
        ):
            raise ValueError("max_markets must be a positive integer")
        if not str(self.collector_name).strip():
            raise ValueError("collector_name is required")
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
        discovered: dict[str, PredictionMarketSnapshot] = {}
        requested = tuple(dict.fromkeys(str(item).strip() for item in (market_ids or ()) if str(item).strip()))
        configured = requested or self.config.market_ids
        if configured:
            ids = list(configured)
        else:
            try:
                for snapshot in self.provider.markets(active=self.config.active):
                    if isinstance(snapshot, PredictionMarketSnapshot):
                        discovered[snapshot.market_id] = snapshot
            except Exception as exc:
                self.store.save_collection_error(None, started, "discovery", str(exc))
            # Previously observed markets remain resumable after discovery outages.
            ids = list(dict.fromkeys([*discovered, *self.store.tracked_polymarket_markets()]))
        if self.config.max_markets is not None:
            ids = ids[: self.config.max_markets]
        counters = {
            "metadata_inserted": 0,
            "snapshots_inserted": 0,
            "snapshot_duplicates": 0,
            "trades_inserted": 0,
            "trade_duplicates": 0,
            "errors": 0,
        }
        for market_id in ids:
            try:
                self._collect_market(market_id, discovered.get(market_id), started, counters)
            except Exception as exc:
                counters["errors"] += 1
                self.store.save_collection_error(market_id, started, "collector", str(exc))
        ended = ensure_utc(now or self.clock())
        if ended < started:
            ended = started
        self.store.set_collector_state(
            self.config.collector_name,
            {
                "last_cycle_started_at": started.isoformat(),
                "last_cycle_ended_at": ended.isoformat(),
                "markets_seen": len(ids),
                **counters,
            },
        )
        return CollectionCycle(started, ended, len(ids), **counters)

    def run_forever(
        self,
        *,
        cycles: int | None = None,
        stop_event: Any | None = None,
    ) -> list[CollectionCycle]:
        """Run finite cycles in tests or continue until ``stop_event`` is set."""
        if cycles is not None and cycles < 0:
            raise ValueError("cycles must be non-negative or None")
        results: list[CollectionCycle] = []
        completed = 0
        while cycles is None or completed < cycles:
            if stop_event is not None and stop_event.is_set():
                break
            results.append(self.collect_once())
            completed += 1
            if cycles is not None and completed >= cycles:
                break
            if stop_event is not None and stop_event.is_set():
                break
            self.sleep(self.config.interval_seconds)
        return results

    def _collect_market(
        self,
        market_id: str,
        discovered: PredictionMarketSnapshot | None,
        observed_at: datetime,
        counters: dict[str, int],
    ) -> None:
        cycle_errors_before = counters["errors"]
        snapshot = self.provider.market(market_id) or discovered
        if snapshot is None:
            counters["errors"] += 1
            self.store.save_collection_error(market_id, observed_at, "market_unavailable", "market metadata unavailable")
            return
        try:
            metadata = self.provider.metadata(market_id)
        except Exception as exc:
            metadata = None
            counters["errors"] += 1
            self.store.save_collection_error(market_id, observed_at, "metadata", str(exc))
        metadata_payload: dict[str, Any] = {
            "snapshot": to_record(snapshot),
            "metadata": to_record(metadata) if metadata is not None else None,
            "token_ids": {
                "yes": snapshot.yes_token_id,
                "no": snapshot.no_token_id,
            },
            "source": getattr(self.provider, "provider_name", type(self.provider).__name__),
        }
        with self.store.transaction():
            if self.store.save_polymarket_market_metadata(market_id, metadata_payload, observed_at=observed_at):
                counters["metadata_inserted"] += 1
            try:
                books = dict(self.provider.order_books(market_id, depth=self.config.depth))
            except Exception as exc:
                books = {}
                counters["errors"] += 1
                self.store.save_collection_error(market_id, observed_at, "order_book", str(exc))
            yes_book = books.get("yes")
            no_book = books.get("no")
            if not isinstance(yes_book, OrderBookSnapshot):
                yes_book = None
            if not isinstance(no_book, OrderBookSnapshot):
                no_book = None
            canonical, source_timestamp = _canonical_snapshot(snapshot, yes_book, no_book)
            quality = ResearchQuality.ORDER_BOOK_SIMULATED if yes_book is not None or no_book is not None else ResearchQuality.PRICE_PROXY
            payload = {
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
                "depth": {
                    "yes": _book_depth(yes_book),
                    "no": _book_depth(no_book),
                },
                "volume": canonical.volume,
                "liquidity": canonical.liquidity,
                "observed_at": observed_at.isoformat(),
                "source_timestamp": source_timestamp.isoformat(),
                "time_to_resolution_seconds": (
                    max(0.0, (canonical.expiry - source_timestamp).total_seconds()) if canonical.expiry else None
                ),
                "yes_token_id": canonical.yes_token_id,
                "no_token_id": canonical.no_token_id,
                "settlement": canonical.settlement.value,
                "research_quality": quality.value,
            }
            snapshot_id = hashlib.sha256(_stable_payload(payload).encode("utf-8")).hexdigest()
            if self.store.save_polymarket_snapshot(
                snapshot_id,
                market_id,
                source_timestamp,
                observed_at,
                payload,
                quality=quality,
            ):
                counters["snapshots_inserted"] += 1
            else:
                counters["snapshot_duplicates"] += 1
            state_key = f"{self.config.collector_name}:{market_id}"
            state = self.store.get_collector_state(state_key) or {}
            last_trade = _parse_iso(state.get("last_trade_timestamp"))
            try:
                trades = self.provider.trades(market_id, start=last_trade, end=observed_at)
            except Exception as exc:
                trades = ()
                counters["errors"] += 1
                self.store.save_collection_error(market_id, observed_at, "trades", str(exc))
            latest_trade: datetime | None = last_trade
            for trade in trades:
                if not isinstance(trade, TradePrint):
                    counters["errors"] += 1
                    self.store.save_collection_error(market_id, observed_at, "malformed_trade", "provider returned non-TradePrint")
                    continue
                if trade.timestamp > observed_at:
                    counters["errors"] += 1
                    self.store.save_collection_error(market_id, observed_at, "future_trade", "trade timestamp is after collection observation")
                    continue
                if self.store.save_polymarket_trade(market_id, trade):
                    counters["trades_inserted"] += 1
                else:
                    counters["trade_duplicates"] += 1
                latest_trade = max(latest_trade, trade.timestamp) if latest_trade else trade.timestamp
            self.store.set_collector_state(
                state_key,
                {
                    "market_id": market_id,
                    "last_observed_at": observed_at.isoformat(),
                    "last_source_timestamp": source_timestamp.isoformat(),
                    "last_trade_timestamp": latest_trade.isoformat() if latest_trade else None,
                    "polls": int(state.get("polls", 0)) + 1,
                    "errors": int(state.get("errors", 0)) + counters["errors"] - cycle_errors_before,
                },
            )


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
