"""Deterministic ingestion orchestration for provider data and SQLite storage."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable, Mapping, Sequence

from ..domain import (
    MarketType,
    OrderBookLevel,
    OrderBookSnapshot,
    PredictionMarketSnapshot,
    SettlementState,
    SimulationQuality,
    ensure_utc,
    to_record,
)
from ..evaluation import dataset_version
from ..storage import AxiomStore
from .interfaces import CryptoMarketDataProvider, PredictionMarketDataProvider


@dataclass(frozen=True, slots=True)
class IngestionReport:
    dataset_id: str
    dataset_version: str
    provider: str
    market_type: MarketType
    records: int
    quality: SimulationQuality
    started_at: datetime | None = None
    ended_at: datetime | None = None
    errors: tuple[str, ...] = ()

    @property
    def count(self) -> int:
        return self.records

    def as_record(self) -> dict[str, Any]:
        return {
            "dataset_id": self.dataset_id,
            "dataset_version": self.dataset_version,
            "provider": self.provider,
            "market_type": self.market_type.value,
            "records": self.records,
            "quality": self.quality.value,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "ended_at": self.ended_at.isoformat() if self.ended_at else None,
            "errors": list(self.errors),
        }


class MarketDataPipeline:
    """Fetch provider observations, assign provenance, and persist atomically."""

    def __init__(self, store: AxiomStore) -> None:
        self.store = store

    def ingest_crypto(
        self,
        provider: CryptoMarketDataProvider,
        symbol: str = "BTC/USDT",
        *,
        start: datetime | None = None,
        end: datetime | None = None,
        interval: str = "1d",
        version: str | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> IngestionReport:
        bars = tuple(provider.historical_ohlcv(symbol, start=start, end=end, interval=interval))
        resolved_version = version or dataset_version(bars)
        quality = _provider_quality(provider, SimulationQuality.MEDIUM if bars else SimulationQuality.LOW)
        dataset_id = f"crypto:{symbol.replace('/', '').replace('-', '').upper()}"
        try:
            instrument = provider.metadata(symbol)
        except Exception:
            instrument = None
        payload_metadata = {
            "provider": getattr(provider, "provider_name", provider.__class__.__name__),
            "symbol": symbol,
            "interval": interval,
            "instrument_metadata": to_record(instrument) if instrument is not None else None,
            "instrument_metadata_available": instrument is not None,
            **dict(metadata or {}),
        }
        with self.store.transaction():
            self.store.save_dataset(
                dataset_id,
                resolved_version,
                bars,
                metadata=payload_metadata,
                quality=quality,
            )
            if bars:
                self.store.save_bars(symbol, bars, dataset_id=dataset_id, dataset_version=resolved_version)
        return IngestionReport(
            dataset_id,
            resolved_version,
            str(getattr(provider, "provider_name", provider.__class__.__name__)),
            MarketType.CRYPTO_SPOT,
            len(bars),
            quality,
            bars[0].timestamp if bars else None,
            bars[-1].timestamp if bars else None,
        )

    def ingest_prediction(
        self,
        provider: PredictionMarketDataProvider,
        market_ids: Iterable[str] | None = None,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
        version_prefix: str | None = None,
    ) -> tuple[IngestionReport, ...]:
        if market_ids is None:
            market_ids = (snapshot.market_id for snapshot in provider.markets(active=False))
        reports: list[IngestionReport] = []
        provider_name = str(getattr(provider, "provider_name", provider.__class__.__name__))
        for market_id in market_ids:
            market = provider.market(str(market_id))
            if market is None:
                reports.append(
                    IngestionReport(
                        str(market_id),
                        "unavailable",
                        provider_name,
                        MarketType.PREDICTION,
                        0,
                        SimulationQuality.LOW,
                        errors=("market metadata unavailable",),
                    )
                )
                continue
            history = tuple(provider.price_history(market.market_id, start=start, end=end))
            snapshots = tuple(_historical_snapshots(market, history))
            raw_records: Sequence[Any] = history or (market,)
            resolved_version = version_prefix or dataset_version(raw_records)
            quality = _provider_quality(
                provider,
                SimulationQuality.LOW
                if not history or not all(_has_depth(item) for item in snapshots)
                else SimulationQuality.MEDIUM,
            )
            dataset_id = f"prediction:{market.market_id}"
            with self.store.transaction():
                self.store.save_dataset(
                    dataset_id,
                    resolved_version,
                    raw_records,
                    metadata={
                        "provider": provider_name,
                        "market_id": market.market_id,
                        "question": market.question,
                        "resolution_criteria": market.resolution_criteria,
                        "expiry": market.expiry,
                        "historical_order_book": all(_has_depth(item) for item in snapshots),
                    },
                    quality=quality,
                )
                if snapshots:
                    self.store.save_prediction_snapshots(
                        market.market_id,
                        snapshots,
                        dataset_id=dataset_id,
                        dataset_version=resolved_version,
                    )
            reports.append(
                IngestionReport(
                    dataset_id,
                    resolved_version,
                    provider_name,
                    MarketType.PREDICTION,
                    len(snapshots),
                    quality,
                    snapshots[0].timestamp if snapshots else market.timestamp,
                    snapshots[-1].timestamp if snapshots else market.timestamp,
                )
            )
        return tuple(reports)


def _finite_float(value: Any) -> float | None:
    try:
        number = float(value) if value is not None else None
    except (TypeError, ValueError):
        return None
    return number if number is not None and number == number and abs(number) != float("inf") else None


def _price01(value: Any) -> float | None:
    number = _finite_float(value)
    return number if number is not None and 0.0 <= number <= 1.0 else None

def _nonnegative_float(value: Any) -> float | None:
    number = _finite_float(value)
    return number if number is not None and number >= 0 else None


def _provider_quality(provider: Any, fallback: SimulationQuality) -> SimulationQuality:
    value = getattr(provider, "simulation_quality", None)
    if isinstance(value, SimulationQuality):
        return value
    try:
        return SimulationQuality(str(value)) if value is not None else fallback
    except ValueError:
        return fallback

def _has_depth(snapshot: PredictionMarketSnapshot) -> bool:
    if not isinstance(snapshot, PredictionMarketSnapshot):
        return False
    if snapshot.order_book is not None:
        return bool(snapshot.order_book.bids and snapshot.order_book.asks)
    return (
        snapshot.yes_bid is not None
        and snapshot.yes_ask is not None
        and snapshot.yes_bid <= snapshot.yes_ask
    )
def _book_from_value(value: Any, timestamp: datetime) -> OrderBookSnapshot | None:
    if isinstance(value, OrderBookSnapshot):
        return value
    if not isinstance(value, Mapping):
        return None
    def levels(raw: Any, *, reverse: bool) -> tuple[OrderBookLevel, ...]:
        result: list[OrderBookLevel] = []
        if not isinstance(raw, (list, tuple)):
            return ()
        for item in raw:
            if isinstance(item, Mapping):
                price, size = item.get("price", item.get("p")), item.get("size", item.get("quantity", item.get("q")))
            elif isinstance(item, (list, tuple)) and len(item) >= 2:
                price, size = item[0], item[1]
            else:
                continue
            try:
                result.append(OrderBookLevel(float(price), float(size)))
            except (TypeError, ValueError):
                continue
        return tuple(sorted(result, key=lambda level: level.price, reverse=reverse))

    bids, asks = levels(value.get("bids"), reverse=True), levels(value.get("asks"), reverse=False)
    if not bids and not asks:
        return None
    raw_timestamp = value.get("timestamp", timestamp)
    if isinstance(raw_timestamp, str):
        from ..data._http import parse_timestamp

        raw_timestamp = parse_timestamp(raw_timestamp) or timestamp
    if not isinstance(raw_timestamp, datetime):
        raw_timestamp = timestamp
    try:
        return OrderBookSnapshot(ensure_utc(raw_timestamp), bids, asks)
    except ValueError:
        return None


def _historical_snapshots(
    market: PredictionMarketSnapshot,
    history: Sequence[Mapping[str, Any]],
) -> Iterable[PredictionMarketSnapshot]:
    for point in history:
        if not isinstance(point, Mapping):
            continue
        timestamp = point.get("timestamp", market.timestamp)
        if isinstance(timestamp, str):
            from ..data._http import parse_timestamp

            timestamp = parse_timestamp(timestamp) or market.timestamp
        if not isinstance(timestamp, datetime):
            timestamp = market.timestamp
        timestamp = ensure_utc(timestamp)
        yes_mid = _price01(point.get("price", point.get("yes_mid")))
        yes_bid = _price01(point.get("yes_bid"))
        yes_ask = _price01(point.get("yes_ask"))
        no_bid = _price01(point.get("no_bid"))
        no_ask = _price01(point.get("no_ask"))
        no_mid = _price01(point.get("no_mid"))
        if no_mid is None and yes_mid is not None:
            no_mid = 1.0 - yes_mid
        if no_bid is None and yes_ask is not None:
            no_bid = 1.0 - yes_ask
        if no_ask is None and yes_bid is not None:
            no_ask = 1.0 - yes_bid
        if yes_bid is not None and yes_ask is not None and yes_bid > yes_ask:
            yes_bid = yes_ask = None
        if no_bid is not None and no_ask is not None and no_bid > no_ask:
            no_bid = no_ask = None
        raw_book = point.get("order_book", point.get("book"))
        order_book = _book_from_value(raw_book, timestamp)
        # Never copy a current resolution label into an earlier price point.
        yield PredictionMarketSnapshot(
            timestamp=timestamp,
            market_id=market.market_id,
            question=market.question,
            yes_bid=yes_bid,
            yes_ask=yes_ask,
            yes_mid=yes_mid,
            no_bid=no_bid,
            no_ask=no_ask,
            no_mid=no_mid,
            volume=_nonnegative_float(point.get("volume")),
            liquidity=_nonnegative_float(point.get("liquidity")),
            expiry=market.expiry,
            settlement=SettlementState.OPEN,
            resolution_criteria=market.resolution_criteria,
            category=market.category,
            tags=market.tags,
            order_book=order_book,
            source=str(point.get("source", market.source)),
        )


DataPipeline = MarketDataPipeline


__all__ = ["DataPipeline", "IngestionReport", "MarketDataPipeline"]
