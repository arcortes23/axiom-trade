"""Deterministic ingestion orchestration for provider data and SQLite storage."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from itertools import islice
from typing import Any, Iterable, Mapping, Sequence

from ..domain import (
    MarketType,
    OrderBookLevel,
    OrderBookSnapshot,
    PredictionMarketSnapshot,
    SettlementState,
    SimulationQuality,
    ensure_utc,
    parse_timestamp,
    to_record,
)
from ..evaluation import dataset_version
from ..storage import AxiomStore
from .interfaces import CryptoMarketDataProvider, PredictionMarketDataProvider
_MAX_INGEST_MARKETS = 1_000
_MAX_INGEST_RECORDS = 100_000


def _consume_transport_errors(provider: Any, context: str) -> list[str]:
    consume = getattr(provider, "consume_transport_errors", None)
    if not callable(consume):
        return []
    try:
        errors = consume()
    except Exception as exc:
        return [f"{context}: transport error collector failed: {exc}"]
    messages: list[str] = []
    for error in errors or ():
        status = getattr(error, "status", None)
        messages.append(f"{context}: HTTP {status}" if status is not None else f"{context}: {error}")
    return messages


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
        errors: list[str] = []
        try:
            bars = tuple(islice(iter(provider.historical_ohlcv(symbol, start=start, end=end, interval=interval)), _MAX_INGEST_RECORDS))
        except Exception as exc:
            bars = ()
            errors.append(f"historical data error: {exc}")
        errors.extend(_consume_transport_errors(provider, "historical data"))
        resolved_version = version or dataset_version(bars)
        quality = _provider_quality(provider, SimulationQuality.MEDIUM if bars else SimulationQuality.LOW)
        dataset_id = f"crypto:{symbol.replace('/', '').replace('-', '').upper()}"
        try:
            instrument = provider.metadata(symbol)
        except Exception as exc:
            instrument = None
            errors.append(f"metadata error: {exc}")
        errors.extend(_consume_transport_errors(provider, "metadata"))
        payload_metadata = {
            "provider": getattr(provider, "provider_name", provider.__class__.__name__),
            "symbol": symbol,
            "interval": interval,
            "instrument_metadata": to_record(instrument) if instrument is not None else None,
            "instrument_metadata_available": instrument is not None,
            **dict(metadata or {}),
        }
        if not errors or bars:
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
            tuple(errors),
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
            try:
                discovered = tuple(islice(iter(provider.markets(active=False)), _MAX_INGEST_MARKETS))
                market_ids = (snapshot.market_id for snapshot in discovered)
                discovery_errors = _consume_transport_errors(provider, "market discovery")
                if discovery_errors:
                    return (
                        IngestionReport(
                            "prediction:unavailable",
                            "unavailable",
                            str(getattr(provider, "provider_name", provider.__class__.__name__)),
                            MarketType.PREDICTION,
                            0,
                            SimulationQuality.LOW,
                            errors=tuple(discovery_errors),
                        ),
                    )
            except Exception as exc:
                discovery_errors = [f"market discovery error: {exc}"]
                discovery_errors.extend(_consume_transport_errors(provider, "market discovery"))
                return (
                    IngestionReport(
                        "prediction:unavailable",
                        "unavailable",
                        str(getattr(provider, "provider_name", provider.__class__.__name__)),
                        MarketType.PREDICTION,
                        0,
                        SimulationQuality.LOW,
                        errors=tuple(discovery_errors),
                    ),
                )
        provider_name = str(getattr(provider, "provider_name", provider.__class__.__name__))
        reports: list[IngestionReport] = []
        for market_id in islice(iter(market_ids), _MAX_INGEST_MARKETS):
            try:
                market = provider.market(str(market_id))
                market_error: str | None = None
            except Exception as exc:
                market = None
                market_error = f"market lookup error: {exc}"
            market_transport_errors = _consume_transport_errors(provider, f"market {market_id}")
            if market is None:
                reports.append(
                    IngestionReport(
                        str(market_id),
                        "unavailable",
                        provider_name,
                        MarketType.PREDICTION,
                        0,
                        errors=tuple(
                            item
                            for item in (market_error, *market_transport_errors, "market metadata unavailable")
                            if item
                        ),
                    )
                )
                continue
            dataset_id = f"prediction:{market.market_id}"
            try:
                instrument = provider.metadata(market.market_id)
                metadata_error: str | None = None if instrument is not None else "instrument metadata unavailable"
            except Exception as exc:
                instrument = None
                metadata_error = f"instrument metadata error: {exc}"
            metadata_transport_errors = _consume_transport_errors(provider, f"metadata {market.market_id}")
            try:
                history = tuple(islice(iter(provider.price_history(market.market_id, start=start, end=end)), _MAX_INGEST_RECORDS))
                history_error: str | None = None
            except Exception as exc:
                history = ()
                history_error = f"price history error: {exc}"
            history_transport_errors = _consume_transport_errors(provider, f"price history {market.market_id}")
            snapshots = tuple(_historical_snapshots(market, history))
            resolved_version = version_prefix or dataset_version(
                history or ({"market_id": market.market_id, "timestamp": market.timestamp},)
            )
            quality = _provider_quality(
                provider,
                SimulationQuality.LOW
                if not snapshots or not history or not all(_has_depth(item) for item in snapshots)
                else SimulationQuality.MEDIUM,
            )
            errors = tuple(
                item
                for item in (
                    market_error,
                    *market_transport_errors,
                    history_error,
                    *history_transport_errors,
                    metadata_error,
                    *metadata_transport_errors,
                    "price history unavailable" if not history else None,
                )
                if item
            )
            if history_error is None and history:
                raw_market = to_record(market)
                metadata_record = to_record(instrument) if instrument is not None else None
                market_extra = getattr(instrument, "extra", {}) if instrument is not None else {}
                with self.store.transaction():
                    self.store.save_dataset(
                        dataset_id,
                        resolved_version,
                        history,
                        metadata={
                            "provider": provider_name,
                            "market_id": market.market_id,
                            "question": market.question,
                            "resolution_criteria": market.resolution_criteria,
                            "rules": market.resolution_criteria,
                            "expiry": market.expiry,
                            "yes_token_id": market.yes_token_id or market_extra.get("yes_token_id"),
                            "no_token_id": market.no_token_id or market_extra.get("no_token_id"),
                            "condition_id": market_extra.get("condition_id"),
                            "outcomes": market_extra.get("outcomes"),
                            "active": market_extra.get(
                                "active",
                                market.settlement not in {
                                    SettlementState.RESOLVED_YES,
                                    SettlementState.RESOLVED_NO,
                                    SettlementState.VOID,
                                },
                            ),
                            "closed": market_extra.get(
                                "closed",
                                market.settlement in {
                                    SettlementState.RESOLVED_YES,
                                    SettlementState.RESOLVED_NO,
                                    SettlementState.VOID,
                                },
                            ),
                            "start": market_extra.get("start") or raw_market.get("timestamp"),
                            "end": market_extra.get("end") or market.expiry,
                            "raw_market": raw_market,
                            "instrument_metadata": metadata_record,
                            "instrument_metadata_available": instrument is not None,
                            "historical_order_book": bool(snapshots and all(item.order_book is not None for item in snapshots)),
                            "research_quality": "ORDER_BOOK_SIMULATED" if any(item.order_book is not None for item in snapshots) else "PRICE_PROXY",
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
                    errors,
                )
            )
        return tuple(reports)


def _provider_quality(provider: Any, fallback: SimulationQuality) -> SimulationQuality:
    value = getattr(provider, "simulation_quality", None)
    if isinstance(value, SimulationQuality):
        return value
    try:
        return SimulationQuality(str(value)) if value is not None else fallback
    except ValueError:
        return fallback


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


def _has_depth(snapshot: PredictionMarketSnapshot) -> bool:
    if not isinstance(snapshot, PredictionMarketSnapshot):
        return False
    if snapshot.order_book is not None:
        return bool(snapshot.order_book.bids and snapshot.order_book.asks)
    return any(
        bid is not None and ask is not None and bid <= ask
        for bid, ask in (
            (snapshot.yes_bid, snapshot.yes_ask),
            (snapshot.no_bid, snapshot.no_ask),
        )
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
    parsed_timestamp = parse_timestamp(raw_timestamp) or timestamp
    try:
        return OrderBookSnapshot(
            parsed_timestamp,
            bids,
            asks,
            token_id=value.get("token_id"),
        )
    except (TypeError, ValueError):
        return None


def _historical_snapshots(
    market: PredictionMarketSnapshot,
    history: Sequence[Mapping[str, Any]],
) -> Iterable[PredictionMarketSnapshot]:
    terminal_states = {
        SettlementState.RESOLVED_YES,
        SettlementState.RESOLVED_NO,
        SettlementState.VOID,
    }
    valid_points: list[tuple[Mapping[str, Any], datetime]] = []
    for point in history:
        if not isinstance(point, Mapping):
            continue
        timestamp = parse_timestamp(point.get("timestamp", point.get("t")))
        if timestamp is not None:
            valid_points.append((point, timestamp))
    valid_points.sort(key=lambda item: item[1])
    for index, (point, timestamp) in enumerate(valid_points):
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
        state = SettlementState.OPEN
        raw_state = point.get("settlement")
        try:
            candidate_state = (
                SettlementState(str(raw_state).strip().lower()) if raw_state is not None else SettlementState.OPEN
            )
        except ValueError:
            candidate_state = SettlementState.UNKNOWN
        if candidate_state in terminal_states:
            state = candidate_state
        elif (
            index == len(valid_points) - 1
            and market.settlement in terminal_states
            and timestamp >= market.timestamp
        ):
            # Resolution is observable only at the source's resolution
            # timestamp, never copied backward into prior price points.
            state = market.settlement
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
            settlement=state,
            resolution_criteria=market.resolution_criteria,
            category=market.category,
            tags=market.tags,
            order_book=order_book,
            source=str(point.get("source", market.source)),
            yes_token_id=market.yes_token_id,
            no_token_id=market.no_token_id,
        )


DataPipeline = MarketDataPipeline


__all__ = ["DataPipeline", "IngestionReport", "MarketDataPipeline"]
