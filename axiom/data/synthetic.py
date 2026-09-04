"""Deterministic in-memory and synthetic providers for offline research."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Sequence

from ..domain import (
    CryptoTicker,
    InstrumentMetadata,
    MarketType,
    OHLCVBar,
    OrderBookLevel,
    OrderBookSnapshot,
    PredictionMarketSnapshot,
    SettlementState,
    Side,
    SimulationQuality,
    TradePrint,
)
from .interfaces import CryptoMarketDataProvider, PredictionMarketDataProvider

_DEFAULT_START = datetime(2024, 1, 1, tzinfo=timezone.utc)


def _utc(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


def _in_range(timestamp: datetime, start: datetime | None, end: datetime | None) -> bool:
    stamp = _utc(timestamp)
    return (start is None or stamp >= _utc(start)) and (end is None or stamp <= _utc(end))


class SyntheticCryptoProvider(CryptoMarketDataProvider):
    """Reproducible BTC/USDT OHLCV stream with no network access.

    Prices are generated from integer arithmetic (not random state), making
    repeated experiments identical. These observations are explicitly
    synthetic and should normally be marked ``SimulationQuality.LOW`` by a
    research caller; they are not exchange history or execution quotes.
    """

    provider_name = "synthetic"
    simulation_quality = SimulationQuality.LOW

    def __init__(
        self,
        symbol: str = "BTCUSDT",
        *,
        start: datetime = _DEFAULT_START,
        periods: int = 30,
        interval: str = "1d",
    ) -> None:
        if periods < 0:
            raise ValueError("periods must be non-negative")
        self.symbol = str(symbol).replace("/", "").replace("-", "").upper()
        if not self.symbol:
            raise ValueError("symbol must not be empty")
        self._start = _utc(start)
        self._interval = interval
        self._bars = self._generate(periods)

    def _generate(self, periods: int) -> tuple[OHLCVBar, ...]:
        step = _interval_delta(self._interval)
        bars: list[OHLCVBar] = []
        for index in range(periods):
            base = 42_000.0 + index * 37.0 + ((index * 17) % 11) * 9.0
            open_price = base
            close = base + (index % 5 - 2) * 23.0
            high = max(open_price, close) + 41.0 + (index % 4) * 3.0
            low = min(open_price, close) - 38.0 - (index % 3) * 2.0
            bars.append(
                OHLCVBar(
                    timestamp=self._start + index * step,
                    open=open_price,
                    high=high,
                    low=low,
                    close=close,
                    volume=100.0 + index * 2.5,
                    trades=1000 + index * 13,
                )
            )
        return tuple(bars)

    def historical_ohlcv(
        self,
        symbol: str,
        start: datetime | None = None,
        end: datetime | None = None,
        interval: str = "1d",
    ) -> Sequence[OHLCVBar]:
        del interval
        if self._normalize(symbol) != self.symbol:
            return []
        return tuple(bar for bar in self._bars if _in_range(bar.timestamp, start, end))

    def ticker(self, symbol: str) -> CryptoTicker | None:
        if self._normalize(symbol) != self.symbol or not self._bars:
            return None
        bar = self._bars[-1]
        spread = round(bar.close * 0.0004, 8)
        return CryptoTicker(
            timestamp=bar.timestamp + _interval_delta(self._interval),
            symbol=self.symbol,
            last=bar.close,
            bid=bar.close - spread,
            ask=bar.close + spread,
            volume_24h=bar.volume,
        )

    def trades(
        self,
        symbol: str,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> Sequence[TradePrint]:
        if self._normalize(symbol) != self.symbol:
            return []
        result: list[TradePrint] = []
        for bar in self._bars:
            if not _in_range(bar.timestamp, start, end):
                continue
            result.extend(
                (
                    TradePrint(bar.timestamp + timedelta(minutes=10), bar.open, bar.volume * 0.1, Side.BUY),
                    TradePrint(
                        bar.timestamp + timedelta(minutes=20),
                        bar.close,
                        bar.volume * 0.1,
                        Side.SELL if bar.close < bar.open else Side.BUY,
                    ),
                )
            )
        return tuple(result)

    def order_book(self, symbol: str, depth: int = 20) -> OrderBookSnapshot | None:
        if isinstance(depth, bool) or not isinstance(depth, int) or depth <= 0:
            raise ValueError("depth must be a positive integer")
        ticker = self.ticker(symbol)
        if ticker is None:
            return None
        bids = tuple(
            OrderBookLevel(round(ticker.bid - index * 2.0, 8), 0.5 + index * 0.1)
            for index in range(depth)
        )
        asks = tuple(
            OrderBookLevel(round(ticker.ask + index * 2.0, 8), 0.5 + index * 0.1)
            for index in range(depth)
        )
        return OrderBookSnapshot(timestamp=ticker.timestamp, bids=bids, asks=asks)

    def metadata(self, symbol: str) -> InstrumentMetadata | None:
        if self._normalize(symbol) != self.symbol:
            return None
        base, quote = _split_symbol(self.symbol)
        return InstrumentMetadata(
            symbol=self.symbol,
            market_type=MarketType.CRYPTO_SPOT,
            provider=self.provider_name,
            base_asset=base,
            quote_asset=quote,
            tick_size=0.01,
            lot_size=0.000001,
            currency=quote,
            extra={"synthetic": True, "interval": self._interval},
        )

    @staticmethod
    def _normalize(symbol: str) -> str:
        return str(symbol).replace("/", "").replace("-", "").upper()


class InMemoryCryptoProvider(SyntheticCryptoProvider):
    """In-memory crypto provider accepting caller-supplied canonical records."""

    provider_name = "memory"

    def __init__(
        self,
        bars: Mapping[str, Sequence[OHLCVBar]] | Sequence[OHLCVBar] | None = None,
        *,
        tickers: Mapping[str, CryptoTicker] | None = None,
        trades: Mapping[str, Sequence[TradePrint]] | None = None,
        order_books: Mapping[str, OrderBookSnapshot] | None = None,
        metadata: Mapping[str, InstrumentMetadata] | None = None,
    ) -> None:
        if isinstance(bars, Mapping):
            bars_by_symbol = bars
            first_symbol = next(iter(bars), "BTCUSDT") if bars else "BTCUSDT"
        elif bars is None:
            bars_by_symbol = {}
            first_symbol = "BTCUSDT"
        else:
            bars_by_symbol = {"BTCUSDT": bars}
            first_symbol = "BTCUSDT"
        super().__init__(first_symbol, periods=0)
        self._bars_by_symbol = {
            self._normalize(key): tuple(sorted(value, key=lambda item: item.timestamp))
            for key, value in bars_by_symbol.items()
        }
        self._tickers = {self._normalize(key): value for key, value in (tickers or {}).items()}
        self._trades_by_symbol = {self._normalize(key): tuple(value) for key, value in (trades or {}).items()}
        self._books = {self._normalize(key): value for key, value in (order_books or {}).items()}
        self._metadata = {self._normalize(key): value for key, value in (metadata or {}).items()}

    def historical_ohlcv(self, symbol: str, start: datetime | None = None, end: datetime | None = None, interval: str = "1d") -> Sequence[OHLCVBar]:
        del interval
        return tuple(bar for bar in self._bars_by_symbol.get(self._normalize(symbol), ()) if _in_range(bar.timestamp, start, end))

    def ticker(self, symbol: str) -> CryptoTicker | None:
        key = self._normalize(symbol)
        if key in self._tickers:
            return self._tickers[key]
        bars = self._bars_by_symbol.get(key, ())
        if not bars:
            return None
        bar = bars[-1]
        return CryptoTicker(bar.timestamp, key, bar.close)

    def trades(self, symbol: str, start: datetime | None = None, end: datetime | None = None) -> Sequence[TradePrint]:
        return tuple(item for item in self._trades_by_symbol.get(self._normalize(symbol), ()) if _in_range(item.timestamp, start, end))

    def order_book(self, symbol: str, depth: int = 20) -> OrderBookSnapshot | None:
        if isinstance(depth, bool) or not isinstance(depth, int) or depth <= 0:
            raise ValueError("depth must be a positive integer")
        book = self._books.get(self._normalize(symbol))
        if book is None:
            return None
        return OrderBookSnapshot(book.timestamp, book.bids[:depth], book.asks[:depth])

    def metadata(self, symbol: str) -> InstrumentMetadata | None:
        return self._metadata.get(self._normalize(symbol))


class SyntheticPredictionProvider(PredictionMarketDataProvider):
    """One deterministic YES/NO market and probability history."""

    provider_name = "synthetic"
    simulation_quality = SimulationQuality.LOW

    def __init__(self, market_id: str = "synthetic-event-1") -> None:
        self.market_id = str(market_id)
        stamp = datetime(2024, 1, 1, tzinfo=timezone.utc)
        self._snapshot = PredictionMarketSnapshot(
            timestamp=stamp,
            market_id=self.market_id,
            question="Will the synthetic reference price finish above its baseline?",
            yes_bid=0.48,
            yes_ask=0.52,
            yes_mid=0.50,
            no_bid=0.48,
            no_ask=0.52,
            no_mid=0.50,
            volume=10_000.0,
            liquidity=2_500.0,
            expiry=stamp + timedelta(days=30),
            settlement=SettlementState.OPEN,
            resolution_criteria="Resolve YES when the reference close is above 42,000 at expiry; otherwise NO.",
            category="synthetic",
            tags=("synthetic", "offline"),
            order_book=OrderBookSnapshot(
                timestamp=stamp,
                bids=(OrderBookLevel(0.48, 100.0), OrderBookLevel(0.47, 200.0)),
                asks=(OrderBookLevel(0.52, 100.0), OrderBookLevel(0.53, 200.0)),
            ),
            source=self.provider_name,
        )
        self._history = tuple(
            {"timestamp": stamp + timedelta(days=index), "price": 0.45 + index * 0.01}
            for index in range(6)
        )

    def markets(self, active: bool = True) -> Sequence[PredictionMarketSnapshot]:
        if active and self._snapshot.settlement is not SettlementState.OPEN:
            return ()
        return (self._snapshot,)

    def market(self, market_id: str) -> PredictionMarketSnapshot | None:
        return self._snapshot if str(market_id) == self.market_id else None

    def price_history(self, market_id: str, start: datetime | None = None, end: datetime | None = None) -> Sequence[Mapping[str, Any]]:
        if str(market_id) != self.market_id:
            return ()
        return tuple(item for item in self._history if _in_range(item["timestamp"], start, end))

    def order_book(self, market_id: str, depth: int = 20) -> OrderBookSnapshot | None:
        if depth <= 0:
            raise ValueError("depth must be positive")
        snapshot = self.market(market_id)
        if snapshot is None or snapshot.order_book is None:
            return None
        book = snapshot.order_book
        return OrderBookSnapshot(book.timestamp, book.bids[:depth], book.asks[:depth])

    def metadata(self, market_id: str) -> InstrumentMetadata | None:
        snapshot = self.market(market_id)
        if snapshot is None:
            return None
        return InstrumentMetadata(
            symbol=self.market_id,
            market_type=MarketType.PREDICTION,
            provider=self.provider_name,
            currency="USD",
            market_id=self.market_id,
            question=snapshot.question,
            resolution_criteria=snapshot.resolution_criteria,
            category=snapshot.category,
            tags=snapshot.tags,
            expiry=snapshot.expiry,
            extra={"synthetic": True, "settlement": snapshot.settlement.value},
        )


class InMemoryPredictionProvider(SyntheticPredictionProvider):
    """In-memory prediction provider for deterministic fixture datasets."""

    provider_name = "memory"

    def __init__(
        self,
        markets: Sequence[PredictionMarketSnapshot] | Mapping[str, PredictionMarketSnapshot] | None = None,
        *,
        histories: Mapping[str, Sequence[Mapping[str, Any]]] | None = None,
        order_books: Mapping[str, OrderBookSnapshot] | None = None,
        metadata: Mapping[str, InstrumentMetadata] | None = None,
    ) -> None:
        entries = list(markets.values()) if isinstance(markets, Mapping) else list(markets or ())
        if entries:
            super().__init__(entries[0].market_id)
        else:
            super().__init__()
        self._markets = {item.market_id: item for item in entries}
        self._histories = {str(key): tuple(value) for key, value in (histories or {}).items()}
        self._books = {str(key): value for key, value in (order_books or {}).items()}
        self._metadata = {str(key): value for key, value in (metadata or {}).items()}

    def markets(self, active: bool = True) -> Sequence[PredictionMarketSnapshot]:
        return tuple(item for item in self._markets.values() if not active or item.settlement is SettlementState.OPEN)

    def market(self, market_id: str) -> PredictionMarketSnapshot | None:
        return self._markets.get(str(market_id))

    def price_history(self, market_id: str, start: datetime | None = None, end: datetime | None = None) -> Sequence[Mapping[str, Any]]:
        return tuple(item for item in self._histories.get(str(market_id), ()) if _in_range(item["timestamp"], start, end))

    def order_book(self, market_id: str, depth: int = 20) -> OrderBookSnapshot | None:
        if depth <= 0:
            raise ValueError("depth must be positive")
        book = self._books.get(str(market_id))
        if book is None:
            snapshot = self.market(market_id)
            book = snapshot.order_book if snapshot is not None else None
        if book is None:
            return None
        return OrderBookSnapshot(book.timestamp, book.bids[:depth], book.asks[:depth])

    def metadata(self, market_id: str) -> InstrumentMetadata | None:
        if str(market_id) in self._metadata:
            return self._metadata[str(market_id)]
        snapshot = self.market(market_id)
        if snapshot is None:
            return None
        return InstrumentMetadata(
            symbol=snapshot.market_id,
            market_type=MarketType.PREDICTION,
            provider=self.provider_name,
            currency="USD",
            market_id=snapshot.market_id,
            question=snapshot.question,
            resolution_criteria=snapshot.resolution_criteria,
            category=snapshot.category,
            tags=snapshot.tags,
            expiry=snapshot.expiry,
            extra={"in_memory": True, "settlement": snapshot.settlement.value},
        )


def _interval_delta(interval: str) -> timedelta:
    text = str(interval).strip().lower()
    try:
        amount = max(1, int(text[:-1])) if text[:-1] else 1
    except ValueError:
        amount = 1
    if text.endswith("m"):
        return timedelta(minutes=amount)
    if text.endswith("h"):
        return timedelta(hours=amount)
    if text.endswith("w"):
        return timedelta(weeks=amount)
    return timedelta(days=amount)


def _split_symbol(symbol: str) -> tuple[str, str]:
    for quote in ("USDT", "USDC", "BUSD", "USD", "BTC", "ETH"):
        if symbol.endswith(quote) and len(symbol) > len(quote):
            return symbol[: -len(quote)], quote
    return symbol, "USD"


SyntheticCryptoMarketDataProvider = SyntheticCryptoProvider
SyntheticPredictionMarketDataProvider = SyntheticPredictionProvider

__all__ = [
    "InMemoryCryptoProvider",
    "InMemoryPredictionProvider",
    "SyntheticCryptoMarketDataProvider",
    "SyntheticCryptoProvider",
    "SyntheticPredictionMarketDataProvider",
    "SyntheticPredictionProvider",
]
