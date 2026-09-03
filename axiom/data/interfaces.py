"""Read-only interfaces shared by market data adapters.

The interfaces deliberately contain no order execution methods.  Historical
series from an adapter can be used for backtests, but callers should account
for source quality (missing bars, stale quotes, and exchange maintenance) when
assigning a :class:`~axiom.domain.SimulationQuality` to an experiment.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Mapping, Sequence

from ..domain import (
    CryptoTicker,
    InstrumentMetadata,
    OHLCVBar,
    OrderBookSnapshot,
    PredictionMarketSnapshot,
    TradePrint,
)


class CryptoMarketDataProvider(ABC):
    """Abstract, read-only crypto market data provider.

    Implementations must return UTC-aware canonical domain objects.  Network
    failures are represented by an empty sequence or ``None``; an adapter must
    not replace unavailable observations with fabricated prices.
    """

    @abstractmethod
    def historical_ohlcv(
        self,
        symbol: str,
        start: datetime | None = None,
        end: datetime | None = None,
        interval: str = "1d",
    ) -> Sequence[OHLCVBar]:
        """Return bars in chronological order."""

    @abstractmethod
    def ticker(self, symbol: str) -> CryptoTicker | None:
        """Return the latest ticker, or ``None`` when unavailable."""

    @abstractmethod
    def trades(
        self,
        symbol: str,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> Sequence[TradePrint]:
        """Return public trade prints in chronological order."""

    @abstractmethod
    def order_book(self, symbol: str, depth: int = 20) -> OrderBookSnapshot | None:
        """Return a level-2 snapshot, or ``None`` when unavailable."""

    @abstractmethod
    def metadata(self, symbol: str) -> InstrumentMetadata | None:
        """Return exchange instrument metadata, or ``None`` when unavailable."""


class PredictionMarketDataProvider(ABC):
    """Abstract, read-only prediction market data provider."""

    @abstractmethod
    def markets(self, active: bool = True) -> Sequence[PredictionMarketSnapshot]:
        """Discover markets while preserving source questions and rules."""

    @abstractmethod
    def market(self, market_id: str) -> PredictionMarketSnapshot | None:
        """Return a single market snapshot, if found."""

    @abstractmethod
    def price_history(
        self,
        market_id: str,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> Sequence[Mapping[str, Any]]:
        """Return timestamped probability observations as plain mappings."""

    @abstractmethod
    def order_book(
        self, market_id: str, depth: int = 20
    ) -> OrderBookSnapshot | None:
        """Return the YES-token order book, or ``None`` when unavailable."""

    @abstractmethod
    def metadata(self, market_id: str) -> InstrumentMetadata | None:
        """Return exact question, rules, tags, expiry and settlement metadata."""


__all__ = ["CryptoMarketDataProvider", "PredictionMarketDataProvider"]
