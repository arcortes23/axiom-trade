"""Market data provider interfaces and deterministic adapters.

Adapters are read-only: they never place orders or submit transactions. Network
adapters return empty/``None`` values when a public endpoint is unavailable,
while synthetic providers make offline research reproducible.
"""

from .interfaces import CryptoMarketDataProvider, PredictionMarketDataProvider
from .binance import BinanceAdapter
from .polymarket import PolymarketAdapter
from .synthetic import (
    InMemoryCryptoProvider,
    InMemoryPredictionProvider,
    SyntheticCryptoMarketDataProvider,
    SyntheticCryptoProvider,
    SyntheticPredictionMarketDataProvider,
    SyntheticPredictionProvider,
)
from .pipeline import DataPipeline, IngestionReport, MarketDataPipeline


__all__ = [
    "BinanceAdapter",
    "CryptoMarketDataProvider",
    "DataPipeline",
    "InMemoryCryptoProvider",
    "InMemoryPredictionProvider",
    "IngestionReport",
    "MarketDataPipeline",
    "PolymarketAdapter",
    "PredictionMarketDataProvider",
    "SyntheticCryptoMarketDataProvider",
    "SyntheticCryptoProvider",
    "SyntheticPredictionMarketDataProvider",
    "SyntheticPredictionProvider",
]
