"""Read-only Binance Spot REST adapter.

The adapter only uses public market-data endpoints.  HTTP failures, rate limits,
and offline environments return empty/``None`` values rather than inventing
observations.  Returned timestamps are timezone-aware UTC values.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Callable, Mapping, Sequence

from ..domain import (
    CryptoTicker,
    InstrumentMetadata,
    MarketType,
    OHLCVBar,
    OrderBookLevel,
    OrderBookSnapshot,
    Side,
    TradePrint,
    ensure_utc,
    utc_now,
)
from ._http import as_float, as_int, fetch_json, parse_timestamp, query_url
from .interfaces import CryptoMarketDataProvider


class BinanceAdapter(CryptoMarketDataProvider):
    """Binance Spot adapter, defaulting to BTC/USDT.

    Args:
        symbol: Default symbol used by convenience callers. Individual method
            calls may provide another Binance symbol.
        base_url: Public REST API origin, useful for test fixtures.
        timeout: Per-request timeout in seconds; never disabled implicitly.
        opener: Optional urllib-compatible callable for deterministic tests.
    """

    provider_name = "binance"

    def __init__(
        self,
        symbol: str = "BTCUSDT",
        *,
        base_url: str = "https://api.binance.com",
        timeout: float = 10.0,
        opener: Callable[..., Any] | None = None,
    ) -> None:
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        self.symbol = self._normalize_symbol(symbol)
        self.base_url = base_url.rstrip("/")
        self.timeout = float(timeout)
        self._opener = opener

    @staticmethod
    def _normalize_symbol(symbol: str) -> str:
        normalized = str(symbol).replace("/", "").replace("-", "").strip().upper()
        if not normalized:
            raise ValueError("symbol must not be empty")
        return normalized

    def _get(self, path: str, **params: Any) -> Any | None:
        return fetch_json(query_url(self.base_url, path, params), self.timeout, self._opener)

    def historical_ohlcv(
        self,
        symbol: str,
        start: datetime | None = None,
        end: datetime | None = None,
        interval: str = "1d",
    ) -> Sequence[OHLCVBar]:
        symbol = self._normalize_symbol(symbol)
        params: dict[str, Any] = {
            "symbol": symbol,
            "interval": interval,
            "limit": 1000,
            "startTime": _millis(start),
            "endTime": _millis(end),
        }
        bars: list[OHLCVBar] = []
        while True:
            payload = self._get("/api/v3/klines", **params)
            if not isinstance(payload, list):
                break
            for row in payload:
                if not isinstance(row, (list, tuple)) or len(row) < 6:
                    continue
                timestamp = parse_timestamp(row[0])
                values = [as_float(item) for item in row[1:6]]
                if (
                    timestamp is None
                    or any(value is None for value in values)
                    or min(values[:4]) <= 0
                    or values[1] < max(values[0], values[3], values[2])
                    or values[2] > min(values[0], values[3], values[1])
                    or values[4] < 0
                ):
                    continue
                bars.append(
                    OHLCVBar(
                        timestamp=timestamp,
                        open=values[0],
                        high=values[1],
                        low=values[2],
                        close=values[3],
                        volume=values[4],
                        trades=as_int(row[8]) if len(row) > 8 else None,
                    )
                )
            # Without an explicit start, preserve the endpoint's usual
            # "latest 1000" behavior rather than downloading all history.
            if start is None or len(payload) < 1000:
                break
            last_open = as_int(payload[-1][0]) if payload and isinstance(payload[-1], (list, tuple)) else None
            if last_open is None:
                break
            next_start = last_open + 1
            end_ms = _millis(end)
            if next_start <= int(params.get("startTime") or 0) or (end_ms is not None and next_start > end_ms):
                break
            params["startTime"] = next_start
        bars.sort(key=lambda bar: bar.timestamp)
        return bars

    def ticker(self, symbol: str) -> CryptoTicker | None:
        symbol = self._normalize_symbol(symbol)
        payload = self._get("/api/v3/ticker/24hr", symbol=symbol)
        if not isinstance(payload, Mapping):
            return None
        last = as_float(payload.get("lastPrice"))
        if last is None or last <= 0:
            return None
        bid = as_float(payload.get("bidPrice"))
        ask = as_float(payload.get("askPrice"))
        volume = as_float(payload.get("volume"))
        if bid is not None and bid <= 0:
            bid = None
        if ask is not None and ask <= 0:
            ask = None
        if bid is not None and ask is not None and bid > ask:
            bid = ask = None
        if volume is not None and volume < 0:
            volume = None
        timestamp = parse_timestamp(payload.get("closeTime")) or utc_now()
        return CryptoTicker(
            timestamp=timestamp,
            symbol=symbol,
            last=last,
            bid=bid,
            ask=ask,
            volume_24h=volume,
        )

    def trades(
        self,
        symbol: str,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> Sequence[TradePrint]:
        symbol = self._normalize_symbol(symbol)
        historical = start is not None or end is not None
        path = "/api/v3/aggTrades" if historical else "/api/v3/trades"
        params: dict[str, Any] = {"symbol": symbol, "limit": 1000}
        if start is not None:
            params["startTime"] = _millis(start)
        if end is not None:
            params["endTime"] = _millis(end)
        result: list[TradePrint] = []
        seen: set[str] = set()
        for _ in range(1000 if historical else 1):
            payload = self._get(path, **params)
            if not isinstance(payload, list) or not payload:
                break
            last_timestamp: datetime | None = None
            last_id: int | None = None
            for row in payload:
                if not isinstance(row, Mapping):
                    continue
                timestamp = parse_timestamp(row.get("T", row.get("time")))
                price = as_float(row.get("p", row.get("price")))
                size = as_float(row.get("q", row.get("qty")))
                if timestamp is None or price is None or size is None or price <= 0 or size <= 0:
                    continue
                trade_id_value = row.get("a", row.get("id"))
                identity = str(trade_id_value) if trade_id_value is not None else f"{timestamp.isoformat()}|{price}|{size}"
                if identity in seen:
                    continue
                seen.add(identity)
                buyer_maker = _boolish(row.get("m", row.get("isBuyerMaker")))
                side = Side.SELL if buyer_maker else Side.BUY
                result.append(
                    TradePrint(
                        timestamp=timestamp,
                        price=price,
                        size=size,
                        side=side,
                        trade_id=identity,
                        market_id=symbol,
                    )
                )
                last_timestamp = timestamp if last_timestamp is None or timestamp > last_timestamp else last_timestamp
                try:
                    row_id = int(trade_id_value) if trade_id_value is not None else None
                except (TypeError, ValueError):
                    row_id = None
                if row_id is not None:
                    last_id = row_id if last_id is None or row_id > last_id else last_id
            if not historical or len(payload) < 1000:
                break
            if end is not None and last_timestamp is not None and last_timestamp >= ensure_utc(end):
                break
            if last_id is not None:
                params["fromId"] = last_id + 1
                params.pop("startTime", None)
            elif last_timestamp is not None:
                params["startTime"] = int(last_timestamp.timestamp() * 1000) + 1
            else:
                break
        result.sort(key=lambda trade: trade.timestamp)
        return result

    def order_book(self, symbol: str, depth: int = 20) -> OrderBookSnapshot | None:
        symbol = self._normalize_symbol(symbol)
        if depth <= 0:
            raise ValueError("depth must be positive")
        payload = self._get("/api/v3/depth", symbol=symbol, limit=min(int(depth), 5000))
        if not isinstance(payload, Mapping):
            return None
        bids = self._levels(payload.get("bids"), reverse=True, depth=depth)
        asks = self._levels(payload.get("asks"), reverse=False, depth=depth)
        if not bids and not asks:
            return None
        if bids and asks and bids[0].price > asks[0].price:
            return None
        # The depth endpoint has no server timestamp. Retrieval time is the
        # observation timestamp, not a fabricated market price.
        timestamp = parse_timestamp(payload.get("E")) or utc_now()
        return OrderBookSnapshot(timestamp=timestamp, bids=tuple(bids), asks=tuple(asks))

    @staticmethod
    def _levels(value: Any, *, reverse: bool, depth: int) -> list[OrderBookLevel]:
        levels: list[OrderBookLevel] = []
        if not isinstance(value, list):
            return levels
        for row in value:
            if not isinstance(row, (list, tuple)) or len(row) < 2:
                continue
            price, size = as_float(row[0]), as_float(row[1])
            if price is None or size is None or price <= 0 or size <= 0:
                continue
            levels.append(OrderBookLevel(price=price, size=size))
        levels.sort(key=lambda level: level.price, reverse=reverse)
        return levels[:depth]

    def metadata(self, symbol: str) -> InstrumentMetadata | None:
        symbol = self._normalize_symbol(symbol)
        payload = self._get("/api/v3/exchangeInfo", symbol=symbol)
        if not isinstance(payload, Mapping):
            return None
        records = payload.get("symbols")
        if not isinstance(records, list):
            return None
        record = next(
            (
                item
                for item in records
                if isinstance(item, Mapping)
                and str(item.get("symbol", "")).upper() == symbol
            ),
            None,
        )
        if record is None:
            return None
        filters = record.get("filters")
        tick_size = lot_size = None
        if isinstance(filters, list):
            for item in filters:
                if not isinstance(item, Mapping):
                    continue
                kind = item.get("filterType")
                if kind == "PRICE_FILTER":
                    tick_size = as_float(item.get("tickSize"))
                elif kind == "LOT_SIZE":
                    lot_size = as_float(item.get("stepSize"))
        extra = {
            key: record[key]
            for key in ("status", "permissions", "quoteOrderQtyMarketAllowed", "isSpotTradingAllowed")
            if key in record
        }
        return InstrumentMetadata(
            symbol=str(record.get("symbol", symbol)),
            market_type=MarketType.CRYPTO_SPOT,
            provider=self.provider_name,
            base_asset=record.get("baseAsset"),
            quote_asset=record.get("quoteAsset"),
            tick_size=tick_size,
            lot_size=lot_size,
            currency=str(record.get("quoteAsset") or "USD"),
            extra=extra,
        )


def _millis(value: datetime | None) -> int | None:
    return int(ensure_utc(value).timestamp() * 1000) if value is not None else None


def _boolish(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    return bool(value)


__all__ = ["BinanceAdapter"]
