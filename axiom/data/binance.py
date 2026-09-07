"""Read-only Binance Spot REST adapter.

The adapter only uses public market-data endpoints.  HTTP failures, rate limits,
and offline environments return empty/``None`` values rather than inventing
observations.  Returned timestamps are timezone-aware UTC values.
"""
from __future__ import annotations

from datetime import datetime
import math
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
from ._http import HTTPFetchError, as_float, as_int, fetch_json_strict, parse_timestamp, query_url
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
        clock: Callable[[], datetime] | None = None,
        close_grace: float | Any = 0.0,
        grace: float | Any | None = None,
    ) -> None:
        timeout_value = float(timeout)
        if not math.isfinite(timeout_value) or timeout_value <= 0:
            raise ValueError("timeout must be finite and positive")
        self.symbol = self._normalize_symbol(symbol)
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout_value
        self._opener = opener
        self._transport_errors: list[HTTPFetchError] = []
        self._clock = clock or utc_now
        selected_grace = close_grace if grace is None else grace
        if hasattr(selected_grace, "total_seconds"):
            grace_seconds = float(selected_grace.total_seconds())
        else:
            grace_seconds = float(selected_grace)
        if not math.isfinite(grace_seconds) or grace_seconds < 0:
            raise ValueError("close_grace must be finite and non-negative")
        self.close_grace = grace_seconds

    @staticmethod
    def _normalize_symbol(symbol: str) -> str:
        normalized = str(symbol).replace("/", "").replace("-", "").strip().upper()
        if not normalized:
            raise ValueError("symbol must not be empty")
        return normalized

    def historical_ohlcv(
        self,
        symbol: str,
        start: datetime | None = None,
        end: datetime | None = None,
        interval: str = "1d",
    ) -> Sequence[OHLCVBar]:
        """Return Binance klines, retaining the legacy open-candle behavior.

        This method intentionally does not discard the currently forming kline.
        Canary collection must call :meth:`closed_historical_ohlcv` instead.
        """
        symbol = self._normalize_symbol(symbol)
        rows = self._fetch_kline_rows(symbol, start=start, end=end, interval=interval)
        bars: list[OHLCVBar] = []
        for row in rows:
            bar = self._bar_from_kline(row)
            if bar is not None:
                bars.append(bar)
        bars.sort(key=lambda bar: bar.timestamp)
        return bars

    def closed_historical_ohlcv(
        self,
        symbol: str,
        start: datetime | None = None,
        end: datetime | None = None,
        interval: str = "1d",
        *,
        now: datetime | None = None,
        grace: float | Any | None = None,
        limit: int | None = None,
    ) -> Sequence[OHLCVBar]:
        """Return only klines whose exchange-provided ``closeTime`` has passed.

        Binance's kline open timestamp alone cannot establish that a bar is
        immutable.  The canary path therefore requires the response's
        ``closeTime`` and compares it with an injected UTC clock.  ``grace`` is
        subtracted from the clock to avoid accepting a bar while the exchange
        is still finalizing it.  The legacy :meth:`historical_ohlcv` method is
        unchanged and may return the open/current bar.
        """
        symbol = self._normalize_symbol(symbol)
        rows = self._fetch_kline_rows(symbol, start=start, end=end, interval=interval, limit=limit)
        if now is None:
            now = self._clock()
        cutoff = ensure_utc(now)
        selected_grace = self.close_grace if grace is None else _duration_seconds(grace, "grace")
        if selected_grace < 0 or not math.isfinite(selected_grace):
            raise ValueError("grace must be finite and non-negative")
        cutoff = cutoff.timestamp() - selected_grace
        bars: list[OHLCVBar] = []
        for row in rows:
            if not isinstance(row, (list, tuple)) or len(row) <= 6:
                continue
            close_time = parse_timestamp(row[6])
            if close_time is None or close_time.timestamp() > cutoff:
                continue
            bar = self._bar_from_kline(row)
            if bar is not None:
                bars.append(bar)
        bars.sort(key=lambda bar: bar.timestamp)
        return bars

    # Explicit aliases make the immutable-bar contract discoverable without
    # changing the established historical_ohlcv interface.
    closed_ohlcv = closed_historical_ohlcv
    historical_ohlcv_closed = closed_historical_ohlcv

    def _fetch_kline_rows(
        self,
        symbol: str,
        *,
        start: datetime | None,
        end: datetime | None,
        interval: str,
        limit: int | None = None,
    ) -> list[Any]:
        request_limit = 1000 if limit is None else int(limit)
        if isinstance(limit, bool) or request_limit <= 0:
            raise ValueError("limit must be a positive integer")
        request_limit = min(request_limit, 1000)
        params: dict[str, Any] = {
            "symbol": symbol,
            "interval": interval,
            "limit": request_limit,
            "startTime": _millis(start),
            "endTime": _millis(end),
        }
        rows: list[Any] = []
        for _page in range(100):
            payload = self._get("/api/v3/klines", **params)
            if not isinstance(payload, list):
                break
            rows.extend(payload)
            if start is None or len(payload) < request_limit:
                break
            last_open = as_int(payload[-1][0]) if payload and isinstance(payload[-1], (list, tuple)) else None
            if last_open is None:
                break
            next_start = last_open + 1
            end_ms = _millis(end)
            if next_start <= int(params.get("startTime") or 0) or (end_ms is not None and next_start > end_ms):
                break
            params["startTime"] = next_start
        return rows

    @staticmethod
    def _bar_from_kline(row: Any) -> OHLCVBar | None:
        if not isinstance(row, (list, tuple)) or len(row) < 6:
            return None
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
            return None
        return OHLCVBar(
            timestamp=timestamp,
            open=values[0],
            high=values[1],
            low=values[2],
            close=values[3],
            volume=values[4],
            trades=as_int(row[8]) if len(row) > 8 else None,
        )

    def _get(self, path: str, **params: Any) -> Any | None:
        try:
            return fetch_json_strict(query_url(self.base_url, path, params), self.timeout, self._opener)
        except HTTPFetchError as exc:
            self._transport_errors.append(exc)
            return None


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
        if isinstance(depth, bool) or not isinstance(depth, int) or depth <= 0:
            raise ValueError("depth must be a positive integer")
        payload = self._get("/api/v3/depth", symbol=symbol, limit=min(depth, 5000))
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

    def exchange_info(self, *, symbol: str | None = None) -> Mapping[str, Any] | None:
        """Return public Spot exchange metadata without credentials."""
        normalized = self._normalize_symbol(symbol) if symbol is not None else None
        payload = self._get("/api/v3/exchangeInfo", symbol=normalized)
        return payload if isinstance(payload, Mapping) else None

    def exchange_symbols(self, *, quote_asset: str | None = None) -> tuple[Mapping[str, Any], ...] | None:
        """Return public Binance symbol records, optionally narrowed by quote."""
        payload = self.exchange_info()
        if payload is None:
            return None
        records = payload.get("symbols")
        if not isinstance(records, list):
            return None
        quote = str(quote_asset).strip().upper() if quote_asset is not None else None
        return tuple(
            dict(record)
            for record in records
            if isinstance(record, Mapping)
            and (quote is None or str(record.get("quoteAsset", "")).upper() == quote)
        )

    def discover_spot_symbols(self, *, quote_asset: str = "USDT") -> tuple[str, ...] | None:
        """Return currently tradable Spot symbols for a quote asset."""
        records = self.exchange_symbols(quote_asset=quote_asset)
        if records is None:
            return None
        symbols: list[str] = []
        for record in records:
            if str(record.get("status", "")).upper() != "TRADING":
                continue
            allowed = record.get("isSpotTradingAllowed")
            if allowed is None:
                permissions = record.get("permissions")
                allowed = isinstance(permissions, (list, tuple, set)) and any(
                    str(permission).upper() == "SPOT" for permission in permissions
                )
            if bool(allowed) and str(record.get("symbol", "")).strip():
                symbols.append(str(record["symbol"]).upper())
        return tuple(sorted(set(symbols)))

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
        # Keep the complete public exchangeInfo symbol record.  Downstream
        # canary/risk checks need status, permissions, orderTypes,
        # permissionSets, and every filter (not only tick/lot size).
        extra = dict(record)
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

def _duration_seconds(value: Any, name: str) -> float:
    if hasattr(value, "total_seconds"):
        seconds = float(value.total_seconds())
    else:
        seconds = float(value)
    if not math.isfinite(seconds):
        raise ValueError(f"{name} must be finite")
    return seconds


def _millis(value: datetime | None) -> int | None:
    return int(ensure_utc(value).timestamp() * 1000) if value is not None else None


def _boolish(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    return bool(value)


__all__ = ["BinanceAdapter"]
