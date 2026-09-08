from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import time
from threading import Event, Lock
from urllib.parse import parse_qs, urlsplit
import unittest
from unittest.mock import patch

from axiom.binance_market import AUTO_DEADLINE_EXPIRED, BoundedBinanceMarketCollector
from axiom.crypto_universe import UniverseSnapshot
from axiom.data.binance import BinanceAdapter, BinanceDeadlineExpired
from axiom.domain import CryptoTicker, OHLCVBar, OrderBookLevel, OrderBookSnapshot



UTC = timezone.utc
NOW = datetime(2026, 1, 2, 12, 0, tzinfo=UTC)


def _kline(open_at: datetime, *, close_time: datetime, close: float = 101.0) -> list[object]:
    stamp = int(open_at.timestamp() * 1000)
    close_stamp = int(close_time.timestamp() * 1000)
    return [stamp, "100", "105", "99", str(close), "12", close_stamp, "1200", 7, "0", "0", "0"]


def _bar(stamp: datetime, close: float = 101.0) -> OHLCVBar:
    return OHLCVBar(stamp, 100.0, 105.0, 99.0, close, 12.0, trades=7)


def _book(stamp: datetime = NOW) -> OrderBookSnapshot:
    return OrderBookSnapshot(
        timestamp=stamp,
        bids=(OrderBookLevel(99.0, 1.5), OrderBookLevel(98.5, 2.0)),
        asks=(OrderBookLevel(101.0, 1.0), OrderBookLevel(101.5, 2.0)),
    )


def _universe(*, status: str = "CURRENT") -> UniverseSnapshot:
    return UniverseSnapshot(
        universe_id="crypto-universe",
        version="2026-01-02T12:00:00Z",
        snapshot_hash="sha256:universe-snapshot",
        observed_at=NOW,
        status=status,
        records=(
            {
                "symbol": "Ethereum",
                "asset_symbol": "ETH",
                "asset_id": "ethereum",
                "binance_symbol": "ETHUSDT",
                "rank": 2,
                "selected": True,
            },
            {
                "symbol": "Bitcoin",
                "asset_symbol": "BTC",
                "asset_id": "bitcoin",
                "binance_symbol": "BTCUSDT",
                "rank": 1,
                "selected": True,
            },
            {
                "symbol": "XRP",
                "asset_symbol": "XRP",
                "asset_id": "xrp",
                "binance_symbol": "XRPUSDT",
                "rank": 3,
                "selected": True,
            },
        ),
        labels=("CURRENT_UNIVERSE", "POINT_IN_TIME"),
        metadata={
            "methodology": "fixture-ranking",
            "survivorship_bias": "point_in_time",
        },
    )


class JsonResponse:
    status = 200
    headers: dict[str, str] = {}

    def __init__(self, payload: object) -> None:
        self.payload = payload

    def read(self) -> bytes:
        return json.dumps(self.payload).encode("utf-8")

    def close(self) -> None:
        return None


class RoutedResponse(JsonResponse):
    def __init__(
        self,
        payload: object,
        *,
        status: int = 200,
        response_url: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        super().__init__(payload)
        self.status = status
        self.headers = headers or {}
        self.response_url = response_url

    def geturl(self) -> str | None:
        return self.response_url


class FakeOpener:
    """urllib-compatible public fixture; no credentials or signing surface."""

    def __init__(self, routes: dict[str, object]) -> None:
        self.routes = routes
        self.calls: list[tuple[str, float, dict[str, list[str]]]] = []

    def __call__(self, request: object, *, timeout: float) -> JsonResponse:
        url = str(getattr(request, "full_url", request))
        parsed = urlsplit(url)
        query = parse_qs(parsed.query)
        self.calls.append((parsed.path, timeout, query))
        return JsonResponse(self.routes[parsed.path])


class PublicProvider:
    provider_name = "fake-binance-public"

    def __init__(self, *, blocked: str | None = None, failed: str | None = None) -> None:
        self.blocked = blocked
        self.failed = failed
        self.started = Event()
        self.release = Event()
        self.calls: list[tuple[str, str]] = []
        self.exchange: dict[str, dict[str, object]] = {
            "BTCUSDT": {
                "symbol": "BTCUSDT",
                "status": "TRADING",
                "quoteAsset": "USDT",
                "isSpotTradingAllowed": True,
                "permissions": ["SPOT"],
            },
            "ETHUSDT": {
                "symbol": "ETHUSDT",
                "status": "TRADING",
                "quoteAsset": "USDT",
                "isSpotTradingAllowed": True,
                "permissions": ["SPOT"],
            },
            "XRPUSDT": {
                "symbol": "XRPUSDT",
                "status": "TRADING",
                "quoteAsset": "USDT",
                "isSpotTradingAllowed": False,
                "permissions": [],
            },
            "LTCUSDT": {
                "symbol": "LTCUSDT",
                "status": "TRADING",
                "quoteAsset": "USDT",
                "isSpotTradingAllowed": True,
                "permissions": ["SPOT"],
            },
        }

    def exchange_info(self, symbol: str) -> dict[str, object]:
        symbol = symbol.upper()
        self.calls.append(("exchange_info", symbol))
        return self.exchange.get(symbol, {"symbol": symbol, "status": "BREAK", "quoteAsset": "USDT"})

    def closed_historical_ohlcv(
        self,
        symbol: str,
        *,
        interval: str = "1d",
        limit: int = 1000,
        now: datetime | None = None,
        grace: float = 0.0,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> tuple[OHLCVBar, ...]:
        symbol = symbol.upper()
        self.calls.append(("closed_historical_ohlcv", symbol))
        if symbol == self.blocked:
            self.started.set()
            self.release.wait(timeout=2.0)
        if symbol == self.failed:
            raise RuntimeError("fixture kline failure")
        return (_bar(NOW - timedelta(minutes=2)),)

    def historical_ohlcv(self, symbol: str, start=None, end=None, interval="1d") -> tuple[OHLCVBar, ...]:
        self.calls.append(("historical_ohlcv", symbol.upper()))
        return (_bar(NOW - timedelta(minutes=2)),)

    def ticker(self, symbol: str) -> CryptoTicker:
        symbol = symbol.upper()
        self.calls.append(("ticker", symbol))
        return CryptoTicker(NOW - timedelta(seconds=20), symbol, 100.5, bid=99.0, ask=101.0, volume_24h=1000.0)

    def order_book(self, symbol: str, depth: int = 20) -> OrderBookSnapshot:
        symbol = symbol.upper()
        self.calls.append(("order_book", symbol))
        return _book(NOW - timedelta(seconds=20))


class LegacyProvider(PublicProvider):
    closed_historical_ohlcv = None  # type: ignore[assignment]

    def historical_ohlcv(self, symbol: str, start=None, end=None, interval="1d") -> list[dict[str, object]]:
        return [
            {
                "timestamp": NOW - timedelta(minutes=3),
                "open": 100,
                "high": 105,
                "low": 99,
                "close": 101,
                "volume": 12,
                "closeTime": NOW.timestamp() * 1000 - 60_000,
            },
            {
                "timestamp": NOW - timedelta(minutes=1),
                "open": 100,
                "high": 105,
                "low": 99,
                "close": 101,
                "volume": 12,
                "closeTime": NOW.timestamp() * 1000 + 60_000,
            },
        ]


class BinanceAdapterPublicTests(unittest.TestCase):
    def test_closed_candles_filter_exchange_close_time_and_preserve_legacy_method(self):
        routes = {
            "/api/v3/klines": [
                _kline(NOW - timedelta(minutes=3), close_time=NOW - timedelta(seconds=1), close=101),
                _kline(NOW - timedelta(minutes=2), close_time=NOW, close=102),
                _kline(NOW - timedelta(minutes=1), close_time=NOW + timedelta(seconds=1), close=103),
            ]
        }
        opener = FakeOpener(routes)
        adapter = BinanceAdapter(opener=opener, clock=lambda: NOW, close_grace=timedelta(seconds=1))

        legacy = adapter.historical_ohlcv("btc/usdt", interval="1m")
        closed = adapter.closed_historical_ohlcv("BTC-USDT", interval="1m")
        alias = adapter.closed_ohlcv("BTCUSDT", interval="1m", now=NOW, grace=0)
        second_alias = adapter.historical_ohlcv_closed("BTCUSDT", interval="1m", now=NOW, grace=0)

        self.assertEqual(len(legacy), 3, "legacy historical method intentionally retains the open candle")
        self.assertEqual([bar.close for bar in closed], [101.0])
        self.assertEqual([bar.close for bar in alias], [101.0, 102.0])
        self.assertEqual([bar.close for bar in second_alias], [101.0, 102.0])
        self.assertEqual(opener.calls[0][2]["symbol"], ["BTCUSDT"])
        self.assertEqual(opener.calls[1][2]["symbol"], ["BTCUSDT"])

    def test_public_opener_covers_ticker_book_exchange_metadata_without_order_surface(self):
        routes = {
            "/api/v3/klines": [_kline(NOW - timedelta(minutes=2), close_time=NOW - timedelta(seconds=10))],
            "/api/v3/ticker/24hr": {
                "symbol": "BTCUSDT",
                "lastPrice": "100.5",
                "bidPrice": "100",
                "askPrice": "101",
                "volume": "123",
                "closeTime": int((NOW - timedelta(seconds=5)).timestamp() * 1000),
            },
            "/api/v3/depth": {
                "E": int((NOW - timedelta(seconds=4)).timestamp() * 1000),
                "bids": [["100", "2"], ["99", "3"]],
                "asks": [["101", "1"], ["102", "4"]],
            },
            "/api/v3/exchangeInfo": {
                "symbols": [
                    {
                        "symbol": "BTCUSDT",
                        "status": "TRADING",
                        "quoteAsset": "USDT",
                        "baseAsset": "BTC",
                        "isSpotTradingAllowed": True,
                        "permissions": ["SPOT"],
                        "orderTypes": ["LIMIT"],
                        "filters": [{"filterType": "PRICE_FILTER", "tickSize": "0.01"}],
                    }
                ]
            },
        }
        adapter = BinanceAdapter(opener=FakeOpener(routes), clock=lambda: NOW)

        ticker = adapter.ticker("BTCUSDT")
        book = adapter.order_book("BTCUSDT", depth=2)
        info = adapter.exchange_info(symbol="BTCUSDT")
        metadata = adapter.metadata("BTCUSDT")

        self.assertEqual(ticker.symbol, "BTCUSDT")
        self.assertEqual(book.best_bid, 100.0)
        self.assertEqual(book.best_ask, 101.0)
        self.assertEqual(info["symbols"][0]["permissions"], ["SPOT"])
        self.assertEqual(metadata.extra["orderTypes"], ["LIMIT"])
        for name in ("account", "orders", "place_order", "cancel_order", "create_order"):
            self.assertFalse(callable(getattr(adapter, name, None)), name)

    def test_public_redirect_and_alternate_final_origin_are_rejected_before_decode(self):
        calls: list[str] = []
        responses = iter(
            (
                RoutedResponse(
                    {},
                    status=302,
                    headers={"Location": "https://evil.example/api/v3/ticker/24hr"},
                ),
                RoutedResponse(
                    {
                        "symbol": "BTCUSDT",
                        "lastPrice": "100.5",
                        "bidPrice": "100",
                        "askPrice": "101",
                    },
                    response_url="https://evil.example/api/v3/ticker/24hr",
                ),
            )
        )

        def opener(request: object, *, timeout: float) -> RoutedResponse:
            del timeout
            calls.append(str(getattr(request, "full_url", request)))
            return next(responses)

        origin = "https://testnet.binance.vision"
        adapter = BinanceAdapter(base_url=origin, opener=opener, clock=lambda: NOW)

        self.assertIsNone(adapter.ticker("BTCUSDT"))
        self.assertIsNone(adapter.ticker("BTCUSDT"))
        self.assertEqual(calls, [f"{origin}/api/v3/ticker/24hr?symbol=BTCUSDT"] * 2)

    def test_trades_propagate_absolute_deadline_across_pages(self):
        adapter = BinanceAdapter()
        first_page = [
            {"a": index, "T": int((NOW + timedelta(seconds=index)).timestamp() * 1000), "p": "100", "q": "1"}
            for index in range(1000)
        ]
        second_page = [{"a": 1000, "T": int((NOW + timedelta(seconds=1000)).timestamp() * 1000), "p": "100", "q": "1"}]
        calls: list[float | None] = []
        pages = [first_page, second_page]

        def fake_get(path: str, *, deadline_monotonic: float | None = None, **params: object) -> object:
            calls.append(deadline_monotonic)
            return pages.pop(0)

        adapter._get = fake_get  # type: ignore[method-assign]
        deadline = time.monotonic() + 30.0
        trades = adapter.trades("BTCUSDT", start=NOW, deadline_monotonic=deadline)

        self.assertEqual(len(trades), 1001)
        self.assertEqual(calls, [deadline, deadline])

    def test_transport_error_crossing_deadline_is_not_swallowed(self):
        calls: list[float] = []

        class SlowFailure:
            def __call__(self, request: object, *, timeout: float) -> object:
                calls.append(timeout)
                raise TimeoutError("fixture transport timeout")

        adapter = BinanceAdapter(opener=SlowFailure(), timeout=1.0)
        with patch("axiom.data.binance.time.monotonic", side_effect=(100.0, 100.002)):
            with self.assertRaises(BinanceDeadlineExpired):
                adapter.ticker("BTCUSDT", deadline_monotonic=100.001)
        self.assertEqual(len(calls), 1)



class BinanceMarketCollectorTests(unittest.TestCase):
    def test_exact_universe_provenance_rank_order_recheck_and_market_evidence(self):
        provider = PublicProvider()
        collector = BoundedBinanceMarketCollector(
            provider,
            _universe(),
            max_workers=2,
            timeout=1,
            clock=lambda: NOW,
            freshness=60,
            depth=2,
            fill_quantity=2.5,
            source="fixture-public",
            quality="HIGH",
            survivorship_bias="point_in_time",
        )

        result = collector.collect(interval="5m", limit=10, dataset_version="dataset-v9", now=NOW)

        self.assertEqual([record.symbol for record in result], ["BTCUSDT", "ETHUSDT", "XRPUSDT"])
        self.assertEqual(result.provenance, {
            "universe_id": "crypto-universe",
            "universe_version": "2026-01-02T12:00:00Z",
            "version": "2026-01-02T12:00:00Z",
            "snapshot_hash": "sha256:universe-snapshot",
            "interval": "5m",
            "dataset_version": "dataset-v9",
            "source": "fixture-public",
            "quality": "HIGH",
            "survivorship_bias": "point_in_time",
            "observed_at": NOW.isoformat(),
        })
        btc = result["BTCUSDT"]
        self.assertEqual(btc.asset_symbol, "BTC")
        self.assertEqual(btc.asset_id, "bitcoin")
        self.assertEqual(btc.rank, 1)
        self.assertTrue(btc.tradable)
        self.assertTrue(btc.new_entry_allowed)
        self.assertTrue(btc.ticker_fresh)
        self.assertTrue(btc.book_fresh)
        self.assertEqual(btc.depth["requested_levels"], 2)
        self.assertEqual(btc.depth["bid_levels"], 2)
        self.assertEqual(btc.depth["ask_levels"], 2)
        self.assertEqual(btc.depth["bid_size"], 3.5)
        self.assertEqual(btc.depth["ask_size"], 3.0)
        self.assertEqual(btc.spread, 2.0)
        self.assertEqual(btc.spread_evidence["bps"], 200.0)
        self.assertEqual(btc.fill_evidence["buy"]["filled_quantity"], 2.5)
        self.assertTrue(btc.fill_evidence["buy"]["complete"])
        self.assertEqual(btc.fill_evidence["sell"]["filled_quantity"], 2.5)
        xrp = result["XRPUSDT"]
        self.assertFalse(xrp.tradable)
        self.assertFalse(xrp.new_entry_allowed)
        self.assertIn("SPOT_NOT_ALLOWED", xrp.reasons)
        self.assertEqual(result.as_mapping()["ETHUSDT"].provenance["dataset_version"], "dataset-v9")

    def test_legacy_historical_provider_filters_open_bar_before_canary_evidence(self):
        provider = LegacyProvider()
        result = BoundedBinanceMarketCollector(provider, _universe(), clock=lambda: NOW, freshness=60).collect(
            interval="1m", limit=10, now=NOW
        )

        for record in result:
            self.assertEqual(len(record.bars), 1)
            self.assertEqual(record.bars[0].timestamp, NOW - timedelta(minutes=3))

    def test_removed_symbol_is_exit_only_and_stale_snapshot_never_allows_entry(self):
        provider = PublicProvider()
        result = BoundedBinanceMarketCollector(provider, _universe(), clock=lambda: NOW).collect(
            exit_symbols=("LTC/USDT",), reconciliation=True, now=NOW
        )
        removed = result["LTCUSDT"]
        self.assertTrue(removed.exit_only)
        self.assertFalse(removed.selected)
        self.assertFalse(removed.new_entry_allowed)
        self.assertIn("EXIT_ONLY_SYMBOL", removed.reasons)
        self.assertEqual(removed.rank, 10**9)

        stale = BoundedBinanceMarketCollector(provider, _universe(status="STALE"), clock=lambda: NOW).collect(now=NOW)
        for record in stale:
            self.assertFalse(record.new_entry_allowed)
            self.assertIn("UNIVERSE_STALE", record.reasons)
            self.assertEqual(record.universe_version, "2026-01-02T12:00:00Z")
            self.assertEqual(record.snapshot_hash, "sha256:universe-snapshot")

    def test_bounded_workers_return_healthy_and_failed_symbols_while_blocked_symbol_times_out(self):
        provider = PublicProvider(blocked="XRPUSDT", failed="ETHUSDT")
        collector = BoundedBinanceMarketCollector(
            provider,
            _universe(),
            max_workers=2,
            timeout=0.05,
            clock=lambda: NOW,
        )
        result = collector.collect(now=NOW)
        try:
            self.assertEqual([record.symbol for record in result], ["BTCUSDT", "ETHUSDT", "XRPUSDT"])
            self.assertTrue(provider.started.is_set())
            self.assertTrue(result["BTCUSDT"].new_entry_allowed)
            self.assertIn("BARS_ERROR: fixture kline failure", result["ETHUSDT"].reasons)
            self.assertTrue(result["XRPUSDT"].timed_out)
            self.assertIn("TIMEOUT", result["XRPUSDT"].reasons)
        finally:
            provider.release.set()
            provider.started.wait(timeout=2.0)
    def test_deadline_propagates_to_every_provider_call(self):
        class DeadlineProvider(PublicProvider):
            def __init__(self) -> None:
                super().__init__()
                self.deadlines: list[float | None] = []

            def exchange_info(self, symbol: str, *, deadline_monotonic: float | None = None) -> dict[str, object]:
                self.deadlines.append(deadline_monotonic)
                return super().exchange_info(symbol)

            def closed_historical_ohlcv(
                self, symbol: str, *, deadline_monotonic: float | None = None, **kwargs: object
            ) -> tuple[OHLCVBar, ...]:
                self.deadlines.append(deadline_monotonic)
                return super().closed_historical_ohlcv(symbol, **kwargs)

            def ticker(self, symbol: str, *, deadline_monotonic: float | None = None) -> CryptoTicker:
                self.deadlines.append(deadline_monotonic)
                return super().ticker(symbol)

            def order_book(
                self, symbol: str, depth: int = 20, *, deadline_monotonic: float | None = None
            ) -> OrderBookSnapshot:
                self.deadlines.append(deadline_monotonic)
                return super().order_book(symbol, depth)

        provider = DeadlineProvider()
        deadline = time.monotonic() + 30.0
        BoundedBinanceMarketCollector(provider, _universe(), max_workers=1, clock=lambda: NOW).collect(
            now=NOW, deadline_monotonic=deadline
        )
        self.assertTrue(provider.deadlines)
        self.assertEqual(set(provider.deadlines), {deadline})

    def test_already_expired_deadline_makes_zero_provider_calls(self):
        provider = PublicProvider()
        result = BoundedBinanceMarketCollector(provider, _universe(), clock=lambda: NOW).collect(
            now=NOW, deadline_monotonic=time.monotonic() - 1.0
        )

        self.assertEqual(provider.calls, [])
        self.assertTrue(all(record.timed_out for record in result))
        self.assertTrue(all(record.error == AUTO_DEADLINE_EXPIRED for record in result))
        self.assertTrue(all(AUTO_DEADLINE_EXPIRED in record.reasons for record in result))

    def test_deadline_cancels_queued_work_and_joins_started_workers(self):
        class SlowFirstProvider(PublicProvider):
            def __init__(self) -> None:
                super().__init__()
                self.lock = Lock()
                self.active = 0
                self.max_active = 0
                self.exchange_calls: list[str] = []

            def exchange_info(self, symbol: str, *, deadline_monotonic: float | None = None) -> dict[str, object]:
                with self.lock:
                    self.active += 1
                    self.max_active = max(self.max_active, self.active)
                    self.exchange_calls.append(symbol)
                try:
                    if symbol == "BTCUSDT":
                        time.sleep(0.03)
                    return super().exchange_info(symbol)
                finally:
                    with self.lock:
                        self.active -= 1

        provider = SlowFirstProvider()
        result = BoundedBinanceMarketCollector(
            provider, _universe(), max_workers=1, clock=lambda: NOW
        ).collect(now=NOW, deadline_monotonic=time.monotonic() + 0.002)

        self.assertEqual(provider.active, 0)
        self.assertLessEqual(len(provider.exchange_calls), 1)
        self.assertTrue(all(record.timed_out for record in result))


if __name__ == "__main__":
    unittest.main()
