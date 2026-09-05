from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import unittest
from urllib.parse import parse_qs, urlparse

from axiom.bootstrap import _coerce_universe_snapshot

from axiom.crypto_universe import (
    CURRENT_UNIVERSE,
    SURVIVORSHIP_BIAS_PRESENT,
    TOP_50_MARKET_CAP_BINANCE_USDT,
    CoinGeckoRankingProvider,
    CryptoUniverseBuilder,
    UniverseConfig,
    crypto_universe_status,
    load_crypto_universe,
)
from axiom.data.binance import BinanceAdapter
from axiom.storage import AxiomStore


UTC = timezone.utc
T0 = datetime(2025, 1, 1, tzinfo=UTC)


def ranked(*items: tuple[int, str, str, str]) -> list[dict[str, object]]:
    return [
        {"market_cap_rank": rank, "id": asset_id, "symbol": symbol, "name": name, "market_cap": 1_000_000 - rank}
        for rank, asset_id, symbol, name in items
    ]


class StaticRanking:
    provider_name = "fixture-ranking"

    def __init__(self, rows: object) -> None:
        self.rows = rows

    def rankings(self, limit: int) -> object:
        if isinstance(self.rows, BaseException):
            raise self.rows
        return self.rows  # type: ignore[return-value]


class StaticBinance:
    def __init__(self, rows: object) -> None:
        self.rows = rows

    def exchange_symbols(self, *, quote_asset: str) -> object:
        if isinstance(self.rows, BaseException):
            raise self.rows
        return self.rows


class CryptoUniverseTests(unittest.TestCase):
    def test_top_n_order_intersection_and_exclusion_reasons(self) -> None:
        rows = ranked(
            (3, "bitcoin", "btc", "Bitcoin"),
            (1, "ethereum", "eth", "Ethereum"),
            (2, "tether", "usdt", "Tether"),
            (4, "usd-coin", "usdc", "USD Coin"),
            (5, "wrapped-bitcoin", "wbtc", "Wrapped Bitcoin"),
            (6, "bitcoin-up", "BTCUP", "Bitcoin Up"),
            (7, "xrp", "xrp", "XRP"),
            (8, "cardano", "ada", "Cardano"),
            (9, "dogecoin", "doge", "Dogecoin"),
        )
        exchange = [
            {"symbol": "ETHUSDT", "baseAsset": "ETH", "quoteAsset": "USDT", "status": "TRADING", "isSpotTradingAllowed": True},
            {"symbol": "BTCUSDT", "baseAsset": "BTC", "quoteAsset": "USDT", "status": "TRADING", "isSpotTradingAllowed": True},
            {"symbol": "USDTUSDT", "baseAsset": "USDT", "quoteAsset": "USDT", "status": "TRADING", "isSpotTradingAllowed": True},
            {"symbol": "USDCUSDT", "baseAsset": "USDC", "quoteAsset": "USDT", "status": "TRADING", "isSpotTradingAllowed": True},
            {"symbol": "WBTCUSDT", "baseAsset": "WBTC", "quoteAsset": "USDT", "status": "TRADING", "isSpotTradingAllowed": True},
            {"symbol": "BTCUPUSDT", "baseAsset": "BTCUP", "quoteAsset": "USDT", "status": "TRADING", "isSpotTradingAllowed": True},
            {"symbol": "XRPUSDT", "baseAsset": "XRP", "quoteAsset": "USDT", "status": "BREAK", "isSpotTradingAllowed": True},
            {"symbol": "ADAUSDT", "baseAsset": "ADA", "quoteAsset": "USDT", "status": "TRADING", "isSpotTradingAllowed": False},
            {"symbol": "DOGEBTC", "baseAsset": "DOGE", "quoteAsset": "BTC", "status": "TRADING", "isSpotTradingAllowed": True},
        ]
        config = UniverseConfig(top_n=8, wrapped_staked_pegged_ids={"wrapped-bitcoin"})
        with AxiomStore(":memory:") as store:
            result = CryptoUniverseBuilder(StaticRanking(rows), StaticBinance(exchange), store, config=config).build(now=T0)
        self.assertEqual([row["rank"] for row in result.records], list(range(1, 9)))
        self.assertEqual(result.selected_symbols, ("ETHUSDT", "BTCUSDT"))
        reasons = {row["asset_id"]: row["reason"] for row in result.records}
        self.assertEqual(reasons["tether"], "STABLECOIN")
        self.assertEqual(reasons["usd-coin"], "STABLECOIN")
        self.assertEqual(reasons["wrapped-bitcoin"], "CONFIGURED_DUPLICATE")
        self.assertEqual(reasons["bitcoin-up"], "LEVERAGED_TOKEN_STYLE")
        self.assertEqual(reasons["xrp"], "BINANCE_STATUS_NOT_TRADING")
        self.assertEqual(reasons["cardano"], "BINANCE_SPOT_NOT_ALLOWED")
        self.assertEqual(result.labels, (CURRENT_UNIVERSE, SURVIVORSHIP_BIAS_PRESENT))


    def test_custom_universe_id_uses_immutable_namespace_for_refresh_status_and_load(self) -> None:
        rows = ranked((1, "bitcoin", "btc", "Bitcoin"))
        exchange = [{"symbol": "BTCUSDT", "baseAsset": "BTC", "quoteAsset": "USDT", "status": "TRADING", "isSpotTradingAllowed": True}]
        config = UniverseConfig(universe_id="custom-id", top_n=1)
        self.assertEqual(config.universe_id, "custom-id")
        self.assertEqual(config.persisted_dataset_id, "universe:custom-id")
        with AxiomStore(":memory:") as store:
            refreshed = CryptoUniverseBuilder(StaticRanking(rows), StaticBinance(exchange), store, config=config).build(now=T0)
            loaded = load_crypto_universe(store, universe_id="custom-id", version=refreshed.version)
            status = crypto_universe_status(store, universe_id="custom-id")
            self.assertEqual(refreshed.universe_id, "custom-id")
            self.assertEqual(loaded, refreshed)
            self.assertEqual(status["universe_id"], "custom-id")
            self.assertEqual(status["dataset_id"], "universe:custom-id")
            self.assertEqual(status["versions"], [refreshed.version])


    def test_bootstrap_coercion_requires_exact_persisted_membership(self) -> None:
        rows = ranked((1, "bitcoin", "btc", "Bitcoin"))
        exchange = [
            {
                "symbol": "BTCUSDT",
                "baseAsset": "BTC",
                "quoteAsset": "USDT",
                "status": "TRADING",
                "isSpotTradingAllowed": True,
            }
        ]
        with AxiomStore(":memory:") as store:
            snapshot = CryptoUniverseBuilder(StaticRanking(rows), StaticBinance(exchange), store).build(now=T0)
            loaded = _coerce_universe_snapshot(store, snapshot, snapshot.version)
            self.assertIsNotNone(loaded)
            assert loaded is not None
            self.assertEqual(loaded.version, snapshot.version)
            self.assertEqual(loaded.selected_symbols, snapshot.selected_symbols)

            mapping = snapshot.as_dict()
            accepted = _coerce_universe_snapshot(store, mapping, None)
            self.assertIsNotNone(accepted)
            assert accepted is not None
            self.assertEqual(accepted.version, snapshot.version)
            self.assertEqual(accepted.records, loaded.records)

            forged = dict(mapping)
            forged["selected_symbols"] = ["FORGEDUSDT"]
            forged["symbols"] = ["FORGEDUSDT"]
            with self.assertRaisesRegex(ValueError, "selected membership"):
                _coerce_universe_snapshot(store, forged, None)


    def test_stale_fallback_is_exact_and_versions_are_immutable(self) -> None:
        rows = ranked((1, "bitcoin", "btc", "Bitcoin"))
        exchange = [{"symbol": "BTCUSDT", "baseAsset": "BTC", "quoteAsset": "USDT", "status": "TRADING", "isSpotTradingAllowed": True}]
        with AxiomStore(":memory:") as store:
            builder = CryptoUniverseBuilder(StaticRanking(rows), StaticBinance(exchange), store)
            first = builder.build(now=T0)
            second = CryptoUniverseBuilder(StaticRanking(RuntimeError("provider outage")), StaticBinance(exchange), store).build(force=True, now=T0 + timedelta(days=1))
            self.assertEqual(second.status, "STALE")
            self.assertEqual(second.records, first.records)
            self.assertEqual(second.version, first.version)
            self.assertEqual(store.dataset_versions("universe:" + TOP_50_MARKET_CAP_BINANCE_USDT), [first.version])
            changed = CryptoUniverseBuilder(StaticRanking(rows), StaticBinance(exchange), store).build(force=True, now=T0 + timedelta(days=2))
            self.assertNotEqual(changed.version, first.version)
            self.assertEqual(len(store.dataset_versions("universe:" + TOP_50_MARKET_CAP_BINANCE_USDT)), 2)
            catalog = store.load_dataset_catalog("universe:" + TOP_50_MARKET_CAP_BINANCE_USDT, first.version)
            self.assertEqual(catalog["metadata"]["point_in_time"], True)  # type: ignore[index]

    def test_daily_refresh_uses_cached_snapshot_unless_forced(self) -> None:
        rows = ranked((1, "bitcoin", "btc", "Bitcoin"))
        exchange = [{"symbol": "BTCUSDT", "baseAsset": "BTC", "quoteAsset": "USDT", "status": "TRADING", "isSpotTradingAllowed": True}]
        with AxiomStore(":memory:") as store:
            builder = CryptoUniverseBuilder(StaticRanking(rows), StaticBinance(exchange), store)
            first = builder.build(now=T0)
            cached = CryptoUniverseBuilder(StaticRanking(RuntimeError("must not call")), StaticBinance(exchange), store).build(now=T0 + timedelta(hours=23))
            self.assertEqual(cached.version, first.version)
            self.assertEqual(cached.status, "CURRENT")

    def test_coingecko_request_is_bounded_and_public(self) -> None:
        calls: list[str] = []

        class Response:
            status = 200
            headers = {}

            def read(self) -> bytes:
                return json.dumps([{"id": "bitcoin", "symbol": "btc", "name": "Bitcoin", "market_cap_rank": 1}]).encode()

            def close(self) -> None:
                return

        def opener(request: object, timeout: float) -> Response:
            calls.append(str(getattr(request, "full_url")))
            return Response()

        provider = CoinGeckoRankingProvider(per_page=7, opener=opener, clock=lambda: T0)
        result = provider.rankings(4)
        query = parse_qs(urlparse(calls[0]).query)
        self.assertEqual(query["per_page"], ["4"])
        self.assertEqual(result[0]["observed_at"], T0)


if __name__ == "__main__":
    unittest.main()
