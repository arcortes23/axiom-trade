from __future__ import annotations

from datetime import datetime, timedelta, timezone
import unittest

from axiom.data import InMemoryCryptoProvider
from axiom.domain import MarketType, OHLCVBar
from axiom.research import run_crypto_research, run_multi_symbol_crypto_research
from axiom.storage import AxiomStore


class ScalabilityCryptoResearchTests(unittest.TestCase):
    @staticmethod
    def _seed_universe(store: AxiomStore, symbols: list[str], version: str = "snapshot:7") -> None:
        snapshot_hash = f"sha256:{version}"
        rows = [{"symbol": symbol, "selected": True} for symbol in symbols]
        metadata = {
            "universe_id": "crypto-study",
            "snapshot_hash": snapshot_hash,
            "methodology": "fixed ranked snapshot",
            "survivorship_bias": "none",
            "point_in_time": True,
        }
        store.save_dataset("universe:crypto-study", version, rows, metadata=metadata, quality="HIGH")
        store.save_dataset_catalog(
            "universe:crypto-study",
            version,
            provider="fixture",
            instrument="crypto-study",
            market_type=MarketType.CRYPTO_SPOT,
            timeframe="point_in_time",
            row_count=len(rows),
            completeness=1.0,
            quality="HIGH",
            source_type="FORWARD_COLLECTED",
            snapshot_id=f"crypto-study:{version}",
            metadata=metadata,
        )
    @staticmethod
    def _bars(*, rising: bool) -> tuple[OHLCVBar, ...]:
        start = datetime(2020, 1, 1, tzinfo=timezone.utc)
        rows: list[OHLCVBar] = []
        for index in range(30):
            close = 100.0 + index if rising else 130.0 - index
            rows.append(
                OHLCVBar(
                    timestamp=start + timedelta(days=index),
                    open=close - 0.25,
                    high=close + 1.0,
                    low=close - 1.0,
                    close=close,
                    volume=100.0 + index,
                )
            )
        return tuple(rows)

    def test_multi_symbol_results_keep_metrics_and_strategy_identity_separate(self) -> None:
        provider = InMemoryCryptoProvider(
            {
                "BTCUSDT": self._bars(rising=True),
                "ETHUSDT": self._bars(rising=False),
            }
        )
        with AxiomStore(":memory:") as store:
            self._seed_universe(store, ["BTC/USDT", "ETH/USDT"], version="2024-01-01")
            report = run_multi_symbol_crypto_research(
                symbols=["BTC/USDT", "ETH/USDT"],
                providers={"BTC/USDT": provider, "ETH/USDT": provider},
                store=store,
                universe_provenance={
                    "universe_id": "crypto-study",
                    "version": "2024-01-01",
                    "methodology": "fixed ranked snapshot",
                    "survivorship_bias": "none; delisted assets retained",
                },
            )

        self.assertEqual(report["symbols"], ["BTC/USDT", "ETH/USDT"])
        self.assertEqual(set(report["metrics"]), {"BTC/USDT", "ETH/USDT"})
        self.assertEqual(len(report["metrics"]["BTC/USDT"]), 8)
        self.assertEqual(len(report["metrics"]["ETH/USDT"]), 8)
        self.assertTrue(all(item["strategy_id"].startswith("btc-usdt-") for item in report["per_symbol"]["BTC/USDT"]["experiments"]))
        self.assertTrue(all(item["strategy_id"].startswith("eth-usdt-") for item in report["per_symbol"]["ETH/USDT"]["experiments"]))
        self.assertNotEqual(report["metrics"]["BTC/USDT"], report["metrics"]["ETH/USDT"])

    def test_cross_sectional_counts_provenance_failures_and_execution_safety(self) -> None:
        provider = InMemoryCryptoProvider({"BTCUSDT": self._bars(rising=True), "ETHUSDT": self._bars(rising=False)})
        provenance = {
            "universe_id": "crypto-study",
            "universe_version": "snapshot:7",
            "methodology": "fixed ranked snapshot",
            "survivorship_bias": "none",
            "selection": {"limit": 3, "quote": "USDT"},
        }
        with AxiomStore(":memory:") as store:
            self._seed_universe(store, ["BTC/USDT", "ETH/USDT", "BAD/USDT"])
            report = run_multi_symbol_crypto_research(
                ["BTC/USDT", "ETH/USDT", "BAD/USDT"],
                {"BTC/USDT": provider, "ETH/USDT": provider},
                store=store,
                universe_provenance=provenance,
            )

        self.assertEqual(report["universe_id"], provenance["universe_id"])
        self.assertEqual(report["universe_version"], provenance["universe_version"])
        self.assertEqual(report["methodology"], provenance["methodology"])
        self.assertEqual(report["survivorship_bias"], provenance["survivorship_bias"])
        self.assertEqual(report["universe_provenance"]["selection"], provenance["selection"])
        self.assertIn("BAD/USDT", report["bad_symbols"])
        self.assertEqual(report["per_symbol"]["BAD/USDT"]["status"], "failed")
        self.assertFalse(report["live_execution"])
        for summary in report["family_counts"].values():
            self.assertEqual(summary["symbol_count"], 3)
            self.assertEqual(summary["evaluated"], 2)
            self.assertEqual(summary["failures"], 1)
            self.assertEqual(summary["survivors"] + summary["rejects"] + summary["failures"], 3)
            self.assertEqual(summary["distribution"]["fitness"]["count"], 2)
    def test_partial_dataset_mapping_rejects_without_provider_calls(self) -> None:
        class ProviderMustNotBeCalled:
            def __init__(self) -> None:
                self.calls = 0

            def historical_ohlcv(self, *_: object, **__: object) -> tuple[object, ...]:
                self.calls += 1
                raise AssertionError("provider must not be consulted for an incomplete exact binding")

            def metadata(self, *_: object, **__: object) -> None:
                self.calls += 1
                raise AssertionError("provider metadata must not be consulted for an incomplete exact binding")

        provider = ProviderMustNotBeCalled()
        with self.assertRaisesRegex(ValueError, r"ETH/USDT:.*dataset_id"):
            run_multi_symbol_crypto_research(
                symbols=["BTC/USDT", "ETH/USDT"],
                provider=provider,
                dataset_id={"BTC/USDT": "crypto:BTCUSDT"},
                dataset_version={"BTC/USDT": "fixture-v1"},
                universe_provenance={
                    "universe_id": "crypto-study",
                    "universe_version": "snapshot:7",
                    "selected_symbols": ["BTC/USDT", "ETH/USDT"],
                },
            )
        self.assertEqual(provider.calls, 0)


    def test_bound_research_filters_persisted_bars_inclusive_utc_before_metrics(self) -> None:
        bars = self._bars(rising=True)
        dataset_id = "crypto:BTCUSDT"
        offset = timezone(timedelta(hours=2))
        start = datetime(2020, 1, 5, 2, tzinfo=offset)
        end = datetime(2020, 1, 15, 2, tzinfo=offset)
        with AxiomStore(":memory:") as store:
            store.save_dataset(
                dataset_id,
                "fixture-v1",
                bars,
                metadata={"instrument": "BTC/USDT", "timeframe": "1d", "source_type": "HISTORICAL"},
            )
            store.save_bars("BTC/USDT", bars, dataset_id=dataset_id, dataset_version="fixture-v1")
            store.save_dataset_catalog(
                dataset_id,
                "fixture-v1",
                provider="fixture",
                instrument="BTC/USDT",
                market_type=MarketType.CRYPTO_SPOT,
                timeframe="1d",
                row_count=len(bars),
                completeness=1.0,
                quality="OHLCV",
                source_type="HISTORICAL",
                snapshot_id=f"{dataset_id}:fixture-v1",
            )

            report = run_crypto_research(
                symbol="BTC/USDT",
                store=store,
                dataset_id=dataset_id,
                dataset_version="fixture-v1",
                timeframe="1d",
                source_type="HISTORICAL",
                start=start,
                end=end,
            )

        self.assertEqual(report["bars"], 11)
        self.assertEqual(report["historical_coverage"]["start"], "2020-01-05T00:00:00+00:00")
        self.assertEqual(report["historical_coverage"]["end"], "2020-01-15T00:00:00+00:00")
        self.assertEqual(len(report["experiments"]), 8)



    def test_bound_reports_are_idempotently_persisted_and_network_data_is_not_a_dataset(self) -> None:
        bars_by_symbol = {
            "BTC/USDT": self._bars(rising=True),
            "ETH/USDT": self._bars(rising=False),
        }
        dataset_ids = {symbol: f"crypto:{symbol.replace('/', '')}" for symbol in bars_by_symbol}
        with AxiomStore(":memory:") as store:
            self._seed_universe(store, list(bars_by_symbol))
            for symbol, bars in bars_by_symbol.items():
                dataset_id = dataset_ids[symbol]
                store.save_dataset(
                    dataset_id,
                    "fixture-v1",
                    bars,
                    metadata={"instrument": symbol, "timeframe": "1h", "source_type": "HISTORICAL"},
                )
                store.save_bars(symbol, bars, dataset_id=dataset_id, dataset_version="fixture-v1")
                store.save_dataset_catalog(
                    dataset_id,
                    "fixture-v1",
                    provider="fixture",
                    instrument=symbol,
                    market_type=MarketType.CRYPTO_SPOT,
                    timeframe="1h",
                    row_count=len(bars),
                    completeness=1.0,
                    quality="OHLCV",
                    source_type="HISTORICAL",
                    snapshot_id=f"{dataset_id}:fixture-v1",
                )
            kwargs = {
                "symbols": list(bars_by_symbol),
                "store": store,
                "dataset_id": dataset_ids,
                "dataset_version": {symbol: "fixture-v1" for symbol in bars_by_symbol},
                "timeframe": {symbol: "1h" for symbol in bars_by_symbol},
                "source_type": {symbol: "HISTORICAL" for symbol in bars_by_symbol},
                "universe_provenance": {
                    "universe_id": "crypto-study",
                    "universe_version": "snapshot:7",
                    "selected_symbols": list(bars_by_symbol),
                },
            }
            first = run_multi_symbol_crypto_research(**kwargs)
            second = run_multi_symbol_crypto_research(**kwargs)

            self.assertEqual(first, second)
            self.assertEqual(len(store.list_reports()), 1)
            self.assertEqual(first["universe_provenance"]["selected_symbols"], list(bars_by_symbol))
            for symbol in bars_by_symbol:
                result = first["per_symbol"][symbol]
                self.assertEqual(result["dataset_id"], dataset_ids[symbol])
                self.assertEqual(result["dataset_version"], "fixture-v1")
                self.assertEqual(result["timeframe"], "1h")
                self.assertEqual(result["source_type"], "HISTORICAL")
                self.assertEqual(store.dataset_versions(dataset_ids[symbol]), ["fixture-v1"])
            network_provider = InMemoryCryptoProvider(
                {"BTCUSDT": bars_by_symbol["BTC/USDT"], "ETHUSDT": bars_by_symbol["ETH/USDT"]}
            )
            run_multi_symbol_crypto_research(
                symbols=list(bars_by_symbol),
                provider=network_provider,
                store=store,
                universe_provenance={
                    "universe_id": "crypto-study",
                    "universe_version": "snapshot:7",
                    "selected_symbols": list(bars_by_symbol),
                },
            )
            self.assertEqual(len(store.list_reports()), 2)
            for symbol, dataset_id in dataset_ids.items():
                self.assertEqual(store.dataset_versions(dataset_id), ["fixture-v1"])




if __name__ == "__main__":
    unittest.main()
