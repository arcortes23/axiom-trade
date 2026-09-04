from __future__ import annotations

from datetime import datetime, timedelta, timezone
import unittest

from axiom.data import InMemoryCryptoProvider
from axiom.domain import OHLCVBar
from axiom.research import run_multi_symbol_crypto_research


class ScalabilityCryptoResearchTests(unittest.TestCase):
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
        report = run_multi_symbol_crypto_research(
            symbols=["BTC/USDT", "ETH/USDT"],
            providers={"BTC/USDT": provider, "ETH/USDT": provider},
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
        report = run_multi_symbol_crypto_research(
            ["BTC/USDT", "ETH/USDT", "BAD/USDT"],
            {"BTC/USDT": provider, "ETH/USDT": provider},
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


if __name__ == "__main__":
    unittest.main()
