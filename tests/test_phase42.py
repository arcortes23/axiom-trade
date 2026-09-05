from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import math
from pathlib import Path
import tempfile
import unittest
from urllib.request import urlopen

from axiom.bootstrap import (
    BTC_DATASET_IDS,
    HistoricalBootstrapper,
    classify_market_category,
    label_btc_regimes,
    run_btc_historical_research,
)
from axiom.dashboard import DashboardData, DashboardServer, _dashboard_html
from axiom.domain import InstrumentMetadata, MarketType, OHLCVBar, PredictionMarketSnapshot, SettlementState, to_record
from axiom.evaluation import dataset_version
from axiom.storage import AxiomStore


UTC = timezone.utc
T0 = datetime(2020, 1, 1, tzinfo=UTC)


def btc_bars(start: datetime, count: int) -> list[OHLCVBar]:
    result: list[OHLCVBar] = []
    for index in range(count):
        close = 100.0 + index * 0.25 + 3.0 * math.sin(index / 11.0)
        result.append(
            OHLCVBar(
                start + timedelta(days=index),
                close,
                close + 1.0,
                close - 1.0,
                close,
                1_000.0 + index,
            )
        )
    return result


class FakeBinance:
    provider_name = "fake-binance"
    base_url = "https://fixture.invalid"

    def __init__(self, bars: list[OHLCVBar], *, failures: int = 0) -> None:
        self.bars = bars
        self.failures = failures
        self.calls: list[tuple[datetime, datetime, str]] = []

    def historical_ohlcv(self, symbol: str, *, start: datetime, end: datetime, interval: str) -> list[OHLCVBar]:
        self.calls.append((start, end, interval))
        if self.failures:
            self.failures -= 1
            raise RuntimeError("temporary fixture failure")
        return [bar for bar in self.bars if start <= bar.timestamp <= end]


class FakePolymarket:
    provider_name = "fake-polymarket"

    def __init__(self) -> None:
        self.base = T0
        self.markets_by_id = {
            "m-politics": self._market("m-politics", "Will the election result be certified?", tags=("election",)),
            "m-crypto": self._market("m-crypto", "Will Bitcoin exceed the target?", tags=("crypto",)),
        }

    @staticmethod
    def _market(market_id: str, question: str, *, tags: tuple[str, ...]) -> PredictionMarketSnapshot:
        return PredictionMarketSnapshot(
            timestamp=T0,
            market_id=market_id,
            question=question,
            yes_bid=0.44,
            yes_ask=0.46,
            yes_mid=0.45,
            no_bid=0.54,
            no_ask=0.56,
            no_mid=0.55,
            volume=1000.0,
            liquidity=250.0,
            expiry=T0 + timedelta(days=30),
            settlement=SettlementState.RESOLVED_YES,
            resolution_criteria="official public result",
            tags=tags,
            yes_token_id=f"{market_id}-yes",
            no_token_id=f"{market_id}-no",
        )

    def markets(self, active: bool = True, *, limit: int | None = None) -> list[PredictionMarketSnapshot]:
        values = list(self.markets_by_id.values())
        return values if limit is None else values[:limit]

    def market(self, market_id: str) -> PredictionMarketSnapshot | None:
        return self.markets_by_id.get(market_id)

    def metadata(self, market_id: str) -> InstrumentMetadata | None:
        market = self.market(market_id)
        if market is None:
            return None
        return InstrumentMetadata(
            symbol=market.market_id,
            market_type=MarketType.PREDICTION,
            provider=self.provider_name,
            market_id=market.market_id,
            question=market.question,
            resolution_criteria=market.resolution_criteria,
            tags=market.tags,
            extra={"yes_token_id": market.yes_token_id, "no_token_id": market.no_token_id},
        )

    def price_history(self, market_id: str) -> list[dict[str, object]]:
        return [
            {"timestamp": self.base, "price": 0.40, "token_id": f"{market_id}-yes"},
            {"timestamp": self.base + timedelta(days=1), "price": 0.60, "token_id": f"{market_id}-yes"},
        ]

    def consume_transport_errors(self) -> tuple[object, ...]:
        return ()


class Phase42HistoricalTests(unittest.TestCase):
    def test_btc_bootstrap_retries_incrementally_and_catalogs_immutable_versions(self) -> None:
        bars = btc_bars(T0, 5)
        provider = FakeBinance(bars, failures=1)
        with AxiomStore(":memory:") as store:
            bootstrapper = HistoricalBootstrapper(store, crypto_provider=provider, sleep=lambda _: None, max_attempts=2, backoff=0)
            first = bootstrapper.bootstrap_crypto(intervals=("1d",), start=T0, end=T0 + timedelta(days=2))
            self.assertEqual(first[0].status, "COMPLETE")
            self.assertEqual(first[0].retries, 1)
            dataset_id = BTC_DATASET_IDS["1d"]
            catalog = store.load_dataset_catalog(dataset_id)
            self.assertIsNotNone(catalog)
            assert catalog is not None
            self.assertEqual(catalog["source_type"], "HISTORICAL")
            self.assertEqual(catalog["row_count"], 3)
            self.assertEqual(catalog["completeness"], 1.0)
            first_version = catalog["dataset_version"]

            second = bootstrapper.bootstrap_crypto(intervals=("1d",), start=T0, end=T0 + timedelta(days=4))
            self.assertEqual(second[0].status, "COMPLETE")
            versions = store.dataset_versions(dataset_id)
            self.assertGreaterEqual(len(versions), 2)
            self.assertIn(first_version, versions)
            latest = store.load_dataset_catalog(dataset_id)
            self.assertIsNotNone(latest)
            assert latest is not None
            self.assertEqual(latest["row_count"], 5)
            self.assertEqual(len(store.load_dataset(dataset_id)), 5)
            self.assertTrue(all(call[2] == "1d" for call in provider.calls))

    def test_btc_bootstrap_keeps_failed_cursor_resumable(self) -> None:
        provider = FakeBinance(btc_bars(T0, 3), failures=10)
        with AxiomStore(":memory:") as store:
            bootstrapper = HistoricalBootstrapper(store, crypto_provider=provider, sleep=lambda _: None, max_attempts=1)
            failed = bootstrapper.bootstrap_crypto(intervals=("1d",), start=T0, end=T0 + timedelta(days=2))
            self.assertEqual(failed[0].status, "PARTIAL")
            state = store.load_dataset_bootstrap_state(BTC_DATASET_IDS["1d"])
            self.assertIsNotNone(state)
            assert state is not None
            self.assertEqual(state["next_timestamp"], T0)
            provider.failures = 0
            resumed = bootstrapper.bootstrap_crypto(intervals=("1d",), start=T0, end=T0 + timedelta(days=2), resume=True)
            self.assertEqual(resumed[0].status, "COMPLETE")
            self.assertEqual(resumed[0].records, 3)

    def test_catalog_source_types_are_explicitly_separated(self) -> None:
        with AxiomStore(":memory:") as store:
            store.save_dataset_catalog(
                "historical-btc",
                "hist-v1",
                provider="binance",
                instrument="BTCUSDT",
                market_type=MarketType.CRYPTO_SPOT,
                timeframe="1d",
                row_count=1,
                completeness=1.0,
                quality="OHLCV",
                source_type="HISTORICAL",
                snapshot_id="historical-btc:hist-v1",
            )
            store.save_dataset_catalog(
                "forward-pm",
                "cycle-v1",
                provider="polymarket",
                instrument="POLYMARKET",
                market_type=MarketType.PREDICTION,
                timeframe="live",
                row_count=1,
                completeness=1.0,
                quality="ORDER_BOOK_SIMULATED",
                source_type="FORWARD_COLLECTED",
                snapshot_id="cycle-v1",
            )
            self.assertEqual([item["source_type"] for item in store.list_dataset_catalog(source_type="HISTORICAL")], ["HISTORICAL"])
            self.assertEqual([item["source_type"] for item in store.list_dataset_catalog(source_type="FORWARD_COLLECTED")], ["FORWARD_COLLECTED"])

    def test_regime_labels_are_overlapping_and_include_low_volatility(self) -> None:
        labels = label_btc_regimes(btc_bars(T0, 80))
        self.assertEqual(len(labels), 80)
        self.assertTrue(any("LOW_VOLATILITY" in item["labels"] for item in labels))
        self.assertTrue(any("SIDEWAYS" in item["labels"] for item in labels))
        self.assertTrue(any(len(item["labels"]) > 1 for item in labels))

    def test_btc_walk_forward_report_uses_catalog_only_and_persists_labels(self) -> None:
        bars = btc_bars(T0, 4 * 365 + 5)
        version = dataset_version([to_record(bar) for bar in bars])
        with AxiomStore(":memory:") as store:
            store.save_bars("BTCUSDT", bars, dataset_id=BTC_DATASET_IDS["1d"], dataset_version=version)
            store.save_dataset_catalog(
                BTC_DATASET_IDS["1d"],
                version,
                provider="binance",
                instrument="BTCUSDT",
                market_type=MarketType.CRYPTO_SPOT,
                timeframe="1d",
                start_timestamp=bars[0].timestamp,
                end_timestamp=bars[-1].timestamp,
                row_count=len(bars),
                completeness=1.0,
                quality="OHLCV",
                source_type="HISTORICAL",
                snapshot_id=f"{BTC_DATASET_IDS['1d']}:{version}",
            )
            report = run_btc_historical_research(store, train_years=1, validation_years=1, holdout_years=1, step_years=1)
            self.assertEqual(report["source_type"], "HISTORICAL")
            self.assertGreater(report["walk_forward"]["windows"], 0)
            self.assertEqual(len(store.load_historical_regime_labels(BTC_DATASET_IDS["1d"], version)), len(bars))
            self.assertEqual(len(report["experiments"]), 8)
            metric_names = {"total_return", "max_drawdown", "expectancy", "sharpe", "sortino", "turnover", "fees", "slippage"}
            self.assertTrue(metric_names.issubset(report["experiments"][0]["walk_forward"][0]["metrics"]))
            self.assertIn("parameter_stability", report["experiments"][0])
            self.assertIsNotNone(store.load_report("btc-historical-walk-forward:" + BTC_DATASET_IDS["1d"] + ":" + version))


class Phase42PolymarketTests(unittest.TestCase):
    def test_polymarket_bootstrap_preserves_price_proxy_and_resumes_aggregate(self) -> None:
        provider = FakePolymarket()
        with AxiomStore(":memory:") as store:
            bootstrapper = HistoricalBootstrapper(store, prediction_provider=provider, sleep=lambda _: None, max_attempts=1)
            first = bootstrapper.bootstrap_polymarket(max_markets=2)
            self.assertEqual(first.status, "COMPLETE")
            self.assertEqual(first.records, 4)
            self.assertEqual(first.metadata["research_quality"], "PRICE_PROXY")
            catalog = store.load_dataset_catalog("prediction:m-crypto")
            self.assertIsNotNone(catalog)
            assert catalog is not None
            self.assertEqual(catalog["quality"], "PRICE_PROXY")
            records = store.load_dataset("prediction:m-crypto")
            self.assertEqual(len(records), 2)
            self.assertTrue(all(item["order_book"] is None for item in records))
            self.assertTrue(all(item["executable_quote"] is False for item in records))
            self.assertEqual(classify_market_category(provider.markets_by_id["m-politics"]), "politics")

            second = bootstrapper.bootstrap_polymarket(max_markets=2, resume=True)
            self.assertEqual(second.status, "COMPLETE")
            self.assertEqual(second.records, 4)
            aggregate = store.load_dataset_catalog("Polymarket-historical")
            self.assertIsNotNone(aggregate)
            assert aggregate is not None
            self.assertEqual(aggregate["row_count"], 4)


class Phase42DashboardTests(unittest.TestCase):
    def test_dashboard_empty_state_and_dynamic_json_api(self) -> None:
        html = _dashboard_html()
        for text in ("Live trading", "Paper risk engine", "Historical / forward coverage", "Candidate lifecycle funnel", "setInterval(load, 10000)", "Research maturity", "Paper forward", "Research queue and node status"):
            self.assertIn(text, html)
        with AxiomStore(":memory:") as store:
            data = DashboardData(store=store)
            operator = data.operator_data()
            self.assertFalse(operator["live_trading"]["enabled"])
            self.assertTrue(operator["paper_risk_engine"]["enabled"])
            self.assertTrue(all(item["state"] == "NOT INITIALIZED" for item in operator["components"]))
            server = DashboardServer(port=0, data=data).start()
            try:
                assert server.url is not None
                with urlopen(server.url + "/api/operator", timeout=3) as response:
                    payload = json.loads(response.read().decode("utf-8"))
                self.assertFalse(payload["live_execution"])
                self.assertEqual(payload["coverage"]["historical_count"], 0)
                self.assertEqual(payload["coverage"]["forward_count"], 0)
            finally:
                server.stop()


    def test_operator_exposes_current_health_and_exact_collector_reason(self) -> None:
        health = {
            "grade": "C",
            "grade_scope": "collector_health",
            "reason_code": "STALE_MARKETS",
            "reasons": [{"code": "STALE_MARKETS", "reason": "Two active markets missed the collection threshold."}],
            "window_start": "2026-01-02T11:00:00+00:00",
            "window_end": "2026-01-02T12:00:00+00:00",
            "historical_maturity_grade": "B",
            "historical_error_count": 3,
        }
        operator = DashboardData(data={"dataset-health": health}).operator_data()
        self.assertEqual(operator["dataset_health"]["grade"], "C")
        self.assertEqual(operator["health_grade"], "C")
        self.assertEqual(operator["grade_scope"], "collector_health")
        self.assertEqual(operator["reason_code"], "STALE_MARKETS")
        self.assertEqual(operator["reasons"], health["reasons"])
        self.assertEqual(operator["window_start"], health["window_start"])
        self.assertEqual(operator["window_end"], health["window_end"])
        self.assertEqual(operator["source_type"], "FORWARD_COLLECTED")
        self.assertEqual(operator["historical_maturity_grade"], "B")
        self.assertEqual(operator["historical_error_count"], 3)
        collector = operator["components"][1]["detail"]
        self.assertEqual(collector["reason"], health["reasons"][0]["reason"])
        self.assertEqual(collector["reason_code"], "STALE_MARKETS")
        self.assertEqual(collector["reasons"], health["reasons"])
        self.assertEqual(collector["source_type"], "FORWARD_COLLECTED")

    def test_dashboard_populated_state_exposes_coverage_candidates_and_detail(self) -> None:
        with AxiomStore(":memory:") as store:
            store.save_dataset_catalog(
                "hist-btc",
                "hist-v1",
                provider="binance",
                instrument="BTCUSDT",
                market_type=MarketType.CRYPTO_SPOT,
                timeframe="1d",
                row_count=12,
                completeness=1.0,
                quality="OHLCV",
                source_type="HISTORICAL",
                snapshot_id="hist-v1",
            )
            store.save_dataset_catalog(
                "fwd-pm",
                "cycle-v1",
                provider="polymarket",
                instrument="POLYMARKET",
                market_type=MarketType.PREDICTION,
                timeframe="live",
                row_count=2,
                completeness=1.0,
                quality="ORDER_BOOK_SIMULATED",
                source_type="FORWARD_COLLECTED",
                snapshot_id="cycle-v1",
            )
            forward_now = datetime.now(UTC)
            store.save_polymarket_snapshot(
                "fwd-pm:market-1:1",
                "market-1",
                forward_now,
                forward_now,
                {
                    "snapshot": {
                        "market_id": "market-1",
                        "question": "Will the fixture resolve YES?",
                        "settlement": "open",
                        "yes_mid": 0.45,
                        "liquidity": 100.0,
                    },
                    "source_type": "FORWARD_COLLECTED",
                },
                quality="ORDER_BOOK_SIMULATED",
                source_type="FORWARD_COLLECTED",
            )
            store.save_polymarket_market_metadata(
                "market-1",
                {
                    "market_id": "market-1",
                    "question": "Will the fixture resolve YES?",
                    "active": True,
                    "closed": False,
                    "snapshot": {"settlement": "open", "expiry": (forward_now + timedelta(days=1)).isoformat()},
                },
                observed_at=forward_now,
                source_type="FORWARD_COLLECTED",
            )
            
            store.save_candidate_lifecycle(
                "candidate-1",
                "IDEA",
                {
                    "strategy_id": "btc-trend",
                    "experiment_family": "trend",
                    "market_type": "crypto_spot",
                    "hypothesis": "A bounded trend signal is testable.",
                    "generation": 1,
                },
            )
            data = DashboardData(store=store)
            operator = data.operator_data()
            self.assertEqual(operator["coverage"]["historical_count"], 1)
            self.assertEqual(operator["coverage"]["forward_count"], 1)
            self.assertEqual(operator["components"][2]["state"], "READY")
            self.assertEqual(operator["components"][1]["state"], "READY")
            self.assertEqual(operator["candidates"][0]["strategy_id"], "btc-trend")
            detail = data.strategy_detail("candidate-1")
            self.assertTrue(detail["available"])
            self.assertEqual(detail["strategy"]["family"], "trend")

    def test_dashboard_historical_only_state_does_not_claim_forward_health(self) -> None:
        with AxiomStore(":memory:") as store:
            observed_at = datetime.now(UTC)
            store.save_polymarket_snapshot(
                "hist-pm:market-1:1",
                "market-1",
                observed_at,
                observed_at,
                {
                    "snapshot": {
                        "market_id": "market-1",
                        "question": "Historical-only fixture",
                        "settlement": "open",
                        "yes_mid": 0.45,
                    },
                    "source_type": "HISTORICAL",
                },
                quality="PRICE_PROXY",
                source_type="HISTORICAL",
            )
            data = DashboardData(store=store)
            health = data.dataset_health()
            self.assertEqual(health["grade"], "F")
            self.assertEqual(health["reason_code"], "NO_FORWARD_SNAPSHOTS")
            self.assertEqual(data.operator_data()["components"][1]["state"], "NOT INITIALIZED")
if __name__ == "__main__":
    unittest.main()
