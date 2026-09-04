from __future__ import annotations

from datetime import datetime, timedelta, timezone
import unittest

from axiom.domain import OrderBookLevel, OrderBookSnapshot, PredictionMarketSnapshot, SettlementState
from axiom.node import NodeConfig, ResearchNode
from axiom.storage import AxiomStore


UTC = timezone.utc
T0 = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)


class _StepClock:
    def __init__(self, *values: datetime) -> None:
        self.values = tuple(values)
        self.index = 0

    def __call__(self) -> datetime:
        value = self.values[min(self.index, len(self.values) - 1)]
        self.index += 1
        return value


class _HealthProvider:
    provider_name = "health-fixture"

    def __init__(self, market_timestamp: datetime, book_timestamp: datetime) -> None:
        self.market_timestamp = market_timestamp
        self.book_timestamp = book_timestamp
        self.book_calls = 0

    def markets(self, active: bool = True, *, limit: int | None = None):
        return (self._market(),)

    def order_books(self, market_id: str, depth: int = 20):
        self.book_calls += 1
        return {"yes": self._book()}

    def _market(self) -> PredictionMarketSnapshot:
        return PredictionMarketSnapshot(
            timestamp=self.market_timestamp,
            market_id="health-market",
            question="Will the fixture resolve YES?",
            yes_bid=0.40,
            yes_ask=0.42,
            yes_mid=0.41,
            no_bid=0.58,
            no_ask=0.60,
            no_mid=0.59,
            volume=100.0,
            liquidity=20.0,
            expiry=T0 + timedelta(days=1),
            settlement=SettlementState.OPEN,
            resolution_criteria="fixture",
        )

    def _book(self) -> OrderBookSnapshot:
        return OrderBookSnapshot(
            timestamp=self.book_timestamp,
            bids=(OrderBookLevel(0.40, 10.0),),
            asks=(OrderBookLevel(0.42, 10.0),),
        )


class ScalabilityHealthTests(unittest.TestCase):
    def _run(
        self,
        provider: _HealthProvider,
        clock: _StepClock,
        *,
        skew: float = 5.0,
    ) -> AxiomStore:
        store = AxiomStore(":memory:")
        node = ResearchNode(
            NodeConfig(
                ":memory:",
                crypto_enabled=False,
                max_markets=1,
                max_provider_clock_skew_seconds=skew,
            ),
            provider=provider,
            opportunity_model={"health-market": 0.8},
            store=store,
            clock=clock,
            sleep=lambda _: None,
        )
        node._run_opportunity_pipeline()
        return store

    def test_provider_timestamps_within_request_or_skew_window_are_evidence(self) -> None:
        market_timestamp = T0 + timedelta(seconds=2)
        book_timestamp = T0 + timedelta(seconds=7)
        provider = _HealthProvider(market_timestamp, book_timestamp)
        store = self._run(
            provider,
            _StepClock(T0, T0, T0 + timedelta(seconds=3), T0 + timedelta(seconds=4), T0 + timedelta(seconds=5), T0 + timedelta(seconds=6), T0 + timedelta(seconds=8)),
        )
        try:
            rows = store.list_opportunity_snapshots(limit=10)
            self.assertTrue(rows)
            evidence = rows[0]["opportunity"]["evidence"]
            self.assertEqual(evidence["market"]["source_timestamp"], market_timestamp.isoformat())
            self.assertEqual(evidence["order_books"]["yes"]["source_timestamp"], book_timestamp.isoformat())
            self.assertEqual(evidence["provider_timestamp"], book_timestamp.isoformat())
            self.assertEqual(rows[0]["opportunity"]["source_timestamp"], book_timestamp.isoformat())
        finally:
            store.close()

    def test_truly_future_market_timestamp_is_rejected_with_exact_request_details(self) -> None:
        market_timestamp = T0 + timedelta(seconds=30)
        provider = _HealthProvider(market_timestamp, T0)
        store = self._run(provider, _StepClock(T0, T0, T0 + timedelta(seconds=1), T0 + timedelta(seconds=2), T0 + timedelta(seconds=3)), skew=5.0)
        try:
            self.assertEqual(store.list_opportunity_snapshots(limit=10), [])
            errors = store.list_collection_errors("health-market")
            self.assertEqual(len(errors), 1)
            error = errors[0]
            self.assertEqual(error["kind"], "future_observation")
            self.assertEqual(error["payload"]["source_timestamp"], market_timestamp.isoformat())
            self.assertEqual(error["payload"]["provider_timestamp"], market_timestamp.isoformat())
            self.assertEqual(error["payload"]["request_started_at"], T0.isoformat())
            self.assertEqual(error["payload"]["response_received_at"], (T0 + timedelta(seconds=1)).isoformat())
            self.assertEqual(error["payload"]["allowed_until"], (T0 + timedelta(seconds=6)).isoformat())
        finally:
            store.close()

    def test_worker_state_persists_latest_degrading_reason(self) -> None:
        provider = _HealthProvider(T0, T0 + timedelta(seconds=20))
        store = self._run(
            provider,
            _StepClock(T0, T0, T0 + timedelta(seconds=1), T0 + timedelta(seconds=2), T0 + timedelta(seconds=3), T0 + timedelta(seconds=4), T0 + timedelta(seconds=5)),
            skew=5.0,
        )
        try:
            state = next(row for row in store.list_worker_states(limit=20) if row["worker_name"] == "opportunity-pipeline")
            self.assertEqual(state["status"], "degraded")
            reason = "health-market yes: order-book timestamp is in the future"
            self.assertEqual(state["payload"]["degrading_reason"], reason)
            self.assertEqual(state["payload"]["last_degrading_reason"], reason)
            self.assertEqual(state["payload"]["last_error"], reason)
        finally:
            store.close()


if __name__ == "__main__":
    unittest.main()
