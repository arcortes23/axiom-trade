from __future__ import annotations

from datetime import datetime, timedelta, timezone
import unittest

from axiom.canary import CanaryService
from axiom.collector import CollectorConfig, PolymarketCollector
from axiom.data import InMemoryPredictionProvider
from axiom.director import validate_hermes_proposal
from axiom.domain import (
    InstrumentMetadata,
    MarketType,
    OrderBookLevel,
    OrderBookSnapshot,
    PredictionMarketSnapshot,
    SettlementState,
)
from axiom.experiment_plan import ExperimentPlan
from axiom.market_scope import MATCHED
from axiom.ranker import CandidateCanaryRanker
from axiom.research_bus import DurableResearchBus, ResearchQueueStatus
from axiom.storage import AxiomStore
from axiom.strategy.signals import CONSTANT_BASELINE, evaluate_model_document

from tests.test_phase4 import experiment_plan, prediction_rows, processor, proposal, relaxed_criteria


UTC = timezone.utc
T0 = datetime(2025, 1, 1, tzinfo=UTC)
DATASET_ID = "SYNTHETIC_OFFLINE-polymarket-history"
DATASET_VERSION = "synthetic-v1"
MARKET_ID = "SYNTHETIC_OFFLINE-future-politics-market"
CONDITION_ID = "SYNTHETIC_OFFLINE-future-politics-condition"
YES_TOKEN_ID = "SYNTHETIC_OFFLINE-future-politics-YES"
NO_TOKEN_ID = "SYNTHETIC_OFFLINE-future-politics-NO"
CANDIDATE_ID = "SYNTHETIC_OFFLINE-scope-candidate"
PROPOSAL_ID = "SYNTHETIC_OFFLINE-scope-proposal"


def _offline_market(stamp: datetime, yes_mid: float) -> tuple[PredictionMarketSnapshot, OrderBookSnapshot]:
    yes_bid = yes_mid - 0.01
    yes_ask = yes_mid + 0.01
    no_mid = 1.0 - yes_mid
    no_bid = no_mid - 0.01
    no_ask = no_mid + 0.01
    yes_book = OrderBookSnapshot(
        timestamp=stamp,
        bids=(OrderBookLevel(yes_bid, 100.0),),
        asks=(OrderBookLevel(yes_ask, 100.0),),
        token_id=YES_TOKEN_ID,
        condition_id=CONDITION_ID,
        provider_timestamp=stamp,
        source="SYNTHETIC_OFFLINE",
    )
    no_book = OrderBookSnapshot(
        timestamp=stamp,
        bids=(OrderBookLevel(no_bid, 100.0),),
        asks=(OrderBookLevel(no_ask, 100.0),),
        token_id=NO_TOKEN_ID,
        condition_id=CONDITION_ID,
        provider_timestamp=stamp,
        source="SYNTHETIC_OFFLINE",
    )
    market = PredictionMarketSnapshot(
        timestamp=stamp,
        market_id=MARKET_ID,
        question="Will the SYNTHETIC_OFFLINE public politics event resolve YES?",
        yes_bid=yes_bid,
        yes_ask=yes_ask,
        yes_mid=yes_mid,
        no_bid=no_bid,
        no_ask=no_ask,
        no_mid=no_mid,
        volume=10_000.0,
        liquidity=100.0,
        expiry=T0 + timedelta(days=30),
        settlement=SettlementState.OPEN,
        resolution_criteria="SYNTHETIC_OFFLINE: resolve from the public event result.",
        category="politics",
        tags=("politics", "SYNTHETIC_OFFLINE"),
        order_book=yes_book,
        source="SYNTHETIC_OFFLINE",
        yes_token_id=YES_TOKEN_ID,
        no_token_id=NO_TOKEN_ID,
        condition_id=CONDITION_ID,
        slug="synthetic-offline-future-politics",
        provider_timestamp=stamp,
        active=True,
        closed=False,
        accepting_orders=True,
        enable_order_book=True,
    )
    return market, no_book


class _SyntheticOfflinePublicProvider(InMemoryPredictionProvider):
    provider_name = "SYNTHETIC_OFFLINE"

    def __init__(self) -> None:
        market, no_book = _offline_market(T0, 0.50)
        super().__init__((market,))
        self._no_book = no_book
        self.markets_calls = 0
        self.market_calls = 0
        self.metadata_calls = 0
        self.order_books_calls = 0
        self.submission_attempts = 0

    def set_quote(self, stamp: datetime, yes_mid: float) -> None:
        market, no_book = _offline_market(stamp, yes_mid)
        self._markets[MARKET_ID] = market
        self._no_book = no_book

    def markets(self, active: bool = True):
        self.markets_calls += 1
        return super().markets(active=active)

    def market(self, market_id: str):
        self.market_calls += 1
        return super().market(market_id)

    def metadata(self, market_id: str):
        self.metadata_calls += 1
        snapshot = self.market(market_id)
        if snapshot is None:
            return None
        return InstrumentMetadata(
            symbol=MARKET_ID,
            market_type=MarketType.PREDICTION,
            provider=self.provider_name,
            market_id=MARKET_ID,
            condition_id=CONDITION_ID,
            question=snapshot.question,
            resolution_criteria=snapshot.resolution_criteria,
            category="politics",
            tags=("politics", "SYNTHETIC_OFFLINE"),
            expiry=snapshot.expiry,
            provider_timestamp=snapshot.provider_timestamp,
            active=True,
            closed=False,
            accepting_orders=True,
            enable_order_book=True,
            order_book_available=True,
            extra={
                "fixture_label": "SYNTHETIC_OFFLINE",
                "token_ids": {"yes": YES_TOKEN_ID, "no": NO_TOKEN_ID},
            },
        )

    def order_books(self, market_id: str, depth: int = 20):
        self.order_books_calls += 1
        snapshot = self.market(market_id)
        if snapshot is None or snapshot.order_book is None:
            return {}
        return {
            "yes": OrderBookSnapshot(
                snapshot.order_book.timestamp,
                snapshot.order_book.bids[:depth],
                snapshot.order_book.asks[:depth],
                token_id=YES_TOKEN_ID,
                condition_id=CONDITION_ID,
                provider_timestamp=snapshot.order_book.provider_timestamp,
                source=self.provider_name,
            ),
            "no": OrderBookSnapshot(
                self._no_book.timestamp,
                self._no_book.bids[:depth],
                self._no_book.asks[:depth],
                token_id=NO_TOKEN_ID,
                condition_id=CONDITION_ID,
                provider_timestamp=self._no_book.provider_timestamp,
                source=self.provider_name,
            ),
        }


class MarketScopeEndToEndTests(unittest.TestCase):
    def _canonical_plan(self) -> dict[str, object]:
        plan = experiment_plan(
            dataset_id=DATASET_ID,
            dataset_version=DATASET_VERSION,
            max_variants=1,
        )
        plan.update(
            {
                "target": {"instrument": "POLYMARKET", "categories": ["politics"]},
                "market_scope": {
                    "schema_version": "1",
                    "mode": "RULE_BASED_MARKETS",
                    "instrument": "POLYMARKET",
                    "categories": ["politics"],
                    "market_ids": [],
                    "filters": {"category": "politics"},
                    "regime_restrictions": {},
                    "provenance": "canonical",
                },
                "model_document": {"probability": 0.80},
            }
        )
        return plan

    @staticmethod
    def _seed_forward_metadata(store: AxiomStore) -> None:
        expiry = (T0 + timedelta(days=30)).isoformat()
        store.save_polymarket_market_metadata(
            MARKET_ID,
            {
                "source_type": "FORWARD_COLLECTED",
                "instrument": "POLYMARKET",
                "venue": "POLYMARKET",
                "market_type": "prediction",
                "active": True,
                "closed": False,
                "fixture_label": "SYNTHETIC_OFFLINE",
                "metadata": {
                    "instrument": "POLYMARKET",
                    "category": "politics",
                    "active": True,
                    "closed": False,
                    "fixture_label": "SYNTHETIC_OFFLINE",
                },
                "snapshot": {
                    "market_id": MARKET_ID,
                    "condition_id": CONDITION_ID,
                    "yes_token_id": YES_TOKEN_ID,
                    "no_token_id": NO_TOKEN_ID,
                    "token_ids": {"yes": YES_TOKEN_ID, "no": NO_TOKEN_ID},
                    "category": "politics",
                    "settlement": "open",
                    "expiry": expiry,
                    "active": True,
                    "closed": False,
                    "accepting_orders": True,
                    "enable_order_book": True,
                },
            },
            observed_at=T0,
            source_type="FORWARD_COLLECTED",
        )

    def test_synthetic_offline_scope_survives_queue_collection_qualification_and_evaluation(self) -> None:
        rows = [
            {
                **row,
                "fixture_label": "SYNTHETIC_OFFLINE",
                "instrument": "POLYMARKET",
                "dataset_version": DATASET_VERSION,
            }
            for row in prediction_rows(model_probability=0.80)
        ]
        plan_document = self._canonical_plan()
        plan = ExperimentPlan.from_mapping(plan_document, hypothesis_id=PROPOSAL_ID)
        self.assertEqual(plan.market_scope.mode, "RULE_BASED_MARKETS")
        self.assertEqual(plan.market_scope.instrument, "POLYMARKET")
        self.assertEqual(plan.market_scope.categories, ("politics",))
        self.assertEqual(plan.market_scope_hash, plan.market_scope.scope_hash)
        self.assertTrue(plan.plan_hash.startswith("sha256:"))
        proposal_document = proposal(
            PROPOSAL_ID,
            dataset_id=DATASET_ID,
            dataset_version=DATASET_VERSION,
            model_probability=0.80,
            max_variants=1,
        )
        proposal_document["source"] = "SYNTHETIC_OFFLINE"
        proposal_document["experiment_plan"] = plan_document
        validation = validate_hermes_proposal(proposal_document)
        self.assertTrue(validation.accepted, validation.reasons)
        normalized_plan = ExperimentPlan.from_proposal(validation.normalized or proposal_document)
        self.assertEqual(normalized_plan.plan_hash, plan.plan_hash)
        self.assertEqual(normalized_plan.market_scope_hash, plan.market_scope_hash)

        with AxiomStore(":memory:") as store:
            store.save_dataset(
                DATASET_ID,
                DATASET_VERSION,
                rows,
                metadata={
                    "fixture_label": "SYNTHETIC_OFFLINE",
                    "source_type": "HISTORICAL",
                    "provider": "SYNTHETIC_OFFLINE",
                },
                quality="PRICE_PROXY",
            )
            store.save_dataset_catalog(
                DATASET_ID,
                DATASET_VERSION,
                provider="SYNTHETIC_OFFLINE",
                instrument="POLYMARKET",
                market_type=MarketType.PREDICTION,
                timeframe="event",
                start_timestamp=T0,
                end_timestamp=T0 + timedelta(hours=19),
                row_count=len(rows),
                completeness=1.0,
                missing_ranges=(),
                quality="PRICE_PROXY",
                source_type="HISTORICAL",
                snapshot_id="SYNTHETIC_OFFLINE-historical-catalog-v1",
                metadata={
                    "fixture_label": "SYNTHETIC_OFFLINE",
                    "provider": "SYNTHETIC_OFFLINE",
                    "source_type": "HISTORICAL",
                    "research_quality": "PRICE_PROXY",
                    "historical_order_book_available": False,
                },
            )
            self.assertEqual(store.load_dataset(DATASET_ID, DATASET_VERSION), rows)
            catalog = store.load_dataset_catalog(DATASET_ID, DATASET_VERSION)
            self.assertIsNotNone(catalog)
            assert catalog is not None
            self.assertEqual(
                {
                    key: catalog[key]
                    for key in (
                        "dataset_id",
                        "dataset_version",
                        "provider",
                        "instrument",
                        "market_type",
                        "timeframe",
                        "start_timestamp",
                        "end_timestamp",
                        "row_count",
                        "completeness",
                        "missing_ranges",
                        "quality",
                        "source_type",
                        "snapshot_id",
                    )
                },
                {
                    "dataset_id": DATASET_ID,
                    "dataset_version": DATASET_VERSION,
                    "provider": "SYNTHETIC_OFFLINE",
                    "instrument": "POLYMARKET",
                    "market_type": "prediction",
                    "timeframe": "event",
                    "start_timestamp": T0,
                    "end_timestamp": T0 + timedelta(hours=19),
                    "row_count": len(rows),
                    "completeness": 1.0,
                    "missing_ranges": [],
                    "quality": "PRICE_PROXY",
                    "source_type": "HISTORICAL",
                    "snapshot_id": "SYNTHETIC_OFFLINE-historical-catalog-v1",
                },
            )
            self.assertEqual(catalog["row_count"], len(rows))
            self.assertEqual(catalog["source_type"], "HISTORICAL")
            self.assertEqual(catalog["snapshot_id"], "SYNTHETIC_OFFLINE-historical-catalog-v1")
            attestation = store.verify_dataset_integrity_attestation(
                DATASET_ID,
                DATASET_VERSION,
                force=True,
            )
            self.assertEqual(attestation["status"], "CURRENT")
            self.assertEqual(attestation["row_count"], len(rows))
            self.assertEqual(attestation["contamination_result"], "PASS")
            self.assertTrue(str(attestation["attestation_hash"]).startswith("sha256:"))

            self._seed_forward_metadata(store)
            bus = DurableResearchBus(store)
            queued_item = bus.submit_hypothesis(
                proposal_document,
                available_at=T0,
                dedupe_key=PROPOSAL_ID,
            )
            active = processor(
                store,
                bus,
                criteria=relaxed_criteria(),
            )
            queue_cycle = active.process_pending(now=T0)
            self.assertEqual(queue_cycle.completed, 1, repr(queue_cycle))
            self.assertEqual(queue_cycle.rejected, 0)
            queued = bus.get(queued_item.item_id)
            self.assertIsNotNone(queued)
            assert queued is not None
            self.assertEqual(queued.status, ResearchQueueStatus.COMPLETED)
            self.assertTrue(queued.result["accepted"])
            self.assertEqual(queued.result["plan_hash"], plan.plan_hash)
            self.assertEqual(queued.result["market_scope_hash"], plan.market_scope_hash)

            lifecycle = store.load_candidate_lifecycle(limit=None)
            self.assertEqual(len(lifecycle), 1)
            candidate = lifecycle[0]
            self.assertEqual(candidate["candidate_id"], queued.result["selected_candidate_ids"][0])
            self.assertEqual(candidate["stage"], "PAPER_FORWARD")
            self.assertEqual(candidate["payload"]["plan_hash"], plan.plan_hash)
            self.assertEqual(candidate["payload"]["market_scope_hash"], plan.market_scope_hash)
            self.assertEqual(candidate["payload"]["market_scope_version"], "1")
            self.assertEqual(candidate["payload"]["dataset_attestation"]["status"], "CURRENT")
            candidate_id = str(candidate["candidate_id"])

            provider = _SyntheticOfflinePublicProvider()
            collector = PolymarketCollector(
                provider,
                store,
                CollectorConfig(
                    collector_name="SYNTHETIC_OFFLINE-collector",
                    max_markets=4,
                    discovery_budget_per_cycle=4,
                    max_attempts=1,
                    backoff_initial_seconds=0.0,
                    jitter_seconds=0.0,
                    freshness_sla_seconds=60.0,
                ),
                clock=lambda: T0,
                sleep=lambda _seconds: None,
            )
            first_collection = collector.collect_once(now=T0)
            self.assertEqual(first_collection.errors, 0)
            self.assertEqual(first_collection.candidate_bound_scheduled, (MARKET_ID,))
            self.assertIn(MARKET_ID, first_collection.candidate_bound_fresh)
            self.assertEqual(first_collection.discovery_scheduled, ())
            self.assertEqual(provider.markets_calls, 1)
            self.assertGreaterEqual(provider.order_books_calls, 1)

            resolution = store.load_market_scope_resolution(candidate_id)
            self.assertIsNotNone(resolution)
            assert resolution is not None
            self.assertEqual(resolution.status, MATCHED)
            self.assertEqual(resolution.scope_hash, plan.market_scope_hash)
            self.assertEqual(resolution.scope_version, "1")
            self.assertEqual(len(resolution.matched_markets), 1)
            matched = resolution.matched_markets[0]
            self.assertEqual(matched.market_id, MARKET_ID)
            self.assertEqual(matched.condition_id, CONDITION_ID)
            self.assertEqual(matched.yes_token_id, YES_TOKEN_ID)
            self.assertEqual(matched.no_token_id, NO_TOKEN_ID)
            self.assertEqual(resolution.provenance["resolution_source"], "persisted_current_markets")

            snapshots = store.load_polymarket_snapshots(
                MARKET_ID,
                source_type="FORWARD_COLLECTED",
                latest=True,
                limit=4,
            )
            self.assertEqual(len(snapshots), 1)
            first_snapshot = snapshots[0]
            self.assertEqual(first_snapshot["source_timestamp"], T0)
            self.assertEqual(first_snapshot["observed_at"], T0)
            self.assertEqual(first_snapshot["source_type"], "FORWARD_COLLECTED")
            first_payload = first_snapshot["payload"]
            self.assertEqual(first_payload["snapshot"]["yes_mid"], 0.50)
            self.assertEqual(first_payload["yes_token_id"], YES_TOKEN_ID)
            self.assertEqual(first_payload["no_token_id"], NO_TOKEN_ID)
            self.assertEqual(first_payload["yes_order_book"]["timestamp"], T0.isoformat())
            self.assertEqual(first_payload["no_order_book"]["timestamp"], T0.isoformat())
            self.assertEqual(first_payload["research_quality"], "ORDER_BOOK_SIMULATED")

            now = [T0]
            service = CanaryService(store, clock=lambda: now[0])
            ranker = CandidateCanaryRanker(store, service=service, clock=lambda: now[0])
            ranking = ranker.evaluate_and_select(T0)
            qualification = service.validate_eligibility(candidate_id)
            self.assertTrue(qualification["eligible"], qualification)
            self.assertEqual(
                qualification["qualification"]["market_scope_hash"],
                plan.market_scope_hash,
            )
            self.assertEqual(candidate["payload"]["experiment_plan"]["model_document"], {"probability": 0.80})
            model_evaluation = evaluate_model_document(plan.model_for(), {})
            self.assertEqual(model_evaluation.probability, 0.80)
            self.assertEqual(model_evaluation.evidence["model_source"], CONSTANT_BASELINE)

            ready = service.evaluate_signal(candidate_id, cycle_id="SYNTHETIC_OFFLINE-ready")
            self.assertEqual(ready["reason_code"], "READY_SIGNAL")
            self.assertEqual(ready["market_id"], MARKET_ID)
            self.assertEqual(ready["signal"]["status"], "READY")
            self.assertEqual(ready["signal"]["outcome"], "yes")
            self.assertEqual(ready["signal"]["token_id"], YES_TOKEN_ID)
            self.assertEqual(ready["signal"]["evidence"]["model_probability"], 0.80)
            self.assertEqual(ready["evidence"]["current_execution_evidence"], "CURRENT_ORDER_BOOK")
            self.assertEqual(ready["evidence"]["scope_hash"], plan.market_scope_hash)
            self.assertEqual(ready["signal"]["evidence"]["scope_resolution_id"], resolution.resolution_id)
            self.assertEqual(ready["signal"]["evidence"]["market_scope_hash"], plan.market_scope_hash)
            self.assertEqual(ready["signal"]["evidence"]["market_scope_version"], "1")
            now[0] = T0 + timedelta(seconds=61)
            provider.set_quote(now[0], 0.80)
            collector.clock = lambda: now[0]
            second_collection = collector.collect_once(now=now[0])
            self.assertEqual(second_collection.errors, 0)
            self.assertIn(MARKET_ID, second_collection.candidate_bound_fresh)
            latest_resolution = store.load_market_scope_resolution(candidate_id)
            self.assertIsNotNone(latest_resolution)
            assert latest_resolution is not None
            self.assertEqual(latest_resolution.status, MATCHED)
            self.assertEqual(latest_resolution.scope_hash, plan.market_scope_hash)
            self.assertEqual(latest_resolution.matched_markets[0].yes_token_id, YES_TOKEN_ID)
            self.assertEqual(latest_resolution.matched_markets[0].no_token_id, NO_TOKEN_ID)

            latest_snapshots = store.load_polymarket_snapshots(
                MARKET_ID,
                source_type="FORWARD_COLLECTED",
                latest=False,
                limit=4,
            )
            self.assertEqual(len(latest_snapshots), 2)
            self.assertEqual(latest_snapshots[0]["source_timestamp"], T0)
            newest = latest_snapshots[-1]
            self.assertEqual(newest["source_timestamp"], now[0])
            self.assertEqual(newest["observed_at"], now[0])
            self.assertEqual(newest["payload"]["snapshot"]["yes_mid"], 0.80)
            self.assertEqual(newest["payload"]["yes_token_id"], YES_TOKEN_ID)
            self.assertEqual(newest["payload"]["no_token_id"], NO_TOKEN_ID)
            self.assertEqual(newest["payload"]["yes_order_book"]["timestamp"], now[0].isoformat())
            self.assertEqual(newest["payload"]["no_order_book"]["timestamp"], now[0].isoformat())

            declined = service.evaluate_signal(candidate_id, cycle_id="SYNTHETIC_OFFLINE-declined")
            self.assertEqual(declined["reason_code"], "STRATEGY_EVALUATED_DECLINED")
            self.assertEqual(declined["market_id"], MARKET_ID)
            self.assertIsNone(declined["signal"])
            self.assertEqual(declined["evidence"]["scope_hash"], plan.market_scope_hash)
            self.assertEqual(declined["evidence"]["scope_version"], "1")
            self.assertEqual(declined["evidence"]["resolved_market_ids"], [MARKET_ID])

            evaluations = service.list_signal_evaluations(candidate_id=candidate_id, limit=10)
            self.assertEqual(
                {row["cycle_id"] for row in evaluations},
                {"SYNTHETIC_OFFLINE-ready", "SYNTHETIC_OFFLINE-declined"},
            )
            self.assertEqual(
                {row["reason_code"] for row in evaluations},
                {"READY_SIGNAL", "STRATEGY_EVALUATED_DECLINED"},
            )
            self.assertEqual(provider.submission_attempts, 0)
            self.assertEqual(
                store.connection.execute("SELECT COUNT(*) FROM canary_ledger").fetchone()[0],
                0,
            )
            self.assertEqual(
                store.connection.execute("SELECT COUNT(*) FROM canary_execution_events").fetchone()[0],
                0,
            )
            self.assertFalse(store.list_collection_errors())


if __name__ == "__main__":
    unittest.main()
