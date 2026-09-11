from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
import unittest

from axiom.autonomous import AutonomousResearchConfig, AutonomousResearchProcessor
from axiom.forward import ForwardTestRegistry
from axiom.lifecycle import CandidateStage, PromotionCriteria
from axiom.paper_engine import run_forward_paper
from axiom.canary import (
    CanaryService,
    EXECUTION_FEASIBILITY_MARKET_CAP,
)
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
from axiom.experiment_plan import ExperimentPlan, normalize_market_scope
from axiom.market_scope import MATCHED
from axiom.ranker import CandidateCanaryRanker
from axiom.research_bus import DurableResearchBus, ResearchQueueStatus
from axiom.storage import AxiomStore
from axiom.strategy.signals import CONSTANT_BASELINE, evaluate_model_document

from tests.test_phase4 import experiment_plan, prediction_rows, processor, proposal, relaxed_criteria
from tests.test_polymarket_research_orchestration import (
    _legacy_prediction_predecessor,
    _seed_predeclared_historical_dataset,
)



UTC = timezone.utc
T0 = datetime(2025, 1, 1, tzinfo=UTC)
SYNTHETIC_MODEL_PROBABILITY = 0.80
SYNTHETIC_MODEL_LABEL = "synthetic-only"
SYNTHETIC_MARKET_COUNT = 30
DATASET_ID = "SYNTHETIC_OFFLINE-polymarket-history"
DATASET_VERSION = "synthetic-v1"
MARKET_ID = "SYNTHETIC_OFFLINE-future-politics-market"
CONDITION_ID = "SYNTHETIC_OFFLINE-future-politics-condition"
YES_TOKEN_ID = "SYNTHETIC_OFFLINE-future-politics-YES"
NO_TOKEN_ID = "SYNTHETIC_OFFLINE-future-politics-NO"
CANDIDATE_ID = "SYNTHETIC_OFFLINE-scope-candidate"
PROPOSAL_ID = "SYNTHETIC_OFFLINE-scope-proposal"


def _offline_market(
    stamp: datetime,
    yes_mid: float,
    *,
    market_id: str = MARKET_ID,
    condition_id: str = CONDITION_ID,
    yes_token_id: str = YES_TOKEN_ID,
    no_token_id: str = NO_TOKEN_ID,
) -> tuple[PredictionMarketSnapshot, OrderBookSnapshot]:
    yes_bid = yes_mid - 0.01
    yes_ask = yes_mid + 0.01
    no_mid = 1.0 - yes_mid
    no_bid = no_mid - 0.01
    no_ask = no_mid + 0.01
    yes_book = OrderBookSnapshot(
        timestamp=stamp,
        bids=(OrderBookLevel(yes_bid, 100.0),),
        asks=(OrderBookLevel(yes_ask, 100.0),),
        token_id=yes_token_id,
        condition_id=condition_id,
        provider_timestamp=stamp,
        source="SYNTHETIC_OFFLINE",
    )
    no_book = OrderBookSnapshot(
        timestamp=stamp,
        bids=(OrderBookLevel(no_bid, 100.0),),
        asks=(OrderBookLevel(no_ask, 100.0),),
        token_id=no_token_id,
        condition_id=condition_id,
        provider_timestamp=stamp,
        source="SYNTHETIC_OFFLINE",
    )
    default_fixture = market_id == MARKET_ID
    question = (
        "Will the SYNTHETIC_OFFLINE public politics event resolve YES?"
        if default_fixture
        else f"Will the {market_id} public politics event resolve YES?"
    )
    resolution_criteria = (
        "SYNTHETIC_OFFLINE: resolve from the public event result."
        if default_fixture
        else f"{market_id}: resolve from the public event result."
    )
    slug = "synthetic-offline-future-politics" if default_fixture else market_id.lower()
    market = PredictionMarketSnapshot(
        timestamp=stamp,
        market_id=market_id,
        question=question,
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
        resolution_criteria=resolution_criteria,
        category="politics",
        tags=("politics", "SYNTHETIC_OFFLINE"),
        order_book=yes_book,
        source="SYNTHETIC_OFFLINE",
        yes_token_id=yes_token_id,
        no_token_id=no_token_id,
        condition_id=condition_id,
        slug=slug,
        provider_timestamp=stamp,
        active=True,
        closed=False,
        accepting_orders=True,
        enable_order_book=True,
    )
    return market, no_book


class _SyntheticOfflinePublicProvider(InMemoryPredictionProvider):
    provider_name = "SYNTHETIC_OFFLINE"

    def __init__(self, market_ids: tuple[str, ...] | None = None, *, stamp: datetime = T0) -> None:
        configured_ids = tuple(market_ids or (MARKET_ID,))
        entries: list[PredictionMarketSnapshot] = []
        self._no_books: dict[str, OrderBookSnapshot] = {}
        for market_id in configured_ids:
            condition_id = CONDITION_ID if market_id == MARKET_ID else f"{market_id}-condition"
            yes_token_id = YES_TOKEN_ID if market_id == MARKET_ID else f"{market_id}-yes"
            no_token_id = NO_TOKEN_ID if market_id == MARKET_ID else f"{market_id}-no"
            market, no_book = _offline_market(
                stamp,
                0.50,
                market_id=market_id,
                condition_id=condition_id,
                yes_token_id=yes_token_id,
                no_token_id=no_token_id,
            )
            entries.append(market)
            self._no_books[market_id] = no_book
        super().__init__(tuple(entries))
        self.markets_calls = 0
        self.market_calls = 0
        self.metadata_calls = 0
        self.order_books_calls = 0
        self.submission_attempts = 0

    def set_quote(self, stamp: datetime, yes_mid: float) -> None:
        market, no_book = _offline_market(stamp, yes_mid)
        self._markets[MARKET_ID] = market
        self._no_books[MARKET_ID] = no_book

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
            symbol=snapshot.market_id,
            market_type=MarketType.PREDICTION,
            provider=self.provider_name,
            market_id=snapshot.market_id,
            condition_id=snapshot.condition_id,
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
                "token_ids": {"yes": snapshot.yes_token_id, "no": snapshot.no_token_id},
            },
        )

    def order_books(self, market_id: str, depth: int = 20):
        self.order_books_calls += 1
        snapshot = self.market(market_id)
        no_book = self._no_books.get(str(market_id))
        if snapshot is None or snapshot.order_book is None or no_book is None:
            return {}
        return {
            "yes": OrderBookSnapshot(
                snapshot.order_book.timestamp,
                snapshot.order_book.bids[:depth],
                snapshot.order_book.asks[:depth],
                token_id=snapshot.yes_token_id,
                condition_id=snapshot.condition_id,
                provider_timestamp=snapshot.order_book.provider_timestamp,
                source=self.provider_name,
            ),
            "no": OrderBookSnapshot(
                no_book.timestamp,
                no_book.bids[:depth],
                no_book.asks[:depth],
                token_id=snapshot.no_token_id,
                condition_id=snapshot.condition_id,
                provider_timestamp=no_book.provider_timestamp,
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
    def _seed_predeclared_history(store: AxiomStore, *, attested: bool = True) -> None:
        rows = [
            {
                "timestamp": (T0 + timedelta(minutes=index)).isoformat(),
                "market_id": f"seed-market-{index % 64:02d}",
                "yes_mid": 0.50,
                "source_type": "HISTORICAL",
            }
            for index in range(4096)
        ]
        store.save_dataset(
            "Polymarket-historical",
            "e2e-4096-v1",
            rows,
            metadata={
                "source_type": "HISTORICAL",
                "provider": "SYNTHETIC_OFFLINE",
                "research_quality": "PRICE_PROXY",
            },
            quality="PRICE_PROXY",
        )
        store.save_dataset_catalog(
            "Polymarket-historical",
            "e2e-4096-v1",
            provider="SYNTHETIC_OFFLINE",
            instrument="POLYMARKET",
            market_type=MarketType.PREDICTION,
            timeframe="event",
            start_timestamp=T0,
            end_timestamp=T0 + timedelta(minutes=4095),
            row_count=4096,
            completeness=1.0,
            missing_ranges=(),
            quality="PRICE_PROXY",
            source_type="HISTORICAL",
            snapshot_id="SYNTHETIC_OFFLINE-e2e-4096-v1",
            metadata={
                "provider": "SYNTHETIC_OFFLINE",
                "source_type": "HISTORICAL",
                "research_quality": "PRICE_PROXY",
            },
        )
        if attested:
            attestation = store.verify_dataset_integrity_attestation(
                "Polymarket-historical",
                "e2e-4096-v1",
                force=True,
            )
            assert attestation["status"] == "CURRENT"
            assert attestation["row_count"] == 4096

    def test_predeclared_seed_enqueues_only_exact_currently_attested_4096_row_history(self) -> None:
        with AxiomStore(":memory:") as store:
            self._seed_predeclared_history(store)
            bus = DurableResearchBus(store)
            processor = AutonomousResearchProcessor(
                store,
                bus=bus,
                clock=lambda: T0,
            )

            queued = processor._enqueue_predeclared_from_persisted_scope(T0)

            self.assertEqual(len(queued), 3)
            self.assertEqual(store.research_queue_stats()["total"], 3)
            self.assertEqual(
                {
                    (
                        item.payload["dataset_id"],
                        item.payload["dataset_version"],
                        item.payload["dataset_selector"]["source_type"],
                    )
                    for item in queued
                },
                {("Polymarket-historical", "e2e-4096-v1", "HISTORICAL")},
            )
            self.assertEqual(store.list_reports(), [])

    def test_predeclared_seed_persists_missing_attestation_prerequisite_without_queue_rows(self) -> None:
        with AxiomStore(":memory:") as store:
            self._seed_predeclared_history(store, attested=False)
            processor = AutonomousResearchProcessor(store, clock=lambda: T0)

            self.assertEqual(processor._enqueue_predeclared_from_persisted_scope(T0), ())
            self.assertEqual(store.research_queue_stats()["total"], 0)
            reports = store.list_reports()
            self.assertEqual(len(reports), 1)
            evidence = reports[0]["report"]
            self.assertEqual(evidence["report_type"], "autonomous_predeclared_seed_blocked")
            self.assertEqual(evidence["blocker"], "DATASET_ATTESTATION_MISSING")
            self.assertEqual(evidence["next_action"], "PERSIST_CURRENT_HISTORICAL_DATASET_ATTESTATION")
            self.assertEqual(
                evidence["required_dataset"],
                {
                    "dataset_id": "Polymarket-historical",
                    "dataset_version": "e2e-4096-v1",
                    "source_type": "HISTORICAL",
                    "market_type": "prediction",
                    "attestation_status": "CURRENT",
                    "contamination_result": "PASS",
                },
            )

    def test_predeclared_seed_persists_stale_attestation_prerequisite_without_queue_rows(self) -> None:
        with AxiomStore(":memory:") as store:
            self._seed_predeclared_history(store)
            store.connection.execute(
                "UPDATE dataset_integrity_attestation "
                "SET status='STALE', reason='BOUNDS_MISMATCH' "
                "WHERE dataset_id=? AND dataset_version=?",
                ("Polymarket-historical", "e2e-4096-v1"),
            )
            processor = AutonomousResearchProcessor(store, clock=lambda: T0)

            self.assertEqual(processor._enqueue_predeclared_from_persisted_scope(T0), ())
            self.assertEqual(store.research_queue_stats()["total"], 0)
            evidence = store.list_reports()[0]["report"]
            self.assertEqual(evidence["blocker"], "DATASET_ATTESTATION_STALE")
            self.assertEqual(evidence["next_action"], "REFRESH_CURRENT_HISTORICAL_DATASET_ATTESTATION")
            self.assertEqual(evidence["attestation"]["status"], "STALE")
            self.assertEqual(evidence["attestation"]["reason"], "BOUNDS_MISMATCH")
    def test_predeclared_seed_persists_contamination_blocker_without_queue_rows(self) -> None:
        with AxiomStore(":memory:") as store:
            self._seed_predeclared_history(store)
            store.connection.execute(
                "UPDATE dataset_integrity_attestation "
                "SET contamination_result='FAIL', reason='FORWARD_CONTAMINATION', status='CURRENT' "
                "WHERE dataset_id=? AND dataset_version=?",
                ("Polymarket-historical", "e2e-4096-v1"),
            )
            processor = AutonomousResearchProcessor(store, clock=lambda: T0)

            self.assertEqual(processor._enqueue_predeclared_from_persisted_scope(T0), ())
            self.assertEqual(store.research_queue_stats()["total"], 0)
            evidence = store.list_reports()[0]["report"]
            self.assertEqual(evidence["blocker"], "DATASET_PROVENANCE_INVALID")
            self.assertEqual(
                evidence["next_action"],
                "REMOVE_FORWARD_CONTAMINATION_AND_REATTEST_HISTORICAL_DATASET",
            )
            self.assertEqual(evidence["attestation"]["contamination_result"], "FAIL")

    def test_predeclared_seed_evidence_history_keeps_latest_blocker_and_next_action(self) -> None:
        with AxiomStore(":memory:") as store:
            self._seed_predeclared_history(store)
            binding = "autonomous-predeclared-seed:Polymarket-historical:e2e-4096-v1"
            store.connection.execute(
                "UPDATE dataset_integrity_attestation "
                "SET status='STALE', reason='BOUNDS_MISMATCH' "
                "WHERE dataset_id=? AND dataset_version=?",
                ("Polymarket-historical", "e2e-4096-v1"),
            )
            processor = AutonomousResearchProcessor(store, clock=lambda: T0)

            self.assertEqual(processor._enqueue_predeclared_from_persisted_scope(T0), ())
            stale_history = store.list_reports(experiment_id=binding, newest_first=True)
            self.assertEqual(len(stale_history), 1)
            stale_evidence = deepcopy(stale_history[0]["report"])
            self.assertEqual(stale_evidence["blocker"], "DATASET_ATTESTATION_STALE")
            self.assertEqual(
                stale_evidence["next_action"],
                "REFRESH_CURRENT_HISTORICAL_DATASET_ATTESTATION",
            )

            store.connection.execute(
                "UPDATE dataset_integrity_attestation "
                "SET contamination_result='FAIL', reason='FORWARD_CONTAMINATION', status='CURRENT' "
                "WHERE dataset_id=? AND dataset_version=?",
                ("Polymarket-historical", "e2e-4096-v1"),
            )
            self.assertEqual(processor._enqueue_predeclared_from_persisted_scope(T0), ())
            self.assertEqual(processor._enqueue_predeclared_from_persisted_scope(T0), ())

            history = store.list_reports(experiment_id=binding, newest_first=True)
            self.assertEqual(len(history), 2)
            self.assertEqual(history[0]["report"]["blocker"], "DATASET_PROVENANCE_INVALID")
            self.assertEqual(history[0]["report"]["progress"], "BLOCKED")
            self.assertEqual(
                history[0]["report"]["next_action"],
                "REMOVE_FORWARD_CONTAMINATION_AND_REATTEST_HISTORICAL_DATASET",
            )
            self.assertEqual(history[0]["report"]["attestation"]["status"], "CURRENT")
            self.assertEqual(history[0]["report"]["attestation"]["contamination_result"], "FAIL")
            self.assertEqual(history[1]["report"]["blocker"], "DATASET_ATTESTATION_STALE")
            self.assertEqual(history[1]["report"]["attestation"]["status"], "STALE")


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
                end_timestamp=T0 + timedelta(hours=20),
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
                    "end_timestamp": T0 + timedelta(hours=20),
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
            forward_test_id = candidate["payload"]["forward_test_id"]
            frozen_forward = ForwardTestRegistry(store).get(forward_test_id)
            self.assertIsNotNone(frozen_forward)

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
            self.assertEqual(first_collection.candidate_bound_markets, ())
            self.assertEqual(first_collection.candidate_bound_scheduled, ())
            self.assertEqual(first_collection.candidate_bound_fresh, ())
            self.assertEqual(first_collection.candidate_references, {})
            self.assertEqual(first_collection.paper_forward_markets, (MARKET_ID,))
            self.assertEqual(first_collection.paper_forward_scheduled, (MARKET_ID,))
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
            self.assertEqual(
                ForwardTestRegistry(store).get(forward_test_id),
                frozen_forward,
            )
            self.assertEqual(second_collection.candidate_bound_markets, (MARKET_ID,))
            self.assertEqual(second_collection.candidate_bound_scheduled, (MARKET_ID,))
            self.assertIn(MARKET_ID, second_collection.candidate_bound_fresh)
            self.assertEqual(
                second_collection.candidate_references,
                {MARKET_ID: [candidate_id]},
            )
            self.assertEqual(second_collection.paper_forward_markets, ())
            self.assertEqual(second_collection.paper_forward_scheduled, ())
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
            paper_execution_events = store.connection.execute(
                "SELECT COUNT(*) AS n FROM paper_execution_events"
            ).fetchone()
            self.assertIsNotNone(paper_execution_events)
            assert paper_execution_events is not None
            self.assertEqual(int(paper_execution_events["n"]), 0)
            self.assertFalse(store.list_collection_errors())


def _synthetic_offline_history() -> list[dict[str, object]]:
    """Create immutable, clearly labelled offline history for the queue proof."""
    rows: list[dict[str, object]] = []
    for index in range(SYNTHETIC_MARKET_COUNT):
        market_id = f"synthetic-market-{index:02d}"
        opened = T0 + timedelta(days=index)
        for step in range(5):
            stamp = opened + timedelta(hours=step)
            rows.append(
                {
                    "market_id": market_id,
                    "question": "Synthetic offline market resolves YES.",
                    "category": "politics",
                    "yes_bid": 0.49,
                    "yes_ask": 0.51,
                    "yes_mid": 0.50,
                    "no_bid": 0.49,
                    "no_ask": 0.51,
                    "no_mid": 0.50,
                    "model_probability": SYNTHETIC_MODEL_PROBABILITY,
                    "liquidity": 1_500.0,
                    "spread": 0.02,
                    "expiry": (opened + timedelta(days=2)).isoformat(),
                    "resolution_criteria": "synthetic offline fixture outcome",
                    "timestamp": stamp.isoformat(),
                    "settlement": "open",
                    "regime": ("calm", "volatile", "transition")[step % 3],
                    "source_type": "HISTORICAL",
                    "fixture_label": "SYNTHETIC_OFFLINE",
                    "model_label": SYNTHETIC_MODEL_LABEL,
                }
            )
        rows.append(
            {
                "market_id": market_id,
                "question": "Synthetic offline market resolves YES.",
                "category": "politics",
                "yes_bid": 0.49,
                "yes_ask": 0.51,
                "yes_mid": 0.50,
                "no_bid": 0.49,
                "no_ask": 0.51,
                "no_mid": 0.50,
                "model_probability": SYNTHETIC_MODEL_PROBABILITY,
                "liquidity": 1_500.0,
                "spread": 0.02,
                "expiry": (opened + timedelta(days=2)).isoformat(),
                "resolution_criteria": "synthetic offline fixture outcome",
                "timestamp": (opened + timedelta(hours=5)).isoformat(),
                "settlement": "resolved_yes",
                "regime": "transition",
                "source_type": "HISTORICAL",
                "fixture_label": "SYNTHETIC_OFFLINE",
                "model_label": SYNTHETIC_MODEL_LABEL,
            }
        )
    return rows


def _runtime_plan(proposal_id: str) -> dict[str, object]:
    plan: dict[str, object] = {
        "market_type": "prediction",
        "template": "probability_mispricing",
        "dataset_id": "synthetic-offline-history",
        "dataset_version": "synthetic-offline-v1",
        "dataset_selector": {
            "dataset_id": "synthetic-offline-history",
            "dataset_version": "synthetic-offline-v1",
            "source_type": "HISTORICAL",
        },
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
        "filters": {"category": "politics"},
        "parameters": {"threshold": [0.03]},
        "methodology": {
            "time_split": "train-validation-holdout",
            "initial_cash": 10_000.0,
            "fee_bps": 0.0,
            "slippage_bps": 0.0,
            "allocation": 0.25,
        },
        "metrics": ["expectancy", "drawdown", "trade_count", "sample_count"],
        "min_samples": 30,
        "min_trades": 0,
        "max_variants": 1,
        "exit_policy": {"type": "fixed_holding_period", "holding_period": 4},
        "model_document": {"probability": SYNTHETIC_MODEL_PROBABILITY},
        "paper_only": True,
    }
    return {
        "proposal_id": proposal_id,
        "statement": "A deterministic synthetic offline probability edge is testable.",
        "source": f"SYNTHETIC_OFFLINE {SYNTHETIC_MODEL_LABEL} model",
        "tests": ["chronological train-validation-holdout", "bounded robustness checks"],
        "dataset_version": "synthetic-offline-v1",
        "time_split": "train-validation-holdout",
        "paper_only": True,
        "experiment_plan": plan,
    }


def _forward_metadata(market_id: str) -> dict[str, object]:
    expiry = (T0 + timedelta(days=30)).isoformat()
    return {
        "market_id": market_id,
        "condition_id": f"{market_id}-condition",
        "yes_token_id": f"{market_id}-yes",
        "no_token_id": f"{market_id}-no",
        "instrument": "POLYMARKET",
        "market_type": "prediction",
        "category": "politics",
        "yes_bid": 0.49,
        "yes_ask": 0.51,
        "yes_mid": 0.50,
        "liquidity": 1_500.0,
        "spread": 0.02,
        "expiry": expiry,
        "active": True,
        "closed": False,
        "metadata": {
            "instrument": "POLYMARKET",
            "market_type": "prediction",
            "category": "politics",
            "active": True,
            "closed": False,
            "expiry": expiry,
            "provenance_label": SYNTHETIC_MODEL_LABEL,
        },
        "snapshot": {
            "market_id": market_id,
            "instrument": "POLYMARKET",
            "market_type": "prediction",
            "category": "politics",
            "yes_mid": 0.50,
            "liquidity": 1_500.0,
            "spread": 0.02,
            "expiry": expiry,
            "settlement": "open",
        },
        "provenance_label": SYNTHETIC_MODEL_LABEL,
    }


def _forward_observation(
    market_id: str,
    stamp: datetime,
    *,
    settlement: str = "open",
    regime: str = "calm",
    no_fill: bool = False,
) -> dict[str, object]:
    observation: dict[str, object] = {
        "market_id": market_id,
        "instrument": "POLYMARKET",
        "market_type": "prediction",
        "timestamp": stamp.isoformat(),
        "yes_bid": 0.49,
        "yes_ask": 0.51,
        "yes_mid": 0.50,
        "no_bid": 0.49,
        "no_ask": 0.51,
        "no_mid": 0.50,
        "model_probability": SYNTHETIC_MODEL_PROBABILITY,
        "liquidity": 1_500.0,
        "spread": 0.02,
        "expiry": (T0 + timedelta(days=30)).isoformat(),
        "resolution_criteria": "synthetic offline fixture outcome",
        "settlement": settlement,
        "regime": regime,
        "source_type": "FORWARD_COLLECTED",
        "model_label": SYNTHETIC_MODEL_LABEL,
    }
    if no_fill:
        # A bid-only book is a real paper-execution no-fill, not a fabricated
        # result: the strategy signals, the book has no executable ask, and
        # the paper engine records ORDER_ATTEMPT followed by NO_FILL.
        observation["yes_order_book"] = {
            "timestamp": stamp.isoformat(),
            "bids": [{"price": 0.49, "size": 10.0}],
            "asks": [],
        }
    return observation


class MarketScopeRuntimeQualificationTests(unittest.TestCase):
    def test_ordinary_queue_qualifies_and_rejects_with_runtime_defaults(self) -> None:
        runtime_criteria = PromotionCriteria()
        relaxed_criteria = PromotionCriteria(
            min_independent_samples=0,
            min_trades=0,
            max_drawdown=1.0,
            min_expectancy=-1.0,
            min_confidence_lower_bound=-1.0,
            min_stability=0.0,
            min_calibration=0.0,
            min_liquidity=0.0,
            min_forward_duration_seconds=0.0,
            min_regimes=0,
        )
        runtime_record = runtime_criteria.as_record()
        relaxed_record = relaxed_criteria.as_record()
        configuration_record = {"relaxed": relaxed_record, "runtime": runtime_record}
        self.assertEqual(
            {key for key in runtime_record if relaxed_record[key] != runtime_record[key]},
            {
                "min_independent_samples",
                "min_trades",
                "max_drawdown",
                "min_expectancy",
                "min_confidence_lower_bound",
                "min_stability",
                "min_calibration",
                "min_forward_duration_seconds",
                "min_regimes",
            },
        )
        self.assertEqual(configuration_record["runtime"], PromotionCriteria().as_record())
        self.assertEqual(configuration_record["relaxed"], relaxed_criteria.as_record())

        with AxiomStore(":memory:") as store:
            history = _synthetic_offline_history()
            store.save_dataset(
                "synthetic-offline-history",
                "synthetic-offline-v1",
                history,
                metadata={
                    "source_type": "SYNTHETIC_OFFLINE",
                    "model_label": SYNTHETIC_MODEL_LABEL,
                    "model_probability": SYNTHETIC_MODEL_PROBABILITY,
                },
            )
            store.save_dataset_catalog(
                "synthetic-offline-history",
                "synthetic-offline-v1",
                provider="SYNTHETIC_OFFLINE",
                instrument="POLYMARKET",
                market_type=MarketType.PREDICTION,
                timeframe="event",
                start_timestamp=T0,
                end_timestamp=T0 + timedelta(days=29, hours=5),
                row_count=len(history),
                completeness=1.0,
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
            attestation = store.verify_dataset_integrity_attestation(
                "synthetic-offline-history",
                "synthetic-offline-v1",
            )
            self.assertEqual(attestation["status"], "CURRENT")
            self.assertEqual(attestation["row_count"], len(history))
            self.assertEqual(attestation["contamination_result"], "PASS")
            dataset_record = store.load_dataset_record("synthetic-offline-history", "synthetic-offline-v1")
            self.assertIsNotNone(dataset_record)
            assert dataset_record is not None
            self.assertEqual(dataset_record["metadata"]["source_type"], "SYNTHETIC_OFFLINE")
            self.assertEqual(dataset_record["metadata"]["model_label"], SYNTHETIC_MODEL_LABEL)
            self.assertEqual(dataset_record["metadata"]["model_probability"], SYNTHETIC_MODEL_PROBABILITY)
            self.assertEqual(len(history), SYNTHETIC_MARKET_COUNT * 6)

            market_ids = tuple(f"synthetic-market-{index:02d}" for index in range(SYNTHETIC_MARKET_COUNT))
            for market_id in market_ids:
                store.save_polymarket_market_metadata(
                    market_id,
                    _forward_metadata(market_id),
                    observed_at=T0,
                    source_type="FORWARD_COLLECTED",
                )

            bus = DurableResearchBus(store)
            qualifying_item = bus.submit_hypothesis(
                _runtime_plan("runtime-qualifying"),
                available_at=T0,
                dedupe_key="runtime-qualifying",
            )
            rejecting_item = bus.submit_hypothesis(
                _runtime_plan("runtime-rejecting"),
                available_at=T0,
                dedupe_key="runtime-rejecting",
            )
            processor = AutonomousResearchProcessor(
                store,
                bus=bus,
                config=AutonomousResearchConfig(
                    max_items_per_cycle=2,
                    max_plan_variants=1,
                    max_children_per_parent=0,
                    promotion_criteria=runtime_criteria,
                ),
                clock=lambda: T0,
            )

            queue_cycle = processor.process_pending(worker="ordinary-runtime-test", now=T0)
            self.assertEqual(queue_cycle.claimed, 2, repr(queue_cycle))
            self.assertEqual(queue_cycle.completed, 2, repr(queue_cycle))
            self.assertEqual(queue_cycle.rejected, 0, repr(queue_cycle))
            self.assertEqual(queue_cycle.failed, 0, repr(queue_cycle))
            qualifying_queue_item = bus.get(qualifying_item.item_id)
            rejecting_queue_item = bus.get(rejecting_item.item_id)
            self.assertIsNotNone(qualifying_queue_item)
            self.assertIsNotNone(rejecting_queue_item)
            assert qualifying_queue_item is not None
            assert rejecting_queue_item is not None
            self.assertEqual(qualifying_queue_item.status, ResearchQueueStatus.COMPLETED)
            self.assertEqual(rejecting_queue_item.status, ResearchQueueStatus.COMPLETED)
            qualifying_queue_result = qualifying_queue_item.result
            rejecting_queue_result = rejecting_queue_item.result
            self.assertIsInstance(qualifying_queue_result, dict)
            self.assertIsInstance(rejecting_queue_result, dict)
            assert isinstance(qualifying_queue_result, dict)
            assert isinstance(rejecting_queue_result, dict)
            self.assertEqual(
                qualifying_queue_result["hypothesis_id"],
                qualifying_item.payload["proposal_id"],
            )
            self.assertEqual(
                rejecting_queue_result["hypothesis_id"],
                rejecting_item.payload["proposal_id"],
            )
            qualifying_results = qualifying_queue_result["candidate_results"]
            rejecting_results = rejecting_queue_result["candidate_results"]
            self.assertEqual(len(qualifying_results), 1)
            self.assertEqual(len(rejecting_results), 1)
            qualifying_result = qualifying_results[0]
            rejecting_result = rejecting_results[0]
            self.assertEqual(qualifying_result["stage"], CandidateStage.PAPER_FORWARD.value)
            self.assertEqual(rejecting_result["stage"], CandidateStage.PAPER_FORWARD.value)
            qualifying_id = str(qualifying_result["candidate_id"])
            rejecting_id = str(rejecting_result["candidate_id"])

            records = store.load_candidate_lifecycle(limit=None)
            candidates = {
                str(record["candidate_id"]): record
                for record in records
                if isinstance(record.get("payload"), dict)
            }
            qualifying = candidates[qualifying_id]
            rejecting = candidates[rejecting_id]
            qualifying_payload = qualifying["payload"]
            rejecting_payload = rejecting["payload"]
            self.assertEqual(qualifying_payload["hypothesis_id"], qualifying_item.payload["proposal_id"])
            self.assertEqual(rejecting_payload["hypothesis_id"], rejecting_item.payload["proposal_id"])
            self.assertEqual(qualifying["stage"], CandidateStage.PAPER_FORWARD.value)
            self.assertEqual(rejecting["stage"], CandidateStage.PAPER_FORWARD.value)
            qualifying_spec = ForwardTestRegistry(store).get(qualifying_payload["forward_test_id"])
            rejecting_spec = ForwardTestRegistry(store).get(rejecting_payload["forward_test_id"])
            self.assertIsNotNone(qualifying_spec)
            self.assertIsNotNone(rejecting_spec)
            assert qualifying_spec is not None
            assert rejecting_spec is not None
            self.assertEqual(qualifying_spec.config["execution"], "paper_only")
            self.assertEqual(rejecting_spec.config["execution"], "paper_only")
            self.assertEqual(qualifying_spec.model_hash, rejecting_spec.model_hash)
            self.assertEqual(tuple(qualifying_spec.allowed_markets), market_ids)
            self.assertEqual(len(qualifying_spec.allowed_markets), SYNTHETIC_MARKET_COUNT)
            self.assertEqual(
                normalize_market_scope(qualifying_spec.config["market_scope"]).as_dict(),
                qualifying_payload["market_scope"],
            )
            self.assertEqual(qualifying_spec.config["market_scope_hash"], qualifying_payload["market_scope_hash"])
            self.assertEqual(qualifying_spec.config["market_scope_version"], qualifying_payload["market_scope_version"])
            self.assertEqual(
                normalize_market_scope(rejecting_spec.config["market_scope"]).as_dict(),
                rejecting_payload["market_scope"],
            )
            self.assertEqual(rejecting_spec.config["market_scope_hash"], rejecting_payload["market_scope_hash"])
            self.assertEqual(rejecting_spec.config["market_scope_version"], rejecting_payload["market_scope_version"])

            persisted_plans = store.list_experiment_plans(limit=10)
            self.assertEqual(len(persisted_plans), 2)
            for persisted in persisted_plans:
                plan = persisted["plan"]
                self.assertEqual(plan["min_samples"], 30)
                self.assertEqual(plan["min_trades"], 0)
                self.assertEqual(
                    plan["model_document"],
                    {"probability": SYNTHETIC_MODEL_PROBABILITY},
                )


            qualifying_observations = [
                item
                for market_id in qualifying_spec.allowed_markets
                for item in (
                    _forward_observation(market_id, T0 + timedelta(hours=1), regime=("calm", "volatile", "transition")[int(market_id[-2:]) % 3]),
                    _forward_observation(market_id, T0 + timedelta(hours=1, minutes=1), settlement="resolved_yes"),
                )
            ]
            qualifying_cycle = run_forward_paper(
                qualifying_spec,
                store=store,
                strategy=qualifying_payload["strategy"],
                model={"probability": SYNTHETIC_MODEL_PROBABILITY},
                observations=qualifying_observations,
                now=T0 + timedelta(days=7),
            )
            self.assertEqual(qualifying_cycle.fills_inserted, SYNTHETIC_MARKET_COUNT)
            self.assertEqual(qualifying_cycle.settlements, SYNTHETIC_MARKET_COUNT)
            self.assertEqual(qualifying_cycle.errors, ())

            rejecting_observations = [
                item
                for market_id in rejecting_spec.allowed_markets[:5]
                for item in (
                    _forward_observation(
                        market_id,
                        T0 + timedelta(hours=2),
                        regime="calm",
                        no_fill=True,
                    ),
                    _forward_observation(
                        market_id,
                        T0 + timedelta(hours=2, minutes=1),
                        settlement="resolved_yes",
                    ),
                )
            ]
            rejecting_cycle = run_forward_paper(
                rejecting_spec,
                store=store,
                strategy=rejecting_payload["strategy"],
                model={"probability": SYNTHETIC_MODEL_PROBABILITY},
                observations=rejecting_observations,
                now=T0 + timedelta(days=7),
            )
            self.assertEqual(rejecting_cycle.fills_inserted, 0)
            self.assertEqual(rejecting_cycle.settlements, 5)
            self.assertEqual(rejecting_cycle.errors, ())
            # Current scope authority is written by the normal collector
            # stage, not by autonomous queue processing.  Use one provider
            # inventory for both candidates so the persisted resolution is
            # the same canonical handoff consumed by canary evaluation.
            scope_provider = _SyntheticOfflinePublicProvider(
                market_ids=market_ids,
                stamp=T0 + timedelta(days=7),
            )
            collector = PolymarketCollector(
                scope_provider,
                store,
                CollectorConfig(
                    collector_name="SYNTHETIC_OFFLINE-runtime-collector",
                    max_markets=SYNTHETIC_MARKET_COUNT,
                    discovery_budget_per_cycle=SYNTHETIC_MARKET_COUNT,
                    max_attempts=1,
                    backoff_initial_seconds=0.0,
                    jitter_seconds=0.0,
                    freshness_sla_seconds=60.0,
                ),
                clock=lambda: T0 + timedelta(days=7),
                sleep=lambda _seconds: None,
            )
            collection = collector.collect_once(now=T0 + timedelta(days=7))
            self.assertEqual(collection.errors, 0, repr(collection))
            self.assertEqual(collection.candidate_bound_markets, ())
            self.assertEqual(collection.candidate_bound_scheduled, ())
            self.assertEqual(collection.candidate_bound_fresh, ())
            self.assertEqual(collection.candidate_references, {})
            self.assertEqual(set(collection.paper_forward_markets), set(market_ids))
            self.assertEqual(set(collection.paper_forward_scheduled), set(market_ids))
            self.assertEqual(scope_provider.markets_calls, 1)
            self.assertEqual(scope_provider.order_books_calls, SYNTHETIC_MARKET_COUNT)

            reevaluated = processor.reevaluate_forward_candidates(now=T0 + timedelta(days=7))
            outcomes = {item["candidate_id"]: item for item in reevaluated}
            self.assertEqual(outcomes[qualifying_id]["candidate_id"], qualifying_id)
            self.assertEqual(outcomes[rejecting_id]["candidate_id"], rejecting_id)
            self.assertEqual(outcomes[qualifying_id]["stage"], CandidateStage.PAPER_PROMOTABLE.value)
            self.assertEqual(outcomes[qualifying_id]["promotion_reasons"], [])
            self.assertEqual(outcomes[rejecting_id]["stage"], CandidateStage.REJECTED.value)
            self.assertIn("execution_impossible", outcomes[rejecting_id]["promotion_reasons"])
            self.assertEqual(rejecting_payload["hypothesis_id"], rejecting_item.payload["proposal_id"])
            self.assertEqual(rejecting_result["candidate_id"], rejecting_id)
            self.assertEqual(rejecting_result["stage"], CandidateStage.PAPER_FORWARD.value)
            # Consume the collector's durable canonical authority rather than
            # rebuilding a second scope from ad hoc records.  The forward
            # handoff must preserve the immutable candidate identity and
            # policy before the canary applies its execution cap.
            scope_resolution = store.load_market_scope_resolution(qualifying_id)
            self.assertIsNotNone(scope_resolution)
            assert scope_resolution is not None
            self.assertEqual(scope_resolution.status, MATCHED)
            self.assertEqual(scope_resolution.candidate_id, qualifying_id)
            self.assertEqual(scope_resolution.scope_hash, qualifying_payload["market_scope_hash"])
            self.assertEqual(scope_resolution.scope_version, qualifying_payload["market_scope_version"])
            self.assertEqual(scope_resolution.policy, qualifying_payload["market_scope"])
            self.assertEqual(len(scope_resolution.matched_markets), SYNTHETIC_MARKET_COUNT)
            self.assertEqual(scope_resolution.provenance["resolution_source"], "persisted_current_markets")
            self.assertEqual(
                tuple(item.market_id for item in scope_resolution.matched_markets),
                market_ids,
            )

            canary_evaluation = CanaryService(
                store,
                clock=lambda: T0 + timedelta(days=7),
            ).evaluate_signal(
                qualifying_id,
                cycle_id="runtime-canary-cap",
            )
            self.assertEqual(
                canary_evaluation["reason_code"],
                EXECUTION_FEASIBILITY_MARKET_CAP,
            )
            self.assertIsNone(canary_evaluation["signal"])
            self.assertEqual(
                canary_evaluation["evidence"]["resolved_market_count"],
                SYNTHETIC_MARKET_COUNT,
            )
            self.assertEqual(
                canary_evaluation["evidence"]["execution_market_cap"],
                8,
            )
            self.assertEqual(
                CanaryService(store, clock=lambda: T0 + timedelta(days=7))
                .list_signal_evaluations(
                    candidate_id=qualifying_id,
                    cycle_id="runtime-canary-cap",
                    limit=1,
                )[0]["reason_code"],
                EXECUTION_FEASIBILITY_MARKET_CAP,
            )
            qualifying_events = store.list_candidate_lifecycle_events(qualifying_id, limit=100)
            rejecting_events = store.list_candidate_lifecycle_events(rejecting_id, limit=100)
            qualifying_promotion_events = [
                event
                for event in qualifying_events
                if event["candidate_id"] == qualifying_id
                and event["to_stage"] == CandidateStage.PAPER_PROMOTABLE.value
                and event["reason"] == "paper-forward criteria passed; human review required"
            ]
            rejecting_rejection_events = [
                event
                for event in rejecting_events
                if event["candidate_id"] == rejecting_id
                and event["to_stage"] == CandidateStage.REJECTED.value
                and event["reason"] == "execution_impossible"
            ]
            self.assertEqual(len(qualifying_promotion_events), 1)
            self.assertEqual(len(rejecting_rejection_events), 1)
            qualifying_promotion_event = qualifying_promotion_events[0]
            rejecting_rejection_event = rejecting_rejection_events[0]
            self.assertEqual(qualifying_promotion_event["candidate_id"], qualifying_id)
            self.assertEqual(rejecting_rejection_event["candidate_id"], rejecting_id)
            self.assertEqual(
                rejecting_rejection_event["payload"]["rejection_reason"],
                "execution_impossible",
            )
            self.assertEqual(
                store.load_candidate_lifecycle(rejecting_id)["payload"]["rejection_reason"],
                "execution_impossible",
            )

            execution_events = (
                store.list_paper_execution_events(qualifying_spec.experiment_id)
                + store.list_paper_execution_events(rejecting_spec.experiment_id)
            )
            self.assertTrue(execution_events)
            self.assertTrue(all(item["payload"].get("paper_only") is True for item in execution_events))
            self.assertFalse(any(item["payload"].get("live_execution") for item in execution_events))
            live_submission_events = [
                item
                for item in execution_events
                if item["payload"].get("live_execution") is True
            ]
            self.assertEqual(live_submission_events, [])
    def test_ordinary_worker_recovers_legacy_predecessor_without_live_events(self) -> None:
        legacy_dataset_id = "prediction:legacy-market"
        legacy_dataset_version = "legacy-v1"
        legacy_plan = {
            "market_type": "prediction",
            "template": "momentum",
            "parameters": {"lookback": [1], "threshold": [0.05]},
            "target": {"market_ids": ["legacy-market"]},
            "dataset_id": legacy_dataset_id,
            "dataset_version": legacy_dataset_version,
            "allowed_features": ["timestamp", "market_id", "yes_mid"],
            "time_split": "train-validation-holdout",
            "min_samples": 1,
            "min_trades": 0,
            "max_variants": 1,
            "methodology": {"initial_cash": 1_000.0, "allocation": 0.25},
            "exit_policy": {"type": "fixed_holding_period", "holding_period": 1},
            "paper_only": True,
        }
        predecessor = {
            "candidate_id": "legacy-predecessor-e2e",
            "frozen_hash": "sha256:legacy-e2e-frozen",
            "hypothesis_id": "legacy-e2e-hypothesis",
            "statement": "A persisted legacy scope remains researchable.",
            "source": "offline legacy fixture",
            "market_type": "prediction",
            "experiment_plan": legacy_plan,
            "paper_only": True,
        }
        rows = [
            {
                "timestamp": (T0 + timedelta(hours=index)).isoformat(),
                "market_id": "legacy-market",
                "yes_mid": 0.40 + 0.02 * index,
                "settlement": "open",
                "source_type": "HISTORICAL",
            }
            for index in range(6)
        ]
        before = deepcopy(predecessor)
        with AxiomStore(":memory:") as store:
            store.save_dataset(
                legacy_dataset_id,
                legacy_dataset_version,
                rows,
                metadata={
                    "source_type": "HISTORICAL",
                    "provider": "polymarket",
                    "instrument": "POLYMARKET",
                    "market_type": "prediction",
                },
                quality="PRICE_PROXY",
            )
            store.save_dataset_catalog(
                legacy_dataset_id,
                legacy_dataset_version,
                provider="polymarket",
                instrument="POLYMARKET",
                market_type="prediction",
                timeframe="event",
                start_timestamp=T0,
                end_timestamp=T0 + timedelta(hours=5),
                row_count=len(rows),
                completeness=1.0,
                missing_ranges=(),
                quality="PRICE_PROXY",
                source_type="HISTORICAL",
                snapshot_id="legacy-history-snapshot",
                metadata={
                    "source_type": "HISTORICAL",
                    "provider": "polymarket",
                    "instrument": "POLYMARKET",
                    "market_type": "prediction",
                    "market_id": "legacy-market",
                    "research_quality": "PRICE_PROXY",
                    "historical_order_book_available": False,
                },
            )
            store.verify_dataset_integrity_attestation(
                legacy_dataset_id,
                legacy_dataset_version,
                force=True,
            )
            store.save_candidate_lifecycle("legacy-predecessor-e2e", CandidateStage.IDEA.value, predecessor)
            store.save_candidate_lifecycle("legacy-predecessor-e2e", CandidateStage.FROZEN.value, predecessor)
            bus = DurableResearchBus(store)
            processor = AutonomousResearchProcessor(
                store,
                bus=bus,
                config=AutonomousResearchConfig(max_items_per_cycle=1),
                clock=lambda: T0,
            )
            cycle = processor.process_pending(worker="ordinary-research", now=T0)
            self.assertEqual(cycle.claimed, 1, repr(cycle))
            self.assertEqual(len(cycle.legacy_recovery), 1)
            recovery = cycle.legacy_recovery[0]
            self.assertEqual(recovery["classification"], "LEGACY_UNAMBIGUOUS")
            self.assertEqual(recovery["progress"], "ENQUEUED")
            self.assertTrue(recovery["evidence_persisted"])
            successor_item = bus.get(str(recovery["queue_item_id"]))
            self.assertIsNotNone(successor_item)
            assert successor_item is not None
            self.assertIn(successor_item.status, {ResearchQueueStatus.COMPLETED, ResearchQueueStatus.REJECTED})
            self.assertEqual(successor_item.payload["successor_relation"], "LEGACY_SCOPE_SUCCESSOR")
            self.assertEqual(successor_item.payload["predecessor_candidate_id"], "legacy-predecessor-e2e")
            self.assertEqual(store.load_candidate_lifecycle("legacy-predecessor-e2e")["payload"], before)
            self.assertEqual(
                store.connection.execute("SELECT COUNT(*) AS n FROM canary_submission_attempts").fetchone()["n"],
                0,
            )
            self.assertEqual(
                store.connection.execute("SELECT COUNT(*) AS n FROM paper_execution_events").fetchone()["n"],
                0,
            )

    def test_generated_legacy_successor_rejects_stale_attestation_before_freeze(self) -> None:
        with AxiomStore(":memory:") as store:
            _seed_predeclared_historical_dataset(store)
            predecessor = _legacy_prediction_predecessor(candidate_id="legacy-stale-generated")
            store.save_candidate_lifecycle(
                "legacy-stale-generated",
                CandidateStage.IDEA.value,
                predecessor,
            )
            store.save_candidate_lifecycle(
                "legacy-stale-generated",
                CandidateStage.FROZEN.value,
                predecessor,
            )
            bus = DurableResearchBus(store)
            processor = AutonomousResearchProcessor(
                store,
                bus=bus,
                config=AutonomousResearchConfig(max_items_per_cycle=1),
                clock=lambda: T0,
            )
            recovery = processor._recover_legacy_predecessors(T0)
            self.assertEqual(recovery[0]["progress"], "ENQUEUED")
            successor_item = bus.get(str(recovery[0]["queue_item_id"]))
            self.assertIsNotNone(successor_item)
            assert successor_item is not None
            internal = successor_item.payload["provenance"]["internal"]
            self.assertEqual(internal["schema"], "axiom-generated-queue-v1")
            self.assertTrue(internal["generated"])
            self.assertEqual(internal["kind"], "legacy_scope_successor")
            self.assertEqual(internal["dataset_id"], "Polymarket-historical")
            self.assertEqual(internal["dataset_version"], "history-v1")
            self.assertTrue(str(internal["attestation_hash"]).startswith("sha256:"))

            store.connection.execute(
                "UPDATE dataset_integrity_attestation "
                "SET status='CURRENT', attestation_hash='sha256:stale-legacy-attestation', "
                "reason='ATTESTATION_ROTATED' "
                "WHERE dataset_id=? AND dataset_version=?",
                ("Polymarket-historical", "history-v1"),
            )
            cycle = processor.process_pending(worker="ordinary-research", now=T0)
            self.assertEqual(cycle.claimed, 1)
            self.assertEqual(cycle.rejected, 1)
            self.assertEqual(cycle.results[0]["reason_code"], "DATASET_ATTESTATION_CHANGED")

            old_item_id = successor_item.item_id
            old_payload = deepcopy(successor_item.payload)
            generated_successors = [
                item
                for item in store.list_research_items(limit=20)
                if item["payload"].get("successor_relation") == "LEGACY_SCOPE_SUCCESSOR"
            ]
            self.assertEqual(len(generated_successors), 2)
            old_item = next(item for item in generated_successors if item["item_id"] == old_item_id)
            new_items = [item for item in generated_successors if item["item_id"] != old_item_id]
            self.assertEqual(len(new_items), 1)
            new_item = new_items[0]
            self.assertEqual(old_item["payload"], old_payload)
            self.assertNotEqual(new_item["item_id"], old_item_id)
            self.assertEqual(
                new_item["payload"]["proposal_id"],
                old_payload["proposal_id"],
            )
            self.assertEqual(
                new_item["payload"]["provenance"]["internal"]["attestation_hash"],
                "sha256:stale-legacy-attestation",
            )
            self.assertEqual(new_item["status"], ResearchQueueStatus.PENDING.value)

            retry_cycle = processor.process_pending(worker="ordinary-research", now=T0)
            self.assertEqual(retry_cycle.claimed, 1)
            self.assertEqual(len(retry_cycle.legacy_recovery), 1)
            self.assertEqual(
                retry_cycle.legacy_recovery[0]["queue_item_id"],
                new_item["item_id"],
            )
            self.assertEqual(
                retry_cycle.legacy_recovery[0]["next_action"],
                "CLAIM_AND_PROCESS_CANONICAL_SUCCESSOR",
            )
            generated_after_retry = [
                item
                for item in store.list_research_items(limit=20)
                if item["payload"].get("successor_relation") == "LEGACY_SCOPE_SUCCESSOR"
            ]
            self.assertEqual(len(generated_after_retry), 2)

    def test_ordinary_worker_leaves_malformed_legacy_scope_untouched(self) -> None:
        predecessor = {
            "candidate_id": "legacy-malformed-e2e",
            "frozen_hash": "sha256:legacy-malformed-frozen",
            "hypothesis_id": "legacy-malformed-hypothesis",
            "statement": "Malformed legacy scope must remain blocked.",
            "source": "offline legacy fixture",
            "market_type": "prediction",
            "experiment_plan": {
                "market_type": "prediction",
                "template": "momentum",
                "parameters": {"lookback": [1], "threshold": [0.05]},
                "target": {},
                "dataset_id": "missing-history",
                "dataset_version": "missing-v1",
                "paper_only": True,
            },
            "paper_only": True,
        }
        before = deepcopy(predecessor)
        with AxiomStore(":memory:") as store:
            store.save_candidate_lifecycle("legacy-malformed-e2e", CandidateStage.IDEA.value, predecessor)
            store.save_candidate_lifecycle("legacy-malformed-e2e", CandidateStage.FROZEN.value, predecessor)
            processor = AutonomousResearchProcessor(store, clock=lambda: T0)
            cycle = processor.process_pending(worker="ordinary-research", now=T0)
            self.assertEqual(cycle.claimed, 0)
            self.assertEqual(store.research_queue_stats()["total"], 0)
            self.assertEqual(store.load_candidate_lifecycle("legacy-malformed-e2e")["payload"], before)
            self.assertEqual(len(cycle.legacy_recovery), 1)
            recovery = cycle.legacy_recovery[0]
            self.assertEqual(recovery["classification"], "INVALID")
            self.assertEqual(recovery["reason_code"], "MISSING_MARKET_SCOPE")
            self.assertEqual(recovery["progress"], "BLOCKED")
            self.assertEqual(recovery["blocker"], "MISSING_MARKET_SCOPE")
            self.assertTrue(recovery["provenance_state_digest"].startswith("sha256:"))
            processor.process_pending(worker="ordinary-research", now=T0)
            history = store.list_reports(
                experiment_id="legacy-malformed-e2e",
                newest_first=True,
            )
            self.assertEqual(len(history), 1)
            self.assertEqual(
                history[0]["report"]["provenance_state_digest"],
                recovery["provenance_state_digest"],
            )



if __name__ == "__main__":
    unittest.main()
