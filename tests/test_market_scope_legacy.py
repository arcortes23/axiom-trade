from __future__ import annotations

from copy import deepcopy
import unittest

from axiom import (
    CANONICAL_VALID,
    INVALID,
    LEGACY_AMBIGUOUS,
    LEGACY_UNAMBIGUOUS,
    AxiomStore,
    DurableResearchBus,
    LegacyScopeError,
    classify_legacy_scope,
    create_legacy_successor,
    enqueue_legacy_successor,
)


from axiom.lifecycle import CandidateStage
from axiom.ranker import CandidateCanaryRanker

class LegacyMarketScopeTests(unittest.TestCase):
    @staticmethod
    def _legacy() -> dict[str, object]:
        return {
            "candidate_id": "legacy-candidate",
            "frozen_hash": "sha256:legacy-frozen",
            "hypothesis_id": "legacy-hypothesis",
            "statement": "A bounded legacy scope remains researchable",
            "source": "offline-fixture",
            "market_type": "prediction",
            "template": "probability_mispricing",
            "target": {"market_ids": ["market-1"]},
            "dataset_id": "prediction:market-1",
            "dataset_version": "legacy-v1",
            "time_split": "train-validation-holdout",
            "paper_only": True,
        }

    def test_classification_does_not_mutate_source_and_canonical_is_distinct(self) -> None:
        document = self._legacy()
        before = deepcopy(document)
        assessment = classify_legacy_scope(document)
        self.assertEqual(assessment.classification, LEGACY_UNAMBIGUOUS)
        self.assertEqual(document, before)

        canonical = {
            "candidate_id": "canonical",
            "market_scope": assessment.scope.as_dict() if assessment.scope is not None else {},
        }
        self.assertEqual(classify_legacy_scope(canonical).classification, CANONICAL_VALID)

    def test_successor_is_deterministic_canonical_and_preserves_predecessor(self) -> None:
        first = create_legacy_successor(self._legacy())
        second = create_legacy_successor(deepcopy(self._legacy()))
        self.assertEqual(first.successor_id, second.successor_id)
        self.assertEqual(dict(first.proposal), dict(second.proposal))
        self.assertEqual(first.predecessor_candidate_id, "legacy-candidate")
        self.assertEqual(first.predecessor_frozen_hash, "sha256:legacy-frozen")
        self.assertEqual(first.proposal["experiment_plan"]["market_scope"]["provenance"], "canonical")
        self.assertEqual(first.proposal["provenance"]["predecessor_frozen_hash"], "sha256:legacy-frozen")


    def test_enqueue_uses_pending_queue_without_lifecycle_or_eligibility_mutation(self) -> None:
        source = self._legacy()
        before = deepcopy(source)
        with AxiomStore(":memory:") as store:
            store.save_dataset_catalog(
                "prediction:market-1",
                "legacy-v1",
                provider="fixture",
                instrument="prediction",
                market_type="prediction",
                row_count=1,
                completeness=1.0,
                source_type="HISTORICAL",
                snapshot_id="snapshot-1",
                metadata={"market_id": "market-1"},
            )
            successor = enqueue_legacy_successor(source, store=store)
            self.assertIsNotNone(successor.queue_item)
            assert successor.queue_item is not None
            self.assertEqual(successor.queue_item.status.value, "PENDING")
            self.assertIsNone(store.load_candidate_lifecycle(successor.successor_id))
            table = store.connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='canary_eligibility'"
            ).fetchone()
            if table is not None:
                self.assertIsNone(
                    store.connection.execute(
                        "SELECT 1 FROM canary_eligibility WHERE candidate_id=?",
                        (successor.successor_id,),
                    ).fetchone()
                )
        self.assertEqual(source, before)

    def test_legacy_source_stays_unselected_while_successor_is_pending(self) -> None:
        source = self._legacy()
        before = deepcopy(source)
        with AxiomStore(":memory:") as store:
            store.save_dataset(
                "prediction:market-1",
                "legacy-v1",
                [
                    {
                        "timestamp": "2026-01-01T00:00:00+00:00",
                        "market_id": "market-1",
                        "price": 0.5,
                        "source_type": "HISTORICAL",
                    }
                ],
            )
            store.save_dataset_catalog(
                "prediction:market-1",
                "legacy-v1",
                provider="fixture",
                instrument="prediction",
                market_type="prediction",
                timeframe="event",
                row_count=1,
                completeness=1.0,
                quality="PRICE_PROXY",
                source_type="HISTORICAL",
                snapshot_id="snapshot-legacy",
                metadata={
                    "market_id": "market-1",
                    "source_type": "HISTORICAL",
                    "research_quality": "PRICE_PROXY",
                    "historical_order_book_available": False,
                },
            )
            store.verify_dataset_integrity_attestation(
                "prediction:market-1",
                "legacy-v1",
            )
            store.save_candidate_lifecycle(
                source["candidate_id"],
                CandidateStage.IDEA.value,
                source,
            )
            store.save_candidate_lifecycle(
                source["candidate_id"],
                CandidateStage.FROZEN.value,
                source,
            )
            result = CandidateCanaryRanker(store).evaluate_and_select()
            self.assertIsNone(result["selected_candidate"])
            self.assertEqual(result["eligible_count"], 0)
            self.assertEqual(result["rankable_count"], 0)
            self.assertEqual(
                store.load_candidate_lifecycle(source["candidate_id"])["payload"],
                source,
            )

            successor = enqueue_legacy_successor(source, store=store)
            self.assertIsNotNone(successor.queue_item)
            assert successor.queue_item is not None
            self.assertEqual(successor.queue_item.status.value, "PENDING")
            self.assertEqual(
                successor.proposal["experiment_plan"]["market_scope"]["provenance"],
                "canonical",
            )
            self.assertIsNone(store.load_candidate_lifecycle(successor.successor_id))
        self.assertEqual(source, before)

    def test_ambiguous_and_malformed_scopes_fail_closed_without_queue_writes(self) -> None:
        ambiguous = self._legacy()
        ambiguous["target_market_ids"] = ["different-market"]
        self.assertEqual(classify_legacy_scope(ambiguous).classification, LEGACY_AMBIGUOUS)
        malformed = self._legacy()
        malformed["target"] = {"market_ids": [""]}
        self.assertEqual(classify_legacy_scope(malformed).classification, INVALID)
        with AxiomStore(":memory:") as store:
            with self.assertRaises(LegacyScopeError):
                enqueue_legacy_successor(ambiguous, store=store)
            self.assertEqual(store.research_queue_stats().get("total", 0), 0)

    def test_single_candidate_api_has_no_bulk_migration_path(self) -> None:
        self.assertFalse(hasattr(DurableResearchBus, "migrate_legacy_scopes"))
        with self.assertRaises(LegacyScopeError):
            create_legacy_successor({"market_ids": ["m"]})


if __name__ == "__main__":
    unittest.main()
