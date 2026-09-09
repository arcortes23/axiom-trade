from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
import unittest

from axiom.canary import _canary_current_scope_resolution
from axiom.lifecycle import CandidateStage, _stage_gate
from axiom.ranker import CandidateCanaryRanker
from axiom.storage import AxiomStore

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
import unittest

from axiom.canary import _canary_current_scope_resolution
from axiom.lifecycle import CandidateStage, _stage_gate


UTC = timezone.utc
NOW = datetime(2026, 1, 1, tzinfo=UTC)


class ResolutionStore:
    def __init__(self, resolution: object | None) -> None:
        self.resolution = resolution
        self.calls: list[tuple[str, str | None, str | None]] = []

    def load_market_scope_resolution(
        self,
        candidate_id: str,
        *,
        scope_hash: str | None = None,
        scope_version: str | None = None,
        resolved_at: object | None = None,
    ) -> object | None:
        self.calls.append((candidate_id, scope_hash, scope_version))
        if scope_hash is None and scope_version is None:
            return self.resolution
        if self.resolution is None:
            return None
        if (
            getattr(self.resolution, "scope_hash", None) == scope_hash
            and getattr(self.resolution, "scope_version", None) == scope_version
        ):
            return self.resolution
        return None


def payload() -> dict[str, object]:
    return {
        "plan_hash": "sha256:plan",
        "market_scope_hash": "sha256:scope",
        "market_scope_version": "market-scope-v1",
        "market_scope": {
            "mode": "EXACT_MARKETS",
            "market_ids": ["m-1"],
        },
        "dataset_selector": {
            "dataset_id": "history",
            "dataset_version": "v1",
            "source_type": "HISTORICAL",
        },
        "dataset_attestation": {"status": "CURRENT", "hash": "sha256:dataset"},
    }


def resolution(
    *,
    scope_hash: str = "sha256:scope",
    status: str = "MATCHED",
    markets: tuple[object, ...] = (),
    resolved_at: datetime = NOW,
    reason: str = "MATCHED",
) -> SimpleNamespace:
    return SimpleNamespace(
        candidate_id="candidate-1",
        scope_hash=scope_hash,
        scope_version="market-scope-v1",
        resolved_at=resolved_at,
        status=status,
        reason=reason,
        matched_markets=markets,
        excluded_markets=(),
        deferred_markets=(),
        resolution_id="resolution-1",
    )


class MarketScopeAuthorityTests(unittest.TestCase):
    def test_matching_resolution_requires_frozen_scope_hash_and_version(self) -> None:
        market = SimpleNamespace(
            market_id="m-1", yes_token_id="yes-1", no_token_id="no-1"
        )
        store = ResolutionStore(resolution(markets=(market,)))
        result = _canary_current_scope_resolution(
            store, "candidate-1", payload(), now=NOW
        )
        self.assertTrue(result["bound"])
        self.assertEqual(result["matched_markets"][0]["yes_token_id"], "yes-1")
        self.assertEqual(store.calls[0][1:], ("sha256:scope", "market-scope-v1"))
    def test_scope_less_candidate_never_uses_persisted_resolution(self) -> None:
        market = SimpleNamespace(
            market_id="m-1", yes_token_id="yes-1", no_token_id="no-1"
        )
        store = ResolutionStore(resolution(markets=(market,)))
        result = _canary_current_scope_resolution(
            store,
            "candidate-1",
            {"market_ids": ["m-1"]},
            now=NOW,
        )
        self.assertFalse(result["bound"])
        self.assertEqual(result["reason_code"], "LEGACY_SCOPE_SUCCESSOR_REQUIRED")
        self.assertEqual(store.calls, [])

    def test_hash_mismatch_is_fail_closed(self) -> None:
        store = ResolutionStore(resolution(scope_hash="sha256:other"))
        result = _canary_current_scope_resolution(
            store, "candidate-1", payload(), now=NOW
        )
        self.assertFalse(result["bound"])
        self.assertEqual(result["reason_code"], "SCOPE_RESOLUTION_SCOPE_MISMATCH")

    def test_stale_resolution_is_not_current(self) -> None:
        store = ResolutionStore(
            resolution(
                markets=(SimpleNamespace(market_id="m-1", yes_token_id="y", no_token_id="n"),),
                resolved_at=NOW - timedelta(seconds=61),
            )
        )
        result = _canary_current_scope_resolution(
            store, "candidate-1", payload(), now=NOW
        )
        self.assertEqual(result["reason_code"], "SCOPE_RESOLUTION_STALE")

    def test_resolution_statuses_and_token_mismatch_are_distinct(self) -> None:
        research = _canary_current_scope_resolution(
            ResolutionStore(resolution(status="RESEARCH_ONLY", reason="RESEARCH_ONLY")),
            "candidate-1",
            payload(),
            now=NOW,
        )
        self.assertEqual(research["reason_code"], "RESEARCH_ONLY")
        deferred = _canary_current_scope_resolution(
            ResolutionStore(resolution(status="DEFERRED", reason="DEFERRED_MARKETS")),
            "candidate-1",
            payload(),
            now=NOW,
        )
        self.assertEqual(deferred["reason_code"], "DEFERRED_MARKETS")
        zero = _canary_current_scope_resolution(
            ResolutionStore(resolution(status="ZERO_MATCHES", reason="ZERO_MATCHES")),
            "candidate-1",
            payload(),
            now=NOW,
        )
        self.assertEqual(zero["reason_code"], "SCOPE_RESOLUTION_ZERO_MATCHES")
        token = _canary_current_scope_resolution(
            ResolutionStore(
                resolution(
                    markets=(SimpleNamespace(market_id="m-1", yes_token_id="yes-1"),)
                )
            ),
            "candidate-1",
            payload(),
            now=NOW,
        )
        self.assertEqual(token["reason_code"], "SCOPE_RESOLUTION_TOKEN_MISMATCH")

    def test_frozen_scope_evidence_requires_selector_and_attestation(self) -> None:
        evidence = {
            "frozen": True,
            "holdout_used": False,
            "strategy_hash": "strategy",
            "model_hash": "model",
            "config_hash": "config",
            "risk_snapshot": {"max_position_fraction": 0.05},
            "plan_hash": "sha256:plan",
            "market_scope_hash": "sha256:scope",
            "market_scope_version": "market-scope-v1",
            "market_scope": {
                "schema_version": "1",
                "mode": "EXACT_MARKETS",
                "market_ids": ["m-1"],
                "provenance": "canonical",
            },
        }
        self.assertEqual(
            _stage_gate(CandidateStage.FROZEN, evidence),
            "missing frozen scope evidence: dataset_selector",
        )

    def test_scope_less_frozen_document_requires_canonical_successor(self) -> None:
        evidence = {
            "frozen": True,
            "holdout_used": False,
            "strategy_hash": "strategy",
            "model_hash": "model",
            "config_hash": "config",
            "risk_snapshot": {"max_position_fraction": 0.05},
        }
        self.assertEqual(
            _stage_gate(CandidateStage.FROZEN, evidence),
            "LEGACY_SCOPE_SUCCESSOR_REQUIRED",
        )
    def test_ranker_hash_canonicalizes_equivalent_forward_evidence_forms(self) -> None:
        forward = {
            "forward_expectancy": 0.40,
            "forward_confidence_lower_bound": 0.20,
            "forward_independent_resolved_bets": 30,
            "forward_successful_order_attempts": 20,
        }
        nested = {
            "candidate_id": "candidate-1",
            "stage": CandidateStage.PAPER_FORWARD.value,
            "payload": {**payload(), "forward_evidence": forward},
        }
        duplicated = {
            "candidate_id": "candidate-1",
            "stage": CandidateStage.PAPER_FORWARD.value,
            "payload": {
                **payload(),
                "forward_evidence": dict(forward),
                **forward,
            },
        }
        with AxiomStore(":memory:") as store:
            ranker = CandidateCanaryRanker(store, clock=lambda: NOW)
            nested_hashes = ranker._snapshot_hashes(
                nested,
                nested["payload"],
                {},
                "sha256:frozen",
            )
            duplicate_hashes = ranker._snapshot_hashes(
                duplicated,
                duplicated["payload"],
                {},
                "sha256:frozen",
            )
            self.assertIsNotNone(nested_hashes)
            self.assertIsNotNone(duplicate_hashes)
            assert nested_hashes is not None
            assert duplicate_hashes is not None
            self.assertEqual(nested_hashes[1:], duplicate_hashes[1:])

            mutated = {
                **duplicated,
                "payload": {**duplicated["payload"], "forward_expectancy": 0.05},
            }
            mutated_hashes = ranker._snapshot_hashes(
                mutated,
                mutated["payload"],
                {},
                "sha256:frozen",
            )
            self.assertIsNotNone(mutated_hashes)
            assert mutated_hashes is not None
            self.assertEqual(mutated_hashes[1], duplicate_hashes[1])
            self.assertNotEqual(mutated_hashes[2], duplicate_hashes[2])


    def test_rejected_candidate_is_not_executable_after_ranker_tick(self) -> None:
        rejected_payload = {**payload(), "candidate_id": "candidate-1"}
        with AxiomStore(":memory:") as store:
            ranker = CandidateCanaryRanker(store, clock=lambda: NOW)
            store.save_candidate_lifecycle(
                "candidate-1",
                CandidateStage.IDEA.value,
                rejected_payload,
                timestamp=NOW,
            )
            store.save_candidate_lifecycle(
                "candidate-1",
                CandidateStage.REJECTED.value,
                rejected_payload,
                from_stage=CandidateStage.IDEA.value,
                timestamp=NOW,
            )
            with store.connection:
                store.connection.execute(
                    "INSERT INTO canary_selection("
                    "singleton,ranking_run_id,candidate_id,component_scores_json,"
                    "evidence_versions_json,reason,selected_at,selection_status,"
                    "selection_valid,last_selected_candidate"
                    ") VALUES(1,?,?,?,?,?,?,?,?,?)",
                    (
                        "previous-run",
                        "candidate-1",
                        "{}",
                        "{}",
                        "SELECTED_WINNER",
                        NOW.isoformat(),
                        "CURRENT",
                        1,
                        "candidate-1",
                    ),
                )

            result = ranker.evaluate_and_select(NOW)

            self.assertIsNone(result["selected_candidate"])
            self.assertEqual(result["selection_status"], "STALE")
            self.assertFalse(result["selection_valid"])
            self.assertEqual(
                result["selection_invalidation_reason"],
                "LIFECYCLE_REJECTED",
            )
            self.assertIsNone(ranker.current_selection())
            self.assertIsNone(
                store.connection.execute(
                    "SELECT candidate_id FROM canary_eligibility "
                    "WHERE candidate_id=?",
                    ("candidate-1",),
                ).fetchone()
            )


if __name__ == "__main__":
    unittest.main()
