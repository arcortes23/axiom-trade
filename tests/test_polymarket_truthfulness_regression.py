from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch
import tempfile
import unittest

from axiom.auto_canary import AutonomousCanaryWorker
from axiom.canary import (
    AUTONOMOUS_MICRO_LIVE,
    CanaryBlocked,
    CanaryService,
    CredentialStore,
)
from axiom.lifecycle import CandidateLifecycleManager, CandidateStage
from axiom.ranker import CandidateCanaryRanker
from axiom.storage import AxiomStore



T0 = datetime(2026, 1, 2, 12, tzinfo=timezone.utc)


class PolymarketTruthfulnessRegressionTests(unittest.TestCase):
    def make_store(self) -> AxiomStore:
        temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(temporary_directory.cleanup)
        store = AxiomStore(Path(temporary_directory.name) / "truthfulness.sqlite")
        self.addCleanup(store.close)
        return store

    def test_unresolved_authority_is_public_and_durable(self) -> None:
        store = self.make_store()
        lifecycle = CandidateLifecycleManager(store)
        candidate_id = "authority-unresolved"
        lifecycle.register_idea(
            candidate_id,
            {"filters": {"category": "no-such-forward-market"}},
        )
        lifecycle.advance(
            candidate_id,
            CandidateStage.SCHEMA_VALIDATED,
            {"schema_valid": True},
        )
        lifecycle.advance(
            candidate_id,
            CandidateStage.BACKTESTED,
            {"backtest_complete": True},
        )
        lifecycle.advance(
            candidate_id,
            CandidateStage.VALIDATED,
            {"validation_complete": True, "holdout_used": False},
        )
        lifecycle.advance(
            candidate_id,
            CandidateStage.ROBUSTNESS_CHECKED,
            {"robustness_passed": True},
        )
        lifecycle.advance(
            candidate_id,
            CandidateStage.FROZEN,
            {
                "frozen": True,
                "holdout_used": False,
                "strategy_hash": "strategy-hash",
                "model_hash": "model-hash",
                "config_hash": "config-hash",
                "risk_snapshot": {"max_loss": 1},
            },
        )

        service = CanaryService(store, clock=lambda: T0)
        result = service.evaluate_signal(candidate_id)

        self.assertEqual(
            result["reason_code"], "CANDIDATE_FORWARD_MARKET_UNRESOLVED"
        )
        self.assertIsNone(result["market_id"])
        self.assertEqual(
            result["evidence"]["authority_reason_code"],
            "CANDIDATE_FORWARD_MARKET_UNRESOLVED",
        )
        persisted = service.list_signal_evaluations("authority-unresolved")
        self.assertEqual(len(persisted), 1)
        self.assertEqual(
            persisted[0]["reason_code"], "CANDIDATE_FORWARD_MARKET_UNRESOLVED"
        )
        self.assertEqual(
            persisted[0]["evidence"]["authority_reason_code"],
            "CANDIDATE_FORWARD_MARKET_UNRESOLVED",
        )

    def test_unknown_submission_is_not_successful_and_is_not_resubmitted(self) -> None:
        store = self.make_store()
        service = CanaryService(store, clock=lambda: T0)
        service.enable_autonomous_micro_live()
        candidate_id = "unknown-submission"
        row = {
            "candidate_id": candidate_id,
            "qualification_hash": "qualification-hash",
            "cluster_key": "cluster",
            "cluster_representative": 1,
            "rank": 1,
            "total_score": 1.0,
        }
        ready_evaluation = {
            "candidate_id": candidate_id,
            "reason_code": "READY_SIGNAL",
            "signal": {
                "status": "READY",
                "signal_id": "unknown-signal",
                "candidate_id": candidate_id,
            },
        }
        unknown_evaluation = {
            "candidate_id": candidate_id,
            "reason_code": "COLLECTOR_CANDIDATE_HEALTH_BLOCKED",
            "signal": {
                "status": "UNKNOWN",
                "signal_id": "unknown-signal",
                "candidate_id": candidate_id,
            },
        }
        worker = AutonomousCanaryWorker(
            store,
            clock=lambda: T0,
            venue_factory=lambda: object(),
        )
        worker._current_rankings = lambda *_args, **_kwargs: [row]

        with patch.object(
            CandidateCanaryRanker,
            "evaluate_and_select",
            return_value={
                "ranking_run_id": "ranking-run",
                "eligible_count": 1,
                "rankable_count": 1,
            },
        ), patch.object(
            CanaryService,
            "evaluate_signal",
            side_effect=[ready_evaluation, unknown_evaluation],
        ), patch.object(
            CanaryService,
            "bind_autonomous_actionable_candidate",
            return_value={},
        ), patch.object(
            CanaryService,
            "submit_signal",
            side_effect=CanaryBlocked("CANARY_SUBMISSION_UNKNOWN"),
        ) as submit_signal, patch.object(
            CredentialStore, "configured", return_value=True
        ):
            first = worker.tick(now=T0)
            first_report = CanaryService(store, clock=lambda: T0).status_report()
            second = worker.tick(now=T0)

        self.assertEqual(first["status"], "BLOCKED")
        self.assertEqual(first["decision"], "UNKNOWN_NO_RETRY")
        self.assertEqual(second["decision"], "UNKNOWN_NO_RETRY")
        submit_signal.assert_called_once()

        worker_state = first_report["worker"]
        self.assertEqual(worker_state["worker_status"], "DEGRADED")
        self.assertEqual(worker_state["blocker"], "UNKNOWN_NO_RETRY")
        self.assertEqual(
            worker_state["last_error_code"], "CANARY_SUBMISSION_UNKNOWN"
        )
        self.assertIsNone(worker_state["last_successful_tick"])
        self.assertEqual(worker_state["consecutive_failures"], 1)
        self.assertEqual(worker_state["next_retry_at"], "2026-01-02T12:00:01+00:00")
        self.assertEqual(worker_state["last_tick_completed_at"], T0.isoformat())
        self.assertEqual(
            first_report["autonomous"]["last_error_code"],
            "CANARY_SUBMISSION_UNKNOWN",
        )
        self.assertEqual(first_report["micro_live_canary"], AUTONOMOUS_MICRO_LIVE)


if __name__ == "__main__":
    unittest.main()
