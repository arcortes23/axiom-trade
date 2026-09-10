from __future__ import annotations

from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
import shutil
from unittest.mock import patch
import tempfile
import unittest

from axiom.auto_canary import AutonomousCanaryWorker
from axiom.canary import (
    AUTONOMOUS_MICRO_LIVE,
    CanaryBlocked,
    CanaryService,
    CredentialStore,
    PolymarketClobV2Venue,
)
from axiom.dashboard import DashboardData

from axiom.lifecycle import CandidateLifecycleManager, CandidateStage
from axiom.node import NodeConfig, ResearchNode
from axiom.ranker import CandidateCanaryRanker
from axiom.storage import AxiomStore
from axiom.experiment_plan import normalize_market_scope
from axiom.market_scope import resolve_market_scope


def _canonical_unresolved_scope() -> tuple[dict[str, object], str, str]:
    policy = normalize_market_scope(
        {
            "schema_version": "1",
            "mode": "RULE_BASED_MARKETS",
            "instrument": "POLYMARKET",
            "categories": ["no-such-forward-market"],
            "market_ids": [],
            "filters": {"category": "no-such-forward-market"},
            "regime_restrictions": {},
            "provenance": "canonical",
        }
    )
    return policy.as_dict(), policy.scope_hash, policy.scope_version

T0 = datetime(2026, 1, 2, 12, tzinfo=timezone.utc)
class _FixtureCredentialStore(CredentialStore):
    _VALUES = {
        "private_key": "fixture-private-key",
        "wallet_address": "0x0000000000000000000000000000000000000001",
    }

    def configured(self, **_kwargs: object) -> bool:
        return True

    def load(self, **_kwargs: object) -> dict[str, str]:
        return dict(self._VALUES)




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
        scope, scope_hash, scope_version = _canonical_unresolved_scope()
        lifecycle.register_idea(
            candidate_id,
            {
                "market_type": "prediction",
                "market_scope": scope,
                "market_scope_hash": scope_hash,
                "market_scope_version": scope_version,
                "plan_hash": "sha256:truthfulness-plan",
                "dataset_selector": {
                    "dataset_id": "prediction-history",
                    "dataset_version": "v1",
                    "source_type": "HISTORICAL",
                },
                "dataset_attestation": {
                    "dataset_id": "prediction-history",
                    "dataset_version": "v1",
                    "status": "CURRENT",
                    "policy_version": "prediction-integrity-v1",
                    "attestation_hash": "sha256:truthfulness-attestation",
                },
            },
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
        store.save_market_scope_resolution(
            resolve_market_scope(
                candidate_id,
                {"market_scope": scope},
                [],
                resolved_at=T0,
            )
        )

        service = CanaryService(store, clock=lambda: T0)
        result = service.evaluate_signal(candidate_id)

        self.assertEqual(
            result["reason_code"], "SCOPE_RESOLUTION_ZERO_MATCHES"
        )
        self.assertIsNone(result["market_id"])
        self.assertEqual(
            result["evidence"]["scope_resolution_reason"],
            "SCOPE_RESOLUTION_ZERO_MATCHES",
        )
        persisted = service.list_signal_evaluations("authority-unresolved")
        self.assertEqual(len(persisted), 1)
        self.assertEqual(
            persisted[0]["reason_code"], "SCOPE_RESOLUTION_ZERO_MATCHES"
        )
        self.assertEqual(
            persisted[0]["evidence"]["scope_resolution_reason"],
            "SCOPE_RESOLUTION_ZERO_MATCHES",
        )

    def test_unknown_submission_is_not_successful_and_is_not_resubmitted(self) -> None:
        store = self.make_store()
        service = CanaryService(
            store,
            credentials=_FixtureCredentialStore(),
            clock=lambda: T0,
        )
        settings = service.settings.snapshot()
        service.enable_autonomous_micro_live(
            venue="polymarket",
            config_id=str(settings["config_id"]),
            expected_generation=int(settings["generation"]),
        )
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

    def test_isolated_profile_blocks_real_transport_before_and_after_copied_enable(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            source_path = Path(temporary) / "source.sqlite3"
            copied_path = Path(temporary) / "copied.sqlite3"
            source_store = AxiomStore(str(source_path))
            try:
                source_service = CanaryService(
                    source_store,
                    credentials=_FixtureCredentialStore(),
                    clock=lambda: T0,
                )
                with patch.dict(os.environ, {"AXIOM_EXECUTION_PROFILE": "isolated"}):
                    with self.assertRaisesRegex(
                        CanaryBlocked, "ISOLATED_EXECUTION_PROFILE"
                    ):
                        CredentialStore().configured()
                    with self.assertRaisesRegex(
                        CanaryBlocked, "ISOLATED_EXECUTION_PROFILE"
                    ):
                        PolymarketClobV2Venue().geoblock()
                with patch.dict(os.environ, {"AXIOM_EXECUTION_PROFILE": "production"}):
                    settings = source_service.settings.snapshot()
                    source_service.enable_autonomous_micro_live(
                        venue="polymarket",
                        config_id=str(settings["config_id"]),
                        expected_generation=int(settings["generation"]),
                    )
            finally:
                source_store.close()
            shutil.copyfile(source_path, copied_path)

            copied_store = AxiomStore(str(copied_path))
            try:
                copied_service = CanaryService(
                    copied_store,
                    credentials=_FixtureCredentialStore(),
                    clock=lambda: T0,
                )
                with patch.dict(os.environ, {"AXIOM_EXECUTION_PROFILE": "isolated"}):
                    self.assertEqual(
                        copied_service.status()["micro_live_canary"],
                        AUTONOMOUS_MICRO_LIVE,
                    )
                    with self.assertRaisesRegex(
                        CanaryBlocked, "ISOLATED_EXECUTION_PROFILE"
                    ):
                        PolymarketClobV2Venue().account()
            finally:
                copied_store.close()

    def test_dashboard_exposes_stale_and_error_reasons_not_no_signal(self) -> None:
        store = self.make_store()
        service = CanaryService(store, clock=lambda: T0)
        service.record_autonomous_decision(
            next_decision="WAIT_FOR_FRESH_INPUTS",
            blocker="STALE_INPUT",
            worker_status="DEGRADED",
            timestamp=T0,
        )
        service.publish_readiness_snapshot(reason="STALE_INPUT")
        store.save_polymarket_market_metadata(
            "ui-error-market",
            {
                "market_id": "ui-error-market",
                "active": True,
                "closed": False,
                "snapshot": {"settlement": "open"},
                "source_type": "FORWARD_COLLECTED",
            },
            observed_at=T0,
        )
        store.save_polymarket_snapshot(
            "ui-error-snapshot",
            "ui-error-market",
            T0,
            T0,
            {
                "market_id": "ui-error-market",
                "source_type": "FORWARD_COLLECTED",
                "snapshot": {
                    "settlement": "open",
                    "expiry": (T0 + timedelta(hours=2)).isoformat(),
                },
            },
            quality="TIMESTAMPED_DEPTH",
            source_type="FORWARD_COLLECTED",
        )
        store.save_collection_error(
            "ui-error-market",
            T0,
            kind="TRANSPORT_ERROR",
            detail="fixture transport unavailable",
            source_type="FORWARD_COLLECTED",
        )
        node = ResearchNode(
            NodeConfig(":memory:", crypto_enabled=False),
            provider=object(),
            store=store,
            clock=lambda: T0,
        )
        self.addCleanup(node.stop)
        self.assertTrue(node._run_health_monitor())

        canonical = service.status_report()
        self.assertEqual(canonical["worker"]["blocker"], "STALE_INPUT")
        self.assertEqual(canonical["blocker"], "STALE_INPUT")
        dashboard = DashboardData(store=store, clock=lambda: T0)
        canary = dashboard.canary_data()
        self.assertEqual(canary["canary"]["autonomous"]["blocker"], "STALE_INPUT")
        self.assertNotEqual(canary["canary"]["autonomous"]["blocker"], "NO_SIGNAL")
        overview = dashboard.overview_summary()
        self.assertEqual(overview["health_reason_code"], "CURRENT_COLLECTION_FAILURES")
        self.assertEqual(overview["historical_error_count"], 0)
        self.assertNotEqual(overview["health_reason_code"], "NO_SIGNAL")

if __name__ == "__main__":
    unittest.main()
