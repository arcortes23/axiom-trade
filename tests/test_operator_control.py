from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import Mock, patch
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from types import SimpleNamespace

from axiom.canary import AUTONOMOUS_CANARY_LIMITS, CanaryService, CredentialStore
from axiom.dashboard import DashboardData, DashboardServer, _DashboardHandler
from axiom.operator import BOOTSTRAP_JOB_NAME, OperatorControlPlane
from axiom.ranker import CandidateCanaryRanker
from axiom.storage import AxiomStore


class OperatorControlTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.db = str(Path(self.tempdir.name) / "operator.sqlite")
        self.store = AxiomStore(self.db)
        self.control = OperatorControlPlane(self.store)
        self.server = DashboardServer(
            port=0,
            data=DashboardData(store=self.store, control=self.control),
        ).start()
        self.addCleanup(self.store.close)
        self.addCleanup(self.server.stop)

    def _post(self, body: dict, *, token: str | None = None) -> tuple[int, dict]:
        assert self.server.url is not None
        headers = {"Content-Type": "application/json"}
        if token is not None:
            headers["X-Axiom-Control-Token"] = token
        request = Request(
            self.server.url + "/api/control",
            data=json.dumps(body).encode(),
            headers=headers,
            method="POST",
        )
        with urlopen(request, timeout=3) as response:
            return response.status, json.loads(response.read())

    def test_localhost_control_requires_token_and_allowlist(self) -> None:
        assert self.server._server is not None
        assert self.server.url is not None
        body = {"action": "hermes.pause"}
        with self.assertRaises(HTTPError) as context:
            self._post(body)
        self.assertEqual(context.exception.code, 403)
        status, result = self._post(body, token=self.server._server.control_token)
        self.assertEqual(status, 200)
        self.assertTrue(result["ok"])
        self.assertEqual(self.store.get_scheduler_state("hermes-control")["status"], "PAUSED")
        self.assertFalse(_DashboardHandler._loopback_client(type("Request", (), {"client_address": ("192.0.2.1", 1)})()))

    def test_no_arbitrary_command_execution(self) -> None:
        launcher = Mock()
        control = OperatorControlPlane(self.store, node_launcher=launcher)
        result = control.execute("shell.exec", "python", confirm="")
        self.assertFalse(result["ok"])
        self.assertEqual(result["reason"], "ACTION_NOT_ALLOWED")
        launcher.assert_not_called()
        self.assertNotIn("command", json.dumps(result).lower())

    def test_duplicate_node_prevention(self) -> None:
        launcher = Mock()
        control = OperatorControlPlane(self.store, node_launcher=launcher)
        running = {"status": "RUNNING", "pid": 123, "worker_identity_valid": True}
        with patch.object(control, "_node_status", return_value=running):
            self.assertEqual(control.ensure_node(), running)
        launcher.assert_not_called()

    def test_fixed_hermes_adapter_operations_are_persisted(self) -> None:
        self.assertTrue(self.control.execute("hermes.pause")["ok"])
        self.assertEqual(self.control.status()["hermes"]["status"], "PAUSED")
        self.assertTrue(self.control.execute("hermes.resume")["ok"])
        self.assertEqual(self.control.status()["hermes"]["status"], "ACTIVE")
        processor = Mock()
        processor.process_pending.return_value = SimpleNamespace(claimed=1, completed=1, rejected=0, failed=0)
        with patch("axiom.operator.AutonomousResearchProcessor", return_value=processor):
            result = self.control.execute("hermes.run_now")
        self.assertTrue(result["ok"])
        processor.process_pending.assert_called_once_with(worker="operator-hermes")
        self.assertNotIn("argv", json.dumps(result).lower())

    def test_bootstrap_duplicate_and_resume_behavior(self) -> None:
        snapshot = SimpleNamespace(selected_symbols=("BTCUSDT",))
        started = threading.Event()
        release = threading.Event()

        def blocked_worker(**_: object) -> None:
            started.set()
            release.wait(2)

        with patch("axiom.operator.load_crypto_universe", return_value=snapshot), patch.object(
            self.control, "_bootstrap_worker", side_effect=blocked_worker
        ):
            first = self.control.start_bootstrap(resume=False)
            self.assertEqual(first["status"], "RUNNING")
            self.assertTrue(started.wait(1))
            with self.assertRaisesRegex(RuntimeError, "BOOTSTRAP_ALREADY_RUNNING"):
                self.control.start_bootstrap(resume=True)
            release.set()
            thread = self.control._bootstrap_threads[BOOTSTRAP_JOB_NAME]
            thread.join(2)
        self.store.set_operator_job(
            BOOTSTRAP_JOB_NAME,
            "FAILED",
            {"total_symbols": 1, "total_datasets": 4},
            pid=None,
            last_error="BOOTSTRAP_DATASET_FAILED",
            resumable=True,
        )
        with patch("axiom.operator.load_crypto_universe", return_value=snapshot), patch.object(
            self.control, "_bootstrap_worker", return_value=None
        ):
            resumed = self.control.start_bootstrap(resume=True)
            self.assertEqual(resumed["status"], "RUNNING")
            thread = self.control._bootstrap_threads[BOOTSTRAP_JOB_NAME]
            thread.join(2)

    def _seed_candidate(
        self,
        candidate_id: str = "candidate-1",
        *,
        store: AxiomStore | None = None,
        timestamp: datetime | None = None,
    ) -> None:
        target = store or self.store
        dataset_timestamp = datetime(2025, 1, 1, tzinfo=timezone.utc)
        target.save_dataset(
            "dataset-1",
            "dataset-v1",
            [{"timestamp": dataset_timestamp.isoformat(), "price": 0.5, "source_type": "HISTORICAL"}],
        )
        target.save_dataset_catalog(
            "dataset-1",
            "dataset-v1",
            provider="polymarket",
            instrument="POLYMARKET",
            market_type="prediction",
            timeframe="event",
            start_timestamp=dataset_timestamp,
            end_timestamp=dataset_timestamp,
            row_count=1,
            completeness=1.0,
            quality="PRICE_PROXY",
            source_type="HISTORICAL",
            snapshot_id="dataset-1:dataset-v1",
            metadata={
                "provider": "polymarket",
                "source_type": "HISTORICAL",
                "research_quality": "PRICE_PROXY",
                "historical_order_book_available": False,
            },
        )
        strategy_hash = "strategy-hash"
        model_hash = "model-hash"
        config_hash = "config-hash"
        frozen_hash = hashlib.sha256(f"{strategy_hash}|{model_hash}|{config_hash}".encode()).hexdigest()
        payload = {
            "candidate_id": candidate_id,
            "strategy_id": "strategy-1",
            "experiment_family": "test-family",
            "market_type": "prediction",
            "instrument": "MARKET-1",
            "dataset_id": "dataset-1",
            "dataset_version": "dataset-v1",
            "dataset_provenance": {
                "dataset_id": "dataset-1",
                "dataset_version": "dataset-v1",
                "source_type": "HISTORICAL",
                "time_split": "train-validation-holdout",
            },
            "source_type": "HISTORICAL",
            "timeframe": "event",
            "strategy_hash": strategy_hash,
            "model_hash": model_hash,
            "config_hash": config_hash,
            "frozen_hash": frozen_hash,
            "schema_validated": True,
            "historical_backtest_passed": True,
            "validation_passed": True,
            "robustness_passed": True,
            "data_quality_passed": True,
            "minimum_sample_check": {
                "passed": True,
                "count": 30,
                "trades": 0,
                "min_observations": 30,
                "min_trades": 0,
                "checks": {"observations": True, "trades": True},
            },
            "holdout_used": False,
            "frozen": True,
            "critical_error": None,
        }
        target.save_candidate_lifecycle(candidate_id, "IDEA", payload, timestamp=timestamp)
        target.save_candidate_lifecycle(candidate_id, "FROZEN", payload, from_stage="IDEA", timestamp=timestamp)
    def test_dashboard_restart_hydrates_persisted_overview_and_canary(self) -> None:
        restart_db = str(Path(self.tempdir.name) / "restart.sqlite")
        timestamp = datetime(2026, 1, 2, 12, tzinfo=timezone.utc)
        secrets = ("FAKE_PRIVATE_KEY", "FAKE_WALLET_ADDRESS", "FAKE_RELAYER_KEY")
        credentials = Mock()
        credentials.configured.return_value = True
        credentials.load.return_value = {
            "private_key": secrets[0],
            "wallet_address": secrets[1],
            "relayer_api_key": secrets[2],
        }
        credentials.safe_projection.return_value = {
            "configured": True,
            "status": "CONFIGURED",
            "secret_values_exposed": False,
        }

        with patch("axiom.storage._now_iso", return_value=timestamp.isoformat()):
            with AxiomStore(restart_db) as original_store:
                original_control = OperatorControlPlane(original_store)
                original_dashboard = DashboardData(store=original_store, control=original_control)
                self._seed_candidate("winner", store=original_store, timestamp=timestamp)
                winner = original_store.load_candidate_lifecycle("winner")
                self.assertIsInstance(winner, dict)
                winner_payload = dict(winner["payload"])

                def rankable_payload(candidate_id: str, score: float) -> dict[str, object]:
                    return {
                        **winner_payload,
                        "candidate_id": candidate_id,
                        "validation_expectancy": score,
                        "validation_confidence_lower_bound": score,
                        "validation_stability": 0.90,
                        "validation_calibration": 0.90,
                        "validation_sample_count": 100,
                        "validation_trade_count": 50,
                        "validation_execution_quality": 0.90,
                    }

                winner_payload = rankable_payload("winner", 0.42)
                original_store.save_candidate_lifecycle(
                    "winner",
                    "FROZEN",
                    winner_payload,
                    from_stage="FROZEN",
                    timestamp=timestamp,
                )
                runner_payload = rankable_payload("runner", 0.21)
                original_store.save_candidate_lifecycle("runner", "IDEA", runner_payload, timestamp=timestamp)
                original_store.save_candidate_lifecycle(
                    "runner",
                    "FROZEN",
                    runner_payload,
                    from_stage="IDEA",
                    timestamp=timestamp,
                )

                canary_service = CanaryService(original_store, clock=lambda: timestamp)
                ranking = CandidateCanaryRanker(
                    original_store,
                    service=canary_service,
                    clock=lambda: timestamp,
                ).evaluate_and_select(timestamp)
                self.assertEqual(ranking["selected_candidate"], "winner")
                selected = ranking["selected"]
                self.assertIsInstance(selected, dict)
                selected_score = selected["total_score"]
                self.assertIsInstance(selected_score, float)

                # Use the typed operator action to persist the disabled state and
                # its restart-safe autonomous decision/blocker.
                self.assertTrue(original_control.execute("canary.disarm", confirm="DISARM")["ok"])

                queue_item = original_store.enqueue_research_item(
                    "hypothesis",
                    {
                        "dataset_id": "dataset-1",
                        "dataset_version": "dataset-v1",
                        "family": "test-family",
                        "reason_code": "HERMES_FIXTURE_COMPLETE",
                    },
                    dedupe_key="hermes-restart-fixture",
                    source="hermes",
                    author="fixture",
                    item_id="hermes-restart-item",
                    available_at=timestamp,
                )
                self.assertEqual(queue_item["status"], "PENDING")
                claimed = original_store.claim_research_item("hermes-fixture", now=timestamp)
                self.assertIsNotNone(claimed)
                original_store.complete_research_item(
                    "hermes-restart-item",
                    "COMPLETED",
                    result={
                        "dataset_id": "dataset-1",
                        "dataset_version": "dataset-v1",
                        "family": "test-family",
                        "reason_code": "HERMES_FIXTURE_COMPLETE",
                    },
                    now=timestamp + timedelta(seconds=1),
                    worker="hermes-fixture",
                )

            # The context boundary above closes the original store before the
            # fresh objects below are created.
            with AxiomStore(restart_db) as reopened_store:
                reopened_control = OperatorControlPlane(reopened_store)
                reopened_dashboard = DashboardData(store=reopened_store, control=reopened_control)
                before_hydration_changes = reopened_store.connection.total_changes
                with patch("axiom.operator.CredentialStore", return_value=credentials), patch(
                    "axiom.canary.CredentialStore", return_value=credentials
                ):
                    overview = reopened_dashboard.overview_summary()
                    operator = reopened_dashboard.operator_data()
                    canary = reopened_dashboard.canary_data()
                self.assertEqual(reopened_store.connection.total_changes, before_hydration_changes)

        self.assertLessEqual(len(overview["latest_activity"]), 8)
        self.assertEqual(overview["coverage"]["historical_count"], 1)
        self.assertEqual(overview["coverage"]["historical_rows"], 1)
        self.assertEqual(overview["lifecycle_funnel"]["FROZEN"], 2)
        self.assertTrue(overview["latest_activity"])
        self.assertEqual(overview["research_cards"]["canary_eligible"], 2)
        self.assertEqual(
            {item["candidate_id"] for item in overview["latest_candidates"]},
            {"winner", "runner"},
        )
        overview_hermes = overview["hermes_latest_outcome"]
        self.assertIsInstance(overview_hermes, dict)
        self.assertEqual(overview_hermes["status"], "COMPLETED")
        self.assertEqual(overview_hermes["outcome_type"], "EXPERIMENT_COMPLETED")

        self.assertEqual(operator["research_cards"]["canary_eligible"], 2)
        self.assertEqual(operator["coverage"]["historical_count"], 1)
        self.assertEqual(operator["coverage"]["historical_rows"], 1)
        self.assertEqual(operator["lifecycle_funnel"]["FROZEN"], 2)
        latest_candidates = operator["latest_candidates"]
        self.assertLessEqual(len(latest_candidates), 10)
        self.assertEqual(
            {item["candidate_id"] for item in latest_candidates},
            {"winner", "runner"},
        )
        hermes_outcome = operator["hermes_latest_outcome"]
        self.assertIsInstance(hermes_outcome, dict)
        self.assertEqual(hermes_outcome["status"], "COMPLETED")
        self.assertEqual(hermes_outcome["outcome_type"], "EXPERIMENT_COMPLETED")
        self.assertEqual(hermes_outcome["reason_code"], "HERMES_FIXTURE_COMPLETE")
        self.assertEqual(hermes_outcome["dataset_id"], "dataset-1")
        self.assertEqual(hermes_outcome["dataset_version"], "dataset-v1")
        self.assertTrue(any(item["kind"] == "research" for item in operator["latest_activity"]))

        status = canary["canary"]
        self.assertEqual(status["micro_live_canary"], "DISARMED")
        self.assertEqual(status["display_state"], "DISABLED")
        self.assertEqual(status["risk_envelope"], dict(AUTONOMOUS_CANARY_LIMITS))
        self.assertEqual(status["risk_limits"], dict(AUTONOMOUS_CANARY_LIMITS))
        self.assertEqual(status["eligible_count"], 2)
        self.assertEqual(status["rankable_count"], 2)
        self.assertEqual(status["execution_event_count"], 0)
        self.assertEqual(status["real_execution_events"], 0)
        self.assertEqual(status["trades"], [])
        self.assertIsNone(canary["canary_signal"])
        self.assertEqual(canary["research_cards"]["canary_eligible"], 2)
        self.assertEqual(canary["candidate_status"]["rankable"], 2)
        self.assertEqual(canary["real_execution_events"], 0)

        autonomous = canary["autonomous_canary"]
        self.assertEqual(autonomous["selected_candidate"], "winner")
        self.assertEqual(autonomous["rank"], 1)
        self.assertEqual(autonomous["score"], selected_score)
        self.assertEqual(autonomous["selection_reason"], "SELECTED_WINNER")
        self.assertEqual(autonomous["eligible_count"], 2)
        self.assertEqual(autonomous["rankable_count"], 2)
        self.assertEqual(autonomous["historical_data_integrity"], "PASS")
        self.assertEqual(autonomous["historical_execution_fidelity"], "PRICE_PROXY · LIMITED")
        self.assertEqual(autonomous["current_execution_evidence"], "CURRENT_ORDER_BOOK_REQUIRED")
        self.assertEqual(autonomous["next_decision"], "ENABLE AUTO CANARY")
        self.assertEqual(autonomous["blocker"], "AUTONOMOUS_CANARY_DISABLED")

        for payload in (operator, canary):
            projected = payload["credentials"]
            self.assertEqual(
                set(projected),
                {"configured", "status", "secret_values_exposed"},
            )
            self.assertTrue(projected["configured"])
            self.assertEqual(projected["status"], "CONFIGURED")
            self.assertFalse(projected["secret_values_exposed"])
        encoded = json.dumps({"operator": operator, "canary": canary}, default=str)
        for secret in secrets:
            self.assertNotIn(secret, encoded)

    def test_candidate_provenance_is_explicit_and_complete(self) -> None:
        self._seed_candidate()
        data = DashboardData(store=self.store, control=self.control)
        row = data._candidate_row(self.store.load_candidate_lifecycle("candidate-1"))
        self.assertEqual(row["market"], "prediction")
        self.assertEqual(row["provenance"]["dataset_id"], "dataset-1")
        detail = data.strategy_detail("candidate-1")
        self.assertEqual(detail["provenance"]["dataset_version"], "dataset-v1")
        missing = data._candidate_row({"candidate_id": "missing", "payload": {}})
        self.assertEqual(missing["market"], "MISSING PROVENANCE")
        self.assertEqual(missing["provenance"]["status"], "MISSING PROVENANCE")

    def test_canary_eligibility_uses_backend_and_never_trades(self) -> None:
        self._seed_candidate()
        dry_run = CanaryService(self.store, initialize=False).validate_eligibility("candidate-1")
        self.assertTrue(dry_run["eligible"])
        service = Mock()
        service.validate_eligibility.return_value = {"candidate_id": "candidate-1", "eligible": True, "checks": []}
        with patch("axiom.operator.CanaryService", return_value=service):
            verified = self.control.execute("canary.eligibility.verify", "candidate-1")
            marked = self.control.execute(
                "canary.eligibility.mark", "candidate-1", confirm="MARK CANARY ELIGIBLE"
            )
        self.assertTrue(verified["ok"])
        self.assertTrue(marked["ok"])
        service.validate_eligibility.assert_called()
        service.mark_eligible.assert_called_once_with("candidate-1")
        service.arm.assert_not_called()
        service.submit.assert_not_called()

    def test_arm_requires_exact_confirmation_and_has_no_submit_path(self) -> None:
        credentials = Mock()
        credentials.configured.return_value = True
        service = Mock()
        service.arm.return_value = {"micro_live_canary": "ARMED", "limits": {"target_notional_usd": "1"}}
        with patch("axiom.operator.CredentialStore", return_value=credentials), patch(
            "axiom.operator.CanaryService", return_value=service
        ):
            denied = self.control.execute("canary.arm", "candidate-1", confirm="arm")
            armed = self.control.execute("canary.arm", "candidate-1", confirm="ARM")
        self.assertFalse(denied["ok"])
        self.assertEqual(denied["reason"], "EXACT_CONFIRMATION_REQUIRED")
        self.assertTrue(armed["ok"])
        service.arm.assert_called_once()
        service.submit.assert_not_called()
        self.assertNotIn("canary.submit", json.dumps(armed))

    def test_secret_isolation_and_audit_activity(self) -> None:
        credentials = Mock()
        credentials.configured.return_value = True
        credentials.load.return_value = {"private_key": "PRIVATE", "wallet_address": "WALLET"}
        with patch("axiom.operator.CredentialStore", return_value=credentials):
            status = self.control.status()
        encoded = json.dumps(status, default=str)
        self.assertNotIn("PRIVATE", encoded)
        self.assertNotIn("WALLET", encoded)
        self.assertFalse(status["credentials"]["secret_values_exposed"])
        self.control.execute("hermes.pause")
        self.control.execute("not.allowed")
        actions = self.store.list_operator_actions()
        self.assertGreaterEqual(len(actions), 2)
        activity = self.store.paginate_research_activity(kind="operator", page_size=25)
        self.assertGreaterEqual(activity["total"], 2)
    def test_manual_arm_target_is_distinct_from_persisted_autonomous_winner(self) -> None:
        timestamp = datetime(2026, 1, 2, 12, tzinfo=timezone.utc)
        self._seed_candidate("A", timestamp=timestamp)
        candidate_b = dict(self.store.load_candidate_lifecycle("A")["payload"])
        candidate_b["candidate_id"] = "B"
        self.store.save_candidate_lifecycle(
            "B",
            "IDEA",
            candidate_b,
            timestamp=timestamp,
        )
        self.store.save_candidate_lifecycle(
            "B",
            "FROZEN",
            candidate_b,
            from_stage="IDEA",
            timestamp=timestamp,
        )
        credentials = Mock()
        credentials.configured.return_value = True
        service = CanaryService(
            self.store,
            credentials=credentials,
            clock=lambda: timestamp,
        )
        service.mark_eligible("A")
        service.mark_eligible("B")
        self.store.polymarket_health = lambda **_: {"grade": "A", "errors": 0}
        with self.store.connection:
            self.store.connection.execute(
                "INSERT INTO canary_selection("
                "singleton,ranking_run_id,candidate_id,rank,total_score,"
                "component_scores_json,evidence_versions_json,reason,selected_at) "
                "VALUES(1,?,?,?,?,?,?,?,?)",
                (
                    "rank-manual-target",
                    "B",
                    1,
                    0.99,
                    "{}",
                    "{}",
                    "SELECTED_WINNER",
                    timestamp.isoformat(),
                ),
            )

        class ArmVenue:
            @staticmethod
            def geoblock() -> dict[str, bool]:
                return {"blocked": False, "close_only": False}

        armed = service.arm("A", venue=ArmVenue(), credentials_configured=True)
        self.assertEqual(armed["candidate"], "A")
        self.assertEqual(armed["autonomous"]["selected_candidate"], "B")

        check_a = service.check(candidate_id="A", venue=ArmVenue())
        check_b = service.check(candidate_id="B", venue=ArmVenue())
        self.assertNotIn("CANDIDATE_NOT_ARMED", check_a["failures"])
        self.assertIn("CANDIDATE_NOT_ARMED", check_b["failures"])

        with patch.object(
            CredentialStore,
            "safe_projection",
            return_value={
                "configured": False,
                "status": "NOT CONFIGURED",
                "secret_values_exposed": False,
            },
        ):
            dashboard = DashboardData(store=self.store, control=self.control).canary_data()
        self.assertEqual(dashboard["canary"]["candidate"], "A")
        self.assertEqual(dashboard["autonomous_canary"]["selected_candidate"], "B")


    def test_get_dashboard_is_side_effect_free_and_survives_control_failure(self) -> None:
        before_changes = self.store.connection.total_changes
        before_actions = len(self.store.list_operator_actions())
        response = self.store
        DashboardData(store=response, control=self.control).operator_data()
        self.assertEqual(self.store.connection.total_changes, before_changes)
        self.assertEqual(len(self.store.list_operator_actions()), before_actions)
        failing = Mock()
        failing.status.side_effect = RuntimeError("background failure")
        server = DashboardServer(port=0, data=DashboardData(store=self.store, control=failing)).start()
        self.addCleanup(server.stop)
        assert server.url is not None
        with self.assertRaises(HTTPError) as context:
            urlopen(server.url + "/api/operator", timeout=3)
        self.assertEqual(context.exception.code, 503)
        with urlopen(server.url + "/", timeout=3) as root:
            self.assertEqual(root.status, 200)


if __name__ == "__main__":
    unittest.main()
