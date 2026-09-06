from __future__ import annotations

from datetime import datetime, timezone
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

from axiom.canary import CanaryService
from axiom.dashboard import DashboardData, DashboardServer, _DashboardHandler
from axiom.operator import BOOTSTRAP_JOB_NAME, OperatorControlPlane
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

    def _seed_candidate(self, candidate_id: str = "candidate-1") -> None:
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
            "source_type": "FORWARD_COLLECTED",
            "timeframe": "live",
            "strategy_hash": strategy_hash,
            "model_hash": model_hash,
            "config_hash": config_hash,
            "frozen_hash": frozen_hash,
            "schema_validated": True,
            "historical_backtest_passed": True,
            "validation_passed": True,
            "robustness_passed": True,
            "data_quality_passed": True,
            "holdout_used": False,
            "frozen": True,
            "critical_error": None,
        }
        self.store.save_candidate_lifecycle(candidate_id, "IDEA", payload)
        self.store.save_candidate_lifecycle(candidate_id, "FROZEN", payload, from_stage="IDEA")

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
