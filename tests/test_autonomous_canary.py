from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
import unittest
from unittest.mock import patch

from axiom.auto_canary import AutonomousCanaryWorker
from axiom.canary import (
    AUTONOMOUS_MICRO_LIVE,
    AUTONOMOUS_CANARY_LIMITS,
    CanaryBlocked,
    CanaryService,
    CredentialStore,
)
from axiom.dashboard import DashboardData
from axiom.node import NodeConfig, ResearchNode
from axiom.operator import HermesOperatorAdapter, OperatorControlPlane
from axiom.ranker import CandidateCanaryRanker
from axiom.storage import AxiomStore


T0 = datetime(2026, 1, 2, 12, tzinfo=timezone.utc)


class HealthyStore(AxiomStore):
    def polymarket_health(self, **kwargs):
        return {"grade": "A", "errors": 0}


class TestCredentials(CredentialStore):
    def __init__(self, configured: bool = True):
        self._configured = configured

    def configured(self, **kwargs):
        return self._configured


class TestVenue:
    def __init__(self):
        self.submissions = []

    def geoblock(self):
        return {"blocked": False, "close_only": False, "country": "ZZ", "region": "T"}

    def market_context(self, market_id, token_id):
        return {
            "accepting_orders": True,
            "min_order_size": "1",
            "tick_size": "0.01",
            "bids": [{"price": "0.49", "size": "100"}],
            "asks": [{"price": "0.50", "size": "100"}],
            "fee_bps": "10",
        }

    def balance(self):
        return Decimal("10")

    def submit_limit_order(self, **kwargs):
        self.submissions.append(kwargs)
        return {
            "ok": True,
            "order_id": "quality-order",
            "status": "matched",
            "fill_quantity": kwargs["size"],
            "actual_average_price": "0.51",
            "fees": "0.02",
        }



def candidate_payload(
    candidate_id: str,
    *,
    market_type: str = "prediction",
    cluster: str = "cluster-a",
    score: float = 0.10,
    holdout_used: bool = False,
    executable: bool = False,
) -> dict:
    parts = ("strategy-v1", "model-v1", "config-v1")
    payload = {
        "market_type": market_type,
        "lineage": [cluster],
        "mutation_cluster": cluster,
        "schema_validated": True,
        "historical_backtest_passed": True,
        "validation_passed": True,
        "robustness_passed": True,
        "data_quality_passed": True,
        "frozen": True,
        "holdout_used": holdout_used,
        "strategy_hash": parts[0],
        "model_hash": parts[1],
        "config_hash": parts[2],
        "frozen_hash": hashlib.sha256("|".join(parts).encode()).hexdigest(),
        "validation_expectancy": score,
        "validation_confidence_lower_bound": score,
        "validation_stability": 0.90,
        "validation_calibration": 0.90,
        "validation_sample_count": 100,
        "validation_trade_count": 50,
        "validation_execution_quality": 0.90,
        "data_quality": "HIGH",
    }
    if executable:
        strategy = {
            "version": 1,
            "market_type": "prediction",
            "family": "probability_mispricing",
            "parameters": {"threshold": 0.05},
            "operations": [],
            "probability_model": "fixed",
            "resolution_aware": True,
            "resolution_inputs": ["expiry", "settlement"],
            "strategy_id": candidate_id,
        }
        model = {"probability": 0.80}
        payload["strategy_document"] = strategy
        payload["model_document"] = model
        payload["strategy_hash"] = "sha256:" + hashlib.sha256(
            json.dumps(strategy, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
        ).hexdigest()
        payload["model_hash"] = "sha256:" + hashlib.sha256(
            json.dumps(model, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
        ).hexdigest()
        payload["config_hash"] = "config-hash"
        payload["frozen_hash"] = hashlib.sha256(
            "|".join((payload["strategy_hash"], payload["model_hash"], payload["config_hash"])).encode()
        ).hexdigest()
    return payload


class AutonomousWorkflowTests(unittest.TestCase):
    def setUp(self):
        self.store = HealthyStore(":memory:")
        self.addCleanup(self.store.close)
        self.service = CanaryService(
            self.store,
            credentials=TestCredentials(True),
            clock=lambda: T0,
        )

    def seed_candidate(self, candidate_id: str, **kwargs):
        payload = candidate_payload(candidate_id, **kwargs)
        self.store.save_candidate_lifecycle(candidate_id, "IDEA", payload, timestamp=T0)
        self.store.save_candidate_lifecycle(candidate_id, "FROZEN", payload, timestamp=T0)
        return payload

    def test_ranker_automatically_binds_eligible_prediction_candidates(self):
        self.seed_candidate("winner", cluster="cluster-a", score=0.40)
        self.seed_candidate("sibling", cluster="cluster-a", score=0.30)
        self.seed_candidate("crypto", cluster="cluster-c", score=0.99, market_type="crypto_spot")
        result = CandidateCanaryRanker(self.store, clock=lambda: T0).evaluate_and_select(T0)
        self.assertEqual(result["selected_candidate"], "winner")
        self.assertEqual(result["eligible_count"], 2)
        self.assertEqual(self.service.validate_eligibility("winner")["eligible"], True)
        self.assertEqual(self.service.validate_eligibility("sibling")["eligible"], True)
        self.assertNotIn("crypto", [row["candidate_id"] for row in result["rankings"]])
        self.assertIsNone(self.store.connection.execute("SELECT 1 FROM canary_eligibility WHERE candidate_id='crypto'").fetchone())

    def test_holdout_evidence_is_excluded_and_ranking_is_deterministic(self):
        self.seed_candidate("alpha", cluster="cluster-a", score=0.20)
        self.seed_candidate("beta", cluster="cluster-b", score=0.20)
        self.seed_candidate("holdout", cluster="cluster-c", score=0.99, holdout_used=True)
        ranker = CandidateCanaryRanker(self.store, clock=lambda: T0)
        first = ranker.evaluate_and_select(T0)
        first_rows = ranker.rankings()
        second = ranker.evaluate_and_select(T0)
        second_rows = ranker.rankings()
        self.assertEqual(first["ranking_run_id"], second["ranking_run_id"])
        self.assertEqual([row["candidate_id"] for row in first_rows], ["alpha", "beta"])
        self.assertEqual([row["candidate_id"] for row in second_rows], ["alpha", "beta"])
        self.assertFalse(any(row["candidate_id"] == "holdout" for row in second_rows))
        self.assertFalse(any(row["evidence_versions"]["locked_holdout_used"] for row in second_rows))

    def test_mutation_cluster_has_one_representative_and_winner_is_persisted(self):
        self.seed_candidate("weak", cluster="cluster-a", score=0.10)
        self.seed_candidate("strong", cluster="cluster-a", score=0.50)
        self.seed_candidate("other", cluster="cluster-b", score=0.20)
        ranker = CandidateCanaryRanker(self.store, clock=lambda: T0)
        result = ranker.evaluate_and_select(T0)
        rows = {row["candidate_id"]: row for row in ranker.rankings()}
        self.assertEqual(result["selected_candidate"], "strong")
        self.assertEqual(rows["strong"]["cluster_representative"], 1)
        self.assertEqual(rows["weak"]["cluster_representative"], 0)
        self.assertEqual(rows["weak"]["reason"], "DIVERSITY_CLUSTER_NON_REPRESENTATIVE")
        self.assertEqual(ranker.current_winner()["candidate_id"], "strong")

    def test_dashboard_uses_authoritative_service_gate_projection(self):
        payload = self.seed_candidate("gated")
        dashboard = DashboardData(store=self.store)
        record = self.store.load_candidate_lifecycle("gated")
        fields = dashboard._candidate_status_fields(record)
        self.assertTrue(fields["canary_eligible"])
        self.assertEqual(fields["historical_gates"], "PASSED")
        changed = dict(payload)
        changed["validation_passed"] = False
        self.store.save_candidate_lifecycle("gated", "FROZEN", changed, from_stage="FROZEN", timestamp=T0)
        invalid = dashboard._candidate_status_fields(self.store.load_candidate_lifecycle("gated"))
        self.assertFalse(invalid["canary_eligible"])
        self.assertEqual(invalid["historical_gates"], "NOT_PASSED")

    def test_enable_requires_exact_confirmation_and_freezes_risk_envelope(self):
        control = OperatorControlPlane(self.store)
        denied = control.execute("canary.enable_auto", confirm="ENABLE AUTO CANARY ")
        self.assertFalse(denied["ok"])
        self.assertEqual(denied["reason"], "EXACT_CONFIRMATION_REQUIRED")
        enabled = control.execute("canary.enable_auto", confirm="ENABLE AUTO CANARY")
        self.assertTrue(enabled["ok"])
        status = self.service.status()
        self.assertEqual(status["micro_live_canary"], AUTONOMOUS_MICRO_LIVE)
        self.assertEqual(status["risk_envelope"], AUTONOMOUS_CANARY_LIMITS)
        self.assertEqual(status["limits"], AUTONOMOUS_CANARY_LIMITS)
        self.assertFalse(status["live_execution"])

    def test_kill_latch_precedes_disarm_and_reenable(self):
        self.service.enable_autonomous_micro_live()
        self.service.kill()
        self.service.disarm()
        self.assertEqual(self.service.status()["micro_live_canary"], "KILLED")
        with self.assertRaisesRegex(CanaryBlocked, "CANARY_KILLED"):
            self.service.enable_autonomous_micro_live()

    def test_worker_disabled_and_no_signal_never_constructs_submission_venue(self):
        calls = []
        worker = AutonomousCanaryWorker(
            self.store,
            clock=lambda: T0,
            venue_factory=lambda: calls.append("constructed"),
        )
        disabled = worker.tick(now=T0)
        self.assertEqual(disabled["status"], "DISABLED")
        self.service.enable_autonomous_micro_live()
        blocked = worker.tick(now=T0)
        self.assertEqual(blocked["blocker"], "NO_ELIGIBLE_RANKABLE_CANDIDATE")
        self.assertEqual(calls, [])
    def test_hermes_cannot_change_autonomous_risk_controls(self):
        self.service.enable_autonomous_micro_live()
        before = self.service.status()
        hermes = HermesOperatorAdapter(self.store, "hermes-test")
        hermes.set_status("PAUSED")
        hermes.set_status("ACTIVE")
        after = self.service.status()
        self.assertEqual(after["micro_live_canary"], AUTONOMOUS_MICRO_LIVE)
        self.assertEqual(after["risk_envelope"], before["risk_envelope"])
        self.assertEqual(after["limits"], before["limits"])


    def test_unknown_signal_is_terminal_for_worker_without_external_retry(self):
        self.seed_candidate("winner", score=0.4)
        self.service.enable_autonomous_micro_live()
        worker = AutonomousCanaryWorker(self.store, clock=lambda: T0, venue_factory=TestVenue)
        with patch.object(
            CanaryService,
            "generate_signal",
            return_value={"status": "UNKNOWN", "signal_id": "unknown-signal"},
        ), patch.object(CanaryService, "submit_signal") as submit:
            result = worker.tick(now=T0)
            self.assertEqual(result["blocker"], "UNKNOWN_NO_RETRY")
            self.assertEqual(worker.tick(now=T0)["blocker"], "UNKNOWN_NO_RETRY")
            submit.assert_not_called()
        state = self.store.connection.execute(
            "SELECT next_decision,blocker,last_signal_id FROM canary_autonomous_state WHERE singleton=1"
        ).fetchone()
        self.assertEqual(state["next_decision"], "WAIT_FOR_FRESH_ACTIONABLE_SIGNAL")
        self.assertEqual(state["last_signal_id"], "unknown-signal")

    def test_node_worker_isolated_from_collector(self):
        node = ResearchNode(
            NodeConfig(":memory:", crypto_enabled=False),
            provider=object(),
            store=self.store,
            clock=lambda: T0,
            sleep=lambda _: None,
        )
        collector_calls = []

        class CollectorSentinel:
            def collect_once(self):
                collector_calls.append(True)
                raise AssertionError("autonomous worker touched collector")

        node.collector = CollectorSentinel()
        self.assertIs(node._auto_canary_worker.store, self.store)
        result = node._auto_canary_worker.tick(now=T0)
        self.assertEqual(result["status"], "DISABLED")
        self.assertEqual(collector_calls, [])

    def test_execution_quality_deltas_are_persisted(self):
        payload = self.seed_candidate("executable", executable=True)
        self.store.save_polymarket_snapshot(
            "snapshot-1",
            "market-1",
            T0,
            T0,
            {
                "source_type": "FORWARD_COLLECTED",
                "snapshot": {
                    "market_id": "market-1",
                    "timestamp": T0.isoformat(),
                    "yes_ask": "0.50",
                    "yes_token_id": "yes",
                    "no_ask": "0.50",
                    "no_token_id": "no",
                    "settlement": "open",
                },
                "active": True,
                "settlement": "open",
            },
            source_type="FORWARD_COLLECTED",
        )
        CandidateCanaryRanker(self.store, clock=lambda: T0).evaluate_and_select(T0)
        self.service.enable_autonomous_micro_live()
        signal = self.service.generate_signal("executable")
        self.assertIsNotNone(signal)
        result = self.service.submit_signal(
            signal["signal_id"], venue=TestVenue(), allow_test_venue=True
        )
        self.assertEqual(result["status"], "matched")
        row = self.store.connection.execute(
            "SELECT latency_ms,price_difference,fee_difference,slippage_difference "
            "FROM canary_ledger WHERE signal_id=?",
            (signal["signal_id"],),
        ).fetchone()
        self.assertIsNotNone(row)
        self.assertIsNotNone(row["latency_ms"])
        self.assertEqual(row["price_difference"], "0.01")
        self.assertIsNotNone(row["fee_difference"])
        self.assertIsNotNone(row["slippage_difference"])
        event = self.store.connection.execute(
            "SELECT evidence_json FROM canary_execution_events WHERE canary_event_id=?",
            ("canary-" + hashlib.sha256(signal["signal_id"].encode()).hexdigest()[:24],),
        ).fetchone()
        evidence = json.loads(event["evidence_json"])
        self.assertEqual(evidence["paper_expected_price"], "0.50")
        self.assertEqual(evidence["actual_average_price"], "0.51")
        self.assertIn("slippage_difference_bps", evidence)


if __name__ == "__main__":
    unittest.main()
