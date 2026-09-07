from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
import threading
import unittest
from unittest.mock import Mock, patch

from axiom.auto_canary import AutonomousCanaryWorker
from axiom.canary import (
    AUTONOMOUS_MICRO_LIVE,
    AUTONOMOUS_CANARY_LIMITS,
    CanaryBlocked,
    CanaryService,
    CredentialStore,
)
from axiom.dashboard import DashboardData
from axiom.lifecycle import CandidateLifecycleManager, CandidateStage
from axiom.node import NodeConfig, ResearchNode
from axiom.operator import (
    CANARY_CONNECTIVITY_CONFIG_KEY,
    HermesOperatorAdapter,
    OperatorControlPlane,
)
from axiom.ranker import CandidateCanaryRanker
from axiom.storage import AxiomStore
from axiom.data_quality import PRICE_PROXY, TIMESTAMPED_DEPTH, evaluate_prediction_data_quality



T0 = datetime(2026, 1, 2, 12, tzinfo=timezone.utc)


def connectivity_projection(*, ready: bool, failure_codes: list[str] | None = None) -> dict[str, object]:
    codes = list(failure_codes or ([] if ready else ["CANARY_ALLOWANCE_INSUFFICIENT"]))
    return {
        "ready": ready,
        "status": "READY" if ready else "BLOCKED",
        "checked_at": T0.isoformat(),
        "sdk": {
            "installed": True,
            "name": "polymarket-client",
            "version": "0.9.0",
            "status": "INSTALLED",
        },
        "credentials": {"status": "CONFIGURED"},
        "authentication": {"status": "PASS"},
        "account": {"status": "PASS", "wallet_type": "EOA"},
        "geoblock": {"status": "PASS", "country": "ZZ", "region": "T"},
        "balance": {"status": "PASS", "available_usd": "10"},
        "allowance": {"status": "SUFFICIENT" if ready else "INSUFFICIENT"},
        "market": {"status": "SKIPPED"},
        "order_book": {"status": "SKIPPED"},
        "failure_codes": codes,
        "failure_reasons": (
            []
            if ready
            else [
                {
                    "code": codes[0],
                    "reason": "Current allowance is below the amount required for a $1 canary.",
                }
            ]
        ),
        "live_execution": False,
    }


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
    dataset_id: str = "prediction-history",
    dataset_version: str = "v1",
) -> dict:
    parts = ("strategy-v1", "model-v1", "config-v1")
    payload = {
        "market_type": market_type,
        "dataset_id": dataset_id,
        "dataset_version": dataset_version,
        "dataset_provenance": {
            "dataset_id": dataset_id,
            "dataset_version": dataset_version,
            "source_type": "HISTORICAL",
            "time_split": "train-validation-holdout",
        },
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
        "data_quality": "PRICE_PROXY",
        "minimum_sample_check": {
            "passed": True,
            "count": 100,
            "trades": 50,
            "checks": {"observations": True, "trades": True},
        },
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
        self.store.save_dataset(
            "prediction-history",
            "v1",
            [{"timestamp": T0.isoformat(), "price": 0.5, "source_type": "HISTORICAL"}],
        )
        self.store.save_dataset_catalog(
            "prediction-history",
            "v1",
            provider="polymarket",
            instrument="POLYMARKET",
            market_type="prediction",
            timeframe="event",
            start_timestamp=T0,
            end_timestamp=T0,
            row_count=1,
            completeness=1.0,
            quality="PRICE_PROXY",
            source_type="HISTORICAL",
            snapshot_id="prediction-history:v1",
            metadata={
                "provider": "polymarket",
                "source_type": "HISTORICAL",
                "research_quality": "PRICE_PROXY",
                "historical_order_book_available": False,
            },
        )
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
    def save_quality_dataset(
        self,
        dataset_id: str,
        *,
        quality: str = PRICE_PROXY,
        rows: list[dict[str, object]] | None = None,
        historical_order_book_available: bool = False,
    ) -> None:
        values = rows or [{"timestamp": T0.isoformat(), "price": 0.5, "source_type": "HISTORICAL"}]
        self.store.save_dataset(dataset_id, "v1", values)
        self.store.save_dataset_catalog(
            dataset_id,
            "v1",
            provider="polymarket",
            instrument="POLYMARKET",
            market_type="prediction",
            timeframe="event",
            start_timestamp=T0,
            end_timestamp=T0,
            row_count=len(values),
            completeness=1.0,
            quality=quality,
            source_type="HISTORICAL",
            snapshot_id=f"{dataset_id}:v1",
            metadata={
                "provider": "polymarket",
                "source_type": "HISTORICAL",
                "research_quality": quality,
                "historical_order_book_available": historical_order_book_available,
            },
        )
    def save_forward_canary_snapshot(self, snapshot_id: str = "selection-binding-snapshot") -> None:
        self.store.save_polymarket_snapshot(
            snapshot_id,
            "market-1",
            T0,
            T0,
            {
                "source_type": "FORWARD_COLLECTED",
                "snapshot": {
                    "market_id": "market-1",
                    "timestamp": T0.isoformat(),
                    "yes_ask": "0.50",
                    "yes_order_book": {
                        "asks": [{"price": "0.50", "size": "100"}],
                        "bids": [],
                        "timestamp": T0.isoformat(),
                        "token_id": "yes",
                    },
                    "no_order_book": {
                        "asks": [{"price": "0.50", "size": "100"}],
                        "bids": [],
                        "timestamp": T0.isoformat(),
                        "token_id": "no",
                    },
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

    def insert_selection_snapshot(
        self,
        baseline: dict[str, object],
        *,
        omitted: frozenset[str] = frozenset(),
        updates: dict[str, object] | None = None,
    ) -> None:
        row = dict(baseline)
        row.update(updates or {})
        columns = (
            "ranking_run_id",
            "ranking_timestamp",
            "candidate_id",
            "rank",
            "total_score",
            "component_scores_json",
            "evidence_versions_json",
            "reason",
            "selected_at",
            "qualification_hash",
            "ranking_snapshot_hash",
            "selection_status",
            "selection_valid",
            "selection_invalidation_reason",
            "last_selected_candidate",
        )
        persisted = tuple(column for column in columns if column not in omitted)
        placeholders = ",".join("?" for _ in persisted)
        with self.store.connection:
            self.store.connection.execute("DELETE FROM canary_selection WHERE singleton=1")
            self.store.connection.execute(
                "INSERT INTO canary_selection(singleton,"
                + ",".join(persisted)
                + ") VALUES(1,"
                + placeholders
                + ")",
                tuple(row[column] for column in persisted),
            )

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


    def test_winner_switch_keeps_new_selection_current_and_exposes_previous_history(self):
        self.seed_candidate("first-winner", cluster="cluster-a", score=0.40)
        ranker = CandidateCanaryRanker(self.store, clock=lambda: T0)

        first = ranker.evaluate_and_select(T0)
        self.assertEqual(first["selected_candidate"], "first-winner")
        self.assertEqual(first["selection_status"], "CURRENT")
        self.assertTrue(first["selection_valid"])

        self.seed_candidate("new-winner", cluster="cluster-b", score=0.90)
        switched = ranker.evaluate_and_select(T0)

        self.assertEqual(switched["selected_candidate"], "new-winner")
        self.assertEqual(switched["winner_id"], "new-winner")
        self.assertEqual(switched["last_selected_candidate"], "first-winner")
        self.assertEqual(switched["selection_status"], "CURRENT")
        self.assertTrue(switched["selection_valid"])
        self.assertIsNone(switched["selection_invalidation_reason"])
        self.assertEqual(switched["selected"]["candidate_id"], "new-winner")
        self.assertEqual(
            switched["selected"]["last_selected_candidate"],
            "first-winner",
        )
        self.assertEqual(self.service.status()["selected_candidate"], "new-winner")
        self.assertEqual(
            self.service.status()["last_selected_candidate"],
            "first-winner",
        )
        self.assertEqual(ranker.current_selection()["candidate_id"], "new-winner")

    def test_rankings_read_is_serialized_against_concurrent_writer(self):
        self.seed_candidate("ranking-lock", score=0.40)
        ranker = CandidateCanaryRanker(self.store, clock=lambda: T0)
        ranker.evaluate_and_select(T0)

        entered = threading.Event()
        release = threading.Event()
        writer_attempted = threading.Event()
        writer_acquired = threading.Event()
        reader_ident = None
        result = {}
        original_lock = self.store._lock

        class GatedLock:
            def acquire(self, blocking=True, timeout=-1):
                if not blocking:
                    acquired = original_lock.acquire(False)
                elif timeout == -1:
                    acquired = original_lock.acquire()
                else:
                    acquired = original_lock.acquire(True, timeout)
                if acquired and threading.get_ident() == reader_ident:
                    entered.set()
                    if not release.wait(timeout=2):
                        raise AssertionError("ranking read was not released")
                return acquired

            def release(self):
                original_lock.release()

            def __enter__(self):
                self.acquire()
                return self

            def __exit__(self, exc_type, exc_value, traceback):
                self.release()
                return False

        gated_lock = GatedLock()
        self.store._lock = gated_lock

        def read_rankings():
            nonlocal reader_ident
            reader_ident = threading.get_ident()
            try:
                result["rows"] = ranker.rankings()
            except BaseException as exc:
                result["error"] = exc

        def competing_writer():
            acquired = gated_lock.acquire(blocking=False)
            writer_attempted.set()
            if not acquired:
                return
            try:
                writer_acquired.set()
            finally:
                gated_lock.release()

        reader = threading.Thread(target=read_rankings)
        reader.start()
        try:
            self.assertTrue(entered.wait(timeout=2))
            writer = threading.Thread(target=competing_writer)
            writer.start()
            self.assertTrue(writer_attempted.wait(timeout=2))
            writer.join(timeout=2)
            self.assertFalse(writer_acquired.is_set())
            release.set()
            reader.join(timeout=2)
        finally:
            release.set()
            reader.join(timeout=2)
            self.store._lock = original_lock

        self.assertFalse(reader.is_alive())
        self.assertNotIn("error", result)
        self.assertEqual([row["candidate_id"] for row in result["rows"]], ["ranking-lock"])

    def test_dashboard_uses_authoritative_service_gate_projection(self):
        payload = self.seed_candidate("gated")
        dashboard = DashboardData(store=self.store)
        record = self.store.load_candidate_lifecycle("gated")
        fields = dashboard._candidate_status_fields(record)
        self.assertTrue(fields["canary_eligible"])
        self.assertEqual(fields["historical_gates"], "PASSED")
        self.assertEqual(fields["historical_data_integrity"], "PASS")
        self.assertEqual(fields["historical_execution_fidelity"], "PRICE_PROXY · LIMITED")
        self.assertEqual(fields["canary_data_quality_gate"], "PASS FOR $1 MICRO-LIVE")
        self.assertEqual(fields["production_evidence"], "INSUFFICIENT")
        changed = dict(payload)
        changed["validation_passed"] = False
        self.store.save_candidate_lifecycle("gated", "FROZEN", changed, from_stage="FROZEN", timestamp=T0)
        invalid = dashboard._candidate_status_fields(self.store.load_candidate_lifecycle("gated"))
        self.assertFalse(invalid["canary_eligible"])
        self.assertEqual(invalid["historical_gates"], "NOT_PASSED")
    def test_price_proxy_is_limited_historical_fidelity(self):
        self.seed_candidate("proxy")
        quality = evaluate_prediction_data_quality(
            self.store,
            self.store.load_candidate_lifecycle("proxy")["payload"],
        )
        self.assertEqual(quality["historical_data_integrity"], "PASS")
        self.assertEqual(quality["historical_execution_fidelity"], PRICE_PROXY)
        self.assertEqual(quality["historical_execution_fidelity_score"], 0.35)
        self.assertEqual(quality["canary_data_quality_status"], "CANARY_DATA_QUALITY_ACCEPTABLE_LIMITED")
        self.assertNotEqual(quality["historical_execution_fidelity"], TIMESTAMPED_DEPTH)

    def test_valid_price_proxy_candidate_passes_limited_canary_gate(self):
        self.seed_candidate("proxy-eligible")
        validation = self.service.validate_eligibility("proxy-eligible")
        self.assertTrue(validation["eligible"])
        self.assertTrue(validation["historical_data_integrity_passed"])
        self.assertEqual(validation["historical_execution_fidelity"], PRICE_PROXY)
        self.assertTrue(validation["canary_data_quality_acceptable"])
        self.assertEqual(
            validation["canary_data_quality_status"],
            "CANARY_DATA_QUALITY_ACCEPTABLE_LIMITED",
        )
        self.assertEqual(validation["production_evidence_status"], "INSUFFICIENT")

    def test_malformed_provenance_blocks_prediction_canary(self):
        payload = candidate_payload("malformed")
        payload["dataset_provenance"]["dataset_version"] = "wrong-version"
        self.store.save_candidate_lifecycle("malformed", "IDEA", payload, timestamp=T0)
        self.store.save_candidate_lifecycle("malformed", "FROZEN", payload, timestamp=T0)
        validation = self.service.validate_eligibility("malformed")
        self.assertFalse(validation["eligible"])
        self.assertFalse(validation["historical_data_integrity_passed"])
        self.assertIn("HISTORICAL_PROVENANCE_INCOMPLETE", validation["data_quality"]["reasons"])
    def test_missing_historical_dataset_version_rejects_prediction_canary(self):
        payload = candidate_payload(
            "missing-dataset",
            dataset_id="missing-prediction-history",
            dataset_version="v404",
        )
        self.store.save_candidate_lifecycle("missing-dataset", "IDEA", payload, timestamp=T0)
        self.store.save_candidate_lifecycle("missing-dataset", "FROZEN", payload, timestamp=T0)

        validation = self.service.validate_eligibility("missing-dataset")
        self.assertFalse(validation["eligible"])
        self.assertFalse(validation["historical_data_integrity_passed"])
        self.assertIn(
            "HISTORICAL_DATASET_VERSION_NOT_FOUND",
            validation["data_quality"]["reasons"],
        )

        result = CandidateCanaryRanker(self.store, clock=lambda: T0).evaluate_and_select(T0)
        self.assertEqual(result["selection_status"], "NONE")
        self.assertIsNone(result["selected_candidate"])


    def test_forward_contamination_blocks_prediction_canary(self):
        self.save_quality_dataset(
            "contaminated",
            rows=[{"timestamp": T0.isoformat(), "price": 0.5, "source_type": "FORWARD_COLLECTED"}],
        )
        self.seed_candidate(
            "contaminated-candidate",
            dataset_id="contaminated",
            dataset_version="v1",
        )
        quality = evaluate_prediction_data_quality(
            self.store,
            self.store.load_candidate_lifecycle("contaminated-candidate")["payload"],
        )
        self.assertFalse(quality["historical_no_forward_contamination"])
        self.assertIn("HISTORICAL_FORWARD_CONTAMINATION", quality["reasons"])
        self.assertFalse(self.service.validate_eligibility("contaminated-candidate")["eligible"])

    def test_timestamped_depth_ranks_above_price_proxy(self):
        self.save_quality_dataset(
            "timestamped-depth",
            quality=TIMESTAMPED_DEPTH,
            historical_order_book_available=True,
        )
        self.seed_candidate("proxy-ranker", dataset_id="prediction-history", dataset_version="v1", score=0.20)
        self.seed_candidate("depth-ranker", dataset_id="timestamped-depth", dataset_version="v1", score=0.20)
        ranker = CandidateCanaryRanker(self.store, clock=lambda: T0)
        ranker.evaluate_and_select(T0)
        rows = {row["candidate_id"]: row for row in ranker.rankings()}
        self.assertGreater(
            rows["depth-ranker"]["total_score"],
            rows["proxy-ranker"]["total_score"],
        )
        self.assertEqual(
            rows["depth-ranker"]["component_scores"]["components"]["execution_fidelity_score"],
            1.0,
        )
        self.assertEqual(
            rows["proxy-ranker"]["component_scores"]["components"]["execution_fidelity_score"],
            0.35,
        )
        self.assertEqual(rows["depth-ranker"]["component_scores"]["fidelity_penalty"], 0.0)
        self.assertEqual(rows["proxy-ranker"]["component_scores"]["fidelity_penalty"], 0.65)
    def test_current_signal_requires_fresh_forward_order_book(self):
        self.seed_candidate("book-required", executable=True)
        self.store.save_polymarket_snapshot(
            "snapshot-without-book",
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
                    "settlement": "open",
                },
                "active": True,
                "settlement": "open",
            },
            source_type="FORWARD_COLLECTED",
        )
        CandidateCanaryRanker(self.store, clock=lambda: T0).evaluate_and_select(T0)
        self.assertIsNone(self.service.generate_signal("book-required"))


    def test_enable_requires_exact_confirmation_and_connectivity_gate(self):
        control = OperatorControlPlane(self.store)
        denied = control.execute("canary.enable_auto", confirm="ENABLE AUTO CANARY ")
        self.assertFalse(denied["ok"])
        self.assertEqual(denied["reason"], "EXACT_CONFIRMATION_REQUIRED")

        absent_service = Mock()
        with patch("axiom.operator.CanaryService", return_value=absent_service) as service_factory:
            absent = control.execute("canary.enable_auto", confirm="ENABLE AUTO CANARY")
        service_factory.assert_not_called()
        self.assertFalse(absent["ok"])
        self.assertEqual(absent["reason"], "CONNECTIVITY_CHECK_REQUIRED")
        absent_service.enable_autonomous_micro_live.assert_not_called()

        blocked = connectivity_projection(
            ready=False,
            failure_codes=["CANARY_ALLOWANCE_INSUFFICIENT"],
        )
        self.store.set_operator_config(CANARY_CONNECTIVITY_CONFIG_KEY, blocked)
        blocked_service = Mock()
        with patch("axiom.operator.CanaryService", return_value=blocked_service) as service_factory:
            blocked_result = control.execute(
                "canary.enable_auto",
                confirm="ENABLE AUTO CANARY",
            )
        service_factory.assert_not_called()
        self.assertFalse(blocked_result["ok"])
        self.assertEqual(blocked_result["reason"], "CANARY_ALLOWANCE_INSUFFICIENT")
        blocked_service.enable_autonomous_micro_live.assert_not_called()

        self.store.set_operator_config(
            CANARY_CONNECTIVITY_CONFIG_KEY,
            connectivity_projection(ready=True),
        )
        enabled = control.execute("canary.enable_auto", confirm="ENABLE AUTO CANARY")
        self.assertTrue(enabled["ok"])
        status = self.service.status()
        self.assertEqual(status["micro_live_canary"], AUTONOMOUS_MICRO_LIVE)
        self.assertEqual(status["risk_envelope"], AUTONOMOUS_CANARY_LIMITS)
        self.assertEqual(status["limits"], AUTONOMOUS_CANARY_LIMITS)
        self.assertFalse(status["live_execution"])

    def test_enable_rejects_partial_and_contradictory_stored_ready_projection(self):
        valid = connectivity_projection(ready=True)
        partial = dict(valid)
        partial.pop("balance")
        extra = {**valid, "unexpected": "tampered"}
        contradictory_status = {**valid, "status": "BLOCKED"}
        contradictory_failure = {
            **valid,
            "failure_codes": ["CANARY_ALLOWANCE_INSUFFICIENT"],
            "failure_reasons": [
                {
                    "code": "CANARY_ALLOWANCE_INSUFFICIENT",
                    "reason": "Current allowance is below the amount required for a $1 canary.",
                }
            ],
        }
        cases = (
            ("partial", partial),
            ("extra", extra),
            ("contradictory status", contradictory_status),
            ("contradictory failure", contradictory_failure),
        )
        sdk_null = {**valid, "sdk": {**valid["sdk"], "version": None}}
        sdk_unsupported = {**valid, "sdk": {**valid["sdk"], "version": "1.0.0"}}
        balance_below_target = {
            **valid,
            "balance": {**valid["balance"], "available_usd": "0"},
        }
        cases += (
            ("sdk version null", sdk_null),
            ("unsupported sdk version", sdk_unsupported),
            ("balance below canary target", balance_below_target),
        )

        control = OperatorControlPlane(self.store)
        for label, stored in cases:
            with self.subTest(label=label):
                self.store.set_operator_config(CANARY_CONNECTIVITY_CONFIG_KEY, stored)
                service = Mock()
                with patch("axiom.operator.CanaryService", return_value=service) as service_factory:
                    result = control.execute(
                        "canary.enable_auto",
                        confirm="ENABLE AUTO CANARY",
                    )
                self.assertFalse(result["ok"])
                self.assertEqual(result["reason"], "CONNECTIVITY_CHECK_REQUIRED")
                service_factory.assert_not_called()
                service.enable_autonomous_micro_live.assert_not_called()



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
    def test_ranker_re_evaluates_legacy_full_payload_into_qualification_binding(self):
        payload = self.seed_candidate("legacy")
        legacy_evidence = {
            **payload,
            "forward_duration_seconds": 999_999,
            "forward_observations": 100_000,
            "forward_fills": 100_000,
            "forward_trades": 100_000,
            "forward_order_attempts": 100_000,
            "forward_liquidity": 100_000,
            "forward_max_drawdown": 0.01,
            "forward_spread": 0.01,
            "forward_requested_quantity": 100_000,
            "forward_filled_quantity": 100_000,
        }
        with self.store.connection:
            self.store.connection.execute(
                "INSERT INTO canary_eligibility(candidate_id,eligible_at,frozen_hash,evidence_json) "
                "VALUES(?,?,?,?)",
                (
                    "legacy",
                    T0.isoformat(),
                    payload["frozen_hash"],
                    json.dumps(legacy_evidence, sort_keys=True),
                ),
            )
        before = self.store.connection.execute(
            "SELECT evidence_json FROM canary_eligibility WHERE candidate_id=?",
            ("legacy",),
        ).fetchone()
        self.assertNotEqual(json.loads(before["evidence_json"]).get("schema"), "canary-qualification-v1")

        result = CandidateCanaryRanker(self.store, clock=lambda: T0).evaluate_and_select(T0)
        evidence_row = self.store.connection.execute(
            "SELECT evidence_json FROM canary_eligibility WHERE candidate_id=?",
            ("legacy",),
        ).fetchone()
        evidence = json.loads(evidence_row["evidence_json"])
        self.assertEqual(evidence["schema"], "canary-qualification-v1")
        self.assertEqual(evidence["candidate_id"], "legacy")
        self.assertEqual(evidence["frozen_hash"], payload["frozen_hash"])
        self.assertNotIn("forward_duration_seconds", evidence)
        self.assertNotIn("forward_observations", evidence)

        ranking_row = self.store.connection.execute(
            "SELECT qualification_hash,ranking_snapshot_hash FROM canary_rankings "
            "WHERE candidate_id=?",
            ("legacy",),
        ).fetchone()
        self.assertTrue(ranking_row["qualification_hash"])
        self.assertTrue(ranking_row["ranking_snapshot_hash"])
        self.assertEqual(result["eligibility_raw_count"], 1)
        self.assertEqual(result["eligible_count"], 1)
        self.assertEqual(result["rankable_raw_count"], 1)
        self.assertEqual(result["rankable_count"], 1)

    def test_rejected_candidate_is_not_executable_after_ranker_tick(self):
        payload = self.seed_candidate("rejected", executable=True, score=0.90)
        ranker = CandidateCanaryRanker(self.store, clock=lambda: T0)
        initial = ranker.evaluate_and_select(T0)
        self.assertEqual(initial["selected_candidate"], "rejected")

        CandidateLifecycleManager(self.store).reject(
            "rejected",
            "hard research gate failed",
            expected_stage=CandidateStage.FROZEN,
        )
        result = ranker.evaluate_and_select(T0)
        self.assertIsNone(result["selected_candidate"])
        self.assertFalse(result["selection_valid"])
        self.assertEqual(result["selection_invalidation_reason"], "LIFECYCLE_REJECTED")
        self.assertEqual(result["eligible_count"], 0)
        self.assertEqual(result["rankable_count"], 0)
        self.assertIsNone(self.service.generate_signal("rejected"))
        self.assertIsNone(self.service.status()["winner_id"])

    def test_concurrent_paper_lifecycle_update_never_publishes_mixed_current_selection(self):
        payload = self.seed_candidate("paper-race", score=0.40)
        paper_payload = {**payload, "forward_expectancy": 0.40}
        self.store.save_candidate_lifecycle(
            "paper-race",
            "PAPER_FORWARD",
            paper_payload,
            from_stage="FROZEN",
            timestamp=T0,
        )
        ranker = CandidateCanaryRanker(self.store, clock=lambda: T0)
        entered = threading.Event()
        release = threading.Event()
        result_holder = {}
        original_rank_evidence = ranker._rank_evidence
        first_call = True

        def blocked_rank_evidence(payload, frozen_hash, quality=None):
            nonlocal first_call
            if first_call:
                first_call = False
                entered.set()
                if not release.wait(timeout=5):
                    raise AssertionError("ranking barrier was not released")
            return original_rank_evidence(payload, frozen_hash, quality=quality)

        ranker._rank_evidence = blocked_rank_evidence

        def run_ranker():
            try:
                result_holder["result"] = ranker.evaluate_and_select(T0)
            except BaseException as exc:
                result_holder["error"] = exc

        ranking_thread = threading.Thread(target=run_ranker)
        ranking_thread.start()
        try:
            self.assertTrue(entered.wait(timeout=5))
            CandidateLifecycleManager(self.store).record_evidence(
                "paper-race",
                {
                    "forward_expectancy": -0.40,
                    "forward_duration_seconds": 123,
                    "forward_observations": 456,
                },
                expected_stage=CandidateStage.PAPER_FORWARD,
                reason="concurrent paper update",
            )
        finally:
            release.set()
            ranking_thread.join(timeout=5)
        self.assertFalse(ranking_thread.is_alive())
        self.assertNotIn("error", result_holder)
        raced = result_holder["result"]
        raced_selection = ranker.current_selection()
        raced_rows = {
            row["candidate_id"]: row
            for row in ranker.rankings()
        }

        # A bounded retry may publish a fresh current selection; if it does,
        # it must be identical to a clean post-update ranking, never a mixed
        # pre-update ranking paired with post-update lifecycle evidence.
        clean = CandidateCanaryRanker(self.store, clock=lambda: T0).evaluate_and_select(T0)
        clean_rows = {
            row["candidate_id"]: row
            for row in CandidateCanaryRanker(self.store, clock=lambda: T0).rankings()
        }
        if raced["selection_status"] == "CURRENT":
            self.assertEqual(raced["selected_candidate"], clean["selected_candidate"])
            self.assertIsNotNone(raced_selection)
            self.assertEqual(
                raced_rows["paper-race"]["ranking_snapshot_hash"],
                clean_rows["paper-race"]["ranking_snapshot_hash"],
            )
        else:
            self.assertFalse(raced["selection_valid"])
            self.assertNotEqual(raced["selection_status"], "CURRENT")
            self.assertIsNone(raced["selected_candidate"])

    def test_ranking_relevant_forward_change_stales_selection_until_reevaluation(self):
        self.seed_candidate("forward-change", score=0.40)
        lifecycle = CandidateLifecycleManager(self.store)
        lifecycle.record_evidence(
            "forward-change",
            {"forward_expectancy": 0.40},
            expected_stage=CandidateStage.FROZEN,
            reason="seed ranking-relevant forward evidence",
        )
        ranker = CandidateCanaryRanker(self.store, clock=lambda: T0)
        first = ranker.evaluate_and_select(T0)
        old_row = self.store.connection.execute(
            "SELECT qualification_hash,ranking_snapshot_hash FROM canary_rankings "
            "WHERE candidate_id=?",
            ("forward-change",),
        ).fetchone()
        self.assertEqual(first["selection_status"], "CURRENT")
        self.assertTrue(first["selection_valid"])

        lifecycle.record_evidence(
            "forward-change",
            {
                "forward_expectancy": -0.40,
                "forward_duration_seconds": 1,
                "forward_observations": 999,
            },
            expected_stage=CandidateStage.FROZEN,
            reason="ranking-relevant forward evidence changed",
        )
        stale = self.service.status()
        self.assertEqual(stale["selection_status"], "STALE")
        self.assertFalse(stale["selection_valid"])
        self.assertEqual(stale["selection_invalidation_reason"], "RANKING_EVIDENCE_CHANGED")
        self.assertIsNone(stale["selected_candidate"])
        self.assertIsNone(stale["winner_id"])
        last_selected = stale["last_selected_candidate"]
        self.assertEqual(
            last_selected.get("candidate_id") if isinstance(last_selected, dict) else last_selected,
            "forward-change",
        )

        refreshed = CandidateCanaryRanker(self.store, clock=lambda: T0).evaluate_and_select(T0)
        new_row = self.store.connection.execute(
            "SELECT qualification_hash,ranking_snapshot_hash FROM canary_rankings "
            "WHERE candidate_id=?",
            ("forward-change",),
        ).fetchone()
        self.assertEqual(refreshed["selection_status"], "CURRENT")
        self.assertTrue(refreshed["selection_valid"])
        self.assertEqual(refreshed["selected_candidate"], "forward-change")
        self.assertEqual(old_row["qualification_hash"], new_row["qualification_hash"])
        self.assertNotEqual(old_row["ranking_snapshot_hash"], new_row["ranking_snapshot_hash"])
    def test_nested_validation_ranking_input_change_stales_selection_until_reevaluation(self):
        self.seed_candidate("nested-validation", score=0.40)
        lifecycle = CandidateLifecycleManager(self.store)
        lifecycle.record_evidence(
            "nested-validation",
            {"validation": {"liquidity": 0.20}},
            expected_stage=CandidateStage.FROZEN,
            reason="seed nested ranking evidence",
        )
        ranker = CandidateCanaryRanker(self.store, clock=lambda: T0)
        first = ranker.evaluate_and_select(T0)
        old_row = self.store.connection.execute(
            "SELECT qualification_hash,ranking_snapshot_hash,total_score "
            "FROM canary_rankings WHERE candidate_id=?",
            ("nested-validation",),
        ).fetchone()
        self.assertEqual(first["selection_status"], "CURRENT")
        self.assertTrue(first["selection_valid"])
        self.assertIsNotNone(old_row)

        lifecycle.record_evidence(
            "nested-validation",
            {"validation": {"liquidity": 0.80}},
            expected_stage=CandidateStage.FROZEN,
            reason="nested ranking evidence changed",
        )
        stale = self.service.status()
        self.assertEqual(stale["selection_status"], "STALE")
        self.assertFalse(stale["selection_valid"])
        self.assertEqual(stale["selection_invalidation_reason"], "RANKING_EVIDENCE_CHANGED")
        self.assertIsNone(stale["selected_candidate"])
        self.assertIsNone(stale["winner_id"])

        refreshed = CandidateCanaryRanker(self.store, clock=lambda: T0).evaluate_and_select(T0)
        new_row = self.store.connection.execute(
            "SELECT qualification_hash,ranking_snapshot_hash,total_score "
            "FROM canary_rankings WHERE candidate_id=?",
            ("nested-validation",),
        ).fetchone()
        self.assertEqual(refreshed["selection_status"], "CURRENT")
        self.assertTrue(refreshed["selection_valid"])
        self.assertEqual(refreshed["selected_candidate"], "nested-validation")
        self.assertEqual(old_row["qualification_hash"], new_row["qualification_hash"])
        self.assertNotEqual(old_row["ranking_snapshot_hash"], new_row["ranking_snapshot_hash"])
        self.assertNotEqual(old_row["total_score"], new_row["total_score"])


    def test_new_ranker_instance_preserves_persisted_selection_semantics(self):
        self.seed_candidate("restart-alpha", score=0.30)
        self.seed_candidate("restart-beta", score=0.20)
        first_ranker = CandidateCanaryRanker(self.store, clock=lambda: T0)
        first = first_ranker.evaluate_and_select(T0)
        first_rows = first_ranker.rankings()
        second_ranker = CandidateCanaryRanker(self.store, clock=lambda: T0)
        second = second_ranker.evaluate_and_select(T0)
        second_rows = second_ranker.rankings()

        self.assertEqual(first["selected_candidate"], second["selected_candidate"])
        self.assertEqual(first["ranking_run_id"], second["ranking_run_id"])
        self.assertEqual(first["ranking_timestamp"], second["ranking_timestamp"])
        self.assertEqual(first["selection_status"], "CURRENT")
        self.assertTrue(first["selection_valid"])
        self.assertEqual(
            [(row["candidate_id"], row["qualification_hash"], row["ranking_snapshot_hash"]) for row in first_rows],
            [(row["candidate_id"], row["qualification_hash"], row["ranking_snapshot_hash"]) for row in second_rows],
        )
    def test_untampered_atomic_selection_is_current_when_rows_match(self):
        candidate_id = "atomic-current"
        self.seed_candidate(candidate_id, executable=True, score=0.40)
        ranker = CandidateCanaryRanker(self.store, clock=lambda: T0)
        result = ranker.evaluate_and_select(T0)
        selection = dict(
            self.store.connection.execute(
                "SELECT * FROM canary_selection WHERE singleton=1"
            ).fetchone()
        )
        ranking = dict(
            self.store.connection.execute(
                "SELECT * FROM canary_rankings WHERE candidate_id=?",
                (candidate_id,),
            ).fetchone()
        )

        self.assertEqual(result["selection_status"], "CURRENT")
        self.assertTrue(result["selection_valid"])
        self.assertEqual(result["selected_candidate"], candidate_id)
        self.assertEqual(result["winner_id"], candidate_id)
        self.assertEqual(ranking["selected"], 1)
        self.assertIsNotNone(ranking["total_score"])
        for field in (
            "candidate_id",
            "ranking_run_id",
            "ranking_timestamp",
            "rank",
            "total_score",
            "component_scores_json",
            "evidence_versions_json",
            "qualification_hash",
            "ranking_snapshot_hash",
        ):
            self.assertTrue(selection[field])
            self.assertEqual(selection[field], ranking[field])
        self.assertEqual(selection["reason"], "SELECTED_WINNER")
        self.assertEqual(ranking["reason"], "")
        self.assertEqual(selection["selection_status"], "CURRENT")
        self.assertEqual(selection["selection_valid"], 1)
        self.assertEqual(selection["last_selected_candidate"], candidate_id)
        self.assertEqual(selection["selected_at"], ranking["ranking_timestamp"])
        self.assertEqual(self.service.status()["selection_status"], "CURRENT")
        self.assertIsNotNone(ranker.current_selection())
        self.assertIsNotNone(ranker.current_winner())

    def test_selection_metadata_tampering_cannot_bind_autonomous_canary(self):
        candidate_id = "selection-binding"
        self.seed_candidate(candidate_id, executable=True, score=0.40)
        self.save_forward_canary_snapshot()
        ranker = CandidateCanaryRanker(self.store, clock=lambda: T0)
        initial = ranker.evaluate_and_select(T0)
        self.assertEqual(initial["selection_status"], "CURRENT")
        self.assertTrue(initial["selection_valid"])
        baseline_selection = dict(
            self.store.connection.execute(
                "SELECT * FROM canary_selection WHERE singleton=1"
            ).fetchone()
        )
        baseline_ranking = dict(
            self.store.connection.execute(
                "SELECT * FROM canary_rankings WHERE candidate_id=?",
                (candidate_id,),
            ).fetchone()
        )
        signal = self.service.generate_signal(candidate_id)
        self.assertIsNotNone(signal)
        signal_id = str(signal["signal_id"])

        cases = (
            ("selection qualification hash omitted", frozenset({"qualification_hash"}), {}, {}),
            ("selection ranking hash omitted", frozenset({"ranking_snapshot_hash"}), {}, {}),
            ("ranking run empty", frozenset(), {"ranking_run_id": ""}, {}),
            ("selection run tampered", frozenset(), {"ranking_run_id": "selection-run-tamper"}, {}),
            ("ranking run disagrees", frozenset(), {}, {"ranking_run_id": "ranking-run-tamper"}),
            ("ranking timestamp omitted", frozenset({"ranking_timestamp"}), {}, {}),
            ("selection timestamp tampered", frozenset(), {"ranking_timestamp": "selection-time-tamper"}, {}),
            ("ranking timestamp disagrees", frozenset(), {}, {"ranking_timestamp": "ranking-time-tamper"}),
            ("selected-at empty", frozenset(), {"selected_at": ""}, {}),
            ("selection status omitted", frozenset({"selection_status"}), {}, {}),
            ("selection valid omitted", frozenset({"selection_valid"}), {}, {}),
            ("selection status tampered", frozenset(), {"selection_status": "STALE"}, {}),
            ("selection valid flag tampered", frozenset(), {"selection_valid": 0}, {}),
            ("ranking row is not selected", frozenset(), {}, {"selected": 0}),
            (
                "selection qualification hash disagrees",
                frozenset(),
                {"qualification_hash": "selection-qualification-tamper"},
                {},
            ),
            (
                "selection ranking hash disagrees",
                frozenset(),
                {"ranking_snapshot_hash": "selection-ranking-tamper"},
                {},
            ),
            (
                "ranking qualification hash disagrees",
                frozenset(),
                {},
                {"qualification_hash": "ranking-qualification-tamper"},
            ),
            (
                "ranking snapshot hash disagrees",
                frozenset(),
                {},
                {"ranking_snapshot_hash": "ranking-snapshot-tamper"},
            ),
            (
                "selection rank disagrees",
                frozenset(),
                {"rank": baseline_selection["rank"] + 1},
                {},
            ),
            (
                "ranking rank disagrees",
                frozenset(),
                {},
                {"rank": baseline_ranking["rank"] + 1},
            ),
            (
                "selection total score disagrees",
                frozenset(),
                {"total_score": baseline_selection["total_score"] + 0.123},
                {},
            ),
            (
                "ranking total score disagrees",
                frozenset(),
                {},
                {"total_score": baseline_ranking["total_score"] + 0.123},
            ),
            (
                "selection component scores disagree",
                frozenset(),
                {"component_scores_json": json.dumps({"tampered": True}, sort_keys=True)},
                {},
            ),
            (
                "ranking component scores disagree",
                frozenset(),
                {},
                {"component_scores_json": json.dumps({"tampered": True}, sort_keys=True)},
            ),
            (
                "selection evidence versions disagree",
                frozenset(),
                {"evidence_versions_json": json.dumps({"tampered": True}, sort_keys=True)},
                {},
            ),
            (
                "ranking evidence versions disagree",
                frozenset(),
                {},
                {"evidence_versions_json": json.dumps({"tampered": True}, sort_keys=True)},
            ),
            (
                "selection reason disagrees",
                frozenset(),
                {"reason": "selection-reason-tamper"},
                {},
            ),
            (
                "ranking reason disagrees",
                frozenset(),
                {},
                {"reason": "ranking-reason-tamper"},
            ),
        )
        for label, omitted, selection_updates, ranking_updates in cases:
            with self.subTest(label=label):
                self.insert_selection_snapshot(
                    baseline_selection,
                    omitted=omitted,
                    updates=selection_updates,
                )
                with self.store.connection:
                    self.store.connection.execute(
                        "UPDATE canary_rankings SET ranking_run_id=?,ranking_timestamp=?,"
                        "rank=?,total_score=?,component_scores_json=?,evidence_versions_json=?,"
                        "selected=?,reason=?,qualification_hash=?,ranking_snapshot_hash=? "
                        "WHERE candidate_id=?",
                        (
                            baseline_ranking["ranking_run_id"],
                            baseline_ranking["ranking_timestamp"],
                            baseline_ranking["rank"],
                            baseline_ranking["total_score"],
                            baseline_ranking["component_scores_json"],
                            baseline_ranking["evidence_versions_json"],
                            baseline_ranking["selected"],
                            baseline_ranking["reason"],
                            baseline_ranking["qualification_hash"],
                            baseline_ranking["ranking_snapshot_hash"],
                            candidate_id,
                        ),
                    )
                    for column, value in ranking_updates.items():
                        self.store.connection.execute(
                            f"UPDATE canary_rankings SET {column}=? WHERE candidate_id=?",
                            (value, candidate_id),
                        )

                stale = self.service.status()
                self.assertEqual(stale["selection_status"], "STALE")
                self.assertFalse(stale["selection_valid"])
                self.assertIsNone(stale["selected_candidate"])
                self.assertIsNone(stale["winner_id"])
                self.assertEqual(stale["last_selected_candidate"], candidate_id)
                self.assertIsNone(ranker.current_selection())
                self.assertIsNone(ranker.current_winner())

                self.service.disarm()
                enabled = self.service.enable_autonomous_micro_live()
                control = self.store.connection.execute(
                    "SELECT candidate_id FROM canary_control WHERE singleton=1"
                ).fetchone()
                self.assertIsNone(control["candidate_id"])
                self.assertIsNone(enabled["candidate"])
                self.assertIsNone(enabled["winner_id"])

                with self.store.connection:
                    self.store.connection.execute(
                        "UPDATE canary_signals SET status='READY',reason=NULL "
                        "WHERE signal_id=?",
                        (signal_id,),
                    )
                venue = TestVenue()
                with self.assertRaises(CanaryBlocked):
                    self.service.submit_signal(signal_id, venue=venue)
                self.assertEqual(venue.submissions, [])

    def test_disabled_autonomous_tick_has_no_signals_submissions_or_orders_across_cycles(self):
        self.seed_candidate("disabled-executable", executable=True, score=0.90)
        venue = TestVenue()
        worker = AutonomousCanaryWorker(
            self.store,
            clock=lambda: T0,
            venue_factory=lambda: venue,
        )
        results = [worker.tick(now=T0) for _ in range(3)]
        self.assertEqual([result["status"] for result in results], ["DISABLED"] * 3)
        self.assertEqual(venue.submissions, [])
        for table in ("canary_signals", "canary_ledger", "canary_execution_events"):
            count = self.store.connection.execute(f"SELECT COUNT(*) AS n FROM {table}").fetchone()["n"]
            self.assertEqual(count, 0, table)
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
                    "yes_order_book": {
                        "asks": [{"price": "0.50", "size": "100"}],
                        "bids": [],
                        "timestamp": T0.isoformat(),
                        "token_id": "yes",
                    },
                    "no_order_book": {
                        "asks": [{"price": "0.50", "size": "100"}],
                        "bids": [],
                        "timestamp": T0.isoformat(),
                        "token_id": "no",
                    },
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
