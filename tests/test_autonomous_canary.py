from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import json
import threading
import unittest
from unittest.mock import Mock, patch
from typing import Mapping
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
        "dataset_selector": {
            "dataset_id": dataset_id,
            "dataset_version": dataset_version,
            "source_type": "HISTORICAL",
        },
        "dataset_attestation": {
            "dataset_id": dataset_id,
            "dataset_version": dataset_version,
            "status": "CURRENT",
            "policy_version": "v1",
            "attestation_hash": "fixture-attestation",
        },
        "market_scope": {
            "schema_version": "1",
            "mode": "EXACT_MARKETS",
            "instrument": "POLYMARKET",
            "categories": [],
            "market_ids": ["market-1"],
            "filters": {},
            "regime_restrictions": {},
            "provenance": "canonical",
        },
        "market_scope_hash": "sha256:fixture-market-scope",
        "market_scope_version": "market-scope-v1",
        "plan_hash": "sha256:fixture-plan",
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
        payload["target_market_ids"] = ["market-1"]
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
        self.store.save_market_scope_resolution(
            {
                "candidate_id": candidate_id,
                "scope_hash": payload["market_scope_hash"],
                "scope_version": payload["market_scope_version"],
                "resolved_at": T0,
                "status": "MATCHED",
                "reason": "MATCHED",
                "policy": payload["market_scope"],
                "matched_markets": [
                    {
                        "market_id": "market-1",
                        "condition_id": "condition-1",
                        "yes_token_id": "yes",
                        "no_token_id": "no",
                        "metadata_provenance": {"source": "fixture"},
                    }
                ],
                "provenance": {"source": "fixture"},
            }
        )
        return payload

    def bind_fixture_scope(self, candidate_id: str, payload: Mapping[str, object]) -> None:
        self.store.save_market_scope_resolution(
            {
                "candidate_id": candidate_id,
                "scope_hash": payload["market_scope_hash"],
                "scope_version": payload["market_scope_version"],
                "resolved_at": T0,
                "status": "MATCHED",
                "reason": "MATCHED",
                "policy": payload["market_scope"],
                "matched_markets": [
                    {
                        "market_id": "market-1",
                        "condition_id": "condition-1",
                        "yes_token_id": "yes",
                        "no_token_id": "no",
                        "metadata_provenance": {"source": "fixture"},
                    }
                ],
                "provenance": {"source": "fixture"},
            }
        )
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
        self.store.save_polymarket_market_metadata(
            "market-1",
            {
                "market_id": "market-1",
                "active": True,
                "closed": False,
                "settlement": "open",
            },
            observed_at=T0,
            source_type="FORWARD_COLLECTED",
        )
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

    def test_node_initializing_publication_is_cheap_and_not_last_good(self):
        node = ResearchNode(
            NodeConfig(":memory:", crypto_enabled=False),
            provider=object(),
            store=self.store,
            clock=lambda: T0,
            sleep=lambda _: None,
        )
        with patch.object(
            CanaryService,
            "authoritative_status",
            side_effect=AssertionError("startup must not evaluate authoritative status"),
        ), patch.object(
            CandidateCanaryRanker,
            "evaluate_and_select",
            side_effect=AssertionError("startup must not rank candidates"),
        ), patch(
            "axiom.canary.evaluate_prediction_data_quality",
            side_effect=AssertionError("startup must not evaluate data quality"),
        ):
            node._publish_autonomous_initializing(T0)

        row = self.store.connection.execute(
            "SELECT projection_version,readiness_snapshot_status,"
            "readiness_snapshot_stale,readiness_snapshot_reason "
            "FROM canary_readiness_snapshot WHERE singleton=1"
        ).fetchone()
        self.assertEqual(row["projection_version"], 0)
        self.assertEqual(row["readiness_snapshot_status"], "STALE")
        self.assertEqual(row["readiness_snapshot_stale"], 1)
        self.assertEqual(row["readiness_snapshot_reason"], "READINESS_SNAPSHOT_INITIALIZING")
        status = self.service.status()
        self.assertEqual(status["selection_status"], "UNKNOWN")
        self.assertIsNone(status["selection_valid"])
        self.assertIsNone(status["selected_candidate"])
        self.assertIsNone(status["winner_id"])
        for field in (
            "eligibility_raw_count",
            "eligible_count",
            "rankable_raw_count",
            "rankable_count",
        ):
            self.assertIsNone(status[field])
        self.assertEqual(status["autonomous"]["worker_status"], "INITIALIZING")
        self.assertIsNone(status["autonomous"]["last_successful_tick"])

    def test_first_successful_ranker_tick_publishes_current_projection(self):
        self.seed_candidate("startup-ranker", score=0.40)
        node = ResearchNode(
            NodeConfig(":memory:", crypto_enabled=False),
            provider=object(),
            store=self.store,
            clock=lambda: T0,
            sleep=lambda _: None,
        )
        node._publish_autonomous_initializing(T0)

        result = node._auto_canary_worker.tick(now=T0)
        self.assertEqual(result["status"], "DISABLED")
        row = self.store.connection.execute(
            "SELECT projection_version,readiness_snapshot_status,"
            "readiness_snapshot_stale FROM canary_readiness_snapshot "
            "WHERE singleton=1"
        ).fetchone()
        self.assertGreater(row["projection_version"], 0)
        self.assertEqual(row["readiness_snapshot_status"], "CURRENT")
        self.assertEqual(row["readiness_snapshot_stale"], 0)

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
        with patch.object(
            CanaryService,
            "validate_eligibility",
            side_effect=AssertionError("dashboard projection must not qualify"),
        ):
            fields = dashboard._candidate_status_fields(record)
        self.assertFalse(fields["canary_eligible"])
        self.assertEqual(fields["historical_gates"], "NOT_PASSED")
        self.assertEqual(fields["historical_data_integrity"], "UNKNOWN")
        self.assertEqual(fields["historical_execution_fidelity"], "UNKNOWN")
        self.assertEqual(fields["canary_data_quality_gate"], "NOT PASSED")
        self.assertEqual(fields["production_evidence"], "INSUFFICIENT")

        self.service.mark_eligible("gated")
        with patch.object(
            CanaryService,
            "validate_eligibility",
            side_effect=AssertionError("dashboard projection must not qualify"),
        ):
            eligible = dashboard._candidate_status_fields(
                self.store.load_candidate_lifecycle("gated")
            )
        self.assertTrue(eligible["canary_eligible"])

        changed = dict(payload)
        changed["validation_passed"] = False
        self.store.save_candidate_lifecycle("gated", "FROZEN", changed, from_stage="FROZEN", timestamp=T0)
        authoritative = self.service.validate_eligibility("gated")
        self.assertFalse(authoritative["eligible"])
        with patch.object(
            CanaryService,
            "validate_eligibility",
            side_effect=AssertionError("dashboard projection must not qualify"),
        ):
            invalid = dashboard._candidate_status_fields(
                self.store.load_candidate_lifecycle("gated")
            )
        self.assertTrue(invalid["canary_eligible"])
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

    def test_worker_durably_scans_eligible_candidate_without_ranking_evidence(self):
        candidate_id = "fresh-without-ranking"
        self.seed_candidate(candidate_id)
        self.service.mark_eligible(candidate_id)
        self.service.enable_autonomous_micro_live()
        checked: list[str] = []

        def evaluate_signal(service, identifier, **kwargs):
            checked.append(identifier)
            return service._persist_signal_evaluation(
                {
                    "candidate_id": identifier,
                    "cycle_id": kwargs.get("cycle_id"),
                    "evaluated_at": T0.isoformat(),
                    "reason_code": "CANDIDATE_FORWARD_MARKET_UNRESOLVED",
                    "market_id": None,
                    "signal": None,
                    "required_health": {},
                    "evidence": {
                        "authority_reason_code": "CANDIDATE_FORWARD_MARKET_UNRESOLVED"
                    },
                }
            )

        worker = AutonomousCanaryWorker(self.store, clock=lambda: T0)
        with patch.object(
            CandidateCanaryRanker,
            "evaluate_and_select",
            return_value={
                "ranking_run_id": "empty-ranking-run",
                "eligible_count": 1,
                "rankable_count": 0,
                "rankings": [],
            },
        ), patch.object(
            CanaryService,
            "evaluate_signal",
            autospec=True,
            side_effect=evaluate_signal,
        ):
            result = worker.tick(now=T0)

        self.assertEqual(result["status"], "BLOCKED")
        self.assertEqual(
            result["decision"],
            "CANDIDATE_FORWARD_MARKET_UNRESOLVED",
        )
        self.assertEqual(result["candidates_ranked"], 0)
        self.assertEqual(result["candidates_signal_checked"], 1)
        self.assertEqual(result["candidate_id"], candidate_id)
        self.assertEqual(result["signal_scan_remaining_this_cycle"], 0)
        self.assertTrue(result["signal_scan_cycle_complete"])
        reason_counts = result["signal_scan_reason_counts_json"]
        if isinstance(reason_counts, str):
            reason_counts = json.loads(reason_counts)
        self.assertEqual(
            reason_counts["CANDIDATE_FORWARD_MARKET_UNRESOLVED"],
            1,
        )
        checked_row = self.store.connection.execute(
            "SELECT candidate_id,qualification_hash FROM canary_signal_scan_checked "
            "WHERE cycle_id=?",
            (result["signal_scan_cycle_id"],),
        ).fetchone()
        self.assertIsNotNone(checked_row)
        self.assertEqual(checked_row["candidate_id"], candidate_id)
        self.assertTrue(checked_row["qualification_hash"])
        evaluation_row = self.store.connection.execute(
            "SELECT reason_code FROM canary_signal_evaluations "
            "WHERE candidate_id=? ORDER BY evaluated_at DESC LIMIT 1",
            (candidate_id,),
        ).fetchone()
        self.assertIsNotNone(evaluation_row)
        self.assertEqual(
            evaluation_row["reason_code"],
            "CANDIDATE_FORWARD_MARKET_UNRESOLVED",
        )

    def test_worker_scans_rank0_follower_after_empty_representative_and_submits_same_tick(self):
        shared_payload = candidate_payload(
            "shared-strategy",
            cluster="shared-cluster",
            score=0.90,
            executable=True,
        )
        follower_payload = dict(shared_payload)
        follower_payload["validation_expectancy"] = 0.80
        follower_payload["validation_confidence_lower_bound"] = 0.80
        for candidate_id, payload in (
            ("same-cluster-representative", shared_payload),
            ("same-cluster-follower", follower_payload),
        ):
            self.store.save_candidate_lifecycle(
                candidate_id,
                "IDEA",
                payload,
                timestamp=T0,
            )
            self.store.save_candidate_lifecycle(
                candidate_id,
                "FROZEN",
                payload,
                timestamp=T0,
            )
            self.bind_fixture_scope(candidate_id, payload)
        self.save_forward_canary_snapshot()
        self.service.enable_autonomous_micro_live()
        venue = TestVenue()
        checked: list[str] = []
        generated_follower_signal: dict[str, object] = {}
        original_evaluate_signal = CanaryService.evaluate_signal

        def evaluate(service, candidate_id, **kwargs):
            checked.append(candidate_id)
            if candidate_id == "same-cluster-representative":
                return self._signal_evaluation(candidate_id, "NO_STRATEGY_SIGNAL")
            evaluation = original_evaluate_signal(
                service,
                candidate_id,
                **kwargs,
            )
            signal = (
                evaluation.get("signal")
                if isinstance(evaluation, Mapping)
                else None
            )
            generated_follower_signal["signal"] = signal
            return evaluation

        worker = AutonomousCanaryWorker(
            self.store,
            clock=lambda: T0,
            venue_factory=lambda: venue,
            allow_test_venue=True,
        )
        with patch.object(
            CanaryService,
            "evaluate_signal",
            autospec=True,
            side_effect=evaluate,
        ), patch.object(CredentialStore, "configured", return_value=True):
            result = worker.tick(now=T0)

        self.assertEqual(result["status"], "SUBMITTED")
        self.assertEqual(result["candidate_id"], "same-cluster-follower")
        self.assertEqual(result["candidates_ranked"], 2)
        self.assertEqual(result["candidates_signal_checked"], 2)
        self.assertEqual(result["candidates_no_signal"], 1)
        self.assertEqual(result["actionable_candidates_found"], 1)
        self.assertEqual(result["selected_actionable_candidate"], "same-cluster-follower")
        self.assertEqual(result["selected_actionable_rank"], 2)
        self.assertEqual(result["signal_scan_cursor"], 0)
        self.assertEqual(result["next_signal_scan_start_rank"], 0)
        self.assertEqual(result["next_signal_scan_end_rank"], 2)
        self.assertEqual(
            checked,
            ["same-cluster-representative", "same-cluster-follower"],
        )
        follower_signal = generated_follower_signal["signal"]
        self.assertIsInstance(follower_signal, dict)
        assert isinstance(follower_signal, dict)
        self.assertEqual(follower_signal["status"], "READY")
        self.assertEqual(follower_signal["candidate_id"], "same-cluster-follower")
        self.assertEqual(len(venue.submissions), 1)
        ranking_rows = {
            row["candidate_id"]: row
            for row in CandidateCanaryRanker(self.store, clock=lambda: T0).rankings()
        }
        self.assertEqual(ranking_rows["same-cluster-representative"]["rank"], 1)
        self.assertEqual(ranking_rows["same-cluster-representative"]["cluster_representative"], 1)
        self.assertEqual(ranking_rows["same-cluster-follower"]["rank"], 0)
        self.assertEqual(
            ranking_rows["same-cluster-follower"]["reason"],
            "DIVERSITY_CLUSTER_NON_REPRESENTATIVE",
        )

    def test_venue_factory_failure_is_retryable_without_unknown_signal_or_order_attempt(self):
        self.seed_candidate("venue-retry", executable=True)
        self.save_forward_canary_snapshot()
        self.service.enable_autonomous_micro_live()
        venues: list[TestVenue] = []
        factory_calls = 0

        def venue_factory():
            nonlocal factory_calls
            factory_calls += 1
            if factory_calls == 1:
                raise RuntimeError("venue construction failed")
            venue = TestVenue()
            venues.append(venue)
            return venue

        worker = AutonomousCanaryWorker(
            self.store,
            clock=lambda: T0,
            venue_factory=venue_factory,
            allow_test_venue=True,
        )
        with patch.object(CredentialStore, "configured", return_value=True):
            first = worker.tick(now=T0)
            signal = self.service.latest_signal("venue-retry")
            self.assertIsNotNone(signal)
            assert signal is not None
            self.assertEqual(signal["status"], "READY")
            self.assertEqual(
                self.service.status()["autonomous"]["orders_attempted"],
                0,
            )
            self.assertEqual(
                self.store.connection.execute(
                    "SELECT COUNT(*) FROM canary_ledger"
                ).fetchone()[0],
                0,
            )
            second = worker.tick(now=T0)

        self.assertEqual(first["status"], "ERROR")
        self.assertEqual(first["decision"], "AUTONOMOUS_WORKER_EXCEPTION")
        self.assertEqual(first["blocker"], "AUTONOMOUS_WORKER_EXCEPTION")
        self.assertEqual(first["error_type"], "RuntimeError")
        self.assertEqual(second["status"], "SUBMITTED")
        self.assertEqual(second["candidate_id"], "venue-retry")
        self.assertEqual(factory_calls, 2)
        self.assertEqual(len(venues), 1)
        self.assertEqual(len(venues[0].submissions), 1)
        self.assertEqual(
            self.store.connection.execute(
                "SELECT status FROM canary_ledger WHERE signal_id=?",
                (signal["signal_id"],),
            ).fetchone()[0],
            "SUBMITTED",
        )

    def test_worker_uses_authoritative_control_when_dashboard_snapshot_is_missing(self):
        self.seed_candidate("durable-winner", score=0.90)
        enabled = self.service.enable_autonomous_micro_live()
        self.assertEqual(enabled["micro_live_canary"], AUTONOMOUS_MICRO_LIVE)

        # The durable control row remains authoritative when the bounded
        # readiness projection is absent.  The projection reports live
        # control truthfully while leaving readiness and quality claims
        # unknown until a fresh publication completes.
        with self.store.connection:
            self.store.connection.execute(
                "DELETE FROM canary_readiness_snapshot WHERE singleton=1"
            )
        dashboard_projection = self.service.status()
        self.assertEqual(
            dashboard_projection["micro_live_canary"],
            AUTONOMOUS_MICRO_LIVE,
        )
        self.assertEqual(dashboard_projection["readiness_snapshot_status"], "STALE")
        self.assertTrue(dashboard_projection["readiness_snapshot_stale"])
        self.assertEqual(
            dashboard_projection["readiness_snapshot_reason"],
            "READINESS_SNAPSHOT_MISSING",
        )
        self.assertEqual(dashboard_projection["historical_data_integrity"], "UNKNOWN")
        self.assertEqual(
            dashboard_projection["historical_execution_fidelity"],
            "UNKNOWN",
        )
        self.assertEqual(dashboard_projection["selection_status"], "UNKNOWN")
        dashboard_projection.update(
            {
                "candidate": "dashboard-only",
                "selected_candidate": "dashboard-only",
            }
        )

        worker = AutonomousCanaryWorker(self.store, clock=lambda: T0)
        with patch.object(
            CanaryService,
            "publish_readiness_snapshot",
            autospec=True,
            side_effect=lambda service, reason="AUTHORITATIVE_UPDATE": (
                service.authoritative_status()
            ),
        ), patch.object(CanaryService, "status", return_value=dashboard_projection):
            result = worker.tick(now=T0)

        self.assertEqual(result["status"], "BLOCKED")
        self.assertEqual(result["blocker"], "CANDIDATE_EXECUTABLE_DOCUMENTS_UNAVAILABLE")
        self.assertEqual(result["candidate_id"], "durable-winner")
        authoritative = self.service.authoritative_status()
        self.assertEqual(authoritative["micro_live_canary"], AUTONOMOUS_MICRO_LIVE)
        self.assertEqual(authoritative["candidate"], "durable-winner")

    def test_ranker_requires_canonical_scope_successor_for_legacy_payload(self):
        payload = candidate_payload("legacy")
        source_payload = dict(payload)
        for field in (
            "market_scope",
            "market_scope_hash",
            "market_scope_version",
            "plan_hash",
            "dataset_selector",
            "dataset_attestation",
        ):
            source_payload.pop(field, None)
        self.store.save_candidate_lifecycle(
            "legacy",
            "IDEA",
            source_payload,
            timestamp=T0,
        )
        self.store.save_candidate_lifecycle(
            "legacy",
            "FROZEN",
            source_payload,
            timestamp=T0,
        )
        with self.store.connection:
            self.store.connection.execute(
                "INSERT INTO canary_eligibility(candidate_id,eligible_at,frozen_hash,evidence_json) "
                "VALUES(?,?,?,?)",
                (
                    "legacy",
                    T0.isoformat(),
                    source_payload["frozen_hash"],
                    json.dumps(source_payload, sort_keys=True),
                ),
            )
        before = self.store.load_candidate_lifecycle("legacy")
        result = CandidateCanaryRanker(self.store, clock=lambda: T0).evaluate_and_select(T0)
        after = self.store.load_candidate_lifecycle("legacy")
        self.assertEqual(after["payload"], before["payload"])
        self.assertIsNone(result["selected_candidate"])
        self.assertEqual(result["eligible_count"], 0)
        self.assertEqual(result["rankable_count"], 0)
        self.assertEqual(self.store.connection.execute(
            "SELECT COUNT(*) FROM canary_eligibility WHERE candidate_id=?",
            ("legacy",),
        ).fetchone()[0], 0)
        self.assertEqual(
            CandidateCanaryRanker(self.store, clock=lambda: T0).rankings(),
            [],
        )

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

    def test_older_ranker_failure_cannot_clobber_newer_current_projection(self):
        self.service.publish_readiness_snapshot(reason="RANKING_BASELINE")
        ranker = CandidateCanaryRanker(self.store, clock=lambda: T0)

        def stale_evaluation(now=None):
            self.service.publish_readiness_snapshot(reason="NEWER_CURRENT")
            raise RuntimeError("older ranking failed")

        ranker._evaluate_and_select = stale_evaluation
        with self.assertRaisesRegex(RuntimeError, "older ranking failed"):
            ranker.evaluate_and_select(T0)

        row = self.store.connection.execute(
            "SELECT payload_json,projection_version,readiness_snapshot_status,"
            "readiness_snapshot_stale,readiness_snapshot_reason "
            "FROM canary_readiness_snapshot WHERE singleton=1"
        ).fetchone()
        self.assertEqual(row["readiness_snapshot_status"], "CURRENT")
        self.assertEqual(row["readiness_snapshot_stale"], 0)
        self.assertEqual(row["readiness_snapshot_reason"], "NEWER_CURRENT")
        payload = json.loads(row["payload_json"])
        self.assertEqual(payload["readiness_snapshot_reason"], "NEWER_CURRENT")
        self.assertNotEqual(
            payload.get("readiness_evaluation_error_code"),
            "RUNTIMEERROR",
        )

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
        self.assertIn("error", result_holder)
        self.assertEqual(
            getattr(result_holder["error"], "error_code", None),
            "LIFECYCLE_SNAPSHOT_CHANGED",
        )
        self.assertIsNone(ranker.current_selection())
        self.assertEqual(ranker.rankings(), [])
        snapshot = self.store.connection.execute(
            "SELECT payload_json,readiness_snapshot_status,"
            "readiness_snapshot_stale,readiness_snapshot_reason "
            "FROM canary_readiness_snapshot WHERE singleton=1"
        ).fetchone()
        self.assertEqual(snapshot["readiness_snapshot_status"], "STALE")
        self.assertEqual(snapshot["readiness_snapshot_stale"], 1)
        self.assertEqual(snapshot["readiness_snapshot_reason"], "EVALUATION_FAILED")
        failure = json.loads(snapshot["payload_json"])
        self.assertEqual(failure["readiness_snapshot_reason"], "EVALUATION_FAILED")
        self.assertEqual(failure["readiness_evaluation_error_code"], "LIFECYCLE_SNAPSHOT_CHANGED")
        retried = ranker.evaluate_and_select(T0)
        self.assertEqual(retried["selection_status"], "CURRENT")
        self.assertEqual(retried["selected_candidate"], "paper-race")
        self.assertEqual(self.service.status()["readiness_snapshot_status"], "CURRENT")
        self.assertEqual(
            self.service.status()["readiness_snapshot_reason"],
            "RANKING_EVALUATED",
        )

    def test_concurrent_c_d_telemetry_update_does_not_abort_ranker(self):
        candidate_id = "telemetry-race"
        payload = self.seed_candidate(candidate_id, score=0.40)
        paper_payload = {**payload, "forward_expectancy": 0.40}
        self.store.save_candidate_lifecycle(
            candidate_id,
            "PAPER_FORWARD",
            paper_payload,
            from_stage="FROZEN",
            timestamp=T0,
        )
        ranker = CandidateCanaryRanker(self.store, clock=lambda: T0)
        self.service.publish_readiness_snapshot(reason="C_D_BASELINE")
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
                candidate_id,
                {
                    "forward_evidence": {
                        "forward_liquidity": 0.42,
                        "forward_max_drawdown": 0.05,
                        "forward_fills": 9,
                        "forward_observations": 77,
                    }
                },
                expected_stage=CandidateStage.PAPER_FORWARD,
                reason="concurrent telemetry-only update",
            )
            mid_update = self.service.status()
            self.assertEqual(mid_update["readiness_snapshot_status"], "CURRENT")
            self.assertEqual(
                mid_update["readiness_snapshot_reason"],
                "C_D_BASELINE",
            )
        finally:
            release.set()
            ranking_thread.join(timeout=5)

        self.assertFalse(ranking_thread.is_alive())
        self.assertNotIn("error", result_holder)
        self.assertEqual(result_holder["result"]["selection_status"], "CURRENT")
        self.assertEqual(
            result_holder["result"]["selected_candidate"],
            candidate_id,
        )
        self.assertEqual(
            self.service.status()["readiness_snapshot_status"],
            "CURRENT",
        )

    def test_new_candidate_between_prevalidation_and_commit_fails_closed(self):
        candidate_id = "inventory-race"
        self.seed_candidate(candidate_id, score=0.40)
        ranker = CandidateCanaryRanker(self.store, clock=lambda: T0)
        initial = ranker.evaluate_and_select(T0)
        self.assertEqual(initial["selection_status"], "CURRENT")
        baseline_ranking = dict(
            self.store.connection.execute(
                "SELECT * FROM canary_rankings WHERE candidate_id=?",
                (candidate_id,),
            ).fetchone()
        )
        baseline_selection = dict(
            self.store.connection.execute(
                "SELECT * FROM canary_selection WHERE singleton=1"
            ).fetchone()
        )
        entered = threading.Event()
        release = threading.Event()
        result_holder = {}
        original_rank_evidence = ranker._rank_evidence

        def blocked_rank_evidence(payload, frozen_hash, quality=None):
            entered.set()
            if not release.wait(timeout=5):
                raise AssertionError("ranking barrier was not released")
            return original_rank_evidence(payload, frozen_hash, quality=quality)

        ranker._rank_evidence = blocked_rank_evidence

        def run_ranker():
            try:
                ranker.evaluate_and_select(T0)
            except BaseException as exc:
                result_holder["error"] = exc

        ranking_thread = threading.Thread(target=run_ranker)
        ranking_thread.start()
        try:
            self.assertTrue(entered.wait(timeout=5))
            self.seed_candidate("inventory-new", score=0.90)
        finally:
            release.set()
            ranking_thread.join(timeout=5)

        self.assertFalse(ranking_thread.is_alive())
        self.assertEqual(
            getattr(result_holder.get("error"), "error_code", None),
            "LIFECYCLE_SNAPSHOT_CHANGED",
        )
        after_ranking = dict(
            self.store.connection.execute(
                "SELECT * FROM canary_rankings WHERE candidate_id=?",
                (candidate_id,),
            ).fetchone()
        )
        after_selection = dict(
            self.store.connection.execute(
                "SELECT * FROM canary_selection WHERE singleton=1"
            ).fetchone()
        )
        self.assertEqual(after_ranking, baseline_ranking)
        self.assertEqual(after_selection, baseline_selection)
        self.assertIsNone(
            self.store.connection.execute(
                "SELECT 1 FROM canary_rankings WHERE candidate_id='inventory-new'"
            ).fetchone()
        )
        snapshot = self.store.connection.execute(
            "SELECT payload_json,readiness_snapshot_status,"
            "readiness_snapshot_stale,readiness_snapshot_reason "
            "FROM canary_readiness_snapshot WHERE singleton=1"
        ).fetchone()
        self.assertEqual(snapshot["readiness_snapshot_status"], "STALE")
        self.assertEqual(snapshot["readiness_snapshot_stale"], 1)
        self.assertEqual(snapshot["readiness_snapshot_reason"], "EVALUATION_FAILED")
        failure = json.loads(snapshot["payload_json"])
        self.assertEqual(failure["readiness_snapshot_reason"], "EVALUATION_FAILED")
        self.assertEqual(failure["last_selected_candidate"], candidate_id)
        self.assertEqual(
            failure["readiness_evaluation_error_code"],
            "LIFECYCLE_SNAPSHOT_CHANGED",
        )

    def test_attestation_policy_change_between_prevalidation_and_commit_fails_closed(self):
        candidate_id = "attestation-policy-race"
        self.seed_candidate(candidate_id, score=0.40)
        ranker = CandidateCanaryRanker(self.store, clock=lambda: T0)
        initial = ranker.evaluate_and_select(T0)
        self.assertEqual(initial["selection_status"], "CURRENT")
        baseline_ranking = dict(
            self.store.connection.execute(
                "SELECT * FROM canary_rankings WHERE candidate_id=?",
                (candidate_id,),
            ).fetchone()
        )
        baseline_selection = dict(
            self.store.connection.execute(
                "SELECT * FROM canary_selection WHERE singleton=1"
            ).fetchone()
        )
        entered = threading.Event()
        release = threading.Event()
        result_holder = {}
        original_rank_evidence = ranker._rank_evidence

        def blocked_rank_evidence(payload, frozen_hash, quality=None):
            entered.set()
            if not release.wait(timeout=5):
                raise AssertionError("ranking barrier was not released")
            return original_rank_evidence(payload, frozen_hash, quality=quality)

        ranker._rank_evidence = blocked_rank_evidence

        def run_ranker():
            try:
                ranker.evaluate_and_select(T0)
            except BaseException as exc:
                result_holder["error"] = exc

        ranking_thread = threading.Thread(target=run_ranker)
        ranking_thread.start()
        try:
            self.assertTrue(entered.wait(timeout=5))
            attestation = self.store.load_dataset_integrity_attestation(
                "prediction-history",
                "v1",
            )
            self.assertIsNotNone(attestation)
            changed = dict(attestation)
            changed["policy_version"] = "prediction-integrity-v2"
            changed.pop("attestation_hash", None)
            self.store.save_dataset_integrity_attestation(
                "prediction-history",
                "v1",
                changed,
            )
        finally:
            release.set()
            ranking_thread.join(timeout=5)

        self.assertFalse(ranking_thread.is_alive())
        self.assertEqual(
            getattr(result_holder.get("error"), "error_code", None),
            "DATASET_EVIDENCE_CHANGED",
        )
        after_ranking = dict(
            self.store.connection.execute(
                "SELECT * FROM canary_rankings WHERE candidate_id=?",
                (candidate_id,),
            ).fetchone()
        )
        after_selection = dict(
            self.store.connection.execute(
                "SELECT * FROM canary_selection WHERE singleton=1"
            ).fetchone()
        )
        self.assertEqual(after_ranking, baseline_ranking)
        self.assertEqual(after_selection, baseline_selection)
        snapshot = self.store.connection.execute(
            "SELECT payload_json,readiness_snapshot_status,"
            "readiness_snapshot_stale,readiness_snapshot_reason "
            "FROM canary_readiness_snapshot WHERE singleton=1"
        ).fetchone()
        self.assertEqual(snapshot["readiness_snapshot_status"], "STALE")
        self.assertEqual(snapshot["readiness_snapshot_stale"], 1)
        self.assertEqual(snapshot["readiness_snapshot_reason"], "EVALUATION_FAILED")
        failure = json.loads(snapshot["payload_json"])
        self.assertEqual(failure["readiness_snapshot_reason"], "EVALUATION_FAILED")
        self.assertEqual(failure["last_selected_candidate"], candidate_id)
        self.assertEqual(
            failure["readiness_evaluation_error_code"],
            "DATASET_EVIDENCE_CHANGED",
        )

    def test_qualification_lifecycle_change_between_prevalidation_and_commit_fails_closed(self):
        candidate_id = "qualification-race"
        payload = self.seed_candidate(candidate_id, score=0.40)
        ranker = CandidateCanaryRanker(self.store, clock=lambda: T0)
        initial = ranker.evaluate_and_select(T0)
        self.assertEqual(initial["selection_status"], "CURRENT")
        baseline_ranking = dict(
            self.store.connection.execute(
                "SELECT * FROM canary_rankings WHERE candidate_id=?",
                (candidate_id,),
            ).fetchone()
        )
        baseline_selection = dict(
            self.store.connection.execute(
                "SELECT * FROM canary_selection WHERE singleton=1"
            ).fetchone()
        )
        entered = threading.Event()
        release = threading.Event()
        result_holder = {}
        original_rank_evidence = ranker._rank_evidence

        def blocked_rank_evidence(payload, frozen_hash, quality=None):
            entered.set()
            if not release.wait(timeout=5):
                raise AssertionError("ranking barrier was not released")
            return original_rank_evidence(payload, frozen_hash, quality=quality)

        ranker._rank_evidence = blocked_rank_evidence

        def run_ranker():
            try:
                ranker.evaluate_and_select(T0)
            except BaseException as exc:
                result_holder["error"] = exc

        ranking_thread = threading.Thread(target=run_ranker)
        ranking_thread.start()
        try:
            self.assertTrue(entered.wait(timeout=5))
            changed = dict(payload)
            changed["config_hash"] = "config-race"
            changed["frozen_hash"] = hashlib.sha256(
                "|".join(
                    (
                        changed["strategy_hash"],
                        changed["model_hash"],
                        changed["config_hash"],
                    )
                ).encode()
            ).hexdigest()
            self.store.save_candidate_lifecycle(
                candidate_id,
                "FROZEN",
                changed,
                from_stage="FROZEN",
                timestamp=T0.replace(second=13),
            )
        finally:
            release.set()
            ranking_thread.join(timeout=5)

        self.assertFalse(ranking_thread.is_alive())
        self.assertEqual(
            getattr(result_holder.get("error"), "error_code", None),
            "LIFECYCLE_SNAPSHOT_CHANGED",
        )
        after_ranking = dict(
            self.store.connection.execute(
                "SELECT * FROM canary_rankings WHERE candidate_id=?",
                (candidate_id,),
            ).fetchone()
        )
        after_selection = dict(
            self.store.connection.execute(
                "SELECT * FROM canary_selection WHERE singleton=1"
            ).fetchone()
        )
        self.assertEqual(after_ranking, baseline_ranking)
        self.assertEqual(after_selection, baseline_selection)
        snapshot = self.store.connection.execute(
            "SELECT payload_json,readiness_snapshot_status,"
            "readiness_snapshot_stale,readiness_snapshot_reason "
            "FROM canary_readiness_snapshot WHERE singleton=1"
        ).fetchone()
        self.assertEqual(snapshot["readiness_snapshot_status"], "STALE")
        self.assertEqual(snapshot["readiness_snapshot_stale"], 1)
        self.assertEqual(snapshot["readiness_snapshot_reason"], "EVALUATION_FAILED")
        failure = json.loads(snapshot["payload_json"])
        self.assertEqual(failure["readiness_snapshot_reason"], "EVALUATION_FAILED")
        self.assertEqual(failure["last_selected_candidate"], candidate_id)
        self.assertEqual(
            failure["readiness_evaluation_error_code"],
            "LIFECYCLE_SNAPSHOT_CHANGED",
        )

    def test_critical_error_toggle_between_prevalidation_and_commit_aborts_stale_rank(self):
        candidate_id = "critical-error-race"
        self.seed_candidate(candidate_id, score=0.40)
        ranker = CandidateCanaryRanker(self.store, clock=lambda: T0)
        initial = ranker.evaluate_and_select(T0)
        self.assertEqual(initial["selection_status"], "CURRENT")
        baseline_ranking = dict(
            self.store.connection.execute(
                "SELECT * FROM canary_rankings WHERE candidate_id=?",
                (candidate_id,),
            ).fetchone()
        )
        baseline_selection = dict(
            self.store.connection.execute(
                "SELECT * FROM canary_selection WHERE singleton=1"
            ).fetchone()
        )
        entered = threading.Event()
        release = threading.Event()
        result_holder = {}
        original_rank_evidence = ranker._rank_evidence

        def blocked_rank_evidence(payload, frozen_hash, quality=None):
            entered.set()
            if not release.wait(timeout=5):
                raise AssertionError("ranking barrier was not released")
            return original_rank_evidence(payload, frozen_hash, quality=quality)

        ranker._rank_evidence = blocked_rank_evidence

        def run_ranker():
            try:
                ranker.evaluate_and_select(T0)
            except BaseException as exc:
                result_holder["error"] = exc

        ranking_thread = threading.Thread(target=run_ranker)
        ranking_thread.start()
        try:
            self.assertTrue(entered.wait(timeout=5))
            CandidateLifecycleManager(self.store).record_evidence(
                candidate_id,
                {"critical_error": "runtime-failure"},
                expected_stage=CandidateStage.FROZEN,
                reason="critical error toggled during ranking",
            )
        finally:
            release.set()
            ranking_thread.join(timeout=5)

        self.assertFalse(ranking_thread.is_alive())
        self.assertEqual(
            getattr(result_holder.get("error"), "error_code", None),
            "LIFECYCLE_SNAPSHOT_CHANGED",
        )
        after_ranking = dict(
            self.store.connection.execute(
                "SELECT * FROM canary_rankings WHERE candidate_id=?",
                (candidate_id,),
            ).fetchone()
        )
        after_selection = dict(
            self.store.connection.execute(
                "SELECT * FROM canary_selection WHERE singleton=1"
            ).fetchone()
        )
        self.assertEqual(after_ranking, baseline_ranking)
        self.assertEqual(after_selection, baseline_selection)
        validation = self.service.validate_eligibility(candidate_id)
        self.assertFalse(validation["eligible"])
        self.assertFalse(
            next(check for check in validation["checks"] if check["name"] == "Critical errors")[
                "passed"
            ]
        )
        snapshot = self.store.connection.execute(
            "SELECT readiness_snapshot_status,readiness_snapshot_stale,"
            "readiness_snapshot_reason,payload_json "
            "FROM canary_readiness_snapshot WHERE singleton=1"
        ).fetchone()
        self.assertEqual(snapshot["readiness_snapshot_status"], "STALE")
        self.assertEqual(snapshot["readiness_snapshot_stale"], 1)
        self.assertEqual(snapshot["readiness_snapshot_reason"], "EVALUATION_FAILED")
        self.assertEqual(
            json.loads(snapshot["payload_json"])["readiness_evaluation_error_code"],
            "LIFECYCLE_SNAPSHOT_CHANGED",
        )

    def test_top_level_ranking_alias_change_aborts_stale_rank_commit(self):
        candidate_id = "ranking-alias-race"
        payload = candidate_payload(candidate_id, score=0.40)
        payload.pop("validation_expectancy")
        payload["expectancy"] = 0.40
        self.store.save_candidate_lifecycle(candidate_id, "IDEA", payload, timestamp=T0)
        self.store.save_candidate_lifecycle(candidate_id, "FROZEN", payload, timestamp=T0)
        ranker = CandidateCanaryRanker(self.store, clock=lambda: T0)
        initial = ranker.evaluate_and_select(T0)
        self.assertEqual(initial["selection_status"], "CURRENT")
        baseline_ranking = dict(
            self.store.connection.execute(
                "SELECT * FROM canary_rankings WHERE candidate_id=?",
                (candidate_id,),
            ).fetchone()
        )
        baseline_selection = dict(
            self.store.connection.execute(
                "SELECT * FROM canary_selection WHERE singleton=1"
            ).fetchone()
        )
        entered = threading.Event()
        release = threading.Event()
        result_holder = {}
        original_rank_evidence = ranker._rank_evidence

        def blocked_rank_evidence(payload, frozen_hash, quality=None):
            entered.set()
            if not release.wait(timeout=5):
                raise AssertionError("ranking barrier was not released")
            return original_rank_evidence(payload, frozen_hash, quality=quality)

        ranker._rank_evidence = blocked_rank_evidence

        def run_ranker():
            try:
                ranker.evaluate_and_select(T0)
            except BaseException as exc:
                result_holder["error"] = exc

        ranking_thread = threading.Thread(target=run_ranker)
        ranking_thread.start()
        try:
            self.assertTrue(entered.wait(timeout=5))
            CandidateLifecycleManager(self.store).record_evidence(
                candidate_id,
                {"expectancy": 0.80},
                expected_stage=CandidateStage.FROZEN,
                reason="ranking alias changed during ranking",
            )
        finally:
            release.set()
            ranking_thread.join(timeout=5)

        self.assertFalse(ranking_thread.is_alive())
        self.assertEqual(
            getattr(result_holder.get("error"), "error_code", None),
            "LIFECYCLE_SNAPSHOT_CHANGED",
        )
        self.assertEqual(
            dict(
                self.store.connection.execute(
                    "SELECT * FROM canary_rankings WHERE candidate_id=?",
                    (candidate_id,),
                ).fetchone()
            ),
            baseline_ranking,
        )
        self.assertEqual(
            dict(
                self.store.connection.execute(
                    "SELECT * FROM canary_selection WHERE singleton=1"
                ).fetchone()
            ),
            baseline_selection,
        )
        status = self.service.status()
        self.assertEqual(status["selection_status"], "STALE")
        self.assertFalse(status["selection_valid"])
        self.assertIsNone(status["selected_candidate"])

    def test_nested_market_source_change_aborts_stale_rank_commit(self):
        candidate_id = "nested-market-source-race"
        payload = candidate_payload(candidate_id, score=0.40)
        payload["strategy"] = {"market_type": "prediction"}
        self.store.save_candidate_lifecycle(candidate_id, "IDEA", payload, timestamp=T0)
        self.store.save_candidate_lifecycle(candidate_id, "FROZEN", payload, timestamp=T0)
        ranker = CandidateCanaryRanker(self.store, clock=lambda: T0)
        initial = ranker.evaluate_and_select(T0)
        self.assertEqual(initial["selection_status"], "CURRENT")
        baseline_ranking = dict(
            self.store.connection.execute(
                "SELECT * FROM canary_rankings WHERE candidate_id=?",
                (candidate_id,),
            ).fetchone()
        )
        baseline_selection = dict(
            self.store.connection.execute(
                "SELECT * FROM canary_selection WHERE singleton=1"
            ).fetchone()
        )
        entered = threading.Event()
        release = threading.Event()
        result_holder = {}
        original_rank_evidence = ranker._rank_evidence

        def blocked_rank_evidence(payload, frozen_hash, quality=None):
            entered.set()
            if not release.wait(timeout=5):
                raise AssertionError("ranking barrier was not released")
            return original_rank_evidence(payload, frozen_hash, quality=quality)

        ranker._rank_evidence = blocked_rank_evidence

        def run_ranker():
            try:
                ranker.evaluate_and_select(T0)
            except BaseException as exc:
                result_holder["error"] = exc

        ranking_thread = threading.Thread(target=run_ranker)
        ranking_thread.start()
        try:
            self.assertTrue(entered.wait(timeout=5))
            CandidateLifecycleManager(self.store).record_evidence(
                candidate_id,
                {"strategy": {"market_type": "crypto_spot"}},
                expected_stage=CandidateStage.FROZEN,
                reason="nested market source changed during ranking",
            )
        finally:
            release.set()
            ranking_thread.join(timeout=5)

        self.assertFalse(ranking_thread.is_alive())
        self.assertEqual(
            getattr(result_holder.get("error"), "error_code", None),
            "LIFECYCLE_SNAPSHOT_CHANGED",
        )
        self.assertEqual(
            dict(
                self.store.connection.execute(
                    "SELECT * FROM canary_rankings WHERE candidate_id=?",
                    (candidate_id,),
                ).fetchone()
            ),
            baseline_ranking,
        )
        self.assertEqual(
            dict(
                self.store.connection.execute(
                    "SELECT * FROM canary_selection WHERE singleton=1"
                ).fetchone()
            ),
            baseline_selection,
        )
        status = self.service.status()
        self.assertEqual(status["selection_status"], "STALE")
        self.assertFalse(status["selection_valid"])
        self.assertIsNone(status["selected_candidate"])

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
        self.assertEqual(stale["selection_invalidation_reason"], "LIFECYCLE_RANKING_EVIDENCE_UPDATED")
        self.assertIsNone(stale["selected_candidate"])
        self.assertIsNone(stale["winner_id"])
        authoritative = self.service.authoritative_status()
        self.assertEqual(authoritative["selection_status"], "STALE")
        self.assertFalse(authoritative["selection_valid"])
        self.assertEqual(
            authoritative["selection_invalidation_reason"],
            "RANKING_EVIDENCE_CHANGED",
        )
        self.assertIsNone(authoritative["selected_candidate"])
        self.assertIsNone(authoritative["winner_id"])
        last_selected = authoritative["last_selected_candidate"]
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
        self.assertEqual(stale["selection_invalidation_reason"], "LIFECYCLE_RANKING_EVIDENCE_UPDATED")
        self.assertIsNone(stale["selected_candidate"])
        self.assertIsNone(stale["winner_id"])
        authoritative = self.service.authoritative_status()
        self.assertEqual(authoritative["selection_status"], "STALE")
        self.assertFalse(authoritative["selection_valid"])
        self.assertEqual(
            authoritative["selection_invalidation_reason"],
            "RANKING_EVIDENCE_CHANGED",
        )
        self.assertIsNone(authoritative["selected_candidate"])
        self.assertIsNone(authoritative["winner_id"])
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


    def test_worker_recovers_after_one_exception_and_persists_error_metadata(self):
        worker = AutonomousCanaryWorker(self.store, clock=lambda: T0)
        ranking_without_winner = {
            "selected_candidate": None,
            "rankings": [],
            "eligible_count": 0,
        }
        with patch.object(
            CandidateCanaryRanker,
            "evaluate_and_select",
            side_effect=[RuntimeError("injected ranking failure"), ranking_without_winner],
        ) as evaluate:
            failed = worker.tick(now=T0)
            report = self.service.status_report()
            recovered = worker.tick(now=T0)

        self.assertEqual(failed["status"], "ERROR")
        self.assertEqual(failed["decision"], "AUTONOMOUS_WORKER_EXCEPTION")
        self.assertEqual(report["worker"]["worker_status"], "DEGRADED")
        self.assertEqual(
            report["worker"]["last_error_code"],
            "AUTONOMOUS_WORKER_EXCEPTION",
        )
        self.assertEqual(report["worker"]["consecutive_failures"], 1)
        self.assertIsNotNone(report["worker"]["next_retry_at"])
        self.assertEqual(recovered["status"], "DISABLED")
        self.assertEqual(evaluate.call_count, 2)

    def test_node_restarts_dead_autonomous_thread_with_durable_restart_state(self):
        node = ResearchNode(
            NodeConfig(":memory:", auto_canary_interval_seconds=1.0, crypto_enabled=False),
            provider=object(),
            store=self.store,
            clock=lambda: T0,
            sleep=lambda _: None,
        )

        class DeadThread:
            def is_alive(self):
                return False

        node._auto_canary_thread = DeadThread()
        replacement = Mock()
        with patch("axiom.node.threading.Thread", return_value=replacement) as factory:
            with patch.object(node.stop_event, "wait", return_value=False):
                self.assertTrue(node._supervise_autonomous_thread())

        factory.assert_called_once_with(
            target=node._auto_canary_worker_loop,
            name=f"{node.config.worker_name}-autonomous-canary",
            daemon=True,
        )
        replacement.start.assert_called_once_with()
        state = next(
            row
            for row in self.store.list_worker_states(limit=64)
            if row["worker_name"] == "autonomous-canary"
        )
        self.assertEqual(state["status"], "degraded")
        self.assertEqual(state["payload"]["decision"], "AUTONOMOUS_THREAD_RESTARTING")
        self.assertEqual(state["payload"]["blocker"], "AUTONOMOUS_THREAD_EXITED")

    def test_unknown_signal_is_terminal_for_worker_without_external_retry(self):
        self.seed_candidate("winner", score=0.4)
        self.service.enable_autonomous_micro_live()
        worker = AutonomousCanaryWorker(self.store, clock=lambda: T0, venue_factory=TestVenue)
        with patch.object(
            CanaryService,
            "evaluate_signal",
            autospec=True,
            return_value=self._signal_evaluation(
                "winner",
                "COLLECTOR_CANDIDATE_HEALTH_BLOCKED",
                signal={"status": "UNKNOWN", "signal_id": "unknown-signal"},
            ),
        ), patch.object(CanaryService, "submit_signal") as submit:
            result = worker.tick(now=T0)
            self.assertEqual(result["blocker"], "UNKNOWN_NO_RETRY")
            self.assertEqual(worker.tick(now=T0)["blocker"], "UNKNOWN_NO_RETRY")
            submit.assert_not_called()
        worker_state = self.service.status_report()["worker"]
        self.assertEqual(worker_state["next_decision"], "WAIT_FOR_FRESH_ACTIONABLE_SIGNAL")
        self.assertEqual(worker_state["blocker"], "UNKNOWN_NO_RETRY")
        self.assertEqual(worker_state["last_signal_id"], "unknown-signal")

    _SCAN_FIELDS = (
        "candidates_ranked",
        "candidates_signal_checked",
        "candidates_no_signal",
        "actionable_candidates_found",
        "selected_actionable_candidate",
        "selected_actionable_rank",
        "selected_actionable_score",
        "signal_scan_cursor",
        "signal_scan_ranking_run_id",
        "next_signal_scan_start_rank",
        "next_signal_scan_end_rank",
    )

    def _enable_worker(self):
        self.service.enable_autonomous_micro_live()

    @staticmethod
    def _ready_signal(candidate_id: str, *, feasible: bool = True) -> dict[str, object]:
        return {
            "status": "READY",
            "signal_id": f"signal-{candidate_id}",
            "candidate_id": candidate_id,
            "execution_feasible": feasible,
            "liquidity_feasible": feasible,
            "slippage_feasible": feasible,
        }
    _SIGNAL_REASON_CODES = (
        "READY_SIGNAL",
        "NO_STRATEGY_SIGNAL",
        "NO_FORWARD_SNAPSHOT",
        "STALE_FORWARD_EVIDENCE",
        "MARKET_CLOSED",
        "MARKET_FILTER_MISMATCH",
        "CANDIDATE_FORWARD_MARKET_UNRESOLVED",
        "COLLECTOR_CANDIDATE_HEALTH_BLOCKED",
    )

    @classmethod
    def _signal_evaluation(
        cls,
        candidate_id: str,
        reason_code: str,
        *,
        signal: Mapping[str, object] | None = None,
        market_id: str | None = None,
        required_health: Mapping[str, object] | None = None,
        evidence: Mapping[str, object] | None = None,
    ) -> dict[str, object]:
        return {
            "candidate_id": candidate_id,
            "evaluated_at": T0.isoformat(),
            "reason_code": reason_code,
            "market_id": market_id,
            "signal": signal,
            "required_health": dict(required_health or {}),
            "evidence": dict(evidence or {}),
        }

    @staticmethod
    def _normalized_reason_counts(value):
        if isinstance(value, str):
            value = json.loads(value or "{}")
        if not isinstance(value, Mapping):
            return {}
        # Production persists the additive reason taxonomy. Legacy tests
        # intentionally assert only the stable compatibility projection.
        return {
            reason: int(value.get(reason, 0) or 0)
            for reason in AutonomousWorkflowTests._SIGNAL_REASON_CODES
        }

    def _assert_reason_counts(
        self,
        result: Mapping[str, object],
        expected: Mapping[str, int],
        *,
        checked: int | None = None,
    ) -> None:
        expected_map = {
            reason: int(expected.get(reason, 0))
            for reason in self._SIGNAL_REASON_CODES
        }
        result_map = self._normalized_reason_counts(
            result["signal_scan_reason_counts_json"]
        )
        self.assertEqual(result_map, expected_map)
        if checked is not None:
            self.assertEqual(sum(result_map.values()), checked)
            self.assertLessEqual(sum(result_map.values()), checked)
        state = self.store.connection.execute(
            "SELECT signal_scan_reason_counts_json "
            "FROM canary_autonomous_state WHERE singleton=1"
        ).fetchone()
        self.assertIsNotNone(state)
        assert state is not None
        self.assertEqual(
            self._normalized_reason_counts(state["signal_scan_reason_counts_json"]),
            expected_map,
        )
        report = self.service.status_report()
        self.assertEqual(
            self._normalized_reason_counts(
                report["worker"]["signal_scan_reason_counts_json"]
            ),
            expected_map,
        )
        self.assertEqual(
            self._normalized_reason_counts(
                report["autonomous"]["signal_scan_reason_counts_json"]
            ),
            expected_map,
        )
        status = self.service.status()
        self.assertEqual(
            self._normalized_reason_counts(
                status["autonomous"]["signal_scan_reason_counts_json"]
            ),
            expected_map,
        )


    def _assert_scan_metrics(self, result, expected):
        state = self.store.connection.execute(
            "SELECT * FROM canary_autonomous_state WHERE singleton=1"
        ).fetchone()
        self.assertIsNotNone(state)
        worker = self.service.status_report()["worker"]
        for field in self._SCAN_FIELDS:
            self.assertIn(field, result)
            self.assertIn(field, state.keys())
            self.assertIn(field, worker)
            self.assertEqual(result[field], expected[field], field)
            self.assertEqual(state[field], expected[field], field)
            self.assertEqual(worker[field], expected[field], field)
    def _durable_ranking_patch(self, candidate_ids, run_ids):
        """Install a deterministic ranking feed while exercising durable scan state."""
        real_ranker = CandidateCanaryRanker(self.store, clock=lambda: T0)
        real_ranker.evaluate_and_select(T0)
        original_evaluate = CandidateCanaryRanker.evaluate_and_select
        runs = iter(run_ids)

        def evaluate(*args, **kwargs):
            try:
                ranking_run_id = next(runs)
            except StopIteration:
                ranking_run_id = run_ids[-1]
            existing = {
                row["candidate_id"]
                for row in self.store.connection.execute(
                    "SELECT candidate_id FROM canary_rankings"
                ).fetchall()
            }
            if any(candidate_id not in existing for candidate_id in candidate_ids):
                original_evaluate(real_ranker, T0)
            valid_rows = self.store.connection.execute(
                "SELECT r.* FROM canary_rankings r "
                "JOIN candidate_lifecycle l ON l.candidate_id=r.candidate_id "
                "WHERE l.stage <> 'REJECTED' "
                "ORDER BY r.candidate_id"
            ).fetchall()
            with self.store.connection:
                for rank, row in enumerate(valid_rows, start=1):
                    evidence_row = self.store.connection.execute(
                        "SELECT evidence_json FROM canary_eligibility WHERE candidate_id=?",
                        (row["candidate_id"],),
                    ).fetchone()
                    qualification_hash = row["qualification_hash"]
                    if evidence_row is not None:
                        qualification_hash = json.loads(
                            evidence_row["evidence_json"]
                        ).get("qualification_hash", qualification_hash)
                    self.store.connection.execute(
                        "UPDATE canary_rankings SET ranking_run_id=?,rank=?,"
                        "qualification_hash=? WHERE candidate_id=?",
                        (
                            ranking_run_id,
                            rank,
                            qualification_hash,
                            row["candidate_id"],
                        ),
                    )
            rows = [
                dict(row)
                for row in self.store.connection.execute(
                    "SELECT * FROM canary_rankings WHERE ranking_run_id=? "
                    "ORDER BY rank,candidate_id",
                    (ranking_run_id,),
                ).fetchall()
            ]
            return {
                "ranking_run_id": ranking_run_id,
                "eligible_count": len(rows),
                "rankable_count": len(rows),
                "rankings": rows,
            }

        return patch.object(
            CandidateCanaryRanker,
            "evaluate_and_select",
            side_effect=evaluate,
        )

    def _seed_durable_candidates(self, count, *, prefix="cycle"):
        candidate_ids = [f"{prefix}-{index:02d}" for index in range(count)]
        for index, candidate_id in enumerate(candidate_ids):
            self.seed_candidate(
                candidate_id,
                cluster=f"{prefix}-cluster-{index}",
                score=0.90 - index / 1000,
            )
        return candidate_ids

    def _durable_scan_patches(self, candidate_ids, run_ids, checked):
        def evaluate(service, candidate_id, **kwargs):
            checked.append(candidate_id)
            return self._signal_evaluation(candidate_id, "NO_STRATEGY_SIGNAL")

        return (
            self._durable_ranking_patch(candidate_ids, run_ids),
            patch.object(CandidateCanaryRanker, "validate_persisted_ranking", return_value=True),
            patch.object(CanaryService, "evaluate_signal", autospec=True, side_effect=evaluate),
        )

    def test_signal_reason_counts_are_exact_bounded_and_one_per_checked_candidate(self):
        candidate_ids = self._seed_durable_candidates(8, prefix="reasons")
        self._enable_worker()
        reasons = {
            candidate_ids[0]: "READY_SIGNAL",
            candidate_ids[1]: "NO_STRATEGY_SIGNAL",
            candidate_ids[2]: "NO_FORWARD_SNAPSHOT",
            candidate_ids[3]: "STALE_FORWARD_EVIDENCE",
            candidate_ids[4]: "MARKET_CLOSED",
            candidate_ids[5]: "MARKET_FILTER_MISMATCH",
            candidate_ids[6]: "CANDIDATE_FORWARD_MARKET_UNRESOLVED",
            candidate_ids[7]: "COLLECTOR_CANDIDATE_HEALTH_BLOCKED",
        }
        checked: list[str] = []
        venue_calls: list[bool] = []

        def evaluate(service, candidate_id, **kwargs):
            checked.append(candidate_id)
            reason_code = reasons[candidate_id]
            signal = (
                self._ready_signal(candidate_id)
                if reason_code == "READY_SIGNAL"
                else None
            )
            return self._signal_evaluation(
                candidate_id,
                reason_code,
                signal=signal,
                market_id="market-1" if signal else None,
            )

        worker = AutonomousCanaryWorker(
            self.store,
            clock=lambda: T0,
            venue_factory=lambda: venue_calls.append(True),
        )
        with patch.object(
            CanaryService,
            "evaluate_signal",
            autospec=True,
            side_effect=evaluate,
        ), patch.object(CredentialStore, "configured", return_value=False):
            result = worker.tick(now=T0)

        expected = {reason: 1 for reason in self._SIGNAL_REASON_CODES}
        self.assertEqual(checked, candidate_ids)
        self.assertEqual(result["candidates_signal_checked"], len(candidate_ids))
        self.assertEqual(result["actionable_candidates_found"], 1)
        self.assertEqual(result["selected_actionable_candidate"], candidate_ids[0])
        self.assertEqual(result["selected_actionable_rank"], 1)
        self.assertEqual(result["status"], "BLOCKED")
        self.assertEqual(result["blocker"], "CREDENTIALS_NOT_CONFIGURED")
        self.assertEqual(venue_calls, [])
        self._assert_reason_counts(result, expected, checked=len(candidate_ids))
        self.assertEqual(
            self.store.connection.execute(
                "SELECT COUNT(*) FROM canary_ledger"
            ).fetchone()[0],
            0,
        )

    def test_reason_counts_persist_across_restart_ranking_and_c_d_telemetry(self):
        candidate_ids = self._seed_durable_candidates(12, prefix="reason-stable")
        self._enable_worker()
        checked: list[str] = []

        def evaluate(service, candidate_id, **kwargs):
            checked.append(candidate_id)
            return self._signal_evaluation(candidate_id, "NO_STRATEGY_SIGNAL")

        ranking_patch = self._durable_ranking_patch(
            candidate_ids,
            ["reason-run-a", "reason-run-b"],
        )
        worker = AutonomousCanaryWorker(self.store, clock=lambda: T0)
        with ranking_patch, patch.object(
            CandidateCanaryRanker,
            "validate_persisted_ranking",
            return_value=True,
        ), patch.object(
            CanaryService,
            "evaluate_signal",
            autospec=True,
            side_effect=evaluate,
        ):
            first = worker.tick(now=T0)
            self.assertEqual(first["signal_scan_checked_this_cycle"], 10)
            self._assert_reason_counts(
                first,
                {"NO_STRATEGY_SIGNAL": 10},
                checked=10,
            )

            before = self.service.status()
            CandidateLifecycleManager(self.store).record_evidence(
                candidate_ids[0],
                {
                    "forward_evidence": {
                        "forward_liquidity": 0.42,
                        "forward_max_drawdown": 0.05,
                        "forward_fills": 17,
                        "forward_observations": 99,
                    }
                },
                expected_stage=CandidateStage.FROZEN,
                reason="reason-count telemetry update",
            )
            after = self.service.status()
            for key in (
                "readiness_snapshot_status",
                "readiness_snapshot_stale",
                "readiness_snapshot_reason",
            ):
                self.assertEqual(after[key], before[key])

            restarted = AutonomousCanaryWorker(self.store, clock=lambda: T0)
            second = restarted.tick(now=T0)

        self.assertEqual(second["signal_scan_cycle_id"], first["signal_scan_cycle_id"])
        self.assertEqual(second["signal_scan_checked_this_cycle"], 12)
        self.assertEqual(checked[:10], candidate_ids[:10])
        self.assertEqual(set(checked[10:]), set(candidate_ids[10:]))
        self.assertEqual(second["signal_scan_ranking_run_id"], "reason-run-b")
        self._assert_reason_counts(
            second,
            {"NO_STRATEGY_SIGNAL": 12},
            checked=12,
        )

    def test_reason_counts_reset_only_when_durable_cycle_is_new(self):
        candidate_ids = self._seed_durable_candidates(2, prefix="reason-reset")
        self._enable_worker()
        checked: list[str] = []

        def evaluate(service, candidate_id, **kwargs):
            checked.append(candidate_id)
            return self._signal_evaluation(candidate_id, "NO_STRATEGY_SIGNAL")

        ranking_patch = self._durable_ranking_patch(
            candidate_ids,
            ["reason-reset-run"] * 2,
        )
        worker = AutonomousCanaryWorker(self.store, clock=lambda: T0)
        with ranking_patch, patch.object(
            CandidateCanaryRanker,
            "validate_persisted_ranking",
            return_value=True,
        ), patch.object(
            CanaryService,
            "evaluate_signal",
            autospec=True,
            side_effect=evaluate,
        ):
            first = worker.tick(now=T0)
            self._assert_reason_counts(
                first,
                {"NO_STRATEGY_SIGNAL": 2},
                checked=2,
            )
            second = worker.tick(now=T0)

        self.assertEqual(first["signal_scan_status"], "COMPLETE_NO_SIGNAL")
        self.assertEqual(second["signal_scan_status"], "COMPLETE_NO_SIGNAL")
        self.assertNotEqual(
            first["signal_scan_cycle_id"],
            second["signal_scan_cycle_id"],
        )
        self.assertEqual(checked, candidate_ids + candidate_ids)
        self._assert_reason_counts(
            second,
            {"NO_STRATEGY_SIGNAL": 2},
            checked=2,
        )
        cycle_counts = self.store.connection.execute(
            "SELECT cycle_id,COUNT(*) AS n "
            "FROM canary_signal_scan_checked GROUP BY cycle_id "
            "ORDER BY cycle_id"
        ).fetchall()
        self.assertEqual(sorted(row["n"] for row in cycle_counts), [2, 2])

    def test_scan_history_retains_latest_eight_cycles_and_active_restart_state(self):
        completed_cycles = [f"completed-{index:02d}" for index in range(10)]
        for index, cycle_id in enumerate(completed_cycles):
            cycle_time = T0 + timedelta(minutes=index)
            self.service.record_autonomous_decision(
                next_decision="WAIT_FOR_NEXT_DECISION",
                worker_status="IDLE",
                timestamp=cycle_time,
                signal_scan_cycle_id=cycle_id,
                signal_scan_cycle_started_at=cycle_time.isoformat(),
                signal_scan_cycle_completed_at=cycle_time.isoformat(),
                signal_scan_cycle_complete=True,
                signal_scan_checked_this_cycle=2,
                signal_scan_remaining_this_cycle=0,
                signal_scan_coverage_percentage=100.0,
                signal_scan_reason_counts_json='{"NO_STRATEGY_SIGNAL":2}',
                signal_scan_status="COMPLETE_NO_SIGNAL",
                signal_scan_checked_keys=[
                    {
                        "cycle_id": cycle_id,
                        "candidate_id": f"{cycle_id}-candidate-0",
                        "qualification_hash": f"{cycle_id}-qualification-0",
                        "checked_at": cycle_time.isoformat(),
                    },
                    {
                        "cycle_id": cycle_id,
                        "candidate_id": f"{cycle_id}-candidate-1",
                        "qualification_hash": f"{cycle_id}-qualification-1",
                        "checked_at": cycle_time.isoformat(),
                    },
                ],
            )

        active_cycle = "active-current"
        active_time = T0 + timedelta(minutes=10)
        active_reason_counts = {
            reason: (1 if reason == "NO_STRATEGY_SIGNAL" else 0)
            for reason in self._SIGNAL_REASON_CODES
        }
        self.service.record_autonomous_decision(
            next_decision="EVALUATING_CANDIDATES",
            worker_status="RUNNING",
            timestamp=active_time,
            signal_scan_cycle_id=active_cycle,
            signal_scan_cycle_started_at=active_time.isoformat(),
            signal_scan_cycle_completed_at=None,
            signal_scan_cycle_complete=False,
            signal_scan_checked_this_cycle=1,
            signal_scan_remaining_this_cycle=1,
            signal_scan_coverage_percentage=50.0,
            signal_scan_reason_counts_json=json.dumps(active_reason_counts),
            signal_scan_status="IN_PROGRESS",
            signal_scan_checked_keys=[
                {
                    "cycle_id": active_cycle,
                    "candidate_id": "active-candidate",
                    "qualification_hash": "active-qualification",
                    "checked_at": active_time.isoformat(),
                }
            ],
        )

        rows = self.store.connection.execute(
            "SELECT cycle_id,candidate_id,qualification_hash "
            "FROM canary_signal_scan_checked "
            "ORDER BY cycle_id,candidate_id"
        ).fetchall()
        self.assertEqual(
            {str(row["cycle_id"]) for row in rows},
            set(completed_cycles[3:]) | {active_cycle},
        )
        self.assertEqual(len(rows), 15)
        self.assertEqual(
            [(row["cycle_id"], row["candidate_id"]) for row in rows],
            sorted(
                [
                    (cycle_id, f"{cycle_id}-candidate-{candidate_index}")
                    for cycle_id in completed_cycles[3:]
                    for candidate_index in range(2)
                ]
                + [(active_cycle, "active-candidate")]
            ),
        )

        restarted = CanaryService(
            self.store,
            credentials=TestCredentials(True),
            clock=lambda: active_time,
        )
        report = restarted.status_report()
        for projection in (report["worker"], report["autonomous"]):
            self.assertEqual(projection["signal_scan_cycle_id"], active_cycle)
            self.assertEqual(projection["signal_scan_checked_this_cycle"], 1)
            self.assertEqual(projection["signal_scan_remaining_this_cycle"], 1)
            self.assertEqual(projection["signal_scan_coverage_percentage"], 50.0)
            self.assertEqual(
                self._normalized_reason_counts(
                    projection["signal_scan_reason_counts_json"]
                ),
                active_reason_counts,
            )
        self.assertEqual(
            report["worker"]["signal_scan_checked_keys"],
            [
                {
                    "cycle_id": active_cycle,
                    "candidate_id": "active-candidate",
                    "qualification_hash": "active-qualification",
                    "checked_at": active_time.isoformat(),
                    "rank_at_check": None,
                    "ranking_run_id": None,
                }
            ],
        )

    def test_ready_reason_count_and_selection_are_rank_ordered_without_order(self):
        candidate_ids = self._seed_durable_candidates(3, prefix="reason-ready")
        self._enable_worker()
        checked: list[str] = []
        venue_calls: list[bool] = []

        def evaluate(service, candidate_id, **kwargs):
            checked.append(candidate_id)
            if candidate_id in candidate_ids[:2]:
                signal = self._ready_signal(candidate_id)
                return self._signal_evaluation(
                    candidate_id,
                    "READY_SIGNAL",
                    signal=signal,
                    market_id="market-1",
                )
            return self._signal_evaluation(candidate_id, "NO_STRATEGY_SIGNAL")

        worker = AutonomousCanaryWorker(
            self.store,
            clock=lambda: T0,
            venue_factory=lambda: venue_calls.append(True),
        )
        with patch.object(
            CanaryService,
            "evaluate_signal",
            autospec=True,
            side_effect=evaluate,
        ), patch.object(CredentialStore, "configured", return_value=False):
            result = worker.tick(now=T0)

        self.assertEqual(checked, candidate_ids)
        self.assertEqual(result["actionable_candidates_found"], 2)
        self.assertEqual(result["selected_actionable_candidate"], candidate_ids[0])
        self.assertEqual(result["selected_actionable_rank"], 1)
        self.assertEqual(result["blocker"], "CREDENTIALS_NOT_CONFIGURED")
        self.assertEqual(venue_calls, [])
        self._assert_reason_counts(
            result,
            {"READY_SIGNAL": 2, "NO_STRATEGY_SIGNAL": 1},
            checked=3,
        )
        self.assertEqual(
            self.store.connection.execute(
                "SELECT COUNT(*) FROM canary_ledger"
            ).fetchone()[0],
            0,
        )

    def test_ready_candidates_produce_at_most_one_order_decision(self):
        candidate_ids = self._seed_durable_candidates(3, prefix="reason-order")
        self._enable_worker()
        checked: list[str] = []
        venue = TestVenue()

        def evaluate(service, candidate_id, **kwargs):
            checked.append(candidate_id)
            signal = self._ready_signal(candidate_id)
            return self._signal_evaluation(
                candidate_id,
                "READY_SIGNAL",
                signal=signal,
                market_id="market-1",
            )

        worker = AutonomousCanaryWorker(
            self.store,
            clock=lambda: T0,
            venue_factory=lambda: venue,
            allow_test_venue=True,
        )
        with patch.object(
            CanaryService,
            "evaluate_signal",
            autospec=True,
            side_effect=evaluate,
        ), patch.object(
            CanaryService,
            "bind_autonomous_actionable_candidate",
            return_value={"bound": True},
        ) as bind, patch.object(
            CanaryService,
            "submit_signal",
            return_value={"ok": True, "order_id": "reason-order"},
        ) as submit, patch.object(
            CredentialStore,
            "configured",
            return_value=True,
        ):
            result = worker.tick(now=T0)

        self.assertEqual(result["status"], "SUBMITTED")
        self.assertEqual(checked, candidate_ids)
        self.assertEqual(result["actionable_candidates_found"], 3)
        self.assertEqual(result["selected_actionable_candidate"], candidate_ids[0])
        submit.assert_called_once_with(
            "signal-" + candidate_ids[0],
            venue=venue,
            allow_test_venue=True,
        )
        bind.assert_called_once_with(
            candidate_ids[0],
            ranking_run_id=result["ranking"]["ranking_run_id"],
            signal_id="signal-" + candidate_ids[0],
        )
        self.assertEqual(len(venue.submissions), 0)
        self.assertEqual(
            self.store.connection.execute(
                "SELECT orders_attempted FROM canary_autonomous_state "
                "WHERE singleton=1"
            ).fetchone()[0],
            1,
        )
        self._assert_reason_counts(
            result,
            {"READY_SIGNAL": 3},
            checked=3,
        )

    def test_lifecycle_snapshot_changed_retries_at_most_three_times_without_reset(self):
        candidate_ids = self._seed_durable_candidates(12, prefix="reason-race")
        self._enable_worker()
        checked: list[str] = []

        def evaluate(service, candidate_id, **kwargs):
            checked.append(candidate_id)
            return self._signal_evaluation(candidate_id, "NO_STRATEGY_SIGNAL")

        first_patches = self._durable_scan_patches(
            candidate_ids,
            ["reason-race-run"],
            checked,
        )
        worker = AutonomousCanaryWorker(self.store, clock=lambda: T0)
        with first_patches[0], first_patches[1], patch.object(
            CanaryService,
            "evaluate_signal",
            autospec=True,
            side_effect=evaluate,
        ):
            first = worker.tick(now=T0)

        self.assertEqual(first["signal_scan_checked_this_cycle"], 10)
        cycle_id = first["signal_scan_cycle_id"]
        self._assert_reason_counts(
            first,
            {"NO_STRATEGY_SIGNAL": 10},
            checked=10,
        )

        class LifecycleSnapshotChanged(RuntimeError):
            error_code = "LIFECYCLE_SNAPSHOT_CHANGED"

        restarted = AutonomousCanaryWorker(self.store, clock=lambda: T0)
        with patch.object(
            CandidateCanaryRanker,
            "evaluate_and_select",
            autospec=True,
            side_effect=LifecycleSnapshotChanged("ranker race"),
        ) as evaluate_and_select, patch.object(
            CanaryService,
            "evaluate_signal",
            autospec=True,
            side_effect=AssertionError("transient rank race must not scan candidates"),
        ):
            raced = restarted.tick(now=T0)

        self.assertEqual(evaluate_and_select.call_count, 3)
        self.assertEqual(raced["status"], "ERROR")
        self.assertEqual(checked, candidate_ids[:10])
        state = self.store.connection.execute(
            "SELECT signal_scan_cycle_id,signal_scan_checked_this_cycle,"
            "signal_scan_remaining_this_cycle,signal_scan_reason_counts_json "
            "FROM canary_autonomous_state WHERE singleton=1"
        ).fetchone()
        self.assertEqual(state["signal_scan_cycle_id"], cycle_id)
        self.assertEqual(state["signal_scan_checked_this_cycle"], 10)
        self.assertEqual(state["signal_scan_remaining_this_cycle"], 2)
        self.assertEqual(
            self._normalized_reason_counts(
                state["signal_scan_reason_counts_json"]
            ),
            {
                reason: 10 if reason == "NO_STRATEGY_SIGNAL" else 0
                for reason in self._SIGNAL_REASON_CODES
            },
        )
        report = self.service.status_report()
        self.assertEqual(
            self._normalized_reason_counts(
                report["worker"]["signal_scan_reason_counts_json"]
            ),
            {
                reason: 10 if reason == "NO_STRATEGY_SIGNAL" else 0
                for reason in self._SIGNAL_REASON_CODES
            },
        )
        self.assertEqual(
            self._normalized_reason_counts(
                report["autonomous"]["signal_scan_reason_counts_json"]
            ),
            {
                reason: 10 if reason == "NO_STRATEGY_SIGNAL" else 0
                for reason in self._SIGNAL_REASON_CODES
            },
        )
        self.assertEqual(
            self.store.connection.execute(
                "SELECT COUNT(*) FROM canary_signal_scan_checked "
                "WHERE cycle_id=?",
                (cycle_id,),
            ).fetchone()[0],
            10,
        )

    def test_durable_no_signal_cycle_is_exactly_10_10_8_then_next_tick_starts_cycle(self):
        candidate_ids = self._seed_durable_candidates(28, prefix="exact")
        checked = []
        self._enable_worker()
        patches = self._durable_scan_patches(
            candidate_ids,
            ["run-exact"] * 4,
            checked,
        )
        worker = AutonomousCanaryWorker(self.store, clock=lambda: T0)
        with patches[0], patches[1], patches[2]:
            first = worker.tick(now=T0)
            self._assert_reason_counts(
                first,
                {"NO_STRATEGY_SIGNAL": 10},
                checked=10,
            )
            second = worker.tick(now=T0)
            self._assert_reason_counts(
                second,
                {"NO_STRATEGY_SIGNAL": 20},
                checked=20,
            )
            restarted = AutonomousCanaryWorker(self.store, clock=lambda: T0)
            third = restarted.tick(now=T0)
            self._assert_reason_counts(
                third,
                {"NO_STRATEGY_SIGNAL": 28},
                checked=28,
            )
            complete = restarted.tick(now=T0)
            self._assert_reason_counts(
                complete,
                {"NO_STRATEGY_SIGNAL": 10},
                checked=10,
            )

        self.assertEqual(
            [first["candidates_signal_checked"],
             second["candidates_signal_checked"],
             third["candidates_signal_checked"]],
            [10, 10, 8],
        )
        self.assertEqual(
            [first["signal_scan_checked_this_cycle"],
             second["signal_scan_checked_this_cycle"],
             third["signal_scan_checked_this_cycle"]],
            [10, 20, 28],
        )
        self.assertEqual(first["signal_scan_status"], "IN_PROGRESS")
        self.assertEqual(second["signal_scan_status"], "IN_PROGRESS")
        self.assertEqual(third["signal_scan_status"], "COMPLETE_NO_SIGNAL")
        self.assertTrue(third["signal_scan_cycle_complete"])
        self.assertEqual(third["signal_scan_remaining_this_cycle"], 0)
        self.assertEqual(third["signal_scan_coverage_percentage"], 100.0)
        skip_reasons = third["signal_scan_skip_reasons_json"]
        self.assertEqual(
            set(skip_reasons),
            {
                "INVALID_RANKING_BINDING",
                "QUALIFICATION_INVALID",
                "DUPLICATE_CLUSTER_DEFERRED",
                "CYCLE_REMAINDER",
                "LEGACY_SCOPE_SUCCESSOR_REQUIRED",
            },
        )
        self.assertTrue(
            all(
                isinstance(count, int) and 0 <= count <= 28
                for count in skip_reasons.values()
            )
        )
        self.assertEqual(complete["signal_scan_checked_this_cycle"], 10)
        self.assertEqual(complete["signal_scan_status"], "IN_PROGRESS")
        self.assertNotEqual(
            third["signal_scan_cycle_id"],
            complete["signal_scan_cycle_id"],
        )
        self.assertEqual(len(checked), 38)
        self.assertEqual(len(set(checked[:28])), 28)
        persisted = self.store.connection.execute(
            "SELECT COUNT(*) FROM canary_signal_scan_checked WHERE cycle_id=?",
            (third["signal_scan_cycle_id"],),
        ).fetchone()[0]
        self.assertEqual(persisted, 28)
        self.assertEqual(
            self.store.connection.execute(
                "SELECT COUNT(*) FROM canary_ledger"
            ).fetchone()[0],
            0,
        )
    def test_ranker_failure_after_first_scan_window_preserves_cycle_and_checked_rows(self):
        candidate_ids = self._seed_durable_candidates(20, prefix="failure")
        checked: list[str] = []
        ranking = CandidateCanaryRanker(self.store, clock=lambda: T0).evaluate_and_select(T0)
        self._enable_worker()


        worker = AutonomousCanaryWorker(self.store, clock=lambda: T0)
        with patch.object(
            CandidateCanaryRanker,
            "evaluate_and_select",
            autospec=True,
            side_effect=[ranking, RuntimeError("injected ranking failure"), ranking],
        ), patch.object(
            CandidateCanaryRanker,
            "validate_persisted_ranking",
            return_value=True,
        ), patch.object(
            CanaryService,
            "evaluate_signal",
            autospec=True,
            side_effect=lambda service, candidate_id, **kwargs: (
                checked.append(candidate_id)
                or self._signal_evaluation(candidate_id, "NO_STRATEGY_SIGNAL")
            ),
        ):
            first = worker.tick(now=T0)
            failed = worker.tick(now=T0)

            self.assertEqual(first["status"], "NO_SIGNAL")
            self.assertEqual(first["candidates_signal_checked"], 10)
            self.assertEqual(first["signal_scan_checked_this_cycle"], 10)
            self.assertEqual(first["signal_scan_remaining_this_cycle"], 10)
            self.assertEqual(first["signal_scan_status"], "IN_PROGRESS")
            cycle_id = first["signal_scan_cycle_id"]
            self.assertTrue(cycle_id)
            self.assertEqual(failed["status"], "ERROR")
            self.assertEqual(failed["blocker"], "AUTONOMOUS_WORKER_EXCEPTION")

            state = self.store.connection.execute(
                "SELECT signal_scan_cycle_id,signal_scan_cycle_complete,"
                "signal_scan_checked_this_cycle,signal_scan_remaining_this_cycle,"
                "signal_scan_status,worker_status,last_error_code "
                "FROM canary_autonomous_state WHERE singleton=1"
            ).fetchone()
            self.assertEqual(state["signal_scan_cycle_id"], cycle_id)
            self.assertEqual(state["signal_scan_cycle_complete"], 0)
            self.assertEqual(state["signal_scan_checked_this_cycle"], 10)
            self.assertEqual(state["signal_scan_remaining_this_cycle"], 10)
            self.assertEqual(state["worker_status"], "DEGRADED")
            self.assertEqual(state["last_error_code"], "AUTONOMOUS_WORKER_EXCEPTION")
            self.assertEqual(state["signal_scan_status"], "IN_PROGRESS")
            self.assertEqual(
                self.store.connection.execute(
                    "SELECT COUNT(*) FROM canary_signal_scan_checked "
                    "WHERE cycle_id=?",
                    (cycle_id,),
                ).fetchone()[0],
                10,
            )
            worker_state = self.service.status_report()["worker"]
            self.assertEqual(worker_state["worker_status"], "DEGRADED")
            self.assertEqual(
                worker_state["last_error_code"],
                "AUTONOMOUS_WORKER_EXCEPTION",
            )

            recovered = worker.tick(now=T0)

        self.assertEqual(recovered["status"], "NO_SIGNAL")
        self.assertEqual(recovered["candidates_signal_checked"], 10)
        self.assertEqual(recovered["signal_scan_cycle_id"], cycle_id)
        self.assertEqual(recovered["signal_scan_checked_this_cycle"], 20)
        self.assertEqual(recovered["signal_scan_remaining_this_cycle"], 0)
        self.assertEqual(recovered["signal_scan_status"], "COMPLETE_NO_SIGNAL")
        self.assertEqual(checked[:10], candidate_ids[:10])
        self.assertEqual(checked[10:], candidate_ids[10:])
        self.assertNotEqual(checked[:10], checked[10:])
        self.assertEqual(
            self.store.connection.execute(
                "SELECT COUNT(*) FROM canary_signal_scan_checked "
                "WHERE cycle_id=?",
                (cycle_id,),
            ).fetchone()[0],
            20,
        )

    def test_signal_evaluation_exception_persists_active_cycle_for_restart(self):
        candidate_ids = self._seed_durable_candidates(11, prefix="signal-failure")
        self._enable_worker()
        calls: list[str] = []
        failed_once = True

        def evaluate(service, candidate_id, **kwargs):
            nonlocal failed_once
            calls.append(candidate_id)
            if failed_once and len(calls) == 2:
                failed_once = False
                raise RuntimeError("injected signal-evaluation failure")
            return self._signal_evaluation(candidate_id, "NO_STRATEGY_SIGNAL")

        ranking_patch = self._durable_ranking_patch(
            candidate_ids,
            ["signal-failure"],
        )
        worker = AutonomousCanaryWorker(self.store, clock=lambda: T0)
        with ranking_patch, patch.object(
            CandidateCanaryRanker,
            "validate_persisted_ranking",
            return_value=True,
        ), patch.object(
            CanaryService,
            "evaluate_signal",
            autospec=True,
            side_effect=evaluate,
        ):
            failed = worker.tick(now=T0)

            self.assertEqual(failed["status"], "ERROR")
            self.assertEqual(failed["blocker"], "AUTONOMOUS_WORKER_EXCEPTION")
            state = self.store.connection.execute(
                "SELECT signal_scan_cycle_id,signal_scan_cycle_complete,"
                "signal_scan_checked_this_cycle,signal_scan_remaining_this_cycle,"
                "signal_scan_coverage_percentage,signal_scan_status,"
                "last_error_code,worker_status "
                "FROM canary_autonomous_state WHERE singleton=1"
            ).fetchone()
            self.assertTrue(state["signal_scan_cycle_id"])
            self.assertEqual(state["signal_scan_cycle_complete"], 0)
            self.assertEqual(state["signal_scan_checked_this_cycle"], 1)
            self.assertEqual(state["signal_scan_remaining_this_cycle"], 10)
            self.assertAlmostEqual(state["signal_scan_coverage_percentage"], 100 / 11)
            self.assertEqual(state["signal_scan_status"], "IN_PROGRESS")
            self.assertEqual(state["last_error_code"], "AUTONOMOUS_WORKER_EXCEPTION")
            self.assertEqual(state["worker_status"], "DEGRADED")
            cycle_id = state["signal_scan_cycle_id"]
            self.assertEqual(
                self.store.connection.execute(
                    "SELECT COUNT(*) FROM canary_signal_scan_checked "
                    "WHERE cycle_id=?",
                    (cycle_id,),
                ).fetchone()[0],
                1,
            )

            restarted = AutonomousCanaryWorker(self.store, clock=lambda: T0)
            recovered = restarted.tick(now=T0)

        self.assertEqual(recovered["status"], "NO_SIGNAL")
        self.assertEqual(recovered["candidates_signal_checked"], 10)
        self.assertEqual(recovered["signal_scan_cycle_id"], cycle_id)
        self.assertEqual(recovered["signal_scan_checked_this_cycle"], 11)
        self.assertEqual(recovered["signal_scan_remaining_this_cycle"], 0)
        self.assertEqual(recovered["signal_scan_coverage_percentage"], 100.0)
        self.assertEqual(recovered["signal_scan_status"], "COMPLETE_NO_SIGNAL")
        self.assertEqual(calls[0], candidate_ids[0])
        self.assertNotIn(candidate_ids[0], calls[2:])
        self.assertEqual(set(calls[2:]), set(candidate_ids[1:]))
        self.assertEqual(
            self.store.connection.execute(
                "SELECT COUNT(*) FROM canary_signal_scan_checked "
                "WHERE cycle_id=?",
                (cycle_id,),
            ).fetchone()[0],
            11,
        )

    def test_ranking_evidence_change_preserves_checked_coverage(self):
        candidate_ids = self._seed_durable_candidates(12, prefix="ranking")
        self._enable_worker()
        checked = []
        patches = self._durable_scan_patches(
            candidate_ids,
            ["ranking-a", "ranking-b"],
            checked,
        )
        worker = AutonomousCanaryWorker(self.store, clock=lambda: T0)
        with patches[0], patches[1], patches[2]:
            first = worker.tick(now=T0)
            CandidateLifecycleManager(self.store).record_evidence(
                candidate_ids[0],
                {
                    "forward_evidence": {
                        "forward_duration_seconds": 30 * 86400,
                        "forward_expectancy": -0.15,
                    }
                },
                expected_stage=CandidateStage.FROZEN,
                reason="ranking evidence update",
            )
            stale = self.service.status()
            self.assertEqual(stale["readiness_snapshot_status"], "STALE")
            self.assertEqual(
                stale["readiness_snapshot_reason"],
                "LIFECYCLE_RANKING_EVIDENCE_UPDATED",
            )
            second = worker.tick(now=T0)

        self.assertEqual(first["signal_scan_checked_this_cycle"], 10)
        self.assertEqual(second["signal_scan_checked_this_cycle"], 12)
        self.assertEqual(
            first["signal_scan_cycle_id"],
            second["signal_scan_cycle_id"],
        )
        self.assertNotIn(candidate_ids[0], checked[10:])
        self.assertEqual(second["signal_scan_status"], "COMPLETE_NO_SIGNAL")

    def test_ranking_run_change_and_telemetry_change_do_not_reset_cycle(self):
        candidate_ids = self._seed_durable_candidates(12, prefix="stable")
        self._enable_worker()
        checked = []
        patches = self._durable_scan_patches(
            candidate_ids,
            ["run-a", "run-b"],
            checked,
        )
        worker = AutonomousCanaryWorker(self.store, clock=lambda: T0)
        with patches[0], patches[1], patches[2]:
            first = worker.tick(now=T0)
            lifecycle = CandidateLifecycleManager(self.store)
            status_before_update = self.service.status()
            lifecycle.record_evidence(
                candidate_ids[0],
                {
                    "forward_evidence": {
                        "forward_liquidity": 0.42,
                        "forward_max_drawdown": 0.05,
                        "forward_fills": 17,
                        "forward_observations": 99,
                    }
                },
                expected_stage=CandidateStage.FROZEN,
                reason="telemetry-only update",
            )
            status_after_update = self.service.status()
            for key in (
                "readiness_snapshot_status",
                "readiness_snapshot_stale",
                "readiness_snapshot_reason",
            ):
                self.assertEqual(status_after_update[key], status_before_update[key])
            second = worker.tick(now=T0)

        self.assertEqual(first["candidates_signal_checked"], 10)
        self.assertEqual(second["candidates_signal_checked"], 2)
        self.assertEqual(first["signal_scan_checked_this_cycle"], 10)
        self.assertEqual(second["signal_scan_checked_this_cycle"], 12)
        self.assertEqual(
            first["signal_scan_cycle_id"],
            second["signal_scan_cycle_id"],
        )
        self.assertEqual(first["signal_scan_ranking_run_id"], "run-a")
        self.assertEqual(second["signal_scan_ranking_run_id"], "run-b")
        self.assertEqual(
            checked[:10],
            candidate_ids[:10],
        )
        self.assertEqual(
            set(checked[10:]),
            set(candidate_ids[10:]),
        )
        self.assertEqual(second["signal_scan_status"], "COMPLETE_NO_SIGNAL")

    def test_qualification_hash_change_rechecks_only_affected_candidate(self):
        candidate_ids = self._seed_durable_candidates(12, prefix="qualification")
        self._enable_worker()
        checked = []
        patches = self._durable_scan_patches(
            candidate_ids,
            ["qualification-run"] * 2,
            checked,
        )
        worker = AutonomousCanaryWorker(self.store, clock=lambda: T0)
        with patches[0], patches[1], patches[2]:
            first = worker.tick(now=T0)
            old_key = self.store.connection.execute(
                "SELECT qualification_hash FROM canary_signal_scan_checked "
                "WHERE cycle_id=? AND candidate_id=?",
                (first["signal_scan_cycle_id"], candidate_ids[0]),
            ).fetchone()["qualification_hash"]
            lifecycle = CandidateLifecycleManager(self.store)
            lifecycle.record_evidence(
                candidate_ids[0],
                {"validation_expectancy": 0.55},
                expected_stage=CandidateStage.FROZEN,
                reason="qualification change",
            )
            stale = self.service.status()
            self.assertEqual(stale["readiness_snapshot_status"], "STALE")
            self.assertTrue(stale["readiness_snapshot_stale"])
            self.assertEqual(
                stale["readiness_snapshot_reason"],
                "LIFECYCLE_QUALIFICATION_UPDATED",
            )
            self.service.mark_eligible(candidate_ids[0])
            second = worker.tick(now=T0)
        self.assertEqual(first["candidates_signal_checked"], 10)
        self.assertEqual(second["candidates_signal_checked"], 3)
        self.assertEqual(first["signal_scan_checked_this_cycle"], 10)
        self.assertEqual(second["signal_scan_checked_this_cycle"], 12)
        self.assertEqual(
            first["signal_scan_cycle_id"],
            second["signal_scan_cycle_id"],
        )
        self.assertIn(candidate_ids[0], checked[10:])
        self.assertEqual(
            set(checked[10:]),
            {candidate_ids[0], candidate_ids[10], candidate_ids[11]},
        )
        self.assertEqual(
            self.store.connection.execute(
                "SELECT COUNT(*) FROM canary_signal_scan_checked "
                "WHERE cycle_id=? AND candidate_id=?",
                (first["signal_scan_cycle_id"], candidate_ids[0]),
            ).fetchone()[0],
            2,
        )
        qualification_keys = {
            row["qualification_hash"]
            for row in self.store.connection.execute(
                "SELECT qualification_hash FROM canary_signal_scan_checked "
                "WHERE cycle_id=? AND candidate_id=?",
                (first["signal_scan_cycle_id"], candidate_ids[0]),
            ).fetchall()
        }
        self.assertEqual(len(qualification_keys), 2)
        self.assertIn(old_key, qualification_keys)
        self.assertEqual(len(qualification_keys - {old_key}), 1)

    def test_candidate_addition_joins_active_cycle_and_ineligible_removal_disappears(self):
        candidate_ids = self._seed_durable_candidates(12, prefix="membership")
        self._enable_worker()
        checked = []
        patches = self._durable_scan_patches(
            candidate_ids + ["membership-new"],
            ["membership-run"] * 3,
            checked,
        )
        worker = AutonomousCanaryWorker(self.store, clock=lambda: T0)
        with patches[0], patches[1], patches[2]:
            first = worker.tick(now=T0)
            self.seed_candidate(
                "membership-new",
                cluster="membership-new-cluster",
                score=0.95,
            )
            CandidateLifecycleManager(self.store).reject(
                candidate_ids[10],
                "test ineligibility",
                expected_stage=CandidateStage.FROZEN,
            )
            second = worker.tick(now=T0)
        self.assertEqual(first["candidates_signal_checked"], 10)
        self.assertEqual(second["candidates_signal_checked"], 2)
        self.assertEqual(first["signal_scan_checked_this_cycle"], 10)
        self.assertEqual(second["signal_scan_checked_this_cycle"], 12)
        self.assertEqual(second["signal_scan_cycle_id"], first["signal_scan_cycle_id"])
        self.assertIn("membership-new", checked[10:])
        self.assertNotIn(candidate_ids[10], checked[10:])
        self.assertEqual(second["signal_scan_status"], "COMPLETE_NO_SIGNAL")
        self.assertEqual(second["signal_scan_remaining_this_cycle"], 0)

    def test_remaining_candidates_follow_current_rank_order_after_score_change(self):
        candidate_ids = self._seed_durable_candidates(12, prefix="rescore")
        self._enable_worker()
        checked = []
        worker = AutonomousCanaryWorker(self.store, clock=lambda: T0)
        original_evaluate = CandidateCanaryRanker.evaluate_and_select
        calls = 0

        def evaluate(ranker, *args, **kwargs):
            nonlocal calls
            calls += 1
            result = original_evaluate(ranker, T0)
            if calls == 2:
                with self.store.connection:
                    self.store.connection.execute(
                        "UPDATE canary_rankings SET rank=? WHERE candidate_id=?",
                        (1, candidate_ids[11]),
                    )
                    self.store.connection.execute(
                        "UPDATE canary_rankings SET rank=? WHERE candidate_id=?",
                        (2, candidate_ids[10]),
                    )
            return result

        def signal_evaluation(service, candidate_id, **kwargs):
            checked.append(candidate_id)
            return self._signal_evaluation(candidate_id, "NO_STRATEGY_SIGNAL")

        with patch.object(
            CandidateCanaryRanker,
            "evaluate_and_select",
            autospec=True,
            side_effect=evaluate,
        ), patch.object(
            CandidateCanaryRanker,
            "validate_persisted_ranking",
            return_value=True,
        ), patch.object(
            CanaryService,
            "evaluate_signal",
            autospec=True,
            side_effect=signal_evaluation,
        ):
            first = worker.tick(now=T0)
            second = worker.tick(now=T0)

        self.assertEqual(first["candidates_signal_checked"], 10)
        self.assertEqual(second["candidates_signal_checked"], 2)
        self.assertEqual(first["signal_scan_checked_this_cycle"], 10)
        self.assertEqual(second["signal_scan_checked_this_cycle"], 12)
        self.assertEqual(
            checked[10:],
            [candidate_ids[11], candidate_ids[10]],
        )

    def test_lower_rank_ready_is_submitted_once_without_replacing_research_order(self):
        candidate_ids = self._seed_durable_candidates(2, prefix="ready")
        self._enable_worker()
        checked = []
        venue = TestVenue()
        worker = AutonomousCanaryWorker(
            self.store,
            clock=lambda: T0,
            venue_factory=lambda: venue,
            allow_test_venue=True,
        )

        def signal_evaluation(service, candidate_id, **kwargs):
            checked.append(candidate_id)
            if candidate_id == candidate_ids[1]:
                signal = self._ready_signal(candidate_id)
                return self._signal_evaluation(
                    candidate_id,
                    "READY_SIGNAL",
                    signal=signal,
                    market_id="market-1",
                )
            return self._signal_evaluation(candidate_id, "NO_STRATEGY_SIGNAL")

        ranking_patch = self._durable_ranking_patch(candidate_ids, ["ready-run"])
        with ranking_patch, patch.object(
            CandidateCanaryRanker,
            "validate_persisted_ranking",
            return_value=True,
        ), patch.object(
            CanaryService,
            "evaluate_signal",
            autospec=True,
            side_effect=signal_evaluation,
        ), patch.object(
            CanaryService,
            "bind_autonomous_actionable_candidate",
            return_value={"bound": True},
        ), patch.object(
            CanaryService,
            "submit_signal",
            return_value={"ok": True, "order_id": "one"},
        ) as submit, patch.object(
            CredentialStore,
            "configured",
            return_value=True,
        ):
            result = worker.tick(now=T0)

        self.assertEqual(result["status"], "SUBMITTED")
        self.assertEqual(result["candidate_id"], candidate_ids[1])
        self.assertEqual(result["selected_actionable_rank"], 2)
        self.assertEqual(checked, candidate_ids)
        submit.assert_called_once()
        self.assertEqual(len(venue.submissions), 0)

    def _post_rank_mutation(self, mutation):
        real_ranker = CandidateCanaryRanker(self.store, clock=lambda: T0)
        original = CandidateCanaryRanker.evaluate_and_select

        def evaluate(*args, **kwargs):
            timestamp = kwargs.get("now")
            if timestamp is None and args:
                timestamp = args[0]
            result = original(real_ranker, timestamp)
            mutation(result)
            return result

        return patch.object(
            CandidateCanaryRanker,
            "evaluate_and_select",
            side_effect=evaluate,
        )

    def test_scan_continues_after_rank_one_no_signal_to_rank_two_ready(self):
        self.seed_candidate("rank-one", cluster="cluster-one", score=0.90)
        self.seed_candidate("rank-two", cluster="cluster-two", score=0.80)
        self._enable_worker()
        checked = []
        venue = TestVenue()
        worker = AutonomousCanaryWorker(
            self.store, clock=lambda: T0, venue_factory=lambda: venue
        )

        def signal_evaluation(service, candidate_id, **kwargs):
            checked.append(candidate_id)
            if candidate_id == "rank-two":
                signal = self._ready_signal(candidate_id)
                return self._signal_evaluation(
                    candidate_id,
                    "READY_SIGNAL",
                    signal=signal,
                    market_id="market-1",
                )
            return self._signal_evaluation(candidate_id, "NO_STRATEGY_SIGNAL")

        with patch.object(CanaryService, "evaluate_signal", autospec=True, side_effect=signal_evaluation), \
            patch.object(
                CanaryService,
                "submit_signal",
                return_value={"ok": True, "order_id": "one"},
            ) as submit, \
            patch.object(
                CanaryService,
                "bind_autonomous_actionable_candidate",
                return_value={"bound": True},
            ) as bind, \
            patch.object(CredentialStore, "configured", return_value=True):
            result = worker.tick(now=T0)

        self.assertEqual(checked, ["rank-one", "rank-two"])
        self.assertEqual(result["status"], "SUBMITTED")
        self.assertEqual(result["candidate_id"], "rank-two")
        submit.assert_called_once()
        bind.assert_called_once_with(
            "rank-two",
            ranking_run_id=result["ranking"]["ranking_run_id"],
            signal_id="signal-rank-two",
        )
        ranking = result["ranking"]
        rank_two = next(
            row for row in ranking["rankings"] if row["candidate_id"] == "rank-two"
        )
        self._assert_scan_metrics(
            result,
            {
                "candidates_ranked": 2,
                "candidates_signal_checked": 2,
                "candidates_no_signal": 1,
                "actionable_candidates_found": 1,
                "selected_actionable_candidate": "rank-two",
                "selected_actionable_rank": 2,
                "selected_actionable_score": rank_two["total_score"],
                "signal_scan_cursor": 0,
                "signal_scan_ranking_run_id": ranking["ranking_run_id"],
                "next_signal_scan_start_rank": 0,
                "next_signal_scan_end_rank": 2,
            },
        )
    def test_real_worker_rebinds_actionable_rank_two_without_replacing_research_winner(self):
        self.seed_candidate(
            "research-rank-one",
            cluster="research-cluster-one",
            score=0.90,
            executable=False,
        )
        self.seed_candidate(
            "actionable-rank-two",
            cluster="research-cluster-two",
            score=0.80,
            executable=True,
        )
        self.save_forward_canary_snapshot("rank-two-ready-snapshot")
        initial = CandidateCanaryRanker(self.store, clock=lambda: T0).evaluate_and_select(T0)
        self.assertEqual(initial["selected_candidate"], "research-rank-one")
        self.assertEqual(initial["winner_id"], "research-rank-one")

        self.service.enable_autonomous_micro_live()
        venue = TestVenue()
        worker = AutonomousCanaryWorker(
            self.store,
            clock=lambda: T0,
            venue_factory=lambda: venue,
            allow_test_venue=True,
        )
        with patch.object(CredentialStore, "configured", return_value=True):
            result = worker.tick(now=T0)

        self.assertEqual(result["status"], "SUBMITTED")
        self.assertEqual(result["candidate_id"], "actionable-rank-two")
        self.assertEqual(result["selected_actionable_candidate"], "actionable-rank-two")
        self.assertEqual(result["ranking"]["selected_candidate"], "research-rank-one")
        self.assertEqual(result["ranking"]["winner_id"], "research-rank-one")
        self.assertEqual(len(venue.submissions), 1)
        self.assertEqual(venue.submissions[0]["token_id"], "yes")

        signal = self.service.latest_signal("actionable-rank-two")
        self.assertIsNotNone(signal)
        self.assertEqual(signal["status"], "SUBMITTED")
        self.assertEqual(signal["candidate_id"], "actionable-rank-two")
        self.assertIsNone(self.service.latest_signal("research-rank-one"))

        control = self.service.authoritative_status()
        self.assertEqual(control["candidate"], "research-rank-one")
        self.assertEqual(control["selected_candidate"], "research-rank-one")
        self.assertEqual(control["control_candidate"], "actionable-rank-two")
        self.assertEqual(control["winner_id"], "research-rank-one")
        selection = self.store.connection.execute(
            "SELECT candidate_id FROM canary_selection WHERE singleton=1"
        ).fetchone()
        self.assertEqual(selection["candidate_id"], "research-rank-one")


    def test_ready_rank_one_is_preferred_over_later_ready_candidate(self):
        self.seed_candidate("rank-one-ready", cluster="cluster-one", score=0.90)
        self.seed_candidate("rank-two-ready", cluster="cluster-two", score=0.80)
        self._enable_worker()
        checked = []
        worker = AutonomousCanaryWorker(
            self.store, clock=lambda: T0, venue_factory=TestVenue
        )

        def signal_evaluation(service, candidate_id, **kwargs):
            checked.append(candidate_id)
            signal = self._ready_signal(candidate_id)
            return self._signal_evaluation(
                candidate_id,
                "READY_SIGNAL",
                signal=signal,
                market_id="market-1",
            )

        with patch.object(CanaryService, "evaluate_signal", autospec=True, side_effect=signal_evaluation), \
            patch.object(
                CanaryService,
                "submit_signal",
                return_value={"ok": True, "order_id": "rank-one-order"},
            ) as submit, \
            patch.object(
                CanaryService,
                "bind_autonomous_actionable_candidate",
                return_value={"bound": True},
            ) as bind, \
            patch.object(CredentialStore, "configured", return_value=True):
            result = worker.tick(now=T0)

        self.assertEqual(checked, ["rank-one-ready", "rank-two-ready"])
        self.assertEqual(result["candidate_id"], "rank-one-ready")
        submit.assert_called_once()
        bind.assert_called_once_with(
            "rank-one-ready",
            ranking_run_id=result["ranking"]["ranking_run_id"],
            signal_id="signal-rank-one-ready",
        )
        self._assert_scan_metrics(
            result,
            {
                "candidates_ranked": 2,
                "candidates_signal_checked": 2,
                "candidates_no_signal": 0,
                "actionable_candidates_found": 2,
                "selected_actionable_candidate": "rank-one-ready",
                "selected_actionable_rank": 1,
                "selected_actionable_score": next(
                    row["total_score"]
                    for row in result["ranking"]["rankings"]
                    if row["candidate_id"] == "rank-one-ready"
                ),
                "signal_scan_cursor": 0,
                "signal_scan_ranking_run_id": result["ranking"]["ranking_run_id"],
                "next_signal_scan_start_rank": 0,
                "next_signal_scan_end_rank": 2,
            },
        )

    def test_expired_persisted_ready_signal_refreshes_same_tick_and_prefers_rank_one(self):
        self.seed_candidate(
            "expired-persisted",
            cluster="expired-cluster",
            score=0.90,
            executable=True,
        )
        self.seed_candidate(
            "later-valid",
            cluster="later-cluster",
            score=0.80,
            executable=True,
        )
        self.save_forward_canary_snapshot("expired-signal-snapshot")
        self._enable_worker()
        CandidateCanaryRanker(self.store, clock=lambda: T0).evaluate_and_select(T0)

        persisted = self.service.generate_signal("expired-persisted")
        self.assertIsNotNone(persisted)
        assert persisted is not None
        signal_id = str(persisted["signal_id"])
        with self.store.connection:
            self.store.connection.execute(
                "UPDATE canary_signals SET expires_at=? WHERE signal_id=?",
                ((T0 - timedelta(seconds=1)).isoformat(), signal_id),
            )

        checked: list[str] = []
        generated: dict[str, object] = {}
        venue = TestVenue()
        worker = AutonomousCanaryWorker(
            self.store,
            clock=lambda: T0,
            venue_factory=lambda: venue,
            allow_test_venue=True,
        )
        original_evaluate_signal = CanaryService.evaluate_signal

        def evaluate_signal(service, candidate_id, **kwargs):
            checked.append(candidate_id)
            evaluation = original_evaluate_signal(
                service,
                candidate_id,
                **kwargs,
            )
            generated[candidate_id] = (
                evaluation.get("signal")
                if isinstance(evaluation, Mapping)
                else None
            )
            return evaluation

        with patch.object(
            CanaryService,
            "evaluate_signal",
            autospec=True,
            side_effect=evaluate_signal,
        ), patch.object(CredentialStore, "configured", return_value=True):
            result = worker.tick(now=T0)

        self.assertEqual(checked, ["expired-persisted", "later-valid"])
        self.assertEqual(result["status"], "SUBMITTED")
        self.assertEqual(result["candidate_id"], "expired-persisted")
        self.assertEqual(result["selected_actionable_candidate"], "expired-persisted")
        self.assertEqual(result["selected_actionable_rank"], 1)
        self.assertEqual(result["candidates_ranked"], 2)
        self.assertEqual(result["candidates_signal_checked"], 2)
        self.assertEqual(result["candidates_no_signal"], 0)
        self.assertEqual(result["actionable_candidates_found"], 2)
        state = self.store.connection.execute(
            "SELECT candidates_evaluated, signals_generated, orders_attempted "
            "FROM canary_autonomous_state WHERE singleton=1"
        ).fetchone()
        self.assertIsNotNone(state)
        assert state is not None
        self.assertEqual(state["candidates_evaluated"], 2)
        self.assertEqual(state["signals_generated"], 2)
        self.assertEqual(state["orders_attempted"], 1)

        old_signal = self.service.get_signal(signal_id)
        self.assertIsNotNone(old_signal)
        assert old_signal is not None
        self.assertEqual(old_signal["status"], "EXPIRED")
        self.assertEqual(old_signal["reason"], "SIGNAL_EXPIRED")

        refreshed = generated["expired-persisted"]
        self.assertIsInstance(refreshed, dict)
        assert isinstance(refreshed, dict)
        self.assertEqual(refreshed["status"], "READY")
        self.assertEqual(refreshed["candidate_id"], "expired-persisted")
        self.assertNotEqual(refreshed["signal_id"], signal_id)
        self.assertEqual(result["signal_id"], refreshed["signal_id"])

        latest_expired_candidate = self.service.latest_signal("expired-persisted")
        self.assertIsNotNone(latest_expired_candidate)
        assert latest_expired_candidate is not None
        self.assertEqual(latest_expired_candidate["status"], "SUBMITTED")
        self.assertEqual(latest_expired_candidate["signal_id"], refreshed["signal_id"])

        later = generated["later-valid"]
        self.assertIsInstance(later, dict)
        assert isinstance(later, dict)
        self.assertEqual(later["status"], "READY")
        self.assertEqual(later["candidate_id"], "later-valid")
        self.assertEqual(self.service.latest_signal("later-valid"), later)
        self.assertEqual(
            self.store.connection.execute(
                "SELECT COUNT(*) FROM canary_ledger WHERE signal_id=?",
                (later["signal_id"],),
            ).fetchone()[0],
            0,
        )

        submit_rows = self.store.connection.execute(
            "SELECT signal_id,candidate_id,status FROM canary_signals "
            "WHERE status='SUBMITTED'"
        ).fetchall()
        self.assertEqual(len(submit_rows), 1)
        self.assertEqual(submit_rows[0]["candidate_id"], "expired-persisted")
        self.assertEqual(len(venue.submissions), 1)
        self.assertEqual(
            self.store.connection.execute(
                "SELECT COUNT(*) FROM canary_ledger"
            ).fetchone()[0],
            1,
        )
        signal_rows = self.store.connection.execute(
            "SELECT status FROM canary_signals WHERE candidate_id=?",
            ("expired-persisted",),
        ).fetchall()
        self.assertEqual(
            sorted(row["status"] for row in signal_rows),
            ["EXPIRED", "SUBMITTED"],
        )

        ranking = result["ranking"]
        rank_one = next(
            row for row in ranking["rankings"] if row["candidate_id"] == "expired-persisted"
        )
        self._assert_scan_metrics(
            result,
            {
                "candidates_ranked": 2,
                "candidates_signal_checked": 2,
                "candidates_no_signal": 0,
                "actionable_candidates_found": 2,
                "selected_actionable_candidate": "expired-persisted",
                "selected_actionable_rank": 1,
                "selected_actionable_score": rank_one["total_score"],
                "signal_scan_cursor": 0,
                "signal_scan_ranking_run_id": ranking["ranking_run_id"],
                "next_signal_scan_start_rank": 0,
                "next_signal_scan_end_rank": 2,
            },
        )


    def test_follower_evidence_change_preserves_checked_ids_and_reorders_remaining_candidates(self):
        for index in range(1, 13):
            self.seed_candidate(
                f"follower-reset-{index:02d}",
                cluster=f"follower-reset-cluster-{index:02d}",
                score=1.0 - index / 100.0,
            )
        follower_a = candidate_payload(
            "follower-reset-follower-a",
            cluster="follower-reset-cluster-01",
            score=0.10,
        )
        follower_a["forward_expectancy"] = 0.20
        follower_b = candidate_payload(
            "follower-reset-follower-b",
            cluster="follower-reset-cluster-01",
            score=0.09,
        )
        follower_b["forward_expectancy"] = 0.10
        for candidate_id, payload in (
            ("follower-reset-follower-a", follower_a),
            ("follower-reset-follower-b", follower_b),
        ):
            self.store.save_candidate_lifecycle(
                candidate_id,
                "IDEA",
                payload,
                timestamp=T0,
            )
            self.store.save_candidate_lifecycle(
                candidate_id,
                "FROZEN",
                payload,
                timestamp=T0,
            )
            self.bind_fixture_scope(candidate_id, payload)
        self._enable_worker()
        checked: list[str] = []
        worker = AutonomousCanaryWorker(
            self.store,
            clock=lambda: T0,
            venue_factory=TestVenue,
        )

        with patch.object(
            CanaryService,
            "evaluate_signal",
            autospec=True,
            side_effect=lambda service, candidate_id, **kwargs: (
                checked.append(candidate_id)
                or self._signal_evaluation(candidate_id, "NO_STRATEGY_SIGNAL")
            ),
        ):
            first = worker.tick(now=T0)
            first_state = self.store.connection.execute(
                "SELECT * FROM canary_autonomous_state WHERE singleton=1"
            ).fetchone()
            mutated_a = dict(follower_a)
            mutated_a["forward_expectancy"] = 0.10
            mutated_b = dict(follower_b)
            mutated_b["forward_expectancy"] = 0.20
            self.store.save_candidate_lifecycle(
                "follower-reset-follower-a",
                "FROZEN",
                mutated_a,
                from_stage="FROZEN",
                timestamp=T0 + timedelta(seconds=1),
            )
            self.store.save_candidate_lifecycle(
                "follower-reset-follower-b",
                "FROZEN",
                mutated_b,
                from_stage="FROZEN",
                timestamp=T0 + timedelta(seconds=1),
            )
            second = worker.tick(now=T0 + timedelta(seconds=2))

        first_rows = first["ranking"]["rankings"]
        second_rows = second["ranking"]["rankings"]
        first_representatives = [
            row["candidate_id"]
            for row in first_rows
            if int(row["cluster_representative"] or 0) == 1
        ]
        second_representatives = [
            row["candidate_id"]
            for row in second_rows
            if int(row["cluster_representative"] or 0) == 1
        ]
        first_followers = [
            row["candidate_id"]
            for row in first_rows
            if int(row["cluster_representative"] or 0) == 0
        ]
        second_followers = [
            row["candidate_id"]
            for row in second_rows
            if int(row["cluster_representative"] or 0) == 0
        ]
        self.assertEqual(first["signal_scan_cursor"], 10)
        self.assertEqual(first_state["signal_scan_cursor"], 10)
        self.assertEqual(first_representatives, second_representatives)
        self.assertNotEqual(first_followers, second_followers)
        self.assertNotEqual(
            first["signal_scan_ranking_run_id"],
            second["signal_scan_ranking_run_id"],
        )
        self.assertEqual(
            checked[:10],
            [f"follower-reset-{index:02d}" for index in range(1, 11)],
        )
        self.assertTrue(set(checked[:10]).isdisjoint(checked[10:]))
        self.assertEqual(
            set(checked[10:]),
            {
                "follower-reset-11",
                "follower-reset-12",
                "follower-reset-follower-a",
                "follower-reset-follower-b",
            },
        )
        self.assertEqual(second["signal_scan_cursor"], 0)
        self.assertEqual(second["next_signal_scan_start_rank"], 0)



    def test_no_actionable_signal_never_submits(self):
        self.seed_candidate("no-signal-one", cluster="cluster-one", score=0.90)
        self.seed_candidate("no-signal-two", cluster="cluster-two", score=0.80)
        self._enable_worker()
        checked = []
        venue_calls = []
        worker = AutonomousCanaryWorker(
            self.store,
            clock=lambda: T0,
            venue_factory=lambda: venue_calls.append(True),
        )

        def signal_evaluation(service, candidate_id, **kwargs):
            checked.append(candidate_id)
            return self._signal_evaluation(candidate_id, "NO_STRATEGY_SIGNAL")

        with patch.object(
            CanaryService,
            "evaluate_signal",
            autospec=True,
            side_effect=signal_evaluation,
        ), \
            patch.object(CanaryService, "submit_signal") as submit:
            result = worker.tick(now=T0)

        self.assertEqual(checked, ["no-signal-one", "no-signal-two"])
        self.assertEqual(result["status"], "NO_SIGNAL")
        self.assertEqual(result["blocker"], "NO_ACTIONABLE_SIGNAL")
        submit.assert_not_called()
        self.assertEqual(venue_calls, [])
        self._assert_scan_metrics(
            result,
            {
                "candidates_ranked": 2,
                "candidates_signal_checked": 2,
                "candidates_no_signal": 2,
                "actionable_candidates_found": 0,
                "selected_actionable_candidate": None,
                "selected_actionable_rank": None,
                "selected_actionable_score": None,
                "signal_scan_cursor": 0,
                "signal_scan_ranking_run_id": result["ranking"]["ranking_run_id"],
                "next_signal_scan_start_rank": 0,
                "next_signal_scan_end_rank": 0,
            },
        )

    def test_scan_window_advances_without_starving_later_ranked_candidate(self):
        for index in range(1, 13):
            self.seed_candidate(
                f"window-{index:02d}",
                cluster=f"window-cluster-{index:02d}",
                score=1.0 - index / 100.0,
            )
        self._enable_worker()
        checked = []
        worker = AutonomousCanaryWorker(
            self.store, clock=lambda: T0, venue_factory=TestVenue
        )

        def signal_evaluation(service, candidate_id, **kwargs):
            checked.append(candidate_id)
            if candidate_id == "window-12":
                signal = self._ready_signal(candidate_id)
                return self._signal_evaluation(
                    candidate_id,
                    "READY_SIGNAL",
                    signal=signal,
                    market_id="market-1",
                )
            return self._signal_evaluation(candidate_id, "NO_STRATEGY_SIGNAL")

        with patch.object(CanaryService, "evaluate_signal", autospec=True, side_effect=signal_evaluation), \
            patch.object(
                CanaryService,
                "submit_signal",
                return_value={"ok": True, "order_id": "window-order"},
            ) as submit, \
            patch.object(
                CanaryService,
                "bind_autonomous_actionable_candidate",
                return_value={"bound": True},
            ) as bind, \
            patch.object(CredentialStore, "configured", return_value=True):
            first = worker.tick(now=T0)
            self.assertEqual(first["status"], "NO_SIGNAL")
            first_state = self.store.connection.execute(
                "SELECT * FROM canary_autonomous_state WHERE singleton=1"
            ).fetchone()
            second = worker.tick(now=T0)

        self.assertEqual(
            checked[:10],
            [f"window-{index:02d}" for index in range(1, 11)],
        )
        self.assertEqual(first["candidates_signal_checked"], 10)
        self.assertEqual(first_state["signal_scan_cursor"], 10)
        self.assertEqual(first_state["next_signal_scan_start_rank"], 10)
        self.assertEqual(first_state["next_signal_scan_end_rank"], 12)
        self.assertEqual(checked[10:], ["window-11", "window-12"])
        self.assertEqual(second["status"], "SUBMITTED")
        self.assertEqual(second["candidate_id"], "window-12")
        submit.assert_called_once()
        bind.assert_called_once_with(
            "window-12",
            ranking_run_id=second["ranking"]["ranking_run_id"],
            signal_id="signal-window-12",
        )
        self.assertEqual(second["selected_actionable_rank"], 12)
        self.assertEqual(second["signal_scan_cursor"], 0)
        self.assertEqual(second["next_signal_scan_start_rank"], 0)
        self.assertEqual(second["next_signal_scan_end_rank"], 10)
    def test_ranking_change_adds_candidate_to_current_cycle_without_rechecking_checked_ids(self):
        for index in range(1, 13):
            self.seed_candidate(
                f"reset-{index:02d}",
                cluster=f"reset-cluster-{index:02d}",
                score=1.0 - index / 100.0,
            )
        self._enable_worker()
        checked = []
        worker = AutonomousCanaryWorker(
            self.store, clock=lambda: T0, venue_factory=TestVenue
        )

        with patch.object(
            CanaryService,
            "evaluate_signal",
            autospec=True,
            side_effect=lambda service, candidate_id, **kwargs: (
                checked.append(candidate_id)
                or self._signal_evaluation(candidate_id, "NO_STRATEGY_SIGNAL")
            ),
        ):
            first = worker.tick(now=T0)
            first_state = self.store.connection.execute(
                "SELECT * FROM canary_autonomous_state WHERE singleton=1"
            ).fetchone()
            self.seed_candidate(
                "ranking-reset-new",
                cluster="reset-new-cluster",
                score=2.0,
            )
            second = worker.tick(now=T0)

        self.assertEqual(first["signal_scan_cursor"], 10)
        self.assertEqual(first_state["signal_scan_cursor"], 10)
        self.assertEqual(first_state["next_signal_scan_start_rank"], 10)
        self.assertEqual(second["signal_scan_cursor"], 0)
        self.assertEqual(second["next_signal_scan_start_rank"], 0)
        self.assertEqual(second["next_signal_scan_end_rank"], 0)
        self.assertNotEqual(
            first_state["signal_scan_ranking_run_id"],
            second["signal_scan_ranking_run_id"],
        )
        self.assertEqual(
            checked[:10],
            [f"reset-{index:02d}" for index in range(1, 11)],
        )
        self.assertTrue(set(checked[:10]).isdisjoint(checked[10:]))
        self.assertEqual(
            set(checked[10:]),
            {"ranking-reset-new", "reset-11", "reset-12"},
        )
        self.assertEqual(second["candidates_ranked"], 13)
        self.assertEqual(second["candidates_signal_checked"], 3)
        self.assertEqual(second["signal_scan_checked_this_cycle"], 13)
        self.assertEqual(second["signal_scan_remaining_this_cycle"], 0)
        self.assertEqual(second["signal_scan_status"], "COMPLETE_NO_SIGNAL")
        self.assertEqual(second["actionable_candidates_found"], 0)


    def test_equivalent_mutation_clusters_do_not_consume_initial_scan_budget(self):
        self.seed_candidate("cluster-a-one", cluster="a", score=0.99)
        self.seed_candidate("cluster-a-two", cluster="a", score=0.98)
        self.seed_candidate("cluster-b-one", cluster="b", score=0.97)
        self.seed_candidate("cluster-b-two", cluster="b", score=0.96)
        for index, cluster in enumerate("cdefghij", start=3):
            self.seed_candidate(
                f"cluster-{cluster}-one",
                cluster=cluster,
                score=1.0 - index / 100.0,
            )
        self._enable_worker()
        checked = []
        worker = AutonomousCanaryWorker(
            self.store, clock=lambda: T0, venue_factory=TestVenue
        )

        def signal_evaluation(service, candidate_id, **kwargs):
            checked.append(candidate_id)
            if candidate_id == "cluster-j-one":
                signal = self._ready_signal(candidate_id)
                return self._signal_evaluation(
                    candidate_id,
                    "READY_SIGNAL",
                    signal=signal,
                    market_id="market-1",
                )
            return self._signal_evaluation(candidate_id, "NO_STRATEGY_SIGNAL")

        with patch.object(CanaryService, "evaluate_signal", autospec=True, side_effect=signal_evaluation), \
            patch.object(
                CanaryService,
                "submit_signal",
                return_value={"ok": True, "order_id": "cluster-order"},
            ), \
            patch.object(
                CanaryService,
                "bind_autonomous_actionable_candidate",
                return_value={"bound": True},
            ) as bind, \
            patch.object(CredentialStore, "configured", return_value=True):
            result = worker.tick(now=T0)

        self.assertEqual(result["status"], "SUBMITTED")
        self.assertEqual(result["candidate_id"], "cluster-j-one")
        bind.assert_called_once_with(
            "cluster-j-one",
            ranking_run_id=result["ranking"]["ranking_run_id"],
            signal_id="signal-cluster-j-one",
        )
        self.assertIn("cluster-j-one", checked)
        self.assertLessEqual(len(checked), 10)
        checked_clusters = {
            candidate_id.rsplit("-", 2)[1]
            for candidate_id in checked
            if candidate_id.endswith("-one")
        }
        self.assertEqual(len(checked_clusters), len(checked))
        self.assertNotIn("cluster-a-two", checked)
        self.assertNotIn("cluster-b-two", checked)
        self.assertEqual(result["candidates_signal_checked"], len(checked))

    def test_duplicate_cluster_followers_are_deferred_and_eventually_checked(self):
        representatives = []
        followers = []
        for index in range(10):
            cluster = f"follower-cluster-{index:02d}"
            representative = f"{cluster}-representative"
            follower = f"{cluster}-follower"
            self.seed_candidate(
                representative,
                cluster=cluster,
                score=0.99 - index / 1000,
            )
            self.seed_candidate(
                follower,
                cluster=cluster,
                score=0.98 - index / 1000,
            )
            representatives.append(representative)
            followers.append(follower)
        self._enable_worker()
        checked: list[str] = []
        worker = AutonomousCanaryWorker(self.store, clock=lambda: T0)

        with patch.object(
            CanaryService,
            "evaluate_signal",
            autospec=True,
            side_effect=lambda service, candidate_id, **kwargs: (
                checked.append(candidate_id)
                or self._signal_evaluation(candidate_id, "NO_STRATEGY_SIGNAL")
            ),
        ):
            first = worker.tick(now=T0)
            second = worker.tick(now=T0)

        self.assertGreater(
            max(
                first["signal_scan_skip_reasons_json"]["DUPLICATE_CLUSTER_DEFERRED"],
                second["signal_scan_skip_reasons_json"]["DUPLICATE_CLUSTER_DEFERRED"],
            ),
            0,
        )
        self.assertEqual(first["candidates_signal_checked"], 10)
        self.assertEqual(second["status"], "NO_SIGNAL")
        self.assertEqual(second["signal_scan_status"], "COMPLETE_NO_SIGNAL")
        self.assertEqual(second["signal_scan_checked_this_cycle"], 20)
        self.assertEqual(second["signal_scan_coverage_percentage"], 100.0)
        self.assertEqual(checked[:10], representatives)
        self.assertEqual(set(checked[10:]), set(followers))
        self.assertEqual(set(checked), set(representatives + followers))

    def test_stale_and_invalid_rankings_are_skipped_before_signal_evaluation(self):
        self.seed_candidate("stale-ranking", cluster="stale", score=0.90)
        self.seed_candidate("valid-ranking", cluster="valid", score=0.80)
        self.seed_candidate("invalid-ranking", cluster="invalid", score=0.70)
        self._enable_worker()
        checked = []
        worker = AutonomousCanaryWorker(
            self.store, clock=lambda: T0, venue_factory=TestVenue
        )

        def mutate(result):
            with self.store.connection:
                self.store.connection.execute(
                    "UPDATE canary_rankings SET ranking_snapshot_hash=? "
                    "WHERE candidate_id=?",
                    ("stale-hash", "stale-ranking"),
                )
                self.store.connection.execute(
                    "UPDATE canary_rankings SET total_score=NULL "
                    "WHERE candidate_id=?",
                    ("invalid-ranking",),
                )

        def signal_evaluation(service, candidate_id, **kwargs):
            checked.append(candidate_id)
            if candidate_id == "valid-ranking":
                signal = self._ready_signal(candidate_id)
                return self._signal_evaluation(
                    candidate_id,
                    "READY_SIGNAL",
                    signal=signal,
                    market_id="market-1",
                )
            return self._signal_evaluation(candidate_id, "NO_STRATEGY_SIGNAL")

        with self._post_rank_mutation(mutate), \
            patch.object(CanaryService, "evaluate_signal", autospec=True, side_effect=signal_evaluation), \
            patch.object(
                CanaryService,
                "submit_signal",
                return_value={"ok": True, "order_id": "valid-order"},
            ) as submit, \
            patch.object(
                CanaryService,
                "bind_autonomous_actionable_candidate",
                return_value={"bound": True},
            ) as bind, \
            patch.object(CredentialStore, "configured", return_value=True):
            result = worker.tick(now=T0)

        self.assertEqual(checked, ["valid-ranking"])
        self.assertEqual(result["candidate_id"], "valid-ranking")
        submit.assert_called_once()
        bind.assert_called_once_with(
            "valid-ranking",
            ranking_run_id=result["ranking"]["ranking_run_id"],
            signal_id="signal-valid-ranking",
        )
        self.assertEqual(result["candidates_ranked"], 3)
        self.assertEqual(result["candidates_signal_checked"], 1)
        self.assertEqual(result["candidates_no_signal"], 0)

    def test_multiple_ready_signals_use_deterministic_fallback_order(self):
        for candidate_id, score in (
            ("fallback-alpha", 0.90),
            ("fallback-beta", 0.80),
            ("fallback-gamma", 0.70),
        ):
            self.seed_candidate(
                candidate_id,
                cluster=candidate_id,
                score=score,
            )
        self._enable_worker()
        worker = AutonomousCanaryWorker(
            self.store, clock=lambda: T0, venue_factory=TestVenue
        )
        with patch.object(
            CanaryService,
            "evaluate_signal",
            autospec=True,
            side_effect=lambda service, candidate_id, **kwargs: self._signal_evaluation(
                candidate_id,
                "READY_SIGNAL",
                signal=self._ready_signal(candidate_id),
                market_id="market-1",
            ),
        ), patch.object(
            CanaryService,
            "submit_signal",
            return_value={"ok": True, "order_id": "fallback-order"},
        ) as submit, patch.object(
            CanaryService,
            "bind_autonomous_actionable_candidate",
            return_value={"bound": True},
        ) as bind, patch.object(
            CredentialStore, "configured", return_value=True
        ):
            result = worker.tick(now=T0)

        self.assertEqual(result["candidate_id"], "fallback-alpha")
        submit.assert_called_once()
        bind.assert_called_once_with(
            "fallback-alpha",
            ranking_run_id=result["ranking"]["ranking_run_id"],
            signal_id="signal-fallback-alpha",
        )
        self.assertEqual(result["actionable_candidates_found"], 3)
        self.assertEqual(result["selected_actionable_rank"], 1)
        self.assertEqual(
            result["selected_actionable_score"],
            next(
                row["total_score"]
                for row in result["ranking"]["rankings"]
                if row["candidate_id"] == "fallback-alpha"
            ),
        )

    def test_worker_makes_exactly_one_submission_decision(self):
        self.seed_candidate("one-submit", cluster="one-submit", score=0.90)
        self.seed_candidate("not-submitted", cluster="not-submitted", score=0.80)
        self._enable_worker()
        submit_calls = []
        worker = AutonomousCanaryWorker(
            self.store, clock=lambda: T0, venue_factory=TestVenue
        )
        with patch.object(
            CanaryService,
            "evaluate_signal",
            autospec=True,
            side_effect=lambda service, candidate_id, **kwargs: self._signal_evaluation(
                candidate_id,
                "READY_SIGNAL",
                signal=self._ready_signal(candidate_id),
                market_id="market-1",
            ),
        ), patch.object(
            CanaryService,
            "submit_signal",
            side_effect=lambda signal_id, **kwargs: submit_calls.append(signal_id)
            or {"ok": True, "order_id": "exactly-one"},
        ) as submit, patch.object(
            CanaryService,
            "bind_autonomous_actionable_candidate",
            return_value={"bound": True},
        ) as bind, patch.object(
            CredentialStore, "configured", return_value=True
        ):
            result = worker.tick(now=T0)

        self.assertEqual(result["status"], "SUBMITTED")
        self.assertEqual(submit.call_count, 1)
        self.assertEqual(submit_calls, ["signal-one-submit"])
        bind.assert_called_once_with(
            "one-submit",
            ranking_run_id=result["ranking"]["ranking_run_id"],
            signal_id="signal-one-submit",
        )

    def test_actionable_scan_preserves_autonomous_risk_limits(self):
        self._enable_worker()
        before = self.service.status()["limits"]
        worker = AutonomousCanaryWorker(
            self.store, clock=lambda: T0, venue_factory=TestVenue
        )
        with patch.object(CandidateCanaryRanker, "evaluate_and_select") as evaluate:
            evaluate.return_value = {
                "ranking_run_id": "empty-run",
                "rankings": [],
                "selected_candidate": None,
            }
            result = worker.tick(now=T0)
        after = self.service.status()["limits"]
        self.assertEqual(before, after)
        self.assertEqual(result["selected_actionable_candidate"], None)
        stored_limits = json.loads(
            self.store.connection.execute(
                "SELECT limits_json FROM canary_control WHERE singleton=1"
            ).fetchone()["limits_json"]
        )
        self.assertEqual(stored_limits, before)

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
        self.store.save_polymarket_market_metadata(
            "market-1",
            {
                "market_id": "market-1",
                "active": True,
                "closed": False,
                "settlement": "open",
            },
            observed_at=T0,
            source_type="FORWARD_COLLECTED",
        )
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
