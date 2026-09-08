from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from axiom.auto_canary import AutonomousCanaryWorker
from axiom.canary import CanaryService
from axiom.dashboard import DashboardData
from axiom.storage import AxiomStore


T0 = datetime(2025, 1, 2, 12, 0, tzinfo=timezone.utc)


class _ExplodingProvider:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def __getattr__(self, name: str):
        def fail(*args, **kwargs):
            self.calls.append(name)
            raise AssertionError(f"dashboard called provider method {name}")

        return fail


class _NoCredentialProbe:
    forbidden_calls: list[str] = []

    @classmethod
    def cached_projection(cls, *args, **kwargs):
        return {
            "configured": None,
            "status": "NOT CHECKED",
            "secret_values_exposed": False,
        }

    def configured(self, *args, **kwargs):
        type(self).forbidden_calls.append("configured")
        raise AssertionError("dashboard attempted a credential check")

    def load(self, *args, **kwargs):
        type(self).forbidden_calls.append("load")
        raise AssertionError("dashboard attempted credential loading")


def _document_hash(value: object) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _historical_dataset(store: AxiomStore) -> None:
    store.save_dataset(
        "prediction-history",
        "v1",
        [{"timestamp": T0.isoformat(), "price": 0.5, "source_type": "HISTORICAL"}],
    )
    store.save_dataset_catalog(
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


def _candidate_payload(
    candidate_id: str,
    *,
    target_market_id: str | None,
    executable: bool,
    model_probability: float = 0.80,
    cluster: str = "cluster-a",
    score: float = 0.10,
) -> dict[str, object]:
    parts = ("strategy-v1", "model-v1", "config-v1")
    payload: dict[str, object] = {
        "market_type": "prediction",
        "dataset_id": "prediction-history",
        "dataset_version": "v1",
        "dataset_provenance": {
            "dataset_id": "prediction-history",
            "dataset_version": "v1",
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
        "holdout_used": False,
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
    if target_market_id:
        payload["target_market_ids"] = [target_market_id]
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
        model = {"probability": model_probability}
        payload["strategy_document"] = strategy
        payload["model_document"] = model
        payload["strategy_hash"] = _document_hash(strategy)
        payload["model_hash"] = _document_hash(model)
        payload["config_hash"] = "config-hash"
        payload["frozen_hash"] = hashlib.sha256(
            "|".join(
                (
                    str(payload["strategy_hash"]),
                    str(payload["model_hash"]),
                    str(payload["config_hash"]),
                )
            ).encode()
        ).hexdigest()
    return payload


def _seed_candidate(
    store: AxiomStore,
    service: CanaryService,
    candidate_id: str,
    *,
    target_market_id: str | None,
    executable: bool,
    model_probability: float = 0.80,
    cluster: str = "cluster-a",
    score: float = 0.10,
) -> dict[str, object]:
    payload = _candidate_payload(
        candidate_id,
        target_market_id=target_market_id,
        executable=executable,
        model_probability=model_probability,
        cluster=cluster,
        score=score,
    )
    store.save_candidate_lifecycle(candidate_id, "IDEA", payload, timestamp=T0)
    store.save_candidate_lifecycle(
        candidate_id,
        "FROZEN",
        payload,
        from_stage="IDEA",
        timestamp=T0,
    )
    service.mark_eligible(candidate_id, publish_readiness=False)
    return payload


def _seed_market(
    store: AxiomStore,
    market_id: str,
    *,
    snapshot: bool,
    stale_model_probability: float = 0.10,
) -> None:
    expiry = (T0 + timedelta(hours=2)).isoformat()
    store.save_polymarket_market_metadata(
        market_id,
        {
            "market_id": market_id,
            "question": f"Will {market_id} resolve yes?",
            "active": True,
            "closed": False,
            "settlement": "open",
            "expiry": expiry,
            "source_type": "FORWARD_COLLECTED",
        },
        observed_at=T0,
        source_type="FORWARD_COLLECTED",
    )
    if not snapshot:
        return
    order_book = {
        "timestamp": T0.isoformat(),
        "token_id": f"{market_id}-yes-token",
        "asks": [{"price": "0.21", "size": "10"}],
    }
    observation = {
        "market_id": market_id,
        "question": f"Will {market_id} resolve yes?",
        "yes_mid": 0.20,
        "yes_ask": 0.21,
        "no_mid": 0.80,
        "no_ask": 0.81,
        "model_probability": stale_model_probability,
        "settlement": "open",
        "expiry": expiry,
        "active": True,
        "closed": False,
        "yes_token_id": f"{market_id}-yes-token",
        "no_token_id": f"{market_id}-no-token",
        "token_ids": {
            "yes": f"{market_id}-yes-token",
            "no": f"{market_id}-no-token",
        },
    }
    store.save_polymarket_snapshot(
        f"{market_id}-snapshot-1",
        market_id,
        T0,
        T0,
        {
            "source_type": "FORWARD_COLLECTED",
            "snapshot": observation,
            "yes_order_book": order_book,
            "research_quality": "TIMESTAMPED_DEPTH",
        },
        quality="TIMESTAMPED_DEPTH",
        source_type="FORWARD_COLLECTED",
    )


class PolymarketCombinedIntegrationTests(unittest.TestCase):
    def _store(self):
        directory = tempfile.TemporaryDirectory()
        try:
            store = AxiomStore(str(Path(directory.name) / "combined.sqlite"))
        except BaseException:
            directory.cleanup()
            raise

        def cleanup() -> None:
            try:
                store.close()
            finally:
                directory.cleanup()

        self.addCleanup(cleanup)
        return store

    def test_dashboard_combined_reads_are_storage_only_and_bounded(self):
        store = self._store()
        market_count = 1_050
        for index in range(market_count):
            _seed_market(store, f"dashboard-market-{index:04d}", snapshot=True)

        crypto_provider = _ExplodingProvider()
        prediction_provider = _ExplodingProvider()
        _NoCredentialProbe.forbidden_calls = []
        dashboard = DashboardData(
            store=store,
            crypto_provider=crypto_provider,
            prediction_provider=prediction_provider,
        )
        with patch("axiom.dashboard.canary_module.CredentialStore", _NoCredentialProbe):
            overview = dashboard.overview_summary()
            prediction = dashboard.prediction()
            page = dashboard.paginate_polymarket_markets(
                {"page": 1, "page_size": 10, "sort": "observed_at", "direction": "desc"}
            )

        self.assertTrue(overview["available"])
        self.assertLessEqual(len(overview["latest_activity"]), 8)
        self.assertLessEqual(len(overview["latest_candidates"]), 10)
        self.assertLessEqual(
            len(overview["forward_evidence"]["market_diagnostics"]), 100
        )
        self.assertLessEqual(len(prediction["markets"]), 1_000)
        self.assertEqual(page["page"], 1)
        self.assertEqual(page["page_size"], 10)
        self.assertEqual(len(page["items"]), 10)
        self.assertEqual(
            page["items"][0]["payload"]["source_type"],
            "FORWARD_COLLECTED",
        )
        self.assertEqual(crypto_provider.calls, [])
        self.assertEqual(prediction_provider.calls, [])
        self.assertEqual(_NoCredentialProbe.forbidden_calls, [])

    def test_worker_scans_eligible_candidates_without_persisted_ranking(self):
        store = self._store()
        _historical_dataset(store)
        service = CanaryService(store, clock=lambda: T0)
        candidate_ids = [f"scan-candidate-{index:02d}" for index in range(12)]
        for index, candidate_id in enumerate(candidate_ids):
            market_id = f"scan-market-{index:02d}"
            _seed_market(store, market_id, snapshot=False)
            _seed_candidate(
                store,
                service,
                candidate_id,
                target_market_id=market_id,
                executable=True,
                cluster="cluster-a" if index < 6 else f"cluster-{index:02d}",
                score=0.90 - index / 100,
            )
        service.enable_autonomous_micro_live()

        with store._lock:
            ranking_count_before = store.connection.execute(
                "SELECT COUNT(*) AS n FROM canary_rankings"
            ).fetchone()["n"]
        self.assertEqual(ranking_count_before, 0)

        empty_ranking = {
            "candidates_evaluated": len(candidate_ids),
            "eligible_count": len(candidate_ids),
            "rankable_count": 0,
            "rankings": [],
            "ranking_run_id": None,
        }
        worker = AutonomousCanaryWorker(
            store,
            clock=lambda: T0,
            venue_factory=lambda: AssertionError("venue must not be constructed"),
        )
        with patch(
            "axiom.auto_canary.CandidateCanaryRanker.evaluate_and_select",
            return_value=empty_ranking,
        ):
            result = worker.tick(now=T0)

        self.assertEqual(result["status"], "NO_SIGNAL")
        self.assertEqual(result["decision"], "NO_ACTIONABLE_SIGNAL")
        self.assertEqual(result["blocker"], "NO_ACTIONABLE_SIGNAL")
        self.assertNotEqual(result["blocker"], "NO_ELIGIBLE_RANKABLE_CANDIDATE")
        self.assertEqual(result["candidates_signal_checked"], worker._SCAN_CAP)
        self.assertEqual(result["candidates_no_signal"], worker._SCAN_CAP)
        self.assertEqual(
            result["signal_scan_reason_counts_json"]["NO_FORWARD_SNAPSHOT"],
            worker._SCAN_CAP,
        )
        self.assertEqual(result["signal_scan_checked_this_cycle"], worker._SCAN_CAP)
        self.assertEqual(result["signal_scan_remaining_this_cycle"], 2)
        self.assertEqual(len(result["signal_scan_checked_keys"]), worker._SCAN_CAP)
        checked_ids = {
            item["candidate_id"] for item in result["signal_scan_checked_keys"]
        }
        self.assertEqual(len(checked_ids), worker._SCAN_CAP)
        self.assertTrue(checked_ids.issubset(set(candidate_ids)))
        self.assertEqual(len(set(candidate_ids) - checked_ids), 2)
        cycle_id = result["signal_scan_cycle_id"]
        evaluations = service.list_signal_evaluations(cycle_id=cycle_id, limit=20)
        self.assertEqual(len(evaluations), worker._SCAN_CAP)
        self.assertEqual(
            {
                item["candidate_id"]: item["reason_code"]
                for item in evaluations
            },
            {candidate_id: "NO_FORWARD_SNAPSHOT" for candidate_id in checked_ids},
        )
        with store._lock:
            ranking_count_after = store.connection.execute(
                "SELECT COUNT(*) AS n FROM canary_rankings"
            ).fetchone()["n"]
            checked_count = store.connection.execute(
                "SELECT COUNT(*) AS n FROM canary_signal_scan_checked WHERE cycle_id=?",
                (cycle_id,),
            ).fetchone()["n"]
        self.assertEqual(ranking_count_after, 0)
        self.assertEqual(checked_count, worker._SCAN_CAP)

    def test_worker_surfaces_missing_executable_authority_after_binding_fence(self):
        store = self._store()
        _historical_dataset(store)
        service = CanaryService(store, clock=lambda: T0)
        candidate_id = "missing-executable-authority"
        market_id = "missing-authority-market"
        _seed_market(store, market_id, snapshot=True)
        payload = _seed_candidate(
            store,
            service,
            candidate_id,
            target_market_id=market_id,
            executable=True,
        )
        service.enable_autonomous_micro_live()

        original_evaluate = CanaryService.evaluate_signal

        def evaluate_then_remove_authority(current_service, *args, **kwargs):
            result = original_evaluate(current_service, *args, **kwargs)
            if result.get("reason_code") == "READY_SIGNAL":
                lifecycle = store.load_candidate_lifecycle(candidate_id)
                mutated = dict(lifecycle["payload"])
                mutated.pop("strategy_document", None)
                mutated.pop("model_document", None)
                with store._lock:
                    store.connection.execute(
                        "UPDATE candidate_lifecycle SET payload_json=? WHERE candidate_id=?",
                        (
                            json.dumps(mutated, sort_keys=True, separators=(",", ":")),
                            candidate_id,
                        ),
                    )
                    store.connection.commit()
            return result

        worker = AutonomousCanaryWorker(
            store,
            clock=lambda: T0,
            venue_factory=lambda: object(),
        )
        with patch.object(
            CanaryService,
            "evaluate_signal",
            new=evaluate_then_remove_authority,
        ), patch(
            "axiom.canary.CredentialStore.configured",
            return_value=True,
        ):
            result = worker.tick(now=T0)

        self.assertEqual(result["status"], "BLOCKED")
        self.assertEqual(
            result["blocker"], "CANDIDATE_EXECUTABLE_DOCUMENTS_UNAVAILABLE"
        )
        self.assertEqual(
            result["decision"], "CANDIDATE_EXECUTABLE_DOCUMENTS_UNAVAILABLE"
        )
        self.assertEqual(result["candidate_id"], candidate_id)
        state = service.authoritative_status()
        self.assertEqual(
            state["autonomous"]["blocker"],
            "CANDIDATE_EXECUTABLE_DOCUMENTS_UNAVAILABLE",
        )
        evaluation = service.list_signal_evaluations(
            candidate_id=candidate_id,
            limit=1,
        )[0]
        self.assertEqual(evaluation["reason_code"], "READY_SIGNAL")
        signal = service.get_signal(result["signal_id"])
        self.assertIsNotNone(signal)
        self.assertEqual(signal["status"], "NO_LONGER_VALID")
        self.assertEqual(signal["reason"], "CANDIDATE_EXECUTABLE_DOCUMENTS_UNAVAILABLE")
        blocked_evaluation = service.evaluate_signal(
            candidate_id,
            cycle_id="authority-missing-cycle",
        )
        self.assertEqual(
            blocked_evaluation["reason_code"],
            "COLLECTOR_CANDIDATE_HEALTH_BLOCKED",
        )
        self.assertEqual(
            blocked_evaluation["evidence"]["binding_reason"],
            "CANDIDATE_EXECUTABLE_DOCUMENTS_UNAVAILABLE",
        )
        self.assertEqual(payload["target_market_ids"], [market_id])

    def test_ready_signal_uses_fresh_candidate_bound_model_probability(self):
        store = self._store()
        _historical_dataset(store)
        service = CanaryService(store, clock=lambda: T0)
        candidate_id = "fresh-model-candidate"
        market_id = "fresh-model-market"
        _seed_market(store, market_id, snapshot=True, stale_model_probability=0.10)
        _seed_candidate(
            store,
            service,
            candidate_id,
            target_market_id=market_id,
            executable=True,
            model_probability=0.80,
        )

        result = service.evaluate_signal(candidate_id, cycle_id="fresh-model-cycle")

        self.assertEqual(result["reason_code"], "READY_SIGNAL")
        self.assertEqual(result["market_id"], market_id)
        signal = result["signal"]
        self.assertIsInstance(signal, dict)
        self.assertEqual(signal["status"], "READY")
        self.assertEqual(signal["source_snapshot_id"], f"{market_id}-snapshot-1")
        self.assertEqual(signal["evidence"]["model_probability"], 0.80)
        self.assertNotEqual(signal["evidence"]["model_probability"], 0.10)
        self.assertEqual(
            result["evidence"]["source_snapshot_id"],
            f"{market_id}-snapshot-1",
        )
        self.assertEqual(result["evidence"]["source_type"], "FORWARD_COLLECTED")
        self.assertEqual(result["evidence"]["current_execution_evidence"], "CURRENT_ORDER_BOOK")
        evaluation = service.list_signal_evaluations(
            candidate_id=candidate_id,
            cycle_id="fresh-model-cycle",
            limit=1,
        )[0]
        self.assertEqual(evaluation["reason_code"], "READY_SIGNAL")
        self.assertEqual(evaluation["signal"]["evidence"]["model_probability"], 0.80)
        self.assertEqual(
            evaluation["evidence"]["source_snapshot_id"],
            f"{market_id}-snapshot-1",
        )


if __name__ == "__main__":
    unittest.main()
