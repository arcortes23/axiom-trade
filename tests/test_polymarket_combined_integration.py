from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from axiom.auto_canary import AutonomousCanaryWorker
from axiom.canary import CanaryBlocked, CanaryService, CredentialStore
from axiom.canary_positions import CanaryPositionManager
from axiom.dashboard import DashboardData

from axiom.experiment_plan import normalize_market_scope
from axiom.legacy_scope import (
    freeze_canonical_scope_proposal,
    handoff_current_scope_resolution,
)
from axiom.market_scope import resolve_market_scope
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

class _QualifiedCanaryCredentials(CredentialStore):
    _VALUES = {
        "private_key": "fixture-private-key",
        "wallet_address": "0x0000000000000000000000000000000000000001",
    }

    def configured(self, **_kwargs: object) -> bool:
        return True

    def load(self, **_kwargs: object) -> dict[str, str]:
        return dict(self._VALUES)


class _PositionCanaryVenue:
    fixture_label = "SYNTHETIC_TEST_ONLY"
    def __init__(self, *, timestamp: datetime = T0) -> None:
        self.submissions: list[dict[str, object]] = []
        self.orders: dict[str, dict[str, object]] = {}
        self._entry_count = 0
        self.timestamp = timestamp

    def geoblock(self) -> dict[str, object]:
        return {"blocked": False, "close_only": False, "country": "ZZ"}

    def connectivity_check(self) -> bool:
        return True

    def balance(self) -> Decimal:
        return Decimal("100")

    def market_context(self, market_id: str, token_id: str) -> dict[str, object]:
        return {
            "market_id": market_id,
            "token_id": token_id,
            "asset_id": token_id,
            "market_version": "v2",
            "neg_risk": False,
            "accepting_orders": True,
            "min_order_size": "0.01",
            "tick_size": "0.01",
            "fee_bps": "10",
            "bids": [{"price": "0.22", "size": "100"}],
            "asks": [{"price": "0.21", "size": "100"}],
        }

    def submit_limit_order(
        self,
        *,
        token_id: str,
        side: str,
        price: Decimal,
        size: Decimal,
    ) -> dict[str, object]:
        is_exit = str(side).upper() == "SELL"
        order_id = (
            "exit-order"
            if is_exit
            else f"entry-order-{self._entry_count + 1}"
        )
        if not is_exit:
            self._entry_count += 1
        quantity = Decimal(str(size))
        fill_quantity = quantity if is_exit or self._entry_count > 1 else quantity / 2
        order = {
            "ok": True,
            "order_id": order_id,
            "status": "FILLED" if is_exit else "MATCHED",
            "side": str(side).upper(),
            "market_id": "synthetic-position-market",
            "token_id": token_id,
            "original_size": str(quantity),
            "fill_quantity": str(fill_quantity),
            "settlement_status": "SETTLED" if is_exit else None,
            "actual_average_price": str(price),
            "fees": str(fill_quantity * price * (Decimal("1") - price) * Decimal("0.001")),
            "fill_timestamp": self.timestamp.isoformat(),
        }
        self.submissions.append(
            {
                "token_id": token_id,
                "side": side,
                "price": str(price),
                "size": str(size),
            }
        )
        self.orders[order_id] = order
        return order

    def get_order(self, *, order_id: str) -> dict[str, object]:
        return dict(self.orders[order_id])

    def list_account_trades(self, *, order_id: str) -> list[dict[str, object]]:
        order = self.orders[order_id]
        return [
            {
                "id": f"{order_id}-trade",
                "order_id": order_id,
                "market_id": order["market_id"],
                "token_id": order["token_id"],
                "side": order["side"],
                "quantity": order["fill_quantity"],
                "price": order["actual_average_price"],
                "fee": order["fees"],
                "status": "CONFIRMED",
                "timestamp": order["fill_timestamp"],
            }
        ]


def _document_hash(value: object) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _historical_dataset(
    store: AxiomStore,
    *,
    dataset_id: str = "prediction-history",
    dataset_version: str = "v1",
    market_id: str | None = None,
) -> tuple[str, str]:
    row: dict[str, object] = {
        "timestamp": T0.isoformat(),
        "price": 0.5,
        "source_type": "HISTORICAL",
    }
    if market_id:
        row["market_id"] = market_id
    store.save_dataset(dataset_id, dataset_version, [row])
    catalog_metadata: dict[str, object] = {
        "provider": "polymarket",
        "source_type": "HISTORICAL",
        "research_quality": "PRICE_PROXY",
        "historical_order_book_available": False,
    }
    if market_id:
        catalog_metadata["market_id"] = market_id
    store.save_dataset_catalog(
        dataset_id,
        dataset_version,
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
        snapshot_id=f"{dataset_id}:{dataset_version}",
        metadata=catalog_metadata,
    )
    store.verify_dataset_integrity_attestation(
        dataset_id,
        dataset_version,
    )
    return dataset_id, dataset_version


def _candidate_payload(
    candidate_id: str,
    *,
    target_market_id: str | None,
    executable: bool,
    model_probability: float = 0.80,
    cluster: str = "cluster-a",
    score: float = 0.10,
    dataset_id: str = "prediction-history",
    dataset_version: str = "v1",
    dataset_attestation: dict[str, object] | None = None,
) -> dict[str, object]:
    parts = ("strategy-v1", "model-v1", "config-v1")
    payload: dict[str, object] = {
        "market_type": "prediction",
        "dataset_id": dataset_id,
        "dataset_version": dataset_version,
        "plan_hash": "sha256:fixture-plan",
        "dataset_selector": {
            "dataset_id": dataset_id,
            "dataset_version": dataset_version,
            "source_type": "HISTORICAL",
        },
        "dataset_attestation": dataset_attestation
        or {
            "dataset_id": dataset_id,
            "dataset_version": dataset_version,
            "status": "CURRENT",
            "policy_version": "v1",
            "attestation_hash": "fixture-attestation",
        },
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
        "exit_policy": {"type": "fixed_holding_period", "holding_period_seconds": 0},
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
        policy = normalize_market_scope(
            {
                "schema_version": "1",
                "mode": "EXACT_MARKETS",
                "instrument": "POLYMARKET",
                "categories": [],
                "market_ids": [target_market_id],
                "filters": {},
                "regime_restrictions": {},
                "provenance": "canonical",
            }
        )
        payload["target_market_ids"] = [target_market_id]
        payload["market_scope"] = policy.as_dict()
        payload["market_scope_hash"] = policy.scope_hash
        payload["market_scope_version"] = policy.scope_version
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
    if executable and target_market_id:
        resolution = resolve_market_scope(
            candidate_id,
            payload,
            [
                {
                    "market_id": target_market_id,
                    "condition_id": f"{target_market_id}-condition",
                    "yes_token_id": f"{target_market_id}-yes-token",
                    "no_token_id": f"{target_market_id}-no-token",
                    "source_type": "CURRENT",
                    "provider": "polymarket",
                    "instrument": "POLYMARKET",
                    "active": True,
                    "open": True,
                    "closed": False,
                    "settlement": "open",
                    "accepting_orders": True,
                    "order_book_available": True,
                    "expiry": (T0 + timedelta(hours=2)).isoformat(),
                }
            ],
            resolved_at=T0,
        )
        store.save_market_scope_resolution(resolution)
    return payload


def _seed_market(
    store: AxiomStore,
    market_id: str,
    *,
    snapshot: bool,
    stale_model_probability: float = 0.10,
    observed_at: datetime = T0,
    snapshot_id: str | None = None,
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
        observed_at=observed_at,
        source_type="FORWARD_COLLECTED",
    )
    if not snapshot:
        return
    order_book = {
        "timestamp": observed_at.isoformat(),
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
        snapshot_id or f"{market_id}-snapshot-1",
        market_id,
        observed_at,
        observed_at,
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
        service = CanaryService(
            store,
            credentials=_QualifiedCanaryCredentials(),
            clock=lambda: T0,
        )
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
        settings = service.settings.snapshot()
        service.enable_autonomous_micro_live(
            venue="polymarket",
            config_id=str(settings["config_id"]),
            expected_generation=int(settings["generation"]),
        )

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
        service = CanaryService(
            store,
            credentials=_QualifiedCanaryCredentials(),
            clock=lambda: T0,
        )
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
        settings = service.settings.snapshot()
        service.enable_autonomous_micro_live(
            venue="polymarket",
            config_id=str(settings["config_id"]),
            expected_generation=int(settings["generation"]),
        )

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

    @patch.dict("os.environ", {"AXIOM_EXECUTION_PROFILE": "production"})
    def test_dashboard_canary_accepts_qualified_partial_full_and_owned_exit(self):
        store = self._store()
        _historical_dataset(store)
        now = [T0]
        service = CanaryService(
            store,
            credentials=_QualifiedCanaryCredentials(),
            clock=lambda: now[0],
        )
        candidate_id = "synthetic-qualified-position-candidate"
        market_id = "synthetic-position-market"
        _seed_market(store, market_id, snapshot=True)
        _seed_candidate(
            store,
            service,
            candidate_id,
            target_market_id=market_id,
            executable=True,
        )
        venue = _PositionCanaryVenue(timestamp=now[0])
        settings = service.settings.snapshot()
        armed = service.arm(
            candidate_id,
            venue=venue,
            credentials_configured=True,
            config_id=str(settings["config_id"]),
            expected_generation=int(settings["generation"]),
        )
        self.assertEqual(armed["micro_live_canary"], "ARMED")
        positions = CanaryPositionManager(service)
        positions.reconcile_pending(venue, allow_test_venue=True)

        first_evaluation = service.evaluate_signal(
            candidate_id,
            cycle_id="synthetic-position-partial",
        )
        self.assertEqual(first_evaluation["reason_code"], "READY_SIGNAL")
        first_signal = first_evaluation.get("signal")
        self.assertIsInstance(first_signal, dict)
        persisted_first_signal = service.get_signal(str(first_signal["signal_id"]))
        self.assertIsNotNone(persisted_first_signal)
        assert persisted_first_signal is not None
        self.assertEqual(persisted_first_signal["status"], "READY")
        self.assertEqual(
            persisted_first_signal["evidence"]["source_type"],
            "FORWARD_COLLECTED",
        )
        self.assertEqual(
            persisted_first_signal["evidence"]["current_execution_evidence"],
            "CURRENT_ORDER_BOOK",
        )
        self.assertEqual(venue.fixture_label, "SYNTHETIC_TEST_ONLY")
        first_submission = service.submit_signal(
            str(first_signal["signal_id"]),
            venue=venue,
            allow_test_venue=True,
        )
        self.assertEqual(first_submission["execution_status"], "MATCHED", first_submission)
        provisional_event = store.connection.execute(
            "SELECT event_id,status,fill_quantity,settlement FROM canary_ledger "
            "WHERE signal_id=?",
            (first_signal["signal_id"],),
        ).fetchone()
        self.assertIsNotNone(provisional_event)
        assert provisional_event is not None
        self.assertEqual(provisional_event["status"], "MATCHED")
        self.assertIsNone(provisional_event["fill_quantity"])
        self.assertIsNone(provisional_event["settlement"])
        self.assertIsNone(
            store.connection.execute(
                "SELECT 1 FROM canary_position_lots WHERE event_id=?",
                (provisional_event["event_id"],),
            ).fetchone()
        )
        self.assertIsNone(
            store.connection.execute(
                "SELECT 1 FROM canary_risk_fills AS f "
                "JOIN canary_risk_reservations AS r "
                "ON r.reservation_id=f.reservation_id WHERE r.event_id=?",
                (provisional_event["event_id"],),
            ).fetchone()
        )
        entry_reconciliation = positions.reconcile_pending(
            venue,
            allow_test_venue=True,
        )
        self.assertEqual(entry_reconciliation["status"], "RECONCILED")
        self.assertEqual(entry_reconciliation["blocked"], 0)
        first_event = store.connection.execute(
            "SELECT event_id,fill_quantity,submitted_quantity,status "
            "FROM canary_ledger WHERE signal_id=?",
            (first_signal["signal_id"],),
        ).fetchone()
        self.assertIsNotNone(first_event)
        assert first_event is not None
        self.assertEqual(first_event["status"], "CONFIRMED")
        self.assertGreater(Decimal(first_event["fill_quantity"]), Decimal("0"))
        self.assertLess(
            Decimal(first_event["fill_quantity"]),
            Decimal(first_event["submitted_quantity"]),
        )
        self.assertEqual(
            store.connection.execute(
                "SELECT COUNT(*) FROM canary_risk_fills AS f "
                "JOIN canary_risk_reservations AS r "
                "ON r.reservation_id=f.reservation_id WHERE r.side='BUY'"
            ).fetchone()[0],
            1,
        )
        first_accounting = store.canary_risk_accounting(now=now[0])
        self.assertEqual(first_accounting["open_positions"], 1)
        self.assertEqual(first_accounting["open_market_ids"], [market_id])
        self.assertEqual(first_accounting["open_lot_slots"], 1)

        now[0] += timedelta(seconds=1)
        venue.timestamp = now[0]
        _seed_market(
            store,
            market_id,
            snapshot=True,
            observed_at=now[0],
            snapshot_id=f"{market_id}-snapshot-2",
        )
        second_evaluation = service.evaluate_signal(
            candidate_id,
            cycle_id="synthetic-position-full",
        )
        self.assertEqual(second_evaluation["reason_code"], "READY_SIGNAL")
        second_signal = second_evaluation.get("signal")
        self.assertIsInstance(second_signal, dict)
        second_submission = service.submit_signal(
            str(second_signal["signal_id"]),
            venue=venue,
            allow_test_venue=True,
        )
        self.assertEqual(second_submission["execution_status"], "MATCHED", second_submission)
        entry_reconciliation = positions.reconcile_pending(
            venue,
            allow_test_venue=True,
        )
        self.assertEqual(entry_reconciliation["status"], "RECONCILED")
        self.assertEqual(entry_reconciliation["blocked"], 0)
        second_event = store.connection.execute(
            "SELECT event_id,fill_quantity,submitted_quantity FROM canary_ledger "
            "WHERE signal_id=?",
            (second_signal["signal_id"],),
        ).fetchone()
        self.assertIsNotNone(second_event)
        assert second_event is not None
        self.assertEqual(
            Decimal(second_event["fill_quantity"]),
            Decimal(second_event["submitted_quantity"]),
        )
        buy_fills = store.connection.execute(
            "SELECT f.quantity,f.price,f.fee,f.cost "
            "FROM canary_risk_fills AS f "
            "JOIN canary_risk_reservations AS r "
            "ON r.reservation_id=f.reservation_id WHERE r.side='BUY' "
            "ORDER BY f.filled_at,f.fill_id"
        ).fetchall()
        self.assertEqual(len(buy_fills), 2)
        full_lot_before_exit = store.connection.execute(
            "SELECT quantity,cost_basis,fees,status FROM canary_position_lots "
            "WHERE event_id=?",
            (second_event["event_id"],),
        ).fetchone()
        self.assertIsNotNone(full_lot_before_exit)
        assert full_lot_before_exit is not None
        self.assertEqual(
            Decimal(full_lot_before_exit["cost_basis"]),
            Decimal(buy_fills[1]["cost"]),
        )
        full_lot_basis = Decimal(full_lot_before_exit["cost_basis"])
        second_accounting = store.canary_risk_accounting(now=now[0])
        self.assertEqual(second_accounting["open_positions"], 1)
        self.assertEqual(second_accounting["open_market_ids"], [market_id])
        self.assertEqual(second_accounting["open_lot_slots"], 2)
        full_reservation = store.connection.execute(
            "SELECT status,remaining_cost,released_at FROM canary_risk_reservations "
            "WHERE event_id=?",
            (second_event["event_id"],),
        ).fetchone()
        self.assertEqual(full_reservation["status"], "RELEASED")
        self.assertEqual(Decimal(full_reservation["remaining_cost"]), Decimal("0"))
        self.assertIsNotNone(full_reservation["released_at"])
        dashboard = DashboardData(store=store, control=service)
        canary_projection = dashboard.canary_data()
        self.assertGreaterEqual(
            canary_projection["canary"]["execution_event_count"],
            2,
        )
        self.assertEqual(
            canary_projection["canary"]["production_live_trading"],
            "DISABLED",
        )
        latest_signal = canary_projection["canary"]["latest_signal"]
        self.assertIsInstance(latest_signal, dict)
        self.assertNotEqual(latest_signal["status"], "NO_SIGNAL")
        pre_exit_accounting = store.canary_risk_accounting(now[0])

        # The minimum-sized partial fill is below the venue's exit minimum.
        # Exit the fully filled lot without consuming that separate inventory.
        position_id = "position:" + str(second_event["event_id"])
        exit_settings = service.settings.snapshot()
        exit_request = positions.submit_exit(
            position_id,
            venue,
            expected_generation=int(exit_settings["generation"]),
            config_id=str(exit_settings["config_id"]),
            allow_test_venue=True,
        )
        self.assertEqual(exit_request["status"], "FILLED", exit_request)
        exit_position_request = store.connection.execute(
            "SELECT request_id,position_id,reservation_id,event_id "
            "FROM canary_position_requests WHERE request_id=?",
            (exit_request["request_id"],),
        ).fetchone()
        self.assertIsNotNone(exit_position_request)
        assert exit_position_request is not None
        self.assertEqual(exit_position_request["request_id"], exit_request["request_id"])
        self.assertEqual(exit_position_request["position_id"], position_id)
        self.assertEqual(exit_position_request["reservation_id"], exit_request["reservation_id"])
        self.assertEqual(exit_position_request["event_id"], exit_request["reservation_id"])
        exit_reservation = store.connection.execute(
            "SELECT reservation_id,intent_id,event_id FROM canary_risk_reservations "
            "WHERE reservation_id=?",
            (exit_request["reservation_id"],),
        ).fetchone()
        self.assertIsNotNone(exit_reservation)
        assert exit_reservation is not None
        self.assertEqual(exit_reservation["reservation_id"], exit_request["reservation_id"])
        self.assertEqual(exit_reservation["intent_id"], exit_request["request_id"])
        self.assertEqual(exit_reservation["event_id"], exit_request["request_id"])
        pending_lot = store.connection.execute(
            "SELECT sold_quantity,pending_exit_quantity FROM canary_position_lots "
            "WHERE position_id=?",
            (position_id,),
        ).fetchone()
        self.assertEqual(Decimal(pending_lot["sold_quantity"]), Decimal("0"))
        self.assertEqual(
            Decimal(pending_lot["pending_exit_quantity"]),
            Decimal(second_event["fill_quantity"]),
        )
        reconciliation = positions.reconcile_pending(venue, allow_test_venue=True)
        self.assertEqual(reconciliation["status"], "RECONCILED")
        self.assertEqual(reconciliation["blocked"], 0)
        self.assertEqual(reconciliation["requests"][0]["status"], "SETTLED")
        lot = store.connection.execute(
            "SELECT sold_quantity,pending_exit_quantity,status "
            "FROM canary_position_lots WHERE position_id=?",
            (position_id,),
        ).fetchone()
        self.assertIsNotNone(lot)
        assert lot is not None
        self.assertEqual(Decimal(lot["pending_exit_quantity"]), Decimal("0"))
        self.assertEqual(lot["status"], "CLOSED")
        self.assertEqual(Decimal(lot["sold_quantity"]), Decimal(second_event["fill_quantity"]))
        partial_lot = store.connection.execute(
            "SELECT quantity,sold_quantity,cost_basis FROM canary_position_lots "
            "WHERE event_id=?",
            (first_event["event_id"],),
        ).fetchone()
        self.assertIsNotNone(partial_lot)
        assert partial_lot is not None
        remaining_quantity = (
            Decimal(partial_lot["quantity"]) - Decimal(partial_lot["sold_quantity"])
        )
        remaining_basis = (
            Decimal(partial_lot["cost_basis"])
            * remaining_quantity
            / Decimal(partial_lot["quantity"])
        )
        exited_quantity = Decimal(lot["sold_quantity"])
        released_basis = full_lot_basis * exited_quantity / Decimal(
            second_event["fill_quantity"]
        )
        expected_remaining = Decimal("0.0010508295")
        exit_fill = store.connection.execute(
            "SELECT f.fill_id,f.reservation_id,f.detail_json,f.quantity,f.price,f.fee,f.cost "
            "FROM canary_risk_fills AS f "
            "JOIN canary_risk_reservations AS r "
            "ON r.reservation_id=f.reservation_id "
            "WHERE r.side='SELL' AND r.reservation_id=?",
            (exit_request["reservation_id"],),
        ).fetchone()
        self.assertIsNotNone(exit_fill)
        assert exit_fill is not None
        self.assertEqual(exit_fill["fill_id"], "exit-order-trade")
        self.assertEqual(exit_fill["reservation_id"], exit_request["reservation_id"])
        exit_detail = json.loads(exit_fill["detail_json"])
        self.assertEqual(exit_detail["position_id"], position_id)
        self.assertEqual(exit_detail["request_id"], exit_request["request_id"])
        expected_exit_pnl = (
            Decimal(exit_fill["quantity"]) * Decimal(exit_fill["price"])
            - Decimal(exit_fill["fee"])
            - Decimal(full_lot_before_exit["cost_basis"])
        )
        settled_lot = store.connection.execute(
            "SELECT quantity,sold_quantity,cost_basis,gross_proceeds,exit_fees,"
            "realized_pnl,status FROM canary_position_lots WHERE position_id=?",
            (position_id,),
        ).fetchone()
        self.assertIsNotNone(settled_lot)
        assert settled_lot is not None
        self.assertEqual(
            Decimal(settled_lot["gross_proceeds"]),
            Decimal(exit_fill["quantity"]) * Decimal(exit_fill["price"]),
        )
        self.assertEqual(Decimal(settled_lot["exit_fees"]), Decimal(exit_fill["fee"]))
        self.assertEqual(Decimal(settled_lot["realized_pnl"]), expected_exit_pnl)
        self.assertEqual(
            store.canary_risk_accounting(now=now[0])["today_realized_pnl_usd"],
            format(expected_exit_pnl, "f"),
        )
        partial_lot_after_exit = store.connection.execute(
            "SELECT quantity,sold_quantity,cost_basis FROM canary_position_lots "
            "WHERE event_id=?",
            (first_event["event_id"],),
        ).fetchone()
        self.assertIsNotNone(partial_lot_after_exit)
        assert partial_lot_after_exit is not None
        expected_open_cost = Decimal(partial_lot_after_exit["cost_basis"]) * (
            Decimal(partial_lot_after_exit["quantity"])
            - Decimal(partial_lot_after_exit["sold_quantity"])
        ) / Decimal(partial_lot_after_exit["quantity"])
        pending_buy_cost = sum(
            (
                Decimal(row["remaining_cost"])
                for row in store.connection.execute(
                    "SELECT remaining_cost FROM canary_risk_reservations "
                    "WHERE side='BUY' AND status IN "
                    "('HELD','RESERVED','SUBMITTING','ACKNOWLEDGED',"
                    "'PARTIALLY_FILLED','PARTIAL','OPEN','SUBMITTED','UNKNOWN')"
                ).fetchall()
            ),
            Decimal("0"),
        )
        final_accounting = store.canary_risk_accounting(now=now[0])
        self.assertEqual(
            Decimal(final_accounting["today_realized_pnl_usd"]),
            Decimal(exit_detail["realized_pnl_usd"]),
        )
        self.assertEqual(
            Decimal(final_accounting["aggregate_open_cost_usd"]),
            Decimal(pre_exit_accounting["aggregate_open_cost_usd"]) - released_basis,
        )
        self.assertEqual(
            Decimal(final_accounting["aggregate_exposure_usd"]),
            Decimal(pre_exit_accounting["aggregate_exposure_usd"]) - released_basis,
        )
        self.assertEqual(remaining_basis, expected_remaining)
        self.assertEqual(
            Decimal(final_accounting["aggregate_open_cost_usd"]),
            expected_remaining,
        )
        repeated_accounting = store.canary_risk_accounting(now[0])
        self.assertEqual(
            repeated_accounting["aggregate_open_cost_usd"],
            final_accounting["aggregate_open_cost_usd"],
        )
        self.assertEqual(
            repeated_accounting["aggregate_exposure_usd"],
            final_accounting["aggregate_exposure_usd"],
        )
        self.assertEqual(
            repeated_accounting["today_realized_pnl_usd"],
            final_accounting["today_realized_pnl_usd"],
        )
        self.assertEqual(
            Decimal(final_accounting["aggregate_open_cost_usd"]),
            expected_open_cost,
        )
        self.assertEqual(
            Decimal(final_accounting["aggregate_exposure_usd"]),
            expected_open_cost + pending_buy_cost,
        )
        self.assertEqual(final_accounting["open_positions"], 1)
        self.assertEqual(final_accounting["open_market_ids"], [market_id])
        self.assertEqual(
            store.connection.execute("SELECT COUNT(*) FROM canary_risk_fills").fetchone()[0],
            3,
        )
        self.assertEqual(
            store.connection.execute(
                "SELECT COUNT(*) FROM canary_position_requests WHERE status='SETTLED'"
            ).fetchone()[0],
            1,
        )
        self.assertEqual(len(venue.submissions), 3)
        self.assertEqual(
            {str(item["side"]).upper() for item in venue.submissions},
            {"BUY", "SELL"},
        )
        # The fixture remains paper-disabled and every transport call above is
        # the explicit synthetic venue; no real execution path is activated.
        self.assertFalse(service.status()["live_execution"])
        self.assertEqual(service.status()["production_live_trading"], "DISABLED")

        now[0] += timedelta(seconds=1)
        _seed_market(
            store,
            market_id,
            snapshot=True,
            observed_at=now[0],
            snapshot_id=f"{market_id}-snapshot-3",
        )
        disarmed_signal = service.evaluate_signal(
            candidate_id,
            cycle_id="synthetic-position-disarmed",
        )["signal"]
        self.assertIsInstance(disarmed_signal, dict)
        service.disarm()
        with self.assertRaisesRegex(CanaryBlocked, "CANARY_NOT_ARMED"):
            service.submit_signal(
                str(disarmed_signal["signal_id"]),
                venue=venue,
                allow_test_venue=True,
            )
        self.assertEqual(len(venue.submissions), 3)
        rearm_settings = service.settings.snapshot()
        rearmed = service.arm(
            candidate_id,
            venue=venue,
            credentials_configured=True,
            config_id=str(rearm_settings["config_id"]),
            expected_generation=int(rearm_settings["generation"]),
        )
        self.assertEqual(rearmed["micro_live_canary"], "ARMED")
        self.assertFalse(service.status()["live_execution"])
        self.assertEqual(len(venue.submissions), 3)

        now[0] += timedelta(seconds=1)
        _seed_market(
            store,
            market_id,
            snapshot=True,
            observed_at=now[0],
            snapshot_id=f"{market_id}-snapshot-4",
        )
        killed_signal = service.evaluate_signal(
            candidate_id,
            cycle_id="synthetic-position-killed",
        )["signal"]
        self.assertIsInstance(killed_signal, dict)
        service.kill()
        self.assertEqual(service.status()["micro_live_canary"], "KILLED")
        with self.assertRaisesRegex(CanaryBlocked, "CANARY_NOT_ARMED"):
            service.submit_signal(
                str(killed_signal["signal_id"]),
                venue=venue,
                allow_test_venue=True,
            )
        self.assertEqual(len(venue.submissions), 3)
        self.assertFalse(service.status()["live_execution"])

    def test_canonical_proposal_research_freeze_resolution_fresh_inputs_decide(self):
        store = self._store()
        market_id = "canonical-chain-market"
        canonical_identity = _document_hash(
            [{"timestamp": T0.isoformat(), "price": 0.5, "market_id": market_id}]
        )
        dataset_id, dataset_version = _historical_dataset(
            store,
            dataset_id=f"prediction:{market_id}",
            dataset_version=canonical_identity,
            market_id=market_id,
        )
        dataset_attestation = store.verify_dataset_integrity_attestation(
            dataset_id,
            dataset_version,
        )
        candidate_id = "canonical-chain-predecessor"
        predecessor_hash = "canonical-predecessor-frozen"
        proposal_document = {
            "proposal_id": "canonical-chain-proposal",
            "candidate_id": candidate_id,
            "predecessor_frozen_hash": predecessor_hash,
            "statement": "A bounded current market probability edge is testable.",
            "source": "canonical integration fixture",
            "tests": ["chronological train-validation-holdout"],
            "dataset_version": dataset_version,
            "time_split": "train-validation-holdout",
            "paper_only": True,
            "market_scope": {
                "schema_version": "1",
                "mode": "EXACT_MARKETS",
                "instrument": "POLYMARKET",
                "categories": [],
                "market_ids": [market_id],
                "filters": {},
                "regime_restrictions": {},
                "provenance": "canonical",
            },
            "experiment_plan": {
                "market_type": "prediction",
                "dataset_id": dataset_id,
                "dataset_version": dataset_version,
                "dataset_selector": {
                    "dataset_id": dataset_id,
                    "dataset_version": dataset_version,
                    "source_type": "HISTORICAL",
                },
                "market_scope": {
                    "schema_version": "1",
                    "mode": "EXACT_MARKETS",
                    "instrument": "POLYMARKET",
                    "categories": [],
                    "market_ids": [market_id],
                    "filters": {},
                    "regime_restrictions": {},
                    "provenance": "canonical",
                },
            },
        }
        # The proposal enters the ordinary durable research queue before the
        # canonical scope is frozen; the completion record is the research
        # evidence consumed by the successor proposal.
        from axiom.research_bus import DurableResearchBus

        bus = DurableResearchBus(store, source="integration", author="test")
        queued = bus.submit_proposal(
            proposal_document,
            dedupe_key="canonical-chain-proposal",
            available_at=T0,
        )
        claimed = bus.claim("canonical-chain-researcher", now=T0)
        self.assertIsNotNone(claimed)
        assert claimed is not None
        research = {
            "proposal_id": proposal_document["proposal_id"]
            if "proposal_id" in proposal_document
            else queued.payload.get("proposal_id"),
            "status": "HISTORICAL_RESEARCH_COMPLETE",
            "metrics": {
                "sample_count": 100,
                "trade_count": 50,
                "expectancy": 0.10,
            },
        }
        completed = bus.complete(
            claimed.item_id,
            result=research,
            worker="canonical-chain-researcher",
            now=T0,
        )
        self.assertEqual(completed.status.value, "COMPLETED")

        frozen = freeze_canonical_scope_proposal(
            {**proposal_document, "research": research},
            assumptions={"input_age_seconds": 30, "execution": "paper_only"},
            source_candidate_id=candidate_id,
            source_frozen_hash=predecessor_hash,
        )
        self.assertEqual(frozen["canonical_scope_provenance"], "canonical")
        self.assertEqual(frozen["assumptions"]["input_age_seconds"], 30)
        self.assertEqual(frozen["predecessor_candidate_id"], candidate_id)
        current_resolution = {
            "market_id": market_id,
            "question": f"Will {market_id} resolve yes?",
            "settlement": "open",
            "outcome": None,
            "expiry": (T0 + timedelta(hours=2)).isoformat(),
            "token_ids": {
                "yes": f"{market_id}-yes-token",
                "no": f"{market_id}-no-token",
            },
        }
        handed_off = handoff_current_scope_resolution(frozen, current_resolution)
        self.assertEqual(handed_off["current_resolution"], current_resolution)
        self.assertTrue(str(handed_off["resolution_handoff_hash"]).startswith("sha256:"))

        _seed_market(store, market_id, snapshot=True, stale_model_probability=0.05)
        service = CanaryService(store, clock=lambda: T0)
        payload = _candidate_payload(
            frozen["proposal_id"],
            target_market_id=market_id,
            executable=True,
            model_probability=0.80,
            dataset_id=dataset_id,
            dataset_version=dataset_version,
            dataset_attestation=dict(dataset_attestation),
        )
        payload.update(
            {
                "proposal_id": frozen["proposal_id"],
                "research": research,
                "current_resolution": handed_off["current_resolution"],
                "resolution_fields": handed_off["resolution_fields"],
                "resolution_handoff_hash": handed_off["resolution_handoff_hash"],
            }
        )
        store.save_candidate_lifecycle(
            frozen["proposal_id"],
            "IDEA",
            payload,
            timestamp=T0,
        )
        store.save_candidate_lifecycle(
            frozen["proposal_id"],
            "FROZEN",
            payload,
            from_stage="IDEA",
            timestamp=T0,
        )
        service.mark_eligible(frozen["proposal_id"], publish_readiness=False)
        resolution = resolve_market_scope(
            frozen["proposal_id"],
            payload,
            [
                {
                    **current_resolution,
                    "condition_id": f"{market_id}-condition",
                    "yes_token_id": f"{market_id}-yes-token",
                    "no_token_id": f"{market_id}-no-token",
                    "source_type": "CURRENT",
                    "provider": "polymarket",
                    "instrument": "POLYMARKET",
                    "active": True,
                    "open": True,
                    "closed": False,
                    "accepting_orders": True,
                    "order_book_available": True,
                }
            ],
            resolved_at=T0,
        )
        store.save_market_scope_resolution(resolution)

        decision = service.evaluate_signal(
            frozen["proposal_id"],
            cycle_id="canonical-chain-fresh-inputs",
        )
        self.assertEqual(decision["reason_code"], "READY_SIGNAL")
        self.assertEqual(decision["market_id"], market_id)
        self.assertEqual(decision["evidence"]["scope_resolution_status"], "MATCHED")
        self.assertEqual(decision["evidence"]["source_type"], "FORWARD_COLLECTED")
        self.assertEqual(decision["signal"]["evidence"]["model_probability"], 0.80)
        self.assertNotEqual(decision["signal"]["evidence"]["model_probability"], 0.05)

if __name__ == "__main__":
    unittest.main()
