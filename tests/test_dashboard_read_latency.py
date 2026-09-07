from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import urlopen

from axiom.canary import AUTONOMOUS_MICRO_LIVE, CanaryBlocked, CanaryService
from axiom.dashboard import DashboardData, DashboardServer
from axiom.operator import CANARY_CONNECTIVITY_CONFIG_KEY, OperatorControlPlane
from axiom.ranker import CandidateCanaryRanker
from axiom.storage import AxiomStore


UTC = timezone.utc
T0 = datetime.now(UTC)
CANDIDATE_COUNT = 28
HISTORICAL_MARKET_COUNT = 4
HISTORICAL_ROWS_PER_MARKET = 8
SECRET_VALUES = (
    "latency-private-key-sentinel",
    "latency-wallet-address-sentinel",
    "latency-relayer-key-sentinel",
)


class _NeverUsedVenue:
    def geoblock(self):
        raise AssertionError("execution venue must not be used after failed validation")

    def market_context(self, *_args):
        raise AssertionError("execution venue must not be used after failed validation")

    def balance(self):
        raise AssertionError("execution venue must not be used after failed validation")

    def submit_limit_order(self, **_kwargs):
        raise AssertionError("execution venue must not be used after failed validation")


class DashboardReadLatencyFixture(unittest.TestCase):
    """Persist a representative canary state without using runtime data."""

    def setUp(self) -> None:
        global T0
        T0 = datetime.now(UTC)
        self._temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self._temporary_directory.cleanup)
        self.database_path = Path(self._temporary_directory.name) / "dashboard-latency.sqlite3"
        self.store = AxiomStore(str(self.database_path))
        self.addCleanup(self.store.close)
        self._seed_historical_aggregate()
        self._seed_candidates()
        self.service = CanaryService(self.store, clock=lambda: T0, initialize=True)
        ranker = CandidateCanaryRanker(self.store, service=self.service, clock=lambda: T0)
        ranking = ranker.evaluate_and_select(T0)
        self.assertEqual(ranking["selected_candidate"], "candidate-00")
        self.assertEqual(ranking["eligible_count"], CANDIDATE_COUNT)
        self.assertEqual(ranking["rankable_count"], CANDIDATE_COUNT)
        self.assertEqual(len(ranker.rankings(limit=1000)), CANDIDATE_COUNT)

        # Publish an explicit fixture snapshot after durable control and
        # autonomous-state setup so endpoint assertions have one stable reason.
        self.service.enable_autonomous_micro_live()
        self.service.record_autonomous_decision(
            next_decision="WAITING_FOR_NEXT_TICK",
            blocker=None,
            worker_status="IDLE",
            timestamp=T0,
        )
        published = self.service.publish_readiness_snapshot(reason="FIXTURE_SEED")
        self.assertIsInstance(published, dict)
        # Deliberately place secret-shaped values in an untrusted persisted
        # config.  Public projections must retain no values from this mapping.
        self.store.set_operator_config(
            CANARY_CONNECTIVITY_CONFIG_KEY,
            {
                "private_key": SECRET_VALUES[0],
                "wallet_address": SECRET_VALUES[1],
                "relayer_api_key": SECRET_VALUES[2],
            },
        )

        self.dashboard_store = AxiomStore(str(self.database_path))
        self.addCleanup(self.dashboard_store.close)
        self.control = OperatorControlPlane(self.dashboard_store)
        self.server = DashboardServer(
            port=0,
            data=DashboardData(store=self.dashboard_store, control=self.control),
        ).start()
        self.addCleanup(self.server.stop)

    def _seed_historical_aggregate(self) -> None:
        all_records: list[dict[str, object]] = []
        market_versions: list[dict[str, object]] = []
        for market_index in range(HISTORICAL_MARKET_COUNT):
            market_id = f"market-{market_index:02d}"
            dataset_id = f"prediction:{market_id}"
            records: list[dict[str, object]] = []
            for row_index in range(HISTORICAL_ROWS_PER_MARKET):
                stamp = T0 + timedelta(minutes=market_index * HISTORICAL_ROWS_PER_MARKET + row_index)
                records.append(
                    {
                        "market_id": market_id,
                        "source_timestamp": stamp.isoformat(),
                        "timestamp": stamp.isoformat(),
                        "price": 0.35 + row_index / 100,
                        "token_id": f"{market_id}-yes",
                        "source_type": "HISTORICAL",
                    }
                )
            metadata = {
                "provider": "fixture-provider",
                "source_type": "HISTORICAL",
                "market_id": market_id,
                "question": f"Will fixture market {market_index:02d} resolve yes?",
                "token_ids": {"yes": f"{market_id}-yes", "no": f"{market_id}-no"},
            }
            self.store.save_dataset(
                dataset_id,
                "v1",
                records,
                metadata=metadata,
                quality="PRICE_PROXY",
            )
            self.store.save_dataset_catalog(
                dataset_id,
                "v1",
                provider="fixture-provider",
                instrument="POLYMARKET",
                market_type="prediction",
                timeframe="event",
                start_timestamp=T0 + timedelta(minutes=market_index * HISTORICAL_ROWS_PER_MARKET),
                end_timestamp=T0
                + timedelta(minutes=market_index * HISTORICAL_ROWS_PER_MARKET + HISTORICAL_ROWS_PER_MARKET - 1),
                row_count=len(records),
                completeness=1.0,
                quality="PRICE_PROXY",
                source_type="HISTORICAL",
                snapshot_id=f"{dataset_id}:v1",
                metadata=metadata,
                created_at=T0,
                updated_at=T0,
            )
            market_versions.append(
                {
                    "market_id": market_id,
                    "dataset_id": dataset_id,
                    "version": "v1",
                    "records": len(records),
                }
            )
            all_records.extend(records)

        aggregate_metadata = {
            "provider": "fixture-provider",
            "source_type": "HISTORICAL",
            "research_quality": "PRICE_PROXY",
            "historical_order_book_available": False,
            "market_versions": market_versions,
        }
        self.store.save_dataset_catalog(
            "Polymarket-historical",
            "v1",
            provider="fixture-provider",
            instrument="POLYMARKET",
            market_type="prediction",
            timeframe="event",
            start_timestamp=T0,
            end_timestamp=T0 + timedelta(minutes=len(all_records) - 1),
            row_count=len(all_records),
            completeness=1.0,
            quality="PRICE_PROXY",
            source_type="HISTORICAL",
            snapshot_id="Polymarket-historical:v1",
            metadata=aggregate_metadata,
            created_at=T0,
            updated_at=T0,
        )

    def _candidate_payload(self, candidate_id: str, index: int) -> dict[str, object]:
        strategy_document = {
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
        model_document = {"probability": 0.80}
        strategy_hash = "sha256:" + hashlib.sha256(
            json.dumps(strategy_document, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        model_hash = "sha256:" + hashlib.sha256(
            json.dumps(model_document, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        config_hash = f"config-hash-{index:02d}"
        return {
            "candidate_id": candidate_id,
            "strategy_id": f"strategy-{index:02d}",
            "strategy_document": strategy_document,
            "model_document": model_document,
            "experiment_family": f"latency-family-{index:02d}",
            "market_type": "prediction",
            "source_type": "HISTORICAL",
            "dataset_id": "Polymarket-historical",
            "dataset_version": "v1",
            "dataset_provenance": {
                "dataset_id": "Polymarket-historical",
                "dataset_version": "v1",
                "source_type": "HISTORICAL",
                "time_split": "train-validation-holdout",
            },
            "lineage": [candidate_id],
            "mutation_cluster": f"latency-cluster-{index:02d}",
            "schema_validated": True,
            "historical_backtest_passed": True,
            "validation_passed": True,
            "robustness_passed": True,
            "data_quality_passed": True,
            "data_quality": "PRICE_PROXY",
            "frozen": True,
            "holdout_used": False,
            "strategy_hash": strategy_hash,
            "model_hash": model_hash,
            "config_hash": config_hash,
            "frozen_hash": hashlib.sha256(
                "|".join((strategy_hash, model_hash, config_hash)).encode("utf-8")
            ).hexdigest(),
            "validation_expectancy": 0.40 if index == 0 else 0.10,
            "validation_confidence_lower_bound": 0.30 if index == 0 else 0.05,
            "validation_stability": 0.90,
            "validation_calibration": 0.90,
            "validation_sample_count": 100,
            "validation_trade_count": 50,
            "validation_execution_quality": 0.90,
            "minimum_sample_check": {
                "passed": True,
                "count": 100,
                "trades": 50,
                "min_observations": 30,
                "min_trades": 10,
                "checks": {"observations": True, "trades": True},
            },
            "experiment_plan": {
                "min_independent_samples": 30,
                "min_trades": 10,
            },
        }

    def _seed_candidates(self) -> None:
        # CanaryService.mark_eligible is intentionally used here so every row
        # has the same authoritative binding shape as a production writer.
        service = CanaryService(self.store, clock=lambda: T0, initialize=True)
        for index in range(CANDIDATE_COUNT):
            candidate_id = f"candidate-{index:02d}"
            payload = self._candidate_payload(candidate_id, index)
            self.store.save_candidate_lifecycle(candidate_id, "IDEA", payload, timestamp=T0)
            self.store.save_candidate_lifecycle(
                candidate_id,
                "PAPER_FORWARD",
                payload,
                from_stage="IDEA",
                reason="latency fixture",
                timestamp=T0,
            )
            service.mark_eligible(candidate_id)

    def _request(
        self,
        server: DashboardServer,
        path: str,
        **params: object,
    ) -> tuple[int, object, str]:
        assert server.url is not None
        query = urlencode({key: value for key, value in params.items() if value is not None})
        url = f"{server.url}/{path}"
        if query:
            url += "?" + query
        try:
            with urlopen(url, timeout=3) as response:
                body = response.read().decode("utf-8")
                return response.status, json.loads(body), body
        except HTTPError as error:
            body = error.read().decode("utf-8")
            try:
                payload: object = json.loads(body)
            except json.JSONDecodeError:
                payload = body
            return error.code, payload, body

    def _new_dashboard_server(self) -> tuple[DashboardServer, AxiomStore]:
        reader = AxiomStore(str(self.database_path))
        self.addCleanup(reader.close)
        control = OperatorControlPlane(reader)
        server = DashboardServer(
            port=0,
            data=DashboardData(store=reader, control=control),
        ).start()
        self.addCleanup(server.stop)
        return server, reader

    @contextmanager
    def _forbid_dataset_reads(self):
        with patch.object(
            AxiomStore,
            "load_dataset",
            side_effect=AssertionError("dashboard loaded a dataset"),
        ), patch.object(
            AxiomStore,
            "load_dataset_catalog",
            side_effect=AssertionError("dashboard loaded a dataset catalog"),
        ):
            yield

    @contextmanager
    def _forbid_eligibility_mutations(self):
        with patch.object(
            CanaryService,
            "mark_eligible",
            side_effect=AssertionError("dashboard mutated eligibility"),
        ), patch.object(
            CanaryService,
            "invalidate_eligibility",
            side_effect=AssertionError("dashboard mutated eligibility"),
        ):
            yield

    def test_cold_startup_canary_and_overview_stay_under_one_second(self) -> None:
        for endpoint in ("api/v2/canary", "api/v2/overview-summary"):
            with self.subTest(endpoint=endpoint):
                server, _reader = self._new_dashboard_server()
                started = time.perf_counter()
                status, payload, _body = self._request(server, endpoint)
                elapsed = time.perf_counter() - started
                self.assertEqual(status, 200)
                self.assertIsInstance(payload, dict)
                self.assertLess(elapsed, 1.0)
                assert isinstance(payload, dict)
                if endpoint.endswith("/canary"):
                    canary = payload["canary"]
                    self.assertEqual(canary["eligibility_raw_count"], CANDIDATE_COUNT)
                    self.assertEqual(canary["eligible_count"], CANDIDATE_COUNT)
                    self.assertEqual(canary["rankable_raw_count"], CANDIDATE_COUNT)
                    self.assertEqual(canary["rankable_count"], CANDIDATE_COUNT)
                else:
                    candidate_status = payload["candidate_status"]
                    self.assertEqual(candidate_status["canary_eligible"], CANDIDATE_COUNT)
                    self.assertEqual(candidate_status["rankable"], CANDIDATE_COUNT)

    def test_dashboard_gets_never_load_datasets(self) -> None:
        with self._forbid_dataset_reads():
            for endpoint in (
                "api/v2/overview-summary",
                "api/v2/canary",
                "api/v2/candidates",
            ):
                with self.subTest(endpoint=endpoint):
                    status, _payload, _body = self._request(
                        self.server,
                        endpoint,
                        page=1,
                        page_size=100,
                    )
                    self.assertEqual(status, 200)

    def test_dashboard_gets_never_mutate_eligibility(self) -> None:
        before = self.service.readiness_snapshot()
        with self._forbid_eligibility_mutations():
            for endpoint in (
                "api/v2/overview-summary",
                "api/v2/canary",
                "api/v2/candidates",
            ):
                with self.subTest(endpoint=endpoint):
                    status, _payload, _body = self._request(
                        self.server,
                        endpoint,
                        page=1,
                        page_size=100,
                    )
                    self.assertEqual(status, 200)
        self.assertEqual(self.service.readiness_snapshot(), before)

    def test_dashboard_sql_call_count_is_bounded_for_28_candidates(self) -> None:
        reader = AxiomStore(str(self.database_path))
        self.addCleanup(reader.close)
        server = DashboardServer(
            port=0,
            data=DashboardData(store=reader, control=OperatorControlPlane(reader)),
        ).start()
        self.addCleanup(server.stop)
        statements: list[str] = []
        reader.connection.set_trace_callback(statements.append)
        status, payload, _body = self._request(server, "api/v2/canary")
        self.assertEqual(status, 200)
        self.assertIsInstance(payload, dict)
        self.assertLessEqual(len(statements), 96)

    def test_readiness_snapshot_current_stale_and_timestamp_semantics(self) -> None:
        initial = self.service.readiness_snapshot()
        self.assertEqual(initial["readiness_snapshot_status"], "CURRENT")
        self.assertFalse(initial["readiness_snapshot_stale"])
        self.assertEqual(initial["readiness_snapshot_reason"], "FIXTURE_SEED")
        initial_updated_at = initial["readiness_snapshot_updated_at"]
        self.assertIsInstance(initial_updated_at, str)
        parsed_initial = datetime.fromisoformat(initial_updated_at)
        self.assertIsNotNone(parsed_initial.tzinfo)

        status, payload, _body = self._request(self.server, "api/v2/canary")
        self.assertEqual(status, 200)
        self.assertIsInstance(payload, dict)
        assert isinstance(payload, dict)
        canary = payload["canary"]
        self.assertEqual(canary["readiness_snapshot_status"], "CURRENT")
        self.assertFalse(canary["readiness_snapshot_stale"])
        self.assertEqual(canary["readiness_snapshot_reason"], "FIXTURE_SEED")
        self.assertEqual(canary["readiness_snapshot_updated_at"], initial_updated_at)

        record = self.store.load_candidate_lifecycle("candidate-00")
        assert isinstance(record, dict)
        payload_row = dict(record["payload"])
        payload_row["lifecycle_marker"] = "fixture-update"
        self.store.save_candidate_lifecycle(
            "candidate-00",
            "PAPER_FORWARD",
            payload_row,
            from_stage="PAPER_FORWARD",
            reason="fixture lifecycle update",
            timestamp=T0 + timedelta(minutes=1),
        )

        stale = self.service.readiness_snapshot()
        self.assertEqual(stale["readiness_snapshot_status"], "STALE")
        self.assertTrue(stale["readiness_snapshot_stale"])
        self.assertIsInstance(stale["readiness_snapshot_reason"], str)
        self.assertNotEqual(stale["readiness_snapshot_reason"], "")
        self.assertEqual(stale["readiness_snapshot_updated_at"], initial_updated_at)
        status, stale_payload, _body = self._request(self.server, "api/v2/canary")
        self.assertEqual(status, 200)
        self.assertIsInstance(stale_payload, dict)
        assert isinstance(stale_payload, dict)
        stale_canary = stale_payload["canary"]
        self.assertEqual(stale_canary["readiness_snapshot_status"], "STALE")
        self.assertTrue(stale_canary["readiness_snapshot_stale"])
        self.assertEqual(stale_canary["readiness_snapshot_updated_at"], initial_updated_at)
        self.assertIsNone(stale_canary["selected_candidate"])
        self.assertEqual(stale_canary["last_selected_candidate"], "candidate-00")
        reranked = CandidateCanaryRanker(
            self.store,
            service=self.service,
            clock=lambda: T0 + timedelta(minutes=1),
        ).evaluate_and_select(T0 + timedelta(minutes=1))
        self.assertEqual(reranked["selected_candidate"], "candidate-00")
        republished = self.service.publish_readiness_snapshot(
            reason="AFTER_LIFECYCLE_REEVALUATION"
        )
        self.assertEqual(republished["readiness_snapshot_status"], "CURRENT")
        self.assertFalse(republished["readiness_snapshot_stale"])
        self.assertEqual(
            republished["readiness_snapshot_reason"],
            "AFTER_LIFECYCLE_REEVALUATION",
        )
        self.assertIsInstance(republished["readiness_snapshot_updated_at"], str)
        self.assertIsNotNone(
            datetime.fromisoformat(republished["readiness_snapshot_updated_at"]).tzinfo
        )

        status, current_payload, _body = self._request(self.server, "api/v2/canary")
        self.assertEqual(status, 200)
        assert isinstance(current_payload, dict)
        current_canary = current_payload["canary"]
        self.assertEqual(current_canary["readiness_snapshot_status"], "CURRENT")
        self.assertEqual(current_canary["selected_candidate"], "candidate-00")

    def test_ranking_and_lifecycle_changes_refresh_projection(self) -> None:
        record = self.store.load_candidate_lifecycle("candidate-00")
        assert isinstance(record, dict)
        self.store.save_candidate_lifecycle(
            "candidate-00",
            "REJECTED",
            dict(record["payload"]),
            from_stage="PAPER_FORWARD",
            reason="latency ranking refresh",
            timestamp=T0 + timedelta(minutes=1),
        )
        ranking = CandidateCanaryRanker(
            self.store,
            service=self.service,
            clock=lambda: T0 + timedelta(minutes=1),
        ).evaluate_and_select(T0 + timedelta(minutes=1))
        self.assertEqual(ranking["selected_candidate"], "candidate-01")
        self.assertEqual(ranking["eligible_count"], CANDIDATE_COUNT - 1)
        self.assertEqual(ranking["rankable_count"], CANDIDATE_COUNT - 1)
        published = self.service.publish_readiness_snapshot(reason="RANKING_REFRESH")
        self.assertEqual(published["readiness_snapshot_status"], "CURRENT")

        status, canary_payload, _body = self._request(self.server, "api/v2/canary")
        self.assertEqual(status, 200)
        self.assertIsInstance(canary_payload, dict)
        assert isinstance(canary_payload, dict)
        canary = canary_payload["canary"]
        self.assertEqual(canary["selected_candidate"], "candidate-01")
        self.assertEqual(canary["last_selected_candidate"], "candidate-00")
        self.assertEqual(canary["eligible_count"], CANDIDATE_COUNT - 1)
        self.assertEqual(canary["rankable_count"], CANDIDATE_COUNT - 1)

        status, candidates_payload, _body = self._request(
            self.server,
            "api/v2/candidates",
            page=1,
            page_size=100,
            sort="candidate_id",
            direction="asc",
        )
        self.assertEqual(status, 200)
        self.assertIsInstance(candidates_payload, dict)
        assert isinstance(candidates_payload, dict)
        self.assertEqual(candidates_payload["total"], CANDIDATE_COUNT)
        candidate_zero = next(
            item
            for item in candidates_payload["items"]
            if item["candidate_id"] == "candidate-00"
        )
        self.assertEqual(candidate_zero["stage"], "REJECTED")

    def test_canary_enable_and_decision_signal_updates_projection(self) -> None:
        enabled = self.service.enable_autonomous_micro_live()
        self.assertEqual(enabled["micro_live_canary"], AUTONOMOUS_MICRO_LIVE)
        self.service.record_autonomous_decision(
            next_decision="WAIT_FOR_CURRENT_ORDER_BOOK",
            blocker="CURRENT_ORDER_BOOK_REQUIRED",
            signal_id="latency-signal-update",
            worker_status="IDLE",
            timestamp=T0 + timedelta(minutes=2),
        )
        self.service.publish_readiness_snapshot(reason="AUTONOMOUS_DECISION")

        status, payload, _body = self._request(self.server, "api/v2/canary")
        self.assertEqual(status, 200)
        self.assertIsInstance(payload, dict)
        assert isinstance(payload, dict)
        canary = payload["canary"]
        self.assertEqual(canary["micro_live_canary"], AUTONOMOUS_MICRO_LIVE)
        autonomous = payload["autonomous_canary"]
        self.assertTrue(autonomous["enabled"])
        self.assertEqual(autonomous["selected_candidate"], "candidate-00")
        self.assertEqual(autonomous["next_decision"], "WAIT_FOR_CURRENT_ORDER_BOOK")
        self.assertEqual(autonomous["blocker"], "CURRENT_ORDER_BOOK_REQUIRED")
        self.assertEqual(autonomous["last_signal_id"], "latency-signal-update")
        self.assertEqual(autonomous["readiness_snapshot_status"], "CURRENT")

    def test_execution_path_rechecks_authoritative_validation(self) -> None:
        signal_id = "latency-validation-guard"
        lifecycle = self.store.load_candidate_lifecycle("candidate-00")
        assert isinstance(lifecycle, dict)
        payload = lifecycle["payload"]
        self.store.connection.execute(
            "INSERT INTO canary_signals("
            "signal_id,candidate_id,frozen_hash,strategy_hash,model_hash,config_hash,"
            "market_id,token_id,outcome,side,paper_expected_price,source_snapshot_id,"
            "source_timestamp,generated_at,expires_at,status,reason,evidence_json,updated_at) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                signal_id,
                "candidate-00",
                payload["frozen_hash"],
                payload["strategy_hash"],
                payload["model_hash"],
                payload["config_hash"],
                "market-00",
                "market-00-yes",
                "yes",
                "BUY",
                "0.50",
                "fixture-snapshot",
                T0.isoformat(),
                T0.isoformat(),
                (T0 + timedelta(minutes=5)).isoformat(),
                "READY",
                None,
                json.dumps({"current_execution_evidence": "CURRENT_ORDER_BOOK"}),
                T0.isoformat(),
            ),
        )
        self.store.connection.commit()

        def reject(
            _service: CanaryService,
            _candidate_id: str,
            **_kwargs: object,
        ) -> dict[str, object]:
            return {
                "eligible": False,
                "reason_code": "CANDIDATE_RESEARCH_GATES_INCOMPLETE",
            }

        with patch.object(
            CanaryService,
            "validate_eligibility",
            autospec=True,
            side_effect=reject,
        ) as validation:
            with self.assertRaisesRegex(
                CanaryBlocked,
                "CANDIDATE_RESEARCH_GATES_INCOMPLETE",
            ):
                self.service.submit_signal(
                    signal_id,
                    venue=_NeverUsedVenue(),
                    allow_test_venue=True,
                )
        validation.assert_called()

    def test_concurrent_writer_and_independent_dashboard_reader_use_wal(self) -> None:
        writer = AxiomStore(str(self.database_path))
        self.addCleanup(writer.close)
        self.assertEqual(
            str(writer.connection.execute("PRAGMA journal_mode").fetchone()[0]).lower(),
            "wal",
        )
        entered = threading.Event()
        release = threading.Event()
        errors: list[BaseException] = []

        def hold_writer_transaction() -> None:
            try:
                with writer.transaction(immediate=True):
                    writer.save_paper_state(
                        "writer-paper-state",
                        {"status": "OPEN", "portfolio": {"equity": 1000.0}},
                        timestamp=T0,
                    )
                    entered.set()
                    if not release.wait(timeout=2):
                        raise AssertionError("writer release timed out")
            except BaseException as exc:
                errors.append(exc)

        thread = threading.Thread(target=hold_writer_transaction)
        thread.start()
        self.assertTrue(entered.wait(timeout=2))
        try:
            started = time.perf_counter()
            status, payload, _body = self._request(
                self.server,
                "api/v2/overview-summary",
            )
            elapsed = time.perf_counter() - started
        finally:
            release.set()
            thread.join(timeout=2)
        self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(status, 200)
        self.assertIsInstance(payload, dict)
        self.assertLess(elapsed, 1.0)

    def test_dashboard_responses_never_expose_secret_values(self) -> None:
        responses: list[str] = []
        for endpoint in ("api/v2/overview-summary", "api/v2/canary", "api/operator"):
            status, _payload, body = self._request(self.server, endpoint)
            self.assertEqual(status, 200)
            responses.append(body)
        encoded = "\n".join(responses)
        for secret in SECRET_VALUES:
            self.assertNotIn(secret, encoded)

if __name__ == "__main__":
    unittest.main()
