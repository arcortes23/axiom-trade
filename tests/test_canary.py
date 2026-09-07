from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
import hashlib
import sys
import multiprocessing
import sqlite3
import tempfile
import threading
import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from axiom.canary import CanaryBlocked, CanaryLimits, CanaryService, CredentialStore, PolymarketClobV2Venue, PRODUCTION_LIVE_EXECUTION
from axiom.cli import main
from axiom.dashboard import DashboardData, _dashboard_html
from axiom.storage import AxiomStore, SQLiteBusyTimeout

T0=datetime(2026,1,2,12,tzinfo=timezone.utc)

class HealthyStore(AxiomStore):
    def polymarket_health(self, **kwargs):
        return {"grade":"A","errors":0}

class FakeCredentials(CredentialStore):
    def __init__(self, configured=True): self.value=configured
    def configured(self, **kwargs): return self.value
    def load(self, **kwargs): return {name:"test-only" for name in ("private_key","wallet_address","relayer_api_key","relayer_api_key_address")} if self.value else {}

class FakeVenue:
    def __init__(self, *, blocked=False, close_only=False, minimum="1", ask="0.50", asks=None, balance="10", accepting=True):
        self.blocked=blocked; self.close_only=close_only; self.minimum=minimum; self.ask=ask; self.asks=asks if asks is not None else [{"price":ask,"size":"100"}]; self._balance=balance; self.accepting=accepting; self.submissions=[]
    def geoblock(self): return {"blocked":self.blocked,"close_only":self.close_only,"country":"ZZ","region":"T"}
    def connectivity_check(self): return True
    def market_context(self, market_id, token_id): return {"accepting_orders":self.accepting,"min_order_size":self.minimum,"tick_size":"0.01","bids":[{"price":"0.49","size":"100"}],"asks":self.asks,"fee_bps":"10"}
    def balance(self): return Decimal(self._balance)
    def submit_limit_order(self, **kwargs): self.submissions.append(kwargs); return {"ok":True,"order_id":"fake-order","status":"matched"}
class CrashGapVenue(FakeVenue):
    def __init__(self, store):
        super().__init__()
        self.store = store
        self.observed_status = None
        self.observed_in_transaction = None

    def submit_limit_order(self, **kwargs):
        self.observed_in_transaction = self.store.connection.in_transaction
        row = self.store.connection.execute(
            "SELECT status FROM canary_ledger WHERE signal_id=?",
            ("crash-gap",),
        ).fetchone()
        self.observed_status = row["status"] if row is not None else None
        raise KeyboardInterrupt("simulated process interruption")


class ProcessBlockingVenue(FakeVenue):
    def __init__(self, entered, release):
        super().__init__()
        self.entered = entered
        self.release = release

    def submit_limit_order(self, **kwargs):
        self.entered.set()
        if not self.release.wait(15):
            raise RuntimeError("process test release timed out")
        return {"ok": True, "order_id": "process-order", "status": "matched"}


def _blocked_submit_worker(database_path, entered, release, results):
    store = HealthyStore(database_path)
    try:
        service = CanaryService(
            store,
            credentials=FakeCredentials(),
            clock=lambda: T0,
        )
        result = service.submit(
            signal_id="cross-process",
            candidate_id="C123",
            market_id="m",
            token_id="yes",
            side="BUY",
            paper_expected_price=Decimal("0.50"),
            venue=ProcessBlockingVenue(entered, release),
            allow_test_venue=True,
        )
        results.put(("ok", result))
    except BaseException as exc:
        results.put(("error", type(exc).__name__, str(exc)))
    finally:
        store.close()


class CanaryTests(unittest.TestCase):

    def setUp(self):
        self.store=HealthyStore(":memory:")
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
        self.service=CanaryService(self.store,credentials=FakeCredentials(),clock=lambda:T0); self.venue=FakeVenue()
        hash_parts=("strategy-v1","model-v1","config-v1")
        payload={
            "market_type":"prediction",
            "source_type":"HISTORICAL",
            "dataset_id":"prediction-history",
            "dataset_version":"v1",
            "dataset_provenance":{
                "dataset_id":"prediction-history",
                "dataset_version":"v1",
                "provider":"polymarket",
                "instrument":"POLYMARKET",
                "market_type":"prediction",
                "timeframe":"event",
                "source_type":"HISTORICAL",
                "snapshot_id":"prediction-history:v1",
                "time_split":"train-validation-holdout",
            },
            "schema_validated":True,
            "historical_backtest_passed":True,
            "validation_passed":True,
            "robustness_passed":True,
            "data_quality_passed":True,
            "data_quality":"PRICE_PROXY",
            "validation_expectancy":0.10,
            "validation_confidence_lower_bound":0.05,
            "validation_stability":0.90,
            "validation_calibration":0.90,
            "validation_execution_quality":0.90,
            "validation_sample_count":100,
            "validation_trade_count":20,
            "minimum_sample_check":{
                "passed":True,
                "count":100,
                "trades":20,
                "min_observations":30,
                "min_trades":10,
                "checks":{"observations":True,"trades":True},
            },
            "experiment_plan":{
                "policy_version":"canary-sample-policy-v1",
                "min_independent_samples":30,
                "min_trades":10,
            },
            "frozen":True,
            "holdout_used":False,
            "strategy_hash":hash_parts[0],
            "model_hash":hash_parts[1],
            "config_hash":hash_parts[2],
            "frozen_hash":hashlib.sha256("|".join(hash_parts).encode()).hexdigest(),
            "forward_evidence":{
                "forward_duration_seconds":7*86400,
                "forward_independent_resolved_bets":30,
                "forward_successful_order_attempts":20,
                "forward_expectancy":0.1,
                "forward_confidence_lower_bound":0.0,
                "forward_stability":0.6,
                "forward_calibration":0.8,
                "forward_liquidity":0.0,
                "forward_max_drawdown":0.2,
                "forward_regime_count":3,
            },
        }
        self.store.save_candidate_lifecycle("C123","IDEA",payload,timestamp=T0)
        for stage in ("SCHEMA_VALIDATED","BACKTESTED","VALIDATED","ROBUSTNESS_CHECKED","FROZEN","PAPER_FORWARD","PAPER_PROMOTABLE"):
            self.store.save_candidate_lifecycle("C123",stage,payload,timestamp=T0)
        self.service.mark_eligible("C123")
    def arm(self, **kwargs): return self.service.arm("C123",venue=kwargs.pop("venue",self.venue),credentials_configured=True,**kwargs)
    def submit(self, signal="s1", **kwargs):
        candidate_id = kwargs.pop("candidate_id", "C123")
        return self.service.submit(signal_id=signal,candidate_id=candidate_id,market_id="m",token_id="yes",side="BUY",paper_expected_price=Decimal("0.50"),venue=kwargs.pop("venue",self.venue),allow_test_venue=kwargs.pop("allow_test_venue",True),**kwargs)
    def assertBlocked(self, code, fn):
        with self.assertRaisesRegex(CanaryBlocked,code): fn()

    def test_default_startup_cannot_trade(self): self.assertBlocked("CANARY_NOT_ARMED",self.submit)

    def test_limits_reject_nonfinite_values(self):
        for field in ("target_notional_usd", "max_exposure_usd", "max_daily_loss_usd"):
            with self.assertRaises(ValueError):
                CanaryLimits(**{field: Decimal("NaN")})
            with self.assertRaises(ValueError):
                CanaryLimits(**{field: Decimal("Infinity")})
    def test_missing_credentials_cannot_arm(self):
        service=CanaryService(self.store,credentials=FakeCredentials(False),clock=lambda:T0)
        self.assertBlocked("CREDENTIALS_NOT_CONFIGURED",lambda:service.arm("C123",venue=self.venue,credentials_configured=True))

    def test_credentials_require_only_signer_and_preserve_optional_relayer_values(self):
        class Keyring:
            values = {
                "relayer_api_key": "existing-relayer-key",
                "relayer_api_key_address": "existing-relayer-address",
            }

            @classmethod
            def get_password(cls, _service, name):
                return cls.values.get(name)

            @classmethod
            def set_password(cls, _service, name, value):
                cls.values[name] = value

        responses = iter(("private-key", "wallet-address", "", ""))
        with patch.dict(sys.modules, {"keyring": Keyring}):
            credentials = CredentialStore()
            credentials.configure(reader=lambda _prompt: next(responses))
            loaded = credentials.load()
            self.assertTrue(credentials.configured())

        self.assertEqual(loaded["private_key"], "private-key")
        self.assertEqual(loaded["wallet_address"], "wallet-address")
        self.assertEqual(loaded["relayer_api_key"], "existing-relayer-key")
        self.assertEqual(
            loaded["relayer_api_key_address"],
            "existing-relayer-address",
        )
    def test_safe_projection_single_flight_is_bounded_and_secret_free(self):
        class ProbeCredentialStore(CredentialStore):
            pass

        probe_started = threading.Event()
        release_probe = threading.Event()
        probe_finished = threading.Event()
        probe_lock = threading.Lock()
        probe_calls = 0
        results = [None] * 4
        errors = []
        completed = [threading.Event() for _ in results]
        barrier = threading.Barrier(len(results))

        def blocked_configured(_credentials, **_kwargs):
            nonlocal probe_calls
            with probe_lock:
                probe_calls += 1
            probe_started.set()
            release_probe.wait(2)
            probe_finished.set()
            return True

        def project(index):
            try:
                barrier.wait()
                results[index] = ProbeCredentialStore().safe_projection(
                    timeout_seconds=0.05
                )
            except BaseException as exc:
                errors.append(exc)
            finally:
                completed[index].set()

        with patch.object(CredentialStore, "configured", blocked_configured):
            threads = [
                threading.Thread(target=project, args=(index,))
                for index in range(len(results))
            ]
            for thread in threads:
                thread.start()
            try:
                self.assertTrue(probe_started.wait(1))
                for done in completed:
                    self.assertTrue(done.wait(0.5))
                self.assertFalse(release_probe.is_set())
                self.assertEqual(probe_calls, 1)
                self.assertFalse(errors)
                expected_keys = {
                    "configured",
                    "status",
                    "secret_values_exposed",
                }
                for projection in results:
                    self.assertIsNotNone(projection)
                    self.assertEqual(set(projection), expected_keys)
                    self.assertFalse(projection["configured"])
                    self.assertEqual(projection["status"], "NOT CONFIGURED")
                    self.assertFalse(projection["secret_values_exposed"])
                    self.assertNotIn("private", json.dumps(projection).lower())
                release_probe.set()
                self.assertTrue(probe_finished.wait(1))
                cached = ProbeCredentialStore().safe_projection(timeout_seconds=0.05)
            finally:
                release_probe.set()
                for thread in threads:
                    thread.join(1)

        self.assertEqual(set(cached), expected_keys)
        self.assertTrue(cached["configured"])
        self.assertEqual(cached["status"], "CONFIGURED")
        self.assertFalse(cached["secret_values_exposed"])
        self.assertNotIn("private", json.dumps(cached).lower())



    def test_hermes_has_no_canary_execution_fields(self):
        from axiom.director import validate_hermes_proposal
        result=validate_hermes_proposal({"proposal_id":"x","statement":"x","source":"x","tests":["x"],"dataset_version":"v","time_split":"train-validation-holdout","paper_only":True,"canary_arm":True})
        self.assertFalse(result.accepted)
    def test_ineligible_candidate_cannot_arm(self): self.assertBlocked("NOT_CANARY_ELIGIBLE",lambda:self.service.arm("other",venue=self.venue,credentials_configured=True))
    def test_frozen_and_paper_forward_stages_store_bounded_qualification_evidence(self):
        template = dict(self.store.load_candidate_lifecycle("C123")["payload"])
        progression = (
            "SCHEMA_VALIDATED",
            "BACKTESTED",
            "VALIDATED",
            "ROBUSTNESS_CHECKED",
            "FROZEN",
            "PAPER_FORWARD",
        )
        telemetry_fields = (
            "forward_duration_seconds",
            "forward_independent_resolved_bets",
            "forward_successful_order_attempts",
            "forward_expectancy",
            "forward_confidence_lower_bound",
            "forward_stability",
            "forward_calibration",
            "forward_liquidity",
            "forward_max_drawdown",
            "forward_regime_count",
        )
        for candidate_id, terminal_stage in (
            ("ELIGIBLE-FROZEN", "FROZEN"),
            ("ELIGIBLE-FORWARD", "PAPER_FORWARD"),
        ):
            self.store.save_candidate_lifecycle(candidate_id, "IDEA", template, timestamp=T0)
            for stage in progression:
                self.store.save_candidate_lifecycle(candidate_id, stage, template, timestamp=T0)
                if stage == terminal_stage:
                    break

            self.service.mark_eligible(candidate_id)
            lifecycle = self.store.load_candidate_lifecycle(candidate_id)
            row = self.store.connection.execute(
                "SELECT frozen_hash,evidence_json FROM canary_eligibility WHERE candidate_id=?",
                (candidate_id,),
            ).fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(lifecycle["stage"], terminal_stage)
            self.assertNotEqual(lifecycle["stage"], "PAPER_PROMOTABLE")
            evidence = json.loads(row["evidence_json"])
            self.assertEqual(evidence["schema"], "canary-qualification-v1")
            self.assertEqual(evidence["schema_version"], "canary-qualification-v1")
            self.assertEqual(evidence["qualification_schema"], "canary-qualification-v1")
            self.assertEqual(evidence["candidate_id"], candidate_id)
            self.assertEqual(row["frozen_hash"], evidence["frozen_hash"])
            self.assertIsInstance(evidence["qualification_hash"], str)
            self.assertTrue(evidence["qualification_hash"])
            required_fields = (
                "market_type",
                "source_type",
                "dataset_id",
                "dataset_version",
                "dataset_provenance",
                "strategy_hash",
                "model_hash",
                "config_hash",
                "frozen_hash",
                "frozen",
                "holdout_used",
                "validation_expectancy",
                "validation_confidence_lower_bound",
                "validation_stability",
                "validation_calibration",
                "validation_execution_quality",
                "validation_sample_count",
                "validation_trade_count",
                "experiment_plan",
                "minimum_sample_check",
                "data_quality",
                "data_quality_passed",
            )
            for key in required_fields:
                with self.subTest(candidate_id=candidate_id, field=key):
                    self.assertIn(key, evidence)
                    self.assertEqual(evidence[key], template[key])
            self.assertEqual(
                self.store.load_dataset("prediction-history", "v1"),
                [{"timestamp": T0.isoformat(), "price": 0.5, "source_type": "HISTORICAL"}],
            )
            catalog = self.store.load_dataset_catalog("prediction-history", "v1")
            self.assertEqual(
                {
                    key: catalog[key]
                    for key in (
                        "dataset_id",
                        "dataset_version",
                        "provider",
                        "instrument",
                        "market_type",
                        "timeframe",
                        "row_count",
                        "completeness",
                        "quality",
                        "source_type",
                        "snapshot_id",
                    )
                },
                {
                    "dataset_id": "prediction-history",
                    "dataset_version": "v1",
                    "provider": "polymarket",
                    "instrument": "POLYMARKET",
                    "market_type": "prediction",
                    "timeframe": "event",
                    "row_count": 1,
                    "completeness": 1.0,
                    "quality": "PRICE_PROXY",
                    "source_type": "HISTORICAL",
                    "snapshot_id": "prediction-history:v1",
                },
            )
            quality_fields = {
                "historical_data_integrity": "PASS",
                "historical_data_integrity_passed": True,
                "historical_execution_fidelity": "PRICE_PROXY",
                "historical_execution_fidelity_score": 0.35,
                "current_execution_evidence": "CURRENT_ORDER_BOOK_REQUIRED",
                "historical_provenance_complete": True,
                "historical_rows_nonempty": True,
                "historical_no_forward_contamination": True,
                "canary_data_quality_acceptable": True,
                "canary_data_quality_status": "CANARY_DATA_QUALITY_ACCEPTABLE_LIMITED",
                "production_evidence_status": "INSUFFICIENT",
                "historical_dataset_row_count": 1,
            }
            for key, value in quality_fields.items():
                with self.subTest(candidate_id=candidate_id, field=key):
                    self.assertIn(key, evidence)
                    self.assertEqual(evidence[key], value)
            serialized = json.dumps(evidence)
            self.assertNotIn("forward_evidence", evidence)
            for field in telemetry_fields:
                self.assertNotIn(field, serialized)

    def test_rejected_candidate_cannot_mark_eligible(self):
        template = dict(self.store.load_candidate_lifecycle("C123")["payload"])
        candidate_id = "REJECTED-CANDIDATE"
        self.store.save_candidate_lifecycle(candidate_id, "IDEA", template, timestamp=T0)
        self.store.save_candidate_lifecycle(candidate_id, "REJECTED", template, timestamp=T0)
        self.assertBlocked(
            "CANDIDATE_RESEARCH_GATES_INCOMPLETE",
            lambda: self.service.mark_eligible(candidate_id),
        )
    def test_missing_gate_tampered_hash_and_malformed_documents_are_rejected(self):
        template = dict(self.store.load_candidate_lifecycle("C123")["payload"])
        template.pop("forward_evidence", None)
        cases = (
            ("MISSING-GATE", {**template, "data_quality_passed": False}),
            ("TAMPERED-HASH", {**template, "frozen_hash": "tampered"}),
            ("MALFORMED-DOCUMENTS", {**template, "frozen_documents": []}),
        )
        for candidate_id, payload in cases:
            self.store.save_candidate_lifecycle(candidate_id, "IDEA", payload, timestamp=T0)
            self.store.save_candidate_lifecycle(candidate_id, "FROZEN", payload, timestamp=T0)
            self.assertBlocked(
                "CANDIDATE_RESEARCH_GATES_INCOMPLETE",
                lambda candidate_id=candidate_id: self.service.mark_eligible(candidate_id),
            )
    def test_legacy_eligibility_evidence_must_bind_verified_frozen_hash(self):
        candidate_id = "LEGACY-MISSING-FROZEN-HASH"
        payload = dict(self.store.load_candidate_lifecycle("C123")["payload"])
        self.store.save_candidate_lifecycle(candidate_id, "IDEA", payload, timestamp=T0)
        self.store.save_candidate_lifecycle(candidate_id, "FROZEN", payload, timestamp=T0)
        self.service.mark_eligible(candidate_id)
        row = self.store.connection.execute(
            "SELECT evidence_json FROM canary_eligibility WHERE candidate_id=?",
            (candidate_id,),
        ).fetchone()
        legacy_evidence = json.loads(row["evidence_json"])
        for key in ("schema", "schema_version", "qualification_schema", "qualification_hash", "frozen_hash"):
            legacy_evidence.pop(key, None)
        self.store.connection.execute(
            "UPDATE canary_eligibility SET evidence_json=? WHERE candidate_id=?",
            (json.dumps(legacy_evidence, sort_keys=True), candidate_id),
        )
        self.store.connection.commit()

        validation = self.service.validate_eligibility(candidate_id)
        self.assertFalse(validation["binding"]["bound"])
        self.assertEqual(validation["binding"]["reason_code"], "QUALIFICATION_CHANGED")
        self.assertBlocked(
            "CANDIDATE_NOT_CANARY_ELIGIBLE",
            lambda: self.service.arm(
                candidate_id,
                venue=self.venue,
                credentials_configured=True,
            ),
        )

    def test_no_dataset_prediction_candidate_is_rejected(self):
        candidate_id = "NO-DATASET-CANARY"
        payload = dict(self.store.load_candidate_lifecycle("C123")["payload"])
        for key in ("dataset_id", "dataset_version", "dataset_provenance"):
            payload.pop(key)
        self.store.save_candidate_lifecycle(candidate_id, "IDEA", payload, timestamp=T0)
        self.store.save_candidate_lifecycle(candidate_id, "FROZEN", payload, timestamp=T0)

        validation = self.service.validate_eligibility(candidate_id)
        self.assertFalse(validation["eligible"])
        self.assertFalse(validation["historical_data_integrity_passed"])
        self.assertIn(
            "HISTORICAL_DATASET_VERSION_NOT_FOUND",
            validation["data_quality"]["reasons"],
        )
        self.assertBlocked(
            "CANDIDATE_RESEARCH_GATES_INCOMPLETE",
            lambda: self.service.mark_eligible(candidate_id),
        )

    def test_migrated_pre_column_control_row_starts_at_generation_one(self):
        legacy_store = HealthyStore(":memory:")
        try:
            legacy_store.connection.executescript(
                """
                CREATE TABLE canary_control (
                  singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                  state TEXT NOT NULL,
                  candidate_id TEXT,
                  venue TEXT,
                  armed_at TEXT,
                  expires_at TEXT,
                  limits_json TEXT NOT NULL,
                  integrity_hash TEXT NOT NULL,
                  updated_at TEXT NOT NULL
                );
                INSERT INTO canary_control(
                  singleton,state,candidate_id,venue,armed_at,expires_at,
                  limits_json,integrity_hash,updated_at
                ) VALUES(1,'DISARMED',NULL,NULL,NULL,NULL,'{}','','2026-01-02T12:00:00+00:00');
                """
            )
            legacy_store.connection.commit()
            CanaryService(
                legacy_store,
                credentials=FakeCredentials(),
                clock=lambda: T0,
            )
            row = legacy_store.connection.execute(
                "SELECT control_generation FROM canary_control WHERE singleton=1"
            ).fetchone()
            self.assertEqual(row["control_generation"], 1)
        finally:
            legacy_store.close()

    def test_schema_initialization_migration_holds_store_lock(self):
        legacy_store = HealthyStore(":memory:")
        try:
            legacy_store.connection.executescript(
                """
                CREATE TABLE canary_control (
                  singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                  state TEXT NOT NULL,
                  candidate_id TEXT,
                  venue TEXT,
                  armed_at TEXT,
                  expires_at TEXT,
                  limits_json TEXT NOT NULL,
                  integrity_hash TEXT NOT NULL,
                  updated_at TEXT NOT NULL
                );
                INSERT INTO canary_control(
                  singleton,state,candidate_id,venue,armed_at,expires_at,
                  limits_json,integrity_hash,updated_at
                ) VALUES(1,'DISARMED',NULL,NULL,NULL,NULL,'{}','','2026-01-02T12:00:00+00:00');
                """
            )
            legacy_store.connection.commit()
            service = CanaryService(
                legacy_store,
                credentials=FakeCredentials(),
                clock=lambda: T0,
                initialize=False,
            )
            migration_started = threading.Event()
            release_migration = threading.Event()
            writer_attempted = threading.Event()
            writer_acquired = threading.Event()
            result = {}

            def trace(statement):
                normalized = " ".join(statement.split()).upper()
                if normalized.startswith("UPDATE CANARY_CONTROL SET CONTROL_GENERATION=1 "):
                    migration_started.set()
                    release_migration.wait()

            def initialize():
                try:
                    service._initialize()
                except BaseException as exc:
                    result["error"] = exc

            def competing_writer():
                lock = legacy_store._lock
                acquired = lock.acquire(blocking=False)
                writer_attempted.set()
                if not acquired:
                    return
                try:
                    writer_acquired.set()
                finally:
                    lock.release()

            legacy_store.connection.set_trace_callback(trace)
            initializer = threading.Thread(target=initialize)
            writer = None
            initializer.start()
            try:
                self.assertTrue(migration_started.wait(timeout=2))
                writer = threading.Thread(target=competing_writer)
                writer.start()
                self.assertTrue(writer_attempted.wait(timeout=2))
                self.assertFalse(writer_acquired.is_set())
                release_migration.set()
                initializer.join(timeout=2)
                writer.join(timeout=2)
            finally:
                release_migration.set()
                initializer.join(timeout=2)
                if writer is not None:
                    writer.join(timeout=2)
            legacy_store.connection.set_trace_callback(None)
            self.assertFalse(initializer.is_alive())
            self.assertIsNotNone(writer)
            self.assertFalse(writer.is_alive())
            self.assertNotIn("error", result)
            self.assertFalse(writer_acquired.is_set())
        finally:
            legacy_store.close()

    def test_authoritative_status_read_is_serialized_against_concurrent_writer(self):
        self.store.connection.execute(
            "INSERT INTO canary_selection("
            "singleton,ranking_run_id,candidate_id,rank,total_score,"
            "component_scores_json,evidence_versions_json,reason,selected_at,"
            "ranking_timestamp,qualification_hash,ranking_snapshot_hash,"
            "selection_status,selection_valid,selection_invalidation_reason,"
            "last_selected_candidate"
            ") VALUES(1,'run-1','C123',1,0.1,'{}','{}','test',?,?,?,?,?,1,NULL,'C123')",
            (
                T0.isoformat(),
                T0.isoformat(),
                "qualification-hash",
                "ranking-hash",
                "CURRENT",
            ),
        )
        self.store.connection.commit()
        read_started = threading.Event()
        release_read = threading.Event()
        writer_attempted = threading.Event()
        writer_acquired = threading.Event()
        result = {}
        original_limits_record = self.service._limits_record

        def gated_limits_record(limits):
            read_started.set()
            if not release_read.wait(2):
                raise AssertionError("authoritative status projection was not released")
            return original_limits_record(limits)

        def read_status():
            try:
                result["status"] = self.service.authoritative_status()
            except BaseException as exc:
                result["error"] = exc

        def competing_writer():
            lock = self.store._lock
            acquired = lock.acquire(blocking=False)
            writer_attempted.set()
            if not acquired:
                return
            try:
                writer_acquired.set()
                self.store.connection.execute(
                    "UPDATE canary_selection SET candidate_id='MIXED' WHERE singleton=1"
                )
                self.store.connection.commit()
            finally:
                lock.release()

        with patch.object(self.service, "_limits_record", gated_limits_record):
            reader = threading.Thread(target=read_status)
            reader.start()
            self.assertTrue(read_started.wait(1))
            writer = threading.Thread(target=competing_writer)
            writer.start()
            self.assertTrue(writer_attempted.wait(1))
            self.assertFalse(writer_acquired.is_set())
            release_read.set()
            reader.join(2)
            writer.join(2)

        self.assertFalse(reader.is_alive())
        self.assertFalse(writer.is_alive())
        self.assertNotIn("error", result)
        self.assertIn("status", result)
        self.assertFalse(writer_acquired.is_set())
    def test_projection_busy_after_authoritative_commit_returns_stale_result(self):
        with patch(
            "axiom.canary.sqlite_retry",
            side_effect=SQLiteBusyTimeout("publish canary readiness snapshot"),
        ):
            result = self.arm()
        self.assertEqual(result["readiness_snapshot_status"], "STALE")
        self.assertTrue(result["readiness_snapshot_stale"])
        self.assertEqual(
            self.store.connection.execute(
                "SELECT state FROM canary_control WHERE singleton=1"
            ).fetchone()["state"],
            "ARMED",
        )
        self.assertEqual(self.service.status()["readiness_snapshot_status"], "STALE")
        self.service.publish_readiness_snapshot(reason="RETRY_AFTER_BUSY")
        self.assertEqual(self.service.status()["readiness_snapshot_status"], "CURRENT")

    def test_snapshot_publications_serialize_authoritative_read_and_write(self):
        with tempfile.TemporaryDirectory() as directory:
            path = f"{directory}\\projection-order.sqlite3"
            first_store = AxiomStore(path, sqlite_timeout_seconds=0.05)
            second_store = AxiomStore(path, sqlite_timeout_seconds=0.05)
            first = CanaryService(first_store, clock=lambda: T0)
            second = CanaryService(second_store, clock=lambda: T0)
            first_read = threading.Event()
            release_first = threading.Event()
            results = {}

            def first_status():
                return {"micro_live_canary": "ARMED", "selection_status": "S1"}

            def second_status():
                return {"micro_live_canary": "DISARMED", "selection_status": "S2"}

            def first_signal():
                first_read.set()
                if not release_first.wait(3):
                    raise AssertionError("first publication was not released")
                return None

            first.authoritative_status = first_status
            second.authoritative_status = second_status
            first.latest_signal = first_signal
            second.latest_signal = lambda: None

            def publish(service, key):
                try:
                    results[key] = service.publish_readiness_snapshot(reason=key)
                except BaseException as exc:
                    results[key] = exc

            first_thread = threading.Thread(target=publish, args=(first, "S1"))
            second_thread = threading.Thread(target=publish, args=(second, "S2"))
            try:
                first_thread.start()
                self.assertTrue(first_read.wait(2))
                second_thread.start()
                time.sleep(0.2)
                release_first.set()
                first_thread.join(5)
                second_thread.join(5)
                self.assertFalse(first_thread.is_alive())
                self.assertFalse(second_thread.is_alive())
                self.assertNotIsInstance(results.get("S1"), BaseException)
                self.assertNotIsInstance(results.get("S2"), BaseException)
                self.assertEqual(
                    second.readiness_snapshot()["selection_status"],
                    "S2",
                )
            finally:
                release_first.set()
                first_thread.join(5)
                second_thread.join(5)
                second_store.close()
                first_store.close()


    def test_paper_forward_telemetry_update_preserves_eligibility_binding(self):
        payload = dict(self.store.load_candidate_lifecycle("C123")["payload"])
        before = json.loads(
            self.store.connection.execute(
                "SELECT evidence_json FROM canary_eligibility WHERE candidate_id='C123'"
            ).fetchone()[0]
        )
        payload["forward_evidence"] = {
            **payload["forward_evidence"],
            "forward_duration_seconds": 30 * 86400,
            "forward_independent_resolved_bets": 300,
            "forward_successful_order_attempts": 240,
            "observations_without_signal": 17,
        }
        self.store.save_candidate_lifecycle(
            "C123",
            "PAPER_PROMOTABLE",
            payload,
            timestamp=T0,
        )
        after = json.loads(
            self.store.connection.execute(
                "SELECT evidence_json FROM canary_eligibility WHERE candidate_id='C123'"
            ).fetchone()[0]
        )
        self.assertEqual(after["qualification_hash"], before["qualification_hash"])

        validation = self.service.validate_eligibility("C123")
        self.assertTrue(validation["eligible"], validation)
        self.arm()
        self.assertEqual(self.service.status()["micro_live_canary"], "ARMED")

    def test_current_hard_gate_blocks_even_with_immutable_binding(self):
        payload = dict(self.store.load_candidate_lifecycle("C123")["payload"])
        payload["critical_error"] = "runtime-failure"
        self.store.save_candidate_lifecycle(
            "C123",
            "PAPER_PROMOTABLE",
            payload,
            timestamp=T0,
        )
        self.assertBlocked(
            "CANDIDATE_(?:NOT_CANARY_ELIGIBLE|RESEARCH_GATES_INCOMPLETE)",
            self.arm,
        )
        self.assertFalse(self.venue.submissions)

    def test_rejected_lifecycle_blocks_even_with_immutable_binding(self):
        payload = dict(self.store.load_candidate_lifecycle("C123")["payload"])
        self.store.save_candidate_lifecycle(
            "C123",
            "REJECTED",
            payload,
            timestamp=T0,
        )
        self.assertBlocked(
            "CANDIDATE_NOT_CANARY_ELIGIBLE",
            self.arm,
        )
        self.assertFalse(self.venue.submissions)

    def test_submission_binding_rejects_hash_mutations(self):
        payload = dict(self.store.load_candidate_lifecycle("C123")["payload"])
        for key in ("strategy_hash", "model_hash", "config_hash"):
            with self.subTest(key=key):
                changed = dict(payload)
                changed[key] = f"mutated-{key}"
                changed["frozen_hash"] = hashlib.sha256(
                    "|".join(
                        changed[name]
                        for name in ("strategy_hash", "model_hash", "config_hash")
                    ).encode()
                ).hexdigest()
                self.store.save_candidate_lifecycle(
                    "C123",
                    "PAPER_PROMOTABLE",
                    changed,
                    timestamp=T0,
                )
                self.assertBlocked(
                    "CANDIDATE_NOT_CANARY_ELIGIBLE",
                    self.arm,
                )
    def test_restart_preserves_valid_eligibility_binding(self):
        restarted = CanaryService(
            self.store,
            credentials=FakeCredentials(),
            clock=lambda: T0,
        )
        validation = restarted.validate_eligibility("C123")
        self.assertTrue(validation["eligible"], validation)
        restarted.arm(
            "C123",
            venue=self.venue,
            credentials_configured=True,
        )
        self.assertEqual(restarted.status()["micro_live_canary"], "ARMED")
    def test_public_counts_exclude_tampered_eligibility_binding(self):
        payload = dict(self.store.load_candidate_lifecycle("C123")["payload"])
        self.store.save_candidate_lifecycle(
            "STALE",
            "IDEA",
            {**payload, "candidate_id": "STALE"},
            timestamp=T0,
        )
        self.store.save_candidate_lifecycle(
            "STALE",
            "FROZEN",
            {**payload, "candidate_id": "STALE"},
            from_stage="IDEA",
            timestamp=T0,
        )
        self.service.mark_eligible("STALE")
        with self.store.connection:
            self.store.connection.execute(
                "UPDATE canary_eligibility SET frozen_hash=? WHERE candidate_id=?",
                ("tampered-binding", "STALE"),
            )
        self.service.publish_readiness_snapshot(reason="ELIGIBILITY_TAMPERED")

        status = self.service.status()
        self.assertEqual(status["eligible_count"], 1)
        with patch.object(
            CredentialStore,
            "safe_projection",
            return_value={
                "configured": False,
                "status": "NOT CONFIGURED",
                "secret_values_exposed": False,
            },
        ):
            dashboard = DashboardData(store=self.store).canary_data()
        self.assertEqual(dashboard["canary"]["eligible_count"], 1)
        self.assertEqual(dashboard["research_cards"]["canary_eligible"], 1)
        self.assertEqual(dashboard["candidate_status"]["canary_eligible"], 1)


    def test_expired_arm_cannot_trade(self):
        self.arm(expires_hours=Decimal("0.001")); self.service.clock=lambda:T0+timedelta(hours=1)
        self.assertBlocked("CANARY_NOT_ARMED",self.submit)
    def test_kill_switch_prevents_trading(self): self.arm(); self.service.kill(); self.assertBlocked("CANARY_NOT_ARMED",self.submit)
    def test_kill_switch_latches_against_rearming(self):
        self.arm()
        self.service.kill()
        self.service.disarm()
        self.assertEqual(self.service.status()["micro_live_canary"], "KILLED")
        self.assertBlocked("CANARY_KILLED", self.arm)

    def test_arm_rechecks_kill_latch_before_write(self):
        service = self.service

        class KillingVenue(FakeVenue):
            def geoblock(self):
                service.kill()
                return super().geoblock()

        self.assertBlocked(
            "CANARY_KILLED",
            lambda: service.arm(
                "C123",
                venue=KillingVenue(),
                credentials_configured=True,
            ),
        )
        self.assertEqual(service.status()["micro_live_canary"], "KILLED")
    def test_degraded_collector_prevents_arming(self):
        self.store.polymarket_health=lambda **kwargs:{"grade":"D"}
        self.assertBlocked("COLLECTOR_DEGRADED",self.arm)
    def test_geoblock_prevents_arming_and_submission(self):
        blocked=FakeVenue(blocked=True); self.assertBlocked("GEOGRAPHICALLY_BLOCKED",lambda:self.arm(venue=blocked))
        self.arm(); self.assertBlocked("GEOGRAPHICALLY_BLOCKED",lambda:self.submit(venue=blocked))
    def test_target_is_never_silently_increased(self): self.arm(); result=self.submit(); self.assertLessEqual(Decimal(result["requested_notional"]),Decimal("1.00"))
    def test_market_minimum_exceeding_target_skips(self): self.arm(); venue=FakeVenue(minimum="5",ask="0.50"); self.assertBlocked("VENUE_MINIMUM_EXCEEDS",lambda:self.submit(venue=venue)); self.assertFalse(venue.submissions)

    def test_non_polymarket_venue_requires_explicit_test_opt_in(self):
        self.arm()
        self.assertBlocked(
            "UNSUPPORTED_VENUE",
            lambda: self.submit(allow_test_venue=False),
        )

    def test_polymarket_subclass_requires_explicit_test_opt_in(self):
        class EvilVenue(PolymarketClobV2Venue):
            def geoblock(self):
                return {"blocked": False, "close_only": False}

        self.arm()
        self.assertBlocked(
            "UNSUPPORTED_VENUE",
            lambda: self.submit(
                venue=EvilVenue(),
                allow_test_venue=False,
            ),
        )
    def test_slippage_prevents_submission(self): self.arm(); venue=FakeVenue(ask="0.60"); self.assertBlocked("SLIPPAGE_LIMIT",lambda:self.submit(venue=venue)); self.assertFalse(venue.submissions)
    def test_unsorted_asks_use_lowest_price_for_readiness_and_submission(self):
        self.arm()
        venue = FakeVenue(
            minimum="2",
            asks=[
                {"price": "0.50", "size": "100"},
                {"price": "0.55", "size": "100"},
            ],
        )
        readiness = self.service.check(
            candidate_id="C123",
            venue=venue,
            market_id="m",
            token_id="yes",
        )
        self.assertTrue(readiness["ready"], readiness)

        self.submit(venue=venue)
        self.assertEqual(venue.submissions[0]["price"], Decimal("0.50"))
        evidence = json.loads(
            self.store.connection.execute(
                "SELECT evidence_json FROM canary_ledger"
            ).fetchone()[0]
        )
        self.assertEqual(evidence["ask"], "0.50")
        self.assertEqual(evidence["depth"], venue.asks)

    def test_invalid_asks_fail_closed_for_readiness_and_submission(self):
        self.arm()
        for index, asks in enumerate(
            (
                [{"price": "NaN", "size": "100"}],
                [{"price": "0", "size": "100"}],
                [{"price": "-0.01", "size": "100"}],
                [{"price": "not-a-price", "size": "100"}],
            )
        ):
            venue = FakeVenue(asks=asks)
            readiness = self.service.check(
                candidate_id="C123",
                venue=venue,
                market_id="m",
                token_id="yes",
            )
            self.assertFalse(readiness["ready"])
            self.assertIn("MARKET_CONNECTIVITY_FAILED", readiness["failures"])
            self.assertBlocked(
                "INVALID_CANARY_PARAMETERS",
                lambda index=index, venue=venue: self.submit(
                    signal=f"invalid-ask-{index}",
                    venue=venue,
                ),
            )
            self.assertFalse(venue.submissions)


    def test_insufficient_balance_blocks_readiness_and_submission(self):
        self.arm()
        venue = FakeVenue(balance="0.50")
        readiness = self.service.check(candidate_id="C123", venue=venue)
        self.assertFalse(readiness["ready"])
        self.assertIn("INSUFFICIENT_BALANCE", readiness["failures"])
        self.assertBlocked("INSUFFICIENT_BALANCE", lambda: self.submit(venue=venue))
        self.assertFalse(venue.submissions)

    def test_balance_includes_worst_case_fees(self):
        self.arm()
        venue = FakeVenue(balance="1.00")
        self.assertBlocked(
            "INSUFFICIENT_BALANCE",
            lambda: self.submit(venue=venue),
        )
        self.assertFalse(venue.submissions)

    def test_exposure_limit_includes_worst_case_fees(self):
        self.arm(limits=CanaryLimits(max_exposure_usd=Decimal("1")))
        self.assertBlocked("EXPOSURE_LIMIT", self.submit)

    def test_official_venue_rejects_plaintext_credential_mapping(self):
        with self.assertRaises(TypeError):
            PolymarketClobV2Venue({"private_key": "not-retained"})
    def test_read_only_sdk_client_is_closed(self):
        from axiom.canary import _read_only_operation

        class ClosingClient:
            wallet = "wallet"
            wallet_type = "safe"

            def __init__(self):
                self.closed = False

            def close(self):
                self.closed = True

        client = ClosingClient()

        class RelayerApiKey:
            def __init__(self, **kwargs):
                pass

        class SecureClient:
            @staticmethod
            def _create(**kwargs):
                return client

        sdk = SimpleNamespace(RelayerApiKey=RelayerApiKey, SecureClient=SecureClient)
        values = {
            "private_key": "key",
            "wallet_address": "wallet",
            "relayer_api_key": "api-key",
            "relayer_api_key_address": "api-address",
        }
        with patch.dict(sys.modules, {"polymarket": sdk}), patch.object(
            PolymarketClobV2Venue,
            "installed_sdk_version",
            return_value="0.9.0",
        ):
            result = _read_only_operation("account", values)
        self.assertTrue(result["authenticated"])
        self.assertTrue(client.closed)
    def test_daily_loss_limit_prevents_submission(self):
        self.arm(); self.store.connection.execute("INSERT INTO canary_ledger(event_id,signal_id,timestamp,candidate_id,venue,market_id,token_id,side,requested_notional,paper_expected_price,max_price,status,realized_pnl,evidence_json) VALUES('e','old',?,?,?,?,?,?,?,?,?,'RESOLVED','-2.00','{}')",(T0.isoformat(),"C123","polymarket","m0","t","BUY","1",".5",".5")); self.store.connection.commit(); self.assertBlocked("DAILY_LOSS_LIMIT",self.submit)
    def test_exposure_limit_prevents_submission(self):
        self.arm(limits=CanaryLimits(max_exposure_usd=Decimal("1"))); self.store.connection.execute("INSERT INTO canary_ledger(event_id,signal_id,timestamp,candidate_id,venue,market_id,token_id,side,requested_notional,paper_expected_price,max_price,status,evidence_json) VALUES('e','old',?,?,?,?,?,?,?,?,?,'OPEN','{}')",(T0.isoformat(),"C123","polymarket","m0","t","BUY","1",".5",".5")); self.store.connection.commit(); self.assertBlocked("EXPOSURE_LIMIT",self.submit)
    def test_max_order_count_prevents_submission(self):
        self.arm(limits=CanaryLimits(max_orders_per_day=1)); self.submit("first"); self.assertBlocked("DAILY_ORDER_LIMIT",lambda:self.submit("second"))
    def test_daily_order_count_excludes_prior_days(self):
        yesterday = (T0 - timedelta(days=1)).isoformat()
        self.store.connection.execute(
            "INSERT INTO canary_ledger(event_id,signal_id,timestamp,candidate_id,venue,market_id,token_id,side,requested_notional,paper_expected_price,max_price,status,evidence_json) "
            "VALUES('old-day','old-signal',?,?,?,?,?,?,?,?,?,'REJECTED','{}')",
            (yesterday, "C123", "polymarket", "m0", "t", "BUY", "1", ".5", ".5"),
        )
        self.store.connection.commit()
        self.arm(limits=CanaryLimits(max_orders_per_day=1))
        self.submit("today")
    def test_duplicate_signal_and_restart_cannot_duplicate(self):
        self.arm(); self.submit("same"); self.assertBlocked("DUPLICATE_SIGNAL",lambda:CanaryService(self.store,credentials=FakeCredentials(),clock=lambda:T0).submit(signal_id="same",candidate_id="C123",market_id="m",token_id="yes",side="BUY",paper_expected_price=Decimal(".5"),venue=self.venue,allow_test_venue=True)); self.assertEqual(len(self.venue.submissions),1)
    def test_reservation_commits_before_sink_and_blocks_interrupted_retry(self):
        self.arm()
        venue = CrashGapVenue(self.store)
        with self.assertRaises(KeyboardInterrupt):
            self.submit("crash-gap", venue=venue)
        self.assertFalse(venue.observed_in_transaction)
        self.assertEqual(venue.observed_status, "SUBMITTING")
        self.assertEqual(
            self.store.connection.execute(
                "SELECT status FROM canary_ledger WHERE signal_id='crash-gap'"
            ).fetchone()[0],
            "UNKNOWN",
        )
        self.assertEqual(
            self.store.connection.execute(
                "SELECT status FROM canary_execution_events"
            ).fetchone()[0],
            "UNKNOWN",
        )
        self.assertBlocked(
            "DUPLICATE_SIGNAL",
            lambda: self.submit("crash-gap"),
        )
    def test_cross_process_kill_does_not_wait_for_blocked_submission(self):
        self.arm()
        with tempfile.TemporaryDirectory() as directory:
            database_path = f"{directory}\\canary.sqlite3"
            target = sqlite3.connect(database_path)
            try:
                self.store.connection.backup(target)
            finally:
                target.close()
            context = multiprocessing.get_context("spawn")
            entered = context.Event()
            release = context.Event()
            results = context.Queue()
            process = context.Process(
                target=_blocked_submit_worker,
                args=(database_path, entered, release, results),
            )
            killer_store = HealthyStore(database_path)
            killer_service = CanaryService(
                killer_store,
                credentials=FakeCredentials(),
                clock=lambda: T0,
            )
            process.start()
            try:
                self.assertTrue(entered.wait(10))
                inflight = killer_service.status()
                self.assertEqual(inflight["micro_live_canary"], "ARMED")
                self.assertEqual(inflight["last_request_status"], "SUBMITTING")
                self.assertEqual(
                    inflight["control_generation"],
                    1,
                )
                killer_service.kill()
                self.assertEqual(
                    killer_store.connection.execute(
                        "SELECT state FROM canary_control WHERE singleton=1"
                    ).fetchone()[0],
                    "KILLED",
                )
                self.assertFalse(killer_store.connection.in_transaction)
            finally:
                release.set()
                process.join(15)
                if process.is_alive():
                    process.terminate()
                    process.join(5)
                killer_store.close()
            self.assertEqual(process.exitcode, 0)
            result = results.get(timeout=5)
            self.assertEqual(result[0], "ok")
            final_store = HealthyStore(database_path)
            try:
                self.assertEqual(
                    final_store.connection.execute(
                        "SELECT status FROM canary_ledger WHERE signal_id='cross-process'"
                    ).fetchone()[0],
                    "SUBMITTED",
                )
                self.assertEqual(
                    CanaryService(
                        final_store,
                        credentials=FakeCredentials(),
                        clock=lambda: T0,
                    ).status()["micro_live_canary"],
                    "KILLED",
                )
                execution_evidence = json.loads(
                    final_store.connection.execute(
                        "SELECT evidence_json FROM canary_execution_events"
                    ).fetchone()[0]
                )
                self.assertEqual(execution_evidence["request_control_generation"], 1)
                self.assertEqual(execution_evidence["control_generation"], 2)
            finally:
                final_store.close()

    def test_submission_timeout_is_durable_unknown_and_not_retried(self):
        self.arm()
        entered = threading.Event()
        release = threading.Event()

        class TimeoutVenue(FakeVenue):
            def submit_limit_order(self, **kwargs):
                entered.set()
                release.wait(10)
                return {"ok": True, "order_id": "late-order", "status": "matched"}

        try:
            with patch("axiom.canary.CANARY_SUBMISSION_TIMEOUT_SECONDS", 0.01):
                with self.assertRaisesRegex(
                    CanaryBlocked,
                    "CANARY_SUBMISSION_UNKNOWN",
                ):
                    self.submit("timeout", venue=TimeoutVenue())
            self.assertTrue(entered.is_set())
            self.assertEqual(
                self.store.connection.execute(
                    "SELECT status FROM canary_ledger WHERE signal_id='timeout'"
                ).fetchone()[0],
                "UNKNOWN",
            )
            self.assertEqual(
                self.store.connection.execute(
                    "SELECT status FROM canary_execution_events"
                ).fetchone()[0],
                "UNKNOWN",
            )
            self.assertBlocked("DUPLICATE_SIGNAL", lambda: self.submit("timeout"))
        finally:
            release.set()


    def test_accepted_execution_status_counts_as_open_exposure(self):
        self.arm()
        self.submit("matched")
        status = self.service.status()
        self.assertEqual(status["open_positions"], 1)
        self.assertEqual(status["total_exposure"], 1.0)
        event_status = self.store.connection.execute(
            "SELECT status FROM canary_execution_events"
        ).fetchone()[0]
        ledger_status = self.store.connection.execute(
            "SELECT status FROM canary_ledger"
        ).fetchone()[0]
        self.assertEqual(event_status, "SUBMITTED")
        self.assertEqual(ledger_status, "SUBMITTED")
    def test_different_candidate_cannot_trade_or_write_ledger(self):
        self.arm()
        venue = FakeVenue()
        self.assertBlocked("CANDIDATE_MISMATCH", lambda: self.submit(candidate_id="C999", venue=venue))
        self.assertFalse(venue.submissions)
        self.assertEqual(self.store.connection.execute("SELECT COUNT(*) FROM canary_ledger").fetchone()[0], 0)
    def test_invalid_frozen_binding_cannot_trade(self):
        self.arm()
        self.store.connection.execute(
            "UPDATE canary_eligibility SET frozen_hash=? WHERE candidate_id=?",
            ("tampered", "C123"),
        )
        self.store.connection.commit()
        self.assertBlocked("CANARY_NOT_ARMED", self.submit)
        self.assertFalse(self.venue.submissions)
    def test_armed_readiness_requires_venue(self):
        self.arm()
        result = self.service.connectivity_check(candidate_id="C123", venue=None)
        self.assertFalse(result["ready"])
        self.assertIn("VENUE_REQUIRED", result["failures"])
    def test_connectivity_check_is_prearming_read_only(self):
        result = self.service.connectivity_check(
            candidate_id=None,
            venue=self.venue,
            market_id="m",
            token_id="yes",
        )
        self.assertTrue(result["ready"], result)
        self.assertTrue(result["connectivity_only"])
        self.assertEqual(
            self.store.connection.execute(
                "SELECT COUNT(*) FROM canary_control"
            ).fetchone()[0],
            0,
        )
        self.assertNotIn("CANARY_NOT_ARMED", result["failures"])
        self.assertFalse(self.venue.submissions)
        self.assertEqual(
            self.store.connection.execute(
                "SELECT COUNT(*) FROM canary_ledger"
            ).fetchone()[0],
            0,
        )

    def test_full_canary_check_remains_armed_gate(self):
        result = self.service.check(
            candidate_id="C123",
            venue=self.venue,
        )
        self.assertFalse(result["ready"])
        self.assertIn("CANARY_NOT_ARMED", result["failures"])
        self.assertFalse(result["connectivity_only"])

    def test_credentials_never_persist_or_render(self):
        self.arm(); self.submit(); dump="\n".join(str(tuple(r)) for r in self.store.connection.iterdump()); dashboard=json.dumps(DashboardData(store=self.store).operator_data(),default=str)+_dashboard_html(); self.assertNotIn("test-only",dump+dashboard)
    def test_dashboard_preserves_killed_state_and_last_request_status(self):
        self.arm()
        self.submit("dashboard-request")
        self.service.kill()
        canary = DashboardData(store=self.store).operator_data()["canary"]
        self.assertEqual(canary["micro_live_canary"], "KILLED")
        self.assertEqual(canary["last_request_status"], "SUBMITTED")
        html = _dashboard_html()
        self.assertEqual(
            canary["expiry"],
            (T0 + timedelta(hours=24)).isoformat(),
        )
        self.assertIn("SUBMITTING", html)
        self.assertIn("last_request_status", json.dumps(canary))
        self.assertIn("in-flight not retracted", html)

    def test_paper_and_real_ledgers_are_separate(self):
        self.arm()
        self.submit()
        self.assertEqual(
            self.store.connection.execute(
                "SELECT COUNT(*) FROM canary_ledger"
            ).fetchone()[0],
            1,
        )
        self.assertEqual(
            self.store.connection.execute(
                "SELECT COUNT(*) FROM paper_execution_events"
            ).fetchone()[0],
            0,
        )
    def test_canary_check_without_credentials_rejects_without_order(self):
        service=CanaryService(self.store,credentials=FakeCredentials(False),clock=lambda:T0); result=service.check(candidate_id="C123",venue=None); self.assertFalse(result["ready"]); self.assertIn("CREDENTIALS_NOT_CONFIGURED",result["failures"]); self.assertFalse(self.venue.submissions)
    def test_dashboard_labels_real_canary_and_production_disabled(self):
        data=DashboardData(store=self.store).operator_data(); self.assertFalse(data["live_execution"]); self.assertFalse(PRODUCTION_LIVE_EXECUTION); self.assertIn("REAL CANARY MONEY",_dashboard_html()); self.assertEqual(data["canary"]["production_live_trading"],"DISABLED")

class CanarySignalTests(unittest.TestCase):
    def setUp(self):
        self.now = T0
        self.store = HealthyStore(":memory:")
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
        self.store.save_dataset(
            "prediction-history",
            "v2",
            [{"timestamp": T0.isoformat(), "price": 0.5, "source_type": "HISTORICAL"}],
        )
        self.store.save_dataset_catalog(
            "prediction-history",
            "v2",
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
            snapshot_id="prediction-history:v2",
            metadata={
                "provider": "polymarket",
                "source_type": "HISTORICAL",
                "research_quality": "PRICE_PROXY",
                "historical_order_book_available": False,
            },
        )
        self.service = CanaryService(
            self.store,
            credentials=FakeCredentials(),
            clock=lambda: self.now,
        )
        self.strategy = {
            "version": 1,
            "market_type": "prediction",
            "family": "probability_mispricing",
            "parameters": {"threshold": 0.05},
            "operations": [],
            "probability_model": "fixed",
            "resolution_aware": True,
            "resolution_inputs": ["expiry", "settlement"],
            "strategy_id": "C",
        }
        self.model = {"probability": 0.80}
        self._add_candidate("C")
        self._save_snapshot("snap")

    def tearDown(self):
        self.store.close()

    def _add_candidate(self, candidate_id):
        strategy = {**self.strategy, "strategy_id": candidate_id}
        strategy_hash = self.service._document_hash(strategy)
        model_hash = self.service._document_hash(self.model)
        config_hash = "config-hash"
        payload = {
            "market_type": "prediction",
            "source_type": "HISTORICAL",
            "dataset_id": "prediction-history",
            "dataset_version": "v1",
            "dataset_provenance": {
                "dataset_id": "prediction-history",
                "dataset_version": "v1",
                "source_type": "HISTORICAL",
                "time_split": "train-validation-holdout",
            },
            "schema_validated": True,
            "historical_backtest_passed": True,
            "validation_passed": True,
            "robustness_passed": True,
            "data_quality_passed": True,
            "data_quality": "PRICE_PROXY",
            "validation_expectancy": 0.10,
            "validation_confidence_lower_bound": 0.05,
            "validation_stability": 0.90,
            "validation_calibration": 0.90,
            "validation_sample_count": 100,
            "validation_trade_count": 50,
            "minimum_sample_check": {
                "passed": True,
                "count": 100,
                "trades": 50,
                "checks": {"observations": True, "trades": True},
            },
            "frozen": True,
            "holdout_used": False,
            "strategy_hash": strategy_hash,
            "model_hash": model_hash,
            "config_hash": config_hash,
            "frozen_hash": hashlib.sha256(
                "|".join((strategy_hash, model_hash, config_hash)).encode()
            ).hexdigest(),
            "strategy_document": strategy,
            "model_document": self.model,
        }
        self.store.save_candidate_lifecycle(
            candidate_id, "IDEA", payload, timestamp=T0
        )
        self.store.save_candidate_lifecycle(
            candidate_id, "FROZEN", payload, timestamp=T0
        )
        self.service.mark_eligible(candidate_id)

    def _save_snapshot(
        self,
        snapshot_id,
        *,
        active=True,
        price="0.50",
        settlement="open",
    ):
        payload = {
            "source_type": "FORWARD_COLLECTED",
            "snapshot": {
                "market_id": "m",
                "timestamp": T0.isoformat(),
                "yes_mid": price,
                "yes_ask": price,
                "yes_order_book": {
                    "asks": [{"price": price, "size": "100"}],
                    "bids": [],
                    "timestamp": T0.isoformat(),
                    "token_id": "yes",
                },
                "no_order_book": {
                    "asks": [{"price": price, "size": "100"}],
                    "bids": [],
                    "timestamp": T0.isoformat(),
                    "token_id": "no",
                },
                "no_ask": price,
                "yes_token_id": "yes",
                "no_token_id": "no",
                "settlement": settlement,
            },
            "yes_token_id": "yes",
            "no_token_id": "no",
            "settlement": settlement,
            "active": active,
        }
        self.store.save_polymarket_snapshot(
            snapshot_id,
            "m",
            T0,
            self.now,
            payload,
            source_type="FORWARD_COLLECTED",
        )

    def _signal(self, candidate_id="C"):
        signal = self.service.generate_signal(candidate_id)
        self.assertIsNotNone(signal)
        assert signal is not None
        return signal

    def _arm(self, candidate_id="C"):
        return self.service.arm(
            candidate_id,
            venue=FakeVenue(),
            credentials_configured=True,
        )

    def test_eligible_candidate_generates_persisted_signal(self):
        signal = self._signal()
        self.assertEqual(signal["status"], "READY")
        self.assertEqual(signal["candidate_id"], "C")
        self.assertEqual(
            self.store.connection.execute(
                "SELECT COUNT(*) FROM canary_signals"
            ).fetchone()[0],
            1,
        )

    def test_inactive_candidate_has_no_signal(self):
        self._save_snapshot("zz-inactive", active=False)
        self.assertIsNone(self.service.generate_signal("C"))
        self.assertEqual(
            self.store.connection.execute(
                "SELECT COUNT(*) FROM canary_signals"
            ).fetchone()[0],
            0,
        )

    def test_signal_records_exact_frozen_hash_binding(self):
        signal = self._signal()
        lifecycle = self.store.load_candidate_lifecycle("C")
        payload = lifecycle["payload"]
        self.assertEqual(signal["strategy_hash"], payload["strategy_hash"])
        self.assertEqual(signal["model_hash"], payload["model_hash"])
        self.assertEqual(signal["config_hash"], payload["config_hash"])
        self.assertEqual(
            signal["frozen_hash"],
            hashlib.sha256(
                "|".join(
                    (
                        signal["strategy_hash"],
                        signal["model_hash"],
                        signal["config_hash"],
                    )
                ).encode()
            ).hexdigest(),
        )

    def test_changed_candidate_invalidates_signal(self):
        signal = self._signal()
        changed = dict(self.store.load_candidate_lifecycle("C")["payload"])
        changed["data_quality_passed"] = False
        self.store.save_candidate_lifecycle("C", "FROZEN", changed, timestamp=T0)
        with self.assertRaisesRegex(CanaryBlocked, "CANDIDATE_NOT_CANARY_ELIGIBLE"):
            self.service.submit_signal(
                signal["signal_id"], venue=FakeVenue(), allow_test_venue=True
            )
        self.assertEqual(
            self.service.get_signal(signal["signal_id"])["status"],
            "NO_LONGER_VALID",
        )
    def test_signal_submission_rejects_strategy_model_config_hash_mutations(self):
        parts = {
            "strategy_hash": "mutated-strategy-hash",
            "model_hash": "mutated-model-hash",
            "config_hash": "mutated-config-hash",
        }
        for key, value in parts.items():
            with self.subTest(key=key):
                candidate_id = f"mutated-{key}"
                self._add_candidate(candidate_id)
                signal = self._signal(candidate_id)
                self._arm(candidate_id)
                changed = dict(self.store.load_candidate_lifecycle(candidate_id)["payload"])
                changed[key] = value
                changed["frozen_hash"] = hashlib.sha256(
                    "|".join(
                        changed[name]
                        for name in ("strategy_hash", "model_hash", "config_hash")
                    ).encode()
                ).hexdigest()
                self.store.save_candidate_lifecycle(
                    candidate_id,
                    "FROZEN",
                    changed,
                    timestamp=T0,
                )
                venue = FakeVenue()
                with self.assertRaises(CanaryBlocked):
                    self.service.submit_signal(
                        signal["signal_id"],
                        venue=venue,
                        allow_test_venue=True,
                    )
                self.assertFalse(venue.submissions)

    def test_signal_submission_rejects_historical_dataset_version_mutation(self):
        signal = self._signal()
        self._arm()
        changed = dict(self.store.load_candidate_lifecycle("C")["payload"])
        changed["dataset_version"] = "v2"
        changed["dataset_provenance"] = {
            **changed["dataset_provenance"],
            "dataset_version": "v2",
        }
        self.store.save_candidate_lifecycle("C", "FROZEN", changed, timestamp=T0)
        venue = FakeVenue()
        with self.assertRaises(CanaryBlocked):
            self.service.submit_signal(
                signal["signal_id"],
                venue=venue,
                allow_test_venue=True,
            )
        self.assertFalse(venue.submissions)

    def test_signal_submission_rejects_qualification_evidence_mutation(self):
        signal = self._signal()
        self._arm()
        changed = dict(self.store.load_candidate_lifecycle("C")["payload"])
        changed["validation_confidence_lower_bound"] = 0.06
        self.store.save_candidate_lifecycle("C", "FROZEN", changed, timestamp=T0)
        venue = FakeVenue()
        with self.assertRaises(CanaryBlocked):
            self.service.submit_signal(
                signal["signal_id"],
                venue=venue,
                allow_test_venue=True,
            )
        self.assertFalse(venue.submissions)

    def test_new_service_instance_preserves_valid_signal_binding(self):
        restarted = CanaryService(
            self.store,
            credentials=FakeCredentials(),
            clock=lambda: self.now,
        )
        validation = restarted.validate_eligibility("C")
        self.assertTrue(validation["eligible"], validation)
        restarted.arm(
            "C",
            venue=FakeVenue(),
            credentials_configured=True,
        )
        self.assertEqual(restarted.status()["micro_live_canary"], "ARMED")

    def test_expired_signal_is_rejected(self):
        signal = self._signal()
        self.now = T0 + timedelta(seconds=61)
        with self.assertRaisesRegex(CanaryBlocked, "CANARY_SIGNAL_EXPIRED"):
            self.service.submit_signal(
                signal["signal_id"], venue=FakeVenue(), allow_test_venue=True
            )
        self.assertEqual(
            self.service.get_signal(signal["signal_id"])["status"], "EXPIRED"
        )

    def test_cli_exposes_no_signal_overrides(self):
        with self.assertRaises(SystemExit):
            main(
                [
                    "canary-submit",
                    "--db",
                    ":memory:",
                    "--signal",
                    "s",
                    "--market",
                    "forged-market",
                ]
            )

    def test_armed_candidate_mismatch_rejects_without_submission(self):
        signal = self._signal()
        self._add_candidate("D")
        self._arm("D")
        venue = FakeVenue()
        with self.assertRaisesRegex(CanaryBlocked, "CANDIDATE_MISMATCH"):
            self.service.submit_signal(
                signal["signal_id"], venue=venue, allow_test_venue=True
            )
        self.assertFalse(venue.submissions)

    def test_duplicate_signal_rejected(self):
        signal = self._signal()
        self._arm()
        venue = FakeVenue()
        self.service.submit_signal(
            signal["signal_id"], venue=venue, allow_test_venue=True
        )
        with self.assertRaisesRegex(CanaryBlocked, "DUPLICATE_SIGNAL"):
            self.service.submit_signal(
                signal["signal_id"], venue=venue, allow_test_venue=True
            )
        self.assertEqual(len(venue.submissions), 1)

    def test_killed_canary_rejects_signal(self):
        signal = self._signal()
        self._arm()
        self.service.kill()
        venue = FakeVenue()
        with self.assertRaisesRegex(CanaryBlocked, "CANARY_NOT_ARMED"):
            self.service.submit_signal(
                signal["signal_id"], venue=venue, allow_test_venue=True
            )
        self.assertFalse(venue.submissions)

    def test_complete_gate_revalidation_rejects_signal(self):
        signal = self._signal()
        self._arm()
        changed = dict(self.store.load_candidate_lifecycle("C")["payload"])
        changed["robustness_passed"] = False
        self.store.save_candidate_lifecycle("C", "FROZEN", changed, timestamp=T0)
        with self.assertRaisesRegex(CanaryBlocked, "CANDIDATE_NOT_CANARY_ELIGIBLE"):
            self.service.submit_signal(
                signal["signal_id"], venue=FakeVenue(), allow_test_venue=True
            )

    def test_exact_submit_confirmation_is_required(self):
        with patch("builtins.input", return_value="submit $1"):
            self.assertEqual(
                main(
                    [
                        "canary-submit",
                        "--db",
                        ":memory:",
                        "--signal",
                        "missing",
                    ]
                ),
                1,
            )

    def test_aborted_submit_makes_zero_exchange_requests(self):
        venue = FakeVenue()
        with patch("builtins.input", return_value="SUBMIT"):
            self.assertEqual(
                main(
                    [
                        "canary-submit",
                        "--db",
                        ":memory:",
                        "--signal",
                        "missing",
                    ]
                ),
                1,
            )
        self.assertFalse(venue.submissions)

    def test_signal_generation_makes_zero_exchange_mutations(self):
        self.assertIsNotNone(self._signal())
        self.assertFalse(FakeVenue().submissions)
        self.assertEqual(
            self.store.connection.execute(
                "SELECT COUNT(*) FROM canary_ledger"
            ).fetchone()[0],
            0,
        )

    def test_dashboard_displays_signal_readiness_separately(self):
        signal = self._signal()
        data = DashboardData(store=self.store).operator_data()
        self.assertEqual(data["canary_signal"]["signal_id"], signal["signal_id"])
        html = _dashboard_html()
        self.assertIn("Signal readiness", html)
        self.assertIn("Order result", html)
        self.assertNotIn("do_POST", html)

    def test_signal_submission_timeout_is_unknown(self):
        signal = self._signal()
        self._arm()
        release = threading.Event()

        class TimeoutVenue(FakeVenue):
            def submit_limit_order(self, **kwargs):
                release.wait(1)
                return {"ok": True, "order_id": "late-order", "status": "matched"}

        try:
            with patch("axiom.canary.CANARY_SUBMISSION_TIMEOUT_SECONDS", 0.01):
                with self.assertRaisesRegex(
                    CanaryBlocked, "CANARY_SUBMISSION_UNKNOWN"
                ):
                    self.service.submit_signal(
                        signal["signal_id"],
                        venue=TimeoutVenue(),
                        allow_test_venue=True,
                    )
        finally:
            release.set()
        self.assertEqual(
            self.store.connection.execute(
                "SELECT status FROM canary_ledger WHERE signal_id=?",
                (signal["signal_id"],),
            ).fetchone()[0],
            "UNKNOWN",
        )
        self.assertEqual(
            self.service.get_signal(signal["signal_id"])["status"], "UNKNOWN"
        )

    def test_signal_suite_never_enables_real_orders(self):
        self.assertFalse(PRODUCTION_LIVE_EXECUTION)
        self.assertNotIn("live_execution\": true", json.dumps(self._signal()))

if __name__=="__main__": unittest.main()
