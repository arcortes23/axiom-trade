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
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from axiom.canary import CanaryBlocked, CanaryLimits, CanaryService, CredentialStore, PolymarketClobV2Venue, PRODUCTION_LIVE_EXECUTION
from axiom.cli import main
from axiom.dashboard import DashboardData, _dashboard_html
from axiom.storage import AxiomStore

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
        self.store=HealthyStore(":memory:"); self.service=CanaryService(self.store,credentials=FakeCredentials(),clock=lambda:T0); self.venue=FakeVenue()
        hash_parts=("strategy-v1","model-v1","config-v1")
        payload={
            "schema_validated":True,
            "historical_backtest_passed":True,
            "validation_passed":True,
            "robustness_passed":True,
            "data_quality_passed":True,
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

    def test_hermes_has_no_canary_execution_fields(self):
        from axiom.director import validate_hermes_proposal
        result=validate_hermes_proposal({"proposal_id":"x","statement":"x","source":"x","tests":["x"],"dataset_version":"v","time_split":"train-validation-holdout","paper_only":True,"canary_arm":True})
        self.assertFalse(result.accepted)
    def test_ineligible_candidate_cannot_arm(self): self.assertBlocked("NOT_CANARY_ELIGIBLE",lambda:self.service.arm("other",venue=self.venue,credentials_configured=True))
    def test_frozen_and_paper_forward_stages_can_mark_eligible_without_promotion(self):
        template = dict(self.store.load_candidate_lifecycle("C123")["payload"])
        template.pop("forward_evidence", None)
        progression = (
            "SCHEMA_VALIDATED",
            "BACKTESTED",
            "VALIDATED",
            "ROBUSTNESS_CHECKED",
            "FROZEN",
            "PAPER_FORWARD",
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
            self.assertEqual(row["frozen_hash"], template["frozen_hash"])
            self.assertEqual(json.loads(row["evidence_json"]), lifecycle["payload"])

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

    def test_lifecycle_evidence_change_invalidates_existing_eligibility(self):
        payload = dict(self.store.load_candidate_lifecycle("C123")["payload"])
        payload["forward_evidence"] = {
            **payload["forward_evidence"],
            "observation_marker": "changed-after-eligibility",
        }
        self.store.save_candidate_lifecycle("C123", "PAPER_PROMOTABLE", payload, timestamp=T0)
        self.assertBlocked(
            "CANDIDATE_NOT_CANARY_ELIGIBLE",
            lambda: self.service.arm("C123", venue=self.venue, credentials_configured=True),
        )

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

if __name__=="__main__": unittest.main()
