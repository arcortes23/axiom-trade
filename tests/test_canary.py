from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from dataclasses import replace
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
import axiom.canary as canary_module

from axiom.lifecycle import CandidateLifecycleManager, CandidateStage
from axiom.experiment_plan import normalize_market_scope
from axiom.market_scope import MarketScopeResolution, resolve_market_scope
from axiom.auto_canary import AutonomousCanaryWorker
from axiom.ranker import CandidateCanaryRanker
from axiom.canary import (
    AUTONOMOUS_CANARY_LIMITS,
    CANARY_EXECUTION_MARKET_CAP,
    CanaryBlocked,
    CanaryLimits,
    CanaryService,
    CredentialStore,
    EXECUTION_FEASIBILITY_MARKET_CAP,
    PolymarketClobV2Venue,
    PRODUCTION_LIVE_EXECUTION,
    _canary_lifecycle_snapshot_hashes,
    _canary_lifecycle_evidence_class,
)
from axiom.cli import main
from axiom.dashboard import DashboardData, _dashboard_html
from axiom.storage import AxiomStore, SQLiteBusyTimeout

T0=datetime(2026,1,2,12,tzinfo=timezone.utc)

def _persist_test_scope(
    store,
    candidate_id,
    payload,
    market_ids=("m",),
    *,
    now=T0,
    market_records=None,
):
    """Bind a fixture to one immutable current-market scope resolution.

    ``market_records`` lets diagnostic fixtures model the resolver's exact
    matched/excluded/deferred universe without allowing the canary to infer
    authority from snapshots or market metadata.
    """
    ids = [str(item).strip() for item in market_ids if str(item).strip()]
    source = payload.get("experiment_plan")
    source = source if isinstance(source, dict) else payload
    filters = source.get("filters", payload.get("frozen_filters", {}))
    target = source.get("target") if isinstance(source, dict) else None
    target_ids = None
    if isinstance(target, dict):
        target_ids = target.get("market_ids", target.get("exact_market_ids"))
    policy = normalize_market_scope(
        market_ids=target_ids if target_ids is not None else ids,
        filters=filters if isinstance(filters, dict) else {},
    )
    policy = normalize_market_scope(
        {**policy.as_dict(), "provenance": "canonical"}
    )
    payload["market_scope"] = policy.as_dict()
    payload["market_scope_hash"] = policy.scope_hash
    payload["market_scope_version"] = policy.scope_version
    payload.setdefault("plan_hash", "sha256:test-plan")
    payload.setdefault(
        "dataset_selector",
        {
            "dataset_id": payload.get("dataset_id", "prediction-history"),
            "dataset_version": payload.get("dataset_version", "v1"),
            "source_type": "HISTORICAL",
        },
    )
    payload.setdefault(
        "dataset_attestation",
        {
            "status": "CURRENT",
            "hash": "sha256:test-dataset",
        },
    )

    supplied = market_records if isinstance(market_records, dict) else {}
    records = []
    for market_id in ids:
        record = supplied.get(market_id, {})
        if record is None:
            continue
        record = dict(record) if isinstance(record, dict) else {}
        records.append(
            {
                "market_id": market_id,
                "condition_id": f"condition-{market_id}",
                "yes_token_id": "yes",
                "no_token_id": "no",
                "instrument": "POLYMARKET",
                "venue": "POLYMARKET",
                "source_type": "CURRENT",
                "active": True,
                "open": True,
                "closed": False,
                "settlement": "open",
                "accepting_orders": True,
                "enable_order_book": True,
                "metadata": {"category": "politics"},
                "metadata_provenance": {
                    "source_type": "CURRENT",
                    "metadata_hash": f"sha256:{market_id}",
                },
                **record,
            }
        )
    resolution = resolve_market_scope(
        str(candidate_id),
        {"market_scope": payload["market_scope"]},
        records,
        resolved_at=now,
    )
    store.save_market_scope_resolution(resolution)
    return resolution

class HealthyStore(AxiomStore):
    def polymarket_health(self, **kwargs):
        return {"grade":"A","errors":0}
class BlockedRequiredHealthStore(HealthyStore):
    def polymarket_required_health(self, **kwargs):
        result = super().polymarket_required_health(**kwargs)
        result.update({"grade": "C", "reason_code": "REQUIRED_MARKETS_STALE"})
        return result


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
        _persist_test_scope(self.store, "C123", payload, ("m",))
        self.store.save_candidate_lifecycle("C123","IDEA",payload,timestamp=T0)
        for stage in ("SCHEMA_VALIDATED","BACKTESTED","VALIDATED","ROBUSTNESS_CHECKED","FROZEN","PAPER_FORWARD","PAPER_PROMOTABLE"):
            self.store.save_candidate_lifecycle("C123",stage,payload,timestamp=T0)
        self.service.mark_eligible("C123")
    def _ensure_direct_signal(self, signal_id, candidate_id="C123"):
        if candidate_id != "C123":
            return
        existing = self.store.connection.execute(
            "SELECT 1 FROM canary_signals WHERE signal_id=?", (signal_id,)
        ).fetchone()
        if existing is not None:
            return
        lifecycle = self.store.load_candidate_lifecycle(candidate_id)
        payload = lifecycle["payload"]
        evidence = {
            "current_execution_evidence": "CURRENT_ORDER_BOOK",
            "current_order_book_timestamp": T0.isoformat(),
            "scope_hash": payload["market_scope_hash"],
            "scope_version": payload["market_scope_version"],
        }
        self.store.connection.execute(
            "INSERT INTO canary_signals("
            "signal_id,candidate_id,frozen_hash,strategy_hash,model_hash,config_hash,"
            "market_id,token_id,outcome,side,paper_expected_price,source_snapshot_id,"
            "source_timestamp,generated_at,expires_at,status,reason,evidence_json,updated_at)"
            " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                signal_id,
                candidate_id,
                payload["frozen_hash"],
                payload["strategy_hash"],
                payload["model_hash"],
                payload["config_hash"],
                "m",
                "yes",
                "yes",
                "BUY",
                "0.50",
                "direct-test",
                T0.isoformat(),
                T0.isoformat(),
                (T0 + timedelta(seconds=60)).isoformat(),
                "READY",
                None,
                json.dumps(evidence, sort_keys=True),
                T0.isoformat(),
            ),
        )
        self.store.connection.commit()
    def _expand_direct_scope_beyond_execution_cap(self):
        resolution = self.store.load_market_scope_resolution("C123")
        self.assertIsNotNone(resolution)
        assert resolution is not None
        first = resolution.matched_markets[0]
        overflow = tuple(
            replace(
                first,
                market_id=f"direct-overflow-{index}",
                condition_id=f"direct-overflow-condition-{index}",
                yes_token_id=f"direct-overflow-yes-{index}",
                no_token_id=f"direct-overflow-no-{index}",
            )
            for index in range(CANARY_EXECUTION_MARKET_CAP)
        )
        expanded_at = T0 + timedelta(microseconds=1)
        self.store.save_market_scope_resolution(
            replace(
                resolution,
                resolved_at=expanded_at,
                matched_markets=(*resolution.matched_markets, *overflow),
                resolution_id="",
            )
        )
        self.service.clock = lambda: expanded_at

    def test_direct_submit_rejects_over_cap_scope_before_venue(self):
        self.arm()
        self._ensure_direct_signal("direct-over-cap")
        self._expand_direct_scope_beyond_execution_cap()
        venue = FakeVenue()
        self.assertBlocked(
            EXECUTION_FEASIBILITY_MARKET_CAP,
            lambda: self.submit("direct-over-cap", venue=venue),
        )
        self.assertFalse(venue.submissions)
        signal = self.service.get_signal("direct-over-cap")
        self.assertEqual(signal["status"], "NO_LONGER_VALID")
        self.assertEqual(signal["reason"], EXECUTION_FEASIBILITY_MARKET_CAP)
        self.assertEqual(
            signal["evidence"]["execution_market_cap"],
            CANARY_EXECUTION_MARKET_CAP,
        )

    def test_direct_submit_scope_expansion_after_reservation_blocks_sink(self):
        self.arm()
        venue = FakeVenue()
        original_publish = self.service.publish_readiness_snapshot

        def expand_before_sink(*, reason):
            if reason == "CANARY_SUBMITTING":
                self._expand_direct_scope_beyond_execution_cap()
            return original_publish(reason=reason)

        self.service.publish_readiness_snapshot = expand_before_sink
        self.assertBlocked(
            EXECUTION_FEASIBILITY_MARKET_CAP,
            lambda: self.submit("direct-toctou-cap", venue=venue),
        )
        self.assertFalse(venue.submissions)
        signal = self.service.get_signal("direct-toctou-cap")
        self.assertEqual(signal["status"], "NO_LONGER_VALID")
        self.assertEqual(signal["reason"], EXECUTION_FEASIBILITY_MARKET_CAP)
        self.assertEqual(
            signal["evidence"]["resolved_market_count"],
            CANARY_EXECUTION_MARKET_CAP + 1,
        )
    def _assert_direct_expiry_rejected(self, signal_id, expires_at):
        self.arm()
        self._ensure_direct_signal(signal_id)
        self.store.connection.execute(
            "UPDATE canary_signals SET expires_at=? WHERE signal_id=?",
            (expires_at.isoformat(), signal_id),
        )
        self.store.connection.commit()
        venue = FakeVenue()
        self.assertBlocked(
            "CANARY_SIGNAL_EXPIRED",
            lambda: self.submit(signal_id, venue=venue),
        )
        self.assertFalse(venue.submissions)
        signal = self.service.get_signal(signal_id)
        self.assertEqual(signal["status"], "REJECTED")
        self.assertEqual(signal["reason"], "CANARY_SIGNAL_EXPIRED")

    def test_direct_submit_rejects_expired_signal_before_sink(self):
        self._assert_direct_expiry_rejected(
            "direct-expired",
            T0 - timedelta(microseconds=1),
        )

    def test_direct_submit_rejects_signal_at_exact_expiry_boundary(self):
        self._assert_direct_expiry_rejected("direct-expiry-boundary", T0)

    def test_test_venue_expiry_crossing_after_reservation_rejects_before_sink(self):
        self.arm()
        signal_id = "direct-expiry-after-reservation"
        venue = FakeVenue()
        original_publish = self.service.publish_readiness_snapshot

        def expire_after_reservation(*, reason):
            if reason == "CANARY_SUBMITTING":
                self.service.clock = lambda: T0 + timedelta(seconds=60)
            return original_publish(reason=reason)

        with patch.object(
            self.service,
            "publish_readiness_snapshot",
            side_effect=expire_after_reservation,
        ):
            self.assertBlocked(
                "CANARY_SIGNAL_EXPIRED",
                lambda: self.submit(signal_id, venue=venue),
            )
        self.assertFalse(venue.submissions)
        signal = self.service.get_signal(signal_id)
        self.assertEqual(signal["status"], "REJECTED")
        self.assertEqual(signal["reason"], "CANARY_SIGNAL_EXPIRED")
        self.assertEqual(
            self.store.connection.execute(
                "SELECT status FROM canary_ledger WHERE signal_id=?",
                (signal_id,),
            ).fetchone()[0],
            "REJECTED",
        )
    def test_official_venue_expiry_crossing_after_reservation_rejects_before_sink(self):
        class SDKClient:
            def __init__(self):
                self.post_calls = []
                self._ctx = {
                    "environment_config": {
                        "exchange_v3": "0xexchange-v3",
                    }
                }

            def create_limit_order(self, **kwargs):
                return {"maker_amount": "1000000"}

            def get_balance_allowance(self, **kwargs):
                return {
                    "balance": "2500000",
                    "allowances": {"0xexchange-v3": "1000000"},
                }

            def post_order(self, signed):
                self.post_calls.append(signed)
                return {"ok": True, "order_id": "unexpected"}

            def close(self):
                pass

        context = {
            "asset_id": "position-yes",
            "market_version": "v2",
            "neg_risk": False,
            "accepting_orders": True,
            "min_order_size": "1",
            "tick_size": "0.01",
            "bids": [{"price": "0.49", "size": "100"}],
            "asks": [{"price": "0.50", "size": "100"}],
            "fee_bps": "10",
        }
        self.arm()
        signal_id = "official-expiry-after-reservation"
        self._ensure_direct_signal(signal_id)
        client = SDKClient()

        class SecureClient:
            @staticmethod
            def _create(**kwargs):
                return client

        sdk = SimpleNamespace(SecureClient=SecureClient)
        venue = PolymarketClobV2Venue()
        original_publish = self.service.publish_readiness_snapshot

        def expire_after_reservation(*, reason):
            if reason == "CANARY_SUBMITTING":
                self.service.clock = lambda: T0 + timedelta(seconds=60)
            return original_publish(reason=reason)

        with patch.dict(sys.modules, {"polymarket": sdk}), patch.object(
            PolymarketClobV2Venue,
            "installed_sdk_version",
            return_value="0.9.2",
        ), patch.object(
            PolymarketClobV2Venue,
            "geoblock",
            return_value={"blocked": False, "close_only": False},
        ), patch.object(
            PolymarketClobV2Venue,
            "market_context",
            return_value=context,
        ), patch.object(
            PolymarketClobV2Venue,
            "balance",
            return_value=Decimal("10"),
        ), patch.object(
            self.service,
            "publish_readiness_snapshot",
            side_effect=expire_after_reservation,
        ):
            with self.assertRaisesRegex(
                CanaryBlocked,
                "CANARY_SIGNAL_EXPIRED",
            ):
                self.service.submit(
                    signal_id=signal_id,
                    candidate_id="C123",
                    market_id="m",
                    token_id="yes",
                    side="BUY",
                    paper_expected_price=Decimal("0.50"),
                    venue=venue,
                )
        self.assertFalse(client.post_calls)
        signal = self.service.get_signal(signal_id)
        self.assertEqual(signal["status"], "REJECTED")
        self.assertEqual(signal["reason"], "CANARY_SIGNAL_EXPIRED")
        self.assertEqual(
            self.store.connection.execute(
                "SELECT status FROM canary_ledger WHERE signal_id=?",
                (signal_id,),
            ).fetchone()[0],
            "REJECTED",
        )


    def test_initial_readiness_keeps_qualification_and_selection_unknown(self):
        store = AxiomStore(":memory:")
        self.addCleanup(store.close)
        service = CanaryService(store, clock=lambda: T0)
        status = service.status()
        self.assertEqual(status["readiness_snapshot_status"], "STALE")
        self.assertEqual(
            status["readiness_snapshot_reason"],
            "READINESS_SNAPSHOT_INITIALIZING",
        )
        for field in ("eligibility_raw_count", "eligible_count", "rankable_raw_count", "rankable_count"):
            self.assertIsNone(status[field])
        self.assertEqual(status["selection_status"], "UNKNOWN")
        self.assertIsNone(status["selection_valid"])
        self.assertIsNone(status["selected_candidate"])
        self.assertIsNone(status["winner_id"])
        self.assertIsNone(status["selection_reason"])
        self.assertIsNone(status["selection_invalidation_reason"])

    def arm(self, **kwargs): return self.service.arm("C123",venue=kwargs.pop("venue",self.venue),credentials_configured=True,**kwargs)
    def submit(self, signal="s1", **kwargs):
        candidate_id = kwargs.pop("candidate_id", "C123")
        self._ensure_direct_signal(signal, candidate_id)
        return self.service.submit(signal_id=signal,candidate_id=candidate_id,market_id="m",token_id="yes",side="BUY",paper_expected_price=Decimal("0.50"),venue=kwargs.pop("venue",self.venue),allow_test_venue=kwargs.pop("allow_test_venue",True),**kwargs)
    def assertBlocked(self, code, fn):
        with self.assertRaisesRegex(CanaryBlocked,code): fn()

    def test_default_startup_cannot_trade(self): self.assertBlocked("CANARY_NOT_ARMED",self.submit)
    def test_direct_submit_requires_persisted_ready_signal(self):
        self.arm()
        with self.assertRaisesRegex(CanaryBlocked, "CANARY_SIGNAL_NOT_FOUND"):
            self.service.submit(
                signal_id="not-persisted",
                candidate_id="C123",
                market_id="m",
                token_id="yes",
                side="BUY",
                paper_expected_price=Decimal("0.50"),
                venue=self.venue,
                allow_test_venue=True,
            )
        self.assertEqual(
            self.store.connection.execute("SELECT COUNT(*) FROM canary_ledger").fetchone()[0],
            0,
        )

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
            ledger_columns = {
                str(row["name"])
                for row in legacy_store.connection.execute(
                    "PRAGMA table_info(canary_ledger)"
                )
            }
            self.assertIn("control_generation", ledger_columns)
        finally:
            legacy_store.close()
    def test_status_report_retains_latest_signal_and_legacy_projection_aliases(self):
        self.store.connection.execute(
            "INSERT INTO canary_signals("
            "signal_id,candidate_id,frozen_hash,strategy_hash,model_hash,config_hash,"
            "market_id,token_id,outcome,side,paper_expected_price,source_snapshot_id,"
            "source_timestamp,generated_at,expires_at,status,reason,evidence_json,updated_at"
            ") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "status-report-signal",
                "C123",
                "frozen",
                "strategy",
                "model",
                "config",
                "market-1",
                "yes",
                "yes",
                "BUY",
                "0.50",
                "snapshot-1",
                T0.isoformat(),
                T0.isoformat(),
                (T0 + timedelta(minutes=1)).isoformat(),
                "READY",
                None,
                json.dumps({"depth": [{"price": "0.50"}] * 200}),
                T0.isoformat(),
            ),
        )
        self.store.connection.commit()
        report = self.service.status_report()
        self.assertEqual(report["latest_signal"]["signal_id"], "status-report-signal")
        self.assertEqual(report["readiness"]["latest_signal"]["signal_id"], "status-report-signal")
        self.assertIn("micro_live_canary", report)
        self.assertIn("control_generation", report)
        self.assertIn("selection_status", report)
        self.assertIn("execution", report)
        self.assertLessEqual(
            len(report["latest_signal"]["evidence"]["depth"]),
            1,
        )

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

    def test_matched_evaluation_failure_marks_projection_stale(self):
        baseline = self.service.publish_readiness_snapshot(reason="BASELINE")
        version = self.store.connection.execute(
            "SELECT projection_version FROM canary_readiness_snapshot "
            "WHERE singleton=1"
        ).fetchone()["projection_version"]

        result = self.service._persist_evaluation_failure(
            error_code="MATCHED_FAILURE",
            expected_projection_version=version,
        )

        row = self.store.connection.execute(
            "SELECT payload_json,projection_version,readiness_snapshot_status,"
            "readiness_snapshot_stale,readiness_snapshot_reason "
            "FROM canary_readiness_snapshot WHERE singleton=1"
        ).fetchone()
        self.assertEqual(row["projection_version"], version)
        self.assertEqual(row["readiness_snapshot_status"], "STALE")
        self.assertEqual(row["readiness_snapshot_stale"], 1)
        self.assertEqual(row["readiness_snapshot_reason"], "EVALUATION_FAILED")
        payload = json.loads(row["payload_json"])
        self.assertEqual(payload["readiness_evaluation_error_code"], "MATCHED_FAILURE")
        self.assertEqual(payload["micro_live_canary"], baseline["micro_live_canary"])
        self.assertEqual(result["readiness_snapshot_reason"], "EVALUATION_FAILED")

    def test_older_evaluation_failure_cannot_clobber_newer_current_projection(self):
        self.service.publish_readiness_snapshot(reason="OLDER_EVALUATION")
        expected_version = self.store.connection.execute(
            "SELECT projection_version FROM canary_readiness_snapshot "
            "WHERE singleton=1"
        ).fetchone()["projection_version"]
        newer = self.service.publish_readiness_snapshot(reason="NEWER_CURRENT")
        before = self.store.connection.execute(
            "SELECT payload_json,projection_version,readiness_snapshot_status,"
            "readiness_snapshot_stale,readiness_snapshot_reason "
            "FROM canary_readiness_snapshot WHERE singleton=1"
        ).fetchone()

        result = self.service._persist_evaluation_failure(
            error_code="OLDER_FAILURE",
            expected_projection_version=expected_version,
        )

        after = self.store.connection.execute(
            "SELECT payload_json,projection_version,readiness_snapshot_status,"
            "readiness_snapshot_stale,readiness_snapshot_reason "
            "FROM canary_readiness_snapshot WHERE singleton=1"
        ).fetchone()
        self.assertEqual(after["payload_json"], before["payload_json"])
        self.assertEqual(after["projection_version"], before["projection_version"])
        self.assertEqual(after["readiness_snapshot_status"], "CURRENT")
        self.assertEqual(after["readiness_snapshot_stale"], 0)
        self.assertEqual(after["readiness_snapshot_reason"], "NEWER_CURRENT")
        self.assertEqual(result, newer)

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

    def test_lifecycle_evidence_is_classified_without_weakening_binding(self):
        baseline_record = self.store.load_candidate_lifecycle("C123")
        _, baseline_qualification_hash, baseline_ranking_hash = (
            _canary_lifecycle_snapshot_hashes(self.store, baseline_record)
        )
        for hash_name, hash_value in (
            ("qualification_hash", baseline_qualification_hash),
            ("ranking_snapshot_hash", baseline_ranking_hash),
        ):
            with self.subTest(hash_name=hash_name):
                self.assertIsInstance(hash_value, str)
                self.assertTrue(hash_value)
        self.service.publish_readiness_snapshot(reason="CLASSIFICATION_BASELINE")
        readiness_row = self.store.connection.execute(
            "SELECT readiness_snapshot_status,readiness_snapshot_stale "
            "FROM canary_readiness_snapshot WHERE singleton=1"
        ).fetchone()
        self.assertIsNotNone(readiness_row)
        self.assertEqual(readiness_row["readiness_snapshot_status"], "CURRENT")
        self.assertFalse(readiness_row["readiness_snapshot_stale"])
        eligibility_row = self.store.connection.execute(
            "SELECT evidence_json FROM canary_eligibility WHERE candidate_id='C123'"
        ).fetchone()
        self.assertIsNotNone(eligibility_row)
        before_evidence = json.loads(eligibility_row["evidence_json"])
        self.assertEqual(
            before_evidence["qualification_hash"],
            baseline_qualification_hash,
        )
        lifecycle = CandidateLifecycleManager(self.store)

        # A: changing the immutable config/frozen binding and quality projection
        # invalidates the old qualification and requires a fresh attestation.
        config_hash = "config-v2"
        frozen_hash = hashlib.sha256(
            "|".join(("strategy-v1", "model-v1", config_hash)).encode()
        ).hexdigest()
        lifecycle.record_evidence(
            "C123",
            {
                "config_hash": config_hash,
                "frozen_hash": frozen_hash,
                "data_quality": "TIMESTAMPED_DEPTH",
            },
            expected_stage=CandidateStage.PAPER_PROMOTABLE,
            reason="qualification evidence changed",
        )
        stale = self.service.status()
        self.assertEqual(stale["readiness_snapshot_status"], "STALE")
        self.assertTrue(stale["readiness_snapshot_stale"])
        self.assertEqual(
            stale["readiness_snapshot_reason"],
            "LIFECYCLE_QUALIFICATION_UPDATED",
        )
        changed_binding = self.service.validate_eligibility("C123")
        self.assertTrue(changed_binding["eligible"], changed_binding)
        self.assertFalse(changed_binding["binding"]["bound"], changed_binding)
        self.assertEqual(
            changed_binding["binding"]["reason_code"],
            "QUALIFICATION_CHANGED",
        )

        self.service.mark_eligible("C123")
        after_a_eligibility_row = self.store.connection.execute(
            "SELECT evidence_json FROM canary_eligibility WHERE candidate_id='C123'"
        ).fetchone()
        self.assertIsNotNone(after_a_eligibility_row)
        after_a_evidence = json.loads(after_a_eligibility_row["evidence_json"])
        after_a_record = self.store.load_candidate_lifecycle("C123")
        _, after_a_qualification_hash, after_a_ranking_hash = (
            _canary_lifecycle_snapshot_hashes(self.store, after_a_record)
        )
        self.assertNotEqual(
            before_evidence["qualification_hash"],
            after_a_evidence["qualification_hash"],
        )
        self.assertEqual(
            after_a_evidence["qualification_hash"],
            after_a_qualification_hash,
        )
        before_b_record = self.store.load_candidate_lifecycle("C123")
        _, before_b_qualification_hash, before_b_ranking_hash = (
            _canary_lifecycle_snapshot_hashes(self.store, before_b_record)
        )
        self.assertEqual(after_a_qualification_hash, before_b_qualification_hash)
        self.assertEqual(after_a_ranking_hash, before_b_ranking_hash)

        # B: forward duration/expectancy affect ranking but not qualification.
        self.service.publish_readiness_snapshot(reason="QUALIFICATION_REBOUND")
        lifecycle.record_evidence(
            "C123",
            {
                "forward_evidence": {
                    **dict(self.store.load_candidate_lifecycle("C123")["payload"]["forward_evidence"]),
                    "forward_duration_seconds": 30 * 86400,
                    "forward_expectancy": -0.15,
                }
            },
            expected_stage=CandidateStage.PAPER_PROMOTABLE,
            reason="ranking evidence changed",
        )
        stale = self.service.status()
        self.assertEqual(stale["readiness_snapshot_status"], "STALE")
        self.assertEqual(
            stale["readiness_snapshot_reason"],
            "LIFECYCLE_RANKING_EVIDENCE_UPDATED",
        )
        b_eligibility_row = self.store.connection.execute(
            "SELECT evidence_json FROM canary_eligibility "
            "WHERE candidate_id='C123'"
        ).fetchone()
        self.assertIsNotNone(b_eligibility_row)
        self.assertEqual(
            json.loads(b_eligibility_row["evidence_json"])["qualification_hash"],
            after_a_evidence["qualification_hash"],
        )
        after_b_record = self.store.load_candidate_lifecycle("C123")
        _, after_b_qualification_hash, after_b_ranking_hash = (
            _canary_lifecycle_snapshot_hashes(self.store, after_b_record)
        )
        self.assertEqual(
            before_b_qualification_hash,
            after_b_qualification_hash,
        )
        self.assertNotEqual(
            before_b_ranking_hash,
            after_b_ranking_hash,
        )

        # C/D: liquidity/drawdown telemetry and fill/observation counters are
        # deliberately outside both immutable qualification and ranking hashes.
        self.service.publish_readiness_snapshot(reason="RANKING_REBOUND")
        lifecycle.record_evidence(
            "C123",
            {
                "forward_evidence": {
                    **dict(self.store.load_candidate_lifecycle("C123")["payload"]["forward_evidence"]),
                    "forward_liquidity": 42.0,
                    "forward_max_drawdown": 0.04,
                    "forward_fills": 17,
                    "forward_observations": 101,
                }
            },
            expected_stage=CandidateStage.PAPER_PROMOTABLE,
            reason="telemetry-only evidence changed",
        )
        current = self.service.status()
        self.assertEqual(current["readiness_snapshot_status"], "CURRENT")
        self.assertFalse(current["readiness_snapshot_stale"])
        self.assertEqual(current["readiness_snapshot_reason"], "RANKING_REBOUND")
        self.assertEqual(
            self.service.validate_eligibility("C123")["binding"]["reason_code"],
            None,
        )
        # D: harmless lifecycle metadata changes neither hash projection nor
        # telemetry, so the current readiness publication remains current.
        self.service.publish_readiness_snapshot(reason="TELEMETRY_REBOUND")
        lifecycle.record_evidence(
            "C123",
            {"operator_note": "classification metadata"},
            expected_stage=CandidateStage.PAPER_PROMOTABLE,
            reason="harmless classification metadata changed",
        )
        current = self.service.status()
        self.assertEqual(current["readiness_snapshot_status"], "CURRENT")
        self.assertFalse(current["readiness_snapshot_stale"])
        self.assertEqual(current["readiness_snapshot_reason"], "TELEMETRY_REBOUND")
        self.assertEqual(
            self.service.validate_eligibility("C123")["binding"]["reason_code"],
            None,
        )
        # Stage changes remain a hard fail-closed boundary even when the
        # preceding telemetry update was intentionally ignored.
        lifecycle.reject(
            "C123",
            "classification stage safety",
            expected_stage=CandidateStage.PAPER_PROMOTABLE,
        )
        self.assertFalse(self.service.validate_eligibility("C123")["eligible"])

    def test_qualification_record_evidence_refreshes_lifecycle_hash_before_requalification(self):
        before_record = self.store.load_candidate_lifecycle("C123")
        self.assertIsNotNone(before_record)
        _, before_qualification_hash, _ = _canary_lifecycle_snapshot_hashes(
            self.store,
            before_record,
        )
        canonical_payload = dict(before_record["payload"])
        canonical_payload["qualification_hash"] = before_qualification_hash
        self.store.save_candidate_lifecycle(
            "C123",
            "PAPER_PROMOTABLE",
            canonical_payload,
            from_stage="PAPER_PROMOTABLE",
            timestamp=T0,
        )
        self.service.mark_eligible("C123")

        before_eligibility = self.store.connection.execute(
            "SELECT evidence_json FROM canary_eligibility WHERE candidate_id=?",
            ("C123",),
        ).fetchone()
        self.assertIsNotNone(before_eligibility)
        before_evidence = json.loads(before_eligibility["evidence_json"])
        self.assertEqual(before_evidence["qualification_hash"], before_qualification_hash)

        changed_config = "config-before-requalification"
        changed_frozen = hashlib.sha256(
            "|".join(("strategy-v1", "model-v1", changed_config)).encode()
        ).hexdigest()
        CandidateLifecycleManager(self.store).record_evidence(
            "C123",
            {
                "config_hash": changed_config,
                "frozen_hash": changed_frozen,
            },
            expected_stage=CandidateStage.PAPER_PROMOTABLE,
            reason="qualification changed before requalification",
        )

        changed_record = self.store.load_candidate_lifecycle("C123")
        self.assertIsNotNone(changed_record)
        _, changed_qualification_hash, _ = _canary_lifecycle_snapshot_hashes(
            self.store,
            changed_record,
        )
        self.assertNotEqual(changed_qualification_hash, before_qualification_hash)
        self.assertEqual(
            changed_record["payload"]["qualification_hash"],
            changed_qualification_hash,
        )
        stale_feed = self.store.research_feed_status(now=T0)
        self.assertEqual(stale_feed["candidates"]["eligible"], 0)

        self.service.mark_eligible("C123")
        after_eligibility = self.store.connection.execute(
            "SELECT evidence_json FROM canary_eligibility WHERE candidate_id=?",
            ("C123",),
        ).fetchone()
        self.assertIsNotNone(after_eligibility)
        after_evidence = json.loads(after_eligibility["evidence_json"])
        self.assertEqual(
            after_evidence["qualification_hash"],
            changed_qualification_hash,
        )
        refreshed_feed = self.store.research_feed_status(now=T0)
        self.assertEqual(refreshed_feed["candidates"]["eligible"], 1)

    def test_top_level_ranking_aliases_are_classified_as_b_and_change_hash(self):
        base_payload = dict(self.store.load_candidate_lifecycle("C123")["payload"])
        aliases = (
            ("expectancy", "validation_expectancy", 0.31),
            ("confidence_lower_bound", "validation_confidence_lower_bound", 0.21),
            ("stability", "validation_stability", 0.71),
            ("calibration", "validation_calibration", 0.72),
            ("sample_count", "validation_sample_count", 90),
            ("trade_count", "validation_trade_count", 19),
            ("execution_quality", "validation_execution_quality", 0.31),
            ("max_drawdown", "validation_max_drawdown", 0.19),
            ("liquidity", "validation_liquidity", 0.31),
            ("quality", "data_quality", "TIMESTAMPED_DEPTH"),
            ("execution_fidelity_score", None, 0.31),
        )
        lifecycle = CandidateLifecycleManager(self.store)
        for alias, canonical, value in aliases:
            with self.subTest(alias=alias):
                candidate_id = f"ranking-alias-{alias}"
                payload = {**base_payload, "candidate_id": candidate_id}
                if canonical is not None:
                    payload.pop(canonical, None)
                if alias == "quality":
                    payload.pop("validation_data_quality", None)
                    payload.pop("data_quality", None)
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
                before = self.store.load_candidate_lifecycle(candidate_id)
                self.assertIsNotNone(before)
                _, before_qualification, before_ranking = (
                    _canary_lifecycle_snapshot_hashes(self.store, before)
                )
                lifecycle.record_evidence(
                    candidate_id,
                    {alias: value},
                    expected_stage=CandidateStage.FROZEN,
                    reason=f"ranking alias {alias} changed",
                )
                after = self.store.load_candidate_lifecycle(candidate_id)
                self.assertIsNotNone(after)
                _, after_qualification, after_ranking = (
                    _canary_lifecycle_snapshot_hashes(self.store, after)
                )
                self.assertEqual(after_qualification, before_qualification)
                self.assertNotEqual(after_ranking, before_ranking)
                self.assertEqual(
                    _canary_lifecycle_evidence_class(self.store, before, after),
                    "B",
                )

    def test_prediction_market_sources_are_b_ranking_inputs_without_requalifying(self):
        base_payload = dict(self.store.load_candidate_lifecycle("C123")["payload"])
        base_payload["type"] = "prediction"
        for source in ("experiment_plan", "strategy", "forward_config", "market"):
            existing = base_payload.get(source)
            source_payload = dict(existing) if isinstance(existing, dict) else {}
            source_payload["market_type"] = "prediction"
            base_payload[source] = source_payload

        source_mutations = (
            ("top-level-type", "type"),
            ("experiment-plan-market-type", "experiment_plan"),
            ("nested-strategy-market-type", "strategy"),
            ("nested-forward-config-market-type", "forward_config"),
            ("nested-market-market-type", "market"),
        )
        lifecycle = CandidateLifecycleManager(self.store)
        for label, source in source_mutations:
            with self.subTest(source=label):
                candidate_id = f"ranking-market-source-{label}"
                payload = {**base_payload, "candidate_id": candidate_id}
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
                    from_stage="IDEA",
                    timestamp=T0,
                )
                before = self.store.load_candidate_lifecycle(candidate_id)
                self.assertIsNotNone(before)
                _, before_qualification, before_ranking = (
                    _canary_lifecycle_snapshot_hashes(self.store, before)
                )
                if source == "type":
                    changed_evidence = {"type": "crypto_spot"}
                else:
                    changed_source = dict(payload[source])
                    changed_source["market_type"] = "crypto_spot"
                    changed_evidence = {source: changed_source}
                lifecycle.record_evidence(
                    candidate_id,
                    changed_evidence,
                    expected_stage=CandidateStage.FROZEN,
                    reason=f"{label} changed",
                )
                after = self.store.load_candidate_lifecycle(candidate_id)
                self.assertIsNotNone(after)
                _, after_qualification, after_ranking = (
                    _canary_lifecycle_snapshot_hashes(self.store, after)
                )
                self.assertEqual(after_qualification, before_qualification)
                self.assertNotEqual(after_ranking, before_ranking)
                self.assertEqual(
                    _canary_lifecycle_evidence_class(self.store, before, after),
                    "B",
                )

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
            "cached_projection",
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
        with self.store.connection:
            self.store.connection.execute(
                "UPDATE canary_readiness_snapshot SET "
                "readiness_snapshot_status='CURRENT',"
                "readiness_snapshot_stale=0,"
                "readiness_snapshot_updated_at=? "
                "WHERE singleton=1",
                ((T0 - timedelta(hours=1)).isoformat(),),
            )
        stale = self.service.status()
        self.assertEqual(stale["readiness_snapshot_status"], "STALE")
        self.assertEqual(stale["readiness_snapshot_reason"], "READINESS_SNAPSHOT_TOO_OLD")
        self.assertEqual(stale["eligible_count"], 1)
        with patch.object(
            CredentialStore,
            "cached_projection",
            return_value={
                "configured": False,
                "status": "NOT CONFIGURED",
                "secret_values_exposed": False,
            },
        ):
            stale_dashboard = DashboardData(store=self.store).canary_data()
        self.assertEqual(stale_dashboard["canary"]["eligible_count"], 1)
        self.assertEqual(stale_dashboard["candidate_status"]["canary_eligible"], 1)


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
    def test_collector_grades_c_and_d_prevent_arming(self):
        for grade in ("C", "D"):
            with self.subTest(grade=grade):
                self.store.polymarket_health = lambda **kwargs: {"grade": grade}
                self.assertBlocked("COLLECTOR_DEGRADED", self.arm)
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
    def test_control_kill_after_submitting_transition_blocks_sink(self):
        self.arm()
        venue = FakeVenue()
        original_publish = self.service.publish_readiness_snapshot

        def kill_before_final_fence(*, reason):
            if reason == "CANARY_SUBMITTING":
                self.service.kill()
            return original_publish(reason=reason)

        with patch.object(
            self.service,
            "publish_readiness_snapshot",
            side_effect=kill_before_final_fence,
        ):
            with self.assertRaisesRegex(CanaryBlocked, "CANARY_KILLED"):
                self.submit("killed-before-sink", venue=venue)
        self.assertFalse(venue.submissions)

    def test_disarm_after_reservation_blocks_sink(self):
        self.arm()
        venue = FakeVenue()
        original_publish = self.service.publish_readiness_snapshot

        def disarm_before_final_fence(*, reason):
            if reason == "CANARY_SUBMITTING":
                self.service.disarm()
            return original_publish(reason=reason)

        with patch.object(
            self.service,
            "publish_readiness_snapshot",
            side_effect=disarm_before_final_fence,
        ):
            with self.assertRaisesRegex(CanaryBlocked, "CANARY_NOT_ARMED"):
                self.submit("disarmed-before-sink", venue=venue)
        self.assertFalse(venue.submissions)

    def test_generation_change_after_reservation_blocks_sink(self):
        self.arm()
        venue = FakeVenue()
        original_publish = self.service.publish_readiness_snapshot

        def change_generation_before_final_fence(*, reason):
            if reason == "CANARY_SUBMITTING":
                with self.store._lock:
                    self.store.connection.execute(
                        "UPDATE canary_control SET control_generation="
                        "control_generation+1 WHERE singleton=1"
                    )
                    self.store.connection.commit()
            return original_publish(reason=reason)

        with patch.object(
            self.service,
            "publish_readiness_snapshot",
            side_effect=change_generation_before_final_fence,
        ):
            with self.assertRaisesRegex(CanaryBlocked, "CANARY_CONTROL_CHANGED"):
                self.submit("generation-changed-before-sink", venue=venue)
        self.assertFalse(venue.submissions)
    def test_final_risk_and_health_fences_block_direct_sink(self):
        mutations = (
            (
                "daily-loss",
                "DAILY_LOSS_LIMIT",
                lambda: (
                    self.store.connection.execute(
                        "INSERT INTO canary_ledger("
                        "event_id,signal_id,timestamp,candidate_id,venue,market_id,"
                        "token_id,side,requested_notional,paper_expected_price,max_price,"
                        "status,realized_pnl,evidence_json) "
                        "VALUES('final-risk-loss','final-risk-loss-signal',?,?,?,?,?,?,?,?,?,'RESOLVED','-2.00','{}')",
                        (
                            T0.isoformat(),
                            "C123",
                            "polymarket",
                            "m0",
                            "t",
                            "BUY",
                            "1",
                            ".5",
                            ".5",
                        ),
                    ),
                    self.store.connection.commit(),
                )[-1],
            ),
            (
                "collector-health",
                "COLLECTOR_DEGRADED",
                lambda: setattr(
                    self.store,
                    "polymarket_health",
                    lambda **kwargs: {"grade": "C", "reason_code": "STALE_MARKETS"},
                ),
            ),
        )
        for index, (label, expected, mutate) in enumerate(mutations):
            if index:
                self.store.close()
                self.setUp()
            self.arm()
            signal_id = f"final-direct-{label}"
            venue = FakeVenue()
            original_publish = self.service.publish_readiness_snapshot

            def mutate_before_final_fence(*, reason, mutate=mutate):
                if reason == "CANARY_SUBMITTING":
                    mutate()
                return original_publish(reason=reason)

            with patch.object(
                self.service,
                "publish_readiness_snapshot",
                side_effect=mutate_before_final_fence,
            ):
                with self.assertRaisesRegex(CanaryBlocked, expected):
                    self.submit(signal_id, venue=venue)
            self.assertFalse(venue.submissions)
            signal = self.service.get_signal(signal_id)
            self.assertEqual(signal["reason"], expected)
            self.assertEqual(signal["status"], "REJECTED")
            self.assertEqual(
                self.store.connection.execute(
                    "SELECT status FROM canary_ledger WHERE signal_id=?",
                    (signal_id,),
                ).fetchone()[0],
                "REJECTED",
            )
            self.assertEqual(
                self.store.connection.execute(
                    "SELECT status FROM canary_execution_events WHERE canary_event_id=?",
                    (
                        "canary-"
                        + hashlib.sha256(signal_id.encode()).hexdigest()[:24],
                    ),
                ).fetchone()[0],
                "REJECTED",
            )

    def test_final_risk_and_health_fences_block_official_sink(self):
        class SDKClient:
            def __init__(self):
                self.post_calls = []
                self._ctx = {
                    "environment_config": {
                        "exchange_v3": "0xexchange-v3",
                    }
                }

            def create_limit_order(self, **kwargs):
                return {"maker_amount": "1000000"}

            def get_balance_allowance(self, **kwargs):
                return {
                    "balance": "2500000",
                    "allowances": {"0xexchange-v3": "1000000"},
                }

            def post_order(self, signed):
                self.post_calls.append(signed)
                return {"ok": True, "order_id": "unexpected"}

            def close(self):
                pass

        context = {
            "asset_id": "position-yes",
            "market_version": "v2",
            "neg_risk": False,
            "accepting_orders": True,
            "min_order_size": "1",
            "tick_size": "0.01",
            "bids": [{"price": "0.49", "size": "100"}],
            "asks": [{"price": "0.50", "size": "100"}],
            "fee_bps": "10",
        }
        mutations = (
            (
                "daily-loss",
                "DAILY_LOSS_LIMIT",
                lambda: (
                    self.store.connection.execute(
                        "INSERT INTO canary_ledger("
                        "event_id,signal_id,timestamp,candidate_id,venue,market_id,"
                        "token_id,side,requested_notional,paper_expected_price,max_price,"
                        "status,realized_pnl,evidence_json) "
                        "VALUES('final-official-loss','final-official-loss-signal',?,?,?,?,?,?,?,?,?,'RESOLVED','-2.00','{}')",
                        (
                            T0.isoformat(),
                            "C123",
                            "polymarket",
                            "m0",
                            "t",
                            "BUY",
                            "1",
                            ".5",
                            ".5",
                        ),
                    ),
                    self.store.connection.commit(),
                )[-1],
            ),
            (
                "collector-health",
                "COLLECTOR_DEGRADED",
                lambda: setattr(
                    self.store,
                    "polymarket_health",
                    lambda **kwargs: {"grade": "C", "reason_code": "STALE_MARKETS"},
                ),
            ),
        )
        for index, (label, expected, mutate) in enumerate(mutations):
            if index:
                self.store.close()
                self.setUp()
            self.arm()
            signal_id = f"final-official-{label}"
            self._ensure_direct_signal(signal_id)
            client = SDKClient()

            class SecureClient:
                @staticmethod
                def _create(**kwargs):
                    return client

            sdk = SimpleNamespace(SecureClient=SecureClient)
            venue = PolymarketClobV2Venue()
            original_publish = self.service.publish_readiness_snapshot

            def mutate_before_final_fence(*, reason, mutate=mutate):
                if reason == "CANARY_SUBMITTING":
                    mutate()
                return original_publish(reason=reason)

            with patch.dict(sys.modules, {"polymarket": sdk}), patch.object(
                PolymarketClobV2Venue,
                "installed_sdk_version",
                return_value="0.9.2",
            ), patch.object(
                PolymarketClobV2Venue,
                "geoblock",
                return_value={"blocked": False, "close_only": False},
            ), patch.object(
                PolymarketClobV2Venue,
                "market_context",
                return_value=context,
            ), patch.object(
                PolymarketClobV2Venue,
                "balance",
                return_value=Decimal("10"),
            ), patch.object(
                self.service,
                "publish_readiness_snapshot",
                side_effect=mutate_before_final_fence,
            ):
                with self.assertRaisesRegex(CanaryBlocked, expected):
                    self.service.submit(
                        signal_id=signal_id,
                        candidate_id="C123",
                        market_id="m",
                        token_id="yes",
                        side="BUY",
                        paper_expected_price=Decimal("0.50"),
                        venue=venue,
                    )
            self.assertFalse(client.post_calls)
            signal = self.service.get_signal(signal_id)
            self.assertEqual(signal["reason"], expected)
            self.assertEqual(signal["status"], "REJECTED")
            self.assertEqual(
                self.store.connection.execute(
                    "SELECT status FROM canary_ledger WHERE signal_id=?",
                    (signal_id,),
                ).fetchone()[0],
                "REJECTED",
            )
            self.assertEqual(
                self.store.connection.execute(
                    "SELECT status FROM canary_execution_events WHERE canary_event_id=?",
                    (
                        "canary-"
                        + hashlib.sha256(signal_id.encode()).hexdigest()[:24],
                    ),
                ).fetchone()[0],
                "REJECTED",
            )
    def _scope_race_mutations(self, resolution):
        current_at = T0 + timedelta(microseconds=1)
        stale_at = T0 + timedelta(seconds=62)

        def refresh_direct_signal_timestamps(timestamp):
            row = self.store.connection.execute(
                "SELECT signal_id,evidence_json FROM canary_signals "
                "ORDER BY signal_id DESC LIMIT 1"
            ).fetchone()
            if row is None:
                return
            try:
                evidence = json.loads(row["evidence_json"] or "{}")
            except (TypeError, ValueError, json.JSONDecodeError):
                evidence = {}
            if not isinstance(evidence, dict):
                evidence = {}
            evidence["current_order_book_timestamp"] = timestamp.isoformat()
            self.store.connection.execute(
                "UPDATE canary_signals SET source_timestamp=?,expires_at=?,"
                "evidence_json=? WHERE signal_id=?",
                (
                    timestamp.isoformat(),
                    (timestamp + timedelta(seconds=60)).isoformat(),
                    json.dumps(evidence, sort_keys=True),
                    row["signal_id"],
                ),
            )
            self.store.connection.commit()

        def stale():
            changed = replace(
                resolution,
                resolved_at=current_at,
                resolution_id="",
            )
            self.store.save_market_scope_resolution(changed)
            self.service.clock = lambda: stale_at
            refresh_direct_signal_timestamps(stale_at)
            return changed

        def unbound():
            changed = replace(
                resolution,
                resolved_at=current_at,
                resolution_id="",
                status="RESEARCH_ONLY",
                reason="RESEARCH_ONLY",
                matched_markets=(),
            )
            self.store.save_market_scope_resolution(changed)
            self.service.clock = lambda: current_at
            return changed

        def hash_mismatch():
            changed = replace(
                resolution,
                resolved_at=T0 + timedelta(microseconds=2),
                scope_hash="sha256:scope-race-mismatch",
                resolution_id="",
            )
            self.store.save_market_scope_resolution(changed)
            self.service.clock = lambda: T0 + timedelta(microseconds=2)
            return changed

        return (
            ("stale", stale, "SCOPE_RESOLUTION_STALE"),
            ("unbound", unbound, "RESEARCH_ONLY"),
            (
                "hash-mismatch",
                hash_mismatch,
                "SCOPE_RESOLUTION_SCOPE_MISMATCH",
            ),
        )

    def _scope_race_loader(self, mutate):
        original_loader = self.store.load_market_scope_resolution
        scoped_calls = 0
        mutated_resolution = None

        def load(candidate_id, *args, **kwargs):
            nonlocal scoped_calls, mutated_resolution
            result = original_loader(candidate_id, *args, **kwargs)
            if kwargs.get("scope_hash") and kwargs.get("scope_version"):
                scoped_calls += 1
                if scoped_calls == 2:
                    mutated_resolution = mutate()
                elif mutated_resolution is not None:
                    return mutated_resolution
            return result

        return patch.object(
            self.store,
            "load_market_scope_resolution",
            side_effect=load,
        )

    def test_scope_races_after_submitting_check_block_direct_sink(self):
        for index, (label, mutate, reason) in enumerate(
            self._scope_race_mutations(
                self.store.load_market_scope_resolution("C123")
            )
        ):
            if index:
                self.store.close()
                self.setUp()
            self.arm()
            signal_id = f"scope-race-direct-{label}"
            self._ensure_direct_signal(signal_id)
            resolution = self.store.load_market_scope_resolution("C123")
            self.assertIsNotNone(resolution)
            mutate = self._scope_race_mutations(resolution)[index][1]
            venue = FakeVenue()
            with self._scope_race_loader(mutate):
                self.assertBlocked(
                    reason,
                    lambda: self.submit(signal_id, venue=venue),
                )
            self.assertFalse(venue.submissions)
            signal = self.service.get_signal(signal_id)
            self.assertEqual(signal["reason"], reason)

    def test_scope_races_after_submitting_check_block_official_sink(self):
        class SDKClient:
            def __init__(self):
                self.post_calls = []
                self._ctx = {
                    "environment_config": {
                        "exchange_v3": "0xexchange-v3",
                    }
                }

            def create_limit_order(self, **kwargs):
                return {"maker_amount": "1000000"}

            def get_balance_allowance(self, **kwargs):
                return {
                    "balance": "2500000",
                    "allowances": {"0xexchange-v3": "1000000"},
                }

            def post_order(self, signed):
                self.post_calls.append(signed)
                return {"ok": True, "order_id": "unexpected"}

            def close(self):
                pass

        context = {
            "asset_id": "position-yes",
            "market_version": "v2",
            "neg_risk": False,
            "accepting_orders": True,
            "min_order_size": "1",
            "tick_size": "0.01",
            "bids": [{"price": "0.49", "size": "100"}],
            "asks": [{"price": "0.50", "size": "100"}],
            "fee_bps": "10",
        }
        for index, (label, mutate, reason) in enumerate(
            self._scope_race_mutations(
                self.store.load_market_scope_resolution("C123")
            )
        ):
            if index:
                self.store.close()
                self.setUp()
            self.arm()
            signal_id = f"scope-race-official-{label}"
            self._ensure_direct_signal(signal_id)
            resolution = self.store.load_market_scope_resolution("C123")
            self.assertIsNotNone(resolution)
            mutate = self._scope_race_mutations(resolution)[index][1]
            client = SDKClient()

            class SecureClient:
                @staticmethod
                def _create(**kwargs):
                    return client

            sdk = SimpleNamespace(SecureClient=SecureClient)
            venue = PolymarketClobV2Venue()
            with self._scope_race_loader(mutate), patch.dict(
                sys.modules, {"polymarket": sdk}
            ), patch.object(
                PolymarketClobV2Venue,
                "installed_sdk_version",
                return_value="0.9.2",
            ), patch.object(
                PolymarketClobV2Venue,
                "geoblock",
                return_value={"blocked": False, "close_only": False},
            ), patch.object(
                PolymarketClobV2Venue,
                "market_context",
                return_value=context,
            ), patch.object(
                PolymarketClobV2Venue,
                "balance",
                return_value=Decimal("10"),
            ):
                self.assertBlocked(
                    reason,
                    lambda: self.service.submit(
                        signal_id=signal_id,
                        candidate_id="C123",
                        market_id="m",
                        token_id="yes",
                        side="BUY",
                        paper_expected_price=Decimal("0.50"),
                        venue=venue,
                    ),
                )
            self.assertFalse(client.post_calls)
            signal = self.service.get_signal(signal_id)
            self.assertEqual(signal["reason"], reason)

    def test_kill_during_official_allowance_blocks_post_order(self):
        self.arm()
        signal_id = "official-killed-before-sink"
        self._ensure_direct_signal(signal_id)

        class SDKClient:
            def __init__(self):
                self.post_calls = []
                self.closed = False
                self._ctx = {
                    "environment_config": {
                        "exchange_v3": "0xexchange-v3",
                    }
                }

            def create_limit_order(self, **kwargs):
                return {"maker_amount": "1000000"}

            def get_balance_allowance(self, **kwargs):
                self.service.kill()
                return {
                    "balance": "2500000",
                    "allowances": {"0xexchange-v3": "1000000"},
                }

            def post_order(self, signed):
                self.post_calls.append(signed)
                return {"ok": True, "order_id": "unexpected"}

            def close(self):
                self.closed = True

        client = SDKClient()
        client.service = self.service

        class SecureClient:
            @staticmethod
            def _create(**kwargs):
                return client

        context = {
            "asset_id": "position-yes",
            "market_version": "v2",
            "neg_risk": False,
            "accepting_orders": True,
            "min_order_size": "1",
            "tick_size": "0.01",
            "bids": [{"price": "0.49", "size": "100"}],
            "asks": [{"price": "0.50", "size": "100"}],
            "fee_bps": "10",
        }
        sdk = SimpleNamespace(SecureClient=SecureClient)
        with patch.dict(sys.modules, {"polymarket": sdk}), patch.object(
            PolymarketClobV2Venue,
            "installed_sdk_version",
            return_value="0.9.2",
        ), patch.object(
            PolymarketClobV2Venue,
            "geoblock",
            return_value={"blocked": False, "close_only": False},
        ), patch.object(
            PolymarketClobV2Venue,
            "market_context",
            return_value=context,
        ):
            with self.assertRaisesRegex(CanaryBlocked, "CANARY_KILLED"):
                self.service.submit(
                    signal_id=signal_id,
                    candidate_id="C123",
                    market_id="m",
                    token_id="yes",
                    side="BUY",
                    paper_expected_price=Decimal("0.50"),
                    venue=PolymarketClobV2Venue(),
                )
        self.assertFalse(client.post_calls)
        self.assertTrue(client.closed)

    def test_cross_process_kill_does_not_wait_for_blocked_submission(self):
        self.arm()
        self._ensure_direct_signal("cross-process")
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
        self.assertBlocked("CANARY_SIGNAL_NOT_FOUND", lambda: self.submit(candidate_id="C999", venue=venue))
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
    def test_autonomous_scan_reaches_persisted_rank_after_1000_without_exceeding_tick_cap(self):
        self.service.enable_autonomous_micro_live()
        ranking_run_id = "persisted-run-after-1000"
        ranking_timestamp = T0.isoformat()
        rows = [
            (
                f"scan-candidate-{rank:04d}",
                ranking_run_id,
                ranking_timestamp,
                rank,
                1.0 - rank / 10000.0,
                json.dumps({"expectancy": 0.5}, sort_keys=True),
                json.dumps({"formula_version": "test"}, sort_keys=True),
                f"scan-cluster-{rank:04d}",
                1,
                0,
                "",
                "qualification-hash",
                "ranking-hash",
            )
            for rank in range(1, 1002)
        ]
        scan_rows = [
            {
                "candidate_id": row[0],
                "rank": row[3],
                "total_score": row[4],
                "cluster_key": row[7],
                "cluster_representative": row[8],
                "reason": row[10],
                "qualification_hash": row[11],
            }
            for row in rows
        ]
        with self.store.connection:
            self.store.connection.executemany(
                "INSERT INTO canary_rankings("
                "candidate_id,ranking_run_id,ranking_timestamp,rank,total_score,"
                "component_scores_json,evidence_versions_json,cluster_key,"
                "cluster_representative,selected,reason,qualification_hash,"
                "ranking_snapshot_hash) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                rows,
            )

        checked: list[str] = []

        def evaluate_signal(candidate_id, *, cycle_id=None):
            checked.append(candidate_id)
            if candidate_id == "scan-candidate-1001":
                signal = {
                    "status": "READY",
                    "signal_id": "scan-signal-1001",
                    "candidate_id": candidate_id,
                }
                return {
                    "candidate_id": candidate_id,
                    "evaluated_at": T0.isoformat(),
                    "reason_code": "READY_SIGNAL",
                    "market_id": "scan-market-1001",
                    "signal": signal,
                    "required_health": {"grade": "A"},
                    "evidence": {"cycle_id": cycle_id},
                }
            return {
                "candidate_id": candidate_id,
                "evaluated_at": T0.isoformat(),
                "reason_code": "NO_STRATEGY_SIGNAL",
                "market_id": None,
                "signal": None,
                "required_health": {"grade": "A"},
                "evidence": {"cycle_id": cycle_id},
            }


        worker = AutonomousCanaryWorker(self.store, clock=lambda: T0)
        ranking = {
            "ranking_run_id": ranking_run_id,
            "eligible_count": 1001,
            "rankable_count": 1001,
        }
        with patch.object(
            CandidateCanaryRanker,
            "evaluate_and_select",
            return_value=ranking,
        ), patch.object(
            CandidateCanaryRanker,
            "validate_persisted_ranking",
            return_value=True,
        ), patch.object(
            AutonomousCanaryWorker,
            "_current_rankings",
            return_value=scan_rows,
        ), patch.object(
            CanaryService,
            "evaluate_signal",
            side_effect=evaluate_signal,
        ), patch.object(
            CredentialStore,
            "configured",
            return_value=False,
        ):
            results = [worker.tick(now=T0) for _ in range(101)]

        self.assertEqual(len(checked), 1001)
        self.assertEqual(checked[-1], "scan-candidate-1001")
        self.assertEqual(
            max(result["candidates_signal_checked"] for result in results),
            10,
        )
        self.assertEqual(results[-1]["candidates_signal_checked"], 1)
        self.assertEqual(results[-1]["candidate_id"], "scan-candidate-1001")
        self.assertEqual(results[-1]["blocker"], "CREDENTIALS_NOT_CONFIGURED")

    def test_published_readiness_projects_scan_cycle_fields_consistently(self):
        self.service.enable_autonomous_micro_live()
        ranking_run_id = "readiness-cycle-run"
        scan_rows = [
            {
                "candidate_id": f"readiness-candidate-{rank:02d}",
                "rank": rank,
                "total_score": 1.0 - rank / 100.0,
                "cluster_key": f"readiness-cluster-{rank:02d}",
                "cluster_representative": 1,
                "qualification_hash": f"qualification-{rank:02d}",
            }
            for rank in range(1, 13)
        ]
        ranking = {
            "ranking_run_id": ranking_run_id,
            "eligible_count": len(scan_rows),
            "rankable_count": len(scan_rows),
        }
        worker = AutonomousCanaryWorker(self.store, clock=lambda: T0)
        with patch.object(
            CandidateCanaryRanker,
            "evaluate_and_select",
            return_value=ranking,
        ), patch.object(
            CandidateCanaryRanker,
            "validate_persisted_ranking",
            return_value=True,
        ), patch.object(
            AutonomousCanaryWorker,
            "_current_rankings",
            return_value=scan_rows,
        ), patch.object(
            CanaryService,
            "generate_signal",
            return_value=None,
        ):
            result = worker.tick(now=T0)

        self.assertEqual(result["signal_scan_cycle_complete"], 0)
        self.assertEqual(result["signal_scan_checked_this_cycle"], 10)
        self.assertEqual(result["signal_scan_remaining_this_cycle"], 2)
        self.assertEqual(result["signal_scan_status"], "IN_PROGRESS")
        published = self.service.publish_readiness_snapshot(reason="SCAN_PUBLISHED")
        read_back = self.service.readiness_snapshot()
        report = self.service.status_report()
        worker_projection = report["worker"]
        state = self.store.connection.execute(
            "SELECT signal_scan_cycle_id,signal_scan_candidate_universe_hash,"
            "signal_scan_cycle_started_at,signal_scan_cycle_completed_at,"
            "signal_scan_cycle_complete,signal_scan_checked_this_cycle,"
            "signal_scan_remaining_this_cycle,signal_scan_coverage_percentage,"
            "signal_scan_skip_reasons_json,signal_scan_status,worker_status "
            "FROM canary_autonomous_state WHERE singleton=1"
        ).fetchone()
        cycle_fields = (
            "signal_scan_cycle_id",
            "signal_scan_candidate_universe_hash",
            "signal_scan_cycle_started_at",
            "signal_scan_cycle_completed_at",
            "signal_scan_cycle_complete",
            "signal_scan_checked_this_cycle",
            "signal_scan_remaining_this_cycle",
            "signal_scan_coverage_percentage",
            "signal_scan_skip_reasons_json",
            "signal_scan_status",
        )
        self.assertEqual(state["signal_scan_cycle_id"], result["signal_scan_cycle_id"])
        self.assertEqual(state["signal_scan_checked_this_cycle"], 10)
        self.assertEqual(state["signal_scan_remaining_this_cycle"], 2)
        self.assertEqual(state["signal_scan_status"], "IN_PROGRESS")
        self.assertEqual(published["readiness_snapshot_status"], "CURRENT")
        self.assertEqual(read_back["readiness_snapshot_status"], "CURRENT")
        for field in cycle_fields:
            with self.subTest(field=field):
                expected = state[field]
                if field == "signal_scan_skip_reasons_json":
                    expected = json.loads(expected)
                for projection in (
                    result,
                    published,
                    read_back,
                    published["autonomous"],
                    read_back["autonomous"],
                    worker_projection,
                ):
                    actual = projection[field]
                    if field == "signal_scan_skip_reasons_json" and isinstance(actual, str):
                        actual = json.loads(actual)
                    self.assertEqual(actual, expected)

    def test_durable_scan_schema_and_autonomous_risk_envelope_are_persisted(self):
        state_columns = {
            row["name"]
            for row in self.store.connection.execute(
                "PRAGMA table_info(canary_autonomous_state)"
            ).fetchall()
        }
        self.assertTrue(
            {
                "signal_scan_cycle_id",
                "signal_scan_candidate_universe_hash",
                "signal_scan_cycle_started_at",
                "signal_scan_cycle_completed_at",
                "signal_scan_cycle_complete",
                "signal_scan_checked_this_cycle",
                "signal_scan_remaining_this_cycle",
                "signal_scan_coverage_percentage",
                "signal_scan_skip_reasons_json",
                "signal_scan_status",
            }.issubset(state_columns)
        )
        checked_columns = {
            row["name"]: row["pk"]
            for row in self.store.connection.execute(
                "PRAGMA table_info(canary_signal_scan_checked)"
            ).fetchall()
        }
        self.assertEqual(
            {
                "cycle_id": 1,
                "candidate_id": 2,
                "qualification_hash": 3,
            },
            {
                key: checked_columns.get(key)
                for key in ("cycle_id", "candidate_id", "qualification_hash")
            },
        )
        self.assertEqual(self.service.autonomous_limits(), AUTONOMOUS_CANARY_LIMITS)
        self.assertFalse(PRODUCTION_LIVE_EXECUTION)
        self.service.enable_autonomous_micro_live()
        control = self.store.connection.execute(
            "SELECT limits_json FROM canary_control WHERE singleton=1"
        ).fetchone()
        self.assertEqual(json.loads(control["limits_json"]), AUTONOMOUS_CANARY_LIMITS)
        self.assertEqual(
            self.store.connection.execute(
                "SELECT COUNT(*) FROM canary_ledger"
            ).fetchone()[0],
            0,
        )


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

    def _add_candidate(
        self,
        candidate_id,
        *,
        market_ids=("m",),
        frozen_filters=None,
        strategy=None,
        model=None,
        dataset_market_ids=(),
        experiment_plan=None,
        scope_records=None,
    ):
        strategy = {**(strategy or self.strategy), "strategy_id": candidate_id}
        model = dict(model or self.model)
        strategy_hash = self.service._document_hash(strategy)
        model_hash = self.service._document_hash(model)
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
                **(
                    {"historical_market_ids": list(dataset_market_ids)}
                    if dataset_market_ids
                    else {}
                ),
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
            "model_document": model,
            "market_ids": list(market_ids),
        }
        if frozen_filters:
            payload["frozen_filters"] = dict(frozen_filters)
        if experiment_plan is not None:
            payload["experiment_plan"] = dict(experiment_plan)
        scope_market_ids = tuple(str(item) for item in market_ids)
        if isinstance(experiment_plan, dict):
            target = experiment_plan.get("target")
            if isinstance(target, dict) and target.get("market_ids"):
                scope_market_ids = tuple(str(item) for item in target["market_ids"])
        _persist_test_scope(
            self.store,
            candidate_id,
            payload,
            scope_market_ids,
            market_records=scope_records,
        )
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
        market_id="m",
        active=True,
        closed=None,
        price="0.50",
        settlement="open",
        source_timestamp=None,
        observed_at=None,
        category=None,
        feature_probability=None,
    ):
        source_timestamp = source_timestamp or T0
        observed_at = observed_at or self.now
        metadata = {"category": category} if category is not None else {}
        nested = {
            "market_id": market_id,
            "timestamp": source_timestamp.isoformat(),
            "yes_mid": price,
            "yes_ask": price,
            "yes_order_book": {
                "asks": [{"price": price, "size": "100"}],
                "bids": [],
                "timestamp": source_timestamp.isoformat(),
                "token_id": "yes",
            },
            "no_order_book": {
                "asks": [{"price": price, "size": "100"}],
                "bids": [],
                "timestamp": source_timestamp.isoformat(),
                "token_id": "no",
            },
            "no_ask": price,
            "yes_token_id": "yes",
            "no_token_id": "no",
            "settlement": settlement,
        }
        if feature_probability is not None:
            nested["feature_probability"] = feature_probability
        payload = {
            "source_type": "FORWARD_COLLECTED",
            "snapshot": nested,
            "yes_token_id": "yes",
            "no_token_id": "no",
            "settlement": settlement,
            "active": active,
            "metadata": metadata,
        }
        if closed is not None:
            payload["closed"] = closed
        self.store.save_polymarket_snapshot(
            snapshot_id,
            market_id,
            source_timestamp,
            observed_at,
            payload,
            source_type="FORWARD_COLLECTED",
        )

    def _signal(self, candidate_id="C"):
        signal = self.service.generate_signal(candidate_id)
        self.assertIsNotNone(signal)
        assert signal is not None
        return signal
    def _save_metadata(
        self,
        market_id,
        *,
        active=True,
        closed=False,
        settlement="open",
        category=None,
        observed_at=None,
    ):
        metadata = {
            "source_type": "FORWARD_COLLECTED",
            "active": active,
            "closed": closed,
            "metadata": {"category": category} if category is not None else {},
            "snapshot": {
                "market_id": market_id,
                "settlement": settlement,
                "expiry": (T0 + timedelta(days=1)).isoformat(),
            },
        }
        self.store.save_polymarket_market_metadata(
            market_id,
            metadata,
            observed_at=observed_at or self.now,
            source_type="FORWARD_COLLECTED",
        )

    def _assert_evaluation(
        self,
        candidate_id,
        reason_code,
        *,
        market_id=None,
        cycle_id=None,
        expect_signal=False,
        assert_market_id=False,
    ):
        cycle_id = cycle_id or f"cycle-{candidate_id}"
        result = self.service.evaluate_signal(candidate_id, cycle_id=cycle_id)
        for field in (
            "candidate_id",
            "evaluated_at",
            "reason_code",
            "market_id",
            "signal",
            "required_health",
            "evidence",
        ):
            self.assertIn(field, result)
        self.assertEqual(result["candidate_id"], candidate_id)
        self.assertEqual(result["reason_code"], reason_code)
        if assert_market_id:
            self.assertEqual(result["market_id"], market_id)
        if expect_signal:
            self.assertIsInstance(result["signal"], dict)
        else:
            self.assertIsNone(result["signal"])
        self.assertIsInstance(result["required_health"], dict)
        self.assertIsInstance(result["evidence"], dict)
        row = self.store.connection.execute(
            "SELECT reason_code, market_id, signal_json, required_health_json, evidence_json "
            "FROM canary_signal_evaluations "
            "WHERE candidate_id=? AND cycle_id=? ORDER BY evaluated_at DESC LIMIT 1",
            (candidate_id, cycle_id),
        ).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row["reason_code"], reason_code)
        self.assertEqual(row["market_id"], result["market_id"])
        persisted_signal = (
            json.loads(row["signal_json"]) if row["signal_json"] is not None else None
        )
        self.assertEqual(persisted_signal, result["signal"])
        self.assertEqual(json.loads(row["required_health_json"]), result["required_health"])
        self.assertEqual(json.loads(row["evidence_json"]), result["evidence"])
        listed = self.service.list_signal_evaluations(
            candidate_id=candidate_id, cycle_id=cycle_id, limit=10
        )
        self.assertEqual(len(listed), 1)
        self.assertEqual(listed[0]["reason_code"], reason_code)
        return result

    def test_evaluate_signal_ready_persists_exact_fresh_evidence_and_wrapper_is_compatible(self):
        result = self.service.evaluate_signal("C", cycle_id="ready-cycle")
        self.assertEqual(result["reason_code"], "READY_SIGNAL")
        self.assertEqual(result["candidate_id"], "C")
        self.assertEqual(result["market_id"], "m")
        self.assertIsInstance(result["signal"], dict)
        self.assertEqual(result["signal"]["status"], "READY")
        self.assertEqual(result["required_health"]["grade"], "A")
        self.assertEqual(
            result["evidence"]["current_execution_evidence"],
            "CURRENT_ORDER_BOOK",
        )
        self.assertEqual(self.service.generate_signal("C"), result["signal"])
        rows = self.service.list_signal_evaluations(candidate_id="C", limit=10)
        self.assertGreaterEqual(len(rows), 2)
        self.assertEqual(rows[0]["candidate_id"], "C")

    def test_no_strategy_signal_is_a_persisted_non_actionable_outcome(self):
        self._add_candidate(
            "no-strategy",
            market_ids=("no-strategy-market",),
            model={"probability": 0.50},
        )
        self._save_snapshot(
            "no-strategy-snapshot",
            market_id="no-strategy-market",
            price="0.50",
        )
        self._assert_evaluation(
            "no-strategy",
            "STRATEGY_EVALUATED_DECLINED",
            market_id="no-strategy-market",
            assert_market_id=True,
        )

    def test_missing_forward_snapshot_is_distinct_from_unresolved_authority(self):
        self._add_candidate(
            "missing-forward",
            market_ids=("missing-forward-market",),
        )
        self._save_metadata("missing-forward-market")
        self._assert_evaluation(
            "missing-forward",
            "NO_FORWARD_SNAPSHOT",
            market_id="missing-forward-market",
        )

    def test_stale_forward_evidence_is_rejected_even_when_market_is_authorized(self):
        stale = T0 - timedelta(seconds=61)
        self._add_candidate("stale-forward", market_ids=("stale-market",))
        self._save_snapshot(
            "stale-snapshot",
            market_id="stale-market",
            source_timestamp=stale,
            observed_at=stale,
        )
        self._assert_evaluation(
            "stale-forward",
            "STALE_FORWARD_EVIDENCE",
            market_id="stale-market",
        )

    def test_current_signal_market_normalizes_persisted_closed_flags(self):
        open_market = "string-false-open-market"
        closed_market = "string-true-closed-market"
        self._save_snapshot(
            "string-false-open-snapshot",
            market_id=open_market,
            closed="false",
        )
        self._save_snapshot(
            "string-true-closed-snapshot",
            market_id=closed_market,
            closed="true",
        )

        self.assertIsNotNone(
            self.service._current_signal_market(open_market, now=self.now)
        )
        self.assertIsNone(
            self.service._current_signal_market(closed_market, now=self.now)
        )

        self._add_candidate("string-false-open-candidate", market_ids=(open_market,))
        self._add_candidate("string-true-closed-candidate", market_ids=(closed_market,))
        self._assert_evaluation(
            "string-false-open-candidate",
            "READY_SIGNAL",
            market_id=open_market,
            cycle_id="string-false-open-evaluation-cycle",
            expect_signal=True,
            assert_market_id=True,
        )
        self._assert_evaluation(
            "string-true-closed-candidate",
            "MARKET_CLOSED",
            market_id=closed_market,
            cycle_id="string-true-closed-evaluation-cycle",
            assert_market_id=True,
        )

    def test_closed_market_is_not_reclassified_as_missing_or_unresolved(self):
        self._add_candidate(
            "closed-forward",
            market_ids=("unresolved-declared", "closed-market"),
        )
        self._save_metadata(
            "closed-market",
            active=False,
            closed=True,
            settlement="closed",
        )
        self._assert_evaluation(
            "closed-forward",
            "MARKET_CLOSED",
            market_id="closed-market",
        )

    def test_frozen_filter_mismatch_is_an_exact_non_actionable_reason(self):
        self._add_candidate(
            "filter-mismatch",
            market_ids=("sports-market",),
            frozen_filters={"category": "politics"},
            scope_records={
                "sports-market": {"metadata": {"category": "sports"}},
            },
        )
        self._save_metadata("sports-market", category="sports")
        self._assert_evaluation(
            "filter-mismatch",
            "MARKET_FILTER_MISMATCH",
            market_id="sports-market",
        )

    def test_declared_missing_target_is_persisted_as_exact_market_scoped_reason(self):
        missing_market = "never-seen"
        self._add_candidate(
            "unresolved-forward",
            market_ids=(missing_market,),
            scope_records={missing_market: None},
        )
        result = self._assert_evaluation(
            "unresolved-forward",
            "NO_FORWARD_SNAPSHOT",
            market_id=missing_market,
            assert_market_id=True,
        )
        self.assertEqual(result["market_id"], missing_market)


    def test_declared_missing_target_remains_diagnostic_only(self):
        candidate_id = "missing-diagnostic"
        missing_market = "missing-declared"
        authorized_market = "authorized-no-signal"
        self._add_candidate(
            candidate_id,
            market_ids=(missing_market, authorized_market),
            model={"probability": 0.50},
            scope_records={
                missing_market: None,
                authorized_market: {},
            },
        )
        self._save_snapshot(
            "authorized-no-signal-for-missing",
            market_id=authorized_market,
        )

        with patch.object(
            self.service,
            "_forward_snapshot_rows",
            wraps=self.service._forward_snapshot_rows,
        ) as load_rows:
            result = self._assert_evaluation(
                candidate_id,
                "NO_FORWARD_SNAPSHOT",
                market_id=missing_market,
                cycle_id="missing-diagnostic-cycle",
                assert_market_id=True,
            )

        evaluated_market_ids = [call.args[0] for call in load_rows.call_args_list]
        self.assertEqual(evaluated_market_ids, [authorized_market])
        self.assertNotIn(missing_market, evaluated_market_ids)
        self.assertEqual(
            result["evidence"]["market_failures"],
            [
                {
                    "market_id": authorized_market,
                    "reason_code": "STRATEGY_EVALUATED_DECLINED",
                },
                {
                    "market_id": missing_market,
                    "reason_code": "NO_FORWARD_SNAPSHOT",
                },
            ],
        )

    def test_required_collector_health_blocks_a_ready_candidate(self):
        health = {
            "candidate_bound_markets": ["m"],
            "scheduled": ["m"],
            "fresh": ["m"],
            "stale": [],
            "missing": [],
            "grade": "C",
            "grade_scope": "required_forward_markets",
            "reason_code": "COLLECTOR_DEGRADED",
            "candidate_references": {"m": ["C"]},
        }
        with patch.object(
            self.store,
            "polymarket_required_health",
            return_value=health,
        ):
            self._assert_evaluation(
                "C",
                "COLLECTOR_CANDIDATE_HEALTH_BLOCKED",
                market_id="m",
                cycle_id="health-cycle",
                assert_market_id=True,
            )

    def test_historical_validation_market_is_ignored_for_current_executable_authority(self):
        self._add_candidate(
            "historical-independent",
            market_ids=("current-market",),
            dataset_market_ids=("historical-market",),
        )
        self._save_snapshot(
            "current-snapshot",
            market_id="current-market",
        )
        requirements = self.store.candidate_forward_requirements(
            candidate_ids=["historical-independent"], now=self.now
        )
        candidate = requirements["candidates"][0]
        self.assertEqual(candidate["historical_market_ids_ignored"], [])
        self.assertNotIn("historical-market", candidate["market_ids"])
        self.assertEqual(candidate["market_ids"], ["current-market"])
        result = self._assert_evaluation(
            "historical-independent",
            "READY_SIGNAL",
            market_id="current-market",
            expect_signal=True,
            assert_market_id=True,
        )
        self.assertEqual(result["signal"]["market_id"], "current-market")

    def test_frozen_filters_select_only_the_matching_current_market(self):
        self._add_candidate(
            "filter-match",
            market_ids=("politics-market",),
            frozen_filters={"category": "politics"},
        )
        self._save_snapshot(
            "politics-snapshot",
            market_id="politics-market",
            category="politics",
        )
        result = self._assert_evaluation(
            "filter-match",
            "READY_SIGNAL",
            market_id="politics-market",
            expect_signal=True,
            assert_market_id=True,
        )
        self.assertEqual(result["signal"]["market_id"], "politics-market")

    def test_filter_excluded_declared_market_never_enters_signal_evaluation(self):
        candidate_id = "excluded-target-not-executable"
        authorized_market = "authorized-no-signal"
        excluded_market = "instrument-filter-excluded"
        self._add_candidate(
            candidate_id,
            market_ids=(authorized_market, excluded_market),
            frozen_filters={"category": "politics"},
            model={"probability": 0.50},
            scope_records={
                authorized_market: {"metadata": {"category": "politics"}},
                excluded_market: {"metadata": {"category": "sports"}},
            },
        )
        self._save_snapshot(
            "authorized-no-signal-snapshot",
            market_id=authorized_market,
            category="politics",
        )
        self._save_snapshot(
            "excluded-ready-snapshot",
            market_id=excluded_market,
            category="sports",
        )

        with patch.object(
            self.service,
            "_forward_snapshot_rows",
            wraps=self.service._forward_snapshot_rows,
        ) as load_rows, patch.object(
            self.service,
            "_apply_signal_model",
            wraps=self.service._apply_signal_model,
        ) as apply_model:
            result = self._assert_evaluation(
                candidate_id,
                "MARKET_FILTER_MISMATCH",
                market_id=excluded_market,
                cycle_id="excluded-target-cycle",
                assert_market_id=True,
            )

        evaluated_market_ids = [call.args[0] for call in load_rows.call_args_list]
        self.assertEqual(evaluated_market_ids, [authorized_market])
        self.assertNotIn(excluded_market, evaluated_market_ids)
        self.assertEqual(apply_model.call_count, 1)
        self.assertEqual(result["evidence"]["market_failures"], [
            {
                "market_id": authorized_market,
                "reason_code": "STRATEGY_EVALUATED_DECLINED",
            },
            {
                "market_id": excluded_market,
                "reason_code": "MARKET_FILTER_MISMATCH",
            },
        ])

    def test_nested_plan_closed_target_returns_exact_market_scoped_reason_without_model_evaluation(self):
        candidate_id = "nested-closed-target"
        closed_market = "nested-closed-market"
        self._add_candidate(
            candidate_id,
            market_ids=("legacy-top-level-market",),
            experiment_plan={
                "hypothesis_id": candidate_id,
                "market_type": "prediction",
                "template": "probability_mispricing",
                "parameters": {"threshold": [0.05]},
                "target": {"market_ids": [closed_market]},
                "dataset_selector": {
                    "dataset_id": "prediction-history",
                    "dataset_version": "v1",
                },
                "experiment_family": "probability_mispricing",
                "max_variants": 1,
                "min_samples": 30,
                "min_trades": 0,
                "paper_only": True,
            },
        )
        requirements = self.store.candidate_forward_requirements(
            candidate_ids=[candidate_id],
            now=self.now,
        )
        candidate_entry = requirements["candidates"][0]
        self.assertEqual(candidate_entry["declared_market_ids"], [closed_market])
        self.assertEqual(candidate_entry["market_ids"], [])
        self._save_metadata(
            closed_market,
            active=False,
            closed=True,
            settlement="closed",
        )

        with patch.object(
            self.service,
            "_forward_snapshot_rows",
            wraps=self.service._forward_snapshot_rows,
        ) as load_rows, patch.object(
            self.service,
            "_apply_signal_model",
            wraps=self.service._apply_signal_model,
        ) as apply_model:
            result = self._assert_evaluation(
                candidate_id,
                "MARKET_CLOSED",
                market_id=closed_market,
                cycle_id="nested-closed-target-cycle",
                assert_market_id=True,
            )

        self.assertEqual(load_rows.call_count, 0)
        self.assertEqual(apply_model.call_count, 0)
        self.assertEqual(
            result["evidence"]["market_failures"],
            [
                {
                    "market_id": closed_market,
                    "reason_code": "MARKET_CLOSED",
                }
            ],
        )

    def test_nested_plan_filter_exclusion_is_diagnostic_only_and_never_ready(self):
        candidate_id = "nested-filter-excluded-target"
        authorized_market = "nested-authorized-market"
        excluded_market = "nested-filter-excluded-market"
        self._add_candidate(
            candidate_id,
            market_ids=("legacy-top-level-market",),
            model={"probability": 0.50},
            experiment_plan={
                "hypothesis_id": candidate_id,
                "market_type": "prediction",
                "template": "probability_mispricing",
                "parameters": {"threshold": [0.05]},
                "filters": {"category": "politics"},
                "target": {
                    "market_ids": [authorized_market, excluded_market],
                },
                "dataset_selector": {
                    "dataset_id": "prediction-history",
                    "dataset_version": "v1",
                },
                "experiment_family": "probability_mispricing",
                "max_variants": 1,
                "min_samples": 30,
                "min_trades": 0,
                "paper_only": True,
            },
            scope_records={
                authorized_market: {"metadata": {"category": "politics"}},
                excluded_market: {"metadata": {"category": "sports"}},
            },
        )
        self._save_metadata(authorized_market, category="politics")
        self._save_metadata(excluded_market, category="sports")
        self._save_snapshot(
            "nested-authorized-snapshot",
            market_id=authorized_market,
            category="politics",
        )
        self._save_snapshot(
            "nested-excluded-snapshot",
            market_id=excluded_market,
            category="sports",
        )
        requirements = self.store.candidate_forward_requirements(
            candidate_ids=[candidate_id],
            now=self.now,
        )
        candidate_entry = requirements["candidates"][0]
        self.assertEqual(
            candidate_entry["declared_market_ids"],
            [authorized_market, excluded_market],
        )
        self.assertEqual(candidate_entry["market_ids"], [authorized_market])

        with patch.object(
            self.service,
            "_forward_snapshot_rows",
            wraps=self.service._forward_snapshot_rows,
        ) as load_rows, patch.object(
            self.service,
            "_apply_signal_model",
            wraps=self.service._apply_signal_model,
        ) as apply_model:
            result = self._assert_evaluation(
                candidate_id,
                "MARKET_FILTER_MISMATCH",
                market_id=excluded_market,
                cycle_id="nested-filter-excluded-target-cycle",
                assert_market_id=True,
            )

        evaluated_market_ids = [call.args[0] for call in load_rows.call_args_list]
        self.assertEqual(evaluated_market_ids, [authorized_market])
        self.assertNotIn(excluded_market, evaluated_market_ids)
        self.assertEqual(apply_model.call_count, 1)
        self.assertEqual(
            result["evidence"]["market_failures"],
            [
                {
                    "market_id": authorized_market,
                    "reason_code": "STRATEGY_EVALUATED_DECLINED",
                },
                {
                    "market_id": excluded_market,
                    "reason_code": "MARKET_FILTER_MISMATCH",
                },
            ],
        )

    def test_declared_closed_and_filter_targets_beyond_catalog_page_remain_exact_diagnostics(self):
        candidate_id = "paginated-diagnostic-targets"
        closed_market = "zz-paginated-closed-target"
        excluded_market = "zz-paginated-filter-target"
        self._add_candidate(
            candidate_id,
            market_ids=("legacy-top-level-market",),
            experiment_plan={
                "hypothesis_id": candidate_id,
                "market_type": "prediction",
                "template": "probability_mispricing",
                "parameters": {"threshold": [0.05]},
                "filters": {"category": "politics"},
                "target": {
                    "market_ids": [closed_market, excluded_market],
                },
                "dataset_selector": {
                    "dataset_id": "prediction-history",
                    "dataset_version": "v1",
                },
                "experiment_family": "probability_mispricing",
                "max_variants": 1,
                "min_samples": 30,
                "min_trades": 0,
                "paper_only": True,
            },
            scope_records={
                closed_market: {
                    "active": False,
                    "open": False,
                    "closed": True,
                    "settlement": "closed",
                    "metadata": {"category": "politics"},
                },
                excluded_market: {"metadata": {"category": "sports"}},
            },
        )
        for index in range(1001):
            self._save_metadata(f"distractor-{index:04d}", category="politics")
        self._save_metadata(
            closed_market,
            active=False,
            closed=True,
            settlement="closed",
            category="politics",
        )
        # String false is a valid persisted flag and must not outrank the
        # frozen-filter diagnostic as a truthy Python value.
        self._save_metadata(
            excluded_market,
            active=True,
            closed="false",
            settlement="open",
            category="sports",
        )

        requirements = self.store.candidate_forward_requirements(
            candidate_ids=[candidate_id],
            now=self.now,
        )
        candidate_entry = requirements["candidates"][0]
        self.assertEqual(
            candidate_entry["declared_market_ids"],
            [closed_market, excluded_market],
        )
        self.assertEqual(candidate_entry["market_ids"], [])

        with patch.object(
            self.service,
            "_forward_snapshot_rows",
            wraps=self.service._forward_snapshot_rows,
        ) as load_rows, patch.object(
            self.service,
            "_apply_signal_model",
            wraps=self.service._apply_signal_model,
        ) as apply_model:
            result = self._assert_evaluation(
                candidate_id,
                "MARKET_CLOSED",
                market_id=closed_market,
                cycle_id="paginated-diagnostic-targets-cycle",
                assert_market_id=True,
            )

        self.assertEqual(load_rows.call_count, 0)
        self.assertEqual(apply_model.call_count, 0)
        self.assertEqual(
            result["evidence"]["market_failures"],
            [
                {
                    "market_id": closed_market,
                    "reason_code": "MARKET_CLOSED",
                },
                {
                    "market_id": excluded_market,
                    "reason_code": "MARKET_FILTER_MISMATCH",
                },
            ],
        )

    def test_unrelated_discovery_staleness_is_not_required_health(self):
        self._save_metadata("unrelated-stale")
        self._save_snapshot(
            "unrelated-stale-snapshot",
            market_id="unrelated-stale",
            source_timestamp=T0 - timedelta(minutes=10),
            observed_at=T0 - timedelta(minutes=10),
        )
        result = self._assert_evaluation(
            "C",
            "READY_SIGNAL",
            market_id="m",
            expect_signal=True,
            assert_market_id=True,
        )

    def test_non_actionable_first_authorized_market_does_not_block_later_ready_market(self):
        candidate_id = "multi-market-ready"
        first_market = "multi-market-missing"
        second_market = "multi-market-ready"
        self._add_candidate(
            candidate_id,
            market_ids=(first_market, second_market),
        )
        self._save_metadata(first_market)
        self._save_metadata(second_market)
        self._save_snapshot(
            "multi-market-ready-snapshot",
            market_id=second_market,
        )

        result = self.service.evaluate_signal(candidate_id, cycle_id="multi-ready-cycle")

        self.assertEqual(result["reason_code"], "READY_SIGNAL")
        self.assertEqual(result["market_id"], second_market)
        self.assertEqual(result["signal"]["market_id"], second_market)
        rows = self.service.list_signal_evaluations(
            candidate_id=candidate_id,
            cycle_id="multi-ready-cycle",
            limit=10,
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["reason_code"], "READY_SIGNAL")

    def test_all_market_failures_apply_severity_and_preserve_resolver_order(self):
        candidate_id = "multi-market-failure"
        closed_market = "multi-market-closed"
        stale_market = "multi-market-stale"
        missing_market = "multi-market-missing"
        self._add_candidate(
            candidate_id,
            market_ids=(closed_market, stale_market, missing_market),
            scope_records={
                closed_market: {
                    "active": False,
                    "open": False,
                    "closed": True,
                    "settlement": "closed",
                },
                stale_market: {},
                missing_market: None,
            },
        )
        self._save_metadata(closed_market)
        self._save_snapshot(
            "multi-market-closed-snapshot",
            market_id=closed_market,
            active=False,
            settlement="closed",
        )
        stale = T0 - timedelta(seconds=61)
        self._save_metadata(stale_market)
        self._save_snapshot(
            "multi-market-stale-snapshot",
            market_id=stale_market,
            source_timestamp=stale,
            observed_at=stale,
        )
        self._save_metadata(missing_market)

        result = self.service.evaluate_signal(candidate_id, cycle_id="multi-failure-cycle")

        self.assertEqual(result["reason_code"], "MARKET_CLOSED")
        self.assertEqual(result["market_id"], closed_market)
        self.assertEqual(result["evidence"]["market_id"], closed_market)
        # Matched markets are evaluated first; immutable excluded/deferred
        # diagnostics are appended afterward. Selection still uses the
        # explicit safety precedence, so MARKET_CLOSED wins over stale/missing.
        self.assertEqual(
            result["evidence"]["market_failures"],
            [
                {"market_id": stale_market, "reason_code": "STALE_FORWARD_EVIDENCE"},
                {"market_id": closed_market, "reason_code": "MARKET_CLOSED"},
                {"market_id": missing_market, "reason_code": "NO_FORWARD_SNAPSHOT"},
            ],
        )
        rows = self.service.list_signal_evaluations(
            candidate_id=candidate_id,
            cycle_id="multi-failure-cycle",
            limit=10,
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["market_id"], closed_market)

    def test_evaluation_rejects_more_than_eight_markets_without_evaluation(self):
        market_ids = tuple(f"over-cap-market-{index:02d}" for index in range(12))
        self._add_candidate("over-cap-candidate", market_ids=market_ids)
        with patch.object(
            self.service,
            "_forward_snapshot_rows",
            wraps=self.service._forward_snapshot_rows,
        ) as load_rows, patch.object(
            self.service,
            "_apply_signal_model",
            wraps=self.service._apply_signal_model,
        ) as apply_model:
            result = self.service.evaluate_signal(
                "over-cap-candidate",
                cycle_id="over-cap-cycle",
            )
        self.assertEqual(result["reason_code"], EXECUTION_FEASIBILITY_MARKET_CAP)
        self.assertIsNone(result["signal"])
        self.assertIsNone(result["market_id"])
        self.assertEqual(load_rows.call_count, 0)
        self.assertEqual(apply_model.call_count, 0)
        evidence = result["evidence"]
        self.assertEqual(evidence["resolved_market_count"], 12)
        self.assertEqual(evidence["declared_market_count"], 12)
        self.assertEqual(evidence["execution_market_cap"], CANARY_EXECUTION_MARKET_CAP)
        self.assertEqual(evidence["total_market_count"], 12)
        persisted = self.service.list_signal_evaluations(
            candidate_id="over-cap-candidate",
            cycle_id="over-cap-cycle",
            limit=10,
        )
        self.assertEqual(len(persisted), 1)
        self.assertEqual(persisted[0]["reason_code"], EXECUTION_FEASIBILITY_MARKET_CAP)
        self.assertEqual(
            persisted[0]["evidence"]["resolved_market_ids"],
            list(market_ids),
        )

    def test_evaluation_and_submission_keep_all_eight_markets_authorized(self):
        market_ids = tuple(f"within-cap-market-{index:02d}" for index in range(8))
        self._add_candidate("within-cap-candidate", market_ids=market_ids)
        for index, market_id in enumerate(market_ids):
            self._save_snapshot(
                f"within-cap-snapshot-{index:02d}",
                market_id=market_id,
            )
        result = self.service.evaluate_signal(
            "within-cap-candidate",
            cycle_id="within-cap-cycle",
        )
        self.assertEqual(result["reason_code"], "READY_SIGNAL")
        self.assertEqual(result["market_id"], market_ids[0])
        self.assertEqual(
            result["required_health"]["candidate_bound_markets"],
            list(market_ids),
        )
        self.assertEqual(result["evidence"]["resolved_market_ids"], list(market_ids))

        signal = result["signal"]
        self.assertIsInstance(signal, dict)
        assert signal is not None
        resolution = self.store.load_market_scope_resolution("within-cap-candidate")
        self.assertIsNotNone(resolution)
        assert resolution is not None
        expanded_resolution = MarketScopeResolution(
            candidate_id=resolution.candidate_id,
            scope_hash=resolution.scope_hash,
            scope_version=resolution.scope_version,
            resolved_at=T0 + timedelta(microseconds=1),
            status=resolution.status,
            reason=resolution.reason,
            policy=resolution.policy,
            matched_markets=(
                *resolution.matched_markets,
                replace(
                    resolution.matched_markets[0],
                    market_id="within-cap-overflow",
                    condition_id="within-cap-overflow-condition",
                    yes_token_id="within-cap-overflow-yes",
                    no_token_id="within-cap-overflow-no",
                ),
            ),
            excluded_markets=resolution.excluded_markets,
            deferred_markets=resolution.deferred_markets,
            provenance=resolution.provenance,
            schema_version=resolution.schema_version,
        )
        self.store.save_market_scope_resolution(expanded_resolution)
        self.now = T0 + timedelta(seconds=1)
        venue = FakeVenue()
        with self.assertRaisesRegex(
            CanaryBlocked,
            EXECUTION_FEASIBILITY_MARKET_CAP,
        ):
            self.service.submit_signal(
                signal["signal_id"],
                venue=venue,
                allow_test_venue=True,
            )
        self.assertFalse(venue.submissions)
        submitted_signal = self.service.get_signal(signal["signal_id"])
        self.assertEqual(submitted_signal["status"], "NO_LONGER_VALID")
        self.assertEqual(submitted_signal["reason"], EXECUTION_FEASIBILITY_MARKET_CAP)
        self.assertEqual(submitted_signal["evidence"]["resolved_market_count"], 9)
        self.assertEqual(
            submitted_signal["evidence"]["execution_market_cap"],
            CANARY_EXECUTION_MARKET_CAP,
        )

    def test_model_probability_is_derived_from_persisted_snapshot_feature(self):
        self._add_candidate(
            "derived-model",
            market_ids=("derived-market",),
            model={"field": "feature_probability"},
        )
        self._save_snapshot(
            "derived-model-snapshot",
            market_id="derived-market",
            feature_probability=0.80,
        )
        result = self._assert_evaluation(
            "derived-model",
            "READY_SIGNAL",
            market_id="derived-market",
            expect_signal=True,
            assert_market_id=True,
        )
        self.assertEqual(result["signal"]["evidence"]["model_probability"], 0.80)
        self.assertNotIn("model_probability", self.store.load_polymarket_snapshots(
            "derived-market", source_type="FORWARD_COLLECTED"
        )[0]["payload"]["snapshot"])

    def test_generate_and_evaluate_signal_make_no_network_calls(self):
        with patch(
            "urllib.request.urlopen",
            side_effect=AssertionError("signal evaluation must not access network"),
        ):
            evaluation = self.service.evaluate_signal("C", cycle_id="offline-cycle")
            signal = self.service.generate_signal("C")
        self.assertEqual(evaluation["reason_code"], "READY_SIGNAL")
        self.assertEqual(signal, evaluation["signal"])

    def test_ready_signal_does_not_bypass_arm_or_credential_gates(self):
        result = self.service.evaluate_signal("C", cycle_id="gate-cycle")
        venue = FakeVenue()
        with self.assertRaisesRegex(CanaryBlocked, "CANARY_NOT_ARMED"):
            self.service.submit_signal(
                result["signal"]["signal_id"],
                venue=venue,
                allow_test_venue=True,
            )
        self.assertFalse(venue.submissions)
        no_credentials = CanaryService(
            self.store,
            credentials=FakeCredentials(False),
            clock=lambda: self.now,
        )
        self.assertEqual(
            no_credentials.evaluate_signal("C", cycle_id="credential-cycle")["reason_code"],
            "READY_SIGNAL",
        )
        with self.assertRaisesRegex(CanaryBlocked, "CREDENTIALS_NOT_CONFIGURED"):
            no_credentials.arm(
                "C",
                venue=venue,
                credentials_configured=True,
            )

    def test_selected_candidate_stale_market_blocks_submission_before_order(self):
        signal = self._signal()
        self._save_snapshot(
            "newest-but-stale-source",
            market_id="m",
            source_timestamp=T0 - timedelta(minutes=2),
            observed_at=T0 + timedelta(seconds=1),
        )
        self.now = T0 + timedelta(seconds=30)
        self._arm()
        venue = FakeVenue()
        with self.assertRaisesRegex(CanaryBlocked, "CANARY_SIGNAL_STALE"):
            self.service.submit_signal(
                signal["signal_id"],
                venue=venue,
                allow_test_venue=True,
            )
        self.assertFalse(venue.submissions)
        self.assertEqual(
            self.service.get_signal(signal["signal_id"])["status"],
            "STALE",
        )

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
    def test_canary_signal_transient_retention_preserves_references_and_current_ready(self):
        current = self._signal()
        transient_rows = []
        for index in range(5):
            signal_id = f"retention-transient-{index}"
            generated_at = T0 - timedelta(minutes=5 - index)
            transient_rows.append(signal_id)
            self.store.connection.execute(
                "INSERT INTO canary_signals("
                "signal_id,candidate_id,frozen_hash,strategy_hash,model_hash,config_hash,"
                "market_id,token_id,outcome,side,paper_expected_price,source_snapshot_id,"
                "source_timestamp,generated_at,expires_at,status,reason,evidence_json,updated_at"
                ") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    signal_id,
                    f"retention-candidate-{index}",
                    "frozen",
                    "strategy",
                    "model",
                    "config",
                    "market",
                    "token",
                    "yes",
                    "BUY",
                    "0.50",
                    f"snapshot-{index}",
                    generated_at.isoformat(),
                    generated_at.isoformat(),
                    (generated_at - timedelta(seconds=1)).isoformat(),
                    "EXPIRED",
                    "SIGNAL_EXPIRED",
                    "{}",
                    generated_at.isoformat(),
                ),
            )
        self.store.connection.execute(
            "INSERT INTO canary_signal_evaluations("
            "evaluation_id,candidate_id,cycle_id,evaluated_at,reason_code,"
            "market_id,signal_id,signal_json,required_health_json,evidence_json"
            ") VALUES(?,?,?,?,?,?,?,?,?,?)",
            (
                "retention-note",
                "retention-note-candidate",
                "retention-note-cycle",
                (T0 - timedelta(minutes=4)).isoformat(),
                "READY_SIGNAL",
                "market",
                transient_rows[0],
                None,
                "{}",
                '{"note":"audit"}',
            ),
        )
        self.store.connection.execute(
            "INSERT INTO canary_ledger("
            "event_id,signal_id,timestamp,candidate_id,venue,market_id,token_id,side,"
            "requested_notional,paper_expected_price,max_price,status,evidence_json"
            ") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "retention-reservation",
                transient_rows[1],
                (T0 - timedelta(minutes=4)).isoformat(),
                "retention-order-candidate",
                "polymarket",
                "market",
                "token",
                "BUY",
                "1.00",
                "0.50",
                "0.50",
                "RESERVED",
                "{}",
            ),
        )
        self.store.connection.commit()

        with patch.object(canary_module, "CANARY_SIGNAL_TRANSIENT_RETENTION", 2):
            self.service.evaluate_signal("C", cycle_id="retention-cleanup")

        retained = {
            row["signal_id"]
            for row in self.store.connection.execute(
                "SELECT signal_id FROM canary_signals WHERE status='EXPIRED'"
            ).fetchall()
        }
        self.assertEqual(
            retained,
            {
                transient_rows[0],
                transient_rows[1],
                transient_rows[3],
                transient_rows[4],
            },
        )
        self.assertNotIn(transient_rows[2], retained)
        self.assertEqual(self.service.get_signal(current["signal_id"])["status"], "READY")
    def test_canary_signal_retention_bounds_older_unexpired_ready_duplicates(self):
        current = self._signal()
        ready_ids = []
        for index in range(4):
            signal_id = f"retention-ready-{index}"
            generated_at = T0 - timedelta(seconds=4 - index)
            ready_ids.append(signal_id)
            self.store.connection.execute(
                "INSERT INTO canary_signals("
                "signal_id,candidate_id,frozen_hash,strategy_hash,model_hash,config_hash,"
                "market_id,token_id,outcome,side,paper_expected_price,source_snapshot_id,"
                "source_timestamp,generated_at,expires_at,status,reason,evidence_json,updated_at"
                ") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    signal_id,
                    "retention-ready-candidate",
                    "frozen",
                    "strategy",
                    "model",
                    "config",
                    "market",
                    "token",
                    "yes",
                    "BUY",
                    "0.50",
                    f"ready-snapshot-{index}",
                    generated_at.isoformat(),
                    generated_at.isoformat(),
                    (T0 + timedelta(minutes=1)).isoformat(),
                    "READY",
                    None,
                    "{}",
                    generated_at.isoformat(),
                ),
            )
        self.store.connection.commit()

        with patch.object(canary_module, "CANARY_SIGNAL_TRANSIENT_RETENTION", 1):
            self.service.evaluate_signal("C", cycle_id="ready-retention-cleanup")

        candidate_rows = self.store.connection.execute(
            "SELECT signal_id FROM canary_signals "
            "WHERE candidate_id=? ORDER BY generated_at DESC,signal_id DESC",
            ("retention-ready-candidate",),
        ).fetchall()
        self.assertEqual(
            [row["signal_id"] for row in candidate_rows],
            [ready_ids[3], ready_ids[2]],
        )
        self.assertEqual(self.service.get_signal(current["signal_id"])["status"], "READY")

    def test_autonomous_binding_rejects_forged_rank_zero_representative(self):
        ranking = CandidateCanaryRanker(
            self.store,
            clock=lambda: self.now,
        ).evaluate_and_select(self.now)
        signal = self._signal()
        self.service.enable_autonomous_micro_live()
        with self.store.connection:
            self.store.connection.execute(
                "UPDATE canary_rankings SET rank=0, cluster_representative=1, "
                "reason=? WHERE candidate_id=?",
                ("FORGED_RANK_ZERO_REPRESENTATIVE", "C"),
            )
        forged = self.store.connection.execute(
            "SELECT rank,cluster_representative,reason FROM canary_rankings "
            "WHERE candidate_id=?",
            ("C",),
        ).fetchone()
        self.assertEqual(forged["rank"], 0)
        self.assertEqual(forged["cluster_representative"], 1)
        self.assertEqual(forged["reason"], "FORGED_RANK_ZERO_REPRESENTATIVE")

        control_candidate_before_bind = self.service.authoritative_status()[
            "control_candidate"
        ]
        with self.assertRaisesRegex(
            CanaryBlocked,
            "AUTONOMOUS_RANKING_NOT_CURRENT",
        ):
            self.service.bind_autonomous_actionable_candidate(
                "C",
                ranking_run_id=ranking["ranking_run_id"],
                signal_id=signal["signal_id"],
            )
        self.assertEqual(
            self.service.authoritative_status()["control_candidate"],
            control_candidate_before_bind,
        )
        self.assertEqual(
            self.store.connection.execute(
                "SELECT status FROM canary_signals WHERE signal_id=?",
                (signal["signal_id"],),
            ).fetchone()[0],
            "READY",
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
        venue = FakeVenue()
        with self.assertRaisesRegex(CanaryBlocked, "CANARY_SIGNAL_EXPIRED"):
            self.service.submit_signal(
                signal["signal_id"], venue=venue, allow_test_venue=True
            )
        persisted = self.service.get_signal(signal["signal_id"])
        self.assertEqual(persisted["status"], "REJECTED")
        self.assertEqual(persisted["reason"], "CANARY_SIGNAL_EXPIRED")
        self.assertFalse(venue.submissions)

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

class CanaryForwardAuthorityContractTests(unittest.TestCase):
    @staticmethod
    def _metadata(store: AxiomStore, market_id: str, category: str) -> None:
        store.save_polymarket_market_metadata(
            market_id,
            {
                "source_type": "FORWARD_COLLECTED",
                "active": True,
                "closed": False,
                "metadata": {"category": category},
                "snapshot": {
                    "market_id": market_id,
                    "settlement": "open",
                    "expiry": (T0 + timedelta(days=1)).isoformat(),
                },
            },
            observed_at=T0,
            source_type="FORWARD_COLLECTED",
        )

    @staticmethod
    def _freeze_candidate(store: AxiomStore, candidate_id: str, payload: dict[str, object]) -> None:
        canonical = dict(payload)
        if canonical.get("market_ids"):
            policy = normalize_market_scope(market_ids=canonical["market_ids"])
        else:
            policy = normalize_market_scope(
                filters=canonical.get("frozen_filters") or {"category": "politics"}
            )
        policy = normalize_market_scope(
            {**policy.as_dict(), "provenance": "canonical"}
        )
        canonical.update(
            {
                "market_scope": policy.as_dict(),
                "market_scope_hash": policy.scope_hash,
                "market_scope_version": policy.scope_version,
                "plan_hash": "sha256:test-plan",
                "dataset_selector": {
                    "dataset_id": "test-history",
                    "dataset_version": "v1",
                    "source_type": "HISTORICAL",
                },
                "dataset_attestation": {
                    "status": "CURRENT",
                    "hash": "sha256:test-dataset",
                },
            }
        )
        lifecycle = CandidateLifecycleManager(store)
        lifecycle.register_idea(candidate_id, {"candidate_id": candidate_id, **canonical})
        lifecycle.advance(candidate_id, CandidateStage.SCHEMA_VALIDATED, {"schema_valid": True})
        lifecycle.advance(candidate_id, CandidateStage.BACKTESTED, {"backtest_complete": True})
        lifecycle.advance(
            candidate_id,
            CandidateStage.VALIDATED,
            {"validation_complete": True, "holdout_used": False},
        )
        lifecycle.advance(
            candidate_id,
            CandidateStage.ROBUSTNESS_CHECKED,
            {"robustness_passed": True},
        )
        lifecycle.advance(
            candidate_id,
            CandidateStage.FROZEN,
            {
                "frozen": True,
                "holdout_used": False,
                "strategy_hash": "strategy-hash",
                "model_hash": "model-hash",
                "config_hash": "config-hash",
                "risk_snapshot": {"max_position_fraction": 0.05},
                "plan_hash": canonical["plan_hash"],
                "market_scope": canonical["market_scope"],
                "market_scope_hash": canonical["market_scope_hash"],
                "market_scope_version": canonical["market_scope_version"],
                "dataset_selector": canonical["dataset_selector"],
                "dataset_attestation": canonical["dataset_attestation"],
            },
        )

    def test_authority_order_and_candidate_references_are_stable_without_network_or_writes(self) -> None:
        with AxiomStore(":memory:") as store:
            self._metadata(store, "current-politics", "politics")
            self._metadata(store, "current-sports", "sports")
            self._freeze_candidate(
                store,
                "candidate-filter",
                {"frozen_filters": {"category": "POLITICS"}},
            )
            self._freeze_candidate(
                store,
                "candidate-exact",
                {"market_ids": ["current-sports"]},
            )
            before = store.connection.total_changes
            with patch(
                "urllib.request.urlopen",
                side_effect=AssertionError("forward authority must not access the network"),
            ):
                first = store.candidate_forward_requirements(
                    candidate_ids=["candidate-exact", "candidate-filter"],
                    now=T0,
                )
                second = store.candidate_forward_requirements(
                    candidate_ids=["candidate-exact", "candidate-filter"],
                    now=T0,
                )
                health = store.polymarket_required_health(
                    requirements=first,
                    scheduled_market_ids=["current-sports", "current-politics"],
                    now=T0,
                    stale_after_seconds=60,
                )
            after = store.connection.total_changes

        self.assertEqual(first, second)
        self.assertEqual(
            first["market_ids"],
            ["current-sports", "current-politics"],
        )
        self.assertEqual(
            first["candidate_references"],
            {
                "current-sports": ["candidate-exact"],
                "current-politics": ["candidate-filter"],
            },
        )
        self.assertEqual(
            first["candidate_bound_markets"],
            {
                "candidate-exact": ["current-sports"],
                "candidate-filter": ["current-politics"],
            },
        )
        self.assertEqual(
            health["candidate_references"],
            {
                "current-sports": ["candidate-exact"],
                "current-politics": ["candidate-filter"],
            },
        )
        self.assertEqual(before, after)
if __name__=="__main__": unittest.main()
