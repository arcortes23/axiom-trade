from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
import unittest
from unittest.mock import patch

from axiom.canary import CanaryBlocked, CanaryLimits, CanaryService, CredentialStore, PRODUCTION_LIVE_EXECUTION
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
    def __init__(self, *, blocked=False, close_only=False, minimum="1", ask="0.50", balance="10", accepting=True):
        self.blocked=blocked; self.close_only=close_only; self.minimum=minimum; self.ask=ask; self._balance=balance; self.accepting=accepting; self.submissions=[]
    def geoblock(self): return {"blocked":self.blocked,"close_only":self.close_only,"country":"ZZ","region":"T"}
    def connectivity_check(self): return True
    def market_context(self, market_id, token_id): return {"accepting_orders":self.accepting,"min_order_size":self.minimum,"tick_size":"0.01","bids":[{"price":"0.49","size":"100"}],"asks":[{"price":self.ask,"size":"100"}],"fee_bps":"10"}
    def balance(self): return Decimal(self._balance)
    def submit_limit_order(self, **kwargs): self.submissions.append(kwargs); return {"ok":True,"order_id":"fake-order","status":"matched"}

class CanaryTests(unittest.TestCase):
    def setUp(self):
        self.store=HealthyStore(":memory:"); self.service=CanaryService(self.store,credentials=FakeCredentials(),clock=lambda:T0); self.venue=FakeVenue()
        payload={"schema_validated":True,"historical_backtest_passed":True,"validation_passed":True,"robustness_passed":True,"frozen_hash":"abc","data_quality_passed":True,"paper_only":True}
        self.store.save_candidate_lifecycle("C123","IDEA",payload,timestamp=T0)
        for stage in ("SCHEMA_VALIDATED","BACKTESTED","VALIDATED","ROBUSTNESS_CHECKED","FROZEN"):
            self.store.save_candidate_lifecycle("C123",stage,payload,timestamp=T0)
        self.service.mark_eligible("C123")
    def tearDown(self): self.store.close()
    def arm(self, **kwargs): return self.service.arm("C123",venue=kwargs.pop("venue",self.venue),credentials_configured=True,**kwargs)
    def submit(self, signal="s1", **kwargs): return self.service.submit(signal_id=signal,candidate_id="C123",market_id="m",token_id="yes",side="BUY",paper_expected_price=Decimal("0.50"),venue=kwargs.pop("venue",self.venue),**kwargs)
    def assertBlocked(self, code, fn):
        with self.assertRaisesRegex(CanaryBlocked,code): fn()

    def test_default_startup_cannot_trade(self): self.assertBlocked("CANARY_NOT_ARMED",self.submit)
    def test_missing_credentials_cannot_arm(self):
        service=CanaryService(self.store,credentials=FakeCredentials(False),clock=lambda:T0)
        self.assertBlocked("CREDENTIALS_NOT_CONFIGURED",lambda:service.arm("C123",venue=self.venue))
    def test_hermes_has_no_canary_execution_fields(self):
        from axiom.director import validate_hermes_proposal
        result=validate_hermes_proposal({"proposal_id":"x","statement":"x","source":"x","tests":["x"],"dataset_version":"v","time_split":"train-validation-holdout","paper_only":True,"canary_arm":True})
        self.assertFalse(result.accepted)
    def test_ineligible_candidate_cannot_arm(self): self.assertBlocked("NOT_CANARY_ELIGIBLE",lambda:self.service.arm("other",venue=self.venue,credentials_configured=True))
    def test_expired_arm_cannot_trade(self):
        self.arm(expires_hours=Decimal("0.001")); self.service.clock=lambda:T0+timedelta(hours=1)
        self.assertBlocked("CANARY_NOT_ARMED",self.submit)
    def test_kill_switch_prevents_trading(self): self.arm(); self.service.kill(); self.assertBlocked("CANARY_NOT_ARMED",self.submit)
    def test_degraded_collector_prevents_arming(self):
        self.store.polymarket_health=lambda **kwargs:{"grade":"D"}
        self.assertBlocked("COLLECTOR_DEGRADED",self.arm)
    def test_geoblock_prevents_arming_and_submission(self):
        blocked=FakeVenue(blocked=True); self.assertBlocked("GEOGRAPHICALLY_BLOCKED",lambda:self.arm(venue=blocked))
        self.arm(); self.assertBlocked("GEOGRAPHICALLY_BLOCKED",lambda:self.submit(venue=blocked))
    def test_target_is_never_silently_increased(self): self.arm(); result=self.submit(); self.assertLessEqual(Decimal(result["requested_notional"]),Decimal("1.00"))
    def test_market_minimum_exceeding_target_skips(self): self.arm(); venue=FakeVenue(minimum="5",ask="0.50"); self.assertBlocked("VENUE_MINIMUM_EXCEEDS",lambda:self.submit(venue=venue)); self.assertFalse(venue.submissions)
    def test_slippage_prevents_submission(self): self.arm(); venue=FakeVenue(ask="0.60"); self.assertBlocked("SLIPPAGE_LIMIT",lambda:self.submit(venue=venue)); self.assertFalse(venue.submissions)
    def test_daily_loss_limit_prevents_submission(self):
        self.arm(); self.store.connection.execute("INSERT INTO canary_ledger(event_id,signal_id,timestamp,candidate_id,venue,market_id,token_id,side,requested_notional,paper_expected_price,max_price,status,realized_pnl,evidence_json) VALUES('e','old',?,?,?,?,?,?,?,?,?,'RESOLVED','-2.00','{}')",(T0.isoformat(),"C123","polymarket","m0","t","BUY","1",".5",".5")); self.assertBlocked("DAILY_LOSS_LIMIT",self.submit)
    def test_exposure_limit_prevents_submission(self):
        self.arm(limits=CanaryLimits(max_exposure_usd=Decimal("1"))); self.store.connection.execute("INSERT INTO canary_ledger(event_id,signal_id,timestamp,candidate_id,venue,market_id,token_id,side,requested_notional,paper_expected_price,max_price,status,evidence_json) VALUES('e','old',?,?,?,?,?,?,?,?,?,'OPEN','{}')",(T0.isoformat(),"C123","polymarket","m0","t","BUY","1",".5",".5")); self.assertBlocked("EXPOSURE_LIMIT",self.submit)
    def test_max_order_count_prevents_submission(self):
        self.arm(limits=CanaryLimits(max_orders_per_day=1)); self.submit("first"); self.assertBlocked("DAILY_ORDER_LIMIT",lambda:self.submit("second"))
    def test_duplicate_signal_and_restart_cannot_duplicate(self):
        self.arm(); self.submit("same"); self.assertBlocked("DUPLICATE_SIGNAL",lambda:CanaryService(self.store,credentials=FakeCredentials(),clock=lambda:T0).submit(signal_id="same",candidate_id="C123",market_id="m",token_id="yes",side="BUY",paper_expected_price=Decimal(".5"),venue=self.venue)); self.assertEqual(len(self.venue.submissions),1)
    def test_credentials_never_persist_or_render(self):
        self.arm(); self.submit(); dump="\n".join(str(tuple(r)) for r in self.store.connection.iterdump()); dashboard=json.dumps(DashboardData(store=self.store).operator_data(),default=str)+_dashboard_html(); self.assertNotIn("test-only",dump+dashboard)
    def test_paper_and_real_ledgers_are_separate(self):
        self.arm(); self.submit(); self.assertEqual(self.store.connection.execute("SELECT COUNT(*) FROM canary_ledger").fetchone()[0],1); self.assertEqual(self.store.connection.execute("SELECT COUNT(*) FROM paper_execution_events").fetchone()[0],0)
    def test_canary_check_without_credentials_rejects_without_order(self):
        service=CanaryService(self.store,credentials=FakeCredentials(False),clock=lambda:T0); result=service.check(candidate_id="C123",venue=None); self.assertFalse(result["ready"]); self.assertIn("CREDENTIALS_NOT_CONFIGURED",result["failures"]); self.assertFalse(self.venue.submissions)
    def test_dashboard_labels_real_canary_and_production_disabled(self):
        data=DashboardData(store=self.store).operator_data(); self.assertFalse(data["live_execution"]); self.assertFalse(PRODUCTION_LIVE_EXECUTION); self.assertIn("REAL CANARY MONEY",_dashboard_html()); self.assertEqual(data["canary"]["production_live_trading"],"DISABLED")

if __name__=="__main__": unittest.main()
