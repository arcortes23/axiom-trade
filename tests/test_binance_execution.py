from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
import tempfile
import threading
import unittest

from axiom.binance_execution import (
    ACKNOWLEDGED,
    ENABLE_CONFIRMATION,
    FILLED,
    INTENT,
    PAUSED,
    PARTIALLY_FILLED,
    REJECTED,
    RESERVED,
    UNKNOWN,
    BinanceExecutionService,
)
from axiom.binance_risk import BinanceRiskEnvelope
from axiom.storage import AxiomStore


UTC = timezone.utc
T0 = datetime(2026, 1, 2, 12, tzinfo=UTC)


def signal(signal_id="s1", *, intent="ENTRY", side=None, candidate="c1", binding=None, symbol="BTCUSDT"):
    return {
        "signal_id": signal_id,
        "opportunity_id": "opp-" + signal_id,
        "candidate_id": candidate,
        "binding_hash": binding,
        "symbol": symbol,
        "environment": "PAPER",
        "decision_interval": "1h",
        "decision_at": T0.isoformat(),
        "intent": intent,
        "side": side or ("BUY" if intent == "ENTRY" else "SELL"),
        "reason": "fixture",
        "exit_policy": {"stop": "0.05"},
    }


class FakeVenue:
    def __init__(self):
        self.submissions = []
        self.cancellations = []
        self.orders = {}
        self.trades = {}
        self.fail_query = False
        self.fail_trades = False

    def place_limit_order(self, **kwargs):
        self.submissions.append(dict(kwargs))
        client_id = kwargs.get("new_client_order_id") or kwargs.get("newClientOrderId") or kwargs.get("client_order_id")
        order_id = str(len(self.submissions))
        self.orders[client_id] = {"status": "NEW", "orderId": order_id, "clientOrderId": client_id}
        return self.orders[client_id]

    def query_order(self, **kwargs):
        if self.fail_query:
            raise ConnectionError("query disconnected")
        return self.orders.get(kwargs.get("orig_client_order_id") or kwargs.get("client_order_id"), {"status": "UNKNOWN"})

    def my_trades(self, **kwargs):
        if self.fail_trades:
            raise ConnectionError("myTrades disconnected")
        return {"status": "OK", "trades": self.trades.get(str(kwargs.get("order_id")), [])}

    def cancel_owned_order(self, **kwargs):
        self.cancellations.append(dict(kwargs))
        return {"status": "CANCELED", "orderId": kwargs.get("order_id")}


class AcceptedDisconnectVenue(FakeVenue):
    def place_limit_order(self, **kwargs):
        super().place_limit_order(**kwargs)
        raise ConnectionError("response lost after acceptance")


class BlockingVenue(FakeVenue):
    def __init__(self):
        super().__init__()
        self.started = threading.Event()
        self.release = threading.Event()

    def place_limit_order(self, **kwargs):
        self.started.set()
        if not self.release.wait(timeout=2):
            raise TimeoutError("test venue was not released")
        return super().place_limit_order(**kwargs)


class TimeoutVenue(FakeVenue):
    def place_limit_order(self, **kwargs):
        self.submissions.append(dict(kwargs))
        raise TimeoutError("blocked")


class CrashBoundary(BaseException):
    pass

class BinanceExecutionContractTests(unittest.TestCase):
    def setUp(self):
        self.store = AxiomStore(":memory:")
        self.venue = FakeVenue()
        self.service = BinanceExecutionService(self.store, venue=self.venue, environment="PAPER")
        self.service.update_account({"quote_available": "100", "owned_inventory": {}})

    def tearDown(self):
        self.store.close()

    def test_default_control_is_disabled_and_exact_confirmation_arms_only_binance(self):
        self.assertEqual(self.service.control()["state"], "DISABLED")
        with self.assertRaises(PermissionError):
            self.service.enable_auto_canary("ENABLE BINANCE CANARY")
        self.service.enable_auto_canary(ENABLE_CONFIRMATION)
        self.assertEqual(self.service.control()["state"], "ARMED")
        self.assertTrue(self.service.control()["authorized"])

    def test_entry_reserves_before_send_and_repeated_signal_does_not_resubmit(self):
        self.service.enable_auto_canary(ENABLE_CONFIRMATION)
        result = self.service.submit_signal(signal(), price="10", quantity="1")
        self.assertEqual(result["state"], ACKNOWLEDGED)
        self.assertEqual(len(self.venue.submissions), 1)
        states = [row["to_state"] for row in self.store.connection.execute("SELECT to_state FROM binance_execution_order_transitions ORDER BY transition_id")]
        self.assertEqual(states[:3], [INTENT, RESERVED, "SUBMITTING"])
        again = self.service.submit_signal(signal(), price="10", quantity="1")
        self.assertEqual(again["client_order_id"], result["client_order_id"])
        self.assertEqual(len(self.venue.submissions), 1)

    def test_acknowledgement_without_fill_is_distinct_from_partial_fill(self):
        self.service.enable_auto_canary(ENABLE_CONFIRMATION)
        entry = self.service.submit_signal(signal(), price="10", quantity="1")
        self.assertEqual(entry["state"], ACKNOWLEDGED)
        self.assertEqual(self.service.fills(), [])

        self.venue.trades["1"] = [{
            "tradeId": "partial-1",
            "qty": "0.25",
            "price": "10",
            "quoteQty": "2.5",
            "commission": "0",
            "time": T0.isoformat(),
        }]
        self.assertEqual(self.service.reconcile()["status"], "SUCCESS")
        self.assertEqual(self.service.orders()[0]["state"], PARTIALLY_FILLED)
        self.assertEqual(len(self.service.fills()), 1)

    def test_partial_buy_then_exit_derives_owned_position_and_realized_net_pnl(self):
        self.service.enable_auto_canary(ENABLE_CONFIRMATION)
        entry = self.service.submit_signal(signal(), price="10", quantity="1")
        self.service.record_fills(entry["intent_id"], [{"tradeId": "t1", "qty": "1", "price": "10", "quoteQty": "10", "commission": "0.01", "commissionAsset": "BTC", "time": T0.isoformat()}])
        position = self.service.position("BTCUSDT")
        self.assertEqual(position["quantity"], "0.99")
        exit_result = self.service.submit_signal(signal("s2", intent="EXIT"), price="12", quantity="0.5")
        self.service.record_fills(exit_result["intent_id"], [{"tradeId": "t2", "qty": "0.5", "price": "12", "quoteQty": "6", "commission": "0.006", "commissionAsset": "USDT", "time": T0.isoformat()}])
        position = self.service.position("BTCUSDT")
        self.assertEqual(position["quantity"], "0.49")
        self.assertEqual(Decimal(position["realized_pnl"]), Decimal("0.9434949494949494949494949495"))
        self.assertEqual(position["candidate_id"], "c1")
        self.assertEqual(position["exit_policy"], {"stop": "0.05"})

    def test_timeout_is_unknown_and_reconcile_uses_client_id_without_resubmit(self):
        service = BinanceExecutionService(self.store, venue=TimeoutVenue(), environment="PAPER")
        service.update_account({"quote_available": "100", "owned_inventory": {}})
        service.enable_auto_canary(ENABLE_CONFIRMATION)
        result = service.submit_signal(signal(), price="10", quantity="1")
        self.assertEqual(result["state"], UNKNOWN)
        self.assertEqual(len(service.venue.submissions), 1)
        self.assertEqual(service.orders(state=UNKNOWN)[0]["client_order_id"], result["client_order_id"])

    def test_duplicate_and_out_of_order_fills_are_idempotent_and_secrets_are_redacted(self):
        self.service.update_account({"quote_available": "100", "api_secret": "DO_NOT_PERSIST"})
        self.service.enable_auto_canary(ENABLE_CONFIRMATION)
        entry = self.service.submit_signal(signal(), price="10", quantity="1")
        fill = {"tradeId": "same", "qty": "1", "price": "10", "quoteQty": "10", "commission": "0", "time": T0.isoformat()}
        self.venue.trades["1"] = [dict(fill), dict(fill)]
        self.assertEqual(self.service.reconcile()["status"], "SUCCESS")
        self.assertEqual(len(self.service.fills()), 1)
        self.assertEqual(self.service.record_fills(entry["intent_id"], [fill]), 0)
        raw = self.store.connection.execute("SELECT account_json FROM binance_execution_account").fetchone()[0]
        self.assertNotIn("DO_NOT_PERSIST", raw)


    def test_restart_from_reserved_queries_client_id_without_blind_submit(self):
        with tempfile.TemporaryDirectory() as directory:
            path = directory + "/execution.sqlite"
            venue = FakeVenue()

            def crash(point):
                if point == "before_network_submit":
                    raise CrashBoundary()

            first = BinanceExecutionService(path, venue=venue, environment="PAPER", fault_hook=crash)
            try:
                first.update_account({"quote_available": "100", "owned_inventory": {}})
                first.enable_auto_canary(ENABLE_CONFIRMATION)
                with self.assertRaises(CrashBoundary):
                    first.submit_signal(signal(), price="10", quantity="1")
            finally:
                first.close()

            second = BinanceExecutionService(path, venue=venue, environment="PAPER")
            try:
                result = second.reconcile()
                self.assertEqual(result["status"], "SUCCESS")
                self.assertEqual(len(venue.submissions), 0)
                self.assertEqual(second.orders()[0]["state"], UNKNOWN)
            finally:
                second.close()

    def test_restart_from_submitting_queries_client_id_without_blind_submit(self):
        with tempfile.TemporaryDirectory() as directory:
            path = directory + "/execution.sqlite"
            venue = FakeVenue()

            def crash(point):
                if point == "before_venue_call":
                    raise CrashBoundary()

            first = BinanceExecutionService(path, venue=venue, environment="PAPER", fault_hook=crash)
            try:
                first.update_account({"quote_available": "100", "owned_inventory": {}})
                first.enable_auto_canary(ENABLE_CONFIRMATION)
                with self.assertRaises(CrashBoundary):
                    first.submit_signal(signal(), price="10", quantity="1")
                self.assertEqual(first.orders()[0]["state"], "SUBMITTING")
            finally:
                first.close()

            second = BinanceExecutionService(path, venue=venue, environment="PAPER")
            try:
                self.assertEqual(second.reconcile()["status"], "SUCCESS")
                self.assertEqual(len(venue.submissions), 0)
                self.assertEqual(second.orders()[0]["state"], UNKNOWN)
            finally:
                second.close()

    def test_accepted_response_lost_before_commit_reconciles_once_after_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            path = directory + "/execution.sqlite"
            venue = FakeVenue()

            def crash(point):
                if point == "before_order_result_commit":
                    raise CrashBoundary()

            first = BinanceExecutionService(path, venue=venue, environment="PAPER", fault_hook=crash)
            try:
                first.update_account({"quote_available": "100", "owned_inventory": {}})
                first.enable_auto_canary(ENABLE_CONFIRMATION)
                with self.assertRaises(CrashBoundary):
                    first.submit_signal(signal(), price="10", quantity="1")
            finally:
                first.close()

            second = BinanceExecutionService(path, venue=venue, environment="PAPER")
            try:
                self.assertEqual(second.reconcile()["status"], "SUCCESS")
                self.assertEqual(len(venue.submissions), 1)
                self.assertEqual(second.orders()[0]["state"], ACKNOWLEDGED)
            finally:
                second.close()


    def test_disconnect_after_acceptance_is_unknown_then_client_id_reconciles(self):
        venue = AcceptedDisconnectVenue()
        service = BinanceExecutionService(self.store, venue=venue, environment="PAPER")
        service.update_account({"quote_available": "100", "owned_inventory": {}})
        service.enable_auto_canary(ENABLE_CONFIRMATION)
        result = service.submit_signal(signal(), price="10", quantity="1")
        self.assertEqual(result["state"], UNKNOWN)
        self.assertEqual(service.reconcile()["status"], "SUCCESS")
        self.assertEqual(service.orders()[0]["state"], ACKNOWLEDGED)
        self.assertEqual(len(venue.submissions), 1)

    def test_two_connections_racing_same_signal_create_one_reservation_and_submission(self):
        with tempfile.TemporaryDirectory() as directory:
            path = directory + "/execution.sqlite"
            venue = FakeVenue()
            envelope = BinanceRiskEnvelope(max_reserved_exposure=Decimal("10"), max_aggregate_exposure=Decimal("10"))
            first = BinanceExecutionService(path, venue=venue, environment="PAPER", risk_envelope=envelope)
            second = None
            try:
                second = BinanceExecutionService(path, venue=venue, environment="PAPER", risk_envelope=envelope)
                first.update_account({"quote_available": "100", "owned_inventory": {}})
                second.update_account({"quote_available": "100", "owned_inventory": {}})
                first.enable_auto_canary(ENABLE_CONFIRMATION)
                second.enable_auto_canary(ENABLE_CONFIRMATION)
                barrier = threading.Barrier(2)
                results = []
                errors = []

                def submit(service):
                    try:
                        barrier.wait()
                        results.append(service.submit_signal(signal(), price="10", quantity="1"))
                    except BaseException as exc:
                        errors.append(exc)

                threads = [threading.Thread(target=submit, args=(first,)), threading.Thread(target=submit, args=(second,))]
                for thread in threads:
                    thread.start()
                for thread in threads:
                    thread.join(timeout=2)
                self.assertFalse(errors)
                self.assertEqual(len(results), 2)
                self.assertEqual(len(venue.submissions), 1)
                self.assertEqual(len(first.orders()), 1)
                self.assertEqual(first.orders()[0]["risk_reservation"]["status"], "HELD")
                rejected = first.submit_signal(signal("s2"), price="10", quantity="1")
                self.assertEqual(rejected["state"], REJECTED)
                self.assertIn("AGGREGATE_EXPOSURE", rejected["reason"])
            finally:
                if second is not None:
                    second.close()
                first.close()

    def test_kill_during_blocked_venue_call_cannot_claim_retraction(self):
        venue = BlockingVenue()
        service = BinanceExecutionService(self.store, venue=venue, environment="PAPER")
        service.update_account({"quote_available": "100", "owned_inventory": {}})
        service.enable_auto_canary(ENABLE_CONFIRMATION)
        result = []
        errors = []

        def submit():
            try:
                result.append(service.submit_signal(signal(), price="10", quantity="1"))
            except BaseException as exc:
                errors.append(exc)

        thread = threading.Thread(target=submit)
        thread.start()
        self.assertTrue(venue.started.wait(timeout=2))
        service.kill()
        venue.release.set()
        thread.join(timeout=2)
        self.assertFalse(thread.is_alive())
        self.assertFalse(errors)
        self.assertEqual(result[0]["state"], UNKNOWN)
        self.assertEqual(len(venue.submissions), 1)

    def test_query_or_my_trades_failure_pauses_entries_but_reconcile_remains_callable(self):
        self.service.enable_auto_canary(ENABLE_CONFIRMATION)
        self.service.submit_signal(signal(), price="10", quantity="1")
        self.venue.fail_trades = True
        failed = self.service.reconcile()
        self.assertEqual(failed["status"], "FAILURE")
        self.assertEqual(self.service.control()["state"], PAUSED)
        blocked = self.service.submit_signal(signal("s2"), price="10", quantity="1")
        self.assertEqual(blocked["state"], REJECTED)
        self.assertEqual(self.service.reconcile()["status"], "FAILURE")

    def test_epoch_change_pauses_testnet_and_marks_reset(self):
        self.service.enable_auto_canary(ENABLE_CONFIRMATION)
        self.service.update_account({"quote_available": "100", "owned_inventory": {}}, epoch="epoch-1")
        self.assertEqual(self.service.reconcile()["status"], "SUCCESS")
        reset = self.service.reconcile(account={"quote_available": "100", "owned_inventory": {}, "epoch": "epoch-2"})
        self.assertEqual(reset["status"], "RESET")
        self.assertEqual(self.service.control()["state"], PAUSED)
        self.assertEqual(self.service.control()["pause_reason"], "TESTNET_RESET")

    def test_unknown_reservation_survives_utc_day_rollover(self):
        envelope = BinanceRiskEnvelope(max_reserved_exposure=Decimal("10"), max_aggregate_exposure=Decimal("100"))
        service = BinanceExecutionService(self.store, venue=TimeoutVenue(), environment="PAPER", risk_envelope=envelope)
        service.update_account({"quote_available": "100", "owned_inventory": {}})
        service.enable_auto_canary(ENABLE_CONFIRMATION)
        first = service.submit_signal(signal(), price="10", quantity="1", now=T0)
        self.assertEqual(first["state"], UNKNOWN)
        next_day = service.submit_signal(signal("s2"), price="10", quantity="1", now=T0 + timedelta(days=1))
        self.assertEqual(next_day["state"], REJECTED)
        self.assertIn("RESERVED_EXPOSURE", next_day["reason"])

    def test_quote_fee_buy_increases_cost_basis(self):
        self.service.enable_auto_canary(ENABLE_CONFIRMATION)
        entry = self.service.submit_signal(signal(), price="10", quantity="1")
        self.service.record_fills(entry["intent_id"], [{"tradeId": "quote-fee", "qty": "1", "price": "10", "quoteQty": "10", "commission": "0.2", "commissionAsset": "USDT", "time": T0.isoformat()}])
        position = self.service.position("BTCUSDT")
        self.assertEqual(position["quantity"], "1")
        self.assertEqual(position["cost_basis"], "10.2")

    def test_known_third_asset_fee_changes_net_pnl(self):
        self.service.enable_auto_canary(ENABLE_CONFIRMATION)
        entry = self.service.submit_signal(signal(symbol="ETHUSDT"), price="10", quantity="1")
        self.service.record_fills(entry["intent_id"], [{"tradeId": "bnb-buy", "qty": "1", "price": "10", "quoteQty": "10", "commission": "1", "commissionAsset": "BNB", "fee_marks": {"BNB": "2"}, "time": T0.isoformat()}])
        exit_result = self.service.submit_signal(signal("s2", intent="EXIT", symbol="ETHUSDT"), price="15", quantity="1")
        self.service.record_fills(exit_result["intent_id"], [{"tradeId": "bnb-sell", "qty": "1", "price": "15", "quoteQty": "15", "commission": "1", "commissionAsset": "BNB", "fee_marks": {"BNB": "2"}, "time": T0.isoformat()}])
        position = self.service.position("ETHUSDT")
        self.assertEqual(position["realized_pnl"], "1")
        self.assertEqual(position["fees_quote"], "4")

    def test_unknown_fee_makes_pnl_unknown_and_pauses_new_entries(self):
        self.service.enable_auto_canary(ENABLE_CONFIRMATION)
        entry = self.service.submit_signal(signal(), price="10", quantity="1")
        self.service.record_fills(entry["intent_id"], [{"tradeId": "unknown-fee", "qty": "1", "price": "10", "quoteQty": "10", "commission": "1", "commissionAsset": "BNB", "time": T0.isoformat()}])
        position = self.service.position("BTCUSDT")
        self.assertEqual(position["valuation_status"], "UNKNOWN")
        self.assertIsNone(position["unrealized_pnl"])
        self.assertEqual(self.service.control()["state"], PAUSED)
        self.assertEqual(self.service.submit_signal(signal("s2"), price="10", quantity="1")["state"], REJECTED)

    def test_cancel_requires_exact_owned_client_id_and_symbol(self):
        self.service.enable_auto_canary(ENABLE_CONFIRMATION)
        entry = self.service.submit_signal(signal(), price="10", quantity="1")
        with self.assertRaises(ValueError):
            self.service.cancel(entry["client_order_id"], symbol="ETHUSDT")
        with self.assertRaises(ValueError):
            self.service.cancel("AXIOM-not-owned", symbol="BTCUSDT")
        self.assertEqual(self.venue.cancellations, [])
        self.assertEqual(self.service.cancel(entry["client_order_id"], symbol="BTCUSDT")["state"], "CANCELED")
        self.assertEqual(self.venue.cancellations[-1]["orig_client_order_id"], entry["client_order_id"])

    def test_signal_environment_mismatch_is_rejected_before_persistence(self):
        service = BinanceExecutionService(":memory:", venue=FakeVenue(), environment="BINANCE_SPOT_TESTNET")
        service.update_account({"quote_available": "100", "owned_inventory": {}})
        service.enable_auto_canary(ENABLE_CONFIRMATION)
        with self.assertRaises(ValueError):
            service.submit_signal(signal(), price="10", quantity="1")
        service.close()
if __name__ == "__main__":
    unittest.main()
