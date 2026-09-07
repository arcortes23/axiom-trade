from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
import unittest

from axiom.binance_execution import (
    ACKNOWLEDGED,
    ENABLE_CONFIRMATION,
    FILLED,
    INTENT,
    PARTIALLY_FILLED,
    RESERVED,
    UNKNOWN,
    BinanceExecutionService,
)
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
        self.orders = {}
        self.trades = {}

    def place_limit_order(self, **kwargs):
        self.submissions.append(dict(kwargs))
        client_id = kwargs.get("new_client_order_id") or kwargs.get("newClientOrderId") or kwargs.get("client_order_id")
        order_id = str(len(self.submissions))
        self.orders[client_id] = {"status": "NEW", "orderId": order_id, "clientOrderId": client_id}
        return self.orders[client_id]

    def query_order(self, **kwargs):
        return self.orders.get(kwargs.get("orig_client_order_id") or kwargs.get("client_order_id"), {"status": "UNKNOWN"})

    def my_trades(self, **kwargs):
        return {"status": "OK", "trades": self.trades.get(str(kwargs.get("order_id")), [])}

    def cancel_owned_order(self, **kwargs):
        return {"status": "CANCELED", "orderId": kwargs.get("order_id")}


class TimeoutVenue(FakeVenue):
    def place_limit_order(self, **kwargs):
        self.submissions.append(dict(kwargs))
        raise TimeoutError("blocked")


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


if __name__ == "__main__":
    unittest.main()
