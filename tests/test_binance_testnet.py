from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
import json
import sqlite3
import tempfile
import threading
import unittest

from axiom.binance_spot import BINANCE_SPOT_TESTNET, BinanceRuntimeProfile, BinanceSpotResult
from axiom.binance_testnet import PROBE_LABEL, BinanceTestnetGateService


class FakeVenue:
    environment = BINANCE_SPOT_TESTNET
    origin = "https://testnet.binance.vision"

    def __init__(
        self,
        *,
        auth_code: int | None = None,
        ambiguous: bool = False,
        accepted_then_unknown: bool = False,
        base_fee: str = "0",
        partial: bool = False,
        trades_status: str | None = None,
        credentials: object | None = None,
        bid_price: str = "99.90",
        min_notional: str = "5",
    ):
        self.calls: list[tuple[str, dict]] = []
        self.auth_code = auth_code
        self.ambiguous = ambiguous
        self.accepted_then_unknown = accepted_then_unknown
        self.base_fee = base_fee
        self.partial = partial
        self.trades_status = trades_status
        self.credentials = credentials
        self.bid_price = bid_price
        self.min_notional = min_notional
        self.epoch = "epoch-1"
        self.orders: dict[str, dict] = {}
        self.fills: dict[str, list[dict]] = {}
        self.next_order = 40
        self.next_trade = 90

    def time(self):
        self.calls.append(("time", {}))
        return BinanceSpotResult("OK", {"serverTime": 1_700_000_000_000})

    def account(self, *, timestamp, recvWindow):
        self.calls.append(("account", {"timestamp": timestamp, "recvWindow": recvWindow}))
        if self.auth_code is not None:
            return BinanceSpotResult("REJECTED", {"code": self.auth_code, "msg": "rejected"}, error_code=self.auth_code)
        return BinanceSpotResult(
            "OK",
            {
                "accountType": "SPOT",
                "canTrade": True,
                "permissions": ["SPOT", "TRADE"],
                "epoch": self.epoch,
                "balances": [{"asset": "USDT", "free": "100", "locked": "0"}],
            },
        )

    def _info(self, symbol="ETHUSDT"):
        return {
            "symbol": symbol,
            "status": "TRADING",
            "baseAsset": "ETH",
            "quoteAsset": "USDT",
            "isSpotTradingAllowed": True,
            "orderTypes": ["LIMIT"],
            "timeInForce": ["IOC", "FOK"],
            "filters": [
                {"filterType": "PRICE_FILTER", "minPrice": "0.01", "maxPrice": "100000", "tickSize": "0.01"},
                {"filterType": "LOT_SIZE", "minQty": "0.0001", "maxQty": "100", "stepSize": "0.0001"},
                {"filterType": "MIN_NOTIONAL", "minNotional": self.min_notional, "applyToMarket": True},
            ],
        }

    def exchange_info(self, *, symbol=None):
        kwargs = {"symbol": symbol} if symbol is not None else {}
        self.calls.append(("exchange_info", kwargs))
        return BinanceSpotResult("OK", {"symbols": [self._info(symbol or "ETHUSDT")]})

    def ticker_book(self, *, symbol):
        kwargs = {"symbol": symbol}
        self.calls.append(("ticker_book", kwargs))
        return BinanceSpotResult("OK", {"symbol": symbol, "bidPrice": self.bid_price, "askPrice": "100.00", "weightedAvgPrice": "100.00"})

    def test_order(self, *, symbol, side, quantity, price, time_in_force="IOC"):
        self.calls.append(("test_order", {"symbol": symbol, "side": side, "quantity": quantity, "price": price, "time_in_force": time_in_force}))
        return BinanceSpotResult("OK", {"status": "TEST_ORDER", "valid": True})

    def place_limit_order(self, *, symbol, side, quantity, price, new_client_order_id, time_in_force="IOC"):
        kwargs = {
            "symbol": symbol,
            "side": side,
            "quantity": quantity,
            "price": price,
            "time_in_force": time_in_force,
            "new_client_order_id": new_client_order_id,
        }
        self.calls.append(("place_limit_order", dict(kwargs)))
        self.next_order += 1
        order_id = str(self.next_order)
        requested = Decimal(quantity)
        executed = requested / 2 if self.partial and side == "BUY" else requested
        row = {
            "symbol": symbol, "orderId": order_id,
            "clientOrderId": new_client_order_id,
            "status": "PARTIALLY_FILLED" if executed < requested else "FILLED",
            "executedQty": str(executed), "origQty": quantity,
            "price": price,
        }
        self.orders[order_id] = row
        self.next_trade += 1
        self.fills[order_id] = [{
            "tradeId": str(self.next_trade), "orderId": order_id,
            "symbol": symbol, "qty": str(executed),
            "price": price,
            "quoteQty": str(executed * Decimal(price)),
            "commission": self.base_fee if side == "BUY" else "0.001",
            "commissionAsset": "ETH" if side == "BUY" else "USDT",
            "time": 1_700_000_000_100,
        }]
        if (self.ambiguous or self.accepted_then_unknown) and side == "BUY":
            return BinanceSpotResult("UNKNOWN", {"code": -1007, "msg": "timeout after acceptance"}, error_code=-1007)
        return BinanceSpotResult("OK", row)

    def query_order(self, *, symbol, order_id=None, orig_client_order_id=None):
        kwargs = {"symbol": symbol, "order_id": order_id, "orig_client_order_id": orig_client_order_id}
        self.calls.append(("query_order", kwargs))
        key = str(order_id)
        if self.ambiguous:
            return BinanceSpotResult("UNKNOWN", {"code": -1007, "msg": "timeout"}, error_code=-1007)
        if key not in self.orders:
            client = str(orig_client_order_id or "")
            key = next((order_id for order_id, row in self.orders.items() if row.get("clientOrderId") == client), key)
        if key not in self.orders:
            return BinanceSpotResult("REJECTED", {"code": -2013, "msg": "Order does not exist"}, error_code=-2013)
        return BinanceSpotResult("OK", dict(self.orders[key]))

    def my_trades(self, *, symbol, order_id=None):
        kwargs = {"symbol": symbol, "order_id": order_id}
        self.calls.append(("my_trades", kwargs))
        if self.trades_status is not None:
            return BinanceSpotResult(self.trades_status, {"code": -1007, "msg": "trades unavailable"}, error_code=-1007)
        return BinanceSpotResult("OK", {"trades": list(self.fills.get(str(order_id), ()))})


class BinanceTestnetGateTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.profile = BinanceRuntimeProfile.testnet(root)
        self.connection = sqlite3.connect(":memory:")
        self.connection.row_factory = sqlite3.Row

    def tearDown(self):
        self.connection.close()
        self.temp.cleanup()

    def service(self, venue=None, credentials=None):
        return BinanceTestnetGateService(
            self.connection,
            profile=self.profile,
            venue=venue,
            credentials=credentials,
            clock=lambda: datetime.fromtimestamp(1_700_000_000, timezone.utc),
        )

    def test_absent_credentials_and_venue_make_zero_calls(self):
        venue = FakeVenue()
        service = self.service(venue)
        result = service.check_connectivity()
        self.assertEqual(result["status"], "BLOCKED")
        self.assertEqual(result["reason"], "CREDENTIALS_NOT_CONFIGURED")
        self.assertEqual(service.probe_status()["status"], "BLOCKED")
        self.assertEqual(venue.calls, [])

    def test_authentication_error_mapping_never_discloses_secret(self):
        venue = FakeVenue(auth_code=-1022)
        result = self.service(venue, {"api_key": "public", "api_secret": "private-secret"}).check_connectivity()
        self.assertEqual(result["status"], "BLOCKED")
        self.assertEqual(result["reason"], "SIGNATURE_REJECTED")
        self.assertNotIn("private-secret", str(result))
        self.assertEqual([name for name, _ in venue.calls], ["time", "account"])
    def test_invalid_key_permission_is_distinct_from_signature_error(self):
        venue = FakeVenue(auth_code=-2015)
        result = self.service(venue, {"api_key": "public", "api_secret": "private-secret"}).check_connectivity()
        self.assertEqual(result["status"], "BLOCKED")
        self.assertEqual(result["reason"], "INVALID_API_KEY_OR_PERMISSION")
        self.assertNotEqual(result["reason"], "SIGNATURE_REJECTED")


    def test_validation_uses_test_order_without_order_side_effect(self):
        venue = FakeVenue()
        service = self.service(venue, {"api_key": "public", "api_secret": "private-secret"})
        result = service.validate_order()
        self.assertEqual(result["status"], "PASS")
        self.assertEqual(result["symbol"], "ETHUSDT")
        self.assertEqual(len([name for name, _ in venue.calls if name == "test_order"]), 1)
        self.assertEqual(len([name for name, _ in venue.calls if name == "place_limit_order"]), 0)
    def test_account_read_uses_server_offset_and_bounded_recv_window(self):
        venue = FakeVenue()
        result = self.service(venue, {"api_key": "public", "api_secret": "private-secret"}).check_connectivity()
        self.assertEqual(result["status"], "PASS")
        account_call = next(kwargs for name, kwargs in venue.calls if name == "account")
        self.assertEqual(account_call["timestamp"], 1_700_000_000_000)
        self.assertEqual(account_call["recvWindow"], 5000)

    def test_partial_fill_and_base_fee_reduce_owned_quantity(self):
        venue = FakeVenue(partial=True, base_fee="0.0001")
        service = self.service(venue, {"api_key": "public", "api_secret": "private-secret"})
        service.execute_probe()
        result = service.reconcile_probe()
        self.assertEqual(result["intent"]["state"], "PARTIALLY_FILLED")
        self.assertEqual(result["owned_quantity"], "0.0250")
        self.assertEqual(result["exit"]["state"], "DUST")

    def test_durable_reservation_is_held_for_unknown_and_blocks_repeat_submission(self):
        venue = FakeVenue(
            ambiguous=True,
            credentials={"api_key": "public", "api_secret": "private-secret"},
        )
        service = self.service(venue, {"api_key": "public", "api_secret": "private-secret"})
        service.execute_probe()
        held = self.connection.execute(
            "SELECT status FROM binance_testnet_probe_reservations WHERE status='HELD'"
        ).fetchall()
        self.assertEqual(len(held), 1)
        # A fresh service hydrates the unchanged venue credentials before
        # checking the durable binding and must not submit a second order.
        service_again = self.service(venue)
        service_again.execute_probe()
        self.assertEqual(len([name for name, _ in venue.calls if name == "place_limit_order"]), 1)

    def test_blank_credentials_are_unconfigured_and_make_zero_calls(self):
        for credentials in (
            {"api_key": "   ", "api_secret": "secret"},
            {"api_key": "key", "api_secret": None},
            (None, "secret"),
        ):
            with self.subTest(credentials=credentials):
                venue = FakeVenue()
                service = self.service(venue, credentials)
                result = service.check_connectivity()
                self.assertEqual(result["reason"], "CREDENTIALS_NOT_CONFIGURED")
                self.assertFalse(result["credentials"]["configured"])
                self.assertEqual(venue.calls, [])

    def test_unavailable_my_trades_keeps_filled_order_unknown_and_reserved(self):
        venue = FakeVenue(trades_status="UNKNOWN")
        service = self.service(venue, {"api_key": "public", "api_secret": "private-secret"})
        service.execute_probe()
        result = service.reconcile_probe()
        self.assertEqual(result["intent"]["state"], "UNKNOWN")
        held = self.connection.execute(
            "SELECT status FROM binance_testnet_probe_reservations WHERE status='HELD'"
        ).fetchall()
        self.assertEqual(len(held), 1)
        self.assertEqual(
            len([name for name, _ in venue.calls if name == "place_limit_order"]),
            1,
        )

    def test_atomic_admission_is_idempotent_across_service_instances(self):
        database = Path(self.temp.name) / "shared.sqlite"
        connections = [
            sqlite3.connect(str(database), timeout=5, check_same_thread=False)
            for _ in range(2)
        ]
        for connection in connections:
            connection.row_factory = sqlite3.Row
        venue = FakeVenue()
        services = [
            BinanceTestnetGateService(
                connection,
                profile=self.profile,
                venue=venue,
                credentials={"api_key": "public", "api_secret": "private-secret"},
                clock=lambda: datetime.fromtimestamp(1_700_000_000, timezone.utc),
            )
            for connection in connections
        ]
        barrier = threading.Barrier(2)
        results: list[object] = []

        def submit(service):
            try:
                barrier.wait()
                results.append(service.execute_probe())
            except BaseException as exc:  # surfaced by the assertions below
                results.append(exc)

        threads = [threading.Thread(target=submit, args=(service,)) for service in services]
        try:
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=10)
            self.assertEqual(len(results), 2)
            self.assertFalse(any(isinstance(result, BaseException) for result in results))
            self.assertEqual(
                len([name for name, _ in venue.calls if name == "place_limit_order"]),
                1,
            )
            count = connections[0].execute(
                "SELECT COUNT(*) FROM binance_testnet_probe_intents WHERE side='BUY'"
            ).fetchone()[0]
            self.assertEqual(count, 1)
        finally:
            for connection in connections:
                connection.close()

    def test_full_buy_exit_is_authoritative_and_isolated(self):
        venue = FakeVenue(base_fee="0.0001")
        self.connection.execute("CREATE TABLE binance_execution_signals(id INTEGER)")
        self.connection.execute("INSERT INTO binance_execution_signals VALUES (1)")
        service = self.service(venue, {"api_key": "public", "api_secret": "private-secret"})
        submitted = service.execute_probe()
        self.assertIn(submitted["status"], {"ACKNOWLEDGED", "FILLED"})
        buy_submission = next(
            kwargs
            for name, kwargs in venue.calls
            if name == "place_limit_order" and kwargs["side"] == "BUY"
        )
        self.assertEqual(buy_submission["quantity"], "0.0502")
        self.assertLessEqual(
            Decimal(buy_submission["quantity"]) * Decimal(buy_submission["price"]) * Decimal("1.001"),
            Decimal("10"),
        )
        validation_json = self.connection.execute(
            "SELECT result_json FROM binance_testnet_gate_validation ORDER BY checked_at_utc DESC LIMIT 1"
        ).fetchone()[0]
        validation = json.loads(validation_json)
        self.assertTrue(validation["planned_exit_viable"])
        self.assertEqual(validation["planned_exit"]["quantity"], "0.0501")
        reconciled = service.reconcile_probe()
        self.assertEqual(reconciled["exit"]["state"], "FILLED")
        sell_submission = next(
            kwargs
            for name, kwargs in venue.calls
            if name == "place_limit_order" and kwargs["side"] == "SELL"
        )
        self.assertEqual(sell_submission["quantity"], "0.0501")
        self.assertEqual(len([name for name, _ in venue.calls if name == "place_limit_order"]), 2)
        self.assertEqual(self.connection.execute("SELECT COUNT(*) FROM binance_execution_signals").fetchone()[0], 1)
        names = {row[0] for row in self.connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        self.assertTrue({"binance_testnet_probe_intents", "binance_testnet_probe_fills"} <= names)
        self.assertEqual(service.dashboard_projection()["probe_kind"], PROBE_LABEL)

    def test_planned_exit_impossible_blocks_before_any_order_mutation(self):
        venue = FakeVenue(bid_price="50.00")
        service = self.service(venue, {"api_key": "public", "api_secret": "private-secret"})
        result = service.execute_probe()
        self.assertEqual(result["status"], "BLOCKED")
        self.assertEqual(result["reason"], "VALIDATION_NO_VIABLE_PLANNED_EXIT_WITHIN_ENTRY_CAP")
        self.assertEqual(len([name for name, _ in venue.calls if name == "test_order"]), 0)
        self.assertEqual(len([name for name, _ in venue.calls if name == "place_limit_order"]), 0)
        validation_json = self.connection.execute(
            "SELECT result_json FROM binance_testnet_gate_validation ORDER BY checked_at_utc DESC LIMIT 1"
        ).fetchone()[0]
        validation = json.loads(validation_json)
        self.assertFalse(validation["planned_exit_viable"])
        self.assertEqual(validation["planned_exit"]["status"], "BLOCKED")

    def test_ambiguous_submission_is_unknown_and_idempotent(self):
        venue = FakeVenue(ambiguous=True)
        service = self.service(venue, {"api_key": "public", "api_secret": "private-secret"})
        first = service.execute_probe()
        self.assertEqual(first["status"], "UNKNOWN")
        count = len([name for name, _ in venue.calls if name == "place_limit_order"])
        second = service.execute_probe()
        self.assertEqual(second["status"], "UNKNOWN")
        self.assertEqual(len([name for name, _ in venue.calls if name == "place_limit_order"]), count)
    def test_timeout_after_accepted_mutation_reconciles_by_client_id_once(self):
        venue = FakeVenue(accepted_then_unknown=True)
        service = self.service(venue, {"api_key": "public", "api_secret": "private-secret"})
        first = service.execute_probe()
        self.assertEqual(first["status"], "UNKNOWN")
        buy_id = first["intent"]["client_order_id"]
        result = service.reconcile_probe()
        self.assertEqual(result["intent"]["state"], "FILLED")
        buy_submissions = [kwargs for name, kwargs in venue.calls if name == "place_limit_order" and kwargs["side"] == "BUY"]
        self.assertEqual(len(buy_submissions), 1)
        query = next(kwargs for name, kwargs in venue.calls if name == "query_order")
        self.assertEqual(query["orig_client_order_id"], buy_id)


    def test_reset_missing_history_remains_unknown(self):
        venue = FakeVenue()
        service = self.service(venue, {"api_key": "public", "api_secret": "private-secret"})
        service.execute_probe()
        venue.epoch = "testnet-reset"
        venue.orders.clear()
        result = service.reconcile_probe()
        self.assertEqual(result["status"], "UNKNOWN")
        self.assertEqual(result["intent"]["state"], "UNKNOWN")


if __name__ == "__main__":
    unittest.main()
