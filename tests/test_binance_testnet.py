from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
import json
import sqlite3
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from axiom.binance_spot import BINANCE_SPOT_TESTNET, BinanceCredentialRef, BinanceRuntimeProfile, BinanceSpotRESTClient, BinanceSpotResult
from axiom.binance_testnet import AUTO_DEADLINE_EXPIRED, PROBE_LABEL, SOURCE, BinanceTestnetGateService, _invoke


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
                "updateTime": 1_700_000_000_000,
                "uid": 123456,
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
            "symbol": symbol, "side": side, "orderId": order_id,
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
class BlockingSubmissionVenue(FakeVenue):
    def __init__(self):
        super().__init__()
        self.submission_entered = threading.Event()
        self.release_submission = threading.Event()
        self.query_started = threading.Event()

    def place_limit_order(
        self,
        *,
        symbol,
        side,
        quantity,
        price,
        new_client_order_id,
        time_in_force="IOC",
    ):
        self.submission_entered.set()
        if not self.release_submission.wait(5):
            raise RuntimeError("submission release not signaled")
        return super().place_limit_order(
            symbol=symbol,
            side=side,
            quantity=quantity,
            price=price,
            new_client_order_id=new_client_order_id,
            time_in_force=time_in_force,
        )

    def query_order(self, **kwargs):
        self.query_started.set()
        return super().query_order(**kwargs)


class MutableCredentialStore:
    def __init__(self, value):
        self.ref = BinanceCredentialRef("binance-testnet", BINANCE_SPOT_TESTNET)
        self.value = value

    def load(self):
        return self.value

    def safe_projection(self):
        return {"configured": self.value is not None}




class BookTickerReferenceVenue(FakeVenue):
    def _info(self, symbol="ETHUSDT"):
        info = super()._info(symbol)
        info["filters"].append(
            {
                "filterType": "PERCENT_PRICE",
                "multiplierUp": "1.05",
                "multiplierDown": "0.95",
                "avgPriceMins": 5,
            }
        )
        return info

    def ticker_book(self, *, symbol):
        self.calls.append(("ticker_book", {"symbol": symbol}))
        return BinanceSpotResult(
            "OK",
            {"symbol": symbol, "bidPrice": self.bid_price, "askPrice": "100.00"},
        )

    def ticker_24hr(self, *, symbol):
        self.calls.append(("ticker_24hr", {"symbol": symbol}))
        return BinanceSpotResult(
            "OK",
            {
                "symbol": symbol,
                "weightedAvgPrice": "100.00",
                "lastPrice": "100.10",
            },
        )


class TerminalEvidenceVenue(FakeVenue):
    def __init__(
        self,
        *,
        buy_status: str = "FILLED",
        sell_status: str = "FILLED",
        buy_partial: bool = False,
        sell_partial: bool = False,
        trades_available: bool = True,
    ):
        super().__init__(partial=buy_partial)
        self.buy_status = buy_status
        self.sell_status = sell_status
        self.sell_partial = sell_partial
        self.trades_available = trades_available
        self._delayed_fills: dict[str, list[dict]] = {}

    def place_limit_order(
        self,
        *,
        symbol,
        side,
        quantity,
        price,
        new_client_order_id,
        time_in_force="IOC",
    ):
        response = super().place_limit_order(
            symbol=symbol,
            side=side,
            quantity=quantity,
            price=price,
            new_client_order_id=new_client_order_id,
            time_in_force=time_in_force,
        )
        order_id = str(self.next_order)
        row = self.orders[order_id]
        side = str(side).upper()
        if side == "SELL" and self.sell_partial:
            fill = self.fills[order_id][0]
            quantity = Decimal(fill["qty"]) / 2
            fill["qty"] = str(quantity)
            fill["quoteQty"] = str(quantity * Decimal(fill["price"]))
            row["executedQty"] = str(quantity)
        row["status"] = self.buy_status if side == "BUY" else self.sell_status
        self._delayed_fills[order_id] = list(self.fills[order_id])
        if not self.trades_available:
            self.fills[order_id] = []
        return BinanceSpotResult("OK", dict(row))

    def my_trades(self, *, symbol, order_id=None):
        key = str(order_id)
        if key in self._delayed_fills and self.trades_available:
            self.fills[key] = self._delayed_fills.pop(key)
        return super().my_trades(symbol=symbol, order_id=order_id)

    def publish_trades(self):
        self.trades_available = True

class MissingAuthoritativeOrderIdVenue(FakeVenue):
    def __init__(self, *, omit_order_id_sides=()):
        super().__init__()
        self.omit_order_id_sides = {str(side).upper() for side in omit_order_id_sides}
        self.order_sides: dict[str, str] = {}

    def place_limit_order(
        self,
        *,
        symbol,
        side,
        quantity,
        price,
        new_client_order_id,
        time_in_force="IOC",
    ):
        response = super().place_limit_order(
            symbol=symbol,
            side=side,
            quantity=quantity,
            price=price,
            new_client_order_id=new_client_order_id,
            time_in_force=time_in_force,
        )
        self.order_sides[str(self.next_order)] = str(side).upper()
        return response

    def query_order(self, *, symbol, order_id=None, orig_client_order_id=None):
        response = super().query_order(
            symbol=symbol,
            order_id=order_id,
            orig_client_order_id=orig_client_order_id,
        )
        key = str(order_id)
        if key not in self.orders:
            client = str(orig_client_order_id or "")
            key = next((order_id for order_id, row in self.orders.items() if row.get("clientOrderId") == client), key)
        if self.order_sides.get(key) not in self.omit_order_id_sides:
            return response
        payload = dict(response.payload) if isinstance(response.payload, dict) else response.payload
        if isinstance(payload, dict):
            payload.pop("orderId", None)
        return BinanceSpotResult(response.status, payload, error_code=response.error_code)

    def my_trades(self, *, symbol, order_id=None):
        if self.order_sides.get(str(order_id)) in self.omit_order_id_sides:
            order = self.orders[str(order_id)]
            return BinanceSpotResult(
                "OK",
                {
                    "trades": [
                        {
                            "tradeId": f"unrelated-{order_id}",
                            "orderId": f"unrelated-{order_id}",
                            "symbol": symbol,
                            "qty": "8",
                            "price": order["price"],
                            "quoteQty": str(Decimal("8") * Decimal(order["price"])),
                            "commission": "7",
                            "commissionAsset": "USDT",
                            "time": 1_700_000_000_100,
                        }
                    ]
                },
            )
        return super().my_trades(symbol=symbol, order_id=order_id)
class MalformedTradeSymbolVenue(FakeVenue):
    def my_trades(self, *, symbol, order_id=None):
        response = super().my_trades(symbol=symbol, order_id=order_id)
        payload = dict(response.payload)
        payload["trades"] = [
            dict(trade, symbol="ETH/USDT")
            for trade in payload.get("trades", ())
            if isinstance(trade, dict)
        ]
        return BinanceSpotResult(response.status, payload, error_code=response.error_code)


class MalformedBuyCumulativeVenue(FakeVenue):
    def query_order(self, *, symbol, order_id=None, orig_client_order_id=None):
        response = super().query_order(
            symbol=symbol,
            order_id=order_id,
            orig_client_order_id=orig_client_order_id,
        )
        payload = dict(response.payload) if isinstance(response.payload, dict) else response.payload
        if isinstance(payload, dict) and payload.get("side") == "BUY":
            payload["executedQty"] = "not-a-number"
        return BinanceSpotResult(response.status, payload, error_code=response.error_code)




class BinanceTestnetGateTests(unittest.TestCase):

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.profile = BinanceRuntimeProfile.testnet(root)
        self.connection = sqlite3.connect(":memory:", check_same_thread=False)
        self.connection.row_factory = sqlite3.Row

    def tearDown(self):
        self.connection.close()
        self.temp.cleanup()

    def service(self, venue=None, credentials=None, credential_store=None):
        return BinanceTestnetGateService(
            self.connection,
            profile=self.profile,
            venue=venue,
            credentials=credentials,
            credential_store=credential_store,
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
    def test_book_ticker_uses_official_24hr_reference_for_percent_filter(self):
        venue = BookTickerReferenceVenue()
        service = self.service(venue, {"api_key": "public", "api_secret": "private-secret"})
        result = service.validate_order()
        self.assertEqual(result["status"], "PASS")
        self.assertEqual(result["market_reference"], "100.00")
        names = [name for name, _ in venue.calls]
        self.assertLess(names.index("ticker_book"), names.index("ticker_24hr"))
        self.assertLess(names.index("ticker_24hr"), names.index("test_order"))

    def test_unknown_third_asset_fee_blocks_and_persists_new_entry(self):
        venue = FakeVenue()
        service = self.service(venue, {"api_key": "public", "api_secret": "private-secret"})
        service.check_connectivity()
        self.connection.execute(
            "INSERT INTO binance_testnet_probe_fills("
            "trade_id,intent_id,environment,source,probe_kind,symbol,side,"
            "quantity,price,quote_quantity,commission,commission_asset,"
            "trade_time_utc,payload_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "fee-unknown",
                "legacy-intent",
                BINANCE_SPOT_TESTNET,
                SOURCE,
                PROBE_LABEL,
                "ETHUSDT",
                "BUY",
                "1",
                "10",
                "10",
                "0.01",
                "BNB",
                "2023-11-14T22:13:20+00:00",
                "{}",
            ),
        )
        self.connection.commit()
        result = service.execute_probe()
        self.assertEqual(result["status"], "BLOCKED")
        self.assertEqual(result["reason"], "FEE_VALUATION_UNAVAILABLE")
        self.assertEqual(result["risk"]["unknown_fee_assets"], ["BNB"])
        self.assertEqual(
            self.connection.execute(
                "SELECT COUNT(*) FROM binance_testnet_probe_intents"
            ).fetchone()[0],
            0,
        )
        self.assertEqual(service.validation_status()["reason"], "FEE_VALUATION_UNAVAILABLE")
        self.assertEqual(
            len([name for name, _ in venue.calls if name == "place_limit_order"]),
            0,
        )

    def test_realized_quote_fees_are_not_double_counted_in_equity_loss(self):
        venue = FakeVenue()
        service = self.service(venue, {"api_key": "public", "api_secret": "private-secret"})
        service.check_connectivity()
        rows = (
            ("fee-buy", "BUY", "0.1"),
            ("fee-sell", "SELL", "0.1"),
        )
        for trade_id, side, commission in rows:
            self.connection.execute(
                "INSERT INTO binance_testnet_probe_fills("
                "trade_id,intent_id,environment,source,probe_kind,symbol,side,"
                "quantity,price,quote_quantity,commission,commission_asset,"
                "trade_time_utc,payload_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    trade_id,
                    "ledger-intent",
                    BINANCE_SPOT_TESTNET,
                    SOURCE,
                    PROBE_LABEL,
                    "ETHUSDT",
                    side,
                    "1",
                    "10",
                    "10",
                    commission,
                    "USDT",
                    f"2023-11-14T22:13:2{0 if side == 'BUY' else 1}+00:00",
                    "{}",
                ),
            )
        self.connection.commit()
        risk = service.probe_status()["risk"]
        self.assertEqual(risk["realized_pnl"], "-0.2")
        self.assertEqual(risk["fees_quote"], "0.2")
        self.assertEqual(risk["equity_loss"], "0.2")

    def test_autonomous_held_reservation_blocks_probe_from_shared_account(self):
        self.connection.executescript(
            """
            CREATE TABLE binance_execution_order_intents (
                intent_id TEXT PRIMARY KEY, intent TEXT, side TEXT, symbol TEXT,
                quantity TEXT, notional TEXT, fee_reserve TEXT, state TEXT,
                submitted_at TEXT, updated_at TEXT, client_order_id TEXT,
                exchange_order_id TEXT
            );
            CREATE TABLE binance_execution_risk_reservations (
                intent_id TEXT, symbol TEXT, side TEXT, amount TEXT,
                reserved_quantity TEXT, fee_reserve TEXT, status TEXT
            );
            CREATE TABLE binance_execution_fills (
                trade_id TEXT, intent_id TEXT, client_order_id TEXT,
                exchange_order_id TEXT, symbol TEXT, side TEXT, quantity TEXT,
                price TEXT, quote_quantity TEXT, commission TEXT,
                commission_asset TEXT, trade_time TEXT, payload_json TEXT
            );
            CREATE TABLE binance_execution_positions (
                symbol TEXT, quantity TEXT, cost_basis TEXT, realized_pnl TEXT,
                unrealized_pnl TEXT, fees_quote TEXT, valuation_status TEXT
            );
            INSERT INTO binance_execution_order_intents VALUES
                ('auto-held','ENTRY','BUY','ETHUSDT','1','10','0.01','RESERVED',NULL,'2023-11-14T22:13:20+00:00','auto-held-client',NULL);
            INSERT INTO binance_execution_risk_reservations VALUES
                ('auto-held','ETHUSDT','BUY','10','0','0.01','HELD');
            """
        )
        self.connection.commit()
        service = self.service(FakeVenue(), {"api_key": "public", "api_secret": "private-secret"})
        result = service.execute_probe()
        self.assertEqual(result["status"], "BLOCKED")
        self.assertEqual(result["reason"], "AUTONOMOUS_UNRESOLVED")
        self.assertEqual(
            self.connection.execute("SELECT COUNT(*) FROM binance_testnet_probe_intents").fetchone()[0],
            0,
        )

    def test_account_read_uses_server_offset_and_bounded_recv_window(self):
        venue = FakeVenue()
        result = self.service(venue, {"api_key": "public", "api_secret": "private-secret"}).check_connectivity()
        self.assertEqual(result["status"], "PASS")
        account_call = next(kwargs for name, kwargs in venue.calls if name == "account")
        self.assertEqual(account_call["timestamp"], 1_700_000_000_000)
        self.assertEqual(account_call["recvWindow"], 5000)

    def test_prohibited_permission_blocks_even_with_spot_trade(self):
        service = self.service(FakeVenue(), {"api_key": "public", "api_secret": "private-secret"})
        valid, reason, account = service._account_projection(
            {
                "accountType": "SPOT",
                "canTrade": True,
                "permissions": ["SPOT", "TRADE", "WITHDRAWAL"],
                "balances": [{"asset": "USDT", "free": "100", "locked": "0"}],
            }
        )
        self.assertFalse(valid)
        self.assertEqual(reason, "PROHIBITED_PERMISSION")
        self.assertNotIn("WITHDRAWAL", account["permissions"])

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

    def test_missing_authoritative_buy_order_id_rejects_unrelated_trade(self):
        venue = MissingAuthoritativeOrderIdVenue(omit_order_id_sides={"BUY"})
        service = self.service(venue, {"api_key": "public", "api_secret": "private-secret"})
        submitted = service.execute_probe()
        buy_id = submitted["intent"]["exchange_order_id"]
        before_reservation = dict(
            self.connection.execute(
                "SELECT status,amount,fee_reserve,reserved_quantity,released_at_utc "
                "FROM binance_testnet_probe_reservations WHERE side='BUY'"
            ).fetchone()
        )
        venue.fills[buy_id] = [
            {
                "tradeId": "unrelated-buy",
                "orderId": "unrelated-buy",
                "symbol": "ETHUSDT",
                "qty": "8",
                "price": "100",
                "quoteQty": "800",
                "commission": "7",
                "commissionAsset": "ETH",
                "time": 1_700_000_000_100,
            }
        ]

        result = service.reconcile_probe()

        self.assertEqual(result["intent"]["state"], "UNKNOWN")
        self.assertEqual(result["intent"]["filled_quantity"], "0")
        self.assertEqual(result["intent"]["fee_paid"], "0")
        self.assertEqual(result["owned_quantity"], "0")
        self.assertEqual(result["realized_pnl"], "0")
        self.assertEqual(
            self.connection.execute("SELECT COUNT(*) FROM binance_testnet_probe_fills").fetchone()[0],
            0,
        )
        self.assertEqual(
            dict(
                self.connection.execute(
                    "SELECT status,amount,fee_reserve,reserved_quantity,released_at_utc "
                    "FROM binance_testnet_probe_reservations WHERE side='BUY'"
                ).fetchone()
            ),
            before_reservation,
        )
        self.assertEqual(
            len([name for name, _ in venue.calls if name == "place_limit_order"]),
            1,
        )

    def test_missing_authoritative_sell_order_id_rejects_unrelated_trade_and_release(self):
        venue = MissingAuthoritativeOrderIdVenue(omit_order_id_sides={"SELL"})
        service = self.service(venue, {"api_key": "public", "api_secret": "private-secret"})
        service.execute_probe()

        first = service.reconcile_probe()

        self.assertEqual(first["intent"]["state"], "FILLED")
        self.assertIsNotNone(first["exit"])
        self.assertEqual(first["exit"]["state"], "UNKNOWN")
        self.assertEqual(first["status"], "UNKNOWN")
        self.assertFalse(first["risk"]["admissible"])
        self.assertIn("PROBE_UNKNOWN", first["risk"]["reasons"])
        self.assertEqual(first["exit"]["filled_quantity"], "0")
        self.assertEqual(first["exit"]["fee_paid"], "0")
        self.assertEqual(first["realized_pnl"], "0")
        self.assertEqual(
            self.connection.execute(
                "SELECT COUNT(*) FROM binance_testnet_probe_fills WHERE side='BUY'"
            ).fetchone()[0],
            1,
        )
        self.assertEqual(
            self.connection.execute(
                "SELECT COUNT(*) FROM binance_testnet_probe_fills WHERE side='SELL'"
            ).fetchone()[0],
            0,
        )
        buy_fill = self.connection.execute(
            "SELECT quantity FROM binance_testnet_probe_fills WHERE side='BUY'"
        ).fetchone()
        self.assertEqual(first["owned_quantity"], buy_fill["quantity"])
        sell_reservation = dict(
            self.connection.execute(
                "SELECT status,reserved_quantity,released_at_utc "
                "FROM binance_testnet_probe_reservations WHERE side='SELL'"
            ).fetchone()
        )
        self.assertEqual(sell_reservation["status"], "HELD")
        self.assertIsNone(sell_reservation["released_at_utc"])

        second = service.reconcile_probe()

        self.assertEqual(second["exit"]["state"], "UNKNOWN")
        self.assertEqual(second["owned_quantity"], first["owned_quantity"])
        self.assertEqual(second["realized_pnl"], "0")
        self.assertEqual(
            dict(
                self.connection.execute(
                    "SELECT status,reserved_quantity,released_at_utc "
                    "FROM binance_testnet_probe_reservations WHERE side='SELL'"
                ).fetchone()
            ),
            sell_reservation,
        )
        self.assertEqual(
            self.connection.execute(
                "SELECT COUNT(*) FROM binance_testnet_probe_fills WHERE side='SELL'"
            ).fetchone()[0],
            0,
        )
        self.assertEqual(
            len([name for name, kwargs in venue.calls if name == "place_limit_order" and kwargs["side"] == "SELL"]),
            1,
        )

    def test_trade_evidence_requires_exact_order_id_and_recovers_idempotently(self):
        venue = FakeVenue(partial=True)
        service = self.service(venue, {"api_key": "public", "api_secret": "private-secret"})
        submitted = service.execute_probe()
        buy_id = submitted["intent"]["exchange_order_id"]
        exact_trade = dict(venue.fills[buy_id][0])
        exact_trade["commission"] = "0.0002"
        exact_trade["commissionAsset"] = "USDT"
        missing_order_id = dict(exact_trade)
        missing_order_id.pop("orderId")
        missing_order_id.update({"tradeId": "missing-order-id", "qty": "9", "commission": "4"})
        malformed = dict(exact_trade, tradeId="malformed", qty="not-a-number", commission="8")
        unrelated = dict(exact_trade, tradeId="unrelated", orderId="999999", qty="8", commission="7")
        venue.fills[buy_id] = [None, missing_order_id, malformed, unrelated]

        first = service.reconcile_probe()
        self.assertEqual(first["intent"]["state"], "UNKNOWN")
        self.assertEqual(first["owned_quantity"], "0")
        self.assertEqual(
            self.connection.execute("SELECT COUNT(*) FROM binance_testnet_probe_fills").fetchone()[0],
            0,
        )
        held = self.connection.execute(
            "SELECT status FROM binance_testnet_probe_reservations WHERE side='BUY'"
        ).fetchone()
        self.assertEqual(held["status"], "HELD")

        venue.fills[buy_id].append(exact_trade)
        recovered = service.reconcile_probe()
        self.assertEqual(recovered["intent"]["state"], "PARTIALLY_FILLED")
        self.assertEqual(recovered["owned_quantity"], "0.0251")
        self.assertEqual(recovered["intent"]["fee_paid"], "0.0002")
        self.assertEqual(
            self.connection.execute("SELECT COUNT(*) FROM binance_testnet_probe_fills").fetchone()[0],
            1,
        )
        fills = self.connection.execute(
            "SELECT quantity,commission FROM binance_testnet_probe_fills"
        ).fetchone()
        self.assertEqual((fills["quantity"], fills["commission"]), ("0.0251", "0.0002"))

        again = service.reconcile_probe()
        self.assertEqual(again["intent"]["state"], "PARTIALLY_FILLED")
        self.assertEqual(again["owned_quantity"], "0.0251")
        self.assertEqual(again["intent"]["fee_paid"], "0.0002")
        self.assertEqual(
            self.connection.execute("SELECT COUNT(*) FROM binance_testnet_probe_fills").fetchone()[0],
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


    def test_reset_missing_history_is_durable_pause_and_never_resubmitted(self):
        venue = FakeVenue()
        service = self.service(venue, {"api_key": "public", "api_secret": "private-secret"})
        service.execute_probe()
        remote_before = {
            name: len([called for called, _ in venue.calls if called == name])
            for name in ("cancel_order", "place_limit_order")
        }
        query_before = len([called for called, _ in venue.calls if called == "query_order"])
        venue.orders.clear()
        result = service.reconcile_probe()
        self.assertEqual(result["status"], "UNKNOWN")
        self.assertEqual(result["reason"], "TESTNET_RESET_HISTORY_MISSING")
        self.assertEqual(
            {
                name: len([called for called, _ in venue.calls if called == name])
                for name in remote_before
            },
            remote_before,
        )
        self.assertEqual(
            len([called for called, _ in venue.calls if called == "query_order"]),
            query_before + 1,
        )
        self.assertEqual(result["intent"]["state"], "UNKNOWN")
        self.assertTrue(result["reset_detected"])
        self.assertTrue(result["control_path_paused"])
        self.assertTrue(result["execution_paused"])
        self.assertEqual(
            self.connection.execute(
                "SELECT status FROM binance_testnet_probe_reservations WHERE side='BUY'"
            ).fetchone()["status"],
            "HELD",
        )
        calls_before = len([name for name in venue.calls if name == "place_limit_order"])
        again = service.execute_probe()
        self.assertEqual(again["reason"], "TESTNET_RESET_HISTORY_MISSING")
        self.assertEqual(
            len([name for name in venue.calls if name == "place_limit_order"]),
            calls_before,
        )
    def test_authoritative_buy_history_allows_exit_with_production_account_shape(self):
        venue = FakeVenue()
        service = self.service(venue, {"api_key": "public", "api_secret": "private-secret"})
        service.execute_probe()
        result = service.reconcile_probe()
        self.assertEqual(result["intent"]["state"], "FILLED")
        self.assertEqual(result["exit"]["state"], "FILLED")
        self.assertEqual(
            len(
                [
                    name
                    for name, kwargs in venue.calls
                    if name == "place_limit_order" and kwargs["side"] == "SELL"
                ]
            ),
            1,
        )
        account = service.check_connectivity()["account"]
        self.assertNotIn("epoch", account)
    def test_concurrent_reconcile_serializes_single_exit_submission(self):
        venue = FakeVenue()
        service = self.service(venue, {"api_key": "public", "api_secret": "private-secret"})
        service.execute_probe()
        barrier = threading.Barrier(3)
        results: list[dict] = []
        errors: list[BaseException] = []

        def reconcile() -> None:
            try:
                barrier.wait()
                results.append(service.reconcile_probe())
            except BaseException as exc:
                errors.append(exc)

        threads = [threading.Thread(target=reconcile) for _ in range(2)]
        for thread in threads:
            thread.start()
        barrier.wait()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])
        self.assertEqual(len(results), 2)
        self.assertEqual(
            len(
                [
                    name
                    for name, kwargs in venue.calls
                    if name == "place_limit_order" and kwargs["side"] == "SELL"
                ]
            ),
            1,
        )
        self.assertEqual(
            self.connection.execute(
                "SELECT COUNT(*) FROM binance_testnet_probe_intents WHERE side='SELL'"
            ).fetchone()[0],
            1,
        )
        self.assertEqual({result["exit"]["state"] for result in results}, {"FILLED"})
    def test_execute_and_reconcile_share_operation_lock(self):
        venue = BlockingSubmissionVenue()
        service = self.service(venue, {"api_key": "public", "api_secret": "private-secret"})
        execute_result: list[object] = []
        reconcile_result: list[object] = []

        execute_thread = threading.Thread(
            target=lambda: execute_result.append(service.execute_probe())
        )
        execute_thread.start()
        self.assertTrue(venue.submission_entered.wait(5))

        reconcile_thread = threading.Thread(
            target=lambda: reconcile_result.append(service.reconcile_probe())
        )
        reconcile_thread.start()
        self.assertFalse(venue.query_started.wait(0.1))

        venue.release_submission.set()
        execute_thread.join(timeout=5)
        reconcile_thread.join(timeout=5)
        self.assertFalse(execute_thread.is_alive())
        self.assertFalse(reconcile_thread.is_alive())
        self.assertEqual(len(execute_result), 1)
        self.assertEqual(len(reconcile_result), 1)
        self.assertEqual(
            len(
                [
                    name
                    for name, _ in venue.calls
                    if name == "place_limit_order" and _["side"] == "BUY"
                ]
            ),
            1,
        )
        self.assertEqual(
            len(
                [
                    name
                    for name, _ in venue.calls
                    if name == "place_limit_order" and _["side"] == "SELL"
                ]
            ),
            1,
        )

    def test_credential_store_removal_and_rotation_block_before_network(self):
        old = {"api_key": "old-key", "api_secret": "old-secret"}
        store = MutableCredentialStore(old)
        venue = FakeVenue(credentials=old)
        service = self.service(venue, old, store)
        store.value = None
        removed = service.check_connectivity()
        self.assertEqual(removed["status"], "BLOCKED")
        self.assertEqual(removed["reason"], "CREDENTIALS_NOT_CONFIGURED")
        self.assertEqual(venue.calls, [])

        store = MutableCredentialStore(old)
        venue = FakeVenue(credentials=old)
        service = self.service(venue, old, store)
        store.value = {"api_key": "rotated-key", "api_secret": "rotated-secret"}
        rotated = service.check_connectivity()
        self.assertEqual(rotated["status"], "BLOCKED")
        self.assertEqual(rotated["reason"], "CREDENTIALS_CHANGED")
        self.assertEqual(venue.calls, [])
    def test_final_durable_fence_rejects_reset_before_submit(self):
        venue = FakeVenue()
        service = self.service(venue, {"api_key": "public", "api_secret": "private-secret"})
        original_transition = service._transition

        def transition(intent_id, state, reason, **kwargs):
            original_transition(intent_id, state, reason, **kwargs)
            if reason == "before venue call":
                self.connection.execute(
                    "UPDATE binance_testnet_probe_intents "
                    "SET reason='TESTNET_RESET_HISTORY_MISSING' "
                    "WHERE intent_id=?",
                    (intent_id,),
                )
                self.connection.commit()

        service._transition = transition
        result = service.execute_probe()
        self.assertEqual(result["status"], "UNKNOWN")
        self.assertEqual(result["reason"], "TESTNET_RESET_HISTORY_MISSING")
        self.assertEqual(
            len([name for name, _ in venue.calls if name == "place_limit_order"]),
            0,
        )
    def test_final_credential_fence_rejects_rotation_before_submit(self):
        old = {"api_key": "old-key", "api_secret": "old-secret"}
        store = MutableCredentialStore(old)
        venue = FakeVenue(credentials=old)
        service = self.service(venue, old, store)
        original_transition = service._transition

        def transition(intent_id, state, reason, **kwargs):
            original_transition(intent_id, state, reason, **kwargs)
            if reason == "before venue call":
                store.value = {
                    "api_key": "rotated-key",
                    "api_secret": "rotated-secret",
                }

        service._transition = transition
        result = service.execute_probe()
        self.assertEqual(result["status"], "BLOCKED")
        self.assertEqual(result["reason"], "CREDENTIALS_CHANGED")
        self.assertEqual(
            len([name for name, _ in venue.calls if name == "place_limit_order"]),
            0,
        )



    def test_reset_during_market_sizing_blocks_stale_exit(self):
        class HistoryDisappearsDuringMarketVenue(FakeVenue):
            def ticker_book(self, *, symbol):
                result = super().ticker_book(symbol=symbol)
                self.orders.clear()
                self.fills.clear()
                return result

        venue = HistoryDisappearsDuringMarketVenue()
        service = self.service(venue, {"api_key": "public", "api_secret": "private-secret"})
        service.execute_probe()
        result = service.reconcile_probe()
        self.assertEqual(result["intent"]["state"], "UNKNOWN")
        self.assertEqual(result["reason"], "TESTNET_RESET_HISTORY_MISSING")
        self.assertEqual(
            self.connection.execute(
                "SELECT status FROM binance_testnet_probe_reservations WHERE side='BUY'"
            ).fetchone()["status"],
            "HELD",
        )
        self.assertEqual(
            len(
                [
                    name
                    for name, kwargs in venue.calls
                    if name == "place_limit_order" and kwargs["side"] == "SELL"
                ]
            ),
            0,
        )
        self.assertEqual(
            len([name for name, _ in venue.calls if name == "cancel_order"]),
            0,
        )


    def test_canceled_terminal_without_my_trades_is_unknown_and_held(self):
        venue = TerminalEvidenceVenue(
            buy_status="CANCELED",
            buy_partial=True,
            trades_available=False,
        )
        service = self.service(venue, {"api_key": "public", "api_secret": "private-secret"})
        submitted = service.execute_probe()
        self.assertNotIn("fills", submitted["intent"])
        result = service.reconcile_probe()
        self.assertEqual(result["intent"]["state"], "UNKNOWN")
        self.assertEqual(result["owned_quantity"], "0")
        self.assertIsNone(result["exit"])
        held = self.connection.execute(
            "SELECT status FROM binance_testnet_probe_reservations WHERE side='BUY'"
        ).fetchone()
        self.assertEqual(held["status"], "HELD")
        self.assertEqual(
            len([name for name, _ in venue.calls if name == "place_limit_order" and _["side"] == "SELL"]),
            0,
        )

    def test_expired_terminal_without_my_trades_is_unknown_and_retried(self):
        venue = TerminalEvidenceVenue(
            buy_status="EXPIRED",
            buy_partial=True,
            trades_available=False,
        )
        service = self.service(venue, {"api_key": "public", "api_secret": "private-secret"})
        service.execute_probe()
        first = service.reconcile_probe()
        self.assertEqual(first["intent"]["state"], "UNKNOWN")
        venue.publish_trades()
        second = service.reconcile_probe()
        self.assertEqual(second["intent"]["state"], "EXPIRED")
        self.assertEqual(second["owned_quantity"], "0.0251")
        self.assertEqual(second["exit"]["state"], "DUST")
        self.assertEqual(
            self.connection.execute(
                "SELECT COUNT(*) FROM binance_testnet_probe_fills"
            ).fetchone()[0],
            1,
        )

    def test_delayed_my_trades_recovery_is_idempotent_for_partial_buy(self):
        venue = TerminalEvidenceVenue(
            buy_status="CANCELED",
            buy_partial=True,
            trades_available=False,
        )
        service = self.service(venue, {"api_key": "public", "api_secret": "private-secret"})
        service.execute_probe()
        first = service.reconcile_probe()
        self.assertEqual(first["intent"]["state"], "UNKNOWN")
        venue.publish_trades()
        recovered = service.reconcile_probe()
        self.assertEqual(recovered["intent"]["state"], "CANCELED")
        self.assertEqual(recovered["owned_quantity"], "0.0251")
        self.assertEqual(recovered["exit"]["state"], "DUST")
        fills = self.connection.execute(
            "SELECT COUNT(*) FROM binance_testnet_probe_fills"
        ).fetchone()[0]
        again = service.reconcile_probe()
        self.assertEqual(again["owned_quantity"], "0.0251")
        self.assertEqual(
            self.connection.execute(
                "SELECT COUNT(*) FROM binance_testnet_probe_fills"
            ).fetchone()[0],
            fills,
        )

    def test_nonpass_connectivity_persistence_crossing_deadline_projects_expiry(self):
        venue = FakeVenue(auth_code=-1022)
        service = self.service(venue, {"api_key": "public", "api_secret": "private-secret"})
        expired = {"value": False}
        original_persist = service._persist_connectivity

        def persist(result):
            original_persist(result)
            expired["value"] = True

        service._persist_connectivity = persist
        with patch("axiom.binance_testnet._deadline_expired", side_effect=lambda _deadline: expired["value"]):
            result = service.check_connectivity(deadline_monotonic=1.0)

        self.assertEqual(result["status"], "BLOCKED")
        self.assertEqual(result["reason"], AUTO_DEADLINE_EXPIRED)
        latest = self.connection.execute(
            "SELECT status,reason FROM binance_testnet_gate_connectivity ORDER BY record_id DESC LIMIT 1"
        ).fetchone()
        self.assertEqual((latest["status"], latest["reason"]), ("BLOCKED", AUTO_DEADLINE_EXPIRED))
        self.assertEqual([name for name, _ in venue.calls], ["time", "account"])

    def test_nonpass_validation_persistence_crossing_deadline_projects_expiry(self):
        venue = FakeVenue(auth_code=-1022)
        service = self.service(venue, {"api_key": "public", "api_secret": "private-secret"})
        expired = {"value": False}
        original_persist = service._persist_validation

        def persist(result):
            original_persist(result)
            expired["value"] = True

        service._persist_validation = persist
        with patch("axiom.binance_testnet._deadline_expired", side_effect=lambda _deadline: expired["value"]):
            result = service.validate_order(deadline_monotonic=1.0)

        self.assertEqual(result["status"], "BLOCKED")
        self.assertEqual(result["reason"], AUTO_DEADLINE_EXPIRED)
        latest = self.connection.execute(
            "SELECT status,reason FROM binance_testnet_gate_validation ORDER BY record_id DESC LIMIT 1"
        ).fetchone()
        self.assertEqual((latest["status"], latest["reason"]), ("BLOCKED", AUTO_DEADLINE_EXPIRED))
        self.assertEqual([name for name, _ in venue.calls], ["time", "account"])

    def test_malformed_trade_symbol_keeps_buy_ledger_unchanged(self):
        venue = MalformedTradeSymbolVenue()
        service = self.service(venue, {"api_key": "public", "api_secret": "private-secret"})
        submitted = service.execute_probe()
        before_intent = dict(
            self.connection.execute(
                "SELECT intent_id,client_order_id,exchange_order_id,filled_quantity,fee_paid "
                "FROM binance_testnet_probe_intents WHERE side='BUY'"
            ).fetchone()
        )
        before_reservation = dict(
            self.connection.execute(
                "SELECT status,amount,fee_reserve,reserved_quantity,released_at_utc "
                "FROM binance_testnet_probe_reservations WHERE side='BUY'"
            ).fetchone()
        )

        result = service.reconcile_probe()

        after_intent = dict(
            self.connection.execute(
                "SELECT intent_id,client_order_id,exchange_order_id,filled_quantity,fee_paid "
                "FROM binance_testnet_probe_intents WHERE side='BUY'"
            ).fetchone()
        )
        after_reservation = dict(
            self.connection.execute(
                "SELECT status,amount,fee_reserve,reserved_quantity,released_at_utc "
                "FROM binance_testnet_probe_reservations WHERE side='BUY'"
            ).fetchone()
        )
        self.assertEqual(result["intent"]["state"], "UNKNOWN")
        self.assertEqual(after_intent, before_intent)
        self.assertEqual(after_reservation, before_reservation)
        self.assertEqual(result["intent"]["intent_id"], submitted["intent"]["intent_id"])
        self.assertEqual(result["owned_quantity"], "0")
        self.assertEqual(result["realized_pnl"], "0")
        self.assertEqual(
            self.connection.execute("SELECT COUNT(*) FROM binance_testnet_probe_fills").fetchone()[0],
            0,
        )

    def test_malformed_buy_cumulative_keeps_buy_ledger_unchanged(self):
        venue = MalformedBuyCumulativeVenue()
        service = self.service(venue, {"api_key": "public", "api_secret": "private-secret"})
        submitted = service.execute_probe()
        before_intent = dict(
            self.connection.execute(
                "SELECT intent_id,client_order_id,exchange_order_id,filled_quantity,fee_paid "
                "FROM binance_testnet_probe_intents WHERE side='BUY'"
            ).fetchone()
        )
        before_reservation = dict(
            self.connection.execute(
                "SELECT status,amount,fee_reserve,reserved_quantity,released_at_utc "
                "FROM binance_testnet_probe_reservations WHERE side='BUY'"
            ).fetchone()
        )

        result = service.reconcile_probe()

        after_intent = dict(
            self.connection.execute(
                "SELECT intent_id,client_order_id,exchange_order_id,filled_quantity,fee_paid "
                "FROM binance_testnet_probe_intents WHERE side='BUY'"
            ).fetchone()
        )
        after_reservation = dict(
            self.connection.execute(
                "SELECT status,amount,fee_reserve,reserved_quantity,released_at_utc "
                "FROM binance_testnet_probe_reservations WHERE side='BUY'"
            ).fetchone()
        )
        self.assertEqual(result["intent"]["state"], "UNKNOWN")
        self.assertEqual(after_intent, before_intent)
        self.assertEqual(after_reservation, before_reservation)
        self.assertEqual(result["intent"]["intent_id"], submitted["intent"]["intent_id"])
        self.assertEqual(result["owned_quantity"], "0")
        self.assertEqual(result["realized_pnl"], "0")
        self.assertEqual(
            self.connection.execute("SELECT COUNT(*) FROM binance_testnet_probe_fills").fetchone()[0],
            0,
        )

    def test_partial_sell_releases_only_remainder_and_never_oversells(self):
        venue = TerminalEvidenceVenue(
            sell_status="CANCELED",
            sell_partial=True,
        )
        service = self.service(venue, {"api_key": "public", "api_secret": "private-secret"})
        service.execute_probe()
        result = service.reconcile_probe()
        self.assertEqual(result["exit"]["state"], "CANCELED")
        self.assertEqual(result["owned_quantity"], "0.0251")
        sell = next(
            kwargs
            for name, kwargs in venue.calls
            if name == "place_limit_order" and kwargs["side"] == "SELL"
        )
        self.assertEqual(sell["quantity"], "0.0502")
        reservation = self.connection.execute(
            "SELECT status,reserved_quantity FROM binance_testnet_probe_reservations WHERE side='SELL'"
        ).fetchone()
        self.assertEqual(reservation["status"], "RELEASED")
        self.assertEqual(reservation["reserved_quantity"], "0.0251")
        service.reconcile_probe()
        self.assertEqual(
            len([name for name, kwargs in venue.calls if name == "place_limit_order" and kwargs["side"] == "SELL"]),
            1,
        )

    def test_expired_gate_is_persisted_blocked_and_makes_no_later_call(self):
        venue = FakeVenue()
        service = self.service(venue, {"api_key": "public", "api_secret": "private-secret"})
        result = service.validate_order(deadline_monotonic=time.monotonic() - 1)
        self.assertEqual(result["reason"], "AUTO_DEADLINE_EXPIRED")
        self.assertEqual(venue.calls, [])
        self.assertEqual(service.validation_status()["reason"], "AUTO_DEADLINE_EXPIRED")
        self.assertNotEqual(service.validation_status()["status"], "PASS")

    def test_timeout_cap_is_locked_and_restored_for_overlapping_direct_call(self):
        calls: list[float] = []
        entered = threading.Event()
        release = threading.Event()

        class Response:
            status = 200
            headers = {}

            @staticmethod
            def read():
                return b'{"serverTime":1700000000000}'

        def opener(_request, *, timeout):
            calls.append(timeout)
            if len(calls) == 1:
                entered.set()
                release.wait(2)
            return Response()

        client = BinanceSpotRESTClient(
            BINANCE_SPOT_TESTNET,
            {"api_key": "public", "api_secret": "private-secret"},
            opener=opener,
            timeout=10,
        )
        first = threading.Thread(
            target=lambda: _invoke(client.time, {}, deadline_monotonic=time.monotonic() + 5)
        )
        first.start()
        self.assertTrue(entered.wait(1))
        second_done = threading.Event()
        second = threading.Thread(target=lambda: (client.time(), second_done.set()))
        second.start()
        self.assertTrue(second_done.wait(1))
        release.set()
        first.join(2)
        second.join(2)
        self.assertEqual(calls[0], 1.0)
        self.assertEqual(calls[1], 10.0)
        self.assertEqual(client.timeout, 10.0)

    def test_query_identity_and_trade_economics_preserve_accounting_snapshot(self):
        for field, value in (("clientOrderId", "wrong"), ("symbol", "BTCUSDT"), ("side", "SELL")):
            with self.subTest(field=field):
                class IdentityVenue(FakeVenue):
                    def query_order(self, *, symbol, order_id=None, orig_client_order_id=None):
                        response = super().query_order(
                            symbol=symbol, order_id=order_id, orig_client_order_id=orig_client_order_id
                        )
                        payload = dict(response.payload)
                        payload[field] = value
                        return BinanceSpotResult("OK", payload)

                venue = IdentityVenue()
                service = self.service(venue, {"api_key": "public", "api_secret": "private-secret"})
                service.execute_probe()
                before = (
                    service.probe_status()["owned_quantity"],
                    service.probe_status()["realized_pnl"],
                    [dict(row) for row in self.connection.execute("SELECT * FROM binance_testnet_probe_reservations")],
                    [dict(row) for row in self.connection.execute("SELECT * FROM binance_testnet_probe_fills")],
                )
                service.reconcile_probe()
                after = (
                    service.probe_status()["owned_quantity"],
                    service.probe_status()["realized_pnl"],
                    [dict(row) for row in self.connection.execute("SELECT * FROM binance_testnet_probe_reservations")],
                    [dict(row) for row in self.connection.execute("SELECT * FROM binance_testnet_probe_fills")],
                )
                self.assertEqual(before, after)

        class InvalidTradeVenue(FakeVenue):
            def my_trades(self, *, symbol, order_id=None):
                response = super().my_trades(symbol=symbol, order_id=order_id)
                payload = {"trades": [dict(response.payload["trades"][0], tradeId="bad", quoteQty="-1")]}
                return BinanceSpotResult("OK", payload)

        venue = InvalidTradeVenue()
        service = self.service(venue, {"api_key": "public", "api_secret": "private-secret"})
        service.execute_probe()
        before = service.probe_status()
        service.reconcile_probe()
        after = service.probe_status()
        self.assertEqual(before["owned_quantity"], after["owned_quantity"])
        self.assertEqual(before["realized_pnl"], after["realized_pnl"])
        self.assertEqual(
            self.connection.execute("SELECT COUNT(*) FROM binance_testnet_probe_fills").fetchone()[0],
            0,
        )

if __name__ == "__main__":
    unittest.main()
