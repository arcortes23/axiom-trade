from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
import tempfile
import sqlite3
import threading
import unittest

from axiom.binance_execution import (
    ACKNOWLEDGED,
    CANCELED,
    ENABLE_CONFIRMATION,
    EXPIRED,
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
from axiom.binance_dev import PaperBinanceSpotVenue
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


def FakeVenue(**_: object) -> PaperBinanceSpotVenue:
    """Exact built-in paper venue in scripted no-fill mode."""
    venue = PaperBinanceSpotVenue(fill_ratio="0")
    venue.empty_fill_status = "NEW"
    venue._order_number = 0
    venue._trade_number = 0
    return venue


class _UntrustedVenue:
    def __init__(self, *, environment: str = "PAPER", origin: str | None = None):
        self.environment = environment
        self.origin = origin


def AcceptedDisconnectVenue() -> PaperBinanceSpotVenue:
    venue = FakeVenue()
    place = venue.place_limit_order

    def submit(**kwargs: object):
        place(**kwargs)
        raise ConnectionError("response lost after acceptance")

    venue.place_limit_order = submit
    return venue


def BlockingVenue() -> PaperBinanceSpotVenue:
    venue = FakeVenue()
    venue.started = threading.Event()
    venue.release = threading.Event()
    place = venue.place_limit_order

    def submit(**kwargs: object):
        venue.started.set()
        if not venue.release.wait(timeout=2):
            raise TimeoutError("test venue was not released")
        return place(**kwargs)

    venue.place_limit_order = submit
    return venue


def TimeoutVenue() -> PaperBinanceSpotVenue:
    venue = FakeVenue()
    place = venue.place_limit_order

    def submit(**kwargs: object):
        place(**kwargs)
        raise TimeoutError("blocked")

    venue.place_limit_order = submit
    return venue


def DelayedSubmitVenue(*, fail: bool = False) -> PaperBinanceSpotVenue:
    """Delay one submit so a second connection can commit a terminal state."""
    venue = FakeVenue()
    venue.submit_started = threading.Event()
    venue.submit_release = threading.Event()
    place = venue.place_limit_order

    def submit(**kwargs: object):
        venue.submit_started.set()
        if not venue.submit_release.wait(timeout=2):
            raise TimeoutError("test venue was not released")
        if fail:
            raise TimeoutError("late submit response lost")
        return place(**kwargs)

    venue.place_limit_order = submit
    return venue


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
                self.assertEqual(result["status"], "RESET")
                self.assertEqual(second.control()["state"], PAUSED)
                self.assertEqual(second.control()["pause_reason"], "TESTNET_RESET")
                self.assertEqual(len(venue.submissions), 0)
                self.assertEqual(second.orders()[0]["state"], RESERVED)
                retry = second.submit_signal(signal(), price="10", quantity="1")
                self.assertEqual(retry["intent_id"], second.orders()[0]["intent_id"])
                self.assertEqual(len(venue.submissions), 0)
                transitions = second.store.connection.execute(
                    "SELECT to_state FROM binance_execution_order_transitions "
                    "ORDER BY transition_id"
                ).fetchall()
                self.assertEqual([row[0] for row in transitions], [INTENT, RESERVED])
                audit = second.store.connection.execute(
                    "SELECT status,details_json FROM binance_execution_reconciliation "
                    "WHERE key='TESTNET_RESET'"
                ).fetchone()
                self.assertEqual(audit[0], "RESET")
                self.assertIn("authoritative_missing", audit[1])
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
                result = second.reconcile()
                self.assertEqual(result["status"], "RESET")
                self.assertEqual(second.control()["state"], PAUSED)
                self.assertEqual(second.control()["pause_reason"], "TESTNET_RESET")
                self.assertEqual(len(venue.submissions), 0)
                self.assertEqual(second.orders()[0]["state"], "SUBMITTING")
                retry = second.submit_signal(signal(), price="10", quantity="1")
                self.assertEqual(retry["intent_id"], second.orders()[0]["intent_id"])
                self.assertEqual(len(venue.submissions), 0)
                transitions = second.store.connection.execute(
                    "SELECT to_state FROM binance_execution_order_transitions "
                    "ORDER BY transition_id"
                ).fetchall()
                self.assertEqual(
                    [row[0] for row in transitions],
                    [INTENT, RESERVED, "SUBMITTING"],
                )
                audit = second.store.connection.execute(
                    "SELECT status,details_json FROM binance_execution_reconciliation "
                    "WHERE key='TESTNET_RESET'"
                ).fetchone()
                self.assertEqual(audit[0], "RESET")
                self.assertIn("authoritative_missing", audit[1])
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

    def test_restart_after_fill_commit_releases_durable_filled_reservation(self):
        with tempfile.TemporaryDirectory() as directory:
            path = directory + "/execution.sqlite"
            venue = FakeVenue()
            venue.fill_ratio = Decimal("1")

            def crash(point):
                if point == "after_order_result_commit":
                    raise CrashBoundary()

            first = BinanceExecutionService(
                path,
                venue=venue,
                environment="PAPER",
                fault_hook=crash,
            )
            try:
                first.update_account({"quote_available": "100", "owned_inventory": {}})
                first.enable_auto_canary(ENABLE_CONFIRMATION)
                with self.assertRaises(CrashBoundary):
                    first.submit_signal(signal(), price="10", quantity="1")
                self.assertEqual(first.orders()[0]["state"], FILLED)
                self.assertEqual(
                    first.orders()[0]["risk_reservation"]["status"],
                    "HELD",
                )
            finally:
                first.close()

            second = BinanceExecutionService(path, venue=venue, environment="PAPER")
            try:
                self.assertEqual(second.reconcile()["status"], "SUCCESS")
                order = second.orders()[0]
                self.assertEqual(order["state"], FILLED)
                self.assertEqual(order["risk_reservation"]["status"], "RELEASED")
                self.assertEqual(second.position("BTCUSDT")["quantity"], "1")
                self.assertEqual(len(venue.submissions), 1)
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

    def test_ok_transport_missing_or_unknown_exchange_status_is_unknown_and_held(self):
        responses = (
            {"status": "OK", "orderId": "missing-status"},
            {
                "status": "OK",
                "payload": {"status": "QUEUED", "orderId": "unknown-status"},
            },
        )
        for index, response in enumerate(responses):
            with self.subTest(response=response):
                with tempfile.TemporaryDirectory() as directory:
                    venue = FakeVenue()

                    def submit(**_: object):
                        return response

                    venue.place_limit_order = submit
                    service = BinanceExecutionService(
                        directory + "/execution.sqlite",
                        venue=venue,
                        environment="PAPER",
                    )
                    try:
                        service.update_account(
                            {"quote_available": "100", "owned_inventory": {}}
                        )
                        service.enable_auto_canary(ENABLE_CONFIRMATION)
                        result = service.submit_signal(
                            signal("malformed-" + str(index)),
                            price="10",
                            quantity="1",
                        )
                        self.assertEqual(result["state"], UNKNOWN)
                        self.assertEqual(
                            result["risk_reservation"]["status"],
                            "HELD",
                        )
                        transitions = [
                            row[0]
                            for row in service.store.connection.execute(
                                "SELECT to_state "
                                "FROM binance_execution_order_transitions "
                                "ORDER BY transition_id"
                            )
                        ]
                        self.assertIn(UNKNOWN, transitions)
                        self.assertNotIn(ACKNOWLEDGED, transitions)
                    finally:
                        service.close()

    def test_rate_limit_submission_stays_held_and_reconciles_by_client_id(self):
        def rate_limited(**_: object):
            return {"status": "RATE_LIMIT", "code": 429}

        self.venue.place_limit_order = rate_limited
        self.service.enable_auto_canary(ENABLE_CONFIRMATION)
        result = self.service.submit_signal(signal(), price="10", quantity="1")
        self.assertEqual(result["state"], UNKNOWN)
        self.assertEqual(result["risk_reservation"]["status"], "HELD")

        queries = []

        def query(**kwargs: object):
            queries.append(dict(kwargs))
            return {
                "status": "NEW",
                "orderId": "rate-limit-order",
                "executedQty": "0",
            }

        self.venue.query_order = query
        self.assertEqual(self.service.reconcile()["status"], "SUCCESS")
        self.assertEqual(self.service.orders()[0]["state"], ACKNOWLEDGED)
        self.assertEqual(
            self.service.orders()[0]["risk_reservation"]["status"],
            "HELD",
        )
        self.assertEqual(len(queries), 1)
        self.assertEqual(
            queries[0]["orig_client_order_id"],
            result["client_order_id"],
        )
        self.assertNotIn("order_id", queries[0])

        with tempfile.TemporaryDirectory() as directory:
            rejected_venue = FakeVenue()

            def rejected(**_: object):
                return {"status": "REJECTED", "code": -2010}

            rejected_venue.place_limit_order = rejected
            rejected_service = BinanceExecutionService(
                directory + "/execution.sqlite",
                venue=rejected_venue,
                environment="PAPER",
            )
            try:
                rejected_service.update_account(
                    {"quote_available": "100", "owned_inventory": {}}
                )
                rejected_service.enable_auto_canary(ENABLE_CONFIRMATION)
                explicit = rejected_service.submit_signal(
                    signal("explicit-rejection"),
                    price="10",
                    quantity="1",
                )
                self.assertEqual(explicit["state"], REJECTED)
                self.assertEqual(
                    explicit["risk_reservation"]["status"],
                    "RELEASED",
                )
            finally:
                rejected_service.close()

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

    def test_stale_unknown_after_terminal_filled_cannot_regress_or_retain_reservation(self):
        with tempfile.TemporaryDirectory() as directory:
            path = directory + "/execution.sqlite"
            venue = DelayedSubmitVenue(fail=True)
            first = BinanceExecutionService(path, venue=venue, environment="PAPER", owner_id="first")
            second = None
            errors: list[BaseException] = []
            results: list[dict] = []
            try:
                first.update_account({"quote_available": "100", "owned_inventory": {}})
                first.enable_auto_canary(ENABLE_CONFIRMATION)
                second = BinanceExecutionService(path, venue=venue, environment="PAPER", owner_id="second")

                def submit():
                    try:
                        results.append(first.submit_signal(signal(), price="10", quantity="1"))
                    except BaseException as exc:
                        errors.append(exc)

                thread = threading.Thread(target=submit)
                thread.start()
                self.assertTrue(venue.submit_started.wait(timeout=2))
                intent_id = second.orders()[0]["intent_id"]
                second.record_fills(
                    intent_id,
                    [{"tradeId": "terminal-fill", "qty": "1", "price": "10", "quoteQty": "10"}],
                )
                self.assertEqual(second.orders()[0]["state"], FILLED)
                venue.submit_release.set()
                thread.join(timeout=2)

                self.assertFalse(thread.is_alive())
                self.assertEqual(errors, [])
                self.assertEqual(results[0]["state"], FILLED)
                self.assertEqual(first.orders()[0]["state"], FILLED)
                self.assertEqual(first.orders()[0]["risk_reservation"]["status"], "RELEASED")
                self.assertEqual(len(first.fills()), 1)
                transitions = first.store.connection.execute(
                    "SELECT to_state FROM binance_execution_order_transitions "
                    "ORDER BY transition_id"
                ).fetchall()
                self.assertEqual(
                    [row[0] for row in transitions],
                    [INTENT, RESERVED, "SUBMITTING", FILLED],
                )
            finally:
                venue.submit_release.set()
                if second is not None:
                    second.close()
                first.close()

    def test_late_submit_ack_after_canceled_cannot_resurrect_order(self):
        with tempfile.TemporaryDirectory() as directory:
            path = directory + "/execution.sqlite"
            venue = DelayedSubmitVenue()
            first = BinanceExecutionService(path, venue=venue, environment="PAPER", owner_id="first")
            second = None
            errors: list[BaseException] = []
            results: list[dict] = []
            try:
                first.update_account({"quote_available": "100", "owned_inventory": {}})
                first.enable_auto_canary(ENABLE_CONFIRMATION)
                second = BinanceExecutionService(path, venue=venue, environment="PAPER", owner_id="second")

                def submit():
                    try:
                        results.append(first.submit_signal(signal(), price="10", quantity="1"))
                    except BaseException as exc:
                        errors.append(exc)

                thread = threading.Thread(target=submit)
                thread.start()
                self.assertTrue(venue.submit_started.wait(timeout=2))
                row = second.orders()[0]
                venue.cancel_result = {"status": "CANCELED", "executedQty": "0"}
                canceled = second.cancel(row["client_order_id"], symbol="BTCUSDT")
                self.assertEqual(canceled["state"], "CANCELED")
                self.assertEqual(canceled["risk_reservation"]["status"], "RELEASED")
                venue.submit_release.set()
                thread.join(timeout=2)

                self.assertFalse(thread.is_alive())
                self.assertEqual(errors, [])
                self.assertEqual(results[0]["state"], "CANCELED")
                self.assertEqual(first.orders()[0]["state"], "CANCELED")
                self.assertEqual(first.orders()[0]["risk_reservation"]["status"], "RELEASED")
                self.assertEqual(len(venue.submissions), 1)
                transitions = first.store.connection.execute(
                    "SELECT to_state FROM binance_execution_order_transitions "
                    "ORDER BY transition_id"
                ).fetchall()
                self.assertEqual(
                    [row[0] for row in transitions],
                    [INTENT, RESERVED, "SUBMITTING", "CANCELED"],
                )
                self.assertNotIn("ACKNOWLEDGED", [row[0] for row in transitions])
            finally:
                venue.submit_release.set()
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

    def test_unresolved_sell_reservation_blocks_full_exit_after_pause_and_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            path = directory + "/execution.sqlite"
            venue = TimeoutVenue()
            first = BinanceExecutionService(path, venue=venue, environment="PAPER", owner_id="first")
            second = None
            try:
                first.update_account(
                    {
                        "quote_available": "100",
                        "owned_inventory": {"BTCUSDT": "1"},
                    }
                )
                first.enable_auto_canary(ENABLE_CONFIRMATION)
                unresolved = first.submit_signal(
                    signal("sell-unresolved", intent="EXIT"),
                    price="10",
                    quantity="1",
                )
                self.assertEqual(unresolved["state"], UNKNOWN)
                self.assertEqual(unresolved["risk_reservation"]["status"], "HELD")
                first.pause("SELL_UNRESOLVED")

                second = BinanceExecutionService(
                    path,
                    venue=venue,
                    environment="PAPER",
                    owner_id="second",
                )
                blocked = second.submit_signal(
                    signal("sell-retry", intent="EXIT"),
                    price="10",
                    quantity="1",
                )
                self.assertEqual(blocked["state"], REJECTED)
                self.assertIn("INSUFFICIENT_OWNED_INVENTORY", blocked["reason"])
                self.assertEqual(second.control()["state"], PAUSED)
                self.assertEqual(
                    second.orders(state=UNKNOWN)[0]["risk_reservation"]["status"],
                    "HELD",
                )
                self.assertEqual(len(venue.submissions), 1)
            finally:
                if second is not None:
                    second.close()
                first.close()

    def test_partial_sell_reserves_only_symbol_remaining_quantity_transactionally(self):
        venue = PaperBinanceSpotVenue(
            initial_balances={"USDT": "100", "BTC": "1"},
            fill_ratio="0",
        )
        service = BinanceExecutionService(
            self.store,
            venue=venue,
            environment="PAPER",
        )
        service.update_account(
            {
                "quote_available": "100",
                "owned_inventory": {"BTCUSDT": "1"},
            }
        )
        service.enable_auto_canary(ENABLE_CONFIRMATION)
        seed = service.submit_signal(signal("seed"), price="10", quantity="1")
        service.record_fills(
            seed["intent_id"],
            [{"tradeId": "seed-fill", "qty": "1", "price": "10", "quoteQty": "10"}],
        )
        venue.fill_ratio = Decimal("0.5")
        partial = service.submit_signal(
            signal("sell-partial", intent="EXIT"),
            price="10",
            quantity="1",
        )
        self.assertEqual(partial["state"], PARTIALLY_FILLED)
        self.assertEqual(partial["risk_reservation"]["status"], "HELD")
        observed = next(row for row in service.orders() if row["intent_id"] == partial["intent_id"])
        self.assertEqual(observed["risk_reservation"]["reserved_quantity"], "0.5")
        partial_fill = service.fills(client_order_id=partial["client_order_id"])[0]
        service.record_fills(partial["intent_id"], [partial_fill])
        duplicate = next(row for row in service.orders() if row["intent_id"] == partial["intent_id"])
        self.assertEqual(duplicate["risk_reservation"]["reserved_quantity"], "0.5")
        blocked = service.submit_signal(
            signal("sell-full-retry", intent="EXIT"),
            price="10",
            quantity="1",
        )
        self.assertEqual(blocked["state"], REJECTED)
        self.assertIn("INSUFFICIENT_OWNED_INVENTORY", blocked["reason"])
        self.assertEqual(len(venue.submissions), 2)
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
        self.assertEqual(self.venue.cancellations[-1]["order_id"], entry["exchange_order_id"])

    def test_equal_exchange_trade_ids_are_scoped_by_symbol(self):
        self.service.enable_auto_canary(ENABLE_CONFIRMATION)
        btc = self.service.submit_signal(signal("btc"), price="10", quantity="1")
        eth = self.service.submit_signal(signal("eth", symbol="ETHUSDT"), price="20", quantity="1")
        self.service.record_fills(
            btc["intent_id"],
            [{"tradeId": "same-trade", "qty": "1", "price": "10", "quoteQty": "10", "time": T0.isoformat()}],
        )
        self.service.record_fills(
            eth["intent_id"],
            [{"tradeId": "same-trade", "qty": "1", "price": "20", "quoteQty": "20", "time": T0.isoformat()}],
        )
        self.assertEqual(len(self.service.fills()), 2)
        self.assertEqual(self.service.position("BTCUSDT")["quantity"], "1")
        self.assertEqual(self.service.position("ETHUSDT")["quantity"], "1")

    def test_legacy_raw_trade_id_migration_is_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            first = BinanceExecutionService(directory + "/execution.sqlite", venue=FakeVenue(), environment="PAPER")
            first.update_account({"quote_available": "100", "owned_inventory": {}})
            first.enable_auto_canary(ENABLE_CONFIRMATION)
            entry = first.submit_signal(signal(), price="10", quantity="1")
            first.record_fills(
                entry["intent_id"],
                [{"tradeId": "legacy", "qty": "1", "price": "10", "quoteQty": "10"}],
            )
            first.store.connection.execute(
                "UPDATE binance_execution_fills SET trade_id='legacy'"
            )
            first.store.connection.commit()
            first.close()
            second = BinanceExecutionService(directory + "/execution.sqlite", venue=FakeVenue(), environment="PAPER")
            try:
                rows = second.fills()
                self.assertEqual(len(rows), 1)
                self.assertEqual(rows[0]["trade_id"], "BTCUSDT:legacy")
            finally:
                second.close()
            third = BinanceExecutionService(directory + "/execution.sqlite", venue=FakeVenue(), environment="PAPER")
            try:
                self.assertEqual(len(third.fills()), 1)
                self.assertEqual(third.fills()[0]["trade_id"], "BTCUSDT:legacy")
            finally:
                third.close()

    def test_legacy_reserved_quantity_migration_backfills_held_sells_idempotently(self):
        with tempfile.TemporaryDirectory() as directory:
            path = directory + "/execution.sqlite"
            first = BinanceExecutionService(path, venue=FakeVenue(), environment="PAPER")
            first.update_account(
                {
                    "quote_available": "1000",
                    "owned_inventory": {"BTCUSDT": "3"},
                }
            )
            first.enable_auto_canary(ENABLE_CONFIRMATION)
            partial = first.submit_signal(
                signal("legacy-partial", intent="EXIT"),
                price="10",
                quantity="2",
            )
            first.record_fills(
                partial["intent_id"],
                [{"tradeId": "legacy-partial-fill", "qty": "0.75", "price": "10", "quoteQty": "7.5"}],
            )
            no_fill = first.submit_signal(
                signal("legacy-no-fill", intent="EXIT"),
                price="10",
                quantity="1",
            )
            buy = first.submit_signal(signal("legacy-buy"), price="10", quantity="1")
            terminal = first.submit_signal(signal("legacy-terminal"), price="10", quantity="1")
            first.cancel(terminal["client_order_id"], symbol="BTCUSDT")
            first.close()

            connection = sqlite3.connect(path)
            connection.execute(
                "ALTER TABLE binance_execution_risk_reservations "
                "RENAME TO binance_execution_risk_reservations_current"
            )
            connection.execute(
                """
                CREATE TABLE binance_execution_risk_reservations_legacy (
                    reservation_id TEXT PRIMARY KEY, intent_id TEXT NOT NULL UNIQUE,
                    symbol TEXT NOT NULL, side TEXT NOT NULL, amount TEXT NOT NULL,
                    fee_reserve TEXT NOT NULL, status TEXT NOT NULL,
                    created_at TEXT NOT NULL, released_at TEXT
                )
                """
            )
            connection.execute(
                """
                INSERT INTO binance_execution_risk_reservations_legacy(
                    reservation_id,intent_id,symbol,side,amount,fee_reserve,status,created_at,released_at
                )
                SELECT reservation_id,intent_id,symbol,side,amount,fee_reserve,status,created_at,released_at
                FROM binance_execution_risk_reservations_current
                """
            )
            connection.execute("DROP TABLE binance_execution_risk_reservations_current")
            connection.execute(
                "ALTER TABLE binance_execution_risk_reservations_legacy "
                "RENAME TO binance_execution_risk_reservations"
            )
            connection.commit()
            connection.close()

            second = BinanceExecutionService(path, venue=FakeVenue(), environment="PAPER")
            try:
                orders = {row["intent_id"]: row for row in second.orders()}
                self.assertEqual(
                    orders[partial["intent_id"]]["risk_reservation"]["reserved_quantity"],
                    "1.25",
                )
                self.assertEqual(
                    orders[no_fill["intent_id"]]["risk_reservation"]["reserved_quantity"],
                    "1",
                )
                self.assertEqual(
                    orders[buy["intent_id"]]["risk_reservation"]["reserved_quantity"],
                    "0",
                )
                self.assertEqual(
                    orders[terminal["intent_id"]]["risk_reservation"]["status"],
                    "RELEASED",
                )
                self.assertEqual(
                    orders[terminal["intent_id"]]["risk_reservation"]["reserved_quantity"],
                    "0",
                )

                second.enable_auto_canary(ENABLE_CONFIRMATION)
                blocked = second.submit_signal(
                    signal("legacy-duplicate-exit", intent="EXIT"),
                    price="10",
                    quantity="2",
                )
                self.assertEqual(blocked["state"], REJECTED)
                self.assertIn("INSUFFICIENT_OWNED_INVENTORY", blocked["reason"])
                before_reopen = {
                    row["intent_id"]: row["reserved_quantity"]
                    for row in second.store.connection.execute(
                        "SELECT intent_id,reserved_quantity "
                        "FROM binance_execution_risk_reservations "
                        "WHERE intent_id IN (?,?) ORDER BY intent_id",
                        (partial["intent_id"], no_fill["intent_id"]),
                    ).fetchall()
                }
            finally:
                second.close()

            third = BinanceExecutionService(path, venue=FakeVenue(), environment="PAPER")
            try:
                after_reopen = {
                    row["intent_id"]: row["reserved_quantity"]
                    for row in third.store.connection.execute(
                        "SELECT intent_id,reserved_quantity "
                        "FROM binance_execution_risk_reservations "
                        "WHERE intent_id IN (?,?) ORDER BY intent_id",
                        (partial["intent_id"], no_fill["intent_id"]),
                    ).fetchall()
                }
                self.assertEqual(after_reopen, before_reopen)
            finally:
                third.close()

    def test_filled_without_trade_rows_retains_reservation_until_trades_arrive(self):
        self.service.enable_auto_canary(ENABLE_CONFIRMATION)
        entry = self.service.submit_signal(signal(), price="10", quantity="1")
        self.venue.orders[entry["client_order_id"]] = {
            "status": "FILLED",
            "orderId": entry["exchange_order_id"],
            "executedQty": "1",
        }
        self.assertEqual(self.service.reconcile()["status"], "SUCCESS")
        order = self.service.orders()[0]
        self.assertEqual(order["state"], FILLED)
        self.assertEqual(order["risk_reservation"]["status"], "HELD")
        self.venue.trades[entry["exchange_order_id"]] = [
            {"tradeId": "filled-later", "qty": "1", "price": "10", "quoteQty": "10"}
        ]
        self.assertEqual(self.service.reconcile()["status"], "SUCCESS")
        self.assertEqual(self.service.orders()[0]["risk_reservation"]["status"], "RELEASED")

    def test_rejected_query_keeps_reservation_and_pauses_entries(self):
        self.service.enable_auto_canary(ENABLE_CONFIRMATION)
        self.service.submit_signal(signal(), price="10", quantity="1")
        self.venue.query_result = {"status": "REJECTED", "code": -2010}
        self.assertEqual(self.service.reconcile()["status"], "FAILURE")
        self.assertEqual(self.service.orders()[0]["state"], UNKNOWN)
        self.assertEqual(self.service.orders()[0]["risk_reservation"]["status"], "HELD")
        self.assertEqual(self.service.control()["state"], PAUSED)

    def test_rate_limited_cancel_keeps_reservation_and_pauses_entries(self):
        self.service.enable_auto_canary(ENABLE_CONFIRMATION)
        entry = self.service.submit_signal(signal(), price="10", quantity="1")
        self.venue.cancel_result = {"status": "RATE_LIMIT"}
        self.assertEqual(self.service.cancel(entry["client_order_id"], symbol="BTCUSDT")["state"], UNKNOWN)
        self.assertEqual(self.service.orders()[0]["risk_reservation"]["status"], "HELD")
        self.assertEqual(self.service.control()["state"], PAUSED)

    def test_cancel_race_ingests_partial_and_filled_payloads(self):
        self.service.enable_auto_canary(ENABLE_CONFIRMATION)
        partial = self.service.submit_signal(signal("partial"), price="10", quantity="1")
        self.venue.cancel_result = {
            "status": "CANCELED",
            "orderId": partial["exchange_order_id"],
            "executedQty": "0.25",
            "fills": [{"tradeId": "cancel-partial", "qty": "0.25", "price": "10", "quoteQty": "2.5"}],
        }
        canceled = self.service.cancel(partial["client_order_id"], symbol="BTCUSDT")
        self.assertEqual(canceled["state"], CANCELED)
        self.assertEqual(canceled["risk_reservation"]["status"], "RELEASED")

        filled = self.service.submit_signal(signal("filled"), price="10", quantity="1")
        self.venue.cancel_result = {
            "status": "FILLED",
            "orderId": filled["exchange_order_id"],
            "executedQty": "1",
            "fills": [{"tradeId": "cancel-filled", "qty": "1", "price": "10", "quoteQty": "10"}],
        }
        canceled = self.service.cancel(filled["client_order_id"], symbol="BTCUSDT")
        self.assertEqual(canceled["state"], FILLED)
        self.assertEqual(canceled["risk_reservation"]["status"], "RELEASED")

    def test_partial_terminal_observation_releases_remainder_and_preserves_fills(self):
        cases = (
            (CANCELED, "0.25", [{"tradeId": "partial-terminal", "qty": "0.25"}], "0.25", CANCELED),
            (EXPIRED, "0.25", [{"tradeId": "partial-expired", "qty": "0.25"}], "0.25", EXPIRED),
            (
                CANCELED,
                "1",
                [
                    {"tradeId": "partial-full", "qty": "0.25"},
                    {"tradeId": "remaining-full", "qty": "0.75"},
                ],
                "1",
                FILLED,
            ),
        )
        for index, (terminal, executed, payload_fills, expected_quantity, expected_state) in enumerate(cases):
            with self.subTest(terminal=terminal, executed=executed):
                with tempfile.TemporaryDirectory() as directory:
                    venue = FakeVenue()
                    service = BinanceExecutionService(
                        directory + "/execution.sqlite",
                        venue=venue,
                        environment="PAPER",
                    )
                    try:
                        service.update_account(
                            {"quote_available": "100", "owned_inventory": {}}
                        )
                        service.enable_auto_canary(ENABLE_CONFIRMATION)
                        entry = service.submit_signal(
                            signal("partial-terminal-" + str(index)),
                            price="10",
                            quantity="1",
                        )
                        initial_fill = {
                            **payload_fills[0],
                            "price": "10",
                            "quoteQty": "2.5",
                        }
                        service.record_fills(entry["intent_id"], [initial_fill])
                        self.assertEqual(
                            service.orders()[0]["state"],
                            PARTIALLY_FILLED,
                        )
                        venue.cancel_result = {
                            "status": terminal,
                            "orderId": entry["exchange_order_id"],
                            "executedQty": executed,
                            "fills": [
                                {
                                    **fill,
                                    "price": "10",
                                    "quoteQty": str(Decimal(fill["qty"]) * Decimal("10")),
                                }
                                for fill in payload_fills
                            ],
                        }
                        outcome = service.cancel(
                            entry["client_order_id"],
                            symbol="BTCUSDT",
                        )
                        self.assertEqual(outcome["state"], expected_state)
                        self.assertEqual(
                            outcome["risk_reservation"]["status"],
                            "RELEASED",
                        )
                        durable_fills = service.fills(
                            client_order_id=entry["client_order_id"]
                        )
                        self.assertEqual(
                            sum(
                                (Decimal(fill["quantity"]) for fill in durable_fills),
                                Decimal("0"),
                            ),
                            Decimal(expected_quantity),
                        )
                        self.assertEqual(
                            Decimal(service.position("BTCUSDT")["quantity"]),
                            Decimal(expected_quantity),
                        )
                    finally:
                        service.close()

    def test_venue_identity_is_required_and_profile_bound(self):
        with self.assertRaises(ValueError):
            BinanceExecutionService(":memory:", venue=object(), environment="PAPER")
        with self.assertRaises(ValueError):
            BinanceExecutionService(
                ":memory:",
                venue=_UntrustedVenue(environment="PAPER", origin="https://testnet.binance.vision"),
                environment="PAPER",
            )
        with self.assertRaises(ValueError):
            BinanceExecutionService(
                ":memory:",
                venue=_UntrustedVenue(environment="PAPER"),
                environment="BINANCE_SPOT_TESTNET",
            )
        with self.assertRaises(ValueError):
            BinanceExecutionService(
                ":memory:",
                venue=_UntrustedVenue(environment="BINANCE_SPOT_LIVE", origin="https://api.binance.com"),
                environment="BINANCE_SPOT_LIVE",
            )

    def test_signal_environment_mismatch_is_rejected_before_persistence(self):
        # Keep the venue trusted PAPER; only the signal environment is
        # intentionally mismatched.  The constructor must still enforce the
        # exact built-in venue boundary.
        with self.assertRaises(ValueError):
            self.service.submit_signal(
                {**signal(), "environment": "BINANCE_SPOT_TESTNET"},
                price="10",
                quantity="1",
            )
        persisted = self.store.connection.execute(
            "SELECT COUNT(*) FROM binance_execution_signals"
        ).fetchone()[0]
        self.assertEqual(persisted, 0)
        self.assertEqual(self.venue.submissions, [])
if __name__ == "__main__":
    unittest.main()
