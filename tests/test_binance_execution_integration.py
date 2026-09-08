from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
import sqlite3
import tempfile
import unittest

from axiom.binance_execution import (
    ACKNOWLEDGED,
    ARMED,
    ENABLE_CONFIRMATION,
    FILLED,
    ENABLE_TESTNET_CONFIRMATION,
    INTENT,
    KILLED,
    PAUSED,
    PARTIALLY_FILLED,
    REJECTED,
    RESERVED,
    SUBMITTING,
    UNKNOWN,
    BinanceExecutionService,
)
from axiom.binance_dev import PaperBinanceSpotVenue
from axiom.binance_risk import SymbolRules
from axiom.canary import CanaryService
from axiom.binance_spot import (
    BINANCE_SPOT_LIVE,
    BINANCE_SPOT_TESTNET,
    BinanceCredentialRef,
    BinanceSpotRESTClient,
    PAPER,
)
from axiom.storage import AxiomStore


UTC = timezone.utc
T0 = datetime(2026, 9, 9, 12, tzinfo=UTC)


def signal(
    signal_id: str,
    *,
    intent: str = "ENTRY",
    candidate_id: str = "candidate-1",
    symbol: str = "BTCUSDT",
    binding: dict[str, object] | None = None,
    provenance: dict[str, object] | None = None,
    strategy_ref: dict[str, object] | None = None,
) -> dict[str, object]:
    return {
        "signal_id": signal_id,
        "opportunity_id": "opportunity-" + signal_id,
        "candidate_id": candidate_id,
        "binding_hash": binding.get("binding_hash") if binding else None,
        "binding": binding,
        "provenance": provenance,
        "strategy_ref": strategy_ref,
        "symbol": symbol,
        "environment": PAPER,
        "decision_interval": "1h",
        "decision_at": T0.isoformat(),
        "intent": intent,
        "side": "BUY" if intent == "ENTRY" else "SELL",
        "reason": "integration fixture",
        "exit_policy": {"stop": "0.05"},
    }


def rules(*, min_notional: str = "10") -> SymbolRules:
    return SymbolRules(
        symbol="BTCUSDT",
        base_asset="BTC",
        status="TRADING",
        order_types=("LIMIT",),
        time_in_force=("IOC",),
        min_qty=Decimal("0.01"),
        step_size=Decimal("0.01"),
        min_notional=Decimal(min_notional),
    )


class BinanceExecutionIntegrationTests(unittest.TestCase):
    def _service(
        self,
        venue: PaperBinanceSpotVenue | None = None,
        *,
        account: dict[str, object] | None = None,
        **kwargs: object,
    ) -> tuple[AxiomStore, BinanceExecutionService, PaperBinanceSpotVenue]:
        store = AxiomStore(":memory:")
        selected = venue or PaperBinanceSpotVenue(fill_ratio="1")
        service = BinanceExecutionService(
            store,
            venue=selected,
            environment=PAPER,
            **kwargs,
        )
        service.update_account(account or {"quote_available": "100", "owned_inventory": {}})
        return store, service, selected

    def _close(self, store: AxiomStore, service: BinanceExecutionService) -> None:
        service.close()
        store.close()

    def test_paper_entry_exit_round_trip_is_a_compatible_binance_order_flow(self) -> None:
        store, service, venue = self._service()
        try:
            self.assertEqual(service.control()["state"], "DISABLED")
            service.enable_auto_canary(ENABLE_CONFIRMATION)

            entry = service.submit_signal(
                signal("entry"),
                price="10",
                quantity="1",
                time_in_force="IOC",
            )
            self.assertEqual(entry["state"], FILLED)
            self.assertEqual(len(venue.submissions), 1)
            self.assertEqual(venue.submissions[0]["symbol"], "BTCUSDT")
            self.assertEqual(venue.submissions[0]["side"], "BUY")
            self.assertEqual(venue.submissions[0]["time_in_force"], "IOC")
            self.assertEqual(
                venue.submissions[0]["new_client_order_id"],
                entry["client_order_id"],
            )
            self.assertEqual(service.position("BTCUSDT")["quantity"], "1")

            exit_order = service.submit_signal(
                signal("exit", intent="EXIT"),
                price="12",
                quantity="1",
                time_in_force="IOC",
            )
            self.assertEqual(exit_order["state"], FILLED)
            self.assertEqual(len(venue.submissions), 2)
            self.assertEqual(
                next(
                    row for row in service.orders()
                    if row["intent_id"] == entry["intent_id"]
                )["state"],
                FILLED,
            )
            transitions = [
                row[0]
                for row in store.connection.execute(
                    "SELECT to_state FROM binance_execution_order_transitions "
                    "WHERE intent_id=? ORDER BY transition_id",
                    (entry["intent_id"],),
                )
            ]
            self.assertEqual(transitions, [INTENT, RESERVED, SUBMITTING, FILLED])
        finally:
            self._close(store, service)

    def test_binance_namespace_coexists_without_mutating_polymarket_projection(self) -> None:
        store = AxiomStore(":memory:")
        poly = CanaryService(store)
        del poly
        try:
            before = store.connection.execute(
                "SELECT payload_json, readiness_snapshot_status, "
                "readiness_snapshot_reason FROM canary_readiness_snapshot "
                "WHERE singleton=1"
            ).fetchone()
            self.assertIsNotNone(before)

            venue = PaperBinanceSpotVenue(fill_ratio="1")
            service = BinanceExecutionService(store, venue=venue, environment=PAPER)
            try:
                service.update_account({"quote_available": "100", "owned_inventory": {}})
                service.enable_auto_canary(ENABLE_CONFIRMATION)
                self.assertEqual(
                    service.submit_signal(signal("isolated"), price="10", quantity="1")["state"],
                    FILLED,
                )

                after = store.connection.execute(
                    "SELECT payload_json, readiness_snapshot_status, "
                    "readiness_snapshot_reason FROM canary_readiness_snapshot "
                    "WHERE singleton=1"
                ).fetchone()
                self.assertEqual(tuple(after), tuple(before))
                self.assertEqual(
                    store.connection.execute("SELECT COUNT(*) FROM canary_ledger").fetchone()[0],
                    0,
                )
                self.assertEqual(
                    store.connection.execute(
                        "SELECT COUNT(*) FROM binance_execution_order_intents"
                    ).fetchone()[0],
                    1,
                )
            finally:
                service.close()
        finally:
            store.close()

    def test_intent_and_reservation_are_committed_before_external_submit(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = directory + "/execution.sqlite"
            observer = sqlite3.connect(path, timeout=0, check_same_thread=False)
            observer.execute(
                "CREATE TABLE observer_writes (marker TEXT NOT NULL)"
            )
            observer.commit()
            venue = PaperBinanceSpotVenue(fill_ratio="0")
            venue.empty_fill_status = "NEW"
            original_submit = venue.place_limit_order
            observed: list[tuple[str, str]] = []

            def submit(**kwargs: object):
                row = observer.execute(
                    "SELECT i.state, r.status "
                    "FROM binance_execution_order_intents AS i "
                    "JOIN binance_execution_risk_reservations AS r "
                    "ON r.intent_id=i.intent_id"
                ).fetchone()
                observed.append((str(row[0]), str(row[1])))
                observer.execute("BEGIN IMMEDIATE")
                observer.execute("INSERT INTO observer_writes VALUES ('committed')")
                observer.commit()
                return original_submit(**kwargs)

            venue.place_limit_order = submit
            store = AxiomStore(path)
            service = BinanceExecutionService(store, venue=venue, environment=PAPER)
            try:
                service.update_account({"quote_available": "100", "owned_inventory": {}})
                service.enable_auto_canary(ENABLE_CONFIRMATION)
                result = service.submit_signal(
                    signal("durable-before-submit"), price="10", quantity="1"
                )
                self.assertEqual(result["state"], ACKNOWLEDGED)
                self.assertEqual(observed, [(SUBMITTING, "HELD")])
                self.assertEqual(
                    observer.execute("SELECT marker FROM observer_writes").fetchone()[0],
                    "committed",
                )
            finally:
                service.close()
                store.close()
                observer.close()

    def test_ambiguous_submission_becomes_unknown_and_is_never_blindly_resubmitted(self) -> None:
        venue = PaperBinanceSpotVenue(fill_ratio="0")
        venue.empty_fill_status = "NEW"
        original_submit = venue.place_limit_order
        query_calls: list[dict[str, object]] = []
        original_query = venue.query_order

        def submit(**kwargs: object):
            original_submit(**kwargs)
            raise TimeoutError("response lost after submission")

        def query(**kwargs: object):
            query_calls.append(dict(kwargs))
            return original_query(**kwargs)

        venue.place_limit_order = submit
        venue.query_order = query
        store, service, _ = self._service(venue)
        try:
            service.enable_auto_canary(ENABLE_CONFIRMATION)
            first = service.submit_signal(signal("ambiguous"), price="10", quantity="1")
            self.assertEqual(first["state"], UNKNOWN)
            self.assertEqual(first["risk_reservation"]["status"], "HELD")

            repeated = service.submit_signal(signal("ambiguous"), price="10", quantity="1")
            self.assertEqual(repeated["intent_id"], first["intent_id"])
            self.assertEqual(len(venue.submissions), 1)

            self.assertEqual(service.reconcile()["status"], "SUCCESS")
            self.assertEqual(service.orders()[0]["state"], ACKNOWLEDGED)
            self.assertEqual(len(venue.submissions), 1)
            self.assertEqual(query_calls[0]["orig_client_order_id"], first["client_order_id"])
            self.assertNotIn("order_id", query_calls[0])
        finally:
            self._close(store, service)

    def test_reconciliation_uses_authoritative_order_and_trade_evidence(self) -> None:
        venue = PaperBinanceSpotVenue(fill_ratio="0")
        venue.empty_fill_status = "NEW"
        store, service, _ = self._service(venue)
        try:
            service.enable_auto_canary(ENABLE_CONFIRMATION)
            entry = service.submit_signal(signal("reconcile"), price="10", quantity="1")
            self.assertEqual(entry["state"], ACKNOWLEDGED)
            order_id = str(entry["exchange_order_id"])
            order = venue.orders[order_id]
            order.update(
                {
                    "status": "FILLED",
                    "executedQty": "1",
                    "cummulativeQuoteQty": "10",
                    "fills": [],
                }
            )
            venue.trades[order_id] = [
                {
                    "tradeId": "authoritative-trade",
                    "orderId": order_id,
                    "symbol": "BTCUSDT",
                    "side": "BUY",
                    "price": "10",
                    "qty": "1",
                    "quoteQty": "10",
                    "commission": "0",
                    "commissionAsset": "USDT",
                    "time": T0.isoformat(),
                }
            ]

            result = service.reconcile()
            self.assertEqual(result["status"], "SUCCESS")
            self.assertEqual(service.orders()[0]["state"], FILLED)
            self.assertEqual(service.orders()[0]["risk_reservation"]["status"], "RELEASED")
            self.assertEqual(service.position("BTCUSDT")["quantity"], "1")
            self.assertEqual(len(service.fills()), 1)
        finally:
            self._close(store, service)

    def test_partial_fill_fee_accounting_reduces_owned_base_and_tracks_quote_fee(self) -> None:
        venue = PaperBinanceSpotVenue(fill_ratio="0")
        venue.empty_fill_status = "NEW"
        store, service, _ = self._service(venue)
        try:
            service.enable_auto_canary(ENABLE_CONFIRMATION)
            entry = service.submit_signal(signal("partial-fee"), price="10", quantity="1")
            self.assertEqual(entry["state"], ACKNOWLEDGED)
            inserted = service.record_fills(
                entry["intent_id"],
                [
                    {
                        "tradeId": "partial-fee-trade",
                        "orderId": entry["exchange_order_id"],
                        "symbol": "BTCUSDT",
                        "side": "BUY",
                        "qty": "0.5",
                        "price": "10",
                        "quoteQty": "5",
                        "commission": "0.01",
                        "commissionAsset": "BTC",
                        "time": T0.isoformat(),
                    }
                ],
            )
            self.assertEqual(inserted, 1)
            self.assertEqual(service.orders()[0]["state"], PARTIALLY_FILLED)
            position = service.position("BTCUSDT")
            self.assertEqual(position["quantity"], "0.49")
            self.assertEqual(position["cost_basis"], "5")
            self.assertEqual(position["fees_quote"], "0.10")
            self.assertEqual(position["valuation_status"], "KNOWN")
            self.assertEqual(service.orders()[0]["risk_reservation"]["status"], "HELD")
        finally:
            self._close(store, service)

    def test_claimed_inventory_cannot_prove_exit_but_durable_origin_can(self) -> None:
        binding = {
            "candidate_id": "candidate-1",
            "symbol": "BTCUSDT",
            "environment": PAPER,
            "binding_hash": "binding-v1",
        }
        strategy_ref = {"strategy_id": "strategy-v1"}
        provenance = {
            "candidate_id": "candidate-1",
            "symbol": "BTCUSDT",
            "strategy_ref": strategy_ref,
        }

        def authorize(_: dict[str, object]) -> tuple[bool, str]:
            return True, "AUTHORIZED"

        venue = PaperBinanceSpotVenue(fill_ratio="1")
        store, service, _ = self._service(
            venue,
            account={
                "quote_available": "100",
                "owned_inventory": {"BTCUSDT": "1"},
            },
            entry_binding_authorizer=authorize,
            entry_policy_hash="BINANCE_CURRENT_QUALIFICATION_V1",
        )
        try:
            service.enable_auto_canary(ENABLE_CONFIRMATION)
            unproven_exit = service.submit_signal(
                signal(
                    "unproven-exit",
                    intent="EXIT",
                    binding=binding,
                    provenance=provenance,
                    strategy_ref=strategy_ref,
                ),
                price="10",
                quantity="1",
            )
            self.assertEqual(unproven_exit["state"], REJECTED)
            self.assertEqual(unproven_exit["reason"], "EXIT_ORIGIN_UNRESOLVED")
            self.assertEqual(venue.submissions, [])

            entry = service.submit_signal(
                signal(
                    "origin-entry",
                    binding=binding,
                    provenance=provenance,
                    strategy_ref=strategy_ref,
                ),
                price="10",
                quantity="1",
            )
            self.assertEqual(entry["state"], FILLED)
            self.assertEqual(service.position("BTCUSDT")["candidate_id"], "candidate-1")

            proven_exit = service.submit_signal(
                signal(
                    "proven-exit",
                    intent="EXIT",
                    binding=binding,
                    provenance=provenance,
                    strategy_ref=strategy_ref,
                ),
                price="12",
                quantity="1",
            )
            self.assertEqual(proven_exit["state"], FILLED)
            self.assertEqual([row["side"] for row in venue.submissions], ["BUY", "SELL"])
        finally:
            self._close(store, service)

    def test_exit_dust_and_oversell_are_rejected_without_venue_submission(self) -> None:
        venue = PaperBinanceSpotVenue(fill_ratio="1")
        store, service, _ = self._service(venue)
        try:
            service.enable_auto_canary(ENABLE_CONFIRMATION)
            entry = service.submit_signal(
                signal("owned"), price="10", quantity="1", rules=rules()
            )
            self.assertEqual(entry["state"], FILLED)
            submissions_before = len(venue.submissions)

            dust = service.submit_signal(
                signal("dust", intent="EXIT"),
                price="10",
                quantity="0.01",
                rules=rules(),
            )
            self.assertEqual(dust["state"], REJECTED)
            self.assertIn("EXIT_BELOW_MINIMUM", dust["reason"])
            self.assertEqual(len(venue.submissions), submissions_before)

            oversell = service.submit_signal(
                signal("oversell", intent="EXIT"),
                price="10",
                quantity="2",
                rules=rules(),
            )
            self.assertEqual(oversell["state"], REJECTED)
            self.assertIn("INSUFFICIENT_OWNED_INVENTORY", oversell["reason"])
            self.assertEqual(len(venue.submissions), submissions_before)
        finally:
            self._close(store, service)

    def test_pause_disarm_and_kill_never_submit_liquidation(self) -> None:
        venue = PaperBinanceSpotVenue(fill_ratio="1")
        store, service, _ = self._service(venue)
        try:
            service.enable_auto_canary(ENABLE_CONFIRMATION)
            entry = service.submit_signal(signal("held-position"), price="10", quantity="1")
            self.assertEqual(entry["state"], FILLED)
            self.assertEqual([row["side"] for row in venue.submissions], ["BUY"])

            self.assertEqual(service.pause("pause before operator review")["state"], PAUSED)
            self.assertEqual(service.disarm("disarm before operator review")["state"], "DISARMED")
            self.assertEqual(service.kill("kill before operator review")["state"], KILLED)
            self.assertEqual([row["side"] for row in venue.submissions], ["BUY"])

            self.assertEqual(service.position("BTCUSDT")["quantity"], "1")
        finally:
            self._close(store, service)
    def test_testnet_gate_is_strict_and_requires_its_distinct_confirmation(self) -> None:
        qualification = object()

        def authorize(_: dict[str, object]) -> tuple[bool, str]:
            return True, "AUTHORIZED"

        authorize._axiom_testnet_runtime_authorizer = True
        authorize._axiom_testnet_qualification = qualification
        credentials = {"api_key": "test-key", "api_secret": "test-secret"}
        opener_calls: list[object] = []

        def never_network(*args: object, **kwargs: object):
            opener_calls.append((args, kwargs))
            raise AssertionError("integration regressions must not use the network")

        venue = BinanceSpotRESTClient(
            BINANCE_SPOT_TESTNET,
            credentials,
            opener=never_network,
        )
        store = AxiomStore(":memory:")
        service = BinanceExecutionService(
            store,
            venue=venue,
            environment=BINANCE_SPOT_TESTNET,
            credentials=credentials,
            credential_ref=BinanceCredentialRef(
                instance="binance-testnet",
                environment=BINANCE_SPOT_TESTNET,
            ),
            qualification=qualification,
            entry_binding_authorizer=authorize,
            entry_policy_hash="BINANCE_TESTNET_CURRENT_QUALIFICATION_V1",
        )
        try:
            self.assertEqual(service.control()["state"], "DISABLED")
            with self.assertRaises(PermissionError):
                service.enable_auto_canary(ENABLE_CONFIRMATION)
            self.assertEqual(
                service.enable_auto_canary(ENABLE_TESTNET_CONFIRMATION)["state"],
                ARMED,
            )
            self.assertEqual(opener_calls, [])
        finally:
            service.close()
            store.close()

    def test_live_environment_is_refused_and_execution_is_opt_in(self) -> None:
        venue = PaperBinanceSpotVenue(fill_ratio="1")
        with self.assertRaises(ValueError):
            BinanceExecutionService(":memory:", venue=venue, environment=BINANCE_SPOT_LIVE)

        store, service, selected = self._service(venue)
        try:
            blocked = service.submit_signal(signal("not-armed"), price="10", quantity="1")
            self.assertEqual(blocked["state"], REJECTED)
            self.assertEqual(blocked["reason"], "CONTROL_DISABLED")
            self.assertEqual(selected.submissions, [])
            self.assertEqual(service.control()["state"], "DISABLED")
        finally:
            self._close(store, service)


if __name__ == "__main__":
    unittest.main()
