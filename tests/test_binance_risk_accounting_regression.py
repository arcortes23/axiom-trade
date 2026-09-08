from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
import sqlite3
import tempfile
import unittest
from pathlib import Path

from axiom.binance_execution import BinanceExecutionService
from axiom.binance_risk import BinanceRiskEnvelope, assess_entry
from axiom.binance_spot import (
    BINANCE_SPOT_TESTNET,
    BinanceCredentialRef,
    BinanceSpotRESTClient,
)


UTC = timezone.utc
T0 = datetime(2026, 1, 2, 12, tzinfo=UTC)


class BinanceRiskAccountingRegressionTests(unittest.TestCase):
    def _service(self, database: Path) -> BinanceExecutionService:
        credentials = {"api_key": "test-key", "api_secret": "test-secret"}
        qualification = object()

        def authorize(_: object) -> tuple[bool, str]:
            return True, "AUTHORIZED"

        authorize._axiom_testnet_runtime_authorizer = True
        authorize._axiom_testnet_qualification = qualification
        venue = BinanceSpotRESTClient(
            BINANCE_SPOT_TESTNET,
            credentials,
            opener=lambda *_args, **_kwargs: None,
        )
        return BinanceExecutionService(
            str(database),
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
            clock=lambda: T0,
        )

    @staticmethod
    def _insert_local_reservation(
        service: BinanceExecutionService,
        intent_id: str,
        side: str,
        amount: str,
        fee: str,
    ) -> None:
        conn = service._conn
        intent = "ENTRY" if side == "BUY" else "EXIT"
        conn.execute(
            """
            INSERT INTO binance_execution_order_intents(
                intent_id,signal_id,candidate_id,binding_hash,symbol,environment,
                intent,side,price,quantity,notional,fee_reserve,client_order_id,
                state,generation,updated_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                intent_id,
                "signal-" + intent_id,
                "candidate-" + intent_id,
                "binding",
                "BTCUSDT",
                BINANCE_SPOT_TESTNET,
                intent,
                side,
                amount,
                "1",
                amount,
                fee,
                "client-" + intent_id,
                "RESERVED",
                0,
                T0.isoformat(),
            ),
        )
        conn.execute(
            """
            INSERT INTO binance_execution_risk_reservations(
                reservation_id,intent_id,symbol,side,amount,reserved_quantity,
                fee_reserve,status,created_at
            ) VALUES(?,?,?,?,?,?,?,?,?)
            """,
            (
                "reservation-" + intent_id,
                intent_id,
                "BTCUSDT",
                side,
                amount,
                "1" if side == "SELL" else "0",
                fee,
                "HELD",
                T0.isoformat(),
            ),
        )

    @staticmethod
    def _create_peer_tables(conn: sqlite3.Connection) -> None:
        conn.executescript(
            """
            CREATE TABLE binance_testnet_probe_intents (
                intent_id TEXT PRIMARY KEY, probe_kind TEXT, symbol TEXT, side TEXT,
                quantity TEXT, notional TEXT, fee_reserve TEXT, state TEXT,
                reason TEXT, created_at_utc TEXT
            );
            CREATE TABLE binance_testnet_probe_reservations (
                intent_id TEXT, probe_kind TEXT, symbol TEXT, side TEXT,
                amount TEXT, fee_reserve TEXT, reserved_quantity TEXT, status TEXT
            );
            CREATE TABLE binance_testnet_probe_fills (
                intent_id TEXT, symbol TEXT, side TEXT, quantity TEXT, price TEXT,
                quote_quantity TEXT, commission TEXT, commission_asset TEXT,
                trade_time_utc TEXT, trade_id TEXT
            );
            CREATE TABLE binance_testnet_probe_events (
                intent_id TEXT, to_state TEXT, observed_at_utc TEXT
            );
            """
        )

    @staticmethod
    def _insert_peer_reservation(
        conn: sqlite3.Connection,
        intent_id: str,
        side: str,
        amount: str,
        fee: str,
    ) -> None:
        conn.execute(
            "INSERT INTO binance_testnet_probe_intents VALUES(?,?,?,?,?,?,?,?,?,?)",
            (
                intent_id,
                "TESTNET EXECUTION PROBE",
                "ETHUSDT",
                side,
                "1",
                amount,
                fee,
                "RESERVED",
                "",
                T0.isoformat(),
            ),
        )
        conn.execute(
            "INSERT INTO binance_testnet_probe_reservations VALUES(?,?,?,?,?,?,?,?)",
            (
                intent_id,
                "TESTNET EXECUTION PROBE",
                "ETHUSDT",
                side,
                amount,
                fee,
                "1" if side == "SELL" else "0",
                "HELD",
            ),
        )

    def test_local_and_peer_reservations_are_counted_once_and_sell_is_base_only(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            service = self._service(Path(directory) / "binance.sqlite")
            try:
                service.update_account({"quote_available": "100", "owned_inventory": {}})
                self._create_peer_tables(service._conn)
                self._insert_local_reservation(service, "local-buy", "BUY", "4", "0.4")
                self._insert_local_reservation(service, "local-sell", "SELL", "5", "0.5")
                self._insert_peer_reservation(service._conn, "peer-buy", "BUY", "3", "0.3")
                self._insert_peer_reservation(service._conn, "peer-sell", "SELL", "9", "0.9")
                service._conn.commit()

                snapshot = service._snapshot(T0)

                # Local BUY is represented by its pending order reservation;
                # peer BUY has no local pending row and is in reserved_exposure.
                # This partition keeps each BUY amount+fee in effective quote
                # buying power exactly once while SELL reserves only base.
                self.assertEqual(snapshot.aggregate_exposure, Decimal("7"))
                self.assertEqual(snapshot.reserved_exposure, Decimal("3.3"))
                self.assertEqual(snapshot.pending_reservation, Decimal("4.4"))
                self.assertEqual(snapshot.unresolved_exposure, Decimal("7.7"))
                self.assertEqual(
                    sum(order.reservation for order in snapshot.pending_orders),
                    Decimal("4.4"),
                )

                envelope = BinanceRiskEnvelope(
                    entry_notional="10",
                    max_aggregate_exposure="20",
                    max_reserved_exposure="8",
                )
                admission = assess_entry(
                    snapshot,
                    envelope,
                    requested_notional="0.3",
                    reserved_fee="0.03",
                    now=T0,
                )
                self.assertFalse(admission.allowed)
                self.assertIn("RESERVED_EXPOSURE", admission.reasons)
                self.assertEqual(admission.projected_reserved, Decimal("8.03"))
            finally:
                service.close()


if __name__ == "__main__":
    unittest.main()
