from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import os
import tempfile
import threading
import time
import unittest
from unittest.mock import Mock, patch
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from types import SimpleNamespace

from axiom import node as node_module
from axiom.autonomous import AutonomousResearchProcessor
from axiom.collector import _validated_scope_draft
from axiom.canary import (
    CanaryBlocked,
    CanaryService,
    CredentialStore,
    credential_fingerprint,
)
from axiom.canary_positions import RECOVERY_ACTION, RECOVERY_ATTACHED, RECOVERY_CONFIRMATION
from axiom.dashboard import DashboardData, DashboardServer, _DashboardHandler
from axiom.operator import (
    BOOTSTRAP_JOB_NAME,
    CANARY_CONNECTIVITY_CONFIG_KEY,
    DEFAULT_HERMES_JOB_ID,
    OperatorControlError,
    OperatorControlPlane,
    ROLLING_EXPLORATORY_PROPOSAL_CONFIG_KEY,
    ROLLING_EXPLORATORY_SCOPE_DRAFT_CONFIG_KEY,
    _loopback_host,
    _project_connectivity,
)
from axiom.forward import _operational_setup_hash
from axiom.ranker import CandidateCanaryRanker
from axiom.experiment_plan import normalize_market_scope
from axiom.market_scope import resolve_market_scope
from axiom.storage import AxiomStore
from axiom.rolling_portfolio import RollingAdmissionPolicy, RollingEvidence


CONNECTIVITY_SECRET_VALUES = (
    "PRIVATE-KEY-SENTINEL",
    "WALLET-ADDRESS-SENTINEL",
    "RAW-DIAGNOSTIC-SENTINEL",
    "SPENDER-IDENTIFIER-SENTINEL",
)

CONNECTIVITY_CREDENTIAL_VALUES = {
    "private_key": CONNECTIVITY_SECRET_VALUES[0],
    "wallet_address": CONNECTIVITY_SECRET_VALUES[1],
}
CONNECTIVITY_CREDENTIAL_FINGERPRINT = credential_fingerprint(
    CONNECTIVITY_CREDENTIAL_VALUES
)


def _configured_credentials() -> Mock:
    credentials = Mock()
    credentials.configured.return_value = True
    credentials.load.return_value = dict(CONNECTIVITY_CREDENTIAL_VALUES)
    return credentials

def _raw_connectivity_result(*, allowance_status: str = "OK") -> dict[str, object]:
    allowance: dict[str, object] = {
        "status": allowance_status,
        "available_base_units": "1000000" if allowance_status == "OK" else "1",
        "spender": CONNECTIVITY_SECRET_VALUES[3],
        "raw": CONNECTIVITY_SECRET_VALUES[2],
    }
    return {
        "ready": allowance_status == "OK",
        "credentials_configured": True,
        "message": "READY FOR CANARY CONNECTIVITY",
        "failures": [] if allowance_status == "OK" else ["CANARY_ALLOWANCE_INSUFFICIENT"],
        "diagnostics": {
            "sdk_version": "0.9.0",
            "credentials_configured": True,
            "authentication": {
                "status": "OK",
                "private_key": CONNECTIVITY_SECRET_VALUES[0],
            },
            "account": {
                "authenticated": True,
                "wallet_type": "EOA",
                "wallet_address": CONNECTIVITY_SECRET_VALUES[1],
                "credential_fingerprint": CONNECTIVITY_CREDENTIAL_FINGERPRINT,
            },
            "geoblock": {
                "blocked": False,
                "close_only": False,
                "country": "ZZ",
                "region": "T",
                "diagnostic": CONNECTIVITY_SECRET_VALUES[2],
            },
            "balance": {
                "status": "OK",
                "available_usd": "10",
                "private_key": CONNECTIVITY_SECRET_VALUES[0],
            },
            "allowance": allowance,
            "market": {"status": "SKIPPED", "raw": CONNECTIVITY_SECRET_VALUES[2]},
            "book": {"status": "SKIPPED", "raw": CONNECTIVITY_SECRET_VALUES[2]},
        },
        "live_execution": False,
        "connectivity_only": True,
    }


def _safe_connectivity_projection(
    *,
    ready: bool,
    allowance_status: str,
    checked_at: str = "2026-01-02T12:00:00+00:00",
    credentials_configured: bool = True,
) -> dict[str, object]:
    raw = _raw_connectivity_result(
        allowance_status=(
            "OK"
            if allowance_status in {"OK", "AVAILABLE", "SUFFICIENT"}
            else allowance_status
        )
    )
    raw["ready"] = bool(ready and credentials_configured)
    raw["checked_at"] = checked_at
    return _project_connectivity(
        raw,
        checked_at=checked_at,
        authoritative_credentials_configured=credentials_configured,
        authoritative_fingerprint=(
            CONNECTIVITY_CREDENTIAL_FINGERPRINT if credentials_configured else None
        ),
    )


class ConnectivityVenueSentinel:
    """Read-only venue fake whose execution methods fail loudly if touched."""

    def __init__(self, *, allowance_status: str = "OK") -> None:
        self.allowance_status = allowance_status
        self.order_calls = 0
        self.connectivity_calls = 0
        self.approval_calls = 0

    @staticmethod
    def installed_sdk_version() -> str:
        return "0.9.0"

    def geoblock(self) -> dict[str, object]:
        return {
            "blocked": False,
            "close_only": False,
            "country": "ZZ",
            "region": "T",
            "private_key": CONNECTIVITY_SECRET_VALUES[0],
        }

    def connectivity_check(self) -> bool:
        self.connectivity_calls += 1
        return True

    def account(self) -> dict[str, object]:
        return {
            "credential_fingerprint": CONNECTIVITY_CREDENTIAL_FINGERPRINT,
            "authenticated": True,
            "wallet_type": "EOA",
            "wallet_address": CONNECTIVITY_SECRET_VALUES[1],
        }

    @staticmethod
    def balance() -> Decimal:
        return Decimal("10")

    def allowance(self) -> dict[str, object]:
        return {
            "status": self.allowance_status,
            "available_base_units": "1000000" if self.allowance_status == "OK" else "1",
            "spender": CONNECTIVITY_SECRET_VALUES[3],
        }


    def market_context(self, market_id: str, token_id: str) -> dict[str, object]:
        return {
            "accepting_orders": True,
            "min_order_size": "1",
            "tick_size": "0.01",
            "bids": [{"price": "0.49", "size": "100"}],
            "asks": [{"price": "0.50", "size": "100"}],
            "allowance": self.allowance(),
            "raw": CONNECTIVITY_SECRET_VALUES[2],
        }

    def submit_limit_order(self, **_: object) -> None:
        self.order_calls += 1
        raise AssertionError("connectivity check attempted to submit an order")

    def create_limit_order(self, **_: object) -> None:
        self.order_calls += 1
        raise AssertionError("connectivity check attempted to create an order")

    def post_order(self, *_: object, **__: object) -> None:
        self.order_calls += 1
        raise AssertionError("connectivity check attempted to post an order")

    def approve(self, **_: object) -> None:
        self.approval_calls += 1
        raise AssertionError("connectivity check attempted an approval")
 
 
class ProposedReadinessVenue(ConnectivityVenueSentinel):
    """Read-only venue bound to the proposal's exact market/token identities."""

    def __init__(
        self,
        market_ids: tuple[str, ...] = ("MARKET-1",),
        *,
        detailed_books: bool = False,
    ) -> None:
        super().__init__()
        self.market_ids = frozenset(str(market_id) for market_id in market_ids)
        self.detailed_books = detailed_books
        self.market_context_calls: list[tuple[str, str]] = []

    def market_context(self, market_id: str, token_id: str) -> dict[str, object]:
        pair = (str(market_id), str(token_id))
        self.market_context_calls.append(pair)
        if pair[0] not in self.market_ids or pair[1] not in {
            f"{pair[0]}-YES",
            f"{pair[0]}-NO",
        }:
            raise AssertionError(f"unexpected proposed market/token binding: {pair!r}")
        outcome = "yes" if pair[1].endswith("-YES") else "no"
        bid_price = "0.001" if outcome == "yes" else "0.998"
        ask_price = "0.002" if outcome == "yes" else "0.999"
        if self.detailed_books:
            if outcome == "yes":
                bids = [
                    {
                        "price": bid_price,
                        "size": "100",
                        "level": 0,
                        "raw_level_detail": "bid-" + ("x" * 64),
                    }
                ]
                asks = [
                    {
                        "price": str(Decimal("0.041") - Decimal("0.001") * index),
                        "size": "100",
                        "level": index,
                        "raw_level_detail": "ask-" + ("x" * 64),
                    }
                    for index in range(40)
                ]
            else:
                bids = [
                    {
                        "price": str(Decimal("0.959") + Decimal("0.001") * index),
                        "size": "100",
                        "level": index,
                        "raw_level_detail": "bid-" + ("x" * 64),
                    }
                    for index in range(40)
                ]
                asks = [
                    {
                        "price": ask_price,
                        "size": "100",
                        "level": 0,
                        "raw_level_detail": "ask-" + ("x" * 64),
                    }
                ]
        else:
            bids = [{"price": bid_price, "size": "100"}]
            asks = [{"price": ask_price, "size": "100"}]
        return {
            "market_id": pair[0],
            "token_id": pair[1],
            "asset_id": pair[1],
            "position_id": pair[1],
            "market_version": "v2",
            "outcome": outcome,
            "outcome_index": 0 if outcome == "yes" else 1,
            "identity_bindings": [
                {
                    "index": 0 if outcome == "yes" else 1,
                    "outcome": outcome,
                    "token_id": pair[1],
                    "position_id": pair[1],
                }
            ],
            "accepting_orders": True,
            "min_order_size": "5",
            "tick_size": "0.001",
            "neg_risk": False,
            "bids": bids,
            "asks": asks,
            "fee_bps": "0",
            "allowance": self.allowance(),
        }

class RecoveryVenue:
    def __init__(self, order: dict[str, object]) -> None:
        self.order = dict(order)
        self.order_calls = 0
        self.trade_calls = 0
        self.submit_calls = 0

    def get_order(self, order_id: str) -> dict[str, object]:
        self.order_calls += 1
        return {**self.order, "order_id": order_id}

    def list_account_trades(self, order_id: str) -> list[dict[str, object]]:
        self.trade_calls += 1
        return [
            {
                "trade_id": "trade-recovery-1",
                "order_id": order_id,
                "side": "BUY",
                "token_id": "token-1",
                "asset_id": "position-1",
                "price": "0.50",
                "size": "2",
                "timestamp": "2026-01-02T12:00:01+00:00",
                "status": "CONFIRMED",
            }
        ]

    def submit_limit_order(self, **_: object) -> None:
        self.submit_calls += 1
        raise AssertionError("recovery must never submit")


class OperatorControlTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        # The release fixture is isolated by default; transport/fake-venue
        # coverage opts into the production profile explicitly.
        self._production_profile = patch.dict(
            os.environ,
            {"AXIOM_EXECUTION_PROFILE": "production"},
        )
        self._production_profile.start()
        self.addCleanup(self._production_profile.stop)
        self.db = str(Path(self.tempdir.name) / "operator.sqlite")
        self.store = AxiomStore(self.db)
        self.addCleanup(self.store.close)
        self.control = OperatorControlPlane(self.store, system_bootstrap_enabled=True)
        self.server = DashboardServer(
            port=0,
            data=DashboardData(store=self.store, control=self.control),
        ).start()
        self.addCleanup(self.server.stop)
    def _seed_recovery_entry(self) -> None:
        service = CanaryService(
            self.store,
            credentials=_configured_credentials(),
            clock=lambda: datetime(2026, 1, 2, 12, 0, 5, tzinfo=timezone.utc),
            initialize=True,
        )
        self.store.connection.execute(
            "INSERT OR REPLACE INTO canary_control("
            "singleton,state,candidate_id,venue,armed_at,expires_at,limits_json,"
            "integrity_hash,updated_at,control_generation,credential_fingerprint) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (
                1, "DISARMED", "candidate-1", "polymarket", None, None, "{}",
                "test-integrity", "2026-01-02T12:00:00+00:00", 1,
                CONNECTIVITY_CREDENTIAL_FINGERPRINT,
            ),
        )
        self.store.connection.execute(
            "INSERT INTO canary_signals("
            "signal_id,candidate_id,frozen_hash,strategy_hash,model_hash,config_hash,"
            "market_id,token_id,outcome,side,paper_expected_price,source_snapshot_id,"
            "source_timestamp,generated_at,expires_at,status,reason,evidence_json,updated_at)"
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "signal-1", "candidate-1", "frozen-1", "strategy-1", "model-1", "config-1",
                "market-1", "token-1", "yes", "BUY", "0.50", "snapshot-1",
                "2026-01-02T12:00:00+00:00", "2026-01-02T12:00:00+00:00",
                "2026-01-02T12:01:00+00:00", "UNKNOWN", None, "{}", "2026-01-02T12:00:00+00:00",
            ),
        )
        self.store.connection.execute(
            "INSERT INTO canary_ledger("
            "event_id,signal_id,timestamp,candidate_id,venue,market_id,token_id,side,"
            "requested_notional,paper_expected_price,max_price,submitted_quantity,"
            "exchange_order_id,status,evidence_json,control_generation)"
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "event-1", "signal-1", "2026-01-02T12:00:00+00:00", "candidate-1",
                "polymarket", "market-1", "token-1", "BUY", "1.00", "0.50", "0.51",
                "2", None, "UNKNOWN",
                json.dumps(
                    {
                        "control_generation": 1,
                        "control_state": "DISARMED",
                        "control_candidate": "candidate-1",
                        "control_expiry": None,
                        "signal_candidate_id": "candidate-1",
                        "signal_market_id": "market-1",
                        "signal_token_id": "token-1",
                        "resolved_asset_id": "position-1",
                        "market_version": "v2",
                        "outcome_index": 0,
                        "identity_bindings": [
                            {
                                "index": 0,
                                "outcome": "yes",
                                "token_id": "token-1",
                                "position_id": "position-1",
                            }
                        ],
                        "selected_token_id": "token-1",
                        "selected_position_id": "position-1",
                        "signal_side": "BUY",
                        "signal_frozen_hash": "frozen-1",
                        "signal_strategy_hash": "strategy-1",
                        "signal_model_hash": "model-1",
                        "signal_config_hash": "config-1",
                    }
                ),
                1,
            ),
        )
        self.store.connection.commit()
        try:
            service.publish_readiness_snapshot(reason="TEST_RECOVERY")
        except Exception:
            pass

    def test_unknown_entry_recovery_attaches_exact_id_and_reconciles_read_only(self) -> None:
        self._seed_recovery_entry()
        credentials = _configured_credentials()
        venue = RecoveryVenue(
            {
                "side": "BUY",
                "token_id": "token-1",
                "asset_id": "position-1",
                "market_id": "market-1",
                "price": "0.51",
                "original_size": "2",
                "status": "FILLED",
            }
        )
        with patch("axiom.operator.CredentialStore", return_value=credentials), patch(
            "axiom.operator.PolymarketClobV2Venue", return_value=venue
        ):
            response = self.control.execute(
                RECOVERY_ACTION,
                "event-1",
                confirm=RECOVERY_CONFIRMATION,
                payload={"event_id": "event-1", "signal_id": "signal-1", "exchange_order_id": "order-1"},
            )
        self.assertTrue(response["ok"])
        self.assertEqual(response["result"]["recovery"]["exchange_order_id"], "order-1")
        row = self.store.connection.execute(
            "SELECT status,exchange_order_id,evidence_json FROM canary_ledger WHERE event_id='event-1'"
        ).fetchone()
        self.assertEqual((row["status"], row["exchange_order_id"]), ("FILLED", "order-1"))
        recovery_evidence = json.loads(row["evidence_json"])
        self.assertEqual(
            recovery_evidence["trade_provenance"],
            [{
                "confirmed": True,
                "market_id": None,
                "order_id": "order-1",
                "owner": None,
                "price": "0.50",
                "quantity": "2",
                "side": "BUY",
                "status": "CONFIRMED",
                "timestamp": "2026-01-02T12:00:01+00:00",
                "token_id": "position-1",
                "signal_token_id": "token-1",
                "trade_id": "trade-recovery-1",
            }],
        )
        self.assertEqual((venue.order_calls, venue.trade_calls, venue.submit_calls), (1, 1, 0))
        audit = self.store.list_operator_actions(limit=1)[0]
        self.assertEqual(audit["action"], RECOVERY_ACTION)
        self.assertNotIn("private", json.dumps(audit, default=str).lower())

    def _recovery_service(self) -> CanaryService:
        return CanaryService(
            self.store,
            credentials=_configured_credentials(),
            clock=lambda: datetime(2026, 1, 2, 12, 0, 5, tzinfo=timezone.utc),
            initialize=False,
        )

    def _valid_recovery_venue(self) -> RecoveryVenue:
        return RecoveryVenue(
            {
                "side": "BUY",
                "token_id": "token-1",
                "asset_id": "position-1",
                "market_id": "market-1",
                "price": "0.51",
                "original_size": "2",
                "status": "FILLED",
            }
        )

    def test_recovery_rejects_wrong_v2_token_with_right_position(self) -> None:
        self._seed_recovery_entry()
        service = self._recovery_service()
        venue = self._valid_recovery_venue()
        venue.order["token_id"] = "wrong-token"
        with self.assertRaisesRegex(CanaryBlocked, "CANARY_RECOVERY_TOKEN_MISMATCH"):
            service.recover_entry_intent(
                "event-1",
                "order-1",
                signal_id="signal-1",
                venue=venue,
                confirmation=RECOVERY_CONFIRMATION,
            )
        row = self.store.connection.execute(
            "SELECT status,exchange_order_id FROM canary_ledger WHERE event_id='event-1'"
        ).fetchone()
        self.assertEqual((row["status"], row["exchange_order_id"]), ("UNKNOWN", None))

    def test_recovery_rejects_conflicting_v2_token_aliases(self) -> None:
        self._seed_recovery_entry()
        service = self._recovery_service()
        venue = self._valid_recovery_venue()
        venue.order["tokenId"] = "wrong-token"
        with self.assertRaisesRegex(CanaryBlocked, "CANARY_RECOVERY_TOKEN_MISMATCH"):
            service.recover_entry_intent(
                "event-1",
                "order-1",
                signal_id="signal-1",
                venue=venue,
                confirmation=RECOVERY_CONFIRMATION,
            )

    def test_recovery_passes_only_supported_trade_scope_kwargs(self) -> None:
        self._seed_recovery_entry()
        service = self._recovery_service()
        venue = self._valid_recovery_venue()
        observed_scope: dict[str, object] = {}

        def scoped_trades(
            order_id: str,
            *,
            asset_id: str | None = None,
            token_id: str | None = None,
            market: str | None = None,
        ) -> list[dict[str, object]]:
            observed_scope.update(
                {"asset_id": asset_id, "token_id": token_id, "market": market}
            )
            return RecoveryVenue.list_account_trades(venue, order_id)

        venue.list_account_trades = scoped_trades  # type: ignore[method-assign]
        service.recover_entry_intent(
            "event-1",
            "order-1",
            signal_id="signal-1",
            venue=venue,
            confirmation=RECOVERY_CONFIRMATION,
        )
        self.assertEqual(
            observed_scope,
            {"asset_id": "position-1", "token_id": "token-1", "market": "market-1"},
        )

    def test_recovery_rejects_lower_price_wrong_order_without_attachment(self) -> None:
        self._seed_recovery_entry()
        service = self._recovery_service()
        venue = RecoveryVenue(
            {
                "side": "BUY",
                "token_id": "token-1",
                "asset_id": "position-1",
                "market_id": "market-1",
                "price": "0.50",
                "original_size": "2",
                "status": "FILLED",
            }
        )
        with self.assertRaisesRegex(CanaryBlocked, "CANARY_RECOVERY_ORDER_BOUNDS_MISMATCH"):
            service.recover_entry_intent(
                "event-1", "order-1", signal_id="signal-1", venue=venue,
                confirmation=RECOVERY_CONFIRMATION,
            )
        row = self.store.connection.execute(
            "SELECT status,exchange_order_id FROM canary_ledger WHERE event_id='event-1'"
        ).fetchone()
        self.assertEqual((row["status"], row["exchange_order_id"]), ("UNKNOWN", None))

    def test_recovery_rejects_trade_from_different_order(self) -> None:
        self._seed_recovery_entry()
        service = self._recovery_service()
        venue = self._valid_recovery_venue()
        venue.list_account_trades = lambda order_id: [
            {
                "order_id": "different-order",
                "side": "BUY",
                "token_id": "token-1",
                "price": "0.50",
                "size": "2",
                "timestamp": "2026-01-02T12:00:01+00:00",
            }
        ]
        with self.assertRaisesRegex(CanaryBlocked, "CANARY_RECOVERY_TRADE_ORDER_ID_MISMATCH"):
            service.recover_entry_intent(
                "event-1", "order-1", signal_id="signal-1", venue=venue,
                confirmation=RECOVERY_CONFIRMATION,
            )
        row = self.store.connection.execute(
            "SELECT status,exchange_order_id FROM canary_ledger WHERE event_id='event-1'"
        ).fetchone()
        self.assertEqual((row["status"], row["exchange_order_id"]), ("UNKNOWN", None))

    def test_malformed_recovery_provenance_blocks_before_venue_reads(self) -> None:
        self._seed_recovery_entry()
        self.store.connection.execute(
            "UPDATE canary_ledger SET evidence_json='not-json' WHERE event_id='event-1'"
        )
        self.store.connection.commit()
        service = self._recovery_service()
        venue = self._valid_recovery_venue()
        with self.assertRaisesRegex(CanaryBlocked, "CANARY_RECOVERY_BINDING_INVALID"):
            service.recover_entry_intent(
                "event-1", "order-1", signal_id="signal-1", venue=venue,
                confirmation=RECOVERY_CONFIRMATION,
            )
        self.assertEqual((venue.order_calls, venue.trade_calls), (0, 0))

    def test_recovered_id_survives_late_submit_writer_and_records_attachment_state(self) -> None:
        self._seed_recovery_entry()
        service = self._recovery_service()
        service.recover_entry_intent(
            "event-1", "order-1", signal_id="signal-1",
            venue=self._valid_recovery_venue(), confirmation=RECOVERY_CONFIRMATION,
        )
        late = self.store.connection.execute(
            "UPDATE canary_ledger SET status='SUBMITTED',exchange_order_id=? "
            "WHERE event_id='event-1' AND status='SUBMITTING' AND exchange_order_id IS NULL",
            ("late-order",),
        )
        self.store.connection.commit()
        self.assertEqual(late.rowcount, 0)
        row = self.store.connection.execute(
            "SELECT status,exchange_order_id FROM canary_ledger WHERE event_id='event-1'"
        ).fetchone()
        self.assertEqual(row["exchange_order_id"], "order-1")
        event = self.store.connection.execute(
            "SELECT status FROM canary_execution_events "
            "WHERE execution_event_id='event-1-recovery'"
        ).fetchone()
        self.assertEqual(event["status"], RECOVERY_ATTACHED)
    def test_canceled_recovery_with_truncated_trade_history_stays_unknown(self) -> None:
        self._seed_recovery_entry()
        service = self._recovery_service()
        venue = self._valid_recovery_venue()
        venue.order["status"] = "CANCELED"
        venue.list_account_trades = lambda order_id: {  # type: ignore[method-assign]
            "trades": [
                {
                    "trade_id": "visible-before-truncation",
                    "order_id": order_id,
                    "side": "BUY",
                    "token_id": "position-1",
                    "price": "0.50",
                    "size": "1",
                    "status": "CONFIRMED",
                }
            ],
            "truncated": True,
        }
        with self.assertRaisesRegex(
            CanaryBlocked, "CANARY_TRADE_HISTORY_INCOMPLETE"
        ):
            service.recover_entry_intent(
                "event-1",
                "order-1",
                signal_id="signal-1",
                venue=venue,
                confirmation=RECOVERY_CONFIRMATION,
            )
        row = self.store.connection.execute(
            "SELECT status,exchange_order_id FROM canary_ledger WHERE event_id='event-1'"
        ).fetchone()
        self.assertIsNotNone(row)
        assert row is not None
        self.assertEqual((row["status"], row["exchange_order_id"]), ("UNKNOWN", None))
    def test_recovery_rejects_authenticated_sell_trade_without_mutation(self) -> None:
        self._seed_recovery_entry()
        service = self._recovery_service()
        venue = self._valid_recovery_venue()
        venue.list_account_trades = lambda order_id: [
            {
                "trade_id": "wrong-side-recovery",
                "order_id": order_id,
                "side": "SELL",
                "market_id": "market-1",
                "token_id": "token-1",
                "asset_id": "position-1",
                "price": "0.50",
                "size": "2",
                "timestamp": "2026-01-02T12:00:01+00:00",
                "status": "CONFIRMED",
            }
        ]
        with self.assertRaisesRegex(
            CanaryBlocked, "CANARY_RECOVERY_TRADE_SIDE_MISMATCH"
        ):
            service.recover_entry_intent(
                "event-1",
                "order-1",
                signal_id="signal-1",
                venue=venue,
                confirmation=RECOVERY_CONFIRMATION,
            )
        row = self.store.connection.execute(
            "SELECT status,exchange_order_id,fill_quantity FROM canary_ledger "
            "WHERE event_id='event-1'"
        ).fetchone()
        self.assertIsNotNone(row)
        assert row is not None
        self.assertEqual(
            (row["status"], row["exchange_order_id"], row["fill_quantity"]),
            ("UNKNOWN", None, None),
        )


    def test_unknown_entry_recovery_blocks_isolated_profile_before_credentials_or_venue(self) -> None:
        self._seed_recovery_entry()
        credentials = Mock()
        venue = Mock()
        isolated = OperatorControlPlane(self.store, profile={"environment": "ISOLATED"})
        with patch("axiom.operator.CredentialStore", return_value=credentials), patch(
            "axiom.operator.PolymarketClobV2Venue", return_value=venue
        ):
            response = isolated.execute(
                RECOVERY_ACTION,
                "event-1",
                confirm=RECOVERY_CONFIRMATION,
                payload={"event_id": "event-1", "signal_id": "signal-1", "exchange_order_id": "order-1"},
            )
        self.assertFalse(response["ok"])
        self.assertEqual(response["reason"], "RECOVERY_PRODUCTION_PROFILE_REQUIRED")
        credentials.assert_not_called()
        venue.assert_not_called()
        row = self.store.connection.execute(
            "SELECT status,exchange_order_id FROM canary_ledger WHERE event_id='event-1'"
        ).fetchone()
        self.assertEqual((row["status"], row["exchange_order_id"]), ("UNKNOWN", None))
    def test_isolated_operator_fences_production_canary_actions_before_transport(self) -> None:
        with patch.dict(os.environ, {"AXIOM_EXECUTION_PROFILE": "isolated"}):
            isolated = OperatorControlPlane(self.store)
            snapshot = isolated.risk_settings_snapshot()
            config_id = snapshot["config_id"]
            generation = snapshot["generation"]
            with patch("axiom.operator.CredentialStore") as credentials, patch(
                "axiom.operator.PolymarketClobV2Venue"
            ) as venue, patch("axiom.operator.CanaryService") as service:
                blocked = (
                    isolated.execute("canary.connectivity_check"),
                    isolated.execute(
                        "canary.arm",
                        "candidate-1",
                        confirm="ARM",
                    ),
                    isolated.execute(
                        "canary.enable_auto",
                        confirm=f"ENABLE AUTO CANARY POLYMARKET {config_id} {generation}",
                        payload={
                            "venue": "polymarket",
                            "config_id": config_id,
                            "expected_generation": generation,
                        },
                    ),
                    isolated.execute(
                        "canary.settings.activate_draft",
                        confirm="ACTIVATE RISK SETTINGS DRAFT",
                        payload={
                            "config_id": config_id,
                            "expected_generation": generation,
                            "actor": "isolated-operator",
                        },
                    ),
                )
            for response in blocked:
                self.assertFalse(response["ok"])
                self.assertEqual(response["reason"], "ISOLATED_EXECUTION_PROFILE")
            credentials.assert_not_called()
            venue.assert_not_called()
            service.assert_not_called()
            self.assertEqual(
                isolated.risk_settings_snapshot()["generation"],
                generation,
            )

    def test_isolated_operator_preserves_emergency_disarm_and_kill(self) -> None:
        with patch.dict(os.environ, {"AXIOM_EXECUTION_PROFILE": "isolated"}):
            isolated = OperatorControlPlane(self.store)
            disarmed = isolated.execute("canary.disarm", confirm="DISARM")
            killed = isolated.execute("canary.kill", confirm="KILL")
        self.assertTrue(disarmed["ok"])
        self.assertEqual(disarmed["result"]["canary"]["micro_live_canary"], "DISARMED")
        self.assertTrue(killed["ok"])
        self.assertEqual(killed["result"]["canary"]["micro_live_canary"], "KILLED")

    def test_malformed_operator_profile_isolation_fence_is_not_production(self) -> None:
        with patch.dict(os.environ, {"AXIOM_EXECUTION_PROFILE": "production-ish"}):
            isolated = OperatorControlPlane(self.store)
            with patch("axiom.operator.CredentialStore") as credentials:
                response = isolated.execute("canary.connectivity_check")
        self.assertFalse(response["ok"])
        self.assertEqual(response["reason"], "ISOLATED_EXECUTION_PROFILE")
        credentials.assert_not_called()


    def test_connectivity_control_persists_one_safe_success_projection(self) -> None:
        expected = _safe_connectivity_projection(
            ready=True,
            allowance_status="AVAILABLE",
        )
        raw_result = _raw_connectivity_result(allowance_status="OK")
        raw_result["checked_at"] = expected["checked_at"]
        service = Mock()
        service.connectivity_check.return_value = raw_result
        credentials = _configured_credentials()
        venue = ConnectivityVenueSentinel()
        with patch("axiom.operator.CredentialStore", return_value=credentials), patch(
            "axiom.operator.PolymarketClobV2Venue",
            return_value=venue,
        ), patch("axiom.operator.CanaryService", return_value=service), patch(
            "axiom.operator.utc_now",
            return_value=datetime(2026, 1, 2, 12, tzinfo=timezone.utc),
        ):
            response = self.control.execute("canary.connectivity_check")
        connectivity = response["result"]["connectivity"]

        self.assertFalse(response["live_execution"])
        self.assertTrue(response["ok"])
        self.assertEqual(connectivity["status"], "READY")
        self.assertTrue(connectivity["ready"])
        self.assertEqual(connectivity["failure_codes"], [])
        persisted = self.store.get_operator_config(CANARY_CONNECTIVITY_CONFIG_KEY)
        self.assertEqual(persisted["status"], "READY")
        self.assertTrue(persisted["ready"])
        self.assertEqual(persisted["failure_codes"], [])
        native = self.control.status()["connectivity"]
        self.assertEqual(native["status"], "READY")
        self.assertTrue(native["ready"])
        self.assertEqual(native["failure_codes"], [])
        service.submit.assert_not_called()
        service.submit_signal.assert_not_called()
        service.create_limit_order.assert_not_called()
        service.post_order.assert_not_called()
        service.approve.assert_not_called()
        service.enable_autonomous_micro_live.assert_not_called()
        self.assertEqual(venue.order_calls, 0)
        self.assertEqual(venue.approval_calls, 0)

        dashboard = DashboardData(store=self.store, control=self.control).canary_data()
        dashboard_connectivity = dashboard["connectivity"]
        self.assertEqual(dashboard_connectivity["status"], "READY")
        self.assertTrue(dashboard_connectivity["ready"])
        self.assertEqual(dashboard_connectivity["failure_codes"], [])
        encoded = json.dumps(
            {
                "response": response,
                "stored": self.store.get_operator_config(CANARY_CONNECTIVITY_CONFIG_KEY),
                "dashboard": dashboard,
            },
            default=str,
        )
        for secret in CONNECTIVITY_SECRET_VALUES:
            self.assertNotIn(secret, encoded)
        self.assertFalse(connectivity["live_execution"])

    def test_connectivity_control_runs_real_service_authenticated_read_only(self) -> None:
        credentials = _configured_credentials()
        venue = ConnectivityVenueSentinel()
        with patch("axiom.operator.CredentialStore", return_value=credentials), patch(
            "axiom.operator.PolymarketClobV2Venue",
            return_value=venue,
        ), patch(
            "axiom.operator.utc_now",
            return_value=datetime(2026, 1, 2, 12, tzinfo=timezone.utc),
        ):
            response = self.control.execute("canary.connectivity_check")
        connectivity = response["result"]["connectivity"]

        self.assertTrue(response["ok"])
        self.assertFalse(response["live_execution"])
        self.assertEqual(connectivity["status"], "READY")
        self.assertTrue(connectivity["ready"])
        self.assertEqual(connectivity["failure_codes"], [])
        persisted = self.store.get_operator_config(CANARY_CONNECTIVITY_CONFIG_KEY)
        self.assertEqual(persisted["status"], "READY")
        self.assertTrue(persisted["ready"])
        self.assertEqual(persisted["failure_codes"], [])
        native = self.control.status()["connectivity"]
        self.assertEqual(native["status"], "READY")
        self.assertTrue(native["ready"])
        self.assertEqual(native["failure_codes"], [])
        dashboard = DashboardData(store=self.store, control=self.control).canary_data()
        dashboard_connectivity = dashboard["connectivity"]
        self.assertEqual(dashboard_connectivity["status"], "READY")
        self.assertTrue(dashboard_connectivity["ready"])
        self.assertEqual(dashboard_connectivity["failure_codes"], [])
        encoded = json.dumps(
            {
                "response": response,
                "stored": self.store.get_operator_config(CANARY_CONNECTIVITY_CONFIG_KEY),
                "dashboard": dashboard,
            },
            default=str,
        )
        for secret in CONNECTIVITY_SECRET_VALUES:
            self.assertNotIn(secret, encoded)

        self.assertEqual(credentials.configured.call_count, 2)
        self.assertEqual(venue.connectivity_calls, 1)
        self.assertEqual(venue.order_calls, 0)
        self.assertEqual(venue.approval_calls, 0)

    def test_connectivity_control_real_service_blocks_without_credentials(self) -> None:
        credentials = Mock()
        credentials.configured.return_value = False
        with patch("axiom.operator.CredentialStore", return_value=credentials), patch(
            "axiom.operator.PolymarketClobV2Venue"
        ) as venue_factory, patch(
            "axiom.operator.utc_now",
            return_value=datetime(2026, 1, 2, 12, tzinfo=timezone.utc),
        ):
            response = self.control.execute("canary.connectivity_check")

        self.assertTrue(response["ok"])
        self.assertFalse(response["live_execution"])
        connectivity = response["result"]["connectivity"]
        self.assertFalse(connectivity["ready"])
        self.assertEqual(connectivity["status"], "BLOCKED")
        self.assertEqual(connectivity["credentials"], {"status": "NOT CONFIGURED"})
        self.assertEqual(connectivity["authentication"], {"status": "SKIPPED"})
        self.assertEqual(
            connectivity["account"],
            {
                "status": "SKIPPED",
                "wallet_type": None,
                "credential_fingerprint": None,
            },
        )
        self.assertEqual(connectivity["balance"], {"status": "SKIPPED", "available_usd": None})
        self.assertEqual(connectivity["failure_codes"], ["CREDENTIALS_NOT_CONFIGURED"])
        self.assertEqual(
            connectivity["failure_reasons"],
            [
                {
                    "code": "CREDENTIALS_NOT_CONFIGURED",
                    "reason": "Polymarket credentials are not configured.",
                }
            ],
        )
        self.assertEqual(
            self.store.get_operator_config(CANARY_CONNECTIVITY_CONFIG_KEY),
            connectivity,
        )
        venue_factory.assert_not_called()
        credentials.configured.assert_called_with(allow_environment=False)


    def test_connectivity_blocked_allowance_has_exact_readable_failure(self) -> None:
        expected = _safe_connectivity_projection(
            ready=False,
            allowance_status="INSUFFICIENT",
        )
        raw_result = _raw_connectivity_result(allowance_status="INSUFFICIENT")
        raw_result["checked_at"] = expected["checked_at"]
        service = Mock()
        service.connectivity_check.return_value = raw_result
        credentials = _configured_credentials()
        venue = ConnectivityVenueSentinel(allowance_status="INSUFFICIENT")
        with patch("axiom.operator.CredentialStore", return_value=credentials), patch(
            "axiom.operator.PolymarketClobV2Venue",
            return_value=venue,
        ), patch("axiom.operator.CanaryService", return_value=service), patch(
            "axiom.operator.utc_now",
            return_value=datetime(2026, 1, 2, 12, tzinfo=timezone.utc),
        ):
            response = self.control.execute("canary.connectivity_check")

        self.assertTrue(response["ok"])
        self.assertFalse(response["live_execution"])
        connectivity = response["result"]["connectivity"]
        self.assertFalse(connectivity["ready"])
        self.assertEqual(connectivity["status"], "BLOCKED")
        self.assertEqual(connectivity["allowance"], {"status": "INSUFFICIENT"})
        self.assertEqual(connectivity["failure_codes"], ["CANARY_ALLOWANCE_INSUFFICIENT"])
        persisted = self.store.get_operator_config(CANARY_CONNECTIVITY_CONFIG_KEY)
        self.assertEqual(persisted["status"], "BLOCKED")
        self.assertFalse(persisted["ready"])
        self.assertEqual(persisted["failure_codes"], ["CANARY_ALLOWANCE_INSUFFICIENT"])
        native = self.control.status()["connectivity"]
        self.assertEqual(native["status"], "BLOCKED")
        self.assertFalse(native["ready"])
        self.assertEqual(native["failure_codes"], ["CANARY_ALLOWANCE_INSUFFICIENT"])
        self.assertEqual(venue.order_calls, 0)
        self.assertEqual(venue.approval_calls, 0)
        service.connectivity_check.assert_called_once_with(
            venue=venue,
            allow_environment=False,
        )
        service.submit.assert_not_called()
        service.submit_signal.assert_not_called()
        service.create_limit_order.assert_not_called()
        service.post_order.assert_not_called()
        service.approve.assert_not_called()

    def test_connectivity_unknown_failure_is_collapsed_without_echoing_source(self) -> None:
        opaque_failure = "upstream exploded: PRIVATE-KEY-SENTINEL"
        raw_result = _raw_connectivity_result()
        raw_result["failures"] = [opaque_failure]
        credentials = _configured_credentials()
        service = Mock()
        service.connectivity_check.return_value = raw_result
        venue = ConnectivityVenueSentinel()
        with patch("axiom.operator.CredentialStore", return_value=credentials), patch(
            "axiom.operator.PolymarketClobV2Venue",
            return_value=venue,
        ), patch("axiom.operator.CanaryService", return_value=service), patch(
            "axiom.operator.utc_now",
            return_value=datetime(2026, 1, 2, 12, tzinfo=timezone.utc),
        ):
            response = self.control.execute("canary.connectivity_check")

        self.assertTrue(response["ok"])
        connectivity = response["result"]["connectivity"]
        self.assertEqual(connectivity["status"], "BLOCKED")
        self.assertEqual(connectivity["failure_codes"], ["CONNECTIVITY_CHECK_FAILED"])
        self.assertEqual(
            connectivity["failure_reasons"],
            [{"code": "CONNECTIVITY_CHECK_FAILED", "reason": "Connectivity check failed."}],
        )
        encoded = json.dumps(
            {
                "response": response,
                "stored": self.store.get_operator_config(CANARY_CONNECTIVITY_CONFIG_KEY),
                "actions": self.store.list_operator_actions(),
            },
            default=str,
        )
        self.assertNotIn(opaque_failure, encoded)
        self.assertNotIn("PRIVATE-KEY-SENTINEL", encoded)

    def test_connectivity_dual_failure_fields_include_legacy_unknown_without_echo(self) -> None:
        opaque_failure = "legacy failure: PRIVATE-KEY-SENTINEL"
        raw_result = _raw_connectivity_result()
        raw_result["failure_codes"] = []
        raw_result["failures"] = [opaque_failure]
        credentials = _configured_credentials()
        service = Mock()
        service.connectivity_check.return_value = raw_result
        venue = ConnectivityVenueSentinel()
        with patch("axiom.operator.CredentialStore", return_value=credentials), patch(
            "axiom.operator.PolymarketClobV2Venue",
            return_value=venue,
        ), patch("axiom.operator.CanaryService", return_value=service), patch(
            "axiom.operator.utc_now",
            return_value=datetime(2026, 1, 2, 12, tzinfo=timezone.utc),
        ):
            response = self.control.execute("canary.connectivity_check")

        self.assertTrue(response["ok"])
        connectivity = response["result"]["connectivity"]
        self.assertFalse(connectivity["ready"])
        self.assertEqual(connectivity["status"], "BLOCKED")
        self.assertEqual(connectivity["failure_codes"], ["CONNECTIVITY_CHECK_FAILED"])
        encoded = json.dumps(
            {
                "response": response,
                "stored": self.store.get_operator_config(CANARY_CONNECTIVITY_CONFIG_KEY),
                "actions": self.store.list_operator_actions(),
            },
            default=str,
        )
        self.assertNotIn(opaque_failure, encoded)
        self.assertNotIn("PRIVATE-KEY-SENTINEL", encoded)
    def test_connectivity_raw_ready_with_failed_account_is_blocked(self) -> None:
        raw_result = _raw_connectivity_result()
        raw_result["failure_codes"] = []
        raw_result["failures"] = []
        account = raw_result["diagnostics"]["account"]
        account["authenticated"] = False
        account["status"] = "FAIL"
        credentials = _configured_credentials()
        service = Mock()
        service.connectivity_check.return_value = raw_result
        venue = ConnectivityVenueSentinel()
        with patch("axiom.operator.CredentialStore", return_value=credentials), patch(
            "axiom.operator.PolymarketClobV2Venue",
            return_value=venue,
        ), patch("axiom.operator.CanaryService", return_value=service), patch(
            "axiom.operator.utc_now",
            return_value=datetime(2026, 1, 2, 12, tzinfo=timezone.utc),
        ):
            response = self.control.execute("canary.connectivity_check")

        self.assertTrue(response["ok"])
        connectivity = response["result"]["connectivity"]
        self.assertFalse(connectivity["ready"])
        self.assertEqual(connectivity["status"], "BLOCKED")
        self.assertEqual(connectivity["account"]["status"], "FAIL")
        self.assertEqual(connectivity["failure_codes"], ["ACCOUNT_CHECK_FAILED"])
        persisted = self.store.get_operator_config(CANARY_CONNECTIVITY_CONFIG_KEY)
        self.assertEqual(persisted["status"], "BLOCKED")
        self.assertFalse(persisted["ready"])
        self.assertEqual(persisted["failure_codes"], ["ACCOUNT_CHECK_FAILED"])
        native = self.control.status()["connectivity"]
        self.assertEqual(native["status"], "BLOCKED")
        self.assertFalse(native["ready"])
        self.assertEqual(native["failure_codes"], ["ACCOUNT_CHECK_FAILED"])
        service.connectivity_check.assert_called_once_with(
            venue=venue,
            allow_environment=False,
        )
        service.enable_autonomous_micro_live.assert_not_called()
        self.assertEqual(venue.order_calls, 0)
        self.assertEqual(venue.approval_calls, 0)



    def test_connectivity_failure_codes_cap_reserves_unknown_fallback(self) -> None:
        opaque_failure = "legacy unknown: WALLET-ADDRESS-SENTINEL"
        known_codes = [
            "CREDENTIALS_NOT_CONFIGURED",
            "VENUE_REQUIRED",
            "OFFICIAL_POLYMARKET_SDK_NOT_INSTALLED",
            "UNSUPPORTED_POLYMARKET_SDK",
            "OFFICIAL_POLYMARKET_SDK_NOT_READONLY_COMPATIBLE",
            "GEOGRAPHICALLY_BLOCKED",
            "GEOBLOCK_CHECK_FAILED",
            "AUTHENTICATED_CONNECTIVITY_FAILED",
            "ACCOUNT_CHECK_FAILED",
            "BALANCE_CHECK_FAILED",
            "BALANCE_RESPONSE_INVALID",
            "INSUFFICIENT_BALANCE",
            "CANARY_ALLOWANCE_UNAVAILABLE",
            "CANARY_ALLOWANCE_INSUFFICIENT",
            "CANARY_SPENDER_UNAVAILABLE",
            "MARKET_CONNECTIVITY_FAILED",
            "CONNECTIVITY_CHECK_FAILED",
        ]
        raw_result = _raw_connectivity_result()
        raw_result["failure_codes"] = known_codes
        raw_result["failures"] = [opaque_failure]
        credentials = _configured_credentials()
        service = Mock()
        service.connectivity_check.return_value = raw_result
        venue = ConnectivityVenueSentinel()
        with patch("axiom.operator.CredentialStore", return_value=credentials), patch(
            "axiom.operator.PolymarketClobV2Venue",
            return_value=venue,
        ), patch("axiom.operator.CanaryService", return_value=service), patch(
            "axiom.operator.utc_now",
            return_value=datetime(2026, 1, 2, 12, tzinfo=timezone.utc),
        ):
            response = self.control.execute("canary.connectivity_check")

        self.assertTrue(response["ok"])
        connectivity = response["result"]["connectivity"]
        self.assertFalse(connectivity["ready"])
        self.assertEqual(connectivity["status"], "BLOCKED")
        failure_codes = connectivity["failure_codes"]
        self.assertEqual(len(failure_codes), 16)
        self.assertIn("CONNECTIVITY_CHECK_FAILED", failure_codes)
        self.assertNotIn(opaque_failure, failure_codes)
        encoded = json.dumps(
            {
                "response": response,
                "stored": self.store.get_operator_config(CANARY_CONNECTIVITY_CONFIG_KEY),
                "actions": self.store.list_operator_actions(),
            },
            default=str,
        )
        self.assertNotIn(opaque_failure, encoded)
        self.assertNotIn("WALLET-ADDRESS-SENTINEL", encoded)
    def test_concurrent_connectivity_checks_serialize_and_keep_newer_result(self) -> None:
        first_started = threading.Event()
        release_first = threading.Event()
        second_attempted = threading.Event()
        second_started = threading.Event()
        responses: dict[str, dict[str, object]] = {}

        first_raw = _raw_connectivity_result()
        second_raw = _raw_connectivity_result(allowance_status="INSUFFICIENT")
        first_service = Mock()
        second_service = Mock()

        def delayed_first_check(**_: object) -> dict[str, object]:
            first_started.set()
            if not release_first.wait(2):
                raise AssertionError("first connectivity check was not released")
            return first_raw

        def second_check(**_: object) -> dict[str, object]:
            second_started.set()
            return second_raw

        first_service.connectivity_check.side_effect = delayed_first_check
        second_service.connectivity_check.side_effect = second_check
        credentials = _configured_credentials()
        venue = ConnectivityVenueSentinel()

        def run(name: str) -> None:
            responses[name] = self.control.execute(
                "canary.connectivity_check",
                target=f"fixture-{name}",
            )
        with patch("axiom.operator.CredentialStore", return_value=credentials), patch(
            "axiom.operator.PolymarketClobV2Venue",
            return_value=venue,
        ), patch(
            "axiom.operator.CanaryService",
            side_effect=[first_service, second_service],
        ):
            first_thread = threading.Thread(target=run, args=("first",), daemon=True)
            second_thread = threading.Thread(
                target=lambda: (second_attempted.set(), run("second")),
                daemon=True,
            )
            try:
                first_thread.start()
                self.assertTrue(first_started.wait(1))
                second_thread.start()
                self.assertTrue(second_attempted.wait(1))
                self.assertFalse(second_started.wait(0.2))
            finally:
                release_first.set()
                first_thread.join(2)
                second_thread.join(2)

        self.assertFalse(first_thread.is_alive())
        self.assertFalse(second_thread.is_alive())
        self.assertTrue(second_started.is_set())
        self.assertTrue(responses["first"]["ok"])
        self.assertTrue(responses["second"]["ok"])
        persisted = self.store.get_operator_config(CANARY_CONNECTIVITY_CONFIG_KEY)
        self.assertEqual(persisted["status"], "BLOCKED")
        self.assertFalse(persisted["ready"])
        self.assertEqual(persisted["failure_codes"], ["CANARY_ALLOWANCE_INSUFFICIENT"])
        native = self.control.status()["connectivity"]
        self.assertEqual(native["status"], "BLOCKED")
        self.assertFalse(native["ready"])
        self.assertEqual(native["failure_codes"], ["CANARY_ALLOWANCE_INSUFFICIENT"])
        self.assertEqual(
            responses["second"]["result"]["connectivity"]["failure_codes"],
            ["CANARY_ALLOWANCE_INSUFFICIENT"],
        )
        first_service.connectivity_check.assert_called_once_with(
            venue=venue,
            allow_environment=False,
        )
        second_service.connectivity_check.assert_called_once_with(
            venue=venue,
            allow_environment=False,
        )
        self.assertEqual(venue.order_calls, 0)
        self.assertEqual(venue.approval_calls, 0)



    def test_enable_waits_for_inflight_blocked_connectivity_check(self) -> None:
        connectivity_started = threading.Event()
        release_connectivity = threading.Event()
        enable_attempted = threading.Event()
        readiness_read = threading.Event()
        results: dict[str, dict[str, object]] = {}

        self.store.set_operator_config(
            CANARY_CONNECTIVITY_CONFIG_KEY,
            _safe_connectivity_projection(
                ready=True,
                allowance_status="AVAILABLE",
            ),
        )
        blocked_raw = _raw_connectivity_result(allowance_status="INSUFFICIENT")
        blocked_service = Mock()
        transition_service = Mock()
        settings_snapshot = self.control.risk_settings_snapshot()
        active_config_id = settings_snapshot["config_id"]
        active_generation = settings_snapshot["generation"]

        def delayed_blocked_check(**_: object) -> dict[str, object]:
            connectivity_started.set()
            if not release_connectivity.wait(2):
                raise AssertionError("blocked connectivity check was not released")
            return blocked_raw

        blocked_service.connectivity_check.side_effect = delayed_blocked_check
        credentials = _configured_credentials()
        venue = ConnectivityVenueSentinel(allowance_status="INSUFFICIENT")
        original_get = self.store.get_operator_config

        def observed_get(key: str, default: object = None) -> object:
            if key == CANARY_CONNECTIVITY_CONFIG_KEY:
                readiness_read.set()
            return original_get(key, default)

        def run_connectivity() -> None:
            results["connectivity"] = self.control.execute("canary.connectivity_check")

        def run_enable() -> None:
            enable_attempted.set()
            results["enable"] = self.control.execute(
                "canary.enable_auto",
                confirm=f"ENABLE AUTO CANARY POLYMARKET {active_config_id} {active_generation}",
                payload={
                    "venue": "polymarket",
                    "config_id": active_config_id,
                    "expected_generation": active_generation,
                },
            )

        with patch.object(self.store, "get_operator_config", side_effect=observed_get), patch(
            "axiom.operator.CredentialStore",
            return_value=credentials,
        ), patch(
            "axiom.operator.PolymarketClobV2Venue",
            return_value=venue,
        ), patch(
            "axiom.operator.CanaryService",
            side_effect=[blocked_service, transition_service],
        ):
            connectivity_thread = threading.Thread(target=run_connectivity, daemon=True)
            enable_thread = threading.Thread(target=run_enable, daemon=True)
            try:
                connectivity_thread.start()
                self.assertTrue(connectivity_started.wait(1))
                enable_thread.start()
                self.assertTrue(enable_attempted.wait(1))
                self.assertFalse(readiness_read.wait(0.2))
            finally:
                release_connectivity.set()
                connectivity_thread.join(2)
                enable_thread.join(2)

        self.assertFalse(connectivity_thread.is_alive())
        self.assertFalse(enable_thread.is_alive())
        self.assertTrue(results["connectivity"]["ok"])
        self.assertNotEqual(results["enable"].get("ok"), True)
        self.assertEqual(results["enable"]["reason"], "CANARY_ALLOWANCE_INSUFFICIENT")
        blocked_service.connectivity_check.assert_called_once_with(
            venue=venue,
            allow_environment=False,
        )
        transition_service.enable_autonomous_micro_live.assert_not_called()
        self.assertEqual(venue.order_calls, 0)
        self.assertEqual(venue.approval_calls, 0)

        persisted = self.store.get_operator_config(CANARY_CONNECTIVITY_CONFIG_KEY)
        self.assertEqual(persisted["status"], "BLOCKED")
        self.assertEqual(persisted["failure_codes"], ["CANARY_ALLOWANCE_INSUFFICIENT"])





    def test_connectivity_exponent_balance_is_null_and_failed_without_expansion(self) -> None:
        raw_result = _raw_connectivity_result()
        raw_result["failures"] = ["BALANCE_RESPONSE_INVALID"]
        raw_result["diagnostics"]["balance"]["status"] = "FAILED"
        raw_result["diagnostics"]["balance"]["available_usd"] = "1e2"
        credentials = _configured_credentials()
        service = Mock()
        service.connectivity_check.return_value = raw_result

        venue = ConnectivityVenueSentinel()
        with patch("axiom.operator.CredentialStore", return_value=credentials), patch(
            "axiom.operator.PolymarketClobV2Venue",
            return_value=venue,
        ), patch("axiom.operator.CanaryService", return_value=service), patch(
            "axiom.operator.utc_now",
            return_value=datetime(2026, 1, 2, 12, tzinfo=timezone.utc),
        ):
            response = self.control.execute("canary.connectivity_check")

        self.assertTrue(response["ok"])
        connectivity = response["result"]["connectivity"]
        self.assertEqual(connectivity["status"], "BLOCKED")
        self.assertEqual(connectivity["balance"], {"status": "FAIL", "available_usd": None})
        self.assertEqual(connectivity["failure_codes"], ["BALANCE_RESPONSE_INVALID"])

    def test_connectivity_exception_replaces_prior_ready_with_safe_block(self) -> None:
        prior = _safe_connectivity_projection(ready=True, allowance_status="AVAILABLE")
        self.store.set_operator_config(CANARY_CONNECTIVITY_CONFIG_KEY, prior)
        exception_secret = "VENUE-EXCEPTION-PRIVATE-KEY-SENTINEL"
        credentials = _configured_credentials()
        service = Mock()
        service.connectivity_check.side_effect = RuntimeError(exception_secret)
        venue = ConnectivityVenueSentinel()
        with patch("axiom.operator.CredentialStore", return_value=credentials), patch(
            "axiom.operator.PolymarketClobV2Venue",
            return_value=venue,
        ), patch("axiom.operator.CanaryService", return_value=service), patch(
            "axiom.operator.utc_now",
            return_value=datetime(2026, 1, 2, 12, tzinfo=timezone.utc),
        ):
            response = self.control.execute("canary.connectivity_check")

        self.assertTrue(response["ok"])
        self.assertEqual(response["action"], "canary.connectivity_check")
        connectivity = response["result"]["connectivity"]
        self.assertFalse(connectivity["ready"])
        self.assertEqual(connectivity["status"], "BLOCKED")
        self.assertEqual(connectivity["failure_codes"], ["CONNECTIVITY_CHECK_FAILED"])
        self.assertEqual(
            self.store.get_operator_config(CANARY_CONNECTIVITY_CONFIG_KEY),
            connectivity,
        )
        self.assertNotEqual(connectivity, prior)
        encoded = json.dumps(
            {
                "response": response,
                "stored": self.store.get_operator_config(CANARY_CONNECTIVITY_CONFIG_KEY),
                "actions": self.store.list_operator_actions(),
            },
            default=str,
        )
        self.assertNotIn(exception_secret, encoded)

    def test_connectivity_constructor_exception_replaces_prior_ready_with_safe_block(self) -> None:
        prior = _safe_connectivity_projection(ready=True, allowance_status="AVAILABLE")
        self.store.set_operator_config(CANARY_CONNECTIVITY_CONFIG_KEY, prior)
        exception_secret = "CONNECTIVITY-CONSTRUCTOR-PRIVATE-KEY-SENTINEL"
        credentials = _configured_credentials()
        with patch("axiom.operator.CredentialStore", return_value=credentials), patch(
            "axiom.operator.PolymarketClobV2Venue",
            side_effect=RuntimeError(exception_secret),
        ), patch("axiom.operator.utc_now", return_value=datetime(2026, 1, 2, 12, tzinfo=timezone.utc)):
            response = self.control.execute("canary.connectivity_check")

        self.assertTrue(response["ok"])
        connectivity = response["result"]["connectivity"]
        self.assertFalse(connectivity["ready"])
        self.assertEqual(connectivity["status"], "BLOCKED")
        self.assertEqual(connectivity["failure_codes"], ["CONNECTIVITY_CHECK_FAILED"])
        self.assertEqual(
            self.store.get_operator_config(CANARY_CONNECTIVITY_CONFIG_KEY),
            connectivity,
        )
        encoded = json.dumps(
            {
                "response": response,
                "stored": self.store.get_operator_config(CANARY_CONNECTIVITY_CONFIG_KEY),
                "actions": self.store.list_operator_actions(),
            },
            default=str,
        )
        self.assertNotIn(exception_secret, encoded)


    def test_operator_status_separates_unknown_external_hermes_from_internal_queue(self) -> None:
        before_changes = self.store.connection.total_changes
        status = self.control.status()
        self.assertEqual(self.store.connection.total_changes, before_changes)

        hermes = status["hermes"]
        external = hermes["external_hermes"]
        self.assertEqual(external["job_id"], DEFAULT_HERMES_JOB_ID)
        self.assertEqual(external["status"], "UNKNOWN")
        self.assertIn("evidence", external)
        self.assertNotEqual(external["status"], hermes["internal_queue"]["status"])
        self.assertEqual(hermes["internal_queue"]["status"], "ACTIVE")
        self.assertIn("trigger", hermes["internal_queue"])
        self.assertIsNone(hermes["internal_queue"]["last_cycle_at"])
        self.assertEqual(hermes["control_scope"], "INTERNAL_RESEARCH_QUEUE_PROCESSOR")

    def test_operator_status_retains_latest_canary_signal(self) -> None:
        latest_signal = {
            "signal_id": "signal-status-regression",
            "candidate_id": "candidate-status-regression",
            "status": "READY",
        }
        with patch.object(CanaryService, "latest_signal", return_value=latest_signal):
            status = self.control.status()
        self.assertEqual(status["canary"]["latest_signal"], latest_signal)
    def test_operator_status_projects_mocked_service_without_qualname(self) -> None:
        mocked_service = Mock(spec=[])
        with patch("axiom.operator.CanaryService", mocked_service):
            status = self.control.status()
        service_identity = status["identity"]["service_identity"]
        self.assertIsInstance(service_identity, str)
        self.assertTrue(service_identity)

    def test_operator_status_observes_without_current_selection_despite_old_authorization(self) -> None:
        with patch.object(
            self.control,
            "rolling_portfolio_state",
            return_value={"selection_status": "NONE", "selection": None},
        ), patch.object(
            self.control,
            "execution_authorization_snapshot",
            return_value={
                "active": {
                    "status": "ACTIVE",
                    "authorization_id": "old-server-authorization",
                },
                "status": "ACTIVE",
            },
        ):
            status = self.control.status()
        self.assertEqual(status["mode"], "observing")
        self.assertEqual(status["execution_state"]["mode"], "observing")

    def test_generation_fences_reject_bool_and_fraction_before_side_effect(self) -> None:
        initial = self.control.risk_settings_snapshot()
        draft_response = self.control.execute(
            "risk.settings.save_draft",
            confirm="SAVE RISK SETTINGS DRAFT",
            payload={"values": {"max_orders_per_day": 4}, "actor": "operator"},
        )
        self.assertTrue(draft_response["ok"])
        draft = draft_response["result"]["risk_settings"]
        draft_id = draft["config_id"]
        invalid_values = (True, 1.5, "1.5")
        for value in invalid_values:
            with self.subTest(action="activate", value=value):
                result = self.control.execute(
                    "risk.settings.activate_draft",
                    confirm="ACTIVATE RISK SETTINGS DRAFT",
                    payload={
                        "config_id": draft_id,
                        "expected_generation": value,
                        "actor": "operator",
                    },
                )
                self.assertEqual(result["reason"], "RISK_SETTINGS_GENERATION_REQUIRED")
        for value in invalid_values:
            with self.subTest(action="enable", value=value):
                result = self.control.execute(
                    "canary.enable_auto",
                    confirm="ENABLE AUTO CANARY POLYMARKET ignored 1",
                    payload={
                        "venue": "polymarket",
                        "config_id": initial["config_id"],
                        "expected_generation": value,
                    },
                )
                self.assertEqual(result["reason"], "CANARY_GENERATION_REQUIRED")
        self.assertEqual(self.control.risk_settings_snapshot()["config_id"], initial["config_id"])
    def test_settings_activation_action_target_uses_exact_config_identity(self) -> None:
        entered = threading.Event()
        release = threading.Event()
        calls: list[str] = []
        first_result: list[dict[str, object]] = []

        def activate(config_id: str, **_: object) -> dict[str, object]:
            calls.append(config_id)
            if config_id == "cfg-first":
                entered.set()
                self.assertTrue(release.wait(5))
            return {"config_id": config_id}

        payload = {
            "config_id": "cfg-first",
            "expected_generation": 1,
            "actor": "operator",
        }
        with patch.object(self.control, "activate_risk_settings_draft", side_effect=activate):
            worker = threading.Thread(
                target=lambda: first_result.append(
                    self.control.execute(
                        "risk.settings.activate_draft",
                        confirm="ACTIVATE RISK SETTINGS DRAFT",
                        payload=payload,
                    )
                )
            )
            worker.start()
            self.assertTrue(entered.wait(5))

            duplicate = self.control.execute(
                "canary.settings.activate_draft",
                confirm="ACTIVATE RISK SETTINGS DRAFT",
                payload=payload,
            )
            self.assertFalse(duplicate["ok"])
            self.assertEqual(duplicate["reason"], "ACTION_ALREADY_RUNNING")
            self.assertEqual(duplicate["target"], "cfg-first:1")

            distinct = self.control.execute(
                "risk.settings.activate_draft",
                confirm="ACTIVATE RISK SETTINGS DRAFT",
                payload={
                    "config_id": "cfg-second",
                    "expected_generation": 1,
                    "actor": "operator",
                },
            )
            self.assertTrue(distinct["ok"])
            self.assertEqual(distinct["target"], "cfg-second:1")
            release.set()
            worker.join(timeout=5)

        self.assertFalse(worker.is_alive())
        self.assertTrue(first_result[0]["ok"])
        self.assertEqual(calls, ["cfg-first", "cfg-second"])


    def test_enable_rejects_missing_future_and_stale_connectivity_readiness(self) -> None:
        settings = self.control.risk_settings_snapshot()
        checked_now = datetime.now(timezone.utc)
        projections: dict[str, dict[str, object]] = {
            "missing": _safe_connectivity_projection(
                ready=True,
                allowance_status="AVAILABLE",
            ),
            "future": _safe_connectivity_projection(
                ready=True,
                allowance_status="AVAILABLE",
                checked_at=(checked_now + timedelta(seconds=1)).isoformat(),
            ),
            "stale": _safe_connectivity_projection(
                ready=True,
                allowance_status="AVAILABLE",
                checked_at=(checked_now - timedelta(seconds=61)).isoformat(),
            ),
        }
        projections["missing"].pop("checked_at")
        service = Mock()
        with patch("axiom.operator.CanaryService", return_value=service):
            for label, projection in projections.items():
                with self.subTest(projection=label):
                    self.store.set_operator_config(
                        CANARY_CONNECTIVITY_CONFIG_KEY,
                        projection,
                    )
                    response = self.control.execute(
                        "canary.enable_auto",
                        confirm=(
                            f"ENABLE AUTO CANARY POLYMARKET "
                            f"{settings['config_id']} {settings['generation']}"
                        ),
                        payload={
                            "venue": "polymarket",
                            "config_id": settings["config_id"],
                            "expected_generation": settings["generation"],
                        },
                    )
                    self.assertFalse(response["ok"])
                    self.assertEqual(response["reason"], "CONNECTIVITY_CHECK_REQUIRED")
        service.enable_autonomous_micro_live.assert_not_called()


    def test_draft_activation_and_enable_use_exact_active_settings_identity(self) -> None:
        initial = self.control.risk_settings_snapshot()
        initial_id = initial["config_id"]
        initial_generation = initial["generation"]
        draft_response = self.control.execute(
            "canary.settings.save_draft",
            confirm="SAVE RISK SETTINGS DRAFT",
            payload={"values": {"max_orders_per_day": 4}, "actor": "operator"},
        )
        self.assertTrue(draft_response["ok"])
        draft_id = draft_response["result"]["risk_settings"]["config_id"]
        self.assertNotEqual(draft_id, initial_id)

        activation = self.control.execute(
            "canary.settings.activate_draft",
            confirm="ACTIVATE RISK SETTINGS DRAFT",
            payload={
                "config_id": draft_id,
                "expected_generation": initial_generation,
                "actor": "operator",
            },
        )
        self.assertTrue(activation["ok"])
        active = activation["result"]["risk_settings"]
        self.assertEqual(active["config_id"], draft_id)
        self.assertEqual(active["generation"], initial_generation + 1)
        self.assertEqual(active["values"]["max_submitted_orders_per_day"], 4)

        fresh_checked_at = datetime.now(timezone.utc).isoformat()
        self.store.set_operator_config(
            CANARY_CONNECTIVITY_CONFIG_KEY,
            _safe_connectivity_projection(
                ready=True,
                allowance_status="AVAILABLE",
                checked_at=fresh_checked_at,
            ),
        )
        service = Mock()
        service.enable_autonomous_micro_live.return_value = {
            "micro_live_canary": "AUTONOMOUS_MICRO_LIVE",
            "settings_config_id": draft_id,
            "settings_generation": active["generation"],
        }
        assert self.server._server is not None
        credentials = _configured_credentials()
        with patch("axiom.operator.CredentialStore", return_value=credentials), patch(
            "axiom.operator.CanaryService", return_value=service
        ):
            status, enabled = self._post(
                {
                    "action": "canary.enable_auto",
                    "confirm": f"ENABLE AUTO CANARY POLYMARKET {draft_id} {active['generation']}",
                    "payload": {
                        "venue": "polymarket",
                        "config_id": draft_id,
                        "expected_generation": active["generation"],
                    },
                },
                token=self.server._server.control_token,
            )
        self.assertEqual(status, 200)
        self.assertTrue(enabled["ok"])
        service.enable_autonomous_micro_live.assert_called_once_with(
            venue="polymarket",
            config_id=draft_id,
            expected_generation=active["generation"],
            expected_credential_fingerprint=CONNECTIVITY_CREDENTIAL_FINGERPRINT,
        )
        self.assertEqual(
            self.control.risk_settings_snapshot()["active"]["config_id"],
            draft_id,
        )

    def test_read_only_success_audit_recovers_stale_latch_and_bounds_state(self) -> None:
        started_at = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
        self.store.set_operator_config(
            "operator_action_state",
            {
                "actions": [
                    {
                        "action_id": "operator-action:stale-connectivity",
                        "action": "canary.connectivity_check",
                        "target": "",
                        "status": "RUNNING",
                        "started_at": started_at,
                        "pid": 15220,
                    }
                ]
            },
        )
        audit_result = {
            "connectivity": {
                "ready": True,
                "status": "READY",
                "diagnostics": {
                    "token_readiness": [
                        {"market_id": f"MARKET-{index}", "book": "x" * 2048}
                        for index in range(8)
                    ]
                },
            }
        }
        self.store.record_operator_action(
            "canary.connectivity_check",
            "",
            success=True,
            result=audit_result,
        )
        with patch.object(
            self.control,
            "_run_connectivity_probe",
            side_effect=AssertionError("recovered action must not replay"),
        ):
            recovered = self.control.execute("canary.connectivity_check")
        self.assertTrue(recovered["ok"])
        self.assertEqual(recovered["action_status"], "COMPLETE")
        self.assertEqual(recovered["action_id"], "operator-action:stale-connectivity")
        self.assertEqual(recovered["result"], audit_result)
        state = self.store.get_operator_config("operator_action_state", {})
        actions = state.get("actions", []) if isinstance(state, dict) else []
        self.assertEqual(actions[0]["status"], "COMPLETE")
        self.assertEqual(actions[0]["reason"], "RECOVERED_FROM_SUCCESS_AUDIT")
        self.assertLessEqual(len(json.dumps(state, separators=(",", ":"))), 16_384)
        with patch.object(
            self.control,
            "_run_connectivity_probe",
            return_value={"ready": False},
        ) as fresh_probe:
            refreshed = self.control.execute("canary.connectivity_check")
        self.assertTrue(refreshed["ok"])
        fresh_probe.assert_called_once_with(None)
        self.assertEqual(refreshed["action_status"], "COMPLETE")
        state = self.store.get_operator_config("operator_action_state", {})
        actions = state.get("actions", []) if isinstance(state, dict) else []
        self.assertEqual([action["status"] for action in actions], ["COMPLETE", "COMPLETE"])
        self.assertLessEqual(len(json.dumps(state, separators=(",", ":"))), 16_384)

    def test_inflight_action_identity_survives_restart_without_second_restart(self) -> None:
        started = threading.Event()
        release = threading.Event()
        first_result: dict[str, object] = {}
        launcher = Mock()
        restart_calls: list[str] = []

        def blocked_restart() -> dict[str, object]:
            restart_calls.append("restart")
            started.set()

            if not release.wait(2):
                raise AssertionError("restart side effect was not released")
            return {"status": "RUNNING", "pid": 4242, "revision": "fixture-restarted"}

        def run_first() -> None:
            first_result.update(self.control.execute("node.restart"))

        with patch.object(self.control, "restart_node", side_effect=blocked_restart):
            thread = threading.Thread(target=run_first, daemon=True)
            thread.start()
            self.assertTrue(started.wait(1))
            with AxiomStore(self.db) as reopened_store:
                reopened = OperatorControlPlane(reopened_store, node_launcher=launcher)
                duplicate = reopened.execute("node.restart")
            self.assertEqual(duplicate["reason"], "ACTION_ALREADY_RUNNING")
            self.assertEqual(duplicate["action_status"], "RUNNING")
            action_id = duplicate["action_id"]
            self.assertTrue(str(action_id).startswith("operator-action:"))
            launcher.assert_not_called()
            self.assertEqual(restart_calls, ["restart"])
            release.set()
            thread.join(2)
        self.assertFalse(thread.is_alive())
        self.assertTrue(first_result["ok"])
        self.assertEqual(first_result["action_id"], action_id)
        self.assertEqual(first_result["action_status"], "COMPLETE")
    def test_enable_rejects_mismatched_fresh_credential_binding_before_service(self) -> None:
        settings = self.control.risk_settings_snapshot()
        projection = _safe_connectivity_projection(
            ready=True,
            allowance_status="AVAILABLE",
            checked_at=datetime.now(timezone.utc).isoformat(),
        )
        projection["account"]["credential_fingerprint"] = "sha256:v1:" + ("1" * 64)
        self.store.set_operator_config(CANARY_CONNECTIVITY_CONFIG_KEY, projection)
        credentials = _configured_credentials()
        with patch("axiom.operator.CredentialStore", return_value=credentials), patch(
            "axiom.operator.CanaryService"
        ) as service:
            response = self.control.execute(
                "canary.enable_auto",
                confirm=(
                    f"ENABLE AUTO CANARY POLYMARKET "
                    f"{settings['config_id']} {settings['generation']}"
                ),
                payload={
                    "venue": "polymarket",
                    "config_id": settings["config_id"],
                    "expected_generation": settings["generation"],
                },
            )
        self.assertFalse(response["ok"])
        self.assertEqual(response["reason"], "CREDENTIAL_BINDING_MISMATCH")
        service.assert_not_called()
    def test_status_reads_persisted_projection_without_process_or_credential_probes(self) -> None:
        class ExplodingKeyring:
            @staticmethod
            def get_password(*_args: object, **_kwargs: object) -> str:
                raise AssertionError("status touched keyring")

        class StorageOnlyCredentials:
            @classmethod
            def cached_projection(cls, **_kwargs: object) -> dict[str, object]:
                return {
                    "configured": None,
                    "status": "NOT CHECKED",
                    "secret_values_exposed": False,
                }

            def __init__(self) -> None:
                raise AssertionError("status constructed credential provider")

        with patch.dict("sys.modules", {"keyring": ExplodingKeyring}), patch(
            "axiom.operator.CredentialStore",
            StorageOnlyCredentials,
        ), patch(
            "axiom.operator.PolymarketClobV2Venue",
            side_effect=AssertionError("status constructed venue"),
        ) as venue, patch(
            "axiom.operator.subprocess.run",
            side_effect=AssertionError("status launched subprocess"),
        ) as subprocess_run, patch(
            "axiom.operator.subprocess.Popen",
            side_effect=AssertionError("status launched process"),
        ) as subprocess_popen:
            status = self.control.status()
        self.assertIsInstance(status, dict)
        self.assertEqual(status["credentials"]["status"], "NOT CHECKED")
        venue.assert_not_called()
        subprocess_run.assert_not_called()
        subprocess_popen.assert_not_called()

    def test_stale_running_worker_is_not_reported_live_or_signal_ready(self) -> None:
        old_heartbeat = datetime.now(timezone.utc) - timedelta(minutes=10)
        self.store.save_worker_state(
            "autonomous-canary",
            "RUNNING",
            {
                "last_signal_id": "stale-signal",
                "stale_after_seconds": 1,
                "last_error_code": "WORKER_HEARTBEAT_EXPIRED",
            },
            heartbeat_at=old_heartbeat,
        )
        status = self.control.status()
        worker = status["autonomous_canary_worker"]
        self.assertEqual(worker["status"], "STALE")
        self.assertEqual(worker["last_error_code"], "WORKER_HEARTBEAT_EXPIRED")
        self.assertIsNone(status["canary"]["latest_signal"])
        self.assertNotEqual(status["canary"]["autonomous"].get("worker_status"), "LIVE")


    def test_operator_exports_default_hermes_job_id_and_checks_loopback_hosts(self) -> None:
        exported: dict[str, object] = {}
        exec("from axiom.operator import *", exported)
        self.assertEqual(exported["DEFAULT_HERMES_JOB_ID"], DEFAULT_HERMES_JOB_ID)
        self.assertTrue(_loopback_host("localhost"))
        self.assertTrue(_loopback_host("127.0.0.1"))
        self.assertTrue(_loopback_host("::1"))
        self.assertFalse(_loopback_host("192.0.2.1"))


    def test_connectivity_projection_survives_fresh_file_backed_dashboard(self) -> None:
        restart_db = str(Path(self.tempdir.name) / "connectivity-restart.sqlite")
        expected = _safe_connectivity_projection(
            ready=False,
            allowance_status="INSUFFICIENT",
            credentials_configured=False,
        )
        service = Mock()
        service.connectivity_check.return_value = expected
        credentials = Mock()
        credentials.configured.return_value = False
        with AxiomStore(restart_db) as original_store:
            original_control = OperatorControlPlane(original_store)
            with patch("axiom.operator.CredentialStore", return_value=credentials), patch(
                "axiom.operator.CanaryService",
                return_value=service,
            ), patch(
                "axiom.operator.utc_now",
                return_value=datetime(2026, 1, 2, 12, tzinfo=timezone.utc),
            ):
                response = original_control.execute("canary.connectivity_check")
            self.assertTrue(response["ok"])

        with AxiomStore(restart_db) as reopened_store:
            reopened_control = OperatorControlPlane(reopened_store)
            dashboard = DashboardData(
                store=reopened_store,
                control=reopened_control,
            ).canary_data()
            persisted = reopened_store.get_operator_config(CANARY_CONNECTIVITY_CONFIG_KEY)
            self.assertEqual(persisted["status"], "BLOCKED")
            self.assertFalse(persisted["ready"])
            self.assertEqual(persisted["failure_codes"], ["CANARY_ALLOWANCE_INSUFFICIENT"])
            native = reopened_control.status()["connectivity"]
            self.assertEqual(native["status"], "BLOCKED")
            self.assertFalse(native["ready"])
            self.assertEqual(native["failure_codes"], ["CANARY_ALLOWANCE_INSUFFICIENT"])
            dashboard_connectivity = dashboard["connectivity"]
            self.assertEqual(dashboard_connectivity["status"], "BLOCKED")
            self.assertFalse(dashboard_connectivity["ready"])
            self.assertEqual(
                dashboard_connectivity["failure_codes"],
                ["CANARY_ALLOWANCE_INSUFFICIENT"],
            )
            encoded = json.dumps(dashboard, default=str)
            for secret in CONNECTIVITY_SECRET_VALUES:
                self.assertNotIn(secret, encoded)

    def _post(
        self,
        body: dict,
        *,
        token: str | None = None,
        host: str | None = None,
        origin: str | None = None,
    ) -> tuple[int, dict]:
        assert self.server.url is not None
        headers = {"Content-Type": "application/json"}
        if token is not None:
            headers["X-Axiom-Control-Token"] = token
        if host is not None:
            headers["Host"] = host
        if origin is not None:
            headers["Origin"] = origin
        request = Request(
            self.server.url + "/api/control",
            data=json.dumps(body).encode(),
            headers=headers,
            method="POST",
        )
        with urlopen(request, timeout=3) as response:
            return response.status, json.loads(response.read())
    def test_exploratory_flat_target_fields_are_accepted_and_forwarded(self) -> None:
        assert self.server._server is not None
        with patch.object(self.control, "execute", return_value={"ok": True}) as execute:
            status, result = self._post(
                {
                    "action": "exploratory.live.review_confirm",
                    "confirm": "CONFIRM EXPLORATORY LIVE",
                    "candidate_id": "candidate-live",
                    "market_id": "market-live",
                    "token_id": "token-live",
                },
                token=self.server._server.control_token,
            )
        self.assertEqual(status, 200)
        self.assertTrue(result["ok"])
        payload = execute.call_args.kwargs["payload"]
        self.assertEqual(
            payload,
            {
                "candidate_id": "candidate-live",
                "market_id": "market-live",
                "token_id": "token-live",
            },
        )


    def test_localhost_control_requires_token_and_allowlist(self) -> None:
        assert self.server._server is not None
        assert self.server.url is not None
        body = {"action": "hermes.pause"}
        with self.assertRaises(HTTPError) as context:
            self._post(body)
        self.assertEqual(context.exception.code, 403)
        status, result = self._post(body, token=self.server._server.control_token)
        self.assertEqual(status, 200)
        self.assertTrue(result["ok"])
        self.assertEqual(self.store.get_scheduler_state("hermes-control")["status"], "PAUSED")
        self.assertFalse(_DashboardHandler._loopback_client(type("Request", (), {"client_address": ("192.0.2.1", 1)})()))
    def test_dashboard_requires_exact_host_and_origin_while_local_reads_work(self) -> None:
        assert self.server._server is not None
        assert self.server.url is not None
        port = int(self.server._server.server_address[1])
        authority = f"127.0.0.1:{port}"
        valid_get = Request(
            self.server.url + "/api/operator",
            headers={"Host": authority},
            method="GET",
        )
        with urlopen(valid_get, timeout=3) as response:
            self.assertEqual(response.status, 200)
            self.assertIsInstance(json.loads(response.read()), dict)

        hostile_get = Request(
            self.server.url + "/api/operator",
            headers={"Host": f"attacker.invalid:{port}"},
            method="GET",
        )
        with self.assertRaises(HTTPError) as hostile:
            urlopen(hostile_get, timeout=3)
        self.assertEqual(hostile.exception.code, 403)

        body = {"action": "hermes.resume"}
        with self.assertRaises(HTTPError) as wrong_origin:
            self._post(
                body,
                token=self.server._server.control_token,
                host=authority,
                origin=f"http://127.0.0.1:{port + 1}",
            )
        self.assertEqual(wrong_origin.exception.code, 403)
        status, result = self._post(
            body,
            token=self.server._server.control_token,
            host=authority,
            origin=f"http://127.0.0.1:{port}",
        )
        self.assertEqual(status, 200)
        self.assertTrue(result["ok"])

    def test_no_arbitrary_command_execution(self) -> None:
        launcher = Mock()
        control = OperatorControlPlane(self.store, node_launcher=launcher)
        result = control.execute("shell.exec", "python", confirm="")
        self.assertNotEqual(result.get("ok"), True)
        self.assertEqual(result["reason"], "ACTION_NOT_ALLOWED")
        launcher.assert_not_called()
        self.assertNotIn("command", json.dumps(result).lower())

    def test_duplicate_node_prevention(self) -> None:
        launcher = Mock()
        with patch.dict(os.environ, {"AXIOM_EXECUTION_PROFILE": "isolated"}):
            control = OperatorControlPlane(self.store, node_launcher=launcher)
            running = {
                "status": "RUNNING",
                "pid": 123,
                "worker_identity_valid": True,
                "execution_profile": "isolated",
            }
            with patch.object(control, "_node_status", return_value=running):
                self.assertEqual(control.ensure_node(), running)
        launcher.assert_not_called()

    def test_isolated_operator_rejects_running_production_node(self) -> None:
        launcher = Mock()
        with patch.dict(os.environ, {"AXIOM_EXECUTION_PROFILE": "isolated"}):
            control = OperatorControlPlane(self.store, node_launcher=launcher)
            running = {
                "status": "RUNNING",
                "pid": 123,
                "worker_identity_valid": True,
                "execution_profile": "production",
            }
            with patch.object(control, "_node_status", return_value=running):
                with self.assertRaisesRegex(
                    OperatorControlError,
                    "^NODE_EXECUTION_PROFILE_MISMATCH$",
                ):
                    control.ensure_node()
        launcher.assert_not_called()

    def test_isolated_operator_rejects_restarting_production_node(self) -> None:
        with patch.dict(os.environ, {"AXIOM_EXECUTION_PROFILE": "isolated"}):
            control = OperatorControlPlane(self.store)
            running = {
                "status": "RUNNING",
                "pid": 123,
                "worker_identity_valid": True,
                "execution_profile": "production",
            }
            with patch.object(control, "_node_status", return_value=running), patch(
                "axiom.operator.os.kill"
            ) as kill:
                with self.assertRaisesRegex(
                    OperatorControlError,
                    "^NODE_EXECUTION_PROFILE_MISMATCH$",
                ):
                    control.restart_node()
        kill.assert_not_called()
    def test_windows_liveness_query_failure_is_unknown_without_kill(self) -> None:
        import ctypes

        kernel32 = Mock()
        kernel32.OpenProcess.return_value = 0
        kernel32.GetLastError.return_value = 5  # ERROR_ACCESS_DENIED
        windll = SimpleNamespace(kernel32=kernel32)
        with patch.object(node_module.os, "name", "nt"), patch.object(
            node_module.os, "kill"
        ) as kill, patch.object(ctypes, "windll", windll, create=True):
            self.assertIsNone(node_module._pid_alive(424242))
        kill.assert_not_called()

    def test_unknown_node_liveness_keeps_lock_and_rejects_start(self) -> None:
        lock_path = Path(f"{self.db}.lock")
        lock_path.write_text("424242\nowned-run\n", encoding="ascii")
        launcher = Mock()
        control = OperatorControlPlane(self.store, node_launcher=launcher)
        with patch("axiom.operator._pid_alive", return_value=None):
            with self.assertRaisesRegex(OperatorControlError, "^NODE_STATUS_UNKNOWN$"):
                control.ensure_node()
        self.assertTrue(lock_path.exists())
        launcher.assert_not_called()
    def test_dead_stale_lock_requires_manual_recovery_and_keeps_lock(self) -> None:
        lock_path = Path(f"{self.db}.lock")
        marker = b"424242\nstale-run\n"
        lock_path.write_bytes(marker)
        launcher = Mock()
        control = OperatorControlPlane(self.store, node_launcher=launcher)
        with patch("axiom.operator._pid_alive", return_value=False):
            with self.assertRaisesRegex(
                OperatorControlError,
                "^NODE_STALE_LOCK_MANUAL_RECOVERY$",
            ):
                control.ensure_node()
        self.assertEqual(lock_path.read_bytes(), marker)
        launcher.assert_not_called()


    def test_fixed_hermes_adapter_operations_are_persisted(self) -> None:
        paused = self.control.execute("hermes.pause")
        self.assertTrue(paused["ok"])
        self.assertEqual(paused["control_scope"], "INTERNAL_RESEARCH_QUEUE_PROCESSOR")
        self.assertEqual(paused["result"]["hermes"]["control_scope"], "INTERNAL_RESEARCH_QUEUE_PROCESSOR")
        blocked = self.control.execute("hermes.run_now")
        self.assertNotEqual(blocked.get("ok"), True)
        self.assertEqual(blocked["reason"], "HERMES_PAUSED")
        self.assertEqual(blocked["control_scope"], "INTERNAL_RESEARCH_QUEUE_PROCESSOR")
        self.assertEqual(paused["result"]["hermes"]["internal_queue"]["status"], "PAUSED")
        self.assertEqual(paused["result"]["hermes"]["external_hermes"]["status"], "UNKNOWN")
        self.assertEqual(self.control.status()["hermes"]["status"], "PAUSED")

        resumed = self.control.execute("hermes.resume")
        self.assertTrue(resumed["ok"])
        self.assertEqual(resumed["control_scope"], "INTERNAL_RESEARCH_QUEUE_PROCESSOR")
        self.assertEqual(resumed["result"]["hermes"]["internal_queue"]["status"], "ACTIVE")
        self.assertEqual(resumed["result"]["hermes"]["external_hermes"]["status"], "UNKNOWN")
        self.assertEqual(self.control.status()["hermes"]["status"], "ACTIVE")

        processor = Mock()
        processor.process_pending.return_value = SimpleNamespace(claimed=1, completed=1, rejected=0, failed=0)
        with patch("axiom.operator.AutonomousResearchProcessor", return_value=processor):
            result = self.control.execute("hermes.run_now")
        self.assertTrue(result["ok"])
        self.assertEqual(result["control_scope"], "INTERNAL_RESEARCH_QUEUE_PROCESSOR")
        self.assertEqual(result["result"]["hermes"]["external_hermes"]["status"], "UNKNOWN")
        self.assertEqual(result["result"]["hermes"]["internal_queue"]["status"], "ACTIVE")
        processor.process_pending.assert_called_once_with(worker="operator-hermes")
        self.assertNotIn("argv", json.dumps(result).lower())


    def test_bootstrap_duplicate_and_resume_behavior(self) -> None:
        snapshot = SimpleNamespace(selected_symbols=("BTCUSDT",))
        started = threading.Event()
        release = threading.Event()

        def blocked_worker(**_: object) -> None:
            started.set()
            release.wait(2)

        with patch("axiom.operator.load_crypto_universe", return_value=snapshot), patch.object(
            self.control, "_bootstrap_worker", side_effect=blocked_worker
        ):
            first = self.control.start_bootstrap(resume=False)
            self.assertEqual(first["status"], "RUNNING")
            self.assertTrue(started.wait(1))
            with self.assertRaisesRegex(RuntimeError, "BOOTSTRAP_ALREADY_RUNNING"):
                self.control.start_bootstrap(resume=True)
            release.set()
            thread = self.control._bootstrap_threads[BOOTSTRAP_JOB_NAME]
            thread.join(2)
        self.store.set_operator_job(
            BOOTSTRAP_JOB_NAME,
            "FAILED",
            {"total_symbols": 1, "total_datasets": 4},
            pid=None,
            last_error="BOOTSTRAP_DATASET_FAILED",
            resumable=True,
        )
        with patch("axiom.operator.load_crypto_universe", return_value=snapshot), patch.object(
            self.control, "_bootstrap_worker", return_value=None
        ):
            resumed = self.control.start_bootstrap(resume=True)
            self.assertEqual(resumed["status"], "RUNNING")
            thread = self.control._bootstrap_threads[BOOTSTRAP_JOB_NAME]
            thread.join(2)

    def _seed_candidate(
        self,
        candidate_id: str = "candidate-1",
        *,
        store: AxiomStore | None = None,
        timestamp: datetime | None = None,
    ) -> None:
        target = store or self.store
        dataset_timestamp = datetime(2025, 1, 1, tzinfo=timezone.utc)
        target.save_dataset(
            "dataset-1",
            "dataset-v1",
            [{"timestamp": dataset_timestamp.isoformat(), "price": 0.5, "source_type": "HISTORICAL"}],
        )
        target.save_dataset_catalog(
            "dataset-1",
            "dataset-v1",
            provider="polymarket",
            instrument="POLYMARKET",
            market_type="prediction",
            timeframe="event",
            start_timestamp=dataset_timestamp,
            end_timestamp=dataset_timestamp,
            row_count=1,
            completeness=1.0,
            quality="PRICE_PROXY",
            source_type="HISTORICAL",
            snapshot_id="dataset-1:dataset-v1",
            metadata={
                "provider": "polymarket",
                "source_type": "HISTORICAL",
                "research_quality": "PRICE_PROXY",
                "historical_order_book_available": False,
            },
        )
        strategy_hash = "strategy-hash"
        model_hash = "model-hash"
        config_hash = "config-hash"
        frozen_hash = hashlib.sha256(f"{strategy_hash}|{model_hash}|{config_hash}".encode()).hexdigest()
        attestation = target.verify_dataset_integrity_attestation(
            "dataset-1",
            "dataset-v1",
            force=True,
        )
        policy = normalize_market_scope(
            {
                **normalize_market_scope(market_ids=["MARKET-1"]).as_dict(),
                "provenance": "canonical",
            }
        )
        payload = {
            "candidate_id": candidate_id,
            "strategy_id": "strategy-1",
            "experiment_family": "test-family",
            "market_type": "prediction",
            "instrument": "MARKET-1",
            "dataset_id": "dataset-1",
            "dataset_version": "dataset-v1",
            "dataset_selector": {
                "dataset_id": "dataset-1",
                "dataset_version": "dataset-v1",
                "source_type": "HISTORICAL",
            },
            "dataset_attestation": attestation,
            "dataset_provenance": {
                "dataset_id": "dataset-1",
                "dataset_version": "dataset-v1",
                "source_type": "HISTORICAL",
                "time_split": "train-validation-holdout",
            },
            "source_type": "HISTORICAL",
            "timeframe": "event",
            "market_scope": policy.as_dict(),
            "market_scope_hash": policy.scope_hash,
            "market_scope_version": policy.scope_version,
            "plan_hash": "sha256:operator-plan-v1",
            "experiment_plan": {
                "dataset_selector": {
                    "dataset_id": "dataset-1",
                    "dataset_version": "dataset-v1",
                    "source_type": "HISTORICAL",
                },
                "market_scope": policy.as_dict(),
                "min_independent_samples": 30,
                "min_trades": 0,
            },
            "strategy_hash": strategy_hash,
            "model_hash": model_hash,
            "config_hash": config_hash,
            "frozen_hash": frozen_hash,
            "schema_validated": True,
            "historical_backtest_passed": True,
            "validation_passed": True,
            "robustness_passed": True,
            "data_quality_passed": True,
            "minimum_sample_check": {
                "passed": True,
                "count": 30,
                "trades": 0,
                "min_observations": 30,
                "min_trades": 0,
                "checks": {"observations": True, "trades": True},
            },
            "holdout_used": False,
            "frozen": True,
            "critical_error": None,
        }
        target.save_candidate_lifecycle(candidate_id, "IDEA", payload, timestamp=timestamp)
        target.save_candidate_lifecycle(
            candidate_id,
            "FROZEN",
            payload,
            from_stage="IDEA",
            timestamp=timestamp,
        )
        resolved_at = timestamp or datetime.now(timezone.utc)
        target.save_market_scope_resolution(
            resolve_market_scope(
                candidate_id,
                {"market_scope": payload["market_scope"]},
                [
                    {
                        "market_id": "MARKET-1",
                        "condition_id": "MARKET-1-CONDITION",
                        "yes_token_id": "MARKET-1-YES",
                        "no_token_id": "MARKET-1-NO",
                        "instrument": "POLYMARKET",
                        "venue": "POLYMARKET",
                        "source_type": "CURRENT",
                        "active": True,
                        "open": True,
                        "closed": False,
                        "settlement": "open",
                        "accepting_orders": True,
                        "enable_order_book": True,
                        "metadata_provenance": {
                            "source_type": "CURRENT",
                            "metadata_hash": "sha256:MARKET-1",
                        },
                    }
                ],
                resolved_at=resolved_at,
            )
        )
    def test_dashboard_restart_hydrates_persisted_overview_and_canary(self) -> None:
        restart_db = str(Path(self.tempdir.name) / "restart.sqlite")
        timestamp = datetime.now(timezone.utc)
        secrets = ("FAKE_PRIVATE_KEY", "FAKE_WALLET_ADDRESS", "FAKE_RELAYER_KEY")
        credentials = Mock()
        credentials.configured.return_value = True
        credentials.load.return_value = {
            "private_key": secrets[0],
            "wallet_address": secrets[1],
            "relayer_api_key": secrets[2],
        }
        credentials.cached_projection.return_value = {
            "configured": True,
            "status": "CONFIGURED",
            "secret_values_exposed": False,
        }

        with patch("axiom.storage._now_iso", return_value=timestamp.isoformat()):
            with AxiomStore(restart_db) as original_store:
                original_control = OperatorControlPlane(original_store)
                original_dashboard = DashboardData(store=original_store, control=original_control)
                self._seed_candidate("winner", store=original_store, timestamp=timestamp)
                winner = original_store.load_candidate_lifecycle("winner")
                self.assertIsInstance(winner, dict)
                winner_payload = dict(winner["payload"])

                def rankable_payload(candidate_id: str, score: float) -> dict[str, object]:
                    return {
                        **winner_payload,
                        "candidate_id": candidate_id,
                        "validation_expectancy": score,
                        "validation_confidence_lower_bound": score,
                        "validation_stability": 0.90,
                        "validation_calibration": 0.90,
                        "validation_sample_count": 100,
                        "validation_trade_count": 50,
                        "validation_execution_quality": 0.90,
                    }

                winner_payload = rankable_payload("winner", 0.42)
                original_store.save_candidate_lifecycle(
                    "winner",
                    "FROZEN",
                    winner_payload,
                    from_stage="FROZEN",
                    timestamp=timestamp,
                )
                runner_payload = rankable_payload("runner", 0.21)
                original_store.save_candidate_lifecycle("runner", "IDEA", runner_payload, timestamp=timestamp)
                original_store.save_candidate_lifecycle(
                    "runner",
                    "FROZEN",
                    runner_payload,
                    from_stage="IDEA",
                    timestamp=timestamp,
                )
                original_store.save_market_scope_resolution(
                    resolve_market_scope(
                        "runner",
                        {"market_scope": runner_payload["market_scope"]},
                        [
                            {
                                "market_id": "MARKET-1",
                                "condition_id": "MARKET-1-CONDITION",
                                "yes_token_id": "MARKET-1-YES",
                                "no_token_id": "MARKET-1-NO",
                                "metadata_provenance": {
                                    "source_type": "CURRENT",
                                    "metadata_hash": "sha256:MARKET-1",
                                },
                            }
                        ],
                        resolved_at=timestamp,
                    )
                )

                canary_service = CanaryService(original_store, clock=lambda: timestamp)
                ranking = CandidateCanaryRanker(
                    original_store,
                    service=canary_service,
                    clock=lambda: timestamp,
                ).evaluate_and_select(timestamp)
                self.assertEqual(ranking["selected_candidate"], "winner")
                selected = ranking["selected"]
                self.assertIsInstance(selected, dict)
                selected_score = selected["total_score"]
                self.assertIsInstance(selected_score, float)

                # Use the typed operator action to persist the disabled state and
                # its restart-safe autonomous decision/blocker.
                self.assertTrue(original_control.execute("canary.disarm", confirm="DISARM")["ok"])

                queue_item = original_store.enqueue_research_item(
                    "hypothesis",
                    {
                        "dataset_id": "dataset-1",
                        "dataset_version": "dataset-v1",
                        "family": "test-family",
                        "reason_code": "HERMES_FIXTURE_COMPLETE",
                    },
                    dedupe_key="hermes-restart-fixture",
                    source="hermes",
                    author="fixture",
                    item_id="hermes-restart-item",
                    available_at=timestamp,
                )
                self.assertEqual(queue_item["status"], "PENDING")
                claimed = original_store.claim_research_item("hermes-fixture", now=timestamp)
                self.assertIsNotNone(claimed)
                original_store.complete_research_item(
                    "hermes-restart-item",
                    "COMPLETED",
                    result={
                        "dataset_id": "dataset-1",
                        "dataset_version": "dataset-v1",
                        "family": "test-family",
                        "reason_code": "HERMES_FIXTURE_COMPLETE",
                    },
                    now=timestamp + timedelta(seconds=1),
                    worker="hermes-fixture",
                )

            # The context boundary above closes the original store before the
            # fresh objects below are created.
            with AxiomStore(restart_db) as reopened_store:
                reopened_control = OperatorControlPlane(reopened_store)
                reopened_dashboard = DashboardData(store=reopened_store, control=reopened_control)
                before_hydration_changes = reopened_store.connection.total_changes
                with patch(
                    "axiom.canary.CredentialStore.cached_projection",
                    return_value=credentials.cached_projection.return_value,
                ):
                    overview = reopened_dashboard.overview_summary()
                    operator = reopened_dashboard.operator_data()
                    canary = reopened_dashboard.canary_data()
                self.assertEqual(reopened_store.connection.total_changes, before_hydration_changes)

        self.assertLessEqual(len(overview["latest_activity"]), 8)
        self.assertEqual(overview["coverage"]["historical_count"], 1)
        self.assertEqual(overview["coverage"]["historical_rows"], 1)
        self.assertEqual(overview["lifecycle_funnel"]["FROZEN"], 2)
        self.assertTrue(overview["latest_activity"])
        self.assertEqual(overview["research_cards"]["canary_eligible"], 2)
        self.assertEqual(
            {item["candidate_id"] for item in overview["latest_candidates"]},
            {"winner", "runner"},
        )
        overview_hermes = overview["hermes_latest_outcome"]
        self.assertIsInstance(overview_hermes, dict)
        self.assertEqual(overview_hermes["status"], "COMPLETED")
        self.assertEqual(overview_hermes["outcome_type"], "EXPERIMENT_COMPLETED")

        self.assertEqual(operator["research_cards"]["canary_eligible"], 2)
        self.assertEqual(operator["coverage"]["historical_count"], 1)
        self.assertEqual(operator["coverage"]["historical_rows"], 1)
        self.assertEqual(operator["lifecycle_funnel"]["FROZEN"], 2)
        latest_candidates = operator["latest_candidates"]
        self.assertLessEqual(len(latest_candidates), 10)
        self.assertEqual(
            {item["candidate_id"] for item in latest_candidates},
            {"winner", "runner"},
        )
        hermes_outcome = operator["hermes_latest_outcome"]
        self.assertIsInstance(hermes_outcome, dict)
        self.assertEqual(hermes_outcome["status"], "COMPLETED")
        self.assertEqual(hermes_outcome["outcome_type"], "EXPERIMENT_COMPLETED")
        self.assertEqual(hermes_outcome["reason_code"], "HERMES_FIXTURE_COMPLETE")
        self.assertEqual(hermes_outcome["dataset_id"], "dataset-1")
        self.assertEqual(hermes_outcome["dataset_version"], "dataset-v1")
        self.assertTrue(any(item["kind"] == "research" for item in operator["latest_activity"]))

        status = canary["canary"]
        self.assertEqual(status["micro_live_canary"], "DISARMED")
        self.assertEqual(status["display_state"], "DISABLED")
        self.assertEqual(status["risk_envelope"], canary["risk_settings"]["effective_limits"])
        self.assertEqual(status["risk_limits"], canary["risk_settings"]["effective_limits"])
        self.assertEqual(status["eligible_count"], 2)
        self.assertEqual(status["rankable_count"], 2)
        self.assertEqual(status["execution_event_count"], 0)
        self.assertEqual(status["real_execution_events"], 0)
        self.assertEqual(status["trades"], [])
        self.assertIsNone(canary["canary_signal"])
        self.assertEqual(canary["research_cards"]["canary_eligible"], 2)
        self.assertEqual(canary["candidate_status"]["rankable"], 2)
        self.assertEqual(canary["real_execution_events"], 0)

        autonomous = canary["autonomous_canary"]
        self.assertEqual(autonomous["selected_candidate"], "winner")
        self.assertEqual(autonomous["rank"], 1)
        self.assertEqual(autonomous["score"], selected_score)
        self.assertEqual(autonomous["selection_reason"], "SELECTED_WINNER")
        self.assertEqual(autonomous["eligible_count"], 2)
        self.assertEqual(autonomous["rankable_count"], 2)
        self.assertEqual(autonomous["historical_data_integrity"], "PASS")
        self.assertEqual(autonomous["historical_execution_fidelity"], "PRICE_PROXY · LIMITED")
        self.assertEqual(autonomous["current_execution_evidence"], "CURRENT_ORDER_BOOK_REQUIRED")
        self.assertEqual(autonomous["next_decision"], "ENABLE AUTO CANARY")
        self.assertEqual(autonomous["blocker"], "AUTONOMOUS_CANARY_DISABLED")

        for payload in (operator, canary):
            projected = payload["credentials"]
            self.assertEqual(
                set(projected),
                {"configured", "status", "secret_values_exposed"},
            )
            self.assertTrue(projected["configured"], msg=f"credentials={projected!r}; payload={payload!r}")
            self.assertEqual(projected["status"], "CONFIGURED")
            self.assertFalse(projected["secret_values_exposed"])
        encoded = json.dumps({"operator": operator, "canary": canary}, default=str)
        for secret in secrets:
            self.assertNotIn(secret, encoded)
        self.assertEqual(operator["historical_count"], 1)
        self.assertEqual(operator["historical_rows"], 1)
        for field in (
            "instance",
            "revision",
            "mode",
            "armed",
            "armed_state",
            "policy",
            "daily_budget",
            "lifetime_budget",
            "qualification_coverage",
            "active_strategies",
            "suspended_strategies",
            "current_signals",
            "execution_state",
            "blockers",
            "last_review",
            "next_review",
            "execution_authorization_id",
        ):
            self.assertIn(field, operator)

    def test_authorization_projection_preserves_unknown_terminal_states_and_server_ids(self) -> None:
        dashboard = DashboardData(store=self.store, control=self.control)
        for state in ("UNKNOWN", "EXPIRED", "REVOKED"):
            authorization_id = f"server-generated-{state.lower()}"
            self.store.set_operator_config(
                "execution_authorization_review",
                {
                    "status": state,
                    "mode": "EVIDENCE_SELECTED",
                    "authorization_id": authorization_id,
                    "private_key": f"{state}-PRIVATE-KEY-SENTINEL",
                },
            )
            projected = dashboard.execution_authorization_data()
            self.assertEqual(projected["status"], state)
            self.assertEqual(projected["mode"], "EVIDENCE_SELECTED")
            self.assertEqual(projected["authorization_id"], authorization_id)
            self.assertNotIn("PRIVATE-KEY-SENTINEL", json.dumps(projected))

    def test_execution_authorization_acknowledgment_only_required_for_rejected_funding(self) -> None:
        base_context = {
            "now": datetime(2026, 1, 2, 12, tzinfo=timezone.utc),
            "selection_id": "selection-auth-review",
            "selection_hash": "selection-hash",
            "strategy_versions": ["accepted-strategy"],
            "rejected_strategy_versions": [],
            "selection_policy_hash": "policy-hash",
            "scope_hash": "scope-hash",
            "scope_version": "scope-v1",
            "active_settings_hash": "settings-hash",
            "active_settings_generation": 4,
        }
        values = {
            "purpose": "review accepted strategy",
            "exact_strategy_versions": ["accepted-strategy"],
            "lifetime_budget": "1.00",
            "stop_rules": {"max_submissions": 1},
            "expires_at": "2026-01-03T12:00:00+00:00",
        }
        with patch.object(self.control, "_authorization_context", return_value=base_context), patch.object(
            self.store,
            "register_execution_authorization_draft",
            return_value={"authorization_id": "accepted-auth", "generation": 1, "status": "DRAFT"},
        ) as register:
            accepted = self.control.review_execution_authorization(values)
        self.assertEqual(accepted["status"], "DRAFT")
        self.assertFalse(accepted["draft"]["adverse_evidence_ack_required"])
        self.assertEqual(
            register.call_args.kwargs["adverse_evidence_ack"],
            {"acknowledged": True, "required": False},
        )
        conflicting_values = dict(values)
        conflicting_values.update(
            {
                "selection_policy_hash": "policy-hash",
                "policy_hash": "different-policy",
            }
        )
        with patch.object(
            self.control, "_authorization_context", return_value=base_context
        ), patch.object(
            self.store, "register_execution_authorization_draft"
        ) as conflicting_register:
            with self.assertRaisesRegex(
                OperatorControlError,
                "^EXECUTION_AUTHORIZATION_BINDING_STALE$",
            ):
                self.control.review_execution_authorization(conflicting_values)
        conflicting_register.assert_not_called()

        rejected_context = dict(base_context)
        rejected_context["strategy_versions"] = ["rejected-strategy"]
        rejected_context["rejected_strategy_versions"] = ["rejected-strategy"]
        rejected_values = dict(values)
        rejected_values["exact_strategy_versions"] = ["rejected-strategy"]
        with patch.object(
            self.control, "_authorization_context", return_value=rejected_context
        ), patch.object(
            self.store, "register_execution_authorization_draft"
        ) as rejected_register:
            with self.assertRaisesRegex(
                OperatorControlError,
                "^EXECUTION_AUTHORIZATION_ADVERSE_EVIDENCE_ACK_REQUIRED$",
            ):
                self.control.review_execution_authorization(rejected_values)
        rejected_register.assert_not_called()

        rejected_values["adverse_evidence_ack"] = {
            "acknowledged": True,
            "required": False,
        }
        with patch.object(
            self.control, "_authorization_context", return_value=rejected_context
        ), patch.object(
            self.store, "register_execution_authorization_draft"
        ) as pseudo_register:
            with self.assertRaisesRegex(
                OperatorControlError,
                "^EXECUTION_AUTHORIZATION_ADVERSE_EVIDENCE_ACK_REQUIRED$",
            ):
                self.control.review_execution_authorization(rejected_values)
        pseudo_register.assert_not_called()
        rejected_values["adverse_evidence_ack"] = True
        with patch.object(
            self.control, "_authorization_context", return_value=rejected_context
        ), patch.object(
            self.store,
            "register_execution_authorization_draft",
            return_value={"authorization_id": "rejected-bool-auth", "generation": 3, "status": "DRAFT"},
        ) as bool_register:
            bool_acknowledged = self.control.review_execution_authorization(
                rejected_values
            )
        strict_ack = {"acknowledged": True, "required": True}
        self.assertEqual(bool_register.call_args.kwargs["adverse_evidence_ack"], strict_ack)
        self.assertEqual(bool_acknowledged["draft"]["adverse_evidence_ack"], strict_ack)
        self.assertTrue(bool_acknowledged["draft"]["adverse_evidence_ack_required"])

        rejected_values["adverse_evidence_ack"] = {
            "acknowledged": True,
            "required": True,
        }
        with patch.object(
            self.control, "_authorization_context", return_value=rejected_context
        ), patch.object(
            self.store,
            "register_execution_authorization_draft",
            return_value={"authorization_id": "rejected-auth", "generation": 2, "status": "DRAFT"},
        ):
            acknowledged = self.control.review_execution_authorization(rejected_values)
        self.assertTrue(acknowledged["draft"]["adverse_evidence_ack_required"])
    def test_execution_authorization_requires_explicit_expiry_and_stop_rules(self) -> None:
        context = {
            "now": datetime(2026, 1, 2, 12, tzinfo=timezone.utc),
            "selection_id": "selection-auth-review",
            "selection_hash": "selection-hash",
            "strategy_versions": ["accepted-strategy"],
            "rejected_strategy_versions": [],
            "selection_policy_hash": "policy-hash",
            "scope_hash": "scope-hash",
            "scope_version": "scope-v1",
            "active_settings_hash": "settings-hash",
            "active_settings_generation": 4,
        }
        values = {
            "purpose": "review accepted strategy",
            "exact_strategy_versions": ["accepted-strategy"],
            "lifetime_budget": "1.00",
        }
        with patch.object(self.control, "_authorization_context", return_value=context), patch.object(
            self.store, "register_execution_authorization_draft"
        ) as register:
            with self.assertRaisesRegex(
                OperatorControlError,
                "^EXECUTION_AUTHORIZATION_EXPIRES_AT_REQUIRED$",
            ):
                self.control.review_execution_authorization(values)
        register.assert_not_called()

        values["expires_at"] = "2026-01-03T12:00:00+00:00"
        with patch.object(self.control, "_authorization_context", return_value=context), patch.object(
            self.store, "register_execution_authorization_draft"
        ) as register:
            with self.assertRaisesRegex(
                OperatorControlError,
                "^EXECUTION_AUTHORIZATION_STOP_RULES_REQUIRED$",
            ):
                self.control.review_execution_authorization(values)
        register.assert_not_called()

    def test_exploratory_live_review_snapshot_explains_missing_proposal_members(self) -> None:
        context = {
            "selection_id": "selection-observe",
            "selection_hash": "selection-hash",
            "selection": {
                "status": "OBSERVE",
                "k": 0,
                "global_budget": "0",
                "members": [],
                "reasons": ["NO_ELIGIBLE_PROPOSED_MEMBERS"],
                "operating_policy": {"mode": "EXPLORATORY_LIVE"},
            },
            "policy_id": "policy-live",
            "policy_version": "policy-v1",
            "policy_hash": "policy-hash",
            "scope": {},
            "scope_draft": {},
            "draft_scope": {},
            "setup_bindings": [],
            "draft_member_bindings": [],
            "active_settings_hash": "settings-hash",
            "active_settings_generation": 1,
            "selection_actionable_reasons": {"NO_ELIGIBLE_PROPOSED_MEMBERS": "Run paper evaluation"},
        }
        limits = {
            "max_all_in_buy_usd": "1.00",
            "max_fee_reserve_usd": "0.01",
            "max_gross_daily_buy_usd": "5.00",
            "max_aggregate_open_cost_usd": "5.00",
            "max_aggregate_exposure_usd": "5.00",
            "max_positions": 3,
            "max_submitted_orders_per_day": 5,
            "realized_loss_entry_stop_usd": "2.00",
            "equity_loss_entry_stop_usd": "2.00",
            "max_slippage_bps": 100,
        }
        with patch.object(self.control, "_prepare_reviewed_proposed_selection"), patch.object(
            self.control, "_authorization_context", return_value=context
        ), patch.object(self.control, "_rolling_effective_limits", return_value=limits), patch.object(
            self.control,
            "_selected_market_readiness",
            return_value={"status": "BLOCKED", "blockers": ["MARKET_SELECTION_REQUIRED"]},
        ), patch.object(
            self.control,
            "execution_authorization_snapshot",
            return_value={"authorization": {"status": "DRAFT"}},
        ), patch.object(
            self.control,
            "risk_settings_snapshot",
            return_value={"usage": {}},
        ):
            snapshot = self.control.exploratory_live_review_snapshot()
        self.assertEqual(snapshot["proposal_status"], "NONE")
        self.assertEqual(
            snapshot["no_member_reason"]["code"],
            "EXPLORATORY_LIVE_NO_ELIGIBLE_PROPOSED_MEMBERS",
        )
        self.assertEqual(snapshot["no_member_reason"]["k"], 0)
        self.assertEqual(snapshot["allocation"]["proposed_members"], 0)
        self.assertIn("EXPLORATORY_LIVE_PURPOSE_REQUIRED", snapshot["blockers"])
    def test_real_snapshot_requires_proposal_risk_metadata(self) -> None:
        self._seed_proposed_selection()
        selection = self.store.load_current_portfolio_selection()
        self.assertIsNotNone(selection)
        assert selection is not None
        successor = dict(selection)
        successor.pop("proposed_allocation_total", None)
        successor.pop("proposed_allocation_risk_snapshot", None)
        successor.pop("proposed_allocation_risk_digest", None)
        successor.pop("allocation_activation", None)
        successor.pop("supersedes_portfolio_selection_id", None)
        successor.pop("selection_hash", None)
        successor.update(
            {
                "selection_id": "selection-risk-omission",
                "portfolio_selection_id": "selection-risk-omission",
            }
        )
        self.store.commit_portfolio_selection(successor, successor["members"])
        snapshot = self.control.exploratory_live_review_snapshot()
        self.assertIn(
            "EXPLORATORY_LIVE_PROPOSED_ALLOCATION_TOTAL_REQUIRED",
            snapshot["blockers"],
        )
        self.assertIn(
            "EXPLORATORY_LIVE_PROPOSED_ALLOCATION_RISK_DIGEST_REQUIRED",
            snapshot["blockers"],
        )



    def test_dashboard_authorization_actions_forward_server_id_and_generation(self) -> None:
        assert self.server._server is not None
        self.store.set_operator_config(
            "execution_authorization_review",
            {
                "status": "DRAFT",
                "authorization_id": "auth-server",
                "generation": 11,
            },
        )
        with patch.object(
            self.control,
            "activate_execution_authorization",
            return_value={"status": "ACTIVE"},
        ) as activate:
            status, result = self._post(
                {
                    "action": "execution_authorization.activate",
                    "confirm": "ACTIVATE EXPLORATORY AUTHORIZATION",
                    "payload": {
                        "authorization_id": "auth-server",
                        "expected_generation": 11,
                    },
                },
                token=self.server._server.control_token,
            )
        self.assertEqual(status, 200)
        self.assertTrue(result["ok"])
        activate.assert_called_once_with(
            "auth-server",
            actor="operator",
            expected_generation=11,
        )

        active = {
            "status": "ACTIVE",
            "authorization_id": "auth-server",
            "generation": 11,
        }
        with patch.object(
            self.control,
            "execution_authorization_snapshot",
            return_value={"active": active},
        ), patch.object(
            self.control,
            "revoke_execution_authorization",
            return_value={"status": "REVOKED"},
        ) as revoke:
            status, result = self._post(
                {
                    "action": "execution_authorization.revoke",
                    "confirm": "REVOKE EXPLORATORY AUTHORIZATION",
                    "payload": {
                        "authorization_id": "auth-server",
                        "expected_generation": 11,
                        "reason": "operator_test",
                    },
                },
                token=self.server._server.control_token,
            )
        self.assertEqual(status, 200)
        self.assertTrue(result["ok"])
        revoke.assert_called_once_with(
            "auth-server",
            actor="operator",
            expected_generation=11,
            reason="operator_test",
        )

    def test_candidate_provenance_is_explicit_and_complete(self) -> None:
        self._seed_candidate()
        data = DashboardData(store=self.store, control=self.control)
        row = data._candidate_row(self.store.load_candidate_lifecycle("candidate-1"))
        self.assertEqual(row["market"], "prediction")
        self.assertEqual(row["provenance"]["dataset_id"], "dataset-1")
        detail = data.strategy_detail("candidate-1")
        self.assertEqual(detail["provenance"]["dataset_version"], "dataset-v1")
        missing = data._candidate_row({"candidate_id": "missing", "payload": {}})
        self.assertEqual(missing["market"], "MISSING PROVENANCE")
        self.assertEqual(missing["provenance"]["status"], "MISSING PROVENANCE")

    def test_canary_eligibility_uses_backend_and_never_trades(self) -> None:
        self._seed_candidate()
        dry_run = CanaryService(self.store, initialize=False).validate_eligibility("candidate-1")
        self.assertTrue(dry_run["eligible"])
        service = Mock()
        service.validate_eligibility.return_value = {"candidate_id": "candidate-1", "eligible": True, "checks": []}
        with patch("axiom.operator.CanaryService", return_value=service):
            verified = self.control.execute("canary.eligibility.verify", "candidate-1")
            marked = self.control.execute(
                "canary.eligibility.mark", "candidate-1", confirm="MARK CANARY ELIGIBLE"
            )
        self.assertTrue(verified["ok"])
        self.assertTrue(marked["ok"])
        service.validate_eligibility.assert_called()
        service.mark_eligible.assert_called_once_with("candidate-1")
        service.arm.assert_not_called()
        service.submit.assert_not_called()

    def test_arm_requires_exact_confirmation_and_has_no_submit_path(self) -> None:
        credentials = _configured_credentials()
        service = Mock()
        service.arm.return_value = {"micro_live_canary": "ARMED", "limits": {"target_notional_usd": "1.00"}}
        venue = Mock()
        venue.geoblock.return_value = {"blocked": False, "close_only": False}
        with patch("axiom.operator.CredentialStore", return_value=credentials), patch(
            "axiom.operator.CanaryService", return_value=service
        ), patch(
            "axiom.operator.PolymarketClobV2Venue", return_value=venue
        ):
            denied = self.control.execute("canary.arm", "candidate-1", confirm="arm")
            armed = self.control.execute("canary.arm", "candidate-1", confirm="ARM")
        self.assertNotEqual(denied.get("ok"), True)
        self.assertEqual(denied["reason"], "EXACT_CONFIRMATION_REQUIRED")
        self.assertTrue(armed["ok"])
        service.arm.assert_called_once()
        service.submit.assert_not_called()
        self.assertNotIn("canary.submit", json.dumps(armed))

    def test_secret_isolation_and_audit_activity(self) -> None:
        credentials = Mock()
        credentials.configured.return_value = True
        credentials.load.return_value = {"private_key": "PRIVATE", "wallet_address": "WALLET"}
        with patch("axiom.operator.CredentialStore", return_value=credentials):
            status = self.control.status()
        encoded = json.dumps(status, default=str)
        self.assertNotIn("PRIVATE", encoded)
        self.assertNotIn("WALLET", encoded)
        self.assertFalse(status["credentials"]["secret_values_exposed"])
        self.control.execute("hermes.pause")
        self.control.execute("not.allowed")
        actions = self.store.list_operator_actions()
        self.assertGreaterEqual(len(actions), 2)
        activity = self.store.paginate_research_activity(kind="operator", page_size=25)
        self.assertGreaterEqual(activity["total"], 2)
    def test_manual_arm_target_is_distinct_from_persisted_autonomous_winner(self) -> None:
        timestamp = datetime.now(timezone.utc)
        self._seed_candidate("A", timestamp=timestamp)
        candidate_a = dict(self.store.load_candidate_lifecycle("A")["payload"])

        def rankable_payload(candidate_id: str, score: float) -> dict[str, object]:
            payload = {
                **candidate_a,
                "candidate_id": candidate_id,
                "mutation_cluster": f"cluster-{candidate_id}",
                "lineage": [candidate_id],
                "validation_expectancy": score,
                "validation_confidence_lower_bound": score,
                "validation_stability": 0.90,
                "validation_calibration": 0.90,
                "validation_sample_count": 100,
                "validation_trade_count": 50,
                "validation_execution_quality": 0.90,
            }
            return payload

        candidate_a = rankable_payload("A", 0.21)
        self.store.save_candidate_lifecycle(
            "A",
            "FROZEN",
            candidate_a,
            from_stage="FROZEN",
            timestamp=timestamp,
        )
        candidate_b = rankable_payload("B", 0.99)
        self.store.save_candidate_lifecycle(
            "B",
            "IDEA",
            candidate_b,
            timestamp=timestamp,
        )
        self.store.save_candidate_lifecycle(
            "B",
            "FROZEN",
            candidate_b,
            from_stage="IDEA",
            timestamp=timestamp,
        )
        self.store.save_market_scope_resolution(
            resolve_market_scope(
                "B",
                {"market_scope": candidate_b["market_scope"]},
                [
                    {
                        "market_id": "MARKET-1",
                        "condition_id": "MARKET-1-CONDITION",
                        "yes_token_id": "MARKET-1-YES",
                        "no_token_id": "MARKET-1-NO",
                        "metadata_provenance": {
                            "source_type": "CURRENT",
                            "metadata_hash": "sha256:MARKET-1",
                        },
                    }
                ],
                resolved_at=timestamp,
            )
        )
        credentials = _configured_credentials()
        service = CanaryService(
            self.store,
            credentials=credentials,
            clock=lambda: timestamp,
        )
        self.store.polymarket_health = lambda **_: {"grade": "A", "errors": 0}

        ranking = CandidateCanaryRanker(
            self.store,
            service=service,
            clock=lambda: timestamp,
        ).evaluate_and_select(timestamp)
        self.assertEqual(ranking["selected_candidate"], "B")
        self.assertEqual(ranking["winner_id"], "B")
        self.assertEqual(ranking["selection_status"], "CURRENT")
        self.assertTrue(ranking["selection_valid"])
        self.assertEqual(ranking["selected"]["candidate_id"], "B")

        class ArmVenue:
            @staticmethod
            def geoblock() -> dict[str, bool]:
                return {"blocked": False, "close_only": False}

        settings = service.settings.snapshot(now=timestamp)
        armed = service.arm(
            "A",
            venue=ArmVenue(),
            credentials_configured=True,
            config_id=settings["config_id"],
            expected_generation=settings["generation"],
        )
        self.assertEqual(armed["candidate"], "A")
        self.assertEqual(armed["selected_candidate"], "B")
        self.assertEqual(armed["selection_status"], "CURRENT")
        self.assertTrue(armed["selection_valid"])
        self.assertEqual(armed["autonomous"]["selected_candidate"], "B")
        self.assertEqual(armed["autonomous"]["last_selected_candidate"], "B")
        self.assertIsInstance(armed["selected_winner"], dict)
        self.assertEqual(armed["selected_winner"]["candidate_id"], "B")
        self.assertEqual(armed["selected_winner"]["selected_candidate"], "B")
        self.assertEqual(armed["selected_winner"]["last_selected_candidate"], "B")

        check_a = service.check(candidate_id="A", venue=ArmVenue())
        check_b = service.check(candidate_id="B", venue=ArmVenue())
        self.assertNotIn("CANDIDATE_NOT_ARMED", check_a["failures"])
        self.assertIn("CANDIDATE_NOT_ARMED", check_b["failures"])

        with patch.object(
            CredentialStore,
            "safe_projection",
            return_value={
                "configured": False,
                "status": "NOT CONFIGURED",
                "secret_values_exposed": False,
            },
        ):
            dashboard = DashboardData(store=self.store, control=self.control).canary_data()
        self.assertEqual(dashboard["canary"]["candidate"], "A")
        self.assertEqual(dashboard["canary"]["selected_candidate"], "B")
        self.assertEqual(dashboard["canary"]["selection_status"], "CURRENT")
        self.assertTrue(dashboard["canary"]["selection_valid"])
        self.assertEqual(dashboard["autonomous_canary"]["selected_candidate"], "B")
        self.assertEqual(dashboard["autonomous_canary"]["last_selected_candidate"], "B")
        self.assertEqual(dashboard["canary"]["selected_winner"]["candidate_id"], "B")
        self.assertEqual(dashboard["canary"]["selected_winner"]["selected_candidate"], "B")
        self.assertEqual(
            dashboard["canary"]["selected_winner"]["last_selected_candidate"], "B"
        )


    def _exploratory_confirm_context(self) -> tuple[dict[str, object], dict[str, object], dict[str, object]]:
        binding = {
            "strategy_version_id": "strategy-live",
            "candidate_id": "candidate-live",
            "setup_id": "setup-live",
            "setup_version": "setup-v1",
            "setup_hash": "setup-hash",
        }
        context = {
            "selection_id": "selection-live",
            "selection_hash": "selection-hash",
            "policy_id": "policy-live",
            "policy_version": "policy-v1",
            "policy_hash": "policy-hash",
            "setup_bindings": [binding],
            "strategy_versions": ["strategy-live"],
            "scope_hash": "scope-hash",
            "scope_version": "scope-v1",
            "active_settings_hash": "settings-hash",
            "active_settings_generation": 1,
        }
        draft = {
            "authorization_id": "auth-live",
            "generation": 1,
            "status": "DRAFT",
            "purpose": "test exploratory review",
            "exact_strategy_versions": ["strategy-live"],
            "lifetime_budget": "1.00",
            "expires_at": (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat(),
            "stop_rules": {"on_any_blocker": "STOP"},
            **context,
        }
        review = {
            "blockers": [],
            "members": [{"candidate_id": "candidate-live"}],
        }
        return context, draft, review

    def _seed_proposed_selection(self) -> tuple[dict[str, object], dict[str, object]]:
        policy = RollingAdmissionPolicy(
            policy_id="rolling-proposed",
            version="v1",
            global_budget=Decimal("3.00"),
            max_members=3,
            min_completed_outcomes=0,
            min_reliability=Decimal("0"),
        )
        self.store.save_admission_policy(policy.as_dict())
        policy_document = {
            "policy_id": "polymarket-exploratory-live",
            "version": "exploratory-live-v1",
            "mode": "EXPLORATORY_LIVE",
            "target_members": {"minimum": 1, "maximum": 3},
            "paper_only": True,
            "allocation_active": False,
            "canary_armed": False,
        }
        selection = {
            "portfolio_selection_id": "selection-proposed",
            "selection_id": "selection-proposed",
            "policy_id": policy.policy_id,
            "policy_version": policy.version,
            "risk_config_id": "risk-proposed",
            "active_risk_config_id": "risk-proposed",
            "risk_config_generation": 1,
            "active_risk_config_generation": 1,
            "risk_config_hash": "risk-hash",
            "active_risk_config_hash": "risk-hash",
            "global_budget": "3.00",
            "selected_at": "2026-01-01T00:00:00+00:00",
            "review_due_at": "2026-01-02T00:00:00+00:00",
            "status": "PAPER",
            "paper_only": True,
            "operating_policy": policy_document,
            "exploratory_policy": policy_document,
        }
        setup = {
            "setup_id": "momentum:absolute-move-v1:L1:H1:exploratory-live-v1",
            "setup_version": "exploratory-live-v1",
            "setup_policy": policy_document,
        }
        draft = self.control._prepare_rolling_exploratory_scope_draft()
        member = {
            "strategy_version_id": "strategy-proposed",
            "research_trial_id": "trial-proposed",
            "candidate_id": "candidate-proposed",
            "evidence_window_id": "evidence-proposed",
            "allocation": "0",
            "proposed_allocation": "1.00",
            "status": "PAPER",
            "action": "OBSERVE",
            "score": "1",
            "reason": "ELIGIBLE",
            "operational_setup": setup,
            "operational_setup_hash": _operational_setup_hash(setup),
            "draft_bound": True,
            "draft_id": draft["draft_id"],
            "draft_hash": draft["draft_hash"],
            "scope_hash": draft["scope_hash"],
            "scope_version": draft["scope_version"],
            "market_bindings": [
                {
                    "market_id": "MARKET-1",
                    "condition_id": "MARKET-1-CONDITION",
                    "yes_token_id": "MARKET-1-YES",
                    "no_token_id": "MARKET-1-NO",
                }
            ],
            "paper_only": True,
            "allocation_active": False,
            "canary_armed": False,
        }
        member["scope_resolution"] = {
            "scope_hash": draft["scope_hash"],
            "scope_version": draft["scope_version"],
            "resolution_id": "resolution-proposed",
            "matched_markets": [
                {
                    "market_id": "MARKET-1",
                    "condition_id": "MARKET-1-CONDITION",
                    "yes_token_id": "MARKET-1-YES",
                    "no_token_id": "MARKET-1-NO",
                }
            ],
        }
        self.store.save_strategy_version(
            {
                "strategy_version_id": "strategy-proposed",
                "strategy_id": "strategy-proposed",
                "version": "1",
                "code_hash": "strategy-hash",
                "config_hash": "strategy-config-hash",
            }
        )
        candidate_payload = {
            "candidate_id": "candidate-proposed",
            "strategy_version_id": "strategy-proposed",
            "scope_hash": draft["scope_hash"],
            "scope_version": draft["scope_version"],
            "market_scope": draft["scope"],
        }
        self.store.save_candidate_lifecycle(
            "candidate-proposed",
            "IDEA",
            candidate_payload,
        )
        self.store.save_candidate_lifecycle(
            "candidate-proposed",
            "FROZEN",
            candidate_payload,
            from_stage="IDEA",
        )
        self.store.save_research_trial(
            {
                "research_trial_id": "trial-proposed",
                "strategy_version_id": "strategy-proposed",
                "status": "PAPER_FORWARD",
                "candidate_id": "candidate-proposed",
            }
        )
        evidence = RollingEvidence(
            strategy_version_id="strategy-proposed",
            evidence_window_id="evidence-proposed",
            candidate_id="candidate-proposed",
            research_trial_id="trial-proposed",
            available_from=datetime(2025, 12, 25, tzinfo=timezone.utc),
            available_through=datetime(2026, 1, 1, tzinfo=timezone.utc),
            requested_days=7,
            actual_coverage_seconds=Decimal("604800"),
            observation_completeness=Decimal("1"),
            source_class="HISTORICAL",
            requested_source_class="HISTORICAL",
            fee_assumption=Decimal("0"),
            slippage_assumption=Decimal("0"),
            allocated_capital_net_return=Decimal("1"),
            realized_pnl=Decimal("1"),
            unrealized_pnl=Decimal("0"),
            fees=Decimal("0"),
            costs=Decimal("0"),
            drawdown=Decimal("0"),
            completed_outcomes=12,
            reliability=Decimal("0.9"),
            execution_feasibility=True,
            overlap_key="overlap:proposal",
        ).as_dict()
        evidence["execution_feasibility"] = "True"
        evidence["evidence_digest"] = RollingEvidence.from_mapping(evidence).evidence_digest
        self.store.save_strategy_evidence_window(evidence)
        CanaryService(
            self.store,
            credentials=_configured_credentials(),
            initialize=True,
        )
        risk_settings = self.control.risk_settings_snapshot()
        selection.update(
            {
                "risk_config_id": risk_settings.get(
                    "risk_config_id", risk_settings.get("config_id")
                ),
                "active_risk_config_id": risk_settings.get(
                    "risk_config_id", risk_settings.get("config_id")
                ),
                "risk_config_generation": risk_settings.get(
                    "risk_config_generation", risk_settings.get("generation")
                ),
                "active_risk_config_generation": risk_settings.get(
                    "risk_config_generation", risk_settings.get("generation")
                ),
                "risk_config_hash": risk_settings.get(
                    "risk_config_hash", risk_settings.get("config_hash")
                ),
                "active_risk_config_hash": risk_settings.get(
                    "risk_config_hash", risk_settings.get("config_hash")
                ),
            }
        )
        risk_snapshot = self.control._fresh_proposed_risk_capacity(selection)
        selection.update(
            {
                "proposed_allocation_total": member["proposed_allocation"],
                "proposed_allocation_risk_snapshot": risk_snapshot,
                "proposed_allocation_risk_digest": self.control._rolling_canonical_hash(
                    risk_snapshot
                ),
            }
        )
        self.store.commit_portfolio_selection(selection, [member])
        self.control.review_rolling_admission_policy(
            policy.as_dict(),
            actor="test-operator",
        )
        context = {
            "selection_id": "selection-proposed",
            "policy_id": policy.policy_id,
            "policy_version": policy.version,
            "policy_hash": policy.config_hash,
            "active_settings_hash": "settings-hash",
            "active_settings_generation": 1,
        }
        return context, member



    def test_real_store_review_snapshot_shows_unactivated_proposal_without_auth(self) -> None:
        self._seed_proposed_selection()
        snapshot = self.control.exploratory_live_review_snapshot()
        self.assertEqual(snapshot["proposal_status"], "UNACTIVATED")
        self.assertEqual(snapshot["authorization_choices"]["status"], "UNREVIEWED")
        self.assertEqual(snapshot["proposal"]["status"], "UNACTIVATED")
        self.assertEqual(snapshot["members"][0]["allocation"], "1.00")
        self.assertEqual(snapshot["members"][0]["proposed_allocation"], "1.00")
        self.assertFalse(snapshot["members"][0]["allocation_active"])
        self.assertEqual(snapshot["authorization_choices"]["shared_allocation"], "1.00")
        self.assertEqual(snapshot["shared_allocation"], "1.00")
        self.assertTrue(snapshot["session_setup"]["required"])
        self.assertIsNotNone(snapshot["proposal"]["proposed_allocation_total"])
        self.assertIsNotNone(snapshot["proposal"]["proposed_allocation_risk_digest"])
        self.assertNotIn(
            "EXPLORATORY_LIVE_PROPOSED_ALLOCATION_TOTAL_REQUIRED",
            snapshot["blockers"],
        )
        self.assertNotIn(
            "EXPLORATORY_LIVE_PROPOSED_ALLOCATION_RISK_DIGEST_REQUIRED",
            snapshot["blockers"],
        )
        self.assertNotIn("EXPLORATORY_LIVE_RISK_BINDING_STALE", snapshot["blockers"])
        self.assertIsNone(
            self.store.load_active_execution_authorization(
                mode="EXPLORATORY_MICRO_CANARY",
                now=datetime.now(timezone.utc),
            )
        )

    def test_real_http_draft_review_preserves_public_market_bindings(self) -> None:
        self._seed_proposed_selection()
        assert self.server._server is not None
        status, result = self._post(
            {
                "action": "execution_authorization.review",
                "target": "",
                "confirm": "REVIEW EXPLORATORY AUTHORIZATION",
                "payload": {
                    "values": {
                        "purpose": "HTTP DRAFT binding review",
                        "lifetime_budget": "1.00",
                        "expires_at": (
                            datetime.now(timezone.utc) + timedelta(days=1)
                        ).isoformat(),
                        "stop_rules": {"on_any_blocker": "STOP"},
                    }
                },
            },
            token=self.server._server.control_token,
        )
        self.assertEqual(status, 200)
        self.assertTrue(result["ok"])
        self.assertEqual(
            result["result"]["execution_authorization"]["status"],
            "DRAFT",
        )
        assert self.server.url is not None
        with urlopen(self.server.url + "/api/operator", timeout=3) as response:
            payload = json.loads(response.read())
        review = payload["control_status"]["exploratory_live_review"]
        bindings = review["authorization_bindings"]
        expected_market = {
            "market_id": "MARKET-1",
            "condition_id": "MARKET-1-CONDITION",
            "yes_token_id": "MARKET-1-YES",
            "no_token_id": "MARKET-1-NO",
        }
        public_member = bindings["draft_member_bindings"][0]
        self.assertEqual(public_member["market_bindings"][0], expected_market)
        public_setup = bindings["setup_bindings"][0]
        self.assertEqual(public_setup["market_bindings"][0], expected_market)
        persisted_draft = review["authorization"]["draft"]
        self.assertEqual(persisted_draft["status"], "DRAFT")
        self.assertEqual(
            persisted_draft["draft_member_bindings"][0]["market_bindings"][0],
            expected_market,
        )

    def test_real_http_connectivity_target_publishes_fresh_proposed_readiness(self) -> None:
        self._seed_proposed_selection()
        selection = self.store.load_current_portfolio_selection()
        self.assertIsInstance(selection, dict)
        assert isinstance(selection, dict)
        baseline = self.control._selected_market_readiness(
            selection,
            target_candidate_id="candidate-proposed",
        )
        self.assertEqual(baseline["status"], "BLOCKED")
        self.assertEqual(baseline["diagnostics"]["account"], {})
        self.assertEqual(baseline["diagnostics"]["market"], {})

        now = datetime(2026, 1, 2, 12, tzinfo=timezone.utc)
        credentials = _configured_credentials()
        venue = ProposedReadinessVenue()
        assert self.server._server is not None
        with patch("axiom.operator.CredentialStore", return_value=credentials), patch(
            "axiom.operator.PolymarketClobV2Venue",
            return_value=venue,
        ), patch("axiom.operator.utc_now", return_value=now):
            status, result = self._post(
                {
                    "action": "canary.connectivity_check",
                    "target": "candidate-proposed",
                },
                token=self.server._server.control_token,
            )
            self.assertEqual(status, 200)
            self.assertTrue(result["ok"])
            self.assertTrue(result["result"]["connectivity"]["ready"])

            assert self.server.url is not None
            with urlopen(self.server.url + "/api/operator", timeout=3) as response:
                payload = json.loads(response.read())

        review = payload["control_status"]["exploratory_live_review"]
        readiness = review["readiness"]
        self.assertEqual(readiness["status"], "READY")
        self.assertTrue(readiness["fresh"])
        self.assertEqual(readiness["market_id"], "MARKET-1")
        self.assertEqual(readiness["diagnostics"]["account"]["authenticated"], True)
        self.assertFalse(readiness["diagnostics"]["geoblock"]["blocked"])
        self.assertFalse(readiness["diagnostics"]["geoblock"]["close_only"])
        self.assertEqual(readiness["diagnostics"]["balance"]["status"], "OK")
        self.assertEqual(readiness["diagnostics"]["allowance"]["status"], "OK")
        self.assertEqual(readiness["diagnostics"]["market"]["market_id"], "MARKET-1")
        legs = readiness["diagnostics"]["token_readiness"]
        self.assertEqual(
            {(leg["outcome"], leg["token_id"]) for leg in legs},
            {("YES", "MARKET-1-YES"), ("NO", "MARKET-1-NO")},
        )
        leg_by_outcome = {leg["outcome"]: leg for leg in legs}
        self.assertTrue(leg_by_outcome["YES"]["ready"])
        self.assertTrue(leg_by_outcome["YES"]["trade_ready"])
        self.assertTrue(leg_by_outcome["NO"]["ready"])
        self.assertFalse(leg_by_outcome["NO"]["trade_ready"])
        self.assertIn(
            "VENUE_MINIMUM_EXCEEDS_CANARY_TARGET",
            leg_by_outcome["NO"]["trade_blockers"],
        )
        depth = leg_by_outcome["YES"]["diagnostics"]["book"]["depth_assessment"]
        self.assertEqual(depth["action"], "SUITABLE")
        self.assertEqual(depth["requested_quantity"], "5")
        self.assertEqual(depth["required_quantity"], "5")
        self.assertEqual(depth["required_cost"], "0.010")
        self.assertLessEqual(Decimal(depth["required_cost"]), Decimal("1.00"))
        self.assertEqual(review["proposal_status"], "UNACTIVATED")
        self.assertEqual(review["authorization_choices"]["status"], "UNREVIEWED")
        self.assertTrue(review["paper_only"])
        self.assertFalse(review["live_execution"])
        self.assertEqual(
            set(venue.market_context_calls),
            {("MARKET-1", "MARKET-1-YES"), ("MARKET-1", "MARKET-1-NO")},
        )
        self.assertEqual(venue.order_calls, 0)
        self.assertEqual(venue.approval_calls, 0)
        encoded = json.dumps({"response": result, "review": review}, default=str)
        for secret in CONNECTIVITY_SECRET_VALUES:
            self.assertNotIn(secret, encoded)

    def test_real_http_connectivity_checks_every_market_binding_for_each_member(self) -> None:
        self._seed_proposed_selection()
        selection = self.store.load_current_portfolio_selection()
        self.assertIsInstance(selection, dict)
        assert isinstance(selection, dict)
        original_member = selection["members"][0]
        self.assertIsInstance(original_member, dict)
        assert isinstance(original_member, dict)
        market_two = {
            "market_id": "MARKET-2",
            "condition_id": "MARKET-2-CONDITION",
            "yes_token_id": "MARKET-2-YES",
            "no_token_id": "MARKET-2-NO",
        }
        member_one = dict(original_member)
        member_one["market_bindings"] = [
            *list(original_member["market_bindings"]),
            market_two,
        ]
        member_two = {
            **member_one,
            "strategy_version_id": "strategy-proposed-two",
            "research_trial_id": "trial-proposed-two",
            "candidate_id": "candidate-proposed-two",
        }
        selected = {**selection, "members": [member_one, member_two]}
        now = datetime(2026, 1, 2, 12, tzinfo=timezone.utc)
        credentials = _configured_credentials()
        venue = ProposedReadinessVenue(("MARKET-1", "MARKET-2"), detailed_books=True)
        assert self.server._server is not None
        with patch.object(
            self.store,
            "load_current_portfolio_selection",
            return_value=selected,
        ), patch("axiom.operator.CredentialStore", return_value=credentials), patch(
            "axiom.operator.PolymarketClobV2Venue",
            return_value=venue,
        ), patch("axiom.operator.utc_now", return_value=now):
            status, result = self._post(
                {"action": "canary.connectivity_check"},
                token=self.server._server.control_token,
            )
            persisted_connectivity = self.control.status()["connectivity"]
        self.assertEqual(status, 200)
        self.assertTrue(result["ok"])
        connectivity = result["result"]["connectivity"]
        self.assertTrue(connectivity["ready"])
        legs = connectivity["diagnostics"]["token_readiness"]
        expected_pairs = {
            ("MARKET-1", "YES", "MARKET-1-YES"),
            ("MARKET-1", "NO", "MARKET-1-NO"),
            ("MARKET-2", "YES", "MARKET-2-YES"),
            ("MARKET-2", "NO", "MARKET-2-NO"),
        }
        self.assertEqual(
            {
                (leg["market_id"], leg["outcome"], leg["token_id"])
                for leg in legs
            },
            expected_pairs,
        )
        self.assertEqual(len(legs), len(expected_pairs) * 2)
        yes_leg = next(
            leg
            for leg in legs
            if leg["market_id"] == "MARKET-1" and leg["outcome"] == "YES"
        )
        self.assertEqual(len(yes_leg["diagnostics"]["book"]["bids"]), 1)
        self.assertEqual(len(yes_leg["diagnostics"]["book"]["asks"]), 32)
        self.assertEqual(
            yes_leg["diagnostics"]["book"]["bids"][0]["price"],
            "0.001",
        )
        self.assertEqual(
            yes_leg["diagnostics"]["book"]["asks"][0]["price"],
            "0.002",
        )
        stored = self.store.get_operator_config(CANARY_CONNECTIVITY_CONFIG_KEY, {})
        self.assertLessEqual(
            len(json.dumps(stored, separators=(",", ":"), default=str)),
            16_384,
        )
        stored_legs = stored["diagnostics"]["token_readiness"]
        self.assertEqual(len(stored_legs), len(legs))
        self.assertEqual(
            {
                (leg["market_id"], leg["outcome"], leg["token_id"])
                for leg in stored_legs
            },
            expected_pairs,
        )
        stored_yes = next(
            leg
            for leg in stored_legs
            if leg["market_id"] == "MARKET-1" and leg["outcome"] == "YES"
        )
        self.assertEqual(
            stored_yes["diagnostics"]["book"]["bids"][0]["price"],
            "0.001",
        )
        self.assertEqual(
            stored_yes["diagnostics"]["book"]["asks"][0]["price"],
            "0.002",
        )
        stored_no = next(
            leg
            for leg in stored_legs
            if leg["market_id"] == "MARKET-1" and leg["outcome"] == "NO"
        )
        self.assertEqual(
            stored_no["diagnostics"]["book"]["bids"][0]["price"],
            "0.998",
        )
        self.assertEqual(
            stored_no["diagnostics"]["book"]["asks"][0]["price"],
            "0.999",
        )
        for leg in stored_legs:
            market = leg["diagnostics"]["market"]
            book = leg["diagnostics"]["book"]
            self.assertEqual(market["fee_bps"], "0")
            self.assertEqual(book["min_order_size"], "5")
            self.assertEqual(book["tick_size"], "0.001")
            self.assertEqual(len(book["bids"]), 1)
            self.assertEqual(len(book["asks"]), 1)
            self.assertIn("rules", book)
            if leg["outcome"] == "YES":
                self.assertIn("depth_assessment", book)
            else:
                self.assertFalse(leg["trade_ready"])
                self.assertIn(
                    "VENUE_MINIMUM_EXCEEDS_CANARY_TARGET",
                    leg["trade_blockers"],
                )
        self.assertEqual(persisted_connectivity["status"], "READY")
        persisted_legs = persisted_connectivity["diagnostics"]["token_readiness"]
        self.assertEqual(len(persisted_legs), len(expected_pairs) * 2)
        self.assertEqual(
            {
                (leg["market_id"], leg["outcome"], leg["token_id"])
                for leg in persisted_legs
            },
            expected_pairs,
        )
        persisted_bbo = {
            (leg["market_id"], leg["outcome"]): (
                leg["diagnostics"]["book"]["bids"][0]["price"],
                leg["diagnostics"]["book"]["asks"][0]["price"],
            )
            for leg in persisted_legs
        }
        self.assertEqual(
            persisted_bbo,
            {
                ("MARKET-1", "YES"): ("0.001", "0.002"),
                ("MARKET-1", "NO"): ("0.998", "0.999"),
                ("MARKET-2", "YES"): ("0.001", "0.002"),
                ("MARKET-2", "NO"): ("0.998", "0.999"),
            },
        )
        self.assertEqual(set(venue.market_context_calls), {
            (market_id, token_id)
            for market_id, _outcome, token_id in expected_pairs
        })
        self.assertEqual(len(venue.market_context_calls), len(expected_pairs))
        self.assertEqual(venue.order_calls, 0)
        self.assertEqual(venue.approval_calls, 0)

    def test_real_http_readiness_rejects_immutable_successor_and_stale_proof(self) -> None:
        self._seed_proposed_selection()
        now = datetime(2026, 1, 2, 12, tzinfo=timezone.utc)
        credentials = _configured_credentials()
        venue = ProposedReadinessVenue()
        assert self.server._server is not None
        with patch("axiom.operator.CredentialStore", return_value=credentials), patch(
            "axiom.operator.PolymarketClobV2Venue",
            return_value=venue,
        ), patch("axiom.operator.utc_now", return_value=now):
            status, result = self._post(
                {
                    "action": "canary.connectivity_check",
                    "target": "candidate-proposed",
                },
                token=self.server._server.control_token,
            )
        self.assertEqual(status, 200)
        self.assertTrue(result["ok"])
        self.assertTrue(result["result"]["connectivity"]["ready"])
        predecessor = self.store.load_current_portfolio_selection()
        self.assertIsInstance(predecessor, dict)
        assert isinstance(predecessor, dict)
        predecessor_id = predecessor["selection_id"]
        successor = dict(predecessor)
        successor_id = "selection-proposed-wrong-successor"
        successor.update(
            {
                "selection_id": successor_id,
                "portfolio_selection_id": successor_id,
                "supersedes_portfolio_selection_id": predecessor_id,
            }
        )
        successor.pop("selection_hash", None)
        wrong_member = dict(successor["members"][0])
        wrong_member["current_market_binding"] = {
            "market_id": "MARKET-WRONG",
            "condition_id": "MARKET-WRONG-CONDITION",
            "token_id": "MARKET-WRONG-YES",
            "yes_token_id": "MARKET-WRONG-YES",
            "no_token_id": "MARKET-WRONG-NO",
        }
        successor["members"] = [wrong_member]
        self.store.commit_portfolio_selection(successor, [wrong_member])
        current = self.store.load_current_portfolio_selection()
        self.assertEqual(current["selection_id"], successor_id)
        self.assertNotEqual(current["selection_id"], predecessor["selection_id"])

        with patch("axiom.operator.utc_now", return_value=now):
            assert self.server.url is not None
            with urlopen(self.server.url + "/api/operator", timeout=3) as response:
                payload = json.loads(response.read())
        readiness = payload["control_status"]["exploratory_live_review"]["readiness"]
        self.assertIn(
            "SELECTED_MARKET_READINESS_BINDING_STALE",
            readiness["blockers"],
        )

        with patch(
            "axiom.operator.utc_now",
            return_value=now + timedelta(seconds=61),
        ):
            with urlopen(self.server.url + "/api/operator", timeout=3) as response:
                stale_payload = json.loads(response.read())
        stale_readiness = stale_payload["control_status"]["exploratory_live_review"]["readiness"]
        self.assertEqual(stale_readiness["status"], "BLOCKED")
        self.assertIn("SELECTED_MARKET_READINESS_STALE", stale_readiness["blockers"])
        self.assertEqual(venue.order_calls, 0)
        self.assertEqual(venue.approval_calls, 0)


    def test_real_store_selection_reason_drives_no_member_root(self) -> None:
        self._seed_proposed_selection()
        prepared = self.control._prepare_reviewed_proposed_selection()
        self.assertIsNotNone(prepared)
        assert prepared is not None
        selection = dict(prepared)
        selection.pop("allocation_activation", None)
        selection.pop("supersedes_portfolio_selection_id", None)
        selection.pop("selection_hash", None)
        selection.update(
            {
                "selection_id": "selection-no-member-root",
                "portfolio_selection_id": "selection-no-member-root",
                "status": "PAPER",
                "k": 0,
                "global_budget": "0",
                "members": [],
                "selected_members": [],
                "reasons": ["NO_ELIGIBLE_PROPOSED_MEMBERS"],
                "actionable_reasons": {
                    "NO_ELIGIBLE_PROPOSED_MEMBERS": "Run paper evaluation first"
                },
            }
        )
        self.store.commit_portfolio_selection(selection, [])
        self.store.set_operator_config(
            ROLLING_EXPLORATORY_PROPOSAL_CONFIG_KEY,
            {"selection_id": "selection-no-member-root"},
        )
        snapshot = self.control.exploratory_live_review_snapshot()
        reason = snapshot["no_member_reason"]
        self.assertEqual(
            reason["code"],
            "EXPLORATORY_LIVE_NO_ELIGIBLE_PROPOSED_MEMBERS",
        )
        self.assertEqual(reason["k"], 0)
        self.assertEqual(reason["global_budget"], "0")
        self.assertEqual(
            reason["selection_reasons"],
            ["NO_ELIGIBLE_PROPOSED_MEMBERS"],
        )
        self.assertEqual(
            reason["actionable_reasons"]["NO_ELIGIBLE_PROPOSED_MEMBERS"],
            "Run paper evaluation first",
        )
        self.assertEqual(snapshot["proposal_status"], "NONE")

    def test_real_store_review_persists_draft_without_activation(self) -> None:
        self._seed_proposed_selection()
        reviewed = self.control.review_execution_authorization(
            {
                "purpose": "isolated transition review",
                "lifetime_budget": "1.00",
                "expires_at": (datetime.now(timezone.utc) + timedelta(days=1)).isoformat(),
                "stop_rules": {
                    "on_any_blocker": "STOP",
                    "halt_on_unknown_execution": True,
                },
            },
            actor="transition-test",
        )
        self.assertEqual(reviewed["status"], "DRAFT")
        self.assertEqual(reviewed["draft"]["status"], "DRAFT")
        self.assertEqual(reviewed["draft"]["purpose"], "isolated transition review")
        self.assertIsNone(
            self.store.load_active_execution_authorization(
                mode="EXPLORATORY_MICRO_CANARY",
                now=datetime.now(timezone.utc),
            )
        )
        persisted_selection = self.store.load_current_portfolio_selection()
        self.assertIsNotNone(persisted_selection)
        assert persisted_selection is not None
        self.assertEqual(persisted_selection["members"][0]["allocation"], "0")
        self.assertEqual(persisted_selection["members"][0]["proposed_allocation"], "1.00")
        self.assertFalse(persisted_selection["members"][0]["allocation_active"])
        paper_review = self.control.exploratory_live_review_snapshot()
        self.assertEqual(paper_review["members"][0]["allocation"], "1.00")
        self.assertEqual(paper_review["members"][0]["proposed_allocation"], "1.00")
        self.assertFalse(paper_review["members"][0]["allocation_active"])
    def test_real_store_review_confirm_exact_then_stale_binding_stays_safe(self) -> None:
        self._seed_proposed_selection()
        reviewed = self.control.review_execution_authorization(
            {
                "purpose": "same-fixture final review",
                "lifetime_budget": "1.00",
                "expires_at": (datetime.now(timezone.utc) + timedelta(days=1)).isoformat(),
                "stop_rules": {"on_any_blocker": "STOP"},
            },
            actor="transition-test",
        )
        draft = reviewed["draft"]
        disclosure = self.control.exploratory_live_review_snapshot()
        self.assertEqual(disclosure["proposal_status"], "UNACTIVATED")
        self.assertEqual(
            disclosure["authorization_bindings"]["selection_id"],
            draft["selection_id"],
        )
        self.assertEqual(
            disclosure["authorization_bindings"]["selection_hash"],
            draft["selection_hash"],
        )
        unavailable_credentials = Mock()
        unavailable_credentials.configured.return_value = False
        with patch(
            "axiom.operator.CredentialStore",
            return_value=unavailable_credentials,
        ), patch("axiom.operator.PolymarketClobV2Venue") as venue_factory:
            with self.assertRaises(OperatorControlError):
                self.control.confirm_exploratory_live(
                    {"confirmation": "CONFIRM EXPLORATORY LIVE"}
                )
        unavailable_credentials.configured.assert_called_with(allow_environment=False)
        venue_factory.assert_not_called()
        self.assertEqual(
            self.store.get_operator_config("execution_authorization_review", {})["status"],
            "DRAFT",
        )
        self.assertIsNone(
            self.store.load_active_execution_authorization(
                mode="EXPLORATORY_MICRO_CANARY",
                now=datetime.now(timezone.utc),
            )
        )

        stale = self.store.load_current_portfolio_selection()
        self.assertIsNotNone(stale)
        assert stale is not None
        stale = dict(stale)
        stale.update(
            {
                "selection_id": "selection-stale-after-review",
                "portfolio_selection_id": "selection-stale-after-review",
            }
        )
        self.store.commit_portfolio_selection(stale, stale["members"])
        self.store.set_operator_config(
            ROLLING_EXPLORATORY_PROPOSAL_CONFIG_KEY,
            {
                "selection_id": "selection-stale-after-review",
                "selection_hash": stale.get("selection_hash", ""),
            },
        )
        credentials = _configured_credentials()
        venue = ProposedReadinessVenue()
        with patch("axiom.operator.CredentialStore", return_value=credentials), patch(
            "axiom.operator.PolymarketClobV2Venue",
            return_value=venue,
        ):
            with patch.object(
                self.control,
                "_selected_market_readiness",
                return_value={"status": "READY", "blockers": []},
            ):
                with self.assertRaisesRegex(
                    OperatorControlError,
                    "^EXPLORATORY_LIVE_AUTHORIZATION_BINDING_STALE$",
                ):
                    self.control.confirm_exploratory_live(
                        {"confirmation": "CONFIRM EXPLORATORY LIVE"}
                    )
        self.assertEqual(
            self.store.get_operator_config("execution_authorization_review", {})["status"],
            "DRAFT",
        )
        self.assertEqual(
            self.store.connection.execute(
                "SELECT COUNT(*) FROM canary_ledger WHERE status IN ('SUBMITTING','SUBMITTED')"
            ).fetchone()[0],
            0,
        )


    def test_real_store_confirm_without_auth_does_not_arm_or_submit(self) -> None:
        self._seed_proposed_selection()
        CanaryService(
            self.store,
            credentials=_configured_credentials(),
            initialize=True,
        )
        with self.assertRaisesRegex(
            OperatorControlError,
            "^EXPLORATORY_LIVE_AUTHORIZATION_REQUIRED$",
        ):
            self.control.confirm_exploratory_live(
                {"confirmation": "CONFIRM EXPLORATORY LIVE"}
            )
        control_row = self.store.connection.execute(
            "SELECT state FROM canary_control WHERE singleton=1"
        ).fetchone()
        if control_row is not None:
            self.assertNotEqual(str(control_row["state"]).upper(), "ARMED")
        self.assertEqual(
            self.store.connection.execute(
                "SELECT COUNT(*) FROM canary_ledger WHERE status IN ('SUBMITTING','SUBMITTED')"
            ).fetchone()[0],
            0,
        )

    def test_proposed_allocation_promotion_failure_leaves_paper_selection_unchanged(self) -> None:
        context, _ = self._seed_proposed_selection()
        before = self.store.load_current_portfolio_selection()
        self.assertIsNotNone(before)
        prepared = self.control._prepare_reviewed_proposed_selection()
        self.assertIsNotNone(prepared)
        assert prepared is not None
        prepared_context = dict(
            context,
            selection_id=prepared["selection_id"],
            selection_hash=prepared["selection_hash"],
        )
        with patch.object(
            self.control,
            "_promote_portfolio_current_pointer",
            side_effect=RuntimeError("activation promotion failed"),
        ):
            with self.assertRaisesRegex(RuntimeError, "activation promotion failed"):
                self.control._activate_reviewed_proposed_selection(
                    context=prepared_context,
                    authorization={"authorization_id": "auth-proposed"},
                    actor="test-operator",
                )
        self.assertEqual(self.store.load_current_portfolio_selection(), before)
    def test_prepared_successor_keeps_identity_until_reconciliation(self) -> None:
        context, _ = self._seed_proposed_selection()
        predecessor = self.store.load_current_portfolio_selection()
        assert predecessor is not None
        prepared = self.control._prepare_reviewed_proposed_selection()
        assert prepared is not None
        self.assertNotEqual(prepared["selection_id"], predecessor["selection_id"])
        self.assertEqual(prepared["members"][0]["allocation"], "1.00")
        self.assertEqual(prepared["members"][0]["status"], "PAPER")
        self.assertFalse(prepared["members"][0]["allocation_active"])
        self.assertTrue(prepared["paper_only"])
        prepared_context = dict(
            context,
            selection_id=prepared["selection_id"],
            selection_hash=prepared["selection_hash"],
        )
        activated = self.control._activate_reviewed_proposed_selection(
            context=prepared_context,
            authorization={"authorization_id": "auth-prepared"},
            actor="test-operator",
        )
        assert activated is not None
        self.assertEqual(activated["selection_id"], prepared["selection_id"])
        current = self.store.load_current_portfolio_selection()
        assert current is not None
        self.assertEqual(current["selection_id"], prepared["selection_id"])
        self.assertEqual(current["selection_hash"], prepared["selection_hash"])
        self.assertEqual(current["members"][0]["allocation"], "1.00")
        self.assertTrue(current["members"][0]["allocation_active"])
        self.assertFalse(current["paper_only"])
        self.assertIsNotNone(
            self.store.get_operator_config(
                "canary_selection_binding_rollback", None
            )
        )

    def test_prepared_activation_rejects_fresh_obligation_change(self) -> None:
        context, _ = self._seed_proposed_selection()
        prepared = self.control._prepare_reviewed_proposed_selection()
        assert prepared is not None
        risk_snapshot = dict(prepared["proposed_allocation_risk_snapshot"])
        risk_snapshot.update(
            {
                "global_budget": "3.00",
                "active_obligations": "0",
                "uncovered_obligations": "0",
                "external_obligations": "0",
                "available_budget": "3.00",
                "runtime_accounting_available": True,
                "runtime_accounting_metadata": {"available": True},
            }
        )
        prepared_with_risk = dict(prepared)
        prepared_with_risk["proposed_allocation_risk_snapshot"] = risk_snapshot
        prepared_with_risk["proposed_allocation_risk_digest"] = (
            self.control._rolling_canonical_hash(risk_snapshot)
        )
        accounting = {
            "rolling_global_reserved_usd": "2.00",
            "rolling_strategy_reserved_usd": {"external-strategy": "2.00"},
            "rolling_strategy_allocations": {},
        }
        with patch.object(
            self.store,
            "load_portfolio_selection",
            return_value=prepared_with_risk,
        ), patch.object(
            self.store,
            "canary_risk_accounting",
            return_value=accounting,
        ):
            with self.assertRaisesRegex(
                OperatorControlError, "EXPLORATORY_LIVE_RISK_CAPACITY_CHANGED"
            ):
                self.control._activate_reviewed_proposed_selection(
                    context={
                        **context,
                        "selection_id": prepared["selection_id"],
                        "selection_hash": prepared["selection_hash"],
                    },
                    authorization={"authorization_id": "auth-obligation-change"},
                    actor="test-operator",
                )

    def test_prepared_activation_rollback_restores_canary_singleton_and_binding(self) -> None:
        context, _ = self._seed_proposed_selection()
        policy_before = self.store.get_operator_config(
            "rolling_admission_policy_active", None
        )
        prepared = self.control._prepare_reviewed_proposed_selection()
        assert prepared is not None
        activated = self.control._activate_reviewed_proposed_selection(
            context={
                **context,
                "selection_id": prepared["selection_id"],
                "selection_hash": prepared["selection_hash"],
            },
            authorization={"authorization_id": "auth-rollback"},
            actor="test-operator",
        )
        self.assertIsInstance(activated, dict)
        assert isinstance(activated, dict)
        marker = self.store.get_operator_config(
            "canary_selection_binding_rollback", None
        )
        self.assertIsInstance(marker, dict)
        self.assertEqual(marker["rolling_admission_policy_before"], policy_before)
        active_policy = self.store.get_operator_config(
            "rolling_admission_policy_active", None
        )
        self.assertIsInstance(active_policy, dict)
        assert isinstance(active_policy, dict)
        self.assertEqual(active_policy["policy_id"], "rolling-proposed")
        self.assertEqual(
            active_policy["risk_config_id"],
            self.control._rolling_risk_binding()["risk_config_id"],
        )
        self.control._restore_canary_selection_binding(
            activated["_canary_selection_binding_before"]
        )
        self.assertIsNone(
            self.store.connection.execute(
                "SELECT candidate_id FROM canary_selection WHERE singleton=1"
            ).fetchone()
        )
        self.assertIsNone(
            self.store.get_operator_config("canary_selection_binding", None)
        )
        self.assertTrue(self.control._reconcile_pending_canary_selection_binding())
        self.assertEqual(
            self.store.get_operator_config("rolling_admission_policy_active", None),
            policy_before,
        )


    def test_system_bootstrap_activates_exact_exploratory_policy_idempotently(self) -> None:
        active = self.store.get_operator_config("rolling_admission_policy_active", None)
        self.assertIsInstance(active, dict)
        assert isinstance(active, dict)
        self.assertEqual(active["system_bootstrap_id"], "system:polymarket-exploratory-live")
        self.assertEqual(active["system_bootstrap_version"], "exploratory-live-bootstrap-v1")
        self.assertTrue(active["paper_only"])
        self.assertFalse(active["live_execution"])
        self.assertFalse(active["allocation_active"])
        self.assertFalse(active["canary_armed"])
        operating = active["operating_policy"]
        self.assertEqual(operating["policy_id"], "polymarket-exploratory-live")
        self.assertEqual(operating["version"], "exploratory-live-v1")
        self.assertEqual(operating["pool_cap"], 10)
        self.assertEqual(operating["target_members"], {"minimum": 1, "maximum": 3})
        self.assertEqual(operating["paper_only"], True)
        self.assertEqual(operating["allocation_active"], False)
        self.assertEqual(operating["canary_armed"], False)
        before_job = self.store.get_operator_job("rolling_admission_policy_active")
        restarted = OperatorControlPlane(self.store)
        self.assertEqual(
            restarted.store.get_operator_config("rolling_admission_policy_active", None),
            active,
        )
        self.assertEqual(
            restarted.store.get_operator_job("rolling_admission_policy_active"),
            before_job,
        )

    def test_rolling_scope_draft_is_unactivated_and_idempotent(self) -> None:
        active_before = self.store.get_operator_config(
            "rolling_admission_policy_active", None
        )
        draft_before = self.store.get_operator_config(
            ROLLING_EXPLORATORY_SCOPE_DRAFT_CONFIG_KEY, None
        )
        first = self.control.rolling_exploratory_scope_draft()
        second = self.control.rolling_exploratory_scope_draft()
        self.assertEqual(first, second)
        self.assertEqual(
            self.store.get_operator_config(
                ROLLING_EXPLORATORY_SCOPE_DRAFT_CONFIG_KEY, None
            ),
            draft_before,
        )
        self.assertEqual(first, second)
        self.assertEqual(first["status"], "DRAFT")
        self.assertEqual(first["draft_id"], "rolling-exploratory-scope-draft:polymarket:standard:v1")
        self.assertEqual(first["scope"]["mode"], "RULE_BASED_MARKETS")
        self.assertEqual(first["scope"]["instrument"], "POLYMARKET")
        self.assertEqual(first["scope"]["categories"], [])
        self.assertEqual(first["scope"]["market_ids"], [])
        self.assertEqual(first["supported_market_types"], ["prediction"])
        self.assertEqual(first["category_restriction"], {"mode": "UNRESTRICTED", "categories": []})
        self.assertTrue(first["paper_only"])
        self.assertFalse(first["live_execution"])
        self.assertFalse(first["allocation_active"])
        self.assertFalse(first["canary_armed"])
        self.assertIn("COMBO", first["exclusions"])
        self.assertEqual(first["trace_requirements"]["discovery"]["pool_cap"], 10)
        self.assertEqual(
            self.store.get_operator_config("rolling_admission_policy_active", None),
            active_before,
        )

    def test_rolling_scope_draft_full_hash_is_accepted_with_nested_templates(self) -> None:
        draft = self.control._prepare_rolling_exploratory_scope_draft()
        unsigned = {
            key: value for key, value in draft.items() if key != "draft_hash"
        }
        expected = "sha256:" + hashlib.sha256(
            json.dumps(
                unsigned,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
                allow_nan=False,
                default=str,
            ).encode("utf-8")
        ).hexdigest()
        self.assertEqual(
            draft["trace_requirements"]["evaluation"]["templates"],
            ["momentum", "mean_reversion"],
        )
        self.assertEqual(draft["draft_hash"], expected)
        self.assertEqual(_validated_scope_draft(draft), draft)
        self.assertNotEqual(
            draft["draft_hash"],
            "sha256:" + self.control._rolling_canonical_hash(unsigned),
        )

    def test_rolling_scope_draft_repairs_only_known_unactivated_legacy_hash(self) -> None:
        active_before = self.store.get_operator_config(
            "rolling_admission_policy_active", None
        )
        draft = self.control._prepare_rolling_exploratory_scope_draft()
        unsigned = {
            key: value for key, value in draft.items() if key != "draft_hash"
        }
        legacy = dict(unsigned)
        legacy["draft_hash"] = "sha256:" + self.control._rolling_canonical_hash(unsigned)
        self.assertNotEqual(legacy["draft_hash"], draft["draft_hash"])
        self.store.set_operator_config(
            ROLLING_EXPLORATORY_SCOPE_DRAFT_CONFIG_KEY, legacy
        )

        restarted = OperatorControlPlane(self.store, system_bootstrap_enabled=True)
        repaired = self.store.get_operator_config(
            ROLLING_EXPLORATORY_SCOPE_DRAFT_CONFIG_KEY, None
        )
        self.assertEqual(repaired, restarted.rolling_exploratory_scope_draft())
        self.assertEqual(repaired["draft_hash"], draft["draft_hash"])
        self.assertEqual(_validated_scope_draft(repaired), repaired)
        self.assertEqual(
            self.store.get_operator_config("rolling_admission_policy_active", None),
            active_before,
        )

        tampered = dict(legacy)
        tampered_requirements = dict(tampered["trace_requirements"])
        tampered_evaluation = dict(tampered_requirements["evaluation"])
        tampered_evaluation["templates"] = ["tampered"]
        tampered_requirements["evaluation"] = tampered_evaluation
        tampered["trace_requirements"] = tampered_requirements
        self.store.set_operator_config(
            ROLLING_EXPLORATORY_SCOPE_DRAFT_CONFIG_KEY, tampered
        )
        with self.assertRaisesRegex(
            OperatorControlError, "^EXPLORATORY_SCOPE_DRAFT_MISMATCH$"
        ):
            self.control._prepare_rolling_exploratory_scope_draft()
        self.assertEqual(
            self.store.get_operator_config(
                ROLLING_EXPLORATORY_SCOPE_DRAFT_CONFIG_KEY, None
            ),
            tampered,
        )

        active = dict(legacy)
        active.update(
            {
                "status": "ACTIVE",
                "paper_only": False,
                "live_execution": True,
                "allocation_active": True,
                "canary_armed": True,
            }
        )
        active_unsigned = {
            key: value for key, value in active.items() if key != "draft_hash"
        }
        active["draft_hash"] = (
            "sha256:" + self.control._rolling_canonical_hash(active_unsigned)
        )
        self.store.set_operator_config(
            ROLLING_EXPLORATORY_SCOPE_DRAFT_CONFIG_KEY, active
        )
        with self.assertRaisesRegex(
            OperatorControlError, "^EXPLORATORY_SCOPE_DRAFT_MISMATCH$"
        ):
            self.control._prepare_rolling_exploratory_scope_draft()
        self.assertEqual(
            self.store.get_operator_config(
                ROLLING_EXPLORATORY_SCOPE_DRAFT_CONFIG_KEY, None
            ),
            active,
        )


    def test_rolling_scope_draft_mismatch_fails_closed_without_overwrite(self) -> None:
        self.control._prepare_rolling_exploratory_scope_draft()
        draft = self.control.rolling_exploratory_scope_draft()
        altered = dict(draft)
        altered["draft_hash"] = "sha256:altered"
        self.store.set_operator_config(
            ROLLING_EXPLORATORY_SCOPE_DRAFT_CONFIG_KEY, altered
        )
        with self.assertRaisesRegex(
            OperatorControlError, "^EXPLORATORY_SCOPE_DRAFT_MISMATCH$"
        ):
            self.control.rolling_exploratory_scope_draft()
        self.assertEqual(
            self.store.get_operator_config(
                ROLLING_EXPLORATORY_SCOPE_DRAFT_CONFIG_KEY, None
            ),
            altered,
        )
    def test_old_scope_active_or_proposed_member_fails_closed(self) -> None:
        self._seed_proposed_selection()
        for case in ("ACTIVE", "PROPOSED"):
            selection = self.store.load_current_portfolio_selection()
            self.assertIsInstance(selection, dict)
            assert isinstance(selection, dict)
            old_member = dict(selection["members"][0])
            for key in (
                "draft_bound",
                "draft_id",
                "draft_hash",
                "scope_hash",
                "scope_version",
                "market_bindings",
            ):
                old_member.pop(key, None)
            successor_id = f"selection-old-scope-{case.lower()}"
            selection["portfolio_selection_id"] = successor_id
            selection["selection_id"] = successor_id
            if case == "ACTIVE":
                old_member.pop("proposed_allocation", None)
                old_member["status"] = "ACTIVE"
                old_member["allocation"] = "1.00"
                old_member["allocation_active"] = True
                old_member["paper_only"] = False
                old_member["canary_armed"] = True
                selection["k"] = 1
                selection["status"] = "ACTIVE"
                selection["paper_only"] = False
                selection["allocation_active"] = True
                selection["canary_armed"] = True
            else:
                old_member["status"] = "PAPER"
                old_member["allocation"] = "0"
                old_member["proposed_allocation"] = "1.00"
                old_member["allocation_active"] = False
                old_member["paper_only"] = True
                old_member["canary_armed"] = False
                selection["k"] = 0
                selection["status"] = "PAPER"
                selection["paper_only"] = True
                selection["allocation_active"] = False
                selection["canary_armed"] = False
            self.store.commit_portfolio_selection(selection, [old_member])
            with self.assertRaisesRegex(
                OperatorControlError,
                "^EXPLORATORY_SCOPE_DRAFT_MEMBER_BINDING_REQUIRED$",
            ):
                self.control._prepare_reviewed_proposed_selection()

    def test_genuine_draft_member_exact_binding_is_reviewable(self) -> None:
        self._seed_proposed_selection()
        persisted = self.store.load_current_portfolio_selection()
        self.assertIsInstance(persisted, dict)
        assert isinstance(persisted, dict)
        persisted_member = persisted["members"][0]
        self.assertEqual(
            persisted_member["market_bindings"][0]["market_id"], "MARKET-1"
        )
        context = self.control._authorization_context(require_draft_members=True)
        bindings = context["draft_member_bindings"]
        self.assertEqual(len(bindings), 1)
        binding = bindings[0]
        self.assertTrue(binding["draft_bound"])
        self.assertEqual(binding["draft_id"], context["scope_draft_id"])
        self.assertEqual(binding["draft_hash"], context["scope_draft_hash"])
        self.assertEqual(binding["scope_hash"], context["scope_hash"])
        self.assertEqual(binding["scope_version"], context["scope_version"])
        self.assertEqual(binding["market_bindings"][0]["market_id"], "MARKET-1")

    def test_execution_authorization_rejects_unknown_caller_fields(self) -> None:
        with self.assertRaisesRegex(
            OperatorControlError,
            "^UNSUPPORTED_EXECUTION_AUTHORIZATION_FIELDS$",
        ):
            self.control.review_execution_authorization({"unexpected": "value"})



    def test_system_bootstrap_rejects_altered_exploratory_hash(self) -> None:
        operating = dict(
            self.store.get_operator_config("rolling_exploratory_operating_policy", {})
        )
        operating["config_hash"] = "sha256:altered"
        self.store.set_operator_config("rolling_exploratory_operating_policy", operating)
        with self.assertRaisesRegex(
            OperatorControlError, "ROLLING_POLICY_BOOTSTRAP_HASH_MISMATCH"
        ):
            self.control._bootstrap_system_exploratory_admission_policy()
        self.assertEqual(
            self.store.get_operator_config("rolling_admission_policy_active", None)[
                "system_bootstrap_id"
            ],
            "system:polymarket-exploratory-live",
        )

    def test_system_bootstrap_rejects_risk_drift_and_live_flags(self) -> None:
        active = dict(self.store.get_operator_config("rolling_admission_policy_active", {}))
        active["risk_config_hash"] = "sha256:drift"
        self.store.set_operator_config("rolling_admission_policy_active", active)
        with self.assertRaisesRegex(
            OperatorControlError, "ROLLING_POLICY_BOOTSTRAP_RISK_DRIFT"
        ):
            self.control._bootstrap_system_exploratory_admission_policy()
        active["risk_config_hash"] = self.control.risk_settings_snapshot()["config_hash"]
        active["live_execution"] = True
        self.store.set_operator_config("rolling_admission_policy_active", active)
        with self.assertRaisesRegex(
            OperatorControlError, "ROLLING_POLICY_BOOTSTRAP_DRIFT"
        ):
            self.control._bootstrap_system_exploratory_admission_policy()

    def test_system_bootstrap_rejects_arbitrary_policy_and_preserves_review(self) -> None:
        review = {
            "policy_id": "arbitrary-reviewed-policy",
            "version": "v1",
            "config_hash": "sha256:arbitrary",
            "status": "REVIEWED",
        }
        self.store.set_operator_config("rolling_admission_policy_review", review)
        active = dict(self.store.get_operator_config("rolling_admission_policy_active", {}))
        active["policy_id"] = "arbitrary-reviewed-policy"
        self.store.set_operator_config("rolling_admission_policy_active", active)
        with self.assertRaisesRegex(
            OperatorControlError, "ROLLING_POLICY_BOOTSTRAP_DRIFT"
        ):
            self.control._bootstrap_system_exploratory_admission_policy()
        self.assertEqual(
            self.store.get_operator_config("rolling_admission_policy_review", None),
            review,
        )

    def test_system_bootstrap_ctor_blocker_is_visible_in_status(self) -> None:
        active = dict(self.store.get_operator_config("rolling_admission_policy_active", {}))
        active["live_execution"] = True
        self.store.set_operator_config("rolling_admission_policy_active", active)
        blocked = OperatorControlPlane(self.store, system_bootstrap_enabled=True)
        state = blocked.rolling_portfolio_state()
        self.assertEqual(state["status"], "BLOCKED")
        self.assertIn("ROLLING_POLICY_BOOTSTRAP_DRIFT", state["blockers"])

    def test_system_bootstrap_rejects_stale_risk_binding_inside_transaction(self) -> None:
        self.store.set_operator_config("rolling_admission_policy_active", None)
        initial = self.control._rolling_risk_binding()
        changed = dict(initial)
        changed["risk_config_generation"] = int(initial["risk_config_generation"]) + 1
        with patch.object(
            self.control,
            "_rolling_risk_binding",
            side_effect=[initial, changed],
        ):
            with self.assertRaisesRegex(
                OperatorControlError, "ROLLING_POLICY_BOOTSTRAP_RISK_DRIFT"
            ):
                self.control._bootstrap_system_exploratory_admission_policy()
        self.assertIsNone(
            self.store.get_operator_config("rolling_admission_policy_active", None)
        )

    def test_rolling_worker_blocks_without_exact_active_bootstrap_envelope(self) -> None:
        active = dict(self.store.get_operator_config("rolling_admission_policy_active", {}))
        self.store.set_operator_config("rolling_admission_policy_active", None)
        processor = AutonomousResearchProcessor(self.store, clock=lambda: datetime(2026, 1, 2, tzinfo=timezone.utc))
        before = self.store.research_queue_stats()
        blocked = processor.refresh_rolling_evidence(datetime(2026, 1, 2, tzinfo=timezone.utc))
        after = self.store.research_queue_stats()
        self.assertEqual(blocked["status"], "BLOCKED")
        self.assertEqual(blocked["blocker"], "ROLLING_POLICY_BOOTSTRAP_ACTIVE_REQUIRED")
        self.assertEqual(after, before)
        self.store.set_operator_config("rolling_admission_policy_active", active)
        self.assertIsNone(
            processor._rolling_bootstrap_blocker(processor._rolling_policy())
        )

    def test_rolling_worker_blocks_when_bootstrap_config_is_missing(self) -> None:
        self.store.set_operator_config("rolling_exploratory_operating_policy", None)
        self.store.set_operator_config("rolling_admission_policy_active", None)
        processor = AutonomousResearchProcessor(self.store, clock=lambda: datetime(2026, 1, 2, tzinfo=timezone.utc))
        before = self.store.research_queue_stats()
        with patch.object(
            processor,
            "_ensure_exploratory_live_bootstrap",
            return_value={"status": "DEFERRED"},
        ):
            blocked = processor.refresh_rolling_evidence(datetime(2026, 1, 2, tzinfo=timezone.utc))
        self.assertEqual(blocked["status"], "BLOCKED")
        self.assertEqual(blocked["blocker"], "ROLLING_POLICY_BOOTSTRAP_REQUIRED")
        self.assertEqual(self.store.research_queue_stats(), before)

    def test_system_bootstrap_rejects_generic_review_activation(self) -> None:
        reviewed = self.control.review_rolling_admission_policy(
            {"global_budget": "1.00"},
            actor="reviewer",
        )
        draft = reviewed["draft"]
        active_before = dict(
            self.store.get_operator_config("rolling_admission_policy_active", {})
        )
        with self.assertRaisesRegex(
            OperatorControlError,
            "ROLLING_POLICY_BOOTSTRAP_MANUAL_ACTIVATION_BLOCKED",
        ):
            self.control.activate_rolling_admission_policy(
                draft["policy_id"],
                draft["version"],
            )
        with self.assertRaisesRegex(
            OperatorControlError,
            "ROLLING_POLICY_BOOTSTRAP_MANUAL_ACTIVATION_BLOCKED",
        ):
            self.control.activate_rolling_admission_policy(
                "rolling-default",
                "v1",
            )
        active = self.store.get_operator_config("rolling_admission_policy_active", {})
        self.assertEqual(active, active_before)

    def test_startup_reconciles_crash_after_canary_sync_idempotently(self) -> None:
        context, _ = self._seed_proposed_selection()
        prepared = self.control._prepare_reviewed_proposed_selection()
        assert prepared is not None
        activated = self.control._activate_reviewed_proposed_selection(
            context={
                **context,
                "selection_id": prepared["selection_id"],
                "selection_hash": prepared["selection_hash"],
            },
            authorization={"authorization_id": "auth-crash"},
            actor="test-operator",
        )
        self.assertIsInstance(activated, dict)
        self.assertIsNotNone(
            self.store.get_operator_config(
                "canary_selection_binding_rollback", None
            )
        )
        restarted = OperatorControlPlane(self.store)
        recovered = restarted.store.load_current_portfolio_selection()
        assert recovered is not None
        self.assertEqual(recovered["selection_id"], context["selection_id"])
        self.assertEqual(recovered["status"], "PAPER")
        self.assertTrue(recovered["paper_only"])
        recovered_members = recovered.get("members", ())
        self.assertFalse(
            [
                item
                for item in recovered_members
                if isinstance(item, dict)
                and (
                    item.get("allocation_active") is True
                    or item.get("canary_armed") is True
                )
            ]
        )
        self.assertIsNone(
            self.store.load_active_execution_authorization(
                mode="EXPLORATORY_MICRO_CANARY",
                now=datetime.now(timezone.utc),
            )
        )
        prepared_paper = restarted.store.load_portfolio_selection(
            prepared["selection_id"]
        )
        self.assertIsNotNone(prepared_paper)
        assert prepared_paper is not None
        self.assertEqual(prepared_paper["status"], "PAPER")
        self.assertTrue(prepared_paper["paper_only"])
        self.assertIsNone(
            self.store.get_operator_config(
                "canary_selection_binding_rollback", None
            )
        )
        self.assertIsNone(
            self.store.connection.execute(
                "SELECT candidate_id FROM canary_selection WHERE singleton=1"
            ).fetchone()
        )
        repeated = OperatorControlPlane(self.store)
        self.assertEqual(
            repeated.store.load_current_portfolio_selection(),
            recovered,
        )

    def test_startup_recovery_does_not_reset_newer_selection_or_singleton(self) -> None:
        context, _ = self._seed_proposed_selection()
        prepared = self.control._prepare_reviewed_proposed_selection()
        assert prepared is not None
        self.control._activate_reviewed_proposed_selection(
            context={
                **context,
                "selection_id": prepared["selection_id"],
                "selection_hash": prepared["selection_hash"],
            },
            authorization={"authorization_id": "auth-crash-newer"},
            actor="test-operator",
        )
        self.store.connection.execute(
            "UPDATE canary_selection SET candidate_id=? WHERE singleton=1",
            ("candidate-newer",),
        )
        self.store.connection.commit()
        self.store.set_operator_config(
            "canary_selection_binding",
            {
                "candidate_id": "candidate-newer",
                "selection_id": "selection-newer",
                "selection_hash": "hash-newer",
            },
        )
        newer = {
            "selection_id": "selection-newer",
            "portfolio_selection_id": "selection-newer",
            "selection_hash": "hash-newer",
            "status": "ACTIVE",
            "paper_only": False,
            "allocation_active": True,
            "canary_armed": True,
            "members": [
                {
                    "strategy_version_id": "strategy-newer",
                    "candidate_id": "candidate-newer",
                    "allocation": "1.00",
                    "allocation_active": True,
                }
            ],
        }
        with patch.object(
            self.store,
            "load_current_portfolio_selection",
            return_value=newer,
        ):
            restarted = OperatorControlPlane(self.store)
        self.assertEqual(
            self.store.get_operator_config("canary_selection_binding"),
            {
                "candidate_id": "candidate-newer",
                "selection_id": "selection-newer",
                "selection_hash": "hash-newer",
            },
        )
        self.assertEqual(
            self.store.connection.execute(
                "SELECT candidate_id FROM canary_selection WHERE singleton=1"
            ).fetchone()[0],
            "candidate-newer",
        )
        self.assertIsNone(
            self.store.get_operator_config(
                "canary_selection_binding_rollback", None
            )
        )
        self.assertEqual(
            self.store.get_operator_config("rolling_selection_activation")["status"],
            "ACTIVE",
        )
        self.assertIsNotNone(restarted)
    def test_exploratory_live_review_confirm_success_merges_persisted_bindings(self) -> None:

        context, draft, review = self._exploratory_confirm_context()
        credentials = _configured_credentials()
        service = Mock()
        service.authoritative_status.side_effect = [
            {"micro_live_canary": "DISARMED"},
            {
                "micro_live_canary": "ARMED",
                "control_candidate": "candidate-live",
                "selected_candidate": "candidate-live",
            },
            {
                "micro_live_canary": "AUTONOMOUS_MICRO_LIVE",
                "control_candidate": "candidate-live",
                "selected_candidate": "candidate-live",
            },
        ]
        service.enable_autonomous_micro_live.return_value = {
            "state": "AUTONOMOUS_MICRO_LIVE",
            "control_candidate": "candidate-live",
            "selected_candidate": "candidate-live",
        }
        activated = {
            "authorization": {
                "authorization_id": "auth-live",
                "generation": 2,
                "status": "ACTIVE",
                "exact_strategy_versions": ["strategy-live"],
                "lifetime_budget": draft["lifetime_budget"],
                "expires_at": draft["expires_at"],
                "stop_rules": draft["stop_rules"],
                "scope_hash": "scope-hash",
                "scope_version": "scope-v1",
                "active_settings_hash": "settings-hash",
                "active_settings_generation": 1,
            }
        }
        with patch.object(self.control, "execution_authorization_snapshot", return_value={"active": None, "draft": draft}), patch.object(
            self.control, "_authorization_context", return_value=context
        ), patch.object(self.control, "exploratory_live_review_snapshot", return_value=review), patch.object(
            self.control, "activate_execution_authorization", return_value=activated
        ), patch("axiom.operator.CredentialStore", return_value=credentials), patch(
            "axiom.operator.PolymarketClobV2Venue", return_value=Mock()
        ), patch("axiom.operator.CanaryService", return_value=service), patch.object(
            self.control.settings, "snapshot", return_value={"config_id": "risk", "generation": 1}
        ):
            result = self.control.confirm_exploratory_live(
                {"confirmation": "CONFIRM EXPLORATORY LIVE"}
            )
        self.assertTrue(result["live_execution"])
        self.assertFalse(result["paper_only"])
        self.assertEqual(result["authorization"]["policy_hash"], "policy-hash")
        service.arm.assert_called_once()
        service.enable_autonomous_micro_live.assert_called_once()

    def test_startup_recovery_does_not_restore_over_newer_binding_config(self) -> None:
        context, _ = self._seed_proposed_selection()
        prepared = self.control._prepare_reviewed_proposed_selection()
        assert prepared is not None
        self.control._activate_reviewed_proposed_selection(
            context={
                **context,
                "selection_id": prepared["selection_id"],
                "selection_hash": prepared["selection_hash"],
            },
            authorization={"authorization_id": "auth-crash-binding"},
            actor="test-operator",
        )
        newer_binding = {
            "candidate_id": "candidate-proposed",
            "selection_id": "selection-newer",
            "selection_hash": "hash-newer",
        }
        self.store.set_operator_config("canary_selection_binding", newer_binding)
        OperatorControlPlane(self.store)
        self.assertEqual(
            self.store.get_operator_config("canary_selection_binding"),
            newer_binding,
        )
        self.assertEqual(
            self.store.connection.execute(
                "SELECT candidate_id FROM canary_selection WHERE singleton=1"
            ).fetchone()[0],
            "candidate-proposed",
        )
        self.assertIsNotNone(
            self.store.get_operator_config(
                "canary_selection_binding_rollback", None
            )
        )
        current = self.store.load_current_portfolio_selection()
        assert current is not None
        self.assertEqual(current["status"], "ACTIVE")

    def test_startup_recovery_does_not_restore_over_newer_singleton(self) -> None:
        context, _ = self._seed_proposed_selection()
        prepared = self.control._prepare_reviewed_proposed_selection()
        assert prepared is not None
        self.control._activate_reviewed_proposed_selection(
            context={
                **context,
                "selection_id": prepared["selection_id"],
                "selection_hash": prepared["selection_hash"],
            },
            authorization={"authorization_id": "auth-crash-singleton"},
            actor="test-operator",
        )
        self.store.connection.execute(
            "UPDATE canary_selection SET candidate_id=? WHERE singleton=1",
            ("candidate-newer",),
        )
        self.store.connection.commit()
        OperatorControlPlane(self.store)
        binding = self.store.get_operator_config("canary_selection_binding")
        self.assertEqual(binding["candidate_id"], "candidate-proposed")
        self.assertEqual(
            self.store.connection.execute(
                "SELECT candidate_id FROM canary_selection WHERE singleton=1"
            ).fetchone()[0],
            "candidate-newer",
        )
        self.assertIsNotNone(
            self.store.get_operator_config(
                "canary_selection_binding_rollback", None
            )
        )
        current = self.store.load_current_portfolio_selection()
        assert current is not None
        self.assertEqual(current["status"], "ACTIVE")

    def test_startup_recovery_retains_marker_on_status_failure_then_clears_exact_live(self) -> None:
        context, _ = self._seed_proposed_selection()
        prepared = self.control._prepare_reviewed_proposed_selection()
        assert prepared is not None
        self.control._activate_reviewed_proposed_selection(
            context={
                **context,
                "selection_id": prepared["selection_id"],
                "selection_hash": prepared["selection_hash"],
            },
            authorization={"authorization_id": "auth-crash-live"},
            actor="test-operator",
        )
        status_failure = Mock()
        status_failure.authoritative_status.side_effect = RuntimeError("transient status")
        with patch("axiom.operator.CanaryService", return_value=status_failure):
            OperatorControlPlane(self.store)
        status_failure.disarm.assert_not_called()
        self.assertIsNotNone(
            self.store.get_operator_config(
                "canary_selection_binding_rollback", None
            )
        )
        current = self.store.load_current_portfolio_selection()
        assert current is not None
        self.assertEqual(current["status"], "ACTIVE")
        live_service = Mock()
        live_service.authoritative_status.return_value = {
            "micro_live_canary": "AUTONOMOUS_MICRO_LIVE",
            "control_candidate": "candidate-proposed",
            "selected_candidate": "candidate-proposed",
        }
        with patch("axiom.operator.CanaryService", return_value=live_service), patch.object(
            self.store,
            "load_active_execution_authorization",
            return_value={
                "authorization_id": "auth-crash-live",
                "generation": 1,
                "selection_id": prepared["selection_id"],
                "selection_hash": prepared["selection_hash"],
            },
        ):
            OperatorControlPlane(self.store)
        live_service.disarm.assert_not_called()
        self.assertIsNone(
            self.store.get_operator_config(
                "canary_selection_binding_rollback", None
            )
        )
        self.assertEqual(
            self.store.load_current_portfolio_selection()["status"],
            "ACTIVE",
        )
        OperatorControlPlane(self.store)
        self.assertEqual(
            self.store.load_current_portfolio_selection()["status"],
            "ACTIVE",
        )
    def test_exploratory_live_confirmation_cannot_retain_stale_candidate(self) -> None:
        context, draft, review = self._exploratory_confirm_context()

        def reviewed(candidate_id: str, strategy_id: str) -> tuple[dict[str, object], dict[str, object], dict[str, object]]:
            current = dict(context)
            current["strategy_versions"] = [strategy_id]
            current["setup_bindings"] = [
                {
                    "strategy_version_id": strategy_id,
                    "candidate_id": candidate_id,
                    "setup_id": "setup-live",
                    "setup_version": "setup-v1",
                    "setup_hash": "setup-hash",
                }
            ]
            current["selection"] = {
                "members": [
                    {
                        "candidate_id": candidate_id,
                        "strategy_version_id": strategy_id,
                        "allocation": "1.00",
                        "status": "ACTIVE",
                    }
                ]
            }
            authorization = dict(draft)
            authorization.update(
                {
                    "status": "ACTIVE",
                    "exact_strategy_versions": [strategy_id],
                    "setup_bindings": list(current["setup_bindings"]),
                }
            )
            disclosure = {
                "blockers": [],
                "members": [{"candidate_id": candidate_id}],
            }
            return current, authorization, disclosure

        context_a, auth_a, review_a = reviewed("candidate-a", "strategy-a")
        context_b, auth_b, review_b = reviewed("candidate-b", "strategy-b")
        credentials = _configured_credentials()
        stale_service = Mock()
        stale_service.authoritative_status.return_value = {
            "micro_live_canary": "ARMED",
            "control_candidate": "candidate-a",
            "selected_candidate": "candidate-a",
        }
        with patch.object(
            self.control,
            "execution_authorization_snapshot",
            return_value={"active": auth_a, "draft": None},
        ), patch.object(
            self.control,
            "_authorization_context",
            side_effect=[context_a, context_b],
        ), patch.object(
            self.control,
            "exploratory_live_review_snapshot",
            return_value=review_a,
        ), patch("axiom.operator.CredentialStore", return_value=credentials), patch(
            "axiom.operator.PolymarketClobV2Venue", return_value=Mock()
        ), patch("axiom.operator.CanaryService", return_value=stale_service), patch.object(
            self.control.settings,
            "snapshot",
            return_value={"config_id": "risk", "generation": 1},
        ):
            with self.assertRaisesRegex(
                OperatorControlError, "^EXPLORATORY_LIVE_AUTHORIZATION_BINDING_STALE$"
            ):
                self.control.confirm_exploratory_live(
                    {"candidate_id": "candidate-a"}
                )
        stale_service.disarm.assert_called_once()
        stale_service.enable_autonomous_micro_live.assert_not_called()

        current_service = Mock()
        current_service.authoritative_status.side_effect = [
            {"micro_live_canary": "DISARMED"},
            {
                "micro_live_canary": "ARMED",
                "control_candidate": "candidate-b",
                "selected_candidate": "candidate-b",
            },
            {
                "micro_live_canary": "AUTONOMOUS_MICRO_LIVE",
                "control_candidate": "candidate-b",
                "selected_candidate": "candidate-b",
            },
        ]
        current_service.arm.return_value = {
            "micro_live_canary": "ARMED",
            "control_candidate": "candidate-b",
            "selected_candidate": "candidate-b",
        }
        current_service.enable_autonomous_micro_live.return_value = {
            "micro_live_canary": "AUTONOMOUS_MICRO_LIVE",
            "control_candidate": "candidate-b",
            "selected_candidate": "candidate-b",
        }
        with patch.object(
            self.control,
            "execution_authorization_snapshot",
            return_value={"active": auth_b, "draft": None},
        ), patch.object(
            self.control, "_authorization_context", return_value=context_b
        ), patch.object(
            self.control,
            "exploratory_live_review_snapshot",
            return_value=review_b,
        ), patch("axiom.operator.CredentialStore", return_value=credentials), patch(
            "axiom.operator.PolymarketClobV2Venue", return_value=Mock()
        ), patch("axiom.operator.CanaryService", return_value=current_service), patch.object(
            self.control.settings,
            "snapshot",
            return_value={"config_id": "risk", "generation": 1},
        ):
            result = self.control.confirm_exploratory_live(
                {"candidate_id": "candidate-b"}
            )
        self.assertTrue(result["live_execution"])
        current_service.arm.assert_called_once_with(
            "candidate-b",
            venue=current_service.arm.call_args.kwargs["venue"],
            config_id="risk",
            expected_generation=1,
            credentials_configured=True,
        )
        current_service.enable_autonomous_micro_live.assert_called_once()

    def test_exploratory_review_binds_readiness_to_funded_target_and_caps_statuses(self) -> None:
        context, _draft, _review = self._exploratory_confirm_context()
        review_context = dict(context)
        review_context["selection"] = {
            "operating_policy": {"mode": "EXPLORATORY_LIVE"},
            "members": [
                {
                    "candidate_id": f"candidate-{index}",
                    "status": status,
                    "allocation": "1.00",
                    "market_scope": {
                        "mode": "EXACT_MARKETS",
                        "market_ids": [f"market-{index}"],
                    },
                }
                for index, status in enumerate(
                    ("ACTIVE", "PAPER", "RETAINED", "REDUCE")
                )
            ],
        }
        with patch.object(self.control, "_authorization_context", return_value=review_context):
            with self.assertRaisesRegex(
                OperatorControlError,
                "^EXPLORATORY_LIVE_AUTHORIZATION_BINDING_STALE$",
            ):
                self.control.exploratory_live_review_snapshot(
                    {"candidate_id": "candidate-3"}
                )
            review = self.control.exploratory_live_review_snapshot()
        self.assertIn("BOUNDED_ALLOCATION_REQUIRED", review["blockers"])
        self.assertEqual(review["allocation"]["selected_members"], 4)
        self.assertIn("EXPLORATORY_SETUP_BINDING_REQUIRED", review["blockers"])

    def test_exploratory_live_activation_binding_failure_rolls_back(self) -> None:
        context, draft, review = self._exploratory_confirm_context()
        activated = {
            "authorization": {
                "authorization_id": "auth-live",
                "generation": 2,
                "status": "ACTIVE",
                "policy_hash": "stale-policy",
            }
        }
        with patch.object(self.control, "execution_authorization_snapshot", return_value={"active": None, "draft": draft}), patch.object(
            self.control, "_authorization_context", return_value=context
        ), patch.object(self.control, "exploratory_live_review_snapshot", return_value=review), patch.object(
            self.control, "activate_execution_authorization", return_value=activated
        ), patch.object(self.control, "revoke_execution_authorization") as revoke:
            with self.assertRaisesRegex(
                OperatorControlError, "^EXPLORATORY_LIVE_AUTHORIZATION_BINDING_STALE$"
            ):
                self.control.confirm_exploratory_live(
                    {"confirmation": "CONFIRM EXPLORATORY LIVE"}
                )
        revoke.assert_called_once()

    def test_exploratory_live_arm_failure_rolls_back_authorization(self) -> None:
        context, draft, review = self._exploratory_confirm_context()
        service = Mock()
        service.authoritative_status.return_value = {"micro_live_canary": "DISARMED"}
        service.arm.side_effect = RuntimeError("arm failed")
        activated = {"authorization": {**draft, "status": "ACTIVE"}}
        with patch.object(self.control, "execution_authorization_snapshot", return_value={"active": None, "draft": draft}), patch.object(
            self.control, "_authorization_context", return_value=context
        ), patch.object(self.control, "exploratory_live_review_snapshot", return_value=review), patch.object(
            self.control, "activate_execution_authorization", return_value=activated
        ), patch.object(self.control, "revoke_execution_authorization") as revoke, patch(
            "axiom.operator.CredentialStore", return_value=_configured_credentials()
        ), patch("axiom.operator.PolymarketClobV2Venue", return_value=Mock()), patch(
            "axiom.operator.CanaryService", return_value=service
        ), patch.object(self.control.settings, "snapshot", return_value={"config_id": "risk", "generation": 1}):
            with self.assertRaises(RuntimeError):
                self.control.confirm_exploratory_live(
                    {"confirmation": "CONFIRM EXPLORATORY LIVE"}
                )
        revoke.assert_called_once()

    def test_exploratory_live_enable_failure_rolls_back_arm_and_authorization(self) -> None:
        context, draft, review = self._exploratory_confirm_context()
        service = Mock()
        service.authoritative_status.return_value = {"micro_live_canary": "DISARMED"}
        service.enable_autonomous_micro_live.side_effect = RuntimeError("enable failed")
        activated = {"authorization": {**draft, "status": "ACTIVE"}}
        with patch.object(self.control, "execution_authorization_snapshot", return_value={"active": None, "draft": draft}), patch.object(
            self.control, "_authorization_context", return_value=context
        ), patch.object(self.control, "exploratory_live_review_snapshot", return_value=review), patch.object(
            self.control, "activate_execution_authorization", return_value=activated
        ), patch.object(self.control, "revoke_execution_authorization") as revoke, patch(
            "axiom.operator.CredentialStore", return_value=_configured_credentials()
        ), patch("axiom.operator.PolymarketClobV2Venue", return_value=Mock()), patch(
            "axiom.operator.CanaryService", return_value=service
        ), patch.object(self.control.settings, "snapshot", return_value={"config_id": "risk", "generation": 1}):
            with self.assertRaises(RuntimeError):
                self.control.confirm_exploratory_live(
                    {"confirmation": "CONFIRM EXPLORATORY LIVE"}
                )
        service.disarm.assert_called_once()
        revoke.assert_called_once()

    def test_exploratory_live_status_truthful_after_review_confirm(self) -> None:
        context, draft, review = self._exploratory_confirm_context()
        self.assertEqual(draft["status"], "DRAFT")
        self.assertFalse(review["blockers"])
        self.assertTrue(context["setup_bindings"])

    def test_status_preserves_domain_review_blocker(self) -> None:
        private_detail = "private-key=review-secret"
        with patch.object(
            self.control,
            "exploratory_live_review_snapshot",
            side_effect=OperatorControlError(
                "CREDENTIALS_NOT_CONFIGURED",
                private_detail,
            ),
        ):
            status = self.control.status()
        review = status["exploratory_live_review"]
        self.assertEqual(review["blockers"], ["CREDENTIALS_NOT_CONFIGURED"])
        self.assertNotIn(private_detail, json.dumps(review, sort_keys=True))
        self.assertNotIn("review-secret", json.dumps(review, sort_keys=True))

    def test_status_redacts_unexpected_review_exception(self) -> None:
        private_detail = "private-key=operator-review-secret"
        with patch.object(
            self.control,
            "exploratory_live_review_snapshot",
            side_effect=RuntimeError(private_detail),
        ):
            status = self.control.status()
        review = status["exploratory_live_review"]
        self.assertEqual(review["blockers"], ["EXPLORATORY_LIVE_REVIEW_UNAVAILABLE"])
        self.assertNotIn(private_detail, json.dumps(review, sort_keys=True))
        self.assertNotIn("operator-review-secret", json.dumps(review, sort_keys=True))

    def test_operator_data_preserves_real_nested_exploratory_review_projection(self) -> None:
        payload = DashboardData(store=self.store, control=self.control).operator_data()
        controls = payload.get("operator_controls")
        self.assertIsInstance(controls, dict)
        review = controls["exploratory_live_review"]
        self.assertEqual(review["proposal_status"], "NONE")
        self.assertEqual(
            review["scope"]["draft"]["draft_id"],
            "rolling-exploratory-scope-draft:polymarket:standard:v1",
        )
        self.assertEqual(
            review["scope"]["draft"]["scope"]["mode"],
            "RULE_BASED_MARKETS",
        )
        self.assertNotEqual(review["scope"]["draft"]["scope"], "<truncated>")
        blockers = review["blockers"]
        self.assertIsInstance(blockers, list)
        self.assertTrue(blockers)
        self.assertTrue(all(isinstance(code, str) and code for code in blockers))
        self.assertTrue(review["paper_only"])
        self.assertFalse(review["live_execution"])

    def test_selected_market_preflight_requires_authoritative_provider_identity_and_suitable_depth(self) -> None:
        selection = {
            "members": [
                {
                    "candidate_id": "candidate-live",
                    "status": "PAPER",
                    "paper_only": True,
                    "allocation_active": False,
                    "proposed_allocation": "1.00",
                    "operating_policy": {"mode": "EXPLORATORY_LIVE"},
                    "direction": "BUY YES",
                    "market_scope": {"mode": "EXACT_MARKETS", "market_ids": ["market-live"]},
                    "operational_setup": {
                        "capture_spec": {
                            "direction": {
                                "positive_delta": "BUY YES",
                                "negative_delta": "BUY NO",
                                "buy_interpretation": "BUY",
                            }
                        }
                    },
                    "current_market_binding": {
                        "market_id": "market-live",
                        "token_id": "token-yes",
                    },
                }
            ]
        }
        self.store.set_operator_config(
            CANARY_CONNECTIVITY_CONFIG_KEY,
            {
                "ready": True,
                "status": "READY",
                "checked_at": datetime.now(timezone.utc).isoformat(),
                "diagnostics": {
                    "authentication": {"status": "OK"},
                    "account": {"authenticated": True},
                    "geoblock": {"blocked": False, "close_only": False},
                    "balance": {"status": "OK"},
                    "allowance": {"status": "OK"},
                    "market": {
                        "market_id": "market-live",
                        "token_id": "token-yes",
                        "accepting_orders": True,
                    },
                    "book": {
                        "min_order_size": "1",
                        "tick_size": "0.01",
                        "bids": [{"price": "0.49", "size": "10"}],
                        "asks": [{"price": "0.51", "size": "10"}],
                        "depth_assessment": {"action": "UNKNOWN"},
                    },
                    "token_readiness": [
                        {
                            "candidate_id": "candidate-live",
                            "market_id": "market-live",
                            "token_id": "token-yes",
                            "ready": True,
                            "trade_ready": True,
                            "diagnostics": {
                                "market": {
                                    "market_id": "market-live",
                                    "token_id": "token-yes",
                                    "accepting_orders": True,
                                },
                                "book": {
                                    "min_order_size": "1",
                                    "tick_size": "0.01",
                                    "bids": [{"price": "0.49", "size": "10"}],
                                    "asks": [{"price": "0.51", "size": "10"}],
                                    "depth_assessment": {"action": "UNKNOWN"},
                                },
                            },
                        }
                    ],
                },
            },
        )
        materialized = SimpleNamespace(
            matched_markets=[
                {
                    "market_id": "market-other",
                    "yes_token_id": "other-yes",
                    "no_token_id": "other-no",
                },
                {
                    "market_id": "market-live",
                    "yes_token_id": "token-yes",
                    "no_token_id": "token-no",
                },
            ]
        )
        with patch.object(self.store, "load_current_market_resolution", return_value=materialized):
            blocked = self.control._selected_market_readiness(selection)
        self.assertEqual(blocked["status"], "BLOCKED")
        self.assertIn("SELECTED_MARKET_DEPTH_REQUIRED", blocked["blockers"])
        ready_projection = self.store.get_operator_config(CANARY_CONNECTIVITY_CONFIG_KEY, {})
        ready_projection["diagnostics"]["book"]["depth_assessment"]["action"] = "SUITABLE"
        ready_projection["diagnostics"]["token_readiness"][0]["diagnostics"]["book"][
            "depth_assessment"
        ]["action"] = "SUITABLE"
        self.store.set_operator_config(CANARY_CONNECTIVITY_CONFIG_KEY, ready_projection)
        with patch.object(self.store, "load_current_market_resolution", return_value=materialized):
            ready = self.control._selected_market_readiness(selection)
        self.assertEqual(ready["status"], "READY")
        self.assertEqual(ready["market_id"], "market-live")
        self.assertEqual(ready["token_id"], "token-yes")

    def test_exploratory_live_compensation_failure_forces_kill_and_unknown_status(self) -> None:
        context, draft, review = self._exploratory_confirm_context()
        service = Mock()
        service.authoritative_status.return_value = {"micro_live_canary": "DISARMED"}
        service.enable_autonomous_micro_live.side_effect = RuntimeError("enable failed")
        service.disarm.side_effect = RuntimeError("disarm failed")
        activated = {"authorization": {**draft, "status": "ACTIVE", "generation": 2}}
        with patch.object(self.control, "execution_authorization_snapshot", return_value={"active": None, "draft": draft}), patch.object(
            self.control, "_authorization_context", return_value=context
        ), patch.object(self.control, "exploratory_live_review_snapshot", return_value=review), patch.object(
            self.control, "activate_execution_authorization", return_value=activated
        ), patch.object(
            self.control, "revoke_execution_authorization", side_effect=RuntimeError("revoke failed")
        ), patch("axiom.operator.CredentialStore", return_value=_configured_credentials()), patch(
            "axiom.operator.PolymarketClobV2Venue", return_value=Mock()
        ), patch("axiom.operator.CanaryService", return_value=service), patch.object(
            self.control.settings, "snapshot", return_value={"config_id": "risk", "generation": 1}
        ):
            with self.assertRaisesRegex(
                OperatorControlError, "^EXPLORATORY_LIVE_ROLLBACK_INCOMPLETE$"
            ):
                self.control.confirm_exploratory_live(
                    {"confirmation": "CONFIRM EXPLORATORY LIVE"}
                )
        service.kill.assert_called_once()
        persisted = self.store.get_operator_config("execution_authorization_review", {})
        self.assertEqual(persisted["rollback_status"], "INCOMPLETE")
        self.assertFalse(persisted["live_execution"])

    def test_exploratory_live_execute_wrapper_projects_live_flags(self) -> None:
        with patch.object(
            self.control,
            "confirm_exploratory_live",
            return_value={
                "status": "AUTONOMOUS_MICRO_LIVE",
                "paper_only": False,
                "live_execution": True,
            },
        ):
            response = self.control.execute(
                "exploratory.live.review_confirm",
                confirm="CONFIRM EXPLORATORY LIVE",
            )
        self.assertTrue(response["live_execution"])
        self.assertFalse(response["paper_only"])

    def test_get_dashboard_is_side_effect_free_and_survives_control_failure(self) -> None:
        before_changes = self.store.connection.total_changes
        before_actions = len(self.store.list_operator_actions())
        response = self.store
        DashboardData(store=response, control=self.control).operator_data()
        self.assertEqual(self.store.connection.total_changes, before_changes)
        self.assertEqual(len(self.store.list_operator_actions()), before_actions)
        failing = Mock()
        failing.status.side_effect = RuntimeError("background failure")
        server = DashboardServer(port=0, data=DashboardData(store=self.store, control=failing)).start()
        self.addCleanup(server.stop)
        assert server.url is not None
        with self.assertRaises(HTTPError) as context:
            urlopen(server.url + "/api/operator", timeout=3)
        self.assertEqual(context.exception.code, 503)
        with urlopen(server.url + "/", timeout=3) as root:
            self.assertEqual(root.status, 200)


if __name__ == "__main__":
    unittest.main()
