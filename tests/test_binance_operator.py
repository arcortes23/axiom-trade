from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
import json
import sqlite3
import unittest

from axiom.binance_operator import BinanceCanaryControlPlane, EXACT_ENABLE_PHRASE
from axiom.binance_risk import DEFAULT_BINANCE_RISK_ENVELOPE
from axiom.binance_spot import PAPER, canonical_sha256


class _Store:
    def __init__(self, connection: sqlite3.Connection) -> None:
        self.connection = connection
        self.path = ":memory:"


class _Venue:
    def __init__(self) -> None:
        self.account_calls = 0
        self.order_test_calls = 0
        self.place_calls = 0

    def order_test(self, **kwargs):
        self.order_test_calls += 1
        return {"status": "OK", "symbol": kwargs["symbol"]}

    def place_order(self, **kwargs):
        self.place_calls += 1
        raise AssertionError("validation must never place an order")


class _CredentialRef:
    def __init__(self, value=None, error: Exception | None = None) -> None:
        self.value = value
        self.error = error

    def stable_id(self):
        if self.error is not None:
            raise self.error
        return self.value


class _SecretStableID:
    def __init__(self, secret: str) -> None:
        self.secret = secret

    def __str__(self) -> str:
        return self.secret

    def __repr__(self) -> str:
        return f"_SecretStableID({self.secret!r})"


class _Execution:
    environment = PAPER
    risk_envelope = DEFAULT_BINANCE_RISK_ENVELOPE
    venue = None
    credentials = None
    credential_ref = None

    def __init__(self) -> None:
        self.state = "DISABLED"
        self.control_credential_hash = None
        self.calls: list[str] = []
        self._checked_at = "2026-09-07T00:00:00+00:00"
        self._positions = [
            {"symbol": "BTCUSDT", "quantity": "1", "cost_basis": "10", "realized_pnl": "1", "unrealized_pnl": "2", "fees_quote": "0.1"}
        ]
        self._orders = [
            {"intent_id": "intent-" + "x" * 40, "client_order_id": "AXIOM-" + "y" * 30, "symbol": "BTCUSDT", "state": "UNKNOWN", "notional": "3", "fee_reserve": "0.1"}
        ]
        self._fills = [{"trade_id": "trade-" + "z" * 40, "symbol": "BTCUSDT", "quantity": "1", "price": "10", "commission": "0.1"}]

    def control(self):
        return {"state": self.state, "authorized": self.state == "ARMED", "credential_hash": self.control_credential_hash}

    def heartbeat(self):
        return {
            "connectivity": {"status": "UNKNOWN", "checked_at": self._checked_at, "heartbeat_at": self._checked_at},
            "reconciliation": {"status": "SUCCESS", "heartbeat_at": self._checked_at},
        }

    def schema(self):
        return {"namespace": "BINANCE_SPOT_EXECUTION", "schema_version": "test"}

    def positions(self):
        return self._positions

    def orders(self):
        return self._orders

    def fills(self):
        return self._fills

    def check_connectivity(self):
        self.calls.append("connectivity")
        return {"status": "OK", "checked_at": self._checked_at}

    def enable_auto_canary(self, confirmation="", **kwargs):
        self.calls.append("enable")
        self.state = "ARMED"
        return self.control()

    def pause(self, **kwargs):
        self.calls.append("pause")
        self.state = "PAUSED"
        return self.control()

    def disarm(self, **kwargs):
        self.calls.append("disarm")
        self.state = "DISARMED"
        return self.control()

    def kill(self, **kwargs):
        self.calls.append("kill")
        self.state = "KILLED"
        return self.control()


class _Qualification:
    def status(self):
        return {
            "selection_status": "STALE",
            "selection_valid": False,
            "reason": "EVIDENCE_CHANGED",
            "qualified_count": 1,
            "ranking_count": 2,
        }

    def current_selection(self):
        return None

    def actionable_rankings(self, limit=5):
        return [{"candidate_id": "candidate-long-" + "a" * 40, "symbol": "BTCUSDT", "family": "trend"}][:limit]


class _Worker:
    def status(self):
        return {"status": "IDLE"}

    def stop(self):
        return {"status": "STOPPED"}


class BinanceOperatorTests(unittest.TestCase):
    def setUp(self):
        self.connection = sqlite3.connect(":memory:")
        self.execution = _Execution()
        self.control = BinanceCanaryControlPlane(
            _Store(self.connection),
            self.execution,
            qualification=_Qualification(),
            worker=_Worker(),
            clock=lambda: datetime(2026, 9, 7, tzinfo=timezone.utc),
        )

    def tearDown(self):
        self.connection.close()

    def test_safe_defaults_and_independent_projection(self):
        status = self.control.status()
        self.assertEqual(status["control"]["state"], "DISABLED")
        self.assertEqual(status["polymarket_transport"], "DISABLED")
        self.assertFalse(status["credentials"]["configured"])
        self.assertEqual(status["risk"]["limits"]["entry_notional"], "10.00")
        self.assertEqual(status["risk"]["limits"]["max_aggregate_exposure"], "30.00")
        self.assertEqual(status["risk"]["limits"]["max_positions"], 5)
        self.assertEqual(status["risk"]["limits"]["max_submissions_per_day"], 20)
        self.assertEqual(status["qualification"]["current_vs_stale"], "STALE")
        self.assertEqual(status["qualification"]["family"], None)

    def test_credential_status_rejects_redacted_malformed_and_nested_hashes(self):
        absent_hash = canonical_sha256({"configured": False})
        secret = "opaque-secret-that-must-not-leak"
        for value in (
            "<redacted>",
            "arbitrary-credential-value",
            "a" * 63,
            "a" * 65,
            {"nested": "a" * 64, "value": secret},
            ["a" * 64, secret],
            b"a" * 64,
        ):
            with self.subTest(value_type=type(value).__name__):
                self.execution.control_credential_hash = value
                status = self.control.status()
                self.assertFalse(status["credentials"]["configured"])
                self.assertNotIn(secret, json.dumps(status, default=str))

        self.execution.control_credential_hash = absent_hash
        self.assertFalse(self.control.status()["credentials"]["configured"])

        self.execution.control_credential_hash = "a" * 64
        self.assertTrue(self.control.status()["credentials"]["configured"])

    def test_credential_status_accepts_explicit_credentials_only(self):
        self.execution.control_credential_hash = "<redacted>"
        self.execution.credentials = {
            "api_key": "configured-key",
            "api_secret": "configured-secret",
            "metadata": {"token": ["nested-token"]},
        }
        status = self.control.status()
        self.assertTrue(status["credentials"]["configured"])
        self.assertEqual(set(status["credentials"]), {"configured", "reference_hash"})
        self.assertNotIn("configured-key", json.dumps(status, default=str))
        self.assertNotIn("configured-secret", json.dumps(status, default=str))

        self.execution.credentials = {
            "api_key": {"nested": "malformed-key"},
            "api_secret": "configured-secret",
        }
        self.execution.control_credential_hash = canonical_sha256({"configured": False})
        self.assertFalse(self.control.status()["credentials"]["configured"])


    def test_credential_reference_projection_requires_valid_scalar_hash(self):
        valid_hash = canonical_sha256({"credential_ref": "valid"})
        self.execution.credential_ref = _CredentialRef(valid_hash)
        status = self.control.status()
        snapshot = self.control.snapshot(page_size=1)
        for projection in (status["credentials"], snapshot["status"]["credentials"]):
            self.assertTrue(projection["configured"])
            self.assertEqual(projection["reference_hash"], valid_hash)

        secret = "credential-reference-secret-must-not-leak"
        invalid_values = (
            {"nested": "a" * 64, "secret": secret},
            ["a" * 64, secret],
            _SecretStableID(secret),
            None,
            "malformed-" + secret,
        )
        for value in invalid_values:
            with self.subTest(value_type=type(value).__name__):
                self.execution.credentials = None
                self.execution.control_credential_hash = None
                self.execution.credential_ref = _CredentialRef(value)
                status = self.control.status()
                snapshot = self.control.snapshot(page_size=1)
                for projection in (status["credentials"], snapshot["status"]["credentials"]):
                    self.assertFalse(projection["configured"])
                    self.assertIsNone(projection["reference_hash"])
                encoded = json.dumps({"status": status, "snapshot": snapshot}, default=str)
                self.assertNotIn(secret, encoded)

        self.execution.credential_ref = _CredentialRef(error=RuntimeError(secret))
        status = self.control.status()
        snapshot = self.control.snapshot(page_size=1)
        for projection in (status["credentials"], snapshot["status"]["credentials"]):
            self.assertFalse(projection["configured"])
            self.assertIsNone(projection["reference_hash"])
        self.assertNotIn(secret, json.dumps({"status": status, "snapshot": snapshot}, default=str))

        self.execution.credentials = {
            "api_key": "configured-key",
            "api_secret": "configured-secret",
        }
        self.execution.credential_ref = _CredentialRef({"secret": secret})
        status = self.control.status()
        self.assertTrue(status["credentials"]["configured"])
        self.assertIsNone(status["credentials"]["reference_hash"])
        self.assertNotIn(secret, json.dumps(status, default=str))

    def test_every_success_and_failure_is_persisted(self):
        failed = self.control.action("ENABLE", {"confirmation": "wrong"})
        self.assertFalse(failed["ok"])
        succeeded = self.control.action("CONNECTIVITY_CHECK")
        self.assertTrue(succeeded["ok"])
        rows = self.connection.execute("SELECT action,success,result_json,timestamp_utc,timestamp_pht FROM binance_operator_actions ORDER BY rowid").fetchall()
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0][0], "ENABLE")
        self.assertEqual(rows[0][1], 0)
        self.assertEqual(rows[1][0], "CONNECTIVITY_CHECK")
        self.assertEqual(rows[1][1], 1)
        self.assertIn("+00:00", rows[1][3])
        self.assertIn("+08:00", rows[1][4])
        self.assertEqual(json.loads(rows[1][2])["ok"], True)

    def test_connectivity_is_separate_from_order_validation(self):
        venue = _Venue()
        self.execution.environment = "BINANCE_SPOT_TESTNET"
        self.execution.venue = venue
        connectivity = self.control.action("CONNECTIVITY_CHECK")
        validation = self.control.action("ORDER_VALIDATION_TEST", {"symbol": "BTCUSDT", "price": "10", "quantity": "0.1"})
        self.assertTrue(connectivity["ok"])
        self.assertTrue(validation["ok"])
        self.assertEqual(self.execution.calls, ["connectivity"])
        self.assertEqual(venue.order_test_calls, 1)
        self.assertEqual(venue.place_calls, 0)

    def test_enable_resume_phrase_and_live_refusal(self):
        wrong = self.control.action("ENABLE", {"confirmation": EXACT_ENABLE_PHRASE + " "})
        self.assertFalse(wrong["ok"])
        enabled = self.control.action("ENABLE", {"confirmation": EXACT_ENABLE_PHRASE})
        self.assertTrue(enabled["ok"])
        self.assertTrue(enabled["result"]["envelope"])
        self.execution.state = "PAUSED"
        resumed = self.control.action("RESUME", {"confirm": EXACT_ENABLE_PHRASE})
        self.assertTrue(resumed["ok"])
        self.execution.environment = "BINANCE_SPOT_LIVE"
        refused = self.control.action("ENABLE", {"confirmation": EXACT_ENABLE_PHRASE})
        self.assertFalse(refused["ok"])
        self.assertEqual(refused["reason"], "DEVELOPMENT_LIVE_REFUSED")
        self.assertEqual(self.execution.calls.count("enable"), 2)

    def test_bounded_pages_keep_complete_ids_in_detail_records(self):
        snapshot = self.control.snapshot(page_size=1)
        item = snapshot["orders"]["items"][0]
        detail = snapshot["orders"]["detail_records"][0]
        self.assertTrue(item["intent_id"].startswith("intent-"))
        self.assertNotEqual(item["intent_id"], detail["intent_id"])
        self.assertEqual(len(detail["intent_id"]), len(self.execution._orders[0]["intent_id"]))
        self.assertEqual(snapshot["unknown"]["total"], 1)

    def test_host_secret_and_forbidden_transport_rejection_has_no_effect(self):
        for payload, reason in (
            ({"host": "evil.example"}, "HOST_NOT_ALLOWED"),
            ({"api_secret": "do-not-store"}, "SECRET_PAYLOAD_REJECTED"),
            ({"nested": {"endpoint": "https://evil.example"}}, "ARBITRARY_ENDPOINT_REJECTED"),
            ({"transport": "Polymarket"}, "TRANSPORT_NOT_ALLOWED"),
        ):
            result = self.control.action("CONNECTIVITY_CHECK", payload)
            self.assertFalse(result["ok"])
            self.assertEqual(result["reason"], reason)
        self.assertEqual(self.execution.calls, [])
        row = self.connection.execute("SELECT payload_json FROM binance_operator_actions WHERE reason='SECRET_PAYLOAD_REJECTED'").fetchone()
        self.assertNotIn("do-not-store", row[0])
    def test_secret_subtrees_are_redacted_in_status_snapshot_history_and_audit(self):
        raw_values = (
            "nested-api-secret",
            "nested-api-key",
            "nested-token",
            "nested-authorization",
        )
        self.execution.credentials = {
            "api_key": "configured-key",
            "api_secret": "configured-secret",
            "metadata": {"token": ["nested-token"]},
        }
        self.execution._orders = [
            {
                **self.execution._orders[0],
                "metadata": {"authorization": {"token": "nested-authorization"}},
                "payload_json": json.dumps({"safe": True, "api_secret": "nested-api-secret"}),
            }
        ]
        self.execution.check_connectivity = lambda: {
            "status": "OK",
            "credentials": {"api_key": "nested-api-key", "token": ["nested-token"]},
            "safe_json": json.dumps({"authorization": {"bearer": "nested-authorization"}}),
        }

        for key, value in (
            ("api_secret", {"child": ["nested-api-secret"]}),
            ("api_key", [{"child": "nested-api-key"}]),
            ("token", {"child": "nested-token"}),
            ("credentials", [{"authorization": "nested-authorization"}]),
            ("authorization", {"nested": ["nested-authorization"]}),
        ):
            rejected = self.control.action("CONNECTIVITY_CHECK", {key: value})
            self.assertFalse(rejected["ok"])
            self.assertEqual(rejected["reason"], "SECRET_PAYLOAD_REJECTED")

        result = self.control.action("CONNECTIVITY_CHECK")
        status = self.control.status()
        snapshot = self.control.snapshot(page_size=1)
        history = self.control.list_actions()
        audit = self.connection.execute(
            "SELECT payload_json,result_json FROM binance_operator_actions"
        ).fetchall()
        encoded = json.dumps(
            {"result": result, "status": status, "snapshot": snapshot, "history": history, "audit": audit},
            default=str,
        )
        for value in raw_values:
            self.assertNotIn(value, encoded)
        self.assertEqual(set(status["credentials"]), {"configured", "reference_hash"})

    def test_sell_reservations_use_remaining_held_quantity_without_quote_budget(self):
        self.execution._orders = [
            {
                "symbol": "BTCUSDT",
                "side": "BUY",
                "state": "FILLED",
                "notional": "4",
                "fee_reserve": "1",
                "risk_reservation": {
                    "side": "BUY",
                    "status": "HELD",
                    "reserved_quantity": "99",
                },
            },
            {
                "symbol": "BTCUSDT",
                "side": "SELL",
                "state": "PARTIALLY_FILLED",
                "notional": "99",
                "fee_reserve": "3",
                "quantity": "2",
                "filled_quantity": "0.75",
                "risk_reservation": {
                    "side": "SELL",
                    "status": "HELD",
                    "reserved_quantity": "1.25",
                },
            },
            {
                "symbol": "ETHUSDT",
                "side": "SELL",
                "state": "FILLED",
                "quantity": "9",
                "risk_reservation": {
                    "side": "SELL",
                    "status": "RELEASED",
                    "reserved_quantity": "9",
                },
            },
            {
                "symbol": "ETHUSDT",
                "side": "SELL",
                "state": "FILLED",
                "quantity": "8",
                "risk_reservation": {
                    "side": "SELL",
                    "status": "HELD",
                    "reserved_quantity": "8",
                },
            },
        ]
        risk = self.control.status()["risk"]
        self.assertEqual(risk["reservations"], "5.00")
        self.assertEqual(risk["quote_reservations"], "5.00")
        self.assertEqual(risk["remaining"]["entry_notional"], "5.00")
        self.assertEqual(risk["remaining"]["max_reserved_exposure"], "25.00")
        self.assertEqual(risk["base_reservations"], {"BTCUSDT": "1.25", "ETHUSDT": "8.00"})
        self.assertNotIsInstance(risk["remaining"]["entry_notional"], Decimal)

    def test_pause_disarm_kill_warn_and_restart_visibility(self):
        paused = self.control.action("PAUSE")
        self.assertTrue(paused["ok"])
        self.assertTrue(paused["result"]["entries_paused"])
        disarmed = self.control.action("DISARM")
        self.assertTrue(disarmed["result"]["position_warning"])
        killed = self.control.action("KILL")
        self.assertTrue(killed["result"]["reconciliation_continues"])
        restarted = self.control.action("RESTART")
        self.assertTrue(restarted["ok"])
        self.assertTrue(restarted["result"]["restart"]["restart_visible"])


if __name__ == "__main__":
    unittest.main()
