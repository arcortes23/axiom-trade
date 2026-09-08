from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

from axiom.binance_operator import (
    BinanceCanaryControlPlane,
    BinanceOperatorError,
    BinanceTestnetControlPlane,
    EXACT_ENABLE_PHRASE,
    EXACT_PROBE_PHRASE,
    EXACT_TESTNET_ENABLE_PHRASE,
)
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

class _TestnetGate:
    def __init__(self, connection, *, configured=False):
        self.connection = connection
        self.credentials = (
            {"api_key": "configured", "api_secret": "configured"}
            if configured
            else None
        )
        self.calls = []
        self.venue_calls = []
        self.profile = {
            "environment": "BINANCE_SPOT_TESTNET",
            "host": "127.0.0.1",
            "port": 8082,
            "runtime_identity": "binance-testnet",
            "db_path": "runtime-data/binance-testnet.sqlite",
        }
        self.connectivity = {"status": "BLOCKED", "reason": "CREDENTIALS_NOT_CONFIGURED"}
        self.validation = {"status": "BLOCKED", "reason": "NOT_CHECKED"}
        self.probe = {"status": "BLOCKED", "reason": "NOT_STARTED"}

    def connectivity_status(self):
        return dict(self.connectivity)

    def validation_status(self):
        return dict(self.validation)

    def probe_status(self):
        return dict(self.probe)

    def dashboard_projection(self):
        return {
            "environment": "BINANCE_SPOT_TESTNET",
            "source": "binance_testnet_gate",
            "probe_kind": "TESTNET EXECUTION PROBE",
            "profile": dict(self.profile),
            "credentials": {"configured": bool(self.credentials)},
            "risk_envelope": {"entry_notional": "10.00"},
        }

    def check_connectivity(self):
        self.calls.append("connectivity")
        if not self.credentials:
            return {**self.connectivity}
        self.venue_calls.append("account")
        self.connectivity = {"status": "PASS", "reason": ""}
        return dict(self.connectivity)

    def validate_order(self, symbol=None):
        self.calls.append(("validation", symbol))
        self.validation = {"status": "PASS", "symbol": symbol or "BTCUSDT"}
        return dict(self.validation)

    def execute_probe(self, symbol=None):
        self.calls.append(("probe", symbol))
        self.probe = {"status": "PASS", "symbol": symbol or "BTCUSDT"}
        return dict(self.probe)

    def reconcile_probe(self):
        self.calls.append("reconcile")
        return dict(self.probe)


class _TestnetStrategy:
    def __init__(self):
        self.state = "DISABLED"
        self.calls = []

    def status(self):
        return {
            "state": self.state,
            "enabled": self.state == "ARMED",
            "selected_candidate": {"candidate_id": "candidate-1"},
            "current_signal": {"signal_id": "signal-1"},
            "no_trade_reason": "WAITING",
        }

    def submit_signal(self, *_args, **_kwargs):
        raise AssertionError("operator gate actions must not submit strategy signals")

    def enable_auto_canary(self, confirmation="", **_kwargs):
        self.calls.append(("enable", confirmation))
        self.state = "ARMED"
        return {"state": self.state, "enabled": True}

    def pause(self, **kwargs):
        self.calls.append(("pause", kwargs))
        self.state = "PAUSED"
        return {"state": self.state}

    def resume(self, **kwargs):
        self.calls.append(("resume", kwargs))
        self.state = "ARMED"
        return {"state": self.state}

    def disarm(self, **kwargs):
        self.calls.append(("disarm", kwargs))
        self.state = "DISARMED"
        return {"state": self.state}

    def kill(self, **kwargs):
        self.calls.append(("kill", kwargs))
        self.state = "KILLED"
        return {"state": self.state}


class _TestnetWorker:
    def __init__(self, strategy, *, supports_persisted_strategy=True):
        self.strategy = strategy
        self._supports_persisted_strategy = supports_persisted_strategy

    def supports_persisted_strategy(self):
        return self._supports_persisted_strategy

    def cycle(self):
        return {"status": "NO_TRADE"}


class BinanceTestnetOperatorTests(unittest.TestCase):
    def setUp(self):
        self.connection = sqlite3.connect(":memory:")
        self.connection.row_factory = sqlite3.Row
        self.gate = _TestnetGate(self.connection)

    def tearDown(self):
        self.connection.close()

    def test_missing_credentials_blocks_gate_without_venue_calls(self):
        control = BinanceTestnetControlPlane(self.gate)
        status = control.status()
        self.assertEqual(status["connectivity"]["status"], "BLOCKED")
        self.assertEqual(status["autonomous"]["blocked_reason"], "CREDENTIALS_NOT_CONFIGURED")
        result = control.action("CONNECTIVITY_CHECK")
        self.assertTrue(result["ok"])
        self.assertEqual(result["result"]["status"], "BLOCKED")
        self.assertEqual(self.gate.venue_calls, [])
        self.assertEqual(self.gate.calls, ["connectivity"])

    def test_status_and_snapshot_identify_strict_testnet(self):
        control = BinanceTestnetControlPlane(self.gate)
        status = control.status()
        snapshot = control.snapshot(page_size=1)

        self.assertIs(status["strict_testnet"], True)
        self.assertIs(snapshot["strict_testnet"], True)

    def test_status_blocker_precedence_is_gate_first_and_network_free(self):
        strategy = _TestnetStrategy()
        control = BinanceTestnetControlPlane(self.gate, execution=strategy, worker=_TestnetWorker(strategy))

        # Missing credentials are actionable before connectivity or strategy.
        status = control.status()
        self.assertEqual(status["autonomous"]["blocked_reason"], "CREDENTIALS_NOT_CONFIGURED")
        self.assertEqual(
            status["strategy_evidence"],
            {
                "selected_candidate": {"candidate_id": "candidate-1"},
                "current_signal": {"signal_id": "signal-1"},
                "no_trade_reason": "WAITING",
            },
        )

        self.gate.credentials = {"api_key": "configured", "api_secret": "configured"}
        self.gate.connectivity = {"status": "BLOCKED", "reason": "NOT_CHECKED"}
        self.gate.validation = {"status": "PASS"}
        self.assertEqual(
            control.status()["autonomous"]["blocked_reason"],
            "CONNECTIVITY_NOT_PASS",
        )

        self.gate.connectivity = {"status": "PASS"}
        self.gate.validation = {"status": "BLOCKED", "reason": "NOT_CHECKED"}
        self.assertEqual(
            control.status()["autonomous"]["blocked_reason"],
            "VALIDATION_NOT_PASS",
        )
        self.assertEqual(self.gate.calls, [])
        self.assertEqual(self.gate.venue_calls, [])

    def test_validation_probe_and_reconcile_dispatch_are_not_strategy_signals(self):
        strategy = _TestnetStrategy()
        control = BinanceTestnetControlPlane(self.gate, execution=strategy, worker=_TestnetWorker(strategy))
        validation = control.action("ORDER_VALIDATION_TEST", {"symbol": "BTCUSDT"})
        probe = control.action(
            "EXECUTION_PROBE",
            {"symbol": "BTCUSDT", "confirmation": EXACT_PROBE_PHRASE},
        )
        reconcile = control.action("RECONCILE_PROBE")
        self.assertTrue(validation["ok"])
        self.assertTrue(probe["ok"])
        self.assertTrue(reconcile["ok"])
        self.assertEqual(
            self.gate.calls,
            [
                ("validation", "BTCUSDT"),
                ("probe", "BTCUSDT"),
                "reconcile",
            ],
        )
        self.assertEqual(strategy.calls, [])

    def test_probe_requires_exact_confirmation(self):
        control = BinanceTestnetControlPlane(self.gate)
        wrong = control.action(
            "EXECUTION_PROBE",
            {"confirmation": EXACT_PROBE_PHRASE + " "},
        )
        self.assertFalse(wrong["ok"])
        self.assertEqual(wrong["reason"], "EXACT_CONFIRMATION_REQUIRED")
        self.assertEqual(self.gate.calls, [])
    def test_enable_and_resume_actions_never_arm_and_private_authorization_does(self):
        strategy = _TestnetStrategy()
        control = BinanceTestnetControlPlane(
            self.gate,
            execution=strategy,
            worker=_TestnetWorker(strategy),
        )
        self.gate.credentials = {"api_key": "configured", "api_secret": "configured"}
        self.gate.connectivity = {"status": "PASS"}
        self.gate.validation = {"status": "PASS"}

        for action, payload in (
            ("ENABLE", {"confirmation": EXACT_TESTNET_ENABLE_PHRASE, "window_seconds": 30}),
            ("RESUME", {"confirmation": EXACT_TESTNET_ENABLE_PHRASE}),
        ):
            blocked = control.action(action, payload)
            self.assertFalse(blocked["ok"])
            self.assertEqual(blocked["reason"], "BOUNDED_AUTO_REQUIRES_CLI")
        self.assertEqual(strategy.calls, [])
        self.assertEqual(strategy.state, "DISABLED")

        authorized = control.authorize_bounded_auto(
            EXACT_TESTNET_ENABLE_PHRASE,
            30,
        )
        self.assertEqual(strategy.state, "ARMED")
        self.assertEqual(strategy.calls[0], ("enable", EXACT_TESTNET_ENABLE_PHRASE))
        self.assertEqual(authorized["autonomous"], {"seconds": 30, "bounded": True})

    def test_private_authorization_rejects_invalid_bounded_windows_before_authorization(self):
        invalid_windows = (
            0,
            29,
            901,
            3601,
            True,
            False,
            None,
            "30",
            30.0,
            {},
            [],
        )
        for window in invalid_windows:
            with self.subTest(window=window):
                strategy = _TestnetStrategy()
                control = BinanceTestnetControlPlane(
                    self.gate,
                    execution=strategy,
                    worker=_TestnetWorker(strategy),
                )
                self.gate.connectivity = {"status": "PASS"}
                self.gate.validation = {"status": "PASS"}
                with self.assertRaises(BinanceOperatorError) as context:
                    control.authorize_bounded_auto(
                        EXACT_TESTNET_ENABLE_PHRASE,
                        window,
                    )
                self.assertEqual(context.exception.reason, "BOUNDED_WINDOW_REQUIRED")
                self.assertEqual(strategy.calls, [])
                self.assertEqual(strategy.state, "DISABLED")
                self.assertIsNone(control._autonomous_window)

    def test_private_authorization_accepts_integer_bounded_window(self):
        strategy = _TestnetStrategy()
        control = BinanceTestnetControlPlane(
            self.gate,
            execution=strategy,
            worker=_TestnetWorker(strategy),
        )
        self.gate.credentials = {"api_key": "configured", "api_secret": "configured"}
        self.gate.connectivity = {"status": "PASS"}
        self.gate.validation = {"status": "PASS"}
        result = control.authorize_bounded_auto(
            EXACT_TESTNET_ENABLE_PHRASE,
            900,
        )
        self.assertEqual(strategy.state, "ARMED")
        self.assertEqual(strategy.calls, [("enable", EXACT_TESTNET_ENABLE_PHRASE)])
        self.assertEqual(result["autonomous"], {"seconds": 900, "bounded": True})

    def test_private_authorization_rechecks_deadline_before_durable_arm(self):
        strategy = _TestnetStrategy()
        self.gate.credentials = {"api_key": "configured", "api_secret": "configured"}
        self.gate.connectivity = {"status": "PASS"}
        self.gate.validation = {"status": "PASS"}
        ticks = iter((0.0, 0.0, 0.0, 0.0, 0.0, 101.0))
        control = BinanceTestnetControlPlane(
            self.gate,
            execution=strategy,
            worker=_TestnetWorker(strategy),
            monotonic_clock=lambda: next(ticks),
        )

        with self.assertRaises(BinanceOperatorError) as context:
            control.authorize_bounded_auto(
                EXACT_TESTNET_ENABLE_PHRASE,
                30,
                deadline_monotonic=100.0,
            )

        self.assertEqual(context.exception.reason, "AUTO_DEADLINE_EXPIRED")
        self.assertEqual(strategy.calls, [])
        self.assertEqual(strategy.state, "DISABLED")
        self.assertIsNone(getattr(control, "_autonomous_window", None))

    def test_private_authorization_rejects_stale_pass_without_current_credentials(self):
        strategy = _TestnetStrategy()
        control = BinanceTestnetControlPlane(
            self.gate,
            execution=strategy,
            worker=_TestnetWorker(strategy),
        )
        # Persisted PASS statuses must not authorize when credentials are absent.
        self.gate.connectivity = {"status": "PASS"}
        self.gate.validation = {"status": "PASS"}
        self.gate.credentials = None

        with self.assertRaises(BinanceOperatorError) as context:
            control.authorize_bounded_auto(EXACT_TESTNET_ENABLE_PHRASE, 30)

        self.assertEqual(context.exception.reason, "CREDENTIALS_NOT_CONFIGURED")
        self.assertEqual(strategy.calls, [])
        self.assertEqual(self.gate.calls, [])
        self.assertEqual(self.gate.venue_calls, [])
        self.assertEqual(strategy.state, "DISABLED")


    def test_history_is_independent_bounded_and_secret_free(self):
        control = BinanceTestnetControlPlane(self.gate, store=_Store(self.connection))
        result = control.action(
            "CONNECTIVITY_CHECK",
            {"api_secret": "must-not-persist"},
        )
        self.assertFalse(result["ok"])
        row = self.connection.execute(
            "SELECT environment,source,probe_kind,payload_json FROM binance_testnet_operator_actions"
        ).fetchone()
        self.assertEqual(row["environment"], "BINANCE_SPOT_TESTNET")
        self.assertEqual(row["source"], "binance_testnet_operator")
        self.assertEqual(row["probe_kind"], "TESTNET EXECUTION PROBE")
        self.assertNotIn("must-not-persist", row["payload_json"])
        legacy_table = self.connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='binance_operator_actions'"
        ).fetchone()
        self.assertIsNone(legacy_table)

    def test_absent_strategy_reports_autonomous_blocked_and_separate_evidence(self):
        self.gate.credentials = {"api_key": "configured", "api_secret": "configured"}
        self.gate.connectivity = {"status": "PASS"}
        self.gate.validation = {"status": "PASS"}
        control = BinanceTestnetControlPlane(self.gate)
        status = control.status()
        self.assertEqual(status["title"], "BINANCE SPOT TESTNET")
        self.assertEqual(status["autonomous"]["state"], "BLOCKED")
        self.assertEqual(status["autonomous"]["blocked_reason"], "STRATEGY_NOT_CONFIGURED")
        self.assertIsNone(status["autonomous"]["selected_candidate"])
        self.assertIsNone(status["autonomous"]["current_signal"])
        self.assertEqual(status["isolation"]["strategy_ledgers_touched"], False)


    def test_execution_only_runtime_is_blocked_without_execution_calls(self):
        self.gate.credentials = {"api_key": "configured", "api_secret": "configured"}
        self.gate.connectivity = {"status": "PASS"}
        self.gate.validation = {"status": "PASS"}
        execution = _TestnetStrategy()
        control = BinanceTestnetControlPlane(self.gate, execution=execution)

        status = control.status()
        self.assertEqual(status["autonomous"]["state"], "BLOCKED")
        self.assertEqual(status["autonomous"]["blocked_reason"], "STRATEGY_NOT_CONFIGURED")
        result = control.action(
            "ENABLE",
            {
                "confirmation": EXACT_TESTNET_ENABLE_PHRASE,
                "window_seconds": 30,
            },
        )
        self.assertFalse(result["ok"])
        self.assertEqual(result["reason"], "BOUNDED_AUTO_REQUIRES_CLI")
        with self.assertRaises(BinanceOperatorError) as context:
            control.authorize_bounded_auto(EXACT_TESTNET_ENABLE_PHRASE, 30)
        self.assertEqual(context.exception.reason, "STRATEGY_NOT_CONFIGURED")
        self.assertEqual(execution.calls, [])

    def test_non_executable_worker_is_blocked_even_with_strategy_and_passed_gates(self):
        strategy = _TestnetStrategy()
        worker = _TestnetWorker(strategy, supports_persisted_strategy=False)
        self.gate.credentials = {"api_key": "configured", "api_secret": "configured"}
        self.gate.connectivity = {"status": "PASS"}
        self.gate.validation = {"status": "PASS"}
        control = BinanceTestnetControlPlane(self.gate, execution=strategy, worker=worker)

        status = control.status()
        self.assertEqual(status["autonomous"]["state"], "BLOCKED")
        self.assertEqual(status["autonomous"]["blocked_reason"], "STRATEGY_NOT_CONFIGURED")
        with self.assertRaises(BinanceOperatorError) as context:
            control.authorize_bounded_auto(EXACT_TESTNET_ENABLE_PHRASE, 30)
        self.assertEqual(context.exception.reason, "STRATEGY_NOT_CONFIGURED")
        self.assertEqual(strategy.calls, [])

    def test_persisted_strategy_worker_can_authorize_without_injected_strategy(self):
        execution = _TestnetStrategy()
        worker = _TestnetWorker(None)
        self.gate.credentials = {"api_key": "configured", "api_secret": "configured"}
        self.gate.connectivity = {"status": "PASS"}
        self.gate.validation = {"status": "PASS"}
        control = BinanceTestnetControlPlane(self.gate, execution=execution, worker=worker)

        authorized = control.authorize_bounded_auto(EXACT_TESTNET_ENABLE_PHRASE, 30)

        self.assertEqual(execution.state, "ARMED")
        self.assertEqual(authorized["autonomous"], {"seconds": 30, "bounded": True})

    def test_configured_worker_routes_control_to_execution(self):
        strategy = _TestnetStrategy()
        control = BinanceTestnetControlPlane(
            self.gate,
            execution=strategy,
            worker=_TestnetWorker(strategy),
        )
        self.gate.connectivity = {"status": "PASS"}
        self.gate.credentials = {"api_key": "configured", "api_secret": "configured"}
        self.gate.validation = {"status": "PASS"}

        control.authorize_bounded_auto(EXACT_TESTNET_ENABLE_PHRASE, 30)
        control.action("PAUSE", {"reason": "operator test"})
        control.action("DISARM", {"reason": "operator test"})

        self.assertEqual(
            [call[0] for call in strategy.calls],
            ["enable", "pause", "disarm"],
        )

    def test_explicit_audit_connection_cannot_commit_gate_transaction(self):
        with tempfile.TemporaryDirectory() as directory:
            database = str(Path(directory) / "testnet.sqlite")
            gate_connection = sqlite3.connect(database, timeout=0.1)
            audit_connection = sqlite3.connect(database, timeout=0.1)
            try:
                gate_connection.execute("PRAGMA journal_mode=WAL")
                gate_connection.commit()
                gate_connection.execute("CREATE TABLE gate_transaction_probe(value TEXT)")
                gate_connection.commit()
                gate = _TestnetGate(gate_connection)
                control = BinanceTestnetControlPlane(
                    gate,
                    store=_Store(audit_connection),
                )
                self.assertIs(control._conn, audit_connection)
                self.assertIsNot(control._conn, gate_connection)

                # Acquire SQLite's write lock and make the gate work
                # uncommitted before the audit connection attempts its write.
                gate_connection.execute("BEGIN IMMEDIATE")
                gate_connection.execute(
                    "INSERT INTO gate_transaction_probe VALUES ('uncommitted')"
                )
                self.assertTrue(gate_connection.in_transaction)

                result = control.action("CONNECTIVITY_CHECK")

                # The separate audit writer cannot commit while the gate
                # transaction owns SQLite's write lock.  The operator boundary
                # reports that persistence failed rather than committing the
                # gate transaction through an aliased connection.
                self.assertFalse(result["ok"])
                self.assertEqual(result["reason"], "ACTION_AUDIT_FAILED")
                self.assertEqual(result["audit_error"], "OperationalError")
                self.assertEqual(
                    gate_connection.execute(
                        "SELECT COUNT(*) FROM gate_transaction_probe"
                    ).fetchone()[0],
                    1,
                )

                gate_connection.rollback()
                self.assertEqual(
                    gate_connection.execute(
                        "SELECT COUNT(*) FROM gate_transaction_probe"
                    ).fetchone()[0],
                    0,
                )

                # Once the gate transaction is rolled back, the same distinct
                # audit connection can persist a normal operator action.
                recovered = control.action("CONNECTIVITY_CHECK")
                self.assertTrue(recovered["ok"])
                self.assertEqual(
                    audit_connection.execute(
                        "SELECT COUNT(*) FROM binance_testnet_operator_actions"
                    ).fetchone()[0],
                    1,
                )
            finally:
                audit_connection.close()
                gate_connection.close()


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
