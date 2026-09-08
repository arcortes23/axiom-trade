from __future__ import annotations

import io
import json
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from axiom.binance_dev import BinanceTestnetRuntime
from axiom.binance_operator import BinanceTestnetControlPlane
from axiom.binance_spot import (
    BINANCE_SPOT_TESTNET,
    PAPER,
    BinanceCredentialRef,
    BinanceCredentialStore,
    BinanceRuntimeProfile,
)
from axiom.canary import CanaryService, CredentialStore as PolymarketCredentialStore
from axiom.dashboard import DashboardData, _DashboardHandler, _jsonable
from axiom.node import NodeConfig, ResearchNode
from axiom.operator import OperatorControlPlane
from axiom.storage import AxiomStore


NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)
CONTROL_TOKEN = "integration-control-token"


def _row_counts(store: AxiomStore, *, prefixes: tuple[str, ...], names: tuple[str, ...] = ()) -> dict[str, int]:
    rows = store.connection.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
    selected = [
        str(row[0])
        for row in rows
        if str(row[0]).startswith(prefixes) or str(row[0]) in names
    ]
    counts: dict[str, int] = {}
    for name in selected:
        quoted = '"' + name.replace('"', '""') + '"'
        counts[name] = int(store.connection.execute(f"SELECT COUNT(*) FROM {quoted}").fetchone()[0])
    return counts


def _polymarket_sentinel_rows(store: AxiomStore) -> dict[str, tuple[tuple[object, ...], ...]]:
    return {
        table: tuple(
            tuple(row)
            for row in store.connection.execute(f"SELECT * FROM {table} ORDER BY rowid")
        )
        for table in (
            "canary_control",
            "canary_readiness_snapshot",
            "operator_actions",
        )
    }


class _NoNetworkPolymarketProvider:
    def __getattr__(self, name: str) -> object:
        raise AssertionError(f"normal startup attempted provider operation: {name}")


class _NoSocketServer:
    def __init__(self, _host: str, _port: int, *, data: object) -> None:
        self.data = data
        self.started = False
        self.stopped = False
        self.url = None

    def start(self) -> "_NoSocketServer":
        self.started = True
        return self

    def stop(self) -> None:
        self.stopped = True


class _MissingBinanceCredentials:
    ref = BinanceCredentialRef(
        instance="binance-testnet",
        environment=BINANCE_SPOT_TESTNET,
    )

    def __init__(self) -> None:
        self.load_calls = 0

    def load(self) -> None:
        self.load_calls += 1
        return None

    def safe_projection(self) -> dict[str, object]:
        return {
            "namespace": "AXIOM-BINANCE-SPOT-TESTNET",
            "instance": "binance-testnet",
            "environment": BINANCE_SPOT_TESTNET,
            "configured": False,
            "secret_values_exposed": False,
        }


class _MissingCredentialGate:
    strict_testnet = True
    environment = BINANCE_SPOT_TESTNET

    def __init__(self) -> None:
        self.profile = None
        self.store = None
        self.credential_store = None
        self.credentials = None

    def _blocked(self, reason: str) -> dict[str, object]:
        return {"status": "BLOCKED", "reason": reason, "environment": BINANCE_SPOT_TESTNET}

    def connectivity_status(self) -> dict[str, object]:
        return self._blocked("CREDENTIALS_NOT_CONFIGURED")

    def validation_status(self) -> dict[str, object]:
        return self._blocked("CREDENTIALS_NOT_CONFIGURED")

    def probe_status(self) -> dict[str, object]:
        return self._blocked("CREDENTIALS_NOT_CONFIGURED")

    def close(self) -> None:
        return None


class _IsolatedTestnetControl:
    strict_testnet = True
    environment = BINANCE_SPOT_TESTNET

    def __init__(self, gate: _MissingCredentialGate, *, store: AxiomStore) -> None:
        self.gate = gate
        self.profile = gate.profile
        self.store = store
        self.execution = None
        self.worker = None
        self.calls: list[tuple[str, dict[str, object]]] = []
        self.state = "DISARMED"

    def status(self) -> dict[str, object]:
        return {
            "title": "BINANCE SPOT TESTNET",
            "strict_testnet": True,
            "environment": BINANCE_SPOT_TESTNET,
            "profile": self.profile.projection(),
            "credentials": {
                "configured": False,
                "namespace": "AXIOM-BINANCE-SPOT-TESTNET",
            },
            "autonomous": {
                "enabled": False,
                "state": self.state,
                "blocked_reason": "CREDENTIALS_NOT_CONFIGURED",
            },
        }

    def list_actions(self, *, limit: int = 25) -> list[dict[str, object]]:
        del limit
        return []

    def action(self, action: str, payload: dict[str, object] | None = None) -> dict[str, object]:
        self.calls.append((action, dict(payload or {})))
        return {"ok": False, "action": action.upper(), "reason": "ACTION_NOT_ALLOWED"}

    def close(self) -> None:
        return None


class _FakePolymarketControl:
    def __init__(self) -> None:
        self.state = "DISARMED"
        self.calls: list[tuple[str, str, str]] = []

    def status(self) -> dict[str, object]:
        return {
            "state": self.state,
            "venue": "polymarket",
            "credentials": {
                "configured": True,
                "service": "AXIOM-POLYMARKET-CANARY",
                "private_key": "polymarket-private-key",
            },
        }

    def execute(self, action: str, target: str = "", *, confirm: str = "") -> dict[str, object]:
        self.calls.append((action, target, confirm))
        if action != "canary.enable_auto":
            return {"ok": False, "action": action, "reason": "ACTION_NOT_ALLOWED"}
        self.state = "ENABLED"
        return {"ok": True, "action": action, "state": self.state}


class _FakeBinanceFacade:
    strict_testnet = True
    environment = BINANCE_SPOT_TESTNET

    def __init__(self) -> None:
        self.state = "DISARMED"
        self.snapshot_calls = 0
        self.calls: list[tuple[str, dict[str, object]]] = []

    def status(self) -> dict[str, object]:
        return {
            "title": "BINANCE SPOT TESTNET",
            "strict_testnet": True,
            "environment": BINANCE_SPOT_TESTNET,
            "credentials": {
                "configured": True,
                "namespace": "AXIOM-BINANCE-SPOT-TESTNET",
                "api_key": "binance-api-key",
            },
            "autonomous": {"state": self.state, "enabled": self.state == "ARMED"},
        }

    def snapshot(self, *, page: int, page_size: int) -> dict[str, object]:
        self.snapshot_calls += 1
        empty_page = {"page": page, "page_size": page_size, "total": 0, "items": []}
        return {
            "status": self.status(),
            "positions": empty_page,
            "orders": empty_page,
            "fills": empty_page,
            "unknown": empty_page,
        }

    def action(self, action: str, payload: dict[str, object]) -> dict[str, object]:
        self.calls.append((action, dict(payload)))
        if action.upper() != "PAUSE":
            return {"ok": False, "action": action.upper(), "reason": "ACTION_NOT_ALLOWED"}
        self.state = "PAUSED"
        return {"ok": True, "action": action.upper(), "state": self.state}

    def list_actions(self, *, limit: int = 25) -> list[dict[str, object]]:
        del limit
        return []


class _HandlerProbe(_DashboardHandler):
    def __init__(
        self,
        data: DashboardData,
        path: str,
        *,
        body: dict[str, object] | None = None,
        token: str = CONTROL_TOKEN,
    ) -> None:
        self.server = SimpleNamespace(
            dashboard_data=data,
            control_token=token,
        )
        self.path = path
        self.client_address = ("127.0.0.1", 43210)
        self.responses: list[tuple[int, object]] = []
        if body is None:
            self.headers = {}
            self.rfile = io.BytesIO(b"")
        else:
            encoded = json.dumps(body).encode("utf-8")
            self.headers = {
                "Content-Type": "application/json",
                "Content-Length": str(len(encoded)),
                "X-Axiom-Control-Token": token,
            }
            self.rfile = io.BytesIO(encoded)

    def _send(self, status: int, payload: object, _content_type: str = "application/json; charset=utf-8") -> None:
        self.responses.append((status, payload))


class BinancePolymarketIntegrationTests(unittest.TestCase):
    def test_normal_polymarket_startup_has_no_binance_control_or_table_mutations(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            db_path = str(Path(directory) / "axiom.sqlite")
            store = AxiomStore(db_path)
            node: ResearchNode | None = None
            try:
                before = _row_counts(store, prefixes=("binance_",))
                node = ResearchNode(
                    NodeConfig(
                        db_path=db_path,
                        lock_path=str(Path(directory) / "node.lock"),
                        log_path=str(Path(directory) / "node.log"),
                    ),
                    provider=_NoNetworkPolymarketProvider(),
                    store=store,
                    clock=lambda: NOW,
                )
                node.run(max_cycles=0)
                control = OperatorControlPlane(store, db_path=db_path)
                after = _row_counts(store, prefixes=("binance_",))

                self.assertEqual(before, after)
                for owner in (node, control):
                    self.assertFalse(hasattr(owner, "binance_canary"))
                    self.assertFalse(hasattr(owner, "binance_testnet"))
                    self.assertFalse(hasattr(owner, "binance_execution"))
                self.assertIsNone(node.crypto_provider)
                self.assertIsNone(node._crypto_trader)
                self.assertNotIsInstance(control, BinanceTestnetControlPlane)
            finally:
                if node is not None:
                    node.stop()
                store.close()

    def test_strict_testnet_startup_disables_polymarket_control_and_mutations(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            profile = BinanceRuntimeProfile.testnet(root)
            credentials = _MissingBinanceCredentials()
            seed_store = AxiomStore(profile.db_path)
            CanaryService(seed_store, clock=lambda: NOW)
            with seed_store.connection:
                seed_store.connection.execute(
                    """
                    INSERT INTO canary_control (
                        singleton, state, candidate_id, venue, armed_at, expires_at,
                        limits_json, integrity_hash, updated_at, control_generation
                    ) VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "DISARMED",
                        "polymarket-sentinel",
                        "polymarket",
                        NOW.isoformat(),
                        NOW.isoformat(),
                        '{"sentinel":true}',
                        "polymarket-control-sentinel",
                        NOW.isoformat(),
                        41,
                    ),
                )
                seed_store.connection.execute(
                    """
                    UPDATE canary_readiness_snapshot SET
                        payload_json=?,
                        readiness_snapshot_status=?,
                        readiness_snapshot_stale=?,
                        readiness_snapshot_reason=?,
                        readiness_snapshot_updated_at=?,
                        source_control_generation=?,
                        projection_version=?
                    WHERE singleton=1
                    """,
                    (
                        '{"sentinel":"polymarket-readiness"}',
                        "STALE",
                        1,
                        "POLYMARKET_SENTINEL",
                        NOW.isoformat(),
                        41,
                        7,
                    ),
                )
                seed_store.connection.execute(
                    """
                    INSERT INTO operator_actions (
                        action_id, action, target, timestamp, success, reason, result_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "polymarket-action-sentinel",
                        "SENTINEL",
                        "polymarket",
                        NOW.isoformat(),
                        1,
                        "POLYMARKET_SENTINEL",
                        '{"sentinel":"polymarket-action"}',
                    ),
                )
            expected_rows = _polymarket_sentinel_rows(seed_store)

            def store_factory(path: str) -> AxiomStore:
                return AxiomStore(path)

            gate = _MissingCredentialGate()

            def gate_factory(store: AxiomStore, **kwargs: object) -> _MissingCredentialGate:
                gate.store = store
                gate.profile = kwargs["profile"]
                gate.credential_store = kwargs["credential_store"]
                return gate

            def control_factory(
                gate_value: _MissingCredentialGate,
                *,
                store: AxiomStore,
                **_: object,
            ) -> _IsolatedTestnetControl:
                return _IsolatedTestnetControl(gate_value, store=store)

            runtime = BinanceTestnetRuntime(
                root,
                profile=profile,
                credential_store=credentials,
                store_factory=store_factory,
                gate_factory=gate_factory,
                control_factory=control_factory,
                dashboard_server_factory=_NoSocketServer,
            )
            try:
                initial_load_calls = credentials.load_calls
                self.assertEqual(_polymarket_sentinel_rows(seed_store), expected_rows)

                runtime.start()
                self.assertEqual(_polymarket_sentinel_rows(seed_store), expected_rows)
                status = runtime.status()
                self.assertEqual(_polymarket_sentinel_rows(seed_store), expected_rows)

                self.assertIsNone(runtime.dashboard_data.control)
                self.assertNotIsInstance(runtime.control, OperatorControlPlane)
                self.assertEqual(status["operator_control_plane"], "BINANCE_TESTNET")
                self.assertEqual(status["isolation"]["polymarket_transport"], "DISABLED")
                self.assertFalse(status["credentials"]["configured"])
                self.assertFalse(status["autonomous"]["enabled"])
                self.assertEqual(status["autonomous"]["blocked_reason"], "CREDENTIALS_NOT_CONFIGURED")
                self.assertEqual(credentials.load_calls, initial_load_calls + 1)
            finally:
                runtime.stop()
                self.assertEqual(_polymarket_sentinel_rows(seed_store), expected_rows)
                seed_store.close()

    def test_polymarket_and_binance_credentials_use_disjoint_fake_keyring_namespaces(self) -> None:
        class FakeKeyring:
            def __init__(self) -> None:
                self.values: dict[tuple[str, str], str] = {}

            def set_password(self, service: str, username: str, password: str) -> None:
                self.values[(service, username)] = password

            def get_password(self, service: str, username: str) -> str | None:
                return self.values.get((service, username))

        backend = FakeKeyring()
        with patch.dict(sys.modules, {"keyring": backend}):
            polymarket = PolymarketCredentialStore()
            values = iter(("polymarket-private-key", "polymarket-wallet"))
            polymarket.configure(reader=lambda _prompt: next(values))

            binance = BinanceCredentialStore(
                environment=BINANCE_SPOT_TESTNET,
                keyring_backend=backend,
            )
            binance.configure("binance-api-key", "binance-api-secret")

            self.assertNotEqual(polymarket.service, binance.service)
            self.assertEqual(
                polymarket.load(),
                {
                    "private_key": "polymarket-private-key",
                    "wallet_address": "polymarket-wallet",
                },
            )
            loaded_binance = binance.load()
            self.assertIsNotNone(loaded_binance)
            self.assertEqual(loaded_binance.api_key, "binance-api-key")
            self.assertEqual(loaded_binance.api_secret, "binance-api-secret")

    def test_dashboard_polymarket_and_binance_routes_coexist_without_cross_projection(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = AxiomStore(str(Path(directory) / "dashboard.sqlite"))
            try:
                store.save_polymarket_market_metadata(
                    "poly-market-1",
                    {
                        "market_id": "poly-market-1",
                        "question": "Will the fixture resolve yes?",
                        "category": "integration",
                        "settlement": "open",
                    },
                    observed_at=NOW,
                    source_type="FORWARD_COLLECTED",
                )
                polymarket = _FakePolymarketControl()
                binance = _FakeBinanceFacade()
                data = DashboardData(
                    store=store,
                    control=polymarket,
                    binance_canary=binance,
                )

                polymarket_probe = _HandlerProbe(data, "/api/v2/polymarket?page=1&page_size=10")
                _DashboardHandler.do_GET(polymarket_probe)
                polymarket_status, polymarket_body = polymarket_probe.responses[-1]
                self.assertEqual(polymarket_status, 200)
                self.assertEqual(polymarket_body["items"][0]["market_id"], "poly-market-1")
                self.assertEqual(binance.snapshot_calls, 0)
                self.assertNotIn("BINANCE SPOT TESTNET", json.dumps(_jsonable(polymarket_body)))

                binance_probe = _HandlerProbe(data, "/api/v2/binance-canary?page=1&page_size=10")
                _DashboardHandler.do_GET(binance_probe)
                binance_status, binance_body = binance_probe.responses[-1]
                self.assertEqual(binance_status, 200)
                self.assertEqual(binance.snapshot_calls, 1)
                self.assertEqual(binance_body["status"]["environment"], BINANCE_SPOT_TESTNET)
                self.assertEqual(binance_body["status"]["credentials"]["configured"], True)
                self.assertNotIn("binance-api-key", json.dumps(_jsonable(binance_body)))
                self.assertNotIn("polymarket-private-key", json.dumps(_jsonable(binance_body)))
            finally:
                store.close()

    def test_each_control_route_rejects_the_other_venue_action_without_state_change(self) -> None:
        polymarket = _FakePolymarketControl()
        binance = _FakeBinanceFacade()
        data = DashboardData(control=polymarket, binance_canary=binance)

        polymarket_probe = _HandlerProbe(
            data,
            "/api/control",
            body={"action": "canary.enable_auto"},
        )
        _DashboardHandler.do_POST(polymarket_probe)
        polymarket_status, polymarket_body = polymarket_probe.responses[-1]
        self.assertEqual(polymarket_status, 200)
        self.assertTrue(polymarket_body["ok"])
        self.assertEqual(polymarket.state, "ENABLED")
        self.assertEqual(binance.calls, [])

        binance_probe = _HandlerProbe(
            data,
            "/api/binance/control",
            body={"action": "PAUSE", "payload": {}},
        )
        _DashboardHandler.do_POST(binance_probe)
        binance_status, binance_body = binance_probe.responses[-1]
        self.assertEqual(binance_status, 200)
        self.assertTrue(binance_body["ok"])
        self.assertEqual(binance.state, "PAUSED")
        self.assertEqual(polymarket.calls, [("canary.enable_auto", "", "")])

        polymarket_probe = _HandlerProbe(
            data,
            "/api/control",
            body={"action": "PAUSE"},
        )
        _DashboardHandler.do_POST(polymarket_probe)
        polymarket_status, polymarket_body = polymarket_probe.responses[-1]
        self.assertEqual(polymarket_status, 400)
        self.assertFalse(polymarket_body["ok"])
        self.assertEqual(polymarket_body["reason"], "ACTION_NOT_ALLOWED")
        self.assertEqual(polymarket.state, "ENABLED")
        self.assertEqual(binance.calls, [("PAUSE", {})])

        binance_probe = _HandlerProbe(
            data,
            "/api/binance/control",
            body={"action": "canary.enable_auto", "payload": {}},
        )
        _DashboardHandler.do_POST(binance_probe)
        binance_status, binance_body = binance_probe.responses[-1]
        self.assertEqual(binance_status, 400)
        self.assertFalse(binance_body["ok"])
        self.assertEqual(binance_body["reason"], "ACTION_NOT_ALLOWED")
        self.assertEqual(binance.state, "PAUSED")
        self.assertEqual(
            polymarket.calls,
            [("canary.enable_auto", "", ""), ("PAUSE", "", "")],
        )


if __name__ == "__main__":
    unittest.main()
