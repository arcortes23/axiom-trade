from __future__ import annotations

import json
from decimal import Decimal
from urllib.error import HTTPError
from urllib.request import Request, urlopen
import unittest

from axiom.dashboard import DashboardData, DashboardServer


class FakeBinanceCanary:
    def __init__(self, reference_hash: object = "a" * 64) -> None:
        self.calls: list[tuple[str, dict[str, object]]] = []
        self._actions: list[dict[str, object]] = []
        self.reference_hash = reference_hash
        self._positions = [
            {"position_id": f"position-{index}-with-a-complete-identifier", "symbol": "BTCUSDT", "quantity": "1"}
            for index in range(150)
        ]

    def status(self) -> dict[str, object]:
        return {
            "timestamp": "2026-01-01T00:00:00+00:00",
            "profile": {
                "environment": "TESTNET",
                "feature_instance": "binance-dashboard-test",
                "db_path": ":memory:",
                "revision": "schema-r7",
            },
            "transport": {"binance": "ENABLED", "polymarket": "DISABLED"},
            "credentials": {
                "configured": True,
                "reference_hash": self.reference_hash,
                "api_key": "never-return-this-key",
                "api_secret": {"nested": ["never-return-this-secret"]},
                "authorization": {"bearer": "never-return-this-authorization"},
            },
            "safe_json": json.dumps(
                {
                    "credentials": {"token": "never-return-this-json-token"},
                    "nested": [{"api_secret": "never-return-this-json-secret"}],
                }
            ),
            "connectivity": {"status": "READY", "checked_at": "2026-01-01T00:00:00+00:00", "stale": False},
            "readiness": {"status": "READY", "ready": True},
            "qualification": {"current_vs_stale": "CURRENT", "selection": {"strategy_id": "strategy-full-id"}, "family": "momentum"},
            "risk": {"limits": {"entry_notional": "10.00"}, "remaining": {"entry_notional": "10.00"}, "net_pnl": "0.00", "fees": "0.00"},
        }

    def snapshot(self, *, page: int, page_size: int) -> dict[str, object]:
        start = (page - 1) * page_size
        items = self._positions[start : start + page_size]
        page_data = {"page": page, "page_size": page_size, "total": len(self._positions), "items": items, "detail_records": items}
        return {"status": self.status(), "positions": page_data, "orders": page_data, "fills": page_data, "unknown": {**page_data, "items": []}}

    def action(self, action: str, payload: dict[str, object]) -> dict[str, object]:
        self.calls.append((action, payload))
        result = {"ok": True, "action": action.upper(), "action_id": f"persisted-{len(self.calls)}", "result": {"state": "PAUSED"}}
        self._actions.insert(0, result)
        return result

    def list_actions(self, *, limit: int = 25) -> list[dict[str, object]]:
        return self._actions[:limit]


class FakeTestnetFacade(FakeBinanceCanary):
    strict_testnet = True

    def status(self) -> dict[str, object]:
        return {
            "title": "BINANCE SPOT TESTNET",
            "strict_testnet": True,
            "environment": "BINANCE_SPOT_TESTNET",
            "profile": {
                "environment": "TESTNET",
                "identity": "binance-testnet",
                "db_path": "runtime-data/binance-testnet.sqlite",
            },
            "credentials": {
                "configured": True,
                "api_key": "testnet-api-key-secret",
                "api_secret": "testnet-api-secret",
            },
            "connectivity": {
                "status": "PASS",
                "checked_at": {
                    "utc": "2026-01-01T00:00:00+00:00",
                    "pht": "2026-01-01T08:00:00+08:00",
                },
                "authentication": "PASS",
                "server_time_ms": 1767225600000,
                "account": {
                    "account_type": "SPOT",
                    "can_trade": True,
                    "balances": [{"asset": "USDT", "free": "10", "locked": "0"}],
                },
            },
            "validation": {
                "status": "PASS",
                "symbol": "BTCUSDT",
                "side": "BUY",
                "price": "100.00",
                "quantity": "0.01",
                "fee_reserve": "0.001",
                "reservation": {"status": "HELD"},
            },
            "probe": {
                "status": "FILLED",
                "label": "TESTNET EXECUTION PROBE",
                "intent": {
                    "exchange_order_id": "exchange-entry-1",
                    "client_order_id": "client-entry-1",
                },
                "fills": [{"quantity": "0.01", "commission": "0.00001"}],
                "owned_quantity": "0.00999",
                "exit": {
                    "state": "FILLED",
                    "exchange_order_id": "exchange-exit-1",
                    "client_order_id": "client-exit-1",
                },
                "realized_pnl": "0.10",
                "reconciliation": {"status": "PASS"},
            },
            "isolation": {"status": "ISOLATED"},
            "autonomous": {
                "enabled": False,
                "state": "BLOCKED",
                "blocked_reason": "NO_STRATEGY",
                "selected_candidate": None,
                "current_signal": None,
                "no_trade_reason": "AUTONOMOUS_BLOCKED",
                "risk_envelope": {"entry_notional": "10"},
                "bounded_window": None,
            },
            "enable_phrase": "ENABLE BINANCE TESTNET AUTO CANARY",
            "probe_confirmation": "RUN BINANCE TESTNET EXECUTION PROBE",
        }


class FakePaperFacade(FakeBinanceCanary):
    def status(self) -> dict[str, object]:
        result = super().status()
        result["profile"] = {**result["profile"], "environment": "PAPER"}
        result["environment"] = "PAPER"
        return result


class FakeForeignTestnetFacade(FakeBinanceCanary):
    """TESTNET-shaped facade without the strict control-plane marker."""

    def status(self) -> dict[str, object]:
        result = super().status()
        result["title"] = "BINANCE SPOT TESTNET"
        result["environment"] = "BINANCE_SPOT_TESTNET"
        return result



class SecretReference:
    def __init__(self, value: str) -> None:
        self.value = value

    def __str__(self) -> str:
        return self.value


class BinanceDashboardTests(unittest.TestCase):
    def test_constructor_without_binance_remains_valid(self) -> None:
        data = DashboardData()
        self.assertFalse(data.binance_canary_data()["available"])

    def test_real_testnet_control_marker_sets_initial_label_without_status_io(self) -> None:
        from unittest.mock import patch

        from axiom.binance_operator import BinanceTestnetControlPlane

        gate = FakeTestnetFacade()
        control = BinanceTestnetControlPlane(gate)
        with patch.object(control, "status", side_effect=AssertionError("status must not drive initial label")):
            self.assertEqual(DashboardData(binance_canary=control).binance_nav_label(), "BINANCE SPOT TESTNET")

    def test_hermes_projection_preserves_json_safe_nested_scalars(self) -> None:
        payload = {
            "metrics": {
                "enabled": True,
                "count": 7,
                "notional": Decimal("12.3400"),
                "score": 0.875,
                "overflow": float("inf"),
            }
        }
        item = {
            "item_id": "scalar-projection",
            "status": "COMPLETED",
            "payload": payload,
        }
        server = DashboardServer(
            port=0,
            data=DashboardData(data={"hermes": {"items": [item]}}),
        ).start()
        self.addCleanup(server.stop)
        assert server.url is not None

        with urlopen(server.url + "/api/v2/hermes?page=1&page_size=10", timeout=3) as response:
            projected = json.load(response)

        metrics = projected["items"][0]["payload"]["metrics"]
        self.assertEqual(
            metrics,
            {
                "enabled": True,
                "count": 7,
                "notional": "12.3400",
                "score": 0.875,
                "overflow": None,
            },
        )


    def test_ephemeral_server_separates_binance_and_polymarket(self) -> None:
        fake = FakeBinanceCanary()
        server = DashboardServer(port=0, data=DashboardData(binance_canary=fake)).start()
        self.addCleanup(server.stop)
        assert server.url is not None

        with urlopen(server.url + "/", timeout=3) as response:
            html = response.read().decode("utf-8")
        self.assertIn("Polymarket Canary", html)
        self.assertIn("BINANCE SPOT CANARY", html)
        self.assertIn("POLYMARKET TRANSPORT: DISABLED", html)
        self.assertIn("/api/v2/binance-canary", html)

        with urlopen(server.url + "/api/v2/canary", timeout=3) as response:
            polymarket = json.load(response)
        with urlopen(server.url + "/api/v2/binance-canary?page=2&page_size=10", timeout=3) as response:
            binance_body = response.read()
        binance = json.loads(binance_body)
        self.assertNotEqual(polymarket.get("canary"), binance.get("status"))
        self.assertEqual(binance["page"], 2)
        self.assertLessEqual(len(binance["positions"]["items"]), 10)
        credentials = binance["status"]["credentials"]
        self.assertEqual(
            credentials,
            {"configured": True, "reference_hash": "a" * 64},
        )
        self.assertEqual(len(credentials["reference_hash"]), 64)
        self.assertNotIn("never-return-this-key", binance_body.decode())
        self.assertNotIn("never-return-this-secret", binance_body.decode())
        self.assertNotIn("never-return-this-authorization", binance_body.decode())
        self.assertNotIn("never-return-this-json-token", binance_body.decode())
        self.assertNotIn("never-return-this-json-secret", binance_body.decode())

        request = Request(
            server.url + "/api/binance/control",
            data=json.dumps({"action": "PAUSE", "payload": {"reason": "dashboard test"}}).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with self.assertRaises(HTTPError) as denied:
            urlopen(request, timeout=3)
        self.assertEqual(denied.exception.code, 403)

        token = server._server.control_token  # type: ignore[union-attr]
        request.add_header("X-Axiom-Control-Token", token)
        with urlopen(request, timeout=3) as response:
            result = json.load(response)
        self.assertTrue(result["ok"])
        self.assertEqual(fake.calls, [("PAUSE", {"reason": "dashboard test"})])
        self.assertEqual(result["action_id"], "persisted-1")

    def test_testnet_json_projection_is_named_bounded_and_secret_free(self) -> None:
        fake = FakeTestnetFacade()
        projected = DashboardData(binance_canary=fake).binance_canary_data(
            {"page": 1, "page_size": 100}
        )
        self.assertEqual(projected["title"], "BINANCE SPOT TESTNET")
        self.assertEqual(projected["status"]["title"], "BINANCE SPOT TESTNET")
        self.assertEqual(
            projected["profile"]["db_path"],
            "runtime-data/binance-testnet.sqlite",
        )
        self.assertEqual(projected["probe"]["label"], "TESTNET EXECUTION PROBE")
        self.assertEqual(projected["autonomous"]["blocked_reason"], "NO_STRATEGY")
        self.assertEqual(
            projected["status"]["credentials"],
            {"configured": True, "reference_hash": None},
        )
        self.assertIs(projected["strict_testnet"], True)
        self.assertIs(projected["status"]["strict_testnet"], True)
        self.assertLessEqual(len(projected["positions"]["items"]), 100)
        encoded = json.dumps(projected)
        self.assertNotIn("testnet-api-key-secret", encoded)
        self.assertNotIn("testnet-api-secret", encoded)
        self.assertNotIn("probe_confirmation", encoded)

    def test_testnet_http_surface_rejects_order_bearing_actions(self) -> None:
        fake = FakeTestnetFacade()
        server = DashboardServer(
            port=0, data=DashboardData(binance_canary=fake)
        ).start()
        self.addCleanup(server.stop)
        assert server.url is not None

        with urlopen(server.url + "/", timeout=3) as response:
            html = response.read().decode("utf-8")
        self.assertIn("BINANCE SPOT TESTNET", html)
        self.assertIn("TESTNET CONNECTIVITY", html)
        self.assertIn("TESTNET EXECUTION PROBE", html)
        self.assertIn("No browser action can place an order.", html)

        with urlopen(server.url + "/api/v2/binance-canary", timeout=3) as response:
            projected = json.load(response)
        self.assertEqual(projected["probe"]["intent"]["client_order_id"], "client-entry-1")
        self.assertEqual(projected["probe"]["fills"][0]["commission"], "0.00001")
        self.assertEqual(projected["probe"]["realized_pnl"], "0.10")
        self.assertEqual(projected["probe"]["reconciliation"]["status"], "PASS")

        token = server._server.control_token  # type: ignore[union-attr]
        for action, payload in (
            (
                "EXECUTION_PROBE",
                {"confirmation": "RUN BINANCE TESTNET EXECUTION PROBE"},
            ),
            ("RECONCILE_PROBE", {}),
        ):
            request = Request(
                server.url + "/api/binance/control",
                data=json.dumps({"action": action, "payload": payload}).encode(),
                headers={
                    "Content-Type": "application/json",
                    "X-Axiom-Control-Token": token,
                },
                method="POST",
            )
            with self.assertRaises(HTTPError) as denied:
                urlopen(request, timeout=3)
            self.assertEqual(denied.exception.code, 403)
            denied_body = json.load(denied.exception)
            self.assertEqual(
                denied_body,
                {
                    "action": action,
                    "ok": False,
                    "reason": "BROWSER_ACTION_FORBIDDEN",
                },
            )
        self.assertEqual(fake.calls, [])

        for action in ("CONNECTIVITY_CHECK", "ORDER_VALIDATION_TEST"):
            request = Request(
                server.url + "/api/binance/control",
                data=json.dumps({"action": action, "payload": {}}).encode(),
                headers={
                    "Content-Type": "application/json",
                    "X-Axiom-Control-Token": token,
                },
                method="POST",
            )
            with urlopen(request, timeout=3) as response:
                allowed = json.load(response)
            self.assertTrue(allowed["ok"])
        self.assertEqual(
            fake.calls,
            [("CONNECTIVITY_CHECK", {}), ("ORDER_VALIDATION_TEST", {})],
        )

    def test_foreign_testnet_shape_without_marker_keeps_legacy_projection(self) -> None:
        fake = FakeForeignTestnetFacade()
        projected = DashboardData(binance_canary=fake).binance_canary_data()

        self.assertFalse(
            DashboardData._binance_testnet_status(projected["status"], projected)
        )
        self.assertEqual(projected["environment"], "BINANCE_SPOT_TESTNET")
        self.assertEqual(projected["title"], "BINANCE SPOT TESTNET")
        self.assertNotIn("strict_testnet", projected)
        self.assertNotIn("strict_testnet", projected["status"])
        self.assertFalse(
            DashboardData._binance_testnet_status(
                {
                    "environment": "BINANCE_SPOT_TESTNET",
                    "title": "BINANCE SPOT TESTNET",
                    "strict_testnet": "true",
                }
            )
        )
        self.assertNotIn("enable_phrase", projected["status"])
        self.assertNotIn("probe_confirmation", projected["status"])


    def test_initial_binance_nav_label_uses_only_strict_facade_marker(self) -> None:
        cases = (
            (FakeTestnetFacade(), "BINANCE SPOT TESTNET"),
            (FakeForeignTestnetFacade(), "BINANCE SPOT CANARY"),
            (FakePaperFacade(), "BINANCE SPOT CANARY"),
        )
        for facade, expected in cases:
            with self.subTest(facade=type(facade).__name__):
                server = DashboardServer(
                    port=0, data=DashboardData(binance_canary=facade)
                ).start()
                try:
                    assert server.url is not None
                    with urlopen(server.url + "/", timeout=3) as response:
                        html = response.read().decode("utf-8")
                finally:
                    server.stop()
                nav_label = f'data-view="binance-canary">{expected}</button>'
                self.assertIn(nav_label, html)
                other = (
                    "BINANCE SPOT CANARY"
                    if expected == "BINANCE SPOT TESTNET"
                    else "BINANCE SPOT TESTNET"
                )
                self.assertNotIn(
                    f'data-view="binance-canary">{other}</button>',
                    html,
                )

    def test_paper_projection_keeps_legacy_environment_shape(self) -> None:
        projected = DashboardData(
            binance_canary=FakePaperFacade()
        ).binance_canary_data()
        self.assertEqual(projected["environment"], "PAPER")
        self.assertNotEqual(projected.get("title"), "BINANCE SPOT TESTNET")
        self.assertEqual(projected["status"]["profile"]["environment"], "PAPER")

    def test_binance_credentials_reference_hash_is_strictly_validated(self) -> None:
        nested_secret = "nested-reference-secret"
        malformed_values = (
            {"token": nested_secret},
            [nested_secret],
            "ref-" + "a" * 256,
            "g" * 64,
            "a" * 63,
            b"secret-reference-bytes",
            42,
            SecretReference(nested_secret),
        )
        for value in malformed_values:
            with self.subTest(value_type=type(value).__name__):
                fake = FakeBinanceCanary(reference_hash=value)
                projected = DashboardData(binance_canary=fake).binance_canary_data()
                credentials = projected["status"]["credentials"]
                self.assertEqual(
                    credentials,
                    {"configured": True, "reference_hash": None},
                )
                self.assertNotIn(nested_secret, json.dumps(projected, default=str))


if __name__ == "__main__":
    unittest.main()
