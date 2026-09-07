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


class SecretReference:
    def __init__(self, value: str) -> None:
        self.value = value

    def __str__(self) -> str:
        return self.value


class BinanceDashboardTests(unittest.TestCase):
    def test_constructor_without_binance_remains_valid(self) -> None:
        data = DashboardData()
        self.assertFalse(data.binance_canary_data()["available"])

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
        self.assertIn("Binance Spot Canary", html)
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
