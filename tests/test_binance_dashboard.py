from __future__ import annotations

import json
from urllib.error import HTTPError
from urllib.request import Request, urlopen
import unittest

from axiom.dashboard import DashboardData, DashboardServer


class FakeBinanceCanary:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, object]]] = []
        self._actions: list[dict[str, object]] = []
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
            "credentials": {"configured": True, "api_key": "never-return-this-key", "api_secret": "never-return-this-secret"},
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


class BinanceDashboardTests(unittest.TestCase):
    def test_constructor_without_binance_remains_valid(self) -> None:
        data = DashboardData()
        self.assertFalse(data.binance_canary_data()["available"])

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
        self.assertEqual(
            binance["positions"]["detail_records"][0]["position_id"],
            "position-10-with-a-complete-identifier",
        )
        self.assertNotIn("never-return-this-key", binance_body.decode())
        self.assertNotIn("never-return-this-secret", binance_body.decode())

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


if __name__ == "__main__":
    unittest.main()
