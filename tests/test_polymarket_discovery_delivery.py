from __future__ import annotations

import json
from datetime import datetime, timezone
from urllib.parse import parse_qs, urlparse
import unittest

from axiom.data.polymarket import MarketDiscoveryPage, PolymarketAdapter
from axiom.domain import PredictionMarketSnapshot, SettlementState
from axiom.market_scope import MATCHED, ZERO_MATCHES, resolve_market_scope


class _Response:
    status = 200
    headers: dict[str, str] = {}

    def __init__(self, payload: object) -> None:
        self.payload = payload

    def read(self) -> bytes:
        return json.dumps(self.payload).encode("utf-8")

    def close(self) -> None:
        return None


CONDITION = "0x" + "ab" * 32
MARKET = {
    "id": "market-delivery",
    "conditionId": CONDITION,
    "question": "Will the event happen?",
    "outcomes": '["No", "Yes"]',
    "clobTokenIds": '["no-token", "yes-token"]',
    "outcomePrices": '["0.40", "0.60"]',
    "active": True,
    "closed": False,
    "archived": False,
    "acceptingOrders": True,
    "enableOrderBook": True,
    "endDate": "2030-01-01T00:00:00Z",
}


class PolymarketDiscoveryDeliveryTests(unittest.TestCase):
    def test_market_inventory_uses_keyset_cursor_not_offsets(self) -> None:
        calls: list[str] = []

        def opener(request: object, *, timeout: float) -> _Response:
            url = str(getattr(request, "full_url", request))
            calls.append(url)
            path = urlparse(url).path
            if path == "/markets/keyset":
                query = parse_qs(urlparse(url).query)
                if "after_cursor" not in query:
                    return _Response({"markets": [MARKET], "next_cursor": "opaque-next"})
                return _Response({"markets": [], "next_cursor": None})
            if path == "/markets/market-delivery":
                return _Response(MARKET)
            if path == "/trades":
                return _Response([])
            raise AssertionError(path)

        adapter = PolymarketAdapter(opener=opener)
        rows = adapter.markets(limit=1)
        self.assertEqual([row.market_id for row in rows], ["market-delivery"])
        self.assertTrue(all(urlparse(url).path == "/markets/keyset" for url in calls))
        self.assertEqual(adapter.market_page(1).coverage_status, "PARTIAL")

    def test_public_market_trades_preserve_condition_and_resumable_offset(self) -> None:
        calls: list[str] = []

        def opener(request: object, *, timeout: float) -> _Response:
            url = str(getattr(request, "full_url", request))
            calls.append(url)
            path = urlparse(url).path
            if path == "/markets/market-delivery":
                return _Response(MARKET)
            if path == "/trades":
                query = parse_qs(urlparse(url).query)
                self.assertEqual(query["market"], [CONDITION])
                self.assertEqual(query["takerOnly"], ["true"])
                self.assertEqual(query["offset"], ["0"])
                return _Response([{
                    "conditionId": CONDITION,
                    "asset": "yes-token",
                    "side": "BUY",
                    "price": 0.6,
                    "size": 2,
                    "timestamp": 1_700_000_000,
                    "transactionHash": "tx-1",
                }])
            raise AssertionError(path)

        adapter = PolymarketAdapter(opener=opener)
        trades = adapter.trades("market-delivery", max_pages=1)
        self.assertEqual(len(trades), 1)
        self.assertEqual(trades[0].token_id, "yes-token")
        self.assertEqual(trades[0].market_id, "market-delivery")
        self.assertTrue(adapter.last_trades_complete)
        self.assertEqual(adapter.trade_provenance(trades[0])["source_type"], "FORWARD_COLLECTED")

    def test_public_trade_identity_preserves_same_transaction_multiple_fills(self) -> None:
        def opener(request: object, *, timeout: float) -> _Response:
            url = str(getattr(request, "full_url", request))
            path = urlparse(url).path
            if path == "/markets/market-delivery":
                return _Response(MARKET)
            if path == "/trades":
                row = {
                    "conditionId": CONDITION,
                    "asset": "yes-token",
                    "side": "BUY",
                    "price": 0.6,
                    "size": 2,
                    "timestamp": 1_700_000_000,
                    "transactionHash": "tx-multi-fill",
                }
                return _Response(
                    [
                        row,
                        {**row, "asset": "no-token", "price": 0.4, "size": 3},
                        row,
                    ]
                )
            raise AssertionError(path)

        adapter = PolymarketAdapter(opener=opener)
        trades = adapter.trades("market-delivery", max_pages=1)
        self.assertEqual(len(trades), 2)
        self.assertEqual({trade.token_id for trade in trades}, {"yes-token", "no-token"})
        self.assertEqual(len({trade.trade_id for trade in trades}), 2)

    def test_trade_offset_cap_keeps_inclusive_boundary_and_reports_partial_gap(self) -> None:
        rows = [
            {
                "conditionId": CONDITION,
                "asset": "yes-token",
                "side": "BUY",
                "price": 0.6,
                "size": 1,
                "timestamp": 1_700_000_000,
                "transactionHash": f"tx-{index}",
            }
            for index in range(11_000)
        ]

        def opener(request: object, *, timeout: float) -> _Response:
            del timeout
            url = str(getattr(request, "full_url", request))
            path = urlparse(url).path
            if path == "/markets/market-delivery":
                return _Response(MARKET)
            if path == "/trades":
                query = parse_qs(urlparse(url).query)
                offset = int(query["offset"][0])
                end = int(query["end"][0]) if "end" in query else None
                bounded_rows = (
                    rows
                    if end is None
                    else [row for row in rows if row["timestamp"] <= end]
                )
                return _Response(bounded_rows[offset : offset + 1_000])
            raise AssertionError(path)

        adapter = PolymarketAdapter(opener=opener)
        trades = adapter.trades("market-delivery", max_pages=100)
        self.assertEqual(len(trades), 11_000)
        self.assertFalse(adapter.last_trades_complete)
        cursor = adapter.last_trade_cursor
        self.assertIsNotNone(cursor)
        self.assertTrue(cursor.startswith("window:"))
        window_parts = cursor.split(":")
        self.assertEqual(window_parts[0:2], ["window", "1700000000.000000"])
        self.assertEqual(window_parts[2:4], ["0", "partial"])
        self.assertTrue(window_parts[4])
        self.assertEqual(window_parts[5], "1700000000.000000")

        initial_by_id = {trade.trade_id: trade for trade in trades}
        initial_trade = next(
            trade
            for trade in trades
            if adapter.trade_provenance(trade)["source_identity"]["transaction_hash"] == "tx-0"
        )
        initial_evidence = adapter.trade_provenance(initial_trade)

        replayed = adapter.trades("market-delivery", max_pages=1, cursor=cursor)
        self.assertEqual(len(replayed), 1_000)
        self.assertFalse(adapter.last_trades_complete)
        self.assertTrue({trade.trade_id for trade in replayed} <= set(initial_by_id))
        replayed_trade = next(
            trade
            for trade in replayed
            if adapter.trade_provenance(trade)["source_identity"]["transaction_hash"] == "tx-0"
        )
        self.assertEqual(replayed_trade, initial_by_id[replayed_trade.trade_id])
        self.assertEqual(adapter.trade_provenance(replayed_trade), initial_evidence)

        next_cursor = adapter.last_trade_cursor
        self.assertEqual(
            next_cursor,
            "window:1699999999.000000:0:partial:-:1700000000.000000",
        )
        self.assertEqual(adapter.trades("market-delivery", max_pages=1, cursor=next_cursor), ())
        self.assertFalse(adapter.last_trades_complete)
        self.assertEqual(adapter.last_trade_cursor, "gap:1700000000.000000")

    def test_historical_constituent_is_not_rule_scope_authority(self) -> None:
        current = {
            "market_id": "market-delivery",
            "condition_id": CONDITION,
            "yes_token_id": "yes-token",
            "no_token_id": "no-token",
            "venue": "POLYMARKET",
            "instrument": "POLYMARKET",
            "source_type": "CURRENT",
            "active": True,
            "open": True,
            "closed": False,
            "accepting_orders": True,
            "enable_order_book": True,
            "category": "politics",
        }
        result = resolve_market_scope(
            "candidate",
            {
                "market_scope": {
                    "schema_version": "1",
                    "mode": "RULE_BASED_MARKETS",
                    "categories": ["politics"],
                },
                "dataset_selector": {"historical_market_ids": ["market-delivery"]},
            },
            [current],
            resolved_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )
        self.assertEqual(result.status, ZERO_MATCHES)
        self.assertEqual(result.excluded_markets[0].reason, "HISTORICAL_CONSTITUENT")


if __name__ == "__main__":
    unittest.main()
