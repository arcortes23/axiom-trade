from __future__ import annotations

import json
from urllib.parse import urlparse
import unittest

from axiom.data import PolymarketAdapter
from axiom.data.polymarket import PolymarketTokenMappingError
from axiom.domain import SettlementState


class _Response:
    def __init__(self, payload: object) -> None:
        self.payload = payload
        self.status = 200
        self.headers: dict[str, str] = {}
        self.closed = False

    def read(self) -> bytes:
        return json.dumps(self.payload).encode("utf-8")

    def close(self) -> None:
        self.closed = True


GAMMA_MARKET = {
    "id": "gamma-market-42",
    "conditionId": "0xcondition42",
    "slug": "will-the-event-happen",
    "question": "Will the event happen?",
    "rules": "Resolves from the named official result.",
    "outcomes": '["No", "Yes"]',
    "clobTokenIds": '["no-token-42", "yes-token-42"]',
    "outcomePrices": '["0.40", "0.60"]',
    "active": True,
    "closed": False,
    "archived": False,
    "acceptingOrders": True,
    "enableOrderBook": True,
    "startDate": "2025-01-01T00:00:00Z",
    "endDate": "2025-02-01T00:00:00Z",
    "createdAt": "2024-12-01T00:00:00Z",
    "updatedAt": "2025-01-02T00:00:00Z",
    "orderMinSize": "5",
    "orderPriceMinTickSize": "0.01",
    "negRisk": True,
}


class PolymarketMarketScopeAdapterTests(unittest.TestCase):
    def _adapter(self, gamma: object = GAMMA_MARKET, book: object | None = None) -> PolymarketAdapter:
        def opener(request: object, *, timeout: float) -> _Response:
            path = urlparse(str(getattr(request, "full_url", request))).path
            if path.startswith("/markets/"):
                return _Response(gamma)
            if path == "/book":
                return _Response(book if book is not None else {"bids": [], "asks": []})
            raise AssertionError(f"unexpected fixture request: {path}")

        return PolymarketAdapter(opener=opener)

    def test_gamma_identity_lifecycle_provenance_and_aligned_tokens(self) -> None:
        adapter = self._adapter()
        snapshot = adapter.market("gamma-market-42")
        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertEqual(snapshot.market_id, "gamma-market-42")
        self.assertEqual(snapshot.condition_id, "0xcondition42")
        self.assertEqual(snapshot.slug, "will-the-event-happen")
        self.assertEqual(adapter.token_ids(snapshot.market_id), {"yes": "yes-token-42", "no": "no-token-42"})
        metadata = adapter.metadata(snapshot.market_id)
        self.assertIsNotNone(metadata)
        assert metadata is not None
        self.assertEqual(metadata.condition_id, "0xcondition42")
        self.assertEqual(metadata.extra["lifecycle"]["acceptingOrders"], True)
        self.assertEqual(metadata.extra["timestamps"]["updated_at"], "2025-01-02T00:00:00Z")
        self.assertEqual(metadata.extra["min_order_size"], 5.0)
        self.assertEqual(metadata.extra["tick_size"], 0.01)
        self.assertIsNotNone(snapshot.provider_timestamp)
        self.assertEqual(adapter.provider_timestamp_for(snapshot.market_id), snapshot.provider_timestamp)

    def test_absent_official_resolution_stays_unknown_even_at_terminal_prices(self) -> None:
        fixture = dict(GAMMA_MARKET, closed=True, active=False, outcomePrices='["0", "1"]')
        adapter = self._adapter(fixture)
        snapshot = adapter.market("gamma-market-42")
        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertEqual(snapshot.settlement, SettlementState.UNKNOWN)

    def test_misaligned_tokens_are_rejected_without_position_fallback(self) -> None:
        fixture = dict(GAMMA_MARKET, clobTokenIds='["only-one-token"]')
        adapter = self._adapter(fixture)
        self.assertIsNone(adapter.market("gamma-market-42"))
        self.assertEqual(adapter.token_ids("gamma-market-42"), {})
        errors = adapter.consume_validation_errors()
        self.assertTrue(any(isinstance(error, PolymarketTokenMappingError) for error in errors))

    def test_empty_book_is_available_and_unavailable_book_is_none(self) -> None:
        empty = self._adapter(book={
            "asset_id": "yes-token-42",
            "market": "0xcondition42",
            "bids": [],
            "asks": [],
            "hash": "empty-book-hash",
        })
        self.assertIsNotNone(empty.market("gamma-market-42"))
        book = empty.order_book("gamma-market-42")
        self.assertIsNotNone(book)
        assert book is not None
        self.assertEqual(book.bids, ())
        self.assertEqual(book.asks, ())
        self.assertEqual(book.book_hash, "empty-book-hash")
        self.assertIsNone(book.provider_timestamp)
        self.assertEqual(empty.provider_timestamp_for("gamma-market-42", "order_book"), None)

        unavailable = self._adapter(book={})
        self.assertIsNone(unavailable.order_book("gamma-market-42"))

    def test_book_identity_mismatch_is_rejected(self) -> None:
        adapter = self._adapter(book={
            "asset_id": "different-token",
            "market": "0xcondition42",
            "bids": [],
            "asks": [],
        })
        self.assertIsNotNone(adapter.market("gamma-market-42"))
        self.assertIsNone(adapter.order_book("gamma-market-42"))


if __name__ == "__main__":
    unittest.main()
