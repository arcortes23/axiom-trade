from __future__ import annotations

import json
from urllib.parse import parse_qs, urlparse
import unittest

from axiom.data import PolymarketAdapter
from axiom.data.polymarket import MarketDiscoveryPage, PolymarketTokenMappingError
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


    def test_keyset_page_encodes_scope_cursor_and_slug_lookup(self) -> None:
        calls: list[str] = []

        def opener(request: object, *, timeout: float) -> _Response:
            url = str(getattr(request, "full_url", request))
            calls.append(url)
            path = urlparse(url).path
            if path == "/tags/slug/politics":
                return _Response({"id": "7", "slug": "politics", "label": "Politics"})
            if path == "/markets/keyset":
                return _Response(
                    {
                        "markets": [dict(GAMMA_MARKET, tags=[{"label": "Politics", "slug": "politics"}])],
                        "next_cursor": "opaque-cursor/1",
                    }
                )
            raise AssertionError(f"unexpected fixture request: {path}")

        adapter = PolymarketAdapter(opener=opener)
        self.assertEqual(adapter.resolve_tag_slug("politics"), 7)
        page = adapter.market_page(
            2,
            tag_ids=(7, 11),
            liquidity_num_min=1000,
            end_date_min="2025-01-01T00:00:00Z",
            end_date_max="2025-02-01T00:00:00Z",
        )
        self.assertIsInstance(page, MarketDiscoveryPage)
        self.assertEqual(page.request_path, "/markets/keyset")
        self.assertEqual(page.next_cursor, "opaque-cursor/1")
        self.assertEqual(page.coverage_status, "PARTIAL")
        query = parse_qs(urlparse(calls[-1]).query)
        self.assertEqual(urlparse(calls[-1]).path, "/markets/keyset")
        self.assertEqual(query["limit"], ["2"])
        self.assertEqual(query["closed"], ["false"])
        self.assertEqual(query["include_tag"], ["true"])
        self.assertEqual(query["tag_id[]"], ["7", "11"])
        self.assertEqual(query["liquidity_num_min"], ["1000"])
        self.assertEqual(query["end_date_min"], ["2025-01-01T00:00:00Z"])
        self.assertEqual(query["end_date_max"], ["2025-02-01T00:00:00Z"])
        self.assertNotIn("after_cursor", query)
        metadata = adapter.metadata("gamma-market-42")
        self.assertIsNotNone(metadata)
        assert metadata is not None
        self.assertEqual(metadata.extra["lifecycle"]["acceptingOrders"], True)
        self.assertEqual(metadata.tags, ("Politics",))

        cursor_page = adapter.market_page(
            2,
            after_cursor="opaque-cursor/1",
            tag_ids=(7, 11),
            liquidity_num_min=1000,
            end_date_min="2025-01-01T00:00:00Z",
            end_date_max="2025-02-01T00:00:00Z",
        )
        self.assertEqual(page.query_fingerprint, cursor_page.query_fingerprint)
        self.assertEqual(parse_qs(urlparse(calls[-1]).query)["after_cursor"], ["opaque-cursor/1"])
        self.assertFalse(any(urlparse(url).path in {"/orders", "/order", "/trades"} for url in calls))
        self.assertEqual(adapter.trades("gamma-market-42"), ())

    def test_keyset_page_accounts_for_duplicates_and_malformed_rows(self) -> None:
        calls: list[str] = []

        def opener(request: object, *, timeout: float) -> _Response:
            url = str(getattr(request, "full_url", request))
            calls.append(url)
            if len(calls) == 1:
                return _Response(
                    {
                        "markets": [
                            GAMMA_MARKET,
                            dict(GAMMA_MARKET),
                            None,
                            {"question": "missing identity"},
                        ],
                        "next_cursor": "opaque-next",
                    }
                )
            return _Response({"markets": [GAMMA_MARKET]})

        adapter = PolymarketAdapter(opener=opener)
        page = adapter.market_page(4)
        self.assertEqual(page.raw_count, 4)
        self.assertEqual(page.unique_count, 1)
        self.assertEqual(page.duplicate_count, 1)
        self.assertEqual(page.malformed_count, 2)
        self.assertEqual(page.coverage_status, "PARTIAL")
        self.assertEqual(page.next_cursor, "opaque-next")
        complete = adapter.market_page(4, after_cursor=page.next_cursor)
        self.assertEqual(complete.raw_count, 1)
        self.assertEqual(complete.unique_count, 1)
        self.assertEqual(complete.coverage_status, "COMPLETE")
        self.assertIsNone(complete.next_cursor)

    def test_keyset_page_rejects_repeated_cursor_without_continuation(self) -> None:
        calls: list[str] = []

        def opener(request: object, *, timeout: float) -> _Response:
            url = str(getattr(request, "full_url", request))
            calls.append(url)
            self.assertEqual(urlparse(url).path, "/markets/keyset")
            return _Response({"markets": [GAMMA_MARKET], "next_cursor": "opaque-repeat"})

        adapter = PolymarketAdapter(opener=opener)
        page = adapter.market_page(2, after_cursor="opaque-repeat")

        self.assertEqual(len(calls), 1)
        self.assertEqual(page.raw_count, 1)
        self.assertEqual(page.unique_count, 1)
        self.assertEqual(page.coverage_status, "ERROR")
        self.assertEqual(page.error_reason, "REPEATED_CURSOR")
        self.assertIsNone(page.next_cursor)
        query = parse_qs(urlparse(calls[0]).query)
        self.assertEqual(query["after_cursor"], ["opaque-repeat"])

    def test_keyset_fingerprint_changes_with_filters_not_cursor(self) -> None:
        def opener(request: object, *, timeout: float) -> _Response:
            return _Response({"markets": [], "next_cursor": None})

        adapter = PolymarketAdapter(opener=opener)
        first = adapter.market_page(2, liquidity_num_min=1000)
        larger_page = adapter.market_page(100, liquidity_num_min=1000)
        self.assertEqual(first.query_fingerprint, larger_page.query_fingerprint)
        cursor = adapter.market_page(2, after_cursor="opaque", liquidity_num_min=1000)
        changed = adapter.market_page(2, liquidity_num_min=1001)
        self.assertEqual(first.query_fingerprint, cursor.query_fingerprint)
        self.assertNotEqual(first.query_fingerprint, changed.query_fingerprint)
        self.assertEqual(first.coverage_status, "COMPLETE")

if __name__ == "__main__":
    unittest.main()
