from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
import unittest

from axiom.market_scope import (
    DEFERRED,
    INVALID_POLICY,
    MATCHED,
    RESEARCH_ONLY,
    ZERO_MATCHES,
    MarketScopeResolution,
    resolve_market_scope,
)
from axiom.storage import AxiomStore


UTC = timezone.utc
T0 = datetime(2025, 1, 1, tzinfo=UTC)


def market(market_id: str, *, category: str = "politics", active: bool = True) -> dict[str, object]:
    return {
        "market_id": market_id,
        "condition_id": f"condition-{market_id}",
        "yes_token_id": f"yes-{market_id}",
        "no_token_id": f"no-{market_id}",
        "instrument": "POLYMARKET",
        "venue": "POLYMARKET",
        "source_type": "CURRENT",
        "active": active,
        "open": active,
        "closed": not active,
        "accepting_orders": active,
        "enable_order_book": active,
        "metadata": {"category": category},
        "metadata_provenance": {"source_type": "CURRENT", "metadata_hash": f"hash-{market_id}"},
    }

class MarketScopeResolutionTests(unittest.TestCase):
    def test_roundtrip_preserves_identity_tokens_and_provenance(self) -> None:
        policy = {"schema_version": "1", "mode": "EXACT_MARKETS", "market_ids": ["m1"]}
        result = resolve_market_scope("candidate-1", {"market_scope": policy}, [market("m1")], resolved_at=T0)
        self.assertEqual(result.status, MATCHED)
        self.assertEqual(result.matched_markets[0].condition_id, "condition-m1")
        self.assertEqual(result.matched_markets[0].yes_token_id, "yes-m1")
        self.assertEqual(result.matched_markets[0].no_token_id, "no-m1")
        with AxiomStore(":memory:") as store:
            identity = store.save_market_scope_resolution(result)
            self.assertEqual(identity, result.resolution_id)
            loaded = store.load_market_scope_resolution("candidate-1", scope_hash=result.scope_hash, scope_version="1")
            self.assertEqual(loaded, result)
            self.assertEqual(store.list_market_scope_resolutions(limit=1), [result])

    def test_statuses_distinguish_research_zero_and_deferred(self) -> None:
        with AxiomStore(":memory:") as store:
            research = resolve_market_scope(
                "research", {"market_scope": {"schema_version": "1", "mode": "RESEARCH_ONLY"}}, [market("m")], resolved_at=T0
            )
            zero = resolve_market_scope(
                "zero",
                {"market_scope": {"schema_version": "1", "mode": "RULE_BASED_MARKETS", "categories": ["politics"]}},
                [],
                resolved_at=T0,
            )
            deferred = resolve_market_scope(
                "deferred",
                {"market_scope": {"schema_version": "1", "mode": "EXACT_MARKETS", "market_ids": ["missing"]}},
                [],
                resolved_at=T0,
                max_markets=1,
            )
            self.assertEqual(research.status, RESEARCH_ONLY)
            self.assertEqual(zero.status, ZERO_MATCHES)
            self.assertEqual(deferred.status, DEFERRED)
            for result in (research, zero, deferred):
                store.save_market_scope_resolution(result)
            funnel = store.market_scope_resolution_funnel()
            self.assertEqual(funnel["total"], 3)
            self.assertEqual(funnel["status_counts"][DEFERRED], 1)

    def test_non_current_or_unavailable_markets_are_not_authorized(self) -> None:
        policy = {"schema_version": "1", "mode": "EXACT_MARKETS", "market_ids": ["m1"]}
        closed = resolve_market_scope("closed", {"market_scope": policy}, [market("m1", active=False)], resolved_at=T0)
        self.assertEqual(closed.status, ZERO_MATCHES)
        self.assertEqual(closed.excluded_markets[0].reason, "INACTIVE_MARKET")

        missing_state = market("m1")
        for key in ("accepting_orders", "enable_order_book"):
            missing_state.pop(key)
        deferred = resolve_market_scope("deferred-state", {"market_scope": policy}, [missing_state], resolved_at=T0)
        self.assertEqual(deferred.status, DEFERRED)
        self.assertIn(deferred.deferred_markets[0].reason, {"ACCEPTING_ORDERS_UNKNOWN", "ORDER_BOOK_UNKNOWN"})

    def test_exact_ids_also_require_every_policy_constraint(self) -> None:
        policy = {
            "schema_version": "1",
            "mode": "EXACT_MARKETS",
            "market_ids": [
                "good",
                "price-fail",
                "category-fail",
                "expiry-fail",
                "liquidity-fail",
                "spread-fail",
                "regime-fail",
                "instrument-fail",
                "regime-missing",
            ],
            "instrument": "POLYMARKET",
            "categories": ["politics"],
            "filters": {
                "entry_price": [0.4, 0.6],
                "minimum_hours_to_resolution": 1,
                "min_liquidity": 50,
                "max_spread": 0.05,
            },
            "regime_restrictions": {"allowed_regimes": ["calm"]},
        }

        def constrained(market_id: str, *, category: str = "politics") -> dict[str, object]:
            row = market(market_id, category=category)
            row.update(
                {
                    "expiry": T0 + timedelta(hours=2),
                    "yes_mid": 0.5,
                    "liquidity": 100.0,
                    "spread": 0.02,
                    "regime": "calm",
                }
            )
            return row

        rows = [
            constrained("good"),
            {**constrained("price-fail"), "yes_mid": 0.7},
            constrained("category-fail", category="sports"),
            {**constrained("expiry-fail"), "expiry": T0 + timedelta(minutes=30)},
            {**constrained("liquidity-fail"), "liquidity": 10.0},
            {**constrained("spread-fail"), "spread": 0.10},
            {**constrained("regime-fail"), "regime": "volatile"},
            {key: value for key, value in constrained("instrument-fail").items() if key != "instrument"},
            {key: value for key, value in constrained("regime-missing").items() if key != "regime"},
        ]

        result = resolve_market_scope("exact-filtered", {"market_scope": policy}, rows, resolved_at=T0)

        self.assertEqual(result.status, "PARTIAL")
        self.assertEqual([item.market_id for item in result.matched_markets], ["good"])
        excluded = {item.market_id: item for item in result.excluded_markets}
        self.assertEqual(
            {item.reason for item in excluded.values()},
            {"RULE_MISMATCH"},
        )
        self.assertEqual(excluded["price-fail"].detail, "PRICE_MISMATCH")
        self.assertEqual(excluded["category-fail"].detail, "CATEGORY_MISMATCH")
        self.assertEqual(excluded["expiry-fail"].detail, "EXPIRY_MISMATCH")
        self.assertEqual(excluded["liquidity-fail"].detail, "LIQUIDITY_MISMATCH")
        self.assertEqual(excluded["spread-fail"].detail, "SPREAD_MISMATCH")
        self.assertEqual(excluded["regime-fail"].detail, "REGIME_MISMATCH")
        self.assertEqual(excluded["instrument-fail"].detail, "INSTRUMENT_MISMATCH")
        self.assertEqual(excluded["regime-missing"].detail, "REGIME_MISMATCH")

    def test_exact_resolution_order_is_canonical_under_match_capacity(self) -> None:
        base_policy = {"schema_version": "1", "mode": "EXACT_MARKETS"}
        first_policy = {**base_policy, "market_ids": ["m3", "m1", "m2"]}
        second_policy = {**base_policy, "market_ids": ["m2", "m3", "m1"]}
        rows = [market("m1"), market("m2"), market("m3")]

        first = resolve_market_scope(
            "capacity-order",
            {"market_scope": first_policy},
            rows,
            resolved_at=T0,
            max_matches=2,
        )
        second = resolve_market_scope(
            "capacity-order",
            {"market_scope": second_policy},
            rows,
            resolved_at=T0,
            max_matches=2,
        )

        self.assertEqual([item.market_id for item in first.matched_markets], ["m1", "m2"])
        self.assertEqual([item.market_id for item in first.deferred_markets], ["m3"])
        self.assertEqual(first.matched_markets, second.matched_markets)
        self.assertEqual(first.deferred_markets, second.deferred_markets)
        self.assertEqual(first.scope_hash, second.scope_hash)
        self.assertEqual(first.resolution_id, second.resolution_id)

    def test_original_frozen_document_is_not_mutated(self) -> None:
        document = {"market_scope": {"schema_version": "1", "mode": "RULE_BASED_MARKETS", "categories": ["Politics"]}}
        original = deepcopy(document)
        result = resolve_market_scope("candidate-immutable", document, [market("m1")], resolved_at=T0)
        self.assertEqual(document, original)
        self.assertNotEqual(result.scope_hash, "")

    def test_malformed_and_ambiguous_legacy_scopes_fail_closed(self) -> None:
        malformed = resolve_market_scope("bad", {"target_market_ids": ["m1"], "filters": {"unknown": 1}}, [market("m1")], resolved_at=T0)
        ambiguous = resolve_market_scope(
            "ambiguous",
            {"target": {"market_ids": ["m1"]}, "target_market_ids": ["m2"]},
            [market("m1"), market("m2")],
            resolved_at=T0,
        )
        self.assertEqual(malformed.status, INVALID_POLICY)
        self.assertEqual(ambiguous.status, RESEARCH_ONLY)
        self.assertEqual(ambiguous.provenance["policy_provenance"], "legacy")


    def test_historical_constituents_never_become_rule_authority(self) -> None:
        document = {
            "market_scope": {
                "schema_version": "1",
                "mode": "RULE_BASED_MARKETS",
                "categories": ["politics"],
            },
            "dataset_selector": {"historical_market_ids": ["m1"]},
        }
        result = resolve_market_scope("historical", document, [market("m1")], resolved_at=T0)
        self.assertEqual(result.status, ZERO_MATCHES)
        self.assertEqual(result.excluded_markets[0].reason, "HISTORICAL_CONSTITUENT")

        exact = resolve_market_scope(
            "exact",
            {
                "market_scope": {
                    "schema_version": "1",
                    "mode": "EXACT_MARKETS",
                    "market_ids": ["m1"],
                },
                "dataset_selector": {"historical_market_ids": ["m1"]},
            },
            [market("m1")],
            resolved_at=T0,
        )
        self.assertEqual(exact.status, MATCHED)
if __name__ == "__main__":
    unittest.main()
