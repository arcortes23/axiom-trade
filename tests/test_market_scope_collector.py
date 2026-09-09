from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
import unittest

from axiom.collector import CollectorConfig, PolymarketCollector
from axiom.data import InMemoryPredictionProvider
from axiom.domain import (
    InstrumentMetadata,
    MarketType,
    OrderBookLevel,
    OrderBookSnapshot,
    PredictionMarketSnapshot,
    SettlementState,
)
from axiom.market_scope import (
    MATCHED,
    RESEARCH_ONLY,
    ZERO_MATCHES,
    resolve_market_scope,
)


UTC = timezone.utc
T0 = datetime(2026, 1, 1, tzinfo=UTC)


def market(
    market_id: str,
    *,
    category: str | None = None,
    tags: tuple[str, ...] = (),
    settlement: SettlementState = SettlementState.OPEN,
    closed: bool | None = False,
    yes_mid: float = 0.50,
) -> PredictionMarketSnapshot:
    book = OrderBookSnapshot(
        T0,
        (OrderBookLevel(max(0.0, yes_mid - 0.01), 10.0),),
        (OrderBookLevel(min(1.0, yes_mid + 0.01), 10.0),),
        f"yes-{market_id}",
    )
    return PredictionMarketSnapshot(
        timestamp=T0,
        market_id=market_id,
        question=f"Will {market_id} happen?",
        yes_bid=max(0.0, yes_mid - 0.01),
        yes_ask=min(1.0, yes_mid + 0.01),
        yes_mid=yes_mid,
        no_bid=max(0.0, 1.0 - yes_mid - 0.01),
        no_ask=min(1.0, 1.0 - yes_mid + 0.01),
        no_mid=1.0 - yes_mid,
        volume=1_000.0,
        liquidity=100.0,
        expiry=T0 + timedelta(days=10),
        settlement=settlement,
        category=category,
        tags=tags,
        order_book=book,
        condition_id=f"condition-{market_id}",
        yes_token_id=f"yes-{market_id}",
        no_token_id=f"no-{market_id}",
        active=not bool(closed),
        closed=closed,
        accepting_orders=True,
        enable_order_book=True,
    )


def scope(mode: str, *, market_ids: tuple[str, ...] = (), category: str | None = None) -> dict[str, object]:
    return {
        "schema_version": "1",
        "mode": mode,
        # Canonical RESEARCH_ONLY forbids an instrument or any other
        # authority-bearing constraint; all forward modes require Polymarket.
        "instrument": None if mode == "RESEARCH_ONLY" else "POLYMARKET",
        "categories": [category] if category else [],
        "market_ids": list(market_ids),
        "filters": {},
        "regime_restrictions": {},
        "provenance": "canonical",
    }


class _RecordingProvider(InMemoryPredictionProvider):
    provider_name = "offline-scope-fixture"

    def __init__(self, markets: tuple[PredictionMarketSnapshot, ...]) -> None:
        super().__init__(markets)
        self.markets_calls = 0
        self.markets_active: list[bool] = []
        self.market_calls: list[str] = []

    def markets(self, active: bool = True):
        self.markets_calls += 1
        self.markets_active.append(active)
        return super().markets(active=active)

    def market(self, market_id: str):
        self.market_calls.append(str(market_id))
        return super().market(market_id)

    def metadata(self, market_id: str):
        snapshot = self.market(market_id)
        if snapshot is None:
            return None
        return InstrumentMetadata(
            symbol=str(market_id),
            market_type=MarketType.PREDICTION,
            provider=self.provider_name,
            market_id=str(market_id),
            question=snapshot.question,
            category=snapshot.category,
            tags=snapshot.tags,
            expiry=snapshot.expiry,
        )


class _ScopeStore:
    def __init__(
        self,
        documents: dict[str, dict[str, object]],
        *,
        fail_resolver: bool = False,
        fail_saver: bool = False,
    ) -> None:
        self.documents = {
            candidate_id: {"candidate_id": candidate_id, "stage": "FROZEN", "payload": payload}
            for candidate_id, payload in documents.items()
        }
        self.fail_resolver = fail_resolver
        self.fail_saver = fail_saver
        self.resolutions: list[object] = []
        self.states: dict[str, dict[str, object]] = {}
        self.errors: list[tuple[object, ...]] = []
        self.requirement_calls: list[tuple[str, ...]] = []

    def load_candidate_lifecycle(self, candidate_id: str | None = None, *, limit: int = 1000):
        if candidate_id is not None:
            return self.documents.get(str(candidate_id))
        return list(self.documents.values())[:limit]

    def resolve_market_scope(self, *args, **kwargs):
        if self.fail_resolver:
            raise RuntimeError("resolver unavailable")
        return resolve_market_scope(*args, **kwargs)

    def save_market_scope_resolution(self, result, *, if_absent: bool = True):
        if self.fail_saver:
            raise RuntimeError("scope store unavailable")
        del if_absent
        self.resolutions.append(result)
        return result.resolution_id
    def candidate_forward_requirements(self, candidate_ids=None, **kwargs):
        del kwargs
        self.requirement_calls.append(tuple(candidate_ids or ()))
        if not candidate_ids:
            return {}
        # A deliberately tempting legacy authority; scope candidates must not
        # reach this projection after the new resolver is engaged.
        return {"market_ids": ["legacy-leak"]}

    def get_collector_state(self, key: str):
        return dict(self.states.get(str(key), {}))

    def set_collector_state(self, key: str, payload):
        self.states[str(key)] = dict(payload)

    def save_collection_error(self, market_id, observed_at, kind, detail):
        self.errors.append((market_id, observed_at, kind, detail))

    def save_polymarket_market_metadata(self, *args, **kwargs):
        return True

    def save_polymarket_snapshot(self, *args, **kwargs):
        return True

    def save_polymarket_trade(self, *args, **kwargs):
        return True

    def load_polymarket_snapshots(self, *args, **kwargs):
        return []

    def save_collection_cycle(self, *args, **kwargs):
        return None

    def save_dataset_catalog(self, *args, **kwargs):
        return None


class _ScopeCollector(PolymarketCollector):
    def __init__(self, *args, candidate_ids: tuple[str, ...], **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._candidate_ids = candidate_ids

    def _active_primary_candidate_ids(self) -> list[str]:
        return list(self._candidate_ids)


class MarketScopeCollectorTests(unittest.TestCase):
    def _collector(
        self,
        provider: _RecordingProvider,
        store: _ScopeStore,
        candidate_ids: tuple[str, ...],
        *,
        max_markets: int = 10,
    ) -> _ScopeCollector:
        return _ScopeCollector(
            provider,
            store,
            CollectorConfig(
                max_markets=max_markets,
                discovery_budget_per_cycle=10,
                max_attempts=1,
                backoff_initial_seconds=0,
                jitter_seconds=0,
            ),
            candidate_ids=candidate_ids,
            clock=lambda: T0,
            sleep=lambda _seconds: None,
        )

    def test_shared_bounded_discovery_and_only_resolved_markets_are_scheduled(self) -> None:
        fixtures = (
            market("politics-match", category="politics", tags=("election",)),
            market("exact-match", category="economics"),
            market("title-only", category="economics"),
            market("closed-politics", category="politics", settlement=SettlementState.RESOLVED_YES, closed=True),
        )
        store = _ScopeStore(
            {
                "rule-candidate": {"experiment_plan": {"market_scope": scope("RULE_BASED_MARKETS", category="politics")}},
                "exact-candidate": {"experiment_plan": {"market_scope": scope("EXACT_MARKETS", market_ids=("exact-match",))}},
            }
        )
        provider = _RecordingProvider(fixtures)
        cycle = self._collector(provider, store, ("rule-candidate", "exact-candidate")).collect_once(now=T0)
        self.assertEqual(provider.markets_calls, 1)
        self.assertEqual(provider.markets_active, [True])
        self.assertEqual(list(cycle.candidate_bound_scheduled), ["politics-match", "exact-match"])
        self.assertEqual(list(cycle.discovery_scheduled), [])
        self.assertNotIn("title-only", cycle.candidate_bound_scheduled)
        self.assertNotIn("closed-politics", cycle.candidate_bound_scheduled)
        self.assertEqual(len(store.resolutions), 2)
        self.assertEqual({item.status for item in store.resolutions}, {MATCHED})
        self.assertEqual(set(provider.market_calls), {"politics-match", "exact-match"})

    def test_exact_and_rule_resolution_persist_exclusion_and_defer_taxonomy(self) -> None:
        provider = _RecordingProvider(
            (
                market("rule-match", category="politics"),
                market("title-only", category="economics"),
                market("closed", category="politics", settlement=SettlementState.RESOLVED_NO, closed=True),
            )
        )
        records = [PolymarketCollector._scope_market_record(item, T0, provider) for item in provider._markets.values()]
        exact = resolve_market_scope(
            "exact",
            {"market_scope": scope("EXACT_MARKETS", market_ids=("rule-match", "closed", "missing"))},
            records,
            resolved_at=T0,
        )
        self.assertEqual(exact.status, "PARTIAL")
        self.assertEqual([item.market_id for item in exact.matched_markets], ["rule-match"])
        self.assertTrue({item.reason for item in exact.excluded_markets} & {"INACTIVE_MARKET", "MARKET_CLOSED", "RESOLVED_MARKET"})
        self.assertEqual({item.reason for item in exact.deferred_markets}, {"NOT_OBSERVED"})

        rule = resolve_market_scope(
            "rule",
            {"market_scope": scope("RULE_BASED_MARKETS", category="politics")},
            records,
            resolved_at=T0,
        )
        self.assertEqual(rule.status, MATCHED)
        self.assertEqual([item.market_id for item in rule.matched_markets], ["rule-match"])
        self.assertIn("RULE_MISMATCH", {item.reason for item in rule.excluded_markets})
        self.assertTrue({item.reason for item in rule.excluded_markets} & {"INACTIVE_MARKET", "MARKET_CLOSED", "RESOLVED_MARKET"})

    def test_missing_lifecycle_flags_defer_instead_of_being_synthesized(self) -> None:
        provider = _RecordingProvider(
            (
                market("active-unknown"),
                market("closed-unknown", settlement=SettlementState.RESOLVED_YES),
                market("accepting-unknown"),
                market("book-unknown"),
            )
        )
        snapshots = {
            "active-unknown": replace(
                provider._markets["active-unknown"],
                active=None,
                closed=False,
            ),
            "closed-unknown": replace(
                provider._markets["closed-unknown"],
                active=True,
                closed=None,
            ),
            "accepting-unknown": replace(
                provider._markets["accepting-unknown"],
                active=True,
                closed=False,
                accepting_orders=None,
            ),
            "book-unknown": replace(
                provider._markets["book-unknown"],
                active=True,
                closed=False,
                enable_order_book=None,
            ),
        }
        expected = {
            "active-unknown": ("ACTIVE_UNKNOWN",),
            "closed-unknown": ("CLOSED_UNKNOWN",),
            "accepting-unknown": ("ACCEPTING_ORDERS_UNKNOWN",),
            "book-unknown": ("ORDER_BOOK_UNKNOWN",),
        }
        for market_id, snapshot in snapshots.items():
            record = PolymarketCollector._scope_market_record(snapshot, T0, provider)
            if market_id == "active-unknown":
                self.assertIsNone(record["active"])
                self.assertIsNone(record["open"])
            if market_id == "closed-unknown":
                self.assertIsNone(record["closed"])
                self.assertEqual(record["settlement"], SettlementState.RESOLVED_YES.value)
            if market_id == "accepting-unknown":
                self.assertIsNone(record["accepting_orders"])
                self.assertIsNone(record["acceptingOrders"])
            if market_id == "book-unknown":
                self.assertIsNone(record["book"])
                self.assertIsNone(record["book_available"])
                self.assertIsNone(record["order_book_available"])
            result = resolve_market_scope(
                market_id,
                {"market_scope": scope("EXACT_MARKETS", market_ids=(market_id,))},
                [record],
                resolved_at=T0,
            )
            self.assertEqual(
                {item.reason for item in result.deferred_markets},
                set(expected[market_id]),
            )

    def test_historical_constituent_is_not_current_authority_and_title_is_not_a_fallback(self) -> None:
        fixtures = (market("historical-politics", category="politics"), market("title-only", category="economics"))
        store = _ScopeStore(
            {
                "candidate": {
                    "experiment_plan": {"market_scope": scope("RULE_BASED_MARKETS", category="politics")},
                    "dataset_provenance": {
                        "source_type": "HISTORICAL",
                        "historical_market_ids": ["historical-politics"],
                    },
                }
            }
        )
        provider = _RecordingProvider(fixtures)
        cycle = self._collector(provider, store, ("candidate",)).collect_once(now=T0)

        self.assertEqual(list(cycle.candidate_bound_scheduled), [])
        self.assertEqual(len(store.resolutions), 1)
        resolution = store.resolutions[0]
        self.assertEqual([item.market_id for item in resolution.matched_markets], [])
        excluded = {item.market_id for item in resolution.excluded_markets}
        self.assertIn("historical-politics", excluded)
        self.assertNotIn("title-only", cycle.candidate_bound_scheduled)

    def test_resolver_failure_cannot_fall_back_to_legacy_candidate_authority(self) -> None:
        store = _ScopeStore(
            {"candidate": {"experiment_plan": {"market_scope": scope("EXACT_MARKETS", market_ids=("market",))}}},
            fail_saver=True,
        )
        provider = _RecordingProvider((market("market"),))
        cycle = self._collector(provider, store, ("candidate",)).collect_once(now=T0)

        self.assertEqual(list(cycle.candidate_bound_scheduled), [])
        self.assertEqual(list(cycle.discovery_scheduled), [])
        self.assertTrue(store.errors)
        self.assertNotIn(("candidate",), store.requirement_calls)

    def test_research_only_and_zero_matches_have_distinct_statuses(self) -> None:
        store = _ScopeStore(
            {
                "research": {"experiment_plan": {"market_scope": scope("RESEARCH_ONLY")}},
                "zero": {"experiment_plan": {"market_scope": scope("RULE_BASED_MARKETS", category="politics")}},
            }
        )
        provider = _RecordingProvider((market("economics", category="economics"),))
        self._collector(provider, store, ("research", "zero")).collect_once(now=T0)

        by_candidate = {item.candidate_id: item for item in store.resolutions}
        self.assertEqual(by_candidate["research"].status, RESEARCH_ONLY)
        self.assertEqual(by_candidate["research"].reason, RESEARCH_ONLY)
        self.assertEqual(by_candidate["zero"].status, ZERO_MATCHES)
        self.assertEqual(by_candidate["zero"].reason, ZERO_MATCHES)


if __name__ == "__main__":
    unittest.main()
