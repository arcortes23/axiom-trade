from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json
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



class _PagedProvider(_RecordingProvider):
    def __init__(
        self,
        markets: tuple[PredictionMarketSnapshot, ...],
        pages: tuple[dict[str, object], ...],
        *,
        tag_ids: dict[str, int] | None = None,
    ) -> None:
        super().__init__(markets)
        self.pages = list(pages)
        self.page_calls: list[dict[str, object]] = []
        self.tag_calls: list[str] = []
        self.book_calls: list[str] = []
        self._tag_ids = dict(tag_ids or {})

    def resolve_tag_slug(self, slug: str):
        self.tag_calls.append(slug)
        return self._tag_ids.get(slug)

    def market_page(self, **kwargs):
        self.page_calls.append(dict(kwargs))
        if not self.pages:
            raise AssertionError("unexpected metadata page")
        page = dict(self.pages.pop(0))
        page.setdefault("query", dict(kwargs))
        return page

    def order_books(self, market_id: str, depth: int = 20):
        self.book_calls.append(str(market_id))
        return super().order_books(market_id, depth=depth)


class _AdvisoryLookupFailureProvider(_PagedProvider):
    def __init__(
        self,
        markets: tuple[PredictionMarketSnapshot, ...],
        pages: tuple[dict[str, object], ...],
    ) -> None:
        super().__init__(markets, pages)
        self.transport_errors: list[Exception] = []
        self.validation_errors: list[Exception] = []
        self.drained_transport_errors = 0
        self.drained_validation_errors = 0

    def resolve_tag_slug(self, slug: str):
        self.tag_calls.append(slug)
        self.transport_errors.append(RuntimeError("advisory tag transport failure"))
        self.validation_errors.append(ValueError("advisory tag validation failure"))
        raise RuntimeError("tag lookup failed")

    def consume_transport_errors(self) -> tuple[Exception, ...]:
        errors = tuple(self.transport_errors)
        self.transport_errors.clear()
        self.drained_transport_errors += len(errors)
        return errors

    def consume_validation_errors(self) -> tuple[Exception, ...]:
        errors = tuple(self.validation_errors)
        self.validation_errors.clear()
        self.drained_validation_errors += len(errors)
        return errors

    def market_page(self, **kwargs):
        if self.transport_errors or self.validation_errors:
            raise AssertionError("advisory lookup errors were not drained")
        return super().market_page(**kwargs)


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

    def test_keyset_scope_continuation_is_opaque_and_budgeted(self) -> None:
        first = market("page-one", category="politics")
        second = market("page-two", category="politics")
        provider = _PagedProvider(
            (first, second),
            (
                {"snapshots": (first,), "next_cursor": "opaque-1", "raw_count": 1, "unique_count": 1},
                {"snapshots": (second,), "next_cursor": None, "raw_count": 1, "unique_count": 1},
            ),
            tag_ids={"politics": 11},
        )
        store = _ScopeStore(
            {"candidate": {"experiment_plan": {"market_scope": scope("RULE_BASED_MARKETS", category="politics")}}}
        )
        collector = self._collector(provider, store, ("candidate",), max_markets=1)

        collector.collect_once(now=T0)
        collector.collect_once(now=T0)

        self.assertEqual([call["after_cursor"] for call in provider.page_calls], [None, "opaque-1"])
        self.assertEqual(provider.page_calls[0]["closed"], False)
        continuation = store.states["polymarket"]["scope_inventory_continuation"]
        self.assertEqual(continuation["coverage_status"], "COMPLETE")
        self.assertEqual(continuation["cumulative"], {
            "raw_count": 2,
            "unique_count": 2,
            "duplicate_count": 0,
            "malformed_count": 0,
        })
        self.assertEqual(len(provider.book_calls), 2)

    def test_scope_pushdown_is_shared_and_never_uses_price_or_spread(self) -> None:
        rich = replace(
            market("liquid", category="politics"),
            liquidity=2_000.0,
            expiry=T0 + timedelta(hours=120),
        )
        policy = scope("RULE_BASED_MARKETS", category="politics")
        policy["filters"] = {
            "category": "politics",
            "min_liquidity": 1_000.0,
            "minimum_hours_to_resolution": 24.0,
            "maximum_hours_to_resolution": 168.0,
        }
        provider = _PagedProvider(
            (rich,),
            ({"snapshots": (rich,), "next_cursor": None},),
            tag_ids={"politics": 17},
        )
        store = _ScopeStore({"candidate": {"experiment_plan": {"market_scope": policy}}})
        collector = self._collector(provider, store, ("candidate",), max_markets=1)

        collector.collect_once(now=T0)

        request = provider.page_calls[0]
        self.assertEqual(request["tag_ids"], (17,))
        self.assertEqual(request["liquidity_num_min"], 1_000.0)
        self.assertEqual(request["end_date_min"], (T0 + timedelta(hours=24)).isoformat())
        self.assertEqual(request["end_date_max"], (T0 + timedelta(hours=168)).isoformat())
        self.assertNotIn("price", request)
        self.assertNotIn("spread", request)
        self.assertEqual(provider.tag_calls, ["politics"])
        self.assertEqual(len(provider.book_calls), 1)

    def test_scope_query_change_resets_cursor_and_duplicate_only_pages_advance(self) -> None:
        duplicate = market("duplicate", category="politics")
        replacement = market("replacement", category="economics")
        provider = _PagedProvider(
            (duplicate, replacement),
            (
                {
                    "snapshots": (duplicate, duplicate),
                    "next_cursor": "stale-cursor",
                    "raw_count": 2,
                    "unique_count": 1,
                    "duplicate_count": 1,
                },
                {
                    "snapshots": (),
                    "next_cursor": "unused-cursor",
                    "raw_count": 1,
                    "unique_count": 0,
                    "duplicate_count": 1,
                    "coverage_status": "PARTIAL",
                },
                {"snapshots": (replacement,), "next_cursor": None},
            ),
            tag_ids={"politics": 21, "economics": 22},
        )
        politics = scope("RULE_BASED_MARKETS", category="politics")
        store = _ScopeStore({"candidate": {"experiment_plan": {"market_scope": politics}}})
        collector = self._collector(provider, store, ("candidate",), max_markets=1)

        collector.collect_once(now=T0)
        state_after_first = store.states["polymarket"]["scope_inventory_continuation"]
        self.assertEqual(state_after_first["coverage_status"], "BUDGET_EXHAUSTED")
        self.assertEqual(state_after_first["cumulative_duplicate_count"], 1)

        economics = scope("RULE_BASED_MARKETS", category="economics")
        store.documents["candidate"]["payload"]["experiment_plan"]["market_scope"] = economics
        collector.collect_once(now=T0)

        self.assertEqual([call["after_cursor"] for call in provider.page_calls], [None, None])
        self.assertEqual(provider.page_calls[1]["tag_ids"], (22,))



    def test_failed_tag_lookup_drains_advisory_errors_before_broader_page(self) -> None:
        broader = market("broader-page", category="politics")
        provider = _AdvisoryLookupFailureProvider(
            (broader,),
            ({"snapshots": (broader,), "next_cursor": None},),
        )
        policy = scope("RULE_BASED_MARKETS", category="politics")
        store = _ScopeStore({"candidate": {"experiment_plan": {"market_scope": policy}}})
        collector = self._collector(provider, store, ("candidate",), max_markets=1)

        cycle = collector.collect_once(now=T0)

        self.assertEqual(list(cycle.candidate_bound_scheduled), ["broader-page"])
        self.assertEqual(provider.tag_calls, ["politics"])
        self.assertEqual(provider.page_calls[0]["tag_ids"], ())
        self.assertEqual(provider.drained_transport_errors, 1)
        self.assertEqual(provider.drained_validation_errors, 1)
        self.assertEqual(store.errors, [])
        self.assertEqual(
            [item.market_id for item in store.resolutions[0].matched_markets],
            ["broader-page"],
        )

    def test_repeated_cursor_error_persists_error_and_rebases_without_cursor(self) -> None:
        repeated = market("repeated-page", category="politics")
        provider = _PagedProvider(
            (repeated,),
            (
                {"snapshots": (repeated,), "next_cursor": "opaque-repeat"},
                {
                    "snapshots": (repeated,),
                    "next_cursor": "opaque-repeat",
                },
                {"snapshots": (repeated,), "next_cursor": None},
            ),
        )
        policy = scope("RULE_BASED_MARKETS", category="politics")
        store = _ScopeStore({"candidate": {"experiment_plan": {"market_scope": policy}}})
        collector = self._collector(provider, store, ("candidate",), max_markets=1)

        first_cycle = collector.collect_once(now=T0)
        second_cycle = collector.collect_once(now=T0)
        self.assertEqual(first_cycle.errors, 0)
        self.assertEqual(second_cycle.errors, 1)

        state_after_error = store.states["polymarket"]["scope_inventory_continuation"]
        self.assertEqual(state_after_error["coverage_status"], "ERROR")
        self.assertEqual(state_after_error["error_reason"], "REPEATED_CURSOR")
        self.assertIsNone(state_after_error["after_cursor"])
        self.assertIsNone(state_after_error["opaque_cursor"])
        self.assertIsNone(state_after_error["cursor"])

        collector.collect_once(now=T0)

        self.assertEqual(
            [call["after_cursor"] for call in provider.page_calls],
            [None, "opaque-repeat", None],
        )

    def test_cursor_cycle_detects_non_adjacent_repeat_and_discards_page(self) -> None:
        first = market("cycle-first", category="politics")
        second = market("cycle-second", category="politics")
        compromised = market("cycle-compromised", category="politics")
        rebased = market("cycle-rebased", category="politics")
        provider = _PagedProvider(
            (first, second, compromised, rebased),
            (
                {"snapshots": (first,), "next_cursor": "cursor-a"},
                {"snapshots": (second,), "next_cursor": "cursor-b"},
                {"snapshots": (compromised,), "next_cursor": "cursor-a"},
                {"snapshots": (rebased,), "next_cursor": None},
            ),
        )
        policy = scope("RULE_BASED_MARKETS", category="politics")
        store = _ScopeStore({"candidate": {"experiment_plan": {"market_scope": policy}}})
        collector = self._collector(provider, store, ("candidate",), max_markets=1)

        collector.collect_once(now=T0)
        self.assertEqual(
            store.states["polymarket"]["scope_inventory_continuation"]["seen_cursor_history"],
            ["cursor-a"],
        )
        collector.collect_once(now=T0)
        self.assertEqual(
            store.states["polymarket"]["scope_inventory_continuation"]["seen_cursor_history"],
            ["cursor-a", "cursor-b"],
        )
        collector.collect_once(now=T0)

        state_after_error = store.states["polymarket"]["scope_inventory_continuation"]
        self.assertEqual(
            [call["after_cursor"] for call in provider.page_calls],
            [None, "cursor-a", "cursor-b"],
        )
        self.assertEqual(state_after_error["coverage_status"], "ERROR")
        self.assertEqual(state_after_error["error_reason"], "REPEATED_CURSOR")
        self.assertEqual(state_after_error["seen_cursor_history"], [])
        self.assertEqual(
            [item.candidate_id for item in store.resolutions],
            ["candidate", "candidate"],
        )
        self.assertEqual(
            provider.book_calls,
            ["cycle-first", "cycle-second"],
        )

        collector.collect_once(now=T0)

        self.assertEqual(
            [call["after_cursor"] for call in provider.page_calls],
            [None, "cursor-a", "cursor-b", None],
        )
        self.assertEqual(provider.book_calls, ["cycle-first", "cycle-second", "cycle-rebased"])


    def test_complete_terminal_restart_rebases_seen_ids_and_counts(self) -> None:
        terminal = market("complete-terminal", category="politics")
        provider = _PagedProvider(
            (terminal,),
            (
                {"snapshots": (terminal,), "next_cursor": None},
                {"snapshots": (terminal,), "next_cursor": None},
            ),
        )
        policy = scope("RULE_BASED_MARKETS", category="politics")
        store = _ScopeStore({"candidate": {"experiment_plan": {"market_scope": policy}}})
        collector = self._collector(provider, store, ("candidate",), max_markets=1)

        collector.collect_once(now=T0)
        collector.collect_once(now=T0)

        continuation = store.states["polymarket"]["scope_inventory_continuation"]
        self.assertEqual([call["after_cursor"] for call in provider.page_calls], [None, None])
        self.assertEqual(
            continuation["cumulative"],
            {
                "raw_count": 1,
                "unique_count": 1,
                "duplicate_count": 0,
                "malformed_count": 0,
            },
        )
        self.assertEqual(continuation["seen_cursor_history"], [])
        self.assertEqual(continuation["seen_market_ids"], ["complete-terminal"])
        self.assertEqual(provider.book_calls, ["complete-terminal", "complete-terminal"])

    def test_malformed_terminal_without_cursor_restarts_from_page_one(self) -> None:
        fresh = market("after-malformed", category="politics")
        provider = _PagedProvider(
            (fresh,),
            (
                {"snapshots": ("malformed-row",), "next_cursor": None},
                {"snapshots": (fresh,), "next_cursor": None},
            ),
        )
        policy = scope("RULE_BASED_MARKETS", category="politics")
        store = _ScopeStore({"candidate": {"experiment_plan": {"market_scope": policy}}})
        collector = self._collector(provider, store, ("candidate",), max_markets=1)

        collector.collect_once(now=T0)
        collector.collect_once(now=T0)

        continuation = store.states["polymarket"]["scope_inventory_continuation"]
        self.assertEqual([call["after_cursor"] for call in provider.page_calls], [None, None])
        self.assertEqual(continuation["coverage_status"], "COMPLETE")
        self.assertEqual(
            continuation["cumulative"],
            {
                "raw_count": 1,
                "unique_count": 1,
                "duplicate_count": 0,
                "malformed_count": 0,
            },
        )
        self.assertEqual(provider.book_calls, ["after-malformed"])

    def test_fingerprint_mismatch_discards_page_and_rebases_next_cycle(self) -> None:
        first = market("fingerprint-first", category="politics")
        mismatched = market("fingerprint-mismatched", category="politics")
        rebased = market("fingerprint-rebased", category="politics")
        provider = _PagedProvider(
            (first, mismatched, rebased),
            (
                {
                    "snapshots": (first,),
                    "next_cursor": "opaque-1",
                    "query_fingerprint": "provider-generation",
                },
                {
                    "snapshots": (mismatched,),
                    "next_cursor": None,
                    "query_fingerprint": "wrong-generation",
                },
                {"snapshots": (rebased,), "next_cursor": None},
            ),
        )
        policy = scope("RULE_BASED_MARKETS", category="politics")
        store = _ScopeStore({"candidate": {"experiment_plan": {"market_scope": policy}}})
        collector = self._collector(provider, store, ("candidate",), max_markets=1)

        first_cycle = collector.collect_once(now=T0)
        second_cycle = collector.collect_once(now=T0)
        self.assertEqual(first_cycle.errors, 0)
        self.assertEqual(second_cycle.errors, 1)

        reset = store.states["polymarket"]["scope_inventory_continuation"]
        self.assertEqual([call["after_cursor"] for call in provider.page_calls], [None, "opaque-1"])
        self.assertEqual(reset["coverage_status"], "ERROR")
        self.assertEqual(reset["error_reason"], "QUERY_RESET")
        self.assertEqual(reset["query_reset_reason"], "QUERY_FINGERPRINT_MISMATCH")
        self.assertTrue(reset["query_reset"])
        self.assertTrue(reset["rebase_required"])
        self.assertIsNone(reset["after_cursor"])
        self.assertEqual(reset["cumulative_unique_count"], 0)
        self.assertEqual(reset["seen_market_ids"], [])
        self.assertEqual(len(store.resolutions), 1)
        self.assertEqual(provider.book_calls, ["fingerprint-first"])

        collector.collect_once(now=T0)

        continuation = store.states["polymarket"]["scope_inventory_continuation"]
        self.assertEqual(
            [call["after_cursor"] for call in provider.page_calls],
            [None, "opaque-1", None],
        )
        self.assertEqual(continuation["cumulative_unique_count"], 1)
        self.assertEqual(continuation["seen_market_ids"], ["fingerprint-rebased"])
        self.assertEqual(len(store.resolutions), 2)
        self.assertEqual(provider.book_calls, ["fingerprint-first", "fingerprint-rebased"])
    def test_non_string_next_cursor_is_integrity_error_and_rebases(self) -> None:
        rejected = market("invalid-cursor", category="politics")
        accepted = market("after-invalid-cursor", category="politics")
        provider = _PagedProvider(
            (rejected, accepted),
            (
                {"snapshots": (rejected,), "next_cursor": 17},
                {"snapshots": (accepted,), "next_cursor": None},
            ),
        )
        policy = scope("RULE_BASED_MARKETS", category="politics")
        store = _ScopeStore({"candidate": {"experiment_plan": {"market_scope": policy}}})
        collector = self._collector(provider, store, ("candidate",), max_markets=1)

        rejected_cycle = collector.collect_once(now=T0)
        continuation = store.states["polymarket"]["scope_inventory_continuation"]
        self.assertEqual(rejected_cycle.errors, 1)
        self.assertEqual(continuation["coverage_status"], "ERROR")
        self.assertEqual(continuation["error_reason"], "INVALID_NEXT_CURSOR")
        self.assertEqual(continuation["seen_market_ids"], [])
        self.assertIsNone(continuation["after_cursor"])
        self.assertEqual(provider.book_calls, [])

        accepted_cycle = collector.collect_once(now=T0)
        self.assertEqual(accepted_cycle.errors, 0)
        self.assertEqual([call["after_cursor"] for call in provider.page_calls], [None, None])
        self.assertEqual(provider.book_calls, ["after-invalid-cursor"])

    def test_explicit_error_page_discards_ids_and_counts_cycle_error(self) -> None:
        poisoned = market("error-page-id", category="politics")
        accepted = market("after-error-page", category="politics")
        provider = _PagedProvider(
            (poisoned, accepted),
            (
                {
                    "snapshots": (poisoned,),
                    "next_cursor": "should-not-continue",
                    "coverage_status": "ERROR",
                    "error_reason": "provider_payload_error",
                },
                {"snapshots": (accepted,), "next_cursor": None},
            ),
        )
        policy = scope("RULE_BASED_MARKETS", category="politics")
        store = _ScopeStore({"candidate": {"experiment_plan": {"market_scope": policy}}})
        collector = self._collector(provider, store, ("candidate",), max_markets=1)

        error_cycle = collector.collect_once(now=T0)
        continuation = store.states["polymarket"]["scope_inventory_continuation"]
        self.assertEqual(error_cycle.errors, 1)
        self.assertEqual(continuation["error_reason"], "PROVIDER_PAYLOAD_ERROR")
        self.assertEqual(continuation["seen_market_ids"], [])
        self.assertIsNone(continuation["after_cursor"])
        self.assertEqual(provider.book_calls, [])

        collector.collect_once(now=T0)
        self.assertEqual([call["after_cursor"] for call in provider.page_calls], [None, None])
        self.assertEqual(provider.book_calls, ["after-error-page"])

    def test_first_provider_fingerprint_is_adopted_without_request_hash_comparison(self) -> None:
        first = market("optional-fingerprint-first", category="politics")
        second = market("optional-fingerprint-second", category="politics")
        provider = _PagedProvider(
            (first, second),
            (
                {
                    "snapshots": (first,),
                    "next_cursor": "cursor-after-start",
                    "query_fingerprint": "provider-generation-a",
                },
                {
                    "snapshots": (second,),
                    "next_cursor": None,
                    "query_fingerprint": "provider-generation-a",
                },
            ),
        )
        policy = scope("RULE_BASED_MARKETS", category="politics")
        store = _ScopeStore({"candidate": {"experiment_plan": {"market_scope": policy}}})
        store.states["polymarket"] = {
            "scope_inventory_continuation": {
                "after_cursor": "cursor-start",
                "coverage_status": "PARTIAL",
                "seen_cursor_history": ["cursor-start"],
                "seen_market_ids": [],
            }
        }
        collector = self._collector(provider, store, ("candidate",), max_markets=1)

        first_cycle = collector.collect_once(now=T0)
        second_cycle = collector.collect_once(now=T0)
        continuation = store.states["polymarket"]["scope_inventory_continuation"]
        self.assertEqual(first_cycle.errors, 0)
        self.assertEqual(second_cycle.errors, 0)
        self.assertEqual(
            [call["after_cursor"] for call in provider.page_calls],
            ["cursor-start", "cursor-after-start"],
        )
        self.assertEqual(continuation["provider_query_fingerprint"], "provider-generation-a")
        self.assertEqual(provider.book_calls, ["optional-fingerprint-first", "optional-fingerprint-second"])

    def test_provider_query_metadata_is_bounded_json_without_losing_cursor(self) -> None:
        first = market("metadata-first", category="politics")
        second = market("metadata-second", category="politics")

        class UnsupportedQueryValue:
            pass

        provider = _PagedProvider(
            (first, second),
            (
                {
                    "snapshots": (first,),
                    "next_cursor": "metadata-cursor",
                    "request_path": object(),
                    "query": {"unsafe": UnsupportedQueryValue()},
                },
                {"snapshots": (second,), "next_cursor": None},
            ),
        )
        policy = scope("RULE_BASED_MARKETS", category="politics")
        store = _ScopeStore({"candidate": {"experiment_plan": {"market_scope": policy}}})
        collector = self._collector(provider, store, ("candidate",), max_markets=1)

        first_cycle = collector.collect_once(now=T0)
        continuation = store.states["polymarket"]["scope_inventory_continuation"]
        self.assertEqual(first_cycle.errors, 0)
        self.assertEqual(continuation["request_path"], "/markets/keyset")
        self.assertIsInstance(continuation["query"], dict)
        self.assertNotIn("unsafe", continuation["query"])
        json.dumps(continuation, allow_nan=False)
        self.assertEqual(continuation["after_cursor"], "metadata-cursor")

        collector.collect_once(now=T0)
        self.assertEqual(
            [call["after_cursor"] for call in provider.page_calls],
            [None, "metadata-cursor"],
        )
        self.assertEqual(provider.book_calls, ["metadata-first", "metadata-second"])


if __name__ == "__main__":

    unittest.main()
