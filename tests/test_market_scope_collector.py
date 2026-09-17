from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
import threading
import time
import sqlite3

import unittest

from axiom.collector import (
    CollectorConfig,
    PolymarketCollector,
    _MAX_SCOPE_INVENTORY,
    _MAX_SCOPE_SUITABILITY_CACHE,
    _stable_payload,
)
from axiom.data import InMemoryPredictionProvider
from axiom.storage import AxiomStore, _COLLECTOR_STATE_MAX_BYTES
from axiom.polymarket_rules import assess_selected_token_depth, parse_polymarket_rules
from axiom.domain import (
    InstrumentMetadata,
    MarketType,
    OrderBookLevel,
    OrderBookSnapshot,
    PredictionMarketSnapshot,
    SettlementState,
    Side,
    TradePrint,
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
        min_order_size=0.01,
        tick_size=0.01,
        neg_risk=False,
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


class _RepeatingTradeProvider(_RecordingProvider):
    def __init__(self, markets: tuple[PredictionMarketSnapshot, ...]) -> None:
        super().__init__(markets)
        self.trade_calls = 0
        self.last_trades_complete = True
        self.last_trade_cursor: str | None = None

    def trades(self, market_id: str, start=None, end=None, **kwargs):
        del start, end, kwargs
        self.trade_calls += 1
        return (
            TradePrint(
                T0,
                0.60,
                2.0,
                side=Side.BUY,
                trade_id="stable-public-fill",
                market_id=market_id,
                token_id=f"yes-{market_id}",
            ),
        )

    def trade_provenance(self, trade: TradePrint):
        return {
            "source_type": "FORWARD_COLLECTED",
            "provider": self.provider_name,
            "endpoint": "/trades",
            "condition_id": f"condition-{trade.market_id}",
            "response_timestamp": trade.timestamp.isoformat(),
            "query": {"poll": self.trade_calls},
        }


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


class _HangingScopeProvider(_PagedProvider):
    def __init__(
        self,
        markets: tuple[PredictionMarketSnapshot, ...],
        pages: tuple[dict[str, object], ...],
    ) -> None:
        super().__init__(markets, pages)
        self.entered = threading.Event()
        self.release = threading.Event()
        self.keyset_calls = 0

    def market_page(self, **kwargs):
        self.keyset_calls += 1
        self.entered.set()
        if self.keyset_calls == 1:
            self.release.wait(timeout=2.0)
        return super().market_page(**kwargs)


class _HangingScopeBookProvider(_PagedProvider):
    def __init__(
        self,
        markets: tuple[PredictionMarketSnapshot, ...],
        pages: tuple[dict[str, object], ...],
    ) -> None:
        super().__init__(markets, pages)
        self.scope_entered = threading.Event()
        self.scope_calls = 0
        self._hang = threading.Event()

    def order_book(self, market_id: str, depth: int = 20):
        if "scope-hang" in str(market_id):
            self.scope_calls += 1
            self.scope_entered.set()
            self._hang.wait()
        return super().order_book(market_id, depth=depth)
    def order_books(self, market_id: str, depth: int = 20):
        if "scope-hang" in str(market_id):
            self.scope_calls += 1
            self.scope_entered.set()
            self._hang.wait()
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
        self.forward_tests: dict[str, dict[str, object]] = {}

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
    def load_forward_tests(self, *, limit: int = 1000):
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 0:
            raise ValueError("limit must be a non-negative integer")
        return list(self.forward_tests.values())[:limit]

    def load_forward_test(self, experiment_id: str):
        if not str(experiment_id).strip():
            raise ValueError("experiment_id is required")
        record = self.forward_tests.get(str(experiment_id).strip())
        return dict(record) if record is not None else None

    def save_forward_test(self, experiment_id: str, spec):
        self.forward_tests[str(experiment_id).strip()] = dict(spec)
        return True


    def save_collection_cycle(self, *args, **kwargs):
        return None

    def save_dataset_catalog(self, *args, **kwargs):
        return None


class _SelectedRollingScopeStore(_ScopeStore):
    def __init__(
        self,
        candidate_id: str,
        strategy_version_id: str,
        payload: dict[str, object],
        *,
        status: str = "OBSERVE",
    ) -> None:
        super().__init__({candidate_id: payload})
        self._selection = {
            "portfolio_selection_id": "selection-current",
            "members": [
                {
                    "strategy_version_id": strategy_version_id,
                    "candidate_id": candidate_id,
                    "status": status,
                }
            ],
        }
        self.connection = sqlite3.connect(":memory:")
        self.connection.execute(
            "CREATE TABLE strategy_versions("
            "strategy_version_id TEXT PRIMARY KEY,payload_json TEXT NOT NULL)"
        )
        self.connection.execute(
            "INSERT INTO strategy_versions(strategy_version_id,payload_json) VALUES (?,?)",
            (strategy_version_id, json.dumps(payload, sort_keys=True)),
        )
        self.connection.commit()

    def load_current_portfolio_selection(self):
        return dict(self._selection)

class _ImmutableTradeStore(_ScopeStore):
    def __init__(self, documents: dict[str, dict[str, object]]) -> None:
        super().__init__(documents)
        self.trade_payloads: dict[tuple[str, str], str] = {}

    def save_polymarket_trade(self, market_id, trade):
        key = (str(market_id), str(trade["trade_id"]))
        payload = json.dumps(dict(trade), sort_keys=True)
        previous = self.trade_payloads.get(key)
        if previous is not None and previous != payload:
            raise AssertionError("immutable trade payload changed across collection cycles")
        self.trade_payloads[key] = payload
        return previous is None


class _ScopeCollector(PolymarketCollector):
    def __init__(self, *args, candidate_ids: tuple[str, ...], **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._candidate_ids = candidate_ids

    def _active_primary_candidate_ids(self) -> list[str]:
        return list(self._candidate_ids)

class _BudgetedScopeCollector(_ScopeCollector):
    """Expose a deterministic monotonic budget for queue-order regression."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._synthetic_budget = 2.0

    def _cycle_remaining_seconds(self):
        provider = self.provider
        return self._synthetic_budget - (
            0.10 * len(getattr(provider, "page_calls", ()))
            + 0.20 * len(getattr(provider, "market_calls", ()))
            + 0.20 * len(getattr(provider, "book_calls", ()))
        )

class _TwoWindowScopeCollector(_ScopeCollector):
    """Exercise a custom cycle budget of two provider windows."""

    def collect_once(self, *args, **kwargs):
        self._custom_cycle_deadline = time.monotonic() + (
            2.0 * float(self.config.provider_timeout_seconds)
        )
        return super().collect_once(*args, **kwargs)

    def _cycle_remaining_seconds(self):
        return self._custom_cycle_deadline - time.monotonic()


class MarketScopeCollectorTests(unittest.TestCase):

    def _collector(
        self,
        provider: _RecordingProvider,
        store: _ScopeStore,
        candidate_ids: tuple[str, ...],
        *,
        max_markets: int = 10,
        market_ids: tuple[str, ...] = (),
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
                market_ids=market_ids,
            ),
            candidate_ids=candidate_ids,
            clock=lambda: T0,
            sleep=lambda _seconds: None,
        )

    def test_selected_rolling_exact_scope_direct_lookup_bypasses_empty_legacy_authority(self) -> None:
        selected = market("rolling-direct-open")
        store = _SelectedRollingScopeStore(
            "rolling-candidate",
            "rolling-strategy",
            {
                "candidate_id": "rolling-candidate",
                "experiment_plan": {
                    "market_scope": scope(
                        "EXACT_MARKETS",
                        market_ids=("rolling-direct-open",),
                    )
                },
            },
        )
        provider = _PagedProvider(
            (selected,),
            ({"markets": (), "next_cursor": None},),
        )

        cycle = self._collector(provider, store, ()).collect_once(now=T0)

        self.assertIn("rolling-direct-open", provider.market_calls)
        self.assertEqual(list(cycle.candidate_bound_scheduled), ["rolling-direct-open"])
        self.assertEqual(list(cycle.discovery_scheduled), [])
        self.assertTrue(store.requirement_calls)
        self.assertTrue(all(not call for call in store.requirement_calls))
        self.assertEqual(len(store.resolutions), 1)
        self.assertEqual(store.resolutions[0].status, MATCHED)

    def test_selected_rolling_exact_scope_direct_lookup_persists_closed_and_no_orders_exclusions(self) -> None:
        closed = replace(
            market(
                "rolling-direct-closed",
                settlement=SettlementState.RESOLVED_YES,
                closed=True,
            ),
            active=True,
        )
        no_orders = replace(
            market("rolling-direct-no-orders"),
            accepting_orders=False,
        )
        payload = {
            "candidate_id": "rolling-candidate",
            "experiment_plan": {
                "market_scope": scope(
                    "EXACT_MARKETS",
                    market_ids=(
                        "rolling-direct-closed",
                        "rolling-direct-no-orders",
                        "rolling-direct-missing",
                    ),
                )
            },

        }
        store = _SelectedRollingScopeStore(
            "rolling-candidate",
            "rolling-strategy",
            payload,
            status="FROZEN",
        )
        provider = _PagedProvider(
            (closed, no_orders),
            ({"markets": (), "next_cursor": None},),
        )

        cycle = self._collector(provider, store, ()).collect_once(now=T0)

        self.assertEqual(list(cycle.candidate_bound_scheduled), [])
        self.assertEqual(len(store.resolutions), 1)
        resolution = store.resolutions[0]
        self.assertEqual(resolution.status, "DEFERRED")

        self.assertEqual(
            {item.reason for item in resolution.excluded_markets},
            {"MARKET_CLOSED", "ACCEPTING_ORDERS_FALSE"},
        )
        self.assertEqual(
            {item.reason for item in resolution.deferred_markets},
            {"NOT_OBSERVED"},
        )


    def test_direct_exact_snapshot_overrides_stale_carried_inventory_before_resolver_cap(self) -> None:
        fresh = market("carried-duplicate")
        stale = replace(
            fresh,
            active=True,
            closed=True,
            settlement=SettlementState.RESOLVED_YES,
        )
        store = _ScopeStore(
            {
                "candidate": {
                    "experiment_plan": {
                        "market_scope": scope(
                            "EXACT_MARKETS",
                            market_ids=("carried-duplicate",),
                        )
                    }
                }
            }
        )
        provider = _PagedProvider(
            (fresh,),
            ({"markets": (), "next_cursor": None},),
        )
        carried = [
            dict(PolymarketCollector._scope_market_record(stale, T0, provider)),
            *({"market_id": f"carried-{index:04d}"} for index in range(999)),
        ]
        store.states["polymarket"] = {
            "scope_inventory_continuation": {
                "coverage_status": "PARTIAL",
                "after_cursor": "opaque-carried",
                "inventory_records": carried,
            }
        }

        cycle = self._collector(provider, store, ("candidate",)).collect_once(now=T0)

        self.assertEqual(list(cycle.candidate_bound_scheduled), ["carried-duplicate"])
        self.assertEqual(len(store.resolutions), 1)
        self.assertEqual(
            [item.market_id for item in store.resolutions[0].matched_markets],
            ["carried-duplicate"],
        )

    def test_failed_direct_exact_lookup_does_not_fall_back_to_stale_carried_record(self) -> None:
        class FailingDirectProvider(_PagedProvider):
            def market(self, market_id: str):
                self.market_calls.append(str(market_id))
                raise RuntimeError("direct lookup unavailable")

        stale = market("failed-carried")
        store = _ScopeStore(
            {
                "candidate": {
                    "experiment_plan": {
                        "market_scope": scope(
                            "EXACT_MARKETS",
                            market_ids=("failed-carried",),
                        )
                    }
                }
            }
        )
        provider = FailingDirectProvider(
            (stale,),
            ({"markets": (), "next_cursor": None},),
        )
        store.states["polymarket"] = {
            "scope_inventory_continuation": {
                "coverage_status": "PARTIAL",
                "after_cursor": "opaque-failed",
                "inventory_records": [
                    dict(PolymarketCollector._scope_market_record(stale, T0, provider))
                ],
            }
        }

        cycle = self._collector(provider, store, ("candidate",)).collect_once(now=T0)

        self.assertEqual(list(cycle.candidate_bound_scheduled), [])
        self.assertEqual(len(store.resolutions), 1)
        resolution = store.resolutions[0]
        self.assertEqual(resolution.status, "DEFERRED")
        self.assertEqual(
            [(item.market_id, item.reason) for item in resolution.deferred_markets],
            [("failed-carried", "NOT_OBSERVED")],
        )

    def test_scope_persistence_unavailable_fails_closed_without_legacy_authority(self) -> None:
        provider = _RecordingProvider((market("scope-store-unavailable"),))
        store = _ScopeStore(
            {
                "candidate": {
                    "experiment_plan": {
                        "market_scope": scope(
                            "EXACT_MARKETS",
                            market_ids=("scope-store-unavailable",),
                        )
                    }
                }
            }
        )
        store.save_market_scope_resolution = None  # type: ignore[method-assign]

        cycle = self._collector(provider, store, ("candidate",)).collect_once(now=T0)

        self.assertEqual(list(cycle.candidate_bound_markets), [])
        self.assertEqual(list(cycle.candidate_bound_scheduled), [])
        self.assertEqual(store.resolutions, [])
        self.assertNotIn("scope-store-unavailable", provider.market_calls)
    def test_closed_current_scope_drops_deadline_resume_id(self) -> None:
        open_market = market("resume-exact")
        store = _ScopeStore(
            {
                "candidate": {
                    "experiment_plan": {
                        "market_scope": scope(
                            "EXACT_MARKETS",
                            market_ids=("resume-exact",),
                        )
                    }
                }
            }
        )
        provider = _RecordingProvider((open_market,))

        collector = self._collector(provider, store, ("candidate",))
        collector.collect_once(now=T0)
        first_market_call_count = provider.market_calls.count("resume-exact")
        provider._markets["resume-exact"] = replace(
            open_market,
            active=False,
            closed=True,
            settlement=SettlementState.RESOLVED_YES,
        )
        store.states["polymarket"]["cycle_continuation"] = {
            "remaining_market_ids": ["resume-exact"],
        }

        second = collector.collect_once(now=T0 + timedelta(seconds=61))

        self.assertEqual(list(second.candidate_bound_scheduled), [])
        self.assertEqual(
            provider.market_calls.count("resume-exact") - first_market_call_count,
            1,
        )
        self.assertEqual(
            [item.reason for item in store.resolutions[-1].excluded_markets],
            ["INACTIVE_MARKET"],
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

    def test_scoped_configured_market_ids_exclude_unauthorized_values_everywhere(self) -> None:
        authorized = market("authorized")
        unauthorized = market("unauthorized")
        store = _ScopeStore(
            {
                "candidate": {
                    "experiment_plan": {
                        "market_scope": scope(
                            "EXACT_MARKETS",
                            market_ids=("authorized",),
                        )
                    }
                }
            }
        )
        provider = _RecordingProvider((authorized, unauthorized))
        cycle = self._collector(
            provider,
            store,
            ("candidate",),
            market_ids=("authorized", "unauthorized"),
        ).collect_once(now=T0)

        self.assertEqual(list(cycle.candidate_bound_markets), ["authorized"])
        self.assertEqual(list(cycle.candidate_bound_scheduled), ["authorized"])
        self.assertEqual(list(cycle.paper_forward_markets), [])
        self.assertEqual(list(cycle.discovery_scheduled), [])
        self.assertEqual(cycle.candidate_references, {"authorized": ["candidate"]})
        self.assertNotIn("unauthorized", cycle.candidate_bound_markets)
        self.assertNotIn("unauthorized", cycle.candidate_bound_missing)
        self.assertNotIn("unauthorized", provider.market_calls)
        self.assertTrue(provider.market_calls)
        self.assertEqual(set(provider.market_calls), {"authorized"})

    def test_scoped_paper_forward_candidate_stays_in_paper_tier(self) -> None:
        paper_market = market("paper-market")
        store = _ScopeStore(
            {
                "paper-candidate": {
                    "experiment_plan": {
                        "market_scope": scope(
                            "EXACT_MARKETS",
                            market_ids=("paper-market",),
                        )
                    }
                }
            }
        )
        store.documents["paper-candidate"]["stage"] = "PAPER_FORWARD"
        provider = _RecordingProvider((paper_market,))
        cycle = self._collector(
            provider,
            store,
            (),
            market_ids=("paper-market",),
        ).collect_once(now=T0)

        self.assertEqual(list(cycle.candidate_bound_markets), [])
        self.assertEqual(list(cycle.candidate_references or {}), [])
        self.assertEqual(list(cycle.paper_forward_markets), ["paper-market"])
        self.assertEqual(list(cycle.paper_forward_scheduled), ["paper-market"])
        self.assertEqual(cycle.tier_attempts["candidate"], 0)  # type: ignore[index]
        self.assertEqual(cycle.tier_attempts["paper_forward"], 1)  # type: ignore[index]
        self.assertTrue(provider.market_calls)
        self.assertEqual(set(provider.market_calls), {"paper-market"})

    def test_observation_intent_scope_never_enters_candidate_authority(self) -> None:
        observation_market = market("observation-market")
        store = _ScopeStore(
            {
                "observation-candidate": {
                    "paper_observation_intent_id": "observation-intent-observation-candidate",
                    "experiment_plan": {
                        "market_scope": scope(
                            "EXACT_MARKETS",
                            market_ids=("observation-market",),
                        )
                    },
                }
            }
        )
        store.documents["observation-candidate"]["stage"] = "SCHEMA_VALIDATED"
        store.forward_tests["observation-intent-observation-candidate"] = {
            "experiment_id": "observation-intent-observation-candidate",
            "strategy_hash": "sha256:observation-strategy",
            "model_hash": "sha256:observation-model",
            "config": {
                "candidate_id": "observation-candidate",
                "observation_intent": True,
                "market_authority_required": False,
                "strategy_document": {"type": "observation"},
                "model_document": {"type": "observation"},
            },
            "start_timestamp": T0.isoformat(),
            "registration_timestamp": T0.isoformat(),
            "bankroll": 10_000.0,
            "allowed_markets": [],
            "risk_limits": {},
            "quality": "PAPER_FORWARD",
        }
        provider = _RecordingProvider((observation_market,))
        cycle = self._collector(provider, store, ()).collect_once(now=T0)

        self.assertEqual(list(cycle.candidate_bound_markets), [])
        self.assertEqual(cycle.candidate_references, {})
        self.assertEqual(list(cycle.paper_forward_markets), ["observation-market"])
        self.assertEqual(list(cycle.paper_forward_scheduled), ["observation-market"])
        self.assertNotIn("observation-candidate", cycle.candidate_references or {})
        self.assertTrue(provider.market_calls)
        self.assertEqual(set(provider.market_calls), {"observation-market"})

    def test_repeated_public_trade_poll_keeps_immutable_evidence_stable(self) -> None:
        provider = _RepeatingTradeProvider((market("repeat-trade"),))
        store = _ImmutableTradeStore(
            {
                "candidate": {
                    "experiment_plan": {
                        "market_scope": scope("EXACT_MARKETS", market_ids=("repeat-trade",))
                    }
                }
            }
        )
        collector = self._collector(provider, store, ("candidate",))

        collector.collect_once(now=T0)
        second = collector.collect_once(now=T0 + timedelta(seconds=61))

        self.assertEqual(provider.trade_calls, 2)
        self.assertEqual(len(store.trade_payloads), 1)
        self.assertEqual(second.trade_duplicates, 1)
        self.assertFalse(any(error[2] == "trades" for error in store.errors))
        payload = json.loads(next(iter(store.trade_payloads.values())))
        self.assertEqual(payload["source_type"], "FORWARD_COLLECTED")
        self.assertEqual(payload["source_timestamp"], T0.isoformat())


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

    def test_broad_discovery_malformed_terminal_page_retries_without_completion(self) -> None:
        valid = market("broad-valid")
        provider = _PagedProvider(
            (valid,),
            (
                {"snapshots": (valid, None), "next_cursor": None},
                {"snapshots": (valid, None), "next_cursor": None},
            ),
        )
        store = _ScopeStore({})
        collector = self._collector(provider, store, (), max_markets=1)

        first = collector.collect_once(now=T0)
        second = collector.collect_once(now=T0)

        self.assertEqual(list(first.discovery_scheduled), ["broad-valid"])
        self.assertEqual(list(second.discovery_scheduled), ["broad-valid"])
        self.assertEqual(first.discovery_coverage_status, "PARTIAL")
        self.assertFalse(first.discovery_complete)
        self.assertFalse(second.discovery_complete)
        self.assertEqual([call["after_cursor"] for call in provider.page_calls], [None, None])
        state = store.states["polymarket"]
        self.assertFalse(state["discovery_complete"])
        continuation = state["discovery_continuation"]
        self.assertEqual(continuation["coverage_status"], "PARTIAL")
        self.assertEqual(continuation["malformed_count"], 1)
        self.assertIsNone(continuation["after_cursor"])
        self.assertEqual(
            [
                (error[2], error[3])
                for error in store.errors
                if error[2] == "discovery_malformed_rows"
            ],
            [
                ("discovery_malformed_rows", "1 malformed public market rows"),
                ("discovery_malformed_rows", "1 malformed public market rows"),
            ],
        )

    def test_keyset_scope_match_keeps_required_market_from_earlier_page(self) -> None:
        required = market("required-earlier", category="politics")
        unrelated = market("unrelated-later", category="economics")
        provider = _PagedProvider(
            (required, unrelated),
            (
                {"snapshots": (required,), "next_cursor": "opaque-1"},
                {"snapshots": (unrelated,), "next_cursor": None},
            ),
        )
        store = _ScopeStore(
            {
                "candidate": {
                    "experiment_plan": {
                        "market_scope": scope(
                            "EXACT_MARKETS",
                            market_ids=("required-earlier",),
                        )
                    }
                }
            }
        )
        collector = self._collector(provider, store, ("candidate",), max_markets=1)

        first = collector.collect_once(now=T0)
        self.assertEqual(first.candidate_bound_scheduled, ())
        self.assertEqual(store.resolutions, [])
        self.assertEqual(provider.book_calls, [])

        second = collector.collect_once(now=T0)
        self.assertEqual(
            [call["after_cursor"] for call in provider.page_calls],
            [None, "opaque-1"],
        )
        self.assertEqual(len(store.resolutions), 1)
        self.assertEqual(
            [item.market_id for item in store.resolutions[0].matched_markets],
            ["required-earlier"],
        )
        self.assertEqual(list(second.candidate_bound_scheduled), ["required-earlier"])
        self.assertEqual(provider.book_calls, ["required-earlier"])

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

        first_cycle = collector.collect_once(now=T0)
        self.assertEqual(first_cycle.candidate_bound_scheduled, ())
        self.assertEqual(store.resolutions, [])
        self.assertEqual(provider.book_calls, [])

        second_cycle = collector.collect_once(now=T0)
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
        # The first page was retained only as continuation; collection starts
        # after the complete inventory proves scope authority.  The scheduler
        # still starts with the earlier-page market, not just the terminal page.
        self.assertEqual(list(second_cycle.candidate_bound_scheduled), ["page-one"])
        self.assertEqual(provider.book_calls, ["page-one"])

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
        # No incomplete page can authorize a scope.  The compromised page is
        # discarded and the next rebased page starts a new bounded inventory.
        self.assertEqual([item.candidate_id for item in store.resolutions], [])
        self.assertEqual(provider.book_calls, [])

        collector.collect_once(now=T0)

        self.assertEqual(
            [call["after_cursor"] for call in provider.page_calls],
            [None, "cursor-a", "cursor-b", None],
        )
        self.assertEqual(provider.book_calls, ["cycle-rebased"])


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

        self.assertEqual(first_cycle.candidate_bound_scheduled, ())
        self.assertEqual(store.resolutions, [])
        self.assertEqual(provider.book_calls, [])

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
        self.assertEqual(len(store.resolutions), 0)
        self.assertEqual(provider.book_calls, [])

        rebased_cycle = collector.collect_once(now=T0)

        continuation = store.states["polymarket"]["scope_inventory_continuation"]
        self.assertEqual(
            [call["after_cursor"] for call in provider.page_calls],
            [None, "opaque-1", None],
        )
        self.assertEqual(continuation["cumulative_unique_count"], 1)
        self.assertEqual(continuation["seen_market_ids"], ["fingerprint-rebased"])
        self.assertEqual(rebased_cycle.candidate_bound_scheduled, ("fingerprint-rebased",))
        self.assertEqual(len(store.resolutions), 1)
        self.assertEqual(provider.book_calls, ["fingerprint-rebased"])
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
        self.assertEqual(first_cycle.candidate_bound_scheduled, ())
        self.assertEqual(
            [call["after_cursor"] for call in provider.page_calls],
            ["cursor-start", "cursor-after-start"],
        )
        self.assertEqual(continuation["provider_query_fingerprint"], "provider-generation-a")
        self.assertEqual(second_cycle.candidate_bound_scheduled, ("optional-fingerprint-first",))
        self.assertEqual(len(store.resolutions), 1)
        self.assertEqual(provider.book_calls, ["optional-fingerprint-first"])

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
        self.assertEqual(provider.book_calls, ["metadata-first"])


    def test_missing_official_rules_and_freshness_never_authorize_suitability(self) -> None:
        base = market("unknown-rules")
        base_book = base.order_book
        self.assertIsNotNone(base_book)
        assert base_book is not None
        provider = _RecordingProvider((base,))
        collector = self._collector(provider, _ScopeStore({}), ())

        unknown_rules = (
            ("min_order_size", replace(base_book, min_order_size=None), "MIN_ORDER_SIZE_MISSING"),
            ("tick_size", replace(base_book, tick_size=None), "TICK_SIZE_MISSING"),
            ("neg_risk", replace(base_book, neg_risk=None), "NEG_RISK_MISSING"),
        )
        for field, book, reason in unknown_rules:
            with self.subTest(field=field):
                snapshot = replace(base, order_book=book)
                assessment = collector._suitable_market_assessment(snapshot, T0, provider)
                self.assertEqual(assessment["action"], "UNSUITABLE")
                self.assertEqual(assessment["category"], "RULES_UNKNOWN")
                self.assertEqual(assessment["reason"], reason)
                self.assertEqual(assessment["next_action"], "recheck_next_discovery_tick")

        accepting_unknown = replace(base, accepting_orders=None)
        accepting_assessment = collector._suitable_market_assessment(accepting_unknown, T0, provider)
        self.assertEqual(accepting_assessment["category"], "MARKET_LIFECYCLE")
        self.assertEqual(accepting_assessment["reason"], "ACCEPTING_ORDERS_UNKNOWN")
        self.assertEqual(accepting_assessment["next_action"], "recheck_next_discovery_tick")

        depth_unknown = replace(base, order_book=replace(base_book, bids=(), asks=()))
        depth_assessment = collector._suitable_market_assessment(depth_unknown, T0, provider)
        self.assertEqual(depth_assessment["category"], "CAPITAL_OR_MARKET_CONSTRAINT")
        self.assertEqual(depth_assessment["reason"], "NO_DEPTH")
        self.assertEqual(depth_assessment["next_action"], "recheck_next_discovery_tick")

        freshness_unknown = replace(base)
        object.__setattr__(freshness_unknown, "timestamp", None)
        freshness_assessment = collector._suitable_market_assessment(freshness_unknown, T0, provider)
        self.assertEqual(freshness_assessment["category"], "DATA_FRESHNESS")
        self.assertEqual(freshness_assessment["reason"], "FRESHNESS_UNKNOWN")
        self.assertEqual(freshness_assessment["next_action"], "recheck_next_discovery_tick")

    def test_unknown_rules_page_is_deferred_before_verified_later_page(self) -> None:
        unknown = market("unknown-first")
        unknown_book = unknown.order_book
        self.assertIsNotNone(unknown_book)
        assert unknown_book is not None
        unknown = replace(unknown, order_book=replace(unknown_book, tick_size=None))
        verified = market("verified-later")
        provider = _PagedProvider(
            (unknown, verified),
            (
                {"snapshots": (unknown,), "next_cursor": "unknown-next"},
                {"snapshots": (verified,), "next_cursor": None, "coverage_status": "COMPLETE"},
            ),
        )
        collector = _ScopeCollector(
            provider,
            _ScopeStore({}),
            CollectorConfig(
                max_markets=1,
                discovery_budget_per_cycle=1,
                max_suitable_pages_per_cycle=3,
                required_capital=1.0,
                max_attempts=1,
                backoff_initial_seconds=0,
                jitter_seconds=0,
            ),
            candidate_ids=(),
            clock=lambda: T0,
            sleep=lambda _seconds: None,
        )

        cycle = collector.collect_once(now=T0)

        self.assertEqual([call["after_cursor"] for call in provider.page_calls], [None, "unknown-next"])
        self.assertEqual(cycle.discovery_scheduled, ("verified-later",))
        self.assertEqual(cycle.discovery_deferred, ("unknown-first",))
        self.assertEqual(cycle.discovery_coverage_status, "COMPLETE")
        unknown_evidence = next(
            item for item in collector._suitable_market_evidence if item["market_id"] == "unknown-first"
        )
        self.assertEqual(unknown_evidence["category"], "RULES_UNKNOWN")
        self.assertEqual(unknown_evidence["reason"], "TICK_SIZE_MISSING")

    def test_suitable_discovery_advances_past_unsuitable_page_and_persists_scores(self) -> None:
        constrained = replace(
            market("capital-constrained"),
            order_book=OrderBookSnapshot(
                T0,
                (OrderBookLevel(0.49, 0.5),),
                (OrderBookLevel(0.51, 0.5),),
                "yes-capital-constrained",
                min_order_size=0.01,
                tick_size=0.01,
                neg_risk=False,
            ),
        )
        low_activity = replace(market("low-activity"), volume=1.0)
        high_activity = replace(market("high-activity"), volume=10_000.0)
        provider = _PagedProvider(
            (constrained, low_activity, high_activity),
            (
                {
                    "snapshots": (constrained,),
                    "next_cursor": "unsuitable-first-next",
                    "coverage_status": "PARTIAL",
                },
                {
                    "snapshots": (low_activity, high_activity),
                    "next_cursor": None,
                    "coverage_status": "COMPLETE",
                },
            ),
        )
        collector = _ScopeCollector(
            provider,
            _ScopeStore({}),
            CollectorConfig(
                max_markets=1,
                discovery_budget_per_cycle=1,
                max_suitable_pages_per_cycle=3,
                required_capital=1.0,
                max_attempts=1,
                backoff_initial_seconds=0,
                jitter_seconds=0,
            ),
            candidate_ids=(),
            clock=lambda: T0,
            sleep=lambda _seconds: None,
        )

        cycle = collector.collect_once(now=T0)
        continuation = collector.store.states["polymarket"]["discovery_continuation"]

        self.assertEqual([call["after_cursor"] for call in provider.page_calls], [None, "unsuitable-first-next"])
        self.assertEqual(cycle.discovery_scheduled, ("high-activity",))
        self.assertEqual(cycle.suitable_market_scheduled, ("high-activity",))
        self.assertEqual(cycle.suitable_market_deferred, ("capital-constrained", "low-activity"))
        self.assertEqual(cycle.discovery_deferred, ("capital-constrained", "low-activity"))
        self.assertEqual(cycle.discovery_coverage_status, "COMPLETE")
        self.assertEqual(continuation["after_cursor"], None)
        self.assertEqual(continuation["coverage_status"], "COMPLETE")
        exclusion = continuation["suitability_exclusions"][0]
        self.assertEqual(exclusion["market_id"], "capital-constrained")
        self.assertEqual(exclusion["category"], "CAPITAL_OR_MARKET_CONSTRAINT")
        self.assertEqual(exclusion["reason"], "NO_DEPTH")
        self.assertEqual(exclusion["observed_required_capital"], 0.51 * exclusion["required_quantity"])
        self.assertEqual(exclusion["intended_token"], "yes")
        scores = {
            item["market_id"]: item
            for item in collector._suitable_market_evidence
        }
        self.assertGreater(scores["high-activity"]["activity_score"], scores["low-activity"]["activity_score"])
        self.assertGreater(scores["high-activity"]["freshness_score"], 0.0)
        self.assertEqual(scores["high-activity"]["depth_score"], 10.0)
        self.assertGreater(scores["high-activity"]["entry_depth"], 0.0)
        self.assertGreater(scores["high-activity"]["exit_depth"], 0.0)
        self.assertEqual(cycle.market_authorization["coverage_status"], "COMPLETE")  # type: ignore[index]

    def test_scope_authorizes_verified_market_before_inventory_completion(self) -> None:
        suitable = market("verified-scope", category="politics")
        later = market("later-scope", category="politics")
        provider = _PagedProvider(
            (suitable, later),
            (
                {
                    "snapshots": (suitable,),
                    "next_cursor": "scope-next",
                },
                {
                    "snapshots": (later,),
                    "next_cursor": None,
                    "coverage_status": "COMPLETE",
                },
            ),
        )
        policy = scope("RULE_BASED_MARKETS", category="politics")
        store = _ScopeStore(
            {
                "scoped-candidate": {
                    "experiment_plan": {"market_scope": policy},
                    "required_capital": 1.0,
                }
            }
        )
        cycle = self._collector(provider, store, ("scoped-candidate",), max_markets=1).collect_once(now=T0)

        continuation = store.states["polymarket"]["scope_inventory_continuation"]
        self.assertEqual(cycle.candidate_bound_scheduled, ("verified-scope",))
        self.assertEqual(cycle.inventory_coverage, "BUDGET_EXHAUSTED")
        self.assertEqual(cycle.market_authorization["status"], "VERIFIED_MARKET_AUTHORIZED")  # type: ignore[index]
        self.assertEqual(cycle.market_authorization["verified_market_ids"], ["verified-scope"])  # type: ignore[index]
        self.assertEqual(continuation["after_cursor"], "scope-next")
        self.assertEqual(continuation["verified_market_ids"], ["verified-scope"])
        self.assertEqual(len(store.resolutions), 1)
        resolution = store.resolutions[0]
        expected = resolve_market_scope(
            "scope-check",
            {"market_scope": policy},
            [PolymarketCollector._scope_market_record(suitable, T0, provider)],
            resolved_at=T0,
        )
        self.assertEqual(resolution.scope_hash, expected.scope_hash)  # type: ignore[union-attr]
        self.assertEqual(resolution.scope_version, expected.scope_version)  # type: ignore[union-attr]
        self.assertEqual(
            cycle.market_authorization["scope_bindings"],  # type: ignore[index]
            [{
                "candidate_id": "scoped-candidate",
                "scope_hash": expected.scope_hash,
                "scope_version": expected.scope_version,
            }],
        )
    def test_scope_suitability_cache_reuses_identical_probe_with_explicit_ceiling(self) -> None:
        snapshot = replace(market("cached-scope"), order_book=None)
        provider = _PagedProvider((snapshot,), ())
        collector = self._collector(provider, _ScopeStore({}), ())
        counters = collector._new_counters()

        first = collector._cached_scope_suitability_assessment(
            snapshot,
            T0,
            provider,
            counters=counters,
            intended_token="yes",
        )
        second = collector._cached_scope_suitability_assessment(
            snapshot,
            T0,
            provider,
            counters=counters,
            intended_token="yes",
        )

        self.assertEqual(first, second)
        self.assertEqual(provider.book_calls, ["cached-scope"])
        self.assertEqual(len(collector._scope_suitability_cache), 1)
        self.assertLessEqual(
            len(collector._scope_suitability_cache),
            _MAX_SCOPE_SUITABILITY_CACHE,
        )

    def test_legacy_scope_continuation_bounds_inventory_and_refresh_queue(self) -> None:
        visible = market("legacy-visible")
        provider = _RecordingProvider((visible,))
        store = _ScopeStore(
            {
                "candidate": {
                    "experiment_plan": {
                        "market_scope": scope(
                            "EXACT_MARKETS",
                            market_ids=("legacy-visible",),
                        )
                    }
                }
            }
        )
        store.states["polymarket"] = {
            "scope_inventory_continuation": {
                "after_cursor": 0,
                "coverage_status": "BUDGET_EXHAUSTED",
                "inventory_records": [
                    {"market_id": f"legacy-{index}"}
                    for index in range(_MAX_SCOPE_INVENTORY + 17)
                ],
                "suitability_refresh_queue": [
                    f"legacy-{index}"
                    for index in range(_MAX_SCOPE_INVENTORY + 23)
                ],
            }
        }

        collector = self._collector(provider, store, ("candidate",), max_markets=1)
        collector.collect_once(now=T0)

        continuation = store.states["polymarket"]["scope_inventory_continuation"]
        self.assertLessEqual(len(continuation["inventory_records"]), _MAX_SCOPE_INVENTORY)
        self.assertLessEqual(
            len(continuation["suitability_refresh_queue"]),
            _MAX_SCOPE_INVENTORY,
        )
        self.assertEqual(continuation["inventory_records"][0]["market_id"], "legacy-0")
        self.assertEqual(continuation["suitability_refresh_queue"][0], "legacy-0")

    def test_scope_cap_keeps_omitted_paper_scopes_out_of_legacy_requirements(self) -> None:
        scoped_market = market("paper-cap-market")
        documents = {
            f"paper-scoped-{index}": {
                "experiment_plan": {
                    "market_scope": scope(
                        "EXACT_MARKETS",
                        market_ids=("paper-cap-market",),
                    )
                }
            }
            for index in range(1_001)
        }
        store = _ScopeStore(documents)
        for record in store.documents.values():
            record["stage"] = "PAPER_FORWARD"
        provider = _RecordingProvider((scoped_market,))
        collector = self._collector(provider, store, (), max_markets=1)

        cycle = collector.collect_once(now=T0)

        self.assertEqual(cycle.paper_forward_scheduled, ("paper-cap-market",))
        self.assertEqual(len(store.resolutions), 1_000)
        self.assertTrue(
            all(
                "paper-scoped-1000" not in candidate_ids
                for candidate_ids in store.requirement_calls
            )
        )

    def test_shared_scope_refresh_snapshot_is_reused_by_later_candidate(self) -> None:
        shared = market("shared-refresh")
        document = {
            "experiment_plan": {
                "market_scope": scope("EXACT_MARKETS", market_ids=("shared-refresh",)),
                "suitability": {"required_capital": 1.0},
            }
        }
        store = _ScopeStore({"first": document, "second": document})
        provider = _RecordingProvider((shared,))
        collector = self._collector(provider, store, ("first", "second"))
        record = PolymarketCollector._scope_market_record(shared, T0, provider)
        collector._scope_inventory_continuation = {
            "coverage_status": "BUDGET_EXHAUSTED",
            "after_cursor": "shared-cursor",
        }
        collector._discover_scope_inventory = lambda *args, **kwargs: (
            [record],
            {},
            "shared-cursor",
        )
        collector._scope_resolutions = {}

        _, candidate_markets, _, _ = collector._resolve_market_scopes(
            T0,
            ("first", "second"),
            {},
            collector._new_counters(),
        )

        self.assertEqual(
            candidate_markets,
            {
                "first": ["shared-refresh"],
                "second": ["shared-refresh"],
            },
        )
        self.assertEqual(provider.market_calls, ["shared-refresh"])

    def test_cached_unsuitable_prefix_does_not_hide_later_unresolved_suitable_market(self) -> None:
        unsuitable = tuple(market(f"unsuitable-{index}") for index in range(1_000))
        suitable = market("later-suitable")
        document = {
            "experiment_plan": {
                "market_scope": scope("EXACT_MARKETS", market_ids=("later-suitable",)),
                "suitability": {"required_capital": 1.0},
            }
        }
        store = _ScopeStore({"candidate": document})
        provider = _RecordingProvider((*unsuitable, suitable))
        collector = self._collector(provider, store, ("candidate",))
        records = [
            PolymarketCollector._scope_market_record(item, T0, provider)
            for item in (*unsuitable, suitable)
        ]
        snapshots = {item.market_id: item for item in unsuitable}
        kwargs = collector._suitability_kwargs(document)
        collector._scope_suitability_cache = {
            (
                item.market_id,
                _stable_payload(kwargs),
            ): {
                "market_id": item.market_id,
                "action": "UNSUITABLE",
                "category": "CAPITAL_OR_MARKET_CONSTRAINT",
                "reason": "NO_DEPTH",
            }
            for item in unsuitable
        }
        collector._scope_inventory_continuation = {
            "coverage_status": "BUDGET_EXHAUSTED",
            "after_cursor": "unsuitable-cursor",
        }
        collector._discover_scope_inventory = lambda *args, **kwargs: (
            records,
            snapshots,
            "unsuitable-cursor",
        )
        collector._scope_resolutions = {}

        _, candidate_markets, _, _ = collector._resolve_market_scopes(
            T0,
            ("candidate",),
            {},
            collector._new_counters(),
        )

        self.assertEqual(candidate_markets, {"candidate": ["later-suitable"]})
        self.assertEqual(provider.market_calls, ["later-suitable"])

    def test_scope_refresh_timeout_rotates_carried_inventory_queue(self) -> None:
        first = market("first-scope", category="politics")
        second = market("second-scope", category="politics")
        provider = _PagedProvider(
            (first, second),
            (
                {"snapshots": (), "next_cursor": "scope-next"},
                {"snapshots": (), "next_cursor": "scope-next-2"},
            ),
        )
        store = _ScopeStore({})
        policy = scope("EXACT_MARKETS", market_ids=("second-scope",))
        document = {
            "experiment_plan": {
                "market_scope": policy,
                "suitability": {"required_capital": 1.0},
            }
        }

        class RotatingCollector(_ScopeCollector):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.refresh_calls: list[str] = []

            def _refresh_scope_snapshot(self, snapshot, provider, observed_at, counters):
                market_id = str(snapshot.market_id)
                self.refresh_calls.append(market_id)
                if market_id == "first-scope":
                    self._scope_refresh_attempted.add(market_id)
                    self._cycle_deadline_exhausted = True
                    return None
                return super()._refresh_scope_snapshot(snapshot, provider, observed_at, counters)

        collector = RotatingCollector(
            provider,
            store,
            CollectorConfig(
                max_markets=2,
                discovery_budget_per_cycle=10,
                max_attempts=1,
                backoff_initial_seconds=0,
                jitter_seconds=0,
            ),
            candidate_ids=(),
            clock=lambda: T0,
            sleep=lambda _seconds: None,
        )
        records = [
            PolymarketCollector._scope_market_record(item, T0, provider)
            for item in (first, second)
        ]
        collector._scope_inventory_continuation = {
            "after_cursor": "scope-cursor",
            "coverage_status": "BUDGET_EXHAUSTED",
            "suitability_enabled": True,
            "suitability_refresh_queue": ["first-scope", "second-scope"],
            "inventory_records": records,
        }
        collector._scope_refresh_attempted = set()
        collector._cycle_deadline_exhausted = False
        collector._discover_scope_inventory(
            T0,
            collector._new_counters(),
            carry_cursor="scope-cursor",
            documents=(document,),
        )
        self.assertEqual(collector.refresh_calls, ["first-scope"])
        self.assertEqual(
            collector._scope_inventory_continuation["suitability_refresh_queue"],
            ["second-scope", "first-scope"],
        )
        self.assertLessEqual(
            len(collector._scope_inventory_continuation["suitability_refresh_queue"]),
            _MAX_SCOPE_INVENTORY,
        )

        collector._scope_refresh_attempted = set()
        collector._cycle_deadline_exhausted = False
        records_after, snapshots_after, _ = collector._discover_scope_inventory(
            T0 + timedelta(seconds=1),
            collector._new_counters(),
            carry_cursor="scope-next",
            documents=(document,),
        )
        self.assertEqual(
            collector.refresh_calls,
            ["first-scope", "second-scope", "first-scope"],
        )
        self.assertEqual(
            collector._scope_inventory_continuation["suitability_refresh_queue"],
            ["second-scope", "first-scope"],
        )
        self.assertLessEqual(
            len(collector._scope_inventory_continuation["suitability_refresh_queue"]),
            _MAX_SCOPE_INVENTORY,
        )
        self.assertEqual(set(snapshots_after), {"second-scope"})
        self.assertEqual(
            collector._scope_inventory_continuation["verified_market_ids"],
            ["second-scope"],
        )
        cached_second = [
            assessment
            for (market_id, _), assessment in collector._scope_suitability_cache.items()
            if market_id == "second-scope"
        ]
        self.assertEqual(len(cached_second), 1)
        self.assertEqual(cached_second[0]["action"], "SUITABLE")
        self.assertEqual(cached_second[0]["market_id"], "second-scope")
        suitability_cache_before = {
            key: dict(value)
            for key, value in collector._scope_suitability_cache.items()
        }
        store.documents["candidate"] = {
            "candidate_id": "candidate",
            "stage": "FROZEN",
            "payload": document,
        }
        refresh_calls_before_resolver = list(collector.refresh_calls)
        books_before_resolver = list(provider.book_calls)
        collector._discover_scope_inventory = lambda *args, **kwargs: (
            records_after,
            snapshots_after,
            "scope-next-2",
        )
        collector._cycle_deadline_monotonic = time.monotonic()
        collector._scope_resolutions = {}
        _, candidate_markets, _, _ = collector._resolve_market_scopes(
            T0 + timedelta(seconds=1),
            ("candidate",),
            {},
            collector._new_counters(),
        )
        self.assertEqual(candidate_markets, {"candidate": ["second-scope"]})
        self.assertEqual(provider.book_calls, books_before_resolver)
        self.assertEqual(collector.refresh_calls, refresh_calls_before_resolver)
        proof = collector._scope_resolutions["candidate"]
        self.assertEqual(proof["status"], MATCHED)
        self.assertEqual(
            [item["market_id"] for item in proof["matched_markets"]],
            ["second-scope"],
        )
    def test_reserved_pipeline_verifies_later_queue_head_before_broad_refresh(self) -> None:
        head = replace(market("queue-head"), order_book=None)
        suitable_base = market("queue-suitable")
        suitable_book = suitable_base.order_book
        self.assertIsNotNone(suitable_book)
        suitable = replace(suitable_base, order_book=None)
        broad_base = market("broad-page")
        broad_book = broad_base.order_book
        self.assertIsNotNone(broad_book)
        broad = replace(broad_base, order_book=None)

        class BudgetedProvider(_PagedProvider):
            def order_books(self, market_id: str, depth: int = 20):
                self.book_calls.append(str(market_id))
                if market_id == "queue-head":
                    return {}
                if market_id == "queue-suitable":
                    return {"yes": suitable_book}
                if market_id == "broad-page":
                    return {"yes": broad_book}
                return super().order_books(market_id, depth=depth)

        provider = BudgetedProvider(
            (head, suitable, broad),
            ({"snapshots": (broad,), "next_cursor": None},),
        )
        document = {
            "experiment_plan": {
                "market_scope": scope("EXACT_MARKETS", market_ids=("queue-suitable",)),
                "suitability": {"required_capital": 1.0},
            }
        }
        store = _ScopeStore({"candidate": document})
        collector = _BudgetedScopeCollector(
            provider,
            store,
            candidate_ids=("candidate",),
            config=CollectorConfig(
                max_markets=1,
                discovery_budget_per_cycle=1,
                provider_timeout_seconds=1.0,
                max_attempts=1,
                backoff_initial_seconds=0,
                jitter_seconds=0,
            ),
            clock=lambda: T0,
            sleep=lambda _seconds: None,
        )
        records = [
            PolymarketCollector._scope_market_record(item, T0, provider)
            for item in (head, suitable)
        ]
        collector._scope_inventory_continuation = {
            "after_cursor": "scope-cursor",
            "coverage_status": "BUDGET_EXHAUSTED",
            "suitability_enabled": True,
            "suitability_refresh_queue": ["queue-head", "queue-suitable"],
            "inventory_records": records,
        }

        records_after, snapshots_after, _ = collector._discover_scope_inventory(
            T0,
            collector._new_counters(),
            carry_cursor="scope-cursor",
            documents=(document,),
        )
        continuation = collector._scope_inventory_continuation
        self.assertIn("queue-suitable", continuation["verified_market_ids"])
        self.assertEqual(continuation["suitability_refresh_queue"][0], "queue-suitable")
        self.assertEqual(provider.market_calls, ["queue-head", "queue-suitable"])
        self.assertEqual(provider.book_calls[:2], ["queue-head", "queue-suitable"])
        self.assertNotEqual(provider.book_calls[:1], ["broad-page"])

        collector._discover_scope_inventory = lambda *args, **kwargs: (
            records_after,
            snapshots_after,
            "scope-next",
        )
        collector._scope_resolutions = {}
        _, candidate_markets, _, _ = collector._resolve_market_scopes(
            T0,
            ("candidate",),
            {},
            collector._new_counters(),
        )
        self.assertEqual(candidate_markets, {"candidate": ["queue-suitable"]})


    def test_invalid_observation_assumptions_do_not_poison_valid_exact_scope(self) -> None:
        valid = market("valid-exact")
        invalid = market("invalid-observation")
        valid_document = {
            "experiment_plan": {
                "market_scope": scope("EXACT_MARKETS", market_ids=("valid-exact",)),
                "suitability": {"required_capital": 1.0},
            }
        }
        invalid_document = {
            "paper_observation_intent_id": "observation-invalid",
            "experiment_plan": {
                "market_scope": scope("EXACT_MARKETS", market_ids=("invalid-observation",)),
                "required_capital": 2.0,
                "suitability": {"required_capital": 1.0},
            },
        }
        store = _ScopeStore(
            {
                "valid-candidate": {"experiment_plan": valid_document["experiment_plan"]},
                "invalid-observation": invalid_document,
            }
        )
        provider = _PagedProvider(
            (valid, invalid),
            ({"snapshots": (valid, invalid), "next_cursor": None},),
        )
        cycle = self._collector(
            provider,
            store,
            ("valid-candidate", "invalid-observation"),
            max_markets=2,
        ).collect_once(now=T0)

        self.assertIn("valid-exact", cycle.candidate_bound_scheduled)
        self.assertNotIn("invalid-observation", cycle.candidate_bound_scheduled)
        self.assertEqual(
            cycle.market_authorization["verified_market_ids"],  # type: ignore[index]
            ["valid-exact"],
        )
        invalid_evidence = [
            item
            for item in cycle.discovery_exclusions
            if item["market_id"] == "invalid-observation"
        ]
        self.assertTrue(invalid_evidence)
        self.assertEqual(invalid_evidence[-1]["reason"], "SUITABILITY_ASSUMPTIONS_UNKNOWN")

    def test_full_cycle_reserves_collection_after_scope_authorization(self) -> None:
        base = market("collect-after-scope")
        selected_book = base.order_book
        self.assertIsNotNone(selected_book)
        selected = replace(base, order_book=None)

        class CollectingProvider(_PagedProvider):
            def order_books(self, market_id: str, depth: int = 20):
                self.book_calls.append(str(market_id))
                return {"yes": selected_book}

        provider = CollectingProvider(
            (selected,),
            ({"snapshots": (selected,), "next_cursor": None},),
        )
        store = _ScopeStore(
            {
                "collect-candidate": {
                    "experiment_plan": {
                        "market_scope": scope(
                            "EXACT_MARKETS",
                            market_ids=("collect-after-scope",),
                        ),
                        "suitability": {"required_capital": 1.0},
                    }
                }
            }
        )
        collector = _BudgetedScopeCollector(
            provider,
            store,
            candidate_ids=("collect-candidate",),
            config=CollectorConfig(
                max_markets=1,
                discovery_budget_per_cycle=1,
                provider_timeout_seconds=1.0,
                max_attempts=1,
                backoff_initial_seconds=0,
                jitter_seconds=0,
            ),
            clock=lambda: T0,
            sleep=lambda _seconds: None,
        )

        cycle = collector.collect_once(now=T0)

        self.assertIn("collect-after-scope", cycle.candidate_bound_scheduled)
        self.assertIn("collect-after-scope", cycle.market_authorization["verified_market_ids"])  # type: ignore[index]
        self.assertGreaterEqual(cycle.markets_attempted, 1)
        self.assertGreaterEqual(cycle.snapshots_inserted, 1)
        self.assertTrue(provider.market_calls)
        self.assertTrue(provider.book_calls)
    def test_hung_scope_book_uses_isolated_pool_for_collection(self) -> None:
        scope_market = replace(market("scope-hang"), order_book=None)
        collect_market = market("collect-live")
        provider = _HangingScopeBookProvider(
            (scope_market, collect_market),
            (
                {"snapshots": (scope_market,), "next_cursor": None},
                {"snapshots": (scope_market,), "next_cursor": None},
            ),
        )
        store = _ScopeStore(
            {
                "scope-candidate": {
                    "experiment_plan": {
                        "market_scope": scope("EXACT_MARKETS", market_ids=("scope-hang",)),
                        "suitability": {"required_capital": 1.0},
                    }
                }
            }
        )

        class IsolatedScopeCollector(_ScopeCollector):
            def _rolling_scope_market_ids(self) -> list[str]:
                return ["collect-live"]

        collector = IsolatedScopeCollector(
            provider,
            store,
            CollectorConfig(
                max_markets=1,
                discovery_budget_per_cycle=1,
                provider_timeout_seconds=0.15,
                max_attempts=1,
                backoff_initial_seconds=0,
                jitter_seconds=0,
                market_ids=("collect-live",),
            ),
            candidate_ids=("scope-candidate",),
            clock=lambda: T0,
            sleep=lambda _seconds: None,
        )

        first = collector.collect_once(now=T0)
        self.assertTrue(provider.scope_entered.wait(timeout=0.2))
        self.assertGreaterEqual(first.markets_attempted, 1)
        self.assertGreaterEqual(first.snapshots_inserted, 1)

        second = collector.collect_once(now=T0 + timedelta(seconds=1))
        self.assertEqual(provider.scope_calls, 1)


    def test_selected_token_book_never_uses_wrong_singleton(self) -> None:
        base = market("exact-book")
        wrong = replace(base.order_book, token_id="no-other-market")

        class WrongSingletonProvider(_RecordingProvider):
            def order_books(self, market_id: str, depth: int = 20):
                del market_id, depth
                return {"other": wrong}

        provider = WrongSingletonProvider((base,))
        collector = self._collector(provider, _ScopeStore({}), ())
        assessment = collector._suitable_market_assessment(
            base,
            T0,
            provider,
            intended_token="no",
        )

        self.assertEqual(assessment["action"], "UNSUITABLE")
        self.assertEqual(assessment["reason"], "NO_DEPTH")

    def test_frozen_suitability_assumptions_apply_fee_to_capital(self) -> None:
        base = market("frozen-assumptions")
        provider = _RecordingProvider((base,))
        collector = self._collector(provider, _ScopeStore({}), ())
        document = {
            "assumptions": {
                "required_capital": 1.0,
                "min_entry_depth": 1.0,
                "min_exit_depth": 1.0,
                "min_activity": 100.0,
                "intended_token": "yes",
                "venue_fee_rate": 0.10,
            }
        }
        kwargs = collector._suitability_kwargs(document)
        assessment = collector._suitable_market_assessment(base, T0, provider, **kwargs)

        self.assertEqual(kwargs["intended_token"], "yes")
        self.assertEqual(kwargs["venue_fee_rate"], 0.10)
        self.assertEqual(assessment["action"], "SUITABLE")
        self.assertEqual(assessment["required_quantity"], 1.78)
        self.assertLessEqual(assessment["observed_required_capital"], 1.0)

    def test_fractional_displayed_depth_is_not_rejected_as_size_precision(self) -> None:
        book = {
            "token_id": "yes-fractional",
            "min_order_size": "0.01",
            "tick_size": "0.01",
            "neg_risk": False,
            "asks": [["0.50", "0.1234"]],
            "bids": [["0.49", "0.1234"]],
        }
        rules = parse_polymarket_rules(book)

        assessment = assess_selected_token_depth(
            book,
            rules,
            side="BUY",
            quantity=Decimal("0.12"),
        )

        self.assertEqual(assessment.action, "SUITABLE")
        self.assertEqual(assessment.available_quantity, Decimal("0.1234"))
        self.assertEqual(assessment.filled_quantity, Decimal("0.12"))

    def test_keyset_list_page_is_consumed_and_repeated_cursor_rebases(self) -> None:
        constrained = replace(
            market("cursor-constrained"),
            order_book=replace(
                market("cursor-constrained").order_book,
                bids=(OrderBookLevel(0.49, 0.5),),
                asks=(OrderBookLevel(0.51, 0.5),),
            ),
        )
        suitable = market("cursor-suitable")
        class CursorList(list):
            def __init__(self, values, *, next_cursor=None):
                super().__init__(values)
                self.next_cursor = next_cursor

        class ListPageProvider(_PagedProvider):
            def market_page(self, **kwargs):
                self.page_calls.append(dict(kwargs))
                if not self.pages:
                    raise AssertionError("unexpected metadata page")
                return self.pages.pop(0)

        provider = ListPageProvider(
            (constrained, suitable),
            (
                CursorList([constrained], next_cursor="loop"),
                {"snapshots": (constrained,), "next_cursor": "loop"},
                (suitable,),
            ),
        )
        collector = _ScopeCollector(
            provider,
            _ScopeStore({}),
            CollectorConfig(
                max_markets=1,
                discovery_budget_per_cycle=1,
                max_suitable_pages_per_cycle=2,
                required_capital=1.0,
                max_attempts=1,
                backoff_initial_seconds=0,
                jitter_seconds=0,
            ),
            candidate_ids=(),
            clock=lambda: T0,
            sleep=lambda _seconds: None,
        )

        first = collector.collect_once(now=T0)
        first_continuation = collector.store.states["polymarket"]["discovery_continuation"]
        second = collector.collect_once(now=T0)

        self.assertEqual(first.discovery_scheduled, ())
        self.assertEqual(second.discovery_scheduled, ("cursor-suitable",))
        self.assertEqual(
            [call["after_cursor"] for call in provider.page_calls],
            [None, "loop", None],
        )
        self.assertIsNone(first_continuation["after_cursor"])
        self.assertTrue(first_continuation["query_reset"])
        self.assertTrue(first_continuation["rebase_required"])

    def test_hanging_scope_inventory_times_out_once_and_retries_after_release(self) -> None:
        provider = _HangingScopeProvider(
            (),
            ({"markets": (), "next_cursor": None},),
        )
        store = _ScopeStore(
            {
                "scoped": {
                    "experiment_plan": {
                        "market_scope": scope("RULE_BASED_MARKETS", category="politics")
                    }
                }
            }
        )
        store.states["polymarket"] = {
            "scope_inventory_continuation": {
                "after_cursor": "opaque-cursor",
                "coverage_status": "BUDGET_EXHAUSTED",
                "request_query": {
                    "limit": 1,
                    "after_cursor": "opaque-cursor",
                    "closed": False,
                    "tag_ids": (),
                    "include_tag": True,
                    "liquidity_num_min": None,
                    "end_date_min": None,
                    "end_date_max": None,
                },
            }
        }
        collector = _ScopeCollector(
            provider,
            store,
            CollectorConfig(
                max_markets=1,
                discovery_budget_per_cycle=1,
                max_attempts=1,
                provider_timeout_seconds=0.05,
                backoff_initial_seconds=0,
                jitter_seconds=0,
            ),
            candidate_ids=("scoped",),
            clock=lambda: T0,
            sleep=lambda _seconds: None,
        )

        started = time.monotonic()
        first = collector.collect_once(now=T0)
        elapsed = time.monotonic() - started
        continuation = store.states["polymarket"]["scope_inventory_continuation"]

        self.assertLess(elapsed, 0.5)
        self.assertEqual(provider.keyset_calls, 1)
        self.assertEqual(first.provider_timeouts, 1)
        self.assertEqual(first.provider_timeout_evidence[0]["reason"], "PROVIDER_CALL_TIMEOUT")
        self.assertEqual(continuation["after_cursor"], "opaque-cursor")
        self.assertEqual(continuation["timeout_reason"], "PROVIDER_CALL_TIMEOUT")
        self.assertEqual(continuation["resolver"], "retry_provider_call")
        self.assertEqual(continuation["next_action"], "retry_next_collection_tick")

        second = collector.collect_once(now=T0)
        self.assertLess(second.duration_seconds, 0.5)
        self.assertEqual(provider.keyset_calls, 1)

        provider.release.set()
        third = collector.collect_once(now=T0)
        self.assertEqual(provider.keyset_calls, 2)

    def test_hanging_inventory_does_not_starve_direct_exact_scope_across_cycles(self) -> None:
        closed = replace(
            market("exact-closed"),
            active=True,
            closed=True,
            settlement=SettlementState.RESOLVED_YES,
        )
        eligible = market("exact-eligible", category="politics")

        class DelayedDirectProvider(_HangingScopeProvider):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.exact_market_latencies: list[float] = []

            def market(self, market_id: str):
                started = time.perf_counter()
                time.sleep(0.005)
                result = super().market(market_id)
                if str(market_id) in {"exact-closed", "exact-eligible"}:
                    self.exact_market_latencies.append(time.perf_counter() - started)
                return result

        provider = DelayedDirectProvider(
            (closed, eligible),
            ({"markets": (), "next_cursor": None},),
        )
        store = _ScopeStore(
            {
                "exact": {
                    "experiment_plan": {
                        "market_scope": scope(
                            "EXACT_MARKETS",
                            market_ids=("exact-closed", "exact-eligible"),
                        )
                    }
                },
                "inventory": {
                    "experiment_plan": {
                        "market_scope": scope(
                            "RULE_BASED_MARKETS",
                            category="politics",
                        )
                    }
                },
            }
        )
        collector = _TwoWindowScopeCollector(
            provider,
            store,
            CollectorConfig(
                max_markets=1,
                discovery_budget_per_cycle=1,
                max_attempts=1,
                provider_timeout_seconds=0.05,
                backoff_initial_seconds=0,
                jitter_seconds=0,
            ),
            candidate_ids=("exact", "inventory"),
            clock=lambda: T0,
            sleep=lambda _seconds: None,
        )

        cycles = [
            collector.collect_once(now=T0 + timedelta(seconds=61 * index))
            for index in range(5)
        ]

        self.assertEqual(provider.keyset_calls, 1)
        self.assertGreaterEqual(provider.market_calls.count("exact-closed"), 5)
        self.assertGreaterEqual(provider.market_calls.count("exact-eligible"), 5)
        self.assertTrue(
            all("exact-eligible" in cycle.candidate_bound_scheduled for cycle in cycles)
        )
        self.assertTrue(all(cycle.snapshots_inserted >= 1 for cycle in cycles))
        self.assertTrue(
            all(latency > 0.001 for latency in provider.exact_market_latencies),
            provider.exact_market_latencies,
        )
        self.assertEqual(len(store.resolutions), 5)
        exact_resolutions = [
            result for result in store.resolutions if result.candidate_id == "exact"
        ]
        self.assertEqual(len(exact_resolutions), 5)
        self.assertTrue(
            all(
                any(
                    item.market_id == "exact-closed"
                    and item.reason == "MARKET_CLOSED"
                    for item in result.excluded_markets
                )
                for result in exact_resolutions
            )
        )
        self.assertIsNotNone(collector._scope_provider_executor)
        self.assertIsNotNone(collector._scope_direct_provider_executor)
        self.assertIsNotNone(collector._collection_provider_executor)
        self.assertEqual(
            len(collector._scope_provider_executor._threads),  # type: ignore[union-attr]
            1,
        )
        self.assertEqual(
            len(collector._scope_direct_provider_executor._threads),  # type: ignore[union-attr]
            1,
        )

        provider.release.set()
        collector.close()
        collector.close()
        for executor in (
            collector._scope_provider_executor,
            collector._scope_direct_provider_executor,
            collector._collection_provider_executor,
        ):
            self.assertTrue(executor._closed)  # type: ignore[union-attr]
            self.assertFalse(
                any(thread.is_alive() for thread in executor._threads)  # type: ignore[union-attr]
            )
    def test_run_forever_closes_provider_executor(self) -> None:
        provider = _RecordingProvider((market("standalone"),))
        collector = _ScopeCollector(
            provider,
            _ScopeStore({}),
            CollectorConfig(
                market_ids=("standalone",),
                max_markets=1,
                discovery_budget_per_cycle=0,
                max_attempts=1,
                provider_timeout_seconds=0.05,
                backoff_initial_seconds=0,
                jitter_seconds=0,
            ),
            candidate_ids=(),
            clock=lambda: T0,
            sleep=lambda _seconds: None,
        )

        results = collector.run_forever(cycles=1)

        self.assertEqual(len(results), 1)
        self.assertIsNotNone(collector._collection_provider_executor)
        self.assertTrue(collector._collection_provider_executor._closed)  # type: ignore[union-attr]
        self.assertFalse(
            any(
                thread.is_alive()
                for thread in collector._collection_provider_executor._threads  # type: ignore[union-attr]
            )
        )
        collector.close()

    def test_provider_stage_telemetry_does_not_rewrite_collector_state(self) -> None:
        with AxiomStore(":memory:") as store:
            collector = PolymarketCollector(
                object(),
                store,
                CollectorConfig(
                    max_attempts=1,
                    provider_timeout_seconds=0.05,
                    backoff_initial_seconds=0,
                    jitter_seconds=0,
                ),
                clock=lambda: T0,
                sleep=lambda _seconds: None,
            )
            collector._set_current_stage("cycle_start", None, T0, persist=True)
            before = int(
                store.connection.execute(
                    "SELECT length(state_json) FROM collector_state "
                    "WHERE collector_name='polymarket'"
                ).fetchone()[0]
            )
            counters = collector._new_counters()
            started = time.monotonic()
            for index in range(256):
                collector._call_provider(
                    f"market:{index}",
                    lambda: None,
                    T0,
                    counters,
                )
            elapsed = time.monotonic() - started
            row = store.connection.execute(
                "SELECT length(state_json), state_json FROM collector_state "
                "WHERE collector_name='polymarket'"
            ).fetchone()

            self.assertLess(elapsed, 2.0)
            self.assertEqual(int(row[0]), before)
            self.assertEqual(json.loads(row[1])["current_stage"], "cycle_start")

    def test_cycle_deadline_bounds_many_markets_and_resumes_work(self) -> None:
        class NearTimeoutProvider(_RecordingProvider):
            def __init__(self, markets):
                super().__init__(markets)
                self.active: set[str] = set()
                self.duplicate_in_flight = False
                self._active_lock = threading.Lock()

            def market(self, market_id: str):
                identifier = str(market_id)
                with self._active_lock:
                    if identifier in self.active:
                        self.duplicate_in_flight = True
                    self.active.add(identifier)
                try:
                    time.sleep(0.018)
                    return super().market(identifier)
                finally:
                    with self._active_lock:
                        self.active.discard(identifier)

            def metadata(self, market_id: str):
                snapshot = self._markets.get(str(market_id))
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

        identifiers = tuple(str(index) for index in range(100))
        provider = NearTimeoutProvider(tuple(market(identifier) for identifier in identifiers))
        with AxiomStore(":memory:") as store:
            config = CollectorConfig(
                market_ids=identifiers,
                max_markets=100,
                max_attempts=1,
                provider_timeout_seconds=0.025,
                failure_cooldown_seconds=0,
                backoff_initial_seconds=0,
                jitter_seconds=0,
            )
            collector = PolymarketCollector(
                provider,
                store,
                config,
                clock=lambda: T0,
                sleep=lambda _seconds: None,
            )
            started = time.monotonic()
            first = collector.collect_once(now=T0)
            elapsed = time.monotonic() - started
            state = store.get_collector_state("polymarket") or {}

            self.assertLessEqual(elapsed, config.cycle_budget_seconds + 0.15)
            self.assertEqual(first.current_stage, "degraded")
            continuation = state["cycle_continuation"]
            self.assertTrue(continuation["retryable"])
            self.assertEqual(continuation["resolver"], "retry_provider_call")
            self.assertTrue(continuation["endpoint"])
            self.assertLessEqual(len(continuation["remaining_market_ids"]), 256)
            first_call_count = len(provider.market_calls)

            collector.collect_once(now=T0 + timedelta(seconds=61))
            self.assertGreater(len(provider.market_calls), first_call_count)
            self.assertFalse(provider.duplicate_in_flight)

    def test_oversized_scope_state_is_compacted_on_load_and_save(self) -> None:
        records = [
            {
                "market_id": str(index),
                "condition_id": f"condition-{index}",
                "yes_token_id": f"yes-{index}",
                "no_token_id": f"no-{index}",
                "question": "question-" + ("x" * 5000),
                "active": True,
                "closed": False,
                "accepting_orders": True,
                "order_book_available": True,
                "source_type": "CURRENT",
            }
            for index in range(3_000)
        ]
        state = {
            "last_trade_cursor": "trade-cursor",
            "scope_inventory_continuation": {
                "after_cursor": "opaque-cursor",
                "coverage_status": "ERROR",
                "timeout_reason": "PROVIDER_CALL_TIMEOUT",
                "timeout_endpoint": "scope_keyset:/markets/keyset",
                "resolver": "retry_provider_call",
                "next_action": "retry_next_collection_tick",
                "inventory_records": records,
                "seen_market_ids": [str(index) for index in range(3_000)],
                "seen_cursor_history": [f"cursor-{index}-" + ("y" * 200) for index in range(200)],
                "suitability_exclusions": [
                    {"market_id": str(index), "reason": "NO_DEPTH", "detail": "z" * 5000}
                    for index in range(300)
                ],
            },
        }
        with AxiomStore(":memory:") as store:
            store.set_collector_state("polymarket", state)
            row = store.connection.execute(
                "SELECT length(state_json) FROM collector_state WHERE collector_name='polymarket'"
            ).fetchone()
            self.assertLessEqual(int(row[0]), _COLLECTOR_STATE_MAX_BYTES)
            loaded = store.get_collector_state("polymarket") or {}
            continuation = loaded["scope_inventory_continuation"]
            self.assertEqual(continuation["after_cursor"], "opaque-cursor")
            self.assertEqual(continuation["timeout_endpoint"], "scope_keyset:/markets/keyset")
            self.assertEqual(continuation["resolver"], "retry_provider_call")
            self.assertEqual(loaded["last_trade_cursor"], "trade-cursor")
            self.assertLessEqual(
                len(json.dumps(loaded, separators=(",", ":")).encode("utf-8")),
                _COLLECTOR_STATE_MAX_BYTES,
            )

if __name__ == "__main__":

    unittest.main()
