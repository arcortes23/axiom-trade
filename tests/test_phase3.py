from __future__ import annotations
from copy import deepcopy
from dataclasses import replace

from datetime import datetime, timedelta, timezone
import io
import threading
import time
import json
from pathlib import Path
import tempfile
import unittest
from urllib.request import Request

from axiom.collector import CollectorConfig, PolymarketCollector
from axiom.data import InMemoryPredictionProvider, PolymarketAdapter
from axiom.data._http import HTTPFetchError, fetch_json_strict
from axiom.canary import CanaryService
from axiom.director import research_summary, validate_hermes_proposal
from axiom.domain import (
    CryptoTicker,
    Fill,
    MarketType,
    OHLCVBar,
    OrderBookLevel,
    OrderBookSnapshot,
    PredictionMarketSnapshot,
    SettlementState,
    Side,
    TradePrint,
)
from axiom.forward import ForwardTestRegistry
from axiom.lifecycle import CandidateLifecycleManager, CandidateStage, PromotionCriteria
from axiom.mutations import DeterministicMutationEngine, ExperimentBudget
from axiom.node import NodeConfig, ResearchNode
from axiom.portfolio import Portfolio
from axiom.paper import PredictionPaperTrader
from axiom.paper_engine import ForwardPaperEngine, historical_replay_id, run_forward_paper, run_historical_replay
from axiom.research_bus import DurableResearchBus, ResearchBusPermissionError, ResearchQueueStatus
from axiom.risk import RiskEngine, RiskLimits
from axiom.storage import AxiomStore
from axiom.strategy import validate_strategy


T0 = datetime(2025, 1, 1, tzinfo=timezone.utc)


def market(market_id: str = "m", *, settlement: SettlementState = SettlementState.OPEN, expiry: datetime | None = None, yes_mid: float = 0.5) -> PredictionMarketSnapshot:
    book = OrderBookSnapshot(T0, (OrderBookLevel(yes_mid - 0.01, 10.0),), (OrderBookLevel(yes_mid + 0.01, 10.0),), "yes")
    return PredictionMarketSnapshot(
        timestamp=T0,
        market_id=market_id,
        question="Will the event resolve YES?",
        yes_bid=yes_mid - 0.01,
        yes_ask=yes_mid + 0.01,
        yes_mid=yes_mid,
        no_bid=1.0 - yes_mid - 0.01,
        no_ask=1.0 - yes_mid + 0.01,
        no_mid=1.0 - yes_mid,
        volume=1000.0,
        liquidity=100.0,
        expiry=expiry,
        settlement=settlement,
        resolution_criteria="public result",
        order_book=book,
    )


class _Response:
    def __init__(self, payload: object, *, status: int = 200, headers: dict[str, str] | None = None) -> None:
        self.payload = payload
        self.status = status
        self.headers = headers or {}
        self.closed = False

    def read(self) -> bytes:
        return json.dumps(self.payload).encode("utf-8")

    def close(self) -> None:
        self.closed = True


class _BuyStrategy:
    def signal(self, context: object) -> dict[str, object]:
        return {"side": "buy", "quantity": 1.0}
    def to_dict(self) -> dict[str, str]:
        return {"id": "strategy"}
class _StaticCryptoProvider:
    provider_name = "test-crypto"

    def ticker(self, symbol: str) -> CryptoTicker:
        return CryptoTicker(T0, symbol, 100.0, bid=99.0, ask=101.0)

    def order_book(self, symbol: str, *, depth: int = 20) -> OrderBookSnapshot:
        return OrderBookSnapshot(T0, (OrderBookLevel(99.0, 10.0),), (OrderBookLevel(101.0, 10.0),))
class _RecordingPredictionProvider(InMemoryPredictionProvider):
    provider_name = "recording-memory"

    def __init__(self, markets: tuple[PredictionMarketSnapshot, ...], *, failures: set[tuple[str, str]] | None = None) -> None:
        super().__init__(markets)
        self.calls: list[tuple[str, str]] = []
        self.failures = set(failures or ())

    def _call(self, operation: str, market_id: str) -> None:
        self.calls.append((operation, market_id))
        if (operation, market_id) in self.failures:
            raise OSError(f"{operation} failed for {market_id}")

    def market(self, market_id: str) -> PredictionMarketSnapshot | None:
        self._call("market", market_id)
        return super().market(market_id)

    def metadata(self, market_id: str):
        self._call("metadata", market_id)
        return super().metadata(market_id)

    def order_books(self, market_id: str, depth: int = 20):
        self._call("order_books", market_id)
        return super().order_books(market_id, depth=depth)

    def trades(self, market_id: str, start: datetime | None = None, end: datetime | None = None):
        self._call("trades", market_id)
        return super().trades(market_id, start=start, end=end)


def _seed_frozen_candidate(store: AxiomStore, candidate_id: str, market_ids: tuple[str, ...]) -> None:
    lifecycle = CandidateLifecycleManager(store)
    lifecycle.register_idea(candidate_id, {"candidate_id": candidate_id, "market_ids": list(market_ids)})
    lifecycle.advance(candidate_id, CandidateStage.SCHEMA_VALIDATED, {"schema_valid": True})
    lifecycle.advance(candidate_id, CandidateStage.BACKTESTED, {"backtest_complete": True})
    lifecycle.advance(candidate_id, CandidateStage.VALIDATED, {"validation_complete": True, "holdout_used": False})
    lifecycle.advance(candidate_id, CandidateStage.ROBUSTNESS_CHECKED, {"robustness_passed": True})
    lifecycle.advance(
        candidate_id,
        CandidateStage.FROZEN,
        {
            "frozen": True,
            "frozen_hash": f"frozen-{candidate_id}",
            "holdout_used": False,
            "strategy_hash": f"strategy-{candidate_id}",
            "model_hash": f"model-{candidate_id}",
            "config_hash": f"config-{candidate_id}",
            "risk_snapshot": {"max_position_fraction": 0.05},
        },
    )


def _seed_candidate_authority(store: AxiomStore, candidate_id: str, rank: int) -> None:
    CanaryService(store, initialize=True)
    payload = store.load_candidate_lifecycle(candidate_id)["payload"]
    frozen_hash = str(payload.get("frozen_hash", f"frozen-{candidate_id}"))
    timestamp = T0.isoformat()
    store.connection.execute(
        "INSERT OR REPLACE INTO canary_eligibility(candidate_id,eligible_at,frozen_hash,evidence_json) VALUES (?,?,?,?)",
        (candidate_id, timestamp, frozen_hash, json.dumps(payload, sort_keys=True)),
    )
    store.connection.execute(
        "INSERT OR REPLACE INTO canary_rankings("
        "candidate_id,ranking_run_id,ranking_timestamp,rank,total_score,component_scores_json,"
        "evidence_versions_json,cluster_key,cluster_representative,selected,reason"
        ") VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (
            candidate_id,
            "test-ranking",
            timestamp,
            rank,
            float(1.0 / max(rank, 1)),
            "{}",
            "{}",
            f"cluster-{candidate_id}",
            1,
            1,
            "selected for collector fixture",
        ),
    )
    store.connection.commit()


def _seed_forward_metadata(store: AxiomStore, market_ids: tuple[str, ...]) -> None:
    for market_id in market_ids:
        store.save_polymarket_market_metadata(
            market_id,
            {
                "source_type": "FORWARD_COLLECTED",
                "active": True,
                "closed": False,
                "metadata": {"category": "test"},
                "snapshot": {
                    "market_id": market_id,
                    "settlement": "open",
                    "expiry": (T0 + timedelta(days=2)).isoformat(),
                },
            },
            observed_at=T0,
            source_type="FORWARD_COLLECTED",
        )
def _timestamped_market(market_id: str, timestamp: datetime) -> PredictionMarketSnapshot:
    base = market(market_id)
    book = OrderBookSnapshot(
        timestamp,
        (OrderBookLevel(base.yes_bid or 0.49, 10.0),),
        (OrderBookLevel(base.yes_ask or 0.51, 10.0),),
        "yes",
    )
    return replace(base, timestamp=timestamp, order_book=book)







class Phase3CollectionTests(unittest.TestCase):
    def test_http_status_is_typed_and_retains_retry_after(self) -> None:
        def opener(request: Request, timeout: float) -> _Response:
            self.assertGreater(timeout, 0)
            return _Response({"error": "busy"}, status=429, headers={"Retry-After": "4"})

        with self.assertRaises(HTTPFetchError) as raised:
            fetch_json_strict("https://example.invalid/data", 1.0, opener)
        self.assertEqual(raised.exception.status, 429)
        self.assertEqual(raised.exception.retry_after, 4.0)
        self.assertTrue(raised.exception.retryable)

    def test_condition_id_payload_is_cached_for_token_and_metadata_calls(self) -> None:
        raw = {
            "conditionId": "condition-1",
            "question": "Will it happen?",
            "outcomes": ["Yes", "No"],
            "clobTokenIds": ["yes-token", "no-token"],
            "outcomePrices": ["0.4", "0.6"],
            "updatedAt": "2025-01-01T00:00:00Z",
        }

        def opener(request: Request, timeout: float) -> _Response:
            return _Response(raw)

        adapter = PolymarketAdapter(opener=opener)
        snapshot = adapter.market("condition-1")
        self.assertIsNotNone(snapshot)
        self.assertEqual(snapshot.market_id, "condition-1")
        self.assertEqual(adapter.token_ids("condition-1"), {"yes": "yes-token", "no": "no-token"})
        self.assertEqual(adapter.metadata("condition-1").market_id, "condition-1")
    def test_crossed_yes_quote_is_sanitized_before_snapshot_construction(self) -> None:
        raw = {
            "conditionId": "crossed",
            "question": "Will it happen?",
            "outcomes": ["Yes", "No"],
            "clobTokenIds": ["yes-token", "no-token"],
            "outcomePrices": ["0.5", "0.5"],
            "yesBid": "0.80",
            "yesAsk": "0.20",
            "updatedAt": "2025-01-01T00:00:00Z",
        }

        adapter = PolymarketAdapter(opener=lambda _request, timeout: _Response(raw))
        snapshot = adapter.market("crossed")

        self.assertIsNotNone(snapshot)
        self.assertIsNone(snapshot.yes_bid)
        self.assertIsNone(snapshot.yes_ask)
        self.assertIsNone(snapshot.no_bid)
        self.assertIsNone(snapshot.no_ask)


    def test_collector_retries_and_retains_bounded_cycles(self) -> None:
        base = market("m", expiry=T0 + timedelta(days=3))

        class FlakyProvider(InMemoryPredictionProvider):
            def __init__(self) -> None:
                super().__init__([base])
                self.metadata_calls = 0

            def metadata(self, market_id: str):
                self.metadata_calls += 1
                if self.metadata_calls == 1:
                    raise OSError("temporary metadata outage")
                return super().metadata(market_id)

        sleeps: list[float] = []
        provider = FlakyProvider()
        with AxiomStore(":memory:") as store:
            collector = PolymarketCollector(
                provider,
                store,
                CollectorConfig(interval_seconds=60, max_attempts=2, backoff_initial_seconds=0, jitter_seconds=0),
                clock=lambda: T0,
                sleep=sleeps.append,
            )
            first = collector.collect_once(now=T0)
            retained = collector.run_forever(cycles=3, retain_cycles=2)
            self.assertEqual(first.metadata_inserted, 1)
            self.assertGreaterEqual(first.retries, 1)
            self.assertEqual(len(retained), 2)
            self.assertTrue(all("requests" in cycle.as_record() for cycle in retained))
            self.assertTrue(sleeps)

    def test_collector_continues_incomplete_trade_pages_with_cursor(self) -> None:
        base = market("m", expiry=T0 + timedelta(days=3))

        class CursorProvider(InMemoryPredictionProvider):
            def __init__(self) -> None:
                super().__init__([base])
                self.last_trades_complete = True
                self.last_trade_cursor: str | None = None

            def trades(
                self,
                market_id: str,
                start: datetime | None = None,
                end: datetime | None = None,
                *,
                max_pages: int = 1,
                cursor: str | None = None,
            ):
                page = int(cursor or "0")
                self.last_trade_cursor = str(page + 1) if page == 0 else None
                self.last_trades_complete = page != 0
                return (TradePrint(T0, 100.0 + page, 1.0, trade_id=f"trade-{page}", market_id=market_id),)

        with AxiomStore(":memory:") as store:
            provider = CursorProvider()
            collector = PolymarketCollector(
                provider,
                store,
                CollectorConfig(interval_seconds=1, max_trade_pages=1, backoff_initial_seconds=0, jitter_seconds=0),
            )
            collector.collect_once(now=T0)
            state_key = f"{collector.config.collector_name}:m"
            self.assertEqual(store.get_collector_state(state_key)["last_trade_cursor"], "1")
            collector.collect_once(now=T0 + timedelta(seconds=1))
            self.assertIsNone(store.get_collector_state(state_key)["last_trade_cursor"])
            self.assertEqual(len(store.load_polymarket_trades("m")), 2)

    def test_candidate_paper_discovery_priority_keeps_required_markets_inside_total_cap(self) -> None:
        ids = ("candidate-a", "candidate-b", "paper-a", "discovery-a", "discovery-b")
        snapshots = tuple(market(identifier) for identifier in ids)
        provider = _RecordingPredictionProvider(snapshots)
        with AxiomStore(":memory:") as store:
            _seed_forward_metadata(store, ids)
            _seed_frozen_candidate(store, "candidate-priority", ("candidate-a", "candidate-b"))
            _seed_candidate_authority(store, "candidate-priority", 1)
            _seed_frozen_candidate(store, "paper-priority", ("paper-a",))
            CandidateLifecycleManager(store).advance(
                "paper-priority",
                CandidateStage.PAPER_FORWARD,
                {"paper_forward_started": True, "forward_test_id": "paper-priority"},
            )
            collector = PolymarketCollector(
                provider,
                store,
                CollectorConfig(
                    interval_seconds=60,
                    max_markets=4,
                    discovery_budget_per_cycle=1,
                    backoff_initial_seconds=0,
                    jitter_seconds=0,
                ),
                clock=lambda: T0,
                sleep=lambda _seconds: None,
            )
            cycle = collector.collect_once(now=T0)

            self.assertEqual(list(cycle.candidate_bound_markets), ["candidate-a", "candidate-b"])
            self.assertEqual(list(cycle.candidate_bound_scheduled), ["candidate-a", "candidate-b"])
            self.assertEqual(list(cycle.candidate_bound_fresh), ["candidate-a", "candidate-b"])
            self.assertEqual(list(cycle.candidate_bound_stale), [])
            self.assertEqual(list(cycle.candidate_bound_missing), [])
            self.assertEqual(list(cycle.paper_forward_markets), ["paper-a"])
            self.assertEqual(list(cycle.paper_forward_scheduled), ["paper-a"])
            self.assertEqual(list(cycle.discovery_scheduled), ["discovery-a"])
            self.assertEqual(list(cycle.discovery_deferred), ["discovery-b"])
            self.assertEqual(cycle.tier_attempts["candidate"], 2)  # type: ignore[index]
            self.assertEqual(cycle.tier_successes["candidate"], 2)  # type: ignore[index]
            self.assertEqual(cycle.tier_attempts["paper_forward"], 1)  # type: ignore[index]
            self.assertEqual(cycle.tier_successes["paper_forward"], 1)  # type: ignore[index]
            self.assertEqual(cycle.tier_attempts["discovery"], 1)  # type: ignore[index]
            self.assertEqual(cycle.tier_successes["discovery"], 1)  # type: ignore[index]
            self.assertEqual(cycle.markets_seen, 4)
            self.assertEqual(cycle.markets_attempted, 4)
            self.assertEqual(
                list(dict.fromkeys(item[1] for item in provider.calls if item[0] == "market")),
                ["candidate-a", "candidate-b", "paper-a", "discovery-a"],
            )
            self.assertLessEqual(
                len([item for item in provider.calls if item[0] == "market"]),
                4 * CollectorConfig().max_attempts,
            )
            self.assertFalse(any(item[0] in {"order", "submit", "sign"} for item in provider.calls))
    def test_discovery_budget_rotates_with_persisted_carry_cursor(self) -> None:
        provider = _RecordingPredictionProvider(tuple(market(f"discovery-{index}") for index in range(3)))
        with AxiomStore(":memory:") as store:
            collector = PolymarketCollector(
                provider,
                store,
                CollectorConfig(
                    interval_seconds=60,
                    max_markets=1,
                    discovery_budget_per_cycle=1,
                    max_attempts=1,
                    jitter_seconds=0,
                ),
                clock=lambda: T0,
                sleep=lambda _seconds: None,
            )
            first = collector.collect_once(now=T0)
            second = collector.collect_once(now=T0 + timedelta(seconds=60))

            self.assertEqual(list(first.discovery_scheduled), ["discovery-0"])
            self.assertEqual(list(first.discovery_deferred), ["discovery-1", "discovery-2"])
            self.assertEqual(list(second.discovery_scheduled), ["discovery-1"])
            self.assertEqual(list(second.discovery_deferred), ["discovery-2", "discovery-0"])
            state = store.get_collector_state("polymarket")
            self.assertTrue(any(key in state for key in ("discovery_carry_cursor", "discovery_cursor")))


    def test_capacity_reason_is_explicit_only_when_candidate_coverage_cannot_fit(self) -> None:
        identifiers = ("required-a", "required-b")
        provider = _RecordingPredictionProvider(tuple(market(identifier) for identifier in identifiers))
        with AxiomStore(":memory:") as store:
            _seed_forward_metadata(store, identifiers)
            _seed_frozen_candidate(store, "capacity-candidate", identifiers)
            _seed_candidate_authority(store, "capacity-candidate", 1)
            collector = PolymarketCollector(
                provider,
                store,
                CollectorConfig(
                    interval_seconds=60,
                    max_markets=1,
                    discovery_budget_per_cycle=0,
                    stale_after_seconds=30,
                    max_attempts=1,
                    jitter_seconds=0,
                ),
                clock=lambda: T0,
                sleep=lambda _seconds: None,
            )
            cycle = collector.collect_once(now=T0)
            state = store.get_collector_state("polymarket")

        self.assertEqual(cycle.capacity_reason, "COLLECTOR_CAPACITY_INSUFFICIENT")
        self.assertEqual(state["capacity_reason"], "COLLECTOR_CAPACITY_INSUFFICIENT")
        self.assertEqual(list(cycle.candidate_bound_markets), list(identifiers))
        self.assertEqual(list(cycle.candidate_bound_missing), ["required-b"])
        self.assertLess(len(cycle.candidate_bound_scheduled), len(cycle.candidate_bound_markets))
    def test_required_health_assesses_all_markets_above_default_health_cap(self) -> None:
        market_ids = tuple(f"required-{index:03d}" for index in range(150))
        candidate_ids = tuple(f"candidate-{index:03d}" for index in range(19))
        provider = _RecordingPredictionProvider(tuple(market(identifier) for identifier in market_ids))
        with AxiomStore(":memory:") as store:
            _seed_forward_metadata(store, market_ids)
            for index, candidate_id in enumerate(candidate_ids):
                start = index * 8
                _seed_frozen_candidate(store, candidate_id, market_ids[start : start + 8])
                _seed_candidate_authority(store, candidate_id, index + 1)
            collector = PolymarketCollector(
                provider,
                store,
                CollectorConfig(
                    interval_seconds=60,
                    max_markets=150,
                    discovery_budget_per_cycle=0,
                    max_attempts=1,
                    jitter_seconds=0,
                ),
                clock=lambda: T0,
                sleep=lambda _seconds: None,
            )
            cycle = collector.collect_once(now=T0)

        self.assertEqual(len(cycle.candidate_bound_markets), 150)
        self.assertEqual(len(cycle.candidate_bound_scheduled), 150)
        self.assertEqual(len(cycle.candidate_bound_fresh), 150)
        self.assertEqual(list(cycle.candidate_bound_stale), [])
        self.assertEqual(list(cycle.candidate_bound_missing), [])
        self.assertIsNone(cycle.capacity_reason)

    def test_provider_failure_isolated_to_market_and_metadata_is_not_requested_twice(self) -> None:
        snapshots = (market("ok"), market("bad"))
        provider = _RecordingPredictionProvider(snapshots, failures={("metadata", "bad")})
        with AxiomStore(":memory:") as store:
            collector = PolymarketCollector(
                provider,
                store,
                CollectorConfig(
                    interval_seconds=60,
                    market_ids=("ok", "ok", "bad"),
                    max_attempts=1,
                    backoff_initial_seconds=0,
                    jitter_seconds=0,
                ),
                clock=lambda: T0,
                sleep=lambda _seconds: None,
            )
            cycle = collector.collect_once()
            metadata_calls = [market_id for operation, market_id in provider.calls if operation == "metadata"]

            self.assertEqual(metadata_calls, ["ok", "bad"])
            self.assertEqual(cycle.markets_attempted, 2)
            self.assertGreaterEqual(cycle.markets_failed, 1)
            self.assertEqual(store.get_collector_state("polymarket:ok")["errors"], 0)
            self.assertGreater(store.get_collector_state("polymarket:bad")["errors"], 0)
            self.assertFalse(any("bad" in str(error) for error in store.list_collection_errors("ok")))
            self.assertEqual(cycle.metadata_failures, 1)
            self.assertEqual(cycle.errors, 1)
            self.assertEqual(cycle.markets_failed, 1)

    def test_raised_planned_tasks_count_once_in_sequential_and_concurrent_paths(self) -> None:
        identifiers = ("raised-a", "raised-b")

        class RaisingProvider(_RecordingPredictionProvider):
            def isolated_worker_factory(self):
                return RaisingProvider(())

        class RaisingCollector(PolymarketCollector):
            def _collect_market(self, *args, **kwargs):
                del args, kwargs
                raise RuntimeError("planned task exploded")

        for concurrency in (1, 2):
            with self.subTest(concurrency=concurrency), AxiomStore(":memory:") as store:
                collector = RaisingCollector(
                    RaisingProvider(tuple(market(identifier) for identifier in identifiers)),
                    store,
                    CollectorConfig(
                        interval_seconds=60,
                        market_ids=identifiers,
                        max_attempts=1,
                        max_concurrency=concurrency,
                        jitter_seconds=0,
                    ),
                    clock=lambda: T0,
                    sleep=lambda _seconds: None,
                )
                cycle = collector.collect_once(now=T0)

                self.assertEqual(cycle.markets_attempted, len(identifiers))
                self.assertEqual(cycle.markets_failed, len(identifiers))
                self.assertEqual(cycle.markets_successful, 0)
                self.assertEqual(cycle.errors, len(identifiers))
                self.assertEqual(cycle.tier_attempts["candidate"], len(identifiers))  # type: ignore[index]
                self.assertEqual(cycle.tier_failures["candidate"], len(identifiers))  # type: ignore[index]
                self.assertEqual(cycle.tier_successes["candidate"], 0)  # type: ignore[index]
                for identifier in identifiers:
                    self.assertEqual(len(store.list_collection_errors(identifier)), 1)

    def test_isolated_workers_are_reused_only_after_their_prior_task_finishes(self) -> None:
        class Activity:
            def __init__(self) -> None:
                self.lock = threading.Lock()
                self.created = 0
                self.active: dict[int, int] = {}
                self.max_active: dict[int, int] = {}
                self.max_total_active = 0

        activity = Activity()

        class BlockingProvider(InMemoryPredictionProvider):
            provider_name = "blocking-isolated"

            def __init__(self, *, worker_number: int) -> None:
                super().__init__([])
                self.worker_number = worker_number

            def isolated_worker_factory(self):
                with activity.lock:
                    worker_number = activity.created
                    activity.created += 1
                return BlockingProvider(worker_number=worker_number)

            def market(self, market_id: str):
                del market_id
                with activity.lock:
                    active = activity.active.get(self.worker_number, 0) + 1
                    activity.active[self.worker_number] = active
                    activity.max_active[self.worker_number] = max(
                        activity.max_active.get(self.worker_number, 0), active
                    )
                    activity.max_total_active = max(
                        activity.max_total_active, sum(activity.active.values())
                    )
                try:
                    time.sleep(0.04 if self.worker_number == 0 else 0.005)
                    return None
                finally:
                    with activity.lock:
                        activity.active[self.worker_number] -= 1

        identifiers = ("worker-a", "worker-b", "worker-c", "worker-d")
        with AxiomStore(":memory:") as store:
            collector = PolymarketCollector(
                BlockingProvider(worker_number=-1),
                store,
                CollectorConfig(
                    interval_seconds=60,
                    market_ids=identifiers,
                    max_markets=len(identifiers),
                    max_attempts=1,
                    max_concurrency=2,
                    jitter_seconds=0,
                ),
                clock=lambda: T0,
                sleep=lambda _seconds: None,
            )
            cycle = collector.collect_once(now=T0)

        self.assertEqual(activity.created, 2)
        self.assertLessEqual(activity.max_total_active, 2)
        self.assertTrue(activity.max_active)
        self.assertTrue(all(value <= 1 for value in activity.max_active.values()))
        self.assertEqual(cycle.markets_attempted, len(identifiers))
        self.assertEqual(cycle.markets_failed, len(identifiers))

    def test_retry_after_is_never_shortened_by_exponential_backoff(self) -> None:
        provider = _RecordingPredictionProvider((market("retry"),))
        provider.failures = set()
        sleeps: list[float] = []

        class RateLimitedProvider(_RecordingPredictionProvider):
            def __init__(self) -> None:
                super().__init__((market("retry"),))
                self.remaining = 1

            def metadata(self, market_id: str):
                self._call("metadata", market_id)
                if self.remaining:
                    self.remaining -= 1
                    raise HTTPFetchError("busy", url="https://example.invalid/metadata", status=429, retry_after=9.0, retryable=True)
                return super(_RecordingPredictionProvider, self).metadata(market_id)

        with AxiomStore(":memory:") as store:
            collector = PolymarketCollector(
                RateLimitedProvider(),
                store,
                CollectorConfig(
                    interval_seconds=60,
                    market_ids=("retry",),
                    max_attempts=2,
                    backoff_initial_seconds=1,
                    backoff_multiplier=2,
                    backoff_max_seconds=30,
                    jitter_seconds=0,
                ),
                clock=lambda: T0,
                sleep=sleeps.append,
            )
            collector.collect_once(now=T0)
        self.assertEqual(sleeps, [9.0])

    def test_request_provider_response_and_observed_timestamps_remain_distinct(self) -> None:
        source = T0 - timedelta(seconds=1)
        provider = _RecordingPredictionProvider((_timestamped_market("timed", source),))
        ticks = [
            T0,
            T0 + timedelta(milliseconds=10),
            T0 + timedelta(milliseconds=20),
            T0 + timedelta(milliseconds=30),
            T0 + timedelta(milliseconds=40),
            T0 + timedelta(milliseconds=50),
            T0 + timedelta(milliseconds=60),
            T0 + timedelta(milliseconds=70),
        ]
        tick_index = 0

        def clock() -> datetime:
            nonlocal tick_index
            value = ticks[min(tick_index, len(ticks) - 1)]
            tick_index += 1
            return value

        with AxiomStore(":memory:") as store:
            collector = PolymarketCollector(
                provider,
                store,
                CollectorConfig(interval_seconds=60, market_ids=("timed",), max_attempts=1, jitter_seconds=0),
                clock=clock,
                sleep=lambda _seconds: None,
            )
            cycle = collector.collect_once()
            payload = store.load_polymarket_snapshots("timed")[0]["payload"]

        self.assertEqual(payload["provider_timestamp"], source.isoformat())
        self.assertNotEqual(payload["request_started_at"], payload["response_received_at"])
        self.assertEqual(payload["observed_at"], payload["response_received_at"])
        self.assertGreater(
            datetime.fromisoformat(payload["response_received_at"]),
            datetime.fromisoformat(payload["request_started_at"]),
        )
        summary = cycle.request_latency_summary
        self.assertGreater(summary["count"], 0)  # type: ignore[index]
        for field in ("min_seconds", "max_seconds", "mean_seconds", "p50_seconds"):
            self.assertIsNotNone(summary[field])  # type: ignore[index]
    def test_missing_provider_timestamp_uses_canonical_book_timestamp(self) -> None:
        class NoTimestampProvider(_RecordingPredictionProvider):
            def provider_timestamp_for(self, market_id: str, *, kind: str) -> None:
                del market_id, kind
                return None

        source = T0 - timedelta(seconds=2)
        provider = NoTimestampProvider((_timestamped_market("no-timestamp", source),))
        with AxiomStore(":memory:") as store:
            collector = PolymarketCollector(
                provider,
                store,
                CollectorConfig(interval_seconds=60, market_ids=("no-timestamp",), max_attempts=1, jitter_seconds=0),
                clock=lambda: T0,
                sleep=lambda _seconds: None,
            )
            collector.collect_once(now=T0)
            row = store.load_polymarket_snapshots("no-timestamp")[0]
            payload = row["payload"]

        self.assertIsNone(payload["provider_timestamp"])
        self.assertEqual(payload["source_timestamp"], source.isoformat())
        self.assertEqual(row["source_timestamp"], source)
        self.assertEqual(payload["snapshot"]["timestamp"], source.isoformat())
        self.assertEqual(payload["yes_order_book"]["timestamp"], source.isoformat())
        self.assertEqual(payload["source_timestamp"], payload["yes_order_book"]["timestamp"])
        self.assertEqual(payload["response_received_at"], T0.isoformat())
        self.assertEqual(payload["observed_at"], T0.isoformat())


    def test_source_timestamp_matches_canonical_when_provider_timestamp_is_stale(self) -> None:
        canonical_stamp = T0 - timedelta(seconds=1)
        provider_stamp = T0 - timedelta(hours=1)

        class StaleTimestampProvider(_RecordingPredictionProvider):
            def provider_timestamp_for(self, market_id: str, *, kind: str) -> datetime:
                del market_id, kind
                return provider_stamp

        provider = StaleTimestampProvider((_timestamped_market("aligned", canonical_stamp),))
        with AxiomStore(":memory:") as store:
            collector = PolymarketCollector(
                provider,
                store,
                CollectorConfig(interval_seconds=60, market_ids=("aligned",), max_attempts=1, jitter_seconds=0),
                clock=lambda: T0,
                sleep=lambda _seconds: None,
            )
            collector.collect_once(now=T0)
            row = store.load_polymarket_snapshots("aligned")[0]
            payload = row["payload"]

        self.assertEqual(payload["provider_timestamp"], provider_stamp.isoformat())
        self.assertEqual(payload["source_timestamp"], canonical_stamp.isoformat())
        self.assertEqual(row["source_timestamp"], canonical_stamp)
        self.assertEqual(payload["snapshot"]["timestamp"], canonical_stamp.isoformat())

    def test_old_and_future_provider_timestamps_are_rejected_without_rewriting_prior_timestamp(self) -> None:
        old_provider = _RecordingPredictionProvider((_timestamped_market("timed", T0 - timedelta(hours=2)),))
        future_provider = _RecordingPredictionProvider((_timestamped_market("timed", T0 + timedelta(minutes=10)),))
        with AxiomStore(":memory:") as store:
            first = PolymarketCollector(
                old_provider,
                store,
                CollectorConfig(interval_seconds=60, market_ids=("timed",), max_attempts=1, jitter_seconds=0),
                clock=lambda: T0,
                sleep=lambda _seconds: None,
            )
            first.collect_once(now=T0)
            saved = store.load_polymarket_snapshots("timed")[0]["payload"]["provider_timestamp"]
            second = PolymarketCollector(
                future_provider,
                store,
                CollectorConfig(interval_seconds=60, market_ids=("timed",), max_attempts=1, jitter_seconds=0),
                clock=lambda: T0,
                sleep=lambda _seconds: None,
            )
            cycle = second.collect_once(now=T0)
            rows = store.load_polymarket_snapshots("timed")
            errors = store.list_collection_errors("timed")

        self.assertEqual(saved, (T0 - timedelta(hours=2)).isoformat())
        self.assertEqual(rows[0]["payload"]["provider_timestamp"], saved)
        self.assertGreater(cycle.errors, 0)
        self.assertTrue(any("future" in error["kind"] for error in errors))
    def test_canonical_market_and_book_future_timestamps_rejected_without_provider_timestamp(self) -> None:
        class NoTimestampProvider(_RecordingPredictionProvider):
            def provider_timestamp_for(self, market_id: str, *, kind: str) -> None:
                del market_id, kind
                return None

        future = T0 + timedelta(minutes=10)
        market_future = NoTimestampProvider((_timestamped_market("future-market", future),))
        old = _timestamped_market("future-book", T0 - timedelta(seconds=1))

        class FutureBookProvider(NoTimestampProvider):
            def order_books(self, market_id: str, depth: int = 20):
                del depth
                self._call("order_books", market_id)
                return {
                    "yes": OrderBookSnapshot(
                        future,
                        (OrderBookLevel(0.4, 10.0),),
                        (OrderBookLevel(0.6, 10.0),),
                        "yes",
                    )
                }

        book_future = FutureBookProvider((old,))
        for provider, market_id in ((market_future, "future-market"), (book_future, "future-book")):
            with self.subTest(market_id=market_id), AxiomStore(":memory:") as store:
                collector = PolymarketCollector(
                    provider,
                    store,
                    CollectorConfig(
                        interval_seconds=60,
                        market_ids=(market_id,),
                        max_attempts=1,
                        jitter_seconds=0,
                    ),
                    clock=lambda: T0,
                    sleep=lambda _seconds: None,
                )
                cycle = collector.collect_once(now=T0)
                self.assertGreater(cycle.errors, 0)
                self.assertEqual(store.load_polymarket_snapshots(market_id), [])
                self.assertTrue(store.list_collection_errors(market_id))




    def test_active_market_filter_and_health_scopes_are_distinct(self) -> None:
        with AxiomStore(":memory:") as store:
            store.save_polymarket_market_metadata(
                "open",
                {"metadata": {"closed": False}, "snapshot": {"settlement": "OPEN", "expiry": (T0 + timedelta(days=1)).isoformat()}},
                observed_at=T0,
            )
            store.save_polymarket_market_metadata(
                "closed",
                {"metadata": {"closed": True}, "snapshot": {"settlement": "OPEN"}},
                observed_at=T0,
            )
            self.assertEqual(store.tracked_polymarket_markets(now=T0), ["open"])
            self.assertEqual(store.tracked_polymarket_markets(active_only=False), ["closed", "open"])
            health = store.polymarket_health(now=T0)
            self.assertEqual(health["grade_scope"], "collector_health")
            self.assertEqual(health["evidence_maturity"]["grade_scope"], "research_evidence_maturity")

    def test_fractional_trade_counts_are_rejected(self) -> None:
        with self.assertRaises(ValueError):
            OHLCVBar(T0, 1, 2, 0.5, 1.5, 10, trades=1.5)


class Phase3PaperAndRiskTests(unittest.TestCase):
    def test_forward_engine_rejects_preregistration_and_deduplicates_after_restart(self) -> None:
        with AxiomStore(":memory:") as store:
            spec = ForwardTestRegistry(store).freeze(
                strategy=_BuyStrategy(),
                model={"id": "model"},
                start_timestamp=T0,
                allowed_markets=("m",),
            )
            pre_registration = {"market_id": "m", "timestamp": T0 - timedelta(seconds=1), "yes_mid": 0.4}
            with self.assertRaises(ValueError):
                run_forward_paper(spec, store=store, strategy=_BuyStrategy(), model={"id": "model"}, observations=[pre_registration], now=T0)
            observation = {
                "market_id": "m",
                "timestamp": T0 + timedelta(minutes=1),
                "yes_mid": 0.4,
                "yes_bid": 0.39,
                "yes_ask": 0.41,
                "expiry": T0 + timedelta(days=1),
                "settlement": "OPEN",
            }
            first = run_forward_paper(spec, store=store, strategy=_BuyStrategy(), model={"id": "model"}, observations=[observation], now=T0 + timedelta(minutes=1))
            second = run_forward_paper(spec, store=store, strategy=_BuyStrategy(), model={"id": "model"}, observations=[observation], now=T0 + timedelta(minutes=2))
            self.assertEqual(first.fills_inserted, 1)
            self.assertEqual(second.fills_inserted, 0)
            self.assertEqual(len(store.load_fills(strategy_id=spec.strategy_hash)), 1)
            state = store.load_paper_state(spec.experiment_id)
            self.assertEqual(state["state"]["fill_count"], 1)
    def test_forward_engine_processes_equal_timestamp_source_pages(self) -> None:
        with AxiomStore(":memory:") as store:
            spec = ForwardTestRegistry(store).freeze(
                strategy=_BuyStrategy(),
                model={"id": "model"},
                start_timestamp=T0,
                allowed_markets=("m",),
            )
            stamp = T0 + timedelta(minutes=1)
            first_observation = {
                "market_id": "m",
                "timestamp": stamp,
                "source_timestamp": stamp,
                "source_snapshot_id": "source-a",
                "yes_mid": 0.4,
                "yes_bid": 0.39,
                "yes_ask": 0.41,
                "settlement": "OPEN",
            }
            second_observation = {
                **first_observation,
                "source_snapshot_id": "source-b",
                "yes_mid": 0.45,
                "yes_bid": 0.44,
                "yes_ask": 0.46,
            }
            cycle = run_forward_paper(
                spec,
                store=store,
                strategy=_BuyStrategy(),
                model={"id": "model"},
                observations=[first_observation, second_observation],
                now=stamp,
            )
            self.assertEqual(cycle.observations_processed, 2)
            state = store.load_paper_state(spec.experiment_id)
            self.assertEqual(state["state"]["source_cursor_by_market"]["m"]["snapshot_id"], "source-b")
            replay = run_forward_paper(
                spec,
                store=store,
                strategy=_BuyStrategy(),
                model={"id": "model"},
                observations=[first_observation, second_observation],
                now=stamp + timedelta(minutes=1),
            )
            self.assertEqual(replay.observations_processed, 0)
            self.assertEqual(replay.observations_skipped, 2)
    def test_forward_engine_pages_persisted_equal_timestamp_snapshots(self) -> None:
        class NoopStrategy:
            def to_dict(self) -> dict[str, str]:
                return {"id": "noop"}

            def signal(self, context: object) -> None:
                return None

        with AxiomStore(":memory:") as store:
            spec = ForwardTestRegistry(store).freeze(
                strategy=NoopStrategy(),
                model={"id": "model"},
                start_timestamp=T0,
                allowed_markets=("m",),
                experiment_id="paged-forward",
            )
            stamp = T0 + timedelta(minutes=1)
            for index in range(513):
                snapshot_id = f"source-{index:03d}"
                store.save_polymarket_snapshot(
                    snapshot_id,
                    "m",
                    stamp,
                    stamp,
                    {
                        "snapshot": {
                            "market_id": "m",
                            "timestamp": stamp,
                            "yes_mid": 0.5,
                            "yes_bid": 0.49,
                            "yes_ask": 0.51,
                            "settlement": "OPEN",
                        },
                        "source_timestamp": stamp,
                        "observed_at": stamp,
                    },
                )
            first = run_forward_paper(
                spec,
                store=store,
                strategy=NoopStrategy(),
                model={"id": "model"},
                now=stamp,
            )
            second = run_forward_paper(
                spec,
                store=store,
                strategy=NoopStrategy(),
                model={"id": "model"},
                now=stamp + timedelta(minutes=1),
            )
            self.assertEqual(first.observations_processed, 512)
            self.assertEqual(second.observations_processed, 1)


    def test_explicit_historical_replay_allows_pre_registration_observations(self) -> None:
        with AxiomStore(":memory:") as store:
            spec = ForwardTestRegistry(store).freeze(
                strategy=_BuyStrategy(),
                model={"id": "model"},
                start_timestamp=T0,
                allowed_markets=("m",),
            )
            historical = {
                "market_id": "m",
                "timestamp": T0 - timedelta(seconds=1),
                "yes_mid": 0.4,
                "yes_bid": 0.39,
                "yes_ask": 0.41,
                "settlement": "OPEN",
            }
            cycle = run_historical_replay(
                spec,
                store=store,
                strategy=_BuyStrategy(),
                model={"id": "model"},
                observations=[historical],
                now=T0,
            )
            self.assertEqual(cycle.observations_processed, 1)
            self.assertIsNotNone(store.load_paper_state(historical_replay_id(spec, [historical])))

    def test_failed_paper_state_cas_restores_caller_owned_objects_in_place(self) -> None:
        with AxiomStore(":memory:") as store:
            spec = ForwardTestRegistry(store).freeze(
                strategy=_BuyStrategy(),
                model={"id": "model"},
                start_timestamp=T0,
                allowed_markets=("m",),
            )
            first = ForwardPaperEngine(spec, store=store, strategy=_BuyStrategy(), model={"id": "model"})
            external_portfolio = Portfolio(spec.bankroll)
            external_risk = RiskEngine(RiskLimits(**dict(spec.risk_limits)), initial_equity=spec.bankroll)
            second = ForwardPaperEngine(
                spec,
                store=store,
                strategy=_BuyStrategy(),
                model={"id": "model"},
                portfolio=external_portfolio,
                risk=external_risk,
            )
            observation_one = {
                "market_id": "m",
                "timestamp": T0 + timedelta(minutes=1),
                "yes_mid": 0.4,
                "yes_bid": 0.39,
                "yes_ask": 0.41,
                "settlement": "OPEN",
            }
            observation_two = {**observation_one, "timestamp": T0 + timedelta(minutes=2)}
            first.run([observation_one], now=observation_one["timestamp"])
            before_cash = external_portfolio.cash
            before_fills = list(external_portfolio.fills)
            before_positions = deepcopy(external_portfolio.positions)
            before_exposure = dict(external_risk.market_exposure)
            cycle = second.run([observation_two], now=observation_two["timestamp"])
            self.assertTrue(any("concurrently" in error for error in cycle.errors))
            self.assertEqual(external_portfolio.cash, before_cash)
            self.assertEqual(external_portfolio.fills, before_fills)
            self.assertEqual(external_portfolio.positions, before_positions)
            self.assertEqual(external_risk.market_exposure, before_exposure)
    def test_failed_paper_state_cas_restores_stateful_strategy(self) -> None:
        class StatefulStrategy(_BuyStrategy):
            def __init__(self) -> None:
                self.calls = 0

            @property
            def definition(self) -> dict[str, str]:
                return {"id": "strategy"}

            def signal(self, context: object) -> dict[str, object]:
                self.calls += 1
                return super().signal(context)

        with AxiomStore(":memory:") as store:
            spec = ForwardTestRegistry(store).freeze(
                strategy=_BuyStrategy(),
                model={"id": "model"},
                start_timestamp=T0,
                allowed_markets=("m",),
            )
            first = ForwardPaperEngine(spec, store=store, strategy=_BuyStrategy(), model={"id": "model"})
            stateful = StatefulStrategy()
            second = ForwardPaperEngine(spec, store=store, strategy=stateful, model={"id": "model"})
            observation_one = {
                "market_id": "m",
                "timestamp": T0 + timedelta(minutes=1),
                "yes_mid": 0.4,
                "yes_bid": 0.39,
                "yes_ask": 0.41,
                "settlement": "OPEN",
            }
            observation_two = {**observation_one, "timestamp": T0 + timedelta(minutes=2)}
            first.run([observation_one], now=observation_one["timestamp"])
            before_calls = stateful.calls
            cycle = second.run([observation_two], now=observation_two["timestamp"])
            self.assertTrue(any("concurrently" in error for error in cycle.errors))
            self.assertEqual(stateful.calls, before_calls)

    def test_settlement_conflict_cannot_overwrite_authoritative_result(self) -> None:
        with AxiomStore(":memory:") as store:
            spec = ForwardTestRegistry(store).freeze(
                strategy=_BuyStrategy(),
                model={"id": "model"},
                start_timestamp=T0,
                allowed_markets=("m",),
            )
            open_observation = {
                "market_id": "m",
                "timestamp": T0 + timedelta(minutes=1),
                "yes_mid": 0.4,
                "yes_bid": 0.39,
                "yes_ask": 0.41,
                "settlement": "OPEN",
            }
            resolved_yes = {
                "market_id": "m",
                "timestamp": T0 + timedelta(minutes=2),
                "settlement": "RESOLVED_YES",
            }
            resolved_no = {**resolved_yes, "timestamp": T0 + timedelta(minutes=3), "settlement": "RESOLVED_NO"}
            run_forward_paper(spec, store=store, strategy=_BuyStrategy(), model={"id": "model"}, observations=[open_observation], now=open_observation["timestamp"])
            run_forward_paper(spec, store=store, strategy=_BuyStrategy(), model={"id": "model"}, observations=[resolved_yes], now=resolved_yes["timestamp"])
            cycle = run_forward_paper(spec, store=store, strategy=_BuyStrategy(), model={"id": "model"}, observations=[resolved_no], now=resolved_no["timestamp"])
            state = store.load_paper_state(spec.experiment_id)
            self.assertEqual(state["state"]["settlement_by_market"]["m"], "RESOLVED_YES")
            self.assertTrue(any("conflicting settlement" in error for error in cycle.errors))

    def test_historical_prediction_fill_uses_point_in_time_price_not_current_quote(self) -> None:
        expiry = T0 + timedelta(days=2)
        current = market("m", settlement=SettlementState.RESOLVED_YES, expiry=expiry, yes_mid=0.9)
        provider = InMemoryPredictionProvider(
            [current],
            histories={
                "m": [
                    {"timestamp": T0 + timedelta(hours=1), "price": 0.2},
                    {"timestamp": expiry, "price": 0.3},
                ]
            },
        )
        trader = PredictionPaperTrader(provider, _BuyStrategy())
        fills = trader.run("m")
        self.assertEqual(len(fills), 1)
        self.assertAlmostEqual(fills[0].price, 0.2)

    def test_prediction_market_cap_aggregates_yes_and_no_outcomes(self) -> None:
        risk = RiskEngine(RiskLimits(max_market_exposure=0.75), initial_equity=100)
        yes = {"market_id": "m", "market_type": "prediction", "outcome": "yes", "side": "buy", "quantity": 1, "price": 0.5}
        no = {"market_id": "m", "market_type": "prediction", "outcome": "no", "side": "buy", "quantity": 1, "price": 0.5}
        self.assertTrue(risk.check_order(yes).allowed)
        risk.record_fill(type("FillLike", (), {"quantity": 1, "price": 0.5, "side": type("S", (), {})(), "market_id": "m", "symbol": "m", "strategy_id": "s", "market_type": MarketType.PREDICTION, "metadata": {}})())
        # The public check is the important boundary; a second outcome cannot bypass the market cap.
        self.assertFalse(risk.check_order(no).allowed)
    def test_group_cap_uses_gross_positions_and_allows_unwind(self) -> None:
        risk = RiskEngine(RiskLimits(max_group_exposure=1.0), initial_equity=100.0)
        risk.record_fill(Fill(T0, MarketType.CRYPTO_SPOT, "A", Side.BUY, 0.6, 1.0, 0.0, 0.0, "s", "a"), group="g")
        risk.record_fill(Fill(T0, MarketType.CRYPTO_SPOT, "B", Side.SELL, 0.6, 1.0, 0.0, 0.0, "s", "b"), group="g")
        self.assertAlmostEqual(risk.group_exposure["g"], 1.2)
        blocked = risk.check_order({"market_id": "C", "market_type": "crypto_spot", "group": "g", "side": "buy", "quantity": 0.1, "price": 1.0})
        self.assertFalse(blocked.allowed)
        unwind = risk.check_order({"market_id": "A", "market_type": "crypto_spot", "group": "g", "side": "sell", "quantity": 0.6, "price": 1.0})
        self.assertTrue(unwind.allowed)
    def test_no_outcome_does_not_fill_without_a_no_quote(self) -> None:
        class NoQuoteStrategy:
            def signal(self, context: object) -> dict[str, object]:
                return {"side": "buy_no", "quantity": 1.0, "outcome": "no"}

            def to_dict(self) -> dict[str, str]:
                return {"id": "no-quote-strategy"}

        with AxiomStore(":memory:") as store:
            strategy = NoQuoteStrategy()
            spec = ForwardTestRegistry(store).freeze(
                strategy=strategy,
                model={"id": "model"},
                start_timestamp=T0,
                allowed_markets=("m",),
            )
            observation = {
                "market_id": "m",
                "timestamp": T0 + timedelta(minutes=1),
                "yes_ask": 0.41,
                "settlement": "OPEN",
            }
            cycle = run_forward_paper(
                spec,
                store=store,
                strategy=strategy,
                model={"id": "model"},
                observations=[observation],
                now=observation["timestamp"],
            )
            self.assertEqual(cycle.observations_processed, 1)
            self.assertEqual(cycle.observations_skipped, 0)
            self.assertEqual(store.load_fills(), [])

    def test_stateful_model_rolls_back_and_restores_across_restart(self) -> None:
        class StatefulModel:
            document = {"id": "model"}

            def __init__(self) -> None:
                self.calls = 0

            def predict_probability(self, observation: object) -> float:
                self.calls += 1
                return 0.6

        observation_one = {
            "market_id": "m",
            "timestamp": T0 + timedelta(minutes=1),
            "yes_mid": 0.4,
            "yes_bid": 0.39,
            "yes_ask": 0.41,
            "settlement": "OPEN",
        }
        observation_two = {**observation_one, "timestamp": T0 + timedelta(minutes=2)}
        with AxiomStore(":memory:") as store:
            first_model = StatefulModel()
            spec = ForwardTestRegistry(store).freeze(
                strategy=_BuyStrategy(),
                model=first_model.document,
                start_timestamp=T0,
                allowed_markets=("m",),
            )
            first = ForwardPaperEngine(spec, store=store, strategy=_BuyStrategy(), model=first_model)
            first.run([observation_one], now=observation_one["timestamp"])
            self.assertEqual(first_model.calls, 1)
            restored_model = StatefulModel()
            second = ForwardPaperEngine(spec, store=store, strategy=_BuyStrategy(), model=restored_model)
            self.assertEqual(restored_model.calls, 1)
            second.run([observation_two], now=observation_two["timestamp"])
            self.assertEqual(restored_model.calls, 2)

    def test_stateful_model_error_isolated_to_one_observation(self) -> None:
        class BrokenModel:
            document = {"id": "model"}

            def predict_probability(self, observation: object) -> float:
                raise RuntimeError("model unavailable")

        with AxiomStore(":memory:") as store:
            spec = ForwardTestRegistry(store).freeze(
                strategy=_BuyStrategy(),
                model=BrokenModel.document,
                start_timestamp=T0,
                allowed_markets=("m",),
            )
            observation = {
                "market_id": "m",
                "timestamp": T0 + timedelta(minutes=1),
                "yes_mid": 0.4,
                "yes_bid": 0.39,
                "yes_ask": 0.41,
                "settlement": "OPEN",
            }
            cycle = ForwardPaperEngine(spec, store=store, strategy=_BuyStrategy(), model=BrokenModel()).run(
                [observation],
                now=observation["timestamp"],
            )
            self.assertEqual(cycle.observations_skipped, 1)
            self.assertTrue(any("model error" in error for error in cycle.errors))


class Phase3LifecycleBusTests(unittest.TestCase):
    def test_lifecycle_requires_ordered_stages_and_persists_rejection(self) -> None:
        criteria = PromotionCriteria(
            min_independent_samples=0,
            min_trades=0,
            min_stability=0,
            min_calibration=0,
            min_regimes=0,
            min_forward_duration_seconds=0,
        )
        with AxiomStore(":memory:") as store:
            manager = CandidateLifecycleManager(store, criteria=criteria)
            manager.register_idea("candidate-1")
            with self.assertRaises(ValueError):
                manager.advance("candidate-1", CandidateStage.BACKTESTED, {"backtest_complete": True})
            manager.advance("candidate-1", CandidateStage.SCHEMA_VALIDATED, {"schema_valid": True})
            manager.advance("candidate-1", CandidateStage.BACKTESTED, {"backtest_complete": True})
            with self.assertRaises(ValueError):
                manager.advance("candidate-1", CandidateStage.VALIDATED, {"validation_complete": True, "holdout_used": True})
            manager.reject("candidate-1", "holdout was used for tuning")
            record = manager.get("candidate-1")
            self.assertEqual(record.stage, CandidateStage.REJECTED)
            self.assertEqual(record.rejection_reason, "holdout was used for tuning")
            self.assertGreaterEqual(len(manager.events("candidate-1")), 2)

    def test_mutations_are_deterministic_lineaged_and_budgeted(self) -> None:
        strategy = validate_strategy({"version": 1, "market_type": "crypto_spot", "family": "momentum", "parameters": {"lookback": 10, "threshold": 0.02}})
        first = DeterministicMutationEngine(seed=7).mutate(strategy, parent_id="root", generation=1, max_variants=3)
        second = DeterministicMutationEngine(seed=7).mutate(strategy, parent_id="root", generation=1, max_variants=3)
        self.assertEqual([item.candidate_id for item in first], [item.candidate_id for item in second])
        self.assertTrue(all(item.lineage == ("root",) for item in first))
        budget = ExperimentBudget(total_limit=2, per_family_limit=2)
        with AxiomStore(":memory:") as store:
            generated = DeterministicMutationEngine(store=store, budget=budget).mutate(strategy, parent_id="root", generation=1, max_variants=8)
            self.assertEqual(len(generated), 2)
            self.assertEqual(store.load_experiment_budget()["budget"]["used_total"], 2)
            self.assertEqual(len(DeterministicMutationEngine(store=store).mutate(strategy, parent_id="root", generation=2, max_variants=1)), 0)

    def test_durable_bus_deduplicates_leases_and_denies_private_mutation(self) -> None:
        with AxiomStore(":memory:") as store:
            bus = DurableResearchBus(store)
            payload = {"proposal_id": "p1", "statement": "prices contain a testable effect", "source": "public paper", "tests": ["walk forward"]}
            first = bus.submit_hypothesis(payload, dedupe_key="p1", available_at=T0)
            duplicate = bus.submit_hypothesis(payload, dedupe_key="p1", available_at=T0)
            self.assertEqual(first.item_id, duplicate.item_id)
            claimed = bus.claim("worker", lease_seconds=1, now=T0)
            self.assertEqual(claimed.status, ResearchQueueStatus.TESTING)
            self.assertEqual(bus.resume_expired(now=T0 + timedelta(seconds=2)), 1)
            claimed_again = bus.claim("worker-2", now=T0 + timedelta(seconds=2))
            self.assertEqual(claimed_again.attempts, 2)
            completed = bus.complete(
                claimed_again.item_id,
                status=ResearchQueueStatus.COMPLETED,
                worker="worker-2",
                result={"result": "paper-only"},
                now=T0 + timedelta(seconds=2),
            )
            self.assertEqual(completed.status, ResearchQueueStatus.COMPLETED)
            self.assertEqual(completed.as_record()["payload"], payload)
            self.assertEqual(completed.result, {"result": "paper-only"})
            with self.assertRaises(ResearchBusPermissionError):
                bus.submit_hypothesis({"statement": "bad", "source": "x", "tests": [], "risk": "override"})
            for forbidden_key in ("apiKey", "privateKey", "accessToken", "client-secret", "api.key", "private/key"):
                with self.subTest(forbidden_key=forbidden_key):
                    with self.assertRaises(ResearchBusPermissionError):
                        bus.submit_hypothesis({"statement": "bad", "source": "x", "tests": [], forbidden_key: "override"})
            for path, smuggled in (
                ("statement", "Use private_key=0xdeadbeef to sign the request."),
                ("source", "api-key: abc123"),
                ("tests", ["cookie: session=abc123"]),
                ("evidence", {"instructions": "Polymarket account credentials are available here."}),
            ):
                with self.subTest(path=path):
                    unsafe = {"statement": "bad", "source": "x", "tests": []}
                    unsafe[path] = smuggled
                    with self.assertRaises(ResearchBusPermissionError):
                        bus.submit_hypothesis(unsafe)

            accepted = bus.submit_hypothesis(
                {
                    "statement": "Public market token identifiers are used for historical research.",
                    "source": "public Binance and Polymarket market data",
                    "tests": ["match market_token_id to token_id"],
                    "token_id": "public-token-123",
                    "market_token_id": "market-token-123",
                },
                dedupe_key="public-token-identifiers",
            )
            self.assertEqual(accepted.payload["token_id"], "public-token-123")
            self.assertEqual(accepted.payload["market_token_id"], "market-token-123")

    def test_director_summary_and_proposal_validation_are_bounded(self) -> None:
        with AxiomStore(":memory:") as store:
            summary = research_summary(store)
            self.assertFalse(summary["live_execution"])
            accepted = validate_hermes_proposal({
                "statement": "test",
                "source": "paper",
                "tests": ["one"],
                "dataset_version": "public-v1",
                "time_split": "train-validation-holdout",
                "paper_only": True,
            })
            rejected = validate_hermes_proposal({
                "statement": "test",
                "source": "paper",
                "tests": ["one"],
                "dataset_version": "public-v1",
                "time_split": "train-validation-holdout",
                "paper_only": True,
                "account": "x",
            })
            self.assertTrue(accepted.accepted)
            self.assertFalse(rejected.accepted)


class Phase3NodeDashboardTests(unittest.TestCase):
    def test_node_lock_shutdown_logging_and_persisted_status(self) -> None:
        provider = InMemoryPredictionProvider([market("m", expiry=T0 + timedelta(days=1))])
        with tempfile.TemporaryDirectory() as directory:
            db = str(Path(directory) / "node.sqlite")
            log = str(Path(directory) / "node.log")
            with AxiomStore(db) as store:
                submitted = DurableResearchBus(store).submit_hypothesis(
                    {"statement": "public-data hypothesis", "source": "test", "tests": ["forward"]},
                    dedupe_key="node-hypothesis",
                    available_at=T0,
                )
                node = ResearchNode(
                    NodeConfig(db, log_path=log, interval_seconds=1, max_markets=1),
                    provider=provider,
                    store=store,
                )
                cycles = node.run(max_cycles=1)
                status = node.status()
                self.assertEqual(len(cycles), 1)
                self.assertEqual(status["status"], "idle")
                self.assertFalse(status["lock_exists"])
                self.assertTrue(Path(log).exists())
                self.assertEqual(store.list_worker_states()[0]["status"], "idle")
                queued = DurableResearchBus(store).get(submitted.item_id)
                self.assertIsNotNone(queued)
                self.assertEqual(queued.status, ResearchQueueStatus.REJECTED)
    def test_crypto_node_deduplicates_ticker_observations_before_execution(self) -> None:
        with AxiomStore(":memory:") as store:
            node = ResearchNode(
                NodeConfig(":memory:", interval_seconds=1, crypto_enabled=True),
                provider=InMemoryPredictionProvider([]),
                crypto_provider=_StaticCryptoProvider(),
                store=store,
            )
            node._crypto_trader.strategy = _BuyStrategy()
            node._run_crypto_paper()
            node._run_crypto_paper()
            self.assertEqual(len(store.load_fills()), 1)
            self.assertEqual(len(store.list_paper_observations(node._crypto_experiment_id, limit=None)), 1)
            self.assertEqual(len(node._crypto_trader.fills), 1)
            self.assertEqual(node._crypto_status.get("deduplicated"), 1)
            restarted = ResearchNode(
                NodeConfig(":memory:", interval_seconds=1, crypto_enabled=True),
                provider=InMemoryPredictionProvider([]),
                crypto_provider=_StaticCryptoProvider(),
                store=store,
            )
            self.assertEqual(restarted._crypto_status["observations"], 1)
            self.assertEqual(restarted._crypto_status["fills"], 1)
    def test_node_paper_worker_accepts_canonical_strategy_document_hash(self) -> None:
        strategy_document = {
            "version": 1,
            "market_type": "prediction",
            "family": "probability_mispricing",
            "parameters": {},
            "probability_model": "market",
            "resolution_aware": True,
            "resolution_inputs": ["expiry"],
        }
        strategy_definition = validate_strategy(strategy_document)
        model_document = {"yes_probability": 0.6}
        config = {
            "execution": "paper_only",
            "live_execution": False,
            "strategy_document": strategy_definition.to_dict(),
            "model_document": model_document,
        }
        with tempfile.TemporaryDirectory() as directory:
            db = str(Path(directory) / "canonical.sqlite")
            with AxiomStore(db) as store:
                spec = ForwardTestRegistry(store).freeze(
                    strategy=strategy_document,
                    model=model_document,
                    config=config,
                    start_timestamp=T0,
                    allowed_markets=("m",),
                    experiment_id="canonical-worker",
                )
                node = ResearchNode(
                    NodeConfig(db, max_markets=1, crypto_enabled=False),
                    provider=InMemoryPredictionProvider([market("m")]),
                    store=store,
                )
                node._run_paper_workers()
                worker = next(row for row in store.list_worker_states() if row["worker_name"] == f"paper:{spec.experiment_id}")
                self.assertEqual(worker["status"], "idle")
                self.assertNotIn("frozen forward-test hashes", str(worker["payload"]))



    def test_dashboard_facade_exposes_phase3_fields(self) -> None:
        from axiom.dashboard import DashboardData, _dashboard_html

        with AxiomStore(":memory:") as store:
            data = DashboardData(store=store)
            for endpoint in ("research-summary", "paper", "opportunities", "queue", "status", "evidence-maturity"):
                self.assertIsNotNone(data.snapshot(endpoint))
            store.save_worker_state(
                "health-monitor",
                "idle",
                {"grade": "D", "paper_only": True, "live_execution": False},
                started_at=T0,
                heartbeat_at=T0,
            )
            self.assertEqual(data.status_data()["status"], "degraded")
            html = _dashboard_html()
            self.assertIn("Research maturity", html)
            self.assertIn("Paper forward", html)
            self.assertIn("Research queue and node status", html)


class CandidateForwardAuthorityTests(unittest.TestCase):
    @staticmethod
    def _metadata(
        store: AxiomStore,
        market_id: str,
        *,
        category: str = "politics",
        observed_at: datetime = T0,
        closed: bool = False,
    ) -> None:
        store.save_polymarket_market_metadata(
            market_id,
            {
                "source_type": "FORWARD_COLLECTED",
                "active": not closed,
                "closed": closed,
                "metadata": {"category": category},
                "snapshot": {
                    "market_id": market_id,
                    "settlement": "resolved_yes" if closed else "open",
                    "expiry": (observed_at + timedelta(days=1)).isoformat(),
                },
            },
            observed_at=observed_at,
            source_type="FORWARD_COLLECTED",
        )

    @staticmethod
    def _historical_catalog(store: AxiomStore) -> None:
        store.save_dataset_catalog(
            "Polymarket-historical",
            "aggregate-v1",
            provider="fixture",
            instrument="POLYMARKET",
            market_type="prediction",
            timeframe="event",
            start_timestamp=T0 - timedelta(days=2),
            end_timestamp=T0 - timedelta(days=1),
            row_count=1,
            completeness=1.0,
            quality="PRICE_PROXY",
            source_type="HISTORICAL",
            snapshot_id="Polymarket-historical:aggregate-v1",
            metadata={
                "source_type": "HISTORICAL",
                "market_versions": [
                    {
                        "market_id": "historical-constituent",
                        "dataset_id": "prediction:historical-constituent",
                        "version": "constituent-v1",
                        "records": 1,
                    }
                ],
            },
        )

    @staticmethod
    def _freeze_candidate(
        store: AxiomStore,
        candidate_id: str,
        payload: dict[str, object],
    ) -> None:
        lifecycle = CandidateLifecycleManager(store)
        lifecycle.register_idea(candidate_id, {"candidate_id": candidate_id, **payload})
        lifecycle.advance(candidate_id, CandidateStage.SCHEMA_VALIDATED, {"schema_valid": True})
        lifecycle.advance(candidate_id, CandidateStage.BACKTESTED, {"backtest_complete": True})
        lifecycle.advance(
            candidate_id,
            CandidateStage.VALIDATED,
            {"validation_complete": True, "holdout_used": False},
        )
        lifecycle.advance(
            candidate_id,
            CandidateStage.ROBUSTNESS_CHECKED,
            {"robustness_passed": True},
        )
        lifecycle.advance(
            candidate_id,
            CandidateStage.FROZEN,
            {
                "frozen": True,
                "strategy_hash": "strategy-hash",
                "model_hash": "model-hash",
                "config_hash": "config-hash",
                "risk_snapshot": {"max_position_fraction": 0.05},
            },
        )

    def test_historical_constituents_are_not_executable_and_exact_current_target_survives(self) -> None:
        with AxiomStore(":memory:") as store:
            self._historical_catalog(store)
            self._metadata(store, "current-exact")
            self._freeze_candidate(
                store,
                "candidate-exact",
                {
                    "dataset_provenance": {
                        "dataset_id": "Polymarket-historical",
                        "dataset_version": "aggregate-v1",
                        "source_type": "HISTORICAL",
                        "market_versions": [
                            {
                                "market_id": "historical-constituent",
                                "version": "constituent-v1",
                                "records": 1,
                            }
                        ],
                    },
                    "market_ids": ["historical-constituent", "current-exact"],
                },
            )

            requirements = store.candidate_forward_requirements(
                candidate_ids=["candidate-exact"],
                now=T0,
            )

        self.assertEqual(requirements["market_ids"], ["current-exact"])
        self.assertEqual(requirements["candidate_bound_markets"], {"candidate-exact": ["current-exact"]})
        candidate = requirements["candidates"][0]
        self.assertEqual(candidate["candidate_id"], "candidate-exact")
        self.assertEqual(candidate["market_ids"], ["current-exact"])
        self.assertEqual(candidate["historical_market_ids_ignored"], ["historical-constituent"])
        self.assertEqual(candidate["reason_code"], "CANDIDATE_FORWARD_MARKET_RESOLVED")
        self.assertEqual(candidate["permitted_market_ids"], ["current-exact"])
        self.assertEqual(candidate["resolution"], "RESOLVED")

    def test_frozen_filters_match_only_open_forward_markets_and_remain_bounded(self) -> None:
        with AxiomStore(":memory:") as store:
            for market_id in ("politics-a", "politics-b", "politics-c"):
                self._metadata(store, market_id, category="politics")
            self._metadata(store, "politics-closed", category="politics", closed=True)
            self._metadata(store, "economics-open", category="economics")
            self._freeze_candidate(
                store,
                "candidate-filter",
                {"frozen_filters": {"category": "PoLiTiCs"}},
            )

            requirements = store.candidate_forward_requirements(
                candidate_ids=["candidate-filter"],
                now=T0,
                max_markets_per_candidate=2,
                max_total_markets=2,
            )

        candidate = requirements["candidates"][0]
        self.assertEqual(candidate["normalized_frozen_filters"], {"category": "politics"})
        self.assertEqual(candidate["market_ids"], ["politics-a", "politics-b"])
        self.assertEqual(requirements["market_ids"], ["politics-a", "politics-b"])
        self.assertNotIn("politics-closed", requirements["market_ids"])
        self.assertNotIn("economics-open", requirements["market_ids"])
        self.assertLessEqual(len(requirements["market_ids"]), 2)
        self.assertEqual(candidate["reason_code"], "CANDIDATE_FORWARD_MARKET_RESOLVED")
        self.assertEqual(candidate["permitted_market_ids"], ["politics-a", "politics-b"])
        self.assertEqual(candidate["resolution"], "RESOLVED")

    def test_empty_or_unsupported_authority_is_unresolved_without_tracked_fallback(self) -> None:
        with AxiomStore(":memory:") as store:
            self._metadata(store, "tracked-but-unbound")
            self._freeze_candidate(store, "candidate-empty", {})
            self._freeze_candidate(
                store,
                "candidate-unsupported",
                {"frozen_filters": {"unsupported_selector": "value"}},
            )

            requirements = store.candidate_forward_requirements(
                candidate_ids=["candidate-empty", "candidate-unsupported"],
                now=T0,
            )

        self.assertEqual(
            requirements["candidate_bound_markets"],
            {"candidate-empty": [], "candidate-unsupported": []},
        )
        self.assertEqual(requirements["unresolved_candidates"], ["candidate-empty", "candidate-unsupported"])
        self.assertEqual(requirements["closed_candidates"], [])
        for candidate in requirements["candidates"]:
            self.assertEqual(candidate["market_ids"], [])
            self.assertEqual(candidate["resolution"], "UNRESOLVED")
            self.assertEqual(candidate["reason_code"], "CANDIDATE_FORWARD_MARKET_UNRESOLVED")

    def test_exact_target_beyond_tracked_inventory_page_is_loaded_directly(self) -> None:
        target = "market-1000"
        with AxiomStore(":memory:") as store:
            for index in range(1001):
                self._metadata(store, f"market-{index:04d}")
            _seed_frozen_candidate(store, "candidate-beyond-page", (target,))

            requirements = store.candidate_forward_requirements(
                candidate_ids=["candidate-beyond-page"],
                now=T0,
            )

        candidate = requirements["candidates"][0]
        self.assertEqual(requirements["market_ids"], [target])
        self.assertEqual(candidate["market_ids"], [target])
        self.assertEqual(candidate["resolution"], "RESOLVED")
        self.assertEqual(candidate["reason_code"], "CANDIDATE_FORWARD_MARKET_RESOLVED")
    def test_terminal_exact_target_is_closed_not_executable(self) -> None:
        with AxiomStore(":memory:") as store:
            self._metadata(store, "terminal-target", closed=True)
            self._freeze_candidate(
                store,
                "candidate-terminal",
                {"market_ids": ["terminal-target"]},
            )

            requirements = store.candidate_forward_requirements(
                candidate_ids=["candidate-terminal"],
                now=T0,
            )

        candidate = requirements["candidates"][0]
        self.assertEqual(candidate["market_ids"], [])
        self.assertEqual(candidate["resolution"], "CLOSED")
        self.assertEqual(candidate["reason_code"], "CANDIDATE_MARKET_CLOSED")
        self.assertEqual(requirements["closed_candidates"], ["candidate-terminal"])

    def test_authority_and_required_health_are_read_only(self) -> None:
        with AxiomStore(":memory:") as store:
            self._metadata(store, "current-exact")
            self._freeze_candidate(
                store,
                "candidate-read-only",
                {"market_ids": ["current-exact"]},
            )
            before = store.connection.total_changes
            requirements = store.candidate_forward_requirements(
                candidate_ids=["candidate-read-only"],
                now=T0,
            )
            health = store.polymarket_required_health(
                requirements=requirements,
                scheduled_market_ids=["current-exact"],
                now=T0,
                stale_after_seconds=60,
            )
            after = store.connection.total_changes

        self.assertEqual(before, after)
        self.assertEqual(health["candidate_bound_markets"], ["current-exact"])
if __name__ == "__main__":
    unittest.main()
