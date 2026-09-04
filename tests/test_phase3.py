from __future__ import annotations
from copy import deepcopy

from datetime import datetime, timedelta, timezone
import io
import json
from pathlib import Path
import tempfile
import unittest
from urllib.request import Request

from axiom.collector import CollectorConfig, PolymarketCollector
from axiom.data import InMemoryPredictionProvider, PolymarketAdapter
from axiom.data._http import HTTPFetchError, fetch_json_strict
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
                self.assertEqual(queued.status, ResearchQueueStatus.PENDING)
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


if __name__ == "__main__":
    unittest.main()
