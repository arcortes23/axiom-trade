from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from urllib.parse import parse_qs, urlparse
from urllib.request import urlopen

from axiom.backtest import CryptoBacktester, PredictionMarketBacktester
from axiom.data import BinanceAdapter, InMemoryCryptoProvider, MarketDataPipeline, SyntheticCryptoProvider, SyntheticPredictionProvider
from axiom.dashboard import DashboardData, DashboardServer
from axiom.domain import (
    Fill,
    MarketType,
    OHLCVBar,
    OrderBookLevel,
    OrderBookSnapshot,
    ResolvedContract,
    SettlementState,
    Side,
)
from axiom.evaluation import evaluate_scores, split_dataset, walk_forward_splits
from axiom.evolution import EvolutionEngine
from axiom.storage import AxiomStore
from axiom.research import run_crypto_research, run_prediction_research
from axiom.probability import BaseRateModel, BetaBelief, CryptoPriceTargetModel, ProbabilityModelRegistry
from axiom.hermes import CandidateMessage, Hermes, HermesPermissions, HermesValidationError
from axiom.metrics import (
    brier_score,
    calibration_buckets,
    calculate_prediction_metrics,
    conditional_value_at_risk,
    expected_calibration_error,
    expected_value,
    log_loss,
)
from axiom.paper import CryptoPaperTrader, LiveExecutionDisabled, PaperTradingConfig
from axiom.portfolio import OrderRequest, Portfolio
from axiom.regime import RegimeEngine, RegimeState
from axiom.risk import RiskEngine, RiskLimits, fractional_kelly_size
from axiom.strategy import StrategyValidationError, validate_strategy


UTC = timezone.utc
T0 = datetime(2024, 1, 1, tzinfo=UTC)


def crypto_strategy(family: str = "momentum", **parameters: object) -> dict[str, object]:
    return {
        "version": 1,
        "market_type": "crypto_spot",
        "family": family,
        "parameters": parameters or {"lookback": 1, "threshold": 0.01},
        "strategy_id": "test-crypto",
    }


def prediction_strategy(family: str = "probability_mispricing", **parameters: object) -> dict[str, object]:
    return {
        "version": 1,
        "market_type": "prediction",
        "family": family,
        "parameters": parameters or {"threshold": 0.05},
        "probability_model": "deterministic-test-v1",
        "resolution_aware": True,
        "resolution_inputs": ["expiry", "resolution_criteria"],
        "strategy_id": "test-prediction",
    }


def fill(
    symbol: str,
    side: Side,
    quantity: float,
    price: float,
    *,
    market_type: MarketType = MarketType.CRYPTO_SPOT,
    order_id: str = "fill-1",
    strategy_id: str = "s",
    market_id: str | None = None,
    metadata: dict[str, object] | None = None,
) -> Fill:
    return Fill(
        timestamp=T0,
        market_type=market_type,
        symbol=symbol,
        side=side,
        quantity=quantity,
        price=price,
        fees=0.0,
        slippage=0.0,
        strategy_id=strategy_id,
        order_id=order_id,
        market_id=market_id,
        metadata=metadata or {},
    )


class StrategyAndAccountingTests(unittest.TestCase):
    def test_strategy_dsl_is_versioned_and_rejects_code(self) -> None:
        strategy = validate_strategy(crypto_strategy())
        self.assertEqual(strategy.version, 1)
        self.assertEqual(validate_strategy(json.loads(strategy.to_json())), strategy)
        with self.assertRaises(StrategyValidationError):
            validate_strategy({**crypto_strategy(), "parameters": {"callback": lambda _: 1}})
        with self.assertRaises(StrategyValidationError):
            validate_strategy({**crypto_strategy(), "operations": [{"op": "eval", "value": "buy()"}]})
        with self.assertRaises(StrategyValidationError):
            validate_strategy({"version": 1, "market_type": "prediction", "family": "tails"})

    def test_crypto_accounting_fees_slippage_and_partial_fill(self) -> None:
        portfolio = Portfolio(1_000.0)
        request = OrderRequest("BTCUSDT", Side.BUY, 2.0, strategy_id="s")
        first = portfolio.execute_order(
            request,
            timestamp=T0,
            price=100.0,
            fee_bps=100.0,
            slippage_bps=100.0,
            available_quantity=1.0,
            order_id="partial",
        )
        self.assertIsNotNone(first)
        assert first is not None
        self.assertAlmostEqual(first.quantity, 1.0)
        self.assertAlmostEqual(first.price, 101.0)
        self.assertAlmostEqual(first.fees, 1.01)
        self.assertAlmostEqual(first.slippage, 1.0)
        self.assertEqual(portfolio.orders["partial"].status, "partially_filled")
        self.assertAlmostEqual(portfolio.orders["partial"].remaining_quantity, 1.0)
        second = portfolio.execute_order(request, timestamp=T0, price=110.0, fee_bps=100.0, order_id="sell-buy")
        self.assertIsNotNone(second)
        self.assertEqual(len(portfolio.fills), 2)
        self.assertLess(portfolio.cash, 1_000.0)
        sell = portfolio.execute_order(
            OrderRequest("BTCUSDT", Side.SELL, 1.0, strategy_id="s"),
            timestamp=T0 + timedelta(days=1),
            price=120.0,
            fee_bps=100.0,
            order_id="sell",
        )
        self.assertIsNotNone(sell)
        self.assertGreater(portfolio.realized_pnl(), 0.0)

    def test_binary_contract_pays_winner_once(self) -> None:
        portfolio = Portfolio(100.0)
        request = OrderRequest(
            "market-1",
            Side.BUY,
            2.0,
            MarketType.PREDICTION,
            strategy_id="binary",
            market_id="market-1",
            outcome="yes",
        )
        portfolio.execute_order(request, timestamp=T0, price=0.40, order_id="yes-buy")
        self.assertAlmostEqual(portfolio.cash, 99.20)
        payout = portfolio.resolve(
            ResolvedContract("market-1", SettlementState.RESOLVED_YES, T0 + timedelta(days=1), "exact rule")
        )
        self.assertAlmostEqual(payout, 2.0)
        self.assertAlmostEqual(portfolio.cash, 101.20)
        self.assertEqual(portfolio.resolve(ResolvedContract("market-1", SettlementState.RESOLVED_YES, T0, "exact rule")), 0.0)
        self.assertEqual(portfolio.get_position("market-1", outcome="yes").quantity, 0.0)  # type: ignore[union-attr]

    def test_portfolio_guards_cash_shorts_and_partial_order_reuse(self) -> None:
        portfolio = Portfolio(10.0)
        request = OrderRequest("BTCUSDT", Side.BUY, 2.0)
        first = portfolio.execute_order(request, timestamp=T0, price=4.0, available_quantity=1.0, order_id="partial")
        self.assertIsNotNone(first)
        second = portfolio.execute_order(request, timestamp=T0, price=4.0, order_id="partial")
        self.assertIsNotNone(second)
        self.assertEqual(portfolio.orders["partial"].status, "filled")
        self.assertAlmostEqual(portfolio.get_position("BTCUSDT").quantity, 2.0)  # type: ignore[union-attr]
        with self.assertRaises(ValueError):
            portfolio.apply_fill(fill("BTCUSDT", Side.SELL, 3.0, 4.0))
        with self.assertRaises(ValueError):
            Portfolio(1.0).apply_fill(fill("BTCUSDT", Side.BUY, 2.0, 1.0))

    def test_order_book_is_sorted_and_walks_displayed_depth(self) -> None:
        book = OrderBookSnapshot(
            T0,
            bids=(OrderBookLevel(0.49, 2.0), OrderBookLevel(0.50, 1.0)),
            asks=(OrderBookLevel(0.53, 2.0), OrderBookLevel(0.52, 1.0)),
        )
        self.assertEqual(book.best_bid, 0.50)
        self.assertEqual(book.best_ask, 0.52)
        price, filled = book.executable_price(Side.BUY, 2.0)
        self.assertAlmostEqual(price, (0.52 + 0.53) / 2.0)
        self.assertEqual(filled, 2.0)
        with self.assertRaises(ValueError):
            OrderBookSnapshot(T0, (OrderBookLevel(0.60, 1.0),), (OrderBookLevel(0.50, 1.0),))


class PredictionAndMetricsTests(unittest.TestCase):
    def test_expected_value_uses_executable_costs(self) -> None:
        result = expected_value(0.52, 0.38, executable_price=0.40, fees=0.01, slippage=0.005)
        self.assertAlmostEqual(result["raw_ev"], 0.14)
        self.assertAlmostEqual(result["executable_ev"], 0.105)
        self.assertAlmostEqual(result["raw_edge"], 0.14)
        self.assertAlmostEqual(result["executable_edge"], 0.105)
    def test_probability_models_are_reproducible_and_versioned(self) -> None:
        model = CryptoPriceTargetModel()
        first = model.estimate(
            current_price=100.0,
            target_price=110.0,
            time_to_expiry_seconds=30 * 86_400,
            realized_volatility=0.50,
            sample_size=100,
        )
        second = model.estimate(
            current_price=100.0,
            target_price=110.0,
            time_to_expiry_seconds=30 * 86_400,
            realized_volatility=0.50,
            sample_size=100,
        )
        self.assertEqual(first, second)
        self.assertLess(first.lower, first.probability)
        base = BaseRateModel().estimate(successes=6, trials=10)
        self.assertEqual(base.model_version, "historical-base-rate-beta-v1")
        registry = ProbabilityModelRegistry({model.version: model})
        self.assertAlmostEqual(registry.estimate(model.version, current_price=100.0, target_price=100.0, time_to_expiry_seconds=1.0, realized_volatility=0.2).probability, 0.5)

    def test_beta_belief_updates_are_versioned_and_uncertainty_aware(self) -> None:
        prior = BetaBelief()
        posterior = prior.observe(True).observe(False, weight=2.0)
        self.assertEqual(prior.mean, 0.5)
        self.assertLess(posterior.mean, prior.mean)
        estimate = posterior.estimate()
        self.assertEqual(estimate.model_version, "beta-belief-v1")
        self.assertLessEqual(estimate.lower, estimate.probability)
        self.assertLessEqual(estimate.probability, estimate.upper)
        with self.assertRaises(ValueError):
            prior.observe(0.5)

    def test_calibration_buckets_brier_logloss_and_ece(self) -> None:
        observations = [(0.7, 1), (0.7, 0), (0.2, 0), (0.2, 0)]
        self.assertAlmostEqual(brier_score(observations), (0.09 + 0.49 + 0.04 + 0.04) / 4)
        self.assertGreater(log_loss(observations), 0.0)
        buckets = calibration_buckets(observations)
        self.assertEqual(len(buckets), 10)
        self.assertEqual(buckets[7]["count"], 2.0)
        self.assertGreater(expected_calibration_error(observations), 0.0)
        metrics = calculate_prediction_metrics([100.0, 110.0], probabilities=observations, initial_equity=100.0)
        self.assertAlmostEqual(metrics["roi"], 0.1)
        self.assertIn("brier", metrics)

    def test_prediction_backtest_respects_resolution_timestamp(self) -> None:
        rows = [
            {"timestamp": T0, "market_id": "m", "yes_mid": 0.20, "yes_ask": 0.21, "no_ask": 0.81, "model_probability": 0.80, "settlement": "open", "liquidity": 100.0},
            {"timestamp": T0 + timedelta(days=1), "market_id": "m", "yes_mid": 0.25, "yes_ask": 0.26, "no_ask": 0.76, "model_probability": 0.80, "settlement": "open", "liquidity": 100.0},
            {"timestamp": T0 + timedelta(days=2), "market_id": "m", "yes_mid": 0.30, "yes_ask": 0.31, "no_ask": 0.71, "model_probability": 0.80, "settlement": "resolved_yes", "liquidity": 100.0},
        ]
        result = PredictionMarketBacktester(initial_cash=100.0, allocation=0.25, fee_bps=0.0, slippage_bps=0.0).run(
            rows,
            prediction_strategy(),
        )
        self.assertTrue(result.fills)
        self.assertTrue(all(item.timestamp < T0 + timedelta(days=2) for item in result.fills))
        self.assertEqual(result.unresolved, ())
        self.assertEqual(result.outcomes, {})


    def test_prediction_edges_preserve_reference_and_complement_no_probability(self) -> None:
        prediction_fill = Fill(
            timestamp=T0,
            market_type=MarketType.PREDICTION,
            symbol="market-1",
            side=Side.BUY,
            quantity=1.0,
            price=0.45,
            fees=0.01,
            slippage=0.05,
            strategy_id="s",
            order_id="prediction-1",
            market_id="market-1",
            expected_probability=0.60,
            executable_probability=0.45,
            metadata={"reference_price": 0.40},
        )
        metrics = calculate_prediction_metrics([100.0, 100.0], fills=[prediction_fill], initial_equity=100.0)
        self.assertAlmostEqual(metrics["raw_edge"], 0.20)
        self.assertAlmostEqual(metrics["executable_edge"], 0.14)

        rows = [
            {"timestamp": T0, "market_id": "m", "yes_mid": 0.80, "yes_ask": 0.81, "yes_bid": 0.79, "no_ask": 0.21, "model_probability": 0.20, "settlement": "open"},
            {"timestamp": T0 + timedelta(days=1), "market_id": "m", "yes_mid": 0.80, "yes_ask": 0.81, "yes_bid": 0.79, "no_ask": 0.21, "model_probability": 0.20, "settlement": "open"},
        ]
        result = PredictionMarketBacktester(fee_bps=0.0, slippage_bps=0.0).run(
            rows,
            prediction_strategy(),
            resolutions={"m": ResolvedContract("m", SettlementState.RESOLVED_NO, T0 + timedelta(days=1), "rule")},
        )
        self.assertTrue(result.fills)
        self.assertEqual(result.fills[0].metadata["outcome"], "no")
        self.assertAlmostEqual(result.fills[0].expected_probability, 0.80)
        self.assertEqual(result.outcomes["m"], "resolved_no")

class RiskRegimeAndEvolutionTests(unittest.TestCase):
    def test_risk_limits_kill_switch_and_correlated_group(self) -> None:
        risk = RiskEngine(RiskLimits(max_account_exposure=100.0, max_group_exposure=100.0), initial_equity=1_000.0)
        order = OrderRequest("BTCUSDT", Side.BUY, 1.0, strategy_id="s")
        self.assertTrue(risk.check_order(order, price=60.0, group="btc-direction").allowed)
        risk.record_fill(fill("BTCUSDT", Side.BUY, 1.0, 60.0, metadata={"group": "btc-direction"}))
        self.assertFalse(risk.check_order(order, price=50.0, group="btc-direction").allowed)
        risk.emergency_stop()
        self.assertFalse(risk.check_order(order, price=1.0).allowed)
        self.assertIn("emergency_kill_switch", risk.check_order(order, price=1.0).reasons)
        mapped = RiskEngine(
            RiskLimits(max_group_exposure=100.0),
            initial_equity=1_000.0,
            correlation_groups={"BTC>100": "btc-direction", "BTC>105": "btc-direction"},
        )
        mapped.record_fill(fill("BTC>100", Side.BUY, 1.0, 60.0))
        self.assertEqual(mapped.correlated_exposure["btc-direction"], 60.0)
        self.assertFalse(mapped.check_order(OrderRequest("BTC>105", Side.BUY, 1.0, strategy_id="s"), price=50.0).allowed)
        capped = RiskEngine(RiskLimits(max_expected_loss=5.0), initial_equity=1_000.0)
        self.assertFalse(capped.check_order(order, price=10.0).allowed)
        self.assertFalse(capped.check_order(order, price=10.0, expected_loss=6.0).allowed)
        self.assertTrue(capped.check_order(order, price=10.0, expected_loss=5.0).allowed)
    def test_prediction_sizing_is_fractional_and_uncertainty_adjusted(self) -> None:
        certain = fractional_kelly_size(0.60, 0.40, 1_000.0, fraction=0.25, confidence=1.0, uncertainty=0.0, max_fraction=0.20)
        uncertain = fractional_kelly_size(0.60, 0.40, 1_000.0, fraction=0.25, confidence=1.0, uncertainty=1.0, max_fraction=0.20)
        self.assertGreater(certain, uncertain)
        self.assertLessEqual(certain * 0.40, 200.0)
        self.assertEqual(fractional_kelly_size(0.30, 0.40, 1_000.0), 0.0)

    def test_cvar_is_a_tail_loss_metric_and_risk_gate(self) -> None:
        self.assertAlmostEqual(conditional_value_at_risk([-0.20, -0.10, 0.05], alpha=2.0 / 3.0), 0.20)
        with self.assertRaises(ValueError):
            conditional_value_at_risk([-0.1], alpha=1.0)
        risk = RiskEngine(RiskLimits(max_cvar=0.10), initial_equity=100.0)
        self.assertFalse(risk.check_order(OrderRequest("BTCUSDT", Side.BUY, 1.0), price=10.0).allowed)
        self.assertIn("cvar_required", risk.status()["reasons"])
        self.assertFalse(risk.update_cvar([-0.20, -0.10, 0.05], alpha=2.0 / 3.0).allowed)
        self.assertIn("max_cvar", risk.status()["reasons"])

    def test_overlapping_regimes(self) -> None:
        bars = [
            {"close": 100.0, "volume": 1.0},
            {"close": 130.0, "volume": 2.0},
            {"close": 129.0, "volume": 2.0},
            {"close": 114.0, "volume": 3.0},
        ]
        crypto = RegimeEngine(trend_threshold=0.03, volatility_threshold=0.04, crash_threshold=0.10).detect_crypto(bars)
        self.assertIn(RegimeState.TRENDING, crypto)
        self.assertIn(RegimeState.HIGH_VOLATILITY, crypto)
        self.assertIn(RegimeState.CRASH, crypto)
        prediction = RegimeEngine(near_expiry_seconds=3600.0, trend_threshold=0.05).detect_prediction([
            {"timestamp": T0, "yes_mid": 0.20, "model_probability": 0.80, "liquidity": 0.0, "time_to_expiry_seconds": 100.0},
            {"timestamp": T0 + timedelta(hours=1), "yes_mid": 0.40, "model_probability": 0.80, "liquidity": 0.0, "time_to_expiry_seconds": 100.0},
        ])
        self.assertIn(RegimeState.NEAR_EXPIRY, prediction)
        self.assertIn(RegimeState.ILLIQUID, prediction)
        self.assertIn(RegimeState.HIGH_MOMENTUM, prediction)
        self.assertIn(RegimeState.HIGH_DISAGREEMENT, prediction)

    def test_evolution_mutation_hybrid_and_locked_holdout(self) -> None:
        engine = EvolutionEngine(population_size=4, seed=7, holdout_fraction=0.25)
        seed = validate_strategy(crypto_strategy())
        population = engine.initialize([seed])
        mutated = engine.mutate(population[0], generation=1)
        self.assertIn(population[0].candidate_id, mutated.lineage)
        hybrid = engine.hybrid(population[0], mutated, generation=1)
        self.assertEqual(hybrid.strategy.market_type, MarketType.CRYPTO_SPOT)
        result = engine.evolve([{"bars": [{"close": 100.0}, {"close": 102.0}]}] * 8, outcomes=[1.0] * 8)
        self.assertEqual(result.generation, 1)
        self.assertEqual(len(result.holdout), 1)
        self.assertIsNotNone(result.holdout[0].holdout_score)


class DataEvaluationAndProductTests(unittest.TestCase):
    def test_dataset_split_walk_forward_and_immutable_versions(self) -> None:
        rows = [{"timestamp": T0 + timedelta(days=i), "value": i, "dataset_version": "v1"} for i in range(8)]
        split = split_dataset(rows, T0 + timedelta(days=3), T0 + timedelta(days=5), T0 + timedelta(days=8), require_nonempty=True)
        self.assertEqual((len(split.train), len(split.validation), len(split.holdout)), (3, 2, 3))
        self.assertEqual(split.dataset_version, "v1")
        with self.assertRaises(ValueError):
            split_dataset([*rows[:-1], {"timestamp": T0, "dataset_version": "v2"}], T0 + timedelta(days=3), T0 + timedelta(days=5))
        windows = walk_forward_splits(rows, 3, 2, 2, step=1)
        self.assertEqual(len(windows), 2)
        evaluation = evaluate_scores({"fitness": 1.0}, {"fitness": 0.5}, {"fitness": 0.4})
        self.assertLess(evaluation.fitness, evaluation.holdout_score)

    def test_storage_roundtrip_and_dataset_version_immutability(self) -> None:
        bar = OHLCVBar(T0, 100.0, 102.0, 99.0, 101.0, 10.0)
        with AxiomStore(":memory:") as store:
            store.save_dataset("btc", "v1", [bar], quality="LOW")
            with self.assertRaises(ValueError):
                store.save_dataset("btc", "v1", [bar], quality="LOW")
            self.assertEqual(store.load_dataset_record("btc", "v1")["quality"], "LOW")  # type: ignore[index]
            store.save_bars("BTCUSDT", [bar], dataset_id="btc", dataset_version="v1")
            self.assertEqual(store.load_bars("BTCUSDT")[0], bar)
            strategy = validate_strategy(crypto_strategy())
            store.save_strategy("test-crypto", strategy, version="1")
            self.assertEqual(store.load_strategy("test-crypto", "1")["family"], "momentum")  # type: ignore[index]
            stored_fill = fill("BTCUSDT", Side.BUY, 1.0, 100.0, order_id="unique")
            store.save_fill(stored_fill)
            second_fill = fill("BTCUSDT", Side.BUY, 0.5, 100.0, order_id="unique")
            store.save_fill(second_fill, fill_id="unique-part-2")
            self.assertEqual(store.load_fill("unique"), stored_fill)
            self.assertEqual(len(store.load_fills(order_id="unique")), 2)
    def test_storage_versions_and_transaction_rollback_are_atomic(self) -> None:
        first = OHLCVBar(T0, 100.0, 101.0, 99.0, 100.5, 10.0)
        second = OHLCVBar(T0 + timedelta(days=1), 100.5, 103.0, 100.0, 102.0, 12.0)
        with AxiomStore(":memory:") as store:
            store.save_dataset("btc", "v1", [first], quality="MEDIUM")
            store.save_bars("BTCUSDT", [first], dataset_id="btc", dataset_version="v1")
            store.save_dataset("btc", "v2", [first, second], quality="HIGH")
            store.save_bars("BTCUSDT", [first, second], dataset_id="btc", dataset_version="v2")
            self.assertEqual(len(store.load_bars("BTCUSDT", dataset_id="btc", dataset_version="v1")), 1)
            self.assertEqual(len(store.load_bars("BTCUSDT", dataset_id="btc", dataset_version="v2")), 2)
            with self.assertRaises(RuntimeError):
                with store.transaction():
                    store.save_dataset("btc", "v3", [first, second], quality="LOW")
                    store.save_bars("BTCUSDT", [first, second], dataset_id="btc", dataset_version="v3")
                    raise RuntimeError("rollback")
            self.assertIsNone(store.load_dataset("btc", "v3"))
            self.assertEqual(store.load_bars("BTCUSDT", dataset_id="btc", dataset_version="v3"), [])

    def test_binance_adapter_paginates_explicit_historical_ranges(self) -> None:
        calls: list[str] = []
        start_ms = int(T0.timestamp() * 1000)
        first_page = [
            [start_ms + index * 60_000, "100", "101", "99", "100.5", "10", start_ms, "10", 1]
            for index in range(1000)
        ]
        second_page = [[start_ms + 1000 * 60_000, "100.5", "101.5", "100", "101", "11", start_ms, "11", 1]]

        class Response:
            def __init__(self, payload: object) -> None:
                self.payload = json.dumps(payload).encode("utf-8")

            def read(self) -> bytes:
                return self.payload

            def close(self) -> None:
                return

        def opener(request: object, timeout: float) -> Response:
            url = str(getattr(request, "full_url"))
            calls.append(url)
            return Response(first_page if len(calls) == 1 else second_page)

        class PaginationAdapter(BinanceAdapter):
            def ticker(self, symbol: str):
                return None

        bars = PaginationAdapter(opener=opener).historical_ohlcv(
            "BTC/USDT",
            start=T0,
            end=T0 + timedelta(days=1),
            interval="1m",
        )
        self.assertEqual(len(bars), 1001)
        self.assertEqual(len(calls), 2)
        self.assertEqual(parse_qs(urlparse(calls[1]).query)["startTime"], [str(start_ms + 999 * 60_000 + 1)])

    def test_storage_migrates_legacy_market_tables_without_losing_rows(self) -> None:
        connection = sqlite3.connect(":memory:")
        connection.executescript(
            """
            CREATE TABLE bars (
                symbol TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                PRIMARY KEY (symbol, timestamp)
            );
            CREATE TABLE snapshots (
                key TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                kind TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                PRIMARY KEY (key, timestamp, kind)
            );
            INSERT INTO bars(symbol, timestamp, payload_json) VALUES (
                'BTCUSDT', '2024-01-01T00:00:00+00:00',
                '{"timestamp":"2024-01-01T00:00:00+00:00","open":100,"high":101,"low":99,"close":100.5,"volume":10}'
            );
            """
        )
        store = AxiomStore(connection=connection)
        try:
            columns = {row["name"] for row in connection.execute("PRAGMA table_info(bars)")}
            self.assertTrue({"dataset_id", "dataset_version", "created_at"} <= columns)
            self.assertEqual(store.load_bars("BTCUSDT")[0].close, 100.5)
        finally:
            store.close()
    def test_data_pipeline_and_offline_research_are_explicitly_low_quality(self) -> None:
        with AxiomStore(":memory:") as store:
            pipeline = MarketDataPipeline(store)
            crypto_report = pipeline.ingest_crypto(SyntheticCryptoProvider(periods=8), "BTC/USDT", version="synthetic-v1")
            self.assertEqual(crypto_report.records, 8)
            self.assertEqual(len(store.load_bars("BTC/USDT")), 8)
            crypto_dataset = store.load_dataset_record("crypto:BTCUSDT", "synthetic-v1")
            self.assertTrue(crypto_dataset["metadata"]["instrument_metadata_available"])  # type: ignore[index]
            prediction_report = pipeline.ingest_prediction(SyntheticPredictionProvider(), ["synthetic-event-1"])[0]
            self.assertEqual(prediction_report.quality.value, "LOW")
            self.assertTrue(store.load_prediction_snapshots("synthetic-event-1"))
            crypto_research = run_crypto_research(SyntheticCryptoProvider(periods=30))
            self.assertEqual(len(crypto_research["experiments"]), 8)
            prediction_research = run_prediction_research(SyntheticPredictionProvider(), market_limit=1)
            self.assertEqual(prediction_research["independent_resolved_markets"], 0)

    def test_paper_trader_reuses_portfolio_risk_and_disables_live(self) -> None:
        class AlwaysBuy:
            def signal(self, _context: object) -> dict[str, object]:
                return {"side": "buy", "quantity": 0.1}

        provider = SyntheticCryptoProvider(periods=3)
        portfolio = Portfolio(10_000.0)
        risk = RiskEngine(RiskLimits(max_order_notional=10_000.0), initial_equity=10_000.0)
        trader = CryptoPaperTrader(
            provider,
            AlwaysBuy(),
            risk,
            portfolio,
            config=PaperTradingConfig(fee_rate=0.001, slippage_bps=10.0),
        )
        observed = trader.run_once("BTCUSDT")
        self.assertIsNotNone(observed)
        self.assertEqual(len(portfolio.fills), 1)
        self.assertGreater(risk.market_exposure["BTCUSDT"], 0.0)
        self.assertGreater(observed.slippage, 0.0)  # type: ignore[union-attr]
        with self.assertRaises(LiveExecutionDisabled):
            CryptoPaperTrader(provider, AlwaysBuy(), config=PaperTradingConfig(live=True))

    def test_dashboard_serves_read_only_json(self) -> None:
        server = DashboardServer(port=0, data=DashboardData(data={"overview": {"status": "ok", "experiment_count": 2}}))
        server.start()
        try:
            assert server.url is not None
            with urlopen(server.url + "/api/overview", timeout=2) as response:
                payload = json.loads(response.read().decode("utf-8"))
            self.assertEqual(payload["experiment_count"], 2)
            self.assertEqual(response.status, 200)
        finally:
            server.stop()

    def test_hermes_is_schema_only(self) -> None:
        bus = Hermes()
        candidate = CandidateMessage("c1", "s1", "1.0.0", {"version": 1, "family": "momentum"})
        bus.intake_candidate(candidate)
        self.assertEqual(len(bus.messages), 1)
        with self.assertRaises(HermesValidationError):
            CandidateMessage("c2", "s2", "1.0.0", {"risk": {"max_loss": 1}})
        with self.assertRaises(HermesValidationError):
            Hermes(HermesPermissions(execute_orders=True))


if __name__ == "__main__":
    unittest.main()
