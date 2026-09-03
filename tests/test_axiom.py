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
from axiom.benchmarks import crypto_benchmarks, prediction_benchmarks
from axiom.collector import CollectorConfig, PolymarketCollector
from axiom.dashboard import DashboardData, DashboardServer
from axiom.data import BinanceAdapter, InMemoryCryptoProvider, InMemoryPredictionProvider, MarketDataPipeline, PolymarketAdapter, SyntheticCryptoProvider, SyntheticPredictionProvider
from axiom.domain import (
    Fill,
    InstrumentMetadata,
    MarketType,
    OHLCVBar,
    OrderBookLevel,
    OrderBookSnapshot,
    OrderType,
    PredictionMarketSnapshot,
    ResolvedContract,
    ResearchQuality,
    SettlementState,
    Side,
    TradePrint,
)
from axiom.evaluation import evaluate_scores, split_dataset, walk_forward_splits
from axiom.evolution import EvolutionEngine
from axiom.forward import ForwardTestRegistry
from axiom.storage import AxiomStore
from axiom.research import run_crypto_research, run_prediction_research
from axiom.probability import BaseRateModel, BetaBelief, CryptoPriceTargetModel, ProbabilityModelRegistry
from axiom.hermes import CandidateMessage, Hermes, HermesPermissions, HermesValidationError
from axiom.metrics import (
    brier_score,
    calculate_crypto_metrics,
    calibration_at_horizons,
    calibration_buckets,
    calculate_prediction_metrics,
    conditional_value_at_risk,
    expected_calibration_error,
    expected_value,
    log_loss,
)
from axiom.risk import RiskEngine, RiskLimits, fractional_kelly_size
from axiom.regime import RegimeEngine, RegimeState
from axiom.robustness import bootstrap_confidence_interval, minimum_sample_check, multiple_testing_summary, neighboring_parameter_stability
from axiom.opportunity import scan_opportunities
from axiom.paper import CryptoPaperTrader, LiveExecutionDisabled, PaperTradingConfig, PredictionPaperTrader
from axiom.portfolio import OrderRequest, Portfolio
from axiom.strategy import StrategyValidationError, evaluate_signal, validate_strategy


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

    def test_metric_lots_do_not_cross_prediction_outcomes(self) -> None:
        fills = (
            fill("m", Side.BUY, 1.0, 0.90, market_type=MarketType.PREDICTION, order_id="no-buy", metadata={"outcome": "no"}),
            fill("m", Side.BUY, 1.0, 0.10, market_type=MarketType.PREDICTION, order_id="yes-buy", metadata={"outcome": "yes"}),
            fill("m", Side.SELL, 1.0, 0.20, market_type=MarketType.PREDICTION, order_id="yes-sell", metadata={"outcome": "yes"}),
        )
        metrics = calculate_crypto_metrics([{"equity": 1.0}], fills=fills, initial_equity=1.0)
        self.assertEqual(metrics["closed_trades"], 1.0)
        self.assertAlmostEqual(metrics["expectancy"], 0.10)


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
        with self.assertRaises(ValueError):
            log_loss(observations, epsilon=0.0)
        with self.assertRaises(ValueError):
            calibration_buckets(observations, bins=2.5)  # type: ignore[arg-type]
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
        self.assertEqual(result.outcomes, {"m": "resolved_yes"})


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
            self.assertEqual(crypto_research["experiments"][0]["evaluation"]["attribution"]["selection_basis"], "validation_only")
            self.assertIn("holdout_score_report_only", crypto_research["experiments"][0]["evaluation"]["attribution"])
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


class PhaseTwoQualityTests(unittest.TestCase):
    def test_limit_order_stops_at_adverse_depth_and_requires_limit(self) -> None:
        book = OrderBookSnapshot(
            T0,
            bids=(OrderBookLevel(0.20, 10.0),),
            asks=(OrderBookLevel(0.40, 1.0), OrderBookLevel(0.60, 10.0)),
        )
        portfolio = Portfolio(10.0)
        with self.assertRaises(ValueError):
            OrderRequest("m", Side.BUY, 1.0, MarketType.PREDICTION, order_type=OrderType.LIMIT)
        fill_result = portfolio.execute_order(
            OrderRequest(
                "m",
                Side.BUY,
                2.0,
                MarketType.PREDICTION,
                order_type=OrderType.LIMIT,
                limit_price=0.50,
                market_id="m",
                outcome="yes",
            ),
            timestamp=T0,
            order_book=book,
            order_id="limited",
        )
        self.assertIsNotNone(fill_result)
        assert fill_result is not None
        self.assertAlmostEqual(fill_result.quantity, 1.0)
        self.assertAlmostEqual(fill_result.price, 0.40)
        self.assertEqual(portfolio.orders["limited"].status, "partially_filled")

    def test_prediction_outcome_risk_exposure_does_not_net_yes_and_no(self) -> None:
        risk = RiskEngine(RiskLimits(max_account_exposure=0.75), initial_equity=1.0)
        risk.record_fill(
            fill(
                "m",
                Side.BUY,
                1.0,
                0.40,
                market_type=MarketType.PREDICTION,
                market_id="m",
                metadata={"outcome": "yes"},
            )
        )
        risk.record_fill(
            fill(
                "m",
                Side.BUY,
                1.0,
                0.40,
                market_type=MarketType.PREDICTION,
                market_id="m",
                order_id="no",
                metadata={"outcome": "no"},
            )
        )
        self.assertAlmostEqual(risk.account_exposure, 0.80)
        decision = risk.check_order(
            OrderRequest(
                "m",
                Side.BUY,
                1.0,
                MarketType.PREDICTION,
                market_id="m",
                outcome="yes",
            ),
            price=0.40,
        )
        self.assertFalse(decision.allowed)
    def test_prediction_group_and_strategy_caps_do_not_net_outcomes(self) -> None:
        risk = RiskEngine(
            RiskLimits(max_group_exposure=0.50, max_strategy_exposure=0.50),
            initial_equity=1.0,
        )
        yes = OrderRequest("m", Side.BUY, 1.0, MarketType.PREDICTION, strategy_id="s", market_id="m", outcome="yes")
        no = OrderRequest("m", Side.BUY, 1.0, MarketType.PREDICTION, strategy_id="s", market_id="m", outcome="no")
        self.assertTrue(risk.check_order(yes, price=0.40, group="event").allowed)
        risk.record_fill(fill("m", Side.BUY, 1.0, 0.40, market_type=MarketType.PREDICTION, market_id="m", strategy_id="s", metadata={"outcome": "yes", "group": "event"}))
        decision = risk.check_order(no, price=0.40, group="event")
        self.assertFalse(decision.allowed)
        self.assertIn("max_group_exposure", decision.reasons)
        self.assertIn("max_strategy_exposure", decision.reasons)


    def test_backtests_do_not_use_future_rows_and_accept_string_timestamps(self) -> None:
        rows = [
            {
                "timestamp": (T0 + timedelta(days=index)).isoformat(),
                "market_id": "m",
                "yes_mid": 0.20,
                "yes_ask": 0.21,
                "yes_bid": 0.19,
                "model_probability": 0.80,
                "settlement": "open",
            }
            for index in range(3)
        ]
        first = PredictionMarketBacktester(fee_bps=0.0, slippage_bps=0.0).run(rows, prediction_strategy())
        changed = [dict(row) for row in rows]
        changed[-1]["yes_ask"] = 0.90
        changed[-1]["model_probability"] = 0.01
        second = PredictionMarketBacktester(fee_bps=0.0, slippage_bps=0.0).run(changed, prediction_strategy())
        self.assertTrue(first.fills and second.fills)
        self.assertEqual(first.fills[0].timestamp, second.fills[0].timestamp)
        self.assertAlmostEqual(first.fills[0].price, second.fills[0].price)
        bars = [
            OHLCVBar(T0, 100.0, 101.0, 99.0, 100.0, 10.0),
            OHLCVBar(T0 + timedelta(days=1), 102.0, 103.0, 101.0, 102.0, 10.0),
            OHLCVBar(T0 + timedelta(days=2), 104.0, 105.0, 103.0, 104.0, 10.0),
        ]
        crypto = CryptoBacktester(fee_bps=0.0, slippage_bps=0.0, allocation=0.5).run(bars, crypto_strategy("momentum", lookback=1, threshold=0.01))
        self.assertTrue(crypto.equity_curve)

        strategy = prediction_strategy("probability_mispricing", threshold=0.1)
        current_snapshot = dict(rows[0])
        current_snapshot.pop("model_probability")
        current_snapshot["yes_mid"] = 0.5
        current_only = {
            "snapshots": [current_snapshot],
            "probabilities": [0.2, 0.9],
        }
        self.assertLess(evaluate_signal(strategy, current_only), 0.0)
    def test_multi_horizon_calibration_and_robustness_reject_invalid_inputs(self) -> None:
        rows = [
            {"market_id": "m", "timestamp": T0, "expiry": T0 + timedelta(days=30), "probability": 0.8, "outcome": 1},
            {"market_id": "m", "timestamp": T0 + timedelta(days=2), "expiry": T0 + timedelta(days=30), "probability": 0.6, "outcome": 1},
            {"market_id": "m", "timestamp": T0 + timedelta(days=29), "expiry": T0 + timedelta(days=30), "probability": 0.5, "outcome": 1},
        ]
        calibration = calibration_at_horizons(rows, horizons={"1d": timedelta(days=1), "7d": timedelta(days=7)})
        self.assertEqual(calibration["1d"]["count"], 1)
        self.assertAlmostEqual(calibration["1d"]["brier"], 0.25)
        self.assertEqual(calibration["7d"]["count"], 1)
        self.assertTrue(minimum_sample_check(2, min_observations=3)["passed"] is False)
        multiple = multiple_testing_summary({"good": 0.01, "bad": "not-a-p", "edge": 0.20})
        self.assertEqual(multiple["invalid_values"], 1)
        interval = bootstrap_confidence_interval([1.0, 4.0], statistic="median", resamples=8, seed=2)
        self.assertAlmostEqual(interval["estimate"], 2.5)
        with self.assertRaises(ValueError):
            bootstrap_confidence_interval([], statistic="unsupported")

    def test_resolved_prediction_research_reports_fixed_horizons(self) -> None:
        market = PredictionMarketSnapshot(
            T0,
            "resolved",
            "Question",
            yes_bid=0.40,
            yes_ask=0.60,
            yes_mid=0.50,
            expiry=T0 + timedelta(days=30),
            settlement=SettlementState.RESOLVED_YES,
        )
        provider = InMemoryPredictionProvider(
            markets=[market],
            histories={
                "resolved": [
                    {"timestamp": T0 + timedelta(days=29), "price": 0.80},
                    {"timestamp": T0 + timedelta(days=1), "price": 0.30},
                    {"timestamp": T0 + timedelta(days=20), "price": 0.60},
                ]
            },
        )
        report = run_prediction_research(provider, market_limit=1)
        self.assertEqual(report["independent_resolved_markets"], 1)
        self.assertEqual(report["research_quality"], ResearchQuality.PRICE_PROXY.value)
        self.assertEqual(report["multi_horizon_calibration"]["1d"]["count"], 1)
        self.assertEqual(report["multi_horizon_calibration"]["7d"]["count"], 1)
        self.assertEqual(report["multi_horizon_calibration"]["30d"]["count"], 0)

    def test_benchmarks_and_research_quality_labels_are_explicit(self) -> None:
        bars = [
            OHLCVBar(T0 + timedelta(days=index), 100.0 + index, 101.0 + index, 99.0 + index, 100.5 + index, 10.0)
            for index in range(4)
        ]
        benchmark_names = {item.name for item in crypto_benchmarks(bars)}
        self.assertEqual(benchmark_names, {"cash", "buy_hold", "dca"})
        prediction = prediction_benchmarks(
            [
                {"yes_mid": 0.4, "probability": 0.7, "outcome": 1},
                {"yes_mid": 0.6, "probability": 0.3, "outcome": 0},
            ]
        )
        self.assertEqual({item.name for item in prediction}, {"market_mid", "constant_0.5", "model"})
        book = OrderBookSnapshot(T0, (OrderBookLevel(0.20, 2.0),), (OrderBookLevel(0.30, 2.0),))
        market_rows = [
            {
                "timestamp": T0,
                "market_id": "m",
                "yes_mid": 0.25,
                "yes_bid": 0.20,
                "yes_ask": 0.30,
                "model_probability": 0.80,
                "order_book": book,
                "settlement": "open",
            },
            {
                "timestamp": T0 + timedelta(days=1),
                "market_id": "m",
                "yes_mid": 0.25,
                "yes_bid": 0.20,
                "yes_ask": 0.30,
                "model_probability": 0.80,
                "order_book": book,
                "settlement": "resolved_yes",
            },
        ]
        result = PredictionMarketBacktester(fee_bps=0.0, slippage_bps=0.0).run(market_rows, prediction_strategy())
        self.assertEqual(len(result.quality_labels), 2)
        self.assertEqual(result.research_quality, ResearchQuality.ORDER_BOOK_SIMULATED)

    def test_collector_is_resumable_immutable_and_health_aware(self) -> None:
        class FakePredictionProvider:
            provider_name = "fake-polymarket"

            def markets(self, active: bool = True):
                return [self.market("m")]

            def market(self, market_id: str):
                return PredictionMarketSnapshot(
                    T0,
                    "m",
                    "Will event happen?",
                    yes_bid=0.40,
                    yes_ask=0.50,
                    yes_mid=0.45,
                    no_bid=0.50,
                    no_ask=0.60,
                    no_mid=0.55,
                    expiry=T0 + timedelta(days=2),
                    resolution_criteria="official result",
                    yes_token_id="yes-token",
                    no_token_id="no-token",
                )

            def price_history(self, market_id: str, start=None, end=None):
                return ()

            def order_book(self, market_id: str, depth: int = 20):
                return self.order_books(market_id, depth)["yes"]

            def order_books(self, market_id: str, depth: int = 20):
                return {
                    "yes": OrderBookSnapshot(T0, (OrderBookLevel(0.40, 10.0),), (OrderBookLevel(0.50, 10.0),), "yes-token"),
                    "no": OrderBookSnapshot(T0, (OrderBookLevel(0.50, 10.0),), (OrderBookLevel(0.60, 10.0),), "no-token"),
                }

            def metadata(self, market_id: str):
                return InstrumentMetadata(
                    "m",
                    MarketType.PREDICTION,
                    provider="fake-polymarket",
                    market_id="m",
                    question="Will event happen?",
                    resolution_criteria="official result",
                    expiry=T0 + timedelta(days=2),
                )

            def trades(self, market_id: str, start=None, end=None):
                return (TradePrint(T0, 0.50, 2.0, Side.BUY, trade_id="trade-1", market_id="m", token_id="yes-token"),)

        with AxiomStore(":memory:") as store:
            collector = PolymarketCollector(
                FakePredictionProvider(),
                store,
                CollectorConfig(interval_seconds=60.0),
                clock=lambda: T0,
                sleep=lambda _seconds: None,
            )
            first = collector.collect_once(now=T0)
            second = collector.collect_once(now=T0 + timedelta(seconds=60))
            self.assertEqual((first.snapshots_inserted, second.snapshots_inserted, second.snapshot_duplicates), (1, 1, 0))
            self.assertEqual(second.trades_inserted, 0)
            self.assertEqual(second.trade_duplicates, 1)
            self.assertEqual(len(store.load_polymarket_trades("m")), 1)
            self.assertEqual(store.polymarket_health(now=T0 + timedelta(seconds=60))["metadata_records"], 1)
            stored_snapshot = store.load_polymarket_snapshots("m")[0]
            self.assertEqual(stored_snapshot["quality"], ResearchQuality.ORDER_BOOK_SIMULATED.value)
            self.assertAlmostEqual(stored_snapshot["payload"]["quotes"]["yes_spread"], 0.10)
            self.assertEqual(stored_snapshot["payload"]["depth"]["yes"]["ask_levels"], 1)
            self.assertTrue(store.save_polymarket_market_metadata("m", {"rules": "changed"}, observed_at=T0))
            self.assertEqual(store.polymarket_health(now=T0 + timedelta(seconds=60))["metadata_records"], 2)

    def test_forward_registry_and_scanner_are_paper_only_and_frozen(self) -> None:
        config = {"nested": {"value": 1}}
        registry = ForwardTestRegistry()
        spec = registry.freeze(
            strategy={"id": "s", "version": 1},
            model={"id": "m", "version": 1},
            config=config,
            start_timestamp=T0,
            allowed_markets=("m",),
        )
        config["nested"]["value"] = 9
        self.assertEqual(spec.config["nested"]["value"], 1)
        with self.assertRaises(TypeError):
            spec.config["nested"]["value"] = 2  # type: ignore[index]
        with self.assertRaises(ValueError):
            registry.freeze(
                strategy={"id": "s", "version": 1},
                model={"id": "m", "version": 1},
                config={"nested": {"value": 2}},
                start_timestamp=T0,
                allowed_markets=("m",),
                experiment_id=spec.experiment_id,
            )
        with AxiomStore(":memory:") as store:
            persisted = ForwardTestRegistry(store)
            persisted_spec = persisted.freeze(
                strategy={"id": "s", "version": 1},
                model={"id": "m", "version": 1},
                config={"nested": {"value": 1}},
                start_timestamp=T0,
                allowed_markets=("m",),
                experiment_id="persisted-forward",
            )
            self.assertEqual(ForwardTestRegistry(store).list()[0].as_record(), persisted_spec.as_record())
        market = PredictionMarketSnapshot(
            T0,
            "m",
            "Question",
            yes_bid=0.20,
            yes_ask=0.30,
            yes_mid=0.25,
            liquidity=10.0,
            order_book=OrderBookSnapshot(T0, (OrderBookLevel(0.20, 10.0),), (OrderBookLevel(0.30, 10.0),)),
        )
        opportunities = scan_opportunities((market,), {"m": 0.80}, min_edge=0.1)
        self.assertEqual(len(opportunities), 1)
        self.assertEqual(opportunities[0].action, "PAPER_ONLY")

    def test_dashboard_exposes_store_health_detail(self) -> None:
        with AxiomStore(":memory:") as store:
            server = DashboardServer(port=0, data=DashboardData(store=store))
            server.start()
            try:
                assert server.url is not None
                with urlopen(server.url + "/api/dataset-health", timeout=2) as response:
                    payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200)
                self.assertEqual(payload["grade"], "F")
            finally:
                server.stop()
    def test_pipeline_drops_invalid_timestamps_and_outage_datasets(self) -> None:
        class InvalidHistoryProvider(SyntheticPredictionProvider):
            def price_history(self, market_id: str, start=None, end=None):
                return (
                    {"timestamp": "not-a-timestamp", "price": 0.99},
                    {"timestamp": T0.isoformat(), "price": 0.50},
                )

        class OutageProvider(SyntheticPredictionProvider):
            def price_history(self, market_id: str, start=None, end=None):
                raise RuntimeError("history unavailable")

        with AxiomStore(":memory:") as store:
            pipeline = MarketDataPipeline(store)
            invalid = pipeline.ingest_prediction(InvalidHistoryProvider(), ["synthetic-event-1"])
            self.assertEqual(invalid[0].records, 1)
            self.assertEqual(invalid[0].quality.value, "LOW")
            self.assertEqual(len(store.load_prediction_snapshots("synthetic-event-1")), 1)
            outage = pipeline.ingest_prediction(OutageProvider(), ["synthetic-event-1"])
            self.assertEqual(outage[0].records, 0)
            self.assertTrue(outage[0].errors)
            self.assertIsNone(store.load_dataset_record("prediction:synthetic-event-1", outage[0].dataset_version))

    def test_paper_trader_sorts_history_and_filters_risk_kwargs(self) -> None:
        class NarrowRisk:
            def check_order(self, order, price, quantity):
                return price > 0 and quantity > 0

        class AlwaysBuy:
            def signal(self, _context):
                return {"side": "buy_yes", "quantity": 1.0}

        crypto_trader = CryptoPaperTrader(
            SyntheticCryptoProvider(periods=2),
            AlwaysBuy(),
            risk=NarrowRisk(),
            portfolio=Portfolio(1_000_000.0),
        )
        self.assertIsNotNone(crypto_trader.run_once("BTCUSDT"))

        market = PredictionMarketSnapshot(
            T0,
            "m",
            "Question",
            yes_bid=0.40,
            yes_ask=0.60,
            yes_mid=0.50,
            no_bid=0.40,
            no_ask=0.60,
            no_mid=0.50,
            expiry=T0 + timedelta(days=1),
            settlement=SettlementState.RESOLVED_YES,
            resolution_criteria="official result",
        )
        provider = InMemoryPredictionProvider(
            markets=[market],
            histories={
                "m": [
                    {"timestamp": T0 + timedelta(days=1), "price": 0.60},
                    {"timestamp": T0, "price": 0.50},
                ]
            },
        )
        portfolio = Portfolio(100.0)
        fills = PredictionPaperTrader(provider, AlwaysBuy(), portfolio=portfolio).run("m", start=T0, end=T0 + timedelta(days=1))
        self.assertEqual(fills[0].timestamp, T0)
        self.assertEqual(portfolio.get_position("m", outcome="yes").quantity, 0.0)  # type: ignore[union-attr]

    def test_polymarket_trades_follow_cursor_after_short_page(self) -> None:
        calls: list[str] = []

        class Response:
            def __init__(self, payload: object) -> None:
                self.payload = json.dumps(payload).encode("utf-8")

            def read(self) -> bytes:
                return self.payload

            def close(self) -> None:
                return

        def opener(request: object, timeout: float) -> Response:
            del timeout
            url = str(getattr(request, "full_url"))
            calls.append(url)
            cursor = parse_qs(urlparse(url).query).get("cursor")
            row = {
                "timestamp": int((T0 + timedelta(minutes=len(calls))).timestamp()),
                "price": "0.50",
                "size": "1.0",
                "id": f"trade-{len(calls)}",
                "side": "BUY",
            }
            return Response({"data": [row], "next_cursor": "next" if not cursor else None})

        trades = PolymarketAdapter(opener=opener, timeout=1.0).trades("m")
        self.assertEqual(len(trades), 2)
        self.assertEqual(len(calls), 2)

    def test_immutable_trade_keys_are_scoped_and_robustness_rejects_bad_counts(self) -> None:
        trade = TradePrint(T0, 0.5, 1.0, trade_id="same")
        with AxiomStore(":memory:") as store:
            self.assertTrue(store.save_polymarket_trade("m1", trade, trade_key="manual"))
            self.assertTrue(store.save_polymarket_trade("m2", trade, trade_key="manual"))
            self.assertEqual(len(store.load_polymarket_trades()), 2)
        with self.assertRaises(ValueError):
            minimum_sample_check(-1)
        with self.assertRaises(ValueError):
            minimum_sample_check(1, trades=-1)
        with self.assertRaises(ValueError):
            neighboring_parameter_stability([1.0, 1.0], tolerance=float("nan"))

if __name__ == "__main__":
    unittest.main()
