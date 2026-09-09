from __future__ import annotations

from datetime import datetime, timedelta, timezone
import unittest

from axiom.backtest.prediction import PredictionMarketBacktester
from axiom.strategy.signals import (
    CONSTANT_BASELINE,
    INSUFFICIENT_LOOKBACK,
    MODEL_INPUT_MISSING,
    SIGNAL_PRODUCED,
    STRATEGY_EVALUATED_DECLINED,
    WARMING_UP,
    evaluate_model_document,
    evaluate_signal_evaluation,
)


UTC = timezone.utc
T0 = datetime(2024, 1, 1, tzinfo=UTC)


def prediction_strategy(family: str, **parameters: object) -> dict[str, object]:
    return {
        "version": 1,
        "market_type": "prediction",
        "family": family,
        "parameters": parameters,
        "probability_model": "test-model",
        "resolution_aware": True,
        "resolution_inputs": ["expiry", "settlement"],
    }


def snapshot(market_id: str, stamp: datetime, yes: float, *, source: datetime | None = None, model: float | None = None) -> dict[str, object]:
    row: dict[str, object] = {
        "market_id": market_id,
        "timestamp": stamp,
        "source_timestamp": source or stamp,
        "yes_mid": yes,
        "yes_ask": min(0.99, yes + 0.01),
        "yes_bid": max(0.01, yes - 0.01),
        "no_ask": min(0.99, 1.0 - yes + 0.01),
        "settlement": "open",
    }
    if model is not None:
        row["model_probability"] = model
    return row


class MarketScopeStrategyInputTests(unittest.TestCase):
    def test_prediction_history_is_market_partitioned_and_source_ordered(self) -> None:
        rows = [
            snapshot("a", T0 + timedelta(minutes=2), 0.70, source=T0 + timedelta(minutes=2)),
            snapshot("b", T0 + timedelta(minutes=1), 0.95, source=T0 + timedelta(minutes=1)),
            snapshot("a", T0, 0.20, source=T0),
        ]
        context = {"market_id": "a", "observations": rows}
        evaluation = evaluate_signal_evaluation(prediction_strategy("momentum", lookback=1, threshold=0.1), context)
        self.assertEqual(evaluation.reason_code, SIGNAL_PRODUCED)
        self.assertGreater(evaluation.score, 0.0)
        backtest = PredictionMarketBacktester(fee_bps=0.0, slippage_bps=0.0).run(
            rows,
            prediction_strategy("momentum", lookback=1, threshold=0.1),
        )
        self.assertTrue(all(record["market_id"] in {"a", "b"} for record in backtest.equity_curve))
        self.assertTrue(all(record["reason_code"] for record in backtest.equity_curve))

    def test_declared_lookbacks_are_enforced(self) -> None:
        rows = [snapshot("a", T0, 0.40)]
        prediction = evaluate_signal_evaluation(prediction_strategy("momentum", lookback=2), {"observations": rows})
        self.assertEqual(prediction.reason_code, INSUFFICIENT_LOOKBACK)
        crypto = {
            "version": 1,
            "market_type": "crypto_spot",
            "family": "mean_reversion",
            "parameters": {"lookback": 3},
        }
        crypto_eval = evaluate_signal_evaluation(crypto, {"bars": [{"close": 100.0}, {"close": 99.0}]})
        self.assertEqual(crypto_eval.reason_code, INSUFFICIENT_LOOKBACK)
        warming = evaluate_signal_evaluation(prediction_strategy("momentum"), {"observations": []})
        self.assertEqual(warming.reason_code, WARMING_UP)

    def test_missing_model_and_constant_baseline_are_explicit(self) -> None:
        missing = evaluate_signal_evaluation(
            prediction_strategy("probability_mispricing", threshold=0.05),
            {"observations": [snapshot("a", T0, 0.40)]},
        )
        self.assertEqual(missing.reason_code, MODEL_INPUT_MISSING)
        baseline = evaluate_model_document({"probability": 0.50}, snapshot("a", T0, 0.40))
        self.assertEqual(baseline.probability, 0.50)
        self.assertEqual(baseline.evidence["model_source"], CONSTANT_BASELINE)

    def test_declined_and_produced_states_are_consumer_visible(self) -> None:
        equal = snapshot("a", T0, 0.50, model=0.50)
        declined = evaluate_signal_evaluation(
            prediction_strategy("probability_mispricing", threshold=0.05),
            {"observations": [equal]},
        )
        self.assertEqual(declined.reason_code, STRATEGY_EVALUATED_DECLINED)
        produced = evaluate_signal_evaluation(
            prediction_strategy("probability_mispricing", threshold=0.05),
            {"observations": [snapshot("a", T0, 0.40, model=0.80)]},
        )
        self.assertEqual(produced.reason_code, SIGNAL_PRODUCED)
        self.assertTrue(produced.actionable)


if __name__ == "__main__":
    unittest.main()
