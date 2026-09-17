from __future__ import annotations

from datetime import datetime, timedelta, timezone
import unittest

from axiom.backtest.prediction import PredictionMarketBacktester
from axiom.strategy import SignalEvaluator, StrategyValidationError
from axiom.strategy.signals import (
    CONSTANT_BASELINE,
    DIRECTIONAL_OOS_TRADING,
    ENTRY_PREDICATE_NOT_SATISFIED,
    INSUFFICIENT_LOOKBACK,
    MODEL_INPUT_MISSING,
    PROBABILITY_CALIBRATION_UNKNOWN,
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


def absolute_move_strategy(family: str, minimum_move: str = "0.05") -> dict[str, object]:
    return prediction_strategy(
        family,
        lookback=1,
        entry_predicate={
            "version": "absolute-move-v1",
            "minimum_move": minimum_move,
            "units": "probability",
            "boundary": "inclusive",
        },
    )


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
        self.assertEqual(evaluation.evidence["assessment_type"], DIRECTIONAL_OOS_TRADING)
        self.assertEqual(
            evaluation.evidence["probability_calibration"],
            PROBABILITY_CALIBRATION_UNKNOWN,
        )
        self.assertGreater(evaluation.score, 0.0)
        backtest = PredictionMarketBacktester(fee_bps=0.0, slippage_bps=0.0).run(
            rows,
            prediction_strategy("momentum", lookback=1, threshold=0.1),
        )
        self.assertTrue(all(record["market_id"] in {"a", "b"} for record in backtest.equity_curve))
        self.assertTrue(all(record["reason_code"] for record in backtest.equity_curve))

    def test_directional_families_produce_or_decline_without_model_input(self) -> None:
        produced = evaluate_signal_evaluation(
            prediction_strategy("momentum", lookback=1, threshold=0.05),
            {
                "market_id": "a",
                "observations": [snapshot("a", T0, 0.40), snapshot("a", T0 + timedelta(minutes=1), 0.50)],
            },
        )
        self.assertEqual(produced.reason_code, SIGNAL_PRODUCED)
        self.assertGreater(produced.score, 0.0)
        self.assertEqual(produced.evidence["assessment_type"], DIRECTIONAL_OOS_TRADING)
        self.assertIsNone(produced.evidence["model"]["probability"])

        declined = evaluate_signal_evaluation(
            prediction_strategy("mean_reversion", lookback=1, threshold=0.05),
            {
                "market_id": "a",
                "observations": [snapshot("a", T0, 0.50), snapshot("a", T0 + timedelta(minutes=1), 0.50)],
            },
        )
        self.assertEqual(declined.reason_code, STRATEGY_EVALUATED_DECLINED)
        self.assertEqual(declined.score, 0.0)
        self.assertEqual(declined.side, "flat")
        self.assertIsNone(declined.evidence["model"]["probability"])


    def test_absolute_move_boundary_is_inclusive_for_momentum_and_mean_reversion(self) -> None:
        cases = {
            "momentum": (
                (0.5499999999995, 0.0499999999995, ENTRY_PREDICATE_NOT_SATISFIED, False),
                (0.55, 0.05, SIGNAL_PRODUCED, True),
                (0.5500000000005, 0.0500000000005, SIGNAL_PRODUCED, True),
                (0.4500000000005, -0.0499999999995, ENTRY_PREDICATE_NOT_SATISFIED, False),
                (0.45, -0.05, SIGNAL_PRODUCED, True),
                (0.4499999999995, -0.0500000000005, SIGNAL_PRODUCED, True),
            ),
            "mean_reversion": (
                (0.4500000000005, 0.0499999999995, ENTRY_PREDICATE_NOT_SATISFIED, False),
                (0.45, 0.05, SIGNAL_PRODUCED, True),
                (0.4499999999995, 0.0500000000005, SIGNAL_PRODUCED, True),
                (0.5499999999995, -0.0499999999995, ENTRY_PREDICATE_NOT_SATISFIED, False),
                (0.55, -0.05, SIGNAL_PRODUCED, True),
                (0.5500000000005, -0.0500000000005, SIGNAL_PRODUCED, True),
            ),
        }
        for family, family_cases in cases.items():
            with self.subTest(family=family):
                for current, expected_delta, expected_reason, expected_eligible in family_cases:
                    evaluation = evaluate_signal_evaluation(
                        absolute_move_strategy(family),
                        {"market_id": "a", "observations": [snapshot("a", T0, 0.50), snapshot("a", T0 + timedelta(minutes=1), current)]},
                    )
                    self.assertEqual(evaluation.reason_code, expected_reason)
                    self.assertEqual(evaluation.evidence["entry_eligible"], expected_eligible)
                    self.assertAlmostEqual(evaluation.evidence["raw_delta"], expected_delta, places=12)
                    self.assertEqual(evaluation.evidence["signal_strength"], evaluation.score)

    def test_absolute_move_predicate_rejects_unknown_or_malformed_documents(self) -> None:
        valid = absolute_move_strategy("momentum")
        invalid_predicates = (
            {"version": "absolute-move-v2", "minimum_move": 0.05, "units": "probability", "boundary": "inclusive"},
            {"version": "absolute-move-v1", "minimum_move": 1.1, "units": "probability", "boundary": "inclusive"},
            {"version": "absolute-move-v1", "minimum_move": 0.05, "units": "odds", "boundary": "inclusive"},
            {"version": "absolute-move-v1", "minimum_move": 0.05, "units": "probability", "boundary": "exclusive"},
            {"version": "absolute-move-v1", "minimum_move": 0.05, "units": "probability", "boundary": "inclusive", "extra": True},
        )
        for predicate in invalid_predicates:
            with self.subTest(predicate=predicate):
                with self.assertRaises(StrategyValidationError):
                    evaluate_signal_evaluation({**valid, "parameters": {"entry_predicate": predicate}}, {"observations": []})
        with self.assertRaises(StrategyValidationError):
            evaluate_signal_evaluation(
                prediction_strategy(
                    "probability_mispricing",
                    threshold=0.05,
                    entry_predicate=valid["parameters"]["entry_predicate"],
                ),
                {"observations": []},
            )

    def test_absolute_move_negative_delta_buys_no(self) -> None:
        evaluation = evaluate_signal_evaluation(
            absolute_move_strategy("momentum"),
            {"market_id": "a", "observations": [snapshot("a", T0, 0.60), snapshot("a", T0 + timedelta(minutes=1), 0.50)]},
        )
        self.assertEqual(evaluation.reason_code, SIGNAL_PRODUCED)
        self.assertEqual(evaluation.side, "buy")
        self.assertEqual(evaluation.evidence["outcome"], "no")
        self.assertLess(evaluation.evidence["raw_delta"], 0.0)

    def test_absolute_move_missing_probability_is_not_eligible(self) -> None:
        missing = snapshot("a", T0 + timedelta(minutes=1), 0.50)
        missing["yes_mid"] = None
        missing.pop("yes_ask")
        evaluation = evaluate_signal_evaluation(
            absolute_move_strategy("mean_reversion"),
            {"market_id": "a", "observations": [snapshot("a", T0, 0.40), missing]},
        )
        self.assertEqual(evaluation.reason_code, MODEL_INPUT_MISSING)
        self.assertFalse(evaluation.evidence["entry_eligible"])

    def test_legacy_probability_mispricing_keeps_tiny_edge_scale(self) -> None:
        evaluation = evaluate_signal_evaluation(
            prediction_strategy("probability_mispricing", threshold=0.05),
            {"observations": [snapshot("a", T0, 0.50, model=0.500001)]},
        )
        self.assertEqual(evaluation.reason_code, SIGNAL_PRODUCED)
        self.assertAlmostEqual(evaluation.score, 0.00002, places=8)

    def test_explicit_market_scope_with_no_rows_stays_warming_up(self) -> None:
        evaluation = evaluate_signal_evaluation(
            prediction_strategy("momentum", lookback=1),
            {
                "market_id": "target-market",
                "observations": [snapshot("other-market", T0, 0.95)],
            },
        )
        self.assertEqual(evaluation.reason_code, WARMING_UP)
        self.assertEqual(evaluation.score, 0.0)

    def test_prediction_lookback_missing_market_price_is_input_missing(self) -> None:
        missing_price = snapshot("a", T0 + timedelta(minutes=1), 0.50)
        missing_price["yes_mid"] = None
        missing_price.pop("yes_ask")
        evaluation = evaluate_signal_evaluation(
            prediction_strategy("momentum", lookback=1),
            {"market_id": "a", "observations": [snapshot("a", T0, 0.40), missing_price]},
        )
        self.assertEqual(evaluation.reason_code, MODEL_INPUT_MISSING)
        self.assertEqual(evaluation.score, 0.0)

    def test_missing_market_price_cannot_be_a_zero_edge(self) -> None:
        missing_price = snapshot("a", T0, 0.40, model=0.40)
        missing_price["yes_mid"] = None
        missing_price.pop("yes_ask")
        evaluation = evaluate_signal_evaluation(
            prediction_strategy("probability_mispricing", threshold=0.05),
            {"market_id": "a", "observations": [missing_price]},
        )
        self.assertEqual(evaluation.reason_code, MODEL_INPUT_MISSING)
        self.assertEqual(evaluation.score, 0.0)

    def test_global_model_sequence_keeps_filtered_observation_identity(self) -> None:
        rows = [
            snapshot("a", T0 + timedelta(minutes=1), 0.40),
            snapshot("b", T0, 0.60),
        ]
        evaluation = evaluate_signal_evaluation(
            prediction_strategy("probability_mispricing", threshold=0.05),
            {"market_id": "b", "observations": rows, "probabilities": [0.10, 0.80]},
        )
        self.assertEqual(evaluation.reason_code, SIGNAL_PRODUCED)
        self.assertEqual(evaluation.evidence["model"]["probability"], 0.80)
        self.assertGreater(evaluation.score, 0.0)

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
        self.assertEqual(declined.score, 0.0)
        self.assertEqual(declined.side, "flat")
        produced = evaluate_signal_evaluation(
            prediction_strategy("probability_mispricing", threshold=0.05),
            {"observations": [snapshot("a", T0, 0.40, model=0.80)]},
        )
        self.assertEqual(produced.reason_code, SIGNAL_PRODUCED)
        self.assertGreater(produced.score, 0.0)
        self.assertEqual(produced.side, "buy")
        self.assertEqual(produced.evidence["model"]["probability"], 0.80)
        negative = evaluate_signal_evaluation(
            prediction_strategy("probability_mispricing", threshold=0.05),
            {"observations": [snapshot("a", T0, 0.60, model=0.20)]},
        )
        self.assertEqual(negative.reason_code, SIGNAL_PRODUCED)
        self.assertLess(negative.score, 0.0)
        self.assertEqual(negative.side, "sell")
        self.assertEqual(negative.evidence["model"]["probability"], 0.20)

    def test_public_signal_evaluator_import_and_record_contract(self) -> None:
        evaluator = SignalEvaluator()
        strategy = prediction_strategy("probability_mispricing", threshold=0.05)
        context = {"observations": [snapshot("a", T0, 0.40, model=0.80)]}
        score = evaluator(strategy, context)
        record = evaluator.evaluate_record(strategy, context)
        self.assertGreater(score, 0.0)
        self.assertEqual(record.score, score)
        self.assertEqual(record.reason_code, SIGNAL_PRODUCED)
        self.assertEqual(record.evidence["model"]["evidence"]["model_source"], "OBSERVATION")


if __name__ == "__main__":
    unittest.main()
