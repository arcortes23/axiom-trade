from __future__ import annotations

from datetime import datetime, timedelta, timezone
import unittest
from unittest.mock import patch

from axiom.backtest.prediction import (
    CANONICAL_EVALUATOR_VERSION,
    PRICE_PROXY_RESEARCH,
    RECORDED_BOOK_REPLAY,
    PredictionMarketBacktester,
    _coerce_book,
    _complement_book,
    select_prediction_paths,
)
from axiom.domain import Fill, MarketType, Side
from axiom.metrics import calculate_prediction_metrics


T0 = datetime(2026, 1, 1, 12, tzinfo=timezone.utc)


def _strategy() -> dict[str, object]:
    return {
        "version": 1,
        "strategy_id": "prediction-accounting-test",
        "market_type": "prediction",
        "family": "momentum",
        "parameters": {"lookback": 1, "threshold": 0.1},
        "probability_model": "deterministic-test-v1",
        "resolution_aware": True,
        "resolution_inputs": ["expiry", "settlement"],
    }


def _price_row(index: int, market_id: str, yes_mid: float) -> dict[str, object]:
    return {
        "market_id": market_id,
        "timestamp": T0 + timedelta(minutes=index),
        "yes_mid": yes_mid,
        "yes_bid": yes_mid - 0.01,
        "yes_ask": yes_mid + 0.01,
        "no_mid": 1.0 - yes_mid,
        "no_bid": 1.0 - yes_mid - 0.01,
        "no_ask": 1.0 - yes_mid + 0.01,
        "liquidity": 100.0,
    }


def _book_row(index: int, yes_mid: float) -> dict[str, object]:
    timestamp = T0 + timedelta(minutes=index)
    return {
        **_price_row(index, "market-1", yes_mid),
        "order_book": {
            "timestamp": timestamp.isoformat(),
            "bids": [[yes_mid - 0.01, 10.0]],
            "asks": [[yes_mid + 0.01, 10.0]],
            "token_id": "yes-token",
            "condition_id": "condition-1",
        },
        "no_order_book": {
            "timestamp": timestamp.isoformat(),
            "bids": [[1.0 - yes_mid - 0.01, 10.0]],
            "asks": [[1.0 - yes_mid + 0.01, 10.0]],
            "token_id": "no-token",
            "condition_id": "condition-1",
        },
    }


def _yes_only_book_row(index: int, yes_mid: float) -> dict[str, object]:
    row = _price_row(index, "market-1", yes_mid)
    timestamp = T0 + timedelta(minutes=index)
    row["order_book"] = {
        "timestamp": timestamp.isoformat(),
        "bids": [[yes_mid - 0.01, 10.0]],
        "asks": [[yes_mid + 0.01, 10.0]],
        "token_id": "yes-token",
        "condition_id": "condition-1",
    }
    return row


def _fill(side: Side, order_id: str, *, execution_kind: str | None = None) -> Fill:
    metadata: dict[str, object] = {"outcome_value": 1.0, "reference_price": 0.5}
    if execution_kind is not None:
        metadata["execution_kind"] = execution_kind
    return Fill(
        timestamp=T0,
        market_type=MarketType.PREDICTION,
        symbol="market-1",
        side=side,
        quantity=1.0,
        price=0.5,
        fees=0.0,
        slippage=0.0,
        strategy_id="prediction-accounting-test",
        order_id=order_id,
        market_id="market-1",
        expected_probability=0.8,
        metadata=metadata,
    )


class PredictionAccountingTests(unittest.TestCase):
    def test_whitespace_equivalent_market_rows_share_one_exit_queue(self) -> None:
        rows = [
            _price_row(0, "market-1", 0.40),
            _price_row(1, " market-1 ", 0.60),
            _price_row(2, "market-1", 0.62),
            _price_row(3, " market-1 ", 0.64),
            _price_row(4, "market-1", 0.64),
            _price_row(5, " market-1 ", 0.64),
        ]
        result = PredictionMarketBacktester().run(rows, _strategy(), mode=PRICE_PROXY_RESEARCH)
        self.assertEqual(
            [fill.metadata["execution_kind"] for fill in result.fills],
            ["entry", "exit"],
        )
        self.assertEqual({fill.market_id for fill in result.fills}, {"market-1"})

    def test_evaluation_reason_counts_are_derived_from_every_curve_observation(self) -> None:
        result = PredictionMarketBacktester().run(
            [
                _price_row(0, "market-1", 0.40),
                _price_row(1, "market-1", 0.60),
                _price_row(2, "market-1", 0.62),
            ],
            _strategy(),
            mode=PRICE_PROXY_RESEARCH,
        )
        expected: dict[str, int] = {}
        for observation in result.equity_curve:
            reason_code = observation["reason_code"]
            expected[reason_code] = expected.get(reason_code, 0) + 1
        evaluation = result.metrics["evaluation"]
        self.assertEqual(evaluation["reason_counts"], dict(sorted(expected.items())))
        self.assertEqual(sum(evaluation["reason_counts"].values()), len(result.equity_curve))
        self.assertEqual(sum(evaluation["reason_counts"].values()), evaluation["evaluated_observations"])

    def test_recorded_replay_end_gap_keeps_position_and_marks_without_exit_fill(self) -> None:
        rows = [_book_row(0, 0.40), _book_row(1, 0.60), _book_row(2, 0.62)]
        yes_book = rows[-1]["order_book"]
        assert isinstance(yes_book, dict)
        yes_book["bids"] = []
        result = PredictionMarketBacktester(
            initial_cash=100.0,
            fee_bps=0.0,
            slippage_bps=0.0,
            allocation=0.50,
        ).run(
            rows,
            _strategy(),
            mode=RECORDED_BOOK_REPLAY,
            holding_period=1,
            exit_policy={"type": "fixed_holding_period", "holding_period": 1},
        )
        accounting = result.metrics["portfolio_accounting"]
        self.assertEqual([fill.metadata["execution_kind"] for fill in result.fills], ["entry"])
        self.assertEqual(accounting["completed_round_trips"], 0)
        self.assertEqual(accounting["closing_fills"], 0)
        self.assertEqual(accounting["open_positions"], ["market-1"])
        self.assertGreater(accounting["unrealized_pnl"], 0.0)
        self.assertEqual(accounting["net_pnl"], accounting["realized_pnl"] + accounting["unrealized_pnl"])

    def test_recorded_replay_terminal_settlement_changes_cash_without_closing_fill(self) -> None:
        rows = [_book_row(0, 0.40), _book_row(1, 0.60), _price_row(2, "market-1", 0.62)]
        rows[-1]["settlement"] = "resolved_yes"
        result = PredictionMarketBacktester(
            initial_cash=100.0,
            fee_bps=0.0,
            slippage_bps=0.0,
            allocation=0.50,
        ).run(
            rows,
            _strategy(),
            mode=RECORDED_BOOK_REPLAY,
            holding_period=1,
            exit_policy={"type": "fixed_holding_period", "holding_period": 1},
        )
        accounting = result.metrics["portfolio_accounting"]
        self.assertEqual([fill.metadata["execution_kind"] for fill in result.fills], ["entry"])
        self.assertEqual(result.unresolved, ())
        self.assertEqual(result.outcomes, {"market-1": "resolved_yes"})
        self.assertEqual(accounting["completed_round_trips"], 0)
        self.assertEqual(accounting["closing_fills"], 0)
        self.assertEqual(accounting["open_positions"], [])
        self.assertGreater(accounting["cash"], 100.0 - accounting["allocated_capital"])
        self.assertEqual(accounting["net_pnl"], accounting["realized_pnl"])

    def test_mapping_order_books_preserve_condition_identity(self) -> None:
        result = PredictionMarketBacktester().run(
            [_book_row(0, 0.40), _book_row(1, 0.60)],
            _strategy(),
            mode=RECORDED_BOOK_REPLAY,
        )
        self.assertEqual(
            {
                row["execution_evidence"]["book_condition_id"]
                for row in result.equity_curve
            },
            {"condition-1"},
        )

    def test_recorded_replay_entry_metadata_reports_depth_vwap_and_partial_fill(self) -> None:
        rows = [_book_row(0, 0.40), _book_row(1, 0.60)]
        order_book = rows[1]["order_book"]
        assert isinstance(order_book, dict)
        order_book["asks"] = [[0.61, 1.0], [0.71, 2.0]]
        result = PredictionMarketBacktester(
            initial_cash=100.0,
            fee_bps=0.0,
            slippage_bps=10.0,
            allocation=0.50,
        ).run(
            rows,
            _strategy(),
            mode=RECORDED_BOOK_REPLAY,
            holding_period=1,
            exit_policy={"type": "fixed_holding_period", "holding_period": 1},
        )
        entry = result.fills[0]
        raw_vwap = (0.61 + 2.0 * 0.71) / 3.0
        self.assertEqual(entry.metadata["execution_kind"], "entry")
        self.assertAlmostEqual(entry.quantity, 3.0)
        self.assertTrue(entry.metadata["partial_fill"])
        self.assertAlmostEqual(entry.metadata["raw_execution_price"], raw_vwap)
        self.assertAlmostEqual(entry.metadata["reference_price"], raw_vwap)
        self.assertAlmostEqual(entry.metadata["assumed_execution_price"], entry.price)
        self.assertNotAlmostEqual(entry.metadata["raw_execution_price"], 0.61)

    def test_legacy_no_trade_with_yes_only_book_preserves_provenance(self) -> None:
        rows = [_yes_only_book_row(0, 0.60), _yes_only_book_row(1, 0.40)]
        result = PredictionMarketBacktester().run(rows, _strategy())

        self.assertEqual(len(result.fills), 1)
        self.assertEqual(result.fills[0].metadata["outcome"], "no")

        yes_book = _coerce_book(rows[1]["order_book"], rows[1]["timestamp"])
        self.assertIsNotNone(yes_book)
        assert yes_book is not None
        no_book = _complement_book(yes_book)
        self.assertEqual(no_book.timestamp, yes_book.timestamp)
        self.assertEqual(no_book.token_id, yes_book.token_id)
        self.assertEqual(no_book.condition_id, yes_book.condition_id)

    def test_evaluator_failure_clears_name_and_evaluated_observations(self) -> None:
        rows = [
            _price_row(0, "market-1", 0.40),
            _price_row(1, "market-1", 0.40),
        ]
        with patch(
            "axiom.backtest.prediction.evaluate_signal_evaluation",
            side_effect=RuntimeError("fixture evaluator exploded"),
        ):
            result = PredictionMarketBacktester().run(
                rows,
                _strategy(),
                mode=PRICE_PROXY_RESEARCH,
            )
        evaluation = result.metrics["evaluation"]
        accounting = result.metrics["portfolio_accounting"]
        self.assertTrue(evaluation["evaluator_invoked"])
        self.assertFalse(evaluation["evaluator_completed"])
        self.assertEqual(evaluation["evaluated_observations"], 0)
        self.assertIsNone(evaluation["evaluator_name"])
        self.assertEqual(evaluation["evaluator_error"], "fixture evaluator exploded")
        self.assertEqual(
            {row["evaluator"] for row in result.equity_curve},
            {CANONICAL_EVALUATOR_VERSION},
        )
        self.assertEqual(
            result.equity_curve[0]["evaluation_evidence"],
            {"error": "fixture evaluator exploded"},
        )
        self.assertFalse(accounting["accounting_available"])
        self.assertIsNone(accounting["realized_pnl"])

    def test_empty_input_does_not_fabricate_evaluator_completion(self) -> None:
        result = PredictionMarketBacktester().run(
            [],
            _strategy(),
            mode=PRICE_PROXY_RESEARCH,
        )
        evaluation = result.metrics["evaluation"]
        self.assertFalse(evaluation["evaluator_invoked"])
        self.assertFalse(evaluation["evaluator_completed"])
        self.assertEqual(evaluation["evaluated_observations"], 0)
        self.assertEqual(evaluation["diagnostic_summary_count"], 1)
        self.assertIsNone(evaluation["evaluator_name"])
        self.assertIsNone(evaluation["evaluator_error"])
        self.assertFalse(result.metrics["portfolio_accounting"]["accounting_available"])

    def test_untagged_sell_does_not_change_forecast_or_calibration(self) -> None:
        entry = _fill(Side.BUY, "entry")
        untagged_sell = _fill(Side.SELL, "untagged-sell")
        baseline = calculate_prediction_metrics([100.0], fills=[entry], initial_equity=100.0)
        with_sell = calculate_prediction_metrics(
            [100.0],
            fills=[entry, untagged_sell],
            initial_equity=100.0,
        )
        for key in ("brier", "log_loss", "ece", "raw_edge", "executable_edge", "expected_value", "expected_roi"):
            self.assertEqual(with_sell[key], baseline[key])

        explicit_entry_sell = _fill(Side.SELL, "explicit-entry-sell", execution_kind="entry")
        with_explicit_entry = calculate_prediction_metrics(
            [100.0],
            fills=[explicit_entry_sell],
            initial_equity=100.0,
        )
        self.assertNotEqual(with_explicit_entry["brier"], 0.0)


    def test_explicit_evaluator_ignores_incomplete_markets_but_keeps_manifest(self) -> None:
        strategy = _strategy()
        strategy["parameters"] = {
            **strategy["parameters"],
            "entry_predicate": {
                "version": "absolute-move-v1",
                "minimum_move": "0.05",
                "units": "probability",
                "boundary": "inclusive",
            },
        }
        rows = [
            _price_row(0, "short", 0.40),
            _price_row(0, "complete", 0.40),
            _price_row(1, "complete", 0.60),
            _price_row(2, "complete", 0.62),
        ]
        result = PredictionMarketBacktester().run(
            rows,
            strategy,
            mode=PRICE_PROXY_RESEARCH,
        )
        manifest = result.metrics["path_manifest"]
        self.assertEqual(manifest["expected_market_ids"], ["short", "complete"])
        self.assertEqual(manifest["eligible_market_ids"], ["complete"])
        self.assertEqual(manifest["excluded_market_ids"], ["short"])
        self.assertEqual({fill.market_id for fill in result.fills}, {"complete"})
        self.assertEqual({row["market_id"] for row in result.equity_curve}, {"complete"})
        self.assertEqual(result.metrics["evaluation"]["evaluated_observations"], 3)

    def test_explicit_replay_unresolved_exit_is_accounted_without_imputation(self) -> None:
        strategy = _strategy()
        strategy["parameters"] = {
            **strategy["parameters"],
            "entry_predicate": {
                "version": "absolute-move-v1",
                "minimum_move": "0.05",
                "units": "probability",
                "boundary": "inclusive",
            },
        }
        rows = [_book_row(0, 0.40), _book_row(1, 0.60), _book_row(2, 0.62)]
        assert isinstance(rows[-1]["order_book"], dict)
        rows[-1]["order_book"]["bids"] = []
        result = PredictionMarketBacktester(
            initial_cash=100.0,
            fee_bps=0.0,
            slippage_bps=0.0,
            allocation=0.50,
        ).run(
            rows,
            strategy,
            mode=RECORDED_BOOK_REPLAY,
            holding_period=1,
        )
        accounting = result.metrics["path_manifest"]["unresolved_exit_accounting"]
        self.assertEqual(accounting["unresolved_exit_count"], 1)
        self.assertEqual(accounting["unresolved_market_ids"], ["market-1"])
        self.assertEqual(accounting["forced_closes"], 0)
        self.assertEqual(accounting["forward_fills"], 0)
        self.assertTrue(result.metrics["path_manifest"]["no_imputation"])
        self.assertEqual([fill.metadata["execution_kind"] for fill in result.fills], ["entry"])
        self.assertEqual(result.metrics["portfolio_accounting"]["open_positions"], ["market-1"])


    def test_entry_requires_canonical_entry_eligibility_and_actionability(self) -> None:
        strategy = _strategy()
        strategy["parameters"] = {
            **strategy["parameters"],
            "entry_predicate": {
                "version": "absolute-move-v1",
                "minimum_move": "0.05",
                "units": "probability",
                "boundary": "inclusive",
            },
        }
        result = PredictionMarketBacktester().run(
            [
                _price_row(0, "subthreshold", 0.40),
                _price_row(1, "subthreshold", 0.44),
                _price_row(2, "subthreshold", 0.44),
            ],
            strategy,
            mode=PRICE_PROXY_RESEARCH,
        )
        self.assertEqual(result.fills, ())
        self.assertEqual(result.metrics["path_manifest"]["eligible_market_ids"], ["subthreshold"])
        self.assertEqual(result.equity_curve[0]["reason_code"], "INSUFFICIENT_LOOKBACK")
        self.assertEqual(
            [row["reason_code"] for row in result.equity_curve[1:]],
            ["ENTRY_PREDICATE_NOT_SATISFIED", "ENTRY_PREDICATE_NOT_SATISFIED"],
        )

    def test_terminal_replay_path_allows_missing_books_but_validates_supplied_books(self) -> None:
        strategy = _strategy()
        strategy["parameters"] = {
            **strategy["parameters"],
            "entry_predicate": {
                "version": "absolute-move-v1",
                "minimum_move": "0.05",
                "units": "probability",
                "boundary": "inclusive",
            },
        }
        terminal = _price_row(2, "market-1", 0.62)
        terminal["settlement"] = "resolved_yes"
        result = PredictionMarketBacktester(
            initial_cash=100.0,
            fee_bps=0.0,
            slippage_bps=0.0,
            allocation=0.50,
        ).run(
            [_book_row(0, 0.40), _book_row(1, 0.60), terminal],
            strategy,
            mode=RECORDED_BOOK_REPLAY,
            holding_period=1,
        )
        self.assertEqual(result.metrics["path_manifest"]["eligible_market_ids"], ["market-1"])
        self.assertEqual([fill.metadata["execution_kind"] for fill in result.fills], ["entry"])
        self.assertEqual(result.unresolved, ())

        future_terminal = dict(terminal)
        future_terminal["order_book"] = {
            "timestamp": (T0 + timedelta(minutes=3)).isoformat(),
            "bids": [[0.60, 10.0]],
            "asks": [[0.61, 10.0]],
        }
        with self.assertRaisesRegex(ValueError, "future-dated"):
            PredictionMarketBacktester().run(
                [_book_row(0, 0.40), _book_row(1, 0.60), future_terminal],
                strategy,
                mode=RECORDED_BOOK_REPLAY,
                holding_period=1,
            )

    def test_max_gap_observations_is_rejected_as_unsupported(self) -> None:
        strategy = _strategy()
        strategy["parameters"] = {
            **strategy["parameters"],
            "entry_predicate": {
                "version": "absolute-move-v1",
                "minimum_move": "0.05",
                "units": "probability",
                "boundary": "inclusive",
            },
            "max_gap_observations": 1,
        }
        with self.assertRaisesRegex(ValueError, "max_gap_observations.*unsupported"):
            select_prediction_paths(
                [_price_row(index, "market-1", 0.40 + index * 0.10) for index in range(3)],
                strategy,
                mode=PRICE_PROXY_RESEARCH,
            )

    def test_declared_max_gap_changes_only_structural_eligibility(self) -> None:
        strategy = _strategy()
        strategy["parameters"] = {
            **strategy["parameters"],
            "entry_predicate": {
                "version": "absolute-move-v1",
                "minimum_move": "0.05",
                "units": "probability",
                "boundary": "inclusive",
            },
            "max_gap_policy": {"max_gap_seconds": 30},
        }
        rows = [
            _price_row(0, "short-gap", 0.40),
            _price_row(1, "short-gap", 0.60),
            _price_row(2, "short-gap", 0.62),
        ]
        manifest = select_prediction_paths(rows, strategy, mode=PRICE_PROXY_RESEARCH)
        self.assertEqual(manifest["eligible_market_ids"], [])
        self.assertEqual(manifest["excluded_market_ids"], ["short-gap"])
        self.assertEqual(manifest["exclusion_reasons"], {"MAX_GAP_EXCEEDED": 1})

if __name__ == "__main__":
    unittest.main()
