from __future__ import annotations

from datetime import datetime, timedelta, timezone
import unittest

from axiom.backtest.prediction import (
    PRICE_PROXY_RESEARCH,
    RECORDED_BOOK_REPLAY,
    PredictionMarketBacktester,
    PredictionResearchMode,
    run_prediction_research_mode,
    select_prediction_paths,
)
from axiom.backtest import (
    run_prediction_research_mode as package_run_prediction_research_mode,
    select_prediction_paths as package_select_prediction_paths,
)
from axiom.domain import ResearchQuality
from axiom.strategy.signals import (
    DIRECTIONAL_OOS_TRADING,
    PROBABILITY_CALIBRATION_UNKNOWN,
    evaluate_signal_evaluation,
)


T0 = datetime(2026, 1, 1, 12, tzinfo=timezone.utc)


def _strategy() -> dict[str, object]:
    return {
        "version": 1,
        "strategy_id": "research-mode-test",
        "market_type": "prediction",
        "family": "momentum",
        "parameters": {"lookback": 1, "threshold": 0.1},
        "probability_model": "deterministic-test-v1",
        "resolution_aware": True,
        "resolution_inputs": ["expiry", "settlement"],
    }


def _row(
    index: int,
    yes_mid: float,
    *,
    book: bool = False,
    market_id: str = "market-1",
) -> dict[str, object]:
    result: dict[str, object] = {
        "market_id": market_id,
        "timestamp": T0 + timedelta(minutes=index),
        "yes_mid": yes_mid,
        "yes_ask": min(0.99, yes_mid + 0.01),
        "yes_bid": max(0.01, yes_mid - 0.01),
        "no_mid": 1.0 - yes_mid,
        "no_ask": min(0.99, 1.0 - yes_mid + 0.01),
        "no_bid": max(0.01, 1.0 - yes_mid - 0.01),
        "liquidity": 100.0,
    }
    if book:
        observed_at = result["timestamp"] + timedelta(seconds=1)
        result.update(
            {
                "source_type": "HISTORICAL",
                "source_timestamp": result["timestamp"],
                "source_snapshot_id": f"source-{market_id}-{index}",
                "provider": "fixture",
                "observed_at": observed_at,
                "available_at": observed_at,
                "response_received_at": observed_at,
                "order_book": {
                    "timestamp": result["timestamp"].isoformat(),
                    "bids": [[yes_mid - 0.01, 10.0]],
                    "asks": [[yes_mid + 0.01, 10.0]],
                    "token_id": f"yes-{market_id}",
                },
                "no_order_book": {
                    "timestamp": result["timestamp"].isoformat(),
                    "bids": [[1.0 - yes_mid - 0.01, 10.0]],
                    "asks": [[1.0 - yes_mid + 0.01, 10.0]],
                    "token_id": f"no-{market_id}",
                },
            }
        )
    return result


class PolymarketResearchModeTests(unittest.TestCase):
    def test_proxy_execution_is_causal_and_marked_conditional(self) -> None:
        result = PredictionMarketBacktester().run(
            [_row(0, 0.4), _row(1, 0.6), _row(2, 0.62)],
            _strategy(),
            mode=PRICE_PROXY_RESEARCH,
        )
        self.assertEqual(result.research_quality, ResearchQuality.PRICE_PROXY)
        self.assertTrue(all(row["research_mode"] == "PRICE_PROXY_RESEARCH" for row in result.equity_curve))
        self.assertTrue(result.fills)
        self.assertGreater(result.fills[0].timestamp, T0)
        self.assertEqual(result.fills[0].metadata["research_mode"], "PRICE_PROXY_RESEARCH")
        self.assertIn(result.equity_curve[0]["price_path_status"], {"PRICE_PATH", "CONDITIONAL_PRICE_PATH"})

    def test_recorded_replay_requires_timestamped_observed_book(self) -> None:
        result = PredictionMarketBacktester().run(
            [_row(0, 0.4, book=True), _row(1, 0.6, book=True)],
            _strategy(),
            mode=RECORDED_BOOK_REPLAY,
        )
        self.assertEqual(result.research_quality, ResearchQuality.ORDER_BOOK_SIMULATED)
        self.assertTrue(all(row["research_mode"] == "RECORDED_BOOK_REPLAY" for row in result.equity_curve))
        self.assertTrue(result.fills)
        self.assertTrue(result.equity_curve[0]["execution_evidence"]["raw_book_observed"])
        with self.assertRaisesRegex(ValueError, "REPLAY_BOOK_REQUIRED"):
            PredictionMarketBacktester().run(
                [_row(0, 0.4), _row(1, 0.6)],
                _strategy(),
                mode=PredictionResearchMode.RECORDED_BOOK_REPLAY,
            )

    def test_prediction_research_helpers_are_publicly_exported(self) -> None:
        self.assertIs(package_run_prediction_research_mode, run_prediction_research_mode)
        self.assertIs(package_select_prediction_paths, select_prediction_paths)

    def test_explicit_replay_rejects_paper_forward_rows(self) -> None:
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
        rows = [_row(index, 0.40 + 0.10 * index, book=True) for index in range(3)]
        rows[0]["source_type"] = "PAPER_FORWARD"
        with self.assertRaisesRegex(ValueError, "PAPER_FORWARD"):
            PredictionMarketBacktester().run(
                rows,
                strategy,
                mode=RECORDED_BOOK_REPLAY,
            )


    def test_proxy_interleaving_uses_per_market_holding_and_cost_provenance(self) -> None:
        rows = [
            _row(0, 0.4, market_id="market-a"),
            _row(0, 0.4, market_id="market-b"),
            _row(1, 0.6, market_id="market-a"),
            _row(1, 0.6, market_id="market-b"),
            _row(2, 0.62, market_id="market-a"),
            _row(2, 0.62, market_id="market-b"),
            _row(3, 0.64, market_id="market-a"),
            _row(3, 0.64, market_id="market-b"),
        ]
        result = PredictionMarketBacktester(
            fee_bps=10.0,
            slippage_bps=5.0,
        ).run(
            rows,
            _strategy(),
            mode=PRICE_PROXY_RESEARCH,
            exit_policy={"type": "fixed_holding_period", "holding_period": 1},
        )
        for market_id in ("market-a", "market-b"):
            fills = [fill for fill in result.fills if fill.market_id == market_id]
            self.assertEqual([fill.metadata["execution_kind"] for fill in fills], ["entry", "exit"])
            self.assertGreater(fills[0].timestamp, T0 + timedelta(minutes=1))
            self.assertGreater(fills[1].timestamp, fills[0].timestamp)
            self.assertNotEqual(
                fills[0].metadata["raw_execution_price"],
                fills[0].metadata["assumed_execution_price"],
            )
            self.assertEqual(fills[0].metadata["assumption_version"], "price-proxy-v1")
            self.assertEqual(fills[1].metadata["assumption_version"], "price-proxy-v1")
            self.assertEqual(fills[0].metadata["holding_observations"], 1)
            self.assertEqual(fills[1].metadata["holding_observations"], 1)

        for observation in result.equity_curve:
            raw_events = observation["execution_evidence"]["raw_execution"]
            self.assertTrue(
                all(event["market_id"] == observation["market_id"] for event in raw_events)
            )
        market_b_row = next(
            row
            for row in result.equity_curve
            if row["market_id"] == "market-b"
            and row["execution_evidence"]["raw_execution"]
        )
        self.assertEqual(
            {event["market_id"] for event in market_b_row["execution_evidence"]["raw_execution"]},
            {"market-b"},
        )

    def test_proxy_holds_pending_execution_across_missing_future_quote(self) -> None:
        missing = _row(2, 0.62)
        for field in ("yes_mid", "yes_ask", "yes_bid"):
            missing[field] = None
        result = PredictionMarketBacktester(
            fee_bps=10.0,
            slippage_bps=5.0,
        ).run(
            [_row(0, 0.4), _row(1, 0.6), missing, _row(3, 0.64), _row(4, 0.66)],
            _strategy(),
            mode=PRICE_PROXY_RESEARCH,
        )
        self.assertEqual([fill.metadata["execution_kind"] for fill in result.fills], ["entry", "exit"])
        self.assertEqual(result.fills[0].metadata["quote_gap_observations"], 1)
        self.assertEqual(result.fills[0].metadata["holding_observations"], 2)
        self.assertEqual(result.fills[1].metadata["quote_gap_observations"], 0)

    def test_prediction_rejects_future_source_but_uses_observed_capture_time(self) -> None:
        future_source = _row(0, 0.4)
        future_source["source_timestamp"] = T0 + timedelta(minutes=1)
        with self.assertRaises(ValueError):
            PredictionMarketBacktester().run(
                [future_source],
                _strategy(),
                mode=PRICE_PROXY_RESEARCH,
            )

        captured_after_source = _row(0, 0.4)
        captured_after_source["response_received_at"] = T0 + timedelta(minutes=1)
        captured_after_source["observed_at"] = T0 + timedelta(minutes=2)
        result = PredictionMarketBacktester().run(
            [captured_after_source],
            _strategy(),
            mode=PRICE_PROXY_RESEARCH,
        )
        self.assertEqual(len(result.equity_curve), 1)

    def test_structured_exit_policy_is_retained_in_curve_evidence(self) -> None:
        policy = {
            "type": "fixed_holding_period",
            "holding_period": 2,
            "assumptions": {"version": "price-proxy-v1"},
        }
        result = PredictionMarketBacktester().run(
            [_row(0, 0.4), _row(1, 0.6), _row(2, 0.62), _row(3, 0.64)],
            _strategy(),
            mode=PRICE_PROXY_RESEARCH,
            exit_policy=policy,
        )
        self.assertEqual(result.equity_curve[0]["exit_policy"], policy)
        self.assertEqual(result.equity_curve[0]["holding_period"], 2)

    def test_holding_period_and_explicit_horizon_are_frozen_in_metadata(self) -> None:
        rows = [_row(index, 0.4 + index * 0.02) for index in range(5)]
        forwarded = run_prediction_research_mode(
            rows,
            _strategy(),
            mode=PRICE_PROXY_RESEARCH,
            holding_period=3,
        )
        self.assertEqual(forwarded.equity_curve[0]["holding_period"], 3)
        self.assertEqual(
            forwarded.equity_curve[0]["exit_policy"]["holding_period"],
            3,
        )
        explicit_horizon = run_prediction_research_mode(
            rows,
            _strategy(),
            mode=PRICE_PROXY_RESEARCH,
            observation_horizon={"count": 3},
        )
        self.assertEqual(explicit_horizon.equity_curve[0]["holding_period"], 3)
        self.assertEqual(
            explicit_horizon.equity_curve[0]["exit_policy"]["holding_period"],
            3,
        )

    def test_research_mode_honors_forwarded_model_document(self) -> None:
        strategy = {
            "version": 1,
            "strategy_id": "model-document-forwarding",
            "market_type": "prediction",
            "family": "probability_mispricing",
            "parameters": {"threshold": 0.10},
            "probability_model": "deterministic-test-v1",
            "resolution_aware": True,
            "resolution_inputs": ["expiry", "settlement"],
        }
        rows = [_row(0, 0.40), _row(1, 0.40), _row(2, 0.40)]
        baseline = run_prediction_research_mode(
            rows,
            strategy,
            mode=PRICE_PROXY_RESEARCH,
        )
        modeled = run_prediction_research_mode(
            rows,
            strategy,
            mode=PRICE_PROXY_RESEARCH,
            model_document={"probability": 0.80},
        )
        self.assertFalse(baseline.fills)
        self.assertTrue(modeled.fills)
        self.assertEqual(
            modeled.equity_curve[0]["evaluation_evidence"]["model"]["probability"],
            0.80,
        )

    def test_prediction_momentum_is_directional_not_probability_calibration(self) -> None:
        signal = evaluate_signal_evaluation(
            _strategy(),
            {"snapshots": [_row(0, 0.4), _row(1, 0.6)]},
        )
        self.assertGreater(signal.score, 0.0)
        self.assertEqual(signal.evidence["assessment_type"], DIRECTIONAL_OOS_TRADING)
        self.assertEqual(
            signal.evidence["probability_calibration"],
            PROBABILITY_CALIBRATION_UNKNOWN,
        )



    def test_structural_manifest_uses_only_complete_same_market_paths(self) -> None:
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
        rows: list[dict[str, object]] = []
        for market_id, count in (
            ("path-1", 1),
            ("path-2", 2),
            ("path-3", 3),
            ("path-4", 4),
        ):
            rows.extend(
                _row(index, 0.40 + 0.05 * index, market_id=market_id)
                for index in range(count)
            )
        manifest = select_prediction_paths(
            rows,
            strategy,
            mode=PRICE_PROXY_RESEARCH,
            holding_period=1,
        )
        self.assertEqual(manifest["required_observations"], 3)
        self.assertEqual(manifest["expected_market_ids"], ["path-1", "path-2", "path-3", "path-4"])
        self.assertEqual(manifest["eligible_market_ids"], ["path-3", "path-4"])
        self.assertEqual(manifest["excluded_market_ids"], ["path-1", "path-2"])
        self.assertEqual(manifest["expected_row_count"], 10)
        self.assertEqual(manifest["eligible_row_count"], 7)
        self.assertEqual(
            manifest["exclusion_reasons"],
            {"INSUFFICIENT_OBSERVATIONS": 2},
        )
        self.assertTrue(manifest["no_imputation"])
        self.assertFalse(manifest["forward_fill"])

    def test_path_choice_is_invariant_to_price_permutation(self) -> None:
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
            _row(0, 0.40, market_id="market-a"),
            _row(0, 0.40, market_id="market-b"),
            _row(1, 0.60, market_id="market-a"),
            _row(1, 0.60, market_id="market-b"),
            _row(2, 0.62, market_id="market-a"),
            _row(2, 0.62, market_id="market-b"),
        ]
        permuted = [dict(row) for row in rows]
        for row, price in zip(permuted, (0.95, 0.05, 0.10, 0.90, 0.20, 0.80)):
            row["yes_mid"] = price
            row["yes_bid"] = max(0.01, price - 0.01)
            row["yes_ask"] = min(0.99, price + 0.01)
            row["no_mid"] = 1.0 - price
            row["no_bid"] = max(0.01, 1.0 - price - 0.01)
            row["no_ask"] = min(0.99, 1.0 - price + 0.01)
        baseline = select_prediction_paths(rows, strategy, mode=PRICE_PROXY_RESEARCH)
        changed = select_prediction_paths(permuted, strategy, mode=PRICE_PROXY_RESEARCH)
        self.assertEqual(baseline["expected_market_ids"], changed["expected_market_ids"])
        self.assertEqual(baseline["eligible_market_ids"], changed["eligible_market_ids"])
        self.assertEqual(baseline["excluded_market_ids"], changed["excluded_market_ids"])
        self.assertEqual(baseline["paths"], changed["paths"])

    def test_replay_depth_gaps_are_execution_gaps_but_missing_books_exclude(self) -> None:
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
        empty_rows = [_row(index, 0.40 + index * 0.1, book=True, market_id="empty") for index in range(3)]
        for row in empty_rows:
            assert isinstance(row["order_book"], dict)
            assert isinstance(row["no_order_book"], dict)
            row["order_book"]["bids"] = []
            row["order_book"]["asks"] = []
            row["no_order_book"]["asks"] = []
        one_sided_rows = [_row(index, 0.40 + index * 0.1, book=True, market_id="one-sided") for index in range(3)]
        for row in one_sided_rows:
            assert isinstance(row["order_book"], dict)
            row["order_book"]["asks"] = []
        missing_rows = [_row(index, 0.40 + index * 0.1, book=True, market_id="missing-book") for index in range(3)]
        for row in missing_rows:
            row.pop("no_order_book")
        manifest = select_prediction_paths(
            empty_rows + one_sided_rows + missing_rows,
            strategy,
            mode=RECORDED_BOOK_REPLAY,
        )
        by_market = {path["market_id"]: path for path in manifest["paths"]}
        self.assertEqual(manifest["eligible_market_ids"], ["empty", "one-sided"])
        self.assertEqual(manifest["excluded_market_ids"], ["missing-book"])
        self.assertTrue(by_market["empty"]["gap_flags"]["execution_gap"])
        self.assertTrue(by_market["empty"]["gap_flags"]["empty_depth"])
        self.assertTrue(by_market["one-sided"]["gap_flags"]["execution_gap"])
        self.assertTrue(by_market["one-sided"]["gap_flags"]["one_sided_depth"])
        self.assertEqual(by_market["missing-book"]["exclusion_reasons"], ["REPLAY_BOOK_REQUIRED"])
if __name__ == "__main__":
    unittest.main()
