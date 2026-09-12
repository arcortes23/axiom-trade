from __future__ import annotations

from datetime import datetime, timedelta, timezone
import unittest

from axiom.backtest.prediction import (
    PRICE_PROXY_RESEARCH,
    RECORDED_BOOK_REPLAY,
    PredictionMarketBacktester,
    PredictionResearchMode,
    run_prediction_research_mode,
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
        with self.assertRaises(ValueError):
            PredictionMarketBacktester().run(
                [_row(0, 0.4), _row(1, 0.6)],
                _strategy(),
                mode=PredictionResearchMode.RECORDED_BOOK_REPLAY,
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


if __name__ == "__main__":
    unittest.main()
