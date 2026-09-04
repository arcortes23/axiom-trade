from __future__ import annotations

from datetime import datetime, timedelta, timezone
import unittest

from axiom.autonomous import AutonomousResearchConfig, AutonomousResearchProcessor
from axiom.domain import MarketType
from axiom.experiment_plan import ExperimentPlan, ExperimentPlanError
from axiom.lifecycle import CandidateStage, PromotionCriteria
from axiom.research_bus import DurableResearchBus
from axiom.storage import AxiomStore


UTC = timezone.utc
T0 = datetime(2025, 1, 1, tzinfo=UTC)


def crypto_plan(**overrides: object) -> dict[str, object]:
    plan: dict[str, object] = {
        "hypothesis_id": "crypto-scale",
        "market_type": "crypto_spot",
        "template": "momentum",
        "target": {"instrument": "BTC/USDT"},
        "dataset_id": "crypto:BTCUSDT",
        "dataset_version": "btc-v1",
        "universe": {
            "universe_id": "spot-major",
            "universe_version": "spot-major-v1",
            "instruments": ["BTC/USDT"],
            "methodology": "fixed-symbol allowlist",
        },
        "allowed_features": ["timestamp", "symbol", "open", "high", "low", "close", "volume"],
        "parameters": {"lookback": {"min": 5, "max": 10, "step": 5}, "threshold": [0.02]},
        "regime_restrictions": {"allowed_regimes": ["rising"]},
        "metrics": ["total_return", "max_drawdown", "expectancy"],
        "min_samples": 1,
        "max_variants": 2,
        "paper_only": True,
    }
    plan.update(overrides)
    return plan


def crypto_rows(version: str = "btc-v1") -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for index in range(12):
        price = 100.0 + index
        rows.append(
            {
                "timestamp": (T0 + timedelta(hours=index)).isoformat(),
                "symbol": "BTC/USDT",
                "open": price,
                "high": price + 1.0,
                "low": price - 1.0,
                "close": price + 0.5,
                "volume": 100.0,
                "regime": "rising",
                "dataset_version": version,
            }
        )
    return rows


class CryptoAutonomyScalabilityTests(unittest.TestCase):
    def test_bounded_crypto_plan_accepts_existing_deterministic_family(self) -> None:
        plan = ExperimentPlan.from_mapping(crypto_plan())
        self.assertIs(plan.market_type, MarketType.CRYPTO_SPOT)
        self.assertEqual(plan.experiment_family, "momentum")
        self.assertEqual(plan.universe["universe_version"], "spot-major-v1")
        self.assertEqual(len(plan.variants()), 2)
        self.assertEqual(plan, ExperimentPlan.from_mapping(plan.as_dict()))

    def test_crypto_plan_rejects_unsupported_family_and_missing_versioned_data(self) -> None:
        with self.assertRaisesRegex(ExperimentPlanError, "UNSUPPORTED_STRATEGY_FAMILY"):
            ExperimentPlan.from_mapping(crypto_plan(template="probability_mispricing"))
        with self.assertRaisesRegex(ExperimentPlanError, "INSUFFICIENT_DATA"):
            ExperimentPlan.from_mapping(crypto_plan(dataset_version="latest"))
        with self.assertRaisesRegex(ExperimentPlanError, "INSUFFICIENT_DATA"):
            ExperimentPlan.from_mapping(crypto_plan(universe=None))

    def test_crypto_worker_uses_backtest_only_and_keeps_locked_holdout_out_of_evidence(self) -> None:
        with AxiomStore(":memory:") as store:
            store.save_dataset("crypto:BTCUSDT", "btc-v1", crypto_rows())
            bus = DurableResearchBus(store)
            proposal = {
                "proposal_id": "crypto-scale",
                "statement": "A deterministic crypto signal is testable.",
                "source": "immutable fixture",
                "tests": ["chronological train-validation backtest"],
                "dataset_version": "btc-v1",
                "time_split": "train-validation-holdout",
                "paper_only": True,
                "experiment_plan": crypto_plan(),
            }
            bus.submit_hypothesis(proposal, available_at=T0, dedupe_key="crypto-scale")
            processor = AutonomousResearchProcessor(
                store,
                bus=bus,
                config=AutonomousResearchConfig(
                    max_plan_variants=8,
                    max_children_per_parent=0,
                    promotion_criteria=PromotionCriteria(
                        min_independent_samples=0,
                        min_trades=0,
                        max_drawdown=1.0,
                        min_expectancy=-1.0,
                        min_confidence_lower_bound=-1.0,
                        min_stability=0.0,
                        min_calibration=0.0,
                        min_liquidity=0.0,
                        min_forward_duration_seconds=0.0,
                        min_regimes=0,
                    ),
                ),
                clock=lambda: T0,
            )
            plan = ExperimentPlan.from_mapping(crypto_plan())
            _, split = processor._load_split(plan)
            self.assertEqual(split.holdout[-1]["close"], 111.5)
            cycle = processor.process_pending(now=T0)
            self.assertEqual(cycle.completed, 1)
            self.assertTrue(cycle.results[0]["accepted"])
            self.assertTrue(cycle.results[0]["research_only"])
            self.assertEqual(store.load_forward_tests(), [])
            lifecycle = store.load_candidate_lifecycle(limit=None)
            self.assertEqual(lifecycle[0]["stage"], CandidateStage.FROZEN.value)
            self.assertTrue(cycle.results[0]["research_only"])
            self.assertFalse(lifecycle[0]["payload"]["holdout_used"])
            self.assertNotIn("111.5", str(lifecycle[0]["payload"]["train"]))


if __name__ == "__main__":
    unittest.main()
