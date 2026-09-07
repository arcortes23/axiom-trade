from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import unittest

from axiom.binance_research import BinanceCryptoQualificationService
from axiom.domain import MarketType
from axiom.storage import AxiomStore


T0 = datetime(2026, 1, 2, 12, tzinfo=timezone.utc)


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


class BinanceResearchContractTests(unittest.TestCase):
    def setUp(self):
        self.store = AxiomStore(":memory:")
        strategy = {"market_type": "crypto_spot", "family": "trend", "parameters": {"lookback": 5}}
        self.store.save_strategy("trend", strategy, version="1")
        self.strategy_hash = digest(strategy)
        self.store.save_dataset("crypto", "v1", [{"timestamp": T0.isoformat(), "symbol": "BTCUSDT"}])
        self.store.save_dataset_catalog(
            "crypto", "v1", provider="fixture", instrument="BTCUSDT", market_type=MarketType.CRYPTO_SPOT,
            timeframe="1h", row_count=1, completeness=1.0, quality="HIGH", source_type="HISTORICAL",
            snapshot_id="dataset-snapshot", metadata={"survivorship": "point_in_time", "universe_id": "u1", "universe_version": "v1", "snapshot_hash": "u-snapshot"},
        )
        payload = {
            "candidate_id": "c1", "market_type": "crypto_spot", "strategy_id": "trend", "strategy_version": "1",
            "strategy_hash": self.strategy_hash, "model_hash": "model-v1", "config_hash": "config-v1", "plan_hash": "plan-v1", "frozen_hash": "frozen-v1",
            "dataset_id": "crypto", "dataset_version": "v1", "timeframe": "1h", "source_type": "HISTORICAL", "quality": "HIGH", "survivorship": "point_in_time",
            "universe_id": "u1", "universe_version": "v1", "universe_snapshot": "u-snapshot", "asset_symbol_mapping": {"BTC": "BTCUSDT"},
            "environment": "PAPER", "venue": "BINANCE_SPOT", "adapter_version": "fixture-1", "family": "trend", "root_lineage": "root-1",
            "net_expectancy_after_costs": 0.12, "max_drawdown": 0.10, "sample_count": 30, "trade_count": 3,
            "walk_forward_consistency": 0.80, "neighbor_stability": 0.80, "cost_slippage_stress": 0.02,
            "forward_paper_evidence": True, "locked_holdout_used": False, "holdout_used": False,
        }
        self.store.save_candidate_lifecycle("c1", "IDEA", {"candidate_id": "c1"})
        prior = "IDEA"
        for stage, marker in (("SCHEMA_VALIDATED", "schema_valid"), ("BACKTESTED", "backtest_complete"), ("VALIDATED", "validation_complete"), ("ROBUSTNESS_CHECKED", "robustness_passed"), ("FROZEN", "frozen")):
            body = dict(payload)
            body[marker] = True
            self.store.save_candidate_lifecycle("c1", stage, body, from_stage=prior)
            prior = stage

    def tearDown(self):
        self.store.close()

    def test_qualification_and_selection_are_crypto_only_and_persisted(self):
        service = BinanceCryptoQualificationService(self.store, clock=lambda: T0)
        rows = service.qualify_all({"BTCUSDT": True}, now=T0)
        self.assertEqual(len(rows), 1)
        self.assertTrue(rows[0]["qualified"], rows[0])
        self.assertNotIn("prediction", json.dumps(rows[0]).lower())
        result = service.rank_and_select({"BTCUSDT": True}, now=T0)
        self.assertEqual(result["selection_status"], "CURRENT")
        self.assertEqual(result["selected"]["candidate_id"], "c1")
        self.assertTrue(result["selected"]["binding_hash"])
        self.assertTrue(result["selected"]["qualification_hash"])
        self.assertEqual(service.status()["selection_status"], "CURRENT")
        self.assertEqual(len(service.actionable_rankings()), 1)

    def test_restart_and_immutable_catalog_mutation_make_selection_stale(self):
        service = BinanceCryptoQualificationService(self.store, clock=lambda: T0)
        service.rank_and_select({"BTCUSDT": True}, now=T0)
        restarted = BinanceCryptoQualificationService(self.store, clock=lambda: T0)
        self.assertEqual(restarted.status()["selection_status"], "CURRENT")
        self.store.connection.execute("UPDATE dataset_catalog SET snapshot_id='mutated' WHERE dataset_id='crypto' AND dataset_version='v1'")
        self.assertEqual(restarted.status()["selection_status"], "STALE")
        self.assertIn("DATASET_EVIDENCE_CHANGED", restarted.status()["reasons"])


if __name__ == "__main__":
    unittest.main()
