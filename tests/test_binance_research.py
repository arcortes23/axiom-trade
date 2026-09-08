from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
import hashlib
import json
import unittest

from axiom.binance_research import (
    BINANCE_SPOT_TESTNET,
    BinanceCryptoQualificationService,
    CryptoExecutionBinding,
    project_testnet_execution_binding,
)
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
        current = restarted.current_selection()
        self.assertEqual(current["strategy_ref"]["strategy_id"], "trend")
        self.assertEqual(current["strategy_ref"]["strategy_version"], "1")
        self.assertEqual(current["lifecycle_ref"]["candidate_id"], "c1")
        actionable = restarted.actionable_rankings()
        self.assertEqual(actionable[0]["strategy_ref"]["strategy_id"], "trend")
        self.assertEqual(actionable[0]["lifecycle_ref"]["candidate_id"], "c1")
        self.store.connection.execute("UPDATE dataset_catalog SET snapshot_id='mutated' WHERE dataset_id='crypto' AND dataset_version='v1'")
        self.assertEqual(restarted.status()["selection_status"], "STALE")
        self.assertIn("DATASET_EVIDENCE_CHANGED", restarted.status()["reasons"])
    def test_nested_crypto_provenance_and_exact_universe_record_revalidation(self):
        universe_records = [{"asset_id": "BTC", "binance_symbol": "BTCUSDT", "selected": True}]
        self.store.save_dataset("universe:u1", "v1", universe_records)
        self.store.save_dataset_catalog(
            "universe:u1", "v1", provider="fixture", instrument="BTCUSDT",
            market_type=MarketType.CRYPTO_SPOT, timeframe="1h", row_count=1,
            completeness=1.0, quality="HIGH", source_type="HISTORICAL",
            snapshot_id="universe-catalog", metadata={
                "universe_id": "u1", "universe_version": "v1",
                "snapshot_hash": "u-snapshot", "point_in_time": True,
            },
        )
        record = self.store.load_candidate_lifecycle("c1")
        payload = dict(record["payload"])
        for key in (
            "dataset_id", "dataset_version", "timeframe", "source_type",
            "quality", "survivorship", "universe_id", "universe_version",
            "universe_snapshot", "asset_symbol_mapping",
        ):
            payload.pop(key, None)
        payload["crypto_provenance"] = {
            "dataset": {
                "dataset_id": "crypto", "dataset_version": "v1",
                "timeframe": "1h", "source_type": "HISTORICAL",
                "quality": "HIGH", "survivorship_bias": "point_in_time",
            },
            "universe": {
                "universe_id": "u1", "universe_version": "v1",
                "snapshot_hash": "u-snapshot", "dataset_id": "universe:u1",
            },
            "selected_symbol": "BTCUSDT",
        }
        self.store.connection.execute(
            "UPDATE candidate_lifecycle SET payload_json=? WHERE candidate_id='c1'",
            (json.dumps(payload, sort_keys=True),),
        )
        self.store.connection.commit()
        service = BinanceCryptoQualificationService(self.store, clock=lambda: T0)
        result = service.rank_and_select({"BTCUSDT": True}, now=T0)
        self.assertEqual(result["selection_status"], "CURRENT")
        self.assertEqual(result["selected"]["binding"]["dataset_id"], "crypto")
        self.assertEqual(result["selected"]["binding"]["universe_snapshot"], "u-snapshot")
        self.assertEqual(result["selected"]["strategy_ref"]["strategy_id"], "trend")

        self.store.connection.execute(
            "UPDATE datasets SET payload_json=? WHERE dataset_id='universe:u1' AND version='v1'",
            (json.dumps([{"asset_id": "BTC", "binance_symbol": "ETHUSDT", "selected": True}]),),
        )
        self.store.connection.commit()
        row = service.qualify_all({"BTCUSDT": True}, now=T0)[0]
        self.assertFalse(row["qualified"])
        self.assertIn("CRYPTO_UNIVERSE_MEMBERSHIP_MISMATCH", row["reasons"])
        self.assertEqual(service.status()["selection_status"], "STALE")
        self.assertIn("UNIVERSE_EVIDENCE_CHANGED", service.status()["reasons"])

    def test_missing_dataset_catalog_rejects_selection(self):
        service = BinanceCryptoQualificationService(self.store, clock=lambda: T0)
        service.rank_and_select({"BTCUSDT": True}, now=T0)
        self.store.connection.execute(
            "DELETE FROM dataset_catalog WHERE dataset_id='crypto' AND dataset_version='v1'"
        )
        status = service.status()
        self.assertEqual(status["selection_status"], "STALE")
        self.assertIn("DATASET_NOT_FOUND", status["reasons"])

    def test_dataset_record_mutation_makes_selection_stale(self):
        service = BinanceCryptoQualificationService(self.store, clock=lambda: T0)
        service.rank_and_select({"BTCUSDT": True}, now=T0)
        self.store.connection.execute(
            "UPDATE datasets SET payload_json=? WHERE dataset_id='crypto' AND version='v1'",
            (json.dumps([{"timestamp": T0.isoformat(), "symbol": "ETHUSDT"}]),),
        )
        status = service.status()
        self.assertEqual(status["selection_status"], "STALE")
        self.assertIn("DATASET_EVIDENCE_CHANGED", status["reasons"])

    def test_projection_round_trip_is_deterministic_and_non_sharing(self):
        source = CryptoExecutionBinding(
            candidate_id="c1", symbol="BTCUSDT", frozen_hash="frozen",
            strategy_hash="strategy", model_hash="model", config_hash="config",
            plan_hash="plan", universe_id="u1", universe_version="v1",
            universe_snapshot="u-snapshot", asset_symbol_mapping={"BTC": "BTCUSDT"},
            dataset_id="crypto", dataset_version="v1", timeframe="1h",
            source="HISTORICAL", quality="HIGH", survivorship="point_in_time",
            environment="PAPER", venue="BINANCE_SPOT", adapter_version="fixture-1",
        )
        mapping = source.as_dict()
        mapping["telemetry"] = {"last_seen": "mutated"}
        successor, source_hash = project_testnet_execution_binding(mapping)
        self.assertEqual(source_hash, source.binding_hash)
        self.assertEqual(successor.environment, BINANCE_SPOT_TESTNET)
        self.assertEqual(successor.venue, "BINANCE_SPOT")
        self.assertEqual(dict(successor.asset_symbol_mapping), {"BTC": "BTCUSDT"})
        with self.assertRaises(TypeError):
            successor.asset_symbol_mapping["BTC"] = "ETHUSDT"
        mapping["asset_symbol_mapping"]["BTC"] = "ETHUSDT"
        self.assertEqual(dict(successor.asset_symbol_mapping), {"BTC": "BTCUSDT"})
        repeat, repeat_hash = project_testnet_execution_binding(source)
        self.assertEqual(repeat_hash, source_hash)
        self.assertEqual(repeat.as_dict(), successor.as_dict())
    def test_projection_rejects_non_paper_and_wrapped_environment_provenance(self):
        source = CryptoExecutionBinding(
            candidate_id="c1", symbol="BTCUSDT", frozen_hash="frozen",
            strategy_hash="strategy", model_hash="model", config_hash="config",
            plan_hash="plan", universe_id="u1", universe_version="v1",
            universe_snapshot="u-snapshot", asset_symbol_mapping={"BTC": "BTCUSDT"},
            dataset_id="crypto", dataset_version="v1", timeframe="1h",
            source="HISTORICAL", quality="HIGH", survivorship="point_in_time",
            environment="PAPER", venue="BINANCE_SPOT", adapter_version="fixture-1",
        )
        for environment in ("LIVE", "BINANCE_SPOT_LIVE", "TESTNET", "BINANCE_SPOT_TESTNET", "paper", " PAPER "):
            with self.subTest(environment=environment):
                with self.assertRaisesRegex(ValueError, "SOURCE_ENVIRONMENT_INVALID"):
                    project_testnet_execution_binding({**source.as_dict(), "environment": environment})
        with self.assertRaisesRegex(ValueError, "SOURCE_ENVIRONMENT_INVALID"):
            project_testnet_execution_binding(replace(source, environment="BINANCE_SPOT_TESTNET"))


        wrapped = source.as_dict()
        wrapped["binding"] = source.as_dict()
        wrapped["environment"] = "LIVE"
        with self.assertRaisesRegex(ValueError, "SOURCE_ENVIRONMENT_INVALID"):
            project_testnet_execution_binding(wrapped)

        contradictory = source.as_dict()
        contradictory["binding"] = source.as_dict()
        contradictory["provenance"] = {"environment": "LIVE"}
        with self.assertRaisesRegex(ValueError, "SOURCE_ENVIRONMENT_INVALID"):
            project_testnet_execution_binding(contradictory)

        selected = {"selection": {"binding": source.as_dict(), "execution_environment": "TESTNET"}}
        with self.assertRaisesRegex(ValueError, "SOURCE_ENVIRONMENT_INVALID"):
            project_testnet_execution_binding(selected)


    def test_projection_rejects_whitespace_and_malformed_mapping(self):
        source = CryptoExecutionBinding(
            candidate_id="c1", symbol="BTCUSDT", frozen_hash="frozen",
            strategy_hash="strategy", model_hash="model", config_hash="config",
            plan_hash="plan", universe_id="u1", universe_version="v1",
            universe_snapshot="u-snapshot", asset_symbol_mapping={"BTC": "BTCUSDT"},
            dataset_id="crypto", dataset_version="v1", timeframe="1h",
            source="HISTORICAL", quality="HIGH", survivorship="point_in_time",
            environment="PAPER", venue="BINANCE_SPOT", adapter_version="fixture-1",
        ).as_dict()
        source["candidate_id"] = "   "
        with self.assertRaises(ValueError):
            project_testnet_execution_binding(source)
        source["candidate_id"] = "c1"
        source["asset_symbol_mapping"] = {"BTC": None}
        with self.assertRaises(ValueError):
            project_testnet_execution_binding(source)

    def test_probe_rows_do_not_influence_research_ranking(self):
        service = BinanceCryptoQualificationService(self.store, clock=lambda: T0)
        baseline = service.rank_and_select({"BTCUSDT": True}, now=T0)
        self.store.connection.execute(
            "CREATE TABLE binance_testnet_probe (candidate_id TEXT, symbol TEXT, score REAL)"
        )
        self.store.connection.execute(
            "INSERT INTO binance_testnet_probe VALUES ('c1', 'BTCUSDT', 999999.0)"
        )
        self.store.connection.commit()
        after_probe = service.rank_and_select({"BTCUSDT": True}, now=T0)
        self.assertEqual(
            baseline["selected"]["qualification_hash"],
            after_probe["selected"]["qualification_hash"],
        )
        self.assertEqual(after_probe["fallbacks"], [])


if __name__ == "__main__":
    unittest.main()
