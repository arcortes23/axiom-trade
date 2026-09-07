from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
import unittest

from axiom.binance_market import BinanceMarketSnapshot
from axiom.binance_research import CryptoExecutionBinding
from axiom.binance_signals import BinanceSignalEngine, CryptoPaperForwardEngine
from axiom.domain import MarketType, OHLCVBar
from axiom.storage import AxiomStore
from axiom.strategy import StrategyDefinition, validate_strategy

UTC = timezone.utc
T0 = datetime(2026, 1, 1, tzinfo=UTC)


def binding(symbol: str = "BTCUSDT", candidate: str = "candidate-a") -> CryptoExecutionBinding:
    return CryptoExecutionBinding(
        candidate_id=candidate,
        symbol=symbol,
        frozen_hash="frozen-" + candidate,
        strategy_hash="strategy-" + candidate,
        model_hash="model-" + candidate,
        config_hash="config-" + candidate,
        plan_hash="plan-" + candidate,
        universe_id="u",
        universe_version="u-1",
        universe_snapshot="u-snapshot",
        asset_symbol_mapping={symbol: symbol},
        dataset_id="bars",
        dataset_version="bars-1",
        timeframe="1h",
        source="test",
        quality="HIGH",
        survivorship="SURVIVORSHIP_BIAS_PRESENT",
        environment="PAPER",
        venue="BINANCE_SPOT",
        adapter_version="test",
    )


def strategy() -> StrategyDefinition:
    return validate_strategy(
        {
            "version": 1,
            "market_type": MarketType.CRYPTO_SPOT.value,
            "family": "momentum",
            "parameters": {"lookback": 1, "threshold": 0.01},
        }
    )


def bars(closes: list[int], *, symbol: str = "BTCUSDT", closed: bool = True) -> BinanceMarketSnapshot:
    rows = []
    for index, close in enumerate(closes):
        opening = T0 + timedelta(hours=index)
        rows.append(
            {
                "timestamp": opening,
                "open": close - 1 if close > 1 else close,
                "high": close + 1,
                "low": close - 1 if close > 1 else close,
                "close": close,
                "volume": 10,
                "closed": closed,
            }
        )
    return BinanceMarketSnapshot(symbol=symbol, bars=tuple(rows), interval="1h")


class BinanceSignalsTests(unittest.TestCase):
    def test_only_closed_bar_and_interval_dedup(self) -> None:
        bound = binding()
        engine = BinanceSignalEngine(bound, strategy(), decision_interval="1h")
        snapshot = bars([100, 102])
        self.assertIsNone(engine.evaluate(snapshot, now=T0 + timedelta(hours=2) - timedelta(seconds=1)))
        first = engine.evaluate(snapshot, now=T0 + timedelta(hours=2))
        self.assertIsNotNone(first)
        self.assertEqual(first.intent, "ENTRY")
        self.assertIsNone(engine.evaluate(snapshot, now=T0 + timedelta(hours=3)))
        self.assertEqual(engine.last_no_trade_reason, "DUPLICATE_INTERVAL")
        replay = BinanceSignalEngine(bound, strategy(), decision_interval="1h", deduplicate=False).evaluate(snapshot, now=T0 + timedelta(hours=2))
        self.assertEqual(first.signal_id, replay.signal_id)

    def test_exit_identity_differs_and_origin_policy_survives_new_winner(self) -> None:
        bound = binding()
        position = {
            "BTCUSDT": {
                "quantity": Decimal("0.1"),
                "entry_time": T0,
                "exit_policy": {"max_holding_bars": 99, "strategy": strategy().to_dict()},
            }
        }
        engine = BinanceSignalEngine(bound, strategy(), decision_interval="1h", positions=position)
        snapshot = bars([100, 102, 95])
        signal = engine.evaluate(snapshot, now=T0 + timedelta(hours=3))
        self.assertIsNotNone(signal)
        self.assertEqual(signal.intent, "EXIT")
        entry_id = signal.make_signal_id(bound.candidate_id, bound.binding_hash, "BTCUSDT", T0 + timedelta(hours=3), "1h", "ENTRY")
        self.assertNotEqual(entry_id, signal.signal_id)

    def test_paper_forward_is_next_open_costed_and_restart_deterministic(self) -> None:
        store = AxiomStore(":memory:")
        bound = binding()
        snapshot = bars([100, 102, 104, 95, 90])
        engine = CryptoPaperForwardEngine(
            bound,
            strategy(),
            {"BTCUSDT": snapshot},
            store=store,
            run_id="paper-1",
            initial_cash=Decimal("1000"),
            fee_rate=Decimal("0.001"),
            decision_interval="1h",
        )
        result = engine.run()
        self.assertEqual(result.evidence["binding_hashes"], result.evidence["binding_hashes"])
        self.assertFalse(result.metrics["holdout_used"])
        self.assertGreaterEqual(result.metrics["sample_count"], 1)
        self.assertGreaterEqual(result.metrics["trade_count"], 1)
        self.assertTrue(any(fill["intent"] == "ENTRY" for fill in result.fills))
        self.assertTrue(any(fill["intent"] == "EXIT" for fill in result.fills))
        for fill in result.fills:
            self.assertEqual(fill["timestamp"], fill["execution_timestamp"])
            self.assertEqual(fill["bar_close"], fill["decision_bar_close"])
            self.assertLess(
                datetime.fromisoformat(fill["decision_bar_close"]),
                datetime.fromisoformat(fill["execution_timestamp"]),
            )
        restarted = CryptoPaperForwardEngine(
            bound, strategy(), {"BTCUSDT": snapshot}, store=store, run_id="paper-1", decision_interval="1h"
        ).run()
        self.assertEqual(result.as_dict(), restarted.as_dict())
        table_names = {
            row[0]
            for row in store.connection.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
        self.assertTrue({
            "binance_spot_paper_runs",
            "binance_spot_paper_observations",
            "binance_spot_paper_fills",
            "binance_spot_paper_positions",
            "binance_spot_paper_evidence",
        } <= table_names)

    def test_holdout_is_not_an_automated_observation(self) -> None:
        store = AxiomStore(":memory:")
        bound = binding()
        snapshot = bars([100, 102, 104, 106])
        holdout = {"BTCUSDT": [snapshot.bars[-1]]}
        result = CryptoPaperForwardEngine(
            bound,
            strategy(),
            {"BTCUSDT": snapshot},
            run_id="paper-holdout",
            decision_interval="1h",
            holdout=holdout,
        ).run()
        self.assertFalse(result.evidence["holdout_used"])
        self.assertNotIn((T0 + timedelta(hours=3)).isoformat(), {row["bar_close"] for row in result.observations})


if __name__ == "__main__":
    unittest.main()
