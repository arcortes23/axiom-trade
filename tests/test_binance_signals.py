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
            self.assertEqual(fill["run_id"], result.run_id)
            self.assertTrue(fill["signal_id"])
            self.assertEqual(fill["timestamp"], fill["execution_timestamp"])
            self.assertEqual(fill["bar_close"], fill["decision_bar_close"])
            self.assertLessEqual(
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

    def test_paper_depth_vwap_caps_consumption_and_recomputes_partial_fill(self) -> None:
        def snapshot(
            asks: list[tuple[str, str]],
            *,
            available_quantity: str | None = None,
            execution_open: int = 100,
        ) -> BinanceMarketSnapshot:
            execution_bar = {
                "timestamp": T0 + timedelta(hours=2),
                "open": execution_open,
                "high": execution_open + 2,
                "low": execution_open - 1 if execution_open > 1 else execution_open,
                "close": execution_open + 1,
                "volume": 10,
                "closed": True,
                "asks": asks,
            }
            if available_quantity is not None:
                execution_bar["available_quantity"] = available_quantity
            return BinanceMarketSnapshot(
                symbol="BTCUSDT",
                bars=(
                    {
                        "timestamp": T0,
                        "open": execution_open - 1 if execution_open > 1 else execution_open,
                        "high": execution_open + 1,
                        "low": execution_open - 2 if execution_open > 2 else execution_open,
                        "close": execution_open,
                        "volume": 10,
                        "closed": True,
                    },
                    {
                        "timestamp": T0 + timedelta(hours=1),
                        "open": execution_open,
                        "high": execution_open + 3,
                        "low": execution_open - 1 if execution_open > 1 else execution_open,
                        "close": execution_open + 2,
                        "volume": 10,
                        "closed": True,
                    },
                    execution_bar,
                ),
                interval="1h",
            )

        small = CryptoPaperForwardEngine(
            binding(),
            strategy(),
            {"BTCUSDT": snapshot([("100", "1"), ("200", "100")])},
            run_id="depth-small",
            fee_rate=Decimal("0"),
            decision_interval="1h",
        ).run(now=T0 + timedelta(hours=3))
        self.assertEqual(len(small.fills), 1)
        self.assertEqual(small.fills[0]["quantity"], Decimal("0.1"))
        self.assertEqual(small.fills[0]["price"], Decimal("100"))

        # The executable request is 10 units at the next open of 1.  Only the
        # 3+4 units in the explicit asks can be consumed, so the fill is
        # partial and the VWAP excludes every unconsumed level.
        partial = CryptoPaperForwardEngine(
            binding(),
            strategy(),
            {"BTCUSDT": snapshot(
                [("1", "3"), ("2", "4")],
                execution_open=1,
            )},
            run_id="depth-partial",
            fee_rate=Decimal("0"),
            decision_interval="1h",
        ).run(now=T0 + timedelta(hours=3))
        self.assertEqual(len(partial.fills), 1)
        self.assertEqual(partial.fills[0]["quantity"], Decimal("7"))
        self.assertTrue(partial.fills[0]["partial"])
        self.assertEqual(partial.fills[0]["price"], Decimal("11") / Decimal("7"))

        scalar_capped = CryptoPaperForwardEngine(
            binding(),
            strategy(),
            {"BTCUSDT": snapshot(
                [("1", "3"), ("2", "4")],
                available_quantity="5",
                execution_open=1,
            )},
            run_id="depth-scalar-stricter",
            fee_rate=Decimal("0"),
            decision_interval="1h",
        ).run(now=T0 + timedelta(hours=3))
        self.assertEqual(scalar_capped.fills[0]["quantity"], Decimal("5"))
        self.assertTrue(scalar_capped.fills[0]["partial"])
        self.assertEqual(scalar_capped.fills[0]["price"], Decimal("7") / Decimal("5"))

    def test_paper_exit_quantity_is_capped_by_valid_bids(self) -> None:
        rows = [
            {"timestamp": T0, "open": 99, "high": 101, "low": 98, "close": 100, "volume": 10, "closed": True},
            {"timestamp": T0 + timedelta(hours=1), "open": 101, "high": 103, "low": 100, "close": 102, "volume": 10, "closed": True},
            {
                "timestamp": T0 + timedelta(hours=2),
                "open": 103,
                "high": 105,
                "low": 102,
                "close": 104,
                "volume": 10,
                "closed": True,
                "asks": [("103", "1")],
            },
            {"timestamp": T0 + timedelta(hours=3), "open": 94, "high": 96, "low": 93, "close": 95, "volume": 10, "closed": True},
            {
                "timestamp": T0 + timedelta(hours=4),
                "open": 89,
                "high": 91,
                "low": 88,
                "close": 90,
                "volume": 10,
                "closed": True,
                "bids": [("89", "0.03"), ("88", "0.02")],
            },
        ]
        result = CryptoPaperForwardEngine(
            binding(),
            strategy(),
            {"BTCUSDT": BinanceMarketSnapshot(symbol="BTCUSDT", bars=tuple(rows), interval="1h")},
            run_id="depth-exit",
            fee_rate=Decimal("0"),
            decision_interval="1h",
        ).run(now=T0 + timedelta(hours=5))
        exits = [fill for fill in result.fills if fill["intent"] == "EXIT"]
        self.assertEqual(len(exits), 1)
        self.assertEqual(exits[0]["quantity"], Decimal("0.05"))
        self.assertTrue(exits[0]["partial"])
        self.assertEqual(exits[0]["price"], (Decimal("89") * Decimal("0.03") + Decimal("88") * Decimal("0.02")) / Decimal("0.05"))

    def test_paper_invalid_book_levels_add_no_liquidity(self) -> None:
        for index, asks in enumerate((
            [("1", "0"), ("2", "-1"), ("bad", "3")],
            [{"price": "bad", "quantity": "3"}, {"price": "1", "quantity": "0"}],
        )):
            result = CryptoPaperForwardEngine(
                binding(),
                strategy(),
                {"BTCUSDT": BinanceMarketSnapshot(
                    symbol="BTCUSDT",
                    bars=(
                        {"timestamp": T0, "open": 0.9, "high": 1.1, "low": 0.8, "close": 1, "volume": 10, "closed": True},
                        {"timestamp": T0 + timedelta(hours=1), "open": 1, "high": 1.1, "low": 0.9, "close": 1.1, "volume": 10, "closed": True},
                        {"timestamp": T0 + timedelta(hours=2), "open": 1, "high": 1.1, "low": 0.9, "close": 1.05, "volume": 10, "closed": True, "asks": asks},
                    ),
                    interval="1h",
                )},
                run_id=f"depth-invalid-{index}",
                fee_rate=Decimal("0"),
                decision_interval="1h",
            ).run(now=T0 + timedelta(hours=3))
            self.assertFalse(result.fills)
            self.assertEqual(result.no_trade_reasons.get("NO_FILL"), 1)

    def test_paper_forward_now_cutoff_filters_future_bars_and_is_in_run_identity(self) -> None:
        store = AxiomStore(":memory:")
        bound = binding()
        snapshot = bars([100, 102, 104, 95])
        cutoff = T0 + timedelta(hours=3)

        def run(at: datetime) -> object:
            return CryptoPaperForwardEngine(
                bound,
                strategy(),
                {"BTCUSDT": snapshot},
                store=store,
                decision_interval="1h",
            ).run(now=at)

        first = run(cutoff)
        replay = run(cutoff)
        self.assertEqual(first.as_dict(), replay.as_dict())
        self.assertEqual(first.evidence["cutoff"], cutoff.isoformat())
        self.assertEqual(first.metrics["sample_count"], 3)
        self.assertEqual(
            {row["bar_close"] for row in first.observations},
            {(T0 + timedelta(hours=index)).isoformat() for index in (1, 2, 3)},
        )
        self.assertTrue(all(datetime.fromisoformat(row["bar_close"]) <= cutoff for row in first.observations))
        self.assertNotIn((T0 + timedelta(hours=4)).isoformat(), {row["bar_close"] for row in first.observations})
        self.assertTrue(all(datetime.fromisoformat(fill["decision_bar_close"]) <= cutoff for fill in first.fills))

        later = run(T0 + timedelta(hours=4))
        self.assertNotEqual(first.run_id, later.run_id)
        self.assertEqual(later.metrics["sample_count"], 4)
        self.assertTrue(first.fills)
        self.assertTrue(later.fills)
        first_fill_ids = {fill["fill_id"] for fill in first.fills}
        later_fill_ids = {fill["fill_id"] for fill in later.fills}
        self.assertTrue(first_fill_ids.isdisjoint(later_fill_ids))
        self.assertEqual(
            (first.fills[0]["signal_id"], first.fills[0]["execution_timestamp"]),
            (later.fills[0]["signal_id"], later.fills[0]["execution_timestamp"]),
        )
        self.assertEqual(
            first_fill_ids,
            {fill["fill_id"] for fill in replay.fills},
        )
        for forward in (first, later):
            for fill in forward.fills:
                self.assertEqual(fill["run_id"], forward.run_id)
                self.assertTrue(fill["signal_id"])
                self.assertEqual(fill["timestamp"], fill["execution_timestamp"])
        persisted = store.connection.execute(
            "SELECT fill_id, run_id FROM binance_spot_paper_fills ORDER BY run_id, fill_id"
        ).fetchall()
        self.assertEqual(len(persisted), len(first.fills) + len(later.fills))
        self.assertEqual(
            {row["run_id"] for row in persisted},
            {first.run_id, later.run_id},
        )
        self.assertEqual(len({row["fill_id"] for row in persisted}), len(persisted))

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
