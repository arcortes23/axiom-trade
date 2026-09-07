from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import json
import unittest

from axiom.binance_auto import BinanceAutonomousWorker
from axiom.binance_dev import PaperBinanceSpotVenue
from axiom.binance_execution import BinanceExecutionService, ENABLE_CONFIRMATION
from axiom.binance_market import BinanceMarketSnapshot
from axiom.binance_operator import BinanceCanaryControlPlane
from axiom.binance_research import (
    BinanceCryptoQualificationService,
    CryptoExecutionBinding,
    CryptoQualificationPolicy,
)
from axiom.binance_signals import BinanceSignalEngine, CryptoPaperForwardEngine
from axiom.crypto_universe import UniverseSnapshot
from axiom.dashboard import DashboardData
from axiom.domain import CryptoTicker, MarketType, OHLCVBar, OrderBookLevel, OrderBookSnapshot
from axiom.storage import AxiomStore


UTC = timezone.utc
T0 = datetime(2026, 1, 1, tzinfo=UTC)


def _digest(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    ).hexdigest()


def _strategy(strategy_id: str) -> dict[str, object]:
    return {
        "version": 1,
        "market_type": "crypto_spot",
        "family": "momentum",
        "parameters": {"lookback": 1, "threshold": 0.01},
        "strategy_id": strategy_id,
        "name": f"Paper {strategy_id}",
    }


def _bar_payload(symbol: str, start: int) -> tuple[dict[str, object], ...]:
    del symbol  # The snapshot carries the symbol; bar payloads remain exchange-like records.
    return tuple(
        {
            "timestamp": (T0 + timedelta(hours=index)).isoformat(),
            "open": start + index * 2,
            "high": start + index * 2 + 2,
            "low": start + index * 2 - 1,
            "close": start + index * 2 + 1,
            "volume": 100,
            "closed": True,
        }
        for index in range(5)
    )


def _stored_bars(start: int) -> tuple[OHLCVBar, ...]:
    return tuple(
        OHLCVBar(
            T0 + timedelta(hours=index),
            start + index * 2,
            start + index * 2 + 2,
            start + index * 2 - 1,
            start + index * 2 + 1,
            100,
        )
        for index in range(5)
    )


def _market_snapshot(symbol: str, start: int) -> BinanceMarketSnapshot:
    rows = _bar_payload(symbol, start)
    buy_price = Decimal(start + 4)
    ticker = CryptoTicker(
        T0 + timedelta(hours=2),
        symbol,
        float(buy_price),
        bid=float(buy_price - 1),
        ask=float(buy_price),
    )
    book = OrderBookSnapshot(
        T0 + timedelta(hours=2),
        (OrderBookLevel(float(buy_price - 1), 1),),
        (OrderBookLevel(float(buy_price), 1),),
    )
    return BinanceMarketSnapshot(
        symbol,
        asset_symbol=symbol.removesuffix("USDT"),
        bars=rows,
        ticker=ticker,
        book=book,
        exchange_info={"symbol": symbol, "status": "TRADING", "isSpotTradingAllowed": True},
        tradable=True,
        new_entry_allowed=True,
        selected=True,
        observed_at=T0 + timedelta(hours=2),
        ticker_fresh=True,
        book_fresh=True,
        depth={"fresh": True, "bid_levels": 1, "ask_levels": 1},
        spread=1.0,
        fill_evidence={
            "buy": {"price": str(buy_price), "complete": True},
            "sell": {"price": str(buy_price + 6), "complete": True},
        },
        universe_id="u-paper",
        universe_version="v1",
        snapshot_hash="u-snapshot",
        interval="1h",
        dataset_version="v1",
        source="fixture",
        quality="HIGH",
    )


class _StaticCollector:
    def __init__(self, records: list[BinanceMarketSnapshot]) -> None:
        self.records = records

    def collect(self, *_args: object, **_kwargs: object) -> list[BinanceMarketSnapshot]:
        return list(self.records)


class _OriginAwareSignalEngine:
    """Delegate to the real signal engine while restoring durable entry time."""

    def __init__(self, engine: BinanceSignalEngine, entry_time: datetime) -> None:
        self.engine = engine
        self.entry_time = entry_time

    @property
    def no_trade_reason(self) -> str:
        return self.engine.no_trade_reason

    def evaluate(
        self,
        snapshot: BinanceMarketSnapshot,
        *,
        now: datetime | None = None,
        positions: dict[str, object] | None = None,
    ) -> object:
        augmented = {
            symbol: {**dict(position), "entry_time": self.entry_time}
            for symbol, position in (positions or {}).items()
        }
        return self.engine.evaluate(snapshot, now=now, positions=augmented)




class BinancePaperIntegrationTests(unittest.TestCase):
    def test_bounded_multisymbol_paper_vertical(self) -> None:
        now = {"value": T0 + timedelta(hours=2)}
        store = AxiomStore(":memory:")
        self.addCleanup(store.close)

        symbols = (("cand-btc", "BTCUSDT", 100), ("cand-eth", "ETHUSDT", 50))
        strategy_docs = {candidate: _strategy(f"momentum-{symbol.lower()}") for candidate, symbol, _ in symbols}
        snapshots = {symbol: _market_snapshot(symbol, start) for _, symbol, start in symbols}
        bindings: dict[str, CryptoExecutionBinding] = {}

        # Persist exact closed bars and immutable dataset provenance for both pairs.
        for candidate, symbol, start in symbols:
            dataset_id = f"crypto-{symbol.lower()}"
            stored_bars = _stored_bars(start)
            store.save_bars(symbol, stored_bars, dataset_id=dataset_id, dataset_version="v1")
            store.save_dataset(dataset_id, "v1", list(_bar_payload(symbol, start)))
            store.save_dataset_catalog(
                dataset_id,
                "v1",
                provider="fixture",
                instrument=symbol,
                market_type=MarketType.CRYPTO_SPOT,
                timeframe="1h",
                start_timestamp=T0,
                end_timestamp=T0 + timedelta(hours=5),
                row_count=len(stored_bars),
                completeness=1.0,
                quality="HIGH",
                source_type="HISTORICAL",
                snapshot_id=f"dataset-snapshot-{symbol}",
                metadata={
                    "universe_id": "u-paper",
                    "universe_version": "v1",
                    "universe_snapshot": "u-snapshot",
                },
            )
            strategy = strategy_docs[candidate]
            store.save_strategy(strategy["strategy_id"], strategy, version="1")
            bindings[symbol] = CryptoExecutionBinding(
                candidate_id=candidate,
                symbol=symbol,
                frozen_hash=f"{candidate}-frozen",
                strategy_hash=_digest(strategy),
                model_hash=f"{candidate}-model",
                config_hash=f"{candidate}-config",
                plan_hash=f"{candidate}-plan",
                universe_id="u-paper",
                universe_version="v1",
                universe_snapshot="u-snapshot",
                asset_symbol_mapping={symbol.removesuffix("USDT"): symbol},
                dataset_id=dataset_id,
                dataset_version="v1",
                timeframe="1h",
                source="HISTORICAL",
                quality="HIGH",
                survivorship="point_in_time",
                environment="PAPER",
                venue="BINANCE_SPOT",
                adapter_version="fixture-1",
            )

        # Run the actual forward engine first; qualification consumes its evidence.
        forward = CryptoPaperForwardEngine(
            bindings=bindings,
            strategies={symbol: strategy_docs[candidate] for candidate, symbol, _ in symbols},
            snapshots=snapshots,
            store=store,
            run_id="paper-vertical-forward",
            initial_cash=Decimal("1000"),
            fee_rate=Decimal("0"),
            max_holding_bars=1,
            decision_interval="1h",
            clock=lambda: T0,
        ).run(now=T0 + timedelta(hours=10))
        self.assertEqual(forward.metrics["sample_count"], 10)
        self.assertEqual(forward.metrics["trade_count"], 2)
        self.assertGreater(Decimal(str(forward.metrics["net_expectancy"])), 0)

        # Use only public lifecycle APIs and advance candidates through every stage.
        for candidate, symbol, _ in symbols:
            strategy = strategy_docs[candidate]
            metrics = {
                key: (str(value) if isinstance(value, Decimal) else value)
                for key, value in forward.metrics.items()
                if key not in {"trade_pnl"}
            }
            metrics["cost_slippage_stress"] = metrics["cost_stress_expectancy"]
            payload: dict[str, object] = {
                "candidate_id": candidate,
                "market_type": "crypto_spot",
                "strategy_id": strategy["strategy_id"],
                "strategy_version": "1",
                "strategy_hash": _digest(strategy),
                "strategy_document": strategy,
                "model_hash": f"{candidate}-model",
                "config_hash": f"{candidate}-config",
                "plan_hash": f"{candidate}-plan",
                "frozen_hash": f"{candidate}-frozen",
                "dataset_id": f"crypto-{symbol.lower()}",
                "dataset_version": "v1",
                "timeframe": "1h",
                "source_type": "HISTORICAL",
                "quality": "HIGH",
                "survivorship": "point_in_time",
                "universe_id": "u-paper",
                "universe_version": "v1",
                "universe_snapshot": "u-snapshot",
                "asset_symbol_mapping": {symbol.removesuffix("USDT"): symbol},
                "environment": "PAPER",
                "venue": "BINANCE_SPOT",
                "adapter_version": "fixture-1",
                "family": "momentum",
                "root_lineage": candidate,
                "metrics_by_symbol": {symbol: metrics},
                **metrics,
                "forward_paper_evidence": True,
                "locked_holdout_used": False,
                "holdout_used": False,
            }
            previous: str | None = None
            for stage in (
                "IDEA",
                "SCHEMA_VALIDATED",
                "BACKTESTED",
                "VALIDATED",
                "ROBUSTNESS_CHECKED",
                "FROZEN",
            ):
                stage_payload = payload if stage == "FROZEN" else {"candidate_id": candidate, "stage_marker": stage}
                store.save_candidate_lifecycle(
                    candidate,
                    stage,
                    stage_payload,
                    from_stage=previous,
                    timestamp=T0 + timedelta(seconds=len(stage)),
                )
                previous = stage

        policy = CryptoQualificationPolicy(min_samples=2, min_trades=1, max_fallbacks=1)
        qualification = BinanceCryptoQualificationService(store, policy=policy, clock=lambda: T0)
        none = qualification.rank_and_select({"BTCUSDT": False, "ETHUSDT": False}, limit=1, now=T0)
        self.assertIsNone(none["selected"])
        self.assertEqual(none["selection_status"], "NONE")
        ranking = qualification.rank_and_select({"BTCUSDT": True, "ETHUSDT": True}, limit=1, now=T0)
        self.assertEqual(ranking["selection_status"], "CURRENT")
        self.assertEqual(ranking["selected"]["candidate_id"], "cand-btc")
        self.assertEqual(
            [(row["candidate_id"], row["symbol"], row["rank"]) for row in ranking["rankings"]],
            [("cand-btc", "BTCUSDT", 1), ("cand-eth", "ETHUSDT", 2)],
        )
        self.assertEqual(
            qualification.rank_and_select({"BTCUSDT": True, "ETHUSDT": True}, limit=1, now=T0)["ranking_run_id"],
            ranking["ranking_run_id"],
        )
        self.assertEqual(qualification.status()["selection_status"], "CURRENT")
        store.connection.execute(
            "UPDATE dataset_catalog SET snapshot_id='mutated' WHERE dataset_id=? AND dataset_version='v1'",
            ("crypto-btcusdt",),
        )
        self.assertEqual(qualification.status()["selection_status"], "STALE")
        # Restore the immutable fixture only through the same catalog API used to seed it.
        store.connection.execute(
            "DELETE FROM dataset_catalog WHERE dataset_id=? AND dataset_version='v1'",
            ("crypto-btcusdt",),
        )
        store.save_dataset_catalog(
            "crypto-btcusdt",
            "v1",
            provider="fixture",
            instrument="BTCUSDT",
            market_type=MarketType.CRYPTO_SPOT,
            timeframe="1h",
            start_timestamp=T0,
            end_timestamp=T0 + timedelta(hours=5),
            row_count=5,
            completeness=1.0,
            quality="HIGH",
            source_type="HISTORICAL",
            snapshot_id="dataset-snapshot-BTCUSDT",
            metadata={"universe_id": "u-paper", "universe_version": "v1", "universe_snapshot": "u-snapshot"},
        )
        ranking = qualification.rank_and_select({"BTCUSDT": True, "ETHUSDT": True}, limit=1, now=T0)

        venue = PaperBinanceSpotVenue(
            initial_balances={"USDT": Decimal("1000"), "BTC": Decimal("0"), "ETH": Decimal("0")},
            liquidity={"BTCUSDT": Decimal("1"), "ETHUSDT": Decimal("1")},
            clock=lambda: now["value"],
        )
        execution = BinanceExecutionService(
            store,
            venue=venue,
            environment="PAPER",
            binding=ranking["selected"],
            clock=lambda: now["value"],
        )
        collector = _StaticCollector(list(snapshots.values()))
        universe = UniverseSnapshot(
            "u-paper",
            "v1",
            "u-snapshot",
            T0,
            "CURRENT",
            (
                {"symbol": "BTC", "binance_symbol": "BTCUSDT", "selected": True},
                {"symbol": "ETH", "binance_symbol": "ETHUSDT", "selected": True},
            ),
            metadata={"methodology": "bounded fixture"},
        )

        def engine_factory(**kwargs: object) -> BinanceSignalEngine:
            policy_value = dict(kwargs.get("exit_policy") or {})
            policy_value.setdefault("max_holding_bars", 1)
            positions = dict(kwargs.get("positions") or {})
            binding = dict(kwargs["binding"])
            strategy = strategy_docs["cand-btc" if binding["candidate_id"] == "cand-btc" else "cand-eth"]
            engine = BinanceSignalEngine(
                binding,
                strategy,
                environment="PAPER",
                decision_interval="1h",
                exit_policy=policy_value,
                positions=positions,
                clock=lambda: now["value"],
            )
            if kwargs.get("intent") == "EXIT":
                return _OriginAwareSignalEngine(engine, T0 + timedelta(hours=2))  # type: ignore[return-value]
            return engine

        worker = BinanceAutonomousWorker(
            store,
            execution,
            collector=collector,
            universe=universe,
            qualification=qualification,
            signal_engine_factory=engine_factory,
            interval="1h",
            max_actionable=1,
            max_entries_per_cycle=1,
            fill_quantity=Decimal("0.05"),
            fee_rate=Decimal("0"),
            clock=lambda: now["value"],
            worker_id="paper-vertical",
        )
        canary = BinanceCanaryControlPlane(
            store,
            execution,
            qualification=qualification,
            worker=worker,
            clock=lambda: now["value"],
            profile={"environment": "PAPER", "host": "127.0.0.1"},
        )

        denied = canary.action("ENABLE", {"confirmation": "ENABLE BINANCE AUTO"})
        self.assertFalse(denied["ok"])
        disabled_cycle = worker.cycle(now=now["value"])
        self.assertEqual(disabled_cycle["no_trade_reason"], "CONTROL_DISABLED")
        self.assertEqual(execution.orders(), [])
        enabled = canary.action("ENABLE", {"confirmation": ENABLE_CONFIRMATION})
        self.assertTrue(enabled["ok"])
        self.assertEqual(execution.control()["state"], "ARMED")
        collector.records = [
            replace(snapshots["BTCUSDT"], new_entry_allowed=False),
            replace(snapshots["ETHUSDT"], new_entry_allowed=False),
        ]
        no_qualified_cycle = worker.cycle(now=now["value"])
        self.assertEqual(no_qualified_cycle["no_trade_reason"], "QUALIFICATION_SELECTION_NOT_CURRENT")
        self.assertEqual(execution.orders(), [])
        collector.records = list(snapshots.values())
        entry_cycle = worker.cycle(now=now["value"])
        self.assertEqual(entry_cycle["status"], "ACTIONED")
        self.assertEqual(len(entry_cycle["entries"]), 1)
        self.assertEqual(entry_cycle["entries"][0]["symbol"], "BTCUSDT")
        self.assertEqual(len(execution.orders()), 1)
        self.assertEqual(execution.orders()[0]["state"], "FILLED")
        self.assertEqual(len(execution.fills()), 1)
        self.assertEqual(execution.fills()[0]["side"], "BUY")
        self.assertEqual(execution.position("BTCUSDT")["quantity"], "0.05")
        # Once the selected BTC position is owned, the lower-ranked ETH
        # fallback is explicitly infeasible for new entries in this interval.
        collector.records = [
            snapshots["BTCUSDT"],
            replace(snapshots["ETHUSDT"], new_entry_allowed=False),
        ]

        repeated_cycle = worker.cycle(now=now["value"])
        self.assertEqual(repeated_cycle["status"], "NO_TRADE")
        self.assertEqual(len(execution.orders()), 1)
        self.assertEqual(len(execution.fills()), 1)

        now["value"] = T0 + timedelta(hours=4)
        exit_cycle = worker.cycle(now=now["value"])
        self.assertEqual(exit_cycle["status"], "ACTIONED")
        self.assertEqual(exit_cycle["exits"][0]["result"]["state"], "FILLED")
        self.assertEqual(len(execution.orders()), 2)
        self.assertEqual(len(execution.fills()), 2)
        closed_position = execution.position("BTCUSDT")
        self.assertEqual(closed_position["quantity"], "0")
        self.assertGreater(Decimal(closed_position["realized_pnl"]), Decimal("0"))

        restarted_worker = BinanceAutonomousWorker(
            store,
            execution,
            collector=collector,
            universe=universe,
            qualification=qualification,
            signal_engine_factory=engine_factory,
            interval="1h",
            max_actionable=1,
            max_entries_per_cycle=1,
            fill_quantity=Decimal("0.05"),
            fee_rate=Decimal("0"),
            clock=lambda: now["value"],
            worker_id="paper-vertical",
        )
        restarted_canary = BinanceCanaryControlPlane(
            store,
            execution,
            qualification=qualification,
            worker=restarted_worker,
            clock=lambda: now["value"],
            profile={"environment": "PAPER", "host": "127.0.0.1"},
        )
        self.assertGreaterEqual(restarted_worker.status()["cycle_number"], 4)
        self.assertTrue(any(action["action"] == "ENABLE" for action in restarted_canary.list_actions()))

        projection = DashboardData(store=store, binance_canary=restarted_canary).binance_canary_data()
        self.assertTrue(projection["available"])
        self.assertEqual(projection["transport"]["polymarket"], "DISABLED")
        self.assertEqual(projection["transport"]["hermes"], "DISABLED")
        self.assertEqual(projection["orders"]["total"], 2)
        self.assertTrue(projection["orders"]["items"])
        self.assertLessEqual(len(projection["orders"]["items"][0]["client_order_id"]), 24)
        self.assertNotIn("secret", json.dumps(projection).lower())
        self.assertNotIn("prediction", json.dumps(projection).lower())


if __name__ == "__main__":
    unittest.main()
