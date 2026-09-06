from __future__ import annotations

from datetime import datetime, timedelta, timezone
import tempfile
import threading
import time
import traceback
import unittest
from pathlib import Path
from typing import Any

from axiom.collector import CollectionCycle
from axiom.dashboard import DashboardData
from axiom.data import InMemoryPredictionProvider
from axiom.forward import ForwardTestRegistry
from axiom.node import NodeConfig, ResearchNode
from axiom.storage import AxiomStore
from axiom.strategy import validate_strategy


UTC = timezone.utc
T0 = datetime(2026, 1, 1, tzinfo=UTC)


class SchedulerScaleTests(unittest.TestCase):
    def test_independent_cadence_fairness_restart_and_wal_reads(self) -> None:
        strategy_document = {
            "version": 1,
            "market_type": "prediction",
            "family": "probability_mispricing",
            "parameters": {},
            "probability_model": "market",
            "resolution_aware": True,
            "resolution_inputs": ["expiry"],
        }
        strategy_definition = validate_strategy(strategy_document)
        model_document = {"yes_probability": 0.6}
        config = {
            "execution": "paper_only",
            "live_execution": False,
            "strategy_document": strategy_definition.to_dict(),
            "model_document": model_document,
        }
        with tempfile.TemporaryDirectory() as directory:
            db = str(Path(directory) / "scale.sqlite")
            with AxiomStore(db) as store:
                market_ids = tuple(f"market-{index:03d}" for index in range(100))
                store.set_collector_state(
                    "polymarket",
                    {
                        "scheduled_market_ids": list(market_ids),
                        "configured_interval_seconds": 0.01,
                        "stale_after_seconds": 1.0,
                    },
                )
                registry = ForwardTestRegistry(store)
                for index in range(30):
                    registry.freeze(
                        strategy=strategy_document,
                        model=model_document,
                        config=config,
                        start_timestamp=T0 + timedelta(seconds=index),
                        allowed_markets=market_ids,
                        experiment_id=f"candidate-{index:02d}",
                    )

                class ScaleCollector:
                    def __init__(self) -> None:
                        self.calls: list[tuple[float, float]] = []

                    def collect_once(self) -> CollectionCycle:
                        started = datetime.now(UTC)
                        monotonic_started = time.monotonic()
                        time.sleep(0.005)
                        ended = datetime.now(UTC)
                        self.calls.append((monotonic_started, time.monotonic()))
                        state = store.get_collector_state("polymarket") or {}
                        state.update(
                            {
                                "last_cycle_started_at": started.isoformat(),
                                "last_cycle_ended_at": ended.isoformat(),
                                "last_cycle_duration_seconds": (ended - started).total_seconds(),
                                "markets_seen": 100,
                                "markets_attempted": 100,
                                "markets_successful": 100,
                                "markets_failed": 0,
                                "scheduled_market_ids": list(market_ids),
                            }
                        )
                        store.set_collector_state("polymarket", state)
                        return CollectionCycle(
                            started,
                            ended,
                            100,
                            markets_attempted=100,
                            markets_successful=100,
                            markets_failed=0,
                            metadata_inserted=0,
                            snapshots_inserted=0,
                            snapshot_duplicates=0,
                            trades_inserted=0,
                            trade_duplicates=0,
                            errors=0,
                            elapsed_seconds=(ended - started).total_seconds(),
                        )

                collector = ScaleCollector()
                node = ResearchNode(
                    NodeConfig(
                        db,
                        interval_seconds=0.01,
                        max_markets=100,
                        paper_candidates_per_cycle=4,
                        crypto_enabled=False,
                        retain_cycles=16,
                    ),
                    provider=InMemoryPredictionProvider([]),
                    opportunity_model={},
                    store=store,
                    sleep=lambda _: None,
                )
                node.collector = collector  # type: ignore[assignment]
                node._configure_logging = lambda: None  # type: ignore[method-assign]
                node._start_heartbeat_watchdog = lambda: None  # type: ignore[method-assign]
                node._stop_heartbeat_watchdog = lambda: None  # type: ignore[method-assign]
                paper_started: list[float] = []
                paper_finished: list[float] = []
                research_queue_runs: list[float] = []

                def slow_paper_worker(spec: Any) -> dict[str, int]:
                    if not paper_started:
                        paper_started.append(time.monotonic())
                    time.sleep(0.03)
                    return {"observations_processed": 1, "fills_inserted": 0}

                def slow_research_queue() -> None:
                    research_queue_runs.append(time.monotonic())
                    time.sleep(0.04)
                    paper_finished.append(time.monotonic())

                node._run_single_paper_worker = slow_paper_worker  # type: ignore[method-assign]
                node._run_research_queue = slow_research_queue  # type: ignore[method-assign]
                reader_stop = threading.Event()
                reader_errors: list[BaseException] = []

                def dashboard_reader() -> None:
                    while not reader_stop.is_set():
                        try:
                            overview = DashboardData(store=store).overview_summary()
                            self.assertIn("components", overview)
                        except BaseException as exc:  # pragma: no cover - assertion captured for main thread
                            reader_errors.append(traceback.format_exc())
                            return

                reader = threading.Thread(target=dashboard_reader, daemon=True)
                reader.start()
                node.run(max_cycles=6)
                reader_stop.set()
                reader.join(timeout=2.0)
                self.assertFalse(reader_errors)
                self.assertEqual(len(collector.calls), 6)
                self.assertTrue(paper_started)
                self.assertTrue(paper_finished)
                self.assertTrue(research_queue_runs)
                collection_span = collector.calls[-1][1] - collector.calls[0][0]
                paper_span = paper_finished[0] - paper_started[0]
                self.assertGreater(paper_span, 0.1)
                self.assertLess(collection_span, paper_span)
                self.assertEqual(store.get_collector_state("polymarket")["markets_seen"], 100)

                overview = DashboardData(store=store).overview_summary()
                components = {item["name"]: item for item in overview["components"]}
                self.assertIn("POLYMARKET COLLECTOR", components)
                self.assertIn("PAPER ENGINE", components)
                self.assertIn("RESEARCH ENGINE", components)
                self.assertEqual(components["POLYMARKET COLLECTOR"]["state"], "IDLE")
                self.assertEqual(components["PAPER ENGINE"]["state"], "IDLE")
                self.assertEqual(components["RESEARCH ENGINE"]["state"], "STOPPED")
                health = store.polymarket_health(expected_interval_seconds=0.01, stale_after_seconds=1.0)
                self.assertEqual(health["configured_interval_seconds"], 0.01)
                self.assertIsNotNone(health["next_scheduled_collection_at"])
                self.assertIsNotNone(health["worker_heartbeat_at"])
                batches: list[list[str]] = []
                for _ in range(8):
                    stats = node._run_paper_workers()
                    batch = list(stats["processed_candidate_ids"])
                    batches.append(batch)
                    self.assertEqual(len(batch), len(set(batch)))
                self.assertEqual(len({item for batch in batches for item in batch}), 30)
                self.assertEqual(batches[0], [f"candidate-{index:02d}" for index in range(4, 8)])
                self.assertEqual(batches[-1], ["candidate-02", "candidate-03", "candidate-04", "candidate-05"])
                scheduler_state = store.get_scheduler_state("paper-engine")
                self.assertEqual(scheduler_state["candidate_count"], 30)
                self.assertEqual(scheduler_state["next_candidate_id"], "candidate-06")

                restarted = ResearchNode(
                    NodeConfig(
                        db,
                        interval_seconds=0.01,
                        max_markets=100,
                        paper_candidates_per_cycle=4,
                        crypto_enabled=False,
                    ),
                    provider=InMemoryPredictionProvider([]),
                    store=store,
                )
                restarted._run_single_paper_worker = slow_paper_worker  # type: ignore[method-assign]
                next_batch = restarted._run_paper_workers()["processed_candidate_ids"]
                self.assertEqual(next_batch, ["candidate-06", "candidate-07", "candidate-08", "candidate-09"])
                paper_worker = next(row for row in store.list_worker_states(limit=64) if row["worker_name"] == "paper-engine")
                self.assertEqual(paper_worker["status"], "idle")
                collector_worker = next(row for row in store.list_worker_states(limit=64) if row["worker_name"] == "polymarket-collector")
                self.assertEqual(collector_worker["payload"]["configured_interval_seconds"], 0.01)
                self.assertIn("next_scheduled_collection_at", collector_worker["payload"])


if __name__ == "__main__":
    unittest.main()
