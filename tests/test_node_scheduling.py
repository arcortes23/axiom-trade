from __future__ import annotations

from datetime import datetime, timedelta, timezone
import math
import os
import sqlite3
import tempfile
import threading
import time
import traceback
import unittest
from unittest.mock import Mock, patch
from pathlib import Path
from typing import Any

from axiom.collector import CollectionCycle, CollectorConfig, PolymarketCollector
from axiom.dashboard import DashboardData
from axiom.data import InMemoryPredictionProvider
from axiom.forward import ForwardTestRegistry
from axiom.node import (
    ISOLATED_EXECUTION_PROFILE,
    PRODUCTION_EXECUTION_PROFILE,
    NodeConfig,
    ResearchNode,
    normalized_execution_profile,
)
from axiom.storage import AxiomStore
from axiom.strategy import validate_strategy



UTC = timezone.utc
T0 = datetime(2026, 1, 1, tzinfo=UTC)


class NodeConfigValidationTests(unittest.TestCase):
    def test_execution_profile_accepts_only_exact_supported_values(self) -> None:
        self.assertEqual(
            normalized_execution_profile(ISOLATED_EXECUTION_PROFILE),
            ISOLATED_EXECUTION_PROFILE,
        )
        self.assertEqual(
            normalized_execution_profile(PRODUCTION_EXECUTION_PROFILE),
            PRODUCTION_EXECUTION_PROFILE,
        )
        for malformed in ("prod", "production ", " PRODUCTION", "isolated\n", 1, False):
            with self.subTest(malformed=malformed):
                with self.assertRaisesRegex(ValueError, "execution profile"):
                    normalized_execution_profile(malformed)

    def test_missing_profile_uses_default_only_when_requested(self) -> None:
        with patch.dict(os.environ, {"AXIOM_EXECUTION_PROFILE": ""}):
            self.assertIsNone(normalized_execution_profile())
            self.assertEqual(
                normalized_execution_profile(default=PRODUCTION_EXECUTION_PROFILE),
                PRODUCTION_EXECUTION_PROFILE,
            )

    def test_node_config_rejects_malformed_profile_without_production_fallback(self) -> None:
        for malformed in ("prod", "production ", "unsafe-profile"):
            with self.subTest(malformed=malformed):
                with self.assertRaisesRegex(ValueError, "execution profile"):
                    NodeConfig(":memory:", execution_profile=malformed)
    def test_direct_construction_rejects_non_positive_depth(self) -> None:
        for depth in (0, -1, False):
            with self.subTest(depth=depth):
                with self.assertRaisesRegex(ValueError, "^depth must be a positive integer$"):
                    NodeConfig(":memory:", depth=depth)

    def test_direct_construction_rejects_invalid_failure_cooldown(self) -> None:
        for cooldown in (-1.0, math.inf, -math.inf, math.nan):
            with self.subTest(cooldown=cooldown):
                with self.assertRaisesRegex(
                    ValueError,
                    "^failure_cooldown_seconds must be finite and non-negative$",
                ):
                    NodeConfig(":memory:", failure_cooldown_seconds=cooldown)

    def test_direct_construction_accepts_positive_depth_and_zero_cooldown(self) -> None:
        config = NodeConfig(
            ":memory:",
            depth=1,
            failure_cooldown_seconds=0,
            discovery_budget_per_cycle=4,
            max_concurrency=2,
        )

        self.assertEqual(config.depth, 1)
        self.assertEqual(config.failure_cooldown_seconds, 0)
        self.assertEqual(config.discovery_budget_per_cycle, 4)
        self.assertEqual(config.max_concurrency, 2)



class NodeIdentityPersistenceTests(unittest.TestCase):
    def test_watchdog_and_root_heartbeat_keep_canonical_execution_profile(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            db = str(Path(directory) / "identity.sqlite")
            with AxiomStore(db) as store:
                node = ResearchNode(
                    NodeConfig(
                        db,
                        execution_profile="isolated",
                        interval_seconds=60.0,
                        crypto_enabled=False,
                    ),
                    provider=InMemoryPredictionProvider([]),
                    store=store,
                )
                node.started_at = T0

                # Caller payloads cannot overwrite the node-owned identity.
                node._heartbeat("running", {"execution_profile": "production"})
                root = next(
                    row for row in store.list_worker_states(limit=32)
                    if row["worker_name"] == node.config.worker_name
                )
                node._start_heartbeat_watchdog()
                try:
                    watchdog = next(
                        row for row in store.list_worker_states(limit=32)
                        if row["worker_name"] == f"{node.config.worker_name}:watchdog"
                    )
                    self.assertEqual(watchdog["payload"]["execution_profile"], "isolated")
                finally:
                    node._stop_heartbeat_watchdog()


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
                def disabled_auto_canary_tick(*, now: datetime | None = None) -> dict[str, Any]:
                    return {
                        "status": "DISABLED",
                        "decision": "AUTONOMOUS_CANARY_DISABLED",
                        "blocker": "AUTONOMOUS_CANARY_DISABLED",
                    }

                node._auto_canary_worker.tick = disabled_auto_canary_tick  # type: ignore[method-assign]
                store.connection.execute(
                    """
                    CREATE TABLE canary_selection (
                        singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                        ranking_run_id TEXT NOT NULL,
                        candidate_id TEXT,
                        rank INTEGER,
                        total_score REAL,
                        component_scores_json TEXT NOT NULL,
                        evidence_versions_json TEXT NOT NULL,
                        reason TEXT NOT NULL,
                        selected_at TEXT NOT NULL
                    )
                    """
                )
                store.connection.execute(
                    """
                    INSERT INTO canary_selection(
                        singleton,ranking_run_id,candidate_id,rank,total_score,
                        component_scores_json,evidence_versions_json,reason,selected_at
                    ) VALUES (1,?,?,?,?,?,?,?,?)
                    """,
                    (
                        "persisted-ranking-run",
                        "persisted-candidate",
                        1,
                        0.875,
                        '{"raw":{"historical_data_integrity":"PASS","historical_execution_fidelity":"PRICE_PROXY","current_execution_evidence":"CURRENT_ORDER_BOOK_REQUIRED"}}',
                        '{"dataset_version":"persisted-v1"}',
                        "SELECTED_WINNER",
                        T0.isoformat(),
                    ),
                )
                store.connection.commit()

                class LockProbe:
                    def __init__(self) -> None:
                        self._lock = threading.RLock()
                        self._state_lock = threading.Lock()
                        self._owner: int | None = None
                        self._depth = 0

                    def acquire(self, *args: Any, **kwargs: Any) -> bool:
                        acquired = self._lock.acquire(*args, **kwargs)
                        if acquired:
                            owner = threading.get_ident()
                            with self._state_lock:
                                if self._owner == owner:
                                    self._depth += 1
                                else:
                                    self._owner = owner
                                    self._depth = 1
                        return acquired

                    def release(self) -> None:
                        with self._state_lock:
                            self._lock.release()
                            if self._owner == threading.get_ident():
                                self._depth -= 1
                                if self._depth == 0:
                                    self._owner = None

                    def __enter__(self) -> "LockProbe":
                        self.acquire()
                        return self

                    def __exit__(self, exc_type: Any, exc_value: Any, traceback_value: Any) -> None:
                        self.release()

                    def held_by_current_thread(self) -> bool:
                        with self._state_lock:
                            return self._owner == threading.get_ident() and self._depth > 0

                original_lock = store._lock
                lock_probe = LockProbe()
                store._lock = lock_probe  # type: ignore[assignment]
                canary_selection_reads: list[bool] = []

                def authorize_canary_selection(
                    _action: int,
                    table: str | None,
                    _column: str | None,
                    _database: str | None,
                    _source: str | None,
                ) -> int:
                    if _action == sqlite3.SQLITE_READ and table == "canary_selection":
                        canary_selection_reads.append(lock_probe.held_by_current_thread())
                        return sqlite3.SQLITE_DENY
                    return sqlite3.SQLITE_OK

                store.connection.set_authorizer(authorize_canary_selection)
                try:
                    locked_overview = DashboardData(store=store).overview_summary()
                    locked_canary = locked_overview["canary"]
                    self.assertIsNone(locked_canary["winner_id"])
                    self.assertIsNone(locked_canary["winner_rank"])
                    self.assertIsNone(locked_canary["selected_candidate"])
                    self.assertIsNone(locked_canary["last_selected_candidate"])
                    self.assertEqual(locked_canary["selection_status"], "UNKNOWN")
                    self.assertIsNone(locked_canary["selection_valid"])
                    self.assertEqual(locked_canary["readiness_snapshot_status"], "STALE")
                    self.assertTrue(locked_canary["readiness_snapshot_stale"])
                    self.assertEqual(
                        locked_canary["readiness_snapshot_reason"],
                        "READINESS_SNAPSHOT_MISSING",
                    )
                    self.assertIsNone(locked_canary["selection_reason"])
                    self.assertIsNone(locked_canary["selection_invalidation_reason"])
                    self.assertGreaterEqual(len(canary_selection_reads), 1)
                    self.assertLessEqual(len(canary_selection_reads), 4)
                    self.assertTrue(all(canary_selection_reads))
                    canary_selection_reads.clear()
                finally:
                    store.connection.set_authorizer(None)
                    store._lock = original_lock

                reader_started = threading.Event()
                reader_stop = threading.Event()
                reader_errors: list[str] = []

                def dashboard_reader() -> None:
                    reader_started.set()
                    while not reader_stop.is_set():
                        try:
                            overview = DashboardData(store=store).overview_summary()
                            self.assertIn("components", overview)
                        except BaseException as exc:  # pragma: no cover - assertion captured for main thread
                            reader_errors.append(
                                f"{type(exc).__name__}: {exc}\n{traceback.format_exc()}"
                            )
                            return

                reader = threading.Thread(target=dashboard_reader, daemon=True)
                reader.start()
                try:
                    self.assertTrue(reader_started.wait(timeout=2.0))
                    node.run(max_cycles=6)
                finally:
                    reader_stop.set()
                    reader.join(timeout=2.0)
                    self.assertFalse(reader.is_alive())

                self.assertFalse(reader_errors, "\n".join(reader_errors))
                self.assertEqual(len(collector.calls), 6)
                self.assertTrue(paper_started)
                self.assertTrue(paper_finished)
                self.assertTrue(research_queue_runs)
                collection_span = collector.calls[-1][1] - collector.calls[0][0]
                paper_span = paper_finished[0] - paper_started[0]
                self.assertGreater(paper_span, 0.1)
                self.assertLess(collection_span, paper_span)
                self.assertEqual(store.get_collector_state("polymarket")["markets_seen"], 100)

                store.connection.set_authorizer(authorize_canary_selection)
                try:
                    overview = DashboardData(store=store).overview_summary()
                finally:
                    store.connection.set_authorizer(None)
                self.assertEqual(canary_selection_reads, [])
                canary = overview["canary"]
                self.assertIsNone(canary["winner_id"])
                self.assertIsNone(canary["winner_rank"])
                self.assertIsNone(canary["selected_candidate"])
                self.assertEqual(canary["last_selected_candidate"], "persisted-candidate")
                self.assertIsNone(canary["winner_score"])
                self.assertEqual(canary["selection_reason"], "SELECTED_WINNER")
                self.assertIsNone(canary["selection_invalidation_reason"])
                self.assertEqual(canary["selection_status"], "UNKNOWN")
                self.assertIsNone(canary["selection_valid"])
                self.assertEqual(canary["readiness_snapshot_status"], "STALE")
                self.assertTrue(canary["readiness_snapshot_stale"])
                self.assertEqual(
                    canary["readiness_snapshot_reason"], "READINESS_SNAPSHOT_INITIALIZING"
                )
                self.assertIsNone(canary["eligible_count"])
                self.assertIsNone(canary["rankable_count"])
                self.assertEqual(canary["execution_event_count"], 0)
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



class MutationSchedulingTests(unittest.TestCase):
    def test_mutation_disabled_never_ticks_or_starts_auto_canary(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            db = str(Path(directory) / "mutation-disabled.sqlite")
            with AxiomStore(db) as store:
                node = ResearchNode(
                    NodeConfig(
                        db,
                        mutation_enabled=False,
                        crypto_enabled=False,
                    ),
                    provider=InMemoryPredictionProvider([]),
                    store=store,
                )
                threads = [Mock(), Mock(), Mock()]
                with patch.object(node._auto_canary_worker, "tick") as tick, patch(
                    "axiom.node.threading.Thread", side_effect=threads
                ) as thread_factory:
                    node._start_worker_threads(max_cycles=1)
                    self.assertIsNone(node._auto_canary_thread)
                    self.assertEqual(thread_factory.call_count, 3)
                    self.assertEqual(
                        [call.kwargs["name"] for call in thread_factory.call_args_list],
                        [
                            "axiom-node-collector",
                            "axiom-node-research",
                            "axiom-node-health",
                        ],
                    )
                    self.assertIs(node._collector_thread, threads[0])
                    self.assertIs(node._research_thread, threads[1])
                    self.assertIs(node._health_thread, threads[2])
                    self.assertNotIn(
                        "autonomous-canary",
                        node._worker_thread_specs(max_cycles=1),
                    )

                node._auto_canary_worker_loop()
                tick.assert_not_called()
                auto_state = next(
                    row
                    for row in store.list_worker_states(limit=64)
                    if row["worker_name"] == "autonomous-canary"
                )
                self.assertEqual(auto_state["status"], "disabled")
                self.assertEqual(
                    auto_state["payload"]["decision"],
                    "AUTONOMOUS_CANARY_DISABLED",
                )
                self.assertEqual(
                    auto_state["payload"]["blocker"],
                    "AUTONOMOUS_CANARY_DISABLED",
                )

    def test_unknown_no_retry_result_marks_worker_degraded(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            db = str(Path(directory) / "unknown-no-retry.sqlite")
            with AxiomStore(db) as store:
                node = ResearchNode(
                    NodeConfig(db, crypto_enabled=False),
                    provider=InMemoryPredictionProvider([]),
                    store=store,
                )
                result = {
                    "status": "BLOCKED",
                    "decision": "UNKNOWN_NO_RETRY",
                    "blocker": "UNKNOWN_NO_RETRY",
                }
                with patch.object(
                    node._auto_canary_worker,
                    "tick",
                    return_value=result,
                ) as tick, patch.object(
                    node,
                    "_worker_tick_completed",
                ) as tick_completed, patch.object(
                    node.stop_event,
                    "wait",
                    return_value=True,
                ):
                    node._auto_canary_worker_loop()
                tick.assert_called_once()
                self.assertFalse(tick_completed.call_args.kwargs["successful"])
                self.assertEqual(
                    tick_completed.call_args.kwargs["error"],
                    "UNKNOWN_NO_RETRY",
                )


class NodeStopPollingTests(unittest.TestCase):
    def test_only_exact_owned_stop_marker_authorizes_shutdown(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            db = str(Path(directory) / "stop-marker.sqlite")
            with AxiomStore(db) as store:
                node = ResearchNode(
                    NodeConfig(db, crypto_enabled=False),
                    provider=InMemoryPredictionProvider([]),
                    store=store,
                )
                lock_marker = f"{os.getpid()}\nowned-run\n"
                Path(node.lock_path).write_text(lock_marker, encoding="ascii")
                malformed = Path(node.stop_path)
                malformed.write_text("not-a-canonical-marker", encoding="ascii")
                self.assertFalse(node._external_stop_requested())
                self.assertFalse(node.stop_event.is_set())
                self.assertEqual(malformed.read_text(encoding="ascii"), "not-a-canonical-marker")

                wrong_run = f"{os.getpid()}\nother-run\n"
                malformed.write_text(wrong_run, encoding="ascii")
                self.assertFalse(node._external_stop_requested())
                self.assertFalse(node.stop_event.is_set())
                self.assertEqual(malformed.read_text(encoding="ascii"), wrong_run)

                malformed.write_text(lock_marker, encoding="ascii")
                self.assertTrue(node._external_stop_requested())
                self.assertTrue(node.stop_event.is_set())

    def test_owned_stop_marker_wakes_long_collection_interval(self) -> None:
        class OneCycleCollector:
            def __init__(self) -> None:
                self.completed = threading.Event()

            def collect_once(self) -> CollectionCycle:
                started = datetime.now(UTC)
                ended = datetime.now(UTC)
                self.completed.set()
                return CollectionCycle(
                    started,
                    ended,
                    0,
                    markets_attempted=0,
                    markets_successful=0,
                    markets_failed=0,
                    metadata_inserted=0,
                    snapshots_inserted=0,
                    snapshot_duplicates=0,
                    trades_inserted=0,
                    trade_duplicates=0,
                    errors=0,
                    elapsed_seconds=0.0,
                )

        with tempfile.TemporaryDirectory() as directory:
            db = str(Path(directory) / "stop-polling.sqlite")
            with AxiomStore(db) as store:
                node = ResearchNode(
                    NodeConfig(
                        db,
                        interval_seconds=60.0,
                        crypto_enabled=False,
                    ),
                    provider=InMemoryPredictionProvider([]),
                    store=store,
                )
                collector = OneCycleCollector()
                node.collector = collector  # type: ignore[assignment]
                node._configure_logging = lambda: None  # type: ignore[method-assign]
                node._start_heartbeat_watchdog = lambda: None  # type: ignore[method-assign]
                node._stop_heartbeat_watchdog = lambda: None  # type: ignore[method-assign]
                errors: list[BaseException] = []

                def run_node() -> None:
                    try:
                        node.run()
                    except BaseException as exc:  # pragma: no cover - surfaced below
                        errors.append(exc)

                runner = threading.Thread(target=run_node, daemon=True)
                runner.start()
                try:
                    self.assertTrue(collector.completed.wait(timeout=2.0))
                    marker = Path(node.lock_path).read_text(encoding="ascii")
                    started_waiting = time.monotonic()
                    Path(node.stop_path).write_text(marker, encoding="ascii")
                    runner.join(timeout=2.0)
                    self.assertFalse(runner.is_alive())
                    self.assertLess(time.monotonic() - started_waiting, 2.0)
                    self.assertFalse(errors, repr(errors))
                finally:
                    node.stop_event.set()
                    runner.join(timeout=2.0)


class CollectorConcurrencyTests(unittest.TestCase):
    def test_max_two_requires_explicit_isolated_workers_and_unsafe_provider_is_serial(self) -> None:
        class SharedActivity:
            def __init__(self) -> None:
                self.lock = threading.Lock()
                self.active = 0
                self.max_active = 0

        class BlockingProvider(InMemoryPredictionProvider):
            provider_name = "blocking-test"

            def __init__(self, activity: SharedActivity, *, isolated: bool = False) -> None:
                super().__init__([])
                self.activity = activity
                if isolated:
                    self.isolated_worker_factory = lambda: BlockingProvider(activity)

            def market(self, market_id: str):
                del market_id
                with self.activity.lock:
                    self.activity.active += 1
                    self.activity.max_active = max(self.activity.max_active, self.activity.active)
                try:
                    time.sleep(0.03)
                    return None
                finally:
                    with self.activity.lock:
                        self.activity.active -= 1

        for isolated, expected_max_active in ((False, 1), (True, 2)):
            with self.subTest(isolated=isolated), AxiomStore(":memory:") as store:
                activity = SharedActivity()
                provider = BlockingProvider(activity, isolated=isolated)
                collector = PolymarketCollector(
                    provider,
                    store,
                    CollectorConfig(
                        interval_seconds=60,
                        market_ids=("worker-a", "worker-b"),
                        max_markets=2,
                        max_attempts=1,
                        max_concurrency=2,
                        jitter_seconds=0,
                    ),
                    clock=lambda: T0,
                    sleep=lambda _seconds: None,
                )
                cycle = collector.collect_once(now=T0)

                self.assertEqual(activity.max_active, expected_max_active)
                self.assertEqual(cycle.markets_attempted, 2)
                self.assertLessEqual(activity.max_active, 2)


if __name__ == "__main__":
    unittest.main()
