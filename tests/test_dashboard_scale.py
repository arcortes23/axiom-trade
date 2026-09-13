from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import tempfile
import threading
import time
import unittest
from axiom.dashboard import DashboardData
from axiom.storage import AxiomStore
from unittest.mock import patch


UTC = timezone.utc
T0 = datetime(2026, 1, 1, tzinfo=UTC)


class DashboardScaleFixtureTests(unittest.TestCase):
    """Exercise bounded overview reads against catalog and activity scale."""

    def test_overview_aggregates_large_fixture_during_concurrent_writes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            database_path = Path(temporary_directory) / "dashboard-scale.sqlite3"
            store = AxiomStore(str(database_path))
            writer = AxiomStore(str(database_path))
            try:
                self._seed_catalog(store, 2_000)
                self._seed_activity(store, 50_000)

                started = time.perf_counter()
                summary = store.dashboard_overview_summary(activity_limit=8)
                elapsed = time.perf_counter() - started

                self.assertEqual(summary["counts"]["dataset_catalog"], 2_000)
                self.assertEqual(summary["counts"]["collection_cycles"], 50_000)
                self.assertGreaterEqual(summary["logical_rows"]["catalog"], 2_000_000)
                self.assertLessEqual(len(summary["latest_activity"]), 8)
                self.assertLess(elapsed, 1.0)

                writer_started = threading.Event()
                writer_errors: list[BaseException] = []

                def append_activity() -> None:
                    try:
                        writer_started.set()
                        for index in range(200):
                            timestamp = T0 + timedelta(days=2, seconds=index)
                            writer.save_collection_cycle(
                                f"writer-cycle-{index:04d}",
                                "polymarket",
                                {"markets_seen": 100, "markets_successful": 100, "markets_failed": 0},
                                started_at=timestamp,
                                ended_at=timestamp,
                            )
                    except BaseException as exc:  # pragma: no cover - assertion below reports it
                        writer_errors.append(exc)

                thread = threading.Thread(target=append_activity, name="dashboard-scale-writer")
                thread.start()
                self.assertTrue(writer_started.wait(timeout=2.0))
                concurrent_started = time.perf_counter()
                concurrent_summary = store.dashboard_overview_summary(activity_limit=8)
                concurrent_elapsed = time.perf_counter() - concurrent_started
                thread.join(timeout=10.0)

                self.assertFalse(thread.is_alive())
                self.assertEqual(writer_errors, [])
                self.assertGreaterEqual(concurrent_summary["counts"]["collection_cycles"], 50_000)
                self.assertLessEqual(len(concurrent_summary["latest_activity"]), 8)
                self.assertLess(concurrent_elapsed, 1.0)
            finally:
                writer.close()
                store.close()

    def test_overview_forward_evidence_stays_bounded_at_catalog_scale(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            database_path = Path(temporary_directory) / "dashboard-forward-scale.sqlite3"
            store = AxiomStore(str(database_path))
            try:
                self._seed_catalog(store, 2_000)
                dashboard = DashboardData(store=store)

                started = time.perf_counter()
                overview = dashboard.overview_summary()
                elapsed = time.perf_counter() - started

                self.assertIsInstance(overview, dict)
                evidence = overview["forward_evidence"]
                self.assertLessEqual(len(evidence["candidate_bound_markets"]), 100)
                self.assertLessEqual(len(evidence["scheduled"]), 100)
                self.assertLessEqual(len(evidence["fresh"]), 100)
                self.assertLessEqual(len(evidence["stale"]), 100)
                self.assertLessEqual(len(evidence["missing"]), 100)
                self.assertIn(evidence["grade"], {"A", "B", "C", "D", "F", "UNKNOWN"})
                self.assertIsInstance(evidence["reason_code"], str)
                self.assertLess(elapsed, 1.0)
            finally:
                store.close()
    def test_campaign_projection_does_not_deserialize_unrelated_jobs(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            database_path = Path(temporary_directory) / "dashboard-campaign-scale.sqlite3"
            store = AxiomStore(str(database_path))
            try:
                store.set_operator_job(
                    "polymarket-research-campaign:bounded",
                    "RUNNING",
                    {
                        "campaign_id": "bounded",
                        "budget_limit": 4,
                        "budget_used": 1,
                    },
                    timestamp=T0,
                )
                # The old dashboard path listed every operator job and
                # deserialized each payload before filtering to campaigns.
                store.set_operator_job(
                    "unrelated-large-job",
                    "DONE",
                    {"unrelated": True, "large": "x" * 60_000},
                    timestamp=T0 + timedelta(seconds=1),
                )
                dashboard = DashboardData(store=store)

                def load_campaign_only(encoded: str) -> object:
                    value = json.loads(encoded)
                    if isinstance(value, dict) and value.get("unrelated"):
                        raise AssertionError("unrelated operator payload was deserialized")
                    return value

                with patch("axiom.storage._load", side_effect=load_campaign_only):
                    progress = dashboard._campaign_progress_projection()

                self.assertEqual(progress["campaign_id"], "bounded")
                self.assertEqual(progress["budget_used"], 1)
                self.assertEqual(progress["budget_remaining"], 3)
            finally:
                store.close()


    def test_overview_skips_deserializing_oversized_worker_payloads(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            database_path = Path(temporary_directory) / "dashboard-oversized-worker.sqlite3"
            store = AxiomStore(str(database_path))
            try:
                store.save_worker_state(
                    "polymarket-historical-refresh",
                    "RUNNING",
                    {"unrelated": "x" * 1_100_000},
                    heartbeat_at=T0,
                )
                dashboard = DashboardData(store=store)

                def reject_oversized_load(encoded: str) -> object:
                    if len(encoded) > 1_000_000:
                        raise AssertionError("oversized worker payload was deserialized")
                    return json.loads(encoded)

                with patch("axiom.storage._load", side_effect=reject_oversized_load):
                    overview = dashboard.overview_summary()

                self.assertTrue(overview["available"])
                workers = overview["research_progress"].get("worker_status", {})
                self.assertIsInstance(workers, dict)
            finally:
                store.close()

    @staticmethod
    def _seed_catalog(store: AxiomStore, count: int) -> None:
        rows = []
        for index in range(count):
            timestamp = T0 + timedelta(seconds=index)
            rows.append(
                (
                    f"scale-dataset-{index:04d}",
                    "scale-v1",
                    "fixture",
                    f"instrument-{index:04d}",
                    "CRYPTO_SPOT" if index % 2 else "PREDICTION",
                    "1h",
                    timestamp.isoformat(),
                    (timestamp + timedelta(hours=1)).isoformat(),
                    1_500 + index,
                    0.99,
                    "[]",
                    "HIGH",
                    "HISTORICAL" if index % 2 else "FORWARD_COLLECTED",
                    f"scale-snapshot-{index:04d}",
                    timestamp.isoformat(),
                    timestamp.isoformat(),
                    json.dumps({"fixture": "dashboard-scale"}),
                )
            )
        with store.transaction():
            store.connection.executemany(
                "INSERT INTO dataset_catalog(" 
                "dataset_id,dataset_version,provider,instrument,market_type,timeframe," 
                "start_timestamp,end_timestamp,row_count,completeness,missing_ranges_json," 
                "quality,source_type,snapshot_id,created_at,updated_at,metadata_json) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                rows,
            )

    @staticmethod
    def _seed_activity(store: AxiomStore, count: int) -> None:
        rows = []
        for index in range(count):
            timestamp = T0 + timedelta(seconds=index)
            payload = json.dumps(
                {
                    "markets_seen": 100,
                    "markets_attempted": 100,
                    "markets_successful": 100,
                    "markets_failed": 0,
                }
            )
            rows.append(
                (
                    f"scale-cycle-{index:05d}",
                    "polymarket",
                    timestamp.isoformat(),
                    timestamp.isoformat(),
                    payload,
                    timestamp.isoformat(),
                )
            )
        with store.transaction():
            store.connection.executemany(
                "INSERT INTO collection_cycles(cycle_id,collector_name,started_at,ended_at,payload_json,created_at) "
                "VALUES (?,?,?,?,?,?)",
                rows,
            )


if __name__ == "__main__":
    unittest.main()
