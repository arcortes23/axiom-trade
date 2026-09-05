from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import tempfile
import threading
import time
import unittest

from axiom.storage import AxiomStore


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
