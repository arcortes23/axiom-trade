from __future__ import annotations

from datetime import datetime, timedelta, timezone
from contextlib import redirect_stdout
import sqlite3
import io
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from axiom.cli import main
from axiom.bootstrap import BTC_DATASET_IDS, HistoricalBootstrapper
from axiom.dashboard import DashboardData
from axiom.domain import OHLCVBar
from axiom.storage import AxiomStore, SQLiteBusyTimeout, sqlite_retry

UTC = timezone.utc
T0 = datetime(2020, 1, 1, tzinfo=UTC)


def _bars(count: int) -> list[OHLCVBar]:
    return [
        OHLCVBar(
            T0 + timedelta(days=index),
            100.0 + index,
            101.0 + index,
            99.0 + index,
            100.5 + index,
            1_000.0 + index,
        )
        for index in range(count)
    ]


class _BarsProvider:
    provider_name = "fixture-binance"
    base_url = "https://fixture.invalid"

    def __init__(self, bars: list[OHLCVBar]) -> None:
        self.bars = bars
        self.calls: list[tuple[datetime, datetime, str]] = []

    def historical_ohlcv(
        self,
        symbol: str,
        *,
        start: datetime,
        end: datetime,
        interval: str,
    ) -> list[OHLCVBar]:
        self.calls.append((start, end, interval))
        return [bar for bar in self.bars if start <= bar.timestamp <= end]


class SQLiteConcurrencyTests(unittest.TestCase):
    def test_file_store_uses_verified_wal_and_memory_store_does_not_assume_wal(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "operational.sqlite3")
            with AxiomStore(path) as store:
                self.assertEqual(store.connection.execute("PRAGMA journal_mode").fetchone()[0], "wal")
                self.assertEqual(store.connection.execute("PRAGMA foreign_keys").fetchone()[0], 1)
                self.assertEqual(store.connection.execute("PRAGMA busy_timeout").fetchone()[0], 45_000)
                self.assertEqual(store.connection.execute("PRAGMA synchronous").fetchone()[0], 1)
        with AxiomStore(":memory:") as store:
            self.assertEqual(store.connection.execute("PRAGMA journal_mode").fetchone()[0], "memory")
            self.assertEqual(store.connection.execute("PRAGMA foreign_keys").fetchone()[0], 1)

    def test_dashboard_operator_read_does_not_create_or_mutate_canary_schema(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "dashboard.sqlite3")
            with AxiomStore(path) as store:
                before = store.connection.total_changes
                DashboardData(store=store).operator_data()
                self.assertEqual(store.connection.total_changes, before)

    def test_node_bootstrap_and_dashboard_coexist_on_one_file_database(self) -> None:
        bars = _bars(1_200)
        provider = _BarsProvider(bars)
        errors: list[BaseException] = []
        started = threading.Event()
        bootstrap_done = threading.Event()
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "shared.sqlite3")
            node_store = AxiomStore(path)
            bootstrap_store = AxiomStore(path)
            dashboard_store = AxiomStore(path)
            try:
                bootstrapper = HistoricalBootstrapper(
                    bootstrap_store,
                    crypto_provider=provider,
                    sleep=lambda _: None,
                    max_attempts=1,
                    backoff=0,
                )
                dashboard = DashboardData(store=dashboard_store)

                def node_writer() -> None:
                    started.wait()
                    try:
                        for index in range(48):
                            node_store.save_worker_state(
                                "node",
                                "RUNNING",
                                {"heartbeat": index},
                                heartbeat_at=T0 + timedelta(seconds=index),
                            )
                    except BaseException as exc:  # report thread failures to the test
                        errors.append(exc)

                def bootstrap_writer() -> None:
                    started.wait()
                    try:
                        report = bootstrapper.bootstrap_crypto(
                            intervals=("1d",),
                            start=T0,
                            end=T0 + timedelta(days=len(bars) - 1),
                        )[0]
                        if report.status != "COMPLETE":
                            errors.append(AssertionError(f"bootstrap status={report.status}"))
                    except BaseException as exc:  # report thread failures to the test
                        errors.append(exc)
                    finally:
                        bootstrap_done.set()

                def dashboard_reader() -> None:
                    started.wait()
                    try:
                        while not bootstrap_done.is_set():
                            dashboard.operator_data()
                            time.sleep(0.002)
                        dashboard.operator_data()
                    except BaseException as exc:  # report thread failures to the test
                        errors.append(exc)

                threads = [
                    threading.Thread(target=node_writer, name="test-node-writer"),
                    threading.Thread(target=bootstrap_writer, name="test-bootstrap-writer"),
                    threading.Thread(target=dashboard_reader, name="test-dashboard-reader"),
                ]
                for thread in threads:
                    thread.start()
                started.set()
                for thread in threads:
                    thread.join(30)
                    self.assertFalse(thread.is_alive(), thread.name)

                self.assertEqual(errors, [])
                dataset_id = BTC_DATASET_IDS["1d"]
                catalog = bootstrap_store.load_dataset_catalog(dataset_id)
                self.assertIsNotNone(catalog)
                assert catalog is not None
                version = str(catalog["dataset_version"])
                self.assertEqual(catalog["row_count"], len(bars))
                self.assertEqual(
                    bootstrap_store.count_bars(
                        "BTCUSDT",
                        dataset_id=dataset_id,
                        dataset_version=version,
                    ),
                    len(bars),
                )
                duplicate_counts = bootstrap_store.connection.execute(
                    "SELECT COUNT(*) AS rows, COUNT(DISTINCT timestamp) AS timestamps "
                    "FROM bars WHERE symbol=? AND dataset_id=? AND dataset_version=?",
                    ("BTCUSDT", dataset_id, version),
                ).fetchone()
                self.assertEqual(duplicate_counts["rows"], duplicate_counts["timestamps"])
                self.assertEqual(len(bootstrap_store.load_dataset(dataset_id, version)), len(bars))
                self.assertEqual(bootstrap_store.load_dataset_staging_bars(dataset_id), [])
                state = bootstrap_store.load_dataset_bootstrap_state(dataset_id)
                self.assertIsNotNone(state)
                self.assertEqual(state["status"], "COMPLETE")
            finally:
                dashboard_store.close()
                bootstrap_store.close()
                node_store.close()

    def test_crash_before_catalog_publication_is_resumable_without_duplicates(self) -> None:
        bars = _bars(1_200)
        provider = _BarsProvider(bars)
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "resume.sqlite3")
            with AxiomStore(path) as store:
                bootstrapper = HistoricalBootstrapper(
                    store,
                    crypto_provider=provider,
                    sleep=lambda _: None,
                    max_attempts=1,
                    backoff=0,
                )
                original_publish = store.publish_dataset_bars_chunk
                calls = 0

                def fail_after_first_chunk(*args: object, **kwargs: object) -> dict[str, int]:
                    nonlocal calls
                    calls += 1
                    result = original_publish(*args, **kwargs)
                    if calls == 1:
                        raise RuntimeError("simulated publication crash")
                    return result

                store.publish_dataset_bars_chunk = fail_after_first_chunk  # type: ignore[method-assign]
                with self.assertRaisesRegex(RuntimeError, "simulated publication crash"):
                    bootstrapper.bootstrap_crypto(
                        intervals=("1d",),
                        start=T0,
                        end=T0 + timedelta(days=len(bars) - 1),
                    )
                del store.publish_dataset_bars_chunk
                dataset_id = BTC_DATASET_IDS["1d"]
                self.assertIsNone(store.load_dataset_catalog(dataset_id))
                state = store.load_dataset_bootstrap_state(dataset_id)
                self.assertIsNotNone(state)
                self.assertIn(state["status"], {"FETCHED", "RUNNING"})
                self.assertEqual(len(store.load_dataset_staging_bars(dataset_id)), len(bars))

                resumed = bootstrapper.bootstrap_crypto(
                    intervals=("1d",),
                    start=T0,
                    end=T0 + timedelta(days=len(bars) - 1),
                    resume=True,
                )[0]
                self.assertEqual(resumed.status, "COMPLETE")
                catalog = store.load_dataset_catalog(dataset_id)
                self.assertIsNotNone(catalog)
                assert catalog is not None
                version = str(catalog["dataset_version"])
                self.assertEqual(store.count_bars("BTCUSDT", dataset_id=dataset_id, dataset_version=version), len(bars))
                self.assertEqual(store.load_dataset_staging_bars(dataset_id), [])

    def test_retry_does_not_retry_integrity_errors(self) -> None:
        calls = 0

        def operation() -> None:
            nonlocal calls
            calls += 1
            raise sqlite3.IntegrityError("immutable conflict")

        with self.assertRaises(sqlite3.IntegrityError):
            sqlite_retry(operation, max_attempts=4, initial_delay=0, max_delay=0)
        self.assertEqual(calls, 1)

    def test_writer_lock_failure_is_bounded_and_friendly(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "locked.sqlite3")
            holder = AxiomStore(path)
            blocked = AxiomStore(path, sqlite_timeout_seconds=0.05)
            try:
                holder.connection.execute("BEGIN IMMEDIATE")
                started = time.monotonic()
                with self.assertRaises(SQLiteBusyTimeout) as caught:
                    blocked.save_dataset("blocked", "v1", {"value": 1})
                elapsed = time.monotonic() - started
                self.assertLess(elapsed, 3.0)
                self.assertIn("SQLITE_BUSY_TIMEOUT", str(caught.exception))
                self.assertIn(
                    "another AXIOM writer held the operational database too long",
                    str(caught.exception),
                )
            finally:
                if holder.connection.in_transaction:
                    holder.connection.rollback()
                blocked.close()
                holder.close()

    def test_cli_formats_exhausted_busy_retry(self) -> None:
        output = io.StringIO()
        with patch(
            "axiom.cli._main_impl",
            side_effect=SQLiteBusyTimeout("bootstrap history"),
        ):
            with redirect_stdout(output):
                result = main(["bootstrap-history", "--crypto"])
        self.assertEqual(result, 1)
        self.assertIn('"code": "SQLITE_BUSY_TIMEOUT"', output.getvalue())
        self.assertIn(
            "another AXIOM writer held the operational database too long",
            output.getvalue(),
        )


if __name__ == "__main__":
    unittest.main()
