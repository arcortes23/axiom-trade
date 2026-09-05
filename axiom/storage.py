"""SQLite persistence for immutable research datasets and experiment artifacts.

AxiomStore is intentionally boring: SQLite, JSON payloads, UTC timestamps, and
append-only records. Dataset versions use a primary key and are never updated;
writing the same ``(dataset_id, version)`` raises ``ValueError``. Stored market
objects are reconstructed as canonical domain dataclasses, while arbitrary
strategy, experiment and report payloads remain plain JSON-compatible values.
"""
from __future__ import annotations
from contextlib import contextmanager
import hashlib
import json
import logging
import math
import os
import threading
import sqlite3
import time
from itertools import islice
from dataclasses import asdict, is_dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, Sequence
_MAX_LATEST_SCAN_ROWS = 10_000
_MAX_EVIDENCE_SCAN_ROWS = 100_000
_QUEUE_RELEASE_BATCH = 256
_QUEUE_LINEAGE_LIMIT = 256
_PAGINATION_PAGE_SIZES = (10, 25, 50, 100)
_DEFAULT_PAGE_SIZE = 25
_POLYMARKET_SOURCE_TYPES = frozenset({"HISTORICAL", "FORWARD_COLLECTED"})
_DEFAULT_OPERATIONAL_WINDOW_SECONDS = 3_600.0
_MAX_OPERATIONAL_WINDOW_SECONDS = 86_400.0
SQLITE_CONNECTION_TIMEOUT_SECONDS = 45.0
SQLITE_BUSY_RETRY_ATTEMPTS = 4
SQLITE_BUSY_RETRY_INITIAL_SECONDS = 0.05
SQLITE_BUSY_RETRY_MAX_SECONDS = 0.5
_LOGGER = logging.getLogger(__name__)


class SQLiteBusyTimeout(sqlite3.OperationalError):
    """Bounded retry exhaustion while another writer owns SQLite."""

    code = "SQLITE_BUSY_TIMEOUT"
    friendly_message = "another AXIOM writer held the operational database too long"

    def __init__(self, operation_name: str) -> None:
        super().__init__(
            f"{self.code}: {self.friendly_message}"
            f" ({operation_name})"
        )


def _is_transient_sqlite_error(exc: BaseException) -> bool:
    if isinstance(exc, SQLiteBusyTimeout) or not isinstance(exc, sqlite3.Error):
        return False
    message = str(exc).lower()
    if "schema" in message and "lock" in message:
        return False
    code = getattr(exc, "sqlite_errorcode", None)
    if isinstance(code, int):
        base_code = code & 0xFF
        if base_code in {sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED}:
            return True
    return any(
        marker in message
        for marker in ("database is locked", "database table is locked", "sqlite_busy")
    )


def sqlite_retry(
    operation: Any,
    *,
    operation_name: str = "SQLite operation",
    max_attempts: int = SQLITE_BUSY_RETRY_ATTEMPTS,
    initial_delay: float = SQLITE_BUSY_RETRY_INITIAL_SECONDS,
    max_delay: float = SQLITE_BUSY_RETRY_MAX_SECONDS,
) -> Any:
    """Run an SQLite operation with bounded retries for transient lock errors."""
    attempts = int(max_attempts)
    if isinstance(max_attempts, bool) or attempts < 1:
        raise ValueError("max_attempts must be positive")
    delay = float(initial_delay)
    delay_cap = float(max_delay)
    if (
        not math.isfinite(delay)
        or not math.isfinite(delay_cap)
        or delay < 0
        or delay_cap < 0
    ):
        raise ValueError("SQLite retry delays must be finite and non-negative")
    delay_cap = max(delay, delay_cap)
    for attempt in range(attempts):
        try:
            return operation()
        except sqlite3.Error as exc:
            if not _is_transient_sqlite_error(exc):
                raise
            if attempt >= attempts - 1:
                _LOGGER.warning(
                    "SQLite busy retry exhausted operation=%s attempts=%d",
                    operation_name,
                    attempts,
                )
                raise SQLiteBusyTimeout(operation_name) from exc
            if delay:
                time.sleep(min(delay, delay_cap))
                delay = min(delay * 2.0, delay_cap)
    raise AssertionError("sqlite_retry exhausted without returning or raising")

from .domain import (
    CryptoTicker,
    Fill,
    InstrumentMetadata,
    MarketType,
    ResearchQuality,
    OHLCVBar,
    OrderBookLevel,
    OrderBookSnapshot,
    PredictionMarketSnapshot,
    TradePrint,
    ResolvedContract,
    SettlementState,
    Side,
    ensure_utc,
    parse_timestamp,
    to_record,
    utc_now,
)


class AxiomStore:
    """Append-only SQLite store for data, strategy runs and reports.

    Args:
        path: SQLite filename or ``":memory:"``. Parent directories are created
            for regular filenames.
        connection: Optional existing connection, useful for test fixtures.
    """

    def __init__(
        self,
        path: str | os.PathLike[str] = ":memory:",
        *,
        connection: sqlite3.Connection | None = None,
        sqlite_timeout_seconds: float = SQLITE_CONNECTION_TIMEOUT_SECONDS,
    ) -> None:
        self.path = str(path)
        timeout = float(sqlite_timeout_seconds)
        if not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("sqlite_timeout_seconds must be finite and positive")
        if connection is None and self.path not in {":memory:", ""} and not self.path.startswith("file:"):
            parent = Path(self.path).expanduser().parent
            if str(parent) not in {"", "."}:
                parent.mkdir(parents=True, exist_ok=True)
        created_connection = connection is None
        self._conn = (
            connection
            if connection is not None
            else sqlite3.connect(
                self.path,
                timeout=timeout,
                check_same_thread=False,
                uri=self.path.startswith("file:"),
            )
        )
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        self._transaction_depth = 0
        self._sqlite_timeout_seconds = timeout
        self._sqlite_busy_timeout_ms = max(1, int(round(timeout * 1000.0)))
        try:
            self._configure_connection()
            self.initialize()
        except BaseException:
            if created_connection:
                self._conn.close()
            raise

    def _database_filename(self) -> str:
        rows = self._conn.execute("PRAGMA database_list").fetchall()
        for row in rows:
            name = str(row[1] if not isinstance(row, Mapping) else row["name"])
            if name == "main":
                value = row[2] if not isinstance(row, Mapping) else row["file"]
                return str(value or "")
        return ""

    def _configure_connection(self) -> None:
        """Configure and verify per-connection SQLite concurrency guarantees."""
        self._conn.execute("PRAGMA foreign_keys=ON")
        foreign_keys = int(self._conn.execute("PRAGMA foreign_keys").fetchone()[0])
        if foreign_keys != 1:
            raise sqlite3.OperationalError("SQLite foreign_keys pragma was not enabled")
        self._conn.execute(f"PRAGMA busy_timeout={self._sqlite_busy_timeout_ms}")
        busy_timeout = int(self._conn.execute("PRAGMA busy_timeout").fetchone()[0])
        if busy_timeout != self._sqlite_busy_timeout_ms:
            raise sqlite3.OperationalError("SQLite busy_timeout pragma was not applied")
        file_backed = bool(self._database_filename())

        def configure_journal() -> None:
            if file_backed:
                result = self._conn.execute("PRAGMA journal_mode=WAL").fetchone()
                actual = str(result[0] if result is not None else "").lower()
                if actual != "wal":
                    raise sqlite3.OperationalError(
                        f"SQLite WAL mode unavailable; actual journal mode is {actual or 'unknown'}"
                    )
            self._conn.execute("PRAGMA synchronous=NORMAL")

        sqlite_retry(configure_journal, operation_name="configure SQLite journal mode")
        journal_result = self._conn.execute("PRAGMA journal_mode").fetchone()
        journal_mode = str(journal_result[0] if journal_result is not None else "").lower()
        if file_backed and journal_mode != "wal":
            raise sqlite3.OperationalError(
                f"SQLite journal mode verification failed: {journal_mode or 'unknown'}"
            )
        synchronous = int(self._conn.execute("PRAGMA synchronous").fetchone()[0])
        if synchronous != 1:
            raise sqlite3.OperationalError("SQLite synchronous=NORMAL was not applied")


    @property
    def connection(self) -> sqlite3.Connection:
        """Underlying connection for dashboard integrations and read-only queries."""
        return self._conn
    @contextmanager
    def transaction(self, *, immediate: bool = False) -> Iterator["AxiomStore"]:
        """Group several append-only writes into one rollback boundary."""
        with self._lock:
            if immediate and not self._transaction_depth:
                sqlite_retry(
                    lambda: self._conn.execute("BEGIN IMMEDIATE"),
                    operation_name="begin immediate SQLite transaction",
                )
                self._transaction_depth += 1
                try:
                    yield self
                except BaseException:
                    self._conn.rollback()
                    raise
                else:
                    try:
                        sqlite_retry(
                            self._conn.commit,
                            operation_name="commit SQLite transaction",
                        )
                    except BaseException:
                        self._conn.rollback()
                        raise
                finally:
                    self._transaction_depth -= 1
                return
            self._transaction_depth += 1
            savepoint = f"axiom_tx_{id(self):x}_{self._transaction_depth}"
            self._conn.execute(f"SAVEPOINT {savepoint}")
            try:
                yield self
            except BaseException:
                self._conn.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
                self._conn.execute(f"RELEASE SAVEPOINT {savepoint}")
                raise
            else:
                self._conn.execute(f"RELEASE SAVEPOINT {savepoint}")
            finally:
                self._transaction_depth -= 1

    @contextmanager
    def _write_context(self) -> Iterator[None]:
        with self._lock:
            if self._transaction_depth:
                savepoint = f"axiom_write_{id(self):x}_{self._transaction_depth}"
                self._conn.execute(f"SAVEPOINT {savepoint}")
                try:
                    yield
                except BaseException:
                    self._conn.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
                    self._conn.execute(f"RELEASE SAVEPOINT {savepoint}")
                    raise
                else:
                    self._conn.execute(f"RELEASE SAVEPOINT {savepoint}")
            else:
                with self._conn:
                    yield


    def initialize(self) -> None:
        """Create schema and indexes without changing existing records."""
        sqlite_retry(self._initialize_schema, operation_name="initialize SQLite schema")

    def _initialize_schema(self) -> None:
        """Create schema and indexes for one initialization attempt."""
        with self._lock, self._conn:
            self._conn.executescript(
                """
                PRAGMA foreign_keys = ON;
                CREATE TABLE IF NOT EXISTS datasets (
                    dataset_id TEXT NOT NULL,
                    version TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    metadata_json TEXT NOT NULL DEFAULT '{}',
                    quality TEXT,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (dataset_id, version)
                );
                CREATE TABLE IF NOT EXISTS dataset_catalog (
                    dataset_id TEXT NOT NULL,
                    dataset_version TEXT NOT NULL,
                    provider TEXT NOT NULL,
                    instrument TEXT NOT NULL,
                    market_type TEXT NOT NULL,
                    timeframe TEXT NOT NULL DEFAULT '',
                    start_timestamp TEXT,
                    end_timestamp TEXT,
                    row_count INTEGER NOT NULL,
                    completeness REAL NOT NULL,
                    missing_ranges_json TEXT NOT NULL DEFAULT '[]',
                    quality TEXT NOT NULL,
                    source_type TEXT NOT NULL,
                    snapshot_id TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    metadata_json TEXT NOT NULL DEFAULT '{}',
                    PRIMARY KEY (dataset_id, dataset_version)
                );
                CREATE INDEX IF NOT EXISTS idx_dataset_catalog_source
                    ON dataset_catalog(source_type, updated_at);
                CREATE INDEX IF NOT EXISTS idx_dataset_catalog_instrument
                    ON dataset_catalog(instrument, timeframe, updated_at);
                CREATE INDEX IF NOT EXISTS idx_dataset_catalog_updated
                    ON dataset_catalog(updated_at, dataset_id, dataset_version);
                CREATE TABLE IF NOT EXISTS dataset_bootstrap_state (
                    dataset_id TEXT PRIMARY KEY,
                    provider TEXT NOT NULL,
                    instrument TEXT NOT NULL,
                    market_type TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    requested_start TEXT,
                    requested_end TEXT,
                    next_timestamp TEXT,
                    base_version TEXT,
                    status TEXT NOT NULL,
                    payload_json TEXT NOT NULL DEFAULT '{}',
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_dataset_bootstrap_updated
                    ON dataset_bootstrap_state(updated_at, dataset_id);
                CREATE TABLE IF NOT EXISTS dataset_staging_bars (
                    dataset_id TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (dataset_id, timestamp)
                );
                CREATE INDEX IF NOT EXISTS idx_dataset_staging_bars_time
                    ON dataset_staging_bars(dataset_id, timestamp);
                CREATE TABLE IF NOT EXISTS historical_regime_labels (
                    dataset_id TEXT NOT NULL,
                    dataset_version TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    labels_json TEXT NOT NULL,
                    confidence_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (dataset_id, dataset_version, timestamp)
                );
                CREATE INDEX IF NOT EXISTS idx_historical_regimes_dataset
                    ON historical_regime_labels(dataset_id, dataset_version, timestamp);
                CREATE TABLE IF NOT EXISTS bars (
                    symbol TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    dataset_id TEXT NOT NULL DEFAULT '',
                    dataset_version TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (symbol, timestamp, dataset_id, dataset_version)
                );
                CREATE INDEX IF NOT EXISTS idx_bars_symbol_time ON bars(symbol, timestamp);
                CREATE TABLE IF NOT EXISTS snapshots (
                    key TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    dataset_id TEXT NOT NULL DEFAULT '',
                    dataset_version TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (key, timestamp, kind, dataset_id, dataset_version)
                );
                CREATE INDEX IF NOT EXISTS idx_snapshots_key_time ON snapshots(key, timestamp);
                CREATE TABLE IF NOT EXISTS strategies (
                    strategy_id TEXT NOT NULL,
                    version TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (strategy_id, version)
                );
                CREATE TABLE IF NOT EXISTS experiments (
                    experiment_id TEXT PRIMARY KEY,
                    strategy_id TEXT,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS fills (
                    fill_id TEXT PRIMARY KEY,
                    order_id TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    strategy_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_fills_strategy_time ON fills(strategy_id, timestamp);
                CREATE TABLE IF NOT EXISTS reports (
                    report_id TEXT PRIMARY KEY,
                    experiment_id TEXT,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_reports_created
                    ON reports(created_at, report_id);
                CREATE TABLE IF NOT EXISTS polymarket_markets (
                    market_id TEXT NOT NULL,
                    observed_at TEXT NOT NULL,
                    metadata_hash TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    source_type TEXT NOT NULL DEFAULT 'FORWARD_COLLECTED',
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (market_id, observed_at, metadata_hash)
                );
                CREATE INDEX IF NOT EXISTS idx_polymarket_markets_observed
                    ON polymarket_markets(market_id, observed_at);
                CREATE INDEX IF NOT EXISTS idx_polymarket_markets_dashboard
                    ON polymarket_markets(observed_at, market_id, metadata_hash);
                CREATE TABLE IF NOT EXISTS polymarket_snapshots (
                    snapshot_id TEXT PRIMARY KEY,
                    market_id TEXT NOT NULL,
                    source_timestamp TEXT NOT NULL,
                    observed_at TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    quality TEXT NOT NULL,
                    source_type TEXT NOT NULL DEFAULT 'FORWARD_COLLECTED',
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_polymarket_snapshots_market_time
                    ON polymarket_snapshots(market_id, source_timestamp, observed_at);
                CREATE INDEX IF NOT EXISTS idx_polymarket_snapshots_dashboard
                    ON polymarket_snapshots(observed_at, market_id, snapshot_id);
                CREATE TABLE IF NOT EXISTS polymarket_trades (
                    trade_key TEXT PRIMARY KEY,
                    market_id TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_polymarket_trades_market_time
                    ON polymarket_trades(market_id, timestamp);
                CREATE TABLE IF NOT EXISTS collector_state (
                    collector_name TEXT PRIMARY KEY,
                    state_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS collection_errors (
                    error_id TEXT PRIMARY KEY,
                    market_id TEXT,
                    observed_at TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    detail TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    source_type TEXT NOT NULL DEFAULT 'FORWARD_COLLECTED',
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_collection_errors_market_time
                    ON collection_errors(market_id, observed_at);
                CREATE INDEX IF NOT EXISTS idx_collection_errors_observed
                    ON collection_errors(observed_at, error_id);
                CREATE TABLE IF NOT EXISTS forward_tests (
                    experiment_id TEXT PRIMARY KEY,
                    strategy_hash TEXT NOT NULL,
                    model_hash TEXT NOT NULL,
                    config_json TEXT NOT NULL,
                    start_timestamp TEXT NOT NULL,
                    bankroll REAL NOT NULL,
                    allowed_markets_json TEXT NOT NULL,
                    risk_limits_json TEXT NOT NULL,
                    quality TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS collection_cycles (
                    cycle_id TEXT PRIMARY KEY,
                    collector_name TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    ended_at TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_collection_cycles_time
                    ON collection_cycles(collector_name, started_at);
                CREATE INDEX IF NOT EXISTS idx_collection_cycles_ended
                    ON collection_cycles(ended_at, cycle_id);
                CREATE TABLE IF NOT EXISTS research_queue (
                    item_id TEXT PRIMARY KEY,
                    item_type TEXT NOT NULL,
                    dedupe_key TEXT NOT NULL UNIQUE,
                    status TEXT NOT NULL,
                    priority INTEGER NOT NULL DEFAULT 0,
                    payload_json TEXT NOT NULL,
                    source TEXT NOT NULL DEFAULT '',
                    author TEXT NOT NULL DEFAULT '',
                    lineage_json TEXT NOT NULL DEFAULT '[]',
                    schema_version TEXT NOT NULL DEFAULT '1',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    available_at TEXT NOT NULL,
                    lease_until TEXT,
                    lease_owner TEXT,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT,
                    result_json TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_research_queue_status
                    ON research_queue(status, priority DESC, available_at, created_at);
                CREATE INDEX IF NOT EXISTS idx_research_queue_dashboard
                    ON research_queue(status, priority DESC, created_at, item_id);
                CREATE INDEX IF NOT EXISTS idx_research_queue_source_created
                    ON research_queue(source, created_at, item_id);
                CREATE INDEX IF NOT EXISTS idx_research_queue_type_created
                    ON research_queue(item_type, created_at, item_id);
                CREATE TABLE IF NOT EXISTS research_queue_events (
                    event_id TEXT PRIMARY KEY,
                    item_id TEXT NOT NULL,
                    from_status TEXT,
                    to_status TEXT NOT NULL,
                    detail TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_research_queue_events_item
                    ON research_queue_events(item_id, created_at);
                CREATE INDEX IF NOT EXISTS idx_research_queue_events_created
                    ON research_queue_events(created_at, event_id);
                CREATE TABLE IF NOT EXISTS candidate_lifecycle (
                    candidate_id TEXT PRIMARY KEY,
                    stage TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_candidate_lifecycle_stage_updated
                    ON candidate_lifecycle(stage, updated_at, candidate_id);
                CREATE INDEX IF NOT EXISTS idx_candidate_lifecycle_updated
                    ON candidate_lifecycle(updated_at, candidate_id);
                CREATE TABLE IF NOT EXISTS candidate_lifecycle_events (
                    event_id TEXT PRIMARY KEY,
                    candidate_id TEXT NOT NULL,
                    from_stage TEXT,
                    to_stage TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_candidate_lifecycle_events_candidate
                    ON candidate_lifecycle_events(candidate_id, created_at);
                CREATE INDEX IF NOT EXISTS idx_candidate_lifecycle_events_created
                    ON candidate_lifecycle_events(created_at, event_id);
                CREATE TABLE IF NOT EXISTS paper_state (
                    experiment_id TEXT PRIMARY KEY,
                    state_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    state_version INTEGER NOT NULL DEFAULT 0
                );
                CREATE INDEX IF NOT EXISTS idx_paper_state_updated
                    ON paper_state(updated_at, experiment_id);
                CREATE TABLE IF NOT EXISTS paper_observations (
                    observation_id TEXT PRIMARY KEY,
                    experiment_id TEXT NOT NULL,
                    market_id TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_paper_observations_experiment_time
                    ON paper_observations(experiment_id, timestamp);
                CREATE INDEX IF NOT EXISTS idx_paper_observations_dashboard
                    ON paper_observations(timestamp, observation_id);
                CREATE TABLE IF NOT EXISTS paper_execution_events (
                    event_id TEXT PRIMARY KEY,
                    experiment_id TEXT NOT NULL,
                    observation_id TEXT NOT NULL,
                    market_id TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    status TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE (experiment_id, observation_id)
                );
                CREATE INDEX IF NOT EXISTS idx_paper_execution_events_experiment_time
                    ON paper_execution_events(experiment_id, timestamp);
                CREATE INDEX IF NOT EXISTS idx_paper_execution_events_dashboard
                    ON paper_execution_events(timestamp, event_id);
                CREATE TABLE IF NOT EXISTS paper_bet_ledger (
                    bet_id TEXT PRIMARY KEY,
                    experiment_id TEXT NOT NULL,
                    market_id TEXT NOT NULL,
                    strategy_id TEXT NOT NULL,
                    outcome TEXT NOT NULL,
                    resolution TEXT NOT NULL,
                    resolved_at TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE (experiment_id, market_id)
                );
                CREATE INDEX IF NOT EXISTS idx_paper_bet_ledger_experiment_time
                    ON paper_bet_ledger(experiment_id, resolved_at);
                CREATE INDEX IF NOT EXISTS idx_paper_bet_ledger_dashboard
                    ON paper_bet_ledger(resolved_at, bet_id);
                CREATE TABLE IF NOT EXISTS opportunity_snapshots (
                    opportunity_id TEXT PRIMARY KEY,
                    observed_at TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_opportunity_snapshots_time
                    ON opportunity_snapshots(observed_at);
                CREATE TABLE IF NOT EXISTS experiment_budget (
                    budget_id TEXT PRIMARY KEY,
                    payload_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS experiment_budget_reservations (
                    budget_id TEXT NOT NULL,
                    reservation_key TEXT NOT NULL,
                    family TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (budget_id, reservation_key)
                );
                CREATE TABLE IF NOT EXISTS experiment_plans (
                    plan_id TEXT PRIMARY KEY,
                    hypothesis_id TEXT NOT NULL,
                    plan_hash TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    status TEXT NOT NULL,
                    result_json TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_experiment_plans_hypothesis
                    ON experiment_plans(hypothesis_id, created_at);
                CREATE INDEX IF NOT EXISTS idx_experiment_plans_status
                    ON experiment_plans(status, updated_at);
                CREATE TABLE IF NOT EXISTS worker_state (
                    worker_name TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    started_at TEXT,
                    heartbeat_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                """
            )
            queue_columns = {str(row["name"]) for row in self._conn.execute("PRAGMA table_info(research_queue)").fetchall()}
            if "updated_at" not in queue_columns:
                self._conn.execute("ALTER TABLE research_queue ADD COLUMN updated_at TEXT")
                self._conn.execute("UPDATE research_queue SET updated_at=created_at WHERE updated_at IS NULL")
            for table in ("polymarket_markets", "polymarket_snapshots", "collection_errors"):
                columns = {str(row["name"]) for row in self._conn.execute(f"PRAGMA table_info({table})").fetchall()}
                if "source_type" not in columns:
                    self._conn.execute(
                        f"ALTER TABLE {table} ADD COLUMN source_type TEXT NOT NULL DEFAULT 'FORWARD_COLLECTED'"
                    )
                # Older payloads carried provenance only in JSON.  Preserve all
                # rows while making the provenance queryable and deterministic.
                self._conn.execute(
                    f"UPDATE {table} SET source_type=CASE "
                    "WHEN upper(COALESCE(json_extract(payload_json, '$.source_type'), ''))='HISTORICAL' THEN 'HISTORICAL' "
                    "WHEN upper(COALESCE(json_extract(payload_json, '$.source_type'), ''))='FORWARD_COLLECTED' THEN 'FORWARD_COLLECTED' "
                    "ELSE COALESCE(NULLIF(upper(source_type), ''), 'FORWARD_COLLECTED') END"
                )
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_polymarket_markets_source_observed "
                "ON polymarket_markets(source_type, observed_at, market_id)"
            )
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_polymarket_snapshots_source_observed "
                "ON polymarket_snapshots(source_type, observed_at, market_id)"
            )
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_collection_errors_source_observed "
                "ON collection_errors(source_type, observed_at, market_id)"
            )
            if "lease_owner" not in queue_columns:
                self._conn.execute("ALTER TABLE research_queue ADD COLUMN lease_owner TEXT")
            if "result_json" not in queue_columns:
                self._conn.execute("ALTER TABLE research_queue ADD COLUMN result_json TEXT")
            paper_state_columns = {str(row["name"]) for row in self._conn.execute("PRAGMA table_info(paper_state)").fetchall()}
            if "state_version" not in paper_state_columns:
                self._conn.execute("ALTER TABLE paper_state ADD COLUMN state_version INTEGER NOT NULL DEFAULT 0")
            self._migrate_market_tables()
    def _migrate_market_tables(self) -> None:
        """Upgrade pre-versioned market tables without discarding records."""
        for table, primary_key, index_name in (
            ("bars", ("symbol", "timestamp", "dataset_id", "dataset_version"), "idx_bars_symbol_time"),
            ("snapshots", ("key", "timestamp", "kind", "dataset_id", "dataset_version"), "idx_snapshots_key_time"),
        ):
            columns = self._conn.execute(f"PRAGMA table_info({table})").fetchall()
            current_key = tuple(row["name"] for row in sorted(columns, key=lambda row: row["pk"]) if row["pk"])
            if current_key == primary_key:
                continue
            names = {str(row["name"]) for row in columns}
            self._conn.execute(f"DROP INDEX IF EXISTS {index_name}")
            legacy = f"{table}_legacy"
            self._conn.execute(f"DROP TABLE IF EXISTS {legacy}")
            self._conn.execute(f"ALTER TABLE {table} RENAME TO {legacy}")
            dataset_id_expr = "dataset_id" if "dataset_id" in names else "''"
            dataset_version_expr = "dataset_version" if "dataset_version" in names else "''"
            created_expr = "created_at" if "created_at" in names else "?"
            if table == "bars":
                self._conn.execute(
                    """
                    CREATE TABLE bars (
                        symbol TEXT NOT NULL,
                        timestamp TEXT NOT NULL,
                        payload_json TEXT NOT NULL,
                        dataset_id TEXT NOT NULL DEFAULT '',
                        dataset_version TEXT NOT NULL DEFAULT '',
                        created_at TEXT NOT NULL,
                        PRIMARY KEY (symbol, timestamp, dataset_id, dataset_version)
                    )
                    """
                )
                query = (
                    "INSERT INTO bars(symbol,timestamp,payload_json,dataset_id,dataset_version,created_at) "
                    f"SELECT symbol,timestamp,payload_json,{dataset_id_expr},{dataset_version_expr},{created_expr} "
                    "FROM bars_legacy"
                )
                self._conn.execute(query, (_now_iso(),) if created_expr == "?" else ())
                self._conn.execute("DROP TABLE bars_legacy")
                self._conn.execute("CREATE INDEX idx_bars_symbol_time ON bars(symbol, timestamp)")
            else:
                self._conn.execute(
                    """
                    CREATE TABLE snapshots (
                        key TEXT NOT NULL,
                        timestamp TEXT NOT NULL,
                        kind TEXT NOT NULL,
                        payload_json TEXT NOT NULL,
                        dataset_id TEXT NOT NULL DEFAULT '',
                        dataset_version TEXT NOT NULL DEFAULT '',
                        created_at TEXT NOT NULL,
                        PRIMARY KEY (key, timestamp, kind, dataset_id, dataset_version)
                    )
                    """
                )
                query = (
                    "INSERT INTO snapshots(key,timestamp,kind,payload_json,dataset_id,dataset_version,created_at) "
                    f"SELECT key,timestamp,kind,payload_json,{dataset_id_expr},{dataset_version_expr},{created_expr} "
                    "FROM snapshots_legacy"
                )
                self._conn.execute(query, (_now_iso(),) if created_expr == "?" else ())
                self._conn.execute("DROP TABLE snapshots_legacy")
                self._conn.execute("CREATE INDEX idx_snapshots_key_time ON snapshots(key, timestamp)")

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def __enter__(self) -> "AxiomStore":
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        self.close()

    # Dataset versions -------------------------------------------------
    def save_dataset(
        self,
        dataset_id: str,
        *args: Any,
        version: str | None = None,
        records: Any | None = None,
        metadata: Mapping[str, Any] | None = None,
        quality: Any | None = None,
    ) -> None:
        """Persist one immutable dataset version.

        The preferred form is ``save_dataset(id, version, records)``. For
        callers that naturally put the payload first,
        ``save_dataset(id, records, version="v1")`` is also accepted; an
        omitted version defaults to ``"1"``. Duplicate versions are rejected
        rather than replaced.
        """
        if len(args) > 2:
            raise TypeError("save_dataset accepts id, version and records")
        if len(args) == 2:
            if version is not None or records is not None:
                raise TypeError("version/records provided both positionally and by keyword")
            version, records = args
        elif len(args) == 1:
            if records is None:
                records = args[0]
                if version is None:
                    version = "1"
            elif version is None:
                version = args[0]
            else:
                raise TypeError("ambiguous dataset version and records")
        if version is None or records is None:
            raise TypeError("save_dataset requires records and a version")
        dataset_id, version = str(dataset_id), str(version)
        payload = _dump(records)
        metadata_json = _dump(dict(metadata or {}))
        quality_value = _enum_value(quality)
        def operation() -> None:
            with self._write_context():
                self._conn.execute(
                    "INSERT INTO datasets(dataset_id, version, payload_json, metadata_json, quality, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                    (dataset_id, version, payload, metadata_json, quality_value, _now_iso()),
                )

        try:
            sqlite_retry(operation, operation_name=f"save dataset {dataset_id}/{version}")
        except sqlite3.IntegrityError as exc:
            raise ValueError(f"dataset version already exists: {dataset_id}/{version}") from exc

    def save_dataset_version(self, dataset_id: str, version: str, records: Any, **kwargs: Any) -> None:
        """Explicit alias for :meth:`save_dataset`."""
        self.save_dataset(dataset_id, version, records, **kwargs)

    def load_dataset(self, dataset_id: str, version: str | None = None) -> Any | None:
        with self._lock:
            if version is None:
                row = self._conn.execute(
                    "SELECT payload_json FROM datasets WHERE dataset_id=? ORDER BY created_at DESC,rowid DESC,version DESC LIMIT 1",
                    (str(dataset_id),),
                ).fetchone()
            else:
                row = self._conn.execute(
                    "SELECT payload_json FROM datasets WHERE dataset_id=? AND version=?",
                    (str(dataset_id), str(version)),
                ).fetchone()
        if row is not None:
            return _load(row["payload_json"])
        catalog = self.load_dataset_catalog(str(dataset_id), version)
        return self._catalog_records(catalog) if catalog is not None else None

    def load_dataset_record(self, dataset_id: str, version: str | None = None) -> dict[str, Any] | None:
        """Return payload plus version, quality and metadata for dashboards."""
        with self._lock:
            if version is None:
                row = self._conn.execute(
                    "SELECT * FROM datasets WHERE dataset_id=? ORDER BY created_at DESC,rowid DESC,version DESC LIMIT 1",
                    (str(dataset_id),),
                ).fetchone()
            else:
                row = self._conn.execute(
                    "SELECT * FROM datasets WHERE dataset_id=? AND version=?",
                    (str(dataset_id), str(version)),
                ).fetchone()
        if row is not None:
            return {
                "dataset_id": row["dataset_id"],
                "version": row["version"],
                "records": _load(row["payload_json"]),
                "metadata": _load(row["metadata_json"]),
                "quality": row["quality"],
                "created_at": _parse_datetime(row["created_at"]),
            }
        catalog = self.load_dataset_catalog(str(dataset_id), version)
        if catalog is None:
            return None
        return {
            "dataset_id": catalog["dataset_id"],
            "version": catalog["dataset_version"],
            "records": self._catalog_records(catalog),
            "metadata": catalog["metadata"],
            "quality": catalog["quality"],
            "created_at": catalog["created_at"],
            "updated_at": catalog["updated_at"],
            "source_type": catalog["source_type"],
            "snapshot_id": catalog["snapshot_id"],
        }

    def load_dataset_by_version(self, version: str) -> dict[str, Any] | None:
        """Return the unique immutable dataset record carrying ``version``."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM datasets WHERE version=? ORDER BY created_at DESC,dataset_id DESC",
                (str(version),),
            ).fetchall()
        if rows:
            row = rows[0]
            return {
                "dataset_id": row["dataset_id"],
                "version": row["version"],
                "records": _load(row["payload_json"]),
                "metadata": _load(row["metadata_json"]),
                "quality": row["quality"],
                "created_at": _parse_datetime(row["created_at"]),
            }
        with self._lock:
            catalog_rows = self._conn.execute(
                "SELECT * FROM dataset_catalog WHERE dataset_version=? ORDER BY updated_at DESC,dataset_id DESC",
                (str(version),),
            ).fetchall()
        if not catalog_rows:
            return None
        catalog = _dataset_catalog_record(catalog_rows[0])
        return {
            "dataset_id": catalog["dataset_id"],
            "version": catalog["dataset_version"],
            "records": self._catalog_records(catalog),
            "metadata": catalog["metadata"],
            "quality": catalog["quality"],
            "created_at": catalog["created_at"],
            "updated_at": catalog["updated_at"],
            "source_type": catalog["source_type"],
            "snapshot_id": catalog["snapshot_id"],
        }

    def dataset_versions(self, dataset_id: str) -> list[str]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT version FROM datasets WHERE dataset_id=? "
                "UNION SELECT dataset_version AS version FROM dataset_catalog WHERE dataset_id=? "
                "ORDER BY version",
                (str(dataset_id), str(dataset_id)),
            ).fetchall()
        return [str(row["version"]) for row in rows]

    def save_dataset_catalog(
        self,
        dataset_id: str,
        dataset_version: str,
        *,
        provider: str,
        instrument: str,
        market_type: Any,
        timeframe: str = "",
        start_timestamp: datetime | None = None,
        end_timestamp: datetime | None = None,
        row_count: int = 0,
        completeness: float = 0.0,
        missing_ranges: Iterable[Any] = (),
        quality: Any = "UNKNOWN",
        source_type: str,
        snapshot_id: str,
        metadata: Mapping[str, Any] | None = None,
        created_at: datetime | None = None,
        updated_at: datetime | None = None,
    ) -> bool:
        """Persist one immutable dataset snapshot in the operator catalog."""

        identifier = str(dataset_id).strip()
        version = str(dataset_version).strip()
        provider_value = str(provider).strip()
        instrument_value = str(instrument).strip()
        market_value = _enum_value(market_type) or str(market_type).strip()
        timeframe_value = str(timeframe).strip()
        source_value = str(source_type).strip().upper()
        snapshot_value = str(snapshot_id).strip()
        if not identifier or not version or not provider_value or not instrument_value or not market_value:
            raise ValueError("dataset catalog identity fields are required")
        if source_value not in {"HISTORICAL", "FORWARD_COLLECTED"}:
            raise ValueError("dataset source_type must be HISTORICAL or FORWARD_COLLECTED")
        if not snapshot_value:
            raise ValueError("dataset catalog snapshot_id is required")
        if isinstance(row_count, bool) or not isinstance(row_count, int) or row_count < 0:
            raise ValueError("dataset catalog row_count must be a non-negative integer")
        completeness_value = float(completeness)
        if not math.isfinite(completeness_value) or not 0.0 <= completeness_value <= 1.0:
            raise ValueError("dataset catalog completeness must be in [0, 1]")
        metadata_json = _dump(dict(metadata or {}))
        missing_json = _dump(list(missing_ranges))
        created_iso = _iso(created_at or utc_now())
        updated_iso = _iso(updated_at or created_at or utc_now())
        values = (
            identifier,
            version,
            provider_value,
            instrument_value,
            market_value,
            timeframe_value,
            _iso(start_timestamp) if start_timestamp is not None else None,
            _iso(end_timestamp) if end_timestamp is not None else None,
            int(row_count),
            completeness_value,
            missing_json,
            _enum_value(quality) or str(quality),
            source_value,
            snapshot_value,
            created_iso,
            updated_iso,
            metadata_json,
        )
        def operation() -> bool:
            with self._write_context():
                existing = self._conn.execute(
                    "SELECT * FROM dataset_catalog WHERE dataset_id=? AND dataset_version=?",
                    (identifier, version),
                ).fetchone()
                if existing is not None:
                    immutable_columns = (
                        "provider",
                        "instrument",
                        "market_type",
                        "timeframe",
                        "start_timestamp",
                        "end_timestamp",
                        "row_count",
                        "completeness",
                        "missing_ranges_json",
                        "quality",
                        "source_type",
                        "snapshot_id",
                        "metadata_json",
                    )
                    expected = tuple(values[index] for index in (2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 16))
                    actual = tuple(existing[column] for column in immutable_columns)
                    if actual != expected:
                        raise ValueError(f"dataset catalog snapshot conflicts with stored payload: {identifier}/{version}")
                    return False
                self._conn.execute(
                    "INSERT INTO dataset_catalog("
                    "dataset_id,dataset_version,provider,instrument,market_type,timeframe,"
                    "start_timestamp,end_timestamp,row_count,completeness,missing_ranges_json,"
                    "quality,source_type,snapshot_id,created_at,updated_at,metadata_json"
                    ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    values,
                )
            return True

        return bool(
            sqlite_retry(
                operation,
                operation_name=f"save dataset catalog {identifier}/{version}",
            )
        )

    def load_dataset_catalog(self, dataset_id: str, dataset_version: str | None = None) -> dict[str, Any] | None:
        clauses = ["dataset_id=?"]
        values: list[Any] = [str(dataset_id)]
        if dataset_version is not None:
            clauses.append("dataset_version=?")
            values.append(str(dataset_version))
        query = "SELECT * FROM dataset_catalog WHERE " + " AND ".join(clauses)
        # Wall-clock timestamps can collide on fast immutable publishes; rowid
        # preserves insertion order so an exact tie still returns the newest
        # catalog rather than selecting by hash text.
        query += " ORDER BY updated_at DESC,created_at DESC,rowid DESC,dataset_version DESC LIMIT 1"
        with self._lock:
            row = self._conn.execute(query, values).fetchone()
        return _dataset_catalog_record(row) if row is not None else None

    def list_dataset_catalog(
        self,
        *,
        source_type: str | None = None,
        market_type: str | None = None,
        limit: int | None = 1000,
    ) -> list[dict[str, Any]]:
        if limit is not None and (isinstance(limit, bool) or not isinstance(limit, int) or limit < 0):
            raise ValueError("limit must be a non-negative integer or None")
        clauses: list[str] = []
        values: list[Any] = []
        if source_type is not None:
            clauses.append("source_type=?")
            values.append(str(source_type).strip().upper())
        if market_type is not None:
            clauses.append("market_type=?")
            values.append(_enum_value(market_type) or str(market_type))
        query = "SELECT * FROM dataset_catalog"
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY updated_at DESC,dataset_id,dataset_version"
        if limit is not None:
            query += " LIMIT ?"
            values.append(int(limit))
        with self._lock:
            rows = self._conn.execute(query, values).fetchall()
        return [_dataset_catalog_record(row) for row in rows]

    def save_dataset_bootstrap_state(self, dataset_id: str, payload: Mapping[str, Any]) -> None:
        identifier = str(dataset_id).strip()
        if not identifier:
            raise ValueError("dataset bootstrap state requires dataset_id")
        body = dict(payload)
        required = ("provider", "instrument", "market_type", "timeframe", "status")
        if any(not str(body.get(name, "")).strip() for name in required):
            raise ValueError("dataset bootstrap state is missing required fields")
        def operation() -> None:
            with self._write_context():
                self._conn.execute(
                    "INSERT INTO dataset_bootstrap_state("
                    "dataset_id,provider,instrument,market_type,timeframe,requested_start,requested_end,"
                    "next_timestamp,base_version,status,payload_json,updated_at"
                    ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?) "
                    "ON CONFLICT(dataset_id) DO UPDATE SET "
                    "provider=excluded.provider,instrument=excluded.instrument,market_type=excluded.market_type,"
                    "timeframe=excluded.timeframe,requested_start=excluded.requested_start,requested_end=excluded.requested_end,"
                    "next_timestamp=excluded.next_timestamp,base_version=excluded.base_version,status=excluded.status,"
                    "payload_json=excluded.payload_json,updated_at=excluded.updated_at",
                    (
                        identifier,
                        str(body["provider"]),
                        str(body["instrument"]),
                        _enum_value(body["market_type"]) or str(body["market_type"]),
                        str(body["timeframe"]),
                        _iso(body["requested_start"]) if isinstance(body.get("requested_start"), datetime) else body.get("requested_start"),
                        _iso(body["requested_end"]) if isinstance(body.get("requested_end"), datetime) else body.get("requested_end"),
                        _iso(body["next_timestamp"]) if isinstance(body.get("next_timestamp"), datetime) else body.get("next_timestamp"),
                        body.get("base_version"),
                        str(body["status"]).upper(),
                        _dump(body),
                        _now_iso(),
                    ),
                )

        sqlite_retry(
            operation,
            operation_name=f"save bootstrap state {identifier}",
        )

    def load_dataset_bootstrap_state(self, dataset_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM dataset_bootstrap_state WHERE dataset_id=?",
                (str(dataset_id),),
            ).fetchone()
        if row is None:
            return None
        payload = _load(row["payload_json"])
        result = dict(payload) if isinstance(payload, Mapping) else {}
        result.update(
            {
                "dataset_id": row["dataset_id"],
                "provider": row["provider"],
                "instrument": row["instrument"],
                "market_type": row["market_type"],
                "timeframe": row["timeframe"],
                "requested_start": _parse_datetime(row["requested_start"]),
                "requested_end": _parse_datetime(row["requested_end"]),
                "next_timestamp": _parse_datetime(row["next_timestamp"]),
                "base_version": row["base_version"],
                "status": row["status"],
                "updated_at": _parse_datetime(row["updated_at"]),
            }
        )
        return result

    def list_dataset_bootstrap_states(self, *, limit: int | None = 1000) -> list[dict[str, Any]]:
        if limit is not None and (isinstance(limit, bool) or not isinstance(limit, int) or limit < 0):
            raise ValueError("limit must be a non-negative integer or None")
        query = "SELECT dataset_id FROM dataset_bootstrap_state ORDER BY updated_at DESC"
        values: list[Any] = []
        if limit is not None:
            query += " LIMIT ?"
            values.append(int(limit))
        with self._lock:
            rows = self._conn.execute(query, values).fetchall()
        return [
            state
            for row in rows
            if (state := self.load_dataset_bootstrap_state(row["dataset_id"])) is not None
        ]

    def save_dataset_staging_bars(self, dataset_id: str, bars: Iterable[OHLCVBar]) -> dict[str, int]:
        identifier = str(dataset_id).strip()
        if not identifier:
            raise ValueError("staging dataset_id is required")
        normalized = []
        for bar in bars:
            if not isinstance(bar, OHLCVBar):
                raise TypeError("save_dataset_staging_bars expects OHLCVBar records")
            normalized.append((_iso(bar.timestamp), _dump(bar)))

        def operation() -> dict[str, int]:
            inserted = duplicates = 0
            with self._write_context():
                for timestamp, payload_json in normalized:
                    existing = self._conn.execute(
                        "SELECT payload_json FROM dataset_staging_bars WHERE dataset_id=? AND timestamp=?",
                        (identifier, timestamp),
                    ).fetchone()
                    if existing is not None:
                        if str(existing["payload_json"]) != payload_json:
                            raise ValueError(f"staged bar conflicts with stored payload: {identifier}/{timestamp}")
                        duplicates += 1
                        continue
                    self._conn.execute(
                        "INSERT INTO dataset_staging_bars(dataset_id,timestamp,payload_json,created_at) VALUES (?,?,?,?)",
                        (identifier, timestamp, payload_json, _now_iso()),
                    )
                    inserted += 1
            return {"inserted": inserted, "duplicates": duplicates}

        return sqlite_retry(
            operation,
            operation_name=f"stage bars {identifier}",
        )

    def load_dataset_staging_bars(self, dataset_id: str, *, limit: int | None = None) -> list[OHLCVBar]:
        if limit is not None and (isinstance(limit, bool) or not isinstance(limit, int) or limit < 0):
            raise ValueError("staging bar limit must be a non-negative integer or None")
        query = "SELECT payload_json FROM dataset_staging_bars WHERE dataset_id=? ORDER BY timestamp"
        values: list[Any] = [str(dataset_id)]
        if limit is not None:
            query += " LIMIT ?"
            values.append(int(limit))
        with self._lock:
            rows = self._conn.execute(query, values).fetchall()
        return [_bar_from_record(_load(row["payload_json"])) for row in rows]
    def clear_dataset_staging_bars(self, dataset_id: str) -> int:
        def operation() -> int:
            with self._write_context():
                cursor = self._conn.execute(
                    "DELETE FROM dataset_staging_bars WHERE dataset_id=?",
                    (str(dataset_id),),
                )
                return int(cursor.rowcount)

        return int(
            sqlite_retry(
                operation,
                operation_name=f"clear staged bars {str(dataset_id).strip()}",
            )
        )

    def save_historical_regime_labels(
        self,
        dataset_id: str,
        dataset_version: str,
        labels: Iterable[Mapping[str, Any]],
    ) -> int:
        rows = []
        for item in labels:
            if not isinstance(item, Mapping):
                raise TypeError("historical regime labels must be mappings")
            timestamp = _parse_datetime(item.get("timestamp"))
            if timestamp is None:
                raise ValueError("historical regime label timestamp is required")
            states = item.get("labels", ())
            confidence = item.get("confidence", {})
            rows.append(
                (
                    str(dataset_id),
                    str(dataset_version),
                    _iso(timestamp),
                    _dump(list(states) if isinstance(states, (list, tuple, set)) else [str(states)]),
                    _dump(dict(confidence) if isinstance(confidence, Mapping) else {}),
                    _now_iso(),
                )
            )
        self._insert_many(
            "historical_regime_labels",
            rows,
            "dataset_id,dataset_version,timestamp",
            ("dataset_id", "dataset_version", "timestamp", "labels_json", "confidence_json", "created_at"),
        )
        return len(rows)

    def load_historical_regime_labels(
        self,
        dataset_id: str,
        dataset_version: str,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        clauses = ["dataset_id=?", "dataset_version=?"]
        values: list[Any] = [str(dataset_id), str(dataset_version)]
        if start is not None:
            clauses.append("timestamp>=?")
            values.append(_iso(start))
        if end is not None:
            clauses.append("timestamp<=?")
            values.append(_iso(end))
        query = "SELECT timestamp,labels_json,confidence_json FROM historical_regime_labels WHERE " + " AND ".join(clauses)
        query += " ORDER BY timestamp"
        if limit is not None:
            if isinstance(limit, bool) or not isinstance(limit, int) or limit < 0:
                raise ValueError("regime label limit must be a non-negative integer")
            query += " LIMIT ?"
            values.append(int(limit))
        with self._lock:
            rows = self._conn.execute(query, values).fetchall()
        return [
            {
                "timestamp": _parse_datetime(row["timestamp"]),
                "labels": _load(row["labels_json"]),
                "confidence": _load(row["confidence_json"]),
            }
            for row in rows
        ]

    def _catalog_records(self, catalog: Mapping[str, Any]) -> list[Any]:
        market_type = str(catalog.get("market_type", "")).strip().lower()
        dataset_id = str(catalog.get("dataset_id", ""))
        version = str(catalog.get("dataset_version", catalog.get("version", "")))
        instrument = str(catalog.get("instrument", ""))
        if market_type == MarketType.CRYPTO_SPOT.value:
            records = self.load_bars(
                instrument,
                dataset_id=dataset_id,
                dataset_version=version,
            )
            if not records and "/" in instrument:
                records = self.load_bars(
                    instrument.replace("/", ""),
                    dataset_id=dataset_id,
                    dataset_version=version,
                )
            return records
        if dataset_id == "Polymarket-historical" and market_type == MarketType.PREDICTION.value:
            return self._aggregate_polymarket_catalog_records(catalog, version)
        metadata = catalog.get("metadata", {})
        if isinstance(metadata, Mapping):
            market_id = metadata.get("market_id")
            if market_id:
                rows = self.load_polymarket_snapshots(str(market_id), limit=None)
                records: list[dict[str, Any]] = []
                for row in rows:
                    payload = row.get("payload", {}) if isinstance(row, Mapping) else {}
                    if not isinstance(payload, Mapping) or str(payload.get("source_type", "")).upper() != "HISTORICAL":
                        continue
                    record = dict(payload)
                    record.update(
                        {
                            "snapshot_id": row.get("snapshot_id"),
                            "source_timestamp": row.get("source_timestamp"),
                            "observed_at": row.get("observed_at"),
                            "quality": row.get("quality"),
                            "dataset_id": dataset_id,
                            "dataset_version": version,
                        }
                    )
                    records.append(record)
                return records
        return []

    def _aggregate_polymarket_catalog_records(
        self,
        catalog: Mapping[str, Any],
        aggregate_version: str,
    ) -> list[dict[str, Any]]:
        """Reconstruct an aggregate only from its exact immutable constituents.

        The historical snapshot table predates dataset provenance columns.  Its
        rows can therefore be used for an aggregate only after their content,
        boundaries and count have been checked against the exact constituent
        catalog/version.  Any ambiguity fails closed rather than selecting a
        newer catalog or mixing forward observations into research input.
        """
        aggregate_metadata = catalog.get("metadata")
        if not isinstance(aggregate_metadata, Mapping):
            return []
        market_versions = aggregate_metadata.get("market_versions")
        if not isinstance(market_versions, Sequence) or isinstance(market_versions, (str, bytes)):
            return []
        aggregate_count = catalog.get("row_count")
        if isinstance(aggregate_count, bool) or not isinstance(aggregate_count, int) or aggregate_count < 0:
            return []
        aggregate_start = _parse_datetime(catalog.get("start_timestamp"))
        aggregate_end = _parse_datetime(catalog.get("end_timestamp"))
        if aggregate_count and (aggregate_start is None or aggregate_end is None):
            return []
        if not aggregate_count and (aggregate_start is not None or aggregate_end is not None):
            return []

        all_records: list[dict[str, Any]] = []
        seen_constituents: set[tuple[str, str]] = set()
        constituent_ranges: list[tuple[datetime, datetime]] = []
        for item in market_versions:
            if not isinstance(item, Mapping):
                return []
            market_id = str(item.get("market_id", "")).strip()
            constituent_id = str(item.get("dataset_id", f"prediction:{market_id}")).strip()
            constituent_version = str(item.get("version", item.get("dataset_version", ""))).strip()
            if (
                not market_id
                or not constituent_version
                or constituent_id != f"prediction:{market_id}"
                or (constituent_id, constituent_version) in seen_constituents
            ):
                return []
            seen_constituents.add((constituent_id, constituent_version))
            expected_count = item.get("records", item.get("row_count"))
            if isinstance(expected_count, bool) or not isinstance(expected_count, int) or expected_count < 0:
                return []

            constituent = self.load_dataset_catalog(constituent_id, constituent_version)
            if constituent is None or not self._valid_polymarket_constituent_catalog(
                constituent, market_id, constituent_id, constituent_version, expected_count, item
            ):
                return []
            constituent_records = self._load_exact_polymarket_constituent(
                constituent, market_id, constituent_id, constituent_version
            )
            if constituent_records is None or len(constituent_records) != expected_count:
                return []
            if constituent_records:
                first = _parse_datetime(constituent_records[0].get("source_timestamp"))
                last = _parse_datetime(constituent_records[-1].get("source_timestamp"))
                start = _parse_datetime(constituent.get("start_timestamp"))
                end = _parse_datetime(constituent.get("end_timestamp"))
                if first is None or last is None or start is None or end is None or first != start or last != end:
                    return []
                constituent_ranges.append((first, last))
            elif constituent.get("start_timestamp") is not None or constituent.get("end_timestamp") is not None:
                return []
            all_records.extend(
                self._with_polymarket_provenance(
                    record,
                    aggregate_dataset_id=str(catalog.get("dataset_id", "Polymarket-historical")),
                    aggregate_dataset_version=aggregate_version,
                    constituent_dataset_id=constituent_id,
                    constituent_dataset_version=constituent_version,
                    market_id=market_id,
                    quality=str(constituent.get("quality") or "PRICE_PROXY"),
                )
                for record in constituent_records
            )

        if len(all_records) != aggregate_count:
            return []
        if all_records:
            observed_starts = [bounds[0] for bounds in constituent_ranges]
            observed_ends = [bounds[1] for bounds in constituent_ranges]
            if (
                not observed_starts
                or aggregate_start != min(observed_starts)
                or aggregate_end != max(observed_ends)
            ):
                return []
        return all_records

    @staticmethod
    def _valid_polymarket_constituent_catalog(
        constituent: Mapping[str, Any],
        market_id: str,
        constituent_id: str,
        version: str,
        expected_count: int,
        aggregate_item: Mapping[str, Any],
    ) -> bool:
        if (
            str(constituent.get("dataset_id", "")) != constituent_id
            or str(constituent.get("dataset_version", "")) != version
            or str(constituent.get("market_type", "")).strip().lower() != MarketType.PREDICTION.value
            or str(constituent.get("source_type", "")).strip().upper() != "HISTORICAL"
            or constituent.get("row_count") != expected_count
        ):
            return False
        metadata = constituent.get("metadata")
        if not isinstance(metadata, Mapping):
            return False
        if str(metadata.get("market_id", metadata.get("polymarket_key", ""))).strip() != market_id:
            return False
        for key in ("category", "question", "resolution_criteria", "settlement", "token_ids"):
            if key in aggregate_item and key in metadata and aggregate_item[key] != metadata[key]:
                return False
        if "historical_order_book" in aggregate_item and "historical_order_book_available" in metadata:
            if bool(aggregate_item["historical_order_book"]) != bool(metadata["historical_order_book_available"]):
                return False
        return True

    def _load_exact_polymarket_constituent(
        self,
        constituent: Mapping[str, Any],
        market_id: str,
        constituent_id: str,
        constituent_version: str,
    ) -> list[dict[str, Any]] | None:
        """Load a constituent by exact version, falling back only to checked legacy rows."""
        with self._lock:
            immutable = self._conn.execute(
                "SELECT payload_json,metadata_json,quality FROM datasets "
                "WHERE dataset_id=? AND version=?",
                (constituent_id, constituent_version),
            ).fetchone()
        if immutable is not None:
            try:
                values = _load(immutable["payload_json"])
                dataset_metadata = _load(immutable["metadata_json"])
            except (TypeError, ValueError, json.JSONDecodeError):
                return None
            if not isinstance(values, Sequence) or isinstance(values, (str, bytes, Mapping)):
                return None
            if isinstance(dataset_metadata, Mapping):
                catalog_metadata = constituent.get("metadata")
                if isinstance(catalog_metadata, Mapping):
                    for key in ("market_id", "category", "question", "resolution_criteria", "settlement", "token_ids"):
                        if key in dataset_metadata and key in catalog_metadata and dataset_metadata[key] != catalog_metadata[key]:
                            return None
            result: list[dict[str, Any]] = []
            for value in values:
                if not isinstance(value, Mapping):
                    return None
                if str(value.get("source_type", "HISTORICAL")).strip().upper() != "HISTORICAL":
                    return None
                if value.get("market_id") is not None and str(value["market_id"]).strip() != market_id:
                    return None
                record = dict(value)
                timestamp = _parse_datetime(record.get("source_timestamp", record.get("timestamp", record.get("time"))))
                if timestamp is None:
                    return None
                record["source_timestamp"] = timestamp
                record.setdefault("market_id", market_id)
                record["quality"] = record.get("quality") or immutable["quality"] or constituent.get("quality") or "PRICE_PROXY"
                result.append(record)
            result.sort(key=lambda value: (_parse_datetime(value["source_timestamp"]) or datetime.min.replace(tzinfo=timezone.utc), _dump(value)))
            return result

        rows = self.load_polymarket_snapshots(market_id, limit=None)
        historical: list[dict[str, Any]] = []
        for row in rows:
            payload = row.get("payload") if isinstance(row, Mapping) else None
            if not isinstance(payload, Mapping):
                continue
            if str(payload.get("source_type", "")).strip().upper() != "HISTORICAL":
                continue
            if payload.get("market_id") is not None and str(payload["market_id"]).strip() != market_id:
                return None
            source_timestamp = _parse_datetime(row.get("source_timestamp"))
            if source_timestamp is None:
                return None
            payload_timestamp = _parse_datetime(payload.get("timestamp", payload.get("time")))
            if payload_timestamp is not None and payload_timestamp != source_timestamp:
                return None
            historical.append(
                {
                    "payload": payload,
                    "snapshot_id": row.get("snapshot_id"),
                    "source_timestamp": source_timestamp,
                    "observed_at": row.get("observed_at"),
                    "quality": row.get("quality"),
                }
            )
        if not historical:
            return []
        expected_start = _parse_datetime(constituent.get("start_timestamp"))
        expected_end = _parse_datetime(constituent.get("end_timestamp"))
        if expected_start is None or expected_end is None:
            return None
        if historical[0]["source_timestamp"] != expected_start or historical[-1]["source_timestamp"] != expected_end:
            return None
        if len(historical) != int(constituent.get("row_count", -1)):
            return None

        metadata = constituent.get("metadata")
        token_default = market_id
        if isinstance(metadata, Mapping):
            token_ids = metadata.get("token_ids")
            if isinstance(token_ids, Mapping):
                token_default = str(token_ids.get("yes") or market_id)
        identities: list[dict[str, Any]] = []
        result = []
        for row in historical:
            payload = row["payload"]
            price = payload.get("price", payload.get("p", payload.get("yes_mid", payload.get("value"))))
            try:
                price_value = float(price)
            except (TypeError, ValueError):
                return None
            if not math.isfinite(price_value) or not 0.0 <= price_value <= 1.0:
                return None
            token_id = str(payload.get("token_id", payload.get("asset_id", token_default)) or token_default)
            if not token_id:
                return None
            identity: dict[str, Any] = {
                "timestamp": row["source_timestamp"],
                "price": price_value,
                "token_id": token_id,
            }
            order_book = payload.get("order_book", payload.get("book"))
            if order_book is not None:
                identity["order_book"] = order_book
            identities.append(identity)
            record = dict(payload)
            record.setdefault("timestamp", row["source_timestamp"])
            record.setdefault("yes_mid", price_value)
            record["market_id"] = market_id
            record["snapshot_id"] = row.get("snapshot_id")
            record["source_timestamp"] = row["source_timestamp"]
            record["observed_at"] = row.get("observed_at")
            record["quality"] = row.get("quality") or payload.get("quality") or payload.get("research_quality") or constituent.get("quality") or "PRICE_PROXY"
            result.append(record)
        if "sha256:" + hashlib.sha256(_dump(identities).encode("utf-8")).hexdigest() != str(constituent.get("dataset_version", "")):
            return None
        return result

    @staticmethod
    def _with_polymarket_provenance(
        record: Mapping[str, Any],
        *,
        aggregate_dataset_id: str,
        aggregate_dataset_version: str,
        constituent_dataset_id: str,
        constituent_dataset_version: str,
        market_id: str,
        quality: str,
    ) -> dict[str, Any]:
        result = dict(record)
        result.setdefault("market_id", market_id)
        result.setdefault("source_timestamp", _parse_datetime(result.get("timestamp")))
        result["aggregate_dataset_id"] = aggregate_dataset_id
        result["aggregate_dataset_version"] = aggregate_dataset_version
        result["constituent_dataset_id"] = constituent_dataset_id
        result["constituent_dataset_version"] = constituent_dataset_version
        result["dataset_id"] = aggregate_dataset_id
        result["dataset_version"] = aggregate_dataset_version
        result["quality"] = result.get("quality") or result.get("research_quality") or quality
        if result.get("yes_mid") is None and result.get("price") is not None:
            result["yes_mid"] = result["price"]
        return result
    # Canonical market records ----------------------------------------
    def save_bars(
        self,
        symbol: str,
        bars: Iterable[OHLCVBar],
        *,
        dataset_id: str | None = None,
        dataset_version: str | None = None,
    ) -> int:
        rows = []
        dataset_id_value = "" if dataset_id is None else str(dataset_id)
        dataset_version_value = "" if dataset_version is None else str(dataset_version)
        for bar in bars:
            if not isinstance(bar, OHLCVBar):
                raise TypeError("save_bars expects OHLCVBar records")
            rows.append(
                (
                    str(symbol),
                    _iso(bar.timestamp),
                    _dump(bar),
                    dataset_id_value,
                    dataset_version_value,
                    _now_iso(),
                )
            )
        self._insert_many(
            "bars",
            rows,
            "symbol,timestamp,dataset_id,dataset_version",
            ("symbol", "timestamp", "payload_json", "dataset_id", "dataset_version", "created_at"),
        )
        return len(rows)
    def publish_dataset_bars_chunk(
        self,
        symbol: str,
        bars: Iterable[OHLCVBar],
        *,
        dataset_id: str,
        dataset_version: str,
    ) -> dict[str, int]:
        """Idempotently copy one staged publication chunk into immutable bars."""
        identifier = str(dataset_id).strip()
        version = str(dataset_version).strip()
        if not identifier or not version:
            raise ValueError("published bars require dataset identity")
        normalized = []
        for bar in bars:
            if not isinstance(bar, OHLCVBar):
                raise TypeError("publish_dataset_bars_chunk expects OHLCVBar records")
            normalized.append(
                (
                    str(symbol),
                    _iso(bar.timestamp),
                    _dump(bar),
                    identifier,
                    version,
                )
            )

        def operation() -> dict[str, int]:
            inserted = duplicates = 0
            with self._write_context():
                for symbol_value, timestamp, payload_json, dataset_value, version_value in normalized:
                    existing = self._conn.execute(
                        "SELECT payload_json FROM bars WHERE symbol=? AND timestamp=? "
                        "AND dataset_id=? AND dataset_version=?",
                        (symbol_value, timestamp, dataset_value, version_value),
                    ).fetchone()
                    if existing is not None:
                        if str(existing["payload_json"]) != payload_json:
                            raise ValueError(
                                "published bar conflicts with stored payload: "
                                f"{dataset_value}/{version_value}/{timestamp}"
                            )
                        duplicates += 1
                        continue
                    self._conn.execute(
                        "INSERT INTO bars(symbol,timestamp,payload_json,dataset_id,dataset_version,created_at) "
                        "VALUES (?,?,?,?,?,?)",
                        (
                            symbol_value,
                            timestamp,
                            payload_json,
                            dataset_value,
                            version_value,
                            _now_iso(),
                        ),
                    )
                    inserted += 1
            return {"inserted": inserted, "duplicates": duplicates}

        return sqlite_retry(
            operation,
            operation_name=f"publish bars {identifier}/{version}",
        )

    def count_bars(
        self,
        symbol: str,
        *,
        dataset_id: str | None = None,
        dataset_version: str | None = None,
    ) -> int:
        clauses = ["symbol=?"]
        values: list[Any] = [str(symbol)]
        if dataset_id is not None:
            clauses.append("dataset_id=?")
            values.append(str(dataset_id))
        if dataset_version is not None:
            clauses.append("dataset_version=?")
            values.append(str(dataset_version))
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) AS n FROM bars WHERE " + " AND ".join(clauses),
                values,
            ).fetchone()
        return int(row["n"]) if row is not None else 0

    def load_bars(
        self,
        symbol: str,
        start: datetime | None = None,
        end: datetime | None = None,
        *,
        dataset_id: str | None = None,
        dataset_version: str | None = None,
        limit: int | None = None,
    ) -> list[OHLCVBar]:
        clauses = ["symbol=?"]
        values: list[Any] = [str(symbol)]
        if dataset_id is not None:
            clauses.append("dataset_id=?")
            values.append(str(dataset_id))
        if dataset_version is not None:
            clauses.append("dataset_version=?")
            values.append(str(dataset_version))
        if start is not None:
            clauses.append("timestamp>=?")
            values.append(_iso(start))
        if end is not None:
            clauses.append("timestamp<=?")
            values.append(_iso(end))
        query = "SELECT payload_json FROM bars WHERE " + " AND ".join(clauses) + " ORDER BY timestamp,dataset_id,dataset_version"
        if limit is not None:
            if isinstance(limit, bool) or not isinstance(limit, int) or limit < 0:
                raise ValueError("limit must be a non-negative integer")
            query += " LIMIT ?"
            values.append(int(limit))
        with self._lock:
            rows = self._conn.execute(query, values).fetchall()
        return [_bar_from_record(_load(row["payload_json"])) for row in rows]

    def save_snapshots(
        self,
        key: str,
        snapshots: Iterable[Any],
        *,
        dataset_id: str | None = None,
        dataset_version: str | None = None,
    ) -> int:
        rows = []
        dataset_id_value = "" if dataset_id is None else str(dataset_id)
        dataset_version_value = "" if dataset_version is None else str(dataset_version)
        for snapshot in snapshots:
            kind, timestamp = _snapshot_kind(snapshot)
            if timestamp is None:
                raise ValueError("snapshot timestamp is required")
            rows.append(
                (
                    str(key),
                    _iso(timestamp),
                    kind,
                    _dump(snapshot),
                    dataset_id_value,
                    dataset_version_value,
                    _now_iso(),
                )
            )
        self._insert_many(
            "snapshots",
            rows,
            "key,timestamp,kind,dataset_id,dataset_version",
            ("key", "timestamp", "kind", "payload_json", "dataset_id", "dataset_version", "created_at"),
        )
        return len(rows)

    def save_snapshot(self, key: str, snapshot: Any, **kwargs: Any) -> int:
        return self.save_snapshots(key, (snapshot,), **kwargs)

    def load_snapshots(
        self,
        key: str,
        start: datetime | None = None,
        end: datetime | None = None,
        *,
        dataset_id: str | None = None,
        dataset_version: str | None = None,
        kind: str | None = None,
        limit: int | None = None,
    ) -> list[Any]:
        clauses = ["key=?"]
        values: list[Any] = [str(key)]
        if start is not None:
            clauses.append("timestamp>=?")
            values.append(_iso(start))
        if end is not None:
            clauses.append("timestamp<=?")
            values.append(_iso(end))
        if dataset_id is not None:
            clauses.append("dataset_id=?")
            values.append(str(dataset_id))
        if dataset_version is not None:
            clauses.append("dataset_version=?")
            values.append(str(dataset_version))
        if kind is not None:
            clauses.append("kind=?")
            values.append(str(kind))
        query = "SELECT kind,payload_json FROM snapshots WHERE " + " AND ".join(clauses) + " ORDER BY timestamp,dataset_id,dataset_version"
        if limit is not None:
            if isinstance(limit, bool) or not isinstance(limit, int) or limit < 0:
                raise ValueError("limit must be a non-negative integer")
            query += " LIMIT ?"
            values.append(limit)
        with self._lock:
            rows = self._conn.execute(query, values).fetchall()
        return [_snapshot_from_record(row["kind"], _load(row["payload_json"])) for row in rows]

    def save_prediction_snapshots(self, key: str, snapshots: Iterable[PredictionMarketSnapshot], **kwargs: Any) -> int:
        return self.save_snapshots(key, snapshots, **kwargs)

    def load_prediction_snapshots(self, key: str, **kwargs: Any) -> list[PredictionMarketSnapshot]:
        return [item for item in self.load_snapshots(key, kind="prediction", **kwargs) if isinstance(item, PredictionMarketSnapshot)]
    # Continuous Polymarket collection --------------------------------
    def save_polymarket_market_metadata(
        self,
        market_id: str,
        payload: Any,
        *,
        observed_at: datetime,
        metadata_hash: str | None = None,
        source_type: str | None = None,
    ) -> bool:
        identifier = str(market_id).strip()
        if not identifier:
            raise ValueError("market_id is required")
        source_value = _polymarket_source_type(source_type, payload)
        if isinstance(payload, Mapping):
            payload = dict(payload)
            payload.setdefault("source_type", source_value)
        observed = _iso(observed_at)
        payload_json = _dump(payload)
        digest = str(metadata_hash or hashlib.sha256(payload_json.encode("utf-8")).hexdigest())
        with self._write_context():
            existing = self._conn.execute(
                "SELECT payload_json,source_type FROM polymarket_markets WHERE market_id=? AND metadata_hash=? LIMIT 1",
                (identifier, digest),
            ).fetchone()
            if existing is not None:
                existing_payload = _load(existing["payload_json"])
                existing_identity = (
                    {key: value for key, value in existing_payload.items() if key != "observed_at"}
                    if isinstance(existing_payload, Mapping)
                    else existing_payload
                )
                current_identity = (
                    {key: value for key, value in payload.items() if key != "observed_at"}
                    if isinstance(payload, Mapping)
                    else payload
                )
                if _dump(existing_identity) != _dump(current_identity) or str(existing["source_type"]).upper() != source_value:
                    raise ValueError(f"metadata hash conflicts with stored payload: {identifier}/{digest}")
                return False
            self._conn.execute(
                "INSERT INTO polymarket_markets(market_id,observed_at,metadata_hash,payload_json,source_type,created_at) VALUES (?,?,?,?,?,?)",
                (identifier, observed, digest, payload_json, source_value, _now_iso()),
            )
        return True

    def save_polymarket_snapshot(
        self,
        snapshot_id: str,
        market_id: str,
        source_timestamp: datetime,
        observed_at: datetime,
        payload: Any,
        *,
        quality: Any = "ORDER_BOOK_SIMULATED",
        source_type: str | None = None,
    ) -> bool:
        identifier = str(market_id).strip()
        if not identifier:
            raise ValueError("market_id is required")
        snapshot_key = str(snapshot_id).strip()
        if not snapshot_key:
            raise ValueError("snapshot_id is required")
        source_value = _polymarket_source_type(source_type, payload)
        if isinstance(payload, Mapping):
            payload = dict(payload)
            payload.setdefault("source_type", source_value)
        source = _iso(source_timestamp)
        observed = _iso(observed_at)
        payload_json = _dump(payload)
        quality_value = _enum_value(quality) or "ORDER_BOOK_SIMULATED"
        with self._write_context():
            existing = self._conn.execute(
                "SELECT market_id,source_timestamp,observed_at,payload_json,quality,source_type FROM polymarket_snapshots WHERE snapshot_id=?",
                (snapshot_key,),
            ).fetchone()
            if existing is not None:
                if (
                    str(existing["market_id"]) != identifier
                    or str(existing["source_timestamp"]) != source
                    or str(existing["observed_at"]) != observed
                    or str(existing["payload_json"]) != payload_json
                    or str(existing["quality"]) != quality_value
                    or str(existing["source_type"]).upper() != source_value
                ):
                    raise ValueError(f"snapshot id conflicts with stored payload: {snapshot_key}")
                return False
            self._conn.execute(
                "INSERT INTO polymarket_snapshots(snapshot_id,market_id,source_timestamp,observed_at,payload_json,quality,source_type,created_at) VALUES (?,?,?,?,?,?,?,?)",
                (snapshot_key, identifier, source, observed, payload_json, quality_value, source_value, _now_iso()),
            )
        return True

    def save_polymarket_trade(
        self,
        market_id: str,
        trade: TradePrint | Mapping[str, Any],
        *,
        trade_key: str | None = None,
    ) -> bool:
        identifier = str(market_id).strip()
        if not identifier:
            raise ValueError("market_id is required")
        payload_json = _dump(trade)
        if isinstance(trade, TradePrint):
            timestamp = trade.timestamp
            supplied_key = trade.trade_id
        elif isinstance(trade, Mapping):
            timestamp = _parse_datetime(trade.get("timestamp")) or _parse_datetime(trade.get("time"))
            if timestamp is None:
                raise ValueError("trade timestamp is required")
            supplied_key = trade.get("trade_id", trade.get("id"))
        else:
            raise TypeError("trade must be TradePrint or mapping")
        supplied_key_text = str(trade_key).strip() if trade_key is not None else ""
        if trade_key is not None and not supplied_key_text:
            raise ValueError("trade key is required")
        if trade_key is not None:
            key = f"{identifier}|{supplied_key_text}"
        elif supplied_key:
            key = f"{identifier}|{str(supplied_key).strip()}"
        else:
            key = hashlib.sha256(_dump({"market_id": identifier, "trade": trade}).encode("utf-8")).hexdigest()
        if not key:
            raise ValueError("trade key is required")
        with self._write_context():
            existing = self._conn.execute(
                "SELECT market_id,timestamp,payload_json FROM polymarket_trades WHERE trade_key=?",
                (key,),
            ).fetchone()
            if existing is not None:
                if (
                    str(existing["market_id"]) != identifier
                    or str(existing["timestamp"]) != _iso(timestamp)
                    or str(existing["payload_json"]) != payload_json
                ):
                    raise ValueError(f"trade key conflicts with stored payload: {key}")
                return False
            self._conn.execute(
                "INSERT INTO polymarket_trades(trade_key,market_id,timestamp,payload_json,created_at) VALUES (?,?,?,?,?)",
                (key, identifier, _iso(timestamp), payload_json, _now_iso()),
            )
        return True

    def load_polymarket_snapshots(
        self,
        market_id: str | None = None,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
        source_start: datetime | None = None,
        source_end: datetime | None = None,
        source_after: tuple[datetime, str] | None = None,
        source_type: str | None = None,
        latest: bool = False,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        values: list[Any] = []
        if market_id is not None:
            clauses.append("market_id=?")
            values.append(str(market_id))
        if start is not None:
            clauses.append("observed_at>=?")
            values.append(_iso(start))
        if end is not None:
            clauses.append("observed_at<=?")
            values.append(_iso(end))
        if source_start is not None:
            clauses.append("source_timestamp>=?")
            values.append(_iso(source_start))
        if source_end is not None:
            clauses.append("source_timestamp<=?")
            values.append(_iso(source_end))
        if source_after is not None:
            if not isinstance(source_after, (tuple, list)) or len(source_after) != 2:
                raise ValueError("source_after must contain timestamp and snapshot id")
            after_timestamp, after_snapshot_id = source_after
            clauses.append("(source_timestamp>? OR (source_timestamp=? AND snapshot_id>?))")
            values.extend([_iso(after_timestamp), _iso(after_timestamp), str(after_snapshot_id)])
        if source_type is not None:
            clauses.append("source_type=?")
            values.append(_polymarket_source_type(source_type))
        query = "SELECT snapshot_id,market_id,source_timestamp,observed_at,payload_json,quality,source_type FROM polymarket_snapshots"
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += (
            " ORDER BY observed_at DESC,market_id,source_timestamp DESC,snapshot_id DESC"
            if latest
            else (
                " ORDER BY source_timestamp,market_id,snapshot_id"
                if source_after is not None or source_start is not None or source_end is not None
                else " ORDER BY observed_at,market_id,source_timestamp,snapshot_id"
            )
        )
        if limit is not None:
            if isinstance(limit, bool) or not isinstance(limit, int) or limit < 0:
                raise ValueError("limit must be a non-negative integer")
            query += " LIMIT ?"
            values.append(limit)
        with self._lock:
            rows = self._conn.execute(query, values).fetchall()
        return [
            {
                "snapshot_id": row["snapshot_id"],
                "market_id": row["market_id"],
                "source_timestamp": _parse_datetime(row["source_timestamp"]),
                "observed_at": _parse_datetime(row["observed_at"]),
                "payload": _load(row["payload_json"]),
                "quality": row["quality"],
                "source_type": str(row["source_type"]).upper(),
            }
            for row in rows
        ]
    def load_latest_polymarket_snapshots(
        self,
        market_ids: Sequence[str] | None = None,
        *,
        source_type: str | None = None,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 0:
            raise ValueError("limit must be a non-negative integer")
        identifiers = tuple(dict.fromkeys(str(item).strip() for item in (market_ids or ()) if str(item).strip()))
        if market_ids is not None and not identifiers:
            return []
        clauses: list[str] = []
        values: list[Any] = []
        if identifiers:
            placeholders = ",".join("?" for _ in identifiers)
            clauses.append(f"market_id IN ({placeholders})")
            values.extend(identifiers)
        if source_type is not None:
            clauses.append("source_type=?")
            values.append(_polymarket_source_type(source_type))
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        query = (
            "SELECT snapshot_id,market_id,source_timestamp,observed_at,payload_json,quality,source_type FROM ("
            "SELECT p.*, ROW_NUMBER() OVER (PARTITION BY market_id "
            "ORDER BY observed_at DESC,source_timestamp DESC,snapshot_id DESC) AS row_number "
            "FROM polymarket_snapshots p"
            f"{where}"
            ") WHERE row_number=1 ORDER BY market_id LIMIT ?"
        )
        values.append(int(limit))
        with self._lock:
            rows = self._conn.execute(query, values).fetchall()
        return [
            {
                "snapshot_id": row["snapshot_id"],
                "market_id": row["market_id"],
                "source_timestamp": _parse_datetime(row["source_timestamp"]),
                "observed_at": _parse_datetime(row["observed_at"]),
                "payload": _load(row["payload_json"]),
                "quality": row["quality"],
                "source_type": str(row["source_type"]).upper(),
            }
            for row in rows
        ]

    def load_polymarket_trades(
        self,
        market_id: str | None = None,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> list[TradePrint | Mapping[str, Any]]:
        clauses: list[str] = []
        values: list[Any] = []
        if market_id is not None:
            clauses.append("market_id=?")
            values.append(str(market_id))
        if start is not None:
            clauses.append("timestamp>=?")
            values.append(_iso(start))
        if end is not None:
            clauses.append("timestamp<=?")
            values.append(_iso(end))
        query = "SELECT payload_json FROM polymarket_trades"
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY timestamp,trade_key"
        with self._lock:
            rows = self._conn.execute(query, values).fetchall()
        result: list[TradePrint | Mapping[str, Any]] = []
        for row in rows:
            payload = _load(row["payload_json"])
            try:
                result.append(_trade_from_record(payload))
            except (KeyError, TypeError, ValueError):
                result.append(payload)
        return result


    def get_collector_state(self, collector_name: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT state_json FROM collector_state WHERE collector_name=?", (str(collector_name),)
            ).fetchone()
        return _load(row["state_json"]) if row else None

    def set_collector_state(self, collector_name: str, state: Mapping[str, Any]) -> None:
        with self._write_context():
            self._conn.execute(
                "INSERT INTO collector_state(collector_name,state_json,updated_at) VALUES (?,?,?) "
                "ON CONFLICT(collector_name) DO UPDATE SET state_json=excluded.state_json,updated_at=excluded.updated_at",
                (str(collector_name), _dump(dict(state)), _now_iso()),
            )

    def save_collection_error(
        self,
        market_id: str | None,
        observed_at: datetime,
        kind: str,
        detail: str,
        payload: Any = None,
        *,
        source_type: str | None = None,
    ) -> str:
        source_value = _polymarket_source_type(source_type, payload)
        if isinstance(payload, Mapping):
            payload = dict(payload)
            payload.setdefault("source_type", source_value)
        body = {"market_id": market_id, "kind": str(kind), "detail": str(detail), "payload": payload, "source_type": source_value}
        error_id = hashlib.sha256(_dump(body | {"observed_at": _iso(observed_at)}).encode("utf-8")).hexdigest()
        with self._write_context():
            self._conn.execute(
                "INSERT OR IGNORE INTO collection_errors(error_id,market_id,observed_at,kind,detail,payload_json,source_type,created_at) VALUES (?,?,?,?,?,?,?,?)",
                (error_id, str(market_id) if market_id is not None else None, _iso(observed_at), str(kind), str(detail), _dump(payload), source_value, _now_iso()),
            )
        return error_id

    def list_collection_errors(self, market_id: str | None = None) -> list[dict[str, Any]]:
        query = "SELECT error_id,market_id,observed_at,kind,detail,payload_json,source_type FROM collection_errors"
        values: tuple[Any, ...] = ()
        if market_id is not None:
            query += " WHERE market_id=?"
            values = (str(market_id),)
        query += " ORDER BY observed_at,error_id"
        with self._lock:
            rows = self._conn.execute(query, values).fetchall()
        return [
            {
                "error_id": row["error_id"],
                "market_id": row["market_id"],
                "observed_at": _parse_datetime(row["observed_at"]),
                "kind": row["kind"],
                "detail": row["detail"],
                "payload": _load(row["payload_json"]),
                "source_type": str(row["source_type"]).upper(),
            }
            for row in rows
        ]

    def save_forward_test(self, experiment_id: str, spec: Mapping[str, Any]) -> bool:
        identifier = str(experiment_id).strip()
        if not identifier:
            raise ValueError("experiment_id is required")
        strategy_hash = str(spec.get("strategy_hash", "")).strip()
        model_hash = str(spec.get("model_hash", "")).strip()
        if not strategy_hash or not model_hash:
            raise ValueError("frozen forward tests require strategy_hash and model_hash")
        start_timestamp = _parse_datetime(spec.get("start_timestamp"))
        if start_timestamp is None:
            raise ValueError("frozen forward test start_timestamp is required")
        bankroll = float(spec.get("bankroll", 0.0))
        if not math.isfinite(bankroll) or bankroll <= 0:
            raise ValueError("frozen forward test bankroll must be finite and positive")
        config = spec.get("config", {})
        if not isinstance(config, Mapping):
            raise ValueError("frozen forward test config must be a mapping")
        public_config: dict[str, Any] = {}
        for key, value in config.items():
            normalized = str(key).replace("-", "_").lower()
            if normalized in {"live", "live_execution"}:
                if value is not None and (not isinstance(value, bool) or value):
                    raise ValueError("frozen forward tests are paper-only")
                continue
            if normalized == "execution":
                if value not in (None, "paper_only"):
                    raise ValueError("frozen forward tests are paper-only")
                continue
            public_config[str(key)] = value
        try:
            from .research_bus import _validate_payload

            _validate_payload(public_config)
        except (TypeError, ValueError) as exc:
            raise ValueError("frozen forward test config contains forbidden private or execution fields") from exc
        config_json = _dump(config)
        try:
            from .forward import _validate_private_fields

            _validate_private_fields(spec.get("risk_limits", {}), path="risk_limits")
        except (TypeError, ValueError) as exc:
            raise ValueError("frozen forward test risk limits contain forbidden private fields") from exc
        allowed_json = _dump(spec.get("allowed_markets", []))
        limits_json = _dump(spec.get("risk_limits", {}))
        quality = _enum_value(spec.get("quality")) or "PAPER_FORWARD"
        if quality != ResearchQuality.PAPER_FORWARD.value:
            raise ValueError("frozen forward tests must use PAPER_FORWARD quality")
        values = (
            identifier,
            strategy_hash,
            model_hash,
            config_json,
            _iso(start_timestamp),
            bankroll,
            allowed_json,
            limits_json,
            quality,
            _now_iso(),
        )
        with self._write_context():
            row = self._conn.execute("SELECT * FROM forward_tests WHERE experiment_id=?", (identifier,)).fetchone()
            if row is not None:
                immutable = (
                    row["strategy_hash"],
                    row["model_hash"],
                    row["config_json"],
                    row["start_timestamp"],
                    float(row["bankroll"]),
                    row["allowed_markets_json"],
                    row["risk_limits_json"],
                    row["quality"],
                )
                if immutable != values[1:9]:
                    raise ValueError(f"forward test is frozen: {identifier}")
                return False
            self._conn.execute(
                "INSERT INTO forward_tests(experiment_id,strategy_hash,model_hash,config_json,start_timestamp,bankroll,allowed_markets_json,risk_limits_json,quality,created_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
                values,
            )
        return True

    def load_forward_tests(self, *, limit: int = 1000) -> list[dict[str, Any]]:
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 0:
            raise ValueError("limit must be a non-negative integer")
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM forward_tests ORDER BY start_timestamp,experiment_id LIMIT ?",
                (int(limit),),
            ).fetchall()
        return [
            {
                "experiment_id": row["experiment_id"],
                "strategy_hash": row["strategy_hash"],
                "model_hash": row["model_hash"],
                "config": _load(row["config_json"]),
                "start_timestamp": _parse_datetime(row["start_timestamp"]),
                "bankroll": float(row["bankroll"]),
                "allowed_markets": _load(row["allowed_markets_json"]),
                "risk_limits": _load(row["risk_limits_json"]),
                "quality": row["quality"],
                "created_at": _parse_datetime(row["created_at"]),
            }
            for row in rows
        ]
    def load_forward_test(self, experiment_id: str) -> dict[str, Any] | None:
        identifier = str(experiment_id).strip()
        if not identifier:
            raise ValueError("experiment_id is required")
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM forward_tests WHERE experiment_id=? LIMIT 1",
                (identifier,),
            ).fetchone()
        if row is None:
            return None
        return {
            "experiment_id": row["experiment_id"],
            "strategy_hash": row["strategy_hash"],
            "model_hash": row["model_hash"],
            "config": _load(row["config_json"]),
            "start_timestamp": _parse_datetime(row["start_timestamp"]),
            "bankroll": float(row["bankroll"]),
            "allowed_markets": _load(row["allowed_markets_json"]),
            "risk_limits": _load(row["risk_limits_json"]),
            "quality": row["quality"],
            "created_at": _parse_datetime(row["created_at"]),
        }

    def polymarket_evidence_maturity(self, *, now: datetime | None = None) -> dict[str, Any]:
        """Report research-evidence maturity separately from collector health."""
        current = ensure_utc(now or utc_now())
        cutoff = current.isoformat()
        with self._lock:
            market_row = self._conn.execute(
                "WITH market_ids AS ("
                "SELECT market_id FROM polymarket_markets "
                "WHERE rowid > COALESCE((SELECT MAX(rowid)-? FROM polymarket_markets),0) AND observed_at <= ? "
                "UNION SELECT market_id FROM polymarket_snapshots "
                "WHERE rowid > COALESCE((SELECT MAX(rowid)-? FROM polymarket_snapshots),0) AND observed_at <= ?"
                ") SELECT COUNT(*) AS market_count FROM market_ids",
                (_MAX_EVIDENCE_SCAN_ROWS, cutoff, _MAX_EVIDENCE_SCAN_ROWS, cutoff),
            ).fetchone()
            snapshot_row = self._conn.execute(
                "SELECT COUNT(*) AS snapshot_count, "
                "COUNT(DISTINCT market_id) AS snapshot_markets, "
                "SUM(CASE WHEN quality = 'ORDER_BOOK_SIMULATED' THEN 1 ELSE 0 END) AS book_snapshots, "
                "SUM(CASE WHEN json_valid(payload_json) THEN "
                "  CASE WHEN json_extract(payload_json, '$.metadata_available') = 1 THEN 1 ELSE 0 END "
                " ELSE 0 END) AS metadata_complete, "
                "SUM(CASE WHEN json_valid(payload_json) THEN "
                "  CASE WHEN json_extract(payload_json, '$.time_to_resolution_seconds') IS NOT NULL THEN 1 ELSE 0 END "
                " ELSE 0 END) AS time_to_resolution, "
                "COUNT(DISTINCT CASE WHEN json_valid(payload_json) THEN "
                "  CASE WHEN lower(CAST(json_extract(payload_json, '$.settlement') AS TEXT)) "
                "       IN ('resolved_yes', 'resolved_no', 'void') THEN market_id ELSE NULL END "
                " ELSE NULL END) AS resolved_markets, "
                "MIN(observed_at) AS first_observed_at, MAX(observed_at) AS latest_observed_at "
                "FROM polymarket_snapshots "
                "WHERE rowid > COALESCE((SELECT MAX(rowid)-? FROM polymarket_snapshots),0) AND observed_at <= ?",
                (_MAX_EVIDENCE_SCAN_ROWS, cutoff),
            ).fetchone()
            regime_row = self._conn.execute(
                "SELECT COUNT(*) AS regime_count FROM ("
                "SELECT DISTINCT CASE WHEN json_valid(payload_json) THEN "
                "  CASE "
                "    WHEN NULLIF(TRIM(CAST(json_extract(payload_json, '$.regime') AS TEXT)), '') IS NOT NULL "
                "      THEN TRIM(CAST(json_extract(payload_json, '$.regime') AS TEXT)) "
                "    WHEN NULLIF(TRIM(CAST(json_extract(payload_json, '$.snapshot.regime') AS TEXT)), '') IS NOT NULL "
                "      THEN TRIM(CAST(json_extract(payload_json, '$.snapshot.regime') AS TEXT)) "
                "    ELSE NULL "
                "  END "
                " ELSE NULL END AS regime "
                "FROM polymarket_snapshots "
                "WHERE rowid > COALESCE((SELECT MAX(rowid)-? FROM polymarket_snapshots),0) AND observed_at <= ?"
                ") WHERE regime IS NOT NULL",
                (_MAX_EVIDENCE_SCAN_ROWS, cutoff),
            ).fetchone()
            trade_row = self._conn.execute(
                "WITH market_ids AS ("
                "SELECT market_id FROM polymarket_markets "
                "WHERE rowid > COALESCE((SELECT MAX(rowid)-? FROM polymarket_markets),0) AND observed_at <= ? "
                "UNION SELECT market_id FROM polymarket_snapshots "
                "WHERE rowid > COALESCE((SELECT MAX(rowid)-? FROM polymarket_snapshots),0) AND observed_at <= ?"
                ") SELECT COUNT(DISTINCT trades.market_id) AS trade_markets "
                "FROM polymarket_trades AS trades "
                "JOIN market_ids ON market_ids.market_id = trades.market_id "
                "WHERE trades.rowid > COALESCE((SELECT MAX(rowid)-? FROM polymarket_trades),0) AND trades.timestamp <= ?",
                (_MAX_EVIDENCE_SCAN_ROWS, cutoff, _MAX_EVIDENCE_SCAN_ROWS, cutoff, _MAX_EVIDENCE_SCAN_ROWS, cutoff),
            ).fetchone()
        market_count = int((market_row["market_count"] if market_row else 0) or 0)
        snapshot_count = int((snapshot_row["snapshot_count"] if snapshot_row else 0) or 0)
        snapshot_markets = int((snapshot_row["snapshot_markets"] if snapshot_row else 0) or 0)
        book_snapshots = int((snapshot_row["book_snapshots"] if snapshot_row else 0) or 0)
        metadata_complete = int((snapshot_row["metadata_complete"] if snapshot_row else 0) or 0)
        time_to_resolution = int((snapshot_row["time_to_resolution"] if snapshot_row else 0) or 0)
        resolved_markets = int((snapshot_row["resolved_markets"] if snapshot_row else 0) or 0)
        trade_markets = int((trade_row["trade_markets"] if trade_row else 0) or 0)
        regime_count = int((regime_row["regime_count"] if regime_row else 0) or 0)
        first_observed = _parse_datetime(snapshot_row["first_observed_at"]) if snapshot_row else None
        latest_observed = _parse_datetime(snapshot_row["latest_observed_at"]) if snapshot_row else None
        duration_seconds = max(0.0, (latest_observed - first_observed).total_seconds()) if first_observed and latest_observed else 0.0
        requirements = {
            "independent_markets": 20,
            "snapshots": 100,
            "observation_days": 7.0,
            "resolved_markets": 10,
            "order_book_coverage": 0.8,
            "metadata_completeness": 0.9,
            "trade_coverage": 0.2,
            "regime_count": 3,
            "time_to_resolution_observations": 50,
        }
        checks = {
            "independent_markets": market_count >= requirements["independent_markets"],
            "snapshots": snapshot_count >= requirements["snapshots"],
            "observation_days": duration_seconds / 86400.0 >= requirements["observation_days"],
            "resolved_markets": resolved_markets >= requirements["resolved_markets"],
            "order_book_coverage": book_snapshots / snapshot_count >= requirements["order_book_coverage"] if snapshot_count else False,
            "metadata_completeness": metadata_complete / snapshot_count >= requirements["metadata_completeness"] if snapshot_count else False,
            "trade_coverage": trade_markets / market_count >= requirements["trade_coverage"] if market_count else False,
            "regime_count": regime_count >= requirements["regime_count"],
            "time_to_resolution_observations": time_to_resolution >= requirements["time_to_resolution_observations"],
        }
        passed = sum(bool(value) for value in checks.values())
        grade = "A" if passed == len(checks) else "B" if passed >= 6 else "C" if passed >= 4 else "D" if passed else "F"
        return {
            "grade": grade,
            "grade_scope": "research_evidence_maturity",
            "independent_markets": market_count,
            "markets_with_snapshots": snapshot_markets,
            "snapshots": snapshot_count,
            "resolved_markets": resolved_markets,
            "order_book_snapshots": book_snapshots,
            "order_book_coverage": book_snapshots / snapshot_count if snapshot_count else 0.0,
            "trade_markets": trade_markets,
            "trade_coverage": trade_markets / market_count if market_count else 0.0,
            "metadata_complete_snapshots": metadata_complete,
            "regime_count": regime_count,
            "time_to_resolution_observations": time_to_resolution,
            "observation_duration_seconds": duration_seconds,
            "latest_observed_at": latest_observed.isoformat() if latest_observed else None,
            "as_of": current.isoformat(),
            "scan_limits": {
                "market_rows": _MAX_EVIDENCE_SCAN_ROWS,
                "snapshot_rows": _MAX_EVIDENCE_SCAN_ROWS,
                "trade_rows": _MAX_EVIDENCE_SCAN_ROWS,
            },
            "requirements": requirements,
            "checks": checks,
        }

    def polymarket_health(
        self,
        *,
        expected_interval_seconds: float = 60.0,
        stale_after_seconds: float | None = None,
        now: datetime | None = None,
        recent_window_seconds: float | None = None,
        recent_cycles: int | None = None,
    ) -> dict[str, Any]:
        """Return current forward-collector health, not historical maturity.

        Historical rows remain available to :meth:`polymarket_evidence_maturity`,
        but cannot make the operational collector appear healthy.
        """
        expected = float(expected_interval_seconds)
        if not math.isfinite(expected) or expected <= 0 or expected > _MAX_OPERATIONAL_WINDOW_SECONDS:
            raise ValueError(
                f"expected_interval_seconds must be finite, positive, and <= {_MAX_OPERATIONAL_WINDOW_SECONDS:g}"
            )
        stale_after = float(stale_after_seconds if stale_after_seconds is not None else expected * 3.0)
        if not math.isfinite(stale_after) or stale_after <= 0 or stale_after > _MAX_OPERATIONAL_WINDOW_SECONDS:
            raise ValueError(
                f"stale_after_seconds must be finite, positive, and <= {_MAX_OPERATIONAL_WINDOW_SECONDS:g}"
            )
        window = float(recent_window_seconds if recent_window_seconds is not None else max(_DEFAULT_OPERATIONAL_WINDOW_SECONDS, stale_after * 2.0))
        if not math.isfinite(window) or window <= 0:
            raise ValueError("recent_window_seconds must be finite and positive")
        window = min(window, _MAX_OPERATIONAL_WINDOW_SECONDS)
        if recent_cycles is not None and (isinstance(recent_cycles, bool) or int(recent_cycles) <= 0):
            raise ValueError("recent_cycles must be a positive integer")
        current = ensure_utc(now or utc_now())
        window_start = current - timedelta(seconds=window)
        latest_window_seconds = max(window, stale_after)
        latest_window_start = current - timedelta(seconds=latest_window_seconds)
        current_iso, window_iso, latest_window_iso = current.isoformat(), window_start.isoformat(), latest_window_start.isoformat()
        with self._lock:
            latest_rows = self._conn.execute(
                "SELECT market_id,observed_at,payload_json FROM ("
                "SELECT market_id,observed_at,payload_json,"
                "ROW_NUMBER() OVER (PARTITION BY market_id ORDER BY observed_at DESC,source_timestamp DESC,snapshot_id DESC) AS row_number "
                "FROM polymarket_snapshots WHERE source_type='FORWARD_COLLECTED' AND observed_at>=? AND observed_at<=?) WHERE row_number=1",
                (latest_window_iso, current_iso),
            ).fetchall()
            recent_rows = self._conn.execute(
                "SELECT market_id,observed_at FROM polymarket_snapshots "
                "WHERE source_type='FORWARD_COLLECTED' AND observed_at>=? AND observed_at<=? "
                "ORDER BY market_id,observed_at,source_timestamp,snapshot_id",
                (window_iso, current_iso),
            ).fetchall()
            error_rows = self._conn.execute(
                "SELECT error_id,market_id,observed_at,kind,detail,payload_json FROM collection_errors "
                "WHERE source_type='FORWARD_COLLECTED' AND observed_at>=? AND observed_at<=? "
                "AND rowid > COALESCE((SELECT MAX(rowid)-? FROM collection_errors WHERE source_type='FORWARD_COLLECTED'),0) "
                "ORDER BY observed_at,error_id LIMIT 256",
                (window_iso, current_iso, _MAX_LATEST_SCAN_ROWS),
            ).fetchall()
            historical_error_count = int(self._conn.execute(
                "SELECT COUNT(*) AS n FROM collection_errors WHERE source_type='HISTORICAL' "
                "AND rowid > COALESCE((SELECT MAX(rowid)-? FROM collection_errors WHERE source_type='HISTORICAL'),0) AND observed_at<=?",
                (_MAX_EVIDENCE_SCAN_ROWS, current_iso),
            ).fetchone()["n"])
            trade_count = int(self._conn.execute(
                "SELECT COUNT(*) AS n FROM ("
                "SELECT trade_key FROM polymarket_trades "
                "WHERE timestamp>=? AND timestamp<=? "
                "ORDER BY timestamp,trade_key LIMIT ?"
                ")",
                (window_iso, current_iso, _MAX_LATEST_SCAN_ROWS),
            ).fetchone()["n"])
            metadata_count = int(self._conn.execute(
                "SELECT COUNT(*) AS n FROM polymarket_markets "
                "WHERE source_type='FORWARD_COLLECTED' AND observed_at>=? AND observed_at<=?",
                (latest_window_iso, current_iso),
            ).fetchone()["n"])
        tracked = self.tracked_polymarket_markets(active_only=True, now=current, include_payload=True, limit=1000)
        active_markets = {
            str(item.get("market_id"))
            for item in tracked
            if isinstance(item, Mapping) and item.get("market_id")
        }
        latest_by_market: dict[str, datetime] = {}
        latest_payload: dict[str, Mapping[str, Any]] = {}
        for row in latest_rows:
            stamp = _parse_datetime(row["observed_at"])
            if stamp is None:
                continue
            market = str(row["market_id"])
            latest_by_market[market] = stamp
            payload = _load(row["payload_json"])
            latest_payload[market] = payload if isinstance(payload, Mapping) else {}
            if market not in active_markets:
                snapshot = latest_payload[market].get("snapshot")
                settlement = str(snapshot.get("settlement", "")).lower() if isinstance(snapshot, Mapping) else ""
                if settlement not in {"resolved_yes", "resolved_no", "void"}:
                    active_markets.add(market)
        current_rows: dict[str, list[datetime]] = {}
        for row in recent_rows:
            stamp = _parse_datetime(row["observed_at"])
            market = str(row["market_id"])
            if stamp is not None:
                current_rows.setdefault(market, []).append(stamp)
                if market not in active_markets:
                    active_markets.add(market)
        gaps: list[dict[str, Any]] = []
        for market, stamps in current_rows.items():
            for previous, observed in zip(stamps, stamps[1:]):
                gap_seconds = (observed - previous).total_seconds()
                if gap_seconds > expected * 1.5:
                    gaps.append({
                        "market_id": market,
                        "from": previous.isoformat(),
                        "to": observed.isoformat(),
                        "seconds": gap_seconds,
                        "missing_intervals": max(1, int(round(gap_seconds / expected)) - 1),
                    })
        stale_markets = sorted(
            market for market in active_markets
            if market not in latest_by_market or (current - latest_by_market[market]).total_seconds() > stale_after
        )
        current_failures = [
            {
                "error_id": row["error_id"],
                "market_id": row["market_id"],
                "observed_at": _parse_datetime(row["observed_at"]).isoformat() if _parse_datetime(row["observed_at"]) else None,
                "kind": row["kind"],
                "detail": row["detail"],
                "reason_code": str(row["kind"]).upper(),
                "reason": str(row["detail"]),
            }
            for row in error_rows
        ]
        malformed_count = sum("MALFORM" in str(item["kind"]).upper() or "PARSE" in str(item["kind"]).upper() for item in current_failures)
        reasons: list[dict[str, str]] = []
        if not current_rows:
            reasons.append({"code": "NO_FORWARD_SNAPSHOTS", "reason": "No FORWARD_COLLECTED snapshot was observed in the current window."})
        if stale_markets:
            reasons.append({"code": "STALE_MARKETS", "reason": f"{len(stale_markets)} active/tracked market(s) have no successful sample within the staleness threshold."})
        if gaps:
            reasons.append({"code": "COLLECTION_GAPS", "reason": f"{len(gaps)} in-window collection gap(s) exceed the expected interval."})
        if malformed_count:
            reasons.append({"code": "MALFORMED_RECORDS", "reason": f"{malformed_count} malformed current collector record(s)."})
        if current_failures:
            reasons.append({"code": "CURRENT_COLLECTION_FAILURES", "reason": f"{len(current_failures)} current collector failure(s) are retained."})
        if not active_markets and not current_rows:
            grade = "F"
        elif malformed_count or (stale_markets and len(stale_markets) / max(1, len(active_markets)) > 0.5):
            grade = "D"
        elif stale_markets or gaps:
            grade = "C"
        elif current_failures:
            grade = "B"
        else:
            grade = "A"
        maturity = self.polymarket_evidence_maturity(now=current)
        collector = {
            "grade": grade,
            "grade_scope": "collector_health",
            "reason_code": reasons[0]["code"] if reasons else None,
            "reasons": reasons,
            "markets": len(active_markets),
            "markets_with_snapshots": len(latest_by_market),
            "metadata_records": metadata_count,
            "current_snapshots": sum(len(items) for items in current_rows.values()),
            "snapshots": sum(len(items) for items in current_rows.values()),
            "trades": trade_count,
            "collection_errors": len(current_failures),
            "current_failures": current_failures,
            "malformed_records": malformed_count,
            "stale_markets": stale_markets,
            "gaps": gaps,
            "gap_count": len(gaps),
            "latest_observed_at": max(latest_by_market.values(), default=None).isoformat() if latest_by_market else None,
            "window_start": window_start.isoformat(),
            "window_end": current.isoformat(),
            "window_seconds": window,
            "expected_interval_seconds": expected,
            "stale_after_seconds": stale_after,
        }
        return {
            "grade": grade,
            "grade_scope": "collector_health",
            "reason_code": collector["reason_code"],
            "reasons": reasons,
            "collector_health": collector,
            "markets": collector["markets"],
            "markets_with_snapshots": collector["markets_with_snapshots"],
            "metadata_records": collector["metadata_records"],
            "snapshots": collector["snapshots"],
            "trades": trade_count,
            "collection_errors": len(current_failures),
            "current_failures": current_failures,
            "stale_markets": stale_markets,
            "gaps": gaps,
            "gap_count": len(gaps),
            "window_start": collector["window_start"],
            "window_end": collector["window_end"],
            "window_seconds": window,
            "historical_error_count": historical_error_count,
            "historical_maturity_grade": maturity.get("grade"),
            "evidence_maturity": maturity,
            "storage_bytes": _storage_bytes(self._conn, self.path),
            "scan_limits": {"latest_window_seconds": latest_window_seconds, "recent_window_seconds": window, "recent_cycles": recent_cycles, "gap_sample": 64},
        }
    # Strategy and experiment artifacts -------------------------------
    def save_strategy(self, strategy_id: str, strategy: Any, *, version: str = "1") -> None:
        try:
            with self._write_context():
                self._conn.execute(
                    "INSERT INTO strategies(strategy_id,version,payload_json,created_at) VALUES (?,?,?,?)",
                    (str(strategy_id), str(version), _dump(strategy), _now_iso()),
                )
        except sqlite3.IntegrityError as exc:
            raise ValueError(f"strategy version already exists: {strategy_id}/{version}") from exc
    def save_strategy_if_absent(self, strategy_id: str, strategy: Any, *, version: str = "1") -> bool:
        """Persist a deterministic strategy exactly once."""
        identifier, version_value = str(strategy_id), str(version)
        payload = _dump(strategy)
        with self._write_context():
            cursor = self._conn.execute(
                "INSERT OR IGNORE INTO strategies(strategy_id,version,payload_json,created_at) VALUES (?,?,?,?)",
                (identifier, version_value, payload, _now_iso()),
            )
        if cursor.rowcount:
            return True
        with self._lock:
            row = self._conn.execute(
                "SELECT payload_json FROM strategies WHERE strategy_id=? AND version=?",
                (identifier, version_value),
            ).fetchone()
        if row is None or row["payload_json"] != payload:
            raise ValueError(f"strategy version already exists with different payload: {identifier}/{version_value}")
        return False


    def load_strategy(self, strategy_id: str, version: str | None = None) -> Any | None:
        with self._lock:
            if version is None:
                row = self._conn.execute(
                    "SELECT payload_json FROM strategies WHERE strategy_id=? ORDER BY created_at DESC,version DESC LIMIT 1",
                    (str(strategy_id),),
                ).fetchone()
            else:
                row = self._conn.execute(
                    "SELECT payload_json FROM strategies WHERE strategy_id=? AND version=?",
                    (str(strategy_id), str(version)),
                ).fetchone()
        return _load(row["payload_json"]) if row else None

    def list_strategies(self, strategy_id: str | None = None) -> list[dict[str, Any]]:
        query = "SELECT strategy_id,version,payload_json,created_at FROM strategies"
        values: tuple[Any, ...] = ()
        if strategy_id is not None:
            query += " WHERE strategy_id=?"
            values = (str(strategy_id),)
        query += " ORDER BY created_at,strategy_id,version"
        with self._lock:
            rows = self._conn.execute(query, values).fetchall()
        return [
            {"strategy_id": row["strategy_id"], "version": row["version"], "strategy": _load(row["payload_json"]), "created_at": _parse_datetime(row["created_at"])}
            for row in rows
        ]

    def save_experiment(self, experiment_id: str, experiment: Any, *, strategy_id: str | None = None) -> None:
        try:
            with self._write_context():
                self._conn.execute(
                    "INSERT INTO experiments(experiment_id,strategy_id,payload_json,created_at) VALUES (?,?,?,?)",
                    (str(experiment_id), strategy_id, _dump(experiment), _now_iso()),
                )
        except sqlite3.IntegrityError as exc:
            raise ValueError(f"experiment already exists: {experiment_id}") from exc
    def save_experiment_if_absent(
        self,
        experiment_id: str,
        experiment: Any,
        *,
        strategy_id: str | None = None,
    ) -> bool:
        """Persist a deterministic experiment exactly once."""
        identifier = str(experiment_id)
        payload = _dump(experiment)
        with self._write_context():
            cursor = self._conn.execute(
                "INSERT OR IGNORE INTO experiments(experiment_id,strategy_id,payload_json,created_at) VALUES (?,?,?,?)",
                (identifier, strategy_id, payload, _now_iso()),
            )
        if cursor.rowcount:
            return True
        with self._lock:
            row = self._conn.execute(
                "SELECT strategy_id,payload_json FROM experiments WHERE experiment_id=?",
                (identifier,),
            ).fetchone()
        if row is None or row["payload_json"] != payload or row["strategy_id"] != strategy_id:
            raise ValueError(f"experiment already exists with different payload: {identifier}")
        return False


    def load_experiment(self, experiment_id: str) -> Any | None:
        with self._lock:
            row = self._conn.execute("SELECT payload_json FROM experiments WHERE experiment_id=?", (str(experiment_id),)).fetchone()
        return _load(row["payload_json"]) if row else None

    def list_experiments(self, strategy_id: str | None = None, *, limit: int | None = None) -> list[dict[str, Any]]:
        if limit is not None and (isinstance(limit, bool) or not isinstance(limit, int) or limit < 0):
            raise ValueError("limit must be a non-negative integer or None")
        query = "SELECT experiment_id,strategy_id,payload_json,created_at FROM experiments"
        values: list[Any] = []
        if strategy_id is not None:
            query += " WHERE strategy_id=?"
            values.append(str(strategy_id))
        query += " ORDER BY created_at,experiment_id"
        if limit is not None:
            query += " LIMIT ?"
            values.append(int(limit))
        with self._lock:
            rows = self._conn.execute(query, values).fetchall()
        return [
            {"experiment_id": row["experiment_id"], "strategy_id": row["strategy_id"], "experiment": _load(row["payload_json"]), "created_at": _parse_datetime(row["created_at"])}
            for row in rows
        ]

    def save_experiment_plan(
        self,
        plan_id: str,
        plan: Any,
        *,
        hypothesis_id: str,
        status: str = "PENDING",
        result: Any | None = None,
        plan_hash: str | None = None,
        timestamp: datetime | None = None,
    ) -> bool:
        """Persist an immutable plan while allowing monotonic status updates."""
        identifier = str(plan_id).strip()
        hypothesis = str(hypothesis_id).strip()
        if not identifier or not hypothesis:
            raise ValueError("plan_id and hypothesis_id are required")
        payload_json = _dump(plan)
        resolved_hash = str(plan_hash or ("sha256:" + hashlib.sha256(payload_json.encode("utf-8")).hexdigest()))
        result_json = _dump(result) if result is not None else None
        stamp = _iso(timestamp or utc_now())
        with self._write_context():
            existing = self._conn.execute(
                "SELECT hypothesis_id,plan_hash,payload_json,status,result_json FROM experiment_plans WHERE plan_id=?",
                (identifier,),
            ).fetchone()
            if existing is not None:
                if (
                    str(existing["hypothesis_id"]) != hypothesis
                    or str(existing["plan_hash"]) != resolved_hash
                    or str(existing["payload_json"]) != payload_json
                ):
                    raise ValueError(f"experiment plan already exists with different payload: {identifier}")
                old_status = str(existing["status"])
                terminal = {"COMPLETED", "REJECTED", "FAILED"}
                next_status = old_status if old_status in terminal and status not in terminal else str(status)
                next_result = result_json if result_json is not None else existing["result_json"]
                self._conn.execute(
                    "UPDATE experiment_plans SET status=?,result_json=?,updated_at=? WHERE plan_id=?",
                    (next_status, next_result, stamp, identifier),
                )
                return False
            self._conn.execute(
                "INSERT INTO experiment_plans(plan_id,hypothesis_id,plan_hash,payload_json,status,result_json,created_at,updated_at) "
                "VALUES (?,?,?,?,?,?,?,?)",
                (identifier, hypothesis, resolved_hash, payload_json, str(status), result_json, stamp, stamp),
            )
        return True

    def load_experiment_plan(self, plan_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute("SELECT * FROM experiment_plans WHERE plan_id=?", (str(plan_id),)).fetchone()
        if row is None:
            return None
        return {
            "plan_id": row["plan_id"],
            "hypothesis_id": row["hypothesis_id"],
            "plan_hash": row["plan_hash"],
            "plan": _load(row["payload_json"]),
            "status": row["status"],
            "result": _load(row["result_json"]) if row["result_json"] else None,
            "created_at": _parse_datetime(row["created_at"]),
            "updated_at": _parse_datetime(row["updated_at"]),
        }

    def list_experiment_plans(
        self,
        *,
        hypothesis_id: str | None = None,
        status: str | None = None,
        limit: int | None = None,
        newest_first: bool = True,
    ) -> list[dict[str, Any]]:
        if limit is not None and (isinstance(limit, bool) or not isinstance(limit, int) or limit < 0):
            raise ValueError("limit must be a non-negative integer or None")
        query = "SELECT * FROM experiment_plans"
        values: list[Any] = []
        clauses: list[str] = []
        if hypothesis_id is not None:
            clauses.append("hypothesis_id=?")
            values.append(str(hypothesis_id))
        if status is not None:
            clauses.append("status=?")
            values.append(str(status))
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        direction = "DESC" if newest_first else "ASC"
        query += f" ORDER BY updated_at {direction},plan_id {direction}"
        if limit is not None:
            query += " LIMIT ?"
            values.append(int(limit))
        with self._lock:
            rows = self._conn.execute(query, values).fetchall()
        return [
            {
                "plan_id": row["plan_id"],
                "hypothesis_id": row["hypothesis_id"],
                "plan_hash": row["plan_hash"],
                "plan": _load(row["payload_json"]),
                "status": row["status"],
                "result": _load(row["result_json"]) if row["result_json"] else None,
                "created_at": _parse_datetime(row["created_at"]),
                "updated_at": _parse_datetime(row["updated_at"]),
            }
            for row in rows
        ]

    def save_fill(self, fill: Fill, *, fill_id: str | None = None) -> str:
        if not isinstance(fill, Fill):
            raise TypeError("save_fill expects a Fill")
        identifier = str(fill_id or ("fill-" + hashlib.sha256(_dump(fill).encode("utf-8")).hexdigest()))
        try:
            with self._write_context():
                self._conn.execute(
                    "INSERT INTO fills(fill_id,order_id,timestamp,strategy_id,symbol,payload_json,created_at) VALUES (?,?,?,?,?,?,?)",
                    (identifier, fill.order_id, _iso(fill.timestamp), fill.strategy_id, fill.symbol, _dump(fill), _now_iso()),
                )
        except sqlite3.IntegrityError as exc:
            raise ValueError(f"fill already exists: {identifier}") from exc
        return identifier
    def save_paper_execution(
        self,
        observation_id: str,
        experiment_id: str,
        market_id: str,
        timestamp: datetime,
        payload: Any,
        fill: Fill,
        *,
        fill_id: str | None = None,
    ) -> bool:
        """Atomically persist one paper fill and its observation claim."""
        identifier = str(fill_id or ("fill-" + hashlib.sha256(_dump(fill).encode("utf-8")).hexdigest()))
        with self.transaction():
            if not self.save_paper_observation(observation_id, experiment_id, market_id, timestamp, payload):
                existing_fill = self.load_fill(identifier)
                if existing_fill is not None and existing_fill != fill:
                    raise ValueError(f"fill already exists with different payload: {identifier}")
                return False
            existing_fill = self.load_fill(identifier)
            if existing_fill is None:
                self.save_fill(fill, fill_id=identifier)
            elif existing_fill != fill:
                raise ValueError(f"fill already exists with different payload: {identifier}")
        return True
    def paper_history_counts(self, experiment_id: str) -> dict[str, int]:
        identifier = str(experiment_id).strip()
        if not identifier:
            raise ValueError("experiment_id is required")
        with self._lock:
            observations = self._conn.execute(
                "SELECT COUNT(*) AS n FROM paper_observations WHERE experiment_id=?",
                (identifier,),
            ).fetchone()
            fills = self._conn.execute(
                "SELECT COUNT(*) AS n FROM fills WHERE fill_id LIKE ? OR fill_id LIKE ?",
                (identifier + "-%", "paper-fill-" + identifier + "-%"),
            ).fetchone()
        return {
            "observations": int(observations["n"]),
            "fills": int(fills["n"]),
        }


    def load_fill(self, fill_id: str) -> Fill | None:
        with self._lock:
            row = self._conn.execute("SELECT payload_json FROM fills WHERE fill_id=? OR order_id=?", (str(fill_id), str(fill_id))).fetchone()
        return _fill_from_record(_load(row["payload_json"])) if row else None

    def load_fills(
        self,
        *,
        strategy_id: str | None = None,
        symbol: str | None = None,
        order_id: str | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> list[Fill]:
        clauses: list[str] = []
        values: list[Any] = []
        for column, value in (("strategy_id", strategy_id), ("symbol", symbol), ("order_id", order_id)):
            if value is not None:
                clauses.append(column + "=?")
                values.append(str(value))
        if start is not None:
            clauses.append("timestamp>=?")
            values.append(_iso(start))
        if end is not None:
            clauses.append("timestamp<=?")
            values.append(_iso(end))
        query = "SELECT payload_json FROM fills"
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY timestamp"
        with self._lock:
            rows = self._conn.execute(query, values).fetchall()
        return [_fill_from_record(_load(row["payload_json"])) for row in rows]

    def save_report(self, report_id: str, report: Any, *, experiment_id: str | None = None) -> None:
        try:
            with self._write_context():
                self._conn.execute(
                    "INSERT INTO reports(report_id,experiment_id,payload_json,created_at) VALUES (?,?,?,?)",
                    (str(report_id), experiment_id, _dump(report), _now_iso()),
                )
        except sqlite3.IntegrityError as exc:
            raise ValueError(f"report already exists: {report_id}") from exc

    def load_report(self, report_id: str) -> Any | None:
        with self._lock:
            row = self._conn.execute("SELECT payload_json FROM reports WHERE report_id=?", (str(report_id),)).fetchone()
        return _load(row["payload_json"]) if row else None

    def list_reports(
        self,
        experiment_id: str | None = None,
        *,
        limit: int | None = None,
        newest_first: bool = False,
    ) -> list[dict[str, Any]]:
        if limit is not None and (isinstance(limit, bool) or not isinstance(limit, int) or limit < 0):
            raise ValueError("limit must be a non-negative integer or None")
        query = "SELECT report_id,experiment_id,payload_json,created_at FROM reports"
        values: list[Any] = []
        if experiment_id is not None:
            query += " WHERE experiment_id=?"
            values.append(str(experiment_id))
        query += " ORDER BY created_at " + ("DESC" if newest_first else "ASC") + ",report_id " + ("DESC" if newest_first else "ASC")
        if limit is not None:
            query += " LIMIT ?"
            values.append(int(limit))
        with self._lock:
            rows = self._conn.execute(query, values).fetchall()
        return [
            {"report_id": row["report_id"], "experiment_id": row["experiment_id"], "report": _load(row["payload_json"]), "created_at": _parse_datetime(row["created_at"])}
            for row in rows
        ]

    # Durable operations ------------------------------------------------
    def save_report_if_absent(self, report_id: str, report: Any, *, experiment_id: str | None = None) -> bool:
        """Insert a report atomically and return whether this call inserted it."""
        identifier = str(report_id)
        payload = _dump(report)
        with self._write_context():
            cursor = self._conn.execute(
                "INSERT OR IGNORE INTO reports(report_id,experiment_id,payload_json,created_at) VALUES (?,?,?,?)",
                (identifier, experiment_id, payload, _now_iso()),
            )
        if cursor.rowcount:
            return True
        with self._lock:
            row = self._conn.execute("SELECT payload_json FROM reports WHERE report_id=?", (identifier,)).fetchone()
        equivalent = False
        if row is not None and row["payload_json"] != payload:
            equivalent = _report_payload_equivalent(_load(row["payload_json"]), report)
        if row is None or (row["payload_json"] != payload and not equivalent):
            raise ValueError(f"report already exists with different payload: {identifier}")
        return False

    def save_collection_cycle(
        self,
        cycle_id: str,
        collector_name: str,
        payload: Any,
        *,
        started_at: datetime,
        ended_at: datetime | None = None,
    ) -> bool:
        started = _iso(started_at)
        ended = _iso(ended_at or started_at)
        payload_json = _dump(payload)
        identifier = str(cycle_id)
        with self._write_context():
            cursor = self._conn.execute(
                "INSERT OR IGNORE INTO collection_cycles(cycle_id,collector_name,started_at,ended_at,payload_json,created_at) "
                "VALUES (?,?,?,?,?,?)",
                (identifier, str(collector_name), started, ended, payload_json, _now_iso()),
            )
            if cursor.rowcount:
                return True
            existing = self._conn.execute(
                "SELECT collector_name,started_at,ended_at,payload_json FROM collection_cycles WHERE cycle_id=?",
                (identifier,),
            ).fetchone()
        if existing is None:
            raise RuntimeError(f"collection cycle disappeared during duplicate check: {identifier}")
        if (
            str(existing["collector_name"]) != str(collector_name)
            or str(existing["started_at"]) != started
            or str(existing["ended_at"]) != ended
            or str(existing["payload_json"]) != payload_json
        ):
            raise ValueError(f"collection cycle already exists with different payload: {identifier}")
        return False

    def list_collection_cycles(self, *, collector_name: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 0:
            raise ValueError("limit must be a non-negative integer")
        query = "SELECT * FROM collection_cycles"
        values: list[Any] = []
        if collector_name is not None:
            query += " WHERE collector_name=?"
            values.append(str(collector_name))
        query += " ORDER BY started_at DESC,cycle_id DESC LIMIT ?"
        values.append(int(limit))
        with self._lock:
            rows = self._conn.execute(query, values).fetchall()
        return [
            {
                "cycle_id": row["cycle_id"],
                "collector_name": row["collector_name"],
                "started_at": _parse_datetime(row["started_at"]),
                "ended_at": _parse_datetime(row["ended_at"]),
                "payload": _load(row["payload_json"]),
                "created_at": _parse_datetime(row["created_at"]),
            }
            for row in rows
        ]

    def enqueue_research_item(
        self,
        item_type: str,
        payload: Any,
        *,
        dedupe_key: str | None = None,
        source: str = "",
        author: str = "",
        lineage: Iterable[Any] = (),
        schema_version: str = "1",
        priority: int = 0,
        available_at: datetime | None = None,
        item_id: str | None = None,
    ) -> dict[str, Any]:
        item_type = str(item_type).strip()
        if not item_type:
            raise ValueError("item_type is required")
        if isinstance(priority, bool) or not isinstance(priority, int):
            raise ValueError("priority must be an integer")
        schema_version = str(schema_version).strip()
        if not schema_version:
            raise ValueError("schema_version is required")
        lineage_value = list(islice(iter(lineage), _QUEUE_LINEAGE_LIMIT + 1))
        if len(lineage_value) > _QUEUE_LINEAGE_LIMIT:
            raise ValueError(f"research lineage exceeds {_QUEUE_LINEAGE_LIMIT} entries")
        payload_json = _dump(payload)
        if dedupe_key is None:
            dedupe_key = hashlib.sha256(
                _dump({"item_type": item_type, "payload": payload, "source": source, "lineage": lineage_value}).encode("utf-8")
            ).hexdigest()
        dedupe_key = str(dedupe_key).strip()
        if not dedupe_key:
            raise ValueError("dedupe_key is required")
        identifier = str(item_id or ("queue-" + hashlib.sha256(dedupe_key.encode("utf-8")).hexdigest())).strip()
        if not identifier:
            raise ValueError("item_id is required")
        now = _now_iso()
        available = _iso(available_at or utc_now())
        with self._write_context():
            self._conn.execute(
                "INSERT OR IGNORE INTO research_queue(item_id,item_type,dedupe_key,status,priority,payload_json,source,author,"
                "lineage_json,schema_version,created_at,updated_at,available_at,lease_until,attempts,last_error) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    identifier,
                    item_type,
                    dedupe_key,
                    "PENDING",
                    int(priority),
                    payload_json,
                    str(source),
                    str(author),
                    _dump(lineage_value),
                    str(schema_version),
                    now,
                    now,
                    available,
                    None,
                    0,
                    None,
                ),
            )
            row = self._conn.execute("SELECT * FROM research_queue WHERE dedupe_key=?", (dedupe_key,)).fetchone()
            if row is not None:
                if (
                    str(row["item_type"]) != item_type
                    or str(row["payload_json"]) != payload_json
                    or str(row["source"]) != str(source)
                    or str(row["author"]) != str(author)
                    or str(row["lineage_json"]) != _dump(lineage_value)
                ):
                    raise ValueError(f"research queue dedupe conflict: {dedupe_key}")
        if row is None:
            raise RuntimeError("research queue insert did not produce a row")
        return _research_queue_record(row)

    def get_research_item(self, item_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute("SELECT * FROM research_queue WHERE item_id=?", (str(item_id),)).fetchone()
        return _research_queue_record(row) if row else None

    def claim_research_item(
        self,
        worker: str,
        *,
        now: datetime | None = None,
        lease_seconds: float = 300.0,
    ) -> dict[str, Any] | None:
        if not str(worker).strip():
            raise ValueError("worker is required")
        lease_value = float(lease_seconds)
        if not math.isfinite(lease_value) or lease_value <= 0:
            raise ValueError("lease_seconds must be finite and positive")
        current = ensure_utc(now or utc_now())
        current_iso = current.isoformat()
        lease_iso = current.timestamp() + lease_value
        lease_time = datetime.fromtimestamp(lease_iso, tz=timezone.utc).isoformat()
        self.release_expired_research_items(now=current)
        with self._write_context():
            row = self._conn.execute(
                "SELECT * FROM research_queue WHERE status='PENDING' AND available_at<=? "
                "ORDER BY CASE WHEN (julianday(?) - julianday(created_at))*86400.0 >= 300 THEN 1 ELSE 0 END DESC,"
                "priority DESC,created_at,item_id LIMIT 1",
                (current_iso, current_iso),
            ).fetchone()
            if row is None:
                return None
            previous = str(row["status"])
            updated = self._conn.execute(
                "UPDATE research_queue SET status='TESTING',lease_until=?,lease_owner=?,attempts=attempts+1,updated_at=? "
                "WHERE item_id=? AND status='PENDING'",
                (lease_time, str(worker), current_iso, row["item_id"]),
            )
            if updated.rowcount != 1:
                return None
            event_id = "queue-event-" + hashlib.sha256(
                _dump({"item_id": row["item_id"], "from": previous, "to": "TESTING", "at": current_iso, "worker": worker}).encode("utf-8")
            ).hexdigest()
            self._conn.execute(
                "INSERT OR IGNORE INTO research_queue_events(event_id,item_id,from_status,to_status,detail,created_at) "
                "VALUES (?,?,?,?,?,?)",
                (event_id, row["item_id"], previous, "TESTING", _dump({"worker": str(worker)}), current_iso),
            )
            claimed = self._conn.execute("SELECT * FROM research_queue WHERE item_id=?", (row["item_id"],)).fetchone()
        return _research_queue_record(claimed) if claimed else None
    def record_research_queue_event(
        self,
        item_id: str,
        to_status: str,
        detail: Any | None = None,
        *,
        timestamp: datetime | None = None,
    ) -> None:
        """Append an auditable processing stage without changing lease state."""
        current = ensure_utc(timestamp or utc_now())
        stamp = current.isoformat()
        with self._write_context():
            row = self._conn.execute(
                "SELECT status FROM research_queue WHERE item_id=?",
                (str(item_id),),
            ).fetchone()
            if row is None:
                raise KeyError(item_id)
            event_id = "queue-event-" + hashlib.sha256(
                _dump({"item_id": str(item_id), "to": str(to_status), "detail": detail, "at": stamp}).encode("utf-8")
            ).hexdigest()
            self._conn.execute(
                "INSERT OR IGNORE INTO research_queue_events(item_id,event_id,from_status,to_status,detail,created_at) "
                "VALUES (?,?,?,?,?,?)",
                (str(item_id), event_id, str(row["status"]), str(to_status), _dump(detail or {}), stamp),
            )

    def list_research_queue_events(self, item_id: str, *, limit: int = 256) -> list[dict[str, Any]]:
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 0:
            raise ValueError("limit must be a non-negative integer")
        with self._lock:
            rows = self._conn.execute(
                "SELECT event_id,item_id,from_status,to_status,detail,created_at "
                "FROM research_queue_events WHERE item_id=? ORDER BY created_at,event_id LIMIT ?",
                (str(item_id), int(limit)),
            ).fetchall()
        return [
            {
                "event_id": row["event_id"],
                "item_id": row["item_id"],
                "from_status": row["from_status"],
                "to_status": row["to_status"],
                "detail": _load(row["detail"]) if row["detail"] else {},
                "created_at": _parse_datetime(row["created_at"]),
            }
            for row in rows
        ]


    def complete_research_item(
        self,
        item_id: str,
        status: str,
        *,
        result: Any | None = None,
        error: str | None = None,
        now: datetime | None = None,
        worker: str | None = None,
    ) -> dict[str, Any]:
        normalized = str(status).upper()
        allowed = {"ACCEPTED", "REJECTED", "COMPLETED", "FAILED", "PENDING"}
        if normalized not in allowed:
            raise ValueError(f"unsupported research queue status: {status}")
        current = ensure_utc(now or utc_now())
        current_iso = current.isoformat()
        with self._write_context():
            row = self._conn.execute("SELECT * FROM research_queue WHERE item_id=?", (str(item_id),)).fetchone()
            if row is None:
                raise KeyError(item_id)
            previous = str(row["status"])
            if previous != "TESTING":
                raise RuntimeError(f"queue item is not leased: {item_id}")
            owner = str(row["lease_owner"] or "")
            if not worker or str(worker) != owner:
                raise PermissionError("queue lease owner mismatch")
            lease_until = _parse_datetime(row["lease_until"])
            if lease_until is None or lease_until <= current:
                raise RuntimeError("queue lease expired")
            result_json = row["result_json"] if result is None else _dump(result)
            updated_cursor = self._conn.execute(
                "UPDATE research_queue SET status=?,updated_at=?,lease_until=NULL,lease_owner=NULL,last_error=?,result_json=? "
                "WHERE item_id=? AND status='TESTING' AND lease_owner=? AND lease_until>?",
                (normalized, current_iso, error, result_json, str(item_id), owner, current_iso),
            )
            if updated_cursor.rowcount != 1:
                raise RuntimeError("queue lease lost before completion")
            event_id = "queue-event-" + hashlib.sha256(
                _dump({"item_id": item_id, "from": previous, "to": normalized, "at": current_iso, "worker": worker}).encode("utf-8")
            ).hexdigest()
            self._conn.execute(
                "INSERT OR IGNORE INTO research_queue_events(event_id,item_id,from_status,to_status,detail,created_at) VALUES (?,?,?,?,?,?)",
                (event_id, str(item_id), previous, normalized, _dump({"error": error, "worker": worker}), current_iso),
            )
            updated = self._conn.execute("SELECT * FROM research_queue WHERE item_id=?", (str(item_id),)).fetchone()
        return _research_queue_record(updated)

    def release_expired_research_items(self, *, now: datetime | None = None) -> int:
        current = ensure_utc(now or utc_now())
        current_iso = current.isoformat()
        released = 0
        with self._write_context():
            while True:
                rows = self._conn.execute(
                    "SELECT item_id FROM research_queue WHERE status='TESTING' "
                    "AND lease_until IS NOT NULL AND lease_until<=? LIMIT ?",
                    (current_iso, _QUEUE_RELEASE_BATCH),
                ).fetchall()
                if not rows:
                    break
                for row in rows:
                    updated = self._conn.execute(
                        "UPDATE research_queue SET status='PENDING',lease_until=NULL,lease_owner=NULL,updated_at=? "
                        "WHERE item_id=? AND status='TESTING' AND lease_until IS NOT NULL AND lease_until<=?",
                        (current_iso, row["item_id"], current_iso),
                    )
                    if updated.rowcount != 1:
                        continue
                    released += 1
                    event_id = "queue-event-" + hashlib.sha256(
                        _dump({"item_id": row["item_id"], "from": "TESTING", "to": "PENDING", "at": current_iso, "reason": "lease_expired"}).encode("utf-8")
                    ).hexdigest()
                    self._conn.execute(
                        "INSERT OR IGNORE INTO research_queue_events(event_id,item_id,from_status,to_status,detail,created_at) VALUES (?,?,?,?,?,?)",
                        (event_id, row["item_id"], "TESTING", "PENDING", _dump({"reason": "lease_expired"}), current_iso),
                    )
        return released
    def list_research_items(self, *, status: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 0:
            raise ValueError("limit must be a non-negative integer")
        query = "SELECT * FROM research_queue"
        values: list[Any] = []
        if status is not None:
            query += " WHERE status=?"
            values.append(str(status).upper())
        query += " ORDER BY priority DESC,created_at,item_id LIMIT ?"
        values.append(int(limit))
        with self._lock:
            rows = self._conn.execute(query, values).fetchall()
        return [_research_queue_record(row) for row in rows]

    def research_queue_stats(self) -> dict[str, int]:
        with self._lock:
            rows = self._conn.execute("SELECT status,COUNT(*) AS n FROM research_queue GROUP BY status").fetchall()
        result = {str(row["status"]): int(row["n"]) for row in rows}
        result["total"] = sum(result.values())
        return result

    def save_candidate_lifecycle(
        self,
        candidate_id: str,
        stage: str,
        payload: Any,
        *,
        from_stage: str | None = None,
        reason: str = "",
        timestamp: datetime | None = None,
    ) -> bool:
        current_iso = _iso(timestamp or utc_now())
        identifier = str(candidate_id).strip()
        stage = str(stage).strip()
        if not identifier:
            raise ValueError("candidate_id is required")
        if stage not in {
            "IDEA",
            "SCHEMA_VALIDATED",
            "BACKTESTED",
            "VALIDATED",
            "ROBUSTNESS_CHECKED",
            "FROZEN",
            "PAPER_FORWARD",
            "PAPER_PROMOTABLE",
            "REJECTED",
        }:
            raise ValueError(f"unsupported candidate stage: {stage}")
        with self._write_context():
            existing = self._conn.execute(
                "SELECT stage,payload_json FROM candidate_lifecycle WHERE candidate_id=?", (identifier,)
            ).fetchone()
            if existing is None and stage != "IDEA":
                raise ValueError("new candidates must start at IDEA")
            payload_json = _dump(payload)
            current_stage: str | None = None
            if existing is not None and str(existing["stage"]) == str(stage) and existing["payload_json"] == payload_json:
                return False
            if existing is not None:
                stage_order = {
                    "IDEA": 0,
                    "SCHEMA_VALIDATED": 1,
                    "BACKTESTED": 2,
                    "VALIDATED": 3,
                    "ROBUSTNESS_CHECKED": 4,
                    "FROZEN": 5,
                    "PAPER_FORWARD": 6,
                    "PAPER_PROMOTABLE": 7,
                    "REJECTED": 8,
                }
                current_stage = str(existing["stage"])
                if current_stage == "REJECTED":
                    raise ValueError(f"rejected candidate is terminal: {identifier}")
                if from_stage is not None and current_stage != str(from_stage):
                    raise ValueError(f"stale candidate lifecycle writer: expected {from_stage}, found {current_stage}")
                if str(stage) != "REJECTED" and stage_order.get(str(stage), -1) < stage_order.get(current_stage, -1):
                    raise ValueError(f"candidate lifecycle cannot regress: {current_stage} -> {stage}")
                updated_cursor = self._conn.execute(
                    "UPDATE candidate_lifecycle SET stage=?,payload_json=?,updated_at=? "
                    "WHERE candidate_id=? AND stage=? AND payload_json=?",
                    (str(stage), payload_json, current_iso, identifier, current_stage, existing["payload_json"]),
                )
                if updated_cursor.rowcount != 1:
                    raise RuntimeError("stale candidate lifecycle writer")
            else:
                inserted_cursor = self._conn.execute(
                    "INSERT OR IGNORE INTO candidate_lifecycle(candidate_id,stage,payload_json,updated_at) VALUES (?,?,?,?)",
                    (identifier, str(stage), payload_json, current_iso),
                )
                if inserted_cursor.rowcount != 1:
                    raise RuntimeError("candidate lifecycle was created concurrently")
            event_from = from_stage if from_stage is not None else current_stage
            event_id = "lifecycle-event-" + hashlib.sha256(
                _dump(
                    {
                        "candidate_id": identifier,
                        "from": event_from,
                        "to": stage,
                        "reason": reason,
                        "payload": payload,
                        "at": current_iso,
                    }
                ).encode("utf-8")
            ).hexdigest()
            self._conn.execute(
                "INSERT OR IGNORE INTO candidate_lifecycle_events(event_id,candidate_id,from_stage,to_stage,reason,payload_json,created_at) "
                "VALUES (?,?,?,?,?,?,?)",
                (event_id, identifier, event_from, str(stage), str(reason), payload_json, current_iso),
            )
        return True

    def load_candidate_lifecycle(
        self,
        candidate_id: str | None = None,
        *,
        limit: int | None = None,
    ) -> list[dict[str, Any]] | dict[str, Any] | None:
        if limit is not None and (isinstance(limit, bool) or not isinstance(limit, int) or limit < 0):
            raise ValueError("limit must be a non-negative integer or None")
        query = "SELECT * FROM candidate_lifecycle"
        values: list[Any] = []
        if candidate_id is not None:
            query += " WHERE candidate_id=?"
            values.append(str(candidate_id))
        query += " ORDER BY updated_at,candidate_id"
        if candidate_id is None and limit is not None:
            query += " LIMIT ?"
            values.append(int(limit))
        with self._lock:
            rows = self._conn.execute(query, values).fetchall()
        records = [
            {
                "candidate_id": row["candidate_id"],
                "stage": row["stage"],
                "payload": _load(row["payload_json"]),
                "updated_at": _parse_datetime(row["updated_at"]),
            }
            for row in rows
        ]
        if candidate_id is not None:
            return records[0] if records else None
        return records

    def list_candidate_lifecycle_events(self, candidate_id: str | None = None, *, limit: int = 100) -> list[dict[str, Any]]:
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 0:
            raise ValueError("limit must be non-negative")
        query = "SELECT * FROM candidate_lifecycle_events"
        values: list[Any] = []
        if candidate_id is not None:
            query += " WHERE candidate_id=?"
            values.append(str(candidate_id))
        query += " ORDER BY created_at,event_id LIMIT ?"
        values.append(int(limit))
        with self._lock:
            rows = self._conn.execute(query, values).fetchall()
        return [
            {
                "event_id": row["event_id"],
                "candidate_id": row["candidate_id"],
                "from_stage": row["from_stage"],
                "to_stage": row["to_stage"],
                "reason": row["reason"],
                "payload": _load(row["payload_json"]),
                "created_at": _parse_datetime(row["created_at"]),
            }
            for row in rows
        ]
    def candidate_lifecycle_funnel(self) -> dict[str, int]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT stage,COUNT(*) AS count FROM candidate_lifecycle GROUP BY stage ORDER BY stage"
            ).fetchall()
        return {str(row["stage"]): int(row["count"]) for row in rows}

    def candidate_rejection_reasons(self, *, limit: int = 100) -> dict[str, int]:
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 0:
            raise ValueError("limit must be a non-negative integer")
        with self._lock:
            rows = self._conn.execute(
                "SELECT json_extract(payload_json,'$.rejection_reason') AS reason,COUNT(*) AS count "
                "FROM candidate_lifecycle WHERE stage='REJECTED' GROUP BY reason ORDER BY count DESC,reason LIMIT ?",
                (int(limit),),
            ).fetchall()
        return {str(row["reason"] or "unknown"): int(row["count"]) for row in rows}


    def save_paper_state(
        self,
        experiment_id: str,
        state: Any,
        *,
        timestamp: datetime | None = None,
        expected_version: int | None = None,
    ) -> int:
        current_iso = _iso(timestamp or utc_now())
        identifier = str(experiment_id).strip()
        if not identifier:
            raise ValueError("experiment_id is required")
        if expected_version is not None and (isinstance(expected_version, bool) or not isinstance(expected_version, int) or expected_version < -1):
            raise ValueError("expected_version must be None or a non-negative integer (or -1 for insert)")
        payload = _dump(state)
        with self._write_context():
            if expected_version is None:
                self._conn.execute(
                    "INSERT INTO paper_state(experiment_id,state_json,updated_at,state_version) VALUES (?,?,?,0) "
                    "ON CONFLICT(experiment_id) DO UPDATE SET state_json=excluded.state_json,"
                    "updated_at=excluded.updated_at,state_version=paper_state.state_version+1",
                    (identifier, payload, current_iso),
                )
            elif expected_version == -1:
                cursor = self._conn.execute(
                    "INSERT INTO paper_state(experiment_id,state_json,updated_at,state_version) VALUES (?,?,?,0) "
                    "ON CONFLICT(experiment_id) DO NOTHING",
                    (identifier, payload, current_iso),
                )
                if cursor.rowcount != 1:
                    raise RuntimeError("paper state was created concurrently")
            else:
                cursor = self._conn.execute(
                    "UPDATE paper_state SET state_json=?,updated_at=?,state_version=state_version+1 "
                    "WHERE experiment_id=? AND state_version=?",
                    (payload, current_iso, identifier, expected_version),
                )
                if cursor.rowcount != 1:
                    raise RuntimeError("paper state changed concurrently")
            row = self._conn.execute("SELECT state_version FROM paper_state WHERE experiment_id=?", (identifier,)).fetchone()
        return int(row["state_version"])

    def load_paper_state(self, experiment_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute("SELECT * FROM paper_state WHERE experiment_id=?", (str(experiment_id),)).fetchone()
        if row is None:
            return None
        return {
            "experiment_id": row["experiment_id"],
            "state": _load(row["state_json"]),
            "updated_at": _parse_datetime(row["updated_at"]),
            "state_version": int(row["state_version"] or 0),
        }
    def list_paper_states(self, *, limit: int = 100) -> list[dict[str, Any]]:
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 0:
            raise ValueError("limit must be non-negative")
        with self._lock:
            rows = self._conn.execute("SELECT * FROM paper_state ORDER BY updated_at DESC,experiment_id LIMIT ?", (int(limit),)).fetchall()
        return [
            {
                "experiment_id": row["experiment_id"],
                "state": _load(row["state_json"]),
                "updated_at": _parse_datetime(row["updated_at"]),
            }
            for row in rows
        ]

    def paper_observation_exists(self, observation_id: str) -> bool:
        identifier = str(observation_id).strip()
        if not identifier:
            raise ValueError("observation_id is required")
        with self._lock:
            return self._conn.execute(
                "SELECT 1 FROM paper_observations WHERE observation_id=? LIMIT 1",
                (identifier,),
            ).fetchone() is not None

    def save_paper_observation(
        self,
        observation_id: str,
        experiment_id: str,
        market_id: str,
        timestamp: datetime,
        payload: Any,
    ) -> bool:
        identifier = str(observation_id).strip()
        experiment = str(experiment_id).strip()
        market = str(market_id).strip()
        if not identifier or not experiment or not market:
            raise ValueError("paper observation identifiers are required")
        timestamp_iso = _iso(timestamp)
        payload_json = _dump(payload)
        with self._write_context():
            existing = self._conn.execute(
                "SELECT experiment_id,market_id,timestamp,payload_json FROM paper_observations WHERE observation_id=?",
                (identifier,),
            ).fetchone()
            if existing is not None:
                if (
                    str(existing["experiment_id"]) != experiment
                    or str(existing["market_id"]) != market
                    or str(existing["timestamp"]) != timestamp_iso
                    or str(existing["payload_json"]) != payload_json
                ):
                    raise ValueError(f"paper observation conflicts with stored payload: {identifier}")
                return False
            self._conn.execute(
                "INSERT INTO paper_observations(observation_id,experiment_id,market_id,timestamp,payload_json,created_at) "
                "VALUES (?,?,?,?,?,?)",
                (identifier, experiment, market, timestamp_iso, payload_json, _now_iso()),
            )
        return True

    def list_paper_observations(self, experiment_id: str, *, limit: int | None = 1000) -> list[dict[str, Any]]:
        if limit is not None and (isinstance(limit, bool) or not isinstance(limit, int) or limit < 0):
            raise ValueError("limit must be a non-negative integer or None")
        query = "SELECT * FROM paper_observations WHERE experiment_id=? ORDER BY timestamp,observation_id"
        values: list[Any] = [str(experiment_id)]
        if limit is not None:
            query += " LIMIT ?"
            values.append(int(limit))
        with self._lock:
            rows = self._conn.execute(query, values).fetchall()
        return [
            {
                "observation_id": row["observation_id"],
                "experiment_id": row["experiment_id"],
                "market_id": row["market_id"],
                "timestamp": _parse_datetime(row["timestamp"]),
                "payload": _load(row["payload_json"]),
                "created_at": _parse_datetime(row["created_at"]),
            }
            for row in rows
        ]
    def save_paper_execution_event(
        self,
        event_id: str,
        experiment_id: str,
        observation_id: str,
        market_id: str,
        timestamp: datetime,
        status: str,
        payload: Any,
    ) -> bool:
        """Persist one idempotent paper execution outcome."""

        identifier = str(event_id).strip()
        experiment = str(experiment_id).strip()
        observation = str(observation_id).strip()
        market = str(market_id).strip()
        outcome = str(status).strip().upper()
        if not identifier or not experiment or not observation or not market or not outcome:
            raise ValueError("paper execution event identifiers and status are required")
        timestamp_iso = _iso(timestamp)
        payload_json = _dump(payload)
        with self._write_context():
            existing = self._conn.execute(
                "SELECT event_id,experiment_id,observation_id,market_id,timestamp,status,payload_json "
                "FROM paper_execution_events WHERE event_id=? OR (experiment_id=? AND observation_id=?)",
                (identifier, experiment, observation),
            ).fetchone()
            if existing is not None:
                if (
                    str(existing["event_id"]) != identifier
                    or str(existing["experiment_id"]) != experiment
                    or str(existing["observation_id"]) != observation
                    or str(existing["market_id"]) != market
                    or str(existing["timestamp"]) != timestamp_iso
                    or str(existing["status"]) != outcome
                    or str(existing["payload_json"]) != payload_json
                ):
                    raise ValueError(f"paper execution event conflicts with stored payload: {identifier}")
                return False
            self._conn.execute(
                "INSERT INTO paper_execution_events(event_id,experiment_id,observation_id,market_id,timestamp,status,payload_json,created_at) "
                "VALUES (?,?,?,?,?,?,?,?)",
                (identifier, experiment, observation, market, timestamp_iso, outcome, payload_json, _now_iso()),
            )
        return True

    def list_paper_execution_events(
        self,
        experiment_id: str,
        *,
        limit: int | None = 1000,
    ) -> list[dict[str, Any]]:
        if limit is not None and (isinstance(limit, bool) or not isinstance(limit, int) or limit < 0):
            raise ValueError("limit must be a non-negative integer or None")
        query = "SELECT * FROM paper_execution_events WHERE experiment_id=? ORDER BY timestamp,observation_id"
        values: list[Any] = [str(experiment_id)]
        if limit is not None:
            query += " LIMIT ?"
            values.append(int(limit))
        with self._lock:
            rows = self._conn.execute(query, values).fetchall()
        return [
            {
                "event_id": row["event_id"],
                "experiment_id": row["experiment_id"],
                "observation_id": row["observation_id"],
                "market_id": row["market_id"],
                "timestamp": _parse_datetime(row["timestamp"]),
                "status": row["status"],
                "payload": _load(row["payload_json"]),
                "created_at": _parse_datetime(row["created_at"]),
            }
            for row in rows
        ]

    def save_paper_bet_ledger(
        self,
        bet_id: str,
        experiment_id: str,
        market_id: str,
        strategy_id: str,
        outcome: str,
        resolution: str,
        resolved_at: datetime,
        payload: Any,
    ) -> bool:
        """Insert or idempotently refresh one resolved prediction bet."""

        identifier = str(bet_id).strip()
        experiment = str(experiment_id).strip()
        market = str(market_id).strip()
        strategy = str(strategy_id).strip()
        outcome_value = str(outcome).strip().lower()
        resolution_value = str(resolution).strip().lower()
        if not identifier or not experiment or not market or not strategy or not outcome_value or not resolution_value:
            raise ValueError("paper bet ledger identifiers and outcomes are required")
        resolved_iso = _iso(resolved_at)
        payload_json = _dump(payload)
        stamp = _now_iso()
        with self._write_context():
            existing = self._conn.execute(
                "SELECT bet_id,experiment_id,market_id,strategy_id,outcome,resolution,resolved_at,payload_json "
                "FROM paper_bet_ledger WHERE bet_id=? OR (experiment_id=? AND market_id=?)",
                (identifier, experiment, market),
            ).fetchone()
            if existing is not None:
                if (
                    str(existing["bet_id"]) != identifier
                    or str(existing["experiment_id"]) != experiment
                    or str(existing["market_id"]) != market
                    or str(existing["strategy_id"]) != strategy
                    or str(existing["outcome"]) != outcome_value
                    or str(existing["resolution"]) != resolution_value
                    or str(existing["resolved_at"]) != resolved_iso
                ):
                    raise ValueError(f"paper bet ledger conflicts with stored position: {identifier}")
                if str(existing["payload_json"]) == payload_json:
                    return False
                self._conn.execute(
                    "UPDATE paper_bet_ledger SET payload_json=?,updated_at=? WHERE bet_id=?",
                    (payload_json, stamp, identifier),
                )
                return False
            self._conn.execute(
                "INSERT INTO paper_bet_ledger(bet_id,experiment_id,market_id,strategy_id,outcome,resolution,resolved_at,payload_json,created_at,updated_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?)",
                (
                    identifier,
                    experiment,
                    market,
                    strategy,
                    outcome_value,
                    resolution_value,
                    resolved_iso,
                    payload_json,
                    stamp,
                    stamp,
                ),
            )
        return True

    def load_paper_bet_ledger(self, bet_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM paper_bet_ledger WHERE bet_id=?",
                (str(bet_id),),
            ).fetchone()
        if row is None:
            return None
        return {
            "bet_id": row["bet_id"],
            "experiment_id": row["experiment_id"],
            "market_id": row["market_id"],
            "strategy_id": row["strategy_id"],
            "outcome": row["outcome"],
            "resolution": row["resolution"],
            "resolved_at": _parse_datetime(row["resolved_at"]),
            "payload": _load(row["payload_json"]),
            "created_at": _parse_datetime(row["created_at"]),
            "updated_at": _parse_datetime(row["updated_at"]),
        }

    def list_paper_bet_ledger(
        self,
        experiment_id: str,
        *,
        limit: int | None = 1000,
    ) -> list[dict[str, Any]]:
        if limit is not None and (isinstance(limit, bool) or not isinstance(limit, int) or limit < 0):
            raise ValueError("limit must be a non-negative integer or None")
        query = "SELECT * FROM paper_bet_ledger WHERE experiment_id=? ORDER BY resolved_at,market_id"
        values: list[Any] = [str(experiment_id)]
        if limit is not None:
            query += " LIMIT ?"
            values.append(int(limit))
        with self._lock:
            rows = self._conn.execute(query, values).fetchall()
        return [
            {
                "bet_id": row["bet_id"],
                "experiment_id": row["experiment_id"],
                "market_id": row["market_id"],
                "strategy_id": row["strategy_id"],
                "outcome": row["outcome"],
                "resolution": row["resolution"],
                "resolved_at": _parse_datetime(row["resolved_at"]),
                "payload": _load(row["payload_json"]),
                "created_at": _parse_datetime(row["created_at"]),
                "updated_at": _parse_datetime(row["updated_at"]),
            }
            for row in rows
        ]

    def list_latest_paper_observations(
        self,
        experiment_id: str,
        *,
        per_market_limit: int = 512,
    ) -> list[dict[str, Any]]:
        if isinstance(per_market_limit, bool) or not isinstance(per_market_limit, int) or per_market_limit < 0:
            raise ValueError("per_market_limit must be a non-negative integer")
        with self._lock:
            rows = self._conn.execute(
                "SELECT observation_id,experiment_id,market_id,timestamp,payload_json,created_at FROM ("
                "SELECT p.*, ROW_NUMBER() OVER (PARTITION BY market_id ORDER BY timestamp DESC,observation_id DESC) AS row_number "
                "FROM paper_observations AS p WHERE experiment_id=?"
                ") WHERE row_number<=? ORDER BY timestamp,market_id,observation_id",
                (str(experiment_id), int(per_market_limit)),
            ).fetchall()
        return [
            {
                "observation_id": row["observation_id"],
                "experiment_id": row["experiment_id"],
                "market_id": row["market_id"],
                "timestamp": _parse_datetime(row["timestamp"]),
                "payload": _load(row["payload_json"]),
                "created_at": _parse_datetime(row["created_at"]),
            }
            for row in rows
        ]


    def save_opportunity_snapshots(self, observed_at: datetime, opportunities: Iterable[Any]) -> int:
        rows = []
        timestamp = _iso(observed_at)
        for opportunity in opportunities:
            payload = _dump(opportunity)
            identifier = "opportunity-" + hashlib.sha256((timestamp + payload).encode("utf-8")).hexdigest()
            rows.append((identifier, timestamp, payload, _now_iso()))
        inserted = 0
        if rows:
            with self._write_context():
                for row in rows:
                    cursor = self._conn.execute(
                        "INSERT OR IGNORE INTO opportunity_snapshots(opportunity_id,observed_at,payload_json,created_at) VALUES (?,?,?,?)",
                        row,
                    )
                    inserted += max(0, int(cursor.rowcount))
        return inserted

    def list_opportunity_snapshots(self, *, limit: int = 100) -> list[dict[str, Any]]:
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 0:
            raise ValueError("limit must be non-negative")
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM opportunity_snapshots ORDER BY observed_at DESC,opportunity_id LIMIT ?", (int(limit),)
            ).fetchall()
        return [
            {
                "opportunity_id": row["opportunity_id"],
                "observed_at": _parse_datetime(row["observed_at"]),
                "opportunity": _load(row["payload_json"]),
                "created_at": _parse_datetime(row["created_at"]),
            }
            for row in rows
        ]

    def save_experiment_budget(self, budget_id: str, budget: Any, *, timestamp: datetime | None = None) -> None:
        identifier = str(budget_id).strip()
        if not identifier or not isinstance(budget, Mapping):
            raise ValueError("budget_id and budget mapping are required")
        payload = dict(budget)
        try:
            total_limit = int(payload["total_limit"])
            per_family_limit = int(payload["per_family_limit"])
            used_total = int(payload.get("used_total", 0))
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError("budget limits and usage must be integers") from exc
        if (
            total_limit < 0
            or per_family_limit < 0
            or used_total < 0
            or used_total > total_limit
            or isinstance(payload.get("total_limit"), bool)
            or isinstance(payload.get("per_family_limit"), bool)
            or isinstance(payload.get("used_total", 0), bool)
        ):
            raise ValueError("budget limits and usage must be non-negative and within limits")
        used_by_family = payload.get("used_by_family", {})
        if not isinstance(used_by_family, Mapping):
            raise ValueError("used_by_family must be a mapping")
        normalized_family: dict[str, int] = {}
        for family, value in used_by_family.items():
            if isinstance(value, bool) or not isinstance(value, int) or value < 0 or value > per_family_limit:
                raise ValueError("family budget usage must be non-negative and within limits")
            normalized_family[str(family)] = value
        if sum(normalized_family.values()) > used_total:
            raise ValueError("family budget usage exceeds total usage")
        payload.update(
            {
                "budget_id": identifier,
                "total_limit": total_limit,
                "per_family_limit": per_family_limit,
                "used_total": used_total,
                "used_by_family": normalized_family,
            }
        )
        payload_json = _dump(payload)
        stamp = _iso(timestamp or utc_now())
        with self._write_context():
            existing = self._conn.execute(
                "SELECT payload_json FROM experiment_budget WHERE budget_id=?", (identifier,)
            ).fetchone()
            reservation_rows = self._conn.execute(
                "SELECT family,COUNT(*) AS n FROM experiment_budget_reservations WHERE budget_id=? GROUP BY family",
                (identifier,),
            ).fetchall()
            reservation_total = sum(int(row["n"]) for row in reservation_rows)
            reservation_family = {str(row["family"]): int(row["n"]) for row in reservation_rows}
            if existing is not None:
                prior = _load(existing["payload_json"])
                prior = prior if isinstance(prior, Mapping) else {}
                if (
                    int(prior.get("total_limit", total_limit)) != total_limit
                    or int(prior.get("per_family_limit", per_family_limit)) != per_family_limit
                ):
                    raise ValueError("experiment budget limits are immutable")
                prior_used = int(prior.get("used_total", 0))
                prior_family = prior.get("used_by_family", {})
                prior_family = prior_family if isinstance(prior_family, Mapping) else {}
                if used_total < max(prior_used, reservation_total):
                    raise ValueError("experiment budget usage cannot be reset or reduced")
                if any(
                    normalized_family.get(str(family), 0) < max(int(prior_family.get(family, 0)), count)
                    for family, count in reservation_family.items()
                ):
                    raise ValueError("experiment family usage cannot be reset or reduced")
                if any(
                    normalized_family.get(str(family), 0) < int(value)
                    for family, value in prior_family.items()
                ):
                    raise ValueError("experiment family usage cannot be reset or reduced")
            self._conn.execute(
                "INSERT INTO experiment_budget(budget_id,payload_json,updated_at) VALUES (?,?,?) "
                "ON CONFLICT(budget_id) DO UPDATE SET payload_json=excluded.payload_json,updated_at=excluded.updated_at",
                (identifier, payload_json, stamp),
            )

    def load_experiment_budget(self, budget_id: str = "default") -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute("SELECT * FROM experiment_budget WHERE budget_id=?", (str(budget_id),)).fetchone()
        if row is None:
            return None
        return {"budget_id": row["budget_id"], "budget": _load(row["payload_json"]), "updated_at": _parse_datetime(row["updated_at"])}
    def count_experiment_budget_reservations(
        self,
        budget_id: str,
        *,
        since: datetime,
        until: datetime | None = None,
    ) -> int:
        """Count durable experiment reservations in a bounded UTC window."""
        start = ensure_utc(since).isoformat()
        end = ensure_utc(until).isoformat() if until is not None else None
        with self._lock:
            if end is None:
                row = self._conn.execute(
                    "SELECT COUNT(*) AS n FROM experiment_budget_reservations "
                    "WHERE budget_id=? AND created_at>=?",
                    (str(budget_id), start),
                ).fetchone()
            else:
                row = self._conn.execute(
                    "SELECT COUNT(*) AS n FROM experiment_budget_reservations "
                    "WHERE budget_id=? AND created_at>=? AND created_at<?",
                    (str(budget_id), start, end),
                ).fetchone()
        return int(row["n"]) if row is not None else 0
    def experiment_budget_reservation_exists(self, budget_id: str, reservation_key: str) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM experiment_budget_reservations WHERE budget_id=? AND reservation_key=?",
                (str(budget_id), str(reservation_key)),
            ).fetchone()
        return row is not None


    def reserve_experiment_budget(
        self,
        budget_id: str,
        *,
        total_limit: int,
        per_family_limit: int,
        family: str,
        reservation_key: str,
        amount: int = 1,
        timestamp: datetime | None = None,
        daily_limit: int | None = None,
        daily_since: datetime | None = None,
        daily_until: datetime | None = None,
    ) -> dict[str, Any]:
        if isinstance(total_limit, bool) or not isinstance(total_limit, int) or total_limit < 0:
            raise ValueError("total_limit must be a non-negative integer")
        if isinstance(per_family_limit, bool) or not isinstance(per_family_limit, int) or per_family_limit < 0:
            raise ValueError("per_family_limit must be a non-negative integer")
        if isinstance(amount, bool) or not isinstance(amount, int) or amount <= 0:
            raise ValueError("amount must be a positive integer")
        if daily_limit is not None and (
            isinstance(daily_limit, bool) or not isinstance(daily_limit, int) or daily_limit < 0
        ):
            raise ValueError("daily_limit must be a non-negative integer or None")
        if (daily_since is None) != (daily_until is None):
            raise ValueError("daily_since and daily_until must be provided together")
        daily_start: str | None = None
        daily_end: str | None = None
        if daily_since is not None and daily_until is not None:
            if not isinstance(daily_since, datetime) or not isinstance(daily_until, datetime):
                raise ValueError("daily_since and daily_until must be datetimes")
            start = ensure_utc(daily_since)
            end = ensure_utc(daily_until)
            if start >= end:
                raise ValueError("daily_since must be before daily_until")
            daily_start = start.isoformat()
            daily_end = end.isoformat()
        if daily_limit is not None and daily_start is None:
            raise ValueError("daily_since and daily_until are required with daily_limit")
        if daily_limit is None and daily_start is not None:
            raise ValueError("daily_limit is required with daily_since and daily_until")
        identifier = str(budget_id)
        family_name = str(family).strip() or "unknown"
        reservation = str(reservation_key).strip()
        if not reservation:
            raise ValueError("reservation_key is required")
        stamp = _iso(timestamp or utc_now())

        def reserve() -> dict[str, Any]:
            existing_row = self._conn.execute(
                "SELECT family FROM experiment_budget_reservations WHERE budget_id=? AND reservation_key=?",
                (identifier, reservation),
            ).fetchone()
            existing_reservation = existing_row is not None
            if existing_row is not None and str(existing_row["family"]) != family_name:
                raise ValueError("reservation_key is already allocated to another experiment family")
            row = self._conn.execute(
                "SELECT payload_json FROM experiment_budget WHERE budget_id=?", (identifier,)
            ).fetchone()
            payload = _load(row["payload_json"]) if row else {}
            payload = dict(payload) if isinstance(payload, Mapping) else {}
            if row is not None:
                stored_total = payload.get("total_limit")
                stored_family = payload.get("per_family_limit")
                if (
                    stored_total is not None and int(stored_total) != total_limit
                ) or (
                    stored_family is not None and int(stored_family) != per_family_limit
                ):
                    raise ValueError("experiment budget limits are immutable")
            used_total = int(payload.get("used_total", 0))
            used_by_family = dict(payload.get("used_by_family", {})) if isinstance(payload.get("used_by_family", {}), Mapping) else {}
            total = int(payload.get("total_limit", total_limit))
            per_family = int(payload.get("per_family_limit", per_family_limit))
            if not existing_reservation:
                used_family = int(used_by_family.get(family_name, 0))
                if used_total + amount > total:
                    raise RuntimeError("experiment budget exhausted")
                if used_family + amount > per_family:
                    raise RuntimeError(f"experiment family budget exhausted: {family_name}")
                if daily_limit is not None:
                    daily_row = self._conn.execute(
                        "SELECT COUNT(*) AS n FROM experiment_budget_reservations "
                        "WHERE budget_id=? AND created_at>=? AND created_at<?",
                        (identifier, daily_start, daily_end),
                    ).fetchone()
                    daily_count = int(daily_row["n"]) if daily_row is not None else 0
                    if daily_count + amount > daily_limit:
                        raise RuntimeError("experiment daily budget exhausted")
                used_total += amount
                used_by_family[family_name] = used_family + amount
                self._conn.execute(
                    "INSERT INTO experiment_budget_reservations(budget_id,reservation_key,family,created_at) VALUES (?,?,?,?)",
                    (identifier, reservation, family_name, stamp),
                )
            record = {
                "budget_id": identifier,
                "total_limit": total,
                "per_family_limit": per_family,
                "used_total": used_total,
                "used_by_family": dict(sorted(used_by_family.items())),
            }
            self._conn.execute(
                "INSERT INTO experiment_budget(budget_id,payload_json,updated_at) VALUES (?,?,?) "
                "ON CONFLICT(budget_id) DO UPDATE SET payload_json=excluded.payload_json,updated_at=excluded.updated_at",
                (identifier, _dump(record), stamp),
            )
            return {"budget": record, "allocated": not existing_reservation}

        with self._lock:
            if self._transaction_depth:
                return reserve()
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                result = reserve()
                self._conn.commit()
                return result
            except Exception:
                self._conn.rollback()
                raise

    def save_worker_state(
        self,
        worker_name: str,
        status: str,
        payload: Any = None,
        *,
        started_at: datetime | None = None,
        heartbeat_at: datetime | None = None,
    ) -> None:
        worker = str(worker_name).strip()
        state = str(status).strip()
        if not worker:
            raise ValueError("worker_name is required")
        if not state:
            raise ValueError("worker status is required")
        heartbeat = heartbeat_at or utc_now()
        def operation() -> None:
            with self._write_context():
                self._conn.execute(
                    "INSERT INTO worker_state(worker_name,status,payload_json,started_at,heartbeat_at,updated_at) VALUES (?,?,?,?,?,?) "
                    "ON CONFLICT(worker_name) DO UPDATE SET status=excluded.status,payload_json=excluded.payload_json,"
                    "started_at=COALESCE(worker_state.started_at,excluded.started_at),heartbeat_at=excluded.heartbeat_at,updated_at=excluded.updated_at",
                    (
                        worker,
                        state,
                        _dump(payload if payload is not None else {}),
                        _iso(started_at) if started_at else None,
                        _iso(heartbeat),
                        _now_iso(),
                    ),
                )

        sqlite_retry(operation, operation_name=f"save worker state {worker}")

    def list_worker_states(self, *, limit: int = 256) -> list[dict[str, Any]]:
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 0:
            raise ValueError("limit must be a non-negative integer")
        with self._lock:
            rows = self._conn.execute("SELECT * FROM worker_state ORDER BY worker_name LIMIT ?", (int(limit),)).fetchall()
        return [
            {
                "worker_name": row["worker_name"],
                "status": row["status"],
                "payload": _load(row["payload_json"]),
                "started_at": _parse_datetime(row["started_at"]),
                "heartbeat_at": _parse_datetime(row["heartbeat_at"]),
                "updated_at": _parse_datetime(row["updated_at"]),
            }
            for row in rows
        ]

    def tracked_polymarket_markets(
        self,
        *,
        active_only: bool = True,
        now: datetime | None = None,
        include_payload: bool = False,
        limit: int = 1000,
    ) -> list[Any]:
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 0:
            raise ValueError("limit must be a non-negative integer")
        current = ensure_utc(now or utc_now())
        with self._lock:
            metadata_rows = self._conn.execute(
                "SELECT market_id,observed_at,metadata_hash,payload_json,source_type FROM ("
                "SELECT market_id,observed_at,metadata_hash,payload_json,source_type,"
                "ROW_NUMBER() OVER (PARTITION BY market_id ORDER BY observed_at DESC,metadata_hash DESC) AS row_number "
                "FROM polymarket_markets WHERE source_type='FORWARD_COLLECTED' AND observed_at<=?) WHERE row_number=1 ORDER BY market_id LIMIT ?",
                (current.isoformat(), int(limit)),
            ).fetchall()
            snapshot_rows = self._conn.execute(
                "SELECT market_id,observed_at,source_timestamp,snapshot_id,payload_json,source_type FROM ("
                "SELECT market_id,observed_at,source_timestamp,snapshot_id,payload_json,source_type,"
                "ROW_NUMBER() OVER (PARTITION BY market_id ORDER BY observed_at DESC,source_timestamp DESC,snapshot_id DESC) AS row_number "
                "FROM polymarket_snapshots WHERE source_type='FORWARD_COLLECTED' AND observed_at<=?) WHERE row_number=1 ORDER BY market_id LIMIT ?",
                (current.isoformat(), int(limit)),
            ).fetchall()
        metadata_latest: dict[str, tuple[datetime | None, Any]] = {}
        snapshot_latest: dict[str, tuple[datetime | None, Any]] = {}
        for row in metadata_rows:
            stamp = _parse_datetime(row["observed_at"])
            if stamp is not None and stamp > current:
                continue
            identifier = str(row["market_id"])
            metadata_latest[identifier] = (stamp, _load(row["payload_json"]))
        for row in snapshot_rows:
            stamp = _parse_datetime(row["observed_at"])
            if stamp is not None and stamp > current:
                continue
            identifier = str(row["market_id"])
            snapshot_latest[identifier] = (stamp, _load(row["payload_json"]))
        result: list[dict[str, Any]] = []
        terminal = {
            SettlementState.RESOLVED_YES.value,
            SettlementState.RESOLVED_NO.value,
            SettlementState.VOID.value,
        }
        for market_id in sorted(set(metadata_latest) | set(snapshot_latest)):
            metadata_stamp, metadata_payload = metadata_latest.get(market_id, (None, None))
            snapshot_stamp, snapshot_payload = snapshot_latest.get(market_id, (None, None))
            if snapshot_stamp is not None and (metadata_stamp is None or snapshot_stamp >= metadata_stamp):
                payload = dict(snapshot_payload) if isinstance(snapshot_payload, Mapping) else {}
            else:
                payload = dict(metadata_payload) if isinstance(metadata_payload, Mapping) else {}
            if isinstance(metadata_payload, Mapping) and "metadata" not in payload:
                payload["metadata"] = metadata_payload.get("metadata")
            if isinstance(snapshot_payload, Mapping) and "snapshot" not in payload:
                payload["snapshot"] = snapshot_payload.get("snapshot")
            metadata_value = payload.get("metadata", {})
            metadata = metadata_value if isinstance(metadata_value, Mapping) else {}
            metadata_extra_value = metadata.get("extra", {})
            metadata_extra = metadata_extra_value if isinstance(metadata_extra_value, Mapping) else {}
            snapshot_value = payload.get("snapshot", {})
            snapshot = snapshot_value if isinstance(snapshot_value, Mapping) else {}
            settlement = str(snapshot.get("settlement", payload.get("settlement", ""))).strip().lower()
            raw_closed = metadata.get(
                "closed",
                payload.get("closed", metadata_extra.get("closed", False)),
            )
            closed = raw_closed if isinstance(raw_closed, bool) else str(raw_closed).strip().lower() in {"1", "true", "yes", "y", "on", "closed"}
            raw_active = metadata.get("active", payload.get("active", metadata_extra.get("active")))
            explicit_active = None if raw_active is None else (
                raw_active if isinstance(raw_active, bool) else str(raw_active).strip().lower() in {"1", "true", "yes", "y", "on", "active"}
            )
            expiry = _parse_datetime(
                snapshot.get("expiry")
                or metadata.get("expiry")
                or payload.get("expiry")
                or metadata_extra.get("expiry")
            )
            observed_at = snapshot_stamp or metadata_stamp
            active = (
                (explicit_active if explicit_active is not None else not closed)
                and not closed
                and settlement not in terminal
                and (expiry is None or expiry > current)
            )
            if active_only and not active:
                continue
            result.append(
                {
                    "market_id": market_id,
                    "observed_at": observed_at,
                    "active": active,
                    "payload": payload,
                }
            )
        bounded = result[:limit]
        return bounded if include_payload else [item["market_id"] for item in bounded]
    def paginate_dataset_catalog(
        self,
        *,
        page: int = 1,
        page_size: int = _DEFAULT_PAGE_SIZE,
        source_type: str | None = None,
        market_type: str | None = None,
        instrument: str | None = None,
        timeframe: str | None = None,
        quality: str | None = None,
        market: str | None = None,
        category: str | None = None,
        dataset_id: str | None = None,
        sort: str = "updated_at",
        direction: str = "desc",
        filter: str | None = None,
    ) -> dict[str, Any]:
        """Read a page of dataset catalog records without materializing the catalog."""
        requested_page, size = _pagination_args(page, page_size)
        sort_columns = {
            "updated_at": "updated_at",
            "last_updated": "updated_at",
            "created_at": "created_at",
            "dataset_id": "dataset_id",
            "dataset_version": "dataset_version",
            "version": "dataset_version",
            "provider": "provider",
            "instrument": "instrument",
            "market_type": "market_type",
            "timeframe": "timeframe",
            "row_count": "row_count",
            "completeness": "completeness",
            "quality": "quality",
            "source_type": "source_type",
        }
        order_column = sort_columns.get(str(sort or "updated_at").strip().lower())
        if order_column is None:
            raise ValueError(f"unsupported dataset catalog sort: {sort}")
        order_direction = str(direction or "desc").strip().lower()
        if order_direction not in {"asc", "desc"}:
            raise ValueError("direction must be 'asc' or 'desc'")
        clauses: list[str] = []
        values: list[Any] = []
        if dataset_id is not None and str(dataset_id).strip():
            clauses.append("dataset_id=?")
            values.append(str(dataset_id).strip())
        exact_filters = (
            ("source_type", source_type, True),
            ("market_type", market_type, False),
            ("instrument", instrument, False),
            ("timeframe", timeframe, False),
            ("quality", quality, False),
        )
        for column, value, uppercase in exact_filters:
            if value is not None and str(value).strip():
                text = _enum_value(value) or str(value).strip()
                clauses.append(f"{column}=?")
                values.append(text.upper() if uppercase else text)
        if market is not None and str(market).strip():
            clauses.append("market_type=?")
            values.append(str(market).strip())
        if category is not None and str(category).strip():
            clauses.append(
                "(lower(json_extract(metadata_json,'$.category'))=lower(?) "
                "OR lower(json_extract(metadata_json,'$.metadata.category'))=lower(?) "
                "OR lower(market_type)=lower(?))"
            )
            category_text = str(category).strip()
            values.extend([category_text, category_text, category_text])
        like_sql, like_values = _like_filter(
            filter,
            (
                "dataset_id",
                "dataset_version",
                "provider",
                "instrument",
                "market_type",
                "timeframe",
                "quality",
                "source_type",
                "metadata_json",
            ),
        )
        if like_sql:
            clauses.append(like_sql)
            values.extend(like_values)
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        with self._lock:
            total = int(self._conn.execute(f"SELECT COUNT(*) AS n FROM dataset_catalog{where}", values).fetchone()["n"])
            actual_page, pages = _pagination_shape(requested_page, size, total)
            query = (
                "SELECT * FROM dataset_catalog"
                f"{where} ORDER BY {order_column} {order_direction.upper()},dataset_id ASC,dataset_version ASC "
                "LIMIT ? OFFSET ?"
            )
            rows = self._conn.execute(query, [*values, size, (actual_page - 1) * size]).fetchall()
        return {
            "items": [_dataset_catalog_record(row) for row in rows],
            "page": actual_page,
            "page_size": size,
            "total": total,
            "pages": pages,
        }

    def paginate_dataset_missing_ranges(
        self,
        dataset_id: str,
        *,
        dataset_version: str | None = None,
        page: int = 1,
        page_size: int = _DEFAULT_PAGE_SIZE,
        sort: str = "range_index",
        direction: str = "asc",
        filter: str | None = None,
    ) -> dict[str, Any]:
        """Page a catalog row's missing-range JSON through SQLite ``json_each``."""
        requested_page, size = _pagination_args(page, page_size)
        identifier = str(dataset_id).strip()
        if not identifier:
            raise ValueError("dataset_id is required")
        sort_columns = {
            "range_index": "range_index",
            "range": "range_json",
            "dataset_version": "dataset_version",
        }
        order_column = sort_columns.get(str(sort or "range_index").strip().lower())
        if order_column is None:
            raise ValueError(f"unsupported missing range sort: {sort}")
        order_direction = str(direction or "asc").strip().lower()
        if order_direction not in {"asc", "desc"}:
            raise ValueError("direction must be 'asc' or 'desc'")
        cte = (
            "WITH ranges AS ("
            "SELECT c.dataset_id,c.dataset_version,CAST(r.key AS INTEGER) AS range_index,"
            "r.value AS range_json FROM dataset_catalog AS c "
            "JOIN json_each(c.missing_ranges_json) AS r "
            "WHERE c.dataset_id=?"
        )
        values: list[Any] = [identifier]
        if dataset_version is not None and str(dataset_version).strip():
            cte += " AND c.dataset_version=?"
            values.append(str(dataset_version).strip())
        cte += ")"
        clauses: list[str] = []
        filter_sql, filter_values = _like_filter(filter, ("dataset_version", "range_json", "range_index"))
        if filter_sql:
            clauses.append(filter_sql)
            values.extend(filter_values)
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        with self._lock:
            total = int(self._conn.execute(f"{cte} SELECT COUNT(*) AS n FROM ranges{where}", values).fetchone()["n"])
            actual_page, pages = _pagination_shape(requested_page, size, total)
            rows = self._conn.execute(
                f"{cte} SELECT dataset_id,dataset_version,range_index,range_json FROM ranges{where} "
                f"ORDER BY {order_column} {order_direction.upper()},dataset_version ASC,dataset_id ASC "
                "LIMIT ? OFFSET ?",
                [*values, size, (actual_page - 1) * size],
            ).fetchall()
        items: list[dict[str, Any]] = []
        for row in rows:
            raw_range = row["range_json"]
            try:
                parsed_range = _load(raw_range)
            except (TypeError, ValueError, json.JSONDecodeError):
                parsed_range = raw_range
            items.append(
                {
                    "dataset_id": row["dataset_id"],
                    "dataset_version": row["dataset_version"],
                    "range_index": int(row["range_index"]),
                    "range": parsed_range,
                    "missing_range": parsed_range,
                }
            )
        return {"items": items, "page": actual_page, "page_size": size, "total": total, "pages": pages}

    def paginate_candidate_lifecycle(
        self,
        *,
        page: int = 1,
        page_size: int = _DEFAULT_PAGE_SIZE,
        stage: str | None = None,
        quality: str | None = None,
        market: str | None = None,
        source_type: str | None = None,
        candidate_id: str | None = None,
        sort: str = "updated_at",
        direction: str = "desc",
        filter: str | None = None,
    ) -> dict[str, Any]:
        """Read the current candidate lifecycle rows with SQL-side filtering."""
        requested_page, size = _pagination_args(page, page_size)
        sort_columns = {
            "updated_at": "updated_at",
            "candidate_id": "candidate_id",
            "stage": "stage",
        }
        order_column = sort_columns.get(str(sort or "updated_at").strip().lower())
        if order_column is None:
            raise ValueError(f"unsupported candidate lifecycle sort: {sort}")
        order_direction = str(direction or "desc").strip().lower()
        if order_direction not in {"asc", "desc"}:
            raise ValueError("direction must be 'asc' or 'desc'")
        clauses: list[str] = []
        values: list[Any] = []
        if candidate_id is not None and str(candidate_id).strip():
            clauses.append("candidate_id=?")
            values.append(str(candidate_id).strip())
        if stage is not None and str(stage).strip():
            clauses.append("stage=?")
            values.append(str(stage).strip().upper())
        if quality is not None and str(quality).strip():
            quality_text = str(quality).strip()
            clauses.append(
                "(lower(json_extract(payload_json,'$.quality'))=lower(?) "
                "OR lower(json_extract(payload_json,'$.research_quality'))=lower(?) "
                "OR lower(json_extract(payload_json,'$.data_quality'))=lower(?))"
            )
            values.extend([quality_text] * 3)
        if market is not None and str(market).strip():
            market_text = str(market).strip()
            clauses.append(
                "(lower(json_extract(payload_json,'$.market'))=lower(?) "
                "OR lower(json_extract(payload_json,'$.market_type'))=lower(?))"
            )
            values.extend([market_text, market_text])
        if source_type is not None and str(source_type).strip():
            source_text = str(source_type).strip()
            clauses.append(
                "(lower(json_extract(payload_json,'$.source'))=lower(?) "
                "OR lower(json_extract(payload_json,'$.source_type'))=lower(?))"
            )
            values.extend([source_text, source_text])
        like_sql, like_values = _like_filter(filter, ("candidate_id", "stage", "payload_json"))
        if like_sql:
            clauses.append(like_sql)
            values.extend(like_values)
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        with self._lock:
            total = int(self._conn.execute(f"SELECT COUNT(*) AS n FROM candidate_lifecycle{where}", values).fetchone()["n"])
            actual_page, pages = _pagination_shape(requested_page, size, total)
            rows = self._conn.execute(
                "SELECT candidate_id,stage,payload_json,updated_at FROM candidate_lifecycle"
                f"{where} ORDER BY {order_column} {order_direction.upper()},candidate_id ASC "
                "LIMIT ? OFFSET ?",
                [*values, size, (actual_page - 1) * size],
            ).fetchall()
        items = [
            {
                "candidate_id": row["candidate_id"],
                "stage": row["stage"],
                "payload": _load(row["payload_json"]),
                "updated_at": _parse_datetime(row["updated_at"]),
            }
            for row in rows
        ]
        return {"items": items, "page": actual_page, "page_size": size, "total": total, "pages": pages}

    def paginate_candidate_lifecycle_events(
        self,
        *,
        candidate_id: str | None = None,
        stage: str | None = None,
        page: int = 1,
        page_size: int = _DEFAULT_PAGE_SIZE,
        sort: str = "created_at",
        direction: str = "desc",
        filter: str | None = None,
    ) -> dict[str, Any]:
        """Page immutable candidate lifecycle transition events."""
        requested_page, size = _pagination_args(page, page_size)
        sort_columns = {
            "created_at": "created_at",
            "event_id": "event_id",
            "candidate_id": "candidate_id",
            "from_stage": "from_stage",
            "to_stage": "to_stage",
        }
        order_column = sort_columns.get(str(sort or "created_at").strip().lower())
        if order_column is None:
            raise ValueError(f"unsupported candidate lifecycle event sort: {sort}")
        order_direction = str(direction or "desc").strip().lower()
        if order_direction not in {"asc", "desc"}:
            raise ValueError("direction must be 'asc' or 'desc'")
        clauses: list[str] = []
        values: list[Any] = []
        if candidate_id is not None and str(candidate_id).strip():
            clauses.append("candidate_id=?")
            values.append(str(candidate_id).strip())
        if stage is not None and str(stage).strip():
            stage_text = str(stage).strip().upper()
            clauses.append("(from_stage=? OR to_stage=?)")
            values.extend([stage_text, stage_text])
        like_sql, like_values = _like_filter(
            filter,
            ("event_id", "candidate_id", "from_stage", "to_stage", "reason", "payload_json"),
        )
        if like_sql:
            clauses.append(like_sql)
            values.extend(like_values)
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        with self._lock:
            total = int(
                self._conn.execute(f"SELECT COUNT(*) AS n FROM candidate_lifecycle_events{where}", values).fetchone()["n"]
            )
            actual_page, pages = _pagination_shape(requested_page, size, total)
            rows = self._conn.execute(
                "SELECT event_id,candidate_id,from_stage,to_stage,reason,payload_json,created_at "
                "FROM candidate_lifecycle_events"
                f"{where} ORDER BY {order_column} {order_direction.upper()},event_id ASC "
                "LIMIT ? OFFSET ?",
                [*values, size, (actual_page - 1) * size],
            ).fetchall()
        items = [
            {
                "event_id": row["event_id"],
                "candidate_id": row["candidate_id"],
                "from_stage": row["from_stage"],
                "to_stage": row["to_stage"],
                "reason": row["reason"],
                "payload": _load(row["payload_json"]),
                "created_at": _parse_datetime(row["created_at"]),
            }
            for row in rows
        ]
        return {"items": items, "page": actual_page, "page_size": size, "total": total, "pages": pages}

    def paginate_research_queue(
        self,
        *,
        page: int = 1,
        page_size: int = _DEFAULT_PAGE_SIZE,
        status: str | None = None,
        source: str | None = None,
        item_type: str | None = None,
        item_id: str | None = None,
        market: str | None = None,
        category: str | None = None,
        quality: str | None = None,
        sort: str = "priority",
        direction: str = "desc",
        filter: str | None = None,
    ) -> dict[str, Any]:
        """Read research queue items with bounded SQL pagination."""
        requested_page, size = _pagination_args(page, page_size)
        sort_columns = {
            "priority": "priority",
            "created_at": "created_at",
            "updated_at": "updated_at",
            "available_at": "available_at",
            "item_id": "item_id",
            "item_type": "item_type",
            "status": "status",
            "source": "source",
        }
        order_column = sort_columns.get(str(sort or "priority").strip().lower())
        if order_column is None:
            raise ValueError(f"unsupported research queue sort: {sort}")
        order_direction = str(direction or "desc").strip().lower()
        if order_direction not in {"asc", "desc"}:
            raise ValueError("direction must be 'asc' or 'desc'")
        clauses: list[str] = []
        values: list[Any] = []
        if item_id is not None and str(item_id).strip():
            clauses.append("item_id=?")
            values.append(str(item_id).strip())
        for column, value, uppercase in (
            ("status", status, True),
            ("source", source, False),
            ("item_type", item_type, False),
        ):
            if value is not None and str(value).strip():
                text = str(value).strip()
                clauses.append(f"{column}=?")
                values.append(text.upper() if uppercase else text)
        for key, value in (("market", market), ("category", category), ("quality", quality)):
            if value is not None and str(value).strip():
                text = str(value).strip()
                paths = {
                    "market": ("$.market", "$.market_id", "$.market_type"),
                    "category": ("$.category", "$.metadata.category"),
                    "quality": ("$.quality", "$.research_quality", "$.data_quality"),
                }[key]
                category_clauses = [f"lower(json_extract(payload_json,'{path}'))=lower(?)" for path in paths]
                if key == "category":
                    category_clauses.insert(0, "lower(item_type)=lower(?)")
                clauses.append("(" + " OR ".join(category_clauses) + ")")
                values.extend([text] * len(category_clauses))
        like_sql, like_values = _like_filter(
            filter,
            ("item_id", "item_type", "status", "source", "author", "payload_json", "last_error"),
        )
        if like_sql:
            clauses.append(like_sql)
            values.extend(like_values)
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        with self._lock:
            total = int(self._conn.execute(f"SELECT COUNT(*) AS n FROM research_queue{where}", values).fetchone()["n"])
            actual_page, pages = _pagination_shape(requested_page, size, total)
            rows = self._conn.execute(
                "SELECT * FROM research_queue"
                f"{where} ORDER BY {order_column} {order_direction.upper()},created_at ASC,item_id ASC "
                "LIMIT ? OFFSET ?",
                [*values, size, (actual_page - 1) * size],
            ).fetchall()
        items = [_research_queue_record(row) for row in rows]
        return {"items": items, "page": actual_page, "page_size": size, "total": total, "pages": pages}

    def paginate_research_activity(
        self,
        *,
        page: int = 1,
        page_size: int = _DEFAULT_PAGE_SIZE,
        source: str | None = None,
        source_type: str | None = None,
        kind: str | None = None,
        item_type: str | None = None,
        status: str | None = None,
        market: str | None = None,
        sort: str = "timestamp",
        direction: str = "desc",
        filter: str | None = None,
    ) -> dict[str, Any]:
        """Page the persisted activity feed using a SQL UNION.

        Every source contributes a stable key (for example ``report_id`` or
        ``event_id``), so equal timestamps never make page boundaries move.
        """
        requested_page, size = _pagination_args(page, page_size)
        sort_columns = {
            "timestamp": "timestamp",
            "created_at": "timestamp",
            "updated_at": "timestamp",
            "kind": "kind",
            "source": "source",
            "status": "status",
            "item_type": "item_type",
            "event_id": "event_id",
        }
        order_column = sort_columns.get(str(sort or "timestamp").strip().lower())
        if order_column is None:
            raise ValueError(f"unsupported activity sort: {sort}")
        order_direction = str(direction or "desc").strip().lower()
        if order_direction not in {"asc", "desc"}:
            raise ValueError("direction must be 'asc' or 'desc'")
        cte = """
            WITH activity(
                kind,timestamp,event_id,message,details_json,source,source_type,
                status,item_type,market_id
            ) AS (
                SELECT
                    'dataset', updated_at,
                    'dataset:' || dataset_id || '/' || dataset_version,
                    'Dataset ' || dataset_id || ' published (' || row_count || ' rows)',
                    json_object(
                        'dataset_id',dataset_id,'dataset_version',dataset_version,
                        'source_type',source_type,'timeframe',timeframe,'quality',quality
                    ),
                    source_type,source_type,NULL,NULL,NULL
                FROM dataset_catalog
                UNION ALL
                SELECT
                    'bootstrap', updated_at, 'bootstrap:' || dataset_id,
                    dataset_id || ' bootstrap ' || lower(status),
                    payload_json, 'bootstrap','bootstrap',status,NULL,NULL
                FROM dataset_bootstrap_state
                UNION ALL
                SELECT
                    'collection', COALESCE(ended_at,started_at), 'collection:' || cycle_id,
                    'Polymarket collection cycle completed (' ||
                        COALESCE(json_extract(payload_json,'$.markets_seen'),0) || ' markets)',
                    payload_json, collector_name,'collection',NULL,NULL,NULL
                FROM collection_cycles
                UNION ALL
                SELECT
                    'lifecycle', created_at, 'lifecycle:' || event_id,
                    'Candidate ' || candidate_id || ' moved to ' || to_stage,
                    json_object('from_stage',from_stage,'reason',reason),
                    'lifecycle','lifecycle',to_stage,NULL,NULL
                FROM candidate_lifecycle_events
                UNION ALL
                SELECT
                    'research', updated_at, 'research:item:' || item_id,
                    'Research item ' || item_type || ' is ' || lower(status),
                    json_object('item_id',item_id,'last_error',last_error),
                    source,'research',status,item_type,
                    json_extract(payload_json,'$.market_id')
                FROM research_queue
                UNION ALL
                SELECT
                    'research', created_at, 'research:event:' || event_id,
                    'Research queue item ' || item_id || ' moved to ' || to_status,
                    detail, 'queue','research',to_status,NULL,NULL
                FROM research_queue_events
                UNION ALL
                SELECT
                    'report', created_at, 'report:' || report_id,
                    'Research report ' || report_id || ' saved',
                    json_object('experiment_id',experiment_id),
                    'report','report',NULL,NULL,NULL
                FROM reports
                UNION ALL
                SELECT
                    'collection_error', observed_at, 'collection_error:' || error_id,
                    'Collection error: ' || kind || ' (' || detail || ')',
                    payload_json, 'collection','collection_error',kind,NULL,market_id
                FROM collection_errors
            )
        """
        clauses: list[str] = []
        values: list[Any] = []
        if source is not None and str(source).strip():
            text = str(source).strip()
            clauses.append("(source=? OR source_type=? OR kind=?)")
            values.extend([text, text, text])
        if source_type is not None and str(source_type).strip():
            text = str(source_type).strip()
            clauses.append("(source_type=? OR source=? OR kind=?)")
            values.extend([text, text, text])
        if kind is not None and str(kind).strip():
            clauses.append("kind=?")
            values.append(str(kind).strip().lower())
        if item_type is not None and str(item_type).strip():
            clauses.append("item_type=?")
            values.append(str(item_type).strip())
        if status is not None and str(status).strip():
            clauses.append("lower(status)=lower(?)")
            values.append(str(status).strip())
        if market is not None and str(market).strip():
            clauses.append("market_id=?")
            values.append(str(market).strip())
        like_sql, like_values = _like_filter(
            filter,
            ("kind", "timestamp", "event_id", "message", "details_json", "source", "source_type", "status", "item_type", "market_id"),
        )
        if like_sql:
            clauses.append(like_sql)
            values.extend(like_values)
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        with self._lock:
            total = int(self._conn.execute(f"{cte} SELECT COUNT(*) AS n FROM activity{where}", values).fetchone()["n"])
            actual_page, pages = _pagination_shape(requested_page, size, total)
            rows = self._conn.execute(
                f"{cte} SELECT kind,timestamp,event_id,message,details_json,source,source_type,status,item_type,market_id "
                f"FROM activity{where} "
                f"ORDER BY {order_column} {order_direction.upper()},event_id ASC "
                "LIMIT ? OFFSET ?",
                [*values, size, (actual_page - 1) * size],
            ).fetchall()
        items: list[dict[str, Any]] = []
        for row in rows:
            details = _load(row["details_json"]) if row["details_json"] else {}
            items.append(
                {
                    "kind": row["kind"],
                    "timestamp": _parse_datetime(row["timestamp"]),
                    "event_id": row["event_id"],
                    "message": row["message"],
                    "details": details if isinstance(details, Mapping) else {"value": details},
                    "source": row["source"],
                    "source_type": row["source_type"],
                    "status": row["status"],
                    "item_type": row["item_type"],
                    "market_id": row["market_id"],
                }
            )
        return {"items": items, "page": actual_page, "page_size": size, "total": total, "pages": pages}

    def paginate_polymarket_markets(
        self,
        *,
        page: int = 1,
        page_size: int = _DEFAULT_PAGE_SIZE,
        market: str | None = None,
        market_id: str | None = None,
        timeframe: str | None = None,
        quality: str | None = None,
        category: str | None = None,
        settlement: str | None = None,
        sort: str = "observed_at",
        direction: str = "desc",
        filter: str | None = None,
        include_snapshots: bool = True,
    ) -> dict[str, Any]:
        """Page one latest metadata/snapshot record per persisted market id."""
        requested_page, size = _pagination_args(page, page_size)
        sort_columns = {
            "observed_at": "observed_at",
            "source_timestamp": "source_timestamp",
            "market_id": "market_id",
            "quality": "quality",
            "category": "category",
            "timeframe": "timeframe",
            "settlement": "settlement",
        }
        order_column = sort_columns.get(str(sort or "observed_at").strip().lower())
        if order_column is None:
            raise ValueError(f"unsupported Polymarket sort: {sort}")
        order_direction = str(direction or "desc").strip().lower()
        if order_direction not in {"asc", "desc"}:
            raise ValueError("direction must be 'asc' or 'desc'")
        cte = """
            WITH metadata_ranked AS (
                SELECT market_id,observed_at,metadata_hash,payload_json,
                    ROW_NUMBER() OVER (
                        PARTITION BY market_id
                        ORDER BY observed_at DESC,metadata_hash DESC
                    ) AS row_number
                FROM polymarket_markets
            ),
            metadata_latest AS (
                SELECT market_id,observed_at,metadata_hash,payload_json
                FROM metadata_ranked WHERE row_number=1
            ),
            snapshot_ranked AS (
                SELECT market_id,observed_at,source_timestamp,snapshot_id,payload_json,quality,
                    ROW_NUMBER() OVER (
                        PARTITION BY market_id
                        ORDER BY observed_at DESC,source_timestamp DESC,snapshot_id DESC
                    ) AS row_number
                FROM polymarket_snapshots
            ),
            snapshot_latest AS (
                SELECT market_id,observed_at,source_timestamp,snapshot_id,payload_json,quality
                FROM snapshot_ranked WHERE row_number=1
            ),
            markets AS (
                SELECT
                    m.market_id,
                    COALESCE(s.observed_at,m.observed_at) AS observed_at,
                    m.observed_at AS metadata_observed_at,
                    m.metadata_hash,
                    m.payload_json AS metadata_payload,
                    s.observed_at AS snapshot_observed_at,
                    s.source_timestamp,
                    s.snapshot_id,
                    s.payload_json AS snapshot_payload,
                    s.quality,
                    COALESCE(
                        json_extract(s.payload_json,'$.category'),
                        json_extract(s.payload_json,'$.snapshot.category'),
                        json_extract(s.payload_json,'$.metadata.category'),
                        json_extract(m.payload_json,'$.category'),
                        json_extract(m.payload_json,'$.metadata.category')
                    ) AS category,
                    COALESCE(
                        json_extract(s.payload_json,'$.timeframe'),
                        json_extract(s.payload_json,'$.snapshot.timeframe'),
                        json_extract(s.payload_json,'$.metadata.timeframe'),
                        json_extract(m.payload_json,'$.timeframe'),
                        json_extract(m.payload_json,'$.metadata.timeframe')
                    ) AS timeframe,
                    lower(COALESCE(
                        json_extract(s.payload_json,'$.settlement'),
                        json_extract(s.payload_json,'$.snapshot.settlement'),
                        json_extract(s.payload_json,'$.metadata.settlement'),
                        json_extract(m.payload_json,'$.settlement'),
                        json_extract(m.payload_json,'$.metadata.settlement')
                    )) AS settlement
                FROM metadata_latest AS m
                LEFT JOIN snapshot_latest AS s ON s.market_id=m.market_id
                UNION ALL
                SELECT
                    s.market_id,
                    s.observed_at,
                    NULL,NULL,NULL,
                    s.observed_at,
                    s.source_timestamp,
                    s.snapshot_id,
                    s.payload_json,
                    s.quality,
                    COALESCE(
                        json_extract(s.payload_json,'$.category'),
                        json_extract(s.payload_json,'$.snapshot.category'),
                        json_extract(s.payload_json,'$.metadata.category')
                    ),
                    COALESCE(
                        json_extract(s.payload_json,'$.timeframe'),
                        json_extract(s.payload_json,'$.snapshot.timeframe'),
                        json_extract(s.payload_json,'$.metadata.timeframe')
                    ),
                    lower(COALESCE(
                        json_extract(s.payload_json,'$.settlement'),
                        json_extract(s.payload_json,'$.snapshot.settlement'),
                        json_extract(s.payload_json,'$.metadata.settlement')
                    ))
                FROM snapshot_latest AS s
                LEFT JOIN metadata_latest AS m ON m.market_id=s.market_id
                WHERE m.market_id IS NULL
            )
        """
        clauses: list[str] = []
        values: list[Any] = []
        identifier = market_id if market_id is not None else market
        if identifier is not None and str(identifier).strip():
            clauses.append("market_id=?")
            values.append(str(identifier).strip())
        for column, value in (("timeframe", timeframe), ("category", category), ("settlement", settlement)):
            if value is not None and str(value).strip():
                clauses.append(f"lower(CAST({column} AS TEXT))=lower(?)")
                values.append(_enum_value(value) or str(value).strip())
        if quality is not None and str(quality).strip():
            quality_text = _enum_value(quality) or str(quality).strip()
            clauses.append(
                "(lower(CAST(quality AS TEXT))=lower(?) "
                "OR lower(json_extract(snapshot_payload,'$.quality'))=lower(?) "
                "OR lower(json_extract(metadata_payload,'$.quality'))=lower(?))"
            )
            values.extend([quality_text] * 3)
        like_sql, like_values = _like_filter(
            filter,
            (
                "market_id",
                "metadata_payload",
                "snapshot_payload",
                "quality",
                "category",
                "timeframe",
                "settlement",
            ),
        )
        if like_sql:
            clauses.append(like_sql)
            values.extend(like_values)
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        with self._lock:
            total = int(self._conn.execute(f"{cte} SELECT COUNT(*) AS n FROM markets{where}", values).fetchone()["n"])
            actual_page, pages = _pagination_shape(requested_page, size, total)
            rows = self._conn.execute(
                f"{cte} SELECT market_id,observed_at,metadata_observed_at,metadata_hash,metadata_payload,"
                "snapshot_observed_at,source_timestamp,snapshot_id,snapshot_payload,quality,category,timeframe,settlement "
                f"FROM markets{where} ORDER BY {order_column} {order_direction.upper()},market_id ASC "
                "LIMIT ? OFFSET ?",
                [*values, size, (actual_page - 1) * size],
            ).fetchall()
        items: list[dict[str, Any]] = []
        for row in rows:
            metadata_payload = _load(row["metadata_payload"]) if row["metadata_payload"] else None
            snapshot_payload = _load(row["snapshot_payload"]) if row["snapshot_payload"] else None
            metadata = metadata_payload if isinstance(metadata_payload, Mapping) else {}
            snapshot_record = snapshot_payload if isinstance(snapshot_payload, Mapping) else {}
            if snapshot_record:
                payload = dict(snapshot_record)
            else:
                payload = dict(metadata)
            if metadata and "metadata" not in payload and "metadata" in metadata:
                payload["metadata"] = metadata["metadata"]
            if snapshot_record and "snapshot" not in payload and "snapshot" in snapshot_record:
                payload["snapshot"] = snapshot_record["snapshot"]
            metadata_value = payload.get("metadata", {})
            metadata_value = metadata_value if isinstance(metadata_value, Mapping) else {}
            snapshot_value = payload.get("snapshot", {})
            snapshot_value = snapshot_value if isinstance(snapshot_value, Mapping) else {}
            closed_value = metadata_value.get("closed", payload.get("closed", False))
            closed = (
                closed_value
                if isinstance(closed_value, bool)
                else str(closed_value).strip().lower() in {"1", "true", "yes", "y", "on", "closed"}
            )
            settlement_value = str(
                snapshot_value.get("settlement", payload.get("settlement", row["settlement"] or ""))
            ).strip().lower()
            active = not closed and settlement_value not in {
                SettlementState.RESOLVED_YES.value,
                SettlementState.RESOLVED_NO.value,
                SettlementState.VOID.value,
            }
            item = {
                "market_id": row["market_id"],
                "observed_at": _parse_datetime(row["observed_at"]),
                "metadata_observed_at": _parse_datetime(row["metadata_observed_at"]),
                "snapshot_observed_at": _parse_datetime(row["snapshot_observed_at"]),
                "source_timestamp": _parse_datetime(row["source_timestamp"]),
                "metadata_hash": row["metadata_hash"],
                "snapshot_id": row["snapshot_id"],
                "quality": row["quality"] or payload.get("quality") or payload.get("research_quality"),
                "category": row["category"],
                "timeframe": row["timeframe"],
                "settlement": settlement_value or None,
                "active": active,
                "payload": payload,
            }
            if include_snapshots:
                item["metadata"] = dict(metadata)
                item["snapshot"] = dict(snapshot_value)
            items.append(item)
        return {"items": items, "page": actual_page, "page_size": size, "total": total, "pages": pages}

    def paginate_paper_records(
        self,
        *,
        page: int = 1,
        page_size: int = _DEFAULT_PAGE_SIZE,
        experiment_id: str | None = None,
        market: str | None = None,
        market_id: str | None = None,
        status: str | None = None,
        record_type: str | None = None,
        sort: str = "timestamp",
        direction: str = "desc",
        filter: str | None = None,
    ) -> dict[str, Any]:
        """Page persisted paper states, observations, executions and bets."""
        requested_page, size = _pagination_args(page, page_size)
        sort_columns = {
            "timestamp": "timestamp",
            "created_at": "created_at",
            "updated_at": "updated_at",
            "record_type": "record_type",
            "record_id": "record_id",
            "experiment_id": "experiment_id",
            "market_id": "market_id",
            "status": "status",
        }
        order_column = sort_columns.get(str(sort or "timestamp").strip().lower())
        if order_column is None:
            raise ValueError(f"unsupported paper record sort: {sort}")
        order_direction = str(direction or "desc").strip().lower()
        if order_direction not in {"asc", "desc"}:
            raise ValueError("direction must be 'asc' or 'desc'")
        cte = """
            WITH paper_records(
                record_type,record_id,experiment_id,market_id,timestamp,status,
                outcome,resolution,strategy_id,payload_json,created_at,updated_at
            ) AS (
                SELECT
                    'state',experiment_id,experiment_id,NULL,updated_at,
                    json_extract(state_json,'$.status'),NULL,NULL,NULL,
                    state_json,updated_at,updated_at
                FROM paper_state
                UNION ALL
                SELECT
                    'observation',observation_id,experiment_id,market_id,timestamp,
                    json_extract(payload_json,'$.status'),NULL,NULL,NULL,
                    payload_json,created_at,created_at
                FROM paper_observations
                UNION ALL
                SELECT
                    'execution',event_id,experiment_id,market_id,timestamp,status,
                    NULL,NULL,NULL,payload_json,created_at,created_at
                FROM paper_execution_events
                UNION ALL
                SELECT
                    'bet',bet_id,experiment_id,market_id,resolved_at,resolution,
                    outcome,resolution,strategy_id,payload_json,created_at,updated_at
                FROM paper_bet_ledger
            )
        """
        clauses: list[str] = []
        values: list[Any] = []
        if experiment_id is not None and str(experiment_id).strip():
            clauses.append("experiment_id=?")
            values.append(str(experiment_id).strip())
        identifier = market_id if market_id is not None else market
        if identifier is not None and str(identifier).strip():
            clauses.append("market_id=?")
            values.append(str(identifier).strip())
        if status is not None and str(status).strip():
            status_text = str(status).strip()
            clauses.append(
                "(lower(CAST(status AS TEXT))=lower(?) "
                "OR lower(CAST(outcome AS TEXT))=lower(?) "
                "OR lower(CAST(resolution AS TEXT))=lower(?) "
                "OR lower(payload_json) LIKE lower(?))"
            )
            values.extend([status_text, status_text, status_text, f"%{status_text}%"])
        if record_type is not None and str(record_type).strip():
            record_type_text = str(record_type).strip().lower()
            record_type_aliases = {
                "states": "state",
                "paper_state": "state",
                "paper_states": "state",
                "observations": "observation",
                "paper_observations": "observation",
                "executions": "execution",
                "execution_events": "execution",
                "paper_execution_events": "execution",
                "bets": "bet",
                "ledger": "bet",
                "paper_bet_ledger": "bet",
            }
            normalized_type = record_type_aliases.get(record_type_text, record_type_text)
            if normalized_type not in {"state", "observation", "execution", "bet"}:
                raise ValueError(f"unsupported paper record type: {record_type}")
            clauses.append("record_type=?")
            values.append(normalized_type)
        like_sql, like_values = _like_filter(
            filter,
            (
                "record_type",
                "record_id",
                "experiment_id",
                "market_id",
                "timestamp",
                "status",
                "outcome",
                "resolution",
                "strategy_id",
                "payload_json",
            ),
        )
        if like_sql:
            clauses.append(like_sql)
            values.extend(like_values)
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        with self._lock:
            total = int(self._conn.execute(f"{cte} SELECT COUNT(*) AS n FROM paper_records{where}", values).fetchone()["n"])
            actual_page, pages = _pagination_shape(requested_page, size, total)
            rows = self._conn.execute(
                f"{cte} SELECT record_type,record_id,experiment_id,market_id,timestamp,status,outcome,"
                "resolution,strategy_id,payload_json,created_at,updated_at "
                f"FROM paper_records{where} ORDER BY {order_column} {order_direction.upper()},record_id ASC,record_type ASC "
                "LIMIT ? OFFSET ?",
                [*values, size, (actual_page - 1) * size],
            ).fetchall()
        items: list[dict[str, Any]] = []
        for row in rows:
            payload = _load(row["payload_json"]) if row["payload_json"] else {}
            item = {
                "record_type": row["record_type"],
                "record_id": row["record_id"],
                "id": row["record_id"],
                "experiment_id": row["experiment_id"],
                "market_id": row["market_id"],
                "timestamp": _parse_datetime(row["timestamp"]),
                "status": row["status"],
                "outcome": row["outcome"],
                "resolution": row["resolution"],
                "strategy_id": row["strategy_id"],
                "payload": payload,
                "created_at": _parse_datetime(row["created_at"]),
                "updated_at": _parse_datetime(row["updated_at"]),
            }
            if row["record_type"] == "state":
                item["state"] = payload
            items.append(item)
        return {"items": items, "page": actual_page, "page_size": size, "total": total, "pages": pages}

    def dashboard_coverage_summary(self) -> dict[str, Any]:
        """Return compact dashboard coverage aggregates without table-sized reads."""
        btc_query = (
            "SELECT * FROM ("
            "SELECT c.*,ROW_NUMBER() OVER (PARTITION BY timeframe "
            "ORDER BY updated_at DESC,dataset_id DESC,dataset_version DESC) AS row_number "
            "FROM dataset_catalog AS c "
            "WHERE source_type='HISTORICAL' AND lower(market_type)='crypto_spot' "
            "AND replace(replace(upper(instrument),'/',''),'-','')='BTCUSDT'"
            ") WHERE row_number=1 ORDER BY timeframe,dataset_id,dataset_version"
        )
        with self._lock:
            total_rows = self._conn.execute(
                "SELECT source_type,COUNT(*) AS dataset_count,COALESCE(SUM(row_count),0) AS row_count "
                "FROM dataset_catalog GROUP BY source_type"
            ).fetchall()
            btc_rows = self._conn.execute(btc_query).fetchall()
            aggregate_row = self._conn.execute(
                "SELECT row_count,quality,metadata_json FROM dataset_catalog "
                "WHERE source_type='HISTORICAL' AND lower(market_type)='prediction' "
                "AND dataset_id='Polymarket-historical' "
                "ORDER BY updated_at DESC,dataset_version DESC LIMIT 1"
            ).fetchone()
            prediction_total = self._conn.execute(
                "SELECT COUNT(DISTINCT dataset_id) AS n FROM dataset_catalog "
                "WHERE source_type='HISTORICAL' AND lower(market_type)='prediction' "
                "AND dataset_id LIKE 'prediction:%'"
            ).fetchone()
            prediction_points = self._conn.execute(
                "SELECT COALESCE(SUM(row_count),0) AS n FROM dataset_catalog "
                "WHERE source_type='HISTORICAL' AND lower(market_type)='prediction' "
                "AND dataset_id LIKE 'prediction:%'"
            ).fetchone()
            category_rows = self._conn.execute(
                "SELECT COALESCE(json_extract(metadata_json,'$.category'),'other') AS category,"
                "COUNT(*) AS n FROM dataset_catalog "
                "WHERE source_type='HISTORICAL' AND lower(market_type)='prediction' "
                "AND dataset_id LIKE 'prediction:%' GROUP BY category ORDER BY category"
            ).fetchall()
            quality_rows = self._conn.execute(
                "SELECT quality,COUNT(*) AS n FROM dataset_catalog "
                "WHERE source_type='HISTORICAL' AND lower(market_type)='prediction' "
                "AND dataset_id LIKE 'prediction:%' GROUP BY quality ORDER BY quality"
            ).fetchall()
            forward_where = (
                "upper(COALESCE(json_extract(payload_json,'$.source_type'),''))<>'HISTORICAL' "
                "AND snapshot_id NOT LIKE 'pmhist:%'"
            )
            forward_stats = self._conn.execute(
                "SELECT COUNT(*) AS snapshot_count,COUNT(DISTINCT market_id) AS market_count,"
                "COALESCE(SUM(CASE WHEN "
                "json_type(payload_json,'$.yes_order_book') IN ('object','array') "
                "OR json_type(payload_json,'$.no_order_book') IN ('object','array') "
                "OR json_type(payload_json,'$.order_book') IN ('object','array') THEN 1 ELSE 0 END),0) AS order_book_rows,"
                "MIN(observed_at) AS since FROM polymarket_snapshots WHERE " + forward_where
            ).fetchone()
            historical_book_rows = self._conn.execute(
                "SELECT COALESCE(SUM(CASE WHEN "
                "lower(COALESCE(json_extract(metadata_json,'$.historical_order_book_available'),'false')) "
                "IN ('1','true','yes','on') THEN row_count ELSE 0 END),0) AS n "
                "FROM dataset_catalog WHERE source_type='HISTORICAL' AND lower(market_type)='prediction' "
                "AND dataset_id LIKE 'prediction:%'"
            ).fetchone()
        totals = {
            str(row["source_type"]).lower(): {
                "count": int(row["dataset_count"]),
                "datasets": int(row["dataset_count"]),
                "rows": int(row["row_count"]),
            }
            for row in total_rows
        }
        historical = totals.get("historical", {"count": 0, "datasets": 0, "rows": 0})
        forward = totals.get("forward_collected", {"count": 0, "datasets": 0, "rows": 0})
        btc_catalog = [_dataset_catalog_record(row) for row in btc_rows]
        aggregate_metadata: Mapping[str, Any] = {}
        if aggregate_row is not None:
            loaded_metadata = _load(aggregate_row["metadata_json"])
            if isinstance(loaded_metadata, Mapping):
                aggregate_metadata = loaded_metadata
        category_value = aggregate_metadata.get("category_counts")
        if isinstance(category_value, Mapping):
            category_counts = {str(key): int(value) for key, value in category_value.items()}
        else:
            category_counts = {str(row["category"] or "other"): int(row["n"]) for row in category_rows}
        quality_counts = {str(row["quality"] or "UNKNOWN"): int(row["n"]) for row in quality_rows}
        prediction_count = int(prediction_total["n"] or 0)
        aggregate_points = int(aggregate_row["row_count"] or 0) if aggregate_row is not None else 0
        price_points = aggregate_points if aggregate_row is not None else int(prediction_points["n"] or 0)
        aggregate_quality = (
            str(aggregate_row["quality"])
            if aggregate_row is not None and aggregate_row["quality"] is not None
            else str(aggregate_metadata.get("research_quality") or "PRICE_PROXY")
        )
        historical_order_book = bool(
            aggregate_metadata.get("historical_order_book_available", False)
            or int(historical_book_rows["n"] or 0) > 0
        )
        forward_summary = {
            "tracked_markets": int(forward_stats["market_count"] or 0),
            "markets": int(forward_stats["market_count"] or 0),
            "snapshots": int(forward_stats["snapshot_count"] or 0),
            "order_book_rows": int(forward_stats["order_book_rows"] or 0),
            "since": _parse_datetime(forward_stats["since"]),
        }
        polymarket_summary = {
            "historical_distinct_prediction_datasets": prediction_count,
            "distinct_prediction_datasets": prediction_count,
            "historical_datasets": prediction_count,
            "price_points": price_points,
            "historical_price_points": price_points,
            "categories": category_counts,
            "category_counts": category_counts,
            "quality": aggregate_quality,
            "research_quality": aggregate_quality,
            "quality_counts": quality_counts,
            "historical_order_book_available": historical_order_book,
            "historical_order_book_rows": int(historical_book_rows["n"] or 0),
        }
        return {
            "historical_count": int(historical["count"]),
            "historical_datasets": int(historical["datasets"]),
            "historical_rows": int(historical["rows"]),
            "forward_count": int(forward["count"]),
            "forward_datasets": int(forward["datasets"]),
            "forward_rows": int(forward["rows"]),
            "btc_latest_catalog": btc_catalog,
            "btc_timeframes": btc_catalog,
            "btc": {"latest_by_timeframe": btc_catalog, "timeframes": btc_catalog},
            "polymarket_historical": polymarket_summary,
            "polymarket": polymarket_summary,
            "forward": forward_summary,
            "forward_tracked": forward_summary,
        }

    # Dashboard and quality ------------------------------------------
    def data_health(self, dataset_id: str | None = None, version: str | None = None) -> dict[str, Any]:
        """Return lightweight provenance/quality counters for dashboards.

        Quality is whatever the ingestion or simulation caller recorded; the
        store never upgrades a low-quality or synthetic dataset automatically.
        """
        clauses: list[str] = []
        values: list[Any] = []
        if dataset_id is not None:
            clauses.append("dataset_id=?")
            values.append(str(dataset_id))
        if version is not None:
            # Dataset version is metadata only on bars; requiring dataset id
            # avoids accidentally combining unrelated versions.
            clauses.append("dataset_version=?")
            values.append(str(version))
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        with self._lock:
            count = self._conn.execute("SELECT COUNT(*) AS n FROM bars" + where, values).fetchone()["n"]
            snapshots = self._conn.execute("SELECT COUNT(*) AS n FROM snapshots").fetchone()["n"]
            datasets = self._conn.execute("SELECT COUNT(*) AS n FROM datasets").fetchone()["n"]
            catalog_count = self._conn.execute("SELECT COUNT(*) AS n FROM dataset_catalog").fetchone()["n"]
            historical_catalog_count = self._conn.execute(
                "SELECT COUNT(*) AS n FROM dataset_catalog WHERE source_type='HISTORICAL'"
            ).fetchone()["n"]
            forward_catalog_count = self._conn.execute(
                "SELECT COUNT(*) AS n FROM dataset_catalog WHERE source_type='FORWARD_COLLECTED'"
            ).fetchone()["n"]
        quality = None
        if dataset_id is not None:
            record = self.load_dataset_record(dataset_id, version)
            quality = record.get("quality") if record else None
        return {
            "bars": int(count),
            "snapshots": int(snapshots),
            "datasets": int(datasets),
            "dataset_catalog": int(catalog_count),
            "historical_catalog": int(historical_catalog_count),
            "forward_catalog": int(forward_catalog_count),
            "quality": quality,
        }

    def dashboard_summary(self) -> dict[str, Any]:
        """Return persisted record counts and latest artifact timestamps."""
        with self._lock:
            result: dict[str, Any] = {}
            for table, label in (
                ("datasets", "datasets"),
                ("bars", "bars"),
                ("snapshots", "snapshots"),
                ("strategies", "strategies"),
                ("experiments", "experiments"),
                ("fills", "fills"),
                ("reports", "reports"),
                ("polymarket_markets", "polymarket_metadata"),
                ("polymarket_snapshots", "polymarket_snapshots"),
                ("polymarket_trades", "polymarket_trades"),
                ("collection_errors", "collection_errors"),
                ("forward_tests", "forward_tests"),
                ("collection_cycles", "collection_cycles"),
                ("research_queue", "research_queue"),
                ("candidate_lifecycle", "candidate_lifecycle"),
                ("paper_state", "paper_state"),
                ("paper_observations", "paper_observations"),
                ("opportunity_snapshots", "opportunity_snapshots"),
                ("experiment_plans", "experiment_plans"),
                ("experiment_budget", "experiment_budget"),
                ("worker_state", "worker_state"),
                ("dataset_catalog", "dataset_catalog"),
                ("dataset_bootstrap_state", "dataset_bootstrap_state"),
                ("historical_regime_labels", "historical_regime_labels"),
            ):
                result[label] = int(self._conn.execute(f"SELECT COUNT(*) AS n FROM {table}").fetchone()["n"])


            for table, label in (("datasets", "latest_dataset"), ("experiments", "latest_experiment"), ("reports", "latest_report")):
                row = self._conn.execute(f"SELECT created_at FROM {table} ORDER BY created_at DESC LIMIT 1").fetchone()
                result[label] = _parse_datetime(row["created_at"]) if row else None
        return result

    def query_dashboard(self) -> dict[str, Any]:
        return self.dashboard_summary()

    def _insert_many(self, table: str, rows: Sequence[Sequence[Any]], key_columns: str, columns: Sequence[str]) -> None:
        if not rows:
            return
        placeholders = ",".join("?" for _ in columns)

        def operation() -> None:
            with self._write_context():
                self._conn.executemany(
                    f"INSERT INTO {table}({','.join(columns)}) VALUES ({placeholders})", rows
                )

        try:
            sqlite_retry(operation, operation_name=f"insert into {table}")
        except sqlite3.IntegrityError as exc:
            raise ValueError(f"duplicate immutable record in {table} ({key_columns})") from exc
def _dataset_catalog_record(row: sqlite3.Row) -> dict[str, Any]:
    metadata = _load(row["metadata_json"])
    missing_ranges = _load(row["missing_ranges_json"])
    return {
        "dataset_id": row["dataset_id"],
        "dataset_version": row["dataset_version"],
        "version": row["dataset_version"],
        "provider": row["provider"],
        "instrument": row["instrument"],
        "market_type": row["market_type"],
        "timeframe": row["timeframe"],
        "start_timestamp": _parse_datetime(row["start_timestamp"]),
        "end_timestamp": _parse_datetime(row["end_timestamp"]),
        "row_count": int(row["row_count"]),
        "completeness": float(row["completeness"]),
        "missing_ranges": missing_ranges if isinstance(missing_ranges, list) else [],
        "quality": row["quality"],
        "source_type": row["source_type"],
        "snapshot_id": row["snapshot_id"],
        "created_at": _parse_datetime(row["created_at"]),
        "updated_at": _parse_datetime(row["updated_at"]),
        "last_updated": _parse_datetime(row["updated_at"]),
        "metadata": metadata if isinstance(metadata, Mapping) else {},
    }

def _research_queue_record(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {
        "item_id": row["item_id"],
        "item_type": row["item_type"],
        "dedupe_key": row["dedupe_key"],
        "status": row["status"],
        "priority": int(row["priority"]),
        "payload": _load(row["payload_json"]),
        "result": None if row["result_json"] is None else _load(row["result_json"]),
        "source": row["source"],
        "author": row["author"],
        "lineage": _load(row["lineage_json"]),
        "schema_version": row["schema_version"],
        "created_at": _parse_datetime(row["created_at"]),
        "updated_at": _parse_datetime(row["updated_at"]),
        "available_at": _parse_datetime(row["available_at"]),
        "lease_until": _parse_datetime(row["lease_until"]),
        "lease_owner": row["lease_owner"],
        "attempts": int(row["attempts"]),
        "last_error": row["last_error"],
    }


def _pagination_args(page: int, page_size: int) -> tuple[int, int]:
    """Validate the bounded dashboard pagination contract."""
    if isinstance(page, bool) or not isinstance(page, int) or page < 1:
        raise ValueError("page must be a positive integer")
    if isinstance(page_size, bool) or not isinstance(page_size, int) or page_size not in _PAGINATION_PAGE_SIZES:
        raise ValueError(f"page_size must be one of {_PAGINATION_PAGE_SIZES}")
    return int(page), int(page_size)


def _pagination_shape(requested_page: int, page_size: int, total: int) -> tuple[int, int]:
    if int(total) <= 0:
        return 1, 0
    pages = (int(total) + page_size - 1) // page_size
    return min(requested_page, pages), pages


def _pagination_response(
    requested_page: int,
    page_size: int,
    total: int,
    items: list[dict[str, Any]],
) -> dict[str, Any]:
    page, pages = _pagination_shape(requested_page, page_size, total)
    return {"items": items, "page": page, "page_size": page_size, "total": int(total), "pages": pages}
def _like_filter(value: Any, columns: Sequence[str]) -> tuple[str, list[Any]]:
    if value is None:
        return "", []
    text = str(value).strip().lower()
    if not text:
        return "", []
    pattern = f"%{text}%"
    return "(" + " OR ".join(f"lower(CAST({column} AS TEXT)) LIKE ?" for column in columns) + ")", [pattern] * len(columns)


def _polymarket_source_type(value: Any = None, payload: Any = None) -> str:
    candidate = value
    if candidate is None and isinstance(payload, Mapping):
        candidate = payload.get("source_type")
    normalized = str(candidate or "FORWARD_COLLECTED").strip().upper()
    if normalized not in _POLYMARKET_SOURCE_TYPES:
        raise ValueError("source_type must be HISTORICAL or FORWARD_COLLECTED")
    return normalized
def _enum_value(value: Any) -> str | None:
    if value is None:
        return None
    return str(value.value if isinstance(value, Enum) else value)


def _jsonable(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        return _iso(value)
    if is_dataclass(value):
        return _jsonable({name: getattr(value, name) for name in value.__dataclass_fields__})
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_jsonable(item) for item in value]
    if isinstance(value, set):
        return sorted(_jsonable(item) for item in value)
    return value


def _dump(value: Any) -> str:
    return json.dumps(_jsonable(value), sort_keys=True, separators=(",", ":"), allow_nan=False)


def _load(value: str) -> Any:
    return json.loads(value)
def _report_payload_equivalent(left: Any, right: Any) -> bool:
    if isinstance(left, Mapping) and isinstance(right, Mapping):
        left = {str(key): value for key, value in left.items() if str(key) != "generated_at"}
        right = {str(key): value for key, value in right.items() if str(key) != "generated_at"}
    return _dump(left) == _dump(right)


def _now_iso() -> str:
    return utc_now().isoformat()


def _iso(value: datetime) -> str:
    return ensure_utc(value).isoformat()


def _parse_datetime(value: Any) -> datetime | None:
    return parse_timestamp(value)


def _bar_from_record(record: Mapping[str, Any]) -> OHLCVBar:
    return OHLCVBar(
        timestamp=_parse_datetime(record.get("timestamp")) or datetime.fromtimestamp(0, tz=timezone.utc),
        open=float(record["open"]),
        high=float(record["high"]),
        low=float(record["low"]),
        close=float(record["close"]),
        volume=float(record["volume"]),
        spread=float(record["spread"]) if record.get("spread") is not None else None,
        trades=int(record["trades"]) if record.get("trades") is not None else None,
    )


def _snapshot_kind(snapshot: Any) -> tuple[str, datetime | None]:
    if isinstance(snapshot, PredictionMarketSnapshot):
        return "prediction", snapshot.timestamp
    if isinstance(snapshot, OrderBookSnapshot):
        return "order_book", snapshot.timestamp
    if isinstance(snapshot, CryptoTicker):
        return "ticker", snapshot.timestamp
    if isinstance(snapshot, ResolvedContract):
        return "resolved_contract", snapshot.resolved_at
    timestamp = getattr(snapshot, "timestamp", None)
    return type(snapshot).__name__.lower(), timestamp


def _level_records(value: Any) -> tuple[OrderBookLevel, ...]:
    if not isinstance(value, (list, tuple)):
        return ()
    result: list[OrderBookLevel] = []
    for item in value:
        if isinstance(item, Mapping):
            result.append(OrderBookLevel(float(item["price"]), float(item["size"])))
    return tuple(result)


def _book_from_record(record: Mapping[str, Any]) -> OrderBookSnapshot:
    return OrderBookSnapshot(
        timestamp=_parse_datetime(record.get("timestamp")) or datetime.fromtimestamp(0, tz=timezone.utc),
        bids=_level_records(record.get("bids")),
        asks=_level_records(record.get("asks")),
        token_id=record.get("token_id"),
    )


def _snapshot_from_record(kind: str, record: Mapping[str, Any]) -> Any:
    if kind == "order_book":
        return _book_from_record(record)
    if kind == "ticker":
        return CryptoTicker(
            timestamp=_parse_datetime(record.get("timestamp")) or datetime.fromtimestamp(0, tz=timezone.utc),
            symbol=str(record.get("symbol", "")),
            last=float(record["last"]),
            bid=float(record["bid"]) if record.get("bid") is not None else None,
            ask=float(record["ask"]) if record.get("ask") is not None else None,
            volume_24h=float(record["volume_24h"]) if record.get("volume_24h") is not None else None,
        )
    if kind == "prediction":
        order_book = record.get("order_book")
        settlement = _settlement(record.get("settlement"))
        return PredictionMarketSnapshot(
            timestamp=_parse_datetime(record.get("timestamp")) or datetime.fromtimestamp(0, tz=timezone.utc),
            market_id=str(record.get("market_id", "")),
            question=str(record.get("question", "")),
            yes_bid=_optional_float(record.get("yes_bid")),
            yes_ask=_optional_float(record.get("yes_ask")),
            yes_mid=_optional_float(record.get("yes_mid")),
            no_bid=_optional_float(record.get("no_bid")),
            no_ask=_optional_float(record.get("no_ask")),
            no_mid=_optional_float(record.get("no_mid")),
            volume=_optional_float(record.get("volume")),
            liquidity=_optional_float(record.get("liquidity")),
            expiry=_parse_datetime(record.get("expiry")),
            settlement=settlement,
            resolution_criteria=str(record.get("resolution_criteria", "")),
            category=record.get("category"),
            tags=tuple(str(item) for item in record.get("tags", ())),
            order_book=_book_from_record(order_book) if isinstance(order_book, Mapping) else None,
            source=str(record.get("source", "")),
            yes_token_id=record.get("yes_token_id"),
            no_token_id=record.get("no_token_id"),
        )
    return record


def _settlement(value: Any) -> SettlementState:
    try:
        return SettlementState(str(value))
    except ValueError:
        return SettlementState.UNKNOWN


def _optional_float(value: Any) -> float | None:
    return float(value) if value is not None else None


def _fill_from_record(record: Mapping[str, Any]) -> Fill:
    try:
        market_type = MarketType(str(record.get("market_type")))
    except ValueError:
        market_type = MarketType.CRYPTO_SPOT
    try:
        side = Side(str(record.get("side")))
    except ValueError:
        side = Side.BUY
    return Fill(
        timestamp=_parse_datetime(record.get("timestamp")) or datetime.fromtimestamp(0, tz=timezone.utc),
        market_type=market_type,
        symbol=str(record.get("symbol", "")),
        side=side,
        quantity=float(record.get("quantity", 0.0)),
        price=float(record.get("price", 0.0)),
        fees=float(record.get("fees", 0.0)),
        slippage=float(record.get("slippage", 0.0)),
        strategy_id=str(record.get("strategy_id", "")),
        order_id=str(record.get("order_id", "")),
        market_id=record.get("market_id"),
        expected_probability=_optional_float(record.get("expected_probability")),
        executable_probability=_optional_float(record.get("executable_probability")),
        metadata=record.get("metadata", {}),
    )
def _trade_from_record(record: Mapping[str, Any]) -> TradePrint:
    raw_side = record.get("side")
    try:
        side = Side(str(raw_side)) if raw_side is not None else None
    except ValueError:
        side = None
    return TradePrint(
        timestamp=_parse_datetime(record.get("timestamp")) or datetime.fromtimestamp(0, tz=timezone.utc),
        price=float(record["price"]),
        size=float(record.get("size", record.get("quantity"))),
        side=side,
        trade_id=record.get("trade_id"),
        market_id=record.get("market_id"),
        token_id=record.get("token_id"),
    )


def _storage_bytes(connection: sqlite3.Connection, path: str) -> int:
    if path not in {":memory:", ""} and not path.startswith("file:"):
        try:
            return int(Path(path).expanduser().stat().st_size)
        except OSError:
            pass
    try:
        page_count = int(connection.execute("PRAGMA page_count").fetchone()[0])
        page_size = int(connection.execute("PRAGMA page_size").fetchone()[0])
        return page_count * page_size
    except sqlite3.Error:
        return 0


__all__ = ["AxiomStore"]
