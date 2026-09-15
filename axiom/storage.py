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
import sqlite3
import threading
import time
from dataclasses import asdict, is_dataclass
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from enum import Enum
from itertools import islice
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator, Mapping, Sequence
from zoneinfo import ZoneInfo
_MAX_LATEST_SCAN_ROWS = 10_000
_CANARY_EQUITY_MARK_MAX_AGE_SECONDS = 300.0
_CANARY_CONFIRMED_SETTLEMENT_STATUSES = frozenset(
    {"CONFIRMED", "TRADE_STATUS_CONFIRMED", "SETTLED", "SETTLED_PARTIAL", "TRADE_STATUS_SETTLED"}
)
_CANARY_FINAL_SETTLEMENT_STATUSES = frozenset(
    {"SETTLED", "SETTLED_PARTIAL", "TRADE_STATUS_SETTLED", "FINAL", "CLOSED", "COMPLETED", "RESOLVED"}
)
_CANARY_RESERVATION_TERMINAL_STATUSES = frozenset(
    {"FILLED", "SETTLED", "RELEASED", "CANCELED", "CANCELLED", "REJECTED"}
)
_CANARY_LIMIT_ALIASES = {
    "target_notional_usd": "max_all_in_buy_usd",
    "max_exposure_usd": "max_aggregate_exposure_usd",
    "max_open_positions": "max_positions",
    "max_orders_per_day": "max_submitted_orders_per_day",
    "orders_per_day": "max_submitted_orders_per_day",
    "max_submissions_per_day": "max_submitted_orders_per_day",
    "submitted_orders_per_day": "max_submitted_orders_per_day",
    "total_submitted_orders_per_day": "max_submitted_orders_per_day",
    "max_total_submitted_orders_per_day": "max_submitted_orders_per_day",
    "daily_submission_limit": "max_submitted_orders_per_day",
    "max_all_in_buy_fee_reserve_usd": "max_all_in_buy_usd",
    "max_all_in_buy_reserve_usd": "max_all_in_buy_usd",
    "max_buy_reserve_usd": "max_all_in_buy_usd",
    "aggregate_open_cost_usd": "max_aggregate_open_cost_usd",
    "aggregate_exposure_usd": "max_aggregate_exposure_usd",
    "positions": "max_positions",
    "realized_loss_stop_usd": "realized_loss_entry_stop_usd",
    "equity_loss_stop_usd": "equity_loss_entry_stop_usd",
    "per_market_cap_usd": "per_market_buy_cap_usd",
    "per_event_cap_usd": "per_event_buy_cap_usd",
    "cumulative_cap_usd": "cumulative_buy_cap_usd",
}
_CANARY_DECIMAL_LIMITS = {
    "max_all_in_buy_usd",
    "max_fee_reserve_usd",
    "max_gross_daily_buy_usd",
    "max_aggregate_open_cost_usd",
    "max_aggregate_exposure_usd",
    "realized_loss_entry_stop_usd",
    "equity_loss_entry_stop_usd",
    "per_market_buy_cap_usd",
    "per_event_buy_cap_usd",
    "cumulative_buy_cap_usd",
}
_MAX_EVIDENCE_SCAN_ROWS = 100_000
_QUEUE_RELEASE_BATCH = 256
_QUEUE_LINEAGE_LIMIT = 256
_DEFAULT_HERMES_JOB_ID = "f1d27bf8c27a"
_PAPER_POSITION_PROJECTION_LIMIT = 32
_DATASET_METADATA_PROJECTION_LIMIT = 64
_DATASET_MISSING_RANGE_PROJECTION_LIMIT = 32
_DASHBOARD_PAYLOAD_MAX_BYTES = 65_536
_DASHBOARD_PAYLOAD_MAX_DEPTH = 4
_DASHBOARD_PAYLOAD_MAX_ITEMS = 64
_DASHBOARD_PAYLOAD_MAX_STRING = 4_096
_DASHBOARD_PAYLOAD_MAX_KEYS = 128
_DEFAULT_PAGE_SIZE = 25
_PAGINATION_PAGE_SIZES = (10, 25, 50, 100)
_POLYMARKET_SOURCE_TYPES = frozenset({"HISTORICAL", "FORWARD_COLLECTED"})
_ROLLING_EVIDENCE_SOURCE_CLASSES = frozenset(
    {"HISTORICAL", "PAPER", "FORWARD_COLLECTED"}
)
_ROLLING_EVALUATION_KINDS = frozenset({"CANONICAL_SIMULATION", "ACTUAL_LEDGER"})
_DEFAULT_OPERATIONAL_WINDOW_SECONDS = 3_600.0
_MAX_OPERATIONAL_WINDOW_SECONDS = 86_400.0
SQLITE_CONNECTION_TIMEOUT_SECONDS = 45.0
SQLITE_BUSY_RETRY_ATTEMPTS = 4
SQLITE_BUSY_RETRY_INITIAL_SECONDS = 0.05
SQLITE_BUSY_RETRY_MAX_SECONDS = 0.5
_MAX_DATASET_ATTESTATION_CONSTITUENTS = 1_000
_DATASET_ATTESTATION_STATUS_CURRENT = "CURRENT"
_DATASET_ATTESTATION_STATUS_STALE = "STALE"
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
        self._after_commit_callbacks: list[list[Callable[[], Any]]] = []
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
        with self._lock:
            rows = self._conn.execute("PRAGMA database_list").fetchall()
        for row in rows:
            if isinstance(row, Mapping):
                name = row.get("name")
                value = row.get("file")
            else:
                try:
                    if len(row) < 3:
                        continue
                    name = row[1]
                    value = row[2]
                except (IndexError, KeyError, TypeError):
                    continue
            if name == "main":
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
        callbacks_to_run: list[Callable[[], Any]] = []
        with self._lock:
            if immediate and not self._transaction_depth:
                sqlite_retry(
                    lambda: self._conn.execute("BEGIN IMMEDIATE"),
                    operation_name="begin immediate SQLite transaction",
                )
                self._transaction_depth += 1
                self._after_commit_callbacks.append([])
                try:
                    yield self
                except BaseException:
                    try:
                        self._conn.rollback()
                    finally:
                        self._after_commit_callbacks.pop()
                        self._transaction_depth -= 1
                    raise
                else:
                    try:
                        sqlite_retry(
                            self._conn.commit,
                            operation_name="commit SQLite transaction",
                        )
                    except BaseException:
                        try:
                            self._conn.rollback()
                        finally:
                            self._after_commit_callbacks.pop()
                            self._transaction_depth -= 1
                        raise
                    callbacks_to_run = self._after_commit_callbacks.pop()
                    self._transaction_depth -= 1
            else:
                self._transaction_depth += 1
                self._after_commit_callbacks.append([])
                savepoint = f"axiom_tx_{id(self):x}_{self._transaction_depth}"
                try:
                    self._conn.execute(f"SAVEPOINT {savepoint}")
                except BaseException:
                    self._after_commit_callbacks.pop()
                    self._transaction_depth -= 1
                    raise
                try:
                    yield self
                except BaseException:
                    try:
                        try:
                            self._conn.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
                        finally:
                            self._conn.execute(f"RELEASE SAVEPOINT {savepoint}")
                    finally:
                        self._after_commit_callbacks.pop()
                        self._transaction_depth -= 1
                    raise
                else:
                    try:
                        self._conn.execute(f"RELEASE SAVEPOINT {savepoint}")
                    except BaseException:
                        try:
                            try:
                                self._conn.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
                            finally:
                                self._conn.execute(f"RELEASE SAVEPOINT {savepoint}")
                        finally:
                            self._after_commit_callbacks.pop()
                            self._transaction_depth -= 1
                        raise
                    callbacks_to_run = self._after_commit_callbacks.pop()
                    self._transaction_depth -= 1
                    if self._after_commit_callbacks:
                        self._after_commit_callbacks[-1].extend(callbacks_to_run)
                        callbacks_to_run = []
        self._run_after_commit_callbacks(callbacks_to_run)

    def _run_after_commit_callbacks(self, callbacks: Sequence[Callable[[], Any]]) -> None:
        """Run committed callbacks without allowing projection failures to undo writes."""
        for callback in callbacks:
            try:
                callback()
            except Exception:
                _LOGGER.exception("after-commit callback failed")

    def after_commit(self, callback: Callable[[], Any]) -> None:
        """Run a callback after the outermost transaction durably commits."""
        if not callable(callback):
            raise TypeError("after_commit callback must be callable")
        with self._lock:
            if self._transaction_depth:
                self._after_commit_callbacks[-1].append(callback)
                return
            self._run_after_commit_callbacks((callback,))

    @contextmanager
    def _write_context(self) -> Iterator[None]:
        with self._lock:
            if self._transaction_depth:
                savepoint = f"axiom_write_{id(self):x}_{self._transaction_depth}"
                self._conn.execute(f"SAVEPOINT {savepoint}")
                try:
                    yield
                except BaseException:
                    try:
                        try:
                            self._conn.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
                        finally:
                            self._conn.execute(f"RELEASE SAVEPOINT {savepoint}")
                    finally:
                        raise
                else:
                    try:
                        self._conn.execute(f"RELEASE SAVEPOINT {savepoint}")
                    except BaseException:
                        try:
                            try:
                                self._conn.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
                            finally:
                                self._conn.execute(f"RELEASE SAVEPOINT {savepoint}")
                        finally:
                            raise
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
                CREATE TABLE IF NOT EXISTS dataset_integrity_attestation (
                    dataset_id TEXT NOT NULL,
                    dataset_version TEXT NOT NULL,
                    source_type TEXT NOT NULL,
                    market_type TEXT NOT NULL,
                    row_count INTEGER NOT NULL,
                    completeness REAL NOT NULL,
                    start_timestamp TEXT,
                    end_timestamp TEXT,
                    execution_fidelity TEXT NOT NULL,
                    contamination_result TEXT NOT NULL,
                    constituent_bindings_json TEXT NOT NULL DEFAULT '[]',
                    provenance_version TEXT NOT NULL DEFAULT '',
                    policy_version TEXT NOT NULL DEFAULT '',
                    reason TEXT NOT NULL DEFAULT '',
                    attestation_hash TEXT NOT NULL,
                    verified_at TEXT NOT NULL,
                    status TEXT NOT NULL,
                    PRIMARY KEY (dataset_id, dataset_version)
                );
                CREATE INDEX IF NOT EXISTS idx_dataset_integrity_attestation_status
                    ON dataset_integrity_attestation(status, dataset_id, dataset_version);
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
                CREATE TABLE IF NOT EXISTS scheduler_state (
                    scheduler_name TEXT PRIMARY KEY,
                    state_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS operator_config (
                    config_key TEXT PRIMARY KEY,
                    value_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS operator_jobs (
                    job_name TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    payload_json TEXT NOT NULL DEFAULT '{}',
                    pid INTEGER,
                    started_at TEXT,
                    updated_at TEXT NOT NULL,
                    last_error TEXT,
                    resumable INTEGER NOT NULL DEFAULT 0
                );
                CREATE INDEX IF NOT EXISTS idx_operator_jobs_updated
                    ON operator_jobs(updated_at, job_name);
                CREATE TABLE IF NOT EXISTS operator_actions (
                    action_id TEXT PRIMARY KEY,
                    action TEXT NOT NULL,
                    target TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    success INTEGER NOT NULL,
                    reason TEXT NOT NULL DEFAULT '',
                    result_json TEXT NOT NULL DEFAULT '{}'
                );
                CREATE INDEX IF NOT EXISTS idx_operator_actions_timestamp
                    ON operator_actions(timestamp, action_id);
                CREATE INDEX IF NOT EXISTS idx_operator_actions_action_target
                    ON operator_actions(action, target, timestamp);
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
                CREATE TABLE IF NOT EXISTS market_scope_resolutions (
                    resolution_id TEXT PRIMARY KEY,
                    candidate_id TEXT NOT NULL,
                    scope_hash TEXT NOT NULL,
                    scope_version TEXT NOT NULL,
                    resolved_at TEXT NOT NULL,
                    status TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    policy_json TEXT NOT NULL,
                    matched_markets_json TEXT NOT NULL,
                    excluded_markets_json TEXT NOT NULL,
                    deferred_markets_json TEXT NOT NULL,
                    provenance_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(candidate_id, scope_hash, scope_version, resolved_at)
                );
                CREATE INDEX IF NOT EXISTS idx_market_scope_resolutions_candidate
                    ON market_scope_resolutions(candidate_id, resolved_at DESC, resolution_id);
                CREATE INDEX IF NOT EXISTS idx_market_scope_resolutions_status
                    ON market_scope_resolutions(status, resolved_at DESC, candidate_id);
                CREATE INDEX IF NOT EXISTS idx_market_scope_resolutions_scope
                    ON market_scope_resolutions(scope_hash, scope_version, resolved_at DESC);
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
                "CREATE INDEX IF NOT EXISTS idx_research_queue_updated "
                "ON research_queue(updated_at DESC, created_at DESC, item_id DESC)"
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
            attestation_columns = {
                str(row["name"])
                for row in self._conn.execute("PRAGMA table_info(dataset_integrity_attestation)").fetchall()
            }
            if "reason" not in attestation_columns:
                self._conn.execute(
                    "ALTER TABLE dataset_integrity_attestation ADD COLUMN reason TEXT NOT NULL DEFAULT ''"
                )
            self._migrate_market_tables()
            self._initialize_canary_risk_schema()
            self._initialize_rolling_portfolio_schema()
            self._create_dataset_attestation_triggers()
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
    def _initialize_rolling_portfolio_schema(self) -> None:
        """Create additive rolling-portfolio tables without touching legacy data."""
        self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS strategy_versions (
                strategy_version_id TEXT PRIMARY KEY,
                strategy_id TEXT NOT NULL DEFAULT '',
                version TEXT NOT NULL DEFAULT '',
                code_hash TEXT NOT NULL DEFAULT '',
                config_hash TEXT NOT NULL DEFAULT '',
                payload_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                CHECK(length(strategy_version_id) BETWEEN 1 AND 256)
            );
            CREATE INDEX IF NOT EXISTS idx_strategy_versions_created
                ON strategy_versions(created_at DESC, strategy_version_id DESC);
            CREATE TABLE IF NOT EXISTS research_trials (
                research_trial_id TEXT PRIMARY KEY,
                strategy_version_id TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT '',
                payload_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                CHECK(length(research_trial_id) BETWEEN 1 AND 256),
                FOREIGN KEY(strategy_version_id) REFERENCES strategy_versions(strategy_version_id)
            );
            CREATE INDEX IF NOT EXISTS idx_research_trials_strategy_created
                ON research_trials(strategy_version_id, created_at DESC, research_trial_id DESC);
            CREATE TABLE IF NOT EXISTS rolling_strategy_enrollments (
                enrollment_id TEXT PRIMARY KEY,
                candidate_id TEXT NOT NULL,
                strategy_version_id TEXT,
                research_trial_id TEXT,
                status TEXT NOT NULL,
                reason TEXT NOT NULL,
                validation_version TEXT NOT NULL DEFAULT 'rolling-enrollment-v1',
                predecessor_enrollment_id TEXT,
                provenance_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                CHECK(length(enrollment_id) BETWEEN 1 AND 256),
                CHECK(length(candidate_id) BETWEEN 1 AND 256),
                CHECK(length(validation_version) BETWEEN 1 AND 128),
                CHECK(status IN ('ACCEPTED','EXCLUDED'))
            );
            CREATE INDEX IF NOT EXISTS idx_rolling_enrollments_candidate
                ON rolling_strategy_enrollments(candidate_id, created_at DESC, enrollment_id);
            CREATE INDEX IF NOT EXISTS idx_rolling_enrollments_status
                ON rolling_strategy_enrollments(status, created_at DESC, enrollment_id);
            CREATE TABLE IF NOT EXISTS admission_policies (
                policy_id TEXT NOT NULL,
                version TEXT NOT NULL,
                config_hash TEXT NOT NULL,
                payload_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                PRIMARY KEY(policy_id, version),
                CHECK(length(policy_id) BETWEEN 1 AND 256),
                CHECK(length(version) BETWEEN 1 AND 128),
                CHECK(length(config_hash) BETWEEN 1 AND 512)
            );
            CREATE INDEX IF NOT EXISTS idx_admission_policies_created
                ON admission_policies(created_at DESC, policy_id, version);
            CREATE TABLE IF NOT EXISTS strategy_evidence_windows (
                evidence_window_id TEXT PRIMARY KEY,
                strategy_version_id TEXT NOT NULL,
                research_trial_id TEXT,
                candidate_id TEXT,
                available_from TEXT NOT NULL,
                available_through TEXT NOT NULL,
                requested_days INTEGER NOT NULL CHECK(requested_days IN (7, 30)),
                actual_coverage_seconds INTEGER NOT NULL CHECK(actual_coverage_seconds >= 0),
                observation_completeness TEXT NOT NULL DEFAULT '0',
                source_class TEXT NOT NULL,
                paper_sizing_assumptions_json TEXT NOT NULL DEFAULT '{}',
                paper_fee_assumptions_json TEXT NOT NULL DEFAULT '{}',
                paper_slippage_assumptions_json TEXT NOT NULL DEFAULT '{}',
                allocated_capital_net_return TEXT NOT NULL DEFAULT '0',
                realized_pnl TEXT NOT NULL DEFAULT '0',
                unrealized_pnl TEXT NOT NULL DEFAULT '0',
                fees TEXT NOT NULL DEFAULT '0',
                costs TEXT NOT NULL DEFAULT '0',
                drawdown TEXT NOT NULL DEFAULT '0',
                completed_outcomes INTEGER NOT NULL DEFAULT 0 CHECK(completed_outcomes >= 0),
                reliability TEXT NOT NULL DEFAULT '0',
                execution_feasibility TEXT NOT NULL DEFAULT '',
                evidence_digest TEXT NOT NULL,
                payload_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                evaluation_run_id TEXT,
                evaluation_version TEXT,
                supersedes_evidence_id TEXT,
                loaded_rows INTEGER,
                valid_input_rows INTEGER,
                evaluator_invoked INTEGER,
                evaluator_completed INTEGER,
                evaluated_observations INTEGER,
                signal_count INTEGER,
                diagnostic_summary_count INTEGER,
                evaluator_name TEXT,
                evaluator_error TEXT,
                evaluator_prerequisite TEXT,
                accounting_available INTEGER,
                evaluation_json TEXT,
                portfolio_accounting_json TEXT,
                CHECK(length(evidence_window_id) BETWEEN 1 AND 256),
                FOREIGN KEY(strategy_version_id) REFERENCES strategy_versions(strategy_version_id)
            );
            CREATE INDEX IF NOT EXISTS idx_strategy_evidence_windows_strategy_available
                ON strategy_evidence_windows(strategy_version_id, available_through DESC,
                                              available_from DESC, evidence_window_id DESC);
            CREATE INDEX IF NOT EXISTS idx_strategy_evidence_windows_created
                ON strategy_evidence_windows(created_at DESC, evidence_window_id DESC);
            CREATE TABLE IF NOT EXISTS portfolio_selections (
                portfolio_selection_id TEXT PRIMARY KEY,
                policy_id TEXT NOT NULL,
                policy_version TEXT NOT NULL,
                risk_config_id TEXT NOT NULL,
                risk_config_generation INTEGER NOT NULL,
                risk_config_hash TEXT NOT NULL,
                global_budget TEXT NOT NULL,
                k INTEGER NOT NULL CHECK(k BETWEEN 0 AND 10),
                selected_at TEXT NOT NULL,
                review_due_at TEXT NOT NULL,
                payload_json TEXT NOT NULL DEFAULT '{}',
                committed_at TEXT NOT NULL,
                CHECK(length(portfolio_selection_id) BETWEEN 1 AND 256),
                CHECK(risk_config_generation >= 0),
                FOREIGN KEY(policy_id, policy_version)
                    REFERENCES admission_policies(policy_id, version)
            );
            CREATE INDEX IF NOT EXISTS idx_portfolio_selections_committed
                ON portfolio_selections(committed_at DESC, portfolio_selection_id DESC);
            CREATE INDEX IF NOT EXISTS idx_portfolio_selections_policy
                ON portfolio_selections(policy_id, policy_version, committed_at DESC);
            CREATE TABLE IF NOT EXISTS portfolio_selection_members (
                portfolio_selection_id TEXT NOT NULL,
                strategy_version_id TEXT NOT NULL,
                research_trial_id TEXT,
                candidate_id TEXT,
                allocation TEXT NOT NULL,
                status TEXT NOT NULL,
                score TEXT NOT NULL,
                reason TEXT NOT NULL,
                evidence_window_id TEXT,
                overlap_key TEXT NOT NULL DEFAULT '',
                payload_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                PRIMARY KEY(portfolio_selection_id, strategy_version_id),
                FOREIGN KEY(portfolio_selection_id) REFERENCES portfolio_selections(portfolio_selection_id),
                FOREIGN KEY(strategy_version_id) REFERENCES strategy_versions(strategy_version_id),
                FOREIGN KEY(evidence_window_id) REFERENCES strategy_evidence_windows(evidence_window_id)
            );
            CREATE INDEX IF NOT EXISTS idx_portfolio_selection_members_selection
                ON portfolio_selection_members(portfolio_selection_id, score DESC, strategy_version_id);
            CREATE INDEX IF NOT EXISTS idx_portfolio_selection_members_strategy
                ON portfolio_selection_members(strategy_version_id, created_at DESC);
            CREATE TABLE IF NOT EXISTS portfolio_current_selection (
                pointer_id TEXT PRIMARY KEY CHECK(pointer_id = 'current'),
                portfolio_selection_id TEXT NOT NULL,
                committed_at TEXT NOT NULL,
                FOREIGN KEY(portfolio_selection_id) REFERENCES portfolio_selections(portfolio_selection_id)
            );
            CREATE TABLE IF NOT EXISTS portfolio_review_state (
                state_id TEXT PRIMARY KEY CHECK(state_id = 'current'),
                portfolio_selection_id TEXT,
                review_due_at TEXT,
                reviewed_at TEXT,
                status TEXT NOT NULL DEFAULT '',
                payload_json TEXT NOT NULL DEFAULT '{}',
                updated_at TEXT NOT NULL,
                FOREIGN KEY(portfolio_selection_id) REFERENCES portfolio_selections(portfolio_selection_id)
            );
            CREATE INDEX IF NOT EXISTS idx_portfolio_review_state_updated
                ON portfolio_review_state(updated_at DESC);
            CREATE TABLE IF NOT EXISTS rolling_evidence_cursor (
                cursor_id TEXT PRIMARY KEY CHECK(cursor_id = 'current'),
                last_strategy_version_id TEXT,
                last_research_trial_id TEXT,
                last_candidate_id TEXT,
                last_requested_days INTEGER,
                last_source_class TEXT,
                next_strategy_version_id TEXT,
                next_research_trial_id TEXT,
                next_candidate_id TEXT,
                next_requested_days INTEGER,
                next_source_class TEXT,
                attempt_timestamps_json TEXT NOT NULL DEFAULT '{}',
                attempts_json TEXT NOT NULL DEFAULT '{}',
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS rolling_evidence_blockers (
                blocker_id TEXT PRIMARY KEY,
                work_key TEXT NOT NULL,
                strategy_version_id TEXT NOT NULL,
                research_trial_id TEXT,
                candidate_id TEXT,
                requested_days INTEGER NOT NULL,
                source_class TEXT NOT NULL,
                prerequisite_fingerprint TEXT NOT NULL,
                blocker TEXT NOT NULL,
                detail TEXT NOT NULL DEFAULT '',
                first_seen_at TEXT NOT NULL,
                last_attempted_at TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 1 CHECK(attempts >= 1),
                payload_json TEXT NOT NULL DEFAULT '{}',
                UNIQUE(work_key, prerequisite_fingerprint)
            );
            CREATE INDEX IF NOT EXISTS idx_rolling_evidence_blockers_work
                ON rolling_evidence_blockers(work_key, last_attempted_at DESC);
            CREATE INDEX IF NOT EXISTS idx_rolling_evidence_blockers_fingerprint
                ON rolling_evidence_blockers(prerequisite_fingerprint, last_attempted_at DESC);
            """
        )
        enrollment_columns = {
            str(row["name"])
            for row in self._conn.execute(
                "PRAGMA table_info(rolling_strategy_enrollments)"
            ).fetchall()
        }
        # Existing exclusions are immutable historical attempts.  Backfill
        # them as v1 so the corrected validator can append a v2 successor.
        for name, definition in (
            ("validation_version", "TEXT NOT NULL DEFAULT 'rolling-enrollment-v1'"),
            ("predecessor_enrollment_id", "TEXT"),
        ):
            if name not in enrollment_columns:
                self._conn.execute(
                    f"ALTER TABLE rolling_strategy_enrollments ADD COLUMN {name} {definition}"
                )
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_rolling_enrollments_candidate_version "
            "ON rolling_strategy_enrollments(candidate_id, validation_version, created_at DESC, enrollment_id)"
        )
        evidence_columns = {
            str(row["name"])
            for row in self._conn.execute(
                "PRAGMA table_info(strategy_evidence_windows)"
            ).fetchall()
        }
        for name, definition in (
            ("evaluation_run_id", "TEXT"),
            ("evaluation_version", "TEXT"),
            ("supersedes_evidence_id", "TEXT"),
            ("loaded_rows", "INTEGER"),
            ("valid_input_rows", "INTEGER"),
            ("evaluator_invoked", "INTEGER"),
            ("evaluator_completed", "INTEGER"),
            ("evaluated_observations", "INTEGER"),
            ("signal_count", "INTEGER"),
            ("diagnostic_summary_count", "INTEGER"),
            ("evaluator_name", "TEXT"),
            ("evaluator_error", "TEXT"),
            ("evaluator_prerequisite", "TEXT"),
            ("accounting_available", "INTEGER"),
            ("evaluation_json", "TEXT"),
            ("portfolio_accounting_json", "TEXT"),
        ):
            if name not in evidence_columns:
                self._conn.execute(
                    f"ALTER TABLE strategy_evidence_windows ADD COLUMN {name} {definition}"
                )
        observation_completeness_added = False
        for name, definition in (
            ("research_trial_id", "TEXT"),
            ("candidate_id", "TEXT"),
            ("observation_completeness", "TEXT NOT NULL DEFAULT '0'"),
        ):
            if name not in evidence_columns:
                self._conn.execute(
                    f"ALTER TABLE strategy_evidence_windows ADD COLUMN {name} {definition}"
                )
                if name == "observation_completeness":
                    observation_completeness_added = True
        if observation_completeness_added:
            for row in self._conn.execute(
                "SELECT evidence_window_id,requested_days,actual_coverage_seconds "
                "FROM strategy_evidence_windows"
            ).fetchall():
                try:
                    requested_days = int(row["requested_days"])
                    actual_coverage = int(row["actual_coverage_seconds"])
                    if requested_days <= 0 or actual_coverage < 0:
                        continue
                    completeness = min(
                        Decimal("1"),
                        Decimal(actual_coverage) / Decimal(requested_days * 86400),
                    )
                except (InvalidOperation, TypeError, ValueError, ZeroDivisionError):
                    continue
                self._conn.execute(
                    "UPDATE strategy_evidence_windows SET observation_completeness=? "
                    "WHERE evidence_window_id=?",
                    (format(completeness, "f"), row["evidence_window_id"]),
                )
        member_columns = {
            str(row["name"])
            for row in self._conn.execute("PRAGMA table_info(portfolio_selection_members)").fetchall()
        }
        for name in ("research_trial_id", "candidate_id"):
            if name not in member_columns:
                self._conn.execute(
                    f"ALTER TABLE portfolio_selection_members ADD COLUMN {name} TEXT"
                )
        selection_columns = {
            str(row["name"])
            for row in self._conn.execute("PRAGMA table_info(portfolio_selections)").fetchall()
        }
        if "k" not in selection_columns:
            self._conn.execute(
                "ALTER TABLE portfolio_selections ADD COLUMN k INTEGER NOT NULL DEFAULT 0"
            )
        selection_sql_row = self._conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='portfolio_selections'"
        ).fetchone()
        selection_sql = " ".join(str(selection_sql_row["sql"] or "").upper().split()) if selection_sql_row else ""
        staged_selection_migration = self._conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' "
            "AND name IN ('portfolio_selection_members_legacy_k',"
            "'portfolio_current_selection_legacy_k','portfolio_review_state_legacy_k',"
            "'portfolio_selections_legacy_k') LIMIT 1"
        ).fetchone() is not None
        if "BETWEEN 1 AND 10" in selection_sql or staged_selection_migration:
            self._migrate_rolling_selection_k_constraint()
        self._conn.execute(
            "UPDATE portfolio_selections SET k=("
            "SELECT COUNT(*) FROM portfolio_selection_members "
            "WHERE portfolio_selection_members.portfolio_selection_id=portfolio_selections.portfolio_selection_id"
            ")"
        )
        for row in self._conn.execute(
            "SELECT portfolio_selection_id,k,payload_json FROM portfolio_selections"
        ).fetchall():
            payload = _load(row["payload_json"])
            canonical_payload = dict(payload) if isinstance(payload, Mapping) else {}
            canonical_payload["k"] = int(row["k"])
            payload_json = _rolling_dump(canonical_payload)
            if payload_json != row["payload_json"]:
                self._conn.execute(
                    "UPDATE portfolio_selections SET payload_json=? WHERE portfolio_selection_id=?",
                    (payload_json, row["portfolio_selection_id"]),
                )
        self._backfill_rolling_selection_member_bindings()

    def _migrate_rolling_selection_k_constraint(self) -> None:
        """Rebuild rolling tables without losing orphan historical rows.

        SQLite's ``executescript`` implicitly commits before running its
        statements, which can strand renamed tables if a foreign-key copy
        fails.  Use explicit statements in one transaction and recover any
        staging names left by an older interrupted migration.  The replacement
        tables intentionally omit foreign keys because prototype history may
        reference parents that were never materialized; new commits enforce
        those relationships in application code.
        """
        bases = (
            "portfolio_selection_members",
            "portfolio_current_selection",
            "portfolio_review_state",
            "portfolio_selections",
        )
        staged = {name: f"{name}_legacy_k" for name in bases}

        def exists(name: str) -> bool:
            return self._conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                (name,),
            ).fetchone() is not None

        started_transaction = False
        if not self._conn.in_transaction:
            self._conn.execute("BEGIN IMMEDIATE")
            started_transaction = True
        try:
            # Recover an interrupted pre-transaction migration.  Staging
            # tables are the source of truth; a replacement table created
            # before a crash is discarded so no partial copy can hide rows.
            if any(exists(name) for name in staged.values()):
                for name, legacy_name in staged.items():
                    if not exists(legacy_name):
                        if not exists(name):
                            raise sqlite3.OperationalError(
                                f"rolling migration staging table missing: {legacy_name}"
                            )
                        self._conn.execute(f"ALTER TABLE {name} RENAME TO {legacy_name}")
                for name in (
                    "portfolio_selection_members",
                    "portfolio_current_selection",
                    "portfolio_review_state",
                    "portfolio_selections",
                ):
                    if exists(name):
                        self._conn.execute(f"DROP TABLE {name}")
            if not all(exists(name) for name in staged.values()):
                if not all(exists(name) for name in bases):
                    raise sqlite3.OperationalError("rolling migration source tables are incomplete")
            for index_name in (
                "idx_portfolio_selections_committed",
                "idx_portfolio_selections_policy",
                "idx_portfolio_selection_members_selection",
                "idx_portfolio_selection_members_strategy",
                "idx_portfolio_review_state_updated",
            ):
                self._conn.execute(f"DROP INDEX IF EXISTS {index_name}")
            if all(exists(name) for name in bases):
                for name in (
                    "portfolio_selection_members",
                    "portfolio_current_selection",
                    "portfolio_review_state",
                    "portfolio_selections",
                ):
                    self._conn.execute(f"ALTER TABLE {name} RENAME TO {staged[name]}")
            legacy_member_columns = {
                str(row["name"])
                for row in self._conn.execute(
                    f"PRAGMA table_info({staged['portfolio_selection_members']})"
                ).fetchall()
            }
            legacy_research_trial = (
                "legacy.research_trial_id"
                if "research_trial_id" in legacy_member_columns
                else "NULL"
            )
            legacy_candidate = (
                "legacy.candidate_id"
                if "candidate_id" in legacy_member_columns
                else "NULL"
            )

            self._conn.execute(
                """
                CREATE TABLE portfolio_selections (
                    portfolio_selection_id TEXT PRIMARY KEY,
                    policy_id TEXT NOT NULL,
                    policy_version TEXT NOT NULL,
                    risk_config_id TEXT NOT NULL,
                    risk_config_generation INTEGER NOT NULL,
                    risk_config_hash TEXT NOT NULL,
                    global_budget TEXT NOT NULL,
                    k INTEGER NOT NULL CHECK(k BETWEEN 0 AND 10),
                    selected_at TEXT NOT NULL,
                    review_due_at TEXT NOT NULL,
                    payload_json TEXT NOT NULL DEFAULT '{}',
                    committed_at TEXT NOT NULL,
                    CHECK(length(portfolio_selection_id) BETWEEN 1 AND 256),
                    CHECK(risk_config_generation >= 0)
                )
                """
            )
            self._conn.execute(
                """
                INSERT INTO portfolio_selections(
                    portfolio_selection_id,policy_id,policy_version,risk_config_id,
                    risk_config_generation,risk_config_hash,global_budget,k,selected_at,
                    review_due_at,payload_json,committed_at
                )
                SELECT
                    legacy.portfolio_selection_id,legacy.policy_id,legacy.policy_version,
                    legacy.risk_config_id,legacy.risk_config_generation,legacy.risk_config_hash,
                    legacy.global_budget,
                    (SELECT COUNT(*) FROM portfolio_selection_members_legacy_k members
                     WHERE members.portfolio_selection_id=legacy.portfolio_selection_id),
                    legacy.selected_at,legacy.review_due_at,legacy.payload_json,legacy.committed_at
                FROM portfolio_selections_legacy_k legacy
                """
            )
            self._conn.execute(
                """
                CREATE TABLE portfolio_selection_members (
                    portfolio_selection_id TEXT NOT NULL,
                    strategy_version_id TEXT NOT NULL,
                    research_trial_id TEXT,
                    candidate_id TEXT,
                    allocation TEXT NOT NULL,
                    status TEXT NOT NULL,
                    score TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    evidence_window_id TEXT,
                    overlap_key TEXT NOT NULL DEFAULT '',
                    payload_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    PRIMARY KEY(portfolio_selection_id, strategy_version_id)
                )
                """
            )
            self._conn.execute(
                f"""
                INSERT INTO portfolio_selection_members(
                    portfolio_selection_id,strategy_version_id,research_trial_id,candidate_id,
                    allocation,status,score,reason,evidence_window_id,overlap_key,payload_json,created_at
                )
                SELECT legacy.portfolio_selection_id,legacy.strategy_version_id,
                       {legacy_research_trial},{legacy_candidate},legacy.allocation,legacy.status,
                       legacy.score,legacy.reason,legacy.evidence_window_id,legacy.overlap_key,
                       legacy.payload_json,legacy.created_at
                FROM portfolio_selection_members_legacy_k legacy
                """
            )
            self._conn.execute(
                """
                CREATE TABLE portfolio_current_selection (
                    pointer_id TEXT PRIMARY KEY CHECK(pointer_id = 'current'),
                    portfolio_selection_id TEXT NOT NULL,
                    committed_at TEXT NOT NULL
                )
                """
            )
            self._conn.execute(
                """
                INSERT INTO portfolio_current_selection(pointer_id,portfolio_selection_id,committed_at)
                SELECT pointer_id,portfolio_selection_id,committed_at
                FROM portfolio_current_selection_legacy_k
                """
            )
            self._conn.execute(
                """
                CREATE TABLE portfolio_review_state (
                    state_id TEXT PRIMARY KEY CHECK(state_id = 'current'),
                    portfolio_selection_id TEXT,
                    review_due_at TEXT,
                    reviewed_at TEXT,
                    status TEXT NOT NULL DEFAULT '',
                    payload_json TEXT NOT NULL DEFAULT '{}',
                    updated_at TEXT NOT NULL
                )
                """
            )
            self._conn.execute(
                """
                INSERT INTO portfolio_review_state(
                    state_id,portfolio_selection_id,review_due_at,reviewed_at,status,payload_json,updated_at
                )
                SELECT state_id,portfolio_selection_id,review_due_at,reviewed_at,status,payload_json,updated_at
                FROM portfolio_review_state_legacy_k
                """
            )
            for legacy_name in staged.values():
                self._conn.execute(f"DROP TABLE {legacy_name}")
            self._conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_portfolio_selections_committed
                    ON portfolio_selections(committed_at DESC, portfolio_selection_id DESC)
                """
            )
            self._conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_portfolio_selections_policy
                    ON portfolio_selections(policy_id, policy_version, committed_at DESC)
                """
            )
            self._conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_portfolio_selection_members_selection
                    ON portfolio_selection_members(portfolio_selection_id, score DESC, strategy_version_id)
                """
            )
            self._conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_portfolio_selection_members_strategy
                    ON portfolio_selection_members(strategy_version_id, created_at DESC)
                """
            )
            self._conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_portfolio_review_state_updated
                    ON portfolio_review_state(updated_at DESC)
                """
            )
        except BaseException:
            if started_transaction:
                self._conn.rollback()
            raise
        else:
            if started_transaction:
                self._conn.commit()

    def _backfill_rolling_selection_member_bindings(self) -> None:
        """Backfill legacy member lineage only when its identity is unambiguous."""
        member_rows = self._conn.execute(
            "SELECT portfolio_selection_id,strategy_version_id,research_trial_id,"
            "candidate_id,allocation,status,reason,payload_json "
            "FROM portfolio_selection_members"
        ).fetchall()
        if not member_rows:
            return

        def text(value: Any) -> str | None:
            if value is None:
                return None
            normalized = str(value).strip()
            return normalized or None

        def payload(value: Any) -> Mapping[str, Any]:
            try:
                parsed = _load(value)
            except Exception:
                return {}
            return parsed if isinstance(parsed, Mapping) else {}

        def payload_text(document: Mapping[str, Any], *names: str) -> str | None:
            values: list[str] = []
            for name in names:
                candidate = text(document.get(name))
                if candidate is not None:
                    values.append(candidate)
            binding = document.get("binding")
            if isinstance(binding, Mapping):
                for name in names:
                    candidate = text(binding.get(name))
                    if candidate is not None:
                        values.append(candidate)
            unique = sorted(set(values))
            return unique[0] if len(unique) == 1 else None

        def unique(values: Sequence[str | None]) -> str | None:
            normalized = sorted({value for value in values if value is not None})
            return normalized[0] if len(normalized) == 1 else None

        strategy_payloads: dict[str, Mapping[str, Any]] = {}
        trials_by_strategy: dict[str, list[sqlite3.Row]] = {}
        trials_by_id: dict[str, sqlite3.Row] = {}
        for trial_row in self._conn.execute(
            "SELECT research_trial_id,strategy_version_id,payload_json FROM research_trials"
        ).fetchall():
            trial_id = text(trial_row["research_trial_id"])
            strategy_id = text(trial_row["strategy_version_id"])
            if trial_id is None or strategy_id is None:
                continue
            trials_by_strategy.setdefault(strategy_id, []).append(trial_row)
            trials_by_id[trial_id] = trial_row

        for row in member_rows:
            selection_id = text(row["portfolio_selection_id"])
            strategy_id = text(row["strategy_version_id"])
            if selection_id is None or strategy_id is None:
                continue
            if strategy_id not in strategy_payloads:
                strategy_row = self._conn.execute(
                    "SELECT payload_json FROM strategy_versions WHERE strategy_version_id=?",
                    (strategy_id,),
                ).fetchone()
                strategy_payloads[strategy_id] = (
                    payload(strategy_row["payload_json"]) if strategy_row is not None else {}
                )
            member_payload = payload(row["payload_json"])
            explicit_trial_values = [
                value
                for value in (
                    text(row["research_trial_id"]),
                    payload_text(member_payload, "research_trial_id", "trial_id"),
                )
                if value is not None
            ]
            trial_id = unique(explicit_trial_values)
            explicit_trial_conflict = len(set(explicit_trial_values)) > 1
            strategy_trials = trials_by_strategy.get(strategy_id, [])
            trial_ids = sorted(
                {
                    text(trial_row["research_trial_id"])
                    for trial_row in strategy_trials
                    if text(trial_row["research_trial_id"]) is not None
                }
            )
            if trial_id is None and not explicit_trial_conflict and len(trial_ids) == 1:
                trial_id = trial_ids[0]
            trial_row = trials_by_id.get(trial_id) if trial_id is not None else None
            trial_is_exact = trial_row is not None and (
                text(trial_row["strategy_version_id"]) == strategy_id
            )
            explicit_candidate_values = [
                value
                for value in (
                    text(row["candidate_id"]),
                    payload_text(
                        member_payload,
                        "candidate_id",
                        "strategy_candidate_id",
                    ),
                )
                if value is not None
            ]
            candidate_id = unique(explicit_candidate_values)
            if candidate_id is None and not explicit_candidate_values:
                inferred_candidates = [
                    payload_text(
                        strategy_payloads[strategy_id],
                        "candidate_id",
                        "strategy_candidate_id",
                    ),
                ]
                if trial_row is not None:
                    inferred_candidates.append(
                        payload_text(
                            payload(trial_row["payload_json"]),
                            "candidate_id",
                            "strategy_candidate_id",
                        )
                    )
                candidate_id = unique(inferred_candidates)

            try:
                allocation = _risk_decimal(row["allocation"], nonnegative=True)
            except (InvalidOperation, TypeError, ValueError):
                allocation = Decimal("0")
            status = str(row["status"] or "").strip().upper()
            funded = allocation > 0 and status in {
                "ACTIVE",
                "RETAINED",
                "REDUCE",
                "PAPER",
            }
            executable = (
                trial_id is not None
                and trial_is_exact
                and candidate_id is not None
            )
            next_status = status
            next_allocation = str(row["allocation"])
            if funded and not executable:
                next_status = "OBSERVE"
                next_allocation = "0"

            next_payload = dict(member_payload)
            if trial_id is not None:
                next_payload["research_trial_id"] = trial_id
            if candidate_id is not None:
                next_payload["candidate_id"] = candidate_id
            if next_status != status:
                next_payload["status"] = next_status
                next_payload["allocation"] = next_allocation
            payload_json = _rolling_dump(next_payload)
            if (
                text(row["research_trial_id"]) != trial_id
                or text(row["candidate_id"]) != candidate_id
                or str(row["status"] or "") != next_status
                or str(row["allocation"] or "") != next_allocation
                or str(row["payload_json"] or "") != payload_json
            ):
                self._conn.execute(
                    "UPDATE portfolio_selection_members SET research_trial_id=?,"
                    "candidate_id=?,allocation=?,status=?,payload_json=? "
                    "WHERE portfolio_selection_id=? AND strategy_version_id=?",
                    (
                        trial_id,
                        candidate_id,
                        next_allocation,
                        next_status,
                        payload_json,
                        selection_id,
                        strategy_id,
                    ),
                )

    def _initialize_canary_risk_schema(self) -> None:
        """Create versioned canary settings and append-only risk accounting.

        The tables are independent of the legacy ``canary_*`` tables so an
        older checkout can be upgraded without rewriting or dropping any live
        ledger evidence.  Missing columns on a partially-created development
        database are added conservatively.
        """
        preflight_columns = {
            "canary_risk_reservations": {
                # Every column used by the indexes and risk methods must exist
                # before the CREATE INDEX statements below.  ALTER TABLE needs
                # defaults for NOT NULL additions so existing partial rows stay
                # readable and are never discarded.
                "event_id": "TEXT",
                "requested_cost": "TEXT NOT NULL DEFAULT '0'",
                "filled_cost": "TEXT NOT NULL DEFAULT '0'",
                "remaining_cost": "TEXT NOT NULL DEFAULT '0'",
                "fee_reserve": "TEXT NOT NULL DEFAULT '0'",
                "quantity": "TEXT NOT NULL DEFAULT '0'",
                "filled_quantity": "TEXT NOT NULL DEFAULT '0'",
                "status": "TEXT NOT NULL DEFAULT 'HELD'",
                "config_generation": "INTEGER",
                "config_hash": "TEXT",
                "config_id": "TEXT",
                "control_generation": "INTEGER",
                "candidate_id": "TEXT",
                "strategy_version_id": "TEXT",
                "research_trial_id": "TEXT",
                "portfolio_selection_id": "TEXT",
                "admission_policy_id": "TEXT",
                "admission_policy_version": "TEXT",
                "risk_config_id": "TEXT",
                "risk_config_generation": "INTEGER",
                "risk_config_hash": "TEXT",
                "allocation": "TEXT",
                "detail_json": "TEXT NOT NULL DEFAULT '{}'",
                "created_at": "TEXT NOT NULL DEFAULT ''",
                "submitted_at": "TEXT",
                "updated_at": "TEXT NOT NULL DEFAULT ''",
                "released_at": "TEXT",
            },
            "canary_submission_attempts": {
                "intent_id": "TEXT NOT NULL DEFAULT ''",
                "side": "TEXT NOT NULL DEFAULT 'BUY'",
                "attempted_at": "TEXT NOT NULL DEFAULT ''",
                "status": "TEXT NOT NULL DEFAULT 'ATTEMPTED'",
                "config_generation": "INTEGER",
                "config_id": "TEXT",
                "control_generation": "INTEGER",
                "candidate_id": "TEXT",
                "strategy_version_id": "TEXT",
                "research_trial_id": "TEXT",
                "portfolio_selection_id": "TEXT",
                "admission_policy_id": "TEXT",
                "admission_policy_version": "TEXT",
                "risk_config_id": "TEXT",
                "risk_config_generation": "INTEGER",
                "risk_config_hash": "TEXT",
                "allocation": "TEXT",
                "detail_json": "TEXT NOT NULL DEFAULT '{}'",
            },
            "canary_risk_fills": {
                "quantity": "TEXT NOT NULL DEFAULT '0'",
                "price": "TEXT NOT NULL DEFAULT '0'",
                "cost": "TEXT NOT NULL DEFAULT '0'",
                "fee": "TEXT NOT NULL DEFAULT '0'",
                "filled_at": "TEXT NOT NULL DEFAULT ''",
                "candidate_id": "TEXT",
                "strategy_version_id": "TEXT",
                "research_trial_id": "TEXT",
                "portfolio_selection_id": "TEXT",
                "admission_policy_id": "TEXT",
                "admission_policy_version": "TEXT",
                "risk_config_id": "TEXT",
                "risk_config_generation": "INTEGER",
                "risk_config_hash": "TEXT",
                "allocation": "TEXT",
                "detail_json": "TEXT NOT NULL DEFAULT '{}'",
            },
            "canary_equity_marks": {
                "mark_id": "TEXT",
                "market_id": "TEXT NOT NULL DEFAULT ''",
                "token_id": "TEXT NOT NULL DEFAULT ''",
                "side": "TEXT NOT NULL DEFAULT 'SELL'",
                "quantity": "TEXT NOT NULL DEFAULT '0'",
                "mark_price": "TEXT NOT NULL DEFAULT '0'",
                "cost_basis_usd": "TEXT NOT NULL DEFAULT '0'",
                "mark_fee": "TEXT NOT NULL DEFAULT '0'",
                "observed_at": "TEXT NOT NULL DEFAULT ''",
                "source": "TEXT NOT NULL DEFAULT ''",
                "config_id": "TEXT",
                "config_generation": "INTEGER",
                "control_generation": "INTEGER",
                "candidate_id": "TEXT",
                "strategy_version_id": "TEXT",
                "research_trial_id": "TEXT",
                "portfolio_selection_id": "TEXT",
                "admission_policy_id": "TEXT",
                "admission_policy_version": "TEXT",
                "risk_config_id": "TEXT",
                "risk_config_generation": "INTEGER",
                "risk_config_hash": "TEXT",
                "allocation": "TEXT",
                "detail_json": "TEXT NOT NULL DEFAULT '{}'",
                "created_at": "TEXT NOT NULL DEFAULT ''",
            },
            "canary_risk_cashflows": {
                "kind": "TEXT NOT NULL DEFAULT 'EXTERNAL'",
                "amount": "TEXT NOT NULL DEFAULT '0'",
                "occurred_at": "TEXT NOT NULL DEFAULT ''",
                "candidate_id": "TEXT",
                "strategy_version_id": "TEXT",
                "research_trial_id": "TEXT",
                "portfolio_selection_id": "TEXT",
                "admission_policy_id": "TEXT",
                "admission_policy_version": "TEXT",
                "risk_config_id": "TEXT",
                "risk_config_generation": "INTEGER",
                "risk_config_hash": "TEXT",
                "allocation": "TEXT",
                "detail_json": "TEXT NOT NULL DEFAULT '{}'",
            },
        }
        for table, columns in preflight_columns.items():
            exists = self._conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                (table,),
            ).fetchone()
            if exists is None:
                continue
            existing = {
                str(row["name"])
                for row in self._conn.execute(f"PRAGMA table_info({table})").fetchall()
            }
            for name, definition in columns.items():
                if name not in existing:
                    self._conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {definition}")
        self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS canary_setting_configs (
                config_id TEXT PRIMARY KEY,
                state TEXT NOT NULL CHECK(state IN ('DRAFT','ACTIVE','ARCHIVED')),
                generation INTEGER NOT NULL,
                config_hash TEXT NOT NULL,
                values_json TEXT NOT NULL,
                actor TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                activated_at TEXT,
                previous_config_id TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_canary_setting_configs_hash
                ON canary_setting_configs(config_hash);
            CREATE UNIQUE INDEX IF NOT EXISTS idx_canary_setting_configs_active
                ON canary_setting_configs(state) WHERE state='ACTIVE';
            CREATE INDEX IF NOT EXISTS idx_canary_setting_configs_state_time
                ON canary_setting_configs(state, created_at DESC, config_id DESC);
            CREATE TABLE IF NOT EXISTS canary_setting_audit (
                audit_id TEXT PRIMARY KEY,
                config_id TEXT NOT NULL,
                action TEXT NOT NULL,
                actor TEXT NOT NULL,
                previous_config_id TEXT,
                previous_config_hash TEXT,
                new_config_id TEXT NOT NULL,
                new_config_hash TEXT NOT NULL,
                previous_generation INTEGER NOT NULL,
                new_generation INTEGER NOT NULL,
                timestamp TEXT NOT NULL,
                detail_json TEXT NOT NULL DEFAULT '{}'
            );
            CREATE INDEX IF NOT EXISTS idx_canary_setting_audit_time
                ON canary_setting_audit(timestamp DESC, audit_id DESC);
            CREATE TABLE IF NOT EXISTS canary_risk_reservations (
                reservation_id TEXT PRIMARY KEY,
                intent_id TEXT NOT NULL UNIQUE,
                side TEXT NOT NULL,
                market_id TEXT,
                event_id TEXT,
                requested_cost TEXT NOT NULL DEFAULT '0',
                filled_cost TEXT NOT NULL DEFAULT '0',
                remaining_cost TEXT NOT NULL DEFAULT '0',
                fee_reserve TEXT NOT NULL DEFAULT '0',
                quantity TEXT NOT NULL DEFAULT '0',
                filled_quantity TEXT NOT NULL DEFAULT '0',
                status TEXT NOT NULL,
                config_generation INTEGER,
                config_hash TEXT,
                config_id TEXT,
                control_generation INTEGER,
                candidate_id TEXT,
                strategy_version_id TEXT,
                research_trial_id TEXT,
                portfolio_selection_id TEXT,
                admission_policy_id TEXT,
                admission_policy_version TEXT,
                risk_config_id TEXT,
                risk_config_generation INTEGER,
                risk_config_hash TEXT,
                allocation TEXT,
                detail_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                submitted_at TEXT,
                updated_at TEXT NOT NULL,
                released_at TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_canary_risk_reservations_status
                ON canary_risk_reservations(status, side, updated_at);
            CREATE INDEX IF NOT EXISTS idx_canary_risk_reservations_market
                ON canary_risk_reservations(market_id, event_id, status);
            CREATE TABLE IF NOT EXISTS canary_submission_attempts (
                attempt_id TEXT PRIMARY KEY,
                intent_id TEXT NOT NULL,
                side TEXT NOT NULL,
                attempted_at TEXT NOT NULL,
                status TEXT NOT NULL,
                config_generation INTEGER,
                config_hash TEXT,
                config_id TEXT,
                control_generation INTEGER,
                candidate_id TEXT,
                strategy_version_id TEXT,
                research_trial_id TEXT,
                portfolio_selection_id TEXT,
                admission_policy_id TEXT,
                admission_policy_version TEXT,
                risk_config_id TEXT,
                risk_config_generation INTEGER,
                risk_config_hash TEXT,
                allocation TEXT,
                detail_json TEXT NOT NULL DEFAULT '{}'
            );
            CREATE INDEX IF NOT EXISTS idx_canary_submission_attempts_time
                ON canary_submission_attempts(attempted_at, attempt_id);
            CREATE TABLE IF NOT EXISTS canary_risk_fills (
                fill_id TEXT PRIMARY KEY,
                reservation_id TEXT NOT NULL,
                quantity TEXT NOT NULL,
                price TEXT NOT NULL,
                cost TEXT NOT NULL,
                fee TEXT NOT NULL DEFAULT '0',
                filled_at TEXT NOT NULL,
                candidate_id TEXT,
                strategy_version_id TEXT,
                research_trial_id TEXT,
                portfolio_selection_id TEXT,
                admission_policy_id TEXT,
                admission_policy_version TEXT,
                risk_config_id TEXT,
                risk_config_generation INTEGER,
                risk_config_hash TEXT,
                allocation TEXT,
                detail_json TEXT NOT NULL DEFAULT '{}'
            );
            CREATE UNIQUE INDEX IF NOT EXISTS idx_canary_risk_fills_reservation_fill
                ON canary_risk_fills(reservation_id, fill_id);
            CREATE INDEX IF NOT EXISTS idx_canary_risk_fills_time
                ON canary_risk_fills(filled_at, reservation_id);
            CREATE TABLE IF NOT EXISTS canary_equity_marks (
                mark_id TEXT PRIMARY KEY,
                market_id TEXT NOT NULL,
                token_id TEXT NOT NULL,
                side TEXT NOT NULL,
                quantity TEXT NOT NULL,
                mark_price TEXT NOT NULL,
                cost_basis_usd TEXT NOT NULL,
                mark_fee TEXT NOT NULL DEFAULT '0',
                observed_at TEXT NOT NULL,
                source TEXT NOT NULL,
                config_generation INTEGER,
                control_generation INTEGER,
                candidate_id TEXT,
                strategy_version_id TEXT,
                research_trial_id TEXT,
                portfolio_selection_id TEXT,
                admission_policy_id TEXT,
                admission_policy_version TEXT,
                risk_config_id TEXT,
                risk_config_generation INTEGER,
                risk_config_hash TEXT,
                allocation TEXT,
                detail_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS canary_risk_cashflows (
                flow_id TEXT PRIMARY KEY,
                kind TEXT NOT NULL,
                amount TEXT NOT NULL,
                occurred_at TEXT NOT NULL,
                candidate_id TEXT,
                strategy_version_id TEXT,
                research_trial_id TEXT,
                portfolio_selection_id TEXT,
                admission_policy_id TEXT,
                admission_policy_version TEXT,
                risk_config_id TEXT,
                risk_config_generation INTEGER,
                risk_config_hash TEXT,
                allocation TEXT,
                detail_json TEXT NOT NULL DEFAULT '{}'
            );
            CREATE INDEX IF NOT EXISTS idx_canary_risk_cashflows_time
                ON canary_risk_cashflows(occurred_at, kind, flow_id);
            """
        )
        # A handful of early local prototypes created tables without
        # activation or lineage columns.  Add only absent columns; never
        # rewrite rows.  ``preflight_columns`` is also applied here so fresh
        # tables created by the script and partially-created tables converge
        # to the same additive shape.
        migration_columns = dict(preflight_columns)
        migration_columns["canary_setting_configs"] = {
            "activated_at": "TEXT",
            "previous_config_id": "TEXT",
        }
        for table, columns in migration_columns.items():
            existing = {
                str(row["name"])
                for row in self._conn.execute(f"PRAGMA table_info({table})").fetchall()
            }
            for name, definition in columns.items():
                if name not in existing:
                    self._conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {definition}")
        self._conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_canary_equity_marks_id "
            "ON canary_equity_marks(mark_id)"
        )
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_canary_equity_marks_identity "
            "ON canary_equity_marks(market_id, token_id, side, observed_at DESC, mark_id DESC)"
        )
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_canary_equity_marks_observed "
            "ON canary_equity_marks(observed_at, mark_id)"
        )
        self._conn.executescript(
            """
            CREATE INDEX IF NOT EXISTS idx_canary_risk_reservations_lineage
                ON canary_risk_reservations(
                    portfolio_selection_id, strategy_version_id, status, updated_at
                );
            CREATE INDEX IF NOT EXISTS idx_canary_risk_reservations_strategy
                ON canary_risk_reservations(strategy_version_id, side, status, updated_at);
            CREATE INDEX IF NOT EXISTS idx_canary_submission_attempts_lineage
                ON canary_submission_attempts(
                    portfolio_selection_id, strategy_version_id, attempted_at, attempt_id
                );
            CREATE INDEX IF NOT EXISTS idx_canary_risk_fills_lineage
                ON canary_risk_fills(
                    portfolio_selection_id, strategy_version_id, filled_at, fill_id
                );
            CREATE INDEX IF NOT EXISTS idx_canary_equity_marks_lineage
                ON canary_equity_marks(
                    portfolio_selection_id, strategy_version_id, observed_at DESC, mark_id DESC
                );
            CREATE INDEX IF NOT EXISTS idx_canary_risk_cashflows_lineage
                ON canary_risk_cashflows(
                    portfolio_selection_id, strategy_version_id, occurred_at, flow_id
                );
            """
        )

    def save_canary_setting_config(
        self,
        *,
        config_id: str,
        state: str,
        generation: int,
        config_hash: str,
        values: Mapping[str, Any],
        actor: str,
        timestamp: datetime | None = None,
        activated_at: datetime | None = None,
        previous_config_id: str | None = None,
    ) -> dict[str, Any]:
        """Persist one immutable settings version and return its projection."""
        identifier = str(config_id or "").strip()
        state_value = str(state or "").strip().upper()
        digest = str(config_hash or "").strip()
        owner = str(actor or "").strip()
        if not identifier or state_value not in {"DRAFT", "ACTIVE", "ARCHIVED"} or not digest or not owner:
            raise ValueError("config_id, state, config_hash, and actor are required")
        if isinstance(generation, bool) or not isinstance(generation, int) or generation < 1:
            raise ValueError("settings generation must be a positive integer")
        encoded = _dump(dict(values))
        stamp = timestamp or utc_now()
        activated = _iso(activated_at) if activated_at is not None else None
        with self._write_context():
            try:
                self._conn.execute(
                    "INSERT INTO canary_setting_configs("
                    "config_id,state,generation,config_hash,values_json,actor,created_at,updated_at,activated_at,previous_config_id"
                    ") VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (
                        identifier,
                        state_value,
                        int(generation),
                        digest,
                        encoded,
                        owner,
                        _iso(stamp),
                        _iso(stamp),
                        activated,
                        str(previous_config_id) if previous_config_id else None,
                    ),
                )
            except sqlite3.IntegrityError as exc:
                existing = self._conn.execute(
                    "SELECT config_hash,values_json FROM canary_setting_configs WHERE config_id=?",
                    (identifier,),
                ).fetchone()
                if existing is None or str(existing["config_hash"]) != digest or str(existing["values_json"]) != encoded:
                    raise ValueError(f"settings config already exists: {identifier}") from exc
        return self.load_canary_setting_config(config_id=identifier) or {}

    def load_canary_setting_config(
        self,
        config_id: str | None = None,
        *,
        state: str | None = None,
        status: str | None = None,
    ) -> dict[str, Any] | None:
        """Load one settings config, or the newest config in a state."""
        state_value = str(state if state is not None else status or "").strip().upper()
        query = "SELECT * FROM canary_setting_configs"
        values: list[Any] = []
        clauses: list[str] = []
        if config_id is not None:
            clauses.append("config_id=?")
            values.append(str(config_id).strip())
        if state_value:
            if state_value not in {"DRAFT", "ACTIVE", "ARCHIVED"}:
                raise ValueError("invalid settings config state")
            clauses.append("state=?")
            values.append(state_value)
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY created_at DESC,rowid DESC,config_id DESC LIMIT 1"
        with self._lock:
            row = self._conn.execute(query, values).fetchone()
        if row is None:
            return None
        return {
            "config_id": row["config_id"],
            "state": row["state"],
            "status": row["state"],
            "generation": int(row["generation"]),
            "config_hash": row["config_hash"],
            "values": _load(row["values_json"]),
            "settings": _load(row["values_json"]),
            "actor": row["actor"],
            "created_at": _parse_datetime(row["created_at"]),
            "updated_at": _parse_datetime(row["updated_at"]),
            "activated_at": _parse_datetime(row["activated_at"]),
            "previous_config_id": row["previous_config_id"],
        }

    def list_canary_setting_configs(self, *, state: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 0:
            raise ValueError("limit must be a non-negative integer")
        state_value = str(state or "").strip().upper()
        if state_value and state_value not in {"DRAFT", "ACTIVE", "ARCHIVED"}:
            raise ValueError("invalid settings config state")
        query = "SELECT * FROM canary_setting_configs"
        values: list[Any] = []
        if state_value:
            query += " WHERE state=?"
            values.append(state_value)
        query += " ORDER BY created_at DESC,rowid DESC,config_id DESC LIMIT ?"
        values.append(int(limit))
        with self._lock:
            rows = self._conn.execute(query, values).fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            result.append({
                "config_id": row["config_id"],
                "state": row["state"],
                "status": row["state"],
                "generation": int(row["generation"]),
                "config_hash": row["config_hash"],
                "values": _load(row["values_json"]),
                "settings": _load(row["values_json"]),
                "actor": row["actor"],
                "created_at": _parse_datetime(row["created_at"]),
                "updated_at": _parse_datetime(row["updated_at"]),
                "activated_at": _parse_datetime(row["activated_at"]),
                "previous_config_id": row["previous_config_id"],
            })
        return result

    def activate_canary_setting_config(
        self,
        *,
        config_id: str,
        actor: str,
        expected_generation: int,
        timestamp: datetime | None = None,
    ) -> dict[str, Any]:
        """Atomically activate a draft with a generation compare-and-swap."""
        identifier = str(config_id or "").strip()
        owner = str(actor or "").strip()
        if not identifier or not owner:
            raise ValueError("config_id and actor are required")
        if isinstance(expected_generation, bool) or not isinstance(expected_generation, int) or expected_generation < 1:
            raise ValueError("expected_generation must be a positive integer")
        stamp = timestamp or utc_now()
        with self.transaction(immediate=True):
            current = self._conn.execute(
                "SELECT * FROM canary_setting_configs WHERE state='ACTIVE' ORDER BY generation DESC LIMIT 1"
            ).fetchone()
            if current is None:
                raise ValueError("active canary settings are unavailable")
            current_generation = int(current["generation"])
            if current_generation != int(expected_generation):
                raise ValueError("settings generation changed")
            draft = self._conn.execute(
                "SELECT * FROM canary_setting_configs WHERE config_id=? AND state='DRAFT'",
                (identifier,),
            ).fetchone()
            if draft is None:
                raise ValueError("settings config is not a draft")
            next_generation = current_generation + 1
            self._conn.execute(
                "UPDATE canary_setting_configs SET state='ARCHIVED',updated_at=? WHERE state='ACTIVE'",
                (_iso(stamp),),
            )
            self._conn.execute(
                "UPDATE canary_setting_configs SET state='ACTIVE',generation=?,updated_at=?,"
                "activated_at=?,previous_config_id=? WHERE config_id=? AND state='DRAFT'",
                (next_generation, _iso(stamp), _iso(stamp), current["config_id"], identifier),
            )
            self._conn.execute(
                "INSERT INTO canary_setting_audit("
                "audit_id,config_id,action,actor,previous_config_id,previous_config_hash,"
                "new_config_id,new_config_hash,previous_generation,new_generation,timestamp,detail_json"
                ") VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    "audit:" + hashlib.sha256(
                        _dump({"config_id": identifier, "generation": next_generation, "timestamp": _iso(stamp)}).encode()
                    ).hexdigest(),
                    identifier,
                    "ACTIVATED",
                    owner,
                    current["config_id"],
                    current["config_hash"],
                    identifier,
                    draft["config_hash"],
                    current_generation,
                    next_generation,
                    _iso(stamp),
                    _dump({"activation": True}),
                ),
            )
        return self.load_canary_setting_config(config_id=identifier) or {}

    def record_canary_setting_audit(
        self,
        *,
        config_id: str,
        action: str,
        actor: str,
        previous_config_id: str | None,
        previous_config_hash: str | None,
        new_config_id: str,
        new_config_hash: str,
        previous_generation: int,
        new_generation: int,
        timestamp: datetime | None = None,
        detail: Mapping[str, Any] | None = None,
    ) -> str:
        stamp = timestamp or utc_now()
        body = {
            "config_id": str(config_id),
            "action": str(action),
            "actor": str(actor),
            "previous_config_id": previous_config_id,
            "previous_config_hash": previous_config_hash,
            "new_config_id": str(new_config_id),
            "new_config_hash": str(new_config_hash),
            "previous_generation": int(previous_generation),
            "new_generation": int(new_generation),
            "timestamp": _iso(stamp),
            "detail": dict(detail or {}),
        }
        audit_id = "audit:" + hashlib.sha256(_dump(body).encode()).hexdigest()
        with self._write_context():
            self._conn.execute(
                "INSERT OR IGNORE INTO canary_setting_audit("
                "audit_id,config_id,action,actor,previous_config_id,previous_config_hash,"
                "new_config_id,new_config_hash,previous_generation,new_generation,timestamp,detail_json"
                ") VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    audit_id,
                    str(config_id),
                    str(action).strip().upper(),
                    str(actor),
                    previous_config_id,
                    previous_config_hash,
                    str(new_config_id),
                    str(new_config_hash),
                    int(previous_generation),
                    int(new_generation),
                    _iso(stamp),
                    _dump(detail or {}),
                ),
            )
        return audit_id

    def list_canary_setting_audit(self, *, limit: int = 100) -> list[dict[str, Any]]:
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 0:
            raise ValueError("limit must be a non-negative integer")
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM canary_setting_audit ORDER BY timestamp DESC,rowid DESC,audit_id DESC LIMIT ?",
                (int(limit),),
            ).fetchall()
        return [
            {
                "audit_id": row["audit_id"],
                "config_id": row["config_id"],
                "action": row["action"],
                "actor": row["actor"],
                "previous_config_id": row["previous_config_id"],
                "previous_config_hash": row["previous_config_hash"],
                "new_config_id": row["new_config_id"],
                "new_config_hash": row["new_config_hash"],
                "previous_generation": row["previous_generation"],
                "new_generation": row["new_generation"],
                "timestamp": _parse_datetime(row["timestamp"]),
                "detail": _load(row["detail_json"]) if row["detail_json"] else {},
            }
            for row in rows
        ]
    def _canary_authority_locked(self) -> dict[str, Any]:
        active = self._conn.execute(
            "SELECT config_id,generation,config_hash,values_json "
            "FROM canary_setting_configs WHERE state='ACTIVE' "
            "ORDER BY generation DESC,created_at DESC,config_id DESC LIMIT 1"
        ).fetchone()
        authority: dict[str, Any] = {
            "config_id": None,
            "generation": None,
            "config_hash": None,
            "limits": {},
            "control_generation": None,
            "control_state": None,
        }
        if active is not None:
            authority.update(
                config_id=str(active["config_id"]),
                generation=int(active["generation"]),
                config_hash=str(active["config_hash"]),
                limits=_load(active["values_json"]),
            )
        control_table = self._conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='canary_control'"
        ).fetchone()
        if control_table is not None:
            control = self._conn.execute(
                "SELECT state,control_generation,settings_config_id,settings_generation "
                "FROM canary_control WHERE singleton=1"
            ).fetchone()
            if control is not None:
                authority["control_state"] = str(control["state"] or "").upper()
                authority["control_generation"] = int(control["control_generation"] or 0)
                control_config = str(control["settings_config_id"] or "").strip()
                control_generation = control["settings_generation"]
                if authority["control_generation"] is not None and authority["control_generation"] <= 0:
                    raise ValueError("canary control generation is invalid")
                if control_config and authority["config_id"] and control_config != authority["config_id"]:
                    raise ValueError("canary control settings config is stale")
                if control_generation is not None and authority["generation"] is not None:
                    if int(control_generation) != int(authority["generation"]):
                        raise ValueError("canary control settings generation is stale")
        return authority

    @staticmethod
    def _canary_effective_limits(
        active: Mapping[str, Any],
        candidate: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        effective: dict[str, Any] = {}
        for raw_name, value in active.items():
            name = _CANARY_LIMIT_ALIASES.get(str(raw_name), str(raw_name))
            effective[name] = value
        if candidate is not None:
            for raw_name, value in candidate.items():
                name = _CANARY_LIMIT_ALIASES.get(str(raw_name), str(raw_name))
                if name not in effective or value in (None, ""):
                    continue
                if name in _CANARY_DECIMAL_LIMITS:
                    proposed = _risk_decimal(value, name=name, nonnegative=True)
                    current_raw = effective.get(name)
                    if current_raw in (None, ""):
                        effective[name] = _risk_text(proposed)
                    else:
                        current = _risk_decimal(current_raw, name=name, nonnegative=True)
                        effective[name] = _risk_text(min(current, proposed))
                elif name in {"max_positions", "max_submitted_orders_per_day"}:
                    if isinstance(value, bool):
                        raise ValueError(f"{name} must be an integer")
                    current = int(effective[name])
                    proposed = int(value)
                    if proposed < 0:
                        raise ValueError(f"{name} must be non-negative")
                    effective[name] = min(current, proposed)
        return effective

    @staticmethod
    def _canary_window(stamp: datetime) -> tuple[str, str]:
        observed = ensure_utc(stamp)
        local = observed.astimezone(ZoneInfo("Asia/Manila"))
        start_local = local.replace(hour=0, minute=0, second=0, microsecond=0)
        return (
            start_local.astimezone(UTC).isoformat(),
            (start_local + timedelta(days=1)).astimezone(UTC).isoformat(),
        )
    def _rolling_open_buy_commitment_locked(self, row: sqlite3.Row) -> Decimal:
        """Return one rolling BUY's still-open capital from canonical lot state."""
        if str(row["side"] or "").strip().upper() != "BUY":
            return Decimal("0")
        lineage = _canary_lineage_from_row(row)
        if not lineage.get("portfolio_selection_id") or not lineage.get("strategy_version_id"):
            return Decimal("0")
        status = str(row["status"] or "").strip().upper()
        filled = _risk_decimal(row["filled_cost"], name="filled cost", nonnegative=True)
        remaining = _risk_decimal(row["remaining_cost"], name="remaining cost", nonnegative=True)
        expected_quantity = _risk_decimal(row["quantity"], name="quantity", nonnegative=True)
        filled_quantity = _risk_decimal(
            row["filled_quantity"], name="filled quantity", nonnegative=True
        )
        active = status in {
            "HELD",
            "RESERVED",
            "SUBMITTING",
            "SUBMITTED",
            "ACKNOWLEDGED",
            "UNKNOWN",
            "PARTIAL",
            "PARTIALLY_FILLED",
            "OPEN",
        }
        if active or (
            status == "FILLED"
            and expected_quantity > 0
            and filled_quantity < expected_quantity
        ):
            return filled + remaining
        if status not in {"FILLED", "RELEASED", "SETTLED"}:
            return Decimal("0")
        try:
            lot_rows = self._conn.execute(
                "SELECT quantity,sold_quantity,cost_basis,fees,status,candidate_id,"
                "strategy_version_id,research_trial_id,portfolio_selection_id "
                "FROM canary_position_lots WHERE reservation_id=?",
                (str(row["reservation_id"]),),
            ).fetchall()
        except sqlite3.OperationalError:
            try:
                lot_rows = self._conn.execute(
                    "SELECT quantity,sold_quantity,cost_basis,status,candidate_id,"
                    "strategy_version_id,research_trial_id,portfolio_selection_id "
                    "FROM canary_position_lots WHERE reservation_id=?",
                    (str(row["reservation_id"]),),
                ).fetchall()
            except sqlite3.OperationalError:
                lot_rows = []
        lot_commitment = Decimal("0")
        saw_matching_lot = False
        for lot in lot_rows:
            if any(
                str(lot[name] or "").strip()
                and str(lot[name]).strip() != str(lineage.get(name) or "").strip()
                for name in (
                    "candidate_id",
                    "strategy_version_id",
                    "research_trial_id",
                    "portfolio_selection_id",
                )
                if name in lot.keys()
            ):
                continue
            lot_status = str(lot["status"] or "").strip().upper()
            if lot_status in {"CLOSED", "DUST", "SETTLED", "RESOLVED"}:
                saw_matching_lot = True
                continue
            quantity = _risk_decimal(lot["quantity"], name="lot quantity", nonnegative=True)
            basis = _risk_decimal(lot["cost_basis"], name="lot cost basis", nonnegative=True)
            sold = _risk_decimal(lot["sold_quantity"], name="lot sold quantity", nonnegative=True)
            if quantity <= 0 or sold >= quantity:
                saw_matching_lot = True
                continue
            saw_matching_lot = True
            lot_commitment += basis * (quantity - sold) / quantity
        if saw_matching_lot:
            return max(Decimal("0"), lot_commitment)
        return filled if status in {"FILLED", "RELEASED"} else Decimal("0")

    def _canary_rolling_binding_locked(
        self,
        *,
        lineage: Mapping[str, Any],
        allow_exit: bool = False,
    ) -> dict[str, Any] | None:
        """Validate an exact rolling selection/member binding.

        New BUY reservations must point at the current selection and a funded
        ACTIVE member.  SELL exits may use an historical selection/member
        lineage after rotation; exact lot ownership is authorized separately
        by the persisted canary position request.
        """
        if not _canary_lineage_is_rolling(lineage):
            return None
        required = (
            "strategy_version_id",
            "research_trial_id",
            "portfolio_selection_id",
            "admission_policy_id",
            "admission_policy_version",
            "risk_config_id",
            "risk_config_generation",
            "risk_config_hash",
        )
        if any(lineage.get(name) in (None, "") for name in required):
            raise ValueError("rolling reservation requires complete lineage")
        if not allow_exit and lineage.get("allocation") in (None, ""):
            raise ValueError("rolling BUY requires allocation")
        selection_id = str(lineage["portfolio_selection_id"]).strip()
        strategy_id = str(lineage["strategy_version_id"]).strip()
        policy_id = str(lineage["admission_policy_id"]).strip()
        policy_version = str(lineage["admission_policy_version"]).strip()
        risk_id = str(lineage["risk_config_id"]).strip()
        risk_hash = str(lineage["risk_config_hash"]).strip()
        risk_generation = _canary_optional_lineage_generation(
            lineage["risk_config_generation"],
            name="risk_config_generation",
        )
        allocation = _risk_decimal(
            lineage.get("allocation") or "0",
            name="allocation",
            nonnegative=True,
        )
        if not selection_id or not strategy_id or not policy_id or not policy_version:
            raise ValueError("rolling reservation lineage identifiers are required")
        if not risk_id or risk_generation is None or not risk_hash:
            raise ValueError("rolling reservation requires funded lineage")
        pointer = self._conn.execute(
            "SELECT portfolio_selection_id FROM portfolio_current_selection "
            "WHERE pointer_id='current'"
        ).fetchone()
        if (
            not allow_exit
            and (pointer is None or str(pointer["portfolio_selection_id"]) != selection_id)
        ):
            raise ValueError("rolling reservation selection is not current")
        selection = self._conn.execute(
            "SELECT * FROM portfolio_selections WHERE portfolio_selection_id=?",
            (selection_id,),
        ).fetchone()
        if selection is None:
            raise ValueError("rolling reservation selection does not exist")
        selection_payload = _load(selection["payload_json"]) if selection["payload_json"] else {}
        selection_payload = selection_payload if isinstance(selection_payload, Mapping) else {}
        selection_identity = dict(selection)
        selection_identity["payload"] = selection_payload
        for aliases, label in (
            (("policy_id", "admission_policy_id"), "policy"),
            (("policy_version", "version"), "policy version"),
            (
                (
                    "active_risk_config_id",
                    "risk_config_id",
                    "config_id",
                ),
                "risk config",
            ),
            (
                (
                    "active_risk_config_generation",
                    "risk_config_generation",
                    "risk_generation",
                    "generation",
                ),
                "risk generation",
            ),
            (
                ("active_risk_config_hash", "risk_config_hash", "config_hash"),
                "risk config hash",
            ),
        ):
            if _rolling_identity_conflict(selection_identity, *aliases):
                raise ValueError(f"rolling reservation {label} identity conflicts")
        if (
            str(selection["policy_id"]) != policy_id
            or str(selection["policy_version"]) != policy_version
            or str(selection["risk_config_id"]) != risk_id
            or int(selection["risk_config_generation"]) != risk_generation
            or str(selection["risk_config_hash"]) != risk_hash
        ):
            raise ValueError("rolling reservation policy/risk binding is stale")
        if self._conn.execute(
            "SELECT 1 FROM admission_policies WHERE policy_id=? AND version=?",
            (policy_id, policy_version),
        ).fetchone() is None:
            raise ValueError("rolling reservation admission policy does not exist")
        trial = self._conn.execute(
            "SELECT strategy_version_id,payload_json FROM research_trials "
            "WHERE research_trial_id=?",
            (str(lineage["research_trial_id"]).strip(),),
        ).fetchone()
        if trial is None or str(trial["strategy_version_id"]) != strategy_id:
            raise ValueError("rolling reservation research trial binding is stale")
        trial_payload = _load(trial["payload_json"]) if trial["payload_json"] else {}
        trial_payload = trial_payload if isinstance(trial_payload, Mapping) else {}
        if _rolling_identity_conflict(
            trial_payload,
            "research_trial_id",
            "trial_id",
        ) or _rolling_identity_conflict(
            trial_payload,
            "candidate_id",
            "candidate",
            "strategy_candidate_id",
        ):
            raise ValueError("rolling reservation research trial provenance conflicts")
        trial_identity = _rolling_identity_value(
            trial_payload,
            "research_trial_id",
            "trial_id",
        )
        if trial_identity is not None and trial_identity != str(lineage["research_trial_id"]).strip():
            raise ValueError("rolling reservation research trial identity is stale")
        trial_candidate = _rolling_identity_value(
            trial_payload,
            "candidate_id",
            "candidate",
            "strategy_candidate_id",
        )
        if trial_candidate is None:
            raise ValueError("rolling reservation research trial candidate provenance is missing")
        member = self._conn.execute(
            "SELECT * FROM portfolio_selection_members "
            "WHERE portfolio_selection_id=? AND strategy_version_id=?",
            (selection_id, strategy_id),
        ).fetchone()
        if member is None:
            raise ValueError("rolling reservation strategy is not selected")
        member_payload = _load(member["payload_json"]) if member["payload_json"] else {}
        member_payload = member_payload if isinstance(member_payload, Mapping) else {}
        member_identity = dict(member)
        member_identity["payload"] = member_payload
        for aliases, label in (
            (("research_trial_id", "trial_id"), "research trial"),
            (("candidate_id", "candidate", "strategy_candidate_id"), "candidate"),
            (("evidence_window_id", "evidence_id", "window_id"), "evidence"),
            (("admission_policy_id", "policy_id"), "policy"),
            (("admission_policy_version", "policy_version", "version"), "policy version"),
            (
                ("risk_config_id", "active_risk_config_id", "config_id"),
                "risk config",
            ),
            (
                (
                    "risk_config_generation",
                    "active_risk_config_generation",
                    "risk_generation",
                    "generation",
                ),
                "risk generation",
            ),
            (("risk_config_hash", "active_risk_config_hash", "config_hash"), "risk config hash"),
        ):
            if _rolling_identity_conflict(member_identity, *aliases):
                raise ValueError(f"rolling reservation member {label} identity conflicts")
        member_trial_id = _rolling_identity_value(
            member_identity,
            "research_trial_id",
            "trial_id",
        )
        if member_trial_id != str(lineage["research_trial_id"]).strip():
            raise ValueError("rolling reservation member research trial binding is stale")
        member_candidate_id = _rolling_identity_value(
            member_identity,
            "candidate_id",
            "candidate",
            "strategy_candidate_id",
        )
        if not member_candidate_id or member_candidate_id != trial_candidate:
            raise ValueError("rolling reservation member candidate binding is stale")
        requested_candidate = str(lineage.get("candidate_id") or "").strip()
        if requested_candidate and requested_candidate != member_candidate_id:
            raise ValueError("rolling reservation candidate binding is stale")
        evidence_id = _rolling_identity_value(
            member_identity,
            "evidence_window_id",
            "evidence_id",
            "window_id",
        )
        if not evidence_id:
            raise ValueError("rolling reservation evidence binding is missing")
        evidence = self._conn.execute(
            "SELECT * FROM strategy_evidence_windows WHERE evidence_window_id=?",
            (evidence_id,),
        ).fetchone()
        if evidence is None or str(evidence["strategy_version_id"]) != strategy_id:
            raise ValueError("rolling reservation evidence binding is stale")
        evidence_payload = _load(evidence["payload_json"]) if evidence["payload_json"] else {}
        evidence_payload = evidence_payload if isinstance(evidence_payload, Mapping) else {}
        evidence_identity = dict(evidence)
        evidence_identity["payload"] = evidence_payload
        if _rolling_identity_conflict(
            evidence_identity,
            "research_trial_id",
            "trial_id",
        ) or _rolling_identity_conflict(
            evidence_identity,
            "candidate_id",
            "candidate",
            "strategy_candidate_id",
        ) or _rolling_identity_conflict(
            evidence_identity,
            "source_class",
            "source_type",
        ) or _rolling_identity_conflict(
            evidence_identity,
            "evidence_digest",
            "digest",
        ):
            raise ValueError("rolling reservation evidence provenance conflicts")
        evidence_trial = _rolling_identity_value(
            evidence_identity,
            "research_trial_id",
            "trial_id",
        )
        evidence_candidate = _rolling_identity_value(
            evidence_identity,
            "candidate_id",
            "candidate",
            "strategy_candidate_id",
        )
        source_class = str(evidence["source_class"] or "").strip().upper()
        if source_class not in _ROLLING_EVIDENCE_SOURCE_CLASSES:
            raise ValueError("rolling reservation evidence source_class is invalid")
        expected_digest = _rolling_evidence_digest(
            _rolling_evidence_mapping_from_row(evidence, evidence_payload)
        )
        if str(evidence["evidence_digest"] or "").strip() != expected_digest:
            raise ValueError("rolling reservation evidence digest is invalid")
        if evidence_trial != member_trial_id or evidence_candidate != member_candidate_id:
            raise ValueError("rolling reservation evidence binding is stale")
        member_status = str(member["status"] or "").strip().upper()
        member_allocation = _risk_decimal(
            member["allocation"],
            name="member allocation",
            nonnegative=True,
        )
        if (not allow_exit and member_status != "ACTIVE") or (
            not allow_exit and member_allocation <= 0
        ):
            raise ValueError("rolling reservation strategy is not ACTIVE")
        if not allow_exit and member_allocation != allocation:
            raise ValueError("rolling reservation allocation conflicts with selection")
        if allow_exit and allocation > 0 and member_allocation > 0 and member_allocation != allocation:
            raise ValueError("rolling exit allocation conflicts with opening member")
        return {
            "rolling": True,
            "selection": selection,
            "member": member,
            "candidate_id": member_candidate_id,
            "allocation": allocation,
            "selection_payload": (
                _load(selection["payload_json"]) if selection["payload_json"] else {}
            ),
        }
    @staticmethod
    def _canary_normalize_lineage(values: Mapping[str, Any] | None = None) -> dict[str, Any]:
        supplied = values or {}
        normalized: dict[str, Any] = {
            name: _canary_optional_lineage_text(supplied.get(name), name=name)
            for name in _CANARY_LINEAGE_FIELDS
            if name not in {"risk_config_generation", "allocation"}
        }
        normalized["risk_config_generation"] = _canary_optional_lineage_generation(
            supplied.get("risk_config_generation"),
            name="risk_config_generation",
        )
        allocation = supplied.get("allocation")
        normalized["allocation"] = (
            _risk_text(_risk_decimal(allocation, name="allocation", nonnegative=True))
            if allocation not in (None, "")
            else None
        )
        return normalized

    @staticmethod
    def _canary_identity_equal(
        row: sqlite3.Row,
        *,
        reservation_id: str,
        side: str,
        market_id: str | None,
        event_id: str | None,
        requested: Decimal,
        fee: Decimal,
        quantity: Decimal,
        config_generation: int | None,
        config_hash: str | None,
        config_id: str | None,
        control_generation: int | None,
        lineage: Mapping[str, Any] | None = None,
        detail_json: str | None = None,
    ) -> bool:
        expected_lineage = lineage or {name: None for name in _CANARY_LINEAGE_FIELDS}
        stored_lineage = _canary_lineage_from_row(row)
        lineage_matches = (
            _canary_lineage_equal(stored_lineage, expected_lineage)
            if _canary_lineage_is_rolling(stored_lineage)
            else _canary_lineage_subset_equal(expected_lineage, stored_lineage)
        )
        return (
            str(row["reservation_id"]) == reservation_id
            and str(row["side"]).upper() == side
            and (row["market_id"] or None) == (market_id or None)
            and (row["event_id"] or None) == (event_id or None)
            and _risk_decimal(row["requested_cost"]) == requested
            and _risk_decimal(row["fee_reserve"]) == fee
            and _risk_decimal(row["quantity"]) == quantity
            and (row["config_generation"] or None) == config_generation
            and (row["config_hash"] or None) == config_hash
            and (row["config_id"] or None) == config_id
            and (row["control_generation"] or None) == control_generation
            and lineage_matches
            and (detail_json is None or str(row["detail_json"] or "{}") == detail_json)
        )


    def record_canary_submission_attempt(
        self,
        *,
        attempt_id: str,
        intent_id: str,
        side: str,
        attempted_at: datetime | None = None,
        status: str = "ATTEMPTED",
        config_generation: int | None = None,
        config_hash: str | None = None,
        config_id: str | None = None,
        control_generation: int | None = None,
        strategy_version_id: str | None = None,
        research_trial_id: str | None = None,
        candidate_id: str | None = None,
        portfolio_selection_id: str | None = None,
        admission_policy_id: str | None = None,
        admission_policy_version: str | None = None,
        risk_config_id: str | None = None,
        risk_config_generation: int | None = None,
        risk_config_hash: str | None = None,
        allocation: Any | None = None,
        detail: Mapping[str, Any] | None = None,
    ) -> str:
        identifier = str(attempt_id or "").strip()
        intent = str(intent_id or "").strip()
        side_value = str(side or "").strip().upper()
        if not identifier or not intent or side_value not in {"BUY", "SELL"}:
            raise ValueError("attempt_id, intent_id, and side BUY/SELL are required")
        stamp = attempted_at or utc_now()
        requested_lineage = self._canary_normalize_lineage(
            {
                "strategy_version_id": strategy_version_id,
                "research_trial_id": research_trial_id,
                "candidate_id": candidate_id,
                "portfolio_selection_id": portfolio_selection_id,
                "admission_policy_id": admission_policy_id,
                "admission_policy_version": admission_policy_version,
                "risk_config_id": risk_config_id,
                "risk_config_generation": risk_config_generation,
                "risk_config_hash": risk_config_hash,
                "allocation": allocation,
            }
        )
        attempted_iso = _iso(stamp)
        status_value = str(status or "ATTEMPTED").strip().upper()
        detail_json = _dump(detail or {})
        with self.transaction(immediate=True):
            prior_by_id = self._conn.execute(
                "SELECT * FROM canary_submission_attempts WHERE attempt_id=?",
                (identifier,),
            ).fetchone()
            if prior_by_id is not None:
                prior_lineage = _canary_lineage_from_row(prior_by_id)
                replay_lineage = dict(requested_lineage)
                if _canary_lineage_is_rolling(prior_lineage):
                    for name in _CANARY_LINEAGE_FIELDS:
                        if replay_lineage.get(name) in (None, ""):
                            replay_lineage[name] = prior_lineage.get(name)
                    lineage_matches = _canary_lineage_equal(replay_lineage, prior_lineage)
                else:
                    lineage_matches = _canary_lineage_subset_equal(
                        requested_lineage,
                        prior_lineage,
                    )
                identity_matches = (
                    str(prior_by_id["intent_id"]) == intent
                    and str(prior_by_id["side"]).upper() == side_value
                    and str(prior_by_id["attempted_at"]) == attempted_iso
                    and str(prior_by_id["status"]).upper() == status_value
                    and (
                        config_generation is None
                        or (prior_by_id["config_generation"] or None) == int(config_generation)
                    )
                    and (
                        config_hash is None
                        or str(prior_by_id["config_hash"]) == str(config_hash)
                    )
                    and (
                        config_id is None
                        or str(prior_by_id["config_id"]).strip() == str(config_id).strip()
                    )
                    and (
                        control_generation is None
                        or (prior_by_id["control_generation"] or None) == int(control_generation)
                    )
                    and str(prior_by_id["detail_json"] or "{}") == detail_json
                    and lineage_matches
                )
                if not identity_matches:
                    raise ValueError("submission attempt identity conflict")
                return identifier

            authority = self._canary_authority_locked()
            if authority["config_id"] is None:
                raise ValueError("active canary settings are unavailable")
            if authority.get("control_state") == "KILLED":
                raise ValueError("canary control is killed")
            if side_value == "BUY" and authority.get("control_state") not in {
                None, "ARMED", "AUTONOMOUS_MICRO_LIVE",
            }:
                raise ValueError("canary entry is not armed")
            reservation = self._conn.execute(
                "SELECT * FROM canary_risk_reservations WHERE intent_id=?",
                (intent,),
            ).fetchone()
            if reservation is None:
                raise ValueError("risk reservation not found")
            bound_lineage = _canary_lineage_from_row(reservation)
            if str(reservation["side"]).upper() != side_value:
                raise ValueError("submission side conflicts with reservation")
            bound_generation = int(reservation["config_generation"])
            bound_hash = str(reservation["config_hash"])
            bound_config_id = str(reservation["config_id"])
            bound_control_generation = (
                int(reservation["control_generation"])
                if reservation["control_generation"] is not None
                else None
            )
            if (
                bound_generation != int(authority["generation"])
                or bound_hash != str(authority["config_hash"])
                or bound_config_id != str(authority["config_id"])
                or bound_control_generation != authority["control_generation"]
            ):
                raise ValueError("canary settings/control generation changed")
            if config_generation is not None and int(config_generation) != bound_generation:
                raise ValueError("config generation conflicts with reservation")
            if config_hash is not None and str(config_hash) != bound_hash:
                raise ValueError("config hash conflicts with reservation")
            if config_id is not None and str(config_id).strip() != bound_config_id:
                raise ValueError("config id conflicts with reservation")
            if _canary_lineage_is_rolling(bound_lineage):
                self._canary_rolling_binding_locked(
                    lineage=bound_lineage,
                    allow_exit=side_value == "SELL",
                )
                if requested_lineage["candidate_id"] is None:
                    requested_lineage["candidate_id"] = bound_lineage["candidate_id"]
                if not _canary_lineage_equal(requested_lineage, bound_lineage):
                    raise ValueError("rolling submission requires exact reservation lineage")
            elif not _canary_lineage_subset_equal(requested_lineage, bound_lineage):
                raise ValueError("submission lineage conflicts with reservation")
            reservation_status = str(reservation["status"]).upper()
            reservation_quantity = _risk_decimal(reservation["quantity"])
            reservation_filled_quantity = _risk_decimal(reservation["filled_quantity"])
            filled_projection_incomplete = (
                reservation_status == "FILLED"
                and reservation_quantity > 0
                and reservation_filled_quantity < reservation_quantity
            )
            if reservation_status in {"RELEASED", "CANCELLED", "CANCELED", "REJECTED", "SETTLED"} or (
                reservation_status == "FILLED" and not filled_projection_incomplete
            ):
                raise ValueError("submission reservation is terminal")
            if reservation_status == "UNKNOWN":
                raise ValueError("submission retry for UNKNOWN is forbidden")
            prior_for_intent = self._conn.execute(
                "SELECT attempt_id FROM canary_submission_attempts WHERE intent_id=? LIMIT 1",
                (intent,),
            ).fetchone()
            if prior_for_intent is not None:
                raise ValueError("submission retry for an intent is forbidden")
            start, end = self._canary_window(stamp)
            limits = dict(authority["limits"])
            max_orders_raw = limits.get(
                "max_submitted_orders_per_day",
                limits.get("max_orders_per_day", 0),
            )
            max_orders = int(max_orders_raw or 0)
            if max_orders <= 0:
                raise ValueError("daily submission limit is unavailable")
            attempts = self._conn.execute(
                "SELECT COUNT(*) AS n FROM canary_submission_attempts "
                "WHERE attempted_at>=? AND attempted_at<?",
                (start, end),
            ).fetchone()
            unsubmitted = self._conn.execute(
                "SELECT COUNT(*) AS n FROM canary_risk_reservations r "
                "WHERE r.created_at>=? AND r.created_at<? "
                "AND (r.status IN ('HELD','RESERVED','SUBMITTING','ACKNOWLEDGED','UNKNOWN',"
                "'PARTIAL','PARTIALLY_FILLED','OPEN') OR "
                "(UPPER(r.status)='FILLED' AND CAST(COALESCE(r.filled_quantity,'0') AS NUMERIC) "
                "< CAST(COALESCE(r.quantity,'0') AS NUMERIC))) "
                "AND NOT EXISTS (SELECT 1 FROM canary_submission_attempts a WHERE a.intent_id=r.intent_id)",
                (start, end),
            ).fetchone()
            if int(attempts["n"] or 0) + int(unsubmitted["n"] or 0) > max_orders:
                raise ValueError("daily submission limit reached")
            self._conn.execute(
                "INSERT INTO canary_submission_attempts("
                "attempt_id,intent_id,side,attempted_at,status,config_generation,config_hash,"
                "config_id,control_generation,strategy_version_id,research_trial_id,candidate_id,"
                "portfolio_selection_id,admission_policy_id,admission_policy_version,"
                "risk_config_id,risk_config_generation,risk_config_hash,allocation,detail_json"
                ") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    identifier,
                    intent,
                    side_value,
                    attempted_iso,
                    status_value,
                    bound_generation,
                    bound_hash,
                    bound_config_id,
                    bound_control_generation,
                    bound_lineage["strategy_version_id"],
                    bound_lineage["research_trial_id"],
                    bound_lineage["candidate_id"],
                    bound_lineage["portfolio_selection_id"],
                    bound_lineage["admission_policy_id"],
                    bound_lineage["admission_policy_version"],
                    bound_lineage["risk_config_id"],
                    bound_lineage["risk_config_generation"],
                    bound_lineage["risk_config_hash"],
                    bound_lineage["allocation"],
                    detail_json,
                ),
            )
            self._conn.execute(
                "UPDATE canary_risk_reservations SET submitted_at=COALESCE(submitted_at,?),"
                "status=CASE WHEN status='HELD' THEN 'SUBMITTING' ELSE status END,"
                "updated_at=? WHERE reservation_id=?",
                (attempted_iso, attempted_iso, reservation["reservation_id"]),
            )
        return identifier
    def record_canary_external_flow(
        self,
        *,
        flow_id: str,
        amount: Any,
        kind: str = "EXTERNAL",
        timestamp: datetime | None = None,
        strategy_version_id: str | None = None,
        research_trial_id: str | None = None,
        candidate_id: str | None = None,
        portfolio_selection_id: str | None = None,
        admission_policy_id: str | None = None,
        admission_policy_version: str | None = None,
        risk_config_id: str | None = None,
        risk_config_generation: int | None = None,
        risk_config_hash: str | None = None,
        allocation: Any | None = None,
        detail: Mapping[str, Any] | None = None,
    ) -> str:
        """Persist one idempotent cash/equity flow separately from trading."""
        identifier = str(flow_id or "").strip()
        kind_value = str(kind or "").strip().upper()
        if not identifier or kind_value not in {"EXTERNAL", "DEPOSIT", "WITHDRAWAL", "EQUITY_LOSS"}:
            raise ValueError("flow_id and a supported flow kind are required")
        lineage = self._canary_normalize_lineage(
            {
                "strategy_version_id": strategy_version_id,
                "research_trial_id": research_trial_id,
                "candidate_id": candidate_id,
                "portfolio_selection_id": portfolio_selection_id,
                "admission_policy_id": admission_policy_id,
                "admission_policy_version": admission_policy_version,
                "risk_config_id": risk_config_id,
                "risk_config_generation": risk_config_generation,
                "risk_config_hash": risk_config_hash,
                "allocation": allocation,
            }
        )
        value = _risk_decimal(amount, name="flow amount")
        stamp = timestamp or utc_now()
        stamp_iso = _iso(stamp)
        detail_json = _dump(detail or {})
        with self._write_context():
            prior = self._conn.execute(
                "SELECT * FROM canary_risk_cashflows WHERE flow_id=?",
                (identifier,),
            ).fetchone()
            if prior is not None:
                if (
                    str(prior["kind"]).upper() != kind_value
                    or _risk_decimal(prior["amount"]) != value
                    or str(prior["occurred_at"]) != stamp_iso
                    or str(prior["detail_json"] or "{}") != detail_json
                    or not _canary_lineage_equal(
                        _canary_lineage_from_row(prior),
                        lineage,
                    )
                ):
                    raise ValueError("cashflow identity conflict")
            else:
                self._conn.execute(
                    "INSERT INTO canary_risk_cashflows("
                    "flow_id,kind,amount,occurred_at,strategy_version_id,research_trial_id,candidate_id,"
                    "portfolio_selection_id,admission_policy_id,admission_policy_version,"
                    "risk_config_id,risk_config_generation,risk_config_hash,allocation,detail_json"
                    ") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        identifier,
                        kind_value,
                        _risk_text(value),
                        stamp_iso,
                        lineage["strategy_version_id"],
                        lineage["research_trial_id"],
                        lineage["candidate_id"],
                        lineage["portfolio_selection_id"],
                        lineage["admission_policy_id"],
                        lineage["admission_policy_version"],
                        lineage["risk_config_id"],
                        lineage["risk_config_generation"],
                        lineage["risk_config_hash"],
                        lineage["allocation"],
                        detail_json,
                    ),
                )
        return identifier
    def adopt_canary_legacy_reservation(
        self,
        *,
        event_id: str,
        intent_id: str | None = None,
        reservation_id: str | None = None,
        side: str = "BUY",
        market_id: str,
        token_id: str,
        requested_cost: Any,
        quantity: Any,
        timestamp: datetime,
        evidence: Mapping[str, Any] | None = None,
        legacy_detail: Mapping[str, Any] | None = None,
        legacy_status: str | None = None,
        fee_reserve: Any = "0",
        venue: str = "polymarket",
        candidate_id: str = "legacy",
        config_id: str | None = None,
        config_generation: int | None = None,
        config_hash: str | None = None,
        control_generation: int | None = None,
    ) -> dict[str, Any]:
        """Adopt a persisted legacy obligation without inventing a fill.

        Adoption only creates the canonical reservation identity and keeps it
        active/UNKNOWN until reconciliation records each genuine venue trade
        through :meth:`record_canary_fill`.  This makes a crash between
        adoption and trade projection visible as an unresolved obligation while
        preserving the original legacy evidence for audit and policy review.
        """
        event = str(event_id or "").strip()
        intent = str(intent_id or "").strip() or event
        reservation = str(reservation_id or "").strip() or event
        side_value = str(side or "").strip().upper()
        market = str(market_id or "").strip()
        token = str(token_id or "").strip()
        venue_value = str(venue or "").strip() or "polymarket"
        candidate_value = str(candidate_id or "").strip() or "legacy"
        if not event or not intent or not reservation or side_value not in {"BUY", "SELL"}:
            raise ValueError("legacy event, intent, reservation, and side BUY/SELL are required")
        if not market or not token or not isinstance(timestamp, datetime):
            raise ValueError("legacy market, token, and timestamp are required")
        requested_value = _risk_decimal(requested_cost, name="requested_cost", nonnegative=True)
        expected_quantity = _risk_decimal(quantity, name="quantity", nonnegative=True)
        fee_value = _risk_decimal(fee_reserve, name="fee_reserve", nonnegative=True)
        remaining = max(Decimal("0"), requested_value + fee_value)
        if expected_quantity <= 0:
            raise ValueError("legacy adoption quantity must be positive")

        # Keep all source material under an auditable namespace.  It is copied
        # verbatim and is never used as a synthetic canonical fill.
        reservation_detail: dict[str, Any] = {
            "legacy_adopted": True,
            "legacy_event_id": event,
            "legacy_status": str(legacy_status or "").strip().upper(),
            "token_id": token,
            "venue": venue_value,
            "candidate_id": candidate_value,
            "legacy_evidence": dict(evidence or {}),
            "legacy_detail": dict(legacy_detail or {}),
        }
        detail_json = _dump(reservation_detail)
        stamp_iso = _iso(timestamp)

        def optional_generation(value: Any, name: str) -> int | None:
            if value is None:
                return None
            if isinstance(value, bool):
                raise ValueError(f"{name} must be an integer")
            number = _risk_decimal(value, name=name, nonnegative=True)
            if number != number.to_integral_value():
                raise ValueError(f"{name} must be an integer")
            return int(number)

        supplied_config_id = str(config_id).strip() if config_id is not None else None
        supplied_config_hash = str(config_hash).strip() if config_hash is not None else None
        supplied_generation = optional_generation(config_generation, "config generation")
        supplied_control_generation = optional_generation(control_generation, "control generation")
        with self.transaction(immediate=True):
            # Read only the active settings identity; migration must not be
            # blocked by a killed/disarmed/stale control row and must not
            # authorize a new submission.
            active = self._conn.execute(
                "SELECT config_id,generation,config_hash FROM canary_setting_configs "
                "WHERE state='ACTIVE' ORDER BY generation DESC,created_at DESC,config_id DESC LIMIT 1"
            ).fetchone()
            bound_config_id = supplied_config_id or (str(active["config_id"]) if active is not None else None)
            bound_generation = supplied_generation if supplied_generation is not None else (
                int(active["generation"]) if active is not None else None
            )
            bound_config_hash = supplied_config_hash or (str(active["config_hash"]) if active is not None else None)
            bound_control_generation = supplied_control_generation
            prior = self._conn.execute(
                "SELECT * FROM canary_risk_reservations WHERE reservation_id=? OR intent_id=? OR event_id=? "
                "ORDER BY created_at,reservation_id LIMIT 1",
                (reservation, intent, event),
            ).fetchone()
            if prior is not None:
                prior_detail = _load(prior["detail_json"]) if prior["detail_json"] else {}
                prior_identity_detail = dict(prior_detail) if isinstance(prior_detail, Mapping) else {}
                prior_identity_detail.pop("_terminal_no_fill_proof", None)
                if (
                    str(prior["reservation_id"]) != reservation
                    or str(prior["intent_id"]) != intent
                    or str(prior["side"]).upper() != side_value
                    or (str(prior["market_id"]).strip() if prior["market_id"] is not None else None) != market
                    or (str(prior["event_id"]).strip() if prior["event_id"] is not None else None) != event
                    or _risk_decimal(prior["requested_cost"]) != requested_value
                    or _risk_decimal(prior["fee_reserve"]) != fee_value
                    or _risk_decimal(prior["quantity"]) != expected_quantity
                    or (str(prior["candidate_id"]).strip() if prior["candidate_id"] is not None else None) != candidate_value
                    or prior_identity_detail != reservation_detail
                ):
                    raise ValueError("legacy adoption identity conflict")
                row = prior
            else:
                self._conn.execute(
                    "INSERT INTO canary_risk_reservations("
                    "reservation_id,intent_id,side,market_id,event_id,requested_cost,filled_cost,remaining_cost,"
                    "fee_reserve,quantity,filled_quantity,status,config_generation,config_hash,config_id,"
                    "control_generation,candidate_id,detail_json,created_at,submitted_at,updated_at,released_at"
                    ") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        reservation,
                        intent,
                        side_value,
                        market,
                        event,
                        _risk_text(requested_value),
                        "0",
                        _risk_text(remaining),
                        _risk_text(fee_value),
                        _risk_text(expected_quantity),
                        "0",
                        "UNKNOWN",
                        bound_generation,
                        bound_config_hash,
                        bound_config_id,
                        bound_control_generation,
                        candidate_value,
                        detail_json,
                        stamp_iso,
                        stamp_iso,
                        stamp_iso,
                        None,
                    ),
                )
                row = self._conn.execute(
                    "SELECT * FROM canary_risk_reservations WHERE reservation_id=?",
                    (reservation,),
                ).fetchone()
        return _canary_reservation_record(row)
    def record_canary_equity_mark(
        self,
        *,
        mark_id: str,
        observed_at: datetime | None = None,
        marked_at: datetime | None = None,
        market_id: str,
        token_id: str,
        side: str = "SELL",
        quantity: Any,
        mark_price: Any,
        cost_basis_usd: Any,
        mark_fee: Any = "0",
        source: str = "POLYMARKET_ORDER_BOOK",
        config_id: str | None = None,
        config_generation: int | None = None,
        control_generation: int | None = None,
        strategy_version_id: str | None = None,
        research_trial_id: str | None = None,
        candidate_id: str | None = None,
        portfolio_selection_id: str | None = None,
        admission_policy_id: str | None = None,
        admission_policy_version: str | None = None,
        risk_config_id: str | None = None,
        risk_config_generation: int | None = None,
        risk_config_hash: str | None = None,
        allocation: Any | None = None,
        detail: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Persist one exact-token, fee-aware mark for owned canary inventory."""
        identifier = str(mark_id or "").strip()
        market = str(market_id or "").strip()
        token = str(token_id or "").strip()
        side_value = str(side or "").strip().upper()
        if side_value == "LONG":
            side_value = "SELL"
        source_value = str(source or "").strip()
        if not identifier or not market or not token or side_value != "SELL" or not source_value:
            raise ValueError("mark_id, market_id, token_id, SELL side, and source are required")
        mark_time = observed_at if observed_at is not None else marked_at
        if not isinstance(mark_time, datetime):
            raise ValueError("observed_at or marked_at must be a datetime")
        if observed_at is not None and marked_at is not None and _iso(observed_at) != _iso(marked_at):
            raise ValueError("observed_at and marked_at must agree")
        quantity_value = _risk_decimal(quantity, name="mark quantity", nonnegative=True)
        price_value = _risk_decimal(mark_price, name="mark price", nonnegative=True)
        basis_value = _risk_decimal(cost_basis_usd, name="mark cost basis", nonnegative=True)
        fee_value = _risk_decimal(mark_fee, name="mark fee", nonnegative=True)
        if quantity_value <= 0:
            raise ValueError("mark quantity must be positive")
        encoded_detail = _dump(detail or {})
        stamp_iso = _iso(mark_time)
        def optional_generation(value: Any, name: str) -> int | None:
            if value is None:
                return None
            if isinstance(value, bool):
                raise ValueError(f"{name} must be an integer")
            decimal_value = _risk_decimal(value, name=name, nonnegative=True)
            if decimal_value != decimal_value.to_integral_value():
                raise ValueError(f"{name} must be an integer")
            return int(decimal_value)
        config_generation_value = optional_generation(config_generation, "config generation")
        control_generation_value = optional_generation(control_generation, "control generation")
        lineage = self._canary_normalize_lineage(
            {
                "strategy_version_id": strategy_version_id,
                "research_trial_id": research_trial_id,
                "candidate_id": candidate_id,
                "portfolio_selection_id": portfolio_selection_id,
                "admission_policy_id": admission_policy_id,
                "admission_policy_version": admission_policy_version,
                "risk_config_id": risk_config_id,
                "risk_config_generation": risk_config_generation,
                "risk_config_hash": risk_config_hash,
                "allocation": allocation,
            }
        )
        config_id_value = str(config_id).strip() if config_id is not None else None
        with self.transaction(immediate=True):
            prior = self._conn.execute(
                "SELECT * FROM canary_equity_marks WHERE mark_id=?",
                (identifier,),
            ).fetchone()
            if prior is not None:
                if (
                    str(prior["market_id"]) != market
                    or str(prior["token_id"]) != token
                    or str(prior["side"]).upper() != side_value
                    or _risk_decimal(prior["quantity"]) != quantity_value
                    or _risk_decimal(prior["mark_price"]) != price_value
                    or _risk_decimal(prior["cost_basis_usd"]) != basis_value
                    or _risk_decimal(prior["mark_fee"]) != fee_value
                    or str(prior["observed_at"]) != stamp_iso
                    or str(prior["source"]) != source_value
                    or (str(prior["config_id"]).strip() if prior["config_id"] is not None else None) != config_id_value
                    or prior["config_generation"] != config_generation_value
                    or prior["control_generation"] != control_generation_value
                    or not _canary_lineage_equal(
                        _canary_lineage_from_row(prior),
                        lineage,
                    )
                    or str(prior["detail_json"] or "{}") != encoded_detail
                ):
                    raise ValueError("equity mark identity conflict")
                row = prior
            else:
                self._conn.execute(
                    "INSERT INTO canary_equity_marks("
                    "mark_id,market_id,token_id,side,quantity,mark_price,cost_basis_usd,mark_fee,"
                    "observed_at,source,config_id,config_generation,control_generation,"
                    "strategy_version_id,research_trial_id,candidate_id,portfolio_selection_id,"
                    "admission_policy_id,admission_policy_version,risk_config_id,"
                    "risk_config_generation,risk_config_hash,allocation,detail_json,created_at"
                    ") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        identifier,
                        market,
                        token,
                        side_value,
                        _risk_text(quantity_value),
                        _risk_text(price_value),
                        _risk_text(basis_value),
                        _risk_text(fee_value),
                        stamp_iso,
                        source_value,
                        config_id_value,
                        config_generation_value,
                        control_generation_value,
                        lineage["strategy_version_id"],
                        lineage["research_trial_id"],
                        lineage["candidate_id"],
                        lineage["portfolio_selection_id"],
                        lineage["admission_policy_id"],
                        lineage["admission_policy_version"],
                        lineage["risk_config_id"],
                        lineage["risk_config_generation"],
                        lineage["risk_config_hash"],
                        lineage["allocation"],
                        encoded_detail,
                        _iso(utc_now()),
                    ),
                )
                row = self._conn.execute(
                    "SELECT * FROM canary_equity_marks WHERE mark_id=?",
                    (identifier,),
                ).fetchone()
        return _canary_equity_mark_record(row)

    def reserve_canary_capacity(
        self,
        *,
        intent_id: str,
        side: str,
        requested_cost: Any = "0",
        fee_reserve: Any = "0",
        quantity: Any = "0",
        market_id: str | None = None,
        event_id: str | None = None,
        reservation_id: str | None = None,
        limits: Mapping[str, Any] | None = None,
        config_id: str | None = None,
        config_generation: int | None = None,
        config_hash: str | None = None,
        control_generation: int | None = None,
        strategy_version_id: str | None = None,
        research_trial_id: str | None = None,
        candidate_id: str | None = None,
        portfolio_selection_id: str | None = None,
        admission_policy_id: str | None = None,
        admission_policy_version: str | None = None,
        risk_config_id: str | None = None,
        risk_config_generation: int | None = None,
        risk_config_hash: str | None = None,
        allocation: Any | None = None,
        detail: Mapping[str, Any] | None = None,
        timestamp: datetime | None = None,
    ) -> dict[str, Any]:
        """Atomically reserve a config-fenced intent.

        The ACTIVE settings row and (when present) the persisted canary control
        row are the authority.  ``limits`` is only a tighter candidate and
        cannot widen or replace that authority.  A reservation is also the
        submission-slot commitment; recording an attempt consumes the same
        slot rather than counting it twice.
        """
        intent = str(intent_id or "").strip()
        side_value = str(side or "").strip().upper()
        if not intent or side_value not in {"BUY", "SELL"}:
            raise ValueError("intent_id and side BUY/SELL are required")
        requested = _risk_decimal(requested_cost, name="requested_cost", nonnegative=True)
        fee = _risk_decimal(fee_reserve, name="fee_reserve", nonnegative=True)
        amount = _risk_decimal(quantity, name="quantity", nonnegative=True)
        stamp = timestamp or utc_now()
        identifier = str(reservation_id or "reservation:" + intent).strip()
        market_key = str(market_id).strip() if market_id is not None and str(market_id).strip() else None
        event_key = str(event_id).strip() if event_id is not None and str(event_id).strip() else None
        encoded_detail = _dump(detail or {})
        lineage = self._canary_normalize_lineage(
            {
                "strategy_version_id": strategy_version_id,
                "research_trial_id": research_trial_id,
                "candidate_id": candidate_id,
                "portfolio_selection_id": portfolio_selection_id,
                "admission_policy_id": admission_policy_id,
                "admission_policy_version": admission_policy_version,
                "risk_config_id": risk_config_id,
                "risk_config_generation": risk_config_generation,
                "risk_config_hash": risk_config_hash,
                "allocation": allocation,
            }
        )
        with self.transaction(immediate=True):
            existing = self._conn.execute(
                "SELECT * FROM canary_risk_reservations WHERE intent_id=?",
                (intent,),
            ).fetchone()
            if existing is not None:
                stored_lineage = _canary_lineage_from_row(existing)
                replay_lineage = dict(lineage)
                if _canary_lineage_is_rolling(stored_lineage):
                    for name in _CANARY_LINEAGE_FIELDS:
                        if replay_lineage.get(name) in (None, ""):
                            replay_lineage[name] = stored_lineage.get(name)
                    lineage_matches = _canary_lineage_equal(replay_lineage, stored_lineage)
                else:
                    lineage_matches = _canary_lineage_subset_equal(lineage, stored_lineage)
                identity_matches = (
                    str(existing["reservation_id"]) == identifier
                    and str(existing["side"]).upper() == side_value
                    and (existing["market_id"] or None) == (market_key or None)
                    and (existing["event_id"] or None) == (event_key or None)
                    and _risk_decimal(existing["requested_cost"]) == requested
                    and _risk_decimal(existing["fee_reserve"]) == fee
                    and _risk_decimal(existing["quantity"]) == amount
                    and (
                        config_generation is None
                        or int(existing["config_generation"]) == int(config_generation)
                    )
                    and (
                        config_hash is None
                        or str(existing["config_hash"]) == str(config_hash)
                    )
                    and (
                        config_id is None
                        or str(existing["config_id"]).strip() == str(config_id).strip()
                    )
                    and (
                        control_generation is None
                        or (existing["control_generation"] or None) == int(control_generation)
                    )
                    and lineage_matches
                    and str(existing["detail_json"] or "{}") == encoded_detail
                )
                if not identity_matches:
                    raise ValueError("reservation identity conflict")
                return _canary_reservation_record(existing)

            authority = self._canary_authority_locked()
            if authority["config_id"] is None:
                raise ValueError("active canary settings are unavailable")
            if authority.get("control_state") == "KILLED":
                raise ValueError("canary control is killed")
            if side_value == "BUY" and authority.get("control_state") not in {
                None, "ARMED", "AUTONOMOUS_MICRO_LIVE",
            }:
                raise ValueError("canary entry is not armed")
            if config_generation is not None and int(config_generation) != int(authority["generation"]):
                raise ValueError("settings generation changed")
            if config_hash is not None and str(config_hash) != str(authority["config_hash"]):
                raise ValueError("settings config hash changed")
            if config_id is not None and str(config_id).strip() != str(authority["config_id"]):
                raise ValueError("settings config id changed")
            if control_generation is not None and authority["control_generation"] is not None:
                if int(control_generation) != int(authority["control_generation"]):
                    raise ValueError("canary control generation changed")
            bound_control_generation = authority["control_generation"]
            rolling_binding = self._canary_rolling_binding_locked(
                lineage=lineage,
                allow_exit=side_value == "SELL",
            )
            bound_lineage = dict(lineage)
            if rolling_binding is not None:
                bound_lineage["candidate_id"] = rolling_binding["candidate_id"]
                lineage["candidate_id"] = rolling_binding["candidate_id"]
                bound_lineage["allocation"] = _risk_text(rolling_binding["allocation"])
                lineage["allocation"] = bound_lineage["allocation"]
            prior_identifier = self._conn.execute(
                "SELECT * FROM canary_risk_reservations WHERE reservation_id=?",
                (identifier,),
            ).fetchone()
            if prior_identifier is not None:
                raise ValueError("reservation id already belongs to another intent")
            usage = self.canary_risk_accounting(stamp)
            selection_limits: dict[str, Any] = {}
            if rolling_binding is not None:
                selection_payload = rolling_binding.get("selection_payload")
                if isinstance(selection_payload, Mapping):
                    for key in ("limits", "risk_limits", "risk"):
                        candidate_limits = selection_payload.get(key)
                        if isinstance(candidate_limits, Mapping):
                            selection_limits.update(candidate_limits)
            if limits:
                selection_limits.update(dict(limits))
            effective = self._canary_effective_limits(
                authority["limits"],
                selection_limits or None,
            )
            all_in = requested + fee
            if rolling_binding is not None and side_value == "BUY":
                budget = _risk_decimal(
                    rolling_binding["selection"]["global_budget"],
                    name="rolling global budget",
                    nonnegative=True,
                )
                strategy_id = str(bound_lineage["strategy_version_id"])
                used_global = Decimal("0")
                used_strategy = Decimal("0")
                rolling_rows = self._conn.execute(
                    "SELECT * FROM canary_risk_reservations "
                    "WHERE UPPER(side)='BUY' AND portfolio_selection_id IS NOT NULL "
                    "AND TRIM(portfolio_selection_id)<>''"
                ).fetchall()
                for rolling_row in rolling_rows:
                    committed = self._rolling_open_buy_commitment_locked(rolling_row)
                    if committed <= 0:
                        continue
                    used_global += committed
                    if str(rolling_row["strategy_version_id"] or "") == strategy_id:
                        used_strategy += committed
                if all_in > budget or used_global + all_in > budget:
                    raise ValueError("rolling global budget exceeded")
                strategy_allocation = _risk_decimal(
                    bound_lineage["allocation"],
                    name="allocation",
                    nonnegative=True,
                )
                if all_in > strategy_allocation or used_strategy + all_in > strategy_allocation:
                    raise ValueError("rolling strategy allocation exceeded")
                details = detail or {}
                venue_minimum: Any = None
                if isinstance(details, Mapping):
                    for key in (
                        "venue_minimum_cost",
                        "venue_minimum_notional",
                        "minimum_notional",
                        "min_notional",
                    ):
                        if details.get(key) not in (None, ""):
                            venue_minimum = details[key]
                            break
                    venue_context = details.get("venue")
                    if venue_minimum in (None, "") and isinstance(venue_context, Mapping):
                        venue_minimum = venue_context.get(
                            "minimum_notional",
                            venue_context.get("min_notional"),
                        )
                if venue_minimum not in (None, ""):
                    minimum_value = _risk_decimal(
                        venue_minimum,
                        name="venue minimum",
                        nonnegative=True,
                    )
                    market_cap = effective.get("per_market_buy_cap_usd")
                    event_cap = effective.get("per_event_buy_cap_usd")
                    if (
                        minimum_value > strategy_allocation
                        or (
                            market_cap not in (None, "")
                            and minimum_value > _risk_decimal(
                                market_cap, name="per_market_buy_cap_usd", nonnegative=True
                            )
                        )
                        or (
                            event_cap not in (None, "")
                            and minimum_value > _risk_decimal(
                                event_cap, name="per_event_buy_cap_usd", nonnegative=True
                            )
                        )
                    ):
                        raise ValueError("venue minimum exceeds rolling allocation/cap")
            incoming_token = ""
            if isinstance(detail, Mapping):
                incoming_token = str(
                    detail.get("token_id") or detail.get("asset_id") or ""
                ).strip()
            if rolling_binding is not None and market_key and incoming_token:
                opposite = "SELL" if side_value == "BUY" else "BUY"
                opposite_rows = self._conn.execute(
                    "SELECT side,status,quantity,filled_quantity,detail_json "
                    "FROM canary_risk_reservations WHERE market_id=? AND UPPER(side)=?",
                    (market_key, opposite),
                ).fetchall()
                for opposite_row in opposite_rows:
                    opposite_status = str(opposite_row["status"] or "").upper()
                    opposite_active = opposite_status in {
                        "HELD", "RESERVED", "SUBMITTING", "ACKNOWLEDGED",
                        "UNKNOWN", "PARTIAL", "PARTIALLY_FILLED", "OPEN", "SUBMITTED",
                    } or (
                        opposite_status == "FILLED"
                        and _risk_decimal(opposite_row["filled_quantity"]) < _risk_decimal(
                            opposite_row["quantity"]
                        )
                    )
                    if not opposite_active:
                        continue
                    opposite_detail = (
                        _load(opposite_row["detail_json"])
                        if opposite_row["detail_json"]
                        else {}
                    )
                    opposite_token = (
                        str(
                            opposite_detail.get("token_id")
                            or opposite_detail.get("asset_id")
                            or ""
                        ).strip()
                        if isinstance(opposite_detail, Mapping)
                        else ""
                    )
                    if opposite_token == incoming_token:
                        raise ValueError("conflicting opposite pending token order")
            if side_value == "BUY":
                risk_breaker = str(usage.get("risk_breaker") or "").strip().upper()
                if risk_breaker:
                    raise ValueError(f"canary risk breaker active: {risk_breaker}")
                equity_status = str(usage.get("equity_status") or "UNKNOWN").strip().upper()
                if equity_status in {"UNKNOWN", "MISSING", "STALE"}:
                    raise ValueError("authoritative equity evidence unavailable")
                all_in_limit = _risk_decimal(
                    effective.get("max_all_in_buy_usd", "0"),
                    name="max_all_in_buy_usd",
                    nonnegative=True,
                )
                if all_in > all_in_limit:
                    raise ValueError("BUY exceeds all-in commitment")
                fee_limit = effective.get("max_fee_reserve_usd")
                if fee_limit not in (None, "") and fee > _risk_decimal(
                    fee_limit, name="max_fee_reserve_usd", nonnegative=True
                ):
                    raise ValueError("BUY exceeds fee reserve")
                gross_limit = _risk_decimal(
                    effective.get("max_gross_daily_buy_usd", "0"),
                    name="max_gross_daily_buy_usd",
                    nonnegative=True,
                )
                if gross_limit > 0 and _risk_decimal(usage["gross_daily_buy_usd"]) + all_in > gross_limit:
                    raise ValueError("BUY exceeds gross daily buy limit")
                exposure_limit = _risk_decimal(
                    effective.get("max_aggregate_exposure_usd", "0"),
                    name="max_aggregate_exposure_usd",
                    nonnegative=True,
                )
                if exposure_limit > 0 and _risk_decimal(usage["aggregate_exposure_usd"]) + all_in > exposure_limit:
                    raise ValueError("BUY exceeds aggregate exposure limit")
                open_cost_limit = _risk_decimal(
                    effective.get("max_aggregate_open_cost_usd", "0"),
                    name="max_aggregate_open_cost_usd",
                    nonnegative=True,
                )
                if open_cost_limit > 0 and _risk_decimal(usage["aggregate_open_cost_usd"]) + all_in > open_cost_limit:
                    raise ValueError("BUY exceeds aggregate open cost limit")
                max_positions = int(effective.get("max_positions", 0) or 0)
                if max_positions > 0:
                    existing_market = market_key in set(usage.get("open_market_ids") or ())
                    if int(usage["open_positions"]) + (0 if existing_market else 1) > max_positions:
                        raise ValueError("BUY exceeds position limit")
                realized_stop = effective.get("realized_loss_entry_stop_usd")
                if realized_stop not in (None, "") and _risk_decimal(usage["realized_loss_usd"]) >= _risk_decimal(
                    realized_stop, name="realized_loss_entry_stop_usd", nonnegative=True
                ):
                    raise ValueError("realized loss entry stop reached")
                equity_stop = effective.get("equity_loss_entry_stop_usd")
                if equity_stop not in (None, "") and _risk_decimal(usage["equity_loss_usd"]) >= _risk_decimal(
                    equity_stop, name="equity_loss_entry_stop_usd", nonnegative=True
                ):
                    raise ValueError("equity loss entry stop reached")
                for cap_name, usage_name, identity in (
                    ("per_market_buy_cap_usd", "per_market_buy_usd", market_key),
                    ("per_event_buy_cap_usd", "per_event_buy_usd", event_key),
                ):
                    cap = effective.get(cap_name)
                    if cap not in (None, "") and identity is not None:
                        cap_value = _risk_decimal(cap, name=cap_name, nonnegative=True)
                        used_value = _risk_decimal((usage[usage_name] or {}).get(identity, "0"))
                        if used_value + all_in > cap_value:
                            raise ValueError(f"BUY exceeds {cap_name}")
                cumulative_cap = effective.get("cumulative_buy_cap_usd")
                if cumulative_cap not in (None, "") and _risk_decimal(usage["cumulative_buy_usd"]) + all_in > _risk_decimal(
                    cumulative_cap, name="cumulative_buy_cap_usd", nonnegative=True
                ):
                    raise ValueError("BUY exceeds cumulative buy cap")
            else:
                if amount <= 0 or market_key is None:
                    raise ValueError("SELL requires a positive quantity and market")
                available = _risk_decimal(
                    (usage.get("open_quantity_by_market") or {}).get(market_key, "0")
                )
                reserved_exits = _risk_decimal(
                    (usage.get("pending_sell_quantity_by_market") or {}).get(market_key, "0")
                )
                if amount + reserved_exits > available:
                    raise ValueError("SELL exceeds owned inventory")
            start, end = self._canary_window(stamp)
            max_orders = int(
                effective.get(
                    "max_submitted_orders_per_day",
                    effective.get("max_orders_per_day", 0),
                )
                or 0
            )
            if max_orders <= 0:
                raise ValueError("daily submission limit is unavailable")
            attempts = self._conn.execute(
                "SELECT COUNT(*) AS n FROM canary_submission_attempts "
                "WHERE attempted_at>=? AND attempted_at<?",
                (start, end),
            ).fetchone()
            unsubmitted = self._conn.execute(
                "SELECT COUNT(*) AS n FROM canary_risk_reservations r "
                "WHERE r.created_at>=? AND r.created_at<? "
                "AND (r.status IN ('HELD','RESERVED','SUBMITTING','ACKNOWLEDGED','UNKNOWN',"
                "'PARTIAL','PARTIALLY_FILLED','OPEN') OR "
                "(UPPER(r.status)='FILLED' AND CAST(COALESCE(r.filled_quantity,'0') AS NUMERIC) "
                "< CAST(COALESCE(r.quantity,'0') AS NUMERIC))) "
                "AND NOT EXISTS (SELECT 1 FROM canary_submission_attempts a WHERE a.intent_id=r.intent_id)",
                (start, end),
            ).fetchone()
            base_reserved = int(attempts["n"] or 0) + int(unsubmitted["n"] or 0)
            if side_value == "BUY":
                exit_slots = int(
                    usage.get("open_lot_slots", usage.get("open_positions", 0)) or 0
                )
                required_slots = base_reserved + 1 + exit_slots + 1
            else:
                required_slots = base_reserved + 1
            if required_slots > max_orders:
                raise ValueError("daily submission/exit capacity reached")
            self._conn.execute(
                "INSERT INTO canary_risk_reservations("
                "reservation_id,intent_id,side,market_id,event_id,requested_cost,filled_cost,remaining_cost,"
                "fee_reserve,quantity,filled_quantity,status,config_generation,config_hash,config_id,"
                "control_generation,candidate_id,strategy_version_id,research_trial_id,portfolio_selection_id,"
                "admission_policy_id,admission_policy_version,risk_config_id,risk_config_generation,"
                "risk_config_hash,allocation,detail_json,created_at,submitted_at,updated_at,released_at"
                ") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    identifier,
                    intent,
                    side_value,
                    market_key,
                    event_key,
                    _risk_text(requested),
                    "0",
                    _risk_text(all_in),
                    _risk_text(fee),
                    _risk_text(amount),
                    "0",
                    "HELD",
                    int(authority["generation"]),
                    str(authority["config_hash"]),
                    str(authority["config_id"]),
                    bound_control_generation,
                    bound_lineage["candidate_id"],
                    bound_lineage["strategy_version_id"],
                    bound_lineage["research_trial_id"],
                    bound_lineage["portfolio_selection_id"],
                    bound_lineage["admission_policy_id"],
                    bound_lineage["admission_policy_version"],
                    bound_lineage["risk_config_id"],
                    bound_lineage["risk_config_generation"],
                    bound_lineage["risk_config_hash"],
                    bound_lineage["allocation"],
                    encoded_detail,
                    _iso(stamp),
                    None,
                    _iso(stamp),
                    None,
                ),
            )
            row = self._conn.execute(
                "SELECT * FROM canary_risk_reservations WHERE reservation_id=?",
                (identifier,),
            ).fetchone()
        return _canary_reservation_record(row)

    def record_canary_fill(
        self,
        *,
        fill_id: str,
        reservation_id: str,
        quantity: Any,
        price: Any,
        cost: Any | None = None,
        fee: Any = "0",
        filled_at: datetime | None = None,
        strategy_version_id: str | None = None,
        research_trial_id: str | None = None,
        candidate_id: str | None = None,
        portfolio_selection_id: str | None = None,
        admission_policy_id: str | None = None,
        admission_policy_version: str | None = None,
        risk_config_id: str | None = None,
        risk_config_generation: int | None = None,
        risk_config_hash: str | None = None,
        allocation: Any | None = None,
        detail: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        fill = str(fill_id or "").strip()
        reservation = str(reservation_id or "").strip()
        if not fill or not reservation:
            raise ValueError("fill_id and reservation_id are required")
        qty = _risk_decimal(quantity, name="fill quantity", nonnegative=True)
        px = _risk_decimal(price, name="fill price", nonnegative=True)
        charge = _risk_decimal(fee, name="fill fee", nonnegative=True)
        computed = qty * px + charge
        value = computed if cost is None else _risk_decimal(cost, name="fill cost", nonnegative=True)
        if value != computed:
            raise ValueError("fill cost must equal quantity*price+fee")
        stamp = filled_at or utc_now()
        stamp_iso = _iso(stamp)
        detail_json = _dump(detail or {})
        lineage = self._canary_normalize_lineage(
            {
                "strategy_version_id": strategy_version_id,
                "research_trial_id": research_trial_id,
                "candidate_id": candidate_id,
                "portfolio_selection_id": portfolio_selection_id,
                "admission_policy_id": admission_policy_id,
                "admission_policy_version": admission_policy_version,
                "risk_config_id": risk_config_id,
                "risk_config_generation": risk_config_generation,
                "risk_config_hash": risk_config_hash,
                "allocation": allocation,
            }
        )
        with self.transaction(immediate=True):
            reservation_row = self._conn.execute(
                "SELECT * FROM canary_risk_reservations WHERE reservation_id=?",
                (reservation,),
            ).fetchone()
            if reservation_row is None:
                raise ValueError("risk reservation not found")
            reservation_lineage = _canary_lineage_from_row(reservation_row)
            if _canary_lineage_is_rolling(reservation_lineage):
                if lineage["candidate_id"] is None:
                    lineage["candidate_id"] = reservation_lineage["candidate_id"]
                if not _canary_lineage_equal(lineage, reservation_lineage):
                    raise ValueError("fill lineage conflicts with reservation")
            elif not _canary_lineage_subset_equal(lineage, reservation_lineage):
                raise ValueError("fill lineage conflicts with reservation")
            prior = self._conn.execute(
                "SELECT * FROM canary_risk_fills WHERE fill_id=?",
                (fill,),
            ).fetchone()
            # SELL fills are only accepted from the exact persisted position
            # request.  Legacy low-level reservations without a position
            # request remain usable for BUY accounting, but a mapped SELL must
            # carry every immutable identity field.
            if str(reservation_row["side"] or "").upper() == "SELL":
                try:
                    mapped_requests = self._conn.execute(
                        "SELECT request_id,position_id,order_id,market_id,token_id,"
                        "side,requested_quantity,status "
                        "FROM canary_position_requests WHERE reservation_id=?",
                        (reservation,),
                    ).fetchall()
                except sqlite3.OperationalError:
                    mapped_requests = []
                incoming_detail = _load(detail_json) if detail_json else {}
                if mapped_requests:
                    if len(mapped_requests) != 1 or not isinstance(
                        incoming_detail, Mapping
                    ):
                        raise ValueError("fill identity conflict")
                    mapped = mapped_requests[0]
                    if (
                        prior is None
                        and str(mapped["status"] or "").strip().upper()
                        in {
                            "CANCELED",
                            "CANCELLED",
                            "EXPIRED",
                            "REJECTED",
                            "FAILED",
                            "ERROR",
                            "SETTLED",
                            "SETTLED_PARTIAL",
                            "RESOLVED",
                            "FINAL",
                            "CLOSED",
                            "COMPLETED",
                        }
                    ):
                        # Request terminality is authoritative even if a
                        # stale reservation projection still says OPEN.
                        raise ValueError("position request is terminal")
                    request_id = str(mapped["request_id"] or "").strip()
                    position_id = str(mapped["position_id"] or "").strip()
                    order_id = str(mapped["order_id"] or "").strip()
                    market_id = str(mapped["market_id"] or "").strip()
                    token_id = str(mapped["token_id"] or "").strip()
                    detail_token_id = str(
                        incoming_detail.get("token_id") or ""
                    ).strip()
                    detail_asset_id = str(
                        incoming_detail.get("asset_id") or ""
                    ).strip()
                    detail_token = detail_token_id or detail_asset_id
                    detail_token_conflict = (
                        bool(detail_token_id)
                        and bool(detail_asset_id)
                        and detail_token_id != detail_asset_id
                    )
                    if (
                        not request_id
                        or not position_id
                        or not order_id
                        or not market_id
                        or not token_id
                        or detail_token_conflict
                        or str(mapped["side"] or "").upper() != "SELL"
                        or str(incoming_detail.get("request_id") or "").strip()
                        != request_id
                        or str(incoming_detail.get("order_id") or "").strip()
                        != order_id
                        or str(incoming_detail.get("side") or "").strip().upper()
                        != "SELL"
                        or str(incoming_detail.get("market_id") or "").strip()
                        != market_id
                        or detail_token != token_id
                    ):
                        raise ValueError("fill identity conflict")
                    try:
                        requested_quantity = _risk_decimal(
                            mapped["requested_quantity"],
                            name="requested quantity",
                            nonnegative=True,
                        )
                        lot = self._conn.execute(
                            "SELECT quantity,sold_quantity,market_id,token_id "
                            "FROM canary_position_lots WHERE position_id=?",
                            (position_id,),
                        ).fetchone()
                    except (InvalidOperation, TypeError, ValueError):
                        lot = None
                        requested_quantity = Decimal("0")
                    if lot is None:
                        raise ValueError("fill identity conflict")
                    available_quantity = max(
                        Decimal("0"),
                        _risk_decimal(lot["quantity"])
                        - _risk_decimal(lot["sold_quantity"]),
                    )
                    if (
                        requested_quantity <= 0
                        or qty <= 0
                        or qty > requested_quantity
                        or (prior is None and qty > available_quantity)
                        or market_id != str(lot["market_id"] or "").strip()
                        or token_id != str(lot["token_id"] or "").strip()
                    ):
                        raise ValueError("fill identity conflict")
            prior = self._conn.execute(
                "SELECT * FROM canary_risk_fills WHERE fill_id=?",
                (fill,),
            ).fetchone()
            prior_is_new = prior is None
            reservation_status_before = str(
                reservation_row["status"] or ""
            ).strip().upper()
            if (
                prior_is_new
                and _canary_reservation_terminal(reservation_status_before)
                and not (
                    reservation_status_before == "RELEASED"
                    and str(reservation_row["side"] or "").strip().upper() == "BUY"
                )
            ):
                # A new venue fill can never reopen a terminal reservation.
                # Exact replays are handled below without changing its
                # durable terminal state.
                raise ValueError("risk reservation is terminal")
            if prior is not None:
                if (
                    str(prior["reservation_id"]) != reservation
                    or _risk_decimal(prior["quantity"]) != qty
                    or _risk_decimal(prior["price"]) != px
                    or _risk_decimal(prior["cost"]) != value
                    or _risk_decimal(prior["fee"]) != charge
                    or str(prior["filled_at"]) != stamp_iso
                    or not _canary_lineage_equal(
                        _canary_lineage_from_row(prior),
                        reservation_lineage,
                    )
                ):
                    raise ValueError("fill identity conflict")
                prior_detail = _load(prior["detail_json"]) if prior["detail_json"] else {}
                current_detail = _load(detail_json) if detail_json else {}
                if prior_detail != current_detail:
                    merged_detail = _merge_canary_fill_details(prior_detail, current_detail)
                    if merged_detail is None:
                        raise ValueError("fill identity conflict")
                    detail_json = _dump(merged_detail)
                    self._conn.execute(
                        "UPDATE canary_risk_fills SET detail_json=? WHERE fill_id=?",
                        (detail_json, fill),
                    )
            else:
                self._conn.execute(
                    "INSERT INTO canary_risk_fills("
                    "fill_id,reservation_id,quantity,price,cost,fee,filled_at,"
                    "strategy_version_id,research_trial_id,candidate_id,portfolio_selection_id,"
                    "admission_policy_id,admission_policy_version,risk_config_id,"
                    "risk_config_generation,risk_config_hash,allocation,detail_json"
                    ") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        fill,
                        reservation,
                        _risk_text(qty),
                        _risk_text(px),
                        _risk_text(value),
                        _risk_text(charge),
                        stamp_iso,
                        reservation_lineage["strategy_version_id"],
                        reservation_lineage["research_trial_id"],
                        reservation_lineage["candidate_id"],
                        reservation_lineage["portfolio_selection_id"],
                        reservation_lineage["admission_policy_id"],
                        reservation_lineage["admission_policy_version"],
                        reservation_lineage["risk_config_id"],
                        reservation_lineage["risk_config_generation"],
                        reservation_lineage["risk_config_hash"],
                        reservation_lineage["allocation"],
                        detail_json,
                    ),
                )
            fill_rows = self._conn.execute(
                "SELECT quantity,cost FROM canary_risk_fills WHERE reservation_id=?",
                (reservation,),
            ).fetchall()
            total_quantity = sum(
                (_risk_decimal(row["quantity"]) for row in fill_rows),
                Decimal("0"),
            )
            total_cost = sum(
                (_risk_decimal(row["cost"]) for row in fill_rows),
                Decimal("0"),
            )
            requested = _risk_decimal(reservation_row["requested_cost"]) + _risk_decimal(
                reservation_row["fee_reserve"]
            )
            remaining = max(Decimal("0"), requested - total_cost)
            side_value = str(reservation_row["side"]).upper()
            if side_value == "BUY" and total_cost > requested:
                reservation_detail = _load(reservation_row["detail_json"]) if reservation_row["detail_json"] else {}
                if not isinstance(reservation_detail, Mapping):
                    reservation_detail = {}
                reservation_detail["actual_cost_overrun_usd"] = _risk_text(total_cost - requested)
                reservation_detail["risk_breaker"] = "ACTUAL_FILL_OVER_PLAN"
                self._conn.execute(
                    "UPDATE canary_risk_reservations SET detail_json=? WHERE reservation_id=?",
                    (_dump(reservation_detail), reservation),
                )
            reservation_status = reservation_status_before
            reopened_released = (
                prior_is_new
                and reservation_status == "RELEASED"
                and side_value == "BUY"
            )
            settlement = _canary_fill_settlement_status(_load(detail_json) if detail_json else {})
            expected_quantity = _risk_decimal(reservation_row["quantity"])
            if expected_quantity > 0 and total_quantity >= expected_quantity:
                # Quantity completion exhausts the reservation, including any
                # fee-reserve dust left by decimal rounding.
                remaining = Decimal("0")
            if side_value == "BUY":
                if settlement not in _CANARY_CONFIRMED_SETTLEMENT_STATUSES:
                    fill_status = "UNKNOWN"
                elif total_quantity >= expected_quantity and expected_quantity > 0:
                    fill_status = "FILLED"
                else:
                    fill_status = "PARTIALLY_FILLED"
            elif side_value == "SELL":
                if settlement not in _CANARY_CONFIRMED_SETTLEMENT_STATUSES:
                    fill_status = "OPEN"
                elif total_quantity > 0 and total_quantity < expected_quantity:
                    fill_status = "PARTIALLY_FILLED"
                elif total_quantity >= expected_quantity and expected_quantity > 0:
                    fill_status = "FILLED"
                else:
                    fill_status = reservation_status
            else:
                fill_status = reservation_status
            # Replaying a fill cannot downgrade a terminal projection.  A
            # genuinely new fill is different evidence: RELEASED may reopen
            # while delayed canonical fills arrive, and FILLED is only final
            # once its quantity is actually complete.
            if reservation_status == "SETTLED" or (
                not prior_is_new and reservation_status == "RELEASED"
            ):
                fill_status = reservation_status
            fill_status = _monotonic_canary_reservation_status(
                reservation_status,
                fill_status,
            )
            updated = self._conn.execute(
                "UPDATE canary_risk_reservations SET filled_cost=?,"
                "remaining_cost=?,filled_quantity=?,status=?,released_at=?,"
                "updated_at=? WHERE reservation_id=? "
                "AND UPPER(COALESCE(status,''))=? "
                "AND filled_quantity IS ? AND filled_cost IS ? "
                "AND remaining_cost IS ? AND released_at IS ?",
                (
                    _risk_text(total_cost),
                    _risk_text(remaining),
                    _risk_text(total_quantity),
                    fill_status,
                    None if reopened_released else reservation_row["released_at"],
                    stamp_iso,
                    reservation,
                    reservation_status_before,
                    reservation_row["filled_quantity"],
                    reservation_row["filled_cost"],
                    reservation_row["remaining_cost"],
                    reservation_row["released_at"],
                ),
            )
            if int(updated.rowcount or 0) != 1:
                raise ValueError("risk reservation state changed")
            row = self._conn.execute(
                "SELECT * FROM canary_risk_reservations WHERE reservation_id=?",
                (reservation,),
            ).fetchone()
        return _canary_reservation_record(row)

    def release_canary_capacity(
        self,
        reservation_id: str,
        *,
        status: str = "RELEASED",
        timestamp: datetime | None = None,
        strategy_version_id: str | None = None,
        research_trial_id: str | None = None,
        candidate_id: str | None = None,
        portfolio_selection_id: str | None = None,
        admission_policy_id: str | None = None,
        admission_policy_version: str | None = None,
        risk_config_id: str | None = None,
        risk_config_generation: int | None = None,
        risk_config_hash: str | None = None,
        allocation: Any | None = None,
        detail: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        identifier = str(reservation_id or "").strip()
        if not identifier:
            raise ValueError("reservation_id is required")
        state = str(status or "").strip().upper()
        if state not in {
            "RELEASED", "CANCELLED", "CANCELED", "REJECTED", "UNKNOWN",
            "SETTLED", "FILLED", "PARTIALLY_FILLED", "PARTIAL", "OPEN",
        }:
            raise ValueError("invalid risk reservation status")
        stamp = timestamp or utc_now()
        release_detail = dict(detail or {})
        lineage = self._canary_normalize_lineage(
            {
                "strategy_version_id": strategy_version_id,
                "research_trial_id": research_trial_id,
                "candidate_id": candidate_id,
                "portfolio_selection_id": portfolio_selection_id,
                "admission_policy_id": admission_policy_id,
                "admission_policy_version": admission_policy_version,
                "risk_config_id": risk_config_id,
                "risk_config_generation": risk_config_generation,
                "risk_config_hash": risk_config_hash,
                "allocation": allocation,
            }
        )
        with self._write_context():
            row = self._conn.execute(
                "SELECT * FROM canary_risk_reservations WHERE reservation_id=?",
                (identifier,),
            ).fetchone()
            if row is None:
                raise ValueError("risk reservation not found")
            stored_lineage = _canary_lineage_from_row(row)
            if _canary_lineage_is_rolling(stored_lineage):
                if lineage["candidate_id"] is None:
                    lineage["candidate_id"] = stored_lineage["candidate_id"]
                if not _canary_lineage_equal(lineage, stored_lineage):
                    raise ValueError("release lineage conflicts with reservation")
            elif not _canary_lineage_subset_equal(lineage, stored_lineage):
                raise ValueError("release lineage conflicts with reservation")
            prior_state = str(row["status"] or "").strip().upper()
            reservation_side = str(row["side"] or "").strip().upper()
            reservation_quantity = _risk_decimal(
                row["quantity"],
                name="reservation quantity",
                nonnegative=True,
            )
            filled_quantity = _risk_decimal(
                row["filled_quantity"],
                name="filled quantity",
                nonnegative=True,
            )
            remaining = _risk_decimal(row["remaining_cost"])
            adopted_detail = _load(row["detail_json"]) if row["detail_json"] else {}
            adopted_legacy = (
                isinstance(adopted_detail, Mapping)
                and adopted_detail.get("legacy_adopted") is True
            )
            canonical_fill_rows = self._conn.execute(
                "SELECT detail_json FROM canary_risk_fills WHERE reservation_id=?",
                (identifier,),
            ).fetchall()
            has_confirmed_fill = any(
                _canary_fill_settlement_status(
                    _load(fill["detail_json"]) if fill["detail_json"] else {}
                ) in _CANARY_CONFIRMED_SETTLEMENT_STATUSES
                for fill in canonical_fill_rows
            )
            if prior_state == "SETTLED" and state != "SETTLED":
                # A stale release/reconciliation cannot rewrite a settled
                # reservation.  Return the durable terminal record unchanged.
                return _canary_reservation_record(row)
            if (
                prior_state == "RELEASED"
                and state not in {"RELEASED", "SETTLED"}
            ):
                # RELEASED may be upgraded by an explicit settlement, but
                # stale provisional/unknown writes cannot reopen it.
                return _canary_reservation_record(row)
            terminal_no_fill = _canary_terminal_no_fill_proof(release_detail)
            if (
                adopted_legacy
                and not has_confirmed_fill
                and state not in {"UNKNOWN", "PARTIALLY_FILLED", "PARTIAL", "OPEN"}
                and not terminal_no_fill
            ):
                # A migration crash or a terminal venue response without any
                # stable trade IDs remains an unresolved active obligation.
                state = "UNKNOWN"
            if (
                adopted_legacy
                and terminal_no_fill
                and canonical_fill_rows
            ):
                # A no-fill proof cannot override even a provisional stable
                # trade record.
                state = "UNKNOWN"
                terminal_no_fill = False
            if terminal_no_fill:
                adopted_detail = dict(adopted_detail)
                adopted_detail["_terminal_no_fill_proof"] = release_detail
                self._conn.execute(
                    "UPDATE canary_risk_reservations SET detail_json=? WHERE reservation_id=?",
                    (_dump(adopted_detail), identifier),
                )
            fully_filled_buy_release = (
                prior_state == "FILLED"
                and state == "RELEASED"
                and reservation_side == "BUY"
                and reservation_quantity > 0
                and filled_quantity >= reservation_quantity
            )
            if not fully_filled_buy_release:
                state = _monotonic_canary_reservation_status(prior_state, state)
            if (
                state not in {"UNKNOWN", "PARTIALLY_FILLED", "PARTIAL", "OPEN", "FILLED"}
            ):
                remaining = Decimal("0")
            if state == "FILLED":
                # FILLED is still awaiting final SELL settlement; it is not
                # itself a capacity release and must retain no release marker.
                released_at = row["released_at"]
            elif (
                prior_state in _CANARY_RESERVATION_TERMINAL_STATUSES
                and state == prior_state
            ):
                released_at = row["released_at"] or _iso(stamp)
            else:
                released_at = (
                    _iso(stamp)
                    if state
                    not in {"UNKNOWN", "PARTIALLY_FILLED", "PARTIAL", "OPEN"}
                    else None
                )
            updated = self._conn.execute(
                "UPDATE canary_risk_reservations SET status=?,remaining_cost=?,released_at=?,"
                "updated_at=? WHERE reservation_id=? AND "
                "(UPPER(status) <> 'SETTLED' OR UPPER(status)=UPPER(?))",
                (
                    state,
                    _risk_text(remaining),
                    released_at,
                    _iso(stamp),
                    identifier,
                    state,
                ),
            )
            if int(updated.rowcount or 0) != 1:
                raise ValueError("risk reservation state changed")
            row = self._conn.execute(
                "SELECT * FROM canary_risk_reservations WHERE reservation_id=?",
                (identifier,),
            ).fetchone()
        return _canary_reservation_record(row)
    def canary_risk_accounting(
        self,
        now: datetime | None = None,
        *,
        strategy_version_id: str | None = None,
        research_trial_id: str | None = None,
        candidate_id: str | None = None,
        portfolio_selection_id: str | None = None,
        admission_policy_id: str | None = None,
        admission_policy_version: str | None = None,
        risk_config_id: str | None = None,
        risk_config_generation: int | None = None,
        risk_config_hash: str | None = None,
        allocation: Any | None = None,
    ) -> dict[str, Any]:
        """Return exact Decimal usage while preserving both ledger generations."""
        observed = ensure_utc(now or utc_now())
        lineage_filter = self._canary_normalize_lineage(
            {
                "strategy_version_id": strategy_version_id,
                "research_trial_id": research_trial_id,
                "candidate_id": candidate_id,
                "portfolio_selection_id": portfolio_selection_id,
                "admission_policy_id": admission_policy_id,
                "admission_policy_version": admission_policy_version,
                "risk_config_id": risk_config_id,
                "risk_config_generation": risk_config_generation,
                "risk_config_hash": risk_config_hash,
                "allocation": allocation,
            }
        )
        start, end = self._canary_window(observed)
        accounting_day_pht = datetime.fromisoformat(start).astimezone(
            ZoneInfo("Asia/Manila")
        ).date().isoformat()
        active_statuses = {
            "HELD", "RESERVED", "SUBMITTING", "ACKNOWLEDGED", "UNKNOWN",
            "PARTIALLY_FILLED", "PARTIAL", "OPEN", "SUBMITTED",
        }
        result: dict[str, Any] = {
            "accounting_day_pht": accounting_day_pht,
            "submitted_orders": 0,
            "buy_filled_usd": Decimal("0"),
            "buy_pending_usd": Decimal("0"),
            "buy_unknown_usd": Decimal("0"),
            "gross_daily_buy_usd": Decimal("0"),
            "all_in_buy_reserved_usd": Decimal("0"),
            "aggregate_open_cost_usd": Decimal("0"),
            "aggregate_exposure_usd": Decimal("0"),
            "open_positions": 0,
            "realized_loss_usd": Decimal("0"),
            "equity_loss_usd": Decimal("0"),
            "today_realized_pnl_usd": Decimal("0"),
            "equity_status": "UNKNOWN",
            "risk_breaker": None,
            "external_flow_usd": Decimal("0"),
            "per_market_buy_usd": {},
            "per_event_buy_usd": {},
            "cumulative_buy_usd": Decimal("0"),
            "open_quantity_by_market": {},
            "pending_sell_quantity_by_market": {},
            "open_market_ids": [],
            "open_lot_slots": 0,
            "lineage": {
                key: value
                for key, value in lineage_filter.items()
                if value not in (None, "")
            },
            "rolling_global_reserved_usd": Decimal("0"),
            "rolling_global_budget_usd": Decimal("0"),
            "rolling_strategy_reserved_usd": {},
            "rolling_strategy_allocations": {},
        }
        strict_candidate_scope = lineage_filter.get("candidate_id") not in (None, "")

        def accounting_lineage_matches(row: sqlite3.Row | None) -> bool:
            stored_lineage = _canary_lineage_from_row(row)
            if strict_candidate_scope:
                # Candidate-scoped reporting is a rolling query boundary, not
                # an idempotent-write compatibility comparison: legacy rows
                # and rows without the requested candidate are excluded.
                if not _canary_lineage_is_rolling(stored_lineage):
                    return False
                if stored_lineage.get("candidate_id") != lineage_filter.get("candidate_id"):
                    return False
            return _canary_lineage_subset_equal(lineage_filter, stored_lineage)

        def add_map(name: str, key: str | None, value: Decimal) -> None:
            if key:
                target = result[name]
                target[key] = target.get(key, Decimal("0")) + value

        def detail_pnl(detail_value: Any, fallback: Mapping[str, Any] | None = None) -> Decimal:
            payload = detail_value if isinstance(detail_value, Mapping) else {}
            source = dict(fallback or {})
            source.update(payload)
            for key in ("realized_pnl_usd", "realized_pnl", "pnl_usd", "pnl"):
                if key in source and source[key] not in (None, ""):
                    return _risk_decimal(source[key], name=key)
            for proceeds_key in ("proceeds_usd", "proceeds", "sell_proceeds_usd"):
                for basis_key in ("entry_cost_usd", "cost_basis_usd", "basis_usd"):
                    if proceeds_key in source and basis_key in source:
                        return _risk_decimal(source[proceeds_key], name=proceeds_key) - _risk_decimal(
                            source[basis_key], name=basis_key
                        )
            return Decimal("0")

        def detail_loss(detail_value: Any, fallback: Mapping[str, Any] | None = None) -> Decimal:
            pnl = detail_pnl(detail_value, fallback)
            return -pnl if pnl < 0 else Decimal("0")
        with self._lock:
            reset_row = self._conn.execute(
                "SELECT timestamp FROM canary_setting_audit "
                "WHERE action='CUMULATIVE_USAGE_RESET' "
                "ORDER BY timestamp DESC,rowid DESC,audit_id DESC LIMIT 1"
            ).fetchone()
            cumulative_reset_at = str(reset_row["timestamp"]) if reset_row is not None else None
            cumulative_fills_by_reservation: dict[str, Decimal] = {}
            if cumulative_reset_at is not None:
                for fill in self._conn.execute(
                    "SELECT reservation_id,cost,filled_at FROM canary_risk_fills"
                ).fetchall():
                    if str(fill["filled_at"]) >= cumulative_reset_at:
                        key = str(fill["reservation_id"])
                        cumulative_fills_by_reservation[key] = (
                            cumulative_fills_by_reservation.get(key, Decimal("0"))
                            + _risk_decimal(fill["cost"])
                        )
            attempt_rows = self._conn.execute(
                "SELECT intent_id FROM canary_submission_attempts "
                "WHERE attempted_at>=? AND attempted_at<?",
                (start, end),
            ).fetchall()
            result["submitted_orders"] = len(attempt_rows)
            reservation_rows = self._conn.execute(
                "SELECT * FROM canary_risk_reservations"
            ).fetchall()
            if any(value not in (None, "") for value in lineage_filter.values()):
                reservation_rows = [
                    row for row in reservation_rows if accounting_lineage_matches(row)
                ]
                selected_intents = {
                    str(row["intent_id"])
                    for row in reservation_rows
                }
                result["submitted_orders"] = sum(
                    1
                    for row in attempt_rows
                    if str(row["intent_id"]) in selected_intents
                )
            reservation_by_id = {str(row["reservation_id"]): row for row in reservation_rows}
            new_event_ids = {
                str(row["event_id"])
                for row in reservation_rows
                if row["event_id"] is not None and str(row["event_id"]).strip()
            }
            fill_rows = self._conn.execute(
                "SELECT * FROM canary_risk_fills ORDER BY filled_at,fill_id"
            ).fetchall()
            fills_by_reservation: dict[str, list[tuple[sqlite3.Row, bool]]] = {}
            day_confirmed: dict[str, Decimal] = {}
            day_provisional: dict[str, Decimal] = {}
            for fill in fill_rows:
                reservation_id = str(fill["reservation_id"])
                settlement = _canary_fill_settlement_status(
                    _load(fill["detail_json"]) if fill["detail_json"] else {}
                )
                confirmed = settlement in _CANARY_CONFIRMED_SETTLEMENT_STATUSES
                fills_by_reservation.setdefault(reservation_id, []).append((fill, confirmed))
                stamp = str(fill["filled_at"])
                if start <= stamp < end:
                    target = day_confirmed if confirmed else day_provisional
                    target[reservation_id] = target.get(reservation_id, Decimal("0")) + _risk_decimal(fill["cost"])
            # Position lots are the authoritative entry-cost basis for exits.
            # Resolve each canonical SELL through its persisted reservation
            # and position-request links.  Market/token identity and payload
            # details are not durable joins: they can be shared by multiple
            # lots or changed by an untrusted venue response.
            lot_by_position: dict[str, sqlite3.Row] = {}
            try:
                lot_rows = self._conn.execute(
                    "SELECT position_id,event_id,market_id,token_id,quantity,"
                    "sold_quantity,cost_basis FROM canary_position_lots"
                ).fetchall()
            except sqlite3.OperationalError:
                lot_rows = []
            lots_by_event: dict[str, list[sqlite3.Row]] = {}
            for lot in lot_rows:
                position_id = str(lot["position_id"] or "").strip()
                event_id = str(lot["event_id"] or "").strip()
                if position_id:
                    lot_by_position[position_id] = lot
                if event_id:
                    lots_by_event.setdefault(event_id, []).append(lot)

            # A position request is the authoritative bridge from a risk
            # reservation to the owned position.  Keep ambiguous or
            # incomplete links unresolved rather than guessing by market or
            # token.
            sell_requests_by_reservation: dict[
                str, list[sqlite3.Row]
            ] = {}
            try:
                request_rows = self._conn.execute(
                    "SELECT request_id,position_id,reservation_id,event_id,status,"
                    "settlement_status,order_id,market_id,token_id,side,"
                    "requested_quantity "
                    "FROM canary_position_requests "
                    "WHERE UPPER(side)='SELL'"
                ).fetchall()
            except sqlite3.OperationalError:
                request_rows = []
            for request_row in request_rows:
                reservation_id = str(request_row["reservation_id"] or "").strip()
                if reservation_id:
                    sell_requests_by_reservation.setdefault(
                        reservation_id, []
                    ).append(request_row)

            sell_quantity_by_position: dict[str, Decimal] = {}

            def linked_lot(
                reservation: sqlite3.Row,
            ) -> tuple[sqlite3.Row, sqlite3.Row] | None:
                reservation_id = str(reservation["reservation_id"] or "").strip()
                event_id = str(reservation["event_id"] or "").strip()
                intent_id = str(reservation["intent_id"] or "").strip()
                requests = sell_requests_by_reservation.get(reservation_id, ())
                if len(requests) != 1:
                    return None
                request = requests[0]
                request_id = str(request["request_id"] or "").strip()
                request_position_id = str(request["position_id"] or "").strip()
                request_event_id = str(request["event_id"] or "").strip()
                request_order_id = str(request["order_id"] or "").strip()
                request_market = str(request["market_id"] or "").strip()
                request_token = str(request["token_id"] or "").strip()
                request_status = str(request["status"] or "").strip().upper()
                request_settlement = str(
                    request["settlement_status"] or ""
                ).strip().upper()
                lot = lot_by_position.get(request_position_id)
                # The combined service persists the same request ID as the
                # reservation intent/event and as the position request event.
                # Requiring all links and explicit final settlement prevents a
                # stale or provisional SELL from releasing another position's
                # basis.
                if (
                    str(reservation["side"] or "").upper() != "SELL"
                    or not reservation_id
                    or not event_id
                    or not intent_id
                    or not request_id
                    or not request_position_id
                    or not request_event_id
                    or not request_order_id
                    or request_id != intent_id
                    or request_event_id != event_id
                    or request_status not in {"SETTLED", "SETTLED_PARTIAL"}
                    or request_settlement not in _CANARY_FINAL_SETTLEMENT_STATUSES
                    or request_market != str(reservation["market_id"] or "").strip()
                    or not request_market
                    or not request_token
                    or lot is None
                    or request_market != str(lot["market_id"] or "").strip()
                    or request_token != str(lot["token_id"] or "").strip()
                ):
                    return None
                return request, lot

            def linked_legacy_lot(
                event_id: str | None,
                detail: Mapping[str, Any],
            ) -> sqlite3.Row | None:
                position_id = str(detail.get("position_id") or "").strip()
                if position_id:
                    return lot_by_position.get(position_id)
                candidates = lots_by_event.get(str(event_id or "").strip(), ())
                return candidates[0] if len(candidates) == 1 else None

            def valid_sell_fill(
                row: sqlite3.Row,
                detail: Mapping[str, Any],
                quantity: Decimal,
                linked: tuple[sqlite3.Row, sqlite3.Row] | None,
            ) -> bool:
                if quantity <= 0 or str(row["status"]).upper() == "UNKNOWN":
                    return False
                if linked is None:
                    return False
                request, lot = linked
                try:
                    expected = _risk_decimal(
                        row["quantity"],
                        name="quantity",
                        nonnegative=True,
                    )
                    requested = _risk_decimal(
                        request["requested_quantity"],
                        name="requested quantity",
                        nonnegative=True,
                    )
                    lot_quantity = _risk_decimal(
                        lot["quantity"],
                        name="owned lot quantity",
                        nonnegative=True,
                    )
                except (InvalidOperation, TypeError, ValueError):
                    return False
                # A fill can release basis only when the persisted request is
                # the exact owner and carries all immutable venue identity.
                if (
                    expected <= 0
                    or quantity > expected
                    or requested <= 0
                    or quantity > requested
                    or quantity > lot_quantity
                    or str(detail.get("request_id") or "").strip()
                    != str(request["request_id"] or "").strip()
                    or str(detail.get("order_id") or "").strip()
                    != str(request["order_id"] or "").strip()
                    or str(detail.get("side") or "").strip().upper() != "SELL"
                    or str(detail.get("market_id") or "").strip()
                    != str(request["market_id"] or "").strip()
                    or str(detail.get("market_id") or "").strip()
                    != str(lot["market_id"] or "").strip()
                    or str(detail.get("token_id") or detail.get("asset_id") or "").strip()
                    != str(request["token_id"] or "").strip()
                    or str(detail.get("token_id") or detail.get("asset_id") or "").strip()
                    != str(lot["token_id"] or "").strip()
                ):
                    return False
                breaker = str(detail.get("risk_breaker") or "").strip().upper()
                return breaker not in {
                    "EXIT_FILL_OVER_PLAN",
                    "ACTUAL_FILL_OVER_PLAN",
                }
            mark_rows = self._conn.execute(
                "SELECT * FROM canary_equity_marks ORDER BY observed_at DESC,rowid DESC,mark_id DESC"
            ).fetchall()
            scoped_lineage = any(value not in (None, "") for value in lineage_filter.values())
            if scoped_lineage:
                mark_rows = [mark for mark in mark_rows if accounting_lineage_matches(mark)]
            latest_marks: dict[tuple[str, str, str, str], sqlite3.Row] = {}
            authority = self._canary_authority_locked()
            for mark in mark_rows:
                if (
                    mark["config_id"] is not None
                    and authority["config_id"] is not None
                    and str(mark["config_id"]) != str(authority["config_id"])
                ):
                    continue
                if (
                    mark["config_generation"] is not None
                    and authority["generation"] is not None
                    and int(mark["config_generation"]) != int(authority["generation"])
                ):
                    continue
                if (
                    mark["control_generation"] is not None
                    and authority["control_generation"] is not None
                    and int(mark["control_generation"]) != int(authority["control_generation"])
                ):
                    continue
                mark_detail = _load(mark["detail_json"]) if mark["detail_json"] else {}
                position_id = (
                    str(mark_detail.get("position_id") or "").strip()
                    if isinstance(mark_detail, Mapping)
                    else ""
                )
                key = (
                    str(mark["market_id"]),
                    str(mark["token_id"]),
                    str(mark["side"]).upper(),
                    position_id,
                )
                if key not in latest_marks:
                    latest_marks[key] = mark
            # New reservations are the canonical source for each durable event.
            # Only final per-trade settlement evidence creates owned inventory.
            market_qty: dict[str, Decimal] = {}
            market_pending_sell: dict[str, Decimal] = {}
            open_lot_counts: dict[str, int] = {}
            owned_inventory: dict[tuple[str, str], Decimal] = {}
            unknown_inventory = False
            unknown_execution = False
            for row in reservation_rows:
                side = str(row["side"]).upper()
                status = str(row["status"]).upper()
                filled_quantity = _risk_decimal(row["filled_quantity"])
                expected_quantity = _risk_decimal(row["quantity"])
                reservation_active = (
                    status in active_statuses
                    or (
                        status == "FILLED"
                        and expected_quantity > 0
                        and filled_quantity < expected_quantity
                    )
                )
                filled = _risk_decimal(row["filled_cost"])
                remaining = _risk_decimal(row["remaining_cost"])
                market = str(row["market_id"]) if row["market_id"] else None
                event = str(row["event_id"]) if row["event_id"] else None
                reservation_id = str(row["reservation_id"])
                reservation_detail = _load(row["detail_json"]) if row["detail_json"] else {}
                if not isinstance(reservation_detail, Mapping):
                    reservation_detail = {}
                breaker = str(reservation_detail.get("risk_breaker") or "").strip().upper()
                if breaker:
                    result["risk_breaker"] = breaker
                fills = fills_by_reservation.get(reservation_id, [])
                confirmed_cost = sum(
                    (_risk_decimal(fill["cost"]) for fill, confirmed in fills if confirmed),
                    Decimal("0"),
                )
                confirmed_quantity = sum(
                    (_risk_decimal(fill["quantity"]) for fill, confirmed in fills if confirmed),
                    Decimal("0"),
                )
                provisional_cost = max(Decimal("0"), filled - confirmed_cost)
                provisional_quantity = sum(
                    (_risk_decimal(fill["quantity"]) for fill, confirmed in fills if not confirmed),
                    Decimal("0"),
                )
                if side == "BUY":
                    day_filled = day_confirmed.get(reservation_id, Decimal("0"))
                    day_unknown = day_provisional.get(reservation_id, Decimal("0"))
                    result["buy_filled_usd"] += day_filled
                    result["buy_pending_usd"] += day_unknown
                    result["buy_unknown_usd"] += day_unknown
                    result["gross_daily_buy_usd"] += day_filled + day_unknown
                    outstanding = remaining if reservation_active else Decimal("0")
                    if reservation_active:
                        result["all_in_buy_reserved_usd"] += filled + outstanding
                        result["buy_pending_usd"] += outstanding
                        result["gross_daily_buy_usd"] += outstanding
                        if status == "UNKNOWN":
                            result["buy_unknown_usd"] += outstanding
                    if provisional_cost > 0 or (status == "UNKNOWN" and outstanding > 0):
                        unknown_execution = True
                    cumulative_filled = (
                        cumulative_fills_by_reservation.get(reservation_id, Decimal("0"))
                        if cumulative_reset_at is not None
                        else filled
                    )
                    result["cumulative_buy_usd"] += cumulative_filled + outstanding
                    exposure = confirmed_cost + provisional_cost + outstanding
                    result["aggregate_open_cost_usd"] += confirmed_cost
                    result["aggregate_exposure_usd"] += exposure
                    add_map("per_market_buy_usd", market, exposure)
                    add_map("per_event_buy_usd", event, exposure)
                    if market:
                        market_qty[market] = market_qty.get(market, Decimal("0")) + confirmed_quantity
                        token = str(
                            reservation_detail.get("token_id")
                            or reservation_detail.get("asset_id")
                            or ""
                        ).strip()
                        if not token and event:
                            try:
                                token_row = self._conn.execute(
                                    "SELECT token_id FROM canary_ledger WHERE event_id=? LIMIT 1",
                                    (event,),
                                ).fetchone()
                            except sqlite3.OperationalError:
                                token_row = None
                            if token_row is not None and token_row["token_id"]:
                                token = str(token_row["token_id"]).strip()
                        if confirmed_quantity > 0:
                            owned_key = (market, token)
                            owned_inventory[owned_key] = (
                                owned_inventory.get(owned_key, Decimal("0")) + confirmed_quantity
                            )
                            if not token:
                                unknown_inventory = True
                        if outstanding > 0:
                            result.setdefault("_pending_market_keys", set()).add(market)
                        if confirmed_quantity > 0 or outstanding > 0:
                            slot_key = market or reservation_id
                            open_lot_counts[slot_key] = (
                                open_lot_counts.get(slot_key, 0) + 1
                            )
                elif side == "SELL":
                    token = str(
                        reservation_detail.get("token_id")
                        or reservation_detail.get("asset_id")
                        or ""
                    ).strip()
                    if not token:
                        try:
                            token_row = self._conn.execute(
                                "SELECT token_id FROM canary_position_requests "
                                "WHERE reservation_id=? ORDER BY submitted_at DESC,request_id DESC LIMIT 1",
                                (str(row["reservation_id"]),),
                            ).fetchone()
                        except sqlite3.OperationalError:
                            token_row = None
                        if token_row is not None and token_row["token_id"]:
                            token = str(token_row["token_id"]).strip()
                    if market:
                        market_qty[market] = market_qty.get(market, Decimal("0")) - confirmed_quantity
                        pending_qty = max(
                            Decimal("0"),
                            _risk_decimal(row["quantity"]) - confirmed_quantity,
                        )
                        if reservation_active and pending_qty > 0:
                            market_pending_sell[market] = (
                                market_pending_sell.get(market, Decimal("0")) + pending_qty
                            )
                        if confirmed_quantity > 0:
                            owned_key = (market, token)
                            owned_inventory[owned_key] = (
                                owned_inventory.get(owned_key, Decimal("0")) - confirmed_quantity
                            )
                            if not token:
                                unknown_inventory = True
                        if provisional_quantity > 0:
                            unknown_execution = True
                    linked = linked_lot(row)
                    valid_identity = linked is not None
                    if valid_identity:
                        for fill, confirmed in fills:
                            if not confirmed:
                                continue
                            fill_detail = (
                                _load(fill["detail_json"])
                                if fill["detail_json"]
                                else {}
                            )
                            if not valid_sell_fill(
                                row,
                                fill_detail,
                                _risk_decimal(fill["quantity"]),
                                linked,
                            ):
                                valid_identity = False
                                break
                    if valid_identity:
                        assert linked is not None
                        _request, lot = linked
                        lot_position_id = str(lot["position_id"] or "").strip()
                        if lot_position_id and confirmed_quantity > 0:
                            sell_quantity_by_position[lot_position_id] = (
                                sell_quantity_by_position.get(lot_position_id, Decimal("0"))
                                + confirmed_quantity
                            )
                    for fill, confirmed in fills:
                        if not confirmed:
                            continue
                        fill_detail = _load(fill["detail_json"]) if fill["detail_json"] else {}
                        pnl = detail_pnl(fill_detail)
                        fill_stamp = str(fill["filled_at"])
                        if start <= fill_stamp < end:
                            result["today_realized_pnl_usd"] += pnl
                        loss = -pnl if pnl < 0 else Decimal("0")
                        if loss:
                            result["realized_loss_usd"] += loss
            legacy_rows: list[sqlite3.Row] = []
            legacy_table = self._conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='canary_ledger'"
            ).fetchone()
            if legacy_table is not None and not scoped_lineage:
                legacy_rows = self._conn.execute("SELECT * FROM canary_ledger").fetchall()
            for row in legacy_rows:
                event_id = str(row["event_id"]) if "event_id" in row.keys() and row["event_id"] else None
                if event_id and event_id in new_event_ids:
                    continue
                side = str(row["side"]).upper()
                stamp_value = str(row["timestamp"])
                try:
                    requested = _risk_decimal(row["requested_notional"])
                    quantity = _risk_decimal(row["fill_quantity"] or "0")
                    average = _risk_decimal(row["actual_average_price"] or "0")
                    fees = _risk_decimal(row["fees"] or "0")
                except (KeyError, TypeError, ValueError):
                    continue
                filled = max(Decimal("0"), quantity * average + fees)
                remaining = max(Decimal("0"), requested - filled)
                status = str(row["status"]).upper()
                market = str(row["market_id"]) if row["market_id"] else None
                event = str(row["event_id"]) if row["event_id"] else None
                token = (
                    str(row["token_id"]).strip()
                    if "token_id" in row.keys() and row["token_id"]
                    else ""
                )
                evidence = (
                    _load(row["evidence_json"])
                    if "evidence_json" in row.keys() and row["evidence_json"]
                    else {}
                )
                settlement = _canary_fill_settlement_status(evidence)
                if not settlement and "settlement" in row.keys() and row["settlement"]:
                    settlement = str(row["settlement"]).strip().upper()
                if not settlement and status in _CANARY_CONFIRMED_SETTLEMENT_STATUSES:
                    settlement = status
                confirmed = settlement in _CANARY_CONFIRMED_SETTLEMENT_STATUSES
                if start <= stamp_value < end:
                    result["submitted_orders"] += 1
                    if side == "BUY":
                        if confirmed:
                            result["buy_filled_usd"] += filled
                            result["gross_daily_buy_usd"] += filled
                        elif filled > 0:
                            result["buy_pending_usd"] += filled
                            result["buy_unknown_usd"] += filled
                            result["gross_daily_buy_usd"] += filled
                if side == "BUY":
                    outstanding = remaining if status in active_statuses else Decimal("0")
                    if not confirmed and filled > 0:
                        unknown_execution = True
                    cumulative_filled = (
                        Decimal("0")
                        if cumulative_reset_at is not None and stamp_value < cumulative_reset_at
                        else filled
                    )
                    result["cumulative_buy_usd"] += cumulative_filled + outstanding
                    if status in active_statuses:
                        result["all_in_buy_reserved_usd"] += filled + outstanding
                        result["buy_pending_usd"] += outstanding
                        result["gross_daily_buy_usd"] += outstanding
                        if status == "UNKNOWN":
                            result["buy_unknown_usd"] += outstanding
                    owned_cost = filled if confirmed else Decimal("0")
                    result["aggregate_open_cost_usd"] += owned_cost
                    exposure = filled + outstanding
                    result["aggregate_exposure_usd"] += exposure
                    add_map("per_market_buy_usd", market, exposure)
                    add_map("per_event_buy_usd", event, exposure)
                    if market and confirmed:
                        market_qty[market] = market_qty.get(market, Decimal("0")) + quantity
                        owned_key = (market, token)
                        owned_inventory[owned_key] = (
                            owned_inventory.get(owned_key, Decimal("0")) + quantity
                        )
                        if not token:
                            unknown_inventory = True
                        if quantity > 0:
                            slot_key = market or str(row["event_id"])
                            open_lot_counts[slot_key] = open_lot_counts.get(slot_key, 0) + 1
                    if market and outstanding > 0:
                        result.setdefault("_pending_market_keys", set()).add(market)
                elif side == "SELL":
                    if not confirmed:
                        unknown_execution = unknown_execution or quantity > 0
                        continue
                    if market:
                        market_qty[market] = market_qty.get(market, Decimal("0")) - quantity
                        owned_key = (market, token)
                        owned_inventory[owned_key] = (
                            owned_inventory.get(owned_key, Decimal("0")) - quantity
                        )
                        if not token:
                            unknown_inventory = True
                    pnl = detail_pnl(
                        evidence,
                        {"realized_pnl": row["realized_pnl"] if "realized_pnl" in row.keys() else "0"},
                    )
                    if start <= stamp_value < end:
                        result["today_realized_pnl_usd"] += pnl
                    result["realized_loss_usd"] += -pnl if pnl < 0 else Decimal("0")

            released_open_cost = Decimal("0")
            released_by_market: dict[str, Decimal] = {}
            released_by_event: dict[str, Decimal] = {}
            # Aggregate confirmations once per lot.  A lot may be closed by
            # multiple SELL reservations or multiple partial fills; applying
            # the lot's sold quantity once keeps the release idempotent and
            # prevents duplicate reservations from releasing its basis twice.
            for position_id, confirmed_quantity in sell_quantity_by_position.items():
                lot = lot_by_position.get(position_id)
                if lot is None:
                    continue
                try:
                    total_quantity = _risk_decimal(
                        lot["quantity"],
                        name="owned lot quantity",
                        nonnegative=True,
                    )
                    sold_quantity = _risk_decimal(
                        lot["sold_quantity"],
                        name="owned lot sold quantity",
                        nonnegative=True,
                    )
                    cost_basis = _risk_decimal(
                        lot["cost_basis"],
                        name="owned lot cost basis",
                        nonnegative=True,
                    )
                except (InvalidOperation, TypeError, ValueError):
                    continue
                # A lot reporting more sold quantity than it owns is an
                # unresolved/excess execution, not permission to release all
                # of its basis.
                if (
                    total_quantity <= 0
                    or sold_quantity <= 0
                    or sold_quantity > total_quantity
                    or confirmed_quantity <= 0
                ):
                    continue
                released_quantity = min(sold_quantity, confirmed_quantity)
                release = min(
                    cost_basis,
                    cost_basis * released_quantity / total_quantity,
                )
                if release <= 0:
                    continue
                released_open_cost += release
                market = str(lot["market_id"] or "").strip()
                event = str(lot["event_id"] or "").strip()
                if market:
                    released_by_market[market] = (
                        released_by_market.get(market, Decimal("0")) + release
                    )
                if event:
                    released_by_event[event] = (
                        released_by_event.get(event, Decimal("0")) + release
                    )
            result["aggregate_open_cost_usd"] = max(
                Decimal("0"),
                result["aggregate_open_cost_usd"] - released_open_cost,
            )
            result["aggregate_exposure_usd"] = max(
                Decimal("0"),
                result["aggregate_exposure_usd"] - released_open_cost,
            )
            for market, release in released_by_market.items():
                if market in result["per_market_buy_usd"]:
                    result["per_market_buy_usd"][market] = max(
                        Decimal("0"),
                        result["per_market_buy_usd"][market] - release,
                    )
            for event, release in released_by_event.items():
                if event in result["per_event_buy_usd"]:
                    result["per_event_buy_usd"][event] = max(
                        Decimal("0"),
                        result["per_event_buy_usd"][event] - release,
                    )

            flow_rows = self._conn.execute(
                "SELECT * FROM canary_risk_cashflows"
            ).fetchall()
            if any(value not in (None, "") for value in lineage_filter.values()):
                flow_rows = [flow for flow in flow_rows if accounting_lineage_matches(flow)]
            for flow in flow_rows:
                kind = str(flow["kind"]).upper()
                amount = _risk_decimal(flow["amount"])
                if kind in {"EXTERNAL", "DEPOSIT", "WITHDRAWAL"}:
                    result["external_flow_usd"] += amount
                elif kind == "EQUITY_LOSS":
                    result["equity_status"] = "KNOWN"
                    if amount > 0:
                        result["equity_loss_usd"] += amount
            # Equity evidence is known only for a flat confirmed ledger or
            # when every open exact-token position has one fresh liquidation
            # mark.  Repeated mark snapshots are collapsed above by identity.
            open_inventory = {
                key: quantity
                for key, quantity in owned_inventory.items()
                if quantity > 0
            }
            mark_quantities: dict[tuple[str, str], Decimal] = {}
            mark_loss = Decimal("0")
            marks_valid = True
            for mark in latest_marks.values():
                market = str(mark["market_id"]).strip()
                token = str(mark["token_id"]).strip()
                source = str(mark["source"] or "").strip()
                if not market or not token or str(mark["side"]).upper() != "SELL" or not source:
                    marks_valid = False
                    continue
                stamp = _parse_datetime(mark["observed_at"])
                if stamp is None:
                    marks_valid = False
                    continue
                age = (observed - ensure_utc(stamp)).total_seconds()
                if age < 0 or age > _CANARY_EQUITY_MARK_MAX_AGE_SECONDS:
                    marks_valid = False
                    continue
                quantity = _risk_decimal(mark["quantity"], name="mark quantity", nonnegative=True)
                price = _risk_decimal(mark["mark_price"], name="mark price", nonnegative=True)
                fee = _risk_decimal(mark["mark_fee"], name="mark fee", nonnegative=True)
                basis = _risk_decimal(mark["cost_basis_usd"], name="mark cost basis", nonnegative=True)
                identity = (market, token)
                mark_quantities[identity] = mark_quantities.get(identity, Decimal("0")) + quantity
                mark_loss += max(Decimal("0"), basis - (quantity * price - fee))
            for identity, quantity in open_inventory.items():
                market, token = identity
                if not market or not token or mark_quantities.get(identity, Decimal("0")) != quantity:
                    marks_valid = False
            if set(mark_quantities) - set(open_inventory):
                marks_valid = False
            if not open_inventory:
                result["equity_status"] = "UNKNOWN" if unknown_execution else "KNOWN"
            elif marks_valid and not unknown_inventory and not unknown_execution:
                result["equity_status"] = "KNOWN"
            else:
                result["equity_status"] = "UNKNOWN"
            result["equity_loss_usd"] = max(
                result["equity_loss_usd"],
                result["realized_loss_usd"] + mark_loss,
            )
            result["open_quantity_by_market"] = {
                key: max(Decimal("0"), value) for key, value in market_qty.items()
            }
            result["pending_sell_quantity_by_market"] = market_pending_sell
            pending_keys = set(result.pop("_pending_market_keys", set()))
            open_market_keys = {
                key for key, value in market_qty.items() if value > 0
            } | pending_keys
            result["open_market_ids"] = sorted(open_market_keys)
            result["open_positions"] = len(open_market_keys)
            result["open_lot_slots"] = sum(
                count
                for key, count in open_lot_counts.items()
                if key in open_market_keys or key not in market_qty
            )
            rolling_budgets: dict[str, Decimal] = {}
            current_selection = None
            if lineage_filter.get("portfolio_selection_id") in (None, ""):
                current_selection = self._conn.execute(
                    "SELECT selection.global_budget,selection.risk_config_id,"
                    "selection.risk_config_generation,selection.risk_config_hash "
                    "FROM portfolio_current_selection pointer "
                    "JOIN portfolio_selections selection "
                    "ON selection.portfolio_selection_id=pointer.portfolio_selection_id "
                    "WHERE pointer.pointer_id='current'"
                ).fetchone()
            for row in reservation_rows:
                if str(row["side"] or "").upper() != "BUY":
                    continue
                row_lineage = _canary_lineage_from_row(row)
                selection_id = row_lineage.get("portfolio_selection_id")
                strategy_id = row_lineage.get("strategy_version_id")
                if not selection_id or not strategy_id:
                    continue
                committed = self._rolling_open_buy_commitment_locked(row)
                if committed <= 0:
                    continue
                result["rolling_global_reserved_usd"] += committed
                strategy_reserved = result["rolling_strategy_reserved_usd"]
                strategy_reserved[strategy_id] = strategy_reserved.get(
                    strategy_id, Decimal("0")
                ) + committed
                if row_lineage.get("allocation") not in (None, ""):
                    result["rolling_strategy_allocations"].setdefault(
                        strategy_id,
                        _risk_decimal(
                            row_lineage["allocation"],
                            name="allocation",
                            nonnegative=True,
                        ),
                    )
                if selection_id not in rolling_budgets:
                    selection = self._conn.execute(
                        "SELECT global_budget FROM portfolio_selections "
                        "WHERE portfolio_selection_id=?",
                        (selection_id,),
                    ).fetchone()
                    rolling_budgets[selection_id] = (
                        _risk_decimal(selection["global_budget"], nonnegative=True)
                        if selection is not None
                        else Decimal("0")
                    )
            if current_selection is not None:
                result["rolling_global_budget_usd"] = _risk_decimal(
                    current_selection["global_budget"],
                    nonnegative=True,
                )
            elif rolling_budgets:
                result["rolling_global_budget_usd"] = sum(
                    rolling_budgets.values(), Decimal("0")
                )
        for name in (
            "buy_filled_usd", "buy_pending_usd", "buy_unknown_usd", "gross_daily_buy_usd",
            "all_in_buy_reserved_usd", "aggregate_open_cost_usd", "aggregate_exposure_usd",
            "realized_loss_usd", "today_realized_pnl_usd", "equity_loss_usd",
            "external_flow_usd", "cumulative_buy_usd",
        ):
            result[name] = _risk_text(result[name])
        result["rolling_global_reserved_usd"] = _risk_text(result["rolling_global_reserved_usd"])
        result["rolling_global_budget_usd"] = _risk_text(result["rolling_global_budget_usd"])
        result["rolling_strategy_reserved_usd"] = {
            key: _risk_text(value)
            for key, value in result["rolling_strategy_reserved_usd"].items()
        }
        result["rolling_strategy_allocations"] = {
            key: _risk_text(value)
            for key, value in result["rolling_strategy_allocations"].items()
        }
        result["per_market_buy_usd"] = {
            key: _risk_text(value) for key, value in result["per_market_buy_usd"].items()
        }
        result["per_event_buy_usd"] = {
            key: _risk_text(value) for key, value in result["per_event_buy_usd"].items()
        }
        result["open_quantity_by_market"] = {
            key: _risk_text(value) for key, value in result["open_quantity_by_market"].items()
        }
        result["pending_sell_quantity_by_market"] = {
            key: _risk_text(value) for key, value in result["pending_sell_quantity_by_market"].items()
        }
        return result


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
    def load_dataset_catalog_dashboard(
        self,
        dataset_id: str,
        dataset_version: str | None = None,
    ) -> dict[str, Any] | None:
        """Load one dataset catalog row through the bounded dashboard projection."""
        clauses = ["dataset_id=?"]
        values: list[Any] = [str(dataset_id)]
        if dataset_version is not None:
            clauses.append("dataset_version=?")
            values.append(str(dataset_version))
        query = (
            "SELECT dataset_id,dataset_version,provider,instrument,market_type,timeframe,"
            "start_timestamp,end_timestamp,row_count,completeness,missing_ranges_json,"
            "quality,source_type,snapshot_id,created_at,updated_at,metadata_json,"
            "(SELECT COUNT(*) FROM json_each(metadata_json)) AS metadata_key_count,"
            "json_extract(metadata_json,'$.category') AS metadata_category,"
            "json_extract(metadata_json,'$.historical_order_book_available') AS metadata_historical_order_book_available,"
            "json_extract(metadata_json,'$.universe_version') AS metadata_universe_version,"
            "json_array_length(missing_ranges_json) AS missing_range_count "
            "FROM dataset_catalog WHERE "
            + " AND ".join(clauses)
            + " ORDER BY updated_at DESC,created_at DESC,rowid DESC,dataset_version DESC LIMIT 1"
        )
        with self._lock:
            row = self._conn.execute(query, values).fetchone()
        return _dataset_catalog_dashboard_record(row) if row is not None else None

    def list_dataset_catalog_dashboard(
        self,
        *,
        source_type: str | None = None,
        market_type: str | None = None,
        limit: int | None = 1000,
    ) -> list[dict[str, Any]]:
        """List catalog rows with metadata/missing-range payloads bounded."""
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
        query = (
            "SELECT dataset_id,dataset_version,provider,instrument,market_type,timeframe,"
            "start_timestamp,end_timestamp,row_count,completeness,missing_ranges_json,"
            "quality,source_type,snapshot_id,created_at,updated_at,metadata_json,"
            "(SELECT COUNT(*) FROM json_each(metadata_json)) AS metadata_key_count,"
            "json_extract(metadata_json,'$.category') AS metadata_category,"
            "json_extract(metadata_json,'$.historical_order_book_available') AS metadata_historical_order_book_available,"
            "json_extract(metadata_json,'$.universe_version') AS metadata_universe_version,"
            "json_array_length(missing_ranges_json) AS missing_range_count "
            "FROM dataset_catalog"
        )
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY updated_at DESC,dataset_id,dataset_version"
        if limit is not None:
            query += " LIMIT ?"
            values.append(int(limit))
        with self._lock:
            rows = self._conn.execute(query, values).fetchall()
        return [_dataset_catalog_dashboard_record(row) for row in rows]

    # Dataset integrity attestations ---------------------------------
    def load_dataset_integrity_attestation(
        self, dataset_id: str, dataset_version: str | None = None
    ) -> dict[str, Any] | None:
        """Load the exact durable attestation without reconstructing history."""
        clauses = ["dataset_id=?"]
        values: list[Any] = [str(dataset_id)]
        if dataset_version is not None:
            clauses.append("dataset_version=?")
            values.append(str(dataset_version))
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM dataset_integrity_attestation WHERE "
                + " AND ".join(clauses)
                + " ORDER BY verified_at DESC LIMIT 1",
                values,
            ).fetchone()
        if row is None:
            return None
        bindings = _load(row["constituent_bindings_json"])
        return {
            "dataset_id": row["dataset_id"],
            "dataset_version": row["dataset_version"],
            "source_type": row["source_type"],
            "market_type": row["market_type"],
            "row_count": int(row["row_count"]),
            "completeness": float(row["completeness"]),
            "start_timestamp": _parse_datetime(row["start_timestamp"]),
            "end_timestamp": _parse_datetime(row["end_timestamp"]),
            "execution_fidelity": row["execution_fidelity"],
            "historical_execution_fidelity": row["execution_fidelity"],
            "contamination_result": row["contamination_result"],
            "constituent_bindings": bindings if isinstance(bindings, list) else [],
            "provenance_version": row["provenance_version"],
            "policy_version": row["policy_version"],
            "reason": row["reason"] if "reason" in row.keys() and row["reason"] else None,
            "attestation_hash": row["attestation_hash"],
            "verified_at": _parse_datetime(row["verified_at"]),
            "status": row["status"],
        }

    get_dataset_integrity_attestation = load_dataset_integrity_attestation

    def _prepare_dataset_integrity_attestation(
        self,
        dataset_id: str,
        dataset_version: str,
        attestation: Mapping[str, Any] | None = None,
        **fields: Any,
    ) -> tuple[Any, ...]:
        """Normalize one attestation and return its durable column values."""
        body = dict(attestation or {})
        body.update(fields)
        identifier = str(dataset_id).strip()
        version = str(dataset_version).strip()
        if not identifier or not version:
            raise ValueError("attestation dataset identity is required")
        status = str(body.get("status") or _DATASET_ATTESTATION_STATUS_STALE).upper()
        if status not in {_DATASET_ATTESTATION_STATUS_CURRENT, _DATASET_ATTESTATION_STATUS_STALE}:
            raise ValueError("attestation status must be CURRENT or STALE")
        source_type = str(body.get("source_type") or "").strip().upper()
        market_type = str(body.get("market_type") or "").strip().lower()
        row_count = body.get("row_count", 0)
        completeness = body.get("completeness", 0.0)
        if isinstance(row_count, bool) or not isinstance(row_count, int) or row_count < 0:
            raise ValueError("attestation row_count must be a non-negative integer")
        completeness = float(completeness)
        if not math.isfinite(completeness) or not 0.0 <= completeness <= 1.0:
            raise ValueError("attestation completeness must be in [0, 1]")
        bindings = body.get("constituent_bindings", body.get("constituents", []))
        if not isinstance(bindings, list):
            raise ValueError("attestation constituent_bindings must be a list")
        if len(bindings) > _MAX_DATASET_ATTESTATION_CONSTITUENTS:
            raise ValueError("attestation constituent_bindings exceeds 1000 entries")
        verified_at = body.get("verified_at")
        verified_iso = _iso(verified_at) if isinstance(verified_at, datetime) else str(verified_at or _now_iso())
        start = body.get("start_timestamp")
        end = body.get("end_timestamp")
        start_iso = _iso(start) if isinstance(start, datetime) else (str(start) if start is not None else None)
        end_iso = _iso(end) if isinstance(end, datetime) else (str(end) if end is not None else None)
        reason = str(body.get("reason") or "").strip().upper()
        canonical = {
            "dataset_id": identifier,
            "dataset_version": version,
            "source_type": source_type,
            "market_type": market_type,
            "row_count": int(row_count),
            "completeness": completeness,
            "start_timestamp": start_iso,
            "end_timestamp": end_iso,
            "execution_fidelity": str(body.get("execution_fidelity") or body.get("historical_execution_fidelity") or "UNKNOWN").upper(),
            "contamination_result": str(body.get("contamination_result") or "FAIL").upper(),
            "constituent_bindings": bindings,
            "provenance_version": str(body.get("provenance_version") or ""),
            "policy_version": str(body.get("policy_version") or ""),
        }
        if reason:
            canonical["reason"] = reason
        expected_hash = "sha256:" + hashlib.sha256(_dump(canonical).encode("utf-8")).hexdigest()
        attestation_hash = str(body.get("attestation_hash") or expected_hash)
        if attestation_hash != expected_hash:
            raise ValueError("attestation_hash does not match canonical attestation")
        return (
            identifier,
            version,
            source_type,
            market_type,
            int(row_count),
            completeness,
            start_iso,
            end_iso,
            canonical["execution_fidelity"],
            canonical["contamination_result"],
            _dump(bindings),
            canonical["provenance_version"],
            canonical["policy_version"],
            reason,
            attestation_hash,
            verified_iso,
            status,
        )

    def save_dataset_integrity_attestation(
        self,
        dataset_id: str,
        dataset_version: str,
        attestation: Mapping[str, Any] | None = None,
        **fields: Any,
    ) -> bool:
        """Publish one attestation projection in a short writer transaction."""
        values = self._prepare_dataset_integrity_attestation(
            dataset_id,
            dataset_version,
            attestation,
            **fields,
        )
        identifier, version = values[0], values[1]

        def operation() -> bool:
            with self._write_context():
                self._conn.execute(
                    "INSERT INTO dataset_integrity_attestation("
                    "dataset_id,dataset_version,source_type,market_type,row_count,completeness,"
                    "start_timestamp,end_timestamp,execution_fidelity,contamination_result,"
                    "constituent_bindings_json,provenance_version,policy_version,reason,attestation_hash,verified_at,status"
                    ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
                    "ON CONFLICT(dataset_id,dataset_version) DO UPDATE SET "
                    "source_type=excluded.source_type,market_type=excluded.market_type,row_count=excluded.row_count,"
                    "completeness=excluded.completeness,start_timestamp=excluded.start_timestamp,"
                    "end_timestamp=excluded.end_timestamp,execution_fidelity=excluded.execution_fidelity,"
                    "contamination_result=excluded.contamination_result,constituent_bindings_json=excluded.constituent_bindings_json,"
                    "provenance_version=excluded.provenance_version,policy_version=excluded.policy_version,"
                    "reason=excluded.reason,attestation_hash=excluded.attestation_hash,verified_at=excluded.verified_at,"
                    "status=excluded.status",
                    values,
                )
            return True

        return bool(sqlite_retry(operation, operation_name=f"save dataset attestation {identifier}/{version}"))

    def invalidate_dataset_integrity_attestation(
        self, dataset_id: str, dataset_version: str, *, reason: str | None = None
    ) -> bool:
        """Fence a prior attestation after immutable identity changes."""
        identifier = str(dataset_id).strip()
        version = str(dataset_version).strip()
        reason_value = str(reason or "").strip().upper()

        def operation() -> bool:
            with self._write_context():
                row = self._conn.execute(
                    "SELECT * FROM dataset_integrity_attestation "
                    "WHERE dataset_id=? AND dataset_version=?",
                    (identifier, version),
                ).fetchone()
                if row is None or row["status"] == _DATASET_ATTESTATION_STATUS_STALE:
                    return False
                if reason_value:
                    bindings = _load(row["constituent_bindings_json"])
                    canonical = {
                        "dataset_id": row["dataset_id"],
                        "dataset_version": row["dataset_version"],
                        "source_type": row["source_type"],
                        "market_type": row["market_type"],
                        "row_count": int(row["row_count"]),
                        "completeness": float(row["completeness"]),
                        "start_timestamp": row["start_timestamp"],
                        "end_timestamp": row["end_timestamp"],
                        "execution_fidelity": row["execution_fidelity"],
                        "contamination_result": row["contamination_result"],
                        "constituent_bindings": bindings if isinstance(bindings, list) else [],
                        "provenance_version": row["provenance_version"],
                        "policy_version": row["policy_version"],
                        "reason": reason_value,
                    }
                    attestation_hash = "sha256:" + hashlib.sha256(_dump(canonical).encode("utf-8")).hexdigest()
                    changed = self._conn.execute(
                        "UPDATE dataset_integrity_attestation SET status=?,reason=?,attestation_hash=? "
                        "WHERE dataset_id=? AND dataset_version=? AND status<>?",
                        (
                            _DATASET_ATTESTATION_STATUS_STALE,
                            reason_value,
                            attestation_hash,
                            identifier,
                            version,
                            _DATASET_ATTESTATION_STATUS_STALE,
                        ),
                    ).rowcount
                else:
                    changed = self._conn.execute(
                        "UPDATE dataset_integrity_attestation SET status=? "
                        "WHERE dataset_id=? AND dataset_version=? AND status<>?",
                        (
                            _DATASET_ATTESTATION_STATUS_STALE,
                            identifier,
                            version,
                            _DATASET_ATTESTATION_STATUS_STALE,
                        ),
                    ).rowcount
            return bool(changed)

        return bool(sqlite_retry(operation, operation_name=f"invalidate dataset attestation {identifier}/{version}"))

    def _snapshot_read_connection(self) -> sqlite3.Connection:
        """Open a dedicated read snapshot; never share the writer handle."""
        # A regular filesystem path is already authoritative.  Avoid asking
        # the writer connection for PRAGMA database_list in this common case:
        # that call takes ``self._lock`` and would put a dashboard read behind
        # an unrelated long-lived writer transaction.
        path = str(self.path)
        if path not in {":memory:", ""} and not path.startswith("file:"):
            filename = path
            connect_target = path
            connect_uri = False
        else:
            filename = self._database_filename()
            connect_target = path if path.startswith("file:") else filename
            connect_uri = path.startswith("file:")
        if filename and filename not in {":memory:", ""} and not filename.startswith("file::memory:"):
            connection = sqlite3.connect(
                connect_target,
                timeout=self._sqlite_timeout_seconds,
                check_same_thread=False,
                uri=connect_uri,
            )
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA query_only=ON")
            return connection
        # ``:memory:`` has no independently addressable database.  Serialize
        # while holding the lock, then release it before doing any historical
        # scan.  ``Connection.backup`` waits forever when the source owns an
        # active SAVEPOINT (the normal queue-processing transaction).
        connection = sqlite3.connect(":memory:", check_same_thread=False)
        connection.row_factory = sqlite3.Row
        with self._lock:
            connection.deserialize(self._conn.serialize())
        connection.execute("PRAGMA query_only=ON")
        return connection

    @staticmethod
    def _attestation_values(
        connection: sqlite3.Connection,
        dataset_id: str,
        dataset_version: str,
        market_id: str | None = None,
    ) -> tuple[bool, int, datetime | None, datetime | None, str, bool]:
        """Read exact immutable/legacy rows and return bounded integrity facts."""
        dataset_row = connection.execute(
            "SELECT payload_json,quality FROM datasets WHERE dataset_id=? AND version=?",
            (dataset_id, dataset_version),
        ).fetchone()
        values: list[Mapping[str, Any]] = []
        fidelity = "UNKNOWN"
        if dataset_row is not None:
            try:
                loaded = _load(dataset_row["payload_json"])
            except (TypeError, ValueError, json.JSONDecodeError):
                return False, 0, None, None, "UNKNOWN", False
            if not isinstance(loaded, Sequence) or isinstance(loaded, (str, bytes, Mapping)):
                return False, 0, None, None, "UNKNOWN", False
            values = [item for item in loaded if isinstance(item, Mapping)]
            if len(values) != len(loaded):
                return False, len(values), None, None, "UNKNOWN", False
            fidelity = str(dataset_row["quality"] or "UNKNOWN").upper()
        elif market_id is not None:
            rows = connection.execute(
                "SELECT snapshot_id,source_timestamp,observed_at,payload_json,quality "
                "FROM polymarket_snapshots WHERE market_id=? ORDER BY source_timestamp,snapshot_id",
                (market_id,),
            ).fetchall()
            historical: list[Mapping[str, Any]] = []
            identities: list[dict[str, Any]] = []
            for row in rows:
                try:
                    payload = _load(row["payload_json"])
                except (TypeError, ValueError, json.JSONDecodeError):
                    return False, 0, None, None, "UNKNOWN", False
                if not isinstance(payload, Mapping):
                    return False, 0, None, None, "UNKNOWN", False
                if str(payload.get("source_type", "")).strip().upper() != "HISTORICAL":
                    continue
                timestamp = _parse_datetime(row["source_timestamp"])
                if timestamp is None:
                    return False, 0, None, None, "UNKNOWN", False
                if payload.get("market_id") is not None and str(payload["market_id"]).strip() != market_id:
                    return False, 0, None, None, "UNKNOWN", False
                price = payload.get("price", payload.get("p", payload.get("yes_mid", payload.get("value"))))
                try:
                    price_value = float(price)
                except (TypeError, ValueError):
                    return False, 0, None, None, "UNKNOWN", False
                if not math.isfinite(price_value) or not 0.0 <= price_value <= 1.0:
                    return False, 0, None, None, "UNKNOWN", False
                token_id = str(payload.get("token_id", payload.get("asset_id", market_id)) or market_id)
                identity: dict[str, Any] = {"timestamp": timestamp, "price": price_value, "token_id": token_id}
                order_book = payload.get("order_book", payload.get("book"))
                if order_book is not None:
                    identity["order_book"] = order_book
                identities.append(identity)
                historical.append(payload)
                fidelity = str(row["quality"] or payload.get("quality") or payload.get("research_quality") or "UNKNOWN").upper()
            if dataset_version.startswith("sha256:"):
                expected = "sha256:" + hashlib.sha256(_dump(identities).encode("utf-8")).hexdigest()
                if expected != dataset_version:
                    return False, len(historical), None, None, fidelity, False
            values = historical
        else:
            return False, 0, None, None, "UNKNOWN", False
        timestamps: list[datetime] = []
        contaminated = False
        for value in values:
            source_type = str(value.get("source_type", "HISTORICAL")).strip().upper()
            if source_type != "HISTORICAL":
                contaminated = True
            stamp = _parse_datetime(value.get("source_timestamp", value.get("timestamp", value.get("time"))))
            if stamp is None:
                return False, len(values), None, None, fidelity, contaminated
            timestamps.append(stamp)
        return (
            bool(values) and not contaminated,
            len(values),
            min(timestamps) if timestamps else None,
            max(timestamps) if timestamps else None,
            fidelity,
            contaminated,
        )

    def _verify_dataset_integrity_snapshot(
        self, connection: sqlite3.Connection, dataset_id: str, dataset_version: str
    ) -> dict[str, Any]:
        row = connection.execute(
            "SELECT * FROM dataset_catalog WHERE dataset_id=? AND dataset_version=?",
            (dataset_id, dataset_version),
        ).fetchone()
        now = _now_iso()
        if row is None:
            canonical = {
                "dataset_id": dataset_id,
                "dataset_version": dataset_version,
                "source_type": "",
                "market_type": "",
                "row_count": 0,
                "completeness": 0.0,
                "start_timestamp": None,
                "end_timestamp": None,
                "execution_fidelity": "UNKNOWN",
                "contamination_result": "FAIL",
                "constituent_bindings": [],
                "provenance_version": "",
                "policy_version": "",
                "reason": "DATASET_CATALOG_NOT_FOUND",
            }
            return {
                **canonical,
                "attestation_hash": "sha256:" + hashlib.sha256(_dump(canonical).encode("utf-8")).hexdigest(),
                "verified_at": now,
                "status": _DATASET_ATTESTATION_STATUS_STALE,
                "reason": "DATASET_CATALOG_NOT_FOUND",
            }
        catalog = _dataset_catalog_record(row)
        metadata = catalog.get("metadata") if isinstance(catalog.get("metadata"), Mapping) else {}
        source_type = str(catalog.get("source_type") or "").upper()
        market_type = str(catalog.get("market_type") or "").lower()
        expected_count = catalog.get("row_count")
        completeness = float(catalog.get("completeness") or 0.0)
        fidelity = str(metadata.get("research_quality") or catalog.get("quality") or "UNKNOWN").upper()
        if metadata.get("historical_order_book_available") is True:
            fidelity = "TIMESTAMPED_DEPTH"
        bindings: list[dict[str, Any]] = []
        reasons: list[str] = []
        observed_count = 0
        observed_start: datetime | None = None
        observed_end: datetime | None = None
        contaminated = False
        market_versions = metadata.get("market_versions")
        if isinstance(market_versions, Sequence) and not isinstance(market_versions, (str, bytes)):
            if len(market_versions) > _MAX_DATASET_ATTESTATION_CONSTITUENTS:
                reasons.append("CONSTITUENT_LIMIT_EXCEEDED")
            for item in market_versions[:_MAX_DATASET_ATTESTATION_CONSTITUENTS]:
                if not isinstance(item, Mapping):
                    reasons.append("CONSTITUENT_BINDING_INVALID")
                    continue
                market_id = str(item.get("market_id") or "").strip()
                constituent_id = str(item.get("dataset_id") or (f"prediction:{market_id}" if market_id else "")).strip()
                constituent_version = str(item.get("dataset_version") or item.get("version") or "").strip()
                item_count = item.get("row_count", item.get("records"))
                if not market_id or constituent_id != f"prediction:{market_id}" or not constituent_version:
                    reasons.append("CONSTITUENT_BINDING_INVALID")
                    continue
                if isinstance(item_count, bool) or not isinstance(item_count, int) or item_count < 0:
                    reasons.append("CONSTITUENT_ROW_COUNT_INVALID")
                    continue
                constituent_row = connection.execute(
                    "SELECT * FROM dataset_catalog WHERE dataset_id=? AND dataset_version=?",
                    (constituent_id, constituent_version),
                ).fetchone()
                if constituent_row is None:
                    reasons.append("CONSTITUENT_CATALOG_NOT_FOUND")
                    continue
                constituent = _dataset_catalog_record(constituent_row)
                if (
                    str(constituent.get("source_type") or "").upper() != "HISTORICAL"
                    or str(constituent.get("market_type") or "").lower() != "prediction"
                    or int(constituent.get("row_count") or -1) != item_count
                ):
                    reasons.append("CONSTITUENT_CATALOG_MISMATCH")
                    continue
                ok, count, start, end, constituent_fidelity, is_contaminated = self._attestation_values(
                    connection, constituent_id, constituent_version, market_id
                )
                if not ok or count != item_count:
                    reasons.append("CONSTITUENT_ROWS_INVALID")
                if is_contaminated:
                    contaminated = True
                if start is not None:
                    observed_start = start if observed_start is None else min(observed_start, start)
                    observed_end = end if observed_end is None else max(observed_end, end)
                bindings.append(
                    {
                        "dataset_id": constituent_id,
                        "dataset_version": constituent_version,
                        "row_count": int(item_count),
                    }
                )
                if constituent_fidelity == "TIMESTAMPED_DEPTH":
                    fidelity = "TIMESTAMPED_DEPTH"
                observed_count += count
        else:
            market_id = str(metadata.get("market_id") or "").strip() or None
            ok, observed_count, observed_start, observed_end, observed_fidelity, is_contaminated = self._attestation_values(
                connection, dataset_id, dataset_version, market_id
            )
            if not ok:
                reasons.append("DATASET_ROWS_INVALID")
            contaminated = contaminated or is_contaminated
            if observed_fidelity != "UNKNOWN":
                fidelity = observed_fidelity
        start = _parse_datetime(catalog.get("start_timestamp"))
        end = _parse_datetime(catalog.get("end_timestamp"))
        if source_type != "HISTORICAL" or market_type != "prediction":
            reasons.append("DATASET_PROVENANCE_INVALID")
        if isinstance(expected_count, bool) or not isinstance(expected_count, int) or expected_count != observed_count:
            reasons.append("ROW_COUNT_MISMATCH")
        if completeness < 1.0:
            reasons.append("INCOMPLETE_DATASET")
        if catalog.get("missing_ranges"):
            reasons.append("INCOMPLETE_DATASET")
        if expected_count and (start is None or end is None or observed_start != start or observed_end != end):
            reasons.append("BOUNDS_MISMATCH")
        if contaminated:
            reasons.append("FORWARD_CONTAMINATION")
        # Contamination is the strongest exact failure and must survive the
        # bounded attestation projection even when row parsing also failed.
        failure_reason = "FORWARD_CONTAMINATION" if contaminated else (reasons[0] if reasons else "")
        canonical = {
            "dataset_id": dataset_id,
            "dataset_version": dataset_version,
            "source_type": source_type,
            "market_type": market_type,
            "row_count": int(expected_count) if isinstance(expected_count, int) and not isinstance(expected_count, bool) else 0,
            "completeness": completeness,
            "start_timestamp": _iso(start) if start is not None else None,
            "end_timestamp": _iso(end) if end is not None else None,
            "execution_fidelity": fidelity,
            "contamination_result": "FAIL" if contaminated or reasons else "PASS",
            "constituent_bindings": bindings,
            "provenance_version": str(metadata.get("provenance_version") or metadata.get("dataset_provenance_version") or "dataset-provenance-v1"),
            "policy_version": str(metadata.get("policy_version") or "prediction-integrity-v1"),
        }
        if failure_reason:
            canonical["reason"] = failure_reason
        identity_payload = {
            key: catalog.get(key)
            for key in (
                "dataset_id",
                "dataset_version",
                "provider",
                "instrument",
                "market_type",
                "timeframe",
                "start_timestamp",
                "end_timestamp",
                "row_count",
                "completeness",
                "missing_ranges",
                "quality",
                "source_type",
                "snapshot_id",
                "metadata",
            )
        }
        return {
            **canonical,
            "attestation_hash": "sha256:" + hashlib.sha256(_dump(canonical).encode("utf-8")).hexdigest(),
            "verified_at": now,
            "status": _DATASET_ATTESTATION_STATUS_CURRENT if not reasons else _DATASET_ATTESTATION_STATUS_STALE,
            "reason": failure_reason or None,
            "_catalog_identity": hashlib.sha256(_dump(identity_payload).encode("utf-8")).hexdigest(),
        }

    def verify_dataset_integrity_attestation(
        self, dataset_id: str, dataset_version: str, *, force: bool = False
    ) -> dict[str, Any]:
        """Verify legacy rows outside the writer lock, then publish with CAS."""
        identifier, version = str(dataset_id).strip(), str(dataset_version).strip()
        if not identifier or not version:
            raise ValueError("dataset identity is required")

        # This token is the publication fence.  The snapshot verifier may run
        # for a long time, so every mutable row field used by the projection
        # must still be the same before an update is allowed.
        with self._lock:
            existing_row = self._conn.execute(
                "SELECT dataset_id,dataset_version,attestation_hash,verified_at,status,policy_version "
                "FROM dataset_integrity_attestation WHERE dataset_id=? AND dataset_version=?",
                (identifier, version),
            ).fetchone()
        existing_token = (
            tuple(existing_row[column] for column in ("dataset_id", "dataset_version", "attestation_hash", "verified_at", "status", "policy_version"))
            if existing_row is not None
            else None
        )
        if not force and existing_row is not None and str(existing_row["status"]) == _DATASET_ATTESTATION_STATUS_CURRENT:
            existing = self.load_dataset_integrity_attestation(identifier, version)
            if isinstance(existing, Mapping):
                return dict(existing)

        filename = self._database_filename()
        if (
            (not filename or filename == ":memory:" or filename.startswith("file::memory:"))
            and self._transaction_depth
        ):
            # SQLite cannot back up an in-memory connection while that same
            # connection owns an open write transaction.  Tests and embedded
            # callers may verify the transaction-local snapshot directly.
            with self._lock:
                result = self._verify_dataset_integrity_snapshot(
                    self._conn,
                    identifier,
                    version,
                )
        else:
            snapshot = self._snapshot_read_connection()
            try:
                result = self._verify_dataset_integrity_snapshot(
                    snapshot,
                    identifier,
                    version,
                )
            finally:
                snapshot.close()

        snapshot_identity = result.get("_catalog_identity")
        requested_policy = str(result.get("policy_version") or "")
        body: dict[str, Any]
        cas_lost = False
        winner_exists = False
        current_catalog_identity: str | None = None

        with self.transaction(immediate=True):
            current = self._conn.execute(
                "SELECT * FROM dataset_catalog WHERE dataset_id=? AND dataset_version=?",
                (identifier, version),
            ).fetchone()
            if current is None:
                result["status"] = _DATASET_ATTESTATION_STATUS_STALE
                result["reason"] = result.get("reason") or "DATASET_CATALOG_NOT_FOUND"
            else:
                current_identity = _dataset_catalog_record(current)
                identity_payload = {
                    key: current_identity.get(key)
                    for key in (
                        "dataset_id",
                        "dataset_version",
                        "provider",
                        "instrument",
                        "market_type",
                        "timeframe",
                        "start_timestamp",
                        "end_timestamp",
                        "row_count",
                        "completeness",
                        "missing_ranges",
                        "quality",
                        "source_type",
                        "snapshot_id",
                        "metadata",
                    )
                }
                current_catalog_identity = hashlib.sha256(_dump(identity_payload).encode("utf-8")).hexdigest()
                if snapshot_identity is not None and current_catalog_identity != snapshot_identity:
                    result["status"] = _DATASET_ATTESTATION_STATUS_STALE
                    result["reason"] = result.get("reason") or "DATASET_CATALOG_IDENTITY_CHANGED"

            body = dict(result)
            body.pop("_catalog_identity", None)
            values = self._prepare_dataset_integrity_attestation(identifier, version, body)
            if existing_token is None:
                changed = self._conn.execute(
                    "INSERT INTO dataset_integrity_attestation("
                    "dataset_id,dataset_version,source_type,market_type,row_count,completeness,"
                    "start_timestamp,end_timestamp,execution_fidelity,contamination_result,"
                    "constituent_bindings_json,provenance_version,policy_version,reason,attestation_hash,verified_at,status"
                    ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
                    "ON CONFLICT(dataset_id,dataset_version) DO NOTHING",
                    values,
                ).rowcount
            else:
                changed = self._conn.execute(
                    "UPDATE dataset_integrity_attestation SET "
                    "source_type=?,market_type=?,row_count=?,completeness=?,start_timestamp=?,end_timestamp=?,"
                    "execution_fidelity=?,contamination_result=?,constituent_bindings_json=?,provenance_version=?,"
                    "policy_version=?,reason=?,attestation_hash=?,verified_at=?,status=? "
                    "WHERE dataset_id=? AND dataset_version=? AND attestation_hash=? AND verified_at=? "
                    "AND status=? AND policy_version=?",
                    tuple(values[2:])
                    + (
                        existing_token[0],
                        existing_token[1],
                        existing_token[2],
                        existing_token[3],
                        existing_token[4],
                        existing_token[5],
                    ),
                ).rowcount
            if changed != 1:
                cas_lost = True
                winner_exists = (
                    self._conn.execute(
                        "SELECT 1 FROM dataset_integrity_attestation "
                        "WHERE dataset_id=? AND dataset_version=?",
                        (identifier, version),
                    ).fetchone()
                    is not None
                )

        winner = self.load_dataset_integrity_attestation(identifier, version) if winner_exists else None
        if cas_lost:
            winner_matches_request = (
                isinstance(winner, Mapping)
                and str(winner.get("status") or "").upper() == _DATASET_ATTESTATION_STATUS_CURRENT
                and str(winner.get("policy_version") or "") == requested_policy
                and snapshot_identity is not None
                and current_catalog_identity == snapshot_identity
            )
            if winner_matches_request:
                return dict(winner)
            failed = dict(winner) if isinstance(winner, Mapping) else dict(body)
            failed["status"] = _DATASET_ATTESTATION_STATUS_STALE
            failed["reason"] = "ATTESTATION_CAS_LOST"
            return failed
        return winner or body

    verify_dataset_integrity = verify_dataset_integrity_attestation
    ensure_dataset_integrity = verify_dataset_integrity_attestation
    load_dataset_attestation = load_dataset_integrity_attestation
    save_dataset_attestation = save_dataset_integrity_attestation

    def _create_dataset_attestation_triggers(self) -> None:
        """Stale attestations when either catalog identity or payload identity changes."""
        self._conn.executescript(
            """
            DROP TRIGGER IF EXISTS dataset_catalog_attestation_identity_update;
            DROP TRIGGER IF EXISTS datasets_attestation_identity_update;
            CREATE TRIGGER dataset_catalog_attestation_identity_update
            AFTER UPDATE OF dataset_id,dataset_version,provider,instrument,market_type,timeframe,start_timestamp,end_timestamp,
                row_count,completeness,missing_ranges_json,quality,source_type,snapshot_id,metadata_json
            ON dataset_catalog BEGIN
                UPDATE dataset_integrity_attestation SET status='STALE'
                WHERE (dataset_id=OLD.dataset_id AND dataset_version=OLD.dataset_version)
                   OR (dataset_id=NEW.dataset_id AND dataset_version=NEW.dataset_version);
            END;
            CREATE TRIGGER datasets_attestation_identity_update
            AFTER UPDATE OF dataset_id,version,payload_json,metadata_json,quality
            ON datasets BEGIN
                UPDATE dataset_integrity_attestation SET status='STALE'
                WHERE (dataset_id=OLD.dataset_id AND dataset_version=OLD.version)
                   OR (dataset_id=NEW.dataset_id AND dataset_version=NEW.version);
            END;
            """
        )
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
    def list_dataset_bootstrap_states_dashboard(self, *, limit: int = 20) -> list[dict[str, Any]]:
        """Return bootstrap card fields without hydrating persisted payloads."""
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 0:
            raise ValueError("limit must be a non-negative integer")
        fields = (
            ("selected_symbol", "$.selected_symbol"),
            ("symbol", "$.symbol"),
            ("instrument", "$.instrument"),
            ("timeframe", "$.timeframe"),
            ("status", "$.status"),
            ("requested_start", "$.requested_start"),
            ("requested_end", "$.requested_end"),
            ("next_timestamp", "$.next_timestamp"),
            ("progress", "$.progress"),
            ("progress_fraction", "$.progress_fraction"),
            ("errors", "$.errors"),
            ("dataset_id", "$.dataset_id"),
        )
        paths = ",".join(repr(path) for _, path in fields)
        query = (
            "SELECT dataset_id,provider,instrument,market_type,timeframe,requested_start,"
            "requested_end,next_timestamp,base_version,status,updated_at,"
            "json_extract(CASE WHEN json_valid(payload_json) THEN payload_json ELSE '{}' END,"
            f"{paths}) AS projected_fields_json "
            "FROM dataset_bootstrap_state ORDER BY updated_at DESC,dataset_id DESC LIMIT ?"
        )
        with self._lock:
            rows = self._conn.execute(query, (int(limit),)).fetchall()

        def decode(value: Any) -> Any:
            try:
                return json.loads(value) if value is not None else None
            except (TypeError, ValueError, json.JSONDecodeError):
                return None

        result: list[dict[str, Any]] = []
        for row in rows:
            values = decode(row["projected_fields_json"])
            values = values if isinstance(values, list) else []
            payload = {
                key: value
                for (key, _), value in zip(fields, values)
                if value is not None
            }
            result.append(
                {
                    **payload,
                    "dataset_id": row["dataset_id"],
                    "provider": row["provider"],
                    "instrument": row["instrument"],
                    "market_type": row["market_type"],
                    "timeframe": row["timeframe"],
                    "requested_start": row["requested_start"],
                    "requested_end": row["requested_end"],
                    "next_timestamp": row["next_timestamp"],
                    "base_version": row["base_version"],
                    "status": row["status"],
                    "updated_at": _parse_datetime(row["updated_at"]),
                }
            )
        return result


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
        if len(market_versions) > _MAX_DATASET_ATTESTATION_CONSTITUENTS:
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
    def load_latest_polymarket_snapshots_dashboard(
        self,
        market_ids: Sequence[str] | None = None,
        *,
        source_type: str | None = None,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        """Load latest market snapshots with bounded dashboard payloads."""
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
        result: list[dict[str, Any]] = []
        for row in rows:
            payload, projection = _dashboard_payload_projection(row["payload_json"])
            result.append(
                {
                    "snapshot_id": row["snapshot_id"],
                    "market_id": row["market_id"],
                    "source_timestamp": _parse_datetime(row["source_timestamp"]),
                    "observed_at": _parse_datetime(row["observed_at"]),
                    "payload": payload if isinstance(payload, Mapping) else {},
                    "quality": row["quality"],
                    "source_type": str(row["source_type"]).upper(),
                    **_dashboard_payload_fields(projection),
                }
            )
        return result

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
        if row is None:
            return None
        try:
            state = json.loads(row["state_json"])
        except (TypeError, ValueError, json.JSONDecodeError):
            return {}
        return state if isinstance(state, Mapping) else {}

    def set_collector_state(self, collector_name: str, state: Mapping[str, Any]) -> None:
        with self._write_context():
            self._conn.execute(
                "INSERT INTO collector_state(collector_name,state_json,updated_at) VALUES (?,?,?) "
                "ON CONFLICT(collector_name) DO UPDATE SET state_json=excluded.state_json,updated_at=excluded.updated_at",
                (str(collector_name), _dump(dict(state)), _now_iso()),
            )
    def get_scheduler_state(self, scheduler_name: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT state_json FROM scheduler_state WHERE scheduler_name=?", (str(scheduler_name),)
            ).fetchone()
        if row is None:
            return None
        try:
            state = json.loads(row["state_json"])
        except (TypeError, ValueError, json.JSONDecodeError):
            return {}
        return state if isinstance(state, Mapping) else {}

    def set_scheduler_state(self, scheduler_name: str, state: Mapping[str, Any]) -> None:
        if not str(scheduler_name).strip() or not isinstance(state, Mapping):
            raise ValueError("scheduler_name and state mapping are required")
        with self._write_context():
            self._conn.execute(
                "INSERT INTO scheduler_state(scheduler_name,state_json,updated_at) VALUES (?,?,?) "
                "ON CONFLICT(scheduler_name) DO UPDATE SET state_json=excluded.state_json,updated_at=excluded.updated_at",
                (str(scheduler_name), _dump(dict(state)), _now_iso()),
            )
    def get_operator_config(self, config_key: str, default: Any = None) -> Any:
        key = str(config_key).strip()
        if not key:
            raise ValueError("config_key is required")
        with self._lock:
            row = self._conn.execute(
                "SELECT value_json FROM operator_config WHERE config_key=?", (key,)
            ).fetchone()
        return _load(row["value_json"]) if row is not None else default

    def set_operator_config(self, config_key: str, value: Any) -> None:
        key = str(config_key).strip()
        if not key:
            raise ValueError("config_key is required")
        encoded = _dump(value)
        if len(encoded) > 16_384:
            raise ValueError("operator config value is too large")
        with self._write_context():
            self._conn.execute(
                "INSERT INTO operator_config(config_key,value_json,updated_at) VALUES (?,?,?) "
                "ON CONFLICT(config_key) DO UPDATE SET value_json=excluded.value_json,updated_at=excluded.updated_at",
                (key, encoded, _now_iso()),
            )

    def compare_and_set_operator_config(
        self,
        config_key: str,
        expected: Any,
        value: Any,
    ) -> bool:
        """Atomically replace one operator config value when it is unchanged."""
        key = str(config_key).strip()
        if not key:
            raise ValueError("config_key is required")
        encoded_expected = _dump(expected)
        encoded_value = _dump(value)
        if len(encoded_value) > 16_384:
            raise ValueError("operator config value is too large")
        with self.transaction(immediate=True):
            cursor = self._conn.execute(
                "UPDATE operator_config SET value_json=?,updated_at=? "
                "WHERE config_key=? AND value_json=?",
                (encoded_value, _now_iso(), key, encoded_expected),
            )
            return cursor.rowcount == 1


    def get_operator_job(self, job_name: str) -> dict[str, Any] | None:
        name = str(job_name).strip()
        if not name:
            raise ValueError("job_name is required")
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM operator_jobs WHERE job_name=?", (name,)
            ).fetchone()
        if row is None:
            return None
        return {
            "job_name": row["job_name"],
            "status": row["status"],
            "payload": _load(row["payload_json"]),
            "pid": row["pid"],
            "started_at": _parse_datetime(row["started_at"]),
            "updated_at": _parse_datetime(row["updated_at"]),
            "last_error": row["last_error"],
            "resumable": bool(row["resumable"]),
        }

    def set_operator_job(
        self,
        job_name: str,
        status: str,
        payload: Mapping[str, Any] | None = None,
        *,
        pid: int | None = None,
        started_at: datetime | None = None,
        last_error: str | None = None,
        resumable: bool = False,
        timestamp: datetime | None = None,
    ) -> None:
        name = str(job_name).strip()
        state = str(status).strip().upper()
        if not name or not state:
            raise ValueError("job_name and status are required")
        body = dict(payload or {})
        encoded = _dump(body)
        if len(encoded) > 64_000:
            raise ValueError("operator job payload is too large")
        updated = timestamp or utc_now()
        with self._write_context():
            self._conn.execute(
                "INSERT INTO operator_jobs(job_name,status,payload_json,pid,started_at,updated_at,last_error,resumable) "
                "VALUES (?,?,?,?,?,?,?,?) "
                "ON CONFLICT(job_name) DO UPDATE SET status=excluded.status,payload_json=excluded.payload_json,"
                "pid=excluded.pid,started_at=COALESCE(excluded.started_at,operator_jobs.started_at),"
                "updated_at=excluded.updated_at,last_error=excluded.last_error,resumable=excluded.resumable",
                (
                    name,
                    state,
                    encoded,
                    int(pid) if pid is not None else None,
                    _iso(started_at) if started_at is not None else None,
                    _iso(updated),
                    str(last_error)[:2_000] if last_error else None,
                    1 if resumable else 0,
                ),
            )

    def list_operator_jobs(
        self,
        *,
        job_prefix: str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """List operator jobs with optional SQL-side prefix/row bounds."""
        if limit is not None and (
            isinstance(limit, bool) or not isinstance(limit, int) or limit < 0
        ):
            raise ValueError("limit must be a non-negative integer or None")
        clauses: list[str] = []
        values: list[Any] = []
        if job_prefix is not None:
            prefix = str(job_prefix)
            clauses.append("job_name LIKE ? ESCAPE '\\'")
            escaped_prefix = prefix.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
            values.append(escaped_prefix + "%")
        query = "SELECT * FROM operator_jobs"
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY updated_at DESC,job_name"
        if limit is not None:
            query += " LIMIT ?"
            values.append(int(limit))
        with self._lock:
            rows = self._conn.execute(query, values).fetchall()
        return [
            {
                "job_name": row["job_name"],
                "status": row["status"],
                "payload": _load(row["payload_json"]),
                "pid": row["pid"],
                "started_at": _parse_datetime(row["started_at"]),
                "updated_at": _parse_datetime(row["updated_at"]),
                "last_error": row["last_error"],
                "resumable": bool(row["resumable"]),
            }
            for row in rows
        ]
    def list_operator_job_progress(
        self,
        *,
        job_prefix: str,
        limit: int = 32,
    ) -> list[dict[str, Any]]:
        """Return bounded campaign progress fields without hydrating payloads."""
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 0:
            raise ValueError("limit must be a non-negative integer")
        prefix = str(job_prefix)
        escaped_prefix = prefix.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        query = (
            "SELECT job_name,status,updated_at,"
            "json_extract(payload_json,'$.campaign_id') AS campaign_id,"
            "json_extract(payload_json,'$.campaign.campaign_id') AS nested_campaign_id,"
            "json_extract(payload_json,'$.status') AS payload_status,"
            "json_extract(payload_json,'$.budget_limit') AS budget_limit,"
            "json_extract(payload_json,'$.budget_used') AS budget_used,"
            "json_extract(payload_json,'$.budget_remaining') AS budget_remaining,"
            "json_extract(payload_json,'$.next_real_job') AS next_real_job,"
            "json_extract(payload_json,'$.dataset_id') AS dataset_id,"
            "json_extract(payload_json,'$.dataset_version') AS dataset_version,"
            "json_extract(payload_json,'$.last_result') AS last_result_json,"
            "COALESCE("
            "CASE WHEN json_type(payload_json,'$.qualified_candidate_ids')='array' "
            "THEN (SELECT json_group_array(value) FROM json_each(payload_json,'$.qualified_candidate_ids') WHERE key<32) END,"
            "CASE WHEN json_type(payload_json,'$.qualified')='array' "
            "THEN (SELECT json_group_array(value) FROM json_each(payload_json,'$.qualified') WHERE key<32) END,"
            "CASE WHEN json_type(payload_json,'$.qualified.candidate_ids')='array' "
            "THEN (SELECT json_group_array(value) FROM json_each(payload_json,'$.qualified.candidate_ids') WHERE key<32) END"
            ") AS qualified_json,"
            "CASE WHEN json_type(payload_json,'$.trials')='array' "
            "THEN MIN(json_array_length(payload_json,'$.trials'),64) END AS trial_count,"
            "CASE WHEN json_type(payload_json,'$.trials')='array' THEN ("
            "SELECT COUNT(*) FROM json_each(payload_json,'$.trials') "
            "WHERE key<64 AND json_extract(value,'$.status') IN "
            "('ECONOMIC_REJECTION','DATA_INSUFFICIENT','SOFTWARE_OR_INPUT_ERROR',"
            "'VALIDATION_QUALIFIED','FINAL_ASSESSMENT')) END AS completed_trial_count,"
            "json_extract(payload_json,'$.counts.economic_rejection') AS economic_rejection,"
            "json_extract(payload_json,'$.counts.data_insufficient') AS data_insufficient,"
            "json_extract(payload_json,'$.counts.software_or_input_error') AS software_or_input_error,"
            "json_extract(payload_json,'$.counts.validation_qualified') AS validation_qualified,"
            "json_extract(payload_json,'$.counts.final_assessment') AS final_assessment "
            "FROM operator_jobs WHERE job_name LIKE ? ESCAPE '\\' "
            "ORDER BY updated_at DESC,job_name LIMIT ?"
        )
        with self._lock:
            rows = self._conn.execute(query, (escaped_prefix + "%", int(limit))).fetchall()
        records: list[dict[str, Any]] = []
        for row in rows:
            payload: dict[str, Any] = {}
            for key in (
                "campaign_id",
                "budget_limit",
                "budget_used",
                "budget_remaining",
                "next_real_job",
                "dataset_id",
                "dataset_version",
            ):
                value = row[key]
                if value is not None:
                    payload[key] = value
            if row["nested_campaign_id"] is not None:
                payload["campaign"] = {"campaign_id": row["nested_campaign_id"]}
            if row["payload_status"] is not None:
                payload["status"] = row["payload_status"]
            if row["last_result_json"] is not None:
                raw_last_result = row["last_result_json"]
                try:
                    loaded = json.loads(raw_last_result)
                except (TypeError, ValueError, json.JSONDecodeError):
                    loaded = raw_last_result
                payload["last_result"] = loaded
            if row["qualified_json"] is not None:
                try:
                    loaded = json.loads(row["qualified_json"])
                except (TypeError, ValueError, json.JSONDecodeError):
                    loaded = []
                payload["qualified_candidate_ids"] = loaded if isinstance(loaded, list) else []
            if row["trial_count"] is not None:
                payload["_trial_count"] = int(row["trial_count"] or 0)
                payload["_completed_trial_count"] = int(row["completed_trial_count"] or 0)
            payload["counts"] = {
                key: int(row[key] or 0)
                for key in (
                    "economic_rejection",
                    "data_insufficient",
                    "software_or_input_error",
                    "validation_qualified",
                    "final_assessment",
                )
            }
            records.append(
                {
                    "job_name": row["job_name"],
                    "status": row["status"],
                    "payload": payload,
                    "updated_at": _parse_datetime(row["updated_at"]),
                }
            )
        return records

    def record_operator_action(
        self,
        action: str,
        target: str,
        *,
        success: bool,
        reason: str = "",
        result: Mapping[str, Any] | None = None,
        timestamp: datetime | None = None,
    ) -> str:
        action_value = str(action).strip()[:160]
        target_value = str(target).strip()[:512]
        reason_value = str(reason or "")[:2_000]
        result_body = dict(result or {})
        encoded = _dump(result_body)
        if len(encoded) > 64_000:
            raise ValueError("operator action result is too large")
        at = _iso(timestamp or utc_now())
        action_id = "operator:" + hashlib.sha256(
            _dump(
                {
                    "action": action_value,
                    "target": target_value,
                    "timestamp": at,
                    "result": result_body,
                }
            ).encode("utf-8")
        ).hexdigest()
        with self._write_context():
            self._conn.execute(
                "INSERT OR IGNORE INTO operator_actions(action_id,action,target,timestamp,success,reason,result_json) "
                "VALUES (?,?,?,?,?,?,?)",
                (action_id, action_value, target_value, at, 1 if success else 0, reason_value, encoded),
            )
        return action_id

    def list_operator_actions(self, *, limit: int = 100) -> list[dict[str, Any]]:
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 0:
            raise ValueError("limit must be a non-negative integer")
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM operator_actions ORDER BY timestamp DESC,action_id DESC LIMIT ?",
                (int(limit),),
            ).fetchall()
        return [
            {
                "action_id": row["action_id"],
                "action": row["action"],
                "target": row["target"],
                "timestamp": _parse_datetime(row["timestamp"]),
                "success": bool(row["success"]),
                "reason": row["reason"],
                "result": _load(row["result_json"]),
            }
            for row in rows
        ]

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
        audit_lineage_fields = {
            "risk_config_id",
            "risk_config_generation",
            "risk_config_hash",
        }
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
            if normalized in audit_lineage_fields:
                if normalized == "risk_config_generation":
                    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                        raise ValueError("risk_config_generation must be a positive integer")
                elif not isinstance(value, str) or not value.strip():
                    raise ValueError(f"{normalized} must be a non-empty string")
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
                "config": json.loads(row["config_json"]),
                "start_timestamp": _parse_datetime(row["start_timestamp"]),
                "bankroll": float(row["bankroll"]),
                "allowed_markets": json.loads(row["allowed_markets_json"]),
                "risk_limits": json.loads(row["risk_limits_json"]),
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
        snapshot = self._snapshot_read_connection()
        try:
            market_row = snapshot.execute(
                "WITH market_ids AS ("
                "SELECT market_id FROM polymarket_markets "
                "WHERE rowid > COALESCE((SELECT MAX(rowid)-? FROM polymarket_markets),0) AND observed_at <= ? "
                "UNION SELECT market_id FROM polymarket_snapshots "
                "WHERE rowid > COALESCE((SELECT MAX(rowid)-? FROM polymarket_snapshots),0) AND observed_at <= ?"
                ") SELECT COUNT(*) AS market_count FROM market_ids",
                (_MAX_EVIDENCE_SCAN_ROWS, cutoff, _MAX_EVIDENCE_SCAN_ROWS, cutoff),
            ).fetchone()
            snapshot_row = snapshot.execute(
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
            regime_row = snapshot.execute(
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
            trade_row = snapshot.execute(
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
        finally:
            snapshot.close()
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
        configured_state = self.get_collector_state("polymarket") or {}
        configured_interval = (
            configured_state.get("configured_interval_seconds")
            if isinstance(configured_state, Mapping)
            else None
        )
        try:
            configured_interval_value = float(configured_interval)
        except (TypeError, ValueError):
            configured_interval_value = 0.0
        if math.isfinite(configured_interval_value) and 0 < configured_interval_value <= _MAX_OPERATIONAL_WINDOW_SECONDS:
            expected_interval_seconds = configured_interval_value
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
        cycle_limit = int(recent_cycles or 24)
        cycle_rows = self.list_collection_cycles(collector_name="polymarket", limit=cycle_limit)
        cycle_payloads = [
            item.get("payload")
            for item in cycle_rows
            if isinstance(item, Mapping) and isinstance(item.get("payload"), Mapping)
        ]
        cycle_starts = [
            stamp
            for item in cycle_rows
            for stamp in [_parse_datetime(item.get("started_at"))]
            if stamp is not None
        ]
        cycle_intervals = [
            (newer - older).total_seconds()
            for newer, older in zip(cycle_starts, cycle_starts[1:])
            if (newer - older).total_seconds() > 0
        ]
        effective_cadence = (
            round(sum(cycle_intervals) / len(cycle_intervals), 3)
            if cycle_intervals
            else None
        )
        latest_cycle_payload = cycle_payloads[0] if cycle_payloads else {}
        last_successful_cycle = next(
            (
                item
                for item in cycle_rows
                if isinstance(item, Mapping)
                and isinstance(item.get("payload"), Mapping)
                and int(item["payload"].get("markets_successful", item["payload"].get("snapshots_inserted", 0)) or 0) > 0
            ),
            None,
        )
        collector_state = self.get_collector_state("polymarket") or {}
        scheduled_values = collector_state.get("scheduled_market_ids") if isinstance(collector_state, Mapping) else None
        scheduled_market_ids = (
            {str(value) for value in scheduled_values if str(value).strip()}
            if isinstance(scheduled_values, (list, tuple, set, frozenset))
            else set()
        )
        snapshot = self._snapshot_read_connection()
        try:
            latest_rows = snapshot.execute(
                "SELECT market_id,observed_at,payload_json FROM ("
                "SELECT market_id,observed_at,payload_json,"
                "ROW_NUMBER() OVER (PARTITION BY market_id ORDER BY observed_at DESC,source_timestamp DESC,snapshot_id DESC) AS row_number "
                "FROM polymarket_snapshots WHERE source_type='FORWARD_COLLECTED' AND observed_at>=? AND observed_at<=?) WHERE row_number=1",
                (latest_window_iso, current_iso),
            ).fetchall()
            recent_rows = snapshot.execute(
                "SELECT market_id,observed_at FROM polymarket_snapshots "
                "WHERE source_type='FORWARD_COLLECTED' AND observed_at>=? AND observed_at<=? "
                "ORDER BY market_id,observed_at,source_timestamp,snapshot_id",
                (window_iso, current_iso),
            ).fetchall()
            error_rows = snapshot.execute(
                "SELECT error_id,market_id,observed_at,kind,detail,payload_json FROM collection_errors "
                "WHERE source_type='FORWARD_COLLECTED' AND observed_at>=? AND observed_at<=? "
                "AND rowid > COALESCE((SELECT MAX(rowid)-? FROM collection_errors WHERE source_type='FORWARD_COLLECTED'),0) "
                "ORDER BY observed_at,error_id LIMIT 256",
                (window_iso, current_iso, _MAX_LATEST_SCAN_ROWS),
            ).fetchall()
            historical_error_count = int(snapshot.execute(
                "SELECT COUNT(*) AS n FROM collection_errors WHERE source_type='HISTORICAL' "
                "AND rowid > COALESCE((SELECT MAX(rowid)-? FROM collection_errors WHERE source_type='HISTORICAL'),0) AND observed_at<=?",
                (_MAX_EVIDENCE_SCAN_ROWS, current_iso),
            ).fetchone()["n"])
            trade_count = int(snapshot.execute(
                "SELECT COUNT(*) AS n FROM ("
                "SELECT trade_key FROM polymarket_trades "
                "WHERE timestamp>=? AND timestamp<=? "
                "ORDER BY timestamp,trade_key LIMIT ?"
                ")",
                (window_iso, current_iso, _MAX_LATEST_SCAN_ROWS),
            ).fetchone()["n"])
            metadata_count = int(snapshot.execute(
                "SELECT COUNT(*) AS n FROM polymarket_markets "
                "WHERE source_type='FORWARD_COLLECTED' AND observed_at>=? AND observed_at<=?",
                (latest_window_iso, current_iso),
            ).fetchone()["n"])
        finally:
            snapshot.close()
        tracked = self.tracked_polymarket_markets(active_only=True, now=current, include_payload=True, limit=1000)
        active_markets = (
            set(scheduled_market_ids)
            if scheduled_market_ids
            else {
                str(item.get("market_id"))
                for item in tracked
                if isinstance(item, Mapping) and item.get("market_id")
            }
        )
        latest_by_market: dict[str, datetime] = {}
        latest_payload: dict[str, Mapping[str, Any]] = {}
        for row in latest_rows:
            market = str(row["market_id"])
            if scheduled_market_ids and market not in scheduled_market_ids:
                continue
            stamp = _parse_datetime(row["observed_at"])
            if stamp is None:
                continue
            latest_by_market[market] = stamp
            payload = _load(row["payload_json"])
            latest_payload[market] = payload if isinstance(payload, Mapping) else {}
            if not scheduled_market_ids and market not in active_markets:
                snapshot = latest_payload[market].get("snapshot")
                settlement = str(snapshot.get("settlement", "")).lower() if isinstance(snapshot, Mapping) else ""
                if settlement not in {"resolved_yes", "resolved_no", "void"}:
                    active_markets.add(market)
        current_rows: dict[str, list[datetime]] = {}
        for row in recent_rows:
            market = str(row["market_id"])
            if scheduled_market_ids and market not in scheduled_market_ids:
                continue
            stamp = _parse_datetime(row["observed_at"])
            if stamp is not None:
                current_rows.setdefault(market, []).append(stamp)
                if not scheduled_market_ids:
                    active_markets.add(market)
        gap_interval = effective_cadence or expected
        gaps: list[dict[str, Any]] = []
        for market, stamps in current_rows.items():
            for previous, observed in zip(stamps, stamps[1:]):
                gap_seconds = (observed - previous).total_seconds()
                if gap_seconds > gap_interval * 1.5:
                    gaps.append({
                        "market_id": market,
                        "from": previous.isoformat(),
                        "to": observed.isoformat(),
                        "seconds": gap_seconds,
                        "missing_intervals": max(1, int(round(gap_seconds / gap_interval)) - 1),
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
        failure_counts: dict[str, int] = {}
        for item in current_failures:
            code = str(item.get("reason_code") or "UNKNOWN")
            failure_counts[code] = failure_counts.get(code, 0) + 1
        top_failure_codes = [
            {"code": code, "count": count}
            for code, count in sorted(failure_counts.items(), key=lambda pair: (-pair[1], pair[0]))[:8]
        ]
        malformed_count = sum("MALFORM" in str(item["kind"]).upper() or "PARSE" in str(item["kind"]).upper() for item in current_failures)
        reasons: list[dict[str, str]] = []
        if not current_rows:
            reasons.append({"code": "NO_FORWARD_SNAPSHOTS", "reason": "No FORWARD_COLLECTED snapshot was observed in the current window."})
        if stale_markets:
            reasons.append({"code": "STALE_MARKETS", "reason": f"{len(stale_markets)} scheduled market(s) have no successful sample within the staleness threshold."})
        if gaps:
            reasons.append({"code": "COLLECTION_GAPS", "reason": f"{len(gaps)} in-window collection gap(s) exceed the measured collection cadence."})
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
        latest_cycle_started = _parse_datetime(
            latest_cycle_payload.get("started_at") if isinstance(latest_cycle_payload, Mapping) else None
        )
        latest_cycle_ended = _parse_datetime(
            latest_cycle_payload.get("ended_at") if isinstance(latest_cycle_payload, Mapping) else None
        )
        last_successful_at = (
            last_successful_cycle.get("ended_at")
            if isinstance(last_successful_cycle, Mapping)
            else None
        )
        collector = {
            "grade": grade,
            "grade_scope": "collector_health",
            "reason_code": reasons[0]["code"] if reasons else None,
            "reasons": reasons,
            "markets": len(active_markets),
            "scheduled_market_count": len(scheduled_market_ids),
            "markets_with_snapshots": len(latest_by_market),
            "metadata_records": metadata_count,
            "current_snapshots": sum(len(items) for items in current_rows.values()),
            "snapshots": sum(len(items) for items in current_rows.values()),
            "trades": trade_count,
            "collection_errors": len(current_failures),
            "current_failures": current_failures,
            "top_failure_codes": top_failure_codes,
            "malformed_records": malformed_count,
            "stale_markets": stale_markets,
            "gaps": gaps,
            "gap_count": len(gaps),
            "latest_observed_at": max(latest_by_market.values(), default=None).isoformat() if latest_by_market else None,
            "window_start": window_start.isoformat(),
            "window_end": current.isoformat(),
            "window_seconds": window,
            "configured_interval_seconds": expected,
            "expected_interval_seconds": expected,
            "effective_collection_cadence_seconds": effective_cadence,
            "gap_interval_seconds": gap_interval,
            "stale_after_seconds": stale_after,
            "last_cycle_started_at": latest_cycle_started.isoformat() if latest_cycle_started else None,
            "last_cycle_ended_at": latest_cycle_ended.isoformat() if latest_cycle_ended else None,
            "last_cycle_duration_seconds": latest_cycle_payload.get("duration_seconds"),
            "last_cycle_markets_attempted": latest_cycle_payload.get("markets_attempted", 0),
            "last_cycle_markets_successful": latest_cycle_payload.get("markets_successful", 0),
            "last_cycle_markets_failed": latest_cycle_payload.get("markets_failed", 0),
            "last_successful_cycle": last_successful_at.isoformat() if hasattr(last_successful_at, "isoformat") else last_successful_at,
            "worker_name": "polymarket-collector",
            "worker_heartbeat_at": collector_state.get("worker_heartbeat_at"),
            "next_scheduled_collection_at": collector_state.get("next_scheduled_collection_at"),
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
            "top_failure_codes": top_failure_codes,
            "scheduled_market_count": collector["scheduled_market_count"],
            "effective_collection_cadence_seconds": collector["effective_collection_cadence_seconds"],
            "configured_interval_seconds": collector["configured_interval_seconds"],
            "expected_interval_seconds": collector["expected_interval_seconds"],
            "stale_after_seconds": collector["stale_after_seconds"],
            "last_cycle_started_at": collector["last_cycle_started_at"],
            "last_cycle_ended_at": collector["last_cycle_ended_at"],
            "last_cycle_duration_seconds": collector["last_cycle_duration_seconds"],
            "last_successful_cycle": collector["last_successful_cycle"],
            "last_cycle_markets_attempted": collector["last_cycle_markets_attempted"],
            "last_cycle_markets_successful": collector["last_cycle_markets_successful"],
            "last_cycle_markets_failed": collector["last_cycle_markets_failed"],
            "worker_name": collector["worker_name"],
            "worker_heartbeat_at": collector["worker_heartbeat_at"],
            "next_scheduled_collection_at": collector["next_scheduled_collection_at"],
            "last_successful_collection_at": collector["last_successful_cycle"],
            "stale_markets": stale_markets,
            "gaps": gaps,
            "window_start": collector["window_start"],
            "window_end": collector["window_end"],
            "window_seconds": window,
            "historical_error_count": historical_error_count,
            "historical_maturity_grade": maturity.get("grade"),
            "evidence_maturity": maturity,
            "storage_bytes": _storage_bytes(self._conn, self.path),
            "scan_limits": {"latest_window_seconds": latest_window_seconds, "recent_window_seconds": window, "recent_cycles": recent_cycles, "gap_sample": 64},
        }
    # Rolling portfolio persistence ------------------------------------
    def save_strategy_version(self, record: Any) -> None:
        data = _rolling_mapping(record, name="strategy_version")
        identifier = _rolling_required_text(data, "strategy_version_id", name="strategy_version_id")
        strategy_id = _rolling_optional_text(data, "strategy_id", "strategy")
        version = _rolling_optional_text(data, "version", "strategy_version")
        code_hash = _rolling_optional_text(data, "code_hash", "strategy_hash")
        config_hash = _rolling_optional_text(data, "config_hash")
        created_at = _rolling_timestamp(data.get("created_at"), name="created_at", default_now=True)
        payload_data = dict(data)
        payload_data.update(
            {
                "strategy_version_id": identifier,
                "strategy_id": strategy_id,
                "version": version,
                "code_hash": code_hash,
                "config_hash": config_hash,
                "created_at": created_at,
            }
        )
        payload_json = _rolling_dump(payload_data)
        values = (identifier, strategy_id, version, code_hash, config_hash, payload_json, created_at)
        with self._write_context():
            existing = self._conn.execute(
                "SELECT * FROM strategy_versions WHERE strategy_version_id=?",
                (identifier,),
            ).fetchone()
            if existing is not None:
                if (
                    str(existing["strategy_id"]) != strategy_id
                    or str(existing["version"]) != version
                    or str(existing["code_hash"]) != code_hash
                    or str(existing["config_hash"]) != config_hash
                    or not _rolling_payload_equal(
                        _load(existing["payload_json"]),
                        payload_data,
                        ignored=frozenset({"created_at"}),
                    )
                ):
                    raise ValueError("strategy version identity conflict")
                return
            self._conn.execute(
                "INSERT INTO strategy_versions("
                "strategy_version_id,strategy_id,version,code_hash,config_hash,payload_json,created_at"
                ") VALUES (?,?,?,?,?,?,?)",
                values,
            )

    def load_strategy_version(self, strategy_version_id: str) -> dict[str, Any] | None:
        identifier = str(strategy_version_id).strip()
        if not identifier:
            raise ValueError("strategy_version_id is required")
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM strategy_versions WHERE strategy_version_id=?",
                (identifier,),
            ).fetchone()
        if row is None:
            return None
        payload = _load(row["payload_json"])
        result = dict(payload) if isinstance(payload, Mapping) else {}
        result.update(
            {
                "strategy_version_id": row["strategy_version_id"],
                "strategy_id": row["strategy_id"],
                "version": row["version"],
                "code_hash": row["code_hash"],
                "config_hash": row["config_hash"],
                "created_at": _parse_datetime(row["created_at"]),
            }
        )
        return result

    def list_strategy_versions(
        self,
        *,
        strategy_id: str | None = None,
        limit: int | None = 100,
    ) -> list[dict[str, Any]]:
        limit_value = _rolling_limit(limit, default=100)
        if strategy_id is None:
            query = "SELECT strategy_version_id FROM strategy_versions ORDER BY created_at,strategy_version_id LIMIT ?"
            values: tuple[Any, ...] = (limit_value,)
        else:
            query = (
                "SELECT strategy_version_id FROM strategy_versions WHERE strategy_id=? "
                "ORDER BY created_at,strategy_version_id LIMIT ?"
            )
            values = (str(strategy_id).strip(), limit_value)
        with self._lock:
            rows = self._conn.execute(query, values).fetchall()
        return [
            record
            for row in rows
            if (record := self.load_strategy_version(str(row["strategy_version_id"]))) is not None
        ]

    def save_research_trial(self, record: Any) -> None:
        data = _rolling_mapping(record, name="research_trial")
        identifier = _rolling_required_text(
            data,
            "research_trial_id",
            "trial_id",
            name="research_trial_id",
        )
        strategy_version_id = _rolling_required_text(
            data,
            "strategy_version_id",
            name="strategy_version_id",
        )
        status = _rolling_optional_text(data, "status")
        created_at = _rolling_timestamp(data.get("created_at"), name="created_at", default_now=True)
        payload_data = dict(data)
        payload_data.update(
            {
                "research_trial_id": identifier,
                "trial_id": identifier,
                "strategy_version_id": strategy_version_id,
                "status": status,
                "created_at": created_at,
            }
        )
        payload_json = _rolling_dump(payload_data)
        with self._write_context():
            if self._conn.execute(
                "SELECT 1 FROM strategy_versions WHERE strategy_version_id=?",
                (strategy_version_id,),
            ).fetchone() is None:
                raise ValueError("research trial strategy version does not exist")
            existing = self._conn.execute(
                "SELECT * FROM research_trials WHERE research_trial_id=?",
                (identifier,),
            ).fetchone()
            if existing is not None:
                if (
                    str(existing["strategy_version_id"]) != strategy_version_id
                    or str(existing["status"]) != status
                    or not _rolling_payload_equal(
                        _load(existing["payload_json"]),
                        payload_data,
                        ignored=frozenset({"created_at"}),
                    )
                ):
                    raise ValueError("research trial identity conflict")
                return
            self._conn.execute(
                "INSERT INTO research_trials("
                "research_trial_id,strategy_version_id,status,payload_json,created_at"
                ") VALUES (?,?,?,?,?)",
                (identifier, strategy_version_id, status, payload_json, created_at),
            )
    def load_research_trial(self, research_trial_id: str) -> dict[str, Any] | None:
        identifier = str(research_trial_id).strip()
        if not identifier:
            raise ValueError("research_trial_id is required")
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM research_trials WHERE research_trial_id=?",
                (identifier,),
            ).fetchone()
        if row is None:
            return None
        payload = _load(row["payload_json"])
        result = dict(payload) if isinstance(payload, Mapping) else {}
        result.update(
            {
                "research_trial_id": row["research_trial_id"],
                "trial_id": row["research_trial_id"],
                "strategy_version_id": row["strategy_version_id"],
                "status": row["status"],
                "created_at": _parse_datetime(row["created_at"]),
            }
        )
        return result

    def list_research_trials(
        self,
        *,
        strategy_version_id: str | None = None,
        limit: int | None = 100,
    ) -> list[dict[str, Any]]:
        limit_value = _rolling_limit(limit, default=100)
        if strategy_version_id is None:
            query = "SELECT research_trial_id FROM research_trials ORDER BY created_at,research_trial_id LIMIT ?"
            values: tuple[Any, ...] = (limit_value,)
        else:
            query = (
                "SELECT research_trial_id FROM research_trials WHERE strategy_version_id=? "
                "ORDER BY created_at,research_trial_id LIMIT ?"
            )
            values = (str(strategy_version_id).strip(), limit_value)
        with self._lock:
            rows = self._conn.execute(query, values).fetchall()
        return [
            record
            for row in rows
            if (record := self.load_research_trial(str(row["research_trial_id"]))) is not None
        ]
    def save_rolling_enrollment(self, record: Any) -> bool:
        """Persist one immutable versioned rolling enrollment decision.

        A validation fix is a new application attempt, not an update to its
        predecessor.  The attempt version and optional predecessor link are
        therefore first-class columns while the source decision remains
        append-only.
        """
        data = _rolling_mapping(record, name="rolling_enrollment")
        candidate_id = _rolling_required_text(data, "candidate_id", "source_candidate_id", name="candidate_id")
        strategy_version_id = _rolling_optional_text(data, "strategy_version_id")
        research_trial_id = _rolling_optional_text(data, "research_trial_id", "trial_id")
        status = _rolling_required_text(data, "status", name="status").upper()
        if status not in {"ACCEPTED", "EXCLUDED"}:
            raise ValueError("rolling enrollment status must be ACCEPTED or EXCLUDED")
        if status == "ACCEPTED" and (not strategy_version_id or not research_trial_id):
            raise ValueError("accepted rolling enrollment requires strategy and trial identities")
        reason = _rolling_required_text(data, "reason", "reason_code", name="reason")
        if len(reason) > 512:
            raise ValueError("rolling enrollment reason exceeds 512 characters")
        version_fields = [
            name
            for name in ("validation_version", "attempt_version")
            if name in data
        ]
        if version_fields:
            version_values: list[str] = []
            for name in version_fields:
                value = _rolling_optional_text(data, name)
                if not value:
                    raise ValueError(f"{name} must not be blank")
                version_values.append(value)
            if len(set(version_values)) != 1:
                raise ValueError("validation_version and attempt_version must agree")
            validation_version = version_values[0]
        else:
            validation_version = "rolling-enrollment-v1"
        predecessor_enrollment_id = _rolling_optional_text(
            data,
            "predecessor_enrollment_id",
            "predecessor_id",
        )
        if predecessor_enrollment_id == _rolling_optional_text(data, "enrollment_id"):
            raise ValueError("rolling enrollment cannot precede itself")
        provenance = data.get("provenance", data.get("provenance_json", {}))
        if isinstance(provenance, str):
            try:
                provenance = _load(provenance)
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                raise ValueError("rolling enrollment provenance must be JSON") from exc
        if not isinstance(provenance, Mapping):
            raise ValueError("rolling enrollment provenance must be a mapping")
        provenance = dict(provenance)
        created_at = _rolling_timestamp(data.get("created_at"), name="created_at", default_now=True)
        identity = {
            "candidate_id": candidate_id,
            "strategy_version_id": strategy_version_id or None,
            "research_trial_id": research_trial_id or None,
            "status": status,
            "reason": reason,
            "validation_version": validation_version,
        }
        enrollment_id = _rolling_optional_text(data, "enrollment_id")
        if not enrollment_id:
            enrollment_id = "rolling-enrollment-" + hashlib.sha256(
                _rolling_dump(identity).encode("utf-8")
            ).hexdigest()[:48]
        payload_json = _rolling_dump(provenance)
        values = (
            enrollment_id,
            candidate_id,
            strategy_version_id or None,
            research_trial_id or None,
            status,
            reason,
            validation_version,
            predecessor_enrollment_id,
            payload_json,
            created_at,
        )
        with self._write_context():
            if predecessor_enrollment_id:
                predecessor = self._conn.execute(
                    "SELECT candidate_id,status FROM rolling_strategy_enrollments "
                    "WHERE enrollment_id=?",
                    (predecessor_enrollment_id,),
                ).fetchone()
                if predecessor is None:
                    raise ValueError("rolling enrollment predecessor does not exist")
                if str(predecessor["candidate_id"]) != candidate_id:
                    raise ValueError("rolling enrollment predecessor candidate mismatch")
                if str(predecessor["status"]).strip().upper() != "EXCLUDED":
                    raise ValueError("rolling enrollment predecessor must be EXCLUDED")
            existing = self._conn.execute(
                "SELECT * FROM rolling_strategy_enrollments WHERE enrollment_id=?",
                (enrollment_id,),
            ).fetchone()
            if existing is not None:
                expected = (
                    existing["candidate_id"],
                    existing["strategy_version_id"],
                    existing["research_trial_id"],
                    existing["status"],
                    existing["reason"],
                    existing["validation_version"],
                    existing["predecessor_enrollment_id"],
                    existing["provenance_json"],
                )
                if expected != values[1:9]:
                    raise ValueError("rolling enrollment identity conflict")
                return False
            self._conn.execute(
                "INSERT INTO rolling_strategy_enrollments("
                "enrollment_id,candidate_id,strategy_version_id,research_trial_id,status,reason,"
                "validation_version,predecessor_enrollment_id,provenance_json,created_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?)",
                values,
            )
        return True

    def load_rolling_enrollment(self, enrollment_id: str) -> dict[str, Any] | None:
        identifier = str(enrollment_id).strip()
        if not identifier:
            raise ValueError("enrollment_id is required")
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM rolling_strategy_enrollments WHERE enrollment_id=?",
                (identifier,),
            ).fetchone()
        return _rolling_enrollment_record(row)

    def list_rolling_enrollments(
        self,
        *,
        candidate_id: str | None = None,
        status: str | None = None,
        limit: int | None = 100,
    ) -> list[dict[str, Any]]:
        limit_value = _rolling_limit(limit, default=100)
        clauses: list[str] = []
        values: list[Any] = []
        if candidate_id is not None:
            clauses.append("candidate_id=?")
            values.append(str(candidate_id).strip())
        if status is not None:
            status_value = str(status).strip().upper()
            if status_value not in {"ACCEPTED", "EXCLUDED"}:
                raise ValueError("status must be ACCEPTED or EXCLUDED")
            clauses.append("status=?")
            values.append(status_value)
        query = "SELECT * FROM rolling_strategy_enrollments"
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY created_at DESC,enrollment_id DESC LIMIT ?"
        values.append(limit_value)
        with self._lock:
            rows = self._conn.execute(query, values).fetchall()
        return [_rolling_enrollment_record(row) for row in rows if row is not None]

    # Compatibility names used by application services.
    save_rolling_strategy_enrollment = save_rolling_enrollment
    load_rolling_strategy_enrollment = load_rolling_enrollment
    list_rolling_strategy_enrollments = list_rolling_enrollments

    def save_admission_policy(self, record: Any) -> None:
        data = _rolling_mapping(record, name="admission_policy")
        policy_id = _rolling_required_text(data, "policy_id", name="policy_id")
        version = _rolling_required_text(data, "version", "policy_version", name="version")
        config_hash = _rolling_required_text(data, "config_hash", name="config_hash")
        created_at = _rolling_timestamp(data.get("created_at"), name="created_at", default_now=True)
        payload_data = dict(data)
        payload_data.update(
            {
                "policy_id": policy_id,
                "version": version,
                "policy_version": version,
                "config_hash": config_hash,
                "created_at": created_at,
            }
        )
        payload_json = _rolling_dump(payload_data)
        with self._write_context():
            existing = self._conn.execute(
                "SELECT * FROM admission_policies WHERE policy_id=? AND version=?",
                (policy_id, version),
            ).fetchone()
            if existing is not None:
                if (
                    str(existing["config_hash"]) != config_hash
                    or not _rolling_payload_equal(
                        _load(existing["payload_json"]),
                        payload_data,
                        ignored=frozenset({"created_at"}),
                    )
                ):
                    raise ValueError("admission policy identity conflict")
                return
            self._conn.execute(
                "INSERT INTO admission_policies("
                "policy_id,version,config_hash,payload_json,created_at"
                ") VALUES (?,?,?,?,?)",
                (policy_id, version, config_hash, payload_json, created_at),
            )

    def load_admission_policy(self, policy_id: str, version: str) -> dict[str, Any] | None:
        identifier, version_value = str(policy_id).strip(), str(version).strip()
        if not identifier or not version_value:
            raise ValueError("policy_id and version are required")
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM admission_policies WHERE policy_id=? AND version=?",
                (identifier, version_value),
            ).fetchone()
        if row is None:
            return None
        payload = _load(row["payload_json"])
        result = dict(payload) if isinstance(payload, Mapping) else {}
        result.update(
            {
                "policy_id": row["policy_id"],
                "version": row["version"],
                "policy_version": row["version"],
                "config_hash": row["config_hash"],
                "created_at": row["created_at"],
            }
        )
        return result

    def save_strategy_evidence_window(self, record: Any) -> None:
        data = _rolling_mapping(record, name="strategy_evidence_window")
        identifier = _rolling_required_text(data, "evidence_window_id", name="evidence_window_id")
        if _rolling_identity_conflict(data, "strategy_version_id"):
            raise ValueError("evidence strategy identity conflicts")
        strategy_version_id = _rolling_required_text(
            data,
            "strategy_version_id",
            name="strategy_version_id",
        )
        evaluation_nested = _rolling_projection_mapping(data, "evaluation")
        accounting_nested = _rolling_projection_mapping(
            data,
            "portfolio_accounting",
        )
        shared_fields = (
            "evaluation_kind",
            "evaluation_run_id",
            "evaluation_version",
            "supersedes_evidence_id",
            "loaded_rows",
            "valid_input_rows",
            "evaluator_invoked",
            "evaluator_completed",
            "evaluated_observations",
            "signal_count",
            "diagnostic_summary_count",
            "evaluator_name",
            "evaluator_error",
            "evaluator_prerequisite",
        )
        shared: dict[str, Any] = {}
        for field_name in shared_fields:
            value = _rolling_contract_value(
                data,
                evaluation_nested,
                field_name,
                strict_null_conflict=field_name
                in {
                    "evaluation_kind",
                    "supersedes_evidence_id",
                    "evaluator_invoked",
                    "evaluator_completed",
                },
            )
            if _rolling_contract_present(data, evaluation_nested, field_name):
                shared[field_name] = value
        for field_name in (
            "evaluation_run_id",
            "evaluation_version",
            "supersedes_evidence_id",
        ):
            if _rolling_identity_conflict(data, field_name):
                raise ValueError("evaluation identity conflicts")
        for field_name in (
            "loaded_rows",
            "valid_input_rows",
            "evaluated_observations",
            "signal_count",
            "diagnostic_summary_count",
        ):
            if field_name in shared and shared[field_name] is not None:
                shared[field_name] = _rolling_nonnegative_integer(
                    shared[field_name],
                    name=field_name,
                )
        for field_name in ("evaluator_invoked", "evaluator_completed"):
            if field_name in shared:
                shared[field_name] = _rolling_optional_boolean(
                    shared[field_name],
                    name=field_name,
                )
        for field_name in (
            "evaluation_kind",
            "evaluation_run_id",
            "evaluation_version",
            "supersedes_evidence_id",
            "evaluator_name",
            "evaluator_error",
            "evaluator_prerequisite",
        ):
            if field_name in shared and shared[field_name] is not None:
                shared[field_name] = _rolling_optional_text(
                    {"value": shared[field_name]},
                    "value",
                ) or None
        if "evaluation_kind" in shared:
            shared["evaluation_kind"] = _rolling_evaluation_kind(
                shared["evaluation_kind"]
            )
        accounting_fields = (
            "accounting_available",
            "accounting_complete",
            "accounting_partial",
            "initial_cash",
            "cash",
            "equity",
            "realized_pnl",
            "unrealized_pnl",
            "net_pnl",
            "fees",
            "costs",
            "open_positions",
            "opening_fills",
            "closing_fills",
            "partial_closing_fills",
            "completed_round_trips",
        )
        accounting: dict[str, Any] = {}
        for field_name in accounting_fields:
            aliases = _ROLLING_ACCOUNTING_ALIASES.get(field_name, ())
            value = _rolling_contract_value(
                data,
                accounting_nested,
                field_name,
                aliases=aliases,
                strict_null_conflict=field_name
                in {
                    "accounting_available",
                    "accounting_complete",
                    "accounting_partial",
                },
            )
            if _rolling_contract_present(
                data,
                accounting_nested,
                field_name,
                aliases=aliases,
            ):
                accounting[field_name] = value
        if "accounting_available" in accounting:
            accounting["accounting_available"] = _rolling_optional_boolean(
                accounting["accounting_available"],
                name="accounting_available",
            )
        if (
            "completed_round_trips" in accounting
            and accounting["completed_round_trips"] is not None
        ):
            accounting["completed_round_trips"] = _rolling_nonnegative_integer(
                accounting["completed_round_trips"],
                name="completed_round_trips",
            )
        for field_name in ("accounting_available", "accounting_complete", "accounting_partial"):
            if field_name in accounting:
                accounting[field_name] = _rolling_optional_boolean(
                    accounting[field_name],
                    name=field_name,
                )
        for field_name in (
            "initial_cash",
            "cash",
            "equity",
            "realized_pnl",
            "unrealized_pnl",
            "net_pnl",
            "fees",
            "costs",
        ):
            if field_name in accounting:
                accounting[field_name] = _rolling_nullable_decimal_text(
                    accounting[field_name],
                    name=field_name,
                )
        if accounting and not isinstance(accounting_nested, Mapping):
            raise ValueError("portfolio_accounting provenance is invalid")
        research_trial_id = _rolling_optional_text(
            data,
            "research_trial_id",
            "trial_id",
        ) or _rolling_identity_value(data, "research_trial_id", "trial_id")
        candidate_id = _rolling_optional_text(
            data,
            "candidate_id",
            "candidate",
            "strategy_candidate_id",
        ) or _rolling_identity_value(data, "candidate_id", "candidate", "strategy_candidate_id")
        if _rolling_identity_conflict(data, "research_trial_id", "trial_id"):
            raise ValueError("evidence research trial identity conflicts")
        if _rolling_identity_conflict(data, "candidate_id", "candidate", "strategy_candidate_id"):
            raise ValueError("evidence candidate identity conflicts")
        available_from = _rolling_timestamp(
            data.get("available_from"),
            name="available_from",
            required=True,
        )
        available_through = _rolling_timestamp(
            data.get("available_through"),
            name="available_through",
            required=True,
        )
        if _parse_datetime(available_through) < _parse_datetime(available_from):
            raise ValueError("available_through must not precede available_from")
        available_span_seconds = int(
            (_parse_datetime(available_through) - _parse_datetime(available_from)).total_seconds()
        )
        requested_days = _rolling_contract_value(
            data,
            {},
            "requested_days",
            aliases=("requested_window_days",),
        )
        requested_days_value = _rolling_nonnegative_integer(
            requested_days,
            name="requested_days",
        )
        if requested_days_value not in {7, 30}:
            raise ValueError("requested_days must be 7 or 30")
        actual_coverage_raw = data.get("actual_coverage_seconds")
        if actual_coverage_raw is None:
            raise ValueError("actual_coverage_seconds is required")
        actual_coverage_seconds = _rolling_nonnegative_integer(
            actual_coverage_raw,
            name="actual_coverage_seconds",
        )
        if actual_coverage_seconds > available_span_seconds:
            raise ValueError("actual_coverage_seconds must not exceed available evidence span")
        expected_completeness = min(
            Decimal("1"),
            Decimal(actual_coverage_seconds) / Decimal(requested_days_value * 86400),
        )
        completeness_raw = data.get("observation_completeness")
        if completeness_raw is None:
            observation_completeness = format(expected_completeness, "f")
        else:
            observation_completeness_value = _rolling_decimal(
                completeness_raw,
                name="observation_completeness",
                nonnegative=True,
            )
            if observation_completeness_value > Decimal("1"):
                raise ValueError("observation_completeness must be between 0 and 1")
            if observation_completeness_value != expected_completeness:
                raise ValueError("observation_completeness is inconsistent with actual coverage")
            observation_completeness = format(observation_completeness_value, "f")
        source_class_raw = _rolling_contract_value(
            data,
            {},
            "source_class",
            aliases=("source_type",),
        )
        source_class = _rolling_required_text(
            {"source_class": source_class_raw},
            "source_class",
            name="source_class",
        )
        if source_class.upper() not in _ROLLING_EVIDENCE_SOURCE_CLASSES:
            raise ValueError("unsupported rolling evidence source_class")
        source_class = source_class.upper()
        assumption_fields = (
            ("paper_sizing_assumptions", ("paper_sizing",), "paper_sizing"),
            (
                "paper_fee_assumptions",
                ("paper_fees", "fee_assumptions", "fee_assumption"),
                "fee_assumption",
            ),
            (
                "paper_slippage_assumptions",
                ("paper_slippage", "slippage_assumptions", "slippage_assumption"),
                "slippage_assumption",
            ),
        )
        assumptions: list[dict[str, Any]] = []
        for field_name, aliases, scalar_key in assumption_fields:
            selected_name = field_name
            value: Any = data.get(field_name)
            if value is None:
                for alias in aliases:
                    candidate = data.get(alias)
                    if candidate is not None:
                        selected_name, value = alias, candidate
                        break
            if value is None:
                value = {}
            if isinstance(value, Mapping):
                assumptions.append(dict(value))
                continue
            if selected_name != scalar_key:
                raise ValueError(f"{field_name} must be a mapping")
            _rolling_decimal(value, name=scalar_key, nonnegative=True)
            assumptions.append({scalar_key: value})
        if "drawdown_usd" in data:
            raise ValueError("drawdown_usd is not supported; drawdown must be a dimensionless ratio")
        drawdown_raw = data.get("drawdown")
        if drawdown_raw is None:
            raise ValueError("drawdown is required")
        drawdown_value = _rolling_decimal(drawdown_raw, name="drawdown", nonnegative=True)
        if drawdown_value > Decimal("1"):
            raise ValueError("drawdown must be between 0 and 1")
        accounting_available = accounting.get("accounting_available")
        if accounting_available is None:
            accounting_available = _rolling_optional_boolean(
                data.get("accounting_available"),
                name="accounting_available",
            )
            if accounting_available is not None:
                accounting["accounting_available"] = accounting_available
        accounting_complete = _rolling_contract_value(
            data,
            accounting_nested,
            "accounting_complete",
        )
        accounting_partial = _rolling_contract_value(
            data,
            accounting_nested,
            "accounting_partial",
        )
        if accounting_complete is not None:
            accounting_complete = _rolling_optional_boolean(
                accounting_complete,
                name="accounting_complete",
            )
        if accounting_partial is not None:
            accounting_partial = _rolling_optional_boolean(
                accounting_partial,
                name="accounting_partial",
            )
        v2_provenance = (
            bool(shared.get("evaluation_kind"))
            or bool(shared.get("evaluation_run_id"))
            or bool(shared.get("evaluation_version"))
            or bool(shared.get("supersedes_evidence_id"))
        )
        if v2_provenance and not shared.get("evaluation_kind"):
            shared["evaluation_kind"] = "CANONICAL_SIMULATION"
        if v2_provenance:
            _rolling_validate_v2_activity(accounting)
        if v2_provenance and shared.get("evaluation_kind") == "ACTUAL_LEDGER" and (
            shared.get("evaluator_invoked") is True
            or shared.get("evaluator_completed") is True
        ):
            raise ValueError("ACTUAL_LEDGER evaluator flags must both be false")
        evaluator_ready = (
            (
                shared.get("evaluator_invoked") is False
                and shared.get("evaluator_completed") is False
            )
            if shared.get("evaluation_kind") == "ACTUAL_LEDGER"
            else (
                shared.get("evaluator_invoked") is True
                and shared.get("evaluator_completed") is True
            )
        )
        canonical_accounting_usable = (
            accounting_available is True
            and accounting_complete is True
            and accounting_partial is False
            and _rolling_v2_accounting_fields_usable(accounting)
        )
        accounting_unavailable = v2_provenance and (
            not evaluator_ready
            or not canonical_accounting_usable
        )
        unavailable_monetary_input = accounting_unavailable and (
            _rolling_v2_has_unredacted_monetary(data)
            or _rolling_v2_has_unredacted_monetary(accounting_nested)
        )
        if accounting_unavailable:
            for field_name in _ROLLING_V2_ACCOUNTING_MONETARY_FIELDS:
                accounting[field_name] = None
            accounting_available = False
            accounting_complete = False
            accounting_partial = True
            accounting["accounting_available"] = False
            accounting["accounting_complete"] = False
            accounting["accounting_partial"] = True
        monetary_aliases = (
            ("allocated_capital_net_return", "allocated_capital_net_return_usd", "net_return"),
            ("realized_pnl", "realized_pnl_usd"),
            ("unrealized_pnl", "unrealized_pnl_usd"),
            ("fees", "fees_usd"),
            ("costs", "costs_usd"),
        )
        monetary: list[str] = []
        monetary_payload: list[str | None] = []
        for names in monetary_aliases:
            field_name, *aliases = names
            present = _rolling_contract_present(
                data,
                accounting_nested,
                field_name,
                aliases=tuple(aliases),
            )
            raw = (
                _rolling_contract_value(
                    data,
                    accounting_nested,
                    field_name,
                    aliases=tuple(aliases),
                )
                if present
                else None
            )
            if raw is None and field_name in accounting and not present:
                raw = accounting[field_name]
            if accounting_unavailable or (
                v2_provenance
                and isinstance(raw, str)
                and raw.strip().lower() in {"none", "null", "unavailable", "unknown", "n/a"}
            ):
                monetary_payload.append(None)
                monetary.append("0")
                continue
            if raw is None and field_name != "allocated_capital_net_return" and accounting_available is not True:
                monetary_payload.append(None)
                monetary.append("0")
                continue
            normalized = (
                _rolling_nullable_decimal_text(raw, name=field_name)
                if v2_provenance
                else _rolling_decimal_text(
                    "0" if raw is None else raw,
                    name=field_name,
                )
            )
            if normalized is None:
                monetary_payload.append(None)
                monetary.append("0")
            else:
                monetary_payload.append(normalized)
                monetary.append(normalized)
        monetary_payload.append(format(drawdown_value, "f"))
        monetary.append(format(drawdown_value, "f"))
        evaluation_document = dict(evaluation_nested)
        evaluation_document.update(shared)
        if evaluation_document:
            evaluation_document["evaluation_run_id"] = shared.get(
                "evaluation_run_id",
                evaluation_document.get("evaluation_run_id"),
            )
        accounting_document = dict(accounting_nested)
        if accounting_document or v2_provenance:
            accounting_document.update(accounting)
        if accounting_unavailable:
            accounting_document = _rolling_v2_unavailable_accounting(accounting_document)
        completed_outcomes = _rolling_nonnegative_integer(
            data.get("completed_outcomes"),
            name="completed_outcomes",
            default=0,
        )
        reliability_raw = data.get("reliability", "0")
        reliability = _rolling_decimal_text(reliability_raw, name="reliability", nonnegative=True)
        if _risk_decimal(reliability, name="reliability", nonnegative=True) > Decimal("1"):
            raise ValueError("reliability must be between 0 and 1")
        execution_feasibility = _rolling_optional_text(
            data,
            "execution_feasibility",
            "execution_feasibility_status",
        )
        if _rolling_identity_conflict(data, "evidence_digest", "digest"):
            raise ValueError("evidence digest aliases conflict")
        supplied_evidence_digest = _rolling_optional_text(
            data,
            "evidence_digest",
            "digest",
        ) or None
        evidence_digest = None
        created_at = _rolling_timestamp(data.get("created_at"), name="created_at", default_now=True)
        payload_data = dict(data)
        for field_name in ("accounting_status", "evaluation_legacy"):
            payload_data.pop(field_name, None)
        if "costs" in payload_data:
            payload_data.pop("slippage_costs", None)
        payload_data.update(
            {
                "evidence_window_id": identifier,
                "strategy_version_id": strategy_version_id,
                "research_trial_id": research_trial_id,
                "candidate_id": candidate_id,
                "available_from": available_from,
                "available_through": available_through,
                "requested_days": requested_days_value,
                "requested_window_days": requested_days_value,
                "actual_coverage_seconds": actual_coverage_seconds,
                "observation_completeness": observation_completeness,
                "source_class": source_class,
                "paper_sizing_assumptions": assumptions[0],
                "paper_fee_assumptions": assumptions[1],
                "paper_slippage_assumptions": assumptions[2],
                "allocated_capital_net_return": monetary_payload[0],
                "realized_pnl": monetary_payload[1],
                "unrealized_pnl": monetary_payload[2],
                "fees": monetary_payload[3],
                "costs": monetary_payload[4],
                "drawdown": monetary_payload[5],
                "completed_outcomes": completed_outcomes,
                "reliability": reliability,
                "execution_feasibility": execution_feasibility,
                **(
                    {
                        "accounting_available": accounting_available,
                        "accounting_complete": accounting_complete,
                        "accounting_partial": accounting_partial,
                    }
                    if v2_provenance
                    else {}
                ),
                **shared,
            }
        )
        if evaluation_document:
            payload_data["evaluation"] = evaluation_document
        if accounting_document:
            payload_data["portfolio_accounting"] = accounting_document
        if accounting_unavailable:
            for field_name in _ROLLING_V2_ACCOUNTING_ROOT_REDACTED_FIELDS:
                payload_data[field_name] = None
                for alias in _ROLLING_ACCOUNTING_ALIASES.get(field_name, ()):
                    payload_data.pop(alias, None)
            metrics_payload = payload_data.get("metrics")
            if isinstance(metrics_payload, Mapping):
                metrics_payload = dict(metrics_payload)
                if "portfolio_accounting" in metrics_payload:
                    metrics_payload["portfolio_accounting"] = dict(accounting_document)
                payload_data["metrics"] = metrics_payload
        computed_evidence_digest = _rolling_evidence_digest(payload_data)
        if supplied_evidence_digest is not None and supplied_evidence_digest != computed_evidence_digest:
            raise ValueError("evidence_digest does not match canonical evidence")
        evidence_digest = computed_evidence_digest
        payload_data["evidence_digest"] = evidence_digest
        payload_json = _rolling_dump(payload_data)
        values = (
            identifier,
            strategy_version_id,
            research_trial_id,
            candidate_id,
            available_from,
            available_through,
            requested_days_value,
            actual_coverage_seconds,
            observation_completeness,
            source_class,
            _rolling_dump(assumptions[0]),
            _rolling_dump(assumptions[1]),
            _rolling_dump(assumptions[2]),
            *monetary,
            completed_outcomes,
            reliability,
            execution_feasibility,
            evidence_digest,
            payload_json,
            created_at,
            shared.get("evaluation_run_id"),
            shared.get("evaluation_version"),
            shared.get("supersedes_evidence_id"),
            shared.get("loaded_rows"),
            shared.get("valid_input_rows"),
            None if shared.get("evaluator_invoked") is None else int(shared["evaluator_invoked"]),
            None if shared.get("evaluator_completed") is None else int(shared["evaluator_completed"]),
            shared.get("evaluated_observations"),
            shared.get("signal_count"),
            shared.get("diagnostic_summary_count"),
            shared.get("evaluator_name"),
            shared.get("evaluator_error"),
            shared.get("evaluator_prerequisite"),
            None if accounting_available is None else int(accounting_available),
            _rolling_dump(evaluation_document) if evaluation_document else None,
            _rolling_dump(accounting_document) if accounting_document else None,
        )
        with self._write_context():
            if self._conn.execute(
                "SELECT 1 FROM strategy_versions WHERE strategy_version_id=?",
                (strategy_version_id,),
            ).fetchone() is None:
                raise ValueError("evidence window strategy version does not exist")
            supersedes_evidence_id = shared.get("supersedes_evidence_id")
            if supersedes_evidence_id:
                if supersedes_evidence_id == identifier:
                    raise ValueError("supersedes_evidence_id cannot reference itself")
                predecessor = self._conn.execute(
                    "SELECT strategy_version_id,candidate_id,research_trial_id,"
                    "requested_days,source_class "
                    "FROM strategy_evidence_windows WHERE evidence_window_id=?",
                    (supersedes_evidence_id,),
                ).fetchone()
                if predecessor is None:
                    raise ValueError("supersedes_evidence_id predecessor does not exist")
                predecessor_identity = (
                    predecessor["strategy_version_id"],
                    predecessor["candidate_id"],
                    predecessor["research_trial_id"],
                    int(predecessor["requested_days"]),
                    str(predecessor["source_class"]).strip().upper(),
                )
                current_identity = (
                    strategy_version_id,
                    candidate_id,
                    research_trial_id,
                    requested_days_value,
                    source_class,
                )
                if predecessor_identity != current_identity:
                    raise ValueError("supersedes_evidence_id predecessor identity mismatch")
            existing = self._conn.execute(
                "SELECT * FROM strategy_evidence_windows WHERE evidence_window_id=?",
                (identifier,),
            ).fetchone()
            immutable_columns = (
                "strategy_version_id",
                "research_trial_id",
                "candidate_id",
                "available_from",
                "available_through",
                "requested_days",
                "actual_coverage_seconds",
                "observation_completeness",
                "source_class",
                "paper_sizing_assumptions_json",
                "paper_fee_assumptions_json",
                "paper_slippage_assumptions_json",
                "allocated_capital_net_return",
                "realized_pnl",
                "unrealized_pnl",
                "fees",
                "costs",
                "drawdown",
                "completed_outcomes",
                "reliability",
                "execution_feasibility",
                "evidence_digest",
                "evaluation_run_id",
                "evaluation_version",
                "supersedes_evidence_id",
                "loaded_rows",
                "valid_input_rows",
                "evaluator_invoked",
                "evaluator_completed",
                "evaluated_observations",
                "signal_count",
                "diagnostic_summary_count",
                "evaluator_name",
                "evaluator_error",
                "evaluator_prerequisite",
                "accounting_available",
                "evaluation_json",
                "portfolio_accounting_json",
            )
            expected_by_column = dict(
                zip(
                    (
                        "strategy_version_id",
                        "research_trial_id",
                        "candidate_id",
                        "available_from",
                        "available_through",
                        "requested_days",
                        "actual_coverage_seconds",
                        "observation_completeness",
                        "source_class",
                        "paper_sizing_assumptions_json",
                        "paper_fee_assumptions_json",
                        "paper_slippage_assumptions_json",
                        "allocated_capital_net_return",
                        "realized_pnl",
                        "unrealized_pnl",
                        "fees",
                        "costs",
                        "drawdown",
                        "completed_outcomes",
                        "reliability",
                        "execution_feasibility",
                        "evidence_digest",
                    ),
                    values[1:23],
                )
            )
            expected_by_column.update(
                {
                    "evaluation_run_id": shared.get("evaluation_run_id"),
                    "evaluation_version": shared.get("evaluation_version"),
                    "supersedes_evidence_id": shared.get("supersedes_evidence_id"),
                    "loaded_rows": shared.get("loaded_rows"),
                    "valid_input_rows": shared.get("valid_input_rows"),
                    "evaluator_invoked": None
                    if shared.get("evaluator_invoked") is None
                    else int(shared["evaluator_invoked"]),
                    "evaluator_completed": None
                    if shared.get("evaluator_completed") is None
                    else int(shared["evaluator_completed"]),
                    "evaluated_observations": shared.get("evaluated_observations"),
                    "signal_count": shared.get("signal_count"),
                    "diagnostic_summary_count": shared.get("diagnostic_summary_count"),
                    "evaluator_name": shared.get("evaluator_name"),
                    "evaluator_error": shared.get("evaluator_error"),
                    "evaluator_prerequisite": shared.get("evaluator_prerequisite"),
                    "accounting_available": None
                    if accounting_available is None
                    else int(accounting_available),
                    "evaluation_json": _rolling_dump(evaluation_document)
                    if evaluation_document
                    else None,
                    "portfolio_accounting_json": _rolling_dump(accounting_document)
                    if accounting_document
                    else None,
                }
            )
            if existing is not None:
                if unavailable_monetary_input:
                    raise ValueError("strategy evidence window identity conflict")
                def existing_column_matches(column: str) -> bool:
                    actual = existing[column]
                    expected = expected_by_column[column]
                    if column == "evaluation_json":
                        return _rolling_optional_document_equal(
                            actual,
                            expected,
                            nullable_fields=frozenset(
                                {
                                    "evaluation_run_id",
                                    "evaluation_version",
                                    "supersedes_evidence_id",
                                }
                            ),
                        )
                    if column != "accounting_available":
                        return actual == expected
                    if actual is None:
                        try:
                            legacy_payload = _load(existing["payload_json"])
                        except (TypeError, ValueError, json.JSONDecodeError):
                            legacy_payload = {}
                        if not isinstance(legacy_payload, Mapping):
                            legacy_payload = {}
                        legacy_accounting = legacy_payload.get("portfolio_accounting")
                        if not isinstance(legacy_accounting, Mapping):
                            metrics = legacy_payload.get("metrics")
                            legacy_accounting = (
                                metrics.get("portfolio_accounting")
                                if isinstance(metrics, Mapping)
                                else {}
                            )
                        if isinstance(legacy_accounting, Mapping):
                            actual = legacy_accounting.get("accounting_available")
                        if actual is None:
                            actual = legacy_payload.get("accounting_available")
                    if actual is None or expected is None:
                        return actual is expected
                    return bool(actual) is bool(expected)

                payload_ignored = frozenset({"created_at"})
                if existing["evaluation_run_id"] is None and existing["evaluation_version"] is None:
                    payload_ignored = frozenset({"created_at", "requested_window_days"})
                existing_payload = _rolling_payload_for_identity(
                    _load(existing["payload_json"]),
                    v2_provenance=v2_provenance,
                )
                candidate_payload = _rolling_payload_for_identity(
                    payload_data,
                    v2_provenance=v2_provenance,
                )
                if any(
                    not existing_column_matches(column)
                    for column in immutable_columns
                ) or not _rolling_payload_equal(
                    existing_payload,
                    candidate_payload,
                    ignored=payload_ignored,
                ):
                    raise ValueError("strategy evidence window identity conflict")
                return
            columns = (
                "evidence_window_id,strategy_version_id,research_trial_id,candidate_id,"
                "available_from,available_through,requested_days,actual_coverage_seconds,"
                "observation_completeness,source_class,"
                "paper_sizing_assumptions_json,paper_fee_assumptions_json,paper_slippage_assumptions_json,"
                "allocated_capital_net_return,realized_pnl,unrealized_pnl,fees,costs,drawdown,"
                "completed_outcomes,reliability,execution_feasibility,evidence_digest,payload_json,created_at,"
                "evaluation_run_id,evaluation_version,supersedes_evidence_id,loaded_rows,valid_input_rows,"
                "evaluator_invoked,evaluator_completed,evaluated_observations,signal_count,"
                "diagnostic_summary_count,evaluator_name,evaluator_error,evaluator_prerequisite,"
                "accounting_available,evaluation_json,portfolio_accounting_json"
            )
            self._conn.execute(
                f"INSERT INTO strategy_evidence_windows({columns}) "
                f"VALUES ({','.join('?' for _ in values)})",
                values,
            )

    def list_strategy_evidence_windows(
        self,
        strategy_version_id: str | None = None,
        *,
        limit: int | None = 100,
    ) -> list[dict[str, Any]]:
        limit_value = _rolling_limit(limit, default=100)
        values: list[Any] = []
        query = "SELECT * FROM strategy_evidence_windows"
        if strategy_version_id is not None:
            query += " WHERE strategy_version_id=?"
            values.append(str(strategy_version_id))
        query += " ORDER BY available_through DESC,available_from DESC,evidence_window_id DESC LIMIT ?"
        values.append(limit_value)
        with self._lock:
            rows = self._conn.execute(query, values).fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            try:
                payload = _load(row["payload_json"])
            except (TypeError, ValueError, json.JSONDecodeError):
                payload = {}
            item = dict(payload) if isinstance(payload, Mapping) else {}
            metrics = item.get("metrics")
            metrics = metrics if isinstance(metrics, Mapping) else {}
            payload_evaluation = item.get("evaluation")
            payload_evaluation = (
                dict(payload_evaluation)
                if isinstance(payload_evaluation, Mapping)
                else {}
            )
            metrics_evaluation = metrics.get("evaluation")
            metrics_evaluation = (
                dict(metrics_evaluation)
                if isinstance(metrics_evaluation, Mapping)
                else {}
            )
            try:
                evaluation_column = (
                    _load(row["evaluation_json"])
                    if row["evaluation_json"]
                    else {}
                )
            except (TypeError, ValueError, json.JSONDecodeError):
                evaluation_column = {}
            evaluation_column = (
                dict(evaluation_column)
                if isinstance(evaluation_column, Mapping)
                else {}
            )
            evaluation = dict(
                evaluation_column
                or payload_evaluation
                or metrics_evaluation
            )
            payload_portfolio = item.get("portfolio_accounting")
            payload_portfolio = (
                dict(payload_portfolio)
                if isinstance(payload_portfolio, Mapping)
                else {}
            )
            metrics_portfolio = metrics.get("portfolio_accounting")
            metrics_portfolio = (
                dict(metrics_portfolio)
                if isinstance(metrics_portfolio, Mapping)
                else {}
            )
            try:
                portfolio_column = (
                    _load(row["portfolio_accounting_json"])
                    if row["portfolio_accounting_json"]
                    else {}
                )
            except (TypeError, ValueError, json.JSONDecodeError):
                portfolio_column = {}
            portfolio_column = (
                dict(portfolio_column)
                if isinstance(portfolio_column, Mapping)
                else {}
            )
            portfolio = dict(
                portfolio_column
                or payload_portfolio
                or metrics_portfolio
            )
            portfolio_payload_present = bool(row["portfolio_accounting_json"]) or any(
                isinstance(candidate, Mapping)
                for candidate in (
                    item.get("portfolio_accounting"),
                    metrics.get("portfolio_accounting"),
                )
            )
            evaluation_projections = (
                ("payload", item),
                ("evaluation", payload_evaluation),
                ("metrics.evaluation", metrics_evaluation),
                ("evaluation_json", evaluation_column),
            )
            accounting_projections = (
                ("payload", item),
                ("portfolio_accounting", payload_portfolio),
                ("metrics.portfolio_accounting", metrics_portfolio),
                ("portfolio_accounting_json", portfolio_column),
            )
            discriminator_fields = (
                "evaluation_kind",
                "evaluation_run_id",
                "evaluation_version",
                "supersedes_evidence_id",
            )
            v2_provenance = any(
                field_name in projection
                and projection[field_name] not in (None, "")
                for _, projection in evaluation_projections
                for field_name in discriminator_fields
            )
            for field_name in (
                "evaluation_run_id",
                "evaluation_version",
                "supersedes_evidence_id",
            ):
                column_value = (
                    row[field_name]
                    if field_name in row.keys()
                    else _ROLLING_HYDRATION_MISSING
                )
                if column_value not in (_ROLLING_HYDRATION_MISSING, None, ""):
                    v2_provenance = True
            kind_present, evaluation_kind = _rolling_reconcile_hydrated_field(
                "evaluation_kind",
                evaluation_projections,
                normalize=lambda value: _rolling_evaluation_kind(value),
                v2_provenance=v2_provenance,
            )
            if kind_present and evaluation_kind is not None:
                v2_provenance = True
            reconciled_evaluation: dict[str, tuple[bool, Any]] = {}
            for field_name in ("evaluator_invoked", "evaluator_completed"):
                column_value = (
                    row[field_name]
                    if field_name in row.keys()
                    else _ROLLING_HYDRATION_MISSING
                )
                reconciled_evaluation[field_name] = _rolling_reconcile_hydrated_field(
                    field_name,
                    evaluation_projections,
                    sql_value=column_value,
                    normalize=lambda value, name=field_name: _rolling_optional_boolean(
                        value,
                        name=name,
                    ),
                    v2_provenance=v2_provenance,
                )
            for field_name in (
                "evaluation_run_id",
                "evaluation_version",
                "supersedes_evidence_id",
            ):
                column_value = (
                    row[field_name]
                    if field_name in row.keys()
                    else _ROLLING_HYDRATION_MISSING
                )
                reconciled_evaluation[field_name] = (
                    _rolling_reconcile_hydrated_field(
                        field_name,
                        evaluation_projections,
                        sql_value=column_value,
                        normalize=lambda value: (
                            _rolling_optional_text({"value": value}, "value")
                            or None
                        ),
                        v2_provenance=v2_provenance,
                    )
                )
            if kind_present:
                reconciled_evaluation["evaluation_kind"] = (
                    True,
                    evaluation_kind,
                )
            for field_name, (present, value) in reconciled_evaluation.items():
                sql_present = (
                    field_name in row.keys()
                    and row[field_name] is not None
                )
                nested_present = any(
                    field_name in projection
                    for _, projection in evaluation_projections[1:]
                )
                if present and (
                    nested_present
                    or sql_present
                    or (v2_provenance and value is not None)
                ):
                    evaluation[field_name] = value
            evaluation_fields = (
                "evaluation_kind",
                "evaluation_run_id",
                "evaluation_version",
                "supersedes_evidence_id",
                "loaded_rows",
                "valid_input_rows",
                "evaluator_invoked",
                "evaluator_completed",
                "evaluated_observations",
                "signal_count",
                "diagnostic_summary_count",
                "evaluator_name",
                "evaluator_error",
                "evaluator_prerequisite",
            )
            for field_name in evaluation_fields:
                if field_name in reconciled_evaluation:
                    continue
                column_value = (
                    row[field_name]
                    if field_name in row.keys()
                    else None
                )
                if column_value is not None:
                    evaluation[field_name] = (
                        bool(column_value)
                        if field_name in {"evaluator_invoked", "evaluator_completed"}
                        else int(column_value)
                        if field_name in {
                            "loaded_rows",
                            "valid_input_rows",
                            "evaluated_observations",
                            "signal_count",
                            "diagnostic_summary_count",
                        }
                        else column_value
                    )
            legacy = not v2_provenance
            if v2_provenance:
                evaluation["evaluation_kind"] = _rolling_evaluation_kind(
                    evaluation.get("evaluation_kind"),
                    default="CANONICAL_SIMULATION",
                )
            reconciled_accounting: dict[str, tuple[bool, Any]] = {}
            for field_name in (
                "accounting_available",
                "accounting_complete",
                "accounting_partial",
            ):
                column_value = (
                    row[field_name]
                    if field_name in row.keys()
                    else _ROLLING_HYDRATION_MISSING
                )
                reconciled_accounting[field_name] = _rolling_reconcile_hydrated_field(
                    field_name,
                    accounting_projections,
                    sql_value=column_value,
                    normalize=lambda value, name=field_name: _rolling_optional_boolean(
                        value,
                        name=name,
                    ),
                    v2_provenance=v2_provenance,
                )
            for field_name, (present, value) in reconciled_accounting.items():
                if present and (
                    v2_provenance
                    or any(
                        field_name in projection
                        for _, projection in accounting_projections[1:]
                    )
                ):
                    portfolio[field_name] = value
            accounting_available = reconciled_accounting["accounting_available"][1]
            accounting_complete = reconciled_accounting["accounting_complete"][1]
            accounting_partial = reconciled_accounting["accounting_partial"][1]
            evaluator_ready = (
                (
                    evaluation.get("evaluator_invoked") is False
                    and evaluation.get("evaluator_completed") is False
                )
                if evaluation.get("evaluation_kind") == "ACTUAL_LEDGER"
                else (
                    evaluation.get("evaluator_invoked") is True
                    and evaluation.get("evaluator_completed") is True
                )
            )
            accounting_ready = v2_provenance and (
                accounting_available is True
                and accounting_complete is True
                and accounting_partial is False
                and evaluator_ready
                and _rolling_v2_accounting_fields_usable(portfolio)
            )
            if v2_provenance and not accounting_ready:
                accounting_available = False
                accounting_complete = False
                accounting_partial = True
                portfolio["accounting_available"] = False
                portfolio["accounting_complete"] = False
                portfolio["accounting_partial"] = True
                item["accounting_available"] = False
                item["accounting_complete"] = False
                item["accounting_partial"] = True
            portfolio_fields = (
                "initial_cash",
                "cash",
                "equity",
                "realized_pnl",
                "unrealized_pnl",
                "net_pnl",
                "fees",
                "costs",
                "open_positions",
                "opening_fills",
                "closing_fills",
                "partial_closing_fills",
                "completed_round_trips",
            )
            for field_name in portfolio_fields:
                if (
                    field_name in item
                    and field_name not in portfolio
                    and (
                        accounting_ready
                        if v2_provenance
                        else portfolio_payload_present
                    )
                ):
                    portfolio[field_name] = item[field_name]
            if "completed_round_trips" in portfolio and portfolio["completed_round_trips"] is not None:
                try:
                    portfolio["completed_round_trips"] = int(portfolio["completed_round_trips"])
                except (TypeError, ValueError):
                    portfolio["completed_round_trips"] = None
            if v2_provenance and not accounting_ready:
                for field_name in (
                    "initial_cash",
                    "cash",
                    "equity",
                    "realized_pnl",
                    "unrealized_pnl",
                    "net_pnl",
                    "fees",
                    "costs",
                ):
                    if field_name in portfolio:
                        portfolio[field_name] = None

            def stored_metric(field_name: str) -> Any:
                if not v2_provenance:
                    return row[field_name]
                if not accounting_ready:
                    return None
                if field_name in portfolio:
                    return portfolio[field_name]
                if field_name in item:
                    return item[field_name]
                # The SQL columns for these fields are legacy NOT NULL
                # compatibility columns.  They are never evidence for v2.
                return None
            accounting_status = (
                "AVAILABLE"
                if accounting_available is True
                else "UNAVAILABLE"
                if accounting_available is False
                else "UNKNOWN"
            )
            item.update(
                {
                    "evidence_window_id": row["evidence_window_id"],
                    "strategy_version_id": row["strategy_version_id"],
                    "research_trial_id": row["research_trial_id"],
                    "candidate_id": row["candidate_id"],
                    "available_from": row["available_from"],
                    "available_through": row["available_through"],
                    "requested_days": int(row["requested_days"]),
                    "requested_window_days": int(row["requested_days"]),
                    "actual_coverage_seconds": int(row["actual_coverage_seconds"]),
                    "observation_completeness": row["observation_completeness"],
                    "source_class": row["source_class"],
                    "paper_sizing_assumptions": _load(row["paper_sizing_assumptions_json"]),
                    "paper_fee_assumptions": _load(row["paper_fee_assumptions_json"]),
                    "paper_slippage_assumptions": _load(row["paper_slippage_assumptions_json"]),
                    "allocated_capital_net_return": (
                        stored_metric("allocated_capital_net_return")
                        if v2_provenance
                        else row["allocated_capital_net_return"]
                    ),
                    "realized_pnl": stored_metric("realized_pnl"),
                    "unrealized_pnl": stored_metric("unrealized_pnl"),
                    "fees": stored_metric("fees"),
                    "costs": stored_metric("costs"),
                    "drawdown": row["drawdown"],
                    "completed_outcomes": int(row["completed_outcomes"]),
                    "reliability": row["reliability"],
                    "execution_feasibility": row["execution_feasibility"],
                    "evidence_digest": row["evidence_digest"],
                    "created_at": row["created_at"],
                    "evaluation": evaluation,
                    "portfolio_accounting": (
                        portfolio
                        if v2_provenance or portfolio_payload_present
                        else item.get("portfolio_accounting")
                    ),
                    "accounting_available": accounting_available,
                    "accounting_status": accounting_status,
                    "evaluation_legacy": legacy,
                    "evaluation_run_id": evaluation.get("evaluation_run_id"),
                    "evaluation_version": evaluation.get("evaluation_version"),
                    "supersedes_evidence_id": evaluation.get("supersedes_evidence_id"),
                }
            )
            if not v2_provenance:
                if not portfolio_payload_present:
                    item.pop("portfolio_accounting", None)
                if not evaluation and "evaluation" not in payload:
                    item.pop("evaluation", None)
                if (
                    accounting_available is None
                    and "accounting_available" not in payload
                ):
                    item.pop("accounting_available", None)
                for field_name in (
                    "evaluation_run_id",
                    "evaluation_version",
                    "supersedes_evidence_id",
                ):
                    if field_name not in payload:
                        item.pop(field_name, None)
            for field_name in evaluation_fields:
                if field_name in evaluation:
                    item[field_name] = evaluation[field_name]
            for field_name in portfolio_fields:
                if field_name in portfolio:
                    item[field_name] = portfolio[field_name]
            if v2_provenance and not accounting_ready:
                for field_name in (
                    "allocated_capital_net_return",
                    "realized_pnl",
                    "unrealized_pnl",
                    "net_pnl",
                    "fees",
                    "costs",
                ):
                    item[field_name] = None
            elif not v2_provenance and accounting_available is not True:
                for field_name in (
                    "realized_pnl",
                    "unrealized_pnl",
                    "net_pnl",
                    "fees",
                    "costs",
                ):
                    if field_name in portfolio:
                        item[field_name] = portfolio.get(field_name)
                if "net_pnl" in portfolio:
                    item["allocated_capital_net_return"] = portfolio.get("net_pnl")
            result.append(item)
        return result
    def load_rolling_evidence_cursor(self) -> dict[str, Any] | None:
        """Load the durable rolling evidence round-robin boundary."""
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM rolling_evidence_cursor WHERE cursor_id='current'"
            ).fetchone()
        if row is None:
            return None
        result = dict(row)
        for field in ("attempt_timestamps_json", "attempts_json"):
            raw = result.pop(field, "{}")
            try:
                decoded = _load(raw)
            except (TypeError, ValueError, json.JSONDecodeError):
                decoded = {}
            result[field.removesuffix("_json")] = decoded if isinstance(decoded, Mapping) else {}
        return result

    def save_rolling_evidence_cursor(self, record: Any) -> None:
        """Persist a restart-safe rolling evidence cursor and attempt telemetry."""
        data = _rolling_mapping(record, name="rolling_evidence_cursor")
        attempts = data.get("attempt_timestamps", data.get("attempt_timestamps_json", {}))
        counts = data.get("attempts", data.get("attempts_json", {}))
        if isinstance(attempts, str):
            try:
                attempts = _load(attempts)
            except (TypeError, ValueError, json.JSONDecodeError):
                attempts = {}
        if isinstance(counts, str):
            try:
                counts = _load(counts)
            except (TypeError, ValueError, json.JSONDecodeError):
                counts = {}
        if not isinstance(attempts, Mapping) or not isinstance(counts, Mapping):
            raise ValueError("rolling evidence cursor attempts must be mappings")
        attempts = {str(key)[:512]: str(value)[:128] for key, value in list(attempts.items())[:256]}
        normalized_counts: dict[str, int] = {}
        for key, value in list(counts.items())[:256]:
            try:
                count = int(value)
            except (TypeError, ValueError, OverflowError) as exc:
                raise ValueError("rolling evidence cursor attempt count is invalid") from exc
            if count < 0:
                raise ValueError("rolling evidence cursor attempt count is invalid")
            normalized_counts[str(key)[:512]] = count
        def optional_text(*names: str) -> str | None:
            return _rolling_optional_text(data, *names) or None
        def optional_days(name: str) -> int | None:
            value = data.get(name)
            if value in (None, ""):
                return None
            parsed = _rolling_nonnegative_integer(value, name=name)
            if parsed not in {7, 30}:
                raise ValueError(f"{name} must be 7 or 30")
            return parsed
        values = (
            "current",
            optional_text("last_strategy_version_id"),
            optional_text("last_research_trial_id"),
            optional_text("last_candidate_id"),
            optional_days("last_requested_days"),
            optional_text("last_source_class"),
            optional_text("next_strategy_version_id"),
            optional_text("next_research_trial_id"),
            optional_text("next_candidate_id"),
            optional_days("next_requested_days"),
            optional_text("next_source_class"),
            _rolling_dump(attempts),
            _rolling_dump(normalized_counts),
            _rolling_timestamp(data.get("updated_at"), name="updated_at", default_now=True),
        )
        with self._write_context():
            self._conn.execute(
                "INSERT INTO rolling_evidence_cursor("
                "cursor_id,last_strategy_version_id,last_research_trial_id,last_candidate_id,"
                "last_requested_days,last_source_class,next_strategy_version_id,"
                "next_research_trial_id,next_candidate_id,next_requested_days,next_source_class,"
                "attempt_timestamps_json,attempts_json,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(cursor_id) DO UPDATE SET "
                "last_strategy_version_id=excluded.last_strategy_version_id,"
                "last_research_trial_id=excluded.last_research_trial_id,"
                "last_candidate_id=excluded.last_candidate_id,"
                "last_requested_days=excluded.last_requested_days,"
                "last_source_class=excluded.last_source_class,"
                "next_strategy_version_id=excluded.next_strategy_version_id,"
                "next_research_trial_id=excluded.next_research_trial_id,"
                "next_candidate_id=excluded.next_candidate_id,"
                "next_requested_days=excluded.next_requested_days,"
                "next_source_class=excluded.next_source_class,"
                "attempt_timestamps_json=excluded.attempt_timestamps_json,"
                "attempts_json=excluded.attempts_json,"
                "updated_at=excluded.updated_at",
                values,
            )

    def save_rolling_evidence_blocker(self, record: Any) -> bool:
        """Persist one terminal prerequisite blocker, deduplicated by fingerprint."""
        data = _rolling_mapping(record, name="rolling_evidence_blocker")
        strategy_id = _rolling_required_text(data, "strategy_version_id", name="strategy_version_id")
        trial_id = _rolling_optional_text(data, "research_trial_id", "trial_id")
        candidate_id = _rolling_optional_text(data, "candidate_id")
        requested_days = _rolling_nonnegative_integer(
            data.get("requested_days", data.get("requested_window_days")),
            name="requested_days",
        )
        if requested_days not in {7, 30}:
            raise ValueError("requested_days must be 7 or 30")
        source_class = _rolling_required_text(data, "source_class", name="source_class").upper()
        fingerprint = _rolling_required_text(
            data,
            "prerequisite_fingerprint",
            "fingerprint",
            name="prerequisite_fingerprint",
        )
        blocker = _rolling_required_text(data, "blocker", "reason", name="blocker")
        detail = _rolling_optional_text(data, "detail", "error") or ""
        work_key = _rolling_optional_text(data, "work_key") or _rolling_hash(
            {
                "strategy_version_id": strategy_id,
                "research_trial_id": trial_id,
                "candidate_id": candidate_id,
                "requested_days": requested_days,
                "source_class": source_class,
            }
        )
        blocker_id = "rolling-blocker-" + _rolling_hash(
            {"work_key": work_key, "prerequisite_fingerprint": fingerprint}
        ).removeprefix("sha256:")[:48]
        now = _rolling_timestamp(
            data.get("last_attempted_at", data.get("created_at")),
            name="last_attempted_at",
            default_now=True,
        )
        first = _rolling_timestamp(data.get("first_seen_at"), name="first_seen_at", default_now=False) or now
        try:
            attempts = int(data.get("attempts", 1))
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError("blocker attempts must be an integer") from exc
        if attempts < 1:
            raise ValueError("blocker attempts must be positive")
        payload = data.get("payload", data.get("provenance", {}))
        payload = payload if isinstance(payload, Mapping) else {}
        with self._write_context():
            existing = self._conn.execute(
                "SELECT attempts,first_seen_at FROM rolling_evidence_blockers "
                "WHERE blocker_id=?",
                (blocker_id,),
            ).fetchone()
            if existing is not None:
                attempts = max(attempts, int(existing["attempts"]) + 1)
                first = existing["first_seen_at"]
            self._conn.execute(
                "INSERT INTO rolling_evidence_blockers("
                "blocker_id,work_key,strategy_version_id,research_trial_id,candidate_id,"
                "requested_days,source_class,prerequisite_fingerprint,blocker,detail,"
                "first_seen_at,last_attempted_at,attempts,payload_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(blocker_id) DO UPDATE SET "
                "detail=excluded.detail,last_attempted_at=excluded.last_attempted_at,"
                "attempts=excluded.attempts,payload_json=excluded.payload_json",
                (
                    blocker_id,
                    work_key,
                    strategy_id,
                    trial_id,
                    candidate_id,
                    requested_days,
                    source_class,
                    fingerprint,
                    blocker,
                    detail[:1024],
                    first,
                    now,
                    attempts,
                    _rolling_dump(payload),
                ),
            )
        return existing is None

    def load_rolling_evidence_blocker(
        self,
        *,
        work_key: str,
        prerequisite_fingerprint: str | None = None,
    ) -> dict[str, Any] | None:
        """Return the latest blocker for one work tuple."""
        key = str(work_key).strip()
        if not key:
            raise ValueError("work_key is required")
        query = "SELECT * FROM rolling_evidence_blockers WHERE work_key=?"
        values: list[Any] = [key]
        if prerequisite_fingerprint:
            query += " AND prerequisite_fingerprint=?"
            values.append(str(prerequisite_fingerprint).strip())
        query += " ORDER BY last_attempted_at DESC,blocker_id DESC LIMIT 1"
        with self._lock:
            row = self._conn.execute(query, values).fetchone()
        if row is None:
            return None
        result = dict(row)
        try:
            payload = _load(result.pop("payload_json", "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            payload = {}
        result["payload"] = payload if isinstance(payload, Mapping) else {}
        return result

    def list_rolling_evidence_blockers(self, *, limit: int | None = 100) -> list[dict[str, Any]]:
        limit_value = _rolling_limit(limit, default=100)
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM rolling_evidence_blockers "
                "ORDER BY last_attempted_at DESC,blocker_id DESC LIMIT ?",
                (limit_value,),
            ).fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            try:
                payload = _load(item.pop("payload_json", "{}"))
            except (TypeError, ValueError, json.JSONDecodeError):
                payload = {}
            item["payload"] = payload if isinstance(payload, Mapping) else {}
            result.append(item)
        return result


    def _rolling_selection_record(
        self,
        row: sqlite3.Row,
        *,
        include_members: bool = True,
    ) -> dict[str, Any]:
        payload = _load(row["payload_json"])
        result = dict(payload) if isinstance(payload, Mapping) else {}
        result.update(
            {
                "portfolio_selection_id": row["portfolio_selection_id"],
                "global_budget": row["global_budget"],
                "k": int(row["k"]),
                "policy_id": row["policy_id"],
                "risk_config_id": row["risk_config_id"],
                "active_risk_config_id": row["risk_config_id"],
                "risk_config_generation": int(row["risk_config_generation"]),
                "active_risk_config_generation": int(row["risk_config_generation"]),
                "risk_config_hash": row["risk_config_hash"],
                "active_risk_config_hash": row["risk_config_hash"],
                "selected_at": row["selected_at"],
                "review_due_at": row["review_due_at"],
                "committed_at": row["committed_at"],
            }
        )
        if include_members:
            member_rows = self._conn.execute(
                "SELECT * FROM portfolio_selection_members "
                "WHERE portfolio_selection_id=? "
                "ORDER BY CAST(score AS REAL) DESC,strategy_version_id",
                (row["portfolio_selection_id"],),
            ).fetchall()
            members: list[dict[str, Any]] = []
            for member in member_rows:
                member_payload = _load(member["payload_json"])
                item = dict(member_payload) if isinstance(member_payload, Mapping) else {}
                item.update(
                    {
                        "portfolio_selection_id": member["portfolio_selection_id"],
                        "strategy_version_id": member["strategy_version_id"],
                        "research_trial_id": member["research_trial_id"],
                        "candidate_id": member["candidate_id"],
                        "allocation": member["allocation"],
                        "status": member["status"],
                        "score": member["score"],
                        "reason": member["reason"],
                        "evidence_window_id": member["evidence_window_id"],
                        "overlap_key": member["overlap_key"],
                        "created_at": member["created_at"],
                    }
                )
                members.append(item)
            result["members"] = members
            # ``k`` is the maintained/funded count persisted on the selection;
            # exit tombstones are intentionally included in ``members`` but
            # must not redefine the selection cardinality.
            result["k"] = int(row["k"])
        return result

    def commit_portfolio_selection(self, selection: Any, members: Iterable[Any]) -> dict[str, Any]:
        data = _rolling_mapping(selection, name="portfolio_selection")
        for aliases, label in (
            (("policy_id", "admission_policy_id"), "policy"),
            (("policy_version", "version"), "policy version"),
            (
                ("active_risk_config_id", "risk_config_id", "config_id"),
                "risk config",
            ),
            (
                (
                    "active_risk_config_generation",
                    "risk_config_generation",
                    "risk_generation",
                    "generation",
                ),
                "risk generation",
            ),
            (
                ("active_risk_config_hash", "risk_config_hash", "config_hash"),
                "risk config hash",
            ),
        ):
            if _rolling_identity_conflict(data, *aliases):
                raise ValueError(f"portfolio selection {label} identity conflicts")
        identifier = _rolling_required_text(
            data,
            "portfolio_selection_id",
            "selection_id",
            name="portfolio_selection_id",
        )
        policy_id = _rolling_required_text(data, "policy_id", name="policy_id")
        policy_version = _rolling_required_text(
            data,
            "policy_version",
            "version",
            name="policy_version",
        )
        risk_config_id = _rolling_required_text(
            data,
            "active_risk_config_id",
            "risk_config_id",
            "config_id",
            name="active_risk_config_id",
        )
        risk_config_generation = _rolling_nonnegative_integer(
            data.get(
                "active_risk_config_generation",
                data.get(
                    "risk_config_generation",
                    data.get("risk_generation", data.get("generation")),
                ),
            ),
            name="active_risk_config_generation",
            default=0,
        )
        risk_config_hash = _rolling_required_text(
            data,
            "active_risk_config_hash",
            "risk_config_hash",
            "config_hash",
            name="active_risk_config_hash",
        )
        global_budget = _rolling_decimal_text(
            data.get("global_budget", data.get("global_budget_usd", data.get("budget"))),
            name="global_budget",
            nonnegative=True,
        )
        selected_at = _rolling_timestamp(
            data.get("selected_at"),
            name="selected_at",
            required=True,
        )
        review_due_at = _rolling_timestamp(
            data.get("review_due_at"),
            name="review_due_at",
            required=True,
        )
        if (
            selected_at is not None
            and review_due_at is not None
            and _parse_datetime(review_due_at) < _parse_datetime(selected_at)
        ):
            raise ValueError("portfolio selection review_due_at precedes selected_at")
        committed_at = _rolling_timestamp(
            data.get("committed_at"),
            name="committed_at",
            default_now=True,
        )
        member_data = [_rolling_mapping(item, name="portfolio_selection_member") for item in members]
        if not 0 <= len(member_data) <= 32:
            raise ValueError("portfolio selection member payload exceeds hard bound")
        k_raw = data.get("k")
        declared_k = (
            _rolling_nonnegative_integer(k_raw, name="k")
            if k_raw is not None
            else None
        )
        allocations: list[Decimal] = []
        seen_strategies: set[str] = set()
        seen_funded_overlaps: set[str] = set()
        normalized_members: list[dict[str, Any]] = []
        for member in member_data:
            strategy_version = _rolling_required_text(
                member,
                "strategy_version_id",
                name="member strategy_version_id",
            )
            if strategy_version in seen_strategies:
                raise ValueError("portfolio selection contains duplicate strategy member")
            seen_strategies.add(strategy_version)
            allocation = _rolling_decimal(
                member.get("allocation"),
                name="member allocation",
                nonnegative=True,
            )
            allocations.append(allocation)
            score = _rolling_decimal_text(member.get("score", "0"), name="member score")
            status = _rolling_required_text(member, "status", name="member status")
            reason = _rolling_optional_text(member, "reason")
            for aliases, label in (
                (("research_trial_id", "trial_id"), "research trial"),
                (("candidate_id", "candidate", "strategy_candidate_id"), "candidate"),
                (("evidence_window_id", "evidence_id", "window_id"), "evidence"),
                (("admission_policy_id", "policy_id"), "policy"),
                (("admission_policy_version", "policy_version", "version"), "policy version"),
                (
                    ("risk_config_id", "active_risk_config_id", "config_id"),
                    "risk config",
                ),
                (
                    (
                        "risk_config_generation",
                        "active_risk_config_generation",
                        "risk_generation",
                        "generation",
                    ),
                    "risk generation",
                ),
                (
                    ("risk_config_hash", "active_risk_config_hash", "config_hash"),
                    "risk config hash",
                ),
            ):
                if _rolling_identity_conflict(member, *aliases):
                    raise ValueError(f"portfolio selection member {label} identity conflicts")
            research_trial_id = _rolling_identity_value(
                member,
                "research_trial_id",
                "trial_id",
            ) or None
            candidate_id = _rolling_identity_value(
                member,
                "candidate_id",
                "candidate",
                "strategy_candidate_id",
            ) or None
            evidence_window_id = _rolling_identity_value(
                member,
                "evidence_window_id",
                "evidence_id",
                "window_id",
            ) or None
            overlap_key = _rolling_optional_text(member, "overlap_key")
            status_upper = status.upper()
            funding_statuses = {"ACTIVE", "PAPER", "RETAINED", "REDUCE"}
            if allocation > 0 and status_upper not in funding_statuses:
                raise ValueError("positive allocation requires a funding status")
            funded_active = allocation > 0 and status_upper in funding_statuses
            if funded_active and research_trial_id is None:
                raise ValueError("funded portfolio member requires an exact research trial")
            if funded_active and candidate_id is None:
                raise ValueError("funded portfolio member requires a candidate binding")
            if funded_active and overlap_key:
                if overlap_key in seen_funded_overlaps:
                    raise ValueError("portfolio selection contains duplicate overlap_key")
                seen_funded_overlaps.add(overlap_key)
            normalized = dict(member)
            normalized.update(
                {
                    "portfolio_selection_id": identifier,
                    "strategy_version_id": strategy_version,
                    "research_trial_id": research_trial_id,
                    "candidate_id": candidate_id,
                    "allocation": format(allocation, "f"),
                    "status": status,
                    "score": score,
                    "reason": reason,
                    "evidence_window_id": evidence_window_id,
                    "overlap_key": overlap_key,
                    "created_at": committed_at,
                }
            )
            normalized_members.append(normalized)
        maintained_member_count = sum(
            1
            for member in normalized_members
            if _rolling_decimal(member["allocation"], name="member allocation", nonnegative=True) > 0
            and str(member["status"]).upper() in {"ACTIVE", "REDUCE"}
        )
        if declared_k is not None and declared_k != maintained_member_count:
            raise ValueError("portfolio selection k must equal maintained member count")
        k_value = maintained_member_count
        if sum(allocations, Decimal("0")) > _rolling_decimal(
            global_budget,
            name="global_budget",
            nonnegative=True,
        ):
            raise ValueError("portfolio member allocations exceed global budget")
        payload_data = dict(data)
        payload_data.update(
            {
                "portfolio_selection_id": identifier,
                "selection_id": identifier,
                "k": k_value,
                "risk_config_id": risk_config_id,
                "active_risk_config_id": risk_config_id,
                "risk_config_generation": risk_config_generation,
                "active_risk_config_generation": risk_config_generation,
                "risk_config_hash": risk_config_hash,
                "active_risk_config_hash": risk_config_hash,
                "global_budget": global_budget,
                "selected_at": selected_at,
                "review_due_at": review_due_at,
                "committed_at": committed_at,
            }
        )
        payload_json = _rolling_dump(payload_data)
        selection_values = (
            identifier,
            policy_id,
            policy_version,
            risk_config_id,
            risk_config_generation,
            risk_config_hash,
            global_budget,
            k_value,
            selected_at,
            review_due_at,
            payload_json,
            committed_at,
        )
        with self._write_context():
            policy_row = self._conn.execute(
                "SELECT payload_json FROM admission_policies "
                "WHERE policy_id=? AND version=?",
                (policy_id, policy_version),
            ).fetchone()
            if policy_row is None:
                raise ValueError("portfolio selection admission policy does not exist")
            policy_payload = _load(policy_row["payload_json"]) if policy_row["payload_json"] else {}
            policy_payload = policy_payload if isinstance(policy_payload, Mapping) else {}
            for aliases, label in (
                (("global_budget", "global_budget_usd", "budget"), "global budget"),
                (("max_members", "max_k", "k"), "max members"),
                (("requested_window_days", "windows_days"), "required windows"),
                (
                    ("min_actual_coverage_seconds", "minimum_evidence_seconds"),
                    "minimum coverage",
                ),
                (("minimum_coverage_ratio", "coverage_ratio"), "coverage ratio"),
                (("min_completed_outcomes", "minimum_completed_outcomes"), "minimum outcomes"),
                (("min_reliability", "minimum_reliability"), "minimum reliability"),
            ):
                if _rolling_identity_conflict(policy_payload, *aliases):
                    raise ValueError(f"portfolio selection policy {label} identity conflicts")

            def policy_value(*names: str, default: Any = None) -> Any:
                values = _rolling_identity_raw_values(policy_payload, *names)
                return values[0] if values else default

            policy_budget = _rolling_decimal(
                policy_value("global_budget", "global_budget_usd", "budget", default="0"),
                name="policy global_budget",
                nonnegative=True,
            )
            submitted_budget = _rolling_decimal(
                global_budget,
                name="portfolio selection global_budget",
                nonnegative=True,
            )
            funded_allocation_total = sum(allocations, Decimal("0"))
            if submitted_budget > policy_budget:
                raise ValueError("portfolio selection global_budget exceeds policy global_budget")
            if funded_allocation_total > policy_budget:
                raise ValueError("portfolio member allocations exceed policy global_budget")
            policy_max_members = _rolling_nonnegative_integer(
                policy_value("max_members", "max_k", "k", default=5),
                name="policy max_members",
            )
            if k_value > policy_max_members:
                raise ValueError("portfolio selection maintained members exceed policy max_members")
            policy_windows_raw = policy_value(
                "requested_window_days",
                "windows_days",
                default=(7, 30),
            )
            if isinstance(policy_windows_raw, int):
                policy_windows = (int(policy_windows_raw),)
            elif isinstance(policy_windows_raw, (list, tuple, set, frozenset)):
                policy_windows = tuple(int(item) for item in policy_windows_raw)
            else:
                raise ValueError("portfolio selection policy required windows are invalid")
            if not policy_windows or any(day not in {7, 30} for day in policy_windows):
                raise ValueError("portfolio selection policy required windows are invalid")
            policy_min_coverage = _rolling_decimal(
                policy_value(
                    "min_actual_coverage_seconds",
                    "minimum_evidence_seconds",
                    default="0",
                ),
                name="policy minimum coverage",
                nonnegative=True,
            )
            policy_min_ratio = _rolling_decimal(
                policy_value(
                    "minimum_coverage_ratio",
                    "coverage_ratio",
                    default="0.80",
                ),
                name="policy coverage ratio",
                nonnegative=True,
            )
            if policy_min_ratio > Decimal("1"):
                raise ValueError("portfolio selection policy coverage ratio is invalid")
            policy_min_outcomes = _rolling_nonnegative_integer(
                policy_value(
                    "min_completed_outcomes",
                    "minimum_completed_outcomes",
                    default=5,
                ),
                name="policy minimum outcomes",
            )
            policy_min_reliability = _rolling_decimal(
                policy_value(
                    "min_reliability",
                    "minimum_reliability",
                    default="0.50",
                ),
                name="policy minimum reliability",
                nonnegative=True,
            )
            if policy_min_reliability > Decimal("1"):
                raise ValueError("portfolio selection policy minimum reliability is invalid")
            for member in normalized_members:
                if self._conn.execute(
                    "SELECT 1 FROM strategy_versions WHERE strategy_version_id=?",
                    (member["strategy_version_id"],),
                ).fetchone() is None:
                    raise ValueError("portfolio selection strategy version does not exist")
                research_trial_id = member["research_trial_id"]
                trial = None
                trial_payload: Mapping[str, Any] = {}
                if research_trial_id is not None:
                    trial = self._conn.execute(
                        "SELECT strategy_version_id,payload_json FROM research_trials "
                        "WHERE research_trial_id=?",
                        (research_trial_id,),
                    ).fetchone()
                    if trial is None:
                        raise ValueError("portfolio selection research trial does not exist")
                    if str(trial["strategy_version_id"]) != member["strategy_version_id"]:
                        raise ValueError("portfolio member research trial strategy mismatch")
                    parsed_trial = _load(trial["payload_json"]) if trial["payload_json"] else {}
                    trial_payload = parsed_trial if isinstance(parsed_trial, Mapping) else {}
                funded_active = (
                    _rolling_decimal(member["allocation"], name="member allocation", nonnegative=True) > 0
                    and str(member["status"]).upper() in funding_statuses
                )
                trial_candidate = _rolling_identity_value(
                    trial_payload,
                    "candidate_id",
                    "candidate",
                    "strategy_candidate_id",
                )
                trial_identity = _rolling_identity_value(
                    trial_payload,
                    "research_trial_id",
                    "trial_id",
                )
                if _rolling_identity_conflict(
                    trial_payload,
                    "research_trial_id",
                    "trial_id",
                ) or _rolling_identity_conflict(
                    trial_payload,
                    "candidate_id",
                    "candidate",
                    "strategy_candidate_id",
                ):
                    raise ValueError("portfolio member research trial provenance conflicts")
                if (
                    trial_identity is not None
                    and trial_identity != str(research_trial_id).strip()
                ):
                    raise ValueError("portfolio member research trial identity conflicts")
                if funded_active and (
                    research_trial_id is None or member["candidate_id"] is None
                ):
                    raise ValueError("funded portfolio member lineage is incomplete")
                if funded_active and (
                    trial_candidate is None
                    or trial_candidate != str(member["candidate_id"]).strip()
                ):
                    raise ValueError("portfolio member candidate is not in research trial provenance")
                if funded_active and member["evidence_window_id"] is None:
                    raise ValueError("funded portfolio member requires an evidence window")
                if member["evidence_window_id"] is not None:
                    evidence = self._conn.execute(
                        "SELECT * FROM strategy_evidence_windows "
                        "WHERE evidence_window_id=?",
                        (member["evidence_window_id"],),
                    ).fetchone()
                    if evidence is None:
                        raise ValueError("portfolio selection evidence window does not exist")
                    if str(evidence["strategy_version_id"]) != member["strategy_version_id"]:
                        raise ValueError("portfolio member evidence window strategy mismatch")
                    evidence_payload = (
                        _load(evidence["payload_json"])
                        if evidence["payload_json"]
                        else {}
                    )
                    evidence_payload = (
                        evidence_payload if isinstance(evidence_payload, Mapping) else {}
                    )
                    evidence_identity = dict(evidence)
                    evidence_identity["payload"] = evidence_payload
                    if _rolling_identity_conflict(
                        evidence_identity,
                        "research_trial_id",
                        "trial_id",
                    ) or _rolling_identity_conflict(
                        evidence_identity,
                        "candidate_id",
                        "candidate",
                        "strategy_candidate_id",
                    ) or _rolling_identity_conflict(
                        evidence_identity,
                        "source_class",
                        "source_type",
                    ) or _rolling_identity_conflict(
                        evidence_identity,
                        "evidence_digest",
                        "digest",
                    ):
                        raise ValueError("portfolio member evidence provenance conflicts")
                    evidence_trial = _rolling_identity_value(
                        evidence_identity,
                        "research_trial_id",
                        "trial_id",
                    )
                    evidence_candidate = _rolling_identity_value(
                        evidence_identity,
                        "candidate_id",
                        "candidate",
                        "strategy_candidate_id",
                    )
                    source_class = str(evidence["source_class"] or "").strip().upper()
                    if source_class not in _ROLLING_EVIDENCE_SOURCE_CLASSES:
                        raise ValueError("portfolio member evidence source_class is invalid")
                    expected_digest = _rolling_evidence_digest(
                        _rolling_evidence_mapping_from_row(evidence, evidence_payload)
                    )
                    if str(evidence["evidence_digest"] or "").strip() != expected_digest:
                        raise ValueError("portfolio member evidence digest is invalid")
                    try:
                        completeness = _risk_decimal(
                            evidence["observation_completeness"],
                            name="observation_completeness",
                            nonnegative=True,
                        )
                        requested_window = int(evidence["requested_days"])
                        actual_coverage = int(evidence["actual_coverage_seconds"])
                        if requested_window not in {7, 30} or actual_coverage < 0:
                            raise ValueError
                        expected_completeness = min(
                            Decimal("1"),
                            Decimal(actual_coverage)
                            / Decimal(requested_window * 86400),
                        )
                    except (TypeError, ValueError, InvalidOperation, ZeroDivisionError) as exc:
                        raise ValueError("portfolio member evidence coverage is invalid") from exc
                    if completeness > Decimal("1") or completeness != expected_completeness:
                        raise ValueError("portfolio member evidence completeness is inconsistent")
                    if requested_window not in policy_windows:
                        raise ValueError("portfolio member evidence window is not required by policy")
                    if funded_active:
                        required_coverage = max(
                            policy_min_coverage,
                            Decimal(requested_window * 86400) * policy_min_ratio,
                        )
                        if (
                            actual_coverage < required_coverage
                            or completeness < policy_min_ratio
                            or int(evidence["completed_outcomes"]) < policy_min_outcomes
                            or _risk_decimal(
                                evidence["reliability"],
                                name="reliability",
                                nonnegative=True,
                            ) < policy_min_reliability
                        ):
                            raise ValueError("portfolio member evidence is below policy minimums")
                        if (
                            evidence_trial != str(research_trial_id).strip()
                            or evidence_candidate != str(member["candidate_id"]).strip()
                        ):
                            raise ValueError("portfolio member evidence window lineage mismatch")
            existing = self._conn.execute(
                "SELECT * FROM portfolio_selections WHERE portfolio_selection_id=?",
                (identifier,),
            ).fetchone()
            if existing is not None:
                immutable_columns = (
                    "policy_id",
                    "policy_version",
                    "risk_config_id",
                    "risk_config_generation",
                    "risk_config_hash",
                    "global_budget",
                    "k",
                    "selected_at",
                    "review_due_at",
                )
                expected = selection_values[1:10]
                if tuple(existing[column] for column in immutable_columns) != expected or not _rolling_payload_equal(
                    _load(existing["payload_json"]),
                    payload_data,
                    ignored=frozenset({"committed_at"}),
                ):
                    raise ValueError("portfolio selection identity conflict")
                existing_members = self._conn.execute(
                    "SELECT * FROM portfolio_selection_members "
                    "WHERE portfolio_selection_id=? ORDER BY strategy_version_id",
                    (identifier,),
                ).fetchall()
                if len(existing_members) != len(normalized_members):
                    raise ValueError("portfolio selection members conflict")
                for stored, proposed in zip(
                    existing_members,
                    sorted(normalized_members, key=lambda item: item["strategy_version_id"]),
                ):
                    member_columns = (
                        "strategy_version_id",
                        "research_trial_id",
                        "candidate_id",
                        "allocation",
                        "status",
                        "score",
                        "reason",
                        "evidence_window_id",
                        "overlap_key",
                    )
                    if tuple(stored[column] for column in member_columns) != tuple(
                        proposed[column] for column in member_columns
                    ) or not _rolling_payload_equal(
                        _load(stored["payload_json"]),
                        proposed,
                        ignored=frozenset({"created_at"}),
                    ):
                        raise ValueError("portfolio selection member identity conflict")
                return self._rolling_selection_record(existing)
            self._conn.execute(
                "INSERT INTO portfolio_selections("
                "portfolio_selection_id,policy_id,policy_version,risk_config_id,"
                "risk_config_generation,risk_config_hash,global_budget,k,selected_at,review_due_at,"
                "payload_json,committed_at"
                ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                selection_values,
            )
            for member in normalized_members:
                self._conn.execute(
                    "INSERT INTO portfolio_selection_members("
                    "portfolio_selection_id,strategy_version_id,research_trial_id,candidate_id,"
                    "allocation,status,score,reason,evidence_window_id,overlap_key,payload_json,created_at"
                    ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        identifier,
                        member["strategy_version_id"],
                        member["research_trial_id"],
                        member["candidate_id"],
                        member["allocation"],
                        member["status"],
                        member["score"],
                        member["reason"],
                        member["evidence_window_id"],
                        member["overlap_key"],
                        _rolling_dump(member),
                        committed_at,
                    ),
                )
            self._conn.execute(
                "INSERT INTO portfolio_current_selection(pointer_id,portfolio_selection_id,committed_at) "
                "VALUES ('current',?,?) "
                "ON CONFLICT(pointer_id) DO UPDATE SET "
                "portfolio_selection_id=excluded.portfolio_selection_id,committed_at=excluded.committed_at",
                (identifier, committed_at),
            )
            row = self._conn.execute(
                "SELECT * FROM portfolio_selections WHERE portfolio_selection_id=?",
                (identifier,),
            ).fetchone()
            return self._rolling_selection_record(row)

    def load_current_portfolio_selection(self) -> dict[str, Any] | None:
        with self._lock:
            pointer = self._conn.execute(
                "SELECT portfolio_selection_id FROM portfolio_current_selection "
                "WHERE pointer_id='current'",
            ).fetchone()
            if pointer is not None:
                row = self._conn.execute(
                    "SELECT * FROM portfolio_selections WHERE portfolio_selection_id=?",
                    (pointer["portfolio_selection_id"],),
                ).fetchone()
            else:
                row = self._conn.execute(
                    "SELECT * FROM portfolio_selections "
                    "ORDER BY committed_at DESC,rowid DESC LIMIT 1",
                ).fetchone()
        return self._rolling_selection_record(row) if row is not None else None

    def list_portfolio_selections(self, *, limit: int | None = 100) -> list[dict[str, Any]]:
        limit_value = _rolling_limit(limit, default=100)
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM portfolio_selections "
                "ORDER BY committed_at DESC,rowid DESC LIMIT ?",
                (limit_value,),
            ).fetchall()
            return [self._rolling_selection_record(row) for row in rows]

    def save_portfolio_review_state(self, state: Any) -> dict[str, Any]:
        data = _rolling_mapping(state, name="portfolio_review_state")
        selection_id_raw = data.get("portfolio_selection_id", data.get("selection_id"))
        selection_id = str(selection_id_raw).strip() if selection_id_raw is not None else None
        review_due_at = _rolling_timestamp(
            data.get("review_due_at"),
            name="review_due_at",
        )
        reviewed_at = _rolling_timestamp(
            data.get("reviewed_at"),
            name="reviewed_at",
        )
        status = _rolling_optional_text(data, "status")
        updated_at = _rolling_timestamp(
            data.get("updated_at"),
            name="updated_at",
            default_now=True,
        )
        payload_data = dict(data)
        payload_data.update(
            {
                "state_id": "current",
                "portfolio_selection_id": selection_id,
                "selection_id": selection_id,
                "review_due_at": review_due_at,
                "reviewed_at": reviewed_at,
                "status": status,
                "updated_at": updated_at,
            }
        )
        with self._write_context():
            if selection_id is not None and self._conn.execute(
                "SELECT 1 FROM portfolio_selections WHERE portfolio_selection_id=?",
                (selection_id,),
            ).fetchone() is None:
                raise ValueError("portfolio review selection does not exist")
            self._conn.execute(
                "INSERT INTO portfolio_review_state("
                "state_id,portfolio_selection_id,review_due_at,reviewed_at,status,payload_json,updated_at"
                ") VALUES ('current',?,?,?,?,?,?) "
                "ON CONFLICT(state_id) DO UPDATE SET "
                "portfolio_selection_id=excluded.portfolio_selection_id,"
                "review_due_at=excluded.review_due_at,reviewed_at=excluded.reviewed_at,"
                "status=excluded.status,payload_json=excluded.payload_json,updated_at=excluded.updated_at",
                (
                    selection_id,
                    review_due_at,
                    reviewed_at,
                    status,
                    _rolling_dump(payload_data),
                    updated_at,
                ),
            )
            row = self._conn.execute(
                "SELECT * FROM portfolio_review_state WHERE state_id='current'",
            ).fetchone()
        payload = _load(row["payload_json"])
        result = dict(payload) if isinstance(payload, Mapping) else {}
        result.update(
            {
                "state_id": "current",
                "portfolio_selection_id": row["portfolio_selection_id"],
                "selection_id": row["portfolio_selection_id"],
                "review_due_at": row["review_due_at"],
                "reviewed_at": row["reviewed_at"],
                "status": row["status"],
                "updated_at": row["updated_at"],
            }
        )
        return result

    def load_portfolio_review_state(self) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM portfolio_review_state WHERE state_id='current'",
            ).fetchone()
        if row is None:
            return None
        payload = _load(row["payload_json"])
        result = dict(payload) if isinstance(payload, Mapping) else {}
        result.update(
            {
                "state_id": "current",
                "portfolio_selection_id": row["portfolio_selection_id"],
                "selection_id": row["portfolio_selection_id"],
                "review_due_at": row["review_due_at"],
                "reviewed_at": row["reviewed_at"],
                "status": row["status"],
                "updated_at": row["updated_at"],
            }
        )
        return result

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
                "payload": json.loads(row["payload_json"]),
                "created_at": _parse_datetime(row["created_at"]),
            }
            for row in rows
        ]
    def list_collection_cycles_dashboard(
        self,
        *,
        collector_name: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """List cycle rows with diagnostics bounded at the dashboard boundary."""
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 0:
            raise ValueError("limit must be a non-negative integer")
        query = (
            "SELECT cycle_id,collector_name,started_at,ended_at,payload_json,created_at "
            "FROM collection_cycles"
        )
        values: list[Any] = []
        if collector_name is not None:
            query += " WHERE collector_name=?"
            values.append(str(collector_name))
        query += " ORDER BY started_at DESC,cycle_id DESC LIMIT ?"
        values.append(int(limit))
        with self._lock:
            rows = self._conn.execute(query, values).fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            payload, projection = _dashboard_payload_projection(row["payload_json"])
            item = {
                "cycle_id": row["cycle_id"],
                "collector_name": row["collector_name"],
                "started_at": _parse_datetime(row["started_at"]),
                "ended_at": _parse_datetime(row["ended_at"]),
                "payload": payload,
                "created_at": _parse_datetime(row["created_at"]),
            }
            item.update(_dashboard_payload_fields(projection))
            result.append(item)
        return result

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
    def list_research_items_dashboard(
        self,
        *,
        status: str | None = None,
        limit: int = 50,
        _order_by: str = "priority DESC,created_at ASC,item_id ASC",
    ) -> list[dict[str, Any]]:
        """Project queue rows without materializing their JSON documents."""
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 0:
            raise ValueError("limit must be a non-negative integer")
        if _order_by not in {
            "priority DESC,created_at ASC,item_id ASC",
            "updated_at DESC,created_at DESC,item_id DESC",
        }:
            raise ValueError("unsupported research queue dashboard ordering")
        clauses = ""
        values: list[Any] = []
        if status is not None:
            clauses = " WHERE status=?"
            values.append(str(status).upper())
        payload_fields = (
            ("candidate_id", "$.candidate_id"),
            ("dataset_id", "$.dataset_id"),
            ("dataset_version", "$.dataset_version"),
            ("market_id", "$.market_id"),
            ("source_type", "$.source_type"),
            ("experiment_family", "$.experiment_family"),
            ("strategy_id", "$.strategy_id"),
            ("hypothesis_id", "$.hypothesis_id"),
            ("plan_id", "$.plan_id"),
            ("plan_hash", "$.plan_hash"),
            ("forward_test_id", "$.forward_test_id"),
            ("paper_observation_intent_id", "$.paper_observation_intent_id"),
            ("stage", "$.stage"),
            ("blocker", "$.blocker"),
            ("reason_code", "$.reason_code"),
            ("market_scope_hash", "$.market_scope_hash"),
            ("market_scope_version", "$.market_scope_version"),
            ("dataset_selector", "$.dataset_selector"),
            ("experiment_plan", "$.experiment_plan"),
            ("provenance", "$.provenance"),
            ("market_scope", "$.market_scope"),
        )
        result_fields = (
            ("accepted", "$.accepted"),
            ("candidate_id", "$.candidate_id"),
            ("dataset_id", "$.dataset_id"),
            ("dataset_version", "$.dataset_version"),
            ("source_type", "$.source_type"),
            ("experiment_family", "$.experiment_family"),
            ("strategy_id", "$.strategy_id"),
            ("hypothesis_id", "$.hypothesis_id"),
            ("plan_id", "$.plan_id"),
            ("plan_hash", "$.plan_hash"),
            ("forward_test_id", "$.forward_test_id"),
            ("paper_observation_intent_id", "$.paper_observation_intent_id"),
            ("stage", "$.stage"),
            ("blocker", "$.blocker"),
            ("reason_code", "$.reason_code"),
            ("reason", "$.reason"),
            ("validation_expectancy", "$.validation_expectancy"),
            ("validation_sample_count", "$.validation_sample_count"),
            ("validation_trade_count", "$.validation_trade_count"),
            ("raw_observations", "$.raw_observations"),
            ("sample_count", "$.sample_count"),
            ("trade_count", "$.trade_count"),
            ("min_samples", "$.min_samples"),
            ("min_trades", "$.min_trades"),
            ("required_samples", "$.required_samples"),
            ("required_trades", "$.required_trades"),
            ("dataset_selector", "$.dataset_selector"),
            ("experiment_plan", "$.experiment_plan"),
            ("minimum_sample_check", "$.minimum_sample_check"),
            ("validation", "$.validation"),
            ("forward_evidence", "$.forward_evidence"),
            ("historical_evidence", "$.historical_evidence"),
        )
        payload_paths = ",".join(repr(path) for _, path in payload_fields)
        result_paths = ",".join(repr(path) for _, path in result_fields)
        query = (
            "SELECT item_id,item_type,dedupe_key,status,priority,source,author,"
            "schema_version,created_at,updated_at,available_at,lease_until,lease_owner,"
            "attempts,last_error,"
            "json_extract(CASE WHEN json_valid(payload_json) THEN payload_json ELSE '{}' END,"
            f"{payload_paths}) AS payload_fields_json,"
            "json_extract(CASE WHEN json_valid(result_json) THEN result_json ELSE '{}' END,"
            f"{result_paths}) AS result_fields_json,"
            "COALESCE((SELECT json_group_array(value) FROM json_each("
            "CASE WHEN json_valid(result_json) THEN result_json ELSE '{}' END,"
            "'$.candidate_results') WHERE key<50),'[]') AS candidate_results_json "
            "FROM research_queue"
            f"{clauses} ORDER BY {_order_by} LIMIT ?"
        )
        values.append(int(limit))
        with self._lock:
            rows = self._conn.execute(query, values).fetchall()

        def decode(value: Any) -> Any:
            try:
                return json.loads(value) if value is not None else None
            except (TypeError, ValueError, json.JSONDecodeError):
                return None

        def mapping(values_json: Any, fields: Sequence[tuple[str, str]]) -> dict[str, Any]:
            values = decode(values_json)
            if not isinstance(values, list):
                return {}
            return {
                key: value
                for (key, _), value in zip(fields, values)
                if value is not None
            }

        result: list[dict[str, Any]] = []
        for row in rows:
            payload_values = mapping(row["payload_fields_json"], payload_fields)
            result_values = mapping(row["result_fields_json"], result_fields)
            for target, source in (
                (payload_values, "dataset_selector"),
                (payload_values, "experiment_plan"),
                (payload_values, "provenance"),
                (payload_values, "market_scope"),
            ):
                decoded = decode(target.get(source))
                if decoded is not None:
                    target[source] = decoded
            for target, source in (
                (result_values, "dataset_selector"),
                (result_values, "experiment_plan"),
                (result_values, "minimum_sample_check"),
                (result_values, "validation"),
                (result_values, "forward_evidence"),
                (result_values, "historical_evidence"),
            ):
                decoded = decode(target.get(source))
                if decoded is not None:
                    target[source] = decoded
            candidates = decode(row["candidate_results_json"])
            if isinstance(candidates, list):
                result_values["candidate_results"] = candidates
            result.append(
                {
                    "item_id": row["item_id"],
                    "item_type": row["item_type"],
                    "dedupe_key": row["dedupe_key"],
                    "status": row["status"],
                    "priority": int(row["priority"] or 0),
                    "payload": payload_values,
                    "result": result_values,
                    "source": row["source"],
                    "author": row["author"],
                    "schema_version": row["schema_version"],
                    "created_at": _parse_datetime(row["created_at"]),
                    "updated_at": _parse_datetime(row["updated_at"]),
                    "available_at": _parse_datetime(row["available_at"]),
                    "lease_until": _parse_datetime(row["lease_until"]),
                    "lease_owner": row["lease_owner"],
                    "attempts": int(row["attempts"] or 0),
                    "last_error": row["last_error"],
                }
            )
        return result
    def get_latest_research_item_dashboard(self) -> dict[str, Any] | None:
        """Project the single most recently updated queue item for overview reads."""
        rows = self.list_research_items_dashboard(
            limit=1,
            _order_by="updated_at DESC,created_at DESC,item_id DESC",
        )
        return rows[0] if rows else None


    def research_queue_stats(self) -> dict[str, int]:
        with self._lock:
            rows = self._conn.execute("SELECT status,COUNT(*) AS n FROM research_queue GROUP BY status").fetchall()
        result = {str(row["status"]): int(row["n"]) for row in rows}
        result["total"] = sum(result.values())
        return result
    def research_feed_status(
        self,
        *,
        now: datetime | None = None,
        hermes_job_id: str | None = None,
    ) -> dict[str, Any]:
        """Return one bounded, read-only research-feed status projection.

        The Hermes job is external to this process, so local scheduler state is
        intentionally projected only as ``internal_queue`` evidence.  Every
        aggregate below is computed in SQLite; this method never hydrates queue
        or lifecycle rows and never performs qualification, network, or write
        operations.
        """
        current = ensure_utc(now or utc_now())
        current_iso = current.isoformat()
        window_start = (current - timedelta(days=1)).isoformat()
        accepted_sql = (
            "(status='ACCEPTED' OR "
            "(status='COMPLETED' AND json_valid(COALESCE(result_json,''))=1 "
            "AND json_extract(result_json,'$.accepted')=1))"
        )

        with self._lock:
            proposal_row = self._conn.execute(
                "SELECT "
                "MAX(CASE WHEN created_at<=? THEN created_at END) AS latest_submitted_at,"
                "MAX(CASE WHEN " + accepted_sql + " AND updated_at<=? THEN updated_at END) AS latest_accepted_at,"
                "COALESCE(SUM(CASE WHEN created_at>=? AND created_at<=? THEN 1 ELSE 0 END),0) AS submitted_24h,"
                "COALESCE(SUM(CASE WHEN " + accepted_sql + " AND updated_at>=? AND updated_at<=? THEN 1 ELSE 0 END),0) AS accepted_24h,"
                "COALESCE(SUM(CASE WHEN status='REJECTED' AND updated_at>=? AND updated_at<=? THEN 1 ELSE 0 END),0) AS rejected_24h,"
                "COALESCE(SUM(CASE WHEN status='FAILED' AND updated_at>=? AND updated_at<=? THEN 1 ELSE 0 END),0) AS failed_24h,"
                "COALESCE(SUM(CASE WHEN status='COMPLETED' THEN 1 ELSE 0 END),0) AS completed,"
                "COALESCE(SUM(CASE WHEN status='PENDING' THEN 1 ELSE 0 END),0) AS pending,"
                "COALESCE(SUM(CASE WHEN status='TESTING' AND lease_until IS NOT NULL AND lease_until>? THEN 1 ELSE 0 END),0) AS processing,"
                "COALESCE(SUM(CASE WHEN status='REJECTED' THEN 1 ELSE 0 END),0) AS rejected,"
                "COALESCE(SUM(CASE WHEN updated_at>=? AND updated_at<=? "
                "AND status IN ('ACCEPTED','COMPLETED','REJECTED','FAILED') THEN 1 ELSE 0 END),0) AS terminal_24h "
                "FROM research_queue WHERE lower(item_type)='hypothesis'",
                (
                    current_iso,
                    current_iso,
                    window_start,
                    current_iso,
                    window_start,
                    current_iso,
                    window_start,
                    current_iso,
                    window_start,
                    current_iso,
                    current_iso,
                    window_start,
                    current_iso,
                ),
            ).fetchone()
            candidate_row = self._conn.execute(
                "SELECT COUNT(*) AS total,"
                "COALESCE(SUM(CASE WHEN stage='IDEA' THEN 1 ELSE 0 END),0) AS new,"
                "COALESCE(SUM(CASE WHEN stage='REJECTED' THEN 1 ELSE 0 END),0) AS rejected "
                "FROM candidate_lifecycle"
            ).fetchone()
            candidate_event_row = self._conn.execute(
                "SELECT "
                "MAX(CASE WHEN from_stage IS NULL AND to_stage='IDEA' AND created_at<=? THEN created_at END) AS latest_created_at,"
                "COALESCE(SUM(CASE WHEN from_stage IS NULL AND to_stage='IDEA' THEN 1 ELSE 0 END),0) AS idea_events,"
                "COALESCE(SUM(CASE WHEN from_stage IS NULL AND to_stage='IDEA' AND created_at>=? AND created_at<=? THEN 1 ELSE 0 END),0) AS created_24h,"
                "COALESCE(SUM(CASE WHEN from_stage IS NULL AND to_stage='IDEA' AND created_at>=? AND created_at<=? "
                "AND json_valid(COALESCE(payload_json,''))=1 "
                "AND (NULLIF(TRIM(COALESCE(json_extract(payload_json,'$.parent_id'),'')),'') IS NOT NULL "
                "OR NULLIF(TRIM(COALESCE(json_extract(payload_json,'$.lineage[0]'),'')),'') IS NOT NULL) "
                "THEN 1 ELSE 0 END),0) AS mutations_24h "
                "FROM candidate_lifecycle_events",
                (current_iso, window_start, current_iso, window_start, current_iso),
            ).fetchone()
            # AxiomStore creates lifecycle events for normal writes.  This
            # fallback keeps the aggregate useful for older/imported rows that
            # predate that event table's creation.
            candidate_fallback_row = None
            if candidate_event_row is None or int(candidate_event_row["idea_events"] or 0) == 0:
                candidate_fallback_row = self._conn.execute(
                    "SELECT "
                    "MAX(CASE WHEN updated_at<=? THEN updated_at END) AS latest_created_at,"
                    "COALESCE(SUM(CASE WHEN updated_at>=? AND updated_at<=? THEN 1 ELSE 0 END),0) AS created_24h,"
                    "COALESCE(SUM(CASE WHEN updated_at>=? AND updated_at<=? "
                    "AND json_valid(COALESCE(payload_json,''))=1 "
                    "AND (NULLIF(TRIM(COALESCE(json_extract(payload_json,'$.parent_id'),'')),'') IS NOT NULL "
                    "OR NULLIF(TRIM(COALESCE(json_extract(payload_json,'$.lineage[0]'),'')),'') IS NOT NULL) "
                    "THEN 1 ELSE 0 END),0) AS mutations_24h "
                    "FROM candidate_lifecycle",
                    (current_iso, window_start, current_iso, window_start, current_iso),
                ).fetchone()
            scheduler_row = self._conn.execute(
                "SELECT state_json FROM scheduler_state WHERE scheduler_name='hermes-control'"
            ).fetchone()
            worker_row = self._conn.execute(
                "SELECT status,payload_json,updated_at FROM worker_state "
                "WHERE worker_name='research-queue' LIMIT 1"
            ).fetchone()
            budget_row = self._conn.execute(
                "SELECT payload_json FROM experiment_budget WHERE budget_id='autonomous' LIMIT 1"
            ).fetchone()
            eligibility_table = self._conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='canary_eligibility' LIMIT 1"
            ).fetchone()
            eligible_count = 0
            if eligibility_table is not None:
                eligible_count = int(
                    self._conn.execute(
                        "SELECT COUNT(*) AS n "
                        "FROM canary_eligibility AS e "
                        "JOIN candidate_lifecycle AS c ON c.candidate_id=e.candidate_id "
                        "WHERE c.stage IN ('FROZEN','PAPER_FORWARD','PAPER_PROMOTABLE') "
                        "AND LENGTH(TRIM(COALESCE(e.frozen_hash,'')))>0 "
                        "AND json_valid(COALESCE(c.payload_json,''))=1 "
                        "AND LENGTH(TRIM(COALESCE(json_extract(c.payload_json,'$.frozen_hash'),'')))>0 "
                        "AND json_type(c.payload_json,'$.frozen_hash')='text' "
                        "AND json_extract(c.payload_json,'$.frozen_hash')=e.frozen_hash "
                        "AND LENGTH(TRIM(COALESCE(json_extract(c.payload_json,'$.qualification_hash'),'')))>0 "
                        "AND json_type(c.payload_json,'$.qualification_hash')='text' "
                        "AND json_valid(COALESCE(e.evidence_json,''))=1 "
                        "AND LENGTH(TRIM(COALESCE(json_extract(e.evidence_json,'$.qualification_hash'),'')))>0 "
                        "AND json_type(e.evidence_json,'$.qualification_hash')='text' "
                        "AND json_extract(c.payload_json,'$.qualification_hash')="
                        "json_extract(e.evidence_json,'$.qualification_hash')"
                    ).fetchone()["n"]
                )

        def _count(row: sqlite3.Row | None, name: str) -> int:
            if row is None:
                return 0
            try:
                value = row[name]
                return max(0, int(value or 0))
            except (KeyError, TypeError, ValueError, OverflowError):
                return 0

        def _timestamp(value: Any) -> str | None:
            if value is None:
                return None
            if isinstance(value, datetime):
                return ensure_utc(value).isoformat()
            text = str(value).strip()
            return text or None

        proposal_submitted = _count(proposal_row, "submitted_24h")
        proposal_accepted = _count(proposal_row, "accepted_24h")
        proposal_rejected = _count(proposal_row, "rejected_24h")
        proposal_failed = _count(proposal_row, "failed_24h")
        proposal_pending = _count(proposal_row, "pending")
        proposal_processing = _count(proposal_row, "processing")
        candidate_created = _count(candidate_event_row, "created_24h")
        candidate_mutations = _count(candidate_event_row, "mutations_24h")
        if _count(candidate_event_row, "idea_events") == 0 and candidate_fallback_row is not None:
            candidate_created = _count(candidate_fallback_row, "created_24h")
            candidate_mutations = _count(candidate_fallback_row, "mutations_24h")
        latest_created = (
            _timestamp(candidate_event_row["latest_created_at"])
            if candidate_event_row is not None and _count(candidate_event_row, "idea_events") > 0
            else _timestamp(candidate_fallback_row["latest_created_at"])
            if candidate_fallback_row is not None
            else None
        )

        scheduler_state = (
            json.loads(scheduler_row["state_json"])
            if scheduler_row is not None and scheduler_row["state_json"]
            else {}
        )
        scheduler_state = scheduler_state if isinstance(scheduler_state, Mapping) else {}

        def _safe_job_id(value: Any) -> str | None:
            text = str(value or "").strip()
            if (
                not text
                or len(text) > 256
                or not (text[0].isascii() and text[0].isalnum())
                or any(not (char.isascii() and (char.isalnum() or char in "_.:-")) for char in text)
            ):
                return None
            return text

        if hermes_job_id is not None:
            job_id = _safe_job_id(hermes_job_id) or _DEFAULT_HERMES_JOB_ID
        else:
            job_id = _safe_job_id(scheduler_state.get("job_id")) or _DEFAULT_HERMES_JOB_ID

        queue_status = str(scheduler_state.get("status") or "ACTIVE").strip().upper()
        if queue_status not in {"ACTIVE", "PAUSED"}:
            queue_status = "ACTIVE"
        trigger = scheduler_state.get("trigger", scheduler_state.get("schedule", "after_each_collection"))
        trigger = str(trigger).strip() if trigger is not None else "after_each_collection"
        if not trigger:
            trigger = "after_each_collection"
        worker_payload = (
            json.loads(worker_row["payload_json"])
            if worker_row is not None and worker_row["payload_json"]
            else {}
        )
        worker_payload = worker_payload if isinstance(worker_payload, Mapping) else {}
        last_cycle_at = (
            worker_payload.get("last_cycle_at")
            or worker_payload.get("cycle_at")
            or scheduler_state.get("last_cycle_at")
            or (worker_row["updated_at"] if worker_row is not None else None)
            or scheduler_state.get("last_run_at")
        )

        budget_payload = (
            json.loads(budget_row["payload_json"])
            if budget_row is not None and budget_row["payload_json"]
            else {}
        )
        budget_payload = budget_payload if isinstance(budget_payload, Mapping) else {}

        def _nonnegative_int(value: Any, default: int = 0) -> int:
            if isinstance(value, bool):
                return default
            try:
                parsed = int(value)
            except (TypeError, ValueError, OverflowError):
                return default
            return parsed if parsed >= 0 else default

        total_limit = _nonnegative_int(budget_payload.get("total_limit"))
        total_used = min(total_limit, _nonnegative_int(budget_payload.get("used_total")))
        total_remaining = max(0, total_limit - total_used)
        raw_total_limit = budget_payload.get("total_limit")
        raw_total_used = budget_payload.get("used_total")
        budget_exhausted = (
            budget_row is not None
            and isinstance(raw_total_limit, int)
            and not isinstance(raw_total_limit, bool)
            and isinstance(raw_total_used, int)
            and not isinstance(raw_total_used, bool)
            and raw_total_limit >= 0
            and raw_total_used >= 0
            and raw_total_used >= raw_total_limit
        )

        per_family_limit = _nonnegative_int(budget_payload.get("per_family_limit"))
        raw_families = budget_payload.get("used_by_family", {})
        raw_families = raw_families if isinstance(raw_families, Mapping) else {}
        families: dict[str, dict[str, int]] = {}
        for family, used in raw_families.items():
            used_value = min(per_family_limit, _nonnegative_int(used))
            family_name = str(family).strip()
            if family_name:
                families[family_name] = {
                    "limit": per_family_limit,
                    "used": used_value,
                    "remaining": max(0, per_family_limit - used_value),
                }

        if proposal_submitted == 0:
            no_new_reason = "NO_NEW_HERMES_PROPOSALS"
        elif budget_exhausted:
            no_new_reason = "RESEARCH_BUDGET_EXHAUSTED"
        elif proposal_pending + proposal_processing > 0:
            no_new_reason = "INTERNAL_QUEUE_HAS_WORK"
        elif (
            _count(proposal_row, "terminal_24h") > 0
            and candidate_created == 0
        ):
            no_new_reason = "QUEUE_CONSUMED_WITHOUT_NEW_CANDIDATES"
        elif candidate_created > 0:
            no_new_reason = "CANDIDATE_FLOW_ACTIVE"
        else:
            no_new_reason = "NO_NEW_CANDIDATES_OBSERVED"

        return {
            "external_hermes": {
                "job_id": job_id,
                "status": "UNKNOWN",
                "evidence": "No local verifier is available for the external Hermes job; scheduler state is internal-only.",
            },
            "internal_queue": {
                "status": queue_status,
                "trigger": trigger,
                "last_cycle_at": _timestamp(last_cycle_at),
            },
            "proposals": {
                "latest_submitted_at": _timestamp(proposal_row["latest_submitted_at"]) if proposal_row is not None else None,
                "latest_accepted_at": _timestamp(proposal_row["latest_accepted_at"]) if proposal_row is not None else None,
                "submitted_24h": proposal_submitted,
                "accepted_24h": proposal_accepted,
                "rejected_24h": proposal_rejected,
                "failed_24h": proposal_failed,
                "pending": proposal_pending,

                "processing": proposal_processing,
                "completed": _count(proposal_row, "completed"),
                "rejected": _count(proposal_row, "rejected"),
            },
            "candidates": {
                "latest_created_at": latest_created,
                "created_24h": candidate_created,
                "mutations_24h": candidate_mutations,
                "total": _count(candidate_row, "total"),
                "new": _count(candidate_row, "new"),
                "eligible": eligible_count,
                "rejected": _count(candidate_row, "rejected"),
            },
            "budgets": {
                "total_limit": total_limit,
                "total_used": total_used,
                "total_remaining": total_remaining,
                "families": families,
            },
            "no_new_candidates_reason": no_new_reason,
        }
    # Current-market scope resolutions ---------------------------------
    def save_market_scope_resolution(
        self,
        result: Any,
        *,
        if_absent: bool = True,
    ) -> str:
        """Persist one immutable current-market scope resolution.

        ``resolution_id`` is derived by the typed result from candidate id,
        frozen scope hash/version and resolution timestamp.  Repeating the
        exact write is idempotent; a write with the same identity but a
        different payload is rejected rather than silently replacing authority.
        """
        from .market_scope import MarketScopeResolution

        resolution = (
            result
            if isinstance(result, MarketScopeResolution)
            else MarketScopeResolution.from_mapping(result)
        )
        payload = resolution.as_dict()
        values = (
            resolution.resolution_id,
            resolution.candidate_id,
            resolution.scope_hash,
            resolution.scope_version,
            resolution.resolved_at.isoformat(),
            resolution.status,
            resolution.reason,
            _dump(payload["policy"]),
            _dump(payload["matched_markets"]),
            _dump(payload["excluded_markets"]),
            _dump(payload["deferred_markets"]),
            _dump(payload["provenance"]),
            _now_iso(),
        )
        with self._write_context():
            existing = self._conn.execute(
                "SELECT * FROM market_scope_resolutions WHERE resolution_id=?",
                (resolution.resolution_id,),
            ).fetchone()
            if existing is not None:
                existing_values = (
                    str(existing["candidate_id"]),
                    str(existing["scope_hash"]),
                    str(existing["scope_version"]),
                    str(existing["resolved_at"]),
                    str(existing["status"]),
                    str(existing["reason"]),
                    str(existing["policy_json"]),
                    str(existing["matched_markets_json"]),
                    str(existing["excluded_markets_json"]),
                    str(existing["deferred_markets_json"]),
                    str(existing["provenance_json"]),
                )
                current_values = values[1:12]
                if existing_values != current_values:
                    raise ValueError(f"market scope resolution identity conflicts with stored payload: {resolution.resolution_id}")
                if not if_absent:
                    raise ValueError(f"market scope resolution already exists: {resolution.resolution_id}")
                return resolution.resolution_id
            try:
                self._conn.execute(
                    "INSERT INTO market_scope_resolutions("
                    "resolution_id,candidate_id,scope_hash,scope_version,resolved_at,status,reason,"
                    "policy_json,matched_markets_json,excluded_markets_json,deferred_markets_json,"
                    "provenance_json,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    values,
                )
            except sqlite3.IntegrityError as exc:
                # The unique natural key protects against two deterministic
                # writers racing with distinct resolution ids.
                duplicate = self._conn.execute(
                    "SELECT resolution_id FROM market_scope_resolutions "
                    "WHERE candidate_id=? AND scope_hash=? AND scope_version=? AND resolved_at=?",
                    values[1:5],
                ).fetchone()
                if duplicate is not None:
                    if str(duplicate["resolution_id"]) == resolution.resolution_id and if_absent:
                        return resolution.resolution_id
                    raise ValueError("market scope resolution identity already exists") from exc
                raise
        return resolution.resolution_id

    def load_market_scope_resolution(
        self,
        candidate_id: str,
        *,
        scope_hash: str | None = None,
        scope_version: str | None = None,
        resolved_at: datetime | None = None,
    ) -> Any | None:
        """Load the newest immutable resolution matching one candidate binding."""
        from .market_scope import MarketScopeResolution

        identifier = str(candidate_id).strip()
        if not identifier:
            raise ValueError("candidate_id is required")
        clauses = ["candidate_id=?"]
        values: list[Any] = [identifier]
        if scope_hash is not None:
            clauses.append("scope_hash=?")
            values.append(str(scope_hash).strip())
        if scope_version is not None:
            clauses.append("scope_version=?")
            values.append(str(scope_version).strip())
        if resolved_at is not None:
            stamp = _parse_datetime(resolved_at)
            if stamp is None:
                raise ValueError("resolved_at must be a datetime or timestamp")
            clauses.append("resolved_at=?")
            values.append(stamp.isoformat())
        query = (
            "SELECT * FROM market_scope_resolutions WHERE "
            + " AND ".join(clauses)
            + " ORDER BY resolved_at DESC,resolution_id DESC LIMIT 1"
        )
        with self._lock:
            row = self._conn.execute(query, values).fetchone()
        return self._market_scope_resolution_from_row(row) if row is not None else None

    def latest_market_scope_resolution(
        self,
        candidate_id: str,
        *,
        scope_hash: str | None = None,
        scope_version: str | None = None,
    ) -> Any | None:
        """Alias for the bounded newest-resolution read."""
        return self.load_market_scope_resolution(
            candidate_id,
            scope_hash=scope_hash,
            scope_version=scope_version,
        )

    def list_market_scope_resolutions(
        self,
        *,
        candidate_id: str | None = None,
        status: str | None = None,
        scope_hash: str | None = None,
        scope_version: str | None = None,
        limit: int = 100,
    ) -> list[Any]:
        """List immutable resolutions in newest-first bounded order."""
        from .market_scope import MarketScopeResolutionStatus

        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 0 or limit > 1000:
            raise ValueError("limit must be an integer in [0,1000]")
        clauses: list[str] = []
        values: list[Any] = []
        for column, value in (
            ("candidate_id", candidate_id),
            ("scope_hash", scope_hash),
            ("scope_version", scope_version),
        ):
            if value is not None:
                text = str(value).strip()
                if column == "candidate_id" and not text:
                    raise ValueError("candidate_id must not be empty")
                clauses.append(column + "=?")
                values.append(text)
        if status is not None:
            normalized = status.value if isinstance(status, MarketScopeResolutionStatus) else str(status).strip().upper()
            if not normalized:
                raise ValueError("status must not be empty")
            clauses.append("status=?")
            values.append(normalized)
        query = "SELECT * FROM market_scope_resolutions"
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY resolved_at DESC,resolution_id DESC LIMIT ?"
        values.append(int(limit))
        with self._lock:
            rows = self._conn.execute(query, values).fetchall()
        return [self._market_scope_resolution_from_row(row) for row in rows]

    def list_market_scope_resolution_markets(
        self,
        candidate_id: str,
        *,
        scope_hash: str | None = None,
        scope_version: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Return matched current markets from one bounded latest resolution."""
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 0 or limit > 1000:
            raise ValueError("limit must be an integer in [0,1000]")
        resolution = self.load_market_scope_resolution(
            candidate_id,
            scope_hash=scope_hash,
            scope_version=scope_version,
        )
        if resolution is None:
            return []
        return [item.as_dict() for item in resolution.matched_markets[:limit]]

    def market_scope_resolution_funnel(
        self,
        *,
        candidate_id: str | None = None,
        limit: int = 1000,
    ) -> dict[str, Any]:
        """Aggregate latest resolution statuses/reasons for dashboard reads."""
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 0 or limit > 1000:
            raise ValueError("limit must be an integer in [0,1000]")
        clauses = ""
        values: list[Any] = []
        if candidate_id is not None:
            identifier = str(candidate_id).strip()
            if not identifier:
                raise ValueError("candidate_id must not be empty")
            clauses = "WHERE candidate_id=?"
            values.append(identifier)
        # One current row per candidate makes the funnel represent current
        # authority, not every historical reevaluation.
        query = (
            "WITH ranked AS ("
            "SELECT candidate_id,status,reason,scope_hash,scope_version,resolved_at,resolution_id,"
            "ROW_NUMBER() OVER (PARTITION BY candidate_id "
            "ORDER BY resolved_at DESC,resolution_id DESC) AS row_number "
            "FROM market_scope_resolutions "
            + clauses
            + ") SELECT candidate_id,status,reason,scope_hash,scope_version,resolved_at,resolution_id "
            "FROM ranked WHERE row_number=1 "
            "ORDER BY resolved_at DESC,resolution_id DESC LIMIT ?"
        )
        values.append(int(limit))
        with self._lock:
            rows = self._conn.execute(query, values).fetchall()
        status_counts: dict[str, int] = {}
        reason_counts: dict[str, int] = {}
        latest: list[dict[str, Any]] = []
        for row in rows:
            status_value = str(row["status"])
            reason_value = str(row["reason"])
            status_counts[status_value] = status_counts.get(status_value, 0) + 1
            reason_counts[reason_value] = reason_counts.get(reason_value, 0) + 1
            latest.append(
                {
                    "candidate_id": str(row["candidate_id"]),
                    "status": status_value,
                    "reason": reason_value,
                    "scope_hash": str(row["scope_hash"]),
                    "scope_version": str(row["scope_version"]),
                    "resolved_at": str(row["resolved_at"]),
                    "resolution_id": str(row["resolution_id"]),
                }
            )
        stages = [{"status": key, "count": value} for key, value in sorted(status_counts.items())]
        blockers = [{"reason": key, "count": value} for key, value in sorted(reason_counts.items())]
        return {
            "total": len(rows),
            "status_counts": dict(status_counts),
            "reason_counts": dict(reason_counts),
            "statuses": dict(status_counts),
            "reasons": dict(reason_counts),
            "stages": stages,
            "blockers": blockers,
            "latest_resolved_at": latest[0]["resolved_at"] if latest else None,
            "items": latest,
            "resolutions": latest,
        }

    def _market_scope_resolution_from_row(self, row: sqlite3.Row) -> Any:
        from .market_scope import MarketScopeResolution

        return MarketScopeResolution(
            candidate_id=row["candidate_id"],
            scope_hash=row["scope_hash"],
            scope_version=row["scope_version"],
            resolved_at=row["resolved_at"],
            status=row["status"],
            reason=row["reason"],
            policy=_load(row["policy_json"]),
            matched_markets=_load(row["matched_markets_json"]),
            excluded_markets=_load(row["excluded_markets_json"]),
            deferred_markets=_load(row["deferred_markets_json"]),
            provenance=_load(row["provenance_json"]),
            schema_version="1",
            resolution_id=row["resolution_id"],
        )
    # Alternate names retained for consumers that call the authority
    # ``current_market_resolution`` rather than ``market_scope_resolution``.
    save_current_market_resolution = save_market_scope_resolution
    load_current_market_resolution = load_market_scope_resolution
    list_current_market_resolutions = list_market_scope_resolutions
    current_market_scope_funnel = market_scope_resolution_funnel


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
    def list_candidate_lifecycle_for_dashboard(self, *, limit: int = 1000) -> list[dict[str, Any]]:
        """Read bounded lifecycle rows without blocking on the writer handle.

        Dashboard portfolio projections only need candidate stage and payload
        identity.  Keep this read path separate from ``load_candidate_lifecycle``
        so lifecycle/evidence callers retain the writer-transaction semantics
        required for read-your-writes behavior.
        """
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 0:
            raise ValueError("limit must be a non-negative integer")
        query = (
            "SELECT candidate_id,stage,payload_json,updated_at "
            "FROM candidate_lifecycle ORDER BY updated_at,candidate_id LIMIT ?"
        )
        if self.path not in {":memory:", ""} and not self.path.startswith("file:"):
            snapshot = self._snapshot_read_connection()
            try:
                rows = snapshot.execute(query, (int(limit),)).fetchall()
            finally:
                snapshot.close()
        else:
            with self._lock:
                rows = self._conn.execute(query, (int(limit),)).fetchall()
        return [
            {
                "candidate_id": row["candidate_id"],
                "stage": row["stage"],
                "payload": _load(row["payload_json"]),
                "updated_at": _parse_datetime(row["updated_at"]),
            }
            for row in rows
        ]

    def list_candidate_lifecycle_dashboard(self, *, limit: int = 50) -> list[dict[str, Any]]:
        """Return candidate card/progress fields without loading full payloads."""
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 0:
            raise ValueError("limit must be a non-negative integer")
        fields = (
            ("candidate_id", "$.candidate_id"),
            ("strategy_id", "$.strategy_id"),
            ("experiment_family", "$.experiment_family"),
            ("family", "$.family"),
            ("market_type", "$.market_type"),
            ("market", "$.market"),
            ("dataset_id", "$.dataset_id"),
            ("dataset_version", "$.dataset_version"),
            ("plan_hash", "$.plan_hash"),
            ("frozen_hash", "$.frozen_hash"),
            ("qualification_hash", "$.qualification_hash"),
            ("stage", "$.stage"),
            ("blocker", "$.blocker"),
            ("reason_code", "$.reason_code"),
            ("validation_sample_count", "$.validation_sample_count"),
            ("validation_trade_count", "$.validation_trade_count"),
            ("raw_observations", "$.raw_observations"),
            ("sample_count", "$.sample_count"),
            ("trade_count", "$.trade_count"),
            ("required_samples", "$.required_samples"),
            ("required_trades", "$.required_trades"),
            ("dataset_selector", "$.dataset_selector"),
            ("dataset_provenance", "$.dataset_provenance"),
            ("provenance", "$.provenance"),
            ("market_scope", "$.market_scope"),
            ("experiment_plan", "$.experiment_plan"),
            ("minimum_sample_check", "$.minimum_sample_check"),
            ("validation", "$.validation"),
            ("forward_evidence", "$.forward_evidence"),
            ("historical_evidence", "$.historical_evidence"),
        )
        paths = ",".join(repr(path) for _, path in fields)
        query = (
            "SELECT candidate_id,stage,updated_at,"
            "json_extract(CASE WHEN json_valid(payload_json) THEN payload_json ELSE '{}' END,"
            f"{paths}) AS projected_fields_json "
            "FROM candidate_lifecycle ORDER BY updated_at DESC,candidate_id ASC LIMIT ?"
        )
        with self._lock:
            rows = self._conn.execute(query, (int(limit),)).fetchall()

        def decode(value: Any) -> Any:
            try:
                return json.loads(value) if value is not None else None
            except (TypeError, ValueError, json.JSONDecodeError):
                return None

        nested = {
            "dataset_selector",
            "dataset_provenance",
            "provenance",
            "market_scope",
            "experiment_plan",
            "minimum_sample_check",
            "validation",
            "forward_evidence",
            "historical_evidence",
        }
        result: list[dict[str, Any]] = []
        for row in rows:
            values = decode(row["projected_fields_json"])
            values = values if isinstance(values, list) else []
            payload = {
                key: value
                for (key, _), value in zip(fields, values)
                if value is not None
            }
            for key in nested:
                decoded = decode(payload.get(key))
                if decoded is not None:
                    payload[key] = decoded
            result.append(
                {
                    "candidate_id": row["candidate_id"],
                    "stage": row["stage"],
                    "payload": payload,
                    "updated_at": _parse_datetime(row["updated_at"]),
                }
            )
        return result

    def load_candidate_lifecycle_page(
        self,
        *,
        limit: int = _DEFAULT_PAGE_SIZE,
        after_updated_at: datetime | str | None = None,
        after_candidate_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """Read one deterministic keyset page of lifecycle rows.

        ``(updated_at, candidate_id)`` is the complete ordering key.  The
        candidate id makes equal timestamps unambiguous, while keyset
        selection avoids offset drift when rows are inserted or deleted
        between pages.
        """
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 0:
            raise ValueError("limit must be a non-negative integer")
        if (after_updated_at is None) != (after_candidate_id is None):
            raise ValueError("after_updated_at and after_candidate_id must be provided together")
        cursor_updated_at: str | None = None
        cursor_candidate_id: str | None = None
        if after_updated_at is not None:
            if isinstance(after_updated_at, datetime):
                cursor_updated_at = _iso(after_updated_at)
            else:
                cursor_updated_at = str(after_updated_at).strip()
            cursor_candidate_id = str(after_candidate_id or "").strip()
            if not cursor_updated_at or not cursor_candidate_id:
                raise ValueError("keyset cursor values must be non-empty")
        query = (
            "SELECT candidate_id,stage,payload_json,updated_at "
            "FROM candidate_lifecycle"
        )
        values: list[Any] = []
        if cursor_updated_at is not None and cursor_candidate_id is not None:
            query += (
                " WHERE updated_at>? "
                "OR (updated_at=? AND candidate_id>?)"
            )
            values.extend((cursor_updated_at, cursor_updated_at, cursor_candidate_id))
        query += " ORDER BY updated_at,candidate_id LIMIT ?"
        values.append(int(limit))
        with self._lock:
            rows = self._conn.execute(query, values).fetchall()
        return [
            {
                "candidate_id": row["candidate_id"],
                "stage": row["stage"],
                "payload": _load(row["payload_json"]),
                "updated_at": _parse_datetime(row["updated_at"]),
            }
            for row in rows
        ]


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
        query = "SELECT * FROM paper_state ORDER BY updated_at DESC,experiment_id LIMIT ?"
        if self.path not in {":memory:", ""} and not self.path.startswith("file:"):
            snapshot = self._snapshot_read_connection()
            try:
                rows = snapshot.execute(query, (int(limit),)).fetchall()
            finally:
                snapshot.close()
        else:
            with self._lock:
                rows = self._conn.execute(query, (int(limit),)).fetchall()
        return [
            {
                "experiment_id": row["experiment_id"],
                "state": _load(row["state_json"]),
                "updated_at": _parse_datetime(row["updated_at"]),
            }
            for row in rows
        ]
    def list_paper_states_for_experiments(
        self,
        experiment_ids: Iterable[str],
        *,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        """Load only paper states bound to the supplied experiment IDs."""
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 0:
            raise ValueError("limit must be non-negative")
        identifiers = sorted(
            {
                str(value).strip()
                for value in experiment_ids
                if str(value).strip()
            }
        )
        if not identifiers or limit == 0:
            return []
        rows: list[sqlite3.Row] = []
        if self.path not in {":memory:", ""} and not self.path.startswith("file:"):
            snapshot = self._snapshot_read_connection()
            try:
                for offset in range(0, len(identifiers), 900):
                    batch = identifiers[offset : offset + 900]
                    placeholders = ",".join("?" for _ in batch)
                    rows.extend(
                        snapshot.execute(
                            "SELECT * FROM paper_state "
                            f"WHERE experiment_id IN ({placeholders}) "
                            "ORDER BY updated_at DESC,experiment_id ASC",
                            batch,
                        ).fetchall()
                    )
            finally:
                snapshot.close()
        else:
            with self._lock:
                for offset in range(0, len(identifiers), 900):
                    batch = identifiers[offset : offset + 900]
                    placeholders = ",".join("?" for _ in batch)
                    rows.extend(
                        self._conn.execute(
                            "SELECT * FROM paper_state "
                            f"WHERE experiment_id IN ({placeholders}) "
                            "ORDER BY updated_at DESC,experiment_id ASC",
                            batch,
                        ).fetchall()
                    )
        rows.sort(key=lambda row: row["experiment_id"])
        rows.sort(key=lambda row: row["updated_at"], reverse=True)
        return [
            {
                "experiment_id": row["experiment_id"],
                "state": _load(row["state_json"]),
                "updated_at": _parse_datetime(row["updated_at"]),
            }
            for row in rows[:limit]
        ]

    def list_paper_portfolio_states(
        self,
        *,
        experiment_ids: Iterable[str] | None = None,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        """Load only the fields needed to render paper portfolio summaries.

        Paper engine state also carries unbounded observation history.  The
        dashboard never renders that history, so avoid transferring or
        decoding it when computing portfolio totals.
        """
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 0:
            raise ValueError("limit must be non-negative")
        identifiers = sorted(
            {
                str(value).strip()
                for value in experiment_ids or ()
                if str(value).strip()
            }
        )
        projection = f"{_paper_state_projection_sql()} AS projection_json"
        rows: list[sqlite3.Row] = []
        if not identifiers:
            query = (
                "SELECT experiment_id,updated_at,"
                f"{projection} FROM paper_state "
                "ORDER BY updated_at DESC,experiment_id ASC LIMIT ?"
            )
            values: list[Any] = _paper_state_projection_parameters(int(limit))
            batches = ((query, values),) if limit else ()
        else:
            batches = []
            for offset in range(0, len(identifiers), 900):
                batch = identifiers[offset : offset + 900]
                placeholders = ",".join("?" for _ in batch)
                batches.append(
                    (
                        "SELECT experiment_id,updated_at,"
                        f"{projection} FROM paper_state "
                        f"WHERE experiment_id IN ({placeholders}) "
                        "ORDER BY updated_at DESC,experiment_id ASC",
                        _paper_state_projection_parameters(*batch),
                    )
                )
        if self.path not in {":memory:", ""} and not self.path.startswith("file:"):
            snapshot = self._snapshot_read_connection()
            try:
                for query, values in batches:
                    rows.extend(snapshot.execute(query, values).fetchall())
            finally:
                snapshot.close()
        else:
            with self._lock:
                for query, values in batches:
                    rows.extend(self._conn.execute(query, values).fetchall())
        rows.sort(key=lambda row: row["experiment_id"])
        rows.sort(key=lambda row: row["updated_at"], reverse=True)
        result: list[dict[str, Any]] = []
        for row in rows[:limit]:
            state = _load(row["projection_json"]) if row["projection_json"] else {}
            state = state if isinstance(state, Mapping) else {}
            result.append(
                {
                    "experiment_id": row["experiment_id"],
                    "state": state,
                    "updated_at": _parse_datetime(row["updated_at"]),
                }
            )
        return result

    def paper_record_counts(self) -> dict[str, int]:
        """Return paper-table counts without scanning unrelated runtime tables."""
        if self.path in {":memory:", ""} or self.path.startswith("file:"):
            with self._lock:
                return self._paper_record_counts_locked()
        snapshot = self._snapshot_read_connection()
        try:
            return self._paper_record_counts_on(snapshot)
        finally:
            snapshot.close()

    @staticmethod
    def _paper_record_counts_on(connection: sqlite3.Connection) -> dict[str, int]:
        return {
            label: int(connection.execute(f"SELECT COUNT(*) AS n FROM {table}").fetchone()["n"])
            for table, label in (
                ("paper_state", "paper_state"),
                ("paper_observations", "paper_observations"),
                ("paper_execution_events", "paper_execution_events"),
                ("paper_bet_ledger", "paper_bet_ledger"),
            )
        }

    def _paper_record_counts_locked(self) -> dict[str, int]:
        return self._paper_record_counts_on(self._conn)


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
        if self.path not in {":memory:", ""} and not self.path.startswith("file:"):
            snapshot = self._snapshot_read_connection()
            try:
                rows = snapshot.execute(query, values).fetchall()
            finally:
                snapshot.close()
        else:
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
    def list_paper_bet_ledger_dashboard(
        self,
        experiment_id: str,
        *,
        limit: int | None = 1000,
    ) -> list[dict[str, Any]]:
        """Read only scalar bet fields needed by dashboard portfolio totals."""
        if limit is not None and (isinstance(limit, bool) or not isinstance(limit, int) or limit < 0):
            raise ValueError("limit must be a non-negative integer or None")
        query = (
            "SELECT bet_id,experiment_id,market_id,strategy_id,outcome,resolution,"
            "resolved_at,created_at,updated_at,json_extract(payload_json,'$.net_pnl') AS net_pnl "
            "FROM paper_bet_ledger WHERE experiment_id=? ORDER BY resolved_at,market_id"
        )
        values: list[Any] = [str(experiment_id)]
        if limit is not None:
            query += " LIMIT ?"
            values.append(int(limit))
        if self.path not in {":memory:", ""} and not self.path.startswith("file:"):
            snapshot = self._snapshot_read_connection()
            try:
                rows = snapshot.execute(query, values).fetchall()
            finally:
                snapshot.close()
        else:
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
                "payload": {"net_pnl": row["net_pnl"]},
                "created_at": _parse_datetime(row["created_at"]),
                "updated_at": _parse_datetime(row["updated_at"]),
            }
            for row in rows
        ]
    def paper_bet_ledger_summary(self, experiment_id: str) -> dict[str, Any]:
        """Return dashboard bet totals without decoding persisted payloads."""
        query = (
            "SELECT COUNT(*) AS count,"
            "COALESCE(SUM(CAST(json_extract(payload_json,'$.net_pnl') AS REAL)),0.0) AS net_pnl,"
            "COALESCE(SUM(CASE WHEN CAST(json_extract(payload_json,'$.net_pnl') AS REAL)>0 THEN 1 ELSE 0 END),0) AS wins "
            "FROM paper_bet_ledger WHERE experiment_id=?"
        )
        with self._lock:
            row = self._conn.execute(query, (str(experiment_id),)).fetchone()
        return {
            "count": int(row["count"] if row is not None else 0),
            "net_pnl": float(row["net_pnl"] if row is not None else 0.0),
            "wins": int(row["wins"] if row is not None else 0),
        }

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
    def get_worker_state(self, worker_name: str) -> dict[str, Any] | None:
        """Return one persisted worker row by its exact name."""
        worker = str(worker_name).strip()
        if not worker:
            return None
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM worker_state WHERE worker_name=? LIMIT 1", (worker,)
            ).fetchone()
        if row is None:
            return None
        return {
            "worker_name": row["worker_name"],
            "status": row["status"],
            "payload": _load(row["payload_json"]),
            "started_at": _parse_datetime(row["started_at"]),
            "heartbeat_at": _parse_datetime(row["heartbeat_at"]),
            "updated_at": _parse_datetime(row["updated_at"]),
        }


    def list_worker_states_dashboard(self, *, limit: int = 32) -> list[dict[str, Any]]:
        """Return worker liveness scalars without transferring raw payloads.

        Worker diagnostics are append-only JSON and may contain multi-megabyte
        histories.  Overview cards only need scalar liveness/progress fields,
        so extract those fields in SQLite and never materialize ``payload_json``
        in the dashboard process.
        """
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 0:
            raise ValueError("limit must be a non-negative integer")
        scalar_fields = (
            ("worker_identity_valid", "$.worker_identity_valid"),
            ("stale_after_seconds", "$.stale_after_seconds"),
            ("degrading_reason", "$.degrading_reason"),
            ("reason_code", "$.reason_code"),
            ("last_error", "$.last_error"),
            ("grade", "$.grade"),
            ("configured_interval_seconds", "$.configured_interval_seconds"),
            ("effective_collection_cadence_seconds", "$.effective_collection_cadence_seconds"),
            ("last_cycle_duration_seconds", "$.last_cycle_duration_seconds"),
            ("last_cycle_started_at", "$.last_cycle_started_at"),
            ("last_cycle_ended_at", "$.last_cycle_ended_at"),
            ("last_cycle_markets_attempted", "$.last_cycle_markets_attempted"),
            ("last_cycle_markets_successful", "$.last_cycle_markets_successful"),
            ("last_cycle_markets_failed", "$.last_cycle_markets_failed"),
            ("last_successful_collection_at", "$.last_successful_collection_at"),
            ("last_successful_tick", "$.last_successful_tick"),
            ("next_scheduled_collection_at", "$.next_scheduled_collection_at"),
            ("worker_heartbeat_at", "$.worker_heartbeat_at"),
            ("collection_errors", "$.collection_errors"),
            ("stale_market_count", "$.stale_market_count"),
            ("gap_count", "$.gap_count"),
            ("markets_attempted", "$.markets_attempted"),
            ("markets_successful", "$.markets_successful"),
            ("markets_failed", "$.markets_failed"),
            ("passes", "$.passes"),
            ("queue_items_processed", "$.queue_items_processed"),
            ("processed_candidates", "$.processed_candidates"),
            ("remaining_candidates", "$.remaining_candidates"),
            ("worker_status", "$.worker_status"),
            ("last_tick_at", "$.last_tick_at"),
            ("last_tick_started_at", "$.last_tick_started_at"),
            ("last_tick_completed_at", "$.last_tick_completed_at"),
            ("last_error_code", "$.last_error_code"),
            ("consecutive_failures", "$.consecutive_failures"),
            ("next_retry_at", "$.next_retry_at"),
            ("candidates_evaluated", "$.candidates_evaluated"),
            ("signals_generated", "$.signals_generated"),
            ("orders_attempted", "$.orders_attempted"),
            ("next_decision", "$.next_decision"),
            ("blocker", "$.blocker"),
            ("last_signal_id", "$.last_signal_id"),
            ("cycle_status", "$.last_cycle.status"),
            ("cycle_completed_at", "$.last_cycle.completed_at"),
            ("cycle_ended_at", "$.last_cycle.ended_at"),
            ("cycle_last_completion_at", "$.last_cycle.last_completion_at"),
            ("cycle_last_completed_at", "$.last_cycle.last_completed_at"),
            ("cycle_cycle_ended_at", "$.last_cycle.cycle_ended_at"),
            ("crypto_enabled", "$.crypto_paper.enabled"),
            ("crypto_last_error", "$.crypto_paper.last_error"),
        )
        projection_paths = ",".join(repr(path) for _, path in scalar_fields)
        query = (
            "SELECT worker_name,status,heartbeat_at,updated_at,LENGTH(payload_json) AS payload_bytes,"
            "json_extract(CASE WHEN LENGTH(payload_json)<=262144 AND json_valid(payload_json) "
            "THEN payload_json ELSE '{}' END,"
            f"{projection_paths}) AS projected_fields_json "
            "FROM worker_state "
            "ORDER BY CASE WHEN worker_name IN "
            "('polymarket-collector','paper-engine','research-engine','health-monitor','axiom-node') "
            "THEN 0 ELSE 1 END,updated_at DESC,worker_name ASC LIMIT ?"
        )
        with self._lock:
            rows = self._conn.execute(query, (int(limit),)).fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            try:
                values = json.loads(row["projected_fields_json"] or "[]")
            except (TypeError, ValueError, json.JSONDecodeError):
                values = []
            values = values if isinstance(values, list) else []
            payload = {
                key: value
                for (key, _), value in zip(scalar_fields, values)
                if value is not None
            }
            cycle = {
                key.removeprefix("cycle_"): value
                for key, _ in scalar_fields
                for value in (payload.pop(key, None),)
                if key.startswith("cycle_") and value is not None
            }
            if cycle:
                payload["last_cycle"] = cycle
            crypto = {
                key.removeprefix("crypto_"): value
                for key, _ in scalar_fields
                for value in (payload.pop(key, None),)
                if key.startswith("crypto_") and value is not None
            }
            if crypto:
                payload["crypto_paper"] = crypto
            result.append(
                {
                    "worker_name": row["worker_name"],
                    "status": row["status"],
                    "payload": payload,
                    "payload_bytes": int(row["payload_bytes"] or 0),
                    "heartbeat_at": _parse_datetime(row["heartbeat_at"]),
                    "updated_at": _parse_datetime(row["updated_at"]),
                }
            )
        return result


    def list_worker_states(self, *, limit: int = 256) -> list[dict[str, Any]]:
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 0:
            raise ValueError("limit must be a non-negative integer")
        with self._lock:
            rows = self._conn.execute(
                "SELECT worker_name,status,payload_json,started_at,heartbeat_at,updated_at,"
                "(SELECT COUNT(*) FROM json_each(payload_json)) AS payload_key_count,"
                "json_extract(payload_json,'$.worker_identity_valid') AS worker_identity_valid,"
                "json_extract(payload_json,'$.stale_after_seconds') AS stale_after_seconds,"
                "json_extract(payload_json,'$.degrading_reason') AS degrading_reason,"
                "json_extract(payload_json,'$.reason_code') AS reason_code,"
                "json_extract(payload_json,'$.last_error') AS last_error,"
                "json_extract(payload_json,'$.grade') AS grade,"
                "json_extract(payload_json,'$.crypto_paper.enabled') AS crypto_enabled,"
                "json_extract(payload_json,'$.crypto_paper.last_error') AS crypto_last_error "
                "FROM worker_state ORDER BY worker_name LIMIT ?",
                (int(limit),),
            ).fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            payload, projection = _worker_payload_projection(
                row["payload_json"],
                key_count=int(row["payload_key_count"] or 0),
                known={
                    "worker_identity_valid": row["worker_identity_valid"],
                    "stale_after_seconds": row["stale_after_seconds"],
                    "degrading_reason": row["degrading_reason"],
                    "reason_code": row["reason_code"],
                    "last_error": row["last_error"],
                    "grade": row["grade"],
                    "crypto_enabled": row["crypto_enabled"],
                    "crypto_last_error": row["crypto_last_error"],
                },
            )
            result.append(
                {
                    "worker_name": row["worker_name"],
                    "status": row["status"],
                    "payload": payload,
                    "payload_sha256": projection["sha256"],
                    "payload_bytes": projection["bytes"],
                    "payload_key_count": projection["key_count"],
                    "payload_truncated": projection["truncated"],
                    "started_at": _parse_datetime(row["started_at"]),
                    "heartbeat_at": _parse_datetime(row["heartbeat_at"]),
                    "updated_at": _parse_datetime(row["updated_at"]),
                }
            )
        return result
    def worker_state_count(self) -> int:
        """Return worker row count without hydrating worker payloads."""
        with self._lock:
            row = self._conn.execute("SELECT COUNT(*) AS n FROM worker_state").fetchone()
        return int(row["n"] if row is not None else 0)

    def candidate_forward_requirements(
        self,
        candidate_ids: Sequence[str] | None = None,
        now: datetime | None = None,
        max_candidates: int = 100,
        max_markets_per_candidate: int = 8,
        max_total_markets: int = 100,
    ) -> dict[str, Any]:
        """Return the bounded, read-only current-market authority.

        Historical dataset constituents are evidence for validation only.  They
        are removed from executable authority when their provenance identifies
        them as historical constituents; no empty authority is replaced by
        discovery of all tracked markets.
        """
        for name, value in (
            ("max_candidates", max_candidates),
            ("max_markets_per_candidate", max_markets_per_candidate),
            ("max_total_markets", max_total_markets),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")
        current = ensure_utc(now or utc_now())
        requested_ids = (
            tuple(dict.fromkeys(str(value).strip() for value in candidate_ids if str(value).strip()))
            if candidate_ids is not None
            else None
        )
        if requested_ids is not None and len(requested_ids) > max_candidates:
            requested_ids = requested_ids[:max_candidates]

        if requested_ids is None:
            rows = self.load_candidate_lifecycle(limit=max_candidates)
            candidate_rows = [item for item in rows if isinstance(item, Mapping)]
        else:
            candidate_rows = []
            for identifier in requested_ids:
                record = self.load_candidate_lifecycle(identifier)
                if isinstance(record, Mapping):
                    candidate_rows.append(record)
                else:
                    candidate_rows.append({"candidate_id": identifier, "stage": "UNKNOWN", "payload": {}})

        from .experiment_plan import (
            ExperimentPlan,
            ExperimentPlanError,
            forward_market_matches,
            historical_market_ids,
            normalize_forward_filters,
        )

        def plain(value: Any) -> Any:
            if isinstance(value, Mapping):
                return {str(key): plain(child) for key, child in value.items()}
            if isinstance(value, (list, tuple)):
                return [plain(child) for child in value]
            return value

        def source_is_historical(value: Any) -> bool:
            if not isinstance(value, Mapping):
                return False
            for key in ("source_type", "dataset_source_type", "historical_source_type"):
                if str(value.get(key, "")).strip().upper() == "HISTORICAL":
                    return True
            for key in ("dataset_provenance", "provenance", "dataset_selector", "metadata"):
                child = value.get(key)
                if source_is_historical(child):
                    return True
            return False

        def market_closed(record: Mapping[str, Any]) -> bool:
            payload = record.get("payload", {})
            payload = payload if isinstance(payload, Mapping) else {}
            snapshot = payload.get("snapshot", {})
            snapshot = snapshot if isinstance(snapshot, Mapping) else {}
            metadata = payload.get("metadata", {})
            if record.get("active") is False:
                return True
            settlement = str(
                snapshot.get("settlement", payload.get("settlement", metadata.get("settlement", "")))
                or ""
            ).strip().lower()
            if settlement in {"resolved_yes", "resolved_no", "void", "closed", "expired"}:
                return True
            closed = metadata.get("closed", payload.get("closed"))
            if isinstance(closed, bool) and closed:
                return True
            if str(closed).strip().lower() in {"1", "true", "yes", "closed"}:
                return True
            expiry = _parse_datetime(
                snapshot.get("expiry")
                or metadata.get("expiry")
                or payload.get("expiry")
            )
            return expiry is not None and expiry <= current

        prepared: list[dict[str, Any]] = []
        for row in candidate_rows[:max_candidates]:
            candidate_id = str(row.get("candidate_id", "")).strip()
            if not candidate_id:
                continue
            payload = row.get("payload", {})
            payload = dict(payload) if isinstance(payload, Mapping) else {}
            plan: ExperimentPlan | None = None
            plan_record = None
            plan_id = str(payload.get("plan_id", "")).strip()
            if plan_id:
                plan_record = self.load_experiment_plan(plan_id)
            raw_plan = plan_record.get("plan") if isinstance(plan_record, Mapping) else None
            if not isinstance(raw_plan, Mapping):
                raw_plan = payload.get("experiment_plan")
            try:
                if isinstance(raw_plan, Mapping):
                    plan = ExperimentPlan.from_mapping(
                        raw_plan,
                        hypothesis_id=str(payload.get("hypothesis_id", "")).strip() or None,
                    )
            except (ExperimentPlanError, TypeError, ValueError):
                plan = None

            declared_market_ids: tuple[str, ...] = ()
            target_markets: tuple[str, ...] = ()
            target_instrument: str | None = None
            filters: Mapping[str, Any] = {}
            restrictions: Mapping[str, Any] = {}
            historical_ids: tuple[str, ...] = ()
            if plan is not None:
                # ExperimentPlan.target_markets is the normalized, deduped
                # target list and enforces the plan's 1000-id bound.  Keep
                # this declaration separate from executable resolution so
                # omitted targets remain visible without broadening authority.
                declared_market_ids = tuple(plan.target_markets)
                target_markets = declared_market_ids
                target_instrument = plan.target_instrument
                filters = plan.market_scope.filters
                restrictions = plan.regime_restrictions
                historical_source = source_is_historical(payload) or source_is_historical(plan.dataset_selector)
                provenance = payload.get("dataset_provenance")
                historical_ids = historical_market_ids(provenance)
                if historical_source or historical_ids:
                    historical_ids = tuple(dict.fromkeys((*historical_ids, *historical_market_ids(plan.as_dict()))))
                    catalog = (
                        self.load_dataset_catalog(plan.dataset_id, plan.dataset_version)
                        if plan.dataset_id
                        else None
                    )
                    if isinstance(catalog, Mapping):
                        historical_ids = tuple(
                            dict.fromkeys((*historical_ids, *historical_market_ids(catalog)))
                        )
            else:
                target_value = payload.get("target_market_ids", payload.get("market_ids"))
                if isinstance(target_value, (list, tuple)):
                    # Legacy candidate payloads predate normalized plans. Keep
                    # their declaration bounded to the same plan limit.
                    declared_market_ids = tuple(
                        dict.fromkeys(str(item).strip() for item in target_value if str(item).strip())
                    )[:1000]
                    target_markets = declared_market_ids
                filters = payload.get("filters", payload.get("frozen_filters", {}))
                filters = filters if isinstance(filters, Mapping) else {}
                provenance = payload.get("dataset_provenance")
                historical_ids = historical_market_ids(provenance)
            historical_set = set(historical_ids)
            executable_targets = tuple(item for item in target_markets if item not in historical_set)
            ignored = tuple(item for item in target_markets if item in historical_set)
            try:
                normalized_filters = normalize_forward_filters(filters, restrictions)
            except (ExperimentPlanError, TypeError, ValueError):
                normalized_filters = {}
                filter_error = True
            else:
                filter_error = False
            prepared.append(
                {
                    "row": row,
                    "candidate_id": candidate_id,
                    "declared_market_ids": declared_market_ids,
                    "target_instrument": target_instrument,
                    "historical_set": historical_set,
                    "executable_targets": executable_targets,
                    "ignored": ignored,
                    "normalized_filters": normalized_filters,
                    "filter_error": filter_error,
                }
            )

        # Keep the projection empty unless a candidate actually authorizes
        # reusable filters.  Explicit targets are resolved in one bounded
        # lookup per active/all projection and cached for every candidate.
        active_by_id: dict[str, Mapping[str, Any]] = {}
        all_by_id: dict[str, Mapping[str, Any]] = {}
        needs_broad_inventory = any(
            not item["filter_error"]
            and not item["executable_targets"]
            and item["normalized_filters"]
            for item in prepared
        )
        if needs_broad_inventory:
            active_records = self.tracked_polymarket_markets(
                active_only=True,
                now=current,
                include_payload=True,
                limit=1000,
            )
            for record in active_records:
                if isinstance(record, Mapping):
                    market_id = str(record.get("market_id", "")).strip()
                    if market_id:
                        active_by_id[market_id] = record

        explicit_active_ids = tuple(
            dict.fromkeys(
                market_id
                for item in prepared
                if not item["filter_error"]
                for market_id in item["executable_targets"]
                if market_id not in active_by_id
            )
        )
        explicit_all_ids = tuple(
            dict.fromkeys(
                market_id
                for item in prepared
                if not item["filter_error"]
                for market_id in item["executable_targets"]
                if market_id not in all_by_id
            )
        )
        if explicit_active_ids:
            explicit_active = self.tracked_polymarket_markets(
                active_only=True,
                now=current,
                include_payload=True,
                limit=len(explicit_active_ids),
                market_ids=explicit_active_ids,
            )
            for record in explicit_active:
                if isinstance(record, Mapping):
                    market_id = str(record.get("market_id", "")).strip()
                    if market_id:
                        active_by_id[market_id] = record
        if explicit_all_ids:
            explicit_all = self.tracked_polymarket_markets(
                active_only=False,
                now=current,
                include_payload=True,
                limit=len(explicit_all_ids),
                market_ids=explicit_all_ids,
            )
            for record in explicit_all:
                if isinstance(record, Mapping):
                    market_id = str(record.get("market_id", "")).strip()
                    if market_id:
                        all_by_id[market_id] = record

        candidates: list[dict[str, Any]] = []
        union_markets: list[str] = []
        candidate_references: dict[str, list[str]] = {}
        candidate_bound_markets: dict[str, list[str]] = {}
        unresolved: list[str] = []
        closed: list[str] = []
        capacity_excluded_candidates: list[str] = []

        for item in prepared:
            row = item["row"]
            candidate_id = item["candidate_id"]
            declared_market_ids = item["declared_market_ids"]
            target_instrument = item["target_instrument"]
            historical_set = item["historical_set"]
            executable_targets = item["executable_targets"]
            ignored = item["ignored"]
            normalized_filters = item["normalized_filters"]
            filter_error = item["filter_error"]
            permitted: list[str] = []
            closed_targets: list[str] = []
            if not filter_error:
                if executable_targets:
                    for market_id in executable_targets:
                        record = active_by_id.get(market_id)
                        if (
                            record is not None
                            and forward_market_matches(
                                record,
                                normalized_filters,
                                now=current,
                                target_instrument=target_instrument,
                            )
                        ):
                            permitted.append(market_id)
                        elif market_id in all_by_id and market_closed(all_by_id[market_id]):
                            closed_targets.append(market_id)
                elif normalized_filters:
                    for market_id, record in active_by_id.items():
                        if market_id in historical_set:
                            continue
                        if forward_market_matches(
                            record,
                            normalized_filters,
                            now=current,
                            target_instrument=target_instrument,
                        ):
                            permitted.append(market_id)

            permitted = list(dict.fromkeys(permitted))[:max_markets_per_candidate]
            # Apply the global cap only while admitting new unique markets.
            # A market already admitted for another candidate remains
            # authoritative for every candidate whose requirements authorize it.
            candidate_markets: list[str] = []
            capacity_excluded_markets: list[str] = []
            for market_id in permitted:
                if market_id in union_markets:
                    candidate_markets.append(market_id)
                    continue
                if len(union_markets) >= max_total_markets:
                    capacity_excluded_markets.append(market_id)
                    continue
                union_markets.append(market_id)
                candidate_markets.append(market_id)
            if candidate_markets:
                resolution, reason_code = "RESOLVED", "CANDIDATE_FORWARD_MARKET_RESOLVED"
            elif capacity_excluded_markets:
                resolution, reason_code = "UNRESOLVED", "COLLECTOR_CAPACITY_INSUFFICIENT"
                capacity_excluded_candidates.append(candidate_id)
                unresolved.append(candidate_id)
            elif closed_targets:
                resolution, reason_code = "CLOSED", "CANDIDATE_MARKET_CLOSED"
            else:
                resolution, reason_code = "UNRESOLVED", "CANDIDATE_FORWARD_MARKET_UNRESOLVED"
            if resolution == "UNRESOLVED" and candidate_id not in unresolved:
                unresolved.append(candidate_id)
            elif resolution == "CLOSED":
                closed.append(candidate_id)
            candidate_bound_markets[candidate_id] = list(candidate_markets)
            for market_id in candidate_markets:
                candidate_references.setdefault(market_id, []).append(candidate_id)
            candidates.append(
                {
                    "candidate_id": candidate_id,
                    "stage": str(row.get("stage", "")),
                    "resolution": resolution,
                    "reason_code": reason_code,
                    "declared_market_ids": list(declared_market_ids),
                    "market_ids": list(candidate_markets),
                    "permitted_market_ids": list(candidate_markets),
                    "capacity_excluded_market_ids": list(capacity_excluded_markets),
                    "normalized_frozen_filters": plain(normalized_filters),
                    "normalized_filters": plain(normalized_filters),
                    "frozen_filters": plain(normalized_filters),
                    "historical_market_ids_ignored": list(ignored),
                }
            )

        return {
            "candidates": candidates,
            "market_ids": list(union_markets),
            "candidate_references": candidate_references,
            "candidate_bound_markets": candidate_bound_markets,
            "unresolved_candidates": unresolved,
            "closed_candidates": closed,
            "capacity_excluded_candidates": capacity_excluded_candidates,
            "capacity_excluded_candidate_count": len(capacity_excluded_candidates),
            "as_of": current.isoformat(),
        }

    def polymarket_required_health(
        self,
        requirements: Mapping[str, Any] | None = None,
        scheduled_market_ids: Sequence[str] | None = None,
        now: datetime | None = None,
        stale_after_seconds: float | None = None,
        max_markets: int = 100,
    ) -> dict[str, Any]:
        """Aggregate fresh/stale/missing health for required authority only."""
        if isinstance(max_markets, bool) or not isinstance(max_markets, int) or max_markets < 0:
            raise ValueError("max_markets must be a non-negative integer")
        bounded_max_markets = min(max_markets, _MAX_LATEST_SCAN_ROWS)
        current = ensure_utc(now or utc_now())
        stale_after = float(stale_after_seconds if stale_after_seconds is not None else 180.0)
        if not math.isfinite(stale_after) or stale_after <= 0 or stale_after > _MAX_OPERATIONAL_WINDOW_SECONDS:
            raise ValueError("stale_after_seconds must be finite, positive, and bounded")
        authority = requirements if isinstance(requirements, Mapping) else self.candidate_forward_requirements(now=current)
        raw_required = authority.get("market_ids")
        if raw_required is None:
            raw_bound = authority.get("candidate_bound_markets", ())
            if isinstance(raw_bound, Mapping):
                raw_required = [market for values in raw_bound.values() for market in values] if all(
                    isinstance(values, (list, tuple, set, frozenset)) for values in raw_bound.values()
                ) else ()
            else:
                raw_required = raw_bound
        if not isinstance(raw_required, (list, tuple, set, frozenset)):
            raw_required = ()
        required = tuple(dict.fromkeys(str(item).strip() for item in raw_required if str(item).strip()))
        assessed_required = required[:bounded_max_markets]
        capacity_truncated = required[bounded_max_markets:]
        refs = authority.get("candidate_references", {})
        refs = refs if isinstance(refs, Mapping) else {}
        normalized_refs = {
            str(market_id): tuple(
                dict.fromkeys(str(candidate).strip() for candidate in values if str(candidate).strip())
            )
            if isinstance(values, (list, tuple, set, frozenset))
            else ()
            for market_id, values in refs.items()
        }
        scheduled = (
            tuple(dict.fromkeys(str(item).strip() for item in scheduled_market_ids if str(item).strip()))
            if scheduled_market_ids is not None
            else tuple(
                dict.fromkeys(
                    str(item).strip()
                    for item in (
                        (self.get_collector_state("polymarket") or {}).get("scheduled_market_ids", ())
                        if isinstance(self.get_collector_state("polymarket") or {}, Mapping)
                        else ()
                    )
                    if str(item).strip()
                )
            )
        )
        latest_rows = self.load_latest_polymarket_snapshots(
            assessed_required,
            source_type="FORWARD_COLLECTED",
            limit=len(assessed_required),
        ) if assessed_required else []
        latest_by_market = {}
        for row in latest_rows:
            market_id = str(row.get("market_id", "")).strip() if isinstance(row, Mapping) else ""
            observed = _parse_datetime(row.get("observed_at")) if isinstance(row, Mapping) else None
            if market_id and (observed is None or observed <= current):
                latest_by_market[market_id] = row
        fresh: list[str] = []
        stale: list[str] = []
        missing: list[str] = []
        diagnostics: list[dict[str, Any]] = []
        required_snapshots: list[tuple[datetime, str]] = []
        for market_id in assessed_required:
            row = latest_by_market.get(market_id)
            if row is None:
                missing.append(market_id)
                diagnostics.append(
                    {
                        "market_id": market_id,
                        "candidate_bound": True,
                        "candidate_references": list(normalized_refs.get(market_id, ())),
                        "source_timestamp": None,
                        "observed_at": None,
                        "freshness_age_seconds": None,
                        "collection_state": "missing",
                        "reason_code": "REQUIRED_MARKET_SNAPSHOT_MISSING",
                    }
                )
                continue
            source_stamp = _parse_datetime(row.get("source_timestamp"))
            observed_stamp = _parse_datetime(row.get("observed_at"))
            if observed_stamp is None:
                missing.append(market_id)
                state, reason = "missing", "REQUIRED_MARKET_SNAPSHOT_MISSING"
                age = None
            else:
                age = max(0.0, (current - observed_stamp).total_seconds())
                if age <= stale_after:
                    fresh.append(market_id)
                    state, reason = "fresh", "REQUIRED_MARKET_SNAPSHOT_FRESH"
                else:
                    stale.append(market_id)
                    state, reason = "stale", "REQUIRED_MARKET_SNAPSHOT_STALE"
            if source_stamp is not None:
                required_snapshots.append((source_stamp, market_id))
            diagnostics.append(
                {
                    "market_id": market_id,
                    "candidate_bound": True,
                    "candidate_references": list(normalized_refs.get(market_id, ())),
                    "source_timestamp": source_stamp.isoformat() if source_stamp is not None else None,
                    "observed_at": observed_stamp.isoformat() if observed_stamp is not None else None,
                    "freshness_age_seconds": age,
                    "collection_state": state,
                    "reason_code": reason,
                }
            )
        if capacity_truncated:
            missing.extend(capacity_truncated)

        unresolved = authority.get("unresolved_candidates", ())
        closed = authority.get("closed_candidates", ())
        capacity_excluded = authority.get("capacity_excluded_candidates", ())
        unresolved = list(unresolved) if isinstance(unresolved, (list, tuple)) else []
        closed = list(closed) if isinstance(closed, (list, tuple)) else []
        capacity_excluded = list(capacity_excluded) if isinstance(capacity_excluded, (list, tuple)) else []
        if capacity_excluded or capacity_truncated:
            grade, reason_code = "D", "COLLECTOR_CAPACITY_INSUFFICIENT"
        elif unresolved:
            grade, reason_code = "D", "CANDIDATE_FORWARD_MARKET_UNRESOLVED"
        elif closed:
            grade, reason_code = "D", "CANDIDATE_MARKET_CLOSED"
        elif missing:
            grade, reason_code = "D", "REQUIRED_MARKETS_MISSING"
        elif stale:
            grade, reason_code = "C", "REQUIRED_MARKETS_STALE"
        elif required:
            grade, reason_code = "A", "REQUIRED_MARKETS_FRESH"
        else:
            grade, reason_code = "D", "CANDIDATE_FORWARD_MARKET_UNRESOLVED"
        ordered_snapshots = sorted(required_snapshots, key=lambda pair: (pair[0], pair[1]))
        return {
            "candidate_bound_markets": list(required),
            "scheduled": list(scheduled),
            "fresh": fresh,
            "stale": stale,
            "missing": missing,
            "newest_required_snapshot": ordered_snapshots[-1][0].isoformat() if ordered_snapshots else None,
            "oldest_required_snapshot": ordered_snapshots[0][0].isoformat() if ordered_snapshots else None,
            "grade": grade,
            "grade_scope": "required_forward_markets",
            "reason_code": reason_code,
            "candidate_references": {
                market_id: list(normalized_refs.get(market_id, ())) for market_id in required
            },
            "market_diagnostics": diagnostics[:100],
            "diagnostics": diagnostics[:100],
            "required_market_count": len(required),
            "unresolved_candidates": unresolved,
            "closed_candidates": closed,
            "capacity_excluded_candidates": capacity_excluded,
            "capacity_excluded_candidate_count": len(capacity_excluded),
            "as_of": current.isoformat(),
        }

    def tracked_polymarket_markets(
        self,
        *,
        active_only: bool = True,
        now: datetime | None = None,
        include_payload: bool = False,
        limit: int = 1000,
        market_ids: Sequence[str] | None = None,
    ) -> list[Any]:
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 0:
            raise ValueError("limit must be a non-negative integer")
        requested_ids = (
            tuple(dict.fromkeys(str(item).strip() for item in market_ids if str(item).strip()))
            if market_ids is not None
            else ()
        )
        if market_ids is not None and not requested_ids:
            return []
        current = ensure_utc(now or utc_now())
        snapshot = self._snapshot_read_connection()
        try:
            metadata_where = "source_type='FORWARD_COLLECTED' AND observed_at<=?"
            metadata_values: list[Any] = [current.isoformat()]
            snapshot_where = "source_type='FORWARD_COLLECTED' AND observed_at<=?"
            snapshot_values: list[Any] = [current.isoformat()]
            if requested_ids:
                placeholders = ",".join("?" for _ in requested_ids)
                metadata_where += f" AND market_id IN ({placeholders})"
                metadata_values.extend(requested_ids)
                snapshot_where += f" AND market_id IN ({placeholders})"
                snapshot_values.extend(requested_ids)
            metadata_rows = snapshot.execute(
                "WITH latest_keys AS ("
                "SELECT rowid AS row_id,market_id,observed_at,metadata_hash,"
                "ROW_NUMBER() OVER (PARTITION BY market_id ORDER BY observed_at DESC,metadata_hash DESC) AS row_number "
                f"FROM polymarket_markets WHERE {metadata_where}) "
                "SELECT p.market_id,p.observed_at,p.metadata_hash,p.payload_json,p.source_type "
                "FROM polymarket_markets AS p JOIN latest_keys AS latest ON p.rowid=latest.row_id "
                "WHERE latest.row_number=1 ORDER BY p.market_id LIMIT ?",
                [*metadata_values, int(limit)],
            ).fetchall()
            snapshot_rows = snapshot.execute(
                "WITH latest_keys AS ("
                "SELECT rowid AS row_id,market_id,observed_at,source_timestamp,snapshot_id,"
                "ROW_NUMBER() OVER (PARTITION BY market_id ORDER BY observed_at DESC,source_timestamp DESC,snapshot_id DESC) AS row_number "
                f"FROM polymarket_snapshots WHERE {snapshot_where}) "
                "SELECT p.market_id,p.observed_at,p.source_timestamp,p.snapshot_id,p.payload_json,p.source_type "
                "FROM polymarket_snapshots AS p JOIN latest_keys AS latest ON p.rowid=latest.row_id "
                "WHERE latest.row_number=1 ORDER BY p.market_id LIMIT ?",
                [*snapshot_values, int(limit)],
            ).fetchall()
        finally:
            snapshot.close()
        metadata_latest: dict[str, tuple[datetime | None, Any]] = {}
        snapshot_latest: dict[str, tuple[datetime | None, Any]] = {}
        for row in metadata_rows:
            stamp = _parse_datetime(row["observed_at"])
            if stamp is not None and stamp > current:
                continue
            identifier = str(row["market_id"])
            payload = (
                _load(row["payload_json"])
                if include_payload
                else _dashboard_payload_projection(row["payload_json"])[0]
            )
            metadata_latest[identifier] = (stamp, payload)
        for row in snapshot_rows:
            stamp = _parse_datetime(row["observed_at"])
            if stamp is not None and stamp > current:
                continue
            identifier = str(row["market_id"])
            payload = (
                _load(row["payload_json"])
                if include_payload
                else _dashboard_payload_projection(row["payload_json"])[0]
            )
            snapshot_latest[identifier] = (stamp, payload)
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
                "SELECT dataset_catalog.*, "
                "(SELECT COUNT(*) FROM json_each(dataset_catalog.metadata_json)) AS metadata_key_count, "
                "json_extract(dataset_catalog.metadata_json,'$.category') AS metadata_category, "
                "json_extract(dataset_catalog.metadata_json,'$.historical_order_book_available') AS metadata_historical_order_book_available, "
                "json_extract(dataset_catalog.metadata_json,'$.universe_version') AS metadata_universe_version, "
                "json_array_length(dataset_catalog.missing_ranges_json) AS missing_range_count "
                "FROM dataset_catalog"
                f"{where} ORDER BY {order_column} {order_direction.upper()},dataset_id ASC,dataset_version ASC "
                "LIMIT ? OFFSET ?"
            )
            rows = self._conn.execute(query, [*values, size, (actual_page - 1) * size]).fetchall()
        return {
            "items": [_dataset_catalog_dashboard_record(row) for row in rows],
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
        any_truncated = False
        for row in rows:
            parsed_range, truncated = _dataset_missing_range_projection(row["range_json"])
            any_truncated = any_truncated or truncated
            items.append(
                {
                    "dataset_id": row["dataset_id"],
                    "dataset_version": row["dataset_version"],
                    "range_index": int(row["range_index"]),
                    "range": parsed_range,
                    "missing_range": parsed_range,
                    "range_truncated": truncated,
                }
            )
        return {
            "items": items,
            "page": actual_page,
            "page_size": size,
            "total": total,
            "pages": pages,
            "range_payload_truncated": any_truncated,
        }

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
                UNION ALL
                SELECT
                    'operator', timestamp, 'operator:' || action_id,
                    'Operator action ' || action || ' on ' || target || ' ' ||
                        CASE WHEN success=1 THEN 'succeeded' ELSE 'failed' END,
                    json_object('success',success,'reason',reason,'result',json(result_json)),
                    'operator','operator',
                    CASE WHEN success=1 THEN 'SUCCEEDED' ELSE 'FAILED' END,
                    action,NULL
                FROM operator_actions
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
        fast_path = (
            str(sort or "observed_at").strip().lower()
            in {"observed_at", "source_timestamp", "market_id", "quality"}
            and not any(
                str(value or "").strip()
                for value in (timeframe, quality, category, settlement, filter)
            )
        )
        fast_cte = """
            WITH metadata_ranked AS (
                SELECT rowid AS row_id,
                    ROW_NUMBER() OVER (
                        PARTITION BY market_id
                        ORDER BY observed_at DESC,metadata_hash DESC
                    ) AS row_number
                FROM polymarket_markets
            ),
            metadata_latest AS (
                SELECT p.rowid AS row_id,p.market_id,p.observed_at,p.metadata_hash
                FROM polymarket_markets AS p
                JOIN metadata_ranked AS ranked ON ranked.row_id=p.rowid
                WHERE ranked.row_number=1
            ),
            snapshot_ranked AS (
                SELECT rowid AS row_id,
                    ROW_NUMBER() OVER (
                        PARTITION BY market_id
                        ORDER BY observed_at DESC,source_timestamp DESC,snapshot_id DESC
                    ) AS row_number
                FROM polymarket_snapshots
            ),
            snapshot_latest AS (
                SELECT p.rowid AS row_id,p.market_id,p.observed_at,p.source_timestamp,
                    p.snapshot_id,p.quality
                FROM polymarket_snapshots AS p
                JOIN snapshot_ranked AS ranked ON ranked.row_id=p.rowid
                WHERE ranked.row_number=1
            ),
            markets AS (
                SELECT
                    m.market_id,
                    COALESCE(s.observed_at,m.observed_at) AS observed_at,
                    m.observed_at AS metadata_observed_at,
                    m.metadata_hash,
                    m.row_id AS metadata_row_id,
                    s.observed_at AS snapshot_observed_at,
                    s.source_timestamp,
                    s.snapshot_id,
                    s.row_id AS snapshot_row_id,
                    s.quality,
                    NULL AS category,NULL AS timeframe,NULL AS settlement
                FROM metadata_latest AS m
                LEFT JOIN snapshot_latest AS s ON s.market_id=m.market_id
                UNION ALL
                SELECT
                    s.market_id,s.observed_at,NULL,NULL,NULL,
                    s.observed_at,s.source_timestamp,s.snapshot_id,s.row_id,
                    s.quality,NULL AS category,NULL AS timeframe,NULL AS settlement
                FROM snapshot_latest AS s
                LEFT JOIN metadata_latest AS m ON m.market_id=s.market_id
                WHERE m.market_id IS NULL
            )
        """
        # Rank only row ids first.  Selecting payload_json in the window CTE
        # forces SQLite to carry every large persisted payload through both
        # latest-per-market scans before LIMIT/OFFSET can apply.  Join the
        # winning rows back to their payloads only after the window is bounded
        # to one row per market.
        cte = """
            WITH metadata_ranked AS (
                SELECT rowid AS row_id,
                    ROW_NUMBER() OVER (
                        PARTITION BY market_id
                        ORDER BY observed_at DESC,metadata_hash DESC
                    ) AS row_number
                FROM polymarket_markets
            ),
            metadata_latest AS (
                SELECT p.market_id,p.observed_at,p.metadata_hash,p.payload_json
                FROM polymarket_markets AS p
                JOIN metadata_ranked AS ranked ON ranked.row_id=p.rowid
                WHERE ranked.row_number=1
            ),
            snapshot_ranked AS (
                SELECT rowid AS row_id,
                    ROW_NUMBER() OVER (
                        PARTITION BY market_id
                        ORDER BY observed_at DESC,source_timestamp DESC,snapshot_id DESC
                    ) AS row_number
                FROM polymarket_snapshots
            ),
            snapshot_latest AS (
                SELECT p.market_id,p.observed_at,p.source_timestamp,p.snapshot_id,
                    p.payload_json,p.quality
                FROM polymarket_snapshots AS p
                JOIN snapshot_ranked AS ranked ON ranked.row_id=p.rowid
                WHERE ranked.row_number=1
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
        query_cte = fast_cte if fast_path else cte
        with self._lock:
            total = int(
                self._conn.execute(
                    f"{query_cte} SELECT COUNT(*) AS n FROM markets{where}",
                    values,
                ).fetchone()["n"]
            )
            actual_page, pages = _pagination_shape(requested_page, size, total)
            if fast_path:
                rows_query = (
                    f"{query_cte}, page AS ("
                    "SELECT market_id,observed_at,metadata_observed_at,metadata_hash,"
                    "metadata_row_id,snapshot_observed_at,source_timestamp,snapshot_id,"
                    "snapshot_row_id,quality "
                    f"FROM markets{where} "
                    f"ORDER BY {order_column} {order_direction.upper()},market_id ASC "
                    "LIMIT ? OFFSET ?) "
                    "SELECT page.market_id,page.observed_at,page.metadata_observed_at,"
                    "page.metadata_hash,meta.payload_json AS metadata_payload,"
                    "page.snapshot_observed_at,page.source_timestamp,page.snapshot_id,"
                    "snap.payload_json AS snapshot_payload,page.quality,"
                    "COALESCE("
                    "json_extract(snap.payload_json,'$.category'),"
                    "json_extract(snap.payload_json,'$.snapshot.category'),"
                    "json_extract(snap.payload_json,'$.metadata.category'),"
                    "json_extract(meta.payload_json,'$.category'),"
                    "json_extract(meta.payload_json,'$.metadata.category')) AS category,"
                    "COALESCE("
                    "json_extract(snap.payload_json,'$.timeframe'),"
                    "json_extract(snap.payload_json,'$.snapshot.timeframe'),"
                    "json_extract(snap.payload_json,'$.metadata.timeframe'),"
                    "json_extract(meta.payload_json,'$.timeframe'),"
                    "json_extract(meta.payload_json,'$.metadata.timeframe')) AS timeframe,"
                    "lower(COALESCE("
                    "json_extract(snap.payload_json,'$.settlement'),"
                    "json_extract(snap.payload_json,'$.snapshot.settlement'),"
                    "json_extract(snap.payload_json,'$.metadata.settlement'),"
                    "json_extract(meta.payload_json,'$.settlement'),"
                    "json_extract(meta.payload_json,'$.metadata.settlement'))) AS settlement "
                    "FROM page "
                    "LEFT JOIN polymarket_markets AS meta ON meta.rowid=page.metadata_row_id "
                    "LEFT JOIN polymarket_snapshots AS snap ON snap.rowid=page.snapshot_row_id"
                )
                query_values = [*values, size, (actual_page - 1) * size]
            else:
                rows_query = (
                    f"{query_cte} SELECT market_id,observed_at,metadata_observed_at,"
                    "metadata_hash,metadata_payload,snapshot_observed_at,source_timestamp,"
                    "snapshot_id,snapshot_payload,quality,category,timeframe,settlement "
                    f"FROM markets{where} ORDER BY {order_column} "
                    f"{order_direction.upper()},market_id ASC LIMIT ? OFFSET ?"
                )
                query_values = [*values, size, (actual_page - 1) * size]
            rows = self._conn.execute(rows_query, query_values).fetchall()
        items: list[dict[str, Any]] = []
        for row in rows:
            metadata, metadata_projection = _dashboard_payload_projection(row["metadata_payload"])
            snapshot, snapshot_projection = _dashboard_payload_projection(row["snapshot_payload"])
            metadata = metadata if isinstance(metadata, Mapping) else {}
            snapshot_record = snapshot if isinstance(snapshot, Mapping) else {}
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
                "metadata_sha256": metadata_projection.get("sha256"),
                "metadata_bytes": metadata_projection.get("bytes", 0),
                "metadata_truncated": bool(metadata_projection.get("truncated")),
                "snapshot_sha256": snapshot_projection.get("sha256"),
                "snapshot_bytes": snapshot_projection.get("bytes", 0),
                "snapshot_truncated": bool(snapshot_projection.get("truncated")),
            }
            if include_snapshots:
                item["metadata"] = dict(metadata)
                item["snapshot"] = dict(snapshot_value)
            items.append(item)
        return {"items": items, "page": actual_page, "page_size": size, "total": total, "pages": pages}

    def _paginate_paper_records_unfiltered(
        self,
        *,
        requested_page: int,
        page_size: int,
        direction: str,
    ) -> dict[str, Any]:
        """Page the common unfiltered timestamp view using per-table indexes."""
        snapshot = self._snapshot_read_connection()
        try:
            counts = self._paper_record_counts_on(snapshot)
            total = sum(counts.values())
            actual_page, pages = _pagination_shape(requested_page, page_size, total)
            if not total:
                return {"items": [], "page": actual_page, "page_size": page_size, "total": 0, "pages": pages}
            source_limit = (actual_page - 1) * page_size + page_size
            order = "DESC" if direction == "desc" else "ASC"
            source_queries = (
                f"""
                    SELECT 'state' AS record_type,experiment_id AS record_id,
                        experiment_id,NULL AS market_id,updated_at AS timestamp,
                        NULL AS status,NULL AS outcome,NULL AS resolution,NULL AS strategy_id,
                        NULL AS payload_json,updated_at AS created_at,updated_at
                    FROM paper_state
                    ORDER BY updated_at {order},experiment_id ASC LIMIT ?
                """,
                f"""
                    SELECT 'observation' AS record_type,observation_id AS record_id,
                        experiment_id,market_id,timestamp,NULL AS status,
                        NULL AS outcome,NULL AS resolution,NULL AS strategy_id,
                        NULL AS payload_json,created_at,created_at AS updated_at
                    FROM paper_observations
                    ORDER BY timestamp {order},observation_id ASC LIMIT ?
                """,
                f"""
                    SELECT 'execution' AS record_type,event_id AS record_id,
                        experiment_id,market_id,timestamp,status,
                        NULL AS outcome,NULL AS resolution,NULL AS strategy_id,
                        NULL AS payload_json,created_at,created_at AS updated_at
                    FROM paper_execution_events
                    ORDER BY timestamp {order},event_id ASC LIMIT ?
                """,
                f"""
                    SELECT 'bet' AS record_type,bet_id AS record_id,
                        experiment_id,market_id,resolved_at AS timestamp,
                        resolution AS status,outcome,resolution,strategy_id,
                        NULL AS payload_json,created_at,updated_at
                    FROM paper_bet_ledger
                    ORDER BY resolved_at {order},bet_id ASC LIMIT ?
                """,
            )
            rows: list[sqlite3.Row] = []
            for query in source_queries:
                rows.extend(snapshot.execute(query, (source_limit,)).fetchall())
            rows.sort(key=lambda row: (row["record_id"], row["record_type"]))
            rows.sort(key=lambda row: row["timestamp"], reverse=direction == "desc")
            offset = (actual_page - 1) * page_size
            page_rows = rows[offset : offset + page_size]
            payloads: dict[tuple[str, str], tuple[Any, Any]] = {}
            payload_specs = (
                ("state", "paper_state", "experiment_id", "state_json", "json_extract(state_json,'$.status')"),
                ("observation", "paper_observations", "observation_id", "payload_json", "json_extract(payload_json,'$.status')"),
                ("execution", "paper_execution_events", "event_id", "payload_json", "NULL"),
                ("bet", "paper_bet_ledger", "bet_id", "payload_json", "NULL"),
            )
            for record_type, table, id_column, payload_column, status_expression in payload_specs:
                identifiers = [
                    str(row["record_id"])
                    for row in page_rows
                    if str(row["record_type"]) == record_type
                ]
                if not identifiers:
                    continue
                placeholders = ",".join("?" for _ in identifiers)
                if record_type == "state":
                    payload_expression = _paper_state_projection_sql()
                    payload_values = _paper_state_projection_parameters(*identifiers)
                else:
                    payload_expression = payload_column
                    payload_values = identifiers
                payload_rows = snapshot.execute(
                    f"SELECT {id_column} AS record_id,{payload_expression} AS payload_json,"
                    f"{status_expression} AS payload_status FROM {table} "
                    f"WHERE {id_column} IN ({placeholders})",
                    payload_values,
                ).fetchall()
                payloads.update(
                    {
                        (record_type, str(row["record_id"])): (
                            row["payload_json"],
                            row["payload_status"],
                        )
                        for row in payload_rows
                    }
                )
        finally:
            snapshot.close()
        items: list[dict[str, Any]] = []
        for row in page_rows:
            payload_json, payload_status = payloads.get(
                (str(row["record_type"]), str(row["record_id"])),
                (None, None),
            )
            metadata: dict[str, Any] = {}
            if str(row["record_type"]) == "state":
                payload = _load(payload_json) if payload_json else {}
            else:
                payload, metadata = _dashboard_payload_projection(payload_json)
            item = {
                "record_type": row["record_type"],
                "record_id": row["record_id"],
                "id": row["record_id"],
                "experiment_id": row["experiment_id"],
                "market_id": row["market_id"],
                "timestamp": _parse_datetime(row["timestamp"]),
                "status": row["status"] if row["status"] is not None else payload_status,
                "outcome": row["outcome"],
                "resolution": row["resolution"],
                "strategy_id": row["strategy_id"],
                "payload": payload,
                "created_at": _parse_datetime(row["created_at"]),
                "updated_at": _parse_datetime(row["updated_at"]),
            }
            if metadata:
                item.update(_dashboard_payload_fields(metadata))
            if row["record_type"] == "state":
                item["state"] = payload
            items.append(item)
        return {"items": items, "page": actual_page, "page_size": page_size, "total": total, "pages": pages}
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
        if order_column == "timestamp" and not any(
            str(value or "").strip()
            for value in (experiment_id, market, market_id, status, record_type, filter)
        ):
            return self._paginate_paper_records_unfiltered(
                requested_page=requested_page,
                page_size=size,
                direction=order_direction,
            )

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
                "resolution,strategy_id,"
                "CASE WHEN record_type='state' THEN NULL ELSE payload_json END AS payload_json,"
                "created_at,updated_at "
                f"FROM paper_records{where} ORDER BY {order_column} {order_direction.upper()},record_id ASC,record_type ASC "
                "LIMIT ? OFFSET ?",
                [*values, size, (actual_page - 1) * size],
            ).fetchall()
            state_ids = [
                str(row["record_id"])
                for row in rows
                if str(row["record_type"]) == "state"
            ]
            state_payloads: dict[str, Any] = {}
            if state_ids:
                placeholders = ",".join("?" for _ in state_ids)
                state_payload_rows = self._conn.execute(
                    f"SELECT experiment_id AS record_id,{_paper_state_projection_sql()} AS payload_json "
                    f"FROM paper_state WHERE experiment_id IN ({placeholders})",
                    _paper_state_projection_parameters(*state_ids),
                ).fetchall()
                state_payloads = {
                    str(row["record_id"]): row["payload_json"]
                    for row in state_payload_rows
                }
        items: list[dict[str, Any]] = []
        for row in rows:
            payload_json = (
                state_payloads.get(str(row["record_id"]))
                if str(row["record_type"]) == "state"
                else row["payload_json"]
            )
            metadata: dict[str, Any] = {}
            if str(row["record_type"]) == "state":
                payload = _load(payload_json) if payload_json else {}
            else:
                payload, metadata = _dashboard_payload_projection(payload_json)
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
            if metadata:
                item.update(_dashboard_payload_fields(metadata))
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
        bar_where = " WHERE " + " AND ".join(f"b.{clause}" for clause in clauses) if clauses else ""
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
            quality_rows = self._conn.execute(
                "SELECT COALESCE(NULLIF(c.quality,''),NULLIF(d.quality,''),"
                "NULLIF(CASE WHEN json_valid(b.payload_json) "
                "THEN json_extract(b.payload_json,'$.quality') END,''),"
                "NULLIF(CASE WHEN json_valid(b.payload_json) "
                "THEN json_extract(b.payload_json,'$.research_quality') END,''),"
                "'UNKNOWN') AS quality,COUNT(*) AS n "
                "FROM bars AS b "
                "LEFT JOIN dataset_catalog AS c "
                "ON c.dataset_id=b.dataset_id AND c.dataset_version=b.dataset_version "
                "LEFT JOIN datasets AS d "
                "ON d.dataset_id=b.dataset_id AND d.version=b.dataset_version"
                + bar_where
                + " GROUP BY 1",
                values,
            ).fetchall()
            quality = {str(row["quality"] or "UNKNOWN"): int(row["n"]) for row in quality_rows}
        return {
            "count": int(count),
            "snapshots": int(snapshots),
            "datasets": int(datasets),
            "catalog_count": int(catalog_count),
            "historical_catalog": int(historical_catalog_count),
            "forward_catalog": int(forward_catalog_count),
            "quality": quality,
        }

    def dashboard_overview_summary(self, *, activity_limit: int = 8) -> dict[str, Any]:
        """Return bounded SQL aggregates for the dashboard overview."""
        if isinstance(activity_limit, bool) or not isinstance(activity_limit, int) or not 1 <= activity_limit <= 32:
            raise ValueError("activity_limit must be between 1 and 32")
        count_tables = (
            ("dataset_catalog", "dataset_catalog", None),
            ("polymarket_snapshots", "polymarket_snapshots", "idx_polymarket_snapshots_dashboard"),
            # Trades are append-only; scanning their payload-bearing table for COUNT(*) makes
            # a cold dashboard read proportional to the full historical tape.
            ("polymarket_trades", "polymarket_trades", "__append_only_rowid__"),
            ("collection_errors", "collection_errors", "idx_collection_errors_observed"),
            ("collection_cycles", "collection_cycles", "idx_collection_cycles_time"),
            ("research_queue", "research_queue", None),
            ("candidate_lifecycle", "candidate_lifecycle", None),
            ("reports", "reports", None),
            ("experiments", "experiments", None),
            ("paper_state", "paper_state", "idx_paper_state_updated"),
            ("paper_observations", "paper_observations", "idx_paper_observations_dashboard"),
            ("paper_execution_events", "paper_execution_events", "idx_paper_execution_events_dashboard"),
            ("paper_bet_ledger", "paper_bet_ledger", None),
        )
        activity_cte = """
            WITH activity(
                kind,timestamp,event_id,message,details_json,source,source_type,
                status,item_type,market_id
            ) AS (
                SELECT 'dataset',updated_at,'dataset:' || dataset_id || '/' || dataset_version,
                    'Dataset ' || dataset_id || ' published (' || row_count || ' rows)',
                    json_object('dataset_id',dataset_id,'dataset_version',dataset_version,
                        'source_type',source_type,'timeframe',timeframe,'quality',quality),
                    source_type,source_type,NULL,NULL,NULL
                FROM (SELECT * FROM dataset_catalog ORDER BY updated_at DESC LIMIT 32)
                UNION ALL
                SELECT 'bootstrap',updated_at,'bootstrap:' || dataset_id,
                    dataset_id || ' bootstrap ' || lower(status),payload_json,
                    'bootstrap','bootstrap',status,NULL,NULL
                FROM (SELECT * FROM dataset_bootstrap_state ORDER BY updated_at DESC LIMIT 32)
                UNION ALL
                SELECT 'collection',COALESCE(ended_at,started_at),'collection:' || cycle_id,
                    'Polymarket collection cycle completed (' ||
                        COALESCE(json_extract(payload_json,'$.markets_seen'),0) || ' markets)',
                    payload_json,collector_name,'collection',NULL,NULL,NULL
                FROM (SELECT * FROM collection_cycles ORDER BY COALESCE(ended_at,started_at) DESC LIMIT 32)
                UNION ALL
                SELECT 'lifecycle',created_at,'lifecycle:' || event_id,
                    'Candidate ' || candidate_id || ' moved to ' || to_stage,
                    json_object('from_stage',from_stage,'reason',reason),
                    'lifecycle','lifecycle',to_stage,NULL,NULL
                FROM (SELECT * FROM candidate_lifecycle_events ORDER BY created_at DESC LIMIT 32)
                UNION ALL
                SELECT 'research',updated_at,'research:item:' || item_id,
                    'Research item ' || item_type || ' is ' || lower(status),
                    json_object('item_id',item_id,'last_error',last_error),
                    source,'research',status,item_type,
                    json_extract(payload_json,'$.market_id')
                FROM (SELECT * FROM research_queue ORDER BY updated_at DESC LIMIT 32)
                UNION ALL
                SELECT 'research',created_at,'research:event:' || event_id,
                    'Research queue item ' || item_id || ' moved to ' || to_status,
                    detail,'queue','research',to_status,NULL,NULL
                FROM (SELECT * FROM research_queue_events ORDER BY created_at DESC LIMIT 32)
                UNION ALL
                SELECT 'report',created_at,'report:' || report_id,
                    'Research report ' || report_id || ' saved',
                    json_object('experiment_id',experiment_id),
                    'report','report',NULL,NULL,NULL
                FROM (SELECT * FROM reports ORDER BY created_at DESC LIMIT 32)
                UNION ALL
                SELECT 'collection_error',observed_at,'collection_error:' || error_id,
                    'Collection error: ' || kind || ' (' || detail || ')',
                    payload_json,'collection','collection_error',kind,NULL,market_id
                FROM (SELECT * FROM collection_errors ORDER BY observed_at DESC LIMIT 32)
            )
        """
        with self._lock:
            counts = {}
            for table, label, index in count_tables:
                if index == "__append_only_rowid__":
                    count_row = self._conn.execute(
                        f"SELECT COALESCE(MAX(rowid),0) AS n FROM {table}"
                    ).fetchone()
                else:
                    source = table if index is None else f"{table} INDEXED BY {index}"
                    count_row = self._conn.execute(
                        f"SELECT COUNT(*) AS n FROM {source}"
                    ).fetchone()
                counts[label] = int(count_row["n"])
            bars_row = self._conn.execute(
                "SELECT COALESCE(SUM(row_count),0) AS n FROM dataset_catalog "
                "WHERE lower(market_type)='crypto_spot'"
            ).fetchone()
            counts["bars"] = int(bars_row["n"] or 0)
            catalog_rows = self._conn.execute(
                "SELECT lower(source_type) AS source_type,COUNT(*) AS dataset_count,"
                "COALESCE(SUM(row_count),0) AS row_count "
                "FROM dataset_catalog GROUP BY lower(source_type)"
            ).fetchall()
            candidate_rows = self._conn.execute(
                "SELECT stage,COUNT(*) AS n FROM candidate_lifecycle GROUP BY stage ORDER BY stage"
            ).fetchall()
            queue_rows = self._conn.execute(
                "SELECT status,COUNT(*) AS n FROM research_queue GROUP BY status ORDER BY status"
            ).fetchall()
            bootstrap_rows = self._conn.execute(
                "SELECT status,COUNT(*) AS n FROM dataset_bootstrap_state GROUP BY status ORDER BY status"
            ).fetchall()
            activity_rows = self._conn.execute(
                f"{activity_cte} SELECT kind,timestamp,event_id,message,details_json,source,source_type,"
                "status,item_type,market_id FROM activity "
                "ORDER BY timestamp DESC,event_id ASC LIMIT ?",
                (activity_limit,),
            ).fetchall()
        workers = self.list_worker_states_dashboard(limit=32)
        catalog = {
            str(row["source_type"] or "unknown"): {
                "datasets": int(row["dataset_count"]),
                "rows": int(row["row_count"]),
            }
            for row in catalog_rows
        }
        activity = []
        for row in activity_rows:
            try:
                details = json.loads(row["details_json"]) if row["details_json"] else {}
            except (TypeError, ValueError, json.JSONDecodeError):
                details = {}
            activity.append(
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
        latest_queue = self.get_latest_research_item_dashboard()
        return {
            "counts": counts,
            "catalog": catalog,
            "candidate_stages": {str(row["stage"]): int(row["n"]) for row in candidate_rows},
            "queue_statuses": {str(row["status"]): int(row["n"]) for row in queue_rows},
            "bootstrap_statuses": {str(row["status"]): int(row["n"]) for row in bootstrap_rows},
            "workers": workers,
            "latest_queue_item": latest_queue,
            "latest_activity": activity,
            "logical_rows": {
                "catalog": sum(value["rows"] for value in catalog.values()),
                "bars": counts["bars"],
                "polymarket_snapshots": counts["polymarket_snapshots"],
                "paper_observations": counts["paper_observations"],
            },
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
def _dashboard_bound_value(
    value: Any,
    *,
    depth: int = 0,
    max_depth: int = _DASHBOARD_PAYLOAD_MAX_DEPTH,
    max_items: int = _DASHBOARD_PAYLOAD_MAX_ITEMS,
    max_string: int = _DASHBOARD_PAYLOAD_MAX_STRING,
) -> tuple[Any, bool]:
    """Bound one decoded dashboard value without mutating the stored object."""
    if isinstance(value, str):
        if len(value) <= max_string:
            return value, False
        return value[: max(0, max_string - 3)] + "...", True
    if depth >= max_depth:
        return "<truncated>", True
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        truncated = len(value) > max_items
        for index, (key, child) in enumerate(value.items()):
            if index >= max_items:
                break
            bounded, child_truncated = _dashboard_bound_value(
                child,
                depth=depth + 1,
                max_depth=max_depth,
                max_items=max_items,
                max_string=max_string,
            )
            result[str(key)] = bounded
            truncated = truncated or child_truncated
        return result, truncated
    if isinstance(value, (list, tuple, set, frozenset)):
        source = list(value)
        result = []
        truncated = len(source) > max_items
        for child in source[:max_items]:
            bounded, child_truncated = _dashboard_bound_value(
                child,
                depth=depth + 1,
                max_depth=max_depth,
                max_items=max_items,
                max_string=max_string,
            )
            result.append(bounded)
            truncated = truncated or child_truncated
        return result, truncated
    return value, False


def _dashboard_payload_projection(
    raw_json: Any,
    *,
    known: Mapping[str, Any] | None = None,
    key_count: int | None = None,
) -> tuple[Any, dict[str, Any]]:
    """Decode a persisted payload only when it fits the dashboard budget.

    The digest and byte count describe the exact stored JSON.  Oversized rows
    retain only scalar fields selected by the SQL read boundary, while normal
    rows are recursively bounded so nested diagnostics cannot amplify a page.
    """
    raw = str(raw_json or "")
    encoded = raw.encode("utf-8")
    metadata: dict[str, Any] = {
        "sha256": hashlib.sha256(encoded).hexdigest(),
        "bytes": len(encoded),
        "key_count": key_count,
        "truncated": False,
    }
    decoded: Any = None
    if len(encoded) <= _DASHBOARD_PAYLOAD_MAX_BYTES:
        try:
            decoded = _load(raw)
        except (TypeError, ValueError, json.JSONDecodeError):
            decoded = None
    if isinstance(decoded, Mapping):
        metadata["key_count"] = len(decoded) if key_count is None else key_count
        payload, value_truncated = _dashboard_bound_value(decoded)
        if isinstance(known, Mapping) and isinstance(payload, Mapping):
            for key, value in known.items():
                if value is not None and not isinstance(value, (Mapping, list, tuple, set, frozenset)):
                    payload.setdefault(str(key), value)
        metadata["item_count"] = len(decoded)
        metadata["truncated"] = bool(value_truncated)
        return payload, metadata
    if decoded is not None:
        payload, value_truncated = _dashboard_bound_value(decoded)
        metadata["item_count"] = len(decoded) if isinstance(decoded, (list, tuple)) else 1
        metadata["truncated"] = bool(value_truncated)
        return payload, metadata
    metadata["truncated"] = bool(raw) or len(encoded) > _DASHBOARD_PAYLOAD_MAX_BYTES
    if isinstance(known, Mapping):
        payload = {
            str(key): value
            for key, value in known.items()
            if value is not None and not isinstance(value, (Mapping, list, tuple, set, frozenset))
        }
    else:
        payload = {}
    metadata["item_count"] = key_count
    return payload, metadata


def _dashboard_payload_fields(metadata: Mapping[str, Any], prefix: str = "payload") -> dict[str, Any]:
    """Expose stable truncation evidence beside dashboard payload fields."""
    return {
        f"{prefix}_sha256": metadata.get("sha256"),
        f"{prefix}_bytes": metadata.get("bytes", 0),
        f"{prefix}_key_count": metadata.get("key_count"),
        f"{prefix}_item_count": metadata.get("item_count"),
        f"{prefix}_truncated": bool(metadata.get("truncated")),
    }


def _paper_state_projection_sql() -> str:
    """Build a compact JSON projection for dashboard paper-state rows."""
    position_count = "(SELECT COUNT(*) FROM json_each(state_json,'$.portfolio.positions'))"
    positions = (
        "(SELECT COALESCE(json_group_object(key,json_object("
        "'symbol',json_extract(value,'$.symbol'),"
        "'quantity',json_extract(value,'$.quantity'),"
        "'average_price',json_extract(value,'$.average_price'),"
        "'realized_pnl',json_extract(value,'$.realized_pnl'),"
        "'unrealized_pnl',json_extract(value,'$.unrealized_pnl'),"
        "'market_type',json_extract(value,'$.market_type'),"
        "'outcome',json_extract(value,'$.outcome'))),'{}') "
        "FROM (SELECT key,value FROM json_each(state_json,'$.portfolio.positions') "
        "ORDER BY key LIMIT ?))"
    )
    return (
        "json_object("
        "'status',json_extract(state_json,'$.status'),"
        "'portfolio',json_object("
        "'equity',json_extract(state_json,'$.portfolio.equity'),"
        "'initial_cash',json_extract(state_json,'$.portfolio.initial_cash'),"
        f"'positions',json({positions}),"
        f"'position_count',{position_count},"
        f"'positions_returned',MIN({position_count},?),"
        f"'positions_truncated',CASE WHEN {position_count}>? THEN 1 ELSE 0 END),"
        "'equity',json_extract(state_json,'$.equity'),"
        "'initial_cash',json_extract(state_json,'$.initial_cash'),"
        "'forward_pnl',json_extract(state_json,'$.forward_pnl'),"
        "'forward_max_drawdown',json_extract(state_json,'$.forward_max_drawdown'),"
        "'fill_count',json_extract(state_json,'$.fill_count'),"
        "'risk',json_object('max_drawdown',json_extract(state_json,'$.risk.max_drawdown'))"
        ")"
    )


def _paper_state_projection_parameters(*values: Any) -> list[Any]:
    """Bind the position-limit placeholders used by the projection SQL."""
    return [_PAPER_POSITION_PROJECTION_LIMIT, _PAPER_POSITION_PROJECTION_LIMIT, _PAPER_POSITION_PROJECTION_LIMIT, *values]


def _dataset_metadata_projection(
    raw_json: Any,
    *,
    key_limit: int = _DATASET_METADATA_PROJECTION_LIMIT,
    key_count: int | None = None,
    known: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Return metadata provenance without recursively decoding an unbounded manifest."""
    raw = str(raw_json or "{}")
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    if len(raw) > 262_144:
        projection: dict[str, Any] = {
            "sha256": digest,
            "bytes": len(raw.encode("utf-8")),
            "key_count": key_count,
            "keys": [],
            "truncated": True,
        }
        for key, value in (known or {}).items():
            if value is not None and not isinstance(value, (Mapping, list, tuple)):
                bounded, _ = _dashboard_bound_value(value, depth=0)
                projection[str(key)] = bounded
        return projection
    try:
        decoded = _load(raw)
    except (TypeError, ValueError, json.JSONDecodeError):
        decoded = {}
    if not isinstance(decoded, Mapping):
        return {
            "sha256": digest,
            "bytes": len(raw.encode("utf-8")),
            "key_count": 0,
            "keys": [],
            "truncated": bool(raw),
        }
    keys = sorted(str(key) for key in decoded)[:key_limit]
    selected_keys = {
        "category",
        "historical_order_book_available",
        "instrument",
        "market_type",
        "provider",
        "research_quality",
        "source_type",
        "timeframe",
        "universe_version",
    }
    projection = {}
    for key, value in decoded.items():
        if str(key) in selected_keys and not isinstance(value, (Mapping, list, tuple)):
            bounded, _ = _dashboard_bound_value(value, depth=0)
            projection[str(key)] = bounded
    projection.update(
        {
            "sha256": digest,
            "bytes": len(raw.encode("utf-8")),
            "key_count": len(decoded),
            "keys": keys,
            "truncated": len(decoded) > key_limit or bool(set(str(key) for key in decoded) - set(projection)),
        }
    )
    return projection


def _dataset_missing_range_projection(raw_json: Any) -> tuple[Any, bool]:
    """Keep range identity fields while bounding arbitrary range payloads."""
    raw = str(raw_json or "")
    if len(raw) > 262_144:
        return {}, True
    try:
        decoded = _load(raw)
    except (TypeError, ValueError, json.JSONDecodeError):
        return ("<truncated>" if len(raw) > 4096 else raw), bool(len(raw) > 4096)
    if isinstance(decoded, Mapping):
        projected = {}
        for key in ("start", "end", "start_timestamp", "end_timestamp", "reason", "kind"):
            if key in decoded:
                bounded, _ = _dashboard_bound_value(decoded[key], depth=0)
                projected[key] = bounded
        if projected:
            return projected, len(_dump(decoded)) > len(_dump(projected))
    if isinstance(decoded, str):
        return ("<truncated>" if len(decoded) > 4096 else decoded), len(decoded) > 4096
    encoded = _dump(decoded)
    return ("<truncated>" if len(encoded) > 4096 else encoded), len(encoded) > 4096


def _worker_payload_projection(
    raw_json: Any,
    *,
    key_count: int | None = None,
    known: Mapping[str, Any] | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Keep worker liveness fields while bounding persisted diagnostics."""
    payload_value, projection = _dashboard_payload_projection(
        raw_json,
        key_count=key_count,
        known=known,
    )
    payload = dict(payload_value) if isinstance(payload_value, Mapping) else {}
    crypto_enabled = (known or {}).get("crypto_enabled")
    crypto_error = (known or {}).get("crypto_last_error")
    if (
        "crypto_paper" not in payload
        and (crypto_enabled is not None or crypto_error is not None)
    ):
        payload["crypto_paper"] = {
            key: value
            for key, value in (
                ("enabled", crypto_enabled),
                ("last_error", crypto_error),
            )
            if value is not None
        }
    payload["_projection"] = projection
    return payload, projection


def _dataset_catalog_dashboard_record(row: sqlite3.Row) -> dict[str, Any]:
    metadata = _dataset_metadata_projection(
        row["metadata_json"],
        key_count=int(row["metadata_key_count"] or 0),
        known={
            "category": row["metadata_category"],
            "historical_order_book_available": row["metadata_historical_order_book_available"],
            "universe_version": row["metadata_universe_version"],
        },
    )
    missing_raw = str(row["missing_ranges_json"] or "[]")
    missing_count = int(row["missing_range_count"] or 0)
    missing_ranges: list[Any] = []
    missing_truncated = False
    if len(missing_raw) <= 262_144:
        try:
            decoded = _load(missing_raw)
        except (TypeError, ValueError, json.JSONDecodeError):
            decoded = []
        if isinstance(decoded, list):
            for value in decoded[:_DATASET_MISSING_RANGE_PROJECTION_LIMIT]:
                projected, truncated = _dataset_missing_range_projection(_dump(value))
                missing_ranges.append(projected)
                missing_truncated = missing_truncated or truncated
            missing_truncated = missing_truncated or len(decoded) > _DATASET_MISSING_RANGE_PROJECTION_LIMIT
        else:
            missing_truncated = bool(missing_raw)
    else:
        missing_truncated = True
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
        "missing_ranges": missing_ranges,
        "missing_range_count": missing_count,
        "missing_ranges_returned": len(missing_ranges),
        "missing_ranges_truncated": missing_truncated,
        "quality": row["quality"],
        "source_type": row["source_type"],
        "snapshot_id": row["snapshot_id"],
        "created_at": _parse_datetime(row["created_at"]),
        "updated_at": _parse_datetime(row["updated_at"]),
        "last_updated": _parse_datetime(row["updated_at"]),
        "metadata": metadata,
        "metadata_truncated": bool(metadata.get("truncated")),
        "metadata_key_count": metadata.get("key_count"),
        "metadata_sha256": metadata.get("sha256"),
    }


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
def _rolling_enrollment_record(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    provenance = _load(row["provenance_json"])
    return {
        "enrollment_id": row["enrollment_id"],
        "candidate_id": row["candidate_id"],
        "source_candidate_id": row["candidate_id"],
        "strategy_version_id": row["strategy_version_id"],
        "research_trial_id": row["research_trial_id"],
        "status": row["status"],
        "reason": row["reason"],
        "validation_version": row["validation_version"],
        "predecessor_enrollment_id": row["predecessor_enrollment_id"],
        "provenance": provenance if isinstance(provenance, Mapping) else {},
        "created_at": _parse_datetime(row["created_at"]),
    }

def _rolling_mapping(record: Any, *, name: str) -> dict[str, Any]:
    if isinstance(record, Mapping):
        return {str(key): value for key, value in record.items()}
    if is_dataclass(record):
        return {
            str(field): getattr(record, field)
            for field in getattr(record, "__dataclass_fields__", {})
        }
    raise TypeError(f"{name} must be a mapping or dataclass")


def _rolling_hash(value: Any) -> str:
    """Hash rolling payloads using the canonical bounded JSON representation."""
    return "sha256:" + hashlib.sha256(
        _rolling_dump(value).encode("utf-8")
    ).hexdigest()

_ROLLING_V2_ACCOUNTING_FIELDS = (
    "initial_cash",
    "cash",
    "equity",
    "realized_pnl",
    "unrealized_pnl",
    "net_pnl",
    "fees",
    "costs",
    "open_positions",
    "opening_fills",
    "closing_fills",
    "partial_closing_fills",
    "completed_round_trips",
)
_ROLLING_V2_ACCOUNTING_MONETARY_FIELDS = (
    "initial_cash",
    "cash",
    "equity",
    "realized_pnl",
    "unrealized_pnl",
    "net_pnl",
    "fees",
    "costs",
)
_ROLLING_V2_ACCOUNTING_ACTIVITY_COUNT_FIELDS = (
    "opening_fills",
    "closing_fills",
    "partial_closing_fills",
    "completed_round_trips",
)
_ROLLING_ACCOUNTING_ALIASES: dict[str, tuple[str, ...]] = {
    "initial_cash": ("initial_cash_usd",),
    "cash": ("cash_usd",),
    "equity": ("equity_usd",),
    "realized_pnl": ("realized_pnl_usd",),
    "unrealized_pnl": ("unrealized_pnl_usd",),
    "net_pnl": ("net_pnl_usd",),
    "fees": ("fees_usd",),
    "costs": ("costs_usd",),
    "allocated_capital_net_return": (
        "allocated_capital_net_return_usd",
        "net_return",
    ),
    "requested_days": ("requested_window_days",),
}
_ROLLING_V2_ACCOUNTING_REDACTED_FIELDS = _ROLLING_V2_ACCOUNTING_MONETARY_FIELDS
_ROLLING_V2_ACCOUNTING_ROOT_REDACTED_FIELDS = (
    *_ROLLING_V2_ACCOUNTING_REDACTED_FIELDS,
    "allocated_capital_net_return",
)


def _rolling_v2_has_unredacted_monetary(value: Any) -> bool:
    if not isinstance(value, Mapping):
        return False
    for field_name in _ROLLING_V2_ACCOUNTING_ROOT_REDACTED_FIELDS:
        for key in (field_name, *_ROLLING_ACCOUNTING_ALIASES.get(field_name, ())):
            if key not in value or value[key] is None:
                continue
            try:
                normalized = _rolling_nullable_decimal_text(
                    value[key],
                    name=field_name,
                )
            except ValueError:
                return True
            if normalized is not None:
                return True
    return False


def _rolling_v2_unavailable_accounting(value: Mapping[str, Any]) -> dict[str, Any]:
    document = dict(value)
    for field_name in _ROLLING_V2_ACCOUNTING_REDACTED_FIELDS:
        document[field_name] = None
        for alias in _ROLLING_ACCOUNTING_ALIASES.get(field_name, ()):
            document.pop(alias, None)
    allocated_names = (
        "allocated_capital_net_return",
        *_ROLLING_ACCOUNTING_ALIASES["allocated_capital_net_return"],
    )
    if any(name in document for name in allocated_names):
        document["allocated_capital_net_return"] = None
        for alias in allocated_names[1:]:
            document.pop(alias, None)
    return document


_ROLLING_NUMERIC_ALIAS_FIELDS = frozenset(
    field_name
    for field_name, aliases in _ROLLING_ACCOUNTING_ALIASES.items()
    for field_name in (field_name, *aliases)
)
_ROLLING_OPEN_POSITIONS_LIMIT = _PAPER_POSITION_PROJECTION_LIMIT


def _rolling_numeric_values_equal(left: Any, right: Any) -> bool:
    if left is None or right is None:
        return left is right
    if isinstance(left, bool) or isinstance(right, bool):
        return left == right
    try:
        left_decimal = Decimal(str(left))
        right_decimal = Decimal(str(right))
    except (InvalidOperation, TypeError, ValueError, OverflowError):
        return _rolling_payload_equal(left, right, ignored=frozenset())
    if not left_decimal.is_finite() or not right_decimal.is_finite():
        return False
    return left_decimal == right_decimal


def _rolling_validate_v2_activity(value: Any) -> None:
    if not isinstance(value, Mapping):
        return
    positions = value.get("open_positions")
    if positions is not None:
        if not isinstance(positions, list):
            raise ValueError("open_positions must be a bounded list")
        if len(positions) > _ROLLING_OPEN_POSITIONS_LIMIT:
            raise ValueError(
                f"open_positions exceeds {_ROLLING_OPEN_POSITIONS_LIMIT} entries"
            )
    for field_name in _ROLLING_V2_ACCOUNTING_ACTIVITY_COUNT_FIELDS:
        if field_name in value and value[field_name] is not None:
            _rolling_nonnegative_integer(value[field_name], name=field_name)


def _rolling_v2_accounting_fields_usable(value: Any) -> bool:
    if not isinstance(value, Mapping):
        return False
    if any(
        field_name not in value or value[field_name] is None
        for field_name in _ROLLING_V2_ACCOUNTING_FIELDS
    ):
        return False
    try:
        _rolling_validate_v2_activity(value)
    except ValueError:
        return False
    for field_name in _ROLLING_V2_ACCOUNTING_MONETARY_FIELDS:
        item = value.get(field_name)
        if isinstance(item, bool):
            return False
        try:
            parsed = Decimal(str(item))
        except (InvalidOperation, TypeError, ValueError, OverflowError):
            return False
        if not parsed.is_finite():
            return False
    completed = value.get("completed_round_trips")
    if isinstance(completed, bool):
        return False
    try:
        completed_number = Decimal(str(completed))
    except (InvalidOperation, TypeError, ValueError, OverflowError):
        return False
    if (
        not completed_number.is_finite()
        or completed_number < 0
        or completed_number != completed_number.to_integral_value()
    ):
        return False
    return True


def _rolling_evidence_digest(record: Mapping[str, Any]) -> str:
    """Compute the rolling model's canonical evidence digest."""
    from .rolling_portfolio import RollingEvidence

    payload = dict(record)
    payload.pop("evidence_digest", None)
    payload.pop("digest", None)
    return str(RollingEvidence.from_mapping(payload).evidence_digest)


def _rolling_evidence_mapping_from_row(
    row: sqlite3.Row,
    payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    document = dict(payload or {})
    for field_name in ("evaluation_json", "portfolio_accounting_json"):
        raw = row[field_name] if field_name in row.keys() else None
        if raw:
            try:
                decoded = _load(raw)
            except (TypeError, ValueError, json.JSONDecodeError):
                decoded = {}
            if isinstance(decoded, Mapping):
                document[
                    "evaluation"
                    if field_name == "evaluation_json"
                    else "portfolio_accounting"
                ] = dict(decoded)
    metrics = document.get("metrics")
    metrics = metrics if isinstance(metrics, Mapping) else {}
    evaluation_candidate = document.get("evaluation")
    if not isinstance(evaluation_candidate, Mapping) or not evaluation_candidate:
        evaluation_candidate = metrics.get("evaluation")
    evaluation = (
        dict(evaluation_candidate)
        if isinstance(evaluation_candidate, Mapping)
        else {}
    )
    if _rolling_contract_present(document, evaluation, "evaluation_kind"):
        evaluation["evaluation_kind"] = _rolling_contract_value(
            document,
            evaluation,
            "evaluation_kind",
            strict_null_conflict=True,
        )
    portfolio_candidate = document.get("portfolio_accounting")
    if not isinstance(portfolio_candidate, Mapping) or not portfolio_candidate:
        portfolio_candidate = metrics.get("portfolio_accounting")
    portfolio = (
        dict(portfolio_candidate)
        if isinstance(portfolio_candidate, Mapping)
        else {}
    )
    for field_name in (
        "accounting_available",
        "accounting_complete",
        "accounting_partial",
    ):
        if _rolling_contract_present(document, portfolio, field_name):
            _rolling_contract_value(
                document,
                portfolio,
                field_name,
                strict_null_conflict=True,
            )
    for field_name in ("evaluator_invoked", "evaluator_completed"):
        if _rolling_contract_present(document, evaluation, field_name):
            _rolling_contract_value(
                document,
                evaluation,
                field_name,
                strict_null_conflict=True,
            )
    accounting_available = row["accounting_available"] if "accounting_available" in row.keys() else None
    if accounting_available is None:
        accounting_available = portfolio.get("accounting_available")
    if accounting_available is None:
        accounting_available = document.get("accounting_available")
    if accounting_available is not None:
        accounting_available = bool(accounting_available)
    v2_provenance = any(
        field_name in document
        and document[field_name] not in (None, "")
        for field_name in (
            "evaluation_kind",
            "evaluation_run_id",
            "evaluation_version",
            "supersedes_evidence_id",
        )
    )
    v2_provenance = v2_provenance or any(
        row[field_name] not in (None, "")
        for field_name in (
            "evaluation_run_id",
            "evaluation_version",
            "supersedes_evidence_id",
        )
        if field_name in row.keys()
    )
    if v2_provenance:
        evaluation["evaluation_kind"] = _rolling_evaluation_kind(
            evaluation.get("evaluation_kind"),
            default="CANONICAL_SIMULATION",
        )
    accounting_complete = document.get("accounting_complete")
    if accounting_complete is None:
        accounting_complete = portfolio.get("accounting_complete")
    accounting_partial = document.get("accounting_partial")
    if accounting_partial is None:
        accounting_partial = portfolio.get("accounting_partial")
    evaluator_invoked = evaluation.get("evaluator_invoked")
    if evaluator_invoked is None and "evaluator_invoked" in row.keys():
        row_value = row["evaluator_invoked"]
        evaluator_invoked = None if row_value is None else bool(row_value)
    if evaluator_invoked is None:
        evaluator_invoked = document.get("evaluator_invoked")
    evaluator_completed = evaluation.get("evaluator_completed")
    if evaluator_completed is None and "evaluator_completed" in row.keys():
        row_value = row["evaluator_completed"]
        evaluator_completed = None if row_value is None else bool(row_value)
    if evaluator_completed is None:
        evaluator_completed = document.get("evaluator_completed")
    evaluator_ready = (
        (
            evaluator_invoked is False
            and evaluator_completed is False
        )
        if evaluation.get("evaluation_kind") == "ACTUAL_LEDGER"
        else (
            evaluator_invoked is True
            and evaluator_completed is True
        )
    )
    accounting_ready = v2_provenance and (
        accounting_available is True
        and accounting_complete is True
        and accounting_partial is False
        and evaluator_ready
        and _rolling_v2_accounting_fields_usable(portfolio)
    )
    if v2_provenance and not accounting_ready:
        accounting_available = False
        accounting_complete = False
        accounting_partial = True

    def stored_metric(field_name: str) -> Any:
        if not v2_provenance:
            return row[field_name]
        if not accounting_ready:
            return None
        if field_name in portfolio:
            return portfolio[field_name]
        if field_name in document:
            return document[field_name]
        return None

    document.update(
        {
            "strategy_version_id": row["strategy_version_id"],
            "evidence_window_id": row["evidence_window_id"],
            "research_trial_id": row["research_trial_id"],
            "candidate_id": row["candidate_id"],
            "available_from": row["available_from"],
            "available_through": row["available_through"],
            "requested_days": row["requested_days"],
            "actual_coverage_seconds": row["actual_coverage_seconds"],
            "observation_completeness": row["observation_completeness"],
            "source_class": row["source_class"],
            "paper_sizing_assumptions": _load(row["paper_sizing_assumptions_json"]),
            "paper_fee_assumptions": _load(row["paper_fee_assumptions_json"]),
            "paper_slippage_assumptions": _load(row["paper_slippage_assumptions_json"]),
            "allocated_capital_net_return": (
                stored_metric("allocated_capital_net_return")
                if v2_provenance
                else row["allocated_capital_net_return"]
            ),
            "realized_pnl": stored_metric("realized_pnl"),
            "unrealized_pnl": stored_metric("unrealized_pnl"),
            "fees": stored_metric("fees"),
            "costs": stored_metric("costs"),
            "drawdown": row["drawdown"],
            "completed_outcomes": row["completed_outcomes"],
            "reliability": row["reliability"],
            "execution_feasibility": row["execution_feasibility"],
            "overlap_key": document.get("overlap_key"),
        }
    )
    for field_name in (
        "evaluation_kind",
        "evaluation_run_id",
        "evaluation_version",
        "supersedes_evidence_id",
        "loaded_rows",
        "valid_input_rows",
        "evaluator_invoked",
        "evaluator_completed",
        "evaluated_observations",
        "signal_count",
        "diagnostic_summary_count",
        "evaluator_name",
        "evaluator_error",
        "evaluator_prerequisite",
    ):
        value = row[field_name] if field_name in row.keys() else None
        if value is None:
            value = evaluation.get(field_name)
        if value is not None:
            document[field_name] = value
            evaluation = dict(evaluation)
            evaluation[field_name] = value
    if evaluation:
        document["evaluation"] = evaluation
    if portfolio:
        accounting_document = dict(portfolio)
        if v2_provenance and not accounting_ready:
            for field_name in (
                "initial_cash",
                "cash",
                "equity",
                "realized_pnl",
                "unrealized_pnl",
                "net_pnl",
                "fees",
                "costs",
            ):
                if field_name in accounting_document:
                    accounting_document[field_name] = None
        document["portfolio_accounting"] = accounting_document
        for field_name, value in accounting_document.items():
            document[field_name] = value
    if accounting_available is not None:
        document["accounting_available"] = accounting_available
        if isinstance(document.get("portfolio_accounting"), Mapping):
            document["portfolio_accounting"] = dict(document["portfolio_accounting"])
            document["portfolio_accounting"]["accounting_available"] = accounting_available
            if v2_provenance and not accounting_ready:
                document["portfolio_accounting"]["accounting_complete"] = False
                document["portfolio_accounting"]["accounting_partial"] = True
    if v2_provenance and not accounting_ready:
        document["accounting_complete"] = False
        document["accounting_partial"] = True
    if v2_provenance and not accounting_ready:
        for field_name in (
            "allocated_capital_net_return",
            "realized_pnl",
            "unrealized_pnl",
            "net_pnl",
            "fees",
            "costs",
        ):
            document[field_name] = None
    elif not v2_provenance and accounting_available is not True and (
        "net_pnl" in portfolio
    ):
        document["allocated_capital_net_return"] = portfolio.get("net_pnl")
    return document


def _rolling_identity_value(payload: Any, *names: str) -> str | None:
    """Return one unambiguous immutable identity from nested rolling payloads."""
    found: set[str] = set()

    def visit(value: Any, depth: int = 0) -> None:
        if depth > 4 or not isinstance(value, Mapping):
            return
        for name in names:
            item = value.get(name)
            if item not in (None, ""):
                text = str(item).strip()
                if text:
                    found.add(text)
        for key in (
            "payload",
            "evaluation",
            "portfolio_accounting",
            "metrics",
            "policy",
            "minimum_evidence",
            "minimum",
            "provenance",
            "binding",
            "lineage",
            "source",
        ):
            child = value.get(key)
            if isinstance(child, Mapping):
                visit(child, depth + 1)

    visit(payload)
    return next(iter(found)) if len(found) == 1 else None

def _rolling_identity_conflict(payload: Any, *names: str) -> bool:
    """Detect conflicting copies of a rolling identity in nested payloads."""
    found: set[str] = set()

    def visit(value: Any, depth: int = 0) -> None:
        if depth > 4 or not isinstance(value, Mapping):
            return
        for name in names:
            item = value.get(name)
            if item not in (None, ""):
                text = str(item).strip()
                if text:
                    found.add(text)
        for key in (
            "payload",
            "evaluation",
            "portfolio_accounting",
            "metrics",
            "policy",
            "minimum_evidence",
            "minimum",
            "provenance",
            "binding",
            "lineage",
            "source",
        ):
            child = value.get(key)
            if isinstance(child, Mapping):
                visit(child, depth + 1)

    visit(payload)
    return len(found) > 1

def _rolling_identity_raw_values(payload: Any, *names: str) -> list[Any]:
    """Collect immutable identity values without stringifying sequences."""
    found: list[Any] = []

    def visit(value: Any, depth: int = 0) -> None:
        if depth > 4 or not isinstance(value, Mapping):
            return
        for name in names:
            item = value.get(name)
            if item not in (None, ""):
                found.append(item)
        for key in (
            "payload",
            "evaluation",
            "portfolio_accounting",
            "metrics",
            "policy",
            "minimum_evidence",
            "minimum",
            "provenance",
            "binding",
            "lineage",
            "source",
        ):
            child = value.get(key)
            if isinstance(child, Mapping):
                visit(child, depth + 1)

    visit(payload)
    return found



def _rolling_jsonable(value: Any) -> Any:
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("rolling portfolio decimals must be finite")
        return format(value, "f")
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        return _iso(value)
    if is_dataclass(value):
        return {
            str(name): _rolling_jsonable(getattr(value, name))
            for name in getattr(value, "__dataclass_fields__", {})
        }
    if isinstance(value, Mapping):
        return {str(key): _rolling_jsonable(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_rolling_jsonable(item) for item in value]
    if isinstance(value, set):
        return sorted(_rolling_jsonable(item) for item in value)
    return value


def _rolling_dump(value: Any) -> str:
    return json.dumps(_rolling_jsonable(value), sort_keys=True, separators=(",", ":"), allow_nan=False)


def _rolling_decimal(
    value: Any,
    *,
    name: str,
    nonnegative: bool = False,
) -> Decimal:
    return _risk_decimal(value, name=name, nonnegative=nonnegative)

def _rolling_decimal_text(value: Any, *, name: str, nonnegative: bool = False) -> str:
    return format(_rolling_decimal(value, name=name, nonnegative=nonnegative), "f")


def _rolling_nullable_decimal_text(value: Any, *, name: str) -> str | None:
    if value is None or (
        isinstance(value, str)
        and value.strip().lower() in {"none", "null", "unavailable", "unknown", "n/a"}
    ):
        return None
    return _rolling_decimal_text(value, name=name)


def _rolling_required_text(data: Mapping[str, Any], *names: str, name: str) -> str:
    for field in names:
        value = data.get(field)
        normalized = _enum_value(value) if value is not None else None
        if normalized is not None and str(normalized).strip():
            return str(normalized).strip()
    raise ValueError(f"{name} is required")


def _rolling_optional_text(data: Mapping[str, Any], *names: str, default: str = "") -> str:
    for field in names:
        value = data.get(field)
        if value is not None:
            normalized = _enum_value(value)
            return str(normalized if normalized is not None else value).strip()
    return default
def _rolling_evaluation_kind(value: Any, *, default: str | None = None) -> str | None:
    if value is None or value == "":
        return default
    normalized = _enum_value(value)
    kind = str(normalized if normalized is not None else value).strip().upper()
    if kind not in _ROLLING_EVALUATION_KINDS:
        raise ValueError("evaluation_kind must be CANONICAL_SIMULATION or ACTUAL_LEDGER")
    return kind


def _rolling_timestamp(
    value: Any,
    *,
    name: str,
    required: bool = False,
    default_now: bool = False,
) -> str | None:
    if value is None:
        if default_now:
            return _now_iso()
        if required:
            raise ValueError(f"{name} is required")
        return None
    parsed = _parse_datetime(value)
    if parsed is None:
        raise ValueError(f"{name} must be a valid timestamp")
    return _iso(parsed)


def _rolling_nonnegative_integer(value: Any, *, name: str, default: int | None = None) -> int:
    if value is None and default is not None:
        return int(default)
    if isinstance(value, bool):
        raise ValueError(f"{name} must be a non-negative integer")
    try:
        number = _rolling_decimal(value, name=name, nonnegative=True)
    except ValueError as exc:
        raise ValueError(f"{name} must be a non-negative integer") from exc
    if number != number.to_integral_value():
        raise ValueError(f"{name} must be a non-negative integer")
    return int(number)


def _rolling_optional_boolean(value: Any, *, name: str) -> bool | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and value in (0, 1):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().upper()
        if normalized in {"TRUE", "1", "YES"}:
            return True
        if normalized in {"FALSE", "0", "NO"}:
            return False
    raise ValueError(f"{name} must be a boolean or null")


def _rolling_json_mapping(value: Any, *, name: str) -> dict[str, Any]:
    if value in (None, ""):
        return {}
    if isinstance(value, Mapping):
        return {str(key): item for key, item in value.items()}
    if isinstance(value, str):
        try:
            decoded = _load(value)
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise ValueError(f"{name} must be a JSON object") from exc
        if isinstance(decoded, Mapping):
            return {str(key): item for key, item in decoded.items()}
    raise ValueError(f"{name} must be a mapping")


def _rolling_projection_mapping(
    data: Mapping[str, Any],
    projection_name: str,
) -> dict[str, Any]:
    """Merge canonical and metrics projections without dropping either one."""
    metrics = data.get("metrics")
    metrics = metrics if isinstance(metrics, Mapping) else {}
    canonical_present = projection_name in data
    metrics_present = projection_name in metrics
    canonical_raw = data.get(projection_name)
    metrics_raw = metrics.get(projection_name)
    if canonical_present and metrics_present:
        if (canonical_raw is None) != (metrics_raw is None):
            raise ValueError(f"{projection_name} projections conflict")
    canonical = _rolling_json_mapping(canonical_raw, name=projection_name)
    metric_projection = _rolling_json_mapping(metrics_raw, name=f"metrics.{projection_name}")
    merged = dict(canonical)
    for field_name, value in metric_projection.items():
        if field_name in merged:
            values_equal = (
                _rolling_numeric_values_equal(merged[field_name], value)
                if field_name in _ROLLING_NUMERIC_ALIAS_FIELDS
                else _rolling_payload_equal(
                    merged[field_name],
                    value,
                    ignored=frozenset(),
                )
            )
            if not values_equal:
                raise ValueError(
                    f"{projection_name} projections conflict for {field_name}"
                )
        merged[field_name] = value
    return merged


def _rolling_contract_present(
    data: Mapping[str, Any],
    nested: Mapping[str, Any],
    name: str,
    *,
    aliases: tuple[str, ...] = (),
) -> bool:
    keys = (name, *aliases)
    return any(key in source for source in (data, nested) for key in keys)

def _rolling_contract_value(
    data: Mapping[str, Any],
    nested: Mapping[str, Any],
    name: str,
    *,
    aliases: tuple[str, ...] = (),
    strict_null_conflict: bool = False,
) -> Any:
    values: list[Any] = []
    keys = (name, *aliases)
    for source in (data, nested):
        for key in keys:
            if key in source:
                values.append(source.get(key))
    if any(value is None for value in values):
        if strict_null_conflict and any(value is not None for value in values):
            raise ValueError(f"{name} conflicts between evidence and nested metrics")
        # A present null is an explicit unavailable value.  It must remain
        # authoritative instead of being resurrected by a legacy scalar.
        return None
    if len(values) > 1:
        equal = (
            all(_rolling_numeric_values_equal(values[0], value) for value in values[1:])
            if name in _ROLLING_NUMERIC_ALIAS_FIELDS
            else all(
                _rolling_payload_equal(values[0], value, ignored=frozenset())
                for value in values[1:]
            )
        )
        if not equal:
            raise ValueError(f"{name} conflicts between evidence and nested metrics")
    return values[0] if values else None


_ROLLING_HYDRATION_MISSING = object()


def _rolling_reconcile_hydrated_field(
    field_name: str,
    projections: Sequence[tuple[str, Mapping[str, Any]]],
    *,
    sql_value: Any = _ROLLING_HYDRATION_MISSING,
    normalize: Callable[[Any], Any] | None = None,
    v2_provenance: bool = False,
) -> tuple[bool, Any]:
    """Reconcile JSON projections before applying SQL compatibility columns.

    A missing legacy SQL column value is tolerated only when the row has no v2
    discriminator.  Explicit nulls in JSON remain meaningful and therefore
    conflict with a non-null projection, rather than being overwritten.
    """
    candidates: list[tuple[str, Any]] = []
    for source_name, projection in projections:
        if field_name in projection:
            candidates.append((source_name, projection[field_name]))
    if sql_value is not _ROLLING_HYDRATION_MISSING:
        if sql_value is not None or v2_provenance:
            candidates.append(("SQL", sql_value))
    if not candidates:
        return False, None
    non_null = [(source, value) for source, value in candidates if value is not None]
    if non_null and len(non_null) != len(candidates):
        raise ValueError(
            f"{field_name} conflicts between SQL and hydrated JSON projections"
        )
    if normalize is None:
        normalized = [(source, value) for source, value in non_null]
    else:
        normalized = [(source, normalize(value)) for source, value in non_null]
    if normalized:
        expected = normalized[0][1]
        if any(value != expected for _, value in normalized[1:]):
            raise ValueError(
                f"{field_name} conflicts between SQL and hydrated JSON projections"
            )
        return True, expected
    return True, None


def _rolling_payload_equal(left: Any, right: Any, *, ignored: frozenset[str]) -> bool:
    def scrub(value: Any) -> Any:
        if isinstance(value, Mapping):
            return {
                str(key): scrub(item)
                for key, item in value.items()
                if str(key) not in ignored
            }
        if isinstance(value, (tuple, list)):
            return [scrub(item) for item in value]
        return value

    try:
        return _rolling_dump(scrub(left)) == _rolling_dump(scrub(right))
    except (TypeError, ValueError):
        return False

_ROLLING_V2_HYDRATED_PROJECTION_FIELDS = frozenset(
    {
        "initial_cash",
        "cash",
        "equity",
        "net_pnl",
        "open_positions",
        "opening_fills",
        "closing_fills",
        "partial_closing_fills",
        "completed_round_trips",
        "evaluation_run_id",
        "evaluation_version",
        "supersedes_evidence_id",
    }
)


def _rolling_payload_for_identity(
    value: Any,
    *,
    v2_provenance: bool,
) -> Any:
    """Remove only list hydration projections from a v2 payload.

    ``list_strategy_evidence_windows`` exposes nested accounting fields at the
    root for compatibility.  They are validated against the nested canonical
    document before this projection is removed, so a mutation still fails
    closed while a hydrated row can be compared with its persisted payload.
    """
    if not v2_provenance or not isinstance(value, Mapping):
        return value
    document = dict(value)
    evaluation = document.get("evaluation")
    if isinstance(evaluation, Mapping):
        evaluation = dict(evaluation)
        for field_name in (
            "evaluation_run_id",
            "evaluation_version",
            "supersedes_evidence_id",
        ):
            if evaluation.get(field_name) is None:
                evaluation.pop(field_name, None)
        document["evaluation"] = evaluation
    return {
        key: item
        for key, item in document.items()
        if str(key) not in _ROLLING_V2_HYDRATED_PROJECTION_FIELDS
    }


def _rolling_optional_document_equal(
    left: Any,
    right: Any,
    *,
    nullable_fields: frozenset[str],
) -> bool:
    """Compare JSON documents while equating omitted optional null fields."""
    if left is None or right is None:
        return left is right
    try:
        left_document = _load(left) if isinstance(left, str) else left
        right_document = _load(right) if isinstance(right, str) else right
    except (TypeError, ValueError, json.JSONDecodeError):
        return False
    if not isinstance(left_document, Mapping) or not isinstance(right_document, Mapping):
        return False
    left_document = dict(left_document)
    right_document = dict(right_document)
    for field_name in nullable_fields:
        if left_document.get(field_name) is None and right_document.get(field_name) is None:
            left_document.pop(field_name, None)
            right_document.pop(field_name, None)
    return _rolling_payload_equal(
        left_document,
        right_document,
        ignored=frozenset(),
    )

def _rolling_limit(limit: Any, *, default: int = 100) -> int:
    value = default if limit is None else limit
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError("limit must be a non-negative integer")
    return int(value)



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

_CANARY_LINEAGE_FIELDS = (
    "candidate_id",
    "strategy_version_id",
    "research_trial_id",
    "portfolio_selection_id",
    "admission_policy_id",
    "admission_policy_version",
    "risk_config_id",
    "risk_config_generation",
    "risk_config_hash",
    "allocation",
)

def _canary_optional_lineage_text(value: Any, *, name: str) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _canary_optional_lineage_generation(value: Any, *, name: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError(f"{name} must be an integer")
    number = _risk_decimal(value, name=name, nonnegative=True)
    if number != number.to_integral_value():
        raise ValueError(f"{name} must be an integer")
    return int(number)


def _canary_lineage_from_row(row: sqlite3.Row | None) -> dict[str, Any]:
    if row is None:
        return {name: None for name in _CANARY_LINEAGE_FIELDS}
    values: dict[str, Any] = {}
    for name in _CANARY_LINEAGE_FIELDS:
        value = row[name] if name in row.keys() else None
        if name == "risk_config_generation":
            values[name] = (
                int(value) if value is not None else None
            )
        elif name == "allocation":
            values[name] = (
                _risk_text(_risk_decimal(value, name="allocation", nonnegative=True))
                if value not in (None, "")
                else None
            )
        else:
            values[name] = str(value).strip() if value not in (None, "") else None
    return values


def _canary_lineage_equal(left: Mapping[str, Any], right: Mapping[str, Any]) -> bool:
    for name in _CANARY_LINEAGE_FIELDS:
        left_value = left.get(name)
        right_value = right.get(name)
        if name == "risk_config_generation":
            if (
                _canary_optional_lineage_generation(left_value, name=name)
                != _canary_optional_lineage_generation(right_value, name=name)
            ):
                return False
        elif name == "allocation":
            if left_value in (None, "") or right_value in (None, ""):
                if (left_value in (None, "")) != (right_value in (None, "")):
                    return False
            elif _risk_decimal(left_value, name=name, nonnegative=True) != _risk_decimal(
                right_value, name=name, nonnegative=True
            ):
                return False
        elif _canary_optional_lineage_text(left_value, name=name) != _canary_optional_lineage_text(
            right_value, name=name
        ):
            return False
    return True


def _canary_lineage_is_rolling(lineage: Mapping[str, Any]) -> bool:
    """Identify rolling rows from mandatory lineage, not optional candidates.

    Legacy reservations may carry a candidate label copied from an older
    canary ledger.  That label is descriptive only; candidate/trial/evidence
    enforcement applies when the rolling portfolio lineage is present.
    """
    return any(
        lineage.get(name) not in (None, "")
        for name in _CANARY_LINEAGE_FIELDS[1:-1]
    )
def _canary_lineage_subset_equal(
    supplied: Mapping[str, Any],
    stored: Mapping[str, Any],
) -> bool:
    """Match supplied lineage while keeping legacy candidate labels optional."""
    rolling = _canary_lineage_is_rolling(supplied) or _canary_lineage_is_rolling(stored)
    for name in _CANARY_LINEAGE_FIELDS:
        value = supplied.get(name)
        if value in (None, ""):
            continue
        if name == "candidate_id" and not rolling:
            # Candidate labels are descriptive on legacy rows.  They must not
            # turn a partial legacy projection into an identity conflict.
            continue
        if name == "candidate_id":
            if _canary_optional_lineage_text(value, name=name) != _canary_optional_lineage_text(
                stored.get(name), name=name
            ):
                return False
        elif name == "allocation":
            if stored.get(name) in (None, ""):
                return False
            if _risk_decimal(value, name=name, nonnegative=True) != _risk_decimal(
                stored.get(name), name=name, nonnegative=True
            ):
                return False
        elif name == "risk_config_generation":
            if _canary_optional_lineage_generation(value, name=name) != _canary_optional_lineage_generation(
                stored.get(name), name=name
            ):
                return False
        elif _canary_optional_lineage_text(value, name=name) != _canary_optional_lineage_text(
            stored.get(name), name=name
        ):
            return False
    return True

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


def _risk_decimal(value: Any = "0", *, name: str = "value", nonnegative: bool = False) -> Decimal:
    if isinstance(value, bool) or value is None:
        raise ValueError(f"{name} must be a finite decimal")
    if isinstance(value, float):
        raise ValueError(f"{name} must be an exact decimal")
    try:
        number = value if isinstance(value, Decimal) else Decimal(str(value).strip())
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a finite decimal") from exc
    if not number.is_finite() or (nonnegative and number < 0):
        raise ValueError(f"{name} must be finite and non-negative")
    return number


def _risk_text(value: Any) -> str:
    return format(_risk_decimal(value), "f")


def _monotonic_canary_reservation_status(current: Any, proposed: Any) -> str:
    """Keep terminal reservation outcomes from being downgraded."""
    prior = str(current or "").strip().upper()
    next_status = str(proposed or "").strip().upper()
    if prior == "SETTLED" and next_status != prior:
        return prior
    if prior == "FILLED" and next_status not in {"FILLED", "SETTLED"}:
        return prior
    if prior in {"CANCELED", "CANCELLED", "REJECTED"} and next_status != prior:
        return prior
    return next_status


def _canary_reservation_terminal(current: Any) -> bool:
    return str(current or "").strip().upper() in _CANARY_RESERVATION_TERMINAL_STATUSES




def _canary_fill_settlement_status(detail: Any) -> str:
    if not isinstance(detail, Mapping):
        return ""
    for key in ("settlement_status", "settlement", "state", "status"):
        value = detail.get(key)
        if value not in (None, ""):
            return str(value).strip().upper()
    return ""
def _canary_terminal_no_fill_proof(detail: Any) -> bool:
    if not isinstance(detail, Mapping) or detail.get("no_fill_confirmed") is not True:
        return False
    status = str(
        detail.get("terminal_status")
        or detail.get("settlement_status")
        or detail.get("state")
        or detail.get("status")
        or ""
    ).strip().upper()
    if status not in {"CANCELLED", "CANCELED", "REJECTED", "FAILED", "EXPIRED"}:
        return False
    source = str(detail.get("source") or detail.get("provenance") or "").strip()
    if not source:
        return False
    try:
        if _risk_decimal(detail.get("filled_quantity"), name="filled_quantity", nonnegative=True) != 0:
            return False
    except ValueError:
        return False
    if "trade_count" in detail:
        try:
            if _risk_decimal(detail["trade_count"], name="trade_count", nonnegative=True) != 0:
                return False
        except ValueError:
            return False
    if "trade_ids" in detail:
        trade_ids = detail["trade_ids"]
        if not isinstance(trade_ids, Sequence) or isinstance(trade_ids, (str, bytes)) or list(trade_ids):
            return False
    return True


def _merge_canary_fill_details(previous: Any, current: Any) -> dict[str, Any] | None:
    if not isinstance(previous, Mapping) or not isinstance(current, Mapping):
        return None
    old_status = _canary_fill_settlement_status(previous)
    new_status = _canary_fill_settlement_status(current)
    ignored = {
        "settlement_status", "settlement", "state", "status", "_settlement_history",
        "order_status", "trade_status", "order_state", "updated_at", "observed_at",
        "last_updated_at",
    }
    if old_status in _CANARY_CONFIRMED_SETTLEMENT_STATUSES:
        if new_status not in _CANARY_CONFIRMED_SETTLEMENT_STATUSES:
            return None
        for key in set(previous) & set(current):
            if key not in ignored and previous[key] != current[key]:
                return None
        merged = dict(previous)
        merged.update({key: value for key, value in current.items() if key != "_settlement_history"})
        return merged
    if not new_status:
        return None
    for key in set(previous) & set(current):
        if key not in ignored and previous[key] != current[key]:
            return None
    merged = dict(previous)
    merged.update({key: value for key, value in current.items() if key != "_settlement_history"})
    history = merged.get("_settlement_history")
    history_values = list(history) if isinstance(history, Sequence) and not isinstance(history, (str, bytes)) else []
    history_values.append({"status": old_status or "UNKNOWN", "detail": dict(previous)})
    merged["_settlement_history"] = history_values
    return merged


def _canary_equity_mark_record(row: sqlite3.Row | None) -> dict[str, Any]:
    if row is None:
        raise ValueError("equity mark not found")
    quantity = _risk_decimal(row["quantity"])
    mark_price = _risk_decimal(row["mark_price"])
    mark_fee = _risk_decimal(row["mark_fee"])
    cost_basis = _risk_decimal(row["cost_basis_usd"])
    mark_value = quantity * mark_price - mark_fee
    unrealized_pnl = mark_value - cost_basis
    return {
        "mark_id": row["mark_id"],
        "market_id": row["market_id"],
        "token_id": row["token_id"],
        "side": row["side"],
        "quantity": _risk_text(quantity),
        "mark_price": _risk_text(mark_price),
        "cost_basis_usd": _risk_text(cost_basis),
        "mark_fee": _risk_text(mark_fee),
        "mark_value_usd": _risk_text(mark_value),
        "unrealized_pnl_usd": _risk_text(unrealized_pnl),
        "equity_loss_usd": _risk_text(max(Decimal("0"), -unrealized_pnl)),
        "valuation_status": "KNOWN",
        "observed_at": _parse_datetime(row["observed_at"]),
        "source": row["source"],
        "config_id": row["config_id"],
        "config_generation": row["config_generation"],
        "control_generation": row["control_generation"],
        "strategy_version_id": row["strategy_version_id"],
        "research_trial_id": row["research_trial_id"],
        "candidate_id": row["candidate_id"],
        "portfolio_selection_id": row["portfolio_selection_id"],
        "admission_policy_id": row["admission_policy_id"],
        "admission_policy_version": row["admission_policy_version"],
        "risk_config_id": row["risk_config_id"],
        "risk_config_generation": row["risk_config_generation"],
        "risk_config_hash": row["risk_config_hash"],
        "allocation": row["allocation"],
        "detail": _load(row["detail_json"]) if row["detail_json"] else {},
        "created_at": _parse_datetime(row["created_at"]),
    }


def _canary_reservation_record(row: sqlite3.Row | None) -> dict[str, Any]:
    if row is None:
        raise ValueError("risk reservation not found")
    return {
        "reservation_id": row["reservation_id"],
        "intent_id": row["intent_id"],
        "side": row["side"],
        "market_id": row["market_id"],
        "event_id": row["event_id"],
        "requested_cost": _risk_text(row["requested_cost"]),
        "filled_cost": _risk_text(row["filled_cost"]),
        "remaining_cost": _risk_text(row["remaining_cost"]),
        "fee_reserve": _risk_text(row["fee_reserve"]),
        "quantity": _risk_text(row["quantity"]),
        "filled_quantity": _risk_text(row["filled_quantity"]),
        "status": row["status"],
        "config_generation": row["config_generation"],
        "config_hash": row["config_hash"],
        "config_id": row["config_id"],
        "strategy_version_id": row["strategy_version_id"],
        "research_trial_id": row["research_trial_id"],
        "candidate_id": row["candidate_id"],
        "portfolio_selection_id": row["portfolio_selection_id"],
        "admission_policy_id": row["admission_policy_id"],
        "admission_policy_version": row["admission_policy_version"],
        "risk_config_id": row["risk_config_id"],
        "risk_config_generation": row["risk_config_generation"],
        "risk_config_hash": row["risk_config_hash"],
        "allocation": row["allocation"],
        "control_generation": row["control_generation"],
        "detail": _load(row["detail_json"]) if row["detail_json"] else {},
        "created_at": _parse_datetime(row["created_at"]),
        "submitted_at": _parse_datetime(row["submitted_at"]),
        "updated_at": _parse_datetime(row["updated_at"]),
        "released_at": _parse_datetime(row["released_at"]),
    }


__all__ = ["AxiomStore"]
