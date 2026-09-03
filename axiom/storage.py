"""SQLite persistence for immutable research datasets and experiment artifacts.

AxiomStore is intentionally boring: SQLite, JSON payloads, UTC timestamps, and
append-only records. Dataset versions use a primary key and are never updated;
writing the same ``(dataset_id, version)`` raises ``ValueError``. Stored market
objects are reconstructed as canonical domain dataclasses, while arbitrary
strategy, experiment and report payloads remain plain JSON-compatible values.
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import threading
from contextlib import contextmanager
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, Sequence

from .domain import (
    CryptoTicker,
    Fill,
    InstrumentMetadata,
    MarketType,
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

    def __init__(self, path: str | os.PathLike[str] = ":memory:", *, connection: sqlite3.Connection | None = None) -> None:
        self.path = str(path)
        if connection is None and self.path not in {":memory:", ""} and not self.path.startswith("file:"):
            parent = Path(self.path).expanduser().parent
            if str(parent) not in {"", "."}:
                parent.mkdir(parents=True, exist_ok=True)
        self._conn = connection or sqlite3.connect(self.path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        self._transaction_depth = 0
        self.initialize()

    @property
    def connection(self) -> sqlite3.Connection:
        """Underlying connection for dashboard integrations and read-only queries."""
        return self._conn
    @contextmanager
    def transaction(self) -> Iterator["AxiomStore"]:
        """Group several append-only writes into one rollback boundary."""
        with self._lock:
            self._transaction_depth += 1
            savepoint = f"axiom_tx_{id(self):x}_{self._transaction_depth}"
            self._conn.execute(f"SAVEPOINT {savepoint}")
            try:
                yield self
            except Exception:
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
                yield
            else:
                with self._conn:
                    yield


    def initialize(self) -> None:
        """Create schema and indexes without changing existing records."""
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
                CREATE TABLE IF NOT EXISTS polymarket_markets (
                    market_id TEXT NOT NULL,
                    observed_at TEXT NOT NULL,
                    metadata_hash TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (market_id, observed_at, metadata_hash)
                );
                CREATE INDEX IF NOT EXISTS idx_polymarket_markets_observed
                    ON polymarket_markets(market_id, observed_at);
                CREATE TABLE IF NOT EXISTS polymarket_snapshots (
                    snapshot_id TEXT PRIMARY KEY,
                    market_id TEXT NOT NULL,
                    source_timestamp TEXT NOT NULL,
                    observed_at TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    quality TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_polymarket_snapshots_market_time
                    ON polymarket_snapshots(market_id, source_timestamp, observed_at);
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
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_collection_errors_market_time
                    ON collection_errors(market_id, observed_at);
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
                """
            )
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
        try:
            with self._write_context():
                self._conn.execute(
                    "INSERT INTO datasets(dataset_id, version, payload_json, metadata_json, quality, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                    (dataset_id, version, payload, metadata_json, quality_value, _now_iso()),
                )
        except sqlite3.IntegrityError as exc:
            raise ValueError(f"dataset version already exists: {dataset_id}/{version}") from exc

    def save_dataset_version(self, dataset_id: str, version: str, records: Any, **kwargs: Any) -> None:
        """Explicit alias for :meth:`save_dataset`."""
        self.save_dataset(dataset_id, version, records, **kwargs)

    def load_dataset(self, dataset_id: str, version: str | None = None) -> Any | None:
        with self._lock:
            if version is None:
                row = self._conn.execute(
                    "SELECT payload_json FROM datasets WHERE dataset_id=? ORDER BY created_at DESC, version DESC LIMIT 1",
                    (str(dataset_id),),
                ).fetchone()
            else:
                row = self._conn.execute(
                    "SELECT payload_json FROM datasets WHERE dataset_id=? AND version=?",
                    (str(dataset_id), str(version)),
                ).fetchone()
        return _load(row["payload_json"]) if row else None

    def load_dataset_record(self, dataset_id: str, version: str | None = None) -> dict[str, Any] | None:
        """Return payload plus version, quality and metadata for dashboards."""
        with self._lock:
            if version is None:
                row = self._conn.execute(
                    "SELECT * FROM datasets WHERE dataset_id=? ORDER BY created_at DESC, version DESC LIMIT 1",
                    (str(dataset_id),),
                ).fetchone()
            else:
                row = self._conn.execute(
                    "SELECT * FROM datasets WHERE dataset_id=? AND version=?",
                    (str(dataset_id), str(version)),
                ).fetchone()
        if row is None:
            return None
        return {
            "dataset_id": row["dataset_id"],
            "version": row["version"],
            "records": _load(row["payload_json"]),
            "metadata": _load(row["metadata_json"]),
            "quality": row["quality"],
            "created_at": _parse_datetime(row["created_at"]),
        }

    def dataset_versions(self, dataset_id: str) -> list[str]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT version FROM datasets WHERE dataset_id=? ORDER BY created_at, version", (str(dataset_id),)
            ).fetchall()
        return [str(row["version"]) for row in rows]

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
            if limit < 0:
                raise ValueError("limit must be non-negative")
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
        if kind is not None:
            clauses.append("kind=?")
            values.append(str(kind))
        query = "SELECT kind,payload_json FROM snapshots WHERE " + " AND ".join(clauses) + " ORDER BY timestamp,dataset_id,dataset_version"
        if limit is not None:
            if limit < 0:
                raise ValueError("limit must be non-negative")
            query += " LIMIT ?"
            values.append(int(limit))
        with self._lock:
            rows = self._conn.execute(query, values).fetchall()
        return [_snapshot_from_record(row["kind"], _load(row["payload_json"])) for row in rows]

    def save_order_books(self, key: str, snapshots: Iterable[OrderBookSnapshot], **kwargs: Any) -> int:
        return self.save_snapshots(key, snapshots, **kwargs)

    def load_order_books(self, key: str, **kwargs: Any) -> list[OrderBookSnapshot]:
        return [item for item in self.load_snapshots(key, kind="order_book", **kwargs) if isinstance(item, OrderBookSnapshot)]

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
    ) -> bool:
        identifier = str(market_id).strip()
        if not identifier:
            raise ValueError("market_id is required")
        observed = _iso(observed_at)
        payload_json = _dump(payload)
        digest = str(metadata_hash or hashlib.sha256(payload_json.encode("utf-8")).hexdigest())
        with self._write_context():
            existing = self._conn.execute(
                "SELECT 1 FROM polymarket_markets WHERE market_id=? AND metadata_hash=? LIMIT 1",
                (identifier, digest),
            ).fetchone()
            if existing is not None:
                return False
            self._conn.execute(
                "INSERT INTO polymarket_markets(market_id,observed_at,metadata_hash,payload_json,created_at) VALUES (?,?,?,?,?)",
                (identifier, observed, digest, payload_json, _now_iso()),
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
    ) -> bool:
        identifier = str(market_id).strip()
        if not identifier:
            raise ValueError("market_id is required")
        snapshot_key = str(snapshot_id).strip()
        if not snapshot_key:
            raise ValueError("snapshot_id is required")
        source = _iso(source_timestamp)
        observed = _iso(observed_at)
        payload_json = _dump(payload)
        quality_value = _enum_value(quality) or "ORDER_BOOK_SIMULATED"
        with self._write_context():
            cursor = self._conn.execute(
                "INSERT OR IGNORE INTO polymarket_snapshots(snapshot_id,market_id,source_timestamp,observed_at,payload_json,quality,created_at) VALUES (?,?,?,?,?,?,?)",
                (snapshot_key, identifier, source, observed, payload_json, quality_value, _now_iso()),
            )
        return cursor.rowcount > 0

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
            cursor = self._conn.execute(
                "INSERT OR IGNORE INTO polymarket_trades(trade_key,market_id,timestamp,payload_json,created_at) VALUES (?,?,?,?,?)",
                (key, identifier, _iso(timestamp), payload_json, _now_iso()),
            )
        return cursor.rowcount > 0

    def load_polymarket_snapshots(
        self,
        market_id: str | None = None,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
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
        query = "SELECT snapshot_id,market_id,source_timestamp,observed_at,payload_json,quality FROM polymarket_snapshots"
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY observed_at,market_id,source_timestamp"
        if limit is not None:
            if limit < 0:
                raise ValueError("limit must be non-negative")
            query += " LIMIT ?"
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

    def tracked_polymarket_markets(self) -> list[str]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT market_id FROM polymarket_markets UNION SELECT market_id FROM polymarket_snapshots ORDER BY market_id"
            ).fetchall()
        return [str(row["market_id"]) for row in rows]

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
    ) -> str:
        body = {"market_id": market_id, "kind": str(kind), "detail": str(detail), "payload": payload}
        error_id = hashlib.sha256(_dump(body | {"observed_at": _iso(observed_at)}).encode("utf-8")).hexdigest()
        with self._write_context():
            self._conn.execute(
                "INSERT OR IGNORE INTO collection_errors(error_id,market_id,observed_at,kind,detail,payload_json,created_at) VALUES (?,?,?,?,?,?,?)",
                (error_id, str(market_id) if market_id is not None else None, _iso(observed_at), str(kind), str(detail), _dump(payload), _now_iso()),
            )
        return error_id

    def list_collection_errors(self, market_id: str | None = None) -> list[dict[str, Any]]:
        query = "SELECT error_id,market_id,observed_at,kind,detail,payload_json FROM collection_errors"
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
        if bankroll <= 0:
            raise ValueError("frozen forward test bankroll must be positive")
        config_json = _dump(spec.get("config", {}))
        allowed_json = _dump(spec.get("allowed_markets", []))
        limits_json = _dump(spec.get("risk_limits", {}))
        quality = _enum_value(spec.get("quality")) or "PAPER_FORWARD"
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

    def load_forward_tests(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute("SELECT * FROM forward_tests ORDER BY start_timestamp,experiment_id").fetchall()
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

    def polymarket_health(
        self,
        *,
        expected_interval_seconds: float = 60.0,
        stale_after_seconds: float | None = None,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        expected = float(expected_interval_seconds)
        if not expected > 0:
            raise ValueError("expected_interval_seconds must be positive")
        stale_after = float(stale_after_seconds if stale_after_seconds is not None else expected * 3.0)
        if stale_after <= 0:
            raise ValueError("stale_after_seconds must be positive")
        current = ensure_utc(now or utc_now())
        with self._lock:
            snapshot_rows = self._conn.execute(
                "SELECT market_id,observed_at,payload_json FROM polymarket_snapshots ORDER BY market_id,observed_at"
            ).fetchall()
            market_count = int(
                self._conn.execute(
                    "SELECT COUNT(*) AS n FROM (SELECT market_id FROM polymarket_markets UNION SELECT market_id FROM polymarket_snapshots)"
                ).fetchone()["n"]
            )
            trade_count = int(self._conn.execute("SELECT COUNT(*) AS n FROM polymarket_trades").fetchone()["n"])
            metadata_count = int(self._conn.execute("SELECT COUNT(*) AS n FROM polymarket_markets").fetchone()["n"])
            error_count = int(self._conn.execute("SELECT COUNT(*) AS n FROM collection_errors").fetchone()["n"])
            malformed_count = int(
                self._conn.execute(
                    "SELECT COUNT(*) AS n FROM collection_errors WHERE lower(kind) LIKE '%malform%' OR lower(kind) LIKE '%parse%'"
                ).fetchone()["n"]
            )
        observations: dict[str, list[datetime]] = {}
        latest_payload: dict[str, Mapping[str, Any]] = {}
        for row in snapshot_rows:
            timestamp = _parse_datetime(row["observed_at"])
            if timestamp is None:
                continue
            market = str(row["market_id"])
            observations.setdefault(market, []).append(timestamp)
            latest_payload[market] = _load(row["payload_json"])
        gaps: list[dict[str, Any]] = []
        latest_by_market: dict[str, datetime] = {}
        for market, stamps in observations.items():
            stamps.sort()
            latest_by_market[market] = stamps[-1]
            for previous, current_stamp in zip(stamps, stamps[1:]):
                gap_seconds = (current_stamp - previous).total_seconds()
                if gap_seconds > expected * 1.5:
                    missing = max(1, int(round(gap_seconds / expected)) - 1)
                    gaps.append(
                        {
                            "market_id": market,
                            "from": previous.isoformat(),
                            "to": current_stamp.isoformat(),
                            "seconds": gap_seconds,
                            "missing_intervals": missing,
                        }
                    )
        stale_markets = sorted(
            market for market, stamp in latest_by_market.items() if (current - stamp).total_seconds() > stale_after
        )
        resolved_markets = 0
        for payload in latest_payload.values():
            snapshot = payload.get("snapshot") if isinstance(payload, Mapping) else None
            settlement = snapshot.get("settlement") if isinstance(snapshot, Mapping) else None
            if str(settlement) in {
                SettlementState.RESOLVED_YES.value,
                SettlementState.RESOLVED_NO.value,
                SettlementState.VOID.value,
            }:
                resolved_markets += 1
        snapshot_count = len(snapshot_rows)
        if snapshot_count == 0:
            grade = "F"
        elif malformed_count or (observations and len(stale_markets) / max(1, len(observations)) > 0.5):
            grade = "D"
        elif stale_markets or gaps:
            grade = "C"
        elif error_count:
            grade = "B"
        else:
            grade = "A"
        storage_bytes = _storage_bytes(self._conn, self.path)
        latest = max(latest_by_market.values(), default=None)
        return {
            "grade": grade,
            "markets": market_count,
            "markets_with_snapshots": len(observations),
            "resolved_markets": resolved_markets,
            "snapshots": snapshot_count,
            "trades": trade_count,
            "metadata_records": metadata_count,
            "collection_errors": error_count,
            "malformed_records": malformed_count,
            "stale_markets": stale_markets,
            "gaps": gaps,
            "latest_observed_at": latest.isoformat() if latest else None,
            "expected_interval_seconds": expected,
            "stale_after_seconds": stale_after,
            "storage_bytes": storage_bytes,
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

    def load_experiment(self, experiment_id: str) -> Any | None:
        with self._lock:
            row = self._conn.execute("SELECT payload_json FROM experiments WHERE experiment_id=?", (str(experiment_id),)).fetchone()
        return _load(row["payload_json"]) if row else None

    def list_experiments(self, strategy_id: str | None = None) -> list[dict[str, Any]]:
        query = "SELECT experiment_id,strategy_id,payload_json,created_at FROM experiments"
        values: tuple[Any, ...] = ()
        if strategy_id is not None:
            query += " WHERE strategy_id=?"
            values = (str(strategy_id),)
        query += " ORDER BY created_at,experiment_id"
        with self._lock:
            rows = self._conn.execute(query, values).fetchall()
        return [
            {"experiment_id": row["experiment_id"], "strategy_id": row["strategy_id"], "experiment": _load(row["payload_json"]), "created_at": _parse_datetime(row["created_at"])}
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

    def list_reports(self, experiment_id: str | None = None) -> list[dict[str, Any]]:
        query = "SELECT report_id,experiment_id,payload_json,created_at FROM reports"
        values: tuple[Any, ...] = ()
        if experiment_id is not None:
            query += " WHERE experiment_id=?"
            values = (str(experiment_id),)
        query += " ORDER BY created_at,report_id"
        with self._lock:
            rows = self._conn.execute(query, values).fetchall()
        return [
            {"report_id": row["report_id"], "experiment_id": row["experiment_id"], "report": _load(row["payload_json"]), "created_at": _parse_datetime(row["created_at"])}
            for row in rows
        ]

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
        quality = None
        if dataset_id is not None:
            record = self.load_dataset_record(dataset_id, version)
            quality = record.get("quality") if record else None
        return {"bars": int(count), "snapshots": int(snapshots), "datasets": int(datasets), "quality": quality}

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
        try:
            with self._write_context():
                self._conn.executemany(
                    f"INSERT INTO {table}({','.join(columns)}) VALUES ({placeholders})", rows
                )
        except sqlite3.IntegrityError as exc:
            raise ValueError(f"duplicate immutable record in {table} ({key_columns})") from exc


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
