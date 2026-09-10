"""Bounded, read-only Polymarket research/forward data inventory.

This utility intentionally opens the explicitly supplied SQLite source with
``mode=ro`` and one deferred read transaction.  It does not construct an
``AxiomStore`` (which can initialise/migrate a database), does not use the
SQLite ``immutable`` URI flag, and never reads credential/control tables.

The report contains exact aggregate facts for the research and forward market
partitions.  A separate JSONL export contains only a deterministic, bounded
historical research slice.  Forward rows are inventoried but are never
presented as historical research input or exported as research records.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
import sqlite3
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean
from typing import Any, Iterable, Mapping, Sequence
from urllib.parse import quote


SCHEMA_VERSION = "polymarket-release-data-inventory-v1"
EXPORT_SCHEMA_VERSION = "polymarket-release-research-export-v1"
DEFAULT_SOURCE_DB = Path("C:/Users/User/projects/axiom/runtime-data/axiom.sqlite")
DEFAULT_REPORT = Path("reports/polymarket_release_data_inventory.json")
DEFAULT_EXPORT = Path("runtime-data/polymarket_release_research_export.jsonl")
DEFAULT_EXPORT_ROWS = 4096
DEFAULT_SEQUENCE_SAMPLE_MARKETS = 32
LONG_GAP_SECONDS = 900.0

# These are the only application tables this report may query.  In particular,
# no control, enable, credential, wallet, order, or execution payload table is
# ever selected by the utility.
ALLOWED_TABLES = (
    "dataset_catalog",
    "dataset_integrity_attestation",
    "dataset_bootstrap_state",
    "polymarket_markets",
    "polymarket_snapshots",
    "polymarket_trades",
    "collection_cycles",
    "collection_errors",
    "collector_state",
)
EXPORT_TABLES = frozenset({"polymarket_markets", "polymarket_snapshots"})
EXCLUDED_TABLE_NAMES = frozenset(
    {
        "canary_control",
        "canary_eligibility",
        "canary_execution_events",
        "canary_ledger",
        "operator_config",
        "operator_actions",
        "operator_jobs",
        "forward_tests",
        "fills",
        "paper_execution_events",
        "paper_bet_ledger",
        "research_queue",
        "research_queue_events",
    }
)
SENSITIVE_KEY_RE = re.compile(
    r"(?:secret|private.?key|password|credential|api.?key|access.?token|refresh.?token|bearer|authorization|cookie|wallet)",
    re.IGNORECASE,
)
SENSITIVE_VALUE_RE = re.compile(
    r"(?:-----BEGIN .*PRIVATE KEY-----|bearer\s+[A-Za-z0-9._-]+|(?:api[_ -]?key|secret|password)\s*[:=])",
    re.IGNORECASE,
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def _hash_json(value: Any) -> str:
    return "sha256:" + hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _json(value: Any, default: Any) -> Any:
    if value is None:
        return default
    try:
        parsed = json.loads(value) if isinstance(value, (str, bytes, bytearray)) else value
    except (TypeError, ValueError, json.JSONDecodeError):
        return default
    return parsed


def _redact(value: Any, key: str | None = None) -> Any:
    """Redact secret-like keys/values without hiding public market identifiers."""
    if key and SENSITIVE_KEY_RE.search(key):
        return "[REDACTED]"
    if isinstance(value, Mapping):
        return {str(k): _redact(v, str(k)) for k, v in value.items()}
    if isinstance(value, list):
        return [_redact(item) for item in value]
    if isinstance(value, tuple):
        return [_redact(item) for item in value]
    if isinstance(value, str) and SENSITIVE_VALUE_RE.search(value):
        return "[REDACTED]"
    return value


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if isinstance(value, bool):
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _parse_time(value: Any) -> datetime | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _iso(value: Any) -> str | None:
    parsed = _parse_time(value)
    return parsed.isoformat() if parsed else (str(value) if value is not None else None)


def _stat(path: Path) -> dict[str, Any] | None:
    try:
        item = path.stat()
    except OSError:
        return None
    return {"path": str(path), "size_bytes": int(item.st_size), "mtime_ns": int(item.st_mtime_ns)}


def _source_files(path: Path) -> dict[str, Any]:
    return {
        "database": _stat(path),
        "wal": _stat(Path(str(path) + "-wal")),
        "shm": _stat(Path(str(path) + "-shm")),
    }


def _connect_read_only(path: Path) -> sqlite3.Connection:
    # Do not add immutable=1: the authorised source may be a live WAL-backed
    # database, and immutable mode would silently ignore its WAL.
    uri = "file:" + quote(str(path.resolve()), safe="/:\\") + "?mode=ro"
    connection = sqlite3.connect(uri, uri=True, timeout=5.0)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA busy_timeout=5000")
    connection.execute("PRAGMA query_only=ON")
    if int(connection.execute("PRAGMA query_only").fetchone()[0]) != 1:
        connection.close()
        raise RuntimeError("SQLite query_only pragma was not enabled")
    connection.execute("BEGIN DEFERRED")
    return connection


def _table_names(connection: sqlite3.Connection) -> set[str]:
    return {str(row[0]) for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def _schema_inventory(connection: sqlite3.Connection, tables: set[str]) -> dict[str, Any]:
    discovered: dict[str, Any] = {}
    for table in ALLOWED_TABLES:
        if table not in tables:
            discovered[table] = {"present": False, "columns": [], "indexes": []}
            continue
        columns = [
            {
                "name": str(row[1]),
                "type": str(row[2] or ""),
                "not_null": bool(row[3]),
                "primary_key_position": int(row[5]),
            }
            for row in connection.execute(f"PRAGMA table_info({table})").fetchall()
        ]
        indexes = []
        for row in connection.execute(f"PRAGMA index_list({table})").fetchall():
            index_name = str(row[1])
            index_columns = [str(item[2]) for item in connection.execute(f"PRAGMA index_info({index_name})").fetchall()]
            indexes.append({"name": index_name, "unique": bool(row[2]), "columns": index_columns})
        discovered[table] = {"present": True, "columns": columns, "indexes": indexes}
    return {
        "allowlisted_tables": list(ALLOWED_TABLES),
        "tables": discovered,
        "excluded_control_or_execution_tables": sorted(EXCLUDED_TABLE_NAMES.intersection(tables)),
        "excluded_table_policy": "Schema names may be discovered; excluded payloads are never selected.",
    }


def _exact_table_counts(connection: sqlite3.Connection, tables: set[str]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for table in ALLOWED_TABLES:
        if table not in tables:
            result[table] = {"present": False, "rows": None}
            continue
        row = connection.execute(f"SELECT COUNT(*) AS rows FROM {table}").fetchone()
        result[table] = {"present": True, "rows": int(row["rows"])}
    return result


def _row_dict(row: sqlite3.Row | Mapping[str, Any]) -> dict[str, Any]:
    return {str(key): row[key] for key in row.keys()} if isinstance(row, sqlite3.Row) else dict(row)


def _catalog_coverage(connection: sqlite3.Connection, tables: set[str]) -> dict[str, Any]:
    if "dataset_catalog" not in tables:
        return {"available": False, "classification": "unavailable"}
    grouped = []
    query = """
        SELECT source_type, market_type, COUNT(*) AS rows,
               COUNT(DISTINCT dataset_id) AS dataset_ids,
               COALESCE(SUM(row_count), 0) AS declared_rows,
               MIN(start_timestamp) AS first_start,
               MAX(end_timestamp) AS last_end,
               SUM(CASE WHEN completeness >= 1.0 THEN 1 ELSE 0 END) AS complete_rows
        FROM dataset_catalog
        GROUP BY source_type, market_type
        ORDER BY source_type, market_type
    """
    for row in connection.execute(query).fetchall():
        grouped.append(
            {
                "source_type": str(row["source_type"] or "").upper(),
                "market_type": str(row["market_type"] or "").lower(),
                "catalog_rows": int(row["rows"]),
                "dataset_ids": int(row["dataset_ids"]),
                "declared_row_count_sum": int(row["declared_rows"]),
                "complete_catalog_rows": int(row["complete_rows"] or 0),
                "first_start_timestamp": row["first_start"],
                "last_end_timestamp": row["last_end"],
            }
        )
    return {
        "available": True,
        "classification": "exact",
        "partitions": grouped,
        "historical_prediction_catalog": [
            _row_dict(row)
            for row in connection.execute(
                """
                SELECT dataset_id, dataset_version, provider, instrument, timeframe,
                       row_count, completeness, start_timestamp, end_timestamp,
                       quality, source_type, snapshot_id, created_at, updated_at
                FROM dataset_catalog
                WHERE upper(source_type)='HISTORICAL' AND lower(market_type)='prediction'
                ORDER BY dataset_id, dataset_version
                """
            ).fetchall()
        ],
    }


def _attestation_coverage(connection: sqlite3.Connection, tables: set[str]) -> dict[str, Any]:
    if "dataset_integrity_attestation" not in tables:
        return {"available": False, "classification": "unavailable"}
    rows = [
        _row_dict(row)
        for row in connection.execute(
            """
            SELECT dataset_id, dataset_version, source_type, market_type, row_count,
                   completeness, start_timestamp, end_timestamp, execution_fidelity,
                   contamination_result, provenance_version, policy_version,
                   attestation_hash, verified_at, status
            FROM dataset_integrity_attestation
            ORDER BY dataset_id, dataset_version
            """
        ).fetchall()
    ]
    return {
        "available": True,
        "classification": "exact",
        "rows": len(rows),
        "passed_rows": sum(1 for row in rows if str(row.get("status", "")).upper() in {"PASS", "CURRENT", "VALID"}),
        "attestations": rows,
    }

def _historical_immutability(connection: sqlite3.Connection, tables: set[str]) -> dict[str, Any]:
    """Check persisted identity uniqueness for the immutable research slice."""
    result: dict[str, Any] = {
        "classification": "exact identity uniqueness checks",
        "source_type": "HISTORICAL",
        "append_only_claim": "Rows are treated as immutable only under the persisted primary/identity keys; this utility never writes or repairs rows.",
    }
    if "polymarket_snapshots" in tables:
        row = connection.execute(
            "SELECT COUNT(*) AS rows, COUNT(DISTINCT snapshot_id) AS distinct_ids FROM polymarket_snapshots WHERE upper(source_type)='HISTORICAL'"
        ).fetchone()
        result["snapshot_identity"] = {
            "identity_key": "snapshot_id",
            "rows": int(row["rows"] or 0),
            "distinct_identity_ids": int(row["distinct_ids"] or 0),
            "duplicate_identity_rows": int(row["rows"] or 0) - int(row["distinct_ids"] or 0),
        }
    if "polymarket_markets" in tables:
        row = connection.execute(
            """
            SELECT COUNT(*) AS duplicate_identity_groups,
                   COALESCE(SUM(identity_rows - 1), 0) AS duplicate_identity_rows
            FROM (
                SELECT market_id, observed_at, metadata_hash, COUNT(*) AS identity_rows
                FROM polymarket_markets
                WHERE upper(source_type)='HISTORICAL'
                GROUP BY market_id, observed_at, metadata_hash
                HAVING COUNT(*) > 1
            )
            """
        ).fetchone()
        total = connection.execute(
            "SELECT COUNT(*) AS rows FROM polymarket_markets WHERE upper(source_type)='HISTORICAL'"
        ).fetchone()
        result["market_metadata_identity"] = {
            "identity_key": "market_id,observed_at,metadata_hash",
            "rows": int(total["rows"] or 0),
            "duplicate_identity_groups": int(row["duplicate_identity_groups"] or 0),
            "duplicate_identity_rows": int(row["duplicate_identity_rows"] or 0),
        }
    return result



def _payload_provider(payload: Mapping[str, Any]) -> str:
    value = payload.get("provider")
    return str(value).strip() if value is not None and str(value).strip() else "(missing)"


def _first_value(value: Any, names: Sequence[str]) -> Any:
    if isinstance(value, Mapping):
        for name in names:
            if name in value and value[name] not in (None, ""):
                return value[name]
        for child in value.values():
            found = _first_value(child, names)
            if found not in (None, ""):
                return found
    elif isinstance(value, list):
        for child in value:
            found = _first_value(child, names)
            if found not in (None, ""):
                return found
    return None


def _first_bool(value: Any, names: Sequence[str]) -> bool | None:
    found = _first_value(value, names)
    return found if isinstance(found, bool) else None


def _latest_markets(connection: sqlite3.Connection, source_type: str) -> dict[str, dict[str, Any]]:
    if not _has_table(connection, "polymarket_markets"):
        return {}
    # The grouped subquery limits payload reads to one latest metadata row per
    # market.  A hash tie is resolved deterministically in Python below.
    rows = connection.execute(
        """
        SELECT m.market_id, m.observed_at, m.metadata_hash, m.payload_json,
               m.created_at, m.source_type
        FROM polymarket_markets AS m
        JOIN (
            SELECT market_id, MAX(observed_at) AS observed_at
            FROM polymarket_markets
            WHERE upper(source_type)=?
            GROUP BY market_id
        ) AS latest
          ON latest.market_id=m.market_id AND latest.observed_at=m.observed_at
        WHERE upper(m.source_type)=?
        ORDER BY m.market_id, m.metadata_hash DESC
        """,
        (source_type, source_type),
    ).fetchall()
    result: dict[str, dict[str, Any]] = {}
    for row in rows:
        market_id = str(row["market_id"])
        if market_id in result:
            continue
        payload = _json(row["payload_json"], {})
        if not isinstance(payload, Mapping):
            payload = {}
        result[market_id] = {
            "market_id": market_id,
            "observed_at": row["observed_at"],
            "metadata_hash": row["metadata_hash"],
            "created_at": row["created_at"],
            "source_type": str(row["source_type"] or "").upper(),
            "payload": payload,
            "payload_valid": isinstance(_json(row["payload_json"], None), Mapping),
        }
    return result


def _has_table(connection: sqlite3.Connection, table: str) -> bool:
    return bool(connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone())


def _sequence_stats(connection: sqlite3.Connection, source_type: str) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    if not _has_table(connection, "polymarket_snapshots"):
        return {"available": False, "classification": "unavailable"}, {}
    aggregate = connection.execute(
        """
        SELECT COUNT(*) AS rows, COUNT(DISTINCT market_id) AS markets,
               COUNT(DISTINCT source_timestamp) AS distinct_source_timestamps,
               MIN(source_timestamp) AS first_source_timestamp,
               MAX(source_timestamp) AS last_source_timestamp,
               MIN(observed_at) AS first_observed_at,
               MAX(observed_at) AS last_observed_at
        FROM polymarket_snapshots
        WHERE upper(source_type)=?
        """,
        (source_type,),
    ).fetchone()
    per_market_rows = connection.execute(
        """
        SELECT market_id, COUNT(*) AS rows,
               COUNT(DISTINCT source_timestamp) AS distinct_source_timestamps,
               MIN(source_timestamp) AS first_source_timestamp,
               MAX(source_timestamp) AS last_source_timestamp,
               MIN(observed_at) AS first_observed_at,
               MAX(observed_at) AS last_observed_at
        FROM polymarket_snapshots
        WHERE upper(source_type)=?
        GROUP BY market_id
        ORDER BY market_id
        """,
        (source_type,),
    ).fetchall()
    stats: dict[str, dict[str, Any]] = {
        str(row["market_id"]): {
            "market_id": str(row["market_id"]),
            "rows": int(row["rows"]),
            "distinct_source_timestamps": int(row["distinct_source_timestamps"]),
            "duplicate_source_timestamp_rows": int(row["rows"]) - int(row["distinct_source_timestamps"]),
            "first_source_timestamp": row["first_source_timestamp"],
            "last_source_timestamp": row["last_source_timestamp"],
            "first_observed_at": row["first_observed_at"],
            "last_observed_at": row["last_observed_at"],
            "transition_count": max(0, int(row["rows"]) - 1),
            "long_gap_transitions": 0,
            "max_gap_seconds": 0.0,
            "non_monotonic_observed_transitions": 0,
        }
        for row in per_market_rows
    }
    transitions = connection.execute(
        """
        WITH ordered AS (
            SELECT market_id, source_timestamp, observed_at, snapshot_id,
                   LAG(source_timestamp) OVER (
                       PARTITION BY market_id ORDER BY source_timestamp, snapshot_id
                   ) AS previous_source_timestamp,
                   LAG(observed_at) OVER (
                       PARTITION BY market_id ORDER BY source_timestamp, snapshot_id
                   ) AS previous_observed_at
            FROM polymarket_snapshots
            WHERE upper(source_type)=?
        )
        SELECT market_id,
               SUM(CASE WHEN previous_source_timestamp IS NOT NULL
                            AND (julianday(source_timestamp)-julianday(previous_source_timestamp))*86400.0 > ?
                        THEN 1 ELSE 0 END) AS long_gap_transitions,
               MAX(CASE WHEN previous_source_timestamp IS NOT NULL
                        THEN MAX(0.0, (julianday(source_timestamp)-julianday(previous_source_timestamp))*86400.0)
                        ELSE 0.0 END) AS max_gap_seconds,
               SUM(CASE WHEN previous_observed_at IS NOT NULL
                            AND observed_at < previous_observed_at THEN 1 ELSE 0 END)
                   AS non_monotonic_observed_transitions
        FROM ordered
        GROUP BY market_id
        ORDER BY market_id
        """,
        (source_type, LONG_GAP_SECONDS),
    ).fetchall()
    long_gap_transitions = 0
    long_gap_markets = 0
    duplicate_rows = 0
    transition_count = 0
    for row in transitions:
        market_id = str(row["market_id"])
        item = stats.get(market_id)
        if item is None:
            continue
        item["long_gap_transitions"] = int(row["long_gap_transitions"] or 0)
        item["max_gap_seconds"] = round(float(row["max_gap_seconds"] or 0.0), 3)
        item["non_monotonic_observed_transitions"] = int(row["non_monotonic_observed_transitions"] or 0)
        long_gap_transitions += item["long_gap_transitions"]
        long_gap_markets += int(item["long_gap_transitions"] > 0)
        duplicate_rows += item["duplicate_source_timestamp_rows"]
        transition_count += item["transition_count"]
    counts = [item["rows"] for item in stats.values()]
    exact = {
        "available": True,
        "classification": "exact",
        "source_type": source_type,
        "rows": int(aggregate["rows"] or 0),
        "markets": int(aggregate["markets"] or 0),
        "distinct_source_timestamps": int(aggregate["distinct_source_timestamps"] or 0),
        "first_source_timestamp": aggregate["first_source_timestamp"],
        "last_source_timestamp": aggregate["last_source_timestamp"],
        "first_observed_at": aggregate["first_observed_at"],
        "last_observed_at": aggregate["last_observed_at"],
        "per_market_rows": {
            "minimum": min(counts) if counts else 0,
            "maximum": max(counts) if counts else 0,
            "mean": round(mean(counts), 3) if counts else 0.0,
        },
        "duplicate_source_timestamp_rows": duplicate_rows,
        "transition_count": transition_count,
        "long_gap_threshold_seconds": LONG_GAP_SECONDS,
        "long_gap_transitions": long_gap_transitions,
        "markets_with_long_gap": long_gap_markets,
        "non_monotonic_observed_transitions": sum(item["non_monotonic_observed_transitions"] for item in stats.values()),
    }
    return exact, stats

def _contemporaneous_coverage(connection: sqlite3.Connection, source_type: str) -> dict[str, Any]:
    """Count snapshots that had persisted metadata at or before observation.

    This is deliberately separate from the latest-metadata lifecycle projection:
    a later metadata row must never be treated as evidence available to an
    earlier snapshot or decision.
    """
    if not _has_table(connection, "polymarket_snapshots") or not _has_table(connection, "polymarket_markets"):
        return {"available": False, "classification": "unavailable"}
    row = connection.execute(
        """
        SELECT COUNT(*) AS snapshot_rows,
               SUM(
                   CASE WHEN EXISTS (
                       SELECT 1
                       FROM polymarket_markets AS m
                       WHERE upper(m.source_type)=upper(s.source_type)
                         AND m.market_id=s.market_id
                         AND m.observed_at <= s.observed_at
                   ) THEN 1 ELSE 0 END
               ) AS snapshots_with_prior_metadata,
               COUNT(DISTINCT CASE WHEN NOT EXISTS (
                   SELECT 1
                   FROM polymarket_markets AS m
                   WHERE upper(m.source_type)=upper(s.source_type)
                     AND m.market_id=s.market_id
                     AND m.observed_at <= s.observed_at
               ) THEN s.market_id END) AS markets_without_prior_metadata,
               MIN(s.observed_at) AS first_snapshot_observed_at,
               MAX(s.observed_at) AS last_snapshot_observed_at
        FROM polymarket_snapshots AS s
        WHERE upper(s.source_type)=?
        """,
        (source_type,),
    ).fetchone()
    snapshot_rows = int(row["snapshot_rows"] or 0)
    with_prior = int(row["snapshots_with_prior_metadata"] or 0)
    return {
        "available": True,
        "classification": "exact",
        "source_type": source_type,
        "snapshot_rows": snapshot_rows,
        "snapshots_with_prior_metadata": with_prior,
        "snapshots_without_prior_metadata": snapshot_rows - with_prior,
        "markets_without_prior_metadata": int(row["markets_without_prior_metadata"] or 0),
        "first_snapshot_observed_at": row["first_snapshot_observed_at"],
        "last_snapshot_observed_at": row["last_snapshot_observed_at"],
        "alignment_rule": "metadata source_type and market_id match, metadata.observed_at <= snapshot.observed_at",
        "latest_metadata_is_not_used_for_prior_snapshot_evidence": True,
    }



def _lifecycle_as_of(
    connection: sqlite3.Connection,
    source_type: str,
    market_rows: Mapping[str, Mapping[str, Any]],
    sequence_rows: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    metadata_ids = set(market_rows)
    snapshot_ids = set(sequence_rows)
    metadata_only = sorted(metadata_ids - snapshot_ids)
    snapshot_only = sorted(snapshot_ids - metadata_ids)
    as_of_values = [str(row.get("observed_at")) for row in market_rows.values() if row.get("observed_at")]
    as_of = max(as_of_values) if as_of_values else None
    status_counts: Counter[str] = Counter()
    expiry_known = 0
    expiry_before_first_snapshot = 0
    snapshots_after_expiry = 0
    snapshots_before_expiry = 0
    rows_without_valid_payload = 0
    per_market_status: dict[str, str] = {}
    for market_id, row in market_rows.items():
        payload = row.get("payload") if isinstance(row.get("payload"), Mapping) else {}
        expiry_value = _first_value(payload, ("expiry", "expiration", "end"))
        expiry = _parse_time(expiry_value)
        closed = _first_bool(payload, ("closed",))
        active = _first_bool(payload, ("active",))
        if not row.get("payload_valid"):
            rows_without_valid_payload += 1
        if closed is True:
            status = "CLOSED"
        elif expiry is not None and _parse_time(row.get("observed_at")) and expiry <= _parse_time(row.get("observed_at")):
            status = "EXPIRED_AS_OF_LATEST_METADATA"
        elif active is False:
            status = "INACTIVE"
        elif active is True:
            status = "ACTIVE"
        elif expiry is not None:
            status = "EXPIRY_KNOWN"
        else:
            status = "UNKNOWN_LIFECYCLE"
        status_counts[status] += 1
        per_market_status[market_id] = status
        item = sequence_rows.get(market_id)
        if expiry is None or item is None:
            continue
        expiry_known += 1
        first_source = _parse_time(item.get("first_source_timestamp"))
        last_source = _parse_time(item.get("last_source_timestamp"))
        if first_source and first_source > expiry:
            expiry_before_first_snapshot += 1
        if last_source and last_source > expiry:
            snapshots_after_expiry += 1
        if first_source and first_source < expiry:
            snapshots_before_expiry += 1
    # A metadata row is one latest projection per market; snapshot counts are
    # exact over all rows in this source partition, while lifecycle status is
    # an as-of projection of the latest metadata observation.
    return {
        "contemporaneous_snapshot_metadata": _contemporaneous_coverage(connection, source_type),
        "available": True,
        "classification": "exact",
        "source_type": source_type,
        "as_of_definition": "latest metadata observed_at in this source partition",
        "as_of_observed_at": as_of,
        "market_metadata_markets": len(metadata_ids),
        "snapshot_markets": len(snapshot_ids),
        "markets_with_both": len(metadata_ids & snapshot_ids),
        "gaps": {
            "metadata_only_markets": len(metadata_only),
            "snapshot_without_metadata_markets": len(snapshot_only),
            "metadata_only_market_ids_sample": metadata_only[:DEFAULT_SEQUENCE_SAMPLE_MARKETS],
            "snapshot_without_metadata_market_ids_sample": snapshot_only[:DEFAULT_SEQUENCE_SAMPLE_MARKETS],
            "sample_ids_are_lexicographically_bounded": True,
        },
        "lifecycle_status_counts": dict(sorted(status_counts.items())),
        "expiry_known_markets_with_snapshots": expiry_known,
        "expiry_before_first_snapshot_markets": expiry_before_first_snapshot,
        "markets_with_snapshot_after_expiry": snapshots_after_expiry,
        "markets_with_snapshot_before_expiry": snapshots_before_expiry,
        "metadata_rows_with_invalid_payload": rows_without_valid_payload,
        "status_projection": {
            "classification": "exact aggregate; per-market IDs are not fully embedded",
            "sample": [
                {"market_id": market_id, "status_as_of_latest_metadata": per_market_status[market_id]}
                for market_id in sorted(per_market_status)[:DEFAULT_SEQUENCE_SAMPLE_MARKETS]
            ],
        },
    }


def _partition_rows(connection: sqlite3.Connection, table: str, source_type: str, column: str = "source_type") -> list[sqlite3.Row]:
    if not _has_table(connection, table):
        return []
    return connection.execute(f"SELECT * FROM {table} WHERE upper({column})=?", (source_type,)).fetchall()


def _partition_aggregate(connection: sqlite3.Connection, source_type: str) -> dict[str, Any]:
    result: dict[str, Any] = {"source_type": source_type, "classification": "exact"}
    if _has_table(connection, "polymarket_markets"):
        row = connection.execute(
            """
            SELECT COUNT(*) AS rows, COUNT(DISTINCT market_id) AS markets,
                   MIN(observed_at) AS first_observed_at, MAX(observed_at) AS last_observed_at
            FROM polymarket_markets WHERE upper(source_type)=?
            """,
            (source_type,),
        ).fetchone()
        result["markets"] = {"rows": int(row["rows"] or 0), "distinct_markets": int(row["markets"] or 0), "first_observed_at": row["first_observed_at"], "last_observed_at": row["last_observed_at"]}
    else:
        result["markets"] = {"available": False}
    if _has_table(connection, "polymarket_snapshots"):
        rows = connection.execute(
            """
            SELECT quality, COUNT(*) AS rows, COUNT(DISTINCT market_id) AS markets,
                   MIN(source_timestamp) AS first_source_timestamp,
                   MAX(source_timestamp) AS last_source_timestamp,
                   MIN(observed_at) AS first_observed_at,
                   MAX(observed_at) AS last_observed_at
            FROM polymarket_snapshots WHERE upper(source_type)=?
            GROUP BY quality ORDER BY quality
            """,
            (source_type,),
        ).fetchall()
        result["snapshots"] = {
            "classification": "exact",
            "quality_partitions": [
                {
                    "quality": str(row["quality"] or ""),
                    "rows": int(row["rows"]),
                    "distinct_markets": int(row["markets"]),
                    "first_source_timestamp": row["first_source_timestamp"],
                    "last_source_timestamp": row["last_source_timestamp"],
                    "first_observed_at": row["first_observed_at"],
                    "last_observed_at": row["last_observed_at"],
                }
                for row in rows
            ],
        }
    else:
        result["snapshots"] = {"available": False}
    return result


def _provenance(connection: sqlite3.Connection, tables: set[str], latest_by_source: Mapping[str, Mapping[str, Mapping[str, Any]]]) -> dict[str, Any]:
    result: dict[str, Any] = {"classification": "exact for persisted source_type columns; latest metadata payload projection"}
    if "dataset_catalog" in tables:
        rows = connection.execute(
            "SELECT source_type, provider, COUNT(*) AS rows FROM dataset_catalog GROUP BY source_type, provider ORDER BY source_type, provider"
        ).fetchall()
        result["catalog_provider_counts"] = [
            {"source_type": str(row["source_type"] or "").upper(), "provider": str(row["provider"] or ""), "rows": int(row["rows"])}
            for row in rows
        ]
    result["latest_market_payload_provider_counts"] = {
        source: dict(sorted(Counter(_payload_provider(item["payload"]) for item in rows.values()).items()))
        for source, rows in latest_by_source.items()
    }
    mismatch: dict[str, int] = {}
    for table in ("polymarket_markets", "polymarket_snapshots"):
        if table not in tables:
            continue
        row = connection.execute(
            f"""
            SELECT COUNT(*) AS rows
            FROM {table}
            WHERE json_extract(payload_json, '$.source_type') IS NOT NULL
              AND upper(CAST(json_extract(payload_json, '$.source_type') AS TEXT)) <> upper(source_type)
            """
        ).fetchone()
        mismatch[table] = int(row["rows"] or 0)
    result["explicit_payload_source_type_mismatches"] = mismatch
    result["timestamp_roles"] = {
        "source_timestamp": "canonical market/order-book chronology for snapshots",
        "observed_at": "AXIOM collection/metadata observation time",
        "created_at": "persistence creation time; not substituted for source chronology",
        "provider_timestamp": "retained only if present inside raw payload; not synthesized",
    }
    return result


def _identity_hashes(connection: sqlite3.Connection, tables: set[str]) -> dict[str, Any]:
    """Hash stable row identities without materialising large payload sets."""
    result: dict[str, Any] = {}
    if "polymarket_markets" in tables:
        for source in ("HISTORICAL", "FORWARD_COLLECTED"):
            digest = hashlib.sha256()
            count = 0
            for row in connection.execute(
                """
                SELECT market_id, observed_at, metadata_hash, source_type, created_at, payload_json
                FROM polymarket_markets WHERE upper(source_type)=?
                ORDER BY market_id, observed_at, metadata_hash
                """,
                (source,),
            ):
                digest.update(
                    _canonical(
                        [
                            row["market_id"],
                            row["observed_at"],
                            row["metadata_hash"],
                            row["source_type"],
                            row["created_at"],
                            row["payload_json"],
                        ]
                    ).encode("utf-8")
                )
                digest.update(b"\n")
                count += 1
            result[f"polymarket_markets_{source.lower()}"] = {
                "rows": count,
                "hash": "sha256:" + digest.hexdigest(),
            }
    if "polymarket_snapshots" in tables:
        for source in ("HISTORICAL", "FORWARD_COLLECTED"):
            digest = hashlib.sha256()
            count = 0
            for row in connection.execute(
                """
                SELECT snapshot_id, market_id, source_timestamp, observed_at,
                       quality, source_type, created_at, payload_json
                FROM polymarket_snapshots WHERE upper(source_type)=?
                ORDER BY market_id, source_timestamp, snapshot_id
                """,
                (source,),
            ):
                digest.update(
                    _canonical(
                        [
                            row["snapshot_id"],
                            row["market_id"],
                            row["source_timestamp"],
                            row["observed_at"],
                            row["quality"],
                            row["source_type"],
                            row["created_at"],
                            row["payload_json"],
                        ]
                    ).encode("utf-8")
                )
                digest.update(b"\n")
                count += 1
            result[f"polymarket_snapshots_{source.lower()}"] = {
                "rows": count,
                "hash": "sha256:" + digest.hexdigest(),
            }
    return result


def _choose_export_sequences(sequence_rows: Mapping[str, Mapping[str, Any]], max_rows: int) -> tuple[list[str], dict[str, Any]]:
    selected: list[str] = []
    remaining = max_rows
    skipped: list[str] = []
    partial_market: str | None = None
    for market_id in sorted(sequence_rows):
        rows = _safe_int(sequence_rows[market_id].get("rows"))
        if rows <= remaining and rows > 0:
            selected.append(market_id)
            remaining -= rows
        elif rows > remaining:
            skipped.append(market_id)
        if remaining == 0:
            break
    # Ensure a non-empty bounded export even when one market is larger than the
    # bound; in that case the prefix is contiguous and reproducible.
    if not selected and sequence_rows and max_rows > 0:
        market_id = sorted(sequence_rows)[0]
        selected = [market_id]
        partial_market = market_id
    return selected, {
        "selection_policy": "lexicographic market_id; whole same-market sequences until bound, otherwise first contiguous prefix",
        "max_snapshot_rows": max_rows,
        "selected_market_count": len(selected),
        "skipped_markets_due_to_bound": len(skipped),
        "skipped_market_ids_sample": skipped[:DEFAULT_SEQUENCE_SAMPLE_MARKETS],
        "partial_market_id": partial_market,
        "scope_is_reproducible": True,
    }


def _selected_snapshot_records(
    connection: sqlite3.Connection,
    source_type: str,
    selected_market_ids: Sequence[str],
    max_rows: int,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    if not selected_market_ids or not _has_table(connection, "polymarket_snapshots"):
        return records
    remaining = max_rows
    for start in range(0, len(selected_market_ids), 500):
        chunk = list(selected_market_ids[start : start + 500])
        placeholders = ",".join("?" for _ in chunk)
        for row in connection.execute(
            f"""
            SELECT snapshot_id, market_id, source_timestamp, observed_at,
                   quality, payload_json, created_at, source_type
            FROM polymarket_snapshots
            WHERE upper(source_type)=? AND market_id IN ({placeholders})
            ORDER BY market_id, source_timestamp, snapshot_id
            """,
            [source_type, *chunk],
        ):
            if remaining <= 0:
                return records
            raw_payload = _json(row["payload_json"], {})
            payload = _redact(raw_payload)
            records.append(
                {
                    "record_type": "snapshot",
                    "source_type": str(row["source_type"] or source_type).upper(),
                    "provider": _payload_provider(payload) if isinstance(payload, Mapping) else "(missing)",
                    "snapshot_id": row["snapshot_id"],
                    "market_id": str(row["market_id"]),
                    "source_timestamp": row["source_timestamp"],
                    "observed_at": row["observed_at"],
                    "quality": row["quality"],
                    "created_at": row["created_at"],
                    "payload": payload,
                }
            )
            remaining -= 1
        if remaining <= 0:
            return records
    return records


def _metadata_records_for_snapshots(
    connection: sqlite3.Connection,
    source_type: str,
    selected_market_ids: Sequence[str],
    snapshots: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Select metadata at or before snapshot anchors, never future metadata."""
    if not selected_market_ids or not snapshots or not _has_table(connection, "polymarket_markets"):
        return []
    anchors: dict[str, list[tuple[str, str]]] = defaultdict(list)
    by_market: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for snapshot in snapshots:
        market_id = str(snapshot["market_id"])
        observed = str(snapshot.get("observed_at") or "")
        if observed:
            by_market[market_id].append(snapshot)
    for market_id, rows in by_market.items():
        ordered = sorted(rows, key=lambda row: (_parse_time(row.get("observed_at")) or datetime.min.replace(tzinfo=timezone.utc), str(row.get("snapshot_id", ""))))
        for label, row in (("first_snapshot_observed_at", ordered[0]), ("last_snapshot_observed_at", ordered[-1])):
            anchor = str(row.get("observed_at") or "")
            if anchor and (anchor, label) not in anchors[market_id]:
                anchors[market_id].append((anchor, label))
    metadata_by_market: dict[str, list[sqlite3.Row]] = defaultdict(list)
    for start in range(0, len(selected_market_ids), 500):
        chunk = list(selected_market_ids[start : start + 500])
        placeholders = ",".join("?" for _ in chunk)
        for row in connection.execute(
            f"""
            SELECT market_id, observed_at, metadata_hash, payload_json, created_at, source_type
            FROM polymarket_markets
            WHERE upper(source_type)=? AND market_id IN ({placeholders})
            ORDER BY market_id, observed_at, metadata_hash
            """,
            [source_type, *chunk],
        ):
            metadata_by_market[str(row["market_id"])].append(row)
    selected: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for market_id in sorted(anchors):
        candidates = metadata_by_market.get(market_id, [])
        for anchor, label in sorted(anchors[market_id], key=lambda item: (_parse_time(item[0]) or datetime.min.replace(tzinfo=timezone.utc), item[1])):
            eligible = [
                row
                for row in candidates
                if (_parse_time(row["observed_at"]) or datetime.min.replace(tzinfo=timezone.utc))
                <= (_parse_time(anchor) or datetime.max.replace(tzinfo=timezone.utc))
            ]
            if not eligible:
                continue
            row = max(
                eligible,
                key=lambda item: (
                    _parse_time(item["observed_at"]) or datetime.min.replace(tzinfo=timezone.utc),
                    str(item["metadata_hash"]),
                ),
            )
            identity = (market_id, str(row["observed_at"]), str(row["metadata_hash"]))
            if identity in seen:
                continue
            seen.add(identity)
            payload = _redact(_json(row["payload_json"], {}))
            selected.append(
                {
                    "record_type": "market_metadata",
                    "source_type": str(row["source_type"] or source_type).upper(),
                    "provider": _payload_provider(payload) if isinstance(payload, Mapping) else "(missing)",
                    "market_id": market_id,
                    "observed_at": row["observed_at"],
                    "metadata_hash": row["metadata_hash"],
                    "created_at": row["created_at"],
                    "metadata_alignment": {
                        "anchor": label,
                        "anchor_snapshot_observed_at": anchor,
                        "metadata_observed_at_is_prior_or_equal": True,
                    },
                    "payload": payload,
                }
            )
    return selected


def _write_json_line(handle: Any, digest: Any, record: Mapping[str, Any]) -> None:
    line = _canonical(record) + "\n"
    handle.write(line)
    digest.update(line.encode("utf-8"))


def _write_export(
    connection: sqlite3.Connection,
    export_path: Path,
    selected_market_ids_by_source: Mapping[str, Sequence[str]],
    scopes_by_source: Mapping[str, Mapping[str, Any]],
    max_rows: int,
    source_db: Path,
    source_identity: Mapping[str, Any],
) -> dict[str, Any]:
    export_path.parent.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256()
    partition_result: dict[str, Any] = {}
    all_snapshot_rows = 0
    all_metadata_rows = 0
    all_redacted_values = 0
    with export_path.open("w", encoding="utf-8", newline="\n") as handle:
        manifest = {
            "record_type": "manifest",
            "export_schema_version": EXPORT_SCHEMA_VERSION,
            "source_type": "MIXED_TYPED_PARTITIONS",
            "research_only": True,
            "historical_research_partition": "HISTORICAL",
            "forward_records_are_replay_model_evidence_only": True,
            "source_database": str(source_db),
            "source_identity_hash": source_identity.get("combined_hash"),
            "partitions": {
                source_type: {
                    "source_type": source_type,
                    "research_only": source_type == "HISTORICAL",
                    "forward_evidence_only": source_type == "FORWARD_COLLECTED",
                    "selection": dict(scope),
                    "selected_market_ids_sample": list(selected_market_ids_by_source.get(source_type, ()))[:DEFAULT_SEQUENCE_SAMPLE_MARKETS],
                    "selected_market_ids_hash": _hash_json(list(selected_market_ids_by_source.get(source_type, ()))),
                }
                for source_type, scope in scopes_by_source.items()
            },
            "provenance": "polymarket_markets/polymarket_snapshots persisted columns; metadata anchors are observed_at <= selected snapshot anchors; raw timestamps preserved",
            "credential_control_enable_tables_exported": False,
        }
        _write_json_line(handle, digest, manifest)
        for source_type in ("HISTORICAL", "FORWARD_COLLECTED"):
            selected_ids = list(selected_market_ids_by_source.get(source_type, ()))
            snapshots = _selected_snapshot_records(connection, source_type, selected_ids, max_rows)
            metadata = _metadata_records_for_snapshots(connection, source_type, selected_ids, snapshots)
            for record in metadata:
                _write_json_line(handle, digest, record)
            for record in snapshots:
                _write_json_line(handle, digest, record)
            partition_result[source_type] = {
                "source_type": source_type,
                "research_only": source_type == "HISTORICAL",
                "forward_evidence_only": source_type == "FORWARD_COLLECTED",
                "selected_market_count": len(selected_ids),
                "selected_market_ids_sample": selected_ids[:DEFAULT_SEQUENCE_SAMPLE_MARKETS],
                "selected_market_ids_hash": _hash_json(selected_ids),
                "market_metadata_records": len(metadata),
                "snapshot_records": len(snapshots),
                "metadata_alignment": "each record is the latest persisted metadata at or before first/last selected snapshot observed_at; absent prior metadata is omitted",
            }
            all_snapshot_rows += len(snapshots)
            all_metadata_rows += len(metadata)
    file_stat = _stat(export_path) or {}
    return {
        "path": str(export_path),
        "format": "JSONL",
        "schema_version": EXPORT_SCHEMA_VERSION,
        "research_only": True,
        "source_type": "MIXED_TYPED_PARTITIONS",
        "partitions": partition_result,
        "market_metadata_records": all_metadata_rows,
        "snapshot_records": all_snapshot_rows,
        "manifest_records": 1,
        "total_records": all_metadata_rows + all_snapshot_rows + 1,
        "max_snapshot_rows_per_partition": max_rows,
        "sha256": "sha256:" + digest.hexdigest(),
        "bytes": file_stat.get("size_bytes"),
        "redacted_payload_records": all_redacted_values,
        "credential_control_enable_tables_exported": False,
        "bounded": True,
    }


def _collection_coverage(connection: sqlite3.Connection, tables: set[str]) -> dict[str, Any]:
    result: dict[str, Any] = {"classification": "exact aggregate; payload details excluded"}
    if "collection_cycles" in tables:
        row = connection.execute("SELECT COUNT(*) rows, MIN(started_at) first_started_at, MAX(ended_at) last_ended_at FROM collection_cycles").fetchone()
        result["cycles"] = {"rows": int(row["rows"]), "first_started_at": row["first_started_at"], "last_ended_at": row["last_ended_at"]}
    if "collection_errors" in tables:
        rows = connection.execute(
            "SELECT source_type, kind, COUNT(*) rows, COUNT(DISTINCT market_id) markets, MIN(observed_at) first_observed_at, MAX(observed_at) last_observed_at FROM collection_errors GROUP BY source_type, kind ORDER BY source_type, kind"
        ).fetchall()
        result["errors_by_source_and_kind"] = [
            {"source_type": str(row["source_type"] or "").upper(), "kind": str(row["kind"]), "rows": int(row["rows"]), "markets": int(row["markets"]), "first_observed_at": row["first_observed_at"], "last_observed_at": row["last_observed_at"]}
            for row in rows
        ]
    if "collector_state" in tables:
        row = connection.execute("SELECT COUNT(*) rows, MIN(updated_at) first_updated_at, MAX(updated_at) last_updated_at FROM collector_state").fetchone()
        result["collector_state"] = {"rows": int(row["rows"]), "first_updated_at": row["first_updated_at"], "last_updated_at": row["last_updated_at"]}
    return result


def build_inventory(
    source_db: Path | str = DEFAULT_SOURCE_DB,
    report_path: Path | str = DEFAULT_REPORT,
    export_path: Path | str = DEFAULT_EXPORT,
    *,
    max_export_rows: int = DEFAULT_EXPORT_ROWS,
    sequence_sample_markets: int = DEFAULT_SEQUENCE_SAMPLE_MARKETS,
) -> dict[str, Any]:
    if isinstance(max_export_rows, bool) or not isinstance(max_export_rows, int) or max_export_rows < 1:
        raise ValueError("max_export_rows must be a positive integer")
    if isinstance(sequence_sample_markets, bool) or not isinstance(sequence_sample_markets, int) or sequence_sample_markets < 1:
        raise ValueError("sequence_sample_markets must be a positive integer")
    source = Path(source_db).expanduser().resolve()
    report = Path(report_path).expanduser().resolve()
    export = Path(export_path).expanduser().resolve()
    started_at = _utc_now()
    source_stat_pre = _source_files(source)
    connection: sqlite3.Connection | None = None
    try:
        connection = _connect_read_only(source)
        tables = _table_names(connection)
        schema = _schema_inventory(connection, tables)
        source_schema_hash = _hash_json(schema)
        counts = _exact_table_counts(connection, tables)
        catalog = _catalog_coverage(connection, tables)
        attestation = _attestation_coverage(connection, tables)
        partitions = {source_type: _partition_aggregate(connection, source_type) for source_type in ("HISTORICAL", "FORWARD_COLLECTED")}
        latest = {source_type: _latest_markets(connection, source_type) for source_type in ("HISTORICAL", "FORWARD_COLLECTED")}
        sequence_exact: dict[str, Any] = {}
        sequences: dict[str, dict[str, dict[str, Any]]] = {}
        lifecycle: dict[str, Any] = {}
        for source_type in ("HISTORICAL", "FORWARD_COLLECTED"):
            sequence_exact[source_type], sequences[source_type] = _sequence_stats(connection, source_type)
            lifecycle[source_type] = _lifecycle_as_of(connection, source_type, latest[source_type], sequences[source_type])
        provenance = _provenance(connection, tables, latest)
        identity_hashes = _identity_hashes(connection, tables)
        combined_hash = _hash_json(identity_hashes)
        identity_hashes["combined_hash"] = combined_hash
        historical_ids, historical_scope = _choose_export_sequences(sequences["HISTORICAL"], max_export_rows)
        forward_ids, forward_scope = _choose_export_sequences(sequences["FORWARD_COLLECTED"], max_export_rows)
        export_result = _write_export(
            connection,
            export,
            {"HISTORICAL": historical_ids, "FORWARD_COLLECTED": forward_ids},
            {"HISTORICAL": historical_scope, "FORWARD_COLLECTED": forward_scope},
            max_export_rows,
            source,
            {**identity_hashes, "combined_hash": combined_hash},
        )
        report_data: dict[str, Any] = {
            "schema_version": SCHEMA_VERSION,
            "generated_at": started_at,
            "completed_at": _utc_now(),
            "scope": {
                "purpose": "release baseline research-data evidence and forward-coverage inventory",
                "allowed_source_types": ["HISTORICAL", "FORWARD_COLLECTED"],
                "research_dataset": "HISTORICAL Polymarket price/depth records as persisted; no synthetic fills",
                "forward_dataset": "FORWARD_COLLECTED market/snapshot coverage; never relabeled as historical",
                "aggregate_classification": "exact over reachable rows in one SQLite read snapshot",
                "sequence_detail_classification": "exact aggregate; bounded illustrative per-market sample",
                "export_classification": "bounded deterministic typed HISTORICAL research + FORWARD_COLLECTED replay-evidence sample",
                "sequence_sample_markets": sequence_sample_markets,
                "max_export_snapshot_rows_per_partition": max_export_rows,
            },
            "source": {
                "database": str(source),
                "read_only_uri": True,
                "sqlite_immutable_uri_used": False,
                "query_only": True,
                "one_consistent_read_transaction": True,
                "transaction": "BEGIN DEFERRED before application SELECTs; rollback after export/report facts",
                "sqlite_version": sqlite3.sqlite_version,
                "source_stat_pre": source_stat_pre,
                "source_schema_hash": source_schema_hash,
                "source_identity_hashes": identity_hashes,
            },
            "schema": schema,
            "table_counts": counts,
            "research_dataset": {
                "classification": "exact",
                "catalog": {key: value for key, value in catalog.items() if key != "historical_prediction_catalog"},
                "historical_prediction_catalog_rows": len(catalog.get("historical_prediction_catalog", [])),
                "historical_prediction_catalog_identity_hash": _hash_json(catalog.get("historical_prediction_catalog", [])),
                "integrity_attestation": attestation,
                "immutability": _historical_immutability(connection, tables),
                "coverage": partitions["HISTORICAL"],
            },
            "forward_market_snapshot": {
                "classification": "exact",
                "coverage": partitions["FORWARD_COLLECTED"],
                "quality_is_persisted": True,
                "trades": "No trade rows are present in the reachable source partition" if counts.get("polymarket_trades", {}).get("rows") == 0 else "Trade rows are inventoried separately; not exported",
            },
            "provenance": provenance,
            "same_market_sequences": {
                "classification": "exact aggregate; sampled detail",
                "definition": "Snapshots ordered by source_timestamp,snapshot_id within market; observed_at monotonicity checked in that order",
                "long_gap_definition": f"source_timestamp transition greater than {LONG_GAP_SECONDS:g} seconds",
                "partitions": {
                    source_type: {
                        **exact,
                        "illustrative_sequences": [
                            {**sequences[source_type][market_id], "market_id": market_id}
                            for market_id in sorted(sequences[source_type])[:sequence_sample_markets]
                        ],
                        "illustrative_sequences_classification": "sampled lexicographic market_id prefix",
                    }
                    for source_type, exact in sequence_exact.items()
                },
            },
            "lifecycle_as_of": lifecycle,
            "collection_coverage": _collection_coverage(connection, tables),
            "export": export_result,
            "caveats": [
                "Forward rows are observed market/snapshot evidence, not historical research records and not executable quotes or fills.",
                "Historical Polymarket rows are PRICE_PROXY when persisted quality says PRICE_PROXY; no depth, spread, settlement, or fill is fabricated.",
                "Aggregate counts and hashes are exact for rows reachable in the single read transaction; illustrative per-market rows and export are bounded.",
                "Lifecycle status is a latest-metadata as-of projection only; contemporaneous metadata coverage is reported separately using metadata.observed_at <= snapshot.observed_at and is not inferred from future/latest rows.",
                "Missing metadata/snapshot and sequence gaps are storage-coverage gaps, not claims about provider availability outside the persisted source.",
                "Source file/WAL stat changes during the read are reported; the transaction still provides one SQLite snapshot to this connection.",
            ],
        }
        source_stat_post = _source_files(source)
        report_data["source"]["source_stat_post"] = source_stat_post
        report_data["source"]["database_or_wal_changed_during_read"] = (
            source_stat_pre.get("database") != source_stat_post.get("database")
            or source_stat_pre.get("wal") != source_stat_post.get("wal")
        )
        report_data["source"]["shm_metadata_changed_during_read"] = source_stat_pre.get("shm") != source_stat_post.get("shm")
        report_data["source"]["source_changed_during_read"] = report_data["source"]["database_or_wal_changed_during_read"]
        report_data["report_identity_hash"] = _hash_json(report_data)
    finally:
        if connection is not None:
            try:
                connection.rollback()
            finally:
                connection.close()
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text(_canonical(report_data) + "\n", encoding="utf-8")
    return report_data


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", "--source-db", dest="source_db", type=Path, default=DEFAULT_SOURCE_DB)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--export", type=Path, default=DEFAULT_EXPORT)
    parser.add_argument("--max-export-rows", type=int, default=DEFAULT_EXPORT_ROWS)
    parser.add_argument("--sequence-sample-markets", type=int, default=DEFAULT_SEQUENCE_SAMPLE_MARKETS)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        report = build_inventory(
            args.source_db,
            args.report,
            args.export,
            max_export_rows=args.max_export_rows,
            sequence_sample_markets=args.sequence_sample_markets,
        )
    except (OSError, RuntimeError, sqlite3.Error, ValueError) as exc:
        print(f"polymarket release data inventory failed: {exc}", file=sys.stderr)
        return 2
    print(
        json.dumps(
            {
                "report": str(Path(args.report).resolve()),
                "report_identity_hash": report.get("report_identity_hash"),
                "export": report.get("export"),
                "source_changed_during_read": report.get("source", {}).get("source_changed_during_read"),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
