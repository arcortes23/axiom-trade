"""Bounded, isolated, public-data release smoke for the Polymarket path.

The command consumes one immutable research export and delegates research,
paper observation, scope, evaluation, and feasibility to the application's
canonical services.  It never opens the protected database or credentials and
never submits an order.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import logging
import math
import os
from pathlib import Path
import sys
import time
from typing import Any, Mapping, Sequence

RELEASE_ROOT = Path("C:/Users/User/projects/axiom-polymarket-release")
PROTECTED_ROOT = Path("C:/Users/User/projects/axiom")
DEFAULT_EXPORT = RELEASE_ROOT / "runtime-data" / "polymarket_release_research_export.jsonl"
DEFAULT_REPORT = RELEASE_ROOT / "reports" / "polymarket_release_smoke.json"
DEFAULT_DATABASE = RELEASE_ROOT / "runtime-data" / "polymarket_release_smoke.sqlite"
DEFAULT_LOG = RELEASE_ROOT / "runtime-data" / "polymarket_release_smoke.log"
EXPORT_SCHEMA = "polymarket-release-research-export-v1"
SMOKE_SCHEMA = "polymarket-release-smoke-v1"
MAX_EXPORT_LINES = 20_000
MAX_HISTORICAL_ROWS = 4_096
MAX_FORWARD_ROWS = 4_096
MAX_CURRENT_MARKETS = 4
MAX_TIMEOUT = 10.0
MAX_CANDIDATES = 1_000


class SmokeBlocked(RuntimeError):
    """A fail-closed smoke precondition or canonical service outcome."""


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: Any) -> str | None:
    if isinstance(value, datetime):
        stamp = value
    else:
        text = str(value or "").strip()
        if not text:
            return None
        try:
            stamp = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except (TypeError, ValueError):
            return None
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=timezone.utc)
    return stamp.astimezone(timezone.utc).isoformat()


def _parse_time(value: Any) -> datetime | None:
    encoded = _iso(value)
    return datetime.fromisoformat(encoded) if encoded is not None else None


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False, default=str)


def _hash_json(value: Any) -> str:
    return "sha256:" + hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def _safe_error(exc: BaseException) -> str:
    text = str(exc).replace("\r", " ").replace("\n", " ").strip()
    return text[:400] or type(exc).__name__


def _bounded(value: Any, *, depth: int = 0, max_items: int = 32) -> Any:
    """Keep service evidence finite without copying provider payloads wholesale."""
    if depth > 4:
        return "<bounded>"
    if isinstance(value, Mapping):
        return {
            str(key): _bounded(child, depth=depth + 1, max_items=max_items)
            for key, child in list(value.items())[:max_items]
        }
    if isinstance(value, (list, tuple)):
        return [_bounded(child, depth=depth + 1, max_items=max_items) for child in list(value)[:max_items]]
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _resolve_under_root(value: str | Path, *, label: str) -> Path:
    path = Path(value)
    if not path.is_absolute():
        path = RELEASE_ROOT / path
    resolved = path.resolve()
    root = RELEASE_ROOT.resolve()
    protected = PROTECTED_ROOT.resolve()
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise SmokeBlocked(f"{label} must be under release root: {resolved}") from exc
    try:
        resolved.relative_to(protected)
    except ValueError:
        return resolved
    raise SmokeBlocked(f"{label} is under protected live root")


def _guard_paths(args: argparse.Namespace) -> dict[str, Path]:
    actual_root = Path(__file__).resolve().parents[1]
    if actual_root != RELEASE_ROOT.resolve():
        raise SmokeBlocked(f"smoke script root is not release root: {actual_root}")
    if str(os.environ.get("AXIOM_EXECUTION_PROFILE", "")).strip().lower() != "isolated":
        raise SmokeBlocked("AXIOM_EXECUTION_PROFILE=isolated is required")
    paths = {
        "export": _resolve_under_root(args.export, label="export"),
        "report": _resolve_under_root(args.report, label="report"),
        "database": _resolve_under_root(args.output_db, label="output database"),
        "log": _resolve_under_root(args.output_log, label="output log"),
    }
    if paths["database"] in {paths["log"], paths["export"], paths["report"]} or paths["log"] == paths["report"]:
        raise SmokeBlocked("output database, log, report, and export must be distinct")
    for path in paths.values():
        path.parent.mkdir(parents=True, exist_ok=True)
    return paths


def _new_stages() -> list[dict[str, Any]]:
    return [
        {"name": name, "status": "NOT_RUN", "reason": None}
        for name in (
            "data",
            "candidate",
            "assessment",
            "current_frozen_policy_resolution",
            "fresh_inputs",
            "paper_observation",
            "actual_canonical_decision",
            "execution_feasibility",
        )
    ]


def _set_stage(
    stages: list[dict[str, Any]],
    name: str,
    status: str,
    reason: str | None = None,
    **evidence: Any,
) -> None:
    row = next(item for item in stages if item["name"] == name)
    row["status"] = status
    row["reason"] = reason
    row.update({key: _bounded(value) for key, value in evidence.items()})


def _empty_report(paths: Mapping[str, Path], *, status: str = "NOT_RUN") -> dict[str, Any]:
    return {
        "schema_version": SMOKE_SCHEMA,
        "status": status,
        "run_mode": "bounded_isolated_public_read_only",
        "release_root": str(RELEASE_ROOT),
        "isolation": {
            "execution_profile": os.environ.get("AXIOM_EXECUTION_PROFILE") or "NOT_SET",
            "required_execution_profile": "isolated",
            "protected_live_root": str(PROTECTED_ROOT),
            "source_export": str(paths.get("export", DEFAULT_EXPORT)),
            "output_database": str(paths.get("database", DEFAULT_DATABASE)),
            "output_log": str(paths.get("log", DEFAULT_LOG)),
            "live_database_accessed": False,
            "credentials_read": False,
            "orders_submitted": False,
            "account_mutation": False,
        },
        "command_contract": {
            "max_export_lines": MAX_EXPORT_LINES,
            "max_historical_rows": MAX_HISTORICAL_ROWS,
            "max_forward_rows": MAX_FORWARD_ROWS,
            "max_current_markets": MAX_CURRENT_MARKETS,
            "paper_only": True,
            "holdout_used_for_selection": False,
            "canonical_research": "axiom.autonomous.AutonomousResearchProcessor.process_pending",
            "canonical_paper": "axiom.node.ResearchNode._run_paper_workers",
            "canonical_evaluator": "axiom.canary.CanaryService.evaluate_signal",
            "canonical_feasibility": "axiom.canary.CanaryService.evaluate_signal",
            "canonical_order": "process_pending -> public_collector -> paper_observation -> canary_evaluator",
            "public_collector": "axiom.collector.PolymarketCollector",
            "public_provider": "axiom.data.polymarket.PolymarketAdapter",
        },
        "source_export": {
            "path": str(paths.get("export", DEFAULT_EXPORT)),
            "schema_version": EXPORT_SCHEMA,
            "status": "NOT_RUN",
            "sha256": None,
            "line_count": None,
            "counts": {},
            "timestamps": {},
        },
        "plan": {
            "status": "PREDECLARED_BY_AUTONOMOUS_SERVICE",
            "dataset_id": None,
            "dataset_version": None,
            "trial_count": 0,
            "plan_hashes": [],
            "assumptions": None,
            "exit_policy": None,
            "holdout_used_for_selection": False,
        },
        "stages": _new_stages(),
        "strategy_evidence": {
            "status": "NOT_RUN",
            "classification": "RESEARCH_ONLY_PROXY_OR_REPLAY",
            "historical_trials": [],
            "recorded_book_replay": {
                "status": "NOT_RUN",
                "research_mode": "RECORDED_BOOK_REPLAY",
                "assumption_version": "recorded-book-replay-v1",
                "trials": [],
            },
            "assessment": "NOT_RUN",
            "holdout_used_for_selection": False,
        },
        "candidate": {"status": "NOT_RUN", "candidate_ids": [], "candidate_stages": []},
        "current_data": {
            "status": "NOT_RUN",
            "source_type": "FORWARD_COLLECTED",
            "authenticated": False,
            "observed_vs_replay": "NOT_RUN",
        },
        "paper_observation": {
            "status": "NOT_RUN",
            "normal_service": "ResearchNode._run_paper_workers",
            "cycle": None,
            "paper_only": True,
        },
        "evaluation": {
            "actual_current_evaluator_ran": False,
            "actual_current_evaluator": "CanaryService.evaluate_signal",
            "result": None,
        },
        "qualification": {
            "status": "NOT_RUN",
            "ready": False,
            "canonical": {},
        },
        "decision": None,
        "feasibility": {"status": "NOT_RUN", "reason": None},
        "software_path": {
            "status": "NOT_RUN",
            "provider": "PolymarketAdapter",
            "collector": "PolymarketCollector",
            "public_transport_called": False,
        },
        "read_only_auth": {"status": "NOT_RUN", "transport_called": False},
        "live_fills": {"status": "NOT_VERIFIED", "observed_count": None},
        "provenance": {
            "export_sha256": None,
            "historical_projection_hash": None,
            "forward_projection_hash": None,
            "historical_raw_provenance_preserved": False,
            "metadata_as_of_rule": "forward metadata is never used by replay; any accepted metadata is prior/equal to its snapshot anchor",
        },
        "blockers": ["SMOKE_NOT_RUN_UNTIL_MAIN_GATE"],
        "assumptions": [
            "The export is the only historical input; the protected live database is never opened.",
            "Historical rows retain source snapshot and raw-record hashes from the immutable export.",
            "Forward replay consumes only timestamped recorded books and never future metadata.",
            "All current observation and canary decisions remain paper-only; live fills are NOT_VERIFIED.",
        ],
    }


def _read_export(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise SmokeBlocked(f"research export is missing: {path}")
    file_hash = _hash_file(path)
    partitions: dict[str, dict[str, Any]] = {
        "HISTORICAL": {"snapshots": [], "markets": []},
        "FORWARD_COLLECTED": {"snapshots": [], "markets": []},
    }
    manifest: Mapping[str, Any] | None = None
    line_count = 0
    with path.open("r", encoding="utf-8") as handle:
        for line_count, line in enumerate(handle, 1):
            if line_count > MAX_EXPORT_LINES:
                raise SmokeBlocked(f"export exceeds bounded line count {MAX_EXPORT_LINES}")
            try:
                record = json.loads(line)
            except json.JSONDecodeError as exc:
                raise SmokeBlocked(f"export line {line_count} is not JSON") from exc
            if not isinstance(record, Mapping):
                raise SmokeBlocked(f"export line {line_count} is not an object")
            if line_count == 1:
                if str(record.get("record_type", "")).lower() != "manifest":
                    raise SmokeBlocked("export manifest must be the first line")
                manifest = record
                if record.get("export_schema_version") != EXPORT_SCHEMA:
                    raise SmokeBlocked("unsupported research export schema")
                if record.get("credential_control_enable_tables_exported") is not False:
                    raise SmokeBlocked("credential/control tables are not allowed in export")
                partition_manifest = record.get("partitions")
                if not isinstance(partition_manifest, Mapping):
                    raise SmokeBlocked("export partition manifest is missing")
                historical_manifest = partition_manifest.get("HISTORICAL")
                forward_manifest = partition_manifest.get("FORWARD_COLLECTED")
                if (
                    not isinstance(historical_manifest, Mapping)
                    or historical_manifest.get("research_only") is not True
                    or not isinstance(forward_manifest, Mapping)
                    or forward_manifest.get("forward_evidence_only") is not True
                    or forward_manifest.get("research_only") is not False
                ):
                    raise SmokeBlocked("export partition controls are invalid")
                continue
            source = str(record.get("source_type", "")).strip().upper()
            if source not in partitions:
                raise SmokeBlocked(f"unknown export source partition at line {line_count}")
            record_type = str(record.get("record_type", "")).strip().lower()
            if record_type not in {"snapshot", "market_metadata"}:
                raise SmokeBlocked(f"unsupported export record type at line {line_count}")
            payload = record.get("payload")
            if not isinstance(payload, Mapping):
                raise SmokeBlocked(f"export payload is not an object at line {line_count}")
            market_id = str(record.get("market_id") or payload.get("market_id") or "").strip()
            if not market_id:
                raise SmokeBlocked(f"export record has no market id at line {line_count}")
            raw_record_hash = _hash_json(record)
            observed_at = _parse_time(record.get("observed_at"))
            if observed_at is None:
                raise SmokeBlocked(f"export record has no valid observed_at at line {line_count}")
            if record_type == "market_metadata":
                alignment = record.get("metadata_alignment")
                if not isinstance(alignment, Mapping) or alignment.get("metadata_observed_at_is_prior_or_equal") is not True:
                    raise SmokeBlocked("forward metadata lacks prior/equal snapshot-anchor evidence")
                anchor = _parse_time(alignment.get("anchor_snapshot_observed_at"))
                if anchor is None or observed_at > anchor:
                    raise SmokeBlocked("forward metadata is newer than its selected snapshot anchor")
                partitions[source]["markets"].append(
                    {
                        "market_id": market_id,
                        "observed_at": observed_at.isoformat(),
                        "metadata_hash": str(record.get("metadata_hash") or ""),
                        "anchor_snapshot_observed_at": anchor.isoformat(),
                        "provider": str(record.get("provider") or payload.get("provider") or ""),
                        "raw_record_hash": raw_record_hash,
                    }
                )
                continue
            source_timestamp = _parse_time(record.get("source_timestamp")) or _parse_time(payload.get("timestamp"))
            if source_timestamp is None:
                raise SmokeBlocked(f"snapshot has no valid source timestamp at line {line_count}")
            partitions[source]["snapshots"].append(
                {
                    "snapshot_id": str(record.get("snapshot_id") or ""),
                    "market_id": market_id,
                    "source_timestamp": source_timestamp.isoformat(),
                    "observed_at": observed_at.isoformat(),
                    "created_at": _iso(record.get("created_at")),
                    "provider": str(record.get("provider") or payload.get("provider") or ""),
                    "quality": str(record.get("quality") or payload.get("research_quality") or "UNKNOWN").upper(),
                    "payload": dict(payload),
                    "raw_record_hash": raw_record_hash,
                }
            )
    if manifest is None:
        raise SmokeBlocked("export manifest is missing")
    if not partitions["HISTORICAL"]["snapshots"]:
        raise SmokeBlocked("historical research partition is empty")
    if len(partitions["HISTORICAL"]["snapshots"]) > MAX_HISTORICAL_ROWS:
        raise SmokeBlocked("historical snapshot bound exceeded")
    if len(partitions["FORWARD_COLLECTED"]["snapshots"]) > MAX_FORWARD_ROWS:
        raise SmokeBlocked("forward snapshot bound exceeded")
    historical: list[dict[str, Any]] = []
    for row in partitions["HISTORICAL"]["snapshots"]:
        payload = row["payload"]
        raw_price = payload.get("yes_mid", payload.get("price"))
        try:
            price = float(raw_price)
        except (TypeError, ValueError, OverflowError):
            continue
        if not math.isfinite(price) or not 0.0 <= price <= 1.0:
            continue
        token_id = str(payload.get("token_id") or payload.get("yes_token_id") or row["market_id"]).strip()
        historical.append(
            {
                "timestamp": row["source_timestamp"],
                "source_timestamp": row["source_timestamp"],
                "observed_at": row["observed_at"],
                "market_id": row["market_id"],
                "token_id": token_id,
                "yes_mid": price,
                "price": price,
                "source_type": "HISTORICAL",
                "source_snapshot_id": row["snapshot_id"],
                "source_record_hash": row["raw_record_hash"],
                "provider": row["provider"],
                "quality": row["quality"],
            }
        )
    if not historical:
        raise SmokeBlocked("historical partition has no valid price observations")
    historical.sort(key=lambda item: (item["timestamp"], item["market_id"], item["token_id"]))
    forward = sorted(
        partitions["FORWARD_COLLECTED"]["snapshots"],
        key=lambda item: (item["observed_at"], item["market_id"], item["source_timestamp"], item["snapshot_id"]),
    )
    count_by_source = {
        source: {
            "snapshot_records": len(value["snapshots"]),
            "market_metadata_records": len(value["markets"]),
            "market_ids": len({row["market_id"] for group in value.values() for row in group}),
            "quality": dict(Counter(row.get("quality", "UNKNOWN") for row in value["snapshots"])),
        }
        for source, value in partitions.items()
    }
    timestamp_summary: dict[str, dict[str, str | None]] = {}
    for source, value in partitions.items():
        stamps = [
            parsed
            for row in value["snapshots"]
            for parsed in (_parse_time(row.get("source_timestamp")), _parse_time(row.get("observed_at")))
            if parsed is not None
        ]
        timestamp_summary[source] = {
            "earliest": min(stamps).isoformat() if stamps else None,
            "latest": max(stamps).isoformat() if stamps else None,
        }
    return {
        "manifest": dict(manifest),
        "file_hash": file_hash,
        "line_count": line_count,
        "partitions": partitions,
        "historical_rows": historical,
        "forward_rows": forward,
        "counts": count_by_source,
        "timestamps": timestamp_summary,
        "historical_projection_hash": _hash_json(historical),
        "forward_projection_hash": _hash_json(forward),
    }


def _identity_version(rows: Sequence[Mapping[str, Any]]) -> str:
    identities = [
        {
            "timestamp": str(row.get("source_timestamp") or row.get("timestamp")),
            "price": float(row.get("price", row.get("yes_mid"))),
            "token_id": str(row.get("token_id") or row.get("market_id")),
            "source_snapshot_id": str(row.get("source_snapshot_id") or ""),
            "source_record_hash": str(row.get("source_record_hash") or ""),
        }
        for row in rows
    ]
    return _hash_json(identities)


def _persist_historical_dataset(store: Any, export: Mapping[str, Any]) -> dict[str, Any]:
    persisted_at = _utc_now()
    rows = list(export["historical_rows"])
    dataset_id = "Polymarket-historical"
    aggregate_version = _hash_json(rows)
    by_market: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_market.setdefault(str(row["market_id"]), []).append(dict(row))
    common_metadata = {
        "source_type": "HISTORICAL",
        "provider": "polymarket",
        "instrument": "POLYMARKET",
        "research_quality": "PRICE_PROXY",
        "historical_order_book_available": False,
        "provenance_version": "dataset-provenance-v1",
        "policy_version": "prediction-integrity-v1",
        "source_export_sha256": export["file_hash"],
        "source_export_schema": EXPORT_SCHEMA,
        "source_export_line_count": export["line_count"],
        "raw_export_provenance": True,
    }
    market_versions: list[dict[str, Any]] = []
    for market_id in sorted(by_market):
        market_rows = by_market[market_id]
        version = _identity_version(market_rows)
        constituent_id = f"prediction:{market_id}"
        metadata = {**common_metadata, "market_id": market_id}
        stamps = [_parse_time(item["source_timestamp"]) for item in market_rows]
        start, end = min(stamps), max(stamps)
        if store.load_dataset(constituent_id, version) is None:
            store.save_dataset(constituent_id, version, market_rows, metadata=metadata, quality="PRICE_PROXY")
        store.save_dataset_catalog(
            constituent_id,
            version,
            provider="polymarket",
            instrument="POLYMARKET",
            market_type="prediction",
            timeframe="event_snapshots",
            start_timestamp=start,
            end_timestamp=end,
            row_count=len(market_rows),
            completeness=1.0,
            quality="PRICE_PROXY",
            source_type="HISTORICAL",
            snapshot_id="export:" + export["file_hash"][7:31],
            created_at=persisted_at,
            updated_at=persisted_at,
        )
        market_versions.append(
            {"market_id": market_id, "dataset_id": constituent_id, "dataset_version": version, "row_count": len(market_rows)}
        )
    stamps = [_parse_time(item["source_timestamp"]) for item in rows]
    aggregate_metadata = {**common_metadata, "market_versions": market_versions}
    if store.load_dataset(dataset_id, aggregate_version) is None:
        store.save_dataset(dataset_id, aggregate_version, rows, metadata=aggregate_metadata, quality="PRICE_PROXY")
    store.save_dataset_catalog(
        dataset_id,
        aggregate_version,
        provider="polymarket",
        instrument="POLYMARKET",
        market_type="prediction",
        timeframe="event_snapshots",
        start_timestamp=min(stamps),
        end_timestamp=max(stamps),
        row_count=len(rows),
        completeness=1.0,
        quality="PRICE_PROXY",
        source_type="HISTORICAL",
        snapshot_id="export:" + export["file_hash"][7:31],
        created_at=persisted_at,
        updated_at=persisted_at,
    )
    attestation = store.verify_dataset_integrity_attestation(dataset_id, aggregate_version, force=True)
    return {
        "dataset_id": dataset_id,
        "dataset_version": aggregate_version,
        "row_count": len(rows),
        "market_count": len(by_market),
        "market_versions": market_versions,
        "attestation": _bounded(attestation),
        "attestation_hash": attestation.get("attestation_hash") if isinstance(attestation, Mapping) else None,
    }


def _candidate_records(store: Any, dataset_id: str, dataset_version: str) -> list[Mapping[str, Any]]:
    records = store.load_candidate_lifecycle(limit=MAX_CANDIDATES)
    if not isinstance(records, list):
        return []
    candidates = []
    for item in records:
        if not isinstance(item, Mapping):
            continue
        payload = item.get("payload")
        if isinstance(payload, Mapping) and payload.get("dataset_id") == dataset_id and payload.get("dataset_version") == dataset_version:
            candidates.append(item)
    candidates.sort(key=lambda item: str(item.get("candidate_id") or ""))
    return candidates


def _forward_replay_rows(export: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for record in export.get("forward_rows", ()):
        payload = record.get("payload")
        if not isinstance(payload, Mapping):
            continue
        # Exports may wrap a provider snapshot, or persist the provider
        # snapshot directly.  Both forms are timestamped book evidence; the
        # separate market_metadata partition is never joined here.
        nested = payload.get("snapshot")
        flattened = dict(nested) if isinstance(nested, Mapping) else dict(payload)
        if isinstance(nested, Mapping):
            for key, value in payload.items():
                if key not in {"snapshot", "metadata", "market_metadata"}:
                    flattened.setdefault(key, value)
        flattened.pop("metadata", None)
        flattened.pop("market_metadata", None)
        flattened.update(
            {
                "market_id": record["market_id"],
                "timestamp": record["source_timestamp"],
                "source_timestamp": record["source_timestamp"],
                "observed_at": record["observed_at"],
                "source_type": "FORWARD_COLLECTED",
                "source_snapshot_id": record.get("snapshot_id"),
                "source_record_hash": record.get("raw_record_hash"),
            }
        )
        rows.append(flattened)
    rows.sort(key=lambda item: (str(item.get("timestamp")), str(item.get("market_id")), str(item.get("source_snapshot_id"))))
    return rows


def _canonical_costs(plan: Any) -> tuple[float, float, Mapping[str, Any]]:
    assumptions = plan.assumptions if isinstance(plan.assumptions, Mapping) else {}
    def number(*names: str) -> float | None:
        for name in names:
            if name not in assumptions:
                continue
            try:
                value = float(assumptions[name])
            except (TypeError, ValueError, OverflowError):
                continue
            if math.isfinite(value):
                return value
        return None
    fee = number("fee_bps", "roundtrip_fee_bps")
    slippage = number("slippage_bps", "roundtrip_slippage_bps")
    if fee is None or slippage is None or fee <= 0.0 or slippage <= 0.0:
        raise SmokeBlocked("canonical plan does not declare positive roundtrip fee and slippage assumptions")
    return fee, slippage, assumptions


def _scope_attr(value: Any, name: str, default: Any = None) -> Any:
    if isinstance(value, Mapping):
        return value.get(name, default)
    return getattr(value, name, default)


def _binding_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _canonical_plan_scope(plan: Any, *, label: str) -> Any:
    scope = getattr(plan, "market_scope", None)
    scope_hash = _binding_text(getattr(plan, "market_scope_hash", None))
    scope_version = _binding_text(getattr(plan, "market_scope_version", None))
    if scope is None or not scope_hash or not scope_version:
        raise SmokeBlocked(f"{label} has no canonical market scope binding")
    return scope, scope_hash, scope_version


def _validate_scope_binding(candidate: Mapping[str, Any], plan: Any) -> tuple[Any, str, str]:
    """Require the candidate's persisted binding to equal its immutable plan."""
    from axiom.experiment_plan import MarketScopePolicy

    scope, scope_hash, scope_version = _canonical_plan_scope(plan, label="canonical plan")
    payload = candidate.get("payload")
    if not isinstance(payload, Mapping):
        raise SmokeBlocked("candidate scope binding is missing")

    declared_scope = payload.get("market_scope")
    if not isinstance(declared_scope, Mapping):
        raise SmokeBlocked("candidate market_scope binding is missing or malformed")
    try:
        normalized_scope = MarketScopePolicy.from_mapping(declared_scope)
    except (TypeError, ValueError) as exc:
        raise SmokeBlocked("candidate market_scope binding is malformed") from exc
    if normalized_scope.scope_hash != scope_hash or normalized_scope.scope_version != scope_version:
        raise SmokeBlocked("candidate market_scope binding does not match immutable plan")

    declared_hashes = [
        _binding_text(payload.get(name))
        for name in ("market_scope_hash", "scope_hash")
        if payload.get(name) is not None
    ]
    declared_versions = [
        _binding_text(payload.get(name))
        for name in ("market_scope_version", "scope_version")
        if payload.get(name) is not None
    ]
    if not declared_hashes or any(value != scope_hash for value in declared_hashes):
        raise SmokeBlocked("candidate scope_hash binding is missing or mismatched")
    if not declared_versions or any(value != scope_version for value in declared_versions):
        raise SmokeBlocked("candidate scope_version binding is missing or mismatched")

    nested_plan = payload.get("experiment_plan")
    if nested_plan is not None:
        if not isinstance(nested_plan, Mapping):
            raise SmokeBlocked("candidate experiment_plan binding is malformed")
        nested_plan_id = _binding_text(nested_plan.get("plan_id"))
        canonical_plan_id = _binding_text(getattr(plan, "plan_id", None))
        if nested_plan_id != canonical_plan_id:
            raise SmokeBlocked("candidate experiment_plan plan_id does not match immutable plan")
        nested_scope = nested_plan.get("market_scope")
        if not isinstance(nested_scope, Mapping):
            raise SmokeBlocked("candidate experiment_plan market_scope binding is missing")
        try:
            nested_policy = MarketScopePolicy.from_mapping(nested_scope)
        except (TypeError, ValueError) as exc:
            raise SmokeBlocked("candidate experiment_plan market_scope is malformed") from exc
        if nested_policy.scope_hash != scope_hash or nested_policy.scope_version != scope_version:
            raise SmokeBlocked("candidate experiment_plan scope does not match immutable plan")
        nested_hashes = [
            _binding_text(nested_plan.get(name))
            for name in ("market_scope_hash", "scope_hash")
            if nested_plan.get(name) is not None
        ]
        nested_versions = [
            _binding_text(nested_plan.get(name))
            for name in ("market_scope_version", "scope_version")
            if nested_plan.get(name) is not None
        ]
        if any(value != scope_hash for value in nested_hashes):
            raise SmokeBlocked("candidate nested scope_hash binding is mismatched")
        if any(value != scope_version for value in nested_versions):
            raise SmokeBlocked("candidate nested scope_version binding is mismatched")
    return scope, scope_hash, scope_version


def _validate_rule_based_resolution(
    store: Any,
    candidate_id: str,
    plan: Any,
    resolution: Any,
    *,
    status: str,
    matched_ids: Sequence[str],
) -> None:
    """Re-run the canonical resolver over the persisted inventory for rules."""
    provenance = _scope_attr(resolution, "provenance")
    if not isinstance(provenance, Mapping):
        raise SmokeBlocked("rule-based scope resolution provenance is missing or malformed")
    current_set = provenance.get("current_market_set")
    if not isinstance(current_set, Mapping):
        raise SmokeBlocked("rule-based scope resolution inventory binding is missing")
    raw_inventory_ids = current_set.get("market_ids")
    if isinstance(raw_inventory_ids, (str, bytes)) or not isinstance(raw_inventory_ids, Sequence):
        raise SmokeBlocked("rule-based scope resolution inventory binding is malformed")
    inventory_ids = tuple(_binding_text(item) for item in raw_inventory_ids)
    if (
        not inventory_ids
        or any(item is None for item in inventory_ids)
        or len(set(inventory_ids)) != len(inventory_ids)
        or inventory_ids != tuple(sorted(inventory_ids))
        or len(inventory_ids) > 1_000
    ):
        raise SmokeBlocked("rule-based scope resolution inventory binding is malformed")
    order_token = _binding_text(current_set.get("order_token"))
    expected_order_token = "sha256:" + hashlib.sha256(
        json.dumps(
            {"market_ids": list(inventory_ids)},
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()
    if order_token != expected_order_token:
        raise SmokeBlocked("rule-based scope resolution inventory binding is mismatched")

    inventory_loader = getattr(store, "tracked_polymarket_markets", None)
    if not callable(inventory_loader):
        raise SmokeBlocked("rule-based scope resolution inventory is unavailable")
    try:
        inventory = inventory_loader(
            active_only=False,
            include_payload=True,
            limit=len(inventory_ids),
            market_ids=list(inventory_ids),
        )
    except Exception as exc:
        raise SmokeBlocked("rule-based scope resolution inventory read failed") from exc
    if isinstance(inventory, (str, bytes)) or not isinstance(inventory, Sequence):
        raise SmokeBlocked("rule-based scope resolution inventory is malformed")
    observed_ids = tuple(_binding_text(_scope_attr(item, "market_id")) for item in inventory)
    if (
        len(observed_ids) != len(inventory_ids)
        or any(item is None for item in observed_ids)
        or len(set(observed_ids)) != len(observed_ids)
        or set(observed_ids) != set(inventory_ids)
    ):
        raise SmokeBlocked("rule-based scope resolution inventory does not match persisted order intent")

    resolved_at = _parse_time(_scope_attr(resolution, "resolved_at"))
    if resolved_at is None:
        raise SmokeBlocked("rule-based scope resolution timestamp is malformed")
    try:
        from axiom.market_scope import resolve_market_scope

        resolved = resolve_market_scope(
            candidate_id,
            {"experiment_plan": plan.as_dict()},
            inventory,
            resolved_at=resolved_at,
            max_matches=100,
            max_markets=len(inventory_ids),
        )
    except Exception as exc:
        raise SmokeBlocked("rule-based scope resolution revalidation failed") from exc
    resolved_status = str(_scope_attr(resolved, "status", "") or "").strip().upper()
    resolved_ids = tuple(
        sorted(
            _binding_text(_scope_attr(item, "market_id"))
            for item in (_scope_attr(resolved, "matched_markets") or ())
            if _binding_text(_scope_attr(item, "market_id")) is not None
        )
    )
    if resolved_status != status or resolved_ids != tuple(sorted(matched_ids)):
        raise SmokeBlocked("persisted rule-based scope resolution is inconsistent with canonical resolver")


def _resolution_market_ids(
    store: Any,
    candidate: Mapping[str, Any],
    plan: Any,
) -> tuple[tuple[str, ...], dict[str, Any]]:
    """Read exactly the persisted current resolution for one immutable binding."""
    scope, scope_hash, scope_version = _validate_scope_binding(candidate, plan)
    candidate_id = _binding_text(candidate.get("candidate_id"))
    if not candidate_id:
        raise SmokeBlocked("candidate id is missing for scope resolution")
    loader = getattr(store, "load_market_scope_resolution", None)
    if not callable(loader):
        raise SmokeBlocked("current market scope resolution is unavailable")
    try:
        resolution = loader(
            candidate_id,
            scope_hash=scope_hash,
            scope_version=scope_version,
        )
    except Exception as exc:
        raise SmokeBlocked("current market scope resolution read failed") from exc
    if resolution is None:
        raise SmokeBlocked("current market scope resolution is missing")

    resolution_candidate = _binding_text(_scope_attr(resolution, "candidate_id"))
    resolution_hash = _binding_text(_scope_attr(resolution, "scope_hash"))
    resolution_version = _binding_text(_scope_attr(resolution, "scope_version"))
    if (
        resolution_candidate != candidate_id
        or resolution_hash != scope_hash
        or resolution_version != scope_version
    ):
        raise SmokeBlocked("current market scope resolution binding is mismatched")

    from axiom.experiment_plan import MarketScopePolicy

    resolved_policy = _scope_attr(resolution, "policy")
    if not isinstance(resolved_policy, Mapping):
        raise SmokeBlocked("current market scope resolution policy is missing or malformed")
    try:
        resolved_scope = MarketScopePolicy.from_mapping(resolved_policy)
    except (TypeError, ValueError) as exc:
        raise SmokeBlocked("current market scope resolution policy is malformed") from exc
    if resolved_scope.scope_hash != scope_hash or resolved_scope.scope_version != scope_version:
        raise SmokeBlocked("current market scope resolution policy is mismatched")

    status = str(_scope_attr(resolution, "status", "") or "").strip().upper()
    if status not in {"MATCHED", "PARTIAL"}:
        raise SmokeBlocked(f"current market scope resolution is not usable: {status or 'UNKNOWN'}")
    matched = _scope_attr(resolution, "matched_markets")
    if isinstance(matched, (str, bytes)) or not isinstance(matched, Sequence):
        raise SmokeBlocked("current market scope resolution matched_markets is malformed")
    matched_ids: list[str] = []
    for market in matched:
        market_id = _binding_text(_scope_attr(market, "market_id"))
        if not market_id or market_id in matched_ids:
            raise SmokeBlocked("current market scope resolution has malformed or duplicate markets")
        matched_ids.append(market_id)
    allowed_ids = tuple(sorted(matched_ids))
    if not allowed_ids:
        raise SmokeBlocked("current market scope resolution has no matched markets")

    mode = str(getattr(scope, "mode", "") or "").strip().upper()
    declared_ids = {
        _binding_text(item)
        for item in (getattr(scope, "market_ids", ()) or ())
        if _binding_text(item)
    }
    if mode == "EXACT_MARKETS":
        if not set(allowed_ids).issubset(declared_ids):
            raise SmokeBlocked("current market scope resolution exceeds immutable exact market scope")
        if status == "MATCHED" and set(allowed_ids) != declared_ids:
            raise SmokeBlocked("matched exact market scope resolution is incomplete")
    elif mode == "RULE_BASED_MARKETS":
        _validate_rule_based_resolution(
            store,
            candidate_id,
            plan,
            resolution,
            status=status,
            matched_ids=allowed_ids,
        )
    else:
        raise SmokeBlocked("current market scope is not a forward replay scope")
    return allowed_ids, {
        "scope_hash": scope_hash,
        "scope_version": scope_version,
        "plan_hash": _binding_text(getattr(plan, "plan_hash", None)),
        "scope_resolution_id": _binding_text(_scope_attr(resolution, "resolution_id")),
        "scope_resolution_status": status,
        "scope_resolution_revalidated": mode == "RULE_BASED_MARKETS",
        "resolved_market_ids": list(allowed_ids),
    }


def _scoped_replay_rows(
    store: Any,
    candidate: Mapping[str, Any],
    plan: Any,
    rows: Sequence[Mapping[str, Any]],
) -> tuple[list[Mapping[str, Any]], dict[str, Any]]:
    allowed_ids, evidence = _resolution_market_ids(store, candidate, plan)
    allowed_set = set(allowed_ids)
    scoped_rows = [
        row
        for row in rows
        if _binding_text(row.get("market_id")) in allowed_set
    ]
    included_ids = tuple(
        sorted(
            {
                market_id
                for row in scoped_rows
                if (market_id := _binding_text(row.get("market_id"))) is not None
            }
        )
    )
    evidence.update(
        {
            "included_market_ids": list(included_ids),
            "included_row_count": len(scoped_rows),
            "excluded_row_count": len(rows) - len(scoped_rows),
        }
    )
    if not scoped_rows:
        error = SmokeBlocked("current market scope resolves no replay rows")
        error.replay_evidence = evidence
        raise error
    return scoped_rows, evidence


def _plan_for_candidate(store: Any, candidate: Mapping[str, Any]) -> tuple[Any, Any]:
    from axiom.experiment_plan import ExperimentPlan

    payload = candidate.get("payload") if isinstance(candidate.get("payload"), Mapping) else {}
    plan_id = _binding_text(payload.get("plan_id"))
    if not plan_id:
        raise SmokeBlocked("candidate is missing canonical plan_id")
    record = store.load_experiment_plan(plan_id)
    if not isinstance(record, Mapping):
        raise SmokeBlocked(f"canonical plan is missing for {plan_id}")
    persisted_plan_id = _binding_text(record.get("plan_id"))
    if persisted_plan_id != plan_id:
        raise SmokeBlocked(f"persisted plan_id is mismatched for {plan_id}")
    raw_plan = record.get("plan")
    if not isinstance(raw_plan, Mapping):
        raise SmokeBlocked(f"canonical plan is missing for {plan_id}")
    embedded_plan_id = _binding_text(raw_plan.get("plan_id"))
    if embedded_plan_id != plan_id:
        raise SmokeBlocked(f"embedded plan_id is mismatched for {plan_id}")
    hypothesis_id = str(record.get("hypothesis_id") or payload.get("hypothesis_id") or plan_id)
    try:
        plan = ExperimentPlan.from_mapping(raw_plan, hypothesis_id=hypothesis_id)
    except (TypeError, ValueError) as exc:
        raise SmokeBlocked(f"canonical plan is malformed for {plan_id}") from exc
    decoded_plan_id = _binding_text(getattr(plan, "plan_id", None))
    if decoded_plan_id != plan_id:
        raise SmokeBlocked(f"decoded plan_id is mismatched for {plan_id}")
    persisted_plan_hash = _binding_text(record.get("plan_hash"))
    if persisted_plan_hash and persisted_plan_hash != plan.plan_hash:
        raise SmokeBlocked(f"canonical plan hash is mismatched for {plan_id}")
    candidate_plan_hash = _binding_text(payload.get("plan_hash"))
    if candidate_plan_hash != plan.plan_hash:
        raise SmokeBlocked("candidate plan_hash binding is missing or mismatched")
    _validate_scope_binding(candidate, plan)
    variants = plan.variants()
    if len(variants) != 1:
        raise SmokeBlocked("canonical predeclared replay plan must contain exactly one variant")
    candidate_id = str(candidate.get("candidate_id") or "").strip()
    return plan, plan.strategy_for(variants[0], candidate_id)


def _run_recorded_book_replay(
    store: Any,
    export: Mapping[str, Any],
    candidates: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    from axiom.backtest.prediction import (
        CANONICAL_EVALUATOR_VERSION,
        RECORDED_BOOK_REPLAY,
        RECORDED_BOOK_REPLAY_ASSUMPTIONS_VERSION,
        run_prediction_research_mode,
    )

    rows = _forward_replay_rows(export)
    base_result = {
        "source_type": "FORWARD_COLLECTED",
        "research_mode": RECORDED_BOOK_REPLAY,
        "assumption_version": RECORDED_BOOK_REPLAY_ASSUMPTIONS_VERSION,
        "rows": len(rows),
    }
    if not rows:
        return {
            **base_result,
            "status": "BLOCKED",
            "reason": "forward archive has no canonical recorded-book rows",
            "trials": [],
        }
    if not any(row.get(name) for row in rows for name in ("order_book", "yes_order_book", "no_order_book")):
        return {
            **base_result,
            "status": "BLOCKED",
            "reason": "forward archive has no recorded order books",
            "trials": [],
        }
    trials: list[dict[str, Any]] = []
    seen_candidates: set[str] = set()
    blocked_candidates = 0
    considered_candidates = 0
    for candidate in candidates:
        candidate_id = str(candidate.get("candidate_id") or "").strip()
        if not candidate_id or candidate_id in seen_candidates:
            continue
        payload = candidate.get("payload") if isinstance(candidate.get("payload"), Mapping) else {}
        if payload.get("generation", 0) not in (0, None):
            continue
        seen_candidates.add(candidate_id)
        considered_candidates += 1
        candidate_evidence: dict[str, Any] = {
            "candidate_id": candidate_id,
            "status": "BLOCKED",
            "research_mode": RECORDED_BOOK_REPLAY,
            "assumption_version": RECORDED_BOOK_REPLAY_ASSUMPTIONS_VERSION,
            "source_type": "FORWARD_COLLECTED",
            "plan_hash": _binding_text(payload.get("plan_hash")),
            "scope_hash": _binding_text(payload.get("market_scope_hash") or payload.get("scope_hash")),
            "scope_version": _binding_text(payload.get("market_scope_version") or payload.get("scope_version")),
            "included_market_ids": [],
            "included_row_count": 0,
            "excluded_row_count": len(rows),
        }
        try:
            plan, strategy = _plan_for_candidate(store, candidate)
            scoped_rows, scope_evidence = _scoped_replay_rows(store, candidate, plan, rows)
            candidate_evidence.update(scope_evidence)
            fee_bps, slippage_bps, assumptions = _canonical_costs(plan)
            exit_policy = dict(plan.exit_policy)
            exit_policy["assumptions"] = {
                "version": RECORDED_BOOK_REPLAY_ASSUMPTIONS_VERSION,
                "fee_bps": fee_bps,
                "slippage_bps": slippage_bps,
            }
            methodology = plan.methodology if isinstance(plan.methodology, Mapping) else {}
            initial_cash = float(methodology.get("initial_cash", 10_000.0))
            allocation = float(methodology.get("allocation", 0.25))
            result = run_prediction_research_mode(
                scoped_rows,
                strategy,
                mode=RECORDED_BOOK_REPLAY,
                initial_cash=initial_cash,
                fee_bps=fee_bps,
                slippage_bps=slippage_bps,
                allocation=allocation,
                model_document=plan.model_document,
                holding_period=int(exit_policy["holding_period"]),
                exit_policy=exit_policy,
            )
            sensitivity = assumptions.get("cost_sensitivity", assumptions.get("sensitivity"))
            trials.append(
                {
                    **scope_evidence,
                    "candidate_id": candidate_id,
                    "status": "PASS",
                    "template": plan.template,
                    "strategy_id": strategy.id,
                    "plan_hash": plan.plan_hash,
                    "research_mode": RECORDED_BOOK_REPLAY,
                    "assumption_version": RECORDED_BOOK_REPLAY_ASSUMPTIONS_VERSION,
                    "canonical_assumption_version": assumptions.get("version"),
                    "cost_assumptions": {
                        "fee_bps": fee_bps,
                        "slippage_bps": slippage_bps,
                        "roundtrip_fee_bps": assumptions.get("roundtrip_fee_bps"),
                        "roundtrip_slippage_bps": assumptions.get("roundtrip_slippage_bps"),
                    },
                    "cost_sensitivity": _bounded(sensitivity),
                    "exit_policy": _bounded(exit_policy),
                    "evaluator": CANONICAL_EVALUATOR_VERSION,
                    "source_type": "FORWARD_COLLECTED",
                    "sample_count": len(result.equity_curve),
                    "filled_trades": len(result.fills),
                    "unresolved_count": len(result.unresolved),
                    "outcome_count": len(result.outcomes),
                    "research_quality": getattr(result.research_quality, "value", str(result.research_quality)),
                    "metrics": _bounded(result.metrics),
                }
            )
        except Exception as exc:
            blocked_candidates += 1
            replay_evidence = getattr(exc, "replay_evidence", None)
            if isinstance(replay_evidence, Mapping):
                candidate_evidence.update(replay_evidence)
            candidate_evidence["reason"] = _safe_error(exc)
            trials.append(candidate_evidence)
    result = {
        **base_result,
        "status": "PASS" if trials and blocked_candidates == 0 else "BLOCKED",
        "trials": trials,
        "considered_candidate_count": considered_candidates,
        "blocked_candidate_count": blocked_candidates,
        "holdout_used_for_selection": False,
        "metadata_used": False,
        "projection_hash": _hash_json(
            [
                {
                    "market_id": row.get("market_id"),
                    "timestamp": row.get("timestamp"),
                    "source_timestamp": row.get("source_timestamp"),
                    "source_snapshot_id": row.get("source_snapshot_id"),
                }
                for row in rows
            ]
        ),
    }
    if not considered_candidates:
        result["reason"] = "NO_REPLAY_CANDIDATES"
    elif blocked_candidates:
        result["reason"] = "CANDIDATE_SCOPE_BINDING_BLOCKED"
    return result


def _run_research(store: Any, dataset: Mapping[str, Any]) -> dict[str, Any]:
    from axiom.autonomous import AutonomousResearchConfig, AutonomousResearchProcessor

    # The application owns seed construction, assumptions, scope, and budget.
    # This is only the smoke's bounded queue batch size: all canonical starter
    # items are claimed by one normal worker cycle, exactly once.
    starter_count = len(AutonomousResearchProcessor.predeclared_starting_set())
    processor = AutonomousResearchProcessor(
        store,
        config=AutonomousResearchConfig(max_items_per_cycle=starter_count),
    )
    cycle = processor.process_pending(worker="release-smoke")
    candidates = _candidate_records(store, dataset["dataset_id"], dataset["dataset_version"])
    trials: list[dict[str, Any]] = []
    plan_hashes: list[str] = []
    for candidate in candidates:
        payload = candidate.get("payload") if isinstance(candidate.get("payload"), Mapping) else {}
        plan_id = str(payload.get("plan_id") or "").strip()
        plan_record = store.load_experiment_plan(plan_id) if plan_id else None
        plan_document = plan_record.get("plan") if isinstance(plan_record, Mapping) else {}
        plan_hash = str(payload.get("plan_hash") or (plan_record or {}).get("plan_hash") or "").strip()
        if plan_hash and plan_hash not in plan_hashes:
            plan_hashes.append(plan_hash)
        trials.append(
            {
                "candidate_id": candidate.get("candidate_id"),
                "stage": candidate.get("stage"),
                "reason": candidate.get("rejection_reason"),
                "plan_id": plan_id,
                "plan_hash": plan_hash or None,
                "research_mode": plan_document.get("research_mode") if isinstance(plan_document, Mapping) else None,
                "assumptions": _bounded(plan_document.get("assumptions")) if isinstance(plan_document, Mapping) else None,
                "exit_policy": _bounded(plan_document.get("exit_policy")) if isinstance(plan_document, Mapping) else None,
                "train": _bounded(payload.get("train")),
                "validation": _bounded(payload.get("validation")),
                "holdout": _bounded(payload.get("holdout") or payload.get("locked_holdout") or payload.get("holdout_evidence")),
                "holdout_used_for_selection": payload.get("holdout_used_for_selection", False),
            }
        )
    return {
        "status": "PASS" if trials else "BLOCKED",
        "normal_service": "AutonomousResearchProcessor.process_pending",
        "normal_seeded_by_service": True,
        "starter_count": starter_count,
        "process_cycles": _bounded([cycle.as_record()]),
        "candidate_ids": [str(item.get("candidate_id")) for item in candidates if item.get("candidate_id")],
        "candidate_stages": [str(item.get("stage") or "") for item in candidates],
        "candidates": trials,
        "plan_hashes": plan_hashes,
        "holdout_used_for_selection": False,
    }


def _run_public_collection(store: Any, *, timeout: float, max_markets: int) -> dict[str, Any]:
    from axiom.collector import CollectorConfig, PolymarketCollector
    from axiom.data.polymarket import PolymarketAdapter

    provider = PolymarketAdapter(timeout=timeout)
    collector = PolymarketCollector(
        provider,
        store,
        config=CollectorConfig(
            interval_seconds=60.0,
            depth=5,
            max_markets=max_markets,
            discovery_budget_per_cycle=max_markets,
            max_concurrency=1,
            max_attempts=1,
            max_trade_pages=1,
            failure_cooldown_seconds=0.0,
            retain_cycles=1,
            freshness_sla_seconds=60.0,
            collector_name="polymarket-release-smoke",
        ),
        sleep=lambda _: None,
    )
    cycle = collector.collect_once()
    collection_cutoff = cycle.ended_at
    rows = store.load_polymarket_snapshots(
        source_type="FORWARD_COLLECTED",
        end=collection_cutoff,
        limit=MAX_FORWARD_ROWS,
    )
    observed = [row["observed_at"] for row in rows if row.get("observed_at") is not None]
    return {
        "cycle": _bounded(cycle.as_record()),
        "snapshot_count": len(rows),
        "market_ids": sorted({str(row.get("market_id")) for row in rows}),
        "source_type": "FORWARD_COLLECTED",
        "observed_at": {
            "earliest": min(observed).isoformat() if observed else None,
            "latest": max(observed).isoformat() if observed else None,
        },
        "quality": dict(Counter(str(row.get("quality") or "UNKNOWN") for row in rows)),
        "projection_hash": _hash_json(
            [
                {
                    "snapshot_id": row.get("snapshot_id"),
                    "market_id": row.get("market_id"),
                    "source_timestamp": _iso(row.get("source_timestamp")),
                    "observed_at": _iso(row.get("observed_at")),
                    "quality": row.get("quality"),
                    "source_type": row.get("source_type"),
                }
                for row in rows
            ]
        ),
    }
def _run_paper_observation(store: Any, database: Path) -> dict[str, Any]:
    from axiom.node import NodeConfig, ResearchNode

    node = ResearchNode(
        NodeConfig(db_path=str(database), execution_profile="isolated", crypto_enabled=False),
        store=store,
        sleep=lambda _: None,
    )
    try:
        cycle = node._run_paper_workers()
    finally:
        node.stop()
    bounded_cycle = _bounded(cycle)
    candidate_count = int(cycle.get("candidate_count", 0)) if isinstance(cycle, Mapping) else 0
    errors = cycle.get("errors", []) if isinstance(cycle, Mapping) else []
    return {
        "status": "PASS" if candidate_count and not errors else "NO_ACTIVE_PAPER_CANDIDATES" if not candidate_count else "BLOCKED",
        "normal_service": "ResearchNode._run_paper_workers",
        "cycle": bounded_cycle,
        "candidate_count": candidate_count,
        "paper_only": True,
    }


def _run_canonical_decision(store: Any, candidate_ids: Sequence[str]) -> dict[str, Any]:
    from axiom.canary import CanaryService
    from axiom.ranker import CandidateCanaryRanker

    service = CanaryService(store, allow_environment=False)
    eligibilities: dict[str, Any] = {}
    for candidate_id in candidate_ids:
        identifier = str(candidate_id).strip()
        if identifier:
            record = store.load_candidate_lifecycle(identifier)
            eligibilities[identifier] = service.validate_eligibility(identifier, _record=record)
    qualified = [identifier for identifier, evidence in eligibilities.items() if isinstance(evidence, Mapping) and evidence.get("eligible")]
    ranking: Mapping[str, Any] | None = None
    decision: Mapping[str, Any] | None = None
    if qualified:
        ranker = CandidateCanaryRanker(store, service=service)
        ranking = ranker.evaluate_and_select()
        selected_id = str(ranking.get("selected_candidate") or "").strip()
        if selected_id in eligibilities:
            decision = service.evaluate_signal(selected_id, cycle_id="release-smoke")
    return {
        "eligibility": _bounded(eligibilities),
        "qualified_candidate_ids": qualified,
        "ranking": _bounded(ranking),
        "decision": _bounded(decision),
        "selected_candidate": ranking.get("selected_candidate") if isinstance(ranking, Mapping) else None,
        "actual_current_evaluator_ran": decision is not None,
        "transport_called": False,
    }


def _write_report(path: Path, report: Mapping[str, Any]) -> None:
    encoded = json.dumps(report, indent=2, sort_keys=True, allow_nan=False) + "\n"
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(encoded, encoding="utf-8")
    temporary.replace(path)


def run(args: argparse.Namespace) -> dict[str, Any]:
    paths = _guard_paths(args)
    report = _empty_report(paths)
    report["blockers"] = []
    logger = logging.getLogger("polymarket_release_smoke")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    handler = logging.FileHandler(paths["log"], encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(handler)
    started = time.monotonic()
    run_at = _utc_now()
    store = None
    try:
        export = _read_export(paths["export"])
        report["source_export"].update(
            {
                "status": "PASS",
                "sha256": export["file_hash"],
                "line_count": export["line_count"],
                "counts": export["counts"],
                "timestamps": export["timestamps"],
                "manifest_partitions": _bounded(export["manifest"].get("partitions", {})),
            }
        )
        report["provenance"].update(
            {
                "export_sha256": export["file_hash"],
                "historical_projection_hash": export["historical_projection_hash"],
                "forward_projection_hash": export["forward_projection_hash"],
                "historical_raw_provenance_preserved": True,
            }
        )
        _set_stage(report["stages"], "data", "PASS", historical_rows=len(export["historical_rows"]), forward_rows=len(export["forward_rows"]))
        from axiom.storage import AxiomStore

        store = AxiomStore(str(paths["database"]))
        dataset = _persist_historical_dataset(store, export)
        report["plan"].update(
            {
                "dataset_id": dataset["dataset_id"],
                "dataset_version": dataset["dataset_version"],
                "dataset_row_count": dataset["row_count"],
                "dataset_market_count": dataset["market_count"],
                "attestation_hash": dataset.get("attestation_hash"),
            }
        )
        research = _run_research(store, dataset)
        report["candidate"] = {
            "status": research.get("status"),
            "candidate_ids": research.get("candidate_ids", []),
            "candidate_stages": research.get("candidate_stages", []),
            "normal_service": research.get("normal_service"),
            "process_cycles": research.get("process_cycles", []),
        }
        candidate_ids = [str(item) for item in research.get("candidate_ids", ()) if str(item).strip()]
        candidate_stages = [str(item).strip().upper() for item in research.get("candidate_stages", ())]
        usable_ids = [
            candidate_id
            for candidate_id, stage in zip(candidate_ids, candidate_stages)
            if stage in {"FROZEN", "PAPER_FORWARD", "PAPER_PROMOTABLE"}
        ]
        trial_evidence = research.get("candidates") or []
        report["plan"].update(
            {
                "trial_count": len(trial_evidence),
                "plan_hashes": research.get("plan_hashes", []),
                "assumptions": trial_evidence[0].get("assumptions") if trial_evidence else None,
                "exit_policy": trial_evidence[0].get("exit_policy") if trial_evidence else None,
            }
        )
        report["strategy_evidence"].update(
            {
                "status": "PASS" if trial_evidence else "BLOCKED",
                "candidate_ids": candidate_ids,
                "candidate_stages": candidate_stages,
                "historical_trials": trial_evidence,
                "assessment": "SUPPORTED_EDGE_CANDIDATE" if usable_ids else "NO_SUPPORTED_EDGE",
            }
        )
        report["candidate"]["eligible_candidate_ids"] = usable_ids
        if usable_ids:
            _set_stage(report["stages"], "candidate", "PASS", candidate_count=len(usable_ids), candidate_ids=usable_ids)
            _set_stage(report["stages"], "assessment", "PASS", trial_count=len(trial_evidence), holdout_used_for_selection=False)
        else:
            reason = "NO_SUPPORTED_EDGE"
            _set_stage(report["stages"], "candidate", "BLOCKED", reason, candidate_count=len(candidate_ids))
            _set_stage(report["stages"], "assessment", "NO_SUPPORTED_EDGE", reason, trial_count=len(trial_evidence), holdout_used_for_selection=False)
            report["blockers"].append(reason)
        # The canonical worker has registered schema-valid intents above;
        # collect once to resolve their current scope and materialize bounded
        # paper specs, then reuse this fresh evidence below.
        try:
            public = _run_public_collection(store, timeout=args.timeout, max_markets=args.max_markets)
            report["current_data"].update(
                {
                    "status": "PASS" if public.get("snapshot_count") else "BLOCKED",
                    "observed_vs_replay": "fresh anonymous public FORWARD_COLLECTED observations" if public.get("snapshot_count") else "NO_FRESH_OBSERVATIONS",
                    **public,
                }
            )
            report["software_path"].update({"status": "VERIFIED", "public_transport_called": True, "cycle": public.get("cycle")})
        except Exception as exc:
            reason = _safe_error(exc)
            public = {"snapshot_count": 0, "market_ids": [], "reason": reason}
            report["current_data"].update({"status": "BLOCKED", "observed_vs_replay": "NOT_OBSERVED", "reason": reason, **public})
            report["software_path"].update({"status": "BLOCKED", "public_transport_called": False, "reason": reason})
            report["blockers"].append(reason)
        try:
            replay = _run_recorded_book_replay(
                store,
                export,
                _candidate_records(
                    store,
                    dataset["dataset_id"],
                    dataset["dataset_version"],
                ),
            )
            report["strategy_evidence"]["recorded_book_replay"] = replay
        except Exception as exc:
            reason = _safe_error(exc)
            report["strategy_evidence"]["recorded_book_replay"] = {
                "status": "BLOCKED",
                "reason": reason,
                "research_mode": "RECORDED_BOOK_REPLAY",
                "assumption_version": "recorded-book-replay-v1",
                "trials": [],
            }
            report["blockers"].append(reason)
        snapshot_count = int(public.get("snapshot_count") or 0)
        resolutions: dict[str, Any] = {}
        for identifier in usable_ids:
            resolution = store.load_market_scope_resolution(identifier)
            if resolution is not None:
                resolutions[identifier] = _bounded(resolution.as_dict() if hasattr(resolution, "as_dict") else resolution)
        _set_stage(report["stages"], "current_frozen_policy_resolution", "PASS" if resolutions else "BLOCKED", None if resolutions else "CURRENT_SCOPE_UNRESOLVED", candidate_ids=usable_ids, resolutions=resolutions)
        if snapshot_count:
            _set_stage(report["stages"], "fresh_inputs", "PASS", snapshot_count=snapshot_count, market_ids=public.get("market_ids"))
        else:
            _set_stage(report["stages"], "fresh_inputs", "BLOCKED", "NO_FRESH_FORWARDED_OBSERVATIONS", snapshot_count=0)
        try:
            paper = _run_paper_observation(store, paths["database"])
            report["paper_observation"] = paper
            _set_stage(report["stages"], "paper_observation", paper["status"], None if paper["status"] == "PASS" else paper["status"], cycle=paper.get("cycle"))
        except Exception as exc:
            reason = _safe_error(exc)
            report["paper_observation"] = {"status": "BLOCKED", "normal_service": "ResearchNode._run_paper_workers", "reason": reason, "paper_only": True}
            _set_stage(report["stages"], "paper_observation", "BLOCKED", reason)
            report["blockers"].append(reason)
        if usable_ids and snapshot_count:
            decision = _run_canonical_decision(store, usable_ids)
            report["decision"] = decision
            report["evaluation"] = {
                "actual_current_evaluator_ran": bool(decision.get("actual_current_evaluator_ran")),
                "actual_current_evaluator": "CanaryService.evaluate_signal",
                "result": decision.get("decision"),
            }
            eligibilities = decision.get("eligibility", {})
            qualified_ids = decision.get("qualified_candidate_ids", [])
            report["qualification"].update({"status": "PASS" if qualified_ids else "BLOCKED", "ready": bool(qualified_ids), "candidate_ids": usable_ids, "qualified_candidate_ids": qualified_ids, "canonical": eligibilities})
            selected_id = str(decision.get("selected_candidate") or "").strip()
            decision_result = decision.get("decision") if isinstance(decision.get("decision"), Mapping) else {}
            reason_code = str(decision_result.get("reason_code") or "CANDIDATE_NOT_SELECTED").strip().upper()
            if selected_id and reason_code == "READY_SIGNAL":
                _set_stage(report["stages"], "actual_canonical_decision", "PASS", reason_code, selected_candidate=selected_id)
                _set_stage(report["stages"], "execution_feasibility", "PASS", reason_code, selected_candidate=selected_id)
                report["feasibility"] = {"status": "PASS", "reason": reason_code, "selected_candidate": selected_id}
                report["status"] = "PASS"
            else:
                _set_stage(report["stages"], "actual_canonical_decision", "BLOCKED", reason_code)
                _set_stage(report["stages"], "execution_feasibility", "BLOCKED", reason_code)
                report["feasibility"] = {"status": "BLOCKED", "reason": reason_code}
                report["blockers"].append(reason_code)
                report["status"] = "BLOCKED"
        else:
            report["evaluation"].update({"actual_current_evaluator_ran": False, "not_run_reason": "NO_ELIGIBLE_CANDIDATE" if not usable_ids else "FRESH_INPUTS_UNAVAILABLE"})
            report["qualification"].update({"status": "NO_SUPPORTED_EDGE" if not usable_ids else "BLOCKED", "ready": False, "candidate_ids": usable_ids})
            reason = "NO_SUPPORTED_EDGE" if not usable_ids else "FRESH_INPUTS_UNAVAILABLE"
            _set_stage(report["stages"], "actual_canonical_decision", "NOT_RUN", reason)
            _set_stage(report["stages"], "execution_feasibility", "NOT_RUN", reason)
            report["feasibility"] = {"status": "NOT_RUN", "reason": reason}
            report["status"] = "BLOCKED"
            if reason not in report["blockers"]:
                report["blockers"].append(reason)
        report["blockers"] = list(dict.fromkeys(report["blockers"]))
        logger.info("bounded isolated smoke completed status=%s", report["status"])
    except Exception as exc:
        report["status"] = "BLOCKED"
        report["blockers"] = [_safe_error(exc)]
        for stage in report["stages"]:
            if stage["status"] == "NOT_RUN":
                stage["reason"] = _safe_error(exc)
        logger.error("bounded isolated smoke blocked: %s", _safe_error(exc))
    finally:
        if store is not None:
            store.close()
        report["elapsed_seconds"] = round(max(0.0, time.monotonic() - started), 3)
        report["run_timestamp"] = run_at.isoformat()
        handler.close()
        logger.removeHandler(handler)
        _write_report(paths["report"], report)
    return report


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--export", default=str(DEFAULT_EXPORT), help="immutable research export under the release root")
    parser.add_argument("--report", default=str(DEFAULT_REPORT), help="bounded report path under the release root")
    parser.add_argument("--output-db", default=str(DEFAULT_DATABASE), help="isolated output database under the release root")
    parser.add_argument("--output-log", default=str(DEFAULT_LOG), help="isolated output log under the release root")
    parser.add_argument("--max-markets", type=int, default=2)
    parser.add_argument("--timeout", type=float, default=5.0)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if not 1 <= args.max_markets <= MAX_CURRENT_MARKETS:
        print("--max-markets must be between 1 and 4", file=sys.stderr)
        return 2
    if not math.isfinite(args.timeout) or not 0 < args.timeout <= MAX_TIMEOUT:
        print("--timeout must be finite and in (0, 10]", file=sys.stderr)
        return 2
    try:
        report = run(args)
    except Exception as exc:
        print(_safe_error(exc), file=sys.stderr)
        return 2
    return 0 if report.get("status") == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
