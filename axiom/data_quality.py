"""Authoritative evidence dimensions for prediction-market canary policy."""
from __future__ import annotations

from typing import Any, Mapping


TIMESTAMPED_DEPTH = "TIMESTAMPED_DEPTH"
PRICE_PROXY = "PRICE_PROXY"
CURRENT_ORDER_BOOK = "CURRENT_ORDER_BOOK"
CURRENT_ORDER_BOOK_REQUIRED = "CURRENT_ORDER_BOOK_REQUIRED"

# A price proxy is valid historical evidence, but it is not historical depth.
# The score is intentionally bounded below genuine timestamped depth and is
# applied as one weighted ranking component rather than an eligibility bypass.
EXECUTION_FIDELITY_SCORES = {
    TIMESTAMPED_DEPTH: 1.0,
    PRICE_PROXY: 0.35,
}


def _text(value: Any) -> str:
    return str(value).strip() if value is not None else ""


def _upper(value: Any) -> str:
    return _text(value).upper()


def _nonempty(value: Any) -> bool:
    return bool(_text(value))


def _catalog_provenance_complete(catalog: Mapping[str, Any]) -> bool:
    required = (
        "dataset_id",
        "dataset_version",
        "provider",
        "instrument",
        "market_type",
        "timeframe",
        "source_type",
        "snapshot_id",
    )
    if not all(_nonempty(catalog.get(key)) for key in required):
        return False
    if _upper(catalog.get("source_type")) != "HISTORICAL":
        return False
    if _upper(catalog.get("market_type")) != "PREDICTION":
        return False
    try:
        row_count = int(catalog.get("row_count"))
        completeness = float(catalog.get("completeness"))
    except (TypeError, ValueError):
        return False
    if row_count <= 0 or not 0.0 <= completeness <= 1.0 or completeness < 1.0:
        return False
    if not _nonempty(catalog.get("start_timestamp")) or not _nonempty(catalog.get("end_timestamp")):
        return False
    metadata = catalog.get("metadata")
    if not isinstance(metadata, Mapping):
        return False
    metadata_source = metadata.get("source_type", catalog.get("source_type"))
    return (
        _upper(metadata_source) == "HISTORICAL"
        and _nonempty(metadata.get("provider", catalog.get("provider")))
        and _nonempty(metadata_source)
    )


def _payload_provenance_matches(payload: Mapping[str, Any], catalog: Mapping[str, Any]) -> bool:
    provenance = payload.get("dataset_provenance")
    if not isinstance(provenance, Mapping):
        return False
    for source in (payload.get("source_type"), provenance.get("source_type")):
        if source is not None and _upper(source) != "HISTORICAL":
            return False
    if _text(provenance.get("dataset_id")) != _text(catalog.get("dataset_id")):
        return False
    if _text(provenance.get("dataset_version")) != _text(catalog.get("dataset_version")):
        return False
    split = provenance.get("time_split")
    if split is not None and not _nonempty(split):
        return False
    return True


def _records_are_historical(records: Any) -> tuple[bool, int]:
    if not isinstance(records, list) or not records:
        return False, 0
    for record in records:
        if not isinstance(record, Mapping):
            return False, len(records)
        # Aggregate records intentionally inherit HISTORICAL provenance from
        # their exact immutable constituents and may omit this copied field.
        source_type = record.get("source_type")
        if source_type is not None and _upper(source_type) != "HISTORICAL":
            return False, len(records)
        for key in ("historical_source_type", "historical_split_source_type"):
            if key in record and _upper(record.get(key)) != "HISTORICAL":
                return False, len(records)
    return True, len(records)


def _fidelity(catalog: Mapping[str, Any], payload: Mapping[str, Any]) -> str:
    metadata = catalog.get("metadata")
    metadata = metadata if isinstance(metadata, Mapping) else {}
    declared = _upper(
        metadata.get(
            "research_quality",
            catalog.get("quality", payload.get("historical_execution_fidelity")),
        )
    )
    historical_depth = metadata.get("historical_order_book_available")
    if historical_depth is True or declared in {"HISTORICAL_ORDER_BOOK", "ORDER_BOOK", TIMESTAMPED_DEPTH}:
        return TIMESTAMPED_DEPTH
    if declared == PRICE_PROXY:
        return PRICE_PROXY
    return declared or "UNKNOWN"


def _map_attestation_reason(value: Any) -> str | None:
    """Map verifier metadata to the stable quality reason vocabulary."""
    reason = _upper(value)
    if not reason:
        return None
    if reason.startswith("HISTORICAL_"):
        return reason
    return {
        "FORWARD_CONTAMINATION": "HISTORICAL_FORWARD_CONTAMINATION",
        "DATASET_PROVENANCE_INVALID": "HISTORICAL_PROVENANCE_INCOMPLETE",
        "CONSTITUENT_BINDING_INVALID": "HISTORICAL_PROVENANCE_INCOMPLETE",
        "CONSTITUENT_CATALOG_NOT_FOUND": "HISTORICAL_PROVENANCE_INCOMPLETE",
        "CONSTITUENT_CATALOG_MISMATCH": "HISTORICAL_PROVENANCE_INCOMPLETE",
        "DATASET_CATALOG_IDENTITY_CHANGED": "HISTORICAL_DATASET_IDENTITY_MISMATCH",
        "DATASET_CATALOG_NOT_FOUND": "HISTORICAL_DATASET_VERSION_NOT_FOUND",
        "CONSTITUENT_LIMIT_EXCEEDED": "HISTORICAL_PROVENANCE_INCOMPLETE",
        "CONSTITUENT_ROW_COUNT_INVALID": "HISTORICAL_ROWS_EMPTY_OR_MISMATCHED",
        "CONSTITUENT_ROWS_INVALID": "HISTORICAL_ROWS_EMPTY_OR_MISMATCHED",
        "DATASET_ROWS_INVALID": "HISTORICAL_ROWS_EMPTY_OR_MISMATCHED",
        "ROW_COUNT_MISMATCH": "HISTORICAL_ROWS_EMPTY_OR_MISMATCHED",
        "INCOMPLETE_DATASET": "HISTORICAL_ROWS_EMPTY_OR_MISMATCHED",
        "BOUNDS_MISMATCH": "HISTORICAL_ROWS_EMPTY_OR_MISMATCHED",
    }.get(reason)


def _attestation_specific_reasons(
    attestation: Mapping[str, Any],
    catalog: Mapping[str, Any],
    payload: Mapping[str, Any],
) -> list[str]:
    """Recover exact cheap failures without treating stale facts as valid evidence."""
    reasons: list[str] = []

    def add(value: Any) -> None:
        mapped = _map_attestation_reason(value)
        if mapped and mapped not in reasons:
            reasons.append(mapped)

    # The verifier's persisted reason is authoritative for a failed snapshot.
    persisted_reasons = attestation.get("reasons")
    if isinstance(persisted_reasons, (list, tuple)):
        for value in persisted_reasons:
            add(value)
    add(attestation.get("reason"))

    # A failed contamination result is useful even when older attestations did
    # not persist the verifier's reason column.
    contamination_result = _upper(attestation.get("contamination_result"))
    if not reasons and contamination_result and contamination_result != "PASS":
        add("FORWARD_CONTAMINATION")

    # Current catalog/provenance metadata can explain a stale attestation
    # without materializing historical records.
    metadata = catalog.get("metadata")
    if isinstance(metadata, Mapping):
        metadata_contamination = metadata.get(
            "contamination_result",
            metadata.get("forward_contamination"),
        )
        if (
            isinstance(metadata_contamination, bool) and metadata_contamination
        ) or _upper(metadata_contamination) in {
            "FAIL",
            "CONTAMINATED",
            "FORWARD_CONTAMINATION",
            "FORWARD_COLLECTED",
            "TRUE",
            "YES",
        }:
            add("FORWARD_CONTAMINATION")
        if _nonempty(metadata.get("source_type")) and _upper(metadata.get("source_type")) != "HISTORICAL":
            add("DATASET_PROVENANCE_INVALID")

    catalog_source = _upper(catalog.get("source_type"))
    if _nonempty(catalog_source) and catalog_source != "HISTORICAL":
        add("DATASET_PROVENANCE_INVALID")
    for source in (payload.get("source_type"),):
        if _nonempty(source) and _upper(source) != "HISTORICAL":
            add("DATASET_PROVENANCE_INVALID")
    provenance = payload.get("dataset_provenance")
    if isinstance(provenance, Mapping):
        source = provenance.get("source_type")
        if _nonempty(source) and _upper(source) != "HISTORICAL":
            add("DATASET_PROVENANCE_INVALID")

    attestation_source = _upper(attestation.get("source_type"))
    attestation_market = _upper(attestation.get("market_type"))
    if attestation_source and attestation_source != "HISTORICAL":
        add("DATASET_PROVENANCE_INVALID")
    if attestation_market and attestation_market != "PREDICTION":
        add("DATASET_PROVENANCE_INVALID")

    expected_count = catalog.get("row_count")
    attestation_count = attestation.get("row_count")
    if (
        isinstance(expected_count, int)
        and not isinstance(expected_count, bool)
        and isinstance(attestation_count, int)
        and not isinstance(attestation_count, bool)
        and attestation_count != expected_count
    ):
        add("ROW_COUNT_MISMATCH")
    completeness = attestation.get("completeness")
    if isinstance(completeness, (int, float)) and float(completeness) < 1.0:
        add("INCOMPLETE_DATASET")
    return reasons


def evaluate_prediction_data_quality(
    store: Any,
    payload: Mapping[str, Any],
    *,
    verify_attestation: bool = True,
) -> dict[str, Any]:
    """Evaluate immutable historical evidence without conflating fidelity.

    The result is the single policy projection consumed by lifecycle gates,
    ranking, signal generation, and the dashboard.  It never upgrades a
    ``PRICE_PROXY`` label; it separately records integrity and fidelity.
    """
    body = payload if isinstance(payload, Mapping) else {}
    dataset_id = _text(body.get("dataset_id"))
    dataset_version = _text(body.get("dataset_version"))
    applicable = bool(dataset_id or dataset_version or _upper(body.get("market_type")) == "PREDICTION")
    base: dict[str, Any] = {
        "applicable": applicable,
        "historical_data_integrity": "FAIL",
        "historical_data_integrity_passed": False,
        "historical_execution_fidelity": "UNKNOWN",
        "historical_execution_fidelity_score": None,
        "current_execution_evidence": CURRENT_ORDER_BOOK_REQUIRED,
        "historical_provenance_complete": False,
        "historical_rows_nonempty": False,
        "historical_no_forward_contamination": False,
        "canary_data_quality_acceptable": False,
        "canary_data_quality_status": "CANARY_DATA_QUALITY_UNACCEPTABLE",
        "production_evidence_status": "INSUFFICIENT",
        "dataset_id": dataset_id or None,
        "dataset_version": dataset_version or None,
        "historical_dataset_row_count": 0,
        "dataset_integrity_attestation_status": None,
        "dataset_integrity_attestation_hash": None,
        "reasons": [],
    }
    if not applicable:
        # Compatibility for pre-policy manual fixtures. Autonomous prediction
        # candidates always carry a dataset binding and enter the strict path.
        if body.get("data_quality_passed") is True:
            base.update(
                {
                    "historical_data_integrity": "PASS",
                    "historical_data_integrity_passed": True,
                    "historical_execution_fidelity": _upper(body.get("data_quality")) or "UNKNOWN",
                    "canary_data_quality_acceptable": True,
                    "canary_data_quality_status": "CANARY_DATA_QUALITY_ACCEPTABLE_LEGACY",
                }
            )
        else:
            base["reasons"] = ["HISTORICAL_DATASET_BINDING_REQUIRED"]
        return base

    reasons: list[str] = []
    catalog = None
    try:
        if dataset_id and dataset_version:
            catalog = store.load_dataset_catalog(dataset_id, dataset_version)
    except Exception:
        catalog = None
    if not isinstance(catalog, Mapping):
        reasons.append("HISTORICAL_DATASET_VERSION_NOT_FOUND")
    else:
        if (
            _text(catalog.get("dataset_id")) != dataset_id
            or _text(catalog.get("dataset_version")) != dataset_version
        ):
            reasons.append("HISTORICAL_DATASET_IDENTITY_MISMATCH")
        provenance_complete = _catalog_provenance_complete(catalog) and _payload_provenance_matches(body, catalog)
        base["historical_provenance_complete"] = provenance_complete
        if not provenance_complete:
            reasons.append("HISTORICAL_PROVENANCE_INCOMPLETE")
        # Recurring consumers only trust a durable CURRENT attestation.  The
        # ranker owns one-time verification outside publication transactions.
        attestation_loader = getattr(store, "load_dataset_integrity_attestation", None)
        attestation_verifier = getattr(store, "verify_dataset_integrity_attestation", None)
        if callable(attestation_loader):
            try:
                attestation = attestation_loader(dataset_id, dataset_version)
                if (
                    attestation is None
                    and verify_attestation
                    and callable(attestation_verifier)
                ):
                    attestation = attestation_verifier(dataset_id, dataset_version)
            except Exception:
                attestation = None
            attestation_status = (
                _upper(attestation.get("status")) if isinstance(attestation, Mapping) else ""
            )
            if isinstance(attestation, Mapping):
                base["dataset_integrity_attestation_status"] = attestation_status or None
                base["dataset_integrity_attestation_hash"] = attestation.get("attestation_hash")
            if not isinstance(attestation, Mapping):
                reasons.append("HISTORICAL_DATASET_ATTESTATION_MISSING")
            elif attestation_status != "CURRENT":
                # Stale attestations fail closed.  Their cheap, explicit
                # failure metadata is still useful and must not be hidden
                # behind the generic stale marker.
                specific_reasons = _attestation_specific_reasons(attestation, catalog, body)
                if specific_reasons:
                    reasons.extend(specific_reasons)
                elif not reasons:
                    reasons.append("HISTORICAL_DATASET_ATTESTATION_STALE")
            else:
                reasons.extend(_attestation_specific_reasons(attestation, catalog, body))
                attestation_fidelity = _upper(
                    attestation.get("execution_fidelity", attestation.get("historical_execution_fidelity"))
                )
                base["historical_execution_fidelity"] = attestation_fidelity or _fidelity(catalog, body)
                base["historical_execution_fidelity_score"] = EXECUTION_FIDELITY_SCORES.get(
                    base["historical_execution_fidelity"]
                )
                base["historical_provenance_complete"] = (
                    _catalog_provenance_complete(catalog)
                    and _payload_provenance_matches(body, catalog)
                    and _upper(attestation.get("source_type")) == "HISTORICAL"
                    and _upper(attestation.get("market_type")) == "PREDICTION"
                )
                attestation_count = attestation.get("row_count")
                expected_count = catalog.get("row_count")
                completeness = attestation.get("completeness")
                base["historical_dataset_row_count"] = (
                    int(attestation_count)
                    if isinstance(attestation_count, int) and not isinstance(attestation_count, bool)
                    else 0
                )
                base["historical_rows_nonempty"] = bool(
                    base["historical_dataset_row_count"] > 0
                    and attestation_count == expected_count
                    and isinstance(completeness, (int, float))
                    and float(completeness) >= 1.0
                )
                base["historical_no_forward_contamination"] = (
                    _upper(attestation.get("contamination_result")) == "PASS"
                )
                if not base["historical_provenance_complete"]:
                    reasons.append("HISTORICAL_PROVENANCE_INCOMPLETE")
                if not base["historical_rows_nonempty"]:
                    reasons.append("HISTORICAL_ROWS_EMPTY_OR_MISMATCHED")
                if not base["historical_no_forward_contamination"]:
                    reasons.append("HISTORICAL_FORWARD_CONTAMINATION")
                if base["historical_execution_fidelity"] not in EXECUTION_FIDELITY_SCORES:
                    reasons.append("HISTORICAL_EXECUTION_FIDELITY_UNKNOWN")
            integrity = not reasons and attestation_status == "CURRENT"
            base["historical_data_integrity_passed"] = integrity
            base["historical_data_integrity"] = "PASS" if integrity else "FAIL"
            acceptable = integrity and base["historical_execution_fidelity"] in {
                PRICE_PROXY,
                TIMESTAMPED_DEPTH,
            }
            base["canary_data_quality_acceptable"] = acceptable
            if acceptable and base["historical_execution_fidelity"] == PRICE_PROXY:
                base["canary_data_quality_status"] = "CANARY_DATA_QUALITY_ACCEPTABLE_LIMITED"
            elif acceptable:
                base["canary_data_quality_status"] = "CANARY_DATA_QUALITY_ACCEPTABLE"
            base["reasons"] = list(dict.fromkeys(reasons))
            return base
        fidelity = _fidelity(catalog, body)
        base["historical_execution_fidelity"] = fidelity
        base["historical_execution_fidelity_score"] = EXECUTION_FIDELITY_SCORES.get(fidelity)
        records_ok = False
        row_count = 0
        cache_key = (dataset_id, dataset_version)

        def load_record_integrity() -> tuple[bool, int]:
            records_cache = getattr(store, "_prediction_quality_records_cache", None)
            if not isinstance(records_cache, dict):
                records_cache = {}
                setattr(store, "_prediction_quality_records_cache", records_cache)
            integrity_cache = getattr(store, "_prediction_quality_integrity_cache", None)
            if not isinstance(integrity_cache, dict):
                integrity_cache = {}
                setattr(store, "_prediction_quality_integrity_cache", integrity_cache)
            cached = integrity_cache.get(cache_key)
            if cached is not None:
                return cached
            records_cached = cache_key in records_cache
            if records_cached:
                records = records_cache[cache_key]
            else:
                records = store.load_dataset(dataset_id, dataset_version)
            cached = _records_are_historical(records)
            if not records_cached:
                records_cache[cache_key] = records
            integrity_cache[cache_key] = cached
            return cached

        try:
            lock = getattr(store, "_lock", None)
            if lock is not None and callable(getattr(lock, "__enter__", None)):
                with lock:
                    records_ok, row_count = load_record_integrity()
            elif lock is not None and callable(getattr(lock, "acquire", None)):
                lock.acquire()
                try:
                    records_ok, row_count = load_record_integrity()
                finally:
                    lock.release()
            else:
                records_ok, row_count = load_record_integrity()
        except Exception:
            # Failed loads/scans remain uncached so a later evaluation can retry.
            records_ok, row_count = False, 0
        expected_count = catalog.get("row_count")
        rows_nonempty = records_ok and row_count > 0 and row_count == expected_count
        base["historical_rows_nonempty"] = rows_nonempty
        base["historical_dataset_row_count"] = row_count
        if not rows_nonempty:
            reasons.append("HISTORICAL_ROWS_EMPTY_OR_MISMATCHED")
        no_contamination = records_ok
        base["historical_no_forward_contamination"] = no_contamination
        if not no_contamination:
            reasons.append("HISTORICAL_FORWARD_CONTAMINATION")
        if fidelity not in EXECUTION_FIDELITY_SCORES:
            reasons.append("HISTORICAL_EXECUTION_FIDELITY_UNKNOWN")
    integrity = bool(
        isinstance(catalog, Mapping)
        and not reasons
        and base["historical_provenance_complete"]
        and base["historical_rows_nonempty"]
        and base["historical_no_forward_contamination"]
    )
    base["historical_data_integrity_passed"] = integrity
    base["historical_data_integrity"] = "PASS" if integrity else "FAIL"
    acceptable = integrity and base["historical_execution_fidelity"] in {PRICE_PROXY, TIMESTAMPED_DEPTH}
    base["canary_data_quality_acceptable"] = acceptable
    if acceptable and base["historical_execution_fidelity"] == PRICE_PROXY:
        base["canary_data_quality_status"] = "CANARY_DATA_QUALITY_ACCEPTABLE_LIMITED"
    elif acceptable:
        base["canary_data_quality_status"] = "CANARY_DATA_QUALITY_ACCEPTABLE"
    base["reasons"] = list(dict.fromkeys(reasons))
    return base


def persisted_quality_fields(result: Mapping[str, Any]) -> dict[str, Any]:
    """Return the non-secret immutable quality projection for lifecycle rows."""
    names = (
        "historical_data_integrity",
        "historical_data_integrity_passed",
        "historical_execution_fidelity",
        "historical_execution_fidelity_score",
        "current_execution_evidence",
        "historical_provenance_complete",
        "historical_rows_nonempty",
        "historical_no_forward_contamination",
        "canary_data_quality_acceptable",
        "canary_data_quality_status",
        "production_evidence_status",
        "historical_dataset_row_count",
        "dataset_integrity_attestation_status",
        "dataset_integrity_attestation_hash",
    )
    return {
        "data_quality_passed": bool(result.get("canary_data_quality_acceptable")),
        **{name: result.get(name) for name in names},
    }


__all__ = [
    "CURRENT_ORDER_BOOK",
    "CURRENT_ORDER_BOOK_REQUIRED",
    "EXECUTION_FIDELITY_SCORES",
    "PRICE_PROXY",
    "TIMESTAMPED_DEPTH",
    "evaluate_prediction_data_quality",
    "persisted_quality_fields",
]
