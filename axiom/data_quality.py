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
    return _nonempty(metadata.get("provider", catalog.get("provider"))) and _nonempty(
        metadata.get("source_type", catalog.get("source_type"))
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


def evaluate_prediction_data_quality(store: Any, payload: Mapping[str, Any]) -> dict[str, Any]:
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
        fidelity = _fidelity(catalog, body)
        base["historical_execution_fidelity"] = fidelity
        base["historical_execution_fidelity_score"] = EXECUTION_FIDELITY_SCORES.get(fidelity)
        records = None
        try:
            cache = getattr(store, "_prediction_quality_records_cache", None)
            if not isinstance(cache, dict):
                cache = {}
                setattr(store, "_prediction_quality_records_cache", cache)
            cache_key = (dataset_id, dataset_version)
            if cache_key in cache:
                records = cache[cache_key]
            else:
                records = store.load_dataset(dataset_id, dataset_version)
                cache[cache_key] = records
        except Exception:
            try:
                records = store.load_dataset(dataset_id, dataset_version)
            except Exception:
                records = None
        records_ok, row_count = _records_are_historical(records)
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
