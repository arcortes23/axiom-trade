"""Deterministic research summary and bounded Hermes proposal validation."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
import re
import math
from itertools import islice
from typing import Any, Mapping

from .research_bus import ResearchBusPermissionError, _validate_payload
from .storage import AxiomStore

_DATASET_VERSION_ALIASES = frozenset({"latest", "current", "default", "unversioned"})
_MAX_RESEARCHABLE_DATASET_SCAN = 256
_MAX_RESEARCHABLE_PREDICTION_CONSTITUENTS = 8
_MAX_RESEARCHABLE_CRYPTO_DATASETS = 32


@dataclass(frozen=True, slots=True)
class ProposalValidation:
    accepted: bool
    proposal_id: str | None
    reasons: tuple[str, ...]
    normalized: Mapping[str, Any] | None = None

    def as_record(self) -> dict[str, Any]:
        return {
            "accepted": self.accepted,
            "proposal_id": self.proposal_id,
            "reasons": list(self.reasons),
            "proposal": dict(self.normalized) if self.normalized is not None else None,
        }


def validate_hermes_proposal(
    proposal: Mapping[str, Any],
    *,
    max_bytes: int = 16_384,
    store: AxiomStore | None = None,
) -> ProposalValidation:
    reasons: list[str] = []
    if isinstance(max_bytes, bool) or not isinstance(max_bytes, int) or max_bytes <= 0:
        raise ValueError("max_bytes must be a positive integer")
    if not isinstance(proposal, Mapping):
        return ProposalValidation(False, None, ("proposal must be a mapping",))
    forbidden_canary = sorted(
        str(key) for key in proposal
        if any(token in str(key).replace("-", "_").lower() for token in ("canary", "credential", "private_key", "place_order", "cancel_order"))
    )
    if forbidden_canary:
        return ProposalValidation(False, None, ("LIVE_EXECUTION_FORBIDDEN", f"Hermes cannot access canary controls: {forbidden_canary}"))
    try:
        normalized = _validate_payload(proposal)
    except ResearchBusPermissionError as exc:
        message = str(exc)
        lowered = message.lower()
        if "holdout" in lowered:
            code = "LOCKED_HOLDOUT_FORBIDDEN"
        elif any(token in lowered for token in ("live", "execute", "execution")):
            code = "LIVE_EXECUTION_FORBIDDEN"
        else:
            code = "UNSAFE_PROPOSAL_FIELD"
        return ProposalValidation(False, None, (code, message))
    except (TypeError, ValueError) as exc:
        return ProposalValidation(False, None, (str(exc),))
    if len(_canonical(normalized).encode("utf-8")) > max_bytes:
        reasons.append("proposal exceeds bounded size")
    statement_value = normalized.get("statement", normalized.get("hypothesis", ""))
    if not isinstance(statement_value, str) or not statement_value.strip():
        reasons.append("statement must be non-empty text")
    source_value = normalized.get("source", normalized.get("author", ""))
    if not isinstance(source_value, str) or not source_value.strip():
        reasons.append("source must be non-empty text")
    dataset_version = normalized.get("dataset_version")
    if not isinstance(dataset_version, str) or not dataset_version.strip():
        reasons.append("dataset_version must be non-empty text")
    tests = normalized.get("tests", normalized.get("validation_plan"))
    if (
        not isinstance(tests, (list, tuple))
        or not tests
        or len(tests) > 16
        or any(not isinstance(test, str) or not test.strip() for test in tests)
    ):
        reasons.append("bounded tests list of non-empty text is required")
    time_split = normalized.get("time_split")
    if not isinstance(time_split, str) or not time_split.strip():
        reasons.append("time_split must be non-empty text")
    else:
        split_tokens = tuple(token for token in re.split(r"[^a-z]+", time_split.lower()) if token)
        positions = [split_tokens.index(token) if token in split_tokens else -1 for token in ("train", "validation", "holdout")]
        if positions != sorted(positions) or any(position < 0 for position in positions) or any(
            token in split_tokens for token in ("random", "k", "fold", "shuffle")
        ):
            reasons.append("time_split must be chronological train-validation-holdout")
    if normalized.get("paper_only") is not True:
        reasons.append("paper_only must be true")
    plan = None
    if not reasons:
        from .experiment_plan import ExperimentPlan, ExperimentPlanError

        plan_proposal = dict(normalized)
        generated_id = "proposal-" + hashlib.sha256(_canonical(normalized).encode("utf-8")).hexdigest()[:24]
        if not str(plan_proposal.get("proposal_id", "")).strip():
            plan_proposal["proposal_id"] = generated_id
        try:
            plan = ExperimentPlan.from_proposal(plan_proposal)
        except ExperimentPlanError as exc:
            reasons.extend((exc.reason, exc.detail))
    if not reasons and store is not None and plan is not None:
        reasons.extend(_persisted_binding_reasons(store, plan))
    proposal_id = str(normalized.get("proposal_id", "")).strip() or None
    if proposal_id is None and not reasons:
        proposal_id = "proposal-" + hashlib.sha256(_canonical(normalized).encode("utf-8")).hexdigest()[:24]
    return ProposalValidation(not reasons, proposal_id, tuple(reasons), normalized if not reasons else None)
def _catalog_version(catalog: Mapping[str, Any]) -> str:
    return str(catalog.get("dataset_version", catalog.get("version", ""))).strip()


def _complete_historical_catalog(catalog: Any) -> bool:
    if not isinstance(catalog, Mapping):
        return False
    if str(catalog.get("source_type", "")).strip().upper() != "HISTORICAL":
        return False
    version = _catalog_version(catalog)
    if not version or version.casefold() in _DATASET_VERSION_ALIASES:
        return False
    try:
        row_count = int(catalog.get("row_count", 0))
        completeness = float(catalog.get("completeness", 0.0))
    except (TypeError, ValueError, OverflowError):
        return False
    return row_count > 0 and math.isfinite(completeness) and completeness >= 1.0 and not catalog.get("missing_ranges")


def _find_persisted_universe_catalog(
    store: AxiomStore,
    universe_id: str,
    universe_version: str,
    *,
    universe_dataset_id: str | None = None,
) -> Mapping[str, Any] | None:
    loader = getattr(store, "load_dataset_catalog", None)
    if callable(loader):
        identifiers = [str(universe_dataset_id).strip()] if universe_dataset_id else []
        identifiers.append(f"universe:{universe_id}")
        for identifier in dict.fromkeys(item for item in identifiers if item):
            candidate = loader(identifier, universe_version)
            if (
                isinstance(candidate, Mapping)
                and str(candidate.get("dataset_id", "")).strip() == identifier
                and _catalog_version(candidate) == universe_version
                and str(candidate.get("market_type", "")).strip().lower() == "crypto_spot"
                and str(candidate.get("timeframe", "")).strip() == "point_in_time"
            ):
                return candidate
    lister = getattr(store, "list_dataset_catalog", None)
    if callable(lister):
        for candidate in lister(limit=_MAX_RESEARCHABLE_DATASET_SCAN):
            if not isinstance(candidate, Mapping):
                continue
            metadata = candidate.get("metadata")
            metadata = metadata if isinstance(metadata, Mapping) else {}
            if (
                str(candidate.get("dataset_version", candidate.get("version", ""))).strip() == universe_version
                and str(metadata.get("universe_id", "")).strip() == universe_id
                and str(candidate.get("dataset_id", "")).strip()
                == str(universe_dataset_id or f"universe:{universe_id}").strip()
                and str(candidate.get("market_type", "")).strip().lower() == "crypto_spot"
                and str(candidate.get("timeframe", "")).strip() == "point_in_time"
            ):
                return candidate
    return None


def _load_historical_catalog(store: AxiomStore, dataset_id: str) -> Mapping[str, Any] | None:
    loader = getattr(store, "load_dataset_catalog", None)
    if not callable(loader):
        return None
    latest = loader(dataset_id)
    if isinstance(latest, Mapping) and _complete_historical_catalog(latest):
        return latest
    versions_loader = getattr(store, "dataset_versions", None)
    if not callable(versions_loader):
        return None
    try:
        versions = list(versions_loader(dataset_id))
    except (TypeError, ValueError):
        return None
    for version in reversed(versions[-_MAX_RESEARCHABLE_DATASET_SCAN:]):
        candidate = loader(dataset_id, str(version))
        if isinstance(candidate, Mapping) and _complete_historical_catalog(candidate):
            return candidate
    return None


def _prediction_catalog_integrity_reason(store: AxiomStore, catalog: Mapping[str, Any]) -> str | None:
    dataset_id = str(catalog.get("dataset_id", "")).strip()
    metadata = catalog.get("metadata")
    metadata = metadata if isinstance(metadata, Mapping) else {}
    if dataset_id.startswith("prediction:"):
        expected_market_id = dataset_id.split(":", 1)[1]
        market_id = str(metadata.get("market_id", metadata.get("polymarket_key", ""))).strip()
        if market_id and market_id != expected_market_id:
            return "prediction constituent market_id does not match dataset_id"
        return None
    if dataset_id != "Polymarket-historical" or "market_versions" not in metadata:
        return None
    market_versions = metadata.get("market_versions")
    if not isinstance(market_versions, (list, tuple)) or not market_versions:
        return "prediction aggregate market_versions metadata is invalid"
    loader = getattr(store, "load_dataset_catalog", None)
    if not callable(loader):
        return "prediction aggregate constituents cannot be resolved exactly"
    aggregate_rows = catalog.get("row_count")
    if isinstance(aggregate_rows, bool) or not isinstance(aggregate_rows, int) or aggregate_rows <= 0:
        return "prediction aggregate row_count is invalid"
    seen: set[tuple[str, str]] = set()
    constituent_rows = 0
    for item in market_versions:
        if not isinstance(item, Mapping):
            return "prediction aggregate constituent metadata is invalid"
        market_id = str(item.get("market_id", "")).strip()
        constituent_id = str(item.get("dataset_id") or f"prediction:{market_id}").strip()
        version = str(item.get("version", item.get("dataset_version", ""))).strip()
        expected_rows = item.get("records", item.get("row_count"))
        identity = (constituent_id, version)
        if (
            not market_id
            or constituent_id != f"prediction:{market_id}"
            or not version
            or version.casefold() in _DATASET_VERSION_ALIASES
            or identity in seen
            or isinstance(expected_rows, bool)
            or not isinstance(expected_rows, int)
            or expected_rows <= 0
        ):
            return "prediction aggregate constituent binding is not exact"
        constituent = loader(constituent_id, version)
        if not isinstance(constituent, Mapping) or not _complete_historical_catalog(constituent):
            return f"prediction aggregate constituent is unavailable: {constituent_id}/{version}"
        if (
            str(constituent.get("dataset_id", "")).strip() != constituent_id
            or _catalog_version(constituent) != version
            or str(constituent.get("market_type", "")).strip().lower() != "prediction"
            or constituent.get("row_count") != expected_rows
        ):
            return "prediction aggregate constituent catalog does not match exact metadata"
        constituent_metadata = constituent.get("metadata")
        constituent_metadata = constituent_metadata if isinstance(constituent_metadata, Mapping) else {}
        persisted_market_id = str(
            constituent_metadata.get("market_id", constituent_metadata.get("polymarket_key", ""))
        ).strip()
        if persisted_market_id != market_id:
            return "prediction aggregate constituent market_id does not match exact metadata"
        seen.add(identity)
        constituent_rows += expected_rows
    if constituent_rows != aggregate_rows:
        return "prediction aggregate row_count does not match exact constituent counts"
    return None


def _crypto_catalog_provenance(
    store: AxiomStore,
    catalog: Mapping[str, Any],
) -> tuple[str, str, str] | None:
    metadata = catalog.get("metadata")
    metadata = dict(metadata) if isinstance(metadata, Mapping) else {}
    universe_id = str(metadata.get("universe_id", "")).strip()
    universe_version = str(metadata.get("universe_version", "")).strip()
    universe_hash = str(metadata.get("universe_snapshot_hash", "")).strip()
    survivorship = str(
        metadata.get("survivorship_bias")
        or metadata.get("survivorship_label")
        or metadata.get("survivorship")
        or ""
    ).strip()
    if not universe_id or not universe_version or not universe_hash or not survivorship:
        return None
    universe = _find_persisted_universe_catalog(
        store,
        universe_id,
        universe_version,
        universe_dataset_id=str(metadata.get("universe_dataset_id", "")).strip() or None,
    )
    if universe is None:
        return None
    universe_metadata = universe.get("metadata")
    universe_metadata = universe_metadata if isinstance(universe_metadata, Mapping) else {}
    persisted_id = str(universe_metadata.get("universe_id", "")).strip()
    persisted_hash = str(universe_metadata.get("snapshot_hash", "")).strip()
    persisted_version = _catalog_version(universe)
    if (
        persisted_id != universe_id
        or persisted_version != universe_version
        or persisted_hash != universe_hash
        or universe_metadata.get("point_in_time") is not True
    ):
        return None
    return universe_id, universe_version, survivorship


def _researchable_dataset_entry(
    catalog: Mapping[str, Any],
    *,
    crypto_provenance: tuple[str, str, str] | None = None,
) -> dict[str, Any]:
    metadata = catalog.get("metadata")
    metadata = metadata if isinstance(metadata, Mapping) else {}
    dataset_id = str(catalog.get("dataset_id", "")).strip()
    instrument = str(catalog.get("instrument") or metadata.get("instrument") or "").strip() or None
    market_id = str(metadata.get("market_id") or "").strip() or None
    entry: dict[str, Any] = {
        "dataset_id": dataset_id,
        "dataset_version": _catalog_version(catalog),
        "market_type": str(catalog.get("market_type", "")).strip().lower(),
        "instrument": instrument,
        "market_id": market_id,
        "timeframe": str(catalog.get("timeframe") or "").strip() or None,
        "source_type": "HISTORICAL",
        "quality": catalog.get("quality"),
        "row_count": int(catalog.get("row_count", 0)),
        "start_timestamp": catalog.get("start_timestamp"),
        "end_timestamp": catalog.get("end_timestamp"),
        "universe_id": None,
        "universe_version": None,
        "survivorship_label": None,
    }
    if dataset_id.startswith("prediction:") and entry["market_id"] is None:
        entry["market_id"] = dataset_id.split(":", 1)[1] or None
    if crypto_provenance is not None:
        entry["symbol"] = instrument
        entry["universe_id"], entry["universe_version"], entry["survivorship_label"] = crypto_provenance
    return _compact(entry)


def _researchable_datasets(store: AxiomStore) -> dict[str, list[dict[str, Any]]]:
    """Return the small allow-list of complete historical dataset bindings."""
    catalogs = store.list_dataset_catalog(
        source_type="HISTORICAL",
        limit=_MAX_RESEARCHABLE_DATASET_SCAN,
    )
    complete = [item for item in catalogs if _complete_historical_catalog(item)]
    prediction_catalogs = [
        item for item in complete
        if str(item.get("market_type", "")).strip().lower() == "prediction"
    ]
    prediction: list[dict[str, Any]] = []
    aggregate = _load_historical_catalog(store, "Polymarket-historical")
    if (
        aggregate is not None
        and _prediction_catalog_integrity_reason(store, aggregate) is None
    ):
        prediction.append(_researchable_dataset_entry(aggregate))
    for item in prediction_catalogs:
        dataset_id = str(item.get("dataset_id", "")).strip()
        if (
            dataset_id == "Polymarket-historical"
            or not dataset_id.startswith("prediction:")
            or len(prediction) >= 1 + _MAX_RESEARCHABLE_PREDICTION_CONSTITUENTS
            or _prediction_catalog_integrity_reason(store, item) is not None
        ):
            continue
        prediction.append(_researchable_dataset_entry(item))

    crypto: list[dict[str, Any]] = []
    for item in complete:
        if (
            str(item.get("market_type", "")).strip().lower() != "crypto_spot"
            or len(crypto) >= _MAX_RESEARCHABLE_CRYPTO_DATASETS
        ):
            continue
        provenance = _crypto_catalog_provenance(store, item)
        if provenance is not None:
            crypto.append(_researchable_dataset_entry(item, crypto_provenance=provenance))
    return {"prediction": prediction, "crypto_spot": crypto}


def _persisted_binding_reasons(store: AxiomStore, plan: Any) -> tuple[str, ...]:
    dataset_id = str(getattr(plan, "dataset_id", "") or "").strip()
    dataset_version = str(getattr(plan, "dataset_version", "") or "").strip()
    if not dataset_id:
        return ("DATASET_NOT_FOUND", "dataset_id must be selected from researchable_datasets")
    if not dataset_version or dataset_version.casefold() in _DATASET_VERSION_ALIASES:
        return ("DATASET_NOT_FOUND", "dataset_version must be an exact immutable version")
    allowed = _researchable_datasets(store)
    listed = any(
        str(item.get("dataset_id", "")).strip() == dataset_id
        and str(item.get("dataset_version", "")).strip() == dataset_version
        for group in allowed.values()
        for item in group
    )
    if not listed:
        return (
            "DATASET_NOT_FOUND",
            f"no complete historical researchable dataset {dataset_id} at version {dataset_version}",
        )
    loader = getattr(store, "load_dataset_catalog", None)
    catalog = loader(dataset_id, dataset_version) if callable(loader) else None
    if not isinstance(catalog, Mapping) or not _complete_historical_catalog(catalog):
        return ("DATASET_NOT_FOUND", f"dataset binding is not a complete historical catalog: {dataset_id}/{dataset_version}")
    if str(catalog.get("dataset_id", "")).strip() != dataset_id or _catalog_version(catalog) != dataset_version:
        return ("DATASET_NOT_FOUND", "dataset catalog identity does not match the exact proposal binding")
    expected_market_type = str(getattr(getattr(plan, "market_type", None), "value", "")).strip().lower()
    if str(catalog.get("market_type", "")).strip().lower() != expected_market_type:
        return ("DATASET_NOT_FOUND", "dataset catalog market_type does not match the experiment plan")
    source_type = str(getattr(plan, "dataset_source_type", "") or "").strip().upper()
    if source_type and source_type != "HISTORICAL":
        return ("DATASET_NOT_FOUND", "research proposals may bind only HISTORICAL datasets")
    timeframe = str(getattr(plan, "dataset_timeframe", "") or "").strip()
    if timeframe and timeframe != str(catalog.get("timeframe", "")).strip():
        return ("DATASET_NOT_FOUND", "dataset timeframe does not match the exact catalog")
    source = str(getattr(plan, "dataset_source", "") or "").strip()
    if source and source != str(catalog.get("provider", "")).strip():
        return ("DATASET_NOT_FOUND", "dataset source does not match the exact catalog")
    if expected_market_type == "prediction":
        integrity_reason = _prediction_catalog_integrity_reason(store, catalog)
        if integrity_reason is not None:
            return ("DATASET_NOT_FOUND", integrity_reason)
    elif expected_market_type == "crypto_spot":
        provenance = _crypto_catalog_provenance(store, catalog)
        if provenance is None:
            return ("DATASET_NOT_FOUND", "crypto dataset lacks exact persisted universe provenance")
        required = (
            str(getattr(plan, "dataset_timeframe", "") or "").strip(),
            str(getattr(plan, "dataset_source", "") or "").strip(),
            str(getattr(plan, "dataset_survivorship", "") or "").strip(),
            str(getattr(plan, "universe_id", "") or "").strip(),
            str(getattr(plan, "universe_version", "") or "").strip(),
            str(getattr(plan, "universe_snapshot_hash", "") or "").strip(),
        )
        if not all(required):
            return ("DATASET_NOT_FOUND", "crypto proposals require exact dataset and universe provenance")
        metadata = catalog.get("metadata")
        metadata = metadata if isinstance(metadata, Mapping) else {}
        if (
            provenance != (
                required[3],
                required[4],
                required[2],
            )
            or str(metadata.get("universe_snapshot_hash", "")).strip() != required[5]
        ):
            return ("DATASET_NOT_FOUND", "crypto dataset and universe provenance do not match the exact catalog")
    return ()


def research_summary(store: AxiomStore, *, now: datetime | None = None, limit: int = 20) -> dict[str, Any]:
    """Return compact, durable evidence for Hermes and the dashboard."""
    if isinstance(limit, bool) or not isinstance(limit, int) or limit < 0:
        raise ValueError("limit must be a non-negative integer")
    health = store.polymarket_health(now=now)
    maturity = health.get("evidence_maturity")
    if not isinstance(maturity, Mapping):
        maturity = store.polymarket_evidence_maturity(now=now)
    if not isinstance(maturity, Mapping):
        maturity = {}
    candidates = store.load_candidate_lifecycle(limit=limit)
    candidates = candidates if isinstance(candidates, list) else []
    candidate_records: list[dict[str, Any]] = []
    for item in candidates:
        payload = item.get("payload", {}) if isinstance(item, Mapping) else {}
        payload = payload if isinstance(payload, Mapping) else {}
        candidate_records.append(
            {
                "candidate_id": item.get("candidate_id"),
                "stage": item.get("stage"),
                "family": _compact(payload.get("experiment_family", payload.get("family"))),
                "hypothesis_id": _compact(payload.get("hypothesis_id")),
                "plan_id": _compact(payload.get("plan_id")),
                "validation_regimes": _compact(
                    payload.get("validation_regime_behavior", {}).get("regimes")
                    if isinstance(payload.get("validation_regime_behavior"), Mapping)
                    else payload.get("validation_regime_count")
                ),
                "validation_stability": _compact(payload.get("validation_stability")),
                "forward_stability": _compact(payload.get("forward_stability")),
                "forward_expectancy": _compact(payload.get("forward_expectancy")),
                "forward_confidence_lower_bound": _compact(payload.get("forward_confidence_lower_bound")),
                "forward_calibration": _compact(payload.get("forward_calibration")),
                "forward_max_drawdown": _compact(payload.get("forward_max_drawdown")),
                "forward_liquidity": _compact(payload.get("forward_liquidity")),
                "forward_markets_observed": _compact(payload.get("markets_observed")),
                "forward_markets_signaled": _compact(payload.get("markets_signaled")),
                "forward_markets_traded": _compact(payload.get("markets_traded")),
                "forward_markets_resolved": _compact(payload.get("markets_resolved")),
                "forward_trades": _compact(payload.get("forward_trades")),
                "forward_fills": _compact(payload.get("fills")),
                "forward_partial_fills": _compact(payload.get("partial_fills")),
                "forward_no_fill_orders": _compact(payload.get("no_fill_orders")),
                "forward_average_slippage": _compact(payload.get("average_slippage")),
                "forward_spread_paid": _compact(payload.get("spread_paid")),
                "forward_independent_resolved_bets": _compact(payload.get("forward_independent_resolved_bets")),
                "rejection_reason": _compact(payload.get("rejection_reason")),
                "lineage": _compact(payload.get("lineage", payload.get("parent_id"))),
                "forward_test_id": _compact(payload.get("forward_test_id")),
            }
        )
    reports: list[dict[str, Any]] = []
    for item in store.list_reports(limit=limit, newest_first=True) if limit else []:
        report = item.get("report", {})
        if not isinstance(report, Mapping):
            continue
        reports.append(
            {
                "report_id": item.get("report_id"),
                "experiment_id": item.get("experiment_id"),
                "quality": _compact(report.get("quality", report.get("research_quality", report.get("data_quality")))),
                "selection": _compact(report.get("selection", report.get("validation", report.get("selected_from_variants")))),
                "errors": _compact(report.get("errors", report.get("rejected_reasons", ()))),
                "calibration": _compact(report.get("calibration", report.get("calibration_summary"))),
                "anomalies": _compact(report.get("anomalies", ())),
            }
        )
    paper_states = []
    for item in store.list_worker_states(limit=256):
        payload = item.get("payload", {}) if isinstance(item, Mapping) else {}
        if str(item.get("worker_name", "")).startswith("paper"):
            paper_states.append(
                {
                    "worker": item.get("worker_name"),
                    "status": item.get("status"),
                    "payload": _compact(payload),
                }
            )
    queue_stats = store.research_queue_stats()
    queue_items = store.list_research_items(limit=max(32, limit))
    all_queue_items = store.list_research_items(limit=10_000)
    hypothesis_items = [
        item for item in all_queue_items if str(item.get("item_type", "")).strip().lower() == "hypothesis"
    ]
    terminal_results = [
        _compact(item.get("result"))
        for item in hypothesis_items
        if item.get("status") in {"COMPLETED", "REJECTED", "FAILED"} and isinstance(item.get("result"), Mapping)
    ]
    accepted = sum(
        1
        for item in hypothesis_items
        if item.get("status") == "COMPLETED"
        and isinstance(item.get("result"), Mapping)
        and item["result"].get("accepted") is True
    )
    rejected = sum(1 for item in hypothesis_items if item.get("status") == "REJECTED")
    pending = sum(1 for item in hypothesis_items if item.get("status") in {"PENDING", "TESTING"})
    queue_worker = next(
        (
            item
            for item in store.list_worker_states(limit=256)
            if str(item.get("worker_name", "")) == "research-queue"
        ),
        None,
    )
    plans = store.list_experiment_plans(limit=limit)
    all_plans = store.list_experiment_plans(limit=10_000)
    plan_records = [
        {
            "plan_id": item.get("plan_id"),
            "hypothesis_id": item.get("hypothesis_id"),
            "plan_hash": item.get("plan_hash"),
            "status": item.get("status"),
            "variants_tested": _compact((item.get("result") or {}).get("variants_tested")) if isinstance(item.get("result"), Mapping) else None,
            "selected_from_variants": _compact((item.get("result") or {}).get("selected_from_variants")) if isinstance(item.get("result"), Mapping) else None,
        }
        for item in plans
    ]
    budget = store.load_experiment_budget("autonomous")
    accounting_now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    accounting_day = accounting_now.replace(hour=0, minute=0, second=0, microsecond=0)
    daily_reservations = store.count_experiment_budget_reservations(
        "autonomous",
        since=accounting_day,
        until=accounting_day + timedelta(days=1),
    )
    all_candidates = store.load_candidate_lifecycle(limit=10_000)
    all_candidates = all_candidates if isinstance(all_candidates, list) else []
    family_accounting: dict[str, dict[str, Any]] = {}
    dataset_reuse: dict[str, int] = {}
    mutation_count = 0
    generation_depth = 0
    for item in all_candidates:
        payload = item.get("payload", {}) if isinstance(item, Mapping) else {}
        payload = payload if isinstance(payload, Mapping) else {}
        family = str(payload.get("experiment_family", payload.get("family", "unknown")))
        family_row = family_accounting.setdefault(
            family,
            {"candidates": 0, "variants": 0, "mutations": 0, "max_generation": 0},
        )
        family_row["candidates"] += 1
        generation = int(payload.get("generation", 0)) if str(payload.get("generation", "")).lstrip("-").isdigit() else 0
        generation_depth = max(generation_depth, generation)
        family_row["max_generation"] = max(family_row["max_generation"], generation)
        if generation == 0:
            family_row["variants"] += 1
        if payload.get("parent_id"):
            mutation_count += 1
            family_row["mutations"] += 1
        dataset_version = str(payload.get("dataset_version", "")).strip()
        if dataset_version:
            dataset_reuse[dataset_version] = dataset_reuse.get(dataset_version, 0) + 1
    variants_tested = 0
    for item in all_plans:
        result = item.get("result") if isinstance(item, Mapping) else None
        if not isinstance(result, Mapping):
            continue
        value = result.get("variants_tested", 0)
        try:
            variants_tested += max(0, int(value))
        except (TypeError, ValueError, OverflowError):
            continue
    forward_count = len(store.load_forward_tests(limit=10_000))
    accounting = {
        "by_family": family_accounting,
        "variants_tested": variants_tested,
        "mutations": mutation_count,
        "max_generation": generation_depth,
        "dataset_reuse": dict(sorted(dataset_reuse.items())),
        "accounting_day": accounting_day.date().isoformat(),
        "daily_reservations": daily_reservations,
        "failed_queue_items": sum(1 for item in all_queue_items if item.get("status") == "FAILED"),
        "forward_tests_registered": forward_count,
        "selection_rule": "validation-only selection; no confidence pooling across variants or siblings",
    }
    funnel = store.candidate_lifecycle_funnel()
    rejection_reasons = store.candidate_rejection_reasons(limit=max(1, limit))
    for item in all_queue_items:
        if item.get("status") not in {"REJECTED", "FAILED"}:
            continue
        result = item.get("result")
        result = result if isinstance(result, Mapping) else {}
        raw_reason = str(result.get("reason_code") or "").strip()
        reason = raw_reason if re.fullmatch(r"[A-Z][A-Z0-9_]{2,63}", raw_reason) else "queue_rejection"
        rejection_reasons[reason] = rejection_reasons.get(reason, 0) + 1
    gaps = []
    if maturity.get("grade") in {"F", "D"}:
        gaps.append("research evidence maturity below independent-data threshold")
        gaps.append("no persisted candidate lifecycle")
    if not reports:
        gaps.append("no persisted research reports")
    return {
        "as_of": (now.isoformat() if now is not None else (health.get("evidence_maturity") or {}).get("as_of")),
        "live_execution": False,
        "collector_health": {
            "grade": health.get("grade"),
            "grade_scope": health.get("grade_scope"),
            "stale_markets": _compact(health.get("stale_markets", [])),
            "gaps": _compact(health.get("gaps", [])),
            "collection_errors": health.get("collection_errors", 0),
        },
        "evidence_maturity": _compact(maturity),
        "candidates": candidate_records,
        "reports": reports,
        "paper_forward": paper_states,
        "queue": queue_stats,
        "hermes": {
            "last_run": _compact(queue_worker.get("updated_at")) if isinstance(queue_worker, Mapping) else None,
            "submitted": len(hypothesis_items),
            "accepted": accepted,
            "rejected": rejected,
            "failed": sum(1 for item in hypothesis_items if item.get("status") == "FAILED"),
            "pending": pending,
            "latest_results": terminal_results[:limit],
        },
        "autonomous": {
            "plans": plan_records,
            "accounting": accounting,
            "budget": _compact(budget),
            "lifecycle_funnel": _compact(funnel),
            "rejection_reasons": _compact(rejection_reasons),
            "queue_items": [
                {
                    "item_id": item.get("item_id"),
                    "item_type": item.get("item_type"),
                    "status": item.get("status"),
                    "attempts": item.get("attempts"),
                    "last_error": _compact(item.get("last_error")),
                    "events": [
                        event.get("to_status")
                        for event in store.list_research_queue_events(str(item.get("item_id")), limit=64)
                    ]
                    if item.get("item_id")
                    else [],
                }
                for item in queue_items[:limit]
            ],
        },
        "researchable_datasets": _researchable_datasets(store),
        "gaps": gaps,
    }


def _compact(value: Any, *, depth: int = 0) -> Any:
    if depth > 3:
        return "<truncated>"
    if isinstance(value, Mapping):
        return {str(key): _compact(child, depth=depth + 1) for key, child in islice(value.items(), 32)}
    if isinstance(value, list):
        return [_compact(child, depth=depth + 1) for child in value[:32]]
    if isinstance(value, tuple):
        return [_compact(child, depth=depth + 1) for child in value[:32]]
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, str):
        return value if len(value) <= 1024 else value[:1021] + "..."
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def compact_report(value: Any) -> Any:
    """Bound a persisted report before exposing it to a prompt or dashboard."""
    return _compact(value)


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False, default=str)


__all__ = ["ProposalValidation", "compact_report", "research_summary", "validate_hermes_proposal"]
