"""Deterministic research summary and bounded Hermes proposal validation."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import hashlib
import json
import re
import math
from itertools import islice
from typing import Any, Mapping

from .research_bus import ResearchBusPermissionError, _validate_payload
from .storage import AxiomStore


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


def validate_hermes_proposal(proposal: Mapping[str, Any], *, max_bytes: int = 16_384) -> ProposalValidation:
    reasons: list[str] = []
    if isinstance(max_bytes, bool) or not isinstance(max_bytes, int) or max_bytes <= 0:
        raise ValueError("max_bytes must be a positive integer")
    if not isinstance(proposal, Mapping):
        return ProposalValidation(False, None, ("proposal must be a mapping",))
    try:
        normalized = _validate_payload(proposal)
    except (ResearchBusPermissionError, TypeError, ValueError) as exc:
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
    proposal_id = str(normalized.get("proposal_id", "")).strip() or None
    if proposal_id is None and not reasons:
        proposal_id = "proposal-" + hashlib.sha256(_canonical(normalized).encode("utf-8")).hexdigest()[:24]
    return ProposalValidation(not reasons, proposal_id, tuple(reasons), normalized if not reasons else None)
def research_summary(store: AxiomStore, *, now: datetime | None = None, limit: int = 20) -> dict[str, Any]:
    """Return a small, stable summary; raw market/database dumps are excluded."""
    if isinstance(limit, bool) or not isinstance(limit, int) or limit < 0:
        raise ValueError("limit must be a non-negative integer")
    health = store.polymarket_health(now=now)
    maturity = health.get("evidence_maturity")
    if not isinstance(maturity, Mapping):
        maturity = store.polymarket_evidence_maturity(now=now)
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
                "family": _compact(payload.get("family")),
                "regimes": _compact(payload.get("regimes", payload.get("regime_count"))),
                "stability": _compact(payload.get("stability", payload.get("regime_stability"))),
                "rejection_reason": _compact(payload.get("rejection_reason")),
                "lineage": _compact(payload.get("lineage", payload.get("parent_id"))),
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
                "quality": _compact(report.get("quality", report.get("research_quality"))),
                "selection": _compact(report.get("selection", report.get("validation"))),
                "errors": _compact(report.get("errors", ())),
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
    queue = store.research_queue_stats()
    gaps = []
    if maturity.get("grade") in {"F", "D"}:
        gaps.append("research evidence maturity below independent-data threshold")
    if not candidates:
        gaps.append("no persisted candidate lifecycle")
    if not reports:
        gaps.append("no persisted research reports")
    return {
        "as_of": (now.isoformat() if now is not None else health.get("evidence_maturity", {}).get("as_of")),
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
        "queue": queue,
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
