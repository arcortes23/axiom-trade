"""Ordered, persisted candidate lifecycle and promotion gates."""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import math
from typing import Any, Mapping

from .storage import AxiomStore


class CandidateStage(str, Enum):
    IDEA = "IDEA"
    SCHEMA_VALIDATED = "SCHEMA_VALIDATED"
    BACKTESTED = "BACKTESTED"
    VALIDATED = "VALIDATED"
    ROBUSTNESS_CHECKED = "ROBUSTNESS_CHECKED"
    FROZEN = "FROZEN"
    PAPER_FORWARD = "PAPER_FORWARD"
    PAPER_PROMOTABLE = "PAPER_PROMOTABLE"
    REJECTED = "REJECTED"


_STAGE_ORDER = (
    CandidateStage.IDEA,
    CandidateStage.SCHEMA_VALIDATED,
    CandidateStage.BACKTESTED,
    CandidateStage.VALIDATED,
    CandidateStage.ROBUSTNESS_CHECKED,
    CandidateStage.FROZEN,
    CandidateStage.PAPER_FORWARD,
    CandidateStage.PAPER_PROMOTABLE,
)


@dataclass(frozen=True, slots=True)
class PromotionCriteria:
    """Explicit, configurable evidence gates; defaults are intentionally strict."""

    min_independent_samples: int = 30
    min_trades: int = 20
    max_drawdown: float = 0.20
    min_expectancy: float = 0.0
    min_confidence_lower_bound: float = 0.0
    min_stability: float = 0.60
    min_calibration: float = 0.80
    min_liquidity: float = 0.0
    min_forward_duration_seconds: float = 7.0 * 86400.0
    min_regimes: int = 3

    def __post_init__(self) -> None:
        for name in ("min_independent_samples", "min_trades", "min_regimes"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")
        for name in (
            "max_drawdown",
            "min_expectancy",
            "min_confidence_lower_bound",
            "min_stability",
            "min_calibration",
            "min_liquidity",
            "min_forward_duration_seconds",
        ):
            value = float(getattr(self, name))
            if not math.isfinite(value):
                raise ValueError(f"{name} must be finite")
        for name in ("max_drawdown", "min_stability", "min_calibration"):
            value = float(getattr(self, name))
            if not 0.0 <= value <= 1.0:
                raise ValueError(f"{name} must be in [0, 1]")
        if self.min_forward_duration_seconds < 0:
            raise ValueError("min_forward_duration_seconds must be non-negative")

    def evaluate(self, evidence: Mapping[str, Any]) -> tuple[str, ...]:
        def number(*names: str, default: float = 0.0) -> float:
            value: Any = default
            for name in names:
                if name in evidence:
                    value = evidence[name]
                    break
            try:
                result = float(value)
            except (TypeError, ValueError):
                return float("nan")
            return result

        def count(*names: str) -> int:
            value: Any = 0
            for name in names:
                if name in evidence:
                    value = evidence[name]
                    break
            if isinstance(value, bool):
                return -1
            if isinstance(value, int):
                return value
            if isinstance(value, float) and math.isfinite(value) and value.is_integer():
                return int(value)
            return -1

        reasons: list[str] = []
        if count("independent_samples", "sample_count") < self.min_independent_samples:
            reasons.append("insufficient_independent_samples")
        if count("trades", "trade_count") < self.min_trades:
            reasons.append("insufficient_trades")
        drawdown = number("max_drawdown", "drawdown", default=float("nan"))
        if not math.isfinite(drawdown) or drawdown > self.max_drawdown:
            reasons.append("drawdown_limit")
        expectancy = number("expectancy", default=float("nan"))
        if not math.isfinite(expectancy) or expectancy < self.min_expectancy:
            reasons.append("expectancy_floor")
        confidence_lower_bound = number("confidence_lower_bound", "ci_lower_bound", default=float("nan"))
        if not math.isfinite(confidence_lower_bound) or confidence_lower_bound < self.min_confidence_lower_bound:
            reasons.append("confidence_lower_bound")
        stability = number("stability", "regime_stability", default=float("nan"))
        if not math.isfinite(stability) or stability < self.min_stability:
            reasons.append("stability_floor")
        calibration = number("calibration", "calibration_score", default=float("nan"))
        if not math.isfinite(calibration) or calibration < self.min_calibration:
            reasons.append("calibration_floor")
        liquidity = number("liquidity", "min_liquidity", default=float("nan"))
        if not math.isfinite(liquidity) or liquidity < self.min_liquidity:
            reasons.append("liquidity_floor")
        forward_duration = number("forward_duration_seconds", "paper_forward_duration_seconds", default=float("nan"))
        if not math.isfinite(forward_duration) or forward_duration < self.min_forward_duration_seconds:
            reasons.append("insufficient_paper_forward_duration")
        if count("regimes", "regime_count") < self.min_regimes:
            reasons.append("insufficient_regime_diversity")
        return tuple(dict.fromkeys(reasons))

    def as_record(self) -> dict[str, Any]:
        return {
            "min_independent_samples": self.min_independent_samples,
            "min_trades": self.min_trades,
            "max_drawdown": self.max_drawdown,
            "min_expectancy": self.min_expectancy,
            "min_confidence_lower_bound": self.min_confidence_lower_bound,
            "min_stability": self.min_stability,
            "min_calibration": self.min_calibration,
            "min_liquidity": self.min_liquidity,
            "min_forward_duration_seconds": self.min_forward_duration_seconds,
            "min_regimes": self.min_regimes,
        }


@dataclass(frozen=True, slots=True)
class CandidateLifecycle:
    candidate_id: str
    stage: CandidateStage
    payload: Mapping[str, Any]
    updated_at: Any = None

    @property
    def rejection_reason(self) -> str | None:
        value = self.payload.get("rejection_reason")
        return str(value) if value is not None else None

    def as_record(self) -> dict[str, Any]:
        return {
            "candidate_id": self.candidate_id,
            "stage": self.stage.value,
            "payload": dict(self.payload),
            "updated_at": self.updated_at,
            "rejection_reason": self.rejection_reason,
        }


class CandidateLifecycleManager:
    """Authoritative transition boundary for every candidate."""

    def __init__(self, store: AxiomStore, *, criteria: PromotionCriteria | None = None) -> None:
        self.store = store
        self.criteria = criteria or PromotionCriteria()

    def register_idea(self, candidate_id: str, payload: Mapping[str, Any] | None = None) -> CandidateLifecycle:
        identifier = str(candidate_id).strip()
        if not identifier:
            raise ValueError("candidate_id is required")
        existing = self.get(identifier)
        if existing is not None:
            return existing
        body = dict(payload or {})
        body.setdefault("candidate_id", identifier)
        body.setdefault("stage", CandidateStage.IDEA.value)
        self.store.save_candidate_lifecycle(identifier, CandidateStage.IDEA.value, body, reason="candidate registered")
        result = self.get(identifier)
        if result is None:
            raise RuntimeError("candidate lifecycle registration did not persist")
        return result

    def get(self, candidate_id: str) -> CandidateLifecycle | None:
        record = self.store.load_candidate_lifecycle(str(candidate_id))
        if not isinstance(record, Mapping):
            return None
        try:
            stage = CandidateStage(str(record["stage"]))
        except (KeyError, ValueError):
            raise ValueError(f"invalid persisted candidate stage: {candidate_id}") from None
        payload = record.get("payload", {})
        return CandidateLifecycle(str(record["candidate_id"]), stage, dict(payload) if isinstance(payload, Mapping) else {}, record.get("updated_at"))

    def advance(
        self,
        candidate_id: str,
        target: CandidateStage | str,
        evidence: Mapping[str, Any] | None = None,
        *,
        reason: str = "",
    ) -> CandidateLifecycle:
        current = self.get(candidate_id)
        if current is None:
            raise KeyError(candidate_id)
        try:
            target_stage = target if isinstance(target, CandidateStage) else CandidateStage(str(target))
        except ValueError:
            raise ValueError(f"unknown candidate stage: {target}") from None
        if current.stage is CandidateStage.REJECTED:
            raise ValueError("rejected candidates are terminal")
        if target_stage is CandidateStage.REJECTED:
            raise ValueError("use reject() to record a rejection")
        current_index = _STAGE_ORDER.index(current.stage)
        target_index = _STAGE_ORDER.index(target_stage)
        if target_index != current_index + 1:
            raise ValueError(f"candidate transition must advance one stage: {current.stage.value} -> {target_stage.value}")
        body = dict(current.payload)
        body.update(dict(evidence or {}))
        check_error = _stage_gate(target_stage, body)
        if check_error is not None:
            raise ValueError(check_error)
        if target_stage is CandidateStage.PAPER_PROMOTABLE:
            reasons = self.criteria.evaluate(body)
            if reasons:
                return self.reject(
                    candidate_id,
                    "; ".join(reasons),
                    evidence=body,
                    expected_stage=current.stage,
                    expected_payload=current.payload,
                )
        self.store.save_candidate_lifecycle(
            str(candidate_id),
            target_stage.value,
            body,
            from_stage=current.stage.value,
            reason=reason or f"advanced to {target_stage.value}",
        )
        result = self.get(str(candidate_id))
        if result is None:
            raise RuntimeError("candidate lifecycle transition did not persist")
        return result

    def reject(
        self,
        candidate_id: str,
        rejection_reason: str,
        *,
        evidence: Mapping[str, Any] | None = None,
        expected_stage: CandidateStage | str | None = None,
        expected_payload: Mapping[str, Any] | None = None,
    ) -> CandidateLifecycle:
        current = self.get(candidate_id)
        if current is None:
            raise KeyError(candidate_id)
        if expected_stage is not None:
            expected_stage_value = expected_stage.value if isinstance(expected_stage, CandidateStage) else str(expected_stage)
            if current.stage.value != expected_stage_value:
                raise RuntimeError(
                    f"stale candidate lifecycle writer: expected {expected_stage_value}, found {current.stage.value}"
                )
        if expected_payload is not None and dict(current.payload) != dict(expected_payload):
            raise RuntimeError("stale candidate lifecycle writer: payload changed")
        if current.stage is CandidateStage.REJECTED:
            return current
        reason = str(rejection_reason).strip()
        if not reason:
            raise ValueError("rejection_reason is required")
        body = dict(current.payload)
        body.update(dict(evidence or {}))
        body["rejection_reason"] = reason
        body["rejected_from"] = current.stage.value
        self.store.save_candidate_lifecycle(
            str(candidate_id),
            CandidateStage.REJECTED.value,
            body,
            from_stage=current.stage.value,
            reason=reason,
        )
        result = self.get(str(candidate_id))
        if result is None:
            raise RuntimeError("candidate rejection did not persist")
        return result

    def events(self, candidate_id: str | None = None, *, limit: int = 100) -> list[dict[str, Any]]:
        return self.store.list_candidate_lifecycle_events(candidate_id, limit=limit)

    def summary(self, *, limit: int = 100) -> list[dict[str, Any]]:
        records = self.store.load_candidate_lifecycle(limit=limit)
        records = records if isinstance(records, list) else []
        return [
            CandidateLifecycle(
                str(item["candidate_id"]),
                CandidateStage(str(item["stage"])),
                dict(item.get("payload", {})),
                item.get("updated_at"),
            ).as_record()
            for item in records
        ]


def _stage_gate(stage: CandidateStage, evidence: Mapping[str, Any]) -> str | None:
    required = {
        CandidateStage.SCHEMA_VALIDATED: "schema_valid",
        CandidateStage.BACKTESTED: "backtest_complete",
        CandidateStage.VALIDATED: "validation_complete",
        CandidateStage.ROBUSTNESS_CHECKED: "robustness_passed",
        CandidateStage.FROZEN: "frozen",
        CandidateStage.PAPER_FORWARD: "paper_forward_started",
    }.get(stage)
    if required is not None and evidence.get(required) is not True:
        return f"missing lifecycle evidence: {required}=true"
    if stage in {
        CandidateStage.VALIDATED,
        CandidateStage.ROBUSTNESS_CHECKED,
        CandidateStage.FROZEN,
        CandidateStage.PAPER_FORWARD,
        CandidateStage.PAPER_PROMOTABLE,
    } and evidence.get("holdout_used") is not False:
        return "holdout usage must be explicitly false before promotion evidence"
    if stage is CandidateStage.FROZEN:
        for name in ("strategy_hash", "model_hash", "config_hash", "risk_snapshot"):
            value = evidence.get(name)
            if value is None or value == "" or value == {}:
                return f"missing frozen evidence: {name}"
    return None


__all__ = ["CandidateLifecycle", "CandidateLifecycleManager", "CandidateStage", "PromotionCriteria"]
