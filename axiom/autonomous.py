"""Durable closed-loop autonomous research processing.

The processor is deliberately boring: queue leases, declarative plans,
deterministic backtests, ordered lifecycle writes, and paper-forward evidence.
Every external boundary is persisted or rejected; no live execution path exists.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import hashlib
import json
import re
import math
from statistics import mean
from typing import Any, Callable, Iterable, Mapping, Sequence

from .backtest import CryptoBacktester, PredictionMarketBacktester
from .forward import ForwardTestRegistry, _content_hash
from .director import compact_report, validate_hermes_proposal
from .domain import Fill, MarketType, ResearchQuality, SettlementState, ensure_utc, parse_timestamp, utc_now
from .evaluation import split_dataset
from .paper_engine import build_resolved_bet
from .lifecycle import CandidateLifecycle, CandidateLifecycleManager, CandidateStage, PromotionCriteria
from .metrics import expected_calibration_error
from .mutations import DeterministicMutationEngine, ExperimentBudget
from .research_bus import DurableResearchBus, ResearchQueueItem, ResearchQueueStatus
from .robustness import bootstrap_confidence_interval, minimum_sample_check, neighboring_parameter_stability
from .storage import AxiomStore
from .strategy import StrategyDefinition, load_strategy
from .experiment_plan import AUTONOMOUS_BUDGET_ID, ExperimentPlan, ExperimentPlanError, MAX_PLAN_VARIANTS


_MAX_QUEUE_RESULT_ITEMS = 64
_MAX_DATASET_ROWS = 100_000
_MAX_FORWARD_ROWS = 100_000


def _binding_value(value: Any) -> str | None:
    """Normalize a persisted binding value without turning ``None`` into text."""
    if value is None:
        return None
    text = str(value).strip()
    return text or None


class AutonomousResearchError(ValueError):
    """A deterministic, auditable queue rejection or unsupported operation."""

    def __init__(self, reason: str, detail: str) -> None:
        self.reason = str(reason).strip().upper() or "AUTONOMOUS_RESEARCH_ERROR"
        self.code = self.reason
        self.detail = str(detail).strip() or self.reason
        super().__init__(f"{self.reason}: {self.detail}")


@dataclass(frozen=True, slots=True)
class AutonomousResearchConfig:
    """Boundaries applied to every autonomous queue cycle and mutation."""

    max_items_per_cycle: int = 1
    lease_seconds: float = 300.0
    total_limit: int = 1000
    family_limit: int = 250
    max_plan_variants: int = 8
    max_children_per_parent: int = 2
    max_generation_depth: int = 2
    max_experiments_per_day: int = 250
    mutation_enabled: bool = True
    promotion_criteria: PromotionCriteria = field(default_factory=PromotionCriteria)

    def __post_init__(self) -> None:
        for name in (
            "max_items_per_cycle",
            "total_limit",
            "family_limit",
            "max_plan_variants",
            "max_children_per_parent",
            "max_generation_depth",
            "max_experiments_per_day",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")
        if self.max_items_per_cycle < 1:
            raise ValueError("max_items_per_cycle must be positive")
        if self.max_plan_variants < 1 or self.max_plan_variants > MAX_PLAN_VARIANTS:
            raise ValueError(f"max_plan_variants must be between one and {MAX_PLAN_VARIANTS}")
        lease = float(self.lease_seconds)
        if not math.isfinite(lease) or lease <= 0:
            raise ValueError("lease_seconds must be finite and positive")
        object.__setattr__(self, "lease_seconds", lease)
        if not isinstance(self.mutation_enabled, bool):
            raise ValueError("mutation_enabled must be boolean")
        if not isinstance(self.promotion_criteria, PromotionCriteria):
            raise ValueError("promotion_criteria must be PromotionCriteria")


@dataclass(frozen=True, slots=True)
class AutonomousQueueCycle:
    released: int
    claimed: int
    completed: int
    rejected: int
    failed: int
    results: tuple[Mapping[str, Any], ...] = ()

    def as_record(self) -> dict[str, Any]:
        return {
            "released": self.released,
            "claimed": self.claimed,
            "completed": self.completed,
            "rejected": self.rejected,
            "failed": self.failed,
            "results": [dict(item) for item in self.results],
            "paper_only": True,
        }


class AutonomousResearchProcessor:
    """Claim and execute work with paper-only forward evidence.

    The locked holdout is intentionally never consumed by this processor. It
    remains an immutable partition available only for a separately controlled
    human audit; no holdout result enters mutation, Hermes, or promotion data.
    """

    def __init__(
        self,
        store: AxiomStore,
        *,
        bus: DurableResearchBus | None = None,
        config: AutonomousResearchConfig | None = None,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self.store = store
        self.bus = bus or DurableResearchBus(store)
        self.config = config or AutonomousResearchConfig()
        self.clock = clock
        self.lifecycle = CandidateLifecycleManager(store, criteria=self.config.promotion_criteria)

    def process_pending(self, *, worker: str = "research-queue", now: datetime | None = None) -> AutonomousQueueCycle:
        """Claim at most the configured bounded number of items.

        Candidate, plan, budget, and queue completion writes share one SQLite
        transaction.  A crash before commit therefore leaves the lease to
        expire and retries the same deterministic identifiers without creating
        duplicate experiments or lifecycle transitions.
        """
        current = ensure_utc(now or self.clock())
        released = self.bus.resume_expired(now=current)
        claimed = completed = rejected = failed = 0
        results: list[Mapping[str, Any]] = []
        for _ in range(self.config.max_items_per_cycle):
            item = self.bus.claim(worker, lease_seconds=self.config.lease_seconds, now=current)
            if item is None:
                break
            claimed += 1
            phase_index = 0

            def phase_event(stage: str, detail: Any | None = None) -> None:
                nonlocal phase_index
                phase_index += 1
                self.store.record_research_queue_event(
                    item.item_id,
                    stage,
                    detail,
                    timestamp=current + timedelta(microseconds=phase_index),
                )
            try:
                with self.store.transaction():
                    phase_event("CLAIM", {"worker": worker, "item_type": item.item_type})
                    phase_event("VALIDATE", {"item_type": item.item_type})
                    result = self._process_item(item, current)
                    result = _bounded_queue_result(result)
                    phase_event(
                        "ACCEPT" if result.get("accepted") is not False else "REJECT",
                        {"reason_code": result.get("reason_code")},
                    )
                    for stage in ("BOUNDED_EXPERIMENT", "TEST", "RESULT", "LIFECYCLE"):
                        phase_event(stage, {"item_type": item.item_type})
                    accepted = result.get("accepted") is not False
                    phase_event(
                        "COMPLETE",
                        {"item_type": item.item_type, "accepted": accepted},
                    )
                    self.bus.complete(
                        item.item_id,
                        status=ResearchQueueStatus.COMPLETED if accepted else ResearchQueueStatus.REJECTED,
                        result=result,
                        error=None if accepted else str(result.get("reason") or result.get("status") or "rejected"),
                        worker=worker,
                        now=current + timedelta(microseconds=phase_index + 1),
                    )
                if accepted:
                    completed += 1
                else:
                    rejected += 1
                results.append(result)
            except AutonomousResearchError as exc:
                result = _bounded_queue_result(
                    {
                        "accepted": False,
                        "reason_code": exc.reason,
                        "reason": exc.detail,
                        "item_type": item.item_type,
                        "item_id": item.item_id,
                        "paper_only": True,
                    }
                )
                try:
                    with self.store.transaction():
                        phase_event(
                            "REJECT",
                            {"reason_code": exc.reason, "item_type": item.item_type},
                        )
                        self.bus.complete(
                            item.item_id,
                            status=ResearchQueueStatus.REJECTED,
                            result=result,
                            error=str(exc),
                            worker=worker,
                            now=current + timedelta(microseconds=phase_index + 1),
                        )
                    rejected += 1
                    results.append(result)
                except RuntimeError:
                    # The lease may have expired while an operator paused the
                    # process.  The storage lease owner remains authoritative;
                    # a later cycle will release and reclaim it safely.
                    continue
            except Exception as exc:
                result = _bounded_queue_result(
                    {
                        "accepted": False,
                        "reason_code": "PROCESSING_FAILED",
                        "reason": str(exc),
                        "item_type": item.item_type,
                        "item_id": item.item_id,
                        "paper_only": True,
                    }
                )
                try:
                    with self.store.transaction():
                        phase_event(
                            "FAILED",
                            {"reason_code": "PROCESSING_FAILED", "item_type": item.item_type},
                        )
                        self.bus.complete(
                            item.item_id,
                            status=ResearchQueueStatus.FAILED,
                            result=result,
                            error=str(exc),
                            worker=worker,
                            now=current + timedelta(microseconds=phase_index + 1),
                        )
                    failed += 1
                    results.append(result)
                except RuntimeError:
                    continue
        return AutonomousQueueCycle(released, claimed, completed, rejected, failed, tuple(results))

    def reevaluate_forward_candidates(self, *, now: datetime | None = None) -> tuple[Mapping[str, Any], ...]:
        """Update active forward evidence and apply configured promotion gates."""
        current = ensure_utc(now or self.clock())
        output: list[Mapping[str, Any]] = []
        records = self.store.load_candidate_lifecycle(limit=10_000)
        if not isinstance(records, list):
            return ()
        for record in records:
            if not isinstance(record, Mapping) or record.get("stage") != CandidateStage.PAPER_FORWARD.value:
                continue
            candidate_id = str(record.get("candidate_id", "")).strip()
            if not candidate_id:
                continue
            try:
                with self.store.transaction():
                    evidence = self._forward_evidence(record, current)
                    candidate = self.lifecycle.get(candidate_id)
                    if candidate is None:
                        continue
                    existing = dict(candidate.payload)
                    changed = any(existing.get(key) != value for key, value in evidence.items())
                    if changed:
                        candidate = self.lifecycle.record_evidence(
                            candidate_id,
                            evidence,
                            expected_stage=CandidateStage.PAPER_FORWARD,
                            reason="forward paper evidence update",
                        )
                    reasons = self.config.promotion_criteria.evaluate({**existing, **evidence})
                    hard_reasons = self.config.promotion_criteria.hard_rejection_reasons(evidence)
                    if hard_reasons:
                        candidate = self.lifecycle.reject(
                            candidate_id,
                            hard_reasons[0],
                            evidence=evidence,
                            expected_stage=CandidateStage.PAPER_FORWARD,
                            expected_payload=candidate.payload,
                        )
                    elif not reasons:
                        candidate = self.lifecycle.advance(
                            candidate_id,
                            CandidateStage.PAPER_PROMOTABLE,
                            {**evidence, "holdout_used": False},
                            reason="paper-forward criteria passed; human review required",
                        )
                    output.append(
                        {
                            "candidate_id": candidate_id,
                            "stage": candidate.stage.value,
                            "forward_evidence": _compact_evidence(evidence),
                            "promotion_reasons": list(reasons),
                        }
                    )
            except (KeyError, RuntimeError, ValueError) as exc:
                output.append({"candidate_id": candidate_id, "stage": "PAPER_FORWARD", "error": str(exc)})
        return tuple(output)

    def _process_item(self, item: ResearchQueueItem, now: datetime) -> Mapping[str, Any]:
        handlers = {
            "hypothesis": self._process_hypothesis,
            "candidate": self._process_candidate,
            "report": self._process_report,
            "review_request": self._process_review_request,
            "experiment_result": self._process_experiment_result,
        }
        handler = handlers.get(str(item.item_type).strip().lower())
        if handler is None:
            raise AutonomousResearchError("UNSUPPORTED_ITEM_TYPE", f"unsupported research item type {item.item_type!r}")
        return handler(item, now)

    def _process_hypothesis(self, item: ResearchQueueItem, now: datetime) -> Mapping[str, Any]:
        proposal = _normalize_hypothesis_payload(item.payload, item)
        validation = validate_hermes_proposal(proposal)
        if not validation.accepted:
            reason_code = next(
                (reason for reason in validation.reasons if re.fullmatch(r"[A-Z][A-Z0-9_]{2,63}", reason)),
                "INVALID_PROPOSAL",
            )
            raise AutonomousResearchError(reason_code, "; ".join(validation.reasons))
        try:
            plan = ExperimentPlan.from_proposal(validation.normalized or proposal)
        except ExperimentPlanError as exc:
            raise AutonomousResearchError(exc.reason, exc.detail) from exc
        if plan.max_variants > self.config.max_plan_variants:
            raise AutonomousResearchError(
                "EXPERIMENT_BUDGET_EXCEEDED",
                f"plan requests {plan.max_variants} variants; node limit is {self.config.max_plan_variants}",
            )
        rows, split = self._load_split(plan)
        if len(split.validation) < plan.min_samples:
            raise AutonomousResearchError(
                "INSUFFICIENT_DATA",
                f"validation sample count {len(split.validation)} is below {plan.min_samples}",
            )
        variants = plan.variants()
        if not variants:
            raise AutonomousResearchError("EXPERIMENT_BUDGET_EXCEEDED", "plan produced no bounded variants")
        prepared: list[dict[str, Any]] = []
        validation_scores: dict[str, float] = {}
        for parameters in variants:
            candidate_id = _candidate_id(plan, parameters, generation=0)
            strategy = plan.strategy_for(parameters, candidate_id)
            try:
                evaluation = self._evaluate_datasets(plan, strategy, split.train, split.validation)
            except AutonomousResearchError:
                raise
            except (KeyError, TypeError, ValueError) as exc:
                raise AutonomousResearchError("INVALID_DATASET", str(exc)) from exc
            prepared.append(
                {
                    "candidate_id": candidate_id,
                    "strategy": strategy,
                    "parameters": dict(parameters),
                    "variant_id": plan.variant_id(parameters),
                    "evaluation": evaluation,
                }
            )
            validation_scores[candidate_id] = _finite(evaluation["validation"].get("expectancy"), 0.0)
        self.store.save_experiment_plan(
            plan.plan_id,
            plan.as_dict(),
            hypothesis_id=plan.hypothesis_id,
            plan_hash=plan.plan_hash,
            status="ACCEPTED",
            timestamp=now,
        )
        results: list[dict[str, Any]] = []
        for candidate in prepared:
            candidate_id = candidate["candidate_id"]
            strategy: StrategyDefinition = candidate["strategy"]
            payload = self._candidate_payload(
                plan,
                candidate_id,
                strategy,
                candidate["parameters"],
                variant_id=candidate["variant_id"],
                generation=0,
                lineage=(),
            )
            self._reserve_candidate(plan, candidate_id, now)
            self.store.save_strategy_if_absent(strategy.id, strategy.to_dict())
            self.store.save_experiment_if_absent(
                candidate_id,
                {
                    "status": "IDEA",
                    "run_id": candidate_id,
                    "candidate_id": candidate_id,
                    "hypothesis_id": plan.hypothesis_id,
                    "plan_id": plan.plan_id,
                    "plan_hash": plan.plan_hash,
                    "dataset_id": plan.dataset_id,
                    "dataset_version": plan.dataset_version,
                    "variant": candidate["parameters"],
                    "paper_only": True,
                },
                strategy_id=strategy.id,
            )
            self.lifecycle.register_idea(candidate_id, payload)
            try:
                result = self._advance_candidate(
                    plan,
                    candidate_id,
                    strategy,
                    candidate["evaluation"],
                    variant_count=len(prepared),
                    validation_scores=validation_scores,
                    now=now,
                    generation=0,
                    lineage=(),
                )
            except AutonomousResearchError as exc:
                current = self.lifecycle.get(candidate_id)
                if current is not None and current.stage is not CandidateStage.REJECTED:
                    self.lifecycle.reject(candidate_id, exc.reason, evidence={"reason_detail": exc.detail})
                result = {"candidate_id": candidate_id, "stage": CandidateStage.REJECTED.value, "reason": exc.detail}
            results.append(result)
        mutations = self._generate_mutations(plan, prepared, validation_scores, now, lineage=())
        summary = self._hypothesis_result(plan, results, mutations)
        self.store.save_experiment_plan(
            plan.plan_id,
            plan.as_dict(),
            hypothesis_id=plan.hypothesis_id,
            plan_hash=plan.plan_hash,
            status="COMPLETED",
            result=summary,
            timestamp=now,
        )
        self.store.save_report_if_absent(
            "autonomous-" + plan.plan_id,
            {"report_type": "autonomous_experiment_result", **summary},
            experiment_id=plan.plan_id,
        )
        return summary

    def _process_candidate(self, item: ResearchQueueItem, now: datetime) -> Mapping[str, Any]:
        payload = dict(item.payload)
        candidate_id = str(payload.get("candidate_id", "")).strip()
        if not candidate_id:
            raise AutonomousResearchError("INVALID_CANDIDATE", "candidate_id is required")
        raw_generation = payload.get("generation", 0)
        if isinstance(raw_generation, bool) or not isinstance(raw_generation, int) or raw_generation < 0:
            raise AutonomousResearchError("INVALID_CANDIDATE", "generation must be a non-negative integer")
        generation = int(raw_generation)
        if generation > self.config.max_generation_depth:
            raise AutonomousResearchError(
                "GENERATION_DEPTH_EXCEEDED",
                f"candidate generation {generation} exceeds node limit {self.config.max_generation_depth}",
            )
        run_record = self.store.load_experiment(candidate_id)
        if not isinstance(run_record, Mapping):
            raise AutonomousResearchError(
                "UNAUTHENTICATED_CANDIDATE",
                "candidate queue items require a persisted worker-generated run",
            )
        persisted_run_id = _binding_value(run_record.get("run_id"))
        persisted_candidate_id = _binding_value(run_record.get("candidate_id"))
        if persisted_run_id != candidate_id or persisted_candidate_id != candidate_id:
            raise AutonomousResearchError(
                "CANDIDATE_BINDING_MISMATCH",
                "persisted worker run identity does not match candidate_id",
            )
        raw_lineage = payload.get("lineage", ())
        if not isinstance(raw_lineage, (list, tuple)) or len(raw_lineage) > 256:
            raise AutonomousResearchError("INVALID_CANDIDATE", "lineage must be a bounded list")
        lineage = tuple(str(value).strip() for value in raw_lineage if str(value).strip())
        plan_id = str(payload.get("plan_id", "")).strip()
        plan_record = self.store.load_experiment_plan(plan_id) if plan_id else None
        if not isinstance(plan_record, Mapping):
            raise AutonomousResearchError(
                "UNAUTHENTICATED_CANDIDATE",
                "candidate must reference a persisted experiment plan",
            )
        raw_plan = plan_record.get("plan")
        if not isinstance(raw_plan, Mapping):
            raise AutonomousResearchError("INVALID_PLAN", "persisted candidate experiment plan is invalid")
        try:
            plan = ExperimentPlan.from_mapping(raw_plan, hypothesis_id=str(payload.get("hypothesis_id", "")).strip() or None)
        except ExperimentPlanError as exc:
            raise AutonomousResearchError(exc.reason, exc.detail) from exc
        if plan.max_variants > self.config.max_plan_variants:
            raise AutonomousResearchError(
                "EXPERIMENT_BUDGET_EXCEEDED",
                f"plan requests {plan.max_variants} variants; node limit is {self.config.max_plan_variants}",
            )
        raw_max_variants = payload.get("max_variants")
        if raw_max_variants is not None and (
            isinstance(raw_max_variants, bool)
            or not isinstance(raw_max_variants, int)
            or raw_max_variants < 1
            or raw_max_variants > self.config.max_plan_variants
            or raw_max_variants > plan.max_variants
        ):
            raise AutonomousResearchError(
                "EXPERIMENT_BUDGET_EXCEEDED",
                "candidate max_variants exceeds the persisted plan or node limit",
            )
        self._validate_worker_candidate_binding(
            payload,
            candidate_id=candidate_id,
            generation=generation,
            lineage=lineage,
            plan=plan,
            plan_record=plan_record,
            run_record=run_record,
        )
        crypto_binding = self._crypto_binding(plan)
        strategy_value = payload.get("strategy", payload.get("strategy_document"))
        if not isinstance(strategy_value, Mapping):
            raise AutonomousResearchError("UNSUPPORTED_STRATEGY_FAMILY", "candidate strategy document is missing")
        try:
            supplied_strategy = load_strategy(strategy_value)
            raw_parameters = payload.get("parameters")
            if not isinstance(raw_parameters, Mapping):
                raise AutonomousResearchError("CANDIDATE_BINDING_MISMATCH", "candidate parameters are required")
            parameters = dict(raw_parameters)
            if dict(supplied_strategy.parameters) != parameters:
                raise AutonomousResearchError(
                    "CANDIDATE_BINDING_MISMATCH",
                    "candidate strategy parameters do not match candidate parameters",
                )
            strategy = plan.strategy_for(parameters, candidate_id)
            supplied_document = supplied_strategy.to_dict()
            expected_document = strategy.to_dict()
            supplied_document.pop("strategy_id", None)
            expected_document.pop("strategy_id", None)
            if supplied_document != expected_document:
                raise AutonomousResearchError(
                    "UNSUPPORTED_STRATEGY_FAMILY",
                    "candidate strategy does not match its declarative experiment plan",
                )
        except AutonomousResearchError:
            raise
        except Exception as exc:
            raise AutonomousResearchError("UNSUPPORTED_STRATEGY_FAMILY", str(exc)) from exc
        if generation == 0:
            raise AutonomousResearchError(
                "UNAUTHENTICATED_CANDIDATE",
                "generation-zero candidates are produced inline, not accepted from the queue",
            )
        else:
            parent_id = _binding_value(payload.get("parent_id"))
            if _binding_value(run_record.get("strategy_id")) != candidate_id:
                raise AutonomousResearchError(
                    "CANDIDATE_BINDING_MISMATCH",
                    "mutation strategy identity is not the persisted candidate identity",
                )
            persisted_variant = run_record.get("variant")
            if not isinstance(persisted_variant, Mapping) or dict(persisted_variant) != parameters:
                raise AutonomousResearchError(
                    "CANDIDATE_BINDING_MISMATCH",
                    "mutation parameters do not match the persisted worker variant",
                )
            persisted_record_strategy = run_record.get("strategy")
            if not isinstance(persisted_record_strategy, Mapping):
                raise AutonomousResearchError(
                    "CANDIDATE_BINDING_MISMATCH",
                    "mutation strategy document is missing from the persisted worker run",
                )
            stored_document = self.store.load_strategy(candidate_id)
            if not parent_id or not isinstance(stored_document, Mapping):
                raise AutonomousResearchError(
                    "CANDIDATE_BINDING_MISMATCH",
                    "mutation candidate lacks persisted parent strategy",
                )
            try:
                stored_strategy = load_strategy(stored_document)
                recorded_strategy = load_strategy(persisted_record_strategy)
            except Exception as exc:
                raise AutonomousResearchError("CANDIDATE_BINDING_MISMATCH", "persisted mutation strategy is invalid") from exc
            supplied_document = supplied_strategy.to_dict()
            persisted_document = stored_strategy.to_dict()
            recorded_document = recorded_strategy.to_dict()
            supplied_document.pop("strategy_id", None)
            persisted_document.pop("strategy_id", None)
            recorded_document.pop("strategy_id", None)
            if supplied_document != persisted_document or supplied_document != recorded_document:
                raise AutonomousResearchError(
                    "CANDIDATE_BINDING_MISMATCH",
                    "mutation strategy does not match the persisted worker strategy",
                )
            expected_candidate_id = _mutation_candidate_id(parent_id, generation, stored_strategy)
            if candidate_id != expected_candidate_id:
                raise AutonomousResearchError(
                    "CANDIDATE_BINDING_MISMATCH",
                    "mutation candidate_id is not deterministic for its parent lineage",
                )
        rows, split = self._load_split(plan)
        try:
            evaluation = self._evaluate_datasets(plan, strategy, split.train, split.validation)
        except AutonomousResearchError:
            raise
        except (KeyError, TypeError, ValueError) as exc:
            raise AutonomousResearchError("INVALID_DATASET", str(exc)) from exc
        # Direct candidate submissions do not pass through hypothesis
        # expansion; reserve their unit inside the queue transaction too.
        self._reserve_candidate(plan, candidate_id, now)
        current = self.lifecycle.get(candidate_id)
        if current is None:
            candidate_record = {
                **payload,
                "candidate_id": candidate_id,
                "plan_id": plan.plan_id,
                "plan_hash": plan.plan_hash,
                "holdout_used": False,
            }
            if crypto_binding is not None:
                candidate_record["crypto_provenance"] = crypto_binding
            self.lifecycle.register_idea(candidate_id, candidate_record)
        result = self._advance_candidate(
            plan,
            candidate_id,
            strategy,
            evaluation,
            variant_count=max(1, len(plan.variants())),
            validation_scores={candidate_id: _finite(evaluation["validation"].get("expectancy"), 0.0)},
            now=now,
            generation=generation,
            lineage=lineage,
            crypto_binding=crypto_binding,
        )
        if (
            result.get("stage") in {
                CandidateStage.ROBUSTNESS_CHECKED.value,
                CandidateStage.FROZEN.value,
                CandidateStage.PAPER_FORWARD.value,
                CandidateStage.PAPER_PROMOTABLE.value,
            }
            and generation < self.config.max_generation_depth
        ):
            mutation_ids = self._generate_mutations(
                plan,
                (
                    {
                        "candidate_id": candidate_id,
                        "strategy": strategy,
                        "evaluation": evaluation,
                    },
                ),
                {candidate_id: _finite(evaluation["validation"].get("expectancy"), 0.0)},
                now,
                lineage=lineage,
            )
            if mutation_ids:
                result = {**dict(result), "mutation_candidates": list(mutation_ids)}
        return result

    def _validate_worker_candidate_binding(
        self,
        payload: Mapping[str, Any],
        *,
        candidate_id: str,
        generation: int,
        lineage: Sequence[str],
        plan: ExperimentPlan,
        plan_record: Mapping[str, Any],
        run_record: Mapping[str, Any],
    ) -> None:
        """Accept only candidate payloads created by the durable worker path."""
        for field, expected in (
            ("plan_id", plan.plan_id),
            ("plan_hash", plan.plan_hash),
            ("dataset_id", plan.dataset_id),
            ("dataset_version", plan.dataset_version),
        ):
            supplied = _binding_value(payload.get(field))
            persisted = _binding_value(run_record.get(field))
            if supplied != expected or persisted != expected:
                raise AutonomousResearchError(
                    "CANDIDATE_BINDING_MISMATCH",
                    f"candidate {field} does not match the persisted experiment plan",
                )
        if _binding_value(plan_record.get("plan_id")) != plan.plan_id or _binding_value(plan_record.get("plan_hash")) != plan.plan_hash:
            raise AutonomousResearchError("CANDIDATE_BINDING_MISMATCH", "candidate plan identity is not exact")

        stored_generation = run_record.get("generation")
        if isinstance(stored_generation, bool) or not isinstance(stored_generation, int) or stored_generation != generation:
            raise AutonomousResearchError("CANDIDATE_BINDING_MISMATCH", "candidate generation is not the persisted worker generation")
        stored_lineage = run_record.get("lineage", ())
        if not isinstance(stored_lineage, (list, tuple)):
            raise AutonomousResearchError("CANDIDATE_BINDING_MISMATCH", "persisted candidate lineage is invalid")
        normalized_stored_lineage = tuple(str(value).strip() for value in stored_lineage if str(value).strip())
        if normalized_stored_lineage != tuple(lineage):
            raise AutonomousResearchError("CANDIDATE_BINDING_MISMATCH", "candidate lineage is not the persisted worker lineage")
        if generation == 0:
            if _binding_value(payload.get("parent_id")) or lineage:
                raise AutonomousResearchError("CANDIDATE_BINDING_MISMATCH", "generation-zero candidates cannot declare lineage")
        else:
            parent_id = _binding_value(payload.get("parent_id"))
            persisted_parent_id = _binding_value(run_record.get("parent_id"))
            if not parent_id or parent_id != persisted_parent_id or parent_id not in lineage:
                raise AutonomousResearchError("CANDIDATE_BINDING_MISMATCH", "mutation parent lineage is not exact")
            if self.lifecycle.get(parent_id) is None:
                raise AutonomousResearchError("CANDIDATE_BINDING_MISMATCH", "mutation parent is not persisted")

    def _process_report(self, item: ResearchQueueItem, now: datetime) -> Mapping[str, Any]:
        payload = compact_report(item.payload)
        report_id = str(payload.get("report_id", item.item_id)).strip() if isinstance(payload, Mapping) else item.item_id
        if not report_id:
            report_id = item.item_id
        self.store.save_report_if_absent(report_id, {"report_type": "informational", "payload": payload})
        return {"accepted": True, "kind": "report", "report_id": report_id, "persisted": True, "paper_only": True}

    def _process_review_request(self, item: ResearchQueueItem, now: datetime) -> Mapping[str, Any]:
        payload = item.payload
        candidate_id = str(payload.get("candidate_id", "")).strip()
        hypothesis_id = str(payload.get("hypothesis_id", "")).strip()
        candidate = self.lifecycle.get(candidate_id) if candidate_id else None
        plan_rows = self.store.list_experiment_plans(hypothesis_id=hypothesis_id, limit=32) if hypothesis_id else []
        response = {
            "accepted": True,
            "kind": "review_request",
            "candidate_id": candidate_id or None,
            "hypothesis_id": hypothesis_id or None,
            "candidate_stage": candidate.stage.value if candidate is not None else None,
            "plans": len(plan_rows),
            "status": "review_output_ready",
            "paper_only": True,
        }
        self.store.save_report_if_absent("review-" + item.item_id, {"report_type": "review", **response})
        return response

    def _process_experiment_result(self, item: ResearchQueueItem, now: datetime) -> Mapping[str, Any]:
        """Attach only evidence derived from a persisted, bound paper run.

        Queue payloads are untrusted input.  In particular, ``result`` and
        ``metrics`` are never copied into lifecycle evidence: a caller can
        submit either field without having run an experiment.  The queue item
        must identify an immutable persisted experiment and the exact plan and
        dataset recorded for the candidate.  Forward metrics are then
        recomputed from the paper observations, execution events, and ledger.
        """
        payload = item.payload
        candidate_id = str(payload.get("candidate_id", "")).strip()
        if not candidate_id:
            raise AutonomousResearchError("INVALID_RESULT", "experiment_result requires candidate_id")
        candidate = self.lifecycle.get(candidate_id)
        if candidate is None:
            raise AutonomousResearchError("INVALID_RESULT", f"unknown candidate {candidate_id}")

        experiment = self.store.load_experiment(candidate_id)
        if not isinstance(experiment, Mapping):
            raise AutonomousResearchError(
                "UNAUTHENTICATED_RESULT",
                "experiment_result requires a persisted immutable worker run",
            )
        self._validate_result_binding(candidate_id, candidate.payload, payload, experiment)

        forward_id = str(candidate.payload.get("forward_test_id", "")).strip()
        if str(payload.get("forward_test_id", "")).strip() != forward_id:
            raise AutonomousResearchError(
                "UNAUTHENTICATED_RESULT",
                "experiment_result forward_test_id does not match the candidate run",
            )
        if candidate.stage is not CandidateStage.PAPER_FORWARD or not forward_id:
            raise AutonomousResearchError(
                "UNTRUSTED_RESULT",
                "experiment_result has no persisted paper forward run to evaluate",
            )
        registry = ForwardTestRegistry(self.store)
        spec = registry.get(forward_id)
        if spec is None:
            raise AutonomousResearchError("UNAUTHENTICATED_RESULT", f"missing persisted forward test {forward_id}")
        self._validate_forward_result_binding(candidate.payload, spec)

        observations = self.store.list_paper_observations(forward_id, limit=_MAX_FORWARD_ROWS)
        execution_events = self.store.list_paper_execution_events(forward_id, limit=_MAX_FORWARD_ROWS)
        ledgers = self.store.list_paper_bet_ledger(forward_id, limit=_MAX_FORWARD_ROWS)
        if not observations and not execution_events and not ledgers:
            raise AutonomousResearchError(
                "UNTRUSTED_RESULT",
                "experiment_result has no persisted paper observations or outcomes",
            )

        # The caller-supplied result/metrics is intentionally ignored.  This
        # is the sole evidence path for a queued experiment result.
        evidence = self._forward_evidence(candidate.as_record(), now)
        evidence["result_attached"] = True
        evidence["holdout_used"] = False
        candidate = self.lifecycle.record_evidence(
            candidate_id,
            evidence,
            expected_stage=CandidateStage.PAPER_FORWARD,
            reason="bound paper result evaluated from persisted observations",
        )
        self._evaluate_forward_candidate(candidate_id, now)
        response = {
            "accepted": True,
            "kind": "experiment_result",
            "candidate_id": candidate_id,
            "stage": (self.lifecycle.get(candidate_id) or candidate).stage.value,
            "attached": True,
            "paper_only": True,
            "evidence_source": "persisted_paper_observations",
        }
        self.store.save_report_if_absent("result-" + item.item_id, {"report_type": "experiment_result", **response})
        return response

    def _validate_result_binding(
        self,
        candidate_id: str,
        candidate_payload: Mapping[str, Any],
        result_payload: Mapping[str, Any],
        experiment: Mapping[str, Any],
    ) -> None:
        """Require queue identity and immutable plan/dataset bindings to agree."""
        run_id = str(result_payload.get("run_id", result_payload.get("experiment_id", ""))).strip()
        if run_id != candidate_id:
            raise AutonomousResearchError(
                "UNAUTHENTICATED_RESULT",
                "experiment_result run_id does not match the persisted worker run",
            )
        for identity_field in ("run_id", "experiment_id"):
            supplied_identity = result_payload.get(identity_field)
            if supplied_identity is not None and str(supplied_identity).strip() != candidate_id:
                raise AutonomousResearchError(
                    "UNAUTHENTICATED_RESULT",
                    f"experiment_result {identity_field} does not match the persisted worker run",
                )
        persisted_run_id = str(experiment.get("run_id", candidate_id)).strip()
        if persisted_run_id != candidate_id:
            raise AutonomousResearchError("RESULT_BINDING_MISMATCH", "persisted worker run identity does not match")
        experiment_candidate = str(experiment.get("candidate_id", candidate_id)).strip()
        if experiment_candidate != candidate_id:
            raise AutonomousResearchError("RESULT_BINDING_MISMATCH", "persisted worker run candidate_id does not match")

        plan_id = _binding_value(candidate_payload.get("plan_id"))
        plan_hash = _binding_value(candidate_payload.get("plan_hash"))
        dataset_id = _binding_value(candidate_payload.get("dataset_id"))
        dataset_version = _binding_value(candidate_payload.get("dataset_version"))
        if not plan_id or not plan_hash or not dataset_version:
            raise AutonomousResearchError(
                "RESULT_BINDING_MISMATCH",
                "candidate lacks exact persisted plan and dataset binding",
            )
        for field, expected in (
            ("plan_id", plan_id),
            ("plan_hash", plan_hash),
            ("dataset_id", dataset_id),
            ("dataset_version", dataset_version),
        ):
            supplied = _binding_value(result_payload.get(field))
            if supplied != expected:
                raise AutonomousResearchError(
                    "RESULT_BINDING_MISMATCH",
                    f"experiment_result {field} does not match the candidate binding",
                )
            persisted = _binding_value(experiment.get(field))
            if persisted != expected:
                raise AutonomousResearchError(
                    "RESULT_BINDING_MISMATCH",
                    f"persisted worker run {field} does not match the candidate binding",
                )

        plan_record = self.store.load_experiment_plan(plan_id)
        if not isinstance(plan_record, Mapping):
            raise AutonomousResearchError("RESULT_BINDING_MISMATCH", f"missing persisted experiment plan {plan_id}")
        if _binding_value(plan_record.get("plan_hash")) != plan_hash:
            raise AutonomousResearchError("RESULT_BINDING_MISMATCH", "persisted experiment plan hash does not match")
        raw_plan = plan_record.get("plan")
        if not isinstance(raw_plan, Mapping):
            raise AutonomousResearchError("RESULT_BINDING_MISMATCH", "persisted experiment plan is invalid")
        try:
            plan = ExperimentPlan.from_mapping(raw_plan)
        except ExperimentPlanError as exc:
            raise AutonomousResearchError("RESULT_BINDING_MISMATCH", str(exc)) from exc
        if plan.plan_id != plan_id or plan.plan_hash != plan_hash:
            raise AutonomousResearchError("RESULT_BINDING_MISMATCH", "experiment plan identity is not exact")
        if _binding_value(plan.dataset_id) != dataset_id or plan.dataset_version != dataset_version:
            raise AutonomousResearchError("RESULT_BINDING_MISMATCH", "experiment plan dataset binding is not exact")

    @staticmethod
    def _validate_forward_result_binding(candidate_payload: Mapping[str, Any], spec: Any) -> None:
        forward_id = str(candidate_payload.get("forward_test_id", "")).strip()
        if str(getattr(spec, "experiment_id", "")).strip() != forward_id:
            raise AutonomousResearchError("RESULT_BINDING_MISMATCH", "forward-test identity does not match candidate")
        config = getattr(spec, "config", {})
        if not isinstance(config, Mapping):
            raise AutonomousResearchError("RESULT_BINDING_MISMATCH", "persisted forward-test config is invalid")
        for field in ("plan_id", "dataset_id", "dataset_version"):
            expected = _binding_value(candidate_payload.get(field))
            if _binding_value(config.get(field)) != expected:
                raise AutonomousResearchError(
                    "RESULT_BINDING_MISMATCH",
                    f"forward-test {field} does not match the candidate binding",
                )

    def _crypto_binding(self, plan: ExperimentPlan) -> dict[str, Any] | None:
        """Resolve and validate immutable crypto inputs from the persisted store."""
        if plan.market_type is not MarketType.CRYPTO_SPOT:
            return None
        dataset_id = plan.dataset_id
        dataset_version = plan.dataset_version
        timeframe = plan.dataset_timeframe
        source = plan.dataset_source
        survivorship = plan.dataset_survivorship
        if not dataset_id or not dataset_version or not timeframe or not source or not survivorship:
            raise AutonomousResearchError(
                "INSUFFICIENT_DATA",
                "crypto plans require exact dataset id, version, timeframe, source, and survivorship provenance",
            )
        if dataset_version.lower() in {"latest", "current", "default", "unversioned"}:
            raise AutonomousResearchError("INSUFFICIENT_DATA", "crypto dataset_version must be immutable and versioned")
        catalog_loader = getattr(self.store, "load_dataset_catalog", None)
        if not callable(catalog_loader):
            raise AutonomousResearchError("INSUFFICIENT_DATA", "crypto research requires a persisted dataset catalog")
        catalog = catalog_loader(dataset_id, dataset_version)
        if not isinstance(catalog, Mapping):
            raise AutonomousResearchError(
                "INSUFFICIENT_DATA",
                f"no exact dataset catalog {dataset_id}/{dataset_version}",
            )
        catalog_id = str(catalog.get("dataset_id", "")).strip()
        catalog_version = str(catalog.get("dataset_version", catalog.get("version", ""))).strip()
        if catalog_id != dataset_id or catalog_version != dataset_version:
            raise AutonomousResearchError("CRYPTO_PROVENANCE_MISMATCH", "dataset catalog identity does not match the plan")
        if str(catalog.get("market_type", "")).strip().lower() != MarketType.CRYPTO_SPOT.value:
            raise AutonomousResearchError("CRYPTO_PROVENANCE_MISMATCH", "dataset catalog market_type is not crypto_spot")
        catalog_timeframe = str(catalog.get("timeframe", "")).strip()
        if catalog_timeframe != timeframe:
            raise AutonomousResearchError("CRYPTO_PROVENANCE_MISMATCH", "dataset timeframe does not match the plan")
        catalog_source = str(catalog.get("provider", catalog.get("source", ""))).strip()
        if catalog_source != source:
            raise AutonomousResearchError("CRYPTO_PROVENANCE_MISMATCH", "dataset source does not match the plan")
        catalog_source_type = str(catalog.get("source_type", "")).strip().upper()
        declared_source_type = plan.dataset_source_type
        if declared_source_type and catalog_source_type != declared_source_type.strip().upper():
            raise AutonomousResearchError("CRYPTO_PROVENANCE_MISMATCH", "dataset source_type does not match the plan")
        if catalog_source_type != "HISTORICAL":
            raise AutonomousResearchError(
                "CRYPTO_PROVENANCE_MISMATCH",
                "crypto autonomous research accepts historical datasets only",
            )
        catalog_metadata = catalog.get("metadata")
        catalog_metadata = dict(catalog_metadata) if isinstance(catalog_metadata, Mapping) else {}
        catalog_survivorship = catalog_metadata.get(
            "survivorship_bias",
            catalog_metadata.get("survivorship", catalog_metadata.get("survivorship_label")),
        )
        if catalog_survivorship is None or str(catalog_survivorship).strip() != survivorship:
            raise AutonomousResearchError("CRYPTO_PROVENANCE_MISMATCH", "dataset survivorship provenance does not match the plan")

        universe_id = plan.universe_id
        universe_version = plan.universe_version
        universe_hash = plan.universe_snapshot_hash
        if not universe_id or not universe_version or not universe_hash:
            raise AutonomousResearchError(
                "INSUFFICIENT_DATA",
                "crypto plans require exact persisted universe id, version, and snapshot_hash",
            )
        if any(
            value.lower() in {"latest", "current", "default", "unversioned"}
            for value in (universe_version, universe_hash)
        ):
            raise AutonomousResearchError("INSUFFICIENT_DATA", "crypto universe binding must be immutable and versioned")
        universe_document = dict(plan.universe or {})
        universe_dataset_id = str(universe_document.get("dataset_id") or f"universe:{universe_id}").strip()
        universe_catalog = catalog_loader(universe_dataset_id, universe_version)
        if not isinstance(universe_catalog, Mapping):
            raise AutonomousResearchError(
                "INSUFFICIENT_DATA",
                f"no exact universe snapshot {universe_dataset_id}/{universe_version}",
            )
        if str(universe_catalog.get("dataset_id", "")).strip() != universe_dataset_id:
            raise AutonomousResearchError("CRYPTO_PROVENANCE_MISMATCH", "universe catalog identity does not match the plan")
        if str(universe_catalog.get("dataset_version", universe_catalog.get("version", ""))).strip() != universe_version:
            raise AutonomousResearchError("CRYPTO_PROVENANCE_MISMATCH", "universe version does not match the plan")
        universe_metadata = universe_catalog.get("metadata")
        universe_metadata = dict(universe_metadata) if isinstance(universe_metadata, Mapping) else {}
        persisted_universe_id = str(universe_metadata.get("universe_id", "")).strip()
        if persisted_universe_id and persisted_universe_id != universe_id:
            raise AutonomousResearchError("CRYPTO_PROVENANCE_MISMATCH", "universe id does not match the persisted snapshot")
        persisted_hash = str(
            universe_metadata.get("snapshot_hash")
            or universe_metadata.get("content_hash")
            or universe_catalog.get("snapshot_hash")
            or ""
        ).strip()
        if persisted_hash != universe_hash:
            raise AutonomousResearchError("CRYPTO_PROVENANCE_MISMATCH", "universe snapshot_hash does not match the persisted snapshot")
        if universe_metadata.get("point_in_time") is not True:
            raise AutonomousResearchError("CRYPTO_PROVENANCE_MISMATCH", "universe snapshot is not point-in-time")

        universe_record_loader = getattr(self.store, "load_dataset_record", None)
        universe_record = (
            universe_record_loader(universe_dataset_id, universe_version)
            if callable(universe_record_loader)
            else None
        )
        universe_rows = universe_record.get("records") if isinstance(universe_record, Mapping) else None
        if isinstance(universe_rows, Mapping):
            universe_rows = universe_rows.get("records", universe_rows.get("rows", ()))
        if not isinstance(universe_rows, (list, tuple)):
            universe_rows = self.store.load_dataset(universe_dataset_id, universe_version)
        if not isinstance(universe_rows, (list, tuple)):
            raise AutonomousResearchError("INSUFFICIENT_DATA", "persisted universe snapshot has no bounded records")
        selected: dict[str, str] = {}
        for row in universe_rows:
            if not isinstance(row, Mapping) or not bool(row.get("selected")):
                continue
            for key in ("binance_symbol", "symbol", "instrument"):
                value = row.get(key)
                if value is not None and _normal_symbol(value):
                    selected.setdefault(_normal_symbol(value), str(value).strip())
        declared_values = universe_document.get("instruments")
        if declared_values is None:
            declared_values = universe_document.get("symbols")
        if declared_values is None:
            declared_values = ()
        if isinstance(declared_values, str) or not isinstance(declared_values, (list, tuple)):
            raise AutonomousResearchError(
                "CRYPTO_PROVENANCE_MISMATCH",
                "plan universe instruments/symbols must be a bounded list",
            )
        if not declared_values:
            raise AutonomousResearchError(
                "CRYPTO_PROVENANCE_MISMATCH",
                "plan universe instruments/symbols must contain at least one symbol",
            )
        declared: set[str] = set()
        for index, value in enumerate(declared_values):
            normalized = _normal_symbol(value)
            if value is None or not str(value).strip() or not normalized:
                raise AutonomousResearchError(
                    "CRYPTO_PROVENANCE_MISMATCH",
                    f"plan universe instruments/symbols contains an empty symbol at index {index}",
                )
            if normalized in declared:
                raise AutonomousResearchError(
                    "CRYPTO_PROVENANCE_MISMATCH",
                    f"plan universe instruments/symbols contains duplicate normalized symbol {normalized!r}",
                )
            declared.add(normalized)
        persisted = set(selected)
        if declared != persisted:
            missing = sorted(persisted - declared)
            unexpected = sorted(declared - persisted)
            raise AutonomousResearchError(
                "CRYPTO_PROVENANCE_MISMATCH",
                "plan universe instruments do not exactly match persisted selected rows: "
                f"declared={sorted(declared)!r}; persisted={sorted(persisted)!r}; "
                f"missing={missing!r}; unexpected={unexpected!r}",
            )

        target = plan.target_instrument
        target_key = _normal_symbol(target)
        if not target_key or target_key not in selected:
            raise AutonomousResearchError(
                "CRYPTO_UNIVERSE_MEMBERSHIP_MISMATCH",
                f"selected instrument {target or '<missing>'} is not a member of the persisted universe",
            )
        return {
            "dataset": {
                "dataset_id": dataset_id,
                "dataset_version": dataset_version,
                "timeframe": catalog_timeframe,
                "source": catalog_source,
                "source_type": catalog_source_type,
                "survivorship_bias": survivorship,
                "snapshot_id": str(catalog.get("snapshot_id", "")).strip(),
            },
            "universe": {
                "universe_id": universe_id,
                "universe_version": universe_version,
                "snapshot_hash": universe_hash,
                "dataset_id": universe_dataset_id,
                "snapshot_id": str(universe_catalog.get("snapshot_id", "")).strip(),
                "methodology": str(universe_document.get("methodology", "")).strip(),
                "point_in_time": True,
            },
            "selected_symbol": selected[target_key],
        }

    def _load_prediction_dataset_by_version(self, version: str) -> Any | None:
        """Load a datasetless prediction plan only when its version is unambiguous."""
        connection = getattr(self.store, "connection", None)
        execute = getattr(connection, "execute", None)
        if callable(execute):
            try:
                matches = execute(
                    "SELECT dataset_id, 'datasets' AS source FROM datasets WHERE version=? "
                    "UNION ALL "
                    "SELECT dataset_id, 'dataset_catalog' AS source "
                    "FROM dataset_catalog WHERE dataset_version=? "
                    "ORDER BY dataset_id, source",
                    (str(version), str(version)),
                ).fetchall()
            except Exception as exc:
                raise AutonomousResearchError(
                    "INSUFFICIENT_DATA",
                    f"immutable dataset lookup failed for version {version}",
                ) from exc
            if len(matches) > 1:
                identities = ", ".join(f"{row[1]}:{row[0]}" for row in matches)
                raise AutonomousResearchError(
                    "INSUFFICIENT_DATA",
                    f"ambiguous immutable datasets at version {version}: {identities}",
                )
            if not matches:
                return None
            dataset_id = str(matches[0][0]).strip()
            loader = getattr(self.store, "load_dataset", None)
            if callable(loader):
                return loader(dataset_id, version)
            return None

        finder = getattr(self.store, "load_dataset_by_version", None)
        if callable(finder):
            found = finder(version)
            if isinstance(found, Mapping):
                found_version = str(found.get("version", found.get("dataset_version", ""))).strip()
                if found_version == str(version):
                    return found.get("records")
        return None

    def _load_split(self, plan: ExperimentPlan) -> tuple[list[Mapping[str, Any]], Any]:
        records: Any = None
        if plan.dataset_version.lower() in {"latest", "current", "default", "unversioned"}:
            if plan.market_type is MarketType.CRYPTO_SPOT:
                raise AutonomousResearchError(
                    "INSUFFICIENT_DATA",
                    "crypto dataset_version must be immutable and versioned",
                )
            raise AutonomousResearchError(
                "INSUFFICIENT_DATA",
                "dataset_version must identify an immutable version",
            )
        if plan.market_type is MarketType.CRYPTO_SPOT:
            self._crypto_binding(plan)
            if not plan.dataset_id:
                raise AutonomousResearchError(
                    "INSUFFICIENT_DATA",
                    "crypto_spot research requires a versioned dataset_id",
                )
            loader = getattr(self.store, "load_dataset_record", None)
            if callable(loader):
                record = loader(plan.dataset_id, plan.dataset_version)
                if isinstance(record, Mapping):
                    record_version = str(record.get("version", record.get("dataset_version", ""))).strip()
                    if record_version and record_version != plan.dataset_version:
                        raise AutonomousResearchError(
                            "CRYPTO_PROVENANCE_MISMATCH",
                            "dataset record version does not match the plan",
                        )
                    records = record.get("records")
            if records is None:
                records = self.store.load_dataset(plan.dataset_id, plan.dataset_version)
        elif plan.dataset_id:
            records = self.store.load_dataset(plan.dataset_id, plan.dataset_version)
        else:
            records = self._load_prediction_dataset_by_version(plan.dataset_version)
        if records is None:
            raise AutonomousResearchError(
                "INSUFFICIENT_DATA",
                f"no immutable dataset {plan.dataset_id or '*'} at version {plan.dataset_version}",
            )
        if isinstance(records, Mapping):
            records = records.get("records", records.get("rows", records.get("observations", ())))
        if not isinstance(records, (list, tuple)):
            raise AutonomousResearchError("INSUFFICIENT_DATA", "dataset records are not a bounded sequence")
        rows = [_normalize_row(item) for item in list(records)[:_MAX_DATASET_ROWS]]
        rows = [row for row in rows if row is not None]
        rows = self._apply_plan_filters(plan, rows)
        rows = self._apply_model_document(plan, rows)
        if not rows:
            raise AutonomousResearchError("INSUFFICIENT_DATA", "dataset contains no rows after plan filters")
        for feature in plan.allowed_features:
            if feature in {"timestamp", "market_id", "symbol", "expiry", "settlement", "question", "resolution_criteria"}:
                continue
            if not any(_value(row, feature) is not None for row in rows):
                raise AutonomousResearchError("INSUFFICIENT_DATA", f"dataset has no values for feature {feature}")
        stamps = [parse_timestamp(_value(row, "timestamp")) for row in rows]
        if any(stamp is None for stamp in stamps):
            raise AutonomousResearchError("INSUFFICIENT_DATA", "dataset contains rows without timestamps")
        ordered = [row for _, row in sorted(zip(stamps, rows), key=lambda pair: (pair[0], _value(pair[1], "market_id", "")))]
        if len(ordered) < 3:
            raise AutonomousResearchError("INSUFFICIENT_DATA", "at least three chronological observations are required")
        train_count = max(1, int(len(ordered) * 0.60))
        validation_count = max(1, int(len(ordered) * 0.20))
        if train_count + validation_count >= len(ordered):
            train_count = max(1, len(ordered) - 2)
            validation_count = 1
        train_end = parse_timestamp(_value(ordered[train_count], "timestamp"))
        validation_end = parse_timestamp(_value(ordered[train_count + validation_count], "timestamp"))
        holdout_end = parse_timestamp(_value(ordered[-1], "timestamp"))
        if train_end is None or validation_end is None or holdout_end is None:
            raise AutonomousResearchError("INSUFFICIENT_DATA", "dataset timestamps are invalid")
        from datetime import timedelta

        split = split_dataset(
            ordered,
            train_end,
            validation_end,
            holdout_end + timedelta(microseconds=1),
            dataset_version=plan.dataset_version,
            require_nonempty=True,
        )
        # Keep the locked holdout in the split object for provenance only.
        # Autonomous evaluation, mutation, Hermes summaries, and promotion use
        # train/validation evidence and never consume split.holdout.
        return ordered, split

    def _apply_plan_filters(self, plan: ExperimentPlan, rows: Sequence[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
        filters = dict(plan.filters)
        restrictions = dict(plan.regime_restrictions)
        supported = {
            "entry_price",
            "minimum_hours_to_resolution",
            "maximum_hours_to_resolution",
            "min_liquidity",
            "max_spread",
            "regime",
            "regimes",
        }
        unknown = sorted(set(filters) - supported)
        if unknown:
            raise AutonomousResearchError("UNSUPPORTED_FEATURE", f"unsupported plan filters: {unknown}")
        supported_restrictions = {"regime", "regimes", "allowed_regimes", "allowed_states"}
        unknown_restrictions = sorted(set(restrictions) - supported_restrictions)
        if unknown_restrictions:
            raise AutonomousResearchError(
                "UNSUPPORTED_FEATURE",
                f"unsupported regime restrictions: {unknown_restrictions}",
            )
        allowed_regimes = filters.get("regime", filters.get("regimes"))
        restricted_regimes = restrictions.get(
            "allowed_regimes",
            restrictions.get("allowed_states", restrictions.get("regime", restrictions.get("regimes"))),
        )
        if allowed_regimes is not None and restricted_regimes is not None:
            allowed_regimes = _as_regime_set(allowed_regimes, "regime filter") & _as_regime_set(
                restricted_regimes,
                "regime restrictions",
            )
        elif allowed_regimes is None:
            allowed_regimes = restricted_regimes
        elif allowed_regimes is not None:
            allowed_regimes = _as_regime_set(allowed_regimes, "regime filter")
        target_markets = set(plan.target_markets)
        target_instrument = plan.target_instrument
        result: list[Mapping[str, Any]] = []
        for row in rows:
            market_id = str(_value(row, "market_id", "")).strip()
            symbol = str(_value(row, "symbol", _value(row, "instrument", ""))).strip()
            if target_markets and market_id not in target_markets:
                continue
            if target_instrument and target_instrument not in {market_id, symbol}:
                continue
            price = _finite(_value(row, "yes_mid", _value(row, "yes_ask")), math.nan)
            if "entry_price" in filters and not _in_bound(price, filters["entry_price"]):
                continue
            seconds = _time_to_expiry(row)
            if "minimum_hours_to_resolution" in filters:
                minimum = _finite(filters["minimum_hours_to_resolution"], math.nan) * 3600.0
                if not math.isfinite(seconds) or seconds < minimum:
                    continue
            if "maximum_hours_to_resolution" in filters:
                maximum = _finite(filters["maximum_hours_to_resolution"], math.nan) * 3600.0
                if not math.isfinite(seconds) or seconds > maximum:
                    continue
            liquidity = _finite(_value(row, "liquidity"), math.nan)
            if "min_liquidity" in filters and (not math.isfinite(liquidity) or liquidity < _finite(filters["min_liquidity"], math.inf)):
                continue
            spread = _finite(_value(row, "spread"), math.nan)
            if "max_spread" in filters and (not math.isfinite(spread) or spread > _finite(filters["max_spread"], -math.inf)):
                continue
            if allowed_regimes is not None:
                regime = _value(row, "regime", _value(row, "regime_state"))
                if str(regime) not in allowed_regimes:
                    continue
            result.append(row)
        return result

    @staticmethod
    def _apply_model_document(plan: ExperimentPlan, rows: Sequence[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
        model = plan.model_for()
        if model is None:
            return list(rows)
        result: list[Mapping[str, Any]] = []
        for row in rows:
            clean = dict(row)
            if "probability" in model:
                clean.setdefault("model_probability", model["probability"])
            elif "yes_probability" in model:
                clean.setdefault("model_probability", model["yes_probability"])
            elif isinstance(model.get("field"), str):
                value = clean.get(model["field"])
                try:
                    probability = float(value)
                except (TypeError, ValueError):
                    probability = math.nan
                if math.isfinite(probability) and 0.0 <= probability <= 1.0:
                    clean.setdefault("model_probability", probability)
            result.append(clean)
        return result

    def _evaluate_datasets(
        self,
        plan: ExperimentPlan,
        strategy: StrategyDefinition,
        train: Sequence[Mapping[str, Any]],
        validation: Sequence[Mapping[str, Any]],
    ) -> dict[str, Mapping[str, Any]]:
        return {
            "train": self._run_backtest(plan, strategy, train),
            "validation": self._run_backtest(plan, strategy, validation),
        }

    @staticmethod
    def _run_backtest(plan: ExperimentPlan, strategy: StrategyDefinition, rows: Sequence[Mapping[str, Any]]) -> Mapping[str, Any]:
        methodology = plan.methodology
        initial_cash = _finite(methodology.get("initial_cash"), 10_000.0)
        fee_bps = _finite(methodology.get("fee_bps"), 0.0)
        slippage_bps = _finite(methodology.get("slippage_bps"), 0.0)
        allocation = _finite(methodology.get("allocation"), 0.25)
        if not math.isfinite(initial_cash) or initial_cash <= 0 or not math.isfinite(fee_bps) or fee_bps < 0 or not math.isfinite(slippage_bps) or slippage_bps < 0:
            raise AutonomousResearchError("INVALID_PLAN", "cost assumptions must be finite and non-negative")
        if plan.market_type is MarketType.PREDICTION:
            result = PredictionMarketBacktester(
                initial_cash=initial_cash,
                fee_bps=fee_bps,
                slippage_bps=slippage_bps,
                allocation=max(0.0, min(1.0, allocation)),
            ).run(rows, strategy, research_quality=ResearchQuality.ORDER_BOOK_SIMULATED if _has_book(rows) else ResearchQuality.PRICE_PROXY)
        else:
            result = CryptoBacktester(
                initial_cash=initial_cash,
                fee_bps=fee_bps,
                slippage_bps=slippage_bps,
                allocation=max(0.0, min(1.0, allocation)),
                symbol=plan.target_instrument or "ASSET",
            ).run(rows, strategy, symbol=plan.target_instrument or "ASSET")
        metrics = {str(key): _finite(value, 0.0) for key, value in result.metrics.items() if _is_finite_number(value)}
        settled = len(result.outcomes)
        if plan.market_type is MarketType.PREDICTION:
            expectancy = metrics.get("roi", 0.0) if settled else metrics.get("expected_value", metrics.get("roi", 0.0))
        else:
            expectancy = metrics.get("expectancy", metrics.get("total_return", 0.0))
        curve_values = [_finite(item.get("equity"), 0.0) for item in result.equity_curve if isinstance(item, Mapping)]
        returns = [current / previous - 1.0 for previous, current in zip(curve_values, curve_values[1:]) if previous > 0]
        confidence = bootstrap_confidence_interval(
            returns or [expectancy],
            resamples=min(256, max(32, len(returns or [expectancy]) * 8)),
            seed=0,
        )
        sample_count = len(rows)
        independent = len({str(_value(row, "market_id", _value(row, "symbol", index))) for index, row in enumerate(rows)})
        liquidity_values = [_finite(_value(row, "liquidity"), math.nan) for row in rows]
        liquidity_values = [value for value in liquidity_values if math.isfinite(value)]
        regimes = {
            str(_value(row, "regime", _value(row, "regime_state", "unknown")))
            for row in rows
            if _value(row, "regime", _value(row, "regime_state")) is not None
        }
        result_quality = result.quality.value if hasattr(result.quality, "value") else str(result.quality)
        summary = {
            "sample_count": sample_count,
            "independent_samples": independent,
            "filled_trades": len(result.fills),
            "settled_markets": settled,
            "expectancy": expectancy,
            "roi": metrics.get("roi", metrics.get("total_return", 0.0)),
            "max_drawdown": metrics.get("max_drawdown", 0.0),
            "calibration": max(0.0, min(1.0, 1.0 - metrics.get("ece", 0.0))) if settled else 0.0,
            "liquidity": mean(liquidity_values) if liquidity_values else 0.0,
            "regime_count": len(regimes) if regimes else (1 if rows else 0),
            "quality": result_quality,
            "costs": metrics.get("fees", 0.0) + metrics.get("slippage", 0.0),
            "confidence_lower_bound": _finite(confidence.get("lower"), 0.0),
            "confidence_interval": {
                "lower": _finite(confidence.get("lower"), 0.0),
                "upper": _finite(confidence.get("upper"), 0.0),
                "count": int(confidence.get("count", 0)),
            },
            "metrics": metrics,
            "outcomes": settled,
        }
        return summary

    def _advance_candidate(
        self,
        plan: ExperimentPlan,
        candidate_id: str,
        strategy: StrategyDefinition,
        evaluation: Mapping[str, Mapping[str, Any]],
        *,
        variant_count: int,
        validation_scores: Mapping[str, float],
        now: datetime,
        generation: int,
        lineage: Sequence[str],
        crypto_binding: Mapping[str, Any] | None = None,
    ) -> Mapping[str, Any]:
        if plan.market_type is MarketType.CRYPTO_SPOT and crypto_binding is None:
            crypto_binding = self._crypto_binding(plan)
        candidate = self.lifecycle.get(candidate_id)
        if candidate is None:
            raise AutonomousResearchError("INVALID_CANDIDATE", f"candidate {candidate_id} is not registered")
        if candidate.stage is CandidateStage.REJECTED:
            return {"candidate_id": candidate_id, "stage": candidate.stage.value, "reason": candidate.rejection_reason}
        if candidate.stage in {CandidateStage.PAPER_FORWARD, CandidateStage.PAPER_PROMOTABLE}:
            return {"candidate_id": candidate_id, "stage": candidate.stage.value}
        base = {
            "candidate_id": candidate_id,
            "hypothesis_id": plan.hypothesis_id,
            "plan_id": plan.plan_id,
            "plan_hash": plan.plan_hash,
            "dataset_id": plan.dataset_id,
            "dataset_version": plan.dataset_version,
            "experiment_family": plan.experiment_family,
            "generation": generation,
            "lineage": list(dict.fromkeys([*(str(value) for value in lineage), *([candidate_id] if generation else [])])),
            "paper_only": True,
            "holdout_used": False,
        }
        if crypto_binding is not None:
            base["crypto_provenance"] = dict(crypto_binding)
        if candidate.stage is CandidateStage.IDEA:
            candidate = self.lifecycle.advance(
                candidate_id,
                CandidateStage.SCHEMA_VALIDATED,
                {
                    **base,
                    "schema_valid": True,
                    "referenced_features": list(plan.allowed_features),
                    "parameter_ranges_bounded": True,
                },
                reason="declarative experiment schema validated",
            )
        train = dict(evaluation["train"])
        validation = dict(evaluation["validation"])
        if candidate.stage is CandidateStage.SCHEMA_VALIDATED:
            candidate = self.lifecycle.advance(
                candidate_id,
                CandidateStage.BACKTESTED,
                {
                    **base,
                    "backtest_complete": True,
                    "train": _compact_evidence(train),
                    "costs_included": True,
                    "raw_observations": train.get("sample_count", 0),
                    "data_quality": train.get("quality"),
                },
                reason="bounded historical simulation completed with costs",
            )
        validation_expectancy = _finite(validation.get("expectancy"), 0.0)
        if validation_expectancy < 0.0:
            self.lifecycle.reject(
                candidate_id,
                "negative_validation_expectancy",
                evidence={
                    **base,
                    "validation_complete": True,
                    "validation": _compact_evidence(validation),
                    "benchmark_comparison": {"baseline_expectancy": 0.0, "delta": validation_expectancy},
                },
                expected_stage=candidate.stage,
                expected_payload=candidate.payload,
            )
            return {"candidate_id": candidate_id, "stage": CandidateStage.REJECTED.value, "reason": "negative_validation_expectancy"}
        if candidate.stage is CandidateStage.BACKTESTED:
            candidate = self.lifecycle.advance(
                candidate_id,
                CandidateStage.VALIDATED,
                {
                    **base,
                    "validation_complete": True,
                    "validation": _compact_evidence(validation),
                    "validation_expectancy": validation_expectancy,
                    "benchmark_comparison": {"baseline_expectancy": 0.0, "delta": validation_expectancy},
                },
                reason="chronological validation completed without locked feedback",
            )
        stability = neighboring_parameter_stability(validation_scores)
        stability_value = _finite(stability.get("stable_fraction"), 0.0)
        sample_check = minimum_sample_check(
            int(validation.get("sample_count", 0)),
            trades=int(validation.get("filled_trades", 0)),
            min_observations=plan.min_samples,
            min_trades=plan.min_trades,
        )
        robust_evidence = {
            **base,
            "robustness_passed": bool(sample_check["passed"] and stability_value >= 0.60 and validation_expectancy >= 0.0),
            "minimum_sample_check": sample_check,
            "validation_stability": stability_value,
            "validation_stability_summary": stability,
            "multiple_testing": {
                "variants_tested": variant_count,
                "selected_from_variants": variant_count,
                "selection_metric": "validation_expectancy",
                "locked_partition_used_for_selection": False,
            },
            "validation_regime_behavior": {"regimes": validation.get("regime_count", 0)},
            "validation_execution_quality": validation.get("quality"),
            "validation_confidence_interval": validation.get("confidence_interval"),
            "validation_expectancy": validation_expectancy,
            "validation_confidence_lower_bound": _finite(validation.get("confidence_lower_bound"), 0.0),
            "validation_calibration": _finite(validation.get("calibration"), 0.0),
        }
        if candidate.stage is CandidateStage.VALIDATED:
            if not sample_check["passed"]:
                self.lifecycle.reject(
                    candidate_id,
                    "INSUFFICIENT_DATA",
                    evidence=robust_evidence,
                    expected_stage=candidate.stage,
                    expected_payload=candidate.payload,
                )
                return {"candidate_id": candidate_id, "stage": CandidateStage.REJECTED.value, "reason": "INSUFFICIENT_DATA"}
            if stability_value < 0.60:
                self.lifecycle.reject(
                    candidate_id,
                    "unstable_neighbor_parameters",
                    evidence=robust_evidence,
                    expected_stage=candidate.stage,
                    expected_payload=candidate.payload,
                )
                return {"candidate_id": candidate_id, "stage": CandidateStage.REJECTED.value, "reason": "unstable_neighbor_parameters"}
            candidate = self.lifecycle.advance(
                candidate_id,
                CandidateStage.ROBUSTNESS_CHECKED,
                robust_evidence,
                reason="sample, neighboring-parameter, regime, and cost checks passed",
            )
        if candidate.stage is CandidateStage.ROBUSTNESS_CHECKED:
            model_document = plan.model_for() or {"type": "deterministic"}
            if plan.market_type is MarketType.CRYPTO_SPOT:
                # Crypto autonomous candidates are historical research only.
                # They are deliberately never registered with ForwardTestRegistry.
                risk_snapshot = {"execution_capability": "none", "max_position_fraction": 0.0}
                forward_config = {
                    "paper_only": True,
                    "execution_capability": "none",
                    "plan_id": plan.plan_id,
                    "dataset_id": plan.dataset_id,
                    "dataset_version": plan.dataset_version,
                    "strategy_document": strategy.to_dict(),
                    "model_document": dict(model_document),
                }
            else:
                risk_snapshot = {"max_position_fraction": 0.05}
                forward_config = {
                    "execution": "paper_only",
                    "live_execution": False,
                    "plan_id": plan.plan_id,
                    "dataset_id": plan.dataset_id,
                    "dataset_version": plan.dataset_version,
                    "strategy_document": strategy.to_dict(),
                    "model_document": dict(model_document),
                }
            config_hash = _hash_document({"config": forward_config, "risk_limits": risk_snapshot})
            strategy_hash = _content_hash(strategy.to_dict())
            model_hash = _content_hash(model_document)
            frozen_hash = hashlib.sha256("|".join((strategy_hash, model_hash, config_hash)).encode("utf-8")).hexdigest()
            candidate = self.lifecycle.advance(
                candidate_id,
                CandidateStage.FROZEN,
                {
                    **robust_evidence,
                    **base,
                    "frozen": True,
                    "strategy_hash": strategy_hash,
                    "model_hash": model_hash,
                    "config_hash": config_hash,
                    "frozen_hash": frozen_hash,
                    "risk_snapshot": risk_snapshot,
                    "dataset_provenance": {
                        "dataset_id": plan.dataset_id,
                        "dataset_version": plan.dataset_version,
                        "time_split": plan.methodology.get("time_split"),
                        "universe": dict(plan.universe or {}),
                    },
                    "experiment_budget_lineage": {
                        "budget_id": AUTONOMOUS_BUDGET_ID,
                        "family": plan.experiment_family,
                        "variants_tested": variant_count,
                    },
                },
                reason="exact strategy, model, configuration, risk, and provenance frozen",
            )
            if plan.market_type is MarketType.CRYPTO_SPOT:
                return {
                    "candidate_id": candidate_id,
                    "stage": candidate.stage.value,
                    "validation_expectancy": validation_expectancy,
                    "variant_count": variant_count,
                    "forward_test_id": None,
                    "research_only": True,
                    "execution_capability": "none",
                    "paper_only": True,
                    "crypto_provenance": dict(crypto_binding or {}),
                }
            registry = ForwardTestRegistry(self.store)
            spec = registry.register_forward_test(
                strategy=strategy.to_dict(),
                model=dict(model_document),
                registration_timestamp=now,
                now=now,
                config=forward_config,
                bankroll=10_000.0,
                allowed_markets=plan.target_markets,
                risk_limits=risk_snapshot,
                experiment_id="forward-" + candidate_id,
            )
            candidate = self.lifecycle.advance(
                candidate_id,
                CandidateStage.PAPER_FORWARD,
                {
                    **base,
                    "paper_forward_started": True,
                    "forward_test_id": spec.experiment_id,
                    "registration_timestamp": spec.registration_timestamp.isoformat(),
                    "forward_duration_seconds": 0.0,
                    "holdout_used": False,
                    "forward_evidence": {
                        "evidence_scope": "forward_only",
                        "forward_order_attempts": 0,
                        "forward_successful_order_attempts": 0,
                        "forward_failed_order_attempts": 0,
                        "fills": 0,
                        "partial_fills": 0,
                        "no_fill_orders": 0,
                        "forward_independent_resolved_bets": 0,
                        "forward_expectancy": None,
                        "forward_confidence_lower_bound": None,
                        "forward_stability": None,
                        "forward_calibration": None,
                        "forward_max_drawdown": 0.0,
                        "paper_only": True,
                    },
                },
                reason="genuine forward registration created at current time",
            )
        return {
            "candidate_id": candidate_id,
            "stage": candidate.stage.value,
            "validation_expectancy": validation_expectancy,
            "variant_count": variant_count,
            "forward_test_id": candidate.payload.get("forward_test_id"),
        }

    def _generate_mutations(
        self,
        plan: ExperimentPlan,
        prepared: Sequence[Mapping[str, Any]],
        validation_scores: Mapping[str, float],
        now: datetime,
        *,
        lineage: Sequence[str] = (),
    ) -> tuple[str, ...]:
        if not self.config.mutation_enabled or self.config.max_children_per_parent <= 0:
            return ()
        if not prepared:
            return ()
        current = ensure_utc(now)
        daily_limit = self.config.max_experiments_per_day
        daily_since: datetime | None = None
        daily_until: datetime | None = None
        if daily_limit is not None:
            daily_since = datetime(current.year, current.month, current.day, tzinfo=timezone.utc)
            daily_until = daily_since + timedelta(days=1)
        parent = max(prepared, key=lambda item: (validation_scores.get(str(item["candidate_id"]), float("-inf")), str(item["candidate_id"])))
        parent_id = str(parent["candidate_id"])
        lifecycle = self.lifecycle.get(parent_id)
        if lifecycle is None or lifecycle.stage not in {CandidateStage.ROBUSTNESS_CHECKED, CandidateStage.FROZEN, CandidateStage.PAPER_FORWARD, CandidateStage.PAPER_PROMOTABLE}:
            return ()
        generation = int(lifecycle.payload.get("generation", 0))
        if generation >= self.config.max_generation_depth:
            return ()
        existing = self.store.load_candidate_lifecycle(limit=10_000)
        children = 0
        if isinstance(existing, list):
            children = sum(
                1
                for record in existing
                if isinstance(record, Mapping)
                and isinstance(record.get("payload"), Mapping)
                and str(record["payload"].get("parent_id", "")) == parent_id
            )
        remaining = max(0, self.config.max_children_per_parent - children)
        if daily_limit is not None:
            assert daily_since is not None and daily_until is not None
            daily_count = self.store.count_experiment_budget_reservations(
                AUTONOMOUS_BUDGET_ID,
                since=daily_since,
                until=daily_until,
            )
            remaining = min(remaining, max(0, daily_limit - daily_count))
        if remaining <= 0:
            return ()
        strategy = parent["strategy"]
        # Experiment plans are untrusted input.  The node owns the shared
        # autonomous budget namespace; never let plan-declared limits poison
        # its persisted immutable limits.
        budget = ExperimentBudget(
            budget_id=AUTONOMOUS_BUDGET_ID,
            total_limit=self.config.total_limit,
            per_family_limit=self.config.family_limit,
        )
        engine = DeterministicMutationEngine(store=self.store, lifecycle=self.lifecycle, budget=budget, seed=0)
        try:
            generated = engine.mutate(
                strategy,
                parent_id=parent_id,
                generation=generation + 1,
                max_variants=remaining,
                provenance={
                    "plan_id": plan.plan_id,
                    "hypothesis_id": plan.hypothesis_id,
                    "validation_only": True,
                    "locked_partition_used": False,
                    "crypto_provenance": dict(self._crypto_binding(plan) or {}),
                },
                lineage=lineage,
                timestamp=now,
                daily_limit=daily_limit,
                daily_since=daily_since,
                daily_until=daily_until,
            )
        except (RuntimeError, ValueError):
            return ()
        child_ids: list[str] = []
        for child in generated:
            child_document = child.strategy.to_dict()
            child_document["strategy_id"] = child.candidate_id
            child_strategy = load_strategy(child_document)
            child_payload = {
                "candidate_id": child.candidate_id,
                "hypothesis_id": plan.hypothesis_id,
                "plan_id": plan.plan_id,
                "plan_hash": plan.plan_hash,
                "experiment_plan": plan.as_dict(),
                "strategy": child_strategy.to_dict(),
                "parameters": dict(child_strategy.parameters),
                "variant_id": "mutation-" + child.candidate_id,
                "generation": child.generation,
                "parent_id": child.parent_id,
                "lineage": list(child.lineage),
                "dataset_version": plan.dataset_version,
                "dataset_id": plan.dataset_id,
                "paper_only": True,
                "holdout_used": False,
                "crypto_provenance": dict(self._crypto_binding(plan) or {}),
            }
            self.store.save_strategy_if_absent(child_strategy.id, child_strategy.to_dict())
            self.store.save_experiment_if_absent(
                child.candidate_id,
                {
                    "run_id": child.candidate_id,
                    "status": "IDEA",
                    "strategy_id": child_strategy.id,
                    "strategy": child_strategy.to_dict(),
                    "variant": dict(child_strategy.parameters),
                    "candidate_id": child.candidate_id,
                    "hypothesis_id": plan.hypothesis_id,
                    "plan_id": plan.plan_id,
                    "plan_hash": plan.plan_hash,
                    "dataset_id": plan.dataset_id,
                    "dataset_version": plan.dataset_version,
                    "generation": child.generation,
                    "parent_id": child.parent_id,
                    "lineage": list(child.lineage),
                    "paper_only": True,
                    "holdout_used": False,
                },
                strategy_id=child_strategy.id,
            )
            self.bus.submit_candidate(
                child_payload,
                dedupe_key="candidate:" + child.candidate_id,
                lineage=child.lineage,
                available_at=now,
            )
            child_ids.append(child.candidate_id)
        return tuple(child_ids)

    def _reserve_candidate(self, plan: ExperimentPlan, candidate_id: str, now: datetime) -> None:
        current = ensure_utc(now)
        daily_since: datetime | None = None
        daily_until: datetime | None = None
        if self.config.max_experiments_per_day is not None:
            daily_since = datetime(current.year, current.month, current.day, tzinfo=timezone.utc)
            daily_until = daily_since + timedelta(days=1)
        try:
            self.store.reserve_experiment_budget(
                AUTONOMOUS_BUDGET_ID,
                total_limit=self.config.total_limit,
                per_family_limit=self.config.family_limit,
                family=plan.experiment_family,
                reservation_key=candidate_id,
                timestamp=current,
                daily_limit=self.config.max_experiments_per_day,
                daily_since=daily_since,
                daily_until=daily_until,
            )
        except RuntimeError as exc:
            reason = (
                "EXPERIMENT_DAILY_LIMIT_EXCEEDED"
                if str(exc) == "experiment daily budget exhausted"
                else "EXPERIMENT_BUDGET_EXCEEDED"
            )
            raise AutonomousResearchError(reason, str(exc)) from exc

    def _candidate_payload(
        self,
        plan: ExperimentPlan,
        candidate_id: str,
        strategy: StrategyDefinition,
        parameters: Mapping[str, Any],
        *,
        variant_id: str,
        generation: int,
        lineage: Sequence[str],
    ) -> dict[str, Any]:
        return {
            "candidate_id": candidate_id,
            "hypothesis_id": plan.hypothesis_id,
            "plan_id": plan.plan_id,
            "experiment_plan": plan.as_dict(),
            "strategy": strategy.to_dict(),
            "parameters": dict(parameters),
            "variant_id": variant_id,
            "generation": generation,
            "lineage": list(lineage),
            "dataset_id": plan.dataset_id,
            "dataset_version": plan.dataset_version,
            "experiment_family": plan.experiment_family,
            "paper_only": True,
            "holdout_used": False,
        }

    def _hypothesis_result(
        self,
        plan: ExperimentPlan,
        results: Sequence[Mapping[str, Any]],
        mutations: Sequence[str],
    ) -> dict[str, Any]:
        rejected = [item for item in results if item.get("stage") == CandidateStage.REJECTED.value]
        selected_stages = (
            {CandidateStage.FROZEN.value}
            if plan.market_type is MarketType.CRYPTO_SPOT
            else {CandidateStage.PAPER_FORWARD.value, CandidateStage.PAPER_PROMOTABLE.value}
        )
        selected = [item for item in results if item.get("stage") in selected_stages]
        reasons: dict[str, int] = {}
        for item in rejected:
            reason = str(item.get("reason", "rejected"))
            reasons[reason] = reasons.get(reason, 0) + 1
        return {
            "accepted": bool(selected),
            "kind": "hypothesis",
            "hypothesis_id": plan.hypothesis_id,
            "plan_id": plan.plan_id,
            "plan_hash": plan.plan_hash,
            "status": (
                "accepted_research_only"
                if plan.market_type is MarketType.CRYPTO_SPOT and selected
                else "accepted" if selected else "unsupported_by_validation"
            ),
            "variants_tested": len(results),
            "selected_from_variants": len(selected),
            "selected_candidate_ids": [item.get("candidate_id") for item in selected[:_MAX_QUEUE_RESULT_ITEMS]],
            "candidate_results": [
                {
                    "candidate_id": item.get("candidate_id"),
                    "stage": item.get("stage"),
                    "reason": item.get("reason"),
                    "forward_test_id": item.get("forward_test_id"),
                }
                for item in results[:_MAX_QUEUE_RESULT_ITEMS]
            ],
            "rejected_reasons": dict(sorted(reasons.items())),
            "mutation_candidates": list(mutations[:_MAX_QUEUE_RESULT_ITEMS]),
            "experiment_family": plan.experiment_family,
            "dataset_version": plan.dataset_version,
            "data_quality": "ORDER_BOOK_SIMULATED" if plan.market_type is MarketType.PREDICTION else "OHLCV_SIMULATED",
            "paper_only": True,
            "research_only": plan.market_type is MarketType.CRYPTO_SPOT,
        }

    def _forward_evidence(self, record: Mapping[str, Any], now: datetime) -> dict[str, Any]:
        payload = record.get("payload", {})
        payload = dict(payload) if isinstance(payload, Mapping) else {}
        forward_id = str(payload.get("forward_test_id", "")).strip()
        if not forward_id:
            raise ValueError("candidate has no forward test")
        spec = ForwardTestRegistry(self.store).get(forward_id)
        if spec is None:
            raise ValueError(f"missing forward test {forward_id}")
        observations = self.store.list_paper_observations(forward_id, limit=_MAX_FORWARD_ROWS)
        fills = [
            fill
            for fill in self.store.load_fills(strategy_id=spec.strategy_hash)
            if str(fill.metadata.get("paper_experiment_id", "")) == forward_id
        ]
        event_records = self.store.list_paper_execution_events(forward_id, limit=_MAX_FORWARD_ROWS)
        ledger_records = self.store.list_paper_bet_ledger(forward_id, limit=_MAX_FORWARD_ROWS)
        terminal_by_market: dict[str, str] = {}
        observed_markets: set[str] = set()
        for row in observations:
            item = row.get("payload", {}) if isinstance(row, Mapping) else {}
            item = item if isinstance(item, Mapping) else {}
            market_id = str(row.get("market_id", item.get("market_id", ""))).strip()
            if market_id:
                observed_markets.add(market_id)
            settlement = _settlement_name(item.get("settlement"))
            if settlement in {"resolved_yes", "resolved_no", "void"} and market_id:
                terminal_by_market[market_id] = settlement

        fills_by_market: dict[str, list[Fill]] = {}
        for fill in fills:
            fills_by_market.setdefault(str(fill.market_id or fill.symbol), []).append(fill)
        ledger_by_market: dict[str, Mapping[str, Any]] = {}
        for row in ledger_records:
            market_id = str(row.get("market_id", "")).strip()
            ledger = row.get("payload")
            if market_id and isinstance(ledger, Mapping):
                ledger_by_market[market_id] = ledger
        for market_id, resolution in terminal_by_market.items():
            if market_id in ledger_by_market:
                continue
            ledger = build_resolved_bet(
                experiment_id=forward_id,
                market_id=market_id,
                strategy_id=spec.strategy_hash,
                settlement=resolution,
                resolved_at=next(
                    (
                        row["timestamp"]
                        for row in observations
                        if str(row.get("market_id", "")) == market_id
                        and isinstance(row.get("payload"), Mapping)
                        and _settlement_name(row["payload"].get("settlement")) == resolution
                    ),
                    now,
                ),
                fills=fills_by_market.get(market_id, ()),
            )
            if ledger is not None:
                self.store.save_paper_bet_ledger(
                    ledger["bet_id"],
                    forward_id,
                    market_id,
                    spec.strategy_hash,
                    ledger["outcome"],
                    ledger["resolution"],
                    parse_timestamp(ledger["resolved_at"]) or now,
                    ledger,
                )
                ledger_by_market[market_id] = ledger
        ledgers = list(ledger_by_market.values())

        if not event_records:
            event_records = [
                {
                    "market_id": str(fill.market_id or fill.symbol),
                    "status": str(fill.metadata.get("execution_status", "FULL_FILL")).upper(),
                    "payload": {
                        "market_id": str(fill.market_id or fill.symbol),
                        "status": str(fill.metadata.get("execution_status", "FULL_FILL")).upper(),
                        "outcomes": ["SIGNAL", "ORDER_ATTEMPT", str(fill.metadata.get("execution_status", "FULL_FILL")).upper()],
                        "order_attempted": True,
                        "requested_quantity": _finite(fill.metadata.get("requested_quantity"), fill.quantity),
                        "filled_quantity": fill.quantity,
                        "liquidity": fill.metadata.get("liquidity"),
                        "spread_paid": fill.metadata.get("spread_paid", 0.0),
                        "depth_consumed": fill.metadata.get("depth_consumed", 0.0),
                    },
                }
                for fill in fills
            ]
        event_payloads = [
            row.get("payload", {})
            for row in event_records
            if isinstance(row, Mapping) and isinstance(row.get("payload"), Mapping)
        ]
        attempt_events = [
            event
            for event in event_payloads
            if bool(event.get("order_attempted"))
            or "ORDER_ATTEMPT" in tuple(event.get("outcomes", ()))
        ]
        successful_events = [
            event
            for event in attempt_events
            if str(event.get("status", "")).upper() in {"FULL_FILL", "PARTIAL_FILL"}
            and _finite(event.get("filled_quantity"), 0.0) > 0
        ]
        no_fill_events = [event for event in attempt_events if str(event.get("status", "")).upper() == "NO_FILL"]
        risk_rejected_events = [
            event for event in attempt_events if str(event.get("status", "")).upper() == "RISK_REJECTED"
        ]
        requested_quantity = sum(max(0.0, _finite(event.get("requested_quantity"), 0.0)) for event in attempt_events)
        filled_quantity = sum(max(0.0, _finite(event.get("filled_quantity"), 0.0)) for event in attempt_events)
        partial_fills = sum(
            1
            for fill in fills
            if bool(fill.metadata.get("partial"))
            or str(fill.metadata.get("execution_status", "")).upper() == "PARTIAL_FILL"
        )
        liquidity_rejections = sum(
            1
            for event in no_fill_events
            if bool(event.get("liquidity_rejected"))
            or str(event.get("reason", "")).lower() in {"insufficient_liquidity", "invalid_order_book"}
        )
        order_attempts = len(attempt_events)
        successful_order_attempts = len(successful_events)
        failed_order_attempts = sum(
            1
            for event in attempt_events
            if str(event.get("status", "")).upper() in {"NO_FILL", "RISK_REJECTED"}
        )
        markets_signaled = {
            str(event.get("market_id", "")).strip()
            for event in event_payloads
            if str(event.get("market_id", "")).strip()
            and "SIGNAL" in tuple(event.get("outcomes", ()))
            and str(event.get("status", "")).upper() != "NO_SIGNAL"
        }
        markets_traded = {str(fill.market_id or fill.symbol).strip() for fill in fills}
        markets_resolved = set(terminal_by_market)
        resolved_positions = sum(int(_finite(ledger.get("positions"), 0.0)) for ledger in ledgers)
        resolved_pnls = [_finite(ledger.get("net_pnl"), math.nan) for ledger in ledgers]
        resolved_pnls = [value for value in resolved_pnls if math.isfinite(value)]
        resolved_rois = [_finite(ledger.get("roi"), math.nan) for ledger in ledgers]
        resolved_rois = [value for value in resolved_rois if math.isfinite(value)]
        forward_expectancy = mean(resolved_pnls) if resolved_pnls else None
        forward_roi = mean(resolved_rois) if resolved_rois else None
        confidence_interval = (
            bootstrap_confidence_interval(
                resolved_pnls,
                resamples=min(256, max(32, len(resolved_pnls) * 8)),
                seed=0,
            )
            if resolved_pnls
            else None
        )
        raw_confidence_lower_bound = (
            _finite(confidence_interval.get("lower"), math.nan)
            if isinstance(confidence_interval, Mapping)
            else math.nan
        )
        forward_confidence_lower_bound = (
            raw_confidence_lower_bound
            if math.isfinite(raw_confidence_lower_bound)
            else None
        )
        forward_stability = (
            sum(1 for value in resolved_rois if value >= 0.0) / len(resolved_rois)
            if len(resolved_rois) >= 2
            else None
        )
        calibration_rows: list[dict[str, float]] = []
        for ledger in ledgers:
            probability = _finite(ledger.get("expected_probability_at_entry"), math.nan)
            resolution = _settlement_name(ledger.get("resolution"))
            if math.isfinite(probability) and 0.0 <= probability <= 1.0 and resolution in {"resolved_yes", "resolved_no"}:
                calibration_rows.append(
                    {
                        "probability": probability,
                        "outcome": 1.0 if resolution == "resolved_yes" else 0.0,
                    }
                )
        forward_calibration = (
            max(0.0, min(1.0, 1.0 - expected_calibration_error(calibration_rows)))
            if calibration_rows
            else None
        )
        state_record = self.store.load_paper_state(forward_id) or {}
        state = state_record.get("state", {}) if isinstance(state_record, Mapping) else {}
        portfolio = state.get("portfolio", {}) if isinstance(state, Mapping) else {}
        risk_state = state.get("risk", {}) if isinstance(state, Mapping) else {}
        duration = max(0.0, (ensure_utc(now) - spec.registration_timestamp).total_seconds())
        risk_equity = _finite(risk_state.get("equity"), spec.bankroll)
        equity = _finite(portfolio.get("equity"), risk_equity)
        prior_peak = max(
            _finite(payload.get("forward_peak_equity"), spec.bankroll),
            _finite(state.get("forward_peak_equity"), spec.bankroll),
        )
        peak = max(prior_peak, equity)
        calculated_drawdown = max(0.0, 1.0 - equity / peak) if peak > 0 else 0.0
        drawdown = max(
            calculated_drawdown,
            _finite(state.get("forward_max_drawdown"), 0.0),
            _finite(risk_state.get("drawdown"), 0.0),
        )
        event_liquidity = [
            _finite(event.get("liquidity"), math.nan)
            for event in attempt_events
            if math.isfinite(_finite(event.get("liquidity"), math.nan))
        ]
        regime_values: set[str] = set()
        for event in attempt_events:
            regime = event.get("regime", event.get("regime_state"))
            if regime is not None and str(regime).strip():
                regime_values.add(str(regime).strip())
        average_slippage = (
            sum(float(fill.slippage) * float(fill.quantity) for fill in fills) / filled_quantity
            if filled_quantity > 0
            else None
        )
        spread_paid = (
            sum(
                max(0.0, _finite(event.get("spread_paid"), 0.0))
                * max(0.0, _finite(event.get("filled_quantity"), 0.0))
                for event in attempt_events
            )
            / filled_quantity
            if filled_quantity > 0
            else None
        )
        risk_reasons = risk_state.get("reasons", ()) if isinstance(risk_state, Mapping) else ()
        risk_reasons = tuple(str(reason) for reason in risk_reasons) if isinstance(risk_reasons, (list, tuple, set)) else ()
        hard_risk_reasons = {
            "max_loss",
            "max_drawdown",
            "max_daily_loss",
            "max_cvar",
            "emergency_kill_switch",
        }
        execution_impossible = (
            order_attempts > 0
            and order_attempts >= self.config.promotion_criteria.min_order_attempts_for_execution_rejection
            and successful_order_attempts == 0
        )
        forward_liquidity = mean(event_liquidity) if event_liquidity else None
        independent_resolved_bets = len(ledgers)
        unresolved_markets = markets_traded - markets_resolved
        total_gross = sum(_finite(ledger.get("gross_pnl"), 0.0) for ledger in ledgers)
        total_fees = sum(_finite(ledger.get("fees"), 0.0) for ledger in ledgers)
        total_slippage = sum(_finite(ledger.get("slippage"), 0.0) for ledger in ledgers)
        total_net = sum(_finite(ledger.get("net_pnl"), 0.0) for ledger in ledgers)
        return {
            "forward_test_id": forward_id,
            "evidence_scope": "forward_only",
            "forward_duration_seconds": duration,
            "markets_observed": len(observed_markets),
            "markets_signaled": len(markets_signaled),
            "markets_traded": len(markets_traded),
            "markets_resolved": len(markets_resolved),
            "forward_trades": successful_order_attempts,
            "trades": successful_order_attempts,
            "forward_order_attempts": order_attempts,
            "order_attempts": order_attempts,
            "forward_successful_order_attempts": successful_order_attempts,
            "successful_order_attempts": successful_order_attempts,
            "forward_failed_order_attempts": failed_order_attempts,
            "failed_order_attempts": failed_order_attempts,
            "risk_rejected_orders": len(risk_rejected_events),
            "fills": len(fills),
            "partial_fills": partial_fills,
            "no_fill_orders": len(no_fill_events),
            "no_fills": len(no_fill_events),
            "observations_without_signal": sum(
                1 for event in event_payloads if str(event.get("status", "")).upper() == "NO_SIGNAL"
            ),
            "requested_quantity": requested_quantity,
            "filled_quantity": filled_quantity,
            "fill_ratio": min(1.0, filled_quantity / requested_quantity) if requested_quantity > 0 else None,
            "depth_consumed": sum(max(0.0, _finite(event.get("depth_consumed"), 0.0)) for event in attempt_events),
            "average_slippage": average_slippage,
            "spread_paid": spread_paid,
            "orders_rejected_for_liquidity": liquidity_rejections,
            "forward_liquidity": forward_liquidity,
            "positions_opened": len({(str(fill.market_id or fill.symbol), str(fill.metadata.get("outcome", "yes"))) for fill in fills}),
            "resolved_positions": resolved_positions,
            "independent_markets_traded": len(markets_traded),
            "forward_independent_resolved_bets": independent_resolved_bets,
            "independent_resolved_bets": independent_resolved_bets,
            "unresolved_markets": len(unresolved_markets),
            "unresolved_fills": sum(
                1
                for fill in fills
                if str(fill.market_id or fill.symbol) not in ledger_by_market
            ),
            "forward_gross_pnl": total_gross if ledgers else None,
            "forward_fees": total_fees if ledgers else None,
            "forward_slippage": total_slippage if ledgers else None,
            "forward_net_pnl": total_net if ledgers else None,
            "forward_expectancy": forward_expectancy,
            "forward_roi": forward_roi,
            "forward_confidence_interval": (
                dict(confidence_interval) if isinstance(confidence_interval, Mapping) else None
            ),
            "forward_confidence_lower_bound": forward_confidence_lower_bound,
            "forward_stability": forward_stability,
            "forward_calibration": forward_calibration,
            "forward_max_drawdown": drawdown,
            "forward_peak_equity": peak,
            "forward_regime_count": len(regime_values),
            "risk_breach": bool(set(risk_reasons) & hard_risk_reasons),
            "model_invalid": False,
            "invariant_violation": False,
            "execution_impossible": execution_impossible,
            "resolved_bet_ids": [str(ledger.get("bet_id", "")) for ledger in ledgers],
            "paper_only": True,
            "forward_benchmark_comparison": (
                {"baseline_expectancy": 0.0, "delta": forward_expectancy}
                if forward_expectancy is not None
                else None
            ),
        }

    def _evaluate_forward_candidate(self, candidate_id: str, now: datetime) -> CandidateLifecycle | None:
        record = self.lifecycle.get(candidate_id)
        if record is None or record.stage is not CandidateStage.PAPER_FORWARD:
            return record
        evidence = self._forward_evidence(record.as_record(), now)
        updated = self.lifecycle.record_evidence(candidate_id, evidence, expected_stage=CandidateStage.PAPER_FORWARD, reason="forward result evaluation")
        reasons = self.config.promotion_criteria.evaluate({**updated.payload, **evidence})
        hard_reasons = self.config.promotion_criteria.hard_rejection_reasons(evidence)
        if hard_reasons:
            return self.lifecycle.reject(
                candidate_id,
                hard_reasons[0],
                evidence=evidence,
                expected_stage=CandidateStage.PAPER_FORWARD,
                expected_payload=updated.payload,
            )
        if not reasons:
            return self.lifecycle.advance(
                candidate_id,
                CandidateStage.PAPER_PROMOTABLE,
                {**evidence, "holdout_used": False},
                reason="paper-forward criteria passed; human review required",
            )
        return updated
def _candidate_id(plan: ExperimentPlan, parameters: Mapping[str, Any], *, generation: int) -> str:
    token = json.dumps(
        {"plan_id": plan.plan_id, "plan_hash": plan.plan_hash, "parameters": dict(parameters), "generation": generation},
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    return "candidate-" + hashlib.sha256(token.encode("utf-8")).hexdigest()[:24]

def _mutation_candidate_id(parent_id: str, generation: int, strategy: StrategyDefinition) -> str:
    document = strategy.to_dict()
    document.pop("strategy_id", None)
    token = json.dumps(
        {"parent": str(parent_id), "generation": generation, "strategy": document, "seed": 0},
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    return "mutation-" + hashlib.sha256(token.encode("utf-8")).hexdigest()[:24]


def _normalize_hypothesis_payload(payload: Mapping[str, Any], item: ResearchQueueItem) -> dict[str, Any]:
    result = dict(payload)
    hypothesis_id = str(result.get("proposal_id", result.get("hypothesis_id", ""))).strip() or item.item_id
    assumptions = result.get("assumptions", {})
    assumptions = assumptions if isinstance(assumptions, Mapping) else {}
    result.setdefault("proposal_id", hypothesis_id)
    result.setdefault("source", str(result.get("author", item.author or item.source or "hermes")) or "hermes")
    result.setdefault("tests", ["bounded chronological backtest and validation"])
    market_type = result.get("market_type")
    plan_value = result.get("experiment_plan")
    if isinstance(plan_value, Mapping):
        market_type = plan_value.get("market_type", market_type)
    # Prediction proposals retain their historical compatibility default. A
    # crypto proposal must state its immutable dataset version explicitly.
    if str(market_type or "prediction").strip().lower() != MarketType.CRYPTO_SPOT.value:
        result.setdefault("dataset_version", assumptions.get("dataset_version", "axiom-persisted-v1"))
    result.setdefault("time_split", "train-validation-holdout")
    result.setdefault("paper_only", True)
    if "experiment_plan" not in result and isinstance(assumptions.get("experiment_plan"), Mapping):
        result["experiment_plan"] = assumptions["experiment_plan"]
    return result


def _normalize_row(value: Any) -> dict[str, Any] | None:
    if isinstance(value, Mapping):
        row = dict(value)
        nested = row.get("snapshot")
        if isinstance(nested, Mapping):
            merged = dict(nested)
            merged.update({key: child for key, child in row.items() if key != "snapshot"})
            row = merged
        payload = row.get("payload")
        if isinstance(payload, Mapping) and isinstance(payload.get("snapshot"), Mapping):
            merged = dict(payload["snapshot"])
            merged.update({key: child for key, child in row.items() if key != "payload"})
            row = merged
        return row
    return None


def _value(row: Mapping[str, Any], name: str, default: Any = None) -> Any:
    return row.get(name, default)

def _normal_symbol(value: Any) -> str:
    return str(value).replace("/", "").replace("-", "").replace("_", "").strip().upper()


def _time_to_expiry(row: Mapping[str, Any]) -> float:
    direct = _finite(row.get("time_to_expiry_seconds"), math.nan)
    if math.isfinite(direct):
        return direct
    stamp = parse_timestamp(row.get("timestamp"))
    expiry = parse_timestamp(row.get("expiry"))
    if stamp is None or expiry is None:
        return math.nan
    return (expiry - stamp).total_seconds()


def _as_regime_set(value: Any, name: str) -> set[str]:
    if isinstance(value, str):
        values = {value}
    elif isinstance(value, (list, tuple, set)):
        values = {str(item) for item in value if str(item).strip()}
    else:
        raise AutonomousResearchError("UNSUPPORTED_FEATURE", f"{name} must be text or a bounded list")
    if not values:
        raise AutonomousResearchError("UNSUPPORTED_FEATURE", f"{name} must not be empty")
    return values


def _in_bound(value: float, bound: Any) -> bool:
    if not math.isfinite(value):
        return False
    if isinstance(bound, (list, tuple)):
        if len(bound) == 2:
            low, high = _finite(bound[0], math.nan), _finite(bound[1], math.nan)
            return math.isfinite(low) and math.isfinite(high) and min(low, high) <= value <= max(low, high)
        return any(abs(value - _finite(item, math.nan)) <= 1e-12 for item in bound if math.isfinite(_finite(item, math.nan)))
    target = _finite(bound, math.nan)
    return math.isfinite(target) and abs(value - target) <= 1e-12


def _has_book(rows: Sequence[Mapping[str, Any]]) -> bool:
    return any(isinstance(row.get("order_book"), Mapping) or isinstance(row.get("yes_order_book"), Mapping) for row in rows)


def _finite(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return float(default)
    return number if math.isfinite(number) else float(default)


def _settlement_name(value: Any) -> str:
    if isinstance(value, SettlementState):
        return value.value
    return str(value or "").strip().lower()


def _is_finite_number(value: Any) -> bool:
    try:
        return math.isfinite(float(value))
    except (TypeError, ValueError):
        return False


def _hash_document(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False, default=str)
    return "sha256:" + hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _compact_evidence(value: Mapping[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, item in value.items():
        if key in {
            "metrics",
            "confidence_interval",
            "forward_confidence_interval",
            "validation_confidence_interval",
            "minimum_sample_check",
            "validation_stability",
            "validation_stability_summary",
            "validation_expectancy",
            "validation_confidence_lower_bound",
            "validation_calibration",
            "benchmark_comparison",
            "forward_benchmark_comparison",
            "validation_regime_behavior",
            "validation_execution_quality",
            "forward_test_id",
            "evidence_scope",
            "forward_duration_seconds",
            "markets_observed",
            "markets_signaled",
            "markets_traded",
            "markets_resolved",
            "forward_order_attempts",
            "order_attempts",
            "forward_trades",
            "trades",
            "forward_successful_order_attempts",
            "successful_order_attempts",
            "forward_failed_order_attempts",
            "failed_order_attempts",
            "risk_rejected_orders",
            "fills",
            "partial_fills",
            "no_fill_orders",
            "no_fills",
            "observations_without_signal",
            "requested_quantity",
            "filled_quantity",
            "fill_ratio",
            "depth_consumed",
            "average_slippage",
            "spread_paid",
            "orders_rejected_for_liquidity",
            "forward_liquidity",
            "positions_opened",
            "resolved_positions",
            "independent_markets_traded",
            "forward_independent_resolved_bets",
            "independent_resolved_bets",
            "unresolved_markets",
            "unresolved_fills",
            "forward_gross_pnl",
            "forward_fees",
            "forward_slippage",
            "forward_net_pnl",
            "forward_expectancy",
            "forward_roi",
            "forward_confidence_lower_bound",
            "forward_stability",
            "forward_calibration",
            "forward_max_drawdown",
            "forward_peak_equity",
            "forward_regime_count",
            "risk_breach",
            "model_invalid",
            "invariant_violation",
            "execution_impossible",
            "resolved_bet_ids",
            "independent_samples",
            "filled_trades",
            "trades",
            "trade_count",
            "expectancy",
            "confidence_lower_bound",
            "stability",
            "calibration",
            "max_drawdown",
            "regime_count",
            "liquidity",
            "costs",
            "paper_only",
        }:
            result[str(key)] = _compact_value(item)
    return result


def _compact_value(value: Any, depth: int = 0) -> Any:
    if depth > 3:
        return "<truncated>"
    if isinstance(value, Mapping):
        return {str(key): _compact_value(child, depth + 1) for key, child in list(value.items())[:32]}
    if isinstance(value, (list, tuple)):
        return [_compact_value(child, depth + 1) for child in list(value)[:32]]
    if isinstance(value, float):
        return value if math.isfinite(value) else 0.0
    if isinstance(value, (str, int, bool)) or value is None:
        return value
    return str(value)


def _bounded_queue_result(value: Mapping[str, Any]) -> dict[str, Any]:
    result = _compact_value(value)
    if not isinstance(result, dict):
        result = {"result": result}
    return result


__all__ = [
    "AutonomousQueueCycle",
    "AutonomousResearchConfig",
    "AutonomousResearchError",
    "AutonomousResearchProcessor",
]
