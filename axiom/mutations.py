"""Deterministic strategy mutations and persisted experiment budgets."""
from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime
import hashlib
import json
import math
from typing import Any, Mapping, Sequence

from .lifecycle import CandidateLifecycleManager
from .storage import AxiomStore
from .strategy import StrategyDefinition, validate_strategy


@dataclass(frozen=True, slots=True)
class MutationCandidate:
    candidate_id: str
    parent_id: str
    generation: int
    strategy: StrategyDefinition
    lineage: tuple[str, ...]
    provenance: Mapping[str, Any]

    def as_record(self) -> dict[str, Any]:
        return {
            "candidate_id": self.candidate_id,
            "parent_id": self.parent_id,
            "generation": self.generation,
            "strategy": self.strategy.to_dict(),
            "lineage": list(self.lineage),
            "provenance": dict(self.provenance),
        }


@dataclass(frozen=True, slots=True)
class ExperimentBudget:
    budget_id: str = "default"
    total_limit: int = 1000
    per_family_limit: int = 250
    used_total: int = 0
    used_by_family: Mapping[str, int] = field(default_factory=dict)
    def __post_init__(self) -> None:
        if isinstance(self.total_limit, bool) or not isinstance(self.total_limit, int) or self.total_limit < 0:
            raise ValueError("total_limit must be a non-negative integer")
        if isinstance(self.per_family_limit, bool) or not isinstance(self.per_family_limit, int) or self.per_family_limit < 0:
            raise ValueError("per_family_limit must be a non-negative integer")
        if isinstance(self.used_total, bool) or not isinstance(self.used_total, int) or self.used_total < 0:
            raise ValueError("used_total must be a non-negative integer")
        family: dict[str, int] = {}
        for key, value in dict(self.used_by_family or {}).items():
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError("family budget usage must be non-negative integers")
            family[str(key)] = value
        if (
            self.used_total > self.total_limit
            or sum(family.values()) > self.used_total
            or any(value > self.per_family_limit for value in family.values())
        ):
            raise ValueError("budget usage exceeds configured limits")
        object.__setattr__(self, "used_by_family", family)

    @property
    def remaining_total(self) -> int:
        return max(0, self.total_limit - self.used_total)

    def allocate(self, family: str, amount: int = 1) -> "ExperimentBudget":
        if isinstance(amount, bool) or not isinstance(amount, int) or amount <= 0:
            raise ValueError("budget amount must be a positive integer")
        family_name = str(family).strip() or "unknown"
        used_family = int(self.used_by_family.get(family_name, 0))
        if self.used_total + amount > self.total_limit:
            raise RuntimeError("experiment budget exhausted")
        if used_family + amount > self.per_family_limit:
            raise RuntimeError(f"experiment family budget exhausted: {family_name}")
        usage = dict(self.used_by_family)
        usage[family_name] = used_family + amount
        return ExperimentBudget(self.budget_id, self.total_limit, self.per_family_limit, self.used_total + amount, usage)

    def as_record(self) -> dict[str, Any]:
        return {
            "budget_id": self.budget_id,
            "total_limit": self.total_limit,
            "per_family_limit": self.per_family_limit,
            "used_total": self.used_total,
            "used_by_family": dict(sorted(self.used_by_family.items())),
        }


class DeterministicMutationEngine:
    """Enumerate bounded numeric mutations; no language model or holdout feedback."""

    def __init__(
        self,
        *,
        store: AxiomStore | None = None,
        lifecycle: CandidateLifecycleManager | None = None,
        budget: ExperimentBudget | None = None,
        seed: int = 0,
    ) -> None:
        self.store = store
        self.lifecycle = lifecycle or (CandidateLifecycleManager(store) if store is not None else None)
        self.budget = budget or ExperimentBudget()
        self.seed = int(seed)
        if self.store is not None:
            persisted = self.store.load_experiment_budget(self.budget.budget_id)
            if persisted and isinstance(persisted.get("budget"), Mapping):
                self.budget = ExperimentBudget(**dict(persisted["budget"]))

    def mutate(
        self,
        parent: StrategyDefinition | Mapping[str, Any] | str,
        *,
        parent_id: str,
        generation: int,
        max_variants: int = 8,
        family: str | None = None,
        provenance: Mapping[str, Any] | None = None,
        lineage: Sequence[str] = (),
        timestamp: datetime | None = None,
    ) -> tuple[MutationCandidate, ...]:
        if isinstance(max_variants, bool) or not isinstance(max_variants, int) or max_variants < 0:
            raise ValueError("max_variants must be a non-negative integer")
        if isinstance(generation, bool) or not isinstance(generation, int) or generation < 0:
            raise ValueError("generation must be a non-negative integer")
        definition = validate_strategy(parent)
        family_name = str(family or getattr(definition, "family", "unknown"))
        parameters = dict(definition.parameters)
        variants: list[dict[str, Any]] = []
        for key in sorted(parameters):
            value = parameters[key]
            if isinstance(value, bool):
                continue
            if isinstance(value, int):
                changes = (value - 2, value - 1, value + 1, value + 2)
            elif isinstance(value, float) and math.isfinite(value):
                changes = (value * 0.9, value * 0.95, value * 1.05, value * 1.1)
            else:
                continue
            for changed in changes:
                next_parameters = dict(parameters)
                next_parameters[key] = max(1, int(changed)) if isinstance(value, int) else max(0.0, float(changed))
                variants.append(next_parameters)
        if not variants:
            variants = [{**parameters, "threshold": threshold} for threshold in (0.01, 0.02, 0.05, 0.1)]
        candidates: list[MutationCandidate] = []
        seen: set[str] = set()
        lineage_value = tuple(dict.fromkeys([*(str(item) for item in lineage), str(parent_id)]))
        for next_parameters in variants:
            if len(candidates) >= max_variants:
                break
            document = definition.to_dict()
            document.pop("strategy_id", None)
            document["parameters"] = next_parameters
            child = validate_strategy(document)
            token = _canonical({"parent": parent_id, "generation": generation, "strategy": child.to_dict(), "seed": self.seed})
            candidate_id = "mutation-" + hashlib.sha256(token.encode("utf-8")).hexdigest()[:24]
            if candidate_id in seen:
                continue
            seen.add(candidate_id)
            previous_budget = self.budget
            try:
                if self.store is not None:
                    with self.store.transaction():
                        reservation = self.store.reserve_experiment_budget(
                            self.budget.budget_id,
                            total_limit=self.budget.total_limit,
                            per_family_limit=self.budget.per_family_limit,
                            family=family_name,
                            reservation_key=candidate_id,
                            timestamp=timestamp,
                        )
                        next_budget = ExperimentBudget(**dict(reservation["budget"]))
                        evidence = {
                            "candidate_id": candidate_id,
                            "family": family_name,
                            "generation": generation,
                            "parent_id": str(parent_id),
                            "lineage": list(lineage_value),
                            "strategy": child.to_dict(),
                            "provenance": dict(provenance or {}),
                            "mutation_engine": "deterministic",
                            "seed": self.seed,
                            "holdout_used": False,
                        }
                        candidate = MutationCandidate(candidate_id, str(parent_id), generation, child, lineage_value, evidence["provenance"])
                        if self.lifecycle is not None:
                            self.lifecycle.register_idea(candidate_id, evidence)
                else:
                    next_budget = self.budget.allocate(family_name)
                    evidence = {
                        "candidate_id": candidate_id,
                        "family": family_name,
                        "generation": generation,
                        "parent_id": str(parent_id),
                        "lineage": list(lineage_value),
                        "strategy": child.to_dict(),
                        "provenance": dict(provenance or {}),
                        "mutation_engine": "deterministic",
                        "seed": self.seed,
                        "holdout_used": False,
                    }
                    candidate = MutationCandidate(candidate_id, str(parent_id), generation, child, lineage_value, evidence["provenance"])
                    if self.lifecycle is not None:
                        self.lifecycle.register_idea(candidate_id, evidence)
            except RuntimeError:
                self.budget = previous_budget
                break
            except Exception:
                self.budget = previous_budget
                raise
            self.budget = next_budget
            candidates.append(candidate)
        if self.store is None:
            self._persist_budget()
        return tuple(candidates)

    def _persist_budget(self) -> None:
        if self.store is not None:
            self.store.save_experiment_budget(self.budget.budget_id, self.budget.as_record())


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False, default=str)


__all__ = ["DeterministicMutationEngine", "ExperimentBudget", "MutationCandidate"]
