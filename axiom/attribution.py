"""Explainable selection-score attribution for research reports."""
from __future__ import annotations

from typing import Any

from .evaluation import evaluate_scores


def fitness_attribution(
    train: Any,
    validation: Any,
    holdout: Any,
    *,
    metric: str = "fitness",
    direction: str = "max",
    degradation_weight: float = 1.0,
    overfit_weight: float = 1.0,
) -> dict[str, float | str]:
    """Return named score components without treating holdout as selection."""
    result = evaluate_scores(
        train,
        validation,
        holdout,
        metric=metric,
        direction=direction,
        degradation_weight=degradation_weight,
        overfit_weight=overfit_weight,
    )
    return {
        "train_score": result.train_score,
        "validation_score": result.validation_score,
        "holdout_score_report_only": result.holdout_score,
        "train_to_validation_degradation": result.degradation,
        "overfit_penalty": result.overfit_penalty,
        "holdout_degradation_report_only": result.holdout_degradation,
        "selection_fitness": result.fitness,
        "selection_basis": "validation_only",
        "metric": result.metric,
    }


__all__ = ["fitness_attribution"]
