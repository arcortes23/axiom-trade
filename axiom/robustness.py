"""Small-sample and multiple-testing safeguards for research reports."""
from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
import math
import random
from statistics import mean, median, pstdev
from typing import Any, Sequence


def bootstrap_confidence_interval(
    values: Iterable[float],
    *,
    statistic: str | Callable[[Sequence[float]], float] = "mean",
    confidence: float = 0.95,
    resamples: int = 2000,
    seed: int = 0,
) -> dict[str, float | int | str]:
    try:
        confidence_value = float(confidence)
    except (TypeError, ValueError) as exc:
        raise ValueError("confidence must be finite and in (0,1)") from exc
    if (
        not math.isfinite(confidence_value)
        or not 0.0 < confidence_value < 1.0
        or not isinstance(resamples, int)
        or isinstance(resamples, bool)
        or resamples < 1
    ):
        raise ValueError("confidence must be finite and in (0,1); resamples must be a positive integer")
    fn: Callable[[Sequence[float]], float]
    if callable(statistic):
        fn = statistic
        label = getattr(statistic, "__name__", "custom")
    elif statistic == "mean":
        fn, label = lambda sample: mean(sample), "mean"
    elif statistic == "median":
        fn, label = lambda sample: median(sample), "median"
    else:
        raise ValueError("statistic must be mean, median, or callable")
    observations: list[float] = []
    for value in values:
        try:
            number = float(value)
        except (TypeError, ValueError):
            continue
        if math.isfinite(number):
            observations.append(number)
    if not observations:
        return {
            "count": 0,
            "estimate": 0.0,
            "lower": 0.0,
            "upper": 0.0,
            "confidence": confidence_value,
            "resamples": int(resamples),
            "statistic": label,
        }

    def evaluate(sample: Sequence[float]) -> float:
        try:
            estimate = float(fn(sample))
        except (TypeError, ValueError, ArithmeticError) as exc:
            raise ValueError("bootstrap statistic must return a finite number") from exc
        if not math.isfinite(estimate):
            raise ValueError("bootstrap statistic must return a finite number")
        return estimate

    rng = random.Random(seed)
    estimates = [
        evaluate([observations[rng.randrange(len(observations))] for _ in observations])
        for _ in range(resamples)
    ]
    estimates.sort()
    lower_index = max(0, min(len(estimates) - 1, int((1.0 - confidence_value) * 0.5 * len(estimates))))
    upper_index = max(0, min(len(estimates) - 1, int((1.0 - (1.0 - confidence_value) * 0.5) * len(estimates)) - 1))
    return {
        "count": len(observations),
        "estimate": evaluate(observations),
        "lower": estimates[lower_index],
        "upper": estimates[upper_index],
        "confidence": confidence_value,
        "resamples": int(resamples),
        "statistic": label,
    }


def minimum_sample_check(
    observations: int | Iterable[Any],
    *,
    min_observations: int = 30,
    trades: int | None = None,
    min_trades: int = 10,
) -> dict[str, Any]:
    """Enforce explicit minimum observation and trade counts."""
    if isinstance(observations, bool):
        raise ValueError("observations must be a non-negative integer or iterable")
    if isinstance(observations, int):
        count = observations
    else:
        count = sum(1 for _ in observations)
    for value, name in ((min_observations, "min_observations"), (min_trades, "min_trades")):
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError(f"{name} must be a non-negative integer")
    if trades is None:
        trade_count = None
    elif isinstance(trades, bool) or not isinstance(trades, int):
        raise ValueError("trades must be a non-negative integer or None")
    else:
        trade_count = trades
    if count < 0 or trade_count is not None and trade_count < 0:
        raise ValueError("sample counts cannot be negative")
    checks = {
        "observations": count >= min_observations,
        "trades": trade_count is None or trade_count >= min_trades,
    }
    return {
        "passed": all(checks.values()),
        "count": count,
        "min_observations": min_observations,
        "trades": trade_count,
        "min_trades": min_trades,
        "checks": checks,
    }


def neighboring_parameter_stability(
    scores: Mapping[str, float] | Iterable[float],
    *,
    tolerance: float = 0.10,
) -> dict[str, float | int | bool]:
    """Summarize whether nearby parameter scores remain close to the center."""
    try:
        tolerance_value = float(tolerance)
    except (TypeError, ValueError) as exc:
        raise ValueError("tolerance must be finite and non-negative") from exc
    if not math.isfinite(tolerance_value) or tolerance_value < 0:
        raise ValueError("tolerance must be finite and non-negative")
    values: list[float] = []
    for value in (scores.values() if isinstance(scores, Mapping) else scores):
        try:
            number = float(value)
        except (TypeError, ValueError):
            continue
        if math.isfinite(number):
            values.append(number)
    if not values:
        return {"count": 0, "mean": 0.0, "stdev": 0.0, "min": 0.0, "max": 0.0, "stable_fraction": 0.0, "stable": False}
    centre = mean(values)
    scale = max(1.0, abs(centre))
    stable = [abs(value - centre) <= tolerance_value * scale for value in values]
    return {
        "count": len(values),
        "mean": centre,
        "stdev": pstdev(values) if len(values) > 1 else 0.0,
        "min": min(values),
        "max": max(values),
        "stable_fraction": sum(stable) / len(stable),
        "stable": all(stable),
    }


def multiple_testing_summary(
    p_values: Mapping[str, float] | Iterable[float],
    *,
    alpha: float = 0.05,
) -> dict[str, Any]:
    """Report Bonferroni and Benjamini-Hochberg discoveries."""
    try:
        alpha_value = float(alpha)
    except (TypeError, ValueError) as exc:
        raise ValueError("alpha must be finite and in (0,1)") from exc
    if not math.isfinite(alpha_value) or not 0.0 < alpha_value < 1.0:
        raise ValueError("alpha must be finite and in (0,1)")
    named = list(p_values.items()) if isinstance(p_values, Mapping) else [(str(index), value) for index, value in enumerate(p_values)]
    valid: list[tuple[str, float]] = []
    for name, value in named:
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            continue
        if math.isfinite(numeric) and 0.0 <= numeric <= 1.0:
            valid.append((str(name), numeric))
    valid.sort(key=lambda item: (item[1], item[0]))
    count = len(valid)
    bonferroni_threshold = alpha_value / count if count else alpha_value
    bonferroni = [name for name, value in valid if value <= bonferroni_threshold]
    bh_rank = 0
    for rank, (_, value) in enumerate(valid, start=1):
        if value <= alpha_value * rank / max(1, count):
            bh_rank = rank
    benjamini = [name for rank, (name, _) in enumerate(valid, start=1) if rank <= bh_rank]
    return {
        "tests": count,
        "alpha": alpha_value,
        "bonferroni_threshold": bonferroni_threshold,
        "bonferroni_discoveries": bonferroni,
        "benjamini_hochberg_discoveries": benjamini,
        "invalid_values": len(named) - count,
    }


__all__ = [
    "bootstrap_confidence_interval",
    "minimum_sample_check",
    "multiple_testing_summary",
    "neighboring_parameter_stability",
]
