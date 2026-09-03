"""Reproducible probability-estimation contracts and baseline models.

These models are intentionally statistical and deterministic. Hermes may propose
features or a model version, but no language-model output is accepted as a
probability estimate.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import math
from statistics import NormalDist
from typing import Any, Mapping, Protocol


@dataclass(frozen=True, slots=True)
class ProbabilityEstimate:
    probability: float
    lower: float
    upper: float
    model_version: str
    features: Mapping[str, Any] = field(default_factory=dict)
    sample_size: int = 0
    uncertainty: float = 1.0

    def __post_init__(self) -> None:
        for name in ("probability", "lower", "upper", "uncertainty"):
            value = float(getattr(self, name))
            if not math.isfinite(value):
                raise ValueError(f"{name} must be finite")
        if not 0.0 <= self.probability <= 1.0:
            raise ValueError("probability must be in [0, 1]")
        if not 0.0 <= self.lower <= self.probability <= self.upper <= 1.0:
            raise ValueError("probability interval must be ordered within [0, 1]")
        if self.sample_size < 0:
            raise ValueError("sample_size cannot be negative")
        if not str(self.model_version).strip():
            raise ValueError("model_version is required")

    @property
    def confidence(self) -> float:
        return max(0.0, min(1.0, 1.0 - self.uncertainty))

    def as_record(self) -> dict[str, Any]:
        return {
            "probability": self.probability,
            "lower": self.lower,
            "upper": self.upper,
            "model_version": self.model_version,
            "features": dict(self.features),
            "sample_size": self.sample_size,
            "uncertainty": self.uncertainty,
            "confidence": self.confidence,
        }


class ProbabilityModel(Protocol):
    version: str

    def estimate(self, **features: Any) -> ProbabilityEstimate:
        ...


def _interval(probability: float, sample_size: int, extra_uncertainty: float = 0.0) -> tuple[float, float, float]:
    probability = max(0.0, min(1.0, probability))
    n = max(1, int(sample_size))
    standard_error = math.sqrt(max(0.0, probability * (1.0 - probability)) / n)
    half_width = min(1.0, 1.96 * standard_error + max(0.0, extra_uncertainty))
    lower, upper = max(0.0, probability - half_width), min(1.0, probability + half_width)
    uncertainty = min(1.0, half_width + (1.0 / math.sqrt(n)))
    return lower, upper, uncertainty


@dataclass(frozen=True, slots=True)
class CryptoPriceTargetModel:
    """Lognormal probability that a price finishes above a target."""

    version: str = "crypto-price-target-normal-v1"
    annualization_seconds: float = 365.25 * 24.0 * 60.0 * 60.0

    def estimate(
        self,
        *,
        current_price: float,
        target_price: float,
        time_to_expiry_seconds: float,
        realized_volatility: float,
        drift: float = 0.0,
        momentum: float = 0.0,
        sample_size: int = 0,
        volatility_uncertainty: float = 0.0,
        **extra: Any,
    ) -> ProbabilityEstimate:
        values = (current_price, target_price, time_to_expiry_seconds, realized_volatility, drift, momentum, volatility_uncertainty)
        if not all(math.isfinite(float(value)) for value in values):
            raise ValueError("price-target features must be finite")
        if current_price <= 0 or target_price <= 0 or time_to_expiry_seconds < 0 or realized_volatility < 0:
            raise ValueError("price-target features are out of range")
        horizon = max(0.0, time_to_expiry_seconds) / self.annualization_seconds
        if horizon == 0.0:
            probability = 1.0 if current_price >= target_price else 0.0
        else:
            sigma = realized_volatility * math.sqrt(horizon)
            mu = (drift + momentum) * horizon
            if sigma <= 1e-12:
                probability = 1.0 if math.log(current_price / target_price) + mu >= 0 else 0.0
            else:
                z = (math.log(target_price / current_price) - mu) / sigma
                probability = 1.0 - NormalDist().cdf(z)
        lower, upper, uncertainty = _interval(probability, sample_size, volatility_uncertainty)
        features = {
            "current_price": current_price,
            "target_price": target_price,
            "time_to_expiry_seconds": time_to_expiry_seconds,
            "realized_volatility": realized_volatility,
            "drift": drift,
            "momentum": momentum,
            **extra,
        }
        return ProbabilityEstimate(probability, lower, upper, self.version, features, max(0, int(sample_size)), uncertainty)


@dataclass(frozen=True, slots=True)
class BaseRateModel:
    """Beta-smoothed historical event-frequency model."""

    version: str = "historical-base-rate-beta-v1"
    prior_probability: float = 0.5
    prior_strength: float = 2.0

    def __post_init__(self) -> None:
        if (
            not math.isfinite(float(self.prior_probability))
            or not math.isfinite(float(self.prior_strength))
            or not 0.0 <= self.prior_probability <= 1.0
            or self.prior_strength <= 0
        ):
            raise ValueError("invalid base-rate prior")
        object.__setattr__(self, "prior_probability", float(self.prior_probability))
        object.__setattr__(self, "prior_strength", float(self.prior_strength))

    def estimate(self, *, successes: int, trials: int, **extra: Any) -> ProbabilityEstimate:
        if (
            any(isinstance(value, bool) or not isinstance(value, int) for value in (successes, trials))
            or trials < 0
            or successes < 0
            or successes > trials
        ):
            raise ValueError("successes/trials must form valid integer counts")
        n = trials + self.prior_strength
        probability = (successes + self.prior_probability * self.prior_strength) / n
        lower, upper, uncertainty = _interval(probability, int(n))
        features = {"successes": successes, "trials": trials, **extra}
        return ProbabilityEstimate(probability, lower, upper, self.version, features, trials, uncertainty)

@dataclass(frozen=True, slots=True)
class BetaBelief:
    """Conjugate Bayesian belief state for a binary event."""

    alpha: float = 1.0
    beta: float = 1.0
    version: str = "beta-belief-v1"

    def __post_init__(self) -> None:
        alpha, beta = float(self.alpha), float(self.beta)
        if not all(math.isfinite(value) and value > 0 for value in (alpha, beta)):
            raise ValueError("Beta belief parameters must be finite and positive")
        if not str(self.version).strip():
            raise ValueError("belief version is required")
        object.__setattr__(self, "alpha", alpha)
        object.__setattr__(self, "beta", beta)

    @property
    def mean(self) -> float:
        return self.alpha / (self.alpha + self.beta)

    @property
    def variance(self) -> float:
        total = self.alpha + self.beta
        return self.alpha * self.beta / (total * total * (total + 1.0))

    def update(self, *, successes: float = 0.0, failures: float = 0.0) -> "BetaBelief":
        successes, failures = float(successes), float(failures)
        if not all(math.isfinite(value) and value >= 0 for value in (successes, failures)):
            raise ValueError("belief updates must be finite and non-negative")
        return BetaBelief(self.alpha + successes, self.beta + failures, self.version)

    def observe(self, outcome: bool | float, *, weight: float = 1.0) -> "BetaBelief":
        value, weight = float(outcome), float(weight)
        if value not in {0.0, 1.0} or not math.isfinite(weight) or weight < 0:
            raise ValueError("binary observations require outcome 0/1 and non-negative weight")
        return self.update(successes=value * weight, failures=(1.0 - value) * weight)

    def estimate(self) -> ProbabilityEstimate:
        total = self.alpha + self.beta
        half_width = min(1.0, 1.96 * math.sqrt(self.variance))
        lower = max(0.0, self.mean - half_width)
        upper = min(1.0, self.mean + half_width)
        uncertainty = min(1.0, half_width + 1.0 / math.sqrt(total))
        return ProbabilityEstimate(
            self.mean,
            lower,
            upper,
            self.version,
            {"alpha": self.alpha, "beta": self.beta},
            max(0, int(round(total))),
            uncertainty,
        )


BayesianBelief = BetaBelief

class ProbabilityModelRegistry:
    """Version-keyed model registry; unknown versions fail closed."""

    def __init__(self, models: Mapping[str, ProbabilityModel] | None = None) -> None:
        self._models: dict[str, ProbabilityModel] = dict(models or {})

    def register(self, model: ProbabilityModel) -> None:
        version = str(getattr(model, "version", "")).strip()
        if not version:
            raise ValueError("probability models require a version")
        self._models[version] = model

    def get(self, version: str) -> ProbabilityModel:
        try:
            return self._models[str(version)]
        except KeyError as exc:
            raise KeyError(f"unknown probability model version: {version}") from exc

    def estimate(self, version: str, **features: Any) -> ProbabilityEstimate:
        return self.get(version).estimate(**features)

    def versions(self) -> tuple[str, ...]:
        return tuple(sorted(self._models))


DEFAULT_PROBABILITY_MODELS = ProbabilityModelRegistry()
DEFAULT_PROBABILITY_MODELS.register(CryptoPriceTargetModel())
DEFAULT_PROBABILITY_MODELS.register(BaseRateModel())


__all__ = [
    "BaseRateModel",
    "BayesianBelief",
    "BetaBelief",
    "CryptoPriceTargetModel",
    "DEFAULT_PROBABILITY_MODELS",
    "ProbabilityEstimate",
    "ProbabilityModel",
    "ProbabilityModelRegistry",
]
