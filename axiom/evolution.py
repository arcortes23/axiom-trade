"""Deterministic, holdout-aware evolution of declarative strategies."""
from __future__ import annotations

from dataclasses import dataclass, replace
import hashlib
import math
import random
from typing import Any, Iterable, Mapping, Sequence

from .domain import MarketType
from .strategy import CRYPTO_FAMILIES, PREDICTION_FAMILIES, StrategyDefinition, evaluate_signal, validate_strategy


@dataclass(slots=True)
class EvolutionCandidate:
    strategy: StrategyDefinition
    candidate_id: str
    lineage: tuple[str, ...] = ()
    generation: int = 0
    score: float | None = None
    train_score: float | None = None
    validation_score: float | None = None
    holdout_score: float | None = None
    rejected: bool = False
    rejection_reason: str = ""

    @property
    def fitness(self) -> float:
        return float(self.score if self.score is not None else float("-inf"))

    @property
    def parents(self) -> tuple[str, ...]:
        return self.lineage


@dataclass(frozen=True, slots=True)
class EvolutionResult:
    generation: int
    population: tuple[EvolutionCandidate, ...]
    selected: tuple[EvolutionCandidate, ...]
    holdout: tuple[EvolutionCandidate, ...] = ()


def _candidate_id(strategy: StrategyDefinition, lineage: Sequence[str] = (), generation: int = 0) -> str:
    token = strategy.to_json() + "|" + ",".join(lineage) + f"|{generation}"
    return hashlib.sha256(token.encode("utf-8")).hexdigest()[:16]


class EvolutionEngine:
    """Generate, score, reject and combine strategies with an isolated holdout."""
    def __init__(self, population_size: int = 16, *, seed: int = 0, holdout_fraction: float = 0.2, mutation_rate: float = 0.35, elite_fraction: float = 0.25) -> None:
        if population_size < 1:
            raise ValueError("population_size must be positive")
        values = (holdout_fraction, mutation_rate, elite_fraction)
        if not all(math.isfinite(float(value)) for value in values):
            raise ValueError("evolution fractions must be finite")
        if not 0 <= holdout_fraction < 1:
            raise ValueError("holdout_fraction must be in [0, 1)")
        if not 0 <= mutation_rate <= 1 or not 0 <= elite_fraction <= 1:
            raise ValueError("mutation_rate and elite_fraction must be in [0, 1]")
        self.population_size = int(population_size)
        self.seed = seed
        self.random = random.Random(seed)
        self.holdout_fraction = float(holdout_fraction)
        self.mutation_rate = float(mutation_rate)
        self.elite_fraction = float(elite_fraction)
        self.generation = 0
        self.population: list[EvolutionCandidate] = []

    def _default_strategy(self, index: int, market_type: MarketType = MarketType.CRYPTO_SPOT) -> StrategyDefinition:
        if market_type is MarketType.CRYPTO_SPOT:
            family = sorted(CRYPTO_FAMILIES)[index % len(CRYPTO_FAMILIES)]
            parameters: dict[str, Any] = {"lookback": 3 + index % 12, "threshold": 0.02}
            if family == "rsi":
                parameters = {"period": 5 + index % 10, "oversold": 30.0, "overbought": 70.0}
            return validate_strategy({"version": 1, "market_type": market_type.value, "family": family, "parameters": parameters, "strategy_id": f"seed-{index}"})
        family = sorted(PREDICTION_FAMILIES)[index % len(PREDICTION_FAMILIES)]
        return validate_strategy({"version": 1, "market_type": market_type.value, "family": family, "parameters": {"threshold": 0.05}, "probability_model": "seed", "resolution_aware": True, "resolution_inputs": ["expiry", "settlement"], "strategy_id": f"seed-{index}"})

    def initialize(self, strategies: Sequence[StrategyDefinition | Mapping[str, Any] | str] | None = None, *, market_type: MarketType | str = MarketType.CRYPTO_SPOT) -> tuple[EvolutionCandidate, ...]:
        kind = market_type if isinstance(market_type, MarketType) else MarketType(str(market_type))
        definitions = [validate_strategy(strategy) for strategy in strategies] if strategies else [self._default_strategy(index, kind) for index in range(self.population_size)]
        self.population = []
        for definition in definitions[: self.population_size]:
            candidate = EvolutionCandidate(definition, _candidate_id(definition, (), self.generation), (), self.generation)
            self.population.append(candidate)
        return tuple(self.population)

    @staticmethod
    def _numeric_mutation(key: str, value: Any, rng: random.Random) -> Any:
        if isinstance(value, bool):
            return value
        if isinstance(value, int):
            return max(1, value + rng.choice((-2, -1, 1, 2)))
        if isinstance(value, float):
            return max(0.0, value * (1.0 + rng.uniform(-0.25, 0.25)))
        return value

    def mutate(self, candidate: EvolutionCandidate | StrategyDefinition | Mapping[str, Any] | str, *, generation: int | None = None) -> EvolutionCandidate:
        parent = candidate if isinstance(candidate, EvolutionCandidate) else EvolutionCandidate(validate_strategy(candidate), "", (), self.generation)
        document = parent.strategy.to_dict()
        parameters = dict(document.get("parameters", {}))
        keys = sorted(parameters)
        if keys:
            key = keys[self.random.randrange(len(keys))]
            parameters[key] = self._numeric_mutation(key, parameters[key], self.random)
        else:
            parameters["threshold"] = round(self.random.uniform(0.01, 0.10), 6)
        document["parameters"] = parameters
        document.pop("strategy_id", None)
        definition = validate_strategy(document)
        generation_value = self.generation if generation is None else generation
        lineage = (parent.candidate_id,) if parent.candidate_id else ()
        return EvolutionCandidate(definition, _candidate_id(definition, lineage, generation_value), lineage, generation_value)

    def hybrid(self, first: EvolutionCandidate | StrategyDefinition | Mapping[str, Any] | str, second: EvolutionCandidate | StrategyDefinition | Mapping[str, Any] | str, *, generation: int | None = None) -> EvolutionCandidate:
        left = first.strategy if isinstance(first, EvolutionCandidate) else validate_strategy(first)
        right = second.strategy if isinstance(second, EvolutionCandidate) else validate_strategy(second)
        if left.market_type is not right.market_type:
            raise ValueError("hybrid parents must share market_type")
        params: dict[str, Any] = {}
        for key in sorted(set(left.parameters) | set(right.parameters)):
            source = left.parameters if self.random.random() < 0.5 else right.parameters
            if key in source:
                params[key] = source[key]
        document = left.to_dict()
        document["parameters"] = params
        document.pop("strategy_id", None)
        definition = validate_strategy(document)
        lineage = tuple(item.candidate_id for item in (first, second) if isinstance(item, EvolutionCandidate) and item.candidate_id)
        generation_value = self.generation if generation is None else generation
        return EvolutionCandidate(definition, _candidate_id(definition, lineage, generation_value), lineage, generation_value)

    def reject(self, candidate: EvolutionCandidate, reason: str = "rejected") -> EvolutionCandidate:
        candidate.rejected = True
        candidate.rejection_reason = reason
        return candidate

    @staticmethod
    def _score_values(scores: Iterable[float]) -> float:
        values = [float(value) for value in scores]
        return sum(values) / len(values) if values else 0.0

    def _score_rows(
        self,
        candidate: EvolutionCandidate,
        observations: Sequence[Any],
        outcomes: Sequence[float] | None = None,
    ) -> float:
        signals = [evaluate_signal(candidate.strategy, observation) for observation in observations]
        if outcomes is not None:
            if len(outcomes) != len(signals):
                raise ValueError("observations and outcomes must have equal lengths")
            values = (signal * (1.0 if float(outcome) > 0 else -1.0) for signal, outcome in zip(signals, outcomes))
        else:
            values = (abs(signal) for signal in signals)
        return self._score_values(values)

    def score_candidate(
        self,
        candidate: EvolutionCandidate,
        observations: Sequence[Any] | None = None,
        *,
        outcomes: Sequence[float] | None = None,
        score: float | None = None,
    ) -> float:
        value = float(score) if score is not None else (
            candidate.score if observations is None and candidate.score is not None
            else self._score_rows(candidate, observations or (), outcomes)
        )
        if not math.isfinite(float(value)):
            raise ValueError("candidate score must be finite")
        candidate.score = float(value)
        candidate.train_score = float(value)
        return float(value)

    score = score_candidate

    def split_holdout(self, observations: Sequence[Any]) -> tuple[tuple[Any, ...], tuple[Any, ...]]:
        cut = int(len(observations) * (1.0 - self.holdout_fraction))
        return tuple(observations[:cut]), tuple(observations[cut:])

    def evaluate_holdout(self, candidate: EvolutionCandidate, observations: Sequence[Any], *, outcomes: Sequence[float] | None = None) -> float:
        _, holdout = self.split_holdout(observations)
        if outcomes is not None:
            if len(outcomes) != len(observations):
                raise ValueError("observations and outcomes must have equal lengths")
            _, holdout_outcomes = self.split_holdout(outcomes)
        else:
            holdout_outcomes = ()
        signals = [evaluate_signal(candidate.strategy, observation) for observation in holdout]
        if outcomes is not None:
            value = self._score_values(signal * (1.0 if float(outcome) > 0 else -1.0) for signal, outcome in zip(signals, holdout_outcomes))
        else:
            value = self._score_values(abs(signal) for signal in signals)
        candidate.holdout_score = value
        return value

    def _evaluate_holdout_rows(self, candidate: EvolutionCandidate, observations: Sequence[Any], outcomes: Sequence[float] | None = None) -> float:
        signals = [evaluate_signal(candidate.strategy, observation) for observation in observations]
        if outcomes is not None:
            if len(outcomes) != len(signals):
                raise ValueError("observations and outcomes must have equal lengths")
            value = self._score_values(signal * (1.0 if float(outcome) > 0 else -1.0) for signal, outcome in zip(signals, outcomes))
        else:
            value = self._score_values(abs(signal) for signal in signals)
        candidate.holdout_score = value
        return value


    def evolve(
        self,
        observations: Sequence[Any] | None = None,
        *,
        outcomes: Sequence[float] | None = None,
        validation: Sequence[Any] | None = None,
        validation_outcomes: Sequence[float] | None = None,
        holdout: Sequence[Any] | None = None,
        holdout_outcomes: Sequence[float] | None = None,
    ) -> EvolutionResult:
        if not self.population:
            self.initialize()
        self.generation += 1
        holdout_candidates: list[EvolutionCandidate] = []
        if observations is not None:
            train = tuple(observations)
            validation_rows: tuple[Any, ...] = ()
            isolated_holdout: tuple[Any, ...]
            train_outcomes: tuple[float, ...] | None = None
            validation_labels: tuple[float, ...] | None = None
            isolated_holdout_labels: tuple[float, ...] | None = None
            supplied_outcomes = tuple(outcomes) if outcomes is not None else None
            if validation is not None:
                validation_rows = tuple(validation)
                isolated_holdout = tuple(holdout or ())
                total = len(train) + len(validation_rows) + len(isolated_holdout)
                if supplied_outcomes is not None:
                    if len(supplied_outcomes) == total:
                        train_outcomes = supplied_outcomes[: len(train)]
                        validation_labels = supplied_outcomes[len(train) : len(train) + len(validation_rows)]
                        isolated_holdout_labels = supplied_outcomes[len(train) + len(validation_rows) :]
                    elif len(supplied_outcomes) == len(train):
                        train_outcomes = supplied_outcomes
                    else:
                        raise ValueError("outcomes must match train rows or concatenated train/validation/holdout rows")
                if validation_outcomes is not None:
                    if len(validation_outcomes) != len(validation_rows):
                        raise ValueError("validation and validation_outcomes must have equal lengths")
                    validation_labels = tuple(validation_outcomes)
                if holdout_outcomes is not None:
                    if len(holdout_outcomes) != len(isolated_holdout):
                        raise ValueError("holdout and holdout_outcomes must have equal lengths")
                    isolated_holdout_labels = tuple(holdout_outcomes)
            elif holdout is None:
                train, isolated_holdout = self.split_holdout(train)
                if supplied_outcomes is not None:
                    if len(supplied_outcomes) != len(train) + len(isolated_holdout):
                        raise ValueError("observations and outcomes must have equal lengths")
                    train_outcomes = supplied_outcomes[: len(train)]
                    isolated_holdout_labels = supplied_outcomes[len(train) :]
            else:
                isolated_holdout = tuple(holdout)
                if supplied_outcomes is not None:
                    if len(supplied_outcomes) == len(train) + len(isolated_holdout):
                        train_outcomes = supplied_outcomes[: len(train)]
                        isolated_holdout_labels = supplied_outcomes[len(train) :]
                    elif len(supplied_outcomes) == len(train):
                        train_outcomes = supplied_outcomes
                    else:
                        raise ValueError("outcomes must match train rows or concatenated train/holdout rows")
                if holdout_outcomes is not None:
                    if len(holdout_outcomes) != len(isolated_holdout):
                        raise ValueError("holdout and holdout_outcomes must have equal lengths")
                    isolated_holdout_labels = tuple(holdout_outcomes)
            for candidate in self.population:
                self.score_candidate(candidate, train, outcomes=train_outcomes)
                candidate.validation_score = None
                if validation_rows:
                    candidate.validation_score = self._score_rows(candidate, validation_rows, validation_labels)
                    # Selection and mutation use validation only; holdout is
                    # evaluated on detached copies below.
                    candidate.score = candidate.validation_score
            holdout_candidates = [
                replace(candidate, holdout_score=None, lineage=tuple(candidate.lineage))
                for candidate in self.population
            ]
            for candidate in holdout_candidates:
                self._evaluate_holdout_rows(candidate, isolated_holdout, isolated_holdout_labels)
        ranked = sorted(
            (candidate for candidate in self.population if not candidate.rejected),
            key=lambda item: item.fitness,
            reverse=True,
        )
        elite_count = max(1, int(len(ranked) * self.elite_fraction)) if ranked else 0
        selected = ranked[:elite_count]
        offspring: list[EvolutionCandidate] = list(selected)
        while len(offspring) < self.population_size and selected:
            parent = selected[self.random.randrange(len(selected))]
            child = (
                self.mutate(parent, generation=self.generation)
                if self.random.random() <= self.mutation_rate
                else self.hybrid(parent, selected[self.random.randrange(len(selected))], generation=self.generation)
            )
            offspring.append(child)
        if offspring:
            self.population = offspring[: self.population_size]
        return EvolutionResult(self.generation, tuple(self.population), tuple(selected), tuple(holdout_candidates))

    next_generation = evolve


Candidate = EvolutionCandidate
Evolution = EvolutionEngine


__all__ = ["Candidate", "Evolution", "EvolutionCandidate", "EvolutionEngine", "EvolutionResult"]
