"""Small, deterministic experiment tracking service.

Tracking is intentionally storage-agnostic.  An :class:`ExperimentTracker`
keeps an immutable in-memory index and mirrors records to an optional
AxiomStore-like object when one is supplied.  The adapter only uses methods
that are present, making this module useful before persistence is configured.
"""
from __future__ import annotations
from dataclasses import dataclass, field, replace
from datetime import datetime
import hashlib
import json
import math
from types import MappingProxyType
from typing import Any, Iterable, Mapping

from .domain import ensure_utc, utc_now


def _freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({str(k): _freeze(v) for k, v in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(v) for v in value)
    if isinstance(value, set):
        return frozenset(_freeze(v) for v in value)
    return value


def _plain(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(k): _plain(v) for k, v in value.items()}
    if isinstance(value, (tuple, list, set, frozenset)):
        return [_plain(v) for v in value]
    if isinstance(value, datetime):
        return ensure_utc(value).isoformat()
    if hasattr(value, "value") and not isinstance(value, (str, bytes)):
        return value.value
    return value
def _assert_json(value: Any, path: str) -> None:
    value = _plain(value)
    if value is None or isinstance(value, (str, bool, int)):
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"{path} must contain finite JSON numbers")
        return
    if isinstance(value, Mapping):
        for key, child in value.items():
            if not isinstance(key, str):
                raise ValueError(f"{path} keys must be strings")
            _assert_json(child, f"{path}.{key}")
        return
    if isinstance(value, list):
        for index, child in enumerate(value):
            _assert_json(child, f"{path}[{index}]")
        return
    raise ValueError(f"{path} contains a non-JSON value")




@dataclass(frozen=True, slots=True)
class ExperimentRecord:
    """Complete provenance and outcome for one research experiment."""

    experiment_id: str
    strategy_id: str
    strategy_version: str
    parent_ids: tuple[str, ...] = ()
    provider: str = ""
    instrument: str = ""
    dataset_version: str = ""
    features: tuple[str, ...] = ()
    model_version: str = ""
    executable_prices: Mapping[str, Any] = field(default_factory=dict)
    regime: str | Mapping[str, Any] = ""
    cost_assumptions: Mapping[str, Any] = field(default_factory=dict)
    metrics: Mapping[str, Any] = field(default_factory=dict)
    fitness: float | None = None
    rejected: bool = False
    rejection_reason: str | None = None
    created_at: datetime = field(default_factory=utc_now)
    updated_at: datetime = field(default_factory=utc_now)

    def __post_init__(self) -> None:
        if not self.experiment_id.strip():
            raise ValueError("experiment_id is required")
        if not self.strategy_id.strip():
            raise ValueError("strategy_id is required")
        if not self.strategy_version.strip():
            raise ValueError("strategy_version is required")
        if self.rejected and not (self.rejection_reason or "").strip():
            raise ValueError("rejected records require rejection_reason")
        object.__setattr__(self, "parent_ids", tuple(str(value) for value in self.parent_ids))
        object.__setattr__(self, "features", tuple(str(value) for value in self.features))
        object.__setattr__(self, "executable_prices", _freeze(self.executable_prices))
        object.__setattr__(self, "regime", _freeze(self.regime))
        object.__setattr__(self, "cost_assumptions", _freeze(self.cost_assumptions))
        object.__setattr__(self, "metrics", _freeze(self.metrics))
        for name in ("executable_prices", "regime", "cost_assumptions", "metrics"):
            _assert_json(getattr(self, name), name)
        object.__setattr__(self, "created_at", ensure_utc(self.created_at))
        object.__setattr__(self, "updated_at", ensure_utc(self.updated_at))
        if self.fitness is not None:
            fitness = float(self.fitness)
            if not math.isfinite(fitness):
                raise ValueError("fitness must be finite")
            object.__setattr__(self, "fitness", fitness)

    @property
    def version(self) -> str:
        return self.strategy_version

    @property
    def parents(self) -> tuple[str, ...]:
        return self.parent_ids

    def as_record(self) -> dict[str, Any]:
        return self.to_record()

    def to_record(self) -> dict[str, Any]:
        return {
            "experiment_id": self.experiment_id,
            "strategy_id": self.strategy_id,
            "strategy_version": self.strategy_version,
            "parent_ids": list(self.parent_ids),
            "provider": self.provider,
            "instrument": self.instrument,
            "dataset_version": self.dataset_version,
            "features": list(self.features),
            "model_version": self.model_version,
            "executable_prices": _plain(self.executable_prices),
            "regime": _plain(self.regime),
            "cost_assumptions": _plain(self.cost_assumptions),
            "metrics": _plain(self.metrics),
            "fitness": self.fitness,
            "rejected": self.rejected,
            "rejection_reason": self.rejection_reason,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }


# Names used by integrations and older callers.
Experiment = ExperimentRecord
TrackedExperiment = ExperimentRecord


def _stable_id(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(_plain(payload), sort_keys=True, separators=(",", ":"), default=repr)
    return "exp-" + hashlib.sha256(encoded.encode("utf-8")).hexdigest()[:24]
def _record_identity(record: ExperimentRecord) -> str:
    payload = record.to_record()
    payload.pop("created_at", None)
    payload.pop("updated_at", None)
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), default=repr)



class ExperimentTracker:
    """Track experiments and optionally persist them through an AxiomStore.

    The service never mutates a record after publication.  Rejection creates a
    replacement value while preserving the original creation timestamp.
    """

    def __init__(self, store: Any | None = None) -> None:
        self.store = store
        self._records: dict[str, ExperimentRecord] = {}

    @property
    def records(self) -> tuple[ExperimentRecord, ...]:
        return tuple(sorted(self._records.values(), key=lambda item: (item.created_at, item.experiment_id)))

    def _persist(self, record: ExperimentRecord) -> None:
        if self.store is None:
            return
        payload = record.to_record()
        save_experiment = getattr(self.store, "save_experiment", None)
        if callable(save_experiment):
            try:
                save_experiment(record.experiment_id, payload, strategy_id=record.strategy_id)
            except ValueError:
                loader = getattr(self.store, "load_experiment", None)
                existing = loader(record.experiment_id) if callable(loader) else None
                if existing != payload:
                    raise
            return
        # Keep compatibility with lightweight store adapters.
        for method_name, args in (
            ("record_experiment", (record,)),
            ("put_experiment", (record.experiment_id, payload)),
            ("put", ("experiments", record.experiment_id, payload)),
            ("save", ("experiments", record.experiment_id, payload)),
            ("insert", ("experiments", payload)),
        ):
            method = getattr(self.store, method_name, None)
            if not callable(method):
                continue
            try:
                method(*args)
            except TypeError:
                method(record.experiment_id, payload)
            return

    def track(
        self,
        strategy_id: str,
        strategy_version: str | None = None,
        *,
        version: str | None = None,
        parent_ids: Iterable[str] = (),
        parents: Iterable[str] | None = None,
        provider: str = "",
        instrument: str = "",
        dataset_version: str = "",
        features: Iterable[str] = (),
        model_version: str = "",
        executable_prices: Mapping[str, Any] | None = None,
        regime: str | Mapping[str, Any] = "",
        cost_assumptions: Mapping[str, Any] | None = None,
        metrics: Mapping[str, Any] | None = None,
        fitness: float | None = None,
        rejected: bool = False,
        rejection_reason: str | None = None,
        experiment_id: str | None = None,
        created_at: datetime | None = None,
    ) -> ExperimentRecord:
        strategy_version = strategy_version if strategy_version is not None else version
        if not strategy_version:
            raise ValueError("strategy_version is required")
        parent_values = tuple(str(value) for value in (parents if parents is not None else parent_ids))
        feature_values = tuple(str(value) for value in features)
        metric_values = dict(metrics or {})
        if fitness is None and metric_values.get("fitness") is not None:
            fitness = float(metric_values["fitness"])
        stamp = ensure_utc(created_at or utc_now())
        identity = {
            "strategy_id": strategy_id,
            "strategy_version": strategy_version,
            "parent_ids": parent_values,
            "provider": provider,
            "instrument": instrument,
            "dataset_version": dataset_version,
            "features": feature_values,
            "model_version": model_version,
            "executable_prices": executable_prices or {},
            "regime": regime,
            "cost_assumptions": cost_assumptions or {},
            "metrics": metric_values,
            "fitness": fitness,
        }
        record = ExperimentRecord(
            experiment_id=experiment_id or _stable_id(identity),
            strategy_id=strategy_id,
            strategy_version=strategy_version,
            parent_ids=parent_values,
            provider=provider,
            instrument=instrument,
            dataset_version=dataset_version,
            features=feature_values,
            model_version=model_version,
            executable_prices=executable_prices or {},
            regime=regime,
            cost_assumptions=cost_assumptions or {},
            metrics=metric_values,
            fitness=fitness,
            rejected=rejected,
            rejection_reason=rejection_reason,
            created_at=stamp,
            updated_at=stamp,
        )
        existing = self._records.get(record.experiment_id)
        if existing is not None:
            if _record_identity(existing) == _record_identity(record):
                return existing
            raise ValueError(f"experiment already exists with different content: {record.experiment_id}")
        # Persist before publishing to the in-memory index so a storage
        # rejection cannot leave two divergent sources of truth.
        self._persist(record)
        self._records[record.experiment_id] = record
        return record

    def reject(self, experiment_id: str, reason: str) -> ExperimentRecord:
        record = self._records.get(experiment_id)
        if record is None:
            raise KeyError(experiment_id)
        reason = reason.strip()
        if not reason:
            raise ValueError("rejection reason is required")
        if record.rejected:
            return record
        suffix = hashlib.sha256(reason.encode("utf-8")).hexdigest()[:12]
        rejection_id = f"{record.experiment_id}:rejected:{suffix}"
        existing = self._records.get(rejection_id)
        if existing is not None:
            return existing
        stamp = utc_now()
        replacement = replace(
            record,
            experiment_id=rejection_id,
            parent_ids=tuple(dict.fromkeys((*record.parent_ids, record.experiment_id))),
            rejected=True,
            rejection_reason=reason,
            created_at=record.created_at,
            updated_at=stamp,
        )
        self._persist(replacement)
        self._records[replacement.experiment_id] = replacement
        return replacement

    def list(self, *, include_rejected: bool = True, strategy_id: str | None = None) -> tuple[ExperimentRecord, ...]:
        return tuple(
            item
            for item in self.records
            if (include_rejected or not item.rejected) and (strategy_id is None or item.strategy_id == strategy_id)
        )

    def best(self, *, strategy_id: str | None = None) -> ExperimentRecord | None:
        candidates = self.list(include_rejected=False, strategy_id=strategy_id)
        return max(candidates, key=lambda item: (item.fitness if item.fitness is not None else float("-inf"), item.created_at), default=None)

    def as_records(self) -> list[dict[str, Any]]:
        return [item.to_record() for item in self.records]


TrackingService = ExperimentTracker
