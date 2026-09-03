"""Leakage-safe dataset evaluation primitives.

The helpers in this module deliberately work with ordinary mappings and the
canonical domain dataclasses.  A split is a value object: rows are copied into
ordered tuples and all boundary/version metadata is retained, so callers
cannot accidentally move a validation or locked holdout row into training.
"""
from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import math
from types import MappingProxyType
from typing import Any, Iterable, Mapping, Sequence

from .domain import ensure_utc, parse_timestamp


_EPSILON = 1e-12


def _freeze(value: Any) -> Any:
    """Recursively freeze containers used in public value objects."""
    if isinstance(value, Mapping):
        return MappingProxyType({str(k): _freeze(v) for k, v in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(v) for v in value)
    if isinstance(value, set):
        return frozenset(_freeze(v) for v in value)
    return value


def _immutable_row(value: Any) -> Any:
    if isinstance(value, (Mapping, list, tuple, set)):
        return _freeze(value)
    try:
        return deepcopy(value)
    except Exception:
        return value


def _timestamp(row: Any, key: str = "timestamp") -> datetime:
    if isinstance(row, Mapping):
        value = row.get(key)
    else:
        value = getattr(row, key, None)
    if value is None:
        raise ValueError(f"dataset row has no {key!r} timestamp")
    if isinstance(value, date) and not isinstance(value, datetime):
        value = datetime.combine(value, datetime.min.time(), tzinfo=timezone.utc)
    timestamp = parse_timestamp(value)
    if timestamp is None:
        raise TypeError(f"dataset timestamp is invalid: {value!r}")
    return timestamp


def _version(row: Any) -> str | None:
    if isinstance(row, Mapping):
        value = row.get("dataset_version", row.get("version"))
    else:
        value = getattr(row, "dataset_version", getattr(row, "version", None))
    return None if value is None else str(value)


def dataset_version(records: Iterable[Any], *, explicit: str | None = None, timestamp_key: str = "timestamp") -> str:
    """Return an explicit version or a stable content-derived version.

    Providers should pass their immutable dataset version.  The content hash
    fallback is useful for synthetic/offline data and changes whenever a row,
    its ordering, or its timestamp changes.
    """
    if explicit not in (None, ""):
        return str(explicit)
    normalized = []
    for index, row in enumerate(records):
        try:
            stamp = _timestamp(row, timestamp_key).isoformat()
        except (TypeError, ValueError):
            stamp = ""
        if isinstance(row, Mapping):
            payload = dict(row)
        elif hasattr(row, "__dataclass_fields__"):
            payload = {name: getattr(row, name) for name in row.__dataclass_fields__}
        else:
            payload = repr(row)
        normalized.append((index, stamp, payload))

    def default(value: Any) -> str:
        if isinstance(value, datetime):
            return ensure_utc(value).isoformat()
        if isinstance(value, (set, frozenset)):
            return repr(sorted(value, key=repr))
        if hasattr(value, "value") and not isinstance(value, (str, bytes)):
            return str(value.value)
        return repr(value)

    encoded = json.dumps(normalized, sort_keys=True, default=default, separators=(",", ":"))
    return "sha256:" + hashlib.sha256(encoded.encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class DatasetSplit:
    """Immutable train/validation/locked-holdout partition."""

    dataset_version: str
    train: tuple[Any, ...]
    validation: tuple[Any, ...]
    holdout: tuple[Any, ...]
    train_start: datetime | None
    train_end: datetime
    validation_end: datetime
    holdout_end: datetime | None
    timestamp_key: str = "timestamp"
    def __post_init__(self) -> None:
        object.__setattr__(self, "train", tuple(_immutable_row(row) for row in self.train))
        object.__setattr__(self, "validation", tuple(_immutable_row(row) for row in self.validation))
        object.__setattr__(self, "holdout", tuple(_immutable_row(row) for row in self.holdout))
        if self.train_start is not None:
            object.__setattr__(self, "train_start", ensure_utc(self.train_start))
        object.__setattr__(self, "train_end", ensure_utc(self.train_end))
        object.__setattr__(self, "validation_end", ensure_utc(self.validation_end))
        if self.holdout_end is not None:
            object.__setattr__(self, "holdout_end", ensure_utc(self.holdout_end))


    @property
    def locked_holdout(self) -> tuple[Any, ...]:
        return self.holdout

    @property
    def version(self) -> str:
        return self.dataset_version

    @property
    def boundaries(self) -> Mapping[str, datetime | None]:
        return MappingProxyType(
            {
                "train_start": self.train_start,
                "train_end": self.train_end,
                "validation_end": self.validation_end,
                "holdout_end": self.holdout_end,
            }
        )

    def as_record(self) -> dict[str, Any]:
        return {
            "dataset_version": self.dataset_version,
            "train_count": len(self.train),
            "validation_count": len(self.validation),
            "holdout_count": len(self.holdout),
            "train_start": self.train_start.isoformat() if self.train_start else None,
            "train_end": self.train_end.isoformat(),
            "validation_end": self.validation_end.isoformat(),
            "holdout_end": self.holdout_end.isoformat() if self.holdout_end else None,
        }


# A descriptive alias used by integrations that call the final partition a lock.
DatasetPartition = DatasetSplit


def split_dataset(
    records: Iterable[Any],
    train_end: datetime,
    validation_end: datetime,
    holdout_end: datetime | None = None,
    *,
    dataset_version: str | None = None,
    version: str | None = None,
    timestamp_key: str = "timestamp",
    require_nonempty: bool = False,
) -> DatasetSplit:
    """Split rows by half-open UTC timestamp intervals.

    Training contains ``[start, train_end)``, validation contains
    ``[train_end, validation_end)``, and the locked holdout contains
    ``[validation_end, holdout_end)`` (or all later rows when ``holdout_end``
    is omitted).  Rows are sorted into a new tuple; the input is never changed.
    A row carrying an explicit ``dataset_version`` must match the split
    version, preventing accidental mixing of provider revisions.
    """
    train_end = ensure_utc(train_end)
    validation_end = ensure_utc(validation_end)
    if validation_end <= train_end:
        raise ValueError("validation_end must be after train_end")
    if holdout_end is not None:
        holdout_end = ensure_utc(holdout_end)
        if holdout_end <= validation_end:
            raise ValueError("holdout_end must be after validation_end")

    rows = [_immutable_row(row) for row in records]
    # Python's stable sort preserves provider order for equal timestamps;
    # adding repr(row) here would make object-addresses affect reproducibility.
    rows.sort(key=lambda row: _timestamp(row, timestamp_key))
    resolved_version = dataset_version if dataset_version is not None else version
    resolved_version = dataset_version_for_rows(rows, explicit=resolved_version, timestamp_key=timestamp_key)
    train: list[Any] = []
    validation: list[Any] = []
    holdout: list[Any] = []
    for row in rows:
        row_version = _version(row)
        if row_version is not None and row_version != resolved_version:
            raise ValueError(f"row dataset version {row_version!r} does not match {resolved_version!r}")
        stamp = _timestamp(row, timestamp_key)
        if stamp < train_end:
            train.append(row)
        elif stamp < validation_end:
            validation.append(row)
        elif holdout_end is None or stamp < holdout_end:
            holdout.append(row)
    if require_nonempty and (not train or not validation or not holdout):
        raise ValueError("train, validation, and locked holdout must all be non-empty")
    start = _timestamp(rows[0], timestamp_key) if rows else None
    return DatasetSplit(
        dataset_version=resolved_version,
        train=tuple(train),
        validation=tuple(validation),
        holdout=tuple(holdout),
        train_start=start,
        train_end=train_end,
        validation_end=validation_end,
        holdout_end=holdout_end,
        timestamp_key=timestamp_key,
    )

def dataset_version_for_rows(
    records: Iterable[Any], *, explicit: str | None = None, timestamp_key: str = "timestamp"
) -> str:
    rows = list(records)
    if explicit not in (None, ""):
        return str(explicit)
    versions = {_version(row) for row in rows}
    versions.discard(None)
    if len(versions) > 1:
        raise ValueError("dataset rows contain multiple dataset versions")
    if versions:
        return next(iter(versions))
    return dataset_version(rows, timestamp_key=timestamp_key)


split_by_timestamp = split_dataset


def _period(value: timedelta | int | float, *, name: str) -> timedelta | int:
    if isinstance(value, timedelta):
        if value <= timedelta(0):
            raise ValueError(f"{name} must be positive")
        return value
    if isinstance(value, (int, float)) and value > 0 and float(value).is_integer():
        return int(value)
    raise ValueError(f"{name} must be a positive timedelta or integer row count")


def walk_forward_splits(
    records: Iterable[Any],
    train_window: timedelta | int,
    validation_window: timedelta | int,
    holdout_window: timedelta | int,
    step: timedelta | int | None = None,
    *,
    dataset_version: str | None = None,
    version: str | None = None,
    timestamp_key: str = "timestamp",
    start: datetime | None = None,
    end: datetime | None = None,
) -> tuple[DatasetSplit, ...]:
    """Build chronological walk-forward windows.

    Durations create timestamp windows; integer windows create row-count
    windows.  Every returned split has an explicit locked holdout boundary.
    The default step is one validation+holdout period (duration mode) or one
    holdout row window (count mode).
    """
    tw, vw, hw = (_period(train_window, name="train_window"), _period(validation_window, name="validation_window"), _period(holdout_window, name="holdout_window"))
    if step is None:
        step_value: timedelta | int = (vw + hw) if isinstance(vw, timedelta) else hw
    else:
        step_value = _period(step, name="step")
        if isinstance(step_value, timedelta) != isinstance(tw, timedelta):
            raise ValueError("step must use the same units as the window sizes")
    rows = sorted((_immutable_row(row) for row in records), key=lambda row: _timestamp(row, timestamp_key))
    if not rows:
        return ()
    resolved_version = dataset_version if dataset_version is not None else version
    resolved_version = dataset_version_for_rows(rows, explicit=resolved_version, timestamp_key=timestamp_key)
    for row in rows:
        row_version = _version(row)
        if row_version is not None and row_version != resolved_version:
            raise ValueError(f"row dataset version {row_version!r} does not match {resolved_version!r}")
    output: list[DatasetSplit] = []
    if isinstance(tw, int):
        if not all(isinstance(value, int) for value in (vw, hw, step_value)):
            raise ValueError("all row-count windows must be integers")
        first = 0
        while first + tw + vw + hw <= len(rows):
            selected = rows[first : first + tw + vw + hw]
            train_rows = tuple(selected[:tw])
            validation_rows = tuple(selected[tw : tw + vw])
            holdout_rows = tuple(selected[tw + vw :])
            train_end = _timestamp(validation_rows[0], timestamp_key)
            validation_end = _timestamp(holdout_rows[0], timestamp_key)
            holdout_end = _timestamp(holdout_rows[-1], timestamp_key) + timedelta(microseconds=1)
            output.append(
                DatasetSplit(resolved_version, train_rows, validation_rows, holdout_rows, _timestamp(train_rows[0], timestamp_key), train_end, validation_end, holdout_end, timestamp_key)
            )
            first += int(step_value)
        return tuple(output)

    first_stamp = _timestamp(rows[0], timestamp_key) if start is None else ensure_utc(start)
    final_stamp = None if end is None else ensure_utc(end)
    cursor = first_stamp
    while True:
        train_end = cursor + tw
        validation_end = train_end + vw
        holdout_end = validation_end + hw
        if final_stamp is not None and holdout_end > final_stamp:
            break
        selected = [row for row in rows if cursor <= _timestamp(row, timestamp_key) < holdout_end]
        train_rows = tuple(row for row in selected if _timestamp(row, timestamp_key) < train_end)
        validation_rows = tuple(row for row in selected if train_end <= _timestamp(row, timestamp_key) < validation_end)
        holdout_rows = tuple(row for row in selected if validation_end <= _timestamp(row, timestamp_key) < holdout_end)
        if train_rows and validation_rows and holdout_rows:
            output.append(DatasetSplit(resolved_version, train_rows, validation_rows, holdout_rows, cursor, train_end, validation_end, holdout_end, timestamp_key))
        # A duration window that no longer overlaps the data cannot produce
        # another split; still permit sparse rows inside a future range.
        if cursor >= _timestamp(rows[-1], timestamp_key):
            break
        cursor += step_value
    return tuple(output)


walk_forward = walk_forward_splits


def _score(value: Any, metric: str) -> float:
    if isinstance(value, Mapping):
        value = value.get(metric, value.get("fitness", value.get("score", 0.0)))
    try:
        result = float(value)
    except (TypeError, ValueError):
        return 0.0
    return result if math.isfinite(result) else 0.0

def degradation_penalty(
    train: Any,
    validation: Any,
    holdout: Any,
    *,
    metric: str = "fitness",
    direction: str = "max",
    validation_weight: float = 1.0,
    holdout_weight: float = 1.0,
    overfit_weight: float = 1.0,
) -> float:
    """Return bounded, additive degradation components.

    The denominator is a stable score scale rather than the validation score
    itself.  A validation score near zero therefore cannot turn a modest
    absolute miss into an unbounded penalty.
    """
    if direction not in {"max", "min"}:
        raise ValueError("direction must be 'max' or 'min'")
    weights = (float(validation_weight), float(holdout_weight), float(overfit_weight))
    if not all(math.isfinite(weight) and weight >= 0 for weight in weights):
        raise ValueError("penalty weights must be finite and non-negative")
    train_score, validation_score, holdout_score = (_score(value, metric) for value in (train, validation, holdout))
    scale = max(1.0, abs(train_score), abs(validation_score), abs(holdout_score))
    if direction == "max":
        train_validation = max(0.0, train_score - validation_score) / scale
        validation_holdout = max(0.0, validation_score - holdout_score) / scale
    else:
        train_validation = max(0.0, validation_score - train_score) / scale
        validation_holdout = max(0.0, holdout_score - validation_score) / scale
    return (
        weights[0] * train_validation
        + weights[1] * validation_holdout
        + weights[2] * train_validation
    )


@dataclass(frozen=True, slots=True)
class EvaluationResult:
    train_score: float
    validation_score: float
    holdout_score: float
    degradation: float
    overfit_penalty: float
    fitness: float
    metric: str = "fitness"
    holdout_degradation: float = 0.0

    @property
    def penalty(self) -> float:
        return self.degradation + self.overfit_penalty


def evaluate_scores(
    train: Any,
    validation: Any,
    holdout: Any,
    *,
    metric: str = "fitness",
    direction: str = "max",
    degradation_weight: float = 1.0,
    overfit_weight: float = 1.0,
) -> EvaluationResult:
    """Evaluate train/validation and report holdout without selecting on it.

    ``fitness`` is a validation-only selection score.  The locked holdout is
    retained for post-selection reporting through ``holdout_score`` and
    ``holdout_degradation``.
    """
    train_score, validation_score, holdout_score = (_score(value, metric) for value in (train, validation, holdout))
    degradation = degradation_penalty(
        train_score,
        validation_score,
        validation_score,
        direction=direction,
        validation_weight=degradation_weight,
        holdout_weight=0.0,
        overfit_weight=0.0,
    )
    overfit = degradation_penalty(
        train_score,
        validation_score,
        validation_score,
        direction=direction,
        validation_weight=0.0,
        holdout_weight=0.0,
        overfit_weight=overfit_weight,
    )
    holdout_degradation = degradation_penalty(
        train_score,
        validation_score,
        holdout_score,
        direction=direction,
        validation_weight=0.0,
        holdout_weight=degradation_weight,
        overfit_weight=0.0,
    )
    fitness = (
        validation_score - degradation - overfit
        if direction == "max"
        else -validation_score - degradation - overfit
    )
    return EvaluationResult(
        train_score,
        validation_score,
        holdout_score,
        degradation,
        overfit,
        fitness,
        metric,
        holdout_degradation,
    )


apply_penalties = evaluate_scores
