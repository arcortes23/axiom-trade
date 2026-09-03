"""Frozen forward-testing specifications with paper-only semantics."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
import hashlib
import json
import math
from typing import Any, Mapping, Sequence

from .domain import ResearchQuality, ensure_utc, parse_timestamp, utc_now
from .storage import AxiomStore


@dataclass(frozen=True, slots=True)
class ForwardTestSpec:
    experiment_id: str
    strategy_hash: str
    model_hash: str
    config: Mapping[str, Any] = field(default_factory=dict)
    start_timestamp: datetime = field(default_factory=utc_now)
    bankroll: float = 10_000.0
    allowed_markets: tuple[str, ...] = ()
    risk_limits: Mapping[str, Any] = field(default_factory=dict)
    quality: ResearchQuality = ResearchQuality.PAPER_FORWARD
    def __post_init__(self) -> None:
        bankroll = float(self.bankroll)
        if (
            not str(self.experiment_id).strip()
            or not str(self.strategy_hash).strip()
            or not str(self.model_hash).strip()
        ):
            raise ValueError("forward specs require experiment, strategy, and model identifiers")
        if not math.isfinite(bankroll) or bankroll <= 0:
            raise ValueError("forward bankroll must be finite and positive")
        if not isinstance(self.quality, ResearchQuality):
            object.__setattr__(self, "quality", ResearchQuality(str(self.quality)))
        if self.quality is not ResearchQuality.PAPER_FORWARD:
            raise ValueError("forward registry accepts PAPER_FORWARD specs only")
        object.__setattr__(self, "start_timestamp", ensure_utc(self.start_timestamp))
        object.__setattr__(self, "bankroll", bankroll)
        object.__setattr__(self, "allowed_markets", tuple(dict.fromkeys(str(item).strip() for item in self.allowed_markets if str(item).strip())))
        object.__setattr__(self, "config", _freeze_json(self.config))
        object.__setattr__(self, "risk_limits", _freeze_json(self.risk_limits))

    def as_record(self) -> dict[str, Any]:
        return {
            "experiment_id": self.experiment_id,
            "strategy_hash": self.strategy_hash,
            "model_hash": self.model_hash,
            "config": _plain(self.config),
            "start_timestamp": self.start_timestamp.isoformat(),
            "bankroll": self.bankroll,
            "allowed_markets": list(self.allowed_markets),
            "risk_limits": _plain(self.risk_limits),
            "quality": self.quality.value,
        }


class ForwardTestRegistry:
    """Append-only registry; it never starts a trader or submits an order."""

    def __init__(self, store: AxiomStore | None = None) -> None:
        self.store = store
        self._specs: dict[str, ForwardTestSpec] = {}

    def freeze(
        self,
        *,
        strategy: Any,
        model: Any,
        config: Mapping[str, Any] | None = None,
        start_timestamp: datetime | None = None,
        bankroll: float = 10_000.0,
        allowed_markets: Sequence[str] = (),
        risk_limits: Mapping[str, Any] | None = None,
        experiment_id: str | None = None,
    ) -> ForwardTestSpec:
        strategy_hash = _content_hash(strategy)
        model_hash = _content_hash(model)
        start = ensure_utc(start_timestamp or utc_now())
        normalized_markets = tuple(dict.fromkeys(str(item).strip() for item in allowed_markets if str(item).strip()))
        payload = {
            "strategy_hash": strategy_hash,
            "model_hash": model_hash,
            "config": dict(config or {}),
            "start_timestamp": start.isoformat(),
            "bankroll": float(bankroll),
            "allowed_markets": list(normalized_markets),
            "risk_limits": dict(risk_limits or {}),
            "quality": ResearchQuality.PAPER_FORWARD.value,
        }
        identifier = experiment_id or "forward-" + hashlib.sha256(_canonical(payload).encode("utf-8")).hexdigest()[:24]
        spec = ForwardTestSpec(identifier, strategy_hash, model_hash, payload["config"], start, bankroll, normalized_markets, payload["risk_limits"])
        existing = self._specs.get(identifier)
        if existing is not None:
            if existing.as_record() != spec.as_record():
                raise ValueError(f"forward test is frozen: {identifier}")
            return existing
        if self.store is not None:
            self.store.save_forward_test(identifier, spec.as_record())
        self._specs[identifier] = spec
        return spec

    def list(self) -> tuple[ForwardTestSpec, ...]:
        if self.store is not None:
            records = self.store.load_forward_tests()
            specs = []
            for record in records:
                start_timestamp = parse_timestamp(record["start_timestamp"])
                if start_timestamp is None:
                    raise ValueError(f"invalid persisted forward-test timestamp: {record['experiment_id']}")
                specs.append(
                    ForwardTestSpec(
                        record["experiment_id"],
                        record["strategy_hash"],
                        record["model_hash"],
                        record["config"],
                        start_timestamp,
                        record["bankroll"],
                        tuple(record["allowed_markets"]),
                        record["risk_limits"],
                        ResearchQuality(record["quality"]),
                    )
                )
            return tuple(specs)
        return tuple(sorted(self._specs.values(), key=lambda spec: (spec.start_timestamp, spec.experiment_id)))


def _content_hash(value: Any) -> str:
    return "sha256:" + hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _canonical(value: Any) -> str:
    def convert(item: Any) -> Any:
        if isinstance(item, Mapping):
            return {str(key): convert(child) for key, child in sorted(item.items(), key=lambda pair: str(pair[0]))}
        if isinstance(item, (list, tuple)):
            return [convert(child) for child in item]
        if isinstance(item, datetime):
            return ensure_utc(item).isoformat()
        if hasattr(item, "value") and not isinstance(item, (str, bytes)):
            return item.value
        if hasattr(item, "to_dict") and callable(item.to_dict):
            return convert(item.to_dict())
        return item
    return json.dumps(convert(value), sort_keys=True, separators=(",", ":"), allow_nan=False, default=repr)


def _freeze_json(value: Mapping[str, Any]) -> Mapping[str, Any]:
    from types import MappingProxyType

    def freeze(item: Any) -> Any:
        if isinstance(item, Mapping):
            return MappingProxyType({str(key): freeze(child) for key, child in item.items()})
        if isinstance(item, list):
            return tuple(freeze(child) for child in item)
        return item

    return freeze(json.loads(_canonical(value)))

def _plain(value: Any) -> Any:
    return json.loads(_canonical(value))


__all__ = ["ForwardTestRegistry", "ForwardTestSpec"]
