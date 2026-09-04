"""Frozen forward-testing specifications with paper-only semantics."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
import hashlib
import json
import math
import re
from typing import Any, Mapping, Sequence

from .domain import ResearchQuality, ensure_utc, parse_timestamp, utc_now
from .research_bus import ResearchBusPermissionError, _validate_payload
from .storage import AxiomStore
from .risk import RiskLimits


_PRIVATE_FORWARD_TOKENS = frozenset(
    {
        "credential",
        "private",
        "secret",
        "api_key",
        "password",
        "token",
        "cookie",
        "session",
        "authorization",
        "bearer",
        "oauth",
        "jwt",
        "broker",
        "wallet",
        "execute",
        "execution",
        "live",
        "withdraw",
    }
)


def _validate_private_fields(value: Any, *, path: str = "value", depth: int = 0) -> None:
    if depth > 8:
        raise ValueError(f"forward input nesting exceeds 8 levels: {path}")
    if isinstance(value, Mapping):
        if len(value) > 256:
            raise ValueError(f"forward input mapping is too large: {path}")
        for key, child in value.items():
            normalized = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", str(key))
            normalized = re.sub(r"[^A-Za-z0-9]+", "_", normalized).strip("_").lower()
            compact = normalized.replace("_", "")
            if any(token in normalized or token in compact for token in _PRIVATE_FORWARD_TOKENS):
                raise ValueError(f"forward input contains forbidden private or execution field: {path}.{key}")
            _validate_private_fields(child, path=f"{path}.{key}", depth=depth + 1)
    elif isinstance(value, (list, tuple)):
        if len(value) > 256:
            raise ValueError(f"forward input collection is too large: {path}")
        for index, child in enumerate(value):
            _validate_private_fields(child, path=f"{path}[{index}]", depth=depth + 1)


def _validate_forward_config(config: Mapping[str, Any]) -> None:
    if not isinstance(config, Mapping):
        raise ValueError("forward test config must be a mapping")
    public: dict[str, Any] = {}
    for key, value in config.items():
        normalized = str(key).replace("-", "_").lower()
        if normalized in {"live", "live_execution"}:
            if value is not None and (not isinstance(value, bool) or value):
                raise ValueError("forward tests are paper-only")
            continue
        if normalized == "execution":
            if value not in (None, "paper_only"):
                raise ValueError("forward tests are paper-only")
            continue
        public[str(key)] = value
    try:
        _validate_payload(public)
    except (ResearchBusPermissionError, TypeError, ValueError) as exc:
        raise ValueError("forward test config contains forbidden private or execution fields") from exc
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
    registration_timestamp: datetime | None = None


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
        start = ensure_utc(self.start_timestamp)
        registration = ensure_utc(self.registration_timestamp or start)
        if registration != start:
            raise ValueError("registration_timestamp must equal start_timestamp for a frozen test")
        object.__setattr__(self, "start_timestamp", start)
        object.__setattr__(self, "registration_timestamp", registration)
        _validate_forward_config(self.config)
        try:
            RiskLimits(**dict(self.risk_limits))
        except (TypeError, ValueError) as exc:
            raise ValueError("forward risk_limits are invalid") from exc
        _validate_private_fields(self.risk_limits, path="risk_limits")
        normalized_markets = tuple(dict.fromkeys(str(item).strip() for item in self.allowed_markets if str(item).strip()))
        if len(normalized_markets) > 1000:
            raise ValueError("forward test allowed_markets exceeds 1000 entries")
        object.__setattr__(self, "allowed_markets", normalized_markets)
        object.__setattr__(self, "config", _freeze_json(self.config))
        object.__setattr__(self, "risk_limits", _freeze_json(self.risk_limits))
    def as_record(self) -> dict[str, Any]:
        return {
            "experiment_id": self.experiment_id,
            "strategy_hash": self.strategy_hash,
            "model_hash": self.model_hash,
            "config": _plain(self.config),
            "start_timestamp": self.start_timestamp.isoformat(),
            "registration_timestamp": self.registration_timestamp.isoformat(),
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
        config_document = config.get("strategy_document") if isinstance(config, Mapping) else None
        config_model_document = config.get("model_document") if isinstance(config, Mapping) else None
        if isinstance(config_document, Mapping) and _content_hash(_normalized_strategy_document(config_document)) != _content_hash(_normalized_strategy_document(strategy)):
            raise ValueError("config strategy_document does not match frozen strategy")
        model_source = getattr(model, "document", model)
        if isinstance(config_model_document, Mapping) and _content_hash(config_model_document) != _content_hash(model_source):
            raise ValueError("config model_document does not match frozen model")
        strategy_value = config_document if isinstance(config_document, Mapping) else strategy
        model_value = config_model_document if isinstance(config_model_document, Mapping) else model_source
        strategy_hash = _content_hash(_normalized_strategy_document(strategy_value))
        model_hash = _content_hash(model_value)
        start = ensure_utc(start_timestamp or utc_now())
        normalized_markets = tuple(dict.fromkeys(str(item).strip() for item in allowed_markets if str(item).strip()))
        payload = {
            "strategy_hash": strategy_hash,
            "model_hash": model_hash,
            "config": dict(config or {}),
            "start_timestamp": start.isoformat(),
            "registration_timestamp": start.isoformat(),
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
    def register_forward_test(
        self,
        *,
        strategy: Any,
        model: Any,
        registration_timestamp: datetime | None = None,
        now: datetime | None = None,
        config: Mapping[str, Any] | None = None,
        bankroll: float = 10_000.0,
        allowed_markets: Sequence[str] = (),
        risk_limits: Mapping[str, Any] | None = None,
        experiment_id: str | None = None,
    ) -> ForwardTestSpec:
        """Register a genuinely forward test; historical starts are rejected."""
        current = ensure_utc(now or utc_now())
        registration = ensure_utc(registration_timestamp or current)
        if registration < current:
            raise ValueError("forward registration_timestamp cannot be in the past")
        return self.freeze(
            strategy=strategy,
            model=model,
            config=config,
            start_timestamp=registration,
            bankroll=bankroll,
            allowed_markets=allowed_markets,
            risk_limits=risk_limits,
            experiment_id=experiment_id,
        )


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
                        start_timestamp,
                    )
                )
            return tuple(specs)
        return tuple(sorted(self._specs.values(), key=lambda spec: (spec.start_timestamp, spec.experiment_id)))
    def get(self, experiment_id: str) -> ForwardTestSpec | None:
        identifier = str(experiment_id).strip()
        if not identifier:
            return None
        if self.store is None:
            return self._specs.get(identifier)
        record = self.store.load_forward_test(identifier)
        if record is None:
            return None
        start_timestamp = parse_timestamp(record["start_timestamp"])
        if start_timestamp is None:
            raise ValueError(f"invalid persisted forward-test timestamp: {identifier}")
        return ForwardTestSpec(
            record["experiment_id"],
            record["strategy_hash"],
            record["model_hash"],
            record["config"],
            start_timestamp,
            record["bankroll"],
            tuple(record["allowed_markets"]),
            record["risk_limits"],
            ResearchQuality(record["quality"]),
            start_timestamp,
        )
def _normalized_strategy_document(value: Any) -> Any:
    if isinstance(value, Mapping):
        try:
            from .strategy import load_strategy

            return load_strategy(value).to_dict()
        except Exception:
            return value
    return value



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
