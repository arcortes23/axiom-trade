"""Hermes research-message contracts.

Hermes is deliberately a research boundary.  It can publish hypotheses and
reports and receive a *schema* describing a candidate, but it has no methods
for accounts, orders, risk state, or historical data mutation.  This makes it
safe to expose to an untrusted research process.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
import json
import math
from types import MappingProxyType
from typing import Any, Iterable, Mapping

from .domain import ensure_utc, utc_now


class HermesValidationError(ValueError):
    """Raised when a Hermes message does not satisfy its contract."""


def _freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({str(k): _freeze(v) for k, v in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(v) for v in value)
    if isinstance(value, set):
        return frozenset(_freeze(v) for v in value)
    return value
def _json_safe(value: Any, path: str) -> None:
    if value is None or isinstance(value, (str, bool, int)):
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise HermesValidationError(f"{path} must contain finite JSON numbers")
        return
    if isinstance(value, Mapping):
        for key, child in value.items():
            if not isinstance(key, str):
                raise HermesValidationError(f"{path} keys must be strings")
            _json_safe(child, f"{path}.{key}")
        return
    if isinstance(value, (list, tuple)):
        for index, child in enumerate(value):
            _json_safe(child, f"{path}[{index}]")
        return
    raise HermesValidationError(f"{path} contains a non-JSON value")



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


def _required(value: str, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise HermesValidationError(f"{name} is required")
    return value.strip()


def _mapping(value: Mapping[str, Any] | None, name: str) -> Mapping[str, Any]:
    if value is None:
        return MappingProxyType({})
    if not isinstance(value, Mapping):
        raise HermesValidationError(f"{name} must be a mapping")
    if any(not isinstance(key, str) or not key.strip() for key in value):
        raise HermesValidationError(f"{name} keys must be non-empty strings")
    _json_safe(value, name)
    return _freeze(value)


@dataclass(frozen=True, slots=True)
class HypothesisMessage:
    hypothesis_id: str
    statement: str
    author: str = ""
    strategy_id: str | None = None
    parent_ids: tuple[str, ...] = ()
    assumptions: Mapping[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=utc_now)
    schema_version: str = "1"
    message_type: str = "hypothesis"

    def __post_init__(self) -> None:
        object.__setattr__(self, "hypothesis_id", _required(self.hypothesis_id, "hypothesis_id"))
        object.__setattr__(self, "statement", _required(self.statement, "statement"))
        if self.strategy_id is not None:
            object.__setattr__(self, "strategy_id", _required(self.strategy_id, "strategy_id"))
        object.__setattr__(self, "parent_ids", tuple(_required(str(value), "parent_id") for value in self.parent_ids))
        object.__setattr__(self, "assumptions", _mapping(self.assumptions, "assumptions"))
        object.__setattr__(self, "created_at", ensure_utc(self.created_at))
        if self.message_type != "hypothesis":
            raise HermesValidationError("invalid hypothesis message_type")

    def to_record(self) -> dict[str, Any]:
        return {
            "message_type": self.message_type,
            "schema_version": self.schema_version,
            "hypothesis_id": self.hypothesis_id,
            "statement": self.statement,
            "author": self.author,
            "strategy_id": self.strategy_id,
            "parent_ids": list(self.parent_ids),
            "assumptions": _plain(self.assumptions),
            "created_at": self.created_at.isoformat(),
        }


@dataclass(frozen=True, slots=True)
class ReportMessage:
    report_id: str
    hypothesis_id: str
    summary: str
    metrics: Mapping[str, Any] = field(default_factory=dict)
    evidence: tuple[str, ...] = ()
    conclusion: str = ""
    author: str = ""
    created_at: datetime = field(default_factory=utc_now)
    schema_version: str = "1"
    message_type: str = "report"

    def __post_init__(self) -> None:
        for value, name in ((self.report_id, "report_id"), (self.hypothesis_id, "hypothesis_id"), (self.summary, "summary")):
            _required(value, name)
        object.__setattr__(self, "report_id", self.report_id.strip())
        object.__setattr__(self, "hypothesis_id", self.hypothesis_id.strip())
        object.__setattr__(self, "summary", self.summary.strip())
        object.__setattr__(self, "metrics", _mapping(self.metrics, "metrics"))
        object.__setattr__(self, "evidence", tuple(str(value) for value in self.evidence))
        object.__setattr__(self, "created_at", ensure_utc(self.created_at))
        if self.message_type != "report":
            raise HermesValidationError("invalid report message_type")

    def to_record(self) -> dict[str, Any]:
        return {
            "message_type": self.message_type,
            "schema_version": self.schema_version,
            "report_id": self.report_id,
            "hypothesis_id": self.hypothesis_id,
            "summary": self.summary,
            "metrics": _plain(self.metrics),
            "evidence": list(self.evidence),
            "conclusion": self.conclusion,
            "author": self.author,
            "created_at": self.created_at.isoformat(),
        }


_FORBIDDEN_SCHEMA_NAMES = frozenset({
    "account", "accounts", "balance", "wallet", "order", "orders", "execute", "execution",
    "risk_state", "risk", "history", "historical", "trades", "positions", "credentials", "secret",
})


def _validate_schema(value: Mapping[str, Any]) -> Mapping[str, Any]:
    schema = _mapping(value, "schema")

    def visit(node: Any, path: str = "schema") -> None:
        if isinstance(node, Mapping):
            for key, child in node.items():
                normalized = str(key).lower().replace("-", "_")
                if normalized in _FORBIDDEN_SCHEMA_NAMES or any(part in normalized for part in ("private_key", "api_key", "password", "token")):
                    raise HermesValidationError(f"candidate schema contains forbidden field {path}.{key}")
                visit(child, f"{path}.{key}")
        elif isinstance(node, (tuple, list)):
            for index, child in enumerate(node):
                visit(child, f"{path}[{index}]")
        elif callable(node):
            raise HermesValidationError(f"candidate schema cannot contain callables ({path})")

    visit(schema)
    return schema


@dataclass(frozen=True, slots=True)
class CandidateMessage:
    candidate_id: str
    strategy_id: str
    strategy_version: str
    schema: Mapping[str, Any]
    hypothesis_id: str | None = None
    parent_ids: tuple[str, ...] = ()
    provider: str = ""
    instrument: str = ""
    dataset_version: str = ""
    features: tuple[str, ...] = ()
    model_version: str = ""
    executable_prices: Mapping[str, Any] = field(default_factory=dict)
    regime: str | Mapping[str, Any] = ""
    cost_assumptions: Mapping[str, Any] = field(default_factory=dict)
    fitness: float | None = None
    created_at: datetime = field(default_factory=utc_now)
    schema_version: str = "1"
    message_type: str = "candidate"

    def __post_init__(self) -> None:
        for value, name in ((self.candidate_id, "candidate_id"), (self.strategy_id, "strategy_id"), (self.strategy_version, "strategy_version")):
            _required(value, name)
        object.__setattr__(self, "candidate_id", self.candidate_id.strip())
        object.__setattr__(self, "strategy_id", self.strategy_id.strip())
        object.__setattr__(self, "strategy_version", self.strategy_version.strip())
        if self.hypothesis_id is not None:
            object.__setattr__(self, "hypothesis_id", _required(self.hypothesis_id, "hypothesis_id"))
        object.__setattr__(self, "parent_ids", tuple(str(value) for value in self.parent_ids))
        object.__setattr__(self, "features", tuple(str(value) for value in self.features))
        object.__setattr__(self, "schema", _validate_schema(self.schema))
        object.__setattr__(self, "executable_prices", _mapping(self.executable_prices, "executable_prices"))
        if isinstance(self.regime, Mapping):
            object.__setattr__(self, "regime", _mapping(self.regime, "regime"))
        elif isinstance(self.regime, str):
            object.__setattr__(self, "regime", self.regime)
        else:
            raise HermesValidationError("regime must be a string or mapping")
        object.__setattr__(self, "cost_assumptions", _mapping(self.cost_assumptions, "cost_assumptions"))
        object.__setattr__(self, "created_at", ensure_utc(self.created_at))
        if self.fitness is not None:
            fitness = float(self.fitness)
            if not math.isfinite(fitness):
                raise HermesValidationError("fitness must be finite")
            object.__setattr__(self, "fitness", fitness)
        if self.message_type != "candidate":
            raise HermesValidationError("invalid candidate message_type")

    def to_record(self) -> dict[str, Any]:
        return {
            "message_type": self.message_type,
            "schema_version": self.schema_version,
            "candidate_id": self.candidate_id,
            "strategy_id": self.strategy_id,
            "strategy_version": self.strategy_version,
            "hypothesis_id": self.hypothesis_id,
            "parent_ids": list(self.parent_ids),
            "provider": self.provider,
            "instrument": self.instrument,
            "dataset_version": self.dataset_version,
            "features": list(self.features),
            "model_version": self.model_version,
            "schema": _plain(self.schema),
            "executable_prices": _plain(self.executable_prices),
            "regime": _plain(self.regime),
            "cost_assumptions": _plain(self.cost_assumptions),
            "fitness": self.fitness,
            "created_at": self.created_at.isoformat(),
        }


Hypothesis = HypothesisMessage
ResearchReport = ReportMessage
Candidate = CandidateMessage


@dataclass(frozen=True, slots=True)
class HermesPermissions:
    """Explicit capability set; unsafe capabilities are always false."""

    publish_hypothesis: bool = True
    publish_report: bool = True
    intake_candidate: bool = True
    execute_orders: bool = False
    mutate_account: bool = False
    mutate_risk: bool = False
    mutate_history: bool = False

    def allows(self, action: str) -> bool:
        return bool(getattr(self, action, False))

    can = allows

    @property
    def allowed_actions(self) -> tuple[str, ...]:
        return tuple(name for name in ("publish_hypothesis", "publish_report", "intake_candidate") if getattr(self, name))


PermissionModel = HermesPermissions


class Hermes:
    """In-process, append-only research message exchange."""

    def __init__(self, permissions: HermesPermissions | None = None) -> None:
        permissions = permissions or HermesPermissions()
        if permissions.execute_orders or permissions.mutate_account or permissions.mutate_risk or permissions.mutate_history:
            raise HermesValidationError("Hermes cannot be granted execution/account/risk/history permissions")
        self.permissions = permissions
        self._messages: list[HypothesisMessage | ReportMessage | CandidateMessage] = []

    @property
    def messages(self) -> tuple[HypothesisMessage | ReportMessage | CandidateMessage, ...]:
        return tuple(self._messages)

    def publish_hypothesis(self, message: HypothesisMessage) -> HypothesisMessage:
        if not self.permissions.publish_hypothesis:
            raise PermissionError("publish_hypothesis is not permitted")
        if not isinstance(message, HypothesisMessage):
            raise HermesValidationError("expected HypothesisMessage")
        self._messages.append(message)
        return message

    def publish_report(self, message: ReportMessage) -> ReportMessage:
        if not self.permissions.publish_report:
            raise PermissionError("publish_report is not permitted")
        if not isinstance(message, ReportMessage):
            raise HermesValidationError("expected ReportMessage")
        self._messages.append(message)
        return message

    def intake_candidate(self, candidate: CandidateMessage | Mapping[str, Any]) -> CandidateMessage:
        if not self.permissions.intake_candidate:
            raise PermissionError("intake_candidate is not permitted")
        if isinstance(candidate, Mapping):
            candidate = CandidateMessage(**dict(candidate))
        if not isinstance(candidate, CandidateMessage):
            raise HermesValidationError("expected CandidateMessage or mapping")
        self._messages.append(candidate)
        return candidate

    def to_records(self) -> list[dict[str, Any]]:
        return [message.to_record() for message in self._messages]

    def to_json(self) -> str:
        return json.dumps(self.to_records(), sort_keys=True, separators=(",", ":"), allow_nan=False)


HermesBus = Hermes


def validate_hypothesis(message: HypothesisMessage | Mapping[str, Any]) -> HypothesisMessage:
    return message if isinstance(message, HypothesisMessage) else HypothesisMessage(**dict(message))


def validate_report(message: ReportMessage | Mapping[str, Any]) -> ReportMessage:
    return message if isinstance(message, ReportMessage) else ReportMessage(**dict(message))


def validate_candidate(message: CandidateMessage | Mapping[str, Any]) -> CandidateMessage:
    return message if isinstance(message, CandidateMessage) else CandidateMessage(**dict(message))
