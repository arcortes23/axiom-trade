"""Durable, permission-bounded research message bus for Hermes integration."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import json
import re
from itertools import islice
import math
from typing import Any, Iterable, Mapping

from .storage import AxiomStore


class ResearchBusPermissionError(ValueError):
    """Raised when a research message attempts an execution or private mutation."""


class ResearchQueueStatus(str, Enum):
    PENDING = "PENDING"
    ACCEPTED = "ACCEPTED"
    REJECTED = "REJECTED"
    TESTING = "TESTING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


@dataclass(frozen=True, slots=True)
class ResearchQueueItem:
    item_id: str
    item_type: str
    status: ResearchQueueStatus
    payload: Mapping[str, Any]
    source: str
    author: str
    lineage: tuple[Any, ...]
    created_at: datetime | None
    updated_at: datetime | None
    attempts: int
    last_error: str | None = None
    lease_until: datetime | None = None
    lease_owner: str | None = None
    dedupe_key: str | None = None
    result: Any | None = None
    schema_version: str = "1"
    @classmethod
    def from_record(cls, record: Mapping[str, Any]) -> "ResearchQueueItem":
        return cls(
            str(record["item_id"]),
            str(record["item_type"]),
            ResearchQueueStatus(str(record["status"])),
            dict(record.get("payload", {})) if isinstance(record.get("payload"), Mapping) else {},
            str(record.get("source", "")),
            str(record.get("author", "")),
            tuple(record.get("lineage", ())),
            record.get("created_at"),
            record.get("updated_at"),
            int(record.get("attempts", 0)),
            str(record["last_error"]) if record.get("last_error") is not None else None,
            record.get("lease_until"),
            str(record["lease_owner"]) if record.get("lease_owner") is not None else None,
            str(record["dedupe_key"]) if record.get("dedupe_key") is not None else None,
            record.get("result"),
            str(record.get("schema_version", "1")),
        )

    def as_record(self) -> dict[str, Any]:
        return {
            "item_id": self.item_id,
            "item_type": self.item_type,
            "status": self.status.value,
            "payload": dict(self.payload),
            "source": self.source,
            "author": self.author,
            "lineage": list(self.lineage),
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "attempts": self.attempts,
            "last_error": self.last_error,
            "lease_until": self.lease_until,
            "lease_owner": self.lease_owner,
            "dedupe_key": self.dedupe_key,
            "result": self.result,
            "schema_version": self.schema_version,
        }


_ALLOWED_TYPES = frozenset({"hypothesis", "candidate", "report", "review_request", "experiment_result"})
_FORBIDDEN_FIELD_TOKENS = frozenset(
    {
        "credential",
        "credentials",
        "private",
        "secret",
        "secrets",
        "password",
        "passwords",
        "token",
        "tokens",
        "cookie",
        "cookies",
        "session",
        "sessions",
        "authorization",
        "bearer",
        "oauth",
        "jwt",
        "order",
        "orders",
        "account",
        "accounts",
        "risk",
        "history",
        "frozen",
        "execute",
        "execution",
        "live",
        "withdraw",
        "withdrawal",
        "withdrawals",
        "wallet",
        "wallets",
    }
)
_FORBIDDEN_EXACT_FIELDS = frozenset(
    {
        "auth",
        "authentication",
        "api_key",
        "private_key",
        "access_token",
        "refresh_token",
        "client_secret",
        "order_id",
        "place_order",
        "submit_order",
        "execute_order",
        "live_execution",
    }
)


_SAFE_IDENTIFIER_FIELDS = frozenset(
    {
        "token_id",
        "token_ids",
        "market_token_id",
        "market_token_ids",
        "yes_token_id",
        "no_token_id",
        "clob_token_id",
        "clob_token_ids",
    }
)


def _is_forbidden_field(key: Any) -> bool:
    normalized = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", str(key))
    normalized = re.sub(r"[^A-Za-z0-9]+", "_", normalized).strip("_").lower()
    if normalized in _SAFE_IDENTIFIER_FIELDS:
        return False
    tokens = frozenset(part for part in normalized.split("_") if part)
    return normalized in _FORBIDDEN_EXACT_FIELDS or bool(tokens & _FORBIDDEN_FIELD_TOKENS)


_CREDENTIAL_MARKER_RE = re.compile(
    r"""(?ix)
    (?:
        \bprivate[\s._-]*(?:key|material)\b
        |\bapi[\s._-]*(?:key|secret)\b
        |\bclient[\s._-]*secret\b
        |\bsecret\b
        |\bpass(?:word|phrase)\b
        |\baccess[\s._-]*token\b
        |\brefresh[\s._-]*token\b
        |\bbearer[\s._-]*token\b
        |\bwallet[\s._-]*(?:private[\s._-]*(?:key|material)|secret|seed|mnemonic)\b
        |\b(?:seed|mnemonic)[\s._-]*phrase\b
        |\b(?:authorization|auth)[\s._-]*header\b
        |\b(?:set[\s._-]*)?cookie\s*[:=]
    )
    """,
)

_CREDENTIAL_ASSIGNMENT_RE = re.compile(
    r"""(?ix)
    (?<![a-z0-9])
    ["']?
    (?:
        private[\s._-]*(?:key|material)
        |api[\s._-]*(?:key|secret)
        |client[\s._-]*secret
        |secret
        |pass(?:word|phrase)
        |access[\s._-]*token
        |refresh[\s._-]*token
        |bearer[\s._-]*token
        |wallet[\s._-]*(?:private[\s._-]*(?:key|material)|secret|seed|mnemonic)
        |(?:seed|mnemonic)[\s._-]*phrase
        |(?:authorization|auth)[\s._-]*header
    )
    ["']?\s*(?:=|:|=>)\s*\S+
    """,
)

_VENUE_CREDENTIAL_RE = re.compile(
    r"""(?ix)
    (?:
        \b(?:binance|polymarket)\b[\s\S]{0,120}
        \b(?:signed?|signature|signing|hmac|credential|credentials|auth(?:entication|orization)?)
        \b
        |
        \b(?:signed?|signature|signing|hmac|credential|credentials|auth(?:entication|orization)?)
        \b[\s\S]{0,120}\b(?:binance|polymarket)\b
        |
        \b(?:binance|polymarket)\b[\s\S]{0,120}
        \baccount[\s._-]*(?:credential|key|secret|auth|access|sign)
        \b
        |
        \baccount[\s._-]*(?:credential|key|secret|auth|access|sign)
        \b[\s\S]{0,120}\b(?:binance|polymarket)\b
    )
    """,
)


def _contains_credential_marker(value: str) -> bool:
    return bool(
        _CREDENTIAL_MARKER_RE.search(value)
        or _CREDENTIAL_ASSIGNMENT_RE.search(value)
        or _VENUE_CREDENTIAL_RE.search(value)
    )


def _is_forbidden_string(value: str) -> bool:
    return _contains_credential_marker(value)


_MAX_PAYLOAD_BYTES = 16_384
_MAX_PAYLOAD_DEPTH = 8
_MAX_COLLECTION_ITEMS = 256
_MAX_STRING_LENGTH = 4_096


class DurableResearchBus:
    """SQLite-backed queue with dedupe, leases, immutable event history and safe payloads."""

    def __init__(self, store: AxiomStore, *, source: str = "hermes", author: str = "hermes") -> None:
        self._store = store
        self.source = str(source)
        self.author = str(author)

    def submit(
        self,
        item_type: str,
        payload: Mapping[str, Any],
        *,
        dedupe_key: str | None = None,
        lineage: Iterable[Any] = (),
        priority: int = 0,
        available_at: datetime | None = None,
        schema_version: str = "1",
    ) -> ResearchQueueItem:
        kind = str(item_type).strip().lower()
        if kind not in _ALLOWED_TYPES:
            raise ValueError(f"unsupported research item type: {item_type}")
        if not isinstance(payload, Mapping):
            raise TypeError("research payload must be a mapping")
        clean = _validate_payload(payload)
        lineage_values = tuple(islice(iter(lineage), _MAX_COLLECTION_ITEMS + 1))
        if len(lineage_values) > _MAX_COLLECTION_ITEMS:
            raise ValueError(f"research lineage exceeds {_MAX_COLLECTION_ITEMS} entries")
        clean_lineage = _validate_payload({"lineage": list(lineage_values)})["lineage"]
        if isinstance(priority, bool) or not isinstance(priority, int):
            raise ValueError("priority must be an integer")
        record = self._store.enqueue_research_item(
            kind,
            clean,
            dedupe_key=dedupe_key,
            source=self.source,
            author=self.author,
            lineage=tuple(clean_lineage),
            priority=priority,
            available_at=available_at,
            schema_version=schema_version,
        )
        return ResearchQueueItem.from_record(record)

    def submit_hypothesis(self, payload: Mapping[str, Any], **kwargs: Any) -> ResearchQueueItem:
        return self.submit("hypothesis", payload, **kwargs)

    def submit_candidate(self, payload: Mapping[str, Any], **kwargs: Any) -> ResearchQueueItem:
        return self.submit("candidate", payload, **kwargs)

    def submit_report(self, payload: Mapping[str, Any], **kwargs: Any) -> ResearchQueueItem:
        return self.submit("report", payload, **kwargs)

    def submit_review_request(self, payload: Mapping[str, Any], **kwargs: Any) -> ResearchQueueItem:
        return self.submit("review_request", payload, **kwargs)

    def submit_experiment_result(self, payload: Mapping[str, Any], **kwargs: Any) -> ResearchQueueItem:
        return self.submit("experiment_result", payload, **kwargs)
    def claim(self, worker: str, *, lease_seconds: float = 300.0, now: datetime | None = None) -> ResearchQueueItem | None:
        if not math.isfinite(float(lease_seconds)) or lease_seconds <= 0:
            raise ValueError("lease_seconds must be finite and positive")
        record = self._store.claim_research_item(str(worker), lease_seconds=lease_seconds, now=now)
        return ResearchQueueItem.from_record(record) if record else None

    def complete(
        self,
        item_id: str,
        *,
        result: Mapping[str, Any] | None = None,
        status: ResearchQueueStatus = ResearchQueueStatus.COMPLETED,
        error: str | None = None,
        worker: str | None = None,
        now: datetime | None = None,
    ) -> ResearchQueueItem:
        if result is not None:
            _validate_payload(result)
        record = self._store.complete_research_item(
            item_id,
            status.value if isinstance(status, ResearchQueueStatus) else str(status),
            result=result,
            error=error,
            worker=worker,
            now=now,
        )
        return ResearchQueueItem.from_record(record)

    def resume_expired(self, *, now: datetime | None = None) -> int:
        return self._store.release_expired_research_items(now=now)

    def get(self, item_id: str) -> ResearchQueueItem | None:
        record = self._store.get_research_item(item_id)
        return ResearchQueueItem.from_record(record) if record else None

    def list(self, *, status: ResearchQueueStatus | str | None = None, limit: int = 100) -> tuple[ResearchQueueItem, ...]:
        value = status.value if isinstance(status, ResearchQueueStatus) else status
        return tuple(ResearchQueueItem.from_record(item) for item in self._store.list_research_items(status=value, limit=limit))

    def stats(self) -> dict[str, int]:
        return self._store.research_queue_stats()


def _validate_payload(value: Mapping[str, Any]) -> dict[str, Any]:
    def walk(item: Any, path: str, depth: int) -> Any:
        if depth > _MAX_PAYLOAD_DEPTH:
            raise ValueError(f"research payload nesting exceeds {_MAX_PAYLOAD_DEPTH} levels: {path}")
        if isinstance(item, Mapping):
            if len(item) > _MAX_COLLECTION_ITEMS:
                raise ValueError(f"research payload mapping is too large: {path}")
            result: dict[str, Any] = {}
            for key, child in item.items():
                key_text = str(key)
                if len(key_text) > _MAX_STRING_LENGTH:
                    raise ValueError(f"research payload key is too long: {path}")
                if _is_forbidden_field(key_text):
                    raise ResearchBusPermissionError(f"Hermes research bus forbids field: {path}.{key}")
                result[key_text] = walk(child, f"{path}.{key}", depth + 1)
            return result
        if isinstance(item, (list, tuple)):
            if len(item) > _MAX_COLLECTION_ITEMS:
                raise ValueError(f"research payload collection is too large: {path}")
            return [walk(child, f"{path}[]", depth + 1) for child in item]
        if isinstance(item, str):
            if len(item) > _MAX_STRING_LENGTH:
                raise ValueError(f"research payload string is too long: {path}")
            if _is_forbidden_string(item):
                raise ResearchBusPermissionError(f"Hermes research bus forbids credential-like content: {path}")
            return item
        if isinstance(item, (int, float, bool)) or item is None:
            if isinstance(item, float) and not math.isfinite(item):
                raise ValueError(f"non-finite research payload value: {path}")
            return item
        raise TypeError(f"unsupported research payload value at {path}: {type(item).__name__}")

    clean = walk(value, "payload", 0)
    encoded = json.dumps(clean, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    if len(encoded) > _MAX_PAYLOAD_BYTES:
        raise ValueError(f"research payload exceeds {_MAX_PAYLOAD_BYTES} UTF-8 bytes")
    return clean


__all__ = ["DurableResearchBus", "ResearchBusPermissionError", "ResearchQueueItem", "ResearchQueueStatus"]
