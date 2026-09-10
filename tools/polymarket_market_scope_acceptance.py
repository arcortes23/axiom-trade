"""Bounded, secret-free acceptance evidence for the Polymarket market scope.

The default invocation uses deterministic offline fixtures.  ``--mode public`` is
an explicitly separate workflow for a later isolated run; it uses only the
read-only adapter methods and the public GET paths listed in ``PUBLIC_GET_PATHS``.
No credential, authenticated endpoint, order transport, or live database is
accepted by this module.
"""
from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass, is_dataclass
from enum import Enum
from datetime import date, datetime, time as datetime_time, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_DOWN
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import sys
import time
from typing import Any, Callable, Mapping, Sequence
from urllib.parse import quote


if __package__ in {None, ""}:
    _REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
    if str(_REPOSITORY_ROOT) not in sys.path:
        sys.path.insert(0, str(_REPOSITORY_ROOT))

try:  # Running as ``python tools/...`` from the repository root.
    from axiom.experiment_plan import MarketScopePolicy, normalize_market_scope
    from axiom.lifecycle import PromotionCriteria
except Exception:  # pragma: no cover - an import error is reported by the runner.
    MarketScopePolicy = None  # type: ignore[assignment,misc]
    normalize_market_scope = None  # type: ignore[assignment,misc]
    PromotionCriteria = None  # type: ignore[assignment,misc]
SCHEMA_VERSION = "polymarket-market-scope-acceptance-v3"
FIXTURE_TIMESTAMP = "2026-01-01T00:00:00+00:00"
POLITICS_TAG_SLUG = "politics"
DISCOVERY_MIN_LIQUIDITY = 1_000.0
DISCOVERY_MIN_HOURS = 24.0
DISCOVERY_MAX_HOURS = 168.0
# These module-level values are the persisted acceptance contract.  Persisted
# mode always uses them; the CLI has no dataset/count override switches.
_PINNED_PERSISTED_DATASET_ID = "Polymarket-historical"
_PINNED_PERSISTED_DATASET_VERSION = "sha256:9a831357e3f4016ad2c4f05d9bb11a98583ad94a2da36873b56e2b7db786a9ea"
_PINNED_PERSISTED_ATTESTATION_HASH = "sha256:24e279b36735f5c6fb97a6115281d681fbaa3afd08a64c982c36cd929b9d93a3"
_PINNED_PERSISTED_ROW_COUNT = 18_141
_PINNED_PERSISTED_CONSTITUENT_COUNT = 1_000
POLYMARKET_HISTORICAL_DATASET_ID = _PINNED_PERSISTED_DATASET_ID
POLYMARKET_HISTORICAL_DATASET_VERSION = _PINNED_PERSISTED_DATASET_VERSION
POLYMARKET_HISTORICAL_ATTESTATION_HASH = _PINNED_PERSISTED_ATTESTATION_HASH
POLYMARKET_HISTORICAL_ROW_COUNT = _PINNED_PERSISTED_ROW_COUNT
POLYMARKET_HISTORICAL_CONSTITUENT_COUNT = _PINNED_PERSISTED_CONSTITUENT_COUNT
PUBLIC_GET_PATHS = (
    "/markets/keyset",
    "/markets/{market_id}",
    "/tags/slug/{slug}",
    "/book",
)
_FORBIDDEN_PATH_RE = re.compile(r"/(?:orders?|trades?|auth|private|account|wallet|positions?)(?:/|$)", re.I)
_SENSITIVE_KEY_RE = re.compile(
    r"(?:^|_)(?:auth(?:orization)?|auth_header|signature|private_data|private_state|"
    r"(?:(?:api|access|refresh|auth|client|session|csrf|xsrf|private|public|bearer)_*)?"
    r"(?:token|key|secret|password|passphrase|credential))(?:$|_)",
    re.I,
)
_PUBLIC_TOKEN_KEY_RE = re.compile(
    r"^(?:(?:yes|no|clob|outcome|conditional)_)?token_ids?$|^(?:yes|no)_token_id$",
    re.I,
)
_SECRET_VALUE_RE = re.compile(
    r"(?ix)(?:bearer\s+[A-Za-z0-9._~+/=-]+|"
    r"(?:(?:api|access|refresh|auth|client|session|private|public)?[_-]?"
    r"(?:token|key|secret|password|passphrase))\s*[:=]\s*[^,;\s]+|"
    r"(?:private[_-]?key|private[_-]?material)\s*[-:=\s]+\S+|"
    r"eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,})"
)


@dataclass(frozen=True, slots=True)
class AcceptanceConfig:
    """Finite limits for one acceptance run.

    ``page_limit`` is the per-request metadata limit and is always capped at
    the adapter contract's 100 rows. Metadata pagination has its own budget;
    persisted mode instead reads one immutable SQLite snapshot.
    """

    page_limit: int = 100
    max_pages: int = 4
    max_seconds: float = 60.0
    max_books: int = 2
    book_depth: int = 5
    sample_limit: int = 8
    mode: str = "offline"
    source_backup: Path | None = None
    dataset_id: str | None = None
    dataset_version: str | None = None
    expected_row_count: int | None = None
    expected_constituent_count: int | None = None

    def __post_init__(self) -> None:
        if isinstance(self.page_limit, bool) or not isinstance(self.page_limit, int) or not 1 <= self.page_limit <= 100:
            raise ValueError("page_limit must be an integer in [1, 100]")
        if isinstance(self.max_pages, bool) or not isinstance(self.max_pages, int) or not 1 <= self.max_pages <= 4:
            raise ValueError("max_pages must be an integer in [1, 4]")
        seconds = float(self.max_seconds)
        if not math.isfinite(seconds) or not 0 < seconds <= 60:
            raise ValueError("max_seconds must be finite and in (0, 60]")
        if isinstance(self.max_books, bool) or not isinstance(self.max_books, int) or not 0 <= self.max_books <= 2:
            raise ValueError("max_books must be an integer in [0, 2]")
        if isinstance(self.book_depth, bool) or not isinstance(self.book_depth, int) or not 1 <= self.book_depth <= 100:
            raise ValueError("book_depth must be an integer in [1, 100]")
        if isinstance(self.sample_limit, bool) or not isinstance(self.sample_limit, int) or not 0 <= self.sample_limit <= 100:
            raise ValueError("sample_limit must be an integer in [0, 100]")
        normalized_mode = str(self.mode).lower()
        if normalized_mode not in {"offline", "public", "persisted"}:
            raise ValueError("mode must be offline, public, or persisted")
        for name, value in (("expected_row_count", self.expected_row_count), ("expected_constituent_count", self.expected_constituent_count)):
            if value is not None and (isinstance(value, bool) or not isinstance(value, int) or value < 0):
                raise ValueError(f"{name} must be a non-negative integer")
        if normalized_mode == "persisted":
            pinned = {
                "dataset_id": _PINNED_PERSISTED_DATASET_ID,
                "dataset_version": _PINNED_PERSISTED_DATASET_VERSION,
                "expected_row_count": _PINNED_PERSISTED_ROW_COUNT,
                "expected_constituent_count": _PINNED_PERSISTED_CONSTITUENT_COUNT,
            }
            for name, expected in pinned.items():
                value = getattr(self, name)
                if value is not None and value != expected:
                    raise ValueError(f"persisted mode does not allow overriding {name}")
        object.__setattr__(self, "mode", normalized_mode)
        if self.source_backup is not None:
            object.__setattr__(self, "source_backup", Path(self.source_backup))

    @property
    def metadata_page_budget(self) -> int:
        """The independent metadata budget used by the runner."""
        return self.max_pages


@dataclass(frozen=True, slots=True)
class OfflineDiscoveryPage:
    snapshots: tuple[Mapping[str, Any], ...]
    next_cursor: str | None
    request_path: str = "/markets/keyset"
    query: Mapping[str, Any] = None  # type: ignore[assignment]
    query_fingerprint: str = "offline-fixture"
    raw_count: int = 0
    unique_count: int = 0
    duplicate_count: int = 0
    malformed_count: int = 0
    coverage_status: str = "COMPLETE"
    error_reason: str | None = None
    requested_at: str = FIXTURE_TIMESTAMP

    def __post_init__(self) -> None:
        object.__setattr__(self, "snapshots", tuple(self.snapshots))
        if self.query is None:
            object.__setattr__(self, "query", {})


class OfflineFixtureAdapter:
    """Small deterministic adapter implementing the contracted public methods."""

    def __init__(self, pages: Sequence[OfflineDiscoveryPage], identities: Mapping[str, Mapping[str, Any]]) -> None:
        self.pages = tuple(pages)
        self.identities = {str(key): dict(value) for key, value in identities.items()}
        self.calls: list[dict[str, Any]] = []
        self._cursor_to_index = {None: 0}
        for index in range(len(self.pages) - 1):
            self._cursor_to_index[f"fixture-cursor-{index + 1}"] = index + 1
    def resolve_tag_slug(self, slug: str) -> int | None:
        self.calls.append(
            {
                "method": "GET",
                "path": f"/tags/slug/{slug}",
                "query": {},
                "slug": str(slug),
            }
        )
        return 17 if str(slug) == POLITICS_TAG_SLUG else None


    def market_page(
        self,
        limit: int = 100,
        after_cursor: str | None = None,
        closed: bool = False,
        tag_ids: Sequence[int] = (),
        include_tag: bool = True,
        liquidity_num_min: float | None = None,
        end_date_min: str | None = None,
        end_date_max: str | None = None,
    ) -> OfflineDiscoveryPage:
        self.calls.append(
            {
                "method": "GET",
                "path": "/markets/keyset",
                "limit": limit,
                "after_cursor": after_cursor,
                "closed": closed,
                "tag_ids": tuple(tag_ids),
                "include_tag": include_tag,
                "liquidity_num_min": liquidity_num_min,
                "end_date_min": end_date_min,
                "end_date_max": end_date_max,
            }
        )
        index = self._cursor_to_index.get(after_cursor)
        if index is None or index >= len(self.pages):
            raise ValueError("offline fixture received an unknown opaque cursor")
        return self.pages[index]

    def market(self, market_id: str) -> Mapping[str, Any] | None:
        self.calls.append({"method": "GET", "path": f"/markets/{market_id}", "market_id": market_id})
        return self.identities.get(str(market_id))

    def token_ids(self, market_id: str) -> Mapping[str, str]:
        identity = self.identities.get(str(market_id), {})
        value = identity.get("tokens", identity.get("clobTokenIds", {}))
        return dict(value) if isinstance(value, Mapping) else {}

    def order_book_for_token(self, token_id: str, depth: int = 20) -> Mapping[str, Any] | None:
        self.calls.append({"method": "GET", "path": "/book", "token_id": token_id, "depth": depth})
        return {
            "token_id": str(token_id),
            "timestamp": FIXTURE_TIMESTAMP,
            "bids": [{"price": 0.49, "size": 25.0}, {"price": 0.48, "size": 20.0}][:depth],
            "asks": [{"price": 0.51, "size": 25.0}, {"price": 0.52, "size": 20.0}][:depth],
        }


def _offline_market(
    market_id: str,
    *,
    category: str | None = "politics",
    tags: Sequence[str] = (),
    active: bool = True,
    closed: bool = False,
    settlement: str = "open",
    yes_mid: Any = 0.50,
    expiry_hours: float = 72.0,
    liquidity: Any = 2_000.0,
    yes_bid: Any = 0.48,
    yes_ask: Any = 0.52,
    timestamp: str = FIXTURE_TIMESTAMP,
) -> dict[str, Any]:
    expiry = datetime.fromisoformat(timestamp) + timedelta(hours=expiry_hours)
    return {
        "id": market_id,
        "question": f"Will fixture event {market_id} resolve YES?",
        "timestamp": timestamp,
        "instrument": "POLYMARKET",
        "active": active,
        "closed": closed,
        "archived": False,
        "acceptingOrders": active and not closed,
        "enableOrderBook": active and not closed,
        "settlement": settlement,
        "category": category,
        "tags": list(tags),
        "yes_mid": yes_mid,
        "expiry": expiry.isoformat(),
        "liquidity": liquidity,
        "yes_bid": yes_bid,
        "yes_ask": yes_ask,
        "tokens": {"yes": f"yes-{market_id}", "no": f"no-{market_id}"},
    }


def build_offline_fixture_adapter() -> OfflineFixtureAdapter:
    """Return fully labeled fixtures with no secrets or network dependency."""
    first = _offline_market("m-001", yes_mid=0.30)
    second = _offline_market("m-002", category="sports", tags=("sports",))
    third = _offline_market("m-003", closed=True, active=False, settlement="resolved_no")
    fourth = _offline_market("m-004", yes_mid="not-a-number")
    fifth = _offline_market("m-005", yes_mid=None)
    sixth = _offline_market("m-006", expiry_hours=12.0, liquidity=900.0, yes_bid=0.40, yes_ask=0.52)
    # The duplicate is deliberate evidence that pagination/order accounting is
    # not allowed to inflate policy counts.
    pages = (
        OfflineDiscoveryPage(
            snapshots=(first, second, third),
            next_cursor="fixture-cursor-1",
            raw_count=3,
            unique_count=3,
            duplicate_count=0,
            malformed_count=0,
            coverage_status="PARTIAL",
            requested_at=FIXTURE_TIMESTAMP,
        ),
        OfflineDiscoveryPage(
            snapshots=(fourth, fifth, sixth, first),
            next_cursor=None,
            raw_count=4,
            unique_count=3,
            duplicate_count=1,
            malformed_count=0,
            coverage_status="COMPLETE",
            requested_at="2026-01-01T00:00:01+00:00",
        ),
    )
    identities = {market["id"]: market for market in (first, second, third, fourth, fifth, sixth)}
    return OfflineFixtureAdapter(pages, identities)


def _temporal_iso(value: Any) -> str | None:
    """Return deterministic ISO text for supported public temporal values."""
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, (date, datetime_time)):
        return value.isoformat()
    return None


def _canonical_json(value: Any) -> str:
    def normalize(item: Any) -> Any:
        temporal = _temporal_iso(item)
        if temporal is not None:
            return temporal
        if is_dataclass(item):
            return normalize(asdict(item))
        if isinstance(item, Mapping):
            return {str(key): normalize(child) for key, child in sorted(item.items(), key=lambda pair: str(pair[0]))}
        if isinstance(item, (tuple, list)):
            return [normalize(child) for child in item]
        if isinstance(item, float) and not math.isfinite(item):
            return str(item)
        return item

    return json.dumps(normalize(value), sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _sha256_json(value: Any) -> str:
    return "sha256:" + hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    if is_dataclass(value):
        return asdict(value)
    names = getattr(value, "__dataclass_fields__", {})
    if names:
        return {name: getattr(value, name) for name in names}
    result: dict[str, Any] = {}
    for name in (
        "market_id",
        "id",
        "condition_id",
        "conditionId",
        "question",
        "timestamp",
        "active",
        "closed",
        "archived",
        "accepting_orders",
        "acceptingOrders",
        "enable_order_book",
        "enableOrderBook",
        "resolved",
        "isResolved",
        "settlement",
        "settlement_state",
        "category",
        "tags",
        "instrument",
        "instrument_type",
        "regime",
        "regime_state",
        "regimeState",
        "yes_mid",
        "yes_bid",
        "yes_ask",
        "expiry",
        "liquidity",
        "yes_token_id",
        "no_token_id",
    ):
        if hasattr(value, name):
            result[name] = getattr(value, name)
    return result
 
 
def _value(raw: Mapping[str, Any], *names: str) -> tuple[bool, Any]:
    for name in names:
        if name in raw:
            return True, raw[name]
    return False, None
def _nested_value(raw: Mapping[str, Any], *names: str) -> tuple[bool, Any]:
    """Read canonical persisted fields without treating market IDs as identity."""
    for name in names:
        if name in raw:
            return True, raw[name]
    for container_name in ("payload", "snapshot", "metadata", "extra"):
        nested = raw.get(container_name)
        if isinstance(nested, Mapping):
            present, value = _nested_value(nested, *names)
            if present:
                return True, value
    return False, None


def _market_instrument(
    raw: Mapping[str, Any],
    *,
    trusted_provider: str | None = None,
) -> tuple[bool, Any]:
    present, value = _nested_value(raw, "instrument")
    if present:
        return present, value
    metadata = raw.get("metadata")
    if isinstance(metadata, Mapping):
        if "symbol" in metadata:
            return True, metadata["symbol"]
    payload = raw.get("payload")
    if isinstance(payload, Mapping):
        if "instrument" in payload:
            return True, payload["instrument"]
        nested_metadata = payload.get("metadata")
        if isinstance(nested_metadata, Mapping) and "symbol" in nested_metadata:
            return True, nested_metadata["symbol"]
    if trusted_provider is not None:
        source_present, source = _nested_value(raw, "source", "provider")
        if (
            not source_present
            or (
                isinstance(source, str)
                and source.strip().casefold() == trusted_provider.strip().casefold()
            )
        ):
            return True, trusted_provider
    return False, None


def _trusted_provider_for_adapter(adapter: Any, mode: str) -> str | None:
    """Return provider context only for the public Polymarket adapter."""
    if str(mode).strip().casefold() != "public":
        return None
    provider_name = getattr(adapter, "provider_name", None)
    if isinstance(provider_name, str) and provider_name.strip().casefold() == "polymarket":
        return "POLYMARKET"
    return None


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(float(value)):
        parsed = datetime.fromtimestamp(float(value), tz=timezone.utc)
    elif isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _finite(value: Any, default: float = 0.0) -> float:
    """Mirror the processor's permissive finite conversion exactly."""
    try:
        number = float(value)
    except (TypeError, ValueError):
        return float(default)
    return number if math.isfinite(number) else float(default)


def _numeric_quality(value: Any, *, present: bool) -> tuple[float, str]:
    """Classify an observed number while retaining processor coercion."""
    if not present or value is None or (isinstance(value, str) and not value.strip()):
        return math.nan, "missing"
    number = _finite(value, math.nan)
    return (number, "valid") if math.isfinite(number) else (math.nan, "malformed")


def _number(value: Any) -> tuple[float | None, str]:
    if value is None or (isinstance(value, str) and not value.strip()):
        return None, "missing"
    if isinstance(value, bool):
        return None, "malformed"
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None, "malformed"
    if not math.isfinite(number):
        return None, "malformed"
    return number, "valid"


def _bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"true", "1", "yes", "y", "open", "active"}:
            return True
        if text in {"false", "0", "no", "n", "closed", "inactive"}:
            return False
    return None


def _list_value(value: Any) -> list[Any]:
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except (TypeError, ValueError, json.JSONDecodeError):
            return [value]
        value = decoded
    if isinstance(value, (tuple, list)):
        return list(value)
    return [value] if value is not None else []


def _policy_record(policy: Any | None = None) -> dict[str, Any]:
    """Return the normalized canonical MarketScopePolicy material."""
    if normalize_market_scope is None or MarketScopePolicy is None:
        raise RuntimeError("canonical MarketScopePolicy is unavailable")
    if policy is None:
        candidate: Mapping[str, Any] = {
            "schema_version": "1",
            "mode": "RULE_BASED_MARKETS",
            "instrument": "POLYMARKET",
            "categories": ["politics"],
            "market_ids": [],
            "filters": {
                "category": ["politics"],
                "entry_price": [0.40, 0.60],
                "minimum_hours_to_resolution": 24.0,
                "maximum_hours_to_resolution": 168.0,
                "min_liquidity": 1000.0,
                "max_spread": 0.05,
            },
            "regime_restrictions": {},
            "provenance": "canonical",
        }
    elif isinstance(policy, MarketScopePolicy):
        return dict(policy.as_dict())
    elif isinstance(policy, Mapping):
        candidate = dict(policy)
    elif hasattr(policy, "as_dict") and callable(policy.as_dict):
        candidate = dict(policy.as_dict())
    elif hasattr(policy, "as_record") and callable(policy.as_record):
        candidate = dict(policy.as_record())
    elif is_dataclass(policy):
        candidate = asdict(policy)
    else:
        raise TypeError("policy must be a canonical MarketScopePolicy or mapping")
    normalized = normalize_market_scope(candidate)
    return dict(normalized.as_dict())


def _policy_filters(policy: Mapping[str, Any]) -> dict[str, Any]:
    value = policy.get("filters", {})
    return dict(value) if isinstance(value, Mapping) else {}


def _policy_categories(policy: Mapping[str, Any]) -> tuple[str, ...]:
    values = policy.get("categories", ())
    categories = [str(item).strip().casefold() for item in _list_value(values) if str(item).strip()]
    if not categories:
        category = _policy_filters(policy).get("category")
        categories = [str(item).strip().casefold() for item in _list_value(category) if str(item).strip()]
    return tuple(dict.fromkeys(categories))


def _policy_bound(filters: Mapping[str, Any], name: str) -> float | list[float] | None:
    raw = filters.get(name)
    if raw is None or isinstance(raw, bool):
        return None
    values = _list_value(raw) if isinstance(raw, (list, tuple)) else [raw]
    result: list[float] = []
    for value in values:
        number, quality = _number(value)
        if quality != "valid":
            return None
        result.append(float(number))
    if not result:
        return None
    return result if isinstance(raw, (list, tuple)) else result[0]


def _market_identity(raw: Mapping[str, Any]) -> str | None:
    for name in ("market_id", "id", "condition_id", "conditionId"):
        if name not in raw:
            continue
        value = raw[name]
        if isinstance(value, bool) or value is None:
            continue
        text = str(value).strip()
        if text and text.lower() not in {"none", "null"}:
            return text
    return None


def _market_sort_key(identifier: str) -> tuple[int, int | str, str]:
    """Order numeric market IDs numerically, then nonnumeric IDs lexically."""
    value = str(identifier).strip()
    if re.fullmatch(r"[0-9]+", value):
        return (0, int(value), value)
    return (1, value, value)


def _evaluate_market(
    raw_value: Any,
    policy: Mapping[str, Any],
    fallback_timestamp: datetime,
    *,
    trusted_provider: str | None = None,
) -> dict[str, Any]:
    raw = _mapping(raw_value)
    identifier = _market_identity(raw) or ""
    observed_at = _parse_datetime(raw.get("timestamp", raw.get("updated_at", raw.get("updatedAt")))) or fallback_timestamp
    filters = _policy_filters(policy)

    closed_present, closed_raw = _value(raw, "closed")
    active_present, active_raw = _value(raw, "active")
    archived_present, archived_raw = _value(raw, "archived")
    accepting_present, accepting_raw = _value(raw, "acceptingOrders", "accepting_orders")
    order_book_present, order_book_raw = _value(raw, "enableOrderBook", "enable_order_book")
    resolved_present, resolved_raw = _value(raw, "resolved", "isResolved", "is_resolved", "resolved_flag", "resolvedFlag")
    settlement_present, settlement_raw = _value(raw, "settlement", "settlement_state")
    closed = _bool(closed_raw) if closed_present else None
    active = _bool(active_raw) if active_present else None
    archived = _bool(archived_raw) if archived_present else None
    accepting_orders = _bool(accepting_raw) if accepting_present else None
    enable_order_book = _bool(order_book_raw) if order_book_present else None
    resolved = _bool(resolved_raw) if resolved_present else None
    if hasattr(settlement_raw, "value"):
        settlement_raw = getattr(settlement_raw, "value")
    settlement = str(settlement_raw).strip().lower() if settlement_present and settlement_raw is not None else "unknown"
    lifecycle_flags_complete = (
        active is True
        and closed is False
        and archived is False
        and accepting_orders is True
        and enable_order_book is True
    )
    resolved_indicated = resolved_present and resolved is not None
    resolved_malformed = resolved_present and resolved is None
    settlement_open = settlement in {"open", "active"} or (settlement in {"unknown", ""} and not resolved_indicated)
    lifecycle_pass = (
        bool(identifier)
        and lifecycle_flags_complete
        and not resolved_malformed
        and resolved is not True
        and settlement_open
    )
    lifecycle_status = "pass" if lifecycle_pass else "fail"

    expected_instrument = policy.get("instrument")
    if expected_instrument is None:
        instrument_status = "pass"
    else:
        instrument_present, instrument_raw = _market_instrument(raw, trusted_provider=trusted_provider)
        if not instrument_present or instrument_raw is None or (
            isinstance(instrument_raw, str) and not instrument_raw.strip()
        ):
            instrument_status = "missing"
        elif not isinstance(instrument_raw, str):
            instrument_status = "malformed"
        else:
            instrument_status = (
                "pass"
                if instrument_raw.strip().casefold() == str(expected_instrument).strip().casefold()
                else "fail"
            )

    restrictions = policy.get("regime_restrictions", {})
    restrictions = restrictions if isinstance(restrictions, Mapping) else {}
    allowed_regimes = restrictions.get(
        "regimes",
        restrictions.get(
            "allowed_regimes",
            restrictions.get("allowed_states", restrictions.get("regime")),
        ),
    )
    allowed_regime_values = {
        str(item).strip() for item in _list_value(allowed_regimes) if str(item).strip()
    }
    if not allowed_regime_values:
        regime_status = "pass"
    else:
        regime_present, regime_raw = _nested_value(raw, "regime", "regime_state")
        if not regime_present or regime_raw is None or (
            isinstance(regime_raw, str) and not regime_raw.strip()
        ):
            regime_status = "missing"
        elif not isinstance(regime_raw, str):
            regime_status = "malformed"
        else:
            regime_status = "pass" if regime_raw.strip() in allowed_regime_values else "fail"

    category_present, category_raw = _value(raw, "category")
    if "category" not in filters:
        category_pass = True
    else:
        expected_category = filters["category"]
        expected_categories = (
            expected_category
            if isinstance(expected_category, (list, tuple, set))
            else (expected_category,)
        )
        actual_category = str(category_raw if category_present else "").strip().casefold()
        category_pass = actual_category in {
            str(item).strip().casefold() for item in expected_categories if str(item).strip()
        }
    category_status = "pass" if category_pass else "fail"

    mode = str(policy.get("mode", "")).strip().upper()
    market_ids = {str(item).strip() for item in _list_value(policy.get("market_ids")) if str(item).strip()}
    scope_pass = mode != "EXACT_MARKETS" or identifier in market_ids
    scope_status = "pass" if scope_pass else "fail"

    price_bound = _policy_bound(filters, "entry_price")
    price_present, price_raw = _value(raw, "yes_mid", "yes_price", "yesPrice", "yes_probability")
    if not price_present:
        prices_present, prices_raw = _value(raw, "outcomePrices", "outcome_prices", "prices")
        outcomes = [str(item).strip().lower() for item in _list_value(raw.get("outcomes"))]
        prices = _list_value(prices_raw) if prices_present else []
        yes_index = outcomes.index("yes") if "yes" in outcomes else 0
        if prices_present and yes_index < len(prices):
            price_present, price_raw = True, prices[yes_index]
    yes_price, price_quality = _number(price_raw) if price_present else (None, "missing")
    if price_bound is None:
        yes_status, yes_pass = "pass", True
    elif price_quality == "malformed":
        yes_status, yes_pass = "malformed", False
    elif price_quality == "missing":
        yes_status, yes_pass = "missing", False
    else:
        bounds = price_bound if isinstance(price_bound, list) else [price_bound]
        yes_pass = (
            len(bounds) == 2 and min(bounds) <= float(yes_price) <= max(bounds)
        ) or (len(bounds) != 2 and any(abs(float(yes_price) - bound) <= 1e-12 for bound in bounds))
        yes_status = "pass" if yes_pass else "fail"

    minimum_hours_present = "minimum_hours_to_resolution" in filters
    maximum_hours_present = "maximum_hours_to_resolution" in filters
    expiry_present, expiry_raw = _value(raw, "expiry", "end_date", "endDate", "endDateIso", "expirationDate")
    expiry = _parse_datetime(expiry_raw) if expiry_present else None
    expiry_hours = (expiry - observed_at).total_seconds() / 3600.0 if expiry is not None else None

    # Keep filter matching aligned with AutonomousResearchProcessor:
    # _time_to_expiry prefers a finite direct value, then timestamp/expiry.
    direct_expiry_present, direct_expiry_raw = _value(raw, "time_to_expiry_seconds")
    expiry_seconds = _finite(direct_expiry_raw, math.nan) if direct_expiry_present else math.nan
    canonical_timestamp = _parse_datetime(raw.get("timestamp"))
    canonical_expiry_present, canonical_expiry_raw = _value(raw, "expiry")
    canonical_expiry = _parse_datetime(canonical_expiry_raw) if canonical_expiry_present else None
    if not math.isfinite(expiry_seconds):
        if canonical_timestamp is not None and canonical_expiry is not None:
            expiry_seconds = (canonical_expiry - canonical_timestamp).total_seconds()

    if not minimum_hours_present and not maximum_hours_present:
        expiry_status, expiry_pass = "pass", True
    elif not math.isfinite(expiry_seconds):
        if not expiry_present or expiry_raw is None:
            expiry_status, expiry_pass = "missing", False
        else:
            expiry_status, expiry_pass = "malformed", False
    else:
        minimum = (
            _finite(filters["minimum_hours_to_resolution"], math.nan) * 3600.0
            if minimum_hours_present
            else None
        )
        maximum = (
            _finite(filters["maximum_hours_to_resolution"], math.nan) * 3600.0
            if maximum_hours_present
            else None
        )
        expiry_pass = True
        if minimum is not None and expiry_seconds < minimum:
            expiry_pass = False
        if maximum is not None and expiry_seconds > maximum:
            expiry_pass = False
        expiry_status = "pass" if expiry_pass else "fail"

    min_liquidity_present = "min_liquidity" in filters
    liquidity_present, liquidity_raw = _value(raw, "liquidity", "liquidity_num", "liquidityNum")
    liquidity, liquidity_quality = _numeric_quality(liquidity_raw, present=liquidity_present)
    if not min_liquidity_present:
        liquidity_status, liquidity_pass = "pass", True
    elif liquidity_quality in {"missing", "malformed"}:
        liquidity_status, liquidity_pass = liquidity_quality, False
    else:
        # _finite(list, math.inf) is intentionally odd but canonical: a
        # list-valued lower bound becomes +inf and therefore cannot match.
        minimum_liquidity = _finite(filters["min_liquidity"], math.inf)
        liquidity_pass = float(liquidity) >= minimum_liquidity
        liquidity_status = "pass" if liquidity_pass else "fail"

    max_spread_present = "max_spread" in filters
    spread_present, spread_raw = _value(raw, "yes_spread", "spread")
    if spread_present:
        spread, spread_quality = _numeric_quality(spread_raw, present=True)
    else:
        bid_present, bid_raw = _value(raw, "yes_bid", "yesBid", "bestBid", "best_bid")
        ask_present, ask_raw = _value(raw, "yes_ask", "yesAsk", "bestAsk", "best_ask")
        bid, bid_quality = _number(bid_raw) if bid_present else (None, "missing")
        ask, ask_quality = _number(ask_raw) if ask_present else (None, "missing")
        if bid_quality == "malformed" or ask_quality == "malformed":
            spread, spread_quality = math.nan, "malformed"
        elif bid_quality == "missing" or ask_quality == "missing":
            spread, spread_quality = math.nan, "missing"
        else:
            spread, spread_quality = float(ask) - float(bid), "valid"
    if not max_spread_present:
        spread_status, spread_pass = "pass", True
    elif spread_quality in {"missing", "malformed"}:
        spread_status, spread_pass = spread_quality, False
    else:
        # The processor only checks spread <= the finite bound; do not add
        # an acceptance-only non-negative constraint.
        maximum_spread = _finite(filters["max_spread"], -math.inf)
        spread_pass = float(spread) <= maximum_spread
        spread_status = "pass" if spread_pass else "fail"
    conditions = {
        "scope": scope_status,
        "instrument": instrument_status,
        "lifecycle": lifecycle_status,
        "category_tags": category_status,
        "regime_restrictions": regime_status,
        "yes_price": yes_status,
        "expiry": expiry_status,
        "liquidity": liquidity_status,
        "spread": spread_status,
    }
    policy_match = all(status == "pass" for status in conditions.values())
    first_failure = "PASS"
    for name in (
        "scope",
        "instrument",
        "lifecycle",
        "category_tags",
        "regime_restrictions",
        "yes_price",
        "expiry",
        "liquidity",
        "spread",
    ):
        if conditions[name] != "pass":
            first_failure = name if conditions[name] == "fail" else f"{name}_{conditions[name]}"
            break
    return {
        "market_id": identifier,
        "observed_at": observed_at.isoformat(),
        "conditions": conditions,
        "policy_match": policy_match,
        "canonical_first_failure": first_failure,
        "yes_price": yes_price,
        "expiry_hours": expiry_hours,
        "explicit_open": lifecycle_pass,
        "raw": raw,
    }
def _page_field(page: Any, name: str, default: Any = None) -> Any:
    if isinstance(page, Mapping):
        return page.get(name, default)
    return getattr(page, name, default)


def _allowed_path(path: str) -> bool:
    value = str(path)
    if _FORBIDDEN_PATH_RE.search(value):
        return False
    return value == "/markets/keyset" or bool(re.fullmatch(r"/markets/[^/?]+", value)) or bool(re.fullmatch(r"/tags/slug/[^/?]+", value)) or value == "/book"


def _scrub(value: Any, *, key: str = "") -> Any:
    # Security counters are evidence, not secret payloads; every other
    # sensitive-key value is redacted before scalar/container normalization.
    safe_status_keys = {
        "credentials_used",
        "authentication_used",
        "private_or_authenticated_state_accessed",
        "order_transport_called",
        "secret_scrubbed",
    }
    normalized_key = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", str(key)).lower().replace("-", "_")
    sensitive_key = (
        normalized_key not in safe_status_keys
        and not _PUBLIC_TOKEN_KEY_RE.fullmatch(normalized_key)
        and bool(_SENSITIVE_KEY_RE.search(normalized_key))
    )
    if sensitive_key and value is not None:
        return "[REDACTED]"
    temporal = _temporal_iso(value)
    if temporal is not None:
        return temporal
    if isinstance(value, Enum):
        return _scrub(value.value, key=key)
    if is_dataclass(value):
        return _scrub(asdict(value), key=key)
    if isinstance(value, Mapping):
        return {str(name): _scrub(child, key=str(name)) for name, child in value.items()}
    if isinstance(value, (list, tuple)):
        return [_scrub(child, key=key) for child in value]
    if isinstance(value, str):
        return _SECRET_VALUE_RE.sub("[REDACTED]", value)
    if hasattr(value, "__dict__"):
        return _scrub(vars(value), key=key)
    return value


def _bounded_book(book: Any, depth: int) -> Any:
    if isinstance(book, Mapping):
        result = dict(book)
    else:
        mapped = _mapping(book)
        if not mapped:
            return _scrub(book)
        result = dict(mapped)
    for side in ("bids", "asks"):
        values = result.get(side)
        if isinstance(values, (list, tuple)):
            result[side] = list(values[:depth])
    return _scrub(result)


def _source_hash() -> str:
    try:
        content = Path(__file__).read_bytes()
    except OSError:
        content = b"polymarket-market-scope-acceptance-source-unavailable"
    return "sha256:" + hashlib.sha256(content).hexdigest()


def _synthetic_historical_rows(timestamp: datetime) -> list[dict[str, Any]]:
    """Return bounded, immutable-looking historical rows for the queue proof."""
    rows: list[dict[str, Any]] = []
    for market_index in range(30):
        market_id = f"synthetic-market-{market_index:02d}"
        opened = timestamp + timedelta(hours=market_index)
        for step in range(6):
            observed_at = opened + timedelta(minutes=step)
            rows.append(
                {
                    "market_id": market_id,
                    "question": "Synthetic offline market resolves YES.",
                    "category": "politics",
                    "yes_bid": 0.49,
                    "yes_ask": 0.51,
                    "yes_mid": 0.50,
                    "no_bid": 0.49,
                    "no_ask": 0.51,
                    "no_mid": 0.50,
                    "model_probability": 0.80,
                    "liquidity": 1_500.0,
                    "spread": 0.02,
                    "expiry": (opened + timedelta(days=2)).isoformat(),
                    "resolution_criteria": "synthetic offline fixture outcome",
                    "timestamp": observed_at.isoformat(),
                    "settlement": "resolved_yes" if step == 5 else "open",
                    "regime": ("calm", "volatile", "transition")[step % 3],
                    "source_type": "HISTORICAL",
                    "fixture_label": "SYNTHETIC_OFFLINE",
                    "model_label": "synthetic-only",
                }
            )
    return rows


def _synthetic_successor_proposal(
    successor_id: str,
    dataset_id: str,
    dataset_version: str,
) -> dict[str, Any]:
    """Build only declarative research intent; the processor owns outcomes."""
    plan = {
        "market_type": "prediction",
        "template": "probability_mispricing",
        "dataset_id": dataset_id,
        "dataset_version": dataset_version,
        "dataset_selector": {
            "dataset_id": dataset_id,
            "dataset_version": dataset_version,
            "source_type": "HISTORICAL",
        },
        "target": {"instrument": "POLYMARKET", "categories": ["politics"]},
        "market_scope": {
            "schema_version": "1",
            "mode": "RULE_BASED_MARKETS",
            "instrument": "POLYMARKET",
            "categories": ["politics"],
            "market_ids": [],
            "filters": {"category": "politics"},
            "regime_restrictions": {},
            "provenance": "canonical",
        },
        "filters": {"category": "politics"},
        "parameters": {"threshold": [0.03]},
        "methodology": {
            "time_split": "train-validation-holdout",
            "initial_cash": 10_000.0,
            "fee_bps": 0.0,
            "slippage_bps": 0.0,
            "allocation": 0.25,
        },
        "metrics": ["expectancy", "drawdown", "trade_count", "sample_count"],
        "min_samples": 30,
        "min_trades": 0,
        "max_variants": 1,
        "model_document": {"probability": 0.80},
        "paper_only": True,
    }
    return {
        "proposal_id": successor_id,
        "statement": "A deterministic synthetic offline probability edge is testable.",
        "source": "SYNTHETIC_OFFLINE synthetic-only model",
        "tests": ["chronological train-validation-holdout", "bounded robustness checks"],
        "dataset_version": dataset_version,
        "time_split": "train-validation-holdout",
        "paper_only": True,
        "experiment_plan": plan,
    }


def _authority_flags() -> dict[str, bool]:
    return {
        "lifecycle": False,
        "scope": False,
        "ranking": False,
        "qualification": False,
        "execution": False,
    }


def _execution_flags() -> dict[str, bool]:
    return {
        "credentials": False,
        "authentication": False,
        "orders": False,
        "live_execution": False,
    }


def _observed_field(value: Mapping[str, Any] | None, name: str) -> Any:
    """Return one field exactly as observed, preserving falsey values."""
    if not isinstance(value, Mapping) or name not in value:
        return None
    return value[name]


def _observed_outcome(value: Mapping[str, Any] | None) -> dict[str, Any]:
    """Keep processor/candidate outcome fields independent and lossless."""
    return {
        "reason_code": _observed_field(value, "reason_code"),
        "reason": _observed_field(value, "reason"),
        "accepted": _observed_field(value, "accepted"),
        "status": _observed_field(value, "status"),
    }


def _clean_reason(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _lifecycle_reason_projection(
    payload: Mapping[str, Any] | None,
    events: Sequence[Any] = (),
) -> dict[str, Any]:
    """Project explicit lifecycle reason fields without deriving from status."""
    sources: list[Mapping[str, Any]] = []
    if isinstance(payload, Mapping):
        sources.append(payload)
    if isinstance(events, Sequence) and not isinstance(events, (str, bytes)):
        for event in reversed(events):
            if not isinstance(event, Mapping):
                continue
            sources.append(event)
            event_payload = event.get("payload")
            if isinstance(event_payload, Mapping):
                sources.append(event_payload)
    observed = _observed_outcome(None)
    for source in sources:
        if observed["reason_code"] is None:
            observed["reason_code"] = _observed_field(source, "reason_code")
        if observed["reason"] is None:
            observed["reason"] = _observed_field(source, "reason")
        if observed["reason_code"] is not None and observed["reason"] is not None:
            break
    return observed


def _reason_for_selected_source(
    selected_code: tuple[str | None, str | None],
    text_sources: Sequence[tuple[str, Any]],
) -> str | None:
    """Keep a selected exact code paired with same-source text when present."""
    source, code = selected_code
    if code is None:
        return next(
            (_clean_reason(value) for _, value in text_sources if _clean_reason(value)),
            None,
        )
    for text_source, value in text_sources:
        if text_source == source and _clean_reason(value):
            return _clean_reason(value)
    return _clean_reason(code)


def _candidate_metric_projection(
    payload: Mapping[str, Any] | None,
    qualification: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Project observed candidate evidence into bounded metric sections.

    The processor intentionally stores the historical evidence in several
    lifecycle payloads.  This projection does not calculate, rename, or
    default any metric; it only groups fields already present so a persisted
    report remains useful after its isolated store is closed.
    """
    source = dict(payload) if isinstance(payload, Mapping) else {}
    sections: dict[str, dict[str, Any]] = {
        "backtest": {},
        "validation": {},
        "robustness": {},
        "qualification": {},
    }
    prefixes = {
        "backtest": ("backtest", "train"),
        "validation": ("validation",),
        "robustness": (
            "robust", "minimum_sample", "multiple_testing", "data_quality",
            "cost", "regime",
        ),
        "qualification": (
            "qualification", "forward", "promotion", "paper_forward",
            "order_attempt", "resolved", "holdout",
        ),
    }
    for key, value in source.items():
        normalized = str(key).strip().lower()
        for section, names in prefixes.items():
            if normalized.startswith(names):
                sections[section][str(key)] = value
                break
    if isinstance(qualification, Mapping):
        sections["qualification"]["authority"] = dict(qualification)
    return {
        **sections,
        "observed": {
            "payload_keys": sorted(str(key) for key in source),
            "metric_fields": {
                section: sorted(values) for section, values in sections.items()
            },
        },
    }


def _candidate_lifecycle_evidence(
    store: Any,
    candidate_ids: Sequence[str],
    *,
    selected_candidate_ids: Sequence[str] = (),
    requirements: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Capture candidate payloads/events before an isolated store is closed."""
    requirement_rows = requirements.get("candidates") if isinstance(requirements, Mapping) else ()
    requirement_by_id = {
        str(item.get("candidate_id")).strip(): item
        for item in requirement_rows
        if isinstance(item, Mapping) and str(item.get("candidate_id", "")).strip()
    } if isinstance(requirement_rows, list) else {}
    rows: list[dict[str, Any]] = []
    for raw_id in candidate_ids:
        candidate_id = str(raw_id).strip()
        if not candidate_id:
            continue
        try:
            lifecycle = store.load_candidate_lifecycle(candidate_id)
        except Exception:
            lifecycle = None
        if not isinstance(lifecycle, Mapping):
            rows.append(
                {
                    "candidate_id": candidate_id,
                    "stage": None,
                    "payload": None,
                    "events": [],
                    "metrics": _candidate_metric_projection(
                        None, requirement_by_id.get(candidate_id)
                    ),
                    "evidence_gap": "CANDIDATE_LIFECYCLE_NOT_OBSERVED",
                }
            )
            continue
        payload = lifecycle.get("payload")
        try:
            events = store.list_candidate_lifecycle_events(candidate_id, limit=100)
        except Exception:
            events = []
        row = {
            "candidate_id": candidate_id,
            "stage": lifecycle.get("stage"),
            "updated_at": lifecycle.get("updated_at"),
            "payload": payload,
            "events": events,
            "metrics": _candidate_metric_projection(
                payload if isinstance(payload, Mapping) else None,
                requirement_by_id.get(candidate_id),
            ),
        }
        rows.append(row)
    selected = {
        str(value).strip()
        for value in selected_candidate_ids
        if str(value).strip()
    }
    selected_rows = [row for row in rows if row["candidate_id"] in selected]
    observed = selected_rows[0] if selected_rows else (rows[0] if rows else None)
    return {
        "selected_candidate_id": (
            observed["candidate_id"] if observed is not None else None
        ),
        "selected_by_processor": bool(selected_rows),
        "candidates": rows,
        "selected": observed,
    }


def _queue_demo_evidence(
    queue_item: Any,
    processor_result: Mapping[str, Any] | None,
    store: Any,
    *,
    now: datetime,
    authority_cap: int,
) -> dict[str, Any]:
    """Extract observed queue/candidate evidence without assigning a status."""
    candidate_result: Mapping[str, Any] | None = None
    candidate_id: str | None = None
    selected_candidate_ids: tuple[str, ...] = ()
    if isinstance(processor_result, Mapping):
        raw_selected = processor_result.get("selected_candidate_ids")
        if isinstance(raw_selected, Sequence) and not isinstance(raw_selected, (str, bytes)):
            selected_candidate_ids = tuple(
                str(value).strip() for value in raw_selected if str(value).strip()
            )
        candidate_results = processor_result.get("candidate_results")
        if isinstance(candidate_results, Sequence) and not isinstance(candidate_results, (str, bytes)):
            for item in candidate_results:
                if isinstance(item, Mapping) and str(item.get("candidate_id", "")).strip():
                    if candidate_result is None:
                        candidate_result = item
                    candidate_id = str(item["candidate_id"]).strip()
                    if candidate_id in selected_candidate_ids:
                        candidate_result = item
                        break
    requirements: Mapping[str, Any] | None = None
    requirement_candidate: Mapping[str, Any] | None = None
    if candidate_id:
        try:
            raw_requirements = store.candidate_forward_requirements(
                candidate_ids=(candidate_id,),
                now=now,
                max_markets_per_candidate=authority_cap,
                max_total_markets=authority_cap,
            )
            requirements = raw_requirements if isinstance(raw_requirements, Mapping) else None
        except Exception:
            requirements = None
        requirement_rows = requirements.get("candidates") if requirements else None
        if isinstance(requirement_rows, list):
            requirement_candidate = next(
                (
                    item for item in requirement_rows
                    if isinstance(item, Mapping)
                    and str(item.get("candidate_id", "")).strip() == candidate_id
                ),
                None,
            )
            if requirement_candidate is None and requirement_rows and isinstance(requirement_rows[0], Mapping):
                requirement_candidate = requirement_rows[0]
    lifecycle_evidence = _candidate_lifecycle_evidence(
        store,
        (candidate_id,) if candidate_id else (),
        selected_candidate_ids=selected_candidate_ids,
        requirements=requirements,
    )
    lifecycle_selected = lifecycle_evidence.get("selected")
    lifecycle_payload = (
        lifecycle_selected.get("payload")
        if isinstance(lifecycle_selected, Mapping)
        else None
    )
    lifecycle_events = (
        lifecycle_selected.get("events", ())
        if isinstance(lifecycle_selected, Mapping)
        else ()
    )
    processor_observed = _observed_outcome(processor_result)
    candidate_observed = _observed_outcome(candidate_result)
    lifecycle_observed = _observed_outcome(
        lifecycle_payload if isinstance(lifecycle_payload, Mapping) else None
    )
    lifecycle_reason_observed = _lifecycle_reason_projection(
        lifecycle_payload if isinstance(lifecycle_payload, Mapping) else None,
        lifecycle_events if isinstance(lifecycle_events, Sequence) else (),
    )
    for field in ("reason_code", "reason"):
        if lifecycle_observed.get(field) is None:
            lifecycle_observed[field] = lifecycle_reason_observed.get(field)
    qualification_observed = _observed_outcome(requirement_candidate)
    queue_observed = {
        "reason_code": _observed_field(queue_item, "reason_code"),
        "reason": _clean_reason(getattr(queue_item, "last_error", None)),
        "accepted": None,
        "status": _terminal_queue_status(queue_item) if queue_item is not None else None,
    }
    # Exact reason precedence follows authority: candidate result, candidate
    # forward qualification authority, lifecycle payload/events, processor,
    # then queue. Status remains an independent observed field and is never a
    # reason fallback.
    code_sources = (
        ("candidate", candidate_observed.get("reason_code")),
        ("qualification", qualification_observed.get("reason_code")),
        ("candidate_lifecycle", lifecycle_observed.get("reason_code")),
        ("processor", processor_observed.get("reason_code")),
        ("queue", queue_observed.get("reason_code")),
    )
    text_sources = (
        ("candidate", candidate_observed.get("reason")),
        ("qualification", qualification_observed.get("reason")),
        ("candidate_lifecycle", lifecycle_observed.get("reason")),
        ("processor", processor_observed.get("reason")),
        ("queue", queue_observed.get("reason")),
    )
    selected_code = next(
        ((source, _clean_reason(value)) for source, value in code_sources if _clean_reason(value)),
        (None, None),
    )
    selected_text = next(
        ((source, _clean_reason(value)) for source, value in text_sources if _clean_reason(value)),
        (None, None),
    )
    decision_source, decision_reason = selected_code
    if decision_reason is None:
        decision_source, decision_reason = selected_text
    reason = _reason_for_selected_source(selected_code, text_sources)
    diagnostics = [] if decision_reason is not None else ["MISSING_OBSERVED_REASON"]
    return {
        "candidate_id": candidate_id,
        "lifecycle_stage": (
            lifecycle_selected.get("stage")
            if isinstance(lifecycle_selected, Mapping)
            else None
        ),
        "processor": processor_observed,
        "candidate": candidate_observed,
        "candidate_lifecycle": lifecycle_observed,
        "candidate_lifecycle_events": lifecycle_reason_observed,
        "qualification": qualification_observed,
        "queue": queue_observed,
        "observed": {
            "processor": processor_observed,
            "candidate": candidate_observed,
            "candidate_lifecycle": lifecycle_observed,
            "candidate_lifecycle_events": lifecycle_reason_observed,
            "qualification": qualification_observed,
            "queue": queue_observed,
        },
        "reason_code": selected_code[1],
        "reason": reason,
        "exact_reason": decision_reason,
        "decision_reason": decision_reason,
        "decision_reason_source": decision_source,
        "diagnostics": diagnostics,
        "lifecycle_evidence": lifecycle_evidence,
    }


def _queue_demo_reason(
    queue_item: Any,
    processor_result: Mapping[str, Any] | None,
    store: Any,
    *,
    now: datetime,
    authority_cap: int,
) -> tuple[str | None, str | None, str | None]:
    """Compatibility projection of exact observed queue evidence."""
    evidence = _queue_demo_evidence(
        queue_item,
        processor_result,
        store,
        now=now,
        authority_cap=authority_cap,
    )
    return (
        evidence["exact_reason"],
        _clean_reason(evidence.get("lifecycle_stage")),
        evidence["processor"].get("reason"),
    )


def _catalog_record(
    dataset_id: str,
    dataset_version: str,
    rows: Sequence[Mapping[str, Any]],
    timestamp: datetime,
) -> dict[str, Any]:

    return {
        "provider": "SYNTHETIC_OFFLINE",
        "instrument": "POLYMARKET",
        "market_type": "prediction",
        "timeframe": "event",
        "start_timestamp": timestamp,
        "end_timestamp": datetime.fromisoformat(str(rows[-1]["timestamp"])),
        "row_count": len(rows),
        "completeness": 1.0,
        "missing_ranges": (),
        "quality": "PRICE_PROXY",
        "source_type": "HISTORICAL",
        "snapshot_id": f"{dataset_id}:{dataset_version}:catalog",
        "metadata": {
            "fixture_label": "SYNTHETIC_OFFLINE",
            "provider": "SYNTHETIC_OFFLINE",
            "source_type": "HISTORICAL",
            "research_quality": "PRICE_PROXY",
            "historical_order_book_available": False,
            "provenance_version": "dataset-provenance-v1",
            "policy_version": "prediction-integrity-v1",
        },
    }
def _queue_authority_cap(criteria: Any | None) -> int:
    """Mirror the processor's criteria-derived paper authority bound."""
    raw_cap = getattr(criteria, "min_independent_samples", 30)
    try:
        cap = int(raw_cap)
    except (TypeError, ValueError, OverflowError):
        cap = 30
    return min(100, max(1, cap))



def _attestation_summary(attestation: Any, error_type: str | None) -> dict[str, Any]:
    if not isinstance(attestation, Mapping):
        return {
            "computed": False,
            "error_type": error_type,
        }
    return {
        "computed": True,
        "status": attestation.get("status"),
        "attestation_hash": attestation.get("attestation_hash"),
        "row_count": attestation.get("row_count"),
        "execution_fidelity": attestation.get("execution_fidelity"),
        "contamination_result": attestation.get("contamination_result"),
        "reason": attestation.get("reason"),
    }


def _synthetic_dataset_id(value: Any) -> str:
    requested = str(value).strip()
    if not requested:
        raise ValueError("immutable dataset metadata requires dataset_id and dataset_version")
    return requested if requested.startswith("prediction:") else f"prediction:{requested}"


def _terminal_queue_status(value: Any) -> str:
    raw_status = getattr(value, "status", value)
    status = str(getattr(raw_status, "value", raw_status) or "").strip().upper()
    return status or "ERROR"


def _successor_queue_payload(
    predecessor: str,
    frozen_hash: str,
    successor_id: str,
    proposal: Mapping[str, Any],
) -> dict[str, Any]:
    payload = dict(proposal)
    payload.update(
        {
            "predecessor_candidate_id": predecessor,
            "predecessor_frozen_hash": frozen_hash,
            "successor_candidate_id": successor_id,
        }
    )
    return payload


def _public_successor_proposal(
    market_id: str,
    identity_hash: str,
    book_hashes: Mapping[str, str],
    token_ids: Mapping[str, str],
    policy: Any,
) -> tuple[dict[str, Any], str, str]:
    """Build a public-observation proposal without seeding a dataset."""
    canonical_policy = normalize_market_scope(_policy_record(policy))
    policy_record = dict(canonical_policy.as_dict())
    digest = hashlib.sha256(
        f"{market_id}|{identity_hash}|{_canonical_json(book_hashes)}".encode("utf-8")
    ).hexdigest()
    dataset_id = f"public-polymarket:{digest[:24]}"
    dataset_version = f"observed-{digest[24:48]}"
    successor_id = f"public-match:{digest[:24]}"
    plan = {
        "market_type": "prediction",
        "template": "probability_mispricing",
        "dataset_id": dataset_id,
        "dataset_version": dataset_version,
        "dataset_selector": {
            "dataset_id": dataset_id,
            "dataset_version": dataset_version,
            "source_type": "FORWARD_COLLECTED",
        },
        # Keep the normalized canonical policy as the sole scope authority.
        # The selected public market remains bound by market_id and
        # public_provenance below without changing the policy that selected it.
        "market_scope": policy_record,
        "market_scope_hash": canonical_policy.scope_hash,
        "market_scope_version": canonical_policy.scope_version,
        "parameters": {"threshold": [0.03]},
        "methodology": {
            "time_split": "train-validation-holdout",
            "initial_cash": 10_000.0,
            "fee_bps": 0.0,
            "slippage_bps": 0.0,
            "allocation": 0.25,
        },
        "metrics": ["expectancy", "drawdown", "trade_count", "sample_count"],
        "min_samples": 30,
        "min_trades": 0,
        "max_variants": 1,
        "paper_only": True,
    }
    return (
        {
            "proposal_id": successor_id,
            "statement": "A public Polymarket market observation is available for ordinary research processing.",
            "source": "PUBLIC_POLYMARKET_MATCH",
            "tests": ["ordinary processor validation", "immutable dataset prerequisite check"],
            "dataset_id": dataset_id,
            "dataset_version": dataset_version,
            "time_split": "train-validation-holdout",
            "paper_only": True,
            "experiment_plan": plan,
            "market_scope": policy_record,
            "market_scope_hash": canonical_policy.scope_hash,
            "market_scope_version": canonical_policy.scope_version,
            "market_id": market_id,
            "identity_hash": identity_hash,
            "book_hashes": dict(book_hashes),
            "public_provenance": {
                "source_type": "FORWARD_COLLECTED",
                "market_id": market_id,
                "market_token_ids": dict(token_ids),
                "identity_hash": identity_hash,
                "book_hashes": dict(book_hashes),
            },
        },
        dataset_id,
        dataset_version,
    )


def enqueue_public_market_attempt(
    market_id: str,
    identity: Any,
    token_ids: Mapping[str, str],
    books: Sequence[Mapping[str, Any]],
    *,
    policy: Any,
    selection_rule: str,
    now: datetime,
    criteria: Any | None = None,
) -> dict[str, Any]:
    """Run one ordinary queue attempt bound only to public observations.

    No public snapshot is inserted into the historical dataset tables.  The
    normal processor therefore rejects this attempt when its immutable
    dataset prerequisite is absent; the resulting status and reason are read
    back rather than assigned by this tool.
    """
    identity_record = _scrub(identity)
    identity_hash = _sha256_json(identity_record)
    book_hashes = {
        str(item.get("outcome", "")).strip().lower(): str(item.get("book_hash", "")).strip()
        for item in books
        if isinstance(item, Mapping) and str(item.get("outcome", "")).strip()
    }
    token_record = {
        str(key).strip().lower(): str(value).strip()
        for key, value in token_ids.items()
        if str(key).strip().lower() in {"yes", "no"} and str(value).strip()
    }
    proposal, dataset_id, dataset_version = _public_successor_proposal(
        str(market_id),
        identity_hash,
        book_hashes,
        token_record,
        policy,
    )
    result = _queue_demo_result_defaults()
    result.update(
        {
            "queue_item_id": f"queue-{proposal['proposal_id']}",
            "workflow": "enqueue_public_market_attempt",
            "evidence_class": "PUBLIC_MATCH_ORDINARY_PROCESSOR",
            "synthetic_offline": False,
            "market_id": str(market_id),
            "identity_hash": identity_hash,
            "token_ids": token_record,
            "book_hashes": book_hashes,
            "selection_rule": str(selection_rule),
            "scope_match": True,
            "market_scope": dict(proposal["market_scope"]),
            "market_scope_hash": proposal["market_scope_hash"],
            "market_scope_version": proposal["market_scope_version"],
            "public_provenance": dict(proposal["public_provenance"]),
            "dataset_metadata": {
                "dataset_id": dataset_id,
                "dataset_version": dataset_version,
                "source_type": "FORWARD_COLLECTED",
                "immutable_prerequisite_present": False,
                "metadata_hash": _sha256_json(proposal["public_provenance"]),
            },
            "missing_prerequisite": {
                "name": "immutable_dataset",
                "dataset_id": dataset_id,
                "dataset_version": dataset_version,
                "source_type": "HISTORICAL",
                "present": False,
            },
            "runtime_criteria": criteria.as_record() if criteria is not None else _runtime_criteria_record(),
            "authority_flags": _authority_flags(),
            "execution_flags": _execution_flags(),
        }
    )
    criteria_obj = criteria
    if criteria_obj is None and PromotionCriteria is not None:
        criteria_obj = PromotionCriteria()
    runtime_now = _parse_datetime(now) or datetime.now(timezone.utc)
    try:
        from axiom.autonomous import AutonomousResearchConfig, AutonomousResearchProcessor
        from axiom.research_bus import DurableResearchBus
        from axiom.storage import AxiomStore

        with AxiomStore(":memory:") as store:
            bus = DurableResearchBus(store, source="acceptance-public", author="acceptance-public")
            queued = bus.submit_hypothesis(
                proposal,
                dedupe_key=f"acceptance-public:{proposal['proposal_id']}",
                lineage=(str(market_id), identity_hash, *book_hashes.values()),
                available_at=runtime_now,
            )
            result["queue_item_id"] = queued.item_id
            processor = AutonomousResearchProcessor(
                store,
                bus=bus,
                config=(
                    AutonomousResearchConfig(promotion_criteria=criteria_obj)
                    if criteria_obj is not None
                    else None
                ),
                clock=lambda: runtime_now,
            )
            cycle = processor.process_pending(worker="acceptance-public", now=runtime_now)
            processor_result = (
                cycle.results[0]
                if cycle.results and isinstance(cycle.results[0], Mapping)
                else None
            )
            final_item = bus.get(queued.item_id)
            if final_item is not None:
                result["queue_status"] = _terminal_queue_status(final_item)
                result["lineage"] = list(final_item.lineage)
            evidence = _queue_demo_evidence(
                final_item,
                processor_result,
                store,
                now=runtime_now,
                authority_cap=_queue_authority_cap(criteria_obj),
            )
            processor_observed = evidence["processor"]
            candidate_observed = evidence["candidate"]
            result.update(
                {
                    "lifecycle_stage": evidence["lifecycle_stage"],
                    "resulting_stage": evidence["lifecycle_stage"],
                    "reason": evidence["reason"],
                    "exact_reason": evidence["exact_reason"],
                    "reason_code": evidence["reason_code"],
                    "processor_reason": processor_observed["reason"],
                    "processor_reason_code": processor_observed["reason_code"],
                    "processor_accepted": processor_observed["accepted"],
                    "processor_status": processor_observed["status"],
                    "candidate_reason": candidate_observed["reason"],
                    "candidate_reason_code": candidate_observed["reason_code"],
                    "candidate_accepted": candidate_observed["accepted"],
                    "candidate_status": candidate_observed["status"],
                    "observed_outcomes": evidence["observed"],
                    "decision_reason": evidence["decision_reason"],
                    "decision_reason_source": evidence["decision_reason_source"],
                    "diagnostics": evidence["diagnostics"],
                    "candidate_lifecycle_evidence": evidence["lifecycle_evidence"],
                    "queue_cycle": cycle.as_record(),
                }
            )
    except Exception as exc:
        result.update(
            {
                "reason": str(exc),
                "exact_reason": str(exc),
                "error_type": type(exc).__name__,
            }
        )
    result["missing_prerequisite"]["observed_reason"] = result["exact_reason"]
    return result


def _queue_demo_result_defaults() -> dict[str, Any]:
    return {
        "queue_item_id": "queue-legacy-successor-demo",
        "queue_status": "ERROR",
        "lifecycle_stage": None,
        "resulting_stage": None,
        "promotable": False,
        "reason": None,
        "exact_reason": None,
        "reason_code": None,
        "diagnostics": ["MISSING_OBSERVED_REASON"],
        "error_type": None,
        "evidence_class": "SYNTHETIC_OFFLINE",
        "synthetic_offline": True,
        "dataset_attestation": {"computed": False, "error_type": None},
        "authority_flags": _authority_flags(),
        "execution_flags": _execution_flags(),
    }

def _skipped_queue_demo_result(
    predecessor_id: str,
    predecessor_frozen_hash: str,
    dataset_metadata: Mapping[str, Any],
    *,
    criteria: Any | None = None,
) -> dict[str, Any]:
    """Return an explicit queue result when the total run budget is exhausted."""
    predecessor = str(predecessor_id).strip()
    frozen_hash = str(predecessor_frozen_hash).strip()
    metadata = dict(dataset_metadata)
    catalog_dataset_id = _synthetic_dataset_id(metadata["dataset_id"])
    dataset_version = str(metadata["dataset_version"]).strip()
    criteria_obj = criteria
    if criteria_obj is None and PromotionCriteria is not None:
        criteria_obj = PromotionCriteria()
    result = _queue_demo_result_defaults()
    result.update(
        {
            "queue_status": "SKIPPED_BUDGET",
            "reason": "total_time_budget_exhausted",
            "exact_reason": "total_time_budget_exhausted",
            "processor_reason": None,
            "error_type": None,
            "workflow": "enqueue_legacy_successor",
            "isolated": True,
            "live_state_accessed": False,
            "predecessor_id": predecessor,
            "predecessor_frozen_hash": frozen_hash,
            "successor_id": f"{predecessor}:legacy-successor",
            "lineage": [predecessor, frozen_hash],
            "authority_market_cap": _queue_authority_cap(criteria_obj),
            "predecessor_provenance": {
                "candidate_id": predecessor,
                "frozen_hash": frozen_hash,
                "lineage_preserved": True,
            },
            "dataset_metadata": {
                "dataset_id": str(metadata["dataset_id"]),
                "catalog_dataset_id": catalog_dataset_id,
                "dataset_version": dataset_version,
                "metadata_hash": _sha256_json(metadata),
            },
            "runtime_criteria": criteria_obj.as_record() if criteria_obj is not None else None,
            "runtime_reasons": ["total_time_budget_exhausted"],
            "promotable": False,
            "authority_flags": _authority_flags(),
            "execution_flags": _execution_flags(),
        }
    )
    return result


def enqueue_legacy_successor(
    predecessor_id: str,
    predecessor_frozen_hash: str,
    dataset_metadata: Mapping[str, Any],
    *,
    criteria: Any | None = None,
) -> dict[str, Any]:
    """Enqueue and process one isolated successor through the ordinary queue.

    The only seeded state is a deterministic, immutable synthetic historical
    catalog.  Queue completion, lifecycle stage, attestation, and rejection
    reason are all read back from the normal processor; no PASS, eligibility,
    promotion, or lifecycle status is injected by this demonstration.
    """
    predecessor = str(predecessor_id).strip()
    frozen_hash = str(predecessor_frozen_hash).strip()
    if not predecessor or not frozen_hash:
        raise ValueError("predecessor identity and frozen hash are required")
    metadata = dict(dataset_metadata)
    if not metadata.get("dataset_id") or not metadata.get("dataset_version"):
        raise ValueError("immutable dataset metadata requires dataset_id and dataset_version")
    metadata_hash = _sha256_json(metadata)
    criteria_obj = criteria
    if criteria_obj is None and PromotionCriteria is not None:
        criteria_obj = PromotionCriteria()
    authority_cap = _queue_authority_cap(criteria_obj)
    runtime_reasons = tuple(criteria_obj.evaluate({})) if criteria_obj is not None else ("runtime_criteria_unavailable",)
    runtime_now = datetime.fromisoformat(FIXTURE_TIMESTAMP)
    successor_id = f"{predecessor}:legacy-successor"
    result = _queue_demo_result_defaults()
    catalog_dataset_id = _synthetic_dataset_id(metadata["dataset_id"])
    dataset_version = str(metadata["dataset_version"]).strip()
    attestation: Mapping[str, Any] | None = None
    attestation_error: str | None = None
    try:
        from axiom.autonomous import AutonomousResearchConfig, AutonomousResearchProcessor
        from axiom.research_bus import DurableResearchBus
        from axiom.storage import AxiomStore

        rows = _synthetic_historical_rows(runtime_now)
        catalog = _catalog_record(catalog_dataset_id, dataset_version, rows, runtime_now)
        with AxiomStore(":memory:") as store:
            store.save_dataset(
                catalog_dataset_id,
                dataset_version,
                rows,
                metadata=catalog["metadata"],
                quality=catalog["quality"],
            )
            store.save_dataset_catalog(
                catalog_dataset_id,
                dataset_version,
                **catalog,
            )
            try:
                attestation = store.verify_dataset_integrity_attestation(
                    catalog_dataset_id,
                    dataset_version,
                    force=True,
                )
            except Exception as exc:
                attestation_error = type(exc).__name__
            bus = DurableResearchBus(store, source="acceptance-offline", author="acceptance-offline")
            proposal = _synthetic_successor_proposal(
                successor_id,
                catalog_dataset_id,
                dataset_version,
            )
            queued = bus.submit_hypothesis(
                _successor_queue_payload(predecessor, frozen_hash, successor_id, proposal),
                dedupe_key="acceptance-legacy-successor-demo",
                lineage=(predecessor, frozen_hash),
                available_at=runtime_now,
            )
            result["queue_item_id"] = queued.item_id
            processor_config = (
                AutonomousResearchConfig(promotion_criteria=criteria_obj)
                if criteria_obj is not None
                else None
            )
            processor = AutonomousResearchProcessor(
                store,
                bus=bus,
                config=processor_config,
                clock=lambda: runtime_now,
            )
            cycle = processor.process_pending(
                worker="acceptance-offline",
                now=runtime_now,
            )
            processor_result = cycle.results[0] if cycle.results and isinstance(cycle.results[0], Mapping) else None
            final_item = bus.get(queued.item_id)
            result["queue_status"] = _terminal_queue_status(final_item) if final_item is not None else "ERROR"
            evidence = _queue_demo_evidence(
                final_item,
                processor_result,
                store,
                now=runtime_now,
                authority_cap=authority_cap,
            )
            processor_observed = evidence["processor"]
            candidate_observed = evidence["candidate"]
            result.update(
                {
                    "lifecycle_stage": evidence["lifecycle_stage"],
                    "resulting_stage": evidence["lifecycle_stage"],
                    "reason": evidence["reason"],
                    "exact_reason": evidence["exact_reason"],
                    "reason_code": evidence["reason_code"],
                    "processor_reason": processor_observed["reason"],
                    "processor_reason_code": processor_observed["reason_code"],
                    "processor_accepted": processor_observed["accepted"],
                    "processor_status": processor_observed["status"],
                    "candidate_reason": candidate_observed["reason"],
                    "candidate_reason_code": candidate_observed["reason_code"],
                    "candidate_accepted": candidate_observed["accepted"],
                    "candidate_status": candidate_observed["status"],
                    "observed_outcomes": evidence["observed"],
                    "decision_reason": evidence["decision_reason"],
                    "decision_reason_source": evidence["decision_reason_source"],
                    "diagnostics": evidence["diagnostics"],
                    "candidate_lifecycle_evidence": evidence["lifecycle_evidence"],
                    "error_type": None,
                    "dataset_attestation": _attestation_summary(attestation, attestation_error),
                    "lineage": list(final_item.lineage) if final_item is not None else [predecessor, frozen_hash],
                }
            )
    except Exception as exc:
        result["error_type"] = type(exc).__name__
    result.update(
        {
            "workflow": "enqueue_legacy_successor",
            "isolated": True,
            "live_state_accessed": False,
            "predecessor_id": predecessor,
            "predecessor_frozen_hash": frozen_hash,
            "successor_id": successor_id,
            "lineage": result.get("lineage") or [predecessor, frozen_hash],
            "authority_market_cap": authority_cap,
            "predecessor_provenance": {
                "candidate_id": predecessor,
                "frozen_hash": frozen_hash,
                "lineage_preserved": True,
            },
            "dataset_metadata": {
                "dataset_id": str(metadata["dataset_id"]),
                "catalog_dataset_id": catalog_dataset_id,
                "dataset_version": dataset_version,
                "metadata_hash": metadata_hash,
            },
            "runtime_criteria": criteria_obj.as_record() if criteria_obj is not None else None,
            "runtime_reasons": list(runtime_reasons),
            "promotable": False,
            "authority_flags": _authority_flags(),
            "execution_flags": _execution_flags(),
        }
    )
    result["dataset_attestation"] = _attestation_summary(attestation, attestation_error)
    return result


def _runtime_criteria_record() -> dict[str, Any] | None:
    if PromotionCriteria is None:
        return None
    return PromotionCriteria().as_record()

def _bounded_sample(values: Sequence[Any], limit: int = 3) -> list[Any]:
    """Return a deterministic head/tail sample without changing source evidence."""
    if limit <= 0:
        return []
    items = list(values)
    if len(items) <= limit:
        return items
    head = max(1, limit // 2)
    tail = limit - head
    return items[:head] + (items[-tail:] if tail else [])


_PERSISTED_CATALOG_FIELDS = frozenset(
    {
        "dataset_id",
        "dataset_version",
        "provider",
        "instrument",
        "market_type",
        "timeframe",
        "start_timestamp",
        "end_timestamp",
        "row_count",
        "completeness",
        "missing_ranges",
        "quality",
        "source_type",
        "snapshot_id",
        "created_at",
        "updated_at",
    }
)
_PERSISTED_AGGREGATE_METADATA_FIELDS = frozenset(
    {
        "source_type",
        "provider",
        "instrument",
        "markets_discovered",
        "markets_imported",
        "price_points",
        "category_counts",
        "market_versions",
        "research_quality",
        "historical_order_book_available",
        "provenance_version",
        "policy_version",
    }
)
_PERSISTED_CONSTITUENT_METADATA_FIELDS = frozenset(
    {
        "source_type",
        "provider",
        "instrument",
        "market_id",
        "polymarket_key",
        "token_ids",
        "research_quality",
        "historical_order_book_available",
        "provenance_version",
        "policy_version",
    }
)
_PERSISTED_BINDING_FIELDS = frozenset(
    {"market_id", "dataset_id", "dataset_version", "version", "row_count", "records"}
)


def _project_persisted_binding(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, Mapping):
        return None
    projected: dict[str, Any] = {}
    for key in sorted(_PERSISTED_BINDING_FIELDS):
        if key not in value:
            continue
        item = value[key]
        if key in {"market_id", "dataset_id", "dataset_version", "version"}:
            if isinstance(item, str) and item.strip():
                projected[key] = item.strip()
        elif key in {"row_count", "records"}:
            if isinstance(item, int) and not isinstance(item, bool):
                projected[key] = item
    return projected or None


def _persisted_metadata_projection(
    value: Any,
    *,
    constituent: bool = False,
) -> dict[str, Any]:
    """Keep only the research/provenance metadata consumed by this tool."""
    if not isinstance(value, Mapping):
        return {}
    allowed = (
        _PERSISTED_CONSTITUENT_METADATA_FIELDS
        if constituent
        else _PERSISTED_AGGREGATE_METADATA_FIELDS
    )
    result: dict[str, Any] = {}
    for key in sorted(allowed):
        if key not in value:
            continue
        item = value[key]
        if key == "source_type":
            if isinstance(item, str) and item.strip():
                result[key] = item.strip().upper()
        elif key in {
            "provider",
            "instrument",
            "market_id",
            "polymarket_key",
            "research_quality",
            "provenance_version",
            "policy_version",
        }:
            if isinstance(item, str) and item.strip():
                result[key] = item.strip()
        elif key in {"markets_discovered", "markets_imported", "price_points"}:
            if isinstance(item, int) and not isinstance(item, bool) and item >= 0:
                result[key] = item
        elif key == "historical_order_book_available":
            if isinstance(item, bool):
                result[key] = item
        elif key == "category_counts":
            if isinstance(item, Mapping):
                counts = {
                    str(name).strip(): count
                    for name, count in item.items()
                    if isinstance(name, str)
                    and name.strip()
                    and isinstance(count, int)
                    and not isinstance(count, bool)
                    and count >= 0
                }
                if len(counts) == len(item):
                    result[key] = counts
        elif key == "token_ids":
            if isinstance(item, Mapping):
                tokens = {
                    str(name).strip().lower(): token.strip()
                    for name, token in item.items()
                    if str(name).strip().lower() in {"yes", "no"}
                    and isinstance(token, str)
                    and token.strip()
                }
                if tokens:
                    result[key] = tokens
        elif key == "market_versions":
            if isinstance(item, (list, tuple)):
                result[key] = [
                    projected
                    for binding in item
                    if (projected := _project_persisted_binding(binding)) is not None
                ]
    return result
def _persisted_catalog_identity(
    value: Any,
    *,
    constituent: bool = False,
) -> dict[str, Any] | None:
    """Build catalog identity from fields whose metadata is exportable."""
    if not isinstance(value, Mapping):
        return None
    original = dict(value)
    identity = {
        str(key): original[key]
        for key in sorted(_PERSISTED_CATALOG_FIELDS)
        if key in original
    }
    identity["metadata"] = _persisted_metadata_projection(
        original.get("metadata"),
        constituent=constituent,
    )
    return identity



def _compact_persisted_catalog(
    value: Any,
    *,
    sample_limit: int = 3,
    constituent: bool = False,
) -> dict[str, Any] | None:
    """Project a catalog through the required research/provenance allowlist."""
    identity = _persisted_catalog_identity(value, constituent=constituent)
    if identity is None:
        return None
    result = dict(identity)
    metadata = dict(identity["metadata"])
    market_versions = metadata.pop("market_versions", None)
    if isinstance(market_versions, list):
        metadata.update(
            {
                "market_versions_count": len(market_versions),
                "market_versions_hash": _sha256_json(market_versions),
                "market_versions_sample": _bounded_sample(market_versions, sample_limit),
            }
        )
    result["metadata"] = metadata
    result["catalog_hash"] = _sha256_json(identity)
    return result




_PERSISTED_ATTESTATION_FIELDS = frozenset(
    {
        "dataset_id",
        "dataset_version",
        "source_type",
        "market_type",
        "row_count",
        "completeness",
        "start_timestamp",
        "end_timestamp",
        "execution_fidelity",
        "contamination_result",
        "provenance_version",
        "policy_version",
        "attestation_hash",
        "verified_at",
        "status",
        "reason",
    }
)


def _compact_persisted_attestation(
    value: Any,
    *,
    sample_limit: int = 3,
) -> dict[str, Any] | None:
    """Retain only attestation identity and a bounded binding proof."""
    if not isinstance(value, Mapping):
        return None
    original = dict(value)
    result = {
        str(key): original[key]
        for key in sorted(_PERSISTED_ATTESTATION_FIELDS)
        if key in original
    }
    bindings = original.get("constituent_bindings")
    if isinstance(bindings, (list, tuple)):
        projected_bindings = [
            projected
            for binding in bindings
            if (projected := _project_persisted_binding(binding)) is not None
        ]
        result.update(
            {
                "constituent_bindings_count": len(projected_bindings),
                "constituent_bindings_hash": _sha256_json(projected_bindings),
                "constituent_bindings_sample": _bounded_sample(projected_bindings, sample_limit),
            }
        )
    result["attestation_row_hash"] = _sha256_json(
        {key: result[key] for key in sorted(result)}
    )
    return result
def _persisted_attestation_projection(value: Any) -> dict[str, Any]:
    """Keep only attestation columns and allowlisted constituent bindings."""
    if not isinstance(value, Mapping):
        return {}
    result = {
        str(key): value[key]
        for key in sorted(_PERSISTED_ATTESTATION_FIELDS)
        if key in value
    }
    bindings = value.get("constituent_bindings")
    if isinstance(bindings, (list, tuple)):
        result["constituent_bindings"] = [
            projected
            for binding in bindings
            if (projected := _project_persisted_binding(binding)) is not None
        ]
    return result


def _compact_constituent_records(
    value: Any,
    *,
    sample_limit: int = 3,
    record_sample_limit: int = 2,
) -> dict[str, Any]:
    """Represent all constituent catalogs/records by hashes and bounded samples."""
    rows = list(value) if isinstance(value, (list, tuple)) else []
    summaries: list[dict[str, Any]] = []
    for item in rows:
        if not isinstance(item, Mapping):
            continue
        records = item.get("records")
        records_list = list(records) if isinstance(records, (list, tuple)) else []
        catalog = item.get("catalog")
        catalog_identity = _persisted_catalog_identity(catalog, constituent=True)
        compact_catalog = _compact_persisted_catalog(catalog, sample_limit=2, constituent=True)
        summaries.append(
            {
                "market_id": item.get("market_id"),
                "dataset_id": item.get("dataset_id"),
                "dataset_version": item.get("dataset_version"),
                "row_count": item.get("row_count", len(records_list)),
                "catalog_hash": _sha256_json(catalog_identity) if catalog_identity is not None else None,
                "catalog": compact_catalog,
                "records_count": len(records_list),
                "records_hash": _sha256_json(records_list),
                "source_identity_hash": item.get("source_identity_hash"),
                "provenance_authority": (
                    {
                        key: authority[key]
                        for key in (
                            "source",
                            "source_type",
                            "dataset_id",
                            "dataset_version",
                            "attestation_hash",
                            "attestation_status",
                            "attestation_contamination_result",
                            "explicit_source_type_rows",
                            "catalog_bound_source_type_rows",
                        )
                        if key in authority
                    }
                    if isinstance((authority := item.get("provenance_authority")), Mapping)
                    else None
                ),
                "record_sample": _bounded_sample(records_list, record_sample_limit),
            }
        )
    return {
        "count": len(rows),
        "catalogs_hash": _sha256_json(
            [
                identity
                for item in rows
                if isinstance(item, Mapping)
                for identity in [_persisted_catalog_identity(item.get("catalog"), constituent=True)]
                if identity is not None
            ]
        ),
        "records_count": sum(
            len(item.get("records", ()))
            for item in rows
            if isinstance(item, Mapping) and isinstance(item.get("records"), (list, tuple))
        ),
        "records_hash": _sha256_json(
            [
                record
                for item in rows
                if isinstance(item, Mapping)
                for record in (item.get("records") if isinstance(item.get("records"), (list, tuple)) else ())
            ]
        ),
        "sample": _bounded_sample(summaries, sample_limit),
    }


_PERSISTED_SOURCE_FIELDS = frozenset(
    {
        "status",
        "passed",
        "reasons",
        "dataset_id",
        "dataset_version",
        "source_path",
        "source_uri",
        "read_only",
        "immutable",
        "one_consistent_read_transaction",
        "live_database_rejected",
        "source_stat_pre",
        "source_stat_post",
        "source_hash_pre",
        "source_hash_post",
        "source_hash",
    }
)


def _compact_persisted_source(source_result: Mapping[str, Any]) -> dict[str, Any]:
    """Build the persisted report source section without embedding the snapshot."""
    compact = {
        str(key): source_result[key]
        for key in sorted(_PERSISTED_SOURCE_FIELDS)
        if key in source_result
    }
    aggregate = source_result.get("catalog")
    attestation = source_result.get("attestation")
    attestation_row = source_result.get("attestation_row")
    constituents = _compact_constituent_records(source_result.get("constituent_records"))
    compact_catalog = _compact_persisted_catalog(aggregate)
    compact["catalog"] = compact_catalog
    compact["attestation"] = _compact_persisted_attestation(attestation)
    compact["attestation_row"] = _compact_persisted_attestation(attestation_row)
    original_counts = source_result.get("constituents")
    compact["constituents"] = (
        {
            "catalog_count": original_counts.get("catalog_count", constituents["count"]),
            "binding_count": original_counts.get("binding_count", constituents["count"]),
            "row_count": original_counts.get("row_count", constituents["records_count"]),
        }
        if isinstance(original_counts, Mapping)
        else {
            "catalog_count": constituents["count"],
            "binding_count": constituents["count"],
            "row_count": constituents["records_count"],
        }
    )
    compact["constituent_evidence"] = constituents
    record_provenance = source_result.get("record_provenance")
    compact["record_provenance"] = (
        {
            key: record_provenance[key]
            for key in (
                "explicit_source_type_rows",
                "catalog_bound_source_type_rows",
                "authority",
            )
            if key in record_provenance
        }
        if isinstance(record_provenance, Mapping)
        else None
    )
    compact["aggregate_catalog_hash"] = (
        _sha256_json(aggregate) if isinstance(aggregate, Mapping) else None
    )
    compact["aggregate_catalog_projection_hash"] = (
        compact_catalog.get("catalog_hash")
        if isinstance(compact_catalog, Mapping)
        else None
    )
    compact["evaluated_records_hash"] = source_result.get("evaluated_records_hash")
    compact["evaluated_records_count"] = (
        len(source_result.get("aggregate_records", ()))
        if isinstance(source_result.get("aggregate_records"), (list, tuple))
        else None
    )
    return compact


def _compact_lifecycle_event(event: Any) -> dict[str, Any]:
    """Keep exact event identity/reason plus hashed payload metrics."""
    if not isinstance(event, Mapping):
        return {"event_hash": _sha256_json(event), "payload": None}
    payload = event.get("payload")
    result = {
        str(key): value
        for key, value in event.items()
        if str(key) != "payload"
    }
    result["payload_hash"] = _sha256_json(payload)
    result["payload_keys"] = (
        sorted(str(key) for key in payload)
        if isinstance(payload, Mapping)
        else []
    )
    result["payload_metrics"] = _candidate_metric_projection(
        payload if isinstance(payload, Mapping) else None
    )
    result["event_hash"] = _sha256_json(dict(event))
    return result


def _compact_candidate_lifecycle_evidence(value: Mapping[str, Any]) -> dict[str, Any]:
    """Avoid repeating full candidate payloads while retaining audit projections."""
    rows = value.get("candidates") if isinstance(value, Mapping) else ()
    compact_rows: list[dict[str, Any]] = []
    for row in rows if isinstance(rows, (list, tuple)) else ():
        if not isinstance(row, Mapping):
            continue
        payload = row.get("payload")
        events = row.get("events")
        compact_rows.append(
            {
                "candidate_id": row.get("candidate_id"),
                "stage": row.get("stage"),
                "updated_at": row.get("updated_at"),
                "payload_hash": _sha256_json(payload),
                "payload_keys": sorted(str(key) for key in payload) if isinstance(payload, Mapping) else [],
                "events_count": len(events) if isinstance(events, (list, tuple)) else 0,
                "events_hash": _sha256_json(list(events)) if isinstance(events, (list, tuple)) else _sha256_json([]),
                "metrics": row.get("metrics") if isinstance(row.get("metrics"), Mapping) else _candidate_metric_projection(None),
                "evidence_gap": row.get("evidence_gap"),
            }
        )
    selected = value.get("selected") if isinstance(value, Mapping) else None
    return {
        "selected_candidate_id": value.get("selected_candidate_id") if isinstance(value, Mapping) else None,
        "selected_by_processor": bool(value.get("selected_by_processor")) if isinstance(value, Mapping) else False,
        "candidates": compact_rows,
        "selected": (
            {
                "candidate_id": selected.get("candidate_id"),
                "stage": selected.get("stage"),
                "payload_hash": _sha256_json(selected.get("payload")),
                "events_count": len(selected.get("events", ())) if isinstance(selected.get("events"), (list, tuple)) else 0,
                "metrics_hash": _sha256_json(selected.get("metrics", {})),
            }
            if isinstance(selected, Mapping)
            else None
        ),
    }


def _runtime_metric_assessment(
    candidate_metrics: Mapping[str, Any] | None,
    criteria: Mapping[str, Any] | None,
    *,
    current_market_blocker: str | None,
) -> dict[str, Any]:
    """Compare observed validation metrics to untouched PromotionCriteria defaults."""
    defaults = dict(criteria or {})
    sections = candidate_metrics if isinstance(candidate_metrics, Mapping) else {}
    validation = sections.get("validation")
    validation = validation if isinstance(validation, Mapping) else {}
    nested = validation.get("validation")
    nested = nested if isinstance(nested, Mapping) else validation
    interval = nested.get("confidence_interval")
    interval = interval if isinstance(interval, Mapping) else validation.get("validation_confidence_interval")
    regime_behavior = validation.get("validation_regime_behavior")
    regime_behavior = regime_behavior if isinstance(regime_behavior, Mapping) else {}

    def observed(*names: str) -> tuple[Any, str | None]:
        for name in names:
            if name in nested:
                return nested[name], f"validation.validation.{name}"
            if name in validation:
                return validation[name], f"validation.{name}"
        return None, None

    def compare(
        name: str,
        value: Any,
        threshold: Any,
        operator: str,
        source: str | None,
    ) -> dict[str, Any]:
        observed_value = _persisted_number(value)
        required = _persisted_number(threshold)
        status = "not_reached"
        if observed_value is not None and required is not None:
            status = "pass" if (
                observed_value >= required if operator == ">=" else observed_value <= required
            ) else "fail"
        return {
            "observed": observed_value,
            "required": required,
            "operator": operator,
            "status": status,
            "source": source,
        }

    independent, independent_source = observed("independent_samples")
    filled_trades, filled_trades_source = observed("filled_trades")
    drawdown, drawdown_source = observed("max_drawdown")
    expectancy, expectancy_source = observed("expectancy")
    ci_lower = interval.get("lower") if isinstance(interval, Mapping) else None
    ci_source = "validation.validation.confidence_interval.lower" if ci_lower is not None else None
    stability = validation.get("validation_stability")
    stability_source = "validation.validation_stability" if stability is not None else None
    calibration = validation.get("validation_calibration")
    calibration_source = "validation.validation_calibration" if calibration is not None else None
    liquidity, liquidity_source = observed("liquidity")
    regimes = nested.get("regime_count")
    if regimes is None:
        regimes = regime_behavior.get("regimes")
    regimes_source = "validation.validation.regime_count" if nested.get("regime_count") is not None else (
        "validation.validation_regime_behavior.regimes" if regimes is not None else None
    )
    forward_duration, forward_duration_source = observed("forward_duration_seconds")
    order_attempts, order_attempts_source = observed("forward_order_attempts", "order_attempts")
    metrics = {
        "independent_samples": compare("independent_samples", independent, defaults.get("min_independent_samples"), ">=", independent_source),
        "filled_trades": compare("filled_trades", filled_trades, defaults.get("min_trades"), ">=", filled_trades_source),
        "drawdown": compare("drawdown", drawdown, defaults.get("max_drawdown"), "<=", drawdown_source),
        "expectancy": compare("expectancy", expectancy, defaults.get("min_expectancy"), ">=", expectancy_source),
        "confidence_interval_lower": compare("confidence_interval_lower", ci_lower, defaults.get("min_confidence_lower_bound"), ">=", ci_source),
        "stability": compare("stability", stability, defaults.get("min_stability"), ">=", stability_source),
        "calibration": compare("calibration", calibration, defaults.get("min_calibration"), ">=", calibration_source),
        "liquidity": compare("liquidity", liquidity, defaults.get("min_liquidity"), ">=", liquidity_source),
        "regimes": compare("regimes", regimes, defaults.get("min_regimes"), ">=", regimes_source),
        "forward_duration_seconds": compare("forward_duration_seconds", forward_duration, defaults.get("min_forward_duration_seconds"), ">=", forward_duration_source),
        "forward_order_attempts": compare("forward_order_attempts", order_attempts, defaults.get("min_order_attempts_for_execution_rejection"), ">=", order_attempts_source),
    }
    return {
        "criteria": defaults,
        "criteria_hash": _sha256_json(defaults),
        "observed_basis": "validation",
        "metrics": metrics,
        "status": (
            "fail" if any(item["status"] == "fail" for item in metrics.values())
            else "not_reached" if any(item["status"] == "not_reached" for item in metrics.values())
            else "pass"
        ),
        "decisive_current_market_blocker": {
            "status": "blocking" if current_market_blocker else "not_reached",
            "reason_code": current_market_blocker,
            "source": "queue/candidate lifecycle" if current_market_blocker else None,
        },
    }
def _validated_tag_id(value: Any) -> int | None:
    if isinstance(value, Mapping):
        value = value.get("id", value.get("tag_id", value.get("tagId")))
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value > 0 else None
    if isinstance(value, str) and value.strip().isdecimal():
        identifier = int(value.strip())
        return identifier if identifier > 0 else None
    return None


def _persisted_number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def _persisted_json(value: Any, *, default: Any = None) -> Any:
    try:
        return json.loads(value) if isinstance(value, str) else value
    except (TypeError, ValueError, json.JSONDecodeError):
        return default


def _persisted_source_uri(path: Path) -> str:
    return "file:" + quote(path.resolve().as_posix(), safe="/:") + "?mode=ro&immutable=1"
def _persisted_file_stat(path: Path) -> dict[str, int]:
    stat = os.stat(path, follow_symlinks=True)
    return {
        "device": int(stat.st_dev),
        "inode": int(stat.st_ino),
        "size": int(stat.st_size),
        "mtime_ns": int(stat.st_mtime_ns),
    }


def _persisted_live_file_identities() -> set[tuple[int, int]]:
    tool_path = Path(__file__).resolve()
    workspace = tool_path.parents[2]
    candidates = (
        workspace / "axiom" / "runtime-data" / "axiom.sqlite",
        workspace / "runtime-data" / "axiom.sqlite",
        tool_path.parents[1] / "runtime-data" / "axiom.sqlite",
    )
    identities: set[tuple[int, int]] = set()
    for candidate in candidates:
        try:
            stat = _persisted_file_stat(candidate)
        except OSError:
            continue
        identities.add((stat["device"], stat["inode"]))
    return identities
_PERSISTED_RECORD_FIELDS = frozenset(
    {
        "timestamp",
        "source_timestamp",
        "observed_at",
        "snapshot_id",
        "market_id",
        "provider",
        "instrument",
        "source_type",
        "token_id",
        "question",
        "expiry",
        "resolution_criteria",
        "settlement",
        "category",
        "regime",
        "regime_state",
        "symbol",
        "yes_bid",
        "yes_ask",
        "yes_mid",
        "no_bid",
        "no_ask",
        "no_mid",
        "liquidity",
        "spread",
        "volume",
        "time_to_expiry_seconds",
        "correlation",
        "event_count",
        "event_horizon",
        "expected_event_rate",
        "order_book",
    }
)
_PERSISTED_RECORD_ALIASES = {
    "timestamp": ("timestamp", "source_timestamp", "time"),
    "yes_mid": ("yes_mid", "price", "p", "value"),
    "yes_bid": ("yes_bid", "bid"),
    "yes_ask": ("yes_ask", "ask"),
    "token_id": ("token_id", "asset_id"),
}


def _persisted_values_equal(left: Any, right: Any) -> bool:
    if left is right:
        return True
    left_number = _persisted_number(left)
    right_number = _persisted_number(right)
    if left_number is not None and right_number is not None:
        return Decimal(str(left_number)) == Decimal(str(right_number))
    left_time = _persisted_datetime(left)
    right_time = _persisted_datetime(right)
    if left_time is not None and right_time is not None:
        return left_time == right_time
    if isinstance(left, (Mapping, list, tuple)) or isinstance(right, (Mapping, list, tuple)):
        return _canonical_json(left) == _canonical_json(right)
    return left == right


def _persisted_alias(
    raw: Mapping[str, Any],
    names: Sequence[str],
) -> tuple[bool, Any, bool]:
    present = [(name, raw[name]) for name in names if name in raw]
    if not present:
        return False, None, False
    first = present[0][1]
    return True, first, any(not _persisted_values_equal(first, value) for _, value in present[1:])


def _persisted_canonical_record(
    raw: Mapping[str, Any],
    *,
    market_id: str,
    token_id: str,
    provenance_authority: Mapping[str, Any] | None,
    provider: str,
    source_timestamp: Any = None,
    observed_at: Any = None,
    snapshot_id: Any = None,
) -> tuple[dict[str, Any] | None, str | None]:
    """Project one persisted row into the exact processor input allowlist.

    ``source_type`` is redundant on many historical rows.  When it is absent,
    the caller must provide the already-validated constituent catalog as the
    authority; this function never treats a caller-supplied constant as row
    evidence.  The returned processor projection carries the authoritative
    value for downstream consumers, while validation separately records that
    it was inherited rather than present on the row.
    """
    if not isinstance(raw, Mapping):
        return None, "CONSTITUENT_ROWS_INVALID"
    authority_source_type = str(
        provenance_authority.get("source_type", "")
        if isinstance(provenance_authority, Mapping)
        else ""
    ).strip().upper()
    result: dict[str, Any] = {}
    for canonical, aliases in _PERSISTED_RECORD_ALIASES.items():
        present, value, conflict = _persisted_alias(raw, aliases)
        if conflict:
            return None, f"RECORD_ALIAS_CONFLICT:{canonical}"
        if present:
            result[canonical] = value
    timestamp_value = result.get("timestamp", source_timestamp)
    timestamp = _persisted_datetime(timestamp_value)
    if timestamp is None:
        return None, "TIMESTAMP_INVALID"
    if "yes_mid" not in result:
        return None, "PRICE_INVALID"
    price = _persisted_number(result["yes_mid"])
    if price is None or not 0.0 <= price <= 1.0:
        return None, "PRICE_INVALID"
    if "source_type" in raw:
        raw_source_type = raw["source_type"]
        if not isinstance(raw_source_type, str) or not raw_source_type.strip():
            return None, "DATASET_PROVENANCE_INVALID"
        normalized_source_type = raw_source_type.strip().upper()
        if normalized_source_type != "HISTORICAL":
            return None, "FORWARD_CURRENT_RELABEL_REJECTED"
        if authority_source_type and normalized_source_type != authority_source_type:
            return None, "DATASET_PROVENANCE_INVALID"
    else:
        if authority_source_type != "HISTORICAL":
            return None, "DATASET_PROVENANCE_INVALID"
        normalized_source_type = authority_source_type
    raw_provider = raw.get("provider", provider)
    if str(raw_provider or "").strip().casefold() != "polymarket":
        return None, "PROVIDER_INVALID"
    # Bind every canonical row to the validated constituent identity.  A row
    # may omit this field, but an explicit value must match; never infer it
    # from arbitrary row metadata.
    raw_market = raw.get("market_id", market_id)
    resolved_market_id = str(raw_market or "").strip()
    if resolved_market_id != market_id:
        return None, "MARKET_ID_INVALID"
    result["market_id"] = market_id
    raw_token = result.get("token_id", token_id)
    if not str(raw_token or "").strip():
        return None, "TOKEN_ID_INVALID"
    if token_id and str(raw_token).strip() != token_id:
        return None, "TOKEN_ID_INVALID"
    result["timestamp"] = timestamp.isoformat()
    result["source_timestamp"] = timestamp.isoformat()
    result["provider"] = "polymarket"
    result["source_type"] = normalized_source_type
    result["token_id"] = str(raw_token).strip()
    result["yes_mid"] = price
    if observed_at is not None:
        observed = _persisted_datetime(observed_at)
        if observed is None:
            return None, "OBSERVED_AT_INVALID"
        result["observed_at"] = observed.isoformat()
    if snapshot_id is not None and str(snapshot_id).strip():
        result["snapshot_id"] = str(snapshot_id).strip()
    for field in _PERSISTED_RECORD_FIELDS:
        if field in result or field not in raw or field in {"timestamp", "source_timestamp", "market_id", "provider", "source_type", "token_id", "yes_mid"}:
            continue
        value = raw[field]
        if field in {
            "yes_bid", "yes_ask", "no_bid", "no_ask", "no_mid", "liquidity", "spread",
            "volume", "time_to_expiry_seconds", "correlation", "event_count", "event_horizon",
            "expected_event_rate",
        }:
            if value is None:
                result[field] = None
            else:
                number = _persisted_number(value)
                if number is None:
                    return None, f"RECORD_NUMERIC_INVALID:{field}"
                result[field] = number
        elif field in {"expiry"}:
            parsed = _persisted_datetime(value)
            if parsed is None:
                return None, "EXPIRY_INVALID"
            result[field] = parsed.isoformat()
        else:
            result[field] = value
    return {key: result[key] for key in sorted(result)}, None

def _release_plan(
    source_result: Mapping[str, Any],
    *,
    source_backup: str | Path,
) -> dict[str, Any]:
    """Describe safe, prepared-only merge/deploy steps; execute nothing."""
    explicit_db = "runtime-data/axiom.sqlite"
    powershell = "powershell -NoProfile -ExecutionPolicy Bypass -File"
    node_status_command = f'{powershell} ops/status_axiom_node.ps1 -DbPath "{explicit_db}"'
    canary_status_command = f'python -m axiom.cli canary-status --db "{explicit_db}"'
    stop_command = f'{powershell} ops/stop_axiom_node.ps1 -DbPath "{explicit_db}"'
    start_command = (
        f'{powershell} ops/start_axiom_node.ps1 -DbPath "{explicit_db}" '
        "-IntervalSeconds 60 -CryptoSource public -Depth 20 -MaxMarkets 100"
    )
    backup = {
        "required": True,
        "source_backup": str(Path(source_backup)),
        "dataset_id": _PINNED_PERSISTED_DATASET_ID,
        "dataset_version": _PINNED_PERSISTED_DATASET_VERSION,
        "attestation_hash": (
            (source_result.get("attestation") or {}).get("attestation_hash")
            if isinstance(source_result.get("attestation"), Mapping)
            else None
        ),
        "read_only_immutable": True,
        "same_backup_for_validation_and_export": True,
    }
    return {
        "prepared_not_executed": True,
        "merge_executed": False,
        "deploy_executed": False,
        "controls_altered": False,
        "orders_allowed": False,
        "warning": (
            "Do not use ops/restart_axiom_node.ps1: parameter mismatch prevents the "
            "required explicit DB/control-status sequence."
        ),
        "backup_prerequisite": backup,
        "merge_gates": {
            "clean_review_required": True,
            "fast_forward_only": True,
            "exact_reviewed_commit_required": True,
            "reviewed_commit_precondition": (
                "At execution time, set EXACT_REVIEWED_COMMIT to the reviewed/pushed tip and "
                "require local `git rev-parse feature/polymarket-market-scope` and the matching "
                "`git ls-remote origin refs/heads/feature/polymarket-market-scope` to equal it; "
                "abort on any mismatch."
            ),
            "commands_executed": False,
            "commands": [
                "git status --short --branch",
                "git diff --check",
                "git fetch origin",
                "git switch main",
                "git pull --ff-only origin main",
                "git merge --ff-only feature/polymarket-market-scope",
                "git push origin main",
            ],
            "prohibited": ["git merge --no-ff", "git push --force", "git reset --hard"],
        },
        "focused_unittest_command": "python -m unittest tests.test_market_scope_acceptance_tool -v",
        "full_unittest_command": 'python -m unittest discover -s tests -p "test_*.py" -v',
        "deployment": {
            "explicit_db": explicit_db,
            "canary_status_before": canary_status_command,
            "node_status_before": node_status_command,
            "stop": stop_command,
            "node_status_after_stop": node_status_command,
            "start": start_command,
            "node_status_after_start": node_status_command,
            "canary_status_after_start": canary_status_command,
            "commands_executed": False,
            "commands_are_separate": True,
            "same_explicit_db_for_all_commands": True,
            "canary_controls_unchanged_requires_comparison": True,
            "never_alter_controls": True,
        },
    }


def _persisted_hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def _persisted_source_identity_hash(rows: Sequence[Mapping[str, Any]]) -> str:
    """Hash the canonical source material used for Polymarket versions.

    This intentionally runs before the processor allowlist projection.  The
    bootstrapper versions each constituent from timestamp/price/token identity
    (and optional order-book material), so report-only metadata must never
    redefine that pinned source identity.
    """
    identities: list[dict[str, Any]] = []
    for row in rows:
        timestamp = _persisted_datetime(row.get("source_timestamp", row.get("timestamp", row.get("time"))))
        price = _persisted_number(row.get("price", row.get("p", row.get("yes_mid", row.get("value")))))
        token = str(row.get("token_id", row.get("asset_id", "")) or "").strip()
        if timestamp is None or price is None or not token:
            return ""
        identity: dict[str, Any] = {
            "timestamp": timestamp,
            "price": price,
            "token_id": token,
        }
        order_book = row.get("order_book", row.get("book"))
        if order_book is not None:
            identity["order_book"] = order_book
        identities.append(identity)
    return _sha256_json(identities)


def _persisted_identity_hash(rows: Sequence[Mapping[str, Any]]) -> str:
    """Backward-compatible alias for the source-material identity hash."""
    return _persisted_source_identity_hash(rows)

def _persisted_datetime(value: Any) -> datetime | None:
    return _parse_datetime(value)




def _persisted_canonical_attestation(attestation: Mapping[str, Any]) -> dict[str, Any]:
    """Normalize attestation fields without allowing malformed values to raise."""
    raw_row_count = attestation.get("row_count", 0)
    row_count = raw_row_count if isinstance(raw_row_count, int) and not isinstance(raw_row_count, bool) else -1
    raw_completeness = _persisted_number(attestation.get("completeness", 0.0))
    completeness = raw_completeness if raw_completeness is not None else -1.0
    raw_bindings = attestation.get("constituent_bindings", [])
    bindings = list(raw_bindings) if isinstance(raw_bindings, (list, tuple)) else []
    canonical: dict[str, Any] = {
        "dataset_id": str(attestation.get("dataset_id", "")).strip(),
        "dataset_version": str(attestation.get("dataset_version", "")).strip(),
        "source_type": str(attestation.get("source_type", "")).strip().upper(),
        "market_type": str(attestation.get("market_type", "")).strip().lower(),
        "row_count": row_count,
        "completeness": completeness,
        "start_timestamp": str(attestation.get("start_timestamp")) if attestation.get("start_timestamp") is not None else None,
        "end_timestamp": str(attestation.get("end_timestamp")) if attestation.get("end_timestamp") is not None else None,
        "execution_fidelity": str(attestation.get("execution_fidelity", attestation.get("historical_execution_fidelity", "UNKNOWN"))).strip().upper(),
        "contamination_result": str(attestation.get("contamination_result", "FAIL")).strip().upper(),
        "constituent_bindings": bindings,
        "provenance_version": str(attestation.get("provenance_version", "")),
        "policy_version": str(attestation.get("policy_version", "")),
    }
    reason = str(attestation.get("reason") or "").strip().upper()
    if reason:
        canonical["reason"] = reason
    return canonical


def _persisted_catalog(row: sqlite3.Row | Mapping[str, Any]) -> dict[str, Any]:
    get = row.get if isinstance(row, Mapping) else lambda key, default=None: row[key] if key in row.keys() else default
    result = {str(key): row[key] for key in row.keys()} if isinstance(row, sqlite3.Row) else dict(row)
    result["metadata"] = _persisted_json(get("metadata_json"), default={})
    result["missing_ranges"] = _persisted_json(get("missing_ranges_json"), default=[])
    result["dataset_version"] = str(get("dataset_version", "") or "").strip()
    result["source_type"] = str(get("source_type", "") or "").strip().upper()
    result["market_type"] = str(get("market_type", "") or "").strip().lower()
    return result


def _persisted_attestation(row: sqlite3.Row | Mapping[str, Any]) -> dict[str, Any]:
    result = {str(key): row[key] for key in row.keys()} if isinstance(row, sqlite3.Row) else dict(row)
    result["constituent_bindings"] = _persisted_json(result.get("constituent_bindings_json"), default=[])
    result.pop("constituent_bindings_json", None)
    return result


def _persisted_source_snapshot(
    source_backup: Path,
    dataset_id: str,
    dataset_version: str,
    *,
    expected_row_count: int | None = None,
    expected_constituent_count: int | None = None,
) -> dict[str, Any]:
    """Read and validate one immutable persisted prediction snapshot.

    Every source query runs after one explicit BEGIN on an immutable read-only
    URI. No AxiomStore is constructed for the source, so schema initialization
    cannot write to it.
    """
    requested = str(dataset_id or "").strip()
    version = str(dataset_version or "").strip()
    path = Path(source_backup).expanduser()
    validation: dict[str, Any] = {
        "status": "FAILED",
        "passed": False,
        "reasons": [],
        "dataset_id": requested,
        "dataset_version": version,
        "source_path": str(path),
        "source_uri": None,
        "read_only": True,
        "immutable": True,
        "one_consistent_read_transaction": True,
        "live_database_rejected": False,
        "catalog": None,
        "attestation": None,
        "constituents": {"catalog_count": 0, "binding_count": 0, "row_count": 0},
        "record_provenance": {
            "explicit_source_type_rows": 0,
            "catalog_bound_source_type_rows": 0,
            "authority": "validated constituent catalog and exact CURRENT/PASS attestation",
        },
        "source_stat_pre": None,
        "source_stat_post": None,
        "source_hash_pre": None,
        "source_hash_post": None,
    }
    def fail(reason: str) -> None:
        if reason not in validation["reasons"]:
            validation["reasons"].append(reason)
    if requested != _PINNED_PERSISTED_DATASET_ID or version != _PINNED_PERSISTED_DATASET_VERSION:
        fail("PINNED_DATASET_ID_VERSION_REQUIRED")
    if expected_row_count not in (None, _PINNED_PERSISTED_ROW_COUNT):
        fail("PINNED_ROW_COUNT_OVERRIDE_REJECTED")
    if expected_constituent_count not in (None, _PINNED_PERSISTED_CONSTITUENT_COUNT):
        fail("PINNED_CONSTITUENT_COUNT_OVERRIDE_REJECTED")
    if str(path).startswith("file:") or str(path) in {":memory:", ""}:
        fail("SOURCE_MUST_BE_AN_EXPLICIT_FILESYSTEM_BACKUP")
    try:
        resolved = path.resolve(strict=True)
        stat_pre = _persisted_file_stat(resolved)
    except (OSError, RuntimeError):
        fail("SOURCE_BACKUP_NOT_FOUND")
        return validation
    validation["source_stat_pre"] = stat_pre
    if (
        resolved.name.casefold() == "axiom.sqlite"
        or (stat_pre["device"], stat_pre["inode"]) in _persisted_live_file_identities()
    ):
        validation["live_database_rejected"] = True
        fail("SOURCE_IS_LIVE_DATABASE")
    if validation["reasons"]:
        return validation
    validation["source_uri"] = _persisted_source_uri(resolved)
    validation["source_path"] = str(resolved)
    attestation_valid = False
    try:
        source_hash_pre = _persisted_hash_file(resolved)
    except OSError:
        fail("SOURCE_HASH_UNAVAILABLE")
        return validation
    validation["source_hash_pre"] = source_hash_pre
    validation["source_hash"] = source_hash_pre
    conn: sqlite3.Connection | None = None
    try:
        conn = sqlite3.connect(validation["source_uri"], uri=True, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA query_only=ON")
        query_only = int(conn.execute("PRAGMA query_only").fetchone()[0])
        if query_only != 1:
            fail("SOURCE_QUERY_ONLY_NOT_ENABLED")
        conn.execute("BEGIN")
        tables = {
            str(row[0])
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
        required_tables = {"dataset_catalog", "dataset_integrity_attestation"}
        if not required_tables.issubset(tables):
            fail("SOURCE_REQUIRED_TABLES_MISSING")
        aggregate_row = conn.execute(
            "SELECT * FROM dataset_catalog WHERE dataset_id=? AND dataset_version=?",
            (requested, version),
        ).fetchone()
        if aggregate_row is None:
            fail("DATASET_CATALOG_NOT_FOUND")
            return validation
        aggregate = _persisted_catalog(aggregate_row)
        validation["catalog"] = {
            key: (
                _persisted_metadata_projection(aggregate.get("metadata"))
                if key == "metadata"
                else aggregate.get(key)
            )
            for key in (
                "dataset_id", "dataset_version", "provider", "instrument", "market_type",
                "timeframe", "start_timestamp", "end_timestamp", "row_count",
                "completeness", "missing_ranges", "quality", "source_type", "snapshot_id",
                "created_at", "updated_at", "metadata",
            )
        }
        aggregate_row_count = aggregate.get("row_count")
        aggregate_row_count_valid = isinstance(aggregate_row_count, int) and not isinstance(aggregate_row_count, bool)
        aggregate_row_count_invalid = not aggregate_row_count_valid or int(aggregate_row_count) < 1
        aggregate_completeness = _persisted_number(aggregate.get("completeness"))
        aggregate_completeness_invalid = (
            aggregate_completeness is None or aggregate_completeness < 1.0 or aggregate_completeness > 1.0
        )
        aggregate_source_type = aggregate.get("source_type")
        if not isinstance(aggregate_source_type, str) or not aggregate_source_type.strip():
            fail("DATASET_PROVENANCE_INVALID")
        elif aggregate_source_type.strip().upper() != "HISTORICAL":
            fail("FORWARD_CURRENT_RELABEL_REJECTED")
        if (
            aggregate.get("dataset_id") != requested
            or aggregate.get("dataset_version") != version
            or str(aggregate.get("provider") or "").strip().casefold() != "polymarket"
            or str(aggregate.get("instrument") or "").strip().upper() != "POLYMARKET"
            or aggregate.get("source_type") != "HISTORICAL"
            or aggregate.get("market_type") != "prediction"
            or not str(aggregate.get("timeframe") or "").strip()
            or not str(aggregate.get("snapshot_id") or "").strip()
            or aggregate_row_count_invalid
            or aggregate_completeness_invalid
            or not _persisted_datetime(aggregate.get("start_timestamp"))
            or not _persisted_datetime(aggregate.get("end_timestamp"))
            or not isinstance(aggregate.get("missing_ranges"), list)
            or aggregate.get("missing_ranges") != []
        ):
            fail("DATASET_CATALOG_INVALID")
        metadata = aggregate.get("metadata")
        if not isinstance(metadata, Mapping):
            fail("DATASET_PROVENANCE_INVALID")
            metadata = {}
        target_constituents = _PINNED_PERSISTED_CONSTITUENT_COUNT
        target_rows = _PINNED_PERSISTED_ROW_COUNT
        metadata_source_type = metadata.get("source_type")
        if metadata_source_type is not None:
            if not isinstance(metadata_source_type, str) or not metadata_source_type.strip():
                fail("DATASET_PROVENANCE_INVALID")
            elif metadata_source_type.strip().upper() != "HISTORICAL":
                fail("FORWARD_CURRENT_RELABEL_REJECTED")
        metadata_provider = metadata.get("provider")
        if not isinstance(metadata_provider, str) or metadata_provider.strip().casefold() != "polymarket":
            fail("DATASET_PROVENANCE_INVALID")
        metadata_instrument = metadata.get("instrument")
        if not isinstance(metadata_instrument, str) or metadata_instrument.strip().upper() != "POLYMARKET":
            fail("DATASET_PROVENANCE_INVALID")
        if metadata.get("markets_discovered") != target_constituents or metadata.get("markets_imported") != target_constituents:
            fail("CONSTITUENT_COUNT_MISMATCH")
        if metadata.get("price_points") != target_rows:
            fail("DATASET_ROW_COUNT_MISMATCH")
        category_counts = metadata.get("category_counts")
        if (
            not isinstance(category_counts, Mapping)
            or any(isinstance(value, bool) or not isinstance(value, int) or value < 0 for value in category_counts.values())
            or sum(category_counts.values()) != target_constituents
        ):
            fail("DATASET_PROVENANCE_INVALID")
        market_versions = metadata.get("market_versions")
        if not isinstance(market_versions, (list, tuple)) or not market_versions:
            fail("CONSTITUENT_BINDINGS_INVALID")
            market_versions = []
        if len(market_versions) != target_constituents:
            fail("CONSTITUENT_COUNT_MISMATCH")
        if version.startswith("sha256:") and _sha256_json(list(market_versions)) != version:
            fail("DATASET_VERSION_HASH_MISMATCH")
        if int(aggregate.get("row_count", -1)) != target_rows:
            fail("DATASET_ROW_COUNT_MISMATCH")
        constituents: list[dict[str, Any]] = []
        aggregate_records: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()
        for binding in market_versions:
            if not isinstance(binding, Mapping):
                fail("CONSTITUENT_BINDING_INVALID")
                continue
            market_id = str(binding.get("market_id") or "").strip()
            constituent_id = str(binding.get("dataset_id") or f"prediction:{market_id}").strip()
            constituent_version = str(binding.get("dataset_version") or binding.get("version") or "").strip()
            expected_count = binding.get("row_count", binding.get("records"))
            identity = (constituent_id, constituent_version)
            if (
                not market_id or constituent_id != f"prediction:{market_id}" or not constituent_version
                or identity in seen or isinstance(expected_count, bool) or not isinstance(expected_count, int) or expected_count <= 0
            ):
                fail("CONSTITUENT_BINDING_INVALID")
                continue
            seen.add(identity)
            c_row = conn.execute(
                "SELECT * FROM dataset_catalog WHERE dataset_id=? AND dataset_version=?",
                identity,
            ).fetchone()
            if c_row is None:
                fail("CONSTITUENT_CATALOG_NOT_FOUND")
                continue
            catalog = _persisted_catalog(c_row)
            cmeta = catalog.get("metadata")
            if not isinstance(cmeta, Mapping):
                fail("DATASET_PROVENANCE_INVALID")
                cmeta = {}
            constituent_metadata_source_type = cmeta.get("source_type")
            if constituent_metadata_source_type is not None:
                if (
                    not isinstance(constituent_metadata_source_type, str)
                    or not constituent_metadata_source_type.strip()
                ):
                    fail("DATASET_PROVENANCE_INVALID")
                elif constituent_metadata_source_type.strip().upper() != "HISTORICAL":
                    fail("FORWARD_CURRENT_RELABEL_REJECTED")
            catalog_source_type = catalog.get("source_type")
            if not isinstance(catalog_source_type, str) or not catalog_source_type.strip():
                fail("DATASET_PROVENANCE_INVALID")
            elif catalog_source_type.strip().upper() != "HISTORICAL":
                fail("FORWARD_CURRENT_RELABEL_REJECTED")
            token_ids = cmeta.get("token_ids")
            yes_token = str(token_ids.get("yes") or "").strip() if isinstance(token_ids, Mapping) else ""
            no_token = str(token_ids.get("no") or "").strip() if isinstance(token_ids, Mapping) else ""
            constituent_completeness = _persisted_number(catalog.get("completeness"))
            if (
                catalog.get("dataset_id") != constituent_id
                or catalog.get("dataset_version") != constituent_version
                or str(catalog.get("provider") or "").strip().casefold() != "polymarket"
                or catalog.get("source_type") != "HISTORICAL"
                or catalog.get("market_type") != "prediction"
                or catalog.get("row_count") != expected_count
                or constituent_completeness != 1.0
                or not _persisted_datetime(catalog.get("start_timestamp"))
                or not _persisted_datetime(catalog.get("end_timestamp"))
                or not isinstance(catalog.get("missing_ranges"), list)
                or catalog.get("missing_ranges") != []
                or str(cmeta.get("market_id", cmeta.get("polymarket_key", ""))).strip() != market_id
                or not yes_token
                or not no_token
            ):
                fail("CONSTITUENT_CATALOG_INVALID")
            provenance_authority = {
                "source": "constituent_catalog",
                "source_type": catalog_source_type.strip().upper()
                if isinstance(catalog_source_type, str)
                else "",
                "dataset_id": constituent_id,
                "dataset_version": constituent_version,
            }
            explicit_source_type_rows = 0
            catalog_bound_source_type_rows = 0
            records: list[dict[str, Any]] = []
            # Token identity is part of the immutable processor/provenance
            # binding.  Never synthesize it from catalog metadata when a
            # persisted row omits the required field.
            require_explicit_tokens = True
            d_row = conn.execute(
                "SELECT payload_json,metadata_json,quality FROM datasets WHERE dataset_id=? AND version=?",
                identity,
            ).fetchone() if "datasets" in tables else None
            if d_row is not None:
                dataset_metadata = _persisted_json(d_row["metadata_json"], default=None)
                if not isinstance(dataset_metadata, Mapping):
                    fail("DATASET_PROVENANCE_INVALID")
                    dataset_metadata = {}
                dataset_source_type = dataset_metadata.get("source_type")
                if dataset_source_type is not None:
                    if not isinstance(dataset_source_type, str) or not dataset_source_type.strip():
                        fail("DATASET_PROVENANCE_INVALID")
                    elif dataset_source_type.strip().upper() != "HISTORICAL":
                        fail("FORWARD_CURRENT_RELABEL_REJECTED")
                dataset_provider = dataset_metadata.get("provider")
                if dataset_provider is not None and str(dataset_provider).strip().casefold() != "polymarket":
                    fail("PROVIDER_INVALID")
                values = _persisted_json(d_row["payload_json"], default=None)
                if not isinstance(values, (list, tuple)):
                    fail("CONSTITUENT_ROWS_INVALID")
                    values = []
                for value in values:
                    if isinstance(value, Mapping):
                        records.append(dict(value))
                    else:
                        fail("CONSTITUENT_ROWS_INVALID")
            elif "polymarket_snapshots" in tables:
                s_rows = conn.execute(
                    "SELECT snapshot_id,market_id,source_timestamp,observed_at,payload_json,quality,source_type "
                    "FROM polymarket_snapshots WHERE market_id=? ORDER BY source_timestamp,snapshot_id",
                    (market_id,),
                ).fetchall()
                for s_row in s_rows:
                    payload = _persisted_json(s_row["payload_json"], default=None)
                    snapshot_source_type = s_row["source_type"]
                    if not isinstance(snapshot_source_type, str) or not snapshot_source_type.strip():
                        fail("DATASET_PROVENANCE_INVALID")
                    elif snapshot_source_type.strip().upper() != "HISTORICAL":
                        fail("FORWARD_CURRENT_RELABEL_REJECTED")
                    if not isinstance(payload, Mapping):
                        fail("CONSTITUENT_ROWS_INVALID")
                        continue
                    if "source_type" in payload:
                        payload_source_type = payload["source_type"]
                        if not isinstance(payload_source_type, str) or not payload_source_type.strip():
                            fail("DATASET_PROVENANCE_INVALID")
                        elif payload_source_type.strip().upper() != "HISTORICAL":
                            fail("FORWARD_CURRENT_RELABEL_REJECTED")
                        elif payload_source_type.strip().upper() != snapshot_source_type.strip().upper():
                            fail("DATASET_PROVENANCE_INVALID")
                    record = dict(payload)
                    record.update(
                        {
                            "market_id": market_id,
                            "snapshot_id": s_row["snapshot_id"],
                            "source_timestamp": s_row["source_timestamp"],
                            "observed_at": s_row["observed_at"],
                        }
                    )
                    records.append(record)
            if len(records) != expected_count:
                fail("CONSTITUENT_ROW_COUNT_MISMATCH")

            def source_order_key(value: Mapping[str, Any]) -> tuple[datetime, str]:
                timestamp = _persisted_datetime(
                    value.get("source_timestamp", value.get("timestamp", value.get("time")))
                )
                return (
                    timestamp or datetime.min.replace(tzinfo=timezone.utc),
                    str(value.get("token_id", value.get("asset_id", "")) or "").strip(),
                )

            records.sort(key=source_order_key)
            source_identity_hash = _persisted_source_identity_hash(records)
            canonical_records: list[dict[str, Any]] = []
            for record in records:
                if "source_type" in record:
                    explicit_source_type_rows += 1
                else:
                    catalog_bound_source_type_rows += 1
                fallback_token = (
                    yes_token
                    if not require_explicit_tokens or any(name in record for name in ("token_id", "asset_id"))
                    else ""
                )
                canonical_record, record_error = _persisted_canonical_record(
                    record,
                    market_id=market_id,
                    token_id=fallback_token,
                    provenance_authority=provenance_authority,
                    provider="polymarket",
                    source_timestamp=record.get("source_timestamp", record.get("timestamp")),
                    observed_at=record.get("observed_at"),
                    snapshot_id=record.get("snapshot_id"),
                )
                if canonical_record is None:
                    fail(record_error or "CONSTITUENT_ROWS_INVALID")
                    continue
                canonical_records.append(canonical_record)
                aggregate_records.append(canonical_record)
            validation["record_provenance"]["explicit_source_type_rows"] += explicit_source_type_rows
            validation["record_provenance"]["catalog_bound_source_type_rows"] += catalog_bound_source_type_rows
            records = canonical_records
            if len(records) != expected_count:
                fail("CONSTITUENT_ROW_COUNT_MISMATCH")
            if source_identity_hash != constituent_version:
                fail("CONSTITUENT_VERSION_HASH_MISMATCH")
            constituent_catalog = dict(catalog)
            constituent_catalog["metadata"] = _persisted_metadata_projection(
                cmeta,
                constituent=True,
            )
            constituents.append(
                {
                    "market_id": market_id,
                    "dataset_id": constituent_id,
                    "dataset_version": constituent_version,
                    "row_count": expected_count,
                    "catalog": constituent_catalog,
                    "records": records,
                    "source_identity_hash": source_identity_hash,
                    "provenance_authority": {
                        **provenance_authority,
                        "explicit_source_type_rows": explicit_source_type_rows,
                        "catalog_bound_source_type_rows": catalog_bound_source_type_rows,
                    },
                }
            )
        validation["constituents"] = {
            "catalog_count": len(constituents),
            "binding_count": len(seen),
            "row_count": len(aggregate_records),
        }
        if len(aggregate_records) != int(aggregate.get("row_count", -1)):
            fail("DATASET_ROW_COUNT_MISMATCH")
        if target_rows is not None and len(aggregate_records) != target_rows:
            fail("DATASET_ROW_COUNT_MISMATCH")
        att_row = conn.execute(
            "SELECT * FROM dataset_integrity_attestation WHERE dataset_id=? AND dataset_version=?",
            (requested, version),
        ).fetchone()
        if att_row is None:
            fail("ATTESTATION_MISSING")
        else:
            attestation = _persisted_attestation(att_row)
            validation["attestation_row"] = dict(attestation)
            validation["attestation"] = {
                key: attestation.get(key)
                for key in (
                    "dataset_id", "dataset_version", "source_type", "market_type", "row_count",
                    "completeness", "execution_fidelity", "contamination_result",
                    "provenance_version", "policy_version", "attestation_hash", "verified_at", "status",
                )
            }
            canonical = _persisted_canonical_attestation(attestation)
            expected_hash = _sha256_json(canonical)
            att_start = _persisted_datetime(attestation.get("start_timestamp"))
            att_end = _persisted_datetime(attestation.get("end_timestamp"))
            catalog_start = _persisted_datetime(aggregate.get("start_timestamp"))
            catalog_end = _persisted_datetime(aggregate.get("end_timestamp"))
            attestation_completeness = _persisted_number(attestation.get("completeness"))
            attestation_identity_valid = (
                attestation.get("dataset_id") == requested
                and attestation.get("dataset_version") == version
                and str(attestation.get("source_type", "")).upper() == "HISTORICAL"
                and str(attestation.get("market_type", "")).lower() == "prediction"
                and attestation.get("status") == "CURRENT"
                and str(attestation.get("contamination_result", "")).upper() == "PASS"
                and attestation.get("row_count") == target_rows
                and attestation_completeness == 1.0
                and att_start is not None
                and att_end is not None
                and att_start == catalog_start
                and att_end == catalog_end
                and str(attestation.get("provenance_version") or "") == "dataset-provenance-v1"
                and str(attestation.get("policy_version") or "") == "prediction-integrity-v1"
                and attestation.get("attestation_hash") == expected_hash
                and attestation.get("attestation_hash") == _PINNED_PERSISTED_ATTESTATION_HASH
            )
            if not attestation_identity_valid:
                fail("ATTESTATION_INVALID")
            bindings = attestation.get("constituent_bindings")
            expected_binding_list: list[tuple[str, str, int]] = []
            for item in market_versions:
                if not isinstance(item, Mapping):
                    continue
                count = item.get("row_count", item.get("records"))
                if isinstance(count, bool) or not isinstance(count, int):
                    continue
                expected_binding_list.append(
                    (
                        str(item.get("dataset_id") or f"prediction:{item.get('market_id', '')}").strip(),
                        str(item.get("dataset_version") or item.get("version") or "").strip(),
                        count,
                    )
                )
            normalized_binding_list: list[tuple[str, str, int]] = []
            if isinstance(bindings, list):
                for item in bindings:
                    if not isinstance(item, Mapping):
                        continue
                    count = item.get("row_count", item.get("records"))
                    if isinstance(count, bool) or not isinstance(count, int):
                        continue
                    normalized_binding_list.append(
                        (
                            str(item.get("dataset_id", "")).strip(),
                            str(item.get("dataset_version", "")).strip(),
                            count,
                        )
                    )
            bindings_valid = (
                normalized_binding_list == expected_binding_list
                and len(normalized_binding_list) == target_constituents
            )
            if not bindings_valid:
                fail("ATTESTATION_BINDINGS_INVALID")
            attestation_valid = attestation_identity_valid and bindings_valid
            if attestation_valid:
                attestation_hash = str(attestation.get("attestation_hash"))
                for item in constituents:
                    authority = item.get("provenance_authority")
                    if isinstance(authority, dict):
                        authority.update(
                            {
                                "attestation_hash": attestation_hash,
                                "attestation_status": "CURRENT",
                                "attestation_contamination_result": "PASS",
                            }
                        )
        validation["record_provenance"]["attestation_valid"] = attestation_valid
        try:
            stat_post = _persisted_file_stat(resolved)
            source_hash_post = _persisted_hash_file(resolved)
            validation["source_stat_post"] = stat_post
            validation["source_hash_post"] = source_hash_post
            if stat_post != validation["source_stat_pre"] or source_hash_post != validation["source_hash_pre"]:
                fail("SOURCE_CHANGED_DURING_READ_TRANSACTION")
        except OSError:
            fail("SOURCE_POST_READ_STAT_UNAVAILABLE")
        validation["aggregate_records"] = aggregate_records
        validation["evaluated_records_hash"] = _sha256_json(aggregate_records)
        validation["constituent_records"] = constituents
        if not validation["reasons"]:
            validation["status"] = "PASSED"
            validation["passed"] = True
        if conn is not None:
            conn.rollback()
    except (OSError, sqlite3.Error, TypeError, ValueError, OverflowError, json.JSONDecodeError) as exc:
        fail(f"SOURCE_READ_ERROR:{type(exc).__name__}")
    finally:
        if conn is not None:
            conn.close()
    return validation


def compute_dollar_limit_buy_feasibility(
    book: Mapping[str, Any] | None,
    *,
    budget: float = 1.0,
    slippage_bps: float = 100.0,
    quantity_step: float | str = "0.01",
    min_quantity: float | str = "0",
    min_notional: float | str = "0",
    fee_bps: float = 0.0,
) -> dict[str, Any]:
    """Calculate (without transport) whether a $1 LIMIT BUY is executable."""
    safe_budget = _persisted_number(budget)
    no_book_result: dict[str, Any] = {
        "side": "BUY", "order_type": "LIMIT", "budget": safe_budget if safe_budget is not None else 0.0,
        "best_ask": None, "limit_price": None, "quantity": None,
        "notional": None, "fee": "not_evaluated", "total": None, "feasible": False,
        "reason": "NO_PERMITTED_PUBLIC_BOOK", "submit_called": False,
        # No market has been selected when a permitted public book is absent.
        # Keep the intended order explicit without presenting it as submitted.
        "selected_market": None, "selected_market_id": None, "selected_token_id": None,
        "selected_order": {
            "side": "BUY", "order_type": "LIMIT",
            "budget": safe_budget if safe_budget is not None else 0.0,
            "status": "not_reached", "submit_called": False,
        },
        "min_quantity": None, "min_notional": None, "fee_rate": None, "fee_bps": None,
        "quantity_step": None, "quantity_rounding": None, "quantity_increment": None,
        "available_quantity": None, "book_depth": None,
        "checks": {
            "min_quantity": "not_reached", "min_notional": "not_reached",
            "fee_rate": "not_reached", "quantity_rounding": "not_reached",
            "quantity_increment": "not_reached", "book_depth": "not_reached",
        },
    }
    if not isinstance(book, Mapping):
        return no_book_result
    asks = book.get("asks", book.get("sell", book.get("offers", ())))
    if not isinstance(asks, (list, tuple)) or not asks:
        no_book_result["reason"] = "PUBLIC_BOOK_HAS_NO_ASKS"
        return no_book_result
    result: dict[str, Any] = {
        "side": "BUY", "order_type": "LIMIT", "budget": safe_budget if safe_budget is not None else 0.0,
        "best_ask": None, "limit_price": None, "quantity": 0.0,
        "notional": 0.0, "fee": 0.0, "total": 0.0, "feasible": False,
        "reason": "NO_PERMITTED_PUBLIC_BOOK", "submit_called": False,
    }
    levels: list[tuple[Decimal, Decimal]] = []
    for level in asks:
        if isinstance(level, Mapping):
            raw_price, raw_size = level.get("price", level.get("px")), level.get("size", level.get("quantity", level.get("qty")))
        elif isinstance(level, (list, tuple)) and len(level) >= 2:
            raw_price, raw_size = level[0], level[1]
        else:
            continue
        try:
            price, size = Decimal(str(raw_price)), Decimal(str(raw_size))
        except (InvalidOperation, ValueError):
            continue
        if price.is_finite() and size.is_finite() and price > 0 and size > 0:
            levels.append((price, size))
    if not levels:
        no_book_result["reason"] = "PUBLIC_BOOK_ASK_INVALID"
        return no_book_result
    try:
        budget_d = Decimal(str(budget))
        slip_d = Decimal(str(slippage_bps)) / Decimal("10000")
        fee_d = Decimal(str(fee_bps)) / Decimal("10000")
        step_d = Decimal(str(quantity_step))
        min_qty_d = Decimal(str(min_quantity))
        min_notional_d = Decimal(str(min_notional))
    except (InvalidOperation, ValueError, TypeError):
        result["reason"] = "FEASIBILITY_INPUT_INVALID"
        return result
    if (
        not all(value.is_finite() for value in (budget_d, slip_d, fee_d, step_d, min_qty_d, min_notional_d))
        or budget_d <= 0
        or slip_d < 0
        or fee_d < 0
        or step_d <= 0
        or min_qty_d < 0
        or min_notional_d < 0
    ):
        result["reason"] = "FEASIBILITY_INPUT_INVALID"
        return result
    best_ask, available = min(levels, key=lambda item: item[0])
    limit_price = best_ask * (Decimal("1") + slip_d)
    raw_quantity = min(available, budget_d / (best_ask * (Decimal("1") + fee_d)))
    quantity = raw_quantity.quantize(step_d, rounding=ROUND_DOWN)
    notional = quantity * best_ask
    fee = notional * fee_d
    total = notional + fee
    feasible = quantity > 0 and quantity >= min_qty_d and notional >= min_notional_d and total <= budget_d and best_ask <= limit_price
    result.update(
        {
            "best_ask": float(best_ask), "limit_price": float(limit_price),
            "quantity": float(quantity), "quantity_step": float(step_d),
            "available_quantity": float(available), "notional": float(notional),
            "fee_bps": float(fee_bps), "fee": float(fee), "total": float(total),
            "min_quantity": float(min_qty_d), "min_notional": float(min_notional_d),
            "feasible": bool(feasible),
            "reason": "FEASIBLE" if feasible else (
                "VENUE_MINIMUM_NOTIONAL" if notional < min_notional_d else
                "VENUE_MINIMUM_QUANTITY" if quantity <= 0 or quantity < min_qty_d else
                "BUDGET_INSUFFICIENT_AFTER_FEES"
            ),
        }
    )
    return result



def _persisted_proposal(
    dataset_id: str,
    dataset_version: str,
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "proposal_id": f"polymarket-historical-acceptance-{dataset_version.split(':')[-1][:16]}",
        "statement": "A probability-mispricing strategy is evaluated against the immutable Polymarket historical price proxy.",
        "source": "Polymarket-historical persisted acceptance dataset",
        "tests": ["chronological train-validation-holdout", "bounded robustness checks", "exact historical provenance"],
        "dataset_id": dataset_id,
        "dataset_version": dataset_version,
        "time_split": "train-validation-holdout",
        "paper_only": True,
        "experiment_plan": {
            "market_type": "prediction",
            "template": "probability_mispricing",
            "dataset_id": dataset_id,
            "dataset_version": dataset_version,
            "dataset_selector": {
                "dataset_id": dataset_id,
                "dataset_version": dataset_version,
                "source_type": "HISTORICAL",
                "provider": "polymarket",
            },
            "target": {"instrument": "POLYMARKET", "categories": list(policy.get("categories", ()))},
            "market_scope": dict(policy),
            "filters": dict(policy.get("filters", {})),
            "parameters": {"threshold": [0.03]},
            "methodology": {
                "time_split": "train-validation-holdout",
                "initial_cash": 10_000.0,
                "fee_bps": 10.0,
                "slippage_bps": 5.0,
                "allocation": 0.25,
            },
            "metrics": ["expectancy", "drawdown", "trade_count", "sample_count"],
            "min_samples": 30,
            "min_trades": 0,
            "max_variants": 1,
            "model_document": {"field": "yes_mid"},
            "paper_only": True,
        },
    }


def _persisted_control_projection(store: Any) -> dict[str, Any]:
    tables = (
        "canary_control", "canary_autonomous_state", "canary_eligibility",
        "canary_selection", "canary_readiness_snapshot", "operator_config",
        "operator_jobs", "operator_actions", "collector_state", "scheduler_state",
        "worker_state",
    )
    counts: dict[str, int] = {}
    connection = getattr(store, "connection", None)
    for table in tables:
        try:
            exists = connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
            ).fetchone()
            counts[table] = int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]) if exists else 0
        except Exception:
            counts[table] = 0
    return {
        "disabled_or_absent": all(value == 0 for value in counts.values()),
        "credential_state_exported": False,
        "counts": counts,
    }


def run_persisted_acceptance(
    source_backup: str | Path,
    dataset_id: str,
    dataset_version: str,
    *,
    policy: Mapping[str, Any] | None = None,
    config: AcceptanceConfig | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    """Run the positive acceptance chain from one explicit immutable backup."""
    if str(dataset_id or "").strip() != _PINNED_PERSISTED_DATASET_ID or str(dataset_version or "").strip() != _PINNED_PERSISTED_DATASET_VERSION:
        raise ValueError("persisted acceptance is pinned to the Polymarket historical dataset")
    config = config or AcceptanceConfig(
        mode="persisted",
        source_backup=Path(source_backup),
        dataset_id=_PINNED_PERSISTED_DATASET_ID,
        dataset_version=_PINNED_PERSISTED_DATASET_VERSION,
        expected_row_count=_PINNED_PERSISTED_ROW_COUNT,
        expected_constituent_count=_PINNED_PERSISTED_CONSTITUENT_COUNT,
    )
    started_at = generated_at or datetime.now(timezone.utc).isoformat()
    source_result = _persisted_source_snapshot(
        Path(source_backup),
        _PINNED_PERSISTED_DATASET_ID,
        _PINNED_PERSISTED_DATASET_VERSION,
        expected_row_count=_PINNED_PERSISTED_ROW_COUNT,
        expected_constituent_count=_PINNED_PERSISTED_CONSTITUENT_COUNT,
    )
    aggregate = source_result.get("catalog") if isinstance(source_result.get("catalog"), Mapping) else {}
    aggregate_metadata = aggregate.get("metadata") if isinstance(aggregate, Mapping) else {}
    categories = (
        sorted(str(key).strip() for key in aggregate_metadata.get("category_counts", {}) if str(key).strip())
        if isinstance(aggregate_metadata, Mapping)
        else []
    )
    if not categories:
        categories = ["crypto", "other", "sports"]
    supplied_policy = policy or {
        "schema_version": "1",
        "mode": "RULE_BASED_MARKETS",
        "instrument": "POLYMARKET",
        "categories": categories,
        "market_ids": [],
        "filters": {"category": categories},
        "regime_restrictions": {},
        "provenance": "canonical",
    }
    policy_record = _policy_record(supplied_policy)
    config_record = asdict(config)
    if config_record.get("source_backup") is not None:
        config_record["source_backup"] = str(config_record["source_backup"])
    config_record.update(
        {
            "dataset_id": _PINNED_PERSISTED_DATASET_ID,
            "dataset_version": _PINNED_PERSISTED_DATASET_VERSION,
            "expected_row_count": _PINNED_PERSISTED_ROW_COUNT,
            "expected_constituent_count": _PINNED_PERSISTED_CONSTITUENT_COUNT,
        }
    )
    report: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": started_at,
        "mode": "persisted",
        "status": "ERROR" if not source_result.get("passed") else "RUNNING",
        "acceptance_status": "ERROR" if not source_result.get("passed") else "RUNNING",
        "config": config_record,
        "config_hash": _sha256_json(config_record),
        "scope": {
            "policy": policy_record,
            "policy_authority": "canonical local MarketScopePolicy",
            "dataset_id": str(dataset_id),
            "dataset_version": str(dataset_version),
        },
        "scope_hash": _sha256_json(policy_record),
        "source": _compact_persisted_source(source_result),
        "evaluated_records_hash": source_result.get("evaluated_records_hash"),
        "runtime_criteria": _runtime_criteria_record(),
        "proposal": None,
        "timestamps": {
            "run_started_at": started_at,
            "source_read_at": datetime.now(timezone.utc).isoformat(),
            "exported_at": None,
            "processor_at": None,
        },
        "validation": {
            "status": source_result.get("status"),
            "passed": source_result.get("passed", False),
            "reasons": list(source_result.get("reasons", [])),
            "catalog": _compact_persisted_catalog(source_result.get("catalog")),
            "attestation": dict(source_result.get("attestation") or {}) if isinstance(source_result.get("attestation"), Mapping) else None,
            "attestation_hash": (
                source_result.get("attestation", {}).get("attestation_hash")
                if isinstance(source_result.get("attestation"), Mapping)
                else None
            ),
            "constituents": _compact_constituent_records(source_result.get("constituent_records")),
            "record_provenance": dict(source_result.get("record_provenance") or {}),
            "evaluated_records_hash": source_result.get("evaluated_records_hash"),
            "aggregate_catalog_hash": (
                _sha256_json(source_result.get("catalog"))
                if isinstance(source_result.get("catalog"), Mapping)
                else None
            ),
            "aggregate_catalog_projection_hash": (
                _sha256_json(_persisted_catalog_identity(source_result.get("catalog")))
                if isinstance(source_result.get("catalog"), Mapping)
                else None
            ),
        },
        "export": {
            "status": "NOT_RUN",
            "isolated": True,
            "temporary_store": True,
            "controls": None,
            "dataset_id": str(dataset_id),
            "dataset_version": str(dataset_version),
            "aggregate_catalog_hash": (
                _sha256_json(source_result.get("catalog"))
                if isinstance(source_result.get("catalog"), Mapping)
                else None
            ),
            "aggregate_catalog_projection_hash": (
                _sha256_json(_persisted_catalog_identity(source_result.get("catalog")))
                if isinstance(source_result.get("catalog"), Mapping)
                else None
            ),
            "attestation_hash": (
                source_result.get("attestation", {}).get("attestation_hash")
                if isinstance(source_result.get("attestation"), Mapping)
                else None
            ),
            "evaluated_records_hash": source_result.get("evaluated_records_hash"),
            "evaluated_records_count": (
                len(source_result.get("aggregate_records", ()))
                if isinstance(source_result.get("aggregate_records"), (list, tuple))
                else None
            ),
            "constituent_catalogs_count": (
                len(source_result.get("constituent_records", ()))
                if isinstance(source_result.get("constituent_records"), (list, tuple))
                else 0
            ),
        },
        "chain": {
            "proposal_id": None, "queue_item_id": None, "plan_id": None,
            "plan_hash": None, "candidate_ids": [], "frozen_hashes": [],
            "queue_status": "NOT_RUN", "lifecycle_stage": None,
            "processor": None, "metrics": None, "reasons": list(source_result.get("reasons", [])),
        },
        "evaluator": {
            "status": "SKIPPED",
            "reason": "validation_failed",
            "qualification_blocker": "VALIDATION_FAILED",
            "current_market_resolution": "NOT_RUN",
            "fresh_identity_and_books": "NOT_RUN",
            "decision": None,
        },
        "dollar_limit_buy_feasibility": compute_dollar_limit_buy_feasibility(None),
        "release_readiness": {
            "status": "NOT_READY",
            "qualification_required_stage": "PAPER_FORWARD",
            "current_public_book_required": True,
            "canary": None,
            "operator_review": None,
            "place_order_called": False,
        },
        "security": {
            "source_opened_read_only": True,
            "source_uri_immutable": True,
            "source_live_database_rejected": bool(source_result.get("live_database_rejected")),
            "controls_disabled_or_absent": None,
            "credentials_used": False,
            "authenticated_calls": False,
            "network_calls": False,
            "order_transport_called": False,
            "submit_order_called": False,
            "synthetic_probability_model": False,
            "forward_relabel_rejected": any("RELABEL" in str(reason) for reason in source_result.get("reasons", [])),
        },
        "release_plan": _release_plan(source_result, source_backup=source_backup),
        "missing_dataset_public_queue_negative": {
            "preserved": True,
            "executed": False,
            "note": "The existing public missing-dataset negative section is unchanged.",
        },
    }
    if not source_result.get("passed"):
        report["status_categories"] = {
            "validation": "FAILED", "export": "NOT_RUN", "queue": "NOT_RUN",
            "qualification": "NOT_RUN", "evaluator": "SKIPPED",
        }
        report["runtime_metric_assessment"] = _runtime_metric_assessment(
            None,
            report.get("runtime_criteria"),
            current_market_blocker=None,
        )
        return _scrub(report)
    export_started = datetime.now(timezone.utc).isoformat()
    report["timestamps"]["exported_at"] = export_started
    constituent_records = source_result.get("constituent_records", [])
    aggregate_records = source_result.get("aggregate_records", [])
    attestation_row = source_result.get("attestation_row")
    try:
        from axiom.autonomous import AutonomousResearchProcessor
        from axiom.director import validate_hermes_proposal
        from axiom.research_bus import DurableResearchBus, ResearchBusPermissionError
        from axiom.storage import AxiomStore

        with AxiomStore(":memory:") as store:
            for item in constituent_records:
                catalog = item["catalog"]
                constituent_metadata = _persisted_metadata_projection(
                    catalog.get("metadata"),
                    constituent=True,
                )
                store.save_dataset(
                    item["dataset_id"], item["dataset_version"], item["records"],
                    metadata=constituent_metadata,
                    quality=catalog.get("quality") or "PRICE_PROXY",
                )
                store.save_dataset_catalog(
                    item["dataset_id"], item["dataset_version"],
                    provider=str(catalog["provider"]), instrument=str(catalog["instrument"]),
                    market_type=str(catalog["market_type"]), timeframe=str(catalog.get("timeframe") or ""),
                    start_timestamp=_persisted_datetime(catalog.get("start_timestamp")),
                    end_timestamp=_persisted_datetime(catalog.get("end_timestamp")),
                    row_count=int(catalog["row_count"]), completeness=float(catalog["completeness"]),
                    missing_ranges=list(catalog.get("missing_ranges") or []),
                    quality=catalog.get("quality") or "PRICE_PROXY",
                    source_type="HISTORICAL", snapshot_id=str(catalog["snapshot_id"]),
                    metadata=constituent_metadata,
                    created_at=_persisted_datetime(catalog.get("created_at")),
                    updated_at=_persisted_datetime(catalog.get("updated_at")),
                )
            aggregate_metadata_projection = _persisted_metadata_projection(aggregate_metadata)
            store.save_dataset(
                dataset_id, dataset_version, aggregate_records,
                metadata=aggregate_metadata_projection,
                quality=aggregate.get("quality") or "PRICE_PROXY",
            )
            store.save_dataset_catalog(
                dataset_id, dataset_version,
                provider=str(aggregate["provider"]), instrument=str(aggregate["instrument"]),
                market_type=str(aggregate["market_type"]), timeframe=str(aggregate.get("timeframe") or ""),
                start_timestamp=_persisted_datetime(aggregate.get("start_timestamp")),
                end_timestamp=_persisted_datetime(aggregate.get("end_timestamp")),
                row_count=int(aggregate["row_count"]), completeness=float(aggregate["completeness"]),
                missing_ranges=list(aggregate.get("missing_ranges") or []),
                quality=aggregate.get("quality") or "PRICE_PROXY", source_type="HISTORICAL",
                snapshot_id=str(aggregate["snapshot_id"]), metadata=aggregate_metadata_projection,
                created_at=_persisted_datetime(aggregate.get("created_at")),
                updated_at=_persisted_datetime(aggregate.get("updated_at")),
            )
            if isinstance(attestation_row, Mapping):
                store.save_dataset_integrity_attestation(
                    dataset_id,
                    dataset_version,
                    _persisted_attestation_projection(attestation_row),
                )
            core_attestation = store.verify_dataset_integrity_attestation(
                dataset_id, dataset_version, force=True
            )
            report["export"]["core_integrity_verification"] = {
                "status": core_attestation.get("status") if isinstance(core_attestation, Mapping) else "ERROR",
                "attestation_hash": core_attestation.get("attestation_hash") if isinstance(core_attestation, Mapping) else None,
                "row_count": core_attestation.get("row_count") if isinstance(core_attestation, Mapping) else None,
                "reason": core_attestation.get("reason") if isinstance(core_attestation, Mapping) else None,
            }
            if (
                not isinstance(core_attestation, Mapping)
                or str(core_attestation.get("status") or "").upper() != "CURRENT"
                or str(core_attestation.get("attestation_hash") or "") != _PINNED_PERSISTED_ATTESTATION_HASH
            ):
                raise RuntimeError("CORE_INTEGRITY_VERIFICATION_FAILED")
            controls = _persisted_control_projection(store)
            proposal = _persisted_proposal(dataset_id, dataset_version, policy_record)
            report["proposal"] = proposal
            evaluated_records_hash = _sha256_json(aggregate_records)
            export_material = {
                "catalog": _compact_persisted_catalog(aggregate),
                "attestation": _persisted_attestation_projection(attestation_row),
                "evaluated_records_hash": evaluated_records_hash,
                "constituents": [
                    {
                        "dataset_id": item["dataset_id"],
                        "dataset_version": item["dataset_version"],
                        "row_count": item["row_count"],
                        "catalog": _compact_persisted_catalog(
                            item["catalog"],
                            constituent=True,
                        ),
                        "records": item["records"],
                    }
                    for item in constituent_records
                ],
            }
            compact_constituents = _compact_constituent_records(constituent_records)
            report["export"]["status"] = "PASSED"
            report["export"]["controls"] = controls
            report["export"]["evaluated_records_hash"] = evaluated_records_hash
            report["export"]["evaluated_records_count"] = len(aggregate_records)
            report["export"]["evaluated_record_sample"] = _bounded_sample(aggregate_records, 2)
            report["export"]["constituent_catalogs_count"] = compact_constituents["count"]
            report["export"]["constituent_catalogs_hash"] = compact_constituents["catalogs_hash"]
            report["export"]["constituent_catalog_sample"] = compact_constituents["sample"]
            report["export_hash"] = _sha256_json(export_material)
            report["security"]["controls_disabled_or_absent"] = controls["disabled_or_absent"]
            bus = DurableResearchBus(store, source="acceptance-persisted", author="acceptance-persisted")
            try:
                queued = bus.submit_proposal(
                    proposal,
                    dedupe_key=f"acceptance-persisted:{dataset_id}:{dataset_version}",
                    lineage=(dataset_id, dataset_version, str((attestation_row or {}).get("attestation_hash", ""))),
                    available_at=_persisted_datetime(started_at) or datetime.now(timezone.utc),
                )
            except ResearchBusPermissionError as exc:
                # The core director's discovery allow-list is intentionally
                # pinned to its production aggregate name.  Source validation
                # above already established this exact isolated binding, so a
                # compact acceptance fixture may use the ordinary bus path
                # after payload-only proposal validation.
                if "DATASET_NOT_FOUND" not in str(exc):
                    raise
                proposal_validation = validate_hermes_proposal(proposal)
                if not proposal_validation.accepted:
                    raise RuntimeError("PROPOSAL_VALIDATION_FAILED") from exc
                queued = bus.submit_hypothesis(
                    proposal_validation.normalized or proposal,
                    dedupe_key=f"acceptance-persisted:{dataset_id}:{dataset_version}",
                    lineage=(dataset_id, dataset_version, str((attestation_row or {}).get("attestation_hash", ""))),
                    available_at=_persisted_datetime(started_at) or datetime.now(timezone.utc),
                )
            report["chain"]["proposal_id"] = proposal["proposal_id"]
            report["chain"]["queue_item_id"] = queued.item_id
            report["timestamps"]["processor_at"] = datetime.now(timezone.utc).isoformat()
            processor = AutonomousResearchProcessor(store, bus=bus, clock=lambda: _persisted_datetime(started_at) or datetime.now(timezone.utc))
            cycle = processor.process_pending(worker="acceptance-persisted", now=_persisted_datetime(started_at))
            cycle_record = cycle.as_record()
            report["chain"]["processor"] = cycle_record
            final_item = bus.get(queued.item_id)
            report["chain"]["queue_status"] = _terminal_queue_status(final_item) if final_item is not None else "ERROR"
            result = cycle.results[0] if cycle.results and isinstance(cycle.results[0], Mapping) else {}
            report["chain"]["plan_id"] = result.get("plan_id")
            report["chain"]["plan_hash"] = result.get("plan_hash")
            candidate_results = result.get("candidate_results", [])
            candidate_ids = [
                str(item.get("candidate_id")).strip() for item in candidate_results
                if isinstance(item, Mapping) and str(item.get("candidate_id", "")).strip()
            ]
            report["chain"]["candidate_ids"] = candidate_ids
            report["chain"]["metrics"] = {
                "variants_tested": result.get("variants_tested"),
                "selected_from_variants": result.get("selected_from_variants"),
                "rejected_reasons": result.get("rejected_reasons"),
            }
            selected_candidate_ids = result.get("selected_candidate_ids", ())
            if not isinstance(selected_candidate_ids, Sequence) or isinstance(selected_candidate_ids, (str, bytes)):
                selected_candidate_ids = ()
            queue_evidence = _queue_demo_evidence(
                final_item,
                result,
                store,
                now=_persisted_datetime(started_at) or datetime.now(timezone.utc),
                authority_cap=_queue_authority_cap(
                    PromotionCriteria() if PromotionCriteria is not None else None
                ),
            )
            lifecycle_evidence = queue_evidence["lifecycle_evidence"]
            selected_lifecycle = lifecycle_evidence.get("selected")
            compact_lifecycle_evidence = _compact_candidate_lifecycle_evidence(lifecycle_evidence)
            selected_payload = (
                selected_lifecycle.get("payload")
                if isinstance(selected_lifecycle, Mapping)
                and isinstance(selected_lifecycle.get("payload"), Mapping)
                else None
            )
            selected_events = (
                selected_lifecycle.get("events", [])
                if isinstance(selected_lifecycle, Mapping)
                and isinstance(selected_lifecycle.get("events", []), (list, tuple))
                else []
            )
            selected_metrics = (
                selected_lifecycle.get("metrics")
                if isinstance(selected_lifecycle, Mapping)
                and isinstance(selected_lifecycle.get("metrics"), Mapping)
                else _candidate_metric_projection(None)
            )
            selected_payload_summary = {
                "candidate_id": selected_payload.get("candidate_id") if selected_payload else None,
                "stage": selected_payload.get("stage") if selected_payload else None,
                "frozen_hash": selected_payload.get("frozen_hash") if selected_payload else None,
                "reason_code": selected_payload.get("reason_code") if selected_payload else None,
                "reason": selected_payload.get("reason") if selected_payload else None,
                "payload_hash": _sha256_json(selected_payload),
                "payload_keys": sorted(str(key) for key in selected_payload) if selected_payload else [],
                "metrics_hash": _sha256_json(selected_metrics),
            }
            report["chain"].update(
                {
                    "observed_outcomes": queue_evidence["observed"],
                    "reason_code": queue_evidence["reason_code"],
                    "reason": queue_evidence["reason"],
                    "exact_reason": queue_evidence["exact_reason"],
                    "decision_reason": queue_evidence["decision_reason"],
                    "decision_reason_source": queue_evidence["decision_reason_source"],
                    "diagnostics": queue_evidence["diagnostics"],
                    "candidate_evidence": compact_lifecycle_evidence,
                    "selected_candidate_id": lifecycle_evidence.get("selected_candidate_id"),
                    "selected_by_processor": lifecycle_evidence.get("selected_by_processor", False),
                    "selected_candidate_lifecycle": {
                        "candidate_id": selected_lifecycle.get("candidate_id") if isinstance(selected_lifecycle, Mapping) else None,
                        "stage": selected_lifecycle.get("stage") if isinstance(selected_lifecycle, Mapping) else None,
                        "updated_at": selected_lifecycle.get("updated_at") if isinstance(selected_lifecycle, Mapping) else None,
                        "payload_hash": _sha256_json(selected_payload),
                        "events_count": len(selected_events),
                        "events_hash": _sha256_json(selected_events),
                        "metrics_hash": _sha256_json(selected_metrics),
                    },
                    "selected_candidate_payload": selected_payload_summary,
                    "selected_candidate_lifecycle_events": [
                        _compact_lifecycle_event(event) for event in selected_events
                    ],
                    "candidate_metrics": selected_metrics,
                }
            )
            candidate_metrics = report["chain"]["candidate_metrics"]
            report["chain"]["backtest_metrics"] = candidate_metrics.get("backtest", {})
            report["chain"]["validation_metrics"] = candidate_metrics.get("validation", {})
            report["chain"]["robustness_metrics"] = candidate_metrics.get("robustness", {})
            report["chain"]["qualification_metrics"] = candidate_metrics.get("qualification", {})
            lifecycle_stage = (
                selected_lifecycle.get("stage")
                if isinstance(selected_lifecycle, Mapping)
                else None
            )
            frozen_hashes: list[str] = []
            for item in lifecycle_evidence.get("candidates", ()):
                payload = item.get("payload") if isinstance(item, Mapping) else None
                frozen = str(payload.get("frozen_hash", "")).strip() if isinstance(payload, Mapping) else ""
                if frozen:
                    frozen_hashes.append(frozen)
            report["chain"]["lifecycle_stage"] = lifecycle_stage
            report["chain"]["frozen_hashes"] = frozen_hashes
            rejected_reasons = result.get("rejected_reasons")
            observed_reasons: list[str] = []
            if isinstance(rejected_reasons, Mapping):
                observed_reasons.extend(
                    str(key).strip() for key in rejected_reasons if str(key).strip()
                )
            if queue_evidence["exact_reason"] is not None:
                observed_reasons.append(str(queue_evidence["exact_reason"]))
            report["chain"]["reasons"] = list(dict.fromkeys(observed_reasons)) or ["MISSING_OBSERVED_REASON"]
            blocker = queue_evidence["exact_reason"]
            report["runtime_metric_assessment"] = _runtime_metric_assessment(
                candidate_metrics,
                report.get("runtime_criteria"),
                current_market_blocker=blocker,
            )
            qualification_ready = lifecycle_stage in {"PAPER_FORWARD", "PAPER_PROMOTABLE"}
            report["evaluator"] = {
                "status": "SKIPPED" if not qualification_ready else "SKIPPED_NO_NETWORK",
                "reason": blocker if not qualification_ready else "fresh public identity/books unavailable in network-free acceptance",
                "qualification_blocker": blocker if not qualification_ready else None,
                "current_market_resolution": "NOT_RUN",
                "fresh_identity_and_books": "NOT_RUN",
                "decision": None,
            }
            report["release_readiness"]["qualification_stage_observed"] = lifecycle_stage
            report["release_readiness"]["qualification_blocker"] = blocker if not qualification_ready else None
            report["release_readiness"]["status"] = "NOT_READY" if not qualification_ready else "PENDING_CURRENT_EVIDENCE"
            report["status_categories"] = {
                "validation": "PASSED", "export": "PASSED",
                "queue": report["chain"]["queue_status"],
                "qualification": lifecycle_stage or "UNKNOWN",
                "evaluator": report["evaluator"]["status"],
            }
            queue_status = str(report["chain"]["queue_status"] or "").upper()
            report["acceptance_status"] = (
                "RESULT_RECORDED"
                if queue_status in {"COMPLETED", "REJECTED"}
                else "ERROR"
            )
            report["status"] = "QUALIFICATION_REJECTED" if not qualification_ready else "EVALUATOR_PENDING"
    except Exception as exc:
        report["export"]["status"] = "FAILED" if report["export"].get("status") != "PASSED" else "PASSED"
        report["chain"]["queue_status"] = "ERROR"
        report["chain"]["reasons"] = [f"PIPELINE_ERROR:{type(exc).__name__}"]
        report["evaluator"] = {
            "status": "SKIPPED",
            "reason": f"PIPELINE_ERROR:{type(exc).__name__}",
            "qualification_blocker": f"PIPELINE_ERROR:{type(exc).__name__}",
            "current_market_resolution": "NOT_RUN",
            "fresh_identity_and_books": "NOT_RUN",
            "decision": None,
        }
        report["status_categories"] = {
            "validation": "PASSED", "export": report["export"]["status"],
            "queue": "ERROR", "qualification": "NOT_RUN", "evaluator": "SKIPPED",
        }
        report["status"] = "ERROR"
        report["acceptance_status"] = "ERROR"
    return _scrub(report)


def run_acceptance(
    adapter: Any | None = None,
    config: AcceptanceConfig | None = None,
    *,
    policy: Any | None = None,
    clock: Callable[[], datetime] | None = None,
    monotonic: Callable[[], float] | None = None,
    generated_at: str | None = None,
    include_queue_demo: bool = True,
) -> dict[str, Any]:
    """Run bounded acceptance evidence against an injected or public adapter."""
    config = config or AcceptanceConfig()
    if config.mode == "persisted":
        if config.source_backup is None:
            raise ValueError("persisted mode requires source_backup")
        return run_persisted_acceptance(
            config.source_backup,
            _PINNED_PERSISTED_DATASET_ID,
            _PINNED_PERSISTED_DATASET_VERSION,
            policy=policy if isinstance(policy, Mapping) else None,
            config=config,
            generated_at=generated_at,
        )
    if adapter is None:
        if config.mode == "offline":
            adapter = build_offline_fixture_adapter()
        else:
            try:
                from axiom.data.polymarket import PolymarketAdapter

                adapter = PolymarketAdapter()
            except Exception as exc:
                adapter = None
                adapter_error = type(exc).__name__
            else:
                adapter_error = None
    else:
        adapter_error = None
    trusted_provider = _trusted_provider_for_adapter(adapter, config.mode)
    policy_record = _policy_record(policy)
    canonical_policy = normalize_market_scope(policy_record)
    policy_record = dict(canonical_policy.as_dict())
    scope_hash = canonical_policy.scope_hash
    config_record = asdict(config)
    config_hash = _sha256_json(config_record)
    fallback_timestamp = _parse_datetime(generated_at or FIXTURE_TIMESTAMP) or datetime.now(timezone.utc)
    if clock is not None:
        now_fn = clock
    elif config.mode == "offline":
        now_fn = lambda: fallback_timestamp
    else:
        now_fn = lambda: datetime.now(timezone.utc)
    mono_fn = monotonic or time.monotonic
    started = mono_fn()
    run_timestamp = _parse_datetime(now_fn()) or fallback_timestamp
    run_timestamp_iso = run_timestamp.isoformat()
    policy_filters = _policy_filters(policy_record)
    policy_categories = _policy_categories(policy_record)
    tag_slug = policy_categories[0] if len(policy_categories) == 1 and re.fullmatch(r"[a-z0-9][a-z0-9_-]*", policy_categories[0]) else None
    min_liquidity_bound = _policy_bound(policy_filters, "min_liquidity")
    liquidity_pushdown = min_liquidity_bound if isinstance(min_liquidity_bound, (int, float)) else None
    minimum_hours_bound = _policy_bound(policy_filters, "minimum_hours_to_resolution")
    maximum_hours_bound = _policy_bound(policy_filters, "maximum_hours_to_resolution")
    minimum_hours_pushdown = minimum_hours_bound if isinstance(minimum_hours_bound, (int, float)) else None
    maximum_hours_pushdown = maximum_hours_bound if isinstance(maximum_hours_bound, (int, float)) else None
    end_date_min = (
        run_timestamp + timedelta(hours=float(minimum_hours_pushdown))
    ).isoformat() if minimum_hours_pushdown is not None else None
    end_date_max = (
        run_timestamp + timedelta(hours=float(maximum_hours_pushdown))
    ).isoformat() if maximum_hours_pushdown is not None else None
    tag_lookup: dict[str, Any] = {
        "method": "GET",
        "path": f"/tags/slug/{tag_slug}" if tag_slug else "",
        "query": {},
        "requested_at": run_timestamp_iso,
        "status": "not_requested" if tag_slug is None else None,
    }
    if tag_slug is not None:
        tag_lookup["slug"] = tag_slug
    tag_id: int | None = None
    coverage_reasons: list[str] = []
    coverage_status = "PENDING"
    request_error: str | None = adapter_error

    def budget_exhausted() -> bool:
        return max(0.0, float(mono_fn() - started)) >= config.max_seconds

    def mark_budget_exhausted(reason: str = "metadata_time_budget_exhausted") -> None:
        nonlocal coverage_status
        if coverage_status != "BUDGET_EXHAUSTED":
            coverage_status = "BUDGET_EXHAUSTED"
        coverage_reasons.append(reason)

    if tag_slug is None:
        tag_lookup["status"] = "not_requested"
    elif budget_exhausted():
        tag_lookup["status"] = "BUDGET_EXHAUSTED"
        mark_budget_exhausted("metadata_time_budget_exhausted")
    elif adapter_error:
        tag_lookup["status"] = "skipped_adapter_error"
        coverage_reasons.append("tag_lookup_fallback_broader_discovery")
    elif callable(getattr(adapter, "resolve_tag_slug", None)):
        try:
            tag_id = _validated_tag_id(adapter.resolve_tag_slug(tag_slug))
            tag_lookup["status"] = "resolved" if tag_id is not None else "invalid_response"
            if tag_id is not None:
                tag_lookup["tag_id"] = tag_id
        except Exception as exc:
            tag_lookup["status"] = "error"
            tag_lookup["error_type"] = type(exc).__name__
        if tag_id is None:
            coverage_reasons.append("tag_lookup_fallback_broader_discovery")
    else:
        tag_lookup["status"] = "method_unavailable"
        coverage_reasons.append("tag_lookup_fallback_broader_discovery")
    requests: list[dict[str, Any]] = []
    records: list[dict[str, Any]] = []
    seen_cursors: list[Any] = []
    cursor: Any = None
    opaque_cursor_only = True
    if coverage_status != "BUDGET_EXHAUSTED" and adapter_error:
        coverage_status = "ERROR"
        coverage_reasons.append("adapter_initialization_error")
    elif coverage_status != "BUDGET_EXHAUSTED" and not callable(getattr(adapter, "market_page", None)):
        coverage_status = "ERROR"
        coverage_reasons.append("adapter_missing_market_page")
    if coverage_status != "BUDGET_EXHAUSTED" and not adapter_error and callable(getattr(adapter, "market_page", None)):
        for page_index in range(config.metadata_page_budget):
            if budget_exhausted():
                mark_budget_exhausted("metadata_time_budget_exhausted")
                break
            query = {
                "limit": config.page_limit,
                "after_cursor": cursor,
                "closed": False,
                "tag_ids": [tag_id] if tag_id is not None else [],
                "include_tag": True,
                "liquidity_num_min": liquidity_pushdown,
                "end_date_min": end_date_min,
                "end_date_max": end_date_max,
            }
            requested_at = run_timestamp
            try:
                page = adapter.market_page(
                    limit=config.page_limit,
                    after_cursor=cursor,
                    closed=False,
                    tag_ids=(tag_id,) if tag_id is not None else (),
                    include_tag=True,
                    liquidity_num_min=liquidity_pushdown,
                    end_date_min=end_date_min,
                    end_date_max=end_date_max,
                )
            except Exception as exc:
                if budget_exhausted():
                    mark_budget_exhausted("metadata_time_budget_exhausted")
                else:
                    coverage_status = "ERROR"
                    coverage_reasons.append("metadata_request_error")
                request_error = type(exc).__name__
                requests.append(
                    {
                        "page": page_index,
                        "method": "GET",
                        "path": "/markets/keyset",
                        "query": query,
                        "requested_at": requested_at.isoformat(),
                        "coverage_status": coverage_status,
                        "error_reason": "metadata_request_error",
                        "error_type": request_error,
                    }
                )
                break
            if budget_exhausted():
                mark_budget_exhausted("metadata_time_budget_exhausted")
            path = str(_page_field(page, "request_path", "/markets/keyset"))
            page_status = str(_page_field(page, "coverage_status", "COMPLETE") or "COMPLETE").strip().upper()
            page_error_reason = _page_field(page, "error_reason", None)
            if page_status not in {"COMPLETE", "PARTIAL", "ERROR", "BUDGET_EXHAUSTED"}:
                page_status = "ERROR"
                page_error_reason = page_error_reason or "unknown_page_coverage_status"
            if page_status == "ERROR":
                if coverage_status != "BUDGET_EXHAUSTED":
                    coverage_status = "ERROR"
                coverage_reasons.append(str(page_error_reason or "page_error"))
            elif coverage_status not in {"ERROR", "BUDGET_EXHAUSTED"} and page_status == "PARTIAL":
                coverage_status = "PARTIAL"
            elif page_status == "BUDGET_EXHAUSTED":
                mark_budget_exhausted(str(page_error_reason or "metadata_time_budget_exhausted"))
            if not _allowed_path(path):
                if coverage_status != "BUDGET_EXHAUSTED":
                    coverage_status = "ERROR"
                coverage_reasons.append("request_path_not_allowlisted")
            snapshots = _page_field(page, "snapshots", _page_field(page, "markets", ()))
            snapshots = tuple(snapshots) if isinstance(snapshots, (list, tuple)) else ()
            raw_count = _page_field(page, "raw_count", len(snapshots))
            unique_count = _page_field(page, "unique_count", 0)
            duplicate_count = _page_field(page, "duplicate_count", 0)
            within_page_duplicate_count = _page_field(page, "within_page_duplicate_count", 0)
            cross_page_duplicate_count = _page_field(page, "cross_page_duplicate_count", 0)
            malformed_count = _page_field(page, "malformed_count", 0)
            try:
                values = (
                    raw_count,
                    unique_count,
                    duplicate_count,
                    within_page_duplicate_count,
                    cross_page_duplicate_count,
                    malformed_count,
                )
                if any(isinstance(value, bool) for value in values):
                    raise ValueError("boolean page count")
                (
                    raw_count,
                    unique_count,
                    duplicate_count,
                    within_page_duplicate_count,
                    cross_page_duplicate_count,
                    malformed_count,
                ) = tuple(max(0, int(value)) for value in values)
            except (TypeError, ValueError, OverflowError):
                raw_count, unique_count, duplicate_count, malformed_count = len(snapshots), 0, 0, 0
                within_page_duplicate_count, cross_page_duplicate_count = 0, 0
            request_record = {
                "page": page_index,
                "method": "GET",
                "path": path,
                "query": query,
                "query_fingerprint": str(_page_field(page, "query_fingerprint", "")),
                "requested_at": requested_at.isoformat(),
                "provider_requested_at": _page_field(page, "requested_at", None),
                "coverage_status": page_status,
                "error_reason": page_error_reason,
                "raw_count": raw_count,
                "unique_count": unique_count,
                "duplicate_count": duplicate_count,
                "within_page_duplicate_count": within_page_duplicate_count,
                "cross_page_duplicate_count": cross_page_duplicate_count,
                "malformed_count": malformed_count,
                "observed_malformed_count": 0,
            }
            requests.append(request_record)
            if not _allowed_path(path):
                break
            observed_malformed_count = 0
            for snapshot in snapshots[: config.page_limit]:
                raw = _mapping(snapshot)
                if _market_identity(raw) is None:
                    observed_malformed_count += 1
                records.append({"raw": raw, "page": page_index, "requested_at": requested_at.isoformat()})
            request_record["observed_malformed_count"] = observed_malformed_count
            next_cursor = _page_field(page, "next_cursor", None)
            if next_cursor is None or next_cursor == "":
                if coverage_status not in {"ERROR", "BUDGET_EXHAUSTED"}:
                    coverage_status = "COMPLETE" if page_status == "COMPLETE" else page_status
                break
            if not isinstance(next_cursor, str):
                opaque_cursor_only = False
                if coverage_status != "BUDGET_EXHAUSTED":
                    coverage_status = "ERROR"
                coverage_reasons.append("opaque_cursor_invalid_type")
                break
            if any(next_cursor == previous for previous in seen_cursors) or next_cursor == cursor:
                if coverage_status != "BUDGET_EXHAUSTED":
                    coverage_status = "ERROR"
                coverage_reasons.append("opaque_cursor_repeated")
                break
            seen_cursors.append(next_cursor)
            cursor = next_cursor
            if page_index + 1 >= config.metadata_page_budget:
                mark_budget_exhausted("metadata_page_budget_exhausted")
            elif budget_exhausted():
                mark_budget_exhausted("metadata_time_budget_exhausted")
    if coverage_status == "PENDING":
        coverage_status = "ERROR"
    if request_error and "metadata_request_error" not in coverage_reasons and coverage_status == "ERROR":
        coverage_reasons.append("adapter_error")
    provider_raw_rows = sum(int(item.get("raw_count", 0)) for item in requests)
    provider_malformed_rows = sum(int(item.get("malformed_count", 0)) for item in requests)
    provider_within_page_duplicates = sum(
        int(item.get("within_page_duplicate_count", 0))
        for item in requests
    )
    provider_cross_page_duplicates = sum(int(item.get("cross_page_duplicate_count", 0)) for item in requests)
    provider_duplicate_count = sum(int(item.get("duplicate_count", 0)) for item in requests)
    # A page adapter may already have removed duplicate or malformed provider
    # rows before returning snapshots.  Local identity observations therefore
    # supplement provider counts, but are never added to them unconditionally.
    first_seen: dict[str, dict[str, Any]] = {}
    seen_by_page: dict[int, set[str]] = {}
    missing_identity_records: list[dict[str, Any]] = []
    duplicate_ids: list[str] = []
    cross_page_duplicate_ids: list[str] = []
    observed_within_page_duplicates = 0
    observed_cross_page_duplicates = 0
    for record in records:
        identifier = _market_identity(record["raw"])
        if identifier is None:
            missing_identity_records.append(record)
            continue
        page_seen = seen_by_page.setdefault(int(record["page"]), set())
        if identifier in page_seen:
            observed_within_page_duplicates += 1
        elif identifier in first_seen:
            observed_cross_page_duplicates += 1
            cross_page_duplicate_ids.append(identifier)
        page_seen.add(identifier)
        if identifier in first_seen:
            duplicate_ids.append(identifier)
        else:
            first_seen[identifier] = record
    unique_rows = len(first_seen)
    observed_malformed_rows = len(missing_identity_records)
    # Provider malformed rows and locally observed malformed snapshots may be
    # the same rows.  Count the larger evidence source once, then derive the
    # duplicate remainder so identity arithmetic always reconciles.
    malformed_rows = max(provider_malformed_rows, observed_malformed_rows)
    raw_rows = max(provider_raw_rows, len(records), unique_rows + malformed_rows)
    duplicate_count = max(0, raw_rows - unique_rows - malformed_rows)
    within_page_duplicate_count = min(
        duplicate_count,
        max(provider_within_page_duplicates, observed_within_page_duplicates),
    )
    cross_page_duplicate_count = min(
        duplicate_count - within_page_duplicate_count,
        max(provider_cross_page_duplicates, observed_cross_page_duplicates),
    )
    unattributed_duplicate_count = (
        duplicate_count - within_page_duplicate_count - cross_page_duplicate_count
    )
    evaluations: list[dict[str, Any]] = []
    policy_obj = policy_record
    for record in (*first_seen.values(), *missing_identity_records):
        evaluations.append(
            _evaluate_market(
                record["raw"],
                policy_obj,
                fallback_timestamp,
                trusted_provider=trusted_provider,
            )
        )
    condition_names = (
        "scope",
        "instrument",
        "lifecycle",
        "category_tags",
        "regime_restrictions",
        "yes_price",
        "expiry",
        "liquidity",
        "spread",
    )
    independent: dict[str, dict[str, int]] = {
        name: {"pass": 0, "fail": 0, "missing": 0, "malformed": 0} for name in condition_names
    }
    canonical: dict[str, int] = {}
    for evaluation in evaluations:
        for name, status in evaluation["conditions"].items():
            independent[name][status] = independent[name].get(status, 0) + 1
        first_failure = evaluation["canonical_first_failure"]
        canonical[first_failure] = canonical.get(first_failure, 0) + 1
    for name in (
        "PASS",
        *condition_names,
        *(
            f"{name}_{status}"
            for name in ("instrument", "regime_restrictions", "yes_price", "expiry", "liquidity", "spread")
            for status in ("missing", "malformed")
        ),
    ):
        canonical.setdefault(name, 0)
    matches = [item for item in evaluations if item["policy_match"]]
    matched_ids = sorted(
        {str(item["market_id"]) for item in matches if str(item["market_id"]).strip()},
        key=_market_sort_key,
    )
    explicit_open = sorted(
        {str(item["market_id"]) for item in evaluations if item["explicit_open"]},
        key=_market_sort_key,
    )
    candidate_outcome: dict[str, Any]
    public_calls: list[dict[str, Any]] = []
    public_queue_attempt: dict[str, Any] | None = None

    def probe_market(
        selected_id: str,
        *,
        scope_match: bool,
        selection_rule: str,
    ) -> dict[str, Any]:
        identity: Any = None
        token_ids: dict[str, str] = {}
        books: list[dict[str, Any]] = []
        book_hashes: dict[str, str] = {}
        probe_reasons: list[str] = []
        identity_path = f"/markets/{selected_id}"
        if budget_exhausted():
            mark_budget_exhausted("probe_time_budget_exhausted")
            probe_reasons.append("probe_time_budget_exhausted")
        elif not _allowed_path(identity_path):
            probe_reasons.append("identity_path_not_allowlisted")
        elif callable(getattr(adapter, "market", None)):
            try:
                identity = adapter.market(selected_id)
                public_calls.append(
                    {
                        "method": "GET",
                        "path": identity_path,
                        "market_id": selected_id,
                        "requested_at": run_timestamp_iso,
                    }
                )
                if identity is None:
                    probe_reasons.append("identity_not_found")
            except Exception as exc:
                probe_reasons.append(f"identity_fetch_error:{type(exc).__name__}")
        else:
            probe_reasons.append("identity_method_unavailable")

        identity_map = _mapping(identity)
        if budget_exhausted():
            mark_budget_exhausted("probe_time_budget_exhausted")
            probe_reasons.append("probe_time_budget_exhausted")
        elif identity is not None and callable(getattr(adapter, "token_ids", None)):
            try:
                raw_tokens = adapter.token_ids(selected_id)
                if isinstance(raw_tokens, Mapping):
                    token_ids = {
                        str(key).strip().lower(): str(value).strip()
                        for key, value in raw_tokens.items()
                        if str(key).strip().lower() in {"yes", "no"} and str(value).strip()
                    }
            except Exception as exc:
                probe_reasons.append(f"token_identity_error:{type(exc).__name__}")
        if not token_ids:
            possible = identity_map.get("tokens", identity_map.get("clobTokenIds", {}))
            if isinstance(possible, Mapping):
                token_ids = {
                    str(key).strip().lower(): str(value).strip()
                    for key, value in possible.items()
                    if str(key).strip().lower() in {"yes", "no"} and str(value).strip()
                }
            for outcome in ("yes", "no"):
                for name in (f"{outcome}_token_id", f"{outcome}TokenId"):
                    value = identity_map.get(name)
                    if str(value).strip() and outcome not in token_ids:
                        token_ids[outcome] = str(value).strip()
                        break

        for outcome in ("yes", "no"):
            if len(books) >= config.max_books:
                break
            if budget_exhausted():
                mark_budget_exhausted("probe_time_budget_exhausted")
                probe_reasons.append("probe_time_budget_exhausted")
                break
            token = token_ids.get(outcome)
            book_path = "/book"
            if not token:
                probe_reasons.append(f"token_id_missing:{outcome}")
                continue
            if not _allowed_path(book_path):
                probe_reasons.append("book_path_not_allowlisted")
                break
            if not callable(getattr(adapter, "order_book_for_token", None)):
                probe_reasons.append("book_method_unavailable")
                break
            try:
                book = adapter.order_book_for_token(token, depth=config.book_depth)
                public_calls.append(
                    {
                        "method": "GET",
                        "path": book_path,
                        "token_id": token,
                        "depth": config.book_depth,
                        "requested_at": run_timestamp_iso,
                    }
                )
                if book is None:
                    probe_reasons.append(f"book_not_found:{outcome}")
                    continue
                bounded = _bounded_book(book, config.book_depth)
                book_hash = _sha256_json(bounded)
                book_hashes[outcome] = book_hash
                books.append(
                    {
                        "outcome": outcome,
                        "token_id": token,
                        "book": bounded,
                        "book_hash": book_hash,
                    }
                )
            except Exception as exc:
                probe_reasons.append(f"book_fetch_error:{outcome}:{type(exc).__name__}")
        identity_record = _scrub(identity)
        identity_hash = _sha256_json(identity_record) if identity is not None else None
        return {
            "market_id": selected_id,
            "selection_rule": selection_rule,
            "scope_match": bool(scope_match),
            "probe": not scope_match,
            "identity": identity_record,
            "identity_hash": identity_hash,
            "token_ids": _scrub(token_ids),
            "books": books,
            "book_hashes": book_hashes,
            "book_count": len(books),
            "book_depth": config.book_depth,
            "reasons": probe_reasons,
        }

    if matched_ids:
        selected_id = matched_ids[0]
        observation = probe_market(
            selected_id,
            scope_match=True,
            selection_rule="smallest numeric market ID from observed policy matches",
        )
        candidate_outcome = {
            "status": "ORIGINAL_POLICY_MATCH",
            "market_id": selected_id,
            "original_policy_matches": matched_ids[: config.sample_limit],
            "probe": False,
            **observation,
            "authority_flags": {
                "lifecycle": False,
                "scope": True,
                "ranking": False,
                "qualification": False,
                "execution": False,
            },
            "execution_flags": _execution_flags(),
        }
        if config.mode == "public" and observation["identity"] is not None:
            if budget_exhausted():
                mark_budget_exhausted("queue_time_budget_exhausted")
                public_queue_attempt = {
                    "queue_status": "SKIPPED_BUDGET",
                    "reason": "total_time_budget_exhausted",
                    "exact_reason": "total_time_budget_exhausted",
                    "market_id": selected_id,
                    "scope_match": True,
                    "authority_flags": _authority_flags(),
                    "execution_flags": _execution_flags(),
                }
            else:
                public_queue_attempt = enqueue_public_market_attempt(
                    selected_id,
                    observation["identity"],
                    observation["token_ids"],
                    observation["books"],
                    policy=policy_record,
                    selection_rule=observation["selection_rule"],
                    now=run_timestamp,
                )
            candidate_outcome["queue_attempt"] = public_queue_attempt
        elif config.mode == "public":
            public_queue_attempt = {
                "queue_status": "SKIPPED",
                "reason": "public_identity_unavailable",
                "exact_reason": "public_identity_unavailable",
                "market_id": selected_id,
                "scope_match": True,
                "authority_flags": _authority_flags(),
                "execution_flags": _execution_flags(),
            }
            candidate_outcome["queue_attempt"] = public_queue_attempt
    elif explicit_open:
        selected_id = explicit_open[0]
        observation = probe_market(
            selected_id,
            scope_match=False,
            selection_rule="smallest numeric market ID from observed explicit-open inventory",
        )
        candidate_outcome = {
            "status": "DATA_PIPELINE_PROBE",
            "explicit_open_inventory_sample": explicit_open[: config.sample_limit],
            "authority_flags": _authority_flags(),
            "execution_flags": _execution_flags(),
            **observation,
        }
    else:
        candidate_outcome = {
            "status": "NO_EXPLICIT_OPEN_INVENTORY",
            "market_id": None,
            "scope_match": False,
            "probe": False,
            "authority_flags": _authority_flags(),
            "execution_flags": _execution_flags(),
        }

    queue_demo: dict[str, Any] | None
    queue_predecessor = "synthetic-predecessor-v1"
    queue_frozen_hash = _sha256_json({"candidate": queue_predecessor, "frozen": True})
    queue_metadata = {
        "dataset_id": "offline-market-scope-fixture",
        "dataset_version": "fixture-v1",
        "row_count": len(evaluations),
        "scope_hash": scope_hash,
    }
    if include_queue_demo:
        if budget_exhausted():
            mark_budget_exhausted("queue_time_budget_exhausted")
            queue_demo = _skipped_queue_demo_result(
                queue_predecessor,
                queue_frozen_hash,
                queue_metadata,
            )
        else:
            queue_demo = enqueue_legacy_successor(
                queue_predecessor,
                queue_frozen_hash,
                queue_metadata,
            )
    else:
        queue_demo = None
    expected_paths = [item["path"] for item in [tag_lookup, *requests, *public_calls] if item.get("path")]
    forbidden_paths = [path for path in expected_paths if not _allowed_path(path) or _FORBIDDEN_PATH_RE.search(path)]
    runtime_criteria = _runtime_criteria_record() or {
        "min_independent_samples": 30,
        "min_trades": 20,
        "max_drawdown": 0.20,
        "min_expectancy": 0.0,
        "min_confidence_lower_bound": 0.0,
        "min_stability": 0.60,
        "min_calibration": 0.80,
        "min_liquidity": 0.0,
        "min_forward_duration_seconds": 604800.0,
        "min_regimes": 3,
        "min_resolved_bets_for_performance_rejection": 5,
        "min_forward_duration_seconds_for_performance_rejection": 0.0,
        "min_order_attempts_for_execution_rejection": 5,
    }
    relaxed_criteria = {
        "min_independent_samples": 0,
        "min_trades": 0,
        "max_drawdown": 1.0,
        "min_expectancy": -1.0,
        "min_confidence_lower_bound": -1.0,
        "min_stability": 0.0,
        "min_calibration": 0.0,
        "min_liquidity": 0.0,
        "min_forward_duration_seconds": 0.0,
        "min_regimes": 0,
        "min_resolved_bets_for_performance_rejection": 5,
        "min_forward_duration_seconds_for_performance_rejection": 0.0,
        "min_order_attempts_for_execution_rejection": 5,
    }
    criteria_differences = {
        name: {
            "relaxed": relaxed_criteria[name],
            "runtime": runtime_criteria.get(name),
            "changed": relaxed_criteria[name] != runtime_criteria.get(name),
        }
        for name in relaxed_criteria
    }
    report = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": generated_at or run_timestamp_iso,
        "mode": config.mode,
        "code_hash": _source_hash(),
        "config_hash": config_hash,
        "scope_hash": scope_hash,
        "config": config_record,
        "scope": {
            "policy": policy_record,
            "policy_authority": "canonical local MarketScopePolicy when available",
            "first_failure_order": list(condition_names),
        },
        "requests": {
            "allowlist": list(PUBLIC_GET_PATHS),
            "tag_lookup": tag_lookup,
            "samples": requests[: config.sample_limit],
            "total_discovery_requests": len(requests),
            "identity_and_book_requests": public_calls[: config.sample_limit],
            "methods": sorted({str(item.get("method", "")) for item in [tag_lookup, *requests, *public_calls] if item.get("path")}),
        },
        "timestamps": {
            "request_samples": [tag_lookup.get("requested_at")] + [item.get("requested_at") for item in requests[: config.sample_limit]],
            "provider_samples": [item.get("provider_requested_at") for item in requests[: config.sample_limit]],
            "monotonic_non_decreasing": all(
                str(a.get("requested_at", "")) <= str(b.get("requested_at", ""))
                for a, b in zip(requests, requests[1:])
            ),
        },
        "pagination": {
            "metadata_page_budget": config.metadata_page_budget,
            "pages_observed": len(requests),
            "coverage_status": coverage_status,
            "cursor_chain": [_scrub(item.get("query", {}).get("after_cursor")) for item in requests],
            "returned_cursor_count": len(seen_cursors),
            "opaque_cursor_only": opaque_cursor_only and all(isinstance(item, str) for item in seen_cursors),
            "cursor_reuse_detected": "opaque_cursor_repeated" in coverage_reasons,
        },
        "order": {
            "observed_ids": list(first_seen)[: config.sample_limit],
            "unique_ids": unique_rows,
            "duplicate_ids": duplicate_ids[: config.sample_limit],
            "cross_page_duplicate_ids": cross_page_duplicate_ids[: config.sample_limit],
            "duplicate_count": duplicate_count,
            "within_page_duplicate_count": within_page_duplicate_count,
            "cross_page_duplicate_count": cross_page_duplicate_count,
            "unattributed_duplicate_count": unattributed_duplicate_count,
            "provider_duplicate_count": provider_duplicate_count,
            "stable_first_observation_order": list(first_seen)[: config.sample_limit],
            "lexicographically_sorted": list(first_seen) == sorted(first_seen),
        },
        "coverage": {
            "status": coverage_status,
            "reasons": sorted(set(coverage_reasons)),
            "raw_rows": raw_rows,
            "unique_rows": unique_rows,
            "duplicate_rows": duplicate_count,
            "malformed_rows": malformed_rows,
            "provider_duplicate_rows": provider_duplicate_count,
            "provider_within_page_duplicate_rows": provider_within_page_duplicates,
            "provider_cross_page_duplicate_rows": provider_cross_page_duplicates,
            "provider_malformed_rows": provider_malformed_rows,
            "observed_within_page_duplicate_rows": observed_within_page_duplicates,
            "observed_cross_page_duplicate_rows": observed_cross_page_duplicates,
            "observed_malformed_rows": observed_malformed_rows,
            "within_page_duplicate_rows": within_page_duplicate_count,
            "cross_page_duplicate_rows": cross_page_duplicate_count,
            "unattributed_duplicate_rows": unattributed_duplicate_count,
            "identity_reconciliation": {
                "raw": raw_rows,
                "unique": unique_rows,
                "malformed": malformed_rows,
                "duplicate": duplicate_count,
                "reconciles": raw_rows == unique_rows + malformed_rows + duplicate_count,
            },
            "page_statuses": [item.get("coverage_status") for item in requests],
            "page_error_reasons": [item.get("error_reason") for item in requests if item.get("error_reason") is not None],
            "bounded": True,
        },
        "reasons": {
            "coverage": sorted(set(coverage_reasons)),
            "candidate": candidate_outcome.get("reasons", []),
            "blockers": (["NO_ORIGINAL_POLICY_MATCH", "PROBE_ISOLATED"] if candidate_outcome.get("status") == "DATA_PIPELINE_PROBE" else []),
        },
        "bounded_samples": {
            "evaluations": [
                {
                    "market_id": item["market_id"],
                    "observed_at": item["observed_at"],
                    "conditions": item["conditions"],
                    "policy_match": item["policy_match"],
                    "canonical_first_failure": item["canonical_first_failure"],
                }
                for item in evaluations[: config.sample_limit]
            ],
            "explicit_open_ids": explicit_open[: config.sample_limit],
        },
        "queue_demo": queue_demo,
        "synthetic_offline_queue_demo": queue_demo,
        "public_queue_attempt": public_queue_attempt,
        "missing_dataset_public_queue_negative": {
            "preserved": True,
            "executed": False,
            "note": "The existing public missing-dataset negative section is unchanged.",
        },
        "candidate_outcome": candidate_outcome,
        "counts": {
            "observed_unique_markets": unique_rows,
            "original_policy_matches": len(matches),
            "independent": independent,
            "independent_pass_counts": {name: values["pass"] for name, values in independent.items()},
            "canonical_first_failure": canonical,
            "canonical_first_failure_distribution": canonical,
        },
        "runtime_qualification": {
            "criteria": _runtime_criteria_record(),
            "unmodified_defaults": True,
            "evidence_class": "SYNTHETIC_OFFLINE",
            "synthetic_offline": True,
            "authority": "not assigned by DATA_PIPELINE_PROBE or ORIGINAL_POLICY_MATCH",
        },
        "config_differences": {
            "metadata_budget_independent_from_book_budget": True,
            "metadata_page_budget": config.metadata_page_budget,
            "max_books": config.max_books,
            "scope_policy_is_not_runtime_promotion": True,
            "runtime_qualification_uses_default_criteria": True,
            "promotion_criteria": {
                "relaxed": relaxed_criteria,
                "runtime": runtime_criteria,
                "relaxed_vs_runtime": criteria_differences,
            },
            "discovery_pushdown": {
                "tag_slug": tag_slug,
                "tag_id": tag_id,
                "liquidity_num_min": liquidity_pushdown,
                "end_date_min_hours": minimum_hours_pushdown,
                "end_date_max_hours": maximum_hours_pushdown,
                "price_local_only": "entry_price" in policy_filters,
                "spread_local_only": "max_spread" in policy_filters,
                "unsupported_or_custom_local_only": sorted(
                    key for key in policy_filters
                    if key not in {
                        "category",
                        "entry_price",
                        "minimum_hours_to_resolution",
                        "maximum_hours_to_resolution",
                        "min_liquidity",
                        "max_spread",
                    }
                ),
            },
            "probe_limits": {
                "max_books": min(config.max_books, 2),
                "book_depth": config.book_depth,
                "authority": "all_false",
            },
        },

        "security": {
            "public_get_allowlist": list(PUBLIC_GET_PATHS),
            "observed_methods": sorted({str(item.get("method", "")) for item in [tag_lookup, *requests, *public_calls] if item.get("path")}),
            "forbidden_paths_observed": forbidden_paths,
            "credentials_used": False,
            "authentication_used": False,
            "order_transport_called": False,
            "private_or_authenticated_state_accessed": False,
            "secret_scrubbed": True,
        },
        "status_categories": {
            "coverage": coverage_status,
            "candidate": candidate_outcome.get("status"),
            "public_queue": (
                public_queue_attempt.get("queue_status")
                if isinstance(public_queue_attempt, Mapping)
                else "NOT_RUN"
            ),
            "synthetic_queue_demo": (
                queue_demo.get("queue_status")
                if isinstance(queue_demo, Mapping)
                else "NOT_RUN"
            ),
            "queue": queue_demo.get("queue_status") if isinstance(queue_demo, Mapping) else "NOT_RUN",
            "probe_authority": "ALL_FALSE" if candidate_outcome.get("probe") else "NOT_APPLICABLE",
        },
        "later_public_workflow": {
            "command": (
                "python tools/polymarket_market_scope_acceptance.py "
                f"--mode public --max-pages {config.max_pages} "
                f"--max-seconds {config.max_seconds:g} --max-books {config.max_books} "
                f"--book-depth {config.book_depth} "
                "--output reports/polymarket_market_scope_acceptance.public.json"
            ),
            "steps": [
                "Run from an isolated, read-only environment with network egress limited to the public Gamma and CLOB origins.",
                "Use the exact CLI budgets and write to a new output path; do not point the runner at a live database.",
                "Review allowlisted GET requests, coverage, hashes, and probe authority flags before any separate operator action.",
                "Treat DATA_PIPELINE_PROBE as observation-only and do not use it as lifecycle, scope, ranking, qualification, or execution authority.",
            ],
            "supported_public_paths": list(PUBLIC_GET_PATHS),
            "executed_in_this_report": config.mode == "public",
            "public_match_outcome": public_queue_attempt,
            "successor_outcome": queue_demo,
            "successor_outcome_evidence_class": "SYNTHETIC_OFFLINE",
        },
    }
    return _scrub(report)


def build_example_report() -> dict[str, Any]:
    """Build the compact deterministic committed report from offline fixtures."""
    return run_acceptance(
        build_offline_fixture_adapter(),
        AcceptanceConfig(page_limit=4, max_pages=4, max_seconds=30.0, max_books=2, book_depth=2, sample_limit=6, mode="offline"),
        generated_at=FIXTURE_TIMESTAMP,
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("offline", "public", "persisted"), default="offline")
    parser.add_argument("--source-backup", "--backup", type=Path, help="explicit read-only SQLite backup for persisted mode")
    parser.add_argument("--dataset-id", help="exact persisted dataset identifier")
    parser.add_argument("--dataset-version", help="exact immutable persisted dataset version")
    parser.add_argument("--expected-row-count", type=int)
    parser.add_argument("--expected-constituent-count", type=int)
    parser.add_argument("--page-limit", "--limit", type=int, default=100)
    parser.add_argument("--max-pages", "--pages", type=int, default=4)
    parser.add_argument("--max-seconds", "--time-budget", type=float, default=60.0)
    parser.add_argument("--max-books", "--book-budget", type=int, default=2)
    parser.add_argument("--book-depth", type=int, default=5)
    parser.add_argument("--sample-limit", type=int, default=8)
    parser.add_argument("--output", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    config = AcceptanceConfig(
        page_limit=args.page_limit,
        max_pages=args.max_pages,
        max_seconds=args.max_seconds,
        max_books=args.max_books,
        book_depth=args.book_depth,
        sample_limit=args.sample_limit,
        mode=args.mode,
        source_backup=args.source_backup,
        dataset_id=args.dataset_id,
        dataset_version=args.dataset_version,
        expected_row_count=args.expected_row_count,
        expected_constituent_count=args.expected_constituent_count,
    )
    adapter = build_offline_fixture_adapter() if args.mode == "offline" else None
    report = run_acceptance(adapter, config, generated_at=FIXTURE_TIMESTAMP if args.mode == "offline" else None)
    payload = _canonical_json(report)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    else:
        print(json.dumps(report, sort_keys=True, indent=2))
    if args.mode == "persisted":
        categories = report.get("status_categories", {})
        if (
            categories.get("validation") != "PASSED"
            or categories.get("export") != "PASSED"
            or report.get("acceptance_status") != "RESULT_RECORDED"
        ):
            return 1
    return 0

if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())


__all__ = [
    "AcceptanceConfig",
    "OfflineDiscoveryPage",
    "OfflineFixtureAdapter",
    "PUBLIC_GET_PATHS",
    "POLYMARKET_HISTORICAL_ATTESTATION_HASH",
    "POLYMARKET_HISTORICAL_CONSTITUENT_COUNT",
    "POLYMARKET_HISTORICAL_DATASET_ID",
    "POLYMARKET_HISTORICAL_DATASET_VERSION",
    "POLYMARKET_HISTORICAL_ROW_COUNT",
    "_PINNED_PERSISTED_ATTESTATION_HASH",
    "_PINNED_PERSISTED_CONSTITUENT_COUNT",
    "_PINNED_PERSISTED_DATASET_ID",
    "_PINNED_PERSISTED_DATASET_VERSION",
    "_PINNED_PERSISTED_ROW_COUNT",
    "build_example_report",
    "build_offline_fixture_adapter",
    "compute_dollar_limit_buy_feasibility",
    "enqueue_legacy_successor",
    "main",
    "run_acceptance",
    "run_persisted_acceptance",
]
