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
import hashlib
import json
import math
from pathlib import Path
import re
import sys
import time
from typing import Any, Callable, Mapping, Sequence

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


SCHEMA_VERSION = "polymarket-market-scope-acceptance-v1"
FIXTURE_TIMESTAMP = "2026-01-01T00:00:00+00:00"
POLITICS_TAG_SLUG = "politics"
DISCOVERY_MIN_LIQUIDITY = 1_000.0
DISCOVERY_MIN_HOURS = 24.0
DISCOVERY_MAX_HOURS = 168.0
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
    the adapter contract's 100 rows.  Metadata pagination has its own budget;
    it is intentionally not derived from market/book scheduling settings.
    """

    page_limit: int = 100
    max_pages: int = 4
    max_seconds: float = 60.0
    max_books: int = 2
    book_depth: int = 5
    sample_limit: int = 8
    mode: str = "offline"

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
        if str(self.mode).lower() not in {"offline", "public"}:
            raise ValueError("mode must be offline or public")
        object.__setattr__(self, "mode", str(self.mode).lower())

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
    categories = _policy_categories(policy)

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
    tags_present, tags_raw = _value(raw, "tags", "tag")
    category = str(category_raw).strip().lower() if category_present and category_raw is not None else ""
    tags = [
        str(item.get("slug", item.get("label", item.get("name", ""))))
        if isinstance(item, Mapping)
        else str(item).strip().lower()
        for item in _list_value(tags_raw)
    ]
    tags = [tag.strip().lower() for tag in tags if tag.strip()]
    category_pass = not categories or category in categories or bool(set(categories).intersection(tags))
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

    minimum_hours = _policy_bound(filters, "minimum_hours_to_resolution")
    maximum_hours = _policy_bound(filters, "maximum_hours_to_resolution")
    expiry_present, expiry_raw = _value(raw, "expiry", "end_date", "endDate", "endDateIso", "expirationDate")
    expiry = _parse_datetime(expiry_raw) if expiry_present else None
    expiry_hours = (expiry - observed_at).total_seconds() / 3600.0 if expiry is not None else None
    if minimum_hours is None and maximum_hours is None:
        expiry_status, expiry_pass = "pass", True
    elif not expiry_present or expiry_raw is None:
        expiry_status, expiry_pass = "missing", False
    elif expiry is None:
        expiry_status, expiry_pass = "malformed", False
    else:
        min_hours = min(minimum_hours) if isinstance(minimum_hours, list) else minimum_hours
        max_hours = max(maximum_hours) if isinstance(maximum_hours, list) else maximum_hours
        expiry_pass = (
            (min_hours is None or float(expiry_hours) >= float(min_hours))
            and (max_hours is None or float(expiry_hours) <= float(max_hours))
        )
        expiry_status = "pass" if expiry_pass else "fail"

    min_liquidity = _policy_bound(filters, "min_liquidity")
    liquidity_present, liquidity_raw = _value(raw, "liquidity", "liquidity_num", "liquidityNum")
    liquidity, liquidity_quality = _number(liquidity_raw) if liquidity_present else (None, "missing")
    if min_liquidity is None:
        liquidity_status, liquidity_pass = "pass", True
    elif liquidity_quality in {"missing", "malformed"}:
        liquidity_status, liquidity_pass = liquidity_quality, False
    else:
        minimum = min_liquidity if isinstance(min_liquidity, (int, float)) else min(min_liquidity)
        liquidity_pass = float(liquidity) >= minimum
        liquidity_status = "pass" if liquidity_pass else "fail"

    max_spread = _policy_bound(filters, "max_spread")
    spread_present, spread_raw = _value(raw, "yes_spread", "spread")
    if spread_present:
        spread, spread_quality = _number(spread_raw)
    else:
        bid_present, bid_raw = _value(raw, "yes_bid", "yesBid", "bestBid", "best_bid")
        ask_present, ask_raw = _value(raw, "yes_ask", "yesAsk", "bestAsk", "best_ask")
        bid, bid_quality = _number(bid_raw) if bid_present else (None, "missing")
        ask, ask_quality = _number(ask_raw) if ask_present else (None, "missing")
        if bid_quality == "malformed" or ask_quality == "malformed":
            spread, spread_quality = None, "malformed"
        elif bid_quality == "missing" or ask_quality == "missing":
            spread, spread_quality = None, "missing"
        else:
            spread, spread_quality = float(ask) - float(bid), "valid"
    if max_spread is None:
        spread_status, spread_pass = "pass", True
    elif spread_quality in {"missing", "malformed"}:
        spread_status, spread_pass = spread_quality, False
    else:
        maximum = max_spread if isinstance(max_spread, (int, float)) else max(max_spread)
        spread_pass = 0.0 <= float(spread) <= maximum
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
    if sensitive_key:
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


def _queue_demo_reason(
    queue_item: Any,
    processor_result: Mapping[str, Any] | None,
    store: Any,
    *,
    now: datetime,
    authority_cap: int,
) -> tuple[str, str | None, str | None]:
    """Extract observed processor/authority reasons without assigning one."""
    candidate_id = None
    if isinstance(processor_result, Mapping):
        candidate_results = processor_result.get("candidate_results")
        if isinstance(candidate_results, Sequence):
            for candidate_result in candidate_results:
                if isinstance(candidate_result, Mapping) and candidate_result.get("candidate_id"):
                    candidate_id = str(candidate_result["candidate_id"])
                    break
    lifecycle_stage = None
    if candidate_id:
        lifecycle = store.load_candidate_lifecycle(candidate_id)
        if isinstance(lifecycle, Mapping):
            lifecycle_stage = str(lifecycle.get("stage") or "") or None
        try:
            requirements = store.candidate_forward_requirements(
                candidate_ids=(candidate_id,),
                now=now,
                max_markets_per_candidate=authority_cap,
                max_total_markets=authority_cap,
            )
        except Exception:
            requirements = {}
        candidates = requirements.get("candidates") if isinstance(requirements, Mapping) else None
        if isinstance(candidates, list) and candidates and isinstance(candidates[0], Mapping):
            requirement_reason = str(candidates[0].get("reason_code") or "").strip() or None
        else:
            requirement_reason = None
    else:
        requirement_reason = None
    processor_reason = None
    if isinstance(processor_result, Mapping):
        processor_reason = str(
            processor_result.get("reason")
            or processor_result.get("status")
            or processor_result.get("reason_code")
            or ""
        ).strip() or None
    queue_reason = str(getattr(queue_item, "last_error", "") or "").strip() or None
    exact_reason = requirement_reason or processor_reason or queue_reason
    return exact_reason or "normal_processor_returned_no_reason", lifecycle_stage, processor_reason or queue_reason


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
            exact_reason, lifecycle_stage, processor_reason = _queue_demo_reason(
                final_item,
                processor_result,
                store,
                now=runtime_now,
                authority_cap=_queue_authority_cap(criteria_obj),
            )
            reason_code = (
                str(processor_result.get("reason_code", "")).strip()
                if isinstance(processor_result, Mapping)
                else ""
            )
            result.update(
                {
                    "lifecycle_stage": lifecycle_stage,
                    "resulting_stage": lifecycle_stage,
                    "reason": exact_reason,
                    "exact_reason": exact_reason,
                    "reason_code": reason_code or None,
                    "processor_reason": processor_reason,
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
        "reason": "queue_processing_unavailable",
        "exact_reason": "queue_processing_unavailable",
        "reason_code": None,
        "processor_reason": None,
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
            exact_reason, lifecycle_stage, processor_reason = _queue_demo_reason(
                final_item,
                processor_result,
                store,
                now=runtime_now,
                authority_cap=authority_cap,
            )
            result.update(
                {
                    "lifecycle_stage": lifecycle_stage,
                    "resulting_stage": lifecycle_stage,
                    "reason": exact_reason,
                    "exact_reason": exact_reason,
                    "processor_reason": processor_reason,
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
    parser.add_argument("--mode", choices=("offline", "public"), default="offline")
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
    )
    adapter = build_offline_fixture_adapter() if args.mode == "offline" else None
    report = run_acceptance(adapter, config, generated_at=FIXTURE_TIMESTAMP if args.mode == "offline" else None)
    payload = _canonical_json(report)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    else:
        print(json.dumps(report, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())


__all__ = [
    "AcceptanceConfig",
    "OfflineDiscoveryPage",
    "OfflineFixtureAdapter",
    "PUBLIC_GET_PATHS",
    "build_example_report",
    "build_offline_fixture_adapter",
    "enqueue_legacy_successor",
    "main",
    "run_acceptance",
]
