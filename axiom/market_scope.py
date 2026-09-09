"""Pure, typed current-market scope resolution.

This module is deliberately independent from providers and transport.  A caller
supplies a frozen candidate document and a bounded snapshot of persisted current
markets; resolution returns an immutable, serializable decision which can be
persisted by :class:`axiom.storage.AxiomStore`.
"""
from __future__ import annotations

from collections.abc import Mapping as MappingABC
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
import hashlib
import json
import math
from types import MappingProxyType
from typing import Any, Iterable, Mapping, Sequence

from .domain import ensure_utc, parse_timestamp, utc_now

MARKET_SCOPE_RESOLUTION_SCHEMA_VERSION = "1"
MARKET_SCOPE_RESOLUTION_VERSION = MARKET_SCOPE_RESOLUTION_SCHEMA_VERSION
MAX_SCOPE_MARKETS = 1_000
MAX_SCOPE_MATCHES = 100


class MarketScopeResolutionStatus(str, Enum):
    INVALID_POLICY = "INVALID_POLICY"
    RESEARCH_ONLY = "RESEARCH_ONLY"
    ZERO_MATCHES = "ZERO_MATCHES"
    MATCHED = "MATCHED"
    PARTIAL = "PARTIAL"
    # Descriptive aliases preserve one persisted status vocabulary.
    PARTIAL_MATCHED = "PARTIAL"
    PARTIAL_DEFERRED = "PARTIAL"
    DEFERRED = "DEFERRED"
    DEFERRED_MARKETS = "DEFERRED"


# Short aliases are useful to consumers that prefer constants over Enum values.
INVALID_POLICY = MarketScopeResolutionStatus.INVALID_POLICY.value
RESEARCH_ONLY = MarketScopeResolutionStatus.RESEARCH_ONLY.value
ZERO_MATCHES = MarketScopeResolutionStatus.ZERO_MATCHES.value
MATCHED = MarketScopeResolutionStatus.MATCHED.value
PARTIAL = MarketScopeResolutionStatus.PARTIAL.value
DEFERRED = MarketScopeResolutionStatus.DEFERRED.value


def _plain(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        return ensure_utc(value).isoformat()
    if isinstance(value, Mapping):
        return {str(key): _plain(child) for key, child in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(child) for child in value]
    if isinstance(value, (set, frozenset)):
        return [_plain(child) for child in sorted(value, key=str)]
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("scope payload contains a non-finite number")
    return value


def _canonical(value: Any) -> str:
    return json.dumps(_plain(value), sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def _text(value: Any) -> str:
    return str(value).strip() if value is not None else ""


def _first(mapping: Mapping[str, Any], *names: str) -> Any:
    for name in names:
        if name in mapping and mapping[name] not in (None, ""):
            return mapping[name]
    return None


def _nested(mapping: Mapping[str, Any], *names: str) -> Any:
    value = _first(mapping, *names)
    if value is not None:
        return value
    for child_name in ("payload", "metadata", "metadata_provenance", "provenance", "snapshot", "extra"):
        child = mapping.get(child_name)
        if isinstance(child, Mapping):
            value = _nested(child, *names)
            if value is not None:
                return value
    return None


def _bool(value: Any, default: bool | None = None) -> bool | None:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    text = _text(value).casefold()
    if text in {"1", "true", "yes", "y", "on", "active", "open"}:
        return True
    if text in {"0", "false", "no", "n", "off", "inactive", "closed"}:
        return False
    return default


@dataclass(frozen=True, slots=True)
class CurrentMarket:
    """Current market identity and provider metadata used by forward scope."""

    market_id: str
    condition_id: str
    yes_token_id: str
    no_token_id: str
    metadata_provenance: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for name in ("market_id", "condition_id", "yes_token_id", "no_token_id"):
            value = _text(getattr(self, name))
            if not value:
                raise ValueError(f"current market {name} is required")
            object.__setattr__(self, name, value)
        identities = (self.market_id, self.condition_id, self.yes_token_id, self.no_token_id)
        if len(set(identities)) != len(identities):
            raise ValueError("current market identity fields must be distinct")
        provenance = _plain(self.metadata_provenance)
        if not isinstance(provenance, Mapping):
            raise TypeError("metadata_provenance must be a mapping")
        object.__setattr__(self, "metadata_provenance", MappingProxyType(dict(provenance)))

    @property
    def metadata(self) -> Mapping[str, Any]:
        """Compatibility alias for callers naming the provenance metadata."""
        return self.metadata_provenance

    @property
    def condition(self) -> str:
        return self.condition_id

    @property
    def yes_token(self) -> str:
        return self.yes_token_id

    @property
    def no_token(self) -> str:
        return self.no_token_id

    def as_dict(self) -> dict[str, Any]:
        return {
            "market_id": self.market_id,
            "condition_id": self.condition_id,
            "yes_token_id": self.yes_token_id,
            "no_token_id": self.no_token_id,
            "metadata_provenance": _plain(self.metadata_provenance),
        }

    to_dict = as_dict

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "CurrentMarket":
        if not isinstance(value, Mapping):
            raise TypeError("current market must be a mapping")

        def decode_jsonish(raw: Any) -> Any:
            if isinstance(raw, str):
                try:
                    return json.loads(raw)
                except (TypeError, ValueError):
                    return None
            return raw

        def token_mapping(raw: Any, outcomes: Any) -> dict[str, Any]:
            raw = decode_jsonish(raw)
            outcomes = decode_jsonish(outcomes)
            if isinstance(raw, Mapping):
                return {
                    str(outcome).strip().casefold(): token
                    for outcome, token in raw.items()
                    if str(outcome).strip() and token not in (None, "")
                }
            if isinstance(raw, (list, tuple)) and all(isinstance(item, Mapping) for item in raw):
                result: dict[str, Any] = {}
                for item in raw:
                    outcome = _nested(item, "outcome", "name", "label")
                    token = _nested(item, "token_id", "tokenId", "id")
                    if outcome is not None and token not in (None, ""):
                        result[_text(outcome).casefold()] = token
                return result
            if isinstance(raw, (list, tuple)) and isinstance(outcomes, (list, tuple)):
                return {
                    str(outcome).strip().casefold(): token
                    for outcome, token in zip(outcomes, raw)
                    if str(outcome).strip() and token not in (None, "")
                }
            return {}

        outcomes = value.get("outcomes")
        token_ids: dict[str, Any] = {}
        for raw_tokens in (
            value.get("token_ids"),
            value.get("clob_token_ids"),
            value.get("clobTokenIds"),
            value.get("tokens"),
        ):
            token_ids = token_mapping(raw_tokens, outcomes)
            if token_ids:
                break
        provenance = value.get("metadata_provenance", value.get("provenance", {}))
        if not isinstance(provenance, Mapping):
            provenance = {}
        return cls(
            _nested(value, "market_id", "id", "market") or "",
            _nested(value, "condition_id", "conditionId", "condition") or "",
            _nested(value, "yes_token_id", "yesTokenId", "yesTokenID", "yes_token", "yes")
            or token_ids.get("yes", ""),
            _nested(value, "no_token_id", "noTokenId", "noTokenID", "no_token", "no")
            or token_ids.get("no", ""),
            provenance,
        )


@dataclass(frozen=True, slots=True)
class MarketScopeDisposition:
    """A market excluded from authority or deferred for a later bounded read."""

    market_id: str
    reason: str
    detail: str = ""
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        market_id = _text(self.market_id) or "<unknown>"
        reason = _text(self.reason).upper()
        if not reason:
            raise ValueError("market disposition reason is required")
        object.__setattr__(self, "market_id", market_id)
        object.__setattr__(self, "reason", reason)
        object.__setattr__(self, "detail", _text(self.detail))
        metadata = _plain(self.metadata)
        if not isinstance(metadata, Mapping):
            raise TypeError("market disposition metadata must be a mapping")
        object.__setattr__(self, "metadata", MappingProxyType(dict(metadata)))

    def as_dict(self) -> dict[str, Any]:
        return {
            "market_id": self.market_id,
            "reason": self.reason,
            "detail": self.detail,
            "metadata": _plain(self.metadata),
        }

    to_dict = as_dict

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "MarketScopeDisposition":
        if not isinstance(value, Mapping):
            raise TypeError("market disposition must be a mapping")
        return cls(value.get("market_id", "<unknown>"), value.get("reason", "UNKNOWN"), value.get("detail", ""), value.get("metadata", {}))


# Explicit names for API discoverability.
ResolvedMarket = CurrentMarket
ExcludedMarket = MarketScopeDisposition
DeferredMarket = MarketScopeDisposition


def _resolution_identity(candidate_id: str, scope_hash: str, scope_version: str, resolved_at: datetime) -> str:
    material = {
        "schema_version": MARKET_SCOPE_RESOLUTION_SCHEMA_VERSION,
        "candidate_id": candidate_id,
        "scope_hash": scope_hash,
        "scope_version": scope_version,
        "resolved_at": ensure_utc(resolved_at).isoformat(),
    }
    return "sha256:" + hashlib.sha256(_canonical(material).encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class MarketScopeResolution(MappingABC[str, Any]):
    """Immutable result of resolving one frozen candidate against current markets."""

    candidate_id: str
    scope_hash: str
    scope_version: str
    resolved_at: datetime
    status: str | MarketScopeResolutionStatus
    reason: str
    policy: Mapping[str, Any] = field(default_factory=dict)
    matched_markets: tuple[CurrentMarket, ...] = ()
    excluded_markets: tuple[MarketScopeDisposition, ...] = ()
    deferred_markets: tuple[MarketScopeDisposition, ...] = ()
    provenance: Mapping[str, Any] = field(default_factory=dict)
    schema_version: str = MARKET_SCOPE_RESOLUTION_SCHEMA_VERSION
    resolution_id: str = ""

    def __post_init__(self) -> None:
        candidate_id = _text(self.candidate_id)
        scope_hash = _text(self.scope_hash)
        scope_version = _text(self.scope_version)
        if not candidate_id or not scope_hash or not scope_version:
            raise ValueError("candidate_id, scope_hash, and scope_version are required")
        schema_version = _text(self.schema_version)
        if schema_version != MARKET_SCOPE_RESOLUTION_SCHEMA_VERSION:
            raise ValueError(f"unsupported market scope resolution schema {schema_version!r}")
        status = self.status.value if isinstance(self.status, MarketScopeResolutionStatus) else _text(self.status).upper()
        if status not in {item.value for item in MarketScopeResolutionStatus}:
            raise ValueError(f"unsupported market scope resolution status {status!r}")
        reason = _text(self.reason).upper()
        if not reason:
            raise ValueError("resolution reason is required")
        stamp = self.resolved_at if isinstance(self.resolved_at, datetime) else parse_timestamp(self.resolved_at)
        if stamp is None:
            raise ValueError("resolved_at must be a datetime or timestamp")
        policy = _plain(self.policy)
        provenance = _plain(self.provenance)
        if not isinstance(policy, Mapping) or not isinstance(provenance, Mapping):
            raise TypeError("policy and provenance must be mappings")
        matched: list[CurrentMarket] = []
        for item in self.matched_markets:
            matched.append(item if isinstance(item, CurrentMarket) else CurrentMarket.from_mapping(item))
        excluded = tuple(item if isinstance(item, MarketScopeDisposition) else MarketScopeDisposition.from_mapping(item) for item in self.excluded_markets)
        deferred = tuple(item if isinstance(item, MarketScopeDisposition) else MarketScopeDisposition.from_mapping(item) for item in self.deferred_markets)
        identity = _resolution_identity(candidate_id, scope_hash, scope_version, stamp)
        supplied = _text(self.resolution_id)
        if supplied and supplied != identity:
            raise ValueError("resolution_id does not match immutable resolution identity")
        object.__setattr__(self, "candidate_id", candidate_id)
        object.__setattr__(self, "scope_hash", scope_hash)
        object.__setattr__(self, "scope_version", scope_version)
        object.__setattr__(self, "resolved_at", ensure_utc(stamp))
        object.__setattr__(self, "status", status)
        object.__setattr__(self, "reason", reason)
        object.__setattr__(self, "policy", MappingProxyType(dict(policy)))
        object.__setattr__(self, "matched_markets", tuple(matched))
        object.__setattr__(self, "excluded_markets", excluded)
        object.__setattr__(self, "deferred_markets", deferred)
        object.__setattr__(self, "provenance", MappingProxyType(dict(provenance)))
        object.__setattr__(self, "schema_version", schema_version)
        object.__setattr__(self, "resolution_id", identity)

    @property
    def scope_hash_version(self) -> tuple[str, str]:
        return self.scope_hash, self.scope_version
    @property
    def market_scope_hash(self) -> str:
        return self.scope_hash

    @property
    def market_scope_version(self) -> str:
        return self.scope_version

    @property
    def resolution_timestamp(self) -> datetime:
        return self.resolved_at

    @property
    def frozen_scope_hash(self) -> str:
        return self.scope_hash

    @property
    def frozen_scope_version(self) -> str:
        return self.scope_version

    @property
    def matched(self) -> tuple[CurrentMarket, ...]:
        return self.matched_markets

    @property
    def excluded(self) -> tuple[MarketScopeDisposition, ...]:
        return self.excluded_markets

    @property
    def deferred(self) -> tuple[MarketScopeDisposition, ...]:
        return self.deferred_markets

    def __getitem__(self, key: str) -> Any:
        return self.as_dict()[key]

    def __iter__(self):
        return iter(self.as_dict())

    def __len__(self) -> int:
        return len(self.as_dict())
    @property
    def matched_market_ids(self) -> tuple[str, ...]:
        return tuple(item.market_id for item in self.matched_markets)

    @property
    def excluded_market_ids(self) -> tuple[str, ...]:
        return tuple(item.market_id for item in self.excluded_markets)

    @property
    def deferred_market_ids(self) -> tuple[str, ...]:
        return tuple(item.market_id for item in self.deferred_markets)

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "resolution_id": self.resolution_id,
            "candidate_id": self.candidate_id,
            "scope_hash": self.scope_hash,
            "scope_version": self.scope_version,
            "resolved_at": self.resolved_at.isoformat(),
            "status": self.status,
            "reason": self.reason,
            "policy": _plain(self.policy),
            "matched_markets": [item.as_dict() for item in self.matched_markets],
            "excluded_markets": [item.as_dict() for item in self.excluded_markets],
            "deferred_markets": [item.as_dict() for item in self.deferred_markets],
            "provenance": _plain(self.provenance),
        }

    to_dict = as_dict
    as_record = as_dict

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "MarketScopeResolution":
        if not isinstance(value, Mapping):
            raise TypeError("market scope resolution must be a mapping")
        return cls(
            candidate_id=value.get("candidate_id", ""),
            scope_hash=value.get("scope_hash", ""),
            scope_version=value.get("scope_version", value.get("version", "")),
            resolved_at=value.get("resolved_at", value.get("resolution_timestamp")),
            status=value.get("status", "INVALID_POLICY"),
            reason=value.get("reason", "UNKNOWN"),
            policy=value.get("policy", {}),
            matched_markets=tuple(
                item if isinstance(item, CurrentMarket) else CurrentMarket.from_mapping(item)
                for item in value.get("matched_markets", ()) or ()
            ),
            excluded_markets=tuple(
                item if isinstance(item, MarketScopeDisposition) else MarketScopeDisposition.from_mapping(item)
                for item in value.get("excluded_markets", ()) or ()
            ),
            deferred_markets=tuple(
                item if isinstance(item, MarketScopeDisposition) else MarketScopeDisposition.from_mapping(item)
                for item in value.get("deferred_markets", ()) or ()
            ),
            provenance=value.get("provenance", {}),
            schema_version=value.get("schema_version", MARKET_SCOPE_RESOLUTION_SCHEMA_VERSION),
            resolution_id=value.get("resolution_id", ""),
        )


# Compatibility spelling used by some integrations.
CurrentMarketResolution = MarketScopeResolution


def _scope_source(document: Mapping[str, Any]) -> Mapping[str, Any]:
    plan = document.get("experiment_plan")
    return plan if isinstance(plan, Mapping) else document


def _extract_policy(document: Mapping[str, Any]) -> tuple[Any, str, str, str | None]:
    """Return (policy, hash, version, provenance) without mutating document."""
    from .experiment_plan import ExperimentPlanError, MarketScopePolicy, normalize_market_scope

    source = _scope_source(document)
    explicit = source.get("market_scope")
    if explicit is None and source is not document:
        explicit = document.get("market_scope")
    declared_hash = _first(source, "market_scope_hash", "scope_hash") or _first(document, "market_scope_hash", "scope_hash")
    declared_version = _first(source, "market_scope_version", "scope_version") or _first(document, "market_scope_version", "scope_version")
    try:
        if explicit is not None:
            policy = MarketScopePolicy.from_mapping(explicit)
        else:
            # Existing frozen candidates use these legacy sources.  Passing the
            # values separately lets normalize_market_scope detect conflicts.
            policy = normalize_market_scope(
                None,
                target=source.get("target"),
                market_ids=source.get("market_ids"),
                target_market_ids=source.get("target_market_ids"),
                target_instrument=source.get("target_instrument"),
                instrument=source.get("instrument"),
                categories=source.get("categories"),
                filters=source.get("filters", source.get("frozen_filters")),
                regime_restrictions=source.get("regime_restrictions"),
            )
        if declared_hash is not None and _text(declared_hash) != policy.scope_hash:
            raise ExperimentPlanError("CONFLICTING_MARKET_SCOPE", "frozen market scope hash differs from canonical material")
        if declared_version is not None and _text(declared_version) != policy.scope_version:
            raise ExperimentPlanError("CONFLICTING_MARKET_SCOPE", "frozen market scope version differs from canonical material")
        return policy, policy.scope_hash, policy.scope_version, policy.provenance
    except (ExperimentPlanError, TypeError, ValueError) as exc:
        # Invalid material still receives a deterministic identity.  The
        # original frozen document is retained verbatim by its owner.
        fallback = _text(declared_hash) or "invalid:" + hashlib.sha256(_canonical(source).encode("utf-8")).hexdigest()
        version = _text(declared_version) or MARKET_SCOPE_RESOLUTION_SCHEMA_VERSION
        reason = getattr(exc, "reason", None) or ("AMBIGUOUS_POLICY" if "conflict" in str(exc).casefold() else "MALFORMED_POLICY")
        return None, fallback, version, reason


def _record_payload(record: Mapping[str, Any]) -> dict[str, Any]:
    payload = record.get("payload")
    result = dict(payload) if isinstance(payload, Mapping) else {}
    for key, value in record.items():
        if key != "payload":
            result.setdefault(str(key), value)
    return result


def _current_market(record: Any) -> tuple[CurrentMarket | None, dict[str, Any], str | None]:
    if isinstance(record, CurrentMarket):
        raw = record.as_dict()
        raw.update(
            {
                str(key): value
                for key, value in record.metadata_provenance.items()
                if str(key) not in raw
            }
        )
        return record, raw, None
    if not isinstance(record, Mapping):
        return None, {}, "MALFORMED_MARKET_RECORD"
    raw = _record_payload(record)
    try:
        current = CurrentMarket.from_mapping(raw)
    except (TypeError, ValueError):
        market_id = _text(_nested(raw, "market_id", "id", "market")) or "<unknown>"
        return None, raw, "MISSING_MARKET_IDENTITY"
    if not current.metadata_provenance:
        provenance: dict[str, Any] = {}
        for name in ("source_type", "metadata_hash", "observed_at", "source_timestamp", "provider", "provider_timestamp"):
            value = _nested(raw, name)
            if value is not None:
                provenance[name] = _plain(value)
        current = CurrentMarket(current.market_id, current.condition_id, current.yes_token_id, current.no_token_id, provenance)
    return current, raw, None
def _historical_constituents(document: Mapping[str, Any]) -> tuple[str, ...]:
    """Return only dataset-provenance-bound market ids."""
    try:
        from .experiment_plan import historical_market_ids

        return historical_market_ids(document)
    except (ImportError, TypeError, ValueError):
        return ()


def _is_closed(raw: Mapping[str, Any], now: datetime) -> bool:
    closed = _nested(raw, "closed", "archived")
    if _bool(closed, False):
        return True
    active = _bool(_nested(raw, "active"), None)
    if active is False:
        return True
    settlement = _text(_nested(raw, "settlement", "outcome")).casefold()
    if settlement in {"resolved_yes", "resolved_no", "void", "closed", "expired"}:
        return True
    expiry = parse_timestamp(_nested(raw, "expiry", "end_date", "endDate"))
    return expiry is not None and expiry <= now
def _current_eligibility(raw: Mapping[str, Any], now: datetime) -> tuple[str, str]:
    """Return ``(action, reason)`` for mandatory current-market checks.

    These are not caller-selectable policy filters.  A forward scope can only
    authorize an active, unresolved Polymarket market with an accepting order
    book and complete YES/NO identity.  Missing provider state is deferred
    because it cannot safely be treated as either eligible or ineligible.
    """
    source_type = _text(_nested(raw, "source_type")).upper()
    if source_type and source_type != "CURRENT":
        return "exclude", "NON_CURRENT_MARKET"
    venue = _text(_nested(raw, "venue"))
    instrument = _text(_nested(raw, "instrument", "instrument_type"))
    provider = _text(_nested(raw, "provider", "source"))
    explicit_values = {item.casefold() for item in (venue, instrument) if item}
    generic = {"prediction", "prediction_market", "predictionmarket", "market"}
    if explicit_values:
        known_non_polymarket = {item for item in explicit_values if item not in generic and item != "polymarket"}
        if known_non_polymarket:
            return "exclude", "VENUE_MISMATCH"
        if not any(item == "polymarket" for item in explicit_values):
            if not provider:
                return "defer", "VENUE_UNKNOWN"
            if provider.casefold() != "polymarket":
                return "exclude", "VENUE_MISMATCH"
    elif provider:
        if provider.casefold() != "polymarket":
            return "exclude", "VENUE_MISMATCH"
    else:
        return "defer", "VENUE_UNKNOWN"

    active = _bool(_nested(raw, "active"), None)
    open_state = _bool(_nested(raw, "open"), None)
    if active is None and open_state is None:
        return "defer", "ACTIVE_UNKNOWN"
    if active is None:
        active = open_state
    if open_state is None:
        open_state = active
    if not active or not open_state:
        return "exclude", "INACTIVE_MARKET"
    archived = _bool(_nested(raw, "archived"), None)
    if archived:
        return "exclude", "MARKET_CLOSED"
    closed = _bool(_nested(raw, "closed"), None)
    if closed is None:
        return "defer", "CLOSED_UNKNOWN"
    if closed:
        return "exclude", "MARKET_CLOSED"
    settlement = _text(_nested(raw, "settlement", "outcome", "resolution_status")).casefold()
    resolved_flag = _bool(_nested(raw, "resolved", "is_resolved"), None)
    if resolved_flag is True or settlement in {"resolved_yes", "resolved_no", "void", "closed", "expired"}:
        return "exclude", "RESOLVED_MARKET"

    accepting = _bool(_nested(raw, "accepting_orders", "acceptingOrders"), None)
    if accepting is None:
        return "defer", "ACCEPTING_ORDERS_UNKNOWN"
    if not accepting:
        return "exclude", "ACCEPTING_ORDERS_FALSE"
    book = _bool(
        _nested(raw, "order_book_available", "book_available", "bookAvailable", "enable_order_book", "enableOrderBook", "book"),
        None,
    )
    if book is None:
        return "defer", "ORDER_BOOK_UNKNOWN"
    if not book:
        return "exclude", "ORDER_BOOK_UNAVAILABLE"
    expiry = parse_timestamp(_nested(raw, "expiry", "end_date", "endDate"))
    if expiry is not None and expiry <= now:
        return "exclude", "MARKET_EXPIRED"
    return "eligible", ""
    
    

def _with_official_category(raw: Mapping[str, Any], filters: Mapping[str, Any]) -> dict[str, Any]:
    """Use an exact category tag only when the record has no category field."""
    if "category" not in filters or _nested(raw, "category") is not None:
        return dict(raw)
    expected = filters.get("category")
    allowed = expected if isinstance(expected, (list, tuple)) else (expected,)
    allowed_values = {str(item).strip().casefold() for item in allowed if str(item).strip()}
    tags_value = _nested(raw, "tags")
    if not tags_value:
        tags_value = _nested(raw, "tag")
    if not tags_value:
        tags_value = _nested(raw, "categories")
    tags = (tags_value,) if isinstance(tags_value, str) else tags_value
    if isinstance(tags, (list, tuple, set, frozenset)):
        for tag in tags:
            if str(tag).strip().casefold() in allowed_values:
                enriched = dict(raw)
                enriched["category"] = str(tag).strip().casefold()
                return enriched
    return dict(raw)
def _rule_mismatch_reason(
    raw: Mapping[str, Any],
    filters: Mapping[str, Any],
    *,
    now: datetime,
    target_instrument: str | None,
) -> str:
    from .experiment_plan import forward_market_matches

    enriched = _with_official_category(raw, filters)
    if target_instrument is not None and not forward_market_matches(
        enriched,
        {},
        now=now,
        target_instrument=target_instrument,
    ):
        return "INSTRUMENT_MISMATCH"
    field_reasons = {
        "entry_price": "PRICE_MISMATCH",
        "minimum_hours_to_resolution": "EXPIRY_MISMATCH",
        "maximum_hours_to_resolution": "EXPIRY_MISMATCH",
        "min_liquidity": "LIQUIDITY_MISMATCH",
        "max_spread": "SPREAD_MISMATCH",
        "category": "CATEGORY_MISMATCH",
        "regime": "REGIME_MISMATCH",
        "regimes": "REGIME_MISMATCH",
    }
    for key, value in filters.items():
        if not forward_market_matches(enriched, {key: value}, now=now):
            return field_reasons.get(str(key), "RULE_MISMATCH")
    return "RULE_MISMATCH"


def _policy_forward_filters(policy: Any) -> Mapping[str, Any]:
    """Combine normalized filters with normalized regime restrictions."""
    from .experiment_plan import normalize_forward_filters

    return normalize_forward_filters(policy.filters, policy.regime_restrictions)


def resolve_market_scope(
    candidate_id: str,
    frozen_document: Mapping[str, Any],
    current_markets: Iterable[Mapping[str, Any] | CurrentMarket],
    *,
    resolved_at: datetime | None = None,
    max_matches: int = MAX_SCOPE_MATCHES,
    max_markets: int = MAX_SCOPE_MARKETS,
) -> MarketScopeResolution:
    """Resolve a frozen scope against supplied current records, fail closed."""
    identifier = _text(candidate_id)
    if not identifier:
        raise ValueError("candidate_id is required")
    if isinstance(max_matches, bool) or not isinstance(max_matches, int) or max_matches < 0 or max_matches > MAX_SCOPE_MATCHES:
        raise ValueError(f"max_matches must be an integer in [0,{MAX_SCOPE_MATCHES}]")
    if isinstance(max_markets, bool) or not isinstance(max_markets, int) or max_markets < 0 or max_markets > MAX_SCOPE_MARKETS:
        raise ValueError(f"max_markets must be an integer in [0,{MAX_SCOPE_MARKETS}]")
    stamp = ensure_utc(resolved_at or utc_now())
    if not isinstance(frozen_document, Mapping):
        scope_hash = "invalid:" + hashlib.sha256(_canonical(frozen_document).encode("utf-8")).hexdigest()
        return MarketScopeResolution(identifier, scope_hash, MARKET_SCOPE_RESOLUTION_SCHEMA_VERSION, stamp, INVALID_POLICY, "MALFORMED_POLICY", provenance={"source": "invalid"})
    policy, scope_hash, scope_version, policy_provenance = _extract_policy(frozen_document)
    historical_ids = set(_historical_constituents(frozen_document))
    if policy is None:
        reason = str(policy_provenance or "MALFORMED_POLICY").upper()
        status = RESEARCH_ONLY if reason == "CONFLICTING_MARKET_SCOPE" or reason == "AMBIGUOUS_POLICY" else INVALID_POLICY
        return MarketScopeResolution(
            identifier,
            scope_hash,
            scope_version,
            stamp,
            status,
            reason,
            provenance={"policy_provenance": "legacy" if status == RESEARCH_ONLY else "invalid"},
        )
    policy_dict = policy.as_dict()
    provenance = {"policy_provenance": policy_provenance, "resolution_source": "persisted_current_markets"}
    mode = policy.mode
    if mode == "RESEARCH_ONLY":
        excluded = tuple(MarketScopeDisposition(item, "HISTORICAL_CONSTITUENT") for item in sorted(historical_ids))
        return MarketScopeResolution(
            identifier,
            scope_hash,
            scope_version,
            stamp,
            RESEARCH_ONLY,
            RESEARCH_ONLY,
            policy_dict,
            excluded_markets=excluded,
            provenance=provenance,
        )
    forward_filters = _policy_forward_filters(policy)
    from .experiment_plan import forward_market_matches
    records: list[tuple[CurrentMarket | None, dict[str, Any], str | None]] = []
    iterator = iter(current_markets or ())
    for _ in range(max_markets):
        try:
            records.append(_current_market(next(iterator)))
        except StopIteration:
            break
    capacity = False
    if len(records) >= max_markets and max_markets:
        try:
            next(iterator)
        except StopIteration:
            pass
        else:
            capacity = True
    by_id: dict[str, tuple[CurrentMarket | None, dict[str, Any], str | None]] = {}
    for item in records:
        current, raw, malformed = item
        market_id = current.market_id if current is not None else _text(_nested(raw, "market_id", "id", "market"))
        if market_id and market_id not in by_id:
            by_id[market_id] = item
    matched: list[CurrentMarket] = []
    excluded: list[MarketScopeDisposition] = []
    deferred: list[MarketScopeDisposition] = []
    if mode == "EXACT_MARKETS":
        candidates: Sequence[str] = tuple(sorted(policy.market_ids))
        for market_id in candidates:
            item = by_id.get(market_id)
            if item is None:
                deferred.append(MarketScopeDisposition(market_id, "NOT_OBSERVED"))
                continue
            current, raw, malformed = item
            if malformed or current is None:
                deferred.append(MarketScopeDisposition(market_id, malformed or "MISSING_MARKET_IDENTITY"))
                continue
            action, eligibility_reason = _current_eligibility(raw, stamp)
            if action == "defer":
                deferred.append(MarketScopeDisposition(market_id, eligibility_reason))
            elif action == "exclude":
                excluded.append(MarketScopeDisposition(market_id, eligibility_reason))
            else:
                enriched = _with_official_category(raw, forward_filters)
                if not forward_market_matches(
                    enriched,
                    forward_filters,
                    now=stamp,
                    target_instrument=policy.instrument,
                ):
                    excluded.append(
                        MarketScopeDisposition(
                            market_id,
                            "RULE_MISMATCH",
                            detail=_rule_mismatch_reason(
                                raw,
                                forward_filters,
                                now=stamp,
                                target_instrument=policy.instrument,
                            ),
                        )
                    )
                elif len(matched) >= max_matches:
                    deferred.append(MarketScopeDisposition(market_id, "MATCH_CAPACITY"))
                else:
                    matched.append(current)
    else:

        for current, raw, malformed in records:
            market_id = current.market_id if current is not None else _text(_nested(raw, "market_id", "id", "market")) or "<unknown>"
            if market_id in historical_ids:
                excluded.append(MarketScopeDisposition(market_id, "HISTORICAL_CONSTITUENT"))
                continue
            if malformed or current is None:
                deferred.append(MarketScopeDisposition(market_id, malformed or "MISSING_MARKET_IDENTITY"))
                continue
            action, eligibility_reason = _current_eligibility(raw, stamp)
            if action == "defer":
                deferred.append(MarketScopeDisposition(market_id, eligibility_reason))
                continue
            if action == "exclude":
                excluded.append(MarketScopeDisposition(market_id, eligibility_reason))
                continue
            enriched = _with_official_category(raw, forward_filters)
            if forward_market_matches(enriched, forward_filters, now=stamp, target_instrument=policy.instrument):
                if len(matched) >= max_matches:
                    deferred.append(MarketScopeDisposition(market_id, "MATCH_CAPACITY"))
                else:
                    matched.append(current)
            else:
                mismatch_reason = _rule_mismatch_reason(
                    raw,
                    forward_filters,
                    now=stamp,
                    target_instrument=policy.instrument,
                )
                excluded.append(
                    MarketScopeDisposition(
                        market_id,
                        "RULE_MISMATCH",
                        detail=mismatch_reason,
                    )
                )
    if capacity:
        deferred.append(MarketScopeDisposition("<capacity>", "INVENTORY_CAPACITY"))
    if matched and (deferred or (mode == "EXACT_MARKETS" and excluded)):
        reason = "MATCHED_WITH_DEFERRED" if deferred else "MATCHED_WITH_EXCLUDED"
        status = PARTIAL
    elif matched:
        status, reason = MATCHED, MATCHED
    elif deferred:
        status, reason = DEFERRED, "DEFERRED_MARKETS"
    else:
        status, reason = ZERO_MATCHES, ZERO_MATCHES
    return MarketScopeResolution(identifier, scope_hash, scope_version, stamp, status, reason, policy_dict, tuple(matched), tuple(excluded), tuple(deferred), provenance)


# Alternate function spelling used in early integrations.
resolve_current_markets = resolve_market_scope


MarketResolution = MarketScopeResolution
MarketScopeResult = MarketScopeResolution
MarketScopeStatus = MarketScopeResolutionStatus
PARTIAL_MATCH = PARTIAL
PARTIAL_DEFERRED = PARTIAL
DEFERRED_MARKETS = DEFERRED
ResolutionStatus = MarketScopeResolutionStatus
MarketResolutionStatus = MarketScopeResolutionStatus

__all__ = [
    "MARKET_SCOPE_RESOLUTION_SCHEMA_VERSION",
    "MARKET_SCOPE_RESOLUTION_VERSION",
    "MarketScopeResolutionStatus",
    "ResolutionStatus",
    "MarketResolutionStatus",
    "MAX_SCOPE_MATCHES",
    "CurrentMarket",
    "ResolvedMarket",
    "MarketScopeDisposition",
    "ExcludedMarket",
    "DeferredMarket",
    "MarketScopeResolution",
    "CurrentMarketResolution",
    "MarketResolution",
    "MarketScopeResult",
    "MarketScopeStatus",
    "resolve_market_scope",
    "resolve_current_markets",
    "INVALID_POLICY",
    "RESEARCH_ONLY",
    "ZERO_MATCHES",
    "MATCHED",
    "PARTIAL",
    "PARTIAL_MATCH",
    "PARTIAL_DEFERRED",
    "DEFERRED",
    "DEFERRED_MARKETS",
]
