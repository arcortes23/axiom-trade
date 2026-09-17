"""Read-only, bounded Polymarket venue feasibility assessment.

The assessor deliberately stops at public market evidence.  It calls only
``market``, ``token_ids``, ``metadata`` and public order-book methods on the
adapter; it never constructs credentials, signs payloads, or submits orders.
All values returned by :meth:`VenueFeasibilityAssessment.to_dict` are JSON
friendly (decimal values are strings) so the result can be persisted without
introducing floating-point rounding into a cap decision.
"""
from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_DOWN
import math
from typing import Any, Callable
from .domain import parse_timestamp, utc_now
from .polymarket_rules import (
    POLYMARKET_RULES_VERSION,
    PolymarketRuleError,
    PolymarketRules,
    SIZE_PRECISION,
    parse_polymarket_rules,
)


UNKNOWN = "UNKNOWN"
FEASIBLE = "FEASIBLE"
INFEASIBLE = "INFEASIBLE"

# A feasibility request is intentionally bounded even when a caller passes an
# accidentally large depth.  The adapter itself also bounds its public calls.
MAX_BOOK_DEPTH = 100
DEFAULT_BOOK_DEPTH = 20
DEFAULT_CAP_USD = Decimal("1.00")
DEFAULT_MAX_AGE_SECONDS = Decimal("60")
_ZERO = Decimal("0")
_ONE = Decimal("1")
_MISSING = object()
_QUANTITY_STEP = Decimal(1).scaleb(-SIZE_PRECISION)


class VenueFeasibilityError(ValueError):
    """Invalid assessor configuration (not a venue response)."""




@dataclass(frozen=True, slots=True)
class VenueFeasibilityAssessment(Mapping[str, Any]):
    """Immutable, mapping-compatible result of a bounded assessment.

    Numeric fields are canonical decimal strings or :data:`UNKNOWN`.  Nested
    mappings are copied when serialized, making this object safe to hand to a
    persistence/reporting layer without exposing adapter objects.
    """

    verdict: str
    market_id: str | None = None
    condition_id: str | None = None
    market: Mapping[str, Any] = field(default_factory=dict)
    token_ids: Mapping[str, str] = field(default_factory=dict)
    outcomes: Mapping[str, str] = field(default_factory=lambda: {"yes": "YES", "no": "NO"})
    timestamps: Mapping[str, str] = field(default_factory=dict)
    min_order_quantity: str = UNKNOWN
    tick_size: str = UNKNOWN
    neg_risk: bool | str = UNKNOWN
    # Report-only compatibility aliases; never required by Polymarket.
    min_notional: str = UNKNOWN
    size_increment: str = UNKNOWN
    fee_reserve: str = "0"
    buy: Mapping[str, Any] = field(default_factory=dict)
    sell: Mapping[str, Any] = field(default_factory=dict)
    reason_codes: tuple[str, ...] = ()
    checked_at: str = UNKNOWN
    cap_usd: str = "1.00"
    depth: int = DEFAULT_BOOK_DEPTH
    provider: str = "polymarket"

    @property
    def status(self) -> str:
        """Alias used by report consumers that call verdicts statuses."""
        return self.verdict

    @property
    def reasons(self) -> tuple[str, ...]:
        return self.reason_codes

    def to_dict(self) -> dict[str, Any]:
        """Return a deterministic, secret-free persistence representation."""
        rules = {
            "min_order_quantity": self.min_order_quantity,
            "min_order_size": self.min_order_quantity,
            "tick_size": self.tick_size,
            "price_increment": self.tick_size,
            "neg_risk": self.neg_risk,
            # These aliases are retained for consumers that deserialize older
            # reports; UNKNOWN means they were not part of official rules.
            "min_notional": self.min_notional,
            "size_increment": self.size_increment,
        }
        return {
            "assessment_version": "polymarket-venue-feasibility-v2",
            "rules_version": POLYMARKET_RULES_VERSION,
            "provider": self.provider,
            "verdict": self.verdict,
            "status": self.verdict,
            "feasible": True if self.verdict == FEASIBLE else False if self.verdict == INFEASIBLE else UNKNOWN,
            "market_id": self.market_id,
            "market": _jsonable(self.market),
            "condition_id": self.condition_id,
            "token_ids": dict(self.token_ids),
            "tokens": dict(self.token_ids),
            "outcomes": dict(self.outcomes),
            "timestamps": dict(self.timestamps),
            "checked_at": self.checked_at,
            "rules": rules,
            "min_order_quantity": self.min_order_quantity,
            "min_order_size": self.min_order_quantity,
            "tick_size": self.tick_size,
            "price_increment": self.tick_size,
            "neg_risk": self.neg_risk,
            "min_notional": self.min_notional,
            "size_increment": self.size_increment,
            "fee_reserve": self.fee_reserve,
            "buy": _jsonable(self.buy),
            "sell": _jsonable(self.sell),
            # Top-level aliases keep reports simple while preserving the
            # outcome-labelled details under buy/sell.
            "buy_by_outcome": _jsonable(self.buy),
            "sell_by_outcome": _jsonable(self.sell),
            "cap_usd": self.cap_usd,
            "depth": self.depth,
            "reason_codes": list(self.reason_codes),
            "reasons": list(self.reason_codes),
        }

    def as_dict(self) -> dict[str, Any]:
        return self.to_dict()

    def __getitem__(self, key: str) -> Any:
        return self.to_dict()[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self.to_dict())

    def __len__(self) -> int:
        return len(self.to_dict())


# Common spelling used by callers that prefer Result over Assessment.
VenueFeasibilityResult = VenueFeasibilityAssessment


def assess_venue_feasibility(
    adapter: Any,
    market_id: str,
    *,
    target_quantity: Any | None = None,
    quantity: Any | None = None,
    cap_usd: Any = DEFAULT_CAP_USD,
    target_notional_usd: Any | None = None,
    fee_rate: Any = _ZERO,
    fee_bps: Any | None = None,
    fee_reserve: Any = _ZERO,
    slippage_bps: Any = _ZERO,
    slippage_rate: Any | None = None,
    depth: int = DEFAULT_BOOK_DEPTH,
    max_age_seconds: Any = DEFAULT_MAX_AGE_SECONDS,
    stale_after_seconds: Any | None = None,
    now: Any | None = None,
) -> VenueFeasibilityAssessment:
    """Assess public Polymarket execution evidence without side effects.

    ``target_quantity`` defaults to the official minimum order size. If neither
    target quantity nor an official minimum is available, the quantity-dependent
    verdict is ``UNKNOWN`` rather than assuming a one-share minimum. ``cap_usd``
    is an all-in BUY cap including adverse slippage, venue fee, and any explicit
    policy fee reserve. Quantities are exact Decimals and limited to two decimals.
    """
    identifier = _required_text(market_id, "market_id")
    bounded_depth = _bounded_depth(depth)
    cap = _positive_decimal(
        target_notional_usd if target_notional_usd is not None else cap_usd,
        "cap_usd",
    )
    if quantity is not None:
        if target_quantity is not None and _decimal(quantity, "quantity") != _decimal(target_quantity, "target_quantity"):
            raise VenueFeasibilityError("quantity and target_quantity disagree")
        requested_quantity = _positive_decimal(quantity, "quantity")
    elif target_quantity is not None:
        requested_quantity = _positive_decimal(target_quantity, "target_quantity")
    else:
        requested_quantity = None

    fee = _positive_or_zero_decimal(fee_rate if fee_bps is None else _decimal(fee_bps, "fee_bps") / Decimal("10000"), "fee_rate")
    if fee_bps is not None and _decimal(fee_bps, "fee_bps") < _ZERO:
        raise VenueFeasibilityError("fee_bps must be non-negative")
    if slippage_rate is not None:
        slip = _positive_or_zero_decimal(slippage_rate, "slippage_rate")
    else:
        raw_slippage_bps = _decimal(slippage_bps, "slippage_bps")
        if raw_slippage_bps < _ZERO:
            raise VenueFeasibilityError("slippage_bps must be non-negative")
        slip = raw_slippage_bps / Decimal("10000")
    if slip >= _ONE:
        raise VenueFeasibilityError("slippage must be less than 100 percent")
    policy_fee_reserve = _positive_or_zero_decimal(fee_reserve, "fee_reserve")

    instant = _timestamp(now) if now is not None else _timestamp(utc_now())
    max_age = _nonnegative_decimal(
        stale_after_seconds if stale_after_seconds is not None else max_age_seconds,
        "max_age_seconds",
    )
    checked_at = _iso(instant)
    reasons: list[str] = []
    timestamps: dict[str, str] = {"checked_at": checked_at}
    unknown_evidence = False
    known_infeasible = False

    # Every call below is public and read-only.  Errors become UNKNOWN evidence
    # rather than escaping into a caller's lifecycle/admission path.
    market = _safe_call(adapter, "market", identifier, reasons, "MARKET_READ_ERROR")
    if market is None:
        reasons.append("MARKET_UNAVAILABLE")
        return _assessment(
            verdict=UNKNOWN,
            market_id=identifier,
            timestamps=timestamps,
            reasons=reasons,
            checked_at=checked_at,
            cap=cap,
            depth=bounded_depth,
        )

    returned_market_id = _text(_value(market, "market_id", "id", "market"))
    if returned_market_id is None:
        reasons.append("MARKET_ID_MISSING")
    elif returned_market_id != identifier:
        reasons.append("MARKET_ID_MISMATCH")

    condition_id = _text(_value(market, "condition_id", "conditionId", "condition"))
    if condition_id is None:
        reasons.append("CONDITION_ID_MISSING")

    market_stamp = _evidence_timestamp(market, "provider_timestamp", "timestamp", "updated_at", "updatedAt")
    _record_timestamp(timestamps, "market", market_stamp)
    _check_fresh(market_stamp, instant, max_age, reasons, "MARKET")

    # Lifecycle is observed only.  No lifecycle row or flag is ever changed.
    closed = _flag_value(
        market,
        ("closed", "is_closed", "market_closed"),
        default=False,
    )
    if closed is None:
        reasons.append("MARKET_CLOSED_FLAG_INVALID")
        unknown_evidence = True
    elif closed:
        reasons.append("MARKET_CLOSED")
        known_infeasible = True

    active = _flag_value(
        market,
        ("active", "is_active", "market_active"),
        default=True,
    )
    if active is None:
        reasons.append("MARKET_ACTIVE_FLAG_INVALID")
        unknown_evidence = True
    elif not active:
        reasons.append("MARKET_INACTIVE")
        known_infeasible = True

    accepting_orders = _flag_value(
        market,
        ("accepting_orders", "acceptingOrders", "accepting", "orders_open"),
        default=True,
    )
    if accepting_orders is None:
        reasons.append("ORDERS_ACCEPTING_FLAG_INVALID")
        unknown_evidence = True
    elif not accepting_orders:
        reasons.append("ORDERS_NOT_ACCEPTED")
        known_infeasible = True

    enable_order_book = _flag_value(
        market,
        ("enable_order_book", "enableOrderBook", "order_book_available", "orderBookAvailable"),
        default=True,
    )
    if enable_order_book is None:
        reasons.append("ORDER_BOOK_AVAILABILITY_FLAG_INVALID")
        unknown_evidence = True
    elif not enable_order_book:
        reasons.append("ORDER_BOOK_DISABLED")
        known_infeasible = True

    mapped = _safe_call(adapter, "token_ids", identifier, reasons, "TOKEN_READ_ERROR")
    token_ids = _normalize_token_mapping(mapped)
    if set(token_ids) != {"yes", "no"}:
        reasons.append("TOKEN_MAPPING_INCOMPLETE")

    market_tokens = {
        "yes": _text(_value(market, "yes_token_id", "yesTokenId")),
        "no": _text(_value(market, "no_token_id", "noTokenId")),
    }
    for outcome in ("yes", "no"):
        if market_tokens[outcome] is not None and token_ids.get(outcome) is not None and market_tokens[outcome] != token_ids[outcome]:
            reasons.append(f"{outcome.upper()}_TOKEN_MISMATCH")
        if token_ids.get(outcome) is None and market_tokens[outcome] is not None:
            token_ids[outcome] = market_tokens[outcome]  # explicit Gamma snapshot value, not an inferred position
    if set(token_ids) != {"yes", "no"} or not all(token_ids.values()):
        reasons.append("TOKEN_IDS_UNAVAILABLE")

    metadata = _safe_call(adapter, "metadata", identifier, reasons, "METADATA_READ_ERROR")
    metadata_market_id = _text(_value(metadata, "market_id", "id", "market")) if metadata is not None else None
    metadata_condition_id = _text(_value(metadata, "condition_id", "conditionId", "condition")) if metadata is not None else None
    if metadata is None:
        reasons.append("METADATA_UNAVAILABLE")
    else:
        if metadata_market_id is None:
            reasons.append("METADATA_MARKET_ID_MISSING")
        elif metadata_market_id != identifier:
            reasons.append("METADATA_MARKET_ID_MISMATCH")
        if metadata_condition_id is None:
            reasons.append("METADATA_CONDITION_ID_MISSING")
        elif condition_id is not None and metadata_condition_id != condition_id:
            reasons.append("METADATA_CONDITION_ID_MISMATCH")
        metadata_stamp = _evidence_timestamp(metadata, "provider_timestamp", "timestamp", "updated_at", "updatedAt")
        _record_timestamp(timestamps, "metadata", metadata_stamp)
        _check_fresh(metadata_stamp, instant, max_age, reasons, "METADATA")

    # The official CLOB book contract has exactly three required rule values.
    # Do not infer venue rules from exchange-style ``min_notional`` or
    # ``size_increment`` aliases.
    official_rules: PolymarketRules | None = None
    metadata_rule_error: str | None = None
    if metadata is not None:
        try:
            official_rules = parse_polymarket_rules(metadata)
        except PolymarketRuleError as exc:
            metadata_rule_error = str(exc)
    min_order = official_rules.min_order_size if official_rules is not None else None
    tick = official_rules.tick_size if official_rules is not None else None
    min_notional = None
    size_increment = None

    books = _load_books(adapter, identifier, token_ids, bounded_depth, reasons)
    buy: dict[str, Any] = {}
    sell: dict[str, Any] = {}

    for outcome in ("yes", "no"):
        token = token_ids.get(outcome)
        book = books.get(outcome)
        if token is None or book is None:
            if book is None:
                reasons.append(f"{outcome.upper()}_BOOK_UNAVAILABLE")
            unknown_evidence = True
            buy[outcome] = _unknown_leg(outcome, token)
            sell[outcome] = _unknown_exit(
                outcome,
                token,
                position_quantity=requested_quantity if requested_quantity is not None else min_order,
            )
            continue

        book_ok, book_stamp, asks, bids = _validate_book(
            book, token, condition_id, instant, max_age, timestamps, outcome, reasons
        )
        if not book_ok:
            unknown_evidence = True
            buy[outcome] = _unknown_leg(
                outcome,
                token,
                timestamp=timestamps.get(f"{outcome}_book"),
            )
            sell[outcome] = _unknown_exit(
                outcome,
                token,
                timestamp=timestamps.get(f"{outcome}_book"),
                position_quantity=requested_quantity if requested_quantity is not None else min_order,
            )
            continue

        # Books are authoritative for the documented rule triplet when they
        # carry it.  A completely absent triplet can use metadata; a partial
        # triplet is an evidence failure rather than a guessed default.
        if _has_official_rule_fields(book):
            try:
                book_rules = parse_polymarket_rules(book)
            except PolymarketRuleError as exc:
                reasons.append(f"{outcome.upper()}_{exc}")
                unknown_evidence = True
                book_rules = None
            if book_rules is not None:
                if official_rules is None:
                    official_rules = book_rules
                    min_order = book_rules.min_order_size
                    tick = book_rules.tick_size
                elif (
                    official_rules.min_order_size != book_rules.min_order_size
                    or official_rules.tick_size != book_rules.tick_size
                    or official_rules.neg_risk != book_rules.neg_risk
                ):
                    reasons.append("OFFICIAL_RULES_CONFLICT")
                    unknown_evidence = True
        if official_rules is not None and not _tick_aligned(
            asks, bids, official_rules.tick_size
        ):
            reasons.append(f"{outcome.upper()}_BOOK_PRICE_TICK_MISMATCH")
            unknown_evidence = True

        effective_quantity = requested_quantity if requested_quantity is not None else min_order
        if effective_quantity is not None and min_order is not None and effective_quantity < min_order:
            reasons.append(f"{outcome.upper()}_QUANTITY_BELOW_MIN_ORDER")
            known_infeasible = True
        if effective_quantity is None:
            unknown_evidence = True
            buy[outcome] = _unknown_leg(
                outcome,
                token,
                timestamp=book_stamp,
                asks=asks,
                bids=bids,
                cap=cap,
            )
        else:
            if _decimal_places(effective_quantity) > 2:
                reasons.append("QUANTITY_PRECISION_INVALID")
                unknown_evidence = True
            buy_leg, buy_bad, buy_unknown = _buy_leg(
                outcome,
                token,
                asks,
                effective_quantity,
                cap,
                fee,
                slip,
                book_stamp,
                fee_reserve=policy_fee_reserve,
            )
            buy[outcome] = buy_leg
            known_infeasible = known_infeasible or buy_bad
            unknown_evidence = unknown_evidence or buy_unknown
            if buy_leg.get("depth_sufficient") is False:
                reasons.append(f"{outcome.upper()}_BUY_DEPTH_INSUFFICIENT")
            elif buy_leg.get("depth_sufficient") == UNKNOWN:
                unknown_evidence = True
        sell_leg = _sell_leg(
            outcome,
            token,
            bids,
            fee,
            slip,
            book_stamp,
            effective_quantity,
            fee_reserve=policy_fee_reserve,
        )
        sell[outcome] = sell_leg
        sell_sufficient = sell_leg.get("depth_sufficient")
        if sell_sufficient is False:
            reasons.append(f"{outcome.upper()}_SELL_DEPTH_INSUFFICIENT")
            known_infeasible = True
        elif sell_sufficient == UNKNOWN:
            reasons.append(f"{outcome.upper()}_SELL_DEPTH_UNCOMPARABLE")
            unknown_evidence = True

    effective_quantity = requested_quantity if requested_quantity is not None else min_order
    if official_rules is None:
        if metadata_rule_error:
            reasons.append(metadata_rule_error)
        else:
            reasons.append("OFFICIAL_RULES_UNAVAILABLE")
        unknown_evidence = True

    min_order_text = _decimal_text(min_order)
    tick_text = _decimal_text(tick)
    min_notional_text = UNKNOWN
    size_increment_text = UNKNOWN


    if any(
        code.endswith(
            (
                "_MISMATCH",
                "_MISSING",
                "_ERROR",
                "_INVALID",
                "_CONFLICT",
                "_UNAVAILABLE",
                "_INCOMPLETE",
                "_STALE",
                "_FUTURE",
            )
        )
        for code in reasons
    ):
        unknown_evidence = True
        verdict = UNKNOWN
    elif known_infeasible:
        verdict = INFEASIBLE
    else:
        verdict = FEASIBLE

    return _assessment(
        verdict=verdict,
        market_id=identifier,
        condition_id=condition_id,
        market=_market_projection(market, identifier, condition_id),
        token_ids=token_ids,
        timestamps=timestamps,
        min_order_quantity=min_order_text,
        tick_size=tick_text,
        min_notional=min_notional_text,
        size_increment=size_increment_text,
        neg_risk=official_rules.neg_risk if official_rules is not None else UNKNOWN,
        fee_reserve=_decimal_text(policy_fee_reserve),
        buy=buy,
        sell=sell,
        reasons=reasons,
        checked_at=checked_at,
        cap=cap,
        depth=bounded_depth,
    )


# Explicit aliases make the utility discoverable without requiring package
# exports (the package root intentionally remains unchanged).
assess_polymarket_venue_feasibility = assess_venue_feasibility
evaluate_venue_feasibility = assess_venue_feasibility
assess_polymarket_feasibility = assess_venue_feasibility


class VenueFeasibilityAssessor:
    """Configured assessor facade for repeated bounded market checks."""

    def __init__(self, adapter: Any, **defaults: Any) -> None:
        self.adapter = adapter
        self.defaults = dict(defaults)

    def assess(self, market_id: str, **overrides: Any) -> VenueFeasibilityAssessment:
        options = dict(self.defaults)
        options.update(overrides)
        return assess_venue_feasibility(self.adapter, market_id, **options)

    assess_market = assess


# Backward-friendly descriptive alias.
PolymarketVenueFeasibilityAssessor = VenueFeasibilityAssessor


def _assessment(**kwargs: Any) -> VenueFeasibilityAssessment:
    return VenueFeasibilityAssessment(
        verdict=str(kwargs.get("verdict", UNKNOWN)),
        market_id=kwargs.get("market_id"),
        condition_id=kwargs.get("condition_id"),
        market=dict(kwargs.get("market") or {}),
        token_ids=dict(kwargs.get("token_ids") or {}),
        timestamps=dict(kwargs.get("timestamps") or {}),
        min_order_quantity=str(kwargs.get("min_order_quantity", UNKNOWN)),
        tick_size=str(kwargs.get("tick_size", UNKNOWN)),
        neg_risk=kwargs.get("neg_risk", UNKNOWN),
        min_notional=str(kwargs.get("min_notional", UNKNOWN)),
        size_increment=str(kwargs.get("size_increment", UNKNOWN)),
        fee_reserve=str(kwargs.get("fee_reserve", "0")),
        buy=dict(kwargs.get("buy") or {}),
        sell=dict(kwargs.get("sell") or {}),
        reason_codes=tuple(dict.fromkeys(str(reason) for reason in kwargs.get("reasons", ()) if str(reason))),
        checked_at=str(kwargs.get("checked_at", UNKNOWN)),
        cap_usd=_decimal_text(kwargs.get("cap", DEFAULT_CAP_USD)) or "1.00",
        depth=int(kwargs.get("depth", DEFAULT_BOOK_DEPTH)),
    )


def _market_projection(value: Any, requested_id: str, condition_id: str | None) -> dict[str, Any]:
    """Keep exact identity and descriptive market fields, not adapter objects."""
    projection: dict[str, Any] = {
        "market_id": requested_id,
        "condition_id": condition_id if condition_id is not None else UNKNOWN,
    }
    for output, names in (
        ("question", ("question",)),
        ("slug", ("slug",)),
        ("timestamp", ("provider_timestamp", "timestamp", "updated_at", "updatedAt")),
        ("active", ("active", "is_active", "market_active")),
        ("closed", ("closed", "is_closed", "market_closed")),
        ("accepting_orders", ("accepting_orders", "acceptingOrders", "accepting", "orders_open")),
        ("enable_order_book", ("enable_order_book", "enableOrderBook", "order_book_available", "orderBookAvailable")),
    ):
        raw = _value(value, *names, default=None)
        if raw is not None:
            if output == "timestamp":
                try:
                    projection[output] = _iso(_timestamp(raw))
                except (TypeError, ValueError, OverflowError, OSError):
                    projection[output] = UNKNOWN
            elif output in {"active", "closed", "accepting_orders", "enable_order_book"}:
                default = output == "closed"
                parsed_flag = _flag_value(value, names, default=default)
                projection[output] = _jsonable(raw) if parsed_flag is not None else UNKNOWN
            else:
                projection[output] = _jsonable(raw)
        else:
            projection[output] = UNKNOWN
    return projection

def _has_official_rule_fields(source: Any) -> bool:
    """Return whether a source carries any official CLOB rule field."""
    names = (
        "min_order_size",
        "orderMinSize",
        "minOrderSize",
        "order_min_size",
        "tick_size",
        "tickSize",
        "orderPriceMinTickSize",
        "order_price_min_tick_size",
        "neg_risk",
        "negRisk",
    )
    if isinstance(source, Mapping):
        return any(name in source for name in names)
    return any(_value(source, name, default=None) is not None for name in names)


def _decimal_places(value: Decimal) -> int:
    return max(0, -value.as_tuple().exponent)


def _tick_aligned(
    asks: list[tuple[Decimal, Decimal]],
    bids: list[tuple[Decimal, Decimal]],
    tick: Decimal,
) -> bool:
    precision = _decimal_places(tick)
    return all(
        _decimal_places(price) <= precision and price % tick == _ZERO
        for price, _ in (*asks, *bids)
    )

def _load_books(adapter: Any, market_id: str, token_ids: Mapping[str, str], depth: int, reasons: list[str]) -> dict[str, Any]:
    """Load exactly the two token-labelled public books when possible."""

    result: dict[str, Any] = {}
    direct = getattr(adapter, "order_book_for_token", None)
    if callable(direct):
        for outcome in ("yes", "no"):
            token = token_ids.get(outcome)
            if not token:
                continue
            try:
                result[outcome] = direct(token, depth=depth)
            except Exception:
                reasons.append(f"{outcome.upper()}_BOOK_READ_ERROR")
        return result

    books_method = getattr(adapter, "order_books", None)
    if callable(books_method):
        try:
            raw = books_method(market_id, depth=depth)
        except Exception:
            reasons.append("ORDER_BOOKS_READ_ERROR")
            return result
        if isinstance(raw, Mapping):
            for key, value in raw.items():
                normalized = _outcome_key(key, token_ids)
                if normalized in {"yes", "no"}:
                    result[normalized] = value
        # A non-token-labelled implementation may expose one book via
        # order_book; never pretend it is the NO book.
        return result
    reasons.append("ORDER_BOOK_METHOD_UNAVAILABLE")
    return result


def _validate_book(
    book: Any,
    token: str,
    condition_id: str | None,
    now: datetime,
    max_age: Decimal,
    timestamps: dict[str, str],
    outcome: str,
    reasons: list[str],
) -> tuple[bool, str | None, list[tuple[Decimal, Decimal]], list[tuple[Decimal, Decimal]]]:
    returned_token = _text(_value(book, "token_id", "tokenId", "asset_id", "assetId"))
    if returned_token is None:
        reasons.append(f"{outcome.upper()}_BOOK_TOKEN_ID_MISSING")
        return False, None, [], []
    if returned_token != token:
        reasons.append(f"{outcome.upper()}_BOOK_TOKEN_ID_MISMATCH")
        return False, None, [], []
    returned_condition = _text(_value(book, "condition_id", "conditionId", "market"))
    if returned_condition is None:
        reasons.append(f"{outcome.upper()}_BOOK_CONDITION_ID_MISSING")
        return False, None, [], []
    if condition_id is None or returned_condition != condition_id:
        reasons.append(f"{outcome.upper()}_BOOK_CONDITION_ID_MISMATCH")
        return False, None, [], []
    available = _flag_value(
        book,
        ("available", "order_book_available", "orderBookAvailable"),
        default=True,
    )
    if available is None:
        reasons.append(f"{outcome.upper()}_BOOK_AVAILABILITY_FLAG_INVALID")
        return False, None, [], []
    if not available:
        reasons.append(f"{outcome.upper()}_BOOK_UNAVAILABLE")
        return False, None, [], []
    stamp = _evidence_timestamp(book, "provider_timestamp", "timestamp", "updated_at", "updatedAt")
    _record_timestamp(timestamps, f"{outcome}_book", stamp)
    if not _check_fresh(stamp, now, max_age, reasons, f"{outcome.upper()}_BOOK"):
        return False, stamp, [], []
    try:
        asks = _levels(_value(book, "asks", "sell", "offer"), reverse=False)
        bids = _levels(_value(book, "bids", "buy"), reverse=True)
    except (TypeError, ValueError):
        reasons.append(f"{outcome.upper()}_BOOK_LEVELS_INVALID")
        return False, stamp, [], []
    return True, stamp, asks, bids


def _buy_leg(
    outcome: str,
    token: str,
    asks: list[tuple[Decimal, Decimal]],
    quantity: Decimal,
    cap: Decimal,
    fee: Decimal,
    slip: Decimal,
    timestamp: str | None,
    *,
    fee_reserve: Decimal = _ZERO,
) -> tuple[dict[str, Any], bool, bool]:
    top = _walk(asks[:1], quantity, fee=fee, slip=slip, side="BUY")
    multi = _walk(asks, quantity, fee=fee, slip=slip, side="BUY")
    available_cap = max(_ZERO, cap - fee_reserve)
    cap_quantity = _cap_quantity(asks, available_cap, fee=fee, slip=slip)
    top_cost = _decimal_text(top["all_in"])
    multi_cost = _decimal_text(multi["all_in"])
    target_text = _decimal_text(quantity)
    filled_text = _decimal_text(multi["filled"])
    target_filled = multi["filled"] >= quantity
    required_cost = multi["all_in"] + fee_reserve if target_filled else None
    cap_ok = target_filled and required_cost is not None and required_cost <= cap
    leg = {
        "action": "SUITABLE" if cap_ok else "UNSUITABLE",
        "reason": (
            "OK"
            if cap_ok
            else "NO_DEPTH"
            if not asks
            else "INSUFFICIENT_DEPTH"
            if not target_filled
            else "CAP_EXCEEDED"
        ),
        "outcome": outcome.upper(),
        "token_id": token,
        "timestamp": timestamp or UNKNOWN,
        "requested_quantity": target_text,
        "buy_quantity": target_text,
        "filled_quantity": filled_text,
        "depth_quantity": _decimal_text(sum((size for _, size in asks), _ZERO)),
        "top": _cost_dict(top, target_text),
        "multilevel": _cost_dict(multi, target_text),
        "top_buy_cost": top_cost if top["filled"] >= quantity else UNKNOWN,
        "multilevel_buy_cost": multi_cost if target_filled else UNKNOWN,
        "top_cost": top_cost if top["filled"] >= quantity else UNKNOWN,
        "multilevel_cost": multi_cost if target_filled else UNKNOWN,
        "top_buy_raw_notional": _decimal_text(top["raw"]) if top["filled"] >= quantity else UNKNOWN,
        "required_cost": _decimal_text(required_cost),
        "fee_reserve": _decimal_text(fee_reserve),
        "multilevel_raw_notional": _decimal_text(multi["raw"]) if target_filled else UNKNOWN,
        "top_buy_slippage_cost": _decimal_text(top["adjusted"] - top["raw"]) if top["filled"] >= quantity else UNKNOWN,
        "multilevel_slippage_cost": _decimal_text(multi["adjusted"] - multi["raw"]) if target_filled else UNKNOWN,
        "top_buy_fee": _decimal_text(top["fee"]) if top["filled"] >= quantity else UNKNOWN,
        "multilevel_fee": _decimal_text(multi["fee"]) if target_filled else UNKNOWN,
        "top_depth_sufficient": top["filled"] >= quantity,
        "depth_sufficient": target_filled,
        "cap_usd": _decimal_text(cap),
        "cap_satisfied": True if cap_ok else False if target_filled else UNKNOWN,
        "max_quantity_under_cap": _decimal_text(cap_quantity),
        "size_increment": UNKNOWN,
    }
    bad = bool(target_filled and required_cost is not None and required_cost > cap) or not target_filled
    unknown = False
    return leg, bad, unknown

def _sell_leg(
    outcome: str,
    token: str,
    bids: list[tuple[Decimal, Decimal]],
    fee: Decimal,
    slip: Decimal,
    timestamp: str | None,
    position_quantity: Decimal | None,
    *,
    fee_reserve: Decimal = _ZERO,
) -> dict[str, Any]:
    depth_quantity = sum((size for _, size in bids), _ZERO)
    walk = (
        _walk(bids, depth_quantity, fee=fee, slip=slip, side="SELL")
        if depth_quantity > _ZERO
        else _empty_walk()
    )
    depth_sufficient: bool | str = (
        UNKNOWN if position_quantity is None else depth_quantity >= position_quantity
    )
    sell_action = (
        "SUITABLE"
        if depth_sufficient is True
        else "UNSUITABLE"
        if depth_sufficient is False
        else UNKNOWN
    )
    sell_reason = (
        "OK"
        if depth_sufficient is True
        else "NO_DEPTH"
        if not bids
        else "INSUFFICIENT_DEPTH"
        if depth_sufficient is False
        else "UNKNOWN"
    )
    quantity_text = _decimal_text(position_quantity)
    return {
        "action": sell_action,
        "reason": sell_reason,
        "outcome": outcome.upper(),
        "token_id": token,
        "timestamp": timestamp or UNKNOWN,
        "requested_quantity": quantity_text,
        "position_quantity": quantity_text,
        "depth_quantity": _decimal_text(depth_quantity),
        "sell_depth_limit": _decimal_text(depth_quantity),
        "max_exit_quantity": _decimal_text(depth_quantity),
        "raw_notional_at_depth": _decimal_text(walk["raw"]),
        "gross_proceeds_at_depth": _decimal_text(walk["adjusted"]),
        "fee_at_depth": _decimal_text(walk["fee"]),
        "net_proceeds_at_depth": _decimal_text(walk["net"]),
        "fee_reserve": _decimal_text(fee_reserve),
        "depth_sufficient": depth_sufficient,
    }


def _unknown_leg(
    outcome: str,
    token: str | None,
    *,
    timestamp: str | None = None,
    asks: list[tuple[Decimal, Decimal]] | None = None,
    bids: list[tuple[Decimal, Decimal]] | None = None,
    cap: Decimal | None = None,
) -> dict[str, Any]:
    return {
        "outcome": outcome.upper(),
        "token_id": token,
        "timestamp": timestamp or UNKNOWN,
        "requested_quantity": UNKNOWN,
        "buy_quantity": UNKNOWN,
        "filled_quantity": UNKNOWN,
        "depth_quantity": _decimal_text(sum((size for _, size in (asks or [])), _ZERO)),
        "top": UNKNOWN,
        "multilevel": UNKNOWN,
        "top_buy_cost": UNKNOWN,
        "multilevel_buy_cost": UNKNOWN,
        "top_cost": UNKNOWN,
        "multilevel_cost": UNKNOWN,
        "top_depth_sufficient": UNKNOWN,
        "depth_sufficient": UNKNOWN,
        "cap_usd": _decimal_text(cap) if cap is not None else UNKNOWN,
        "cap_satisfied": UNKNOWN,
        "max_quantity_under_cap": UNKNOWN,
        "size_increment": UNKNOWN,
    }
def _unknown_exit(
    outcome: str,
    token: str | None,
    *,
    timestamp: str | None = None,
    position_quantity: Decimal | None = None,
) -> dict[str, Any]:
    quantity_text = _decimal_text(position_quantity)
    return {
        "outcome": outcome.upper(),
        "token_id": token,
        "timestamp": timestamp or UNKNOWN,
        "requested_quantity": quantity_text,
        "position_quantity": quantity_text,
        "depth_quantity": UNKNOWN,
        "sell_depth_limit": UNKNOWN,
        "max_exit_quantity": UNKNOWN,
        "raw_notional_at_depth": UNKNOWN,
        "gross_proceeds_at_depth": UNKNOWN,
        "fee_at_depth": UNKNOWN,
        "net_proceeds_at_depth": UNKNOWN,
        "depth_sufficient": UNKNOWN,
    }



def _cost_dict(walk: Mapping[str, Decimal], quantity: str) -> dict[str, Any]:
    return {
        "requested_quantity": quantity,
        "filled_quantity": _decimal_text(walk["filled"]),
        "raw_notional": _decimal_text(walk["raw"]),
        "slippage_cost": _decimal_text(walk["adjusted"] - walk["raw"]),
        "fee": _decimal_text(walk["fee"]),
        "all_in_cost": _decimal_text(walk["all_in"]),
        "depth_sufficient": walk["filled"] >= Decimal(quantity),
    }


def _walk(levels: list[tuple[Decimal, Decimal]], quantity: Decimal, *, fee: Decimal, slip: Decimal, side: str) -> dict[str, Decimal]:
    remaining = quantity
    raw = _ZERO
    adjusted = _ZERO
    filled = _ZERO
    multiplier = _ONE + slip if side == "BUY" else _ONE - slip
    for price, size in levels:
        take = min(remaining, size)
        if take <= _ZERO:
            continue
        raw += take * price
        adjusted += take * price * multiplier
        filled += take
        remaining -= take
        if remaining <= _ZERO:
            break
    charge = adjusted * fee
    return {
        "filled": filled,
        "raw": raw,
        "adjusted": adjusted,
        "fee": charge,
        "all_in": adjusted + charge,
        "net": adjusted - charge,
    }


def _empty_walk() -> dict[str, Decimal]:
    return {"filled": _ZERO, "raw": _ZERO, "adjusted": _ZERO, "fee": _ZERO, "all_in": _ZERO, "net": _ZERO}


def _cap_quantity(
    levels: list[tuple[Decimal, Decimal]],
    cap: Decimal,
    *,
    fee: Decimal,
    slip: Decimal,
) -> Decimal:
    multiplier = (_ONE + slip) * (_ONE + fee)
    remaining_cap = cap
    quantity = _ZERO
    for price, size in levels:
        unit = price * multiplier
        if unit <= _ZERO:
            continue
        take = min(size, remaining_cap / unit)
        if take <= _ZERO:
            break
        quantity += take
        remaining_cap -= take * unit
        if remaining_cap <= _ZERO:
            break

    # Polymarket shares are executable only at the canonical two-decimal
    # precision.  Floor before the exact walk so a cap-derived quantity never
    # exposes a long Decimal expansion or rounds up over the cap.
    quantity = (quantity / _QUANTITY_STEP).to_integral_value(rounding=ROUND_DOWN) * _QUANTITY_STEP

    # Recompute the exact Decimal walk after flooring.  A high-precision
    # division above can still leave a rounded quantity a hair over the cap.
    # Scale once, then floor again and defensively step down until the
    # persisted quantity is demonstrably within the all-in cap.
    for _ in range(4):
        if quantity <= _ZERO:
            break
        cost = _walk(levels, quantity, fee=fee, slip=slip, side="BUY")["all_in"]
        if cost <= cap:
            break
        quantity = quantity * cap / cost
        quantity = (quantity / _QUANTITY_STEP).to_integral_value(rounding=ROUND_DOWN) * _QUANTITY_STEP
    if quantity > _ZERO and _walk(levels, quantity, fee=fee, slip=slip, side="BUY")["all_in"] > cap:
        return _ZERO
    return max(_ZERO, quantity)

def _levels(value: Any, *, reverse: bool) -> list[tuple[Decimal, Decimal]]:
    if value is None:
        return []
    if not isinstance(value, (list, tuple)):
        raise ValueError("book side is not a sequence")
    result: list[tuple[Decimal, Decimal]] = []
    for row in value:
        if isinstance(row, Mapping):
            price_raw = _value(row, "price", "p")
            size_raw = _value(row, "size", "quantity", "q")
        elif isinstance(row, (list, tuple)) and len(row) >= 2:
            price_raw, size_raw = row[0], row[1]
        else:
            price_raw = getattr(row, "price", None)
            size_raw = getattr(row, "size", getattr(row, "quantity", None))
            if price_raw is None or size_raw is None:
                raise ValueError("book level is malformed")
        price = _positive_decimal(price_raw, "book price")
        size = _positive_decimal(size_raw, "book size")
        if price > _ONE:
            raise ValueError("prediction price must be at most one")
        if _decimal_places(size) > 2:
            raise ValueError("book size precision exceeds documented CLOB precision")
        result.append((price, size))
    result.sort(key=lambda item: item[0], reverse=reverse)
    return result



def _record_timestamp(target: dict[str, str], name: str, value: str | None) -> None:
    target[name] = value if value is not None else UNKNOWN


def _check_fresh(value: str | None, now: datetime, max_age: Decimal, reasons: list[str], label: str) -> bool:
    if value is None:
        reasons.append(f"{label}_TIMESTAMP_MISSING")
        return False
    parsed = _timestamp(value)
    age = Decimal(str((now - parsed).total_seconds()))
    if age < _ZERO:
        reasons.append(f"{label}_TIMESTAMP_IN_FUTURE")
        return False
    if age > max_age:
        reasons.append(f"{label}_STALE")
        return False
    return True


def _evidence_timestamp(value: Any, *names: str) -> str | None:
    raw = _value(value, *names, default=None)
    if raw is None or raw == "":
        return None
    try:
        return _iso(_timestamp(raw))
    except (TypeError, ValueError, OverflowError, OSError):
        return None


def _timestamp(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        parsed = parse_timestamp(value)
        if parsed is None:
            raise ValueError("timestamp is malformed")
        return parsed.astimezone(timezone.utc)
    else:
        parsed = parse_timestamp(value)
        if parsed is None:
            raise ValueError("timestamp is malformed")
    return (parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)).astimezone(timezone.utc)


def _iso(value: datetime) -> str:
    return _timestamp(value).isoformat().replace("+00:00", "Z")


def _bounded_depth(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise VenueFeasibilityError("depth must be a positive integer")
    return min(MAX_BOOK_DEPTH, value)


def _decimal(value: Any, label: str) -> Decimal:
    if isinstance(value, bool) or value is None or value == "":
        raise VenueFeasibilityError(f"{label} must be numeric")
    try:
        parsed = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise VenueFeasibilityError(f"{label} must be numeric") from exc
    if not parsed.is_finite():
        raise VenueFeasibilityError(f"{label} must be finite")
    return parsed


def _positive_decimal(value: Any, label: str) -> Decimal:
    parsed = _decimal(value, label)
    if parsed <= _ZERO:
        raise VenueFeasibilityError(f"{label} must be positive")
    return parsed


def _positive_or_zero_decimal(value: Any, label: str) -> Decimal:
    parsed = _decimal(value, label)
    if parsed < _ZERO:
        raise VenueFeasibilityError(f"{label} must be non-negative")
    return parsed


def _nonnegative_decimal(value: Any, label: str) -> Decimal:
    return _positive_or_zero_decimal(value, label)


def _decimal_text(value: Decimal | Any | None) -> str:
    if value is None:
        return UNKNOWN
    if isinstance(value, str) and value == UNKNOWN:
        return UNKNOWN
    try:
        parsed = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return UNKNOWN
    if not parsed.is_finite():
        return UNKNOWN
    return format(parsed, "f")


def _required_text(value: Any, label: str) -> str:
    text = _text(value)
    if text is None:
        raise VenueFeasibilityError(f"{label} is required")
    return text


def _text(value: Any) -> str | None:
    if value is None or isinstance(value, (bool, Mapping, list, tuple, set)):
        return None
    text = str(value).strip()
    return text or None


def _value(source: Any, *names: str, default: Any = None) -> Any:
    if source is None:
        return default
    if isinstance(source, Mapping):
        for name in names:
            if name in source:
                return source[name]
        return default
    for name in names:
        try:
            value = getattr(source, name)
        except Exception:
            value = None
        if value is not None:
            return value
    return default


def _normalize_token_mapping(value: Any) -> dict[str, str]:
    if not isinstance(value, Mapping):
        return {}
    result: dict[str, str] = {}
    for key, token in value.items():
        outcome = _outcome_key(key, {})
        token_text = _text(token)
        if outcome in {"yes", "no"} and token_text is not None:
            result[outcome] = token_text
    return result


def _outcome_key(value: Any, token_ids: Mapping[str, str]) -> str | None:
    text = _text(value)
    if text is None:
        return None
    lowered = text.lower()
    if lowered in {"yes", "y", "true", "1"}:
        return "yes"
    if lowered in {"no", "n", "false", "0"}:
        return "no"
    for outcome, token in token_ids.items():
        if text == token:
            return outcome
    return None


def _bool_value(value: Any, *, default: bool) -> bool | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float, Decimal)) and not isinstance(value, bool):
        if value == 1:
            return True
        if value == 0:
            return False
        return None
    if not isinstance(value, str):
        return None
    lowered = value.strip().lower()
    if lowered in {"1", "true", "yes", "y", "on", "active", "open"}:
        return True
    if lowered in {"0", "false", "no", "n", "off", "closed", "inactive"}:
        return False
    return None


def _flag_value(source: Any, names: tuple[str, ...], *, default: bool) -> bool | None:
    candidates: list[Any] = []
    if isinstance(source, Mapping):
        for name in names:
            if name in source:
                candidates.append(source[name])
        extra = source.get("extra")
    else:
        for name in names:
            try:
                value = getattr(source, name)
            except Exception:
                continue
            if value is not None:
                candidates.append(value)
        extra = _value(source, "extra", default=None)
    if isinstance(extra, Mapping):
        for name in names:
            if name in extra:
                candidates.append(extra[name])
    if not candidates:
        return default
    parsed = [_bool_value(value, default=default) for value in candidates]
    if any(value is None for value in parsed):
        return None
    first = parsed[0]
    return first if all(value == first for value in parsed[1:]) else None

def _safe_call(adapter: Any, name: str, *args: Any) -> Any:
    # The final two positional arguments are the mutable reason sink and code.
    reasons = args[-2]
    error_code = args[-1]
    call_args = args[:-2]
    method = getattr(adapter, name, None)
    if not callable(method):
        reasons.append(error_code)
        return None
    try:
        return method(*call_args)
    except Exception:
        reasons.append(error_code)
        return None


def _jsonable(value: Any) -> Any:
    if isinstance(value, Decimal):
        return _decimal_text(value)
    if isinstance(value, datetime):
        return _iso(value)
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


__all__ = [
    "DEFAULT_BOOK_DEPTH",
    "DEFAULT_CAP_USD",
    "DEFAULT_MAX_AGE_SECONDS",
    "FEASIBLE",
    "INFEASIBLE",
    "MAX_BOOK_DEPTH",
    "UNKNOWN",
    "PolymarketVenueFeasibilityAssessor",
    "VenueFeasibilityAssessment",
    "VenueFeasibilityAssessor",
    "VenueFeasibilityError",
    "VenueFeasibilityResult",
    "assess_polymarket_feasibility",
    "assess_polymarket_venue_feasibility",
    "assess_venue_feasibility",
    "evaluate_venue_feasibility",
]
