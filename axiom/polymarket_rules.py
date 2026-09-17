"""Canonical, read-only Polymarket CLOB rules and depth calculations.

The public CLOB contract used here is the Polymarket SDK 0.9/API-v2 shape:
``min_order_size``, ``tick_size`` and ``neg_risk`` are the only venue rules
required from a book.  This module intentionally does not model exchange-style
``min_notional`` or ``size_increment`` filters.  Rule parsing is strict and the
selected-token depth assessment is pure; neither operation can sign or submit
an order.
"""
from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from typing import Any

SUPPORTED_POLYMARKET_SDK = "0.9.0"
# Pinned contract identifier rather than a moving documentation URL.  The
# version is persisted with assessments so an operator can distinguish rule
# evidence gathered under a later API contract.
POLYMARKET_RULES_DOCS_VERSION = "clob-api-v2"
POLYMARKET_RULES_VERSION = f"polymarket-sdk-{SUPPORTED_POLYMARKET_SDK}/{POLYMARKET_RULES_DOCS_VERSION}"
POLYMARKET_SDK_VERSION = SUPPORTED_POLYMARKET_SDK
POLYMARKET_DOCS_VERSION = POLYMARKET_RULES_DOCS_VERSION

ZERO = Decimal("0")
ONE = Decimal("1")
SIZE_PRECISION = 2
# The documented CLOB tick table.  Amount precision is the precision used by
# the SDK when converting price/size to integer maker/taker amounts.  Keep this
# table aligned with polymarket-client 0.9.0's rounding configuration.
_TICK_PRECISION: dict[Decimal, tuple[int, int]] = {
    Decimal("0.1"): (1, 3),
    Decimal("0.01"): (2, 4),
    Decimal("0.005"): (3, 5),
    Decimal("0.0025"): (4, 6),
    Decimal("0.001"): (3, 5),
    Decimal("0.0001"): (4, 6),
}
UNKNOWN = "UNKNOWN"
SUITABLE = "SUITABLE"
UNSUITABLE = "UNSUITABLE"


class PolymarketRuleError(ValueError):
    """A required official rule is missing, invalid, or unsupported."""


@dataclass(frozen=True, slots=True)
class PolymarketRules(Mapping[str, Any]):
    """Immutable parsed official book rules.

    ``min_order_size`` is in shares.  ``tick_size`` is the market's price tick.
    Quantity precision is fixed at two decimals; price and SDK amount precision
    are selected from the documented tick table.  Exchange metadata is identity
    evidence only and never substitutes for ``neg_risk``.
    """

    min_order_size: Decimal
    tick_size: Decimal
    neg_risk: bool
    sdk_version: str = SUPPORTED_POLYMARKET_SDK
    docs_version: str = POLYMARKET_RULES_DOCS_VERSION
    price_precision: int = 2
    size_precision: int = SIZE_PRECISION
    amount_precision: int = 4
    exchange: str | None = None
    neg_risk_exchange: str | None = None
    neg_risk_market_id: str | None = None
    def __post_init__(self) -> None:
        try:
            min_order = self.min_order_size if isinstance(self.min_order_size, Decimal) else Decimal(str(self.min_order_size))
            tick = self.tick_size if isinstance(self.tick_size, Decimal) else Decimal(str(self.tick_size))
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise PolymarketRuleError("RULE_DECIMAL_INVALID") from exc
        object.__setattr__(self, "min_order_size", min_order)
        object.__setattr__(self, "tick_size", tick)
        if min_order <= ZERO or not min_order.is_finite():
            raise PolymarketRuleError("MIN_ORDER_SIZE_INVALID")
        if tick not in _TICK_PRECISION:
            raise PolymarketRuleError("TICK_SIZE_UNSUPPORTED")
        if not isinstance(self.neg_risk, bool):
            raise PolymarketRuleError("NEG_RISK_INVALID")
        expected_price, expected_amount = _TICK_PRECISION[tick]
        if self.price_precision != expected_price or self.amount_precision != expected_amount:
            raise PolymarketRuleError("PRECISION_TABLE_MISMATCH")
        if self.size_precision != SIZE_PRECISION:
            raise PolymarketRuleError("SIZE_PRECISION_UNSUPPORTED")
        if self.sdk_version != SUPPORTED_POLYMARKET_SDK:
            raise PolymarketRuleError("UNSUPPORTED_POLYMARKET_SDK")
        if self.docs_version != POLYMARKET_RULES_DOCS_VERSION:
            raise PolymarketRuleError("UNSUPPORTED_POLYMARKET_RULES_DOCS")

    @property
    def min_order_quantity(self) -> Decimal:
        """Descriptive alias used by feasibility consumers."""
        return self.min_order_size
    @property
    def minimum_shares(self) -> Decimal:
        return self.min_order_size


    @property
    def quantity_precision(self) -> int:
        return self.size_precision

    @property
    def exchange_metadata(self) -> Mapping[str, str]:
        values = {
            key: value
            for key, value in (
                ("exchange", self.exchange),
                ("neg_risk_exchange", self.neg_risk_exchange),
                ("neg_risk_market_id", self.neg_risk_market_id),
            )
            if value is not None
        }
        return values

    def to_dict(self) -> dict[str, Any]:
        return {
            "min_order_size": _text_decimal(self.min_order_size),
            "min_order_quantity": _text_decimal(self.min_order_size),
            "tick_size": _text_decimal(self.tick_size),
            "neg_risk": self.neg_risk,
            "price_precision": self.price_precision,
            "size_precision": self.size_precision,
            "quantity_precision": self.size_precision,
            "amount_precision": self.amount_precision,
            "sdk_version": self.sdk_version,
            "docs_version": self.docs_version,
            "rules_version": POLYMARKET_RULES_VERSION,
            "exchange": self.exchange,
            "neg_risk_exchange": self.neg_risk_exchange,
            "neg_risk_market_id": self.neg_risk_market_id,
            "exchange_metadata": dict(self.exchange_metadata),
        }

    def __getitem__(self, key: str) -> Any:
        return self.to_dict()[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self.to_dict())

    def __len__(self) -> int:
        return len(self.to_dict())


@dataclass(frozen=True, slots=True)
class DepthAssessment(Mapping[str, Any]):
    """Pure selected-token BUY/SELL depth result.

    Decimal attributes remain exact for callers doing further risk arithmetic;
    mapping serialization uses strings.  A missing selected side is a normal
    ``UNSUITABLE/NO_DEPTH`` result, deliberately distinct from malformed data.
    """

    action: str
    reason: str
    side: str
    token_id: str | None = None
    requested_quantity: Decimal | None = None
    filled_quantity: Decimal = ZERO
    available_quantity: Decimal = ZERO
    raw_notional: Decimal = ZERO
    adjusted_notional: Decimal = ZERO
    slippage_cost: Decimal = ZERO
    venue_fee: Decimal = ZERO
    fee_reserve: Decimal = ZERO
    required_cost: Decimal | None = None
    gross_proceeds: Decimal | None = None
    net_proceeds: Decimal | None = None
    price_bound: Decimal | None = None
    levels_used: int = 0
    rules_version: str = POLYMARKET_RULES_VERSION

    @property
    def suitable(self) -> bool:
        return self.action == SUITABLE

    @property
    def verdict(self) -> str:
        return self.action
    @property
    def status(self) -> str:
        return self.action

    @property
    def reason_code(self) -> str:
        return self.reason

    @property
    def required_quantity(self) -> Decimal | None:
        return self.requested_quantity

    @property
    def depth_quantity(self) -> Decimal:
        """Total executable quantity on the selected side after bounds."""
        return self.available_quantity
    @property
    def levels(self) -> int:
        """Compatibility alias for consumers naming the used levels count."""
        return self.levels_used


    def to_dict(self) -> dict[str, Any]:
        return {
            "action": self.action,
            "status": self.action,
            "reason": self.reason,
            "reason_code": self.reason,
            "suitable": self.suitable,
            "side": self.side,
            "token_id": self.token_id,
            "requested_quantity": _text_decimal(self.requested_quantity),
            "required_quantity": _text_decimal(self.requested_quantity),
            "filled_quantity": _text_decimal(self.filled_quantity),
            "available_quantity": _text_decimal(self.available_quantity),
            "depth_quantity": _text_decimal(self.available_quantity),
            "raw_notional": _text_decimal(self.raw_notional),
            "adjusted_notional": _text_decimal(self.adjusted_notional),
            "slippage_cost": _text_decimal(self.slippage_cost),
            "venue_fee": _text_decimal(self.venue_fee),
            "fee_reserve": _text_decimal(self.fee_reserve),
            "required_cost": _text_decimal(self.required_cost),
            "gross_proceeds": _text_decimal(self.gross_proceeds),
            "net_proceeds": _text_decimal(self.net_proceeds),
            "price_bound": _text_decimal(self.price_bound),
            "levels_used": self.levels_used,
            "levels": self.levels_used,
            "rules_version": self.rules_version,
        }

    def __getitem__(self, key: str) -> Any:
        return self.to_dict()[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self.to_dict())

    def __len__(self) -> int:
        return len(self.to_dict())


def parse_polymarket_rules(
    source: Any,
    *,
    sdk_version: str = SUPPORTED_POLYMARKET_SDK,
    docs_version: str = POLYMARKET_RULES_DOCS_VERSION,
) -> PolymarketRules:
    """Parse only documented Polymarket book rules, failing closed.

    ``source`` may be an API mapping, ``OrderBookSnapshot`` or
    ``InstrumentMetadata``.  Camel-case names are accepted only for the same
    documented fields emitted by the adapter.  Unknown exchange-style fields
    such as ``min_notional`` and ``size_increment`` are ignored, never used as
    required rules.
    """
    if sdk_version != SUPPORTED_POLYMARKET_SDK:
        raise PolymarketRuleError("UNSUPPORTED_POLYMARKET_SDK")
    if docs_version != POLYMARKET_RULES_DOCS_VERSION:
        raise PolymarketRuleError("UNSUPPORTED_POLYMARKET_RULES_DOCS")
    min_order = _required_decimal(
        source, ("min_order_size", "orderMinSize", "minOrderSize", "order_min_size"), "MIN_ORDER_SIZE"
    )
    tick = _required_decimal(
        source,
        ("tick_size", "tickSize", "orderPriceMinTickSize", "order_price_min_tick_size"),
        "TICK_SIZE",
    )
    neg_risk = _required_bool(source, ("neg_risk", "negRisk"), "NEG_RISK")
    if tick not in _TICK_PRECISION:
        raise PolymarketRuleError("TICK_SIZE_UNSUPPORTED")
    price_precision, amount_precision = _TICK_PRECISION[tick]
    return PolymarketRules(
        min_order_size=min_order,
        tick_size=tick,
        neg_risk=neg_risk,
        sdk_version=sdk_version,
        docs_version=docs_version,
        price_precision=price_precision,
        size_precision=SIZE_PRECISION,
        amount_precision=amount_precision,
        exchange=_optional_text(source, ("exchange", "exchange_address")),
        neg_risk_exchange=_optional_text(source, ("neg_risk_exchange", "negRiskExchange", "neg_risk_exchange_address")),
        neg_risk_market_id=_optional_text(source, ("neg_risk_market_id", "negRiskMarketID", "negRiskMarketId")),
    )


# Explicit descriptive alias used by adapters and external audit tools.
parse_official_polymarket_rules = parse_polymarket_rules
parse_polymarket_book_rules = parse_polymarket_rules
parse_rules = parse_polymarket_rules


def assess_selected_token_depth(
    book: Any,
    rules: PolymarketRules | Mapping[str, Any],
    *,
    side: str,
    quantity: Any | None = None,
    price_bound: Any | None = None,
    max_price: Any | None = None,
    min_price: Any | None = None,
    cap_usd: Any | None = None,
    venue_fee_rate: Any = ZERO,
    fee_rate: Any | None = None,
    slippage_rate: Any = ZERO,
    fee_reserve: Any = ZERO,
) -> DepthAssessment:
    """Assess exact selected-token depth without consulting the opposite token.

    BUY walks asks from lowest to highest; SELL walks bids from highest to
    lowest.  ``price_bound`` is an adverse-price guard (BUY maximum / SELL
    minimum), while ``cap_usd`` is an all-in BUY capital cap.  ``fee_reserve``
    is caller policy capital reserve and is reported separately from the
    venue's percentage fee; it is added to BUY required cost only.
    """
    normalized_side = str(side).strip().upper()
    if normalized_side not in {"BUY", "SELL"}:
        raise PolymarketRuleError("SIDE_INVALID")
    parsed_rules = rules if isinstance(rules, PolymarketRules) else parse_polymarket_rules(rules)
    requested = parsed_rules.min_order_size if quantity is None else _positive_decimal(quantity, "QUANTITY")
    if _decimal_places(requested) > parsed_rules.size_precision:
        return _assessment(normalized_side, "QUANTITY_PRECISION", parsed_rules, book, requested)
    if requested < parsed_rules.min_order_size:
        return _assessment(normalized_side, "MIN_ORDER_SIZE", parsed_rules, book, requested)
    aliases = max_price if normalized_side == "BUY" else min_price
    if price_bound is not None and aliases is not None:
        primary_bound = _nonnegative_decimal(price_bound, "PRICE_BOUND")
        alias_bound = _nonnegative_decimal(aliases, "PRICE_BOUND")
        if primary_bound != alias_bound:
            raise PolymarketRuleError("PRICE_BOUND_CONFLICT")
        selected_bound = primary_bound
    else:
        selected_bound = price_bound if price_bound is not None else aliases
    bound = None if selected_bound is None else _nonnegative_decimal(selected_bound, "PRICE_BOUND")
    if bound is not None and bound > ONE:
        raise PolymarketRuleError("PRICE_BOUND_INVALID")
    raw_fee = venue_fee_rate if fee_rate is None else fee_rate
    venue_fee_rate_decimal = _nonnegative_decimal(raw_fee, "VENUE_FEE_RATE")
    slip = _nonnegative_decimal(slippage_rate, "SLIPPAGE_RATE")
    if slip >= ONE:
        raise PolymarketRuleError("SLIPPAGE_RATE_INVALID")
    reserve = _nonnegative_decimal(fee_reserve, "FEE_RESERVE")
    cap = None if cap_usd is None else _positive_decimal(cap_usd, "CAP_USD")

    raw_levels = _value(book, "asks" if normalized_side == "BUY" else "bids", default=None)
    # Empty or omitted selected side means no executable action, not a corrupt
    # book.  In particular, do not alias BUY YES to SELL NO or vice versa.
    if raw_levels is None or (isinstance(raw_levels, Sequence) and not isinstance(raw_levels, (str, bytes)) and not raw_levels):
        return _assessment(normalized_side, "NO_DEPTH", parsed_rules, book, requested, bound=bound, reserve=reserve)
    if not isinstance(raw_levels, Sequence) or isinstance(raw_levels, (str, bytes)):
        return _assessment(normalized_side, "MALFORMED_BOOK", parsed_rules, book, requested, bound=bound, reserve=reserve)
    try:
        levels = _parse_levels(raw_levels, parsed_rules, normalized_side, bound)
    except PolymarketRuleError:
        return _assessment(normalized_side, "MALFORMED_BOOK", parsed_rules, book, requested, bound=bound, reserve=reserve)
    if not levels:
        return _assessment(normalized_side, "NO_DEPTH", parsed_rules, book, requested, bound=bound, reserve=reserve)

    remaining = requested
    filled = ZERO
    raw_notional = ZERO
    levels_used = 0
    for price, amount in levels:
        take = min(remaining, amount)
        if take <= ZERO:
            continue
        filled += take
        raw_notional += take * price
        levels_used += 1
        remaining -= take
        if remaining <= ZERO:
            break
    available = sum((amount for _, amount in levels), ZERO)
    multiplier = (ONE + slip) if normalized_side == "BUY" else (ONE - slip)
    adjusted_notional = raw_notional * multiplier
    slippage_cost = adjusted_notional - raw_notional
    venue_fee = adjusted_notional * venue_fee_rate_decimal
    gross = adjusted_notional if normalized_side == "SELL" else None
    net = adjusted_notional - venue_fee if normalized_side == "SELL" else None
    required_cost = adjusted_notional + venue_fee + reserve if normalized_side == "BUY" and filled >= requested else None
    if filled < requested:
        return _assessment(
            normalized_side,
            "INSUFFICIENT_DEPTH",
            parsed_rules,
            book,
            requested,
            bound=bound,
            filled=filled,
            available=available,
            raw=raw_notional,
            adjusted=adjusted_notional,
            slip_cost=slippage_cost,
            fee=venue_fee,
            reserve=reserve,
            gross=gross,
            net=net,
            levels_used=levels_used,
        )
    if normalized_side == "BUY" and cap is not None and required_cost is not None and required_cost > cap:
        return _assessment(
            normalized_side,
            "CAP_EXCEEDED",
            parsed_rules,
            book,
            requested,
            bound=bound,
            filled=filled,
            available=available,
            raw=raw_notional,
            adjusted=adjusted_notional,
            slip_cost=slippage_cost,
            fee=venue_fee,
            reserve=reserve,
            required=required_cost,
            gross=gross,
            net=net,
            levels_used=levels_used,
        )
    return _assessment(
        normalized_side,
        "OK",
        parsed_rules,
        book,
        requested,
        bound=bound,
        filled=filled,
        available=available,
        raw=raw_notional,
        adjusted=adjusted_notional,
        slip_cost=slippage_cost,
        fee=venue_fee,
        reserve=reserve,
        required=required_cost,
        gross=gross,
        net=net,
        levels_used=levels_used,
    )


assess_polymarket_depth = assess_selected_token_depth
assess_token_depth = assess_selected_token_depth
assess_depth = assess_selected_token_depth


def _assessment(
    side: str,
    reason: str,
    rules: PolymarketRules,
    book: Any,
    requested: Decimal,
    *,
    bound: Decimal | None = None,
    filled: Decimal = ZERO,
    available: Decimal = ZERO,
    raw: Decimal = ZERO,
    adjusted: Decimal = ZERO,
    slip_cost: Decimal = ZERO,
    fee: Decimal = ZERO,
    reserve: Decimal = ZERO,
    required: Decimal | None = None,
    gross: Decimal | None = None,
    net: Decimal | None = None,
    levels_used: int = 0,
) -> DepthAssessment:
    token = _optional_text(book, ("token_id", "tokenId", "asset_id", "assetId"))
    return DepthAssessment(
        action=SUITABLE if reason == "OK" else UNSUITABLE,
        reason=reason,
        side=side,
        token_id=token,
        requested_quantity=requested,
        filled_quantity=filled,
        available_quantity=available,
        raw_notional=raw,
        adjusted_notional=adjusted,
        slippage_cost=slip_cost,
        venue_fee=fee,
        fee_reserve=reserve,
        required_cost=required,
        gross_proceeds=gross,
        net_proceeds=net,
        price_bound=bound,
        levels_used=levels_used,
        rules_version=POLYMARKET_RULES_VERSION,
    )


def _parse_levels(raw_levels: Sequence[Any], rules: PolymarketRules, side: str, bound: Decimal | None) -> list[tuple[Decimal, Decimal]]:
    result: list[tuple[Decimal, Decimal]] = []
    for row in raw_levels:
        if isinstance(row, Mapping):
            price_raw = row.get("price", row.get("p"))
            amount_raw = row.get("size", row.get("quantity", row.get("q")))
        elif isinstance(row, Sequence) and not isinstance(row, (str, bytes)) and len(row) >= 2:
            price_raw, amount_raw = row[0], row[1]
        else:
            price_raw = getattr(row, "price", None)
            amount_raw = getattr(row, "size", getattr(row, "quantity", None))
            if price_raw is None or amount_raw is None:
                raise PolymarketRuleError("BOOK_LEVEL_INVALID")
        price = _positive_decimal(price_raw, "BOOK_PRICE")
        amount = _positive_decimal(amount_raw, "BOOK_SIZE")
        if price > ONE or _decimal_places(price) > rules.price_precision:
            raise PolymarketRuleError("BOOK_LEVEL_INVALID")
        if (price % rules.tick_size) != ZERO:
            raise PolymarketRuleError("BOOK_PRICE_TICK_MISMATCH")
        if bound is not None and ((side == "BUY" and price > bound) or (side == "SELL" and price < bound)):
            continue
        result.append((price, amount))
    result.sort(key=lambda item: item[0], reverse=side == "SELL")
    return result


def _present_values(source: Any, names: tuple[str, ...]) -> list[Any]:
    values: list[Any] = []
    if isinstance(source, Mapping):
        for name in names:
            if name in source and source[name] not in (None, ""):
                values.append(source[name])
        return values
    for name in names:
        try:
            value = getattr(source, name)
        except Exception:
            continue
        if value not in (None, ""):
            values.append(value)
    return values

def _required_decimal(source: Any, names: tuple[str, ...], label: str) -> Decimal:
    values = _present_values(source, names)
    if not values:
        raise PolymarketRuleError(f"{label}_MISSING")
    parsed_values: list[Decimal] = []
    for value in values:
        try:
            parsed = value if isinstance(value, Decimal) else Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise PolymarketRuleError(f"{label}_INVALID") from exc
        if not parsed.is_finite() or parsed <= ZERO:
            raise PolymarketRuleError(f"{label}_INVALID")
        parsed_values.append(parsed)
    if any(value != parsed_values[0] for value in parsed_values[1:]):
        raise PolymarketRuleError(f"{label}_CONFLICT")
    return parsed_values[0]
def _required_bool(source: Any, names: tuple[str, ...], label: str) -> bool:
    values = _present_values(source, names)
    if not values:
        raise PolymarketRuleError(f"{label}_MISSING")
    parsed: list[bool] = []
    for value in values:
        if isinstance(value, bool):
            parsed.append(value)
        elif isinstance(value, str) and value.strip().lower() in {"true", "1", "yes"}:
            parsed.append(True)
        elif isinstance(value, str) and value.strip().lower() in {"false", "0", "no"}:
            parsed.append(False)
        else:
            raise PolymarketRuleError(f"{label}_INVALID")
    if any(value != parsed[0] for value in parsed[1:]):
        raise PolymarketRuleError(f"{label}_CONFLICT")
    return parsed[0]


def _optional_text(source: Any, names: tuple[str, ...]) -> str | None:
    value = _value(source, *names, default=None)
    if value is None or isinstance(value, (bool, Mapping, Sequence)) and not isinstance(value, str):
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
            continue
        if value is not None:
            return value
    return default


def _positive_decimal(value: Any, label: str) -> Decimal:
    try:
        parsed = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise PolymarketRuleError(f"{label}_INVALID") from exc
    if not parsed.is_finite() or parsed <= ZERO:
        raise PolymarketRuleError(f"{label}_INVALID")
    return parsed


def _nonnegative_decimal(value: Any, label: str) -> Decimal:
    if isinstance(value, bool) or value is None:
        raise PolymarketRuleError(f"{label}_INVALID")
    try:
        parsed = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise PolymarketRuleError(f"{label}_INVALID") from exc
    if not parsed.is_finite() or parsed < ZERO:
        raise PolymarketRuleError(f"{label}_INVALID")
    return parsed


def _decimal_places(value: Decimal) -> int:
    return max(0, -value.as_tuple().exponent)


def _text_decimal(value: Decimal | None) -> str:
    if value is None:
        return UNKNOWN
    return format(value, "f")
__all__ = [
    "DepthAssessment",
    "POLYMARKET_DOCS_VERSION",
    "POLYMARKET_RULES_DOCS_VERSION",
    "POLYMARKET_RULES_VERSION",
    "POLYMARKET_SDK_VERSION",
    "PolymarketRuleError",
    "PolymarketRules",
    "SIZE_PRECISION",
    "SUITABLE",
    "SUPPORTED_POLYMARKET_SDK",
    "UNSUITABLE",
    "UNKNOWN",
    "assess_depth",
    "assess_polymarket_depth",
    "assess_selected_token_depth",
    "assess_token_depth",
    "parse_official_polymarket_rules",
    "parse_polymarket_book_rules",
    "parse_polymarket_rules",
    "parse_rules",
]
