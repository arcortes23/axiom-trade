"""Pure, Decimal-only risk contracts for the Binance Spot canary.

The module intentionally contains no network or account mutation code.  It turns
exchange ``exchangeInfo`` metadata and a point-in-time account/market snapshot
into a deterministic order decision.  A caller must re-run the assessment just
before submitting an order; a previous approval is not an authorization token.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_FLOOR
import copy
import hashlib
import json
from types import MappingProxyType
from typing import Any, Mapping, Sequence


UTC = timezone.utc
ZERO = Decimal("0")
ONE = Decimal("1")
QUOTE_ASSET = "USDT"
BINANCE_RISK_ENVELOPE_VERSION = "binance-spot-risk-v1"


def _decimal(value: Any, *, name: str = "value", nonnegative: bool = False) -> Decimal:
    """Convert exchange/account scalar values without ever doing float math."""
    if isinstance(value, bool) or value is None:
        raise ValueError(f"{name} must be a Decimal-compatible number")
    try:
        # str(float) is deterministic and avoids importing a binary float into
        # arithmetic.  Values emitted by Binance are normally strings.
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError(f"{name} must be a finite decimal") from exc
    if not result.is_finite():
        raise ValueError(f"{name} must be finite")
    if nonnegative and result < ZERO:
        raise ValueError(f"{name} must be non-negative")
    return result


def _optional_decimal(value: Any, *, name: str = "value") -> Decimal | None:
    if value is None or value == "":
        return None
    return _decimal(value, name=name)


def _utc(value: datetime | None) -> datetime:
    if value is None:
        return datetime.now(UTC)
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _key(value: Any) -> str:
    return str(value).strip().upper()


def _deep_decimal(value: Any) -> Any:
    """Keep unknown exchange filter values losslessly usable without float math."""
    if isinstance(value, Mapping):
        return {str(k): _deep_decimal(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_deep_decimal(v) for v in value]
    if isinstance(value, tuple):
        return tuple(_deep_decimal(v) for v in value)
    if isinstance(value, (str, int, Decimal)) and not isinstance(value, bool):
        # Preserve textual metadata such as filterType; numeric strings become
        # Decimal only when they parse as numbers.
        if isinstance(value, str):
            try:
                return Decimal(value)
            except InvalidOperation:
                return value
        return _decimal(value)
    return value


def _integer(value: Any, *, name: str) -> int:
    number = _decimal(value, name=name, nonnegative=True)
    if number != number.to_integral_value():
        raise ValueError(f"{name} must be an integer")
    return int(number)


def _optional_integer(value: Any, *, name: str) -> int | None:
    if value is None or value == "":
        return None
    return _integer(value, name=name)


def _audit_decimal(value: Any, *, name: str = "value") -> str:
    """Render an exact, stable USDT decimal with at least two fractional digits."""
    text = format(_decimal(value, name=name), "f")
    if "." not in text:
        return f"{text}.00"
    _, fraction = text.split(".", 1)
    if len(fraction) < 2:
        text += "0" * (2 - len(fraction))
    return text


def _freeze_mapping(value: Mapping[str, Any]) -> Mapping[str, Any]:
    return MappingProxyType(dict(value))


@dataclass(frozen=True, slots=True)
class BinanceRiskEnvelope:
    """Versioned hard limits for the first Binance Spot canary.

    All money and bps limits are :class:`Decimal` values.  The defaults are an
    intentionally small, guarantee-free canary budget and are immutable once
    constructed.
    """

    version: str = BINANCE_RISK_ENVELOPE_VERSION
    quote_asset: str = QUOTE_ASSET
    entry_notional: Decimal = Decimal("10")
    max_aggregate_exposure: Decimal = Decimal("30")
    max_reserved_exposure: Decimal = Decimal("30")
    realized_loss_entry_stop: Decimal = Decimal("5")
    equity_loss_entry_stop: Decimal = Decimal("5")
    max_positions: int = 5
    max_submissions_per_day: int = 20
    max_execution_deviation_bps: Decimal = Decimal("100")
    exit_order_reserve_per_position: int = 1

    def __post_init__(self) -> None:
        if not self.version.strip():
            raise ValueError("version is required")
        if self.quote_asset.strip().upper() != QUOTE_ASSET:
            raise ValueError("the Binance Spot canary quote asset is USDT")
        object.__setattr__(self, "quote_asset", QUOTE_ASSET)
        for name in (
            "entry_notional",
            "max_aggregate_exposure",
            "max_reserved_exposure",
            "realized_loss_entry_stop",
            "equity_loss_entry_stop",
            "max_execution_deviation_bps",
        ):
            value = _decimal(getattr(self, name), name=name, nonnegative=True)
            object.__setattr__(self, name, value)
        for name in ("max_positions", "max_submissions_per_day", "exit_order_reserve_per_position"):
            value = _integer(getattr(self, name), name=name)
            object.__setattr__(self, name, value)

    # Names used by adapters and persisted records in earlier canary drafts.
    @property
    def entry_cap(self) -> Decimal:
        return self.entry_notional

    @property
    def entry_notional_usdt(self) -> Decimal:
        return self.entry_notional

    @property
    def aggregate_exposure(self) -> Decimal:
        return self.max_aggregate_exposure

    @property
    def reserved_exposure(self) -> Decimal:
        return self.max_reserved_exposure

    @property
    def max_entry_notional(self) -> Decimal:
        return self.entry_notional

    @property
    def max_realized_loss(self) -> Decimal:
        return self.realized_loss_entry_stop

    @property
    def max_equity_loss(self) -> Decimal:
        return self.equity_loss_entry_stop

    @property
    def execution_deviation_bps(self) -> Decimal:
        return self.max_execution_deviation_bps

    @property
    def exit_order_reserve(self) -> int:
        return self.exit_order_reserve_per_position

    def as_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "quote_asset": self.quote_asset,
            "entry_notional": _audit_decimal(self.entry_notional, name="entry_notional"),
            "max_aggregate_exposure": _audit_decimal(self.max_aggregate_exposure, name="max_aggregate_exposure"),
            "max_reserved_exposure": _audit_decimal(self.max_reserved_exposure, name="max_reserved_exposure"),
            "realized_loss_entry_stop": _audit_decimal(self.realized_loss_entry_stop, name="realized_loss_entry_stop"),
            "equity_loss_entry_stop": _audit_decimal(self.equity_loss_entry_stop, name="equity_loss_entry_stop"),
            "max_positions": self.max_positions,
            "max_submissions_per_day": self.max_submissions_per_day,
            "max_execution_deviation_bps": str(self.max_execution_deviation_bps),
            "exit_order_reserve_per_position": self.exit_order_reserve_per_position,
        }

    def to_dict(self) -> dict[str, Any]:
        """Return the persisted, secret-free envelope binding."""
        return self.as_dict()

    @property
    def canonical_hash(self) -> str:
        return hashlib.sha256(json.dumps(self.as_dict(), sort_keys=True, separators=(",", ":")).encode()).hexdigest()

    @property
    def binding_hash(self) -> str:
        return self.canonical_hash


DEFAULT_BINANCE_RISK_ENVELOPE = BinanceRiskEnvelope()


@dataclass(frozen=True, slots=True)
class SymbolRules:
    """Decimal representation of all order-relevant exchangeInfo filters."""

    symbol: str
    base_asset: str = ""
    quote_asset: str = QUOTE_ASSET
    status: str = ""
    spot_trading_allowed: bool = True
    order_types: tuple[str, ...] = ()
    time_in_force: tuple[str, ...] = ()
    min_price: Decimal | None = None
    max_price: Decimal | None = None
    tick_size: Decimal | None = None
    min_qty: Decimal | None = None
    max_qty: Decimal | None = None
    step_size: Decimal | None = None
    market_min_qty: Decimal | None = None
    market_max_qty: Decimal | None = None
    market_step_size: Decimal | None = None
    min_notional: Decimal | None = None
    max_notional: Decimal | None = None
    min_notional_apply_to_market: bool = False
    max_notional_apply_to_market: bool = False
    percent_multiplier_up: Decimal | None = None
    percent_multiplier_down: Decimal | None = None
    percent_avg_price_mins: int = 0
    bid_multiplier_up: Decimal | None = None
    bid_multiplier_down: Decimal | None = None
    ask_multiplier_up: Decimal | None = None
    ask_multiplier_down: Decimal | None = None
    side_percent_avg_price_mins: int = 0
    max_num_orders: int | None = None
    max_num_algo_orders: int | None = None
    max_num_iceberg_orders: int | None = None
    exchange_max_num_orders: int | None = None
    exchange_max_num_algo_orders: int | None = None
    max_position: Decimal | None = None
    unknown_filters: Mapping[str, Any] = field(default_factory=dict)
    raw_filters: tuple[Mapping[str, Any], ...] = ()

    @classmethod
    def from_exchange_info(cls, value: Mapping[str, Any]) -> "SymbolRules":
        if not isinstance(value, Mapping):
            raise TypeError("symbol exchangeInfo must be a mapping")
        parsed: dict[str, Any] = {
            "symbol": str(value.get("symbol", "")).upper(),
            "base_asset": str(value.get("baseAsset", "")),
            "quote_asset": str(value.get("quoteAsset", QUOTE_ASSET)).upper(),
            "status": str(value.get("status", "")).upper(),
            "spot_trading_allowed": bool(value.get("isSpotTradingAllowed", True)),
            "order_types": tuple(str(x).upper() for x in value.get("orderTypes", ()) or ()),
            "time_in_force": tuple(str(x).upper() for x in value.get("timeInForce", ()) or ()),
        }
        filters = value.get("filters", ()) or ()
        unknown: dict[str, Any] = {}
        raw: list[Mapping[str, Any]] = []
        for row in filters:
            if not isinstance(row, Mapping):
                continue
            ftype = str(row.get("filterType", "")).upper()
            raw.append(_freeze_mapping(copy.deepcopy(dict(row))))
            if ftype == "PRICE_FILTER":
                parsed.update(min_price=_optional_decimal(row.get("minPrice"), name="minPrice"), max_price=_optional_decimal(row.get("maxPrice"), name="maxPrice"), tick_size=_optional_decimal(row.get("tickSize"), name="tickSize"))
            elif ftype == "LOT_SIZE":
                parsed.update(min_qty=_optional_decimal(row.get("minQty"), name="minQty"), max_qty=_optional_decimal(row.get("maxQty"), name="maxQty"), step_size=_optional_decimal(row.get("stepSize"), name="stepSize"))
            elif ftype == "MARKET_LOT_SIZE":
                parsed.update(market_min_qty=_optional_decimal(row.get("minQty"), name="marketMinQty"), market_max_qty=_optional_decimal(row.get("maxQty"), name="marketMaxQty"), market_step_size=_optional_decimal(row.get("stepSize"), name="marketStepSize"))
            elif ftype == "MIN_NOTIONAL":
                parsed["min_notional"] = _optional_decimal(row.get("minNotional"), name="minNotional")
                parsed["min_notional_apply_to_market"] = bool(row.get("applyToMarket", False))
            elif ftype == "NOTIONAL":
                # NOTIONAL is the newer complete min/max form.  Keep the
                # strictest values when both filters are supplied.
                nmin = _optional_decimal(row.get("minNotional"), name="minNotional")
                nmax = _optional_decimal(row.get("maxNotional"), name="maxNotional")
                oldmin = parsed.get("min_notional")
                parsed["min_notional"] = nmin if oldmin is None else max(oldmin, nmin or ZERO)
                parsed["max_notional"] = nmax
                parsed["max_notional_apply_to_market"] = bool(row.get("applyMaxToMarket", False))
                if "applyMinToMarket" in row:
                    parsed["min_notional_apply_to_market"] = bool(row.get("applyMinToMarket"))
            elif ftype == "PERCENT_PRICE":
                avg_price_mins = _optional_integer(row.get("avgPriceMins"), name="avgPriceMins")
                parsed.update(
                    percent_multiplier_up=_optional_decimal(row.get("multiplierUp"), name="multiplierUp"),
                    percent_multiplier_down=_optional_decimal(row.get("multiplierDown"), name="multiplierDown"),
                    percent_avg_price_mins=avg_price_mins or 0,
                )
            elif ftype == "PERCENT_PRICE_BY_SIDE":
                avg_price_mins = _optional_integer(row.get("avgPriceMins"), name="avgPriceMins")
                parsed.update(
                    bid_multiplier_up=_optional_decimal(row.get("bidMultiplierUp"), name="bidMultiplierUp"),
                    bid_multiplier_down=_optional_decimal(row.get("bidMultiplierDown"), name="bidMultiplierDown"),
                    ask_multiplier_up=_optional_decimal(row.get("askMultiplierUp"), name="askMultiplierUp"),
                    ask_multiplier_down=_optional_decimal(row.get("askMultiplierDown"), name="askMultiplierDown"),
                    side_percent_avg_price_mins=avg_price_mins or 0,
                )
            elif ftype == "MAX_NUM_ORDERS":
                parsed["max_num_orders"] = _optional_integer(row.get("maxNumOrders"), name="maxNumOrders")
            elif ftype == "MAX_NUM_ALGO_ORDERS":
                parsed["max_num_algo_orders"] = _optional_integer(row.get("maxNumAlgoOrders"), name="maxNumAlgoOrders")
            elif ftype == "MAX_NUM_ICEBERG_ORDERS":
                parsed["max_num_iceberg_orders"] = _optional_integer(row.get("maxNumIcebergOrders"), name="maxNumIcebergOrders")
            elif ftype == "EXCHANGE_MAX_NUM_ORDERS":
                parsed["exchange_max_num_orders"] = _optional_integer(row.get("maxNumOrders"), name="maxNumOrders")
            elif ftype == "EXCHANGE_MAX_ALGO_ORDERS":
                parsed["exchange_max_num_algo_orders"] = _optional_integer(row.get("maxNumAlgoOrders", row.get("maxNumOrders")), name="maxNumAlgoOrders")
            elif ftype == "MAX_POSITION":
                parsed["max_position"] = _optional_decimal(row.get("maxPosition"), name="maxPosition")
            else:
                unknown[ftype or "UNKNOWN"] = copy.deepcopy(dict(row))
        parsed["unknown_filters"] = _freeze_mapping(unknown)
        parsed["raw_filters"] = tuple(raw)
        # A missing symbol is malformed metadata, not a permissive wildcard.
        if not parsed["symbol"]:
            raise ValueError("exchangeInfo symbol is required")
        return cls(**parsed)

    parse = from_exchange_info
    from_exchange_symbol = from_exchange_info

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", self.symbol.upper())
        object.__setattr__(self, "quote_asset", self.quote_asset.upper())
        for name in (
            "min_price", "max_price", "tick_size", "min_qty", "max_qty", "step_size",
            "market_min_qty", "market_max_qty", "market_step_size", "min_notional",
            "max_notional", "percent_multiplier_up", "percent_multiplier_down",
            "bid_multiplier_up", "bid_multiplier_down", "ask_multiplier_up", "ask_multiplier_down",
            "max_position",
        ):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, _decimal(value, name=name, nonnegative=True))
        for name in ("max_num_orders", "max_num_algo_orders", "max_num_iceberg_orders", "exchange_max_num_orders", "exchange_max_num_algo_orders"):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, _integer(value, name=name))
        object.__setattr__(self, "unknown_filters", _freeze_mapping(self.unknown_filters))

    @property
    def filters(self) -> Mapping[str, Any]:
        return self.unknown_filters


    def _tick(self, value: Decimal, side: str) -> Decimal:
        tick = self.tick_size
        if tick is None or tick == ZERO:
            return value
        return (
            value / tick
        ).to_integral_value(
            rounding=ROUND_CEILING if _key(side) == "BUY" else ROUND_FLOOR
        ) * tick

    def round_price(self, price: Any, side: str) -> Decimal:
        value = _decimal(price, name="price", nonnegative=True)
        rounded = self._tick(value, side)
        if self.min_price is not None and self.min_price > ZERO and rounded < self.min_price:
            raise ValueError("PRICE_BELOW_MINIMUM")
        if self.max_price is not None and self.max_price > ZERO and rounded > self.max_price:
            raise ValueError("PRICE_ABOVE_MAXIMUM")
        return rounded

    def round_quantity(self, quantity: Any, side: str = "BUY", *, market: bool = False, available: Any = None) -> Decimal:
        value = _decimal(quantity, name="quantity", nonnegative=True)
        step = self.market_step_size if market and self.market_step_size not in (None, ZERO) else self.step_size
        max_qty = self.market_max_qty if market and self.market_max_qty is not None and self.market_max_qty > ZERO else self.max_qty
        if available is not None:
            value = min(value, _decimal(available, name="available", nonnegative=True))
        if step not in (None, ZERO):
            value = (value / step).to_integral_value(rounding=ROUND_FLOOR) * step
        if max_qty is not None and max_qty > ZERO:
            value = min(value, max_qty)
            if step not in (None, ZERO):
                value = (value / step).to_integral_value(rounding=ROUND_FLOOR) * step
        return value

    def round_order(self, side: str, price: Any, quantity: Any, *, market: bool = False, available: Any = None) -> tuple[Decimal, Decimal]:
        return self.round_price(price, side), self.round_quantity(quantity, side, market=market, available=available)


@dataclass(frozen=True, slots=True)
class PendingOrder:
    symbol: str
    side: str
    quantity: Decimal
    price: Decimal | None = None
    status: str = "PENDING"
    fee_reserve: Decimal = ZERO
    order_id: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", self.symbol.upper())
        object.__setattr__(self, "side", _key(self.side))
        object.__setattr__(self, "quantity", _decimal(self.quantity, name="pending quantity", nonnegative=True))
        object.__setattr__(self, "price", _optional_decimal(self.price, name="pending price"))
        object.__setattr__(self, "fee_reserve", _decimal(self.fee_reserve, name="fee reserve", nonnegative=True))

    @classmethod
    def from_value(cls, value: Any) -> "PendingOrder":
        if isinstance(value, PendingOrder):
            return value
        if not isinstance(value, Mapping):
            raise TypeError("pending order must be a mapping")
        return cls(symbol=str(value.get("symbol", "")), side=str(value.get("side", "BUY")), quantity=value.get("quantity", value.get("origQty", 0)), price=value.get("price"), status=str(value.get("status", "PENDING")), fee_reserve=value.get("fee_reserve", value.get("fee", 0)), order_id=value.get("orderId"))

    @property
    def unresolved(self) -> bool:
        return self.status.upper() in {"PENDING", "NEW", "PARTIALLY_FILLED", "UNKNOWN", "ACK_UNKNOWN", "OPEN"}

    @property
    def notional(self) -> Decimal:
        return self.quantity * (self.price or ZERO)

    @property
    def reservation(self) -> Decimal:
        if self.status.upper() in {"UNKNOWN", "ACK_UNKNOWN"}:
            return self.notional + self.fee_reserve
        return self.notional + self.fee_reserve if self.side == "BUY" else ZERO


@dataclass(frozen=True, slots=True)
class RiskSnapshot:
    """Immutable point-in-time state consumed by :func:`assess_order`."""

    observed_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    quote_available: Decimal = ZERO
    aggregate_exposure: Decimal = ZERO
    reserved_exposure: Decimal = ZERO
    owned_inventory: Mapping[str, Decimal] = field(default_factory=dict)
    pending_orders: tuple[PendingOrder, ...] = ()
    positions: int = 0
    entry_submissions_today: int = 0
    exit_submissions_today: int = 0
    realized_loss_today: Decimal = ZERO
    equity_loss_today: Decimal = ZERO
    unrealized_pnl: Decimal = ZERO
    fees_today: Decimal = ZERO
    open_orders: int = 0
    account_order_count: int = 0
    exchange_order_count: int = 0
    account_paused: bool = False
    rate_limited: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "observed_at", _utc(self.observed_at))
        for name in ("quote_available", "aggregate_exposure", "reserved_exposure", "realized_loss_today", "equity_loss_today", "unrealized_pnl", "fees_today"):
            object.__setattr__(self, name, _decimal(getattr(self, name), name=name, nonnegative=name not in {"unrealized_pnl"}))
        inv: dict[str, Decimal] = {}
        for symbol, qty in (self.owned_inventory or {}).items():
            inv[str(symbol).upper()] = _decimal(qty, name="owned inventory", nonnegative=True)
        object.__setattr__(self, "owned_inventory", _freeze_mapping(inv))
        orders = tuple(PendingOrder.from_value(row) for row in (self.pending_orders or ()))
        object.__setattr__(self, "pending_orders", orders)
        for name in ("positions", "entry_submissions_today", "exit_submissions_today", "open_orders", "account_order_count", "exchange_order_count"):
            object.__setattr__(self, name, _integer(getattr(self, name), name=name))
    @property
    def pending_reservation(self) -> Decimal:
        # Unknown and unresolved orders remain reserved until reconciled.  This
        return sum((order.reservation for order in self.pending_orders if order.unresolved), ZERO)

    @property
    def unresolved_exposure(self) -> Decimal:
        return self.reserved_exposure + self.pending_reservation

    @property
    def submissions_today(self) -> int:
        return self.entry_submissions_today + self.exit_submissions_today

    @property
    def owned_positions(self) -> int:
        return self.positions if self.positions else sum(1 for symbol, qty in self.owned_inventory.items() if symbol != QUOTE_ASSET and qty > ZERO)

    @property
    def utc_day(self) -> date:
        return self.observed_at.date()

    def project(self, now: datetime) -> "RiskSnapshot":
        """Project to UTC ``now``; only UTC-day counters/losses reset.

        Pending/UNKNOWN reservations, open orders, inventory and exposure are
        carried through rollover; a clock boundary never creates buying room.
        """
        instant = _utc(now)
        if instant.date() == self.utc_day:
            return self
        return RiskSnapshot(
            observed_at=instant,
            quote_available=self.quote_available,
            aggregate_exposure=self.aggregate_exposure,
            reserved_exposure=self.reserved_exposure,
            owned_inventory=self.owned_inventory,
            pending_orders=self.pending_orders,
            positions=self.positions,
            entry_submissions_today=0,
            exit_submissions_today=0,
            realized_loss_today=ZERO,
            equity_loss_today=ZERO,
            unrealized_pnl=self.unrealized_pnl,
            fees_today=ZERO,
            open_orders=self.open_orders,
            account_order_count=self.account_order_count,
            exchange_order_count=self.exchange_order_count,
            account_paused=self.account_paused,
            rate_limited=self.rate_limited,
        )

    project_utc_day = project
    projected_at = project
    def to_dict(self) -> dict[str, Any]:
        """Secret-free persistence/projection with Decimal values as strings."""
        return {
            "observed_at": self.observed_at.isoformat(),
            "quote_available": _audit_decimal(self.quote_available, name="quote_available"),
            "aggregate_exposure": _audit_decimal(self.aggregate_exposure, name="aggregate_exposure"),
            "reserved_exposure": _audit_decimal(self.reserved_exposure, name="reserved_exposure"),
            "unresolved_exposure": _audit_decimal(self.unresolved_exposure, name="unresolved_exposure"),
            "owned_inventory": {key: str(value) for key, value in self.owned_inventory.items()},
            "positions": self.owned_positions,
            "entry_submissions_today": self.entry_submissions_today,
            "exit_submissions_today": self.exit_submissions_today,
            "realized_loss_today": _audit_decimal(self.realized_loss_today, name="realized_loss_today"),
            "equity_loss_today": _audit_decimal(self.equity_loss_today, name="equity_loss_today"),
            "unrealized_pnl": _audit_decimal(self.unrealized_pnl, name="unrealized_pnl"),
            "fees_today": _audit_decimal(self.fees_today, name="fees_today"),
        }

    @classmethod
    def from_account(cls, account: Mapping[str, Any], *, now: datetime | None = None, pending_orders: Sequence[Any] = ()) -> "RiskSnapshot":
        balances = account.get("owned_inventory", account.get("inventory", {})) or {}
        if isinstance(balances, Sequence) and not isinstance(balances, (str, bytes, Mapping)):
            balances = {row.get("asset"): row.get("free", row.get("quantity", 0)) for row in balances if isinstance(row, Mapping)}
        pending = account.get("pending_orders", pending_orders)
        return cls(
            observed_at=_utc(now or account.get("observed_at")),
            quote_available=account.get("quote_available", account.get("free_quote", account.get("USDT", 0))),
            aggregate_exposure=account.get("aggregate_exposure", account.get("exposure", 0)),
            reserved_exposure=account.get("reserved_exposure", account.get("reserved", 0)),
            owned_inventory=balances,
            pending_orders=tuple(pending or ()),
            positions=account.get("positions", 0),
            entry_submissions_today=account.get("entry_submissions_today", account.get("entry_count_today", 0)),
            exit_submissions_today=account.get("exit_submissions_today", account.get("exit_count_today", 0)),
            realized_loss_today=account.get("realized_loss_today", account.get("daily_realized_loss", 0)),
            equity_loss_today=account.get("equity_loss_today", account.get("daily_equity_loss", 0)),
            unrealized_pnl=account.get("unrealized_pnl", 0),
            fees_today=account.get("fees_today", 0),
            open_orders=account.get("open_orders", account.get("open_order_count", 0)),
            account_order_count=account.get("account_order_count", account.get("open_orders", 0)),
            exchange_order_count=account.get("exchange_order_count", account.get("open_orders", 0)),
            account_paused=bool(account.get("account_paused", False)),
            rate_limited=bool(account.get("rate_limited", False)),
        )


@dataclass(frozen=True, slots=True)
class RiskAssessment:
    allowed: bool
    reasons: tuple[str, ...] = ()
    symbol: str = ""
    side: str = ""
    price: Decimal | None = None
    quantity: Decimal = ZERO
    notional: Decimal = ZERO
    fee_reserve: Decimal = ZERO
    projected_exposure: Decimal = ZERO
    projected_reserved: Decimal = ZERO
    checks: Mapping[str, bool] = field(default_factory=dict)

    def __bool__(self) -> bool:
        return self.allowed

    @property
    def ok(self) -> bool:
        return self.allowed

    @property
    def skip_reasons(self) -> tuple[str, ...]:
        return self.reasons

    @property
    def reason(self) -> str:
        return "; ".join(self.reasons)

    @property
    def order(self) -> Mapping[str, Any]:
        return {"symbol": self.symbol, "side": self.side, "price": self.price, "quantity": self.quantity, "notional": self.notional}
    @property
    def projection(self) -> Mapping[str, str]:
        return {
            "notional": _audit_decimal(self.notional, name="notional"),
            "fee_reserve": _audit_decimal(self.fee_reserve, name="fee_reserve"),
            "projected_exposure": _audit_decimal(self.projected_exposure, name="projected_exposure"),
            "projected_reserved": _audit_decimal(self.projected_reserved, name="projected_reserved"),
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "allowed": self.allowed,
            "reasons": list(self.reasons),
            "symbol": self.symbol,
            "side": self.side,
            "price": None if self.price is None else _audit_decimal(self.price, name="price"),
            "quantity": str(self.quantity),
            "notional": _audit_decimal(self.notional, name="notional"),
            "fee_reserve": _audit_decimal(self.fee_reserve, name="fee_reserve"),
            "projected_exposure": _audit_decimal(self.projected_exposure, name="projected_exposure"),
            "projected_reserved": _audit_decimal(self.projected_reserved, name="projected_reserved"),
            "checks": dict(self.checks),
        }


def _market_value(market: Mapping[str, Any] | None, *names: str) -> Any:
    if not market:
        return None
    for name in names:
        if name in market and market[name] is not None:
            return market[name]
    return None


def _market_checks(market: Mapping[str, Any] | None, *, now: datetime, requested_price: Decimal, fill_price: Decimal, envelope: BinanceRiskEnvelope) -> list[str]:
    if not market:
        return []
    reasons: list[str] = []
    status = str(_market_value(market, "status") or "TRADING").upper()
    if status != "TRADING":
        reasons.append("NOT_TRADING")
    if "isSpotTradingAllowed" in market and not bool(market["isSpotTradingAllowed"]):
        reasons.append("SPOT_NOT_ALLOWED")
    if bool(_market_value(market, "account_paused", "paused")):
        reasons.append("ACCOUNT_PAUSED")
    if bool(_market_value(market, "rate_limited", "rate_limit", "rate_limited_pause")):
        reasons.append("RATE_LIMIT")
    if bool(_market_value(market, "crash", "crash_pause", "crashed")):
        reasons.append("CRASH")
    if _market_value(market, "depth_ok") is False or _market_value(market, "thin_book") is True:
        reasons.append("THIN_BOOK")
    depth = _market_value(market, "depth_available", "available_depth", "conservative_depth")
    min_depth = _market_value(market, "min_depth", "required_depth")
    if depth is not None and min_depth is not None and _decimal(depth) < _decimal(min_depth):
        reasons.append("THIN_BOOK")
    spread = _market_value(market, "spread_bps", "spread")
    max_spread = _market_value(market, "max_spread_bps", "maximum_spread_bps")
    if spread is not None and max_spread is not None and _decimal(spread) > _decimal(max_spread):
        reasons.append("WIDE_SPREAD")
    if _market_value(market, "fresh") is False or _market_value(market, "stale") is True:
        reasons.append("STALE_MARKET")
    seen = _market_value(market, "observed_at", "updated_at", "timestamp")
    max_age = _market_value(market, "max_age_seconds", "freshness_seconds")
    if seen is not None and max_age is not None:
        if isinstance(seen, datetime):
            stamp = _utc(seen)
        else:
            try:
                seconds = _decimal(seen, name="market timestamp")
                epoch = datetime(1970, 1, 1, tzinfo=UTC)
                stamp = epoch + timedelta(microseconds=int(seconds * Decimal("1000000")))
            except (ValueError, TypeError):
                try:
                    stamp = datetime.fromisoformat(str(seen).replace("Z", "+00:00")).astimezone(UTC)
                except ValueError:
                    stamp = now
        max_age_us = int(_decimal(max_age, name="max age", nonnegative=True) * Decimal("1000000"))
        if now - stamp > timedelta(microseconds=max_age_us):
            reasons.append("STALE_MARKET")
    deviation = abs(fill_price - requested_price) / requested_price * Decimal("10000") if requested_price else Decimal("Infinity")
    if deviation > envelope.max_execution_deviation_bps:
        reasons.append("EXECUTION_DEVIATION")
    return list(dict.fromkeys(reasons))


def _filter_reason(rules: SymbolRules, *, side: str, price: Decimal, quantity: Decimal, notional: Decimal, order_type: str, average_price: Decimal | None, max_position_after: Decimal | None) -> list[str]:
    side = _key(side)
    reasons: list[str] = []
    if rules.min_price is not None and rules.min_price > ZERO and price < rules.min_price:
        reasons.append("PRICE_BELOW_MINIMUM")
    if rules.max_price is not None and rules.max_price > ZERO and price > rules.max_price:
        reasons.append("PRICE_ABOVE_MAXIMUM")
    if rules.tick_size not in (None, ZERO) and (price / rules.tick_size) != (price / rules.tick_size).to_integral_value():
        reasons.append("PRICE_TICK")
    min_qty = rules.market_min_qty if order_type == "MARKET" and rules.market_min_qty is not None and rules.market_min_qty > ZERO else rules.min_qty
    max_qty = rules.market_max_qty if order_type == "MARKET" and rules.market_max_qty is not None and rules.market_max_qty > ZERO else rules.max_qty
    step = rules.market_step_size if order_type == "MARKET" and rules.market_step_size not in (None, ZERO) else rules.step_size
    if min_qty is not None and min_qty > ZERO and quantity < min_qty:
        reasons.append("QUANTITY_BELOW_MINIMUM")
    if max_qty is not None and max_qty > ZERO and quantity > max_qty:
        reasons.append("QUANTITY_ABOVE_MAXIMUM")
    if step not in (None, ZERO) and (quantity / step) != (quantity / step).to_integral_value():
        reasons.append("QUANTITY_STEP")
    if rules.min_notional is not None and rules.min_notional > ZERO and notional < rules.min_notional:
        reasons.append("MIN_NOTIONAL")
    if rules.max_notional is not None and rules.max_notional > ZERO and notional > rules.max_notional:
        reasons.append("MAX_NOTIONAL")
    if average_price is not None:
        if rules.percent_multiplier_up is not None and rules.percent_multiplier_up > ZERO and price > average_price * rules.percent_multiplier_up:
            reasons.append("PERCENT_PRICE_ABOVE_MAX")
        if rules.percent_multiplier_down is not None and rules.percent_multiplier_down > ZERO and price < average_price * rules.percent_multiplier_down:
            reasons.append("PERCENT_PRICE_BELOW_MIN")
        up = rules.bid_multiplier_up if side == "BUY" else rules.ask_multiplier_up
        down = rules.bid_multiplier_down if side == "BUY" else rules.ask_multiplier_down
        if up is not None and up > ZERO and price > average_price * up:
            reasons.append("PERCENT_PRICE_BY_SIDE_ABOVE_MAX")
        if down is not None and down > ZERO and price < average_price * down:
            reasons.append("PERCENT_PRICE_BY_SIDE_BELOW_MIN")
    elif any(x is not None and x > ZERO for x in (rules.percent_multiplier_up, rules.percent_multiplier_down, rules.bid_multiplier_up, rules.bid_multiplier_down, rules.ask_multiplier_up, rules.ask_multiplier_down)):
        reasons.append("FILTER_REFERENCE_UNAVAILABLE")
    if max_position_after is not None and rules.max_position is not None and rules.max_position > ZERO and max_position_after > rules.max_position:
        reasons.append("MAX_POSITION")
    return reasons


def assess_order(
    rules: SymbolRules,
    side: str,
    price: Any,
    quantity: Any,
    *,
    snapshot: RiskSnapshot | Mapping[str, Any] | None = None,
    envelope: BinanceRiskEnvelope = DEFAULT_BINANCE_RISK_ENVELOPE,
    market: Mapping[str, Any] | None = None,
    order_type: str = "LIMIT",
    time_in_force: str = "IOC",
    now: datetime | None = None,
    fee_rate: Any = ZERO,
    fee_bps: Any = None,
    available_inventory: Any = None,
) -> RiskAssessment:
    """Assess a LIMIT IOC/FOK (or MARKET) order without side effects."""
    if not isinstance(rules, SymbolRules):
        rules = SymbolRules.from_exchange_info(rules)  # type: ignore[arg-type]
    state = snapshot if isinstance(snapshot, RiskSnapshot) else RiskSnapshot.from_account(snapshot or {}, now=now)
    instant = _utc(now or state.observed_at)
    side = _key(side)
    order_type = _key(order_type)
    tif = _key(time_in_force)
    reasons: list[str] = []
    if side not in {"BUY", "SELL"}:
        reasons.append("INVALID_SIDE")
    if order_type == "LIMIT" and tif not in {"IOC", "FOK"}:
        reasons.append("TIME_IN_FORCE_NOT_ALLOWED")
    if rules.order_types and order_type not in rules.order_types:
        reasons.append("ORDER_TYPE_NOT_ALLOWED")
    if rules.time_in_force and tif not in rules.time_in_force:
        reasons.append("TIME_IN_FORCE_NOT_ALLOWED")
    if rules.status and rules.status != "TRADING":
        reasons.append("NOT_TRADING")
    if not rules.spot_trading_allowed:
        reasons.append("SPOT_NOT_ALLOWED")
    state = state.project(instant)
    fee = _decimal(fee_rate if fee_bps is None else _decimal(fee_bps, name="fee bps") / Decimal("10000"), name="fee rate", nonnegative=True)
    requested_price = _decimal(price, name="price", nonnegative=True)
    requested_quantity = _decimal(quantity, name="quantity", nonnegative=True)
    owned = state.owned_inventory.get(rules.symbol, ZERO)
    if side == "SELL" and rules.symbol not in state.owned_inventory and available_inventory is None:
        reasons.append("FOREIGN_INVENTORY")
    if side == "SELL":
        available = owned if available_inventory is None else min(owned, _decimal(available_inventory, name="available inventory", nonnegative=True))
    else:
        available = None
    try:
        rounded_price = rules.round_price(requested_price, side)
    except ValueError as exc:
        rounded_price = requested_price
        reasons.append(str(exc))
    rounded_quantity = rules.round_quantity(requested_quantity, side, market=order_type == "MARKET", available=available)
    # Entry cap is all-in (quote notional plus the reserved fee), then qty is
    # floored again.  This is what makes a ceiling-rounded BUY safe.
    if side == "BUY" and rounded_price > ZERO:
        cap_qty = envelope.entry_notional / (rounded_price * (ONE + fee))
        rounded_quantity = rules.round_quantity(min(rounded_quantity, cap_qty), side, market=order_type == "MARKET")
    notional = rounded_price * rounded_quantity
    fee_reserve = notional * fee
    if side == "SELL" and rounded_quantity > available:
        reasons.append("INSUFFICIENT_OWNED_INVENTORY")
    if side == "SELL" and rounded_quantity <= ZERO:
        reasons.extend(("DUST", "EXIT_BELOW_MINIMUM"))
    if side == "SELL" and rules.min_notional is not None and rules.min_notional > ZERO and notional < rules.min_notional:
        reasons.append("EXIT_BELOW_MINIMUM")
    reasons.extend(_filter_reason(rules, side=side, price=rounded_price, quantity=rounded_quantity, notional=notional, order_type=order_type, average_price=_optional_decimal(_market_value(market, "average_price", "weighted_average_price", "reference_price", "last_price")), max_position_after=(owned + rounded_quantity if side == "BUY" else owned - rounded_quantity)))
    fill_price = _optional_decimal(_market_value(market, "conservative_fill_price", "fill_price")) or rounded_price
    reasons.extend(_market_checks(market, now=instant, requested_price=requested_price, fill_price=fill_price, envelope=envelope))
    if side == "BUY":
        if notional + fee_reserve > state.quote_available:
            reasons.append("INSUFFICIENT_QUOTE")
        if notional + fee_reserve > envelope.entry_notional:
            reasons.append("ENTRY_CAP_EXCEEDED")
        if state.aggregate_exposure + notional > envelope.max_aggregate_exposure:
            reasons.append("AGGREGATE_EXPOSURE")
        if state.unresolved_exposure + notional + fee_reserve > envelope.max_reserved_exposure:
            reasons.append("RESERVED_EXPOSURE")
        if state.owned_positions >= envelope.max_positions:
            reasons.append("MAX_POSITIONS")
        if state.realized_loss_today >= envelope.realized_loss_entry_stop:
            reasons.append("DAILY_REALIZED_LOSS")
        # Equity loss includes unrealized P/L and fees supplied by the account
        # snapshot, and therefore does not reset merely because a position is
        # marked down between two calls.
        if state.equity_loss_today + max(ZERO, -state.unrealized_pnl) + state.fees_today >= envelope.equity_loss_entry_stop:
            reasons.append("DAILY_EQUITY_LOSS")
        if state.entry_submissions_today + 1 > envelope.max_submissions_per_day:
            reasons.append("SUBMISSIONS_PER_DAY")
    else:
        if state.exit_submissions_today + 1 > envelope.max_submissions_per_day:
            reasons.append("SUBMISSIONS_PER_DAY")
    # Exchange/account order limits are evaluated with the exit reserve kept
    # available for each owned position.
    current_orders = max(state.open_orders, state.account_order_count)
    if rules.max_num_orders is not None and rules.max_num_orders > 0 and current_orders + 1 > rules.max_num_orders:
        reasons.append("MAX_NUM_ORDERS")
    if rules.exchange_max_num_orders is not None and rules.exchange_max_num_orders > 0 and max(state.exchange_order_count, current_orders) + 1 > rules.exchange_max_num_orders:
        reasons.append("EXCHANGE_MAX_NUM_ORDERS")
    reserve_needed = max(0, state.owned_positions * envelope.exit_order_reserve_per_position - (state.exit_submissions_today))
    if side == "BUY" and rules.max_num_orders is not None and rules.max_num_orders > 0 and current_orders + 1 + reserve_needed > rules.max_num_orders:
        reasons.append("EXIT_ORDER_RESERVE")
    if state.account_paused:
        reasons.append("ACCOUNT_PAUSED")
    if state.rate_limited:
        reasons.append("RATE_LIMIT")
    # Stable first-occurrence reasons are useful in persisted skip records.
    reasons = list(dict.fromkeys(reasons))
    return RiskAssessment(
        allowed=not reasons,
        reasons=tuple(reasons),
        symbol=rules.symbol,
        side=side,
        price=rounded_price,
        quantity=rounded_quantity,
        notional=notional,
        fee_reserve=fee_reserve,
        projected_exposure=state.aggregate_exposure + (notional if side == "BUY" else -notional),
        projected_reserved=state.unresolved_exposure + (notional + fee_reserve if side == "BUY" else ZERO),
        checks={reason: reason not in reasons for reason in ("NOT_TRADING", "SPOT_NOT_ALLOWED", "MIN_NOTIONAL", "MAX_NOTIONAL", "MAX_POSITION", "AGGREGATE_EXPOSURE", "RESERVED_EXPOSURE", "MAX_POSITIONS", "DAILY_REALIZED_LOSS", "DAILY_EQUITY_LOSS", "SUBMISSIONS_PER_DAY")},
    )


# Friendly aliases used by the canary coordinator.
assess = assess_order
assess_risk = assess_order


class BinanceRisk:
    """Stateless facade retaining the envelope as an explicit dependency."""

    def __init__(self, envelope: BinanceRiskEnvelope = DEFAULT_BINANCE_RISK_ENVELOPE) -> None:
        self.envelope = envelope

    def assess(self, rules: SymbolRules, side: str, price: Any, quantity: Any, **kwargs: Any) -> RiskAssessment:
        return assess_order(rules, side, price, quantity, envelope=self.envelope, **kwargs)

    assess_order = assess
RiskEngine = BinanceRisk
# Stable aliases are part of the canary boundary and intentionally kept
# explicit instead of making downstream callers depend on implementation names.
@dataclass(frozen=True, slots=True)
class SizedOrder:
    """A Decimal-only side-correctly rounded LIMIT order candidate."""

    valid: bool
    symbol: str
    side: str
    price: Decimal
    quantity: Decimal
    notional: Decimal
    fee_reserve: Decimal = ZERO
    reasons: tuple[str, ...] = ()

    @property
    def reason(self) -> str:
        return "; ".join(self.reasons)

    @property
    def allowed(self) -> bool:
        return self.valid

    @property
    def skip_reasons(self) -> tuple[str, ...]:
        return self.reasons


BinanceRiskSnapshot = RiskSnapshot
BinanceRiskAssessment = RiskAssessment


def size_limit_order(
    rules: SymbolRules,
    side: str,
    price: Any,
    quantity: Any,
    *,
    envelope: BinanceRiskEnvelope = DEFAULT_BINANCE_RISK_ENVELOPE,
    fee_rate: Any = ZERO,
    fee_bps: Any = None,
    available_inventory: Any = None,
) -> SizedOrder:
    """Round and locally validate an entry/exit LIMIT order.

    This function deliberately does not inspect account or market state.  Use
    :func:`assess_order` for the final pre-submit assessment.
    """
    if not isinstance(rules, SymbolRules):
        rules = SymbolRules.from_exchange_info(rules)  # type: ignore[arg-type]
    side = _key(side)
    fee = _decimal(fee_rate if fee_bps is None else _decimal(fee_bps, name="fee bps") / Decimal("10000"), name="fee rate", nonnegative=True)
    requested_price = _decimal(price, name="price", nonnegative=True)
    available = None if side == "BUY" else (_decimal(available_inventory, name="available inventory", nonnegative=True) if available_inventory is not None else ZERO)
    reasons: list[str] = []
    try:
        rounded_price = rules.round_price(requested_price, side)
    except ValueError as exc:
        rounded_price = requested_price
        reasons.append(str(exc))
    rounded_qty = rules.round_quantity(quantity, side, available=available)
    if side == "BUY" and rounded_price > ZERO:
        rounded_qty = rules.round_quantity(
            min(rounded_qty, envelope.entry_notional / (rounded_price * (ONE + fee))),
            side,
        )
    notional = rounded_price * rounded_qty
    fee_reserve = notional * fee
    if side == "SELL" and rounded_qty <= ZERO:
        reasons.extend(("DUST", "EXIT_BELOW_MINIMUM"))
    if side == "SELL" and available is not None and rounded_qty > available:
        reasons.append("INSUFFICIENT_OWNED_INVENTORY")
    reasons.extend(
        _filter_reason(
            rules,
            side=side,
            price=rounded_price,
            quantity=rounded_qty,
            notional=notional,
            order_type="LIMIT",
            average_price=None,
            max_position_after=None,
        )
    )
    if side == "BUY" and notional + fee_reserve > envelope.entry_notional:
        reasons.append("ENTRY_CAP_EXCEEDED")
    if side == "SELL" and rules.min_notional is not None and rules.min_notional > ZERO and notional < rules.min_notional:
        reasons.append("EXIT_BELOW_MINIMUM")
    reasons = list(dict.fromkeys(reasons))
    return SizedOrder(
        valid=not reasons,
        symbol=rules.symbol,
        side=side,
        price=rounded_price,
        quantity=rounded_qty,
        notional=notional,
        fee_reserve=fee_reserve,
        reasons=tuple(reasons),
    )


def _budget_assessment(
    snapshot: RiskSnapshot | Mapping[str, Any],
    envelope: BinanceRiskEnvelope,
    *,
    requested_notional: Any,
    reserved_fee: Any = ZERO,
    now: datetime | None = None,
    side: str,
) -> RiskAssessment:
    state = snapshot if isinstance(snapshot, RiskSnapshot) else RiskSnapshot.from_account(snapshot, now=now)
    state = state.project(_utc(now or state.observed_at))
    notional = _decimal(requested_notional, name="requested notional", nonnegative=True)
    fee = _decimal(reserved_fee, name="reserved fee", nonnegative=True)
    reasons: list[str] = []
    if side == "BUY":
        if notional + fee > state.quote_available:
            reasons.append("INSUFFICIENT_QUOTE")
        if notional + fee > envelope.entry_notional:
            reasons.append("ENTRY_CAP_EXCEEDED")
        if state.aggregate_exposure + notional > envelope.max_aggregate_exposure:
            reasons.append("AGGREGATE_EXPOSURE")
        if state.unresolved_exposure + notional + fee > envelope.max_reserved_exposure:
            reasons.append("RESERVED_EXPOSURE")
        if state.owned_positions >= envelope.max_positions:
            reasons.append("MAX_POSITIONS")
        if state.realized_loss_today >= envelope.realized_loss_entry_stop:
            reasons.append("DAILY_REALIZED_LOSS")
        if state.equity_loss_today + max(ZERO, -state.unrealized_pnl) + state.fees_today >= envelope.equity_loss_entry_stop:
            reasons.append("DAILY_EQUITY_LOSS")
        if state.entry_submissions_today + 1 > envelope.max_submissions_per_day:
            reasons.append("SUBMISSIONS_PER_DAY")
    else:
        if state.exit_submissions_today + 1 > envelope.max_submissions_per_day:
            reasons.append("SUBMISSIONS_PER_DAY")
    reasons.extend(("ACCOUNT_PAUSED",) if state.account_paused else ())
    reasons.extend(("RATE_LIMIT",) if state.rate_limited else ())
    reasons = list(dict.fromkeys(reasons))
    return RiskAssessment(
        allowed=not reasons,
        reasons=tuple(reasons),
        side=side,
        notional=notional,
        fee_reserve=fee,
        projected_exposure=state.aggregate_exposure + (notional if side == "BUY" else -notional),
        projected_reserved=state.unresolved_exposure + (notional + fee if side == "BUY" else ZERO),
    )


def assess_entry(
    snapshot: RiskSnapshot | Mapping[str, Any],
    envelope: BinanceRiskEnvelope = DEFAULT_BINANCE_RISK_ENVELOPE,
    requested_notional: Any = ZERO,
    reserved_fee: Any = ZERO,
    *,
    now: datetime | None = None,
) -> RiskAssessment:
    """Assess account-wide entry budgets without creating an order."""
    return _budget_assessment(
        snapshot,
        envelope,
        requested_notional=requested_notional,
        reserved_fee=reserved_fee,
        now=now,
        side="BUY",
    )


def assess_exit(
    snapshot: RiskSnapshot | Mapping[str, Any],
    envelope: BinanceRiskEnvelope = DEFAULT_BINANCE_RISK_ENVELOPE,
    requested_notional: Any = ZERO,
    reserved_fee: Any = ZERO,
    *,
    now: datetime | None = None,
    symbol: str | None = None,
    quantity: Any | None = None,
    minimum_notional: Any | None = None,
) -> RiskAssessment:
    """Assess an exit; unlike an entry, proceeds are never capped at 10 USDT."""
    state = snapshot if isinstance(snapshot, RiskSnapshot) else RiskSnapshot.from_account(snapshot, now=now)
    state = state.project(_utc(now or state.observed_at))
    reasons: list[str] = []
    if symbol is not None:
        name = symbol.upper()
        if name not in state.owned_inventory:
            reasons.append("FOREIGN_INVENTORY")
        elif quantity is not None and _decimal(quantity, name="exit quantity", nonnegative=True) > state.owned_inventory[name]:
            reasons.append("INSUFFICIENT_OWNED_INVENTORY")
    notional = _decimal(requested_notional, name="requested notional", nonnegative=True)
    if notional <= ZERO:
        reasons.extend(("DUST", "EXIT_BELOW_MINIMUM"))
    if minimum_notional is not None and notional < _decimal(minimum_notional, name="minimum notional", nonnegative=True):
        reasons.append("EXIT_BELOW_MINIMUM")
    budget = _budget_assessment(
        state,
        envelope,
        requested_notional=notional,
        reserved_fee=reserved_fee,
        now=now,
        side="SELL",
    )
    reasons.extend(budget.reasons)
    reasons = list(dict.fromkeys(reasons))
    return RiskAssessment(
        allowed=not reasons,
        reasons=tuple(reasons),
        symbol=(symbol or "").upper(),
        side="SELL",
        quantity=_decimal(quantity, name="exit quantity", nonnegative=True) if quantity is not None else ZERO,
        notional=notional,
        fee_reserve=_decimal(reserved_fee, name="reserved fee", nonnegative=True),
        projected_exposure=budget.projected_exposure,
        projected_reserved=budget.projected_reserved,
    )


__all__ = [
    "BINANCE_RISK_ENVELOPE_VERSION", "DEFAULT_BINANCE_RISK_ENVELOPE", "QUOTE_ASSET",
    "BinanceRiskEnvelope", "SymbolRules", "PendingOrder", "RiskSnapshot",
    "BinanceRiskSnapshot", "RiskAssessment", "BinanceRiskAssessment", "SizedOrder",
    "BinanceRisk", "RiskEngine", "assess_order", "assess", "assess_risk",
    "size_limit_order", "assess_entry", "assess_exit",
]
