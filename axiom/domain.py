"""Canonical, serializable domain objects shared by all Axiom subsystems."""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
import math
from typing import Any, Mapping, Sequence


class MarketType(str, Enum):
    CRYPTO_SPOT = "crypto_spot"
    PREDICTION = "prediction"


class Side(str, Enum):
    BUY = "buy"
    SELL = "sell"


class OrderType(str, Enum):
    MARKET = "market"
    LIMIT = "limit"


class SettlementState(str, Enum):
    OPEN = "open"
    RESOLVED_YES = "resolved_yes"
    RESOLVED_NO = "resolved_no"
    VOID = "void"
    UNKNOWN = "unknown"


class SimulationQuality(str, Enum):
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


class ResearchQuality(str, Enum):
    """Evidence class for prediction-market research workflows."""

    PRICE_PROXY = "PRICE_PROXY"
    ORDER_BOOK_SIMULATED = "ORDER_BOOK_SIMULATED"
    PAPER_FORWARD = "PAPER_FORWARD"

@dataclass(frozen=True, slots=True)
class InstrumentMetadata:
    symbol: str
    market_type: MarketType
    provider: str
    base_asset: str | None = None
    quote_asset: str | None = None
    tick_size: float | None = None
    lot_size: float | None = None
    contract_size: float = 1.0
    currency: str = "USD"
    market_id: str | None = None
    question: str | None = None
    resolution_criteria: str | None = None
    category: str | None = None
    tags: tuple[str, ...] = ()
    expiry: datetime | None = None
    extra: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not str(self.symbol).strip() or not str(self.provider).strip():
            raise ValueError("instrument symbol and provider are required")
        if not isinstance(self.market_type, MarketType):
            raise TypeError("market_type must be a MarketType")
        for name in ("tick_size", "lot_size"):
            value = getattr(self, name)
            if value is not None and (not math.isfinite(float(value)) or float(value) <= 0):
                raise ValueError(f"{name} must be finite and positive")
        if not math.isfinite(float(self.contract_size)) or self.contract_size <= 0:
            raise ValueError("contract_size must be finite and positive")
        if self.expiry is not None:
            object.__setattr__(self, "expiry", ensure_utc(self.expiry))
        object.__setattr__(self, "tags", tuple(str(tag) for tag in self.tags))


@dataclass(frozen=True, slots=True)
class OHLCVBar:
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    spread: float | None = None
    trades: int | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "timestamp", ensure_utc(self.timestamp))
        values = tuple(float(getattr(self, name)) for name in ("open", "high", "low", "close", "volume"))
        if not all(math.isfinite(value) for value in values):
            raise ValueError("OHLCV values must be finite")
        opening, high, low, close, volume = values
        if min(opening, high, low, close) <= 0 or volume < 0 or high < max(opening, close, low) or low > min(opening, close, high):
            raise ValueError("OHLCV bar values are inconsistent")
        for name, value in zip(("open", "high", "low", "close", "volume"), values):
            object.__setattr__(self, name, value)
        if self.spread is not None:
            spread = float(self.spread)
            if not math.isfinite(spread) or spread < 0:
                raise ValueError("OHLCV spread must be finite and non-negative")
            object.__setattr__(self, "spread", spread)
        if self.trades is not None:
            trades = int(self.trades)
            if trades < 0:
                raise ValueError("OHLCV trades must be non-negative")
            object.__setattr__(self, "trades", trades)


@dataclass(frozen=True, slots=True)
class TradePrint:
    timestamp: datetime
    price: float
    size: float
    side: Side | None = None
    trade_id: str | None = None
    market_id: str | None = None
    token_id: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "timestamp", ensure_utc(self.timestamp))
        price, size = float(self.price), float(self.size)
        if not all(math.isfinite(value) and value > 0 for value in (price, size)):
            raise ValueError("trade price and size must be finite and positive")
        if self.side is not None and not isinstance(self.side, Side):
            object.__setattr__(self, "side", Side(str(self.side)))
        object.__setattr__(self, "price", price)
        object.__setattr__(self, "size", size)
        for name in ("trade_id", "market_id", "token_id"):
            value = getattr(self, name)
            if value is not None:
                normalized = str(value).strip()
                if not normalized:
                    raise ValueError(f"{name} must be non-empty when provided")
                object.__setattr__(self, name, normalized)


@dataclass(frozen=True, slots=True)
class OrderBookLevel:
    price: float
    size: float

    def __post_init__(self) -> None:
        price, size = float(self.price), float(self.size)
        if not all(math.isfinite(value) for value in (price, size)) or price < 0 or size <= 0:
            raise ValueError("order-book levels require finite non-negative price and positive size")
        object.__setattr__(self, "price", price)
        object.__setattr__(self, "size", size)

@dataclass(frozen=True, slots=True)
class OrderBookSnapshot:
    timestamp: datetime
    bids: tuple[OrderBookLevel, ...]
    asks: tuple[OrderBookLevel, ...]
    token_id: str | None = None
    def __post_init__(self) -> None:
        bids, asks = tuple(self.bids), tuple(self.asks)
        if not all(isinstance(level, OrderBookLevel) for level in (*bids, *asks)):
            raise TypeError("order-book sides must contain OrderBookLevel values")
        object.__setattr__(self, "timestamp", ensure_utc(self.timestamp))
        bids = tuple(sorted(bids, key=lambda level: level.price, reverse=True))
        asks = tuple(sorted(asks, key=lambda level: level.price))
        if bids and asks and bids[0].price > asks[0].price:
            raise ValueError("order-book bid cannot exceed ask")
        if self.token_id is not None:
            token_id = str(self.token_id).strip()
            if not token_id:
                raise ValueError("token_id must be non-empty when provided")
            object.__setattr__(self, "token_id", token_id)
        object.__setattr__(self, "bids", bids)
        object.__setattr__(self, "asks", asks)


    @property
    def best_bid(self) -> float | None:
        return self.bids[0].price if self.bids else None

    @property
    def best_ask(self) -> float | None:
        return self.asks[0].price if self.asks else None

    @property
    def midpoint(self) -> float | None:
        if self.best_bid is None or self.best_ask is None:
            return None
        return (self.best_bid + self.best_ask) / 2.0

    def executable_price(
        self,
        side: Side,
        quantity: float,
        *,
        limit_price: float | None = None,
        price_multiplier: float = 1.0,
    ) -> tuple[float, float]:
        """Return raw VWAP and filled quantity by walking displayed depth.

        ``limit_price`` is checked against the post-slippage level price when
        ``price_multiplier`` is supplied. A limit order therefore cannot fill
        from a level that is only acceptable after averaging across worse
        levels or before adverse slippage.
        """
        if not isinstance(side, Side):
            side = Side(str(side))
        quantity = float(quantity)
        if not math.isfinite(quantity) or quantity < 0:
            raise ValueError("order-book quantity must be finite and non-negative")
        price_multiplier = float(price_multiplier)
        if not math.isfinite(price_multiplier) or price_multiplier <= 0:
            raise ValueError("price_multiplier must be finite and positive")
        if limit_price is not None:
            limit_price = float(limit_price)
            if not math.isfinite(limit_price) or limit_price < 0:
                raise ValueError("limit_price must be finite and non-negative")
        levels: Sequence[OrderBookLevel] = self.asks if side is Side.BUY else self.bids
        remaining = quantity
        notional = 0.0
        filled = 0.0
        for level in levels:
            adjusted_price = level.price * price_multiplier
            if limit_price is not None:
                if side is Side.BUY and adjusted_price > limit_price + 1e-12:
                    break
                if side is Side.SELL and adjusted_price < limit_price - 1e-12:
                    break
            take = min(remaining, level.size)
            notional += take * level.price
            filled += take
            remaining -= take
            if remaining <= 1e-12:
                break
        return (notional / filled if filled else 0.0, filled)


@dataclass(frozen=True, slots=True)
class CryptoTicker:
    timestamp: datetime
    symbol: str
    last: float
    bid: float | None = None
    ask: float | None = None
    volume_24h: float | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "timestamp", ensure_utc(self.timestamp))
        if not str(self.symbol).strip():
            raise ValueError("ticker symbol is required")
        last = float(self.last)
        if not math.isfinite(last) or last <= 0:
            raise ValueError("ticker last price must be finite and positive")
        object.__setattr__(self, "last", last)
        for name in ("bid", "ask", "volume_24h"):
            value = getattr(self, name)
            if value is not None:
                number = float(value)
                if not math.isfinite(number) or number < 0 or (name != "volume_24h" and number <= 0):
                    raise ValueError(f"ticker {name} must be finite and non-negative")
                object.__setattr__(self, name, number)
        if self.bid is not None and self.ask is not None and self.bid > self.ask:
            raise ValueError("ticker bid cannot exceed ask")

    @property
    def midpoint(self) -> float:
        if self.bid is not None and self.ask is not None:
            return (self.bid + self.ask) / 2.0
        return self.last


@dataclass(frozen=True, slots=True)
class PredictionMarketSnapshot:
    timestamp: datetime
    market_id: str
    question: str
    yes_bid: float | None
    yes_ask: float | None
    yes_mid: float | None
    no_bid: float | None = None
    no_ask: float | None = None
    no_mid: float | None = None
    volume: float | None = None
    liquidity: float | None = None
    expiry: datetime | None = None
    settlement: SettlementState = SettlementState.OPEN
    resolution_criteria: str = ""
    category: str | None = None
    tags: tuple[str, ...] = ()
    order_book: OrderBookSnapshot | None = None
    source: str = ""
    yes_token_id: str | None = None
    no_token_id: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "timestamp", ensure_utc(self.timestamp))
        if not str(self.market_id).strip() or not str(self.question).strip():
            raise ValueError("prediction market id and question are required")
        for name in ("yes_bid", "yes_ask", "yes_mid", "no_bid", "no_ask", "no_mid"):
            value = getattr(self, name)
            if value is not None:
                number = float(value)
                if not math.isfinite(number) or not 0.0 <= number <= 1.0:
                    raise ValueError(f"{name} must be a probability in [0, 1]")
                object.__setattr__(self, name, number)
        if self.yes_bid is not None and self.yes_ask is not None and self.yes_bid > self.yes_ask:
            raise ValueError("yes bid cannot exceed ask")
        if self.no_bid is not None and self.no_ask is not None and self.no_bid > self.no_ask:
            raise ValueError("no bid cannot exceed ask")
        for name in ("volume", "liquidity"):
            value = getattr(self, name)
            if value is not None:
                number = float(value)
                if not math.isfinite(number) or number < 0:
                    raise ValueError(f"{name} must be finite and non-negative")
                object.__setattr__(self, name, number)
        if self.expiry is not None:
            object.__setattr__(self, "expiry", ensure_utc(self.expiry))
        if not isinstance(self.settlement, SettlementState):
            try:
                object.__setattr__(self, "settlement", SettlementState(str(self.settlement)))
            except ValueError:
                object.__setattr__(self, "settlement", SettlementState.UNKNOWN)
        object.__setattr__(self, "tags", tuple(str(tag) for tag in self.tags))
        if self.order_book is not None and not isinstance(self.order_book, OrderBookSnapshot):
            raise TypeError("order_book must be an OrderBookSnapshot")
        for name in ("yes_token_id", "no_token_id"):
            value = getattr(self, name)
            if value is not None:
                normalized = str(value).strip()
                if not normalized:
                    raise ValueError(f"{name} must be non-empty when provided")
                object.__setattr__(self, name, normalized)

    @property
    def yes_spread(self) -> float | None:
        if self.yes_bid is None or self.yes_ask is None:
            return None
        return self.yes_ask - self.yes_bid

    @property
    def no_spread(self) -> float | None:
        if self.no_bid is None or self.no_ask is None:
            return None
        return self.no_ask - self.no_bid

    @property
    def time_to_expiry_seconds(self) -> float | None:
        if self.expiry is None:
            return None
        return (self.expiry - self.timestamp).total_seconds()

    def executable_price(self, side: Side, quantity: float = 1.0) -> tuple[float, float]:
        if not isinstance(side, Side):
            side = Side(str(side))
        if self.order_book is not None:
            return self.order_book.executable_price(side, quantity)
        price = self.yes_ask if side is Side.BUY else self.yes_bid
        if price is None:
            return 0.0, 0.0
        quantity = float(quantity)
        if not math.isfinite(quantity) or quantity < 0:
            raise ValueError("quantity must be finite and non-negative")
        return price, quantity


@dataclass(frozen=True, slots=True)
class Fill:
    timestamp: datetime
    market_type: MarketType
    symbol: str
    side: Side
    quantity: float
    price: float
    fees: float
    slippage: float
    strategy_id: str
    order_id: str
    market_id: str | None = None
    expected_probability: float | None = None
    executable_probability: float | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "timestamp", ensure_utc(self.timestamp))
        if not isinstance(self.market_type, MarketType):
            object.__setattr__(self, "market_type", MarketType(str(self.market_type)))
        if not isinstance(self.side, Side):
            object.__setattr__(self, "side", Side(str(self.side)))
        if not str(self.symbol).strip() or not str(self.order_id).strip():
            raise ValueError("fill symbol and order_id are required")
        values = tuple(float(getattr(self, name)) for name in ("quantity", "price", "fees", "slippage"))
        if not math.isfinite(values[0]) or values[0] <= 0:
            raise ValueError("fill quantity must be finite and positive")
        if not math.isfinite(values[1]) or values[1] <= 0:
            raise ValueError("fill price must be finite and positive")
        if not all(math.isfinite(value) and value >= 0 for value in values[2:]):
            raise ValueError("fill fees and slippage must be finite and non-negative")
        for name, value in zip(("quantity", "price", "fees", "slippage"), values):
            object.__setattr__(self, name, value)
        for name in ("expected_probability", "executable_probability"):
            value = getattr(self, name)
            if value is not None:
                number = float(value)
                if not math.isfinite(number) or not 0.0 <= number <= 1.0:
                    raise ValueError(f"{name} must be a probability in [0, 1]")
                object.__setattr__(self, name, number)
        if not isinstance(self.metadata, Mapping):
            raise TypeError("fill metadata must be a mapping")


@dataclass(frozen=True, slots=True)
class ResolvedContract:
    market_id: str
    outcome: SettlementState
    resolved_at: datetime
    resolution_criteria: str

    def __post_init__(self) -> None:
        if not str(self.market_id).strip():
            raise ValueError("resolved contract market_id is required")
        if not isinstance(self.outcome, SettlementState):
            object.__setattr__(self, "outcome", SettlementState(str(self.outcome)))
        if self.outcome not in {
            SettlementState.RESOLVED_YES,
            SettlementState.RESOLVED_NO,
            SettlementState.VOID,
            SettlementState.UNKNOWN,
        }:
            raise ValueError("resolved contract outcome must be terminal")
        object.__setattr__(self, "resolved_at", ensure_utc(self.resolved_at))
        object.__setattr__(self, "resolution_criteria", str(self.resolution_criteria))
def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
def parse_timestamp(value: Any) -> datetime | None:
    """Parse common UTC timestamps without silently accepting invalid values."""
    if isinstance(value, datetime):
        return ensure_utc(value)
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        number = float(value)
        if not math.isfinite(number):
            return None
        if abs(number) >= 100_000_000_000:
            number /= 1000.0
        try:
            return datetime.fromtimestamp(number, tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            return ensure_utc(datetime.fromisoformat(text))
        except ValueError:
            try:
                return parse_timestamp(float(text))
            except ValueError:
                return None
    return None


def to_record(value: Any) -> dict[str, Any]:
    """Convert a domain dataclass to JSON-friendly primitive values."""
    def convert(item: Any) -> Any:
        if isinstance(item, Enum):
            return item.value
        if isinstance(item, datetime):
            return ensure_utc(item).isoformat()
        if isinstance(item, tuple):
            return [convert(v) for v in item]
        if isinstance(item, list):
            return [convert(v) for v in item]
        if isinstance(item, Mapping):
            return {str(k): convert(v) for k, v in item.items()}
        if hasattr(item, "__dataclass_fields__"):
            return {name: convert(getattr(item, name)) for name in item.__dataclass_fields__}
        return item

    result = convert(value)
    if not isinstance(result, dict):
        raise TypeError("to_record requires a dataclass-like object")
    return result
