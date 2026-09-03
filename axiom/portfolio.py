"""Deterministic portfolio accounting for simulated crypto and binary markets."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
import math
from typing import Any, Mapping
from uuid import uuid4

from .domain import (
    Fill, MarketType, OrderBookSnapshot, OrderType, PredictionMarketSnapshot,
    ResolvedContract, Side, SettlementState, ensure_utc, utc_now,
)


@dataclass(slots=True)
class Position:
    symbol: str
    market_type: MarketType
    quantity: float = 0.0
    average_price: float = 0.0
    realized_pnl: float = 0.0
    fees: float = 0.0
    market_id: str | None = None
    outcome: str | None = None
    last_price: float | None = None

    @property
    def avg_price(self) -> float:
        return self.average_price

    @property
    def notional(self) -> float:
        return abs(self.quantity * (self.last_price if self.last_price is not None else self.average_price))

    @property
    def unrealized_pnl(self) -> float:
        if self.last_price is None or not self.quantity:
            return 0.0
        return (self.last_price - self.average_price) * self.quantity

    def mark(self, price: float) -> float:
        self.last_price = float(price)
        return self.unrealized_pnl


@dataclass(frozen=True, slots=True)
class OrderRequest:
    symbol: str
    side: Side
    quantity: float
    market_type: MarketType = MarketType.CRYPTO_SPOT
    order_type: OrderType = OrderType.MARKET
    limit_price: float | None = None
    strategy_id: str = ""
    market_id: str | None = None
    outcome: str | None = None
    expected_probability: float | None = None

    def __post_init__(self) -> None:
        if not str(self.symbol).strip():
            raise ValueError("order symbol is required")
        if not isinstance(self.side, Side):
            object.__setattr__(self, "side", Side(str(self.side)))
        if not isinstance(self.market_type, MarketType):
            object.__setattr__(self, "market_type", MarketType(str(self.market_type)))
        if not isinstance(self.order_type, OrderType):
            object.__setattr__(self, "order_type", OrderType(str(self.order_type)))
        quantity = float(self.quantity)
        if not math.isfinite(quantity) or quantity <= 0:
            raise ValueError("order quantity must be finite and positive")
        object.__setattr__(self, "quantity", quantity)
        if self.limit_price is not None:
            limit_price = float(self.limit_price)
            if not math.isfinite(limit_price) or limit_price <= 0:
                raise ValueError("limit_price must be finite and positive")
            object.__setattr__(self, "limit_price", limit_price)
        if self.order_type is OrderType.LIMIT and self.limit_price is None:
            raise ValueError("limit orders require limit_price")
        if self.outcome is not None:
            outcome = str(self.outcome).strip().lower()
            if self.market_type is MarketType.PREDICTION and outcome not in {"yes", "no"}:
                raise ValueError("prediction outcome must be yes or no")
            object.__setattr__(self, "outcome", outcome)
        if self.expected_probability is not None:
            probability = float(self.expected_probability)
            if not math.isfinite(probability) or not 0.0 <= probability <= 1.0:
                raise ValueError("expected_probability must be in [0, 1]")
            object.__setattr__(self, "expected_probability", probability)


@dataclass(slots=True)
class OrderState:
    request: OrderRequest
    order_id: str
    status: str = "open"
    requested_quantity: float = 0.0
    filled_quantity: float = 0.0
    fills: list[Fill] = field(default_factory=list)

    @property
    def remaining_quantity(self) -> float:
        return max(0.0, self.requested_quantity - self.filled_quantity)


class Portfolio:
    """Cash, positions, fills and settlement ledger.

    All trades are simulated. Cash is debited/credited at the executable fill
    price, including fees; no network or broker operation is performed.
    """

    def __init__(self, initial_cash: float = 0.0, *, cash: float | None = None, currency: str = "USD") -> None:
        opening = initial_cash if cash is None else cash
        if not math.isfinite(float(opening)) or opening < 0:
            raise ValueError("initial cash must be finite and non-negative")
        self.initial_cash = float(opening)
        self.cash = float(opening)
        self.currency = currency
        self.positions: dict[str, Position] = {}
        self.fills: list[Fill] = []
        self.orders: dict[str, OrderState] = {}
        self.total_fees = 0.0
        self.total_slippage = 0.0
        self._settled_markets: set[str] = set()
    @staticmethod
    def _position_key(symbol: str, market_type: MarketType, outcome: str | None) -> str:
        if market_type is MarketType.PREDICTION:
            normalized = (outcome or "yes").lower()
            if normalized not in {"yes", "no"}:
                raise ValueError("prediction position outcome must be yes or no")
            return f"{symbol}|{normalized}"
        return symbol

    def get_position(self, symbol: str, *, outcome: str | None = None) -> Position | None:
        if outcome is not None:
            return self.positions.get(self._position_key(symbol, MarketType.PREDICTION, outcome))
        direct = self.positions.get(symbol)
        if direct is not None:
            return direct
        prefix = f"{symbol}|"
        return next((position for key, position in self.positions.items() if key.startswith(prefix)), None)

    def submit_order(self, request: OrderRequest, *, order_id: str | None = None) -> OrderState:
        if request.quantity <= 0 or not math.isfinite(request.quantity):
            raise ValueError("order quantity must be positive and finite")
        if order_id is not None and order_id in self.orders:
            existing = self.orders[order_id]
            if existing.request != request:
                raise ValueError("order_id is already bound to a different request")
            if existing.status == "cancelled":
                raise ValueError("cannot reopen a cancelled order")
            return existing
        order = OrderState(request=request, order_id=order_id or uuid4().hex, requested_quantity=float(request.quantity))
        self.orders[order.order_id] = order
        return order

    def cancel_order(self, order_id: str) -> bool:
        order = self.orders.get(order_id)
        if order is None or order.status in {"filled", "cancelled"}:
            return False
        order.status = "cancelled"
        return True

    def apply_fill(self, fill: Fill) -> Position:
        """Apply one (possibly partial) fill and return its resulting position."""
        if fill.quantity <= 0 or not math.isfinite(fill.quantity):
            raise ValueError("fill quantity must be positive and finite")
        if fill.price <= 0 or not math.isfinite(fill.price):
            raise ValueError("fill price must be finite and positive")
        if (
            not math.isfinite(fill.fees)
            or fill.fees < 0
            or not math.isfinite(fill.slippage)
            or fill.slippage < 0
        ):
            raise ValueError("fill fees and slippage must be finite and non-negative")
        outcome = str(fill.metadata.get("outcome", "")).lower() or None
        key = self._position_key(fill.symbol, fill.market_type, outcome)
        position = self.positions.get(key)
        if position is None:
            position = Position(fill.symbol, fill.market_type, market_id=fill.market_id, outcome=outcome)
        if fill.side is Side.SELL and fill.quantity > max(0.0, position.quantity) + 1e-12:
            raise ValueError("cannot sell more than the available position")
        if fill.side is Side.BUY and self.cash + 1e-9 < fill.quantity * fill.price + fill.fees:
            raise ValueError("insufficient cash for fill")
        self.positions[key] = position
        signed = fill.quantity if fill.side is Side.BUY else -fill.quantity
        old_quantity = position.quantity
        if old_quantity and (old_quantity > 0) != (signed > 0):
            closed = min(abs(old_quantity), abs(signed))
            position.realized_pnl += (fill.price - position.average_price) * closed * (1.0 if old_quantity > 0 else -1.0)
            if abs(signed) > closed:
                position.average_price = fill.price
        elif old_quantity == 0:
            position.average_price = fill.price
        else:
            total = abs(old_quantity) + abs(signed)
            if total:
                position.average_price = (abs(old_quantity) * position.average_price + abs(signed) * fill.price) / total
        position.quantity += signed
        if abs(position.quantity) <= 1e-12:
            position.quantity = 0.0
        position.market_id = fill.market_id or position.market_id
        position.outcome = outcome or position.outcome
        position.fees += fill.fees
        self.cash -= signed * fill.price + fill.fees
        self.total_fees += fill.fees
        self.total_slippage += abs(fill.slippage * fill.quantity)
        position.last_price = fill.price
        self.fills.append(fill)
        order = self.orders.get(fill.order_id)
        if order is not None:
            order.fills.append(fill)
            order.filled_quantity += fill.quantity
            order.status = "filled" if order.remaining_quantity <= 1e-12 else "partially_filled"
        return position

    process_fill = apply_fill
    record_fill = apply_fill

    def execute_order(
        self, request: OrderRequest, *, timestamp: datetime | None = None,
        price: float | None = None, order_book: OrderBookSnapshot | None = None,
        fee_bps: float = 0.0, slippage_bps: float = 0.0,
        available_quantity: float | None = None, order_id: str | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> Fill | None:
        """Simulate an executable order, returning a partial fill when depth is limited."""
        if (
            not math.isfinite(float(fee_bps))
            or not math.isfinite(float(slippage_bps))
            or fee_bps < 0
            or slippage_bps < 0
        ):
            raise ValueError("fee_bps and slippage_bps must be finite and non-negative")
        if available_quantity is not None and (
            not math.isfinite(float(available_quantity)) or available_quantity < 0
        ):
            raise ValueError("available_quantity must be finite and non-negative")
        existing_order = order_id is not None and order_id in self.orders
        order = self.submit_order(request, order_id=order_id)
        requested_quantity = order.remaining_quantity if existing_order else request.quantity
        quantity = min(requested_quantity, available_quantity) if available_quantity is not None else requested_quantity
        if request.side is Side.SELL:
            position = self.get_position(
                request.symbol,
                outcome=request.outcome if request.market_type is MarketType.PREDICTION else None,
            )
            quantity = min(quantity, max(0.0, position.quantity if position is not None else 0.0))
        if quantity <= 0:
            if order.remaining_quantity > 1e-12:
                order.status = "cancelled"
            return None
        filled = quantity
        slippage_factor = 1.0 + (slippage_bps / 10_000.0) * (1 if request.side is Side.BUY else -1)
        limit_price = request.limit_price if request.order_type is OrderType.LIMIT else None
        if order_book is not None:
            base_price, filled = order_book.executable_price(
                request.side,
                quantity,
                limit_price=limit_price,
                price_multiplier=slippage_factor,
            )
        elif price is not None:
            base_price = float(price)
        else:
            order.status = "cancelled"
            return None
        if (
            filled <= 0
            or not math.isfinite(filled)
            or base_price <= 0
            or not math.isfinite(base_price)
        ):
            order.status = "cancelled"
            return None
        executed_price = base_price * slippage_factor
        if (
            not math.isfinite(executed_price)
            or executed_price <= 0
            or request.market_type is MarketType.PREDICTION
            and executed_price > 1.0 + 1e-12
        ):
            order.status = "cancelled"
            return None
        if limit_price is not None:
            violates_limit = (
                request.side is Side.BUY and executed_price > limit_price + 1e-12
            ) or (
                request.side is Side.SELL and executed_price < limit_price - 1e-12
            )
            if violates_limit:
                order.status = "cancelled"
                return None
        if request.side is Side.BUY:
            affordable = self.cash / (executed_price * (1.0 + fee_bps / 10_000.0)) if executed_price > 0 else 0.0
            if filled > affordable:
                if order_book is not None:
                    base_price, filled = order_book.executable_price(
                        request.side,
                        affordable,
                        limit_price=limit_price,
                        price_multiplier=slippage_factor,
                    )
                else:
                    filled = affordable
                if filled <= 0:
                    order.status = "cancelled"
                    return None
                executed_price = base_price * slippage_factor
                if (
                    not math.isfinite(executed_price)
                    or executed_price <= 0
                    or request.market_type is MarketType.PREDICTION
                    and executed_price > 1.0 + 1e-12
                ):
                    order.status = "cancelled"
                    return None
                if limit_price is not None and executed_price > limit_price + 1e-12:
                    order.status = "cancelled"
                    return None
                affordable = self.cash / (executed_price * (1.0 + fee_bps / 10_000.0)) if executed_price > 0 else 0.0
                filled = min(filled, affordable)
                if filled <= 1e-12:
                    order.status = "cancelled"
                    return None
        extra = dict(metadata or {})
        extra.setdefault("reference_price", base_price)
        if order_book is not None:
            best = order_book.best_ask if request.side is Side.BUY else order_book.best_bid
            if best is not None and best > 0:
                impact = (base_price / best - 1.0) if request.side is Side.BUY else (best / base_price - 1.0)
                extra.setdefault("price_impact_bps", max(0.0, impact) * 10_000.0)
            extra.setdefault("book_levels", len(order_book.asks if request.side is Side.BUY else order_book.bids))
        if request.outcome:
            extra.setdefault("outcome", request.outcome)
        fill = Fill(
            timestamp=ensure_utc(timestamp or utc_now()), market_type=request.market_type,
            symbol=request.symbol, side=request.side, quantity=filled, price=executed_price,
            fees=abs(executed_price * filled) * max(0.0, fee_bps) / 10_000.0,
            slippage=abs(executed_price - base_price), strategy_id=request.strategy_id,
            order_id=order.order_id, market_id=request.market_id,
            expected_probability=request.expected_probability,
            executable_probability=executed_price if request.market_type is MarketType.PREDICTION else None,
            metadata=extra,
        )
        self.apply_fill(fill)
        return fill

    def mark(self, prices: Mapping[str, float] | Mapping[str, PredictionMarketSnapshot]) -> float:
        for key, position in self.positions.items():
            if position.quantity == 0:
                continue
            value = prices.get(key, prices.get(position.symbol))
            if isinstance(value, PredictionMarketSnapshot):
                value = value.no_mid if (position.outcome or "yes").lower() == "no" else value.yes_mid
            if value is not None:
                position.mark(float(value))
        return self.equity()

    def resolve(self, contract: ResolvedContract) -> float:
        """Settle prediction positions once at binary payout or void refund."""
        if contract.market_id in self._settled_markets:
            return 0.0
        if contract.outcome is SettlementState.UNKNOWN:
            return 0.0
        if contract.outcome is SettlementState.VOID:
            refund_total = 0.0
            for position in self.positions.values():
                if position.market_type is not MarketType.PREDICTION or (position.market_id or position.symbol) != contract.market_id:
                    continue
                refund = position.quantity * position.average_price
                self.cash += refund
                refund_total += refund
                position.quantity = 0.0
                position.last_price = 0.0
            self._settled_markets.add(contract.market_id)
            return refund_total
        winning = "yes" if contract.outcome is SettlementState.RESOLVED_YES else "no" if contract.outcome is SettlementState.RESOLVED_NO else None
        if winning is None:
            return 0.0
        payout_total = 0.0
        for position in self.positions.values():
            if position.market_type is not MarketType.PREDICTION or (position.market_id or position.symbol) != contract.market_id:
                continue
            payout = position.quantity * (1.0 if (position.outcome or "yes").lower() == winning else 0.0)
            position.realized_pnl += payout - position.quantity * position.average_price
            self.cash += payout
            payout_total += payout
            position.quantity = 0.0
            position.last_price = 0.0
        self._settled_markets.add(contract.market_id)
        return payout_total

    settle = resolve

    def gross_exposure(self, prices: Mapping[str, float] | Mapping[str, PredictionMarketSnapshot] | None = None) -> float:
        total = 0.0
        for key, position in self.positions.items():
            value = position.last_price
            if prices is not None:
                raw = prices.get(key)
                if raw is None:
                    raw = prices.get(position.symbol)
                if isinstance(raw, PredictionMarketSnapshot):
                    value = raw.no_mid if (position.outcome or "yes").lower() == "no" else raw.yes_mid
                elif isinstance(raw, (int, float)):
                    value = float(raw)
            total += abs(position.quantity * (value if value is not None else position.average_price))
        return total

    def unrealized_pnl(self) -> float:
        return sum(position.unrealized_pnl for position in self.positions.values())

    def realized_pnl(self) -> float:
        return sum(position.realized_pnl for position in self.positions.values())

    def equity(self, prices: Mapping[str, float] | Mapping[str, PredictionMarketSnapshot] | None = None) -> float:
        if prices is not None:
            self.mark(prices)
        return self.cash + sum(position.quantity * (position.last_price if position.last_price is not None else position.average_price) for position in self.positions.values())

    def snapshot(self, prices: Mapping[str, float] | None = None) -> dict[str, Any]:
        return {
            "cash": self.cash, "equity": self.equity(prices), "realized_pnl": self.realized_pnl(),
            "unrealized_pnl": self.unrealized_pnl(), "gross_exposure": self.gross_exposure(prices),
            "fees": self.total_fees, "slippage": self.total_slippage,
            "positions": {
                key: {"symbol": position.symbol, "quantity": position.quantity, "average_price": position.average_price,
                      "realized_pnl": position.realized_pnl, "unrealized_pnl": position.unrealized_pnl,
                      "market_type": position.market_type.value, "outcome": position.outcome}
                for key, position in self.positions.items() if position.quantity or position.realized_pnl
            },
        }


PortfolioAccounting = Portfolio
Ledger = Portfolio


__all__ = ["Ledger", "OrderRequest", "OrderState", "Portfolio", "PortfolioAccounting", "Position"]
