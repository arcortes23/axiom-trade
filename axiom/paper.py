"""Deterministic paper-trading orchestration for both Axiom markets.

Only read-only provider interfaces are consumed.  There is intentionally no
live execution adapter: setting ``live=True`` raises immediately rather than
silently routing an order to a venue.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
import hashlib
import inspect
import math
from typing import Any, Iterable, Mapping, Sequence

from .domain import (
    CryptoTicker,
    Fill,
    MarketType,
    OrderBookLevel,
    OrderBookSnapshot,
    PredictionMarketSnapshot,
    ResolvedContract,
    SettlementState,
    Side,
    SimulationQuality,
    ensure_utc,
    parse_timestamp,
    utc_now,
)




def _finite_number(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None
class LiveExecutionDisabled(RuntimeError):
    """Raised for every attempt to enable real-money execution."""
def _complement_book(book: OrderBookSnapshot) -> OrderBookSnapshot:
    """Convert a YES-token book into the complementary NO-token book."""
    bids = tuple(OrderBookLevel(1.0 - level.price, level.size) for level in book.asks)
    asks = tuple(OrderBookLevel(1.0 - level.price, level.size) for level in book.bids)
    return OrderBookSnapshot(book.timestamp, bids=bids, asks=asks)




@dataclass(frozen=True, slots=True)
class PaperTradingConfig:
    fee_rate: float = 0.0
    slippage_bps: float = 0.0
    depth: int = 20
    quality: SimulationQuality = SimulationQuality.MEDIUM
    live: bool = False

    def __post_init__(self) -> None:
        if (
            not math.isfinite(float(self.fee_rate))
            or not math.isfinite(float(self.slippage_bps))
            or self.fee_rate < 0
            or self.slippage_bps < 0
        ):
            raise ValueError("fee_rate and slippage_bps must be finite and non-negative")
        if self.depth <= 0:
            raise ValueError("depth must be positive")
        if self.live:
            raise LiveExecutionDisabled("live execution is disabled by policy")


@dataclass(frozen=True, slots=True)
class PaperOrder:
    order_id: str
    market_type: MarketType
    symbol: str
    side: Side
    quantity: float
    requested_at: datetime
    reference_price: float
    strategy_id: str
    market_id: str | None = None
    outcome: str | None = None

    def __post_init__(self) -> None:
        if self.quantity <= 0 or not math.isfinite(self.quantity):
            raise ValueError("quantity must be finite and positive")
        if self.reference_price < 0 or not math.isfinite(self.reference_price):
            raise ValueError("reference_price must be finite and non-negative")
        if self.outcome is not None and self.outcome.lower() not in {"yes", "no"}:
            raise ValueError("prediction outcome must be yes or no")
        object.__setattr__(self, "requested_at", ensure_utc(self.requested_at))


class PaperTrader:
    """Common simulation engine; subclasses provide market observations."""

    market_type: MarketType

    def __init__(
        self,
        provider: Any,
        strategy: Any,
        risk: Any | None = None,
        portfolio: Any | None = None,
        *,
        strategy_id: str | None = None,
        config: PaperTradingConfig | None = None,
        live: bool = False,
    ) -> None:
        self.provider = provider
        self.strategy = strategy
        self.risk = risk
        self.portfolio = portfolio
        self.strategy_id = strategy_id or str(getattr(strategy, "strategy_id", getattr(strategy, "id", strategy.__class__.__name__)))
        self.config = config or PaperTradingConfig()
        if live or self.config.live:
            raise LiveExecutionDisabled("paper traders cannot enable live execution")
        self._fills: list[Fill] = []
        self._sequence = 0

    @property
    def fills(self) -> tuple[Fill, ...]:
        return tuple(self._fills)

    def _strategy_signal(self, observation: Any, context: Mapping[str, Any]) -> Any:
        for name in ("signal", "decide", "generate_signal", "generate"):
            method = getattr(self.strategy, name, None)
            if not callable(method):
                continue
            # Strategy interfaces conventionally accept one observation.  A
            # context mapping is richer while canonical observations remain
            # available under context['market'] / context['ticker'].
            try:
                return method(context)
            except (TypeError, AttributeError):
                return method(observation)
        if callable(self.strategy):
            try:
                return self.strategy(context)
            except (TypeError, AttributeError):
                return self.strategy(observation)
        return None

    @staticmethod
    def _normalize_signal(signal: Any) -> tuple[Side, float | None] | None:
        if signal is None or signal is False:
            return None
        side: Any = signal
        quantity: float | None = None
        if isinstance(signal, Mapping):
            side = signal.get("side", signal.get("action", signal.get("signal")))
            quantity = signal.get("quantity", signal.get("size"))
        elif isinstance(signal, (tuple, list)) and signal:
            side = signal[0]
            if len(signal) > 1:
                quantity = signal[1]
        if isinstance(side, Side):
            normalized = side
        else:
            value = str(side).strip().lower()
            if value in {"buy", "long", "yes", "bid", "buy_yes", "buy_no", "no"}:
                normalized = Side.BUY
            elif value in {"sell", "short", "ask", "sell_yes", "sell_no"}:
                normalized = Side.SELL
            else:
                return None
        if quantity is not None:
            try:
                quantity = float(quantity)
            except (TypeError, ValueError):
                return None
            if quantity <= 0 or not math.isfinite(quantity):
                return None
        return normalized, quantity
    @staticmethod
    def _signal_outcome(signal: Any) -> str | None:
        if isinstance(signal, Mapping):
            explicit = signal.get("outcome", signal.get("token"))
            if explicit is not None and str(explicit).strip().lower() in {"yes", "no"}:
                return str(explicit).strip().lower()
            side = str(signal.get("side", signal.get("action", signal.get("signal", "")))).strip().lower()
        elif isinstance(signal, (tuple, list)) and signal:
            side = str(signal[0]).strip().lower()
        else:
            return None
        if side in {"yes", "buy_yes", "sell_yes"}:
            return "yes"
        if side in {"no", "buy_no", "sell_no"}:
            return "no"
        return None

    def _quantity(self, signal: Any, context: Mapping[str, Any], requested: float | None) -> float:
        normalized = self._normalize_signal(signal)
        candidate = requested if requested is not None else (normalized[1] if normalized else None)
        model_probability = context.get("trade_probability", context.get("model_probability"))
        reference_price = context.get("reference_price")
        if candidate is None and self.risk is not None:
            for name in ("size", "position_size", "quantity"):
                method = getattr(self.risk, name, None)
                if not callable(method):
                    continue
                try:
                    if (
                        self.market_type is MarketType.PREDICTION
                        and model_probability is not None
                        and reference_price is not None
                    ):
                        candidate = method(float(model_probability), float(reference_price))
                    else:
                        candidate = method(context)
                except (TypeError, ValueError):
                    try:
                        candidate = method()
                    except (TypeError, ValueError):
                        continue
                break
        try:
            quantity = float(candidate if candidate is not None else 1.0)
        except (TypeError, ValueError):
            return 0.0
        return quantity if math.isfinite(quantity) and quantity > 0 else 0.0

    def _approved(self, order: PaperOrder, context: Mapping[str, Any]) -> bool:
        if self.risk is None:
            return True
        kwargs = {
            "price": context.get("risk_price", order.reference_price),
            "quantity": order.quantity,
            "strategy_id": order.strategy_id,
            "group": context.get("group"),
            "liquidity": context.get("liquidity"),
            "spread": context.get("spread"),
            "expected_loss": context.get("expected_loss"),
            "cvar": context.get("cvar"),
            "timestamp": order.requested_at,
        }

        def invoke(method: Any) -> Any:
            try:
                signature = inspect.signature(method)
                parameters = tuple(signature.parameters.values())
            except (TypeError, ValueError):
                parameters = ()
            accepts_kwargs = any(parameter.kind is inspect.Parameter.VAR_KEYWORD for parameter in parameters)
            named = {
                parameter.name
                for parameter in parameters
                if parameter.kind in (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY)
            }
            if accepts_kwargs:
                return method(order, **kwargs)
            filtered = {name: value for name, value in kwargs.items() if name in named}
            if filtered:
                return method(order, **filtered)
            positional = tuple(
                parameter
                for parameter in parameters
                if parameter.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
            )
            return method(order, context) if len(positional) >= 2 else method(order)
        checker = getattr(self.risk, "check_order", None)
        if callable(checker):
            try:
                return bool(invoke(checker))
            except Exception:
                return False
        for name in ("approve", "check", "allows"):
            method = getattr(self.risk, name, None)
            if not callable(method):
                continue
            try:
                return bool(invoke(method))
            except Exception:
                return False
        return True

    def _notify_risk(self, fill: Fill, context: Mapping[str, Any]) -> None:
        if self.risk is None:
            return
        method = getattr(self.risk, "record_fill", getattr(self.risk, "on_fill", None))
        if not callable(method):
            return
        try:
            signature = inspect.signature(method)
            parameters = tuple(signature.parameters.values())
            accepts_kwargs = any(parameter.kind is inspect.Parameter.VAR_KEYWORD for parameter in parameters)
            accepts_group = accepts_kwargs or any(
                parameter.name == "group"
                and parameter.kind
                in (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY)
                for parameter in parameters
            )
        except (TypeError, ValueError):
            accepts_group = True
        if accepts_group:
            method(fill, group=context.get("group"))
        else:
            method(fill)

    def _update_risk_equity(self, context: Mapping[str, Any], timestamp: datetime) -> bool:
        if self.risk is None or self.portfolio is None:
            return True
        updater = getattr(self.risk, "update_equity", None)
        equity_method = getattr(self.portfolio, "equity", None)
        if not callable(updater) or not callable(equity_method):
            return True
        prices = context.get("mark_prices")
        try:
            if isinstance(prices, Mapping):
                equity = equity_method(prices)
            else:
                equity = equity_method()
            updater(float(equity), timestamp=ensure_utc(timestamp))
        except Exception:
            # A risk update failure must not create a false approval path.
            return False
        return True

    def _notify_portfolio(self, fill: Fill) -> None:
        if self.portfolio is None:
            return
        for method_name in ("apply_fill", "record_fill", "on_fill", "update"):
            method = getattr(self.portfolio, method_name, None)
            if callable(method):
                method(fill)
                return

    def _order_id(self, symbol: str, timestamp: datetime, side: Side) -> str:
        self._sequence += 1
        raw = f"{self.strategy_id}|{symbol}|{ensure_utc(timestamp).isoformat()}|{side.value}|{self._sequence}"
        return "paper-" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:20]

    def _make_fill(
        self,
        *,
        symbol: str,
        side: Side,
        quantity: float,
        price: float,
        reference: float,
        timestamp: datetime,
        expected_probability: float | None = None,
        order_id: str | None = None,
        market_id: str | None = None,
        outcome: str | None = None,
    ) -> Fill:
        slippage = abs(price - reference)
        fee = abs(price * quantity) * self.config.fee_rate
        metadata: dict[str, Any] = {
            "paper": True,
            "simulation_quality": self.config.quality.value,
            "reference_price": reference,
        }
        if outcome is not None:
            metadata["outcome"] = outcome
        return Fill(
            timestamp=ensure_utc(timestamp),
            market_type=self.market_type,
            symbol=symbol,
            side=side,
            quantity=quantity,
            price=price,
            fees=fee,
            slippage=slippage,
            strategy_id=self.strategy_id,
            order_id=order_id or self._order_id(symbol, timestamp, side),
            market_id=market_id,
            expected_probability=expected_probability,
            executable_probability=price if self.market_type is MarketType.PREDICTION else None,
            metadata=metadata,
        )

    def _run_observation(
        self,
        *,
        symbol: str,
        observation: Any,
        book: OrderBookSnapshot | None,
        no_book: OrderBookSnapshot | None = None,
        timestamp: datetime,
        reference: float,
        quantity: float | None = None,
        market_id: str | None = None,
    ) -> Fill | None:
        def value(name: str, default: Any = None) -> Any:
            return observation.get(name, default) if isinstance(observation, Mapping) else getattr(observation, name, default)

        liquidity = value("liquidity")
        initial_bid = book.best_bid if book is not None else value("yes_bid", value("bid"))
        initial_ask = book.best_ask if book is not None else value("yes_ask", value("ask"))
        bid_number = _finite_number(initial_bid)
        ask_number = _finite_number(initial_ask)
        initial_spread = ask_number - bid_number if bid_number is not None and ask_number is not None else None
        expected = value("model_probability", value("predicted_probability"))
        if self.market_type is MarketType.PREDICTION:
            yes_mark = _finite_number(value("yes_mid", reference))
            if yes_mark is None:
                yes_mark = _finite_number(reference)
            no_mark = _finite_number(value("no_mid"))
            if no_mark is None and yes_mark is not None and 0.0 <= yes_mark <= 1.0:
                no_mark = 1.0 - yes_mark
            mark_prices: dict[str, float] = {}
            if yes_mark is not None and 0.0 <= yes_mark <= 1.0:
                mark_prices[symbol] = yes_mark
            if no_mark is not None and 0.0 <= no_mark <= 1.0:
                mark_prices[f"{symbol}|no"] = no_mark
        else:
            raw_mark = value("last", value("close", reference))
            mark = _finite_number(raw_mark)
            if mark is None:
                mark = _finite_number(reference)
            mark_prices = {symbol: mark} if mark is not None and mark >= 0.0 else {}
        context: dict[str, Any] = {
            "market_type": self.market_type.value,
            "symbol": symbol,
            "observation": observation,
            "market": observation,
            "order_book": book,
            "liquidity": liquidity,
            "spread": initial_spread,
            "reference_price": reference,
            "mark_prices": mark_prices,
            "model_probability": expected,
            "expected_loss": value("expected_loss"),
            "paper": True,
        }
        if not self._update_risk_equity(context, timestamp):
            return None
        if self.market_type is MarketType.PREDICTION:
            raw_state = value("settlement", SettlementState.OPEN)
            try:
                state = raw_state if isinstance(raw_state, SettlementState) else SettlementState(str(raw_state))
            except ValueError:
                state = SettlementState.UNKNOWN
            if state in {
                SettlementState.RESOLVED_YES,
                SettlementState.RESOLVED_NO,
                SettlementState.VOID,
            }:
                if self.portfolio is not None and market_id:
                    try:
                        self.portfolio.resolve(
                            ResolvedContract(
                                market_id,
                                state,
                                timestamp,
                                str(value("resolution_criteria", "")),
                            )
                        )
                    except (TypeError, ValueError):
                        return None
                    if not self._update_risk_equity(context, timestamp):
                        return None
                return None
        signal = self._strategy_signal(observation, context)
        normalized = self._normalize_signal(signal)
        if normalized is None:
            return None
        side, indicated_quantity = normalized
        outcome = self._signal_outcome(signal) if self.market_type is MarketType.PREDICTION else None
        execution_book = book
        if execution_book is None and outcome is None:
            side_quote = initial_ask if side is Side.BUY else initial_bid
            if side_quote is not None:
                try:
                    reference = float(side_quote)
                except (TypeError, ValueError):
                    return None
        try:
            execution_reference = float(reference)
        except (TypeError, ValueError):
            return None
        if not math.isfinite(execution_reference) or execution_reference <= 0:
            return None
        no_bid = value("no_bid")
        no_ask = value("no_ask")
        no_mid = value("no_mid")
        if outcome == "no":
            raw_no_book = no_book or value("no_order_book")
            execution_book = raw_no_book if isinstance(raw_no_book, OrderBookSnapshot) else None
            if execution_book is None and isinstance(book, OrderBookSnapshot):
                execution_book = _complement_book(book)
            yes_mid = value("yes_mid")
            yes_bid = value("yes_bid")
            yes_ask = value("yes_ask")
            try:
                yes_mid_value = float(yes_mid) if yes_mid is not None else None
            except (TypeError, ValueError):
                yes_mid_value = None
            if no_mid is None and yes_mid_value is not None and 0.0 < yes_mid_value < 1.0:
                no_mid = 1.0 - yes_mid_value
            yes_bid_value = _finite_number(yes_bid)
            yes_ask_value = _finite_number(yes_ask)
            if no_ask is None and yes_bid_value is not None and 0.0 < yes_bid_value < 1.0:
                no_ask = 1.0 - yes_bid_value
            if no_bid is None and yes_ask_value is not None and 0.0 < yes_ask_value < 1.0:
                no_bid = 1.0 - yes_ask_value
            quote = no_ask if side is Side.BUY else no_bid
            if quote is None:
                quote = no_mid
            if quote is not None:
                try:
                    execution_reference = float(quote)
                except (TypeError, ValueError):
                    return None
                if not math.isfinite(execution_reference) or execution_reference <= 0:
                    return None
        elif execution_book is None:
            side_quote = initial_ask if side is Side.BUY else initial_bid
            if side_quote is not None:
                try:
                    execution_reference = float(side_quote)
                except (TypeError, ValueError):
                    return None
        if self.market_type is MarketType.PREDICTION and execution_reference > 1.0:
            return None
        if execution_book is not None:
            executable_reference = execution_book.best_ask if side is Side.BUY else execution_book.best_bid
            if executable_reference is not None and executable_reference > 0:
                execution_reference = float(executable_reference)
        if self.market_type is MarketType.PREDICTION and execution_reference > 1.0:
            return None
        if execution_book is not None:
            book_bid = execution_book.best_bid
            book_ask = execution_book.best_ask
        elif outcome == "no":
            book_bid, book_ask = value("no_bid"), value("no_ask")
        else:
            book_bid, book_ask = initial_bid, initial_ask
        spread = (
            float(book_ask) - float(book_bid)
            if book_bid is not None and book_ask is not None
            else None
        )
        trade_probability: float | None = None
        try:
            model_probability = float(expected)
            if math.isfinite(model_probability) and 0.0 <= model_probability <= 1.0:
                trade_probability = model_probability if outcome != "no" else 1.0 - model_probability
        except (TypeError, ValueError):
            pass
        context.update(
            {
                "order_book": execution_book,
                "spread": spread,
                "reference_price": execution_reference,
                "model_probability": expected,
                "trade_probability": trade_probability,
            }
        )
        risk_price = execution_reference
        if execution_book is not None:
            executable = execution_book.best_ask if side is Side.BUY else execution_book.best_bid
            if executable is not None:
                risk_price = executable
        context["risk_price"] = risk_price
        quantity = self._quantity(signal, context, quantity if quantity is not None else indicated_quantity)
        if self.portfolio is not None:
            if side is Side.BUY:
                cash = getattr(self.portfolio, "cash", None)
                try:
                    cash_value = float(cash) if cash is not None else None
                except (TypeError, ValueError):
                    cash_value = None
                if cash_value is not None and math.isfinite(cash_value):
                    cost_factor = (1.0 + self.config.fee_rate) * (1.0 + self.config.slippage_bps / 10000.0)
                    if execution_book is not None:
                        cash_price, available = execution_book.executable_price(side, quantity)
                        if cash_price > 0 and cost_factor > 0:
                            quantity = min(quantity, available, cash_value / (cash_price * cost_factor))
                    elif risk_price > 0 and cost_factor > 0:
                        quantity = min(quantity, cash_value / (risk_price * cost_factor))
            elif hasattr(self.portfolio, "get_position"):
                try:
                    position = self.portfolio.get_position(symbol, outcome=outcome)
                except TypeError:
                    position = self.portfolio.get_position(symbol)
                available = float(getattr(position, "quantity", 0.0)) if position is not None else 0.0
                quantity = min(quantity, max(0.0, available))
        if execution_book is not None:
            slippage_factor = 1.0 + (self.config.slippage_bps / 10000.0) * (1.0 if side is Side.BUY else -1.0)
            if not math.isfinite(slippage_factor) or slippage_factor <= 0:
                return None
            try:
                projected_price, projected_quantity = execution_book.executable_price(
                    side,
                    quantity,
                    price_multiplier=slippage_factor,
                )
            except (TypeError, ValueError):
                return None
            if projected_quantity <= 0 or projected_price <= 0:
                return None
            quantity = min(quantity, projected_quantity)
            if quantity <= 0:
                return None
            if quantity < projected_quantity:
                projected_price, projected_quantity = execution_book.executable_price(
                    side,
                    quantity,
                    price_multiplier=slippage_factor,
                )
            if projected_quantity <= 0 or projected_price <= 0:
                return None
            execution_reference = projected_price
            risk_price = projected_price * slippage_factor
            context["reference_price"] = execution_reference
            context["risk_price"] = risk_price
        if quantity <= 0 or not math.isfinite(quantity):
            return None
        order = PaperOrder(
            order_id=self._order_id(symbol, timestamp, side),
            market_type=self.market_type,
            symbol=symbol,
            side=side,
            quantity=quantity,
            requested_at=timestamp,
            reference_price=execution_reference,
            strategy_id=self.strategy_id,
            market_id=market_id,
            outcome=outcome,
        )
        if not self._approved(order, context):
            return None
        if execution_book is not None:
            price, filled = execution_book.executable_price(side, quantity)
            quantity = filled
            if not quantity:
                return None
        else:
            price = execution_reference
        if price <= 0 or not math.isfinite(price):
            return None
        direction = 1.0 if side is Side.BUY else -1.0
        price *= 1.0 + direction * self.config.slippage_bps / 10000.0
        if (
            price <= 0
            or not math.isfinite(price)
            or self.market_type is MarketType.PREDICTION
            and price > 1.0 + 1e-12
        ):
            return None
        fill = self._make_fill(
            symbol=symbol,
            side=side,
            quantity=quantity,
            price=price,
            reference=execution_reference,
            timestamp=timestamp,
            market_id=market_id,
            outcome=outcome,
            expected_probability=trade_probability,
            order_id=order.order_id,
        )
        self._fills.append(fill)
        self._notify_portfolio(fill)
        self._update_risk_equity(context, timestamp)
        self._notify_risk(fill, context)
        return fill


class CryptoPaperTrader(PaperTrader):
    market_type = MarketType.CRYPTO_SPOT

    def run_once(self, symbol: str, *, quantity: float | None = None, timestamp: datetime | None = None) -> Fill | None:
        ticker: CryptoTicker | None = self.provider.ticker(symbol)
        if ticker is None:
            return None
        book = self.provider.order_book(symbol, depth=self.config.depth)
        stamp = ensure_utc(timestamp or ticker.timestamp)
        reference = ticker.midpoint
        return self._run_observation(symbol=symbol, observation=ticker, book=book, timestamp=stamp, reference=reference, quantity=quantity)

    step = run_once

    def run(self, symbol: str, start: datetime | None = None, end: datetime | None = None, *, interval: str = "1d", quantity: float | None = None) -> tuple[Fill, ...]:
        if start is None and end is None:
            fill = self.run_once(symbol, quantity=quantity)
            return (fill,) if fill else ()
        bars = tuple(self.provider.historical_ohlcv(symbol, start=start, end=end, interval=interval))
        for index in range(1, len(bars)):
            signal_bar = bars[index - 1]
            execution_bar = bars[index]
            if isinstance(execution_bar, Mapping):
                raw_open = execution_bar.get("open")
                raw_timestamp = execution_bar.get("timestamp")
            else:
                raw_open = getattr(execution_bar, "open", None)
                raw_timestamp = getattr(execution_bar, "timestamp", None)
            opening = _finite_number(raw_open)
            stamp = parse_timestamp(raw_timestamp)
            if opening is None or opening <= 0 or stamp is None:
                continue
            self._run_observation(
                symbol=symbol,
                observation=signal_bar,
                book=None,
                timestamp=stamp,
                reference=opening,
                quantity=quantity,
            )
        return self.fills


class PredictionPaperTrader(PaperTrader):
    market_type = MarketType.PREDICTION
    def run_once(self, market_id: str, *, quantity: float | None = None, timestamp: datetime | None = None) -> Fill | None:
        market: PredictionMarketSnapshot | None = self.provider.market(market_id)
        if market is None:
            return None
        try:
            books = self.provider.order_books(market_id, depth=self.config.depth)
        except (AttributeError, TypeError):
            books = {}
        book = books.get("yes") if isinstance(books, Mapping) else None
        no_book = books.get("no") if isinstance(books, Mapping) else None
        book = book if isinstance(book, OrderBookSnapshot) else market.order_book
        if not isinstance(no_book, OrderBookSnapshot):
            no_book = None
        reference = market.yes_mid
        if reference is None:
            reference = market.yes_ask if market.yes_ask is not None else market.yes_bid
        if reference is None:
            reference = market.no_mid
        if reference is None:
            reference = market.no_ask if market.no_ask is not None else market.no_bid
        if reference is None:
            return None
        stamp = ensure_utc(timestamp or market.timestamp)
        if book is not None and book.timestamp > stamp:
            book = None
        if no_book is not None and no_book.timestamp > stamp:
            no_book = None
        return self._run_observation(
            symbol=market_id,
            observation=market,
            book=book,
            no_book=no_book,
            timestamp=stamp,
            reference=float(reference),
            quantity=quantity,
            market_id=market_id,
        )

    step = run_once

    def run(self, market_id: str, start: datetime | None = None, end: datetime | None = None, *, quantity: float | None = None) -> tuple[Fill, ...]:
        market = self.provider.market(market_id)
        if market is None:
            return self.fills
        raw_points = tuple(self.provider.price_history(market_id, start=start, end=end))
        points: list[tuple[Mapping[str, Any], datetime]] = []
        for point in raw_points:
            if not isinstance(point, Mapping):
                continue
            stamp = parse_timestamp(point.get("timestamp", point.get("t")))
            if stamp is None:
                continue
            points.append((point, stamp))
        points.sort(key=lambda item: item[1])
        terminal_state = market.settlement if market.settlement in {
            SettlementState.RESOLVED_YES,
            SettlementState.RESOLVED_NO,
            SettlementState.VOID,
        } else None
        for index, (point, stamp) in enumerate(points):
            price = point.get("yes_mid", point.get("price", market.yes_mid))
            try:
                price = float(price)
            except (TypeError, ValueError):
                continue
            if not math.isfinite(price) or price <= 0:
                continue
            observation = dict(point)
            observation.setdefault("timestamp", stamp)
            observation.setdefault("yes_mid", price)
            observation.setdefault("expiry", market.expiry)
            observation.setdefault("resolution_criteria", market.resolution_criteria)
            observation.setdefault("yes_bid", market.yes_bid)
            observation.setdefault("yes_ask", market.yes_ask)
            observation.setdefault("no_bid", market.no_bid)
            observation.setdefault("no_ask", market.no_ask)
            observation.setdefault("no_mid", market.no_mid)
            observation.setdefault("liquidity", market.liquidity)
            if "settlement" not in observation:
                resolution_visible = (
                    terminal_state is not None
                    and (index == len(points) - 1 or market.expiry is not None and stamp >= market.expiry)
                )
                observation["settlement"] = terminal_state.value if resolution_visible else SettlementState.OPEN.value
            self._run_observation(
                symbol=market_id,
                observation=observation,
                book=None,
                timestamp=stamp,
                reference=price,
                quantity=quantity,
                market_id=market_id,
            )
        return self.fills


CryptoPaperOrchestrator = CryptoPaperTrader
PredictionPaperOrchestrator = PredictionPaperTrader


PaperCryptoTrader = CryptoPaperTrader
PaperPredictionTrader = PredictionPaperTrader
CryptoPaperTrading = CryptoPaperTrader
PredictionPaperTrading = PredictionPaperTrader


def paper_crypto(provider: Any, strategy: Any, risk: Any | None = None, portfolio: Any | None = None, **kwargs: Any) -> CryptoPaperTrader:
    return CryptoPaperTrader(provider, strategy, risk, portfolio, **kwargs)


def paper_prediction(provider: Any, strategy: Any, risk: Any | None = None, portfolio: Any | None = None, **kwargs: Any) -> PredictionPaperTrader:
    return PredictionPaperTrader(provider, strategy, risk, portfolio, **kwargs)


__all__ = [
    "LiveExecutionDisabled", "PaperTradingConfig", "PaperOrder", "PaperTrader",
    "CryptoPaperTrader", "PredictionPaperTrader", "PaperCryptoTrader", "PaperPredictionTrader",
    "CryptoPaperTrading", "PredictionPaperTrading", "paper_crypto", "paper_prediction",
]
