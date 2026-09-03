"""Historical prediction-market simulator with executable YES/NO prices."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import math
from typing import Any, Mapping, Sequence

from axiom.domain import (
    MarketType,
    OrderBookLevel,
    OrderBookSnapshot,
    PredictionMarketSnapshot,
    ResolvedContract,
    ResearchQuality,
    SettlementState,
    Side,
    SimulationQuality,
    ensure_utc,
    parse_timestamp,
)
from axiom.metrics import calculate_prediction_metrics
from axiom.portfolio import OrderRequest, Portfolio
from axiom.strategy import StrategyDefinition, evaluate_signal, validate_strategy
from .types import BacktestResult


def _value(item: Any, name: str, default: Any = None) -> Any:
    return item.get(name, default) if isinstance(item, Mapping) else getattr(item, name, default)


def _time(item: Any) -> datetime:
    stamp = _value(item, "timestamp", datetime.min.replace(tzinfo=timezone.utc))
    return parse_timestamp(stamp) or datetime.min.replace(tzinfo=timezone.utc)


def _number(item: Any, name: str, default: float = 0.0) -> float:
    try:
        value = float(_value(item, name, default))
    except (TypeError, ValueError):
        return default
    return value if math.isfinite(value) else default


def _probability(item: Any, name: str, default: float = 0.0) -> float:
    value = _number(item, name, default)
    return value if 0.0 <= value <= 1.0 else default


def _quality(snapshots: Sequence[Any]) -> SimulationQuality:
    if len(snapshots) < 3:
        return SimulationQuality.LOW
    executable = sum(
        1
        for snap in snapshots
        if (
            (_number(snap, "yes_ask", 0.0) > 0 and _number(snap, "yes_bid", 0.0) > 0)
            or (_number(snap, "no_ask", 0.0) > 0 and _number(snap, "no_bid", 0.0) > 0)
        )
    )
    liquidity = sum(1 for snap in snapshots if _number(snap, "liquidity", 0.0) > 0)
    if executable < len(snapshots) // 2:
        return SimulationQuality.LOW
    return SimulationQuality.HIGH if liquidity * 2 >= len(snapshots) else SimulationQuality.MEDIUM


def _outcome(value: Any) -> str:
    if isinstance(value, SettlementState):
        return value.value
    return str(value or "").lower()
def _complement_book(book: OrderBookSnapshot) -> OrderBookSnapshot:
    """Convert a YES-token book into the complementary NO-token book."""
    bids = tuple(OrderBookLevel(1.0 - level.price, level.size) for level in book.asks)
    asks = tuple(OrderBookLevel(1.0 - level.price, level.size) for level in book.bids)
    return OrderBookSnapshot(book.timestamp, bids=bids, asks=asks)


def _coerce_book(value: Any, fallback_timestamp: datetime) -> OrderBookSnapshot | None:
    if isinstance(value, OrderBookSnapshot):
        return value
    if not isinstance(value, Mapping):
        return None
    def levels(raw: Any, *, reverse: bool) -> tuple[OrderBookLevel, ...]:
        if not isinstance(raw, (list, tuple)):
            return ()
        result: list[OrderBookLevel] = []
        for item in raw:
            if isinstance(item, Mapping):
                price, size = item.get("price", item.get("p")), item.get("size", item.get("quantity", item.get("q")))
            elif isinstance(item, (list, tuple)) and len(item) >= 2:
                price, size = item[0], item[1]
            else:
                continue
            try:
                parsed_price, parsed_size = float(price), float(size)
                if not math.isfinite(parsed_price) or not 0.0 <= parsed_price <= 1.0:
                    continue
                result.append(OrderBookLevel(parsed_price, parsed_size))
            except (TypeError, ValueError):
                continue
        return tuple(sorted(result, key=lambda level: level.price, reverse=reverse))
    bids = levels(value.get("bids"), reverse=True)
    asks = levels(value.get("asks"), reverse=False)
    if not bids and not asks:
        return None
    try:
        return OrderBookSnapshot(
            parse_timestamp(value.get("timestamp")) or fallback_timestamp,
            bids=bids,
            asks=asks,
            token_id=value.get("token_id"),
        )
    except (TypeError, ValueError):
        return None




@dataclass(slots=True)
class PredictionMarketBacktester:
    initial_cash: float = 10_000.0
    fee_bps: float = 10.0
    slippage_bps: float = 5.0
    allocation: float = 0.25

    def __post_init__(self) -> None:
        if (
            not all(math.isfinite(float(value)) for value in (self.initial_cash, self.fee_bps, self.slippage_bps, self.allocation))
            or self.initial_cash < 0
            or self.fee_bps < 0
            or self.slippage_bps < 0
            or not 0.0 <= self.allocation <= 1.0
        ):
            raise ValueError("backtest cash, costs, and allocation must be finite and in range")

    def run(
        self, snapshots: Sequence[PredictionMarketSnapshot | Mapping[str, Any]], strategy: StrategyDefinition | Mapping[str, Any] | str,
        *, resolutions: Mapping[str, ResolvedContract] | Sequence[ResolvedContract] | None = None,
        initial_cash: float | None = None,
        research_quality: ResearchQuality | str | None = None,
    ) -> BacktestResult:
        definition = validate_strategy(strategy)
        if definition.market_type is not MarketType.PREDICTION:
            raise ValueError("PredictionMarketBacktester requires a prediction strategy")
        rows = sorted(list(snapshots), key=_time)
        contracts: dict[str, ResolvedContract] = {}
        if isinstance(resolutions, Mapping):
            contracts.update(resolutions)
        elif resolutions:
            contracts.update({contract.market_id: contract for contract in resolutions})
        portfolio = Portfolio(self.initial_cash if initial_cash is None else initial_cash)
        observed_outcomes: dict[str, str] = {}
        curve: list[dict[str, Any]] = []
        labels: list[SimulationQuality] = []
        for index, snapshot in enumerate(rows):
            timestamp = _time(snapshot)
            market_id = str(_value(snapshot, "market_id", ""))
            # Explicit resolutions and snapshot settlement become observable
            # only at their timestamp; a resolved market cannot be re-entered.
            contract = contracts.get(market_id)
            resolved_now = False
            settlement = _value(snapshot, "settlement", SettlementState.OPEN)
            state = settlement if isinstance(settlement, SettlementState) else _outcome(settlement)
            if contract is not None and ensure_utc(contract.resolved_at) <= timestamp:
                portfolio.resolve(contract)
                observed_outcomes[market_id] = contract.outcome.value
                resolved_now = True
            elif contract is None and (
                state in {
                    SettlementState.RESOLVED_YES,
                    SettlementState.RESOLVED_NO,
                    SettlementState.VOID,
                }
                or state in {"resolved_yes", "resolved_no", "void"}
            ):
                outcome = state if isinstance(state, SettlementState) else SettlementState(state)
                portfolio.resolve(
                    ResolvedContract(
                        market_id,
                        outcome,
                        timestamp,
                        str(_value(snapshot, "resolution_criteria", "")),
                    )
                )
                if outcome is not SettlementState.UNKNOWN:
                    observed_outcomes[market_id] = outcome.value
                resolved_now = True
            effective_state = (
                SettlementState.OPEN
                if contract is not None and ensure_utc(contract.resolved_at) > timestamp
                else state
            )
            # Current quote is observable; model history is strictly rows[:index+1].
            context = {"snapshots": rows[: index + 1]}
            if isinstance(snapshot, Mapping):
                context.update(snapshot)
            score = evaluate_signal(definition, context)
            outcome = "yes" if score > 0 else "no"
            current = portfolio.get_position(market_id, outcome=outcome)
            current_quantity = current.quantity if current else 0.0
            model_probability = _value(snapshot, "model_probability")
            try:
                model_probability = float(model_probability)
                trade_probability = model_probability if outcome == "yes" else 1.0 - model_probability
                if not math.isfinite(trade_probability) or not 0.0 <= trade_probability <= 1.0:
                    trade_probability = None
            except (TypeError, ValueError):
                trade_probability = None
            ask_name = "yes_ask" if score > 0 else "no_ask"
            ask = _probability(snapshot, ask_name, 0.0)
            if score < 0 and ask <= 0:
                yes_bid = _probability(snapshot, "yes_bid", 0.0)
                ask = 1.0 - yes_bid if 0.0 < yes_bid < 1.0 else 0.0
            if outcome == "yes":
                raw_book = _value(snapshot, "order_book", _value(snapshot, "yes_order_book"))
            else:
                raw_book = _value(snapshot, "no_order_book")
            order_book = _coerce_book(raw_book, timestamp)
            if outcome == "no" and order_book is None:
                yes_book = _coerce_book(_value(snapshot, "order_book", _value(snapshot, "yes_order_book")), timestamp)
                if yes_book is not None:
                    order_book = _complement_book(yes_book)
            if order_book is not None and order_book.timestamp > timestamp:
                # A future-captured book cannot be used to fill an earlier quote.
                order_book = None
            if order_book is not None and order_book.best_ask is not None:
                ask = order_book.best_ask
            if (
                not resolved_now
                and effective_state not in {SettlementState.VOID, SettlementState.UNKNOWN, "void", "unknown", SettlementState.RESOLVED_YES, SettlementState.RESOLVED_NO, "resolved_yes", "resolved_no"}
                and score != 0
                and market_id
                and ask > 0
            ):
                desired = max(0.0, portfolio.cash) * max(0.0, min(1.0, self.allocation)) * min(1.0, abs(score)) / ask
                delta = desired - (current_quantity if (current and (current.outcome or "yes") == outcome) else 0.0)
                if delta > 1e-12:
                    portfolio.execute_order(
                        OrderRequest(
                            market_id,
                            Side.BUY,
                            delta,
                            MarketType.PREDICTION,
                            strategy_id=definition.id,
                            market_id=market_id,
                            outcome=outcome,
                            expected_probability=trade_probability,
                        ),
                        timestamp=timestamp,
                        price=None if order_book is not None else ask,
                        order_book=order_book,
                        fee_bps=self.fee_bps,
                        slippage_bps=self.slippage_bps,
                    )
            yes_mid = _probability(snapshot, "yes_mid", _probability(snapshot, "yes_ask", 0.0))
            no_mid = _probability(snapshot, "no_mid", _probability(snapshot, "no_ask", 0.0))
            if no_mid <= 0 and 0.0 < yes_mid < 1.0:
                no_mid = 1.0 - yes_mid
            prices = {market_id: yes_mid, f"{market_id}|no": no_mid}
            equity = portfolio.equity(prices)
            quality = SimulationQuality.HIGH if _number(snapshot, "liquidity", 0.0) > 0 and (
                (_probability(snapshot, "yes_bid", 0.0) > 0 and _probability(snapshot, "yes_ask", 0.0) > 0)
                or (_probability(snapshot, "no_bid", 0.0) > 0 and _probability(snapshot, "no_ask", 0.0) > 0)
            ) else SimulationQuality.MEDIUM
            labels.append(quality)
            curve.append({"timestamp": timestamp, "equity": equity, "cash": portfolio.cash, "market_id": market_id, "quality": quality.value})
        outcomes = dict(observed_outcomes)
        probability_records: list[dict[str, float | None]] = []
        for fill in portfolio.fills:
            terminal = outcomes.get(fill.market_id or fill.symbol)
            if fill.expected_probability is None or terminal not in {
                SettlementState.RESOLVED_YES.value,
                SettlementState.RESOLVED_NO.value,
            }:
                continue
            traded_outcome = str(fill.metadata.get("outcome", "yes")).lower()
            outcome_value = 1.0 if terminal == SettlementState.RESOLVED_YES.value else 0.0
            if traded_outcome == "no":
                outcome_value = 1.0 - outcome_value
            probability_records.append({"probability": fill.expected_probability, "outcome": outcome_value})
        unresolved = tuple(sorted({position.market_id or position.symbol for position in portfolio.positions.values() if position.market_type is MarketType.PREDICTION and position.quantity}))
        metrics = calculate_prediction_metrics(
            curve,
            fills=portfolio.fills,
            probabilities=probability_records,
            initial_equity=portfolio.initial_cash,
        )
        if research_quality is None:
            has_order_book = any(
                _coerce_book(_value(row, "order_book", _value(row, "yes_order_book")), _time(row)) is not None
                or _coerce_book(_value(row, "no_order_book"), _time(row)) is not None
                for row in rows
            )
            resolved_quality = ResearchQuality.ORDER_BOOK_SIMULATED if has_order_book else ResearchQuality.PRICE_PROXY
        else:
            resolved_quality = research_quality if isinstance(research_quality, ResearchQuality) else ResearchQuality(str(research_quality))
        return BacktestResult(
            tuple(curve),
            tuple(portfolio.fills),
            _quality(rows),
            metrics,
            unresolved,
            outcomes,
            tuple(labels),
            resolved_quality,
        )

    simulate = run


PredictionMarketHistoricalSimulator = PredictionMarketBacktester
PredictionBacktester = PredictionMarketBacktester


__all__ = ["PredictionBacktester", "PredictionMarketBacktester", "PredictionMarketHistoricalSimulator"]
