"""Historical prediction-market simulator with executable YES/NO prices."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import math
from typing import Any, Mapping, Sequence

from axiom.domain import (
    MarketType, OrderBookLevel, OrderBookSnapshot, PredictionMarketSnapshot, ResolvedContract, SettlementState, Side,
    SimulationQuality, ensure_utc,
)
from axiom.metrics import calculate_prediction_metrics
from axiom.portfolio import OrderRequest, Portfolio
from axiom.strategy import StrategyDefinition, evaluate_signal, validate_strategy
from .types import BacktestResult


def _value(item: Any, name: str, default: Any = None) -> Any:
    return item.get(name, default) if isinstance(item, Mapping) else getattr(item, name, default)


def _time(item: Any) -> datetime:
    stamp = _value(item, "timestamp", datetime.min.replace(tzinfo=timezone.utc))
    return ensure_utc(stamp) if isinstance(stamp, datetime) else datetime.min.replace(tzinfo=timezone.utc)


def _number(item: Any, name: str, default: float = 0.0) -> float:
    try:
        value = float(_value(item, name, default))
    except (TypeError, ValueError):
        return default
    return value if math.isfinite(value) else default


def _quality(snapshots: Sequence[Any]) -> SimulationQuality:
    if len(snapshots) < 3:
        return SimulationQuality.LOW
    executable = sum(1 for snap in snapshots if _number(snap, "yes_ask", 0.0) > 0 and _number(snap, "yes_bid", 0.0) > 0)
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
                state in {SettlementState.RESOLVED_YES, SettlementState.RESOLVED_NO}
                or state in {"resolved_yes", "resolved_no"}
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
            current = portfolio.get_position(market_id)
            current_quantity = current.quantity if current else 0.0
            outcome = "yes" if score > 0 else "no"
            model_probability = _value(snapshot, "model_probability")
            try:
                model_probability = float(model_probability)
                trade_probability = model_probability if outcome == "yes" else 1.0 - model_probability
                if not math.isfinite(trade_probability) or not 0.0 <= trade_probability <= 1.0:
                    trade_probability = None
            except (TypeError, ValueError):
                trade_probability = None
            ask_name = "yes_ask" if score > 0 else "no_ask"
            ask = _number(snapshot, ask_name, 0.0)
            if score < 0 and ask <= 0:
                yes_bid = _number(snapshot, "yes_bid", 0.0)
                ask = 1.0 - yes_bid if 0.0 < yes_bid < 1.0 else 0.0
            book_name = "order_book" if outcome == "yes" else "no_order_book"
            raw_book = _value(snapshot, book_name)
            order_book = raw_book if isinstance(raw_book, OrderBookSnapshot) else None
            if outcome == "no" and order_book is None:
                yes_book = _value(snapshot, "order_book")
                if isinstance(yes_book, OrderBookSnapshot):
                    order_book = _complement_book(yes_book)
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
            yes_mid = _number(snapshot, "yes_mid", _number(snapshot, "yes_ask", 0.0))
            no_mid = _number(snapshot, "no_mid", _number(snapshot, "no_ask", 0.0))
            if no_mid <= 0 and 0.0 < yes_mid < 1.0:
                no_mid = 1.0 - yes_mid
            prices = {market_id: yes_mid, f"{market_id}|no": no_mid}
            equity = portfolio.equity(prices)
            quality = SimulationQuality.HIGH if _number(snapshot, "liquidity", 0.0) > 0 and _number(snapshot, "yes_bid", 0.0) > 0 else SimulationQuality.MEDIUM
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
        return BacktestResult(tuple(curve), tuple(portfolio.fills), _quality(rows), metrics, unresolved, outcomes, tuple(labels))

    simulate = run


PredictionMarketHistoricalSimulator = PredictionMarketBacktester
PredictionBacktester = PredictionMarketBacktester


__all__ = ["PredictionBacktester", "PredictionMarketBacktester", "PredictionMarketHistoricalSimulator"]
