"""Rank prediction-market opportunities without submitting orders."""
from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Any, Mapping, Sequence

from .domain import OrderBookLevel, OrderBookSnapshot, PredictionMarketSnapshot, Side
from .metrics import expected_value


@dataclass(frozen=True, slots=True)
class Opportunity:
    market_id: str
    outcome: str
    model_probability: float
    market_price: float
    executable_price: float
    spread: float | None
    displayed_quantity: float
    executable_edge: float
    executable_ev: float
    uncertainty: float
    rank_score: float
    action: str = "PAPER_ONLY"

    def as_record(self) -> dict[str, Any]:
        return {
            "market_id": self.market_id,
            "outcome": self.outcome,
            "model_probability": self.model_probability,
            "market_price": self.market_price,
            "executable_price": self.executable_price,
            "spread": self.spread,
            "displayed_quantity": self.displayed_quantity,
            "executable_edge": self.executable_edge,
            "executable_ev": self.executable_ev,
            "uncertainty": self.uncertainty,
            "rank_score": self.rank_score,
            "action": self.action,
        }


def scan_opportunities(
    markets: Sequence[PredictionMarketSnapshot | Mapping[str, Any]],
    model_probabilities: Mapping[str, Any],
    *,
    uncertainties: Mapping[str, float] | None = None,
    exposures: Mapping[str, float] | None = None,
    fee_rate: float = 0.0,
    min_liquidity: float = 0.0,
    max_spread: float | None = None,
    min_edge: float = 0.0,
) -> tuple[Opportunity, ...]:
    """Return ranked, paper-only candidates from executable asks.

    The scanner uses YES/NO asks and displayed depth.  It has no portfolio
    mutation path and intentionally returns a recommendation label instead of
    an order object.
    """
    if not all(math.isfinite(float(value)) for value in (fee_rate, min_liquidity, min_edge)):
        raise ValueError("scanner thresholds must be finite")
    if fee_rate < 0 or min_liquidity < 0 or min_edge < 0:
        raise ValueError("scanner thresholds cannot be negative")
    if max_spread is not None and (not math.isfinite(float(max_spread)) or max_spread < 0):
        raise ValueError("max_spread must be finite and non-negative")
    result: list[Opportunity] = []
    for market in markets:
        market_id = str(_value(market, "market_id", "")).strip()
        if not market_id or _terminal(_value(market, "settlement")):
            continue
        model = _probability(model_probabilities.get(market_id))
        if model is None:
            continue
        uncertainty = max(0.0, _number((uncertainties or {}).get(market_id), _number(_value(market, "uncertainty"), 0.0)))
        liquidity = max(0.0, _number(_value(market, "liquidity"), 0.0))
        if liquidity < min_liquidity:
            continue
        for outcome, probability in (("yes", model), ("no", 1.0 - model)):
            ask = _number(_value(market, f"{outcome}_ask"), 0.0)
            bid = _optional_number(_value(market, f"{outcome}_bid"))
            book = _book(market, outcome)
            if book is not None:
                if book.best_ask is not None:
                    ask = book.best_ask
                bid = book.best_bid
            if ask <= 0 or ask > 1:
                continue
            if bid is not None and (bid < 0 or bid > 1 or bid > ask):
                continue
            spread = (ask - bid) if bid is not None else None
            if spread is not None and max_spread is not None and spread > max_spread:
                continue
            displayed = sum(level.size for level in book.asks) if book is not None else liquidity
            if displayed <= 0:
                continue
            quantity = displayed
            executable, filled = book.executable_price(Side.BUY, quantity) if book is not None else (ask, quantity)
            if executable <= 0 or executable > 1 or filled <= 0:
                continue
            ev = expected_value(probability, ask, executable_price=executable, fees=fee_rate * executable)
            if ev["executable_edge"] < min_edge:
                continue
            exposure_penalty = max(0.0, _number((exposures or {}).get(market_id), 0.0))
            rank = ev["executable_ev"] - uncertainty * 0.25 - exposure_penalty * 0.01
            result.append(
                Opportunity(
                    market_id,
                    outcome,
                    probability,
                    ask,
                    executable,
                    spread,
                    filled,
                    ev["executable_edge"],
                    ev["executable_ev"],
                    uncertainty,
                    rank,
                )
            )
    result.sort(key=lambda item: (-item.rank_score, item.market_id, item.outcome))
    return tuple(result)


def _value(item: Any, name: str, default: Any = None) -> Any:
    return item.get(name, default) if isinstance(item, Mapping) else getattr(item, name, default)
def _complement_book(book: OrderBookSnapshot) -> OrderBookSnapshot:
    bids = tuple(OrderBookLevel(1.0 - level.price, level.size) for level in book.asks)
    asks = tuple(OrderBookLevel(1.0 - level.price, level.size) for level in book.bids)
    return OrderBookSnapshot(book.timestamp, bids=bids, asks=asks)


def _book(item: Any, outcome: str) -> OrderBookSnapshot | None:
    names = ("order_book", "yes_order_book") if outcome == "yes" else ("no_order_book",)
    for name in names:
        value = _value(item, name)
        if isinstance(value, OrderBookSnapshot):
            return value
    if outcome == "no":
        yes_book = _book(item, "yes")
        if yes_book is not None:
            return _complement_book(yes_book)
    return None


def _probability(value: Any) -> float | None:
    if hasattr(value, "probability"):
        value = value.probability
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) and 0.0 <= number <= 1.0 else None


def _number(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return number if math.isfinite(number) else default


def _optional_number(value: Any) -> float | None:
    number = _number(value, math.nan)
    return number if math.isfinite(number) else None


def _terminal(value: Any) -> bool:
    return str(getattr(value, "value", value or "")).lower() in {"resolved_yes", "resolved_no", "void"}


__all__ = ["Opportunity", "scan_opportunities"]
