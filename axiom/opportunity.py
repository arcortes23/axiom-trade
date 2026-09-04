"""Rank prediction-market opportunities without submitting orders."""
from __future__ import annotations

from dataclasses import dataclass, field
import math
from typing import Any, Mapping, Sequence

from .domain import OrderBookLevel, OrderBookSnapshot, PredictionMarketSnapshot, Side, parse_timestamp
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
    family: str = "unknown"
    correlation_group: str | None = None
    research_quality: str = "PRICE_PROXY"
    model_version: str = "deterministic"
    resolution_seconds: float | None = None
    liquidity: float = 0.0
    metadata: Mapping[str, Any] = field(default_factory=dict)

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
            "family": self.family,
            "correlation_group": self.correlation_group,
            "research_quality": self.research_quality,
            "model_version": self.model_version,
            "resolution_seconds": self.resolution_seconds,
            "liquidity": self.liquidity,
            "metadata": dict(self.metadata),
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
    model_version: str = "deterministic",
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
    if not isinstance(model_probabilities, Mapping):
        raise TypeError("model_probabilities must be a mapping")
    result: list[Opportunity] = []
    for market in markets:
        market_id = str(_value(market, "market_id", "")).strip()
        if not market_id or _terminal(_value(market, "settlement")):
            continue
        model = _probability(model_probabilities.get(market_id))
        family = str(_value(market, "family", "unknown")).strip() or "unknown"
        correlation_group = _value(market, "correlation_group", _value(market, "correlation"))
        correlation_group = str(correlation_group).strip() if correlation_group is not None else None
        quality = str(_value(market, "research_quality", "PRICE_PROXY")).strip() or "PRICE_PROXY"
        market_model_version = str(_value(market, "model_version", model_version)).strip() or model_version
        resolution_seconds = _resolution_seconds(market)
        metadata = {
            "family": family,
            "correlation_group": correlation_group,
            "research_quality": quality,
            "model_version": market_model_version,
        }
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
            market_timestamp = parse_timestamp(_value(market, "timestamp")) or parse_timestamp(_value(market, "observed_at"))
            if book is not None and market_timestamp is not None and book.timestamp > market_timestamp:
                book = None
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
            exposure_penalty = max(
                0.0,
                _number(
                    (exposures or {}).get(f"{market_id}|{outcome}", (exposures or {}).get(market_id)),
                    0.0,
                ),
            )
            ev = expected_value(probability, ask, executable_price=executable, fees=fee_rate * executable)
            if ev["executable_edge"] < min_edge:
                continue
            rank = ev["executable_ev"] - uncertainty * 0.25 - exposure_penalty * 0.01
            result.append(
                Opportunity(
                    market_id=market_id,
                    outcome=outcome,
                    model_probability=probability,
                    market_price=ask,
                    executable_price=executable,
                    spread=spread,
                    displayed_quantity=filled,
                    executable_edge=ev["executable_edge"],
                    executable_ev=ev["executable_ev"],
                    uncertainty=uncertainty,
                    rank_score=rank,
                    family=family,
                    correlation_group=correlation_group,
                    research_quality=quality,
                    model_version=market_model_version,
                    resolution_seconds=resolution_seconds,
                    liquidity=liquidity,
                    metadata=metadata,
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


def _coerce_book(value: Any, fallback_timestamp: Any = None) -> OrderBookSnapshot | None:
    if isinstance(value, OrderBookSnapshot):
        return value
    if not isinstance(value, Mapping):
        return None
    timestamp = parse_timestamp(value.get("timestamp", value.get("ts"))) or parse_timestamp(fallback_timestamp)
    if timestamp is None:
        return None
    def levels(raw: Any, *, reverse: bool) -> tuple[OrderBookLevel, ...]:
        if not isinstance(raw, (list, tuple)):
            return ()
        parsed: list[OrderBookLevel] = []
        for point in raw:
            if isinstance(point, Mapping):
                price = _optional_number(point.get("price", point.get("p")))
                size = _optional_number(point.get("size", point.get("quantity", point.get("q"))))
            elif isinstance(point, (list, tuple)) and len(point) >= 2:
                price = _optional_number(point[0])
                size = _optional_number(point[1])
            else:
                continue
            if price is None or size is None or price < 0 or price > 1 or size <= 0:
                continue
            parsed.append(OrderBookLevel(price, size))
        parsed.sort(key=lambda level: level.price, reverse=reverse)
        return tuple(parsed)
    try:
        return OrderBookSnapshot(
            timestamp,
            bids=levels(value.get("bids"), reverse=True),
            asks=levels(value.get("asks"), reverse=False),
            token_id=value.get("token_id"),
        )
    except (TypeError, ValueError):
        return None


def _book(item: Any, outcome: str) -> OrderBookSnapshot | None:
    names = ("order_book", "yes_order_book") if outcome == "yes" else ("no_order_book",)
    fallback_timestamp = _value(item, "timestamp") or _value(item, "observed_at")
    for name in names:
        value = _value(item, name)
        book = _coerce_book(value, fallback_timestamp)
        if book is not None:
            return book
    if outcome == "no":
        yes_book = _book(item, "yes")
        if yes_book is not None:
            return _complement_book(yes_book)
    return None


def _resolution_seconds(item: Any) -> float | None:
    raw = _value(
        item,
        "resolution_seconds",
        _value(item, "time_to_resolution_seconds", _value(item, "time_to_expiry_seconds")),
    )
    value = _optional_number(raw)
    if value is not None:
        return max(0.0, value)
    timestamp = parse_timestamp(_value(item, "timestamp")) or parse_timestamp(_value(item, "observed_at"))
    expiry = parse_timestamp(_value(item, "expiry"))
    if timestamp is not None and expiry is not None:
        return max(0.0, (expiry - timestamp).total_seconds())
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
