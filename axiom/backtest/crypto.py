"""Historical OHLCV simulator with next-bar execution and explicit costs."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import math
from typing import Any, Mapping, Sequence

from axiom.domain import MarketType, OHLCVBar, Side, SimulationQuality, ensure_utc
from axiom.metrics import calculate_crypto_metrics
from axiom.portfolio import OrderRequest, Portfolio
from axiom.strategy import StrategyDefinition, evaluate_signal, validate_strategy
from .types import BacktestResult


def _bar_field(bar: Any, name: str, default: float = 0.0) -> float:
    if isinstance(bar, Mapping):
        value = bar.get(name, default)
    else:
        value = getattr(bar, name, default)
    try:
        value = float(value)
    except (TypeError, ValueError):
        return default
    return value if math.isfinite(value) else default


def _bar_time(bar: Any, default: datetime) -> datetime:
    value = bar.get("timestamp", default) if isinstance(bar, Mapping) else getattr(bar, "timestamp", default)
    return ensure_utc(value) if isinstance(value, datetime) else default


def _quality(bars: Sequence[Any]) -> SimulationQuality:
    if len(bars) < 3:
        return SimulationQuality.LOW
    complete = sum(1 for bar in bars if _bar_field(bar, "open") > 0 and _bar_field(bar, "high") > 0 and _bar_field(bar, "low") > 0 and _bar_field(bar, "close") > 0)
    spreads = sum(1 for bar in bars if (isinstance(bar, Mapping) and bar.get("spread") is not None) or getattr(bar, "spread", None) is not None)
    if complete < len(bars) or spreads * 2 < len(bars):
        return SimulationQuality.MEDIUM
    return SimulationQuality.HIGH


@dataclass(slots=True)
class CryptoBacktester:
    initial_cash: float = 10_000.0
    fee_bps: float = 10.0
    slippage_bps: float = 5.0
    allocation: float = 1.0
    symbol: str = "ASSET"

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
        self, bars: Sequence[OHLCVBar | Mapping[str, Any]], strategy: StrategyDefinition | Mapping[str, Any] | str,
        *, symbol: str | None = None, initial_cash: float | None = None,
    ) -> BacktestResult:
        definition = validate_strategy(strategy)
        if definition.market_type is not MarketType.CRYPTO_SPOT:
            raise ValueError("CryptoBacktester requires a crypto_spot strategy")
        rows = sorted(list(bars), key=lambda item: _bar_time(item, datetime.min.replace(tzinfo=timezone.utc)))
        asset = symbol or self.symbol
        portfolio = Portfolio(self.initial_cash if initial_cash is None else initial_cash)
        curve: list[dict[str, Any]] = []
        labels: list[SimulationQuality] = []
        target_fraction = max(0.0, min(1.0, self.allocation))
        for index, bar in enumerate(rows):
            timestamp = _bar_time(bar, datetime.min.replace(tzinfo=timezone.utc))
            opening = _bar_field(bar, "open")
            closing = _bar_field(bar, "close", opening)
            if index > 0 and opening > 0:
                # The signal only sees bars strictly before this execution bar.
                score = evaluate_signal(definition, rows[:index])
                current = portfolio.get_position(asset)
                current_quantity = current.quantity if current else 0.0
                if score > 0:
                    desired_value = max(0.0, portfolio.cash) * target_fraction * min(1.0, score)
                    desired = desired_value / opening if opening else 0.0
                    delta = desired - max(0.0, current_quantity)
                    if delta > 1e-12:
                        portfolio.execute_order(OrderRequest(asset, Side.BUY, delta, MarketType.CRYPTO_SPOT, strategy_id=definition.id), timestamp=timestamp, price=opening, fee_bps=self.fee_bps, slippage_bps=self.slippage_bps)
                elif score < 0 and current_quantity > 0:
                    portfolio.execute_order(OrderRequest(asset, Side.SELL, current_quantity * min(1.0, -score), MarketType.CRYPTO_SPOT, strategy_id=definition.id), timestamp=timestamp, price=opening, fee_bps=self.fee_bps, slippage_bps=self.slippage_bps)
            equity = portfolio.equity({asset: closing})
            label = SimulationQuality.HIGH if _bar_field(bar, "spread", 0.0) > 0 else SimulationQuality.MEDIUM
            labels.append(label)
            curve.append({"timestamp": timestamp, "equity": equity, "cash": portfolio.cash, "close": closing, "quality": label.value})
        # Empty and one-bar data are still valid simulations, but explicitly low quality.
        overall = _quality(rows)
        metrics = calculate_crypto_metrics(curve, fills=portfolio.fills, initial_equity=portfolio.initial_cash)
        unresolved = tuple(asset for asset, position in portfolio.positions.items() if position.quantity)
        return BacktestResult(tuple(curve), tuple(portfolio.fills), overall, metrics, unresolved, {}, tuple(labels))

    simulate = run


# Explicit alias for callers using the longer name from the public contract.
CryptoOHLCVBacktester = CryptoBacktester


__all__ = ["CryptoBacktester", "CryptoOHLCVBacktester"]
