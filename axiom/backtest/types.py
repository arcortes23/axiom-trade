"""Shared offline backtest result records."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from axiom.domain import Fill, ResearchQuality, SimulationQuality


@dataclass(frozen=True, slots=True)
class BacktestResult:
    equity_curve: tuple[dict[str, Any], ...]
    fills: tuple[Fill, ...]
    quality: SimulationQuality
    metrics: dict[str, float] = field(default_factory=dict)
    unresolved: tuple[str, ...] = ()
    outcomes: dict[str, str] = field(default_factory=dict)
    quality_labels: tuple[SimulationQuality, ...] = ()
    research_quality: ResearchQuality = ResearchQuality.PRICE_PROXY

    @property
    def final_equity(self) -> float:
        return float(self.equity_curve[-1].get("equity", 0.0)) if self.equity_curve else 0.0

    @property
    def trades(self) -> int:
        return len(self.fills)


__all__ = ["BacktestResult"]
