"""Benchmark baselines used to contextualize research candidates.

Benchmarks are deliberately small and deterministic.  They do not choose
strategies or touch the locked holdout; callers decide which split to pass.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import math
from statistics import mean
from typing import Any, Mapping, Sequence

from .domain import MarketType, OHLCVBar, PredictionMarketSnapshot, parse_timestamp
from .metrics import (
    brier_score,
    calculate_crypto_metrics,
    expected_calibration_error,
    log_loss,
)


@dataclass(frozen=True, slots=True)
class BenchmarkResult:
    name: str
    market_type: MarketType
    metrics: Mapping[str, float]
    assumptions: Mapping[str, Any]

    def as_record(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "market_type": self.market_type.value,
            "metrics": dict(self.metrics),
            "assumptions": dict(self.assumptions),
        }


def crypto_benchmarks(
    bars: Sequence[OHLCVBar | Mapping[str, Any]],
    *,
    initial_cash: float = 10_000.0,
    fee_bps: float = 10.0,
    slippage_bps: float = 5.0,
    symbol: str = "asset",
) -> tuple[BenchmarkResult, ...]:
    """Return cash, buy-and-hold, and equal-notional DCA baselines."""
    values = (float(initial_cash), float(fee_bps), float(slippage_bps))
    if (
        not all(math.isfinite(value) for value in values)
        or values[0] < 0
        or values[1] < 0
        or values[2] < 0
    ):
        raise ValueError("benchmark cash and costs must be finite and non-negative")
    rows = [
        row
        for row in bars
        if _stamp(row) != datetime.min.replace(tzinfo=timezone.utc)
        and _number(row, "open") > 0
        and _number(row, "close", _number(row, "open")) > 0
    ]
    rows.sort(key=lambda row: _stamp(row))
    if not rows:
        empty = calculate_crypto_metrics([], initial_equity=initial_cash)
        return tuple(
            BenchmarkResult(name, MarketType.CRYPTO_SPOT, empty, {"symbol": symbol})
            for name in ("cash", "buy_hold", "dca")
        )
    cash_curve = [
        {"timestamp": _stamp(row), "equity": float(initial_cash)}
        for row in rows
    ]
    opens = [_number(row, "open") for row in rows]
    closes = [_number(row, "close", opens[index]) for index, row in enumerate(rows)]
    fee_rate = max(0.0, float(fee_bps)) / 10_000.0
    slip_rate = max(0.0, float(slippage_bps)) / 10_000.0
    first_price = opens[0] * (1.0 + slip_rate)
    quantity = initial_cash / (first_price * (1.0 + fee_rate)) if first_price > 0 else 0.0
    cash_after_buy = max(0.0, initial_cash - quantity * first_price * (1.0 + fee_rate))
    hold_curve = [
        {"timestamp": _stamp(row), "equity": cash_after_buy + quantity * close}
        for row, close in zip(rows, closes)
    ]
    allocation = initial_cash / len(rows)
    dca_quantity = 0.0
    dca_cash = initial_cash
    dca_curve: list[dict[str, Any]] = []
    for row, opening, close in zip(rows, opens, closes):
        executable = opening * (1.0 + slip_rate)
        spend = min(allocation, dca_cash)
        if executable > 0:
            bought = spend / (executable * (1.0 + fee_rate))
            dca_quantity += bought
            dca_cash -= bought * executable * (1.0 + fee_rate)
        dca_curve.append({"timestamp": _stamp(row), "equity": dca_cash + dca_quantity * close})
    assumptions = {
        "symbol": symbol,
        "initial_cash": float(initial_cash),
        "fee_bps": float(fee_bps),
        "slippage_bps": float(slippage_bps),
        "execution": "bar_open_entry_with_fee_and_slippage; close_mark_to_market",
    }
    return (
        BenchmarkResult("cash", MarketType.CRYPTO_SPOT, calculate_crypto_metrics(cash_curve, initial_equity=initial_cash), assumptions),
        BenchmarkResult("buy_hold", MarketType.CRYPTO_SPOT, calculate_crypto_metrics(hold_curve, initial_equity=initial_cash), assumptions),
        BenchmarkResult("dca", MarketType.CRYPTO_SPOT, calculate_crypto_metrics(dca_curve, initial_equity=initial_cash), assumptions),
    )


def prediction_benchmarks(
    records: Sequence[Mapping[str, Any]],
    *,
    bins: int = 10,
) -> tuple[BenchmarkResult, ...]:
    """Return market-mid, fixed 0.5, and supplied-model calibration baselines."""
    market_rows: list[dict[str, Any]] = []
    model_rows: list[dict[str, Any]] = []
    for row in records:
        if not isinstance(row, Mapping):
            continue
        outcome = row.get("outcome", row.get("actual", row.get("y", row.get("settlement"))))
        model_value = row.get("model_probability")
        if model_value is None:
            model_value = row.get("probability")
        for candidate, target in (
            (row.get("yes_mid"), market_rows),
            (model_value, model_rows),
        ):
            try:
                probability = float(candidate)
            except (TypeError, ValueError):
                continue
            if not 0.0 <= probability <= 1.0:
                continue
            target.append({"probability": probability, "outcome": outcome})
    constant_rows = [
        {"probability": 0.5, "outcome": row.get("outcome", row.get("actual", row.get("y", row.get("settlement"))))}
        for row in records
        if isinstance(row, Mapping)
    ]
    def result(name: str, rows: Sequence[Mapping[str, Any]]) -> BenchmarkResult:
        return BenchmarkResult(
            name,
            MarketType.PREDICTION,
            {
                "brier": brier_score(rows),
                "log_loss": log_loss(rows),
                "ece": expected_calibration_error(rows, bins=bins),
                "observations": float(sum(1 for row in rows if _valid_binary(row))),
            },
            {"bins": bins, "execution": "no trading; forecast-only"},
        )
    return (result("market_mid", market_rows), result("constant_0.5", constant_rows), result("model", model_rows))


def _valid_binary(row: Mapping[str, Any]) -> bool:
    outcome = row.get("outcome", row.get("actual", row.get("y", row.get("settlement"))))
    if isinstance(outcome, str):
        return outcome.strip().lower() in {"0", "1", "yes", "no", "true", "false", "resolved_yes", "resolved_no"}
    try:
        number = float(outcome)
    except (TypeError, ValueError):
        return False
    return math.isfinite(number) and number in {0.0, 1.0}


def _stamp(row: Any) -> datetime:
    value = row.get("timestamp") if isinstance(row, Mapping) else getattr(row, "timestamp", None)
    return parse_timestamp(value) or datetime.min.replace(tzinfo=timezone.utc)


def _number(row: Any, name: str, default: float = 0.0) -> float:
    value = row.get(name, default) if isinstance(row, Mapping) else getattr(row, name, default)
    try:
        value = float(value)
    except (TypeError, ValueError):
        return default
    return value if math.isfinite(value) else default


__all__ = ["BenchmarkResult", "crypto_benchmarks", "prediction_benchmarks"]
