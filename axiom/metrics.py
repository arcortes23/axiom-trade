"""Pure, dependency-free performance and probability metrics."""
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timedelta
import math
from statistics import mean, pstdev
from typing import Any, Iterable, Mapping, Sequence

from .domain import Fill, parse_timestamp



def _equities(data: Iterable[Any]) -> list[float]:
    values: list[float] = []
    for item in data:
        value = item.get("equity") if isinstance(item, Mapping) else item
        try:
            value = float(value)
        except (TypeError, ValueError):
            continue
        if math.isfinite(value):
            values.append(value)
    return values


def _returns(equity: Sequence[float]) -> list[float]:
    return [current / previous - 1.0 for previous, current in zip(equity, equity[1:]) if previous not in (0, None)]
def _finite_values(values: Iterable[float]) -> list[float]:
    result: list[float] = []
    for value in values:
        try:
            number = float(value)
        except (TypeError, ValueError):
            continue
        if math.isfinite(number):
            result.append(number)
    return result


def max_drawdown(equity_curve: Iterable[Any]) -> float:
    values = _equities(equity_curve)
    if not values:
        return 0.0
    peak, drawdown = values[0], 0.0
    for value in values:
        peak = max(peak, value)
        drawdown = max(drawdown, (peak - value) / peak if peak > 0 else 0.0)
    return drawdown
def drawdown_series(equity_curve: Iterable[Any]) -> list[float]:
    values = _equities(equity_curve)
    peak, output = 0.0, []
    for value in values:
        peak = max(peak, value)
        output.append((peak - value) / peak if peak > 0 else 0.0)
    return output


def sharpe_ratio(returns: Iterable[float], *, periods_per_year: float = 252.0, risk_free: float = 0.0) -> float:
    try:
        periods = float(periods_per_year)
        risk_free_value = float(risk_free)
    except (TypeError, ValueError) as exc:
        raise ValueError("periods_per_year and risk_free must be numeric") from exc
    if not math.isfinite(periods) or periods <= 0 or not math.isfinite(risk_free_value):
        raise ValueError("periods_per_year must be finite and positive; risk_free must be finite")
    values = _finite_values(returns)
    if len(values) < 2:
        return 0.0
    excess = [value - risk_free_value / periods for value in values]
    deviation = pstdev(excess)
    return mean(excess) / deviation * math.sqrt(periods) if deviation > 0 else 0.0


def sortino_ratio(returns: Iterable[float], *, periods_per_year: float = 252.0, target: float = 0.0) -> float:
    try:
        periods = float(periods_per_year)
        target_value = float(target)
    except (TypeError, ValueError) as exc:
        raise ValueError("periods_per_year and target must be numeric") from exc
    if not math.isfinite(periods) or periods <= 0 or not math.isfinite(target_value):
        raise ValueError("periods_per_year must be finite and positive; target must be finite")
    values = _finite_values(returns)
    downside = [min(0.0, value - target_value) for value in values]
    deviation = math.sqrt(mean(value * value for value in downside)) if downside else 0.0
    return (mean(values) - target_value) / deviation * math.sqrt(periods) if deviation > 0 else 0.0
def conditional_value_at_risk(
    returns: Iterable[float],
    *,
    alpha: float = 0.95,
) -> float:
    """Return expected loss in the worst ``1 - alpha`` return tail.

    Returns are expressed as fractional period returns. The result is a
    non-negative loss fraction; a positive return in a very small sample is
    still included when it falls inside the requested tail.
    """
    alpha = float(alpha)
    if not math.isfinite(alpha) or not 0.0 <= alpha < 1.0:
        raise ValueError("alpha must be finite and in [0, 1)")
    losses: list[float] = []
    for value in returns:
        try:
            number = float(value)
        except (TypeError, ValueError):
            continue
        if math.isfinite(number):
            losses.append(-number)
    if not losses:
        return 0.0
    losses.sort(reverse=True)
    tail_count = max(1, int(math.ceil((1.0 - alpha) * len(losses))))
    return max(0.0, mean(losses[:tail_count]))


cvar = conditional_value_at_risk
expected_shortfall = conditional_value_at_risk


def _closed_trade_pnls(fills: Sequence[Fill]) -> list[float]:
    lots: dict[tuple[str, str, str | None], list[list[float]]] = {}
    realized: list[float] = []
    for item in fills:
        outcome = None
        if item.market_type.value == "prediction":
            outcome = str(item.metadata.get("outcome", "yes")).strip().lower() or "yes"
        lot_key = (item.symbol, item.market_type.value, outcome)
        if item.side.value == "buy":
            lots.setdefault(lot_key, []).append([item.quantity, item.price, item.fees])
            continue
        remaining = item.quantity
        symbol_lots = lots.setdefault(lot_key, [])
        while remaining > 1e-12 and symbol_lots:
            lot_quantity, lot_price, lot_fees = symbol_lots[0]
            matched = min(remaining, lot_quantity)
            fees = lot_fees * matched / lot_quantity + item.fees * matched / item.quantity if item.quantity else 0.0
            realized.append((item.price - lot_price) * matched - fees)
            remaining -= matched
            lot_quantity -= matched
            lot_fees -= lot_fees * matched / (lot_quantity + matched)
            if lot_quantity <= 1e-12:
                symbol_lots.pop(0)
            else:
                symbol_lots[0] = [lot_quantity, lot_price, lot_fees]
    return realized


def profit_factor(trade_pnls: Iterable[float]) -> float:
    values = _finite_values(trade_pnls)
    gains = sum(value for value in values if value > 0)
    losses = -sum(value for value in values if value < 0)
    return gains / losses if losses > 0 else 0.0


def longest_losing_streak(trade_pnls: Iterable[float]) -> int:
    longest = current = 0
    for value in _finite_values(trade_pnls):
        if value < 0:
            current += 1
            longest = max(longest, current)
        else:
            current = 0
    return longest


def calmar_ratio(annualized_return_value: float, drawdown: float) -> float:
    return float(annualized_return_value) / float(drawdown) if drawdown > 0 else 0.0


def calculate_crypto_metrics(
    equity_curve: Iterable[Any],
    *,
    fills: Sequence[Fill] = (),
    initial_equity: float | None = None,
) -> dict[str, float]:
    values = _equities(equity_curve)
    returns = _returns(values)
    start = initial_equity if initial_equity is not None else (values[0] if values else 0.0)
    end = values[-1] if values else start
    drawdown = max_drawdown(values)
    annualized = (end / start) ** (252.0 / max(1, len(returns))) - 1.0 if start > 0 and end >= 0 else 0.0
    trade_pnls = _closed_trade_pnls(fills)
    turnover = sum(abs(item.price * item.quantity) for item in fills)
    total_fees = sum(item.fees for item in fills)
    total_slippage = sum(abs(item.slippage * item.quantity) for item in fills)
    cvar = conditional_value_at_risk(returns)
    result = {
        "initial_equity": float(start),
        "final_equity": float(end),
        "total_return": (end / start - 1.0) if start else 0.0,
        "annualized_return": annualized,
        "cagr": annualized,
        "max_drawdown": drawdown,
        "calmar": calmar_ratio(annualized, drawdown),
        "volatility": pstdev(returns) * math.sqrt(252.0) if len(returns) > 1 else 0.0,
        "cvar": cvar,
        "sharpe": sharpe_ratio(returns),
        "sortino": sortino_ratio(returns),
        "profit_factor": profit_factor(trade_pnls),
        "expectancy": mean(trade_pnls) if trade_pnls else 0.0,
        "volatility_of_returns": pstdev(returns) if len(returns) > 1 else 0.0,
        "turnover": turnover,
        "fees": total_fees,
        "slippage": total_slippage,
        "fills": float(len(fills)),
        "closed_trades": float(len(trade_pnls)),
        "longest_losing_streak": float(longest_losing_streak(trade_pnls)),
    }
    result["win_rate"] = sum(1 for value in trade_pnls if value > 0) / len(trade_pnls) if trade_pnls else 0.0
    return result


def _probability_outcome(item: Any) -> tuple[float, float] | None:
    if isinstance(item, Mapping):
        probability = item.get("probability", item.get("p", item.get("forecast")))
        outcome = item.get("outcome", item.get("actual", item.get("y")))
    elif isinstance(item, Sequence) and not isinstance(item, (str, bytes)) and len(item) >= 2:
        probability, outcome = item[0], item[1]
    else:
        return None
    try:
        p = float(probability)
    except (TypeError, ValueError):
        return None
    if isinstance(outcome, str):
        normalized = outcome.strip().lower()
        if normalized in {"1", "true", "yes", "y", "resolved_yes"}:
            y = 1.0
        elif normalized in {"0", "false", "no", "n", "resolved_no"}:
            y = 0.0
        else:
            return None
    else:
        try:
            y = float(outcome)
        except (TypeError, ValueError):
            return None
    if not math.isfinite(p) or not 0.0 <= p <= 1.0 or not math.isfinite(y) or y not in {0.0, 1.0}:
        return None
    return p, y


def brier_score(predictions: Iterable[Any]) -> float:
    values = [_probability_outcome(item) for item in predictions]
    values = [item for item in values if item is not None]
    return mean((probability - outcome) ** 2 for probability, outcome in values) if values else 0.0


def log_loss(predictions: Iterable[Any], *, epsilon: float = 1e-15) -> float:
    try:
        epsilon_value = float(epsilon)
    except (TypeError, ValueError) as exc:
        raise ValueError("epsilon must be numeric and in (0, 0.5)") from exc
    if not math.isfinite(epsilon_value) or not 0.0 < epsilon_value < 0.5:
        raise ValueError("epsilon must be numeric and in (0, 0.5)")
    values = [_probability_outcome(item) for item in predictions]
    values = [item for item in values if item is not None]
    if not values:
        return 0.0
    return -mean(
        outcome * math.log(max(epsilon_value, min(1 - epsilon_value, probability)))
        + (1 - outcome) * math.log(max(epsilon_value, min(1 - epsilon_value, 1 - probability)))
        for probability, outcome in values
    )


def calibration_buckets(predictions: Iterable[Any], *, bins: int = 10) -> list[dict[str, float]]:
    if not isinstance(bins, int) or isinstance(bins, bool) or bins < 1:
        raise ValueError("bins must be a positive integer")
    result = [{"lower": index / bins, "upper": (index + 1) / bins, "count": 0.0, "mean_probability": 0.0, "frequency": 0.0, "gap": 0.0} for index in range(bins)]
    grouped: list[list[tuple[float, float]]] = [[] for _ in range(bins)]
    for item in predictions:
        parsed = _probability_outcome(item)
        if parsed is None:
            continue
        probability, outcome = parsed
        index = min(bins - 1, int(probability * bins))
        grouped[index].append((probability, outcome))
    for index, values in enumerate(grouped):
        if values:
            average_p, frequency = mean(item[0] for item in values), mean(item[1] for item in values)
            result[index].update(count=float(len(values)), mean_probability=average_p, frequency=frequency, gap=abs(average_p - frequency))
    return result


def expected_calibration_error(predictions: Iterable[Any], *, bins: int = 10) -> float:
    buckets = calibration_buckets(predictions, bins=bins)
    total = sum(bucket["count"] for bucket in buckets)
    return sum(bucket["gap"] * bucket["count"] for bucket in buckets) / total if total else 0.0

# Common abbreviations/aliases.
ece = expected_calibration_error
calibration_error = expected_calibration_error
def calibration_at_horizons(
    records: Iterable[Any],
    *,
    horizons: Mapping[str, timedelta | int | float] | Sequence[tuple[str, timedelta | int | float]] | None = None,
    bins: int = 10,
) -> dict[str, dict[str, float | int | str | None]]:
    """Measure forecast calibration at fixed pre-expiry horizons.

    Each input row must provide ``timestamp``, ``expiry``, a probability, and a
    terminal binary outcome (or a terminal ``settlement`` value).  For each
    market and horizon, only the latest forecast at or before
    ``expiry - horizon`` is selected.  Unsupported horizons report zero
    observations rather than borrowing a nearer timestamp.
    """
    if horizons is None:
        horizon_items: list[tuple[str, timedelta | int | float]] = [
            ("1d", timedelta(days=1)),
            ("7d", timedelta(days=7)),
            ("30d", timedelta(days=30)),
        ]
    elif isinstance(horizons, Mapping):
        horizon_items = [(str(label), value) for label, value in horizons.items()]
    else:
        horizon_items = [(str(label), value) for label, value in horizons]
    normalized: list[tuple[str, timedelta]] = []
    for label, value in horizon_items:
        if isinstance(value, timedelta):
            delta = value
        else:
            try:
                delta = timedelta(seconds=float(value))
            except (TypeError, ValueError, OverflowError) as exc:
                raise ValueError(f"invalid horizon {label!r}") from exc
        if delta <= timedelta(0):
            raise ValueError(f"horizon {label!r} must be positive")
        normalized.append((label, delta))
    grouped: dict[str, list[dict[str, Any]]] = {}
    for index, item in enumerate(records):
        if not isinstance(item, Mapping):
            continue
        stamp = parse_timestamp(item.get("timestamp"))
        expiry = parse_timestamp(item.get("expiry"))
        if stamp is None or expiry is None:
            continue
        parsed = _probability_outcome(
            {
                "probability": item.get("probability", item.get("p", item.get("forecast"))),
                "outcome": item.get(
                    "outcome",
                    item.get("actual", item.get("y", item.get("settlement"))),
                ),
            }
        )
        if parsed is None:
            continue
        probability, outcome = parsed
        market_id = str(item.get("market_id", item.get("key", index)))
        grouped.setdefault(market_id, []).append(
            {"timestamp": stamp, "expiry": expiry, "probability": probability, "outcome": outcome}
        )
    result: dict[str, dict[str, float | int | str | None]] = {}
    for label, delta in normalized:
        selected: list[dict[str, float]] = []
        for rows in grouped.values():
            eligible = [
                row
                for row in rows
                if row["timestamp"] <= row["expiry"] - delta
            ]
            if not eligible:
                continue
            selected.append(max(eligible, key=lambda row: row["timestamp"]))
        observations = [
            {"probability": row["probability"], "outcome": row["outcome"]}
            for row in selected
        ]
        result[label] = {
            "horizon_seconds": int(delta.total_seconds()),
            "count": len(observations),
            "brier": brier_score(observations),
            "log_loss": log_loss(observations),
            "ece": expected_calibration_error(observations, bins=bins),
        }
    return result


def expected_value(
    probability: float,
    market_price: float,
    *,
    executable_price: float | None = None,
    payout: float = 1.0,
    fees: float = 0.0,
    slippage: float = 0.0,
) -> dict[str, float]:
    """Return raw and executable value for one binary contract.

    ``market_price`` is the observed quote and ``executable_price`` is the
    price actually available to the simulated order. Costs are per contract.
    The default payout is one unit, so the raw edge is ``probability-price``.
    """
    try:
        probability = float(probability)
        market_price = float(market_price)
        executable = market_price if executable_price is None else float(executable_price)
        payout = float(payout)
        fees = float(fees)
        slippage = float(slippage)
    except (TypeError, ValueError) as exc:
        raise ValueError("expected-value inputs must be numeric") from exc
    if not math.isfinite(probability) or not 0.0 <= probability <= 1.0:
        raise ValueError("probability must be finite and in [0, 1]")
    if not all(math.isfinite(value) for value in (market_price, executable, payout, fees, slippage)):
        raise ValueError("expected-value inputs must be finite")
    if payout <= 0 or market_price < 0 or executable < 0 or fees < 0 or slippage < 0:
        raise ValueError("payout, prices, and costs must be non-negative")
    raw_ev = probability * payout - market_price
    executable_ev = probability * payout - executable - fees - slippage
    raw_roi = raw_ev / market_price if market_price > 0 else 0.0
    executable_cost = executable + fees + slippage
    executable_roi = executable_ev / executable_cost if executable_cost > 0 else 0.0
    return {
        "raw_ev": raw_ev,
        "executable_ev": executable_ev,
        "raw_edge": raw_ev / payout,
        "executable_edge": executable_ev / payout,
        "raw_roi": raw_roi,
        "executable_roi": executable_roi,
        "market_price": market_price,
        "executable_price": executable,
        "fees": fees,
        "slippage": slippage,
    }


def prediction_expected_value(
    probability: float,
    market_price: float,
    *,
    executable_price: float | None = None,
    payout: float = 1.0,
    fees: float = 0.0,
    slippage: float = 0.0,
) -> dict[str, float]:
    return expected_value(
        probability,
        market_price,
        executable_price=executable_price,
        payout=payout,
        fees=fees,
        slippage=slippage,
    )


def calculate_prediction_metrics(
    records: Iterable[Any],
    *,
    fills: Sequence[Fill] = (),
    probabilities: Iterable[Any] | None = None,
    initial_equity: float | None = None,
) -> dict[str, float]:
    values = _equities(records)
    start = initial_equity if initial_equity is not None else (values[0] if values else 0.0)
    end = values[-1] if values else start
    result = {
        "initial_equity": float(start),
        "final_equity": float(end),
        "roi": (end / start - 1.0) if start else 0.0,
        "fills": float(len(fills)),
    }
    observations = list(probabilities) if probabilities is not None else []
    if probabilities is None:
        observations = [
            {"probability": fill.expected_probability, "outcome": fill.metadata.get("outcome_value")}
            for fill in fills
            if fill.expected_probability is not None and "outcome_value" in fill.metadata
        ]
    if observations:
        result.update({
            "brier": brier_score(observations),
            "log_loss": log_loss(observations),
            "ece": expected_calibration_error(observations),
        })
    else:
        result.update({"brier": 0.0, "log_loss": 0.0, "ece": 0.0})
    edges = []
    for fill in fills:
        if fill.expected_probability is None:
            continue
        cost_per_contract = fill.fees / fill.quantity if fill.quantity > 0 else fill.fees
        execution_price = fill.executable_probability if fill.executable_probability is not None else fill.price
        raw_price = fill.metadata.get("reference_price", fill.price)
        try:
            raw_price = float(raw_price)
        except (TypeError, ValueError):
            raw_price = fill.price
        if not math.isfinite(raw_price) or raw_price < 0:
            raw_price = fill.price
        edges.append(
            expected_value(
                fill.expected_probability,
                raw_price,
                executable_price=execution_price,
                fees=cost_per_contract,
            )
        )
    result["raw_edge"] = mean(item["raw_edge"] for item in edges) if edges else 0.0
    result["executable_edge"] = mean(item["executable_edge"] for item in edges) if edges else 0.0
    result["expected_value"] = mean(item["executable_ev"] for item in edges) if edges else 0.0
    result["expected_roi"] = mean(item["executable_roi"] for item in edges) if edges else 0.0
    return result

crypto_metrics = calculate_crypto_metrics
prediction_metrics = calculate_prediction_metrics
brier = brier_score
ece_score = expected_calibration_error


__all__ = [
    "brier",
    "brier_score",
    "calibration_at_horizons",
    "calibration_error",
    "calculate_crypto_metrics",
    "calculate_prediction_metrics",
    "calmar_ratio",
    "conditional_value_at_risk",
    "crypto_metrics",
    "cvar",
    "ece",
    "expected_shortfall",
    "ece_score",
    "expected_calibration_error",
    "expected_value",
    "log_loss",
    "longest_losing_streak",
    "max_drawdown",
    "prediction_expected_value",
    "prediction_metrics",
    "profit_factor",
    "sharpe_ratio",
    "sortino_ratio",
]
