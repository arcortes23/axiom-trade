"""Independent pre-trade and portfolio risk controls.

Risk checks are deliberately outside strategy definitions. A strategy can propose
an order but cannot disable or weaken any control in this module.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import math
from typing import Any, Iterable, Mapping

from .domain import Fill, MarketType, Side, ensure_utc, utc_now


@dataclass(frozen=True, slots=True)
class RiskLimits:
    max_order_notional: float | None = None
    max_account_exposure: float | None = None
    max_market_exposure: float | None = None
    max_strategy_exposure: float | None = None
    max_group_exposure: float | None = None
    max_loss: float | None = None
    max_expected_loss: float | None = None
    max_drawdown: float | None = None
    max_daily_loss: float | None = None
    min_liquidity: float | None = None
    max_spread: float | None = None
    crash_threshold: float | None = None
    cooldown_seconds: float = 0.0
    max_position_fraction: float | None = 0.05
    kelly_fraction: float = 0.25
    max_cvar: float | None = None

    def __post_init__(self) -> None:
        for name in (
            "max_order_notional",
            "max_account_exposure",
            "max_market_exposure",
            "max_strategy_exposure",
            "max_group_exposure",
            "max_loss",
            "max_expected_loss",
            "max_drawdown",
            "max_daily_loss",
            "min_liquidity",
            "max_spread",
            "crash_threshold",
            "cooldown_seconds",
            "max_cvar",
        ):
            value = getattr(self, name)
            if value is not None and (not math.isfinite(float(value)) or float(value) < 0):
                raise ValueError(f"{name} must be finite and non-negative")
        if self.max_position_fraction is not None and (
            not math.isfinite(float(self.max_position_fraction))
            or not 0 <= float(self.max_position_fraction) <= 1
        ):
            raise ValueError("max_position_fraction must be in [0, 1]")
        if not math.isfinite(float(self.kelly_fraction)) or not 0 <= float(self.kelly_fraction) <= 1:
            raise ValueError("kelly_fraction must be in [0, 1]")


def fractional_kelly_size(
    probability: float,
    price: float,
    bankroll: float,
    *,
    payout: float = 1.0,
    fraction: float = 0.25,
    confidence: float = 1.0,
    uncertainty: float = 0.0,
    max_fraction: float = 0.05,
) -> float:
    """Return conservative binary-contract quantity, never full Kelly.

    ``uncertainty`` is a non-negative model-uncertainty score; larger values
    shrink the allocation. ``confidence`` is an independent [0, 1] stability
    multiplier. Both adjustments happen before the hard portfolio-fraction
    cap, so uncertain probabilities cannot silently receive full size.
    """
    values = (probability, price, bankroll, payout, fraction, confidence, uncertainty, max_fraction)
    if not all(math.isfinite(float(value)) for value in values):
        raise ValueError("position-sizing inputs must be finite")
    if bankroll < 0 or payout <= 0 or price <= 0 or price >= payout:
        return 0.0
    if not 0 <= probability <= 1:
        raise ValueError("probability must be in [0, 1]")
    if not 0 <= fraction <= 1 or not 0 <= confidence <= 1 or uncertainty < 0 or not 0 <= max_fraction <= 1:
        raise ValueError("invalid Kelly sizing bound")
    odds = (payout - price) / price
    full_kelly = (probability * odds - (1.0 - probability)) / odds
    if full_kelly <= 0:
        return 0.0
    uncertainty_multiplier = 1.0 / (1.0 + uncertainty)
    allocation_fraction = min(
        max_fraction,
        full_kelly * fraction * confidence * uncertainty_multiplier,
    )
    return bankroll * allocation_fraction / price

@dataclass(frozen=True, slots=True)
class RiskDecision:
    allowed: bool
    reasons: tuple[str, ...] = ()
    notional: float = 0.0
    checks: Mapping[str, bool] = field(default_factory=dict)

    def __bool__(self) -> bool:
        return self.allowed

    @property
    def reason(self) -> str:
        return "; ".join(self.reasons)


class RiskEngine:
    """Authoritative account, exposure, loss, liquidity and emergency controls."""

    def __init__(
        self,
        limits: RiskLimits | None = None,
        *,
        initial_equity: float = 0.0,
        account_id: str = "default",
        correlation_groups: Mapping[str, str] | None = None,
    ) -> None:
        self.limits = limits or RiskLimits()
        self.account_id = account_id
        self.initial_equity = float(initial_equity)
        if not math.isfinite(self.initial_equity) or self.initial_equity < 0:
            raise ValueError("initial_equity must be finite and non-negative")
        self.equity = float(initial_equity)
        self.peak_equity = float(initial_equity)
        self.day_start_equity = float(initial_equity)
        self._day: datetime.date | None = None
        self.emergency_kill_switch = False
        self.cooldown_until: datetime | None = None
        self.correlation_groups = {str(symbol): str(group) for symbol, group in (correlation_groups or {}).items() if str(group)}
        self.market_exposure: dict[str, float] = {}
        self.strategy_exposure: dict[str, float] = {}
        self.group_exposure: dict[str, float] = {}
        self._market_signed: dict[str, float] = {}
        self._strategy_signed: dict[str, float] = {}
        self._group_signed: dict[str, float] = {}
        self.account_exposure = 0.0
        self._last_prices: dict[str, float] = {}
        self._last_check: RiskDecision | None = None
        self.current_cvar: float | None = None

    @property
    def correlated_exposure(self) -> dict[str, float]:
        return dict(self.group_exposure)

    def set_correlation_group(self, symbol: str, group: str) -> None:
        if not str(group).strip():
            raise ValueError("correlation group is required")
        self.correlation_groups[str(symbol)] = str(group)

    @property
    def killed(self) -> bool:
        return self.emergency_kill_switch

    def set_emergency_kill_switch(self, enabled: bool = True) -> None:
        self.emergency_kill_switch = bool(enabled)

    def emergency_stop(self) -> None:
        self.set_emergency_kill_switch(True)

    def reset_emergency(self) -> None:
        # Explicit reset is available to an operator, never to a strategy order.
        self.emergency_kill_switch = False

    def set_cooldown(self, seconds: float | None = None, *, until: datetime | None = None, now: datetime | None = None) -> None:
        if until is not None:
            self.cooldown_until = ensure_utc(until)
        else:
            duration = self.limits.cooldown_seconds if seconds is None else max(0.0, float(seconds))
            self.cooldown_until = ensure_utc(now or utc_now()) + timedelta(seconds=duration)

    def update_equity(self, equity: float, *, timestamp: datetime | None = None) -> RiskDecision:
        value = float(equity)
        if not math.isfinite(value):
            raise ValueError("equity must be finite")
        now = ensure_utc(timestamp or utc_now())
        if self._day != now.date():
            self._day = now.date()
            self.day_start_equity = value
        self.equity = value
        self.peak_equity = max(self.peak_equity, value)
        return self._loss_decision(now)

    def _loss_decision(self, now: datetime) -> RiskDecision:
        losses = self.initial_equity - self.equity
        drawdown = (self.peak_equity - self.equity) / self.peak_equity if self.peak_equity > 0 else 0.0
        daily_loss = self.day_start_equity - self.equity
        reasons: list[str] = []
        if self.limits.max_loss is not None and losses >= self.limits.max_loss:
            reasons.append("max_loss")
        if self.limits.max_drawdown is not None and drawdown >= self.limits.max_drawdown:
            reasons.append("max_drawdown")
        if self.limits.max_daily_loss is not None and daily_loss >= self.limits.max_daily_loss:
            reasons.append("max_daily_loss")
        return RiskDecision(not reasons, tuple(reasons), checks={"max_loss": "max_loss" not in reasons, "max_drawdown": "max_drawdown" not in reasons, "max_daily_loss": "max_daily_loss" not in reasons})

    @staticmethod
    def _order_parts(order: Any, price: float | None, quantity: float | None, symbol: str | None, strategy_id: str | None, group: str | None) -> tuple[str, Side, float, str, str | None, str | None]:
        if isinstance(order, Mapping):
            name = str(order.get("symbol", symbol or ""))
            side_value = order.get("side", Side.BUY)
            side = side_value if isinstance(side_value, Side) else Side(str(side_value))
            qty = float(order.get("quantity", quantity or 0.0))
            px = float(order.get("price", price or 0.0))
            strategy = order.get("strategy_id", strategy_id)
            return name, side, qty, str(order.get("group", group or "")), str(strategy) if strategy else None, str(order.get("market_type", "")) or None
        name = str(getattr(order, "symbol", symbol or ""))
        side_value = getattr(order, "side", Side.BUY)
        side = side_value if isinstance(side_value, Side) else Side(str(side_value))
        qty = float(getattr(order, "quantity", quantity or 0.0))
        px = float(price if price is not None else getattr(order, "price", 0.0))
        strategy = strategy_id or getattr(order, "strategy_id", None)
        return name, side, qty, str(group or ""), str(strategy) if strategy else None, str(getattr(order, "market_type", "")) or None

    def check_order(
        self,
        order: Any,
        *,
        price: float | None = None,
        quantity: float | None = None,
        symbol: str | None = None,
        strategy_id: str | None = None,
        group: str | None = None,
        liquidity: float | None = None,
        spread: float | None = None,
        price_change: float | None = None,
        expected_loss: float | None = None,
        cvar: float | None = None,
        timestamp: datetime | None = None,
    ) -> RiskDecision:
        """Check an order against every active limit; no strategy override exists."""
        now = ensure_utc(timestamp or utc_now())
        name, side, qty, group_name, strategy, _ = self._order_parts(order, price, quantity, symbol, strategy_id, group)
        group_name = group_name or self.correlation_groups.get(name, "")
        px = float(price if price is not None else getattr(order, "price", 0.0) or (order.get("price", 0.0) if isinstance(order, Mapping) else 0.0))
        notional = abs(qty * px)
        reasons: list[str] = []
        checks: dict[str, bool] = {}
        if self.emergency_kill_switch:
            reasons.append("emergency_kill_switch")
        if self.cooldown_until is not None and now < self.cooldown_until:
            reasons.append("cooldown")
        if qty <= 0 or not math.isfinite(qty) or px <= 0 or not math.isfinite(px):
            reasons.append("invalid_order")
        if self.limits.max_order_notional is not None and notional > self.limits.max_order_notional:
            reasons.append("max_order_notional")
        signed_delta = notional if side is Side.BUY else -notional
        current_market = self._market_signed.get(name, 0.0)
        projected_market_signed = current_market + signed_delta
        projected_market = abs(projected_market_signed)
        if self.limits.max_market_exposure is not None and projected_market > self.limits.max_market_exposure:
            reasons.append("max_market_exposure")
        projected_account = self.account_exposure - abs(current_market) + projected_market
        if self.limits.max_account_exposure is not None and projected_account > self.limits.max_account_exposure:
            reasons.append("max_account_exposure")
        if strategy and self.limits.max_strategy_exposure is not None:
            projected_strategy = abs(self._strategy_signed.get(strategy, 0.0) + signed_delta)
            if projected_strategy > self.limits.max_strategy_exposure:
                reasons.append("max_strategy_exposure")
        if group_name and self.limits.max_group_exposure is not None:
            projected_group = abs(self._group_signed.get(group_name, 0.0) + signed_delta)
            if projected_group > self.limits.max_group_exposure:
                reasons.append("max_group_exposure")
        if self.limits.min_liquidity is not None and (liquidity is None or liquidity < self.limits.min_liquidity):
            reasons.append("min_liquidity")
        if self.limits.max_spread is not None and (spread is None or spread > self.limits.max_spread):
            reasons.append("max_spread")
        if self.limits.crash_threshold is not None and price_change is not None and price_change <= -abs(self.limits.crash_threshold):
            reasons.append("crash")
        if expected_loss is None:
            raw_expected_loss = order.get("expected_loss") if isinstance(order, Mapping) else getattr(order, "expected_loss", None)
            expected_loss = float(raw_expected_loss) if raw_expected_loss is not None else None
        if expected_loss is not None and (not math.isfinite(expected_loss) or expected_loss < 0):
            reasons.append("invalid_expected_loss")
        if self.limits.max_expected_loss is not None:
            if expected_loss is None:
                reasons.append("expected_loss_required")
            elif expected_loss > self.limits.max_expected_loss:
                reasons.append("max_expected_loss")
        observed_cvar = self.current_cvar if cvar is None else cvar
        try:
            observed_cvar = float(observed_cvar) if observed_cvar is not None else None
        except (TypeError, ValueError):
            observed_cvar = None
        if observed_cvar is not None and (not math.isfinite(observed_cvar) or observed_cvar < 0):
            reasons.append("invalid_cvar")
        if self.limits.max_cvar is not None:
            if observed_cvar is None:
                reasons.append("cvar_required")
            elif observed_cvar > self.limits.max_cvar:
                reasons.append("max_cvar")
        loss_check = self._loss_decision(now)
        reasons.extend(reason for reason in loss_check.reasons if reason not in reasons)
        checks.update(loss_check.checks)
        for key in ("emergency_kill_switch", "cooldown", "max_order_notional", "max_market_exposure", "max_account_exposure", "max_strategy_exposure", "max_group_exposure", "min_liquidity", "max_spread", "crash", "max_expected_loss"):
            checks[key] = key not in reasons
        checks["cvar"] = "invalid_cvar" not in reasons
        checks["max_cvar"] = not any(reason in reasons for reason in ("cvar_required", "max_cvar"))
        decision = RiskDecision(not reasons, tuple(reasons), notional, checks)
        self._last_check = decision
        return decision

    def record_fill(self, fill: Fill, *, group: str | None = None) -> None:
        notional = abs(fill.quantity * fill.price)
        sign = 1.0 if fill.side is Side.BUY else -1.0
        key = fill.symbol
        self._market_signed[key] = self._market_signed.get(key, 0.0) + sign * notional
        self.market_exposure[key] = abs(self._market_signed[key])
        self.account_exposure = sum(abs(value) for value in self._market_signed.values())
        if fill.strategy_id:
            self._strategy_signed[fill.strategy_id] = self._strategy_signed.get(fill.strategy_id, 0.0) + sign * notional
            self.strategy_exposure[fill.strategy_id] = abs(self._strategy_signed[fill.strategy_id])
        group_name = group or str(fill.metadata.get("group", "")) or self.correlation_groups.get(key, "")
        if group_name:
            self._group_signed[group_name] = self._group_signed.get(group_name, 0.0) + sign * notional
            self.group_exposure[group_name] = abs(self._group_signed[group_name])
        self._last_prices[key] = fill.price
    def update_cvar(
        self,
        returns: Iterable[float],
        *,
        alpha: float = 0.95,
        timestamp: datetime | None = None,
    ) -> RiskDecision:
        """Update the observed return-tail loss and apply the CVaR limit."""
        from .metrics import conditional_value_at_risk

        value = conditional_value_at_risk(returns, alpha=alpha)
        self.current_cvar = value
        now = ensure_utc(timestamp or utc_now())
        loss_decision = self._loss_decision(now)
        reasons = list(loss_decision.reasons)
        if self.limits.max_cvar is not None and value > self.limits.max_cvar:
            reasons.append("max_cvar")
        checks = dict(loss_decision.checks)
        checks["cvar"] = True
        checks["max_cvar"] = "max_cvar" not in reasons
        decision = RiskDecision(not reasons, tuple(dict.fromkeys(reasons)), 0.0, checks)
        self._last_check = decision
        return decision


    on_fill = record_fill
    def position_size(
        self,
        probability: float,
        price: float,
        *,
        bankroll: float | None = None,
        payout: float = 1.0,
        confidence: float = 1.0,
        uncertainty: float = 0.0,
        max_fraction: float | None = None,
    ) -> float:
        """Size a prediction position under fractional Kelly and risk caps."""
        capital = self.equity if bankroll is None else float(bankroll)
        cap = self.limits.max_position_fraction if max_fraction is None else max_fraction
        quantity = fractional_kelly_size(
            probability,
            price,
            capital,
            payout=payout,
            fraction=self.limits.kelly_fraction,
            confidence=confidence,
            uncertainty=uncertainty,
            max_fraction=0.05 if cap is None else cap,
        )
        if self.limits.max_order_notional is not None:
            quantity = min(quantity, self.limits.max_order_notional / price if price > 0 else 0.0)
        return quantity

    size_prediction = position_size
    conservative_size = position_size


    def status(self) -> dict[str, Any]:
        now = ensure_utc(utc_now())
        loss = self.initial_equity - self.equity
        drawdown = (self.peak_equity - self.equity) / self.peak_equity if self.peak_equity else 0.0
        loss_decision = self._loss_decision(now)
        reasons = list(loss_decision.reasons)
        if self.limits.max_cvar is not None:
            if self.current_cvar is None:
                reasons.append("cvar_required")
            elif self.current_cvar > self.limits.max_cvar:
                reasons.append("max_cvar")
        if self.emergency_kill_switch:
            reasons.append("emergency_kill_switch")
        if self.cooldown_until is not None and now < self.cooldown_until:
            reasons.append("cooldown")
        return {
            "allowed": not reasons,
            "reasons": tuple(dict.fromkeys(reasons)),
            "emergency_kill_switch": self.emergency_kill_switch,
            "cooldown_until": self.cooldown_until.isoformat() if self.cooldown_until else None,
            "equity": self.equity,
            "loss": loss,
            "drawdown": drawdown,
            "cvar": self.current_cvar,
            "max_cvar": self.limits.max_cvar,
            "account_exposure": self.account_exposure,
            "market_exposure": dict(self.market_exposure),
            "strategy_exposure": dict(self.strategy_exposure),
            "group_exposure": dict(self.group_exposure),
        }

RiskCheck = RiskDecision
Limits = RiskLimits


__all__ = ["Limits", "RiskCheck", "RiskDecision", "RiskEngine", "RiskLimits", "fractional_kelly_size"]
