"""Deterministic overlapping market-regime detection."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import math
from statistics import mean, pstdev
from typing import Any, Mapping, Sequence

from .domain import MarketType, PredictionMarketSnapshot, OHLCVBar, ensure_utc


class RegimeState(str, Enum):
    BULLISH = "bullish"
    BEARISH = "bearish"
    STRONG_BULL = "strong_bull"
    BULL = "bull"
    SIDEWAYS = "sideways"
    BEAR = "bear"
    STRONG_BEAR = "strong_bear"
    TRENDING = "trending"
    RANGE_BOUND = "range_bound"
    NORMAL_VOLATILITY = "normal_volatility"
    HIGH_VOLATILITY = "high_volatility"
    EXTREME_VOLATILITY = "extreme_volatility"
    CRASH = "crash"
    LIQUID = "liquid"
    ILLIQUID = "illiquid"
    HIGH_LIQUIDITY = "high_liquidity"
    LOW_LIQUIDITY = "low_liquidity"
    EARLY_MARKET = "early_market"
    MATURE_MARKET = "mature_market"
    NEAR_EXPIRY = "near_expiry"
    HIGH_INFORMATION_FLOW = "high_information_flow"
    SUDDEN_REPRICING = "sudden_repricing"
    STABLE_PROBABILITY = "stable_probability"
    EXTREME_TAIL = "extreme_tail"
    SPREAD_WIDENING = "spread_widening"
    HIGH_MOMENTUM = "high_momentum"
    HIGH_DISAGREEMENT = "high_disagreement"
    EVENT_RICH = "event_rich"
    STALE = "stale"


@dataclass(frozen=True, slots=True)
class Regime:
    state: RegimeState
    market_type: MarketType
    confidence: float
    evidence: Mapping[str, float] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if not isinstance(self.state, RegimeState):
            object.__setattr__(self, "state", RegimeState(str(self.state)))
        if not isinstance(self.market_type, MarketType):
            object.__setattr__(self, "market_type", MarketType(str(self.market_type)))
        confidence = float(self.confidence)
        if not math.isfinite(confidence) or not 0.0 <= confidence <= 1.0:
            raise ValueError("regime confidence must be in [0, 1]")
        object.__setattr__(self, "confidence", confidence)
        if self.evidence is None:
            object.__setattr__(self, "evidence", {})
    @property
    def label(self) -> str:
        return self.state.value

    def __str__(self) -> str:
        return self.state.value


@dataclass(frozen=True, slots=True)
class RegimeSnapshot:
    market_type: MarketType
    regimes: tuple[Regime, ...]
    timestamp: datetime | None = None
    def __post_init__(self) -> None:
        if not isinstance(self.market_type, MarketType):
            object.__setattr__(self, "market_type", MarketType(str(self.market_type)))
        object.__setattr__(self, "regimes", tuple(self.regimes))
        if self.timestamp is not None:
            object.__setattr__(self, "timestamp", ensure_utc(self.timestamp))

    @property
    def states(self) -> tuple[RegimeState, ...]:
        return tuple(regime.state for regime in self.regimes)

    def __iter__(self):
        return iter(self.regimes)

    def __contains__(self, value: object) -> bool:
        return any(value in (regime.state, regime.state.value, regime) for regime in self.regimes)


class RegimeEngine:
    """Classify independent dimensions without forcing one mutually-exclusive label."""

    def __init__(
        self,
        *,
        trend_threshold: float = 0.03,
        volatility_threshold: float = 0.04,
        crash_threshold: float = 0.10,
        near_expiry_seconds: float = 86_400.0,
        liquidity_threshold: float = 0.0,
        strong_trend_threshold: float | None = None,
        extreme_volatility_threshold: float | None = None,
        repricing_threshold: float | None = None,
        extreme_tail_threshold: float = 0.05,
        spread_widening_ratio: float = 0.10,
        early_market_fraction: float = 0.25,
    ) -> None:
        values = (
            trend_threshold,
            volatility_threshold,
            crash_threshold,
            near_expiry_seconds,
            liquidity_threshold,
            extreme_tail_threshold,
            spread_widening_ratio,
            early_market_fraction,
            *(value for value in (strong_trend_threshold, extreme_volatility_threshold, repricing_threshold) if value is not None),
        )
        if not all(math.isfinite(float(value)) for value in values) or any(float(value) < 0 for value in values):
            raise ValueError("regime thresholds must be finite and non-negative")
        if not 0.0 <= float(extreme_tail_threshold) <= 0.5 or not 0.0 <= float(early_market_fraction) <= 1.0:
            raise ValueError("regime fractions are out of range")
        self.trend_threshold = max(0.0, float(trend_threshold))
        self.volatility_threshold = max(0.0, float(volatility_threshold))
        self.crash_threshold = max(0.0, float(crash_threshold))
        self.near_expiry_seconds = max(0.0, float(near_expiry_seconds))
        self.liquidity_threshold = max(0.0, float(liquidity_threshold))
        self.strong_trend_threshold = max(
            self.trend_threshold * 2.0,
            float(strong_trend_threshold) if strong_trend_threshold is not None else self.trend_threshold * 2.0,
        )
        self.extreme_volatility_threshold = max(
            self.volatility_threshold * 2.0,
            float(extreme_volatility_threshold)
            if extreme_volatility_threshold is not None
            else self.volatility_threshold * 2.0,
        )
        self.repricing_threshold = max(
            self.trend_threshold,
            float(repricing_threshold) if repricing_threshold is not None else self.trend_threshold,
        )
        self.extreme_tail_threshold = max(0.0, min(0.5, float(extreme_tail_threshold)))
        self.spread_widening_ratio = max(0.0, float(spread_widening_ratio))
        self.early_market_fraction = max(0.0, min(1.0, float(early_market_fraction)))

    @staticmethod
    def _number(item: Any, name: str, default: float = 0.0) -> float:
        value = item.get(name, default) if isinstance(item, Mapping) else getattr(item, name, default)
        try:
            value = float(value)
        except (TypeError, ValueError):
            return default
        return value if math.isfinite(value) else default

    def detect_crypto(self, bars: Sequence[OHLCVBar | Mapping[str, Any]]) -> RegimeSnapshot:
        if not bars:
            return RegimeSnapshot(MarketType.CRYPTO_SPOT, ())
        values = [self._number(bar, "close") for bar in bars]
        returns = [b / a - 1.0 for a, b in zip(values, values[1:]) if a > 0]
        trend = values[-1] / values[0] - 1.0 if len(values) > 1 and values[0] > 0 else 0.0
        volatility = pstdev(returns) if len(returns) > 1 else 0.0
        states: list[Regime] = []
        trend_confidence = min(1.0, abs(trend) / max(self.trend_threshold, 1e-12))
        if trend >= self.strong_trend_threshold:
            states.append(Regime(RegimeState.STRONG_BULL, MarketType.CRYPTO_SPOT, trend_confidence, {"return": trend}))
        elif trend >= self.trend_threshold:
            states.append(Regime(RegimeState.BULL, MarketType.CRYPTO_SPOT, trend_confidence, {"return": trend}))
        elif trend <= -self.strong_trend_threshold:
            states.append(Regime(RegimeState.STRONG_BEAR, MarketType.CRYPTO_SPOT, trend_confidence, {"return": trend}))
        elif trend <= -self.trend_threshold:
            states.append(Regime(RegimeState.BEAR, MarketType.CRYPTO_SPOT, trend_confidence, {"return": trend}))
        if trend >= self.trend_threshold:
            states.append(Regime(RegimeState.BULLISH, MarketType.CRYPTO_SPOT, trend_confidence, {"return": trend}))
        elif trend <= -self.trend_threshold:
            states.append(Regime(RegimeState.BEARISH, MarketType.CRYPTO_SPOT, trend_confidence, {"return": trend}))
        if abs(trend) >= self.trend_threshold / 2.0 and len(values) > 2:
            states.append(Regime(RegimeState.TRENDING, MarketType.CRYPTO_SPOT, trend_confidence, {"return": trend}))
        else:
            states.append(Regime(RegimeState.SIDEWAYS, MarketType.CRYPTO_SPOT, max(0.0, 1.0 - trend_confidence), {"return": trend}))
            states.append(Regime(RegimeState.RANGE_BOUND, MarketType.CRYPTO_SPOT, max(0.0, 1.0 - trend_confidence), {"return": trend}))
        if volatility >= self.extreme_volatility_threshold:
            states.append(Regime(RegimeState.EXTREME_VOLATILITY, MarketType.CRYPTO_SPOT, min(1.0, volatility / max(self.extreme_volatility_threshold, 1e-12)), {"volatility": volatility}))
            states.append(Regime(RegimeState.HIGH_VOLATILITY, MarketType.CRYPTO_SPOT, 1.0, {"volatility": volatility}))
        elif volatility >= self.volatility_threshold:
            states.append(Regime(RegimeState.HIGH_VOLATILITY, MarketType.CRYPTO_SPOT, min(1.0, volatility / max(self.volatility_threshold, 1e-12)), {"volatility": volatility}))
        else:
            states.append(Regime(RegimeState.NORMAL_VOLATILITY, MarketType.CRYPTO_SPOT, 1.0, {"volatility": volatility}))
        if returns and returns[-1] <= -self.crash_threshold:
            states.append(Regime(RegimeState.CRASH, MarketType.CRYPTO_SPOT, min(1.0, abs(returns[-1]) / max(self.crash_threshold, 1e-12)), {"return": returns[-1]}))
        volumes = [self._number(bar, "volume") for bar in bars]
        average_volume = mean(volumes) if volumes else 0.0
        if average_volume > self.liquidity_threshold and all(value > 0 for value in volumes):
            states.append(Regime(RegimeState.LIQUID, MarketType.CRYPTO_SPOT, 1.0, {"volume": average_volume}))
            states.append(Regime(RegimeState.HIGH_LIQUIDITY, MarketType.CRYPTO_SPOT, 1.0, {"volume": average_volume}))
        else:
            states.append(Regime(RegimeState.ILLIQUID, MarketType.CRYPTO_SPOT, 1.0, {"volume": average_volume}))
            states.append(Regime(RegimeState.LOW_LIQUIDITY, MarketType.CRYPTO_SPOT, 1.0, {"volume": average_volume}))
        timestamp = bars[-1].get("timestamp") if isinstance(bars[-1], Mapping) else getattr(bars[-1], "timestamp", None)
        return RegimeSnapshot(MarketType.CRYPTO_SPOT, tuple(states), timestamp)

    def detect_prediction(self, snapshots: Sequence[PredictionMarketSnapshot | Mapping[str, Any]]) -> RegimeSnapshot:
        if not snapshots:
            return RegimeSnapshot(MarketType.PREDICTION, ())

        def value(item: Any, key: str, default: Any = None) -> Any:
            return item.get(key, default) if isinstance(item, Mapping) else getattr(item, key, default)

        states: list[Regime] = []
        latest = snapshots[-1]
        expiry_seconds = self._number(latest, "time_to_expiry_seconds", math.inf)
        if not math.isfinite(expiry_seconds):
            stamp = value(latest, "timestamp")
            expiry = value(latest, "expiry")
            if isinstance(stamp, datetime) and isinstance(expiry, datetime):
                expiry_seconds = (ensure_utc(expiry) - ensure_utc(stamp)).total_seconds()
        if expiry_seconds <= self.near_expiry_seconds:
            states.append(Regime(RegimeState.NEAR_EXPIRY, MarketType.PREDICTION, 1.0, {"seconds": expiry_seconds}))

        liquidity = self._number(latest, "liquidity", 0.0)
        liquidity_confidence = min(1.0, liquidity / (liquidity + 1.0)) if liquidity > 0 else 1.0
        if liquidity > self.liquidity_threshold:
            states.append(Regime(RegimeState.LIQUID, MarketType.PREDICTION, liquidity_confidence, {"liquidity": liquidity}))
            states.append(Regime(RegimeState.HIGH_LIQUIDITY, MarketType.PREDICTION, liquidity_confidence, {"liquidity": liquidity}))
        else:
            states.append(Regime(RegimeState.ILLIQUID, MarketType.PREDICTION, liquidity_confidence, {"liquidity": liquidity}))
            states.append(Regime(RegimeState.LOW_LIQUIDITY, MarketType.PREDICTION, liquidity_confidence, {"liquidity": liquidity}))

        timestamps = []
        for item in snapshots:
            stamp = value(item, "timestamp")
            if isinstance(stamp, datetime):
                timestamps.append(ensure_utc(stamp))
        age_seconds = (timestamps[-1] - timestamps[0]).total_seconds() if len(timestamps) > 1 else 0.0
        total_seconds = value(latest, "market_duration_seconds", value(latest, "duration_seconds"))
        if total_seconds is None and timestamps and isinstance(value(latest, "expiry"), datetime):
            total_seconds = (ensure_utc(value(latest, "expiry")) - timestamps[0]).total_seconds()
        if total_seconds is not None:
            total_seconds = self._number({"value": total_seconds}, "value", 0.0)
        if (len(snapshots) <= 3 or (total_seconds and age_seconds / total_seconds <= self.early_market_fraction)):
            states.append(Regime(RegimeState.EARLY_MARKET, MarketType.PREDICTION, 1.0, {"age_seconds": age_seconds}))
        else:
            states.append(Regime(RegimeState.MATURE_MARKET, MarketType.PREDICTION, 1.0, {"age_seconds": age_seconds}))

        mids = [self._number(item, "yes_mid", math.nan) for item in snapshots]
        mids = [value for value in mids if math.isfinite(value)]
        changes = [b - a for a, b in zip(mids, mids[1:])]
        latest_change = changes[-1] if changes else 0.0
        if abs(latest_change) >= self.repricing_threshold:
            confidence = min(1.0, abs(latest_change) / max(self.repricing_threshold, 1e-12))
            states.append(Regime(RegimeState.SUDDEN_REPRICING, MarketType.PREDICTION, confidence, {"change": latest_change}))
            states.append(Regime(RegimeState.HIGH_INFORMATION_FLOW, MarketType.PREDICTION, confidence, {"change": latest_change}))
        elif changes and max(abs(change) for change in changes) < self.repricing_threshold / 2.0:
            states.append(Regime(RegimeState.STABLE_PROBABILITY, MarketType.PREDICTION, 1.0, {"max_change": max(abs(change) for change in changes)}))
        if changes and abs(mids[-1] - mids[0]) >= self.trend_threshold:
            states.append(Regime(RegimeState.HIGH_MOMENTUM, MarketType.PREDICTION, min(1.0, abs(mids[-1] - mids[0]) / max(self.trend_threshold, 1e-12)), {"change": mids[-1] - mids[0]}))

        model = self._number(latest, "model_probability", math.nan)
        mid = self._number(latest, "yes_mid", math.nan)
        if math.isfinite(model) and math.isfinite(mid) and abs(model - mid) >= self.trend_threshold:
            states.append(Regime(RegimeState.HIGH_DISAGREEMENT, MarketType.PREDICTION, min(1.0, abs(model - mid) / max(self.trend_threshold, 1e-12)), {"edge": model - mid}))
        if math.isfinite(mid) and (mid <= self.extreme_tail_threshold or mid >= 1.0 - self.extreme_tail_threshold):
            states.append(Regime(RegimeState.EXTREME_TAIL, MarketType.PREDICTION, 1.0, {"mid": mid}))
        bid = self._number(latest, "yes_bid", math.nan)
        ask = self._number(latest, "yes_ask", math.nan)
        explicit_spread = self._number(latest, "spread", math.nan)
        spread_ratio = explicit_spread / mid if math.isfinite(explicit_spread) and math.isfinite(mid) and mid > 0 else ((ask - bid) / mid if math.isfinite(bid) and math.isfinite(ask) and math.isfinite(mid) and mid > 0 else 0.0)
        if spread_ratio >= self.spread_widening_ratio:
            states.append(Regime(RegimeState.SPREAD_WIDENING, MarketType.PREDICTION, min(1.0, spread_ratio / max(self.spread_widening_ratio, 1e-12)), {"spread_ratio": spread_ratio}))
        information = self._number(latest, "information_flow", 0.0)
        event_count = self._number(latest, "event_count", 0.0)
        news_count = self._number(latest, "news_count", 0.0)
        if information > 0 or event_count > 0 or news_count > 0:
            states.append(Regime(RegimeState.HIGH_INFORMATION_FLOW, MarketType.PREDICTION, min(1.0, max(information, event_count, news_count) / 10.0), {"information": max(information, event_count, news_count)}))
        if event_count > 0:
            states.append(Regime(RegimeState.EVENT_RICH, MarketType.PREDICTION, min(1.0, event_count / 10.0), {"events": event_count}))
        stamp = value(latest, "timestamp")
        return RegimeSnapshot(MarketType.PREDICTION, tuple(states), stamp)

    def detect(self, observations: Sequence[Any], market_type: MarketType | str) -> RegimeSnapshot:
        kind = market_type if isinstance(market_type, MarketType) else MarketType(str(market_type))
        return self.detect_crypto(observations) if kind is MarketType.CRYPTO_SPOT else self.detect_prediction(observations)

    classify = detect

    def detect_details(self, observations: Sequence[Any], market_type: MarketType | str) -> tuple[Regime, ...]:
        return self.detect(observations, market_type).regimes


RegimeType = RegimeState


__all__ = ["Regime", "RegimeEngine", "RegimeSnapshot", "RegimeState", "RegimeType"]
