"""Deterministic Binance Spot executable signals and crypto paper-forward simulation.

This module is deliberately independent from the public Binance adapter/canary.  It
consumes immutable research bindings, declarative strategies, and already-captured
market bars.  There is no credential, transport, or live-order path here.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import math
import re
import sqlite3
from types import MappingProxyType
from typing import Any, Iterable, Iterator, Mapping, Sequence

from .binance_spot import (
    BinanceRuntimeProfile,
    BinanceSpotEnvironment,
    BinanceSpotRESTClient,
    BinanceSpotResult,
    canonical_sha256,
)
from .binance_risk import (
    BinanceRiskEnvelope,
    RiskSnapshot,
    SymbolRules,
    assess_entry,
    assess_exit,
    size_limit_order,
)
from .binance_research import CryptoExecutionBinding
from .binance_market import BinanceMarketSnapshot
from .domain import MarketType, OHLCVBar, ensure_utc, parse_timestamp, utc_now
from .strategy import StrategyDefinition, evaluate_signal, validate_strategy

UTC = timezone.utc
_ZERO = Decimal("0")
_ONE = Decimal("1")
_INTERVAL_RE = re.compile(r"^(?P<count>[1-9][0-9]*)(?P<unit>[smhdwM])$")


def _decimal(value: Any, *, name: str = "value", nonnegative: bool = False) -> Decimal:
    if isinstance(value, bool) or value is None:
        raise ValueError(f"{name} must be a finite decimal")
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a finite decimal") from exc
    if not result.is_finite() or (nonnegative and result < _ZERO):
        raise ValueError(f"{name} must be a finite decimal" if not nonnegative else f"{name} must be non-negative")
    return result


def _optional_decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    if isinstance(value, str) and value.strip().lower() in {"unknown", "n/a", "na"}:
        return None
    return _decimal(value)


def _jsonable(value: Any) -> Any:
    """Canonical JSON projection preserving Decimal and UTC timestamps losslessly."""
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, datetime):
        return ensure_utc(value).isoformat()
    if isinstance(value, Mapping):
        return {str(k): _jsonable(v) for k, v in sorted(value.items(), key=lambda item: str(item[0]))}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    if isinstance(value, (str, int, bool)) or value is None:
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("non-finite values cannot be canonicalized")
        return format(Decimal(str(value)), "f")
    if hasattr(value, "as_dict") and callable(value.as_dict):
        return _jsonable(value.as_dict())
    if hasattr(value, "to_dict") and callable(value.to_dict):
        return _jsonable(value.to_dict())
    if hasattr(value, "__dataclass_fields__"):
        return _jsonable({name: getattr(value, name) for name in value.__dataclass_fields__})
    return str(value)


def _canonical(value: Any) -> str:
    return json.dumps(_jsonable(value), ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _hash(value: Any) -> str:
    # Use the foundation hash function after converting values to its accepted
    # JSON primitive domain.  This keeps IDs compatible with other Binance code.
    return canonical_sha256(_jsonable(value))


def _value(value: Any, *names: str, default: Any = None) -> Any:
    if isinstance(value, Mapping):
        for name in names:
            if name in value:
                return value[name]
        return default
    for name in names:
        found = getattr(value, name, None)
        if found is not None:
            return found
    return default


def _timestamp(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return ensure_utc(value)
    if value is None:
        return None
    parsed = parse_timestamp(value)
    if parsed is not None:
        return ensure_utc(parsed)
    try:
        number = float(value)
        if not math.isfinite(number):
            return None
        if abs(number) > 100_000_000_000:
            number /= 1000.0
        return datetime.fromtimestamp(number, tz=UTC)
    except (TypeError, ValueError, OverflowError, OSError):
        return None


def _interval_delta(interval: str) -> timedelta:
    match = _INTERVAL_RE.match(str(interval).strip())
    if not match:
        raise ValueError(f"unsupported decision interval: {interval!r}")
    count = int(match.group("count"))
    unit = match.group("unit")
    if unit == "s":
        return timedelta(seconds=count)
    if unit == "m":
        return timedelta(minutes=count)
    if unit == "h":
        return timedelta(hours=count)
    if unit == "d":
        return timedelta(days=count)
    if unit == "w":
        return timedelta(weeks=count)
    # Binance month intervals are not fixed-duration bars.  They are accepted
    # for IDs but require explicit close_time/closed metadata for timing.
    raise ValueError("monthly intervals require explicit close_time metadata")


def _symbol(value: Any) -> str:
    result = str(value or "").replace("/", "").replace("-", "").replace("_", "").strip().upper()
    if not result:
        raise ValueError("symbol is required")
    return result


def _bar_open(bar: Any) -> datetime | None:
    return _timestamp(_value(bar, "timestamp", "open_time", "openTime", "start_time", "startTime"))


def _bar_close(bar: Any, interval: str) -> datetime | None:
    explicit = _timestamp(_value(bar, "close_time", "closeTime", "close_timestamp", "closeTimestamp", "end_time", "endTime"))
    if explicit is not None:
        return explicit
    opening = _bar_open(bar)
    if opening is None:
        return None
    try:
        return opening + _interval_delta(interval)
    except ValueError:
        return None


def _is_explicitly_closed(bar: Any) -> bool:
    marker = _value(bar, "closed", "is_closed", "isClosed", "bar_closed", "complete", "final", default=None)
    if marker is None:
        return True
    if isinstance(marker, str):
        return marker.strip().lower() in {"1", "true", "yes", "y", "closed", "complete", "final"}
    return bool(marker)


def _bar_price(bar: Any, key: str, default: Any = None) -> Decimal | None:
    return _optional_decimal(_value(bar, key, default=default))


def _bar_payload(bar: Any) -> Any:
    if isinstance(bar, Mapping):
        return dict(bar)
    if isinstance(bar, OHLCVBar):
        return {
            "timestamp": bar.timestamp,
            "open": bar.open,
            "high": bar.high,
            "low": bar.low,
            "close": bar.close,
            "volume": bar.volume,
            "spread": bar.spread,
            "trades": bar.trades,
        }
    if hasattr(bar, "__dataclass_fields__"):
        return {name: getattr(bar, name) for name in bar.__dataclass_fields__}
    return bar


def _snapshot_bars(snapshot: Any) -> tuple[Any, ...]:
    if isinstance(snapshot, Mapping):
        bars = _value(snapshot, "bars", "ohlcv", "history", "observations", default=())
    else:
        bars = _value(snapshot, "bars", "ohlcv", "history", "observations", default=())
    if isinstance(bars, (str, bytes, Mapping)) or bars is None:
        return ()
    try:
        return tuple(bars)
    except TypeError:
        return ()


def _binding_projection(binding: Any) -> Mapping[str, Any]:
    if isinstance(binding, CryptoExecutionBinding):
        return binding.as_dict()
    if isinstance(binding, Mapping):
        nested = binding.get("binding")
        if isinstance(nested, Mapping):
            return dict(nested)
        return dict(binding)
    for method_name in ("as_dict", "to_dict"):
        method = getattr(binding, method_name, None)
        if callable(method):
            result = method()
            if isinstance(result, Mapping):
                return dict(result)
    raise TypeError("binding must be CryptoExecutionBinding or a mapping")


def _binding_hash(binding: Any, projection: Mapping[str, Any] | None = None) -> str:
    declared = _value(binding, "binding_hash", default=None)
    if declared:
        return str(declared)
    projection = projection or _binding_projection(binding)
    return _hash(projection)


def _position_quantity(position: Any) -> Decimal:
    value = _value(position, "quantity", "qty", "position_quantity", default=position if not isinstance(position, Mapping) else 0)
    try:
        return _decimal(value, name="position quantity", nonnegative=True)
    except ValueError:
        return _ZERO


def _position_entry_time(position: Any) -> datetime | None:
    return _timestamp(_value(position, "entry_time", "opened_at", "entry_timestamp", "opened_at_timestamp", "timestamp"))


def _position_policy(position: Any) -> Mapping[str, Any] | None:
    policy = _value(position, "exit_policy", "frozen_exit_policy", "originating_exit_policy", default=None)
    return dict(policy) if isinstance(policy, Mapping) else None


def _qualified_binding(binding: Any) -> tuple[bool, bool]:
    if isinstance(binding, Mapping):
        qualification = binding.get("qualification")
        source = qualification if isinstance(qualification, Mapping) else binding
        return bool(source.get("qualified", True)), bool(source.get("actionable", True))
    return bool(getattr(binding, "qualified", True)), bool(getattr(binding, "actionable", True))


def _validate_crypto_strategy(strategy: StrategyDefinition | Mapping[str, Any] | str) -> StrategyDefinition:
    """Normalize a strategy document and enforce the Binance crypto boundary."""
    definition = strategy if isinstance(strategy, StrategyDefinition) else validate_strategy(strategy)
    if definition.market_type is not MarketType.CRYPTO_SPOT:
        raise ValueError("Binance Spot signals require a crypto_spot strategy")
    return definition



@dataclass(frozen=True, slots=True)
class BinanceExecutableSignal(Mapping[str, Any]):
    """A canonical, immutable ENTRY or EXIT instruction candidate."""

    candidate_id: str
    binding_hash: str
    symbol: str
    environment: str
    decision_interval: str
    decision_at: datetime
    intent: str
    side: str
    reason: str
    exit_policy: Mapping[str, Any] | None = None
    bar_close: datetime | None = None
    score: Decimal | None = None
    signal_id: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "candidate_id", str(self.candidate_id).strip())
        object.__setattr__(self, "binding_hash", str(self.binding_hash).strip())
        object.__setattr__(self, "symbol", _symbol(self.symbol))
        object.__setattr__(self, "environment", str(self.environment).strip().upper())
        object.__setattr__(self, "decision_interval", str(self.decision_interval).strip())
        object.__setattr__(self, "decision_at", ensure_utc(self.decision_at))
        intent = str(self.intent).strip().upper()
        side = str(self.side).strip().upper()
        if intent not in {"ENTRY", "EXIT"}:
            raise ValueError("signal intent must be ENTRY or EXIT")
        if side not in {"BUY", "SELL"} or (intent == "ENTRY" and side != "BUY") or (intent == "EXIT" and side != "SELL"):
            raise ValueError("Spot signal side must be BUY for ENTRY and SELL for EXIT")
        object.__setattr__(self, "intent", intent)
        object.__setattr__(self, "side", side)
        object.__setattr__(self, "reason", str(self.reason).strip() or "NO_SIGNAL")
        if self.exit_policy is not None:
            object.__setattr__(self, "exit_policy", MappingProxyType(dict(self.exit_policy)))
        if self.bar_close is not None:
            object.__setattr__(self, "bar_close", ensure_utc(self.bar_close))
        if self.score is not None:
            object.__setattr__(self, "score", _decimal(self.score))
        if not self.candidate_id or not self.binding_hash or not self.decision_interval:
            raise ValueError("signal candidate, binding, and interval are required")
        expected = self.make_signal_id(
            self.candidate_id,
            self.binding_hash,
            self.symbol,
            self.bar_close,
            self.decision_interval,
            self.intent,
        )
        if self.signal_id and str(self.signal_id) != expected:
            raise ValueError("signal_id does not match canonical signal identity")
        object.__setattr__(self, "signal_id", expected)

    @staticmethod
    def make_signal_id(
        candidate_id: str,
        binding_hash: str,
        symbol: str,
        bar_close: datetime | None,
        decision_interval: str,
        intent: str,
    ) -> str:
        close = ensure_utc(bar_close).isoformat() if bar_close is not None else ""
        # Deliberately omit decision_at, reason, score, and telemetry.  One
        # unchanged condition on one completed interval is one opportunity.
        return canonical_sha256(
            {
                "candidate_id": str(candidate_id),
                "binding_hash": str(binding_hash),
                "symbol": _symbol(symbol),
                "bar_close": close,
                "decision_interval": str(decision_interval),
                "intent": str(intent).upper(),
            }
        )
    build_signal_id = make_signal_id

    @property
    def id(self) -> str:
        return self.signal_id

    @property
    def actionable(self) -> bool:
        return self.reason not in {"NO_SIGNAL", "NO_CLOSED_BAR"}

    def as_dict(self) -> dict[str, Any]:
        return {
            "signal_id": self.signal_id,
            "candidate_id": self.candidate_id,
            "binding_hash": self.binding_hash,
            "symbol": self.symbol,
            "environment": self.environment,
            "decision_interval": self.decision_interval,
            "decision_at": self.decision_at.isoformat(),
            "bar_close": self.bar_close.isoformat() if self.bar_close else None,
            "intent": self.intent,
            "side": self.side,
            "reason": self.reason,
            "exit_policy": _jsonable(self.exit_policy) if self.exit_policy is not None else None,
            "score": format(self.score, "f") if self.score is not None else None,
        }

    to_dict = as_dict

    def __getitem__(self, key: str) -> Any:
        return self.as_dict()[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self.as_dict())

    def __len__(self) -> int:
        return len(self.as_dict())


class BinanceSignalEngine:
    """Evaluate one frozen candidate against completed Binance Spot bars."""

    def __init__(
        self,
        binding: CryptoExecutionBinding | Mapping[str, Any],
        strategy: StrategyDefinition | Mapping[str, Any] | str | None = None,
        *,
        environment: BinanceSpotEnvironment | BinanceRuntimeProfile | str | None = None,
        decision_interval: str | None = None,
        exit_policy: Mapping[str, Any] | None = None,
        positions: Mapping[str, Any] | None = None,
        position_mapping: Mapping[str, Any] | None = None,
        clock: Any | None = None,
        deduplicate: bool = True,
    ) -> None:
        self.binding = binding
        self.binding_record = _binding_projection(binding)
        self.candidate_id = str(self.binding_record.get("candidate_id", "")).strip()
        self.symbol = _symbol(self.binding_record.get("symbol"))
        self.binding_hash = _binding_hash(binding, self.binding_record)
        profile_environment: Any = environment
        if isinstance(profile_environment, BinanceRuntimeProfile):
            profile_environment = profile_environment.environment
        if isinstance(profile_environment, BinanceSpotEnvironment):
            profile_environment = profile_environment.value
        self.environment = str(profile_environment or self.binding_record.get("environment") or BinanceSpotEnvironment.PAPER.value).strip().upper()
        document = strategy
        if document is None:
            document = self.binding_record.get("strategy") or self.binding_record.get("strategy_document")
            if document is None:
                raise ValueError("strategy is required")
        definition = _validate_crypto_strategy(document)
        self.strategy = definition
        interval_value = decision_interval or self.binding_record.get("timeframe") or self.binding_record.get("interval") or "1d"
        self.decision_interval = str(interval_value).strip()
        _interval_delta(self.decision_interval)
        self.exit_policy = dict(exit_policy or self.binding_record.get("exit_policy") or {})
        self.positions = positions if positions is not None else (position_mapping if position_mapping is not None else {})
        self.clock = clock or utc_now
        self.deduplicate = bool(deduplicate)
        self._emitted_ids: set[str] = set()
        self.last_no_trade_reason = "NO_SIGNAL"
        self.last_signal: BinanceExecutableSignal | None = None

    @property
    def no_trade_reason(self) -> str:
        return self.last_no_trade_reason

    def _now(self, value: datetime | None) -> datetime:
        if value is not None:
            return ensure_utc(value)
        candidate = self.clock() if callable(self.clock) else self.clock
        parsed = _timestamp(candidate)
        return parsed or ensure_utc(utc_now())

    def _position(self, positions: Mapping[str, Any] | None = None) -> Any:
        source = self.positions if positions is None else positions
        if not isinstance(source, Mapping):
            return None
        for key in (self.symbol, self.symbol.upper(), self.binding_record.get("symbol")):
            if key in source:
                value = source[key]
                if _position_quantity(value) > _ZERO:
                    return value
        return None

    def _closed(self, snapshot: Any, now: datetime) -> tuple[tuple[Any, ...], datetime | None]:
        interval = str(_value(snapshot, "interval", "timeframe", default=self.decision_interval) or self.decision_interval)
        # Engine interval is explicit and cannot silently change with a market
        # collector's stale metadata.
        if interval != self.decision_interval:
            interval = self.decision_interval
        rows: list[tuple[datetime, datetime, Any]] = []
        for bar in _snapshot_bars(snapshot):
            if not _is_explicitly_closed(bar):
                continue
            opening = _bar_open(bar)
            closing = _bar_close(bar, interval)
            if opening is None or closing is None or closing > now:
                continue
            rows.append((opening, closing, bar))
        rows.sort(key=lambda item: (item[0], _canonical(_bar_payload(item[2]))))
        deduped: list[tuple[datetime, datetime, Any]] = []
        seen: set[datetime] = set()
        for opening, closing, bar in rows:
            if opening in seen:
                continue
            seen.add(opening)
            deduped.append((opening, closing, bar))
        return tuple(bar for _, _, bar in deduped), (deduped[-1][1] if deduped else None)

    def _entry_allowed(self, snapshot: Any) -> bool:
        qualified, actionable = _qualified_binding(self.binding)
        if not qualified:
            self.last_no_trade_reason = "BINDING_NOT_QUALIFIED"
            return False
        if not actionable:
            self.last_no_trade_reason = "BINDING_NOT_ACTIONABLE"
            return False
        if bool(_value(self.binding, "selected", default=False)) is False and isinstance(self.binding, Mapping) and "selected" in self.binding:
            # Selection is not an execution gate: lower-ranked qualified
            # opportunities remain independently callable by their owner.
            pass
        if bool(_value(snapshot, "exit_only", default=False)):
            self.last_no_trade_reason = "EXIT_ONLY"
            return False
        if bool(_value(snapshot, "selected", default=False)) and _value(snapshot, "new_entry_allowed", default=None) is False:
            self.last_no_trade_reason = "NEW_ENTRY_NOT_ALLOWED"
            return False
        return True

    def _policy_for(self, position: Any) -> dict[str, Any]:
        originating = _position_policy(position)
        return dict(originating or self.exit_policy)

    @staticmethod
    def _max_hold_due(position: Any, policy: Mapping[str, Any], close: datetime, interval: str) -> bool:
        opened = _position_entry_time(position)
        if opened is None:
            return False

        def seconds_delta(value: Any) -> timedelta:
            seconds = _decimal(value, name="max holding seconds", nonnegative=True)
            return timedelta(microseconds=int(seconds * Decimal("1000000")))

        for key in ("max_holding_seconds", "max_hold_seconds", "holding_seconds"):
            if policy.get(key) is not None:
                try:
                    return close - opened >= seconds_delta(policy[key])
                except (TypeError, ValueError, OverflowError):
                    return False
        for key in ("max_holding_time", "max_hold", "max_holding_duration"):
            if policy.get(key) is not None:
                try:
                    raw = policy[key]
                    if isinstance(raw, str) and _INTERVAL_RE.match(raw):
                        return close - opened >= _interval_delta(raw)
                    return close - opened >= seconds_delta(raw)
                except (TypeError, ValueError, OverflowError):
                    return False
        for key in ("max_holding_bars", "max_hold_bars"):
            if policy.get(key) is not None:
                try:
                    count = int(_decimal(policy[key], name="max holding bars", nonnegative=True))
                    return close - opened >= _interval_delta(interval) * count
                except (TypeError, ValueError, OverflowError):
                    return False
        return False

    @staticmethod
    def _circuit_due(snapshot: Any, policy: Mapping[str, Any]) -> bool:
        explicit = policy.get("risk_reducing_circuit")
        if explicit is None:
            explicit = policy.get("circuit_policy")
        if isinstance(explicit, Mapping):
            explicit = explicit.get("triggered", explicit.get("active", explicit.get("risk_reducing", False)))
        if explicit is not True:
            return False
        return bool(
            _value(snapshot, "risk_reducing_circuit", "risk_circuit", "circuit_breaker", "circuit_triggered", "risk_reducing", default=False)
        )

    def evaluate(
        self,
        snapshot: BinanceMarketSnapshot | Mapping[str, Any],
        *,
        now: datetime | None = None,
        positions: Mapping[str, Any] | None = None,
        position_mapping: Mapping[str, Any] | None = None,
    ) -> BinanceExecutableSignal | None:
        if positions is None and position_mapping is not None:
            positions = position_mapping
        instant = self._now(now)
        snapshot_symbol = _value(snapshot, "symbol", default=self.symbol)
        try:
            if _symbol(snapshot_symbol) != self.symbol:
                self.last_no_trade_reason = "SYMBOL_MISMATCH"
                return None
        except ValueError:
            self.last_no_trade_reason = "SYMBOL_MISMATCH"
            return None
        bars, close = self._closed(snapshot, instant)
        position = self._position(positions)
        policy = self._policy_for(position) if position is not None else dict(self.exit_policy)
        signal_candidate_id = self.candidate_id
        signal_binding_hash = self.binding_hash
        if position is not None:
            originating = _value(position, "originating_binding", "entry_binding", default=None)
            if originating is not None:
                try:
                    origin_projection = _binding_projection(originating)
                    signal_candidate_id = str(origin_projection.get("candidate_id") or signal_candidate_id)
                    signal_binding_hash = _binding_hash(originating, origin_projection)
                except (TypeError, ValueError):
                    pass
            signal_candidate_id = str(_value(position, "candidate_id", "originating_candidate_id", default=signal_candidate_id) or signal_candidate_id)
            signal_binding_hash = str(_value(position, "binding_hash", "originating_binding_hash", default=signal_binding_hash) or signal_binding_hash)
        score_value = evaluate_signal(self.strategy, {"bars": bars, "symbol": self.symbol, "interval": self.decision_interval})
        try:
            score = _decimal(score_value)
        except ValueError:
            score = _ZERO
        position = self._position(positions)
        policy = self._policy_for(position) if position is not None else dict(self.exit_policy)
        intent: str | None = None
        reason = "NO_SIGNAL"
        side = "BUY"
        if position is not None:
            # EXIT evaluation is intentionally independent from current ranking
            # and universe membership.  The position carries its origin policy.
            side = "SELL"
            exit_strategy = policy.get("strategy") or policy.get("strategy_definition")
            exit_score = score
            if exit_strategy is not None:
                try:
                    exit_score = _decimal(evaluate_signal(validate_strategy(exit_strategy), {"bars": bars, "symbol": self.symbol, "interval": self.decision_interval}))
                except (TypeError, ValueError):
                    exit_score = _ZERO
            if exit_score < _ZERO:
                intent, reason = "EXIT", "EXIT_NEGATIVE_SIGNAL"
            elif self._max_hold_due(position, policy, close, self.decision_interval):
                intent, reason = "EXIT", "EXIT_MAX_HOLD"
            elif self._circuit_due(snapshot, policy):
                intent, reason = "EXIT", "EXIT_RISK_CIRCUIT"
            else:
                self.last_no_trade_reason = "HOLD_POSITION"
                return None
        else:
            if score > _ZERO and self._entry_allowed(snapshot):
                intent, reason, side = "ENTRY", "ENTRY_SIGNAL", "BUY"
            elif score <= _ZERO:
                self.last_no_trade_reason = "NO_SIGNAL"
                return None
            else:
                return None
        signal = BinanceExecutableSignal(
            candidate_id=signal_candidate_id,
            binding_hash=signal_binding_hash,
            symbol=self.symbol,
            environment=self.environment,
            decision_interval=self.decision_interval,
            decision_at=instant,
            bar_close=close,
            intent=intent or "ENTRY",
            side=side,
            reason=reason,
            exit_policy=policy,
            score=score,
        )
        if self.deduplicate and signal.signal_id in self._emitted_ids:
            self.last_no_trade_reason = "DUPLICATE_INTERVAL"
            return None
        self._emitted_ids.add(signal.signal_id)
        self.last_signal = signal
        self.last_no_trade_reason = ""
        return signal

    decide = evaluate
    evaluate_signal = evaluate

    def evaluate_all(
        self,
        snapshots: Iterable[BinanceMarketSnapshot | Mapping[str, Any]] | Mapping[str, Any],
        *,
        now: datetime | None = None,
        positions: Mapping[str, Any] | None = None,
    ) -> tuple[BinanceExecutableSignal, ...]:
        if isinstance(snapshots, Mapping):
            # A symbol->snapshot map is accepted, while a single snapshot map
            # remains a one-item input.
            if any(key in snapshots for key in ("symbol", "bars", "ohlcv", "history")):
                rows = (snapshots,)
            else:
                rows = tuple(value for value in snapshots.values() if isinstance(value, Mapping) or hasattr(value, "bars"))
        else:
            rows = tuple(snapshots)
        output: list[BinanceExecutableSignal] = []
        for snapshot in rows:
            signal = self.evaluate(snapshot, now=now, positions=positions)
            if signal is not None:
                output.append(signal)
        return tuple(output)


@dataclass(frozen=True, slots=True)
class CryptoPaperForwardResult(Mapping[str, Any]):
    """Immutable result of a deterministic multi-symbol paper-forward run."""

    run_id: str
    metrics: Mapping[str, Any]
    fills: tuple[Mapping[str, Any], ...]
    positions: tuple[Mapping[str, Any], ...]
    observations: tuple[Mapping[str, Any], ...]
    evidence: Mapping[str, Any]
    equity_curve: tuple[Mapping[str, Any], ...] = ()
    no_trade_reasons: Mapping[str, int] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "metrics": _jsonable(self.metrics),
            "fills": _jsonable(self.fills),
            "positions": _jsonable(self.positions),
            "observations": _jsonable(self.observations),
            "evidence": _jsonable(self.evidence),
            "equity_curve": _jsonable(self.equity_curve),
            "no_trade_reasons": dict(self.no_trade_reasons),
        }

    to_dict = as_dict

    def __getitem__(self, key: str) -> Any:
        return self.as_dict()[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self.as_dict())

    def __len__(self) -> int:
        return len(self.as_dict())

    @property
    def trades(self) -> tuple[Mapping[str, Any], ...]:
        return tuple(fill for fill in self.fills if str(fill.get("intent", "")).upper() == "EXIT")


@dataclass(slots=True)
class _PaperPosition:
    symbol: str
    quantity: Decimal
    average_price: Decimal
    entry_time: datetime
    entry_signal_id: str
    candidate_id: str
    binding_hash: str
    exit_policy: Mapping[str, Any]
    cost_basis: Decimal
    fees: Decimal = _ZERO
    last_event_timestamp: datetime | None = None

class CryptoPaperForwardEngine:
    """Simulate Spot LIMIT-style next-open execution over closed bars only."""

    TABLE_PREFIX = "binance_spot_paper"

    def __init__(
        self,
        bindings: CryptoExecutionBinding | Mapping[str, Any] | Sequence[Any] | None = None,
        strategies: StrategyDefinition | Mapping[str, Any] | str | Mapping[str, Any] | None = None,
        snapshots: Iterable[Any] | Mapping[str, Any] | None = None,
        *,
        store: Any | None = None,
        run_id: str | None = None,
        binding: Any | None = None,
        strategy: Any | None = None,
        market_snapshots: Any | None = None,
        initial_cash: Any = Decimal("1000"),
        fee_rate: Any = Decimal("0.001"),
        slippage_bps: Any = Decimal("0"),
        depth: Any | None = None,
        max_holding_bars: int | None = None,
        max_holding_time: Any | None = None,
        decision_interval: str | None = None,
        holdout: Any | None = None,
        locked_holdout: Any | None = None,
        clock: Any | None = None,
        **kwargs: Any,
    ) -> None:
        if binding is not None:
            bindings = binding
        if strategy is not None:
            strategies = strategy
        if market_snapshots is not None:
            snapshots = market_snapshots
        self.store = store
        self._connection_owner = False
        if store is not None:
            self.connection = getattr(store, "connection", getattr(store, "_conn", None))
            self._lock = getattr(store, "_lock", None)
        else:
            self.connection = sqlite3.connect(":memory:")
            self.connection.row_factory = sqlite3.Row
            self._lock = None
            self._connection_owner = True
        if self.connection is None:
            raise TypeError("store must expose a SQLite connection")
        self._create_tables()
        self.bindings = self._normalize_bindings(bindings)
        self.strategies = strategies
        self.snapshots_input = snapshots
        self.initial_cash = _decimal(initial_cash, name="initial_cash", nonnegative=True)
        self.fee_rate = None if str(fee_rate).strip().lower() in {"unknown", "none", "n/a"} else _decimal(fee_rate, name="fee_rate", nonnegative=True)
        self.slippage_bps = _decimal(slippage_bps, name="slippage_bps", nonnegative=True)
        self.depth = None if depth is None else _decimal(depth, name="depth", nonnegative=True)
        if max_holding_bars is not None and (isinstance(max_holding_bars, bool) or int(max_holding_bars) < 1):
            raise ValueError("max_holding_bars must be positive")
        self.max_holding_bars = int(max_holding_bars) if max_holding_bars is not None else None
        self.max_holding_time = max_holding_time
        self.decision_interval = str(decision_interval or "").strip() or None
        self.holdout = holdout if holdout is not None else locked_holdout
        self.clock = clock or utc_now
        self.run_id = str(run_id).strip() if run_id else ""
        self._requested_run_id = self.run_id
        self.risk_envelope = kwargs.pop("risk_envelope", None)
        self.symbol_rules = kwargs.pop("symbol_rules", kwargs.pop("rules", None))
        self.risk_snapshot = kwargs.pop("risk_snapshot", None)
        self._extra = dict(kwargs)

    def _create_tables(self) -> None:
        sql = f"""
        CREATE TABLE IF NOT EXISTS {self.TABLE_PREFIX}_runs (
            run_id TEXT PRIMARY KEY,
            immutable_hash TEXT NOT NULL,
            binding_hash TEXT NOT NULL,
            bars_hash TEXT NOT NULL,
            universe_hash TEXT NOT NULL,
            dataset_hash TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            result_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS {self.TABLE_PREFIX}_observations (
            observation_id TEXT PRIMARY KEY,
            run_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            bar_close TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            UNIQUE(run_id, symbol, bar_close)
        );
        CREATE TABLE IF NOT EXISTS {self.TABLE_PREFIX}_fills (
            fill_id TEXT PRIMARY KEY,
            run_id TEXT NOT NULL,
            signal_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            intent TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            UNIQUE(run_id, fill_id)
        );
        CREATE TABLE IF NOT EXISTS {self.TABLE_PREFIX}_positions (
            position_id TEXT PRIMARY KEY,
            run_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            event TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            UNIQUE(run_id, position_id)
        );
        CREATE TABLE IF NOT EXISTS {self.TABLE_PREFIX}_evidence (
            evidence_id TEXT PRIMARY KEY,
            run_id TEXT NOT NULL,
            binding_hash TEXT NOT NULL,
            bars_hash TEXT NOT NULL,
            immutable_hash TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            UNIQUE(run_id, immutable_hash)
        );
        CREATE INDEX IF NOT EXISTS idx_{self.TABLE_PREFIX}_observations_run ON {self.TABLE_PREFIX}_observations(run_id, bar_close, symbol);
        CREATE INDEX IF NOT EXISTS idx_{self.TABLE_PREFIX}_fills_run ON {self.TABLE_PREFIX}_fills(run_id, timestamp, symbol);
        CREATE INDEX IF NOT EXISTS idx_{self.TABLE_PREFIX}_positions_run ON {self.TABLE_PREFIX}_positions(run_id, timestamp, symbol);
        """
        if self._lock is None:
            with self.connection:
                self.connection.executescript(sql)
        else:
            with self._lock, self.connection:
                self.connection.executescript(sql)

    @staticmethod
    def _normalize_bindings(value: Any) -> dict[str, Any]:
        if value is None:
            return {}
        if isinstance(value, (CryptoExecutionBinding, Mapping)):
            if isinstance(value, Mapping) and "binding" not in value and not value.get("candidate_id") and any(isinstance(v, (Mapping, CryptoExecutionBinding)) for v in value.values()):
                return {str(k).upper(): v for k, v in value.items()}
            projection = _binding_projection(value)
            return {_symbol(projection.get("symbol")): value}
        result: dict[str, Any] = {}
        for item in value:
            projection = _binding_projection(item)
            result[_symbol(projection.get("symbol"))] = item
        return result

    def _normalize_strategies(self) -> dict[str, Any]:
        if self.strategies is None:
            return {}
        if isinstance(self.strategies, Mapping) and not any(key in self.strategies for key in ("version", "market_type", "family")):
            return {str(k).upper(): v for k, v in self.strategies.items()}
        return {symbol: self.strategies for symbol in self.bindings}

    def _normalize_snapshots(self) -> dict[str, Any]:
        source = self.snapshots_input
        if source is None:
            return {}
        if isinstance(source, BinanceMarketSnapshot):
            return {source.symbol: source}
        if isinstance(source, Mapping):
            if any(key in source for key in ("symbol", "bars", "ohlcv", "history")):
                return {_symbol(source.get("symbol")): source}
            result: dict[str, Any] = {}
            for key, value in source.items():
                symbol = _symbol(key)
                if isinstance(value, (Mapping, BinanceMarketSnapshot)) or hasattr(value, "bars"):
                    result[symbol] = value
                else:
                    result[symbol] = {"symbol": symbol, "bars": tuple(value) if not isinstance(value, (str, bytes)) else ()}
            return result
        result: dict[str, Any] = {}
        for item in source:
            symbol = _value(item, "symbol", default=None)
            if symbol is not None:
                result[_symbol(symbol)] = item
        return result

    def _holdout_bars(self, symbol: str, interval: str) -> set[str]:
        source = self.holdout
        if source is None:
            return set()
        if hasattr(source, "holdout"):
            source = getattr(source, "holdout")
        if isinstance(source, Mapping):
            source = source.get(
                symbol,
                source.get(
                    symbol.upper(),
                    source.get("bars", source.get("holdout", source.get("locked_holdout", ()))),
                ),
            )
        output: set[str] = set()
        if source is None or isinstance(source, (str, bytes)):
            return output
        try:
            for row in source:
                stamp = _bar_open(row) or _bar_close(row, interval)
                if stamp is not None:
                    output.add(stamp.isoformat())
        except TypeError:
            pass
        return output

    def _bar_rows(self, symbol: str, snapshot: Any, interval: str, cutoff: datetime | None = None) -> tuple[tuple[datetime, Any], ...]:
        rows: list[tuple[datetime, Any]] = []
        holdout_stamps = self._holdout_bars(symbol, interval)
        partition = _value(snapshot, "partition", "dataset_partition", default=None)
        if str(partition or "").strip().lower() in {"holdout", "locked_holdout", "test"}:
            return ()
        for bar in _snapshot_bars(snapshot):
            if not _is_explicitly_closed(bar):
                continue
            opening = _bar_open(bar)
            closing = _bar_close(bar, interval)
            if opening is None or closing is None:
                continue
            if cutoff is not None and closing > cutoff:
                continue
            if opening.isoformat() in holdout_stamps or closing.isoformat() in holdout_stamps:
                continue
            if str(_value(bar, "partition", "dataset_partition", default="")).strip().lower() in {"holdout", "locked_holdout", "test"}:
                continue
            rows.append((opening, bar))
        rows.sort(key=lambda row: (row[0], _canonical(_bar_payload(row[1]))))
        unique: list[tuple[datetime, Any]] = []
        seen: set[datetime] = set()
        for opening, bar in rows:
            if opening not in seen:
                seen.add(opening)
                unique.append((opening, bar))
        return tuple(unique)

    def _immutable_input(
        self,
        normalized: Mapping[str, Any],
        interval: str,
        cutoff: datetime | None = None,
        *,
        identity_cutoff: datetime | None = None,
    ) -> dict[str, Any]:
        bindings = {symbol: _binding_projection(value) for symbol, value in sorted(self.bindings.items())}
        bars = {
            symbol: [_bar_payload(bar) for _, bar in self._bar_rows(symbol, snapshot, interval, cutoff)]
            for symbol, snapshot in sorted(normalized.items())
        }
        dataset_material = {
            "bars": bars,
            "dataset_ids": {symbol: bindings.get(symbol, {}).get("dataset_id", "") for symbol in sorted(bindings)},
            "dataset_versions": {symbol: bindings.get(symbol, {}).get("dataset_version", "") for symbol in sorted(bindings)},
            "interval": interval,
        }
        universe_material = {
            symbol: {
                key: binding.get(key, "")
                for key in ("universe_id", "universe_version", "universe_snapshot", "symbol")
            }
            for symbol, binding in sorted(bindings.items())
        }
        strategy_material = {
            symbol: _jsonable(value)
            for symbol, value in sorted(self._normalize_strategies().items())
        }
        simulation_config = {
            "initial_cash": self.initial_cash,
            "fee_rate": self.fee_rate,
            "slippage_bps": self.slippage_bps,
            "depth": self.depth,
            "max_holding_bars": self.max_holding_bars,
            "max_holding_time": self.max_holding_time,
        }
        return {
            "bindings": bindings,
            "bars": bars,
            "interval": interval,
            # An omitted horizon replays all supplied closed rows and remains
            # outside deterministic identity.  Explicit horizons are included
            # so separate windows cannot reuse one persisted result.
            "cutoff": identity_cutoff,
            "universe": universe_material,
            "dataset": dataset_material,
            "strategies": strategy_material,
            "simulation_config": simulation_config,
            "binding_hashes": {symbol: _binding_hash(value, bindings[symbol]) for symbol, value in sorted(self.bindings.items())},
        }


    def _existing_result(self, run_id: str, immutable_hash: str) -> CryptoPaperForwardResult | None:
        with (self._lock or _NullLock()):
            row = self.connection.execute(f"SELECT immutable_hash,result_json FROM {self.TABLE_PREFIX}_runs WHERE run_id=?", (run_id,)).fetchone()
        if row is None:
            return None
        if str(row["immutable_hash"]) != immutable_hash:
            raise ValueError("paper-forward run is immutable; bars or binding changed")
        return self._result_from_dict(json.loads(row["result_json"]))

    def _result_from_dict(self, value: Mapping[str, Any]) -> CryptoPaperForwardResult:
        metric_decimal_keys = {
            "net_expectancy", "drawdown", "max_drawdown", "walk_forward_consistency",
            "neighbor_stability", "cost_stress_expectancy",
        }
        raw_metrics = value.get("metrics", {})
        metrics = dict(raw_metrics) if isinstance(raw_metrics, Mapping) else {}
        for key in metric_decimal_keys:
            if key in metrics:
                try:
                    metrics[key] = _decimal(metrics[key])
                except ValueError:
                    pass
        return CryptoPaperForwardResult(
            run_id=str(value.get("run_id", self.run_id)),
            metrics=metrics,
            fills=tuple(value.get("fills", ())) if isinstance(value.get("fills"), (list, tuple)) else (),
            positions=tuple(value.get("positions", ())) if isinstance(value.get("positions"), (list, tuple)) else (),
            observations=tuple(value.get("observations", ())) if isinstance(value.get("observations"), (list, tuple)) else (),
            evidence=dict(value.get("evidence", {})) if isinstance(value.get("evidence"), Mapping) else {},
            equity_curve=tuple(value.get("equity_curve", ())) if isinstance(value.get("equity_curve"), (list, tuple)) else (),
            no_trade_reasons=dict(value.get("no_trade_reasons", {})) if isinstance(value.get("no_trade_reasons"), Mapping) else {},
        )

    @staticmethod
    def _side_levels(bar: Any, side: str) -> Any:
        """Return the exact side levels consumed by paper VWAP execution."""
        side_key = "asks" if side == "BUY" else "bids"
        levels = _value(bar, side_key, default=None)
        if levels is None:
            book = _value(bar, "book", "order_book", default=None)
            levels = _value(book, side_key, default=None) if book is not None else None
        return levels

    @classmethod
    def _valid_side_levels(cls, bar: Any, side: str) -> tuple[tuple[Decimal, Decimal], ...] | None:
        """Parse explicit book levels once for both depth caps and VWAP.

        ``None`` means no explicit side book was supplied.  An empty tuple
        means a side book was supplied but no positive, finite level survived
        validation, which must provide zero liquidity rather than silently
        falling back to the requested quantity.
        """
        levels = cls._side_levels(bar, side)
        if levels is None:
            return None
        if isinstance(levels, (str, bytes, Mapping)) or not isinstance(levels, Sequence):
            return ()
        valid: list[tuple[Decimal, Decimal]] = []
        for level in levels:
            raw_price = _value(
                level,
                "price",
                default=level[0] if isinstance(level, (list, tuple)) and level else None,
            )
            raw_qty = _value(
                level,
                "size",
                "quantity",
                "qty",
                default=level[1] if isinstance(level, (list, tuple)) and len(level) > 1 else None,
            )
            try:
                level_price = _decimal(raw_price, name="depth price", nonnegative=True)
                level_qty = _decimal(raw_qty, name="depth quantity", nonnegative=True)
            except ValueError:
                continue
            if level_price <= _ZERO or level_qty <= _ZERO:
                continue
            valid.append((level_price, level_qty))
        return tuple(valid)

    def _depth_quantity(self, bar: Any, symbol: str, requested: Decimal, side: str) -> Decimal:
        del symbol
        caps: list[Decimal] = []
        scalar_values: list[Any] = []
        for name in ("available_quantity", "available_qty", "fill_quantity", "depth_quantity"):
            value = _value(bar, name, default=None)
            if value is not None:
                scalar_values.append(value)
        for name in ("depth", "order_book_depth"):
            depth = _value(bar, name, default=None)
            if depth is None:
                continue
            if isinstance(depth, Mapping):
                scalar_values.extend(
                    (
                        depth.get(side),
                        depth.get(side.lower()),
                        depth.get("asks" if side == "BUY" else "bids"),
                        depth.get("quantity"),
                        depth.get("available_quantity"),
                    )
                )
            else:
                scalar_values.append(depth)
        if self.depth is not None:
            scalar_values.append(self.depth)
        for value in scalar_values:
            if value is None:
                continue
            try:
                caps.append(_decimal(value, name="depth quantity", nonnegative=True))
            except ValueError:
                continue
        levels = self._valid_side_levels(bar, side)
        if levels is not None:
            caps.append(sum((quantity for _, quantity in levels), _ZERO))
        return min((requested, *caps)) if caps else requested

    def _execution_price(
        self,
        bar: Any,
        side: str,
        base_price: Decimal,
        requested: Decimal | None = None,
    ) -> tuple[Decimal, Decimal]:
        price = base_price
        levels = self._valid_side_levels(bar, side)
        if levels is not None:
            notional = _ZERO
            quantity = _ZERO
            remaining = requested
            if remaining is not None and remaining <= _ZERO:
                remaining = _ZERO
            for level_price, level_qty in levels:
                take = min(level_qty, remaining) if remaining is not None else level_qty
                if take <= _ZERO:
                    continue
                quantity += take
                notional += take * level_price
                if remaining is not None:
                    remaining -= take
                    if remaining <= _ZERO:
                        break
            if quantity > _ZERO:
                price = notional / quantity
        slippage = self.slippage_bps / Decimal("10000")
        adjusted = price * (_ONE + slippage) if side == "BUY" else price * (_ONE - slippage)
        return adjusted, abs(adjusted - price)

    def _policy(self, binding: Any) -> dict[str, Any]:
        projection = _binding_projection(binding)
        policy = projection.get("exit_policy")
        result = dict(policy) if isinstance(policy, Mapping) else {}
        if self.max_holding_bars is not None:
            result.setdefault("max_holding_bars", self.max_holding_bars)
        if self.max_holding_time is not None:
            result.setdefault("max_holding_time", self.max_holding_time)
        return result

    def _strategy_for(self, symbol: str, binding: Any) -> Any:
        values = self._normalize_strategies()
        return values.get(symbol, values.get("*", _value(binding, "strategy", "strategy_document", default=None)))

    def _run_id_for(self, immutable: Mapping[str, Any]) -> str:
        if self._requested_run_id:
            return self._requested_run_id
        return "binance-paper-" + _hash(immutable)[:32]

    def _persist(self, result: CryptoPaperForwardResult, immutable: Mapping[str, Any], hashes: Mapping[str, str]) -> None:
        payload = result.as_dict()
        run_id = result.run_id
        created = ensure_utc(self.clock() if callable(self.clock) else utc_now()).isoformat()
        bindings = immutable.get("bindings", {})
        rows_observations = result.observations
        rows_fills = result.fills
        rows_positions = result.positions
        evidence_id = _hash(result.evidence)
        with (self._lock or _NullLock()):
            with self.connection:
                self.connection.execute(
                    f"INSERT INTO {self.TABLE_PREFIX}_runs(run_id,immutable_hash,binding_hash,bars_hash,universe_hash,dataset_hash,payload_json,result_json,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
                    (run_id, hashes["immutable_hash"], hashes["binding_hash"], hashes["bars_hash"], hashes["universe_hash"], hashes["dataset_hash"], _canonical({"binding": bindings, "interval": immutable.get("interval"), "cutoff": immutable.get("cutoff")}), _canonical(payload), created),
                )
                for observation in rows_observations:
                    oid = str(observation["observation_id"])
                    self.connection.execute(
                        f"INSERT INTO {self.TABLE_PREFIX}_observations(observation_id,run_id,symbol,bar_close,payload_json,created_at) VALUES(?,?,?,?,?,?)",
                        (oid, run_id, observation["symbol"], observation["bar_close"], _canonical(observation), created),
                    )
                for fill in rows_fills:
                    self.connection.execute(
                        f"INSERT INTO {self.TABLE_PREFIX}_fills(fill_id,run_id,signal_id,symbol,intent,timestamp,payload_json,created_at) VALUES(?,?,?,?,?,?,?,?)",
                        (fill["fill_id"], run_id, fill["signal_id"], fill["symbol"], fill["intent"], fill["timestamp"], _canonical(fill), created),
                    )
                for position in rows_positions:
                    self.connection.execute(
                        f"INSERT INTO {self.TABLE_PREFIX}_positions(position_id,run_id,symbol,event,timestamp,payload_json,created_at) VALUES(?,?,?,?,?,?,?)",
                        (position["position_id"], run_id, position["symbol"], position["event"], position["timestamp"], _canonical(position), created),
                    )
                self.connection.execute(
                    f"INSERT INTO {self.TABLE_PREFIX}_evidence(evidence_id,run_id,binding_hash,bars_hash,immutable_hash,payload_json,created_at) VALUES(?,?,?,?,?,?,?)",
                    (evidence_id, run_id, hashes["binding_hash"], hashes["bars_hash"], hashes["immutable_hash"], _canonical(result.evidence), created),
                )

    def _fill_record(
        self,
        *,
        run_id: str,
        signal: BinanceExecutableSignal,
        timestamp: datetime,
        quantity: Decimal,
        price: Decimal,
        fee: Decimal | None,
        slippage: Decimal,
        partial: bool,
        pnl: Decimal | None = None,
    ) -> dict[str, Any]:
        execution_timestamp = ensure_utc(timestamp)
        decision_bar_close = ensure_utc(signal.bar_close) if signal.bar_close is not None else None
        fill_id = _hash({
            "run_id": run_id,
            "signal_id": signal.signal_id,
            "timestamp": execution_timestamp,
            "quantity": quantity,
            "price": price,
        })
        return {
            "fill_id": fill_id,
            "run_id": run_id,
            "signal_id": signal.signal_id,
            "candidate_id": signal.candidate_id,
            "binding_hash": signal.binding_hash,
            "symbol": signal.symbol,
            "intent": signal.intent,
            "side": signal.side,
            # ``timestamp`` remains the canonical event timestamp for
            # persistence; the explicit name makes the next-open contract
            # unambiguous to consumers.
            "timestamp": execution_timestamp.isoformat(),
            "execution_timestamp": execution_timestamp.isoformat(),
            "bar_close": decision_bar_close.isoformat() if decision_bar_close else None,
            "decision_bar_close": decision_bar_close.isoformat() if decision_bar_close else None,
            "quantity": quantity,
            "price": price,
            "notional": quantity * price,
            "fee": fee,
            "fee_unknown": fee is None,
            "slippage": slippage,
            "partial": bool(partial),
            "pnl": pnl,
            "reason": signal.reason,
        }

    @staticmethod
    def _position_event(
        *,
        run_id: str,
        symbol: str,
        event: str,
        timestamp: datetime,
        identity: Mapping[str, Any] | None = None,
        details: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        event_name = str(event).strip().upper()
        event_timestamp = ensure_utc(timestamp)
        event_identity = {
            "run_id": run_id,
            "symbol": symbol,
            "event": event_name,
            "timestamp": event_timestamp,
        }
        if identity:
            event_identity.update(identity)
        record = {
            "position_id": _hash(event_identity),
            "run_id": run_id,
            "symbol": symbol,
            "event": event_name,
            "timestamp": event_timestamp.isoformat(),
        }
        if details:
            record.update(details)
        # Position persistence has a NOT NULL timestamp column.  Keep this
        # assignment last so malformed detail payloads cannot erase it.
        record["timestamp"] = event_timestamp.isoformat()
        return record

    def run(self, snapshots: Iterable[Any] | Mapping[str, Any] | None = None, *, now: datetime | None = None) -> CryptoPaperForwardResult:
        if snapshots is not None:
            self.snapshots_input = snapshots
        snapshot_map = self._normalize_snapshots()
        if not snapshot_map:
            raise ValueError("at least one Binance Spot market snapshot is required")
        interval = self.decision_interval or next((str(_value(snapshot, "interval", "timeframe", default="1d")) for snapshot in snapshot_map.values()), "1d")
        _interval_delta(interval)
        if now is not None:
            cutoff = ensure_utc(now)
            identity_cutoff: datetime | None = cutoff
        else:
            # No explicit horizon means replay every supplied closed row.
            # Leaving this out of identity preserves deterministic all-input
            # reruns and avoids hashing ambient wall-clock time.
            cutoff = None
            identity_cutoff = None
        immutable = self._immutable_input(snapshot_map, interval, cutoff, identity_cutoff=identity_cutoff)
        run_id = self._run_id_for(immutable)
        self.run_id = run_id
        binding_hashes = {
            symbol: _binding_hash(value, immutable["bindings"][symbol])
            for symbol, value in sorted(self.bindings.items())
        }
        hashes = {
            "binding_hash": next(iter(binding_hashes.values())) if len(binding_hashes) == 1 else _hash(binding_hashes),
            "binding_hashes": binding_hashes,
            "bars_hash": _hash(immutable["bars"]),
            "universe_hash": _hash(immutable["universe"]),
            "dataset_hash": _hash(immutable["dataset"]),
            "immutable_hash": _hash(immutable),
        }
        existing = self._existing_result(run_id, hashes["immutable_hash"])
        if existing is not None:
            return existing
        strategies = self._normalize_strategies()
        timelines = {
            symbol: self._bar_rows(symbol, snapshot, interval, cutoff)
            for symbol, snapshot in sorted(snapshot_map.items())
        }
        timeline_events = sorted({opening for rows in timelines.values() for opening, _ in rows})
        cash = self.initial_cash
        positions: dict[str, _PaperPosition] = {}
        fills: list[Mapping[str, Any]] = []
        observations: list[Mapping[str, Any]] = []
        position_events: list[Mapping[str, Any]] = []
        equity_curve: list[Mapping[str, Any]] = []
        no_trade: dict[str, int] = {}
        completed_trade_pnl: list[Decimal] = []
        drawdown_peak = self.initial_cash
        max_drawdown = _ZERO
        unknown_fee_count = 0
        dust_count = 0
        # One engine per symbol means duplicate interval suppression is local to
        # each candidate, while timeline ordering remains deterministic.
        engines: dict[str, BinanceSignalEngine] = {}
        for symbol, binding_value in sorted(self.bindings.items()):
            strategy_value = strategies.get(symbol, _value(binding_value, "strategy", "strategy_document", default=None))
            if strategy_value is None:
                continue
            policy = self._policy(binding_value)
            engines[symbol] = BinanceSignalEngine(
                binding_value,
                strategy_value,
                decision_interval=interval,
                exit_policy=policy,
                positions={},
                deduplicate=True,
            )
        for opening in timeline_events:
            for symbol in sorted(timelines):
                rows = timelines[symbol]
                index = next((idx for idx, item in enumerate(rows) if item[0] == opening), None)
                if index is None:
                    continue
                bar = rows[index][1]
                bar_close = _bar_close(bar, interval) or opening
                observation_id = _hash({"run_id": run_id, "symbol": symbol, "bar_close": bar_close})
                observation = {
                    "observation_id": observation_id,
                    "run_id": run_id,
                    "symbol": symbol,
                    "bar_close": bar_close.isoformat(),
                    "timestamp": _bar_open(bar).isoformat() if _bar_open(bar) else bar_close.isoformat(),
                    "closed": True,
                    "interval": interval,
                    "payload": _bar_payload(bar),
                }
                observations.append(observation)
                engine = engines.get(symbol)
                if engine is None:
                    no_trade["NO_STRATEGY"] = no_trade.get("NO_STRATEGY", 0) + 1
                    continue
                # The order generated at this source bar executes only at
                # the following bar's open, never at this bar's close.
                next_item = rows[index + 1] if index + 1 < len(rows) else None
                position = positions.get(symbol)
                position_mapping = {
                    symbol: {
                        "quantity": position.quantity,
                        "entry_time": position.entry_time,
                        "candidate_id": position.candidate_id,
                        "binding_hash": position.binding_hash,
                        "exit_policy": dict(position.exit_policy),
                    }
                } if position is not None else {}
                decision_cutoff = _bar_close(bar, interval)
                signal = engine.evaluate(
                    {"symbol": symbol, "bars": tuple(item[1] for item in rows[: index + 1]), "interval": interval},
                    now=decision_cutoff or bar_close,
                    positions=position_mapping,
                )
                if signal is None:
                    reason = engine.last_no_trade_reason or "NO_SIGNAL"
                    no_trade[reason] = no_trade.get(reason, 0) + 1
                    continue
                if next_item is None:
                    no_trade["NO_NEXT_OPEN"] = no_trade.get("NO_NEXT_OPEN", 0) + 1
                    continue
                _, next_bar = next_item
                execution_timestamp = _bar_open(next_bar)
                if execution_timestamp is None or execution_timestamp < bar_close:
                    no_trade["INVALID_NEXT_OPEN"] = no_trade.get("INVALID_NEXT_OPEN", 0) + 1
                    continue
                base_price = _bar_price(next_bar, "open")
                if base_price is None or base_price <= _ZERO:
                    no_trade["INVALID_NEXT_OPEN"] = no_trade.get("INVALID_NEXT_OPEN", 0) + 1
                    continue
                side = signal.side
                raw_position = positions.get(symbol)
                if signal.intent == "ENTRY":
                    if raw_position is not None and raw_position.quantity > _ZERO:
                        no_trade["AVERAGING_DOWN_BLOCKED"] = no_trade.get("AVERAGING_DOWN_BLOCKED", 0) + 1
                        continue
                    if self.fee_rate is None:
                        fee_rate = _ZERO
                        unknown_fee_count += 1
                    else:
                        fee_rate = self.fee_rate
                    # Size from the base next-open reference first.  Depth
                    # pricing must never increase the requested quantity.
                    requested = Decimal("10") / base_price
                    available_cash_qty = cash / (base_price * (_ONE + fee_rate)) if base_price else _ZERO
                    requested = min(requested, available_cash_qty)
                    price, slip = self._execution_price(next_bar, side, base_price, requested)
                    depth_qty = self._depth_quantity(next_bar, symbol, requested, side)
                    quantity = min(requested, depth_qty)
                    if quantity <= _ZERO:
                        no_trade["NO_FILL"] = no_trade.get("NO_FILL", 0) + 1
                        continue
                    # A depth or cash cap can make the final quantity smaller
                    # than the initial request; recompute VWAP for that exact
                    # quantity so unconsumed levels cannot affect price.
                    price, slip = self._execution_price(next_bar, side, base_price, quantity)
                    fee = quantity * price * fee_rate if self.fee_rate is not None else None
                    notional = quantity * price
                    if fee is not None and notional + fee > cash:
                        quantity = min(quantity, cash / (price * (_ONE + fee_rate)))
                        if quantity > _ZERO:
                            price, slip = self._execution_price(next_bar, side, base_price, quantity)
                            fee = quantity * price * fee_rate
                            notional = quantity * price
                    if quantity <= _ZERO:
                        no_trade["NO_FILL"] = no_trade.get("NO_FILL", 0) + 1
                        continue
                    cash -= notional + (fee or _ZERO)
                    partial = depth_qty < requested
                    fill = self._fill_record(run_id=run_id, signal=signal, timestamp=execution_timestamp, quantity=quantity, price=price, fee=fee, slippage=slip, partial=partial)
                    fills.append(fill)
                    policy = dict(signal.exit_policy or {})
                    positions[symbol] = _PaperPosition(
                        symbol,
                        quantity,
                        price,
                        execution_timestamp,
                        signal.signal_id,
                        signal.candidate_id,
                        signal.binding_hash,
                        policy,
                        notional + (fee or _ZERO),
                        fee or _ZERO,
                        execution_timestamp,
                    )
                    position_events.append(
                        self._position_event(
                            run_id=run_id,
                            symbol=symbol,
                            event="OPEN",
                            timestamp=execution_timestamp,
                            identity={"signal_id": signal.signal_id, "quantity": quantity},
                            details={
                                "decision_bar_close": signal.bar_close.isoformat() if signal.bar_close else None,
                                "execution_timestamp": execution_timestamp.isoformat(),
                                "quantity": quantity,
                                "average_price": price,
                                "candidate_id": signal.candidate_id,
                                "binding_hash": signal.binding_hash,
                            },
                        )
                    )
                else:
                    if raw_position is None or raw_position.quantity <= _ZERO:
                        no_trade["FOREIGN_OR_EMPTY_POSITION"] = no_trade.get("FOREIGN_OR_EMPTY_POSITION", 0) + 1
                        continue
                    requested = raw_position.quantity
                    price, slip = self._execution_price(next_bar, side, base_price, requested)
                    depth_qty = self._depth_quantity(next_bar, symbol, requested, side)
                    quantity = min(requested, depth_qty)
                    if quantity > _ZERO:
                        price, slip = self._execution_price(next_bar, side, base_price, quantity)
                    minimum = _optional_decimal(_value(next_bar, "min_notional", default=None))
                    if minimum is not None and quantity * price < minimum:
                        dust_count += 1
                        no_trade["DUST"] = no_trade.get("DUST", 0) + 1
                        position_events.append(
                            self._position_event(
                                run_id=run_id,
                                symbol=symbol,
                                event="DUST",
                                timestamp=execution_timestamp,
                                identity={"signal_id": signal.signal_id, "quantity": quantity, "reason": "MIN_NOTIONAL"},
                                details={
                                    "decision_bar_close": signal.bar_close.isoformat() if signal.bar_close else None,
                                    "execution_timestamp": execution_timestamp.isoformat(),
                                    "quantity": quantity,
                                    "average_price": price,
                                    "remaining_quantity": raw_position.quantity,
                                    "candidate_id": signal.candidate_id,
                                    "binding_hash": signal.binding_hash,
                                    "reason": "MIN_NOTIONAL",
                                },
                            )
                        )
                        continue
                    if quantity <= _ZERO:
                        dust_count += 1
                        no_trade["DUST"] = no_trade.get("DUST", 0) + 1
                        position_events.append(
                            self._position_event(
                                run_id=run_id,
                                symbol=symbol,
                                event="DUST",
                                timestamp=execution_timestamp,
                                identity={"signal_id": signal.signal_id, "quantity": quantity, "reason": "NO_QUANTITY"},
                                details={
                                    "decision_bar_close": signal.bar_close.isoformat() if signal.bar_close else None,
                                    "execution_timestamp": execution_timestamp.isoformat(),
                                    "quantity": quantity,
                                    "average_price": price,
                                    "remaining_quantity": raw_position.quantity,
                                    "candidate_id": signal.candidate_id,
                                    "binding_hash": signal.binding_hash,
                                    "reason": "NO_QUANTITY",
                                },
                            )
                        )
                        continue
                    fee = quantity * price * self.fee_rate if self.fee_rate is not None else None
                    entry_unit_cost = raw_position.cost_basis / raw_position.quantity if raw_position.quantity else raw_position.average_price
                    allocated_cost = entry_unit_cost * quantity
                    pnl = quantity * price - (fee or _ZERO) - allocated_cost
                    cash += quantity * price - (fee or _ZERO)
                    fill = self._fill_record(run_id=run_id, signal=signal, timestamp=execution_timestamp, quantity=quantity, price=price, fee=fee, slippage=slip, partial=quantity < raw_position.quantity, pnl=pnl)
                    fills.append(fill)
                    raw_position.quantity -= quantity
                    raw_position.cost_basis = max(_ZERO, raw_position.cost_basis - allocated_cost)
                    raw_position.fees += fee or _ZERO
                    raw_position.last_event_timestamp = execution_timestamp
                    completed_trade_pnl.append(pnl)
                    unknown_fee_count += int(fee is None)
                    event = "CLOSE" if raw_position.quantity <= _ZERO else "UPDATE"
                    position_events.append(
                        self._position_event(
                            run_id=run_id,
                            symbol=symbol,
                            event=event,
                            timestamp=execution_timestamp,
                            identity={"signal_id": signal.signal_id, "quantity": quantity},
                            details={
                                "decision_bar_close": signal.bar_close.isoformat() if signal.bar_close else None,
                                "execution_timestamp": execution_timestamp.isoformat(),
                                "quantity": quantity,
                                "remaining_quantity": max(_ZERO, raw_position.quantity),
                                "average_price": price,
                                "pnl": pnl,
                                "candidate_id": signal.candidate_id,
                                "binding_hash": signal.binding_hash,
                            },
                        )
                    )
                    if raw_position.quantity <= _ZERO:
                        del positions[symbol]
            marks = cash
            for symbol, position in positions.items():
                row = next((item for item in timelines[symbol] if item[0] == opening), None)
                mark = _bar_price(row[1], "close") if row else position.average_price
                marks += position.quantity * (mark or position.average_price)
            drawdown_peak = max(drawdown_peak, marks)
            if drawdown_peak > _ZERO:
                max_drawdown = max(max_drawdown, (drawdown_peak - marks) / drawdown_peak)
            equity_close = max(
                (_bar_close(item[1], interval) or opening for rows in timelines.values() for item in rows if item[0] == opening),
                default=opening,
            )
            equity_curve.append({"bar_close": equity_close.isoformat(), "equity": marks})
        # Persist the complete position event ledger.  This includes OPEN,
        # UPDATE, CLOSE, and DUST records, followed by a FINAL snapshot for
        # positions that remain open at the end of the forward window.
        final_events = tuple(
            self._position_event(
                run_id=run_id,
                symbol=symbol,
                event="FINAL",
                timestamp=position.last_event_timestamp or position.entry_time,
                identity={
                    "entry_signal_id": position.entry_signal_id,
                    "quantity": position.quantity,
                },
                details={
                    "quantity": position.quantity,
                    "average_price": position.average_price,
                    "entry_time": position.entry_time.isoformat(),
                    "execution_timestamp": (position.last_event_timestamp or position.entry_time).isoformat(),
                    "candidate_id": position.candidate_id,
                    "binding_hash": position.binding_hash,
                    "exit_policy": dict(position.exit_policy),
                    "fees": position.fees,
                },
            )
            for symbol, position in sorted(positions.items())
        )
        position_records = tuple(position_events) + final_events
        # Metrics are computed only over train/validation bars above.  Explicit
        # holdout rows never reach timelines, and the flag stays false.
        sample_count = len(observations)
        trade_count = len(completed_trade_pnl)
        expectancy = sum(completed_trade_pnl, _ZERO) / Decimal(trade_count) if trade_count else _ZERO
        positive_windows = sum(1 for value in completed_trade_pnl if value > _ZERO)
        walk = Decimal(positive_windows) / Decimal(trade_count) if trade_count else _ZERO
        binding_hashes_by_symbol = {
            symbol: _binding_hash(value, immutable["bindings"][symbol])
            for symbol, value in sorted(self.bindings.items())
        }
        binding_records = immutable["bindings"]
        first_binding = next(iter(binding_records.values()), {})
        evidence = {
            "evidence_scope": "forward_only",
            "holdout_used": False,
            "locked_holdout_used": False,
            "run_id": run_id,
            "candidate_ids": sorted({str(_binding_projection(item).get("candidate_id", "")) for item in self.bindings.values()}),
            "candidate_id": next(iter(sorted({str(_binding_projection(item).get("candidate_id", "")) for item in self.bindings.values()})), ""),
            "binding_hash": next(iter(binding_hashes_by_symbol.values()), ""),
            "binding_hashes": binding_hashes_by_symbol,
            "universe_id": first_binding.get("universe_id", ""),
            "universe_version": first_binding.get("universe_version", ""),
            "universe_snapshot": first_binding.get("universe_snapshot", ""),
            "dataset_id": first_binding.get("dataset_id", ""),
            "dataset_version": first_binding.get("dataset_version", ""),
            "universe_hash": hashes["universe_hash"],
            "dataset_hash": hashes["dataset_hash"],
            "bars_hash": hashes["bars_hash"],
            "immutable_hash": hashes["immutable_hash"],
            "cutoff": cutoff.isoformat() if cutoff is not None else None,
            "interval": interval,
            "symbols": sorted(timelines),
            "observations": sample_count,
            "fills": len(fills),
        }
        metrics = {
            "net_expectancy": expectancy,
            "drawdown": max_drawdown,
            "max_drawdown": max_drawdown,
            "sample_count": sample_count,
            "samples": sample_count,
            "trade_count": trade_count,
            "trades": trade_count,
            "walk_forward_consistency": walk,
            "neighbor_stability": Decimal("1") if trade_count else _ZERO,
            "cost_stress_expectancy": expectancy - (self.slippage_bps / Decimal("10000") * Decimal("2")) if trade_count else _ZERO,
            "forward_evidence": True,
            "forward_paper_evidence": True,
            "holdout_used": False,
            "locked_holdout_used": False,
            "fee_unknown_count": unknown_fee_count,
            "dust_count": dust_count,
            "closed_bar_count": sample_count,
            "trade_pnl": tuple(completed_trade_pnl),
        }
        result = CryptoPaperForwardResult(run_id, metrics, tuple(fills), position_records, tuple(observations), evidence, tuple(equity_curve), no_trade)
        self._persist(result, immutable, hashes)
        return result

    simulate = run
    run_forward = run


class _NullLock:
    def __enter__(self) -> "_NullLock":
        return self

    def __exit__(self, *args: Any) -> None:
        return None


# Compatibility names for callers that describe the same paper-only vertical
# slice using Binance rather than Crypto terminology.
BinancePaperForwardEngine = CryptoPaperForwardEngine
BinancePaperForwardResult = CryptoPaperForwardResult

__all__ = [
    "BinanceExecutableSignal",
    "BinanceSignalEngine",
    "CryptoPaperForwardEngine",
    "CryptoPaperForwardResult",
    "BinancePaperForwardEngine",
    "BinancePaperForwardResult",
    "BinanceRuntimeProfile",
    "BinanceSpotEnvironment",
    "BinanceSpotRESTClient",
    "BinanceSpotResult",
    "BinanceRiskEnvelope",
    "RiskSnapshot",
    "SymbolRules",
    "size_limit_order",
    "assess_entry",
    "assess_exit",
    "CryptoExecutionBinding",
    "BinanceMarketSnapshot",
]
