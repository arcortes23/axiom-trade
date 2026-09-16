"""Continuous deterministic paper execution for frozen forward tests.

The engine consumes only observations already available to the caller or public
read-only providers. It never has an order-submission or credential path.
"""
from __future__ import annotations
from copy import deepcopy
from dataclasses import dataclass, replace
from datetime import date, datetime
from enum import Enum
import hashlib
import math
from decimal import Decimal, InvalidOperation, ROUND_DOWN, ROUND_UP
from itertools import islice
from types import MappingProxyType
from typing import Any, Iterable, Mapping, Sequence
from .domain import (
    Fill,
    MarketType,
    OrderBookLevel,
    OrderBookSnapshot,
    PredictionMarketSnapshot,
    ResolvedContract,
    SettlementState,
    Side,
    ensure_utc,
    parse_timestamp,
    to_record,
    utc_now,
)
from .forward import (
    ForwardTestSpec,
    _content_hash,
    _normalized_strategy_document,
    _paper_assumption_costs,
    _paper_assumptions_explicit,
    _SUPPORTED_PAPER_SIZING_MODELS,
)
from .paper import PaperTrader, PaperTradingConfig, SHADOW_METADATA_KEYS, _shadow_metadata
from .portfolio import Portfolio, Position
from .risk import RiskEngine, RiskLimits
from .storage import AxiomStore
from .strategy.signals import (
    evaluate_model_probability_evidence,
    evaluate_signal_evaluation,
)

_MAX_RUN_OBSERVATIONS = 100_000
_MAX_PAPER_OPENING_ROWS = 100_000
_MAX_PAPER_UNRESOLVED_EVENTS = 256
_MAX_PAPER_FILL_ROWS = 100_000
PAPER_STATE_EXECUTION_BINDING_MISMATCH = "PAPER_STATE_EXECUTION_BINDING_MISMATCH"
PAPER_STATE_OPERATIONAL_COUNTERS_INVALID = "PAPER_STATE_OPERATIONAL_COUNTERS_INVALID"
PAPER_STATE_FILL_RESTORE_OVERFLOW = "PAPER_STATE_FILL_RESTORE_OVERFLOW"
PAPER_STATE_FILL_RESTORE_COUNT_MISMATCH = "PAPER_STATE_FILL_RESTORE_COUNT_MISMATCH"

_UNRESOLVED_OPERATIONAL_STATUSES = frozenset(
    {
        "UNKNOWN",
        "PENDING",
        "NEW",
        "OPEN",
        "SUBMITTING",
        "SUBMITTED",
        "ACCEPTED",
        "ACKNOWLEDGED",
        "MATCHED",
        "FILLED",
        "PARTIAL",
        "PARTIALLY_FILLED",
        "PARTIAL_FILL",
        "LIVE",
        "DELAYED",
        "PREPARED",
        "RESERVED",
        "HELD",
        "ATTEMPTED",
        "EXIT",
        "EXIT_PENDING",
        "EXIT_REQUESTED",
        "RECONCILE",
        "RECONCILE_PENDING",
        "RECONCILIATION_PENDING",
        "MANAGEMENT_BLOCKED",
    }
)
_TERMINAL_OPERATIONAL_STATUSES = frozenset(
    {
        "REJECTED",
        "FAILED",
        "ERROR",
        "CANCELLED",
        "CANCELED",
        "EXPIRED",
        "SETTLED",
        "RESOLVED",
        "VOID",
        "NO_SIGNAL",
        "NO_FILL",
        "RISK_REJECTED",
        "FULL_FILL",
    }
)
class _DuplicatePaperObservation(Exception):
    """Store-level deduplication must skip execution atomically."""


def _policy_number(value: Any, name: str, *, optional: bool = False) -> float | None:
    if optional and value in (None, ""):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        raise ValueError(f"{name} must be numeric") from None
    if not math.isfinite(number) or number < 0:
        raise ValueError(f"{name} must be finite and non-negative")
    return number


def _policy_decimal_text(value: Any) -> str | None:
    if value is None:
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return str(value)
    if not parsed.is_finite():
        return str(value)
    return format(parsed.normalize(), "f")


def _freeze_policy_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType(
            {key: _freeze_policy_value(item) for key, item in value.items()}
        )
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_policy_value(item) for item in value)
    if isinstance(value, set):
        return frozenset(_freeze_policy_value(item) for item in value)
    return value


def _thaw_policy_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _thaw_policy_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_thaw_policy_value(item) for item in value]
    if isinstance(value, (set, frozenset)):
        return [_thaw_policy_value(item) for item in value]
    return value

def _nonnegative_finite(value: Any, *, integer: bool = False) -> bool:
    if isinstance(value, bool):
        return False
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return False
    if not math.isfinite(number) or number < 0:
        return False
    return not integer or number.is_integer()


def _operational_state_blocker() -> dict[str, Any]:
    return {
        "status": "BLOCKED",
        "blocker": PAPER_STATE_OPERATIONAL_COUNTERS_INVALID,
        "reason_code": PAPER_STATE_OPERATIONAL_COUNTERS_INVALID,
        "reason": "persisted operational paper counters are malformed",
        "retryable": False,
        "non_retryable": True,
        "execution_skipped": True,
    }


@dataclass(frozen=True, slots=True)
class OperationalPaperPolicy:
    """Frozen monetary envelope for one isolated paper experiment.

    The defaults intentionally mirror the active Polymarket canary envelope,
    while remaining a paper-only decision input.  ``None`` is used only for
    optional per-market/per-event/lifetime budgets.
    """

    max_all_in_buy_usd: float = 1.0
    max_fee_reserve_usd: float = 0.01
    max_gross_daily_buy_usd: float = 5.0
    max_aggregate_open_cost_usd: float = 5.0
    max_aggregate_exposure_usd: float = 5.0
    max_positions: int = 3
    max_submitted_orders_per_day: int = 5
    realized_loss_entry_stop_usd: float = 2.0
    equity_loss_entry_stop_usd: float = 2.0
    per_market_buy_cap_usd: float | None = None
    per_event_buy_cap_usd: float | None = None
    cumulative_buy_cap_usd: float | None = None
    cumulative_declared_loss_entry_stop_usd: float | None = None
    pending_orders: tuple[Mapping[str, Any], ...] = ()
    require_market_rules: bool = True
    allow_strategy_overlap: bool = False
    allow_event_overlap: bool = False

    def __post_init__(self) -> None:
        for name in (
            "max_all_in_buy_usd",
            "max_fee_reserve_usd",
            "max_gross_daily_buy_usd",
            "max_aggregate_open_cost_usd",
            "max_aggregate_exposure_usd",
            "realized_loss_entry_stop_usd",
            "equity_loss_entry_stop_usd",
        ):
            value = _policy_number(getattr(self, name), name)
            object.__setattr__(self, name, value)
        for name in (
            "per_market_buy_cap_usd",
            "per_event_buy_cap_usd",
            "cumulative_buy_cap_usd",
            "cumulative_declared_loss_entry_stop_usd",
        ):
            value = _policy_number(getattr(self, name), name, optional=True)
            object.__setattr__(self, name, value)
        if isinstance(self.max_positions, bool) or int(self.max_positions) != self.max_positions or int(self.max_positions) < 1:
            raise ValueError("max_positions must be a positive integer")
        if isinstance(self.max_submitted_orders_per_day, bool) or int(self.max_submitted_orders_per_day) != self.max_submitted_orders_per_day or int(self.max_submitted_orders_per_day) < 1:
            raise ValueError("max_submitted_orders_per_day must be a positive integer")
        pending = tuple(_freeze_policy_value(item) for item in self.pending_orders)
        object.__setattr__(self, "pending_orders", pending)
        for name in ("require_market_rules", "allow_strategy_overlap", "allow_event_overlap"):
            if not isinstance(getattr(self, name), bool):
                raise TypeError(f"{name} must be a boolean")

    @classmethod
    def from_value(cls, value: Any) -> "OperationalPaperPolicy":
        if isinstance(value, cls):
            return value
        source = value
        active = getattr(source, "active_limits", None)
        if callable(active):
            source = active()
        elif callable(getattr(source, "active_settings", None)):
            source = source.active_settings()
        if isinstance(source, Mapping):
            source = source.get("values", source.get("limits", source))
        if not isinstance(source, Mapping):
            raise TypeError("operational_policy must be a mapping or policy object")
        aliases = {
            "target_notional_usd": "max_all_in_buy_usd",
            "max_exposure_usd": "max_aggregate_exposure_usd",
            "max_open_positions": "max_positions",
            "max_orders_per_day": "max_submitted_orders_per_day",
            "daily_buy_cap_usd": "max_gross_daily_buy_usd",
            "aggregate_open_cost_usd": "max_aggregate_open_cost_usd",
            "aggregate_exposure_usd": "max_aggregate_exposure_usd",
            "orders_per_day": "max_submitted_orders_per_day",
            "realized_loss_stop_usd": "realized_loss_entry_stop_usd",
            "equity_loss_stop_usd": "equity_loss_entry_stop_usd",
            "max_daily_loss_usd": "realized_loss_entry_stop_usd",
            "cumulative_loss_entry_stop_usd": "cumulative_declared_loss_entry_stop_usd",
        }
        values: dict[str, Any] = {}
        known = set(cls.__dataclass_fields__)
        for raw_name, raw_value in source.items():
            name = str(raw_name).strip().lower().replace("-", "_")
            name = aliases.get(name, name)
            if name in known:
                values[name] = raw_value
        return cls(**values)

    def as_record(self) -> dict[str, Any]:
        values = {
            name: getattr(self, name)
            for name in self.__dataclass_fields__
        }
        values["pending_orders"] = [
            _thaw_policy_value(item) for item in self.pending_orders
        ]
        for name, value in tuple(values.items()):
            if isinstance(value, float):
                values[name] = _policy_decimal_text(value)
        values["policy_version"] = "operational-paper-v1"
        return values
PAPER_OBSERVATION_AUTHORITY_REQUIRED = "PAPER_OBSERVATION_AUTHORITY_REQUIRED"
def _paper_config_from_spec(
    spec: ForwardTestSpec,
    config: PaperTradingConfig | None,
) -> tuple[PaperTradingConfig, float | None]:
    """Materialize immutable nested assumptions for paper execution.

    New forward specs carry fees, slippage, and sizing under
    ``paper_assumptions``.  A caller-supplied execution config may repeat the
    cost values, but it cannot override the frozen assumptions.
    """
    supplied = config
    effective = config or PaperTradingConfig()
    spec_config = spec.config if isinstance(spec.config, Mapping) else {}
    if not _paper_assumptions_explicit(spec_config):
        # Persisted records from before explicit assumptions remain readable;
        # their backfilled assumptions are descriptive, not execution authority.
        return effective, None
    assumptions = spec_config.get("paper_assumptions")
    if not isinstance(assumptions, Mapping):
        raise ValueError("explicit paper assumptions must be a mapping")

    def number(value: Any, name: str) -> float:
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            raise ValueError(f"paper_assumptions.{name} must be numeric") from None
        if not math.isfinite(parsed) or parsed < 0:
            raise ValueError(f"paper_assumptions.{name} must be finite and non-negative")
        return parsed

    expected_fee_rate, expected_slippage_bps = _paper_assumption_costs(spec_config) or (
        None,
        None,
    )
    if supplied is not None:
        if (
            expected_fee_rate is not None
            and float(supplied.fee_rate) != expected_fee_rate
        ):
            raise ValueError("paper fee_rate conflicts with frozen assumptions")
        if (
            expected_slippage_bps is not None
            and float(supplied.slippage_bps) != expected_slippage_bps
        ):
            raise ValueError("paper slippage_bps conflicts with frozen assumptions")
    elif expected_fee_rate is not None or expected_slippage_bps is not None:
        effective = PaperTradingConfig(
            fee_rate=(
                expected_fee_rate
                if expected_fee_rate is not None
                else effective.fee_rate
            ),
            slippage_bps=(
                expected_slippage_bps
                if expected_slippage_bps is not None
                else effective.slippage_bps
            ),
            depth=effective.depth,
            quality=effective.quality,
            live=effective.live,
            operational_policy=effective.operational_policy,
        )

    allocated_capital: float | None = None
    sizing = assumptions.get("sizing")
    sizing = sizing if isinstance(sizing, Mapping) else {}
    model = str(sizing.get("model", assumptions.get("sizing_model", ""))).strip().lower()
    if model not in _SUPPORTED_PAPER_SIZING_MODELS:
        raise ValueError(f"unsupported paper sizing model: {model!r}")
    if sizing.get("allocated_capital") is None:
        raise ValueError(
            "paper_assumptions.sizing.allocated_capital is required"
        )
    allocated_capital = number(
        sizing["allocated_capital"],
        "sizing.allocated_capital",
    )
    if allocated_capital <= 0:
        raise ValueError(
            "paper_assumptions.sizing.allocated_capital must be positive"
        )
    return effective, allocated_capital
def _operational_settings_identity(value: Mapping[str, Any] | Any) -> dict[str, Any]:
    """Normalize the active settings identity carried by an operational run."""
    if not isinstance(value, Mapping):
        raise TypeError("operational_settings must be a mapping")
    config_id = str(value.get("config_id", "")).strip()
    config_hash = str(value.get("config_hash", "")).strip()
    generation = value.get("generation")
    if not config_id or not config_hash:
        raise ValueError("operational settings identity requires config_id and config_hash")
    if isinstance(generation, bool):
        raise ValueError("operational settings generation must be a positive integer")
    try:
        generation = int(generation)
    except (TypeError, ValueError):
        raise ValueError("operational settings generation must be a positive integer") from None
    if generation < 1:
        raise ValueError("operational settings generation must be a positive integer")
    return {
        "config_id": config_id,
        "generation": generation,
        "config_hash": config_hash,
        "paper_only": True,
    }


def paper_execution_binding(
    spec: ForwardTestSpec,
    *,
    config: PaperTradingConfig | None = None,
    storage_namespace: str | None = None,
    execution_mode: str | None = None,
    operational_policy: OperationalPaperPolicy | Mapping[str, Any] | Any | None = None,
    operational_settings: Mapping[str, Any] | None = None,
) -> dict[str, Any]:

    """Build the immutable binding used by one paper execution namespace."""
    if not isinstance(spec, ForwardTestSpec):
        raise TypeError("spec must be a ForwardTestSpec")
    paper_config, _ = _paper_config_from_spec(spec, config)
    run_id = str(storage_namespace or spec.experiment_id).strip()
    if not run_id:
        raise ValueError("storage_namespace must be non-empty")
    mode = str(
        execution_mode
        or ("forward" if storage_namespace is None else "isolated")
    ).strip().lower()
    if mode not in {"forward", "historical_replay", "isolated"}:
        raise ValueError("execution_mode is invalid")
    policy_source = operational_policy
    if policy_source is None:
        policy_source = getattr(paper_config, "operational_policy", None)
    policy = None if policy_source is None else OperationalPaperPolicy.from_value(policy_source)
    research_mode = "RECORDED_BOOK_REPLAY" if mode == "historical_replay" else "PAPER_FORWARD"
    operational_identity = (
        _operational_settings_identity(operational_settings)
        if operational_settings is not None
        else None
    )
    binding = {
        "experiment_id": run_id,
        "spec_experiment_id": spec.experiment_id,
        "execution_mode": mode,
        "research_mode": research_mode,
        "strategy_hash": spec.strategy_hash,
        "model_hash": spec.model_hash,
        "config_hash": hashlib.sha256(
            _canonical_json(
                {
                    "config": spec.config,
                    "risk_limits": spec.risk_limits,
                    "bankroll": spec.bankroll,
                }
            ).encode("utf-8")
        ).hexdigest(),
        "paper_config_hash": hashlib.sha256(
            _canonical_json(
                {
                    "fee_rate": paper_config.fee_rate,
                    "slippage_bps": paper_config.slippage_bps,
                    "depth": paper_config.depth,
                    "quality": paper_config.quality,
                    "live": paper_config.live,
                }
            ).encode("utf-8")
        ).hexdigest(),
    }
    if operational_identity is not None:
        binding["operational_settings"] = dict(operational_identity)
        binding["operational_config_id"] = operational_identity["config_id"]
        binding["operational_config_generation"] = operational_identity["generation"]
        binding["operational_config_hash"] = operational_identity["config_hash"]
    if operational_identity is not None:
        binding["paper_only"] = True
    if policy is not None:
        policy_record = policy.as_record()
        if operational_identity is not None:
            policy_record["operational_settings"] = dict(operational_identity)
        binding["operational_policy"] = policy_record
        binding["operational_policy_hash"] = hashlib.sha256(
            _canonical_json(policy_record).encode("utf-8")
        ).hexdigest()
    else:
        binding["operational_policy"] = None
        binding["operational_policy_hash"] = None
    return binding


def paper_state_binding_blocker(
    persisted_binding: Any,
    current_binding: Mapping[str, Any],
) -> dict[str, Any] | None:
    """Return the stable fail-closed blocker for an incompatible paper state."""
    if (
        isinstance(persisted_binding, Mapping)
        and _canonical_json(persisted_binding) == _canonical_json(current_binding)
    ):
        return None
    persisted_hash = hashlib.sha256(
        _canonical_json(persisted_binding).encode("utf-8")
    ).hexdigest()
    current_hash = hashlib.sha256(
        _canonical_json(current_binding).encode("utf-8")
    ).hexdigest()
    return {
        "status": "BLOCKED",
        "blocker": PAPER_STATE_EXECUTION_BINDING_MISMATCH,
        "reason_code": PAPER_STATE_EXECUTION_BINDING_MISMATCH,
        "reason": "persisted paper state execution binding does not match the current frozen forward test",
        "retryable": False,
        "non_retryable": True,
        "execution_skipped": True,
        "persisted_binding_hash": f"sha256:{persisted_hash}",
        "current_binding_hash": f"sha256:{current_hash}",
        "persisted_execution_binding": deepcopy(persisted_binding),
        "current_execution_binding": deepcopy(dict(current_binding)),
    }


@dataclass(frozen=True, slots=True)
class PaperEngineCycle:
    started_at: datetime
    ended_at: datetime
    observations_seen: int
    observations_processed: int
    observations_skipped: int
    fills_inserted: int
    settlements: int
    errors: tuple[str, ...] = ()
    execution_events: int = 0
    blocker: str | None = None
    retryable: bool | None = None

    def as_record(self) -> dict[str, Any]:
        record = {
            "started_at": self.started_at.isoformat(),
            "ended_at": self.ended_at.isoformat(),
            "duration_seconds": max(0.0, (self.ended_at - self.started_at).total_seconds()),
            "observations_seen": self.observations_seen,
            "observations_processed": self.observations_processed,
            "observations_skipped": self.observations_skipped,
            "fills_inserted": self.fills_inserted,
            "settlements": self.settlements,
            "errors": list(self.errors),
            "execution_events": self.execution_events,
        }
        if self.blocker is not None:
            record.update(
                {
                    "status": "BLOCKED",
                    "blocker": self.blocker,
                    "reason_code": self.blocker,
                    "retryable": self.retryable,
                    "non_retryable": self.retryable is False,
                    "execution_skipped": True,
                }
            )
        return record


class _ObservationTrader(PaperTrader):
    market_type = MarketType.PREDICTION


class ForwardPaperEngine:
    """Process each new post-registration observation exactly once."""

    def __init__(
        self,
        spec: ForwardTestSpec,
        *,
        store: AxiomStore,
        strategy: Any,
        model: Any | None = None,
        provider: Any | None = None,
        risk: RiskEngine | None = None,
        portfolio: Portfolio | None = None,
        config: PaperTradingConfig | None = None,
        storage_namespace: str | None = None,
        execution_mode: str | None = None,
        operational_policy: OperationalPaperPolicy | Mapping[str, Any] | Any | None = None,
        operational_settings: Mapping[str, Any] | None = None,
    ) -> None:
        if not isinstance(spec, ForwardTestSpec):
            raise TypeError("spec must be a ForwardTestSpec")
        self._provider_errors: list[str] = []
        if isinstance(spec.config, Mapping) and bool(spec.config.get("historical_replay")) and storage_namespace is None:
            raise ValueError("historical replay specs require run_historical_replay")
        self.spec = spec
        self.store = store
        self.provider = provider
        self.model = model
        self.strategy = strategy
        self.config, self._allocated_capital = _paper_config_from_spec(spec, config)
        self._run_id = str(storage_namespace or spec.experiment_id).strip()
        if not self._run_id:
            raise ValueError("storage_namespace must be non-empty")
        self._execution_mode = str(
            execution_mode
            or ("forward" if storage_namespace is None else "isolated")
        ).strip().lower()
        if self._execution_mode not in {"forward", "historical_replay", "isolated"}:
            raise ValueError("execution_mode is invalid")
        self._execution_strategy_id = spec.strategy_hash
        self._research_mode = (
            "RECORDED_BOOK_REPLAY"
            if self._execution_mode == "historical_replay"
            else "PAPER_FORWARD"
        )
        # Operational policy/settings are a narrow extension of the original
        # constructor.  Keep the legacy execution identity above initialized
        # before normalizing these optional collaborators.
        config_view = spec.config if isinstance(spec.config, Mapping) else {}
        policy_source = operational_policy
        if policy_source is None:
            policy_source = getattr(self.config, "operational_policy", None)
        if policy_source is None:
            policy_source = config_view.get(
                "operational_paper_policy",
                config_view.get("operational_policy"),
            )
        self._operational_policy = (
            None
            if policy_source is None
            else OperationalPaperPolicy.from_value(policy_source)
        )
        self._operational_settings = (
            _operational_settings_identity(operational_settings)
            if operational_settings is not None
            else None
        )
        self._execution_binding = paper_execution_binding(
            spec,
            config=self.config,
            storage_namespace=storage_namespace,
            execution_mode=execution_mode,
            operational_policy=self._operational_policy,
            operational_settings=self._operational_settings,
        )
        self.portfolio = portfolio or Portfolio(spec.bankroll)
        loaded_state = store.load_paper_state(self._run_id)
        self._state_version = int(loaded_state.get("state_version", 0)) if loaded_state is not None else -1
        raw_state = loaded_state.get("state", {}) if loaded_state is not None else {}
        self._state = dict(raw_state) if isinstance(raw_state, Mapping) else {}
        self._compatibility_blocker = None
        try:
            self._fill_count = max(0, int(self._state.get("fill_count", 0)))
        except (TypeError, ValueError):
            self._fill_count = 0
        operational_state = self._state.get("operational_paper", {})
        counters_present = isinstance(operational_state, Mapping) and "counters" in operational_state
        counters_source = (
            operational_state.get("counters")
            if isinstance(operational_state, Mapping)
            else None
        )
        self._operational_counters: dict[str, Any] = (
            deepcopy(dict(counters_source))
            if isinstance(counters_source, Mapping)
            else {}
        )
        self._operational_counters.setdefault("submitted_total", 0)
        self._operational_counters.setdefault("submitted_today", 0)
        self._operational_counters.setdefault("buy_gross_today", "0")
        self._operational_counters.setdefault("buy_cumulative", "0")
        self._operational_counters.setdefault("market_buy", {})
        self._operational_counters.setdefault("event_buy", {})
        self._operational_counters.setdefault("declared_loss_cumulative", "0")
        self._operational_counters.setdefault("pending_orders", [])
        self._operational_counters.setdefault("day", None)
        counters_shape_invalid = (
            counters_present
            and counters_source is not None
            and not isinstance(counters_source, Mapping)
        )
        if (
            ("operational_paper" in self._state and not isinstance(operational_state, Mapping))
            or counters_shape_invalid
            or not self._operational_counters_valid(self._operational_counters)
        ):
            self._compatibility_blocker = _operational_state_blocker()
        if self._compatibility_blocker is None:
            for name in ("submitted_total", "submitted_today"):
                self._operational_counters[name] = int(float(self._operational_counters[name]))
            if self._operational_counters.get("day") is not None:
                self._operational_counters["day"] = date.fromisoformat(
                    str(self._operational_counters["day"])
                ).isoformat()
        existing_market_buy = self._operational_counters.get("market_buy", {})
        if isinstance(existing_market_buy, Mapping):
            normalized_market_buy: dict[str, Any] = {}
            for raw_market, amount in existing_market_buy.items():
                market_key = str(raw_market)
                if market_key.endswith("|yes") or market_key.endswith("|no"):
                    market_key = market_key.rsplit("|", 1)[0]
                combined = (
                    (_finite_number(normalized_market_buy.get(market_key, 0.0)) or 0.0)
                    + (_finite_number(amount) or 0.0)
                )
                if not math.isfinite(combined):
                    self._compatibility_blocker = _operational_state_blocker()
                    break
                normalized_market_buy[market_key] = _policy_decimal_text(combined) or "0"
            self._operational_counters["market_buy"] = normalized_market_buy
        if (
            self._compatibility_blocker is None
            and self._operational_policy is not None
            and not self._operational_counters.get("pending_orders")
        ):
            self._operational_counters["pending_orders"] = [
                _thaw_policy_value(item)
                for item in self._operational_policy.pending_orders
            ]
        if self._compatibility_blocker is None and self._operational_policy is not None:
            self._hydrate_unresolved_events()
        persisted_binding = self._state.get("execution_binding")
        if (
            loaded_state is not None
            and isinstance(persisted_binding, Mapping)
            and self._operational_policy is None
            and "operational_policy" not in persisted_binding
        ):
            persisted_binding = {
                **dict(persisted_binding),
                "operational_policy": None,
                "operational_policy_hash": None,
            }
            self._state["execution_binding"] = dict(persisted_binding)
        if loaded_state is not None and persisted_binding is None:
            # States written before execution bindings are valid when their
            # durable identity agrees with this namespace.  Adopt the current
            # binding and let the next checkpoint persist the migration.
            legacy_experiment = str(self._state.get("experiment_id", "")).strip()
            legacy_mode = str(self._state.get("research_mode", "")).strip()
            legacy_strategy = str(self._state.get("execution_strategy_id", "")).strip()
            if (
                legacy_experiment not in {"", self._run_id}
                or legacy_mode not in {"", self._research_mode}
                or legacy_strategy not in {"", self._execution_strategy_id}
            ):
                self._compatibility_blocker = paper_state_binding_blocker(
                    persisted_binding,
                    self._execution_binding,
                )
            else:
                self._state["execution_binding"] = dict(self._execution_binding)
                persisted_binding = self._execution_binding
        if self._compatibility_blocker is None and loaded_state is not None:
            self._compatibility_blocker = paper_state_binding_blocker(
                persisted_binding,
                self._execution_binding,
            )
        config_view = spec.config if isinstance(spec.config, Mapping) else {}
        if (
            self._compatibility_blocker is None
            and bool(config_view.get("observation_intent"))
            and not bool(config_view.get("market_authority_required"))
        ):
            self._compatibility_blocker = {
                "blocker": PAPER_OBSERVATION_AUTHORITY_REQUIRED,
                "retryable": False,
            }
        if self._compatibility_blocker is not None:
            # Do not hydrate any mutable state from a stale namespace.  In
            # particular, restoring its ledger or risk status would mutate a
            # caller-owned portfolio before the blocker is exposed.
            self._processed = set()
            self._cursor = {}
            self._source_cursor = {}
            self._observation_open_by_market = {}
            self._settled = set()
            self._settlement_by_market = {}
            self._signal_history = {}
            self.risk = risk or RiskEngine(
                RiskLimits(**dict(spec.risk_limits)),
                initial_equity=spec.bankroll,
            )
            self._strategy_document = {}
            self._model_state_restored = False
            self.trader = _ObservationTrader(
                provider=None,
                strategy=strategy,
                risk=self.risk,
                portfolio=self.portfolio,
                strategy_id=self._execution_strategy_id,
                config=self.config,
            )
            self.trader._operational_gate = self._operational_gate if self._operational_policy is not None else None
            self.trader._operational_commit = self._commit_operational_counters
            return
        self._processed: set[str] = set(str(item) for item in self._state.get("processed_observations", ()))
        self._cursor: dict[str, datetime] = {
            str(key): parsed
            for key, value in dict(self._state.get("cursor_by_market", {})).items()
            if (parsed := parse_timestamp(value)) is not None
        }
        raw_open_times = self._state.get("observation_open_by_market", {})
        self._observation_open_by_market: dict[str, datetime] = {
            str(key): parsed
            for key, value in raw_open_times.items()
            if (parsed := parse_timestamp(value)) is not None
        } if isinstance(raw_open_times, Mapping) else {}
        if loaded_state is not None and not self._observation_open_by_market:
            # Pre-binding states did not persist an opening boundary.  Recover
            # it with the store's bounded MIN(timestamp) scope aggregate,
            # rather than hydrating the complete observation history.
            opening_loader = getattr(self.store, "list_paper_observation_openings", None)
            try:
                if callable(opening_loader):
                    persisted_openings = opening_loader(
                        self._run_id,
                        limit=_MAX_PAPER_OPENING_ROWS,
                    )
                else:
                    persisted_openings = self.store.list_latest_paper_observations(
                        self._run_id,
                        per_market_limit=_MAX_PAPER_OPENING_ROWS,
                        per_scope_limit=_MAX_PAPER_OPENING_ROWS,
                    )
            except Exception:
                persisted_openings = ()
            for item in persisted_openings:
                if not isinstance(item, Mapping):
                    continue
                payload = item.get("payload")
                payload = payload if isinstance(payload, Mapping) else {}
                market_id = str(
                    item.get("market_id", payload.get("market_id", ""))
                ).strip()
                stamp = parse_timestamp(item.get("timestamp"))
                if not market_id or stamp is None:
                    continue
                scope_observation = {
                    key: item.get(key, payload.get(key))
                    for key in (
                        "shadow_job_id",
                        "shadow_member_id",
                        "shadow_assessment",
                        "synthetic_fixture",
                    )
                    if item.get(key, payload.get(key)) is not None
                }
                scope_key = self._observation_scope_key(market_id, scope_observation)
                previous = self._observation_open_by_market.get(scope_key)
                if previous is None or stamp < previous:
                    self._observation_open_by_market[scope_key] = stamp
        raw_source_cursors = self._state.get("source_cursor_by_market", {})
        self._source_cursor: dict[str, tuple[datetime, str]] = {
            str(key): (parsed, str(value.get("snapshot_id")).strip())
            for key, value in raw_source_cursors.items()
            if isinstance(value, Mapping)
            and str(value.get("snapshot_id", "")).strip()
            and (parsed := parse_timestamp(value.get("timestamp"))) is not None
        } if isinstance(raw_source_cursors, Mapping) else {}
        self._settled: set[str] = set(str(item) for item in self._state.get("settled_markets", ()))
        self._settlement_by_market: dict[str, str] = {
            str(key): str(value)
            for key, value in dict(self._state.get("settlement_by_market", {})).items()
        }
        stored_history = self._state.get("signal_history_by_market", {})
        self._signal_history: dict[str, list[Any]] = {
            str(key): list(value[-512:])
            for key, value in stored_history.items()
            if isinstance(value, (list, tuple))
        } if isinstance(stored_history, Mapping) else {}
        for market_id, history in self._signal_history.items():
            if market_id in self._observation_open_by_market:
                continue
            history_times = (
                stamp
                for item in history
                if (stamp := _observation_timestamp(item)) is not None
            )
            first = next(iter(sorted(history_times)), None)
            if first is not None:
                self._observation_open_by_market[market_id] = first
        if risk is None:
            self.risk = RiskEngine(RiskLimits(**dict(spec.risk_limits)), initial_equity=spec.bankroll)
        else:
            self.risk = risk
        self._restore_risk_status()
        strategy_document = _normalized_strategy_document(getattr(strategy, "definition", strategy))
        self._strategy_document = strategy_document
        model_document = getattr(model, "document", model)
        if _content_hash(strategy_document) != spec.strategy_hash:
            raise ValueError("strategy does not match the frozen forward-test hash")
        if _content_hash(model_document) != spec.model_hash:
            raise ValueError("model does not match the frozen forward-test hash")
        frozen_paper_fields = {
            "fee_rate": self.config.fee_rate,
            "slippage_bps": self.config.slippage_bps,
            "depth": self.config.depth,
            "quality": getattr(self.config.quality, "value", self.config.quality),
            "live": self.config.live,
        }
        if isinstance(spec.config, Mapping):
            for key, actual in frozen_paper_fields.items():
                if key in spec.config:
                    expected = getattr(spec.config[key], "value", spec.config[key])
                    if actual != expected:
                        raise ValueError(f"paper config field does not match frozen forward test: {key}")
        expected_limits = RiskLimits(**dict(spec.risk_limits))
        if self.risk.limits != expected_limits:
            raise ValueError("risk limits do not match the frozen forward test")
        if float(getattr(self.portfolio, "initial_cash", spec.bankroll)) != float(spec.bankroll):
            raise ValueError("portfolio bankroll does not match the frozen forward test")
        persisted_model_state = self._state.get("model_state")
        self._model_state_restored = False
        if isinstance(persisted_model_state, Mapping) and _snapshot_object_state(self.model) is not None:
            _restore_object_state(self.model, dict(persisted_model_state))
            self._model_state_restored = True
        self._restore_ledger()
        self._restore_settlements()
        self.trader = _ObservationTrader(
            provider=None,
            strategy=strategy,
            risk=self.risk,
            portfolio=self.portfolio,
            strategy_id=self._execution_strategy_id,
            config=self.config,
        )
        self.trader._operational_commit = self._commit_operational_counters
        self.trader._operational_gate = self._operational_gate if self._operational_policy is not None else None
        # Compact restores retain only the open shadow lots needed by
        # member-attribution strategies; expose those same fills through the
        # trader facade as well as the caller-owned portfolio.
        self.trader._fills = list(self.portfolio.fills)
        try:
            restored_sequence = int(self._state.get("order_sequence", 0))
        except (TypeError, ValueError):
            restored_sequence = 0
        if restored_sequence < 0:
            raise ValueError("paper state order sequence is invalid")
        self.trader._sequence = restored_sequence
        self._warm_strategy_state()


    @staticmethod
    def _market_rule_value(observation: Mapping[str, Any], book: Any, *names: str) -> Any:
        for name in names:
            if name in observation and observation.get(name) not in (None, ""):
                return observation.get(name)
        for name in names:
            value = getattr(book, name, None) if book is not None else None
            if value not in (None, ""):
                return value
        return None

    def _operational_market_rules(self, context: Mapping[str, Any]) -> dict[str, Any]:
        observation = context.get("observation")
        observation = observation if isinstance(observation, Mapping) else {}
        book = context.get("order_book")
        rules = {
            "min_order_size": self._market_rule_value(
                observation, book, "min_order_size", "order_min_size",
                "minimum_order_size", "min_size", "minimum_size",
                "quantity_min", "minQuantity", "minOrderSize",
            ),
            "size_increment": self._market_rule_value(
                observation, book, "size_increment", "quantity_step",
                "order_size_increment", "step_size", "quantity_increment",
                "sizeIncrement",
            ),
            "min_notional": self._market_rule_value(
                observation, book, "min_notional", "minimum_notional",
                "min_cost", "minimum_cost", "min_notional_usd",
                "minimum_notional_usd", "minimum_cost_usd",
                "min_order_value", "minimum_order_value", "minOrderValue",
                "minNotional", "minimumNotional",
            ),
            "tick_size": self._market_rule_value(
                observation, book, "tick_size", "tickSize",
                "price_increment", "price_tick_size",
                "order_price_min_tick_size", "orderPriceMinTickSize",
            ),
        }
        # Per-outcome nested venue contexts are common in collected snapshots.
        outcome = str(context.get("outcome") or "").strip().lower()
        nested = observation.get(f"{outcome}_market_rules") if outcome else None
        if isinstance(nested, Mapping):
            for key in tuple(rules):
                if rules[key] in (None, ""):
                    rules[key] = nested.get(key)
        return rules

    def _operational_positions(self, *, prices: Mapping[str, Any] | None = None) -> tuple[float, float, float, int]:
        snapshot = self.portfolio.snapshot(prices) if self.portfolio is not None else {}
        open_cost = 0.0
        active_markets: set[str] = set()
        for key, position in getattr(self.portfolio, "positions", {}).items():
            quantity = _finite_number(getattr(position, "quantity", 0.0)) or 0.0
            if quantity <= 1e-12:
                continue
            market_id = str(
                getattr(position, "market_id", None)
                or getattr(position, "symbol", None)
                or key
            ).strip()
            if market_id:
                active_markets.add(market_id)
            average = _finite_number(getattr(position, "average_price", 0.0)) or 0.0
            open_cost += max(0.0, quantity * average)
        exposure = _finite_number(snapshot.get("gross_exposure")) or 0.0
        equity = _finite_number(snapshot.get("equity"))
        if equity is None:
            equity = _finite_number(getattr(self.portfolio, "cash", 0.0)) or 0.0
        return open_cost, exposure, equity, len(active_markets)

    def _operational_open_lots(self) -> int:
        return sum(
            1
            for position in getattr(self.portfolio, "positions", {}).values()
            if (_finite_number(getattr(position, "quantity", 0.0)) or 0.0) > 1e-12
        )

    @staticmethod
    def _pending_commitments_checked(raw: Any) -> tuple[float, float, int, bool]:
        if not isinstance(raw, (list, tuple)):
            return 0.0, 0.0, 0, False
        money = 0.0
        buys = 0.0
        count = 0
        valid = True
        for item in raw:
            if not isinstance(item, Mapping):
                valid = False
                continue
            status = str(item.get("status", "PENDING")).strip().upper() or "PENDING"
            if status in _TERMINAL_OPERATIONAL_STATUSES:
                continue
            if status not in _UNRESOLVED_OPERATIONAL_STATUSES:
                valid = False
                continue
            side = str(item.get("side", "BUY")).strip().upper()
            if side not in {"BUY", "SELL"}:
                valid = False
                continue

            quantity_raw = next(
                (
                    item[name]
                    for name in ("remaining_quantity", "quantity", "size", "qty")
                    if name in item
                ),
                None,
            )
            price_raw = next(
                (
                    item[name]
                    for name in ("price", "reference_price", "quote")
                    if name in item
                ),
                None,
            )
            fee_raw = next(
                (
                    item[name]
                    for name in ("fee_reserve", "fee")
                    if name in item
                ),
                0.0,
            )
            quantity = _finite_number(quantity_raw)
            price = _finite_number(price_raw)
            fee = _finite_number(fee_raw)
            if (
                quantity is None
                or price is None
                or fee is None
                or quantity < 0
                or price < 0
                or fee < 0
                or (quantity > 0 and price <= 0)
            ):
                valid = False
                continue
            notional = quantity * price
            if not math.isfinite(notional):
                valid = False
                continue
            if side == "BUY":
                buy_cost = notional + fee
                if not math.isfinite(buy_cost):
                    valid = False
                    continue
                money += buy_cost
                buys += buy_cost
            count += 1
        return money, buys, count, valid

    @classmethod
    def _pending_commitments(cls, raw: Any) -> tuple[float, float, int]:
        money, buys, count, valid = cls._pending_commitments_checked(raw)
        return (money, buys, count) if valid else (0.0, 0.0, 0)

    @classmethod
    def _operational_counters_valid(cls, raw: Any) -> bool:
        if not isinstance(raw, Mapping):
            return False
        for name in ("submitted_total", "submitted_today"):
            if not _nonnegative_finite(raw.get(name), integer=True):
                return False
        for name in ("buy_gross_today", "buy_cumulative", "declared_loss_cumulative"):
            if not _nonnegative_finite(raw.get(name)):
                return False
        for name in ("market_buy", "event_buy"):
            values = raw.get(name)
            if not isinstance(values, Mapping):
                return False
            for key, amount in values.items():
                if not str(key).strip() or not _nonnegative_finite(amount):
                    return False
        day = raw.get("day")
        if day is not None:
            try:
                date.fromisoformat(str(day))
            except (TypeError, ValueError):
                return False
        return cls._pending_commitments_checked(raw.get("pending_orders"))[3]

    def _hydrate_unresolved_events(self) -> None:
        statuses = tuple(_UNRESOLVED_OPERATIONAL_STATUSES)
        bounded_loader = getattr(
            self.store,
            "list_unresolved_paper_execution_events",
            None,
        )
        all_loader = getattr(self.store, "list_paper_execution_events", None)
        if not callable(bounded_loader) and not callable(all_loader):
            self._compatibility_blocker = _operational_state_blocker()
            return
        try:
            if callable(bounded_loader):
                count_loader = getattr(
                    self.store,
                    "count_unresolved_paper_execution_events",
                    None,
                )
                if callable(count_loader) and count_loader(
                    self._run_id,
                    statuses=statuses,
                ) > _MAX_PAPER_UNRESOLVED_EVENTS:
                    # Refuse to hydrate a truncated obligation set.  Losing an
                    # unresolved reservation would make the next submission
                    # unsound, so fail closed instead.
                    self._compatibility_blocker = _operational_state_blocker()
                    return
                prior_events = bounded_loader(
                    self._run_id,
                    statuses=statuses,
                    limit=_MAX_PAPER_UNRESOLVED_EVENTS,
                )
            else:
                prior_events = all_loader(
                    self._run_id,
                    limit=_MAX_PAPER_UNRESOLVED_EVENTS,
                )
        except Exception:
            self._compatibility_blocker = _operational_state_blocker()
            return
        pending = self._operational_counters.setdefault("pending_orders", [])
        if not isinstance(pending, list):
            self._compatibility_blocker = _operational_state_blocker()
            return
        existing: set[str] = set()
        for item in pending:
            if not isinstance(item, Mapping):
                continue
            for name in ("event_id", "order_id", "observation_id"):
                value = str(item.get(name, "")).strip()
                if value:
                    existing.add(name + ":" + value)
        for prior in prior_events:
            if not isinstance(prior, Mapping):
                continue
            payload = prior.get("payload", {})
            payload = payload if isinstance(payload, Mapping) else {}
            status = str(
                prior.get("status", payload.get("status", ""))
            ).strip().upper()
            if status not in _UNRESOLVED_OPERATIONAL_STATUSES:
                continue
            fill_payload = payload.get("fill", {})
            fill_payload = fill_payload if isinstance(fill_payload, Mapping) else {}
            identity_values = (
                ("event_id", prior.get("event_id")),
                ("order_id", payload.get("order_id")),
                ("observation_id", prior.get("observation_id")),
            )
            if any(
                value not in (None, "")
                and name + ":" + str(value).strip() in existing
                for name, value in identity_values
            ):
                continue
            hydrated = {
                "status": status,
                "side": payload.get("side", fill_payload.get("side", "BUY")),
                "quantity": payload.get(
                    "remaining_quantity",
                    payload.get(
                        "requested_quantity",
                        payload.get("quantity", fill_payload.get("quantity", fill_payload.get("filled_quantity"))),
                    ),
                ),
                "price": payload.get(
                    "reference_price",
                    payload.get("price", fill_payload.get("price")),
                ),
                "fee_reserve": payload.get(
                    "fee_reserve",
                    payload.get("fee", fill_payload.get("fees", 0)),
                ),
            }
            for name, value in identity_values:
                if value not in (None, ""):
                    hydrated[name] = value
                    existing.add(name + ":" + str(value).strip())
            pending.append(hydrated)
    def _commit_operational_counters(
        self,
        decision: Mapping[str, Any],
        context: Mapping[str, Any],
        order: Any,
    ) -> None:
        policy = self._operational_policy
        reservation = decision.get("counter_reservation") if isinstance(decision, Mapping) else None
        if policy is None or not isinstance(reservation, Mapping):
            return
        timestamp = ensure_utc(getattr(order, "requested_at", utc_now()))
        order_day = timestamp.date()
        counters = self._operational_counters
        raw_day = counters.get("day")
        try:
            current_day = date.fromisoformat(str(raw_day)) if raw_day is not None else None
        except (TypeError, ValueError):
            current_day = None
        if current_day is None or order_day > current_day:
            counters["day"] = order_day.isoformat()
            counters["submitted_today"] = 0
            counters["buy_gross_today"] = "0"
        counters["submitted_total"] = int(counters.get("submitted_total", 0)) + 1
        counters["submitted_today"] = int(counters.get("submitted_today", 0)) + 1
        if not bool(reservation.get("is_buy")):
            return
        all_in = _finite_number(reservation.get("all_in")) or 0.0
        market_key = str(reservation.get("market_key") or "").strip()
        event_key = str(reservation.get("event_key") or "").strip()
        counters["buy_gross_today"] = _policy_decimal_text(
            (_finite_number(counters.get("buy_gross_today", 0.0)) or 0.0) + all_in
        ) or "0"
        counters["buy_cumulative"] = _policy_decimal_text(
            (_finite_number(counters.get("buy_cumulative", 0.0)) or 0.0) + all_in
        ) or "0"
        market_buys = dict(counters.get("market_buy", {}))
        market_buys[market_key] = _policy_decimal_text(
            (_finite_number(market_buys.get(market_key, 0.0)) or 0.0) + all_in
        ) or "0"
        counters["market_buy"] = market_buys
        event_buys = dict(counters.get("event_buy", {}))
        event_buys[event_key] = _policy_decimal_text(
            (_finite_number(event_buys.get(event_key, 0.0)) or 0.0) + all_in
        ) or "0"
        counters["event_buy"] = event_buys
        declared = _finite_number(reservation.get("declared_loss"))
        if declared is not None and declared > 0:
            counters["declared_loss_cumulative"] = _policy_decimal_text(
                (_finite_number(counters.get("declared_loss_cumulative", 0.0)) or 0.0)
                + declared
            ) or "0"

    def _operational_gate(self, order: Any, context: Mapping[str, Any]) -> dict[str, Any]:
        policy = self._operational_policy
        if policy is None:
            return {"allowed": True}
        timestamp = ensure_utc(getattr(order, "requested_at", utc_now()))
        order_day = timestamp.date()
        counters = self._operational_counters
        try:
            current_day = (
                date.fromisoformat(str(counters.get("day")))
                if counters.get("day") is not None
                else None
            )
        except (TypeError, ValueError):
            return {"allowed": False, "reason_code": PAPER_STATE_OPERATIONAL_COUNTERS_INVALID}
        same_day = current_day is None or order_day <= current_day
        submitted_today = int(counters.get("submitted_today", 0)) if same_day else 0
        rules = self._operational_market_rules(context)
        blockers: list[str] = []
        parsed_rules: dict[str, float] = {}
        if policy.require_market_rules:
            for name in ("min_order_size", "size_increment", "min_notional", "tick_size"):
                try:
                    value = float(rules.get(name))
                except (TypeError, ValueError):
                    value = 0.0
                if not math.isfinite(value) or value <= 0:
                    blockers.append("VENUE_MARKET_RULES_MISSING")
                else:
                    parsed_rules[name] = value
        else:
            for name, value in rules.items():
                try:
                    number = float(value)
                except (TypeError, ValueError):
                    continue
                if math.isfinite(number) and number > 0:
                    parsed_rules[name] = number
        if blockers:
            return {
                "allowed": False,
                "reason_code": blockers[0],
                "blockers": list(dict.fromkeys(blockers)),
                "market_rules": dict(rules),
                "policy_hash": self._execution_binding.get("operational_policy_hash"),
            }
        side = getattr(order, "side", Side.BUY)
        is_buy = side is Side.BUY or str(getattr(side, "value", side)).lower() == "buy"
        observation = context.get("observation")
        if is_buy and isinstance(observation, Mapping) and observation.get("entry_eligible") is False:
            return {
                "allowed": False,
                "reason_code": "ENTRY_PREDICATE_NOT_ELIGIBLE",
                "signal_strength": observation.get("signal_strength"),
                "entry_eligible": False,
            }
        quantity = _finite_number(getattr(order, "quantity", 0.0)) or 0.0
        price = _finite_number(context.get("risk_price", context.get("reference_price"))) or 0.0
        venue_quote = _finite_number(
            context.get("venue_price", context.get("reference_price", price))
        ) or 0.0
        if quantity <= 0 or price <= 0 or venue_quote <= 0:
            return {"allowed": False, "reason_code": "INVALID_ORDER_PARAMETERS"}
        increment = parsed_rules.get("size_increment", 0.0) or 1.0
        minimum = parsed_rules.get("min_order_size", 0.0)
        min_notional = parsed_rules.get("min_notional", 0.0)
        tick = parsed_rules.get("tick_size", 0.0)
        tick_ratio = venue_quote / tick if tick > 0 else 0.0
        if tick > 0 and abs(tick_ratio - round(tick_ratio)) > 1e-8:
            return {
                "allowed": False,
                "reason_code": "VENUE_TICK_SIZE",
                "market_rules": parsed_rules,
                "price": venue_quote,
            }
        pending_money, pending_buys, pending_count, pending_valid = self._pending_commitments_checked(
            counters.get("pending_orders", ())
        )
        if not pending_valid:
            return {
                "allowed": False,
                "reason_code": "PENDING_COMMITMENT_INVALID",
                "pending_commitments": pending_count,
            }
        exit_slots = self._operational_open_lots() if is_buy else 0
        required_slots = pending_count + submitted_today + 1
        if is_buy:
            required_slots += exit_slots + 1
        if required_slots > policy.max_submitted_orders_per_day:
            return {
                "allowed": False,
                "reason_code": "SUBMITTED_ORDER_CAPACITY",
                "pending_commitments": pending_count,
                "required_slots": required_slots,
                "exit_slots_reserved": exit_slots + 1 if is_buy else 0,
            }
        if not is_buy:
            position = None
            try:
                position = self.portfolio.get_position(
                    str(getattr(order, "symbol", "")),
                    outcome=getattr(order, "outcome", None),
                    market_id=getattr(order, "market_id", None),
                )
            except (TypeError, ValueError):
                try:
                    position = self.portfolio.get_position(
                        str(getattr(order, "symbol", "")),
                        outcome=getattr(order, "outcome", None),
                    )
                except (TypeError, ValueError):
                    position = None
            available = _finite_number(getattr(position, "quantity", 0.0)) if position is not None else 0.0
            if available is None or available + 1e-12 < quantity:
                return {
                    "allowed": False,
                    "reason_code": "OWNED_INVENTORY_REQUIRED",
                    "owned_quantity": available or 0.0,
                    "requested_quantity": quantity,
                }
        # Round only down to the venue increment.  A minimum that cannot fit
        # inside the frozen $1 all-in cap is an explicit no-fill blocker.
        minimum_quantity = max(
            minimum,
            math.ceil((min_notional / price) / increment - 1e-12) * increment,
        )
        candidate_quantity = quantity
        if increment > 0:
            candidate_quantity = math.floor(candidate_quantity / increment + 1e-12) * increment
        if is_buy:
            available_all_in = max(0.0, policy.max_all_in_buy_usd)
            candidate_quantity = min(
                candidate_quantity,
                math.floor(
                    max(0.0, available_all_in)
                    / max(price * (1.0 + max(0.0, self.config.fee_rate)), 1e-12)
                    / increment
                    + 1e-12,
                )
                * increment,
            )
            fee = candidate_quantity * price * max(0.0, self.config.fee_rate)
            if fee > policy.max_fee_reserve_usd + 1e-12:
                blockers.append("FEE_RESERVE_CAP")
            if candidate_quantity + 1e-12 < minimum_quantity:
                blockers.append("VENUE_MINIMUM_INFEASIBLE")
        if not is_buy and candidate_quantity + 1e-12 < minimum_quantity:
            blockers.append("VENUE_MINIMUM_INFEASIBLE")
        if candidate_quantity <= 0:
            blockers.append("VENUE_MINIMUM_INFEASIBLE")
        if blockers:
            return {
                "allowed": False,
                "reason_code": blockers[0],
                "blockers": list(dict.fromkeys(blockers)),
                "market_rules": parsed_rules,
                "requested_quantity": quantity,
                "minimum_quantity": minimum_quantity,
            }
        notional = candidate_quantity * price
        fee_reserve = notional * max(0.0, self.config.fee_rate)
        all_in = notional + fee_reserve
        open_cost, exposure, equity, open_positions = self._operational_positions(
            prices=(
                context.get("mark_prices")
                if isinstance(context.get("mark_prices"), Mapping)
                else None
            )
        )
        declared = _finite_number(
            context.get("declared_loss", context.get("expected_loss"))
        ) or 0.0
        if is_buy:
            market_id = str(getattr(order, "market_id", None) or getattr(order, "symbol", "")).strip()
            outcome = str(getattr(order, "outcome", "") or "").strip().lower()
            market_key = market_id
            event_key = str(context.get("event_id") or market_id).strip()
            market_used = _finite_number(counters.get("market_buy", {}).get(market_key, 0.0)) or 0.0
            event_used = _finite_number(counters.get("event_buy", {}).get(event_key, 0.0)) or 0.0
            cumulative = _finite_number(counters.get("buy_cumulative", 0.0)) or 0.0
            daily = (_finite_number(counters.get("buy_gross_today", 0.0)) or 0.0) if same_day else 0.0
            reasons: list[str] = []
            if all_in > policy.max_all_in_buy_usd + 1e-12:
                reasons.append("MAX_ALL_IN_BUY")
            if pending_money + open_cost + all_in > policy.max_aggregate_open_cost_usd + 1e-12:
                reasons.append("AGGREGATE_OPEN_COST")
            if pending_money + exposure + all_in > policy.max_aggregate_exposure_usd + 1e-12:
                reasons.append("AGGREGATE_EXPOSURE")
            if daily + pending_buys + all_in > policy.max_gross_daily_buy_usd + 1e-12:
                reasons.append("GROSS_DAILY_BUY")
            if policy.per_market_buy_cap_usd is not None and market_used + all_in > policy.per_market_buy_cap_usd + 1e-12:
                reasons.append("PER_MARKET_BUY_CAP")
            if policy.per_event_buy_cap_usd is not None and event_used + all_in > policy.per_event_buy_cap_usd + 1e-12:
                reasons.append("PER_EVENT_BUY_CAP")
            if policy.cumulative_buy_cap_usd is not None and cumulative + all_in > policy.cumulative_buy_cap_usd + 1e-12:
                reasons.append("CUMULATIVE_BUY_CAP")
            if open_positions >= policy.max_positions:
                try:
                    existing = self.portfolio.get_position(
                        str(getattr(order, "symbol", "")),
                        outcome=outcome or None,
                        market_id=getattr(order, "market_id", None),
                    )
                except (TypeError, ValueError):
                    existing = None
                if existing is None or (_finite_number(getattr(existing, "quantity", 0.0)) or 0.0) <= 1e-12:
                    reasons.append("MAX_POSITIONS")
            if self._has_strategy_or_event_overlap(order, context, policy):
                reasons.append("STRATEGY_EVENT_OVERLAP")
            realized = max(0.0, -(_finite_number(self.portfolio.realized_pnl()) or 0.0))
            equity_loss = max(0.0, (_finite_number(getattr(self.portfolio, "initial_cash", equity)) or equity) - equity)
            declared_total = _finite_number(counters.get("declared_loss_cumulative", 0.0)) or 0.0
            if realized >= policy.realized_loss_entry_stop_usd:
                reasons.append("REALIZED_LOSS_ENTRY_STOP")
            if equity_loss >= policy.equity_loss_entry_stop_usd:
                reasons.append("EQUITY_LOSS_ENTRY_STOP")
            if policy.cumulative_declared_loss_entry_stop_usd is not None and declared_total + max(0.0, declared or 0.0) >= policy.cumulative_declared_loss_entry_stop_usd:
                reasons.append("CUMULATIVE_DECLARED_LOSS_ENTRY_STOP")
            if reasons:
                return {
                    "allowed": False,
                    "reason_code": reasons[0],
                    "blockers": reasons,
                    "notional": notional,
                    "all_in": all_in,
                    "fee_reserve": fee_reserve,
                    "market_key": market_key,
                    "event_key": event_key,
                }
        reservation: dict[str, Any] = {}
        if is_buy:
            reservation = {
                "is_buy": True,
                "all_in": all_in,
                "market_key": market_key,
                "event_key": event_key,
                "declared_loss": max(0.0, declared or 0.0),
            }
        return {
            "allowed": True,
            "risk_approved": True,
            "policy_hash": self._execution_binding.get("operational_policy_hash"),
            "reason_code": "RISK_APPROVED",
            "quantity": candidate_quantity,
            "notional": notional,
            "all_in": all_in,
            "fee_reserve": fee_reserve,
            "market_rules": parsed_rules,
            "counter_reservation": reservation,
        }
    def _has_strategy_or_event_overlap(
        self,
        order: Any,
        context: Mapping[str, Any],
        policy: OperationalPaperPolicy,
    ) -> bool:
        """Reject overlap only when a matching net position is still open."""
        market = str(getattr(order, "market_id", None) or getattr(order, "symbol", "")).strip()
        strategy = str(getattr(order, "strategy_id", "")).strip()
        event = str(context.get("event_id") or market).strip()
        active_markets: set[str] = set()
        for key, position in getattr(self.portfolio, "positions", {}).items():
            quantity = _finite_number(getattr(position, "quantity", 0.0)) or 0.0
            if quantity > 1e-12:
                active_markets.add(
                    str(
                        getattr(position, "market_id", None)
                        or getattr(position, "symbol", None)
                        or key
                    ).strip()
                )
        if not active_markets:
            return False
        if not policy.allow_strategy_overlap and market in active_markets:
            return True
        if not policy.allow_event_overlap and event in active_markets:
            return True
        strategy_net: dict[tuple[str, str], float] = {}
        event_net: dict[tuple[str, str], float] = {}
        fills = getattr(self.portfolio, "fills", ()) or ()
        if fills:
            for fill in fills:
                fill_market = str(
                    getattr(fill, "market_id", None) or getattr(fill, "symbol", "")
                ).strip()
                if fill_market not in active_markets:
                    continue
                quantity = _finite_number(getattr(fill, "quantity", 0.0)) or 0.0
                side = getattr(fill, "side", None)
                signed = (
                    quantity
                    if side is Side.BUY
                    or str(getattr(side, "value", side)).lower() == "buy"
                    else -quantity
                )
                fill_strategy = str(getattr(fill, "strategy_id", "")).strip()
                strategy_net[(fill_strategy, fill_market)] = strategy_net.get(
                    (fill_strategy, fill_market), 0.0
                ) + signed
                metadata = getattr(fill, "metadata", {}) or {}
                fill_event = str(metadata.get("event_id") or fill_market).strip()
                event_net[(fill_event, fill_market)] = event_net.get(
                    (fill_event, fill_market), 0.0
                ) + signed
        else:
            # A restart restores positions from the compact portfolio snapshot;
            # recover overlap identity from an SQL aggregate rather than
            # hydrating every historical fill into memory.
            aggregator = getattr(self.store, "aggregate_paper_fill_exposure", None)
            try:
                aggregate_rows = (
                    aggregator(self._run_id)
                    if callable(aggregator)
                    else ()
                )
            except Exception:
                aggregate_rows = ()
            for row in aggregate_rows:
                if not isinstance(row, Mapping):
                    continue
                fill_market = str(row.get("market_id", "")).strip()
                if fill_market not in active_markets:
                    continue
                signed = _finite_number(row.get("signed_notional")) or 0.0
                fill_strategy = str(row.get("strategy_id", "")).strip()
                strategy_net[(fill_strategy, fill_market)] = strategy_net.get(
                    (fill_strategy, fill_market), 0.0
                ) + signed
                fill_event = str(row.get("event_id") or fill_market).strip()
                event_net[(fill_event, fill_market)] = event_net.get(
                    (fill_event, fill_market), 0.0
                ) + signed
        if not policy.allow_strategy_overlap and strategy_net.get((strategy, market), 0.0) > 1e-12:
            return True
        if not policy.allow_event_overlap and any(
            fill_event == event and net > 1e-12
            for (fill_event, _fill_market), net in event_net.items()
        ):
            return True
        return False
    def _restore_risk_status(self) -> None:
        if self.risk is None:
            return
        persisted = self._state.get("risk")
        if not isinstance(persisted, Mapping):
            return
        self.risk.emergency_kill_switch = bool(persisted.get("emergency_kill_switch", False))
        self.risk.cooldown_until = parse_timestamp(persisted.get("cooldown_until"))
        for name in ("equity", "peak_equity", "day_start_equity", "current_cvar"):
            value = persisted.get("cvar" if name == "current_cvar" else name)
            if value is None:
                continue
            try:
                number = float(value)
            except (TypeError, ValueError):
                continue
            if math.isfinite(number):
                setattr(self.risk, name, number)
        raw_day = persisted.get("day")
        if raw_day is not None:
            try:
                self.risk._day = date.fromisoformat(str(raw_day))
            except ValueError:
                pass

    @property
    def state(self) -> dict[str, Any]:
        return deepcopy(self._state)
    @property
    def operational_policy(self) -> dict[str, Any] | None:
        return (
            deepcopy(self._operational_policy.as_record())
            if self._operational_policy is not None
            else None
        )

    @property
    def operational_counters(self) -> dict[str, Any]:
        return deepcopy(self._operational_counters)
    @property
    def compatibility_blocker(self) -> dict[str, Any] | None:
        return deepcopy(self._compatibility_blocker)

    @property
    def execution_binding(self) -> dict[str, Any]:
        return deepcopy(self._execution_binding)

    @staticmethod
    def _observation_scope_key(market_id: str, observation: Any) -> str:
        """Partition cursors/history by run, market and stable shadow member.

        ``shadow_group_id`` identifies one scheduler cycle and is therefore
        intentionally excluded.  A new group must continue the same member's
        512-observation lookback; the member id is the durable identity for
        concurrent shadow members.  The run namespace is supplied by the
        owning engine/store state and is not part of this local key.
        """

        shadow = _shadow_metadata(observation)
        member_id = shadow.get("shadow_member_id") if shadow else None
        if member_id not in (None, ""):
            return f"{market_id}|shadow-member:{str(member_id).strip()}"
        if not shadow:
            return str(market_id)
        stable_shadow = {
            key: value for key, value in shadow.items() if key != "shadow_group_id"
        }
        if not stable_shadow:
            return str(market_id)
        digest = hashlib.sha256(
            _canonical_json(stable_shadow).encode("utf-8")
        ).hexdigest()[:24]
        return f"{market_id}|shadow:{digest}"

    @staticmethod
    def _scope_market_id(scope_key: str) -> str:
        return str(scope_key).split("|shadow-member:", 1)[0].split("|shadow:", 1)[0]

    def _append_signal_history(self, market_id: str, observation: Mapping[str, Any]) -> None:
        scope_key = self._observation_scope_key(market_id, observation)
        history = self._signal_history.setdefault(scope_key, [])
        history.append(dict(observation))
        if len(history) > 512:
            del history[:-512]

    def _warm_strategy_state(self) -> None:
        """Replay persisted signal inputs into stateful strategies without execution."""
        try:
            rows = self.store.list_latest_paper_observations(
                self._run_id,
                per_market_limit=512,
                per_scope_limit=512,
            )
        except Exception:
            return
        rows = [item for item in rows if isinstance(item, Mapping)]
        rows.sort(
            key=lambda item: _observation_sort_key(
                item.get("payload"),
                self.spec.registration_timestamp.tzinfo,
            )
        )
        rebuilt: dict[str, list[Any]] = {}
        for item in rows:
            payload = item.get("payload")
            if not isinstance(payload, Mapping):
                continue
            market_id = str(item.get("market_id", payload.get("market_id", ""))).strip()
            if not market_id:
                continue
            observation = dict(payload)
            if not self._model_state_restored and not _is_terminal(observation.get("settlement")):
                model_evaluation = _model_probability_evaluation(self.model, observation)
                if model_evaluation.probability is not None and "model_probability" not in observation:
                    observation["model_probability"] = model_evaluation.probability
                observation["model_evaluation"] = model_evaluation.as_record()
            scope_key = self._observation_scope_key(market_id, observation)
            history = rebuilt.setdefault(scope_key, [])
            warm_context = {
                "market_type": MarketType.PREDICTION.value,
                "symbol": market_id,
                "observation": observation,
                "signal_observation": observation,
                "market": observation,
                "ticker": observation,
                "history": tuple(history[-512:]),
                "order_book": None,
                "liquidity": observation.get("liquidity"),
                "spread": None,
                "reference_price": observation.get("yes_mid", observation.get("yes_ask")),
                "mark_prices": {},
                "model_probability": observation.get("model_probability"),
                "paper": True,
                "warmup": True,
            }
            shadow_metadata = _shadow_metadata(observation)
            if shadow_metadata:
                warm_context.update(shadow_metadata)
                warm_context["metadata"] = dict(shadow_metadata)
            if not _is_terminal(observation.get("settlement")):
                try:
                    self.trader._strategy_signal(observation, warm_context)
                except Exception:
                    pass
            history.append(observation)
            if len(history) > 512:
                del history[:-512]
        if rebuilt:
            self._signal_history = rebuilt

    def run(
        self,
        observations: Iterable[Any] | None = None,
        *,
        now: datetime | None = None,
        max_observations: int | None = None,
    ) -> PaperEngineCycle:
        started = ensure_utc(now or utc_now())
        if max_observations is not None and (
            isinstance(max_observations, bool)
            or not isinstance(max_observations, int)
            or max_observations < 0
        ):
            raise ValueError("max_observations must be non-negative or None")
        if self._compatibility_blocker is not None:
            ended = ensure_utc(now or utc_now())
            if ended < started:
                ended = started
            blocker = str(self._compatibility_blocker["blocker"])
            return PaperEngineCycle(
                started,
                ended,
                0,
                0,
                0,
                0,
                0,
                (blocker,),
                0,
                blocker,
                False,
            )
        observation_cutoff = started
        observation_limit = _MAX_RUN_OBSERVATIONS if max_observations is None else max_observations
        if observations is None:
            raw_observations = list(islice(iter(self._provider_observations()), observation_limit))
            if now is None:
                observation_cutoff = ensure_utc(utc_now())
        else:
            raw_observations = list(islice(iter(observations), observation_limit))
            if now is None:
                observation_cutoff = ensure_utc(utc_now())
        provider_errors = list(self._provider_errors)
        self._provider_errors.clear()
        raw_observations.sort(key=lambda value: _observation_sort_key(value, started.tzinfo))
        processed = 0
        skipped = 0
        fills_inserted = 0
        settlements = 0
        execution_events = 0
        errors: list[str] = provider_errors
        for raw in raw_observations:
            stamp = _observation_timestamp(raw)
            if stamp is None:
                skipped += 1
                errors.append("observation missing timestamp")
                continue
            if self._execution_mode != "historical_replay" and stamp < self.spec.registration_timestamp:
                skipped += 1
                message = f"pre-registration observation rejected: {stamp.isoformat()}"
                if observations is not None:
                    raise ValueError(message)
                errors.append(message)
                continue
            if stamp > observation_cutoff:
                skipped += 1
                errors.append(f"future observation deferred: {stamp.isoformat()}")
                continue
            normalized = _normalize_observation(
                raw,
                default_market_id=(
                    self.spec.allowed_markets[0]
                    if len(self.spec.allowed_markets) == 1
                    else None
                ),
            )
            if normalized is None:
                skipped += 1
                errors.append("malformed prediction observation")
                continue
            market_id, observation, yes_book, no_book = normalized
            market_scope_key = self._observation_scope_key(market_id, observation)
            terminal = _is_terminal(observation.get("settlement"))

            shadow_metadata = _shadow_metadata(observation)
            source_type = str(observation.get("source_type", "FORWARD_COLLECTED")).strip().upper() or "FORWARD_COLLECTED"
            observation["source_type"] = source_type
            if self._execution_mode == "historical_replay":
                if source_type == "PAPER_FORWARD":
                    skipped += 1
                    errors.append(f"historical replay rejects PAPER_FORWARD for {market_id}")
                    continue
                if not terminal and not (
                    _explicit_book_timestamp(_raw_observation_book(raw, "yes"))
                    or _explicit_book_timestamp(_raw_observation_book(raw, "no"))
                ):
                    skipped += 1
                    errors.append(f"historical replay missing observed order book for {market_id}")
                    continue
            elif source_type != "FORWARD_COLLECTED":
                skipped += 1
                errors.append(f"stored fallback requires FORWARD_COLLECTED for {market_id}")
                continue
            if self.spec.allowed_markets and market_id not in self.spec.allowed_markets:
                skipped += 1
                continue
            observed_at = parse_timestamp(observation.get("observed_at"))
            source_future = next(
                (
                    (name, parsed)
                    for name in (
                        "source_timestamp",
                        "as_of_timestamp",
                        "asof_timestamp",
                        "as_of",
                        "provider_timestamp",
                    )
                    if (parsed := parse_timestamp(observation.get(name))) is not None
                    and parsed > stamp
                ),
                None,
            )
            if source_future is not None:
                skipped += 1
                errors.append(f"{source_future[0]} after observation for {market_id}")
                continue
            available_at = parse_timestamp(observation.get("available_at"))
            availability_cutoff = observed_at or stamp
            if available_at is not None and available_at > availability_cutoff:
                skipped += 1
                errors.append(f"observation unavailable until {available_at.isoformat()}")
                continue
            capture_future = next(
                (
                    (name, parsed)
                    for name in ("request_started_at", "response_received_at")
                    if (parsed := parse_timestamp(observation.get(name))) is not None
                    and parsed > availability_cutoff
                ),
                None,
            )
            if capture_future is not None:
                skipped += 1
                errors.append(f"{capture_future[0]} after observation capture for {market_id}")
                continue
            if any(book is not None and ensure_utc(book.timestamp) > stamp for book in (yes_book, no_book)):
                skipped += 1
                errors.append(f"future order book for {market_id}")
                continue
            cursor = self._cursor.get(market_scope_key)
            if cursor is not None and stamp < cursor:
                skipped += 1
                continue
            if market_scope_key in self._settled:
                current_settlement = _settlement_value(observation.get("settlement"))
                previous_settlement = _settlement_value(self._settlement_by_market.get(market_scope_key))
                if (
                    terminal
                    and previous_settlement
                    and current_settlement != previous_settlement
                ):
                    errors.append(
                        f"conflicting settlement for {market_id}: "
                        f"{previous_settlement} -> {current_settlement}"
                    )
                skipped += 1
                continue
            source_timestamp = parse_timestamp(observation.get("source_timestamp")) or stamp
            if source_timestamp > stamp:
                skipped += 1
                errors.append(f"source timestamp after observation for {market_id}")
                continue
            observation_id = self._observation_id(market_id, stamp, observation)
            if observation_id in self._processed:
                skipped += 1
                continue
            model_state_before = _snapshot_object_state(self.model)
            model_evaluation = None
            if not terminal:
                try:
                    model_evaluation = _model_probability_evaluation(self.model, observation)
                except Exception as exc:
                    _restore_object_state(self.model, model_state_before)
                    skipped += 1
                    errors.append(f"model error for {market_id}: {exc}")
                    continue
            model_probability = model_evaluation.probability if model_evaluation is not None else None
            if model_probability is not None and "model_probability" not in observation:
                observation["model_probability"] = model_probability
            if model_evaluation is not None:
                observation["model_evaluation"] = model_evaluation.as_record()
            strategy_evaluation = None
            strategy_record: Mapping[str, Any] | None = None
            strategy_entry_eligible: Any = None
            custom_shadow_strategy = bool(
                shadow_metadata
                and (
                    callable(self.strategy)
                    or any(
                        callable(getattr(self.strategy, name, None))
                        for name in ("signal", "decide", "generate_signal", "generate")
                    )
                )
            )
            if not terminal and isinstance(self._strategy_document, Mapping) and not custom_shadow_strategy:
                signal_inputs = tuple(self._signal_history.get(market_scope_key, ())) + (observation,)
                evaluation_data: dict[str, Any] = {
                    "observations": signal_inputs,
                    "snapshots": signal_inputs,
                    "history": signal_inputs,
                    "market_id": market_id,
                }
                model_document = getattr(self.model, "document", self.model)
                if model_document is not None:
                    evaluation_data["model_document"] = model_document
                try:
                    strategy_evaluation = evaluate_signal_evaluation(self._strategy_document, evaluation_data)
                except (TypeError, ValueError):
                    strategy_evaluation = None
            if strategy_evaluation is not None:
                evaluated_record = strategy_evaluation.as_record()
                strategy_record = evaluated_record if isinstance(evaluated_record, Mapping) else None
                strategy_evidence = strategy_record.get("evidence") if strategy_record is not None else None
                for field_name in ("signal_strength", "entry_eligible"):
                    value = getattr(strategy_evaluation, field_name, None)
                    if value is None and strategy_record is not None:
                        value = strategy_record.get(field_name)
                    if value is None and isinstance(strategy_evidence, Mapping):
                        value = strategy_evidence.get(field_name)
                    if value is not None:
                        observation[field_name] = value
                        if field_name == "entry_eligible":
                            strategy_entry_eligible = value
            replay_missing_book = False
            if self._execution_mode == "historical_replay" and not terminal:
                score = getattr(strategy_evaluation, "score", None)
                if score is not None and score < 0:
                    replay_missing_book = no_book is None or not _explicit_book_timestamp(
                        _raw_observation_book(raw, "no")
                    )
                elif score is not None and score > 0:
                    replay_missing_book = yes_book is None or not _explicit_book_timestamp(
                        _raw_observation_book(raw, "yes")
                    )
                else:
                    replay_missing_book = not (
                        yes_book is not None
                        and no_book is not None
                        and _explicit_book_timestamp(_raw_observation_book(raw, "yes"))
                        and _explicit_book_timestamp(_raw_observation_book(raw, "no"))
                    )
            previous_cursor = self._cursor.get(market_scope_key)
            previous_source_cursor = self._source_cursor.get(market_scope_key)
            previous_open_timestamp = self._observation_open_by_market.get(market_scope_key)
            was_settled = market_scope_key in self._settled
            previous_settlement = self._settlement_by_market.get(market_scope_key)
            if (
                model_evaluation is not None
                and model_evaluation.probability is None
                and model_evaluation.reason_code == "MODEL_INPUT_MISSING"
            ):
                # Keep the distinction in persisted evidence; no default 0.50
                # probability is ever synthesized.
                if "model_probability" not in observation:
                    observation["model_probability"] = None
            if model_evaluation is not None and model_evaluation.probability is None:
                observation["model_evaluation"]["reason_code"] = model_evaluation.reason_code
            price = _reference_price(observation, yes_book, no_book)
            missing_execution_quote = price is None and not terminal
            fill = None
            fill_saved = False
            settlement_saved = False
            inserted_observation = False
            history_before = tuple(self._signal_history.get(market_scope_key, ()))
            portfolio_before = deepcopy(self.portfolio)
            risk_before = deepcopy(self.risk)
            state_before = deepcopy(self._state)
            trader_fills_before = list(self.trader._fills)
            signal_history_before = list(history_before)
            state_version_before = self._state_version
            trader_sequence_before = self.trader._sequence
            strategy_state_before = _snapshot_object_state(self.strategy)
            execution_events_before = execution_events
            operational_counters_before = deepcopy(self._operational_counters)
            fill_count_before = self._fill_count
            try:
                with self.store.transaction():
                    inserted_observation = self.store.save_paper_observation(
                        observation_id,
                        self._run_id,
                        market_id,
                        stamp,
                        observation,
                    )
                    if not inserted_observation:
                        raise _DuplicatePaperObservation()
                    if self._observation_open_by_market.get(market_scope_key) is None:
                        self._observation_open_by_market[market_scope_key] = stamp
                    if replay_missing_book:
                        fill = None
                        execution_event = {
                            "status": "NO_FILL",
                            "reason": "missing_observed_order_book",
                            "liquidity_rejected": True,
                        }
                    elif missing_execution_quote:
                        fill = None
                        execution_event = {
                            "status": "NO_FILL",
                            "reason": "missing_executable_quote",
                            "liquidity_rejected": True,
                        }
                    else:
                        fill = self.trader._run_observation(
                            symbol=market_id,
                            observation=observation,
                            book=yes_book,
                            no_book=no_book,
                            timestamp=stamp,
                            reference=price,
                            allocated_capital=self._allocated_capital,
                            market_id=market_id,
                            signal_history=history_before,
                        )
                    if fill is not None:
                        fill = self._bind_fill(fill)
                        if self.store.save_fill(
                            fill,
                            fill_id="paper-fill-" + self._run_id + "-" + fill.order_id,
                        ):
                            fill_saved = True
                            self._fill_count += 1
                    execution_event = dict(
                        execution_event
                        if replay_missing_book or missing_execution_quote
                        else self.trader.last_execution_event
                    )
                    execution_event["execution_binding"] = dict(self._execution_binding)
                    execution_event["observation_id"] = observation_id
                    execution_event["experiment_id"] = self._run_id
                    execution_event["paper_only"] = True
                    execution_event["research_mode"] = self._research_mode
                    execution_event["source_type"] = source_type
                    execution_event["research_label"] = (
                        "REPLAY" if self._execution_mode == "historical_replay" else "FORWARD"
                    )
                    execution_event["retrospective_replay"] = self._execution_mode == "historical_replay"
                    if shadow_metadata:
                        execution_event.update(shadow_metadata)
                        execution_event["live_execution"] = False
                        if terminal:
                            settlement = _settlement_value(observation.get("settlement"))
                            if settlement in {
                                SettlementState.RESOLVED_YES.value,
                                SettlementState.RESOLVED_NO.value,
                            }:
                                # Keep the resolution source tied to this
                                # public snapshot; unresolved snapshots never
                                # receive a synthesized settlement.
                                execution_event["settlement"] = settlement
                    if self._operational_settings is not None:
                        execution_event["operational_settings"] = dict(
                            self._operational_settings
                        )
                        execution_event["operational_config_id"] = self._operational_settings["config_id"]
                        execution_event["operational_config_generation"] = self._operational_settings["generation"]
                        execution_event["operational_config_hash"] = self._operational_settings["config_hash"]
                    operational_evidence = execution_event.get("operational_evidence")
                    if isinstance(operational_evidence, Mapping):
                        execution_event["risk_approved"] = bool(
                            operational_evidence.get("risk_approved", operational_evidence.get("allowed", False))
                        )
                        execution_event["operational_policy_hash"] = self._execution_binding.get(
                            "operational_policy_hash"
                        )
                    if replay_missing_book or missing_execution_quote:
                        execution_event["execution_blocked"] = True
                    if strategy_evaluation is not None:
                        evaluation_record = strategy_evaluation.as_record()
                        if shadow_metadata and isinstance(evaluation_record, Mapping):
                            evaluation_record = dict(evaluation_record)
                            evaluation_record["evidence"] = {
                                **(
                                    dict(evaluation_record.get("evidence", {}))
                                    if isinstance(evaluation_record.get("evidence"), Mapping)
                                    else {}
                                ),
                                **shadow_metadata,
                            }
                        execution_event["evaluation"] = evaluation_record
                        execution_event["evaluation_reason"] = strategy_evaluation.reason_code
                        execution_event["evaluation_evidence"] = {
                            **dict(strategy_evaluation.evidence),
                            **shadow_metadata,
                        }
                        execution_event["evaluation_actionable"] = bool(strategy_evaluation.actionable)
                        if "operational_evidence" not in execution_event:
                            execution_event["reason_code"] = strategy_evaluation.reason_code
                    elif fill is not None:
                        execution_event["reason_code"] = "SIGNAL_PRODUCED"
                    elif str(execution_event.get("status", "")).upper() == "RESOLUTION":
                        execution_event["reason_code"] = "RESOLUTION"
                    elif "reason_code" not in execution_event:
                        execution_event["reason_code"] = "STRATEGY_EVALUATED_DECLINED"
                    if fill is not None:
                        execution_event["fill"] = to_record(fill)
                        execution_event["fill_id"] = fill.order_id
                        execution_event["fill_is_opening"] = not terminal
                    if model_evaluation is not None:
                        execution_event["model_evaluation"] = model_evaluation.as_record()
                    event_status = str(execution_event.get("status", "NO_SIGNAL")).strip().upper() or "NO_SIGNAL"
                    if self.store.save_paper_execution_event(
                        "paper-execution-" + self._run_id + "-" + observation_id,
                        self._run_id,
                        observation_id,
                        market_id,
                        stamp,
                        event_status,
                        execution_event,
                    ):
                        execution_events += 1
                    if terminal:
                        settlement_saved = True
                        self._settled.add(market_scope_key)
                        self._settlement_by_market[market_scope_key] = str(observation.get("settlement"))
                        scope_market_id = self._scope_market_id(market_scope_key)
                        if self.risk is not None and not self._restore_risk_from_aggregates():
                            self.risk.reconcile_market(
                                scope_market_id,
                                fills=self.portfolio.fills,
                            )
                        open_timestamp = self._observation_open_by_market.get(market_scope_key)
                        # A terminal snapshot can be the first observation
                        # for a market.  It establishes no completed
                        # forward outcome, even if a stale fill exists.
                        if open_timestamp is not None and open_timestamp < stamp:
                            ledger = build_resolved_bet(
                                experiment_id=self._run_id,
                                market_id=market_id,
                                strategy_id=self._execution_strategy_id,
                                settlement=_settlement_value(observation.get("settlement")) or str(observation.get("settlement")),
                                resolved_at=stamp,
                                fills=self._stored_fills_for_market(
                                    market_id,
                                    start=open_timestamp,
                                    end=stamp,
                                ),
                                observation_open_timestamp=open_timestamp,
                            )
                            if ledger is not None:
                                ledger["execution_binding"] = dict(self._execution_binding)
                                ledger["paper_experiment_id"] = self._run_id
                                self.store.save_paper_bet_ledger(
                                    ledger["bet_id"],
                                    self._run_id,
                                    market_id,
                                    self._execution_strategy_id,
                                    ledger["outcome"],
                                    ledger["resolution"],
                                    stamp,
                                    ledger,
                                )
                    self._cursor[market_scope_key] = max(
                        self._cursor.get(market_scope_key, stamp),
                        stamp,
                    )
                    source_snapshot_id = str(observation.get("source_snapshot_id", "")).strip()
                    if source_snapshot_id:
                        source_cursor = (source_timestamp, source_snapshot_id)
                        if previous_source_cursor is None or source_cursor > previous_source_cursor:
                            self._source_cursor[market_scope_key] = source_cursor
                    self._processed.add(observation_id)
                    self._append_signal_history(market_id, observation)
                    self._persist_state(last_timestamp=stamp)
            except _DuplicatePaperObservation:
                _restore_object_state(self.model, model_state_before)
                _restore_object_state(self.strategy, strategy_state_before)
                self._reload_committed_state()
                skipped += 1
                continue
            except Exception as exc:
                _restore_object_state(self.model, model_state_before)
                _restore_object_state(self.strategy, strategy_state_before)
                _restore_mutable_state(self.portfolio, portfolio_before)
                _restore_mutable_state(self.risk, risk_before)
                self.trader.portfolio = self.portfolio
                self.trader.risk = self.risk
                self.trader._fills = trader_fills_before
                self.trader._sequence = trader_sequence_before
                self._state_version = state_version_before
                self._fill_count = fill_count_before
                self._operational_counters = operational_counters_before
                execution_events = execution_events_before
                self._state = state_before
                self._processed.discard(observation_id)
                if previous_cursor is None:
                    self._cursor.pop(market_scope_key, None)
                else:
                    self._cursor[market_scope_key] = previous_cursor
                if previous_source_cursor is None:
                    self._source_cursor.pop(market_scope_key, None)
                else:
                    self._source_cursor[market_scope_key] = previous_source_cursor
                if previous_open_timestamp is None:
                    self._observation_open_by_market.pop(market_scope_key, None)
                else:
                    self._observation_open_by_market[market_scope_key] = previous_open_timestamp
                if not was_settled:
                    self._settled.discard(market_scope_key)
                if previous_settlement is None:
                    self._settlement_by_market.pop(market_scope_key, None)
                else:
                    self._settlement_by_market[market_scope_key] = previous_settlement
                if signal_history_before:
                    self._signal_history[market_scope_key] = signal_history_before
                else:
                    self._signal_history.pop(market_scope_key, None)
                errors.append(f"{market_id}: {exc}")
                continue
            if not inserted_observation:
                skipped += 1
                continue
            processed += 1
            fills_inserted += int(fill_saved)
            settlements += int(settlement_saved)
        ended = ensure_utc(now or utc_now())
        if ended < started:
            ended = started
        cycle = PaperEngineCycle(
            started,
            ended,
            len(raw_observations),
            processed,
            skipped,
            fills_inserted,
            settlements,
            tuple(errors),
            execution_events,
        )
        self._state["last_cycle"] = cycle.as_record()
        try:
            self._persist_state(last_timestamp=None)
        except RuntimeError as exc:
            if "concurrently" not in str(exc):
                raise
            cycle = replace(cycle, errors=(*cycle.errors, str(exc)))
        return cycle

    run_once = run

    def run_forever(
        self,
        *,
        cycles: int | None = None,
        stop_event: Any | None = None,
        sleep: Any | None = None,
        interval_seconds: float = 60.0,
    ) -> list[PaperEngineCycle]:
        if cycles is not None and (isinstance(cycles, bool) or cycles < 0):
            raise ValueError("cycles must be non-negative or None")
        if not math.isfinite(float(interval_seconds)) or interval_seconds <= 0:
            raise ValueError("interval_seconds must be finite and positive")
        sleeper = sleep or __import__("time").sleep
        results: list[PaperEngineCycle] = []
        completed = 0
        while cycles is None or completed < cycles:
            if stop_event is not None and stop_event.is_set():
                break
            results.append(self.run())
            completed += 1
            if cycles is not None and completed >= cycles:
                break
            if stop_event is not None and stop_event.is_set():
                break
            sleeper(float(interval_seconds))
        return results

    def _provider_observations(self) -> list[Any]:
        def record_transport_errors(context: str) -> None:
            consume = getattr(self.provider, "consume_transport_errors", None)
            if not callable(consume):
                return
            try:
                failures = tuple(consume())
            except Exception as exc:
                self._provider_errors.append(f"{context}: transport error collector failed: {exc}")
                return
            for failure in failures:
                status = getattr(failure, "status", None)
                self._provider_errors.append(
                    f"{context}: HTTP {status}" if status is not None else f"{context}: {failure}"
                )

        def stored_observations() -> list[Any]:
            snapshots: list[dict[str, Any]] = []
            market_ids = tuple(self.spec.allowed_markets) or tuple(
                self.store.tracked_polymarket_markets(active_only=False)
            )
            for market_id in market_ids:
                source_after = self._source_cursor.get(market_id)
                snapshots.extend(
                    self.store.load_polymarket_snapshots(
                        market_id=market_id,
                        source_start=self.spec.registration_timestamp,
                        source_after=source_after,
                        source_type="FORWARD_COLLECTED",
                        limit=512,
                    )
                )
            observations: list[dict[str, Any]] = []
            for item in snapshots:
                payload = item.get("payload")
                observation = dict(payload) if isinstance(payload, Mapping) else {}
                observation.setdefault("market_id", item.get("market_id"))
                observation.setdefault("timestamp", item.get("source_timestamp") or item.get("observed_at"))
                observation["source_snapshot_id"] = item.get("snapshot_id")
                observation["source_timestamp"] = item.get("source_timestamp") or item.get("observed_at")
                observations.append(observation)
            return observations

        if self.provider is None:
            return stored_observations()
        market_ids = list(self.spec.allowed_markets)
        if not market_ids:
            try:
                try:
                    market_ids = [item.market_id for item in islice(iter(self.provider.markets(active=True, limit=1000)), 1000)]
                except TypeError:
                    market_ids = [item.market_id for item in islice(self.provider.markets(active=True), 1000)]
            except Exception as exc:
                self._provider_errors.append(f"market catalog error: {exc}")
                market_ids = []
        observations: list[Any] = []
        for market_id in market_ids:
            try:
                market = self.provider.market(market_id)
                books = self.provider.order_books(market_id, depth=self.config.depth)
            except Exception as exc:
                self._provider_errors.append(f"{market_id}: provider error: {exc}")
                continue
            if market is None:
                continue
            payload = dict(to_record(market))
            payload["yes_order_book"] = to_record(books.get("yes")) if isinstance(books, Mapping) and books.get("yes") else None
            payload["no_order_book"] = to_record(books.get("no")) if isinstance(books, Mapping) and books.get("no") else None
            observations.append(payload)
        record_transport_errors("provider")
        if observations:
            return observations
        try:
            stored = stored_observations()
        except Exception as exc:
            self._provider_errors.append(f"stored observation error: {exc}")
            return []
        if self._provider_errors and stored:
            self._provider_errors.append("provider unavailable; using stored observations")
        return stored

    def _observation_id(self, market_id: str, timestamp: datetime, observation: Mapping[str, Any]) -> str:
        digest = hashlib.sha256(_canonical_json(observation).encode("utf-8")).hexdigest()[:24]
        return f"paper-observation-{self._run_id}-{market_id}-{timestamp.isoformat()}-{digest}"
    def _bind_fill(self, fill: Any) -> Any:
        metadata = {
            **dict(getattr(fill, "metadata", {}) or {}),
            "paper_experiment_id": self._run_id,
            "execution_binding": dict(self._execution_binding),
        }
        shadow = _shadow_metadata(metadata)
        if shadow:
            metadata.update(shadow)
            metadata["paper_only"] = True
            metadata["live_execution"] = False
        tagged = replace(fill, metadata=metadata)
        fills = getattr(self.portfolio, "fills", None)
        if isinstance(fills, list):
            for index in range(len(fills) - 1, -1, -1):
                if getattr(fills[index], "order_id", None) == fill.order_id:
                    fills[index] = tagged
                    break
        orders = getattr(self.portfolio, "orders", {})
        if isinstance(orders, Mapping):
            for order in orders.values():
                order_fills = getattr(order, "fills", None)
                if isinstance(order_fills, list):
                    for index, item in enumerate(order_fills):
                        if getattr(item, "order_id", None) == fill.order_id:
                            order_fills[index] = tagged
        return tagged

    def _reload_committed_state(self) -> None:
        """Refresh all mutable state after losing a duplicate-observation race."""
        loaded_state = self.store.load_paper_state(self._run_id)
        if not isinstance(loaded_state, Mapping):
            return
        self._state_version = int(loaded_state.get("state_version", self._state_version))
        raw_state = loaded_state.get("state", {})
        self._state = dict(raw_state) if isinstance(raw_state, Mapping) else {}
        try:
            self._fill_count = max(0, int(self._state.get("fill_count", self._fill_count)))
        except (TypeError, ValueError):
            self._fill_count = 0
        self._processed = set(str(item) for item in self._state.get("processed_observations", ()))
        self._cursor = {
            str(key): parsed
            for key, value in dict(self._state.get("cursor_by_market", {})).items()
            if (parsed := parse_timestamp(value)) is not None
        }
        raw_open = self._state.get("observation_open_by_market", {})
        self._observation_open_by_market = {
            str(key): parsed
            for key, value in raw_open.items()
            if (parsed := parse_timestamp(value)) is not None
        } if isinstance(raw_open, Mapping) else {}
        raw_source = self._state.get("source_cursor_by_market", {})
        self._source_cursor = {
            str(key): (parsed, str(value.get("snapshot_id")).strip())
            for key, value in raw_source.items()
            if isinstance(value, Mapping)
            and str(value.get("snapshot_id", "")).strip()
            and (parsed := parse_timestamp(value.get("timestamp"))) is not None
        } if isinstance(raw_source, Mapping) else {}
        self._settled = set(str(item) for item in self._state.get("settled_markets", ()))
        self._settlement_by_market = {
            str(key): str(value)
            for key, value in dict(self._state.get("settlement_by_market", {})).items()
        }
        stored_history = self._state.get("signal_history_by_market", {})
        self._signal_history = {
            str(key): list(value[-512:])
            for key, value in stored_history.items()
            if isinstance(value, (list, tuple))
        } if isinstance(stored_history, Mapping) else {}
        operational_state = self._state.get("operational_paper", {})
        counters_present = isinstance(operational_state, Mapping) and "counters" in operational_state
        counters_source = (
            operational_state.get("counters")
            if isinstance(operational_state, Mapping)
            else None
        )
        counters = (
            deepcopy(dict(counters_source))
            if isinstance(counters_source, Mapping)
            else {}
        )
        counters.setdefault("submitted_total", 0)
        counters.setdefault("submitted_today", 0)
        counters.setdefault("buy_gross_today", "0")
        counters.setdefault("buy_cumulative", "0")
        counters.setdefault("market_buy", {})
        counters.setdefault("event_buy", {})
        counters.setdefault("declared_loss_cumulative", "0")
        counters.setdefault("pending_orders", [])
        counters.setdefault("day", None)
        counters_shape_invalid = (
            counters_present
            and counters_source is not None
            and not isinstance(counters_source, Mapping)
        )
        if (
            ("operational_paper" in self._state and not isinstance(operational_state, Mapping))
            or counters_shape_invalid
            or not self._operational_counters_valid(counters)
        ):
            self._compatibility_blocker = _operational_state_blocker()
        self._operational_counters = counters
        if self._compatibility_blocker is None and self._operational_policy is not None:
            self._hydrate_unresolved_events()
        if self._compatibility_blocker is not None:
            return
        self._restore_object_state(self.model, self._state.get("model_state"))
        self.portfolio = Portfolio(self.spec.bankroll)
        self._restore_ledger()
        self.risk = RiskEngine(
            RiskLimits(**dict(self.spec.risk_limits)),
            initial_equity=self.spec.bankroll,
        )
        self._restore_risk_status()
        self._restore_settlements()
        self.trader.portfolio = self.portfolio
        self.trader.risk = self.risk
        self.trader._fills = list(self.portfolio.fills)
        try:
            self.trader._sequence = int(self._state.get("order_sequence", 0))
        except (TypeError, ValueError):
            self.trader._sequence = 0
        self.trader._operational_gate = (
            self._operational_gate if self._operational_policy is not None else None
        )
        self.trader._operational_commit = self._commit_operational_counters

    def _shadow_open_lot_records(
        self,
        fills: Iterable[Fill] | None = None,
    ) -> tuple[list[dict[str, Any]], bool]:
        """Project only still-open shadow BUY lots for compact restart state.

        ShadowCompositeStrategy needs the opening fill identity (member,
        market, outcome and timestamp) to choose a managed SELL after restart.
        Reconstructing that identity from the compact position aggregate is
        impossible, while persisting every historical fill defeats compaction.
        FIFO-net the in-memory run ledger and retain only positive residual
        BUY lots.  The list is bounded even if a malformed/custom portfolio
        exposes an unexpectedly large fill collection.
        """
        lots: list[list[Any]] = []
        shadow_seen = False
        source_fills = (
            getattr(self.portfolio, "fills", ()) if fills is None else fills
        )
        ordered = tuple(
            fill
            for fill in (source_fills or ())
            if isinstance(fill, Fill)
        )
        for fill in ordered:
            metadata = getattr(fill, "metadata", {})
            if not isinstance(metadata, Mapping):
                continue
            member_id = str(metadata.get("shadow_member_id", "")).strip()
            if not member_id:
                continue
            shadow_seen = True
            market_id = str(getattr(fill, "market_id", None) or fill.symbol).strip()
            outcome = str(metadata.get("outcome", "yes")).strip().lower() or "yes"
            quantity = _finite_number(getattr(fill, "quantity", 0.0))
            if not market_id or quantity is None or quantity <= 0:
                continue
            side = getattr(fill, "side", None)
            side_value = str(getattr(side, "value", side)).strip().lower()
            if side_value == Side.BUY.value:
                lots.append([fill, quantity, member_id, market_id, outcome])
                continue
            if side_value != Side.SELL.value:
                continue
            remaining = quantity
            for lot in lots:
                if remaining <= 1e-12:
                    break
                if lot[2] != member_id or lot[3] != market_id or lot[4] != outcome:
                    continue
                consumed = min(remaining, float(lot[1]))
                lot[1] = max(0.0, float(lot[1]) - consumed)
                remaining -= consumed
        records: list[dict[str, Any]] = []
        for fill, remaining, member_id, market_id, outcome in lots:
            quantity = _finite_number(remaining)
            if quantity is None or quantity <= 1e-12:
                continue
            record = to_record(fill)
            record["quantity"] = quantity
            record["remaining_quantity"] = quantity
            record["side"] = Side.BUY.value
            record["market_id"] = market_id
            metadata = dict(record.get("metadata", {}))
            metadata["shadow_member_id"] = member_id
            metadata["outcome"] = outcome
            metadata["paper_experiment_id"] = self._run_id
            record["paper_experiment_id"] = self._run_id
            record["metadata"] = metadata
            # Keep the attribution identity available without decoding the
            # nested fill record; this is useful to bounded state consumers.
            record["shadow_member_id"] = member_id
            records.append(record)
        return records[:_MAX_PAPER_FILL_ROWS], shadow_seen

    def _restore_shadow_open_lots(
        self,
        raw_snapshot: Mapping[str, Any],
        *,
        target: Portfolio | None = None,
    ) -> None:
        """Hydrate persisted residual shadow BUY lots, never historical fills."""
        raw_lots = raw_snapshot.get("open_lots")
        if raw_lots is None:
            raw_lots = self._state.get("open_lots")
        if not isinstance(raw_lots, (list, tuple)):
            return
        seen_order_ids: set[str] = set()
        restored_lots: list[Fill] = []
        for raw_lot in raw_lots[:_MAX_PAPER_FILL_ROWS]:
            if not isinstance(raw_lot, Mapping):
                continue
            nested = raw_lot.get("fill")
            record = nested if isinstance(nested, Mapping) else raw_lot
            metadata = record.get("metadata", {})
            metadata = dict(metadata) if isinstance(metadata, Mapping) else {}
            member_id = str(
                raw_lot.get("shadow_member_id", metadata.get("shadow_member_id", ""))
            ).strip()
            if not member_id:
                continue
            persisted_run_id = str(
                raw_lot.get("paper_experiment_id", metadata.get("paper_experiment_id", ""))
            ).strip()
            if persisted_run_id and persisted_run_id != self._run_id:
                continue
            side_value = str(raw_lot.get("side", record.get("side", ""))).strip().lower()
            if side_value != Side.BUY.value:
                continue
            market_id = str(
                raw_lot.get("market_id", record.get("market_id", record.get("symbol", "")))
                or ""
            ).strip()
            order_id = str(record.get("order_id", "")).strip()
            if not market_id or not order_id or order_id in seen_order_ids:
                continue
            quantity = _finite_number(
                raw_lot.get("remaining_quantity", raw_lot.get("quantity", record.get("quantity")))
            )
            price = _finite_number(record.get("price"))
            fees = _finite_number(record.get("fees", 0.0))
            slippage = _finite_number(record.get("slippage", 0.0))
            if (
                quantity is None
                or quantity <= 1e-12
                or price is None
                or price <= 0
                or fees is None
                or fees < 0
                or slippage is None
                or slippage < 0
            ):
                continue
            metadata["shadow_member_id"] = member_id
            metadata.setdefault("paper_experiment_id", self._run_id)
            metadata.setdefault(
                "outcome",
                str(raw_lot.get("outcome", metadata.get("outcome", "yes"))).strip().lower() or "yes",
            )
            try:
                restored_lots.append(
                    Fill(
                        timestamp=parse_timestamp(record.get("timestamp")) or self.spec.registration_timestamp,
                        market_type=MarketType(
                            str(record.get("market_type", MarketType.PREDICTION.value)).strip().lower()
                        ),
                        symbol=str(record.get("symbol") or market_id),
                        side=Side.BUY,
                        quantity=quantity,
                        price=price,
                        fees=fees,
                        slippage=slippage,
                        strategy_id=str(record.get("strategy_id", self._execution_strategy_id)),
                        order_id=order_id,
                        market_id=market_id,
                        expected_probability=_finite_number(record.get("expected_probability")),
                        executable_probability=_finite_number(record.get("executable_probability")),
                        metadata=metadata,
                    )
                )
                seen_order_ids.add(order_id)
            except (TypeError, ValueError):
                continue
        (target or self.portfolio).fills.extend(restored_lots)



    def _legacy_shadow_open_lot_migration_needed(
        self,
        raw_snapshot: Mapping[str, Any],
    ) -> bool:
        if "open_lots" in raw_snapshot or "open_lots" in self._state:
            return False
        definition = getattr(self.strategy, "definition", self.strategy)
        return (
            isinstance(definition, Mapping)
            and str(definition.get("family", "")).strip().lower()
            == "shadow_composite"
        )

    def _load_bounded_paper_fills(self) -> tuple[int, tuple[Fill, ...]]:
        """Load a complete, bounded run ledger after an authoritative count."""
        count_loader = getattr(self.store, "count_paper_fills", None)
        if not callable(count_loader):
            raise ValueError(f"{PAPER_STATE_FILL_RESTORE_COUNT_MISMATCH}: count unavailable")
        try:
            raw_count = count_loader(
                self._run_id,
                strategy_id=self._execution_strategy_id,
            )
            if isinstance(raw_count, bool):
                raise TypeError("boolean count")
            count = int(raw_count)
            if isinstance(raw_count, float) and not raw_count.is_integer():
                raise ValueError("non-integral count")
        except Exception as exc:
            raise ValueError(
                f"{PAPER_STATE_FILL_RESTORE_COUNT_MISMATCH}: invalid count"
            ) from exc
        if count < 0:
            raise ValueError(
                f"{PAPER_STATE_FILL_RESTORE_COUNT_MISMATCH}: invalid count={count}"
            )
        if count > _MAX_PAPER_FILL_ROWS:
            raise ValueError(
                f"{PAPER_STATE_FILL_RESTORE_OVERFLOW}: count={count}, "
                f"limit={_MAX_PAPER_FILL_ROWS}"
            )
        loader = getattr(self.store, "list_paper_fills", None)
        if not callable(loader):
            if count == 0:
                return count, ()
            raise ValueError(
                f"{PAPER_STATE_FILL_RESTORE_COUNT_MISMATCH}: rows unavailable"
            )
        try:
            fills = tuple(
                loader(
                    self._run_id,
                    strategy_id=self._execution_strategy_id,
                    limit=_MAX_PAPER_FILL_ROWS,
                )
            )
        except Exception as exc:
            raise ValueError(
                f"{PAPER_STATE_FILL_RESTORE_COUNT_MISMATCH}: rows unavailable"
            ) from exc
        if len(fills) != count:
            raise ValueError(
                f"{PAPER_STATE_FILL_RESTORE_COUNT_MISMATCH}: "
                f"count={count}, returned={len(fills)}"
            )
        return count, fills

    def _restore_legacy_shadow_open_lots(
        self,
        raw_snapshot: Mapping[str, Any],
        *,
        fills: Sequence[Fill] | None = None,
    ) -> None:
        """Migrate old compact shadow snapshots with one bounded fill query."""
        if not self._legacy_shadow_open_lot_migration_needed(raw_snapshot):
            return
        if fills is None:
            _fill_count, fills = self._load_bounded_paper_fills()
        records, _shadow_seen = self._shadow_open_lot_records(fills)
        if records:
            self._restore_shadow_open_lots({"open_lots": records})

    def _restore_portfolio_snapshot(self) -> bool:
        """Restore compact portfolio state without replaying every fill."""
        raw_snapshot = self._state.get("portfolio")
        if not isinstance(raw_snapshot, Mapping):
            return False
        try:
            cash = float(raw_snapshot.get("cash", self.spec.bankroll))
            if not math.isfinite(cash):
                return False
            raw_positions = raw_snapshot.get("positions", {})
            if not isinstance(raw_positions, Mapping):
                return False
            restored = Portfolio(self.spec.bankroll, cash=cash)
            for raw_key, raw_position in raw_positions.items():
                if not isinstance(raw_position, Mapping):
                    return False
                key = str(raw_key)
                market_type = MarketType(
                    str(raw_position.get("market_type", MarketType.PREDICTION.value))
                )
                symbol = str(raw_position.get("symbol") or key.split("|", 1)[0])
                outcome = raw_position.get("outcome")
                outcome = str(outcome).strip().lower() if outcome is not None else None
                market_id = (
                    key.rsplit("|", 1)[0]
                    if market_type is MarketType.PREDICTION and "|" in key
                    else raw_position.get("market_id")
                )
                quantity = float(raw_position.get("quantity", 0.0))
                average_price = float(raw_position.get("average_price", 0.0))
                realized = float(raw_position.get("realized_pnl", 0.0))
                fees = float(raw_position.get("fees", 0.0))
                unrealized = float(raw_position.get("unrealized_pnl", 0.0))
                values = (quantity, average_price, realized, fees, unrealized)
                if not all(math.isfinite(value) for value in values):
                    return False
                last_price = (
                    average_price + unrealized / quantity
                    if abs(quantity) > 1e-12
                    else average_price
                )
                restored.positions[key] = Position(
                    symbol=symbol,
                    market_type=market_type,
                    quantity=quantity,
                    average_price=average_price,
                    realized_pnl=realized,
                    fees=fees,
                    market_id=(
                        str(market_id).strip() if market_id not in (None, "") else None
                    ),
                    outcome=outcome,
                    last_price=last_price,
                )
            restored.total_fees = float(raw_snapshot.get("fees", 0.0))
            restored.total_slippage = float(raw_snapshot.get("slippage", 0.0))
            if not math.isfinite(restored.total_fees) or not math.isfinite(
                restored.total_slippage
            ):
                return False
            restored._settled_markets.update(
                self._scope_market_id(key) for key in self._settled
            )
        except (TypeError, ValueError):
            return False
        target = self.portfolio
        target.initial_cash = restored.initial_cash
        target.cash = restored.cash
        target.currency = restored.currency
        target.positions = restored.positions
        target.fills = []
        target.orders = {}
        target.total_fees = restored.total_fees
        target.total_slippage = restored.total_slippage
        target._settled_markets = restored._settled_markets
        self._restore_shadow_open_lots(raw_snapshot, target=target)
        return True

    def _restore_risk_from_aggregates(self) -> bool:
        if self.risk is None:
            return False
        aggregator = getattr(self.store, "aggregate_paper_fill_exposure", None)
        if not callable(aggregator):
            return False
        try:
            rows = aggregator(
                self._run_id,
                strategy_id=self._execution_strategy_id,
            )
        except Exception:
            return False
        settled_markets = {
            self._scope_market_id(key)
            for key in self._settled
        }
        aggregate_fills: list[Fill] = []
        for row in rows:
            if not isinstance(row, Mapping):
                continue
            market_id = str(row.get("market_id", "")).strip()
            if not market_id or market_id in settled_markets:
                continue
            signed_notional = _finite_number(row.get("signed_notional")) or 0.0
            price = _finite_number(row.get("price")) or 0.0
            if abs(signed_notional) <= 1e-12 or price <= 0:
                continue
            try:
                market_type = MarketType(str(row.get("market_type", "prediction")).lower())
                timestamp = parse_timestamp(row.get("timestamp")) or self.spec.registration_timestamp
                outcome = str(row.get("outcome", "yes")).strip().lower() or "yes"
                metadata = {"outcome": outcome}
                event_id = str(row.get("event_id", "")).strip()
                if event_id:
                    metadata["event_id"] = event_id
                aggregate_fills.append(
                    Fill(
                        timestamp=timestamp,
                        market_type=market_type,
                        symbol=market_id,
                        side=Side.BUY if signed_notional > 0 else Side.SELL,
                        quantity=abs(signed_notional) / price,
                        price=price,
                        fees=0.0,
                        slippage=0.0,
                        strategy_id=str(row.get("strategy_id", self._execution_strategy_id)),
                        order_id=(
                            "paper-aggregate-"
                            + market_id
                            + "-"
                            + str(len(aggregate_fills))
                        ),
                        market_id=market_id,
                        metadata=metadata,
                    )
                )
            except (TypeError, ValueError):
                continue
        self.risk.reconcile_fills(aggregate_fills)
        return True

    def _stored_fills_for_market(
        self,
        market_id: str,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> tuple[Fill, ...]:
        loader = getattr(self.store, "list_paper_fills", None)
        if callable(loader):
            try:
                return tuple(
                    loader(
                        self._run_id,
                        strategy_id=self._execution_strategy_id,
                        market_id=str(market_id),
                        start=start,
                        end=end,
                        limit=_MAX_PAPER_FILL_ROWS,
                    )
                )
            except Exception:
                return ()
        try:
            fills = self.store.load_fills(
                strategy_id=self._execution_strategy_id,
                symbol=str(market_id),
                start=start,
                end=end,
                limit=_MAX_PAPER_FILL_ROWS,
            )
        except Exception:
            return ()
        return tuple(
            fill
            for fill in fills
            if str(fill.metadata.get("paper_experiment_id", "")) == self._run_id
        )

    def _restore_ledger(self) -> None:
        raw_snapshot = self._state.get("portfolio")
        prefetched_count: int | None = None
        prefetched_fills: tuple[Fill, ...] | None = None
        if (
            isinstance(raw_snapshot, Mapping)
            and self._legacy_shadow_open_lot_migration_needed(raw_snapshot)
        ):
            prefetched_count, prefetched_fills = self._load_bounded_paper_fills()
        if self._restore_portfolio_snapshot():
            if prefetched_count is None:
                count_loader = getattr(self.store, "count_paper_fills", None)
                if callable(count_loader):
                    try:
                        self._fill_count = max(
                            self._fill_count,
                            int(
                                count_loader(
                                    self._run_id,
                                    strategy_id=self._execution_strategy_id,
                                )
                            ),
                        )
                    except Exception:
                        pass
            else:
                self._fill_count = max(self._fill_count, prefetched_count)
            self._restore_legacy_shadow_open_lots(
                raw_snapshot if isinstance(raw_snapshot, Mapping) else {},
                fills=prefetched_fills,
            )
            return
        # Legacy state may not contain a compact portfolio projection.  Keep
        # that migration path bounded and scoped to this paper run.
        if prefetched_count is None or prefetched_fills is None:
            prefetched_count, prefetched_fills = self._load_bounded_paper_fills()
        self._fill_count = max(self._fill_count, prefetched_count)
        existing = {fill.order_id for fill in getattr(self.portfolio, "fills", ())}
        for fill in prefetched_fills:
            if fill.order_id in existing:
                continue
            try:
                self.portfolio.apply_fill(fill)
            except (TypeError, ValueError):
                continue

    def _restore_settlements(self) -> None:
        if not self._settlement_by_market:
            try:
                observations = self.store.list_latest_paper_observations(
                    self._run_id,
                    per_market_limit=1,
                    per_scope_limit=1,
                )
            except Exception:
                observations = []
            for item in observations:
                if not isinstance(item, Mapping):
                    continue
                payload = item.get("payload")
                if not isinstance(payload, Mapping) or not _is_terminal(
                    payload.get("settlement")
                ):
                    continue
                market_id = str(item.get("market_id", payload.get("market_id", "")))
                scope_key = self._observation_scope_key(market_id, payload)
                self._settlement_by_market[scope_key] = str(payload["settlement"])
        for scope_key, raw_state in self._settlement_by_market.items():
            try:
                state = (
                    raw_state
                    if isinstance(raw_state, SettlementState)
                    else SettlementState(str(raw_state).strip().lower())
                )
                if not _is_terminal(state):
                    continue
                self.portfolio.resolve(
                    ResolvedContract(
                        self._scope_market_id(scope_key),
                        state,
                        self.spec.registration_timestamp,
                        "persisted paper settlement",
                    )
                )
                self._settled.add(scope_key)
            except (TypeError, ValueError):
                continue
        if self.risk is not None and not self._restore_risk_from_aggregates():
            settled_market_ids = {
                self._scope_market_id(key)
                for key in self._settled
            }
            self.risk.reconcile_fills(
                fill
                for fill in self.portfolio.fills
                if str(fill.market_id or fill.symbol) not in settled_market_ids
            )

    def _persist_state(self, *, last_timestamp: datetime | None) -> None:
        if last_timestamp is not None:
            self._state["last_timestamp"] = last_timestamp.isoformat()
        risk_state = self.risk.status() if self.risk is not None else None
        if risk_state is not None:
            risk_day = getattr(self.risk, "_day", None)
            risk_state["day"] = risk_day.isoformat() if risk_day is not None else None
            risk_state["day_start_equity"] = self.risk.day_start_equity
        model_state = _json_state(self.model)
        if model_state is _UNSAFE_STATE:
            self._state.pop("model_state", None)
        else:
            self._state["model_state"] = model_state
        portfolio_snapshot = self.portfolio.snapshot()
        open_lots, shadow_seen = self._shadow_open_lot_records()
        if shadow_seen:
            # Keep attribution state beside the compact portfolio projection.
            # Ordinary paper runs have no shadow fills and retain their
            # historical snapshot shape.
            portfolio_snapshot["open_lots"] = open_lots
        current_equity = _finite_number(portfolio_snapshot.get("equity"))
        if current_equity is None:
            current_equity = float(self.portfolio.initial_cash)
        prior_peak = _finite_number(self._state.get("forward_peak_equity"))
        if prior_peak is None or prior_peak <= 0:
            prior_peak = float(self.portfolio.initial_cash)
        peak_equity = max(prior_peak, current_equity)
        current_drawdown = max(0.0, 1.0 - current_equity / peak_equity) if peak_equity > 0 else 0.0
        prior_drawdown = _finite_number(self._state.get("forward_max_drawdown")) or 0.0
        self._state["forward_peak_equity"] = peak_equity
        self._state["forward_max_drawdown"] = max(prior_drawdown, current_drawdown)
        operational_policy_record = (
            self._operational_policy.as_record()
            if self._operational_policy is not None
            else None
        )
        if operational_policy_record is not None and self._operational_settings is not None:
            operational_policy_record["operational_settings"] = dict(
                self._operational_settings
            )
        state_record = {
            "experiment_id": self._run_id,
            "registration_timestamp": self.spec.registration_timestamp.isoformat(),
            "execution_binding": dict(self._execution_binding),
            "research_mode": self._research_mode,
            "execution_strategy_id": self._execution_strategy_id,
            "processed_observations": sorted(self._processed),
            "cursor_by_market": {
                key: value.isoformat() for key, value in sorted(self._cursor.items())
            },
            "observation_open_by_market": {
                key: value.isoformat()
                for key, value in sorted(self._observation_open_by_market.items())
            },
            "source_cursor_by_market": {
                key: {"timestamp": value[0].isoformat(), "snapshot_id": value[1]}
                for key, value in sorted(self._source_cursor.items())
            },
            "settled_markets": sorted(self._settled),
            "signal_history_by_market": {
                key: list(value[-512:])
                for key, value in sorted(self._signal_history.items())
            },
            "settlement_by_market": dict(sorted(self._settlement_by_market.items())),
            "portfolio": portfolio_snapshot,
            "risk": risk_state,
            "order_sequence": self.trader._sequence,
            "fill_count": self._fill_count,
            "paper_only": True,
            "live_execution": False,
            "retrospective_replay": self._execution_mode == "historical_replay",
        }
        # Operational counters are an additive policy extension.  Keep the
        # legacy state shape untouched for ordinary paper runs, while retaining
        # an existing operational section for compatibility migrations.
        if (
            self._operational_policy is not None
            or self._operational_settings is not None
            or "operational_paper" in self._state
        ):
            state_record["operational_paper"] = {
                "policy": operational_policy_record,
                "policy_hash": self._execution_binding.get("operational_policy_hash"),
                "settings": (
                    dict(self._operational_settings)
                    if self._operational_settings is not None
                    else None
                ),
                "counters": deepcopy(self._operational_counters),
            }
        self._state.update(state_record)
        self._state_version = self.store.save_paper_state(
            self._run_id,
            self._state,
            timestamp=last_timestamp or utc_now(),
            expected_version=self._state_version,
        )


def build_resolved_bet(
    *,
    experiment_id: str,
    market_id: str,
    strategy_id: str,
    settlement: Any,
    resolved_at: datetime,
    fills: Iterable[Fill],
    observation_open_timestamp: datetime | None = None,
) -> dict[str, Any] | None:
    """Aggregate one resolved prediction market into one independent bet.

    Fill prices and fees are execution facts.  The ledger computes gross PnL
    from reference-price cash flows, subtracts simulated slippage and fees for
    net PnL, and counts all fills for one market as one independent bet.
    """
    resolution = _settlement_value(settlement)
    if resolution not in {
        SettlementState.RESOLVED_YES.value,
        SettlementState.RESOLVED_NO.value,
        SettlementState.VOID.value,
    }:
        return None
    resolved_timestamp = ensure_utc(resolved_at)
    open_timestamp = (
        ensure_utc(observation_open_timestamp)
        if observation_open_timestamp is not None
        else None
    )
    if open_timestamp is not None and open_timestamp >= resolved_timestamp:
        return None
    ordered = sorted(
        (
            fill
            for fill in fills
            if isinstance(fill, Fill)
            and fill.market_type is MarketType.PREDICTION
            and str(fill.market_id or fill.symbol) == str(market_id)
            and (
                open_timestamp is None
                or open_timestamp <= ensure_utc(fill.timestamp) < resolved_timestamp
            )
        ),
        key=lambda fill: (fill.timestamp, fill.order_id),
    )
    if not ordered:
        return None
    position_by_outcome: dict[str, float] = {}
    average_actual_cost: dict[str, float] = {}
    bought_quantity: dict[str, float] = {}
    reference_cash_flow = 0.0
    fees = 0.0
    slippage = 0.0
    capital_at_risk = 0.0
    expected_probability_total = 0.0
    expected_probability_weight = 0.0
    expected_edge_total = 0.0
    expected_edge_weight = 0.0
    seen_order_ids: set[str] = set()
    partial_order_ids: set[str] = set()
    requested_quantity = 0.0
    filled_quantity = 0.0
    for fill in ordered:
        metadata = dict(fill.metadata or {})
        outcome = str(metadata.get("outcome", "yes")).strip().lower()
        if outcome not in {"yes", "no"}:
            outcome = "yes"
        signed_quantity = fill.quantity if fill.side is Side.BUY else -fill.quantity
        position_by_outcome[outcome] = position_by_outcome.get(outcome, 0.0) + signed_quantity
        reference = _finite_number(metadata.get("reference_price"))
        if reference is None or reference <= 0:
            reference = float(fill.price)
        reference_cash_flow -= signed_quantity * reference
        fees += float(fill.fees)
        slippage += abs(float(fill.price) - reference) * float(fill.quantity)
        if fill.side is Side.BUY:
            bought_quantity[outcome] = bought_quantity.get(outcome, 0.0) + fill.quantity
            average_actual_cost[outcome] = average_actual_cost.get(outcome, 0.0) + fill.quantity * fill.price
        order_id = str(metadata.get("order_attempt_id", fill.order_id))
        if order_id not in seen_order_ids:
            seen_order_ids.add(order_id)
            requested_quantity += _finite_number(metadata.get("requested_quantity")) or fill.quantity
        filled_quantity += float(fill.quantity)
        if bool(metadata.get("partial")) or str(metadata.get("execution_status", "")).upper() == "PARTIAL_FILL":
            partial_order_ids.add(order_id)
        selected_probability = _finite_number(fill.expected_probability)
        if selected_probability is not None:
            selected_probability = max(0.0, min(1.0, selected_probability))
            yes_probability = selected_probability if outcome == "yes" else 1.0 - selected_probability
            expected_probability_total += yes_probability * fill.quantity
            expected_probability_weight += fill.quantity
            expected_edge_total += (selected_probability - reference) * fill.quantity
            expected_edge_weight += fill.quantity
        exposure = 0.0
        for position_outcome, position_quantity in position_by_outcome.items():
            exposure += max(0.0, position_quantity) * float(fill.price)
        capital_at_risk = max(capital_at_risk, exposure)
    if resolution == SettlementState.RESOLVED_YES.value:
        winning = "yes"
        settlement_value = sum(max(0.0, position_by_outcome.get(outcome, 0.0)) for outcome in ("yes",))
    elif resolution == SettlementState.RESOLVED_NO.value:
        winning = "no"
        settlement_value = sum(max(0.0, position_by_outcome.get(outcome, 0.0)) for outcome in ("no",))
    else:
        winning = None
        settlement_value = sum(
            max(0.0, position_by_outcome.get(outcome, 0.0))
            * (
                average_actual_cost.get(outcome, 0.0) / bought_quantity[outcome]
                if bought_quantity.get(outcome, 0.0) > 0
                else 0.0
            )
            for outcome in ("yes", "no")
        )
    gross_pnl = settlement_value + reference_cash_flow
    net_pnl = gross_pnl - fees - slippage
    active_outcomes = tuple(sorted(outcome for outcome, quantity in position_by_outcome.items() if abs(quantity) > 1e-12))
    traded_outcomes = tuple(sorted(position_by_outcome))
    outcome = traded_outcomes[0] if len(traded_outcomes) == 1 else "mixed"
    resolved_position_count = len(traded_outcomes)
    expected_probability = (
        expected_probability_total / expected_probability_weight
        if expected_probability_weight > 0
        else None
    )
    expected_edge = (
        expected_edge_total / expected_edge_weight
        if expected_edge_weight > 0
        else None
    )
    record = {
        "bet_id": f"{experiment_id}:{market_id}",
        "experiment_id": str(experiment_id),
        "market_id": str(market_id),
        "strategy_id": str(strategy_id),
        "outcome": outcome,
        "resolution": resolution,
        "resolved_at": resolved_timestamp.isoformat(),
        "fills": len(ordered),
        "order_attempts": len(seen_order_ids),
        "partial_fills": len(partial_order_ids),
        "positions": resolved_position_count,
        "active_positions_at_resolution": len(active_outcomes),
        "gross_pnl": gross_pnl,
        "fees": fees,
        "slippage": slippage,
        "net_pnl": net_pnl,
        "capital_at_risk": capital_at_risk,
        "allocated_capital": capital_at_risk,
        "realized_pnl": net_pnl,
        "unrealized_pnl": 0.0,
        "costs": slippage,
        "completed_outcomes": 1.0,
        "reliability": 1.0,
        "drawdown": min(1.0, max(0.0, -net_pnl / capital_at_risk)) if capital_at_risk > 0 else 0.0,
        "roi": net_pnl / capital_at_risk if capital_at_risk > 0 else 0.0,
        "expected_probability_at_entry": expected_probability,
        "expected_edge_at_entry": expected_edge,
        "reference_cash_flow": reference_cash_flow,
        "settlement_value": settlement_value,
        "winning_outcome": winning,
        "requested_quantity": requested_quantity,
        "filled_quantity": filled_quantity,
        "fill_ratio": min(1.0, filled_quantity / requested_quantity) if requested_quantity > 0 else 0.0,
        "closed": True,
        "paper_only": True,
    }
    if open_timestamp is not None:
        coverage_seconds = (resolved_timestamp - open_timestamp).total_seconds()
        record.update(
            {
                "observation_open_timestamp": open_timestamp.isoformat(),
                "available_from": open_timestamp.isoformat(),
                "coverage_from": open_timestamp.isoformat(),
                "available_through": resolved_timestamp.isoformat(),
                "coverage_through": resolved_timestamp.isoformat(),
                "observation_coverage_seconds": coverage_seconds,
                "actual_coverage_seconds": coverage_seconds,
            }
        )
    return record


def _raw_observation_book(raw: Any, outcome: str) -> Any:
    if isinstance(raw, PredictionMarketSnapshot):
        return raw.order_book if outcome == "yes" else None
    if not isinstance(raw, Mapping):
        return None
    nested = raw.get("snapshot") if isinstance(raw.get("snapshot"), Mapping) else raw
    source = nested if isinstance(nested, Mapping) else raw
    if outcome == "yes":
        return source.get(
            "yes_order_book",
            source.get("order_book", raw.get("yes_order_book", raw.get("order_book"))),
        )
    return source.get("no_order_book", raw.get("no_order_book"))

def _explicit_book_timestamp(value: Any) -> bool:
    if isinstance(value, OrderBookSnapshot):
        return parse_timestamp(value.timestamp) is not None
    return isinstance(value, Mapping) and parse_timestamp(value.get("timestamp")) is not None


def _normalize_observation(
    raw: Any,
    *,
    default_market_id: str | None = None,
) -> tuple[str, dict[str, Any], OrderBookSnapshot | None, OrderBookSnapshot | None] | None:
    if isinstance(raw, PredictionMarketSnapshot):
        observation = dict(to_record(raw))
        return raw.market_id, observation, raw.order_book, None
    if not isinstance(raw, Mapping):
        return None
    nested = raw.get("snapshot") if isinstance(raw.get("snapshot"), Mapping) else raw
    market_id = str(
        nested.get("market_id", raw.get("market_id", default_market_id or ""))
    ).strip()
    stamp = parse_timestamp(nested.get("timestamp", raw.get("observed_at")))
    if not market_id or stamp is None:
        return None
    observation = dict(nested)
    if "market_id" not in observation:
        observation["market_id"] = market_id
    observation["timestamp"] = stamp
    for key in (
        "settlement",
        "expiry",
        "resolution_criteria",
        "event_id",
        "event_key",
        "event",
        "liquidity",
        *SHADOW_METADATA_KEYS,
        "volume",
        "fee_bps",
        "model_probability",
        "predicted_probability",
        "source_snapshot_id",
        "min_order_size",
        "order_min_size",
        "minimum_order_size",
        "min_size",
        "minimum_size",
        "quantity_min",
        "minQuantity",
        "min_notional",
        "minimum_notional",
        "min_cost",
        "minimum_cost",
        "min_notional_usd",
        "minimum_notional_usd",
        "minimum_cost_usd",
        "min_order_value",
        "minimum_order_value",
        "minOrderValue",
        "size_increment",
        "quantity_step",
        "order_size_increment",
        "step_size",
        "quantity_increment",
        "price_increment",
        "price_tick_size",
        "tick_size",
        "tickSize",
        "source_timestamp",
        "as_of_timestamp",
        "asof_timestamp",
        "as_of",
        "source_type",
        "provider_timestamp",
        "observed_timestamp",
        "observed_at",
        "request_started_at",
        "response_received_at",
        "available_at",
        "depth",
        "order_book_depth",
        "delay",
        "response_delay_seconds",
        "partial",
        "gaps",
        "gap",
        "fees",
        "fee_bps",
    ):
        if key not in observation and key in raw:
            observation[key] = raw[key]
    for key, value in _shadow_metadata(raw).items():
        observation.setdefault(key, value)
    yes_raw = _raw_observation_book(raw, "yes")
    no_raw = _raw_observation_book(raw, "no")
    yes_book = _book(yes_raw, stamp)
    no_book = _book(no_raw, stamp)
    if yes_book is not None:
        observation["yes_order_book"] = yes_book
    if no_book is not None:
        observation["no_order_book"] = no_book
    return market_id, observation, yes_book, no_book


def _book(raw: Any, fallback_timestamp: datetime) -> OrderBookSnapshot | None:
    if isinstance(raw, OrderBookSnapshot):
        return raw
    if not isinstance(raw, Mapping):
        return None
    def levels(value: Any, reverse: bool) -> tuple[OrderBookLevel, ...]:
        result: list[OrderBookLevel] = []
        for item in value if isinstance(value, (list, tuple)) else ():
            if isinstance(item, Mapping):
                price, size = item.get("price"), item.get("size", item.get("quantity"))
            elif isinstance(item, (list, tuple)) and len(item) >= 2:
                price, size = item[0], item[1]
            else:
                continue
            try:
                result.append(OrderBookLevel(float(price), float(size)))
            except (TypeError, ValueError):
                continue
        return tuple(sorted(result, key=lambda level: level.price, reverse=reverse))
    bids, asks = levels(raw.get("bids"), True), levels(raw.get("asks"), False)
    if not bids and not asks:
        return None
    try:
        return OrderBookSnapshot(
            parse_timestamp(raw.get("timestamp")) or fallback_timestamp,
            bids,
            asks,
            raw.get("token_id"),
            raw.get("condition_id"),
            parse_timestamp(raw.get("provider_timestamp")),
            raw.get("book_hash"),
            raw.get("min_order_size", raw.get("order_min_size", raw.get("minimum_order_size"))),
            raw.get("tick_size", raw.get("tickSize")),
            raw.get("neg_risk", raw.get("negRisk")),
            bool(raw.get("available", True)),
            str(raw.get("source", "")),
        )
    except (TypeError, ValueError):
        return None


def _reference_price(
    observation: Mapping[str, Any],
    book: OrderBookSnapshot | None,
    no_book: OrderBookSnapshot | None = None,
) -> float | None:
    candidates = (
        book.best_ask if book is not None else None,
        observation.get("yes_mid"),
        observation.get("yes_ask"),
        observation.get("yes_bid"),
        no_book.best_ask if no_book is not None else None,
        observation.get("no_mid"),
        observation.get("no_ask"),
        observation.get("no_bid"),
    )
    for value in candidates:
        try:
            number = float(value)
        except (TypeError, ValueError):
            continue
        if math.isfinite(number) and 0 < number <= 1:
            return number
    return None


def _snapshot_object_state(target: Any) -> dict[str, Any] | None:
    target_state = getattr(target, "__dict__", None)
    if not isinstance(target_state, dict):
        return None
    snapshot: dict[str, Any] = {}
    for key, value in target_state.items():
        try:
            snapshot[key] = deepcopy(value)
        except Exception:
            snapshot[key] = value
    return snapshot


def _restore_object_state(target: Any, snapshot: dict[str, Any] | None) -> None:
    target_state = getattr(target, "__dict__", None)
    if not isinstance(target_state, dict) or snapshot is None:
        return
    target_state.clear()
    for key, value in snapshot.items():
        try:
            target_state[key] = deepcopy(value)
        except Exception:
            target_state[key] = value
_UNSAFE_STATE = object()


def _json_state(value: Any) -> Any:
    snapshot = _snapshot_object_state(value)
    if snapshot is None:
        return _UNSAFE_STATE

    def convert(item: Any) -> Any:
        if item is None or isinstance(item, (str, bool, int)):
            return item
        if isinstance(item, float):
            return item if math.isfinite(item) else _UNSAFE_STATE
        if isinstance(item, Mapping):
            result: dict[str, Any] = {}
            for key, child in item.items():
                converted = convert(child)
                if converted is _UNSAFE_STATE:
                    return _UNSAFE_STATE
                result[str(key)] = converted
            return result
        if isinstance(item, (list, tuple)):
            result = []
            for child in item:
                converted = convert(child)
                if converted is _UNSAFE_STATE:
                    return _UNSAFE_STATE
                result.append(converted)
            return result
        return _UNSAFE_STATE

    return convert(snapshot)


def _restore_mutable_state(target: Any, snapshot: Any) -> None:
    target_state = getattr(target, "__dict__", None)
    snapshot_state = getattr(snapshot, "__dict__", None)
    if target_state is None or snapshot_state is None:
        raise TypeError("paper rollback requires mutable state objects")
    target_state.clear()
    target_state.update(snapshot_state)


def _observation_timestamp(value: Any) -> datetime | None:
    if isinstance(value, PredictionMarketSnapshot):
        return value.timestamp
    if isinstance(value, Mapping):
        nested = value.get("snapshot") if isinstance(value.get("snapshot"), Mapping) else value
        if isinstance(nested, Mapping):
            return (
                parse_timestamp(nested.get("timestamp"))
                or parse_timestamp(value.get("timestamp"))
                or parse_timestamp(value.get("observed_at"))
            )
    return parse_timestamp(getattr(value, "timestamp", None))


def _observation_sort_key(value: Any, tzinfo: Any) -> tuple[datetime, str, datetime, str]:
    if isinstance(value, Mapping):
        nested = value.get("snapshot") if isinstance(value.get("snapshot"), Mapping) else value
        if isinstance(nested, Mapping):
            decision_stamp = _observation_timestamp(value)
            source_stamps = [
                parse_timestamp(nested.get(name))
                for name in ("source_timestamp", "as_of_timestamp", "asof_timestamp", "as_of", "provider_timestamp")
            ]
            source_stamps.extend(
                parse_timestamp(value.get(name))
                for name in ("source_timestamp", "as_of_timestamp", "asof_timestamp", "as_of", "provider_timestamp")
            )
            source_stamp = max((stamp for stamp in source_stamps if stamp is not None), default=decision_stamp)
            market = nested.get("market_id", value.get("market_id", ""))
        else:
            decision_stamp = _observation_timestamp(value)
            source_stamp = parse_timestamp(getattr(value, "source_timestamp", None)) or decision_stamp
            market = value.get("market_id", "")
    else:
        decision_stamp = _observation_timestamp(value)
        source_stamp = parse_timestamp(getattr(value, "source_timestamp", None)) or decision_stamp
        market = getattr(value, "market_id", "")
    decision = decision_stamp or datetime.fromtimestamp(0, tz=tzinfo)
    source = source_stamp or decision
    return decision, str(market), source, _canonical_json(value)

def _model_probability_evaluation(model: Any | None, observation: Mapping[str, Any]):
    return evaluate_model_probability_evidence(model, observation)


def _model_probability(model: Any | None, observation: Mapping[str, Any]) -> float | None:
    """Numeric compatibility wrapper around the canonical model evaluator."""
    return _model_probability_evaluation(model, observation).probability
def _finite_number(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def _settlement_value(value: Any) -> str:
    if isinstance(value, SettlementState):
        return value.value
    return str(value or "").strip().lower()
def _is_terminal(value: Any) -> bool:
    try:
        state = value if isinstance(value, SettlementState) else SettlementState(str(value).strip().lower())
    except ValueError:
        return False
    return state in {SettlementState.RESOLVED_YES, SettlementState.RESOLVED_NO, SettlementState.VOID}


def _canonical_value(value: Any) -> Any:
    if isinstance(value, Enum):
        return _canonical_value(value.value)
    if isinstance(value, datetime):
        return ensure_utc(value).isoformat()
    if isinstance(value, Mapping):
        return {str(key): _canonical_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_canonical_value(item) for item in value]
    if hasattr(value, "__dataclass_fields__"):
        return _canonical_value(to_record(value))
    return value


def _canonical_json(value: Any) -> str:
    import json
    return json.dumps(_canonical_value(value), sort_keys=True, separators=(",", ":"), default=repr)


def historical_replay_id(spec: ForwardTestSpec, observations: Sequence[Any]) -> str:
    if not isinstance(spec, ForwardTestSpec):
        raise TypeError("spec must be a ForwardTestSpec")
    ordered = sorted(
        observations,
        key=lambda value: _observation_sort_key(value, spec.registration_timestamp.tzinfo),
    )
    digest = hashlib.sha256(_canonical_json(ordered).encode("utf-8")).hexdigest()[:24]
    return f"historical-{spec.experiment_id}-{digest}"


def run_forward_paper(
    spec: ForwardTestSpec,
    *,
    store: AxiomStore,
    strategy: Any,
    model: Any | None = None,
    provider: Any | None = None,
    risk: RiskEngine | None = None,
    portfolio: Portfolio | None = None,
    observations: Iterable[Any] | None = None,
    config: PaperTradingConfig | None = None,
    operational_policy: OperationalPaperPolicy | Mapping[str, Any] | Any | None = None,
    operational_settings: Mapping[str, Any] | None = None,
    now: datetime | None = None,
) -> PaperEngineCycle:
    """Run only post-registration observations for a frozen paper test."""
    return ForwardPaperEngine(
        spec,
        store=store,
        strategy=strategy,
        model=model,
        provider=provider,
        risk=risk,
        portfolio=portfolio,
        config=config,
        operational_policy=operational_policy,
        operational_settings=operational_settings,
    ).run(observations, now=now)


def run_historical_replay(
    spec: ForwardTestSpec,
    *,
    store: AxiomStore,
    strategy: Any,
    observations: Iterable[Any],
    model: Any | None = None,
    risk: RiskEngine | None = None,
    portfolio: Portfolio | None = None,
    config: PaperTradingConfig | None = None,
    now: datetime | None = None,
) -> PaperEngineCycle:
    """Explicit historical replay entry point; it is not forward registration."""
    materialized = list(observations)
    return ForwardPaperEngine(
        spec,
        store=store,
        strategy=strategy,
        model=model,
        risk=risk,
        portfolio=portfolio,
        config=config,
        storage_namespace=historical_replay_id(spec, materialized),
        execution_mode="historical_replay",
    ).run(materialized, now=now)
__all__ = [
    "ForwardPaperEngine",
    "OperationalPaperPolicy",
    "PAPER_OBSERVATION_AUTHORITY_REQUIRED",
    "PAPER_STATE_EXECUTION_BINDING_MISMATCH",
    "PAPER_STATE_OPERATIONAL_COUNTERS_INVALID",
    "PAPER_STATE_FILL_RESTORE_OVERFLOW",
    "PAPER_STATE_FILL_RESTORE_COUNT_MISMATCH",
    "PaperEngineCycle",
    "build_resolved_bet",
    "historical_replay_id",
    "paper_execution_binding",
    "paper_state_binding_blocker",
    "run_forward_paper",
    "run_historical_replay",
]
