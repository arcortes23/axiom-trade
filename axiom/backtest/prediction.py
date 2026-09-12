"""Historical prediction-market simulator with explicit research modes.

The legacy simulator remains available for existing callers.  New research
must choose either ``PRICE_PROXY_RESEARCH`` (price-path assumptions only) or
``RECORDED_BOOK_REPLAY`` (strictly observed, timestamped books).
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
import math
from typing import Any, Mapping, Sequence

from axiom.domain import (
    MarketType,
    OrderBookLevel,
    OrderBookSnapshot,
    PredictionMarketSnapshot,
    ResolvedContract,
    ResearchQuality,
    SettlementState,
    Side,
    SimulationQuality,
    ensure_utc,
    parse_timestamp,
)
from axiom.metrics import calculate_prediction_metrics
from axiom.portfolio import OrderRequest, Portfolio
from axiom.strategy import StrategyDefinition, evaluate_signal, validate_strategy
from axiom.strategy.signals import evaluate_model_probability_evidence, evaluate_signal_evaluation
from .types import BacktestResult


class PredictionResearchMode(str, Enum):
    """Execution evidence modes for prediction-market research."""

    PRICE_PROXY_RESEARCH = "PRICE_PROXY_RESEARCH"
    RECORDED_BOOK_REPLAY = "RECORDED_BOOK_REPLAY"


PRICE_PROXY_RESEARCH = PredictionResearchMode.PRICE_PROXY_RESEARCH
RECORDED_BOOK_REPLAY = PredictionResearchMode.RECORDED_BOOK_REPLAY
PRICE_PROXY_ASSUMPTIONS_VERSION = "price-proxy-v1"
RECORDED_BOOK_REPLAY_ASSUMPTIONS_VERSION = "recorded-book-replay-v1"
CANONICAL_EVALUATOR_VERSION = "evaluate_signal_evaluation:v1"


def _mode(value: Any) -> PredictionResearchMode | None:
    if value is None:
        return None
    if isinstance(value, PredictionResearchMode):
        return value
    normalized = str(value).strip().upper().replace("-", "_").replace(" ", "_")
    aliases = {
        "PRICE_PROXY": PredictionResearchMode.PRICE_PROXY_RESEARCH,
        "PRICE_PROXY_RESEARCH": PredictionResearchMode.PRICE_PROXY_RESEARCH,
        "RECORDED_BOOK": PredictionResearchMode.RECORDED_BOOK_REPLAY,
        "RECORDED_BOOK_REPLAY": PredictionResearchMode.RECORDED_BOOK_REPLAY,
        "BOOK_REPLAY": PredictionResearchMode.RECORDED_BOOK_REPLAY,
    }
    try:
        return aliases[normalized]
    except KeyError as exc:
        raise ValueError(f"unsupported prediction research mode: {value!r}") from exc


def _nested(item: Any) -> Any:
    if isinstance(item, Mapping) and isinstance(item.get("snapshot"), Mapping):
        return item["snapshot"]
    return item


def _market_id(item: Any) -> str:
    nested = _nested(item)
    if isinstance(nested, Mapping):
        return str(nested.get("market_id", item.get("market_id", ""))).strip()
    return str(getattr(nested, "market_id", getattr(item, "market_id", ""))).strip()


def _observation_timestamp(item: Any) -> datetime | None:
    nested = _nested(item)
    for candidate in (nested, item):
        value = candidate.get("timestamp") if isinstance(candidate, Mapping) else getattr(candidate, "timestamp", None)
        stamp = parse_timestamp(value)
        if stamp is not None:
            return stamp
    return None


def _normalize_row(item: Any) -> Any:
    """Flatten a persisted ``{snapshot: {...}, metadata...}`` row safely."""
    if not isinstance(item, Mapping) or not isinstance(item.get("snapshot"), Mapping):
        return item
    nested = dict(item["snapshot"])
    # The snapshot is authoritative for identity and price fields; outer fields
    # carry capture/lineage metadata and are never allowed to overwrite it.
    for key, value in item.items():
        if key != "snapshot":
            nested.setdefault(key, value)
    return nested


def _value(item: Any, name: str, default: Any = None) -> Any:
    nested = _nested(item)
    if isinstance(nested, Mapping):
        value = nested.get(name, default)
    else:
        value = getattr(nested, name, default)
    if value is default and nested is not item and isinstance(item, Mapping):
        value = item.get(name, default)
    return value


def _time(item: Any) -> datetime:
    stamp = _observation_timestamp(item)
    return stamp or datetime.min.replace(tzinfo=timezone.utc)


def _source_time(item: Any) -> datetime | None:
    """Return the latest explicit provider/source timestamp in ``item``."""
    candidates = [_nested(item)]
    if item is not candidates[0]:
        candidates.append(item)
    stamps: list[datetime] = []
    for candidate in candidates:
        for name in (
            "source_timestamp",
            "as_of_timestamp",
            "asof_timestamp",
            "as_of",
            "provider_timestamp",
        ):
            value = candidate.get(name) if isinstance(candidate, Mapping) else getattr(candidate, name, None)
            stamp = parse_timestamp(value)
            if stamp is not None:
                stamps.append(stamp)
    return max(stamps) if stamps else None


def _metadata_times(item: Any) -> tuple[tuple[str, datetime], ...]:
    """Collect causal metadata timestamps before ordering or evaluation."""
    candidates = [_nested(item)]
    if item is not candidates[0]:
        candidates.append(item)
    names = (
        "source_timestamp",
        "as_of_timestamp",
        "asof_timestamp",
        "as_of",
        "provider_timestamp",
        "observed_timestamp",
        "observed_at",
        "available_at",
        "request_started_at",
        "response_received_at",
    )
    result: list[tuple[str, datetime]] = []
    seen: set[tuple[str, datetime]] = set()
    for candidate in candidates:
        for name in names:
            value = candidate.get(name) if isinstance(candidate, Mapping) else getattr(candidate, name, None)
            stamp = parse_timestamp(value)
            if stamp is not None and (name, stamp) not in seen:
                seen.add((name, stamp))
                result.append((name, stamp))
    return tuple(result)


def _sort_key(item: Any) -> tuple[datetime, str, datetime, str, str]:
    nested = _nested(item)
    decision_time = _observation_timestamp(item) or datetime.min.replace(tzinfo=timezone.utc)
    source_time = _source_time(item) or decision_time
    return (
        decision_time,
        _market_id(item),
        source_time,
        str(_value(nested, "source_snapshot_id", _value(nested, "snapshot_id", ""))),
        _canonical_sort_value(item),
    )


def _canonical_sort_value(value: Any) -> str:
    """Stable deterministic tie-breaker that never changes causal ordering."""
    try:
        return repr(value) if not isinstance(value, Mapping) else repr(sorted(value.items(), key=lambda pair: str(pair[0])))
    except Exception:
        return repr(value)




def _number(item: Any, name: str, default: float = 0.0) -> float:
    try:
        value = float(_value(item, name, default))
    except (TypeError, ValueError):
        return default
    return value if math.isfinite(value) else default


def _probability(item: Any, name: str, default: float = 0.0) -> float:
    value = _number(item, name, default)
    return value if 0.0 <= value <= 1.0 else default
def _observed_probability(item: Any, *names: str) -> float | None:
    for name in names:
        raw = _value(item, name, None)
        if raw is None:
            continue
        try:
            value = float(raw)
        except (TypeError, ValueError):
            continue
        if math.isfinite(value) and 0.0 <= value <= 1.0:
            return value
    return None


def _quality(snapshots: Sequence[Any]) -> SimulationQuality:
    if len(snapshots) < 3:
        return SimulationQuality.LOW
    executable = sum(
        1
        for snap in snapshots
        if (
            (_number(snap, "yes_ask", 0.0) > 0 and _number(snap, "yes_bid", 0.0) > 0)
            or (_number(snap, "no_ask", 0.0) > 0 and _number(snap, "no_bid", 0.0) > 0)
        )
    )
    liquidity = sum(1 for snap in snapshots if _number(snap, "liquidity", 0.0) > 0)
    if executable < len(snapshots) // 2:
        return SimulationQuality.LOW
    return SimulationQuality.HIGH if liquidity * 2 >= len(snapshots) else SimulationQuality.MEDIUM


def _outcome(value: Any) -> str:
    if isinstance(value, SettlementState):
        return value.value
    return str(value or "").strip().lower()


def _complement_book(book: OrderBookSnapshot) -> OrderBookSnapshot:
    """Legacy compatibility only; explicit replay never calls this."""
    bids = tuple(OrderBookLevel(1.0 - level.price, level.size) for level in book.asks)
    asks = tuple(OrderBookLevel(1.0 - level.price, level.size) for level in book.bids)
    return OrderBookSnapshot(book.timestamp, bids=bids, asks=asks)


def _coerce_book(
    value: Any,
    fallback_timestamp: datetime,
    *,
    require_timestamp: bool = False,
) -> OrderBookSnapshot | None:
    if isinstance(value, OrderBookSnapshot):
        if require_timestamp and value.timestamp is None:
            return None
        return value
    if not isinstance(value, Mapping):
        return None

    def levels(raw: Any, *, reverse: bool) -> tuple[OrderBookLevel, ...]:
        if not isinstance(raw, (list, tuple)):
            return ()
        result: list[OrderBookLevel] = []
        for item in raw:
            if isinstance(item, Mapping):
                price, size = item.get("price", item.get("p")), item.get("size", item.get("quantity", item.get("q")))
            elif isinstance(item, (list, tuple)) and len(item) >= 2:
                price, size = item[0], item[1]
            else:
                continue
            try:
                parsed_price, parsed_size = float(price), float(size)
                if not math.isfinite(parsed_price) or not 0.0 <= parsed_price <= 1.0:
                    continue
                result.append(OrderBookLevel(parsed_price, parsed_size))
            except (TypeError, ValueError):
                continue
        return tuple(sorted(result, key=lambda level: level.price, reverse=reverse))

    bids, asks = levels(value.get("bids"), reverse=True), levels(value.get("asks"), reverse=False)
    if not bids and not asks:
        return None
    raw_timestamp = parse_timestamp(value.get("timestamp"))
    if require_timestamp and raw_timestamp is None:
        return None
    try:
        return OrderBookSnapshot(
            raw_timestamp or fallback_timestamp,
            bids=bids,
            asks=asks,
            token_id=value.get("token_id"),
        )
    except (TypeError, ValueError):
        return None


def _raw_book(row: Any, outcome: str) -> Any:
    if outcome == "yes":
        return _value(row, "order_book", _value(row, "yes_order_book"))
    return _value(row, "no_order_book")


def _proxy_quote(row: Any, outcome: str, side: Side) -> float | None:
    """Return only an observed same-market quote; no complement is inferred."""
    name = f"{outcome}_{'ask' if side is Side.BUY else 'bid'}"
    value = _probability(row, name, math.nan)
    if math.isfinite(value) and value > 0.0:
        return value
    name = f"{outcome}_mid"
    value = _probability(row, name, math.nan)
    return value if math.isfinite(value) and value > 0.0 else None


def _proxy_lifecycle_label(row: Any) -> str:
    fields = ("active", "closed", "archived", "accepting_orders", "enable_order_book", "lifecycle_as_of")
    known = any(_value(row, key) is not None for key in fields)
    return "PRICE_PATH" if known else "CONDITIONAL_PRICE_PATH"


def _validate_metadata_timestamps(row: Any, timestamp: datetime, market_id: str) -> None:
    """Validate source chronology separately from capture/availability time.

    A source observation can be captured after the event it describes.  The
    source timestamps must not look into the future of the observation, while
    transport and availability metadata are compared with the capture time
    when one is supplied.
    """
    source_names = {
        "source_timestamp",
        "as_of_timestamp",
        "asof_timestamp",
        "as_of",
        "provider_timestamp",
    }
    row_observed_at = _value(row, "observed_at")
    observed_at = parse_timestamp(row_observed_at)
    capture_cutoff = observed_at or timestamp
    for name, metadata_time in _metadata_times(row):
        cutoff = timestamp if name in source_names else capture_cutoff
        if metadata_time > cutoff:
            raise ValueError(
                f"prediction research {name} is future-dated for {market_id}"
            )
def _validate_replay_rows(rows: Sequence[Any]) -> None:
    if not rows:
        return
    ordered = sorted(rows, key=_sort_key)
    seen_market_timestamps: dict[str, datetime] = {}
    for index, row in enumerate(ordered):
        timestamp = _observation_timestamp(row)
        market_id = _market_id(row)
        if timestamp is None:
            raise ValueError(f"recorded replay row {index} is missing timestamp")
        if not market_id:
            raise ValueError(f"recorded replay row {index} is missing market_id")
        _validate_metadata_timestamps(row, timestamp, market_id)
        prior = seen_market_timestamps.get(market_id)
        if prior is not None and timestamp < prior:
            raise ValueError(f"recorded replay chronology is not monotonic for {market_id}")
        seen_market_timestamps[market_id] = timestamp
        yes_raw = _raw_book(row, "yes")
        no_raw = _raw_book(row, "no")
        yes_book = _coerce_book(yes_raw, timestamp, require_timestamp=True)
        no_book = _coerce_book(no_raw, timestamp, require_timestamp=True)
        terminal = _outcome(_value(row, "settlement", "open")) in {"resolved_yes", "resolved_no", "void"}
        if not terminal and (yes_book is None or no_book is None):
            raise ValueError(f"recorded replay row {index} requires observed YES and NO order books")
        for name, book in (("yes", yes_book), ("no", no_book)):
            if book is not None and ensure_utc(book.timestamp) > timestamp:
                raise ValueError(f"recorded replay {name} book is future-dated for {market_id}")
        source_type = str(_value(row, "source_type", "")).strip().upper()
        if source_type == "PAPER_FORWARD":
            raise ValueError("retrospective recorded replay cannot use PAPER_FORWARD rows")


def _validate_temporal_rows(rows: Sequence[Any]) -> None:
    previous: dict[str, datetime] = {}
    for index, row in enumerate(sorted(rows, key=_sort_key)):
        timestamp = _observation_timestamp(row)
        market_id = _market_id(row)
        if timestamp is None:
            raise ValueError(f"prediction research row {index} is missing timestamp")
        if not market_id:
            raise ValueError(f"prediction research row {index} is missing market_id")
        _validate_metadata_timestamps(row, timestamp, market_id)
        prior = previous.get(market_id)
        if prior is not None and timestamp < prior:
            raise ValueError(f"prediction research chronology is not monotonic for {market_id}")
        previous[market_id] = timestamp
        for outcome in ("yes", "no"):
            book = _coerce_book(_raw_book(row, outcome), timestamp)
            if book is not None and ensure_utc(book.timestamp) > timestamp:
                raise ValueError(f"prediction research {outcome} book is future-dated for {market_id}")


def _normalize_exit_contract(
    exit_policy: str | Mapping[str, Any] | None,
    holding_period: int | Mapping[str, Any],
) -> tuple[dict[str, Any], int]:
    """Normalize the frozen prediction exit contract without losing metadata."""
    if exit_policy is None:
        policy: dict[str, Any] = {"type": "fixed_holding_period"}
    elif isinstance(exit_policy, Mapping):
        policy = dict(exit_policy)
    else:
        policy = {"type": str(exit_policy)}
    nested_policy = policy.get("policy")
    if isinstance(nested_policy, Mapping):
        merged = dict(nested_policy)
        merged.update({key: value for key, value in policy.items() if key != "policy"})
        policy = merged
    policy_kind = str(policy.get("type", policy.get("kind", ""))).strip().lower()
    if policy_kind != "fixed_holding_period":
        raise ValueError("explicit price-proxy research requires fixed_holding_period exit_policy")
    raw_period: Any = holding_period
    if isinstance(raw_period, Mapping):
        raw_period = raw_period.get(
            "holding_period",
            raw_period.get("bars", raw_period.get("observations")),
        )
    policy_period = policy.get(
        "holding_period",
        policy.get("bars", policy.get("observations")),
    )
    if policy_period is not None:
        if raw_period in (None, 1):
            raw_period = policy_period
        elif raw_period != policy_period:
            raise ValueError("holding_period and exit_policy disagree")
    if isinstance(raw_period, bool) or not isinstance(raw_period, int) or raw_period < 1:
        raise ValueError("holding_period must be a positive integer")
    policy["type"] = "fixed_holding_period"
    policy["holding_period"] = raw_period
    policy.pop("kind", None)
    policy.pop("bars", None)
    policy.pop("observations", None)
    return policy, raw_period

def _normalize_observation_horizon(
    value: Mapping[str, Any] | int | None,
    holding_period: int,
) -> tuple[dict[str, Any], int]:
    """Normalize a fixed per-market observation-count horizon.

    Research plans intentionally do not mix elapsed-time and bar semantics:
    the same count is used by signal evaluation and by the exit scheduler.
    """
    if value is None:
        count = holding_period
        unit = "observations"
    elif isinstance(value, Mapping):
        unit = str(value.get("unit", "observations")).strip().lower()
        count = value.get("count", value.get("observations", value.get("bars")))
    else:
        unit = "observations"
        count = value
    if unit not in {"observation", "observations", "bar", "bars"}:
        raise ValueError("observation_horizon.unit must be observations")
    if isinstance(count, bool) or not isinstance(count, int) or count < 1:
        raise ValueError("observation_horizon.count must be a positive integer")
    if holding_period != 1 and int(holding_period) != int(count):
        raise ValueError("holding_period and observation_horizon.count disagree")
    return {
        "unit": "observations",
        "count": int(count),
        "semantics": "per_market_observation_count",
    }, int(count)






@dataclass(slots=True)
class PredictionMarketBacktester:
    initial_cash: float = 10_000.0
    fee_bps: float = 10.0
    slippage_bps: float = 5.0
    allocation: float = 0.25

    def __post_init__(self) -> None:
        if (
            not all(math.isfinite(float(value)) for value in (self.initial_cash, self.fee_bps, self.slippage_bps, self.allocation))
            or self.initial_cash < 0
            or self.fee_bps < 0
            or self.slippage_bps < 0
            or not 0.0 <= self.allocation <= 1.0
        ):
            raise ValueError("backtest cash, costs, and allocation must be finite and in range")

    def _run_legacy(
        self,
        snapshots: Sequence[PredictionMarketSnapshot | Mapping[str, Any]],
        strategy: StrategyDefinition | Mapping[str, Any] | str,
        *,
        resolutions: Mapping[str, ResolvedContract] | Sequence[ResolvedContract] | None = None,
        initial_cash: float | None = None,
        research_quality: ResearchQuality | str | None = None,
        model: Any | None = None,
        model_document: Any | None = None,
        mode: PredictionResearchMode | None = None,
        holding_period: int = 1,
        observation_horizon: Mapping[str, Any] | int | None = None,
        exit_policy: str | Mapping[str, Any] = "fixed_holding_period",
    ) -> BacktestResult:
        """Run the shared evaluator/portfolio loop.

        ``mode=None`` retains the historical compatibility behavior. Explicit
        modes are intentionally stricter and bind execution evidence into every
        curve row.
        """
        definition = validate_strategy(strategy)
        if definition.market_type is not MarketType.PREDICTION:
            raise ValueError("PredictionMarketBacktester requires a prediction strategy")
        normalized_exit_policy: str | Mapping[str, Any] = exit_policy
        if mode is not None:
            normalized_exit_policy, holding_period = _normalize_exit_contract(
                exit_policy,
                holding_period,
            )
            if isinstance(normalized_exit_policy, Mapping):
                raw_assumptions = normalized_exit_policy.get(
                    "assumptions",
                    normalized_exit_policy.get("cost_assumptions"),
                )
                if isinstance(raw_assumptions, Mapping):
                    expected_version = (
                        PRICE_PROXY_ASSUMPTIONS_VERSION
                        if mode is PredictionResearchMode.PRICE_PROXY_RESEARCH
                        else RECORDED_BOOK_REPLAY_ASSUMPTIONS_VERSION
                    )
                    supplied_version = str(raw_assumptions.get("version", "")).strip()
                    if supplied_version and supplied_version != expected_version:
                        raise ValueError(
                            f"exit_policy assumptions.version must be {expected_version!r}"
                        )
                    for name, actual in (
                        ("fee_bps", self.fee_bps),
                        ("slippage_bps", self.slippage_bps),
                    ):
                        if name not in raw_assumptions:
                            continue
                        try:
                            expected_cost = float(raw_assumptions[name])
                        except (TypeError, ValueError):
                            raise ValueError(
                                f"exit_policy assumptions.{name} must be numeric"
                            ) from None
                        if (
                            not math.isfinite(expected_cost)
                            or expected_cost < 0
                            or expected_cost != actual
                        ):
                            raise ValueError(
                                f"exit_policy assumptions.{name} does not match execution costs"
                            )
        horizon_document, holding_period = _normalize_observation_horizon(
            observation_horizon,
            holding_period,
        )
        normalized_rows = [_normalize_row(row) for row in snapshots]
        if mode is not None:
            _validate_temporal_rows(normalized_rows)
            if mode is PredictionResearchMode.RECORDED_BOOK_REPLAY:
                _validate_replay_rows(normalized_rows)
        rows = sorted(normalized_rows, key=_sort_key)
        model_source = model if model is not None else model_document
        if model_source is None and isinstance(strategy, Mapping):
            model_source = strategy.get("model_document", strategy.get("model"))
        contracts: dict[str, ResolvedContract] = {}
        if isinstance(resolutions, Mapping):
            contracts.update(resolutions)
        elif resolutions:
            contracts.update({contract.market_id: contract for contract in resolutions})
        portfolio = Portfolio(self.initial_cash if initial_cash is None else initial_cash)
        observed_outcomes: dict[str, str] = {}
        curve: list[dict[str, Any]] = []
        labels: list[SimulationQuality] = []
        history_by_market: dict[str, list[Any]] = {}
        proxy_pending: dict[str, list[dict[str, Any]]] = {}
        proxy_fills: list[dict[str, Any]] = []
        market_observation_index: dict[str, int] = {}
        for index, snapshot in enumerate(rows):
            timestamp = _time(snapshot)
            market_id = str(_value(snapshot, "market_id", ""))
            active_snapshot = dict(snapshot) if isinstance(snapshot, Mapping) else snapshot
            model_evaluation = evaluate_model_probability_evidence(model_source, active_snapshot) if model_source is not None else None
            if model_evaluation is not None and isinstance(active_snapshot, dict):
                active_snapshot["model_evaluation"] = model_evaluation.as_record()
                if model_evaluation.probability is not None and _value(active_snapshot, "model_probability") is None:
                    active_snapshot["model_probability"] = model_evaluation.probability
            market_history = history_by_market.setdefault(market_id, [])
            market_index = market_observation_index.get(market_id, 0)
            context = {
                "snapshots": tuple(market_history + [active_snapshot]),
                "observations": tuple(market_history + [active_snapshot]),
                "history": tuple(market_history + [active_snapshot]),
                "market_id": market_id,
            }
            if isinstance(active_snapshot, Mapping):
                context.update(active_snapshot)
                context["snapshots"] = tuple(market_history + [active_snapshot])
                context["observations"] = tuple(market_history + [active_snapshot])
                context["history"] = tuple(market_history + [active_snapshot])
            if model_evaluation is not None:
                context["model_evaluation"] = model_evaluation.as_record()
                if model_evaluation.probability is not None:
                    context["model_probability"] = model_evaluation.probability
            if model_source is not None:
                context["model_document"] = model_source
            evaluation = evaluate_signal_evaluation(definition, context)
            score = evaluation.score
            history_by_market[market_id].append(active_snapshot)
            # Explicit resolutions and snapshot settlement become observable
            # only at their timestamp; a resolved market cannot be re-entered.
            contract = contracts.get(market_id)
            resolved_now = False
            settlement = _value(snapshot, "settlement", SettlementState.OPEN)
            state = settlement if isinstance(settlement, SettlementState) else _outcome(settlement)
            if contract is not None and ensure_utc(contract.resolved_at) <= timestamp:
                portfolio.resolve(contract)
                observed_outcomes[market_id] = contract.outcome.value
                resolved_now = True
            elif contract is None and (
                state in {
                    SettlementState.RESOLVED_YES,
                    SettlementState.RESOLVED_NO,
                    SettlementState.VOID,
                }
                or state in {"resolved_yes", "resolved_no", "void"}
            ):
                outcome = state if isinstance(state, SettlementState) else SettlementState(state)
                portfolio.resolve(
                    ResolvedContract(
                        market_id,
                        outcome,
                        timestamp,
                        str(_value(snapshot, "resolution_criteria", "")),
                    )
                )
                if outcome is not SettlementState.UNKNOWN:
                    observed_outcomes[market_id] = outcome.value
                resolved_now = True
            effective_state = (
                SettlementState.OPEN
                if contract is not None and ensure_utc(contract.resolved_at) > timestamp
                else state
            )
            proxy_executed = False
            if mode is PredictionResearchMode.PRICE_PROXY_RESEARCH:
                pending_for_market = proxy_pending.get(market_id, [])
                remaining_pending: list[dict[str, Any]] = []
                for pending in pending_for_market:
                    if market_index < pending["due_observation_index"]:
                        remaining_pending.append(pending)
                        continue
                    if resolved_now:
                        continue
                    pending_side = pending["side"]
                    quote = _proxy_quote(snapshot, pending["outcome"], pending_side)
                    if quote is None:
                        pending["gap_observations"] += 1
                        remaining_pending.append(pending)
                        continue
                    position = portfolio.get_position(market_id, outcome=pending["outcome"])
                    if pending["kind"] == "entry":
                        quantity = (
                            max(0.0, portfolio.cash)
                            * max(0.0, min(1.0, self.allocation))
                            * min(1.0, abs(float(pending["score"])))
                            / quote
                        )
                    else:
                        quantity = position.quantity if position is not None else 0.0
                    if quantity <= 1e-12:
                        continue
                    modeled_price = quote * (
                        1.0
                        + (1.0 if pending_side is Side.BUY else -1.0)
                        * self.slippage_bps
                        / 10_000.0
                    )
                    fill = portfolio.execute_order(
                        OrderRequest(
                            symbol=market_id,
                            side=pending_side,
                            quantity=quantity,
                            market_type=MarketType.PREDICTION,
                            strategy_id=definition.id,
                            market_id=market_id,
                            outcome=pending["outcome"],
                            expected_probability=pending.get("trade_probability"),
                        ),
                        timestamp=timestamp,
                        price=quote,
                        fee_bps=self.fee_bps,
                        slippage_bps=self.slippage_bps,
                        metadata={
                            "evaluation_reason": pending["reason_code"],
                            "evaluation_evidence": dict(pending["evidence"]),
                            "research_mode": mode.value,
                            "execution_kind": pending["kind"],
                            "decision_timestamp": pending["decision_timestamp"],
                            "execution_timestamp": timestamp.isoformat(),
                            "decision_source_snapshot_id": pending.get("decision_source_snapshot_id"),
                            "execution_source_snapshot_id": _value(
                                snapshot,
                                "source_snapshot_id",
                                _value(snapshot, "snapshot_id"),
                            ),
                            "decision_observation_index": pending["decision_observation_index"],
                            "execution_observation_index": market_index,
                            "holding_observations": market_index - pending["decision_observation_index"],
                            "quote_gap_observations": pending["gap_observations"],
                            "raw_execution_price": quote,
                            "assumed_execution_price": modeled_price,
                            "fee_bps": self.fee_bps,
                            "slippage_bps": self.slippage_bps,
                            "assumption_version": PRICE_PROXY_ASSUMPTIONS_VERSION,
                            "price_path_status": pending["price_path_status"],
                        },
                    )
                    proxy_executed = True
                    proxy_fills.append(
                        {
                            "timestamp": timestamp,
                            "decision_timestamp": pending["decision_timestamp"],
                            "execution_kind": pending["kind"],
                            "raw_execution_price": quote,
                            "assumed_execution_price": modeled_price,
                            "fee_bps": self.fee_bps,
                            "slippage_bps": self.slippage_bps,
                            "assumption_version": PRICE_PROXY_ASSUMPTIONS_VERSION,
                            "quote_gap_observations": pending["gap_observations"],
                        }
                    )
                    if pending["kind"] == "entry":
                        remaining_pending.append(
                            {
                                "market_id": market_id,
                                "outcome": pending["outcome"],
                                "side": Side.SELL,
                                "kind": "exit",
                                "score": pending["score"],
                                "trade_probability": pending.get("trade_probability"),
                                "reason_code": pending["reason_code"],
                                "evidence": pending["evidence"],
                                "due_observation_index": market_index + holding_period,
                                "decision_observation_index": market_index,
                                "decision_timestamp": timestamp.isoformat(),
                                "decision_source_snapshot_id": _value(
                                    snapshot,
                                    "source_snapshot_id",
                                    _value(snapshot, "snapshot_id"),
                                ),
                                "price_path_status": pending["price_path_status"],
                                "gap_observations": 0,
                            }
                        )
                if remaining_pending:
                    proxy_pending[market_id] = remaining_pending
                else:
                    proxy_pending.pop(market_id, None)
            outcome = "yes" if score > 0 else "no"
            current = portfolio.get_position(market_id, outcome=outcome)
            current_quantity = current.quantity if current else 0.0
            model_probability = _value(active_snapshot, "model_probability")
            if model_probability is None and model_evaluation is not None:
                model_probability = model_evaluation.probability
            try:
                model_probability = float(model_probability)
                trade_probability = model_probability if outcome == "yes" else 1.0 - model_probability
                if not math.isfinite(trade_probability) or not 0.0 <= trade_probability <= 1.0:
                    trade_probability = None
            except (TypeError, ValueError):
                trade_probability = None
            ask_name = "yes_ask" if score > 0 else "no_ask"
            ask = _probability(snapshot, ask_name, 0.0)
            if score < 0 and ask <= 0:
                yes_bid = _probability(snapshot, "yes_bid", 0.0)
                ask = 1.0 - yes_bid if 0.0 < yes_bid < 1.0 else 0.0
            if mode is PredictionResearchMode.PRICE_PROXY_RESEARCH:
                raw_book = None
                order_book = None
            else:
                raw_book = _raw_book(snapshot, outcome)
                order_book = _coerce_book(
                    raw_book,
                    timestamp,
                    require_timestamp=mode is PredictionResearchMode.RECORDED_BOOK_REPLAY,
                )
                if (
                    mode is None
                    and outcome == "no"
                    and order_book is None
                ):
                    yes_book = _coerce_book(_value(snapshot, "order_book", _value(snapshot, "yes_order_book")), timestamp)
                    if yes_book is not None:
                        order_book = _complement_book(yes_book)
                if order_book is not None and order_book.timestamp > timestamp:
                    order_book = None
                if order_book is not None and order_book.best_ask is not None:
                    ask = order_book.best_ask
            if (
                mode is not PredictionResearchMode.PRICE_PROXY_RESEARCH
                and not resolved_now
                and effective_state not in {SettlementState.VOID, SettlementState.UNKNOWN, "void", "unknown", SettlementState.RESOLVED_YES, SettlementState.RESOLVED_NO, "resolved_yes", "resolved_no"}
                and score != 0
                and market_id
                and ask > 0
            ):
                desired = max(0.0, portfolio.cash) * max(0.0, min(1.0, self.allocation)) * min(1.0, abs(score)) / ask
                delta = desired - (current_quantity if (current and (current.outcome or "yes") == outcome) else 0.0)
                if delta > 1e-12:
                    portfolio.execute_order(
                        OrderRequest(
                            symbol=market_id,
                            side=Side.BUY,
                            quantity=delta,
                            market_type=MarketType.PREDICTION,
                            strategy_id=definition.id,
                            market_id=market_id,
                            outcome=outcome,
                            expected_probability=trade_probability,
                        ),
                        timestamp=timestamp,
                        price=None if order_book is not None else ask,
                        order_book=order_book,
                        fee_bps=self.fee_bps,
                        slippage_bps=self.slippage_bps,
                        metadata={
                            "evaluation_reason": evaluation.reason_code,
                            "evaluation_evidence": dict(evaluation.evidence),
                            "research_mode": mode.value if mode is not None else "LEGACY",
                            "raw_execution_price": ask if mode is not None else None,
                            "assumed_execution_price": (
                                ask
                                * (
                                    1.0
                                    + self.slippage_bps / 10_000.0
                                )
                                if mode is not None
                                else None
                            ),
                            "fee_bps": self.fee_bps if mode is not None else None,
                            "slippage_bps": self.slippage_bps if mode is not None else None,
                            "assumption_version": (
                                RECORDED_BOOK_REPLAY_ASSUMPTIONS_VERSION
                                if mode is PredictionResearchMode.RECORDED_BOOK_REPLAY
                                else PRICE_PROXY_ASSUMPTIONS_VERSION
                                if mode is PredictionResearchMode.PRICE_PROXY_RESEARCH
                                else None
                            ),
                        },
                    )
            if (
                mode is PredictionResearchMode.PRICE_PROXY_RESEARCH
                and not proxy_executed
                and not resolved_now
                and effective_state not in {SettlementState.VOID, SettlementState.UNKNOWN, "void", "unknown", SettlementState.RESOLVED_YES, SettlementState.RESOLVED_NO, "resolved_yes", "resolved_no"}
                and score != 0
                and market_id
                and not proxy_pending.get(market_id)
                and portfolio.get_position(market_id, outcome=outcome) is None
            ):
                proxy_pending[market_id] = [
                    {
                        "market_id": market_id,
                        "outcome": outcome,
                        "side": Side.BUY,
                        "kind": "entry",
                        "score": score,
                        "trade_probability": trade_probability,
                        "reason_code": evaluation.reason_code,
                        "evidence": dict(evaluation.evidence),
                        "due_observation_index": market_index + holding_period,
                        "decision_observation_index": market_index,
                        "decision_timestamp": timestamp.isoformat(),
                        "decision_source_snapshot_id": _value(
                            snapshot,
                            "source_snapshot_id",
                            _value(snapshot, "snapshot_id"),
                        ),
                        "price_path_status": _proxy_lifecycle_label(snapshot),
                        "gap_observations": 0,
                    }
                ]
            if mode is None:
                yes_mid = _probability(snapshot, "yes_mid", _probability(snapshot, "yes_ask", 0.0))
                no_mid = _probability(snapshot, "no_mid", _probability(snapshot, "no_ask", 0.0))
                if no_mid <= 0 and 0.0 < yes_mid < 1.0:
                    no_mid = 1.0 - yes_mid
                prices = {market_id: yes_mid, f"{market_id}|no": no_mid}
            else:
                yes_mid = _observed_probability(snapshot, "yes_mid", "yes_ask")
                no_mid = _observed_probability(snapshot, "no_mid", "no_ask")
                prices = {}
                if yes_mid is not None:
                    prices[market_id] = yes_mid
                if no_mid is not None:
                    prices[f"{market_id}|no"] = no_mid
            equity = portfolio.equity(prices)
            quality = SimulationQuality.HIGH if _number(snapshot, "liquidity", 0.0) > 0 and (
                (_probability(snapshot, "yes_bid", 0.0) > 0 and _probability(snapshot, "yes_ask", 0.0) > 0)
                or (_probability(snapshot, "no_bid", 0.0) > 0 and _probability(snapshot, "no_ask", 0.0) > 0)
            ) else SimulationQuality.MEDIUM
            labels.append(quality)
            curve.append(
                {
                    "timestamp": timestamp,
                    "equity": equity,
                    "cash": portfolio.cash,
                    "market_id": market_id,
                    "quality": quality.value,
                    "score": score,
                    "side": evaluation.side,
                    "reason_code": evaluation.reason_code,
                    "evaluation_evidence": dict(evaluation.evidence),
                    "research_mode": mode.value if mode is not None else "LEGACY",
                    "evaluator": CANONICAL_EVALUATOR_VERSION,
                    "assumption_version": (
                        PRICE_PROXY_ASSUMPTIONS_VERSION
                        if mode is PredictionResearchMode.PRICE_PROXY_RESEARCH
                        else RECORDED_BOOK_REPLAY_ASSUMPTIONS_VERSION
                        if mode is PredictionResearchMode.RECORDED_BOOK_REPLAY
                        else None
                    ),
                    "exit_policy": (
                        dict(normalized_exit_policy)
                        if isinstance(normalized_exit_policy, Mapping)
                        else normalized_exit_policy
                    )
                    if mode is not None
                    else None,
                    "holding_period": holding_period if mode is not None else None,
                    "observation_horizon": dict(horizon_document) if mode is not None else None,
                    "price_path_status": _proxy_lifecycle_label(snapshot)
                    if mode is PredictionResearchMode.PRICE_PROXY_RESEARCH
                    else None,
                    "source_type": _value(snapshot, "source_type"),
                    "execution_evidence": (
                        {
                            "raw_book_observed": mode is PredictionResearchMode.RECORDED_BOOK_REPLAY
                            and order_book is not None,
                            "book_timestamp": order_book.timestamp.isoformat()
                            if order_book is not None
                            else None,
                            "depth": _value(snapshot, "depth", _value(snapshot, "order_book_depth")),
                            "delay": _value(snapshot, "delay", _value(snapshot, "response_delay_seconds")),
                            "partial": _value(snapshot, "partial"),
                            "gaps": _value(snapshot, "gaps", _value(snapshot, "gap")),
                            "fees": _value(snapshot, "fees", _value(snapshot, "fee_bps")),
                        }
                        if mode is PredictionResearchMode.RECORDED_BOOK_REPLAY
                        else {
                            "raw_execution": tuple(proxy_fills[-8:]),
                            "assumed_execution": True,
                        }
                        if mode is PredictionResearchMode.PRICE_PROXY_RESEARCH
                        else None
                    ),
                }
            )
            market_observation_index[market_id] = market_index + 1
        outcomes = dict(observed_outcomes)
        probability_records: list[dict[str, float | None]] = []
        for fill in portfolio.fills:
            terminal = outcomes.get(fill.market_id or fill.symbol)
            if fill.expected_probability is None or terminal not in {
                SettlementState.RESOLVED_YES.value,
                SettlementState.RESOLVED_NO.value,
            }:
                continue
            traded_outcome = str(fill.metadata.get("outcome", "yes")).lower()
            outcome_value = 1.0 if terminal == SettlementState.RESOLVED_YES.value else 0.0
            if traded_outcome == "no":
                outcome_value = 1.0 - outcome_value
            probability_records.append({"probability": fill.expected_probability, "outcome": outcome_value})
        unresolved = tuple(sorted({position.market_id or position.symbol for position in portfolio.positions.values() if position.market_type is MarketType.PREDICTION and position.quantity}))
        metrics = calculate_prediction_metrics(
            curve,
            fills=portfolio.fills,
            probabilities=probability_records,
            initial_equity=portfolio.initial_cash,
        )
        if mode is PredictionResearchMode.PRICE_PROXY_RESEARCH:
            resolved_quality = ResearchQuality.PRICE_PROXY
        elif mode is PredictionResearchMode.RECORDED_BOOK_REPLAY:
            resolved_quality = ResearchQuality.ORDER_BOOK_SIMULATED
        elif research_quality is None:
            has_order_book = bool(rows) and all(
                _coerce_book(_raw_book(row, "yes"), _time(row)) is not None
                or _coerce_book(_raw_book(row, "no"), _time(row)) is not None
                for row in rows
            )
            resolved_quality = ResearchQuality.ORDER_BOOK_SIMULATED if has_order_book else ResearchQuality.PRICE_PROXY
        else:
            resolved_quality = research_quality if isinstance(research_quality, ResearchQuality) else ResearchQuality(str(research_quality))
        return BacktestResult(
            tuple(curve),
            tuple(portfolio.fills),
            _quality(rows),
            metrics,
            unresolved,
            outcomes,
            tuple(labels),
            resolved_quality,
        )
    def run(
        self,
        snapshots: Sequence[PredictionMarketSnapshot | Mapping[str, Any]],
        strategy: StrategyDefinition | Mapping[str, Any] | str,
        *,
        resolutions: Mapping[str, ResolvedContract] | Sequence[ResolvedContract] | None = None,
        initial_cash: float | None = None,
        research_quality: ResearchQuality | str | None = None,
        model: Any | None = None,
        model_document: Any | None = None,
        mode: PredictionResearchMode | str | None = None,
        research_mode: PredictionResearchMode | str | None = None,
        holding_period: int = 1,
        observation_horizon: Mapping[str, Any] | int | None = None,
        exit_policy: str | Mapping[str, Any] = "fixed_holding_period",
    ) -> BacktestResult:
        """Evaluate using one explicit evidence mode and the canonical evaluator."""
        if mode is not None and research_mode is not None and _mode(mode) is not _mode(research_mode):
            raise ValueError("mode and research_mode disagree")
        resolved_mode = _mode(mode if mode is not None else research_mode)
        return self._run_legacy(
            snapshots,
            strategy,
            resolutions=resolutions,
            initial_cash=initial_cash,
            research_quality=research_quality,
            model=model,
            model_document=model_document,
            mode=resolved_mode,
            holding_period=holding_period,
            observation_horizon=observation_horizon,
            exit_policy=exit_policy,
        )

    simulate = run


PredictionMarketHistoricalSimulator = PredictionMarketBacktester
PredictionBacktester = PredictionMarketBacktester




def run_prediction_research_mode(
    rows: Sequence[PredictionMarketSnapshot | Mapping[str, Any]],
    strategy: StrategyDefinition | Mapping[str, Any] | str,
    *,
    mode: PredictionResearchMode | str,
    initial_cash: float = 10_000.0,
    fee_bps: float = 10.0,
    slippage_bps: float = 5.0,
    allocation: float = 0.25,
    resolutions: Mapping[str, ResolvedContract] | Sequence[ResolvedContract] | None = None,
    model: Any | None = None,
    model_document: Any | None = None,
    holding_period: int = 1,
    observation_horizon: Mapping[str, Any] | int | None = None,
    exit_policy: str | Mapping[str, Any] = "fixed_holding_period",
) -> BacktestResult:
    """Stable orchestration entry point used by autonomous research."""
    resolved = _mode(mode)
    assert resolved is not None
    return PredictionMarketBacktester(
        initial_cash=initial_cash,
        fee_bps=fee_bps,
        slippage_bps=slippage_bps,
        allocation=allocation,
    ).run(
        rows,
        strategy,
        mode=resolved,
        resolutions=resolutions,
        model=model,
        model_document=model_document,
        observation_horizon=observation_horizon,
        exit_policy=exit_policy,
    )
__all__ = [
    "CANONICAL_EVALUATOR_VERSION",
    "PRICE_PROXY_ASSUMPTIONS_VERSION",
    "RECORDED_BOOK_REPLAY_ASSUMPTIONS_VERSION",
    "PRICE_PROXY_RESEARCH",
    "RECORDED_BOOK_REPLAY",
    "PredictionResearchMode",
    "PredictionBacktester",
    "PredictionMarketBacktester",
    "PredictionMarketHistoricalSimulator",
    "run_prediction_research_mode",
]
