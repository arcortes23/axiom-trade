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
from itertools import islice

from typing import Any, Iterable, Mapping, Sequence
from .domain import (
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
from .forward import ForwardTestSpec, _content_hash, _normalized_strategy_document
from .paper import PaperTrader, PaperTradingConfig
from .portfolio import Portfolio
from .risk import RiskEngine, RiskLimits
from .storage import AxiomStore

_MAX_RUN_OBSERVATIONS = 100_000


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

    def as_record(self) -> dict[str, Any]:
        return {
            "started_at": self.started_at.isoformat(),
            "ended_at": self.ended_at.isoformat(),
            "duration_seconds": max(0.0, (self.ended_at - self.started_at).total_seconds()),
            "observations_seen": self.observations_seen,
            "observations_processed": self.observations_processed,
            "observations_skipped": self.observations_skipped,
            "fills_inserted": self.fills_inserted,
            "settlements": self.settlements,
            "errors": list(self.errors),
        }


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
        self.config = config or PaperTradingConfig()
        self._run_id = str(storage_namespace or spec.experiment_id).strip()
        if not self._run_id:
            raise ValueError("storage_namespace must be non-empty")
        self._execution_mode = str(execution_mode or ("forward" if storage_namespace is None else "isolated")).strip().lower()
        if self._execution_mode not in {"forward", "historical_replay", "isolated"}:
            raise ValueError("execution_mode is invalid")
        self._execution_strategy_id = spec.strategy_hash
        self._execution_binding = {
            "experiment_id": self._run_id,
            "spec_experiment_id": spec.experiment_id,
            "execution_mode": self._execution_mode,
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
                        "fee_rate": self.config.fee_rate,
                        "slippage_bps": self.config.slippage_bps,
                        "depth": self.config.depth,
                        "quality": self.config.quality,
                        "live": self.config.live,
                    }
                ).encode("utf-8")
            ).hexdigest(),
        }
        loaded_state = store.load_paper_state(self._run_id)
        self._state_version = int(loaded_state.get("state_version", 0)) if loaded_state is not None else -1
        raw_state = loaded_state.get("state", {}) if loaded_state is not None else {}
        self._state = dict(raw_state) if isinstance(raw_state, Mapping) else {}
        persisted_binding = self._state.get("execution_binding")
        if persisted_binding is not None and dict(persisted_binding) != self._execution_binding:
            raise ValueError("paper state execution binding does not match the frozen forward test")
        self._processed: set[str] = set(str(item) for item in self._state.get("processed_observations", ()))
        self._cursor: dict[str, datetime] = {
            str(key): parsed
            for key, value in dict(self._state.get("cursor_by_market", {})).items()
            if (parsed := parse_timestamp(value)) is not None
        }
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
        self.portfolio = portfolio or Portfolio(spec.bankroll)
        if risk is None:
            self.risk = RiskEngine(RiskLimits(**dict(spec.risk_limits)), initial_equity=spec.bankroll)
        else:
            self.risk = risk
        self._restore_risk_status()
        strategy_document = _normalized_strategy_document(getattr(strategy, "definition", strategy))
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
        try:
            restored_sequence = int(self._state.get("order_sequence", 0))
        except (TypeError, ValueError):
            restored_sequence = 0
        if restored_sequence < 0:
            raise ValueError("paper state order sequence is invalid")
        self.trader._sequence = restored_sequence
        self._warm_strategy_state()

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
    def _append_signal_history(self, market_id: str, observation: Mapping[str, Any]) -> None:
        history = self._signal_history.setdefault(str(market_id), [])
        history.append(dict(observation))
        if len(history) > 512:
            del history[:-512]

    def _warm_strategy_state(self) -> None:
        """Replay persisted signal inputs into stateful strategies without execution."""
        try:
            rows = self.store.list_latest_paper_observations(self._run_id, per_market_limit=512)
        except Exception:
            return
        rows.sort(key=lambda item: (_observation_timestamp(item.get("payload")) or datetime.fromtimestamp(0, tz=self.spec.registration_timestamp.tzinfo), str(item.get("market_id", ""))))
        rebuilt: dict[str, list[Any]] = {}
        for item in rows:
            payload = item.get("payload")
            if not isinstance(payload, Mapping):
                continue
            market_id = str(item.get("market_id", payload.get("market_id", ""))).strip()
            if not market_id:
                continue
            observation = dict(payload)
            if not self._model_state_restored:
                model_state_before = _snapshot_object_state(self.model)
                try:
                    model_probability = _model_probability(self.model, observation)
                except Exception:
                    _restore_object_state(self.model, model_state_before)
                    model_probability = None
                if model_probability is not None:
                    observation["model_probability"] = model_probability
            history = rebuilt.setdefault(market_id, [])
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
            normalized = _normalize_observation(raw)
            if normalized is None:
                skipped += 1
                errors.append("malformed prediction observation")
                continue
            market_id, observation, yes_book, no_book = normalized
            if self.spec.allowed_markets and market_id not in self.spec.allowed_markets:
                skipped += 1
                continue
            available_at = parse_timestamp(observation.get("available_at"))
            if available_at is not None and available_at > started:
                skipped += 1
                errors.append(f"observation unavailable until {available_at.isoformat()}")
                continue
            if any(book is not None and ensure_utc(book.timestamp) > stamp for book in (yes_book, no_book)):
                skipped += 1
                errors.append(f"future order book for {market_id}")
                continue
            cursor = self._cursor.get(market_id)
            terminal = _is_terminal(observation.get("settlement"))
            if cursor is not None and stamp < cursor:
                skipped += 1
                continue
            if market_id in self._settled:
                current_settlement = _settlement_value(observation.get("settlement"))
                previous_settlement = _settlement_value(self._settlement_by_market.get(market_id))
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
            model_probability = None
            if not terminal:
                try:
                    model_probability = _model_probability(self.model, observation)
                except Exception as exc:
                    _restore_object_state(self.model, model_state_before)
                    skipped += 1
                    errors.append(f"model error for {market_id}: {exc}")
                    continue
            if model_probability is not None:
                observation["model_probability"] = model_probability
            price = _reference_price(observation, yes_book, no_book)
            if price is None and not terminal:
                _restore_object_state(self.model, model_state_before)
                skipped += 1
                errors.append(f"missing executable quote for {market_id}")
                continue
            if price is None:
                price = 0.5
            previous_cursor = self._cursor.get(market_id)
            previous_source_cursor = self._source_cursor.get(market_id)
            was_settled = market_id in self._settled
            previous_settlement = self._settlement_by_market.get(market_id)
            fill = None
            fill_saved = False
            settlement_saved = False
            inserted_observation = False
            history_before = tuple(self._signal_history.get(market_id, ()))
            portfolio_before = deepcopy(self.portfolio)
            risk_before = deepcopy(self.risk)
            state_before = deepcopy(self._state)
            trader_fills_before = list(self.trader._fills)
            signal_history_before = list(history_before)
            state_version_before = self._state_version
            trader_sequence_before = self.trader._sequence
            strategy_state_before = _snapshot_object_state(self.strategy)
            try:
                with self.store.transaction():
                    inserted_observation = self.store.save_paper_observation(
                        observation_id,
                        self._run_id,
                        market_id,
                        stamp,
                        observation,
                    )
                    if inserted_observation:
                        fill = self.trader._run_observation(
                            symbol=market_id,
                            observation=observation,
                            book=yes_book,
                            no_book=no_book,
                            timestamp=stamp,
                            reference=price,
                            market_id=market_id,
                            signal_history=history_before,
                        )
                        if fill is not None:
                            fill = self._bind_fill(fill)
                            if self.store.save_fill(fill, fill_id="paper-fill-" + self._run_id + "-" + fill.order_id):
                                fill_saved = True
                        if terminal:
                            settlement_saved = True
                            self._settled.add(market_id)
                            self._settlement_by_market[market_id] = str(observation.get("settlement"))
                            if self.risk is not None:
                                self.risk.reconcile_market(market_id, fills=self.portfolio.fills)
                        self._cursor[market_id] = max(self._cursor.get(market_id, stamp), stamp)
                        source_snapshot_id = str(observation.get("source_snapshot_id", "")).strip()
                        if source_snapshot_id:
                            source_cursor = (source_timestamp, source_snapshot_id)
                            if previous_source_cursor is None or source_cursor > previous_source_cursor:
                                self._source_cursor[market_id] = source_cursor
                        self._processed.add(observation_id)
                        self._append_signal_history(market_id, observation)
                        self._persist_state(last_timestamp=stamp)
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
                self._state = state_before
                self._processed.discard(observation_id)
                if previous_cursor is None:
                    self._cursor.pop(market_id, None)
                else:
                    self._cursor[market_id] = previous_cursor
                if previous_source_cursor is None:
                    self._source_cursor.pop(market_id, None)
                else:
                    self._source_cursor[market_id] = previous_source_cursor
                if not was_settled:
                    self._settled.discard(market_id)
                if previous_settlement is None:
                    self._settlement_by_market.pop(market_id, None)
                else:
                    self._settlement_by_market[market_id] = previous_settlement
                if signal_history_before:
                    self._signal_history[market_id] = signal_history_before
                else:
                    self._signal_history.pop(market_id, None)
                errors.append(f"{market_id}: {exc}")
                continue
            if not inserted_observation:
                skipped += 1
                continue
            if fill_saved:
                fills_inserted += 1
            if settlement_saved:
                settlements += 1
            processed += 1
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


    def _restore_ledger(self) -> None:
        try:
            fills = self.store.load_fills(strategy_id=self._execution_strategy_id)
        except Exception:
            fills = []
        fills = [
            fill
            for fill in fills
            if str(fill.metadata.get("paper_experiment_id", "")) == self._run_id
        ]
        existing = {fill.order_id for fill in getattr(self.portfolio, "fills", ())}
        for fill in fills:
            if fill.order_id not in existing:
                try:
                    self.portfolio.apply_fill(fill)
                except (TypeError, ValueError):
                    continue
    def _restore_settlements(self) -> None:
        if not self._settlement_by_market:
            try:
                observations = self.store.list_latest_paper_observations(self._run_id, per_market_limit=1)
            except Exception:
                observations = []
            for item in observations:
                payload = item.get("payload", {})
                if isinstance(payload, Mapping) and _is_terminal(payload.get("settlement")):
                    self._settlement_by_market[str(item["market_id"])] = str(payload["settlement"])
        for market_id, raw_state in self._settlement_by_market.items():
            try:
                state = raw_state if isinstance(raw_state, SettlementState) else SettlementState(str(raw_state).strip().lower())
                if not _is_terminal(state):
                    continue
                self.portfolio.resolve(
                    ResolvedContract(
                        market_id,
                        state,
                        self.spec.registration_timestamp,
                        "persisted paper settlement",
                    )
                )
                self._settled.add(market_id)
            except (TypeError, ValueError):
                continue
        if self.risk is not None:
            self.risk.reconcile_fills(
                fill
                for fill in self.portfolio.fills
                if str(fill.market_id or fill.symbol) not in self._settled
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
        self._state.update(
            {
                "experiment_id": self._run_id,
                "registration_timestamp": self.spec.registration_timestamp.isoformat(),
                "execution_binding": dict(self._execution_binding),
                "execution_strategy_id": self._execution_strategy_id,
                "processed_observations": sorted(self._processed),
                "cursor_by_market": {key: value.isoformat() for key, value in sorted(self._cursor.items())},
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
                "portfolio": self.portfolio.snapshot(),
                "risk": risk_state,
                "order_sequence": self.trader._sequence,
                "fill_count": len(self.portfolio.fills),
                "paper_only": True,
                "live_execution": False,
            }
        )
        self._state_version = self.store.save_paper_state(
            self._run_id,
            self._state,
            timestamp=last_timestamp or utc_now(),
            expected_version=self._state_version,
        )


def _normalize_observation(raw: Any) -> tuple[str, dict[str, Any], OrderBookSnapshot | None, OrderBookSnapshot | None] | None:
    if isinstance(raw, PredictionMarketSnapshot):
        observation = dict(to_record(raw))
        return raw.market_id, observation, raw.order_book, None
    if not isinstance(raw, Mapping):
        return None
    nested = raw.get("snapshot") if isinstance(raw.get("snapshot"), Mapping) else raw
    market_id = str(nested.get("market_id", raw.get("market_id", ""))).strip()
    stamp = parse_timestamp(nested.get("timestamp", raw.get("observed_at")))
    if not market_id or stamp is None:
        return None
    observation = dict(nested)
    observation["timestamp"] = stamp
    for key in (
        "settlement",
        "expiry",
        "resolution_criteria",
        "liquidity",
        "volume",
        "model_probability",
        "predicted_probability",
        "source_snapshot_id",
        "source_timestamp",
        "available_at",
    ):
        if key not in observation and key in raw:
            observation[key] = raw[key]
    yes_book = _book(raw.get("yes_order_book", raw.get("order_book")), stamp)
    no_book = _book(raw.get("no_order_book"), stamp)
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
        return OrderBookSnapshot(parse_timestamp(raw.get("timestamp")) or fallback_timestamp, bids, asks, raw.get("token_id"))
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
        return parse_timestamp(nested.get("timestamp", value.get("observed_at")))
    return parse_timestamp(getattr(value, "timestamp", None))
def _observation_sort_key(value: Any, tzinfo: Any) -> tuple[datetime, str, str]:
    stamp = _observation_timestamp(value) or datetime.fromtimestamp(0, tz=tzinfo)
    if isinstance(value, Mapping):
        nested = value.get("snapshot") if isinstance(value.get("snapshot"), Mapping) else value
        market = nested.get("market_id", value.get("market_id", "")) if isinstance(nested, Mapping) else ""
    else:
        market = getattr(value, "market_id", "")
    return stamp, str(market), _canonical_json(value)


def _model_probability(model: Any | None, observation: Mapping[str, Any]) -> float | None:
    if model is None:
        return None
    value: Any = None
    if isinstance(model, Mapping):
        if "probability" in model:
            value = model["probability"]
        elif "yes_probability" in model:
            value = model["yes_probability"]
        else:
            field = model.get("field")
            if isinstance(field, str) and field.strip():
                value = observation.get(field)
    else:
        methods = ("predict_probability", "probability", "predict", "estimate")
        for name in methods:
            method = getattr(model, name, None)
            if not callable(method):
                continue
            try:
                value = method(observation)
            except (TypeError, AttributeError):
                continue
            break
        if value is None and callable(model):
            try:
                value = model(observation)
            except (TypeError, AttributeError):
                return None
    if isinstance(value, Mapping):
        value = value.get("probability", value.get("yes_probability", value.get("prediction")))
    try:
        probability = float(value)
    except (TypeError, ValueError):
        return None
    return probability if math.isfinite(probability) and 0 <= probability <= 1 else None


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


__all__ = ["ForwardPaperEngine", "PaperEngineCycle", "historical_replay_id", "run_forward_paper", "run_historical_replay"]
