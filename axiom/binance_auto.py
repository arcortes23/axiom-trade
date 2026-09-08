"""Bounded autonomous Binance Spot orchestration.

This module is intentionally a small coordinator.  It does not own a venue,
refresh research, or call any other market family.  All dependencies are
injected (and are used through duck typing) so a worker can be restarted over
an existing execution ledger without importing an execution implementation.
"""
from __future__ import annotations

from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import inspect
import json
import os
import threading
import time
import uuid
from typing import Any, Callable, Iterable, Mapping, Sequence

from .binance_execution import ARMED, DISABLED, DISARMED, KILLED, PAUSED
from .binance_market import BoundedBinanceMarketCollector, BinanceMarketSnapshot
from .binance_risk import SymbolRules
from .binance_signals import BinanceSignalEngine
from .crypto_universe import UniverseSnapshot, load_crypto_universe
from .domain import ensure_utc, utc_now
from .storage import AxiomStore
from .strategy import validate_strategy



class _DeadlineExpired(Exception):
    """Internal cooperative stop used when AUTO's hard deadline is reached."""


AUTO_DEADLINE_REASON = "AUTO_DEADLINE_EXPIRED"


def _canonical_value(value: Any) -> Any:
    """Convert a value to the unredacted JSON domain used for content hashes."""
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, datetime):
        return ensure_utc(value).isoformat()
    if isinstance(value, Mapping):
        return {str(key): _canonical_value(child) for key, child in sorted(value.items(), key=lambda item: str(item[0]))}
    if isinstance(value, (list, tuple)):
        return [_canonical_value(child) for child in value]
    if isinstance(value, (set, frozenset)):
        return sorted((_canonical_value(child) for child in value), key=lambda child: repr(child))
    if hasattr(value, "value") and not isinstance(value, (str, bytes)):
        return _canonical_value(value.value)
    if hasattr(value, "as_dict") and callable(value.as_dict):
        return _canonical_value(value.as_dict())
    if hasattr(value, "to_dict") and callable(value.to_dict):
        return _canonical_value(value.to_dict())
    if is_dataclass(value):
        return _canonical_value(asdict(value))
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return value


def _canonical_json(value: Any) -> str:
    return json.dumps(_canonical_value(value), sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def _strategy_digest(value: Any) -> str:
    """Return the frozen strategy content hash used by research.

    This intentionally does not use ``_safe``: metadata whose key resembles a
    credential is still immutable strategy evidence and must affect its hash.
    """
    return "sha256:" + hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()
UTC = timezone.utc
ZERO = Decimal("0")

_SECRET_WORDS = (
    "secret", "password", "token", "credential", "api_key", "apikey",
    "authorization", "private_key", "passphrase", "keyring",
)


def _dec(value: Any, default: Decimal = ZERO) -> Decimal:
    if value is None or value == "":
        return default
    if isinstance(value, bool):
        return default
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return default
    return result if result.is_finite() else default


def _symbol(value: Any) -> str:
    return str(value or "").replace("/", "").replace("-", "").replace("_", "").strip().upper()


def _iso(value: Any) -> str:
    candidate = value if isinstance(value, datetime) else datetime.now(UTC)
    return ensure_utc(candidate).isoformat()


def _value(value: Any, *names: str, default: Any = None) -> Any:
    if isinstance(value, Mapping):
        for name in names:
            if name in value:
                return value[name]
        return default
    for name in names:
        if hasattr(value, name):
            return getattr(value, name)
    return default


def _safe(value: Any, *, depth: int = 0) -> Any:
    """Bound and redact payloads written to the autonomous namespace."""
    if depth > 7:
        return "<truncated>"
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, datetime):
        return ensure_utc(value).isoformat()
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for key, child in list(value.items())[:256]:
            name = str(key)
            lowered = name.lower().replace("-", "_")
            if any(word in lowered for word in _SECRET_WORDS):
                continue
            result[name] = _safe(child, depth=depth + 1)
        return result
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_safe(child, depth=depth + 1) for child in list(value)[:256]]
    if hasattr(value, "as_dict") and callable(value.as_dict):
        try:
            return _safe(value.as_dict(), depth=depth + 1)
        except Exception:
            pass
    if hasattr(value, "to_dict") and callable(value.to_dict):
        try:
            return _safe(value.to_dict(), depth=depth + 1)
        except Exception:
            pass
    if is_dataclass(value):
        try:
            return _safe(asdict(value), depth=depth + 1)
        except Exception:
            pass
    return str(value)


def _json(value: Any) -> str:
    return json.dumps(_safe(value), sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def _call(method: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    """Call a fake or production dependency without requiring one signature."""
    try:
        signature = inspect.signature(method)
    except (TypeError, ValueError):
        signature = None
    if signature is not None:
        params = signature.parameters
        if not any(param.kind == inspect.Parameter.VAR_KEYWORD for param in params.values()):
            kwargs = {key: value for key, value in kwargs.items() if key in params}
    return method(*args, **kwargs)


def _as_mapping(value: Any) -> Mapping[str, Any] | None:
    if isinstance(value, Mapping):
        return value
    if hasattr(value, "as_dict") and callable(value.as_dict):
        try:
            result = value.as_dict()
            return result if isinstance(result, Mapping) else None
        except Exception:
            return None
    if hasattr(value, "to_dict") and callable(value.to_dict):
        try:
            result = value.to_dict()
            return result if isinstance(result, Mapping) else None
        except Exception:
            return None
    return None


def _quantity(position: Any) -> Decimal:
    values = [
        _value(
            position,
            "quantity",
            "qty",
            "position_quantity",
            "base_quantity",
            "net_quantity",
            default=None,
        ),
        _value(position, "owned_quantity", "owned_qty", default=None),
    ]
    if not any(value is not None for value in values) and not isinstance(position, (Mapping, list, tuple, set, frozenset)):
        values.append(position)
    for value in values:
        quantity = _dec(value)
        if quantity > ZERO:
            return quantity
    return ZERO



def _flag(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() not in {"", "0", "false", "no", "n", "none", "null"}
    return bool(value)


def _axiom_owned(position: Any) -> bool:
    foreign = _value(
        position,
        "foreign",
        "is_foreign",
        "foreign_position",
        "external",
        "is_external",
        "non_axiom",
        "not_axiom_owned",
        default=None,
    )
    if foreign is not None and _flag(foreign):
        return False
    marker = _value(position, "axiom_owned", "is_axiom_owned", "owned_by_axiom", "owned", default=None)
    if marker is not None:
        return _flag(marker)
    owner = _value(position, "owner", "owner_name", "ownership", "source", default=None)
    if owner is None:
        # ``execution.positions()`` is an AXIOM ledger projection by contract.
        return True
    if isinstance(owner, Mapping):
        owner = _value(owner, "name", "id", "owner", "system", default=None)
    owner_text = str(owner or "").strip().upper().replace("-", "_").replace(" ", "_")
    if "AXIOM" in owner_text:
        return True
    # Venue/source labels (for example ``BINANCE``) do not establish foreign
    # ownership.  Only an explicit foreign marker opts a positive position out.
    return not (
        owner_text in {"FOREIGN", "EXTERNAL", "OTHER", "NON_AXIOM", "NOT_AXIOM"}
        or "FOREIGN" in owner_text
        or "EXTERNAL" in owner_text
        or "NON_AXIOM" in owner_text
        or "NOT_AXIOM" in owner_text
    )



def _score(value: Any) -> Decimal:
    return _dec(value)


class BinanceAutonomousWorker:
    """Run one serialized, bounded Binance Spot decision cycle.

    The worker is deliberately conservative: it reconciles before looking at
    entries, never enables execution, and treats missing or malformed evidence
    as a no-trade condition.  It accepts either real services or tiny fakes;
    no venue or network object is constructed here.
    """

    namespace = "BINANCE_SPOT_AUTONOMOUS"
    schema_version = "binance-auto-v1"
    worker_name = "binance-auto"

    def __init__(
        self,
        store: AxiomStore,
        execution: Any | None = None,
        *,
        collector: Any | None = None,
        provider: Any | None = None,
        universe: UniverseSnapshot | Mapping[str, Any] | Any | None = None,
        universe_snapshot: UniverseSnapshot | Mapping[str, Any] | None = None,
        universe_loader: Callable[..., Any] | None = None,
        universe_builder: Any | None = None,
        qualification: Any | None = None,
        qualification_service: Any | None = None,
        signal_engine_factory: Callable[..., Any] | None = None,
        signal_engine: Any | None = None,
        strategy: Any | None = None,
        interval_seconds: float = 60.0,
        interval: str = "1d",
        limit: int = 1000,
        max_actionable: int = 3,
        max_candidates: int | None = None,
        max_entries_per_cycle: int = 1,
        fill_quantity: Any = Decimal("1"),
        fee_rate: Any = Decimal("0"),
        time_in_force: str = "IOC",
        depth: int = 20,
        collector_kwargs: Mapping[str, Any] | None = None,
        clock: Callable[[], Any] = utc_now,
        sleeper: Callable[[float], Any] | None = None,
        stop_event: threading.Event | None = None,
        worker_id: str | None = None,
        profile: Any | None = None,
    ) -> None:
        if not isinstance(store, AxiomStore):
            raise TypeError("store must be AxiomStore")
        self.store = store
        self.execution = execution
        self.clock = clock
        self.sleeper = sleeper or (lambda seconds: self._stop_event.wait(seconds))
        self._stop_event = stop_event or threading.Event()
        self._decision_lock = threading.Lock()
        self.worker_id = str(worker_id or self.worker_name)
        self.interval_seconds = float(interval_seconds)
        if self.interval_seconds < 0 or self.interval_seconds != self.interval_seconds or self.interval_seconds == float("inf"):
            raise ValueError("interval_seconds must be finite and non-negative")
        self.interval = str(interval).strip() or "1d"
        if isinstance(limit, bool) or int(limit) <= 0:
            raise ValueError("limit must be positive")
        self.limit = int(limit)
        self.max_actionable = max(1, int(max_candidates if max_candidates is not None else max_actionable))
        self.max_entries_per_cycle = max(0, int(max_entries_per_cycle))
        self.fill_quantity = _dec(fill_quantity, Decimal("1"))
        if self.fill_quantity <= ZERO:
            raise ValueError("fill_quantity must be positive")
        self.fee_rate = _dec(fee_rate)
        if self.fee_rate < ZERO:
            raise ValueError("fee_rate must be non-negative")
        self.time_in_force = str(time_in_force or "IOC")
        self.depth = max(1, int(depth))
        self.profile = profile
        self.strategy = strategy
        self.qualification = qualification or qualification_service
        self.signal_engine_factory = signal_engine_factory
        self.signal_engine = signal_engine
        self._universe_loader = universe_loader
        self._universe_source = universe_snapshot if universe_snapshot is not None else (universe_builder if universe_builder is not None else universe)
        self._collector = collector
        self._provider = provider
        self._collector_kwargs = dict(collector_kwargs or {})
        self._cycle_number = 0
        self._last_status: dict[str, Any] = {"status": "IDLE", "worker_name": self.worker_id}
        self._event_sink: list[dict[str, Any]] | None = None
        self._init_schema()
        self._load_restart_state()

    def _profile_environment(self) -> str:
        value = _value(self.profile, "environment", default=None)
        if value is None:
            value = _value(self.execution, "environment", default=None)
        if hasattr(value, "value"):
            value = value.value
        return str(value or "PAPER").strip().upper()
    @staticmethod
    def _declared_environment(source: Any) -> str | None:
        value = _value(source, "environment", default=None)
        if value is None:
            return None
        if hasattr(value, "value"):
            value = value.value
        normalized = str(value or "").strip().upper()
        return normalized or None

    @staticmethod
    def _is_testnet_environment(value: Any) -> bool:
        if hasattr(value, "value"):
            value = value.value
        return str(value or "").strip().upper() in {"TESTNET", "BINANCE_SPOT_TESTNET"}

    def _worker_environments(self) -> tuple[str, ...]:
        """Return every explicitly configured worker/execution environment."""
        values = (
            self._declared_environment(self.profile),
            self._declared_environment(self.execution),
        )
        return tuple(value for value in values if value is not None)
    @staticmethod
    def _environment_values(sources: Sequence[Mapping[str, Any]]) -> tuple[Any, ...]:
        values: list[Any] = []
        for source in sources:
            for name in ("environment", "execution_environment"):
                value = source.get(name)
                if value not in (None, ""):
                    values.append(value)
        return tuple(values)


    def _paper_static_candidate(
        self,
        row: Mapping[str, Any],
        lifecycle: Mapping[str, Any] | None,
    ) -> bool:
        worker_environments = self._worker_environments()
        if not worker_environments or any(value != "PAPER" for value in worker_environments):
            return False
        sources: tuple[Any, ...] = (row,)
        if isinstance(lifecycle, Mapping):
            sources += (lifecycle.get("payload"),)
        environments = {
            str(getattr(value, "value", value))
            for value in self._environment_values(self._mapping_sources(*sources))
            if value not in (None, "")
        }
        # A PAPER worker may use its injected binding only when all persisted
        # environment evidence agrees exactly.  This prevents a Testnet
        # candidate hidden behind a PAPER wrapper from bypassing hydration.
        return bool(environments) and environments == {"PAPER"}


    @staticmethod
    def _store_identity(store: Any) -> str | None:
        """Return the canonical SQLite identity, not the Python store identity."""
        if store is None:
            return None
        connection = getattr(store, "connection", None)
        if connection is None:
            connection = getattr(store, "_conn", None)
        filename = ""
        if connection is not None:
            try:
                row = connection.execute("PRAGMA database_list").fetchone()
                if row is not None:
                    filename = str(row[2] or "")
            except Exception:
                filename = ""
        path = filename or str(getattr(store, "path", "") or "")
        if not path or path == ":memory:":
            return f"memory:{id(connection)}" if connection is not None else None
        if path.startswith("file:"):
            path = path[5:].split("?", 1)[0]
        try:
            return os.path.normcase(os.path.realpath(os.path.abspath(path)))
        except (OSError, ValueError):
            return os.path.normcase(path)

    def supports_persisted_strategy(self) -> bool:
        """Return whether the complete persisted hydration path is usable."""
        qualification_store = _value(self.qualification, "store", default=None)
        if self.qualification is None or qualification_store is None:
            return False
        if self._store_identity(qualification_store) != self._store_identity(self.store):
            return False
        rank_usable = callable(getattr(self.qualification, "rank_and_select", None)) or (
            callable(getattr(self.qualification, "qualify_all", None))
            and callable(getattr(self.qualification, "actionable_rankings", None))
        )
        return rank_usable and callable(getattr(self.store, "load_candidate_lifecycle", None)) and callable(
            getattr(self.store, "load_strategy", None)
        )

    @staticmethod
    def _deadline_expired(deadline_monotonic: float | None) -> bool:
        if deadline_monotonic is None:
            return False
        try:
            return time.monotonic() >= float(deadline_monotonic)
        except (TypeError, ValueError, OverflowError):
            return True
    @staticmethod
    def _is_deadline_exception(exc: BaseException) -> bool:
        return bool(
            isinstance(exc, _DeadlineExpired)
            or getattr(exc, "deadline_expired", False)
            or str(exc).upper() == AUTO_DEADLINE_REASON
            or AUTO_DEADLINE_REASON in str(exc).upper()
        )

    def _checkpoint(self, deadline_monotonic: float | None) -> None:
        if self._deadline_expired(deadline_monotonic):
            raise _DeadlineExpired(AUTO_DEADLINE_REASON)

    @staticmethod
    def _mapping_sources(*values: Any) -> tuple[Mapping[str, Any], ...]:
        result: list[Mapping[str, Any]] = []
        pending = list(values)
        seen: set[int] = set()
        while pending:
            value = pending.pop(0)
            mapped = _as_mapping(value)
            if mapped is None or id(mapped) in seen:
                continue
            seen.add(id(mapped))
            result.append(mapped)
            for key in (
                "crypto_provenance", "dataset_provenance", "provenance", "origin", "originating",
                "originating_provenance", "originating_binding", "entry_binding", "source_binding",
                "source", "strategy_ref", "binding", "selection", "frozen", "forward_config",
                "config", "lifecycle", "payload",
            ):
                child = mapped.get(key)
                if isinstance(child, Mapping):
                    pending.append(child)
        return tuple(result)

    @staticmethod
    def _source_values(sources: Sequence[Mapping[str, Any]], *names: str) -> list[Any]:
        values: list[Any] = []
        for source in sources:
            value = _value(source, *names, default=None)
            if value not in (None, ""):
                values.append(value)
        return values


    @classmethod
    def _consistent_source_value(cls, sources: Sequence[Mapping[str, Any]], *names: str, default: Any = None) -> Any:
        values = cls._source_values(sources, *names)
        if not values:
            return default
        normalized = {_canonical_json(value) for value in values}
        if len(normalized) != 1:
            raise ValueError("PERSISTED_EVIDENCE_MISMATCH")
        return values[0]

    def _strategy_from_document(self, document: Any, expected_hash: str | None = None) -> Any:
        definition = validate_strategy(document)
        if str(getattr(definition.market_type, "value", definition.market_type)).lower() != "crypto_spot":
            raise ValueError("strategy is not crypto_spot")
        normalized_digest = _strategy_digest(definition.to_dict())
        raw_document = document.to_dict() if hasattr(document, "to_dict") and callable(document.to_dict) else document
        raw_digest = _strategy_digest(raw_document)
        if expected_hash and str(expected_hash) not in {
            normalized_digest, normalized_digest.removeprefix("sha256:"),
            raw_digest, raw_digest.removeprefix("sha256:"),
        }:
            raise ValueError("strategy hash mismatch")
        return definition

    def _load_exact_strategy(
        self,
        lifecycle_payload: Mapping[str, Any],
        source_binding: Mapping[str, Any],
        *,
        deadline_monotonic: float | None = None,
    ) -> tuple[Any, dict[str, Any]]:
        sources = self._mapping_sources(lifecycle_payload, source_binding)
        refs = [value for value in self._source_values(sources, "strategy_ref") if isinstance(value, Mapping)]
        ref_sources = self._mapping_sources(*refs)
        all_sources = (*ref_sources, *sources)
        ids = {
            str(value).strip()
            for value in self._source_values(all_sources, "strategy_id", "id")
            if str(value).strip()
        }
        if len(ids) > 1:
            raise ValueError("STRATEGY_REFERENCE_MISMATCH")
        strategy_id = next(iter(ids), "")
        if not strategy_id:
            raise ValueError("STRATEGY_REFERENCE_MISSING")
        versions = {
            str(value).strip()
            for value in self._source_values(all_sources, "strategy_version", "version")
            if str(value).strip()
        }
        if len(versions) > 1:
            raise ValueError("STRATEGY_REFERENCE_MISMATCH")
        strategy_version = next(iter(versions), "1")
        raw_hashes = {
            str(value).strip()
            for value in self._source_values(all_sources, "strategy_hash", "hash")
            if str(value).strip()
        }
        normalized_hashes = {
            value if value.startswith("sha256:") else "sha256:" + value
            for value in raw_hashes
        }
        if len(normalized_hashes) > 1:
            raise ValueError("STRATEGY_HASH_MISMATCH")
        expected_hash = next(iter(normalized_hashes), "")
        if not strategy_id or not expected_hash:
            raise ValueError("STRATEGY_REFERENCE_MISSING")
        self._checkpoint(deadline_monotonic)
        loaded = self.store.load_strategy(strategy_id, strategy_version)
        self._checkpoint(deadline_monotonic)
        if loaded is None:
            raise ValueError("STRATEGY_NOT_FOUND")
        strategy = self._strategy_from_document(loaded, expected_hash)
        documents = [
            value for value in self._source_values(all_sources, "strategy_document", "strategy")
            if isinstance(value, Mapping)
        ]
        for document in documents:
            checked = self._strategy_from_document(document, expected_hash)
            if _strategy_digest(checked.to_dict()) != _strategy_digest(strategy.to_dict()):
                raise ValueError("STRATEGY_EVIDENCE_MISMATCH")
        return strategy, {
            "strategy_id": strategy_id,
            "strategy_version": strategy_version,
            "strategy_hash": expected_hash,
        }
    def _project_successor(
        self,
        source_binding: Mapping[str, Any],
        *,
        candidate_id: str,
        symbol: str,
        deadline_monotonic: float | None = None,
    ) -> tuple[dict[str, Any], str]:
        from .binance_research import project_testnet_execution_binding

        self._checkpoint(deadline_monotonic)
        successor, source_hash = project_testnet_execution_binding(source_binding)
        self._checkpoint(deadline_monotonic)
        successor_map = _as_mapping(successor)
        if successor_map is None:
            raise ValueError("TESTNET_BINDING_INVALID")
        result = dict(successor_map)
        if str(result.get("candidate_id", "")) != candidate_id or _symbol(result.get("symbol")) != symbol:
            raise ValueError("TESTNET_BINDING_IDENTITY_MISMATCH")
        result["binding_hash"] = str(getattr(successor, "binding_hash", "") or "")
        if not result["binding_hash"]:
            raise ValueError("TESTNET_BINDING_HASH_MISSING")
        return result, str(source_hash or "")

    @staticmethod
    def _candidate_row_hashes(row: Mapping[str, Any]) -> tuple[Any, ...]:
        """Return only hashes explicitly published by the ranking row."""
        values = [row.get("binding_hash"), row.get("source_binding_hash")]
        binding = row.get("binding")
        if isinstance(binding, Mapping):
            values.extend((binding.get("binding_hash"), binding.get("source_binding_hash")))
        return tuple(value for value in values if value not in (None, ""))

    @staticmethod
    def _strict_candidate_row(row: Mapping[str, Any]) -> bool:
        return any(
            key in row
            for key in ("binding", "binding_hash", "source_binding_hash", "strategy_hash", "strategy_ref",
                        "qualification_hash", "immutable_hashes", "crypto_provenance", "provenance")
        )
    def _hydrate_candidate(
        self,
        row: Mapping[str, Any],
        deadline_monotonic: float | None = None,
    ) -> tuple[dict[str, Any], Any, dict[str, Any]]:
        """Load the frozen candidate and project one verified execution successor.

        Ranking rows are wrappers.  A wrapper may be used directly only for
        explicitly injected PAPER/static workers; once it carries persisted
        evidence, every field is checked against the durable FROZEN record.
        """
        candidate_id = str(row.get("candidate_id") or "").strip()
        symbol = _symbol(row.get("symbol"))
        if not candidate_id or not symbol:
            raise ValueError("CANDIDATE_IDENTITY_MISSING")
        row_sources = self._mapping_sources(row)
        row_hashes = self._candidate_row_hashes(row)

        persisted_store_usable = self.supports_persisted_strategy()
        self._checkpoint(deadline_monotonic)
        lifecycle = self.store.load_candidate_lifecycle(candidate_id) if callable(
            getattr(self.store, "load_candidate_lifecycle", None)
        ) else None
        self._checkpoint(deadline_monotonic)
        if isinstance(lifecycle, Mapping) and not persisted_store_usable and not row_hashes:
            raise ValueError("PERSISTED_EVIDENCE_STORE_MISMATCH")

        # A caller that explicitly runs PAPER with an injected execution
        # binding must retain the legacy source binding.  Do not let merely
        # sharing the canonical database promote that row into a Testnet
        # successor; explicit Testnet workers still take the strict branch.
        paper_static = self._paper_static_candidate(row, lifecycle)
        worker_environments = self._worker_environments()
        evidence_sources = self._mapping_sources(row)
        if isinstance(lifecycle, Mapping):
            evidence_sources += self._mapping_sources(lifecycle.get("payload"))
        evidence_environments = self._environment_values(evidence_sources)
        if any(
            str(getattr(value, "value", value)) != "PAPER"
            for value in evidence_environments
            if value not in (None, "")
        ):
            raise ValueError("SOURCE_ENVIRONMENT_INVALID")
        testnet_evidence = any(self._is_testnet_environment(value) for value in evidence_environments)
        worker_requires_strict = any(value != "PAPER" for value in worker_environments)
        strict = (
            self._strict_candidate_row(row)
            or persisted_store_usable
            or worker_requires_strict
            or testnet_evidence
        ) and not paper_static
        if paper_static:
            binding = row.get("binding")
            binding = dict(binding) if isinstance(binding, Mapping) else dict(row)
            binding.setdefault("candidate_id", candidate_id)
            binding.setdefault("symbol", symbol)
            strategy = row.get("strategy") if isinstance(row.get("strategy"), Mapping) else self.strategy
            if strategy is None and persisted_store_usable and isinstance(lifecycle, Mapping):
                payload = lifecycle.get("payload")
                stage = getattr(lifecycle.get("stage"), "value", lifecycle.get("stage"))
                if str(stage or "").upper() == "FROZEN" and isinstance(payload, Mapping):
                    strategy_binding = dict(binding)
                    strategy_ref = row.get("strategy_ref")
                    if isinstance(strategy_ref, Mapping):
                        strategy_binding["strategy_ref"] = dict(strategy_ref)
                    strategy, _ = self._load_exact_strategy(
                        payload, strategy_binding, deadline_monotonic=deadline_monotonic
                    )
            return binding, strategy, {}
        if not isinstance(lifecycle, Mapping):
            if strict:
                raise ValueError("FROZEN_EVIDENCE_MISSING")
            binding = row.get("binding")
            binding = dict(binding) if isinstance(binding, Mapping) else dict(row)
            binding.setdefault("candidate_id", candidate_id)
            binding.setdefault("symbol", symbol)
            strategy = row.get("strategy") if isinstance(row.get("strategy"), Mapping) else self.strategy
            return binding, strategy, {}
        stage = getattr(lifecycle.get("stage"), "value", lifecycle.get("stage"))
        if str(stage or "").upper() != "FROZEN":
            raise ValueError("FROZEN_CANDIDATE_NOT_CURRENT")
        payload = lifecycle.get("payload")
        if not isinstance(payload, Mapping):
            raise ValueError("FROZEN_EVIDENCE_MISSING")
        payload_sources = self._mapping_sources(payload)
        source_binding = row.get("binding")
        if isinstance(source_binding, Mapping) and isinstance(source_binding.get("binding"), Mapping):
            source_binding = source_binding["binding"]
        if not isinstance(source_binding, Mapping):
            for source in payload_sources:
                candidate = source.get("binding")
                if isinstance(candidate, Mapping) and candidate.get("candidate_id"):
                    source_binding = candidate
                    break
        if not isinstance(source_binding, Mapping):
            raise ValueError("SOURCE_BINDING_MISSING")
        if (
            str(source_binding.get("candidate_id", candidate_id)) != candidate_id
            or _symbol(source_binding.get("symbol", symbol)) != symbol
        ):
            raise ValueError("SOURCE_BINDING_IDENTITY_MISMATCH")
        strategy, strategy_ref = self._load_exact_strategy(
            payload, source_binding, deadline_monotonic=deadline_monotonic
        )
        successor, source_hash = self._project_successor(
            source_binding, candidate_id=candidate_id, symbol=symbol, deadline_monotonic=deadline_monotonic
        )
        declared_source_hashes: list[Any] = []
        for evidence_source in (*row_sources, *payload_sources):
            for hash_name in ("binding_hash", "source_binding_hash"):
                hash_value = evidence_source.get(hash_name)
                if hash_value not in (None, ""):
                    declared_source_hashes.append(hash_value)
        if any(str(value).strip() != source_hash for value in declared_source_hashes):
            raise ValueError("SOURCE_BINDING_HASH_MISMATCH")
        qualification_hash = self._consistent_source_value(
            (*self._mapping_sources(row), *payload_sources), "qualification_hash", default=""
        )
        immutable_values = self._source_values((*self._mapping_sources(row), *payload_sources), "immutable_hashes")
        immutable_hashes: Mapping[str, Any] = {}
        if immutable_values:
            normalized = {_canonical_json(value) for value in immutable_values if isinstance(value, Mapping)}
            if len(normalized) > 1:
                raise ValueError("IMMUTABLE_EVIDENCE_MISMATCH")
            immutable_hashes = next((value for value in immutable_values if isinstance(value, Mapping)), {})
        provenance = {
            "source_binding_hash": source_hash,
            "qualification_hash": str(qualification_hash or ""),
            "immutable_hashes": dict(immutable_hashes),
            "source_candidate_id": candidate_id,
            "source_symbol": symbol,
            "strategy_ref": dict(strategy_ref),
            "lifecycle_stage": "FROZEN",
        }
        return successor, strategy, {
            "binding": dict(successor),
            "binding_hash": successor["binding_hash"],
            "source_binding_hash": source_hash,
            "qualification_hash": str(qualification_hash or ""),
            "immutable_hashes": dict(immutable_hashes),
            "strategy_ref": dict(strategy_ref),
            "provenance": provenance,
            "lifecycle": {"candidate_id": candidate_id, "stage": "FROZEN", "payload": dict(payload)},
            "source_binding": dict(source_binding),
        }

    # ---- autonomous namespace -------------------------------------------------
    def _init_schema(self) -> None:
        with self.store._lock:
            conn = self.store.connection
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS binance_auto_schema_lock (
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                    namespace TEXT NOT NULL, schema_version TEXT NOT NULL, created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS binance_auto_state (
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1), worker_name TEXT NOT NULL,
                    status TEXT NOT NULL, cycle_number INTEGER NOT NULL DEFAULT 0,
                    cycle_id TEXT, no_trade_reason TEXT, pause_reason TEXT,
                    state_json TEXT NOT NULL, updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS binance_auto_cycles (
                    cycle_id TEXT PRIMARY KEY, worker_name TEXT NOT NULL, cycle_number INTEGER NOT NULL,
                    started_at TEXT NOT NULL, completed_at TEXT, status TEXT NOT NULL,
                    control_state TEXT, universe_id TEXT, universe_version TEXT, universe_hash TEXT,
                    dataset_version TEXT, ranking_run_id TEXT, selected_candidate TEXT,
                    selected_symbol TEXT, entry_count INTEGER NOT NULL DEFAULT 0,
                    exit_count INTEGER NOT NULL DEFAULT 0, no_trade_reason TEXT,
                    error TEXT, payload_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS binance_auto_events (
                    event_id TEXT PRIMARY KEY, cycle_id TEXT NOT NULL, observed_at TEXT NOT NULL,
                    event_type TEXT NOT NULL, status TEXT NOT NULL, symbol TEXT,
                    candidate_id TEXT, signal_id TEXT, reason TEXT, payload_json TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_binance_auto_cycles_time
                    ON binance_auto_cycles(worker_name, started_at, cycle_id);
                CREATE INDEX IF NOT EXISTS idx_binance_auto_events_cycle
                    ON binance_auto_events(cycle_id, observed_at, event_id);
                CREATE INDEX IF NOT EXISTS idx_binance_auto_events_signal
                    ON binance_auto_events(signal_id, event_type, status);
                """
            )
            now = _iso(self.clock())
            conn.execute(
                "INSERT OR IGNORE INTO binance_auto_schema_lock(singleton,namespace,schema_version,created_at) VALUES(1,?,?,?)",
                (self.namespace, self.schema_version, now),
            )
            conn.commit()

    def _load_restart_state(self) -> None:
        with self.store._lock:
            row = self.store.connection.execute(
                "SELECT cycle_number,state_json,status FROM binance_auto_state WHERE singleton=1 AND worker_name=?",
                (self.worker_id,),
            ).fetchone()
        if row is not None:
            self._cycle_number = int(row[0] or 0)
            try:
                self._last_status = json.loads(row[1])
            except (TypeError, ValueError, json.JSONDecodeError):
                self._last_status = {"status": str(row[2])}

    def _persist_state(self, result: Mapping[str, Any]) -> None:
        state = _safe(dict(result))
        now = _iso(self.clock())
        with self.store._lock:
            self.store.connection.execute(
                "INSERT INTO binance_auto_state(singleton,worker_name,status,cycle_number,cycle_id,no_trade_reason,pause_reason,state_json,updated_at) VALUES(1,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(singleton) DO UPDATE SET worker_name=excluded.worker_name,status=excluded.status,cycle_number=excluded.cycle_number,cycle_id=excluded.cycle_id,no_trade_reason=excluded.no_trade_reason,pause_reason=excluded.pause_reason,state_json=excluded.state_json,updated_at=excluded.updated_at",
                (
                    self.worker_id,
                    str(result.get("status", "IDLE")), self._cycle_number, result.get("cycle_id"),
                    result.get("no_trade_reason"), result.get("pause_reason"), _json(state), now,
                ),
            )
            self.store.connection.commit()
        self._last_status = dict(state) if isinstance(state, Mapping) else {"status": str(result.get("status", "IDLE"))}

    def _persist_cycle(self, result: Mapping[str, Any], started_at: str, completed_at: str) -> None:
        provenance = result.get("provenance") if isinstance(result.get("provenance"), Mapping) else {}
        ranking = result.get("ranking") if isinstance(result.get("ranking"), Mapping) else {}
        selected = ranking.get("selected") if isinstance(ranking.get("selected"), Mapping) else {}
        with self.store._lock:
            self.store.connection.execute(
                "INSERT OR REPLACE INTO binance_auto_cycles(cycle_id,worker_name,cycle_number,started_at,completed_at,status,control_state,universe_id,universe_version,universe_hash,dataset_version,ranking_run_id,selected_candidate,selected_symbol,entry_count,exit_count,no_trade_reason,error,payload_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    result["cycle_id"], self.worker_id, self._cycle_number, started_at, completed_at,
                    result.get("status", "NO_TRADE"), result.get("control_state"), provenance.get("universe_id"),
                    provenance.get("universe_version"), provenance.get("snapshot_hash"), provenance.get("dataset_version"),
                    ranking.get("ranking_run_id"), selected.get("candidate_id"), selected.get("symbol"),
                    len(result.get("entries", ()) or ()), len(result.get("exits", ()) or ()),
                    result.get("no_trade_reason"), result.get("error"), _json(result),
                ),
            )
            self.store.connection.commit()

    def _event(self, cycle_id: str, event_type: str, status: str, *, now: Any, symbol: Any = None, candidate_id: Any = None, signal_id: Any = None, reason: Any = None, payload: Any = None) -> dict[str, Any]:
        event = {
            "event_id": uuid.uuid4().hex,
            "cycle_id": cycle_id,
            "observed_at": _iso(now),
            "event_type": str(event_type),
            "status": str(status),
            "symbol": _symbol(symbol) if symbol else None,
            "candidate_id": None if candidate_id is None else str(candidate_id),
            "signal_id": None if signal_id is None else str(signal_id),
            "reason": None if reason is None else str(reason),
            "payload": payload,
        }
        if self._event_sink is not None:
            self._event_sink.append(_safe(event))
        with self.store._lock:
            self.store.connection.execute(
                "INSERT INTO binance_auto_events(event_id,cycle_id,observed_at,event_type,status,symbol,candidate_id,signal_id,reason,payload_json) VALUES(?,?,?,?,?,?,?,?,?,?)",
                (event["event_id"], cycle_id, event["observed_at"], event["event_type"], event["status"], event["symbol"], event["candidate_id"], event["signal_id"], event["reason"], _json(event["payload"])),
            )
            self.store.connection.commit()
        return event

    def _signal_seen(self, signal_id: str) -> bool:
        if not signal_id:
            return False
        with self.store._lock:
            row = self.store.connection.execute(
                "SELECT 1 FROM binance_auto_events WHERE signal_id=? AND event_type IN ('ENTRY_SUBMIT','EXIT_SUBMIT') AND status IN ('SUBMITTED','ACCEPTED','UNKNOWN','ACKNOWLEDGED','FILLED','PARTIALLY_FILLED') LIMIT 1",
                (signal_id,),
            ).fetchone()
        return row is not None
    def _control(self, deadline_monotonic: float | None = None) -> dict[str, Any]:
        self._checkpoint(deadline_monotonic)
        if self.execution is None:
            return {"state": DISABLED, "authorized": False}
        method = getattr(self.execution, "control", None) or getattr(self.execution, "status", None)
        try:
            value = _call(method, deadline_monotonic=deadline_monotonic) if callable(method) else method
            self._checkpoint(deadline_monotonic)
        except _DeadlineExpired:
            raise
        except BaseException as exc:
            if self._is_deadline_exception(exc):
                raise _DeadlineExpired(AUTO_DEADLINE_REASON)
            return {"state": PAUSED, "authorized": False, "pause_reason": type(exc).__name__}
        if isinstance(value, Mapping):
            result = dict(value)
            if "state" not in result and result.get("control_state") is not None:
                result["state"] = result["control_state"]
            result.setdefault("state", DISABLED)
            result.setdefault("authorized", False)
            return result
        return {"state": DISABLED, "authorized": False}

    def _pause(self, reason: str) -> None:
        if self.execution is not None:
            method = getattr(self.execution, "pause", None)
            if callable(method):
                try:
                    _call(method, str(reason))
                except Exception:
                    pass
    def _reconcile(self, now: datetime, deadline_monotonic: float | None = None) -> tuple[dict[str, Any], str | None]:
        self._checkpoint(deadline_monotonic)
        if self.execution is None:
            return ({"status": "FAILURE", "error": "execution service unavailable"}, "EXECUTION_UNAVAILABLE")
        method = getattr(self.execution, "reconcile", None) or getattr(self.execution, "poll", None)
        if not callable(method):
            return ({"status": "FAILURE", "error": "reconcile unavailable"}, "RECONCILIATION_UNAVAILABLE")
        try:
            result = _call(method, deadline_monotonic=deadline_monotonic)
            self._checkpoint(deadline_monotonic)
            result = dict(result) if isinstance(result, Mapping) else {"status": "SUCCESS", "result": result}
            if str(result.get("error", "")).upper() == AUTO_DEADLINE_REASON:
                raise _DeadlineExpired(AUTO_DEADLINE_REASON)
            status = str(result.get("status", "SUCCESS")).upper()
            if status in {"FAILURE", "ERROR", "UNKNOWN", "PAUSED"}:
                reason = "RECONCILIATION_" + status
                self._pause(reason)
                return result, reason
            return result, None
        except _DeadlineExpired:
            raise
        except BaseException as exc:
            if self._is_deadline_exception(exc):
                raise _DeadlineExpired(AUTO_DEADLINE_REASON)
            reason = "RECONCILIATION_TRANSPORT_" + type(exc).__name__.upper()
            self._pause(reason)
            return {"status": "FAILURE", "error": type(exc).__name__}, reason

    def _connectivity(self, deadline_monotonic: float | None = None) -> str | None:
        self._checkpoint(deadline_monotonic)
        if self.execution is None:
            return "EXECUTION_UNAVAILABLE"
        method = getattr(self.execution, "check_connectivity", None) or getattr(self.execution, "connectivity", None)
        if not callable(method):
            return None
        try:
            result = _call(method, deadline_monotonic=deadline_monotonic)
            self._checkpoint(deadline_monotonic)
            status = str(result.get("status", "OK") if isinstance(result, Mapping) else "OK").upper()
            if isinstance(result, Mapping) and str(result.get("error", "")).upper() == AUTO_DEADLINE_REASON:
                raise _DeadlineExpired(AUTO_DEADLINE_REASON)
            if status not in {"OK", "SUCCESS", "ACKNOWLEDGED"}:
                reason = "CONNECTIVITY_" + status
                self._pause(reason)
                return reason
        except _DeadlineExpired:
            raise
        except BaseException as exc:
            if self._is_deadline_exception(exc):
                raise _DeadlineExpired(AUTO_DEADLINE_REASON)
            reason = "CONNECTIVITY_TRANSPORT_" + type(exc).__name__.upper()
            self._pause(reason)
            return reason
        return None

    def _load_universe(self, now: datetime, deadline_monotonic: float | None = None) -> UniverseSnapshot | None:
        self._checkpoint(deadline_monotonic)
        source = self._universe_source
        if self._universe_loader is not None:
            value = _call(self._universe_loader, now=now, deadline_monotonic=deadline_monotonic)
        elif isinstance(source, UniverseSnapshot):
            value = source
        elif isinstance(source, Mapping):
            value = UniverseSnapshot.from_record(source, universe_id=source.get("universe_id"))
        elif source is not None:
            loader = getattr(source, "load_persisted", None) or getattr(source, "load", None) or getattr(source, "snapshot", None)
            if callable(loader):
                value = _call(loader, deadline_monotonic=deadline_monotonic)
            elif callable(source):
                value = _call(source, now=now, deadline_monotonic=deadline_monotonic)
            else:
                value = source
        else:
            value = load_crypto_universe(self.store)
        self._checkpoint(deadline_monotonic)
        if value is None:
            return None
        if isinstance(value, UniverseSnapshot):
            return value
        if isinstance(value, Mapping):
            return UniverseSnapshot.from_record(value, universe_id=value.get("universe_id"))
        raise TypeError("universe loader did not return UniverseSnapshot")


    def _collector_for(self, snapshot: UniverseSnapshot) -> Any:
        if self._collector is not None:
            return self._collector
        if self._provider is None:
            raise RuntimeError("market collector/provider unavailable")
        kwargs = dict(self._collector_kwargs)
        kwargs.setdefault("max_workers", 4)
        kwargs.setdefault("depth", self.depth)
        kwargs.setdefault("fill_quantity", float(self.fill_quantity))
        kwargs.setdefault("clock", self.clock)
        return BoundedBinanceMarketCollector(self._provider, snapshot, **kwargs)

    def _collect(
        self,
        snapshot: UniverseSnapshot,
        exit_symbols: Sequence[str],
        now: datetime,
        deadline_monotonic: float | None = None,
    ) -> list[Any]:
        self._checkpoint(deadline_monotonic)
        collector = self._collector_for(snapshot)
        collect = getattr(collector, "collect", None)
        if not callable(collect):
            raise RuntimeError("collector has no collect method")
        result = _call(
            collect, snapshot, interval=self.interval, limit=self.limit, exit_symbols=tuple(exit_symbols),
            reconciliation=False, now=now, deadline_monotonic=deadline_monotonic,
        )
        self._checkpoint(deadline_monotonic)
        if isinstance(result, Mapping):
            status = str(result.get("status", "SUCCESS")).upper()
            if status in {"FAILURE", "ERROR", "PAUSED"} or result.get("error"):
                raise RuntimeError("market collection " + status)
            if "records" in result or "snapshots" in result:
                records = result.get("records", result.get("snapshots", ()))
                return list(records) if isinstance(records, (list, tuple)) else []
            if result.get("symbol") is not None:
                return [result]
            records: list[Any] = []
            for symbol, record in result.items():
                if not isinstance(record, Mapping):
                    continue
                item = dict(record)
                item.setdefault("symbol", symbol)
                records.append(item)
            return records
        return list(result or ())

    # ---- positions and signals ------------------------------------------------
    def _positions(self, deadline_monotonic: float | None = None) -> dict[str, Any]:
        self._checkpoint(deadline_monotonic)
        if self.execution is None:
            return {}
        method = getattr(self.execution, "positions", None)
        if callable(method):
            try:
                result = _call(method, deadline_monotonic=deadline_monotonic)
                self._checkpoint(deadline_monotonic)
            except _DeadlineExpired:
                raise
            except Exception as exc:
                if self._is_deadline_exception(exc):
                    raise _DeadlineExpired(AUTO_DEADLINE_REASON)
                return {}
        else:
            result = _value(self.execution, "positions", default={})

        nested_keys = ("positions", "rows", "items", "position", "data")
        metadata_keys = {
            "symbol", "pair", "market", "market_symbol", "asset", "asset_symbol",
            "status", "count", "total", "page", "page_size", "next",
        }

        def rows(value: Any, symbol_hint: str = "", depth: int = 0) -> Iterable[tuple[str, Any]]:
            if depth > 6:
                return
            if isinstance(value, Mapping):
                explicit_symbol = _symbol(
                    _value(value, "symbol", "pair", "market_symbol", "asset_symbol", default=symbol_hint)
                )
                nested = _value(value, *nested_keys, default=None)
                if explicit_symbol:
                    # A list item such as ``{"symbol": "BTCUSDT",
                    # "quantity": "1"}`` is already a position row.
                    if _quantity(value) > ZERO:
                        yield explicit_symbol, value
                    # Some adapters wrap that row in ``position``/``data``.
                    # Keep the outer symbol as a hint while walking inward.
                    if nested is not None:
                        yield from rows(nested, explicit_symbol, depth + 1)
                    return
                if nested is not None:
                    yield from rows(nested, symbol_hint, depth + 1)
                    return
                # Also accept a mapping keyed by symbol, including one nested
                # inside a list: ``[{"BTCUSDT": {"quantity": "1"}}]``.
                for key, child in value.items():
                    name = str(key).strip().lower()
                    if name in metadata_keys or name in nested_keys:
                        continue
                    child_symbol = _symbol(key)
                    if isinstance(child, (Mapping, list, tuple)):
                        yield from rows(child, child_symbol or symbol_hint, depth + 1)
                return
            if isinstance(value, (list, tuple)):
                for item in value:
                    yield from rows(item, symbol_hint, depth + 1)
                return
            mapped = _as_mapping(value)
            if mapped is not None:
                yield from rows(mapped, symbol_hint, depth + 1)
                return
            explicit_symbol = _symbol(_value(value, "symbol", "pair", default=symbol_hint))
            if explicit_symbol:
                yield explicit_symbol, value

        output: dict[str, Any] = {}
        for symbol, row in rows(result):
            if _axiom_owned(row) and _quantity(row) > ZERO:
                output[symbol] = row
        return output

    def _unresolved_exit_symbols(self, deadline_monotonic: float | None = None) -> set[str]:
        """Return symbols with an owned SELL whose reservation is still held."""
        self._checkpoint(deadline_monotonic)
        if self.execution is None:
            return set()
        method = getattr(self.execution, "orders", None)
        if not callable(method):
            return set()
        try:
            rows = _call(method, deadline_monotonic=deadline_monotonic)
            self._checkpoint(deadline_monotonic)
        except _DeadlineExpired:
            raise
        except Exception as exc:
            if self._is_deadline_exception(exc):
                raise _DeadlineExpired(AUTO_DEADLINE_REASON)
            return set()
        result: set[str] = set()
        for row in rows or ():
            if not isinstance(row, Mapping):
                continue
            if str(row.get("intent", "")).upper() != "EXIT":
                continue
            if str(row.get("side", "")).upper() != "SELL":
                continue
            reservation = row.get("risk_reservation")
            if not isinstance(reservation, Mapping) or str(reservation.get("status", "")).upper() != "HELD":
                continue
            symbol = _symbol(row.get("symbol"))
            if symbol:
                result.add(symbol)
        return result

    def _origin_binding(self, position: Any) -> dict[str, Any]:
        for name in ("originating_binding", "entry_binding", "binding"):
            value = _value(position, name, default=None)
            if isinstance(value, Mapping):
                nested = value.get("binding")
                result = dict(nested if isinstance(nested, Mapping) else value)
                result.setdefault("environment", self._profile_environment())
                return result
        value = _value(position, "binding_json", default=None)
        if isinstance(value, Mapping):
            result = dict(value)
            result.setdefault("environment", self._profile_environment())
            return result
        candidate = _value(position, "candidate_id", "originating_candidate_id", default="")
        symbol = _symbol(_value(position, "symbol", default=""))
        return {
            "candidate_id": str(candidate or ("position-" + symbol)),
            "symbol": symbol,
            "binding_hash": _value(position, "binding_hash", "originating_binding_hash", default=""),
            "environment": self._profile_environment(),
            "timeframe": self.interval,
            "exit_policy": _value(position, "exit_policy", "frozen_exit_policy", "originating_exit_policy", default={}) or {},
        }


    def _strict_origin(self, position: Any, binding: Mapping[str, Any]) -> bool:
        worker_environments = self._worker_environments()
        if any(value != "PAPER" for value in worker_environments):
            return True
        if worker_environments:
            evidence_sources = self._mapping_sources(position, binding)
            evidence_environments = self._environment_values(evidence_sources)
            if any(
                str(getattr(value, "value", value)).strip().upper() != "PAPER"
                for value in evidence_environments
            ):
                return True
            # An explicitly declared PAPER worker retains the injected origin
            # binding, even when it carries the persisted-looking fields used
            # by the strict Testnet restart path.
            return False
        return any(
            _value(position, name, default=None) not in (None, "")
            for name in ("originating_strategy_ref", "strategy_ref", "originating_provenance", "provenance")
        ) or any(
            key in binding for key in ("strategy_hash", "strategy_ref", "source_binding_hash", "qualification_hash", "immutable_hashes")
        )

    def _hydrate_origin(
        self,
        position: Any,
        deadline_monotonic: float | None = None,
    ) -> tuple[Any | None, dict[str, Any]]:
        """Hydrate the exact strategy/evidence that opened an owned position."""
        binding = self._origin_binding(position)
        position_symbol = _symbol(_value(position, "symbol", "pair", "market_symbol", default=""))
        binding_symbol = _symbol(binding.get("symbol"))
        if position_symbol and binding_symbol != position_symbol:
            raise ValueError("EXIT_ORIGIN_SYMBOL_MISMATCH")
        strict = self._strict_origin(position, binding)
        projected_origin = False
        for binding_environment in self._environment_values((binding,)):
            normalized_binding_environment = str(
                getattr(binding_environment, "value", binding_environment)
            ).strip().upper()
            if normalized_binding_environment in {"TESTNET", "BINANCE_SPOT_TESTNET"}:
                projected_origin = True
            elif binding_environment != "PAPER":
                raise ValueError("EXIT_ORIGIN_ENVIRONMENT_INVALID")
        origin_provenance = _value(position, "originating_provenance", "provenance", default={})
        provenance_sources = self._mapping_sources(origin_provenance)
        for provenance_environment in self._environment_values(provenance_sources):
            normalized_provenance_environment = str(
                getattr(provenance_environment, "value", provenance_environment)
            ).strip().upper()
            if normalized_provenance_environment in {"TESTNET", "BINANCE_SPOT_TESTNET"}:
                projected_origin = True
            elif provenance_environment != "PAPER":
                raise ValueError("EXIT_ORIGIN_ENVIRONMENT_INVALID")
        projected_origin = projected_origin or strict
        root_environments = self._environment_values((position,))
        root_testnet = False
        for root_environment in root_environments:
            normalized_root_environment = str(
                getattr(root_environment, "value", root_environment)
            ).strip().upper()
            if normalized_root_environment in {"TESTNET", "BINANCE_SPOT_TESTNET"}:
                root_testnet = True
            elif root_environment != "PAPER":
                raise ValueError("EXIT_ORIGIN_ENVIRONMENT_INVALID")
        strict_testnet_execution = any(
            self._is_testnet_environment(value) for value in self._worker_environments()
        )
        if projected_origin or root_testnet or strict_testnet_execution:
            strict = True
            projected_origin = True
            if any(
                str(getattr(value, "value", value)).strip().upper()
                not in {"TESTNET", "BINANCE_SPOT_TESTNET"}
                for value in root_environments
            ):
                raise ValueError("EXIT_ORIGIN_ENVIRONMENT_INVALID")
        if projected_origin and not strict:
            raise ValueError("EXIT_ORIGIN_ENVIRONMENT_INVALID")
        if not strict:
            for declared_environment in self._environment_values(self._mapping_sources(position, binding)):
                normalized_environment = str(getattr(declared_environment, "value", declared_environment)).strip().upper()
                if normalized_environment != "PAPER":
                    raise ValueError("EXIT_ORIGIN_ENVIRONMENT_INVALID")
        if not strict:
            policy = _value(position, "exit_policy", "frozen_exit_policy", "originating_exit_policy", default={})
            static_strategy = policy.get("strategy") if isinstance(policy, Mapping) else None
            return static_strategy if static_strategy is not None else self.strategy, {}
        candidate_id = str(
            binding.get("candidate_id")
            or _value(position, "candidate_id", "originating_candidate_id", default="")
        ).strip()
        symbol = _symbol(binding.get("symbol") or _value(position, "symbol", default=""))
        if not candidate_id or not symbol:
            raise ValueError("EXIT_ORIGIN_EVIDENCE_MISMATCH")
        declared_binding_hash = str(
            binding.get("binding_hash")
            or _value(position, "binding_hash", "originating_binding_hash", default="")
            or ""
        )
        self._checkpoint(deadline_monotonic)
        lifecycle = self.store.load_candidate_lifecycle(candidate_id)
        self._checkpoint(deadline_monotonic)
        if not isinstance(lifecycle, Mapping):
            raise ValueError("EXIT_ORIGIN_EVIDENCE_MISMATCH")
        stage = getattr(lifecycle.get("stage"), "value", lifecycle.get("stage"))
        if str(stage or "").upper() != "FROZEN":
            raise ValueError("EXIT_ORIGIN_FROZEN_STAGE_MISMATCH")
        payload = lifecycle.get("payload")
        if not isinstance(payload, Mapping):
            raise ValueError("EXIT_ORIGIN_EVIDENCE_MISMATCH")
        payload_sources = self._mapping_sources(payload)
        source_binding: Mapping[str, Any] | None = None
        for source in payload_sources:
            candidate = source.get("binding")
            if isinstance(candidate, Mapping) and candidate.get("candidate_id"):
                source_binding = candidate
                break
        if source_binding is None:
            raise ValueError("EXIT_ORIGIN_BINDING_MISSING")
        if (
            str(source_binding.get("candidate_id", candidate_id)) != candidate_id
            or _symbol(source_binding.get("symbol", symbol)) != symbol
        ):
            raise ValueError("EXIT_ORIGIN_IDENTITY_MISMATCH")
        if any(
            str(getattr(value, "value", value)) != "PAPER"
            for value in self._environment_values(payload_sources)
            if value not in (None, "")
        ):
            raise ValueError("EXIT_ORIGIN_SOURCE_ENVIRONMENT_INVALID")
        successor, source_hash = self._project_successor(
            source_binding, candidate_id=candidate_id, symbol=symbol, deadline_monotonic=deadline_monotonic
        )
        expected_successor_hash = str(successor.get("binding_hash") or "")
        root_binding_hash = _value(position, "binding_hash", default=None)
        root_source_binding_hash = _value(position, "source_binding_hash", default=None)
        if projected_origin:
            if root_binding_hash not in (None, "") and str(root_binding_hash).strip() != expected_successor_hash:
                raise ValueError("EXIT_ORIGIN_BINDING_HASH_MISMATCH")
            if root_source_binding_hash not in (None, "") and str(root_source_binding_hash).strip() != source_hash:
                raise ValueError("EXIT_ORIGIN_PROVENANCE_MISMATCH")
        if projected_origin and not declared_binding_hash:
            raise ValueError("EXIT_ORIGIN_BINDING_HASH_MISSING")
        if declared_binding_hash and declared_binding_hash != expected_successor_hash:
            raise ValueError("EXIT_ORIGIN_BINDING_HASH_MISMATCH")
        payload_hashes: list[tuple[str, Any]] = []
        for payload_source in payload_sources:
            for hash_name in ("binding_hash", "source_binding_hash"):
                hash_value = payload_source.get(hash_name)
                if hash_value not in (None, ""):
                    payload_hashes.append((hash_name, hash_value))
        for hash_name, hash_value in payload_hashes:
            expected_hashes = {source_hash, expected_successor_hash} if hash_name == "binding_hash" else {source_hash}
            if str(hash_value).strip() not in expected_hashes:
                raise ValueError("EXIT_ORIGIN_PROVENANCE_MISMATCH")
        provenance = _value(position, "originating_provenance", "provenance", default={})
        provenance = dict(provenance) if isinstance(provenance, Mapping) else {}
        position_source_hash = provenance.get("source_binding_hash")
        if projected_origin and (not position_source_hash or str(position_source_hash).strip() != source_hash):
            raise ValueError("EXIT_ORIGIN_PROVENANCE_MISMATCH")
        for provenance_source in provenance_sources:
            for hash_name in ("binding_hash", "source_binding_hash"):
                hash_value = provenance_source.get(hash_name)
                if hash_value in (None, ""):
                    continue
                expected_hashes = {expected_successor_hash} if hash_name == "binding_hash" else {source_hash}
                if str(hash_value).strip() not in expected_hashes:
                    raise ValueError("EXIT_ORIGIN_PROVENANCE_MISMATCH")
        position_ref = _value(position, "originating_strategy_ref", "strategy_ref", default=None)
        position_ref = position_ref if isinstance(position_ref, Mapping) else provenance.get("strategy_ref", {})
        merged_binding = dict(binding)
        if isinstance(position_ref, Mapping) and position_ref:
            merged_binding["strategy_ref"] = dict(position_ref)
        strategy, strategy_ref = self._load_exact_strategy(
            payload, merged_binding, deadline_monotonic=deadline_monotonic
        )
        verified_provenance = dict(provenance)
        verified_provenance.update({
            "binding": dict(successor),
            "source_binding_hash": source_hash,
            "strategy_ref": dict(strategy_ref),
            "lifecycle": {"candidate_id": candidate_id, "stage": "FROZEN", "payload": dict(payload)},
        })
        return strategy, {
            "binding": dict(successor),
            "binding_hash": expected_successor_hash,
            "source_binding_hash": source_hash,
            "strategy_ref": dict(strategy_ref),
            "provenance": verified_provenance,
            "lifecycle": {"candidate_id": candidate_id, "stage": "FROZEN", "payload": dict(payload)},
        }
    def _attach_signal(
        self,
        signal: Mapping[str, Any],
        *,
        binding: Mapping[str, Any],
        evidence: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        output = dict(signal)
        strict_evidence = bool(evidence)
        expected_candidate = str(binding.get("candidate_id") or "")
        expected_symbol = _symbol(binding.get("symbol"))
        expected_binding_hash = str(binding.get("binding_hash") or "")
        if strict_evidence:
            if expected_candidate and output.get("candidate_id") not in (None, "", expected_candidate):
                raise ValueError("SIGNAL_CANDIDATE_MISMATCH")
            if expected_symbol and output.get("symbol") not in (None, "") and _symbol(output.get("symbol")) != expected_symbol:
                raise ValueError("SIGNAL_SYMBOL_MISMATCH")
            if expected_binding_hash and output.get("binding_hash") not in (None, "", expected_binding_hash):
                raise ValueError("SIGNAL_BINDING_HASH_MISMATCH")
            if isinstance(output.get("binding"), Mapping):
                wrapper = output["binding"]
                if expected_candidate and str(wrapper.get("candidate_id", expected_candidate)) != expected_candidate:
                    raise ValueError("SIGNAL_CANDIDATE_MISMATCH")
                if expected_symbol and _symbol(wrapper.get("symbol", expected_symbol)) != expected_symbol:
                    raise ValueError("SIGNAL_SYMBOL_MISMATCH")
                if expected_binding_hash and wrapper.get("binding_hash") not in (None, "", expected_binding_hash):
                    raise ValueError("SIGNAL_BINDING_HASH_MISMATCH")
            verified_fields = (
                "source_binding_hash", "qualification_hash", "immutable_hashes",
                "strategy_ref", "lifecycle",
            )
            for key in verified_fields:
                expected = evidence.get(key)
                if expected in (None, ""):
                    if key in output and output[key] not in (None, ""):
                        raise ValueError("SIGNAL_EVIDENCE_UNVERIFIED")
                    continue
                if key in output and output[key] not in (None, "") and _canonical_json(output[key]) != _canonical_json(expected):
                    raise ValueError("SIGNAL_EVIDENCE_MISMATCH")
                output[key] = _canonical_value(expected)
            if "binding_hash" in evidence:
                verified_hash = str(evidence["binding_hash"])
                if output.get("binding_hash") not in (None, "", verified_hash):
                    raise ValueError("SIGNAL_BINDING_HASH_MISMATCH")
                output["binding_hash"] = verified_hash
        output.update({
            "candidate_id": expected_candidate or output.get("candidate_id"),
            "symbol": expected_symbol or _symbol(output.get("symbol")),
            "binding": dict(binding),
            "successor_binding": dict(binding),
            "binding_hash": expected_binding_hash or output.get("binding_hash", ""),
        })
        if evidence:
            provenance = dict(evidence.get("provenance", {})) if isinstance(evidence.get("provenance"), Mapping) else {}
            provenance.update({
                "binding": dict(binding),
                "source_binding_hash": evidence.get("source_binding_hash"),
                "qualification_hash": evidence.get("qualification_hash"),
                "immutable_hashes": evidence.get("immutable_hashes", {}),
                "strategy_ref": evidence.get("strategy_ref", {}),
                "lifecycle": evidence.get("lifecycle", {}),
            })
            output["provenance"] = _canonical_value(provenance)
        return output

    def _engine(
        self,
        binding: Mapping[str, Any],
        *,
        position: Any = None,
        intent: str = "ENTRY",
        row: Mapping[str, Any] | None = None,
        strategy: Any | None = None,
        deadline_monotonic: float | None = None,
    ) -> Any:
        self._checkpoint(deadline_monotonic)
        policy = _value(position, "exit_policy", "frozen_exit_policy", "originating_exit_policy", default=None) if position is not None else None
        if not isinstance(policy, Mapping):
            policy = _value(binding, "exit_policy", default={}) or {}
        if strategy is None and self._profile_environment() == "PAPER" and self.strategy is not None:
            # Explicitly injected PAPER workers may still use their static
            # strategy.  Persisted rows are hydrated before reaching here.
            strategy = self.strategy
        kwargs = {
            "binding": binding,
            "strategy": strategy,
            "environment": _value(binding, "environment", default=self._profile_environment()),
            "decision_interval": _value(binding, "timeframe", "interval", default=self.interval),
            "exit_policy": dict(policy) if isinstance(policy, Mapping) else {},
            "positions": {_symbol(_value(position, "symbol", default="")): position} if position is not None else {},
            "clock": self.clock,
            "intent": intent,
            "row": row,
            "deadline_monotonic": deadline_monotonic,
        }
        factory = self.signal_engine_factory
        if factory is not None:
            try:
                result = _call(factory, **kwargs)
            except TypeError:
                for args in ((binding,), (binding, strategy), (binding, position)):
                    try:
                        result = factory(*args)
                        break
                    except TypeError:
                        continue
                else:
                    raise
            self._checkpoint(deadline_monotonic)
            return result
        if self.signal_engine is not None:
            result = (
                _call(self.signal_engine, **kwargs)
                if callable(self.signal_engine) and not hasattr(self.signal_engine, "evaluate")
                else self.signal_engine
            )
            self._checkpoint(deadline_monotonic)
            return result
        if strategy is None:
            raise ValueError("strategy unavailable for BinanceSignalEngine")
        result = BinanceSignalEngine(
            binding, strategy, environment=kwargs["environment"], decision_interval=kwargs["decision_interval"],
            exit_policy=kwargs["exit_policy"], positions=kwargs["positions"], clock=self.clock,
        )
        self._checkpoint(deadline_monotonic)
        return result

    def _evaluate(
        self,
        engine: Any,
        market: Any,
        *,
        now: datetime,
        positions: Mapping[str, Any],
        deadline_monotonic: float | None = None,
    ) -> tuple[Mapping[str, Any] | None, str]:
        self._checkpoint(deadline_monotonic)
        method = getattr(engine, "evaluate", None) or getattr(engine, "decide", None)
        if not callable(method):
            raise RuntimeError("signal engine has no evaluate method")
        value = _call(method, market, now=now, positions=positions, deadline_monotonic=deadline_monotonic)
        self._checkpoint(deadline_monotonic)
        if value is None:
            reason = str(_value(engine, "no_trade_reason", default="NO_SIGNAL") or "NO_SIGNAL")
            if reason.upper() == AUTO_DEADLINE_REASON:
                raise _DeadlineExpired(AUTO_DEADLINE_REASON)
            return None, reason
        result = _as_mapping(value)
        if result is None:
            raise TypeError("signal engine returned a non-mapping signal")
        if str(result.get("reason", "")).upper() == AUTO_DEADLINE_REASON:
            raise _DeadlineExpired(AUTO_DEADLINE_REASON)
        return result, ""

    def _market_map(self, records: Iterable[Any]) -> dict[str, Any]:
        output: dict[str, Any] = {}
        if isinstance(records, Mapping):
            items = records.items()
        else:
            items = ((None, record) for record in records)
        for key, record in items:
            symbol = _symbol(_value(record, "symbol", default=key or ""))
            if symbol:
                if key and isinstance(record, Mapping) and not _value(record, "symbol", default=None):
                    record = {**record, "symbol": symbol}
                output[symbol] = record
        return output

    def _feasible(self, market: Any) -> bool:
        if bool(_value(market, "error", default=None)) or bool(_value(market, "timed_out", default=False)):
            return False
        if not bool(_value(market, "tradable", default=False)):
            return False
        status = _value(market, "status", default=None)
        if status is not None and str(status).strip() and str(status).upper() != "TRADING":
            return False
        exchange_info = _value(market, "exchange_info", default=None)
        if isinstance(exchange_info, Mapping):
            info_status = str(exchange_info.get("status", "")).upper()
            if info_status and info_status != "TRADING":
                return False
            if exchange_info.get("isSpotTradingAllowed") is False:
                return False
        rules = _value(market, "symbol_rules", "rules", default=None)
        if rules is not None:
            rule_status = _value(rules, "status", default=None)
            if rule_status is not None and str(rule_status).strip() and str(rule_status).upper() != "TRADING":
                return False
            if _value(rules, "spot_trading_allowed", default=True) is False:
                return False
        if not bool(_value(market, "new_entry_allowed", default=False)):
            return False
        if not bool(_value(market, "ticker_fresh", "fresh_ticker", default=False)) or not bool(_value(market, "book_fresh", "fresh_book", default=False)):
            return False
        depth = _value(market, "depth", default={})
        if not isinstance(depth, Mapping) or depth.get("fresh") is False:
            return False
        try:
            if int(depth.get("bid_levels", 0) or 0) < 1 or int(depth.get("ask_levels", 0) or 0) < 1:
                return False
        except (TypeError, ValueError, OverflowError):
            return False
        spread = _value(market, "spread", default=None)
        if spread is None or _dec(spread) <= ZERO:
            return False
        evidence = _value(market, "fill_evidence", default={})
        if not isinstance(evidence, Mapping):
            return False
        buy, sell = evidence.get("buy"), evidence.get("sell")
        return isinstance(buy, Mapping) and isinstance(sell, Mapping) and bool(buy.get("complete")) and bool(sell.get("complete"))

    def _rules(self, market: Any) -> SymbolRules:
        existing = _value(market, "symbol_rules", "rules", default=None)
        if isinstance(existing, SymbolRules):
            return existing
        info = _value(market, "exchange_info", default=None)
        if isinstance(info, Mapping):
            try:
                return SymbolRules.from_exchange_info(info)
            except (TypeError, ValueError):
                pass
        return SymbolRules(symbol=_symbol(_value(market, "symbol", default="")), status="TRADING", spot_trading_allowed=True)

    def _price(self, market: Any, side: str) -> Decimal:
        evidence = _value(market, "fill_evidence", default={})
        side_data = evidence.get("buy" if side == "BUY" else "sell", {}) if isinstance(evidence, Mapping) else {}
        price = _dec(side_data.get("price") if isinstance(side_data, Mapping) else None)
        if price > ZERO:
            return price
        ticker = _value(market, "ticker", default=None)
        return _dec(_value(ticker, "ask" if side == "BUY" else "bid", "last", default=ZERO))

    def _submit(
        self,
        signal: Mapping[str, Any],
        market: Any,
        *,
        position: Any = None,
        now: datetime,
        cycle_id: str,
        event_type: str,
        candidate_id: Any = None,
        deadline_monotonic: float | None = None,
    ) -> tuple[dict[str, Any], bool]:
        self._checkpoint(deadline_monotonic)
        signal_id = str(signal.get("signal_id") or signal.get("id") or "")
        symbol = _symbol(signal.get("symbol") or _value(market, "symbol", default=""))
        if not signal_id:
            self._event(cycle_id, event_type, "BLOCKED", now=now, symbol=symbol, candidate_id=candidate_id, reason="SIGNAL_ID_MISSING", payload=signal)
            return {"status": "BLOCKED", "reason": "SIGNAL_ID_MISSING"}, False
        if self._signal_seen(signal_id):
            self._event(cycle_id, event_type, "DEDUPLICATED", now=now, symbol=symbol, candidate_id=candidate_id, signal_id=signal_id, reason="DUPLICATE_INTERVAL", payload=signal)
            return {"status": "DEDUPLICATED", "reason": "DUPLICATE_INTERVAL", "signal_id": signal_id}, False
        side = str(signal.get("side", "SELL" if signal.get("intent") == "EXIT" else "BUY")).upper()
        price = self._price(market, side)
        quantity = _quantity(position) if position is not None else self.fill_quantity
        if price <= ZERO or quantity <= ZERO:
            reason = "CONSERVATIVE_PRICE_OR_QUANTITY_UNAVAILABLE"
            self._event(cycle_id, event_type, "BLOCKED", now=now, symbol=symbol, candidate_id=candidate_id, signal_id=signal_id, reason=reason, payload=signal)
            return {"status": "BLOCKED", "reason": reason}, False
        if self.execution is None or not callable(getattr(self.execution, "submit_signal", None)):
            reason = "EXECUTION_UNAVAILABLE"
            self._event(cycle_id, event_type, "BLOCKED", now=now, symbol=symbol, candidate_id=candidate_id, signal_id=signal_id, reason=reason, payload=signal)
            return {"status": "BLOCKED", "reason": reason}, False
        self._checkpoint(deadline_monotonic)
        try:
            result = _call(
                self.execution.submit_signal, signal, market=market, price=price, quantity=quantity, rules=self._rules(market),
                fee_rate=self.fee_rate, time_in_force=self.time_in_force, now=now, opportunity_id=candidate_id,
                deadline_monotonic=deadline_monotonic,
            )
            self._checkpoint(deadline_monotonic)
            output = dict(result) if isinstance(result, Mapping) else {"status": "SUBMITTED", "result": result}
        except _DeadlineExpired:
            raise
        except BaseException as exc:
            if self._is_deadline_exception(exc):
                raise _DeadlineExpired(AUTO_DEADLINE_REASON)
            reason = "SUBMIT_TRANSPORT_" + type(exc).__name__.upper()
            self._pause(reason)
            self._event(cycle_id, event_type, "PAUSED", now=now, symbol=symbol, candidate_id=candidate_id, signal_id=signal_id, reason=reason, payload=signal)
            return {"status": "PAUSED", "reason": reason}, False
        state = str(output.get("state", output.get("status", "SUBMITTED"))).upper()
        blocked_reason = str(output.get("reason", ""))
        if blocked_reason.upper() == AUTO_DEADLINE_REASON or str(output.get("error", "")).upper() == AUTO_DEADLINE_REASON:
            raise _DeadlineExpired(AUTO_DEADLINE_REASON)
        accepted = state not in {"REJECTED", "BLOCKED", "PAUSED", "DISABLED", "DISARMED", "KILLED"} and not blocked_reason.startswith("CONTROL_")
        self._event(cycle_id, event_type, "ACCEPTED" if accepted else "BLOCKED", now=now, symbol=symbol, candidate_id=candidate_id, signal_id=signal_id, reason=blocked_reason or state, payload={"signal": signal, "result": output})
        return output, accepted

    def _rank(self, feasibility: Mapping[str, Any], now: datetime, deadline_monotonic: float | None = None) -> dict[str, Any]:
        self._checkpoint(deadline_monotonic)
        if self.qualification is None:
            return {"selection_status": "NONE", "rankings": [], "reason": "QUALIFICATION_UNAVAILABLE"}
        rank = getattr(self.qualification, "rank_and_select", None) or getattr(self.qualification, "rank", None)
        if callable(rank):
            result = _call(rank, feasibility, limit=self.max_actionable, now=now, deadline_monotonic=deadline_monotonic)
            self._checkpoint(deadline_monotonic)
            projected = _as_mapping(result)
            if projected is not None:
                return dict(projected)
            metadata = getattr(result, "meta", None)
            output = dict(metadata) if isinstance(metadata, Mapping) else {}
            output["rankings"] = list(result or ())
            return output
        qualify = getattr(self.qualification, "qualify_all", None)
        if callable(qualify):
            qualified = _call(qualify, feasibility, now=now, deadline_monotonic=deadline_monotonic)
            self._checkpoint(deadline_monotonic)
        else:
            qualified = ()
        action = getattr(self.qualification, "actionable_rankings", None)
        rows = _call(action, self.max_actionable, deadline_monotonic=deadline_monotonic) if callable(action) else qualified
        self._checkpoint(deadline_monotonic)
        return {"selection_status": "CURRENT" if rows else "NONE", "rankings": list(rows or ())}

    def _qualification_current(
        self,
        ranking: Mapping[str, Any],
        deadline_monotonic: float | None = None,
    ) -> bool:
        self._checkpoint(deadline_monotonic)
        status = str(ranking.get("selection_status", ranking.get("status", ""))).upper()
        if status in {"STALE", "NONE", "INVALID", "PAUSED"}:
            return False
        current = getattr(self.qualification, "status", None) if self.qualification is not None else None
        if current is not None:
            try:
                value = _call(current, deadline_monotonic=deadline_monotonic) if callable(current) else current
                self._checkpoint(deadline_monotonic)
                if isinstance(value, Mapping):
                    current_status = str(value.get("selection_status", value.get("status", ""))).upper()
                    if current_status and current_status not in {"CURRENT", "QUALIFIED"}:
                        return False
                elif value is not None:
                    current_status = str(value).upper()
                    if current_status and current_status not in {"CURRENT", "QUALIFIED"}:
                        return False
            except _DeadlineExpired:
                raise
            except BaseException as exc:
                if self._is_deadline_exception(exc):
                    raise _DeadlineExpired(AUTO_DEADLINE_REASON)
                return False
        self._checkpoint(deadline_monotonic)
        return True

    def _candidates(self, ranking: Mapping[str, Any]) -> list[dict[str, Any]]:
        rows: list[Any] = []
        for key in ("rankings", "actionable_rankings", "fallbacks"):
            values = ranking.get(key)
            if isinstance(values, (list, tuple)):
                rows.extend(values)
        selected = ranking.get("selected") or ranking.get("winner")
        if selected:
            rows.insert(0, selected)
        result: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()
        for raw in rows:
            if not isinstance(raw, Mapping):
                continue
            row = dict(raw)
            symbol = _symbol(row.get("symbol"))
            candidate = str(row.get("candidate_id", ""))
            if not symbol or not candidate:
                continue
            if row.get("qualified") is False or row.get("actionable") is False:
                continue
            key = (candidate, symbol)
            if key in seen:
                continue
            seen.add(key)
            row["symbol"] = symbol
            result.append(row)

        def order_key(row: Mapping[str, Any]) -> tuple[int, Decimal, str, str]:
            try:
                rank = int(row.get("rank") or 10**9)
            except (TypeError, ValueError, OverflowError):
                rank = 10**9
            return rank, -_score(row.get("total_score")), str(row.get("candidate_id")), str(row.get("symbol"))

        result.sort(key=order_key)
        # The qualification limit counts fallbacks, while ``selected`` /
        # ``winner`` is returned separately by the production ranker.  Keep
        # that selected row plus the bounded fallback set so an infeasible
        # winner cannot hide the next actionable candidate.  Without a
        # selection, retain the original actionable bound.
        candidate_limit = self.max_actionable + (1 if selected else 0)
        return result[:candidate_limit]

    # ---- cycle/run -------------------------------------------------------------
    def cycle(
        self,
        *,
        now: datetime | None = None,
        deadline_monotonic: float | None = None,
        symbol: str | None = None,
        entry_symbol: str | None = None,
    ) -> dict[str, Any]:
        requested_symbol = _symbol(symbol or entry_symbol) or None
        if not self._decision_lock.acquire(blocking=False):
            return {"status": "BUSY", "worker_name": self.worker_id, "reason": "CYCLE_ALREADY_IN_PROGRESS"}
        started = ensure_utc(now or self.clock())
        self._cycle_number += 1
        cycle_id = "cycle-" + uuid.uuid4().hex
        result: dict[str, Any] = {
            "schema_version": self.schema_version, "worker_name": self.worker_id, "cycle_id": cycle_id,
            "cycle_number": self._cycle_number, "symbol": requested_symbol, "deadline_monotonic": deadline_monotonic,
            "status": "NO_TRADE", "control_state": None,
            "entries": [], "exits": [], "events": [], "no_trade_reason": "NO_ACTIONABLE_SIGNAL",
            "pause_reason": None, "error": None, "provenance": {}, "ranking": {}, "reconciliation": {},
        }
        self._event_sink = []
        try:
            self._checkpoint(deadline_monotonic)
            # This is deliberately first, including DISABLED/DISARMED/KILLED.
            reconciliation, reconcile_reason = self._reconcile(started, deadline_monotonic)
            self._checkpoint(deadline_monotonic)
            result["reconciliation"] = reconciliation
            if reconcile_reason:
                result["pause_reason"] = reconcile_reason
            self._checkpoint(deadline_monotonic)
            result["control_state"] = self._control(deadline_monotonic).get("state")
            self._checkpoint(deadline_monotonic)
            snapshot = self._load_universe(started, deadline_monotonic)
            if snapshot is None:
                # Reconciliation/control have already run, but no market,
                # provider, or qualification service may be touched.
                if reconcile_reason:
                    result["status"] = "PAUSED"
                    return result
                result["status"] = "NO_TRADE"
                result["no_trade_reason"] = "NO_UNIVERSE"
                result["pause_reason"] = None
                result["error"] = None
                return result
            self._checkpoint(deadline_monotonic)
            connectivity_reason = self._connectivity(deadline_monotonic)
            if connectivity_reason and not result.get("pause_reason"):
                result["pause_reason"] = connectivity_reason
            positions = self._positions(deadline_monotonic)
            unresolved_exits = self._unresolved_exit_symbols(deadline_monotonic)
            exit_symbols = tuple(sorted(set(positions) - unresolved_exits))
            result["provenance"] = {
                "universe_id": snapshot.universe_id, "universe_version": snapshot.version,
                "snapshot_hash": snapshot.snapshot_hash, "dataset_version": snapshot.version,
                "status": snapshot.status, "selected_symbols": list(snapshot.selected_symbols),
            }
            records = self._collect(snapshot, exit_symbols, started, deadline_monotonic)
            self._checkpoint(deadline_monotonic)
            market_failures = [
                market for market in records
                if bool(_value(market, "error", default=None)) or bool(_value(market, "timed_out", default=False))
            ]
            if market_failures and not result.get("pause_reason"):
                result["pause_reason"] = "MARKET_COLLECTION_FAILURE"
                self._pause(result["pause_reason"])
            markets = self._market_map(records)

            # Exits are evaluated and submitted before any rank/entry work.
            for symbol in exit_symbols:
                self._checkpoint(deadline_monotonic)
                position = positions[symbol]
                position_symbol = _symbol(_value(position, "symbol", "pair", "market_symbol", default=""))
                binding = self._origin_binding(position)
                if not position_symbol or _symbol(binding.get("symbol")) != position_symbol:
                    reason = "EXIT_ORIGIN_SYMBOL_MISMATCH"
                    self._event(
                        cycle_id, "EXIT_EVALUATION", "NO_TRADE", now=started, symbol=symbol,
                        candidate_id=binding.get("candidate_id"), reason=reason,
                        payload={"position": position, "binding": binding},
                    )
                    result["exits"].append({
                        "symbol": symbol, "intent": "EXIT", "status": "NO_TRADE",
                        "result": {"status": "NO_TRADE", "reason": reason}, "reason": reason,
                    })
                    continue
                try:
                    strategy, origin_evidence = self._hydrate_origin(position, deadline_monotonic)
                    verified_binding = origin_evidence.get("binding", binding)
                    if _symbol(verified_binding.get("symbol")) != position_symbol:
                        raise ValueError("EXIT_ORIGIN_SYMBOL_MISMATCH")
                    market = markets.get(symbol)
                    if market is None:
                        reason = "EXIT_MARKET_EVIDENCE_UNAVAILABLE"
                        self._event(
                            cycle_id, "EXIT_EVALUATION", "BLOCKED", now=started, symbol=symbol,
                            candidate_id=verified_binding.get("candidate_id"), reason=reason,
                            payload={"position": position},
                        )
                        result["exits"].append({
                            "symbol": symbol, "intent": "EXIT", "status": "BLOCKED",
                            "result": {"status": "BLOCKED", "reason": reason}, "reason": reason,
                        })
                        continue
                    engine = self._engine(
                        verified_binding, position=position, intent="EXIT",
                        strategy=strategy, deadline_monotonic=deadline_monotonic,
                    )
                    signal, reason = self._evaluate(
                        engine, market, now=started, positions=positions,
                        deadline_monotonic=deadline_monotonic,
                    )
                    if signal is None:
                        reason = reason or "NO_SIGNAL"
                        self._event(
                            cycle_id, "EXIT_EVALUATION", "NO_TRADE", now=started, symbol=symbol,
                            candidate_id=verified_binding.get("candidate_id"), reason=reason,
                            payload={"binding": verified_binding, "position": position},
                        )
                        result["exits"].append({
                            "symbol": symbol, "intent": "EXIT", "status": "NO_TRADE",
                            "result": {"status": "NO_TRADE", "reason": reason}, "reason": reason,
                        })
                        continue
                    signal = self._attach_signal(
                        signal, binding=verified_binding, evidence=origin_evidence
                    )
                    signal_intent = str(signal.get("intent", "EXIT")).upper()
                    signal_side = str(signal.get("side", "SELL" if signal_intent == "EXIT" else "BUY")).upper()
                    if signal_intent != "EXIT" or signal_side != "SELL":
                        reason = "NON_EXIT_SIGNAL"
                        self._event(
                            cycle_id, "EXIT_EVALUATION", "NO_TRADE", now=started, symbol=symbol,
                            candidate_id=verified_binding.get("candidate_id"), reason=reason, payload=signal,
                        )
                        result["exits"].append({
                            "symbol": symbol, "intent": "EXIT", "status": "NO_TRADE",
                            "result": {"status": "NO_TRADE", "reason": reason}, "reason": reason,
                        })
                        continue
                    submitted, accepted = self._submit(
                        signal, market, position=position, now=started, cycle_id=cycle_id,
                        event_type="EXIT_SUBMIT", candidate_id=verified_binding.get("candidate_id"),
                        deadline_monotonic=deadline_monotonic,
                    )
                    result["exits"].append({
                        "symbol": symbol, "intent": "EXIT",
                        "status": "SUBMITTED" if accepted else "BLOCKED",
                        "result": submitted, "signal_id": signal.get("signal_id"),
                    })
                except _DeadlineExpired:
                    raise
                except ValueError as exc:
                    reason = str(exc) or "EXIT_ORIGIN_EVIDENCE_MISMATCH"
                    if not reason.startswith("EXIT_"):
                        reason = "EXIT_ORIGIN_" + reason
                    self._event(
                        cycle_id, "EXIT_EVALUATION", "NO_TRADE", now=started, symbol=symbol,
                        candidate_id=binding.get("candidate_id"), reason=reason,
                        payload={"error": str(exc), "binding": binding},
                    )
                    result["exits"].append({
                        "symbol": symbol, "intent": "EXIT", "status": "NO_TRADE",
                        "result": {"status": "NO_TRADE", "reason": reason}, "reason": reason,
                    })
                except BaseException as exc:
                    if self._is_deadline_exception(exc):
                        raise _DeadlineExpired(AUTO_DEADLINE_REASON)
                    reason = "EXIT_EVALUATION_" + type(exc).__name__.upper()
                    self._event(
                        cycle_id, "EXIT_EVALUATION", "BLOCKED", now=started, symbol=symbol,
                        candidate_id=binding.get("candidate_id"), reason=reason,
                        payload={"error": str(exc), "binding": binding},
                    )
                    result["exits"].append({
                        "symbol": symbol, "intent": "EXIT", "status": "BLOCKED",
                        "result": {"status": "BLOCKED", "reason": reason}, "reason": reason,
                    })

            self._checkpoint(deadline_monotonic)
            feasibility = {symbol: self._feasible(market) for symbol, market in markets.items()}
            self._checkpoint(deadline_monotonic)
            result["feasibility"] = feasibility
            try:
                ranking = self._rank(feasibility, started, deadline_monotonic)
            except BaseException as exc:
                if self._is_deadline_exception(exc):
                    raise _DeadlineExpired(AUTO_DEADLINE_REASON)
                self._pause("QUALIFICATION_TRANSPORT_" + type(exc).__name__.upper())
                ranking = {"selection_status": "PAUSED", "rankings": [], "reason": "QUALIFICATION_FAILURE", "error": type(exc).__name__}
                result["pause_reason"] = "QUALIFICATION_FAILURE"
            self._checkpoint(deadline_monotonic)
            result["ranking"] = _safe(ranking)
            self._checkpoint(deadline_monotonic)
            control = self._control(deadline_monotonic)
            self._checkpoint(deadline_monotonic)
            result["control_state"] = control.get("state")
            entry_gate_reason = None
            if result.get("pause_reason"):
                entry_gate_reason = str(result["pause_reason"])
            elif str(snapshot.status).upper() != "CURRENT":
                entry_gate_reason = "UNIVERSE_STALE"
            elif not self._qualification_current(ranking, deadline_monotonic):
                entry_gate_reason = "QUALIFICATION_SELECTION_NOT_CURRENT"
            elif str(control.get("state", DISABLED)).upper() != ARMED or not bool(control.get("authorized", False)):
                entry_gate_reason = "CONTROL_" + str(control.get("state", DISABLED)).upper()
            self._checkpoint(deadline_monotonic)
            entries_attempted = 0
            if entry_gate_reason is None:
                for row in self._candidates(ranking):
                    self._checkpoint(deadline_monotonic)
                    if entries_attempted >= self.max_entries_per_cycle:
                        break
                    candidate, symbol = str(row["candidate_id"]), _symbol(row["symbol"])
                    if requested_symbol is not None and symbol != requested_symbol:
                        continue
                    market = markets.get(symbol)
                    if symbol in positions:
                        reason = "OWNED_POSITION_NO_AVERAGING"
                    elif market is None:
                        reason = "MARKET_EVIDENCE_UNAVAILABLE"
                    elif not feasibility.get(symbol, False):
                        reason = "EXECUTION_EVIDENCE_INFEASIBLE"
                    else:
                        reason = ""
                    if reason:
                        self._event(
                            cycle_id, "ENTRY_EVALUATION", "NO_TRADE", now=started, symbol=symbol,
                            candidate_id=candidate, reason=reason, payload=row,
                        )
                        result["no_trade_reason"] = reason
                        continue
                    try:
                        binding, strategy, entry_evidence = self._hydrate_candidate(row, deadline_monotonic)
                        engine = self._engine(
                            binding, intent="ENTRY", row=row, strategy=strategy,
                            deadline_monotonic=deadline_monotonic,
                        )
                        signal, signal_reason = self._evaluate(
                            engine, market, now=started, positions=positions,
                            deadline_monotonic=deadline_monotonic,
                        )
                        if signal is None:
                            reason = signal_reason or "NO_SIGNAL"
                            self._event(
                                cycle_id, "ENTRY_EVALUATION", "NO_TRADE", now=started,
                                symbol=symbol, candidate_id=candidate, reason=reason, payload=row,
                            )
                            result["no_trade_reason"] = reason
                            continue
                        signal = self._attach_signal(signal, binding=binding, evidence=entry_evidence)
                        if str(signal.get("intent", "ENTRY")).upper() != "ENTRY" or str(signal.get("side", "BUY")).upper() != "BUY":
                            reason = "NON_ENTRY_SIGNAL"
                            self._event(
                                cycle_id, "ENTRY_EVALUATION", "NO_TRADE", now=started,
                                symbol=symbol, candidate_id=candidate, reason=reason, payload=signal,
                            )
                            result["no_trade_reason"] = reason
                            continue
                        self._event(
                            cycle_id, "ENTRY_EVALUATION", "EVALUATED", now=started,
                            symbol=symbol, candidate_id=candidate, signal_id=signal.get("signal_id"),
                            reason="SIGNAL_READY", payload=signal,
                        )
                        submitted, accepted = self._submit(
                            signal, market, now=started, cycle_id=cycle_id,
                            event_type="ENTRY_SUBMIT", candidate_id=candidate,
                            deadline_monotonic=deadline_monotonic,
                        )
                        if accepted:
                            entries_attempted += 1
                            result["entries"].append({
                                "candidate_id": candidate, "symbol": symbol, "intent": "ENTRY",
                                "status": "SUBMITTED", "result": submitted,
                                "signal_id": signal.get("signal_id"),
                            })
                            result["no_trade_reason"] = ""
                        else:
                            reason = str(submitted.get("reason", "ENTRY_REJECTED"))
                            if reason == "DUPLICATE_INTERVAL":
                                result["entries"].append({
                                    "candidate_id": candidate, "symbol": symbol, "intent": "ENTRY",
                                    "status": "DEDUPLICATED", "result": submitted,
                                    "signal_id": signal.get("signal_id"),
                                })
                                result["no_trade_reason"] = reason
                                break
                            result["no_trade_reason"] = reason
                    except _DeadlineExpired:
                        raise
                    except ValueError as exc:
                        reason = "ENTRY_EVIDENCE_" + (str(exc) or "MISMATCH")
                        self._event(
                            cycle_id, "ENTRY_EVALUATION", "NO_TRADE", now=started,
                            symbol=symbol, candidate_id=candidate, reason=reason,
                            payload={"error": str(exc), "row": row},
                        )
                        result["no_trade_reason"] = reason
                    except BaseException as exc:
                        if self._is_deadline_exception(exc):
                            raise _DeadlineExpired(AUTO_DEADLINE_REASON)
                        reason = "ENTRY_EVALUATION_" + type(exc).__name__.upper()
                        self._event(
                            cycle_id, "ENTRY_EVALUATION", "NO_TRADE", now=started,
                            symbol=symbol, candidate_id=candidate, reason=reason,
                            payload={"error": str(exc), "row": row},
                        )
                        result["no_trade_reason"] = reason
            else:
                result["no_trade_reason"] = entry_gate_reason
                self._event(
                    cycle_id, "ENTRY_GATE", "NO_TRADE", now=started,
                    reason=entry_gate_reason, payload={"control": control, "ranking": ranking},
                )
            if any(item.get("status") == "SUBMITTED" for item in result["entries"]) or any(item.get("status") == "SUBMITTED" for item in result["exits"]):
                result["status"] = "ACTIONED"
            elif result.get("pause_reason"):
                result["status"] = "PAUSED"
            else:
                result["status"] = "NO_TRADE"
        except _DeadlineExpired:
            result["status"] = "NO_TRADE"
            result["no_trade_reason"] = AUTO_DEADLINE_REASON
            result["pause_reason"] = None
            result["error"] = None
        except BaseException as exc:
            if self._is_deadline_exception(exc):
                result["status"] = "NO_TRADE"
                result["no_trade_reason"] = AUTO_DEADLINE_REASON
                result["pause_reason"] = None
                result["error"] = None
            else:
                result["status"] = "PAUSED"
                result["pause_reason"] = "CYCLE_" + type(exc).__name__.upper()
                result["error"] = type(exc).__name__
                self._pause(str(result["pause_reason"]))
        finally:
            completed = ensure_utc(self.clock())
            result["events"] = list(self._event_sink or ())
            self._event_sink = None
            try:
                self._persist_cycle(result, _iso(started), _iso(completed))
                self._persist_state(result)
            except Exception:
                # A persistence failure is visible in process state, but must
                # never make a long-running worker thread die.
                self._last_status = dict(result)
            self._decision_lock.release()
        return result

    def run(
        self,
        max_cycles: int | None = None,
        *,
        symbol: str | None = None,
        deadline_monotonic: float | None = None,
    ) -> list[dict[str, Any]]:
        if max_cycles is not None and (isinstance(max_cycles, bool) or int(max_cycles) < 0):
            raise ValueError("max_cycles must be non-negative or None")
        target = None if max_cycles is None else int(max_cycles)
        requested_symbol = _symbol(symbol) or None
        results: list[dict[str, Any]] = []
        while target is None or len(results) < target:
            if self._stop_event.is_set():
                break
            if self._deadline_expired(deadline_monotonic):
                results.append({
                    "status": "NO_TRADE",
                    "symbol": requested_symbol,
                    "deadline_monotonic": deadline_monotonic,
                    "no_trade_reason": AUTO_DEADLINE_REASON,
                })
                break
            try:
                results.append(self.cycle(
                    symbol=requested_symbol, deadline_monotonic=deadline_monotonic,
                ))
            except _DeadlineExpired:
                results.append({
                    "status": "NO_TRADE",
                    "symbol": requested_symbol,
                    "deadline_monotonic": deadline_monotonic,
                    "no_trade_reason": AUTO_DEADLINE_REASON,
                })
                break
            except BaseException as exc:
                if self._is_deadline_exception(exc):
                    results.append({
                        "status": "NO_TRADE",
                        "symbol": requested_symbol,
                        "deadline_monotonic": deadline_monotonic,
                        "no_trade_reason": AUTO_DEADLINE_REASON,
                    })
                    break
                self._pause("RUN_" + type(exc).__name__.upper())
                results.append({"status": "PAUSED", "reason": type(exc).__name__})
            if target is not None and len(results) >= target:
                break
            if self._stop_event.is_set() or self._deadline_expired(deadline_monotonic):
                break
            if self.interval_seconds > 0:
                sleep_seconds = self.interval_seconds
                if deadline_monotonic is not None:
                    try:
                        remaining = float(deadline_monotonic) - time.monotonic()
                    except (TypeError, ValueError, OverflowError):
                        remaining = 0.0
                    if remaining <= 0:
                        break
                    sleep_seconds = min(sleep_seconds, remaining)
                try:
                    self.sleeper(sleep_seconds)
                except BaseException as exc:
                    if self._is_deadline_exception(exc):
                        break
                    self._pause("SLEEP_" + type(exc).__name__.upper())
                    break
        return results

    def stop(self) -> None:
        self._stop_event.set()

    def status(self) -> dict[str, Any]:
        with self.store._lock:
            row = self.store.connection.execute("SELECT * FROM binance_auto_state WHERE singleton=1").fetchone()
            cycle = self.store.connection.execute("SELECT * FROM binance_auto_cycles WHERE worker_name=? ORDER BY cycle_number DESC,cycle_id DESC LIMIT 1", (self.worker_id,)).fetchone()
        result = dict(self._last_status)
        result.update({"worker_name": self.worker_id, "namespace": self.namespace, "schema_version": self.schema_version, "cycle_number": self._cycle_number})
        if row is not None:
            result.update({"status": row["status"], "no_trade_reason": row["no_trade_reason"], "pause_reason": row["pause_reason"], "updated_at": row["updated_at"]})
        if cycle is not None:
            result["last_cycle"] = {"cycle_id": cycle["cycle_id"], "status": cycle["status"], "started_at": cycle["started_at"], "completed_at": cycle["completed_at"], "entry_count": cycle["entry_count"], "exit_count": cycle["exit_count"], "no_trade_reason": cycle["no_trade_reason"], "error": cycle["error"]}
        return _safe(result)


__all__ = ["BinanceAutonomousWorker"]
