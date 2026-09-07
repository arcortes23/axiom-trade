"""Crypto-only Binance Spot execution qualification and deterministic ranking.

This module is deliberately separate from the prediction canary.  It reads
already-persisted lifecycle/evidence records and writes successor qualification
bindings; it never changes the source frozen lifecycle document and never
handles credentials or submits an order.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
import math
import re
import sqlite3
from typing import Any, Iterable, Mapping, Sequence

from .domain import utc_now
from .storage import AxiomStore

PAPER = "PAPER"
BINANCE_SPOT_TESTNET = "BINANCE_SPOT_TESTNET"
BINANCE_SPOT_LIVE = "BINANCE_SPOT_LIVE"
USDT = "USDT"

QUALIFICATION_SCHEMA_VERSION = "binance-crypto-execution-qualification-v1"
BINDING_SCHEMA_VERSION = "crypto-execution-binding-v1"
FORMULA_VERSION = "binance-crypto-ranking-v1"
POLICY_VERSION = "binance-crypto-execution-policy-v1"
MAX_FALLBACKS = 3

# Keys whose values are mutable operational observations, rather than immutable
# research evidence.  They must not make a binding or qualification hash move.
_TELEMETRY_WORDS = frozenset(
    {
        "telemetry", "heartbeat", "last_seen", "last_checked", "checked_at",
        "observed_at", "updated_at", "received_at", "latency", "uptime",
        "health", "status_detail", "current_price", "quote_balance",
        "available_balance", "connection", "request_id", "poll_count",
    }
)
_SECRET_WORDS = frozenset(
    {
        "secret", "credential", "credentials", "api_key", "apikey", "private_key",
        "password", "passphrase", "access_token", "refresh_token", "keyring",
    }
)
_FORBIDDEN_WORDS = frozenset(
    {
        "prediction", "probability", "settlement", "calibration", "yes", "no",
        "resolved_bet", "resolved_bets", "polymarket",
    }
)


def _jsonable(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        stamp = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
        return stamp.astimezone(timezone.utc).isoformat()
    if isinstance(value, Mapping):
        return {str(k): _jsonable(v) for k, v in sorted(value.items(), key=lambda item: str(item[0]))}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    if isinstance(value, (set, frozenset)):
        return sorted((_jsonable(v) for v in value), key=lambda item: repr(item))
    if hasattr(value, "value") and not isinstance(value, (str, bytes, bytearray)):
        return _jsonable(value.value)
    if hasattr(value, "to_dict") and callable(value.to_dict):
        try:
            return _jsonable(value.to_dict())
        except Exception:
            pass
    if hasattr(value, "__dataclass_fields__"):
        try:
            from dataclasses import asdict
            return _jsonable(asdict(value))
        except Exception:
            pass
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _canonical(value: Any) -> str:
    return json.dumps(_jsonable(value), sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def _hash(value: Any) -> str:
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _clean_key(key: Any) -> str:
    return str(key).strip().lower().replace("-", "_")


def _is_forbidden_key(key: Any) -> bool:
    name = _clean_key(key)
    if name in _TELEMETRY_WORDS:
        return True
    if any(word in name for word in _SECRET_WORDS):
        return True
    # Do not leak prediction-only concepts into this crypto projection.  A
    # regular field called ``no_credentials`` is retained because it is a
    # safety assertion, not a prediction outcome.
    if name == "no_credentials":
        return False
    return any(word in name for word in _FORBIDDEN_WORDS)


def _public(value: Any, *, telemetry: bool = False) -> Any:
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for key, child in value.items():
            name = str(key)
            lower = _clean_key(name)
            if any(word in lower for word in _SECRET_WORDS):
                continue
            if not telemetry and _is_forbidden_key(name):
                continue
            result[name] = _public(child, telemetry=telemetry)
        return result
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_public(item, telemetry=telemetry) for item in value]
    return _jsonable(value)


def _symbol(value: Any) -> str:
    return str(value or "").replace("/", "").replace("-", "").replace("_", "").strip().upper()


def _number(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _count(value: Any) -> int | None:
    number = _number(value)
    if number is None or number < 0 or not number.is_integer():
        return None
    return int(number)


def _truth(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, Mapping):
        for key in ("available", "feasible", "ready", "passed", "positive", "enabled", "valid"):
            if key in value:
                return _truth(value[key])
        return bool(value)
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"true", "yes", "pass", "passed", "ready", "available", "ok", "feasible"}:
            return True
        if text in {"false", "no", "fail", "failed", "blocked", "unavailable", "infeasible"}:
            return False
    if value is None:
        return None
    return bool(value)


def _lookup(sources: Sequence[Mapping[str, Any]], names: Sequence[str]) -> Any:
    wanted = {_clean_key(name) for name in names}
    for source in sources:
        for key, value in source.items():
            if _clean_key(key) in wanted:
                return value
    return None


def _maps(value: Any) -> Iterable[Mapping[str, Any]]:
    if isinstance(value, Mapping):
        yield value


@dataclass(frozen=True, slots=True)
class CryptoQualificationPolicy:
    """Explicit, serializable gates for crypto execution qualification."""

    max_drawdown: Decimal = Decimal("0.20")
    min_samples: int = 30
    min_trades: int = 3
    min_walk_forward_consistency: Decimal = Decimal("0.60")
    min_neighbor_stability: Decimal = Decimal("0.60")
    max_fallbacks: int = MAX_FALLBACKS
    policy_version: str = POLICY_VERSION
    formula_version: str = FORMULA_VERSION

    def __post_init__(self) -> None:
        if isinstance(self.min_samples, bool) or self.min_samples < 1:
            raise ValueError("min_samples must be positive")
        if isinstance(self.min_trades, bool) or self.min_trades < 1:
            raise ValueError("min_trades must be positive")
        if self.max_fallbacks < 0:
            raise ValueError("max_fallbacks must be non-negative")
        for name in ("max_drawdown", "min_walk_forward_consistency", "min_neighbor_stability"):
            value = Decimal(str(getattr(self, name)))
            if not value.is_finite() or value < 0:
                raise ValueError(f"{name} must be finite and non-negative")
        if self.max_drawdown > 1:
            raise ValueError("max_drawdown must be <= 1")

    def as_dict(self) -> dict[str, Any]:
        return {
            "max_drawdown": str(self.max_drawdown),
            "min_samples": self.min_samples,
            "min_trades": self.min_trades,
            "min_walk_forward_consistency": str(self.min_walk_forward_consistency),
            "min_neighbor_stability": str(self.min_neighbor_stability),
            "max_fallbacks": self.max_fallbacks,
            "policy_version": self.policy_version,
            "formula_version": self.formula_version,
        }


@dataclass(frozen=True, slots=True)
class CryptoExecutionBinding:
    """The non-secret immutable successor binding to a frozen candidate."""

    candidate_id: str
    symbol: str
    frozen_hash: str
    strategy_hash: str
    model_hash: str
    config_hash: str
    plan_hash: str
    universe_id: str
    universe_version: str
    universe_snapshot: str
    asset_symbol_mapping: Mapping[str, str]
    dataset_id: str
    dataset_version: str
    timeframe: str
    source: str
    quality: str
    survivorship: str
    environment: str
    venue: str
    adapter_version: str
    policy_version: str = POLICY_VERSION
    formula_version: str = FORMULA_VERSION
    schema_version: str = BINDING_SCHEMA_VERSION

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "candidate_id": self.candidate_id,
            "symbol": self.symbol,
            "frozen_hash": self.frozen_hash,
            "strategy_hash": self.strategy_hash,
            "model_hash": self.model_hash,
            "config_hash": self.config_hash,
            "plan_hash": self.plan_hash,
            "universe_id": self.universe_id,
            "universe_version": self.universe_version,
            "universe_snapshot": self.universe_snapshot,
            "universe_snapshot_hash": self.universe_snapshot,
            "asset_symbol_mapping": dict(self.asset_symbol_mapping),
            "dataset_id": self.dataset_id,
            "dataset_version": self.dataset_version,
            "timeframe": self.timeframe,
            "source": self.source,
            "source_type": self.source,
            "quality": self.quality,
            "survivorship": self.survivorship,
            "environment": self.environment,
            "venue": self.venue,
            "adapter_version": self.adapter_version,
            "policy_version": self.policy_version,
            "formula_version": self.formula_version,
            "no_credentials": True,
        }

    @property
    def binding_hash(self) -> str:
        return _hash(self.as_dict())


class _QualificationRows(list):
    """List result with small mapping conveniences for downstream callers."""

    def __init__(self, rows: Iterable[Mapping[str, Any]], **meta: Any) -> None:
        super().__init__(dict(row) for row in rows)
        self.meta = meta

    def get(self, key: str, default: Any = None) -> Any:
        if key in self.meta:
            return self.meta[key]
        return default


class BinanceCryptoQualificationService:
    """Read frozen crypto research and rank executable Binance Spot pairs."""

    WEIGHTS = {
        "net_expectancy": 0.24,
        "drawdown": 0.12,
        "sample": 0.10,
        "trades": 0.08,
        "walk_forward": 0.14,
        "neighbor_stability": 0.12,
        "cost_slippage_stress": 0.10,
        "execution_feasibility": 0.05,
        "forward_paper": 0.05,
    }
    _STAGES = frozenset({"FROZEN"})

    def __init__(
        self,
        store: AxiomStore,
        *,
        policy: CryptoQualificationPolicy | None = None,
        clock=utc_now,
        venue: str = "BINANCE_SPOT",
        adapter_version: str = "unknown",
    ) -> None:
        self.store = store
        self.policy = policy or CryptoQualificationPolicy()
        self.clock = clock
        self.default_venue = str(venue)
        self.default_adapter_version = str(adapter_version)
        self._ensure_schema()

    # Schema is intentionally owned here rather than added to the generic
    # store's schema: this keeps crypto execution additive and allows old
    # stores to restart without migrating unrelated canary tables.
    def _ensure_schema(self) -> None:
        with self.store._lock:
            conn = self.store.connection
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS binance_crypto_qualification (
                    candidate_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    qualification_hash TEXT NOT NULL,
                    binding_hash TEXT NOT NULL,
                    binding_json TEXT NOT NULL,
                    raw_metrics_json TEXT NOT NULL,
                    gates_json TEXT NOT NULL,
                    reasons_json TEXT NOT NULL,
                    immutable_hashes_json TEXT NOT NULL,
                    qualified INTEGER NOT NULL,
                    lifecycle_stage TEXT NOT NULL,
                    evaluated_at TEXT NOT NULL,
                    PRIMARY KEY(candidate_id, symbol)
                );
                CREATE INDEX IF NOT EXISTS idx_binance_crypto_qualification_qualified
                    ON binance_crypto_qualification(qualified, candidate_id, symbol);
                CREATE TABLE IF NOT EXISTS binance_crypto_rankings (
                    candidate_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    ranking_run_id TEXT NOT NULL,
                    ranking_timestamp TEXT NOT NULL,
                    rank INTEGER NOT NULL,
                    total_score REAL,
                    raw_metrics_json TEXT NOT NULL,
                    component_scores_json TEXT NOT NULL,
                    weights_json TEXT NOT NULL,
                    tie_breaks_json TEXT NOT NULL,
                    binding_json TEXT NOT NULL,
                    evidence_hashes_json TEXT NOT NULL,
                    cluster_key TEXT NOT NULL,
                    cluster_representative INTEGER NOT NULL,
                    selected INTEGER NOT NULL,
                    actionable INTEGER NOT NULL,
                    reason TEXT NOT NULL,
                    binding_hash TEXT NOT NULL,
                    qualification_hash TEXT NOT NULL,
                    PRIMARY KEY(candidate_id, symbol)
                );
                CREATE INDEX IF NOT EXISTS idx_binance_crypto_rankings_order
                    ON binance_crypto_rankings(rank, total_score DESC, candidate_id, symbol);
                CREATE TABLE IF NOT EXISTS binance_crypto_selection (
                    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                    ranking_run_id TEXT NOT NULL,
                    ranking_timestamp TEXT NOT NULL,
                    candidate_id TEXT,
                    symbol TEXT,
                    rank INTEGER,
                    total_score REAL,
                    selection_json TEXT NOT NULL,
                    selection_status TEXT NOT NULL,
                    selection_valid INTEGER NOT NULL,
                    selection_invalidation_reason TEXT,
                    last_selected_candidate TEXT,
                    last_selected_symbol TEXT,
                    fallbacks_json TEXT NOT NULL,
                    feasibility_json TEXT NOT NULL
                );
                """
            )
            conn.commit()

    @staticmethod
    def _context(payload: Mapping[str, Any], *, pair: Mapping[str, Any] | None = None) -> list[Mapping[str, Any]]:
        result: list[Mapping[str, Any]] = []
        if pair:
            result.append(pair)
        result.append(payload)
        for key in (
            "frozen", "strategy", "model", "config", "experiment_plan", "plan",
            "metrics", "validation", "robustness", "execution_metrics", "execution",
            "paper_forward", "forward_evidence", "forward_paper",
        ):
            value = payload.get(key)
            if isinstance(value, Mapping):
                result.append(value)
        return result

    @classmethod
    def _find_market_type(cls, payload: Mapping[str, Any]) -> str:
        value = _lookup(cls._context(payload), ("market_type", "market", "instrument_type"))
        if isinstance(value, Mapping):
            value = value.get("market_type", value.get("type"))
        return str(value or "").strip().lower()

    @classmethod
    def _extract_mapping(cls, payload: Mapping[str, Any]) -> dict[str, str]:
        sources = cls._context(payload)
        raw = _lookup(sources, ("asset_symbol_mapping", "symbol_mapping", "asset_to_symbol", "symbols"))
        result: dict[str, str] = {}
        if isinstance(raw, Mapping):
            for asset, value in raw.items():
                if isinstance(value, Mapping):
                    value = value.get("binance_symbol", value.get("symbol"))
                if isinstance(value, (list, tuple)):
                    for child in value:
                        normalized = _symbol(child)
                        if normalized:
                            result[str(asset)] = normalized
                            break
                else:
                    normalized = _symbol(value)
                    if normalized:
                        result[str(asset)] = normalized
        elif isinstance(raw, (list, tuple, set, frozenset)):
            for value in raw:
                if isinstance(value, Mapping):
                    asset = value.get("asset", value.get("asset_id", value.get("base_asset", value.get("symbol"))))
                    symbol = value.get("binance_symbol", value.get("symbol"))
                    if symbol:
                        result[str(asset or symbol)] = _symbol(symbol)
                else:
                    normalized = _symbol(value)
                    if normalized:
                        result[normalized.removesuffix(USDT)] = normalized
        if not result:
            for key in ("universe", "universe_snapshot", "universe_records"):
                rows = payload.get(key)
                if isinstance(rows, Mapping):
                    rows = rows.get("records", rows.get("assets", []))
                if isinstance(rows, (list, tuple)):
                    for item in rows:
                        if not isinstance(item, Mapping):
                            continue
                        if item.get("selected") is False:
                            continue
                        asset = item.get("asset_id", item.get("asset", item.get("base_asset", item.get("id"))))
                        symbol = item.get("binance_symbol", item.get("symbol"))
                        if symbol:
                            result[str(asset or symbol)] = _symbol(symbol)
        if not result:
            one = _lookup(sources, ("binance_symbol", "symbol", "instrument"))
            if one:
                normalized = _symbol(one)
                result[normalized.removesuffix(USDT)] = normalized
        return {key: value for key, value in sorted(result.items()) if value}

    @staticmethod
    def _pair_payload(payload: Mapping[str, Any], symbol: str) -> dict[str, Any]:
        body = dict(payload)
        normalized = _symbol(symbol)
        for key in ("metrics_by_symbol", "symbol_metrics", "per_symbol", "execution_metrics_by_symbol"):
            mapping = payload.get(key)
            if isinstance(mapping, Mapping):
                for candidate, child in mapping.items():
                    if _symbol(candidate) == normalized and isinstance(child, Mapping):
                        body.update(child)
        metrics = payload.get("metrics")
        if isinstance(metrics, Mapping):
            for candidate, child in metrics.items():
                if _symbol(candidate) == normalized and isinstance(child, Mapping):
                    body.update(child)
        return body

    @staticmethod
    def _dataset_values(payload: Mapping[str, Any], catalog: Mapping[str, Any] | None) -> dict[str, str]:
        metadata = catalog.get("metadata", {}) if isinstance(catalog, Mapping) else {}
        metadata = metadata if isinstance(metadata, Mapping) else {}
        sources = [payload, metadata]
        return {
            "dataset_id": str(_lookup(sources, ("dataset_id", "data_id")) or ""),
            "dataset_version": str(_lookup(sources, ("dataset_version", "data_version", "version")) or ""),
            "timeframe": str(_lookup(sources, ("timeframe", "interval")) or (catalog or {}).get("timeframe", "")),
            "source": str(_lookup(sources, ("source", "source_type", "data_source")) or (catalog or {}).get("source_type", "")),
            "quality": str(_lookup(sources, ("quality", "data_quality")) or (catalog or {}).get("quality", "")),
            "survivorship": str(_lookup(sources, ("survivorship", "survivorship_policy")) or ""),
        }

    @staticmethod
    def _universe_values(payload: Mapping[str, Any], catalog: Mapping[str, Any] | None) -> dict[str, str]:
        metadata = catalog.get("metadata", {}) if isinstance(catalog, Mapping) else {}
        metadata = metadata if isinstance(metadata, Mapping) else {}
        universe = payload.get("universe")
        universe = universe if isinstance(universe, Mapping) else {}
        sources = [payload, universe, metadata]
        snapshot = _lookup(sources, ("universe_snapshot", "snapshot_hash", "universe_snapshot_hash", "content_hash"))
        if isinstance(snapshot, Mapping):
            snapshot = snapshot.get("snapshot_hash", snapshot.get("content_hash", snapshot.get("snapshot_id")))
        return {
            "universe_id": str(_lookup(sources, ("universe_id", "universe")) or ""),
            "universe_version": str(_lookup(sources, ("universe_version", "universe_snapshot_version", "version")) or ""),
            "universe_snapshot": str(snapshot or (catalog or {}).get("snapshot_id", "") or ""),
        }

    def _catalog(self, dataset_id: str, version: str) -> Mapping[str, Any] | None:
        loader = getattr(self.store, "load_dataset_catalog", None)
        if not callable(loader) or not dataset_id or not version:
            return None
        value = loader(dataset_id, version)
        return value if isinstance(value, Mapping) else None

    def _binding_for(
        self,
        candidate_id: str,
        symbol: str,
        payload: Mapping[str, Any],
        mapping: Mapping[str, str],
    ) -> tuple[CryptoExecutionBinding | None, list[str], dict[str, str]]:
        reasons: list[str] = []
        context = self._context(payload)
        dataset_hint = str(_lookup(context, ("dataset_id", "data_id")) or "")
        version_hint = str(_lookup(context, ("dataset_version", "data_version")) or "")
        catalog = self._catalog(dataset_hint, version_hint)
        dataset = self._dataset_values(payload, catalog)
        if not dataset["dataset_id"] or not dataset["dataset_version"]:
            reasons.append("DATASET_REFERENCE_MISSING")
        elif catalog is None:
            reasons.append("DATASET_NOT_FOUND")
        else:
            expected_market = str(catalog.get("market_type", "")).lower()
            if expected_market and expected_market != "crypto_spot":
                reasons.append("DATASET_NOT_CRYPTO_SPOT")
            for field, catalog_field in (("dataset_version", "dataset_version"), ("timeframe", "timeframe"), ("source", "source_type")):
                expected = dataset[field]
                actual = str(catalog.get(catalog_field, ""))
                if expected and actual and expected != actual:
                    reasons.append("DATASET_EVIDENCE_CHANGED")
            if not dataset["timeframe"] or not dataset["source"] or not dataset["quality"] or not dataset["survivorship"]:
                reasons.append("DATASET_METADATA_MISSING")

        # Hash the exact persisted catalog and its reconstructed records.  The
        # hash is stored beside the successor binding, so a changed immutable
        # dataset or universe invalidates a previously selected pair even when
        # its lifecycle payload has not changed.
        evidence_hash = ""
        if catalog is not None:
            evidence_body: dict[str, Any] = {"catalog": _public(catalog)}
            loader = getattr(self.store, "load_dataset_record", None)
            if callable(loader):
                try:
                    record = loader(dataset["dataset_id"], dataset["dataset_version"])
                except Exception:
                    record = None
                if isinstance(record, Mapping):
                    evidence_body["records"] = _public(record.get("records", []))
            evidence_hash = _hash(evidence_body)

        universe = self._universe_values(payload, catalog)
        if not all(universe.values()):
            reasons.append("UNIVERSE_REFERENCE_MISSING")
        elif catalog is not None:
            metadata = catalog.get("metadata", {})
            metadata = metadata if isinstance(metadata, Mapping) else {}
            for field in ("universe_id", "universe_version", "universe_snapshot"):
                actual = str(metadata.get(field, metadata.get("snapshot_hash" if field == "universe_snapshot" else field, "")) or "")
                if actual and actual != universe[field]:
                    reasons.append("UNIVERSE_EVIDENCE_CHANGED")

        def hash_value(*names: str) -> str:
            value = _lookup(context, names)
            return str(value).strip() if value not in (None, "") else ""

        frozen_hash = hash_value("frozen_hash")
        strategy_hash = hash_value("strategy_hash")
        model_hash = hash_value("model_hash")
        config_hash = hash_value("config_hash")
        plan_hash = hash_value("plan_hash", "experiment_plan_hash")
        for name, value in (("FROZEN_HASH_MISSING", frozen_hash), ("STRATEGY_HASH_MISSING", strategy_hash),
                            ("MODEL_HASH_MISSING", model_hash), ("CONFIG_HASH_MISSING", config_hash),
                            ("PLAN_HASH_MISSING", plan_hash)):
            if not value:
                reasons.append(name)

        strategy_id = str(_lookup(context, ("strategy_id",)) or "")
        strategy_version = str(_lookup(context, ("strategy_version",)) or "")
        strategy_evidence_hash = ""
        if strategy_id and strategy_hash:
            loaded = getattr(self.store, "load_strategy", lambda *_args, **_kwargs: None)(strategy_id, strategy_version or None)
            if loaded is None:
                reasons.append("STRATEGY_NOT_FOUND")
            else:
                loaded_digest = _hash(loaded)
                public_digest = _hash(_public(loaded))
                strategy_evidence_hash = public_digest
                loaded_hashes = {
                    loaded_digest,
                    "sha256:" + loaded_digest,
                    public_digest,
                    "sha256:" + public_digest,
                }
                if strategy_hash not in loaded_hashes:
                    reasons.append("STRATEGY_EVIDENCE_CHANGED")

        # When executable documents are carried in the frozen payload, verify
        document_checks = (
            ("strategy_hash", ("strategy_document",), "STRATEGY_EVIDENCE_CHANGED"),
            ("model_hash", ("model_document", "model"), "MODEL_EVIDENCE_CHANGED"),
            ("config_hash", ("config", "forward_config"), "CONFIG_EVIDENCE_CHANGED"),
            ("plan_hash", ("experiment_plan", "plan"), "PLAN_EVIDENCE_CHANGED"),
        )
        for hash_name, document_names, reason in document_checks:
            declared = {
                "strategy_hash": strategy_hash,
                "model_hash": model_hash,
                "config_hash": config_hash,
                "plan_hash": plan_hash,
            }[hash_name]
            document = _lookup(context, document_names)
            if declared and isinstance(document, Mapping):
                digest = _hash(document)
                if declared not in {digest, "sha256:" + digest}:
                    reasons.append(reason)

        environment = str(_lookup(context, ("environment", "execution_environment")) or PAPER)
        venue = str(_lookup(context, ("venue", "exchange", "execution_venue")) or self.default_venue)
        adapter_version = str(_lookup(context, ("adapter_version", "binance_adapter_version")) or self.default_adapter_version)
        if not mapping or symbol not in mapping.values():
            reasons.append("ASSET_SYMBOL_MAPPING_MISSING")
        binding = None
        if not reasons:
            binding = CryptoExecutionBinding(
                candidate_id=candidate_id,
                symbol=symbol,
                frozen_hash=frozen_hash,
                strategy_hash=strategy_hash,
                model_hash=model_hash,
                config_hash=config_hash,
                plan_hash=plan_hash,
                universe_id=universe["universe_id"],
                universe_version=universe["universe_version"],
                universe_snapshot=universe["universe_snapshot"],
                asset_symbol_mapping=mapping,
                dataset_id=dataset["dataset_id"],
                dataset_version=dataset["dataset_version"],
                timeframe=dataset["timeframe"],
                source=dataset["source"],
                quality=dataset["quality"],
                survivorship=dataset["survivorship"],
                environment=environment,
                venue=venue,
                adapter_version=adapter_version,
                policy_version=self.policy.policy_version,
                formula_version=self.policy.formula_version,
            )
        catalog_universe: dict[str, str] = {}
        if isinstance(catalog, Mapping):
            metadata = catalog.get("metadata", {})
            metadata = metadata if isinstance(metadata, Mapping) else {}
            catalog_universe = {
                "universe_id": str(metadata.get("universe_id", "") or ""),
                "universe_version": str(metadata.get("universe_version", "") or ""),
                "universe_snapshot": str(
                    metadata.get(
                        "universe_snapshot",
                        metadata.get("snapshot_hash", metadata.get("universe_snapshot_hash", metadata.get("content_hash", ""))),
                    )
                    or ""
                ),
            }
        universe_evidence_hash = _hash(
            {"universe": universe, "catalog_universe": catalog_universe}
        ) if universe["universe_id"] else ""
        hashes = {
            "frozen_hash": frozen_hash,
            "strategy_hash": strategy_hash,
            "strategy_evidence_hash": strategy_evidence_hash,
            "model_hash": model_hash,
            "config_hash": config_hash,
            "plan_hash": plan_hash,
            "dataset_id": dataset["dataset_id"],
            "dataset_version": dataset["dataset_version"],
            "dataset_evidence_hash": evidence_hash,
            "universe_id": universe["universe_id"],
            "universe_version": universe["universe_version"],
            "universe_snapshot": universe["universe_snapshot"],
            "universe_evidence_hash": universe_evidence_hash,
        }
        return binding, list(dict.fromkeys(reasons)), hashes

    def _metrics(self, payload: Mapping[str, Any], symbol: str) -> dict[str, Any]:
        pair = self._pair_payload(payload, symbol)
        sources = self._context(pair, pair=pair)
        aliases = {
            "net_expectancy": ("net_expectancy_after_costs", "net_expectancy", "after_cost_expectancy", "expectancy_after_costs", "forward_net_expectancy"),
            "gross_expectancy": ("gross_expectancy", "expectancy"),
            "entry_cost": ("entry_cost", "entry_cost_bps", "entry_fee", "entry_fees"),
            "exit_cost": ("exit_cost", "exit_cost_bps", "exit_fee", "exit_fees"),
            "max_drawdown": ("max_drawdown", "drawdown", "forward_max_drawdown"),
            "samples": ("sample_count", "samples", "validation_sample_count", "forward_sample_count"),
            "trades": ("trade_count", "trades", "validation_trade_count", "forward_trade_count"),
            "walk_forward_consistency": ("walk_forward_consistency", "walk_forward", "walkforward_consistency"),
            "neighbor_stability": ("neighbor_stability", "neighboring_parameter_stability", "neighbour_stability"),
            "cost_slippage_stress": ("cost_slippage_stress", "cost_stress", "slippage_stress", "stress_net_expectancy"),
            "execution_feasibility": ("execution_feasibility", "current_execution_feasibility", "execution_ready"),
            "forward_paper_evidence": ("forward_paper_evidence", "paper_forward_evidence", "forward_evidence", "paper_evidence"),
            "locked_holdout_used": ("locked_holdout_used", "holdout_used"),
            "family": ("family", "strategy_family", "experiment_family"),
            "root_lineage": ("root_lineage", "root_lineage_id", "root_candidate_id", "lineage_root", "parent_id"),
        }
        output: dict[str, Any] = {}
        for name, names in aliases.items():
            value = _lookup(sources, names)
            if value is not None:
                output[name] = _public(value)
        # Explicit nested metrics are allowed to carry fields not covered by
        # aliases, but only non-prediction, non-telemetry public metrics enter
        # the immutable raw projection.
        metrics_source = pair.get("metrics")
        if isinstance(metrics_source, Mapping):
            for key, value in metrics_source.items():
                if not _is_forbidden_key(key) and _clean_key(key) not in _TELEMETRY_WORDS:
                    output.setdefault(str(key), _public(value))
        if "net_expectancy" not in output:
            gross = _number(output.get("gross_expectancy"))
            entry = _number(output.get("entry_cost"))
            exit_cost = _number(output.get("exit_cost"))
            if gross is not None and entry is not None and exit_cost is not None:
                output["net_expectancy"] = gross - entry - exit_cost
        return output


    @staticmethod
    def _feasibility(value: Any) -> bool | None:
        return _truth(value)

    def _feasibility_for(self, feasibility: Mapping[str, Any] | None, candidate_id: str, symbol: str, metrics: Mapping[str, Any]) -> bool | None:
        if isinstance(feasibility, Mapping):
            for key in (symbol, _symbol(symbol), candidate_id):
                if key in feasibility:
                    value = self._feasibility(feasibility[key])
                    if value is not None:
                        return value
            nested = feasibility.get(candidate_id)
            if isinstance(nested, Mapping):
                for key in (symbol, _symbol(symbol)):
                    if key in nested:
                        value = self._feasibility(nested[key])
                        if value is not None:
                            return value
        return self._feasibility(metrics.get("execution_feasibility"))

    def _gates(self, metrics: Mapping[str, Any], *, feasibility: bool | None) -> tuple[dict[str, Any], list[str]]:
        reasons: list[str] = []
        net = _number(metrics.get("net_expectancy"))
        if net is None:
            gross = _number(metrics.get("gross_expectancy"))
            entry = _number(metrics.get("entry_cost"))
            exit_cost = _number(metrics.get("exit_cost"))
            if gross is not None and entry is not None and exit_cost is not None:
                net = gross - entry - exit_cost
                metrics = dict(metrics)
                metrics["net_expectancy"] = net
        drawdown = _number(metrics.get("max_drawdown"))
        samples = _count(metrics.get("samples"))
        trades = _count(metrics.get("trades"))
        walk = metrics.get("walk_forward_consistency")
        neighbor = metrics.get("neighbor_stability")
        stress = metrics.get("cost_slippage_stress")
        forward = metrics.get("forward_paper_evidence")
        holdout = metrics.get("locked_holdout_used")

        def score(value: Any, threshold: Decimal) -> tuple[bool, float | bool | None]:
            if isinstance(value, bool):
                return value, value
            number = _number(value)
            if number is None:
                return False, None
            return number >= float(threshold), number

        walk_pass, walk_value = score(walk, self.policy.min_walk_forward_consistency)
        neighbor_pass, neighbor_value = score(neighbor, self.policy.min_neighbor_stability)
        stress_pass: bool
        stress_value: Any
        if isinstance(stress, Mapping):
            stress_value = stress.get("net_expectancy", stress.get("expectancy", stress.get("positive", stress.get("passed"))))
            if isinstance(stress_value, bool):
                stress_pass = stress_value
            else:
                number = _number(stress_value)
                stress_pass = number is not None and number > 0
                stress_value = number
        elif isinstance(stress, bool):
            stress_value, stress_pass = stress, stress
        else:
            stress_value = _number(stress)
            stress_pass = stress_value is not None and stress_value > 0
        forward_pass = _truth(forward)
        gates: dict[str, Any] = {
            "net_expectancy_after_costs": {"value": net, "passed": net is not None and net > 0},
            "bounded_drawdown": {"value": drawdown, "limit": str(self.policy.max_drawdown), "passed": drawdown is not None and 0 <= drawdown <= float(self.policy.max_drawdown)},
            "minimum_samples": {"value": samples, "limit": self.policy.min_samples, "passed": samples is not None and samples >= self.policy.min_samples},
            "minimum_trades": {"value": trades, "limit": self.policy.min_trades, "passed": trades is not None and trades >= self.policy.min_trades},
            "walk_forward_consistency": {"value": walk_value, "limit": str(self.policy.min_walk_forward_consistency), "passed": walk_pass},
            "neighbor_stability": {"value": neighbor_value, "limit": str(self.policy.min_neighbor_stability), "passed": neighbor_pass},
            "cost_slippage_stress": {"value": stress_value, "passed": stress_pass},
            "current_execution_feasibility": {"value": feasibility, "passed": feasibility is True},
            "forward_paper_evidence": {"value": _public(forward), "passed": forward_pass is True},
            "locked_holdout_unused": {"value": holdout, "passed": holdout is False},
        }
        code_by_gate = {
            "net_expectancy_after_costs": "NET_EXPECTANCY_NOT_POSITIVE",
            "bounded_drawdown": "DRAWDOWN_UNBOUNDED",
            "minimum_samples": "INSUFFICIENT_SAMPLES",
            "minimum_trades": "INSUFFICIENT_TRADES",
            "walk_forward_consistency": "WALK_FORWARD_INCONSISTENT",
            "neighbor_stability": "NEIGHBOR_UNSTABLE",
            "cost_slippage_stress": "COST_SLIPPAGE_STRESS_FAILED",
            "current_execution_feasibility": "EXECUTION_INFEASIBLE",
            "forward_paper_evidence": "FORWARD_PAPER_EVIDENCE_MISSING",
            "locked_holdout_unused": "LOCKED_HOLDOUT_USED_OR_UNDECLARED",
        }
        for name, gate in gates.items():
            if not gate["passed"]:
                reasons.append(code_by_gate[name])
        return gates, reasons

    def _evaluate_pair(
        self,
        record: Mapping[str, Any],
        symbol: str,
        mapping: Mapping[str, str],
        *,
        feasibility: Mapping[str, Any] | None,
        evaluated_at: str,
    ) -> dict[str, Any]:
        candidate_id = str(record.get("candidate_id") or "").strip()
        payload = record.get("payload")
        payload = payload if isinstance(payload, Mapping) else {}
        reasons: list[str] = []
        if str(record.get("stage") or "") not in self._STAGES:
            reasons.append("LIFECYCLE_NOT_FROZEN")
        if self._find_market_type(payload) != "crypto_spot":
            reasons.append("CRYPTO_SPOT_ONLY")
        binding, binding_reasons, immutable_hashes = self._binding_for(candidate_id, symbol, payload, mapping)
        immutable_hashes = {
            **immutable_hashes,
            # Lifecycle payloads are immutable research evidence after the
            # mutable telemetry and secret projections have been removed.
            "lifecycle_evidence_hash": _hash(_public(payload)),
        }
        reasons.extend(binding_reasons)
        metrics = self._metrics(payload, symbol)
        feasible = self._feasibility_for(feasibility, candidate_id, symbol, metrics)
        gates, gate_reasons = self._gates(metrics, feasibility=feasible)
        reasons.extend(gate_reasons)
        binding_dict = binding.as_dict() if binding is not None else {
            "schema_version": BINDING_SCHEMA_VERSION,
            "candidate_id": candidate_id,
            "symbol": symbol,
            "no_credentials": True,
        }
        binding_hash = binding.binding_hash if binding is not None else _hash(binding_dict)
        immutable_projection = {
            "candidate_id": candidate_id,
            "symbol": symbol,
            "binding": binding_dict,
            "raw_metrics": _public(metrics),
            "gates": _public(gates),
            "policy_version": self.policy.policy_version,
            "formula_version": self.policy.formula_version,
        }
        qualification_hash = _hash(immutable_projection)
        unique_reasons = list(dict.fromkeys(reasons))
        return {
            "schema_version": QUALIFICATION_SCHEMA_VERSION,
            "candidate_id": candidate_id,
            "symbol": symbol,
            "binding": binding_dict,
            "binding_hash": binding_hash,
            "qualification_hash": qualification_hash,
            "raw_metrics": _public(metrics),
            "gates": _public(gates),
            "immutable_hashes": immutable_hashes,
            "qualified": not unique_reasons,
            "reasons": unique_reasons,
            "reason": unique_reasons[0] if unique_reasons else "QUALIFIED",
            "lifecycle_stage": str(record.get("stage") or ""),
            "evaluated_at": evaluated_at,
            "no_credentials": True,
        }

    @staticmethod
    def _records(store: AxiomStore) -> list[Mapping[str, Any]]:
        records = store.load_candidate_lifecycle(limit=10000)
        return [item for item in records if isinstance(item, Mapping)] if isinstance(records, list) else []

    def _persist_qualifications(self, rows: Sequence[Mapping[str, Any]]) -> None:
        with self.store._lock:
            conn = self.store.connection
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.executemany(
                    "INSERT INTO binance_crypto_qualification(candidate_id,symbol,qualification_hash,binding_hash,binding_json,raw_metrics_json,gates_json,reasons_json,immutable_hashes_json,qualified,lifecycle_stage,evaluated_at) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(candidate_id,symbol) DO UPDATE SET qualification_hash=excluded.qualification_hash,binding_hash=excluded.binding_hash,binding_json=excluded.binding_json,raw_metrics_json=excluded.raw_metrics_json,gates_json=excluded.gates_json,reasons_json=excluded.reasons_json,immutable_hashes_json=excluded.immutable_hashes_json,qualified=excluded.qualified,lifecycle_stage=excluded.lifecycle_stage,evaluated_at=excluded.evaluated_at",
                    [
                        (
                            row["candidate_id"], row["symbol"], row["qualification_hash"], row["binding_hash"],
                            _canonical(row["binding"]), _canonical(row["raw_metrics"]), _canonical(row["gates"]),
                            _canonical(row["reasons"]), _canonical(row["immutable_hashes"]), int(bool(row["qualified"])),
                            row["lifecycle_stage"], row["evaluated_at"],
                        )
                        for row in rows
                    ],
                )
                conn.commit()
            except BaseException:
                if conn.in_transaction:
                    conn.rollback()
                raise

    def qualify_all(self, feasibility_by_symbol: Mapping[str, Any] | None = None, *, now: datetime | None = None) -> _QualificationRows:
        evaluated_at = (now or self.clock()).astimezone(timezone.utc).isoformat()
        rows: list[dict[str, Any]] = []
        for record in self._records(self.store):
            payload = record.get("payload")
            if not isinstance(payload, Mapping):
                continue
            mapping = self._extract_mapping(payload)
            symbols = sorted(set(mapping.values()))
            if not symbols:
                symbols = [_symbol(payload.get("symbol") or payload.get("binance_symbol")) or ""]
            for symbol in symbols:
                rows.append(self._evaluate_pair(record, symbol, mapping, feasibility=feasibility_by_symbol, evaluated_at=evaluated_at))
        rows.sort(key=lambda row: (str(row["candidate_id"]), str(row["symbol"])))
        self._persist_qualifications(rows)
        return _QualificationRows(rows, evaluated_at=evaluated_at, qualified_count=sum(bool(row["qualified"]) for row in rows))

    @classmethod
    def _raw_number(cls, row: Mapping[str, Any], key: str) -> float | None:
        return _number((row.get("raw_metrics") or {}).get(key)) if isinstance(row.get("raw_metrics"), Mapping) else None

    def _rank_item(self, row: Mapping[str, Any]) -> dict[str, Any]:
        metrics = row.get("raw_metrics") if isinstance(row.get("raw_metrics"), Mapping) else {}
        gates = row.get("gates") if isinstance(row.get("gates"), Mapping) else {}
        net = _number(metrics.get("net_expectancy")) or 0.0
        dd = _number(metrics.get("max_drawdown"))
        samples = _number(metrics.get("samples")) or 0.0
        trades = _number(metrics.get("trades")) or 0.0
        walk = _number(metrics.get("walk_forward_consistency"))
        neighbor = _number(metrics.get("neighbor_stability"))
        stress = _number(metrics.get("cost_slippage_stress"))
        components = {
            "net_expectancy": max(0.0, min(1.0, 0.5 + net / 2.0)),
            "drawdown": max(0.0, min(1.0, 1.0 - (dd or float(self.policy.max_drawdown)) / float(self.policy.max_drawdown))),
            "sample": max(0.0, min(1.0, samples / max(1.0, self.policy.min_samples * 2))),
            "trades": max(0.0, min(1.0, trades / max(1.0, self.policy.min_trades * 2))),
            "walk_forward": max(0.0, min(1.0, walk if walk is not None else 0.0)),
            "neighbor_stability": max(0.0, min(1.0, neighbor if neighbor is not None else 0.0)),
            "cost_slippage_stress": max(0.0, min(1.0, 0.5 + (stress or 0.0) / 2.0)),
            "execution_feasibility": 1.0 if bool((gates.get("current_execution_feasibility") or {}).get("passed")) else 0.0,
            "forward_paper": 1.0 if bool((gates.get("forward_paper_evidence") or {}).get("passed")) else 0.0,
        }
        weights = dict(self.WEIGHTS)
        score = sum(components[key] * weights[key] for key in components) / sum(weights.values())
        tie_breaks = ["total_score_desc", "net_expectancy_desc", "walk_forward_desc", "neighbor_stability_desc", "candidate_id_asc", "symbol_asc"]
        cluster_material = {
            "family": row.get("binding", {}).get("family", "") if isinstance(row.get("binding"), Mapping) else "",
            "root_lineage": row.get("binding", {}).get("root_lineage", "") if isinstance(row.get("binding"), Mapping) else "",
            "dataset_id": (row.get("binding", {}) or {}).get("dataset_id", ""),
            "dataset_version": (row.get("binding", {}) or {}).get("dataset_version", ""),
            "universe_id": (row.get("binding", {}) or {}).get("universe_id", ""),
            "universe_version": (row.get("binding", {}) or {}).get("universe_version", ""),
        }
        # Family/root lineage are lifecycle metadata, so copy them from raw
        # immutable payload fields if the compact binding has no such fields.
        raw = row.get("raw_metrics") if isinstance(row.get("raw_metrics"), Mapping) else {}
        cluster_material["family"] = raw.get("family", cluster_material["family"])
        cluster_material["root_lineage"] = raw.get("root_lineage", raw.get("root_candidate_id", cluster_material["root_lineage"]))
        cluster_key = "cluster-" + _hash(cluster_material)[:20]
        return {
            **dict(row),
            "components": components,
            "weights": weights,
            "total_score": score,
            "tie_breaks": tie_breaks,
            "cluster_key": cluster_key,
        }

    def _persist_ranking(self, ranked: Sequence[Mapping[str, Any]], selection: Mapping[str, Any], run_id: str, timestamp: str, feasibility: Mapping[str, Any] | None) -> None:
        with self.store._lock:
            conn = self.store.connection
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute("DELETE FROM binance_crypto_rankings")
                conn.executemany(
                    "INSERT INTO binance_crypto_rankings(candidate_id,symbol,ranking_run_id,ranking_timestamp,rank,total_score,raw_metrics_json,component_scores_json,weights_json,tie_breaks_json,binding_json,evidence_hashes_json,cluster_key,cluster_representative,selected,actionable,reason,binding_hash,qualification_hash) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    [
                        (
                            row["candidate_id"], row["symbol"], run_id, timestamp, int(row.get("rank") or 0),
                            row.get("total_score"), _canonical(row.get("raw_metrics", {})), _canonical(row.get("components", {})),
                            _canonical(row.get("weights", {})), _canonical(row.get("tie_breaks", [])), _canonical(row.get("binding", {})),
                            _canonical({"binding_hash": row.get("binding_hash", ""), "qualification_hash": row.get("qualification_hash", "")}),
                            row.get("cluster_key", ""), int(bool(row.get("cluster_representative"))), int(bool(row.get("selected"))),
                            int(bool(row.get("actionable"))), str(row.get("reason", "")), row.get("binding_hash", ""), row.get("qualification_hash", ""),
                        )
                        for row in ranked
                    ],
                )
                previous = conn.execute("SELECT last_selected_candidate,last_selected_symbol FROM binance_crypto_selection WHERE singleton=1").fetchone()
                previous_id = str(previous["last_selected_candidate"] or "") if previous else ""
                winner = next((row for row in ranked if row.get("selected")), None)
                status = "CURRENT" if winner else ("STALE" if previous_id else "NONE")
                reason = "" if winner else ("NO_QUALIFIED_CANDIDATE" if not previous_id else "NO_QUALIFIED_CANDIDATE")
                persisted_selection = dict(winner or {})
                fallback = [dict(row) for row in ranked if row.get("actionable") and not row.get("selected")][: self.policy.max_fallbacks]
                selection_json = _canonical(persisted_selection)
                conn.execute(
                    "INSERT INTO binance_crypto_selection(singleton,ranking_run_id,ranking_timestamp,candidate_id,symbol,rank,total_score,selection_json,selection_status,selection_valid,selection_invalidation_reason,last_selected_candidate,last_selected_symbol,fallbacks_json,feasibility_json) VALUES(1,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
                    "ON CONFLICT(singleton) DO UPDATE SET ranking_run_id=excluded.ranking_run_id,ranking_timestamp=excluded.ranking_timestamp,candidate_id=excluded.candidate_id,symbol=excluded.symbol,rank=excluded.rank,total_score=excluded.total_score,selection_json=excluded.selection_json,selection_status=excluded.selection_status,selection_valid=excluded.selection_valid,selection_invalidation_reason=excluded.selection_invalidation_reason,last_selected_candidate=excluded.last_selected_candidate,last_selected_symbol=excluded.last_selected_symbol,fallbacks_json=excluded.fallbacks_json,feasibility_json=excluded.feasibility_json",
                    (
                        run_id, timestamp, winner.get("candidate_id") if winner else None, winner.get("symbol") if winner else None,
                        winner.get("rank") if winner else None, winner.get("total_score") if winner else None, selection_json, status,
                        int(bool(winner)), None if winner else reason, winner.get("candidate_id") if winner else previous_id or None,
                        winner.get("symbol") if winner else (str(previous["last_selected_symbol"] or "") if previous else None), _canonical(fallback), _canonical(_public(feasibility or {})),
                    ),
                )
                conn.commit()
            except BaseException:
                if conn.in_transaction:
                    conn.rollback()
                raise

    def rank_and_select(self, feasibility_by_symbol: Mapping[str, Any] | None = None, *, limit: int = MAX_FALLBACKS, now: datetime | None = None) -> dict[str, Any]:
        bounded_limit = max(0, min(int(limit), max(0, int(self.policy.max_fallbacks))))
        qualified = self.qualify_all(feasibility_by_symbol, now=now)
        items = [self._rank_item(row) for row in qualified if row.get("qualified")]
        items.sort(key=lambda row: (-float(row["total_score"]), -(_number(row.get("raw_metrics", {}).get("net_expectancy")) or 0.0), -(_number(row.get("raw_metrics", {}).get("walk_forward_consistency")) or 0.0), -(_number(row.get("raw_metrics", {}).get("neighbor_stability")) or 0.0), str(row["candidate_id"]), str(row["symbol"])) )
        representatives: dict[str, Mapping[str, Any]] = {}
        for item in items:
            representatives.setdefault(str(item["cluster_key"]), item)
        reps = list(representatives.values())
        reps.sort(key=lambda row: (-float(row["total_score"]), -(_number(row.get("raw_metrics", {}).get("net_expectancy")) or 0.0), str(row["candidate_id"]), str(row["symbol"])))
        rank_map = {(str(item["candidate_id"]), str(item["symbol"])): index for index, item in enumerate(reps, 1)}
        winner_key = next(iter(rank_map), None)
        final_rows: list[dict[str, Any]] = []
        for item in [self._rank_item(row) for row in qualified]:
            key = (str(item["candidate_id"]), str(item["symbol"]))
            representative = representatives.get(str(item["cluster_key"])) is not None and representatives.get(str(item["cluster_key"]))["candidate_id"] == item["candidate_id"] and representatives.get(str(item["cluster_key"]))["symbol"] == item["symbol"]
            item["cluster_representative"] = representative
            item["rank"] = rank_map.get(key, 0)
            item["selected"] = bool(representative and key == winner_key)
            item["actionable"] = bool(representative and item["rank"] > 0 and item["rank"] <= bounded_limit + 1)
            if not item.get("qualified"):
                item["reason"] = ";".join(item.get("reasons", [])) or "NOT_QUALIFIED"
            elif not representative:
                item["reason"] = "MATERIAL_CLUSTER_NON_REPRESENTATIVE"
            else:
                item["reason"] = "SELECTED" if item["selected"] else "ACTIONABLE_FALLBACK"
            final_rows.append(item)
        final_rows.sort(key=lambda row: (0 if row.get("rank") else 1, int(row.get("rank") or 0), str(row["candidate_id"]), str(row["symbol"])))
        timestamp = (now or self.clock()).astimezone(timezone.utc).isoformat()
        seed = [(row["candidate_id"], row["symbol"], row["qualification_hash"], row.get("total_score"), row.get("rank")) for row in reps]
        run_id = "crypto-rank-" + _hash(seed)[:24]
        fallback_rows = [row for row in reps if not row.get("selected")][:bounded_limit]
        winner = next((row for row in final_rows if row.get("selected")), None)
        self._persist_ranking(final_rows, winner or {}, run_id, timestamp, feasibility_by_symbol)
        status = "CURRENT" if winner else ("STALE" if self._stored_selection_exists() else "NONE")
        return {
            "schema_version": QUALIFICATION_SCHEMA_VERSION,
            "ranking_run_id": run_id,
            "ranking_timestamp": timestamp,
            "selection_status": status,
            "selection_valid": bool(winner),
            "selected": dict(winner) if winner else None,
            "winner": dict(winner) if winner else None,
            "fallbacks": [dict(row) for row in fallback_rows],
            "rankings": final_rows,
            "qualified_count": sum(bool(row.get("qualified")) for row in qualified),
            "candidate_count": len(qualified),
            "reason": "" if winner else "NO_QUALIFIED_CANDIDATE",
            "formula_version": self.policy.formula_version,
            "policy_version": self.policy.policy_version,
            "no_credentials": True,
        }

    def _stored_selection_exists(self) -> bool:
        with self.store._lock:
            row = self.store.connection.execute("SELECT last_selected_candidate FROM binance_crypto_selection WHERE singleton=1").fetchone()
        return bool(row and row["last_selected_candidate"])

    def _read_selection(self) -> dict[str, Any] | None:
        with self.store._lock:
            row = self.store.connection.execute("SELECT * FROM binance_crypto_selection WHERE singleton=1").fetchone()
        if row is None:
            return None
        result = dict(row)
        for key in ("selection_json", "fallbacks_json", "feasibility_json"):
            try:
                result[key.removesuffix("_json")] = json.loads(result.get(key) or ("{}" if key != "fallbacks_json" else "[]"))
            except (TypeError, ValueError, json.JSONDecodeError):
                result[key.removesuffix("_json")] = {} if key != "fallbacks_json" else []
        return result
    def _current_state(self) -> tuple[str, list[str], dict[str, Any] | None]:
        # Keep the persisted selection and every evidence lookup in one
        # re-entrant store lock.  Otherwise a catalog/lifecycle writer could
        # produce a mixed evidence snapshot during revalidation.
        with self.store._lock:
            selection = self._read_selection()
            if selection is None:
                return "NONE", [], None

            persisted_status = str(selection.get("selection_status") or "NONE")
            reasons: list[str] = []
            selected = selection.get("selection")
            selected = selected if isinstance(selected, Mapping) else {}
            candidate_id = str(selection.get("candidate_id") or selected.get("candidate_id") or "")
            symbol = _symbol(selection.get("symbol") or selected.get("symbol"))
            old_hashes = selected.get("immutable_hashes", {})
            old_hashes = old_hashes if isinstance(old_hashes, Mapping) else {}
            old_binding = selected.get("binding", {})
            old_binding = old_binding if isinstance(old_binding, Mapping) else {}

            if candidate_id and symbol and selected:
                record = self.store.load_candidate_lifecycle(candidate_id)
                if not isinstance(record, Mapping):
                    reasons.append("FROZEN_EVIDENCE_MISSING")
                else:
                    payload = record.get("payload")
                    mapping = self._extract_mapping(payload) if isinstance(payload, Mapping) else {}
                    current = self._evaluate_pair(
                        record,
                        symbol,
                        mapping,
                        feasibility=selection.get("feasibility"),
                        evaluated_at=str(selection.get("ranking_timestamp") or ""),
                    )
                    new_hashes = current.get("immutable_hashes", {})
                    new_hashes = new_hashes if isinstance(new_hashes, Mapping) else {}

                    # Compare the selected binding to freshly reconstructed
                    # evidence, rather than trusting selection_valid/status or
                    # qualification_hash persisted in the selection row.
                    if old_hashes.get("frozen_hash") != new_hashes.get("frozen_hash"):
                        reasons.append("FROZEN_EVIDENCE_CHANGED")
                    if (
                        old_hashes.get("dataset_version") != new_hashes.get("dataset_version")
                        or old_hashes.get("dataset_id") != new_hashes.get("dataset_id")
                        or old_hashes.get("dataset_evidence_hash") != new_hashes.get("dataset_evidence_hash")
                    ):
                        reasons.append("DATASET_EVIDENCE_CHANGED")
                    if (
                        old_hashes.get("universe_snapshot") != new_hashes.get("universe_snapshot")
                        or old_hashes.get("universe_version") != new_hashes.get("universe_version")
                        or old_hashes.get("universe_id") != new_hashes.get("universe_id")
                        or old_hashes.get("universe_evidence_hash") != new_hashes.get("universe_evidence_hash")
                    ):
                        reasons.append("UNIVERSE_EVIDENCE_CHANGED")
                    if (
                        old_hashes.get("strategy_hash") != new_hashes.get("strategy_hash")
                        or (
                            "strategy_evidence_hash" in old_hashes
                            and old_hashes.get("strategy_evidence_hash") != new_hashes.get("strategy_evidence_hash")
                        )
                    ):
                        reasons.append("STRATEGY_EVIDENCE_CHANGED")
                    if (
                        "lifecycle_evidence_hash" in old_hashes
                        and old_hashes.get("lifecycle_evidence_hash") != new_hashes.get("lifecycle_evidence_hash")
                    ):
                        reasons.append("LIFECYCLE_EVIDENCE_CHANGED")
                    if old_binding != current.get("binding", {}) and not reasons:
                        reasons.append("BINDING_CHANGED")
                    if (
                        str(selected.get("qualification_hash") or selection.get("qualification_hash") or "")
                        != str(current.get("qualification_hash") or "")
                        and not reasons
                    ):
                        reasons.append("LIFECYCLE_EVIDENCE_CHANGED")
                    if not current.get("qualified"):
                        reasons.extend(str(item) for item in current.get("reasons", []))

                    if reasons:
                        return "STALE", list(dict.fromkeys(reasons)), selection
                    # A stale/invalid persisted bit is not authoritative.  A
                    # selected pair with unchanged evidence remains CURRENT.
                    return "CURRENT", [], selection
            if reasons:
                return "STALE", list(dict.fromkeys(reasons)), selection

            if selection.get("last_selected_candidate"):
                return "STALE", [str(selection.get("selection_invalidation_reason") or "REEVALUATION_REQUIRED")], selection
            if persisted_status == "CURRENT":
                return "STALE", ["SELECTION_EVIDENCE_MISSING"], selection
            return "NONE", [str(selection.get("selection_invalidation_reason"))] if selection.get("selection_invalidation_reason") else [], selection

    @staticmethod
    def _selection_result(status: str, reasons: Sequence[str], selection: Mapping[str, Any] | None) -> dict[str, Any] | None:
        if selection is None or status != "CURRENT":
            return None
        selected = selection.get("selection")
        if not isinstance(selected, Mapping):
            return None
        result = dict(selected)
        result.update({"selection_status": status, "selection_valid": True, "reason": ";".join(reasons)})
        return result

    def current_selection(self) -> dict[str, Any] | None:
        status, reasons, selection = self._current_state()
        return self._selection_result(status, reasons, selection)

    def actionable_rankings(self, limit: int = MAX_FALLBACKS) -> list[dict[str, Any]]:
        bounded = max(1, min(int(limit), max(1, int(self.policy.max_fallbacks) + 1)))
        with self.store._lock:
            rows = self.store.connection.execute("SELECT * FROM binance_crypto_rankings WHERE actionable=1 ORDER BY rank,total_score DESC,candidate_id,symbol LIMIT ?", (bounded,)).fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            for key in ("raw_metrics_json", "component_scores_json", "weights_json", "tie_breaks_json", "binding_json", "evidence_hashes_json"):
                try:
                    item[key.removesuffix("_json")] = json.loads(item.get(key) or ("[]" if key == "tie_breaks_json" else "{}"))
                except (TypeError, ValueError, json.JSONDecodeError):
                    item[key.removesuffix("_json")] = {} if key != "tie_breaks_json" else []
            result.append(item)
        return result

    def status(self) -> dict[str, Any]:
        with self.store._lock:
            selection_status, reasons, selection = self._current_state()
            qualified_count = int(self.store.connection.execute("SELECT COUNT(*) AS n FROM binance_crypto_qualification WHERE qualified=1").fetchone()["n"])
            ranking_count = int(self.store.connection.execute("SELECT COUNT(*) AS n FROM binance_crypto_rankings").fetchone()["n"])
            current_selection = self._selection_result(selection_status, reasons, selection)
            return {
                "selection_status": selection_status,
                "status": selection_status,
                "selection_valid": selection_status == "CURRENT",
                "reason": ";".join(reasons),
                "reasons": reasons,
                "current_selection": current_selection,
                "qualified_count": qualified_count,
                "ranking_count": ranking_count,
                "actionable_rankings": self.actionable_rankings(self.policy.max_fallbacks + 1),
                "formula_version": self.policy.formula_version,
                "policy_version": self.policy.policy_version,
                "no_credentials": True,
            }

    # Compatibility aliases used by callers that name this component a ranker.
    evaluate = rank_and_select
    rank = rank_and_select


CryptoExecutionRanker = BinanceCryptoQualificationService
BinanceCryptoResearch = BinanceCryptoQualificationService


def qualify_all(store: AxiomStore, feasibility_by_symbol: Mapping[str, Any] | None = None, **kwargs: Any) -> _QualificationRows:
    return BinanceCryptoQualificationService(store, **{key: value for key, value in kwargs.items() if key in {"policy", "clock", "venue", "adapter_version"}}).qualify_all(feasibility_by_symbol)


def rank_and_select(store: AxiomStore, feasibility_by_symbol: Mapping[str, Any] | None = None, **kwargs: Any) -> dict[str, Any]:
    service = BinanceCryptoQualificationService(store, **{key: value for key, value in kwargs.items() if key in {"policy", "clock", "venue", "adapter_version"}})
    return service.rank_and_select(feasibility_by_symbol, limit=int(kwargs.get("limit", MAX_FALLBACKS)), now=kwargs.get("now"))


__all__ = [
    "PAPER", "BINANCE_SPOT_TESTNET", "BINANCE_SPOT_LIVE", "USDT",
    "QUALIFICATION_SCHEMA_VERSION", "BINDING_SCHEMA_VERSION", "FORMULA_VERSION", "POLICY_VERSION",
    "CryptoExecutionBinding", "CryptoQualificationPolicy", "BinanceCryptoQualificationService",
    "CryptoExecutionRanker", "BinanceCryptoResearch", "qualify_all", "rank_and_select",
]
