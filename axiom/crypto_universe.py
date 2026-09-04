"""Deterministic, point-in-time crypto universe construction.

The universe is deliberately a research input, not an execution primitive.  A
configured market-cap ranking source is intersected with Binance Spot's public
``exchangeInfo`` response and the complete ranked decision table is persisted
as an immutable dataset snapshot.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
import re
from typing import Any, Callable, Iterable, Mapping, Protocol, Sequence

from .data._http import HTTPFetchError, as_float, as_int, fetch_json_strict, query_url
from .data.binance import BinanceAdapter
from .domain import MarketType, ensure_utc, utc_now
from .storage import AxiomStore


TOP_50_MARKET_CAP_BINANCE_USDT = "TOP_50_MARKET_CAP_BINANCE_USDT"
UNIVERSE_SCHEMA_VERSION = "crypto-universe-v1"
CURRENT_UNIVERSE = "CURRENT_UNIVERSE"
SURVIVORSHIP_BIAS_PRESENT = "SURVIVORSHIP_BIAS_PRESENT"

_DEFAULT_STABLECOIN_IDS = frozenset(
    {
        "tether",
        "usd-coin",
        "binance-usd",
        "dai",
        "true-usd",
        "first-digital-usd",
        "pax-dollar",
        "usdd",
        "frax",
        "gemini-dollar",
        "paypal-usd",
        "liquity-usd",
        "usde",
        "usds",
        "terrausd",
        "magic-internet-money",
        "vai",
        "usd1",
        "ripple-usd",
    }
)
_DEFAULT_STABLECOIN_SYMBOLS = frozenset(
    {
        "USDT",
        "USDC",
        "BUSD",
        "DAI",
        "TUSD",
        "FDUSD",
        "USDP",
        "USDD",
        "FRAX",
        "GUSD",
        "PYUSD",
        "LUSD",
        "USDE",
        "USDS",
        "UST",
        "MIM",
        "VAI",
        "USD1",
        "RLUSD",
    }
)
_LEVERAGED_TOKEN_SUFFIX = re.compile(r"(?:UP|DOWN|BULL|BEAR|[2-9]L|[2-9]S)$", re.IGNORECASE)


class RankingProvider(Protocol):
    """Protocol for a bounded current global market-cap ranking source."""

    provider_name: str

    def rankings(self, limit: int) -> Sequence[Mapping[str, Any]]:
        ...


@dataclass(frozen=True, slots=True)
class UniverseConfig:
    """Selection and exclusion policy for one durable universe identity."""

    policy: str = TOP_50_MARKET_CAP_BINANCE_USDT
    top_n: int = 50
    quote_asset: str = "USDT"
    refresh_interval: timedelta = timedelta(days=1)
    stablecoin_ids: frozenset[str] = _DEFAULT_STABLECOIN_IDS
    stablecoin_symbols: frozenset[str] = _DEFAULT_STABLECOIN_SYMBOLS
    duplicate_ids: frozenset[str] = frozenset()
    wrapped_staked_pegged_ids: frozenset[str] = frozenset()
    dataset_id: str | None = None

    def __post_init__(self) -> None:
        top_n = int(self.top_n)
        if isinstance(self.top_n, bool) or top_n <= 0 or top_n > 250:
            raise ValueError("top_n must be an integer in [1, 250]")
        if not str(self.policy).strip():
            raise ValueError("policy must not be empty")
        quote = str(self.quote_asset).strip().upper()
        if not quote:
            raise ValueError("quote_asset must not be empty")
        interval = self.refresh_interval
        if isinstance(interval, timedelta):
            seconds = interval.total_seconds()
        else:
            seconds = float(interval)  # type: ignore[arg-type]
            interval = timedelta(seconds=seconds)
        if not math.isfinite(seconds) or seconds <= 0:
            raise ValueError("refresh_interval must be positive")
        object.__setattr__(self, "top_n", top_n)
        object.__setattr__(self, "quote_asset", quote)
        object.__setattr__(self, "refresh_interval", interval)
        object.__setattr__(self, "stablecoin_ids", _normalize_ids(self.stablecoin_ids))
        object.__setattr__(self, "stablecoin_symbols", _normalize_symbols(self.stablecoin_symbols))
        object.__setattr__(self, "duplicate_ids", _normalize_ids(self.duplicate_ids))
        object.__setattr__(self, "wrapped_staked_pegged_ids", _normalize_ids(self.wrapped_staked_pegged_ids))
        if self.dataset_id is not None and not str(self.dataset_id).strip():
            raise ValueError("dataset_id must not be empty")

    @property
    def universe_id(self) -> str:
        if self.policy == TOP_50_MARKET_CAP_BINANCE_USDT and self.top_n == 50 and self.quote_asset == "USDT":
            return TOP_50_MARKET_CAP_BINANCE_USDT
        return f"TOP_{self.top_n}_MARKET_CAP_BINANCE_{self.quote_asset}"

    @property
    def persisted_dataset_id(self) -> str:
        return str(self.dataset_id or f"universe:{self.universe_id}")

    @property
    def exclusion_ids(self) -> frozenset[str]:
        return frozenset((*self.stablecoin_ids, *self.duplicate_ids, *self.wrapped_staked_pegged_ids))


@dataclass(frozen=True, slots=True)
class UniverseSnapshot:
    """Immutable in-memory representation of one persisted (or stale) snapshot."""

    universe_id: str
    version: str | None
    snapshot_hash: str | None
    observed_at: datetime | None
    status: str
    records: tuple[Mapping[str, Any], ...]
    labels: tuple[str, ...] = (CURRENT_UNIVERSE, SURVIVORSHIP_BIAS_PRESENT)
    metadata: Mapping[str, Any] = field(default_factory=dict)
    error: str | None = None

    def __post_init__(self) -> None:
        if not str(self.universe_id).strip():
            raise ValueError("universe_id is required")
        if self.status not in {"CURRENT", "STALE"}:
            raise ValueError("snapshot status must be CURRENT or STALE")
        if self.observed_at is not None:
            object.__setattr__(self, "observed_at", ensure_utc(self.observed_at))
        object.__setattr__(self, "records", tuple(dict(record) for record in self.records))
        object.__setattr__(self, "labels", tuple(str(label) for label in self.labels))
        object.__setattr__(self, "metadata", dict(self.metadata))

    @property
    def selected_records(self) -> tuple[Mapping[str, Any], ...]:
        return tuple(record for record in self.records if bool(record.get("selected")))

    @property
    def selected_symbols(self) -> tuple[str, ...]:
        return tuple(str(record.get("binance_symbol") or record.get("symbol")) for record in self.selected_records)

    def to_provenance(self) -> dict[str, Any]:
        """Return a JSON-safe provenance object suitable for research inputs."""
        return {
            "universe_id": self.universe_id,
            "version": self.version,
            "snapshot_hash": self.snapshot_hash,
            "observed_at": _iso(self.observed_at) if self.observed_at else None,
            "status": self.status,
            "labels": list(self.labels),
        }

    def as_dict(self) -> dict[str, Any]:
        return {
            **self.to_provenance(),
            "records": [dict(record) for record in self.records],
            "metadata": dict(self.metadata),
            "error": self.error,
        }


class CoinGeckoRankingProvider:
    """Bounded, credential-free CoinGecko global market-cap ranking provider."""

    provider_name = "coingecko"
    max_per_page = 250

    def __init__(
        self,
        *,
        base_url: str = "https://api.coingecko.com/api/v3",
        vs_currency: str = "usd",
        per_page: int = 50,
        timeout: float = 10.0,
        opener: Callable[..., Any] | None = None,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        timeout_value = float(timeout)
        if not math.isfinite(timeout_value) or timeout_value <= 0:
            raise ValueError("timeout must be finite and positive")
        if isinstance(per_page, bool) or not isinstance(per_page, int) or not 1 <= per_page <= self.max_per_page:
            raise ValueError(f"per_page must be an integer in [1, {self.max_per_page}]")
        currency = str(vs_currency).strip().lower()
        if not currency:
            raise ValueError("vs_currency must not be empty")
        self.base_url = str(base_url).rstrip("/")
        self.vs_currency = currency
        self.per_page = per_page
        self.timeout = timeout_value
        self._opener = opener
        self._clock = clock

    def rankings(self, limit: int | None = None) -> tuple[Mapping[str, Any], ...]:
        requested = self.per_page if limit is None else limit
        if isinstance(requested, bool) or not isinstance(requested, int) or not 1 <= requested <= self.max_per_page:
            raise ValueError(f"limit must be an integer in [1, {self.max_per_page}]")
        url = query_url(
            self.base_url,
            "/coins/markets",
            {
                "vs_currency": self.vs_currency,
                "order": "market_cap_desc",
                "per_page": requested,
                "page": 1,
                "sparkline": "false",
            },
        )
        payload = fetch_json_strict(url, self.timeout, self._opener)
        if not isinstance(payload, list):
            raise HTTPFetchError("CoinGecko ranking response was not a list", url=url)
        observed_at = ensure_utc(self._clock())
        result: list[Mapping[str, Any]] = []
        for row in payload:
            if not isinstance(row, Mapping):
                continue
            item = dict(row)
            item["observed_at"] = observed_at
            result.append(item)
        if not result:
            raise HTTPFetchError("CoinGecko ranking response contained no rows", url=url)
        return tuple(result)

    fetch_rankings = rankings
    fetch = rankings


class CryptoUniverseBuilder:
    """Build and persist a daily-or-forced Binance USDT crypto universe."""

    def __init__(
        self,
        ranking_provider: RankingProvider | Callable[..., Sequence[Mapping[str, Any]]] | Sequence[Mapping[str, Any]],
        binance_provider: BinanceAdapter | Any,
        store: AxiomStore,
        *,
        config: UniverseConfig | None = None,
    ) -> None:
        self.ranking_provider = ranking_provider
        self.binance_provider = binance_provider
        self.store = store
        self.config = config or UniverseConfig()

    def build(self, *, force: bool = False, now: datetime | None = None) -> UniverseSnapshot:
        observation_time = ensure_utc(now or utc_now())
        latest = self._latest()
        if latest is not None and not force and _is_fresh(latest, observation_time, self.config.refresh_interval):
            return latest

        try:
            ranking_rows = _invoke_rankings(self.ranking_provider, self.config.top_n)
            if not ranking_rows:
                raise RuntimeError("ranking provider returned no rows")
            exchange_rows = _invoke_exchange_symbols(self.binance_provider, self.config.quote_asset)
            if exchange_rows is None:
                raise RuntimeError("Binance exchangeInfo was unavailable")
            records = _build_records(ranking_rows, exchange_rows, self.config, observation_time)
            if not records:
                raise RuntimeError("ranking provider returned no valid ranked rows")
            version = _snapshot_hash(self.config.universe_id, observation_time, records)
            metadata = _metadata(self.config, observation_time, version, "CURRENT")
            self._persist(records, version, metadata, observation_time)
            return UniverseSnapshot(
                universe_id=self.config.universe_id,
                version=version,
                snapshot_hash=version,
                observed_at=observation_time,
                status="CURRENT",
                records=tuple(records),
                labels=(CURRENT_UNIVERSE, SURVIVORSHIP_BIAS_PRESENT),
                metadata=metadata,
            )
        except Exception as exc:
            if latest is not None:
                stale_metadata = dict(latest.metadata)
                stale_metadata["status"] = "STALE"
                stale_metadata["stale_reason"] = str(exc)
                return UniverseSnapshot(
                    universe_id=latest.universe_id,
                    version=latest.version,
                    snapshot_hash=latest.snapshot_hash,
                    observed_at=latest.observed_at,
                    status="STALE",
                    records=latest.records,
                    labels=latest.labels,
                    metadata=stale_metadata,
                    error=str(exc),
                )
            return UniverseSnapshot(
                universe_id=self.config.universe_id,
                version=None,
                snapshot_hash=None,
                observed_at=None,
                status="STALE",
                records=(),
                labels=(CURRENT_UNIVERSE, SURVIVORSHIP_BIAS_PRESENT),
                metadata=_metadata(self.config, observation_time, None, "STALE"),
                error=str(exc),
            )

    refresh = build

    def _latest(self) -> UniverseSnapshot | None:
        loaded = self.store.load_dataset_record(self.config.persisted_dataset_id)
        if not loaded:
            return None
        metadata = loaded.get("metadata")
        if not isinstance(metadata, Mapping):
            metadata = {}
        rows = loaded.get("records")
        if not isinstance(rows, list):
            return None
        observed_at = _parse_datetime(metadata.get("observed_at")) or _parse_datetime(loaded.get("created_at"))
        version = str(loaded.get("version")) if loaded.get("version") is not None else None
        snapshot_hash = metadata.get("snapshot_hash") or version
        return UniverseSnapshot(
            universe_id=str(metadata.get("universe_id") or self.config.universe_id),
            version=version,
            snapshot_hash=str(snapshot_hash) if snapshot_hash is not None else None,
            observed_at=observed_at,
            status=str(metadata.get("status") or "CURRENT"),
            records=tuple(row for row in rows if isinstance(row, Mapping)),
            labels=tuple(metadata.get("labels") or (CURRENT_UNIVERSE, SURVIVORSHIP_BIAS_PRESENT)),
            metadata=metadata,
        )

    def _persist(self, records: Sequence[Mapping[str, Any]], version: str, metadata: Mapping[str, Any], observed_at: datetime) -> None:
        dataset_id = self.config.persisted_dataset_id
        with self.store.transaction():
            try:
                self.store.save_dataset(dataset_id, version, list(records), metadata=metadata, quality="HIGH")
            except ValueError:
                existing = self.store.load_dataset_record(dataset_id, version)
                if existing is None or existing.get("records") != list(records):
                    raise
            self.store.save_dataset_catalog(
                dataset_id,
                version,
                provider=str(getattr(self.ranking_provider, "provider_name", type(self.ranking_provider).__name__)),
                instrument=self.config.universe_id,
                market_type=MarketType.CRYPTO_SPOT,
                timeframe="point_in_time",
                start_timestamp=observed_at,
                end_timestamp=observed_at,
                row_count=len(records),
                completeness=1.0,
                quality="HIGH",
                source_type="FORWARD_COLLECTED",
                snapshot_id=f"{self.config.universe_id}:{version}",
                metadata=metadata,
                created_at=observed_at,
                updated_at=observed_at,
            )


UniverseBuilder = CryptoUniverseBuilder


def build_crypto_universe(
    ranking_provider: RankingProvider | Callable[..., Sequence[Mapping[str, Any]]] | Sequence[Mapping[str, Any]],
    binance_provider: BinanceAdapter | Any,
    store: AxiomStore,
    *,
    config: UniverseConfig | None = None,
    force: bool = False,
    now: datetime | None = None,
) -> UniverseSnapshot:
    """Convenience wrapper around :class:`CryptoUniverseBuilder`."""
    return CryptoUniverseBuilder(ranking_provider, binance_provider, store, config=config).build(force=force, now=now)


def is_leveraged_token_style(symbol: str) -> bool:
    """Return whether a symbol has a structural Binance leveraged-token suffix."""
    normalized = _normalize_symbol(symbol)
    return bool(_LEVERAGED_TOKEN_SUFFIX.search(normalized))


def _build_records(
    ranking_rows: Sequence[Any],
    exchange_rows: Sequence[Any],
    config: UniverseConfig,
    observed_at: datetime,
) -> list[dict[str, Any]]:
    normalized_rows = [_normalize_ranking_row(row, index + 1, observed_at) for index, row in enumerate(ranking_rows)]
    normalized_rows = [row for row in normalized_rows if row is not None]
    normalized_rows.sort(key=lambda row: (int(row["rank"]), str(row["asset_id"]), str(row["symbol"])))
    normalized_rows = normalized_rows[: config.top_n]

    exchange = _normalize_exchange_rows(exchange_rows, config.quote_asset)
    records: list[dict[str, Any]] = []
    for row in normalized_rows:
        asset_id = str(row["asset_id"])
        symbol = str(row["symbol"])
        symbol_key = _normalize_symbol(symbol)
        matches = exchange.get(symbol_key, ())
        match = _best_exchange_match(matches)
        reason = "SELECTED"
        selected = True
        if asset_id.lower() in config.stablecoin_ids or symbol_key in config.stablecoin_symbols or symbol_key == config.quote_asset:
            reason, selected = "STABLECOIN", False
        elif asset_id.lower() in config.duplicate_ids or asset_id.lower() in config.wrapped_staked_pegged_ids:
            reason, selected = "CONFIGURED_DUPLICATE", False
        elif is_leveraged_token_style(symbol_key):
            reason, selected = "LEVERAGED_TOKEN_STYLE", False
        elif not matches:
            reason, selected = "NOT_BINANCE_USDT_SPOT", False
        elif not any(_status_is_trading(item) for item in matches):
            reason, selected = "BINANCE_STATUS_NOT_TRADING", False
        elif not any(_spot_allowed(item) and _status_is_trading(item) for item in matches):
            reason, selected = "BINANCE_SPOT_NOT_ALLOWED", False

        records.append(
            {
                "universe_id": config.universe_id,
                "rank": int(row["rank"]),
                "market_cap_rank": int(row["rank"]),
                "asset_id": asset_id,
                "symbol": symbol,
                "name": str(row["name"]),
                "market_cap": row["market_cap"],
                "observed_at": _iso(row["observed_at"]),
                "binance_symbol": _exchange_value(match, "symbol"),
                "binance_base_asset": _exchange_value(match, "baseAsset"),
                "binance_quote_asset": _exchange_value(match, "quoteAsset"),
                "binance_status": _exchange_value(match, "status"),
                "binance_spot_trading_allowed": _spot_allowed(match) if match else False,
                "selected": selected,
                "exclusion_reason": None if selected else reason,
                "reason": reason,
            }
        )
    return records
def _normalize_ranking_row(value: Any, fallback_rank: int, observed_at: datetime) -> dict[str, Any] | None:
    source = value if isinstance(value, Mapping) else {name: getattr(value, name, None) for name in ("id", "asset_id", "coin_id", "symbol", "name", "market_cap", "market_cap_rank", "rank", "observed_at")}
    asset_id = source.get("asset_id") or source.get("id") or source.get("coin_id")
    symbol = source.get("symbol")
    if asset_id is None or symbol is None:
        return None
    rank_value = source.get("market_cap_rank", source.get("rank", source.get("position", fallback_rank)))
    rank = as_int(rank_value)
    if rank is None or rank <= 0:
        rank = fallback_rank
    market_cap = as_float(source.get("market_cap"))
    row_observed = _parse_datetime(source.get("observed_at")) or observed_at
    return {
        "asset_id": str(asset_id).strip(),
        "symbol": str(symbol).strip().upper(),
        "name": str(source.get("name") or symbol).strip(),
        "rank": rank,
        "market_cap": market_cap,
        "observed_at": row_observed,
    }


def _normalize_exchange_rows(rows: Sequence[Any], quote_asset: str) -> dict[str, tuple[Mapping[str, Any], ...]]:
    by_base: dict[str, list[Mapping[str, Any]]] = {}
    for value in rows:
        if isinstance(value, str):
            symbol = _normalize_symbol(value)
            if symbol.endswith(quote_asset):
                record: Mapping[str, Any] = {"symbol": symbol, "baseAsset": symbol[: -len(quote_asset)], "quoteAsset": quote_asset, "status": "TRADING", "isSpotTradingAllowed": True}
            else:
                continue
        elif isinstance(value, Mapping):
            record = dict(value)
            if str(record.get("quoteAsset", "")).upper() != quote_asset:
                continue
        else:
            continue
        base = _normalize_symbol(record.get("baseAsset") or str(record.get("symbol", ""))[: -len(quote_asset)])
        if not base:
            continue
        by_base.setdefault(base, []).append(record)
    return {key: tuple(sorted(value, key=lambda item: str(item.get("symbol", "")))) for key, value in by_base.items()}


def _best_exchange_match(matches: Sequence[Mapping[str, Any]]) -> Mapping[str, Any] | None:
    if not matches:
        return None
    return sorted(matches, key=lambda item: (not (_status_is_trading(item) and _spot_allowed(item)), str(item.get("symbol", ""))))[0]


def _status_is_trading(record: Mapping[str, Any]) -> bool:
    return str(record.get("status", "")).upper() == "TRADING"


def _spot_allowed(record: Mapping[str, Any]) -> bool:
    if not record:
        return False
    if "isSpotTradingAllowed" in record:
        return _boolish(record.get("isSpotTradingAllowed"))
    permissions = record.get("permissions")
    if isinstance(permissions, (list, tuple, set)) and any(str(item).upper() == "SPOT" for item in permissions):
        return True
    permission_sets = record.get("permissionSets")
    if isinstance(permission_sets, (list, tuple)):
        return any(isinstance(group, (list, tuple, set)) and any(str(item).upper() == "SPOT" for item in group) for group in permission_sets)
    return False


def _boolish(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    return bool(value)


def _exchange_value(record: Mapping[str, Any] | None, key: str) -> Any:
    return record.get(key) if record is not None else None


def _invoke_rankings(provider: Any, limit: int) -> Sequence[Any]:
    if isinstance(provider, Sequence) and not isinstance(provider, (str, bytes, bytearray)):
        return provider[:limit]
    if callable(provider):
        return provider(limit)
    for name in ("rankings", "fetch_rankings", "get_rankings", "fetch"):
        method = getattr(provider, name, None)
        if callable(method):
            try:
                return method(limit=limit)
            except TypeError:
                return method(limit)
    raise TypeError("ranking provider must expose rankings(limit) or be callable")


def _invoke_exchange_symbols(provider: Any, quote_asset: str) -> Sequence[Any] | None:
    for name in ("exchange_symbols", "discover_exchange_symbols", "exchange_info", "discover_spot_symbols"):
        method = getattr(provider, name, None)
        if not callable(method):
            continue
        try:
            payload = method(quote_asset=quote_asset)
        except TypeError:
            payload = method()
        if isinstance(payload, Mapping):
            symbols = payload.get("symbols")
            return symbols if isinstance(symbols, list) else None
        if isinstance(payload, (list, tuple)):
            return payload
        return None
    if isinstance(provider, Mapping):
        symbols = provider.get("symbols")
        return symbols if isinstance(symbols, list) else None
    return None


def _metadata(config: UniverseConfig, observed_at: datetime, version: str | None, status: str) -> dict[str, Any]:
    return {
        "schema_version": UNIVERSE_SCHEMA_VERSION,
        "policy": config.policy,
        "universe_id": config.universe_id,
        "snapshot_hash": version,
        "version": version,
        "observed_at": _iso(observed_at),
        "status": status,
        "labels": [CURRENT_UNIVERSE, SURVIVORSHIP_BIAS_PRESENT],
        "point_in_time": True,
        "quote_asset": config.quote_asset,
        "top_n": config.top_n,
    }


def _snapshot_hash(universe_id: str, observed_at: datetime, records: Sequence[Mapping[str, Any]]) -> str:
    body = _canonical({"schema_version": UNIVERSE_SCHEMA_VERSION, "universe_id": universe_id, "observed_at": _iso(observed_at), "records": records})
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def _canonical(value: Any) -> str:
    return json.dumps(_jsonable(value), sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def _jsonable(value: Any) -> Any:
    if isinstance(value, datetime):
        return _iso(value)
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    return value


def _normalize_ids(values: Iterable[Any]) -> frozenset[str]:
    return frozenset(str(value).strip().lower() for value in values if str(value).strip())


def _normalize_symbols(values: Iterable[Any]) -> frozenset[str]:
    return frozenset(_normalize_symbol(value) for value in values if str(value).strip())


def _normalize_symbol(value: Any) -> str:
    return str(value).replace("/", "").replace("-", "").replace("_", "").strip().upper()


def _parse_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return ensure_utc(value)
    text = str(value).strip()
    if not text:
        return None
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        return ensure_utc(datetime.fromisoformat(text))
    except (TypeError, ValueError, OverflowError):
        return None


def _iso(value: datetime) -> str:
    return ensure_utc(value).isoformat()


def _is_fresh(snapshot: UniverseSnapshot, now: datetime, interval: timedelta) -> bool:
    if snapshot.status != "CURRENT" or snapshot.observed_at is None:
        return False
    age = now - ensure_utc(snapshot.observed_at)
    return age >= timedelta(0) and age < interval


__all__ = [
    "CoinGeckoRankingProvider",
    "CryptoUniverseBuilder",
    "CURRENT_UNIVERSE",
    "RankingProvider",
    "SURVIVORSHIP_BIAS_PRESENT",
    "TOP_50_MARKET_CAP_BINANCE_USDT",
    "UNIVERSE_SCHEMA_VERSION",
    "UniverseBuilder",
    "UniverseConfig",
    "UniverseSnapshot",
    "build_crypto_universe",
    "is_leveraged_token_style",
]
