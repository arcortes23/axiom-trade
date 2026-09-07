"""Bounded, public Binance Spot canary market collection.

This module deliberately sits above :class:`axiom.data.binance.BinanceAdapter`.
It binds every observation to one immutable :class:`UniverseSnapshot`, fetches
symbols with bounded worker concurrency, and never exposes order or account
operations.  A collection record is useful to downstream research without
being an execution primitive.
"""
from __future__ import annotations

from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import hashlib
import inspect
import json
import math
import re
import threading
import time
from typing import Any, Callable, Iterable, Mapping, Sequence
from .domain import CryptoTicker, OHLCVBar, OrderBookSnapshot, ensure_utc, to_record, utc_now
from axiom.crypto_universe import SURVIVORSHIP_BIAS_PRESENT, UniverseSnapshot

PAPER = "PAPER"
BINANCE_SPOT_TESTNET = "BINANCE_SPOT_TESTNET"
BINANCE_SPOT_LIVE = "BINANCE_SPOT_LIVE"
USDT = "USDT"
REST_ORIGINS: Mapping[str, str | None] = {
    PAPER: None,
    BINANCE_SPOT_TESTNET: "https://testnet.binance.vision",
    BINANCE_SPOT_LIVE: "https://api.binance.com",
}

_SECRET_KEYS = frozenset(
    {
        "apikey",
        "api_key",
        "secret",
        "secretkey",
        "privatekey",
        "private_key",
        "password",
        "signature",
        "authorization",
    }
)
_INTERVAL_RE = re.compile(r"^(?P<n>\d+)(?P<unit>[smhdwM])$")


def _normalize_symbol(value: Any) -> str:
    symbol = str(value).replace("/", "").replace("-", "").replace("_", "").strip().upper()
    if not symbol:
        raise ValueError("symbol must not be empty")
    return symbol


def _boolish(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    return bool(value)


def _duration_seconds(value: Any, name: str, *, default: float | None = None) -> float:
    if value is None and default is not None:
        return default
    if hasattr(value, "total_seconds"):
        result = float(value.total_seconds())
    else:
        result = float(value)
    if not math.isfinite(result) or result < 0:
        raise ValueError(f"{name} must be finite and non-negative")
    return result


def _iso(value: datetime | None) -> str | None:
    return ensure_utc(value).isoformat() if value is not None else None


def _public_copy(value: Any) -> Any:
    """Copy JSON-like public payloads while dropping credential-shaped keys."""
    if isinstance(value, Mapping):
        return {
            str(key): _public_copy(item)
            for key, item in value.items()
            if str(key).strip().lower() not in _SECRET_KEYS
        }
    if isinstance(value, (list, tuple)):
        return [_public_copy(item) for item in value]
    if isinstance(value, set):
        return sorted(_public_copy(item) for item in value)
    return value


def _canonical(value: Any) -> str:
    return json.dumps(_public_copy(value), sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def _content_hash(value: Any) -> str:
    return "sha256:" + hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _interval_delta(interval: str) -> timedelta | None:
    match = _INTERVAL_RE.match(str(interval).strip())
    if match is None:
        return None
    count = int(match.group("n"))
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
    return None


def _value(row: Any, *names: str) -> Any:
    if isinstance(row, Mapping):
        for name in names:
            if name in row:
                return row[name]
        return None
    for name in names:
        result = getattr(row, name, None)
        if result is not None:
            return result
    return None


def _close_time(row: Any) -> datetime | None:
    value = _value(row, "closeTime", "close_time", "closeTimestamp", "close_timestamp", "end_time")
    if value is None and isinstance(row, (list, tuple)) and len(row) > 6:
        value = row[6]
    if isinstance(value, datetime):
        return ensure_utc(value)
    if value is None:
        return None
    try:
        number = float(value)
        if not math.isfinite(number):
            return None
        if abs(number) > 100_000_000_000:
            number /= 1000.0
        return datetime.fromtimestamp(number, tz=timezone.utc)
    except (TypeError, ValueError, OverflowError, OSError):
        return None


def _coerce_bar(row: Any) -> OHLCVBar | Any | None:
    if isinstance(row, OHLCVBar):
        return row
    if isinstance(row, (list, tuple)):
        if len(row) < 6:
            return None
        fields = (row[0], row[1], row[2], row[3], row[4], row[5])
        trades = row[8] if len(row) > 8 else None
    else:
        fields = tuple(_value(row, name) for name in ("timestamp", "open", "high", "low", "close", "volume"))
        trades = _value(row, "trades", "numberOfTrades", "trade_count")
    try:
        stamp = fields[0]
        if not isinstance(stamp, datetime):
            number = float(stamp)
            if abs(number) > 100_000_000_000:
                number /= 1000.0
            stamp = datetime.fromtimestamp(number, tz=timezone.utc)
        return OHLCVBar(
            timestamp=ensure_utc(stamp),
            open=float(fields[1]),
            high=float(fields[2]),
            low=float(fields[3]),
            close=float(fields[4]),
            volume=float(fields[5]),
            trades=int(trades) if trades is not None else None,
        )
    except (TypeError, ValueError, OverflowError, OSError):
        return None


def _invoke(method: Callable[..., Any], symbol: str, **kwargs: Any) -> Any:
    """Invoke a fake or real provider without requiring one exact signature."""
    try:
        signature = inspect.signature(method)
    except (TypeError, ValueError):
        return method(symbol, **kwargs)
    parameters = signature.parameters
    symbol_parameter = parameters.get("symbol")
    accepts_kwargs = any(param.kind is inspect.Parameter.VAR_KEYWORD for param in parameters.values())
    if symbol_parameter is None:
        if accepts_kwargs:
            return method(**kwargs)
        supported = {
            name: value
            for name, value in kwargs.items()
            if name in parameters and value is not None
        }
        return method(**supported)
    if accepts_kwargs:
        return method(symbol=symbol, **kwargs) if symbol_parameter.kind is inspect.Parameter.KEYWORD_ONLY else method(symbol, **kwargs)
    supported = {
        name: value
        for name, value in kwargs.items()
        if name in parameters and value is not None
    }
    if symbol_parameter.kind is inspect.Parameter.KEYWORD_ONLY:
        return method(symbol=symbol, **supported)
    return method(symbol, **supported)


def _records_from_exchange(payload: Any, symbol: str) -> Mapping[str, Any] | None:
    if not isinstance(payload, Mapping):
        return None
    rows = payload.get("symbols")
    if isinstance(rows, Sequence) and not isinstance(rows, (str, bytes, bytearray)):
        for row in rows:
            if not isinstance(row, Mapping):
                continue
            try:
                row_symbol = _normalize_symbol(row.get("symbol", ""))
            except ValueError:
                continue
            if row_symbol == symbol:
                return _public_copy(row)
        return None
    direct_symbol = payload.get("symbol")
    if direct_symbol is None:
        return None
    try:
        if _normalize_symbol(direct_symbol) != symbol:
            return None
    except ValueError:
        return None
    return _public_copy(payload)


def _spot_allowed(record: Mapping[str, Any]) -> bool:
    if "isSpotTradingAllowed" in record:
        return _boolish(record.get("isSpotTradingAllowed"))
    permissions = record.get("permissions")
    if isinstance(permissions, (list, tuple, set)) and any(str(value).upper() == "SPOT" for value in permissions):
        return True
    sets = record.get("permissionSets")
    if isinstance(sets, (list, tuple, set)):
        for permission_set in sets:
            if isinstance(permission_set, (list, tuple, set)) and any(str(value).upper() == "SPOT" for value in permission_set):
                return True
    return False


def _rank_key(record: Mapping[str, Any], index: int) -> tuple[int, int]:
    try:
        value = int(record.get("rank", record.get("market_cap_rank", index + 1)))
    except (TypeError, ValueError):
        value = index + 1
    return value, index


@dataclass(frozen=True, slots=True)
class BinanceMarketSnapshot:
    """One symbol's public canary observations and immutable provenance."""

    symbol: str
    asset_symbol: str | None = None
    asset_id: str | None = None
    rank: int | None = None
    bars: tuple[Any, ...] = ()
    ticker: CryptoTicker | None = None
    book: OrderBookSnapshot | None = None
    exchange_info: Mapping[str, Any] | None = None
    tradable: bool = False
    new_entry_allowed: bool = False
    selected: bool = False
    exit_only: bool = False
    timed_out: bool = False
    error: str | None = None
    reasons: tuple[str, ...] = ()
    observed_at: datetime | None = None
    ticker_fresh: bool = False
    book_fresh: bool = False
    depth: Mapping[str, Any] = field(default_factory=dict)
    spread: float | None = None
    spread_evidence: Mapping[str, Any] = field(default_factory=dict)
    fill_evidence: Mapping[str, Any] = field(default_factory=dict)
    universe_id: str | None = None
    universe_version: str | None = None
    snapshot_hash: str | None = None
    interval: str = "1d"
    dataset_version: str | None = None
    source: str = "binance"
    quality: str = "HIGH"
    survivorship_bias: str = SURVIVORSHIP_BIAS_PRESENT

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", _normalize_symbol(self.symbol))
        if self.observed_at is not None:
            object.__setattr__(self, "observed_at", ensure_utc(self.observed_at))
        object.__setattr__(self, "bars", tuple(self.bars))
        object.__setattr__(self, "reasons", tuple(str(reason) for reason in self.reasons))
        object.__setattr__(self, "depth", dict(self.depth))
        object.__setattr__(self, "spread_evidence", dict(self.spread_evidence))
        object.__setattr__(self, "fill_evidence", dict(self.fill_evidence))
        if self.exchange_info is not None:
            object.__setattr__(self, "exchange_info", _public_copy(self.exchange_info))

    @property
    def order_book(self) -> OrderBookSnapshot | None:
        return self.book

    @property
    def asset(self) -> str | None:
        return self.asset_symbol

    @property
    def fresh_ticker(self) -> bool:
        return self.ticker_fresh

    @property
    def fresh_book(self) -> bool:
        return self.book_fresh

    @property
    def universe_snapshot_hash(self) -> str | None:
        return self.snapshot_hash

    @property
    def version(self) -> str | None:
        return self.universe_version

    @property
    def provenance(self) -> dict[str, Any]:
        return {
            "universe_id": self.universe_id,
            "universe_version": self.universe_version,
            "version": self.universe_version,
            "universe_snapshot_hash": self.snapshot_hash,
            "snapshot_hash": self.snapshot_hash,
            "interval": self.interval,
            "dataset_version": self.dataset_version,
            "source": self.source,
            "quality": self.quality,
            "survivorship_bias": self.survivorship_bias,
            "survivorship": self.survivorship_bias,
        }

    def as_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "asset_symbol": self.asset_symbol,
            "asset_id": self.asset_id,
            "rank": self.rank,
            "bars": [_public_copy(to_record(bar)) if hasattr(bar, "__dataclass_fields__") else _public_copy(bar) for bar in self.bars],
            "ticker": _public_copy(to_record(self.ticker)) if self.ticker is not None else None,
            "book": _public_copy(to_record(self.book)) if self.book is not None else None,
            "order_book": _public_copy(to_record(self.book)) if self.book is not None else None,
            "exchange_info": _public_copy(self.exchange_info),
            "tradable": self.tradable,
            "new_entry_allowed": self.new_entry_allowed,
            "selected": self.selected,
            "exit_only": self.exit_only,
            "timed_out": self.timed_out,
            "error": self.error,
            "reasons": list(self.reasons),
            "observed_at": _iso(self.observed_at),
            "ticker_fresh": self.ticker_fresh,
            "book_fresh": self.book_fresh,
            "depth": _public_copy(self.depth),
            "spread": self.spread,
            "spread_evidence": _public_copy(self.spread_evidence),
            "fill_evidence": _public_copy(self.fill_evidence),
            "provenance": self.provenance,
        }

    def __getitem__(self, key: str) -> Any:
        return self.as_dict()[key]


class BinanceMarketCollection(list[BinanceMarketSnapshot]):
    """List-compatible deterministic results with convenient symbol lookup."""

    def __init__(self, records: Iterable[BinanceMarketSnapshot], provenance: Mapping[str, Any]) -> None:
        super().__init__(records)
        self.provenance = dict(_public_copy(provenance))

    def __getitem__(self, key: int | slice | str) -> Any:
        if isinstance(key, str):
            wanted = _normalize_symbol(key)
            for record in self:
                if record.symbol == wanted:
                    return record
            raise KeyError(wanted)
        return super().__getitem__(key)

    @property
    def records(self) -> tuple[BinanceMarketSnapshot, ...]:
        return tuple(self)

    def as_mapping(self) -> dict[str, BinanceMarketSnapshot]:
        return {record.symbol: record for record in self}

    def as_dict(self) -> dict[str, Any]:
        return {"provenance": dict(self.provenance), "records": [record.as_dict() for record in self]}


class BoundedBinanceMarketCollector:
    """Collect public Spot evidence for an exact universe snapshot.

    The executor has at most four workers by construction.  A worker timeout
    affects only its symbol; healthy symbols are returned immediately and all
    records are finally ordered by the bound universe rank.
    """

    def __init__(
        self,
        provider: Any,
        universe_snapshot: UniverseSnapshot | Mapping[str, Any] | None = None,
        *,
        max_workers: int = 4,
        timeout: float = 10.0,
        per_symbol_timeout: float | None = None,
        clock: Callable[[], datetime] | None = None,
        grace: float | timedelta = 0.0,
        freshness: float | timedelta = 120.0,
        depth: int = 20,
        fill_quantity: float = 1.0,
        source: str | None = None,
        quality: str = "HIGH",
        survivorship_bias: str = SURVIVORSHIP_BIAS_PRESENT,
    ) -> None:
        if isinstance(provider, (UniverseSnapshot, Mapping)) and not isinstance(universe_snapshot, (UniverseSnapshot, Mapping)):
            provider, universe_snapshot = universe_snapshot, provider
        if provider is None:
            raise TypeError("provider is required")
        workers = int(max_workers)
        if isinstance(max_workers, bool) or workers <= 0 or workers > 4:
            raise ValueError("max_workers must be an integer in [1, 4]")
        selected_timeout = timeout if per_symbol_timeout is None else per_symbol_timeout
        timeout_value = float(selected_timeout)
        if not math.isfinite(timeout_value) or timeout_value <= 0:
            raise ValueError("timeout must be finite and positive")
        if isinstance(depth, bool) or int(depth) <= 0:
            raise ValueError("depth must be a positive integer")
        quantity = float(fill_quantity)
        if not math.isfinite(quantity) or quantity <= 0:
            raise ValueError("fill_quantity must be finite and positive")
        self.provider = provider
        self.universe_snapshot = self._coerce_snapshot(universe_snapshot)
        self.max_workers = workers
        self.timeout = timeout_value
        self.clock = clock or utc_now
        self.grace = _duration_seconds(grace, "grace")
        self.freshness = _duration_seconds(freshness, "freshness")
        self.depth_limit = int(depth)
        self.fill_quantity = quantity
        self.source = str(source or getattr(provider, "provider_name", "binance"))
        self.quality = str(quality)
        self.survivorship_bias = str(survivorship_bias)

    @staticmethod
    def _coerce_snapshot(value: UniverseSnapshot | Mapping[str, Any] | None) -> UniverseSnapshot:
        if isinstance(value, UniverseSnapshot):
            return value
        if isinstance(value, Mapping):
            return UniverseSnapshot.from_record(value, universe_id=value.get("universe_id"))
        raise TypeError("universe_snapshot must be a UniverseSnapshot or persisted mapping")

    def collect(
        self,
        universe_snapshot: UniverseSnapshot | Mapping[str, Any] | None = None,
        interval: str = "1d",
        limit: int = 1000,
        exit_symbols: Iterable[str] = (),
        *,
        start: datetime | None = None,
        end: datetime | None = None,
        dataset_version: str | None = None,
        source: str | None = None,
        quality: str | None = None,
        reconciliation: bool = False,
        now: datetime | None = None,
    ) -> BinanceMarketCollection:
        """Collect selected symbols, plus explicit exit-only symbols.

        ``universe_snapshot`` is optional only for constructor compatibility;
        when supplied it replaces the bound snapshot for this call and is
        still used verbatim (no refresh or membership reconstruction).
        """
        snapshot = self._coerce_snapshot(universe_snapshot) if universe_snapshot is not None else self.universe_snapshot
        interval = str(interval).strip()
        if not interval:
            raise ValueError("interval must not be empty")
        if isinstance(limit, bool) or int(limit) <= 0:
            raise ValueError("limit must be a positive integer")
        limit = int(limit)
        observed_at = ensure_utc(now or self.clock())
        exits = tuple(dict.fromkeys(_normalize_symbol(symbol) for symbol in exit_symbols))
        selected = self._universe_symbols(snapshot)
        selected_map = {entry["symbol"]: entry for entry in selected}
        requested = list(selected)
        for symbol in exits:
            if symbol not in selected_map:
                requested.append({
                    "symbol": symbol,
                    "asset_symbol": None,
                    "asset_id": None,
                    "rank": 10**9,
                    "selected": False,
                    "index": len(snapshot.records) + len(requested),
                })
        requested.sort(key=lambda entry: (entry["rank"], entry["index"]))
        requested_entries = tuple(requested)
        resolved_source = str(source or self.source)
        resolved_quality = str(quality or self.quality)
        resolved_dataset = str(dataset_version or _content_hash({
            "universe_version": snapshot.version,
            "snapshot_hash": snapshot.snapshot_hash,
            "symbols": [entry["symbol"] for entry in requested_entries],
            "interval": interval,
            "limit": limit,
        }))
        common = {
            "universe_id": snapshot.universe_id,
            "universe_version": snapshot.version,
            "snapshot_hash": snapshot.snapshot_hash,
            "interval": interval,
            "dataset_version": resolved_dataset,
            "source": resolved_source,
            "quality": resolved_quality,
            "survivorship_bias": str(snapshot.metadata.get("survivorship_bias", self.survivorship_bias)),
            "observed_at": observed_at,
        }

        def run(entry: Mapping[str, Any]) -> BinanceMarketSnapshot:
            return self._collect_symbol(
                entry,
                snapshot=snapshot,
                interval=interval,
                limit=limit,
                start=start,
                end=end,
                observed_at=observed_at,
                reconciliation=reconciliation or entry["symbol"] in exits,
                common=common,
            )

        futures: dict[Future[BinanceMarketSnapshot], Mapping[str, Any]] = {}
        started: dict[str, float] = {}
        start_lock = threading.Lock()

        def wrapped(entry: Mapping[str, Any]) -> BinanceMarketSnapshot:
            with start_lock:
                started[str(entry["symbol"])] = time.monotonic()
            return run(entry)

        executor = ThreadPoolExecutor(max_workers=self.max_workers, thread_name_prefix="binance-market")
        try:
            for entry in requested_entries:
                future = executor.submit(wrapped, entry)
                futures[future] = entry
            pending = set(futures)
            completed: dict[str, BinanceMarketSnapshot] = {}
            while pending:
                done, _ = wait(pending, timeout=0.02, return_when=FIRST_COMPLETED)
                for future in done:
                    pending.discard(future)
                    entry = futures[future]
                    try:
                        completed[str(entry["symbol"])] = future.result()
                    except BaseException as exc:
                        completed[str(entry["symbol"])] = self._error_snapshot(entry, common, f"{type(exc).__name__}: {exc}")
                current = time.monotonic()
                for future in tuple(pending):
                    entry = futures[future]
                    started_at = started.get(str(entry["symbol"]))
                    if started_at is None or current - started_at < self.timeout:
                        continue
                    pending.discard(future)
                    future.cancel()
                    completed[str(entry["symbol"])] = self._error_snapshot(entry, common, f"timeout after {self.timeout:g}s", timed_out=True)
            records = [completed[str(entry["symbol"])] for entry in requested_entries]
        finally:
            # A timed-out urllib call may still be in a worker.  Never wait for
            # it here: one bad symbol must not starve healthy records.
            executor.shutdown(wait=False, cancel_futures=True)
        return BinanceMarketCollection(records, {
            "universe_id": snapshot.universe_id,
            "universe_version": snapshot.version,
            "version": snapshot.version,
            "snapshot_hash": snapshot.snapshot_hash,
            "interval": interval,
            "dataset_version": resolved_dataset,
            "source": resolved_source,
            "quality": resolved_quality,
            "survivorship_bias": common["survivorship_bias"],
            "observed_at": _iso(observed_at),
        })

    def _universe_symbols(self, snapshot: UniverseSnapshot) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        for index, record in enumerate(snapshot.records):
            if not bool(record.get("selected")):
                continue
            raw = record.get("binance_symbol") or record.get("exchange_symbol") or record.get("symbol")
            if raw is None or not str(raw).strip():
                continue
            symbol = _normalize_symbol(raw)
            try:
                rank = int(record.get("rank", record.get("market_cap_rank", index + 1)))
            except (TypeError, ValueError):
                rank = index + 1
            result.append({
                "symbol": symbol,
                "asset_symbol": record.get("asset_symbol") or record.get("symbol") or record.get("base_asset"),
                "asset_id": record.get("asset_id") or record.get("id"),
                "rank": rank,
                "selected": True,
                "index": index,
            })
        result.sort(key=lambda entry: (entry["rank"], entry["index"]))
        return result

    def _collect_symbol(
        self,
        entry: Mapping[str, Any],
        *,
        snapshot: UniverseSnapshot,
        interval: str,
        limit: int,
        start: datetime | None,
        end: datetime | None,
        observed_at: datetime,
        reconciliation: bool,
        common: Mapping[str, Any],
    ) -> BinanceMarketSnapshot:
        symbol = str(entry["symbol"])
        reasons: list[str] = []
        exchange_info: Mapping[str, Any] | None = None
        try:
            exchange_info = self._exchange_info(symbol)
        except BaseException as exc:
            reasons.append(f"EXCHANGE_INFO_ERROR: {exc}")
        tradable = self._tradable(exchange_info, symbol)
        if exchange_info is None:
            reasons.append("TRADABILITY_UNAVAILABLE")
        elif not tradable:
            status = str(exchange_info.get("status", "")).upper()
            if status != "TRADING":
                reasons.append("NOT_TRADING")
            elif not _spot_allowed(exchange_info):
                reasons.append("SPOT_NOT_ALLOWED")
            elif str(exchange_info.get("quoteAsset", USDT)).upper() != USDT:
                reasons.append("QUOTE_NOT_USDT")
            else:
                reasons.append("NOT_TRADABLE")
        bars: tuple[Any, ...] = ()
        ticker: CryptoTicker | None = None
        book: OrderBookSnapshot | None = None
        try:
            bars = self._closed_bars(symbol, interval, limit, start, end, observed_at)
            if not bars:
                reasons.append("NO_CLOSED_BARS")
        except BaseException as exc:
            reasons.append(f"BARS_ERROR: {exc}")
        try:
            ticker = self._provider_call("ticker", symbol)
        except BaseException as exc:
            reasons.append(f"TICKER_ERROR: {exc}")
        try:
            book = self._provider_call("order_book", symbol, depth=self.depth_limit)
        except BaseException as exc:
            reasons.append(f"BOOK_ERROR: {exc}")
        ticker_fresh = ticker is not None and self._fresh(getattr(ticker, "timestamp", None), observed_at)
        book_fresh = book is not None and self._fresh(getattr(book, "timestamp", None), observed_at)
        if ticker is None:
            reasons.append("NO_TICKER")
        elif not ticker_fresh:
            reasons.append("TICKER_STALE")
        if book is None:
            reasons.append("NO_BOOK")
        elif not book_fresh:
            reasons.append("BOOK_STALE")
        depth_evidence, spread, spread_evidence, fill_evidence = self._book_evidence(book, ticker, ticker_fresh and book_fresh)
        selected = bool(entry.get("selected"))
        stale_universe = snapshot.status != "CURRENT"
        new_entry_allowed = selected and tradable and not stale_universe and not reconciliation
        if stale_universe:
            reasons.append("UNIVERSE_STALE")
        if reconciliation and not selected:
            reasons.append("EXIT_ONLY_SYMBOL")
        return BinanceMarketSnapshot(
            symbol=symbol,
            asset_symbol=str(entry.get("asset_symbol")) if entry.get("asset_symbol") is not None else None,
            asset_id=str(entry.get("asset_id")) if entry.get("asset_id") is not None else None,
            rank=int(entry["rank"]) if entry.get("rank") is not None else None,
            bars=bars,
            ticker=ticker,
            book=book,
            exchange_info=exchange_info,
            tradable=tradable,
            new_entry_allowed=new_entry_allowed,
            selected=selected,
            exit_only=reconciliation and not selected,
            reasons=tuple(dict.fromkeys(reasons)),
            ticker_fresh=ticker_fresh,
            book_fresh=book_fresh,
            depth=depth_evidence,
            spread=spread,
            spread_evidence=spread_evidence,
            fill_evidence=fill_evidence,
            **common,
        )

    def _error_snapshot(
        self,
        entry: Mapping[str, Any],
        common: Mapping[str, Any],
        error: str,
        *,
        timed_out: bool = False,
    ) -> BinanceMarketSnapshot:
        reason = "TIMEOUT" if timed_out else "ERROR"
        return BinanceMarketSnapshot(
            symbol=str(entry["symbol"]),
            asset_symbol=str(entry.get("asset_symbol")) if entry.get("asset_symbol") is not None else None,
            asset_id=str(entry.get("asset_id")) if entry.get("asset_id") is not None else None,
            rank=int(entry["rank"]) if entry.get("rank") is not None else None,
            selected=bool(entry.get("selected")),
            error=error,
            timed_out=timed_out,
            reasons=(reason,),
            **common,
        )

    def _provider_call(self, name: str, symbol: str, **kwargs: Any) -> Any:
        method = getattr(self.provider, name, None)
        if not callable(method):
            raise AttributeError(f"provider has no public {name} method")
        return _invoke(method, symbol, **kwargs)

    def _exchange_info(self, symbol: str) -> Mapping[str, Any] | None:
        method = getattr(self.provider, "exchange_info", None)
        if callable(method):
            payload = _invoke(method, symbol)
            record = _records_from_exchange(payload, symbol)
            if record is not None:
                return record
        method = getattr(self.provider, "exchange_symbols", None)
        if callable(method):
            payload = _invoke(method, symbol, quote_asset=USDT)
            if isinstance(payload, Mapping):
                record = _records_from_exchange(payload, symbol)
                if record is not None:
                    return record
            if isinstance(payload, Sequence) and not isinstance(payload, (str, bytes, bytearray)):
                for item in payload:
                    if isinstance(item, Mapping) and _normalize_symbol(item.get("symbol", "")) == symbol:
                        return _public_copy(item)
        metadata = getattr(self.provider, "metadata", None)
        if callable(metadata):
            instrument = _invoke(metadata, symbol)
            extra = getattr(instrument, "extra", None)
            if isinstance(extra, Mapping):
                record = dict(_public_copy(extra))
                record.setdefault("symbol", symbol)
                return record
        return None

    @staticmethod
    def _tradable(record: Mapping[str, Any] | None, symbol: str) -> bool:
        if record is None:
            return False
        return (
            _normalize_symbol(record.get("symbol", symbol)) == symbol
            and str(record.get("status", "")).upper() == "TRADING"
            and _spot_allowed(record)
            and str(record.get("quoteAsset", USDT)).upper() == USDT
        )

    def _closed_bars(
        self,
        symbol: str,
        interval: str,
        limit: int,
        start: datetime | None,
        end: datetime | None,
        observed_at: datetime,
    ) -> tuple[Any, ...]:
        method = None
        for name in ("closed_historical_ohlcv", "closed_ohlcv", "historical_ohlcv_closed", "closed_candles"):
            candidate = getattr(self.provider, name, None)
            if callable(candidate):
                method = candidate
                break
        closed_method = method is not None
        if method is None:
            method = getattr(self.provider, "historical_ohlcv", None)
        if not callable(method):
            raise AttributeError("provider has no public OHLCV method")
        rows = _invoke(method, symbol, start=start, end=end, interval=interval, limit=limit, now=observed_at, grace=self.grace)
        if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes, bytearray)):
            return ()
        result: list[Any] = []
        delta = _interval_delta(interval)
        cutoff = observed_at.timestamp() - self.grace
        for raw in rows:
            bar = _coerce_bar(raw)
            if bar is None:
                continue
            if not closed_method:
                close_time = _close_time(raw)
                if close_time is None and delta is not None:
                    close_time = bar.timestamp + delta
                if close_time is None or close_time.timestamp() > cutoff:
                    continue
            result.append(bar)
        result.sort(key=lambda bar: bar.timestamp)
        return tuple(result[-limit:])

    def _fresh(self, stamp: datetime | None, observed_at: datetime) -> bool:
        if stamp is None:
            return False
        try:
            age = (observed_at - ensure_utc(stamp)).total_seconds()
        except (TypeError, ValueError):
            return False
        return 0 <= age <= self.freshness

    def _book_evidence(
        self,
        book: OrderBookSnapshot | None,
        ticker: CryptoTicker | None,
        fresh: bool,
    ) -> tuple[dict[str, Any], float | None, dict[str, Any], dict[str, Any]]:
        if book is None:
            return ({"requested_levels": self.depth_limit, "bid_levels": 0, "ask_levels": 0, "fresh": False}, None, {}, {})
        bid_levels = len(book.bids)
        ask_levels = len(book.asks)
        bid_depth = sum(level.size for level in book.bids)
        ask_depth = sum(level.size for level in book.asks)
        depth = {
            "requested_levels": self.depth_limit,
            "bid_levels": bid_levels,
            "ask_levels": ask_levels,
            "total_levels": bid_levels + ask_levels,
            "bid_size": bid_depth,
            "ask_size": ask_depth,
            "fresh": fresh,
        }
        spread = None
        spread_evidence: dict[str, Any] = {"fresh": fresh, "bid": book.best_bid, "ask": book.best_ask}
        if book.best_bid is not None and book.best_ask is not None:
            spread = book.best_ask - book.best_bid
            midpoint = book.midpoint
            spread_evidence.update({"absolute": spread, "midpoint": midpoint, "bps": spread / midpoint * 10_000 if midpoint else None})
        fill: dict[str, Any] = {"quantity": self.fill_quantity, "fresh": fresh}
        for side, label in (("buy", "asks"), ("sell", "bids")):
            try:
                price, filled = book.executable_price("buy" if side == "buy" else "sell", self.fill_quantity)  # type: ignore[arg-type]
            except (TypeError, ValueError):
                # Domain Side is a str enum but providers may use a strict
                # implementation; calculating from levels keeps evidence local.
                levels = getattr(book, label)
                remaining = self.fill_quantity
                notional = 0.0
                filled = 0.0
                for level in levels:
                    take = min(remaining, level.size)
                    filled += take
                    notional += take * level.price
                    remaining -= take
                    if remaining <= 1e-12:
                        break
                price = notional / filled if filled else 0.0
            fill[side] = {"price": price, "filled_quantity": filled, "complete": filled + 1e-12 >= self.fill_quantity}
        return depth, spread, spread_evidence, fill


# Stable aliases used by integrations that call the item a canary observation.
BinanceCanarySnapshot = BinanceMarketSnapshot
BinanceCollection = BinanceMarketCollection

__all__ = [
    "PAPER",
    "BINANCE_SPOT_TESTNET",
    "BINANCE_SPOT_LIVE",
    "USDT",
    "REST_ORIGINS",
    "BinanceMarketSnapshot",
    "BinanceCanarySnapshot",
    "BinanceMarketCollection",
    "BinanceCollection",
    "BoundedBinanceMarketCollector",
]
