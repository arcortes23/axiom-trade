"""Resumable historical data bootstrap and BTC research workflows.

This module only uses the existing read-only Binance and Polymarket adapters.
Historical rows are staged before publication, immutable dataset catalog records
carry provenance, and prediction-market history stays explicitly price-proxy
when the source does not provide timestamped order-book depth.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
import time
from statistics import mean, median, pstdev
from typing import Any, Callable, Iterable, Mapping, Sequence

from .backtest import CryptoBacktester
from .crypto_universe import (
    CURRENT_UNIVERSE,
    SURVIVORSHIP_BIAS_PRESENT,
    UniverseConfig,
    UniverseSnapshot,
    load_crypto_universe,
    refresh_crypto_universe,
)
from .data import BinanceAdapter, PolymarketAdapter
from .data._http import HTTPFetchError
from .domain import MarketType, OHLCVBar, PredictionMarketSnapshot, ResearchQuality, SettlementState, parse_timestamp, to_record, utc_now
from .evaluation import dataset_version, walk_forward_splits
from .regime import RegimeEngine, RegimeState
from .storage import AxiomStore
from .strategy import validate_strategy


BTC_SYMBOL = "BTCUSDT"
BTC_HISTORY_START = datetime(2017, 8, 17, tzinfo=timezone.utc)
BTC_INTERVAL_SECONDS: dict[str, int] = {
    "1d": 86_400,
    "4h": 14_400,
    "1h": 3_600,
    "15m": 900,
}
BTC_DATASET_IDS = {interval: f"{BTC_SYMBOL}-{interval}-full" for interval in BTC_INTERVAL_SECONDS}
POLYMARKET_DATASET_ID = "Polymarket-historical"
POLYMARKET_HISTORICAL_JOB_NAME = "polymarket-historical-refresh"
BOOTSTRAP_PUBLICATION_CHUNK_SIZE = 512


@dataclass(frozen=True, slots=True)
class BootstrapReport:
    dataset_id: str
    source_type: str
    provider: str
    instrument: str
    timeframe: str
    status: str
    dataset_version: str | None
    records: int
    start_timestamp: datetime | None
    end_timestamp: datetime | None
    completeness: float
    missing_ranges: tuple[Mapping[str, Any], ...] = ()
    duplicates: int = 0
    retries: int = 0
    errors: tuple[str, ...] = ()
    metadata: Mapping[str, Any] | None = None
    universe_id: str | None = None
    universe_version: str | None = None
    survivorship_bias: str | None = None

    def as_record(self) -> dict[str, Any]:
        metadata = dict(self.metadata or {})
        universe_id = self.universe_id or metadata.get("universe_id")
        universe_version = self.universe_version or metadata.get("universe_version") or metadata.get("version")
        survivorship_bias = self.survivorship_bias or metadata.get("survivorship_bias")
        return {
            "dataset_id": self.dataset_id,
            "source_type": self.source_type,
            "provider": self.provider,
            "instrument": self.instrument,
            "timeframe": self.timeframe,
            "status": self.status,
            "dataset_version": self.dataset_version,
            "records": self.records,
            "start_timestamp": self.start_timestamp.isoformat() if self.start_timestamp else None,
            "end_timestamp": self.end_timestamp.isoformat() if self.end_timestamp else None,
            "completeness": self.completeness,
            "missing_ranges": [_jsonable(item) for item in self.missing_ranges],
            "duplicates": self.duplicates,
            "retries": self.retries,
            "errors": list(self.errors),
            "metadata": _jsonable(metadata),
            "universe_id": universe_id,
            "universe_version": universe_version,
            "survivorship_bias": survivorship_bias,
        }


@dataclass(frozen=True, slots=True)
class _CallResult:
    value: Any
    errors: tuple[str, ...]
    retries: int
    retry_after: float = 0.0
    retryable: bool = False
    request_failed: bool = False

def _coerce_retry_after(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed) or parsed < 0:
        return None
    return parsed



def _jsonable(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_jsonable(item) for item in value]
    if hasattr(value, "value") and not isinstance(value, (str, bytes)):
        return _jsonable(value.value)
    if hasattr(value, "__dataclass_fields__"):
        return _jsonable(to_record(value))
    return value


def _stable_hash(value: Any) -> str:
    encoded = json.dumps(_jsonable(value), sort_keys=True, separators=(",", ":"), allow_nan=False)
    return "sha256:" + hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _stamp(value: Any) -> datetime | None:
    parsed = parse_timestamp(value)
    return parsed.astimezone(timezone.utc) if parsed is not None else None


def _require_stamp(value: Any, name: str) -> datetime:
    result = _stamp(value)
    if result is None:
        raise ValueError(f"{name} must be a UTC timestamp")
    return result
def _align_start(value: datetime, interval_seconds: int) -> datetime:
    timestamp = _require_stamp(value, "timestamp")
    step = max(1, int(interval_seconds))
    seconds = int(timestamp.timestamp())
    aligned = ((seconds + step - 1) // step) * step
    return datetime.fromtimestamp(aligned, timezone.utc)


def _align_end(value: datetime, interval_seconds: int) -> datetime:
    timestamp = _require_stamp(value, "timestamp")
    step = max(1, int(interval_seconds))
    seconds = int(timestamp.timestamp())
    aligned = (seconds // step) * step
    return datetime.fromtimestamp(aligned, timezone.utc)



def _bar(value: Any) -> OHLCVBar | None:
    if isinstance(value, OHLCVBar):
        return value
    if not isinstance(value, Mapping):
        return None
    timestamp = _stamp(value.get("timestamp", value.get("time")))
    if timestamp is None:
        return None
    try:
        return OHLCVBar(
            timestamp=timestamp,
            open=float(value["open"]),
            high=float(value["high"]),
            low=float(value["low"]),
            close=float(value["close"]),
            volume=float(value.get("volume", 0.0)),
            spread=float(value["spread"]) if value.get("spread") is not None else None,
            trades=int(value["trades"]) if value.get("trades") is not None else None,
        )
    except (KeyError, TypeError, ValueError, OverflowError):
        return None


def _bar_identity(value: OHLCVBar) -> dict[str, Any]:
    return to_record(value)


def _dedupe_bars(values: Iterable[Any]) -> tuple[list[OHLCVBar], int, list[str]]:
    by_timestamp: dict[datetime, OHLCVBar] = {}
    duplicates = 0
    conflicts: list[str] = []
    for value in values:
        item = _bar(value)
        if item is None:
            continue
        previous = by_timestamp.get(item.timestamp)
        if previous is None:
            by_timestamp[item.timestamp] = item
            continue
        duplicates += 1
        if _stable_hash(_bar_identity(previous)) != _stable_hash(_bar_identity(item)):
            conflicts.append(item.timestamp.isoformat())
    return [by_timestamp[key] for key in sorted(by_timestamp)], duplicates, conflicts


def _missing_ranges(
    bars: Sequence[OHLCVBar],
    interval_seconds: int,
    requested_start: datetime,
    requested_end: datetime,
) -> tuple[tuple[dict[str, Any], ...], float]:
    """Describe absent cadence slots across the requested inclusive range."""
    start = _require_stamp(requested_start, "requested_start")
    end = _require_stamp(requested_end, "requested_end")
    if end < start:
        return (
            (
                {
                    "start": start.isoformat(),
                    "end": end.isoformat(),
                    "missing_intervals": 0,
                    "reason": "invalid_range",
                },
            ),
            0.0,
        )
    step = max(1, int(interval_seconds))
    expected = int((end - start).total_seconds() // step) + 1
    in_range = sorted(
        {
            item.timestamp
            for item in bars
            if start <= item.timestamp <= end
        }
    )
    present = set(in_range)
    ranges: list[dict[str, Any]] = []

    def add_range(first: datetime, last: datetime, reason: str) -> None:
        if first > last:
            return
        ranges.append(
            {
                "start": first.isoformat(),
                "end": last.isoformat(),
                "missing_intervals": int((last - first).total_seconds() // step) + 1,
                "reason": reason,
            }
        )

    cursor = start
    gap_start: datetime | None = None
    for _ in range(expected):
        if cursor not in present:
            gap_start = gap_start or cursor
        elif gap_start is not None:
            add_range(gap_start, cursor - timedelta(seconds=step), "cadence_gap")
            gap_start = None
        cursor += timedelta(seconds=step)
    if gap_start is not None:
        add_range(gap_start, end, "cadence_gap")
    missing = sum(int(item["missing_intervals"]) for item in ranges)
    return tuple(ranges), max(0.0, min(1.0, len(present) / max(1, expected)))


def _error_text(context: str, error: Any) -> str:
    status = getattr(error, "status", None)
    if status is not None:
        return f"{context}: HTTP {status}"
    return f"{context}: {error}"


def _consume_errors(provider: Any, context: str) -> tuple[tuple[str, ...], tuple[Any, ...]]:
    consume = getattr(provider, "consume_transport_errors", None)
    if not callable(consume):
        return (), ()
    try:
        raw = tuple(consume() or ())
    except Exception as exc:
        return (f"{context}: transport error collector failed: {exc}",), ()
    return tuple(_error_text(context, item) for item in raw), raw


def _retryable_exception(exc: Exception) -> bool:
    if isinstance(exc, HTTPFetchError):
        return bool(exc.retryable)
    return not isinstance(exc, (ValueError, TypeError, KeyError, AssertionError))


def _call_with_retries(
    provider: Any,
    operation: Callable[[], Any],
    *,
    context: str,
    max_attempts: int,
    backoff: float,
    sleep: Callable[[float], None],
) -> _CallResult:
    attempts = max(1, int(max_attempts))
    errors: list[str] = []
    retries = 0
    for attempt in range(attempts):
        try:
            value = operation()
            direct_error: Exception | None = None
        except Exception as exc:  # adapters are read-only and failures stay in the report
            value = None
            direct_error = exc
        transport_text, transport_errors = _consume_errors(provider, context)
        errors.extend(transport_text)
        retry_values = [
            parsed
            for parsed in (
                _coerce_retry_after(getattr(item, "retry_after", None))
                for item in transport_errors
            )
            if parsed is not None
        ]
        retry_after = max(retry_values, default=0.0)
        retryable = any(bool(getattr(item, "retryable", False)) for item in transport_errors)
        if direct_error is not None:
            errors.append(_error_text(context, direct_error))
            direct_retry_after = _coerce_retry_after(getattr(direct_error, "retry_after", None))
            if direct_retry_after is not None:
                retry_after = max(retry_after, direct_retry_after)
            retryable = _retryable_exception(direct_error)
        if not retryable or attempt + 1 >= attempts:
            return _CallResult(value, tuple(dict.fromkeys(errors)), retries, retry_after)
        retries += 1
        delay = retry_after if retry_after > 0 else min(60.0, max(0.0, float(backoff)) * (2**attempt))
        if delay > 0:
            sleep(delay)
    return _CallResult(None, tuple(dict.fromkeys(errors)), retries, retry_after)

def _single_provider_call(
    provider: Any,
    operation: Callable[[], Any],
    *,
    context: str,
) -> _CallResult:
    """Execute one bounded provider request without sleeping under storage locks."""
    direct_error: Exception | None = None
    try:
        value = operation()
    except Exception as exc:  # provider adapters are intentionally read-only
        value = None
        direct_error = exc
    transport_text, transport_errors = _consume_errors(provider, context)

    errors = list(transport_text)
    retry_values = [
        parsed
        for parsed in (
            _coerce_retry_after(getattr(item, "retry_after", None))
            for item in transport_errors
        )
        if parsed is not None
    ]
    retry_after = max(retry_values, default=0.0)
    retryable = any(bool(getattr(item, "retryable", False)) for item in transport_errors)
    if direct_error is not None:
        errors.append(_error_text(context, direct_error))
        direct_retry_after = _coerce_retry_after(getattr(direct_error, "retry_after", None))
        if direct_retry_after is not None:
            retry_after = max(retry_after, direct_retry_after)
        retryable = _retryable_exception(direct_error)
    return _CallResult(
        value,
        tuple(dict.fromkeys(errors)),
        0,
        retry_after=max(0.0, retry_after),
        retryable=bool(retryable),
        request_failed=bool(transport_errors) or (
            direct_error is not None and not isinstance(
                direct_error, (ValueError, TypeError, KeyError, AssertionError)
            )
        ),
    )





def _provider_name(provider: Any) -> str:
    return str(getattr(provider, "provider_name", provider.__class__.__name__)).strip() or provider.__class__.__name__


def _same_time(left: Any, right: Any) -> bool:
    a, b = _stamp(left), _stamp(right)
    return a is not None and b is not None and a == b


def _catalog_report(catalog: Mapping[str, Any], *, status: str = "COMPLETE") -> BootstrapReport:
    metadata = dict(catalog.get("metadata", {})) if isinstance(catalog.get("metadata"), Mapping) else {}
    return BootstrapReport(
        dataset_id=str(catalog["dataset_id"]),
        source_type=str(catalog.get("source_type", "HISTORICAL")),
        provider=str(catalog.get("provider", "")),
        instrument=str(catalog.get("instrument", "")),
        timeframe=str(catalog.get("timeframe", "")),
        status=status,
        dataset_version=str(catalog.get("dataset_version", catalog.get("version", ""))),
        records=int(catalog.get("row_count", 0)),
        start_timestamp=_stamp(catalog.get("start_timestamp")),
        end_timestamp=_stamp(catalog.get("end_timestamp")),
        completeness=float(catalog.get("completeness", 0.0)),
        missing_ranges=tuple(catalog.get("missing_ranges", ())),
        metadata=metadata,
        universe_id=str(metadata.get("universe_id")) if metadata.get("universe_id") is not None else None,
        universe_version=str(metadata.get("universe_version") or metadata.get("version")) if metadata.get("universe_version") is not None or metadata.get("version") is not None else None,
        survivorship_bias=str(metadata.get("survivorship_bias")) if metadata.get("survivorship_bias") is not None else None,
    )


def _canonical_bootstrap_symbol(symbol: Any) -> str:
    return str(symbol).replace("/", "").replace("-", "").replace("_", "").strip().upper()


def crypto_universe_dataset_id(universe_id: Any, universe_version: Any, symbol: Any, timeframe: Any) -> str:
    """Return the stable per-symbol/timeframe dataset binding."""
    def safe(value: Any) -> str:
        text = str(value).strip()
        return "".join(character if character.isalnum() or character in "._-" else "_" for character in text)
    return f"crypto-universe:{safe(universe_id)}:{safe(universe_version)}:{_canonical_bootstrap_symbol(symbol)}:{safe(timeframe)}"


def _listing_start(provider: Any, symbol: str, hint: Any = None) -> datetime | None:
    candidates: list[Any] = [hint]
    metadata_method = getattr(provider, "metadata", None)
    if callable(metadata_method):
        try:
            metadata = metadata_method(symbol)
        except Exception:
            metadata = None
        if isinstance(metadata, Mapping):
            candidates.extend((metadata, metadata.get("extra")))
        elif metadata is not None:
            candidates.extend((getattr(metadata, "extra", None), metadata))
    keys = (
        "listing_timestamp", "listing_start", "listed_at", "listing_date",
        "onboardDate", "onboard_date", "first_trade_timestamp", "first_available_timestamp",
    )
    def find(value: Any) -> Any:
        if isinstance(value, Mapping):
            for key in keys:
                if value.get(key) not in (None, ""):
                    return value[key]
            nested = value.get("extra")
            if nested is not value:
                return find(nested)
        for key in keys:
            item = getattr(value, key, None) if value is not None else None
            if item not in (None, ""):
                return item
        return None
    for candidate in candidates:
        stamp = _stamp(find(candidate))
        if stamp is not None:
            return stamp
    return None


def _coerce_universe_snapshot(
    store: AxiomStore,
    universe: UniverseSnapshot | Mapping[str, Any] | None,
    version: str | None,
) -> UniverseSnapshot | None:
    if universe is None:
        return load_crypto_universe(store, version=version)
    if not isinstance(universe, (UniverseSnapshot, Mapping)):
        raise TypeError("universe must be a UniverseSnapshot or mapping")

    def _text(value: Any) -> str:
        return str(value).strip()

    def _symbols(values: Any, name: str) -> tuple[str, ...]:
        if not isinstance(values, Sequence) or isinstance(values, (str, bytes, bytearray, Mapping)):
            raise ValueError(f"{name} must be a sequence of symbols")
        result: list[str] = []
        for item in values:
            if item is None:
                raise ValueError(f"{name} must not contain empty symbols")
            symbol = _text(item).upper()
            if not symbol:
                raise ValueError(f"{name} must not contain empty symbols")
            result.append(symbol)
        return tuple(result)

    def _records(values: Any) -> tuple[Mapping[str, Any], ...]:
        if not isinstance(values, Sequence) or isinstance(values, (str, bytes, bytearray, Mapping)):
            raise ValueError("records must be a sequence of mappings")
        result = tuple(item for item in values if isinstance(item, Mapping))
        if len(result) != len(values):
            raise ValueError("records must contain only mappings")
        return result

    supplied_records: tuple[Mapping[str, Any], ...] | None = None
    supplied_symbols: tuple[str, ...] | None = None
    supplied_hash: str | None = None
    if isinstance(universe, UniverseSnapshot):
        universe_id = _text(universe.universe_id)
        supplied_version = _text(universe.version) if universe.version is not None else ""
        supplied_records = tuple(universe.records)
        supplied_symbols = _symbols(universe.selected_symbols, "selected_symbols")
        if universe.snapshot_hash is not None:
            supplied_hash = _text(universe.snapshot_hash)
            if not supplied_hash:
                raise ValueError("snapshot_hash must not be empty")
    else:
        id_values: list[str] = []
        for key in ("universe_id", "id"):
            if key in universe and universe[key] is not None:
                value = _text(universe[key])
                if not value:
                    raise ValueError("universe_id is required")
                id_values.append(value)
        if not id_values:
            raise ValueError("universe_id is required")
        if len(set(id_values)) != 1:
            raise ValueError("universe_id fields do not match")
        universe_id = id_values[0]

        version_values: list[str] = []
        for key in ("universe_version", "version"):
            if key in universe and universe[key] is not None:
                value = _text(universe[key])
                if value:
                    version_values.append(value)
        if len(set(version_values)) != 1:
            raise ValueError("universe_version fields do not match")
        supplied_version = version_values[0] if version_values else ""

        if "snapshot_hash" in universe and universe["snapshot_hash"] is not None:
            supplied_hash = _text(universe["snapshot_hash"])
            if not supplied_hash:
                raise ValueError("snapshot_hash must not be empty")
        if "records" in universe and universe["records"] is not None:
            supplied_records = _records(universe["records"])
        symbol_values: list[tuple[str, ...]] = []
        for key in ("selected_symbols", "symbols"):
            if key in universe and universe[key] is not None:
                symbol_values.append(_symbols(universe[key], key))
        if symbol_values and any(item != symbol_values[0] for item in symbol_values[1:]):
            raise ValueError("selected symbol fields do not match")
        if symbol_values:
            supplied_symbols = symbol_values[0]

    if not universe_id:
        raise ValueError("universe_id is required")
    requested_version = _text(version) if version is not None else ""
    if version is not None and not requested_version:
        raise ValueError("universe_version is required")
    if requested_version and supplied_version and requested_version != supplied_version:
        message = (
            "universe_version does not match supplied snapshot"
            if isinstance(universe, UniverseSnapshot)
            else "universe_version does not match supplied universe"
        )
        raise ValueError(message)
    resolved_version = requested_version or supplied_version
    if not resolved_version:
        raise ValueError("versioned universe provenance requires universe_version")
    if resolved_version.casefold() in {"latest", "current", "default", "unversioned"}:
        raise ValueError("universe_version must be immutable and versioned")

    persisted = load_crypto_universe(store, universe_id=universe_id, version=resolved_version)
    if persisted is None:
        raise ValueError(f"no persisted crypto universe found for {universe_id}/{resolved_version}")
    if persisted.universe_id != universe_id or persisted.version != resolved_version:
        raise ValueError("persisted universe identity does not match supplied universe")
    if supplied_hash is not None and supplied_hash != (persisted.snapshot_hash or ""):
        raise ValueError("universe snapshot_hash does not match persisted snapshot")

    persisted_symbols = _symbols(persisted.selected_symbols, "persisted selected_symbols")
    if supplied_symbols is not None and supplied_symbols != persisted_symbols:
        raise ValueError("universe selected membership does not match persisted snapshot")
    if supplied_records is not None:
        supplied_selected = tuple(row for row in supplied_records if bool(row.get("selected", True)))
        supplied_record_json = json.dumps(
            _jsonable(supplied_records), sort_keys=True, separators=(",", ":"), allow_nan=False
        )
        persisted_record_json = json.dumps(
            _jsonable(persisted.records), sort_keys=True, separators=(",", ":"), allow_nan=False
        )
        selected_record_json = json.dumps(
            _jsonable(persisted.selected_records), sort_keys=True, separators=(",", ":"), allow_nan=False
        )
        selected_only = bool(supplied_records) and all(
            bool(row.get("selected", True)) for row in supplied_records
        )
        if supplied_record_json != persisted_record_json and not (
            selected_only
            and json.dumps(
                _jsonable(supplied_selected), sort_keys=True, separators=(",", ":"), allow_nan=False
            )
            == selected_record_json
        ):
            raise ValueError("universe records do not match persisted snapshot")
        supplied_record_symbols = _symbols(
            tuple(row.get("binance_symbol") or row.get("symbol") for row in supplied_selected),
            "selected record symbols",
        )
        if supplied_record_symbols != persisted_symbols:
            raise ValueError("universe selected membership does not match persisted snapshot")
    return persisted


class HistoricalBootstrapper:
    """Resumable, append-only bootstrap coordinator for public historical data."""

    def __init__(
        self,
        store: AxiomStore,
        *,
        crypto_provider: Any | None = None,
        prediction_provider: Any | None = None,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], datetime] = utc_now,
        max_attempts: int = 4,
        backoff: float = 0.5,
    ) -> None:
        if isinstance(max_attempts, bool) or int(max_attempts) < 1:
            raise ValueError("max_attempts must be positive")
        if float(backoff) < 0 or not math.isfinite(float(backoff)):
            raise ValueError("backoff must be finite and non-negative")
        self.store = store
        self.crypto_provider = crypto_provider or BinanceAdapter()
        self.prediction_provider = prediction_provider or PolymarketAdapter()
        self.sleep = sleep
        self.clock = clock
        self.max_attempts = int(max_attempts)
        self.backoff = float(backoff)

    def _publish_crypto_version(
        self,
        symbol: str,
        dataset_id: str,
        version: str,
        bars: Sequence[OHLCVBar],
        catalog: Mapping[str, Any],
    ) -> None:
        """Publish a fully staged immutable version in short writer windows."""
        if self.store.load_dataset_catalog(dataset_id, version) is None:
            for offset in range(0, len(bars), BOOTSTRAP_PUBLICATION_CHUNK_SIZE):
                self.store.publish_dataset_bars_chunk(
                    symbol,
                    bars[offset : offset + BOOTSTRAP_PUBLICATION_CHUNK_SIZE],
                    dataset_id=dataset_id,
                    dataset_version=version,
                )
        with self.store.transaction(immediate=True):
            if self.store.load_dataset_catalog(dataset_id, version) is None:
                published = self.store.count_bars(
                    symbol,
                    dataset_id=dataset_id,
                    dataset_version=version,
                )
                if published != len(bars):
                    raise ValueError(
                        f"immutable bar publication incomplete: {dataset_id}/{version} "
                        f"{published}/{len(bars)}"
                    )
                self.store.save_dataset_catalog(dataset_id, version, **dict(catalog))
            self.store.clear_dataset_staging_bars(dataset_id)

    def status(self) -> dict[str, Any]:
        return {
            "catalog": self.store.list_dataset_catalog(limit=10_000),
            "bootstrap": self.store.list_dataset_bootstrap_states(limit=10_000),
        }

    def bootstrap_crypto(
        self,
        *,
        intervals: Sequence[str] = tuple(BTC_INTERVAL_SECONDS),
        start: datetime = BTC_HISTORY_START,
        end: datetime | None = None,
        full_15m: bool = False,
        resume: bool = False,
    ) -> tuple[BootstrapReport, ...]:
        requested = tuple(dict.fromkeys(str(item).strip() for item in intervals))
        unknown = [item for item in requested if item not in BTC_INTERVAL_SECONDS]
        if unknown:
            raise ValueError(f"unsupported BTC interval(s): {', '.join(unknown)}")
        raw_end = _stamp(end) or _stamp(self.clock())
        if raw_end is None:
            raise ValueError("clock must return a UTC timestamp")
        raw_start = _stamp(start) or BTC_HISTORY_START
        if raw_start is None:
            raw_start = BTC_HISTORY_START
        output: list[BootstrapReport] = []
        for interval in requested:
            interval_start = _align_start(raw_start, BTC_INTERVAL_SECONDS[interval])
            interval_end = _align_end(raw_end, BTC_INTERVAL_SECONDS[interval])
            if interval == "15m" and not full_15m:
                interval_start = max(interval_start, _align_start(raw_end - timedelta(days=365 * 3), BTC_INTERVAL_SECONDS[interval]))
            output.append(
                self._bootstrap_crypto_interval(
                    interval,
                    requested_start=interval_start,
                    requested_end=interval_end,
                    resume=bool(resume),
                )
            )
        return tuple(output)

    def bootstrap_crypto_universe(
        self,
        universe: UniverseSnapshot | Mapping[str, Any] | None = None,
        *,
        universe_version: str | None = None,
        intervals: Sequence[str] = tuple(BTC_INTERVAL_SECONDS),
        start: datetime = BTC_HISTORY_START,
        end: datetime | None = None,
        full_15m: bool = False,
        resume: bool = False,
        max_symbols: int = 50,
    ) -> tuple[BootstrapReport, ...]:
        """Bootstrap each selected symbol/timeframe with independent state.

        The selected universe is resolved once.  Every durable dataset and
        cursor carries that exact snapshot version; a later universe refresh
        therefore cannot silently alter an in-flight or completed bootstrap.
        """
        if isinstance(max_symbols, bool) or not isinstance(max_symbols, int) or not 1 <= max_symbols <= 50:
            raise ValueError("max_symbols must be an integer from 1 to 50")
        snapshot = _coerce_universe_snapshot(self.store, universe, universe_version)
        if snapshot is None or snapshot.version is None:
            raise ValueError("a persisted versioned crypto universe is required")
        selected = tuple(dict.fromkeys(str(item).strip().upper() for item in snapshot.selected_symbols if str(item).strip()))
        if not selected:
            raise ValueError("crypto universe has no selected symbols")
        if len(selected) > max_symbols:
            raise ValueError(f"crypto universe exceeds max_symbols={max_symbols}")
        requested_intervals = tuple(dict.fromkeys(str(item).strip() for item in intervals))
        unknown = [item for item in requested_intervals if item not in BTC_INTERVAL_SECONDS]
        if unknown:
            raise ValueError(f"unsupported crypto interval(s): {', '.join(unknown)}")
        raw_end = _stamp(end) or _stamp(self.clock())
        raw_start = _stamp(start) or BTC_HISTORY_START
        if raw_end is None or raw_start is None:
            raise ValueError("bootstrap range must contain UTC timestamps")
        if raw_end < raw_start:
            raise ValueError("end must be on or after start")
        by_symbol = {
            _canonical_bootstrap_symbol(row.get("binance_symbol") or row.get("symbol")): row
            for row in snapshot.selected_records
            if row.get("binance_symbol") or row.get("symbol")
        }
        reports: list[BootstrapReport] = []
        for symbol in selected:
            listing_hint = by_symbol.get(symbol, {}).get("listing_timestamp") if by_symbol.get(symbol) else None
            listing = _listing_start(self.crypto_provider, symbol, listing_hint)
            for interval in requested_intervals:
                interval_start = _align_start(raw_start, BTC_INTERVAL_SECONDS[interval])
                if listing is not None:
                    interval_start = max(interval_start, _align_start(listing, BTC_INTERVAL_SECONDS[interval]))
                if interval == "15m" and not full_15m:
                    interval_start = max(interval_start, _align_start(raw_end - timedelta(days=365 * 3), BTC_INTERVAL_SECONDS[interval]))
                interval_end = _align_end(raw_end, BTC_INTERVAL_SECONDS[interval])
                dataset_id = crypto_universe_dataset_id(snapshot.universe_id, snapshot.version, symbol, interval)
                reports.append(
                    self._bootstrap_symbol_interval(
                        symbol,
                        interval,
                        dataset_id=dataset_id,
                        requested_start=interval_start,
                        requested_end=interval_end,
                        resume=bool(resume),
                        universe=snapshot,
                        listing_start=listing,
                    )
                )
        return tuple(reports)

    bootstrap_universe = bootstrap_crypto_universe
    bootstrap_selected_universe = bootstrap_crypto_universe

    def _bootstrap_symbol_interval(
        self,
        symbol: str,
        interval: str,
        *,
        dataset_id: str,
        requested_start: datetime,
        requested_end: datetime,
        resume: bool,
        universe: UniverseSnapshot,
        listing_start: datetime | None,
    ) -> BootstrapReport:
        provider = self.crypto_provider
        symbol = _canonical_bootstrap_symbol(symbol)
        step = timedelta(seconds=BTC_INTERVAL_SECONDS[interval])
        provenance = {
            "universe_id": universe.universe_id,
            "universe_version": universe.version,
            "universe_snapshot_hash": universe.snapshot_hash,
            "selected_symbol": symbol,
            "timeframe": interval,
            "source_type": "HISTORICAL",
            "survivorship_bias": SURVIVORSHIP_BIAS_PRESENT,
            "universe_labels": list(universe.labels),
            "listing_start": listing_start,
        }
        errors: list[str] = []
        retries = 0
        duplicate_count = 0
        latest = self.store.load_dataset_catalog(dataset_id)
        state = self.store.load_dataset_bootstrap_state(dataset_id)
        state_status = str(state.get("status", "")) if state else ""
        if state and state_status not in {"COMPLETE", "EMPTY"} and not resume:
            message = f"{dataset_id} has an incomplete bootstrap; rerun with --resume"
            return BootstrapReport(
                dataset_id, "HISTORICAL", _provider_name(provider), symbol, interval, "BLOCKED",
                None, 0, None, None, 0.0, errors=(message,), metadata=provenance,
                universe_id=universe.universe_id, universe_version=universe.version,
                survivorship_bias=SURVIVORSHIP_BIAS_PRESENT,
            )
        base_catalog = latest
        base_version = str(latest["dataset_version"]) if latest else None
        base_start = _stamp(latest.get("start_timestamp")) if latest else None
        if base_start is not None and base_start > requested_start:
            base_catalog = None
            base_version = None
        if latest is not None and base_catalog is not None and _same_time(latest.get("end_timestamp"), requested_end):
            if float(latest.get("completeness", 0.0)) >= 1.0 and not latest.get("missing_ranges"):
                self.store.clear_dataset_staging_bars(dataset_id)
                self.store.save_dataset_bootstrap_state(
                    dataset_id,
                    {
                        **provenance,
                        "provider": _provider_name(provider),
                        "instrument": symbol,
                        "market_type": MarketType.CRYPTO_SPOT.value,
                        "timeframe": interval,
                        "requested_start": requested_start,
                        "requested_end": requested_end,
                        "next_timestamp": requested_end + step,
                        "base_version": base_version,
                        "status": "COMPLETE",
                        "records": int(latest.get("row_count", 0)),
                        "message": "already complete",
                    },
                )
                return _catalog_report(latest)
        cursor = requested_start
        if state and state_status not in {"COMPLETE", "EMPTY"}:
            if not _same_time(state.get("requested_start"), requested_start) or not _same_time(state.get("requested_end"), requested_end):
                message = f"{dataset_id} bootstrap request differs from stored state"
                return BootstrapReport(
                    dataset_id, "HISTORICAL", _provider_name(provider), symbol, interval, "BLOCKED",
                    None, 0, None, None, 0.0, errors=(message,), metadata=provenance,
                    universe_id=universe.universe_id, universe_version=universe.version,
                    survivorship_bias=SURVIVORSHIP_BIAS_PRESENT,
                )
            if str(state.get("universe_version") or universe.version) != str(universe.version):
                message = f"{dataset_id} universe version changed; inspect status before resuming"
                return BootstrapReport(
                    dataset_id, "HISTORICAL", _provider_name(provider), symbol, interval, "BLOCKED",
                    None, 0, None, None, 0.0, errors=(message,), metadata=provenance,
                    universe_id=universe.universe_id, universe_version=universe.version,
                    survivorship_bias=SURVIVORSHIP_BIAS_PRESENT,
                )
            cursor = _stamp(state.get("next_timestamp")) or cursor
        elif base_catalog is not None:
            base_end = _stamp(base_catalog.get("end_timestamp"))
            if base_end is not None and base_end < requested_end and float(base_catalog.get("completeness", 0.0)) >= 1.0 and not base_catalog.get("missing_ranges"):
                cursor = max(cursor, base_end + step)
            elif base_catalog.get("missing_ranges"):
                cursor = requested_start
        self.store.save_dataset_bootstrap_state(
            dataset_id,
            {
                **provenance,
                "provider": _provider_name(provider),
                "instrument": symbol,
                "market_type": MarketType.CRYPTO_SPOT.value,
                "timeframe": interval,
                "requested_start": requested_start,
                "requested_end": requested_end,
                "next_timestamp": cursor,
                "base_version": base_version,
                "status": "RUNNING",
                "records_staged": len(self.store.load_dataset_staging_bars(dataset_id)),
                "errors": errors,
            },
        )
        chunk = step * max(1, min(999, 1000 - 1))
        fetch_complete = True
        while cursor <= requested_end:
            chunk_start = cursor
            chunk_end = min(requested_end, cursor + chunk - step)
            call = _call_with_retries(
                provider,
                lambda cursor=chunk_start, chunk_end=chunk_end: provider.historical_ohlcv(
                    symbol, start=cursor, end=chunk_end, interval=interval
                ),
                context=f"binance {symbol} {interval} {chunk_start.isoformat()}",
                max_attempts=self.max_attempts,
                backoff=self.backoff,
                sleep=self.sleep,
            )
            retries += call.retries
            errors.extend(call.errors)
            raw_values = call.value if isinstance(call.value, Sequence) and not isinstance(call.value, (str, bytes, Mapping)) else ()
            selected = [
                item for value in raw_values
                if (item := _bar(value)) is not None and chunk_start <= item.timestamp <= chunk_end
            ]
            normalized, duplicates, conflicts = _dedupe_bars(selected)
            duplicate_count += duplicates
            if conflicts:
                errors.append(f"conflicting duplicate bars at {', '.join(conflicts[:8])}")
            if normalized:
                try:
                    staged = self.store.save_dataset_staging_bars(dataset_id, normalized)
                    duplicate_count += int(staged["duplicates"])
                except ValueError as exc:
                    errors.append(str(exc))
                    fetch_complete = False
            if call.value is None or (call.errors and not normalized):
                fetch_complete = False
                errors.append(f"unfetched range: {chunk_start.isoformat()} to {chunk_end.isoformat()}")
                self.store.save_dataset_bootstrap_state(
                    dataset_id,
                    {
                        **provenance,
                        "provider": _provider_name(provider),
                        "instrument": symbol,
                        "market_type": MarketType.CRYPTO_SPOT.value,
                        "timeframe": interval,
                        "requested_start": requested_start,
                        "requested_end": requested_end,
                        "next_timestamp": chunk_start,
                        "base_version": base_version,
                        "status": "PARTIAL",
                        "records_staged": len(self.store.load_dataset_staging_bars(dataset_id)),
                        "retries": retries,
                        "errors": list(dict.fromkeys(errors[-32:])),
                    },
                )
                break
            cursor = chunk_end + step
            if normalized:
                cursor = max(cursor, normalized[-1].timestamp + step)
            self.store.save_dataset_bootstrap_state(
                dataset_id,
                {
                    **provenance,
                    "provider": _provider_name(provider),
                    "instrument": symbol,
                    "market_type": MarketType.CRYPTO_SPOT.value,
                    "timeframe": interval,
                    "requested_start": requested_start,
                    "requested_end": requested_end,
                    "next_timestamp": cursor,
                    "base_version": base_version,
                    "status": "RUNNING" if cursor <= requested_end else "FETCHED",
                    "records_staged": len(self.store.load_dataset_staging_bars(dataset_id)),
                    "retries": retries,
                    "errors": list(dict.fromkeys(errors[-32:])),
                },
            )
        base_bars: tuple[OHLCVBar, ...] = ()
        if base_catalog is not None:
            base_bars = tuple(self.store.load_bars(symbol, dataset_id=dataset_id, dataset_version=str(base_catalog["dataset_version"])))
        staged_bars = tuple(self.store.load_dataset_staging_bars(dataset_id))
        combined, combined_duplicates, conflicts = _dedupe_bars((*base_bars, *staged_bars))
        duplicate_count += combined_duplicates
        if conflicts:
            errors.append(f"conflicting duplicate bars at {', '.join(conflicts[:8])}")
        combined = [item for item in combined if requested_start <= item.timestamp <= requested_end]
        missing, completeness = _missing_ranges(combined, BTC_INTERVAL_SECONDS[interval], requested_start, requested_end)
        if not combined:
            status = "PARTIAL" if not fetch_complete else ("FAILED" if errors else "EMPTY")
            self.store.save_dataset_bootstrap_state(
                dataset_id,
                {
                    **provenance,
                    "provider": _provider_name(provider),
                    "instrument": symbol,
                    "market_type": MarketType.CRYPTO_SPOT.value,
                    "timeframe": interval,
                    "requested_start": requested_start,
                    "requested_end": requested_end,
                    "next_timestamp": cursor if status == "PARTIAL" else requested_end + step,
                    "base_version": base_version,
                    "status": status,
                    "records": 0,
                    "retries": retries,
                    "missing_ranges": list(missing),
                    "errors": list(dict.fromkeys(errors[-64:])),
                },
            )
            return BootstrapReport(
                dataset_id, "HISTORICAL", _provider_name(provider), symbol, interval, status,
                None, 0, None, None, completeness, missing, duplicate_count, retries,
                tuple(dict.fromkeys(errors)), provenance, universe.universe_id,
                universe.version, SURVIVORSHIP_BIAS_PRESENT,
            )
        version = dataset_version([_bar_identity(item) for item in combined])
        metadata = {
            **provenance,
            "provider": _provider_name(provider),
            "instrument": symbol,
            "dataset_id": dataset_id,
            "immutable_version": version,
            "requested_start": requested_start,
            "requested_end": requested_end,
            "integrity": {
                "sorted": all(a.timestamp < b.timestamp for a, b in zip(combined, combined[1:])),
                "duplicates_removed": duplicate_count,
                "conflicting_duplicates": conflicts,
                "missing_ranges": list(missing),
                "completeness": completeness,
            },
            "incremental": base_version is not None,
        }
        try:
            self._publish_crypto_version(
                symbol,
                dataset_id,
                version,
                combined,
                {
                    "provider": _provider_name(provider),
                    "instrument": symbol,
                    "market_type": MarketType.CRYPTO_SPOT,
                    "timeframe": interval,
                    "start_timestamp": combined[0].timestamp,
                    "end_timestamp": combined[-1].timestamp,
                    "row_count": len(combined),
                    "completeness": completeness,
                    "missing_ranges": missing,
                    "quality": "OHLCV",
                    "source_type": "HISTORICAL",
                    "snapshot_id": f"{dataset_id}:{version}",
                    "metadata": metadata,
                },
            )
        except ValueError as exc:
            errors.append(str(exc))
            return BootstrapReport(
                dataset_id, "HISTORICAL", _provider_name(provider), symbol, interval, "FAILED",
                None, len(combined), combined[0].timestamp, combined[-1].timestamp, completeness,
                missing, duplicate_count, retries, tuple(dict.fromkeys(errors)), metadata,
                universe.universe_id, universe.version, SURVIVORSHIP_BIAS_PRESENT,
            )
        status = "COMPLETE" if fetch_complete and completeness >= 1.0 and not missing else "PARTIAL"
        retry_cursor = requested_end + step if status == "COMPLETE" else (_stamp(missing[0].get("start")) if missing else cursor)
        self.store.save_dataset_bootstrap_state(
            dataset_id,
            {
                **provenance,
                "provider": _provider_name(provider),
                "instrument": symbol,
                "market_type": MarketType.CRYPTO_SPOT.value,
                "timeframe": interval,
                "requested_start": requested_start,
                "requested_end": requested_end,
                "next_timestamp": retry_cursor,
                "base_version": version,
                "status": status,
                "records": len(combined),
                "dataset_version": version,
                "retries": retries,
                "errors": list(dict.fromkeys(errors[-64:])),
                "missing_ranges": list(missing),
            },
        )
        report = BootstrapReport(
            dataset_id, "HISTORICAL", _provider_name(provider), symbol, interval, status,
            version, len(combined), combined[0].timestamp, combined[-1].timestamp,
            completeness, missing, duplicate_count, retries, tuple(dict.fromkeys(errors)),
            metadata, universe.universe_id, universe.version, SURVIVORSHIP_BIAS_PRESENT,
        )
        self.store.save_report_if_absent(
            f"historical-bootstrap:{dataset_id}:{version}", report.as_record(), experiment_id=dataset_id
        )
        return report

    def _bootstrap_crypto_interval(
        self,
        interval: str,
        *,
        requested_start: datetime,
        requested_end: datetime,
        resume: bool,
    ) -> BootstrapReport:
        provider = self.crypto_provider
        dataset_id = BTC_DATASET_IDS[interval]
        step = timedelta(seconds=BTC_INTERVAL_SECONDS[interval])
        errors: list[str] = []
        retries = 0
        duplicate_count = 0
        latest = self.store.load_dataset_catalog(dataset_id)
        state = self.store.load_dataset_bootstrap_state(dataset_id)
        state_status = str(state.get("status", "")) if state else ""
        if state and state_status not in {"COMPLETE", "EMPTY"} and not resume:
            message = f"{dataset_id} has an incomplete bootstrap; rerun with --resume"
            return BootstrapReport(dataset_id, "HISTORICAL", _provider_name(provider), BTC_SYMBOL, interval, "BLOCKED", None, 0, None, None, 0.0, errors=(message,))

        base_catalog = latest
        base_version: str | None = str(latest["dataset_version"]) if latest else None
        if latest is not None and (_stamp(latest.get("start_timestamp")) or requested_start) > requested_start:
            # A full 15m request may extend backwards beyond an existing rolling
            # snapshot; publish a new immutable version with the requested range.
            base_catalog = None
            base_version = None
        if latest is not None and base_catalog is not None and _same_time(latest.get("end_timestamp"), requested_end):
            if float(latest.get("completeness", 0.0)) >= 1.0 and not latest.get("missing_ranges"):
                self.store.clear_dataset_staging_bars(dataset_id)
                self.store.save_dataset_bootstrap_state(
                    dataset_id,
                    {
                        "provider": _provider_name(provider),
                        "instrument": BTC_SYMBOL,
                        "market_type": MarketType.CRYPTO_SPOT.value,
                        "timeframe": interval,
                        "requested_start": requested_start,
                        "requested_end": requested_end,
                        "next_timestamp": requested_end + step,
                        "base_version": base_version,
                        "status": "COMPLETE",
                        "records": int(latest.get("row_count", 0)),
                        "message": "already complete",
                    },
                )
                return _catalog_report(latest)

        cursor = requested_start
        if state and state_status not in {"COMPLETE", "EMPTY"}:
            if not _same_time(state.get("requested_start"), requested_start) or not _same_time(state.get("requested_end"), requested_end):
                message = f"{dataset_id} bootstrap request differs from stored state"
                return BootstrapReport(dataset_id, "HISTORICAL", _provider_name(provider), BTC_SYMBOL, interval, "BLOCKED", None, 0, None, None, 0.0, errors=(message,))
            if str(state.get("base_version") or "") != str(base_version or ""):
                message = f"{dataset_id} base dataset changed; inspect status before resuming"
                return BootstrapReport(dataset_id, "HISTORICAL", _provider_name(provider), BTC_SYMBOL, interval, "BLOCKED", None, 0, None, None, 0.0, errors=(message,))
            cursor = _stamp(state.get("next_timestamp")) or cursor
        elif base_catalog is not None and _stamp(base_catalog.get("end_timestamp")) is not None:
            base_end = _stamp(base_catalog.get("end_timestamp"))
            if base_end is not None and base_end < requested_end and float(base_catalog.get("completeness", 0.0)) >= 1.0 and not base_catalog.get("missing_ranges"):
                cursor = max(cursor, base_end + step)
            elif base_catalog.get("missing_ranges"):
                cursor = requested_start

        self.store.save_dataset_bootstrap_state(
            dataset_id,
            {
                "provider": _provider_name(provider),
                "instrument": BTC_SYMBOL,
                "market_type": MarketType.CRYPTO_SPOT.value,
                "timeframe": interval,
                "requested_start": requested_start,
                "requested_end": requested_end,
                "next_timestamp": cursor,
                "base_version": base_version,
                "status": "RUNNING",
                "records_staged": len(self.store.load_dataset_staging_bars(dataset_id)),
                "errors": errors,
            },
        )

        chunk_bars = max(1, min(900, 1000 - 1))
        chunk = step * chunk_bars
        fetch_complete = True
        while cursor <= requested_end:
            chunk_start = cursor
            chunk_end = min(requested_end, cursor + chunk - step)
            call = _call_with_retries(
                provider,
                lambda cursor=chunk_start, chunk_end=chunk_end: provider.historical_ohlcv(
                    BTC_SYMBOL,
                    start=cursor,
                    end=chunk_end,
                    interval=interval,
                ),
                context=f"binance {BTC_SYMBOL} {interval} {chunk_start.isoformat()}",
                max_attempts=self.max_attempts,
                backoff=self.backoff,
                sleep=self.sleep,
            )
            retries += call.retries
            errors.extend(call.errors)
            raw_values = call.value if isinstance(call.value, Sequence) and not isinstance(call.value, (str, bytes, Mapping)) else ()
            selected: list[OHLCVBar] = []
            for value in raw_values:
                item = _bar(value)
                if item is not None and chunk_start <= item.timestamp <= chunk_end:
                    selected.append(item)
            normalized, duplicates, conflicts = _dedupe_bars(selected)
            duplicate_count += duplicates
            if conflicts:
                errors.append(f"conflicting duplicate bars at {', '.join(conflicts[:8])}")
            if normalized:
                staged = self.store.save_dataset_staging_bars(dataset_id, normalized)
                duplicate_count += int(staged["duplicates"])
            if call.value is None or (call.errors and not normalized):
                fetch_complete = False
                errors.append(
                    f"unfetched range: {chunk_start.isoformat()} to {chunk_end.isoformat()}"
                )
                self.store.save_dataset_bootstrap_state(
                    dataset_id,
                    {
                        "provider": _provider_name(provider),
                        "instrument": BTC_SYMBOL,
                        "market_type": MarketType.CRYPTO_SPOT.value,
                        "timeframe": interval,
                        "requested_start": requested_start,
                        "requested_end": requested_end,
                        "next_timestamp": chunk_start,
                        "base_version": base_version,
                        "status": "PARTIAL",
                        "records_staged": len(self.store.load_dataset_staging_bars(dataset_id)),
                        "retries": retries,
                        "errors": list(dict.fromkeys(errors[-32:])),
                    },
                )
                break
            cursor = chunk_end + step
            if normalized:
                cursor = max(cursor, normalized[-1].timestamp + step)
            self.store.save_dataset_bootstrap_state(
                dataset_id,
                {
                    "provider": _provider_name(provider),
                    "instrument": BTC_SYMBOL,
                    "market_type": MarketType.CRYPTO_SPOT.value,
                    "timeframe": interval,
                    "requested_start": requested_start,
                    "requested_end": requested_end,
                    "next_timestamp": cursor,
                    "base_version": base_version,
                    "status": "RUNNING" if cursor <= requested_end else "FETCHED",
                    "records_staged": len(self.store.load_dataset_staging_bars(dataset_id)),
                    "chunks_completed": int((cursor - requested_start).total_seconds() // max(1, chunk.total_seconds())),
                    "retries": retries,
                    "errors": list(dict.fromkeys(errors[-32:])),
                },
            )

        base_bars = ()
        if base_catalog is not None:
            base_bars = tuple(
                self.store.load_bars(
                    str(base_catalog.get("instrument", BTC_SYMBOL)),
                    dataset_id=dataset_id,
                    dataset_version=str(base_catalog["dataset_version"]),
                )
            )
        staged_bars = tuple(self.store.load_dataset_staging_bars(dataset_id))
        combined, combined_duplicates, conflicts = _dedupe_bars((*base_bars, *staged_bars))
        duplicate_count += combined_duplicates
        if conflicts:
            errors.append(f"conflicting duplicate bars at {', '.join(conflicts[:8])}")
        combined = [
            item
            for item in combined
            if requested_start <= item.timestamp <= requested_end
        ]
        missing, completeness = _missing_ranges(combined, BTC_INTERVAL_SECONDS[interval], requested_start, requested_end)
        if not combined:
            status = "PARTIAL" if not fetch_complete else ("FAILED" if errors else "EMPTY")
            self.store.save_dataset_bootstrap_state(
                dataset_id,
                {
                    "provider": _provider_name(provider),
                    "instrument": BTC_SYMBOL,
                    "market_type": MarketType.CRYPTO_SPOT.value,
                    "timeframe": interval,
                    "requested_start": requested_start,
                    "requested_end": requested_end,
                    "next_timestamp": cursor if status == "PARTIAL" else requested_end + step,
                    "base_version": base_version,
                    "status": status,
                    "records": 0,
                    "retries": retries,
                    "missing_ranges": list(missing),
                    "errors": list(dict.fromkeys(errors[-64:])),
                },
            )
            return BootstrapReport(dataset_id, "HISTORICAL", _provider_name(provider), BTC_SYMBOL, interval, status, None, 0, None, None, completeness, missing, duplicate_count, retries, tuple(dict.fromkeys(errors)))

        version = dataset_version([_bar_identity(item) for item in combined])
        metadata = {
            "provider": _provider_name(provider),
            "source_type": "HISTORICAL",
            "symbol": BTC_SYMBOL,
            "instrument": BTC_SYMBOL,
            "timeframe": interval,
            "requested_start": requested_start,
            "requested_end": requested_end,
            "timezone": "UTC",
            "adapter": provider.__class__.__name__,
            "source_url": getattr(provider, "base_url", "https://api.binance.com"),
            "base_version": base_version,
            "immutable_version": version,
            "integrity": {
                "sorted": all(a.timestamp < b.timestamp for a, b in zip(combined, combined[1:])),
                "duplicates_removed": duplicate_count,
                "conflicting_duplicates": conflicts,
                "missing_ranges": list(missing),
                "completeness": completeness,
            },
            "incremental": base_version is not None,
        }
        try:
            self._publish_crypto_version(
                BTC_SYMBOL,
                dataset_id,
                version,
                combined,
                {
                    "provider": _provider_name(provider),
                    "instrument": BTC_SYMBOL,
                    "market_type": MarketType.CRYPTO_SPOT,
                    "timeframe": interval,
                    "start_timestamp": combined[0].timestamp,
                    "end_timestamp": combined[-1].timestamp,
                    "row_count": len(combined),
                    "completeness": completeness,
                    "missing_ranges": missing,
                    "quality": "OHLCV",
                    "source_type": "HISTORICAL",
                    "snapshot_id": f"{dataset_id}:{version}",
                    "metadata": metadata,
                },
            )
        except ValueError as exc:
            errors.append(str(exc))
            self.store.save_dataset_bootstrap_state(
                dataset_id,
                {
                    "provider": _provider_name(provider),
                    "instrument": BTC_SYMBOL,
                    "market_type": MarketType.CRYPTO_SPOT.value,
                    "timeframe": interval,
                    "requested_start": requested_start,
                    "requested_end": requested_end,
                    "next_timestamp": cursor,
                    "base_version": base_version,
                    "status": "FAILED",
                    "records": len(combined),
                    "retries": retries,
                    "errors": list(dict.fromkeys(errors[-64:])),
                },
            )
            return BootstrapReport(dataset_id, "HISTORICAL", _provider_name(provider), BTC_SYMBOL, interval, "FAILED", None, len(combined), combined[0].timestamp, combined[-1].timestamp, completeness, missing, duplicate_count, retries, tuple(dict.fromkeys(errors)), metadata)

        status = "COMPLETE" if fetch_complete and completeness >= 1.0 and not missing else "PARTIAL"
        retry_cursor = requested_end + step
        if status != "COMPLETE" and missing:
            retry_cursor = _stamp(missing[0].get("start")) or cursor
        self.store.save_dataset_bootstrap_state(
            dataset_id,
            {
                "provider": _provider_name(provider),
                "instrument": BTC_SYMBOL,
                "market_type": MarketType.CRYPTO_SPOT.value,
                "timeframe": interval,
                "requested_start": requested_start,
                "requested_end": requested_end,
                "next_timestamp": retry_cursor,
                "base_version": version,
                "status": status,
                "records": len(combined),
                "dataset_version": version,
                "retries": retries,
                "errors": list(dict.fromkeys(errors[-64:])),
                "missing_ranges": list(missing),
            },
        )
        report = BootstrapReport(
            dataset_id,
            "HISTORICAL",
            _provider_name(provider),
            BTC_SYMBOL,
            interval,
            status,
            version,
            len(combined),
            combined[0].timestamp,
            combined[-1].timestamp,
            completeness,
            missing,
            duplicate_count,
            retries,
            tuple(dict.fromkeys(errors)),
            metadata,
        )
        self.store.save_report_if_absent(f"historical-bootstrap:{dataset_id}:{version}", report.as_record(), experiment_id=dataset_id)
        return report

    def bootstrap_polymarket(
        self,
        *,
        max_markets: int = 1000,
        resume: bool = False,
        request_budget: int | None = None,
        market_budget: int | None = None,
        max_requests: int | None = None,
    ) -> BootstrapReport:
        """Advance one bounded, durable Polymarket historical-refresh tick.

        Discovery and market requests are deliberately performed outside any
        explicit store transaction.  The cursor and every market outcome are
        checkpointed after each network boundary so a process restart resumes
        from the last opaque Gamma cursor and retries only failed markets.
        """
        if isinstance(max_markets, bool) or not isinstance(max_markets, int) or max_markets < 0:
            raise ValueError("max_markets must be a non-negative integer")
        if max_requests is not None:
            if request_budget is not None and int(max_requests) != int(request_budget):
                raise ValueError("request_budget and max_requests disagree")
            request_budget = max_requests
        if request_budget is not None and (
            isinstance(request_budget, bool)
            or not isinstance(request_budget, int)
            or request_budget < 0
        ):
            raise ValueError("request_budget must be a non-negative integer")
        if market_budget is not None and (
            isinstance(market_budget, bool)
            or not isinstance(market_budget, int)
            or market_budget < 0
        ):
            raise ValueError("market_budget must be a non-negative integer")
        target_markets = int(max_markets)
        market_limit = target_markets if market_budget is None else min(
            target_markets, int(market_budget)
        )
        request_limit = (
            max(1, target_markets * 4 + 1)
            if request_budget is None
            else int(request_budget)
        )
        provider = self.prediction_provider
        provider_name = _provider_name(provider)
        now = _stamp(self.clock()) or utc_now()
        state = self.store.load_dataset_bootstrap_state(POLYMARKET_DATASET_ID) or {}
        # A zero effective budget is an intentional no-op.  In particular,
        # do not clear durable IDs or rebuild/publish an empty aggregate just
        # because a caller asked for a bounded tick with no capacity.
        if market_limit == 0 or request_limit == 0:
            latest = self.store.load_dataset_catalog(POLYMARKET_DATASET_ID)

            def _catalog_int(value: Any) -> int:
                try:
                    return max(0, int(value))
                except (TypeError, ValueError, OverflowError):
                    return 0

            def _catalog_float(value: Any) -> float:
                try:
                    parsed = float(value)
                except (TypeError, ValueError, OverflowError):
                    return 0.0
                return parsed if math.isfinite(parsed) else 0.0

            def _state_ids(key: str) -> tuple[str, ...]:
                raw = state.get(key, ())
                if isinstance(raw, (str, bytes, Mapping)):
                    return ()
                try:
                    values = iter(raw)
                except TypeError:
                    return ()
                return tuple(
                    sorted(
                        {
                            str(item).strip()
                            for item in values
                            if str(item).strip()
                        }
                    )
                )

            discovered_snapshot = _state_ids("discovered_market_ids")
            processed_snapshot = _state_ids("processed_market_ids")
            no_work_metadata = {
                "job_name": POLYMARKET_HISTORICAL_JOB_NAME,
                "no_work_reason": "EFFECTIVE_BUDGET_ZERO",
                "market_limit": target_markets,
                "market_budget": market_limit,
                "request_budget": request_limit,
                "discovery_complete": bool(state.get("discovery_complete", False)),
                "discovered_markets": len(discovered_snapshot),
                "processed_markets": len(processed_snapshot),
                "changed": False,
            }
            return BootstrapReport(
                POLYMARKET_DATASET_ID,
                "HISTORICAL",
                provider_name,
                "POLYMARKET",
                "event",
                "EXHAUSTED",
                str(latest.get("dataset_version")) if latest else None,
                _catalog_int(latest.get("row_count")) if latest else 0,
                _stamp(latest.get("start_timestamp")) if latest else None,
                _stamp(latest.get("end_timestamp")) if latest else None,
                _catalog_float(latest.get("completeness")) if latest else 0.0,
                errors=("effective budget is zero; no work performed",),
                metadata=no_work_metadata,
            )

        prior_status = str(state.get("status") or "").upper()
        if (
            state
            and prior_status not in {
                "COMPLETE",
                "EMPTY",
                "NO_NEW_DATA",
                "EXHAUSTED",
            }
            and not resume
        ):
            latest = self.store.load_dataset_catalog(POLYMARKET_DATASET_ID)
            return BootstrapReport(
                POLYMARKET_DATASET_ID,
                "HISTORICAL",
                provider_name,
                "POLYMARKET",
                "event",
                "SCHEDULED",
                str(latest.get("dataset_version")) if latest else None,
                int(latest.get("row_count", 0)) if latest else 0,
                _stamp(latest.get("start_timestamp")) if latest else None,
                _stamp(latest.get("end_timestamp")) if latest else None,
                float(latest.get("completeness", 0.0)) if latest else 0.0,
                errors=("incomplete historical refresh is resumable; pass resume=True to advance",),
            )

        def _positive_or_zero(value: Any, default: int = 0) -> int:
            try:
                parsed = int(value)
            except (TypeError, ValueError):
                return default
            return parsed if parsed >= 0 else default

        discovered_ids = {
            str(item).strip()
            for item in state.get("discovered_market_ids", ())
            if str(item).strip()
        }
        processed = {
            str(item).strip()
            for item in state.get("processed_market_ids", ())
            if str(item).strip()
        }
        market_statuses = {
            str(key): str(value).upper()
            for key, value in (
                state.get("market_statuses", {})
                if isinstance(state.get("market_statuses"), Mapping)
                else {}
            ).items()
            if str(key).strip()
        }
        # Processed IDs are durable commit markers.  Discard legacy markers
        # without a non-empty immutable constituent catalog so an
        # interrupted/empty market can be resumed instead of being skipped.
        for market_id in tuple(processed):
            catalog = self.store.load_dataset_catalog(f"prediction:{market_id}")
            try:
                row_count = int(catalog.get("row_count", 0)) if catalog else 0
            except (TypeError, ValueError):
                row_count = 0
            if catalog is None or row_count <= 0:
                processed.discard(market_id)
                market_statuses.pop(market_id, None)
        # Discovery is a resumable frontier, while processed IDs are durable
        # publication markers.  A legacy state or a query-scope migration may
        # have reset that frontier; never let it omit a valid published
        # constituent from the discovered universe.
        discovered_ids.update(processed)
        failed_markets: dict[str, dict[str, Any]] = {}
        raw_failed = state.get("failed_markets", {})
        if isinstance(raw_failed, Mapping):
            for key, value in raw_failed.items():
                if isinstance(value, Mapping) and str(key).strip():
                    failed_markets[str(key)] = dict(value)
        discovery_cursor = state.get("discovery_cursor")
        discovery_cursor = (
            str(discovery_cursor) if isinstance(discovery_cursor, str) and discovery_cursor else None
        )
        query_fingerprint = str(state.get("query_fingerprint") or "").strip() or None
        discovery_complete = bool(state.get("discovery_complete", False))
        if target_markets == 0:
            discovery_complete = True
            discovered_ids.clear()
            processed.clear()
        errors: list[str] = []
        no_new_data_count = _positive_or_zero(state.get("no_new_data_count"))
        retry_count = _positive_or_zero(state.get("retry_count"))
        request_count = _positive_or_zero(state.get("request_count"))
        tick_requests = 0
        error_count = _positive_or_zero(state.get("error_count"))
        request_failed = False
        budget_exhausted = False
        changed_market = False
        changed_aggregate = False
        attestation: Mapping[str, Any] | None = None
        discovery_retry_after: float | None = None
        discovery_next_attempt_at: datetime | None = None
        discovery_failed = False
        discovery_scope_reset = False


        def _coverage() -> dict[str, Any]:
            return {
                "market_limit": target_markets,
                "market_budget": market_limit,
                "request_budget": request_limit,
                "discovery_complete": discovery_complete,
                "discovered_markets": len(discovered_ids),
                "processed_markets": len(processed),
                "history_endpoint": "CLOB /prices-history for the aligned YES token",
            }
        def _completeness() -> float:
            # The processed set is a subset of the discovered universe by
            # construction.  Keep the bound as a defensive guard for any
            # malformed legacy payload while preserving that invariant.
            discovered_ids.update(processed)
            return min(
                1.0,
                len(processed) / len(discovered_ids),
            ) if discovered_ids else 0.0

        def _next_failed_at() -> datetime | None:
            values = [
                _stamp(value.get("next_attempt_at"))
                for value in failed_markets.values()
                if isinstance(value, Mapping)
            ]
            values = [value for value in values if value is not None]
            return min(values) if values else None

        def _persist(
            status: str,
            *,
            last_error: str | None = None,
            next_attempt_at: datetime | None = None,
        ) -> None:
            # Keep the durable publication set in the discovered universe on
            # every checkpoint, including checkpoints taken during migration
            # or after individual market outcomes.
            discovered_ids.update(processed)
            retry_values = (
                [discovery_retry_after]
                if discovery_retry_after is not None
                else []
            )
            retry_values.extend(
                parsed
                for item in failed_markets.values()
                if isinstance(item, Mapping)
                for parsed in (_coerce_retry_after(item.get("retry_after")),)
                if parsed is not None
            )

            payload: dict[str, Any] = {
                "provider": provider_name,
                "instrument": "POLYMARKET",
                "market_type": MarketType.PREDICTION.value,
                "timeframe": "event",
                "requested_start": None,
                "requested_end": None,
                "next_timestamp": None,
                "base_version": state.get("base_version"),
                "status": str(status).upper(),
                "discovery_cursor": discovery_cursor,
                "query_fingerprint": query_fingerprint,
                "discovery_complete": discovery_complete,
                "discovered_market_ids": sorted(discovered_ids),
                "processed_market_ids": sorted(processed),
                "market_statuses": dict(sorted(market_statuses.items())),
                "failed_markets": {
                    key: dict(value) for key, value in sorted(failed_markets.items())
                },
                "request_budget": request_limit,
                "market_budget": market_limit,
                "request_count": request_count,
                "error_count": error_count,
                "no_new_data_count": no_new_data_count,
                "retry_count": retry_count,
                "retry_after": max(retry_values, default=0.0),
                "backoff": self.backoff,
                "next_attempt_at": (
                    next_attempt_at
                    or discovery_next_attempt_at
                    or _next_failed_at()
                ),

                "requested_coverage": _coverage(),
                "honest_gaps": [
                    {
                        "market_id": key,
                        "status": value.get("status"),
                        "reason": value.get("last_error"),
                        "next_attempt_at": value.get("next_attempt_at"),
                    }
                    for key, value in sorted(failed_markets.items())
                ],
                "errors": list(dict.fromkeys(errors[-64:])),
                "updated_at": now.isoformat(),
            }
            state.update(payload)
            self.store.save_dataset_bootstrap_state(POLYMARKET_DATASET_ID, payload)
            job_payload = dict(payload)
            job_payload["job_kind"] = "POLYMARKET_HISTORICAL_REFRESH"
            job_payload["resumable"] = True
            self.store.set_operator_job(
                POLYMARKET_HISTORICAL_JOB_NAME,
                str(status).upper(),
                job_payload,
                pid=None,
                started_at=_stamp(state.get("started_at")) or now,
                last_error=last_error or (errors[-1] if errors else None),
                resumable=True,
                timestamp=now,
            )
        def _record_market_failure(
            market_id: str,
            failed: Mapping[str, Any] | None,
            phase_error: str,
            retry_after: float = 0.0,
        ) -> None:
            nonlocal retry_count
            previous_attempts = _positive_or_zero(
                failed.get("attempts") if failed else 0
            )
            retry_count += int(previous_attempts > 0)
            delay = retry_after or min(
                3600.0, self.backoff * (2**previous_attempts)
            )
            retry_at = now + timedelta(seconds=max(0.0, delay))
            failed_markets[market_id] = {
                "attempts": previous_attempts + 1,
                "status": "FAILED",
                "last_error": phase_error,
                "retry_after": retry_after,
                "next_attempt_at": retry_at.isoformat(),
            }
            market_statuses[market_id] = "FAILED"
            _persist("FAILED", last_error=phase_error, next_attempt_at=retry_at)


        # A scheduled retry is not allowed to consume a request before its
        # persisted Retry-After/backoff boundary.
        scheduled_at = _stamp(state.get("next_attempt_at"))
        if scheduled_at is not None and scheduled_at > now and (
            prior_status in {"SCHEDULED", "FAILED", "PARTIAL"}
        ):
            latest = self.store.load_dataset_catalog(POLYMARKET_DATASET_ID)
            retry_after = _coerce_retry_after(state.get("retry_after"))
            next_attempt_text = scheduled_at.isoformat()
            return BootstrapReport(
                POLYMARKET_DATASET_ID,
                "HISTORICAL",
                provider_name,
                "POLYMARKET",
                "event",
                "SCHEDULED",
                str(latest.get("dataset_version")) if latest else None,
                int(latest.get("row_count", 0)) if latest else 0,
                _stamp(latest.get("start_timestamp")) if latest else None,
                _stamp(latest.get("end_timestamp")) if latest else None,
                float(latest.get("completeness", 0.0)) if latest else 0.0,
                errors=tuple(dict.fromkeys(errors)),
                metadata={
                    "job_name": POLYMARKET_HISTORICAL_JOB_NAME,
                    "bootstrap_status": "WAITING_FOR_RETRY",
                    "next_attempt_at": next_attempt_text,
                    "retry_after": retry_after,
                    "backoff": {
                        "status": "WAITING",
                        "retry_after": retry_after,
                        "next_attempt_at": next_attempt_text,
                    },
                    "requested_coverage": _coverage(),
                },
            )


        _persist("RUNNING")
        def _request(operation: Callable[[], Any], context: str) -> _CallResult | None:
            nonlocal request_count, tick_requests, error_count, request_failed, budget_exhausted
            if tick_requests >= request_limit:
                budget_exhausted = True
                return None
            tick_requests += 1
            request_count += 1
            result = _single_provider_call(provider, operation, context=context)
            if result.errors:
                errors.extend(result.errors)
            if result.request_failed:
                error_count += 1
                request_failed = True
            return result

        # One page per tick keeps discovery bounded and persists the opaque
        # cursor and scope fingerprint before any market detail requests.
        if not discovery_complete and len(discovered_ids) < target_markets:
            page_size = max(1, min(100, target_markets - len(discovered_ids)))
            market_page = getattr(provider, "market_page", None)
            if callable(market_page):
                page_call = _request(
                    lambda: market_page(
                        page_size,
                        after_cursor=discovery_cursor,
                        closed=True,
                    ),
                    "polymarket market discovery",
                )
                if page_call is not None and page_call.value is not None:
                    page = page_call.value
                    snapshots = getattr(page, "snapshots", ())
                    page_fingerprint = str(
                        getattr(page, "query_fingerprint", "") or ""
                    ).strip()
                    fingerprint_changed = bool(
                        page_fingerprint
                        and query_fingerprint
                        and page_fingerprint != query_fingerprint
                    )
                    if fingerprint_changed:
                        # A cursor is scoped to the query that produced it.
                        # Discard the fetched page rather than mixing scopes,
                        # but retain every successfully published constituent.
                        query_fingerprint = page_fingerprint
                        discovery_cursor = None
                        discovery_complete = False
                        discovered_ids = set(processed)
                        market_statuses = {
                            key: value
                            for key, value in market_statuses.items()
                            if key in processed
                        }
                        failed_markets.clear()
                        discovery_scope_reset = True
                    else:
                        if query_fingerprint is None and page_fingerprint:
                            query_fingerprint = page_fingerprint
                        if isinstance(snapshots, Sequence) and not isinstance(
                            snapshots, (str, bytes, Mapping)
                        ):
                            for item in snapshots:
                                market_id = str(getattr(item, "market_id", "")).strip()
                                if market_id:
                                    discovered_ids.add(market_id)
                        if page_call.request_failed and not snapshots:
                            discovery_failed = True
                            discovery_retry_after = _coerce_retry_after(page_call.retry_after)
                            if discovery_retry_after is not None and discovery_retry_after > 0:
                                discovery_next_attempt_at = now + timedelta(
                                    seconds=discovery_retry_after
                                )
                        returned_cursor = getattr(page, "next_cursor", None)
                        discovery_cursor = (
                            str(returned_cursor)
                            if isinstance(returned_cursor, str) and returned_cursor
                            else None
                        )
                        if str(getattr(page, "coverage_status", "")).upper() == "ERROR":
                            reason = str(getattr(page, "error_reason", "") or "MALFORMED_PAGE")
                            errors.append(f"polymarket market discovery: {reason}")
                            request_failed = True
                        elif discovery_cursor is None or len(discovered_ids) >= target_markets:
                            discovery_complete = True
                elif page_call is None:
                    budget_exhausted = True
            elif callable(getattr(provider, "markets", None)):
                remaining = target_markets - len(discovered_ids)
                market_call = _request(
                    lambda: provider.markets(active=False, limit=remaining),
                    "polymarket market discovery",
                )
                if market_call is not None:
                    values = market_call.value
                    if market_call.request_failed and not values:
                        discovery_failed = True
                        discovery_retry_after = _coerce_retry_after(market_call.retry_after)
                        if discovery_retry_after is not None and discovery_retry_after > 0:
                            discovery_next_attempt_at = now + timedelta(
                                seconds=discovery_retry_after
                            )
                    elif values is not None:
                        if isinstance(values, Sequence) and not isinstance(
                            values, (str, bytes, Mapping)
                        ):
                            for item in values:
                                market_id = str(getattr(item, "market_id", "")).strip()
                                if market_id:
                                    discovered_ids.add(market_id)
                        discovery_complete = True
                elif market_call is None:
                    budget_exhausted = True
            else:
                errors.append("polymarket market discovery: provider has no discovery API")
                request_failed = True
            if discovery_scope_reset:
                _persist("SCHEDULED")
            else:
                _persist(
                    "PARTIAL" if discovery_failed else "RUNNING",
                    next_attempt_at=discovery_next_attempt_at if discovery_failed else None,
                )


        attempted_markets = 0
        for market_id in sorted(discovered_ids):
            if market_id in processed:
                continue
            if attempted_markets >= market_limit:
                break
            failed = failed_markets.get(market_id)
            due_at = _stamp(failed.get("next_attempt_at")) if failed else None
            if due_at is not None and due_at > now:
                continue
            if tick_requests >= request_limit:
                budget_exhausted = True
                break
            attempted_markets += 1
            market_call = _request(
                lambda market_id=market_id: provider.market(market_id),
                f"polymarket market {market_id}",
            )
            if market_call is None:
                budget_exhausted = True
                break
            discovered_market = (
                market_call.value
                if isinstance(market_call.value, PredictionMarketSnapshot)
                else None
            )
            if discovered_market is None:
                phase_error = (
                    market_call.errors[-1]
                    if market_call.request_failed and market_call.errors
                    else f"polymarket market {market_id}: "
                    + ("request failed" if market_call.request_failed else "identity unavailable")
                )
                if not market_call.request_failed:
                    errors.append(phase_error)
                _record_market_failure(
                    market_id,
                    failed,
                    phase_error,
                    market_call.retry_after,
                )
                continue
            metadata_call = _request(
                lambda market_id=market_id: provider.metadata(market_id),
                f"polymarket metadata {market_id}",
            )
            if metadata_call is None:
                budget_exhausted = True
                break
            if metadata_call.request_failed:
                phase_error = (
                    metadata_call.errors[-1]
                    if metadata_call.errors
                    else f"polymarket metadata {market_id}: request failed"
                )
                _record_market_failure(
                    market_id,
                    failed,
                    phase_error,
                    metadata_call.retry_after,
                )
                continue
            history_call = _request(
                lambda market_id=market_id: provider.price_history(market_id),
                f"polymarket price history {market_id}",
            )
            if history_call is None:
                budget_exhausted = True
                break
            if history_call.request_failed:
                phase_error = (
                    history_call.errors[-1]
                    if history_call.errors
                    else f"polymarket price history {market_id}: request failed"
                )
                _record_market_failure(
                    market_id,
                    failed,
                    phase_error,
                    history_call.retry_after,
                )
                continue
            instrument = metadata_call.value
            history = _normalize_prediction_history(discovered_market, history_call.value)
            if not history:
                no_new_data_count += 1
                phase_error = (
                    f"polymarket price history {market_id}: "
                    "empty or incomplete historical payload"
                )
                errors.append(phase_error)
                _record_market_failure(market_id, failed, phase_error)
                continue
            category = classify_market_category(discovered_market)
            extra = getattr(instrument, "extra", {}) if instrument is not None else {}
            token_ids = {
                "yes": discovered_market.yes_token_id
                or (extra.get("yes_token_id") if isinstance(extra, Mapping) else None),
                "no": discovered_market.no_token_id
                or (extra.get("no_token_id") if isinstance(extra, Mapping) else None),
            }
            history_has_order_book = any(
                item.get("order_book") is not None for item in history
            )
            history_quality = (
                "HISTORICAL_ORDER_BOOK"
                if history_has_order_book
                else ResearchQuality.PRICE_PROXY.value
            )
            metadata_payload = {
                "source_type": "HISTORICAL",
                "provider": provider_name,
                "market_id": market_id,
                "condition_id": discovered_market.condition_id,
                "question": discovered_market.question,
                "resolution_criteria": discovered_market.resolution_criteria,
                "settlement": discovered_market.settlement.value,
                "volume": discovered_market.volume,
                "liquidity": discovered_market.liquidity,
                "expiry": discovered_market.expiry,
                "category": category,
                "tags": list(discovered_market.tags),
                "token_ids": token_ids,
                "instrument_metadata": to_record(instrument) if instrument is not None else None,
                "raw_market": to_record(discovered_market),
                "historical_order_book_available": history_has_order_book,
                "research_quality": history_quality,
                "provenance_version": "dataset-provenance-v1",
                "policy_version": "prediction-integrity-v1",
            }
            metadata_hash = _stable_hash(metadata_payload)
            try:
                self.store.save_polymarket_market_metadata(
                    market_id,
                    metadata_payload,
                    observed_at=now,
                    metadata_hash=metadata_hash,
                    source_type="HISTORICAL",
                )
            except ValueError as exc:
                errors.append(f"{market_id}: metadata persistence: {exc}")
            version = _stable_hash(history)
            constituent_id = f"prediction:{market_id}"
            previous_catalog = self.store.load_dataset_catalog(constituent_id)
            try:
                previous_rows = int(previous_catalog.get("row_count", 0)) if previous_catalog else 0
                previous_completeness = (
                    float(previous_catalog.get("completeness", 0.0))
                    if previous_catalog
                    else 0.0
                )
            except (TypeError, ValueError):
                previous_rows = 0
                previous_completeness = 0.0
            previous_complete = (
                previous_catalog is not None
                and str(previous_catalog.get("dataset_version", "")) == version
                and previous_rows == len(history)
                and previous_rows > 0
                and math.isfinite(previous_completeness)
                and previous_completeness >= 1.0
            )
            market_changed = not previous_complete
            publication_accepted = not market_changed
            if market_changed:
                changed_market = True
                immutable_rows: list[dict[str, Any]] = []
                for point in history:
                    immutable_rows.append(
                        {
                            "source_type": "HISTORICAL",
                            "provider": provider_name,
                            "market_id": market_id,
                            "condition_id": discovered_market.condition_id,
                            "question": discovered_market.question,
                            "timestamp": point["timestamp"],
                            "source_timestamp": point["timestamp"],
                            "price": point["price"],
                            "yes_mid": point["price"],
                            "token_id": point["token_id"],
                            "category": category,
                            "tags": list(discovered_market.tags),
                            "settlement": discovered_market.settlement.value,
                            "resolution_criteria": discovered_market.resolution_criteria,
                            "volume": discovered_market.volume,
                            "liquidity": discovered_market.liquidity,
                            "expiry": discovered_market.expiry,
                            "order_book": point.get("order_book"),
                            "research_quality": history_quality,
                            "historical_order_book": point.get("order_book") is not None,
                            "executable_quote": False,
                        }
                    )
                try:
                    # Keep the immutable constituent dataset, every snapshot,
                    # and its catalog publication in one rollback boundary.
                    # All network reads above are complete before entering
                    # this transaction.
                    with self.store.transaction(immediate=True):
                        if self.store.load_dataset(constituent_id, version) is None:
                            self.store.save_dataset(
                                constituent_id,
                                version,
                                immutable_rows,
                                metadata=metadata_payload,
                                quality=history_quality,
                            )
                        for point, immutable_row in zip(history, immutable_rows):
                            snapshot_id = (
                                f"pmhist:{market_id}:{version}:"
                                f"{point['token_id']}:{point['timestamp'].isoformat()}"
                            )
                            self.store.save_polymarket_snapshot(
                                snapshot_id,
                                market_id,
                                point["timestamp"],
                                now,
                                immutable_row,
                                quality=history_quality,
                                source_type="HISTORICAL",
                            )
                        self.store.save_dataset_catalog(
                            constituent_id,
                            version,
                            provider=provider_name,
                            instrument=str(
                                getattr(instrument, "symbol", market_id) or market_id
                            ),
                            market_type=MarketType.PREDICTION,
                            timeframe="event",
                            start_timestamp=history[0]["timestamp"] if history else None,
                            end_timestamp=history[-1]["timestamp"] if history else None,
                            row_count=len(history),
                            completeness=1.0 if history else 0.0,
                            missing_ranges=(),
                            quality=history_quality,
                            source_type="HISTORICAL",
                            snapshot_id=f"pmhist:{market_id}:{version}",
                            metadata={
                                **metadata_payload,
                                "dataset_id": constituent_id,
                                "dataset_version": version,
                            },
                        )
                        published_catalog = self.store.load_dataset_catalog(
                            constituent_id,
                            version,
                        )
                        try:
                            published_rows = (
                                int(published_catalog.get("row_count", 0))
                                if published_catalog
                                else 0
                            )
                            published_completeness = (
                                float(published_catalog.get("completeness", 0.0))
                                if published_catalog
                                else 0.0
                            )
                        except (TypeError, ValueError):
                            published_rows = 0
                            published_completeness = 0.0
                        if (
                            published_catalog is None
                            or str(published_catalog.get("dataset_version", "")) != version
                            or published_rows != len(history)
                            or published_rows <= 0
                            or not math.isfinite(published_completeness)
                            or published_completeness < 1.0
                        ):
                            raise ValueError(
                                f"immutable publication incomplete: {constituent_id}/{version}"
                            )
                    publication_accepted = True
                except (TypeError, ValueError) as exc:
                    phase_error = f"{market_id}: immutable publication: {exc}"
                    errors.append(phase_error)
                    _record_market_failure(market_id, failed, phase_error)
                    continue
            if not publication_accepted:
                phase_error = (
                    f"{market_id}: immutable publication was not accepted"
                )
                errors.append(phase_error)
                _record_market_failure(market_id, failed, phase_error)
                continue
            market_statuses[market_id] = "COMPLETE"
            processed.add(market_id)
            failed_markets.pop(market_id, None)
            _persist("RUNNING")

        # Rebuild the aggregate only from exact immutable constituent catalog
        # versions.  Its version is therefore a content/identity hash, not a
        # refresh timestamp.
        market_versions: list[dict[str, Any]] = []
        category_counts: dict[str, int] = {}
        starts: list[datetime] = []
        ends: list[datetime] = []
        imported = 0
        points_total = 0
        timestamped_order_books = 0
        for market_id in sorted(processed):
            catalog = self.store.load_dataset_catalog(f"prediction:{market_id}")
            if catalog is None:
                continue
            metadata = catalog.get("metadata", {})
            metadata = metadata if isinstance(metadata, Mapping) else {}
            rows = int(catalog.get("row_count", 0))
            category = str(metadata.get("category") or "other")
            has_book = bool(metadata.get("historical_order_book_available", False))
            category_counts[category] = category_counts.get(category, 0) + 1
            timestamped_order_books += int(has_book)
            points_total += rows
            imported += int(rows > 0)
            version = str(catalog.get("dataset_version", ""))
            market_versions.append(
                {
                    "market_id": market_id,
                    "dataset_id": f"prediction:{market_id}",
                    "version": version,
                    "dataset_version": version,
                    "records": rows,
                    "category": category,
                    "historical_order_book": has_book,
                }
            )
            start = _stamp(catalog.get("start_timestamp"))
            end = _stamp(catalog.get("end_timestamp"))
            if start is not None:
                starts.append(start)
            if end is not None:
                ends.append(end)
        aggregate_version = _stable_hash(market_versions)
        aggregate_start = min(starts) if starts else None
        aggregate_end = max(ends) if ends else None
        aggregate_has_order_book = bool(market_versions) and (
            timestamped_order_books == len(market_versions)
        )
        aggregate_quality = (
            "HISTORICAL_ORDER_BOOK"
            if aggregate_has_order_book
            else ResearchQuality.PRICE_PROXY.value
        )
        aggregate_metadata = {
            "source_type": "HISTORICAL",
            "provider": provider_name,
            "instrument": "POLYMARKET",
            "markets_discovered": len(discovered_ids),
            "markets_imported": imported,
            "price_points": points_total,
            "category_counts": category_counts,
            "market_versions": market_versions,
            "research_quality": aggregate_quality,
            "historical_order_book_available": aggregate_has_order_book,
            "provenance_version": "dataset-provenance-v1",
            "policy_version": "prediction-integrity-v1",
            "requested_coverage": _coverage(),
            "honest_gaps": [
                {
                    "market_id": key,
                    "status": value.get("status"),
                    "reason": value.get("last_error"),
                    "next_attempt_at": value.get("next_attempt_at"),
                }
                for key, value in sorted(failed_markets.items())
            ],
            "note": (
                "Only timestamped CLOB price history is stored; no historical "
                "depth, spread, fills, or executable quotes are fabricated."
            ),
        }
        previous_aggregate = self.store.load_dataset_catalog(POLYMARKET_DATASET_ID)
        changed_aggregate = (
            previous_aggregate is None
            or str(previous_aggregate.get("dataset_version", "")) != aggregate_version
        )
        if changed_aggregate:
            changed_aggregate = True
            try:
                self.store.save_dataset_catalog(
                    POLYMARKET_DATASET_ID,
                    aggregate_version,
                    provider=provider_name,
                    instrument="POLYMARKET",
                    market_type=MarketType.PREDICTION,
                    timeframe="event",
                    start_timestamp=aggregate_start,
                    end_timestamp=aggregate_end,
                    row_count=points_total,
                    completeness=_completeness(),
                    missing_ranges=tuple(
                        {
                            "market_id": key,
                            "reason": value.get("last_error"),
                        }
                        for key, value in sorted(failed_markets.items())
                    ),
                    quality=aggregate_quality,
                    source_type="HISTORICAL",
                    snapshot_id=f"{POLYMARKET_DATASET_ID}:{aggregate_version}",
                    metadata=aggregate_metadata,
                )
                attestation = self.store.verify_dataset_integrity_attestation(
                    POLYMARKET_DATASET_ID,
                    aggregate_version,
                    force=True,
                )
            except (TypeError, ValueError) as exc:
                errors.append(
                    f"{POLYMARKET_DATASET_ID}: immutable publication: {exc}"
                )
        else:
            aggregate_version = str(previous_aggregate.get("dataset_version", aggregate_version))
        remaining = (
            len(discovered_ids) < target_markets
            or any(
                market_id not in processed
                for market_id in discovered_ids
            )
        )
        if discovery_scope_reset:
            final_status = "SCHEDULED"
        elif discovery_failed:
            final_status = "PARTIAL"
        elif request_failed:
            final_status = "FAILED"
        elif budget_exhausted and remaining:
            final_status = "EXHAUSTED"
        elif remaining:
            final_status = "SCHEDULED"
        elif errors:
            final_status = "FAILED"
        elif not changed_aggregate and previous_aggregate is not None:
            final_status = "NO_NEW_DATA"
        elif discovered_ids:
            final_status = "COMPLETE"
        else:
            final_status = "NO_NEW_DATA"
        state["base_version"] = aggregate_version or state.get("base_version")
        _persist(final_status)
        completeness = _completeness()
        report_metadata: dict[str, Any] = {
            **aggregate_metadata,
            "job_name": POLYMARKET_HISTORICAL_JOB_NAME,
            "request_count": request_count,
            "error_count": error_count,
            "no_new_data_count": no_new_data_count,
            "retry_count": retry_count,
            "request_budget": request_limit,
            "market_budget": market_limit,
            "discovery_cursor": discovery_cursor,
            "query_fingerprint": query_fingerprint,
            "next_attempt_at": discovery_next_attempt_at or _next_failed_at(),
            "retry_after": discovery_retry_after,
            "changed": bool(changed_aggregate),
            "attestation": dict(attestation) if isinstance(attestation, Mapping) else None,
        }
        report = BootstrapReport(
            POLYMARKET_DATASET_ID,
            "HISTORICAL",
            provider_name,
            "POLYMARKET",
            "event",
            final_status,
            aggregate_version or None,
            points_total,
            aggregate_start,
            aggregate_end,
            completeness,
            tuple(
                {
                    "market_id": key,
                    "reason": value.get("last_error"),
                }
                for key, value in sorted(failed_markets.items())
            ),
            0,
            retry_count,
            tuple(dict.fromkeys(errors)),
            report_metadata,
        )
        self.store.save_report_if_absent(
            f"historical-bootstrap:{POLYMARKET_DATASET_ID}:"
            f"{aggregate_version or 'none'}:{final_status}:{request_count}",
            report.as_record(),
            experiment_id=POLYMARKET_DATASET_ID,
        )
        return report
    def publish_polymarket_forward_replay(
        self,
        *,
        cutoff: datetime | None = None,
        dataset_id: str = "Polymarket-recorded-book-replay",
        market_ids: Sequence[str] | None = None,
        max_rows: int | None = None,
    ) -> dict[str, Any]:
        """Freeze an exact, provenance-preserving replay of forward snapshots.

        A replay remains ``FORWARD_COLLECTED`` because it is a retrospective
        view of captured evidence, not a historical provider reconstruction.
        ``research_mode`` is the only new interpretation label.
        """
        cutoff_value = _stamp(cutoff) or _stamp(self.clock()) or utc_now()
        dataset_identifier = str(dataset_id).strip()
        if not dataset_identifier:
            raise ValueError("dataset_id is required")
        if max_rows is not None and (
            isinstance(max_rows, bool)
            or not isinstance(max_rows, int)
            or max_rows < 0
        ):
            raise ValueError("max_rows must be a non-negative integer")
        selected = (
            tuple(
                dict.fromkeys(
                    str(item).strip()
                    for item in market_ids
                    if str(item).strip()
                )
            )
            if market_ids is not None
            else None
        )
        raw_rows = self.store.load_polymarket_snapshots(
            source_type="FORWARD_COLLECTED",
            end=cutoff_value,
            limit=max_rows,
        )
        rows: list[dict[str, Any]] = []
        for row in raw_rows:
            market_id = str(row.get("market_id") or "").strip()
            if selected is not None and market_id not in selected:
                continue
            observed_at = _stamp(row.get("observed_at"))
            source_timestamp = _stamp(row.get("source_timestamp"))
            if not market_id or observed_at is None or observed_at > cutoff_value:
                continue
            payload = row.get("payload")
            if not isinstance(payload, Mapping):
                continue
            row_source_type = str(
                row.get("source_type") or payload.get("source_type") or ""
            ).strip().upper()
            if row_source_type != "FORWARD_COLLECTED":
                continue
            if str(payload.get("source_type") or "").strip().upper() == "PAPER_FORWARD":
                raise ValueError("forward replay cannot include PAPER_FORWARD snapshots")
            snapshot_payload = payload.get("snapshot", payload)
            if not isinstance(snapshot_payload, Mapping):
                continue
            record = dict(snapshot_payload)
            record.update(
                {
                    key: payload[key]
                    for key in (
                        "yes_order_book",
                        "no_order_book",
                        "order_book",
                        "available_at",
                        "response_received_at",
                        "provider_timestamp",
                    )
                    if key in payload
                }
            )
            record["market_id"] = market_id
            record["timestamp"] = source_timestamp or observed_at
            record["source_timestamp"] = source_timestamp or observed_at
            record["observed_at"] = observed_at
            record["source_type"] = "FORWARD_COLLECTED"
            record["research_mode"] = "RECORDED_BOOK_REPLAY"
            record["replay_cutoff"] = cutoff_value
            source_snapshot_id = str(row.get("snapshot_id") or "").strip()
            if not source_snapshot_id:
                continue
            source_record_hash = _stable_hash(
                {
                    "snapshot_id": source_snapshot_id,
                    "market_id": market_id,
                    "source_timestamp": source_timestamp or observed_at,
                    "observed_at": observed_at,
                    "payload": payload,
                }
            )
            record["source_snapshot_id"] = source_snapshot_id
            record["source_record_hash"] = source_record_hash
            record["provenance"] = {
                "source_type": "FORWARD_COLLECTED",
                "source_snapshot_id": source_snapshot_id,
                "source_record_hash": source_record_hash,
                "captured_at": observed_at,
            }
            rows.append(record)
        rows.sort(
            key=lambda item: (
                _stamp(item.get("source_timestamp")) or cutoff_value,
                str(item.get("market_id") or ""),
                str(item.get("source_snapshot_id") or ""),
            )
        )
        manifest = [
            {
                "snapshot_id": str(item["source_snapshot_id"]),
                "source_record_hash": str(item["source_record_hash"]),
                "market_id": str(item["market_id"]),
                "source_timestamp": _stamp(item["source_timestamp"]),
            }
            for item in rows
        ]
        version = _stable_hash(
            {
                "dataset_id": dataset_identifier,
                "research_mode": "RECORDED_BOOK_REPLAY",
                "cutoff": cutoff_value,
                "manifest": manifest,
                "rows": rows,
            }
        )
        metadata = {
            "source_type": "FORWARD_COLLECTED",
            "provider": "polymarket",
            "instrument": "POLYMARKET",
            "research_mode": "RECORDED_BOOK_REPLAY",
            "exact_cutoff": cutoff_value,
            "snapshot_manifest": manifest,
            "provenance_version": "forward-snapshot-provenance-v1",
            "policy_version": "recorded-book-replay-v1",
        }
        if self.store.load_dataset(dataset_identifier, version) is None:
            self.store.save_dataset(
                dataset_identifier,
                version,
                rows,
                metadata=metadata,
                quality="ORDER_BOOK_SIMULATED",
            )
        starts = [_stamp(item.get("source_timestamp")) for item in rows]
        starts = [item for item in starts if item is not None]
        self.store.save_dataset_catalog(
            dataset_identifier,
            version,
            provider="polymarket",
            instrument="POLYMARKET",
            market_type=MarketType.PREDICTION,
            timeframe="event_snapshots",
            start_timestamp=min(starts) if starts else None,
            end_timestamp=max(starts) if starts else None,
            row_count=len(rows),
            completeness=1.0 if rows else 0.0,
            missing_ranges=(),
            quality="ORDER_BOOK_SIMULATED",
            source_type="FORWARD_COLLECTED",
            snapshot_id=f"{dataset_identifier}:{version}",
            metadata=metadata,
        )
        return {
            "dataset_id": dataset_identifier,
            "dataset_version": version,
            "source_type": "FORWARD_COLLECTED",
            "research_mode": "RECORDED_BOOK_REPLAY",
            "cutoff": cutoff_value,
            "row_count": len(rows),
            "snapshot_manifest": manifest,
            "provenance_preserved": True,
        }

    publish_forward_replay = publish_polymarket_forward_replay
    create_forward_replay_dataset = publish_polymarket_forward_replay


    def bootstrap(self, *, crypto: bool = False, polymarket: bool = False, all_sources: bool = False, resume: bool = False, full_15m: bool = False, max_markets: int = 1000) -> dict[str, Any]:
        selected_crypto = bool(crypto or all_sources or not (crypto or polymarket or all_sources))
        selected_polymarket = bool(polymarket or all_sources or not (crypto or polymarket or all_sources))
        result: dict[str, Any] = {}
        if selected_crypto:
            result["crypto"] = [item.as_record() for item in self.bootstrap_crypto(resume=resume, full_15m=full_15m)]
        if selected_polymarket:
            result["polymarket"] = self.bootstrap_polymarket(max_markets=max_markets, resume=resume).as_record()
        return result


def _normalize_prediction_history(market: PredictionMarketSnapshot, values: Any) -> list[dict[str, Any]]:
    if not isinstance(values, Sequence) or isinstance(values, (str, bytes, Mapping)):
        return []
    token_default = market.yes_token_id or market.market_id
    expected_yes = str(token_default).strip()
    by_key: dict[tuple[datetime, str], dict[str, Any]] = {}
    for value in values:
        if not isinstance(value, Mapping):
            continue
        stamp = _stamp(value.get("timestamp", value.get("t", value.get("time"))))
        raw_price = value.get("price", value.get("p", value.get("yes_mid", value.get("value"))))
        try:
            price = float(raw_price)
        except (TypeError, ValueError):
            continue
        if stamp is None or not math.isfinite(price) or not 0.0 <= price <= 1.0:
            continue
        token = str(value.get("token_id", value.get("asset_id", token_default)) or token_default).strip()
        # ``price_history`` is requested for YES.  A NO or unrelated asset
        # must never be silently relabeled as the YES price path.
        if expected_yes and token != expected_yes:
            continue
        item = {"timestamp": stamp, "price": price, "token_id": token}
        if value.get("order_book") is not None or value.get("book") is not None:
            item["order_book"] = value.get("order_book", value.get("book"))
        key = (stamp, token)
        previous = by_key.get(key)
        if previous is None or _stable_hash(value) > _stable_hash(previous):
            by_key[key] = item
    return [by_key[key] for key in sorted(by_key)]


def classify_market_category(market: PredictionMarketSnapshot | Mapping[str, Any] | Any) -> str:
    """Classify a market using stable explicit fields and keyword rules only."""
    if isinstance(market, Mapping):
        category = market.get("category")
        question = str(market.get("question", ""))
        tags = market.get("tags", ())
    else:
        category = getattr(market, "category", None)
        question = str(getattr(market, "question", ""))
        tags = getattr(market, "tags", ())
    explicit = str(category or "").strip().lower()
    aliases = {
        "crypto": "crypto",
        "cryptocurrency": "crypto",
        "politics": "politics",
        "political": "politics",
        "economics": "economics",
        "economic": "economics",
        "sports": "sports",
        "sport": "sports",
        "weather": "weather",
        "technology": "technology",
        "tech": "technology",
        "entertainment": "entertainment",
    }
    if explicit in aliases:
        return aliases[explicit]
    text = " ".join([question, *(str(item) for item in tags)]).lower()
    keywords: tuple[tuple[str, tuple[str, ...]], ...] = (
        ("crypto", ("bitcoin", "btc", "ethereum", "crypto", "solana", "token", "defi")),
        ("politics", ("president", "election", "senate", "congress", "prime minister", "vote", "democrat", "republican")),
        ("economics", ("fed", "interest rate", "inflation", "gdp", "unemployment", "recession", "cpi", "treasury")),
        ("sports", ("nfl", "nba", "mlb", "nhl", "soccer", "football", "basketball", "world cup", "match", "game")),
        ("weather", ("temperature", "rain", "hurricane", "storm", "weather", "snow")),
        ("technology", ("ai", "artificial intelligence", "software", "apple", "google", "openai", "technology")),
        ("entertainment", ("oscar", "movie", "music", "grammy", "celebrity", "television")),
    )
    for label, words in keywords:
        if any(word in text for word in words):
            return label
    return "other"


def label_btc_regimes(
    bars: Sequence[OHLCVBar | Mapping[str, Any]],
    *,
    engine: RegimeEngine | None = None,
    lookback: int = 30,
) -> list[dict[str, Any]]:
    """Store overlapping, reproducible normalized BTC regime labels per bar."""
    if isinstance(lookback, bool) or int(lookback) < 2:
        raise ValueError("lookback must be at least two bars")
    normalized_bars = [item for raw in bars if (item := _bar(raw)) is not None]
    ordered = sorted(normalized_bars, key=lambda item: item.timestamp)
    detector = engine or RegimeEngine()
    labels: list[dict[str, Any]] = []
    mapping = {
        RegimeState.BULL: "BULL",
        RegimeState.BULLISH: "BULL",
        RegimeState.STRONG_BULL: "BULL",
        RegimeState.BEAR: "BEAR",
        RegimeState.BEARISH: "BEAR",
        RegimeState.STRONG_BEAR: "BEAR",
        RegimeState.SIDEWAYS: "SIDEWAYS",
        RegimeState.RANGE_BOUND: "SIDEWAYS",
        RegimeState.HIGH_VOLATILITY: "HIGH_VOLATILITY",
        RegimeState.EXTREME_VOLATILITY: "EXTREME_VOLATILITY",
        RegimeState.CRASH: "CRASH",
        RegimeState.LOW_VOLATILITY: "LOW_VOLATILITY",
        RegimeState.NORMAL_VOLATILITY: "LOW_VOLATILITY",
    }
    for index, item in enumerate(ordered):
        window = ordered[max(0, index - int(lookback) + 1) : index + 1]
        snapshot = detector.detect_crypto(window)
        normalized: list[str] = []
        confidence: dict[str, float] = {}
        for regime in snapshot.regimes:
            label = mapping.get(regime.state)
            if label is None:
                continue
            if label not in normalized:
                normalized.append(label)
            confidence[label] = max(confidence.get(label, 0.0), float(regime.confidence))
        if not normalized:
            normalized.append("SIDEWAYS")
            confidence["SIDEWAYS"] = 1.0
        labels.append({"timestamp": item.timestamp, "labels": normalized, "confidence": confidence})
    return labels


def _mean_metric(rows: Sequence[Mapping[str, Any]], name: str) -> float:
    values = []
    for row in rows:
        try:
            value = float(row.get(name, 0.0))
        except (TypeError, ValueError):
            continue
        if math.isfinite(value):
            values.append(value)
    return mean(values) if values else 0.0


def _parameter_stability(rows: Sequence[Mapping[str, Any]], parameters: Mapping[str, Any]) -> dict[str, Any]:
    returns = []
    for row in rows:
        try:
            value = float(row.get("metrics", {}).get("total_return", 0.0))
        except (TypeError, ValueError):
            continue
        if math.isfinite(value):
            returns.append(value)
    average = mean(returns) if returns else 0.0
    dispersion = pstdev(returns) if len(returns) > 1 else 0.0
    score = max(0.0, min(1.0, 1.0 - dispersion / max(abs(average), 0.05))) if returns else 0.0
    return {
        "parameters": dict(parameters),
        "same_parameters_across_windows": True,
        "windows": len(rows),
        "positive_window_fraction": sum(1 for value in returns if value > 0) / len(returns) if returns else 0.0,
        "return_dispersion": dispersion,
        "score": score,
    }


def run_btc_historical_research(
    store: AxiomStore,
    *,
    dataset_id: str | None = None,
    version: str | None = None,
    symbol: str = BTC_SYMBOL,
    initial_cash: float = 10_000.0,
    train_years: int = 3,
    validation_years: int = 1,
    holdout_years: int = 1,
    step_years: int = 1,
) -> dict[str, Any]:
    """Run deterministic BTC walk-forward research on a historical catalog only."""
    selected_id = dataset_id or BTC_DATASET_IDS["1d"]
    catalog = store.load_dataset_catalog(selected_id, version)
    report: dict[str, Any] = {
        "kind": "btc_historical_walk_forward",
        "source_type": "HISTORICAL",
        "dataset_id": selected_id,
        "dataset_version": catalog.get("dataset_version") if catalog else None,
        "instrument": symbol,
        "generated_at": utc_now(),
        "experiments": [],
        "regimes": {},
        "limitations": [],
    }
    if catalog is None:
        report["limitations"] = ["historical BTC dataset catalog is unavailable"]
        return report
    if str(catalog.get("source_type", "")).upper() != "HISTORICAL":
        report["limitations"] = ["forward-collected data cannot be used as historical BTC research input"]
        return report
    resolved_version = str(catalog["dataset_version"])
    bars = store.load_bars(str(catalog.get("instrument", symbol)), dataset_id=selected_id, dataset_version=resolved_version)
    bars = sorted(bars, key=lambda item: item.timestamp)
    report["coverage"] = {
        "start": bars[0].timestamp if bars else None,
        "end": bars[-1].timestamp if bars else None,
        "rows": len(bars),
        "completeness": catalog.get("completeness", 0.0),
        "missing_ranges": catalog.get("missing_ranges", []),
    }
    if not bars:
        report["limitations"] = ["historical BTC catalog has no OHLCV rows"]
        return report
    labels = label_btc_regimes(bars)
    existing_labels = store.load_historical_regime_labels(selected_id, resolved_version, limit=1)
    if not existing_labels:
        store.save_historical_regime_labels(selected_id, resolved_version, labels)
    label_counts: dict[str, int] = {}
    for item in labels:
        for label in item["labels"]:
            label_counts[label] = label_counts.get(label, 0) + 1
    report["regimes"] = {
        "engine": "RegimeEngine",
        "lookback": 30,
        "rows": len(labels),
        "label_counts": label_counts,
        "overlapping": True,
        "stored": True,
    }
    try:
        train_window = timedelta(days=365 * int(train_years))
        validation_window = timedelta(days=365 * int(validation_years))
        holdout_window = timedelta(days=365 * int(holdout_years))
        step = timedelta(days=365 * int(step_years))
        splits = walk_forward_splits(
            bars,
            train_window,
            validation_window,
            holdout_window,
            step,
            dataset_version=resolved_version,
        )
    except (TypeError, ValueError) as exc:
        report["limitations"] = [f"walk-forward split configuration invalid: {exc}"]
        return report
    report["walk_forward"] = {
        "train_years": int(train_years),
        "validation_years": int(validation_years),
        "holdout_years": int(holdout_years),
        "step_years": int(step_years),
        "windows": len(splits),
        "holdout_locked": True,
    }
    families: tuple[tuple[str, dict[str, Any]], ...] = (
        ("dip", {"lookback": 14, "threshold": 0.03}),
        ("momentum", {"lookback": 14, "threshold": 0.03}),
        ("trend", {"fast": 10, "slow": 30, "threshold": 0.02}),
        ("mean_reversion", {"lookback": 20, "sigma": 2.0}),
        ("breakout", {"lookback": 20, "threshold": 0.02}),
        ("volatility", {"lookback": 20, "target": 0.02}),
        ("rsi", {"period": 14, "oversold": 30.0, "overbought": 70.0}),
        ("volume_filter", {"lookback": 20, "multiplier": 1.2}),
    )
    for family, parameters in families:
        definition = validate_strategy(
            {
                "version": 1,
                "market_type": MarketType.CRYPTO_SPOT.value,
                "family": family,
                "parameters": parameters,
                "strategy_id": f"btc-usdt-{family}",
            }
        )
        windows: list[dict[str, Any]] = []
        for index, split in enumerate(splits):
            result = CryptoBacktester(
                initial_cash=initial_cash,
                fee_bps=10.0,
                slippage_bps=5.0,
                allocation=0.50,
                symbol=symbol,
            ).run(split.holdout, definition, symbol=symbol, warmup=(*split.train, *split.validation))
            windows.append(
                {
                    "window": index + 1,
                    "train_start": split.train_start,
                    "train_end": split.train_end,
                    "validation_end": split.validation_end,
                    "holdout_start": split.validation_end,
                    "holdout_end": split.holdout_end,
                    "train_rows": len(split.train),
                    "validation_rows": len(split.validation),
                    "holdout_rows": len(split.holdout),
                    "metrics": dict(result.metrics),
                    "quality": result.quality.value,
                    "fills": len(result.fills),
                }
            )
        aggregate = {
            "windows": len(windows),
            "mean_total_return": _mean_metric(windows, "total_return"),
            "median_total_return": median([float(item["metrics"].get("total_return", 0.0)) for item in windows]) if windows else 0.0,
            "mean_max_drawdown": _mean_metric(windows, "max_drawdown"),
            "mean_expectancy": _mean_metric(windows, "expectancy"),
            "mean_sharpe": _mean_metric(windows, "sharpe"),
            "mean_sortino": _mean_metric(windows, "sortino"),
            "mean_turnover": _mean_metric(windows, "turnover"),
            "mean_fees": _mean_metric(windows, "fees"),
            "mean_slippage": _mean_metric(windows, "slippage"),
            "positive_window_fraction": sum(1 for item in windows if float(item["metrics"].get("total_return", 0.0)) > 0) / len(windows) if windows else 0.0,
        }
        report["experiments"].append(
            {
                "strategy_id": definition.id,
                "family": family,
                "parameters": parameters,
                "walk_forward": windows,
                "aggregate": aggregate,
                "parameter_stability": _parameter_stability(windows, parameters),
                "selection_basis": "validation_only; locked holdout report-only",
            }
        )
    label_lookup = {item["timestamp"].isoformat(): item["labels"] for item in labels}
    previous_by_timestamp = {
        bar.timestamp: bars[index - 1]
        for index, bar in enumerate(bars)
        if index > 0
    }
    regime_values: dict[str, list[float]] = {}
    for experiment in report["experiments"]:
        for window in experiment["walk_forward"]:
            # Regime performance is based on locked holdout bar returns, not
            # train or validation observations.
            split_start = _stamp(window["holdout_start"])
            split_end = _stamp(window["holdout_end"])
            if split_start is None:
                continue
            for bar in bars:
                if bar.timestamp < split_start or (split_end is not None and bar.timestamp >= split_end):
                    continue
                previous = previous_by_timestamp.get(bar.timestamp)
                if previous is None or previous.close <= 0:
                    continue
                value = bar.close / previous.close - 1.0
                for label in label_lookup.get(bar.timestamp.isoformat(), ()):
                    regime_values.setdefault(label, []).append(value)
    report["regime_performance"] = {
        label: {
            "observations": len(values),
            "mean_return": mean(values) if values else 0.0,
            "volatility": pstdev(values) if len(values) > 1 else 0.0,
            "positive_fraction": sum(1 for value in values if value > 0) / len(values) if values else 0.0,
        }
        for label, values in sorted(regime_values.items())
    }
    report["limitations"] = [
        "OHLCV next-bar execution is a simulation, not executable historical depth.",
        "Strategy parameters are fixed deterministic baselines; no ML or autonomous optimization is used.",
        "Locked holdout windows are reported after the validation partition and are not selection inputs.",
    ]
    report_id = f"btc-historical-walk-forward:{selected_id}:{resolved_version}"
    store.save_report_if_absent(report_id, report, experiment_id=selected_id)
    return report


__all__ = [
    "BTC_DATASET_IDS",
    "BTC_HISTORY_START",
    "BTC_INTERVAL_SECONDS",
    "BootstrapReport",
    "HistoricalBootstrapper",
    "POLYMARKET_DATASET_ID",
    "POLYMARKET_HISTORICAL_JOB_NAME",
    "classify_market_category",
    "label_btc_regimes",
    "run_btc_historical_research",
]
