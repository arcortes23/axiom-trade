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

    def as_record(self) -> dict[str, Any]:
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
            "metadata": _jsonable(dict(self.metadata or {})),
        }


@dataclass(frozen=True, slots=True)
class _CallResult:
    value: Any
    errors: tuple[str, ...]
    retries: int


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
        retry_after = max((float(getattr(item, "retry_after", 0.0) or 0.0) for item in transport_errors), default=0.0)
        retryable = any(bool(getattr(item, "retryable", False)) for item in transport_errors)
        if direct_error is not None:
            errors.append(_error_text(context, direct_error))
            retryable = _retryable_exception(direct_error)
        if not retryable or attempt + 1 >= attempts:
            return _CallResult(value, tuple(dict.fromkeys(errors)), retries)
        retries += 1
        delay = retry_after if retry_after > 0 else min(60.0, max(0.0, float(backoff)) * (2**attempt))
        if delay > 0:
            sleep(delay)
    return _CallResult(None, tuple(dict.fromkeys(errors)), retries)


def _provider_name(provider: Any) -> str:
    return str(getattr(provider, "provider_name", provider.__class__.__name__)).strip() or provider.__class__.__name__


def _same_time(left: Any, right: Any) -> bool:
    a, b = _stamp(left), _stamp(right)
    return a is not None and b is not None and a == b


def _catalog_report(catalog: Mapping[str, Any], *, status: str = "COMPLETE") -> BootstrapReport:
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
        metadata=dict(catalog.get("metadata", {})) if isinstance(catalog.get("metadata"), Mapping) else {},
    )


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
            with self.store.transaction():
                if self.store.load_dataset_catalog(dataset_id, version) is None:
                    self.store.save_bars(BTC_SYMBOL, combined, dataset_id=dataset_id, dataset_version=version)
                    self.store.save_dataset_catalog(
                        dataset_id,
                        version,
                        provider=_provider_name(provider),
                        instrument=BTC_SYMBOL,
                        market_type=MarketType.CRYPTO_SPOT,
                        timeframe=interval,
                        start_timestamp=combined[0].timestamp,
                        end_timestamp=combined[-1].timestamp,
                        row_count=len(combined),
                        completeness=completeness,
                        missing_ranges=missing,
                        quality="OHLCV",
                        source_type="HISTORICAL",
                        snapshot_id=f"{dataset_id}:{version}",
                        metadata=metadata,
                    )
                self.store.clear_dataset_staging_bars(dataset_id)
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

    def bootstrap_polymarket(self, *, max_markets: int = 1000, resume: bool = False) -> BootstrapReport:
        if isinstance(max_markets, bool) or int(max_markets) < 0:
            raise ValueError("max_markets must be non-negative")
        provider = self.prediction_provider
        state = self.store.load_dataset_bootstrap_state(POLYMARKET_DATASET_ID)
        state_status = str(state.get("status", "")) if state else ""
        if state and state_status not in {"COMPLETE", "EMPTY"} and not resume:
            return BootstrapReport(POLYMARKET_DATASET_ID, "HISTORICAL", _provider_name(provider), "POLYMARKET", "event", "BLOCKED", None, 0, None, None, 0.0, errors=(f"{POLYMARKET_DATASET_ID} has an incomplete bootstrap; rerun with --resume",))
        discovered_call = _call_with_retries(
            provider,
            lambda: provider.markets(active=False, limit=int(max_markets)),
            context="polymarket market discovery",
            max_attempts=self.max_attempts,
            backoff=self.backoff,
            sleep=self.sleep,
        )
        if discovered_call.value is None and any("limit" in item.lower() for item in discovered_call.errors):
            discovered_call = _call_with_retries(
                provider,
                lambda: provider.markets(active=False),
                context="polymarket market discovery",
                max_attempts=self.max_attempts,
                backoff=self.backoff,
                sleep=self.sleep,
            )
        errors = list(discovered_call.errors)
        retries = discovered_call.retries
        if discovered_call.value is None or (discovered_call.errors and not discovered_call.value):
            existing_version = (state or {}).get("base_version")
            status = "PARTIAL" if errors else "FAILED"
            self.store.save_dataset_bootstrap_state(
                POLYMARKET_DATASET_ID,
                {
                    "provider": _provider_name(provider),
                    "instrument": "POLYMARKET",
                    "market_type": MarketType.PREDICTION.value,
                    "timeframe": "event",
                    "requested_start": None,
                    "requested_end": None,
                    "next_timestamp": None,
                    "base_version": existing_version,
                    "status": status,
                    "processed_market_ids": list((state or {}).get("processed_market_ids", ())),
                    "errors": list(dict.fromkeys(errors[-64:])),
                },
            )
            return BootstrapReport(
                POLYMARKET_DATASET_ID,
                "HISTORICAL",
                _provider_name(provider),
                "POLYMARKET",
                "event",
                status,
                str(existing_version) if existing_version else None,
                0,
                None,
                None,
                0.0,
                errors=tuple(dict.fromkeys(errors)),
            )
        raw_markets = discovered_call.value if isinstance(discovered_call.value, Sequence) and not isinstance(discovered_call.value, (str, bytes, Mapping)) else ()
        markets = [item for item in raw_markets if isinstance(item, PredictionMarketSnapshot)]
        markets = markets[: int(max_markets)] if max_markets else []
        processed = {
            str(item)
            for item in (state or {}).get("processed_market_ids", ())
            if str(item).strip()
        }
        self.store.save_dataset_bootstrap_state(
            POLYMARKET_DATASET_ID,
            {
                "provider": _provider_name(provider),
                "instrument": "POLYMARKET",
                "market_type": MarketType.PREDICTION.value,
                "timeframe": "event",
                "requested_start": None,
                "requested_end": None,
                "next_timestamp": None,
                "base_version": (state or {}).get("base_version"),
                "status": "RUNNING",
                "processed_market_ids": sorted(processed),
                "markets_discovered": len(markets),
                "errors": errors[-32:],
            },
        )
        imported = 0
        points_total = 0
        category_counts: dict[str, int] = {}
        market_versions: list[dict[str, Any]] = []
        starts: list[datetime] = []
        ends: list[datetime] = []
        timestamped_order_books = 0
        for market_id in sorted(processed):
            catalog = self.store.load_dataset_catalog(f"prediction:{market_id}")
            if catalog is None:
                continue
            metadata = catalog.get("metadata", {})
            metadata = metadata if isinstance(metadata, Mapping) else {}
            has_order_book = bool(metadata.get("historical_order_book_available", False))
            if has_order_book:
                timestamped_order_books += 1
            category = str(metadata.get("category") or "other")
            category_counts[category] = category_counts.get(category, 0) + 1
            rows = int(catalog.get("row_count", 0))
            if rows:
                imported += 1
                points_total += rows
            version = str(catalog.get("dataset_version", ""))
            market_versions.append(
                {
                    "market_id": market_id,
                    "version": version,
                    "records": rows,
                    "category": category,
                    "historical_order_book": has_order_book,
                }
            )
            start = _stamp(catalog.get("start_timestamp"))
            end = _stamp(catalog.get("end_timestamp"))
            if start is not None:
                starts.append(start)
            if end is not None:
                ends.append(end)
        for discovered in markets:
            market_id = str(discovered.market_id)
            if market_id in processed:
                continue
            market_call = _call_with_retries(
                provider,
                lambda market_id=market_id: provider.market(market_id),
                context=f"polymarket market {market_id}",
                max_attempts=self.max_attempts,
                backoff=self.backoff,
                sleep=self.sleep,
            )
            retries += market_call.retries
            errors.extend(market_call.errors)
            market = market_call.value if isinstance(market_call.value, PredictionMarketSnapshot) else discovered
            metadata_call = _call_with_retries(
                provider,
                lambda market_id=market_id: provider.metadata(market_id),
                context=f"polymarket metadata {market_id}",
                max_attempts=self.max_attempts,
                backoff=self.backoff,
                sleep=self.sleep,
            )
            retries += metadata_call.retries
            errors.extend(metadata_call.errors)
            instrument = metadata_call.value
            history_call = _call_with_retries(
                provider,
                lambda market_id=market_id: provider.price_history(market_id),
                context=f"polymarket price history {market_id}",
                max_attempts=self.max_attempts,
                backoff=self.backoff,
                sleep=self.sleep,
            )
            retries += history_call.retries
            errors.extend(history_call.errors)
            history = _normalize_prediction_history(market, history_call.value)
            category = classify_market_category(market)
            category_counts[category] = category_counts.get(category, 0) + 1
            extra = getattr(instrument, "extra", {}) if instrument is not None else {}
            history_has_order_book = any(item.get("order_book") is not None for item in history)
            if history_has_order_book:
                timestamped_order_books += 1
            history_quality = (
                "HISTORICAL_ORDER_BOOK"
                if history_has_order_book
                else ResearchQuality.PRICE_PROXY.value
            )
            token_ids = {
                "yes": market.yes_token_id or (extra.get("yes_token_id") if isinstance(extra, Mapping) else None),
                "no": market.no_token_id or (extra.get("no_token_id") if isinstance(extra, Mapping) else None),
            }
            raw_market = to_record(market)
            metadata_payload = {
                "source_type": "HISTORICAL",
                "provider": _provider_name(provider),
                "market_id": market_id,
                "question": market.question,
                "resolution_criteria": market.resolution_criteria,
                "settlement": market.settlement.value,
                "volume": market.volume,
                "liquidity": market.liquidity,
                "expiry": market.expiry,
                "category": category,
                "tags": list(market.tags),
                "token_ids": token_ids,
                "instrument_metadata": to_record(instrument) if instrument is not None else None,
                "raw_market": raw_market,
                "historical_order_book_available": history_has_order_book,
                "research_quality": history_quality,
            }
            metadata_hash = _stable_hash(metadata_payload)
            try:
                self.store.save_polymarket_market_metadata(
                    market_id,
                    metadata_payload,
                    observed_at=self.clock(),
                    metadata_hash=metadata_hash,
                )
            except ValueError as exc:
                errors.append(f"{market_id}: metadata persistence: {exc}")
            for point in history:
                snapshot_id = f"pmhist:{market_id}:{point['token_id']}:{point['timestamp'].isoformat()}"
                payload = {
                    "source_type": "HISTORICAL",
                    "provider": _provider_name(provider),
                    "market_id": market_id,
                    "question": market.question,
                    "timestamp": point["timestamp"],
                    "price": point["price"],
                    "yes_mid": point["price"],
                    "token_id": point["token_id"],
                    "category": category,
                    "tags": list(market.tags),
                    "settlement": market.settlement.value,
                    "resolution_criteria": market.resolution_criteria,
                    "volume": market.volume,
                    "liquidity": market.liquidity,
                    "expiry": market.expiry,
                    "order_book": point.get("order_book"),
                    "research_quality": history_quality,
                    "historical_order_book": point.get("order_book") is not None,
                    "executable_quote": False,
                }
                try:
                    self.store.save_polymarket_snapshot(
                        snapshot_id,
                        market_id,
                        point["timestamp"],
                        point["timestamp"],
                        payload,
                        quality=history_quality,
                    )
                except ValueError as exc:
                    errors.append(f"{market_id}: snapshot persistence: {exc}")
            version = _stable_hash(history)
            starts.extend(item["timestamp"] for item in history)
            ends.extend(item["timestamp"] for item in history)
            if history:
                points_total += len(history)
                imported += 1
            market_versions.append({"market_id": market_id, "version": version, "records": len(history), "category": category, "historical_order_book": history_has_order_book})
            try:
                self.store.save_dataset_catalog(
                    f"prediction:{market_id}",
                    version,
                    provider=_provider_name(provider),
                    instrument=str(getattr(instrument, "symbol", market_id) or market_id),
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
                        "market_id": market_id,
                        "polymarket_key": market_id,
                        "category": category,
                        "question": market.question,
                        "resolution_criteria": market.resolution_criteria,
                        "volume": market.volume,
                        "liquidity": market.liquidity,
                        "token_ids": token_ids,
                        "settlement": market.settlement.value,
                        "historical_order_book_available": history_has_order_book,
                        "research_quality": history_quality,
                    },
                )
            except ValueError as exc:
                errors.append(f"{market_id}: catalog persistence: {exc}")
            processed.add(market_id)
            self.store.save_dataset_bootstrap_state(
                POLYMARKET_DATASET_ID,
                {
                    "provider": _provider_name(provider),
                    "instrument": "POLYMARKET",
                    "market_type": MarketType.PREDICTION.value,
                    "timeframe": "event",
                    "requested_start": None,
                    "requested_end": None,
                    "next_timestamp": None,
                    "base_version": (state or {}).get("base_version"),
                    "status": "RUNNING",
                    "processed_market_ids": sorted(processed),
                    "markets_discovered": len(markets),
                    "markets_imported": imported,
                    "price_points": points_total,
                    "categories": category_counts,
                    "errors": list(dict.fromkeys(errors[-32:])),
                },
            )
        aggregate_has_order_book = bool(market_versions) and timestamped_order_books == len(market_versions)
        aggregate_quality = (
            "HISTORICAL_ORDER_BOOK"
            if aggregate_has_order_book
            else ResearchQuality.PRICE_PROXY.value
        )
        aggregate_version = _stable_hash(market_versions)
        aggregate_start = min(starts) if starts else None
        aggregate_end = max(ends) if ends else None
        aggregate_metadata = {
            "source_type": "HISTORICAL",
            "provider": _provider_name(provider),
            "instrument": "POLYMARKET",
            "markets_discovered": len(markets),
            "markets_imported": imported,
            "price_points": points_total,
            "category_counts": category_counts,
            "market_versions": market_versions,
            "research_quality": aggregate_quality,
            "historical_order_book_available": aggregate_has_order_book,
            "note": "Only timestamped price history is stored; no historical depth, spread, fills, or executable quotes are fabricated.",
        }
        completeness = imported / len(markets) if markets else 0.0
        try:
            self.store.save_dataset_catalog(
                POLYMARKET_DATASET_ID,
                aggregate_version,
                provider=_provider_name(provider),
                instrument="POLYMARKET",
                market_type=MarketType.PREDICTION,
                timeframe="event",
                start_timestamp=aggregate_start,
                end_timestamp=aggregate_end,
                row_count=points_total,
                completeness=completeness,
                missing_ranges=(),
                quality=aggregate_quality,
                source_type="HISTORICAL",
                snapshot_id=f"{POLYMARKET_DATASET_ID}:{aggregate_version}",
                metadata=aggregate_metadata,
            )
        except ValueError as exc:
            errors.append(f"{POLYMARKET_DATASET_ID}: catalog persistence: {exc}")
        status = "COMPLETE" if markets and imported == len(markets) and not errors else ("EMPTY" if not markets and not errors else "PARTIAL")
        self.store.save_dataset_bootstrap_state(
            POLYMARKET_DATASET_ID,
            {
                "provider": _provider_name(provider),
                "instrument": "POLYMARKET",
                "market_type": MarketType.PREDICTION.value,
                "timeframe": "event",
                "requested_start": None,
                "requested_end": None,
                "next_timestamp": None,
                "base_version": aggregate_version,
                "status": status,
                "processed_market_ids": sorted(processed),
                "markets_discovered": len(markets),
                "markets_imported": imported,
                "price_points": points_total,
                "categories": category_counts,
                "errors": list(dict.fromkeys(errors[-64:])),
            },
        )
        report = BootstrapReport(
            POLYMARKET_DATASET_ID,
            "HISTORICAL",
            _provider_name(provider),
            "POLYMARKET",
            "event",
            status,
            aggregate_version,
            points_total,
            aggregate_start,
            aggregate_end,
            completeness,
            (),
            0,
            retries,
            tuple(dict.fromkeys(errors)),
            aggregate_metadata,
        )
        self.store.save_report_if_absent(f"historical-bootstrap:{POLYMARKET_DATASET_ID}:{aggregate_version}", report.as_record(), experiment_id=POLYMARKET_DATASET_ID)
        return report

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
        token = str(value.get("token_id", value.get("asset_id", token_default)) or token_default)
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
    "classify_market_category",
    "label_btc_regimes",
    "run_btc_historical_research",
]
