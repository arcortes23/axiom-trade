"""Safe, repeatable dashboard latency profile over a temporary SQLite fixture.

This module intentionally has no production-database defaults.  Every run builds
an immutable fixture in :class:`tempfile.TemporaryDirectory`, profiles only the
loopback dashboard, and prints one JSON document to stdout.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from functools import wraps
import hashlib
import json
from pathlib import Path
import shutil
import sys
import tempfile
import time
from typing import Any, Callable, Iterator, Mapping
from urllib.request import Request, build_opener, ProxyHandler

# Running a profiling utility must not leave bytecode beside the source tree.
sys.dont_write_bytecode = True
_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import axiom.canary as canary_module
import axiom.data_quality as quality_module
from axiom.canary import CanaryService, CredentialStore
from axiom.dashboard import DashboardData, DashboardServer
from axiom.ranker import CandidateCanaryRanker
from axiom.storage import AxiomStore


DEFAULT_CANDIDATES = 28
DEFAULT_HISTORY_ROWS = 100_000
DEFAULT_WARM_CALLS = 5
ENDPOINTS = (
    "/api/v2/canary",
    "/api/v2/overview-summary",
    "/api/operator",
)
CONTRIBUTORS = (
    "canary_service_status",
    "validated_eligibility",
    "current_selection",
    "evaluate_prediction_data_quality",
    "load_dataset_catalog",
    "load_dataset",
    "aggregate_reconstruction",
    "lifecycle_loads",
    "ranking_validation",
)
T0 = datetime(2024, 1, 1, tzinfo=timezone.utc)


@dataclass
class _Contributor:
    calls: int = 0
    inclusive_ns: int = 0


@dataclass
class _Stats:
    values: dict[str, _Contributor] = field(
        default_factory=lambda: {name: _Contributor() for name in CONTRIBUTORS}
    )

    def record(self, name: str, started_ns: int) -> None:
        value = self.values[name]
        value.calls += 1
        value.inclusive_ns += max(0, time.perf_counter_ns() - started_ns)

    def report(self) -> dict[str, dict[str, int | float]]:
        return {
            name: {
                "calls": self.values[name].calls,
                "inclusive_elapsed_ns": self.values[name].inclusive_ns,
                "inclusive_elapsed_ms": round(self.values[name].inclusive_ns / 1_000_000, 6),
            }
            for name in CONTRIBUTORS
        }


_ACTIVE_STATS: _Stats | None = None


def _active_stats(store: Any = None) -> _Stats | None:
    value = getattr(store, "_dashboard_profile_stats", None) if store is not None else None
    return value if isinstance(value, _Stats) else _ACTIVE_STATS


@contextmanager
def _request_stats(stats: _Stats | None) -> Iterator[None]:
    global _ACTIVE_STATS
    previous = _ACTIVE_STATS
    _ACTIVE_STATS = stats
    try:
        yield
    finally:
        _ACTIVE_STATS = previous


def _timed_store(name: str, function: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(function)
    def wrapped(store: Any, *args: Any, **kwargs: Any) -> Any:
        stats = _active_stats(store)
        if stats is None:
            return function(store, *args, **kwargs)
        started = time.perf_counter_ns()
        try:
            return function(store, *args, **kwargs)
        finally:
            stats.record(name, started)

    return wrapped


def _timed_service(name: str, function: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(function)
    def wrapped(service: Any, *args: Any, **kwargs: Any) -> Any:
        stats = _active_stats(getattr(service, "store", None))
        if stats is None:
            return function(service, *args, **kwargs)
        started = time.perf_counter_ns()
        try:
            return function(service, *args, **kwargs)
        finally:
            stats.record(name, started)

    return wrapped


def _timed_function(name: str, function: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(function)
    def wrapped(*args: Any, **kwargs: Any) -> Any:
        stats = _active_stats()
        if stats is None:
            return function(*args, **kwargs)
        started = time.perf_counter_ns()
        try:
            return function(*args, **kwargs)
        finally:
            stats.record(name, started)

    return wrapped


@contextmanager
def _instrument_contributors() -> Iterator[None]:
    """Patch only measurement references, restoring every symbol on exit.

    ``ranking_validation`` is the immutable ranking-snapshot hash helper used by
    both persisted-selection validation and the rankable-row validation loop.
    ``current_selection`` is the persisted singleton-selection read.  The two
    are deliberately separate so the profile cannot hide selection validation
    inside one aggregate number.
    """
    patches: list[tuple[Any, str, Any]] = []

    def patch(target: Any, attribute: str, replacement: Any) -> None:
        original = getattr(target, attribute)
        patches.append((target, attribute, original))
        setattr(target, attribute, replacement)

    patch(AxiomStore, "load_dataset_catalog", _timed_store("load_dataset_catalog", AxiomStore.load_dataset_catalog))
    patch(AxiomStore, "load_dataset", _timed_store("load_dataset", AxiomStore.load_dataset))
    patch(
        AxiomStore,
        "_aggregate_polymarket_catalog_records",
        _timed_store("aggregate_reconstruction", AxiomStore._aggregate_polymarket_catalog_records),
    )
    patch(AxiomStore, "load_candidate_lifecycle", _timed_store("lifecycle_loads", AxiomStore.load_candidate_lifecycle))
    patch(CanaryService, "status", _timed_service("canary_service_status", CanaryService.status))
    patch(
        CanaryService,
        "validate_eligibility",
        _timed_service("validated_eligibility", CanaryService.validate_eligibility),
    )
    patch(
        CanaryService,
        "_selection_record",
        _timed_service("current_selection", CanaryService._selection_record),
    )
    patch(
        canary_module,
        "evaluate_prediction_data_quality",
        _timed_function("evaluate_prediction_data_quality", canary_module.evaluate_prediction_data_quality),
    )
    patch(
        canary_module,
        "_canary_ranking_snapshot_hash",
        _timed_function("ranking_validation", canary_module._canary_ranking_snapshot_hash),
    )
    # Keep direct consumers of the public quality module covered if a dashboard
    # implementation imports it lazily in a future version.
    if quality_module.evaluate_prediction_data_quality is not canary_module.evaluate_prediction_data_quality:
        patch(
            quality_module,
            "evaluate_prediction_data_quality",
            _timed_function("evaluate_prediction_data_quality", quality_module.evaluate_prediction_data_quality),
        )
    try:
        yield
    finally:
        for target, attribute, original in reversed(patches):
            setattr(target, attribute, original)


@contextmanager
def _safe_credential_projection() -> Iterator[None]:
    """Prevent dashboard profiling from touching keyring or environment data."""
    original = CredentialStore.safe_projection

    def safe_projection(_self: CredentialStore, **_kwargs: Any) -> dict[str, Any]:
        return {
            "configured": False,
            "status": "NOT CONFIGURED",
            "secret_values_exposed": False,
        }

    CredentialStore.safe_projection = safe_projection  # type: ignore[method-assign]
    try:
        yield
    finally:
        CredentialStore.safe_projection = original  # type: ignore[method-assign]


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


def _historical_records(market_id: str, count: int, market_index: int) -> list[dict[str, Any]]:
    start = T0 + timedelta(hours=market_index * 8)
    result: list[dict[str, Any]] = []
    for index in range(count):
        timestamp = start + timedelta(hours=index)
        result.append(
            {
                "market_id": market_id,
                "source_timestamp": _iso(timestamp),
                "timestamp": _iso(timestamp),
                "price": round(0.35 + (index % 11) / 100, 4),
                "yes_mid": round(0.35 + (index % 11) / 100, 4),
                "token_id": f"{market_id}-yes",
                "source_type": "HISTORICAL",
                "quality": "PRICE_PROXY",
            }
        )
    return result


def _candidate_payload(candidate_index: int, history_rows: int) -> dict[str, Any]:
    strategy_hash = f"fixture-strategy-{candidate_index:04d}"
    model_hash = f"fixture-model-{candidate_index:04d}"
    config_hash = f"fixture-config-{candidate_index:04d}"
    frozen_hash = hashlib.sha256(
        f"{strategy_hash}|{model_hash}|{config_hash}".encode("utf-8")
    ).hexdigest()
    quality_fields = {
        "historical_data_integrity": "PASS",
        "historical_data_integrity_passed": True,
        "historical_execution_fidelity": "PRICE_PROXY",
        "historical_execution_fidelity_score": 0.35,
        "current_execution_evidence": "CURRENT_ORDER_BOOK_REQUIRED",
        "historical_provenance_complete": True,
        "historical_rows_nonempty": True,
        "historical_no_forward_contamination": True,
        "canary_data_quality_acceptable": True,
        "canary_data_quality_status": "CANARY_DATA_QUALITY_ACCEPTABLE_LIMITED",
        "production_evidence_status": "INSUFFICIENT",
        "historical_dataset_row_count": history_rows,
    }
    return {
        "market_type": "prediction",
        "dataset_id": "Polymarket-historical",
        "dataset_version": "v1",
        "dataset_provenance": {
            "dataset_id": "Polymarket-historical",
            "dataset_version": "v1",
            "source_type": "HISTORICAL",
            "time_split": "train-validation-holdout",
        },
        "lineage": [f"candidate-{candidate_index:04d}"],
        "mutation_cluster": f"fixture-cluster-{candidate_index:04d}",
        "experiment_family": "dashboard-latency-profile",
        "schema_validated": True,
        "historical_backtest_passed": True,
        "validation_passed": True,
        "robustness_passed": True,
        "data_quality_passed": True,
        "frozen": True,
        "holdout_used": False,
        "strategy_hash": strategy_hash,
        "model_hash": model_hash,
        "config_hash": config_hash,
        "frozen_hash": frozen_hash,
        "validation_expectancy": round(0.72 - candidate_index / 1000, 6),
        "validation_confidence_lower_bound": round(0.61 - candidate_index / 2000, 6),
        "validation_stability": round(0.85 - candidate_index / 5000, 6),
        "validation_calibration": round(0.82 - candidate_index / 5000, 6),
        "validation_sample_count": max(100, history_rows),
        "validation_trade_count": max(50, history_rows // 2),
        "validation_execution_quality": round(0.88 - candidate_index / 5000, 6),
        "validation_max_drawdown": 0.12,
        "validation_liquidity": 0.9,
        "data_quality": "PRICE_PROXY",
        "minimum_sample_check": {
            "passed": True,
            "count": max(100, history_rows),
            "trades": max(50, history_rows // 2),
            "min_observations": 30,
            "min_trades": 10,
            "checks": {"observations": True, "trades": True},
        },
        "experiment_plan": {
            "min_independent_samples": 30,
            "min_trades": 10,
            "dataset_id": "Polymarket-historical",
            "dataset_version": "v1",
        },
        "forward_evidence": {
            "forward_expectancy": round(0.42 - candidate_index / 2000, 6),
            "forward_sample_count": max(100, history_rows),
        },
        **quality_fields,
    }


def build_fixture(
    database_path: Path,
    candidate_count: int = DEFAULT_CANDIDATES,
    history_rows: int = DEFAULT_HISTORY_ROWS,
) -> dict[str, Any]:
    """Build and validate one temporary representative fixture."""
    for name, value in (("candidate_count", candidate_count), ("history_rows", history_rows)):
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise ValueError(f"{name} must be a positive integer")
    store = AxiomStore(str(database_path))
    try:
        market_count = max(1, min(4, (history_rows + 6) // 7))
        base, remainder = divmod(history_rows, market_count)
        market_sizes = [base + (1 if index < remainder else 0) for index in range(market_count)]
        market_versions: list[dict[str, Any]] = []
        history_start: datetime | None = None
        history_end: datetime | None = None
        for market_index, market_size in enumerate(market_sizes):
            market_id = f"fixture-market-{market_index:02d}"
            records = _historical_records(market_id, market_size, market_index)
            market_start = T0 + timedelta(hours=market_index * 8)
            market_end = market_start + timedelta(hours=max(0, market_size - 1))
            metadata = {
                "provider": "temporary-fixture",
                "source_type": "HISTORICAL",
                "market_id": market_id,
                "category": "fixture",
                "question": f"Will fixture event {market_index:02d} resolve yes?",
                "resolution_criteria": "fixture resolution",
                "settlement": "resolved_yes",
                "token_ids": {"yes": f"{market_id}-yes", "no": f"{market_id}-no"},
                "research_quality": "PRICE_PROXY",
                "historical_order_book_available": False,
            }
            constituent_id = f"prediction:{market_id}"
            store.save_dataset(
                constituent_id,
                "v1",
                records,
                metadata=metadata,
                quality="PRICE_PROXY",
            )
            store.save_dataset_catalog(
                constituent_id,
                "v1",
                provider="temporary-fixture",
                instrument=market_id,
                market_type="PREDICTION",
                timeframe="1h",
                start_timestamp=market_start,
                end_timestamp=market_end,
                row_count=market_size,
                completeness=1.0,
                quality="PRICE_PROXY",
                source_type="HISTORICAL",
                snapshot_id=f"pmhist:{market_id}:v1",
                metadata=metadata,
                created_at=T0,
                updated_at=T0,
            )
            market_versions.append(
                {
                    "market_id": market_id,
                    "dataset_id": constituent_id,
                    "version": "v1",
                    "records": market_size,
                }
            )
            if market_size:
                history_start = market_start if history_start is None else min(history_start, market_start)
                history_end = market_end if history_end is None else max(history_end, market_end)

        aggregate_metadata = {
            "provider": "temporary-fixture",
            "source_type": "HISTORICAL",
            "research_quality": "PRICE_PROXY",
            "historical_order_book_available": False,
            "market_versions": market_versions,
            "category_counts": {"fixture": market_count},
        }
        store.save_dataset_catalog(
            "Polymarket-historical",
            "v1",
            provider="temporary-fixture",
            instrument="POLYMARKET",
            market_type="PREDICTION",
            timeframe="event",
            start_timestamp=history_start,
            end_timestamp=history_end,
            row_count=history_rows,
            completeness=1.0,
            quality="PRICE_PROXY",
            source_type="HISTORICAL",
            snapshot_id="pmhist:aggregate:v1",
            metadata=aggregate_metadata,
            created_at=T0,
            updated_at=T0,
        )

        service = CanaryService(store, clock=lambda: T0)
        for candidate_index in range(candidate_count):
            candidate_id = f"candidate-{candidate_index:04d}"
            payload = _candidate_payload(candidate_index, history_rows)
            store.save_candidate_lifecycle(candidate_id, "IDEA", payload, reason="temporary fixture", timestamp=T0)
            store.save_candidate_lifecycle(
                candidate_id,
                "FROZEN",
                payload,
                from_stage="IDEA",
                reason="temporary fixture",
                timestamp=T0,
            )
            store.save_candidate_lifecycle(
                candidate_id,
                "PAPER_FORWARD",
                payload,
                from_stage="FROZEN",
                reason="temporary fixture",
                timestamp=T0,
            )
            service.mark_eligible(candidate_id)

        ranking = CandidateCanaryRanker(store, service=service, clock=lambda: T0).evaluate_and_select(T0)
        service.enable_autonomous_micro_live()
        status = service.status()
        ranking_rows = store.connection.execute("SELECT COUNT(*) AS n FROM canary_rankings").fetchone()["n"]
        eligibility_rows = store.connection.execute("SELECT COUNT(*) AS n FROM canary_eligibility").fetchone()["n"]
        candidate_rows = store.connection.execute("SELECT COUNT(*) AS n FROM candidate_lifecycle WHERE stage='PAPER_FORWARD'").fetchone()["n"]
        if (
            int(candidate_rows) != candidate_count
            or int(eligibility_rows) != candidate_count
            or int(ranking_rows) != candidate_count
            or int(status.get("eligible_count") or 0) != candidate_count
            or int(status.get("rankable_count") or 0) != candidate_count
            or status.get("selection_status") != "CURRENT"
            or status.get("selection_valid") is not True
            or status.get("micro_live_canary") != "AUTONOMOUS_MICRO_LIVE"
            or not ranking.get("selected_candidate")
        ):
            raise RuntimeError("temporary dashboard fixture failed its persisted projection contract")
        return {
            "candidate_count": candidate_count,
            "history_rows": history_rows,
            "paper_forward_candidates": candidate_count,
            "eligibility_rows": int(eligibility_rows),
            "validated_eligibility_rows": int(status.get("eligible_count") or 0),
            "ranking_rows": int(ranking_rows),
            "valid_ranking_rows": int(status.get("rankable_count") or 0),
            "current_winners": 1,
            "selected_candidate": str(status.get("selected_candidate") or ""),
            "autonomous_control": str(status.get("micro_live_canary") or ""),
            "historical_aggregate": {
                "dataset_id": "Polymarket-historical",
                "dataset_version": "v1",
                "rows": history_rows,
                "constituent_catalogs": market_count,
            },
        }
    finally:
        # Close/checkpoint before a byte-for-byte fixture copy.  Both paths are
        # under TemporaryDirectory and are therefore safe to mutate.
        try:
            store.connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        except Exception:
            pass
        store.close()

def _request(opener: Any, server: DashboardServer, endpoint: str, stats: _Stats | None) -> dict[str, Any]:
    if not server.url:
        raise RuntimeError("dashboard server did not bind an ephemeral port")
    request = Request(
        f"{server.url}{endpoint}",
        headers={"Accept": "application/json", "Connection": "close"},
    )
    started = time.perf_counter_ns()
    with _request_stats(stats):
        try:
            with opener.open(request, timeout=30) as response:
                body = response.read()
                status = int(response.status)
        finally:
            elapsed_ns = max(0, time.perf_counter_ns() - started)
    if status != 200:
        raise RuntimeError(f"{endpoint} returned HTTP {status}")
    return {
        "status": status,
        "response_bytes": len(body),
        "elapsed_ns": elapsed_ns,
        "elapsed_ms": round(elapsed_ns / 1_000_000, 6),
    }


def _open_profile_server(database_path: Path) -> tuple[AxiomStore, DashboardServer]:
    store = AxiomStore(str(database_path))
    server = DashboardServer(
        host="127.0.0.1",
        port=0,
        data=DashboardData(store=store),
    ).start()
    return store, server


def _profile_cold(database_template: Path, directory: Path) -> dict[str, Any]:
    result: dict[str, Any] = {}
    opener = build_opener(ProxyHandler({}))
    for index, endpoint in enumerate(ENDPOINTS):
        database_path = directory / f"cold-{index}.sqlite3"
        shutil.copyfile(database_template, database_path)
        store, server = _open_profile_server(database_path)
        stats = _Stats()
        try:
            timing = _request(opener, server, endpoint, stats)
            timing["contributors"] = stats.report()
            result[endpoint] = timing
        finally:
            server.stop()
            store.close()
    return result


def _profile_warm(database_template: Path, directory: Path, warm_calls: int) -> dict[str, Any]:
    result: dict[str, Any] = {}
    opener = build_opener(ProxyHandler({}))
    for index, endpoint in enumerate(ENDPOINTS):
        database_path = directory / f"warm-{index}.sqlite3"
        shutil.copyfile(database_template, database_path)
        store, server = _open_profile_server(database_path)
        try:
            # Prime this endpoint without including the first-call cold caches
            # in the warm series.  Each endpoint receives its own fresh store.
            _request(opener, server, endpoint, None)
            stats = _Stats()
            samples: list[dict[str, Any]] = []
            for _ in range(warm_calls):
                samples.append(_request(opener, server, endpoint, stats))
            elapsed_ns = [int(item["elapsed_ns"]) for item in samples]
            result[endpoint] = {
                "calls": warm_calls,
                "prime_excluded": True,
                "samples": samples,
                "summary": {
                    "min_ns": min(elapsed_ns),
                    "max_ns": max(elapsed_ns),
                    "mean_ns": round(sum(elapsed_ns) / warm_calls),
                    "min_ms": round(min(elapsed_ns) / 1_000_000, 6),
                    "max_ms": round(max(elapsed_ns) / 1_000_000, 6),
                    "mean_ms": round(sum(elapsed_ns) / warm_calls / 1_000_000, 6),
                },
                "contributors": stats.report(),
            }
        finally:
            server.stop()
            store.close()
    return result


def profile(
    candidate_count: int = DEFAULT_CANDIDATES,
    history_rows: int = DEFAULT_HISTORY_ROWS,
    warm_calls: int = DEFAULT_WARM_CALLS,
) -> dict[str, Any]:
    """Build a temporary fixture and return cold/warm JSON-compatible evidence."""
    for name, value in (
        ("candidate_count", candidate_count),
        ("history_rows", history_rows),
        ("warm_calls", warm_calls),
    ):
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise ValueError(f"{name} must be a positive integer")
    with tempfile.TemporaryDirectory(prefix="axiom-dashboard-profile-") as temporary:
        directory = Path(temporary)
        template = directory / "fixture.sqlite3"
        fixture_size = build_fixture(template, candidate_count, history_rows)
        with _safe_credential_projection(), _instrument_contributors():
            cold = _profile_cold(template, directory)
            warm = _profile_warm(template, directory, warm_calls)
        return {
            "schema": "axiom.dashboard_latency_profile.v1",
            "fixture_size": fixture_size,
            "measurement": {
                "endpoints": list(ENDPOINTS),
                "cold_isolation": "fresh-temporary-store-and-dashboard-server-per-endpoint",
                "warm_isolation": "fresh-temporary-store-per-endpoint-with-sequential-repeated-calls",
                "warm_calls": warm_calls,
                "credential_projection": "forced-safe-not-configured",
                "external_network": False,
            },
            "counts": {
                "cold_requests": len(ENDPOINTS),
                "warm_requests": len(ENDPOINTS) * warm_calls,
                "successful_requests": len(ENDPOINTS) * (warm_calls + 1),
            },
            "cold": cold,
            "warm": warm,
        }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--candidates",
        "--candidate-count",
        dest="candidate_count",
        type=int,
        default=DEFAULT_CANDIDATES,
        help=f"number of PAPER_FORWARD candidates (default: {DEFAULT_CANDIDATES})",
    )
    parser.add_argument(
        "--history-rows",
        "--rows",
        "--row-count",
        dest="history_rows",
        type=int,
        default=DEFAULT_HISTORY_ROWS,
        help=f"historical aggregate row count (default: {DEFAULT_HISTORY_ROWS})",
    )
    parser.add_argument(
        "--warm-calls",
        type=int,
        default=DEFAULT_WARM_CALLS,
        help=f"measured repeated calls per warm endpoint (default: {DEFAULT_WARM_CALLS})",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    if arguments.candidate_count < 1:
        raise SystemExit("--candidates must be a positive integer")
    if arguments.history_rows < 1:
        raise SystemExit("--history-rows must be a positive integer")
    if arguments.warm_calls < 1:
        raise SystemExit("--warm-calls must be a positive integer")
    payload = profile(arguments.candidate_count, arguments.history_rows, arguments.warm_calls)
    print(json.dumps(payload, allow_nan=False, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
