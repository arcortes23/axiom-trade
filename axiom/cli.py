"""Command-line entry point for deterministic offline Axiom workflows."""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import hashlib
import json
from typing import Any, Sequence

from .collector import CollectorConfig, PolymarketCollector
from .dashboard import DashboardData, DashboardServer
from .data import PolymarketAdapter
from .domain import OHLCVBar, utc_now
from .evaluation import evaluate_scores, split_dataset
from .forward import ForwardTestRegistry
from .research import run_crypto_research, run_initial_research, write_report
from .storage import AxiomStore
from .tracking import ExperimentTracker

_SYNTHETIC_START = datetime(2024, 1, 1, tzinfo=timezone.utc)


def synthetic_bars(count: int = 30, *, start: datetime = _SYNTHETIC_START, symbol: str = "SYNTH") -> tuple[OHLCVBar, ...]:
    """Generate reproducible bars without network access."""
    if count <= 0:
        raise ValueError("count must be positive")
    start = start.astimezone(timezone.utc) if start.tzinfo else start.replace(tzinfo=timezone.utc)
    bars = []
    for index in range(count):
        base = 100.0 + index * 0.25 + (index % 5) * 0.1
        bars.append(OHLCVBar(start + timedelta(days=index), base, base + 1.0, base - 1.0, base + 0.5, 1000.0 + index * 10.0))
    return tuple(bars)


def run_synthetic_research(*, rows: int = 30, train: int = 15, validation: int = 7, holdout: int = 5, strategy_id: str = "synthetic-trend") -> dict[str, Any]:
    bars = synthetic_bars(rows)
    if train <= 0 or validation <= 0 or holdout <= 0 or train + validation + holdout > rows:
        raise ValueError("train, validation, and holdout must be positive and fit within rows")
    train_end = bars[train].timestamp
    validation_end = bars[train + validation].timestamp
    holdout_end = bars[train + validation + holdout - 1].timestamp + timedelta(microseconds=1)
    split = split_dataset(bars, train_end, validation_end, holdout_end, dataset_version="synthetic-v1", require_nonempty=True)
    # A tiny transparent score: average close-to-open return per partition.
    def score(partition: Sequence[OHLCVBar]) -> float:
        if not partition:
            return 0.0
        return sum((bar.close - bar.open) / bar.open for bar in partition) / len(partition)

    evaluation = evaluate_scores(score(split.train), score(split.validation), score(split.holdout))
    tracker = ExperimentTracker()
    experiment = tracker.track(
        strategy_id,
        "1.0.0",
        provider="synthetic",
        instrument="SYNTH",
        dataset_version=split.dataset_version,
        features=("open", "close", "volume"),
        model_version="none",
        executable_prices={"SYNTH": split.holdout[-1].close},
        regime="rising",
        cost_assumptions={"fee_rate": 0.0, "slippage_bps": 0.0},
        metrics={"train": evaluation.train_score, "validation": evaluation.validation_score, "holdout": evaluation.holdout_score, "fitness": evaluation.fitness},
        fitness=evaluation.fitness,
        created_at=_SYNTHETIC_START,
    )
    return {"split": split.as_record(), "evaluation": {"train_score": evaluation.train_score, "validation_score": evaluation.validation_score, "holdout_score": evaluation.holdout_score, "degradation": evaluation.degradation, "overfit_penalty": evaluation.overfit_penalty, "fitness": evaluation.fitness}, "experiment": experiment.to_record()}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="axiom", description="Deterministic Axiom research tools")
    commands = parser.add_subparsers(dest="command")
    demo = commands.add_parser("demo", aliases=("synthetic-demo",), help="run an offline synthetic demonstration")
    demo.add_argument("--rows", type=int, default=30)
    demo.add_argument("--train", type=int, default=15)
    demo.add_argument("--validation", type=int, default=7)
    demo.add_argument("--holdout", type=int, default=5)
    demo.add_argument("--strategy", default="synthetic-trend")
    research = commands.add_parser("research", help="run deterministic offline research")
    research.add_argument("--rows", type=int, default=30)
    research.add_argument("--train", type=int, default=15)
    research.add_argument("--validation", type=int, default=7)
    research.add_argument("--holdout", type=int, default=5)
    research.add_argument("--strategy", default="synthetic-trend")
    dashboard = commands.add_parser("dashboard", help="serve the local read-only dashboard")
    dashboard.add_argument("--host", default="127.0.0.1")
    dashboard.add_argument("--port", type=int, default=8080)
    dashboard.add_argument("--db", help="optional SQLite artifact database path")
    dashboard.add_argument("--once", action="store_true", help="bind and stop after readiness smoke check")
    dashboard_commands = dashboard.add_subparsers(dest="dashboard_command")
    start = dashboard_commands.add_parser("start", help="start dashboard HTTP server")
    start.add_argument("--host", default="127.0.0.1")
    start.add_argument("--port", type=int, default=8080)
    start.add_argument("--db", help="optional SQLite artifact database path")
    start.add_argument("--once", action="store_true", help="bind and stop after readiness smoke check")
    historical = commands.add_parser("historical", help="run public Binance and Polymarket research")
    historical.add_argument("--markets", type=int, default=20, help="maximum resolved prediction markets to inspect")
    historical.add_argument("--timeout", type=float, default=10.0)
    historical.add_argument("--output", help="optional JSON report path")
    historical.add_argument("--db", help="optional SQLite artifact database path")
    collect = commands.add_parser("collect-data", help="collect immutable Polymarket metadata, books, and trades")
    collect.add_argument("--db", required=True, help="SQLite collection database path")
    collect.add_argument("--cycles", type=int, default=1, help="number of cycles; 0 runs continuously")
    collect.add_argument("--interval", type=float, default=60.0)
    collect.add_argument("--depth", type=int, default=20)
    collect.add_argument("--market-id", action="append", default=[])
    collect.add_argument("--max-markets", type=int)
    collect.add_argument("--timeout", type=float, default=10.0)
    health = commands.add_parser("dataset-health", help="show Polymarket collection health")
    health.add_argument("--db", required=True)
    health.add_argument("--interval", type=float, default=60.0)
    health.add_argument("--stale-after", type=float)
    backtests = commands.add_parser("run-backtests", help="run deterministic crypto backtests")
    backtests.add_argument("--db", help="optional SQLite artifact database path")
    backtests.add_argument("--symbol", default="BTC/USDT")
    backtests.add_argument("--timeout", type=float, default=10.0)
    backtests.add_argument("--output")
    forward = commands.add_parser("run-forward-paper", help="freeze a paper-only forward-test specification")
    forward.add_argument("--db", required=True)
    forward.add_argument("--strategy", required=True, help="strategy document or identifier")
    forward.add_argument("--model", required=True, help="model document or identifier")
    forward.add_argument("--start", default=_SYNTHETIC_START.isoformat(), help="UTC ISO timestamp; explicit default keeps runs reproducible")
    forward.add_argument("--experiment")
    forward.add_argument("--bankroll", type=float, default=10_000.0)
    forward.add_argument("--market-id", action="append", default=[])
    summary = commands.add_parser("research-summary", help="print a saved research report")
    summary.add_argument("--report")
    summary.add_argument("--db")
    queue = commands.add_parser("candidate-queue", help="list eligible experiments and frozen paper tests")
    queue.add_argument("--db", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command in {"demo", "synthetic-demo", "research"}:
        result = run_synthetic_research(rows=args.rows, train=args.train, validation=args.validation, holdout=args.holdout, strategy_id=args.strategy)
        print(json.dumps(result, sort_keys=True, indent=2))
        return 0
    if args.command == "historical":
        store = AxiomStore(args.db) if args.db else None
        try:
            report = run_initial_research(store=store, market_limit=args.markets, timeout=args.timeout)
            payload = report.to_dict()
            if store is not None:
                stable_payload = {key: payload[key] for key in ("crypto", "prediction", "limitations")}
                report_id = "initial-" + hashlib.sha256(
                    json.dumps(stable_payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
                ).hexdigest()[:24]
                if store.load_report(report_id) is None:
                    store.save_report(report_id, payload)
        finally:
            if store is not None:
                store.close()
        if args.output:
            write_report(report, args.output)
        print(json.dumps(payload, sort_keys=True, indent=2))
        return 0
    if args.command == "collect-data":
        store = AxiomStore(args.db)
        try:
            collector = PolymarketCollector(
                PolymarketAdapter(timeout=args.timeout),
                store,
                CollectorConfig(
                    interval_seconds=args.interval,
                    depth=args.depth,
                    max_markets=args.max_markets,
                    market_ids=tuple(args.market_id),
                ),
            )
            cycles = None if args.cycles == 0 else args.cycles
            results = collector.run_forever(cycles=cycles)
            payload = {
                "cycles": [result.as_record() for result in results],
                "health": store.polymarket_health(
                    expected_interval_seconds=args.interval,
                    stale_after_seconds=args.interval * 3.0,
                ),
                "live_execution": False,
            }
        finally:
            store.close()
        print(json.dumps(payload, sort_keys=True, indent=2))
        return 0
    if args.command == "dataset-health":
        with AxiomStore(args.db) as store:
            payload = store.polymarket_health(
                expected_interval_seconds=args.interval,
                stale_after_seconds=args.stale_after,
            )
        print(json.dumps(payload, sort_keys=True, indent=2))
        return 0
    if args.command == "run-backtests":
        store = AxiomStore(args.db) if args.db else None
        try:
            payload = run_crypto_research(store=store, symbol=args.symbol, timeout=args.timeout)
        finally:
            if store is not None:
                store.close()
        if args.output:
            write_report(payload, args.output)
        print(json.dumps(payload, sort_keys=True, indent=2))
        return 0
    if args.command == "run-forward-paper":
        with AxiomStore(args.db) as store:
            spec = ForwardTestRegistry(store).freeze(
                strategy=args.strategy,
                model=args.model,
                start_timestamp=_parse_cli_timestamp(args.start) or _SYNTHETIC_START,
                bankroll=args.bankroll,
                allowed_markets=tuple(args.market_id),
                config={"execution": "paper_only", "live_execution": False},
                risk_limits={"max_position_fraction": 0.05},
                experiment_id=args.experiment,
            )
            payload = spec.as_record()
        print(json.dumps(payload, sort_keys=True, indent=2))
        return 0
    if args.command == "research-summary":
        if args.report:
            with open(args.report, "r", encoding="utf-8") as handle:
                payload = json.load(handle)
        elif args.db:
            with AxiomStore(args.db) as store:
                reports = store.list_reports()
                payload = reports[-1]["report"] if reports else {"error": "no persisted report"}
        else:
            payload = {"error": "provide --report or --db"}
        print(json.dumps(payload, sort_keys=True, indent=2))
        return 0 if "error" not in payload else 1
    if args.command == "candidate-queue":
        with AxiomStore(args.db) as store:
            experiments = [
                item for item in store.list_experiments()
                if not bool((item.get("experiment") or {}).get("rejected", False))
            ]
            payload = {
                "experiments": experiments,
                "forward_tests": store.load_forward_tests(),
                "live_execution": False,
            }
        print(json.dumps(payload, sort_keys=True, indent=2, default=str))
        return 0
    if args.command == "dashboard" and args.dashboard_command in {None, "start"}:
        dashboard_store = AxiomStore(args.db) if args.db else None
        server = DashboardServer(args.host, args.port, data=DashboardData(store=dashboard_store))
        if args.once:
            server.start()
            print(server.url)
            server.stop()
            if dashboard_store is not None:
                dashboard_store.close()
            return 0
        try:
            print(f"Axiom dashboard: http://{args.host}:{args.port}")
            server.serve_forever()
        except KeyboardInterrupt:
            return 0
        finally:
            server.stop()
            if dashboard_store is not None:
                dashboard_store.close()
        return 0
    build_parser().print_help()
    return 0
def _parse_cli_timestamp(value: str | None) -> datetime | None:
    if value is None:
        return None
    text = value.strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"invalid ISO timestamp: {value}") from exc
    return parsed.astimezone(timezone.utc) if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)




if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["synthetic_bars", "run_synthetic_research", "build_parser", "main"]
