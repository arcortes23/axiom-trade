"""Command-line entry point for deterministic offline Axiom workflows."""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import json
from typing import Any, Sequence

from .dashboard import DashboardServer
from .domain import OHLCVBar
from .evaluation import evaluate_scores, split_dataset
from .research import run_initial_research, write_report
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
    dashboard_commands = dashboard.add_subparsers(dest="dashboard_command")
    start = dashboard_commands.add_parser("start", help="start dashboard HTTP server")
    start.add_argument("--host", default="127.0.0.1")
    start.add_argument("--port", type=int, default=8080)
    start.add_argument("--once", action="store_true", help="start, print URL, and stop (useful for smoke checks)")
    historical = commands.add_parser("historical", help="run public Binance and Polymarket research")
    historical.add_argument("--markets", type=int, default=20, help="maximum resolved prediction markets to inspect")
    historical.add_argument("--timeout", type=float, default=10.0)
    historical.add_argument("--output", help="optional JSON report path")
    historical.add_argument("--db", help="optional SQLite artifact database path")
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
        finally:
            if store is not None:
                store.close()
        if args.output:
            write_report(report, args.output)
        print(json.dumps(payload, sort_keys=True, indent=2))
        return 0
    if args.command == "dashboard" and args.dashboard_command == "start":
        server = DashboardServer(args.host, args.port)
        if args.once:
            server.start()
            print(server.url)
            server.stop()
            return 0
        try:
            print(f"Axiom dashboard: http://{args.host}:{args.port}")
            server.serve_forever()
        except KeyboardInterrupt:
            return 0
        finally:
            server.stop()
        return 0
    build_parser().print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["synthetic_bars", "run_synthetic_research", "build_parser", "main"]
