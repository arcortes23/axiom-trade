"""Command-line entry point for deterministic offline Axiom workflows."""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
from pathlib import Path
from typing import Any, Mapping, Sequence

from .collector import CollectorConfig, PolymarketCollector
from .dashboard import DashboardData, DashboardServer
from .data import BinanceAdapter, PolymarketAdapter, SyntheticCryptoProvider
from .director import compact_report, research_summary, validate_hermes_proposal
from .domain import OHLCVBar, utc_now
from .evaluation import evaluate_scores, split_dataset
from .forward import ForwardTestRegistry, _content_hash
from .node import NodeConfig, ResearchNode
from .paper_engine import historical_replay_id, run_forward_paper, run_historical_replay
from .research import run_crypto_research, run_initial_research, write_report
from .research_bus import DurableResearchBus, ResearchBusPermissionError, _validate_payload
from .strategy import evaluate_signal_record, load_strategy
from .tracking import ExperimentTracker
from .storage import AxiomStore

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
class _CliStrategy:
    def __init__(self, definition: Any) -> None:
        self.definition = definition
        self.strategy_id = definition.id
        self._history: dict[str, list[Any]] = {}

    def signal(self, context: Mapping[str, Any]) -> Mapping[str, Any] | None:
        symbol = str(context.get("symbol", ""))
        history = self._history.setdefault(symbol, [])
        history.append(context.get("market", context.get("observation")))
        if len(history) > 512:
            del history[:-512]
        record = evaluate_signal_record(self.definition, {"observations": tuple(history)})
        if not record.actionable:
            return None
        score = float(record.score)
        if self.definition.market_type.value == "prediction":
            outcome = "yes" if score > 0 else "no"
            side = f"buy_{outcome}"
        else:
            outcome = None
            side = record.side
        return {"side": side, "quantity": abs(score), "outcome": outcome}


class _CliProbabilityModel:
    def __init__(self, document: Mapping[str, Any]) -> None:
        self.document = dict(document)

    def predict_probability(self, observation: Mapping[str, Any]) -> float | None:
        try:
            if "probability" in self.document:
                return float(self.document["probability"])
            if "yes_probability" in self.document:
                return float(self.document["yes_probability"])
            field = self.document.get("field")
            if isinstance(field, str) and field.strip():
                value = observation.get(field)
                return float(value) if value is not None else None
        except (TypeError, ValueError):
            return None
        return None




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
    dashboard.add_argument("dashboard_command", nargs="?", choices=("start",), help="optional explicit start verb")
    dashboard.add_argument("--host", default="127.0.0.1")
    dashboard.add_argument("--port", type=int, default=8080)
    dashboard.add_argument("--db", help="optional SQLite artifact database path")
    dashboard.add_argument("--once", action="store_true", help="bind and stop after readiness smoke check")
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
    collect.add_argument("--max-markets", type=int, default=100)
    collect.add_argument("--timeout", type=float, default=10.0)
    collect.add_argument("--max-attempts", type=int, default=3)
    collect.add_argument("--cooldown", type=float, default=30.0)
    health = commands.add_parser("dataset-health", help="show Polymarket collection health")
    health.add_argument("--db", required=True)
    health.add_argument("--interval", type=float, default=60.0)
    health.add_argument("--stale-after", type=float)
    backtests = commands.add_parser("run-backtests", help="run deterministic crypto backtests")
    backtests.add_argument("--db", help="optional SQLite artifact database path")
    backtests.add_argument("--symbol", default="BTC/USDT")
    backtests.add_argument("--source", choices=("synthetic", "public"), default="synthetic")
    backtests.add_argument("--rows", type=int, default=30)
    backtests.add_argument("--start")
    backtests.add_argument("--end")
    backtests.add_argument("--timeout", type=float, default=10.0)
    backtests.add_argument("--output")
    forward = commands.add_parser("run-forward-paper", aliases=("run-forward",), help="run a registered paper-only forward test")
    forward.add_argument("--db", required=True)
    forward.add_argument("--strategy", required=True, help="strategy document or identifier")
    forward.add_argument("--model", required=True, help="model document or identifier")
    forward.add_argument("--experiment", required=True, help="registered forward-test experiment id")
    register = commands.add_parser("register-forward-test", help="register a new paper-only forward test")
    register.add_argument("--db", required=True)
    register.add_argument("--strategy", required=True)
    register.add_argument("--model", required=True)
    register.add_argument("--start")
    register.add_argument("--experiment")
    register.add_argument("--bankroll", type=float, default=10_000.0)
    register.add_argument("--market-id", action="append", default=[])
    replay = commands.add_parser("run-historical-replay", help="explicitly replay persisted historical observations")
    replay.add_argument("--db", required=True)
    replay.add_argument("--strategy", required=True)
    replay.add_argument("--model", required=True)
    replay.add_argument("--start", default=_SYNTHETIC_START.isoformat())
    replay.add_argument("--end")
    replay.add_argument("--experiment")
    replay.add_argument("--bankroll", type=float, default=10_000.0)
    replay.add_argument("--market-id", action="append", default=[])
    replay.add_argument("--max-observations", type=int, default=100_000)
    summary = commands.add_parser("research-summary", help="print the compact persisted research summary")
    summary.add_argument("--report")
    summary.add_argument("--db")
    queue = commands.add_parser("candidate-queue", help="list lifecycle candidates and frozen paper tests")
    queue.add_argument("--db", required=True)
    node_run = commands.add_parser("node-run", aliases=("run-research-node",), help="run the always-on public-data paper node")
    node_run.add_argument("--db", required=True)
    node_run.add_argument("--cycles", type=int, default=0, help="finite test cycles; 0 runs until stopped")
    node_run.add_argument("--interval", type=float, default=60.0)
    node_run.add_argument("--depth", type=int, default=20)
    node_run.add_argument("--max-markets", type=int, default=100)
    node_run.add_argument("--log")
    node_run.add_argument("--lock")
    node_run.add_argument("--crypto-source", choices=("public", "synthetic", "disabled"), default="public")
    node_run.add_argument("--crypto-symbol", default="BTC/USDT")
    node_run.add_argument("--crypto-timeout", type=float, default=10.0)
    node_status = commands.add_parser("node-status", help="show persisted node status")
    node_status.add_argument("--db", required=True)
    node_status.add_argument("--lock")
    node_status.add_argument("--log")
    proposal = commands.add_parser("submit-proposal", help="validate and enqueue a bounded Hermes research proposal")
    proposal.add_argument("--db", required=True)
    proposal.add_argument("--proposal", required=True, help="JSON object or path to a JSON file")
    return parser
def _register_cli_forward_test(args: argparse.Namespace, store: AxiomStore, *, historical: bool = False) -> Any:
    current = utc_now()
    start = _parse_cli_timestamp(args.start)
    if start is None:
        start = _SYNTHETIC_START if historical else current
    strategy_definition, model_document = _load_cli_documents(args.strategy, args.model)
    if strategy_definition.market_type.value != "prediction":
        raise ValueError("CLI forward and replay commands require prediction strategies")
    registry = ForwardTestRegistry(store)
    execution_config = {
        "execution": "paper_only",
        "live_execution": False,
        "strategy_document": strategy_definition.to_dict(),
        "model_document": dict(model_document),
    }
    if historical:
        execution_config["historical_replay"] = True
        return registry.freeze(
            strategy=strategy_definition,
            model=model_document,
            start_timestamp=start,
            bankroll=args.bankroll,
            allowed_markets=tuple(args.market_id),
            config=execution_config,
            risk_limits={"max_position_fraction": 0.05},
            experiment_id=args.experiment,
        )
    return registry.register_forward_test(
        strategy=strategy_definition,
        model=model_document,
        registration_timestamp=start,
        now=current,
        bankroll=args.bankroll,
        allowed_markets=tuple(args.market_id),
        config=execution_config,
        risk_limits={"max_position_fraction": 0.05},
        experiment_id=args.experiment,
    )


def _resolve_cli_forward_test(args: argparse.Namespace, store: AxiomStore, *, historical: bool = False) -> Any:
    registry = ForwardTestRegistry(store)
    if historical:
        if args.experiment:
            spec = registry.get(args.experiment)
            if spec is not None:
                if not bool(spec.config.get("historical_replay")):
                    raise ValueError("historical replay cannot reuse a registered forward-test experiment")
                return spec
        return _register_cli_forward_test(args, store, historical=True)
    if not args.experiment:
        raise ValueError("run-forward-paper requires --experiment from register-forward-test")
    spec = registry.get(args.experiment)
    if spec is not None:
        return spec
    raise ValueError(f"unknown registered forward-test experiment: {args.experiment}")

def _load_json_argument(value: str) -> Any:
    path = Path(value)
    if path.is_file():
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    return json.loads(value)
def _load_cli_documents(strategy_value: str, model_value: str) -> tuple[Any, Mapping[str, Any]]:
    try:
        strategy = load_strategy(_load_json_argument(strategy_value))
    except Exception as exc:
        raise ValueError(f"strategy must be a valid JSON strategy document or path: {exc}") from exc
    try:
        raw_model = _load_json_argument(model_value)
    except Exception as exc:
        raise ValueError(f"model must be a JSON model document or path: {exc}") from exc
    if isinstance(raw_model, (int, float)) and not isinstance(raw_model, bool):
        raw_model = {"probability": raw_model}
    if not isinstance(raw_model, Mapping):
        raise ValueError("model document must be an object or a probability number")
    model = dict(raw_model)
    if "probability" in model or "yes_probability" in model:
        value = model.get("probability", model.get("yes_probability"))
        try:
            probability = float(value)
        except (TypeError, ValueError) as exc:
            raise ValueError("model probability must be numeric") from exc
        if not math.isfinite(probability) or not 0.0 <= probability <= 1.0:
            raise ValueError("model probability must be finite and within [0, 1]")
    elif not isinstance(model.get("field"), str) or not model["field"].strip():
        raise ValueError("model document requires probability, yes_probability, or field")
    try:
        _validate_payload(strategy.to_dict())
        model = _validate_payload(model)
    except (ResearchBusPermissionError, TypeError, ValueError) as exc:
        raise ValueError("strategy/model documents contain forbidden private or execution fields") from exc
    return strategy, model

def _load_cli_execution_inputs(args: argparse.Namespace, spec: Any) -> tuple[Any, Any]:
    strategy_definition, model_document = _load_cli_documents(args.strategy, args.model)
    if strategy_definition.market_type.value != "prediction":
        raise ValueError("CLI forward and replay commands require prediction strategies")
    if _content_hash(strategy_definition) != spec.strategy_hash:
        raise ValueError("strategy document does not match the frozen forward-test hash")
    if _content_hash(model_document) != spec.model_hash:
        raise ValueError("model document does not match the frozen forward-test hash")
    return _CliStrategy(strategy_definition), _CliProbabilityModel(model_document)


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
                store.save_report_if_absent(report_id, payload)
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
                    max_attempts=args.max_attempts,
                    failure_cooldown_seconds=args.cooldown,
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
            start = _parse_cli_timestamp(args.start)
            end = _parse_cli_timestamp(args.end)
            if start is not None and end is not None and end < start:
                raise ValueError("--end must be on or after --start")
            if args.source == "public":
                if start is None or end is None:
                    raise ValueError("public backtests require --start and --end")
                payload = run_crypto_research(
                    store=store,
                    symbol=args.symbol,
                    start=start,
                    end=end,
                    timeout=args.timeout,
                )
            else:
                provider = SyntheticCryptoProvider(
                    args.symbol,
                    start=start or _SYNTHETIC_START,
                    periods=args.rows,
                )
                payload = run_crypto_research(
                    provider=provider,
                    store=store,
                    symbol=args.symbol,
                    start=start,
                    end=end,
                    timeout=args.timeout,
                )
            payload["input_mode"] = args.source
            payload["frozen_input"] = args.source == "synthetic"
        finally:
            if store is not None:
                store.close()
        if args.output:
            write_report(payload, args.output)
        print(json.dumps(payload, sort_keys=True, indent=2))
        return 0
    if args.command == "register-forward-test":
        with AxiomStore(args.db) as store:
            spec = _register_cli_forward_test(args, store)
            payload = {
                "forward_test": spec.as_record(),
                "paper_only": True,
                "live_execution": False,
            }
        print(json.dumps(payload, sort_keys=True, indent=2, default=str))
        return 0
    if args.command in {"run-forward-paper", "run-forward"}:
        with AxiomStore(args.db) as store:
            spec = _resolve_cli_forward_test(args, store)
            strategy, model = _load_cli_execution_inputs(args, spec)
            cycle = run_forward_paper(spec, store=store, strategy=strategy, model=model, now=utc_now())
            payload = {
                "forward_test": spec.as_record(),
                "cycle": cycle.as_record(),
                "state": store.load_paper_state(spec.experiment_id),
                "paper_only": True,
                "live_execution": False,
            }
        print(json.dumps(payload, sort_keys=True, indent=2, default=str))
        return 0
    if args.command == "run-historical-replay":
        with AxiomStore(args.db) as store:
            spec = _resolve_cli_forward_test(args, store, historical=True)
            strategy, model = _load_cli_execution_inputs(args, spec)
            market_ids = tuple(args.market_id) or tuple(store.tracked_polymarket_markets(active_only=False))
            replay_start = _parse_cli_timestamp(args.start)
            replay_end = _parse_cli_timestamp(args.end)
            if replay_start is not None and replay_end is not None and replay_end < replay_start:
                raise ValueError("--end must be on or after --start")
            if isinstance(args.max_observations, bool) or args.max_observations < 0:
                raise ValueError("--max-observations must be non-negative")
            observations: list[Any] = []
            remaining = args.max_observations
            for market_id in market_ids:
                if remaining <= 0:
                    break
                rows = store.load_polymarket_snapshots(
                    market_id,
                    source_start=replay_start,
                    source_end=replay_end,
                    limit=remaining,
                )
                remaining -= len(rows)
                for row in rows:
                    source_timestamp = row.get("source_timestamp") or row.get("observed_at")
                    if source_timestamp is None:
                        continue
                    observation = dict(row.get("payload") or {})
                    observation.setdefault("market_id", market_id)
                    observation.setdefault("timestamp", source_timestamp)
                    observations.append(observation)
            cycle = run_historical_replay(
                spec,
                store=store,
                strategy=strategy,
                model=model,
                observations=observations,
            )
            payload = {
                "forward_test": spec.as_record(),
                "cycle": cycle.as_record(),
                "state": store.load_paper_state(historical_replay_id(spec, observations)),
                "historical_replay": True,
                "paper_only": True,
                "live_execution": False,
            }
        print(json.dumps(payload, sort_keys=True, indent=2, default=str))
        return 0
    if args.command == "research-summary":
        if args.report:
            with open(args.report, "r", encoding="utf-8") as handle:
                payload = compact_report(json.load(handle))
        elif args.db:
            with AxiomStore(args.db) as store:
                payload = research_summary(store)
        else:
            payload = {"error": "provide --report or --db"}
        print(json.dumps(payload, sort_keys=True, indent=2, default=str))
        return 0 if "error" not in payload else 1
    if args.command == "candidate-queue":
        with AxiomStore(args.db) as store:
            experiments = [
                item for item in store.list_experiments(limit=100)
                if not bool((item.get("experiment") or {}).get("rejected", False))
            ]
            payload = {
                "experiments": experiments,
                "forward_tests": store.load_forward_tests(limit=100),
                "candidate_lifecycle": store.load_candidate_lifecycle(limit=100),
                "research_queue": store.list_research_items(limit=100),
                "queue_stats": store.research_queue_stats(),
                "live_execution": False,
            }
        print(json.dumps(payload, sort_keys=True, indent=2, default=str))
        return 0
    if args.command in {"node-run", "run-research-node"}:
        if args.crypto_source == "public":
            crypto_provider = BinanceAdapter(args.crypto_symbol, timeout=args.crypto_timeout)
        elif args.crypto_source == "synthetic":
            crypto_provider = SyntheticCryptoProvider(args.crypto_symbol, start=_SYNTHETIC_START, periods=1000)
        else:
            crypto_provider = None
        node = ResearchNode(
            NodeConfig(
                db_path=args.db,
                lock_path=args.lock,
                log_path=args.log,
                interval_seconds=args.interval,
                depth=args.depth,
                max_markets=args.max_markets,
                crypto_symbol=args.crypto_symbol,
                crypto_enabled=args.crypto_source != "disabled",
            ),
            crypto_provider=crypto_provider,
        )
        try:
            cycles = None if args.cycles == 0 else args.cycles
            results = node.run(max_cycles=cycles)
            payload = {
                "cycles": [cycle.as_record() for cycle in results],
                "status": node.status(),
                "paper_only": True,
                "live_execution": False,
            }
        except KeyboardInterrupt:
            node.stop()
            payload = {"status": node.status(), "stopped": True, "paper_only": True, "live_execution": False}
        print(json.dumps(payload, sort_keys=True, indent=2, default=str))
        return 0
    if args.command == "node-status":
        with AxiomStore(args.db) as store:
            node = ResearchNode(
                NodeConfig(db_path=args.db, lock_path=args.lock, log_path=args.log),
                store=store,
            )
            payload = node.status()
        print(json.dumps(payload, sort_keys=True, indent=2, default=str))
        return 0
    if args.command == "submit-proposal":
        proposal = _load_json_argument(args.proposal)
        validation = validate_hermes_proposal(proposal)
        if not validation.accepted:
            print(json.dumps(validation.as_record(), sort_keys=True, indent=2, default=str))
            return 1
        with AxiomStore(args.db) as store:
            item = DurableResearchBus(store).submit_hypothesis(
                validation.normalized or {},
                dedupe_key=validation.proposal_id,
            )
            payload = {"validation": validation.as_record(), "queue_item": item.as_record(), "live_execution": False}
        print(json.dumps(payload, sort_keys=True, indent=2, default=str))
        return 0
    if args.command == "dashboard" and getattr(args, "dashboard_command", None) in {None, "start"}:
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
