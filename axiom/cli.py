"""Command-line entry point for deterministic offline Axiom workflows."""
from __future__ import annotations

import getpass
import argparse
import os
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping, Sequence
from decimal import Decimal
import webbrowser

from .collector import CollectorConfig, PolymarketCollector
from .canary import (
    CanaryBlocked,
    CanaryService,
    CredentialStore,
    PolymarketClobV2Venue,
)
from .operator import OperatorControlPlane
from .dashboard import DashboardData, DashboardServer
from .data import BinanceAdapter, PolymarketAdapter, SyntheticCryptoProvider
from .director import compact_report, research_summary, validate_hermes_proposal
from .domain import OHLCVBar, utc_now
from .evaluation import evaluate_scores, split_dataset
from .forward import ForwardTestRegistry, _content_hash
from .paper_engine import historical_replay_id, run_forward_paper, run_historical_replay
from .node import (
    EXECUTION_PROFILE_ENV,
    ISOLATED_EXECUTION_PROFILE,
    NodeConfig,
    ResearchNode,
    normalized_execution_profile,
)
from .research import run_crypto_research, run_initial_research, write_report
from .research import run_multi_symbol_crypto_research
from .research_bus import DurableResearchBus, ResearchBusPermissionError, _validate_payload
from .strategy import evaluate_signal_record, load_strategy
from .strategy.signals import evaluate_model_document_probability
from .tracking import ExperimentTracker
from .storage import AxiomStore, SQLiteBusyTimeout
from .binance_dev import BinanceDevelopmentRuntime, BinanceTestnetRuntime
from .binance_spot import (
    BINANCE_SPOT_TESTNET,
    BinanceCredentialRef,
    BinanceCredentialStore,
)
from .bootstrap import (
    BTC_HISTORY_START,
    BTC_INTERVAL_SECONDS,
    HistoricalBootstrapper,
    crypto_universe_dataset_id,
    run_btc_historical_research,
)
from .crypto_universe import (
    CoinGeckoRankingProvider,
    UniverseConfig,
    crypto_universe_status,
    load_crypto_universe,
    refresh_crypto_universe,
)

_SYNTHETIC_START = datetime(2024, 1, 1, tzinfo=timezone.utc)
DEFAULT_DB_PATH = "runtime-data/axiom.sqlite"
_BINANCE_TESTNET_CREDENTIAL_INSTANCE = "binance-testnet"
_BINANCE_TESTNET_CREDENTIAL_NAMESPACE = "AXIOM-BINANCE-SPOT-TESTNET"

_CANARY_SUBMIT_SUCCESS_STATUSES = frozenset(
    {"SUBMITTED", "ACCEPTED", "MATCHED", "PARTIALLY_FILLED", "SETTLED"}
)
_CANARY_SUBMIT_FAILURE_STATUSES = frozenset(
    {
        "REJECTED",
        "UNKNOWN",
        "AMBIGUOUS",
        "FAILED",
        "FAILURE",
        "ERROR",
        "BLOCKED",
        "DENIED",
        "TIMEOUT",
        "DISCONNECTED",
        "RATE_LIMIT",
        "CANCELED",
        "CANCELLED",
        "EXPIRED",
    }
)


def _canary_status_ready(payload: Mapping[str, Any]) -> bool:
    """Return the storage-only status exit predicate.

    Readiness keeps its existing CURRENT/stale contract.  An expired deadline
    is additionally rejected even when the read-only report has projected the
    persisted ARMED row to DISARMED, so a stale arm cannot look submittable.
    """
    readiness = payload.get("readiness")
    if not (
        isinstance(readiness, Mapping)
        and readiness.get("status") == "CURRENT"
        and not bool(readiness.get("stale"))
    ):
        return False
    control = payload.get("control")
    if not isinstance(control, Mapping):
        control = payload.get("authoritative_control")
    if not isinstance(control, Mapping):
        return True
    state = str(control.get("state") or "").strip().upper()
    expires_at = control.get("expires_at")
    if state in {"ARMED", "DISARMED"} and expires_at:
        try:
            parsed_expiry = datetime.fromisoformat(str(expires_at).replace("Z", "+00:00"))
            parsed_expiry = (
                parsed_expiry.astimezone(timezone.utc)
                if parsed_expiry.tzinfo is not None
                else parsed_expiry.replace(tzinfo=timezone.utc)
            )
        except (TypeError, ValueError, OverflowError):
            return False if state == "ARMED" else True
        if parsed_expiry <= utc_now():
            return False
    return True


def _canary_submit_succeeded(payload: Any) -> bool:
    """Accept only an explicit canonical submit outcome from CanaryService."""
    if not isinstance(payload, Mapping):
        return False
    execution_status = str(payload.get("execution_status") or "").strip().upper()
    if execution_status not in _CANARY_SUBMIT_SUCCESS_STATUSES:
        return False
    status = str(payload.get("status") or "").strip().upper()
    return not status or status not in _CANARY_SUBMIT_FAILURE_STATUSES


class _OperatorPortAction(argparse.Action):
    """Record explicit operator ports so isolated mode can choose 8187."""

    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values: Any,
        option_string: str | None = None,
    ) -> None:
        setattr(namespace, self.dest, int(values))
        setattr(namespace, "_operator_port_explicit", True)

def _binance_testnet_credential_ref() -> BinanceCredentialRef:
    return BinanceCredentialRef(
        instance=_BINANCE_TESTNET_CREDENTIAL_INSTANCE,
        environment=BINANCE_SPOT_TESTNET,
        namespace=_BINANCE_TESTNET_CREDENTIAL_NAMESPACE,
    )


def _binance_testnet_credential_status(*, configured: bool) -> dict[str, Any]:
    """Return the only credential metadata the CLI is allowed to expose."""

    return {
        "environment": BINANCE_SPOT_TESTNET,
        "namespace": _BINANCE_TESTNET_CREDENTIAL_NAMESPACE,
        "configured": bool(configured),
        "secret_values_exposed": False,
    }


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
        return evaluate_model_document_probability(self.document, observation)




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
    dashboard.add_argument("--db", default=DEFAULT_DB_PATH, help="SQLite operational database path")
    dashboard.add_argument("--once", action="store_true", help="bind and stop after readiness smoke check")
    binance_dev = commands.add_parser("binance-dev", help="serve the isolated Binance Spot development runtime")
    binance_dev.add_argument("--once", action="store_true", help="run one bounded worker cycle and stop")
    operator = commands.add_parser("operator", help="supervise one paper node and its localhost operator dashboard")
    binance_testnet = commands.add_parser(
        "binance-testnet",
        help="operate the isolated Binance Spot TESTNET runtime",
    )
    binance_testnet_commands = binance_testnet.add_subparsers(
        dest="binance_testnet_command",
        required=True,
    )
    binance_testnet_commands.add_parser("status", help="show TESTNET status without network calls")
    binance_testnet_commands.add_parser("connectivity", help="perform the explicit TESTNET connectivity check")
    validate_testnet = binance_testnet_commands.add_parser(
        "validate",
        help="perform the explicit TESTNET order validation gate",
    )
    validate_testnet.add_argument("--symbol")
    probe_testnet = binance_testnet_commands.add_parser(
        "probe",
        help="run the explicit TESTNET execution probe",
    )
    probe_testnet.add_argument("--symbol")
    probe_testnet.add_argument("--confirmation", required=True)
    binance_testnet_commands.add_parser(
        "reconcile",
        help="reconcile the persisted TESTNET execution probe",
    )
    serve_testnet = binance_testnet_commands.add_parser(
        "serve",
        help="serve the TESTNET dashboard on 127.0.0.1:8082",
    )
    serve_testnet.add_argument(
        "--once",
        action="store_true",
        help="bind and stop after the dashboard readiness smoke check",
    )
    auto_testnet = binance_testnet_commands.add_parser(
        "auto",
        help="run the explicitly enabled bounded TESTNET autonomous window",
    )
    auto_testnet.add_argument("--confirmation", required=True)
    auto_testnet.add_argument("--window-seconds", required=True, type=int)
    auto_testnet.add_argument("--symbol")
    operator.add_argument("--db", default=DEFAULT_DB_PATH, help="canonical SQLite operational database path")
    operator.add_argument("--port", type=int, default=8080, action=_OperatorPortAction)
    operator.add_argument("--hermes-job-id")
    operator.add_argument("--isolated", action="store_true", help="deny production credentials and order/account transports")
    operator.add_argument("--open-browser", action="store_true", help="open the localhost operator dashboard in the default browser")
    operator.add_argument("--once", action="store_true", help="bind and stop after readiness smoke check")
    historical = commands.add_parser("historical", help="run public Binance and Polymarket research")
    historical.add_argument("--markets", type=int, default=20, help="maximum resolved prediction markets to inspect")
    historical.add_argument("--timeout", type=float, default=10.0)
    historical.add_argument("--output", help="optional JSON report path")
    historical.add_argument("--markdown-output", help="optional Markdown report path")
    historical.add_argument("--db", default=DEFAULT_DB_PATH, help="SQLite operational database path")
    bootstrap = commands.add_parser("bootstrap-history", help="bootstrap resumable historical BTC and Polymarket datasets")
    bootstrap.add_argument("--db", default=DEFAULT_DB_PATH)
    bootstrap.add_argument("--crypto", action="store_true", help="bootstrap BTC OHLCV")
    bootstrap.add_argument("--polymarket", action="store_true", help="bootstrap resolved and closed Polymarket history")
    bootstrap.add_argument("--all", action="store_true", help="bootstrap both sources")
    bootstrap.add_argument("--resume", action="store_true", help="resume an incomplete staged bootstrap")
    bootstrap.add_argument("--status", action="store_true", help="show catalog and staged state without network access")
    bootstrap.add_argument("--full-15m", action="store_true", help="request BTC 15-minute history from the earliest configured date")
    bootstrap.add_argument("--interval", action="append", choices=("1d", "4h", "1h", "15m"), default=[])
    bootstrap.add_argument("--start")
    bootstrap.add_argument("--end")
    bootstrap.add_argument("--max-markets", type=int, default=1000)
    bootstrap.add_argument("--timeout", type=float, default=10.0)
    bootstrap.add_argument("--max-attempts", type=int, default=4)
    bootstrap.add_argument("--backoff", type=float, default=0.5)
    bootstrap.add_argument("--output")
    bootstrap.add_argument("--universe", action="store_true", help="bootstrap the selected immutable crypto universe")
    bootstrap.add_argument("--crypto-universe", action="store_true", help="bootstrap the latest persisted immutable crypto universe")
    bootstrap.add_argument("--universe-version", help="exact immutable crypto universe version")
    bootstrap.add_argument("--universe-id", help="exact immutable crypto universe identity")
    bootstrap.add_argument("--max-symbols", type=int, default=50)
    crypto_universe = commands.add_parser("crypto-universe", help="show or refresh the bounded crypto universe")
    universe_action = crypto_universe.add_mutually_exclusive_group(required=True)
    universe_action.add_argument("--status", action="store_true", help="show the persisted crypto universe status")
    universe_action.add_argument("--refresh", action="store_true", help="refresh the bounded crypto universe")
    crypto_universe.add_argument("--top", dest="top_n", type=int, default=50, help="maximum ranked assets (1-50)")
    crypto_universe.add_argument("--db", default=DEFAULT_DB_PATH)
    crypto_universe.add_argument("--timeout", type=float, default=10.0)
    universe_status = commands.add_parser("universe-status", help="show the persisted crypto universe status")
    universe_status.add_argument("--db", default=DEFAULT_DB_PATH)
    universe_status.add_argument("--universe-id")
    universe_status.add_argument("--universe-version")
    universe_refresh = commands.add_parser("universe-refresh", help="refresh the bounded crypto universe")
    universe_refresh.add_argument("--db", default=DEFAULT_DB_PATH)
    universe_refresh.add_argument("--force", action="store_true")
    universe_refresh.add_argument("--universe-id")
    universe_refresh.add_argument("--top-n", type=int, default=50)
    universe_refresh.add_argument("--quote-asset", default="USDT")
    universe_refresh.add_argument("--timeout", type=float, default=10.0)
    universe_research = commands.add_parser("crypto-research-universe", help="research one immutable crypto universe version")
    universe_research.add_argument("--db", default=DEFAULT_DB_PATH)
    universe_research.add_argument("--universe-version", required=True)
    universe_research.add_argument("--universe-id", help="exact immutable crypto universe identity")
    universe_research.add_argument("--timeframe", default="1d")
    universe_research.add_argument(
        "--source-type",
        choices=("HISTORICAL", "FORWARD_COLLECTED"),
        default="HISTORICAL",
    )
    universe_research.add_argument("--start")
    universe_research.add_argument("--end")
    universe_research.add_argument("--output")
    universe_research.add_argument("--timeout", type=float, default=10.0)
    crypto_research = commands.add_parser("crypto-research", help="research the exact persisted crypto universe")
    crypto_research.add_argument("--universe", required=True, help="persisted universe version or latest")
    crypto_research.add_argument("--universe-id", help="exact immutable crypto universe identity")
    crypto_research.add_argument("--db", default=DEFAULT_DB_PATH)
    crypto_research.add_argument("--timeframe", default="1d")
    crypto_research.add_argument(
        "--source-type",
        choices=("HISTORICAL", "FORWARD_COLLECTED"),
        default="HISTORICAL",
    )
    crypto_research.add_argument("--start")
    crypto_research.add_argument("--end")
    crypto_research.add_argument("--timeout", type=float, default=10.0)
    crypto_research.add_argument("--output")
    catalog = commands.add_parser("dataset-catalog", help="list immutable historical and forward dataset catalog records")
    catalog.add_argument("--db", default=DEFAULT_DB_PATH)
    catalog.add_argument("--source", choices=("HISTORICAL", "FORWARD_COLLECTED"))
    catalog.add_argument("--status", action="store_true", help="include resumable bootstrap state")
    btc_research = commands.add_parser("btc-research", help="run BTC historical walk-forward research from the catalog")
    btc_research.add_argument("--db", default=DEFAULT_DB_PATH)
    btc_research.add_argument("--dataset")
    btc_research.add_argument("--version")
    btc_research.add_argument("--output")
    btc_research.add_argument("--train-years", type=int, default=3)
    btc_research.add_argument("--validation-years", type=int, default=1)
    btc_research.add_argument("--holdout-years", type=int, default=1)
    btc_research.add_argument("--step-years", type=int, default=1)
    collect = commands.add_parser("collect-data", help="collect immutable Polymarket metadata, books, and trades")
    collect.add_argument("--db", default=DEFAULT_DB_PATH, help="SQLite operational database path")
    collect.add_argument("--cycles", type=int, default=1, help="number of cycles; 0 runs continuously")
    collect.add_argument("--interval", type=float, default=60.0)
    collect.add_argument("--depth", type=int, default=20)
    collect.add_argument("--market-id", action="append", default=[])
    collect.add_argument("--max-markets", type=int, default=100)
    collect.add_argument("--timeout", type=float, default=10.0)
    collect.add_argument("--max-attempts", type=int, default=3)
    collect.add_argument("--cooldown", type=float, default=30.0)
    health = commands.add_parser("dataset-health", help="show Polymarket collection health")
    health.add_argument("--db", default=DEFAULT_DB_PATH)
    health.add_argument("--interval", type=float, default=60.0)
    health.add_argument("--stale-after", type=float)
    backtests = commands.add_parser("run-backtests", help="run deterministic crypto backtests")
    backtests.add_argument("--db", default=DEFAULT_DB_PATH, help="SQLite operational database path")
    backtests.add_argument("--symbol", default="BTC/USDT")
    backtests.add_argument("--source", choices=("synthetic", "public"), default="synthetic")
    backtests.add_argument("--rows", type=int, default=30)
    backtests.add_argument("--start")
    backtests.add_argument("--end")
    backtests.add_argument("--timeout", type=float, default=10.0)
    backtests.add_argument("--output")
    forward = commands.add_parser("run-forward-paper", aliases=("run-forward",), help="run a registered paper-only forward test")
    forward.add_argument("--db", default=DEFAULT_DB_PATH)
    forward.add_argument("--strategy", required=True, help="strategy document or identifier")
    forward.add_argument("--model", required=True, help="model document or identifier")
    forward.add_argument("--experiment", required=True, help="registered forward-test experiment id")
    register = commands.add_parser("register-forward-test", help="register a new paper-only forward test")
    register.add_argument("--db", default=DEFAULT_DB_PATH)
    register.add_argument("--strategy", required=True)
    register.add_argument("--model", required=True)
    register.add_argument("--start")
    register.add_argument("--experiment")
    register.add_argument("--bankroll", type=float, default=10_000.0)
    register.add_argument("--market-id", action="append", default=[])
    replay = commands.add_parser("run-historical-replay", help="explicitly replay persisted historical observations")
    replay.add_argument("--db", default=DEFAULT_DB_PATH)
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
    summary.add_argument("--db", default=DEFAULT_DB_PATH)
    queue = commands.add_parser("candidate-queue", help="list lifecycle candidates and frozen paper tests")
    queue.add_argument("--db", default=DEFAULT_DB_PATH)
    node_run = commands.add_parser("node-run", aliases=("run-research-node",), help="run the always-on public-data paper node")
    node_run.add_argument("--db", default=DEFAULT_DB_PATH)
    node_run.add_argument("--cycles", type=int, default=0, help="finite test cycles; 0 runs until stopped")
    node_run.add_argument("--isolated", action="store_true", help="deny production credentials and order/account transports")
    node_run.add_argument("--interval", type=float, default=60.0, help="collection interval in seconds")
    node_run.add_argument("--depth", type=int, default=20)
    node_run.add_argument("--max-markets", type=int, default=100)
    node_run.add_argument("--log")
    node_run.add_argument("--lock")
    node_run.add_argument("--pid")
    node_run.add_argument("--crypto-source", choices=("public", "synthetic", "disabled"), default="public")
    node_run.add_argument("--crypto-symbol", default="BTC/USDT")
    node_run.add_argument("--research-items", type=int, default=1, help="bounded queue items per node cycle")
    node_run.add_argument("--paper-candidates", type=int, default=4, help="bounded PAPER_FORWARD candidates per scheduler pass")
    node_run.add_argument("--paper-observations", type=int, default=64, help="bounded observations per PAPER_FORWARD candidate pass")
    node_run.add_argument("--research-lease", type=float, default=300.0, help="queue lease seconds")
    node_run.add_argument("--experiment-total-limit", type=int, default=1000)
    node_run.add_argument("--experiment-family-limit", type=int, default=250)
    node_run.add_argument("--max-plan-variants", type=int, default=8)
    node_run.add_argument("--max-children-per-parent", type=int, default=2)
    node_run.add_argument("--max-generation-depth", type=int, default=2)
    node_run.add_argument("--max-experiments-per-day", type=int, default=250)
    node_run.add_argument("--disable-mutations", action="store_true")
    node_run.add_argument("--disable-research", action="store_true")
    node_run.add_argument("--crypto-timeout", type=float, default=10.0)
    node_status = commands.add_parser("node-status", help="show persisted node status")
    node_status.add_argument("--db", default=DEFAULT_DB_PATH)
    node_status.add_argument("--lock")
    node_status.add_argument("--log")
    node_status.add_argument("--pid")
    node_status.add_argument("--isolated", action="store_true", help="report the isolated execution profile")
    proposal = commands.add_parser("submit-proposal", help="validate and enqueue a bounded Hermes research proposal")
    proposal.add_argument("--db", default=DEFAULT_DB_PATH)
    proposal.add_argument("--proposal", required=True, help="JSON object or path to a JSON file")
    credentials = commands.add_parser("credentials", help="configure or inspect secure venue credentials")
    credential_commands = credentials.add_subparsers(dest="credentials_command", required=True)
    credential_configure = credential_commands.add_parser("configure")
    credential_configure.add_argument("venue", choices=("polymarket",))
    credential_status = credential_commands.add_parser("status")
    credential_status.add_argument("venue", nargs="?", choices=("polymarket",), default="polymarket")
    credential_status.add_argument("--allow-environment", action="store_true")
    binance_credentials = commands.add_parser(
        "binance-credentials",
        help="configure or inspect isolated Binance Spot TESTNET credentials",
    )
    binance_credential_commands = binance_credentials.add_subparsers(
        dest="binance_credentials_command",
        required=True,
    )
    binance_credential_configure = binance_credential_commands.add_parser(
        "configure",
        help="configure Binance Spot TESTNET credentials using hidden prompts",
    )
    binance_credential_configure.add_argument(
        "--environment",
        choices=("testnet",),
        required=True,
        help="credential environment (only testnet is permitted)",
    )
    binance_credential_status = binance_credential_commands.add_parser(
        "status",
        help="show safe Binance Spot TESTNET credential status",
    )
    binance_credential_status.add_argument(
        "--environment",
        choices=("testnet",),
        required=True,
        help="credential environment (only testnet is permitted)",
    )
    for name, help_text in (
        ("canary-status", "show micro-live canary state"),
        ("canary-disarm", "return immediately to paper-only"),
        ("canary-kill", "prevent all further canary submissions"),
    ):
        command = commands.add_parser(name, help=help_text)
        command.add_argument("--db", default=DEFAULT_DB_PATH)
    eligible = commands.add_parser("canary-eligible", help="verify and record independent canary eligibility")
    eligible.add_argument("--db", default=DEFAULT_DB_PATH)
    eligible.add_argument("--candidate", required=True)
    arm = commands.add_parser("canary-arm", help="explicitly arm an expiring Polymarket micro-live canary")
    arm.add_argument("--db", default=DEFAULT_DB_PATH)
    arm.add_argument("--venue", choices=("polymarket",), required=True)
    arm.add_argument("--config-id", required=True, help="exact ACTIVE settings configuration ID under review")
    arm.add_argument("--expected-generation", required=True, type=int, help="exact ACTIVE settings generation under review")
    arm.add_argument("--candidate", required=True)
    arm.add_argument("--target-notional-usd", type=Decimal, default=Decimal("1.00"))
    arm.add_argument("--expires-hours", type=Decimal, default=Decimal("24"))
    arm.add_argument("--allow-environment", action="store_true")
    check = commands.add_parser(
        "canary-check",
        aliases=("canary-connectivity-check",),
        help="perform strict authenticated read-only checks without placing an order",
    )
    check.add_argument("--db", default=DEFAULT_DB_PATH)
    check.add_argument("--isolated", action="store_true", help="deny production credentials and order/account transports")
    check.add_argument("--candidate")
    check.add_argument("--market")
    check.add_argument("--market-id", dest="market")
    check.add_argument("--token")
    check.add_argument("--asset-id", dest="token")
    check.add_argument("--allow-environment", action="store_true")
    signal_command = commands.add_parser(
        "canary-signal",
        help="evaluate one eligible candidate against the latest stored observation",
    )
    signal_command.add_argument("--db", default=DEFAULT_DB_PATH)
    signal_command.add_argument("--candidate", required=True)
    submit_command = commands.add_parser(
        "canary-submit",
        help="submit exactly one persisted canary signal after operator confirmation",
    )
    submit_command.add_argument("--db", default=DEFAULT_DB_PATH)
    submit_command.add_argument("--signal", required=True)
    submit_command.add_argument("--allow-environment", action="store_true")
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


_MAX_CLI_UNIVERSE_SIZE = 50


def _validate_cli_universe_size(value: Any) -> int:
    """Validate a CLI universe bound independently of helper defaults."""
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= _MAX_CLI_UNIVERSE_SIZE:
        raise ValueError(f"crypto universe size must be an integer from 1 to {_MAX_CLI_UNIVERSE_SIZE}")
    return value


def _load_cli_universe(
    store: AxiomStore,
    selector: str | None = None,
    *,
    universe_id: str | None = None,
) -> tuple[Any, str, dict[str, Any], tuple[str, ...]]:
    """Resolve a selector to one persisted snapshot and immutable provenance."""
    requested = None if selector is None or selector.strip().lower() == "latest" else selector.strip()
    explicit_id = str(universe_id).strip() if universe_id else None
    snapshot = load_crypto_universe(store, universe_id=explicit_id, version=requested)
    if snapshot is None:
        label = "latest" if requested is None else requested
        if explicit_id:
            label = f"{explicit_id}/{label}"
        raise ValueError(f"no persisted crypto universe found for {label}")
    if isinstance(snapshot, Mapping):
        version_value = snapshot.get("universe_version", snapshot.get("version", snapshot.get("snapshot_hash")))
        provenance = dict(snapshot)
        raw_records = snapshot.get("records")
        if isinstance(raw_records, Sequence) and not isinstance(raw_records, (str, bytes, bytearray)):
            symbols = tuple(
                str(row.get("binance_symbol") or row.get("symbol"))
                for row in raw_records
                if isinstance(row, Mapping) and row.get("selected", True)
            )
        else:
            symbols = tuple(str(item) for item in snapshot.get("selected_symbols", snapshot.get("symbols", ())))
    else:
        version_value = getattr(snapshot, "version", None)
        provenance_method = getattr(snapshot, "to_provenance", None)
        provenance = dict(provenance_method()) if callable(provenance_method) else dict(snapshot.as_dict())
        symbols = tuple(str(item) for item in getattr(snapshot, "selected_symbols", ()))
    version = str(version_value).strip() if version_value is not None else ""
    if not version:
        raise ValueError("persisted crypto universe must have an exact version")
    loaded_id = str(provenance.get("universe_id") or "").strip()
    if explicit_id and loaded_id and loaded_id != explicit_id:
        raise ValueError("persisted crypto universe identity does not match requested universe_id")
    if explicit_id:
        loaded_id = explicit_id
    if not loaded_id:
        raise ValueError("persisted crypto universe must have an exact universe_id")
    symbols = tuple(dict.fromkeys(item.strip() for item in symbols if item.strip()))
    _validate_cli_universe_size(len(symbols))
    provenance["universe_id"] = loaded_id
    provenance["universe_version"] = version
    provenance.setdefault("version", version)
    provenance.setdefault("snapshot_hash", version)
    provenance.setdefault("selected_symbols", list(symbols))
    provenance.setdefault("symbols", list(symbols))
    return snapshot, version, provenance, symbols


def _universe_config(*, universe_id: str | None = None, top_n: int = 50, quote_asset: str = "USDT") -> UniverseConfig:
    """Build a CLI universe config while retaining custom identity aliases."""
    _validate_cli_universe_size(top_n)
    explicit_id = str(universe_id).strip() if universe_id else None
    return UniverseConfig(top_n=top_n, quote_asset=quote_asset, universe_id=explicit_id)


def _snapshot_payload(item: Any) -> Any:
    return item.as_dict() if hasattr(item, "as_dict") else item


def _canonical_cli_symbol(value: Any) -> str:
    return str(value).replace("/", "").replace("-", "").replace("_", "").strip().upper()


def _load_cli_dataset_binding(
    store: AxiomStore,
    *,
    universe_id: str,
    universe_version: str,
    symbol: str,
    timeframe: str,
    source_type: str,
) -> dict[str, Any]:
    """Resolve one symbol to an exact catalog entry without contacting a provider."""
    timeframe_value = str(timeframe).strip()
    source_value = str(source_type).strip().upper()
    if not timeframe_value:
        raise ValueError("crypto research timeframe must not be empty")
    if source_value not in {"HISTORICAL", "FORWARD_COLLECTED"}:
        raise ValueError("crypto research source_type must be HISTORICAL or FORWARD_COLLECTED")

    dataset_id = crypto_universe_dataset_id(universe_id, universe_version, symbol, timeframe_value)
    catalog_loader = getattr(store, "load_dataset_catalog", None)
    if not callable(catalog_loader):
        raise ValueError("crypto research requires a persisted dataset catalog")

    candidates: list[Mapping[str, Any]] = []
    latest = catalog_loader(dataset_id)
    if isinstance(latest, Mapping):
        candidates.append(latest)
    versions_loader = getattr(store, "dataset_versions", None)
    if callable(versions_loader):
        for candidate_version in versions_loader(dataset_id):
            try:
                candidate = catalog_loader(dataset_id, str(candidate_version))
            except TypeError:
                break
            if isinstance(candidate, Mapping):
                candidates.append(candidate)

    matches: list[Mapping[str, Any]] = []
    for catalog in candidates:
        catalog_id = str(catalog.get("dataset_id", "")).strip()
        catalog_version = str(catalog.get("dataset_version", catalog.get("version", ""))).strip()
        metadata = catalog.get("metadata")
        metadata = dict(metadata) if isinstance(metadata, Mapping) else {}
        catalog_timeframe = str(
            catalog.get("timeframe") or metadata.get("timeframe") or metadata.get("interval") or ""
        ).strip()
        catalog_source = str(
            catalog.get("source_type") or metadata.get("source_type") or ""
        ).strip().upper()
        instrument = str(catalog.get("instrument") or metadata.get("instrument") or "").strip()
        market_type = str(catalog.get("market_type", "")).strip().lower()
        if (
            catalog_id != dataset_id
            or not catalog_version
            or catalog_version.casefold() in {"latest", "current", "unversioned"}
            or market_type != "crypto_spot"
            or catalog_timeframe != timeframe_value
            or catalog_source != source_value
            or not instrument
            or _canonical_cli_symbol(instrument) != _canonical_cli_symbol(symbol)
        ):
            continue
        matches.append(catalog)
    if matches:
        catalog = max(
            matches,
            key=lambda item: (
                str(item.get("updated_at") or item.get("created_at") or ""),
                str(item.get("dataset_version", item.get("version", ""))),
            ),
        )
        catalog_version = str(catalog.get("dataset_version", catalog.get("version", ""))).strip()
        return {
            "symbol": symbol,
            "dataset_id": dataset_id,
            "dataset_version": catalog_version,
            "timeframe": timeframe_value,
            "source_type": source_value,
            "catalog": dict(catalog),
        }

    raise ValueError(
        "missing persisted crypto dataset "
        f"{dataset_id} for timeframe={timeframe_value} source_type={source_value}"
    )




def _run_cli_crypto_research(
    args: argparse.Namespace,
    store: AxiomStore,
    *,
    selector: str,
    universe_id: str | None = None,
) -> dict[str, Any]:
    _, version, provenance, symbols = _load_cli_universe(store, selector, universe_id=universe_id)
    universe_id = str(provenance.get("universe_id") or "").strip()
    if not universe_id:
        raise ValueError("persisted crypto universe must have an exact universe_id")
    timeframe = str(getattr(args, "timeframe", "1d") or "").strip()
    source_type = str(getattr(args, "source_type", "HISTORICAL") or "").strip().upper()
    bindings: dict[str, dict[str, Any]] = {}
    missing: list[str] = []
    for symbol in symbols:
        try:
            bindings[symbol] = _load_cli_dataset_binding(
                store,
                universe_id=universe_id,
                universe_version=version,
                symbol=symbol,
                timeframe=timeframe,
                source_type=source_type,
            )
        except ValueError as exc:
            missing.append(f"{symbol}: {exc}")
    if missing:
        raise ValueError(
            "missing persisted crypto dataset(s); refusing provider/latest fallback: "
            + "; ".join(missing)
        )

    start = _parse_cli_timestamp(getattr(args, "start", None))
    end = _parse_cli_timestamp(getattr(args, "end", None))
    if start is not None and end is not None and end < start:
        raise ValueError("--end must be on or after --start")

    dataset_ids = {symbol: binding["dataset_id"] for symbol, binding in bindings.items()}
    dataset_versions = {
        symbol: binding["dataset_version"] for symbol, binding in bindings.items()
    }
    timeframes = {symbol: binding["timeframe"] for symbol, binding in bindings.items()}
    source_types = {symbol: binding["source_type"] for symbol, binding in bindings.items()}
    payload = run_crypto_research(
        provider=None,
        symbols=symbols,
        store=store,
        start=start,
        end=end,
        timeout=getattr(args, "timeout", 10.0),
        universe=provenance,
        universe_provenance=provenance,
        dataset_id=dataset_ids,
        dataset_version=dataset_versions,
        timeframe=timeframes,
        source_type=source_types,
    )
    payload = dict(payload)
    payload.setdefault("universe_id", universe_id)
    payload.setdefault("universe_version", version)
    payload.setdefault("universe_provenance", provenance)
    payload.setdefault("dataset_bindings", bindings)
    payload.setdefault(
        "dataset_provenance",
        {symbol: dict(binding["catalog"]) for symbol, binding in bindings.items()},
    )
    return payload

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
        if evaluate_model_document_probability(model, {}) is None:
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

def _run_binance_credentials(args: argparse.Namespace) -> int:
    """Run the TESTNET-only credential operator flow without exposing secrets."""

    if args.environment != "testnet":
        raise ValueError("Binance credentials support only --environment testnet")
    store = BinanceCredentialStore(ref=_binance_testnet_credential_ref())
    if args.binance_credentials_command == "configure":
        api_key = getpass.getpass("Binance Spot TESTNET API key: ")
        api_secret = getpass.getpass("Binance Spot TESTNET API secret: ")
        if not api_key or not api_secret:
            raise ValueError("Binance Spot TESTNET credential configuration is incomplete")
        try:
            store.configure(api_key, api_secret)
        finally:
            del api_key, api_secret
        configured = True
    elif args.binance_credentials_command == "status":
        configured = store.configured()
    else:  # pragma: no cover - argparse restricts this value.
        raise ValueError("unsupported Binance credentials command")
    print(json.dumps(_binance_testnet_credential_status(configured=configured), sort_keys=True, indent=2))
    return 0

def _run_binance_testnet(args: argparse.Namespace) -> int:
    """Dispatch one explicit TESTNET runtime command."""

    runtime: BinanceTestnetRuntime | None = None
    try:
        runtime = BinanceTestnetRuntime()
        command = args.binance_testnet_command
        if command == "status":
            # Status is intentionally read-only and does not take the profile
            # lock; a serving dashboard remains observable.
            payload = runtime.status()
        elif command == "connectivity":
            payload = runtime.locked_action("CONNECTIVITY_CHECK")
        elif command == "validate":
            payload = runtime.locked_action(
                "ORDER_VALIDATION_TEST",
                {"symbol": args.symbol} if args.symbol else {},
            )
        elif command == "probe":
            payload = runtime.locked_action(
                "EXECUTION_PROBE",
                {
                    **({"symbol": args.symbol} if args.symbol else {}),
                    "confirmation": args.confirmation,
                },
            )
        elif command == "reconcile":
            payload = runtime.locked_action("RECONCILE_PROBE")
        elif command == "serve":
            payload = runtime.serve(once=bool(args.once))
        elif command == "auto":
            payload = runtime.auto(
                confirmation=args.confirmation,
                window_seconds=args.window_seconds,
            )
        else:  # pragma: no cover
            raise ValueError(f"unsupported Binance TESTNET command: {command}")
        print(json.dumps(payload, sort_keys=True, indent=2, default=str))
        return 0
    finally:
        if runtime is not None:
            runtime.stop()

def _validate_cli_node_resource_paths(args: argparse.Namespace) -> None:
    """Keep CLI node ownership markers canonical to the selected database."""
    db_value = str(getattr(args, "db", "") or "").strip()
    if db_value.casefold() == ":memory:" or db_value.casefold().startswith("file:"):
        if getattr(args, "lock", None) or getattr(args, "pid", None):
            raise ValueError("node --lock/--pid overrides require a filesystem database")
        return
    db_path = os.path.abspath(os.path.expanduser(db_value))
    for option, suffix in (("lock", ".lock"), ("pid", ".node.pid")):
        supplied = getattr(args, option, None)
        if supplied is None or not str(supplied).strip():
            continue
        expected = os.path.normcase(os.path.abspath(f"{db_path}{suffix}"))
        actual = os.path.normcase(os.path.abspath(os.path.expanduser(str(supplied))))
        if actual != expected:
            raise ValueError(f"node --{option} must be the canonical database marker {db_path}{suffix}")

def _main_impl(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    isolated = bool(getattr(args, "isolated", False)) or (
        normalized_execution_profile(os.environ.get(EXECUTION_PROFILE_ENV))
        == ISOLATED_EXECUTION_PROFILE
    )
    if isolated:
        # Ambient isolation is authoritative for credential and order paths:
        # copied dashboard controls and --allow-environment cannot re-enable them.
        os.environ[EXECUTION_PROFILE_ENV] = ISOLATED_EXECUTION_PROFILE
    isolated_blocked_commands = {
        "binance-testnet",
        "binance-dev",
        "binance-credentials",
        "credentials",
        "canary-check",
        "canary-connectivity-check",
        "canary-arm",
        "canary-submit",
    }
    operator_port = int(args.port) if args.command == "operator" else None
    if (
        isolated
        and args.command == "operator"
        and not bool(getattr(args, "_operator_port_explicit", False))
    ):
        operator_port = 8187
    if isolated and args.command == "operator" and operator_port == 8080:
        print(
            json.dumps(
                {
                    "ready": False,
                    "blocked": True,
                    "blocker": "ISOLATED_OPERATOR_PORT_MUST_NOT_BE_8080",
                    "port": operator_port,
                    "required_port": 8187,
                },
                sort_keys=True,
                indent=2,
            )
        )
        return 1
    if isolated and args.command in isolated_blocked_commands:
        print(
            json.dumps(
                {
                    "ready": False,
                    "blocked": True,
                    "blocker": "ISOLATED_EXECUTION_PROFILE",
                    "command": args.command,
                    "paper_only": True,
                    "live_execution": False,
                },
                sort_keys=True,
                indent=2,
            )
        )
        return 1
    if args.command == "binance-testnet":
        return _run_binance_testnet(args)
    if args.command == "binance-dev":
        runtime = BinanceDevelopmentRuntime()
        try:
            runtime.start(once=bool(args.once))
            payload = runtime.status()
            if args.once:
                runtime.stop()
                payload["stopped"] = True
                print(json.dumps(payload, sort_keys=True, indent=2, default=str))
                return 0
            print(json.dumps(payload, sort_keys=True, indent=2, default=str))
            runtime.serve_forever()
            return 0
        except KeyboardInterrupt:
            return 0
        finally:
            runtime.stop()
    if args.command == "binance-credentials":
        return _run_binance_credentials(args)
    if args.command == "credentials":
        credentials = CredentialStore()
        if args.credentials_command == "configure":
            print("Use a dedicated Polymarket wallet containing only the small amount intended for AXIOM canary testing.")
            credentials.configure()
        print(json.dumps({"venue": "polymarket", "configured": credentials.configured(allow_environment=getattr(args, "allow_environment", False))}))
        return 0
    if args.command in {
        "canary-status",
        "canary-disarm",
        "canary-kill",
        "canary-eligible",
        "canary-arm",
        "canary-check",
        "canary-connectivity-check",
        "canary-signal",
        "canary-submit",
    }:
        try:
            with AxiomStore(args.db) as store:
                service = CanaryService(
                    store,
                    allow_environment=getattr(args, "allow_environment", False),
                    initialize=args.command != "canary-status",
                )
                if args.command == "canary-signal":
                    signal = service.generate_signal(args.candidate)
                    payload = {
                        "candidate": args.candidate,
                        "signal": signal,
                        "signal_status": "READY" if signal is not None else "NO_CANARY_SIGNAL",
                        "paper_only": True,
                        "live_execution": False,
                    }
                elif args.command == "canary-submit":
                    confirmation = input(
                        f"Submit persisted canary signal {args.signal}. "
                        "Type SUBMIT $1 to continue: "
                    )
                    if confirmation.strip() != "SUBMIT $1":
                        raise CanaryBlocked("OPERATOR_CONFIRMATION_REQUIRED")
                    payload = service.submit_signal(
                        args.signal,
                        venue=PolymarketClobV2Venue(
                            allow_environment=args.allow_environment
                        ),
                        allow_environment=args.allow_environment,
                    )
                elif args.command == "canary-status":
                    payload = service.status_report()
                elif args.command == "canary-disarm":
                    service.disarm()
                    payload = service.status()
                elif args.command == "canary-kill":
                    service.kill()
                    payload = service.status()
                elif args.command == "canary-eligible":
                    service.mark_eligible(args.candidate)
                    payload = {
                        "candidate": args.candidate,
                        "canary_eligible": True,
                        "live_execution": False,
                    }
                else:
                    credentials_configured = service.credentials.configured(
                        allow_environment=args.allow_environment
                    )
                    is_connectivity = args.command == "canary-connectivity-check"
                    # Connectivity constructs a venue backed by fresh
                    # CredentialStore reads so it can report SDK/geoblock
                    # diagnostics while still failing closed when absent.
                    venue = (
                        PolymarketClobV2Venue(
                            allow_environment=args.allow_environment,
                        )
                        if credentials_configured or is_connectivity
                        else None
                    )
                    if is_connectivity:
                        payload = service.connectivity_check(
                            candidate_id=args.candidate,
                            venue=venue,
                            market_id=args.market,
                            token_id=args.token,
                            allow_environment=args.allow_environment,
                        )
                    elif args.command == "canary-check":
                        payload = service.check(
                            candidate_id=args.candidate,
                            venue=venue,
                            market_id=args.market,
                            token_id=args.token,
                            allow_environment=args.allow_environment,
                        )
                    else:
                        if venue is None:
                            raise CanaryBlocked("CREDENTIALS_NOT_CONFIGURED")
                        confirmation = input(
                            f"Arm {args.candidate} on Polymarket for "
                            f"${args.target_notional_usd} until {args.expires_hours}h? "
                            "Type ARM: "
                        )
                        if confirmation.strip() != "ARM":
                            raise CanaryBlocked("OPERATOR_CONFIRMATION_REQUIRED")
                        payload = service.arm(
                            args.candidate,
                            venue=venue,
                            config_id=args.config_id,
                            expected_generation=args.expected_generation,
                            target_notional_usd=args.target_notional_usd,
                            expires_hours=args.expires_hours,
                            credentials_configured=credentials_configured,
                        )
            print(json.dumps(payload, sort_keys=True, indent=2, default=str))
            if args.command == "canary-status":
                return 0 if _canary_status_ready(payload) else 1
            if args.command == "canary-submit":
                return 0 if _canary_submit_succeeded(payload) else 1
            return 0 if payload.get("ready", True) else 1
        except CanaryBlocked as exc:
            print(
                json.dumps(
                    {
                        "ready": False,
                        "message": "NOT READY FOR MICRO LIVE CANARY",
                        "failures": [str(exc)],
                        "live_execution": False,
                    },
                    sort_keys=True,
                    indent=2,
                )
            )
            return 1
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
        if args.markdown_output:
            write_report(report, args.markdown_output)
        print(json.dumps(payload, sort_keys=True, indent=2))
        return 0
    if args.command == "crypto-universe":
        top_n = _validate_cli_universe_size(args.top_n)
        config = _universe_config(top_n=top_n)
        with AxiomStore(args.db) as store:
            if args.status:
                payload = crypto_universe_status(store, config=config)
            else:
                snapshot = refresh_crypto_universe(
                    CoinGeckoRankingProvider(per_page=top_n, timeout=args.timeout),
                    BinanceAdapter(timeout=args.timeout),
                    store,
                    config=config,
                    force=True,
                )
                payload = _snapshot_payload(snapshot)
        print(json.dumps(payload, sort_keys=True, indent=2, default=str))
        return 0
    if args.command == "universe-status":
        config = _universe_config(
            universe_id=args.universe_id,
            top_n=50,
        )
        with AxiomStore(args.db) as store:
            payload = crypto_universe_status(
                store,
                config=config,
                version=args.universe_version,
            )
        print(json.dumps(payload, sort_keys=True, indent=2, default=str))
        return 0
    if args.command == "universe-refresh":
        top_n = _validate_cli_universe_size(args.top_n)
        config = _universe_config(
            universe_id=args.universe_id,
            top_n=top_n,
            quote_asset=args.quote_asset,
        )
        with AxiomStore(args.db) as store:
            snapshot = refresh_crypto_universe(
                CoinGeckoRankingProvider(per_page=top_n, timeout=args.timeout),
                BinanceAdapter(timeout=args.timeout),
                store,
                config=config,
                force=args.force,
            )
            payload = _snapshot_payload(snapshot)
        print(json.dumps(payload, sort_keys=True, indent=2, default=str))
        return 0
    if args.command in {"crypto-research", "crypto-research-universe"}:
        selector = args.universe if args.command == "crypto-research" else args.universe_version
        with AxiomStore(args.db) as store:
            payload = _run_cli_crypto_research(
                args,
                store,
                selector=selector,
                universe_id=args.universe_id,
            )
        if args.output:
            write_report(payload, args.output)
        print(json.dumps(payload, sort_keys=True, indent=2, default=str))
        return 0
    if args.command == "bootstrap-history":
        with AxiomStore(args.db) as store:
            bootstrapper = HistoricalBootstrapper(
                store,
                crypto_provider=BinanceAdapter(timeout=args.timeout),
                prediction_provider=PolymarketAdapter(timeout=args.timeout),
                max_attempts=args.max_attempts,
                backoff=args.backoff,
            )
            if args.status:
                payload = bootstrapper.status()
            else:
                universe_mode = bool(args.crypto_universe or args.universe)
                if universe_mode:
                    if args.crypto or args.polymarket or args.all:
                        raise ValueError("--crypto-universe cannot be combined with --crypto, --polymarket, or --all")
                    max_symbols = _validate_cli_universe_size(args.max_symbols)
                    snapshot, version, _, _ = _load_cli_universe(
                        store,
                        args.universe_version,
                        universe_id=args.universe_id,
                    )
                    start = _parse_cli_timestamp(args.start) or BTC_HISTORY_START
                    end = _parse_cli_timestamp(args.end)
                    if end is not None and end < start:
                        raise ValueError("--end must be on or after --start")
                    payload = {
                        "crypto_universe": [
                            item.as_record()
                            for item in bootstrapper.bootstrap_crypto_universe(
                                snapshot,
                                universe_version=version,
                                intervals=args.interval or tuple(BTC_INTERVAL_SECONDS),
                                start=start,
                                end=end,
                                full_15m=args.full_15m,
                                resume=args.resume,
                                max_symbols=max_symbols,
                            )
                        ]
                    }
                else:
                    start = _parse_cli_timestamp(args.start) or BTC_HISTORY_START
                    end = _parse_cli_timestamp(args.end)
                    if end is not None and end < start:
                        raise ValueError("--end must be on or after --start")
                    selected_crypto = bool(args.crypto or args.all or not (args.crypto or args.polymarket or args.all))
                    selected_polymarket = bool(args.polymarket or args.all or not (args.crypto or args.polymarket or args.all))
                    payload = {}
                    if selected_crypto:
                        payload["crypto"] = [
                            item.as_record()
                            for item in bootstrapper.bootstrap_crypto(
                                intervals=args.interval or tuple(BTC_INTERVAL_SECONDS),
                                start=start,
                                end=end,
                                full_15m=args.full_15m,
                                resume=args.resume,
                            )
                        ]
                    if selected_polymarket:
                        payload["polymarket"] = bootstrapper.bootstrap_polymarket(
                            max_markets=args.max_markets,
                            resume=args.resume,
                        ).as_record()
        if args.output:
            Path(args.output).write_text(json.dumps(payload, sort_keys=True, indent=2, default=str), encoding="utf-8")
        print(json.dumps(payload, sort_keys=True, indent=2, default=str))
        return 0
    if args.command == "dataset-catalog":
        with AxiomStore(args.db) as store:
            payload = {"catalog": store.list_dataset_catalog(source_type=args.source, limit=10_000)}
            if args.status:
                payload["bootstrap"] = store.list_dataset_bootstrap_states(limit=10_000)
        print(json.dumps(payload, sort_keys=True, indent=2, default=str))
        return 0
    if args.command == "btc-research":
        with AxiomStore(args.db) as store:
            payload = run_btc_historical_research(
                store,
                dataset_id=args.dataset,
                version=args.version,
                train_years=args.train_years,
                validation_years=args.validation_years,
                holdout_years=args.holdout_years,
                step_years=args.step_years,
            )
        if args.output:
            Path(args.output).write_text(json.dumps(payload, sort_keys=True, indent=2, default=str), encoding="utf-8")
        print(json.dumps(payload, sort_keys=True, indent=2, default=str))
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
                "experiment_plans": store.list_experiment_plans(limit=100),
                "lifecycle_funnel": store.candidate_lifecycle_funnel(),
                "rejection_reasons": store.candidate_rejection_reasons(),
            }
        print(json.dumps(payload, sort_keys=True, indent=2, default=str))
        return 0
    if args.command in {"node-run", "run-research-node"}:
        _validate_cli_node_resource_paths(args)
        if isolated:
            crypto_provider = None
        elif args.crypto_source == "public":
            crypto_provider = BinanceAdapter(args.crypto_symbol, timeout=args.crypto_timeout)
        elif args.crypto_source == "synthetic":
            crypto_provider = SyntheticCryptoProvider(args.crypto_symbol, start=_SYNTHETIC_START, periods=1000)
        else:
            crypto_provider = None
        node = ResearchNode(
            NodeConfig(
                db_path=args.db,
                lock_path=args.lock,
                pid_path=args.pid,
                log_path=args.log,
                execution_profile=ISOLATED_EXECUTION_PROFILE if isolated else None,
                interval_seconds=args.interval,
                depth=args.depth,
                max_markets=args.max_markets,
                crypto_symbol=args.crypto_symbol,
                crypto_enabled=(not isolated) and args.crypto_source != "disabled",
                research_enabled=not args.disable_research,
                research_max_items_per_cycle=args.research_items,
                paper_candidates_per_cycle=args.paper_candidates,
                paper_observations_per_candidate=args.paper_observations,
                research_lease_seconds=args.research_lease,
                experiment_total_limit=args.experiment_total_limit,
                experiment_family_limit=args.experiment_family_limit,
                max_plan_variants=args.max_plan_variants,
                max_children_per_parent=args.max_children_per_parent,
                max_generation_depth=args.max_generation_depth,
                max_experiments_per_day=args.max_experiments_per_day,
                mutation_enabled=not args.disable_mutations,
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
        _validate_cli_node_resource_paths(args)
        with AxiomStore(args.db) as store:
            node = ResearchNode(
                NodeConfig(
                    db_path=args.db,
                    lock_path=args.lock,
                    pid_path=args.pid,
                    log_path=args.log,
                    execution_profile=ISOLATED_EXECUTION_PROFILE if isolated else None,
                ),
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
            validation = validate_hermes_proposal(proposal, store=store)
            if not validation.accepted:
                print(json.dumps(validation.as_record(), sort_keys=True, indent=2, default=str))
                return 1
            item = DurableResearchBus(store).submit_hypothesis(
                validation.normalized or {},
                dedupe_key=validation.proposal_id,
            )
            payload = {"validation": validation.as_record(), "queue_item": item.as_record(), "live_execution": False}
        print(json.dumps(payload, sort_keys=True, indent=2, default=str))
        return 0
    if args.command == "operator":
        dashboard_store = AxiomStore(args.db)
        control = OperatorControlPlane(
            dashboard_store,
            db_path=args.db,
            hermes_job_id=args.hermes_job_id,
        )
        if dashboard_store.get_operator_config("hermes_research_job_id", None) is None:
            control.configure_hermes_job_id(control.hermes_job_id)
        server: DashboardServer | None = None
        try:
            node_status = control.ensure_node()
            server = DashboardServer(
                "127.0.0.1",
                operator_port,
                data=DashboardData(store=dashboard_store, control=control),
            )
            server.start()
            print(f"Axiom operator: {server.url} · node {node_status.get('status')}")
            if args.open_browser and server.url:
                webbrowser.open(server.url)
            if args.once:
                return 0
            server.serve_forever()
        except KeyboardInterrupt:
            return 0
        finally:
            if server is not None:
                server.stop()
            dashboard_store.close()
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




def main(argv: Sequence[str] | None = None) -> int:
    try:
        return _main_impl(argv)
    except SQLiteBusyTimeout as exc:
        print(
            json.dumps(
                {
                    "ready": False,
                    "code": exc.code,
                    "message": exc.friendly_message,
                    "live_execution": False,
                },
                sort_keys=True,
                indent=2,
            )
        )
        return 1

if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["synthetic_bars", "run_synthetic_research", "build_parser", "main"]
