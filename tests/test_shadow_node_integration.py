from __future__ import annotations

import contextlib
import io
import json
import math
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch
from urllib.request import urlopen

from axiom.canary import CanaryService
from axiom.canary_settings import CanarySettingsService
from axiom.cli import _main_impl, build_parser
from axiom.dashboard import DashboardData, DashboardServer
from axiom.domain import (
    Fill,
    MarketType,
    OrderBookLevel,
    OrderBookSnapshot,
    PredictionMarketSnapshot,
    SettlementState,
    Side,
)
from axiom.forward import (
    ForwardTestRegistry,
    _canonical_forward_config,
    _content_hash,
    _operational_setup_for_strategy,
    _operational_setup_hash,
)
from axiom.node import NodeConfig, ResearchNode
from axiom.lifecycle import CandidateLifecycleManager, CandidateStage
from axiom.operator import OperatorControlPlane
from axiom.paper_engine import PaperEngineCycle
from axiom.shadow import (
    ShadowAssessmentService,
    _normalize_provider_row,
    _shadow_budget_cap,
)
from axiom.storage import AxiomStore
from axiom.strategy import load_strategy
from axiom.domain import ensure_utc
from axiom.experiment_plan import normalize_market_scope
UTC = timezone.utc
T0 = datetime(2025, 1, 1, 12, 0, tzinfo=UTC)


class _CurrentFixtureProvider:
    """Deterministic current-data seam; emitted rows carry no synthetic marker."""

    provider_name = "current-fixture"
    synthetic_fixture = True

    def __init__(self, *, start: datetime = T0) -> None:
        self.start = ensure_utc(start)
        self.calls: list[str] = []
        self._history = (
            self._price_row(self.start, 0.40, "history-0"),
            self._price_row(self.start + timedelta(minutes=1), 0.60, "history-1"),
            self._price_row(
                self.start + timedelta(minutes=3),
                0.70,
                "history-terminal",
                settlement=SettlementState.RESOLVED_YES.value,
            ),
        )
        self._book_timestamp = self.start + timedelta(minutes=2)
        self._market = self._snapshot(self._book_timestamp, 0.45)

    @staticmethod
    def _book(stamp: datetime, *, yes: bool) -> OrderBookSnapshot:
        midpoint = 0.45 if yes else 0.55
        return OrderBookSnapshot(
            timestamp=stamp,
            bids=(OrderBookLevel(price=midpoint - 0.01, size=100.0),),
            asks=(OrderBookLevel(price=midpoint + 0.01, size=100.0),),
            token_id="yes" if yes else "no",
        )

    @classmethod
    def _snapshot(cls, stamp: datetime, midpoint: float) -> PredictionMarketSnapshot:
        return PredictionMarketSnapshot(
            timestamp=stamp,
            market_id="market-1",
            question="Will the fixture event resolve yes?",
            yes_bid=midpoint - 0.01,
            yes_ask=midpoint + 0.01,
            yes_mid=midpoint,
            no_bid=1.0 - midpoint - 0.01,
            no_ask=1.0 - midpoint + 0.01,
            no_mid=1.0 - midpoint,
            volume=1000.0,
            liquidity=100.0,
            expiry=stamp + timedelta(days=1),
            settlement=SettlementState.OPEN,
            resolution_criteria="fixture resolution",
            category="fixture",
            tags=("current",),
            order_book=cls._book(stamp, yes=True),
            source="current-fixture",
            yes_token_id="yes-token",
            no_token_id="no-token",
            condition_id="condition-1",
            slug="fixture-market",
            provider_timestamp=stamp,
            active=True,
            closed=False,
            accepting_orders=True,
            enable_order_book=True,
        )

    @staticmethod
    def _price_row(
        stamp: datetime,
        midpoint: float,
        snapshot_id: str,
        *,
        settlement: str | None = None,
    ) -> dict[str, object]:
        row: dict[str, object] = {
            "market_id": "market-1",
            "timestamp": stamp.isoformat(),
            "observed_at": stamp.isoformat(),
            "source_timestamp": stamp.isoformat(),
            "source_type": "CURRENT",
            "source_snapshot_id": snapshot_id,
            "yes_mid": midpoint,
            "yes_bid": midpoint - 0.01,
            "yes_ask": midpoint + 0.01,
            "no_mid": 1.0 - midpoint,
            "no_bid": 1.0 - midpoint - 0.01,
            "no_ask": 1.0 - midpoint + 0.01,
            "active": True,
            "closed": False,
        }
        if settlement is not None:
            row["settlement"] = settlement
        return row

    def markets(self, *, active: bool = True) -> list[PredictionMarketSnapshot]:
        self.calls.append("markets")
        return [self._market]

    def market(self, market_id: str) -> PredictionMarketSnapshot:
        self.calls.append("market")
        if market_id != "market-1":
            raise KeyError(market_id)
        return self._market

    def price_history(
        self,
        market_id: str,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int | None = None,
    ) -> list[dict[str, object]]:
        self.calls.append("price_history")
        if market_id != "market-1":
            return []
        rows = list(self._history)
        if start is not None:
            start = ensure_utc(start)
            rows = [row for row in rows if ensure_utc(datetime.fromisoformat(str(row["timestamp"]))) >= start]
        if end is not None:
            end = ensure_utc(end)
            rows = [row for row in rows if ensure_utc(datetime.fromisoformat(str(row["timestamp"]))) <= end]
        return rows[:limit] if limit is not None else rows

    def order_books(self, market_id: str, *, depth: int = 20) -> dict[str, OrderBookSnapshot]:
        self.calls.append("order_books")
        if market_id != "market-1":
            return {}
        return {
            "yes": self._book(self._book_timestamp, yes=True),
            "no": self._book(self._book_timestamp, yes=False),
        }

    def trades(self, market_id: str, *, start: datetime | None = None, end: datetime | None = None, limit: int | None = None) -> list[object]:
        self.calls.append("trades")
        return []


class _InclusiveSameTimestampProvider(_CurrentFixtureProvider):
    """Inclusive history seam that can reveal a new snapshot at one boundary."""

    def __init__(self) -> None:
        super().__init__()
        self._catalog = self._market
        self._history = [self._history[0]]

    def markets(self, *, active: bool = True) -> list[PredictionMarketSnapshot]:
        self.calls.append("markets")
        return [self._catalog]

    def market(self, market_id: str) -> PredictionMarketSnapshot | None:
        self.calls.append("market")
        return None

    def add_same_timestamp_snapshot(self, snapshot_id: str, midpoint: float) -> None:
        self._history.append(
            self._price_row(self.start, midpoint, snapshot_id)
        )

class ShadowFixtureMixin:
    def _scope(self) -> tuple[dict[str, object], str, str]:
        policy = normalize_market_scope(
            {
                "schema_version": "1",
                "mode": "EXACT_MARKETS",
                "instrument": "POLYMARKET",
                "categories": [],
                "market_ids": ["market-1"],
                "filters": {},
                "regime_restrictions": {},
                "provenance": "canonical",
            }
        )
        return policy.as_dict(), policy.scope_hash, policy.scope_version

    def candidate_payload(self, store: AxiomStore, candidate_id: str, family: str) -> dict[str, object]:
        scope, scope_hash, scope_version = self._scope()
        strategy = load_strategy(
            {
                "version": 1,
                "market_type": "prediction",
                "family": family,
                "parameters": {
                    "lookback": 1,
                    "threshold": 0.05,
                    "entry_predicate": {
                        "version": "absolute-move-v1",
                        "minimum_move": 0.05,
                        "units": "probability",
                        "boundary": "inclusive",
                    },
                },
                "operations": [],
                "probability_model": "deterministic-fixture-model",
                "resolution_aware": True,
                "resolution_inputs": ["expiry", "settlement"],
                "strategy_id": candidate_id,
            }
        ).to_dict()
        model = {"version": "fixture-model-v1", "probability": 0.5}
        risk_limits = {
            "max_order_notional": 1.0,
            "max_account_exposure": 5.0,
            "max_loss": 2.0,
        }
        config = _canonical_forward_config(
            {
                "candidate_id": candidate_id,
                "market_scope": scope,
                "exit_policy": {"type": "fixed_holding_period", "holding_period": 100},
                "shadow_assessment": True,
                "paper_assumptions": {
                    "version": "paper-assumptions-v1",
                    "currency": "USD",
                    "sizing": {"model": "fixed_allocated_capital", "allocated_capital": "2"},
                    "fees": {"model": "proportional", "fee_bps": "10"},
                    "slippage": {"model": "proportional", "slippage_bps": "5"},
                },
                "paper_assumptions_explicit": True,
            }
        )
        spec = ForwardTestRegistry(store).freeze(
            strategy=strategy,
            model=model,
            config=config,
            start_timestamp=T0,
            bankroll=4.0,
            allowed_markets=("market-1",),
            risk_limits=risk_limits,
            experiment_id=f"candidate-forward-{candidate_id}",
        )
        config = spec.as_record()["config"]
        setup = config["operational_setup"]
        assert isinstance(setup, dict)
        strategy_hash = spec.strategy_hash
        model_hash = spec.model_hash
        config_hash = _content_hash({"config": config, "risk_limits": risk_limits})
        frozen_hash = __import__("hashlib").sha256(
            "|".join((strategy_hash, model_hash, config_hash)).encode("utf-8")
        ).hexdigest()
        return {
            "candidate_id": candidate_id,
            "member_id": candidate_id,
            "family": family,
            "strategy_document": strategy,
            "model_document": model,
            "strategy_hash": strategy_hash,
            "model_hash": model_hash,
            "config": config,
            "config_hash": config_hash,
            "operational_setup": setup,
            "operational_setup_hash": _operational_setup_hash(setup),
            "setup_id": setup["setup_id"],
            "market_scope": scope,
            "market_scope_hash": scope_hash,
            "market_scope_version": scope_version,
            "exit_policy": {"type": "fixed_holding_period", "holding_period": 100},
            "cost_provenance": config["paper_assumptions"],
            "risk_limits": risk_limits,
            "frozen_hash": frozen_hash,
            "rejection_reason": "historical_candidate_not_admitted_to_live_execution",
            "paper_only": True,
            "live_execution": False,
            "rejection_evidence": {"reason_code": "NEGATIVE_VALIDATION_EXPECTANCY"},
        }

    def seed_candidates(self, store: AxiomStore, *, suffix: str = "") -> tuple[str, str]:
        momentum = f"candidate-momentum{suffix}"
        reversion = f"candidate-mean-reversion{suffix}"
        for candidate_id, family in ((momentum, "momentum"), (reversion, "mean_reversion")):
            payload = self.candidate_payload(store, candidate_id, family)
            store.save_candidate_lifecycle(
                candidate_id,
                "IDEA",
                {"candidate_id": candidate_id, "family": family},
            )
            stages = (
                ("SCHEMA_VALIDATED", {"schema_valid": True}, "fixture schema validated"),
                ("BACKTESTED", {"backtest_complete": True}, "fixture backtest completed"),
                ("VALIDATED", {"validation_complete": True, "holdout_used": False}, "fixture validation completed"),
                ("ROBUSTNESS_CHECKED", {"robustness_passed": True, "holdout_used": False}, "fixture robustness completed"),
                (
                    "FROZEN",
                    {
                        "frozen": True,
                        "holdout_used": False,
                        "strategy_hash": payload["strategy_hash"],
                        "model_hash": payload["model_hash"],
                        "config_hash": payload["config_hash"],
                        "risk_snapshot": payload["risk_limits"],
                    },
                    "fixture forward configuration frozen",
                ),
            )
            previous = "IDEA"
            for stage, evidence, reason in stages:
                body = dict(payload)
                body.update(evidence)
                store.save_candidate_lifecycle(
                    candidate_id,
                    stage,
                    body,
                    from_stage=previous,
                    reason=reason,
                )
                previous = stage
            store.save_candidate_lifecycle(
                candidate_id,
                "REJECTED",
                payload,
                from_stage="FROZEN",
                reason="historical_candidate_not_admitted_to_live_execution",
            )
        return momentum, reversion
    def initialize_settings(self, store: AxiomStore, *, now: datetime = T0) -> None:
        settings = CanarySettingsService(store, clock=lambda: now)
        CanaryService(store, clock=lambda: now, settings=settings).disarm()

    def register(
        self,
        store: AxiomStore,
        candidates: tuple[str, str],
        *,
        now: datetime = T0,
        max_cycles: int = 1,
        max_observations: int = 64,
        stop_at: datetime | None = None,
    ) -> tuple[ShadowAssessmentService, dict[str, object]]:
        service = ShadowAssessmentService(store, clock=lambda: now)
        job = service.register(
            candidates,
            bankroll=4.0,
            max_cycles=max_cycles,
            max_observations=max_observations,
            stop_at=stop_at,
            now=now,
        )
        row = dict(job)
        return service, row
class NodeConfigShadowBoundsTests(unittest.TestCase):
    def test_shadow_config_bounds(self) -> None:
        with self.assertRaises(ValueError):
            NodeConfig(db_path=":memory:", shadow_enabled=True, shadow_interval=0.0)
        with self.assertRaises(ValueError):
            NodeConfig(db_path=":memory:", shadow_enabled=True, shadow_interval=math.nan)
        with self.assertRaises(ValueError):
            NodeConfig(db_path=":memory:", shadow_enabled=True, shadow_interval=86400.1)
        with self.assertRaises(ValueError):
            NodeConfig(db_path=":memory:", shadow_enabled=True, shadow_jobs_per_cycle=0)
        with self.assertRaises(ValueError):
            NodeConfig(db_path=":memory:", shadow_enabled=True, shadow_jobs_per_cycle=101)
        with self.assertRaises(ValueError):
            NodeConfig(db_path=":memory:", shadow_enabled=True, shadow_jobs_per_cycle=True)
        config = NodeConfig(
            db_path=":memory:",
            shadow_enabled=True,
            shadow_interval=0.1,
            shadow_jobs_per_cycle=2,
        )
        self.assertEqual(config.shadow_jobs_per_cycle, 2)


class ShadowServiceAndNodeIntegrationTests(ShadowFixtureMixin, unittest.TestCase):
    def test_rejected_candidates_register_and_shadow_tick_are_paper_only(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            db_path = str(Path(directory) / "shadow.sqlite")
            with AxiomStore(db_path) as store:
                self.initialize_settings(store)
                candidates = self.seed_candidates(store)
                service, row = self.register(store, candidates, max_cycles=1)
                self.assertEqual(
                    [
                        (item["family"], item["candidate_id"])
                        for item in row["manifest"]["members"]
                    ],
                    [
                        ("momentum", candidates[0]),
                        ("mean_reversion", candidates[1]),
                    ],
                )
                self.assertEqual(row["manifest"]["shared"]["bankroll"], 4.0)
                self.assertFalse(row["manifest"]["shared"]["spec"].get("live_execution", False))
                result = service.tick(row["job_id"], _CurrentFixtureProvider(), now=T0 + timedelta(minutes=3))
                self.assertEqual(result["status"], "COMPLETED")
                self.assertEqual(result["state"]["cycles"], 1)
                self.assertGreater(result["state"]["public_observations"], 0)
                self.assertEqual(
                    result["state"]["member_observations"],
                    result["state"]["public_observations"] * 2,
                )
                self.assertEqual(CanaryService(store, clock=lambda: T0, initialize=False).status_report()["control"]["state"], "DISARMED")
                events = store.list_paper_execution_events(row["manifest"]["shared"]["run_id"], limit=1000)
                outcomes = set()
                for event in events:
                    payload = event.get("payload")
                    if not isinstance(payload, dict):
                        continue
                    sources = [payload]
                    evaluation = payload.get("evaluation")
                    if isinstance(evaluation, dict):
                        sources.append(evaluation)
                        if isinstance(evaluation.get("evidence"), dict):
                            sources.append(evaluation["evidence"])
                    for key in ("evaluation_evidence", "fill"):
                        value = payload.get(key)
                        if isinstance(value, dict):
                            sources.append(value)
                            if isinstance(value.get("metadata"), dict):
                                sources.append(value["metadata"])
                    for source in sources:
                        outcome = str(source.get("outcome", "")).strip().lower()
                        if outcome in {"yes", "no"}:
                            outcomes.add(outcome)
                self.assertTrue({"yes", "no"}.issubset(outcomes))
                self.assertEqual(
                    store.load_candidate_lifecycle(candidates[0])["stage"],
                    "REJECTED",
                )
                self.assertEqual(
                    store.load_candidate_lifecycle(candidates[1])["stage"],
                    "REJECTED",
                )
                self.assertFalse(result["manifest"]["shared"]["spec"].get("live_execution", False))

    def test_non_rejected_historical_candidate_is_rejected_by_registration(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with AxiomStore(str(Path(directory) / "shadow.sqlite")) as store:
                self.initialize_settings(store)
                candidates = self.seed_candidates(store)
                store.save_candidate_lifecycle(
                    "historical-only",
                    "IDEA",
                    {"candidate_id": "historical-only", "family": "momentum"},
                )
                store.save_candidate_lifecycle(
                    "historical-only",
                    "FROZEN",
                    self.candidate_payload(store, "historical-only", "momentum"),
                )
                with self.assertRaises(Exception):
                    ShadowAssessmentService(store, clock=lambda: T0).register(
                        ("historical-only", candidates[1]), bankroll=4.0, max_cycles=1
                    )
                self.assertEqual(store.load_candidate_lifecycle("historical-only")["stage"], "FROZEN")

    def test_node_due_worker_persists_progress_and_ordinary_paper_worker_skips_shadow(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            db_path = str(Path(directory) / "shadow-node.sqlite")
            now = T0 + timedelta(minutes=3)
            with AxiomStore(db_path) as store:
                self.initialize_settings(store)
                candidates = self.seed_candidates(store, suffix="-node")
                service, row = self.register(store, candidates, now=T0, max_cycles=1)
                node = ResearchNode(
                    NodeConfig(
                        db_path=db_path,
                        interval_seconds=0.01,
                        max_markets=1,
                        research_enabled=False,
                        mutation_enabled=False,
                        crypto_enabled=False,
                        shadow_enabled=True,
                        shadow_interval=0.01,
                        shadow_jobs_per_cycle=1,
                    ),
                    provider=_CurrentFixtureProvider(),
                    store=store,
                    clock=lambda: now,
                )
                # Exercise the bounded ticks directly so progress assertions
                # do not depend on a scheduler thread getting a time slice.
                node._run_shadow_assessment_tick(now=now)
                node._run_paper_workers()
                updated = store.load_shadow_job(row["job_id"])
                self.assertIsNotNone(updated)
                self.assertEqual(updated["status"], "COMPLETED")
                self.assertEqual(updated["state"]["cycles"], 1)
                shadow_worker = store.get_worker_state("shadow-assessment")
                self.assertIsNotNone(shadow_worker)
                self.assertGreaterEqual(shadow_worker["payload"].get("processed_jobs", 0), 1)
                self.assertTrue(shadow_worker["payload"].get("next_evaluation_at"))
                shadow_scheduler = store.get_scheduler_state("shadow-assessment")
                self.assertIsNotNone(shadow_scheduler)
                self.assertGreaterEqual(shadow_scheduler.get("processed_jobs", 0), 1)
                self.assertGreaterEqual(shadow_scheduler.get("completed_jobs", 0), 1)
                self.assertTrue(shadow_scheduler.get("next_evaluation_at"))
                paper_worker = store.get_worker_state("paper-engine")
                self.assertIsNotNone(paper_worker)
                self.assertEqual(paper_worker["payload"].get("candidate_count"), 0)
                self.assertEqual(paper_worker["payload"].get("processed_candidates"), 0)
                root = store.get_worker_state("axiom-node")
                self.assertIsNotNone(root)
                self.assertIn("shadow_assessment", root["payload"])
                self.assertGreaterEqual(root["payload"]["shadow_assessment"].get("processed_jobs", 0), 1)
                paper_scheduler = store.get_scheduler_state("paper-engine") or {}
                self.assertEqual(paper_scheduler.get("candidate_count"), 0)

    def test_restart_resumes_existing_row_without_duplicate_or_wallet_reset(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            db_path = str(Path(directory) / "shadow-restart.sqlite")
            with AxiomStore(db_path) as store:
                self.initialize_settings(store)
                candidates = self.seed_candidates(store, suffix="-restart")
                service, row = self.register(store, candidates, max_cycles=1)
                first = service.tick(row["job_id"], _CurrentFixtureProvider(), now=T0 + timedelta(minutes=3))
                events_before = len(
                    store.list_paper_execution_events(
                        row["manifest"]["shared"]["run_id"], limit=1000
                    )
                )
                paper_before = store.load_paper_state(row["manifest"]["shared"]["run_id"])
                self.assertIsNotNone(paper_before)
                wallet_before = paper_before["state"].get("portfolio", {})
                version_before = first["version"]
            with AxiomStore(db_path) as restarted:
                self.initialize_settings(restarted)
                self.assertEqual(
                    CanaryService(
                        restarted,
                        clock=lambda: T0 + timedelta(minutes=3),
                        initialize=False,
                    ).status_report()["control"]["state"],
                    "DISARMED",
                )
                service_again = ShadowAssessmentService(
                    restarted, clock=lambda: T0 + timedelta(minutes=3)
                )
                same_row = service_again.register(
                    candidates,
                    bankroll=4.0,
                    max_cycles=1,
                    max_observations=64,
                    now=T0 + timedelta(minutes=3),
                )
                same_job = same_row["job_id"]
                self.assertEqual(same_job, row["job_id"])
                self.assertEqual(len(restarted.list_shadow_jobs()), 1)
                resumed = service_again.tick(
                    same_job, _CurrentFixtureProvider(), now=T0 + timedelta(minutes=3)
                )
                self.assertEqual(resumed["status"], "COMPLETED")
                self.assertEqual(resumed["version"], version_before)
                paper_after = restarted.load_paper_state(row["manifest"]["shared"]["run_id"])
                self.assertIsNotNone(paper_after)
                assert paper_after is not None
                self.assertEqual(paper_after["state"].get("portfolio", {}), wallet_before)
                events_after = len(
                    restarted.list_paper_execution_events(
                        row["manifest"]["shared"]["run_id"], limit=1000
                    )
                )
                self.assertEqual(events_after, events_before)
                self.assertEqual(restarted.load_shadow_job(same_job)["state"]["cycles"], 1)

    def test_inclusive_history_repeated_ticks_do_not_duplicate_durable_work(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with AxiomStore(str(Path(directory) / "shadow-inclusive.sqlite")) as store:
                self.initialize_settings(store)
                candidates = self.seed_candidates(store, suffix="-inclusive")
                service, row = self.register(store, candidates, max_cycles=3)
                provider = _CurrentFixtureProvider()
                first = service.tick(
                    row["job_id"],
                    provider,
                    now=T0 + timedelta(minutes=3),
                )
                run_id = row["manifest"]["shared"]["run_id"]
                observations_before = store.list_paper_observations(run_id, limit=None)
                events_before = store.list_paper_execution_events(run_id, limit=None)
                fills_before = store.list_paper_fills(
                    run_id,
                    strategy_id=row["manifest"]["shared"]["spec"]["strategy_hash"],
                    limit=100,
                )
                stats_before = json.loads(json.dumps(first["state"]["members"]))

                second = service.tick(
                    row["job_id"],
                    provider,
                    now=T0 + timedelta(minutes=3),
                )
                third = service.tick(
                    row["job_id"],
                    provider,
                    now=T0 + timedelta(minutes=3),
                )

                self.assertEqual(second["state"]["cycles"], 1)
                self.assertEqual(third["state"]["cycles"], 1)
                self.assertEqual(
                    store.list_paper_observations(run_id, limit=None),
                    observations_before,
                )
                self.assertEqual(
                    store.list_paper_execution_events(run_id, limit=None),
                    events_before,
                )
                self.assertEqual(
                    store.list_paper_fills(
                        run_id,
                        strategy_id=row["manifest"]["shared"]["spec"]["strategy_hash"],
                        limit=100,
                    ),
                    fills_before,
                )
                self.assertEqual(third["state"]["members"], stats_before)

    def test_unseen_same_timestamp_snapshot_gets_one_new_group(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with AxiomStore(str(Path(directory) / "shadow-same-timestamp.sqlite")) as store:
                self.initialize_settings(store)
                candidates = self.seed_candidates(store, suffix="-same-timestamp")
                service, row = self.register(store, candidates, max_cycles=2)
                provider = _InclusiveSameTimestampProvider()

                first = service.tick(
                    row["job_id"],
                    provider,
                    now=T0 + timedelta(minutes=3),
                )
                run_id = row["manifest"]["shared"]["run_id"]
                self.assertEqual(first["state"]["cycles"], 1)
                self.assertEqual(
                    len(store.list_paper_observations(run_id, limit=None)),
                    2,
                )
                provider.add_same_timestamp_snapshot("history-new", 0.55)

                second = service.tick(
                    row["job_id"],
                    provider,
                    now=T0 + timedelta(minutes=3),
                )
                observations = store.list_paper_observations(run_id, limit=None)
                self.assertEqual(second["status"], "COMPLETED")
                self.assertEqual(second["state"]["cycles"], 2)
                self.assertEqual(len(observations), 4)
                self.assertEqual(
                    {
                        item["payload"]["shadow_group_id"]
                        for item in observations
                    },
                    {
                        f"{row['job_id']}:cycle:1",
                        f"{row['job_id']}:cycle:2",
                    },
                )
                self.assertEqual(
                    second["state"]["source_cursor_by_market"]["market-1"]["snapshot_id"],
                    "history-new",
                )
                third = service.tick(
                    row["job_id"],
                    None,
                    now=T0 + timedelta(minutes=3),
                )
                self.assertEqual(
                    len(store.list_paper_observations(run_id, limit=None)),
                    4,
                )
                self.assertEqual(third["state"]["cycles"], 2)

    def test_crash_after_paper_commit_reconciles_without_duplicate_execution(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            db_path = str(Path(directory) / "shadow-reconcile.sqlite")
            with AxiomStore(db_path) as store:
                self.initialize_settings(store)
                candidates = self.seed_candidates(store, suffix="-reconcile")
                service, row = self.register(
                    store,
                    candidates,
                    max_cycles=1,
                    max_observations=1,
                )
                original_update = store.update_shadow_job
                failed = True

                def fail_after_commit(*args: object, **kwargs: object) -> object:
                    nonlocal failed
                    if failed and args and args[0] == row["job_id"]:
                        failed = False
                        raise RuntimeError("simulated shadow CAS crash")
                    return original_update(*args, **kwargs)

                store.update_shadow_job = fail_after_commit  # type: ignore[method-assign]
                with self.assertRaisesRegex(RuntimeError, "simulated shadow CAS crash"):
                    service.tick(
                        row["job_id"],
                        _CurrentFixtureProvider(),
                        now=T0 + timedelta(minutes=3),
                    )
                run_id = row["manifest"]["shared"]["run_id"]
                committed = store.load_paper_state(run_id)
                self.assertIsNotNone(committed)
                events_before = len(store.list_paper_execution_events(run_id, limit=1000))
                observations_before = len(
                    store.list_latest_paper_observations(run_id, per_market_limit=1000)
                )

                resumed = service.tick(
                    row["job_id"],
                    _CurrentFixtureProvider(),
                    now=T0 + timedelta(minutes=3),
                )
                self.assertEqual(resumed["status"], "COMPLETED")
                self.assertEqual(resumed["state"]["cycles"], 1)
                self.assertEqual(
                    len(store.list_paper_execution_events(run_id, limit=1000)),
                    events_before,
                )
                self.assertEqual(
                    len(store.list_latest_paper_observations(run_id, per_market_limit=1000)),
                    observations_before,
                )


    def test_bankroll_cannot_exceed_frozen_member_allocation_ceiling(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with AxiomStore(str(Path(directory) / "shadow-budget.sqlite")) as store:
                self.initialize_settings(store)
                candidates = self.seed_candidates(store, suffix="-budget")
                service = ShadowAssessmentService(store, clock=lambda: T0)
                with self.assertRaisesRegex(
                    ValueError, "bankroll exceeds CURRENT shared risk cap"
                ):
                    service.register(
                        candidates,
                        bankroll=4.01,
                        max_cycles=1,
                        max_observations=1,
                        now=T0,
                    )

    def test_declared_shared_budget_is_an_actual_derived_bankroll_fence(self) -> None:
        cap, sources = _shadow_budget_cap(
            {
                "effective_limits": {
                    "max_aggregate_open_cost_usd": "5.00",
                    "max_aggregate_exposure_usd": "5.00",
                    "shared_budget": "3.25",
                }
            },
            ({"cost_provenance": {"shared_cap": "2.00"}},),
        )
        self.assertEqual(format(cap, "f"), "2.00")
        self.assertIn({"name": "effective_limits.shared_budget", "value": "3.25"}, sources)
        self.assertIn(
            {"name": "members[0].cost_provenance.shared_cap", "value": "2.00"},
            sources,
        )

    def test_completed_row_reconciles_only_bounded_matching_run_fills(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with AxiomStore(str(Path(directory) / "shadow-terminal-reconcile.sqlite")) as store:
                self.initialize_settings(store)
                candidates = self.seed_candidates(store, suffix="-terminal-reconcile")
                service, row = self.register(store, candidates, max_cycles=1)
                completed = service.tick(
                    row["job_id"],
                    _CurrentFixtureProvider(),
                    now=T0 + timedelta(minutes=3),
                )
                self.assertEqual(completed["status"], "COMPLETED")
                spec = completed["manifest"]["shared"]["spec"]
                run_id = spec["experiment_id"]
                for index, member in enumerate(completed["manifest"]["members"]):
                    store.save_fill(
                        Fill(
                            timestamp=T0 + timedelta(minutes=3),
                            market_type=MarketType.PREDICTION,
                            symbol="market-1",
                            side=Side.BUY,
                            quantity=0.25,
                            price=0.4,
                            fees=0.0,
                            slippage=0.0,
                            strategy_id=spec["strategy_hash"],
                            order_id=f"durable-terminal-{index}",
                            market_id="market-1",
                            metadata={
                                "paper_experiment_id": run_id,
                                "shadow_job_id": row["job_id"],
                                "shadow_member_id": member["shadow_member_id"],
                                "shadow_assessment": True,
                                "paper_only": True,
                                "live_execution": False,
                            },
                        ),
                        fill_id=f"paper-fill-{run_id}-durable-terminal-{index}",
                    )
                damaged = json.loads(json.dumps(completed["state"]))
                for stats in damaged["members"].values():
                    stats["fills"] = 0
                    stats["exits"] = 0
                    stats["accounting"] = {
                        "buy_fills": 0,
                        "sell_fills": 0,
                        "filled_quantity": 0.0,
                        "fees": 0.0,
                    }
                store.update_shadow_job(
                    row["job_id"],
                    int(completed["version"]),
                    damaged,
                    "COMPLETED",
                    None,
                    T0 + timedelta(minutes=3),
                )
                reconciled = service.tick(row["job_id"], None, now=T0 + timedelta(minutes=3))
                self.assertEqual(reconciled["status"], "COMPLETED")
                self.assertTrue(all(stats["fills"] > 0 for stats in reconciled["state"]["members"].values()))
                self.assertTrue(
                    all(stats["accounting"]["buy_fills"] > 0 for stats in reconciled["state"]["members"].values())
                )

    def test_aggregate_group_identities_are_not_evicted_after_256(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with AxiomStore(str(Path(directory) / "shadow-groups.sqlite")) as store:
                service = ShadowAssessmentService(store, clock=lambda: T0)
                state: dict[str, object] = {
                    "aggregate_groups": [],
                    "incomplete_groups": [],
                    "cursor_by_market": {},
                    "cycles": 0,
                    "public_observations": 0,
                    "member_observations": 0,
                    "members": {
                        "momentum:m": {"signals": 0, "declines": 0, "risk_rejections": 0, "fills": 0, "exits": 0, "accounting": {}},
                        "mean_reversion:r": {"signals": 0, "declines": 0, "risk_rejections": 0, "fills": 0, "exits": 0, "accounting": {}},
                    },
                }
                manifest = {
                    "job_id": "shadow-groups-job",
                    "members": [
                        {"shadow_member_id": "momentum:m"},
                        {"shadow_member_id": "mean_reversion:r"},
                    ],
                    "shared": {"spec": {"experiment_id": "shadow-groups-run"}},
                }
                for index in range(300):
                    stamp = T0 + timedelta(minutes=index + 1)
                    group_id = f"group-{index}"
                    for member_id in ("momentum:m", "mean_reversion:r"):
                        payload = {
                            "market_id": "market-1",
                            "timestamp": stamp.isoformat(),
                            "source_snapshot_id": group_id,
                            "yes_mid": 0.4,
                            "no_mid": 0.6,
                            "shadow_member_id": member_id,
                            "shadow_group_id": group_id,
                            "shadow_assessment": True,
                            "paper_only": True,
                            "live_execution": False,
                        }
                        observation_id = f"{group_id}-{member_id}"
                        store.save_paper_observation(
                            observation_id, "shadow-groups-run", "market-1", stamp, payload
                        )
                        store.save_paper_execution_event(
                            f"event-{observation_id}",
                            "shadow-groups-run",
                            observation_id,
                            "market-1",
                            stamp,
                            "SIGNAL",
                            payload,
                        )
                service._reconcile_durable(state, manifest)
                service._reconcile_durable(state, manifest)
                self.assertEqual(len(state["aggregate_groups"]), 300)
                self.assertEqual(state["members"]["momentum:m"]["signals"], 300)
                self.assertEqual(state["members"]["mean_reversion:r"]["signals"], 300)

    def test_partial_member_pair_remains_retryable_and_does_not_consume_cycle(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with AxiomStore(str(Path(directory) / "shadow-partial.sqlite")) as store:
                self.initialize_settings(store)
                candidates = self.seed_candidates(store, suffix="-partial")
                service, row = self.register(store, candidates, max_cycles=1)
                run_id = row["manifest"]["shared"]["run_id"]
                group_id = f"{row['job_id']}:cycle:1"
                member_id = row["manifest"]["members"][0]["shadow_member_id"]
                store.save_paper_observation(
                    "partial-shadow-observation",
                    run_id,
                    "market-1",
                    T0 + timedelta(minutes=1),
                    {
                        "market_id": "market-1",
                        "timestamp": (T0 + timedelta(minutes=1)).isoformat(),
                        "source_snapshot_id": "partial",
                        "yes_mid": 0.4,
                        "no_mid": 0.6,
                        "shadow_member_id": member_id,
                        "shadow_group_id": group_id,
                        "shadow_assessment": True,
                        "paper_only": True,
                        "live_execution": False,
                    },
                )
                partial_cycle = PaperEngineCycle(
                    T0, T0, 2, 1, 1, 0, 0, (), 0, None, None
                )
                with patch("axiom.shadow.ForwardPaperEngine.run", return_value=partial_cycle):
                    result = service.tick(
                        row["job_id"],
                        _CurrentFixtureProvider(),
                        now=T0 + timedelta(minutes=3),
                    )
                self.assertEqual(result["status"], "WAITING_FOR_DATA")
                self.assertEqual(result["state"]["cycles"], 0)
                self.assertEqual(result["state"]["public_observations"], 0)
                self.assertTrue(result["state"]["retryable"])
                self.assertEqual(result["state"]["last_blocker"], "SHADOW_MEMBER_PAIR_INCOMPLETE")

    def test_divergent_row_token_aliases_are_rejected(self) -> None:
        normalized = _normalize_provider_row(
            {
                "market_id": "market-1",
                "timestamp": T0.isoformat(),
                "source_type": "CURRENT",
                "yes_mid": 0.4,
                "token_ids": {"yes": "wrong-yes", "no": "no-token"},
            },
            "market-1",
            {"yes": "yes-token", "no": "no-token"},
            explicit_fixture=True,
        )
        self.assertIsNone(normalized)

    def test_node_shadow_interval_is_forwarded_to_service_schedule(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            db_path = str(Path(directory) / "shadow-interval.sqlite")
            with AxiomStore(db_path) as store:
                self.initialize_settings(store)
                candidates = self.seed_candidates(store, suffix="-interval")
                self.register(store, candidates, max_cycles=2)
                now = T0 + timedelta(minutes=3)
                node = ResearchNode(
                    NodeConfig(
                        db_path=db_path,
                        max_markets=1,
                        research_enabled=False,
                        mutation_enabled=False,
                        crypto_enabled=False,
                        shadow_enabled=True,
                        shadow_interval=0.1,
                        shadow_jobs_per_cycle=1,
                    ),
                    provider=_CurrentFixtureProvider(),
                    store=store,
                    clock=lambda: now,
                )
                node._run_shadow_assessment_tick(now=now)
                scheduler = store.get_scheduler_state("shadow-assessment")
                self.assertIsNotNone(scheduler)
                next_at = datetime.fromisoformat(str(scheduler["next_evaluation_at"]))
                self.assertEqual(next_at, now + timedelta(seconds=0.1))

    def test_cli_rules_projection_stop_idempotence_dashboard_and_operator_summary(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            db_path = str(Path(directory) / "shadow-cli.sqlite")
            with AxiomStore(db_path) as store:
                self.initialize_settings(store)
                candidates = self.seed_candidates(store, suffix="-cli")
            future = "2099-01-02T00:00:00+00:00"
            with self.assertRaises(ValueError):
                _main_impl(["shadow-register", "--db", db_path, "--candidate", candidates[0]])
            with self.assertRaises(ValueError):
                _main_impl(
                    [
                        "shadow-register",
                        "--db",
                        db_path,
                        "--candidate",
                        candidates[0],
                        "--candidate",
                        candidates[1],
                    ]
                )
            with self.assertRaises(ValueError):
                _main_impl(
                    [
                        "shadow-register",
                        "--db",
                        db_path,
                        "--candidate",
                        candidates[0],
                        "--candidate",
                        candidates[1],
                        "--stop-at",
                        future,
                        "--interval",
                        "0",
                    ]
                )
            with self.assertRaises(ValueError):
                _main_impl(["shadow-status", "--db", db_path, "--latest", "--list"])
            with self.assertRaises(ValueError):
                _main_impl(["shadow-status", "--db", db_path, "job-id", "--latest"])
            with self.assertRaises(ValueError):
                _main_impl(["shadow-stop", "--db", db_path, "--latest", "job-id"])
            with self.assertRaises(ValueError):
                _main_impl(["shadow-stop", "--db", db_path])
            parser = build_parser()
            parsed = parser.parse_args(
                [
                    "shadow-register",
                    "--db",
                    db_path,
                    "--candidate",
                    candidates[0],
                    "--candidate",
                    candidates[1],
                    "--stop-at",
                    future,
                ]
            )
            self.assertEqual(parsed.command, "shadow-register")
            self.assertEqual(parsed.candidate_ids, list(candidates))

            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                self.assertEqual(
                    _main_impl(
                        [
                            "shadow-register",
                            "--db",
                            db_path,
                            "--candidate",
                            candidates[0],
                            "--candidate",
                            candidates[1],
                            "--budget",
                            "4",
                            "--max-cycles",
                            "1",
                            "--stop-at",
                            future,
                        ]
                    ),
                    0,
                )
            registered = json.loads(output.getvalue())
            self.assertTrue(registered["registered"])
            self.assertTrue(registered["paper_only"])
            self.assertFalse(registered["live_execution"])
            self.assertTrue(registered["read_only"])
            self.assertEqual(len(registered["members"]), 2)
            job_id = registered["job_id"]
            expected_members = [
                ("momentum", candidates[0]),
                ("mean_reversion", candidates[1]),
            ]
            self.assertEqual(
                [
                    (member["family"], member["candidate_id"])
                    for member in registered["members"]
                ],
                expected_members,
            )
            self.assertEqual(registered["status"], "REGISTERED")
            self.assertEqual(registered["progress"]["cycles"], 0)
            self.assertEqual(registered["progress"]["public_observations"], 0)
            self.assertEqual(registered["progress"]["member_observations"], 0)
            self.assertEqual(registered["progress"]["cycle_limit"], 1)


            latest_status_output = io.StringIO()
            with contextlib.redirect_stdout(latest_status_output):
                self.assertEqual(
                    _main_impl(["shadow-status", "--db", db_path, "--latest"]),
                    0,
                )
            latest_status = json.loads(latest_status_output.getvalue())
            self.assertEqual(latest_status["job_id"], job_id)
            self.assertEqual(latest_status["status"], "REGISTERED")
            self.assertEqual(latest_status["progress"]["cycles"], 0)
            self.assertEqual(latest_status["progress"]["public_observations"], 0)
            self.assertEqual(latest_status["progress"]["member_observations"], 0)
            self.assertEqual(latest_status["progress"]["cycle_limit"], 1)
            self.assertEqual(
                [
                    (member["family"], member["candidate_id"])
                    for member in latest_status["members"]
                ],
                expected_members,
            )

            list_status_output = io.StringIO()
            with contextlib.redirect_stdout(list_status_output):
                self.assertEqual(
                    _main_impl(["shadow-status", "--db", db_path, "--list"]),
                    0,
                )
            listed_status = json.loads(list_status_output.getvalue())
            self.assertEqual(listed_status["status"], "LIST")
            self.assertEqual(listed_status["count"], 1)
            self.assertEqual(len(listed_status["jobs"]), 1)
            listed_job = listed_status["jobs"][0]
            self.assertEqual(listed_job["job_id"], job_id)
            self.assertEqual(listed_job["status"], "REGISTERED")
            self.assertEqual(listed_job["progress"], latest_status["progress"])
            self.assertEqual(listed_job["members"], latest_status["members"])


            with AxiomStore(db_path) as store:
                dashboard = DashboardData(store=store)
                list_payload = dashboard.v2_snapshot("shadow")
                latest_payload = dashboard.v2_snapshot("shadow/latest")
                detail_payload = dashboard.v2_snapshot(f"shadow/{job_id}")
                self.assertEqual(list_payload["items"][0]["job_id"], job_id)
                self.assertEqual(latest_payload["job_id"], job_id)
                self.assertEqual(detail_payload["job_id"], job_id)
                self.assertFalse(list_payload["live_execution"])
                dashboard_job = list_payload["items"][0]
                self.assertEqual(dashboard_job["status"], "REGISTERED")
                self.assertEqual(dashboard_job["progress"]["cycles"], 0)
                self.assertEqual(
                    dashboard_job["progress"]["public_observations"],
                    0,
                )
                self.assertEqual(
                    dashboard_job["progress"]["member_observations"],
                    0,
                )
                self.assertEqual(dashboard_job["progress"]["cycle_limit"], 1)
                self.assertEqual(
                    [
                        (member["family"], member["candidate_id"])
                        for member in dashboard_job["members"]
                    ],
                    expected_members,
                )
                operator = OperatorControlPlane(store)
                summary = operator.status()
                self.assertIn("shadow_jobs", summary)
                self.assertFalse(summary["shadow_jobs"]["live_execution"])
                self.assertTrue(summary["shadow_jobs"]["paper_only"])
                self.assertIn("shadow", summary)
                self.assertEqual(summary["shadow"]["current_job_id"], job_id)
                with DashboardServer(data=dashboard) as server:
                    base = server.url
                    assert base is not None
                    for endpoint in ("shadow", "shadow/latest", f"shadow/{job_id}"):
                        with urlopen(f"{base}/api/v2/{endpoint}", timeout=5) as response:
                            payload = json.loads(response.read().decode("utf-8"))
                        self.assertEqual(response.status, 200)
                        if endpoint == "shadow":
                            self.assertIn("items", payload)
                            self.assertEqual(payload["items"][0]["job_id"], job_id)
                        else:
                            self.assertEqual(payload["job_id"], job_id)

            first_stop = io.StringIO()
            with contextlib.redirect_stdout(first_stop):
                self.assertEqual(_main_impl(["shadow-stop", "--db", db_path, "--latest"]), 0)
            stopped = json.loads(first_stop.getvalue())
            self.assertTrue(stopped["stopped"])
            second_stop = io.StringIO()
            with contextlib.redirect_stdout(second_stop):
                self.assertEqual(_main_impl(["shadow-stop", "--db", db_path, "--latest"]), 0)
            stopped_again = json.loads(second_stop.getvalue())
            self.assertTrue(stopped_again["idempotent"])
            self.assertEqual(stopped_again["status"], "STOPPED")


if __name__ == "__main__":
    unittest.main()
