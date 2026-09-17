from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import tempfile
from pathlib import Path
from collections.abc import Mapping
import unittest

from axiom.canary import CanaryService
from axiom.canary_settings import CanarySettingsService
from axiom.domain import parse_timestamp
from axiom.forward import (
    ForwardTestRegistry,
    _content_hash,
)
from axiom.node import NodeConfig, ResearchNode
from axiom.lifecycle import CandidateStage
from axiom.shadow import ShadowAssessmentError, ShadowAssessmentService
from axiom.storage import AxiomStore
from axiom.strategy.signals import evaluate_signal_evaluation
from axiom.venue_feasibility import (
    FEASIBLE,
    INFEASIBLE,
    UNKNOWN,
    assess_venue_feasibility,
)
from axiom.polymarket_rules import (
    POLYMARKET_RULES_DOCS_VERSION,
    DepthAssessment,
    PolymarketRuleError,
    UNSUITABLE,
    assess_selected_token_depth,
    parse_polymarket_rules,
)


UTC = timezone.utc
T0 = datetime(2026, 1, 2, 12, 0, tzinfo=UTC)
MARKET_ID = "market-shadow-1"
CONDITION_ID = "condition-shadow-1"
YES_TOKEN = "token-shadow-yes"
NO_TOKEN = "token-shadow-no"


ENTRY_PREDICATE = {
    "version": "absolute-move-v1",
    "minimum_move": 0.05,
    "units": "probability",
    "boundary": "inclusive",
}


def directional_strategy(family: str) -> dict[str, object]:
    return {
        "version": 1,
        "market_type": "prediction",
        "family": family,
        "parameters": {
            "lookback": 1,
            "threshold": 0.05,
            "entry_predicate": dict(ENTRY_PREDICATE),
        },
        "probability_model": "not_required_for_directional_setup",
        "resolution_aware": True,
        "resolution_inputs": ["settlement"],
    }


def directional_observations(previous: float, current: float) -> dict[str, object]:
    return {
        "market_id": MARKET_ID,
        "observations": [
            {"market_id": MARKET_ID, "timestamp": T0.isoformat(), "yes_mid": previous},
            {
                "market_id": MARKET_ID,
                "timestamp": (T0 + timedelta(minutes=1)).isoformat(),
                "yes_mid": current,
            },
        ],
    }


def _thaw(value: object) -> object:
    if isinstance(value, Mapping):
        return {str(key): _thaw(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_thaw(item) for item in value]
    return value


def _scope() -> dict[str, object]:
    return {
        "schema_version": "1",
        "mode": "EXACT_MARKETS",
        "instrument": "POLYMARKET",
        "categories": [],
        "market_ids": [MARKET_ID],
        "filters": {},
        "regime_restrictions": {},
        "provenance": "canonical",
    }


def _candidate_payload(store: AxiomStore, candidate_id: str, family: str) -> dict[str, object]:
    strategy = directional_strategy(family)
    model = {"probability": 0.50}
    config: dict[str, object] = {
        "candidate_id": candidate_id,
        "market_scope": _scope(),
        "exit_policy": {"type": "fixed_holding_period", "holding_period": 1},
        "shadow_assessment": True,
        "paper_assumptions_explicit": True,
        "paper_assumptions": {
            "version": "paper-assumptions-v1",
            "currency": "USD",
            "sizing": {
                "model": "fixed_allocated_capital",
                "allocated_capital": "0.50",
            },
            "fees": {"model": "proportional", "fee_bps": "10"},
            "slippage": {"model": "proportional", "slippage_bps": "5"},
        },
    }
    risk_limits = {
        "max_order_notional": 1.0,
        "max_account_exposure": 5.0,
        "max_loss": 2.0,
    }
    spec = ForwardTestRegistry(store).freeze(
        strategy=strategy,
        model=model,
        config=config,
        start_timestamp=T0,
        bankroll=1.0,
        allowed_markets=(MARKET_ID,),
        risk_limits=risk_limits,
        experiment_id=f"candidate-forward-{candidate_id}",
    )
    frozen_config = _thaw(spec.config)
    assert isinstance(frozen_config, dict)
    setup = frozen_config["operational_setup"]
    assert isinstance(setup, dict)
    scope = frozen_config["market_scope"]
    assert isinstance(scope, dict)
    config_hash = _content_hash({"config": frozen_config, "risk_limits": risk_limits})
    frozen_hash = hashlib.sha256(
        "|".join((spec.strategy_hash, spec.model_hash, config_hash)).encode()
    ).hexdigest()
    return {
        "candidate_id": candidate_id,
        "strategy": strategy,
        "strategy_hash": spec.strategy_hash,
        "model": model,
        "model_hash": spec.model_hash,
        "config": frozen_config,
        "forward_config": frozen_config,
        "config_hash": config_hash,
        "frozen_hash": frozen_hash,
        "operational_setup": deepcopy(setup),
        "operational_setup_hash": frozen_config["operational_setup_hash"],
        "setup_id": setup["setup_id"],
        "market_scope": deepcopy(scope),
        "market_scope_hash": frozen_config["market_scope_hash"],
        "market_scope_version": frozen_config["market_scope_version"],
        "exit_policy": dict(frozen_config["exit_policy"]),
        "cost_provenance": deepcopy(frozen_config["paper_assumptions"]),
        "risk_limits": risk_limits,
        "rejection_reason": "historical_candidate_not_admitted_to_live_execution",
        "paper_only": True,
        "synthetic_fixture": True,
    }


class _FeasibilityAdapter:
    """Deterministic read-only venue fixture; it has no credential or submit path."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, object]] = []
        self.danger_calls: list[str] = []
        self.market_payload: dict[str, object] = {
            "market_id": MARKET_ID,
            "condition_id": CONDITION_ID,
            "question": "Will the synthetic public fixture resolve YES?",
            "slug": "shadow-feasibility",
            "provider_timestamp": T0.isoformat(),
            "active": True,
            "closed": False,
            "accepting_orders": True,
            "enable_order_book": True,
            "yes_token_id": YES_TOKEN,
            "no_token_id": NO_TOKEN,
        }
        self.metadata_payload: dict[str, object] = {
            "market_id": MARKET_ID,
            "condition_id": CONDITION_ID,
            "provider_timestamp": T0.isoformat(),
            "min_order_size": "1",
            "tick_size": "0.01",
            "neg_risk": False,
        }
        self.books: dict[str, dict[str, object]] = {
            "yes": self._book(YES_TOKEN, asks=(("0.40", "5"),), bids=(("0.35", "5"),)),
            "no": self._book(NO_TOKEN, asks=(("0.60", "5"),), bids=(("0.55", "5"),)),
        }

    @staticmethod
    def _book(
        token_id: str,
        *,
        asks: tuple[tuple[str, str], ...],
        bids: tuple[tuple[str, str], ...],
    ) -> dict[str, object]:
        return {
            "token_id": token_id,
            "condition_id": CONDITION_ID,
            "provider_timestamp": T0.isoformat(),
            "available": True,
            "asks": [[price, size] for price, size in asks],
            "bids": [[price, size] for price, size in bids],
        }

    def market(self, market_id: str) -> dict[str, object]:
        self.calls.append(("market", market_id))
        return deepcopy(self.market_payload)

    def token_ids(self, market_id: str) -> dict[str, str]:
        self.calls.append(("token_ids", market_id))
        return {"yes": YES_TOKEN, "no": NO_TOKEN}

    def metadata(self, market_id: str) -> dict[str, object]:
        self.calls.append(("metadata", market_id))
        return deepcopy(self.metadata_payload)

    def order_books(self, market_id: str, *, depth: int) -> dict[str, object]:
        self.calls.append(("order_books", depth))
        return deepcopy(self.books)

    def submit_order(self, **_: object) -> None:
        self.danger_calls.append("submit_order")
        raise AssertionError("feasibility assessment must never submit")

    def sign_order(self, **_: object) -> None:
        self.danger_calls.append("sign_order")
        raise AssertionError("feasibility assessment must never sign")

    def get_credentials(self) -> None:
        self.danger_calls.append("get_credentials")
        raise AssertionError("feasibility assessment must never read credentials")


class _SyntheticShadowProvider:
    """Software-only fixture standing in for deterministic recorded public rows.

    ``synthetic_fixture`` labels this object as a fixture; rows intentionally do
    not claim public provenance.  The shadow service rewrites accepted rows to
    ``FORWARD_COLLECTED`` and ``paper_only`` rather than presenting fixture data
    as a production result.
    """

    synthetic_fixture = True

    def __init__(self) -> None:
        self.rows = self._rows()
        self.calls: list[str] = []

    @staticmethod
    def _book(token_id: str, timestamp: datetime, *, ask: str, bid: str) -> dict[str, object]:
        return {
            "token_id": token_id,
            "condition_id": CONDITION_ID,
            "timestamp": timestamp.isoformat(),
            "available": True,
            "asks": [[ask, "1"]],
            "bids": [[bid, "1"]],
        }

    @classmethod
    def _rows(cls) -> list[dict[str, object]]:
        rows: list[dict[str, object]] = []
        for index, midpoint in enumerate((0.50, 0.55, 0.50)):
            stamp = T0 + timedelta(minutes=index)
            rows.append(
                {
                    "market_id": MARKET_ID,
                    "condition_id": CONDITION_ID,
                    "timestamp": stamp.isoformat(),
                    "source_timestamp": stamp.isoformat(),
                    "source_snapshot_id": f"snapshot-{index}",
                    "source_type": "CURRENT",
                    "yes_mid": midpoint,
                    "no_mid": 1.0 - midpoint,
                    "settlement": "open",
                    "yes_order_book": cls._book(
                        YES_TOKEN,
                        stamp,
                        ask="0.55" if index == 1 else "0.50",
                        bid="0.545" if index == 1 else "0.495",
                    ),
                    "no_order_book": cls._book(
                        NO_TOKEN,
                        stamp,
                        ask="0.45" if index == 1 else ("0.46" if index == 2 else "0.50"),
                        bid="0.445" if index == 1 else "0.455",
                    ),
                }
            )
        return rows

    def markets(self, *, active: bool = True) -> list[dict[str, object]]:
        self.calls.append("markets")
        return [
            {
                "market_id": MARKET_ID,
                "condition_id": CONDITION_ID,
                "yes_token_id": YES_TOKEN,
                "no_token_id": NO_TOKEN,
                "active": active,
                "closed": False,
                "accepting_orders": True,
                "enable_order_book": True,
                "source_type": "CURRENT",
                "provider": "polymarket",
                "instrument": "POLYMARKET",
                "venue": "POLYMARKET",
                "timestamp": T0.isoformat(),
            }
        ]

    def price_history(
        self,
        market_id: str,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> list[dict[str, object]]:
        self.calls.append("price_history")
        result = []
        for row in self.rows:
            stamp = parse_timestamp(row["timestamp"])
            if stamp is None or (start is not None and stamp <= start) or (end is not None and stamp > end):
                continue
            result.append(deepcopy(row))
        return result

    def market(self, market_id: str) -> dict[str, object]:
        self.calls.append("market")
        return deepcopy(self.rows[-1])

    def order_books(self, market_id: str, *, depth: int = 20) -> dict[str, object]:
        self.calls.append("order_books")
        last = self.rows[-1]
        return {
            "yes": deepcopy(last["yes_order_book"]),
            "no": deepcopy(last["no_order_book"]),
        }


class ShadowVenueFeasibilityTests(unittest.TestCase):
    def test_directional_entry_predicate_is_inclusive_and_family_signed(self) -> None:
        for family, expected_by_move in (
            (
                "momentum",
                {("0.50", "0.55"): (1.0, "yes"), ("0.55", "0.50"): (-1.0, "no")},
            ),
            (
                "mean_reversion",
                {("0.50", "0.55"): (-1.0, "no"), ("0.55", "0.50"): (1.0, "yes")},
            ),
        ):
            for (previous, current), (score, outcome) in expected_by_move.items():
                with self.subTest(family=family, previous=previous, current=current):
                    evaluation = evaluate_signal_evaluation(
                        directional_strategy(family),
                        directional_observations(float(previous), float(current)),
                    )
                    self.assertEqual(evaluation.reason_code, "SIGNAL_PRODUCED")
                    self.assertEqual(evaluation.evidence["entry_eligible"], True)
                    self.assertEqual(evaluation.evidence["entry_predicate_boundary"], "inclusive")
                    self.assertAlmostEqual(evaluation.evidence["raw_delta"], score * 0.05)
                    self.assertEqual(evaluation.score, score)
                    self.assertEqual(evaluation.evidence["outcome"], outcome)
                    self.assertEqual(evaluation.side, "buy")

    def test_shadow_worker_keeps_rejected_pair_paper_only_and_restart_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "shadow.sqlite"
            provider = _SyntheticShadowProvider()
            with AxiomStore(str(database)) as store:
                CanarySettingsService(store, clock=lambda: T0)
                canary = CanaryService(store, clock=lambda: T0)
                canary.disarm()
                payloads = {
                    family: _candidate_payload(store, f"rejected-{family}", family)
                    for family in ("momentum", "mean_reversion")
                }
                for family, payload in payloads.items():
                    candidate_id = str(payload["candidate_id"])
                    store.save_candidate_lifecycle(
                        candidate_id,
                        "IDEA",
                        {"candidate_id": candidate_id, "family": family},
                        timestamp=T0,
                    )
                    previous = "IDEA"
                    stages = (
                        ("SCHEMA_VALIDATED", {"schema_valid": True}),
                        ("BACKTESTED", {"backtest_complete": True}),
                        ("VALIDATED", {"validation_complete": True, "holdout_used": False}),
                        ("ROBUSTNESS_CHECKED", {"robustness_passed": True, "holdout_used": False}),
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
                        ),
                    )
                    for stage, evidence in stages:
                        body = dict(payload)
                        body.update(evidence)
                        store.save_candidate_lifecycle(
                            candidate_id,
                            stage,
                            body,
                            from_stage=previous,
                            timestamp=T0,
                        )
                        previous = stage
                    store.save_candidate_lifecycle(
                        candidate_id,
                        "REJECTED",
                        payload,
                        from_stage="FROZEN",
                        reason="historical_candidate_not_admitted_to_live_execution",
                        timestamp=T0,
                    )
                service = ShadowAssessmentService(store, clock=lambda: T0, max_markets=2, max_observations=8)
                registered = service.register(
                    ("rejected-momentum", "rejected-mean_reversion"),
                    bankroll=1.0,
                    max_observations=3,
                    now=T0,
                )
                self.assertEqual(registered["status"], "REGISTERED")
                self.assertEqual(registered["manifest"]["shared"]["bankroll"], 1.0)
                self.assertEqual(
                    {member["family"] for member in registered["manifest"]["members"]},
                    {"momentum", "mean_reversion"},
                )
                self.assertTrue(all(member["paper_only"] if "paper_only" in member else True for member in registered["manifest"]["members"]))
                job_id = str(registered["job_id"])
                node = ResearchNode(
                    NodeConfig(
                        str(database),
                        worker_name="shadow-test-node",
                        shadow_enabled=True,
                        shadow_jobs_per_cycle=1,
                        paper_observations_per_candidate=8,
                        crypto_enabled=False,
                        research_enabled=False,
                        execution_profile="isolated",
                    ),
                    provider=provider,
                    store=store,
                    clock=lambda: T0,
                )
                canary.disarm()
                evidence = node._run_shadow_assessment_tick(now=T0 + timedelta(minutes=10))
                self.assertEqual(evidence["processed_jobs"], 1)
                self.assertEqual(evidence["failed_jobs"], 0)
                self.assertTrue(evidence["paper_only"])
                self.assertFalse(evidence["live_execution"])
                row = service.load(job_id)
                self.assertIsNotNone(row)
                assert row is not None
                self.assertEqual(row["status"], "COMPLETED")
                self.assertEqual(row["state"]["public_observations"], 3)
                self.assertEqual(row["manifest"]["shared"]["budget"]["bankroll_exact"], "1.0")
                self.assertEqual(
                    {member["family"] for member in row["manifest"]["members"]},
                    {"momentum", "mean_reversion"},
                )
                for candidate_id in ("rejected-momentum", "rejected-mean_reversion"):
                    self.assertEqual(store.load_candidate_lifecycle(candidate_id)["stage"], "REJECTED")
                for table in (
                    "canary_ledger",
                    "canary_execution_events",
                    "canary_signals",
                    "canary_risk_reservations",
                ):
                    count = store.connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                    self.assertEqual(count, 0, table)
                events = store.list_paper_execution_events(row["manifest"]["shared"]["run_id"], limit=None)
                self.assertTrue(any(event["payload"].get("shadow_member_id") == "momentum:rejected-momentum" for event in events))
                self.assertTrue(all(event["payload"].get("paper_only") is True for event in events))
                self.assertTrue(all(event["payload"].get("live_execution") is False for event in events))
                fills = store.load_fills(symbol=MARKET_ID)
                self.assertTrue(fills)
                cash_flows: dict[str, float] = {}
                for fill in fills:
                    member_id = str(fill.metadata.get("shadow_member_id"))
                    signed = float(fill.quantity) if fill.side.value == "sell" else -float(fill.quantity)
                    cash_flows[member_id] = cash_flows.get(member_id, 0.0) + signed * float(fill.price)
                self.assertLess(cash_flows["momentum:rejected-momentum"], 0.0)
                self.assertGreater(cash_flows["mean_reversion:rejected-mean_reversion"], 0.0)
                before_observations = store.paper_history_counts(row["manifest"]["shared"]["run_id"])["observations"]
                before_fills = len(fills)
                before_state = deepcopy(row["state"])
                node2 = ResearchNode(
                    NodeConfig(
                        str(database),
                        worker_name="shadow-test-node-restart",
                        shadow_enabled=True,
                        shadow_jobs_per_cycle=1,
                        paper_observations_per_candidate=8,
                        crypto_enabled=False,
                        research_enabled=False,
                        execution_profile="isolated",
                    ),
                    provider=provider,
                    store=store,
                    clock=lambda: T0,
                )
                restart_evidence = node2._run_shadow_assessment_tick(now=T0 + timedelta(minutes=11))
                self.assertEqual(restart_evidence["processed_jobs"], 0)
                after = service.load(job_id)
                assert after is not None
                self.assertEqual(after["state"], before_state)
                self.assertEqual(
                    store.paper_history_counts(row["manifest"]["shared"]["run_id"])["observations"],
                    before_observations,
                )
                self.assertEqual(len(store.load_fills(symbol=MARKET_ID)), before_fills)

    def test_feasibility_report_is_feasible_under_one_dollar_and_read_only(self) -> None:
        adapter = _FeasibilityAdapter()
        assessment = assess_venue_feasibility(
            adapter,
            MARKET_ID,
            target_quantity="1",
            cap_usd="0.99",
            depth=7,
            now=T0,
        )
        report = assessment.to_dict()
        self.assertEqual(assessment.verdict, FEASIBLE)
        self.assertIs(assessment["feasible"], True)
        self.assertEqual(report["market_id"], MARKET_ID)
        self.assertEqual(report["condition_id"], CONDITION_ID)
        self.assertEqual(report["token_ids"], {"yes": YES_TOKEN, "no": NO_TOKEN})
        self.assertEqual(report["tokens"], report["token_ids"])
        self.assertEqual(report["depth"], 7)
        self.assertEqual(report["cap_usd"], "0.99")
        self.assertEqual(report["rules"], {
            "min_order_quantity": "1",
            "min_order_size": "1",
            "tick_size": "0.01",
            "price_increment": "0.01",
            "neg_risk": False,
            "min_notional": "UNKNOWN",
            "size_increment": "UNKNOWN",
        })
        self.assertEqual(
            set(report["timestamps"]),
            {"checked_at", "market", "metadata", "yes_book", "no_book"},
        )
        self.assertEqual(report["buy"]["yes"]["depth_sufficient"], True)
        self.assertEqual(report["sell"]["no"]["depth_sufficient"], True)
        self.assertEqual([name for name, _ in adapter.calls], ["market", "token_ids", "metadata", "order_books"])
        self.assertEqual(adapter.calls[-1], ("order_books", 7))
        self.assertEqual(adapter.danger_calls, [])

    def test_feasibility_reports_known_infeasible_depth_and_cap(self) -> None:
        cases = (
            ("depth", {"target": "6"}, "0.99", "YES_BUY_DEPTH_INSUFFICIENT"),
            ("cap", {"target": "2"}, "0.50", ""),
        )
        for name, mutation, cap, reason_fragment in cases:
            with self.subTest(name=name):
                adapter = _FeasibilityAdapter()
                target = mutation["target"]
                result = assess_venue_feasibility(
                    adapter, MARKET_ID, target_quantity=target, cap_usd=cap, now=T0
                )
                self.assertEqual(result.verdict, INFEASIBLE)
                self.assertIs(result["feasible"], False)
                if name == "cap":
                    self.assertFalse(result["buy"]["yes"]["cap_satisfied"])
                else:
                    self.assertIn(reason_fragment, result.reasons)
                self.assertEqual(adapter.danger_calls, [])

    def test_feasibility_unknown_missing_rules_conflicts_and_invalid_flags(self) -> None:
        cases: list[tuple[str, callable, str]] = []

        def missing_rules(adapter: _FeasibilityAdapter) -> None:
            adapter.metadata_payload.pop("min_order_size")
            adapter.metadata_payload.pop("tick_size")
            adapter.metadata_payload.pop("neg_risk")

        def conflicting_alias(adapter: _FeasibilityAdapter) -> None:
            adapter.metadata_payload["orderMinSize"] = "2"

        def invalid_flags(adapter: _FeasibilityAdapter) -> None:
            adapter.market_payload["active"] = True
            adapter.market_payload["is_active"] = False

        cases.extend(
            (
                ("missing_rules", missing_rules, "MIN_ORDER_SIZE_MISSING"),
                ("conflicting_alias", conflicting_alias, "MIN_ORDER_SIZE_CONFLICT"),
                ("invalid_flags", invalid_flags, "MARKET_ACTIVE_FLAG_INVALID"),
            )
        )
        for name, mutate, reason in cases:
            with self.subTest(name=name):
                adapter = _FeasibilityAdapter()
                mutate(adapter)
                result = assess_venue_feasibility(
                    adapter, MARKET_ID, target_quantity="1", now=T0
                )
                self.assertEqual(result.verdict, UNKNOWN)
                self.assertEqual(result["feasible"], UNKNOWN)
                self.assertIn(reason, result.reasons)
                self.assertEqual(adapter.danger_calls, [])

    def test_feasibility_unknowns_identity_timestamps_and_partial_exit_depth(self) -> None:
        cases: list[tuple[str, callable, str]] = []

        def mismatched_market(adapter: _FeasibilityAdapter) -> None:
            adapter.market_payload["market_id"] = "other-market"

        def stale_market(adapter: _FeasibilityAdapter) -> None:
            adapter.market_payload["provider_timestamp"] = (T0 - timedelta(seconds=61)).isoformat()

        def future_market(adapter: _FeasibilityAdapter) -> None:
            adapter.market_payload["provider_timestamp"] = (T0 + timedelta(seconds=1)).isoformat()

        def mismatched_book_identity(adapter: _FeasibilityAdapter) -> None:
            adapter.books["no"]["condition_id"] = "other-condition"

        cases.extend(
            (
                ("market_identity", mismatched_market, "MARKET_ID_MISMATCH"),
                ("stale_timestamp", stale_market, "MARKET_STALE"),
                ("future_timestamp", future_market, "MARKET_TIMESTAMP_IN_FUTURE"),
                ("book_identity", mismatched_book_identity, "NO_BOOK_CONDITION_ID_MISMATCH"),
            )
        )
        for name, mutate, reason in cases:
            with self.subTest(name=name):
                adapter = _FeasibilityAdapter()
                mutate(adapter)
                result = assess_venue_feasibility(adapter, MARKET_ID, target_quantity="1", now=T0)
                self.assertEqual(result.verdict, UNKNOWN)
                self.assertIn(reason, result.reasons)
                self.assertEqual(adapter.danger_calls, [])

        adapter = _FeasibilityAdapter()
        adapter.books["yes"]["bids"] = [["0.35", "0.25"]]
        result = assess_venue_feasibility(adapter, MARKET_ID, target_quantity="1", now=T0)
        self.assertEqual(result.verdict, INFEASIBLE)
        self.assertFalse(result["sell"]["yes"]["depth_sufficient"])
        self.assertIn("YES_SELL_DEPTH_INSUFFICIENT", result.reasons)
        self.assertEqual(adapter.danger_calls, [])

    def test_feasibility_quantity_precision_and_fee_slippage(self) -> None:
        misaligned = _FeasibilityAdapter()
        misaligned.metadata_payload["min_order_size"] = "0.01"
        result = assess_venue_feasibility(
            misaligned,
            MARKET_ID,
            target_quantity="1.005",
            cap_usd="1.00",
            now=T0,
        )
        self.assertEqual(result.verdict, UNKNOWN)
        self.assertIn("QUANTITY_PRECISION_INVALID", result.reasons)
        self.assertEqual(misaligned.danger_calls, [])
        flooring = _FeasibilityAdapter()
        flooring.metadata_payload["min_order_size"] = "0.01"
        flooring.books["yes"] = flooring._book(
            YES_TOKEN,
            asks=(("0.50", "5"),),
            bids=(("0.40", "5"),),
        )
        flooring_result = assess_venue_feasibility(
            flooring,
            MARKET_ID,
            target_quantity="0.99",
            cap_usd="0.499",
            now=T0,
        )
        self.assertEqual(flooring_result["buy"]["yes"]["max_quantity_under_cap"], "0.99")


        charged = _FeasibilityAdapter()
        charged.metadata_payload["min_order_size"] = "0.01"
        charged.books["yes"] = charged._book(
            YES_TOKEN,
            asks=(("0.50", "5"),),
            bids=(("0.40", "5"),),
        )
        charged.books["no"] = charged._book(
            NO_TOKEN,
            asks=(("0.40", "5"),),
            bids=(("0.35", "5"),),
        )
        result = assess_venue_feasibility(
            charged,
            MARKET_ID,
            target_quantity="0.99",
            cap_usd="0.50",
            fee_bps="10",
            slippage_bps="10",
            now=T0,
        )
        self.assertEqual(result.verdict, FEASIBLE)
        yes = result["buy"]["yes"]
        self.assertEqual(yes["requested_quantity"], "0.99")
        self.assertEqual(yes["max_quantity_under_cap"], "0.99")
        self.assertEqual(yes["multilevel"]["all_in_cost"], "0.4959904950")
        self.assertEqual(yes["cap_satisfied"], True)
        self.assertEqual(result["sell"]["yes"]["depth_sufficient"], True)
        self.assertEqual(charged.danger_calls, [])

    def test_official_rule_parser_binds_documented_book_fields_only(self) -> None:
        rules = parse_polymarket_rules(
            {
                "min_order_size": "5",
                "tick_size": "0.001",
                "neg_risk": True,
                "min_notional": "999",
                "size_increment": "999",
            }
        )
        self.assertEqual(rules.min_order_size, Decimal("5"))
        self.assertEqual(rules.tick_size, Decimal("0.001"))
        self.assertTrue(rules.neg_risk)
        self.assertEqual(rules.price_precision, 3)
        self.assertEqual(rules.size_precision, 2)
        self.assertEqual(rules.amount_precision, 5)
        finer_tick = parse_polymarket_rules(
            {"min_order_size": "1", "tick_size": "0.0025", "neg_risk": False}
        )
        self.assertEqual(finer_tick.price_precision, 4)
        self.assertEqual(finer_tick.amount_precision, 6)

        self.assertEqual(rules.docs_version, POLYMARKET_RULES_DOCS_VERSION)
        with self.assertRaises(PolymarketRuleError):
            parse_polymarket_rules(
                {"min_order_size": "5", "tick_size": "0.01"}
            )

    def test_selected_token_depth_walks_levels_and_applies_price_bound(self) -> None:
        rules = parse_polymarket_rules(
            {"min_order_size": "5", "tick_size": "0.01", "neg_risk": False}
        )
        book = {
            "token_id": YES_TOKEN,
            "asks": [["0.40", "2"], ["0.42", "3"]],
            "bids": [["0.35", "5"]],
        }
        assessment = assess_selected_token_depth(
            book,
            rules,
            side="BUY",
            quantity="5",
            cap_usd="2.06",
        )
        self.assertIsInstance(assessment, DepthAssessment)
        self.assertEqual(assessment.action, "SUITABLE")
        self.assertEqual(assessment.reason, "OK")
        self.assertEqual(assessment.required_cost, Decimal("2.06"))
        self.assertEqual(assessment.filled_quantity, Decimal("5"))
        self.assertEqual(assessment.available_quantity, Decimal("5"))
        self.assertEqual(assessment.to_dict()["depth_quantity"], "5")
        self.assertEqual(assessment.levels_used, 2)

        sell = assess_selected_token_depth(
            {
                "token_id": YES_TOKEN,
                "asks": [],
                "bids": [["0.35", "2"], ["0.34", "3"]],
            },
            rules,
            side="SELL",
            quantity="5",
            venue_fee_rate="0.01",
            fee_reserve="0.25",
        )
        self.assertEqual(sell.action, "SUITABLE")
        self.assertEqual(sell.available_quantity, Decimal("5"))
        self.assertEqual(sell.gross_proceeds, Decimal("1.72"))
        self.assertEqual(sell.net_proceeds, Decimal("1.7028"))
        self.assertEqual(sell.fee_reserve, Decimal("0.25"))

        bounded = assess_selected_token_depth(
            book,
            rules,
            side="BUY",
            quantity="5",
            price_bound="0.41",
        )
        self.assertEqual(bounded.action, UNSUITABLE)
        self.assertEqual(bounded.reason, "INSUFFICIENT_DEPTH")
        self.assertEqual(bounded.filled_quantity, Decimal("2"))

    def test_selected_token_thin_and_one_sided_books_are_unsuitable_without_aliasing(self) -> None:
        rules = parse_polymarket_rules(
            {"min_order_size": "1", "tick_size": "0.01", "neg_risk": False}
        )
        thin = assess_selected_token_depth(
            {
                "token_id": YES_TOKEN,
                "asks": [["0.40", "0.25"], ["0.41", "0.50"]],
                "bids": [["0.35", "3"]],
            },
            rules,
            side="BUY",
            quantity="1",
        )
        self.assertEqual(thin.action, UNSUITABLE)
        self.assertEqual(thin.reason, "INSUFFICIENT_DEPTH")

        one_sided = assess_selected_token_depth(
            {
                "token_id": YES_TOKEN,
                "asks": [],
                # This bid cannot be used as a BUY/ask fallback.
                "bids": [["0.35", "3"]],
            },
            rules,
            side="BUY",
            quantity="1",
        )
        self.assertEqual(one_sided.action, UNSUITABLE)
        self.assertEqual(one_sided.reason, "NO_DEPTH")
        sell = assess_selected_token_depth(
            {
                "token_id": YES_TOKEN,
                "asks": [],
                "bids": [["0.35", "3"]],
            },
            rules,
            side="SELL",
            quantity="1",
        )
        self.assertEqual(sell.action, "SUITABLE")
        self.assertEqual(sell.net_proceeds, Decimal("0.35"))

    def test_selected_token_precision_min_size_cap_and_fee_reserve(self) -> None:
        rules = parse_polymarket_rules(
            {"min_order_size": "1", "tick_size": "0.01", "neg_risk": False}
        )
        book = {
            "token_id": YES_TOKEN,
            "asks": [["0.50", "5"]],
            "bids": [["0.40", "5"]],
        }
        self.assertEqual(
            assess_selected_token_depth(book, rules, side="BUY", quantity="0.99").reason,
            "MIN_ORDER_SIZE",
        )
        self.assertEqual(
            assess_selected_token_depth(book, rules, side="BUY", quantity="1.005").reason,
            "QUANTITY_PRECISION",
        )
        capped = assess_selected_token_depth(
            book,
            rules,
            side="BUY",
            quantity="2",
            cap_usd="1.01",
            venue_fee_rate="0.01",
            fee_reserve="0.01",
        )
        self.assertEqual(capped.action, UNSUITABLE)
        self.assertEqual(capped.reason, "CAP_EXCEEDED")
        self.assertEqual(capped.to_dict()["fee_reserve"], "0.01")



    def test_venue_assessment_rejects_conflicting_quantity_aliases_without_adapter_access(self) -> None:
        adapter = _FeasibilityAdapter()
        with self.assertRaises(ValueError):
            assess_venue_feasibility(
                adapter,
                MARKET_ID,
                target_quantity="1",
                quantity="2",
                now=T0,
            )
        self.assertEqual(adapter.calls, [])
        self.assertEqual(adapter.danger_calls, [])

if __name__ == "__main__":
    unittest.main()
