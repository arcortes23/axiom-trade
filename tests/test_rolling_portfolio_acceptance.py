from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
import hashlib
import json
import os
import sqlite3
import threading
from typing import Mapping
import tempfile
import unittest
from unittest.mock import patch

from axiom.autonomous import (
    AutonomousResearchError,
    AutonomousResearchProcessor,
    _rolling_hash,
    _rolling_overlap_key,
    _rolling_window_rows,
)
from axiom.auto_canary import AutonomousCanaryWorker
from axiom.canary import CanaryService, CredentialStore, credential_fingerprint
from axiom.canary_positions import CanaryPositionManager, list_positions
from axiom.canary_settings import CanarySettingsService
from axiom.data import InMemoryPredictionProvider
from axiom.dashboard import DashboardData
from axiom.node import NodeConfig, ResearchNode
from axiom.operator import OperatorControlError, OperatorControlPlane
from axiom.rolling_portfolio import (
    RollingAdmissionPolicy,
    RollingEvidence,
    default_rolling_admission_policy,
    evaluate_rolling_selection,
)
from axiom.backtest.prediction import (
    PRICE_PROXY_RESEARCH,
    RECORDED_BOOK_REPLAY,
    run_prediction_research_mode,
    run_prediction_research_mode as _run_prediction_research_mode,
)
from axiom.experiment_plan import normalize_market_scope
from axiom.domain import Fill, MarketType, ResolvedContract, SettlementState, Side
from axiom.portfolio import OrderRequest, Portfolio
from axiom.storage import AxiomStore


UTC = timezone.utc
NOW = datetime(2026, 1, 31, 12, tzinfo=UTC)


class AcceptanceCredentials(CredentialStore):
    def load(self, **_: object) -> dict[str, str]:
        return {
            "private_key": "acceptance-fixture",
            "wallet_address": "0x0000000000000000000000000000000000000001",
        }


class FakeVenue:
    """Scripted, local-only venue used by acceptance paths."""

    environment = "PAPER"
    origin = "paper://acceptance"

    def __init__(self, statuses: tuple[str, ...] = ()) -> None:
        self.statuses = list(statuses)
        self.submissions: list[dict[str, object]] = []

    def geoblock(self) -> dict[str, object]:
        return {"blocked": False, "close_only": False, "country": "ZZ", "region": "TEST"}

    def connectivity_check(self) -> bool:
        return True

    def account(self) -> dict[str, object]:
        return {"authenticated": True, "wallet_type": "fixture"}

    def market_context(self, market_id: str, token_id: str) -> dict[str, object]:
        return {
            "market_id": market_id,
            "token_id": token_id,
            "market_version": "v1",
            "asset_id": token_id,
            "neg_risk": False,
            "accepting_orders": True,
            "min_order_size": "0.01",
            "size_increment": "0.01",
            "min_notional": "0.001",
            "tick_size": "0.01",
            "bids": [{"price": "0.49", "size": "100"}],
            "asks": [{"price": "0.50", "size": "100"}],
            "fee_bps": "0",
        }

    def balance(self) -> Decimal:
        return Decimal("100")

    def submit_limit_order(self, **kwargs: object) -> dict[str, object]:
        self.submissions.append(dict(kwargs))
        status = (self.statuses.pop(0) if self.statuses else "FILLED").upper()
        return {
            "ok": status not in {"REJECTED", "CANCELED", "CANCELLED"},
            "order_id": f"paper-order-{len(self.submissions)}",
            "status": status,
            "fill_quantity": kwargs.get("size", "1") if status == "FILLED" else "0",
            "actual_average_price": kwargs.get("price", "0.50"),
        }
    def get_order(self, order_id: str) -> dict[str, object]:
        return {"order_id": order_id, "status": "FILLED"}

    def list_account_trades(self, **_: object) -> list[dict[str, object]]:
        return []


class RollingLifecycleVenue(FakeVenue):
    """Fake venue with exact order/trade identity for rolling reconciliation."""

    def __init__(
        self,
        statuses: tuple[str, ...] = (),
        *,
        market_id: str = "rolling-market",
    ) -> None:
        super().__init__(statuses)
        self.market_id = market_id
        self._orders: dict[str, dict[str, object]] = {}

    def submit_limit_order(self, **kwargs: object) -> dict[str, object]:
        response = super().submit_limit_order(**kwargs)
        order_id = str(response["order_id"])
        status = str(response.get("status") or "").upper()
        if status == "MALFORMED":
            response["ok"] = "yes"
            response["status"] = {"unexpected": "object"}
        if status in {"MATCHED", "FILLED", "SETTLED"}:
            response["fill_quantity"] = kwargs.get("size", "1")
        self._orders[order_id] = {
            "order_id": order_id,
            "status": status,
            "market_id": self.market_id,
            "token_id": kwargs.get("token_id", "yes"),
            "asset_id": kwargs.get("token_id", "yes"),
            "side": kwargs.get("side", "BUY"),
            "price": kwargs.get("price", "0.50"),
            "size": kwargs.get("size", "1"),
        }
        return response

    def get_order(self, order_id: str) -> dict[str, object]:
        order = dict(self._orders[order_id])
        order["fill_quantity"] = order["size"]
        order["actual_average_price"] = order["price"]
        return order

    def list_account_trades(self, *, order_id: str, **_: object) -> list[dict[str, object]]:
        order = self._orders[order_id]
        return [
            {
                "trade_id": f"trade:{order_id}",
                "order_id": order_id,
                "market_id": order["market_id"],
                "token_id": order["token_id"],
                "asset_id": order["asset_id"],
                "side": order["side"],
                "quantity": order["size"],
                "price": order["price"],
                "fee": "0",
                "status": "CONFIRMED",
                "timestamp": NOW.isoformat(),
            }
        ]


def _policy(policy_id: str = "rolling-default", **overrides: object) -> RollingAdmissionPolicy:
    values: dict[str, object] = {
        "policy_id": policy_id,
        "version": "1",
        "config_hash": f"sha256:{policy_id}",
        "requested_window_days": (7,),
        "review_interval_days": 1,
        "max_members": 3,
        "global_budget": "10.00",
        "min_actual_coverage_seconds": 6 * 86400,
        "min_completed_outcomes": 5,
        "min_reliability": "0.70",
        "min_score": "0.00",
        "replacement_margin": "0.10",
        "cooldown_seconds": 0,
        "experimental_allocation_enabled": False,
        "weights": {"return": "1", "reliability": "1", "drawdown": "1"},
    }
    values.update(overrides)
    return RollingAdmissionPolicy.from_mapping(values)

def _canonicalize_evidence(record: dict[str, object]) -> dict[str, object]:
    canonical = dict(record)
    canonical.pop("evidence_digest", None)
    canonical.pop("digest", None)
    if isinstance(canonical.get("execution_feasibility"), bool):
        canonical["execution_feasibility"] = str(canonical["execution_feasibility"])
    canonical["evidence_digest"] = RollingEvidence.from_mapping(canonical).evidence_digest
    return canonical


def _strategy(strategy_version_id: str) -> dict[str, object]:
    return {
        "strategy_version_id": strategy_version_id,
        "strategy_id": strategy_version_id,
        "version": "1",
        "strategy_hash": f"sha256:{strategy_version_id}",
        "config_hash": f"config:{strategy_version_id}",
        "candidate_id": f"candidate-{strategy_version_id}",
        "created_at": NOW.isoformat(),
        "payload": {"family": "acceptance", "parameters": {"lookback": 7}},
    }


def _trial(strategy_version_id: str, trial_id: str | None = None) -> dict[str, object]:
    return {
        "research_trial_id": trial_id or f"trial-{strategy_version_id}",
        "strategy_version_id": strategy_version_id,
        "candidate_id": f"candidate-{strategy_version_id}",
        "trial_kind": "ROLLING_RESEARCH",
        "status": "COMPLETED",
        "started_at": (NOW - timedelta(days=8)).isoformat(),
        "completed_at": (NOW - timedelta(days=1)).isoformat(),
        "terminal": True,
        "result": {"paper_only": True, "outcomes": 12},
    }




def _evidence(
    strategy_version_id: str,
    window_id: str | None = None,
    *,
    candidate_id: str | None = None,
    research_trial_id: str | None = None,
    days: int = 7,
    actual_days: int | None = None,
    net_return: str = "8.00",
    drawdown: str = "0.03",
    reliability: str = "0.90",
    source_class: str = "HISTORICAL",
    execution_feasibility: bool | str = True,
    costs: str = "0.00",
    overlap_key: str | None = None,
    **overrides: object,
) -> dict[str, object]:
    actual_days = days if actual_days is None else actual_days
    identifier = window_id or f"window-{strategy_version_id}-{days}"
    record: dict[str, object] = {
        "strategy_version_id": strategy_version_id,
        "evidence_window_id": identifier,
        "candidate_id": candidate_id or f"candidate-{strategy_version_id}",
        "research_trial_id": research_trial_id or f"trial-{strategy_version_id}",
        "available_from": (NOW - timedelta(days=actual_days)).isoformat(),
        "available_through": NOW.isoformat(),
        "requested_days": days,
        "observation_completeness": str(
            min(Decimal("1"), Decimal(actual_days) / Decimal(days))
        ),
        "actual_coverage_seconds": actual_days * 86400,
        "source_class": source_class,
        "rolling_research": True,
        "paper_sizing": "10.00",
        "fee_assumption": "0.00",
        "slippage_assumption": "0.00",
        "allocated_capital_net_return": net_return,
        "realized_pnl": net_return,
        "unrealized_pnl": "0.00",
        "fees": "0.00",
        "costs": costs,
        "drawdown": drawdown,
        "completed_outcomes": 12,
        "reliability": reliability,
        "execution_feasibility": execution_feasibility,
        "evidence_digest": "",
        "overlap_key": overlap_key or f"overlap:{strategy_version_id}",
    }
    record.update(overrides)
    return _canonicalize_evidence(record)
def _member(
    strategy_version_id: str,
    selection_id: str,
    *,
    status: str = "ACTIVE",
    allocation: str = "10.00",
    score: str = "0.80",
    evidence_window_id: str | None = None,
    evidence_digest: str | None = None,
    candidate_id: str | None = None,
    research_trial_id: str | None = None,
    overlap_key: str | None = None,
    position_management_state: dict[str, object] | None = None,
) -> dict[str, object]:
    evidence_id = evidence_window_id or f"window-{strategy_version_id}-7"
    member_candidate = (
        f"candidate-{strategy_version_id}" if candidate_id is None else candidate_id
    )
    member_trial = (
        f"trial-{strategy_version_id}"
        if research_trial_id is None
        else research_trial_id
    )
    member_overlap = overlap_key or f"overlap:{strategy_version_id}"
    member_digest = evidence_digest
    if member_digest is None:
        member_digest = str(
            _evidence(
                strategy_version_id,
                evidence_id,
                candidate_id=member_candidate,
                research_trial_id=member_trial,
                overlap_key=member_overlap,
            )["evidence_digest"]
        )
    return {
        "portfolio_selection_id": selection_id,
        "strategy_version_id": strategy_version_id,
        "candidate_id": member_candidate,
        "research_trial_id": member_trial,
        "allocation": allocation,
        "status": status,
        "score": score,
        "reason": "acceptance",
        "evidence_window_id": evidence_id,
        "evidence_digest": member_digest,
        "overlap_key": member_overlap,
        "position_management_state": position_management_state or {},
    }


def _selection(
    selection_id: str,
    policy: RollingAdmissionPolicy,
    *,
    members: list[dict[str, object]],
    selected_at: datetime = NOW,
    review_due_at: datetime | None = None,
) -> dict[str, object]:
    due = review_due_at or selected_at + timedelta(days=1)
    maintained_k = sum(
        1
        for member in members
        if Decimal(str(member.get("allocation", "0"))) > Decimal("0")
        and str(member.get("status", "")).upper() in {"ACTIVE", "REDUCE"}
    )
    return {
        "portfolio_selection_id": selection_id,
        "policy_id": policy.policy_id,
        "policy_version": policy.version,
        "risk_config_id": "risk-acceptance",
        "risk_config_generation": 1,
        "risk_config_hash": "sha256:risk-acceptance",
        "selected_at": selected_at.isoformat(),
        "review_due_at": due.isoformat(),
        "k": maintained_k,
        "global_budget": str(policy.global_budget),
        "members": members,
    }


def _selection_binding_hash(selection: Mapping[str, object]) -> str:
    """Hash the persisted selection payload without a self-referential hash."""
    payload = {
        key: value
        for key, value in selection.items()
        if key not in {"selection_hash"}
    }
    return hashlib.sha256(
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            default=str,
        ).encode("utf-8")
    ).hexdigest()


def _member_map(decision: object) -> dict[str, object]:
    members = getattr(decision, "members", None)
    if members is None:
        members = decision.as_dict().get("members", ())
    result: dict[str, object] = {}
    for member in members:
        key = member["strategy_version_id"] if isinstance(member, dict) else getattr(member, "strategy_version_id")
        result[str(key)] = member
    return result


def _value(item: object, key: str) -> object:
    return item[key] if isinstance(item, dict) else getattr(item, key)



def _raw_application_strategy(
    strategy_version_id: str = "sv-raw-integration",
    *,
    market_id: str = "market-raw-integration",
) -> dict[str, object]:
    strategy_document = {
        "version": 1,
        "strategy_id": strategy_version_id,
        "market_type": "prediction",
        "family": "probability_mispricing",
        "parameters": {"threshold": 0.05},
        "operations": [],
        "probability_model": "fixture-model-v1",
        "resolution_aware": True,
        "resolution_inputs": ["settlement"],
    }
    dataset_id = "raw-market-fixture"
    dataset_version = "v1"
    dataset_selector = {
        "dataset_id": dataset_id,
        "dataset_version": dataset_version,
        "source_type": "HISTORICAL",
    }
    market_scope = normalize_market_scope(
        {
            "schema_version": "1",
            "mode": "EXACT_MARKETS",
            "instrument": "POLYMARKET",
            "categories": [],
            "market_ids": [market_id],
            "filters": {},
            "regime_restrictions": {},
            "provenance": "canonical",
        }
    )
    dataset_provenance = {
        "dataset_id": dataset_id,
        "dataset_version": dataset_version,
        "source_type": "HISTORICAL",
        "time_split": "train-validation-holdout",
        "row_manifest": f"{dataset_id}:{dataset_version}",
    }
    dataset_attestation = {
        "dataset_id": dataset_id,
        "dataset_version": dataset_version,
        "status": "CURRENT",
        "policy_version": "v1",
        "attestation_hash": "sha256:raw-market-fixture-attestation",
    }
    return {
        "strategy_version_id": strategy_version_id,
        "strategy_id": strategy_version_id,
        "version": "1",
        "strategy_hash": f"sha256:{strategy_version_id}",
        "config_hash": f"config:{strategy_version_id}",
        "candidate_id": f"candidate-{strategy_version_id}",
        "research_trial_id": f"trial-{strategy_version_id}",
        "dataset_id": dataset_id,
        "dataset_version": dataset_version,
        "dataset_selector": dataset_selector,
        "dataset_provenance": dataset_provenance,
        "dataset_attestation": dataset_attestation,
        "market_scope": market_scope.as_dict(),
        "market_scope_hash": market_scope.scope_hash,
        "market_scope_version": market_scope.scope_version,
        "strategy_document": strategy_document,
        "model_document": {"probability": 0.80},
        "provenance": {
            "source": "rolling-raw-fixture",
            "candidate_id": f"candidate-{strategy_version_id}",
            "research_trial_id": f"trial-{strategy_version_id}",
            "dataset_id": dataset_id,
            "dataset_version": dataset_version,
            "dataset_selector": dataset_selector,
            "dataset_provenance": dataset_provenance,
            "market_scope": market_scope.as_dict(),
            "market_scope_hash": market_scope.scope_hash,
            "market_scope_version": market_scope.scope_version,
        },
        "frozen": True,
        "paper_only": True,
    }


def _raw_application_row(
    index: int,
    yes_price: float,
    *,
    market_id: str = "market-raw-integration",
    model_probability: float = 0.80,
    book: bool = False,
) -> dict[str, object]:
    timestamp = NOW - timedelta(days=7 - index)
    row: dict[str, object] = {
        "market_id": market_id,
        "timestamp": timestamp,
        "model_probability": model_probability,
        "yes_mid": yes_price,
        "yes_ask": yes_price,
        "yes_bid": yes_price,
        "no_mid": 1.0 - yes_price,
        "no_ask": 1.0 - yes_price,
        "no_bid": 1.0 - yes_price,
        "liquidity": 1000.0,
        "settlement": "open",
    }
    if book:
        row.update(
            {
                "source_type": "HISTORICAL",
                "source_timestamp": timestamp,
                "source_snapshot_id": f"raw-source-{market_id}-{index}",
                "order_book": {
                    "timestamp": timestamp.isoformat(),
                    "bids": [[yes_price, 1000.0]],
                    "asks": [[yes_price, 1000.0]],
                    "token_id": f"yes-{market_id}",
                },
                "no_order_book": {
                    "timestamp": timestamp.isoformat(),
                    "bids": [[1.0 - yes_price, 1000.0]],
                    "asks": [[1.0 - yes_price, 1000.0]],
                    "token_id": f"no-{market_id}",
                },
            }
        )
    return row

def _actual_ledger_row(position_count: int) -> dict[str, object]:
    start = NOW - timedelta(days=7)
    positions = [
        {
            "market_id": f"actual-ledger-market-{index:02d}",
            "quantity": "1",
        }
        for index in range(position_count)
    ]
    accounting: dict[str, object] = {
        "accounting_available": True,
        "accounting_complete": True,
        "accounting_partial": False,
        "initial_cash": "10.00",
        "cash": "11.00",
        "equity": "11.00",
        "realized_pnl": "1.00",
        "unrealized_pnl": "0.00",
        "net_pnl": "1.00",
        "fees": "0.00",
        "costs": "0.00",
        "allocated_capital": "10.00",
        "capital_at_risk": "10.00",
        "open_positions": positions,
        "opening_fills": 0,
        "closing_fills": 0,
        "partial_closing_fills": 0,
        "completed_round_trips": 1,
        "drawdown": "0.00",
        "reliability": "1.00",
        "_available_from": start,
        "_available_through": NOW,
    }
    return {
        "market_id": "actual-ledger-account",
        "timestamp": NOW.isoformat(),
        "_rolling_accounting": accounting,
    }
def _canonical_simulation_evaluation(
    run_id: str,
    *,
    open_positions: list[dict[str, object]] | None = None,
) -> dict[str, object]:
    accounting = dict(_actual_ledger_row(0)["_rolling_accounting"])  # type: ignore[index]
    accounting["open_positions"] = list(open_positions or ())
    accounting["accounting_available"] = True
    accounting["accounting_complete"] = True
    accounting["accounting_partial"] = False
    return {
        "evaluation_version": "rolling-evaluation:v2",
        "evaluation_run_id": run_id,
        "source_digest": f"source:{run_id}",
        "portfolio_accounting": accounting,
        "evaluation": {
            "evaluation_kind": "CANONICAL_SIMULATION",
            "evaluator_invoked": True,
            "evaluator_completed": True,
            "evaluated_observations": 3,
            "signal_count": 3,
            "diagnostic_summary_count": 0,
            "evaluator_name": "acceptance-fixture",
            "evaluator_error": None,
            "evaluator_prerequisite": None,
        },
    }


class RollingPortfolioAcceptanceTests(unittest.TestCase):
    def setUp(self) -> None:
        self._execution_profile = patch.dict(
            os.environ,
            {"AXIOM_EXECUTION_PROFILE": "production"},
        )
        self._execution_profile.start()
        self.addCleanup(self._execution_profile.stop)
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.path = Path(self._tmp.name)
    def test_operational_stage_evidence_is_namespaced_and_digest_bound(self) -> None:
        payload = _canonicalize_evidence(
            {
                **_evidence("sv-acceptance-operational"),
                "evaluation_run_id": "run-acceptance-operational",
                "evaluation_version": "rolling-evaluation:v2",
                "evaluation_kind": "CANONICAL_SIMULATION",
                "evaluator_invoked": True,
                "evaluator_completed": True,
                "operational_evidence": {
                    "valid_observations": 4,
                    "risk_approved_order_attempts": 1,
                    "net_result": "2.00",
                },
            }
        )
        evidence = RollingEvidence.from_mapping(payload)
        serialized = json.loads(json.dumps(evidence.as_dict()))
        self.assertEqual(
            serialized["operational_evidence"],
            {
                "valid_observations": 4,
                "risk_approved_order_attempts": 1,
                "net_result": "2.00",
            },
        )
        restored = RollingEvidence.from_mapping(serialized)
        self.assertEqual(restored.evidence_digest, evidence.evidence_digest)
        self.assertEqual(restored.valid_observations, 4)
        self.assertEqual(restored.net_result, Decimal("2.00"))

        conflict = dict(payload)
        conflict["metrics"] = {
            "operational_evidence": {"valid_observations": 5},
        }
        with self.assertRaisesRegex(ValueError, "valid_observations conflicts"):
            RollingEvidence.from_mapping(conflict)

    def test_settled_market_guards_normalize_whitespace_and_type_variant_ids(self) -> None:
        portfolio = Portfolio(10.0)
        portfolio.resolve(
            ResolvedContract(
                "42",
                SettlementState.RESOLVED_YES,
                NOW,
                "acceptance fixture",
            )
        )

        with self.assertRaises(ValueError):
            portfolio.submit_order(
                OrderRequest(
                    "42",
                    Side.BUY,
                    1.0,
                    MarketType.PREDICTION,
                    market_id=" 42 ",
                    outcome="yes",
                )
            )

        def settled_fill(market_id: object, order_id: str) -> Fill:
            return Fill(
                timestamp=NOW,
                market_type=MarketType.PREDICTION,
                symbol="42",
                side=Side.BUY,
                quantity=1.0,
                price=0.5,
                fees=0.0,
                slippage=0.0,
                strategy_id="acceptance",
                order_id=order_id,
                market_id=market_id,  # type: ignore[arg-type]
                metadata={"outcome": "yes"},
            )

        with self.assertRaises(ValueError):
            portfolio.apply_fill(settled_fill(" 42 ", "settled-apply"))
        with self.assertRaises(ValueError):
            portfolio.record_fill(settled_fill(42, "settled-record"))

        position = portfolio.record_fill(settled_fill("unresolved-market", "unresolved"))
        self.assertEqual(position.quantity, 1.0)
    def test_resolution_matches_normalized_prediction_market_position_ids(self) -> None:
        portfolio = Portfolio(10.0)
        request = OrderRequest(
            "yes-token",
            Side.BUY,
            1.0,
            MarketType.PREDICTION,
            market_id=" 42 ",
            outcome="yes",
        )
        fill = portfolio.execute_order(request, timestamp=NOW, price=0.40, order_id="position")
        self.assertIsNotNone(fill)
        position = portfolio.get_position("yes-token", outcome="yes")
        self.assertIsNotNone(position)
        assert position is not None
        self.assertEqual(position.market_id, "42")

        payout = portfolio.resolve(
            ResolvedContract(42, SettlementState.RESOLVED_YES, NOW, "acceptance fixture"),  # type: ignore[arg-type]
        )
        self.assertAlmostEqual(payout, 1.0)
        self.assertEqual(position.quantity, 0.0)

    def test_filled_order_id_retry_after_resolution_is_idempotent(self) -> None:
        portfolio = Portfolio(10.0)
        request = OrderRequest(
            "yes-token",
            Side.BUY,
            1.0,
            MarketType.PREDICTION,
            market_id="42",
            outcome="yes",
        )
        fill = portfolio.execute_order(request, timestamp=NOW, price=0.40, order_id="filled")
        self.assertIsNotNone(fill)
        portfolio.resolve(
            ResolvedContract(" 42 ", SettlementState.RESOLVED_YES, NOW, "acceptance fixture"),
        )

        retry = portfolio.execute_order(request, timestamp=NOW, price=0.40, order_id="filled")
        self.assertIsNone(retry)
        self.assertEqual(portfolio.orders["filled"].status, "filled")
        self.assertEqual(len(portfolio.fills), 1)

    def test_open_buy_retry_after_resolution_is_terminal_and_non_executable(self) -> None:
        portfolio = Portfolio(10.0)
        request = OrderRequest(
            "yes-token",
            Side.BUY,
            1.0,
            MarketType.PREDICTION,
            market_id="42",
            outcome="yes",
        )
        order = portfolio.submit_order(request, order_id="open")
        portfolio.resolve(
            ResolvedContract(" 42 ", SettlementState.RESOLVED_NO, NOW, "acceptance fixture"),
        )

        retry = portfolio.execute_order(request, timestamp=NOW, price=0.40, order_id="open")
        self.assertIsNone(retry)
        self.assertEqual(order.status, "cancelled")
        self.assertEqual(order.remaining_quantity, 0.0)
        self.assertEqual(portfolio.fills, [])


    def _refresh_raw_historical(
        self,
        store: AxiomStore,
        rows: list[dict[str, object]],
        *,
        strategy_version_id: str = "sv-raw-integration",
    ) -> tuple[AutonomousResearchProcessor, dict[str, object], Mapping[str, object]]:
        strategy = _raw_application_strategy(strategy_version_id)
        store.save_admission_policy(_policy().as_dict())
        processor = AutonomousResearchProcessor(store, clock=lambda: NOW)
        with patch.object(
            processor,
            "_rolling_strategy_documents",
            return_value=(strategy,),
        ), patch.object(
            processor,
            "_load_rolling_historical_dataset",
            return_value=list(rows),
        ):
            state = processor.refresh_rolling_evidence(now=NOW)
        return processor, strategy, state
    def _refresh_raw_replay(
        self,
        store: AxiomStore,
        rows: list[dict[str, object]],
        *,
        strategy_version_id: str = "sv-raw-replay",
    ) -> tuple[AutonomousResearchProcessor, dict[str, object], Mapping[str, object]]:
        strategy = _raw_application_strategy(strategy_version_id)
        store.save_admission_policy(_policy().as_dict())
        replay_rows: list[dict[str, object]] = []
        manifest: list[dict[str, object]] = []
        for index, row in enumerate(rows):
            timestamp = row.get("timestamp")
            market_id = str(row.get("market_id", "")).strip()
            if not isinstance(timestamp, datetime) or not market_id:
                raise AssertionError("replay fixture requires market and timestamp")
            snapshot_id = f"raw-replay-{market_id}-{index}"
            publisher_payload = dict(row)
            publisher_payload["research_mode"] = "RECORDED_BOOK_REPLAY"
            publisher_payload["source_type"] = "FORWARD_COLLECTED"
            store.save_polymarket_snapshot(
                snapshot_id,
                market_id,
                timestamp,
                timestamp,
                publisher_payload,
                source_type="FORWARD_COLLECTED",
            )
            source_record_hash = _rolling_hash(
                {
                    "snapshot_id": snapshot_id,
                    "market_id": market_id,
                    "source_timestamp": timestamp,
                    "observed_at": timestamp,
                    "payload": publisher_payload,
                }
            )
            replay_row = dict(row)
            replay_row.update(
                {
                    "market_id": market_id,
                    "timestamp": timestamp,
                    "source_timestamp": timestamp,
                    "observed_at": timestamp,
                    "source_type": "FORWARD_COLLECTED",
                    "source_snapshot_id": snapshot_id,
                    "source_record_hash": source_record_hash,
                    "research_mode": "RECORDED_BOOK_REPLAY",
                }
            )
            replay_rows.append(replay_row)
            manifest.append(
                {
                    "snapshot_id": snapshot_id,
                    "source_record_hash": source_record_hash,
                    "market_id": market_id,
                    "source_timestamp": timestamp,
                }
            )
        dataset_id = "Polymarket-recorded-book-replay"
        dataset_version = _rolling_hash(
            {
                "dataset_id": dataset_id,
                "research_mode": "RECORDED_BOOK_REPLAY",
                "cutoff": NOW,
                "manifest": manifest,
                "rows": replay_rows,
            }
        )
        store.save_dataset(dataset_id, dataset_version, replay_rows)
        store.save_dataset_catalog(
            dataset_id,
            dataset_version,
            provider="acceptance-fixture",
            instrument="POLYMARKET",
            market_type=MarketType.PREDICTION,
            timeframe="1d",
            start_timestamp=min(item["source_timestamp"] for item in manifest),
            end_timestamp=max(item["source_timestamp"] for item in manifest),
            row_count=len(replay_rows),
            completeness=1.0,
            missing_ranges=(),
            quality="ORDER_BOOK_SIMULATED",
            source_type="FORWARD_COLLECTED",
            snapshot_id=f"manifest:{dataset_version}",
            metadata={
                "research_mode": "RECORDED_BOOK_REPLAY",
                "exact_cutoff": NOW,
                "snapshot_manifest": manifest,
            },
            created_at=NOW,
            updated_at=NOW,
        )
        processor = AutonomousResearchProcessor(store, clock=lambda: NOW)
        with patch.object(
            processor,
            "_rolling_strategy_documents",
            return_value=(strategy,),
        ), patch.object(
            processor,
            "_load_rolling_historical_dataset",
            return_value=[],
        ):
            state = processor.refresh_rolling_evidence(now=NOW)
        return processor, strategy, state


    def test_public_refresh_routes_unbound_price_rows_to_price_proxy(self) -> None:
        raw_rows = [
            _raw_application_row(0, 0.40),
            _raw_application_row(1, 0.40),
            _raw_application_row(2, 0.60),
        ]
        with self._store("raw-price-routing.sqlite3") as store:
            with patch(
                "axiom.autonomous.run_prediction_research_mode",
                wraps=run_prediction_research_mode,
            ) as evaluator:
                _processor, strategy, state = self._refresh_raw_historical(store, raw_rows)
            historical_calls = [
                call
                for call in evaluator.call_args_list
                if call.kwargs.get("mode") == PRICE_PROXY_RESEARCH
            ]
            self.assertTrue(historical_calls)
            self.assertFalse(
                any(
                    call.kwargs.get("mode") == RECORDED_BOOK_REPLAY
                    for call in historical_calls
                )
            )
            stored = store.list_strategy_evidence_windows(
                strategy["strategy_version_id"],
                limit=64,
            )
            produced = [
                item
                for item in state["evidence_windows"]
                if item["strategy_version_id"] == strategy["strategy_version_id"]
                and item.get("evaluation_version") == "rolling-evaluation:v2"
            ]
            self.assertTrue(produced)
            evidence = next(
                item
                for item in stored
                if item["evidence_window_id"] == produced[0]["evidence_window_id"]
            )
            self.assertEqual(
                evidence["source_class"],
                produced[0]["source_class"],
            )
            self.assertEqual(
                evidence["requested_days"],
                produced[0]["requested_days"],
            )
            self.assertNotIn("strategy_version_id", raw_rows[0])
            self.assertEqual(evidence["strategy_version_id"], strategy["strategy_version_id"])
            self.assertEqual(evidence["candidate_id"], strategy["candidate_id"])
            self.assertEqual(evidence["research_trial_id"], strategy["research_trial_id"])
            for key in (
                "evaluation_run_id",
                "evaluation_version",
                "supersedes_evidence_id",
                "loaded_rows",
                "valid_input_rows",
                "evaluator_invoked",
                "evaluator_completed",
                "evaluated_observations",
                "signal_count",
                "diagnostic_summary_count",
                "evaluator_name",
                "evaluator_error",
                "evaluator_prerequisite",
            ):
                self.assertIn(key, evidence)
            self.assertTrue(evidence["evaluator_invoked"])
            self.assertTrue(evidence["evaluator_completed"])
            self.assertEqual(evidence["loaded_rows"], 3)
            self.assertEqual(evidence["valid_input_rows"], 3)
            self.assertEqual(evidence["evaluated_observations"], 3)
            self.assertEqual(evidence["signal_count"], 3)
            self.assertEqual(evidence["diagnostic_summary_count"], 0)
            self.assertIsNone(evidence["evaluator_error"])
            self.assertGreater(Decimal(str(evidence["realized_pnl"])), Decimal("0"))
    def test_live_price_proxy_rows_persist_as_canonical_simulation(self) -> None:
        strategy = _raw_application_strategy()
        class EmptyEvidenceStore:
            def list_strategy_evidence_windows(
                self,
                *,
                strategy_version_id: str,
                limit: int,
            ) -> list[dict[str, object]]:
                del strategy_version_id, limit
                return []

        processor = AutonomousResearchProcessor.__new__(AutonomousResearchProcessor)
        processor.store = EmptyEvidenceStore()
        evidence = processor._rolling_evidence_record(
            strategy,
            [
                _raw_application_row(0, 0.40),
                _raw_application_row(1, 0.40),
                _raw_application_row(2, 0.60),
            ],
            "LIVE",
            7,
            NOW,
        )
        self.assertIsNotNone(evidence)
        assert evidence is not None
        self.assertEqual(evidence["evaluation_kind"], "CANONICAL_SIMULATION")
        self.assertTrue(evidence["evaluator_invoked"])
        self.assertTrue(evidence["evaluator_completed"])
        self.assertTrue(evidence["accounting_available"])
        model = RollingEvidence.from_mapping(evidence)
        self.assertEqual(model.evaluation_kind, "CANONICAL_SIMULATION")
    def test_canonical_simulation_open_positions_over_storage_bound_fails_closed(self) -> None:
        strategy = _raw_application_strategy()
        class EmptyEvidenceStore:
            def list_strategy_evidence_windows(
                self,
                *,
                strategy_version_id: str,
                limit: int,
            ) -> list[dict[str, object]]:
                del strategy_version_id, limit
                return []

        processor = AutonomousResearchProcessor.__new__(AutonomousResearchProcessor)
        processor.store = EmptyEvidenceStore()
        positions = [
            {"market_id": f"canonical-market-{index:02d}", "quantity": "1"}
            for index in range(33)
        ]
        evidence = processor._rolling_evidence_record(
            strategy,
            [
                _raw_application_row(0, 0.40),
                _raw_application_row(1, 0.40),
                _raw_application_row(2, 0.60),
            ],
            "HISTORICAL",
            7,
            NOW,
            _canonical_simulation_evaluation(
                "canonical-over-bound",
                open_positions=positions,
            ),
        )
        self.assertIsNotNone(evidence)
        assert evidence is not None
        self.assertFalse(evidence["accounting_available"])
        self.assertFalse(evidence["accounting_complete"])
        self.assertTrue(evidence["accounting_partial"])
        self.assertEqual(
            evidence["accounting_unavailable_reason"],
            "ACCOUNTING_OPEN_POSITIONS_LIMIT_EXCEEDED",
        )
        self.assertEqual(evidence["positions"], 0)
        self.assertEqual(evidence["open_positions"], [])
        self.assertEqual(evidence["portfolio_accounting"]["open_positions"], [])
        model = RollingEvidence.from_mapping(evidence)
        self.assertEqual(model.portfolio_accounting["open_positions"], ())



    def test_public_refresh_historical_books_remain_price_proxy(self) -> None:
        raw_rows = [
            _raw_application_row(0, 0.40, book=True),
            _raw_application_row(1, 0.40, book=True),
            _raw_application_row(2, 0.60, book=True),
        ]
        with self._store("raw-book-routing.sqlite3") as store:
            with patch(
                "axiom.autonomous.run_prediction_research_mode",
                wraps=run_prediction_research_mode,
            ) as evaluator:
                _processor, strategy, state = self._refresh_raw_historical(store, raw_rows)
            historical_calls = [
                call
                for call in evaluator.call_args_list
                if call.kwargs.get("mode") == PRICE_PROXY_RESEARCH
            ]
            self.assertTrue(historical_calls)
            self.assertFalse(
                any(
                    call.kwargs.get("mode") == RECORDED_BOOK_REPLAY
                    for call in evaluator.call_args_list
                )
            )
            stored = store.list_strategy_evidence_windows(
                strategy["strategy_version_id"],
                limit=64,
            )
            produced = [
                item
                for item in state["evidence_windows"]
                if item["strategy_version_id"] == strategy["strategy_version_id"]
                and item.get("evaluation_version") == "rolling-evaluation:v2"
            ]
            self.assertTrue(produced)
            evidence = next(
                item
                for item in stored
                if item["evidence_window_id"] == produced[0]["evidence_window_id"]
            )
            self.assertEqual(
                evidence["source_class"],
                produced[0]["source_class"],
            )
            self.assertEqual(
                evidence["requested_days"],
                produced[0]["requested_days"],
            )
            self.assertTrue(evidence["evaluator_invoked"])
            self.assertTrue(evidence["evaluator_completed"])
            self.assertEqual(evidence["loaded_rows"], 3)
            self.assertEqual(evidence["valid_input_rows"], 3)
            self.assertEqual(evidence["evaluated_observations"], 3)
            self.assertEqual(evidence["signal_count"], 3)
            self.assertGreater(Decimal(str(evidence["realized_pnl"])), Decimal("0"))
    def test_public_refresh_explicit_replay_uses_recorded_book_catalog(self) -> None:
        raw_rows = [
            _raw_application_row(0, 0.40, book=True),
            _raw_application_row(1, 0.60, book=True),
            _raw_application_row(2, 0.60, book=True),
        ]
        with self._store("raw-explicit-replay.sqlite3") as store:
            with patch(
                "axiom.autonomous.run_prediction_research_mode",
                wraps=run_prediction_research_mode,
            ) as evaluator:
                _processor, strategy, state = self._refresh_raw_replay(store, raw_rows)
            replay_calls = [
                call
                for call in evaluator.call_args_list
                if call.kwargs.get("mode") == RECORDED_BOOK_REPLAY
            ]
            self.assertTrue(replay_calls)
            produced = [
                item
                for item in state["evidence_windows"]
                if item["strategy_version_id"] == strategy["strategy_version_id"]
                and item.get("evaluation_version") == "rolling-evaluation:v2"
            ]
            self.assertTrue(produced)
            evidence = next(
                item
                for item in store.list_strategy_evidence_windows(
                    strategy["strategy_version_id"], limit=64
                )
                if item["evidence_window_id"] == produced[0]["evidence_window_id"]
            )
            manifest = evidence["input_manifest"]
            catalog = store.list_dataset_catalog(
                source_type="FORWARD_COLLECTED",
                market_type=MarketType.PREDICTION,
                limit=8,
            )[0]
            self.assertEqual(manifest["dataset_id"], catalog["dataset_id"])
            self.assertEqual(manifest["dataset_version"], catalog["dataset_version"])
            self.assertEqual(
                manifest["dataset_id"], "Polymarket-recorded-book-replay"
            )
            self.assertEqual(manifest["mode"], RECORDED_BOOK_REPLAY)
            self.assertEqual(manifest["gap_count"], 0)
            self.assertEqual(len(manifest["market_paths"]), 1)

            self.assertEqual(evidence["loaded_rows"], 3)
            self.assertEqual(evidence["valid_input_rows"], 3)
            self.assertEqual(evidence["evaluated_observations"], 3)
            self.assertEqual(evidence["signal_count"], 3)
            self.assertGreater(Decimal(str(evidence["realized_pnl"])), Decimal("0"))

    def test_public_refresh_replay_without_books_never_uses_price_proxy(self) -> None:
        raw_rows = [
            _raw_application_row(0, 0.40),
            _raw_application_row(1, 0.60),
        ]
        with self._store("raw-replay-without-books.sqlite3") as store:
            with patch(
                "axiom.autonomous.run_prediction_research_mode",
                wraps=run_prediction_research_mode,
            ) as evaluator:
                _processor, strategy, state = self._refresh_raw_replay(store, raw_rows)
            proxy_calls = [
                call
                for call in evaluator.call_args_list
                if call.kwargs.get("mode") == PRICE_PROXY_RESEARCH
            ]
            replay_calls = [
                call
                for call in evaluator.call_args_list
                if call.kwargs.get("mode") == RECORDED_BOOK_REPLAY
            ]
            self.assertEqual(proxy_calls, [])
            self.assertEqual(replay_calls, [])
            self.assertEqual(state["evidence_windows"], [])
            self.assertTrue(
                any(
                    item.get("source_class") == "REPLAY"
                    and item.get("reason") == "REPLAY_BOOK_REQUIRED"
                    for item in state["pending"]
                )
            )
            blockers = store.list_rolling_evidence_blockers(limit=64)
            self.assertTrue(
                any(
                    item.get("source_class") == "REPLAY"
                    and item.get("blocker") == "REPLAY_BOOK_REQUIRED"
                    for item in blockers
                )
            )
            stored = store.list_strategy_evidence_windows(
                strategy["strategy_version_id"],
                limit=64,
            )
            self.assertEqual(stored, [])

    def test_public_refresh_requires_nested_evaluator_status(self) -> None:
        raw_rows = [
            _raw_application_row(0, 0.40),
            _raw_application_row(1, 0.60),
        ]

        def without_nested_status(*args: object, **kwargs: object) -> object:
            result = _run_prediction_research_mode(*args, **kwargs)
            result.metrics.pop("evaluation", None)
            return result

        with self._store("raw-missing-evaluator-status.sqlite3") as store:
            with patch(
                "axiom.autonomous.run_prediction_research_mode",
                side_effect=without_nested_status,
            ):
                _processor, strategy, _state = self._refresh_raw_historical(store, raw_rows)
            evidence = next(
                item
                for item in store.list_strategy_evidence_windows(
                    strategy["strategy_version_id"],
                    limit=64,
                )
                if item["requested_days"] == 7
            )
            self.assertFalse(evidence["evaluator_completed"])
            self.assertFalse(evidence["accounting_available"])
            self.assertEqual(evidence["evaluator_prerequisite"], "EVALUATOR_STATUS_REQUIRED")
            self.assertIsNone(evidence["realized_pnl"])
            self.assertIsNone(evidence["unrealized_pnl"])

    def test_public_refresh_invalid_evaluator_counts_cannot_aid_admission(self) -> None:
        raw_rows = [
            _raw_application_row(0, 0.40),
            _raw_application_row(1, 0.60),
        ]

        def fractional_count(*args: object, **kwargs: object) -> object:
            result = _run_prediction_research_mode(*args, **kwargs)
            evaluation = result.metrics["evaluation"]
            evaluation["evaluated_observations"] = 1.5
            return result

        with self._store("raw-invalid-evaluator-count.sqlite3") as store:
            with patch(
                "axiom.autonomous.run_prediction_research_mode",
                side_effect=fractional_count,
            ):
                _processor, strategy, _state = self._refresh_raw_historical(store, raw_rows)
            evidence = next(
                item
                for item in store.list_strategy_evidence_windows(
                    strategy["strategy_version_id"],
                    limit=64,
                )
                if item["requested_days"] == 7
            )
            self.assertFalse(evidence["evaluator_completed"])
            self.assertFalse(evidence["accounting_available"])
            self.assertEqual(evidence["evaluated_observations"], 0)
            self.assertEqual(evidence["accounting_unavailable_reason"], "EVALUATOR_COUNT_METRIC_INVALID")
            self.assertFalse(evidence["admitted"])

    def test_public_refresh_overlap_excludes_rejected_market_rows(self) -> None:
        valid = _raw_application_row(0, 0.40, market_id="market-raw-integration")
        rejected = _raw_application_row(1, 0.60, market_id="foreign-market")
        rejected.update(
            {
                "strategy_hash": "sha256:foreign",
                "strategy_version_id": "sv-foreign",
                "research_trial_id": "trial-foreign",
                "candidate_id": "candidate-foreign",
            }
        )
        with self._store("raw-overlap-rejections.sqlite3") as store:
            _processor, strategy, _state = self._refresh_raw_historical(
                store,
                [valid, rejected],
            )
            evidence = next(
                item
                for item in store.list_strategy_evidence_windows(
                    strategy["strategy_version_id"],
                    limit=64,
                )
                if item["requested_days"] == 7
                and item.get("requested_source_class") == "HISTORICAL"
            )
            self.assertEqual(evidence["valid_input_rows"], 1)
            self.assertEqual(evidence["overlap_key"], _rolling_overlap_key([valid]))
            self.assertNotEqual(
                evidence["overlap_key"],
                _rolling_overlap_key([valid, rejected]),
            )

    def test_public_refresh_rejects_malformed_and_conflicting_raw_provenance(self) -> None:
        conflict = _raw_application_row(0, 0.40)
        conflict.update(
            {
                "strategy_hash": "sha256:foreign",
                "strategy_version_id": "sv-foreign",
                "research_trial_id": "trial-foreign",
                "candidate_id": "candidate-foreign",
            }
        )
        malformed = _raw_application_row(1, 0.40)
        malformed.update(
            {
                "strategy_hash": "sha256:sv-raw-integration",
                "strategy_version_id": "sv-raw-integration",
                "research_trial_id": "trial-sv-raw-integration",
                "candidate_id": "candidate-sv-raw-integration",
                "timestamp": "malformed-timestamp",
            }
        )
        with self._store("raw-rejections.sqlite3") as store:
            _processor, strategy, state = self._refresh_raw_historical(
                store,
                [conflict, malformed],
            )
            reasons = {
                str(item.get("reason"))
                for item in state["pending"]
                if item.get("source_class") == "HISTORICAL"
            }
            self.assertIn("SOURCE_BINDING_CONFLICT", reasons)
            self.assertIn("SOURCE_TIMESTAMP_UNPARSEABLE", reasons)
            self.assertEqual(
                store.list_strategy_evidence_windows(
                    strategy["strategy_version_id"],
                    limit=64,
                ),
                [],
            )

    def test_public_refresh_evaluator_failure_keeps_zero_observations_and_null_pnl(self) -> None:
        raw_rows = [
            _raw_application_row(0, 0.40),
            _raw_application_row(1, 0.40),
        ]
        with self._store("raw-evaluator-failure.sqlite3") as store:
            with patch(
                "axiom.autonomous.run_prediction_research_mode",
                side_effect=RuntimeError("fixture evaluator exploded"),
            ):
                _processor, strategy, _state = self._refresh_raw_historical(store, raw_rows)
            stored = store.list_strategy_evidence_windows(
                strategy["strategy_version_id"],
                limit=64,
            )
            evidence = next(
                item
                for item in stored
                if item["source_class"] == "HISTORICAL"
                and item["requested_days"] == 7
            )
            self.assertTrue(evidence["evaluator_invoked"])
            self.assertFalse(evidence["evaluator_completed"])
            self.assertEqual(evidence["evaluated_observations"], 0)
            self.assertEqual(evidence["signal_count"], 0)
            self.assertEqual(evidence["loaded_rows"], 2)
            self.assertEqual(evidence["valid_input_rows"], 2)
            self.assertEqual(evidence["diagnostic_summary_count"], 1)
            self.assertIsNone(evidence["evaluator_name"])
            self.assertEqual(evidence["evaluator_error"], "fixture evaluator exploded")
            self.assertIsNone(evidence["realized_pnl"])
            self.assertIsNone(evidence["unrealized_pnl"])
            self.assertFalse(evidence["accounting_available"])

    def test_public_refresh_corrected_result_has_new_version_and_predecessor_link(self) -> None:
        initial_rows = [
            _raw_application_row(0, 0.40),
            _raw_application_row(1, 0.40),
        ]
        corrected_rows = [
            _raw_application_row(0, 0.40),
            _raw_application_row(1, 0.40),
            _raw_application_row(2, 0.60),
        ]
        with self._store("raw-version-linkage.sqlite3") as store:
            strategy = _raw_application_strategy()
            store.save_admission_policy(_policy().as_dict())
            processor = AutonomousResearchProcessor(store, clock=lambda: NOW)
            with patch.object(
                processor,
                "_rolling_strategy_documents",
                return_value=(strategy,),
            ), patch.object(
                processor,
                "_load_rolling_historical_dataset",
                side_effect=[initial_rows, corrected_rows],
            ), patch(
                "axiom.autonomous.run_prediction_research_mode",
                side_effect=RuntimeError("fixture evaluator exploded"),
            ):
                processor.refresh_rolling_evidence(now=NOW)
                with patch(
                    "axiom.autonomous.run_prediction_research_mode",
                    wraps=run_prediction_research_mode,
                ):
                    processor.refresh_rolling_evidence(now=NOW + timedelta(hours=1))
            stored = [
                item
                for item in store.list_strategy_evidence_windows(
                    strategy["strategy_version_id"],
                    limit=64,
                )
                if item["source_class"] == "HISTORICAL"
                and item["requested_days"] == 7
            ]
            diagnostic = next(item for item in stored if not item["evaluator_completed"])
            corrected = next(item for item in stored if item["evaluator_completed"])
            self.assertNotEqual(
                diagnostic["evaluation_run_id"],
                corrected["evaluation_run_id"],
            )
            self.assertNotEqual(
                diagnostic["evaluation_version"],
                corrected["evaluation_version"],
            )
            self.assertEqual(
                corrected["supersedes_evidence_id"],
                diagnostic["evidence_window_id"],
            )
            self.assertEqual(corrected["evaluated_observations"], 2)
            self.assertEqual(corrected["valid_input_rows"], 2)
    def test_public_refresh_predecessor_lookup_failure_blocks_correction(self) -> None:
        initial_rows = [
            _raw_application_row(0, 0.40),
            _raw_application_row(1, 0.40),
        ]
        corrected_rows = [
            _raw_application_row(0, 0.40),
            _raw_application_row(1, 0.40),
            _raw_application_row(2, 0.60),
        ]
        with self._store("raw-predecessor-lookup-failure.sqlite3") as store:
            strategy = _raw_application_strategy()
            store.save_admission_policy(_policy().as_dict())
            processor = AutonomousResearchProcessor(store, clock=lambda: NOW)
            with patch.object(
                processor,
                "_rolling_strategy_documents",
                return_value=(strategy,),
            ), patch.object(
                processor,
                "_load_rolling_historical_dataset",
                side_effect=[initial_rows, corrected_rows],
            ):
                processor.refresh_rolling_evidence(now=NOW)
                initial_stored = store.list_strategy_evidence_windows(
                    strategy["strategy_version_id"],
                    limit=64,
                )
                initial_ids = {
                    str(item["evidence_window_id"])
                    for item in initial_stored
                    if item.get("evaluation_version") == "rolling-evaluation:v2"
                }
                self.assertTrue(initial_ids)
                with patch.object(
                    store,
                    "list_strategy_evidence_windows",
                    side_effect=RuntimeError("fixture predecessor listing unavailable"),
                ):
                    state = processor.refresh_rolling_evidence(
                        now=NOW + timedelta(hours=1)
                    )

            stored = store.list_strategy_evidence_windows(
                strategy["strategy_version_id"],
                limit=64,
            )
            stored_ids = {
                str(item["evidence_window_id"])
                for item in stored
                if item.get("evaluation_version") == "rolling-evaluation:v2"
            }
            self.assertEqual(stored_ids, initial_ids)

            pending = [
                item
                for item in state["pending"]
                if item.get("source_class") == "HISTORICAL"
                and item.get("reason") == "PREDECESSOR_LOOKUP_UNAVAILABLE"
            ]
            self.assertTrue(pending)
            self.assertTrue(all(item.get("status") == "BLOCKED" for item in pending))
            self.assertEqual(state["status"], "SCHEDULED")
            blockers = store.list_rolling_evidence_blockers(limit=64)
            matching_blockers = [
                item
                for item in blockers
                if item.get("source_class") == "HISTORICAL"
                and item.get("blocker") == "PREDECESSOR_LOOKUP_UNAVAILABLE"
            ]
            self.assertTrue(matching_blockers)
            self.assertTrue(
                any(
                    (
                        item.get("payload")
                        if isinstance(item.get("payload"), Mapping)
                        else {}
                    ).get("retryable")
                    is True
                    and (
                        item.get("payload")
                        if isinstance(item.get("payload"), Mapping)
                        else {}
                    ).get("non_retryable")
                    is False
                    and (
                        item.get("payload")
                        if isinstance(item.get("payload"), Mapping)
                        else {}
                    ).get("terminal")
                    is False
                    and (
                        item.get("payload")
                        if isinstance(item.get("payload"), Mapping)
                        else {}
                    ).get("next_attempt_at")
                    for item in matching_blockers
                )
            )
    def test_rolling_evidence_missing_predecessor_lister_blocks_retryably(self) -> None:
        strategy = _raw_application_strategy("sv-missing-predecessor-lister")
        rows = [
            _raw_application_row(0, 0.40),
            _raw_application_row(1, 0.40),
            _raw_application_row(2, 0.60),
        ]

        class MissingListerStore:
            pass

        processor = AutonomousResearchProcessor.__new__(AutonomousResearchProcessor)
        processor.store = MissingListerStore()
        with self.assertRaises(AutonomousResearchError) as raised:
            processor._rolling_evidence_record(
                strategy,
                rows,
                "HISTORICAL",
                7,
                NOW,
                _canonical_simulation_evaluation("missing-lister"),
            )
        self.assertEqual(raised.exception.reason, "PREDECESSOR_LOOKUP_UNAVAILABLE")

    def test_rolling_evidence_sqlite_predecessor_lister_failure_blocks_retryably(self) -> None:
        strategy = _raw_application_strategy("sv-sqlite-predecessor-lister")
        rows = [
            _raw_application_row(0, 0.40),
            _raw_application_row(1, 0.40),
            _raw_application_row(2, 0.60),
        ]

        class SqliteFailureStore:
            def list_strategy_evidence_windows(
                self,
                *,
                strategy_version_id: str,
                limit: int,
            ) -> list[dict[str, object]]:
                del strategy_version_id, limit
                raise sqlite3.OperationalError("database is locked")

        processor = AutonomousResearchProcessor.__new__(AutonomousResearchProcessor)
        processor.store = SqliteFailureStore()
        with self.assertRaises(AutonomousResearchError) as raised:
            processor._rolling_evidence_record(
                strategy,
                rows,
                "HISTORICAL",
                7,
                NOW,
                _canonical_simulation_evaluation("sqlite-lister"),
            )
        self.assertEqual(raised.exception.reason, "PREDECESSOR_LOOKUP_UNAVAILABLE")

    def test_rolling_evidence_malformed_predecessor_listing_blocks_retryably(self) -> None:
        strategy = _raw_application_strategy("sv-malformed-predecessor-lister")
        rows = [
            _raw_application_row(0, 0.40),
            _raw_application_row(1, 0.40),
            _raw_application_row(2, 0.60),
        ]

        class MalformedListerStore:
            def __init__(self, result: object) -> None:
                self.result = result

            def list_strategy_evidence_windows(
                self,
                *,
                strategy_version_id: str,
                limit: int,
            ) -> object:
                del strategy_version_id, limit
                return self.result

        malformed_results = (
            None,
            {"evidence_window_id": "not-a-list"},
            "not-a-list",
            [object()],
        )
        for index, result in enumerate(malformed_results):
            with self.subTest(index=index):
                processor = AutonomousResearchProcessor.__new__(
                    AutonomousResearchProcessor
                )
                processor.store = MalformedListerStore(result)
                with self.assertRaises(AutonomousResearchError) as raised:
                    processor._rolling_evidence_record(
                        strategy,
                        rows,
                        "HISTORICAL",
                        7,
                        NOW,
                        _canonical_simulation_evaluation(f"malformed-lister-{index}"),
                    )
                self.assertEqual(
                    raised.exception.reason,
                    "PREDECESSOR_LOOKUP_UNAVAILABLE",
                )

    def test_predecessor_head_uses_available_order_not_created_order(self) -> None:
        strategy = _raw_application_strategy("sv-predecessor-order")
        rows = [
            _raw_application_row(0, 0.40),
            _raw_application_row(1, 0.40),
            _raw_application_row(2, 0.60),
        ]

        class EvidenceStore:
            def __init__(self, records: list[dict[str, object]]) -> None:
                self.records = records

            def list_strategy_evidence_windows(
                self,
                *,
                strategy_version_id: str,
                limit: int,
            ) -> list[dict[str, object]]:
                del strategy_version_id, limit
                return list(self.records)

        older_available = _evidence(
            strategy["strategy_version_id"],
            "created-newer",
            available_from=(NOW - timedelta(days=3)).isoformat(),
            available_through=(NOW - timedelta(days=2)).isoformat(),
            actual_coverage_seconds=86400,
            created_at=(NOW + timedelta(days=7)).isoformat(),
        )
        newer_available = _evidence(
            strategy["strategy_version_id"],
            "available-newer",
            available_from=(NOW - timedelta(days=3)).isoformat(),
            available_through=(NOW - timedelta(days=1)).isoformat(),
            actual_coverage_seconds=2 * 86400,
            created_at=(NOW - timedelta(days=7)).isoformat(),
        )
        processor = AutonomousResearchProcessor.__new__(AutonomousResearchProcessor)
        processor.store = EvidenceStore([older_available, newer_available])
        correction = processor._rolling_evidence_record(
            strategy,
            rows,
            "HISTORICAL",
            7,
            NOW,
            _canonical_simulation_evaluation("available-order"),
        )
        self.assertIsNotNone(correction)
        assert correction is not None
        self.assertEqual(correction["supersedes_evidence_id"], "available-newer")

    def test_same_endpoint_correction_supersedes_active_latest_predecessor(self) -> None:
        strategy = _raw_application_strategy()
        rows = [
            _raw_application_row(0, 0.40),
            _raw_application_row(1, 0.40),
            _raw_application_row(2, 0.60),
        ]

        class EvidenceStore:
            def __init__(self) -> None:
                self.records: list[dict[str, object]] = []

            def list_strategy_evidence_windows(
                self,
                *,
                strategy_version_id: str,
                limit: int,
            ) -> list[dict[str, object]]:
                del strategy_version_id, limit
                return list(self.records)

        store = EvidenceStore()
        processor = AutonomousResearchProcessor.__new__(AutonomousResearchProcessor)
        processor.store = store

        first = processor._rolling_evidence_record(
            strategy,
            rows,
            "HISTORICAL",
            7,
            NOW,
            _canonical_simulation_evaluation("same-endpoint-run-1"),
        )
        self.assertIsNotNone(first)
        assert first is not None
        store.records.append(first)

        second = processor._rolling_evidence_record(
            strategy,
            rows,
            "HISTORICAL",
            7,
            NOW,
            _canonical_simulation_evaluation("same-endpoint-run-2"),
        )
        self.assertIsNotNone(second)
        assert second is not None
        self.assertEqual(
            second["supersedes_evidence_id"],
            first["evidence_window_id"],
        )
        store.records.append(second)

        third = processor._rolling_evidence_record(
            strategy,
            rows,
            "HISTORICAL",
            7,
            NOW,
            _canonical_simulation_evaluation("same-endpoint-run-3"),
        )
        self.assertIsNotNone(third)
        assert third is not None
        self.assertEqual(
            third["supersedes_evidence_id"],
            second["evidence_window_id"],
        )


    def test_actual_ledger_open_positions_at_storage_bound_persist(self) -> None:
        strategy_id = "sv-actual-ledger-bound"
        strategy = _strategy(strategy_id)
        with self._store("actual-ledger-bound.sqlite3") as store:
            store.save_strategy_version(strategy)
            processor = AutonomousResearchProcessor(store, clock=lambda: NOW)
            record = processor._rolling_evidence_record(
                strategy,
                [_actual_ledger_row(32)],
                "PAPER",
                7,
                NOW,
            )
            self.assertIsNotNone(record)
            assert record is not None
            self.assertTrue(record["accounting_available"])
            self.assertTrue(record["accounting_complete"])
            self.assertFalse(record["accounting_partial"])
            self.assertFalse(record["evaluator_invoked"])
            self.assertFalse(record["evaluator_completed"])
            self.assertEqual(len(record["open_positions"]), 32)
            store.save_strategy_evidence_window(record)
            stored = store.list_strategy_evidence_windows(strategy_id, limit=64)
            self.assertEqual(len(stored), 1)
            self.assertEqual(len(stored[0]["open_positions"]), 32)
            self.assertTrue(stored[0]["accounting_available"])
            self.assertTrue(stored[0]["accounting_complete"])

    def test_actual_ledger_open_positions_over_storage_bound_is_unavailable(self) -> None:
        strategy_id = "sv-actual-ledger-over-bound"
        strategy = _strategy(strategy_id)
        with self._store("actual-ledger-over-bound.sqlite3") as store:
            store.save_strategy_version(strategy)
            processor = AutonomousResearchProcessor(store, clock=lambda: NOW)
            record = processor._rolling_evidence_record(
                strategy,
                [_actual_ledger_row(33)],
                "PAPER",
                7,
                NOW,
            )
            self.assertIsNotNone(record)
            assert record is not None
            self.assertFalse(record["accounting_available"])
            self.assertFalse(record["accounting_complete"])
            self.assertTrue(record["accounting_partial"])
            self.assertFalse(record["evaluator_invoked"])
            self.assertFalse(record["evaluator_completed"])
            self.assertEqual(record["accounting_unavailable_reason"], "ACCOUNTING_OPEN_POSITIONS_LIMIT_EXCEEDED")
            self.assertEqual(record["open_positions"], [])
            self.assertEqual(record["portfolio_accounting"]["open_positions"], [])
            store.save_strategy_evidence_window(record)
            stored = store.list_strategy_evidence_windows(strategy_id, limit=64)
            self.assertEqual(len(stored), 1)
            self.assertFalse(stored[0]["accounting_available"])
            self.assertFalse(stored[0]["accounting_complete"])
            self.assertTrue(stored[0]["accounting_partial"])
            self.assertEqual(
                stored[0]["accounting_unavailable_reason"],
                "ACCOUNTING_OPEN_POSITIONS_LIMIT_EXCEEDED",
            )
            self.assertEqual(stored[0]["open_positions"], [])

    def _store(self, name: str = "acceptance.sqlite3") -> AxiomStore:
        return AxiomStore(str(self.path / name))

    def _seed_artifacts(
        self,
        store: AxiomStore,
        policy: RollingAdmissionPolicy,
        strategy_ids: tuple[str, ...],
        *,
        evidence: list[dict[str, object]] | None = None,
    ) -> list[dict[str, object]]:
        store.save_admission_policy(policy.as_dict())
        rows = evidence or [_evidence(strategy_id) for strategy_id in strategy_ids]
        for strategy_id in strategy_ids:
            store.save_strategy_version(_strategy(strategy_id))
            store.save_research_trial(_trial(strategy_id))
        for row in rows:
            store.save_strategy_evidence_window(row)
        return rows
    def _seed_executable_candidate(
        self,
        store: AxiomStore,
        candidate_id: str,
        *,
        market_id: str = "rolling-market",
        exit_policy: dict[str, object] | None = None,
    ) -> dict[str, object]:
        strategy = {
            "version": 1,
            "market_type": "prediction",
            "family": "probability_mispricing",
            "parameters": {"threshold": 0.05},
            "operations": [],
            "probability_model": "fixed",
            "resolution_aware": True,
            "resolution_inputs": ["expiry", "settlement"],
            "strategy_id": candidate_id,
        }
        model = {"probability": 0.80}
        strategy_hash = "sha256:" + hashlib.sha256(
            json.dumps(strategy, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
        ).hexdigest()
        model_hash = "sha256:" + hashlib.sha256(
            json.dumps(model, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
        ).hexdigest()
        config_hash = "config:rolling-worker"
        scope = normalize_market_scope(
            {
                "schema_version": "1",
                "mode": "EXACT_MARKETS",
                "instrument": "POLYMARKET",
                "categories": [],
                "market_ids": [market_id],
                "filters": {},
                "regime_restrictions": {},
                "provenance": "canonical",
            }
        )
        payload: dict[str, object] = {
            "candidate_id": candidate_id,
            "market_type": "prediction",
            "market_scope": scope.as_dict(),
            "market_scope_hash": scope.scope_hash,
            "market_scope_version": scope.scope_version,
            "plan_hash": "sha256:rolling-worker-plan",
            "dataset_id": "rolling-worker-history",
            "dataset_version": "v1",
            "dataset_selector": {
                "dataset_id": "rolling-worker-history",
                "dataset_version": "v1",
                "source_type": "HISTORICAL",
            },
            "dataset_attestation": {
                "dataset_id": "rolling-worker-history",
                "dataset_version": "v1",
                "status": "CURRENT",
                "policy_version": "v1",
                "attestation_hash": "sha256:rolling-worker-attestation",
            },
            "dataset_provenance": {
                "dataset_id": "rolling-worker-history",
                "dataset_version": "v1",
                "source_type": "HISTORICAL",
                "time_split": "train-validation-holdout",
            },
            "lineage": ["rolling-worker"],
            "mutation_cluster": "rolling-worker",
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
            "frozen_hash": hashlib.sha256(
                "|".join((strategy_hash, model_hash, config_hash)).encode()
            ).hexdigest(),
            "validation_expectancy": 0.80,
            "validation_confidence_lower_bound": 0.80,
            "validation_stability": 0.90,
            "validation_calibration": 0.90,
            "validation_sample_count": 100,
            "validation_trade_count": 50,
            "validation_execution_quality": 0.90,
            "data_quality": "PRICE_PROXY",
            "minimum_sample_check": {
                "passed": True,
                "count": 100,
                "trades": 50,
                "checks": {"observations": True, "trades": True},
            },
            "strategy_document": strategy,
            "model_document": model,
            "target_market_ids": [market_id],
            "exit_policy": exit_policy or {
                "type": "fixed_holding_period",
                "holding_period_seconds": 0,
            },
        }
        historical_records = [
            {
                "timestamp": NOW.isoformat(),
                "price": "0.50",
                "source_type": "HISTORICAL",
            }
        ]
        if store.load_dataset("rolling-worker-history", "v1") is None:
            store.save_dataset(
                "rolling-worker-history",
                "v1",
                historical_records,
                quality="PRICE_PROXY",
                metadata={"provider": "polymarket", "source_type": "HISTORICAL"},
            )
        if store.load_dataset_catalog("rolling-worker-history", "v1") is None:
            store.save_dataset_catalog(
                "rolling-worker-history",
                "v1",
                provider="polymarket",
                instrument="POLYMARKET",
                market_type="prediction",
                timeframe="event",
                start_timestamp=NOW,
                end_timestamp=NOW,
                row_count=len(historical_records),
                completeness=1.0,
                quality="PRICE_PROXY",
                source_type="HISTORICAL",
                snapshot_id="rolling-worker-history:v1",
                metadata={
                    "provider": "polymarket",
                    "source_type": "HISTORICAL",
                    "research_quality": "PRICE_PROXY",
                    "historical_order_book_available": False,
                },
                created_at=NOW,
                updated_at=NOW,
            )
        store.save_candidate_lifecycle(candidate_id, "IDEA", payload, timestamp=NOW)
        for stage in (
            "SCHEMA_VALIDATED",
            "BACKTESTED",
            "VALIDATED",
            "ROBUSTNESS_CHECKED",
            "FROZEN",
        ):
            store.save_candidate_lifecycle(candidate_id, stage, payload, timestamp=NOW)
        store.save_market_scope_resolution(
            {
                "candidate_id": candidate_id,
                "scope_hash": payload["market_scope_hash"],
                "scope_version": payload["market_scope_version"],
                "resolved_at": NOW,
                "status": "MATCHED",
                "reason": "MATCHED",
                "policy": payload["market_scope"],
                "matched_markets": [
                    {
                        "market_id": market_id,
                        "condition_id": f"condition:{market_id}",
                        "yes_token_id": "yes",
                        "no_token_id": "no",
                        "instrument": "POLYMARKET",
                        "venue": "POLYMARKET",
                        "source_type": "CURRENT",
                        "active": True,
                        "open": True,
                        "closed": False,
                        "settlement": "open",
                        "accepting_orders": True,
                        "enable_order_book": True,
                        "metadata_provenance": {"source": "acceptance"},
                    }
                ],
                "provenance": {"source": "acceptance"},
            }
        )
        store.save_polymarket_market_metadata(
            market_id,
            {
                "market_id": market_id,
                "active": True,
                "closed": False,
                "settlement": "open",
            },
            observed_at=NOW,
            source_type="FORWARD_COLLECTED",
        )
        store.save_polymarket_snapshot(
            f"snapshot:{candidate_id}",
            market_id,
            NOW,
            NOW,
            {
                "source_type": "FORWARD_COLLECTED",
                "snapshot": {
                    "market_id": market_id,
                    "timestamp": NOW.isoformat(),
                    "yes_ask": "0.50",
                    "yes_order_book": {
                        "asks": [{"price": "0.50", "size": "100"}],
                        "bids": [],
                        "timestamp": NOW.isoformat(),
                        "token_id": "yes",
                    },
                    "no_order_book": {
                        "asks": [{"price": "0.50", "size": "100"}],
                        "bids": [],
                        "timestamp": NOW.isoformat(),
                        "token_id": "no",
                    },
                    "yes_token_id": "yes",
                    "no_ask": "0.50",
                    "no_token_id": "no",
                    "settlement": "open",
                },
                "active": True,
                "settlement": "open",
            },
            source_type="FORWARD_COLLECTED",
        )
        return payload

    def _activate_worker_authorization(
        self,
        settings: CanarySettingsService,
        *,
        strategy_version_id: str,
        selection_id: str,
        selection_hash: str | None = None,
        selection_policy_hash: str | None = None,
        mode: str = "EXPLORATORY_MICRO_CANARY",
    ) -> dict[str, object]:
        config = settings.snapshot(now=NOW)
        active = settings.load_active_execution_authorization(
            mode=mode,
            now=NOW,
        )
        if active is not None:
            settings.revoke_execution_authorization(
                str(active["authorization_id"]),
                "acceptance",
                expected_generation=int(active["generation"]),
                reason="replace acceptance binding",
            )
        authorization_suffix = (selection_hash or "unbound")[:16]
        draft = settings.register_execution_authorization_draft(
            authorization_id=f"authorization:{selection_id}:{authorization_suffix}",
            mode=mode,
            purpose="rolling-worker",
            exact_strategy_versions=(strategy_version_id,),
            reviewed_selection_policy_hash=selection_policy_hash,
            adverse_evidence_ack={"acknowledged": True},
            lifetime_budget={"max_notional_usd": "100.00", "max_orders": 100},
            stop_rules={"max_loss_usd": "100.00"},
            expires_at=NOW + timedelta(days=1),
            scope_hash=hashlib.sha256(selection_id.encode("utf-8")).hexdigest(),
            scope_version="1",
            selection_id=selection_id,
            selection_hash=selection_hash,
            actor="acceptance",
            actor_version="acceptance-v1",
            active_settings_hash=str(config["config_hash"]),
            active_settings_generation=int(config["generation"]),
        )
        return settings.activate_execution_authorization(
            str(draft["authorization_id"]),
            "acceptance",
            expected_generation=int(draft["generation"]),
        )

    def _rolling_worker_fixture(
        self,
        store: AxiomStore,
        *,
        policy_id: str,
        candidate_id: str = "candidate-rolling-worker",
        selection_id: str = "selection-rolling-worker",
        venue: RollingLifecycleVenue,
        exit_policy: dict[str, object] | None = None,
        authorization_mode: str = "EXPLORATORY_MICRO_CANARY",
    ) -> tuple[RollingAdmissionPolicy, CanaryService, AutonomousCanaryWorker]:
        policy = _policy(
            policy_id,
            config_hash=hashlib.sha256(policy_id.encode("utf-8")).hexdigest(),
            max_members=1,
            experimental_allocation_enabled=True,
            replacement_margin="0",
            cooldown_seconds=0,
        )
        strategy_id = candidate_id.removeprefix("candidate-")
        self._seed_artifacts(
            store,
            policy,
            (strategy_id,),
            evidence=[_evidence(strategy_id)],
        )
        self._seed_executable_candidate(
            store,
            candidate_id,
            market_id=venue.market_id,
            exit_policy=exit_policy,
        )
        member = _member(
            strategy_id,
            selection_id,
            allocation="10.00",
            candidate_id=candidate_id,
            research_trial_id=f"trial-{strategy_id}",
            position_management_state={
                "exit_policy": exit_policy
                or {"type": "fixed_holding_period", "holding_period_seconds": 0}
            },
        )
        settings = self._activate_isolated_risk_settings(store)
        config = settings.snapshot(now=NOW)
        active_policy = {
            "policy_id": policy.policy_id,
            "version": policy.version,
            "policy_version": policy.version,
            "config_hash": policy.config_hash,
            "risk_config_id": config["config_id"],
            "risk_config_generation": config["generation"],
            "risk_config_hash": config["config_hash"],
            "status": "ACTIVE",
            "paper_only": True,
        }
        store.set_operator_config("rolling_admission_policy_active", active_policy)
        store.set_operator_job(
            "rolling_admission_policy_active",
            "ACTIVE",
            active_policy,
            resumable=True,
            timestamp=NOW,
        )
        selection = _selection(selection_id, policy, members=[member])
        selection.update(
            {
                "policy_hash": policy.config_hash,
                "risk_config_id": config["config_id"],
                "risk_config_generation": config["generation"],
                "risk_config_hash": config["config_hash"],
                "execution_authorization_mode": authorization_mode,
            }
        )
        selection["selection_hash"] = _selection_binding_hash(selection)
        store.commit_portfolio_selection(selection, [member])
        self._activate_worker_authorization(
            settings,
            strategy_version_id=strategy_id,
            selection_id=selection_id,
            selection_hash=str(selection["selection_hash"]),
            selection_policy_hash=policy.config_hash,
            mode=authorization_mode,
        )
        service = CanaryService(
            store,
            credentials=AcceptanceCredentials(),
            clock=lambda: NOW,
            settings=settings,
        )
        service.mark_eligible(candidate_id)
        service.enable_autonomous_micro_live(
            "polymarket",
            config_id=str(config["config_id"]),
            expected_generation=int(config["generation"]),
        )
        worker = AutonomousCanaryWorker(
            store,
            clock=lambda: NOW,
            venue_factory=lambda: venue,
            allow_test_venue=True,
        )
        return policy, service, worker

    def _reserve(
        self,
        store: AxiomStore,
        intent: str,
        *,
        side: str = "BUY",
        market: str | None = None,
        now: datetime = NOW,
        cost: str = "0.25",
        quantity: str = "1",
        lineage: dict[str, object] | None = None,
        compatibility_mode: bool = True,
    ) -> dict[str, object]:
        values: dict[str, object] = {
            "intent_id": intent,
            "reservation_id": f"reservation:{intent}",
            "side": side,
            "requested_cost": cost,
            "quantity": quantity,
            "market_id": market or f"market:{intent}",
            "event_id": f"event:{intent}",
            "timestamp": now,
        }
        if lineage:
            values.update(lineage)
        values["compatibility_mode"] = compatibility_mode
        return store.reserve_canary_capacity(**values)
    def _activate_isolated_risk_settings(
        self,
        store: AxiomStore,
        *,
        max_orders: int = 20,
        max_all_in_buy_usd: str | None = None,
    ) -> CanarySettingsService:
        settings = CanarySettingsService(store, clock=lambda: NOW)
        observed = settings.snapshot(now=NOW)
        values: dict[str, object] = {"max_orders_per_day": max_orders}
        if max_all_in_buy_usd is not None:
            values["max_all_in_buy_usd"] = max_all_in_buy_usd
        draft = settings.save_draft(
            values,
            "rolling-acceptance",
            expected_generation=int(observed["generation"]),
        )
        settings.activate_draft(
            draft["config_id"],
            "rolling-acceptance",
            expected_generation=int(observed["generation"]),
        )
        return settings

    @staticmethod
    def _lineage(
        *,
        strategy_version_id: str,
        selection_id: str,
        policy: RollingAdmissionPolicy,
        allocation: str,
    ) -> dict[str, object]:
        return {
            "candidate_id": f"candidate-{strategy_version_id}",
            "strategy_version_id": strategy_version_id,
            "research_trial_id": f"trial-{strategy_version_id}",
            "portfolio_selection_id": selection_id,
            "admission_policy_id": policy.policy_id,
            "admission_policy_version": policy.version,
            "risk_config_id": "risk-acceptance",
            "risk_config_generation": 1,
            "risk_config_hash": "sha256:risk-acceptance",
            "allocation": allocation,
        }

    def _seed_owned_inventory(self, store: AxiomStore, *, market: str, now: datetime) -> None:
        adopted = store.adopt_canary_legacy_reservation(
            event_id=f"legacy:{market}",
            intent_id=f"legacy-buy:{market}",
            reservation_id=f"legacy-reservation:{market}",
            side="BUY",
            market_id=market,
            token_id="yes",
            requested_cost="0.10",
            quantity="1",
            timestamp=now,
            legacy_status="FILLED",
        )
        filled = store.record_canary_fill(
            fill_id=f"legacy-fill:{market}",
            reservation_id=adopted["reservation_id"],
            quantity="1",
            price="0.10",
            filled_at=now,
            detail={"settlement": "SETTLED", "token_id": "yes"},
        )
        self.assertEqual(filled["status"], "FILLED")
    def test_simultaneous_active_strategies_share_one_selection_and_budget(self) -> None:
        policy = _policy(max_members=2, global_budget="10.00", experimental_allocation_enabled=True)
        evidence = [_evidence("sv-alpha", net_return="8.00"), _evidence("sv-beta", net_return="7.00")]
        decision = evaluate_rolling_selection(policy, evidence, None, NOW)
        members = _member_map(decision)
        self.assertEqual(set(members), {"sv-alpha", "sv-beta"})
        self.assertTrue(all(_value(member, "status") == "ACTIVE" for member in members.values()))
        self.assertEqual(sum((Decimal(str(_value(member, "allocation"))) for member in members.values()), Decimal()), Decimal("10.00"))
        with self._store() as store:
            self._seed_artifacts(store, policy, ("sv-alpha", "sv-beta"), evidence=evidence)
            stored = store.commit_portfolio_selection(_selection("selection-active", policy, members=[dict(member.as_dict()) for member in members.values()]), [dict(member.as_dict()) for member in members.values()])
            self.assertEqual({row["status"] for row in stored["members"]}, {"ACTIVE"})
            self.assertEqual(stored["k"], 2)

    def test_ordinary_loss_survives_without_rotation_or_allocation_loss(self) -> None:
        policy = _policy(min_score="0.00")
        current = _selection(
            "selection-loss",
            policy,
            members=[_member("sv-incumbent", "selection-loss", allocation="10.00", score="0.80", position_management_state={"open_lot": "lot-1"})],
        )
        ordinary_loss = _evidence("sv-incumbent", net_return="0.00", drawdown="0.80", reliability="0.90")
        decision = evaluate_rolling_selection(policy, [ordinary_loss], current, NOW + timedelta(days=1))
        members = _member_map(decision)
        incumbent = members["sv-incumbent"]
        self.assertEqual(_value(incumbent, "strategy_version_id"), "sv-incumbent")
        self.assertIn(_value(incumbent, "status"), {"ACTIVE", "RETAINED"})
        self.assertEqual(Decimal(str(_value(incumbent, "allocation"))), Decimal("10.00"))
        self.assertIn(_value(decision, "status"), {"ACTIVE", "PAPER"})


    def test_deterioration_hard_failure_pauses_and_reduces_only_affected_strategy(self) -> None:
        policy = _policy(max_members=2)
        current = _selection(
            "selection-deterioration",
            policy,
            members=[
                _member("sv-bad", "selection-deterioration", allocation="5.00", score="0.70"),
                _member("sv-good", "selection-deterioration", allocation="5.00", score="0.60"),
            ],
        )
        decision = evaluate_rolling_selection(
            policy,
            [_evidence("sv-bad", execution_feasibility=False), _evidence("sv-good", net_return="7.00")],
            current,
            NOW + timedelta(days=1),
        )
        members = _member_map(decision)
        self.assertEqual(_value(members["sv-bad"], "status"), "PAUSED")
        self.assertEqual(Decimal(str(_value(members["sv-bad"], "allocation"))), Decimal("0"))
        self.assertNotEqual(_value(members["sv-good"], "status"), "PAUSED")
        self.assertEqual(Decimal(str(_value(members["sv-good"], "allocation"))), Decimal("5.00"))

    def test_original_policy_and_selection_remain_available_for_exit_lineage(self) -> None:
        old_policy = _policy("policy-original", global_budget="10.00")
        new_policy = _policy("policy-replacement", global_budget="10.00")
        with self._store() as store:
            self._seed_artifacts(store, old_policy, ("sv-original",), evidence=[_evidence("sv-original")])
            old = store.commit_portfolio_selection(
                _selection("selection-original", old_policy, members=[_member("sv-original", "selection-original")]),
                [_member("sv-original", "selection-original")],
            )
            store.save_admission_policy(new_policy.as_dict())
            store.commit_portfolio_selection(
                _selection("selection-new", new_policy, members=[]),
                [],
            )
            history = store.list_portfolio_selections(limit=10)
            self.assertEqual({row["portfolio_selection_id"] for row in history}, {"selection-original", "selection-new"})
            original = next(row for row in history if row["portfolio_selection_id"] == "selection-original")
            self.assertEqual(original["policy_id"], "policy-original")
            self.assertEqual(original["members"][0]["strategy_version_id"], "sv-original")
            self.assertEqual(old["risk_config_id"], "risk-acceptance")

    def test_replacement_requires_margin_and_allocates_new_member(self) -> None:
        policy = _policy(max_members=1, global_budget="10.00", experimental_allocation_enabled=True, replacement_margin="0.10")
        current = _selection(
            "selection-replace",
            policy,
            selected_at=NOW - timedelta(days=2),
            members=[_member("sv-old", "selection-replace", allocation="10.00", score="0.10")],
        )
        decision = evaluate_rolling_selection(policy, [_evidence("sv-new", net_return="20.00")], current, NOW)
        members = _member_map(decision)
        self.assertIn("sv-new", members)
        self.assertEqual(_value(members["sv-new"], "status"), "ACTIVE")
        self.assertEqual(Decimal(str(_value(members["sv-new"], "allocation"))), Decimal("10.00"))
        removed = list(getattr(decision, "removed_members", ()))
        self.assertEqual([_value(row, "strategy_version_id") for row in removed], ["sv-old"])

    def test_review_recomputes_removed_member_runtime_obligation(self) -> None:
        policy = _policy(
            "policy-runtime-rotation",
            max_members=1,
            global_budget="10.00",
            experimental_allocation_enabled=True,
            replacement_margin="0.10",
        )
        with self._store() as store:
            self._seed_artifacts(
                store,
                policy,
                ("sv-old", "sv-new"),
                evidence=[
                    _evidence("sv-old", net_return="8.00"),
                    _evidence("sv-new", net_return="20.00"),
                ],
            )
            active_policy = {**policy.as_dict(), "status": "ACTIVE"}
            store.set_operator_config("rolling_admission_policy_active", active_policy)
            store.set_operator_job(
                "rolling_admission_policy_active",
                "ACTIVE",
                active_policy,
                resumable=True,
                timestamp=NOW,
            )
            old_member = _member(
                "sv-old",
                "selection-runtime-old",
                allocation="5.00",
                score="0.80",
            )
            store.commit_portfolio_selection(
                _selection(
                    "selection-runtime-old",
                    policy,
                    selected_at=NOW - timedelta(days=2),
                    members=[old_member],
                ),
                [old_member],
            )
            with patch.object(
                store,
                "canary_risk_accounting",
                return_value={
                    "rolling_global_reserved_usd": "4.00",
                    "rolling_strategy_reserved_usd": {"sv-old": "4.00"},
                },
            ) as accounting:
                state = AutonomousResearchProcessor(store, clock=lambda: NOW).review_rolling_portfolio(
                    now=NOW,
                    force=True,
                )
            accounting.assert_called_once()
            self.assertEqual(accounting.call_args.kwargs["now"], NOW)
            selection = store.load_current_portfolio_selection()
            self.assertIsNotNone(selection)
            assert selection is not None
            self.assertEqual(selection["members"][0]["strategy_version_id"], "sv-new")
            self.assertEqual(selection["members"][0]["allocation"], "6.00")
            self.assertEqual(state["external_obligations"], "4.00")
            self.assertEqual(state["uncovered_obligations"], "4.00")
            self.assertEqual(state["available_budget"], "6.00")

    def test_missing_runtime_reservation_detail_fails_closed(self) -> None:
        policy = _policy(
            "policy-runtime-missing-detail",
            max_members=1,
            global_budget="10.00",
            experimental_allocation_enabled=True,
            replacement_margin="0",
        )
        with self._store("rolling-runtime-missing-detail.sqlite3") as store:
            self._seed_artifacts(
                store,
                policy,
                ("sv-runtime-missing-detail",),
                evidence=[_evidence("sv-runtime-missing-detail", net_return="20.00")],
            )
            active_policy = {**policy.as_dict(), "status": "ACTIVE"}
            store.set_operator_config("rolling_admission_policy_active", active_policy)
            store.set_operator_job(
                "rolling_admission_policy_active",
                "ACTIVE",
                active_policy,
                resumable=True,
                timestamp=NOW,
            )
            with patch.object(
                store,
                "canary_risk_accounting",
                return_value={"rolling_global_reserved_usd": "4.00"},
            ) as accounting:
                state = AutonomousResearchProcessor(store, clock=lambda: NOW).review_rolling_portfolio(
                    now=NOW,
                    force=True,
                )
            accounting.assert_called_once()
            self.assertEqual(state["external_obligations"], "10.00")
            self.assertEqual(state["available_budget"], "0")
            self.assertEqual(
                state["runtime_accounting_unavailable_reason"],
                "ROLLING_STRATEGY_RESERVATIONS_UNAVAILABLE",
            )
            selection = store.load_current_portfolio_selection()
            self.assertIsNotNone(selection)
            assert selection is not None
            self.assertEqual(selection["members"][0]["status"], "OBSERVE")
            self.assertEqual(selection["members"][0]["allocation"], "0")
            self.assertEqual(
                selection["runtime_accounting_unavailable_reason"],
                "ROLLING_STRATEGY_RESERVATIONS_UNAVAILABLE",
            )
            self.assertIn(
                "EXTERNAL_OBLIGATIONS_EXCEED_GLOBAL_BUDGET",
                selection["members"][0]["reason"],
            )

    def test_empty_runtime_reservation_accounting_fails_closed(self) -> None:
        policy = _policy(
            "policy-runtime-empty-accounting",
            max_members=1,
            global_budget="10.00",
            experimental_allocation_enabled=True,
            replacement_margin="0",
        )
        with self._store("rolling-runtime-empty-accounting.sqlite3") as store:
            self._seed_artifacts(
                store,
                policy,
                ("sv-runtime-empty-accounting",),
                evidence=[_evidence("sv-runtime-empty-accounting", net_return="20.00")],
            )
            active_policy = {**policy.as_dict(), "status": "ACTIVE"}
            store.set_operator_config("rolling_admission_policy_active", active_policy)
            store.set_operator_job(
                "rolling_admission_policy_active",
                "ACTIVE",
                active_policy,
                resumable=True,
                timestamp=NOW,
            )
            with patch.object(store, "canary_risk_accounting", return_value={}) as accounting:
                state = AutonomousResearchProcessor(store, clock=lambda: NOW).review_rolling_portfolio(
                    now=NOW,
                    force=True,
                )
            accounting.assert_called_once()
            self.assertEqual(state["external_obligations"], "10.00")
            self.assertEqual(state["available_budget"], "0")
            self.assertEqual(
                state["runtime_accounting_unavailable_reason"],
                "ROLLING_STRATEGY_RESERVATIONS_UNAVAILABLE",
            )
            selection = store.load_current_portfolio_selection()
            self.assertIsNotNone(selection)
            assert selection is not None
            self.assertEqual(selection["members"][0]["status"], "OBSERVE")
            self.assertEqual(selection["members"][0]["allocation"], "0")
            self.assertEqual(
                selection["runtime_accounting_unavailable_reason"],
                "ROLLING_STRATEGY_RESERVATIONS_UNAVAILABLE",
            )

    def test_retained_runtime_reservation_is_not_double_counted(self) -> None:
        policy = _policy(
            "policy-retained-runtime",
            max_members=2,
            global_budget="10.00",
            experimental_allocation_enabled=True,
        )
        current = _selection(
            "selection-retained-runtime",
            policy,
            selected_at=NOW - timedelta(days=2),
            members=[_member("sv-retained", "selection-retained-runtime", allocation="6.00")],
        )
        decision = evaluate_rolling_selection(
            policy,
            [
                _evidence("sv-retained", net_return="8.00"),
                _evidence("sv-new-retained", net_return="7.00"),
            ],
            current,
            NOW,
            external_obligations="0.00",
        )
        members = _member_map(decision)
        self.assertEqual(Decimal(str(_value(members["sv-retained"], "allocation"))), Decimal("6.00"))
        self.assertEqual(Decimal(str(_value(members["sv-new-retained"], "allocation"))), Decimal("4.00"))

    def test_excess_retained_runtime_reservation_reduces_new_allocation(self) -> None:
        policy = _policy(
            "policy-excess-runtime",
            max_members=2,
            global_budget="10.00",
            experimental_allocation_enabled=True,
        )
        current = _selection(
            "selection-excess-runtime",
            policy,
            selected_at=NOW - timedelta(days=2),
            members=[_member("sv-retained-excess", "selection-excess-runtime", allocation="5.00")],
        )
        decision = evaluate_rolling_selection(
            policy,
            [
                _evidence("sv-retained-excess", net_return="8.00"),
                _evidence("sv-new-excess", net_return="7.00"),
            ],
            current,
            NOW,
            external_obligations="3.00",
        )
        members = _member_map(decision)
        self.assertEqual(Decimal(str(_value(members["sv-retained-excess"], "allocation"))), Decimal("5.00"))
        self.assertEqual(Decimal(str(_value(members["sv-new-excess"], "allocation"))), Decimal("2.00"))
        self.assertEqual(
            sum((Decimal(str(_value(member, "allocation"))) for member in members.values()), Decimal("0"))
            + Decimal("3.00"),
            Decimal("10.00"),
        )

    def test_external_obligation_and_selection_allocations_never_exceed_budget(self) -> None:
        policy = _policy(
            "policy-external-total",
            max_members=2,
            global_budget="10.00",
            experimental_allocation_enabled=True,
        )
        decision = evaluate_rolling_selection(
            policy,
            [
                _evidence("sv-external-a", net_return="8.00"),
                _evidence("sv-external-b", net_return="7.00"),
            ],
            None,
            NOW,
            external_obligations="4.00",
        )
        allocations = [
            Decimal(str(_value(member, "allocation")))
            for member in _member_map(decision).values()
        ]
        self.assertEqual(sum(allocations, Decimal("0")) + Decimal("4.00"), Decimal("10.00"))

    def test_shared_budget_rejects_final_dollar_contention_atomically(self) -> None:
        policy = _policy("policy-budget", max_members=1, global_budget="10.00")
        with self._store() as store:
            self._seed_artifacts(store, policy, ("sv-budget",))
            member = _member("sv-budget", "selection-budget", allocation="10.00", score="0.80")
            store.commit_portfolio_selection(
                _selection("selection-budget", policy, members=[member]),
                [member],
            )
            settings = CanarySettingsService(store, clock=lambda: NOW)
            draft = settings.save_draft(
                {
                    "max_all_in_buy_usd": "20.00",
                    "max_gross_daily_buy_usd": "20.00",
                    "max_aggregate_exposure_usd": "20.00",
                    "max_aggregate_open_cost_usd": "20.00",
                    "max_submitted_orders_per_day": 20,
                },
                "acceptance",
            )
            settings.activate_draft(draft["config_id"], "acceptance", expected_generation=settings.snapshot(now=NOW)["generation"])
            lineage = self._lineage(
                strategy_version_id="sv-budget",
                selection_id="selection-budget",
                policy=policy,
                allocation="10.00",
            )
            first = self._reserve(store, "budget-first", cost="9.99", lineage=lineage)
            self.assertEqual(first["status"], "HELD")
            with self.assertRaisesRegex(ValueError, "global|budget|allocation|commitment"):
                self._reserve(store, "budget-final", cost="0.02", lineage=lineage)
            self.assertEqual(store.canary_risk_accounting(NOW)["all_in_buy_reserved_usd"], "9.99")
            self.assertTrue(settings.active_limits())
    def test_replaced_member_blocks_new_buy_but_preserves_opening_exit_lineage(self) -> None:
        policy = _policy("policy-opening-lineage", max_members=1, global_budget="10.00")
        old_member = _member("sv-opening-old", "selection-opening-old", allocation="10.00")
        new_member = _member("sv-opening-new", "selection-opening-new", allocation="10.00")
        old_lineage = self._lineage(
            strategy_version_id="sv-opening-old",
            selection_id="selection-opening-old",
            policy=policy,
            allocation="10.00",
        )
        with self._store() as store:
            self._activate_isolated_risk_settings(store)
            self._seed_artifacts(
                store,
                policy,
                ("sv-opening-old", "sv-opening-new"),
                evidence=[_evidence("sv-opening-old"), _evidence("sv-opening-new")],
            )
            store.commit_portfolio_selection(
                _selection("selection-opening-old", policy, members=[old_member]),
                [old_member],
            )
            entry = self._reserve(
                store,
                "opening-entry",
                market="opening-market",
                cost="0.50",
                lineage=old_lineage,
            )
            store.record_canary_fill(
                fill_id="opening-entry-fill",
                reservation_id=entry["reservation_id"],
                quantity="1",
                price="0.50",
                filled_at=NOW,
                **old_lineage,
                detail={"settlement": "SETTLED", "token_id": "yes"},
            )
            store.commit_portfolio_selection(
                _selection("selection-opening-new", policy, members=[new_member]),
                [new_member],
            )
            with self.assertRaisesRegex(ValueError, "selection is not current"):
                self._reserve(store, "opening-old-buy", lineage=old_lineage)
            opening_exit = self._reserve(
                store,
                "opening-old-sell",
                side="SELL",
                market="opening-market",
                quantity="1",
                lineage=old_lineage,
            )
            self.assertEqual(opening_exit["status"], "HELD")
            foreign_lineage = self._lineage(
                strategy_version_id="sv-opening-new",
                selection_id="selection-opening-old",
                policy=policy,
                allocation="10.00",
            )
            with self.assertRaisesRegex(ValueError, "strategy is not selected|research trial"):
                self._reserve(
                    store,
                    "foreign-opening-sell",
                    side="SELL",
                    market="opening-market",
                    quantity="1",
                    lineage=foreign_lineage,
                )

    def test_position_service_allows_paused_replaced_opening_lineage_sell(self) -> None:
        policy = _policy(
            "policy-position-lineage",
            config_hash=hashlib.sha256(b"policy-position-lineage").hexdigest(),
            max_members=1,
            global_budget="10.00",
        )
        old_member = _member("sv-position-old", "selection-position-old", allocation="10.00")
        new_member = _member("sv-position-new", "selection-position-new", allocation="10.00")
        old_lineage = self._lineage(
            strategy_version_id="sv-position-old",
            selection_id="selection-position-old",
            policy=policy,
            allocation="10.00",
        )
        venue = FakeVenue(("FILLED",))
        with self._store() as store:
            settings = self._activate_isolated_risk_settings(store)
            self._seed_artifacts(
                store,
                policy,
                ("sv-position-old", "sv-position-new"),
                evidence=[_evidence("sv-position-old"), _evidence("sv-position-new")],
            )
            old_selection = _selection(
                "selection-position-old",
                policy,
                members=[old_member],
            )
            old_selection["execution_authorization_mode"] = "EVIDENCE_SELECTED"
            old_selection["policy_hash"] = policy.config_hash
            old_selection["selection_hash"] = _selection_binding_hash(old_selection)
            store.commit_portfolio_selection(old_selection, [old_member])
            lease = store.acquire_canary_controller_lease(
                owner_id="position-manager",
                lease_seconds=86400,
                now=NOW,
            )
            authorization = self._activate_worker_authorization(
                settings,
                strategy_version_id="sv-position-old",
                selection_id="selection-position-old",
                selection_hash=str(old_selection["selection_hash"]),
                selection_policy_hash=policy.config_hash,
                mode="EVIDENCE_SELECTED",
            )
            opening_lineage = {
                **old_lineage,
                "execution_authorization_id": authorization["authorization_id"],
                "controller_owner_id": lease["owner_id"],
                "controller_generation": int(lease["generation"]),
            }
            entry = self._reserve(
                store,
                "position-opening-entry",
                market="position-opening-market",
                cost="0.50",
                lineage=opening_lineage,
                compatibility_mode=False,
            )
            store.record_canary_fill(
                fill_id="position-opening-entry-fill",
                reservation_id=entry["reservation_id"],
                quantity="1",
                price="0.50",
                filled_at=NOW,
                execution_authorization_id=str(authorization["authorization_id"]),
                controller_owner_id=str(lease["owner_id"]),
                controller_generation=int(lease["generation"]),
                **old_lineage,
                detail={
                    "settlement": "SETTLED",
                    "token_id": "yes",
                    "execution_authorization_mode": "EVIDENCE_SELECTED",
                },
            )
            config = settings.snapshot(now=NOW)
            service = CanaryService(
                store,
                credentials=AcceptanceCredentials(),
                clock=lambda: NOW,
                settings=settings,
                controller_owner_id=str(lease["owner_id"]),
            )
            service.controller_generation = int(lease["generation"])
            service.execution_authorization_id = str(authorization["authorization_id"])
            service.execution_authorization_mode = "EVIDENCE_SELECTED"
            credential_hash = credential_fingerprint(AcceptanceCredentials().load())
            with store.connection:
                store.connection.execute(
                    """
                    INSERT INTO canary_control(
                        singleton, state, candidate_id, venue, armed_at, expires_at,
                        limits_json, integrity_hash, updated_at, control_generation,
                        settings_config_id, settings_generation, credential_fingerprint
                    ) VALUES(1, 'PAUSED', NULL, 'polymarket', ?, ?, '{}', '', ?, ?, ?, ?, ?)
                    """,
                    (
                        NOW.isoformat(),
                        (NOW + timedelta(days=1)).isoformat(),
                        NOW.isoformat(),
                        1,
                        config["config_id"],
                        config["generation"],
                        credential_hash,
                    ),
                )
            list_positions(service)
            with store.connection:
                store.connection.execute(
                    """
                    INSERT INTO canary_position_lots(
                        position_id, reservation_id, event_id, venue, market_id, token_id,
                        asset_id, market_version, candidate_id, strategy_id,
                        strategy_version, strategy_hash, model_hash, config_id,
                        config_generation, strategy_version_id, research_trial_id,
                        portfolio_selection_id, admission_policy_id,
                        admission_policy_version, risk_config_id,
                        risk_config_generation, risk_config_hash,
                        execution_authorization_id, execution_authorization_mode,
                        controller_owner_id, controller_generation, lineage_type,
                        exit_policy_json, quantity, sold_quantity, cost_basis, fees,
                        pending_exit_quantity, status, opened_at, updated_at
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        "position:position-opening-entry",
                        entry["reservation_id"],
                        "event:position-opening-entry",
                        "PAPER",
                        "position-opening-market",
                        "yes",
                        "yes",
                        "v1",
                        "candidate-sv-position-old",
                        "strategy-position-old",
                        "1",
                        "sha256:strategy-position-old",
                        "sha256:model-position-old",
                        config["config_id"],
                        config["generation"],
                        old_lineage["strategy_version_id"],
                        old_lineage["research_trial_id"],
                        old_lineage["portfolio_selection_id"],
                        old_lineage["admission_policy_id"],
                        old_lineage["admission_policy_version"],
                        old_lineage["risk_config_id"],
                        old_lineage["risk_config_generation"],
                        old_lineage["risk_config_hash"],
                        authorization["authorization_id"],
                        "EVIDENCE_SELECTED",
                        lease["owner_id"],
                        lease["generation"],
                        "ROLLING_PORTFOLIO",
                        '{"holding_period_seconds": 0, "type": "fixed_holding_period"}',
                        "1",
                        "0",
                        "0.50",
                        "0",
                        "0",
                        "OPEN",
                        NOW.isoformat(),
                        NOW.isoformat(),
                    ),
                )
            opening = store.connection.execute(
                """
                SELECT execution_authorization_id, execution_authorization_mode,
                       controller_owner_id, controller_generation
                FROM canary_position_lots
                WHERE position_id=?
                """,
                ("position:position-opening-entry",),
            ).fetchone()
            self.assertIsNotNone(opening)
            assert opening is not None
            self.assertEqual(
                opening["execution_authorization_id"],
                authorization["authorization_id"],
            )
            self.assertEqual(opening["execution_authorization_mode"], "EVIDENCE_SELECTED")
            self.assertEqual(opening["controller_owner_id"], lease["owner_id"])
            self.assertEqual(opening["controller_generation"], lease["generation"])
            self.assertEqual(service.controller_owner_id, lease["owner_id"])
            self.assertEqual(service.controller_generation, lease["generation"])
            self.assertEqual(
                service.execution_authorization_id,
                authorization["authorization_id"],
            )
            self.assertEqual(service.execution_authorization_mode, "EVIDENCE_SELECTED")
            paused_old = dict(old_member)
            paused_old["status"] = "PAUSED"
            paused_old["allocation"] = "0"
            replacement_selection = _selection(
                "selection-position-new",
                policy,
                members=[paused_old, new_member],
            )
            replacement_selection["execution_authorization_mode"] = "EVIDENCE_SELECTED"
            replacement_selection["policy_hash"] = policy.config_hash
            replacement_selection["selection_hash"] = _selection_binding_hash(
                replacement_selection
            )
            store.commit_portfolio_selection(
                replacement_selection,
                [paused_old, new_member],
            )
            with store.connection:
                store.connection.execute(
                    """
                    UPDATE canary_position_lots
                    SET portfolio_selection_id=?
                    WHERE position_id=?
                    """,
                    ("selection-position-new", "position:position-opening-entry"),
                )
            replacement_authorization = self._activate_worker_authorization(
                settings,
                strategy_version_id="sv-position-old",
                selection_id="selection-position-new",
                selection_hash=str(replacement_selection["selection_hash"]),
                selection_policy_hash=policy.config_hash,
                mode="EVIDENCE_SELECTED",
            )
            service.execution_authorization_id = str(
                replacement_authorization["authorization_id"]
            )
            with store.connection:
                store.connection.execute(
                    """
                    UPDATE canary_position_lots
                    SET execution_authorization_id=?,
                        execution_authorization_mode=?
                    WHERE position_id=?
                    """,
                    (
                        replacement_authorization["authorization_id"],
                        "EVIDENCE_SELECTED",
                        "position:position-opening-entry",
                    ),
                )
            manager = CanaryPositionManager(service)
            submitted = manager.submit_exit(
                "position:position-opening-entry",
                venue,
                expected_generation=int(config["generation"]),
                config_id=str(config["config_id"]),
                allow_test_venue=True,
            )
            self.assertEqual(submitted["status"], "FILLED")
            self.assertEqual(submitted["lineage"]["strategy_version_id"], "sv-position-old")
            self.assertEqual(submitted["lineage"]["portfolio_selection_id"], "selection-position-new")
            request = store.connection.execute(
                """
                SELECT side, strategy_version_id, portfolio_selection_id, lineage_type
                FROM canary_position_requests
                WHERE position_id=?
                """,
                ("position:position-opening-entry",),
            ).fetchone()
            self.assertIsNotNone(request)
            assert request is not None
            self.assertEqual(request["side"], "SELL")
            self.assertEqual(request["strategy_version_id"], "sv-position-old")
            self.assertEqual(request["portfolio_selection_id"], "selection-position-new")
            self.assertEqual(request["lineage_type"], "ROLLING_PORTFOLIO")
            self.assertEqual([row["side"] for row in venue.submissions], ["SELL"])

    def test_foreign_lot_sell_is_rejected_without_sink_submission(self) -> None:
        venue = FakeVenue()
        with self._store() as store:
            self._activate_isolated_risk_settings(store)
            with self.assertRaisesRegex(ValueError, "owned inventory|owned lot|inventory"):
                self._reserve(store, "foreign-exit", side="SELL", market="foreign-market", quantity="1")
            self.assertEqual(venue.submissions, [])
            self.assertEqual(store.canary_risk_accounting(NOW)["open_positions"], 0)

    def test_rejected_canceled_partial_and_unknown_statuses_are_persisted(self) -> None:
        with self._store() as store:
            self._activate_isolated_risk_settings(store)
            rejected = self._reserve(store, "rejected", cost="0.01")
            store.record_canary_submission_attempt(
                attempt_id="attempt:rejected",
                intent_id="rejected",
                side="BUY",
                attempted_at=NOW,
                status="REJECTED",
                compatibility_mode=True,
            )
            self.assertEqual(
                store.release_canary_capacity(
                    rejected["reservation_id"],
                    status="REJECTED",
                    timestamp=NOW,
                )["status"],
                "REJECTED",
            )

            canceled = self._reserve(store, "canceled", cost="0.01")
            store.record_canary_submission_attempt(
                attempt_id="attempt:canceled",
                intent_id="canceled",
                side="BUY",
                attempted_at=NOW,
                status="CANCELED",
                compatibility_mode=True,
            )
            self.assertEqual(
                store.release_canary_capacity(
                    canceled["reservation_id"],
                    status="CANCELED",
                    timestamp=NOW,
                )["status"],
                "CANCELED",
            )

            partial = self._reserve(store, "partial", cost="0.01")
            store.record_canary_submission_attempt(
                attempt_id="attempt:partial",
                intent_id="partial",
                side="BUY",
                attempted_at=NOW,
                status="PARTIALLY_FILLED",
                compatibility_mode=True,
            )

            # Reconcile the unknown attempt before applying the confirmed
            # partial fill.  The fill intentionally remains open for its
            # status assertion, but it must not make this unrelated BUY
            # reservation depend on an equity mark.
            unknown = self._reserve(store, "unknown", cost="0.01")
            store.record_canary_submission_attempt(
                attempt_id="attempt:unknown",
                intent_id="unknown",
                side="BUY",
                attempted_at=NOW,
                status="UNKNOWN",
                compatibility_mode=True,
            )
            self.assertEqual(
                store.release_canary_capacity(
                    unknown["reservation_id"],
                    status="UNKNOWN",
                    timestamp=NOW,
                )["status"],
                "UNKNOWN",
            )

            partial_row = store.record_canary_fill(
                fill_id="fill-partial",
                reservation_id=partial["reservation_id"],
                quantity="0.5",
                price="0.01",
                filled_at=NOW,
                detail={"settlement": "SETTLED"},
            )
            self.assertEqual(partial_row["status"], "PARTIALLY_FILLED")

            rows = {
                row["reservation_id"]: row["status"]
                for row in store.connection.execute(
                    "SELECT reservation_id,status FROM canary_risk_reservations"
                )
            }
            self.assertEqual(rows[rejected["reservation_id"]], "REJECTED")
            self.assertEqual(rows[canceled["reservation_id"]], "CANCELED")
            self.assertEqual(rows[partial["reservation_id"]], "PARTIALLY_FILLED")
            self.assertEqual(rows[unknown["reservation_id"]], "UNKNOWN")

            attempts = {
                row["intent_id"]: row["status"]
                for row in store.connection.execute(
                    "SELECT intent_id,status FROM canary_submission_attempts"
                )
            }
            self.assertEqual(attempts["rejected"], "REJECTED")
            self.assertEqual(attempts["canceled"], "CANCELED")
            self.assertEqual(attempts["partial"], "PARTIALLY_FILLED")
            self.assertEqual(attempts["unknown"], "UNKNOWN")

    def _attempts_until_gate(self, store: AxiomStore, limit: int, *, side: str, now: datetime) -> None:
        settings = CanarySettingsService(store, clock=lambda: now)
        observed = settings.snapshot(now=now)
        # A BUY reservation commits both the entry and its eventual exit slot.
        # Give the reservation phase one slot of headroom, then tighten the
        # active daily limit before exercising the blocked operation.
        reserve_limit = limit + 1
        draft = settings.save_draft(
            {"max_submitted_orders_per_day": reserve_limit},
            "acceptance",
            expected_generation=int(observed["generation"]),
        )
        settings.activate_draft(
            draft["config_id"],
            "acceptance",
            expected_generation=int(observed["generation"]),
        )
        active = settings.snapshot(now=now)
        successful_attempts: list[str] = []
        for index in range(limit):
            intent = f"gate-{side.lower()}-{limit}-{index}"
            reservation = self._reserve(
                store,
                intent,
                side=side,
                now=now,
                market="gate-market" if side == "SELL" else None,
            )
            attempt_id = store.record_canary_submission_attempt(
                attempt_id=f"attempt:{intent}",
                intent_id=intent,
                side=side,
                attempted_at=now,
                status="ATTEMPTED",
                config_id=active["config_id"],
                config_generation=active["generation"],
                config_hash=active["config_hash"],
                compatibility_mode=True,
            )
            successful_attempts.append(attempt_id)
            released = store.release_canary_capacity(
                reservation["reservation_id"],
                status="REJECTED",
                timestamp=now,
            )
            self.assertEqual(released["status"], "REJECTED")
        self.assertEqual(len(successful_attempts), limit)

        observed = settings.snapshot(now=now)
        draft = settings.save_draft(
            {"max_submitted_orders_per_day": limit},
            "acceptance",
            expected_generation=int(observed["generation"]),
        )
        settings.activate_draft(
            draft["config_id"],
            "acceptance",
            expected_generation=int(observed["generation"]),
        )
        active = settings.snapshot(now=now)
        self.assertEqual(
            int(active["effective_limits"]["max_submitted_orders_per_day"]),
            limit,
        )
        with self.assertRaisesRegex(ValueError, "daily|capacity|limit"):
            self._reserve(
                store,
                f"gate-{side.lower()}-{limit}-blocked",
                side=side,
                now=now,
                market="gate-market" if side == "SELL" else None,
            )

    def test_actual_global_five_and_twenty_daily_gates_cover_buy_and_sell(self) -> None:
        scenarios = (
            ("BUY", 5, NOW),
            ("SELL", 5, NOW + timedelta(days=1)),
            ("BUY", 20, NOW + timedelta(days=2)),
            ("SELL", 20, NOW + timedelta(days=3)),
        )
        for side, limit, now in scenarios:
            with self.subTest(side=side, limit=limit):
                with self._store(f"gate-{side.lower()}-{limit}.sqlite3") as store:
                    if side == "SELL":
                        self._seed_owned_inventory(store, market="gate-market", now=now)
                    self._attempts_until_gate(store, limit, side=side, now=now)

    def test_restart_restores_selection_allocation_cursor_schedule_and_ownership(self) -> None:
        path = self.path / "restart.sqlite3"
        policy = _policy(max_members=2, global_budget="10.00", experimental_allocation_enabled=True)
        members = [_member("sv-a", "selection-restart", allocation="6.00"), _member("sv-b", "selection-restart", allocation="4.00")]
        state = {
            "portfolio_selection_id": "selection-restart",
            "cursor": 1,
            "schedule": {"next_review_at": (NOW + timedelta(hours=6)).isoformat()},
            "ownership": {"owner": "rolling-worker-1", "lease_generation": 3},
        }
        with AxiomStore(str(path)) as first:
            self._seed_artifacts(first, policy, ("sv-a", "sv-b"), evidence=[_evidence("sv-a"), _evidence("sv-b")])
            first.commit_portfolio_selection(_selection("selection-restart", policy, members=members), members)
            first.save_worker_state("rolling-portfolio", "RUNNING", state, started_at=NOW, heartbeat_at=NOW)
            first.set_scheduler_state("rolling-portfolio", state["schedule"])
            first.set_operator_config("rolling_portfolio_cursor", {"selection-restart": 1})
        with AxiomStore(str(path)) as restarted:
            selection = restarted.load_current_portfolio_selection()
            worker_state = restarted.get_worker_state("rolling-portfolio")
            schedule = restarted.get_scheduler_state("rolling-portfolio")
            worker = AutonomousCanaryWorker(
                restarted,
                clock=lambda: NOW,
                venue_factory=lambda: FakeVenue(),
                allow_test_venue=True,
            )
            with patch.object(CredentialStore, "configured", return_value=False):
                tick = worker.tick_rolling(now=NOW)
            self.assertEqual(selection["portfolio_selection_id"], "selection-restart")
            self.assertEqual({row["allocation"] for row in selection["members"]}, {"6.00", "4.00"})
            self.assertEqual(worker_state["payload"]["cursor"], 1)
            self.assertEqual(worker_state["payload"]["ownership"]["owner"], "rolling-worker-1")
            self.assertEqual(schedule["next_review_at"], state["schedule"]["next_review_at"])
            # Restart restores the persisted portfolio but remains in the
            # continuous observing state until a current selection is ready.
            self.assertEqual(tick["status"], "OBSERVING")
            self.assertEqual(tick["decision"], "WAIT_FOR_ROLLING_SELECTION")
            self.assertIsNone(tick["blocker"])
            self.assertEqual(tick["submissions"], [])
            self.assertEqual(tick["rolling_cursor"], 0)

    def test_exhausted_batch_loop_continues_from_persisted_cursor(self) -> None:
        policy = _policy("policy-cursor", max_members=2, global_budget="10.00")
        members = [
            _member("sv-cursor-a", "selection-cursor", allocation="5.00"),
            _member("sv-cursor-b", "selection-cursor", allocation="5.00"),
        ]
        with self._store() as store:
            settings = self._activate_isolated_risk_settings(store)
            controller = CanaryService(
                store,
                credentials=AcceptanceCredentials(),
                clock=lambda: NOW,
                settings=settings,
            )
            config = settings.snapshot(now=NOW)
            enabled = controller.enable_autonomous_micro_live(
                "polymarket",
                config_id=str(config["config_id"]),
                expected_generation=int(config["generation"]),
            )
            self.assertEqual(enabled["micro_live_canary"], "AUTONOMOUS_MICRO_LIVE")
            venue = FakeVenue()
            self._seed_artifacts(
                store,
                policy,
                ("sv-cursor-a", "sv-cursor-b"),
                evidence=[_evidence("sv-cursor-a"), _evidence("sv-cursor-b")],
            )
            active_policy = {
                "policy_id": policy.policy_id,
                "version": policy.version,
                "policy_version": policy.version,
                "config_hash": policy.config_hash,
                "risk_config_id": config["config_id"],
                "risk_config_generation": config["generation"],
                "risk_config_hash": config["config_hash"],
                "status": "ACTIVE",
                "paper_only": True,
            }
            store.set_operator_config("rolling_admission_policy_active", active_policy)
            store.set_operator_job(
                "rolling_admission_policy_active",
                "ACTIVE",
                active_policy,
                resumable=True,
                timestamp=NOW,
            )
            selection = _selection("selection-cursor", policy, members=members)
            selection.update(
                {
                    "policy_hash": policy.config_hash,
                    "risk_config_id": config["config_id"],
                    "risk_config_generation": config["generation"],
                    "risk_config_hash": config["config_hash"],
                }
            )
            store.commit_portfolio_selection(selection, members)
            worker = AutonomousCanaryWorker(
                store,
                clock=lambda: NOW,
                venue_factory=lambda: venue,
                allow_test_venue=True,
            )
            with patch.object(CredentialStore, "configured", return_value=False):
                first = worker.tick(now=NOW)
                second = worker.tick(now=NOW)
            self.assertEqual(first["evaluated_members"], 2)
            self.assertEqual(second["evaluated_members"], 2)
            self.assertEqual(first["rolling_cursor"], 1)
            self.assertEqual(second["rolling_cursor"], 0)
            first_order = [row["strategy_version_id"] for row in first["evaluated"]]
            second_order = [row["strategy_version_id"] for row in second["evaluated"]]
            self.assertEqual(first_order, ["sv-cursor-a", "sv-cursor-b"])
            self.assertEqual(second_order, ["sv-cursor-b", "sv-cursor-a"])
            self.assertEqual(venue.submissions, [])

    def test_same_fake_venue_can_be_qualified_then_used_for_fill_and_exit_accounting(self) -> None:
        venue = FakeVenue(("FILLED", "FILLED"))
        with self._store() as store:
            settings = self._activate_isolated_risk_settings(store)
            with patch.object(CredentialStore, "configured", return_value=True):
                readiness = CanaryService(
                    store,
                    credentials=AcceptanceCredentials(),
                    clock=lambda: NOW,
                ).check(venue=venue, _connectivity_only=True)
            self.assertTrue(readiness["diagnostics"]["geoblock"]["blocked"] is False)
            buy = self._reserve(store, "roundtrip-buy", market="roundtrip-market", cost="0.50")

            submitted = venue.submit_limit_order(token_id="yes", side="BUY", price="0.50", size="1")
            self.assertEqual(submitted["status"], "FILLED")
            store.record_canary_submission_attempt(
                attempt_id="attempt:roundtrip-buy",
                intent_id="roundtrip-buy",
                side="BUY",
                attempted_at=NOW,
                status="SUBMITTED",
                config_id=settings.snapshot(now=NOW)["config_id"],
                config_generation=settings.snapshot(now=NOW)["generation"],
                config_hash=settings.snapshot(now=NOW)["config_hash"],
                compatibility_mode=True,
            )
            store.record_canary_fill(
                fill_id="fill-roundtrip-buy",
                reservation_id=buy["reservation_id"],
                quantity="1",
                price="0.50",
                filled_at=NOW,
                detail={"settlement": "SETTLED"},
            )
            accounting = store.canary_risk_accounting(NOW)
            self.assertEqual(accounting["open_quantity_by_market"]["roundtrip-market"], "1")
            self.assertEqual(len(venue.submissions), 1)
            # The exit request is represented by the same exact market/token-owned
            # inventory and a separate venue submission; no foreign inventory can be used.
            sell = self._reserve(store, "roundtrip-sell", side="SELL", market="roundtrip-market", cost="0.50", quantity="1")
            exit_result = venue.submit_limit_order(token_id="yes", side="SELL", price="0.49", size="1")
            self.assertEqual(exit_result["status"], "FILLED")
            sell_fill = store.record_canary_fill(
                fill_id="fill-roundtrip-sell",
                reservation_id=sell["reservation_id"],
                quantity="1",
                price="0.49",
                filled_at=NOW,
                detail={"settlement": "SETTLED", "token_id": "yes"},
            )
            self.assertEqual(sell_fill["status"], "FILLED")
            self.assertEqual(store.canary_risk_accounting(NOW)["open_quantity_by_market"]["roundtrip-market"], "0")
            self.assertEqual(len(venue.submissions), 2)

    def test_walk_forward_evidence_cannot_use_future_availability(self) -> None:
        policy = _policy(min_score="0.00")
        future = _evidence("sv-future")
        future["available_from"] = (NOW + timedelta(hours=1)).isoformat()
        future["available_through"] = (NOW + timedelta(days=8)).isoformat()
        future["actual_coverage_seconds"] = 7 * 86400
        future = _canonicalize_evidence(future)
        decision = evaluate_rolling_selection(policy, [future], None, NOW)
        members = _member_map(decision)
        future_member = members["sv-future"]
        self.assertNotEqual(_value(future_member, "status"), "ACTIVE")
        self.assertEqual(Decimal(str(_value(future_member, "allocation"))), Decimal("0"))
        self.assertIn("FUTURE_EVIDENCE", str(_value(future_member, "reason")))

    def test_cash_and_nonrotating_baselines_preserve_turnover_cost_boundary(self) -> None:
        policy = _policy(min_score="0.00", max_members=1, replacement_margin="0.10")
        cash = evaluate_rolling_selection(policy, [], None, NOW)
        self.assertEqual(getattr(cash, "members", ()), ())
        current = _selection(
            "selection-baseline",
            policy,
            selected_at=NOW - timedelta(days=2),
            members=[_member("sv-flat", "selection-baseline", allocation="10.00", score="0.40")],
        )
        stable = evaluate_rolling_selection(policy, [_evidence("sv-flat", net_return="0.00", drawdown="0.01")], current, NOW)
        stable_member = _member_map(stable)["sv-flat"]
        self.assertEqual(Decimal(str(_value(stable_member, "allocation"))), Decimal("10.00"))
        no_rotation_score = RollingEvidence.from_mapping(_evidence("sv-new", net_return="8.00", costs="0.00")).score(policy)
        rotated_score = RollingEvidence.from_mapping(_evidence("sv-new-costly", net_return="0.00", costs="8.00")).score(policy)
        self.assertGreater(no_rotation_score, rotated_score)

    def test_rolling_horizon_excludes_outside_seven_day_outcomes_but_keeps_thirty_day(self) -> None:
        rows = [
            {
                "market_id": "resolved-ten-days-ago",
                "timestamp": (NOW - timedelta(days=10)).isoformat(),
                "outcome": "resolved_yes",
            },
            {
                "market_id": "resolved-two-days-ago",
                "timestamp": (NOW - timedelta(days=2)).isoformat(),
                "outcome": "resolved_no",
            },
        ]
        seven_day, seven_rejections = _rolling_window_rows(rows, 7, NOW)
        thirty_day, thirty_rejections = _rolling_window_rows(rows, 30, NOW)
        self.assertEqual(
            [row["market_id"] for row in seven_day],
            ["resolved-two-days-ago"],
        )
        self.assertEqual(
            [row["market_id"] for row in thirty_day],
            ["resolved-ten-days-ago", "resolved-two-days-ago"],
        )
        self.assertEqual(seven_rejections, ())
        self.assertEqual(thirty_rejections, ())

    def test_source_class_separation_keeps_historical_and_forward_rows_distinct(self) -> None:
        policy = _policy()
        with self._store() as store:
            self._seed_artifacts(
                store,
                policy,
                ("sv-source",),
                evidence=[_evidence("sv-source", "window-historical", source_class="HISTORICAL")],
            )
            store.save_strategy_evidence_window(_evidence("sv-source", "window-forward", source_class="FORWARD_COLLECTED"))
            rows = store.list_strategy_evidence_windows("sv-source", limit=10)
            self.assertEqual({row["source_class"] for row in rows}, {"HISTORICAL", "FORWARD_COLLECTED"})
            historical = [row for row in rows if row["source_class"] == "HISTORICAL"]
            self.assertEqual(len(historical), 1)
            decision = evaluate_rolling_selection(policy, historical, None, NOW)
            self.assertEqual(_value(_member_map(decision)["sv-source"], "status"), "PAPER")

    def test_processor_persists_default_policy_and_empty_selection_after_terminal_work(self) -> None:
        with self._store() as store:
            processor = AutonomousResearchProcessor(store, clock=lambda: NOW)
            result = processor.review_rolling_portfolio(now=NOW, force=True)
            self.assertIsInstance(result, dict)
            policy = store.load_admission_policy("rolling-default", "rolling-admission-v1")
            self.assertIsNotNone(policy)
            selection = store.load_current_portfolio_selection()
            self.assertIsNotNone(selection)
            self.assertEqual(selection["members"], [])
            self.assertIsNotNone(store.load_portfolio_review_state())

    def test_dashboard_rolling_payload_is_bounded_and_truthful(self) -> None:
        with self._store() as store:
            policy = _policy()
            self._seed_artifacts(store, policy, ("sv-dashboard",), evidence=[_evidence("sv-dashboard")])
            store.commit_portfolio_selection(
                _selection("selection-dashboard", policy, members=[_member("sv-dashboard", "selection-dashboard")]),
                [_member("sv-dashboard", "selection-dashboard")],
            )
            payload = DashboardData(store=store, clock=lambda: NOW).rolling_portfolio_data()
            self.assertIsInstance(payload, dict)
            for key in (
                "controller_status",
                "k",
                "actual_k",
                "actionable",
                "policy",
                "active_policy",
                "reviewed_policy",
                "proposed_policy",
                "policy_review",
                "allocation_review",
                "risk",
                "active_rows",
                "global_limits",
                "event_history",
                "next_jobs",
                "cold_start_requirements",
            ):
                self.assertIn(key, payload)
            self.assertLessEqual(len(payload["active_rows"]), 10)
            self.assertFalse(payload.get("live_execution", True))
    def test_dashboard_exact_rolling_worker_lookup_survives_worker_pagination(self) -> None:
        with self._store("rolling-worker-pagination.sqlite3") as store:
            for index in range(33):
                store.save_worker_state(
                    f"a-earlier-worker-{index:02d}",
                    "IDLE",
                    {"worker_status": "IDLE"},
                    heartbeat_at=NOW,
                )
            heartbeat = NOW - timedelta(seconds=5)
            store.save_worker_state(
                "rolling-portfolio",
                "IDLE",
                {
                    "worker_status": "SCHEDULED",
                    "scheduled": True,
                    "configured_interval_seconds": 300.0,
                    "evidence_interval_seconds": 300.0,
                    "review_interval_seconds": 86_400.0,
                    "next_work": "refresh_rolling_evidence",
                    "last_error": None,
                    "last_error_code": None,
                },
                started_at=NOW - timedelta(minutes=5),
                heartbeat_at=heartbeat,
            )

            payload = DashboardData(
                store=store,
                clock=lambda: NOW,
            ).v2_snapshot("rolling-portfolio")
            controller = payload["controller"]

            self.assertEqual(payload["controller_status"], "SCHEDULED")
            self.assertNotIn(payload["controller_status"], {"COLD_START", "NOT_INITIALIZED"})
            self.assertEqual(controller["worker_name"], "rolling-portfolio")
            self.assertEqual(controller["status"], "SCHEDULED")
            self.assertTrue(controller["scheduled"])
            self.assertEqual(controller["interval_seconds"], 300.0)
            self.assertEqual(controller["cadence_seconds"], 300.0)
            self.assertEqual(controller["heartbeat_at"], heartbeat.isoformat())
            self.assertEqual(controller["next_work"], "refresh_rolling_evidence")
            self.assertEqual(payload["active_policy"], {})
            self.assertEqual(payload["signal"]["status"], "UNKNOWN")


    def test_dashboard_projects_persisted_reviewed_values_only_draft_without_active_policy(self) -> None:
        path = self.path / "rolling-dashboard-reviewed-values.sqlite3"
        with AxiomStore(str(path)) as store:
            settings = self._activate_isolated_risk_settings(store)
            default_policy = default_rolling_admission_policy()
            store.save_admission_policy(default_policy.as_dict())
            binding = settings.snapshot(now=NOW)
            selection = {
                "portfolio_selection_id": "selection-rolling-default",
                "policy_id": default_policy.policy_id,
                "policy_version": default_policy.version,
                "risk_config_id": binding["config_id"],
                "risk_config_generation": binding["generation"],
                "risk_config_hash": binding["config_hash"],
                "selected_at": NOW.isoformat(),
                "review_due_at": (NOW + timedelta(days=1)).isoformat(),
                "k": 0,
                "global_budget": "0",
                "members": [],
            }
            store.commit_portfolio_selection(selection, [])
            operator = OperatorControlPlane(store, settings_service=settings)
            reviewed = operator.review_rolling_admission_policy(
                {},
                actor="production-reviewer",
                expected_risk_config_id=binding["config_id"],
                expected_risk_config_generation=binding["generation"],
                expected_risk_config_hash=binding["config_hash"],
            )
            persisted_review = store.get_operator_config(
                "rolling_admission_policy_review",
                None,
            )
            self.assertIsInstance(persisted_review, dict)
            self.assertIsNone(
                store.get_operator_config("rolling_admission_policy_active", None)
            )
            assert isinstance(persisted_review, dict)
            draft = reviewed["draft"]
            dashboard = DashboardData(
                store=store,
                settings_service=settings,
                clock=lambda: NOW,
            ).rolling_portfolio_data()

            self.assertEqual(dashboard["policy_review"]["status"], "REVIEWED")
            self.assertEqual(dashboard["active_policy"], {})
            self.assertEqual(dashboard["policy"], {})
            proposed = dashboard["policy_review"]["proposed"]
            self.assertEqual(proposed["policy_id"], persisted_review["policy_id"])
            self.assertEqual(proposed["version"], persisted_review["version"])
            self.assertEqual(proposed["config_hash"], persisted_review["config_hash"])
            self.assertEqual(proposed["draft_id"], draft["draft_id"])
            self.assertEqual(proposed["draft_version"], draft["draft_version"])
            self.assertEqual(
                dashboard["policy_review"]["proposed_canary_binding"]["risk_config_id"],
                dashboard["policy_review"]["canary_binding"]["risk_config_id"],
            )
            self.assertEqual(
                str(
                    dashboard["policy_review"]["proposed_canary_binding"][
                        "risk_config_generation"
                    ]
                ),
                str(
                    dashboard["policy_review"]["canary_binding"][
                        "risk_config_generation"
                    ]
                ),
            )
            self.assertEqual(
                dashboard["policy_review"]["proposed_canary_binding"]["risk_config_hash"],
                dashboard["policy_review"]["canary_binding"]["risk_config_hash"],
            )
            self.assertTrue(
                dashboard["policy_review"]["active_vs_proposed"][
                    "requires_explicit_activation"
                ]
            )
            self.assertEqual(dashboard["selection"]["policy_id"], "rolling-default")
            self.assertIn("active_policy_id_unavailable", dashboard["selection"]["blockers"])
            self.assertNotIn("reviewed_immutable_policy_unavailable", dashboard["selection"]["blockers"])
            self.assertTrue(dashboard["paper_only"])
            self.assertFalse(dashboard["live_execution"])

            changed = settings.save_draft(
                {"max_orders_per_day": 19},
                "production-risk-change",
                expected_generation=int(binding["generation"]),
            )
            settings.activate_draft(
                changed["config_id"],
                "production-risk-change",
                expected_generation=int(binding["generation"]),
            )
            stale_risk = DashboardData(
                store=store,
                settings_service=settings,
                clock=lambda: NOW,
            ).rolling_portfolio_data()
            self.assertEqual(stale_risk["policy_review"]["status"], "STALE")

            store.set_operator_config(
                "rolling_admission_policy_review",
                {**persisted_review, "policy_id": "forged-policy-id"},
            )
            stale_identity = DashboardData(
                store=store,
                settings_service=settings,
                clock=lambda: NOW,
            ).rolling_portfolio_data()
            self.assertEqual(stale_identity["policy_review"]["status"], "STALE")


    def test_actual_rolling_worker_reconciles_fill_and_paused_replaced_exit_lineage(self) -> None:
        venue = RollingLifecycleVenue(("FILLED", "SETTLED"))
        long_exit = {"type": "fixed_holding_period", "holding_period_seconds": 86400}
        with self._store("rolling-worker-lifecycle.sqlite3") as store:
            policy, service, worker = self._rolling_worker_fixture(
                store,
                policy_id="policy-worker-lifecycle",
                candidate_id="candidate-sv-worker-old",
                selection_id="selection-worker-old",
                venue=venue,
                exit_policy=long_exit,
            )
            with patch.object(CredentialStore, "configured", return_value=True):
                first = worker.tick_rolling(now=NOW)
            self.assertEqual(first["status"], "SUBMITTED", first)
            self.assertEqual(first["decision"], "SUBMITTED", first)
            self.assertIsNone(first["blocker"], first)
            self.assertEqual(first["evaluated_members"], 1, first)
            self.assertEqual([row["side"] for row in venue.submissions], ["BUY"], first)
            reservation = store.connection.execute(
                """
                SELECT reservation_id, intent_id, execution_authorization_id,
                       controller_owner_id, controller_generation
                FROM canary_risk_reservations
                ORDER BY rowid DESC LIMIT 1
                """
            ).fetchone()
            self.assertIsNotNone(reservation)
            assert reservation is not None
            self.assertEqual(
                reservation["execution_authorization_id"],
                first["execution_authorization_id"],
            )
            self.assertEqual(reservation["controller_owner_id"], first["controller_owner_id"])
            self.assertEqual(
                reservation["controller_generation"],
                first["controller_generation"],
            )
            attempt = store.connection.execute(
                """
                SELECT execution_authorization_id, controller_owner_id,
                       controller_generation
                FROM canary_submission_attempts
                WHERE intent_id=?
                """,
                (reservation["intent_id"],),
            ).fetchone()
            self.assertIsNotNone(attempt)
            assert attempt is not None
            self.assertEqual(
                attempt["controller_generation"],
                reservation["controller_generation"],
            )
            fill = store.connection.execute(
                """
                SELECT execution_authorization_id, controller_owner_id,
                       controller_generation
                FROM canary_risk_fills
                WHERE reservation_id=?
                """,
                (reservation["reservation_id"],),
            ).fetchone()
            self.assertIsNotNone(fill)
            assert fill is not None
            self.assertEqual(
                fill["execution_authorization_id"],
                reservation["execution_authorization_id"],
            )
            self.assertEqual(fill["controller_owner_id"], reservation["controller_owner_id"])
            self.assertEqual(
                fill["controller_generation"],
                reservation["controller_generation"],
            )
            old_member = _member(
                "sv-worker-old",
                "selection-worker-replaced",
                status="PAUSED",
                allocation="0",
                candidate_id="candidate-sv-worker-old",
                research_trial_id="trial-sv-worker-old",
            )
            self._seed_artifacts(
                store,
                policy,
                ("sv-worker-new",),
                evidence=[_evidence("sv-worker-new")],
            )
            replacement = _member(
                "sv-worker-new",
                "selection-worker-replaced",
                status="ACTIVE",
                allocation="10.00",
                candidate_id="candidate-sv-worker-new",
                research_trial_id="trial-sv-worker-new",
            )
            replacement_config = service.settings.snapshot(now=NOW)
            replacement_selection = _selection(
                "selection-worker-replaced",
                policy,
                members=[old_member, replacement],
            )
            replacement_selection.update(
                {
                    "policy_hash": policy.config_hash,
                    "risk_config_id": replacement_config["config_id"],
                    "risk_config_generation": replacement_config["generation"],
                    "risk_config_hash": replacement_config["config_hash"],
                    "selection_hash": _selection_binding_hash(replacement_selection),
                    "execution_authorization_mode": "EVIDENCE_SELECTED",
                    "scope_hash": hashlib.sha256(
                        b"selection-worker-replaced"
                    ).hexdigest(),
                    "scope_version": "1",
                }
            )
            store.commit_portfolio_selection(
                replacement_selection,
                [old_member, replacement],
            )
            with store.connection:
                store.connection.execute(
                    """
                    UPDATE canary_position_lots
                    SET portfolio_selection_id=?
                    WHERE strategy_version_id=?
                    """,
                    ("selection-worker-replaced", "sv-worker-old"),
                )
            replacement_authorization = self._activate_worker_authorization(
                service.settings,
                strategy_version_id="sv-worker-old",
                selection_id="selection-worker-replaced",
                selection_hash=str(replacement_selection["selection_hash"]),
                selection_policy_hash=policy.config_hash,
                mode="EVIDENCE_SELECTED",
            )
            with store.connection:
                store.connection.execute(
                    """
                    UPDATE canary_position_lots
                    SET execution_authorization_id=?,
                        execution_authorization_mode=?
                    WHERE strategy_version_id=?
                    """,
                    (
                        replacement_authorization["authorization_id"],
                        "EVIDENCE_SELECTED",
                        "sv-worker-old",
                    ),
                )
            with patch.object(CredentialStore, "configured", return_value=True):
                second = worker.tick_rolling(now=NOW)
            self.assertEqual(second["position_management"]["submitted"], 1, second)
            managed_positions = second["position_management"]["positions"]
            self.assertEqual(len(managed_positions), 1)
            self.assertEqual(second["submissions"], [])
            pending_lot = store.connection.execute(
                "SELECT status FROM canary_position_lots ORDER BY opened_at, position_id LIMIT 1"
            ).fetchone()
            self.assertIsNotNone(pending_lot)
            assert pending_lot is not None
            self.assertEqual(pending_lot["status"], "EXIT_PENDING")
            with patch.object(CredentialStore, "configured", return_value=True):
                third = worker.tick_rolling(now=NOW)
            self.assertEqual([row["side"] for row in venue.submissions], ["BUY", "SELL"])
            self.assertEqual(third["position_management"]["submitted"], 0)
            self.assertEqual(third["submissions"], [])
            config = service.settings.snapshot(now=NOW)
            lot = store.connection.execute(
                """
                SELECT position_id, candidate_id, strategy_version_id, research_trial_id,
                       portfolio_selection_id, admission_policy_id, admission_policy_version,
                       risk_config_id, risk_config_generation, risk_config_hash, lineage_type,
                       status, quantity, sold_quantity
                FROM canary_position_lots
                ORDER BY opened_at, position_id
                LIMIT 1
                """
            ).fetchone()
            self.assertIsNotNone(lot)
            assert lot is not None
            self.assertEqual(managed_positions[0]["position_id"], lot["position_id"])
            self.assertEqual(lot["candidate_id"], "candidate-sv-worker-old")
            self.assertEqual(lot["strategy_version_id"], "sv-worker-old")
            self.assertEqual(lot["research_trial_id"], "trial-sv-worker-old")
            self.assertEqual(lot["portfolio_selection_id"], "selection-worker-replaced")
            self.assertEqual(lot["admission_policy_id"], policy.policy_id)
            self.assertEqual(lot["admission_policy_version"], policy.version)
            self.assertEqual(lot["risk_config_id"], config["config_id"])
            self.assertEqual(lot["risk_config_generation"], config["generation"])
            self.assertEqual(lot["risk_config_hash"], config["config_hash"])
            self.assertEqual(lot["lineage_type"], "ROLLING_PORTFOLIO")
            self.assertEqual(lot["status"], "CLOSED")
            self.assertEqual(
                Decimal(str(lot["sold_quantity"])),
                Decimal(str(lot["quantity"])),
            )
            requests = store.connection.execute(
                """
                SELECT side, status, settlement_status,
                       reservation_id, execution_authorization_id,
                       controller_owner_id, controller_generation
                FROM canary_position_requests
                WHERE position_id=? AND side='SELL'
                """,
                (lot["position_id"],),
            ).fetchall()
            self.assertEqual(len(requests), 1)
            request = requests[0]
            self.assertEqual(request["side"], "SELL")
            self.assertEqual(request["status"], "SETTLED")
            self.assertEqual(request["settlement_status"], "SETTLED")
            self.assertTrue(str(request["execution_authorization_id"] or "").strip())
            self.assertTrue(str(request["controller_owner_id"] or "").strip())
            self.assertGreater(int(request["controller_generation"] or 0), 0)
            exit_reservation = store.connection.execute(
                """
                SELECT execution_authorization_id, controller_owner_id,
                       controller_generation
                FROM canary_risk_reservations
                WHERE reservation_id=?
                """,
                (request["reservation_id"],),
            ).fetchone()
            self.assertIsNotNone(exit_reservation)
            assert exit_reservation is not None
            self.assertEqual(
                request["execution_authorization_id"],
                exit_reservation["execution_authorization_id"],
            )
            self.assertEqual(
                request["controller_owner_id"],
                exit_reservation["controller_owner_id"],
            )
            self.assertEqual(
                request["controller_generation"],
                exit_reservation["controller_generation"],
            )
    def test_actual_rolling_node_worker_persists_review_and_resumes_after_restart(self) -> None:
        path = self.path / "rolling-node.sqlite3"

        class StopAfterOneWait(threading.Event):
            def wait(self, timeout: float | None = None) -> bool:
                self.set()
                return True

        first_selection_id: str | None = None
        for instance in range(2):
            with AxiomStore(str(path)) as store:
                node = ResearchNode(
                    NodeConfig(
                        str(path),
                        crypto_enabled=False,
                        rolling_review_interval_seconds=1,
                    ),
                    provider=InMemoryPredictionProvider([]),
                    store=store,
                    clock=lambda: NOW,
                )
                node.stop_event = StopAfterOneWait()
                node._rolling_portfolio_worker_loop()
                selection = store.load_current_portfolio_selection()
                review = store.load_portfolio_review_state()
                worker_state = store.get_worker_state("rolling-portfolio")
                self.assertIsNotNone(selection)
                self.assertIsNotNone(review)
                self.assertIsNotNone(worker_state)
                assert selection is not None
                assert review is not None
                assert worker_state is not None
                selection_id = str(selection["portfolio_selection_id"])
                if first_selection_id is None:
                    first_selection_id = selection_id
                self.assertEqual(selection_id, first_selection_id)
                self.assertEqual(review["portfolio_selection_id"], selection_id)
                self.assertEqual(worker_state["status"], "scheduled")
                self.assertTrue(worker_state["payload"]["scheduled"])
                self.assertEqual(
                    worker_state["payload"]["next_work"],
                    (
                        "review_rolling_portfolio"
                        if instance == 0
                        else "refresh_rolling_evidence"
                    ),
                )

    def test_operator_review_creates_bounded_non_active_draft(self) -> None:
        with self._store("rolling-policy-review.sqlite3") as store:
            settings = self._activate_isolated_risk_settings(
                store,
                max_orders=20,
                max_all_in_buy_usd="1.00",
            )
            active_policy = _policy(
                "policy-active-review",
                version="v1",
                config_hash="sha256:active-review",
                max_members=1,
                global_budget="2.00",
            )
            store.save_admission_policy(active_policy.as_dict())
            store.set_operator_config("rolling_admission_policy_active", active_policy.as_dict())
            store.set_operator_job(
                "rolling_admission_policy_active",
                "ACTIVE",
                active_policy.as_dict(),
                resumable=True,
                timestamp=NOW,
            )
            before_job = store.get_operator_job("rolling_admission_policy_active")
            before_selection = store.load_current_portfolio_selection()
            binding = settings.snapshot(now=NOW)
            operator = OperatorControlPlane(store, settings_service=settings)

            reviewed = operator.review_rolling_admission_policy(
                _policy(
                    "policy-proposed-review",
                    version="v2",
                    config_hash="sha256:proposed-review",
                    max_members=2,
                    global_budget="2.00",
                ).as_dict(),
                actor="reviewer",
                expected_risk_config_id=str(binding["config_id"]),
                expected_risk_config_generation=int(binding["generation"]),
                expected_risk_config_hash=str(binding["config_hash"]),
            )

            draft = reviewed["draft"]
            self.assertEqual(reviewed["status"], "REVIEWED")
            self.assertTrue(str(draft["draft_id"]).startswith("rolling-draft:"))
            self.assertTrue(str(draft["draft_version"]).startswith("draft-"))
            self.assertTrue(draft["paper_only"])
            self.assertFalse(draft["live_execution"])
            allocation = reviewed["allocation_review"]
            self.assertTrue(allocation["within_active_caps"])
            self.assertEqual(allocation["max_active_strategies"], 2)
            self.assertEqual(allocation["max_open_positions"], 3)
            self.assertLessEqual(
                Decimal(str(allocation["global_budget_usd"])),
                Decimal(str(allocation["budget_cap_usd"])),
            )
            with self.assertRaises(OperatorControlError) as over_cap:
                operator.review_rolling_admission_policy(
                    _policy(
                        "policy-over-cap-review",
                        version="v1",
                        config_hash="sha256:over-cap-review",
                        max_members=1,
                        global_budget="4.00",
                    ).as_dict(),
                    actor="reviewer",
                )
            self.assertEqual(
                str(over_cap.exception),
                "ROLLING_POLICY_BUDGET_EXCEEDS_ACTIVE_CAP",
            )

            self.assertEqual(store.load_current_portfolio_selection(), before_selection)
            self.assertEqual(
                store.get_operator_job("rolling_admission_policy_active"),
                before_job,
            )

    def test_operator_activation_rejects_stale_risk_binding(self) -> None:
        with self._store("rolling-policy-stale-review.sqlite3") as store:
            settings = self._activate_isolated_risk_settings(store)
            policy = _policy(
                "policy-stale-review",
                version="v1",
                config_hash="sha256:stale-review",
                max_members=1,
                global_budget="2.00",
            )
            operator = OperatorControlPlane(store, settings_service=settings)
            initial_binding = settings.snapshot(now=NOW)
            reviewed = operator.review_rolling_admission_policy(
                policy.as_dict(),
                actor="reviewer",
                expected_risk_config_id=str(initial_binding["config_id"]),
                expected_risk_config_generation=int(initial_binding["generation"]),
                expected_risk_config_hash=str(initial_binding["config_hash"]),
            )
            draft = reviewed["draft"]
            settings_snapshot = settings.snapshot(now=NOW)
            effective_limits = settings_snapshot["effective_limits"]
            replacement = settings.save_draft(
                {"max_orders_per_day": int(effective_limits["max_submitted_orders_per_day"])},
                "rolling-stale-change",
                expected_generation=int(settings_snapshot["generation"]),
            )
            settings.activate_draft(
                replacement["config_id"],
                "rolling-stale-change",
                expected_generation=int(settings_snapshot["generation"]),
            )
            with self.assertRaises(OperatorControlError) as context:
                operator.activate_rolling_admission_policy(
                    draft_id=str(draft["draft_id"]),
                    draft_version=str(draft["draft_version"]),
                    actor="activator",
                    expected_risk_config_id=str(initial_binding["config_id"]),
                    expected_risk_config_generation=int(initial_binding["generation"]),
                    expected_risk_config_hash=str(initial_binding["config_hash"]),
                )
            self.assertEqual(str(context.exception), "ROLLING_POLICY_REVIEW_STALE")

    def test_operator_review_activation_binds_processor_to_custom_immutable_policy(self) -> None:
        with self._store("rolling-policy-activation.sqlite3") as store:
            settings = self._activate_isolated_risk_settings(store)
            policy = _policy(
                "policy-reviewed-custom",
                version="v7",
                config_hash="sha256:immutable-admission-v7",
                max_members=1,
                global_budget="3.00",
                experimental_allocation_enabled=True,
            )
            self._seed_artifacts(store, policy, ("sv-policy-custom",), evidence=[_evidence("sv-policy-custom")])
            operator = OperatorControlPlane(store, settings_service=settings)
            reviewed = operator.review_rolling_admission_policy(
                policy.as_dict(),
                actor="reviewer",
            )
            self.assertEqual(reviewed["status"], "REVIEWED")
            activated = operator.activate_rolling_admission_policy(
                policy.policy_id,
                policy.version,
                actor="activator",
            )
            self.assertEqual(activated["status"], "ACTIVE")
            processor = AutonomousResearchProcessor(store, clock=lambda: NOW)
            result = processor.review_rolling_portfolio(now=NOW, force=True)
            selection = store.load_current_portfolio_selection()
            self.assertIsNotNone(selection)
            assert selection is not None
            self.assertEqual(selection["policy_id"], policy.policy_id)
            self.assertEqual(selection["policy_version"], policy.version)
            self.assertEqual(result["policy"]["policy_id"], policy.policy_id)
            self.assertEqual(result["policy"]["version"], policy.version)
            self.assertEqual(result["policy"]["config_hash"], policy.config_hash)
            active = store.get_operator_job("rolling_admission_policy_active")
            self.assertIsNotNone(active)
            assert active is not None
            self.assertEqual(active["payload"]["policy_id"], policy.policy_id)
            self.assertEqual(active["payload"]["version"], policy.version)
            self.assertEqual(active["payload"]["config_hash"], policy.config_hash)
    def test_metadata_only_policy_envelope_fails_closed_in_operator_and_dashboard(self) -> None:
        with self._store("rolling-policy-metadata-only.sqlite3") as store:
            settings = self._activate_isolated_risk_settings(store)
            metadata_only = {
                "policy_id": "metadata-only",
                "version": "v1",
                "policy_version": "v1",
                "config_hash": "sha256:metadata-only",
                "global_budget": "999.00",
                "max_members": 99,
                "status": "ACTIVE",
            }
            store.set_operator_config("rolling_admission_policy_active", metadata_only)
            operator = OperatorControlPlane(store, settings_service=settings)
            with self.assertRaises(OperatorControlError) as context:
                operator.review_rolling_admission_policy(
                    _policy("proposed-after-metadata-only", global_budget="1.00").as_dict(),
                    actor="reviewer",
                )
            self.assertEqual(str(context.exception), "ROLLING_POLICY_IMMUTABLE_NOT_FOUND")
            dashboard = DashboardData(store=store, clock=lambda: NOW).rolling_portfolio_data()
            self.assertEqual(dashboard["policy"], {})
            self.assertIsNone(dashboard["policy_review"]["active"]["global_budget"])
            self.assertIn(
                "active_immutable_policy_unavailable",
                dashboard["selection"]["blockers"],
            )

    def test_review_does_not_establish_active_authority_without_activation(self) -> None:
        with self._store("rolling-policy-no-authority.sqlite3") as store:
            settings = self._activate_isolated_risk_settings(store)
            operator = OperatorControlPlane(store, settings_service=settings)
            reviewed = operator.review_rolling_admission_policy(
                _policy("policy-reviewed-no-authority", global_budget="1.00").as_dict(),
                actor="reviewer",
            )
            self.assertEqual(reviewed["status"], "REVIEWED")
            self.assertIsNone(
                store.get_operator_config("rolling_admission_policy_active", None)
            )

    def test_activation_review_failure_keeps_prior_active_policy_unchanged(self) -> None:
        with self._store("rolling-policy-activation-rollback.sqlite3") as store:
            settings = self._activate_isolated_risk_settings(store)
            binding = settings.snapshot(now=NOW)
            prior = _policy(
                "policy-prior-rollback",
                version="v1",
                config_hash="sha256:prior-rollback",
                global_budget="1.00",
            )
            proposed = _policy(
                "policy-proposed-rollback",
                version="v2",
                config_hash="sha256:proposed-rollback",
                global_budget="1.00",
            )
            self._seed_artifacts(store, prior, ())
            self._seed_artifacts(store, proposed, ())
            prior_active = {
                **prior.as_dict(),
                "risk_config_id": binding["config_id"],
                "risk_config_generation": binding["generation"],
                "risk_config_hash": binding["config_hash"],
                "status": "ACTIVE",
                "paper_only": True,
            }
            store.set_operator_config("rolling_admission_policy_active", prior_active)
            operator = OperatorControlPlane(store, settings_service=settings)
            reviewed = operator.review_rolling_admission_policy(
                proposed.as_dict(),
                actor="reviewer",
            )
            before = store.get_operator_config("rolling_admission_policy_active", None)
            with patch.object(
                operator._research_processor,
                "review_rolling_portfolio",
                return_value={},
            ):
                with self.assertRaises(OperatorControlError) as context:
                    operator.activate_rolling_admission_policy(
                        draft_id=str(reviewed["draft"]["draft_id"]),
                        draft_version=str(reviewed["draft"]["draft_version"]),
                        actor="activator",
                    )
            self.assertEqual(str(context.exception), "ROLLING_POLICY_SELECTION_REQUIRED")
            self.assertEqual(
                store.get_operator_config("rolling_admission_policy_active", None),
                before,
            )

    def test_rolling_activation_action_identity_includes_derived_policy_target(self) -> None:
        with self._store("rolling-policy-action-target.sqlite3") as store:
            operator = OperatorControlPlane(store)
            result = operator.execute(
                "rolling.admission.activate",
                confirm="ACTIVATE ROLLING ADMISSION POLICY",
                payload={
                    "policy_id": "policy-target",
                    "policy_version": "v1",
                    "draft_id": "rolling-draft:target",
                    "draft_version": "draft-target",
                },
            )
            self.assertFalse(result["ok"])
            state = store.get_operator_config("operator_action_state", {})
            actions = state.get("actions", []) if isinstance(state, dict) else []
            self.assertEqual(len(actions), 1)
            self.assertEqual(actions[0]["target"], "rolling-draft:target:draft-target")


    def test_processor_restart_preserves_cooldown_anchor_until_day_seven_replacement(self) -> None:
        path = self.path / "rolling-cooldown-restart.sqlite3"
        policy = _policy(
            "policy-cooldown-restart",
            max_members=1,
            global_budget="20.00",
            min_score="-100.00",
            replacement_margin="0",
            cooldown_seconds=7 * 86400,
        )
        old_evidence = _evidence(
            "sv-cooldown-old",
            candidate_id="candidate-sv-cooldown-old",
            research_trial_id="trial-sv-cooldown-old",
            net_return="1.00",
        )
        new_evidence = _evidence(
            "sv-cooldown-new",
            candidate_id="candidate-sv-cooldown-new",
            research_trial_id="trial-sv-cooldown-new",
            net_return="9.00",
        )
        with AxiomStore(str(path)) as store:
            self._seed_artifacts(
                store,
                policy,
                ("sv-cooldown-old", "sv-cooldown-new"),
                evidence=[old_evidence, new_evidence],
            )
            store.set_operator_job(
                "rolling_admission_policy_active",
                "ACTIVE",
                policy.as_dict(),
                resumable=True,
                timestamp=NOW,
            )
            processor = AutonomousResearchProcessor(store, clock=lambda: NOW)
            binding = dict(processor.rolling_portfolio_state()["risk_binding"])
            old_member = _member(
                "sv-cooldown-old",
                "selection-cooldown-old",
                status="ACTIVE",
                allocation="0",
                candidate_id="candidate-sv-cooldown-old",
                research_trial_id="trial-sv-cooldown-old",
            )
            initial = _selection(
                "selection-cooldown-old",
                policy,
                members=[old_member],
                selected_at=NOW,
            )
            initial["last_membership_change_at"] = NOW.isoformat()
            initial.update(
                {
                    "risk_config_id": binding["risk_config_id"],
                    "risk_config_generation": binding["risk_config_generation"],
                    "risk_config_hash": binding["risk_config_hash"],
                    "active_risk_config_id": binding["risk_config_id"],
                    "active_risk_config_generation": binding["risk_config_generation"],
                    "active_risk_config_hash": binding["risk_config_hash"],
                }
            )
            store.commit_portfolio_selection(initial, [old_member])
        for day in range(1, 8):
            review_time = NOW + timedelta(days=day)
            with AxiomStore(str(path)) as store:
                processor = AutonomousResearchProcessor(store, clock=lambda review_time=review_time: review_time)
                state = processor.review_rolling_portfolio(now=review_time, force=True)
                selection = store.load_current_portfolio_selection()
                self.assertIsNotNone(selection)
                assert selection is not None
                members = selection["members"]
                ids = {str(member["strategy_version_id"]) for member in members}
                if day < 7:
                    self.assertEqual(ids, {"sv-cooldown-old"})
                    self.assertEqual(
                        selection["last_membership_change_at"],
                        NOW.isoformat(),
                    )
                else:
                    self.assertEqual(ids, {"sv-cooldown-new"})
                    self.assertEqual(
                        selection["last_membership_change_at"],
                        review_time.isoformat(),
                    )
                self.assertEqual(state["portfolio_selection_id"], selection["portfolio_selection_id"])

    def test_actual_exit_rejected_canceled_release_and_malformed_unknown(self) -> None:
        venue = RollingLifecycleVenue(("FILLED", "REJECTED", "CANCELED", "MALFORMED"))
        long_exit = {"type": "fixed_holding_period", "holding_period_seconds": 86400}
        with self._store("rolling-exit-statuses.sqlite3") as store:
            _policy_value, service, worker = self._rolling_worker_fixture(
                store,
                policy_id="policy-exit-statuses",
                candidate_id="candidate-sv-exit-statuses",
                selection_id="selection-exit-statuses",
                venue=venue,
                exit_policy=long_exit,
            )
            with patch.object(CredentialStore, "configured", return_value=True):
                first = worker.tick_rolling(now=NOW)
                second = worker.tick_rolling(now=NOW)
            self.assertEqual(first["status"], "SUBMITTED", first)
            self.assertEqual(first["decision"], "SUBMITTED", first)
            self.assertIsNone(first["blocker"], first)
            self.assertEqual([row["side"] for row in venue.submissions], ["BUY"], first)
            self.assertEqual(first["submissions"][0]["status"], "MATCHED", first)
            self.assertEqual(second["submissions"], [], second)
            store.connection.execute(
                "UPDATE canary_position_lots SET exit_policy_json=?",
                (json.dumps({"type": "fixed_holding_period", "holding_period_seconds": 0}, sort_keys=True),),
            )
            lease = store.connection.execute(
                """
                SELECT owner_id, generation
                FROM canary_controller_leases
                WHERE singleton=1 AND status='ACTIVE'
                ORDER BY generation DESC LIMIT 1
                """
            ).fetchone()
            self.assertIsNotNone(lease)
            assert lease is not None
            service.controller_owner_id = str(lease["owner_id"])
            service.controller_generation = int(lease["generation"])
            manager = CanaryPositionManager(service)
            config = service.settings.snapshot(now=NOW)
            lot = store.connection.execute(
                "SELECT position_id FROM canary_position_lots WHERE status='OPEN' ORDER BY rowid LIMIT 1"
            ).fetchone()
            self.assertIsNotNone(lot)
            assert lot is not None
            position_id = str(lot["position_id"])
            expected = ("REJECTED", "CANCELED", "UNKNOWN")
            for expected_status in expected:
                position = manager.submit_exit(
                    position_id,
                    venue,
                    expected_generation=int(config["generation"]),
                    config_id=str(config["config_id"]),
                    allow_test_venue=True,
                )
                self.assertEqual(position["status"], expected_status)
                request = store.connection.execute(
                    """
                    SELECT status
                    FROM canary_position_requests
                    WHERE request_id=?
                    """,
                    (position["request_id"],),
                ).fetchone()
                reservation = store.connection.execute(
                    "SELECT status FROM canary_risk_reservations WHERE reservation_id=?",
                    (position["reservation_id"],),
                ).fetchone()
                self.assertIsNotNone(reservation)
                assert reservation is not None
                if expected_status in {"REJECTED", "CANCELED"}:
                    self.assertTrue(request is None or request["status"] == expected_status)
                    self.assertEqual(reservation["status"], "RELEASED")
                else:
                    self.assertIsNotNone(request)
                    assert request is not None
                    self.assertEqual(request["status"], "UNKNOWN")
                    self.assertEqual(reservation["status"], "UNKNOWN")
            lot = store.connection.execute(
                "SELECT status FROM canary_position_lots ORDER BY rowid DESC LIMIT 1"
            ).fetchone()
            self.assertIsNotNone(lot)
            assert lot is not None
            self.assertEqual(lot["status"], "EXIT_PENDING")

    def test_hermes_rolling_research_request_is_bounded_and_deduplicated(self) -> None:
        from axiom.operator import HermesOperatorAdapter

        with self._store("rolling-hermes-bounded.sqlite3") as store:
            policy = _policy("policy-hermes-bounded")
            store.save_admission_policy(policy.as_dict())
            for index in range(140):
                strategy_id = f"hermes-sv-{index:03d}"
                candidate_id = f"candidate-{strategy_id}"
                document = {
                    "version": 1,
                    "market_type": "prediction",
                    "family": "probability_mispricing",
                    "parameters": {"threshold": 0.05 + index / 100000},
                    "operations": [],
                    "probability_model": "fixed",
                    "resolution_aware": True,
                    "resolution_inputs": ["expiry", "settlement"],
                    "strategy_id": candidate_id,
                }
                store.save_strategy_version(
                    {
                        "strategy_version_id": strategy_id,
                        "strategy_id": candidate_id,
                        "version": "1",
                        "strategy_hash": f"sha256:{strategy_id}",
                        "config_hash": f"config:{strategy_id}",
                        "created_at": NOW.isoformat(),
                        "strategy_document": document,
                        "provenance": {
                            "rolling_research": True,
                            "candidate_id": candidate_id,
                            "research_trial_id": f"trial-{strategy_id}",
                        },
                    }
                )
            processor = AutonomousResearchProcessor(store, clock=lambda: NOW)
            first = processor.refresh_rolling_evidence(now=NOW)
            second = processor.refresh_rolling_evidence(now=NOW)
            self.assertEqual(first["queue_item_id"], second["queue_item_id"])
            self.assertEqual(len(first["strategy_versions"]), 32)
            self.assertEqual(len(first["research_trials"]), 32)
            self.assertLessEqual(len(first["candidate_ids"]) if "candidate_ids" in first else 32, 32)
            self.assertEqual(first["source_classes"], ["HISTORICAL", "REPLAY", "PAPER", "LIVE"])
            requests = [
                row
                for row in store.list_research_items(limit=10_000)
                if isinstance(row.get("payload"), dict)
                and row["payload"].get("rolling_research") is True
            ]
            self.assertEqual(len(requests), 1)
            queue_payload = requests[0]["payload"]
            self.assertLessEqual(len(queue_payload["strategy_version_ids"]), 32)
            self.assertLessEqual(len(queue_payload["research_trial_ids"]), 32)
            self.assertLessEqual(len(queue_payload["candidate_ids"]), 32)
            self.assertLess(
                len(
                    json.dumps(
                        queue_payload,
                        sort_keys=True,
                        separators=(",", ":"),
                        ensure_ascii=False,
                    ).encode("utf-8")
                ),
                16_384,
            )
            hermes = HermesOperatorAdapter(store, "hermes-rolling-acceptance")
            run = hermes.run_now()
            self.assertEqual(run["job_id"], "hermes-rolling-acceptance")
            self.assertIn(run["status"], {"ACTIVE", "PAUSED"})
            self.assertEqual(
                len(store.list_research_items(limit=10_000)),
                len(requests),
            )

    def test_pre_rotation_open_buy_obligation_reduces_next_selection_shared_budget(self) -> None:
        with self._store("rolling-pre-rotation-budget.sqlite3") as store:
            settings = self._activate_isolated_risk_settings(store, max_all_in_buy_usd="5.00")
            policy = _policy(
                "policy-pre-rotation-budget",
                max_members=1,
                global_budget="5.00",
                min_score="-100.00",
                replacement_margin="0",
                experimental_allocation_enabled=True,
            )
            old_evidence = _evidence("sv-budget-old", net_return="1.00")
            new_evidence = _evidence("sv-budget-new", net_return="8.00")
            self._seed_artifacts(
                store,
                policy,
                ("sv-budget-old", "sv-budget-new"),
                evidence=[old_evidence, new_evidence],
            )
            store.set_operator_job(
                "rolling_admission_policy_active",
                "ACTIVE",
                policy.as_dict(),
                resumable=True,
                timestamp=NOW,
            )
            processor = AutonomousResearchProcessor(store, clock=lambda: NOW)
            binding = dict(processor.rolling_portfolio_state()["risk_binding"])
            old_member = _member(
                "sv-budget-old",
                "selection-budget-old",
                status="ACTIVE",
                allocation="5.00",
                candidate_id="candidate-sv-budget-old",
                research_trial_id="trial-sv-budget-old",
            )
            old_selection = _selection(
                "selection-budget-old",
                policy,
                members=[old_member],
            )
            old_selection.update(
                {
                    "risk_config_id": binding["risk_config_id"],
                    "risk_config_generation": binding["risk_config_generation"],
                    "risk_config_hash": binding["risk_config_hash"],
                    "active_risk_config_id": binding["risk_config_id"],
                    "active_risk_config_generation": binding["risk_config_generation"],
                    "active_risk_config_hash": binding["risk_config_hash"],
                }
            )
            store.commit_portfolio_selection(old_selection, [old_member])
            lineage = self._lineage(
                strategy_version_id="sv-budget-old",
                selection_id="selection-budget-old",
                policy=policy,
                allocation="5.00",
            )
            lineage.update(
                {
                    "risk_config_id": binding["risk_config_id"],
                    "risk_config_generation": binding["risk_config_generation"],
                    "risk_config_hash": binding["risk_config_hash"],
                }
            )
            reservation = self._reserve(
                store,
                "budget-pre-rotation-open-buy",
                market="budget-rotation-market",
                cost="4.00",
                lineage=lineage,
            )
            self.assertEqual(reservation["status"], "HELD")
            new_member = _member(
                "sv-budget-new",
                "selection-budget-new",
                status="ACTIVE",
                allocation="5.00",
                candidate_id="candidate-sv-budget-new",
                research_trial_id="trial-sv-budget-new",
            )
            new_selection = _selection(
                "selection-budget-new",
                policy,
                members=[new_member],
            )
            new_selection.update(
                {
                    "risk_config_id": binding["risk_config_id"],
                    "risk_config_generation": binding["risk_config_generation"],
                    "risk_config_hash": binding["risk_config_hash"],
                    "active_risk_config_id": binding["risk_config_id"],
                    "active_risk_config_generation": binding["risk_config_generation"],
                    "active_risk_config_hash": binding["risk_config_hash"],
                }
            )
            store.commit_portfolio_selection(new_selection, [new_member])
            current = store.load_current_portfolio_selection()
            self.assertIsNotNone(current)
            assert current is not None
            self.assertEqual(current["portfolio_selection_id"], "selection-budget-new")
            self.assertEqual(current["members"][0]["strategy_version_id"], "sv-budget-new")
            accounting = store.canary_risk_accounting(NOW)
            self.assertEqual(accounting["rolling_global_reserved_usd"], "4.00")
            replacement_lineage = self._lineage(
                strategy_version_id="sv-budget-new",
                selection_id=str(current["portfolio_selection_id"]),
                policy=policy,
                allocation=str(current["members"][0]["allocation"]),
            )
            replacement_lineage.update(
                {
                    "risk_config_id": binding["risk_config_id"],
                    "risk_config_generation": binding["risk_config_generation"],
                    "risk_config_hash": binding["risk_config_hash"],
                }
            )
            with self.assertRaisesRegex(ValueError, "rolling global budget exceeded"):
                self._reserve(
                    store,
                    "budget-cross-rotation-overage",
                    market="budget-rotation-market",
                    cost="2.00",
                    lineage=replacement_lineage,
                )

    def test_sparse_endpoint_observations_fail_rolling_coverage_gate(self) -> None:
        policy = _policy(
            "policy-sparse-endpoints",
            min_actual_coverage_seconds=6 * 86400,
            min_score="-100.00",
        )
        sparse = _evidence("sv-sparse-endpoints", actual_days=7)
        sparse.update(
            {
                "observation_completeness": "0.10",
                "observation_count": 2,
                "endpoint_observation_count": 2,
                "observations": [
                    {"timestamp": (NOW - timedelta(days=7)).isoformat(), "price": "0.50"},
                    {"timestamp": NOW.isoformat(), "price": "0.60"},
                ],
            }
        )
        sparse = _canonicalize_evidence(sparse)
        decision = evaluate_rolling_selection(policy, [sparse], None, NOW)
        member = _member_map(decision)["sv-sparse-endpoints"]
        self.assertNotEqual(_value(member, "status"), "ACTIVE")
        self.assertEqual(Decimal(str(_value(member, "allocation"))), Decimal("0"))
        reason = str(_value(member, "reason")).upper()
        self.assertTrue(
            any(token in reason for token in ("COMPLETENESS", "COVERAGE", "EVIDENCE"))
        )
    def test_values_only_review_derives_new_immutable_policy_identity(self) -> None:
        with self._store("rolling-policy-values-only-identity.sqlite3") as store:
            settings = self._activate_isolated_risk_settings(
                store, max_all_in_buy_usd="1.00"
            )
            active = _policy(
                "policy-active-values-only",
                version="v1",
                global_budget="1.00",
            )
            self._seed_artifacts(store, active, ())
            store.set_operator_config(
                "rolling_admission_policy_active",
                {**active.as_dict(), "status": "ACTIVE"},
            )
            operator = OperatorControlPlane(store, settings_service=settings)
            reviewed = operator.review_rolling_admission_policy(
                {"global_budget": "0.50"}, actor="reviewer"
            )
            proposed = reviewed["draft"]
            self.assertNotEqual(proposed["policy_id"], active.policy_id)
            self.assertNotEqual(proposed["version"], active.version)
            self.assertEqual(proposed["global_budget"], "0.50")
            persisted = store.load_admission_policy(
                str(proposed["policy_id"]), str(proposed["version"])
            )
            self.assertIsNotNone(persisted)
            self.assertEqual(persisted["config_hash"], proposed["config_hash"])
            self.assertEqual(
                store.get_operator_config("rolling_admission_policy_active", None)[
                    "policy_id"
                ],
                active.policy_id,
            )
    def test_two_changed_values_only_reviews_before_activation_get_distinct_deterministic_identities(self) -> None:
        with self._store("rolling-policy-values-only-before-activation.sqlite3") as store:
            settings = self._activate_isolated_risk_settings(store, max_all_in_buy_usd="1.00")
            default = _policy(
                "rolling-default",
                version="rolling-admission-v1",
                global_budget="0.25",
            )
            store.save_admission_policy(default.as_dict())
            operator = OperatorControlPlane(store, settings_service=settings)

            first = operator.review_rolling_admission_policy(
                {"global_budget": "0.50"},
                actor="reviewer",
            )
            second = operator.review_rolling_admission_policy(
                {"global_budget": "0.40"},
                actor="reviewer",
            )
            first_draft = first["draft"]
            second_draft = second["draft"]
            self.assertNotEqual(first_draft["policy_id"], "rolling-default")
            self.assertNotEqual(second_draft["policy_id"], "rolling-default")
            self.assertNotEqual(
                (first_draft["policy_id"], first_draft["version"]),
                (second_draft["policy_id"], second_draft["version"]),
            )
            for draft in (first_draft, second_draft):
                persisted = store.load_admission_policy(
                    str(draft["policy_id"]),
                    str(draft["version"]),
                )
                self.assertIsNotNone(persisted)
                self.assertEqual(persisted["config_hash"], draft["config_hash"])
            repeated = operator.review_rolling_admission_policy(
                {"global_budget": "0.50"},
                actor="reviewer",
            )["draft"]
            self.assertEqual(
                (repeated["policy_id"], repeated["version"]),
                (first_draft["policy_id"], first_draft["version"]),
            )
            self.assertIsNone(
                store.get_operator_config("rolling_admission_policy_active", None)
            )
    def test_values_only_default_reviews_follow_final_budget_and_risk_binding(self) -> None:
        with self._store("rolling-policy-values-only-cap-reductions.sqlite3") as store:
            settings = self._activate_isolated_risk_settings(
                store,
                max_all_in_buy_usd="5.00",
            )
            active = _policy(
                "rolling-default",
                version="rolling-admission-v1",
                global_budget="10.00",
            )
            self._seed_artifacts(store, active, ())
            store.set_operator_config(
                "rolling_admission_policy_active",
                {**active.as_dict(), "status": "ACTIVE"},
            )
            operator = OperatorControlPlane(store, settings_service=settings)

            first = operator.review_rolling_admission_policy({}, actor="reviewer")["draft"]
            self.assertEqual(first["global_budget"], "5.00")

            first_binding = settings.snapshot(now=NOW)
            first_settings_draft = settings.save_draft(
                {"max_all_in_buy_usd": "2.00", "max_positions": 1},
                "reviewer",
                expected_generation=int(first_binding["generation"]),
            )
            settings.activate_draft(
                first_settings_draft["config_id"],
                "reviewer",
                int(first_binding["generation"]),
            )
            second = operator.review_rolling_admission_policy({}, actor="reviewer")["draft"]
            self.assertEqual(second["global_budget"], "2.00")

            second_binding = settings.snapshot(now=NOW)
            second_settings_draft = settings.save_draft(
                {"max_all_in_buy_usd": "1.00"},
                "reviewer",
                expected_generation=int(second_binding["generation"]),
            )
            settings.activate_draft(
                second_settings_draft["config_id"],
                "reviewer",
                int(second_binding["generation"]),
            )
            third = operator.review_rolling_admission_policy({}, actor="reviewer")["draft"]
            self.assertEqual(third["global_budget"], "1.00")
            self.assertNotEqual(
                (first["policy_id"], first["version"]),
                (second["policy_id"], second["version"]),
            )
            self.assertNotEqual(
                (second["policy_id"], second["version"]),
                (third["policy_id"], third["version"]),
            )
            for draft in (first, second, third):
                self.assertIsNotNone(
                    store.load_admission_policy(
                        str(draft["policy_id"]),
                        str(draft["version"]),
                    )
                )

            repeated = operator.review_rolling_admission_policy({}, actor="reviewer")["draft"]
            self.assertEqual(
                (repeated["policy_id"], repeated["version"]),
                (third["policy_id"], third["version"]),
            )



    def test_rolling_identity_validation_precedes_action_idempotency(self) -> None:
        with self._store("rolling-policy-identity-preflight.sqlite3") as store:
            settings = self._activate_isolated_risk_settings(store)
            active = _policy("policy-preflight-active", global_budget="1.00")
            self._seed_artifacts(store, active, ())
            store.set_operator_config(
                "rolling_admission_policy_active",
                {**active.as_dict(), "status": "ACTIVE"},
            )
            operator = OperatorControlPlane(store, settings_service=settings)
            reviewed = operator.execute(
                "rolling.admission.review",
                target="caller-target",
                confirm="REVIEW ROLLING ADMISSION POLICY",
                payload={"values": {"global_budget": "0.50"}},
            )
            self.assertTrue(reviewed["ok"])
            state = store.get_operator_config("operator_action_state", {})
            actions = state.get("actions", []) if isinstance(state, Mapping) else []
            self.assertEqual(len(actions), 1)
            self.assertTrue(str(actions[0]["target"]).startswith("values:"))
            rejected = operator.execute(
                "rolling.admission.activate",
                target="ignored",
                confirm="ACTIVATE ROLLING ADMISSION POLICY",
                payload={"policy_id": "partial-only"},
            )
            self.assertFalse(rejected["ok"])
            self.assertEqual(rejected["reason"], "ROLLING_POLICY_IDENTITY_REQUIRED")
            state = store.get_operator_config("operator_action_state", {})
            actions = state.get("actions", []) if isinstance(state, Mapping) else []
            self.assertEqual(len(actions), 1)

    def test_nested_policy_identity_conflict_fails_before_persistence(self) -> None:
        with self._store("rolling-policy-nested-conflict.sqlite3") as store:
            settings = self._activate_isolated_risk_settings(store)
            operator = OperatorControlPlane(store, settings_service=settings)
            conflicting = _policy(
                "policy-outer-conflict",
                version="v1",
                config_hash="sha256:outer-conflict",
            ).as_dict()
            conflicting["policy"] = _policy(
                "policy-inner-conflict",
                version="v1",
                config_hash="sha256:inner-conflict",
            ).as_dict()
            with self.assertRaises(OperatorControlError) as context:
                operator.review_rolling_admission_policy(
                    conflicting, actor="reviewer"
                )
            self.assertEqual(context.exception.code, "ROLLING_POLICY_IDENTITY_INVALID")
            self.assertIsNone(
                store.get_operator_config("rolling_admission_policy_review", None)
            )
            self.assertIsNone(
                store.load_admission_policy("policy-outer-conflict", "v1")
            )

    def test_malformed_review_does_not_block_valid_active_dashboard_projection(self) -> None:
        with self._store("rolling-policy-dashboard-reviewed-malformed.sqlite3") as store:
            active = _policy("policy-dashboard-active", version="v1")
            self._seed_artifacts(store, active, ())
            store.set_operator_config(
                "rolling_admission_policy_active",
                {**active.as_dict(), "status": "ACTIVE"},
            )
            store.set_operator_config(
                "rolling_admission_policy_review",
                {"policy_id": "malformed-reviewed", "status": "REVIEWED"},
            )
            data = DashboardData(store=store, clock=lambda: NOW).rolling_portfolio_data()
            self.assertEqual(
                data["policy_review"]["active"]["policy_id"], active.policy_id
            )
            blockers = data["selection"]["blockers"]
            self.assertFalse(any(str(reason).startswith("reviewed_") for reason in blockers))

    def test_dashboard_fails_closed_on_nested_active_policy_identity_conflict(self) -> None:
        with self._store("rolling-policy-dashboard-nested-conflict.sqlite3") as store:
            active = _policy("policy-dashboard-outer", version="v1")
            self._seed_artifacts(store, active, ())
            nested = _policy("policy-dashboard-inner", version="v1").as_dict()
            envelope = {**active.as_dict(), "status": "ACTIVE", "policy": nested}
            store.set_operator_config("rolling_admission_policy_active", envelope)
            data = DashboardData(store=store, clock=lambda: NOW).rolling_portfolio_data()
            self.assertEqual(data["policy"], {})
            self.assertIsNone(data["policy_review"]["active"]["policy_id"])
            self.assertIn(
                "active_immutable_policy_identity_invalid",
                data["selection"]["blockers"],
            )
    def test_operator_execution_authorization_requires_ack_and_uses_server_id(self) -> None:
        with self._store("execution-authorization-operator.sqlite3") as store:
            settings = self._activate_isolated_risk_settings(store)
            policy = _policy("policy-execution-authorization", config_hash="a" * 64)
            self._seed_artifacts(
                store,
                policy,
                ("accepted-strategy", "rejected-strategy"),
            )
            store.set_operator_config(
                "rolling_admission_policy_active",
                {**policy.as_dict(), "status": "ACTIVE"},
            )
            accepted_selection = _selection(
                "selection-accepted",
                policy,
                members=[
                    _member(
                        "accepted-strategy",
                        "selection-accepted",
                        allocation="1.00",
                    )
                ],
            )
            accepted_selection["policy_hash"] = policy.config_hash
            store.commit_portfolio_selection(
                accepted_selection,
                accepted_selection["members"],
            )
            operator = OperatorControlPlane(store, settings_service=settings)

            reviewed = operator.review_execution_authorization(
                {
                    "purpose": "operator acceptance",
                    "exact_strategy_versions": ["accepted-strategy"],
                    "lifetime_budget": "1.00",
                    "stop_rules": {"on_any_blocker": "STOP"},
                },
                actor="reviewer",
            )
            draft = reviewed["draft"]
            authorization_id = str(draft["authorization_id"])
            self.assertTrue(authorization_id.startswith("auth-"))
            self.assertEqual(draft["status"], "DRAFT")
            self.assertFalse(draft["adverse_evidence_ack_required"])
            self.assertEqual(
                draft["adverse_evidence_ack"],
                {"acknowledged": True, "required": False},
            )
            self.assertEqual(draft["exact_strategy_versions"], ["accepted-strategy"])
            self.assertEqual(draft["selection_id"], "selection-accepted")
            self.assertEqual(draft["reviewed_selection_policy_hash"], policy.config_hash)
            self.assertTrue(str(draft["selection_hash"]).strip())
            self.assertIsNone(
                store.load_active_execution_authorization(
                    mode="EXPLORATORY_MICRO_CANARY",
                    now=datetime.now(UTC),
                )
            )

            rejected_selection = _selection(
                "selection-rejected",
                policy,
                members=[
                    _member(
                        "rejected-strategy",
                        "selection-rejected",
                        status="REJECTED",
                        allocation="0",
                    )
                ],
            )
            rejected_selection["policy_hash"] = policy.config_hash
            store.commit_portfolio_selection(
                rejected_selection,
                rejected_selection["members"],
            )
            with self.assertRaisesRegex(
                OperatorControlError,
                "EXECUTION_AUTHORIZATION_ADVERSE_EVIDENCE_ACK_REQUIRED",
            ):
                operator.review_execution_authorization(
                    {
                        "purpose": "operator acceptance",
                        "exact_strategy_versions": ["rejected-strategy"],
                        "lifetime_budget": "1.00",
                        "stop_rules": {"on_any_blocker": "STOP"},
                    },
                    actor="reviewer",
                )
            self.assertEqual(
                store.get_operator_config("execution_authorization_review", None)[
                    "authorization_id"
                ],
                authorization_id,
            )

            reviewed = operator.review_execution_authorization(
                {
                    "purpose": "operator acceptance",
                    "exact_strategy_versions": ["rejected-strategy"],
                    "lifetime_budget": "1.00",
                    "adverse_evidence_ack": True,
                    "stop_rules": {"on_any_blocker": "STOP"},
                },
                actor="reviewer",
            )
            draft = reviewed["draft"]
            authorization_id = str(draft["authorization_id"])
            self.assertTrue(draft["adverse_evidence_ack_required"])
            self.assertTrue(draft["adverse_evidence_ack"])
            self.assertEqual(draft["selection_id"], "selection-rejected")
            self.assertIsNone(
                store.load_active_execution_authorization(
                    mode="EXPLORATORY_MICRO_CANARY",
                    now=datetime.now(UTC),
                )
            )

            activated = operator.execute(
                "execution_authorization.activate",
                target="caller-supplied-target-is-ignored",
                confirm="ACTIVATE EXPLORATORY AUTHORIZATION",
            )
            self.assertTrue(activated["ok"])
            self.assertEqual(
                activated["target"], f"exploratory:{authorization_id}"
            )
            self.assertEqual(
                activated["result"]["execution_authorization"]["status"], "ACTIVE"
            )
            active = store.load_active_execution_authorization(
                mode="EXPLORATORY_MICRO_CANARY",
                now=datetime.now(UTC),
            )
            self.assertIsNotNone(active)
            assert active is not None
            self.assertEqual(active["authorization_id"], authorization_id)

            revoked = operator.execute(
                "execution_authorization.revoke",
                target="another-caller-target-is-ignored",
                confirm="REVOKE EXPLORATORY AUTHORIZATION",
            )
            self.assertTrue(revoked["ok"])
            self.assertEqual(
                revoked["target"], f"exploratory:{authorization_id}"
            )
            self.assertEqual(
                revoked["result"]["execution_authorization"]["status"], "REVOKED"
            )
            self.assertIsNone(
                store.load_active_execution_authorization(
                    mode="EXPLORATORY_MICRO_CANARY",
                    now=datetime.now(UTC),
                )
            )
    def test_operator_dashboard_projection_exposes_truthful_identity_and_work(self) -> None:
        with self._store("execution-authorization-dashboard.sqlite3") as store:
            settings = self._activate_isolated_risk_settings(store)
            policy = _policy("policy-execution-dashboard", config_hash="b" * 64)
            store.save_admission_policy(policy.as_dict())
            store.set_operator_config(
                "rolling_admission_policy_active",
                {**policy.as_dict(), "status": "ACTIVE"},
            )
            operator = OperatorControlPlane(store, settings_service=settings)
            payload = DashboardData(
                store=store,
                control=operator,
                clock=lambda: NOW,
            ).operator_data()

            for field in (
                "identity",
                "instance",
                "mode",
                "policy",
                "budgets",
                "coverage",
                "exclusions",
                "strategies",
                "signals",
                "execution",
                "last_work",
                "next_work",
                "execution_authorization",
            ):
                self.assertIn(field, payload)
            self.assertFalse(payload["live_execution"])
            self.assertTrue(payload["paper_only"])
            self.assertEqual(
                payload["execution_authorization"]["mode"],
                "EXPLORATORY_MICRO_CANARY",
            )
            self.assertEqual(payload["mode"], "observing")
            self.assertEqual(
                payload["identity"]["service_identity"],
                "axiom.canary.CanaryService",
            )
