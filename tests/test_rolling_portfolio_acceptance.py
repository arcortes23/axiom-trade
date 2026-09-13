from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
import hashlib
import json
import os
import threading
import tempfile
import unittest
from unittest.mock import patch

from axiom.autonomous import AutonomousResearchProcessor
from axiom.auto_canary import AutonomousCanaryWorker
from axiom.canary import CanaryService, CredentialStore, credential_fingerprint
from axiom.canary_positions import CanaryPositionManager, list_positions
from axiom.canary_settings import CanarySettingsService
from axiom.data import InMemoryPredictionProvider
from axiom.dashboard import DashboardData
from axiom.node import NodeConfig, ResearchNode
from axiom.operator import OperatorControlPlane
from axiom.rolling_portfolio import RollingAdmissionPolicy, RollingEvidence, evaluate_rolling_selection
from axiom.experiment_plan import normalize_market_scope
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

    def _rolling_worker_fixture(
        self,
        store: AxiomStore,
        *,
        policy_id: str,
        candidate_id: str = "candidate-rolling-worker",
        selection_id: str = "selection-rolling-worker",
        venue: RollingLifecycleVenue,
        exit_policy: dict[str, object] | None = None,
    ) -> tuple[RollingAdmissionPolicy, CanaryService, AutonomousCanaryWorker]:
        policy = _policy(
            policy_id,
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
            }
        )
        store.commit_portfolio_selection(selection, [member])
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
        policy = _policy("policy-position-lineage", max_members=1, global_budget="10.00")
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
            store.commit_portfolio_selection(
                _selection("selection-position-old", policy, members=[old_member]),
                [old_member],
            )
            entry = self._reserve(
                store,
                "position-opening-entry",
                market="position-opening-market",
                cost="0.50",
                lineage=old_lineage,
            )
            store.record_canary_fill(
                fill_id="position-opening-entry-fill",
                reservation_id=entry["reservation_id"],
                quantity="1",
                price="0.50",
                filled_at=NOW,
                **old_lineage,
                detail={"settlement": "SETTLED", "token_id": "yes"},
            )
            config = settings.snapshot(now=NOW)
            service = CanaryService(
                store,
                credentials=AcceptanceCredentials(),
                clock=lambda: NOW,
                settings=settings,
            )
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
                        risk_config_generation, risk_config_hash, lineage_type,
                        exit_policy_json, quantity, sold_quantity, cost_basis, fees,
                        pending_exit_quantity, status, opened_at, updated_at
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
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
            store.commit_portfolio_selection(
                _selection("selection-position-new", policy, members=[new_member]),
                [new_member],
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
            self.assertEqual(submitted["lineage"]["portfolio_selection_id"], "selection-position-old")
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
            self.assertEqual(request["portfolio_selection_id"], "selection-position-old")
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
            # Restart restores the persisted portfolio, but this fixture does
            # not persist a canary control row, so the worker is disabled.
            self.assertEqual(tick["status"], "BLOCKED")
            self.assertEqual(tick["decision"], "CANARY_NOT_ARMED")
            self.assertEqual(tick["blocker"], "CANARY_NOT_ARMED")
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
            for key in ("controller_status", "k", "actual_k", "actionable", "policy", "risk", "active_rows", "global_limits", "event_history", "next_jobs", "cold_start_requirements"):
                self.assertIn(key, payload)
            self.assertLessEqual(len(payload["active_rows"]), 10)
            self.assertFalse(payload.get("live_execution", True))


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
            self._seed_executable_candidate(
                store,
                "candidate-sv-worker-new",
                market_id=venue.market_id,
            )
            replacement = _member(
                "sv-worker-new",
                "selection-worker-replaced",
                status="PAPER",
                allocation="0",
                candidate_id="candidate-sv-worker-new",
                research_trial_id="trial-sv-worker-new",
            )
            store.commit_portfolio_selection(
                _selection(
                    "selection-worker-replaced",
                    policy,
                    members=[old_member, replacement],
                ),
                [old_member, replacement],
            )
            with patch.object(CredentialStore, "configured", return_value=True):
                second = worker.tick_rolling(now=NOW)
            self.assertEqual([row["side"] for row in venue.submissions], ["BUY", "SELL"])
            self.assertEqual(second["position_management"]["submitted"], 1)
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
            self.assertEqual(lot["portfolio_selection_id"], "selection-worker-old")
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
                SELECT side, status, settlement_status
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

    def test_actual_rolling_node_worker_persists_review_and_resumes_after_restart(self) -> None:
        path = self.path / "rolling-node.sqlite3"

        class StopAfterOneWait(threading.Event):
            def wait(self, timeout: float | None = None) -> bool:
                self.set()
                return True

        first_selection_id: str | None = None
        for _ in range(2):
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
                self.assertEqual(worker_state["payload"]["next_work"], "review_rolling_portfolio")

    def test_operator_review_activation_binds_processor_to_custom_immutable_policy(self) -> None:
        with self._store("rolling-policy-activation.sqlite3") as store:
            settings = self._activate_isolated_risk_settings(store)
            policy = _policy(
                "policy-reviewed-custom",
                version="v7",
                config_hash="sha256:immutable-admission-v7",
                max_members=1,
                global_budget="4.00",
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
