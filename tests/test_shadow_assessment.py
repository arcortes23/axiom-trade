from __future__ import annotations

import copy
import hashlib
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from axiom.canary import CanaryService
from axiom.canary_settings import CanarySettingsService
from axiom.data import InMemoryPredictionProvider
from axiom.domain import PredictionMarketSnapshot, SettlementState
from axiom.experiment_plan import normalize_market_scope
from axiom.forward import (
    ForwardTestRegistry,
    _canonical_forward_config,
    _content_hash,
    _normalized_strategy_document,
    _operational_setup_for_strategy,
    _operational_setup_hash,
)
from axiom.lifecycle import CandidateLifecycleManager, CandidateStage
from axiom.node import NodeConfig, ResearchNode
from axiom.shadow import ShadowAssessmentError, ShadowAssessmentService, _rejected_strategy
from axiom.storage import AxiomStore
from axiom.strategy import load_strategy

def _plain(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _plain(child) for key, child in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(child) for child in value]
    return value


UTC = timezone.utc
T0 = datetime(2025, 1, 1, 12, 0, tzinfo=UTC)
MARKET_ID = "shadow-synthetic-market"
DB_NAME = "shadow.sqlite"


class _SyntheticPublicFixtureProvider(InMemoryPredictionProvider):
    """Synthetic fixture limited to the public read/acquisition boundary.

    The explicit marker lives on the test provider, not in an observation row:
    production provenance validation therefore cannot mistake a fixture row for
    a live venue row, while the normal shadow service, paper engine, and risk
    path remain exercised unchanged.
    """

    provider_name = "synthetic-public-fixture-test-seam"
    synthetic_fixture = True

    def __init__(self, start: datetime = T0) -> None:
        self.start = start
        catalog = PredictionMarketSnapshot(
            timestamp=start,
            market_id=MARKET_ID,
            question="Will the synthetic fixture market resolve YES?",
            yes_bid=0.49,
            yes_ask=0.51,
            yes_mid=0.50,
            no_bid=0.49,
            no_ask=0.51,
            no_mid=0.50,
            volume=100.0,
            liquidity=20.0,
            expiry=start + timedelta(days=1),
            settlement=SettlementState.OPEN,
            resolution_criteria="Synthetic fixture resolution only.",
            category="fixture",
            tags=("synthetic", "offline"),
            source="synthetic-public-fixture-test-seam",
            yes_token_id=f"yes-{MARKET_ID}",
            no_token_id=f"no-{MARKET_ID}",
            condition_id=f"condition-{MARKET_ID}",
            slug="shadow-synthetic-fixture",
            provider_timestamp=start,
            active=True,
            closed=False,
            accepting_orders=True,
            enable_order_book=True,
        )
        super().__init__([catalog])
        self._history = tuple(self._make_history())

    @staticmethod
    def _book(stamp: datetime, token_id: str, bid: float, ask: float, ask_size: float) -> dict[str, Any]:
        return {
            "timestamp": stamp.isoformat(),
            "token_id": token_id,
            # Ten units of bid depth are sufficient for every managed exit;
            # ask depth is varied below to make the partial entry deliberate.
            "bids": [{"price": bid, "size": 10.0}],
            "asks": [{"price": ask, "size": ask_size}],
            "source": "synthetic-public-fixture-test-seam",
        }

    def _make_history(self) -> Sequence[Mapping[str, Any]]:
        rows: list[dict[str, Any]] = []
        points = (
            (0, 0.50, 0.49, 0.499, 10.0, 0.49, 0.499, 10.0, None),
            # The YES member has sufficient ask depth for a full entry; the
            # NO member has only half a unit, leaving a residual to manage.
            (1, 0.40, 0.39, 0.399, 10.0, 0.59, 0.599, 0.5, None),
            # Both managed exits have sufficient bid depth.  The NO exit is
            # profitable while the YES exit is intentionally loss-making.
            (2, 0.55, 0.34, 0.349, 10.0, 0.69, 0.699, 10.0, None),
            (3, 0.55, 0.54, 0.549, 10.0, 0.54, 0.549, 10.0, SettlementState.RESOLVED_YES.value),
        )
        for offset, yes_mid, yes_bid, yes_ask, yes_size, no_bid, no_ask, no_size, settlement in points:
            stamp = self.start + timedelta(hours=offset)
            rows.append(
                {
                    "market_id": MARKET_ID,
                    "timestamp": stamp.isoformat(),
                    "observed_at": stamp.isoformat(),
                    "available_at": stamp.isoformat(),
                    "source_timestamp": stamp.isoformat(),
                    "source_snapshot_id": f"synthetic-shadow-{offset}",
                    "source_type": "CURRENT",
                    "source": self.provider_name,
                    "provider_timestamp": stamp.isoformat(),
                    "yes_mid": yes_mid,
                    "no_mid": 1.0 - yes_mid,
                    "yes_bid": yes_bid,
                    "yes_ask": yes_ask,
                    "no_bid": no_bid,
                    "no_ask": no_ask,
                    "yes_order_book": self._book(stamp, f"yes-{MARKET_ID}", yes_bid, yes_ask, yes_size),
                    "no_order_book": self._book(stamp, f"no-{MARKET_ID}", no_bid, no_ask, no_size),
                    "settlement": settlement or SettlementState.OPEN.value,
                    "resolution_criteria": "Synthetic fixture resolution only.",
                    "event_id": "synthetic-shadow-event",
                    "active": True,
                    "closed": False,
                    "liquidity": 20.0,
                    "volume": 100.0,
                }
            )
        return rows

    # Keep these methods as the only public acquisition seam. There is no
    # submit/cancel/trade method on this fixture or its inherited adapter.
    def markets(self, active: bool = True):
        return super().markets(active=active)

    def market(self, market_id: str):
        # Avoid appending a second current snapshot; all observations come from
        # the explicitly bounded public price-history fixture above.
        return None

    def price_history(self, market_id: str, start=None, end=None):
        rows = list(self._history) if str(market_id) == MARKET_ID else []
        if start is not None:
            start = start.astimezone(UTC) if start.tzinfo else start.replace(tzinfo=UTC)
            rows = [row for row in rows if datetime.fromisoformat(row["timestamp"]) >= start]
        if end is not None:
            end = end.astimezone(UTC) if end.tzinfo else end.replace(tzinfo=UTC)
            rows = [row for row in rows if datetime.fromisoformat(row["timestamp"]) <= end]
        return rows


class ShadowAssessmentIntegrationTests(unittest.TestCase):
    @staticmethod
    def _scope() -> tuple[dict[str, Any], str, str]:
        policy = normalize_market_scope(
            {
                "schema_version": "1",
                "mode": "EXACT_MARKETS",
                "instrument": "POLYMARKET",
                "categories": [],
                "market_ids": [MARKET_ID],
                "filters": {},
                "regime_restrictions": {},
                "provenance": "canonical",
            }
        )
        return policy.as_dict(), policy.scope_hash, policy.scope_version

    @classmethod
    def _candidate_payload(cls, store: AxiomStore, candidate_id: str, family: str) -> dict[str, Any]:
        scope, scope_hash, scope_version = cls._scope()
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
                "probability_model": "synthetic-fixture-model",
                "resolution_aware": True,
                "resolution_inputs": ["expiry", "settlement"],
                "strategy_id": candidate_id,
            }
        ).to_dict()
        model = {"version": "fixture-model-v1", "kind": "constant_baseline", "probability": 0.50}
        costs = {
            "version": "paper-assumptions-v1",
            "currency": "USD",
            "sizing": {"model": "fixed_allocated_capital", "allocated_capital": "0.50"},
            "fees": {"model": "proportional", "fee_bps": "10"},
            "slippage": {"model": "proportional", "slippage_bps": "5"},
            "fee_bps": 10.0,
            "slippage_bps": 5.0,
        }
        exit_policy = {"type": "fixed_holding_period", "holding_period": 1}
        risk_limits = {
            "max_order_notional": 1.0,
            "max_account_exposure": 5.0,
            "max_loss": 2.0,
        }
        config = _canonical_forward_config(
            {
                "candidate_id": candidate_id,
                "market_type": "prediction",
                "market_scope": scope,
                "market_scope_hash": scope_hash,
                "market_scope_version": scope_version,
                "plan_hash": f"sha256:synthetic-plan-{candidate_id}",
                "dataset_selector": {
                    "dataset_id": "synthetic-public-fixture",
                    "dataset_version": "v1",
                    "source_type": "HISTORICAL",
                },
                "dataset_attestation": {
                    "dataset_id": "synthetic-public-fixture",
                    "dataset_version": "v1",
                    "status": "CURRENT",
                    "policy_version": "prediction-integrity-v1",
                    "attestation_hash": f"sha256:synthetic-attestation-{candidate_id}",
                },
                "strategy_document": strategy,
                "model_document": model,
                "exit_policy": exit_policy,
                "paper_assumptions_explicit": True,
                "paper_assumptions": costs,
                "cost_provenance": costs,
                "paper_only": True,
                "live_execution": False,
                "execution": "paper_only",
            }
        )
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
        frozen_config = _plain(spec.config)
        setup = dict(frozen_config["operational_setup"])
        config_hash = _content_hash({"config": frozen_config, "risk_limits": risk_limits})
        frozen_hash = hashlib.sha256("|".join((spec.strategy_hash, spec.model_hash, config_hash)).encode()).hexdigest()
        frozen_documents = {
            "operational_setup": copy.deepcopy(setup),
            "strategy_document": copy.deepcopy(strategy),
            "model_document": copy.deepcopy(model),
            "forward_config": copy.deepcopy(frozen_config),
            "market_scope": copy.deepcopy(scope),
            "exit_policy": copy.deepcopy(exit_policy),
            "cost_provenance": copy.deepcopy(costs),
            "risk_limits": copy.deepcopy(risk_limits),
        }
        return {
            "member_id": f"{family}:{candidate_id}",
            "candidate_id": candidate_id,
            "market_type": "prediction",
            "family": family,
            "market_ids": [MARKET_ID],
            "strategy_document": strategy,
            "model_document": model,
            "strategy_hash": spec.strategy_hash,
            "model_hash": spec.model_hash,
            "config": frozen_config,
            "forward_config": frozen_config,
            "config_hash": config_hash,
            "frozen_hash": frozen_hash,
            "operational_setup": copy.deepcopy(setup),
            "operational_setup_hash": _operational_setup_hash(setup),
            "setup_id": setup["setup_id"],
            "market_scope": scope,
            "market_scope_hash": scope_hash,
            "market_scope_version": scope_version,
            "exit_policy": exit_policy,
            "cost_provenance": costs,
            "risk_limits": risk_limits,
            "frozen_documents": frozen_documents,
            "paper_only": True,
            "live_execution": False,
            "rejection_reason": "historical_validation_negative_not_live_admission",
            "historical_evidence": {
                "source_type": "HISTORICAL",
                "dataset_id": "synthetic-public-fixture",
                "status": "REJECTED",
            },
        }

    @classmethod
    def _seed_rejected_candidate(cls, store: AxiomStore, candidate_id: str, family: str) -> None:
        payload = cls._candidate_payload(store, candidate_id, family)
        lifecycle = CandidateLifecycleManager(store)
        lifecycle.register_idea(candidate_id, payload)
        lifecycle.advance(candidate_id, CandidateStage.SCHEMA_VALIDATED, {"schema_valid": True})
        lifecycle.advance(candidate_id, CandidateStage.BACKTESTED, {"backtest_complete": True})
        lifecycle.advance(
            candidate_id,
            CandidateStage.VALIDATED,
            {"validation_complete": True, "holdout_used": False},
        )
        lifecycle.advance(
            candidate_id,
            CandidateStage.ROBUSTNESS_CHECKED,
            {"robustness_passed": True, "holdout_used": False},
        )
        lifecycle.advance(
            candidate_id,
            CandidateStage.FROZEN,
            {
                "frozen": True,
                "holdout_used": False,
                "strategy_hash": payload["strategy_hash"],
                "model_hash": payload["model_hash"],
                "config_hash": payload["config_hash"],
                "frozen_hash": payload["frozen_hash"],
                "risk_snapshot": payload["risk_limits"],
                "plan_hash": payload["config"]["plan_hash"],
                "market_scope": payload["market_scope"],
                "market_scope_hash": payload["market_scope_hash"],
                "market_scope_version": payload["market_scope_version"],
                "dataset_selector": payload["config"]["dataset_selector"],
                "dataset_attestation": payload["config"]["dataset_attestation"],
            },
        )
        lifecycle.reject(
            candidate_id,
            payload["rejection_reason"],
            expected_stage=CandidateStage.FROZEN,
            evidence={"historical_evidence": payload["historical_evidence"]},
        )

    @classmethod
    def _pre_freeze_rejected_payload(
        cls,
        store: AxiomStore,
        candidate_id: str,
        family: str,
        *,
        malformed: str | None = None,
    ) -> dict[str, Any]:
        source = cls._candidate_payload(store, candidate_id, family)
        strategy_parameters = source["strategy_document"]["parameters"]
        payload: dict[str, Any] = {
            "candidate_id": candidate_id,
            "family": family,
            "strategy": copy.deepcopy(source["strategy_document"]),
            "parameters": {
                name: copy.deepcopy(strategy_parameters[name])
                for name in ("lookback", "threshold")
            },
            "market_scope": copy.deepcopy(source["market_scope"]),
            "market_scope_hash": source["market_scope_hash"],
            "market_scope_version": source["market_scope_version"],
            "operational_setup": copy.deepcopy(source["operational_setup"]),
            "operational_setup_hash": source["operational_setup_hash"],
            "setup_id": source["setup_id"],
            "exit_policy": copy.deepcopy(source["exit_policy"]),
            "cost_assumptions": {
                key: copy.deepcopy(value)
                for key, value in source["cost_provenance"].items()
                if key != "sizing"
            },
            "paper_only": True,
            "research_only": True,
            "rejection_reason": "REJECTED_BEFORE_FORWARD_CONFIG_FREEZE",
            "rejection_evidence": {
                "reason_code": "REJECTED_BEFORE_FORWARD_CONFIG_FREEZE",
                "source_type": "HISTORICAL",
            },
            "historical_evidence": {
                "source_type": "HISTORICAL",
                "status": "REJECTED",
            },
        }
        payload.pop("model_document", None)
        if malformed == "missing_predicate":
            payload["strategy"]["parameters"].pop("entry_predicate", None)
        elif malformed == "missing_scope_hash":
            payload.pop("market_scope_hash", None)
        elif malformed == "missing_costs":
            payload.pop("cost_assumptions", None)
        return payload

    @classmethod
    def _seed_pre_freeze_rejected(
        cls,
        store: AxiomStore,
        candidate_id: str,
        family: str,
        *,
        malformed: str | None = None,
        payload_override: Mapping[str, Any] | None = None,
        rejection_reason: str = "negative_validation_expectancy",
    ) -> None:
        payload = (
            copy.deepcopy(dict(payload_override))
            if payload_override is not None
            else cls._pre_freeze_rejected_payload(
                store,
                candidate_id,
                family,
                malformed=malformed,
            )
        )
        lifecycle = CandidateLifecycleManager(store)
        lifecycle.register_idea(candidate_id, payload)
        lifecycle.advance(
            candidate_id,
            CandidateStage.SCHEMA_VALIDATED,
            {"schema_valid": True},
            reason="fixture schema validated",
        )
        backtested = lifecycle.advance(
            candidate_id,
            CandidateStage.BACKTESTED,
            {"backtest_complete": True},
            reason="fixture historical simulation completed",
        )
        lifecycle.reject(
            candidate_id,
            rejection_reason,
            evidence={"validation_complete": True},
            expected_stage=CandidateStage.BACKTESTED,
            expected_payload=backtested.payload,
        )

    @staticmethod
    def _fill_projection(fill: Any) -> dict[str, Any]:
        return {
            "timestamp": fill.timestamp.isoformat(),
            "market_type": getattr(fill.market_type, "value", fill.market_type),
            "symbol": fill.symbol,
            "market_id": fill.market_id,
            "side": getattr(fill.side, "value", fill.side),
            "quantity": fill.quantity,
            "price": fill.price,
            "fees": fill.fees,

            "slippage": fill.slippage,
            "strategy_id": fill.strategy_id,
            "order_id": fill.order_id,
            "metadata": dict(fill.metadata),
        }
    def test_pre_freeze_rejected_pair_projects_from_candidate_and_current_settings(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with AxiomStore(str(Path(directory) / DB_NAME)) as store:
                settings = CanarySettingsService(store, clock=lambda: T0)
                CanaryService(store, clock=lambda: T0, settings=settings).disarm()
                candidate_ids = ("pre-freeze-momentum", "pre-freeze-mean-reversion")
                for candidate_id, family in zip(
                    candidate_ids,
                    ("momentum", "mean_reversion"),
                ):
                    self._seed_pre_freeze_rejected(store, candidate_id, family)
                expected_strategies = {
                    candidate_id: copy.deepcopy(
                        store.load_candidate_lifecycle(candidate_id)["payload"]["strategy"]
                    )
                    for candidate_id in candidate_ids
                }
                lifecycle_before = {
                    candidate_id: copy.deepcopy(
                        store.load_candidate_lifecycle(candidate_id)
                    )
                    for candidate_id in candidate_ids
                }

                registration = ShadowAssessmentService(
                    store,
                    clock=lambda: T0,
                    max_markets=1,
                ).register(
                    candidate_ids,
                    max_cycles=1,
                    now=T0,
                )
                manifest = registration["manifest"]
                shared = manifest["shared"]
                self.assertEqual(shared["budget"]["bankroll_source"], "derived")
                self.assertEqual(shared["budget"]["cap"], "5.00")
                self.assertEqual(shared["spec"]["bankroll"], 5.0)
                self.assertEqual(
                    shared["spec"]["config"]["paper_assumptions"]["sizing"][
                        "allocated_capital"
                    ],
                    "2.50",
                )
                self.assertEqual(
                    shared["risk_settings_identity"],
                    {
                        "config_id": settings.snapshot(T0)["config_id"],
                        "generation": 1,
                        "config_hash": settings.snapshot(T0)["config_hash"],
                    },
                )
                self.assertEqual(
                    shared["projection_provenance"]["kind"],
                    "HISTORICAL_REJECTED_SETUP_CURRENT_SETTINGS",
                )
                self.assertEqual(
                    shared["projection_provenance"]["settings_identity"],
                    shared["risk_settings_identity"],
                )
                self.assertEqual(
                    shared["spec"]["risk_limits"]["max_account_exposure"],
                    5.0,
                )
                members = manifest["members"]
                self.assertEqual(
                    [(member["family"], member["candidate_id"]) for member in members],
                    [
                        ("momentum", candidate_ids[0]),
                        ("mean_reversion", candidate_ids[1]),
                    ],
                )
                for member in members:
                    self.assertEqual(
                        member["strategy"],
                        expected_strategies[member["candidate_id"]],
                    )
                    self.assertTrue(
                        {
                            "version",
                            "market_type",
                            "family",
                            "operations",
                            "probability_model",
                            "resolution_aware",
                            "resolution_inputs",
                            "parameters",
                        }
                        <= set(member["strategy"])
                    )
                for member in members:
                    self.assertEqual(
                        member["projection_provenance"]["kind"],
                        "HISTORICAL_REJECTED_SETUP_CURRENT_SETTINGS",
                    )
                    self.assertFalse(member["historical_frozen_config"])
                    self.assertNotIn("frozen_hash", member)
                    self.assertTrue(member["config"]["paper_only"])
                    self.assertTrue(member["config"]["research_only"])
                    self.assertFalse(member["config"]["live_execution"])
                    self.assertEqual(
                        member["projection_provenance"]["current_derived"][
                            "settings_identity"
                        ],
                        shared["risk_settings_identity"],
                    )
                    self.assertEqual(
                        member["strategy"]["parameters"]["entry_predicate"]["version"],
                        "absolute-move-v1",
                    )
                    self.assertNotIn(
                        "model",
                        member["projection_provenance"]["candidate_declared"],
                    )
                for candidate_id in candidate_ids:
                    self.assertEqual(
                        store.load_candidate_lifecycle(candidate_id),
                        lifecycle_before[candidate_id],
                    )

                bad_ids = ("pre-freeze-bad-momentum", "pre-freeze-bad-reversion")
                self._seed_pre_freeze_rejected(
                    store,
                    bad_ids[0],
                    "momentum",
                    malformed="missing_predicate",
                )
                self._seed_pre_freeze_rejected(store, bad_ids[1], "mean_reversion")
                bad_before = {
                    candidate_id: copy.deepcopy(
                        store.load_candidate_lifecycle(candidate_id)
                    )
                    for candidate_id in bad_ids
                }
                with self.assertRaisesRegex(
                    ShadowAssessmentError,
                    "entry predicate",
                ):
                    ShadowAssessmentService(store, clock=lambda: T0).register(
                        bad_ids,
                        max_cycles=1,
                        now=T0,
                    )
                for candidate_id in bad_ids:
                    self.assertEqual(
                        store.load_candidate_lifecycle(candidate_id),
                        bad_before[candidate_id],
                    )

    def test_adversarial_candidate_provenance_fails_closed(self) -> None:
        cases: tuple[str, Any, str] = (
            (
                "forged_config",
                lambda payload, source: payload.update(
                    {
                        "config": copy.deepcopy(source["config"]),
                        "forward_config": copy.deepcopy(source["config"]),
                    }
                ),
                "frozen candidate config provenance is ambiguous",
            ),
            (
                "parameters_only",
                lambda payload, source: (
                    payload.pop("strategy", None),
                    payload.pop("strategy_document", None),
                ),
                "missing rejected strategy document",
            ),
            (
                "boolean_cost",
                lambda payload, source: payload["cost_assumptions"]["fees"].update(
                    {"fee_bps": True}
                ),
                "finite and non-negative",
            ),
            (
                "allocation_alias",
                lambda payload, source: payload["cost_assumptions"].update(
                    {"sizing": {"allocated_capital": "0.01"}}
                ),
                "candidate allocation declaration",
            ),
            (
                "numeric_setup_id",
                lambda payload, source: payload.update({"setup_id": 123}),
                "setup id must be a non-empty string",
            ),
        )
        for name, mutate, message in cases:
            with self.subTest(case=name):
                with tempfile.TemporaryDirectory() as directory:
                    with AxiomStore(str(Path(directory) / f"{name}.sqlite")) as store:
                        settings = CanarySettingsService(store, clock=lambda: T0)
                        CanaryService(store, clock=lambda: T0, settings=settings).disarm()
                        source = self._candidate_payload(
                            store, f"{name}-momentum", "momentum"
                        )
                        payload = self._pre_freeze_rejected_payload(
                            store, f"{name}-momentum", "momentum"
                        )
                        mutate(payload, source)
                        self._seed_pre_freeze_rejected(
                            store,
                            f"{name}-momentum",
                            "momentum",
                            payload_override=payload,
                        )
                        self._seed_pre_freeze_rejected(
                            store, f"{name}-reversion", "mean_reversion"
                        )
                        with self.assertRaisesRegex(ShadowAssessmentError, message):
                            ShadowAssessmentService(store, clock=lambda: T0).register(
                                (f"{name}-momentum", f"{name}-reversion"),
                                max_cycles=1,
                                now=T0,
                            )

    def test_rejected_strategy_requires_production_declaration(self) -> None:
        required_fields = (
            "version",
            "market_type",
            "family",
            "operations",
            "probability_model",
            "resolution_aware",
            "resolution_inputs",
            "parameters",
        )
        with tempfile.TemporaryDirectory() as directory:
            with AxiomStore(str(Path(directory) / "strategy-contract.sqlite")) as store:
                for field in required_fields:
                    with self.subTest(missing=field):
                        payload = self._pre_freeze_rejected_payload(
                            store, f"missing-{field}", "momentum"
                        )
                        payload["strategy"].pop(field, None)
                        message = (
                            "missing rejected strategy parameters"
                            if field == "parameters"
                            else f"missing rejected strategy fields: {field}"
                        )
                        with self.assertRaisesRegex(ShadowAssessmentError, message):
                            _rejected_strategy(
                                payload,
                                payload["operational_setup"],
                                "momentum",
                            )

                payload = self._pre_freeze_rejected_payload(
                    store, "missing-entry-predicate", "momentum"
                )
                payload["strategy"]["parameters"].pop("entry_predicate", None)
                payload["parameters"].pop("entry_predicate", None)
                with self.assertRaisesRegex(
                    ShadowAssessmentError,
                    "missing rejected strategy entry predicate",
                ):
                    _rejected_strategy(
                        payload,
                        payload["operational_setup"],
                        "momentum",
                    )

                payload = self._pre_freeze_rejected_payload(
                    store, "wrong-family", "momentum"
                )
                payload["strategy"]["family"] = "mean_reversion"
                with self.assertRaisesRegex(
                    ShadowAssessmentError,
                    "family disagrees with root family",
                ):
                    _rejected_strategy(
                        payload,
                        payload["operational_setup"],
                        "momentum",
                    )

                payload = self._pre_freeze_rejected_payload(
                    store, "conflicting-strategy", "momentum"
                )
                payload["strategy_document"] = copy.deepcopy(payload["strategy"])
                payload["strategy_document"]["probability_model"] = "forged-model"
                with self.assertRaisesRegex(
                    ShadowAssessmentError,
                    "conflicting rejected strategy provenance",
                ):
                    _rejected_strategy(
                        payload,
                        payload["operational_setup"],
                        "momentum",
                    )

                payload = self._pre_freeze_rejected_payload(
                    store, "conflicting-parameters", "momentum"
                )
                payload["parameters"]["threshold"] = 0.10
                with self.assertRaisesRegex(
                    ShadowAssessmentError,
                    "conflicting rejected strategy parameters",
                ):
                    _rejected_strategy(
                        payload,
                        payload["operational_setup"],
                        "momentum",
                    )
                payload = self._pre_freeze_rejected_payload(
                    store, "unknown-root-parameter", "momentum"
                )
                payload["parameters"]["forged"] = "not-in-strategy"
                with self.assertRaisesRegex(
                    ShadowAssessmentError,
                    "root strategy parameters are not explicit",
                ):
                    _rejected_strategy(
                        payload,
                        payload["operational_setup"],
                        "momentum",
                    )

                payload = self._pre_freeze_rejected_payload(
                    store, "missing-root-target", "momentum"
                )
                payload["parameters"]["entry_predicate"] = copy.deepcopy(
                    payload["strategy"]["parameters"]["entry_predicate"]
                )
                payload["strategy"]["parameters"].pop("entry_predicate")
                with self.assertRaisesRegex(
                    ShadowAssessmentError,
                    "root strategy parameters are not explicit",
                ):
                    _rejected_strategy(
                        payload,
                        payload["operational_setup"],
                        "momentum",
                    )


    def test_unrelated_rejection_reason_is_not_a_shadow_entry(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with AxiomStore(str(Path(directory) / "unrelated.sqlite")) as store:
                settings = CanarySettingsService(store, clock=lambda: T0)
                CanaryService(store, clock=lambda: T0, settings=settings).disarm()
                self._seed_pre_freeze_rejected(
                    store,
                    "unrelated-momentum",
                    "momentum",
                    rejection_reason="unrelated_rejection",
                )
                self._seed_pre_freeze_rejected(
                    store, "unrelated-reversion", "mean_reversion"
                )
                with self.assertRaisesRegex(
                    ShadowAssessmentError, "authoritative negative validation expectancy"
                ):
                    ShadowAssessmentService(store, clock=lambda: T0).register(
                        ("unrelated-momentum", "unrelated-reversion"),
                        max_cycles=1,
                        now=T0,
                    )

    def test_shadow_observation_isolated_from_live_admission_and_operationally_complete(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            db_path = str(Path(directory) / DB_NAME)
            with AxiomStore(db_path) as store:
                settings = CanarySettingsService(store, clock=lambda: T0)
                canary = CanaryService(store, clock=lambda: T0, settings=settings)
                canary.disarm()
                self._seed_rejected_candidate(store, "shadow-momentum", "momentum")
                self._seed_rejected_candidate(store, "shadow-mean-reversion", "mean_reversion")
                candidate_ids = ("shadow-momentum", "shadow-mean-reversion")
                lifecycle_before = {candidate: copy.deepcopy(store.load_candidate_lifecycle(candidate)) for candidate in candidate_ids}
                lifecycle_events_before = {
                    candidate: copy.deepcopy(store.list_candidate_lifecycle_events(candidate, limit=100))
                    for candidate in candidate_ids
                }

                provider = _SyntheticPublicFixtureProvider()
                self.assertTrue(provider.synthetic_fixture)
                service = ShadowAssessmentService(store, clock=lambda: T0, max_markets=1)
                first_registration = service.register(
                    candidate_ids,
                    bankroll=1.0,
                    max_cycles=1,
                    max_observations=64,
                    now=T0,
                )
                second_registration = service.register(
                    candidate_ids,
                    bankroll=1.0,
                    max_cycles=1,
                    max_observations=64,
                    now=T0,
                )
                self.assertEqual(first_registration, second_registration)
                self.assertEqual(len(service.list()), 1)
                job_id = first_registration["job_id"]
                manifest = first_registration["manifest"]
                shared_spec = manifest["shared"]["spec"]
                self.assertEqual(shared_spec["bankroll"], 1.0)
                self.assertTrue(manifest["paper_only"])
                self.assertFalse(manifest["live_execution"])
                self.assertEqual({member["family"] for member in manifest["members"]}, {"momentum", "mean_reversion"})
                self.assertEqual(len(manifest["members"]), 2)
                for member in manifest["members"]:
                    self.assertIn("strategy", member)
                    self.assertIn("model", member)
                    self.assertIn("scope", member)
                    self.assertIn("exit_policy", member)
                    self.assertIn("cost_provenance", member)
                    self.assertIn("rejection_evidence", member)
                    self.assertEqual(member["rejection_evidence"]["stage"], "REJECTED")

                node = ResearchNode(
                    NodeConfig(
                        db_path=db_path,
                        max_markets=1,
                        research_enabled=False,
                        crypto_enabled=False,
                        shadow_enabled=True,
                        shadow_interval=1.0,
                        shadow_jobs_per_cycle=1,
                        mutation_enabled=False,
                    ),
                    provider=provider,
                    store=store,
                    clock=lambda: T0 + timedelta(hours=3),
                )
                # This is the same bounded tick invoked by the shadow worker;
                # no direct engine or service shortcut is used.
                evidence = node._run_shadow_assessment_tick(now=T0 + timedelta(hours=3))
                self.assertEqual(evidence["processed_jobs"], 1)
                self.assertEqual(evidence["successful_jobs"], 1)
                self.assertEqual(evidence["completed_jobs"], 1)

                completed = store.load_shadow_job(job_id)
                self.assertEqual(completed["status"], "COMPLETED")
                state = completed["state"]
                self.assertEqual(state["cycles"], 1)
                self.assertEqual(state["public_observations"], 4)
                self.assertEqual(state["member_observations"], 8)
                self.assertTrue(state["paper_only"])
                self.assertFalse(state["live_execution"])
                self.assertEqual({member["family"] for member in manifest["members"]}, {"momentum", "mean_reversion"})
                stats = state["members"]
                self.assertEqual(set(stats), {"momentum:shadow-momentum", "mean_reversion:shadow-mean-reversion"})
                self.assertTrue(all(item["fills"] >= 2 and item["exits"] >= 1 for item in stats.values()))
                self.assertTrue(all(item["accounting"]["buy_fills"] >= 1 for item in stats.values()))
                self.assertTrue(all(item["accounting"]["sell_fills"] >= 1 for item in stats.values()))

                run_id = shared_spec["experiment_id"]
                observations = store.list_paper_observations(run_id, limit=None)
                events = store.list_paper_execution_events(run_id, limit=None)
                fills = store.load_fills()
                run_fills = [fill for fill in fills if fill.strategy_id == shared_spec["strategy_hash"]]
                self.assertEqual(len(observations), 8)
                self.assertGreaterEqual(len(events), 8)
                self.assertEqual(len(run_fills), 4)
                self.assertEqual({item["payload"]["shadow_member_id"] for item in observations}, set(stats))
                self.assertTrue(all(item["payload"]["paper_only"] and not item["payload"]["live_execution"] for item in observations))
                self.assertTrue(all(item["payload"]["shadow_assessment"] for item in observations))
                self.assertTrue(all(item["payload"]["paper_only"] and not item["payload"]["live_execution"] for item in events))
                self.assertTrue(all(fill.metadata["shadow_assessment"] and fill.metadata["paper_only"] and not fill.metadata["live_execution"] for fill in run_fills))
                self.assertIn("FULL_FILL", {event["status"] for event in events})
                self.assertIn("PARTIAL_FILL", {event["status"] for event in events})
                outcome_by_member: dict[str, float] = {}
                for member_id in stats:
                    member_fills = [fill for fill in run_fills if fill.metadata.get("shadow_member_id") == member_id]
                    self.assertEqual({fill.metadata.get("outcome") for fill in member_fills}, {"yes" if member_id.startswith("mean_reversion") else "no"})
                    outcome_by_member[member_id] = sum(
                        (1.0 if getattr(fill.side, "value", fill.side) == "sell" else -1.0) * fill.quantity * fill.price
                        for fill in member_fills
                    )
                self.assertTrue(any(value > 0 for value in outcome_by_member.values()))
                self.assertTrue(any(value < 0 for value in outcome_by_member.values()))
                self.assertTrue(any(event["payload"].get("settlement") == "resolved_yes" for event in events))

                paper_state = store.load_paper_state(run_id)
                self.assertIsNotNone(paper_state)
                wallet = paper_state["state"]["portfolio"]
                self.assertEqual(paper_state["state"]["fill_count"], 4)
                self.assertEqual(paper_state["state"]["portfolio"]["cash"], wallet["cash"])
                self.assertEqual(paper_state["state"]["paper_only"], True)
                self.assertEqual(paper_state["state"]["live_execution"], False)
                canary_state = store.connection.execute("SELECT state FROM canary_control WHERE singleton=1").fetchone()["state"]
                self.assertEqual(canary_state, "DISARMED")

                snapshot = {
                    "job": copy.deepcopy(completed),
                    "manifest": copy.deepcopy(manifest),
                    "observations": copy.deepcopy(observations),
                    "events": copy.deepcopy(events),
                    "fills": [self._fill_projection(fill) for fill in run_fills],
                    "state": copy.deepcopy(paper_state),
                    "lifecycle": lifecycle_before,
                    "lifecycle_events": lifecycle_events_before,
                }

            with AxiomStore(db_path) as reopened_store:
                reopened_service = ShadowAssessmentService(reopened_store, clock=lambda: T0 + timedelta(hours=3), max_markets=1)
                reopened_provider = _SyntheticPublicFixtureProvider()
                reopened_node = ResearchNode(
                    NodeConfig(
                        db_path=db_path,
                        max_markets=1,
                        research_enabled=False,
                        crypto_enabled=False,
                        shadow_enabled=True,
                        shadow_interval=1.0,
                        shadow_jobs_per_cycle=1,
                        mutation_enabled=False,
                    ),
                    provider=reopened_provider,
                    store=reopened_store,
                    clock=lambda: T0 + timedelta(hours=3),
                )
                reopened_node.shadow_service = reopened_service
                evidence_after_restart = reopened_node._run_shadow_assessment_tick(now=T0 + timedelta(hours=3))
                self.assertEqual(evidence_after_restart["processed_jobs"], 0)
                self.assertEqual(reopened_service.register(candidate_ids, bankroll=1.0, max_cycles=1, max_observations=64, now=T0), snapshot["job"])
                job_after = reopened_store.load_shadow_job(snapshot["job"]["job_id"])
                self.assertEqual(job_after, snapshot["job"])
                self.assertEqual(reopened_store.list_paper_observations(snapshot["manifest"]["shared"]["spec"]["experiment_id"], limit=None), snapshot["observations"])
                self.assertEqual(reopened_store.list_paper_execution_events(snapshot["manifest"]["shared"]["spec"]["experiment_id"], limit=None), snapshot["events"])
                run_fills_after = [fill for fill in reopened_store.load_fills() if fill.strategy_id == snapshot["manifest"]["shared"]["spec"]["strategy_hash"]]
                self.assertEqual([self._fill_projection(fill) for fill in run_fills_after], snapshot["fills"])
                self.assertEqual(reopened_store.load_paper_state(snapshot["manifest"]["shared"]["spec"]["experiment_id"]), snapshot["state"])
                for candidate_id in candidate_ids:
                    self.assertEqual(reopened_store.load_candidate_lifecycle(candidate_id), snapshot["lifecycle"][candidate_id])
                    self.assertEqual(reopened_store.list_candidate_lifecycle_events(candidate_id, limit=100), snapshot["lifecycle_events"][candidate_id])
                self.assertEqual(reopened_store.list_shadow_jobs(), [snapshot["job"]])
                self.assertEqual(reopened_store.list_rolling_enrollments(), [])
                for table in (
                    "canary_signals",
                    "canary_signal_evaluations",
                    "canary_ledger",
                    "canary_execution_events",
                    "canary_eligibility",
                ):
                    count = reopened_store.connection.execute(f"SELECT COUNT(*) AS n FROM {table}").fetchone()["n"]
                    self.assertEqual(count, 0, table)
                self.assertEqual(
                    reopened_store.connection.execute("SELECT state FROM canary_control WHERE singleton=1").fetchone()["state"],
                    "DISARMED",
                )


if __name__ == "__main__":
    unittest.main()
