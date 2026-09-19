from __future__ import annotations

from datetime import datetime, timedelta, timezone
import os
import math
import tempfile
import threading
import time
import traceback
import unittest
import sqlite3
from unittest.mock import Mock, patch
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Mapping

from axiom.autonomous import (
    AutonomousResearchConfig,
    AutonomousResearchProcessor,
    _rolling_hash,
)
from axiom.canary_settings import CanarySettingsService
from axiom.collector import CollectionCycle, CollectorConfig, PolymarketCollector
from axiom.data import InMemoryPredictionProvider
from axiom.data.polymarket import MarketDiscoveryPage
from axiom.domain import PredictionMarketSnapshot
from axiom.dashboard import DashboardData
from axiom.experiment_plan import normalize_market_scope
from axiom.forward import (
    ForwardTestRegistry,
    _content_hash,
    _normalized_strategy_document,
    _operational_setup_for_strategy,
    _operational_setup_hash,
)
from axiom.lifecycle import CandidateLifecycleManager, CandidateStage
from axiom.market_scope import resolve_market_scope
from axiom.node import (
    ISOLATED_EXECUTION_PROFILE,
    POLYMARKET_AUTONOMY_PROTOCOL_ID,
    POLYMARKET_AUTONOMY_PROTOCOL_V1_ID,
    POLYMARKET_AUTONOMY_PROTOCOL_V2_ID,
    POLYMARKET_AUTONOMY_JOB_NAME,
    POLYMARKET_DATASET_ID,
    POLYMARKET_HISTORICAL_JOB_NAME,
    PRODUCTION_EXECUTION_PROFILE,
    NodeConfig,
    ResearchNode,
    _HistoricalRequestBudget,
    normalized_execution_profile,
)
from axiom.rolling_portfolio import RollingEvidence
from axiom.storage import AxiomStore
from axiom.strategy import validate_strategy



UTC = timezone.utc
T0 = datetime(2026, 1, 1, tzinfo=UTC)
def _rolling_initialization_document() -> dict[str, object]:
    strategy_document = {
        "version": 1,
        "market_type": "prediction",
        "family": "initialization-test",
        "parameters": {"threshold": 0.05},
        "operations": [],
        "probability_model": "fixed",
        "resolution_aware": True,
        "resolution_inputs": ["expiry", "settlement"],
    }
    return {
        "strategy_version_id": "sv-initialization",
        "strategy_id": "initialization-test",
        "version": "1",
        "strategy_hash": "sha256:initialization-test",
        "config_hash": "config:initialization-test",
        "candidate_id": "candidate-initialization",
        "research_trial_id": "trial-initialization",
        "strategy_document": strategy_document,
        "model_document": {"probability": 0.5},
        "provenance": {"candidate_id": "candidate-initialization"},
    }



def _save_attested_polymarket_dataset(
    store: AxiomStore,
    version: str,
) -> tuple[dict[str, object], dict[str, object]]:
    rows = [
        {
            "timestamp": T0.isoformat(),
            "market_id": "historical-price-proxy-market",
            "yes_mid": 0.50,
            "yes_bid": 0.49,
            "yes_ask": 0.51,
            "price": 0.50,
            "settlement": "open",
            "source_type": "HISTORICAL",
        }
    ]
    metadata = {
        "source_type": "HISTORICAL",
        "market_type": "prediction",
        "instrument": "POLYMARKET",
        "provider": "node-campaign-test",
        "research_quality": "PRICE_PROXY",
    }
    store.save_dataset(
        POLYMARKET_DATASET_ID,
        version,
        rows,
        metadata=metadata,
        quality="PRICE_PROXY",
    )
    store.save_dataset_catalog(
        POLYMARKET_DATASET_ID,
        version,
        provider="node-campaign-test",
        instrument="POLYMARKET",
        market_type="prediction",
        timeframe="event",
        start_timestamp=T0,
        end_timestamp=T0,
        row_count=len(rows),
        completeness=1.0,
        missing_ranges=(),
        quality="PRICE_PROXY",
        source_type="HISTORICAL",
        snapshot_id=f"node-campaign-test:{version}",
        metadata=metadata,
    )
    store.verify_dataset_integrity_attestation(
        POLYMARKET_DATASET_ID,
        version,
        force=True,
    )
    catalog = store.load_dataset_catalog(POLYMARKET_DATASET_ID, version)
    attestation = store.load_dataset_integrity_attestation(POLYMARKET_DATASET_ID, version)
    assert catalog is not None
    assert attestation is not None
    assert attestation["status"] == "CURRENT"
    assert attestation["contamination_result"] == "PASS"
    return catalog, attestation




class NodeConfigValidationTests(unittest.TestCase):
    def test_execution_profile_accepts_only_exact_supported_values(self) -> None:
        self.assertEqual(
            normalized_execution_profile(ISOLATED_EXECUTION_PROFILE),
            ISOLATED_EXECUTION_PROFILE,
        )
        self.assertEqual(
            normalized_execution_profile(PRODUCTION_EXECUTION_PROFILE),
            PRODUCTION_EXECUTION_PROFILE,
        )
        for malformed in ("prod", "production ", " PRODUCTION", "isolated\n", 1, False):
            with self.subTest(malformed=malformed):
                with self.assertRaisesRegex(ValueError, "execution profile"):
                    normalized_execution_profile(malformed)

    def test_missing_profile_uses_default_only_when_requested(self) -> None:
        with patch.dict(os.environ, {"AXIOM_EXECUTION_PROFILE": ""}):
            self.assertIsNone(normalized_execution_profile())
            self.assertEqual(
                normalized_execution_profile(default=PRODUCTION_EXECUTION_PROFILE),
                PRODUCTION_EXECUTION_PROFILE,
            )

    def test_node_config_rejects_malformed_profile_without_production_fallback(self) -> None:
        for malformed in ("prod", "production ", "unsafe-profile"):
            with self.subTest(malformed=malformed):
                with self.assertRaisesRegex(ValueError, "execution profile"):
                    NodeConfig(":memory:", execution_profile=malformed)
    def test_direct_construction_rejects_non_positive_depth(self) -> None:
        for depth in (0, -1, False):
            with self.subTest(depth=depth):
                with self.assertRaisesRegex(ValueError, "^depth must be a positive integer$"):
                    NodeConfig(":memory:", depth=depth)

    def test_direct_construction_rejects_invalid_failure_cooldown(self) -> None:
        for cooldown in (-1.0, math.inf, -math.inf, math.nan):
            with self.subTest(cooldown=cooldown):
                with self.assertRaisesRegex(
                    ValueError,
                    "^failure_cooldown_seconds must be finite and non-negative$",
                ):
                    NodeConfig(":memory:", failure_cooldown_seconds=cooldown)

    def test_direct_construction_accepts_positive_depth_and_zero_cooldown(self) -> None:
        config = NodeConfig(
            ":memory:",
            depth=1,
            failure_cooldown_seconds=0,
            discovery_budget_per_cycle=4,
            max_concurrency=2,
        )

        self.assertEqual(config.depth, 1)
        self.assertEqual(config.failure_cooldown_seconds, 0)
        self.assertEqual(config.discovery_budget_per_cycle, 4)
        self.assertEqual(config.max_concurrency, 2)
    def test_direct_construction_rejects_non_boolean_crypto_enabled(self) -> None:
        for crypto_enabled in (None, 0, 1, "false", object()):
            with self.subTest(crypto_enabled=crypto_enabled):
                with self.assertRaisesRegex(
                    ValueError,
                    "^crypto_enabled must be boolean$",
                ):
                    NodeConfig(":memory:", crypto_enabled=crypto_enabled)  # type: ignore[arg-type]
class HistoricalRefreshSchedulingTests(unittest.TestCase):
    def test_request_budget_counts_discovery_and_blocks_all_detail_requests(self) -> None:
        class BudgetProvider:
            provider_name = "historical-budget-test"

            def __init__(self) -> None:
                self.calls: list[str] = []

            def market_page(
                self,
                limit: int,
                *,
                after_cursor: str | None = None,
                closed: bool = False,
            ) -> MarketDiscoveryPage:
                self.calls.append("market_page")
                snapshot = PredictionMarketSnapshot(
                    timestamp=T0,
                    market_id="historical-market",
                    question="Will the event happen?",
                    yes_bid=0.4,
                    yes_ask=0.6,
                    yes_mid=0.5,
                    source="historical-budget-test",
                    yes_token_id="yes-token",
                    condition_id="condition",
                    active=False,
                    closed=True,
                )
                return MarketDiscoveryPage(
                    snapshots=(snapshot,),
                    next_cursor=None,
                    request_path="/markets/keyset",
                    query={
                        "limit": limit,
                        "after_cursor": after_cursor,
                        "closed": closed,
                    },
                    query_fingerprint="historical-budget-test",
                    raw_count=1,
                    unique_count=1,
                    duplicate_count=0,
                    malformed_count=0,
                    coverage_status="COMPLETE",
                )

            def markets(self, *args: Any, **kwargs: Any) -> None:
                self.calls.append("markets")
                raise AssertionError("markets must be blocked by the request budget")

            def market(self, *args: Any, **kwargs: Any) -> None:
                self.calls.append("market")
                raise AssertionError("market must be blocked by the request budget")

            def metadata(self, *args: Any, **kwargs: Any) -> None:
                self.calls.append("metadata")
                raise AssertionError("metadata must be blocked by the request budget")

            def price_history(self, *args: Any, **kwargs: Any) -> None:
                self.calls.append("price_history")
                raise AssertionError(
                    "price_history must be blocked by the request budget"
                )

        provider = BudgetProvider()
        facade = _HistoricalRequestBudget(provider, 1)
        facade.market_page(1, closed=True)
        for method, args in (
            ("markets", ()),
            ("market", ("historical-market",)),
            ("metadata", ("historical-market",)),
            ("price_history", ("historical-market",)),
        ):
            with self.subTest(method=method):
                with self.assertRaisesRegex(
                    RuntimeError,
                    "^historical request budget exhausted$",
                ):
                    getattr(facade, method)(*args)
        self.assertEqual(facade.requests, 1)
        self.assertEqual(provider.calls, ["market_page"])

        provider.calls.clear()
        with AxiomStore(":memory:") as store:
            node = ResearchNode(
                NodeConfig(
                    ":memory:",
                    historical_refresh_enabled=True,
                    historical_refresh_request_budget=1,
                    historical_refresh_market_budget=1,
                    max_markets=1,
                    max_attempts=1,
                    crypto_enabled=False,
                ),
                provider=InMemoryPredictionProvider([]),
                historical_provider=provider,
                store=store,
                clock=lambda: T0,
                sleep=lambda _seconds: None,
            )
            result = node._run_historical_refresh()

            self.assertEqual(provider.calls, ["market_page"])
            self.assertEqual(result["requests"], 1)
            report = result["report"]
            self.assertEqual(report["status"], "EXHAUSTED")
            self.assertEqual(report["metadata"]["request_count"], 1)
            self.assertEqual(report["metadata"]["request_budget"], 1)

            state = store.load_dataset_bootstrap_state(POLYMARKET_DATASET_ID)
            self.assertIsNotNone(state)
            assert state is not None
            self.assertEqual(state["status"], "EXHAUSTED")
            self.assertEqual(state["request_count"], 1)
            job = store.get_operator_job("polymarket-historical-refresh")
            self.assertIsNotNone(job)
            assert job is not None
            self.assertEqual(job["status"], "EXHAUSTED")
            self.assertEqual(job["payload"]["request_count"], 1)
            persisted = store.list_reports(POLYMARKET_DATASET_ID, limit=10)
            self.assertEqual(len(persisted), 1)
            self.assertEqual(
                persisted[0]["report"]["metadata"]["request_count"],
                1,
            )

    def test_campaign_start_persists_one_real_job_and_fixed_grid(self) -> None:
        metadata = {
            "source_type": "HISTORICAL",
            "market_type": "prediction",
            "instrument": "POLYMARKET",
            "provider": "node-campaign-test",
            "research_quality": "PRICE_PROXY",
        }
        rows = [
            {
                "timestamp": T0.isoformat(),
                "market_id": "campaign-market",
                "yes_mid": 0.5,
                "yes_bid": 0.49,
                "yes_ask": 0.51,
                "settlement": "open",
            }
        ]
        with AxiomStore(":memory:") as store:
            store.save_dataset(
                POLYMARKET_DATASET_ID,
                "v1",
                rows,
                metadata=metadata,
                quality="PRICE_PROXY",
            )
            store.save_dataset_catalog(
                POLYMARKET_DATASET_ID,
                "v1",
                provider="node-campaign-test",
                instrument="POLYMARKET",
                market_type="prediction",
                timeframe="event",
                start_timestamp=T0,
                end_timestamp=T0,
                row_count=len(rows),
                completeness=1.0,
                missing_ranges=(),
                quality="PRICE_PROXY",
                source_type="HISTORICAL",
                snapshot_id="Polymarket-historical:v1",
                metadata=metadata,
            )
            attestation = store.verify_dataset_integrity_attestation(
                POLYMARKET_DATASET_ID,
                "v1",
                force=True,
            )
            self.assertEqual(attestation["status"], "CURRENT")
            self.assertEqual(attestation["contamination_result"], "PASS")
            catalog = store.load_dataset_catalog(POLYMARKET_DATASET_ID, "v1")
            self.assertIsNotNone(catalog)
            assert catalog is not None

            node = ResearchNode(
                NodeConfig(":memory:", crypto_enabled=False),
                provider=InMemoryPredictionProvider([]),
                store=store,
                clock=lambda: T0,
            )
            first = node._start_polymarket_campaign(catalog, attestation, T0)
            stable_campaign_id = f"{POLYMARKET_AUTONOMY_PROTOCOL_ID}:campaign"
            campaign_job_name = (
                f"polymarket-research-campaign:{stable_campaign_id}"
            )
            durable = store.get_operator_job(campaign_job_name)
            self.assertIsNotNone(durable)
            assert durable is not None
            payload = durable["payload"]
            self.assertEqual(first["campaign_id"], stable_campaign_id)
            self.assertEqual(first["campaign_job_name"], campaign_job_name)
            self.assertEqual(first["campaign_job_status"], "RUNNING")
            self.assertEqual(first["campaign"]["status"], "RUNNING")
            self.assertEqual(durable["status"], "RUNNING")
            self.assertGreater(payload["fixed_configuration_count"], 0)
            self.assertEqual(
                len(payload["protocol"]["configuration_manifest"]),
                payload["fixed_configuration_count"],
            )
            queued = node.research_processor.bus.list_campaign_trials(
                stable_campaign_id,
                limit=100,
            )
            self.assertEqual(len(queued), 1)
            self.assertEqual(queued[0].payload["campaign_id"], stable_campaign_id)
            self.assertEqual(
                len(
                    [
                        item
                        for item in store.list_research_items(limit=100)
                        if str(item.get("payload", {}).get("campaign_id", "")).strip()
                        == stable_campaign_id
                    ]
                ),
                1,
            )

            with patch.object(
                node.research_processor,
                "start_polymarket_campaign",
                wraps=node.research_processor.start_polymarket_campaign,
            ) as resume:
                second = node._start_polymarket_campaign(catalog, attestation, T0)
            self.assertEqual(resume.call_count, 1)
            self.assertEqual(second["campaign_id"], stable_campaign_id)
            campaign_jobs = [
                item
                for item in store.list_operator_jobs()
                if str(item.get("job_name", "")).startswith(
                    "polymarket-research-campaign:"
                )
            ]
            self.assertEqual(len(campaign_jobs), 1)
            self.assertEqual(
                len(
                    node.research_processor.bus.list_campaign_trials(
                        stable_campaign_id,
                        limit=100,
                    )
                ),
                1,
            )
    def test_v1_waiting_campaign_rolls_to_v2_once_with_price_proxy_features(self) -> None:
        """An obsolete v1 input error is audited, never treated as economic."""
        from axiom.experiment_plan import ExperimentPlan

        old_campaign_id = f"{POLYMARKET_AUTONOMY_PROTOCOL_V1_ID}:campaign"
        new_campaign_id = f"{POLYMARKET_AUTONOMY_PROTOCOL_V2_ID}:campaign"
        with AxiomStore(":memory:") as store:
            v1_catalog, v1_attestation = _save_attested_polymarket_dataset(store, "v1")
            v2_catalog, v2_attestation = _save_attested_polymarket_dataset(store, "v2")
            node = ResearchNode(
                NodeConfig(":memory:", crypto_enabled=False),
                provider=InMemoryPredictionProvider([]),
                store=store,
                clock=lambda: T0,
            )
            old = node.research_processor.start_polymarket_campaign(
                old_campaign_id,
                dataset_id=POLYMARKET_DATASET_ID,
                dataset_version=str(v1_catalog["dataset_version"]),
                protocol_id=POLYMARKET_AUTONOMY_PROTOCOL_V1_ID,
                now=T0,
            )
            old_protocol = old["protocol"]
            old_queue = node.research_processor.bus.list_campaign_trials(
                old_campaign_id,
                limit=100,
            )
            self.assertEqual(len(old_queue), 1)
            node.research_processor._advance_campaign_after_result(
                old_queue[0],
                {
                    "accepted": False,
                    "reason_code": "INSUFFICIENT_DATA",
                    "candidate_id": "v1-data-error",
                },
                now=T0,
            )
            waiting = node.research_processor.campaign_state(old_campaign_id)
            self.assertIsNotNone(waiting)
            assert waiting is not None
            self.assertEqual(waiting["status"], "WAITING_FOR_DATA")
            store.set_scheduler_state(
                POLYMARKET_AUTONOMY_JOB_NAME,
                {
                    "protocol_id": POLYMARKET_AUTONOMY_PROTOCOL_V1_ID,
                    "campaign_id": old_campaign_id,
                    "dataset_id": POLYMARKET_DATASET_ID,
                    "dataset_version": str(v1_catalog["dataset_version"]),
                    "status": "WAITING_FOR_DATA",
                },
            )

            first = node._start_polymarket_campaign(
                v2_catalog,
                v2_attestation,
                T0,
            )
            old_job = store.get_operator_job(
                node.research_processor.campaign_job_name(old_campaign_id)
            )
            self.assertIsNotNone(old_job)
            assert old_job is not None
            self.assertEqual(old_job["status"], "SOFTWARE_OR_INPUT_ERROR")
            self.assertFalse(old_job["resumable"])
            self.assertEqual(old_job["payload"]["reason_code"], "SUPERSEDED_PROTOCOL")
            self.assertEqual(old_job["payload"]["supersession_reason"], "SUPERSEDED_PROTOCOL")
            self.assertEqual(old_job["payload"]["protocol"], old_protocol)
            self.assertEqual(
                old_job["payload"]["counts"]["economic_rejection"],
                0,
            )
            self.assertEqual(first["protocol_id"], POLYMARKET_AUTONOMY_PROTOCOL_V2_ID)
            self.assertEqual(first["campaign_id"], new_campaign_id)
            queued_v2 = node.research_processor.bus.list_campaign_trials(
                new_campaign_id,
                limit=100,
            )
            self.assertEqual(len(queued_v2), 1)
            v2_plan_payload = queued_v2[0].payload["experiment_plan"]
            self.assertEqual(
                v2_plan_payload["allowed_features"],
                ["timestamp", "market_id", "yes_mid"],
            )
            self.assertNotIn("settlement", v2_plan_payload["allowed_features"])
            v2_plan = ExperimentPlan.from_mapping(
                v2_plan_payload,
                hypothesis_id=queued_v2[0].payload["hypothesis_id"],
            )
            loaded_rows, _ = node.research_processor._load_split(v2_plan, boundary_override=first["campaign"]["protocol"]["dataset_boundary"])
            self.assertEqual(loaded_rows[0]["yes_mid"], 0.50)

            second = node._start_polymarket_campaign(
                v2_catalog,
                v2_attestation,
                T0,
            )
            self.assertEqual(second, first)
            self.assertEqual(
                len(
                    node.research_processor.bus.list_campaign_trials(
                        new_campaign_id,
                        limit=100,
                    )
                ),
                1,
            )
            self.assertEqual(
                store.get_operator_job(
                    node.research_processor.campaign_job_name(old_campaign_id)
                )["payload"]["reason_code"],
                "SUPERSEDED_PROTOCOL",
            )


    def test_declined_campaign_reassessment_keeps_slot_and_suppresses_repeat(self) -> None:
        with AxiomStore(":memory:") as store:
            v1_catalog, v1_attestation = _save_attested_polymarket_dataset(store, "v1")
            v2_catalog, v2_attestation = _save_attested_polymarket_dataset(store, "v2")
            node = ResearchNode(
                NodeConfig(":memory:", crypto_enabled=False),
                provider=InMemoryPredictionProvider([]),
                store=store,
                clock=lambda: T0,
            )
            first = node._start_polymarket_campaign(
                v1_catalog,
                v1_attestation,
                T0,
            )
            campaign_id = first["campaign_id"]
            job_name = node.research_processor.campaign_job_name(campaign_id)

            with patch.object(
                node.research_processor,
                "reassess_campaign",
                wraps=node.research_processor.reassess_campaign,
            ) as reassess:
                declined = node._start_polymarket_campaign(
                    v2_catalog,
                    v2_attestation,
                    T0,
                )
                suppressed = node._start_polymarket_campaign(
                    v2_catalog,
                    v2_attestation,
                    T0,
                )

            self.assertEqual(reassess.call_count, 1)
            self.assertEqual(declined["reassessment_count"], 0)
            self.assertEqual(declined["campaign"]["reassessment_count"], 0)
            self.assertIsNone(declined.get("last_reassessed_dataset_version"))
            self.assertEqual(
                declined["last_reassessment_attempted_dataset_version"],
                "v2",
            )
            self.assertEqual(declined["latest_dataset_version"], "v2")
            self.assertEqual(declined["next_work"], "wait_for_campaign_state")
            self.assertEqual(suppressed, declined)
            durable_campaign = store.get_operator_job(job_name)
            self.assertIsNotNone(durable_campaign)
            assert durable_campaign is not None
            self.assertEqual(
                durable_campaign["payload"]["reassessment_count"],
                declined["reassessment_count"],
            )
            self.assertEqual(
                store.get_scheduler_state(POLYMARKET_AUTONOMY_JOB_NAME),
                declined,
            )

    def test_actual_campaign_reassessment_persists_matching_count_once(self) -> None:
        with AxiomStore(":memory:") as store:
            v1_catalog, v1_attestation = _save_attested_polymarket_dataset(store, "v1")
            v2_catalog, v2_attestation = _save_attested_polymarket_dataset(store, "v2")
            node = ResearchNode(
                NodeConfig(":memory:", crypto_enabled=False),
                provider=InMemoryPredictionProvider([]),
                store=store,
                clock=lambda: T0,
            )
            first = node._start_polymarket_campaign(
                v1_catalog,
                v1_attestation,
                T0,
            )
            campaign_id = first["campaign_id"]
            initial_queue = node.research_processor.bus.list_campaign_trials(
                campaign_id,
                limit=100,
            )
            self.assertEqual(len(initial_queue), 1)
            node.research_processor._advance_campaign_after_result(
                initial_queue[0],
                {
                    "accepted": False,
                    "reason_code": "INSUFFICIENT_DATA",
                    "candidate_id": "node-reassessment-candidate",
                },
                now=T0 + timedelta(minutes=1),
            )
            waiting = node.research_processor.campaign_state(campaign_id)
            self.assertIsNotNone(waiting)
            assert waiting is not None
            self.assertEqual(waiting["reassessment_count"], 0)
            before_trial_ids = {
                item["trial_id"]
                for item in waiting["trials"]
                if isinstance(item, dict)
            }

            with patch.object(
                node.research_processor,
                "reassess_campaign",
                wraps=node.research_processor.reassess_campaign,
            ) as reassess:
                actual = node._start_polymarket_campaign(
                    v2_catalog,
                    v2_attestation,
                    T0 + timedelta(minutes=2),
                )
                repeated = node._start_polymarket_campaign(
                    v2_catalog,
                    v2_attestation,
                    T0 + timedelta(minutes=3),
                )

            self.assertEqual(reassess.call_count, 1)
            self.assertEqual(actual["reassessment_count"], 1)
            self.assertEqual(actual["campaign"]["reassessment_count"], 1)
            self.assertEqual(actual["last_reassessed_dataset_version"], "v2")
            self.assertEqual(actual["next_work"], "process_finite_campaign")
            self.assertEqual(repeated["reassessment_count"], 1)
            self.assertEqual(repeated["last_reassessed_dataset_version"], "v2")
            durable_campaign = node.research_processor.campaign_state(campaign_id)
            self.assertIsNotNone(durable_campaign)
            assert durable_campaign is not None
            self.assertEqual(durable_campaign["reassessment_count"], 1)
            new_trial_ids = {
                item["trial_id"]
                for item in durable_campaign["trials"]
                if isinstance(item, dict)
                and item["trial_id"] not in before_trial_ids
                and item["trial_id"].endswith(":reassessment-1")
            }
            self.assertEqual(len(new_trial_ids), 1)
            self.assertEqual(
                store.get_scheduler_state(POLYMARKET_AUTONOMY_JOB_NAME)[
                    "reassessment_count"
                ],
                durable_campaign["reassessment_count"],
            )

    def test_historical_tick_publishes_idempotent_recorded_book_replay(self) -> None:
        replay_dataset_id = "Polymarket-recorded-book-replay"
        payload = {
            "source_type": "FORWARD_COLLECTED",
            "snapshot": {
                "timestamp": T0.isoformat(),
                "market_id": "replay-market",
                "question": "Will the event happen?",
            },
            "yes_order_book": {
                "timestamp": T0.isoformat(),
                "asks": [{"price": 0.51, "size": 10.0}],
                "bids": [{"price": 0.49, "size": 10.0}],
            },
            "no_order_book": {
                "timestamp": T0.isoformat(),
                "asks": [{"price": 0.51, "size": 10.0}],
                "bids": [{"price": 0.49, "size": 10.0}],
            },
        }
        with AxiomStore(":memory:") as store:
            store.save_polymarket_snapshot(
                "forward-snapshot-1",
                "replay-market",
                T0,
                T0,
                payload,
                source_type="FORWARD_COLLECTED",
            )
            store.set_scheduler_state(
                POLYMARKET_AUTONOMY_JOB_NAME,
                {"status": "RUNNING", "campaign_id": "existing-campaign"},
            )
            node = ResearchNode(
                NodeConfig(
                    ":memory:",
                    historical_refresh_enabled=True,
                    historical_refresh_request_budget=1,
                    historical_refresh_market_budget=1,
                    crypto_enabled=False,
                ),
                provider=InMemoryPredictionProvider([]),
                historical_provider=InMemoryPredictionProvider([]),
                store=store,
                clock=lambda: T0,
                sleep=lambda _seconds: None,
            )
            with patch.object(
                node,
                "_run_historical_refresh",
                return_value={
                    "status": "COMPLETE",
                    "report": {"status": "COMPLETE", "records": 1},
                    "requests": 0,
                    "campaign": {"status": "RUNNING"},
                },
            ):
                node._run_historical_refresh_tick(cutoff=T0)
                first_state = store.get_scheduler_state(POLYMARKET_AUTONOMY_JOB_NAME)
                node._run_historical_refresh_tick(cutoff=T0)
                second_state = store.get_scheduler_state(POLYMARKET_AUTONOMY_JOB_NAME)
                restarted = ResearchNode(
                    node.config,
                    provider=InMemoryPredictionProvider([]),
                    historical_provider=InMemoryPredictionProvider([]),
                    store=store,
                    clock=lambda: T0,
                    sleep=lambda _seconds: None,
                )
                with patch.object(
                    restarted,
                    "_run_historical_refresh",
                    return_value={
                        "status": "COMPLETE",
                        "report": {"status": "COMPLETE", "records": 1},
                        "requests": 0,
                        "campaign": {"status": "RUNNING"},
                    },
                ):
                    restarted._run_historical_refresh_tick(cutoff=T0)
                restarted_state = store.get_scheduler_state(POLYMARKET_AUTONOMY_JOB_NAME)

            catalogs = store.list_dataset_catalog(
                source_type="FORWARD_COLLECTED",
                limit=100,
            )
            replay_catalogs = [
                item
                for item in catalogs
                if item.get("dataset_id") == replay_dataset_id
            ]
            self.assertEqual(len(replay_catalogs), 1)
            catalog = replay_catalogs[0]
            self.assertEqual(catalog["row_count"], 1)
            self.assertEqual(catalog["metadata"]["research_mode"], "RECORDED_BOOK_REPLAY")
            self.assertEqual(catalog["metadata"]["exact_cutoff"], T0.isoformat())
            manifest = catalog["metadata"]["snapshot_manifest"]
            self.assertEqual(len(manifest), 1)
            self.assertEqual(manifest[0]["snapshot_id"], "forward-snapshot-1")
            self.assertEqual(manifest[0]["market_id"], "replay-market")
            self.assertEqual(manifest[0]["source_timestamp"], T0.isoformat())
            self.assertTrue(manifest[0]["source_record_hash"])
            second_replay = second_state["replay"]
            self.assertEqual(second_replay["dataset_id"], replay_dataset_id)
            self.assertEqual(second_replay["dataset_version"], catalog["dataset_version"])
            self.assertEqual(second_replay["status"], "NO_NEW_DATA")
            replay = restarted_state["replay"]
            self.assertEqual(replay["dataset_id"], replay_dataset_id)
            self.assertEqual(replay["dataset_version"], catalog["dataset_version"])
            self.assertEqual(replay["cutoff"], T0.isoformat())
            self.assertEqual(replay["snapshot_manifest"], manifest)
            self.assertEqual(replay["status"], "NO_NEW_DATA")
            self.assertEqual(replay["query_limit"], 10_000)
            self.assertEqual(second_state["status"], "RUNNING")
            self.assertEqual(first_state["replay"]["status"], "PUBLISHED")
            worker = store.get_worker_state(POLYMARKET_HISTORICAL_JOB_NAME)
            self.assertIsNotNone(worker)
            assert worker is not None
            self.assertEqual(worker["payload"]["replay_dataset_version"], catalog["dataset_version"])
            self.assertEqual(worker["payload"]["replay_row_count"], 1)
            self.assertEqual(worker["payload"]["replay_cutoff"], T0.isoformat())
    def test_replay_failure_isolated_from_historical_and_campaign_state(self) -> None:
        with AxiomStore(":memory:") as store:
            store.set_scheduler_state(
                POLYMARKET_AUTONOMY_JOB_NAME,
                {"status": "RUNNING", "campaign_id": "existing-campaign"},
            )
            node = ResearchNode(
                NodeConfig(
                    ":memory:",
                    historical_refresh_enabled=True,
                    historical_refresh_request_budget=1,
                    historical_refresh_market_budget=1,
                    crypto_enabled=False,
                ),
                provider=InMemoryPredictionProvider([]),
                historical_provider=InMemoryPredictionProvider([]),
                store=store,
                clock=lambda: T0,
                sleep=lambda _seconds: None,
            )
            with patch.object(
                node,
                "_run_historical_refresh",
                return_value={
                    "status": "COMPLETE",
                    "report": {"status": "COMPLETE", "records": 1},
                    "requests": 1,
                    "campaign": {"status": "RUNNING"},
                },
            ), patch.object(
                node.historical_bootstrapper,
                "publish_polymarket_forward_replay",
                side_effect=RuntimeError("replay publication failed"),
            ):
                node._run_historical_refresh_tick(cutoff=T0)

            state = store.get_scheduler_state(POLYMARKET_AUTONOMY_JOB_NAME)
            self.assertIsNotNone(state)
            assert state is not None
            self.assertEqual(state["status"], "RUNNING")
            self.assertEqual(state["campaign_id"], "existing-campaign")
            self.assertEqual(state["replay"]["status"], "FAILED")
            self.assertEqual(state["replay"]["error"], "replay publication failed")
            worker = store.get_worker_state(POLYMARKET_HISTORICAL_JOB_NAME)
            self.assertIsNotNone(worker)
            assert worker is not None
            self.assertEqual(worker["payload"]["job_status"], "COMPLETE")
            self.assertIsNone(worker["payload"]["historical_error"])
            self.assertEqual(
                worker["payload"]["replay_error"],
                "replay publication failed",
            )
            self.assertEqual(worker["status"], "degraded")
    def test_empty_replay_is_a_non_failing_historical_tick(self) -> None:
        with AxiomStore(":memory:") as store:
            node = ResearchNode(
                NodeConfig(
                    ":memory:",
                    historical_refresh_enabled=True,
                    historical_refresh_request_budget=1,
                    historical_refresh_market_budget=1,
                    crypto_enabled=False,
                ),
                provider=InMemoryPredictionProvider([]),
                historical_provider=InMemoryPredictionProvider([]),
                store=store,
                clock=lambda: T0,
                sleep=lambda _seconds: None,
            )
            with patch.object(
                node,
                "_run_historical_refresh",
                return_value={
                    "status": "COMPLETE",
                    "report": {"status": "COMPLETE", "records": 0},
                    "requests": 0,
                    "campaign": {"status": "RUNNING"},
                },
            ):
                node._run_historical_refresh_tick(cutoff=T0)

            state = store.get_scheduler_state(POLYMARKET_AUTONOMY_JOB_NAME)
            self.assertIsNotNone(state)
            assert state is not None
            self.assertEqual(state["replay"]["status"], "EMPTY")
            self.assertEqual(state["replay"]["row_count"], 0)
            worker = store.get_worker_state(POLYMARKET_HISTORICAL_JOB_NAME)
            self.assertIsNotNone(worker)
            assert worker is not None
            self.assertEqual(worker["payload"]["job_status"], "COMPLETE")
            self.assertIsNone(worker["payload"]["replay_error"])
            self.assertEqual(worker["status"], "idle")



class NodeIdentityPersistenceTests(unittest.TestCase):
    def test_watchdog_and_root_heartbeat_keep_canonical_execution_profile(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            db = str(Path(directory) / "identity.sqlite")
            with AxiomStore(db) as store:
                node = ResearchNode(
                    NodeConfig(
                        db,
                        execution_profile="isolated",
                        interval_seconds=60.0,
                        crypto_enabled=False,
                    ),
                    provider=InMemoryPredictionProvider([]),
                    store=store,
                )
                node.started_at = T0

                # Caller payloads cannot overwrite the node-owned identity.
                node._heartbeat("running", {"execution_profile": "production"})
                root = next(
                    row for row in store.list_worker_states(limit=32)
                    if row["worker_name"] == node.config.worker_name
                )
                node._start_heartbeat_watchdog()
                try:
                    watchdog = next(
                        row for row in store.list_worker_states(limit=32)
                        if row["worker_name"] == f"{node.config.worker_name}:watchdog"
                    )
                    self.assertEqual(watchdog["payload"]["execution_profile"], "isolated")
                finally:
                    node._stop_heartbeat_watchdog()


class SchedulerScaleTests(unittest.TestCase):
    def test_independent_cadence_fairness_restart_and_wal_reads(self) -> None:
        strategy_document = {
            "version": 1,
            "market_type": "prediction",
            "family": "probability_mispricing",
            "parameters": {},
            "probability_model": "market",
            "resolution_aware": True,
            "resolution_inputs": ["expiry"],
        }
        strategy_definition = validate_strategy(strategy_document)
        model_document = {"yes_probability": 0.6}
        config = {
            "execution": "paper_only",
            "live_execution": False,
            "strategy_document": strategy_definition.to_dict(),
            "model_document": model_document,
        }
        with tempfile.TemporaryDirectory() as directory:
            db = str(Path(directory) / "scale.sqlite")
            with AxiomStore(db) as store:
                market_ids = tuple(f"market-{index:03d}" for index in range(100))
                store.set_collector_state(
                    "polymarket",
                    {
                        "scheduled_market_ids": list(market_ids),
                        "configured_interval_seconds": 0.01,
                        "stale_after_seconds": 1.0,
                    },
                )
                registry = ForwardTestRegistry(store)
                for index in range(30):
                    registry.freeze(
                        strategy=strategy_document,
                        model=model_document,
                        config=config,
                        start_timestamp=T0 + timedelta(seconds=index),
                        allowed_markets=market_ids,
                        experiment_id=f"candidate-{index:02d}",
                    )

                collector_threads: set[int] = set()
                class ScaleCollector:
                    def __init__(self) -> None:
                        self.calls: list[tuple[float, float]] = []

                    def collect_once(self) -> CollectionCycle:
                        started = datetime.now(UTC)
                        monotonic_started = time.monotonic()
                        time.sleep(0.005)
                        ended = datetime.now(UTC)
                        self.calls.append((monotonic_started, time.monotonic()))
                        collector_threads.add(threading.get_ident())
                        state = store.get_collector_state("polymarket") or {}
                        state.update(
                            {
                                "last_cycle_started_at": started.isoformat(),
                                "last_cycle_ended_at": ended.isoformat(),
                                "last_cycle_duration_seconds": (ended - started).total_seconds(),
                                "markets_seen": 100,
                                "markets_attempted": 100,
                                "markets_successful": 100,
                                "markets_failed": 0,
                                "scheduled_market_ids": list(market_ids),
                            }
                        )
                        store.set_collector_state("polymarket", state)
                        return CollectionCycle(
                            started,
                            ended,
                            100,
                            markets_attempted=100,
                            markets_successful=100,
                            markets_failed=0,
                            metadata_inserted=0,
                            snapshots_inserted=0,
                            snapshot_duplicates=0,
                            trades_inserted=0,
                            trade_duplicates=0,
                            errors=0,
                            elapsed_seconds=(ended - started).total_seconds(),
                        )

                collector = ScaleCollector()
                node = ResearchNode(
                    NodeConfig(
                        db,
                        interval_seconds=0.01,
                        max_markets=100,
                        paper_candidates_per_cycle=4,
                        historical_refresh_enabled=True,
                        crypto_enabled=False,
                        retain_cycles=16,
                    ),
                    provider=InMemoryPredictionProvider([]),
                    opportunity_model={},
                    store=store,
                    sleep=lambda _: None,
                )
                node.collector = collector  # type: ignore[assignment]
                node._configure_logging = lambda: None  # type: ignore[method-assign]
                node._start_heartbeat_watchdog = lambda: None  # type: ignore[method-assign]
                node._stop_heartbeat_watchdog = lambda: None  # type: ignore[method-assign]
                paper_started: list[float] = []
                producer_started: list[float] = []
                producer_finished: list[float] = []
                paper_finished: list[float] = []
                research_queue_runs: list[float] = []
                producer_threads: set[int] = set()

                def slow_paper_worker(spec: Any) -> dict[str, int]:
                    if not paper_started:
                        paper_started.append(time.monotonic())
                    time.sleep(0.03)
                    return {"observations_processed": 1, "fills_inserted": 0}

                def slow_research_queue() -> None:
                    research_queue_runs.append(time.monotonic())
                    time.sleep(0.04)
                    paper_finished.append(time.monotonic())
                def slow_historical_refresh() -> None:
                    producer_threads.add(threading.get_ident())
                    producer_started.append(time.monotonic())
                    time.sleep(0.04)
                    producer_finished.append(time.monotonic())

                node._run_single_paper_worker = slow_paper_worker  # type: ignore[method-assign]
                node._run_research_queue = slow_research_queue  # type: ignore[method-assign]
                node._run_historical_refresh_tick = slow_historical_refresh  # type: ignore[method-assign]
                def disabled_auto_canary_tick(*, now: datetime | None = None) -> dict[str, Any]:
                    return {
                        "status": "DISABLED",
                        "decision": "AUTONOMOUS_CANARY_DISABLED",
                        "blocker": "AUTONOMOUS_CANARY_DISABLED",
                    }

                node._auto_canary_worker.tick = disabled_auto_canary_tick  # type: ignore[method-assign]
                store.connection.execute(
                    """
                    CREATE TABLE canary_selection (
                        singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                        ranking_run_id TEXT NOT NULL,
                        candidate_id TEXT,
                        rank INTEGER,
                        total_score REAL,
                        component_scores_json TEXT NOT NULL,
                        evidence_versions_json TEXT NOT NULL,
                        reason TEXT NOT NULL,
                        selected_at TEXT NOT NULL
                    )
                    """
                )
                store.connection.execute(
                    """
                    INSERT INTO canary_selection(
                        singleton,ranking_run_id,candidate_id,rank,total_score,
                        component_scores_json,evidence_versions_json,reason,selected_at
                    ) VALUES (1,?,?,?,?,?,?,?,?)
                    """,
                    (
                        "persisted-ranking-run",
                        "persisted-candidate",
                        1,
                        0.875,
                        '{"raw":{"historical_data_integrity":"PASS","historical_execution_fidelity":"PRICE_PROXY","current_execution_evidence":"CURRENT_ORDER_BOOK_REQUIRED"}}',
                        '{"dataset_version":"persisted-v1"}',
                        "SELECTED_WINNER",
                        T0.isoformat(),
                    ),
                )
                store.connection.commit()

                class LockProbe:
                    def __init__(self) -> None:
                        self._lock = threading.RLock()
                        self._state_lock = threading.Lock()
                        self._owner: int | None = None
                        self._depth = 0

                    def acquire(self, *args: Any, **kwargs: Any) -> bool:
                        acquired = self._lock.acquire(*args, **kwargs)
                        if acquired:
                            owner = threading.get_ident()
                            with self._state_lock:
                                if self._owner == owner:
                                    self._depth += 1
                                else:
                                    self._owner = owner
                                    self._depth = 1
                        return acquired

                    def release(self) -> None:
                        with self._state_lock:
                            self._lock.release()
                            if self._owner == threading.get_ident():
                                self._depth -= 1
                                if self._depth == 0:
                                    self._owner = None

                    def __enter__(self) -> "LockProbe":
                        self.acquire()
                        return self

                    def __exit__(self, exc_type: Any, exc_value: Any, traceback_value: Any) -> None:
                        self.release()

                    def held_by_current_thread(self) -> bool:
                        with self._state_lock:
                            return self._owner == threading.get_ident() and self._depth > 0

                original_lock = store._lock
                lock_probe = LockProbe()
                store._lock = lock_probe  # type: ignore[assignment]
                canary_selection_reads: list[bool] = []

                def authorize_canary_selection(
                    _action: int,
                    table: str | None,
                    _column: str | None,
                    _database: str | None,
                    _source: str | None,
                ) -> int:
                    if _action != sqlite3.SQLITE_READ or table != "canary_selection":
                        return sqlite3.SQLITE_OK
                    canary_selection_reads.append(lock_probe.held_by_current_thread())
                    return sqlite3.SQLITE_DENY

                store.connection.set_authorizer(authorize_canary_selection)
                try:
                    locked_overview = DashboardData(store=store).overview_summary()
                    locked_canary = locked_overview["canary"]
                    self.assertIsNone(locked_canary["winner_id"])
                    self.assertIsNone(locked_canary["winner_rank"])
                    self.assertIsNone(locked_canary["selected_candidate"])
                    self.assertIsNone(locked_canary["last_selected_candidate"])
                    self.assertEqual(locked_canary["selection_status"], "UNKNOWN")
                    self.assertIsNone(locked_canary["selection_valid"])
                    self.assertEqual(locked_canary["readiness_snapshot_status"], "STALE")
                    self.assertTrue(locked_canary["readiness_snapshot_stale"])
                    self.assertEqual(
                        locked_canary["readiness_snapshot_reason"],
                        "READINESS_SNAPSHOT_MISSING",
                    )
                    self.assertIsNone(locked_canary["selection_reason"])
                    self.assertIsNone(locked_canary["selection_invalidation_reason"])
                    self.assertGreaterEqual(len(canary_selection_reads), 1)
                    self.assertLessEqual(len(canary_selection_reads), 4)
                    self.assertTrue(all(canary_selection_reads))
                    canary_selection_reads.clear()
                finally:
                    store.connection.set_authorizer(None)
                    store._lock = original_lock

                reader_started = threading.Event()
                reader_stop = threading.Event()
                reader_errors: list[str] = []

                def dashboard_reader() -> None:
                    reader_started.set()
                    while not reader_stop.is_set():
                        try:
                            overview = DashboardData(store=store).overview_summary()
                            self.assertIn("components", overview)
                        except BaseException as exc:  # pragma: no cover - assertion captured for main thread
                            reader_errors.append(
                                f"{type(exc).__name__}: {exc}\n{traceback.format_exc()}"
                            )
                            return

                reader = threading.Thread(target=dashboard_reader, daemon=True)
                reader.start()
                try:
                    self.assertTrue(reader_started.wait(timeout=2.0))
                    node.run(max_cycles=6)
                finally:
                    reader_stop.set()
                    reader.join(timeout=2.0)
                    self.assertFalse(reader.is_alive())

                self.assertFalse(reader_errors, "\n".join(reader_errors))
                self.assertEqual(len(collector.calls), 6)
                self.assertTrue(paper_started)
                self.assertTrue(paper_finished)
                self.assertTrue(research_queue_runs)
                self.assertTrue(producer_started)
                self.assertTrue(producer_finished)
                self.assertGreater(producer_finished[0] - producer_started[0], 0.03)
                self.assertTrue(collector_threads)
                self.assertTrue(producer_threads)
                self.assertTrue(producer_threads.isdisjoint(collector_threads))
                self.assertEqual(len(collector_threads), 1)
                self.assertEqual(len(producer_threads), 1)
                self.assertEqual(store.get_collector_state("polymarket")["markets_seen"], 100)

                store.connection.set_authorizer(authorize_canary_selection)
                try:
                    overview = DashboardData(store=store).overview_summary()
                finally:
                    store.connection.set_authorizer(None)
                self.assertEqual(canary_selection_reads, [])
                canary = overview["canary"]
                self.assertIsNone(canary["winner_id"])
                self.assertIsNone(canary["winner_rank"])
                self.assertIsNone(canary["selected_candidate"])
                self.assertEqual(canary["last_selected_candidate"], "persisted-candidate")
                self.assertIsNone(canary["winner_score"])
                self.assertEqual(canary["selection_reason"], "SELECTED_WINNER")
                self.assertIsNone(canary["selection_invalidation_reason"])
                self.assertEqual(canary["selection_status"], "UNKNOWN")
                self.assertIsNone(canary["selection_valid"])
                self.assertEqual(canary["readiness_snapshot_status"], "STALE")
                self.assertTrue(canary["readiness_snapshot_stale"])
                self.assertEqual(
                    canary["readiness_snapshot_reason"], "READINESS_SNAPSHOT_INITIALIZING"
                )
                self.assertIsNone(canary["eligible_count"])
                self.assertIsNone(canary["rankable_count"])
                self.assertEqual(canary["execution_event_count"], 0)
                components = {item["name"]: item for item in overview["components"]}
                self.assertIn("POLYMARKET COLLECTOR", components)
                self.assertIn("PAPER ENGINE", components)
                self.assertIn("RESEARCH ENGINE", components)
                self.assertEqual(components["POLYMARKET COLLECTOR"]["state"], "IDLE")
                self.assertEqual(components["PAPER ENGINE"]["state"], "IDLE")
                self.assertEqual(components["RESEARCH ENGINE"]["state"], "STOPPED")
                health = store.polymarket_health(expected_interval_seconds=0.01, stale_after_seconds=1.0)
                self.assertEqual(health["configured_interval_seconds"], 0.01)
                self.assertIsNotNone(health["next_scheduled_collection_at"])
                self.assertIsNotNone(health["worker_heartbeat_at"])
                batches: list[list[str]] = []
                for _ in range(8):
                    stats = node._run_paper_workers()
                    batch = list(stats["processed_candidate_ids"])
                    batches.append(batch)
                    self.assertEqual(len(batch), len(set(batch)))
                self.assertEqual(len({item for batch in batches for item in batch}), 30)
                self.assertEqual(batches[0], [f"candidate-{index:02d}" for index in range(4, 8)])
                self.assertEqual(batches[-1], ["candidate-02", "candidate-03", "candidate-04", "candidate-05"])
                scheduler_state = store.get_scheduler_state("paper-engine")
                self.assertEqual(scheduler_state["candidate_count"], 30)
                self.assertEqual(scheduler_state["next_candidate_id"], "candidate-06")

                restarted = ResearchNode(
                    NodeConfig(
                        db,
                        interval_seconds=0.01,
                        max_markets=100,
                        paper_candidates_per_cycle=4,
                        crypto_enabled=False,
                    ),
                    provider=InMemoryPredictionProvider([]),
                    store=store,
                )
                restarted._run_single_paper_worker = slow_paper_worker  # type: ignore[method-assign]
                next_batch = restarted._run_paper_workers()["processed_candidate_ids"]
                self.assertEqual(next_batch, ["candidate-06", "candidate-07", "candidate-08", "candidate-09"])
                paper_worker = next(row for row in store.list_worker_states(limit=64) if row["worker_name"] == "paper-engine")
                self.assertEqual(paper_worker["status"], "idle")
                collector_worker = next(row for row in store.list_worker_states(limit=64) if row["worker_name"] == "polymarket-collector")
                self.assertEqual(collector_worker["payload"]["configured_interval_seconds"], 0.01)
                self.assertIn("next_scheduled_collection_at", collector_worker["payload"])



class MutationSchedulingTests(unittest.TestCase):
    def test_mutation_disabled_never_ticks_or_starts_auto_canary(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            db = str(Path(directory) / "mutation-disabled.sqlite")
            with AxiomStore(db) as store:
                node = ResearchNode(
                    NodeConfig(
                        db,
                        mutation_enabled=False,
                        crypto_enabled=False,
                    ),
                    provider=InMemoryPredictionProvider([]),
                    store=store,
                )
                threads = [Mock(), Mock(), Mock(), Mock()]
                with patch.object(node._auto_canary_worker, "tick") as tick, patch(
                    "axiom.node.threading.Thread", side_effect=threads
                ) as thread_factory:
                    node._start_worker_threads(max_cycles=1)
                    self.assertIsNone(node._auto_canary_thread)
                    self.assertEqual(thread_factory.call_count, 4)
                    self.assertEqual(
                        [call.kwargs["name"] for call in thread_factory.call_args_list],
                        [
                            "axiom-node-collector",
                            "axiom-node-research",
                            "axiom-node-health",
                            "axiom-node-rolling-portfolio",
                        ],
                    )
                    self.assertIs(node._collector_thread, threads[0])
                    self.assertIs(node._research_thread, threads[1])
                    self.assertIs(node._health_thread, threads[2])
                    self.assertIs(node._rolling_portfolio_thread, threads[3])
                    self.assertEqual(
                        thread_factory.call_args_list[3].kwargs["target"].__name__,
                        "_rolling_portfolio_worker_loop",
                    )
                    self.assertNotIn(
                        "autonomous-canary",
                        node._worker_thread_specs(max_cycles=1),
                    )

                node._auto_canary_worker_loop()
                tick.assert_not_called()
                auto_state = next(
                    row
                    for row in store.list_worker_states(limit=64)
                    if row["worker_name"] == "autonomous-canary"
                )
                self.assertEqual(auto_state["status"], "disabled")
                self.assertEqual(
                    auto_state["payload"]["decision"],
                    "AUTONOMOUS_CANARY_DISABLED",
                )
                self.assertEqual(
                    auto_state["payload"]["blocker"],
                    "AUTONOMOUS_CANARY_DISABLED",
                )
    def test_rolling_tick_reviews_new_enrollment_before_daily_due_once(self) -> None:
        class StopAfterOneWait(threading.Event):
            def wait(self, timeout: float | None = None) -> bool:
                self.set()
                return True

        with tempfile.TemporaryDirectory() as directory:
            db = str(Path(directory) / "rolling-initialization.sqlite")
            enrolled = False
            strategy_document = {
                "version": 1,
                "market_type": "prediction",
                "family": "initialization-test",
                "parameters": {"threshold": 0.05},
                "operations": [],
                "probability_model": "fixed",
                "resolution_aware": True,
                "resolution_inputs": ["expiry", "settlement"],
            }
            rolling_document = {
                "strategy_version_id": "sv-initialization",
                "strategy_id": "initialization-test",
                "version": "1",
                "strategy_hash": "sha256:initialization-test",
                "config_hash": "config:initialization-test",
                "candidate_id": "candidate-initialization",
                "research_trial_id": "trial-initialization",
                "strategy_document": strategy_document,
                "model_document": {"probability": 0.5},
                "provenance": {"candidate_id": "candidate-initialization"},
            }

            def run_tick(at: datetime) -> tuple[dict[str, Any], dict[str, Any], int]:
                with AxiomStore(db) as store:
                    node = ResearchNode(
                        NodeConfig(
                            db,
                            mutation_enabled=False,
                            crypto_enabled=False,
                            rolling_review_interval_seconds=86400,
                        ),
                        provider=InMemoryPredictionProvider([]),
                        store=store,
                        clock=lambda: at,
                    )
                    processor = node.research_processor
                    processor._rolling_strategy_documents = (  # type: ignore[method-assign]
                        lambda: (rolling_document,) if enrolled else ()
                    )
                    processor._rolling_source_rows = lambda *_args: []  # type: ignore[method-assign]
                    node.stop_event = StopAfterOneWait()
                    node._rolling_portfolio_worker_loop()
                    worker = store.get_worker_state("rolling-portfolio")
                    self.assertIsNotNone(worker)
                    assert worker is not None
                    self.assertEqual(
                        worker["payload"]["evidence_interval_seconds"],
                        node.config.rolling_evidence_interval_seconds,
                    )
                    self.assertEqual(
                        worker["payload"]["review_interval_seconds"],
                        node.config.rolling_review_interval_seconds,
                    )
                    self.assertNotEqual(
                        worker["payload"]["evidence_interval_seconds"],
                        worker["payload"]["review_interval_seconds"],
                    )
                    review = store.load_portfolio_review_state()
                    selection = store.load_current_portfolio_selection()
                    self.assertIsNotNone(review)
                    self.assertIsNotNone(selection)
                    assert review is not None
                    assert selection is not None
                    return review, selection, len(store.list_portfolio_selections(limit=32))

            first_review, first_selection, first_count = run_tick(T0)
            self.assertEqual(first_selection["members"], [])
            self.assertEqual(first_review["status"], "OBSERVE")
            self.assertTrue(
                any(
                    item.get("reason") == "NO_STRATEGY_DEFINITIONS"
                    for item in first_review["pending"]
                )
            )
            self.assertEqual(first_review["refresh_identity"]["strategy_versions_total"], 0)
            first_due = first_review["review_due_at"]
            self.assertEqual(first_count, 1)

            enrolled = True
            second_review, second_selection, second_count = run_tick(
                T0 + timedelta(hours=1)
            )
            self.assertIn("rolling-research-evidence", second_review["next_jobs"])
            self.assertEqual(second_selection["members"], [])
            self.assertEqual(second_review["status"], "OBSERVE")
            self.assertEqual(
                second_review["refresh_identity"]["strategy_versions_total"], 1
            )
            self.assertNotEqual(second_review["review_due_at"], first_due)
            self.assertEqual(second_count, 2)
            self.assertNotIn(
                "NO_STRATEGY_DEFINITIONS",
                {item.get("reason") for item in second_review["pending"]},
            )
            self.assertTrue(
                any(
                    str(reason).startswith("sv-initialization:")
                    for reason in second_review["reasons"]
                )
            )
            third_review, third_selection, third_count = run_tick(
                T0 + timedelta(hours=1)
            )
            self.assertEqual(third_selection["portfolio_selection_id"], second_selection["portfolio_selection_id"])
            self.assertEqual(third_review["event_history"], second_review["event_history"])
            self.assertEqual(third_count, second_count)

            with AxiomStore(db) as store:
                for days in (7, 30):
                    evidence = RollingEvidence(
                        strategy_version_id="sv-initialization",
                        evidence_window_id=f"window-initialization-{days}",
                        candidate_id="candidate-initialization",
                        research_trial_id="trial-initialization",
                        available_from=T0 - timedelta(days=days),
                        available_through=T0,
                        requested_days=days,
                        actual_coverage_seconds=days * 86400,
                        observation_completeness="1",
                        source_class="HISTORICAL",
                        paper_sizing="10",
                        allocated_capital_net_return="8",
                        realized_pnl="8",
                        completed_outcomes=12,
                        reliability="0.90",
                        execution_feasibility="TRUE",
                        overlap_key="overlap:initialization",
                    ).as_dict()
                    evidence["rolling_research"] = True
                    store.save_strategy_evidence_window(evidence)
            promoted_review, promoted_selection, promoted_count = run_tick(
                T0 + timedelta(days=2)
            )
            self.assertEqual(promoted_review["status"], "PAPER")
            self.assertEqual(promoted_count, 3)
            self.assertEqual(
                [item["strategy_version_id"] for item in promoted_selection["members"]],
                ["sv-initialization"],
            )
            self.assertEqual(promoted_selection["members"][0]["allocation"], "0")
    def test_rolling_observation_materializes_supported_strategy_identity(self) -> None:
        document = _rolling_initialization_document()
        strategy_document = dict(document["strategy_document"])
        strategy_document["family"] = "probability_mispricing"
        document["strategy_document"] = strategy_document
        document.pop("strategy_hash", None)
        document["market_scope"] = {
            "schema_version": "1",
            "mode": "EXACT_MARKETS",
            "instrument": "POLYMARKET",
            "categories": [],
            "market_ids": ["market-initialization"],
            "filters": {},
            "regime_restrictions": {},
            "provenance": "canonical",
        }

        with tempfile.TemporaryDirectory() as directory:
            db = str(Path(directory) / "rolling-observation.sqlite")
            with AxiomStore(db) as store:
                current_market = {
                    "market_id": "market-initialization",
                    "condition_id": "condition-market-initialization",
                    "yes_token_id": "yes-market-initialization",
                    "no_token_id": "no-market-initialization",
                    "metadata_provenance": {"source_type": "CURRENT"},
                    "source_type": "CURRENT",
                    "provider": "polymarket",
                    "venue": "POLYMARKET",
                    "instrument": "POLYMARKET",
                    "active": True,
                    "closed": False,
                    "settlement": "OPEN",
                    "enable_order_book": True,
                    "accepting_orders": True,
                }
                proof = resolve_market_scope(
                    "candidate-initialization",
                    {"market_scope": document["market_scope"]},
                    [current_market],
                    resolved_at=T0,
                )
                document["scope_resolution"] = proof.as_dict()
                document["scope_resolution_freshness_sla_seconds"] = 60
                store.save_market_scope_resolution(proof)
                processor = AutonomousResearchProcessor(store, clock=lambda: T0)
                result = processor._ensure_rolling_paper_observation(
                    document,
                    T0,
                    market_ids=("market-initialization",),
                )
                registry = ForwardTestRegistry(store)
                intents = registry.list_observation_intents()
                materialized = [
                    spec for spec in registry.list() if spec.allowed_markets
                ]

                self.assertEqual(result["candidate_id"], "candidate-initialization")
                self.assertEqual(len(intents), 1)
                self.assertEqual(intents[0].experiment_id, result["intent_id"])
                self.assertEqual(len(materialized), 1)
                spec = materialized[0]
                self.assertEqual(
                    {
                        key: spec.config[key]
                        for key in (
                            "candidate_id",
                            "strategy_version_id",
                            "research_trial_id",
                        )
                    },
                    {
                        "candidate_id": "candidate-initialization",
                        "strategy_version_id": "sv-initialization",
                        "research_trial_id": "trial-initialization",
                    },
                )
                self.assertTrue(spec.config["market_authority_required"])
                self.assertEqual(spec.allowed_markets, ("market-initialization",))
                self.assertEqual(
                    spec.config["strategy_document"]["family"],
                    "probability_mispricing",
                )

    def test_registry_strict_successor_setup_and_generic_campaign_compatibility(self) -> None:
        malformed_directional = {
            "version": 1,
            "market_type": "prediction",
            "family": "momentum",
            "parameters": {
                "lookback": 1,
                "threshold": 0.10,
                "entry_predicate": {
                    "version": "absolute-move-v1",
                    "minimum_move": 0.05,
                    "units": "probability",
                    "boundary": "inclusive",
                },
            },
            "probability_model": "market-history",
            "resolution_aware": True,
            "resolution_inputs": ["market_history"],
        }
        for strict_fields in (
            {
                "observation_handoff": {
                    "version": "forged-handoff-v1",
                    "predecessor_observation_intent_id": "legacy-intent",
                },
                "canonical_operational_setup_required": True,
            },
            {"canonical_operational_setup_required": True},
        ):
            with self.subTest(strict_fields=strict_fields):
                with self.assertRaisesRegex(
                    ValueError, "OPERATIONAL_SETUP_UNSUPPORTED"
                ):
                    ForwardTestRegistry().register_observation_intent(
                        strategy=malformed_directional,
                        model={"model_required": False},
                        config={
                            "strategy_document": malformed_directional,
                            "model_document": {"model_required": False},
                            **strict_fields,
                        },
                        registration_timestamp=T0,
                        candidate_id="forged-successor-" + str(len(strict_fields)),
                    )
        malformed_predicate = {
            **malformed_directional,
            "parameters": {
                "entry_predicate": {
                    "version": "unsupported-predicate",
                }
            },
        }
        with self.assertRaisesRegex(ValueError, "OPERATIONAL_SETUP_UNSUPPORTED"):
            ForwardTestRegistry().register_observation_intent(
                strategy=malformed_predicate,
                model={"model_required": False},
                config={
                    "strategy_document": malformed_predicate,
                    "model_document": {"model_required": False},
                    "canonical_operational_setup_required": True,
                },
                registration_timestamp=T0,
                candidate_id="strict-malformed-predicate",
            )


        generic = {
            "version": 1,
            "market_type": "prediction",
            "family": "probability_mispricing",
            "parameters": {},
            "probability_model": "market",
            "resolution_aware": True,
            "resolution_inputs": ["expiry"],
        }
        spec = ForwardTestRegistry().register_observation_intent(
            strategy=generic,
            model={"yes_probability": 0.6},
            config={
                "strategy_document": generic,
                "model_document": {"yes_probability": 0.6},
                "operational_setup": {"legacy_setup": "accepted"},
                "operational_setup_hash": "legacy-setup-hash",
            },
            registration_timestamp=T0,
            candidate_id="generic-predeclared-campaign",
        )
        generic_handoff = ForwardTestRegistry().register_observation_intent(
            strategy=generic,
            model={"yes_probability": 0.6},
            config={
                "strategy_document": generic,
                "model_document": {"yes_probability": 0.6},
                "observation_handoff": {
                    "version": "legacy-generic-handoff",
                    "predecessor_observation_intent_id": "generic-legacy-intent",
                },
            },
            registration_timestamp=T0,
            candidate_id="generic-handoff-without-setup",
        )
        self.assertNotIn("operational_setup", generic_handoff.config)
        self.assertEqual(
            generic_handoff.config["observation_handoff"]["version"],
            "legacy-generic-handoff",
        )
        self.assertEqual(spec.config["operational_setup"], {"legacy_setup": "accepted"})

    def test_rolling_successor_binds_setup_and_paper_worker_processes_observation(self) -> None:
        candidate_id = "rolling-setup-successor"
        market_id = "rolling-setup-market"
        strategy_document = {
            "version": 1,
            "market_type": "prediction",
            "family": "momentum",
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
            "probability_model": "market-history",
            "resolution_aware": True,
            "resolution_inputs": ["market_history"],
        }
        policy = normalize_market_scope(
            {
                **normalize_market_scope(
                    market_ids=[market_id],
                    target_instrument="POLYMARKET",
                ).as_dict(),
                "provenance": "canonical",
            }
        )
        source = {
            "candidate_id": candidate_id,
            "strategy_version_id": "rolling-setup-version",
            "research_trial_id": "rolling-setup-trial",
            "strategy_hash": _content_hash(_normalized_strategy_document(strategy_document)),
            "strategy_document": strategy_document,
            "model_document": {"model_required": False},
            "market_scope": policy.as_dict(),
            "plan_id": "rolling-setup-plan",
            "plan_hash": _rolling_hash({"plan_id": "rolling-setup-plan", "version": 1}),
            "dataset_id": "rolling-setup-history",
            "dataset_version": "v1",
            "dataset_attestation": {
                "dataset_id": "rolling-setup-history",
                "dataset_version": "v1",
                "integrity": "sha256:rolling-setup-history",
            },
        }
        current_market = {
            "market_id": market_id,
            "condition_id": "condition-" + market_id,
            "yes_token_id": "yes-" + market_id,
            "no_token_id": "no-" + market_id,
            "metadata_provenance": {"source_type": "CURRENT"},
            "source_type": "CURRENT",
            "provider": "polymarket",
            "venue": "POLYMARKET",
            "instrument": "POLYMARKET",
            "active": True,
            "closed": False,
            "settlement": "OPEN",
            "enable_order_book": True,
            "accepting_orders": True,
        }
        with tempfile.TemporaryDirectory() as directory:
            db = str(Path(directory) / "rolling-setup-worker.sqlite")
            with AxiomStore(db) as store:
                proof = resolve_market_scope(
                    candidate_id,
                    {"market_scope": policy.as_dict()},
                    [current_market],
                    resolved_at=T0,
                )
                store.save_market_scope_resolution(proof)
                processor = AutonomousResearchProcessor(store, clock=lambda: T0)
                binding = processor._ensure_rolling_paper_observation(
                    source,
                    T0,
                    market_ids=(market_id,),
                )
                registry = ForwardTestRegistry(store)
                intent = registry.get(binding["intent_id"])
                self.assertIsNotNone(intent)
                assert intent is not None
                setup = intent.config.get("operational_setup")
                self.assertIsInstance(setup, Mapping)
                assert isinstance(setup, Mapping)
                self.assertEqual(
                    intent.config["operational_setup_hash"],
                    _operational_setup_hash(setup),
                )
                lifecycle_payload = {
                    **dict(intent.config),
                    "candidate_id": candidate_id,
                    "schema_valid": True,
                    "paper_observation_intent": True,
                    "paper_observation_intent_id": intent.experiment_id,
                    "paper_only": True,
                    "research_only": True,
                }
                store.save_candidate_lifecycle(
                    candidate_id,
                    CandidateStage.IDEA.value,
                    {"candidate_id": candidate_id},
                    timestamp=T0,
                )
                CandidateLifecycleManager(store).advance(
                    candidate_id,
                    CandidateStage.SCHEMA_VALIDATED,
                    lifecycle_payload,
                    reason="schema intent registered",
                )
                collector = PolymarketCollector(
                    InMemoryPredictionProvider([]),
                    store,
                    CollectorConfig(max_markets=1),
                    clock=lambda: T0,
                    sleep=lambda _seconds: None,
                )
                counters = collector._new_counters()
                ready = collector._materialize_observation_intents(
                    T0,
                    (candidate_id,),
                    {candidate_id: [market_id]},
                    counters,
                    scope_resolutions={candidate_id: proof.as_dict()},
                )
                collector.close()
                lifecycle = store.load_candidate_lifecycle(candidate_id)
                self.assertIsNotNone(lifecycle)
                assert lifecycle is not None
                self.assertEqual(lifecycle["stage"], CandidateStage.PAPER_FORWARD.value)
                self.assertEqual(
                    lifecycle["payload"]["operational_setup_hash"],
                    intent.config["operational_setup_hash"],
                )
                successor = registry.get(lifecycle["payload"]["forward_test_id"])
                self.assertIsNotNone(successor)
                assert successor is not None
                store.save_polymarket_snapshot(
                    "rolling-setup-snapshot",
                    market_id,
                    T0,
                    T0,
                    {
                        "source_type": "FORWARD_COLLECTED",
                        "snapshot": {
                            "timestamp": T0.isoformat(),
                            "market_id": market_id,
                            "yes_mid": 0.50,
                            "yes_bid": 0.49,
                            "yes_ask": 0.51,
                            "no_mid": 0.50,
                            "no_bid": 0.49,
                            "no_ask": 0.51,
                            "settlement": "OPEN",
                        },
                    },
                    source_type="FORWARD_COLLECTED",
                )
                CanarySettingsService(store, clock=lambda: T0)
                node = ResearchNode(
                    NodeConfig(
                        db,
                        crypto_enabled=False,
                        paper_observations_per_candidate=1,
                    ),
                    provider=InMemoryPredictionProvider([]),
                    store=store,
                    clock=lambda: T0 + timedelta(minutes=1),
                    sleep=lambda _seconds: None,
                )
                result = node._run_single_paper_worker(successor)
                self.assertNotIn("error", result or {})
                self.assertGreaterEqual((result or {}).get("observations_seen", 0), 1)
                worker = next(
                    row
                    for row in store.list_worker_states(limit=32)
                    if row["worker_name"] == f"paper:{successor.experiment_id}"
                )
                self.assertEqual(worker["status"], "idle")
    def test_materialized_successor_rebinds_current_markets_and_remains_schedulable(self) -> None:
        candidate_id = "rolling-materialized-current-set"
        market_ids = tuple(f"rolling-materialized-market-{index}" for index in range(8))
        capture_market_id = market_ids[0]
        strategy_document = {
            "version": 1,
            "market_type": "prediction",
            "family": "momentum",
            "parameters": {"lookback": 1, "threshold": 0.05},
            "probability_model": "market-history",
            "resolution_aware": True,
            "resolution_inputs": ["market_history"],
        }
        policy = normalize_market_scope(
            {
                **normalize_market_scope(
                    market_ids=market_ids,
                    target_instrument="POLYMARKET",
                ).as_dict(),
                "provenance": "canonical",
            }
        )

        def market(market_id: str) -> dict[str, Any]:
            return {
                "market_id": market_id,
                "condition_id": "condition-" + market_id,
                "yes_token_id": "yes-" + market_id,
                "no_token_id": "no-" + market_id,
                "metadata_provenance": {"source_type": "CURRENT"},
                "source_type": "CURRENT",
                "provider": "polymarket",
                "venue": "POLYMARKET",
                "instrument": "POLYMARKET",
                "active": True,
                "closed": False,
                "settlement": "OPEN",
                "enable_order_book": True,
                "accepting_orders": True,
            }

        strategy_hash = _content_hash(
            _normalized_strategy_document(strategy_document)
        )
        with tempfile.TemporaryDirectory() as directory:
            db = str(Path(directory) / "rolling-materialized-current-set.sqlite")
            with AxiomStore(db) as store:
                registry = ForwardTestRegistry(store)
                old_proof = resolve_market_scope(
                    candidate_id,
                    {"market_scope": policy.as_dict()},
                    [market(capture_market_id)],
                    resolved_at=T0,
                )
                store.save_market_scope_resolution(old_proof)
                old_config = {
                    "observation_intent": True,
                    "observation_only_lineage": True,
                    "market_authority_required": False,
                    "candidate_id": candidate_id,
                    "strategy_version_id": "rolling-materialized-version",
                    "research_trial_id": "rolling-materialized-trial",
                    "source_strategy_hash": strategy_hash,
                    "rolling_strategy_hash": strategy_hash,
                    "strategy_document": strategy_document,
                    "model_document": {"model_required": False},
                    "rolling_research": True,
                    "paper_only": True,
                    "observation_capture_only": True,
                    "execution_scope": "OBSERVATION",
                    "research_only": True,
                    "selection_excluded": True,
                    "allocation_active": False,
                    "canary_armed": False,
                    "capture_market_id": capture_market_id,
                    "current_market_ids": [capture_market_id],
                    "scope": policy.as_dict(),
                    "market_scope": policy.as_dict(),
                    "scope_resolution": old_proof.as_dict(),
                    "scope_resolution_freshness_sla_seconds": 600.0,
                }
                old_config.update(
                    {
                        "plan_hash": _rolling_hash(
                            {"plan_id": "rolling-materialized-plan"}
                        ),
                        "plan_id": "rolling-materialized-plan",
                        "dataset_selector": {
                            "dataset_id": "rolling-materialized-history",
                            "dataset_version": "v1",
                        },
                        "dataset_attestation": {
                            "dataset_id": "rolling-materialized-history",
                            "dataset_version": "v1",
                            "integrity": "sha256:rolling-materialized-history",
                        },
                        "market_scope_hash": policy.scope_hash,
                        "market_scope_version": policy.scope_version,
                    }
                )
                old_intent = registry.register_observation_intent(
                    strategy=strategy_document,
                    model={"model_required": False},
                    config=old_config,
                    registration_timestamp=T0,
                    candidate_id=candidate_id,
                    strategy_version_id=old_config["strategy_version_id"],
                    research_trial_id=old_config["research_trial_id"],
                    source_strategy_hash=strategy_hash,
                    rolling_strategy_hash=strategy_hash,
                    scope=policy.as_dict(),
                    scope_resolution=old_proof.as_dict(),
                )
                new_proof = resolve_market_scope(
                    candidate_id,
                    {"market_scope": policy.as_dict()},
                    [market(market_id) for market_id in market_ids],
                    resolved_at=T0 + timedelta(minutes=1),
                )
                store.save_market_scope_resolution(new_proof)
                materialized = registry.materialize_observation_intent(
                    old_intent,
                    allowed_markets=market_ids,
                    registration_timestamp=T0 + timedelta(minutes=1),
                    now=T0 + timedelta(minutes=1),
                    candidate_id=candidate_id,
                    scope_resolution=new_proof.as_dict(),
                )
                self.assertEqual(materialized.allowed_markets, market_ids)
                persisted_old_intent = ForwardTestRegistry(store).get(
                    old_intent.experiment_id
                )
                self.assertIsNotNone(persisted_old_intent)
                assert persisted_old_intent is not None
                self.assertEqual(
                    tuple(persisted_old_intent.config["current_market_ids"]),
                    (capture_market_id,),
                )
                self.assertEqual(
                    tuple(materialized.config["current_market_ids"]),
                    market_ids,
                )
                self.assertIn(
                    materialized.config["capture_market_id"],
                    materialized.allowed_markets,
                )

                retry = ForwardTestRegistry(store).materialize_observation_intent(
                    old_intent,
                    allowed_markets=market_ids,
                    registration_timestamp=T0 + timedelta(hours=1),
                    now=T0 + timedelta(hours=1),
                    candidate_id=candidate_id,
                    scope_resolution=new_proof.as_dict(),
                )
                self.assertEqual(retry.as_record(), materialized.as_record())

                lifecycle_payload = {
                    **dict(materialized.config),
                    "candidate_id": candidate_id,
                    "paper_observation_intent": True,
                    "paper_observation_intent_id": old_intent.experiment_id,
                    "forward_test_id": materialized.experiment_id,
                    "allowed_markets": list(materialized.allowed_markets),
                    "current_market_ids": list(market_ids),
                    "resolved_market_ids": list(market_ids),
                    "scope_resolution": new_proof.as_dict(),
                    "market_scope_resolution": new_proof.as_dict(),
                }
                lifecycle_payload["paper_forward_started"] = True
                lifecycle_payload["holdout_used"] = False
                lifecycle_payload["observation_handoff"] = {
                    "version": "observation-capture-v1",
                    "predecessor_candidate_id": candidate_id,
                    "predecessor_observation_intent_id": old_intent.experiment_id,
                    "predecessor_profitability_inherited": False,
                    "predecessor_allocation_authority_inherited": False,
                }
                store.save_candidate_lifecycle(
                    candidate_id,
                    CandidateStage.IDEA.value,
                    {"candidate_id": candidate_id},
                    timestamp=T0,
                )
                lifecycle_manager = CandidateLifecycleManager(store)
                lifecycle_manager.advance(
                    candidate_id,
                    CandidateStage.SCHEMA_VALIDATED.value,
                    {"candidate_id": candidate_id, "schema_valid": True},
                    reason="schema validated",
                )
                lifecycle_manager.advance(
                    candidate_id,
                    CandidateStage.PAPER_FORWARD.value,
                    lifecycle_payload,
                    reason="materialized current scope",
                    observation_only=True,
                )
                lifecycle = store.load_candidate_lifecycle(candidate_id)
                self.assertIsNotNone(lifecycle)
                assert lifecycle is not None
                self.assertEqual(
                    lifecycle["payload"]["forward_test_id"],
                    materialized.experiment_id,
                )
                self.assertEqual(
                    tuple(lifecycle["payload"]["allowed_markets"]),
                    market_ids,
                )
                self.assertEqual(
                    tuple(lifecycle["payload"]["current_market_ids"]),
                    market_ids,
                )

                store.save_polymarket_snapshot(
                    "rolling-materialized-current-snapshot",
                    capture_market_id,
                    T0,
                    T0,
                    {
                        "source_type": "FORWARD_COLLECTED",
                        "snapshot": {
                            "timestamp": T0.isoformat(),
                            "market_id": capture_market_id,
                            "yes_mid": 0.50,
                            "yes_bid": 0.49,
                            "yes_ask": 0.51,
                            "no_mid": 0.50,
                            "no_bid": 0.49,
                            "no_ask": 0.51,
                            "settlement": "OPEN",
                        },
                    },
                    source_type="FORWARD_COLLECTED",
                )
                node = ResearchNode(
                    NodeConfig(
                        db,
                        crypto_enabled=False,
                        paper_observations_per_candidate=1,
                    ),
                    provider=InMemoryPredictionProvider([]),
                    store=store,
                    clock=lambda: T0 + timedelta(minutes=2),
                    sleep=lambda _seconds: None,
                )
                worker = node._run_single_paper_worker(materialized)
                self.assertNotIn("error", worker or {})
                self.assertNotEqual((worker or {}).get("status"), "BLOCKED")
                self.assertGreaterEqual((worker or {}).get("observations_seen", 0), 1)

                bad_candidate = "rolling-materialized-invalid-capture"
                bad_old_proof = resolve_market_scope(
                    bad_candidate,
                    {"market_scope": policy.as_dict()},
                    [market(capture_market_id)],
                    resolved_at=T0,
                )
                bad_intent = registry.register_observation_intent(
                    strategy=strategy_document,
                    model={"model_required": False},
                    config={
                        **old_config,
                        "candidate_id": bad_candidate,
                        "capture_market_id": "outside-current-scope",
                        "scope_resolution": bad_old_proof.as_dict(),
                    },
                    registration_timestamp=T0,
                    candidate_id=bad_candidate,
                    strategy_version_id=old_config["strategy_version_id"],
                    research_trial_id=old_config["research_trial_id"],
                    source_strategy_hash=strategy_hash,
                    rolling_strategy_hash=strategy_hash,
                    scope=policy.as_dict(),
                    scope_resolution=bad_old_proof.as_dict(),
                )
                bad_proof = resolve_market_scope(
                    bad_candidate,
                    {"market_scope": policy.as_dict()},
                    [market(market_id) for market_id in market_ids],
                    resolved_at=T0 + timedelta(minutes=1),
                )
                with self.assertRaisesRegex(
                    ValueError, "CAPTURE_MARKET_BINDING_INVALID"
                ):
                    registry.materialize_observation_intent(
                        bad_intent,
                        allowed_markets=market_ids,
                        registration_timestamp=T0 + timedelta(minutes=1),
                        now=T0 + timedelta(minutes=1),
                        candidate_id=bad_candidate,
                        scope_resolution=bad_proof.as_dict(),
                    )

    def test_capture_scope_churn_keeps_bound_market_and_three_observation_integrity(self) -> None:
        candidate_id = "capture-scope-churn"
        experiment_id = "forward-capture-scope-churn"
        intent_id = "observation-intent-capture-scope-churn"
        bound_market = "3161879"
        all_market_ids = (
            bound_market,
            "3161881",
            "559688",
            "559691",
            "559692",
        )
        allowed_market_ids = all_market_ids[:3]
        strategy_document = {
            "version": 1,
            "market_type": "prediction",
            "family": "momentum",
            "parameters": {"lookback": 1, "threshold": 0.05},
            "probability_model": "market-history",
            "resolution_aware": True,
            "resolution_inputs": ["market_history"],
        }
        policy = normalize_market_scope(
            {
                **normalize_market_scope(
                    target_instrument="POLYMARKET",
                ).as_dict(),
                "provenance": "canonical",
            }
        )

        def market(market_id: str) -> dict[str, Any]:
            return {
                "market_id": market_id,
                "condition_id": "condition-" + market_id,
                "yes_token_id": "yes-" + market_id,
                "no_token_id": "no-" + market_id,
                "metadata_provenance": {"source_type": "CURRENT"},
                "source_type": "CURRENT",
                "provider": "polymarket",
                "venue": "POLYMARKET",
                "instrument": "POLYMARKET",
                "active": True,
                "closed": False,
                "settlement": "OPEN",
                "enable_order_book": True,
                "accepting_orders": True,
            }

        config = {
            "candidate_id": candidate_id,
            "observation_intent_id": intent_id,
            "paper_observation_intent_id": intent_id,
            "observation_capture_only": True,
            "observation_intent": True,
            "observation_only_lineage": True,
            "paper_only": True,
            "market_authority_required": True,
            "capture_market_id": bound_market,
            "market_scope": policy.as_dict(),
            "scope_resolution_freshness_sla_seconds": 600.0,
            "strategy_version_id": "capture-scope-churn-version",
            "research_trial_id": "capture-scope-churn-trial",
            "strategy_document": strategy_document,
        }
        lifecycle_payload = {
            "candidate_id": candidate_id,
            "forward_test_id": experiment_id,
            "paper_observation_intent_id": intent_id,
            "paper_observation_intent": True,
            "paper_only": True,
            "research_only": True,
            "selection_excluded": True,
            "allocation_active": False,
            "canary_armed": False,
            "observation_only_lineage": True,
            "execution_scope": "OBSERVATION",
            "scope_hash": policy.scope_hash,
            "scope_version": policy.scope_version,
        }
        spec = SimpleNamespace(
            experiment_id=experiment_id,
            config=config,
            allowed_markets=allowed_market_ids,
            registration_timestamp=T0,
            strategy_hash="sha256:capture-scope-churn-strategy",
            model_hash="sha256:capture-scope-churn-model",
        )

        with AxiomStore(":memory:") as store:
            store.load_candidate_lifecycle = (  # type: ignore[method-assign]
                lambda _candidate: {
                    "stage": CandidateStage.PAPER_FORWARD.value,
                    "payload": lifecycle_payload,
                }
            )
            node = ResearchNode.__new__(ResearchNode)
            node.store = store
            node.config = SimpleNamespace(shadow_interval=60)

            def proof_for(
                matched_ids: tuple[str, ...],
                resolved_at: datetime,
            ) -> Any:
                proof = resolve_market_scope(
                    candidate_id,
                    {"market_scope": policy.as_dict()},
                    [market(market_id) for market_id in matched_ids],
                    resolved_at=resolved_at,
                )
                store.save_market_scope_resolution(proof)
                return proof

            def capture(
                proof: Any,
                observed_at: datetime,
                source_id: str,
            ) -> dict[str, Any]:
                return node._capture_legacy_observations(
                    spec,
                    [
                        {
                            "market_id": bound_market,
                            "timestamp": observed_at,
                            "source_timestamp": observed_at,
                            "source_snapshot_id": source_id,
                        }
                    ],
                    store=store,
                    now=observed_at + timedelta(seconds=1),
                )

            first_proof = proof_for(all_market_ids, T0)
            first = capture(first_proof, T0 + timedelta(minutes=1), "capture-1")
            self.assertEqual(first["status"], "OBSERVING")
            self.assertEqual(first["observations_processed"], 1)

            second_proof = proof_for(
                (bound_market, "559688", "559691", "559692"),
                T0 + timedelta(minutes=2),
            )
            second = capture(
                second_proof,
                T0 + timedelta(minutes=3),
                "capture-2",
            )
            self.assertEqual(second["status"], "OBSERVING", second)
            self.assertEqual(second["observations_processed"], 2)

            third_proof = proof_for(
                (bound_market, "3161881", "559692"),
                T0 + timedelta(minutes=4),
            )
            third = capture(
                third_proof,
                T0 + timedelta(minutes=5),
                "capture-3",
            )
            self.assertEqual(third["status"], "DECLINED")
            self.assertEqual(third["observations_processed"], 3)
            records = store.list_paper_observations(experiment_id, limit=None)
            self.assertEqual(len(records), 3)
            self.assertEqual(
                {record["market_id"] for record in records},
                {bound_market},
            )

            removed_proof = proof_for(
                ("3161881", "559688", "559691", "559692"),
                T0 + timedelta(minutes=6),
            )
            removed = capture(
                removed_proof,
                T0 + timedelta(minutes=7),
                "capture-4",
            )
            self.assertEqual(
                removed["blocker"],
                "CURRENT_SCOPE_PROOF_MARKETS_MISMATCH",
            )
            self.assertEqual(
                len(store.list_paper_observations(experiment_id, limit=None)),
                3,
            )

            excluded_proof = proof_for(all_market_ids, T0 + timedelta(minutes=8)).as_dict()
            excluded_proof["excluded_markets"] = [{"market_id": bound_market}]
            original_loader = store.load_market_scope_resolution
            store.load_market_scope_resolution = (  # type: ignore[method-assign]
                lambda *_args, **_kwargs: excluded_proof
            )
            excluded = capture(
                excluded_proof,
                T0 + timedelta(minutes=9),
                "capture-5",
            )
            self.assertEqual(
                excluded["blocker"],
                "CURRENT_SCOPE_PROOF_INCOMPLETE",
            )

            deferred_proof = proof_for(all_market_ids, T0 + timedelta(minutes=10)).as_dict()
            deferred_proof["deferred_markets"] = [{"market_id": bound_market}]
            store.load_market_scope_resolution = (  # type: ignore[method-assign]
                lambda *_args, **_kwargs: deferred_proof
            )
            deferred = capture(
                deferred_proof,
                T0 + timedelta(minutes=11),
                "capture-6",
            )
            self.assertEqual(
                deferred["blocker"],
                "CURRENT_SCOPE_PROOF_INCOMPLETE",
            )
            store.load_market_scope_resolution = original_loader

            state = store.load_paper_state(experiment_id)
            self.assertIsNotNone(state)
            assert state is not None
            evaluations = state["state"]["canonical_capture_evaluations"]
            self.assertEqual(len(evaluations), 3)
            self.assertEqual(store.count_paper_fills(experiment_id), 0)
            self.assertEqual(
                store.list_paper_execution_events(experiment_id, limit=None),
                [],
            )

            restarted = ResearchNode.__new__(ResearchNode)
            restarted.store = store
            restarted.config = SimpleNamespace(shadow_interval=60)
            repeated = restarted._capture_legacy_observations(
                spec,
                [
                    {
                        "market_id": bound_market,
                        "timestamp": T0 + timedelta(minutes=5),
                        "source_timestamp": T0 + timedelta(minutes=5),
                        "source_snapshot_id": "capture-3",
                    }
                ],
                store=store,
                now=T0 + timedelta(minutes=12),
            )
            self.assertEqual(repeated["status"], "DECLINED")
            self.assertEqual(repeated["observations_processed"], 3)
            restarted_state = store.load_paper_state(experiment_id)
            self.assertIsNotNone(restarted_state)
            assert restarted_state is not None
            self.assertEqual(
                len(restarted_state["state"]["canonical_capture_evaluations"]),
                3,
            )
            self.assertEqual(
                len(store.list_paper_observations(experiment_id, limit=None)),
                3,
            )

    def test_capture_successor_rejects_authority_and_preserves_predecessor_hash(self) -> None:
        class WorkerStore:
            def __init__(self) -> None:
                self.states: list[dict[str, Any]] = []

            def save_worker_state(self, *args: Any, **kwargs: Any) -> None:
                self.states.append({"args": args, "kwargs": kwargs})

        worker_store = WorkerStore()
        worker_node = ResearchNode.__new__(ResearchNode)
        worker_node.store = worker_store
        worker_node._paper_store = None
        worker_node.clock = lambda: T0
        for config, blocker in (
            (
                {
                    "observation_capture_only": True,
                    "canonical_operational_setup_required": True,
                },
                "CAPTURE_SAFETY_INVALID",
            ),
            (
                {
                    "observation_capture_only": True,
                    "canonical_operational_setup_required": "true",
                },
                "CANONICAL_OPERATIONAL_SETUP_MARKER_INVALID",
            ),
            ({"operational_setup_hash": "sha256:legacy"}, "MIGRATION_REQUIRED"),
        ):
            result = worker_node._run_single_paper_worker(
                SimpleNamespace(
                    experiment_id="capture-authority-" + blocker,
                    config=config,
                    allowed_markets=("capture-market",),
                )
            )
            self.assertEqual(result["status"], "BLOCKED")
            self.assertEqual(result["blocker"], blocker)

        strategy_document = {
            "version": 1,
            "market_type": "prediction",
            "family": "momentum",
            "parameters": {"lookback": 1, "threshold": 0.05},
            "probability_model": "market-history",
            "resolution_aware": True,
            "resolution_inputs": ["market_history"],
        }
        policy = normalize_market_scope(
            market_ids=["capture-market"],
            target_instrument="POLYMARKET",
        )
        current_market = {
            "market_id": "capture-market",
            "condition_id": "condition-capture-market",
            "yes_token_id": "yes-capture-market",
            "no_token_id": "no-capture-market",
            "metadata_provenance": {"source_type": "CURRENT"},
            "source_type": "CURRENT",
            "provider": "polymarket",
            "venue": "POLYMARKET",
            "instrument": "POLYMARKET",
            "active": True,
            "closed": False,
            "settlement": "OPEN",
            "enable_order_book": True,
            "accepting_orders": True,
        }
        with AxiomStore(":memory:") as store:
            proof = resolve_market_scope(
                "capture-authority",
                {"market_scope": policy.as_dict()},
                [current_market],
                resolved_at=T0,
            )
            processor = AutonomousResearchProcessor(
                store,
                config=AutonomousResearchConfig(
                    scope_resolution_freshness_sla_seconds=60
                ),
                clock=lambda: T0,
            )
            processor._ensure_rolling_paper_observation(
                {
                    "candidate_id": "capture-authority",
                    "strategy_version_id": "capture-version",
                    "research_trial_id": "capture-trial",
                    "strategy_hash": _content_hash(
                        _normalized_strategy_document(strategy_document)
                    ),
                    "strategy_document": strategy_document,
                    "model_document": {"model_required": False},
                    "market_scope": policy.as_dict(),
                    "scope_resolution": proof.as_dict(),
                    "observation_capture_only": True,
                    "operational_setup_hash": "sha256:legacy",
                },
                T0,
                market_ids=("capture-market",),
            )
            successors = [
                item
                for item in ForwardTestRegistry(store).list()
                if item.allowed_markets
            ]
            self.assertEqual(len(successors), 1)
            successor_config = successors[0].config
            self.assertNotIn("operational_setup", successor_config)
            self.assertNotIn("operational_setup_hash", successor_config)
            self.assertEqual(
                successor_config["predecessor_operational_setup_hash"],
                "sha256:legacy",
            )
    def test_rolling_worker_migrates_before_future_review_wait(self) -> None:
        class StopAfterOneWait(threading.Event):
            def wait(self, timeout: float | None = None) -> bool:
                self.set()
                return True

        candidate_id = "rolling-worker-migration"
        market_id = "rolling-worker-market"
        strategy_document = {
            "version": 1,
            "market_type": "prediction",
            "family": "probability_mispricing",
            "parameters": {"threshold": 0.05},
            "probability_model": "fixed",
            "resolution_aware": True,
            "resolution_inputs": ["expiry", "settlement"],
        }
        policy = normalize_market_scope(
            {
                **normalize_market_scope(
                    target_instrument="POLYMARKET",
                ).as_dict(),
                "provenance": "canonical",
            }
        )
        current_market = {
            "market_id": market_id,
            "condition_id": "condition-" + market_id,
            "yes_token_id": "yes-" + market_id,
            "no_token_id": "no-" + market_id,
            "metadata_provenance": {"source_type": "CURRENT"},
            "source_type": "CURRENT",
            "provider": "polymarket",
            "venue": "POLYMARKET",
            "instrument": "POLYMARKET",
            "active": True,
            "closed": False,
            "settlement": "OPEN",
            "enable_order_book": True,
            "accepting_orders": True,
        }
        replacement_market_id = "rolling-worker-replacement"
        replacement_market = {
            **current_market,
            "market_id": replacement_market_id,
            "condition_id": "condition-" + replacement_market_id,
            "yes_token_id": "yes-" + replacement_market_id,
            "no_token_id": "no-" + replacement_market_id,
        }
        replacement_market_ids = (
            replacement_market_id,
            "rolling-worker-replacement-2",
            "rolling-worker-replacement-3",
            "rolling-worker-replacement-4",
            "rolling-worker-replacement-5",
            "rolling-worker-replacement-6",
            "rolling-worker-replacement-7",
            "rolling-worker-replacement-8",
            "rolling-worker-replacement-9",
        )
        replacement_markets = [
            {
                **replacement_market,
                "market_id": market,
                "condition_id": "condition-" + market,
                "yes_token_id": "yes-" + market,
                "no_token_id": "no-" + market,
            }
            for market in replacement_market_ids
        ]
        source = {
            "candidate_id": candidate_id,
            "strategy_version_id": "rolling-worker-version",
            "research_trial_id": "rolling-worker-trial",
            "strategy_hash": _content_hash(
                _normalized_strategy_document(strategy_document)
            ),
            "strategy_document": strategy_document,
            "model_document": {"probability": 0.5},
            "market_scope": policy.as_dict(),
            "dataset_id": "rolling-worker-history",
            "dataset_version": "v1",
            "dataset_attestation": {
                "dataset_id": "rolling-worker-history",
                "dataset_version": "v1",
                "integrity": "sha256:rolling-worker-history",
            },
            "dataset_boundary": {
                "schema_version": "1",
                "dataset_id": "rolling-worker-history",
                "dataset_version": "v1",
                "ordered_row_manifest_digest": "sha256:rolling-worker-boundary",
                "exact_cutoff": "2025-01-01T00:00:00+00:00",
                "row_count": 3,
            },
            "plan_id": "rolling-worker-plan",
            "plan_hash": _rolling_hash({"plan_id": "rolling-worker-plan"}),
        }
        with tempfile.TemporaryDirectory() as directory:
            db = str(Path(directory) / "rolling-worker-migration.sqlite")
            with AxiomStore(db) as store:
                proof = resolve_market_scope(
                    candidate_id,
                    {"market_scope": policy.as_dict()},
                    [current_market],
                    resolved_at=T0 - timedelta(seconds=120),
                )
                store.save_market_scope_resolution(proof)
                registry = ForwardTestRegistry(store)
                intent = registry.register_observation_intent(
                    strategy=strategy_document,
                    model={"probability": 0.5},
                    config={
                        **source,
                        "observation_intent": True,
                        "observation_only_lineage": True,
                        "market_authority_required": False,
                        "scope_resolution": proof.as_dict(),
                        "rolling_research": True,
                        "paper_only": True,
                        "observation_capture_only": True,
                        "capture_market_id": market_id,
                        "current_market_ids": [market_id],
                    },
                    registration_timestamp=T0,
                    candidate_id=candidate_id,
                    strategy_version_id=source["strategy_version_id"],
                    research_trial_id=source["research_trial_id"],
                    source_strategy_hash=source["strategy_hash"],
                    rolling_strategy_hash=source["strategy_hash"],
                    dataset_selector={
                        "dataset_id": source["dataset_id"],
                        "dataset_version": source["dataset_version"],
                    },
                    scope=policy.as_dict(),
                    scope_resolution=proof.as_dict(),
                )
                old_intent_id = intent.experiment_id
                store.save_candidate_lifecycle(
                    candidate_id,
                    CandidateStage.IDEA.value,
                    {"candidate_id": candidate_id},
                    timestamp=T0,
                )
                lifecycle_manager = CandidateLifecycleManager(store)
                lifecycle_manager.advance(
                    candidate_id,
                    CandidateStage.SCHEMA_VALIDATED.value,
                    {"candidate_id": candidate_id, "schema_valid": True},
                    reason="schema validated",
                )
                lifecycle_manager.advance(
                    candidate_id,
                    CandidateStage.PAPER_FORWARD.value,
                    {
                        "candidate_id": candidate_id,
                        "paper_observation_intent": True,
                        "paper_observation_intent_id": old_intent_id,
                        "forward_test_id": old_intent_id,
                        "paper_only": True,
                        "research_only": True,
                        "execution_scope": "OBSERVATION",
                        "observation_only_lineage": True,
                        "selection_excluded": True,
                        "allocation_active": False,
                        "canary_armed": False,
                        "paper_forward_started": True,
                        "holdout_used": False,
                        "allowed_markets": [market_id],
                        "current_market_ids": [market_id],
                        "resolved_market_ids": [market_id],
                        "scope_resolution": proof.as_dict(),
                        "market_scope_resolution": proof.as_dict(),
                        "operational_setup_hash": "sha256:legacy",
                        "market_scope": policy.as_dict(),
                        "market_scope_hash": policy.scope_hash,
                        "market_scope_version": policy.scope_version,
                        "plan_hash": source["plan_hash"],
                        "dataset_selector": {
                            "dataset_id": source["dataset_id"],
                            "dataset_version": source["dataset_version"],
                        },
                        "dataset_attestation": source["dataset_attestation"],
                    },
                    reason="legacy observation handoff",
                    observation_only=True,
                )
                processor = AutonomousResearchProcessor(
                    store,
                    config=AutonomousResearchConfig(
                        scope_resolution_freshness_sla_seconds=60,
                        observation_setup_migration_freshness_sla_seconds=300,
                    ),
                    clock=lambda: T0 + timedelta(milliseconds=30),
                )
                migration_calls: list[datetime] = []
                migrate = processor._migrate_observation_setup_intents

                def migrate_once(now: datetime) -> tuple[Mapping[str, Any], ...]:
                    migration_calls.append(now)
                    return migrate(now)

                processor._migrate_observation_setup_intents = migrate_once
                refresh_calls: list[bool] = []

                def refresh_wait(
                    *,
                    now: datetime,
                    skip_observation_setup_migration: bool = False,
                ) -> Mapping[str, Any]:
                    refresh_calls.append(skip_observation_setup_migration)
                    return {"decision": "WAIT_FOR_ROLLING_REVIEW"}

                processor.refresh_rolling_evidence = refresh_wait
                node = ResearchNode(
                    NodeConfig(
                        db,
                        crypto_enabled=False,
                        rolling_review_interval_seconds=86400,
                    ),
                    provider=InMemoryPredictionProvider([]),
                    store=store,
                    clock=lambda: T0,
                )
                node.research_processor = processor
                node.stop_event = StopAfterOneWait()
                store.load_portfolio_review_state = lambda: {
                    "review_due_at": (T0 + timedelta(days=1)).isoformat(),
                    "refresh_identity": {},
                }
                processor.active_portfolio_selection = lambda: {
                    "portfolio_selection_id": "future-selection"
                }
                node._rolling_portfolio_worker_loop()
                self.assertEqual(len(migration_calls), 1)
                self.assertEqual(refresh_calls, [True])
                migrated = [
                    item
                    for item in registry.list_observation_intents()
                    if item.experiment_id != old_intent_id
                    and isinstance(item.config, Mapping)
                    and isinstance(item.config.get("observation_handoff"), Mapping)
                ]
                self.assertEqual(len(migrated), 1)
                self.assertNotEqual(migrated[0].experiment_id, old_intent_id)
                self.assertEqual(
                    migrated[0].config["observation_handoff"][
                        "predecessor_observation_intent_id"
                    ],
                    old_intent_id,
                )
                self.assertTrue(
                    migrated[0].config.get("observation_capture_only") is True
                )
                proof_b = resolve_market_scope(
                    candidate_id,
                    {"market_scope": policy.as_dict()},
                    replacement_markets,
                    resolved_at=T0 + timedelta(milliseconds=23),
                ).as_dict()
                proof_b["excluded_markets"] = [
                    {
                        "market_id": "rolling-worker-expired",
                        "reason": "MARKET_EXPIRED",
                        "detail": "",
                        "metadata": {
                            "action": "SUITABLE",
                            "category": "SUITABLE_MARKET",
                            "intended_token": "yes",
                            "token_id": "rolling-worker-private-token",
                            "resolver": "inspect_market_depth",
                            "observed_at": (
                                T0 + timedelta(milliseconds=23)
                            ).isoformat(),
                        },
                    }
                ]
                original_loader = store.load_market_scope_resolution
                proof_persisted_during_load = [False]

                def load_current_proof(*args: Any, **kwargs: Any) -> Any:
                    result = original_loader(*args, **kwargs)
                    if not proof_persisted_during_load[0]:
                        store.save_market_scope_resolution(proof_b)
                        proof_persisted_during_load[0] = True
                    return result

                store.load_market_scope_resolution = load_current_proof
                processor._migrate_observation_setup_intents(T0)
                paper_calls: list[str] = []

                def paper_workers() -> Mapping[str, Any]:
                    lifecycle = store.load_candidate_lifecycle(candidate_id)
                    linked_forward = (
                        lifecycle["payload"]["forward_test_id"]
                        if isinstance(lifecycle, Mapping)
                        and isinstance(lifecycle.get("payload"), Mapping)
                        else ""
                    )
                    current_forwards = [
                        item
                        for item in registry.list()
                        if item.config.get("observation_intent") is True
                        and item.config.get("market_authority_required") is True
                        and tuple(item.allowed_markets) == tuple(sorted(replacement_market_ids))
                    ]
                    self.assertEqual(len(current_forwards), 1)
                    self.assertEqual(linked_forward, current_forwards[0].experiment_id)
                    self.assertIn(
                        current_forwards[0].config.get("capture_market_id"),
                        set(current_forwards[0].allowed_markets),
                    )
                    paper_calls.append(linked_forward)
                    return {
                        "processed_candidates": 1,
                        "successful_candidates": 1,
                        "blocked_candidates": 0,
                    }

                node._run_crypto_paper = lambda: None
                node._run_opportunity_pipeline = lambda: True
                node._run_paper_workers = paper_workers
                node._run_research_queue = lambda: {}
                processor.reevaluate_forward_candidates = lambda now: None
                research_result = node._run_research_cycle()
                self.assertGreaterEqual(
                    research_result["observation_setup_migrations"],
                    1,
                )
                self.assertEqual(len(paper_calls), 1)
                intent_ids_after_handoff = tuple(
                    sorted(item.experiment_id for item in registry.list())
                )
                restarted = ResearchNode(
                    NodeConfig(
                        db,
                        crypto_enabled=False,
                        rolling_review_interval_seconds=86400,
                    ),
                    provider=InMemoryPredictionProvider([]),
                    store=store,
                    clock=lambda: T0,
                )
                restarted._run_crypto_paper = lambda: None
                restarted._run_opportunity_pipeline = lambda: True
                restarted._run_paper_workers = lambda: {
                    "processed_candidates": 0,
                    "successful_candidates": 0,
                    "blocked_candidates": 0,
                }
                restarted._run_research_queue = lambda: {}
                restarted.research_processor.reevaluate_forward_candidates = (
                    lambda now: None
                )
                restart_result = restarted._run_research_cycle()
                self.assertEqual(
                    restart_result["observation_setup_migrations"],
                    0,
                )
                def fail_migration(now: datetime) -> tuple[Mapping[str, Any], ...]:
                    raise RuntimeError("migration unavailable")

                restarted.research_processor._migrate_observation_setup_intents = (
                    fail_migration
                )
                failed_result = restarted._run_research_cycle()
                self.assertTrue(failed_result["degraded"])
                self.assertEqual(
                    failed_result["observation_setup_migration_error"],
                    "migration unavailable",
                )
                self.assertIn(
                    "observation setup migration completed with degraded output",
                    failed_result["errors"],
                )
                self.assertEqual(
                    tuple(sorted(item.experiment_id for item in registry.list())),
                    intent_ids_after_handoff,
                )
                self.assertTrue(proof_persisted_during_load[0])
                capture_successors = [
                    item
                    for item in registry.list_observation_intents()
                    if isinstance(item.config, Mapping)
                    and item.config.get("observation_capture_only") is True
                    and isinstance(item.config.get("observation_handoff"), Mapping)
                ]
                self.assertEqual(len(capture_successors), 2)
                replacement_successor = next(
                    item
                    for item in capture_successors
                    if item.config.get("capture_market_id") == replacement_market_id
                )
                self.assertIn(
                    migrated[0].experiment_id,
                    replacement_successor.config.get(
                        "superseded_observation_intent_ids", ()
                    ),
                )
                self.assertEqual(
                    tuple(replacement_successor.config.get("current_market_ids", ())),
                    tuple(sorted(replacement_market_ids)),
                )
                lifecycle_after_replacement = store.load_candidate_lifecycle(candidate_id)
                self.assertIsNotNone(lifecycle_after_replacement)
                assert lifecycle_after_replacement is not None
                self.assertEqual(
                    lifecycle_after_replacement["payload"]["paper_observation_intent_id"],
                    replacement_successor.experiment_id,
                )
                self.assertEqual(
                    lifecycle_after_replacement["payload"]["forward_test_id"],
                    next(
                        item.experiment_id
                        for item in registry.list()
                        if item.config.get("observation_intent") is True
                        and item.config.get("market_authority_required") is True
                        and tuple(item.allowed_markets) == tuple(sorted(replacement_market_ids))
                    ),
                )
                replacement_forward = next(
                    item
                    for item in registry.list()
                    if item.config.get("observation_intent") is True
                    and item.config.get("market_authority_required") is True
                    and tuple(item.allowed_markets) == tuple(sorted(replacement_market_ids))
                )
                persisted_proof = store.load_market_scope_resolution(candidate_id)
                self.assertIsNotNone(persisted_proof)
                assert persisted_proof is not None
                persisted_proof_mapping = persisted_proof.as_dict()
                self.assertEqual(persisted_proof_mapping, proof_b)
                persisted_excluded = persisted_proof_mapping["excluded_markets"][0]
                self.assertEqual(
                    persisted_excluded["metadata"]["intended_token"],
                    "yes",
                )
                self.assertEqual(
                    persisted_excluded["metadata"]["token_id"],
                    "rolling-worker-private-token",
                )
                public_proof = replacement_forward.config["scope_resolution"]
                self.assertEqual(public_proof["resolved_at"], proof_b["resolved_at"])
                self.assertEqual(
                    tuple(item["market_id"] for item in public_proof["matched_markets"]),
                    tuple(sorted(replacement_market_ids)),
                )
                public_excluded = public_proof["excluded_markets"][0]
                self.assertNotIn("intended_token", public_excluded["metadata"])
                self.assertNotIn("token_id", public_excluded["metadata"])
                stale_forward_config = dict(replacement_forward.config)
                stale_forward_config["observation_intent_id"] = migrated[0].experiment_id
                stale_forward_config["paper_observation_intent_id"] = migrated[0].experiment_id
                stale_forward_config["capture_market_id"] = market_id
                stale_forward_config["current_market_ids"] = list(
                    sorted(replacement_market_ids)
                )
                registry.freeze(
                    strategy=stale_forward_config["strategy_document"],
                    model=stale_forward_config["model_document"],
                    config=stale_forward_config,
                    start_timestamp=T0,
                    allowed_markets=tuple(sorted(replacement_market_ids)),
                    experiment_id="forward-aaa-stale-capture",
                )
                processor._migrate_observation_setup_intents(T0)
                capture_successors_after_repeat = [
                    item
                    for item in registry.list_observation_intents()
                    if isinstance(item.config, Mapping)
                    and item.config.get("observation_capture_only") is True
                    and isinstance(item.config.get("observation_handoff"), Mapping)
                ]
                lifecycle_after_coexistence = store.load_candidate_lifecycle(candidate_id)
                self.assertIsNotNone(lifecycle_after_coexistence)
                assert lifecycle_after_coexistence is not None
                self.assertEqual(
                    lifecycle_after_coexistence["payload"]["paper_observation_intent_id"],
                    replacement_successor.experiment_id,
                )
                self.assertEqual(
                    lifecycle_after_coexistence["payload"]["forward_test_id"],
                    replacement_forward.experiment_id,
                )
                self.assertEqual(len(capture_successors_after_repeat), 2)
                replacement_node = ResearchNode.__new__(ResearchNode)
                replacement_node.store = store
                replacement_node.config = SimpleNamespace(shadow_interval=60)
                replacement_capture = replacement_node._capture_legacy_observations(
                    replacement_forward,
                    [
                        {
                            "market_id": replacement_market_id,
                            "timestamp": T0,
                            "source_timestamp": T0,
                            "source_snapshot_id": "replacement-capture",
                        }
                    ],
                    store=store,
                    now=T0 + timedelta(milliseconds=30),
                )
                self.assertEqual(replacement_capture.get("status"), "OBSERVING")
                worker = store.get_worker_state("rolling-portfolio")
                self.assertIsNotNone(worker)
                assert worker is not None
                self.assertEqual(
                    worker["payload"]["next_decision"],
                    "WAIT_FOR_ROLLING_REVIEW",
                )
                successor = next(
                    item
                    for item in registry.list()
                    if tuple(item.allowed_markets) == (market_id,)
                    and item.config.get("observation_capture_only") is True
                )
                capture_node = ResearchNode.__new__(ResearchNode)
                capture_node.store = store
                capture_node.config = SimpleNamespace(shadow_interval=60)
                lifecycle_payload = {
                    "candidate_id": candidate_id,
                    "forward_test_id": successor.experiment_id,
                    "paper_observation_intent_id": (
                        successor.config.get("observation_intent_id")
                        or successor.config.get("paper_observation_intent_id")
                    ),
                    "paper_observation_intent": True,
                    "paper_only": True,
                    "research_only": True,
                    "execution_scope": "OBSERVATION",
                    "observation_only_lineage": True,
                    "selection_excluded": True,
                    "allocation_active": False,
                    "canary_armed": False,
                    "scope_hash": policy.scope_hash,
                    "scope_version": policy.scope_version,
                }
                capture_node.store.load_candidate_lifecycle = (
                    lambda _candidate: {
                        "stage": CandidateStage.PAPER_FORWARD.value,
                        "payload": lifecycle_payload,
                    }
                )
                capture_node.store.load_market_scope_resolution = (
                    lambda *_args, **_kwargs: proof.as_dict()
                )
                stale_capture = capture_node._capture_legacy_observations(
                    successor,
                    [
                        {
                            "market_id": market_id,
                            "timestamp": T0,
                            "source_timestamp": T0,
                            "source_snapshot_id": "stale-proof",
                        }
                    ],
                    store=store,
                    now=T0,
                )
                self.assertEqual(stale_capture["status"], "BLOCKED")
                self.assertEqual(stale_capture["blocker"], "CURRENT_SCOPE_PROOF_STALE")
                fresh_proof = resolve_market_scope(
                    candidate_id,
                    {"market_scope": policy.as_dict()},
                    [current_market],
                    resolved_at=T0,
                )
                capture_node.store.load_market_scope_resolution = (
                    lambda *_args, **_kwargs: fresh_proof.as_dict()
                )
                fresh_capture = capture_node._capture_legacy_observations(
                    successor,
                    [
                        {
                            "market_id": market_id,
                            "timestamp": T0,
                            "source_timestamp": T0,
                            "source_snapshot_id": "fresh-proof",
                        }
                    ],
                    store=store,
                    now=T0,
                )
                self.assertEqual(fresh_capture["status"], "OBSERVING")



    def test_rolling_hash_only_successor_migrates_immutable_rows(self) -> None:
        candidate_id = "rolling-hash-only-successor"
        market_id = "rolling-hash-only-market"
        strategy_document = {
            "version": 1,
            "market_type": "prediction",
            "family": "momentum",
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
            "probability_model": "market-history",
            "resolution_aware": True,
            "resolution_inputs": ["market_history"],
        }
        policy = normalize_market_scope(
            {
                **normalize_market_scope(
                    market_ids=[market_id],
                    target_instrument="POLYMARKET",
                ).as_dict(),
                "provenance": "canonical",
            }
        )
        source = {
            "candidate_id": candidate_id,
            "strategy_version_id": "rolling-hash-only-version",
            "research_trial_id": "rolling-hash-only-trial",
            "strategy_hash": _content_hash(_normalized_strategy_document(strategy_document)),
            "strategy_document": strategy_document,
            "model_document": {"model_required": False},
            "market_scope": policy.as_dict(),
            "plan_id": "rolling-hash-only-plan",
            "plan_hash": _rolling_hash({"plan_id": "rolling-hash-only-plan", "version": 1}),
            "dataset_id": "rolling-hash-only-history",
            "dataset_version": "v1",
            "dataset_attestation": {
                "dataset_id": "rolling-hash-only-history",
                "dataset_version": "v1",
                "attestation_hash": "sha256:rolling-hash-only-attestation",
                "integrity": "sha256:rolling-hash-only-history",
            },
            "dataset_boundary": {
                "schema_version": "1",
                "dataset_id": "rolling-hash-only-history",
                "dataset_version": "v1",
                "ordered_row_manifest_digest": "sha256:rolling-hash-only-boundary",
                "exact_cutoff": "2025-01-01T00:00:00+00:00",
                "row_count": 3,
            },
        }
        current_market = {
            "market_id": market_id,
            "condition_id": "condition-" + market_id,
            "yes_token_id": "yes-" + market_id,
            "no_token_id": "no-" + market_id,
            "metadata_provenance": {"source_type": "CURRENT"},
            "source_type": "CURRENT",
            "provider": "polymarket",
            "venue": "POLYMARKET",
            "instrument": "POLYMARKET",
            "active": True,
            "closed": False,
            "settlement": "OPEN",
            "enable_order_book": True,
            "accepting_orders": True,
        }
        with tempfile.TemporaryDirectory() as directory:
            db = str(Path(directory) / "rolling-hash-only-migration.sqlite")
            with AxiomStore(db) as store:
                proof = resolve_market_scope(
                    candidate_id,
                    {"market_scope": policy.as_dict()},
                    [current_market],
                    resolved_at=T0,
                )
                store.save_market_scope_resolution(proof)
                selector = {
                    "dataset_id": source["dataset_id"],
                    "dataset_version": source["dataset_version"],
                }
                probe = ForwardTestRegistry()
                canonical_intent = probe.register_observation_intent(
                    strategy=strategy_document,
                    model={"model_required": False},
                    config={
                        "observation_intent": True,
                        "observation_only_lineage": True,
                        "market_authority_required": False,
                        "candidate_id": candidate_id,
                        "strategy_version_id": source["strategy_version_id"],
                        "research_trial_id": source["research_trial_id"],
                        "strategy_document": strategy_document,
                        "model_document": {"model_required": False},
                        "dataset_selector": selector,
                        "dataset_version": source["dataset_version"],
                        "dataset_attestation": source["dataset_attestation"],
                        "plan_id": source["plan_id"],
                        "plan_hash": source["plan_hash"],
                        "market_scope": policy.as_dict(),
                        "rolling_research": True,
                        "paper_only": True,
                    },
                    registration_timestamp=T0,
                    candidate_id=candidate_id,
                    strategy_version_id=source["strategy_version_id"],
                    research_trial_id=source["research_trial_id"],
                    source_strategy_hash=source["strategy_hash"],
                    rolling_strategy_hash=source["strategy_hash"],
                    dataset_selector=selector,
                    scope=policy.as_dict(),
                    scope_resolution=proof.as_dict(),
                )
                setup = canonical_intent.config["operational_setup"]
                assert isinstance(setup, Mapping)
                old_intent_id = "observation-intent-" + candidate_id + "-legacy"
                old_intent_config = dict(canonical_intent.config)
                old_intent_config.pop("operational_setup", None)
                old_intent_config["dataset_boundary"] = source["dataset_boundary"]
                old_intent_config["operational_setup_hash"] = _operational_setup_hash(setup)
                old_intent_record = canonical_intent.as_record()
                old_intent_record["experiment_id"] = old_intent_id
                old_intent_record["config"] = old_intent_config
                old_intent_record["allowed_markets"] = []
                store.save_forward_test(old_intent_id, old_intent_record)
                old_forward_id = "forward-" + candidate_id
                old_forward_config = dict(old_intent_config)
                old_forward_config["market_authority_required"] = True
                old_forward_record = canonical_intent.as_record()
                old_forward_record["experiment_id"] = old_forward_id
                old_forward_record["config"] = old_forward_config
                old_forward_record["allowed_markets"] = [market_id]
                store.save_forward_test(old_forward_id, old_forward_record)
                store.save_candidate_lifecycle(
                    candidate_id,
                    CandidateStage.IDEA.value,
                    {"candidate_id": candidate_id},
                    timestamp=T0,
                )
                store.save_candidate_lifecycle(
                    candidate_id,
                    CandidateStage.PAPER_FORWARD.value,
                    {
                        "candidate_id": candidate_id,
                        "paper_observation_intent": True,
                        "paper_observation_intent_id": old_intent_id,
                        "forward_test_id": old_forward_id,
                        "paper_only": True,
                        "research_only": True,
                        "execution_scope": "OBSERVATION",
                        "observation_only_lineage": True,
                        "selection_excluded": True,
                        "allocation_active": False,
                        "canary_armed": False,
                        "allowed_markets": [market_id],
                        "current_market_ids": [market_id],
                        "resolved_market_ids": [market_id],
                        "scope_resolution": proof.as_dict(),
                        "market_scope_resolution": proof.as_dict(),
                        "paper_forward_started": True,
                        "holdout_used": False,
                        "operational_setup_hash": _operational_setup_hash(setup),
                    },
                    from_stage=CandidateStage.IDEA.value,
                    timestamp=T0,
                )
                processor = AutonomousResearchProcessor(store, clock=lambda: T0)
                expected_setup = _operational_setup_for_strategy(strategy_document, source)
                self.assertIsNotNone(expected_setup)
                assert expected_setup is not None
                with self.assertRaisesRegex(ValueError, "OPERATIONAL_SETUP_HASH_MISMATCH"):
                    processor._ensure_rolling_paper_observation(
                        {
                            **source,
                            "operational_setup": dict(expected_setup),
                            "operational_setup_hash": "sha256:" + ("0" * 64),
                        },
                        T0,
                        market_ids=(market_id,),
                    )
                unsupported_strategy = {
                    **strategy_document,
                    "family": "probability_mispricing",
                }
                with self.assertRaisesRegex(ValueError, "OPERATIONAL_SETUP_UNSUPPORTED"):
                    processor._ensure_rolling_paper_observation(
                        {
                            **source,
                            "strategy_document": unsupported_strategy,
                            "strategy_hash": _content_hash(
                                _normalized_strategy_document(unsupported_strategy)
                            ),
                            "operational_setup": dict(setup),
                            "operational_setup_hash": _operational_setup_hash(setup),
                        },
                        T0,
                        market_ids=(market_id,),
                    )
                malformed_directional_documents = (
                    {
                        **strategy_document,
                        "parameters": {
                            **strategy_document["parameters"],
                            "threshold": "0.05",
                        },
                    },
                    {
                        **strategy_document,
                        "parameters": {
                            **strategy_document["parameters"],
                            "lookback": [1],
                        },
                    },
                    {
                        **strategy_document,
                        "market_type": "crypto_spot",
                    },
                )
                for malformed in malformed_directional_documents:
                    with self.assertRaisesRegex(
                        ValueError,
                        "OPERATIONAL_SETUP_UNSUPPORTED",
                    ):
                        processor._ensure_rolling_paper_observation(
                            {
                                **source,
                                "strategy_document": malformed,
                                "strategy_hash": _content_hash(
                                    _normalized_strategy_document(malformed)
                                ),
                                "operational_setup": dict(setup),
                                "operational_setup_hash": _operational_setup_hash(setup),
                            },
                            T0,
                            market_ids=(market_id,),
                        )
                conflicting_sources = (
                    {
                        **source,
                        "dataset_boundary": {
                            **source["dataset_boundary"],
                            "dataset_id": "conflicting-history",
                        },
                    },
                    {
                        **source,
                        "dataset_attestation": {
                            **source["dataset_attestation"],
                            "dataset_version": "conflicting-v2",
                        },
                    },
                    {
                        **source,
                        "dataset_selector": {
                            "dataset_id": "conflicting-selector",
                            "dataset_version": source["dataset_version"],
                        },
                    },
                )
                for conflicting_source in conflicting_sources:
                    with self.assertRaisesRegex(
                        ValueError, "conflicting .*dataset"
                    ):
                        processor._ensure_rolling_paper_observation(
                            conflicting_source,
                            T0,
                            market_ids=(market_id,),
                        )
                refresh_state = processor.refresh_rolling_evidence(T0)
                self.assertIsInstance(refresh_state, Mapping)
                migrated_rows = [
                    item
                    for item in ForwardTestRegistry(store).list_observation_intents()
                    if str(item.config.get("candidate_id", "")).strip() == candidate_id
                ]
                migrated_ids = {
                    str(item.experiment_id)
                    for item in migrated_rows
                    if isinstance(item.config, Mapping)
                    and isinstance(item.config.get("observation_handoff"), Mapping)
                }
                self.assertEqual(len(migrated_ids), 1)
                lifecycle_after_migration = store.load_candidate_lifecycle(candidate_id)
                self.assertIsNotNone(lifecycle_after_migration)
                assert lifecycle_after_migration is not None
                self.assertNotEqual(
                    lifecycle_after_migration["payload"]["forward_test_id"],
                    old_forward_id,
                )
                self.assertEqual(
                    lifecycle_after_migration["payload"]["paper_observation_intent_id"],
                    next(iter(migrated_ids)),
                )
                self.assertEqual(
                    lifecycle_after_migration["payload"]["scope_resolution"],
                    proof.as_dict(),
                )
                refreshed_proof = resolve_market_scope(
                    candidate_id,
                    {"market_scope": policy.as_dict()},
                    [current_market],
                    resolved_at=T0 + timedelta(minutes=1),
                )
                store.save_market_scope_resolution(refreshed_proof)
                second_refresh = processor.refresh_rolling_evidence(T0 + timedelta(minutes=1))
                self.assertIsInstance(second_refresh, Mapping)
                refreshed_lifecycle = store.load_candidate_lifecycle(candidate_id)
                self.assertIsNotNone(refreshed_lifecycle)
                assert refreshed_lifecycle is not None
                self.assertEqual(
                    refreshed_lifecycle["payload"]["scope_resolution"],
                    refreshed_proof.as_dict(),
                )
                migrated_rows_after_restart = [
                    item
                    for item in ForwardTestRegistry(store).list_observation_intents()
                    if str(item.config.get("candidate_id", "")).strip() == candidate_id
                    and isinstance(item.config.get("observation_handoff"), Mapping)
                ]
                self.assertEqual(
                    [item.experiment_id for item in migrated_rows_after_restart],
                    sorted(migrated_ids),
                )
                binding = {"intent_id": next(iter(migrated_ids))}
                registry = ForwardTestRegistry(store)
                migrated_intent = registry.get(binding["intent_id"])
                self.assertIsNotNone(migrated_intent)
                assert migrated_intent is not None
                self.assertEqual(
                    migrated_intent.config["dataset_id"],
                    source["dataset_id"],
                )
                self.assertEqual(
                    migrated_intent.config["dataset_version"],
                    source["dataset_version"],
                )
                self.assertEqual(
                    migrated_intent.config["dataset_boundary"],
                    source["dataset_boundary"],
                )
                self.assertEqual(
                    migrated_intent.config["operational_setup"]["assessment_manifest_ref"][
                        "dataset_boundary"
                    ],
                    source["dataset_boundary"],
                )
                restarted = AutonomousResearchProcessor(store, clock=lambda: T0)
                restarted.refresh_rolling_evidence(T0 + timedelta(minutes=2))
                restarted_rows = [
                    item
                    for item in registry.list_observation_intents()
                    if str(item.config.get("candidate_id", "")).strip() == candidate_id
                    and isinstance(item.config.get("observation_handoff"), Mapping)
                ]
                self.assertEqual(
                    [item.experiment_id for item in restarted_rows],
                    sorted(migrated_ids),
                )
                restarted_binding = {"intent_id": next(iter(migrated_ids))}
                restarted_intent = registry.get(restarted_binding["intent_id"])
                self.assertIsNotNone(restarted_intent)
                assert restarted_intent is not None
                self.assertEqual(
                    restarted_intent.config,
                    migrated_intent.config,
                )
                self.assertNotEqual(migrated_intent.experiment_id, old_intent_id)
                self.assertEqual(
                    migrated_intent.config["observation_handoff"][
                        "predecessor_observation_intent_id"
                    ],
                    old_intent_id,
                )
                migrated_setup = migrated_intent.config["operational_setup"]
                self.assertIsInstance(migrated_setup, Mapping)
                assert isinstance(migrated_setup, Mapping)
                self.assertEqual(
                    migrated_setup["assessment_manifest_ref"]["dataset_id"],
                    source["dataset_id"],
                )
                self.assertEqual(
                    migrated_setup["assessment_manifest_ref"]["dataset_version"],
                    source["dataset_version"],
                )
                self.assertEqual(
                    migrated_setup["assessment_manifest_ref"]["manifest_digest"],
                    source["dataset_boundary"]["ordered_row_manifest_digest"],
                )
                self.assertEqual(
                    migrated_setup["assessment_manifest_ref"]["attestation_hash"],
                    source["dataset_attestation"]["attestation_hash"],
                )
                collector = PolymarketCollector(
                    InMemoryPredictionProvider([]),
                    store,
                    CollectorConfig(max_markets=1),
                    clock=lambda: T0,
                    sleep=lambda _seconds: None,
                )
                counters = collector._new_counters()
                ready = collector._materialize_observation_intents(
                    T0,
                    (candidate_id,),
                    {candidate_id: [market_id]},
                    counters,
                    scope_resolutions={candidate_id: proof.as_dict()},
                )
                self.assertEqual(ready, {candidate_id})
                lifecycle = store.load_candidate_lifecycle(candidate_id)
                self.assertIsNotNone(lifecycle)
                assert lifecycle is not None
                self.assertNotEqual(lifecycle["payload"]["forward_test_id"], old_forward_id)
                successor = registry.get(lifecycle["payload"]["forward_test_id"])
                self.assertIsNotNone(successor)
                assert successor is not None
                self.assertEqual(successor.allowed_markets, (market_id,))
                self.assertEqual(
                    successor.config["operational_setup_hash"],
                    migrated_intent.config["operational_setup_hash"],
                )
                old_successor = registry.get(old_forward_id)
                self.assertIsNotNone(old_successor)
                assert old_successor is not None
                self.assertNotIn("operational_setup", old_successor.config)
                active_candidates = collector._active_observation_intent_ids()
                self.assertEqual(active_candidates, [candidate_id])

    def test_observation_setup_migration_is_bounded_and_retries_failures(self) -> None:
        class _Policy:
            scope_hash = "scope-hash"
            scope_version = "scope-v1"

        class _Store:
            def __init__(self) -> None:
                self.records = []
                self.calls = []
                self.state = {}
                self.fail_load = False
                self.fail_save = False
                self.proof_mode = "MATCHED"

            def load_observation_intents(self, *, limit: int, after_experiment_id: str | None = None):
                self.calls.append((limit, after_experiment_id))
                ordered = self.records
                if after_experiment_id:
                    ordered = [row for row in ordered if row["experiment_id"] > after_experiment_id]
                return ordered[:limit]

            def get_scheduler_state(self, name: str):
                if self.fail_load:
                    raise sqlite3.OperationalError("state read")
                return dict(self.state.get(name, {}))

            def set_scheduler_state(self, name: str, payload: Mapping[str, Any]):
                if self.fail_save:
                    raise sqlite3.OperationalError("state write")
                self.state[name] = dict(payload)

            def load_candidate_lifecycle(self, candidate_id: str):
                stage = "REJECTED" if candidate_id == "candidate-0006" else "PAPER_FORWARD"
                return {"candidate_id": candidate_id, "stage": stage, "payload": {}}
            def load_market_scope_resolution(self, candidate_id: str, **_kwargs: Any):
                if self.proof_mode == "MISSING":
                    return None
                return {
                    "candidate_id": candidate_id,
                    "scope_hash": "scope-hash",
                    "scope_version": "scope-v1",
                    "resolved_at": (
                        T0 + timedelta(seconds=1)
                        if self.proof_mode == "FUTURE"
                        else (
                            T0 - timedelta(hours=2)
                            if self.proof_mode == "STALE"
                            else T0
                        )
                    ).isoformat(),
                    "status": "UNMATCHED" if self.proof_mode == "UNMATCHED" else "MATCHED",
                    "matched_markets": [{"market_id": "market-" + candidate_id}],
                    "excluded_markets": [],
                    "deferred_markets": [],
                }

        T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
        store = _Store()
        for index in range(513):
            candidate_id = f"candidate-{index:04d}"
            store.records.append(
                {
                    "experiment_id": f"observation-intent-{index:04d}",
                    "strategy_hash": "sha256:strategy",
                    "model_hash": "sha256:model",
                    "config": {
                        "observation_intent": True,
                        "candidate_id": candidate_id,
                        "strategy_document": {"family": "directional"},
                        "strategy_version_id": "version-" + candidate_id,
                        "research_trial_id": "trial-" + candidate_id,
                    },
                    "start_timestamp": T0,
                    "bankroll": 100.0,
                    "allowed_markets": [],
                    "risk_limits": {},
                    "quality": "PAPER_FORWARD",
                }
            )
        processor = object.__new__(AutonomousResearchProcessor)
        processor.store = store
        failed_once = {"candidate-0005": True}
        created: list[str] = []

        def ensure(strategy: Mapping[str, Any], _now: datetime, *, market_ids=()):
            candidate_id = str(strategy["candidate_id"])
            if failed_once.pop(candidate_id, False):
                raise sqlite3.OperationalError("transient materialization")
            created.append(candidate_id)
            self.assertEqual(tuple(market_ids), ("market-" + candidate_id,))
            return {"candidate_id": candidate_id}

        processor._ensure_rolling_paper_observation = ensure
        with patch("axiom.autonomous.normalize_market_scope", return_value=_Policy()), patch(
            "axiom.autonomous._operational_setup_for_strategy",
            return_value={"setup": "canonical"},
        ):
            first = processor._migrate_observation_setup_intents(T0)
            self.assertEqual(len(first), 510)
            self.assertEqual(store.calls[0], (512, None))
            self.assertEqual(store.state["autonomous-observation-setup-migration"]["cursor"], "observation-intent-0511")
            self.assertNotIn("candidate-0006", created)
            second = processor._migrate_observation_setup_intents(T0)
            self.assertEqual(store.calls[1], (512, "observation-intent-0511"))
            self.assertEqual(len(second), 1)
            third = processor._migrate_observation_setup_intents(T0)
            self.assertEqual(store.calls[2:], [(512, "observation-intent-0512"), (512, None)])
            self.assertIn("candidate-0005", created)
            self.assertGreaterEqual(len(third), 1)
            self.assertTrue(store.state["autonomous-observation-setup-migration"]["failures"] == [])
            for mode in ("STALE", "FUTURE", "UNMATCHED", "MISSING"):
                store.proof_mode = mode
                store.state["autonomous-observation-setup-migration"]["cursor"] = ""
                before = len(created)
                self.assertEqual(processor._migrate_observation_setup_intents(T0), ())
                self.assertEqual(len(created), before)
            store.proof_mode = "MATCHED"
            store.fail_save = True
            self.assertIsInstance(processor._migrate_observation_setup_intents(T0), tuple)
            store.fail_load = True
            self.assertEqual(processor._migrate_observation_setup_intents(T0), ())
    def test_rolling_initialization_commit_rolls_back_and_restart_retries_once(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            db = str(Path(directory) / "rolling-initialization-atomic.sqlite")
            enrolled = False
            with AxiomStore(db) as store:
                node = ResearchNode(
                    NodeConfig(
                        db,
                        mutation_enabled=False,
                        crypto_enabled=False,
                        rolling_review_interval_seconds=86400,
                    ),
                    provider=InMemoryPredictionProvider([]),
                    store=store,
                    clock=lambda: T0,
                )
                processor = node.research_processor
                processor._rolling_strategy_documents = (  # type: ignore[method-assign]
                    lambda: (_rolling_initialization_document(),) if enrolled else ()
                )
                processor._rolling_source_rows = lambda *_args: []  # type: ignore[method-assign]
                initial = processor.review_rolling_portfolio(now=T0)
                initial_selection = store.load_current_portfolio_selection()
                self.assertIsNotNone(initial_selection)
                assert initial_selection is not None
                initial_id = initial_selection["portfolio_selection_id"]
                self.assertEqual(initial["initialization_transition"], False)

                enrolled = True
                before_count = len(store.list_portfolio_selections(limit=32))
                with patch.object(
                    store,
                    "save_portfolio_review_state",
                    side_effect=RuntimeError("injected review-state failure"),
                ):
                    with self.assertRaisesRegex(RuntimeError, "injected review-state failure"):
                        processor.review_rolling_portfolio(
                            now=T0 + timedelta(hours=1)
                        )
                self.assertEqual(
                    len(store.list_portfolio_selections(limit=32)),
                    before_count,
                )
                rolled_back_selection = store.load_current_portfolio_selection()
                rolled_back_review = store.load_portfolio_review_state()
                self.assertIsNotNone(rolled_back_selection)
                self.assertIsNotNone(rolled_back_review)
                assert rolled_back_selection is not None
                assert rolled_back_review is not None
                self.assertEqual(
                    rolled_back_selection["portfolio_selection_id"], initial_id
                )
                self.assertEqual(
                    rolled_back_review["refresh_identity"]["strategy_versions_total"],
                    0,
                )

                retry = processor.review_rolling_portfolio(
                    now=T0 + timedelta(hours=1)
                )
                self.assertTrue(retry["initialization_transition"])
                self.assertEqual(retry["selection"]["members"], [])
                self.assertEqual(len(store.list_portfolio_selections(limit=32)), 2)

                restarted = ResearchNode(
                    NodeConfig(
                        db,
                        mutation_enabled=False,
                        crypto_enabled=False,
                        rolling_review_interval_seconds=86400,
                    ),
                    provider=InMemoryPredictionProvider([]),
                    store=store,
                    clock=lambda: T0 + timedelta(hours=1),
                )
                restarted_processor = restarted.research_processor
                restarted_processor._rolling_strategy_documents = (  # type: ignore[method-assign]
                    lambda: (_rolling_initialization_document(),)
                )
                restarted_processor._rolling_source_rows = lambda *_args: []  # type: ignore[method-assign]
                restarted_result = restarted_processor.review_rolling_portfolio(
                    now=T0 + timedelta(hours=1)
                )
                self.assertEqual(
                    restarted_result["portfolio_selection_id"],
                    retry["portfolio_selection_id"],
                )
                self.assertEqual(restarted_result["event_history"], retry["event_history"])
                self.assertEqual(len(store.list_portfolio_selections(limit=32)), 2)

    def test_rolling_review_clock_rollback_keeps_committed_timestamps_monotonic(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            db = str(Path(directory) / "rolling-initialization-clock-rollback.sqlite")
            with AxiomStore(db) as store:
                node = ResearchNode(
                    NodeConfig(
                        db,
                        mutation_enabled=False,
                        crypto_enabled=False,
                        rolling_review_interval_seconds=86400,
                    ),
                    provider=InMemoryPredictionProvider([]),
                    store=store,
                    clock=lambda: T0 + timedelta(days=2),
                )
                processor = node.research_processor
                processor._rolling_strategy_documents = lambda: ()  # type: ignore[method-assign]
                processor._rolling_source_rows = lambda *_args: []  # type: ignore[method-assign]
                future = T0 + timedelta(days=2)
                processor.review_rolling_portfolio(now=future, force=True)
                rolled_back = processor.review_rolling_portfolio(now=T0, force=True)
                selection = store.load_current_portfolio_selection()
                review = store.load_portfolio_review_state()
                self.assertIsNotNone(selection)
                self.assertIsNotNone(review)
                assert selection is not None
                assert review is not None
                self.assertEqual(selection["selected_at"], future.isoformat())
                self.assertEqual(selection["review_due_at"], (future + timedelta(days=1)).isoformat())
                self.assertEqual(review["reviewed_at"], future.isoformat())
                self.assertEqual(rolled_back["selection"]["selected_at"], future.isoformat())
                self.assertGreaterEqual(
                    datetime.fromisoformat(review["review_due_at"]),
                    datetime.fromisoformat(review["reviewed_at"]),
                )

                self.assertEqual(review["event_history"][-1]["at"], future.isoformat())

    def test_persisted_autonomous_fatal_survives_restart_while_research_runs(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            db = str(Path(directory) / "persisted-autonomous-fatal.sqlite")
            with AxiomStore(db) as store:
                seed = ResearchNode(
                    NodeConfig(db, mutation_enabled=True, crypto_enabled=False),
                    provider=InMemoryPredictionProvider([]),
                    store=store,
                    clock=lambda: T0,
                    sleep=lambda _: None,
                )
                seed._persist_autonomous_fatal("AUTONOMOUS_WORKER_TICK_EXHAUSTED")

                restarted = ResearchNode(
                    NodeConfig(db, mutation_enabled=True, crypto_enabled=False),
                    provider=InMemoryPredictionProvider([]),
                    store=store,
                    clock=lambda: T0,
                    sleep=lambda _: None,
                )
                self.assertTrue(restarted._auto_canary_fatal)
                self.assertNotIn(
                    "autonomous-canary",
                    restarted._worker_thread_specs(max_cycles=1),
                )

                def collector_worker(max_cycles: int | None) -> None:
                    restarted._collection_count += 1
                    with restarted._worker_condition:
                        restarted._worker_condition.notify_all()

                def research_worker() -> None:
                    with restarted._worker_condition:
                        restarted._research_passes += 1
                        restarted._worker_condition.notify_all()
                    while not restarted.stop_event.wait(0.01):
                        pass

                def health_worker() -> None:
                    with restarted._worker_condition:
                        restarted._health_passes += 1
                        restarted._worker_condition.notify_all()
                    while not restarted.stop_event.wait(0.01):
                        pass

                restarted._collector_worker_loop = collector_worker  # type: ignore[method-assign]
                restarted._research_worker_loop = research_worker  # type: ignore[method-assign]
                restarted._health_worker_loop = health_worker  # type: ignore[method-assign]
                restarted._configure_logging = lambda: None  # type: ignore[method-assign]
                restarted._start_heartbeat_watchdog = lambda: None  # type: ignore[method-assign]
                restarted._stop_heartbeat_watchdog = lambda: None  # type: ignore[method-assign]
                with patch.object(
                    restarted._auto_canary_worker,
                    "tick",
                    side_effect=AssertionError("persisted fatal worker must stay fenced"),
                ) as tick:
                    restarted.run(max_cycles=1)

                tick.assert_not_called()
                self.assertGreaterEqual(restarted._research_passes, 1)
                self.assertGreaterEqual(restarted._health_passes, 1)
                canonical = store.connection.execute(
                    "SELECT worker_status FROM canary_autonomous_state WHERE singleton=1"
                ).fetchone()
                self.assertIsNotNone(canonical)
                assert canonical is not None
                self.assertEqual(canonical["worker_status"], "FATAL")
                auto_state = store.get_worker_state("autonomous-canary")
                self.assertIsNotNone(auto_state)
                assert auto_state is not None
                self.assertEqual(auto_state["status"], "fatal")
                self.assertEqual(auto_state["payload"]["worker_status"], "FATAL")
                self.assertTrue(auto_state["payload"]["fatal"])
                self.assertEqual(
                    restarted.status()["workers"]["autonomous-canary"]["status"],
                    "fatal",
                )

    def test_unknown_no_retry_result_marks_worker_degraded(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            db = str(Path(directory) / "unknown-no-retry.sqlite")
            with AxiomStore(db) as store:
                node = ResearchNode(
                    NodeConfig(db, crypto_enabled=False),
                    provider=InMemoryPredictionProvider([]),
                    store=store,
                )
                result = {
                    "status": "BLOCKED",
                    "decision": "UNKNOWN_NO_RETRY",
                    "blocker": "UNKNOWN_NO_RETRY",
                }
                with patch.object(
                    node._auto_canary_worker,
                    "tick",
                    return_value=result,
                ) as tick, patch.object(
                    node,
                    "_worker_tick_completed",
                ) as tick_completed, patch.object(
                    node.stop_event,
                    "wait",
                    return_value=True,
                ):
                    node._auto_canary_worker_loop()
                tick.assert_called_once()
                self.assertFalse(tick_completed.call_args.kwargs["successful"])
                self.assertEqual(
                    tick_completed.call_args.kwargs["error"],
                    "UNKNOWN_NO_RETRY",
                )


    def test_capture_families_persist_safety_and_reload_for_node_route(self) -> None:
        families = (
            (
                "probability_mispricing",
                {
                    "threshold": 0.05,
                },
                "fixed",
                {"probability": 0.5},
                False,
            ),
            (
                "momentum",
                {
                    "lookback": 1,
                    "threshold": 0.05,
                    "entry_predicate": {
                        "version": "absolute-move-v1",
                        "minimum_move": 0.05,
                        "units": "probability",
                        "boundary": "inclusive",
                    },
                },
                "market-history",
                {"model_required": False},
                True,
            ),
            (
                "mean_reversion",
                {
                    "lookback": 1,
                    "threshold": 0.05,
                    "entry_predicate": {
                        "version": "absolute-move-v1",
                        "minimum_move": 0.05,
                        "units": "probability",
                        "boundary": "inclusive",
                    },
                },
                "market-history",
                {"model_required": False},
                True,
            ),
        )
        for index, (family, parameters, probability_model, model, explicit_capture) in enumerate(
            families
        ):
            with self.subTest(family=family):
                candidate_id = f"capture-family-{family}"
                market_id = f"capture-family-market-{index}"
                strategy_document = {
                    "version": 1,
                    "market_type": "prediction",
                    "family": family,
                    "parameters": parameters,
                    "probability_model": probability_model,
                    "resolution_aware": True,
                    "resolution_inputs": ["expiry", "settlement"],
                }
                policy = normalize_market_scope(
                    target_instrument="POLYMARKET"
                )
                market = {
                    "market_id": market_id,
                    "condition_id": "condition-" + market_id,
                    "yes_token_id": "yes-" + market_id,
                    "no_token_id": "no-" + market_id,
                    "metadata_provenance": {"source_type": "CURRENT"},
                    "source_type": "CURRENT",
                    "provider": "polymarket",
                    "venue": "POLYMARKET",
                    "instrument": "POLYMARKET",
                    "active": True,
                    "closed": False,
                    "settlement": "OPEN",
                    "enable_order_book": True,
                    "accepting_orders": True,
                }
                with tempfile.TemporaryDirectory() as directory:
                    db = str(Path(directory) / "capture-family.sqlite")
                    with AxiomStore(db) as store:
                        proof = resolve_market_scope(
                            candidate_id,
                            {"market_scope": policy.as_dict()},
                            [market],
                            resolved_at=T0,
                        )
                        proof_mapping = proof.as_dict()
                        provenance = dict(proof_mapping["provenance"])
                        current_market_set = dict(provenance["current_market_set"])
                        inventory_digest = current_market_set.pop("inventory_digest")
                        current_market_set["order_token"] = inventory_digest
                        provenance["current_market_set"] = current_market_set
                        proof_mapping["provenance"] = provenance
                        proof_mapping["excluded_markets"] = [
                            {
                                "market_id": f"{market_id}-expired",
                                "reason": "MARKET_EXPIRED",
                                "detail": "",
                                "metadata": {
                                    "action": "SUITABLE",
                                    "category": "SUITABLE_MARKET",
                                    "intended_token": "yes",
                                    "token_id": f"private-token-{family}",
                                    "resolver": "inspect_market_depth",
                                },
                            }
                        ]
                        processor = AutonomousResearchProcessor(
                            store,
                            config=AutonomousResearchConfig(
                                scope_resolution_freshness_sla_seconds=60
                            ),
                            clock=lambda: T0,
                        )
                        store.save_market_scope_resolution(proof_mapping)
                        source = {
                            "candidate_id": candidate_id,
                            "strategy_version_id": f"{candidate_id}-version",
                            "research_trial_id": f"{candidate_id}-trial",
                            "strategy_hash": _content_hash(
                                _normalized_strategy_document(strategy_document)
                            ),
                            "strategy_document": strategy_document,
                            "model_document": model,
                            "market_scope": policy.as_dict(),
                            "scope_resolution_freshness_sla_seconds": 60,
                        }
                        if explicit_capture:
                            source["observation_capture_only"] = True
                        binding = processor._ensure_rolling_paper_observation(
                            source,
                            T0,
                            market_ids=(market_id,),
                        )
                        self.assertIsNotNone(binding)
                        intent_id = str(binding["intent_id"])
                        registry = ForwardTestRegistry(store)
                        materialized = next(
                            item
                            for item in registry.list()
                            if item.config.get("candidate_id") == candidate_id
                            and item.config.get("market_authority_required") is True
                        )
                        config = materialized.config
                        self.assertEqual(config.get("execution_scope"), "OBSERVATION")
                        self.assertTrue(config.get("observation_capture_only"))
                        self.assertTrue(config.get("research_only"))
                        self.assertTrue(config.get("selection_excluded"))
                        self.assertFalse(config.get("allocation_active"))
                        self.assertFalse(config.get("canary_armed"))
                        self.assertNotIn("operational_setup", config)
                        self.assertNotIn("operational_setup_hash", config)
                        self.assertNotIn("canonical_operational_setup_required", config)
                        self.assertEqual(config.get("capture_market_id"), market_id)
                        public_proof = config["scope_resolution"]
                        self.assertEqual(
                            public_proof["resolved_at"],
                            proof_mapping["resolved_at"],
                        )
                        self.assertEqual(
                            tuple(
                                item["market_id"]
                                for item in public_proof["matched_markets"]
                            ),
                            (market_id,),
                        )
                        public_excluded = public_proof["excluded_markets"][0]
                        self.assertNotIn(
                            "intended_token",
                            public_excluded["metadata"],
                        )
                        self.assertNotIn("token_id", public_excluded["metadata"])
                        self.assertEqual(
                            public_proof["provenance"]["current_market_set"][
                                "inventory_digest"
                            ],
                            inventory_digest,
                        )
                        self.assertNotIn(
                            "order_token",
                            public_proof["provenance"]["current_market_set"],
                        )
                        raw_proof = store.load_market_scope_resolution(candidate_id)
                        self.assertIsNotNone(raw_proof)
                        assert raw_proof is not None
                        raw_proof_mapping = raw_proof.as_dict()
                        self.assertEqual(
                            raw_proof_mapping["candidate_id"],
                            proof_mapping["candidate_id"],
                        )
                        self.assertEqual(
                            raw_proof_mapping["matched_markets"],
                            proof_mapping["matched_markets"],
                        )
                        self.assertEqual(
                            raw_proof_mapping["excluded_markets"],
                            proof_mapping["excluded_markets"],
                        )
                        self.assertEqual(
                            raw_proof_mapping["provenance"]["current_market_set"][
                                "inventory_digest"
                            ],
                            inventory_digest,
                        )
                        lifecycle_payload = {
                            "candidate_id": candidate_id,
                            "paper_observation_intent": True,
                            "paper_observation_intent_id": intent_id,
                            "forward_test_id": materialized.experiment_id,
                            "paper_only": True,
                            "research_only": True,
                            "paper_forward_started": True,
                            "holdout_used": False,
                            "execution_scope": "OBSERVATION",
                            "observation_only_lineage": True,
                            "selection_excluded": True,
                            "allocation_active": False,
                            "canary_armed": False,
                            "scope_resolution": proof_mapping,
                            "market_scope_resolution": proof_mapping,
                            "current_market_ids": [market_id],
                            "resolved_market_ids": [market_id],
                            "scope_hash": policy.scope_hash,
                            "scope_version": policy.scope_version,
                        }
                        store.save_candidate_lifecycle(
                            candidate_id,
                            CandidateStage.IDEA.value,
                            {"candidate_id": candidate_id},
                            timestamp=T0,
                        )
                        store.save_candidate_lifecycle(
                            candidate_id,
                            CandidateStage.PAPER_FORWARD.value,
                            lifecycle_payload,
                            from_stage=CandidateStage.IDEA.value,
                            reason="capture family materialized",
                            timestamp=T0,
                        )
                    with AxiomStore(db) as reloaded:
                        persisted = ForwardTestRegistry(reloaded).get(
                            materialized.experiment_id
                        )
                        self.assertIsNotNone(persisted)
                        assert persisted is not None
                        restarted_processor = AutonomousResearchProcessor(
                            reloaded,
                            config=AutonomousResearchConfig(
                                scope_resolution_freshness_sla_seconds=60
                            ),
                            clock=lambda: T0,
                        )
                        repeated = restarted_processor._ensure_rolling_paper_observation(
                            source,
                            T0,
                            market_ids=(market_id,),
                        )
                        self.assertEqual(repeated["intent_id"], intent_id)
                        self.assertEqual(
                            len(
                                [
                                    item
                                    for item in ForwardTestRegistry(reloaded).list()
                                    if item.config.get("candidate_id") == candidate_id
                                    and item.config.get("market_authority_required") is True
                                ]
                            ),
                            1,
                        )
                        capture_node = ResearchNode.__new__(ResearchNode)
                        capture_node.store = reloaded
                        capture_node.config = SimpleNamespace(shadow_interval=60)
                        result = capture_node._capture_legacy_observations(
                            persisted,
                            [
                                {
                                    "market_id": market_id,
                                    "timestamp": T0,
                                    "source_timestamp": T0,
                                    "source_snapshot_id": f"capture-{family}",
                                }
                            ],
                            store=reloaded,
                            now=T0,
                        )
                        self.assertNotEqual(
                            result.get("blocker"),
                            "OBSERVATION_LIFECYCLE_IDENTITY_MISMATCH",
                        )
                        self.assertEqual(result.get("status"), "OBSERVING", repr(result))

class NodeStopPollingTests(unittest.TestCase):
    def test_stop_closes_collector_resources(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            db = str(Path(directory) / "stop-closes-collector.sqlite")
            with AxiomStore(db) as store:
                node = ResearchNode(
                    NodeConfig(db, crypto_enabled=False),
                    provider=InMemoryPredictionProvider([]),
                    store=store,
                )
                close = Mock()
                with patch.object(node.collector, "close", close):
                    node.stop()
                close.assert_called_once_with()

    def test_run_finally_closes_collector_resources(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            db = str(Path(directory) / "run-closes-collector.sqlite")
            with AxiomStore(db) as store:
                node = ResearchNode(
                    NodeConfig(db, crypto_enabled=False),
                    provider=InMemoryPredictionProvider([]),
                    store=store,
                )
                close = Mock()
                with patch.object(node.collector, "close", close):
                    node.run(max_cycles=0)
                close.assert_called_once_with()

    def test_only_exact_owned_stop_marker_authorizes_shutdown(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            db = str(Path(directory) / "stop-marker.sqlite")
            with AxiomStore(db) as store:
                node = ResearchNode(
                    NodeConfig(db, crypto_enabled=False),
                    provider=InMemoryPredictionProvider([]),
                    store=store,
                )
                lock_marker = f"{os.getpid()}\nowned-run\n"
                Path(node.lock_path).write_text(lock_marker, encoding="ascii")
                malformed = Path(node.stop_path)
                malformed.write_text("not-a-canonical-marker", encoding="ascii")
                self.assertFalse(node._external_stop_requested())
                self.assertFalse(node.stop_event.is_set())
                self.assertEqual(malformed.read_text(encoding="ascii"), "not-a-canonical-marker")

                wrong_run = f"{os.getpid()}\nother-run\n"
                malformed.write_text(wrong_run, encoding="ascii")
                self.assertFalse(node._external_stop_requested())
                self.assertFalse(node.stop_event.is_set())
                self.assertEqual(malformed.read_text(encoding="ascii"), wrong_run)

                malformed.write_text(lock_marker, encoding="ascii")
                self.assertTrue(node._external_stop_requested())
                self.assertTrue(node.stop_event.is_set())

    def test_owned_stop_marker_wakes_long_collection_interval(self) -> None:
        class OneCycleCollector:
            def __init__(self) -> None:
                self.completed = threading.Event()

            def collect_once(self) -> CollectionCycle:
                started = datetime.now(UTC)
                ended = datetime.now(UTC)
                self.completed.set()
                return CollectionCycle(
                    started,
                    ended,
                    0,
                    markets_attempted=0,
                    markets_successful=0,
                    markets_failed=0,
                    metadata_inserted=0,
                    snapshots_inserted=0,
                    snapshot_duplicates=0,
                    trades_inserted=0,
                    trade_duplicates=0,
                    errors=0,
                    elapsed_seconds=0.0,
                )

        with tempfile.TemporaryDirectory() as directory:
            db = str(Path(directory) / "stop-polling.sqlite")
            with AxiomStore(db) as store:
                node = ResearchNode(
                    NodeConfig(
                        db,
                        interval_seconds=60.0,
                        crypto_enabled=False,
                    ),
                    provider=InMemoryPredictionProvider([]),
                    store=store,
                )
                collector = OneCycleCollector()
                node.collector = collector  # type: ignore[assignment]
                node._configure_logging = lambda: None  # type: ignore[method-assign]
                node._start_heartbeat_watchdog = lambda: None  # type: ignore[method-assign]
                node._stop_heartbeat_watchdog = lambda: None  # type: ignore[method-assign]
                errors: list[BaseException] = []

                def run_node() -> None:
                    try:
                        node.run()
                    except BaseException as exc:  # pragma: no cover - surfaced below
                        errors.append(exc)

                runner = threading.Thread(target=run_node, daemon=True)
                runner.start()
                try:
                    self.assertTrue(collector.completed.wait(timeout=2.0))
                    marker = Path(node.lock_path).read_text(encoding="ascii")
                    started_waiting = time.monotonic()
                    Path(node.stop_path).write_text(marker, encoding="ascii")
                    runner.join(timeout=2.0)
                    self.assertFalse(runner.is_alive())
                    self.assertLess(time.monotonic() - started_waiting, 2.0)
                    self.assertFalse(errors, repr(errors))
                finally:
                    node.stop_event.set()
                    runner.join(timeout=2.0)


class CollectorConcurrencyTests(unittest.TestCase):
    def test_max_two_requires_explicit_isolated_workers_and_unsafe_provider_is_serial(self) -> None:
        class SharedActivity:
            def __init__(self) -> None:
                self.lock = threading.Lock()
                self.active = 0
                self.max_active = 0

        class BlockingProvider(InMemoryPredictionProvider):
            provider_name = "blocking-test"

            def __init__(self, activity: SharedActivity, *, isolated: bool = False) -> None:
                super().__init__([])
                self.activity = activity
                if isolated:
                    self.isolated_worker_factory = lambda: BlockingProvider(activity)

            def market(self, market_id: str):
                del market_id
                with self.activity.lock:
                    self.activity.active += 1
                    self.activity.max_active = max(self.activity.max_active, self.activity.active)
                try:
                    time.sleep(0.03)
                    return None
                finally:
                    with self.activity.lock:
                        self.activity.active -= 1

        for isolated, expected_max_active in ((False, 1), (True, 2)):
            with self.subTest(isolated=isolated), AxiomStore(":memory:") as store:
                activity = SharedActivity()
                provider = BlockingProvider(activity, isolated=isolated)
                collector = PolymarketCollector(
                    provider,
                    store,
                    CollectorConfig(
                        interval_seconds=60,
                        market_ids=("worker-a", "worker-b"),
                        max_markets=2,
                        max_attempts=1,
                        max_concurrency=2,
                        jitter_seconds=0,
                    ),
                    clock=lambda: T0,
                    sleep=lambda _seconds: None,
                )
                cycle = collector.collect_once(now=T0)

                self.assertEqual(activity.max_active, expected_max_active)
                self.assertEqual(cycle.markets_attempted, 2)
                self.assertLessEqual(activity.max_active, 2)


if __name__ == "__main__":
    unittest.main()
