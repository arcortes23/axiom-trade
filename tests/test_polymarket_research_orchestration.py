from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import json
import unittest
from unittest.mock import patch

from axiom.autonomous import (
    AutonomousResearchConfig,
    AutonomousResearchProcessor,
    _canonical_binding,
    _proposal_identity,
)
from axiom.experiment_plan import ExperimentPlan
from axiom.legacy_scope import LegacyScopeError, create_legacy_successor
from axiom.lifecycle import CandidateStage
from axiom.storage import AxiomStore
from axiom.collector import CollectorConfig, PolymarketCollector
from axiom.data import InMemoryPredictionProvider
from axiom.domain import Fill, MarketType, OrderBookLevel, OrderBookSnapshot, PredictionMarketSnapshot, SettlementState, Side
from axiom.forward import ForwardTestRegistry
from axiom.paper_engine import run_forward_paper
from axiom.research_bus import DurableResearchBus
from axiom.strategy import load_strategy
from tools.polymarket_release_smoke import _run_recorded_book_replay


UTC = timezone.utc
T0 = datetime(2025, 1, 1, tzinfo=UTC)


class _RecordedReplayStore:
    def __init__(self, plans: dict[str, ExperimentPlan], resolutions: dict[str, dict[str, object]]) -> None:
        self._plans = plans
        self._resolutions = resolutions

    def load_experiment_plan(self, plan_id: str) -> dict[str, object] | None:
        plan = self._plans.get(plan_id)
        if plan is None:
            return None
        return {
            "plan_id": plan.plan_id,
            "hypothesis_id": plan.hypothesis_id,
            "plan_hash": plan.plan_hash,
            "plan": plan.as_dict(),
        }

    def load_market_scope_resolution(
        self,
        candidate_id: str,
        *,
        scope_hash: str,
        scope_version: str,
    ) -> dict[str, object] | None:
        resolution = self._resolutions.get(candidate_id)
        if resolution is None:
            return None
        if resolution.get("scope_hash") != scope_hash or resolution.get("scope_version") != scope_version:
            return None
        return resolution


def _recorded_replay_plan(plan_id: str, market_id: str) -> ExperimentPlan:
    return ExperimentPlan.from_mapping(
        {
            "plan_id": plan_id,
            "market_type": "prediction",
            "template": "probability_mispricing",
            "parameters": {"threshold": [0.05]},
            "dataset_id": "replay-dataset",
            "dataset_version": "v1",
            "market_scope": {
                "schema_version": "1",
                "mode": "EXACT_MARKETS",
                "instrument": "POLYMARKET",
                "market_ids": [market_id],
                "provenance": "canonical",
            },
            "research_mode": "RECORDED_BOOK_REPLAY",
            "assumptions": {
                "version": "recorded-book-replay-v1",
                "fee_bps": 10.0,
                "slippage_bps": 5.0,
            },
            "exit_policy": {"type": "fixed_holding_period", "holding_period": 1},
            "methodology": {"initial_cash": 10_000.0, "allocation": 0.25},
            "model_document": {"field": "yes_mid"},
            "paper_only": True,
        },
        hypothesis_id=f"hypothesis-{plan_id}",
    )


def _recorded_replay_candidate(candidate_id: str, plan: ExperimentPlan) -> dict[str, object]:
    scope = plan.market_scope.as_dict()
    return {
        "candidate_id": candidate_id,
        "payload": {
            "candidate_id": candidate_id,
            "plan_id": plan.plan_id,
            "plan_hash": plan.plan_hash,
            "market_scope": scope,
            "market_scope_hash": plan.market_scope_hash,
            "market_scope_version": plan.market_scope_version,
            "scope_hash": plan.market_scope_hash,
            "scope_version": plan.market_scope_version,
            "experiment_plan": plan.as_dict(),
            "generation": 0,
        },
    }


def _recorded_replay_record(market_id: str) -> dict[str, object]:
    timestamp = T0.isoformat()
    return {
        "record_type": "snapshot",
        "market_id": market_id,
        "source_timestamp": timestamp,
        "observed_at": (T0 + timedelta(seconds=1)).isoformat(),
        "snapshot_id": f"snapshot-{market_id}",
        "raw_record_hash": f"sha256:raw-{market_id}",
        "payload": {
            "market_id": market_id,
            "timestamp": timestamp,
            "yes_mid": 0.40,
            "yes_bid": 0.39,
            "yes_ask": 0.41,
            "no_mid": 0.60,
            "no_bid": 0.59,
            "no_ask": 0.61,
            "settlement": "open",
            "order_book": {
                "timestamp": timestamp,
                "bids": [[0.39, 10.0]],
                "asks": [[0.41, 10.0]],
                "token_id": f"yes-{market_id}",
            },
            "no_order_book": {
                "timestamp": timestamp,
                "bids": [[0.59, 10.0]],
                "asks": [[0.61, 10.0]],
                "token_id": f"no-{market_id}",
            },
        },
    }


def _recorded_replay_resolution(
    candidate_id: str,
    plan: ExperimentPlan,
    *,
    matched_ids: tuple[str, ...] | None = None,
    status: str = "MATCHED",
) -> dict[str, object]:
    selected_ids = plan.market_scope.market_ids if matched_ids is None else matched_ids
    return {
        "candidate_id": candidate_id,
        "scope_hash": plan.market_scope_hash,
        "scope_version": plan.market_scope_version,
        "status": status,
        "reason": status,
        "policy": plan.market_scope.as_dict(),
        "matched_markets": [{"market_id": market_id} for market_id in selected_ids],
        "resolution_id": f"resolution-{candidate_id}",
    }
def _seed_predeclared_historical_dataset(store: AxiomStore, *, attested: bool = True) -> None:
    rows = [
        {
            "timestamp": (T0 + timedelta(minutes=index)).isoformat(),
            "market_id": f"market-{index // 3}",
            "yes_mid": 0.40 + ((index % 10) * 0.01),
            "yes_bid": 0.39 + ((index % 10) * 0.01),
            "yes_ask": 0.41 + ((index % 10) * 0.01),
            "no_mid": 0.60 - ((index % 10) * 0.01),
            "no_bid": 0.59 - ((index % 10) * 0.01),
            "no_ask": 0.61 - ((index % 10) * 0.01),
            "expiry": (T0 + timedelta(days=1)).isoformat(),
            "settlement": "resolved_yes" if index % 3 == 2 else "open",
            "source_type": "HISTORICAL",
        }
        for index in range(100)
    ]
    store.save_dataset("Polymarket-historical", "history-v1", rows)
    store.save_dataset_catalog(
        "Polymarket-historical",
        "history-v1",
        provider="fixture",
        instrument="POLYMARKET",
        market_type="prediction",
        timeframe="event",
        start_timestamp=T0,
        end_timestamp=T0 + timedelta(minutes=99),
        row_count=len(rows),
        completeness=1.0,
        missing_ranges=(),
        quality="PRICE_PROXY",
        source_type="HISTORICAL",
        snapshot_id="snapshot-v1",
        metadata={
            "research_quality": "PRICE_PROXY",
            "provenance_version": "dataset-provenance-v1",
            "policy_version": "prediction-integrity-v1",
        },
    )
    if attested:
        attestation = store.verify_dataset_integrity_attestation("Polymarket-historical", "history-v1")
        assert attestation["status"] == "CURRENT"
        assert attestation["contamination_result"] == "PASS"

def _legacy_prediction_predecessor(
    *,
    selector: dict[str, object] | None = None,
    candidate_id: str = "legacy-selector-predecessor",
) -> dict[str, object]:
    plan: dict[str, object] = {
        "market_type": "prediction",
        "template": "momentum",
        "parameters": {"lookback": [1], "threshold": [0.05]},
        "target": {"market_ids": ["market-0"]},
        "dataset_id": "Polymarket-historical",
        "dataset_version": "history-v1",
        "allowed_features": ["timestamp", "market_id", "yes_mid"],
        "time_split": "train-validation-holdout",
        "min_samples": 1,
        "min_trades": 0,
        "max_variants": 1,
        "methodology": {"initial_cash": 1_000.0, "allocation": 0.25},
        "paper_only": True,
    }
    if selector is not None:
        plan["dataset_selector"] = selector
    return {
        "candidate_id": candidate_id,
        "frozen_hash": f"sha256:{candidate_id}-frozen",
        "hypothesis_id": f"{candidate_id}-hypothesis",
        "statement": "A legacy selector must remain bound to its exact catalog.",
        "source": "offline legacy fixture",
        "market_type": "prediction",
        "experiment_plan": plan,
        "paper_only": True,
    }


class PolymarketResearchOrchestrationTests(unittest.TestCase):
    def test_process_pending_seeds_bounded_starting_set_idempotently(self) -> None:
        with AxiomStore(":memory:") as store:
            _seed_predeclared_historical_dataset(store)
            processor = AutonomousResearchProcessor(
                store,
                config=AutonomousResearchConfig(max_items_per_cycle=1),
                clock=lambda: T0,
            )
            cycle = processor.process_pending(now=T0)
            self.assertEqual(cycle.claimed, 1)
            self.assertEqual(store.research_queue_stats().get("total"), 3)
            processor.process_pending(now=T0)
            processor.process_pending(now=T0)
            self.assertEqual(store.research_queue_stats().get("total"), 3)
            queued = store.list_research_items(limit=10)
            self.assertEqual(len(queued), 3)
            self.assertTrue(all(item["payload"].get("predeclared_starting_set") for item in queued))

    def test_predeclared_attestation_rotation_versions_queue_once(self) -> None:
        with AxiomStore(":memory:") as store:
            _seed_predeclared_historical_dataset(store)
            processor = AutonomousResearchProcessor(
                store,
                config=AutonomousResearchConfig(max_items_per_cycle=1),
                clock=lambda: T0,
            )
            first = processor._enqueue_predeclared_from_persisted_scope(T0)
            self.assertEqual(len(first), 3)
            old_payloads = {item.item_id: item.payload for item in first}
            old_hashes = {
                item.payload["provenance"]["internal"]["attestation_hash"]
                for item in first
            }
            self.assertEqual(len(old_hashes), 1)
            rotated_hash = "sha256:rotated-predeclared-attestation"
            store.connection.execute(
                "UPDATE dataset_integrity_attestation "
                "SET status='CURRENT', contamination_result='PASS', "
                "attestation_hash=?, reason='ATTESTATION_ROTATED' "
                "WHERE dataset_id=? AND dataset_version=?",
                (rotated_hash, "Polymarket-historical", "history-v1"),
            )
            store.connection.commit()

            second = processor._enqueue_predeclared_from_persisted_scope(T0)
            self.assertEqual(len(second), 3)
            self.assertEqual(store.research_queue_stats()["total"], 6)
            self.assertEqual(
                {item.payload["provenance"]["internal"]["attestation_hash"] for item in second},
                {rotated_hash},
            )
            self.assertTrue(
                {item.item_id for item in first}.isdisjoint(
                    item.item_id for item in second
                )
            )
            self.assertEqual(
                {_proposal_identity(item.payload) for item in first},
                {_proposal_identity(item.payload) for item in second},
            )
            rows = {
                item["item_id"]: item["payload"]
                for item in store.list_research_items(limit=10)
            }
            self.assertEqual(
                {item_id: rows[item_id] for item_id in old_payloads},
                old_payloads,
            )

            retry = processor._enqueue_predeclared_from_persisted_scope(T0)
            self.assertEqual(
                tuple(item.item_id for item in retry),
                tuple(item.item_id for item in second),
            )
            self.assertEqual(store.research_queue_stats()["total"], 6)


    def test_unchanged_predeclared_generated_identity_processes(self) -> None:
        with AxiomStore(":memory:") as store:
            _seed_predeclared_historical_dataset(store)
            processor = AutonomousResearchProcessor(
                store,
                config=AutonomousResearchConfig(max_items_per_cycle=1),
                clock=lambda: T0,
            )
            processor._enqueue_predeclared_from_persisted_scope(T0)
            queued = store.list_research_items(limit=10)
            self.assertEqual(len(queued), 3)
            for item in queued:
                payload = item["payload"]
                internal = payload["provenance"]["internal"]
                self.assertEqual(internal["proposal_identity"], _proposal_identity(payload))

            cycle = processor.process_pending(now=T0)
            self.assertEqual(cycle.claimed, 1)
            # The fixture intentionally has too few qualifying trades for the
            # starter's research gate; marker validation must still succeed
            # and leave the candidate schema-valid with an explicit
            # insufficiency result rather than PROCESSING_FAILED.
            self.assertEqual(cycle.rejected, 1)
            self.assertEqual(cycle.failed, 0)
            self.assertEqual(
                cycle.results[0]["candidate_results"][0]["stage"],
                CandidateStage.SCHEMA_VALIDATED.value,
            )
            self.assertEqual(cycle.results[0]["reason_code"], "INSUFFICIENT_DATA")

    def test_sparse_predeclared_generated_payload_normalizes_before_marker(self) -> None:
        with AxiomStore(":memory:") as store:
            _seed_predeclared_historical_dataset(store)
            processor = AutonomousResearchProcessor(
                store,
                config=AutonomousResearchConfig(max_items_per_cycle=1),
                clock=lambda: T0,
            )
            sparse = _legacy_prediction_predecessor(candidate_id="sparse-generated-proposal")
            for field in ("source", "tests", "time_split", "paper_only", "candidate_id", "frozen_hash"):
                sparse.pop(field, None)
            queued = processor.enqueue_predeclared_starting_set(
                sparse,
                strategies=(
                    {"template": "momentum", "parameters": {"lookback": (1,), "threshold": (0.05,)}},
                ),
                available_at=T0,
            )
            self.assertEqual(len(queued), 1)
            item = queued[0]
            internal = item.payload["provenance"]["internal"]
            self.assertEqual(internal["proposal_identity"], _proposal_identity(item.payload))

            cycle = processor.process_pending(now=T0)
            self.assertEqual(cycle.claimed, 1)
            self.assertNotEqual(cycle.results[0]["reason_code"], "GENERATED_PROVENANCE_INVALID")

    def test_tampered_generated_marker_rejects_before_processing(self) -> None:
        with AxiomStore(":memory:") as store:
            _seed_predeclared_historical_dataset(store)
            processor = AutonomousResearchProcessor(
                store,
                config=AutonomousResearchConfig(max_items_per_cycle=1),
                clock=lambda: T0,
            )
            processor._enqueue_predeclared_from_persisted_scope(T0)
            item = store.list_research_items(limit=1)[0]
            payload = dict(item["payload"])
            provenance = dict(payload["provenance"])
            internal = dict(provenance["internal"])
            internal["proposal_identity"] = "sha256:tampered-generated-identity"
            provenance["internal"] = internal
            payload["provenance"] = provenance
            store.connection.execute(
                "UPDATE research_queue SET payload_json=? WHERE item_id=?",
                (json.dumps(payload, sort_keys=True, separators=(",", ":")), item["item_id"]),
            )
            store.connection.commit()

            cycle = processor.process_pending(now=T0)
            self.assertEqual(cycle.claimed, 1)
            self.assertEqual(cycle.rejected, 1)
            self.assertEqual(cycle.results[0]["reason_code"], "GENERATED_PROVENANCE_INVALID")

    def test_generated_mutation_requires_exact_provenance_manual_hypothesis_stays_compatible(self) -> None:
        with AxiomStore(":memory:") as store:
            _seed_predeclared_historical_dataset(store)
            predecessor = _legacy_prediction_predecessor(candidate_id="mutation-parent")
            plan_document = dict(predecessor["experiment_plan"])
            plan_document["target"] = {
                "instrument": "POLYMARKET",
                "market_ids": ["market-0"],
            }
            plan_document["market_scope"] = {
                "schema_version": "1",
                "mode": "EXACT_MARKETS",
                "instrument": "POLYMARKET",
                "market_ids": ["market-0"],
                "categories": [],
                "filters": {},
                "regime_restrictions": {},
                "provenance": "canonical",
            }
            plan = ExperimentPlan.from_mapping(
                plan_document,
                hypothesis_id="mutation-parent-hypothesis",
            )
            store.save_experiment_plan(
                plan.plan_id,
                plan.as_dict(),
                hypothesis_id=plan.hypothesis_id,
                plan_hash=plan.plan_hash,
                status="ACCEPTED",
                timestamp=T0,
            )
            parent_id = "mutation-parent"
            parameters = plan.variants()[0]
            strategy = plan.strategy_for(parameters, parent_id)
            parent_payload = {
                "candidate_id": parent_id,
                "plan_id": plan.plan_id,
                "plan_hash": plan.plan_hash,
                "dataset_id": plan.dataset_id,
                "dataset_version": plan.dataset_version,
                "generation": 0,
            }
            store.save_candidate_lifecycle(parent_id, CandidateStage.IDEA.value, parent_payload)
            store.save_candidate_lifecycle(
                parent_id,
                CandidateStage.ROBUSTNESS_CHECKED.value,
                {**parent_payload, "robustness_passed": True, "holdout_used": False},
                from_stage=CandidateStage.IDEA.value,
                timestamp=T0,
            )
            processor = AutonomousResearchProcessor(
                store,
                config=AutonomousResearchConfig(max_items_per_cycle=1, max_children_per_parent=1),
                clock=lambda: T0,
            )
            children = processor._generate_mutations(
                plan,
                [{"candidate_id": parent_id, "strategy": strategy}],
                {parent_id: 1.0},
                T0,
            )
            self.assertEqual(len(children), 1)
            child_item = store.list_research_items(limit=1)[0]
            self.assertTrue(child_item["payload"]["provenance"]["internal"]["generated"])
            self.assertEqual(child_item["payload"]["provenance"]["internal"]["kind"], "mutation_child")

            store.connection.execute(
                "DELETE FROM dataset_catalog WHERE dataset_id=? AND dataset_version=?",
                (plan.dataset_id, plan.dataset_version),
            )
            store.connection.commit()
            blocked = processor.process_pending(now=T0)
            self.assertEqual(blocked.failed, 0, repr(blocked))
            self.assertEqual(blocked.rejected, 1, repr(blocked))
            self.assertEqual(blocked.results[0]["reason_code"], "DATASET_PROVENANCE_INVALID")

            manual = {
                "proposal_id": plan.hypothesis_id,
                "statement": "An ordinary manually submitted proposal remains compatible.",
                "source": "offline legacy fixture",
                "tests": ["chronological validation"],
                "dataset_version": plan.dataset_version,
                "time_split": "train-validation-holdout",
                "paper_only": True,
                "experiment_plan": plan.as_dict(),
            }
            processor.bus.submit_hypothesis(
                manual,
                dedupe_key="manual-compatible",
                available_at=T0,
            )
            compatible = processor.process_pending(now=T0)
            self.assertEqual(compatible.failed, 0, repr(compatible))
            self.assertNotIn(
                compatible.results[0].get("reason_code"),
                {"DATASET_PROVENANCE_INVALID", "DATASET_ATTESTATION_MISSING", "DATASET_ATTESTATION_STALE"},
            )

    def test_predeclared_seed_missing_attestation_blocks_without_enqueue(self) -> None:
        with AxiomStore(":memory:") as store:
            _seed_predeclared_historical_dataset(store, attested=False)
            processor = AutonomousResearchProcessor(store, clock=lambda: T0)

            self.assertEqual(processor._enqueue_predeclared_from_persisted_scope(T0), ())
            self.assertEqual(store.research_queue_stats().get("total"), 0)
            reports = store.list_reports()
            self.assertEqual(len(reports), 1)
            evidence = reports[0]["report"]
            self.assertEqual(evidence["report_type"], "autonomous_predeclared_seed_blocked")
            self.assertEqual(evidence["blocker"], "DATASET_ATTESTATION_MISSING")
            self.assertEqual(evidence["queue_items_enqueued"], 0)
            self.assertEqual(
                evidence["next_action"],
                "PERSIST_CURRENT_HISTORICAL_DATASET_ATTESTATION",
            )

    def test_predeclared_seed_does_not_fall_back_to_unrelated_prediction_dataset(self) -> None:
        with AxiomStore(":memory:") as store:
            store.save_dataset_catalog(
                "unrelated-history",
                "history-v1",
                provider="fixture",
                instrument="POLYMARKET",
                market_type="prediction",
                timeframe="event",
                row_count=100,
                completeness=1.0,
                missing_ranges=(),
                source_type="HISTORICAL",
                snapshot_id="snapshot-v1",
            )
            processor = AutonomousResearchProcessor(store, clock=lambda: T0)
            cycle = processor.process_pending(now=T0)
            self.assertEqual(cycle.claimed, 0)
            self.assertEqual(store.research_queue_stats().get("total"), 0)
    def test_legacy_recovery_rejects_forward_source_selector_without_enqueue(self) -> None:
        with AxiomStore(":memory:") as store:
            _seed_predeclared_historical_dataset(store)
            predecessor = _legacy_prediction_predecessor(
                selector={"source_type": "FORWARD_COLLECTED"},
                candidate_id="legacy-forward-selector",
            )
            store.save_candidate_lifecycle("legacy-forward-selector", CandidateStage.IDEA.value, predecessor)
            store.save_candidate_lifecycle("legacy-forward-selector", CandidateStage.FROZEN.value, predecessor)
            processor = AutonomousResearchProcessor(store, clock=lambda: T0)

            recovery = processor._recover_legacy_predecessors(T0)
            self.assertEqual(len(recovery), 1)
            self.assertEqual(recovery[0]["progress"], "BLOCKED")
            self.assertEqual(recovery[0]["blocker"], "DATASET_PROVENANCE_INVALID")
            self.assertEqual(store.research_queue_stats().get("total"), 0)
            self.assertTrue(recovery[0]["provenance_state_digest"].startswith("sha256:"))
            self.assertEqual(
                len(store.list_reports(experiment_id="legacy-forward-selector")),
                1,
            )
    def test_legacy_recovery_rule_only_nested_target_is_not_unambiguous(self) -> None:
        with AxiomStore(":memory:") as store:
            _seed_predeclared_historical_dataset(store)
            predecessor = _legacy_prediction_predecessor(candidate_id="legacy-rule-only-target")
            plan = dict(predecessor["experiment_plan"])
            plan["target"] = {"filters": {"category": ["politics"]}}
            predecessor["experiment_plan"] = plan
            store.save_candidate_lifecycle("legacy-rule-only-target", CandidateStage.IDEA.value, predecessor)
            store.save_candidate_lifecycle("legacy-rule-only-target", CandidateStage.FROZEN.value, predecessor)
            processor = AutonomousResearchProcessor(store, clock=lambda: T0)

            recovery = processor._recover_legacy_predecessors(T0)
            self.assertEqual(len(recovery), 1)
            self.assertEqual(recovery[0]["classification"], "INVALID")
            self.assertEqual(recovery[0]["reason_code"], "MISSING_NESTED_TARGET_MARKET_IDS")
            self.assertEqual(recovery[0]["progress"], "BLOCKED")
            self.assertEqual(store.research_queue_stats().get("total"), 0)


    def test_legacy_recovery_state_changes_append_and_identical_retry_dedupes(self) -> None:
        with AxiomStore(":memory:") as store:
            _seed_predeclared_historical_dataset(store)
            predecessor = _legacy_prediction_predecessor(candidate_id="legacy-state-transition")
            store.save_candidate_lifecycle("legacy-state-transition", CandidateStage.IDEA.value, predecessor)
            store.save_candidate_lifecycle("legacy-state-transition", CandidateStage.FROZEN.value, predecessor)
            store.connection.execute(
                "UPDATE dataset_integrity_attestation "
                "SET status='STALE', reason='BOUNDS_MISMATCH' "
                "WHERE dataset_id=? AND dataset_version=?",
                ("Polymarket-historical", "history-v1"),
            )
            processor = AutonomousResearchProcessor(store, clock=lambda: T0)

            first = processor._recover_legacy_predecessors(T0)[0]
            self.assertEqual(first["blocker"], "DATASET_ATTESTATION_STALE")
            first_digest = first["provenance_state_digest"]
            self.assertEqual(
                len(store.list_reports(experiment_id="legacy-state-transition")),
                1,
            )
            repeat = processor._recover_legacy_predecessors(T0)[0]
            self.assertEqual(repeat["provenance_state_digest"], first_digest)
            self.assertEqual(
                len(store.list_reports(experiment_id="legacy-state-transition")),
                1,
            )

            store.connection.execute(
                "UPDATE dataset_integrity_attestation "
                "SET status='CURRENT', contamination_result='FAIL', reason='FORWARD_CONTAMINATION' "
                "WHERE dataset_id=? AND dataset_version=?",
                ("Polymarket-historical", "history-v1"),
            )
            changed = processor._recover_legacy_predecessors(T0)[0]
            self.assertEqual(changed["blocker"], "DATASET_PROVENANCE_INVALID")
            self.assertNotEqual(changed["provenance_state_digest"], first_digest)
            history = store.list_reports(
                experiment_id="legacy-state-transition",
                newest_first=True,
            )
            self.assertEqual(len(history), 2)
            self.assertEqual(history[0]["report"]["blocker"], "DATASET_PROVENANCE_INVALID")
            self.assertEqual(history[1]["report"]["blocker"], "DATASET_ATTESTATION_STALE")

    def test_predeclared_seed_rejects_non_polymarket_catalog_instrument(self) -> None:
        with AxiomStore(":memory:") as store:
            _seed_predeclared_historical_dataset(store)
            store.connection.execute(
                "UPDATE dataset_catalog SET instrument=? "
                "WHERE dataset_id=? AND dataset_version=?",
                ("BINANCE", "Polymarket-historical", "history-v1"),
            )
            processor = AutonomousResearchProcessor(store, clock=lambda: T0)

            self.assertEqual(processor._enqueue_predeclared_from_persisted_scope(T0), ())
            self.assertEqual(store.research_queue_stats().get("total"), 0)
            evidence = store.list_reports()[0]["report"]
            self.assertEqual(evidence["blocker"], "DATASET_CATALOG_INVALID")
            self.assertIn("instrument", evidence["reason"])

    def test_predeclared_seed_rejects_mutable_catalog_version_alias(self) -> None:
        with AxiomStore(":memory:") as store:
            _seed_predeclared_historical_dataset(store)
            store.connection.execute(
                "UPDATE dataset_catalog SET dataset_version=? "
                "WHERE dataset_id=? AND dataset_version=?",
                ("latest", "Polymarket-historical", "history-v1"),
            )
            processor = AutonomousResearchProcessor(store, clock=lambda: T0)

            self.assertEqual(processor._enqueue_predeclared_from_persisted_scope(T0), ())
            self.assertEqual(store.research_queue_stats().get("total"), 0)
            evidence = store.list_reports()[0]["report"]
            self.assertEqual(evidence["blocker"], "DATASET_CATALOG_INVALID")
            self.assertIn("mutable alias", evidence["reason"])

    def test_predeclared_starting_set_uses_price_causal_trials_and_excludes_control(self) -> None:
        starters = AutonomousResearchProcessor.predeclared_starting_set()
        self.assertEqual([item["template"] for item in starters], ["momentum", "mean_reversion", "probability_mispricing"])
        self.assertEqual(starters[0]["parameters"], {"lookback": (1,), "threshold": (0.05,)})
        self.assertEqual(starters[1]["parameters"], {"lookback": (1,), "threshold": (0.05,)})
        control = starters[2]
        self.assertEqual(control["metadata"]["research_role"], "ZERO_EDGE_CONTROL")
        self.assertTrue(control["metadata"]["selection_excluded"])
        self.assertTrue(control["metadata"]["proven_zero_edge"])
        self.assertEqual(control["model_document"], {"field": "yes_mid"})

    def test_backtest_uses_normalized_costs_and_structured_exit_contract(self) -> None:
        plan = ExperimentPlan.from_mapping(
            {
                "market_type": "prediction",
                "template": "momentum",
                "allowed_features": ["timestamp", "market_id", "yes_mid"],
                "parameters": {"lookback": [1], "threshold": [0.05]},
                "market_scope": {
                    "mode": "RULE_BASED_MARKETS",
                    "instrument": "POLYMARKET",
                    "provenance": "canonical",
                },
                "dataset_selector": {"dataset_id": "dataset", "dataset_version": "v1"},
                "methodology": {"initial_cash": 1000.0, "allocation": 0.5},
                "assumptions": {
                    "version": "price-proxy-v1",
                    "fee_bps": 12.0,
                    "slippage_bps": 7.0,
                },
                "exit_policy": {"type": "fixed_holding_period", "holding_period": 3},
                "max_variants": 1,
                "min_samples": 1,
                "paper_only": True,
            },
            hypothesis_id="structured-contract",
        )
        strategy = plan.strategy_for(plan.variants()[0], "structured-contract-candidate")
        rows = [
            {
                "timestamp": f"2025-01-01T00:0{index}:00+00:00",
                "market_id": "market-1",
                "yes_mid": price,
                "yes_ask": min(0.99, price + 0.01),
                "yes_bid": max(0.01, price - 0.01),
                "no_ask": min(0.99, 1.0 - price + 0.01),
                "no_bid": max(0.01, 1.0 - price - 0.01),
            }
            for index, price in enumerate((0.40, 0.50, 0.45, 0.55))
        ]
        summary = AutonomousResearchProcessor._run_backtest(plan, strategy, rows)
        self.assertEqual(summary["assumption_version"], "price-proxy-v1")
        self.assertEqual(summary["cost_assumptions"], {"fee_bps": 12.0, "slippage_bps": 7.0})
        self.assertEqual(summary["exit_policy"], {"type": "fixed_holding_period", "holding_period": 3})

    def test_all_declared_trials_are_manifested_and_holdout_cannot_select(self) -> None:
        plan = ExperimentPlan.from_proposal(
            {
                "proposal_id": "two-trial-plan",
                "market_type": "prediction",
                "template": "probability_mispricing",
                "parameters": {"threshold": [0.03, 0.05]},
                "dataset_id": "dataset",
                "dataset_version": "v1",
                "market_scope": {
                    "mode": "RULE_BASED_MARKETS",
                    "instrument": "POLYMARKET",
                    "filters": {"category": "politics"},
                    "provenance": "canonical",
                },
                "time_split": "train-validation-holdout",
                "min_samples": 1,
                "paper_only": True,
            }
        )
        processor = object.__new__(AutonomousResearchProcessor)
        results = (
            {
                "candidate_id": "trial-a",
                "stage": "PAPER_FORWARD",
                "validation_expectancy": 0.1,
                "holdout_evaluated": False,
                "holdout_used_for_selection": False,
            },
            {
                "candidate_id": "trial-b",
                "stage": "PAPER_FORWARD",
                "validation_expectancy": 0.2,
                "holdout_evaluated": False,
                "holdout_used_for_selection": False,
            },
        )
        summary = processor._hypothesis_result(plan, results, ())
        self.assertEqual(summary["variants_tested"], 2)
        self.assertEqual(len(summary["trial_manifest"]), 2)
        self.assertEqual(summary["selected_candidate_ids"], ["trial-a", "trial-b"])
        self.assertTrue(summary["holdout_locked"])
        self.assertFalse(summary["holdout_evaluated"])
        self.assertEqual(summary["holdout_selection_fence"], "validation_only")
        self.assertTrue(all(not item["holdout_used_for_selection"] for item in summary["trial_manifest"]))

    def test_research_results_are_paper_only_before_any_live_control(self) -> None:
        plan = ExperimentPlan.from_proposal(
            {
                "proposal_id": "paper-first",
                "market_type": "prediction",
                "template": "probability_mispricing",
                "parameters": {"threshold": [0.05]},
                "dataset_id": "dataset",
                "dataset_version": "v1",
                "market_scope": {
                    "mode": "RULE_BASED_MARKETS",
                    "instrument": "POLYMARKET",
                    "filters": {"category": "politics"},
                    "provenance": "canonical",
                },
                "time_split": "train-validation-holdout",
                "min_samples": 1,
                "paper_only": True,
            }
        )
        processor = object.__new__(AutonomousResearchProcessor)
        summary = processor._hypothesis_result(
            plan,
            (
                {
                    "candidate_id": "paper-candidate",
                    "stage": "PAPER_FORWARD",
                    "holdout_evaluated": False,
                    "holdout_used_for_selection": False,
                },
            ),
            (),
        )
        self.assertTrue(summary["paper_only"])
        self.assertEqual(summary["next_action"], "AWAIT_PAPER_EVIDENCE")
        self.assertEqual(summary["candidate_results"][0]["stage"], "PAPER_FORWARD")

    def test_successor_rejects_mutated_immutable_predecessor_hash(self) -> None:
        components = {"strategy_hash": "strategy-v1", "model_hash": "model-v1", "config_hash": "config-v1"}
        frozen_hash = hashlib.sha256("|".join(components.values()).encode()).hexdigest()
        document = {
            "candidate_id": "legacy-polymarket",
            "frozen_hash": frozen_hash,
            "immutable_hashes": components,
            "market_type": "prediction",
            "target": {"market_ids": ["market-1"]},
            "dataset_id": "prediction:market-1",
            "dataset_version": "v1",
            "template": "probability_mispricing",
            "parameters": {"threshold": [0.05]},
            "time_split": "train-validation-holdout",
            "research_mode": "PRICE_PROXY_RESEARCH",
            "assumptions": {
                "version": "price-proxy-v1",
                "fee_bps": 10.0,
                "slippage_bps": 5.0,
            },
            "exit_policy": {"type": "fixed_holding_period", "holding_period": 2},
            "trial_budget": {"limit": 1},
            "paper_only": True,
        }
        successor = create_legacy_successor(document)
        self.assertEqual(successor.plan.research_mode, "PRICE_PROXY_RESEARCH")
        self.assertEqual(successor.plan.assumptions["fee_bps"], 10.0)
        self.assertEqual(successor.plan.assumptions["slippage_bps"], 5.0)
        self.assertEqual(successor.plan.exit_policy["holding_period"], 2)
        self.assertEqual(successor.plan.trial_budget["limit"], 1)
        self.assertEqual(successor.predecessor_frozen_hash, frozen_hash)
        mutated = dict(document)
        mutated["immutable_hashes"] = {**components, "model_hash": "mutated"}
        with self.assertRaisesRegex(LegacyScopeError, "PREDECESSOR_HASH_MISMATCH"):
            create_legacy_successor(mutated)

    def test_insufficient_history_still_collects_bounded_paper_observations(self) -> None:
        with AxiomStore(":memory:") as store:
            store.save_dataset(
                "insufficient-history",
                "v1",
                [
                    {
                        "timestamp": T0.isoformat(),
                        "market_id": "market-1",
                        "yes_mid": 0.50,
                        "yes_bid": 0.49,
                        "yes_ask": 0.51,
                        "no_mid": 0.50,
                        "no_bid": 0.49,
                        "no_ask": 0.51,
                        "settlement": "resolved_yes",
                    }
                ],
                metadata={
                    "source_type": "HISTORICAL",
                    "provider": "SYNTHETIC_OFFLINE",
                    "instrument": "POLYMARKET",
                    "research_quality": "PRICE_PROXY",
                },
                quality="PRICE_PROXY",
            )
            store.save_dataset_catalog(
                "insufficient-history",
                "v1",
                provider="SYNTHETIC_OFFLINE",
                instrument="POLYMARKET",
                market_type=MarketType.PREDICTION,
                timeframe="event",
                start_timestamp=T0,
                end_timestamp=T0,
                row_count=1,
                completeness=1.0,
                missing_ranges=(),
                quality="PRICE_PROXY",
                source_type="HISTORICAL",
                snapshot_id="SYNTHETIC_OFFLINE-insufficient-history-v1",
                metadata={
                    "provider": "SYNTHETIC_OFFLINE",
                    "source_type": "HISTORICAL",
                    "instrument": "POLYMARKET",
                    "research_quality": "PRICE_PROXY",
                },
            )
            store.verify_dataset_integrity_attestation(
                "insufficient-history",
                "v1",
                force=True,
            )
            bus = DurableResearchBus(store)
            plan = {
                "market_type": "prediction",
                "template": "probability_mispricing",
                "dataset_id": "insufficient-history",
                "dataset_version": "v1",
                "model_document": {"field": "yes_mid"},
                "market_scope": {
                    "schema_version": "1",
                    "mode": "EXACT_MARKETS",
                    "instrument": "POLYMARKET",
                    "market_ids": ["market-1"],
                    "categories": ["politics"],
                    "filters": {},
                    "regime_restrictions": {},
                    "provenance": "canonical",
                },
                "parameters": {"threshold": [0.05]},
                "time_split": "train-validation-holdout",
                "min_samples": 5,
                "min_trades": 0,
                "max_variants": 1,
                "paper_only": True,
            }
            bus.submit_hypothesis(
                {
                    "proposal_id": "insufficient-history-observation",
                    "statement": "bounded observation remains useful before historical sufficiency",
                    "source": "fixture",
                    "tests": ["chronological validation"],
                    "dataset_version": "v1",
                    "time_split": "train-validation-holdout",
                    "paper_only": True,
                    "experiment_plan": plan,
                },
                available_at=T0,
                dedupe_key="insufficient-history-observation",
            )
            processor = AutonomousResearchProcessor(
                store,
                bus=bus,
                config=AutonomousResearchConfig(max_items_per_cycle=1),
                clock=lambda: T0,
            )
            cycle = processor.process_pending(now=T0)
            self.assertEqual(cycle.failed, 0, repr(cycle))
            lifecycle = store.load_candidate_lifecycle(limit=None)
            self.assertEqual(len(lifecycle), 1, repr(cycle))
            candidate = lifecycle[0]
            self.assertEqual(candidate["stage"], "SCHEMA_VALIDATED")
            self.assertNotIn("forward_test_id", candidate["payload"])
            self.assertIn("dataset_attestation", candidate["payload"])

            market = PredictionMarketSnapshot(
                timestamp=T0,
                market_id="market-1",
                question="Will the public event resolve YES?",
                yes_bid=0.49,
                yes_ask=0.51,
                yes_mid=0.50,
                no_bid=0.49,
                no_ask=0.51,
                no_mid=0.50,
                expiry=T0 + timedelta(days=1),
                settlement=SettlementState.OPEN,
                category="politics",
                order_book=OrderBookSnapshot(
                    timestamp=T0,
                    bids=(OrderBookLevel(0.49, 100.0),),
                    asks=(OrderBookLevel(0.51, 100.0),),
                    token_id="yes-market-1",
                    condition_id="condition-1",
                    provider_timestamp=T0,
                    source="SYNTHETIC_OFFLINE",
                ),
                source="SYNTHETIC_OFFLINE",
                yes_token_id="yes-market-1",
                no_token_id="no-market-1",
                condition_id="condition-1",
                provider_timestamp=T0,
                active=True,
                closed=False,
                accepting_orders=True,
                enable_order_book=True,
            )
            provider = InMemoryPredictionProvider(markets=(market,))
            collector = PolymarketCollector(
                provider,
                store,
                CollectorConfig(max_markets=1, discovery_budget_per_cycle=1),
                clock=lambda: T0,
                sleep=lambda _seconds: None,
            )
            collection = collector.collect_once(now=T0)
            self.assertEqual(collection.errors, 0, repr(collection))
            candidate_id = str(candidate["candidate_id"])
            spec = ForwardTestRegistry(store).get("forward-" + candidate_id)
            self.assertIsNotNone(spec)
            self.assertNotIn("dataset_attestation", spec.config)
            self.assertEqual(spec.experiment_id, "forward-" + candidate_id)
            self.assertEqual(tuple(spec.allowed_markets), ("market-1",))
            self.assertTrue(spec.config["market_authority_required"])
            assert spec is not None
            cycle = run_forward_paper(
                spec,
                store=store,
                strategy=load_strategy(spec.config["strategy_document"]),
                model=spec.config["model_document"],
                observations=[
                    {
                        "market_id": "market-1",
                        "timestamp": T0.isoformat(),
                        "yes_mid": 0.50,
                        "yes_bid": 0.49,
                        "yes_ask": 0.51,
                        "no_mid": 0.50,
                        "no_bid": 0.49,
                        "no_ask": 0.51,
                        "model_probability": 0.80,
                        "settlement": "resolved_yes",
                    }
                ],
                now=T0,
            )
            self.assertGreaterEqual(cycle.observations_processed, 1)
            self.assertTrue(store.list_paper_observations(spec.experiment_id))
            first = processor.reevaluate_forward_candidates(now=T0 + timedelta(minutes=1))
            self.assertEqual(len(first), 1, repr(first))
            self.assertTrue(first[0]["reassessed"])
            reassessed = store.load_candidate_lifecycle(candidate_id)
            self.assertEqual(reassessed["stage"], CandidateStage.PAPER_FORWARD.value)
            self.assertIn("dataset_attestation", reassessed["payload"])
            self.assertEqual(
                _canonical_binding(reassessed["payload"]["dataset_attestation"]),
                _canonical_binding(
                    store.load_dataset_integrity_attestation("insufficient-history", "v1")
                ),
            )
            self.assertNotIn("dataset_attestation", reassessed["payload"]["forward_config"])
            first_identity = reassessed["payload"]["forward_evidence_identity"]
            unchanged = processor.reevaluate_forward_candidates(now=T0 + timedelta(minutes=1))
            self.assertFalse(any(item.get("reassessed") for item in unchanged))
            self.assertEqual(
                store.load_candidate_lifecycle(candidate_id)["payload"]["forward_evidence_identity"],
                first_identity,
            )
            fill_before = len(store.list_candidate_lifecycle_events(candidate_id))
            store.save_fill(
                Fill(
                    timestamp=T0 + timedelta(minutes=2),
                    market_type=MarketType.PREDICTION,
                    symbol="market-1",
                    side=Side.BUY,
                    quantity=1.0,
                    price=0.51,
                    fees=0.001,
                    slippage=0.001,
                    strategy_id=spec.strategy_hash,
                    order_id="manual-fill",
                    market_id="market-1",
                    expected_probability=0.80,
                    metadata={
                        "paper_experiment_id": spec.experiment_id,
                        "execution_status": "FULL_FILL",
                        "requested_quantity": 1.0,
                    },
                ),
                fill_id="paper-fill-manual-fill",
            )
            fill_changed = processor.reevaluate_forward_candidates(now=T0 + timedelta(minutes=2))
            self.assertEqual(len(fill_changed), 1, repr(fill_changed))
            fill_identity = store.load_candidate_lifecycle(candidate_id)["payload"]["forward_evidence_identity"]
            self.assertNotEqual(fill_identity, first_identity)
            fill_events = len(store.list_candidate_lifecycle_events(candidate_id))
            self.assertEqual(fill_events, fill_before + 1)
            fill_unchanged = processor.reevaluate_forward_candidates(now=T0 + timedelta(minutes=2))
            self.assertFalse(any(item.get("reassessed") for item in fill_unchanged))
            self.assertEqual(
                len(store.list_candidate_lifecycle_events(candidate_id)),
                fill_events,
            )
            self.assertEqual(
                store.load_candidate_lifecycle(candidate_id)["payload"]["forward_evidence_identity"],
                fill_identity,
            )
            store.save_paper_observation(
                "manual-new-observation",
                spec.experiment_id,
                "market-1",
                T0 + timedelta(minutes=2),
                {
                    "market_id": "market-1",
                    "timestamp": (T0 + timedelta(minutes=2)).isoformat(),
                    "yes_mid": 0.50,
                    "model_probability": 0.80,
                    "settlement": "open",
                },
            )
            changed = processor.reevaluate_forward_candidates(now=T0 + timedelta(minutes=2))
            self.assertEqual(len(changed), 1, repr(changed))
            self.assertNotEqual(
                store.load_candidate_lifecycle(candidate_id)["payload"]["forward_evidence_identity"],
                first_identity,
            )

    def test_recorded_replay_partitions_rows_by_each_candidate_scope(self) -> None:
        plan_a = _recorded_replay_plan("plan-replay-a", "market-a")
        plan_b = _recorded_replay_plan("plan-replay-b", "market-b")
        candidate_a = _recorded_replay_candidate("candidate-a", plan_a)
        candidate_b = _recorded_replay_candidate("candidate-b", plan_b)
        resolutions = {
            "candidate-a": {
                "candidate_id": "candidate-a",
                "scope_hash": plan_a.market_scope_hash,
                "scope_version": plan_a.market_scope_version,
                "status": "MATCHED",
                "reason": "MATCHED",
                "policy": plan_a.market_scope.as_dict(),
                "matched_markets": [{"market_id": "market-a"}],
                "resolution_id": "resolution-a",
            },
            "candidate-b": {
                "candidate_id": "candidate-b",
                "scope_hash": plan_b.market_scope_hash,
                "scope_version": plan_b.market_scope_version,
                "status": "MATCHED",
                "reason": "MATCHED",
                "policy": plan_b.market_scope.as_dict(),
                "matched_markets": [{"market_id": "market-b"}],
                "resolution_id": "resolution-b",
            },
        }
        store = _RecordedReplayStore(
            {plan_a.plan_id: plan_a, plan_b.plan_id: plan_b},
            resolutions,
        )
        export = {
            "forward_rows": [
                _recorded_replay_record("market-a"),
                _recorded_replay_record("market-b"),
            ]
        }
        from axiom.backtest.prediction import run_prediction_research_mode as canonical_replay

        received: dict[str, tuple[str, ...]] = {}

        def capture(rows, strategy, **kwargs):
            received[strategy.id] = tuple(str(row["market_id"]) for row in rows)
            return canonical_replay(rows, strategy, **kwargs)

        with patch("axiom.backtest.prediction.run_prediction_research_mode", side_effect=capture):
            replay = _run_recorded_book_replay(store, export, [candidate_a, candidate_b])

        self.assertEqual(replay["status"], "PASS")
        self.assertEqual(received, {"candidate-a": ("market-a",), "candidate-b": ("market-b",)})
        trials = {str(item["candidate_id"]): item for item in replay["trials"]}
        self.assertEqual(trials["candidate-a"]["included_market_ids"], ["market-a"])
        self.assertEqual(trials["candidate-b"]["included_market_ids"], ["market-b"])
        self.assertEqual(trials["candidate-a"]["excluded_row_count"], 1)
        self.assertEqual(trials["candidate-b"]["excluded_row_count"], 1)
        self.assertEqual(trials["candidate-a"]["scope_hash"], plan_a.market_scope_hash)
        self.assertEqual(trials["candidate-b"]["scope_version"], plan_b.market_scope_version)
        self.assertEqual(trials["candidate-a"]["plan_hash"], plan_a.plan_hash)
        self.assertEqual(trials["candidate-b"]["plan_hash"], plan_b.plan_hash)

    def test_recorded_replay_blocks_mismatched_embedded_plan_id(self) -> None:
        plan = _recorded_replay_plan("plan-replay-embedded-id", "market-a")
        candidate = _recorded_replay_candidate("candidate-embedded-id", plan)
        payload = dict(candidate["payload"])
        embedded_plan = dict(payload["experiment_plan"])
        embedded_plan["plan_id"] = "another-plan"
        payload["experiment_plan"] = embedded_plan
        candidate["payload"] = payload
        store = _RecordedReplayStore(
            {plan.plan_id: plan},
            {"candidate-embedded-id": _recorded_replay_resolution("candidate-embedded-id", plan)},
        )

        replay = _run_recorded_book_replay(
            store,
            {"forward_rows": [_recorded_replay_record("market-a")]},
            [candidate],
        )

        self.assertEqual(replay["status"], "BLOCKED")
        trial = replay["trials"][0]
        self.assertEqual(trial["status"], "BLOCKED")
        self.assertIn("plan_id", str(trial["reason"]).lower())

    def test_recorded_replay_blocks_incomplete_exact_matched_resolution(self) -> None:
        base_plan = _recorded_replay_plan("plan-replay-incomplete-exact", "market-a")
        scope = dict(base_plan.market_scope.as_dict())
        scope["market_ids"] = ["market-a", "market-b"]
        plan_document = base_plan.as_dict()
        for legacy_key in (
            "target",
            "market_ids",
            "target_market_ids",
            "target_instrument",
            "instrument",
            "categories",
            "filters",
            "regime_restrictions",
            "market_scope_hash",
        ):
            plan_document.pop(legacy_key, None)
        plan_document["market_scope"] = scope
        plan = ExperimentPlan.from_mapping(
            plan_document,
            hypothesis_id=base_plan.hypothesis_id,
        )
        candidate = _recorded_replay_candidate("candidate-incomplete-exact", plan)
        store = _RecordedReplayStore(
            {plan.plan_id: plan},
            {
                "candidate-incomplete-exact": _recorded_replay_resolution(
                    "candidate-incomplete-exact",
                    plan,
                    matched_ids=("market-a",),
                )
            },
        )

        replay = _run_recorded_book_replay(
            store,
            {
                "forward_rows": [
                    _recorded_replay_record("market-a"),
                    _recorded_replay_record("market-b"),
                ]
            },
            [candidate],
        )

        self.assertEqual(replay["status"], "BLOCKED")
        trial = replay["trials"][0]
        self.assertEqual(trial["status"], "BLOCKED")
        self.assertEqual(trial["included_market_ids"], [])
        self.assertIn("incomplete", str(trial["reason"]).lower())

    def test_recorded_replay_preserves_scope_evidence_when_evaluator_fails(self) -> None:
        plan = _recorded_replay_plan("plan-replay-evaluator-failure", "market-a")
        candidate = _recorded_replay_candidate("candidate-evaluator-failure", plan)
        store = _RecordedReplayStore(
            {plan.plan_id: plan},
            {
                "candidate-evaluator-failure": _recorded_replay_resolution(
                    "candidate-evaluator-failure",
                    plan,
                )
            },
        )

        with patch(
            "axiom.backtest.prediction.run_prediction_research_mode",
            side_effect=RuntimeError("evaluator failed"),
        ):
            replay = _run_recorded_book_replay(
                store,
                {
                    "forward_rows": [
                        _recorded_replay_record("market-a"),
                        _recorded_replay_record("market-b"),
                    ]
                },
                [candidate],
            )

        self.assertEqual(replay["status"], "BLOCKED")
        trial = replay["trials"][0]
        self.assertEqual(trial["status"], "BLOCKED")
        self.assertEqual(trial["included_market_ids"], ["market-a"])
        self.assertEqual(trial["included_row_count"], 1)
        self.assertEqual(trial["excluded_row_count"], 1)
        self.assertIn("evaluator failed", str(trial["reason"]))

    def test_recorded_replay_blocks_malformed_or_unresolved_scope(self) -> None:
        plan_a = _recorded_replay_plan("plan-replay-malformed", "market-a")
        plan_b = _recorded_replay_plan("plan-replay-unresolved", "market-b")
        malformed = _recorded_replay_candidate("candidate-malformed", plan_a)
        malformed_payload = dict(malformed["payload"])
        malformed_payload["market_scope"] = {"schema_version": "1", "mode": "EXACT_MARKETS"}
        malformed["payload"] = malformed_payload
        unresolved = _recorded_replay_candidate("candidate-unresolved", plan_b)
        store = _RecordedReplayStore(
            {plan_a.plan_id: plan_a, plan_b.plan_id: plan_b},
            {},
        )
        replay = _run_recorded_book_replay(
            store,
            {"forward_rows": [_recorded_replay_record("market-a")]},
            [malformed, unresolved],
        )

        self.assertEqual(replay["status"], "BLOCKED")
        self.assertEqual(replay["blocked_candidate_count"], 2)
        trials = {str(item["candidate_id"]): item for item in replay["trials"]}
        self.assertTrue(all(item["status"] == "BLOCKED" for item in trials.values()))
        self.assertEqual(trials["candidate-malformed"]["included_market_ids"], [])
        self.assertEqual(trials["candidate-unresolved"]["included_market_ids"], [])
        self.assertEqual(trials["candidate-malformed"]["excluded_row_count"], 1)
        self.assertEqual(trials["candidate-unresolved"]["excluded_row_count"], 1)
        self.assertIn("malformed", str(trials["candidate-malformed"]["reason"]).lower())
        self.assertIn("missing", str(trials["candidate-unresolved"]["reason"]).lower())



if __name__ == "__main__":
    unittest.main()
