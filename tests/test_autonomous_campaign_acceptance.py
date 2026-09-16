from __future__ import annotations

from datetime import datetime, timedelta, timezone
from collections.abc import Mapping
import json
import unittest
from unittest.mock import patch

from axiom.autonomous import (
    AutonomousResearchError,
    AutonomousResearchProcessor,
    _hash_document,
)
from axiom.storage import AxiomStore


UTC = timezone.utc
T0 = datetime(2025, 1, 1, tzinfo=UTC)
SYNTHETIC_LABEL = "SYNTHETIC_CAMPAIGN_ACCEPTANCE"
DATASET_ID = "synthetic-polymarket-campaign-history"


def _rows(*, midpoint: float) -> list[dict[str, object]]:
    return [
        {
            "fixture_label": SYNTHETIC_LABEL,
            "synthetic": True,
            "timestamp": T0.isoformat(),
            "market_id": "synthetic-market-1",
            "yes_mid": midpoint,
            "yes_bid": midpoint - 0.01,
            "yes_ask": midpoint + 0.01,
            "settlement": "open",
            "source_snapshot_id": f"{SYNTHETIC_LABEL}:snapshot:{midpoint}",
        }
    ]


def _large_rows(count: int = 18_141) -> list[dict[str, object]]:
    return [
        {
            "fixture_label": SYNTHETIC_LABEL,
            "synthetic": True,
            "timestamp": (T0 + timedelta(seconds=index)).isoformat(),
            "market_id": f"large-market-{index:05d}",
            "yes_mid": 0.50 + (index % 10) / 1_000,
            "yes_bid": 0.49 + (index % 10) / 1_000,
            "yes_ask": 0.51 + (index % 10) / 1_000,
            "settlement": "open",
            "source_snapshot_id": f"{SYNTHETIC_LABEL}:large:{index}",
        }
        for index in range(count)
    ]


def _save_attested_dataset(store: AxiomStore, version: str, *, midpoint: float) -> dict[str, object]:
    rows = _rows(midpoint=midpoint)
    metadata = {
        "fixture_label": SYNTHETIC_LABEL,
        "synthetic": True,
        "provider": SYNTHETIC_LABEL,
        "source_type": "HISTORICAL",
        "market_type": "prediction",
        "instrument": "POLYMARKET",
        "research_quality": "PRICE_PROXY",
    }
    store.save_dataset(DATASET_ID, version, rows, metadata=metadata, quality="PRICE_PROXY")
    store.save_dataset_catalog(
        DATASET_ID,
        version,
        provider=SYNTHETIC_LABEL,
        instrument="POLYMARKET",
        market_type="prediction",
        timeframe="event",
        start_timestamp=T0,
        end_timestamp=T0,
        row_count=len(rows),
        completeness=1.0,
        quality="PRICE_PROXY",
        source_type="HISTORICAL",
        snapshot_id=f"{SYNTHETIC_LABEL}:catalog:{version}",
        metadata=metadata,
    )
    attestation = store.verify_dataset_integrity_attestation(DATASET_ID, version, force=True)
    assert attestation["status"] == "CURRENT"
    assert attestation["contamination_result"] == "PASS"
    return attestation
def _save_catalog_attested_dataset(
    store: AxiomStore,
    version: str,
    rows: list[dict[str, object]],
) -> dict[str, object]:
    metadata = {
        "fixture_label": SYNTHETIC_LABEL,
        "synthetic": True,
        "provider": SYNTHETIC_LABEL,
        "source_type": "HISTORICAL",
        "market_type": "prediction",
        "instrument": "POLYMARKET",
        "research_quality": "PRICE_PROXY",
    }
    stamps = [
        datetime.fromisoformat(
            str(row["timestamp"]).replace("Z", "+00:00")
        )
        for row in rows
        if row.get("timestamp") is not None
    ]
    store.save_dataset(
        DATASET_ID,
        version,
        rows,
        metadata=metadata,
        quality="PRICE_PROXY",
    )
    store.save_dataset_catalog(
        DATASET_ID,
        version,
        provider=SYNTHETIC_LABEL,
        instrument="POLYMARKET",
        market_type="prediction",
        timeframe="event",
        start_timestamp=min(stamps) if stamps else T0,
        end_timestamp=max(stamps) if stamps else T0,
        row_count=len(rows),
        completeness=1.0 if rows else 0.0,
        quality="PRICE_PROXY",
        source_type="HISTORICAL",
        snapshot_id=f"{SYNTHETIC_LABEL}:catalog:{version}",
        metadata=metadata,
    )
    attestation = store.verify_dataset_integrity_attestation(
        DATASET_ID,
        version,
        force=True,
    )
    assert attestation["status"] == "CURRENT"
    assert attestation["contamination_result"] == "PASS"
    return attestation


class AutonomousCampaignAcceptanceTests(unittest.TestCase):
    def test_large_campaign_uses_compact_locked_provenance(self) -> None:
        with AxiomStore(":memory:") as store:
            _save_catalog_attested_dataset(
                store,
                "large-v1",
                _large_rows(),
            )
            processor = AutonomousResearchProcessor(store, clock=lambda: T0)
            state = processor.start_polymarket_campaign(
                "large-compact-campaign",
                dataset_id=DATASET_ID,
                dataset_version="large-v1",
                now=T0,
            )
            queued = processor.bus.list_campaign_trials("large-compact-campaign", limit=10)
            self.assertEqual(len(queued), 1)
            plan = queued[0].payload["experiment_plan"]
            self.assertLess(len(json.dumps(plan, separators=(",", ":")).encode("utf-8")), 16_384)
            boundary = state["protocol"]["dataset_boundary"]
            self.assertEqual(boundary["row_count"], 18_141)
            self.assertTrue(boundary["ordered_row_manifest_digest"].startswith("sha256:"))
            self.assertNotIn("ordered_row_identities", boundary)
            self.assertNotIn("ordered_content_hashes", boundary)

    def test_v2_price_proxy_protocol_uses_only_present_required_features(self) -> None:
        from axiom.experiment_plan import ExperimentPlan
        rows = [
            {
                "timestamp": T0.isoformat(),
                "market_id": "historical-price-proxy-market",
                "price": 0.50,
            }
        ]
        with AxiomStore(":memory:") as store:
            _save_catalog_attested_dataset(store, "v2", rows)
            processor = AutonomousResearchProcessor(store, clock=lambda: T0)
            state = processor.start_polymarket_campaign(
                "polymarket-paper-campaign-v2:campaign",
                dataset_id=DATASET_ID,
                dataset_version="v2",
                now=T0,
            )
            self.assertEqual(state["schema_version"], "polymarket-finite-campaign-v2")
            self.assertEqual(
                state["protocol"]["required_features"],
                ["timestamp", "market_id", "yes_mid"],
            )
            queued = processor.bus.list_campaign_trials(
                "polymarket-paper-campaign-v2:campaign",
                limit=100,
            )
            self.assertEqual(len(queued), 1)
            plan_payload = queued[0].payload["experiment_plan"]
            self.assertEqual(
                plan_payload["allowed_features"],
                ["timestamp", "market_id", "yes_mid"],
            )
            plan = ExperimentPlan.from_mapping(
                plan_payload,
                hypothesis_id=queued[0].payload["hypothesis_id"],
            )
            loaded_rows, _ = processor._load_split(
                plan,
                boundary_override=state["protocol"]["dataset_boundary"],
            )
            self.assertEqual(loaded_rows[0]["yes_mid"], 0.50)
            self.assertNotIn("yes_bid", loaded_rows[0])
            self.assertNotIn("yes_ask", loaded_rows[0])

    def test_v2_default_selects_only_exact_operational_configurations(self) -> None:
        expected_ids = (
            "momentum:lookback-1:threshold-0.05",
            "mean_reversion:lookback-1:threshold-0.05",
        )
        with AxiomStore(":memory:") as store:
            _save_attested_dataset(store, "v2-default", midpoint=0.50)
            processor = AutonomousResearchProcessor(store, clock=lambda: T0)
            state = processor.start_polymarket_campaign(
                "polymarket-paper-campaign-v2:default",
                dataset_id=DATASET_ID,
                dataset_version="v2-default",
                now=T0,
            )
            self.assertEqual(
                [item["configuration_id"] for item in state["protocol"]["configuration_manifest"]],
                list(expected_ids),
            )
            self.assertEqual(
                [item["configuration_id"] for item in state["trials"]],
                list(expected_ids),
            )
            self.assertEqual(set(state["protocol"]["operational_setups"]), set(expected_ids))
            self.assertEqual(
                {
                    item.payload["campaign_configuration_id"]
                    for item in processor.bus.list_campaign_trials(
                        "polymarket-paper-campaign-v2:default",
                        limit=100,
                    )
                },
                {expected_ids[0]},
            )

    def test_v2_configuration_allowlist_is_exactly_two_operational_setups(self) -> None:
        selected_ids = (
            "momentum:lookback-1:threshold-0.05",
            "mean_reversion:lookback-1:threshold-0.05",
        )
        campaign_id = "polymarket-paper-campaign-v2:allowlisted"
        with AxiomStore(":memory:") as store:
            _save_attested_dataset(store, "allowlisted-v1", midpoint=0.50)
            processor = AutonomousResearchProcessor(store, clock=lambda: T0)
            state = processor.start_polymarket_campaign(
                campaign_id,
                dataset_id=DATASET_ID,
                dataset_version="allowlisted-v1",
                configuration_allowlist=selected_ids,
                now=T0,
            )

            self.assertEqual(state["fixed_configuration_count"], 2)
            self.assertEqual(
                [trial["configuration_id"] for trial in state["trials"]],
                list(selected_ids),
            )
            setups = state["protocol"]["operational_setups"]
            self.assertEqual(set(setups), set(selected_ids))
            for trial in state["trials"]:

                self.assertEqual(
                    trial["operational_setup"],
                    setups[trial["configuration_id"]]["operational_setup"],
                )
                self.assertEqual(
                    trial["operational_setup_hash"],
                    setups[trial["configuration_id"]]["operational_setup_hash"],
                )

            first = processor.bus.list_campaign_trials(campaign_id, limit=100)
            self.assertEqual(len(first), 1)
            self.assertEqual(first[0].payload["campaign_configuration_id"], selected_ids[0])
            self.assertEqual(
                first[0].payload["operational_setup_hash"],
                state["trials"][0]["operational_setup_hash"],
            )

            processor._advance_campaign_after_result(
                first[0],
                {
                    "accepted": False,
                    "reason_code": "NEGATIVE_VALIDATION_EXPECTANCY",
                    "stage": "REJECTED",
                    "candidate_id": "allowlisted-candidate-0",
                },
                T0 + timedelta(minutes=1),
            )
            second = processor.bus.list_campaign_trials(campaign_id, limit=100)
            self.assertEqual(
                {item.payload["campaign_configuration_id"] for item in second},
                set(selected_ids),
            )
            self.assertEqual(len(store.list_experiment_plans(limit=100)), 2)
 
    def test_v2_allowlist_rejects_generic_grid_configuration(self) -> None:
        with AxiomStore(":memory:") as store:
            processor = AutonomousResearchProcessor(store, clock=lambda: T0)
            with self.assertRaises(ValueError):
                processor.start_polymarket_campaign(
                    "polymarket-paper-campaign-v2:generic",
                    protocol_id="polymarket-paper-campaign-v2",
                    configuration_allowlist=("momentum:lookback-3:threshold-0.05",),
                    now=T0,
                )
            self.assertEqual(
                processor.bus.list_campaign_trials("polymarket-paper-campaign-v2:generic"),
                (),
            )

    def test_campaign_configuration_allowlist_rejects_invalid_inputs(self) -> None:
        with AxiomStore(":memory:") as store:
            processor = AutonomousResearchProcessor(store, clock=lambda: T0)
            known_id = processor.campaign_configurations()[0]["configuration_id"]
            grid_ids = [item["configuration_id"] for item in processor.campaign_configurations()]
            invalid_allowlists = (
                "not-a-sequence",
                {"configuration_ids": [known_id]},
                1,
                [""],
                [known_id, known_id],
                ["unknown:configuration"],
                [*grid_ids, known_id],
            )
            for index, allowlist in enumerate(invalid_allowlists):
                with self.assertRaises(ValueError):
                    processor.start_polymarket_campaign(
                        f"invalid-allowlist-{index}",
                        configuration_allowlist=allowlist,
                        now=T0,
                    )
                self.assertEqual(
                    processor.bus.list_campaign_trials(f"invalid-allowlist-{index}"),
                    (),
                )

    def test_v2_configuration_allowlist_applies_prior_filter_and_exhaustion(self) -> None:
        selected_ids = (
            "momentum:lookback-1:threshold-0.05",
            "mean_reversion:lookback-1:threshold-0.05",
        )
        with AxiomStore(":memory:") as store:
            _save_attested_dataset(store, "allowlisted-prior-v1", midpoint=0.50)
            store.save_experiment_plan(
                "allowlisted-prior-momentum",
                {
                    "template": "momentum",
                    "parameters": {"lookback": [1], "threshold": [0.05]},
                },
                hypothesis_id="allowlisted-prior-momentum-hypothesis",
                timestamp=T0,
            )
            processor = AutonomousResearchProcessor(store, clock=lambda: T0)
            remaining = processor.start_polymarket_campaign(
                "allowlisted-prior-omission",
                dataset_id=DATASET_ID,
                dataset_version="allowlisted-prior-v1",
                configuration_allowlist=selected_ids,
                now=T0,
            )
            self.assertEqual(remaining["fixed_configuration_count"], 1)
            self.assertEqual(
                [trial["configuration_id"] for trial in remaining["trials"]],
                [selected_ids[1]],
            )
            self.assertEqual(
                processor.bus.list_campaign_trials("allowlisted-prior-omission")[0]
                .payload["campaign_configuration_id"],
                selected_ids[1],
            )

            store.save_experiment_plan(
                "allowlisted-prior-mean-reversion",
                {
                    "template": "mean_reversion",
                    "parameters": {"lookback": [1], "threshold": [0.05]},
                },
                hypothesis_id="allowlisted-prior-mean-reversion-hypothesis",
                timestamp=T0,
            )
            exhausted = processor.start_polymarket_campaign(
                "allowlisted-prior-exhaustion",
                dataset_id=DATASET_ID,
                dataset_version="allowlisted-prior-v1",
                configuration_allowlist=selected_ids,
                now=T0,
            )
            self.assertEqual(
                exhausted["status"],
                "CAMPAIGN_EXHAUSTED_NO_QUALIFIED_STRATEGY",
            )
            self.assertEqual(exhausted["fixed_configuration_count"], 0)
            self.assertEqual(exhausted["trials"], [])
            self.assertEqual(exhausted["counts"]["planned"], 0)
            self.assertEqual(
                processor.bus.list_campaign_trials("allowlisted-prior-exhaustion"),
                (),
            )

    def test_campaign_rejects_unattested_history_before_queueing_work(self) -> None:
        rows = _rows(midpoint=0.50)
        with AxiomStore(":memory:") as store:
            store.save_dataset(
                DATASET_ID,
                "unattested-v1",
                rows,
                metadata={
                    "source_type": "HISTORICAL",
                    "market_type": "prediction",
                    "instrument": "POLYMARKET",
                },
                quality="PRICE_PROXY",
            )
            processor = AutonomousResearchProcessor(store, clock=lambda: T0)
            with self.assertRaises(AutonomousResearchError) as raised:
                processor.start_polymarket_campaign(
                    "unattested-campaign",
                    dataset_id=DATASET_ID,
                    dataset_version="unattested-v1",
                    now=T0,
                )
            self.assertEqual(raised.exception.reason, "SOFTWARE_OR_INPUT_ERROR")
            self.assertEqual(
                processor.bus.list_campaign_trials("unattested-campaign"),
                (),
            )
    def test_campaign_rejects_attested_but_partial_aggregate_coverage(self) -> None:
        rows = _rows(midpoint=0.50)
        metadata = {
            "fixture_label": SYNTHETIC_LABEL,
            "synthetic": True,
            "provider": SYNTHETIC_LABEL,
            "source_type": "HISTORICAL",
            "market_type": "prediction",
            "instrument": "POLYMARKET",
            "research_quality": "PRICE_PROXY",
            "coverage_status": "PARTIAL",
            "requested_coverage": {"discovery_complete": False},
        }
        with AxiomStore(":memory:") as store:
            store.save_dataset(
                DATASET_ID,
                "partial-aggregate-v1",
                rows,
                metadata=metadata,
                quality="PRICE_PROXY",
            )
            store.save_dataset_catalog(
                DATASET_ID,
                "partial-aggregate-v1",
                provider=SYNTHETIC_LABEL,
                instrument="POLYMARKET",
                market_type="prediction",
                timeframe="event",
                start_timestamp=T0,
                end_timestamp=T0,
                row_count=len(rows),
                completeness=1.0,
                quality="PRICE_PROXY",
                source_type="HISTORICAL",
                snapshot_id=f"{SYNTHETIC_LABEL}:catalog:partial-aggregate-v1",
                metadata=metadata,
            )
            attestation = store.verify_dataset_integrity_attestation(
                DATASET_ID,
                "partial-aggregate-v1",
                force=True,
            )
            self.assertEqual(attestation["status"], "CURRENT")
            processor = AutonomousResearchProcessor(store, clock=lambda: T0)
            with self.assertRaises(AutonomousResearchError) as raised:
                processor.start_polymarket_campaign(
                    "partial-aggregate-campaign",
                    dataset_id=DATASET_ID,
                    dataset_version="partial-aggregate-v1",
                    now=T0,
                )
            self.assertEqual(raised.exception.reason, "SOFTWARE_OR_INPUT_ERROR")


    def test_explicit_missing_ranges_reject_attestation_and_campaign(self) -> None:
        rows = _rows(midpoint=0.50)
        metadata = {
            "fixture_label": SYNTHETIC_LABEL,
            "synthetic": True,
            "provider": SYNTHETIC_LABEL,
            "source_type": "HISTORICAL",
            "market_type": "prediction",
            "instrument": "POLYMARKET",
            "research_quality": "PRICE_PROXY",
        }
        with AxiomStore(":memory:") as store:
            store.save_dataset(
                DATASET_ID,
                "gapped-v1",
                rows,
                metadata=metadata,
                quality="PRICE_PROXY",
            )
            store.save_dataset_catalog(
                DATASET_ID,
                "gapped-v1",
                provider=SYNTHETIC_LABEL,
                instrument="POLYMARKET",
                market_type="prediction",
                timeframe="event",
                start_timestamp=T0,
                end_timestamp=T0,
                row_count=len(rows),
                completeness=1.0,
                missing_ranges=[{"start": T0.isoformat(), "end": T0.isoformat()}],
                quality="PRICE_PROXY",
                source_type="HISTORICAL",
                snapshot_id=f"{SYNTHETIC_LABEL}:catalog:gapped-v1",
                metadata=metadata,
            )
            attestation = store.verify_dataset_integrity_attestation(
                DATASET_ID,
                "gapped-v1",
                force=True,
            )
            self.assertEqual(attestation["status"], "STALE")
            self.assertEqual(attestation["reason"], "INCOMPLETE_DATASET")
            processor = AutonomousResearchProcessor(store, clock=lambda: T0)
            with self.assertRaises(AutonomousResearchError) as raised:
                processor.start_polymarket_campaign(
                    "gapped-campaign",
                    dataset_id=DATASET_ID,
                    dataset_version="gapped-v1",
                    now=T0,
                )
            self.assertEqual(raised.exception.reason, "SOFTWARE_OR_INPUT_ERROR")
            self.assertEqual(processor.bus.list_campaign_trials("gapped-campaign"), ())

    def test_campaign_start_deduplicates_prior_multi_parameter_plan(self) -> None:
        with AxiomStore(":memory:") as store:
            _save_catalog_attested_dataset(
                store,
                "prior-grid-v1",
                _rows(midpoint=0.50),
            )
            store.save_experiment_plan(
                "prior-grid-plan",
                {
                    "template": "momentum",
                    "parameters": {
                        "lookback": [1, 3],
                        "threshold": [0.02, 0.05],
                    },
                },
                hypothesis_id="prior-grid-hypothesis",
                timestamp=T0,
            )

            processor = AutonomousResearchProcessor(store, clock=lambda: T0)
            state = processor.start_polymarket_campaign(
                "prior-grid-campaign",
                dataset_id=DATASET_ID,
                dataset_version="prior-grid-v1",
                now=T0,
            )

            self.assertEqual(state["status"], "RUNNING")
            self.assertEqual(state["fixed_configuration_count"], 8)
            configurations = {
                trial["configuration_id"]
                for trial in state["trials"]
            }
            self.assertTrue(
                {
                    "momentum:lookback-1:threshold-0.02",
                    "momentum:lookback-1:threshold-0.05",
                    "momentum:lookback-3:threshold-0.02",
                    "momentum:lookback-3:threshold-0.05",
                }.isdisjoint(configurations)
            )
            queued = processor.bus.list_campaign_trials("prior-grid-campaign", limit=10)
            self.assertEqual(len(queued), 1)
            self.assertEqual(
                queued[0].payload["campaign_configuration_id"],
                "momentum:lookback-5:threshold-0.02",
            )


    def test_synthetic_campaign_covers_protocol_progress_reassessment_and_exhaustion(self) -> None:
        """Exercise A/B/C/D/G against only labelled, attested offline fixtures."""
        with AxiomStore(":memory:") as store:
            first_attestation = _save_attested_dataset(store, "v1", midpoint=0.50)
            second_attestation = _save_attested_dataset(store, "v2", midpoint=0.52)
            self.assertNotEqual(
                first_attestation["attestation_hash"],
                second_attestation["attestation_hash"],
            )

            # Provider/attestation failures remain evidence for this job only;
            # this injectable fault must not escape into campaign scheduling.
            fault_processor = AutonomousResearchProcessor(store, clock=lambda: T0)
            with patch.object(
                store,
                "load_dataset_integrity_attestation",
                side_effect=RuntimeError("synthetic provider failure"),
            ):
                self.assertIsNone(
                    fault_processor._load_next_dataset_version(
                        dataset_id=DATASET_ID,
                        rejected_version="v1",
                    )
                )

            campaign_id = "synthetic-campaign-a-through-g"
            processor = AutonomousResearchProcessor(store, clock=lambda: T0)
            initial = processor.start_polymarket_campaign(
                campaign_id,
                dataset_id=DATASET_ID,
                dataset_version="v1",
                now=T0,
            )
            campaign_job_name = processor.campaign_job_name(campaign_id)
            durable_job = store.get_operator_job(campaign_job_name)
            self.assertIsNotNone(durable_job)
            assert durable_job is not None
            self.assertEqual(durable_job["payload"]["protocol"], initial["protocol"])
            self.assertEqual(initial["status"], "RUNNING")
            self.assertEqual(initial["protocol"]["dataset_boundary"]["dataset_id"], DATASET_ID)
            self.assertEqual(initial["protocol"]["dataset_boundary"]["dataset_version"], "v1")
            self.assertEqual(
                initial["protocol"]["dataset_boundary"]["attestation_hash"],
                first_attestation["attestation_hash"],
            )
            first_queue = processor.bus.list_campaign_trials(campaign_id, limit=100)
            self.assertEqual(len(first_queue), 1)
            self.assertEqual(first_queue[0].payload["campaign_id"], campaign_id)

            # A: economic rejection advances to the next predeclared configuration
            # without changing the immutable dataset boundary.
            first_trial = first_queue[0]
            processor._advance_campaign_after_result(
                first_trial,
                {
                    "accepted": False,
                    "reason_code": "NEGATIVE_VALIDATION_EXPECTANCY",
                    "candidate_id": "synthetic-candidate-a",
                },
                now=T0 + timedelta(minutes=1),
            )
            after_a = processor.campaign_state(campaign_id)
            self.assertEqual(after_a["status"], "RUNNING")
            self.assertEqual(after_a["counts"]["economic_rejection"], 1)
            self.assertEqual(after_a["budget_used"], 2)
            self.assertEqual(after_a["protocol"]["dataset_boundary"]["dataset_version"], "v1")
            self.assertEqual(after_a["trials"][0]["status"], "ECONOMIC_REJECTION")
            self.assertEqual(after_a["trials"][1]["status"], "RUNNING")
            self.assertNotEqual(
                after_a["trials"][0]["configuration_id"],
                after_a["trials"][1]["configuration_id"],
            )

            # B/C: data insufficiency pauses the trial and names the actual,
            second_trial = next(
                item
                for item in processor.bus.list_campaign_trials(campaign_id, limit=100)
                if item.payload["campaign_trial_id"] == after_a["trials"][1]["trial_id"]
            )
            processor._advance_campaign_after_result(
                second_trial,
                {
                    "accepted": False,
                    "reason_code": "INSUFFICIENT_DATA",
                    "candidate_id": "synthetic-candidate-b",
                },
                now=T0 + timedelta(minutes=2),
            )
            waiting = processor.campaign_state(campaign_id)
            self.assertEqual(waiting["status"], "WAITING_FOR_DATA")
            self.assertEqual(waiting["counts"]["data_insufficient"], 1)
            next_real_job = waiting["next_real_job"]
            self.assertTrue(next_real_job)
            data_job = store.get_operator_job(next_real_job)
            self.assertIsNotNone(data_job)
            assert data_job is not None
            self.assertEqual(data_job["status"], "SCHEDULED")
            self.assertEqual(data_job["payload"]["campaign_id"], campaign_id)
            self.assertEqual(
                data_job["payload"]["producer_job"],
                f"polymarket-dataset-producer:{campaign_id}:{waiting['trials'][1]['trial_id']}",
            )
            self.assertEqual(data_job["payload"]["dataset_version"], "v1")
            # D: a changed, current attestation is accepted once, and duplicate
            # evidence cannot create a second reassessment or queue item.
            queue_before_reassessment = len(processor.bus.list_campaign_trials(campaign_id, limit=100))
            evidence_identity = str(second_attestation["attestation_hash"])
            self.assertNotEqual(evidence_identity, str(first_attestation["attestation_hash"]))
            processor.reassess_campaign(
                campaign_id,
                evidence_identity=evidence_identity,
                now=T0 + timedelta(minutes=3),
            )
            reassessed = processor.campaign_state(campaign_id)
            self.assertEqual(reassessed["reassessment_count"], 1)
            self.assertEqual(reassessed["status"], "RUNNING")
            self.assertEqual(reassessed["reassessment_evidence"]["identity"], evidence_identity)
            self.assertEqual(
                len(processor.bus.list_campaign_trials(campaign_id, limit=100)),
                queue_before_reassessment + 1,
            )
            reassessment_id = f"{reassessed['trials'][1]['trial_id']}:reassessment-1"
            reassessment_trial = next(
                item for item in reassessed["trials"] if item["trial_id"] == reassessment_id
            )
            reassessment_boundary = reassessed["reassessment_boundaries"][reassessment_id]
            self.assertEqual(
                reassessment_boundary["attestation_hash"],
                second_attestation["attestation_hash"],
            )
            self.assertEqual(reassessment_trial["status"], "RUNNING")
            reassessment_queue = [
                item
                for item in processor.bus.list_campaign_trials(campaign_id, limit=100)
                if item.payload["campaign_trial_id"] == reassessment_id
            ]
            self.assertEqual(len(reassessment_queue), 1)
            self.assertEqual(
                reassessment_queue[0].payload["experiment_plan"]["dataset_selector"]["dataset_version"],
                "v2",
            )
            self.assertTrue(
                any(item["status"] == "PLANNED" for item in reassessed["trials"])
            )
            processor.reassess_campaign(
                campaign_id,
                evidence_identity=evidence_identity,
                now=T0 + timedelta(minutes=4),
            )
            self.assertEqual(processor.campaign_state(campaign_id)["reassessment_count"], 1)
            self.assertEqual(
                len(processor.bus.list_campaign_trials(campaign_id, limit=100)),
                queue_before_reassessment + 1,
            )

            # G: every remaining fixed configuration gets exactly one bounded
            # turn; exhaustion is durable and produces no successor queue item.
            for tick in range(32):
                state = processor.campaign_state(campaign_id)
                if state["status"] == "CAMPAIGN_EXHAUSTED_NO_QUALIFIED_STRATEGY":
                    break
                running = next(
                    trial
                    for trial in state["trials"]
                    if trial["status"] == "RUNNING"
                )
                queue_item = next(
                    item
                    for item in processor.bus.list_campaign_trials(campaign_id, limit=100)
                    if item.payload["campaign_trial_id"] == running["trial_id"]
                )
                processor._advance_campaign_after_result(
                    queue_item,
                    {
                        "accepted": False,
                        "reason_code": "NEGATIVE_VALIDATION_EXPECTANCY",
                        "candidate_id": f"synthetic-candidate-{tick + 3}",
                    },
                    now=T0 + timedelta(minutes=5 + tick),
                )
            else:
                self.fail("synthetic fixed campaign did not exhaust within its bounded plan")

            exhausted = processor.campaign_state(campaign_id)
            self.assertEqual(exhausted["status"], "CAMPAIGN_EXHAUSTED_NO_QUALIFIED_STRATEGY")
            self.assertEqual(exhausted["qualified_candidate_ids"], [])
            self.assertEqual(exhausted["next_real_job"], None)
            self.assertTrue(exhausted["final_assessment_evaluated"])
            self.assertEqual(exhausted["counts"]["economic_rejection"], 12)
            self.assertEqual(
                len(processor.bus.list_campaign_trials(campaign_id, limit=100)),
                len(exhausted["trials"]),
            )
            durable_exhaustion = store.get_operator_job(campaign_job_name)
            self.assertIsNotNone(durable_exhaustion)
            assert durable_exhaustion is not None
            self.assertEqual(
                durable_exhaustion["status"],
                "CAMPAIGN_EXHAUSTED_NO_QUALIFIED_STRATEGY",
            )
            self.assertFalse(durable_exhaustion["resumable"])
            persisted = store.load_dataset(DATASET_ID, "v1")
            persisted_rows = (
                persisted.get("records", [])
                if isinstance(persisted, Mapping)
                else persisted
            )
            self.assertEqual(persisted_rows[0]["fixture_label"], SYNTHETIC_LABEL)
            self.assertTrue(persisted_rows[0]["synthetic"])

    def test_terminal_campaign_reassessment_does_not_reopen_legacy_payload(self) -> None:
        with AxiomStore(":memory:") as store:
            initial_attestation = _save_attested_dataset(store, "v1", midpoint=0.50)
            changed_attestation = _save_attested_dataset(store, "v2", midpoint=0.55)
            self.assertNotEqual(
                initial_attestation["attestation_hash"],
                changed_attestation["attestation_hash"],
            )
            processor = AutonomousResearchProcessor(store, clock=lambda: T0)
            campaign_id = "legacy-terminal-reassessment-campaign"
            processor.start_polymarket_campaign(
                campaign_id,
                dataset_id=DATASET_ID,
                dataset_version="v1",
                now=T0,
            )
            first_trial = processor.bus.list_campaign_trials(campaign_id, limit=10)[0]
            processor._advance_campaign_after_result(
                first_trial,
                {
                    "accepted": False,
                    "reason_code": "INSUFFICIENT_DATA",
                    "candidate_id": "legacy-terminal-candidate",
                },
                now=T0 + timedelta(minutes=1),
            )
            waiting = processor.campaign_state(campaign_id)
            self.assertIsNotNone(waiting)
            assert waiting is not None
            self.assertEqual(waiting["status"], "WAITING_FOR_DATA")

            # Older terminal rows did not always preserve either evidence
            # identity.  Keep a waiting trial to prove the guard runs before
            # reassessment eligibility and queue handling.
            legacy_payload = json.loads(json.dumps(waiting))
            legacy_payload["status"] = "CAMPAIGN_EXHAUSTED_NO_QUALIFIED_STRATEGY"
            legacy_payload["last_evidence_identity"] = None
            legacy_boundary = legacy_payload["protocol"]["dataset_boundary"]
            self.assertIsInstance(legacy_boundary, dict)
            legacy_boundary["attestation_hash"] = None
            campaign_job_name = processor.campaign_job_name(campaign_id)
            store.set_operator_job(
                campaign_job_name,
                "CAMPAIGN_EXHAUSTED_NO_QUALIFIED_STRATEGY",
                legacy_payload,
                resumable=False,
                timestamp=T0 + timedelta(minutes=2),
            )
            before_record = store.get_operator_job(campaign_job_name)
            self.assertIsNotNone(before_record)
            assert before_record is not None
            before_payload = before_record["payload"]
            before_queue = tuple(
                (
                    item.item_id,
                    item.status.value,
                    json.dumps(dict(item.payload), sort_keys=True),
                )
                for item in processor.bus.list_campaign_trials(campaign_id, limit=100)
            )

            returned = processor.reassess_campaign(
                campaign_id,
                evidence_identity=str(changed_attestation["attestation_hash"]),
                dataset_id=DATASET_ID,
                dataset_version="v2",
                now=T0 + timedelta(minutes=3),
            )
            self.assertEqual(returned, before_payload)
            after_record = store.get_operator_job(campaign_job_name)
            self.assertIsNotNone(after_record)
            assert after_record is not None
            self.assertEqual(after_record["status"], before_record["status"])
            self.assertEqual(after_record["payload"], before_record["payload"])
            self.assertFalse(after_record["resumable"])
            self.assertEqual(after_record["payload"]["status"], before_payload["status"])
            self.assertEqual(after_record["payload"]["budget_used"], before_payload["budget_used"])
            self.assertEqual(
                after_record["payload"]["budget_remaining"],
                before_payload["budget_remaining"],
            )
            self.assertEqual(
                after_record["payload"]["reassessment_count"],
                before_payload["reassessment_count"],
            )
            after_queue = tuple(
                (
                    item.item_id,
                    item.status.value,
                    json.dumps(dict(item.payload), sort_keys=True),
                )
                for item in processor.bus.list_campaign_trials(campaign_id, limit=100)
            )
            self.assertEqual(after_queue, before_queue)
            self.assertEqual(
                str(after_record["payload"]["last_evidence_identity"] or ""),
                "",
            )
            self.assertIsNone(after_record["payload"]["protocol"]["dataset_boundary"]["attestation_hash"])

    def test_legacy_nonresumable_and_superseded_campaign_reassessment_is_terminal(self) -> None:
        variants = (
            ("nonresumable-error", False, None),
            ("superseded-error", True, "SUPERSEDED_PROTOCOL"),
        )
        for suffix, resumable, supersession_reason in variants:
            with self.subTest(suffix=suffix):
                with AxiomStore(":memory:") as store:
                    initial_attestation = _save_attested_dataset(store, "v1", midpoint=0.50)
                    changed_attestation = _save_attested_dataset(store, "v2", midpoint=0.55)
                    self.assertNotEqual(
                        initial_attestation["attestation_hash"],
                        changed_attestation["attestation_hash"],
                    )
                    processor = AutonomousResearchProcessor(store, clock=lambda: T0)
                    campaign_id = f"legacy-{suffix}-campaign"
                    processor.start_polymarket_campaign(
                        campaign_id,
                        dataset_id=DATASET_ID,
                        dataset_version="v1",
                        now=T0,
                    )
                    first_trial = processor.bus.list_campaign_trials(campaign_id, limit=10)[0]
                    processor._advance_campaign_after_result(
                        first_trial,
                        {
                            "accepted": False,
                            "reason_code": "INSUFFICIENT_DATA",
                            "candidate_id": f"{suffix}-candidate",
                        },
                        now=T0 + timedelta(minutes=1),
                    )
                    waiting = processor.campaign_state(campaign_id)
                    self.assertIsNotNone(waiting)
                    assert waiting is not None
                    legacy_payload = json.loads(json.dumps(waiting))
                    legacy_payload["status"] = "SOFTWARE_OR_INPUT_ERROR"
                    legacy_payload["last_evidence_identity"] = None
                    if supersession_reason is None:
                        legacy_payload.pop("supersession_reason", None)
                    else:
                        legacy_payload["supersession_reason"] = supersession_reason
                    campaign_job_name = processor.campaign_job_name(campaign_id)
                    store.set_operator_job(
                        campaign_job_name,
                        "SOFTWARE_OR_INPUT_ERROR",
                        legacy_payload,
                        resumable=resumable,
                        timestamp=T0 + timedelta(minutes=2),
                    )
                    before_record = store.get_operator_job(campaign_job_name)
                    self.assertIsNotNone(before_record)
                    assert before_record is not None
                    before_payload = before_record["payload"]
                    before_queue_ids = tuple(
                        item.item_id
                        for item in processor.bus.list_campaign_trials(campaign_id, limit=100)
                    )

                    returned = processor.reassess_campaign(
                        campaign_id,
                        evidence_identity=str(changed_attestation["attestation_hash"]),
                        dataset_id=DATASET_ID,
                        dataset_version="v2",
                        now=T0 + timedelta(minutes=3),
                    )
                    self.assertEqual(returned, before_payload)
                    after_record = store.get_operator_job(campaign_job_name)
                    self.assertIsNotNone(after_record)
                    assert after_record is not None
                    self.assertEqual(after_record["status"], before_record["status"])
                    self.assertEqual(after_record["payload"], before_record["payload"])
                    self.assertEqual(after_record["resumable"], before_record["resumable"])
                    self.assertEqual(
                        after_record["payload"]["budget_used"],
                        before_payload["budget_used"],
                    )
                    self.assertEqual(
                        after_record["payload"]["budget_remaining"],
                        before_payload["budget_remaining"],
                    )
                    self.assertEqual(
                        after_record["payload"]["reassessment_count"],
                        before_payload["reassessment_count"],
                    )
                    self.assertEqual(
                        tuple(
                            item.item_id
                            for item in processor.bus.list_campaign_trials(
                                campaign_id,
                                limit=100,
                            )
                        ),
                        before_queue_ids,
                    )

    def test_reassessment_rejects_initial_boundary_attestation_identity(self) -> None:
        with AxiomStore(":memory:") as store:
            initial_attestation = _save_attested_dataset(store, "initial-v1", midpoint=0.50)
            processor = AutonomousResearchProcessor(store, clock=lambda: T0)
            changed_attestation = _save_attested_dataset(store, "changed-v2", midpoint=0.55)
            campaign_id = "unchanged-evidence-campaign"
            processor.start_polymarket_campaign(
                campaign_id,
                dataset_id=DATASET_ID,
                dataset_version="initial-v1",
                now=T0,
            )
            first_trial = processor.bus.list_campaign_trials(campaign_id, limit=10)[0]
            processor._advance_campaign_after_result(
                first_trial,
                {
                    "accepted": False,
                    "reason_code": "INSUFFICIENT_DATA",
                    "candidate_id": "unchanged-evidence-candidate",
                },
                now=T0 + timedelta(minutes=1),
            )
            before = processor.campaign_state(campaign_id)
            self.assertEqual(before["status"], "WAITING_FOR_DATA")
            self.assertEqual(before["reassessment_count"], 0)
            legacy_payload = json.loads(json.dumps(before))
            legacy_payload["last_evidence_identity"] = None
            store.set_operator_job(
                processor.campaign_job_name(campaign_id),
                "WAITING_FOR_DATA",
                legacy_payload,
                resumable=True,
                timestamp=T0 + timedelta(minutes=1, seconds=30),
            )
            before = processor.campaign_state(campaign_id)
            queue_before = len(processor.bus.list_campaign_trials(campaign_id, limit=100))

            after = processor.reassess_campaign(
                campaign_id,
                evidence_identity=str(initial_attestation["attestation_hash"]),
                now=T0 + timedelta(minutes=2),
            )
            self.assertEqual(after["status"], "WAITING_FOR_DATA")
            self.assertEqual(after["reassessment_count"], 0)
            self.assertIsNone(after["last_evidence_identity"])
            self.assertEqual(
                after["protocol"]["dataset_boundary"]["attestation_hash"],
                initial_attestation["attestation_hash"],
            )
            self.assertEqual(
                after["budget_used"],
                before["budget_used"],
            )
            self.assertEqual(
                len(processor.bus.list_campaign_trials(campaign_id, limit=100)),
                queue_before,
            )

            no_baseline_payload = json.loads(json.dumps(after))
            no_baseline_payload["protocol"]["dataset_boundary"]["attestation_hash"] = None
            # Keep the legacy payload's protocol envelope canonical after
            # removing the old boundary attestation.
            no_baseline_payload["protocol_hash"] = _hash_document(
                no_baseline_payload["protocol"]
            )
            store.set_operator_job(
                processor.campaign_job_name(campaign_id),
                "WAITING_FOR_DATA",
                no_baseline_payload,
                resumable=True,
                timestamp=T0 + timedelta(minutes=2, seconds=30),
            )
            no_baseline_before = processor.campaign_state(campaign_id)
            self.assertIsNotNone(no_baseline_before)
            assert no_baseline_before is not None
            no_baseline_queue = tuple(
                item.item_id
                for item in processor.bus.list_campaign_trials(campaign_id, limit=100)
            )
            no_baseline_after = processor.reassess_campaign(
                campaign_id,
                evidence_identity=str(changed_attestation["attestation_hash"]),
                dataset_id=DATASET_ID,
                dataset_version="changed-v2",
                now=T0 + timedelta(minutes=3),
            )
            self.assertEqual(no_baseline_after["status"], "WAITING_FOR_DATA")
            self.assertEqual(no_baseline_after["reassessment_count"], 0)
            self.assertIsNone(no_baseline_after["last_evidence_identity"])
            self.assertEqual(
                no_baseline_after["budget_used"],
                no_baseline_before["budget_used"],
            )
            self.assertEqual(
                no_baseline_after["budget_remaining"],
                no_baseline_before["budget_remaining"],
            )
            self.assertEqual(
                tuple(
                    item.item_id
                    for item in processor.bus.list_campaign_trials(campaign_id, limit=100)
                ),
                no_baseline_queue,
            )

    def test_final_assessment_rejects_zero_trades_with_sufficient_observations(self) -> None:
        campaign_grid = (
            {
                "configuration_id": "momentum:lookback-1:threshold-0.05",
                "template": "momentum",
                "parameters": {"lookback": 1, "threshold": 0.05},
            },
        )
        rows = [
            {
                "fixture_label": SYNTHETIC_LABEL,
                "synthetic": True,
                "timestamp": (T0 + timedelta(seconds=index)).isoformat(),
                "market_id": f"zero-trades-market-{index}",
                "yes_mid": 0.50,
                "yes_bid": 0.49,
                "yes_ask": 0.51,
                "settlement": "open",
                "source_snapshot_id": f"{SYNTHETIC_LABEL}:zero-trades:{index}",
            }
            for index in range(5)
        ]
        gates = {"min_expectancy": 0.0, "min_samples": 1, "min_trades": 1}
        with patch("axiom.autonomous.POLYMARKET_CAMPAIGN_GRID", campaign_grid), AxiomStore(":memory:") as store:
            _save_catalog_attested_dataset(store, "zero-trades-v1", rows)
            processor = AutonomousResearchProcessor(store, clock=lambda: T0)
            state = processor.start_polymarket_campaign(
                "zero-trades-campaign",
                dataset_id=DATASET_ID,
                dataset_version="zero-trades-v1",
                qualification_gates=gates,
                now=T0,
            )
            self.assertEqual(state["protocol"]["qualification_gates"], gates)
            trial = processor.bus.list_campaign_trials("zero-trades-campaign")[0]

            with patch.object(
                processor,
                "_run_backtest",
                return_value={"sample_count": 1, "filled_trades": 0, "expectancy": 0.10},
            ):
                processor._advance_campaign_after_result(
                    trial,
                    {
                        "accepted": True,
                        "reason_code": "VALIDATION_QUALIFIED",
                        "candidate_id": "zero-trades-finalist",
                    },
                    now=T0 + timedelta(minutes=1),
                )

            terminal = processor.campaign_state("zero-trades-campaign")
            self.assertIsNotNone(terminal)
            assert terminal is not None
            self.assertEqual(terminal["status"], "CAMPAIGN_EXHAUSTED_NO_QUALIFIED_STRATEGY")
            self.assertEqual(terminal["qualified_candidate_ids"], [])
            self.assertTrue(terminal["final_assessment_evaluated"])
            assessment = terminal["final_assessment"]["assessments"][0]
            self.assertEqual(assessment["status"], "REJECTED")
            self.assertFalse(assessment["passed"])
            self.assertTrue(assessment["minimum_sample_check"]["checks"]["observations"])
            self.assertFalse(assessment["minimum_sample_check"]["checks"]["trades"])
            self.assertEqual(assessment["minimum_sample_check"]["trades"], 0)
            self.assertEqual(assessment["minimum_sample_check"]["min_trades"], 1)
            durable = store.get_operator_job(processor.campaign_job_name("zero-trades-campaign"))
            self.assertIsNotNone(durable)
            assert durable is not None
            self.assertEqual(durable["status"], "CAMPAIGN_EXHAUSTED_NO_QUALIFIED_STRATEGY")
            self.assertFalse(durable["resumable"])



if __name__ == "__main__":
    unittest.main()
