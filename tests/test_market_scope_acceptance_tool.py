from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
import hashlib
import json
import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch
from types import SimpleNamespace
from contextlib import contextmanager

from axiom.autonomous import AutonomousResearchProcessor
from axiom.domain import MarketType

from axiom.storage import AxiomStore

from tools.polymarket_market_scope_acceptance import (
    AcceptanceConfig,
    OfflineDiscoveryPage,
    OfflineFixtureAdapter,
    POLYMARKET_HISTORICAL_ATTESTATION_HASH,
    POLYMARKET_HISTORICAL_CONSTITUENT_COUNT,
    POLYMARKET_HISTORICAL_DATASET_ID,
    POLYMARKET_HISTORICAL_DATASET_VERSION,
    POLYMARKET_HISTORICAL_ROW_COUNT,
    _candidate_metric_projection,
    _queue_demo_evidence,
    _historical_qualification_assessment,
    _runtime_metric_assessment,
    _sha256_json,
    build_offline_fixture_adapter,
    compute_dollar_limit_buy_feasibility,
    enqueue_legacy_successor,
    main,
    run_acceptance,
    run_persisted_acceptance,
)


PINNED_SOURCE = (
    Path(__file__).resolve().parents[2]
    / "runtime-data"
    / "axiom-forward-proof-20260908T141804Z.sqlite"
)
LIVE_SOURCE = Path(__file__).resolve().parents[2] / "runtime-data" / "axiom.sqlite"
COMPACT_DATASET_ID = "compact-persisted-historical"
COMPACT_MARKET_ID = "compact-market-1"
COMPACT_TIMESTAMP = "2026-01-01T00:00:00+00:00"


def _compact_version(record: dict[str, object]) -> str:
    identity = [{
        "timestamp": record["source_timestamp"],
        "price": record["price"],
        "token_id": record["token_id"],
    }]
    payload = json.dumps(identity, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return "sha256:" + hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _build_compact_persisted_source(
    path: Path,
    *,
    include_record_source_type: bool = True,
    include_record_market_id: bool = True,
) -> tuple[str, str, str]:
    token_id = f"yes-{COMPACT_MARKET_ID}"
    record = {
        "source_timestamp": COMPACT_TIMESTAMP,
        "timestamp": COMPACT_TIMESTAMP,
        "observed_at": COMPACT_TIMESTAMP,
        "price": 0.5,
        "token_id": token_id,
        "provider": "polymarket",
    }
    if include_record_market_id:
        record["market_id"] = COMPACT_MARKET_ID
    if include_record_source_type:
        record["source_type"] = "HISTORICAL"
    constituent_id = f"prediction:{COMPACT_MARKET_ID}"
    constituent_version = _compact_version(record)
    start = datetime.fromisoformat(COMPACT_TIMESTAMP)
    metadata = {
        "market_id": COMPACT_MARKET_ID,
        "token_ids": {"yes": token_id, "no": f"no-{COMPACT_MARKET_ID}"},
        "source_type": "HISTORICAL",
        "provider": "polymarket",
        "instrument": "POLYMARKET",
    }
    aggregate_metadata = {
        "source_type": "HISTORICAL",
        "provider": "polymarket",
        "instrument": "POLYMARKET",
        "markets_discovered": 1,
        "markets_imported": 1,
        "price_points": 1,
        "category_counts": {"other": 1},
        "market_versions": [{
            "market_id": COMPACT_MARKET_ID,
            "dataset_id": constituent_id,
            "dataset_version": constituent_version,
            "row_count": 1,
        }],
    }
    with AxiomStore(path) as store:
        store.save_dataset(constituent_id, constituent_version, [record], metadata=metadata, quality="PRICE_PROXY")
        store.save_dataset_catalog(
            constituent_id,
            constituent_version,
            provider="polymarket",
            instrument="POLYMARKET",
            market_type="prediction",
            timeframe="event",
            start_timestamp=start,
            end_timestamp=start,
            row_count=1,
            completeness=1.0,
            missing_ranges=(),
            quality="PRICE_PROXY",
            source_type="HISTORICAL",
            snapshot_id=f"{constituent_id}:{constituent_version}",
            metadata=metadata,
            created_at=start,
            updated_at=start,
        )
        store.save_dataset_catalog(
            COMPACT_DATASET_ID,
            "compact-v1",
            provider="polymarket",
            instrument="POLYMARKET",
            market_type="prediction",
            timeframe="event",
            start_timestamp=start,
            end_timestamp=start,
            row_count=1,
            completeness=1.0,
            missing_ranges=(),
            quality="PRICE_PROXY",
            source_type="HISTORICAL",
            snapshot_id=f"{COMPACT_DATASET_ID}:compact-v1",
            metadata=aggregate_metadata,
            created_at=start,
            updated_at=start,
        )
        attestation = store.verify_dataset_integrity_attestation(
            COMPACT_DATASET_ID,
            "compact-v1",
            force=True,
        )
        attestation_hash = str(attestation["attestation_hash"])
    return COMPACT_DATASET_ID, "compact-v1", attestation_hash


@contextmanager
def _compact_contract(
    path: Path,
    *,
    expected_row_count: int = 1,
    expected_constituent_count: int = 1,
):
    if not path.exists():
        dataset_id, dataset_version, attestation_hash = _build_compact_persisted_source(path)
    else:
        dataset_id, dataset_version, attestation_hash = (
            COMPACT_DATASET_ID,
            "compact-v1",
            "",
        )
        connection = sqlite3.connect(path)
        try:
            row = connection.execute(
                "SELECT attestation_hash FROM dataset_integrity_attestation WHERE dataset_id=? AND dataset_version=?",
                (dataset_id, dataset_version),
            ).fetchone()
            if row is None:
                raise AssertionError("compact source is missing its aggregate attestation")
            attestation_hash = str(row[0])
        finally:
            connection.close()
    replacements = {
        "tools.polymarket_market_scope_acceptance._PINNED_PERSISTED_DATASET_ID": dataset_id,
        "tools.polymarket_market_scope_acceptance._PINNED_PERSISTED_DATASET_VERSION": dataset_version,
        "tools.polymarket_market_scope_acceptance._PINNED_PERSISTED_ATTESTATION_HASH": attestation_hash,
        "tools.polymarket_market_scope_acceptance._PINNED_PERSISTED_ROW_COUNT": expected_row_count,
        "tools.polymarket_market_scope_acceptance._PINNED_PERSISTED_CONSTITUENT_COUNT": expected_constituent_count,
    }
    patches = []
    try:
        for name, value in replacements.items():
            item = patch(name, value)
            patches.append(item)
            item.start()
        yield dataset_id, dataset_version, attestation_hash
    finally:
        for item in reversed(patches):
            item.stop()


def _compact_report(
    path: Path,
    *,
    expected_row_count: int | None = None,
    expected_constituent_count: int | None = None,
    **config_values: object,
) -> dict[str, object]:
    contract_kwargs = {}
    if expected_row_count is not None:
        contract_kwargs["expected_row_count"] = expected_row_count
    if expected_constituent_count is not None:
        contract_kwargs["expected_constituent_count"] = expected_constituent_count
    with _compact_contract(path, **contract_kwargs) as (dataset_id, dataset_version, _):
        values = {
            "mode": "persisted",
            "source_backup": path,
            "dataset_id": dataset_id,
            "dataset_version": dataset_version,
            "max_books": 0,
            "sample_limit": 1,
        }
        values.update(config_values)
        config = AcceptanceConfig(**values)
        return run_persisted_acceptance(
            path,
            dataset_id,
            dataset_version,
            config=config,
            generated_at=COMPACT_TIMESTAMP,
        )


def _build_compact_historical_source(
    path: Path,
    records: list[dict[str, object]],
) -> tuple[str, str, str]:
    """Create a bounded attested source for chronology and feature-boundary tests."""
    if not records:
        raise ValueError("historical fixture requires at least one record")
    token_id = f"yes-{COMPACT_MARKET_ID}"
    constituent_id = f"prediction:{COMPACT_MARKET_ID}"
    identity = [
        {
            "timestamp": record["source_timestamp"],
            "price": record.get("price", record.get("yes_mid")),
            "token_id": record["token_id"],
        }
        for record in records
    ]
    payload = json.dumps(identity, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    constituent_version = "sha256:" + hashlib.sha256(payload.encode("utf-8")).hexdigest()
    timestamps = [datetime.fromisoformat(str(record["source_timestamp"])) for record in records]
    start, end = min(timestamps), max(timestamps)
    metadata = {
        "market_id": COMPACT_MARKET_ID,
        "token_ids": {"yes": token_id, "no": f"no-{COMPACT_MARKET_ID}"},
        "source_type": "HISTORICAL",
        "provider": "polymarket",
        "instrument": "POLYMARKET",
    }
    aggregate_metadata = {
        "source_type": "HISTORICAL",
        "provider": "polymarket",
        "instrument": "POLYMARKET",
        "markets_discovered": 1,
        "markets_imported": 1,
        "price_points": len(records),
        "category_counts": {"other": 1},
        "market_versions": [{
            "market_id": COMPACT_MARKET_ID,
            "dataset_id": constituent_id,
            "dataset_version": constituent_version,
            "row_count": len(records),
        }],
    }
    with AxiomStore(path) as store:
        store.save_dataset(
            constituent_id,
            constituent_version,
            records,
            metadata=metadata,
            quality="PRICE_PROXY",
        )
        store.save_dataset_catalog(
            constituent_id,
            constituent_version,
            provider="polymarket",
            instrument="POLYMARKET",
            market_type="prediction",
            timeframe="event",
            start_timestamp=start,
            end_timestamp=end,
            row_count=len(records),
            completeness=1.0,
            missing_ranges=(),
            quality="PRICE_PROXY",
            source_type="HISTORICAL",
            snapshot_id=f"{constituent_id}:{constituent_version}",
            metadata=metadata,
            created_at=start,
            updated_at=end,
        )
        store.save_dataset_catalog(
            COMPACT_DATASET_ID,
            "compact-v1",
            provider="polymarket",
            instrument="POLYMARKET",
            market_type="prediction",
            timeframe="event",
            start_timestamp=start,
            end_timestamp=end,
            row_count=len(records),
            completeness=1.0,
            missing_ranges=(),
            quality="PRICE_PROXY",
            source_type="HISTORICAL",
            snapshot_id=f"{COMPACT_DATASET_ID}:compact-v1",
            metadata=aggregate_metadata,
            created_at=start,
            updated_at=end,
        )
        attestation = store.verify_dataset_integrity_attestation(
            COMPACT_DATASET_ID,
            "compact-v1",
            force=True,
        )
        attestation_hash = str(attestation["attestation_hash"])
    return COMPACT_DATASET_ID, "compact-v1", attestation_hash


def _historical_records(
    *,
    count: int = 4,
    market_id: str = COMPACT_MARKET_ID,
) -> list[dict[str, object]]:
    token_id = f"yes-{market_id}"
    return [
        {
            "source_timestamp": f"2026-01-01T0{index}:00:00+00:00",
            "timestamp": f"2026-01-01T0{index}:00:00+00:00",
            "observed_at": f"2026-01-01T0{index}:00:00+00:00",
            "price": 0.40 + index * 0.05,
            "token_id": token_id,
            "market_id": market_id,
            "provider": "polymarket",
            "source_type": "HISTORICAL",
        }
        for index in range(count)
    ]


def _compact_mutate(path: Path, statement: str, parameters: tuple[object, ...] = ()) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.execute(statement, parameters)
        connection.commit()
    finally:
        connection.close()


def _rewrite_compact_record(path: Path, **changes: object) -> None:
    connection = sqlite3.connect(path)
    try:
        row = connection.execute(
            "SELECT payload_json FROM datasets WHERE dataset_id=?",
            (f"prediction:{COMPACT_MARKET_ID}",),
        ).fetchone()
        assert row is not None
        records = json.loads(row[0])
        for key, value in changes.items():
            if value is _MISSING:
                records[0].pop(key, None)
            else:
                records[0][key] = value
        connection.execute(
            "UPDATE datasets SET payload_json=? WHERE dataset_id=?",
            (json.dumps(records, sort_keys=True, separators=(",", ":")), f"prediction:{COMPACT_MARKET_ID}"),
        )
        connection.commit()
    finally:
        connection.close()


def _rewrite_compact_aggregate_metadata(
    path: Path,
    *,
    dataset_id: str = COMPACT_DATASET_ID,
    **changes: object,
) -> None:
    connection = sqlite3.connect(path)
    try:
        row = connection.execute(
            "SELECT metadata_json FROM dataset_catalog WHERE dataset_id=?",
            (dataset_id,),
        ).fetchone()
        assert row is not None
        metadata = json.loads(row[0])
        for key, value in changes.items():
            if value is _MISSING:
                metadata.pop(key, None)
            else:
                metadata[key] = value
        connection.execute(
            "UPDATE dataset_catalog SET metadata_json=? WHERE dataset_id=?",
            (json.dumps(metadata, sort_keys=True, separators=(",", ":")), dataset_id),
        )
        connection.commit()
    finally:
        connection.close()


def _rewrite_compact_catalog_source_type(
    path: Path,
    *,
    dataset_id: str = COMPACT_DATASET_ID,
    source_type: str = "",
) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.execute(
            "UPDATE dataset_catalog SET source_type=? WHERE dataset_id=?",
            (source_type, dataset_id),
        )
        connection.commit()
    finally:
        connection.close()


_MISSING = object()



class MarketScopeAcceptanceToolTests(unittest.TestCase):
    def test_reason_precedence_keeps_processor_and_candidate_outcomes_distinct(self) -> None:
        class EvidenceStore:
            def load_candidate_lifecycle(self, candidate_id: str) -> dict[str, object]:
                return {
                    "candidate_id": candidate_id,
                    "stage": "FROZEN",
                    "payload": {"validation_expectancy": 0.12},
                }

            def list_candidate_lifecycle_events(self, candidate_id: str, *, limit: int) -> list[dict[str, object]]:
                return [{"candidate_id": candidate_id, "to_stage": "FROZEN"}]

            def candidate_forward_requirements(self, **kwargs: object) -> dict[str, object]:
                return {
                    "candidates": [
                        {
                            "candidate_id": "candidate-1",
                            "reason_code": "QUALIFICATION_CODE",
                        }
                    ]
                }

        processor = {
            "reason_code": "PROCESSOR_CODE",
            "reason": "processor text",
            "accepted": False,
            "status": "unsupported_by_validation",
            "candidate_results": [
                {
                    "candidate_id": "candidate-1",
                    "reason_code": "CANDIDATE_CODE",
                    "reason": "candidate text",
                    "accepted": False,
                    "status": "FROZEN",
                }
            ],
        }
        evidence = _queue_demo_evidence(
            SimpleNamespace(status="REJECTED", last_error="queue text"),
            processor,
            EvidenceStore(),
            now=datetime.fromisoformat(COMPACT_TIMESTAMP),
            authority_cap=30,
        )
        self.assertEqual(evidence["reason_code"], "CANDIDATE_CODE")
        self.assertEqual(evidence["reason"], "candidate text")
        self.assertEqual(evidence["exact_reason"], "CANDIDATE_CODE")
        self.assertEqual(evidence["processor"], {
            "reason_code": "PROCESSOR_CODE",
            "reason": "processor text",
            "accepted": False,
            "status": "unsupported_by_validation",
        })
        self.assertEqual(evidence["candidate"]["status"], "FROZEN")

        processor_without_candidate_code = dict(processor)
        processor_without_candidate_code["reason_code"] = None
        processor_without_candidate_code["candidate_results"] = [
            {key: value for key, value in processor["candidate_results"][0].items() if key != "reason_code"}
        ]
        fallback = _queue_demo_evidence(
            SimpleNamespace(status="REJECTED", last_error="queue text"),
            processor_without_candidate_code,
            EvidenceStore(),
            now=datetime.fromisoformat(COMPACT_TIMESTAMP),
            authority_cap=30,
        )
        self.assertEqual(fallback["reason_code"], "QUALIFICATION_CODE")
        self.assertEqual(fallback["reason"], "QUALIFICATION_CODE")
        self.assertNotEqual(fallback["exact_reason"], "unsupported_by_validation")
    def test_lifecycle_event_reason_precedes_processor_status(self) -> None:
        class EventStore:
            def load_candidate_lifecycle(self, candidate_id: str) -> dict[str, object]:
                return {
                    "candidate_id": candidate_id,
                    "stage": "FROZEN",
                    "payload": {"status": "FROZEN"},
                }

            def list_candidate_lifecycle_events(
                self,
                candidate_id: str,
                *,
                limit: int,
            ) -> list[dict[str, object]]:
                return [
                    {
                        "candidate_id": candidate_id,
                        "to_stage": "FROZEN",
                        "reason_code": "LIFECYCLE_EVENT_CODE",
                        "reason": "lifecycle event text",
                    }
                ]

            def candidate_forward_requirements(self, **kwargs: object) -> dict[str, object]:
                return {"candidates": [{"candidate_id": "candidate-1"}]}

        evidence = _queue_demo_evidence(
            SimpleNamespace(status="REJECTED", last_error=None),
            {
                "status": "unsupported_by_validation",
                "candidate_results": [
                    {"candidate_id": "candidate-1", "status": "FROZEN"}
                ],
            },
            EventStore(),
            now=datetime.fromisoformat(COMPACT_TIMESTAMP),
            authority_cap=30,
        )
        self.assertEqual(evidence["reason_code"], "LIFECYCLE_EVENT_CODE")
        self.assertEqual(evidence["reason"], "lifecycle event text")
        self.assertEqual(evidence["exact_reason"], "LIFECYCLE_EVENT_CODE")
        self.assertEqual(evidence["processor"]["status"], "unsupported_by_validation")
        self.assertIsNone(evidence["processor"]["reason"])



    def test_candidate_metric_projection_preserves_observed_stage_evidence(self) -> None:
        payload = {
            "train": {"expectancy": 0.20, "sample_count": 100},
            "validation": {"expectancy": -0.05, "sample_count": 20},
            "robustness_passed": False,
            "minimum_sample_check": {"passed": False, "required": 30},
            "validation_stability": 0.40,
            "forward_expectancy": None,
        }
        projected = _candidate_metric_projection(
            payload,
            {"candidate_id": "candidate-1", "reason_code": "CANDIDATE_FORWARD_MARKET_UNRESOLVED"},
        )
        self.assertEqual(projected["backtest"]["train"], payload["train"])
        self.assertEqual(projected["validation"]["validation"], payload["validation"])
        self.assertEqual(projected["robustness"]["minimum_sample_check"], payload["minimum_sample_check"])
        self.assertEqual(projected["qualification"]["forward_expectancy"], None)
        self.assertEqual(
            projected["qualification"]["authority"]["reason_code"],
            "CANDIDATE_FORWARD_MARKET_UNRESOLVED",
        )

    def test_metadata_and_book_budgets_are_independent_and_finite(self) -> None:
        adapter = build_offline_fixture_adapter()
        report = run_acceptance(
            adapter,
            AcceptanceConfig(page_limit=2, max_pages=1, max_seconds=30, max_books=0, book_depth=1, sample_limit=3),
            generated_at="2026-01-01T00:00:00+00:00",
        )
        self.assertEqual(report["pagination"]["pages_observed"], 1)
        self.assertEqual(len([call for call in adapter.calls if call["path"] == "/markets/keyset"]), 1)
        self.assertEqual(report["candidate_outcome"]["book_count"], 0)
        self.assertEqual(report["config_differences"]["metadata_budget_independent_from_book_budget"], True)
        self.assertLessEqual(report["config"]["page_limit"], 100)

    def test_metadata_budget_boundaries_and_cli_rejections(self) -> None:
        for value in (0, 5):
            with self.subTest(max_pages=value):
                with self.assertRaises(ValueError):
                    AcceptanceConfig(max_pages=value)
        self.assertEqual(AcceptanceConfig(max_pages=4).max_pages, 4)
        for value in (0, 60.0001):
            with self.subTest(max_seconds=value):
                with self.assertRaises(ValueError):
                    AcceptanceConfig(max_seconds=value)
        self.assertEqual(AcceptanceConfig(max_seconds=60).max_seconds, 60.0)
        with self.assertRaises(ValueError):
            main(["--max-pages", "5"])
        with self.assertRaises(ValueError):
            main(["--max-seconds", "61"])

    def test_opaque_cursor_is_passed_exactly_and_no_cursor_is_invented(self) -> None:
        adapter = build_offline_fixture_adapter()
        report = run_acceptance(
            adapter,
            AcceptanceConfig(page_limit=4, max_pages=2, max_seconds=30, max_books=0, book_depth=1),
            generated_at="2026-01-01T00:00:00+00:00",
        )
        discovery = [call for call in adapter.calls if call["path"] == "/markets/keyset"]
        self.assertEqual([call["after_cursor"] for call in discovery], [None, "fixture-cursor-1"])
        self.assertEqual(report["pagination"]["cursor_chain"], [None, "fixture-cursor-1"])
        self.assertTrue(report["pagination"]["opaque_cursor_only"])

    def test_hashes_and_secret_scrubbing_are_stable(self) -> None:
        first = run_acceptance(
            build_offline_fixture_adapter(),
            AcceptanceConfig(page_limit=4, max_pages=2, max_seconds=30, max_books=2, book_depth=2),
            generated_at="2026-01-01T00:00:00+00:00",
        )
        second = run_acceptance(
            build_offline_fixture_adapter(),
            AcceptanceConfig(page_limit=4, max_pages=2, max_seconds=30, max_books=2, book_depth=2),
            generated_at="2026-01-01T00:00:00+00:00",
        )
        for name in ("code_hash", "config_hash", "scope_hash"):
            self.assertEqual(first[name], second[name])
        text = json.dumps(first, sort_keys=True)
        self.assertNotIn("PRIVATE-KEY-SENTINEL", text)
        self.assertNotIn("api-key", text.lower())
        self.assertTrue(first["security"]["secret_scrubbed"])
        secret_adapter = build_offline_fixture_adapter()
        secret_adapter.identities["m-001"]["private_key"] = "PRIVATE-KEY-SENTINEL"
        scrubbed = run_acceptance(
            secret_adapter,
            AcceptanceConfig(page_limit=4, max_pages=2, max_seconds=30, max_books=0, book_depth=1),
            generated_at="2026-01-01T00:00:00+00:00",
        )
        self.assertNotIn("PRIVATE-KEY-SENTINEL", json.dumps(scrubbed, sort_keys=True))
        secret_adapter.identities["m-001"]["private_key"] = 12345
        scalar_scrubbed = run_acceptance(
            secret_adapter,
            AcceptanceConfig(page_limit=4, max_pages=2, max_books=0, book_depth=1),
            generated_at="2026-01-01T00:00:00+00:00",
        )
        self.assertEqual(scalar_scrubbed["candidate_outcome"]["identity"]["private_key"], "[REDACTED]")

    def test_independent_pass_counts_differ_from_canonical_first_failure(self) -> None:
        report = run_acceptance(
            build_offline_fixture_adapter(),
            AcceptanceConfig(page_limit=4, max_pages=2, max_seconds=30, max_books=0, book_depth=1),
            generated_at="2026-01-01T00:00:00+00:00",
        )
        independent = report["counts"]["independent"]
        canonical = report["counts"]["canonical_first_failure"]
        self.assertGreater(independent["category_tags"]["pass"], canonical.get("category_tags", 0))
        self.assertEqual(report["counts"]["original_policy_matches"], 0)
        self.assertGreater(independent["yes_price"]["missing"], 0)
        self.assertGreater(independent["yes_price"]["malformed"], 0)
        self.assertGreater(canonical["lifecycle"], 0)

    def test_probe_selects_smallest_explicit_open_and_has_no_authority(self) -> None:
        adapter = build_offline_fixture_adapter()
        report = run_acceptance(
            adapter,
            AcceptanceConfig(page_limit=4, max_pages=2, max_seconds=30, max_books=2, book_depth=1),
            generated_at="2026-01-01T00:00:00+00:00",
        )
        candidate = report["candidate_outcome"]
        self.assertEqual(candidate["status"], "DATA_PIPELINE_PROBE")
        self.assertEqual(candidate["market_id"], "m-001")
        self.assertEqual(candidate["book_count"], 2)
        self.assertTrue(all(value is False for value in candidate["authority_flags"].values()))
        self.assertEqual(len([call for call in adapter.calls if call["path"] == "/book"]), 2)
        self.assertTrue(report["security"]["private_or_authenticated_state_accessed"] is False)


    def test_public_policy_match_selects_smallest_numeric_and_rejects_without_history(self) -> None:
        fixture = build_offline_fixture_adapter()
        first = dict(fixture.pages[0].snapshots[0])
        second = dict(first)
        first.update({"id": "12", "yes_mid": 0.50, "tokens": {"yes": "yes-12", "no": "no-12"}})
        second.update({"id": "3", "yes_mid": 0.50, "tokens": {"yes": "yes-3", "no": "no-3"}})
        adapter = OfflineFixtureAdapter(
            (
                OfflineDiscoveryPage(
                    snapshots=(first, second),
                    next_cursor=None,
                    raw_count=2,
                    unique_count=2,
                ),
            ),
            {"12": first, "3": second},
        )
        report = run_acceptance(
            adapter,
            AcceptanceConfig(
                page_limit=2,
                max_pages=1,
                max_seconds=30,
                max_books=2,
                book_depth=1,
                mode="public",
            ),
            generated_at="2026-01-01T00:00:00+00:00",
            include_queue_demo=False,
        )
        candidate = report["candidate_outcome"]
        self.assertEqual(candidate["status"], "ORIGINAL_POLICY_MATCH")
        self.assertTrue(candidate["scope_match"])
        self.assertEqual(candidate["market_id"], "3")
        self.assertEqual(candidate["selection_rule"], "smallest numeric market ID from observed policy matches")
        self.assertEqual(candidate["identity"]["id"], "3")
        self.assertEqual(candidate["token_ids"], {"yes": "yes-3", "no": "no-3"})
        self.assertEqual([item["outcome"] for item in candidate["books"]], ["yes", "no"])
        self.assertEqual(set(candidate["book_hashes"]), {"yes", "no"})
        self.assertEqual([call["path"] for call in adapter.calls], [
            "/tags/slug/politics",
            "/markets/keyset",
            "/markets/3",
            "/book",
            "/book",
        ])
        self.assertEqual([call["depth"] for call in adapter.calls if call["path"] == "/book"], [1, 1])
        self.assertFalse(candidate["authority_flags"]["lifecycle"])
        self.assertTrue(candidate["authority_flags"]["scope"])
        self.assertFalse(candidate["authority_flags"]["ranking"])
        self.assertFalse(candidate["authority_flags"]["qualification"])
        self.assertFalse(candidate["authority_flags"]["execution"])
        self.assertTrue(all(value is False for value in candidate["execution_flags"].values()))

        queue = report["public_queue_attempt"]
        self.assertEqual(queue["queue_status"], "REJECTED")
        self.assertEqual(queue["reason_code"], "INSUFFICIENT_DATA")
        self.assertIsNone(queue["lifecycle_stage"])
        self.assertEqual(queue["market_id"], candidate["market_id"])
        self.assertEqual(queue["identity_hash"], candidate["identity_hash"])
        self.assertEqual(queue["book_hashes"], candidate["book_hashes"])
        self.assertFalse(queue["missing_prerequisite"]["present"])
        self.assertFalse(queue["synthetic_offline"])
        self.assertTrue(all(value is False for value in queue["authority_flags"].values()))
        self.assertEqual(report["status_categories"]["candidate"], "ORIGINAL_POLICY_MATCH")
        self.assertEqual(report["status_categories"]["public_queue"], "REJECTED")
        self.assertEqual(report["runtime_qualification"]["evidence_class"], "SYNTHETIC_OFFLINE")
    def test_public_allowlist_has_no_private_or_order_paths(self) -> None:
        report = run_acceptance(
            build_offline_fixture_adapter(),
            AcceptanceConfig(page_limit=4, max_pages=2, max_seconds=30, max_books=2, book_depth=2),
            generated_at="2026-01-01T00:00:00+00:00",
        )
        self.assertEqual(report["security"]["forbidden_paths_observed"], [])
        self.assertEqual(report["security"]["observed_methods"], ["GET"])
        self.assertFalse(report["security"]["order_transport_called"])
        for path in report["security"]["public_get_allowlist"]:
            self.assertNotIn("order", path.lower())

    def test_queue_demo_processes_successor_with_runtime_defaults(self) -> None:
        predecessor_hash = "sha256:synthetic-frozen"
        result = enqueue_legacy_successor(
            "synthetic-predecessor",
            predecessor_hash,
            {"dataset_id": "fixture", "dataset_version": "v1", "row_count": 1},
        )
        self.assertTrue(result["isolated"])
        self.assertFalse(result["live_state_accessed"])
        self.assertEqual(result["predecessor_id"], "synthetic-predecessor")
        self.assertEqual(result["predecessor_frozen_hash"], predecessor_hash)
        self.assertEqual(result["lineage"], ["synthetic-predecessor", predecessor_hash])
        self.assertIn(result["queue_status"], {"COMPLETED", "REJECTED"})
        self.assertEqual(result["lifecycle_stage"], "FROZEN")
        self.assertEqual(result["resulting_stage"], "FROZEN")
        self.assertEqual(result["reason"], "CANDIDATE_FORWARD_MARKET_UNRESOLVED")
        self.assertEqual(result["exact_reason"], "CANDIDATE_FORWARD_MARKET_UNRESOLVED")
        self.assertIsNone(result["processor_reason"])
        self.assertEqual(result["processor_status"], "unsupported_by_validation")
        self.assertFalse(result["promotable"])
        self.assertTrue(result["runtime_reasons"])
        self.assertTrue(result["dataset_attestation"]["computed"])
        self.assertEqual(result["dataset_attestation"]["status"], "CURRENT")
        self.assertTrue(all(value is False for value in result["authority_flags"].values()))
        self.assertTrue(all(value is False for value in result["execution_flags"].values()))
    def test_public_pushdown_and_exact_tag_lookup_request(self) -> None:
        adapter = build_offline_fixture_adapter()
        report = run_acceptance(
            adapter,
            AcceptanceConfig(page_limit=4, max_pages=1, max_seconds=30, max_books=0, book_depth=1),
            generated_at="2026-01-01T00:00:00+00:00",
            include_queue_demo=False,
        )
        lookup = report["requests"]["tag_lookup"]
        self.assertEqual(lookup["path"], "/tags/slug/politics")
        self.assertEqual(lookup["query"], {})
        discovery = next(call for call in adapter.calls if call["path"] == "/markets/keyset")
        self.assertEqual(discovery["tag_ids"], (17,))
        self.assertEqual(discovery["liquidity_num_min"], 1000.0)
        self.assertEqual(discovery["end_date_min"], "2026-01-02T00:00:00+00:00")
        self.assertEqual(discovery["end_date_max"], "2026-01-08T00:00:00+00:00")
        self.assertEqual(report["requests"]["samples"][0]["query"]["tag_ids"], [17])

    def test_unknown_settlement_passes_only_with_explicit_open_lifecycle(self) -> None:
        adapter = build_offline_fixture_adapter()
        unknown = dict(adapter.pages[0].snapshots[0])
        unknown["settlement"] = "unknown"
        adapter.pages = (
            OfflineDiscoveryPage(
                snapshots=(unknown,),
                next_cursor=None,
                raw_count=1,
                unique_count=1,
                requested_at="2026-01-01T00:00:00+00:00",
            ),
        )
        adapter._cursor_to_index = {None: 0}
        report = run_acceptance(
            adapter,
            AcceptanceConfig(page_limit=1, max_pages=1, max_seconds=30, max_books=0, book_depth=1),
            generated_at="2026-01-01T00:00:00+00:00",
            include_queue_demo=False,
        )
        evaluation = report["bounded_samples"]["evaluations"][0]
        self.assertEqual(evaluation["conditions"]["lifecycle"], "pass")
        self.assertEqual(evaluation["canonical_first_failure"], "yes_price")
        self.assertEqual(report["candidate_outcome"]["status"], "DATA_PIPELINE_PROBE")

    def test_tag_lookup_failure_falls_back_without_inventing_tag_id(self) -> None:
        class TagLookupFailureAdapter(OfflineFixtureAdapter):
            def resolve_tag_slug(self, slug: str) -> int | None:
                self.calls.append({"method": "GET", "path": f"/tags/slug/{slug}", "query": {}})
                raise RuntimeError("fixture tag lookup failure")

        adapter = TagLookupFailureAdapter(build_offline_fixture_adapter().pages, build_offline_fixture_adapter().identities)
        report = run_acceptance(
            adapter,
            AcceptanceConfig(page_limit=1, max_pages=1, max_seconds=30, max_books=0, book_depth=1),
            generated_at="2026-01-01T00:00:00+00:00",
            include_queue_demo=False,
        )
        discovery = next(call for call in adapter.calls if call["path"] == "/markets/keyset")
        self.assertEqual(discovery["tag_ids"], ())
        self.assertIsNone(report["requests"]["tag_lookup"].get("tag_id"))
        self.assertIn("tag_lookup_fallback_broader_discovery", report["coverage"]["reasons"])

    def test_config_difference_table_enumerates_runtime_and_relaxed_criteria(self) -> None:
        report = run_acceptance(
            build_offline_fixture_adapter(),
            AcceptanceConfig(page_limit=1, max_pages=1, max_seconds=30, max_books=0, book_depth=1),
            generated_at="2026-01-01T00:00:00+00:00",
            include_queue_demo=False,
        )
        table = report["config_differences"]["promotion_criteria"]["relaxed_vs_runtime"]
        self.assertEqual(
            set(table),
            {
                "min_independent_samples",
                "min_trades",
                "max_drawdown",
                "min_expectancy",
                "min_confidence_lower_bound",
                "min_stability",
                "min_calibration",
                "min_liquidity",
                "min_forward_duration_seconds",
                "min_regimes",
                "min_resolved_bets_for_performance_rejection",
                "min_forward_duration_seconds_for_performance_rejection",
                "min_order_attempts_for_execution_rejection",
            },
        )
        self.assertEqual(table["min_trades"]["relaxed"], 0)
        self.assertEqual(table["min_trades"]["runtime"], 20)

    def test_canonical_policy_controls_pushdown_and_preserves_full_scope(self) -> None:
        adapter = build_offline_fixture_adapter()
        policy = {
            "schema_version": "1",
            "mode": "RULE_BASED_MARKETS",
            "instrument": "POLYMARKET",
            "categories": ["sports"],
            "market_ids": [],
            "filters": {"category": ["sports"], "min_liquidity": 2500.0},
            "regime_restrictions": {},
            "provenance": "canonical",
        }
        report = run_acceptance(
            adapter,
            AcceptanceConfig(page_limit=1, max_pages=1, max_seconds=30, max_books=0, book_depth=1),
            policy=policy,
            generated_at="2026-01-01T00:00:00+00:00",
            include_queue_demo=False,
        )
        discovery = next(call for call in adapter.calls if call["path"] == "/markets/keyset")
        self.assertEqual(report["scope"]["policy"]["mode"], "RULE_BASED_MARKETS")
        self.assertEqual(report["scope"]["policy"]["categories"], ["sports"])
        self.assertEqual(report["scope"]["policy"]["filters"]["min_liquidity"], 2500.0)
        self.assertEqual(discovery["tag_ids"], ())
        self.assertEqual(discovery["liquidity_num_min"], 2500.0)
        self.assertIsNone(discovery["end_date_min"])
        self.assertIsNone(discovery["end_date_max"])
    def test_public_queue_preserves_custom_canonical_scope_binding(self) -> None:
        adapter = build_offline_fixture_adapter()
        policy = {
            "schema_version": "1",
            "mode": "RULE_BASED_MARKETS",
            "instrument": "POLYMARKET",
            "categories": ["sports"],
            "market_ids": [],
            "filters": {
                "category": ["sports"],
                "min_liquidity": 1500.0,
                "max_spread": 0.05,
            },
            "regime_restrictions": {},
            "provenance": "canonical",
        }
        report = run_acceptance(
            adapter,
            AcceptanceConfig(
                page_limit=4,
                max_pages=1,
                max_seconds=30,
                max_books=2,
                book_depth=1,
                mode="public",
            ),
            policy=policy,
            generated_at="2026-01-01T00:00:00+00:00",
            include_queue_demo=False,
        )
        candidate = report["candidate_outcome"]
        queue = report["public_queue_attempt"]
        self.assertEqual(candidate["status"], "ORIGINAL_POLICY_MATCH")
        self.assertEqual(candidate["market_id"], "m-002")
        self.assertEqual(queue["queue_status"], "REJECTED")
        self.assertEqual(queue["reason_code"], "INSUFFICIENT_DATA")
        self.assertEqual(queue["market_id"], candidate["market_id"])
        self.assertEqual(queue["market_scope"], report["scope"]["policy"])
        self.assertEqual(queue["market_scope"]["filters"]["min_liquidity"], 1500.0)
        self.assertEqual(queue["market_scope_hash"], report["scope_hash"])
        self.assertEqual(queue["market_scope_version"], "1")
        self.assertNotIn("scope_hash", queue)

    def test_filter_policy_match_matches_canonical_processor_for_scalar_and_list_values(self) -> None:
        base_policy = {
            "schema_version": "1",
            "mode": "RULE_BASED_MARKETS",
            "instrument": "POLYMARKET",
            "categories": ["sports"],
            "market_ids": [],
            "regime_restrictions": {},
            "provenance": "canonical",
        }
        market = dict(build_offline_fixture_adapter().pages[0].snapshots[1])
        canonical_market = dict(market)
        canonical_market["market_id"] = canonical_market["id"]
        if "spread" not in canonical_market:
            canonical_market["spread"] = canonical_market["yes_ask"] - canonical_market["yes_bid"]
        canonical_plan = SimpleNamespace(
            filters=None,
            regime_restrictions={},
            target_markets=(),
            target_instrument="POLYMARKET",
            market_type=MarketType.PREDICTION,
        )
        cases = (
            ("category", "category_tags", "sports", True),
            ("category", "category_tags", ["sports"], True),
            ("min_liquidity", "liquidity", 1_500.0, True),
            ("min_liquidity", "liquidity", [1_500.0], False),
            ("max_spread", "spread", 0.05, True),
            ("max_spread", "spread", [0.05], False),
            ("minimum_hours_to_resolution", "expiry", 24.0, True),
            ("minimum_hours_to_resolution", "expiry", [24.0], True),
            ("maximum_hours_to_resolution", "expiry", 168.0, True),
            ("maximum_hours_to_resolution", "expiry", [168.0], True),
        )

        with AxiomStore(":memory:") as store:
            processor = AutonomousResearchProcessor(store)
            for filter_name, condition_name, filter_value, expected_match in cases:
                with self.subTest(filter_name=filter_name, filter_value=filter_value):
                    filters = {"category": ["sports"], filter_name: filter_value}
                    policy = dict(base_policy, filters=filters)
                    canonical_plan.filters = filters
                    canonical_match = bool(processor._apply_plan_filters(canonical_plan, [canonical_market]))
                    self.assertEqual(canonical_match, expected_match)
                    report = run_acceptance(
                        build_offline_fixture_adapter(),
                        AcceptanceConfig(page_limit=3, max_pages=1, max_seconds=30, max_books=0, book_depth=1),
                        policy=policy,
                        generated_at="2026-01-01T00:00:00+00:00",
                        include_queue_demo=False,
                    )
                    evaluation = next(
                        item
                        for item in report["bounded_samples"]["evaluations"]
                        if item["market_id"] == "m-002"
                    )
                    self.assertEqual(evaluation["policy_match"], canonical_match)
                    if isinstance(filter_value, list) and filter_name in {"min_liquidity", "max_spread"}:
                        self.assertEqual(evaluation["conditions"][condition_name], "fail")



    def test_page_error_cursor_type_and_provider_duplicates_are_honest(self) -> None:
        page = OfflineDiscoveryPage(
            snapshots=({"conditionId": "m-001"}, {"question": "missing-a"}, {"question": "missing-b"}),
            next_cursor=17,
            raw_count=3,
            unique_count=2,
            duplicate_count=4,
            coverage_status="ERROR",
            error_reason="provider_fixture_error",
        )
        adapter = OfflineFixtureAdapter((page,), {"m-001": {"id": "m-001"}})
        report = run_acceptance(
            adapter,
            AcceptanceConfig(page_limit=3, max_pages=1, max_seconds=30, max_books=0, book_depth=1),
            generated_at="2026-01-01T00:00:00+00:00",
            include_queue_demo=False,
        )
        self.assertEqual(report["coverage"]["status"], "ERROR")
        self.assertEqual(report["coverage"]["page_statuses"], ["ERROR"])
        self.assertIn("provider_fixture_error", report["coverage"]["reasons"])
        self.assertEqual(report["coverage"]["provider_duplicate_rows"], 4)
        self.assertEqual(report["pagination"]["opaque_cursor_only"], False)

    def test_lifecycle_rejects_missing_flags_resolved_unknown_and_boolean_numbers(self) -> None:
        adapter = build_offline_fixture_adapter()
        snapshot = dict(adapter.pages[0].snapshots[0])
        snapshot["yes_mid"] = True
        snapshot["settlement"] = "unknown"
        snapshot["resolved"] = False
        snapshot.pop("archived")
        adapter.pages = (OfflineDiscoveryPage(snapshots=(snapshot,), next_cursor=None, raw_count=1),)
        adapter._cursor_to_index = {None: 0}
        report = run_acceptance(
            adapter,
            AcceptanceConfig(page_limit=1, max_pages=1, max_seconds=30, max_books=0, book_depth=1),
            generated_at="2026-01-01T00:00:00+00:00",
            include_queue_demo=False,
        )
        evaluation = report["bounded_samples"]["evaluations"][0]
        self.assertEqual(evaluation["conditions"]["lifecycle"], "fail")
        self.assertEqual(evaluation["conditions"]["yes_price"], "malformed")

    def test_malformed_discovery_path_fails_closed_without_private_call(self) -> None:
        page = OfflineDiscoveryPage(
            snapshots=({"id": "m-001", "question": "fixture", "active": True, "closed": False},),
            next_cursor=None,
            request_path="/orders",
            raw_count=1,
            unique_count=1,
        )
        adapter = OfflineFixtureAdapter((page,), {"m-001": {"id": "m-001"}})
        report = run_acceptance(
            adapter,
            AcceptanceConfig(page_limit=1, max_pages=1, max_seconds=30, max_books=0, book_depth=1),
            generated_at="2026-01-01T00:00:00+00:00",
        )
        self.assertEqual(report["coverage"]["status"], "ERROR")
        self.assertIn("request_path_not_allowlisted", report["coverage"]["reasons"])
        self.assertEqual([call for call in adapter.calls if call["path"] == "/orders"], [])
        self.assertEqual(report["security"]["forbidden_paths_observed"], ["/orders"])

    def test_report_publishes_queue_successor_lineage_attestation_and_outcome(self) -> None:
        report = run_acceptance(
            build_offline_fixture_adapter(),
            AcceptanceConfig(page_limit=1, max_pages=1, max_seconds=30, max_books=0, book_depth=1),
            generated_at="2026-01-01T00:00:00+00:00",
        )
        queue_demo = report["queue_demo"]
        self.assertEqual(queue_demo["lineage"], ["synthetic-predecessor-v1", queue_demo["predecessor_frozen_hash"]])
        self.assertIn(queue_demo["queue_status"], {"COMPLETED", "REJECTED"})
        self.assertIn("lifecycle_stage", queue_demo)
        self.assertIn("resulting_stage", queue_demo)
        self.assertIn("exact_reason", queue_demo)
        self.assertEqual(queue_demo["dataset_attestation"]["status"], "CURRENT")
        self.assertEqual(report["later_public_workflow"]["successor_outcome"], queue_demo)

    def test_budget_exhaustion_precedes_every_public_request(self) -> None:
        adapter = build_offline_fixture_adapter()
        ticks = iter((0.0, 1.0))
        report = run_acceptance(
            adapter,
            AcceptanceConfig(page_limit=1, max_pages=2, max_seconds=1, max_books=2, book_depth=1),
            generated_at="2026-01-01T00:00:00+00:00",
            monotonic=lambda: next(ticks),
            include_queue_demo=False,
        )
        self.assertEqual(report["coverage"]["status"], "BUDGET_EXHAUSTED")
        self.assertEqual(report["requests"]["tag_lookup"]["status"], "BUDGET_EXHAUSTED")
        self.assertEqual(report["requests"]["samples"], [])
        self.assertEqual(adapter.calls, [])

    def test_total_budget_skips_queue_demo_explicitly(self) -> None:
        adapter = build_offline_fixture_adapter()
        calls = 0

        def monotonic() -> float:
            nonlocal calls
            calls += 1
            return 0.0 if calls < 7 else 2.0

        report = run_acceptance(
            adapter,
            AcceptanceConfig(page_limit=1, max_pages=1, max_seconds=1, max_books=0, book_depth=1),
            generated_at="2026-01-01T00:00:00+00:00",
            monotonic=monotonic,
        )
        self.assertEqual(report["queue_demo"]["queue_status"], "SKIPPED_BUDGET")
        self.assertEqual(report["queue_demo"]["exact_reason"], "total_time_budget_exhausted")
        self.assertEqual(report["later_public_workflow"]["successor_outcome"], report["queue_demo"])
        self.assertIn("queue_time_budget_exhausted", report["coverage"]["reasons"])

    def test_public_polymarket_context_infers_instrument_but_unknown_source_does_not(self) -> None:
        snapshot = dict(build_offline_fixture_adapter().pages[0].snapshots[0])
        snapshot.pop("instrument")
        snapshot["source"] = "polymarket"

        class PolymarketFixtureAdapter(OfflineFixtureAdapter):
            provider_name = "polymarket"

        adapter = PolymarketFixtureAdapter(
            (OfflineDiscoveryPage(snapshots=(snapshot,), next_cursor=None, raw_count=1),),
            {"m-001": snapshot},
        )
        report = run_acceptance(
            adapter,
            AcceptanceConfig(
                page_limit=1,
                max_pages=1,
                max_seconds=30,
                max_books=0,
                book_depth=1,
                mode="public",
            ),
            generated_at="2026-01-01T00:00:00+00:00",
            include_queue_demo=False,
        )
        evaluation = report["bounded_samples"]["evaluations"][0]
        self.assertEqual(evaluation["conditions"]["instrument"], "pass")
        self.assertEqual(evaluation["canonical_first_failure"], "yes_price")

        unknown = dict(snapshot)
        unknown["source"] = "offline-fixture"
        unknown_adapter = OfflineFixtureAdapter(
            (OfflineDiscoveryPage(snapshots=(unknown,), next_cursor=None, raw_count=1),),
            {"m-001": unknown},
        )
        unknown_report = run_acceptance(
            unknown_adapter,
            AcceptanceConfig(page_limit=1, max_pages=1, max_seconds=30, max_books=0, book_depth=1),
            generated_at="2026-01-01T00:00:00+00:00",
            include_queue_demo=False,
        )
        unknown_evaluation = unknown_report["bounded_samples"]["evaluations"][0]
        self.assertEqual(unknown_evaluation["conditions"]["instrument"], "missing")
        self.assertEqual(unknown_evaluation["canonical_first_failure"], "instrument_missing")

    def test_generic_object_snapshots_preserve_instrument_and_regime(self) -> None:
        snapshot = dict(build_offline_fixture_adapter().pages[0].snapshots[0])
        snapshot["instrument"] = "POLYMARKET"
        snapshot["regime"] = "calm"
        object_snapshot = SimpleNamespace(**snapshot)
        adapter = OfflineFixtureAdapter(
            (OfflineDiscoveryPage(snapshots=(object_snapshot,), next_cursor=None, raw_count=1),),
            {"m-001": snapshot},
        )
        report = run_acceptance(
            adapter,
            AcceptanceConfig(page_limit=1, max_pages=1, max_seconds=30, max_books=0, book_depth=1),
            policy={
                "schema_version": "1",
                "mode": "RULE_BASED_MARKETS",
                "instrument": "POLYMARKET",
                "categories": ["politics"],
                "market_ids": [],
                "filters": {"category": ["politics"]},
                "regime_restrictions": {"regimes": ["calm"]},
                "provenance": "canonical",
            },
            generated_at="2026-01-01T00:00:00+00:00",
            include_queue_demo=False,
        )
        conditions = report["bounded_samples"]["evaluations"][0]["conditions"]
        self.assertEqual(conditions["instrument"], "pass")
        self.assertEqual(conditions["regime_restrictions"], "pass")

    def test_one_sided_expiry_bounds_are_evaluated(self) -> None:
        for bound_name, bound_value in (
            ("minimum_hours_to_resolution", 24.0),
            ("maximum_hours_to_resolution", 168.0),
        ):
            with self.subTest(bound_name=bound_name):
                report = run_acceptance(
                    build_offline_fixture_adapter(),
                    AcceptanceConfig(page_limit=1, max_pages=1, max_seconds=30, max_books=0, book_depth=1),
                    policy={
                        "schema_version": "1",
                        "mode": "RULE_BASED_MARKETS",
                        "instrument": "POLYMARKET",
                        "categories": ["politics"],
                        "market_ids": [],
                        "filters": {"category": ["politics"], bound_name: bound_value},
                        "regime_restrictions": {},
                        "provenance": "canonical",
                    },
                    generated_at="2026-01-01T00:00:00+00:00",
                    include_queue_demo=False,
                )
                evaluation = report["bounded_samples"]["evaluations"][0]
                self.assertEqual(evaluation["conditions"]["expiry"], "pass")

    def test_nested_public_object_temporals_are_json_safe(self) -> None:
        adapter = build_offline_fixture_adapter()
        adapter.identities["m-001"]["public_metadata"] = SimpleNamespace(
            observed_at=datetime(2026, 1, 1, 5, 0, tzinfo=timezone(timedelta(hours=5))),
            observed_on=date(2026, 1, 2),
            cutoff_at=time(12, 34, 56, 789000),
            numeric_value=123.45,
            text_value="public-value",
        )
        report = run_acceptance(
            adapter,
            AcceptanceConfig(page_limit=1, max_pages=1, max_seconds=30, max_books=0, book_depth=1),
            generated_at="2026-01-01T00:00:00+00:00",
            include_queue_demo=False,
        )

        metadata = report["candidate_outcome"]["identity"]["public_metadata"]
        json.dumps(report, sort_keys=True)
        self.assertEqual(metadata["observed_at"], "2026-01-01T00:00:00+00:00")
        self.assertEqual(metadata["observed_on"], "2026-01-02")
        self.assertEqual(metadata["cutoff_at"], "12:34:56.789000")
        self.assertEqual(metadata["numeric_value"], 123.45)
        self.assertEqual(metadata["text_value"], "public-value")

    def test_identity_counts_reconcile_without_provider_double_counting(self) -> None:
        first = dict(build_offline_fixture_adapter().pages[0].snapshots[0])
        page = OfflineDiscoveryPage(
            snapshots=(first, first, {"question": "missing-a"}, {"question": "missing-b"}),
            next_cursor=None,
            raw_count=4,
            unique_count=1,
            duplicate_count=1,
            malformed_count=2,
        )
        adapter = OfflineFixtureAdapter((page,), {first["id"]: first})
        report = run_acceptance(
            adapter,
            AcceptanceConfig(page_limit=4, max_pages=1, max_seconds=30, max_books=0, book_depth=1),
            generated_at="2026-01-01T00:00:00+00:00",
            include_queue_demo=False,
        )
        coverage = report["coverage"]
        self.assertEqual(coverage["raw_rows"], 4)
        self.assertEqual(coverage["unique_rows"], 1)
        self.assertEqual(coverage["malformed_rows"], 2)
        self.assertEqual(coverage["duplicate_rows"], 1)
        self.assertEqual(coverage["provider_malformed_rows"], 2)
        self.assertEqual(
            coverage["raw_rows"],
            coverage["unique_rows"] + coverage["malformed_rows"] + coverage["duplicate_rows"],
        )
        self.assertTrue(coverage["identity_reconciliation"]["reconciles"])

    def test_canonical_instrument_and_regime_restrictions_are_local_conditions(self) -> None:
        adapter = build_offline_fixture_adapter()
        snapshot = dict(adapter.pages[0].snapshots[0])
        snapshot["instrument"] = "POLYMARKET"
        snapshot["regime"] = "volatile"
        adapter.pages = (OfflineDiscoveryPage(snapshots=(snapshot,), next_cursor=None, raw_count=1),)
        adapter._cursor_to_index = {None: 0}
        report = run_acceptance(
            adapter,
            AcceptanceConfig(page_limit=1, max_pages=1, max_seconds=30, max_books=0, book_depth=1),
            policy={
                "schema_version": "1",
                "mode": "RULE_BASED_MARKETS",
                "instrument": "POLYMARKET",
                "categories": ["politics"],
                "market_ids": [],
                "filters": {"category": ["politics"]},
                "regime_restrictions": {"regimes": ["calm"]},
                "provenance": "canonical",
            },
            generated_at="2026-01-01T00:00:00+00:00",
            include_queue_demo=False,
        )
        conditions = report["bounded_samples"]["evaluations"][0]["conditions"]
        self.assertEqual(conditions["instrument"], "pass")
        self.assertEqual(conditions["regime_restrictions"], "fail")

    def test_duplicate_counts_add_provider_within_page_and_cross_page_once(self) -> None:
        first = dict(build_offline_fixture_adapter().pages[0].snapshots[0])
        second = dict(build_offline_fixture_adapter().pages[0].snapshots[1])
        adapter = OfflineFixtureAdapter(
            (
                OfflineDiscoveryPage(
                    snapshots=(first, first),
                    next_cursor="fixture-cursor-1",
                    raw_count=2,
                    unique_count=1,
                    duplicate_count=1,
                ),
                OfflineDiscoveryPage(
                    snapshots=(first, second),
                    next_cursor=None,
                    raw_count=2,
                    unique_count=2,
                    duplicate_count=1,
                ),
            ),
            {first["id"]: first, second["id"]: second},
        )
        report = run_acceptance(
            adapter,
            AcceptanceConfig(page_limit=2, max_pages=2, max_seconds=30, max_books=0, book_depth=1),
            generated_at="2026-01-01T00:00:00+00:00",
            include_queue_demo=False,
        )
        coverage = report["coverage"]
        self.assertEqual(coverage["provider_within_page_duplicate_rows"], 0)
        self.assertEqual(coverage["observed_within_page_duplicate_rows"], 1)
        self.assertEqual(coverage["cross_page_duplicate_rows"], 1)
        self.assertEqual(coverage["duplicate_rows"], 2)

    def test_sensitive_scalar_variants_are_redacted_but_public_token_ids_remain(self) -> None:
        adapter = build_offline_fixture_adapter()
        adapter.identities["m-001"].update(
            {
                "api_token": "API-TOKEN-SENTINEL",
                "ApiToken": "CAMEL-TOKEN-SENTINEL",
                "token": "TOKEN-SENTINEL",
                "secretKey": "SECRET-KEY-SENTINEL",
                "password": "PASSWORD-SENTINEL",
                "auth": "AUTH-SENTINEL",
                "authorization": "AUTHORIZATION-SENTINEL",
                "auth_header": "AUTH-HEADER-SENTINEL",
                "signature": "SIGNATURE-SENTINEL",
                "private_data": "PRIVATE-DATA-SENTINEL",
                "private_state": "PRIVATE-STATE-SENTINEL",
            }
        )
        report = run_acceptance(
            adapter,
            AcceptanceConfig(page_limit=1, max_pages=1, max_seconds=30, max_books=0, book_depth=1),
            generated_at="2026-01-01T00:00:00+00:00",
            include_queue_demo=False,
        )
        text = json.dumps(report, sort_keys=True)
        for sentinel in (
            "API-TOKEN-SENTINEL",
            "CAMEL-TOKEN-SENTINEL",
            "TOKEN-SENTINEL",
            "SECRET-KEY-SENTINEL",
            "PASSWORD-SENTINEL",
            "AUTH-SENTINEL",
            "AUTHORIZATION-SENTINEL",
            "AUTH-HEADER-SENTINEL",
            "SIGNATURE-SENTINEL",
            "PRIVATE-DATA-SENTINEL",
            "PRIVATE-STATE-SENTINEL",
        ):
            self.assertNotIn(sentinel, text)
        self.assertEqual(report["candidate_outcome"]["identity"]["tokens"]["yes"], "yes-m-001")

    def test_malformed_resolved_flag_fails_lifecycle(self) -> None:
        adapter = build_offline_fixture_adapter()
        snapshot = dict(adapter.pages[0].snapshots[0])
        snapshot["resolved"] = "indeterminate"
        adapter.pages = (OfflineDiscoveryPage(snapshots=(snapshot,), next_cursor=None, raw_count=1),)
        adapter._cursor_to_index = {None: 0}
        report = run_acceptance(
            adapter,
            AcceptanceConfig(page_limit=1, max_pages=1, max_seconds=30, max_books=0, book_depth=1),
            generated_at="2026-01-01T00:00:00+00:00",
            include_queue_demo=False,
        )
        self.assertEqual(report["bounded_samples"]["evaluations"][0]["conditions"]["lifecycle"], "fail")

    def test_cli_writes_requested_bounded_report(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "acceptance.json"
            result = main(
                [
                    "--mode",
                    "offline",
                    "--max-pages",
                    "1",
                    "--max-books",
                    "0",
                    "--book-depth",
                    "1",
                    "--output",
                    str(output),
                ]
            )
            self.assertEqual(result, 0)
            payload = json.loads(output.read_text(encoding="utf-8"))
            self.assertEqual(payload["config"]["max_pages"], 1)
            self.assertLessEqual(payload["pagination"]["pages_observed"], 1)

    def test_persisted_pinned_identity_counts_and_attestation_are_public_contract(self) -> None:
        self.assertEqual(POLYMARKET_HISTORICAL_DATASET_ID, "Polymarket-historical")
        self.assertEqual(
            POLYMARKET_HISTORICAL_DATASET_VERSION,
            "sha256:9a831357e3f4016ad2c4f05d9bb11a98583ad94a2da36873b56e2b7db786a9ea",
        )
        self.assertEqual(POLYMARKET_HISTORICAL_ROW_COUNT, 18_141)
        self.assertEqual(POLYMARKET_HISTORICAL_CONSTITUENT_COUNT, 1_000)
        self.assertEqual(
            POLYMARKET_HISTORICAL_ATTESTATION_HASH,
            "sha256:24e279b36735f5c6fb97a6115281d681fbaa3afd08a64c982c36cd929b9d93a3",
        )
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "missing-source.json"
            result = main(
                [
                    "--mode",
                    "persisted",
                    "--source-backup",
                    str(Path(directory) / "does-not-exist.sqlite"),
                    "--output",
                    str(output),
                ]
            )
            payload = json.loads(output.read_text(encoding="utf-8"))
            self.assertNotEqual(result, 0)
            self.assertEqual(payload["config"]["dataset_id"], POLYMARKET_HISTORICAL_DATASET_ID)
            self.assertEqual(payload["config"]["dataset_version"], POLYMARKET_HISTORICAL_DATASET_VERSION)
            self.assertEqual(payload["config"]["expected_row_count"], POLYMARKET_HISTORICAL_ROW_COUNT)
            self.assertEqual(
                payload["config"]["expected_constituent_count"],
                POLYMARKET_HISTORICAL_CONSTITUENT_COUNT,
            )
            self.assertIn("SOURCE_BACKUP_NOT_FOUND", payload["validation"]["reasons"])
            control = payload["no_edge_control"]
            self.assertEqual(control["evaluation_status"], "NOT_RUN")
            self.assertIsNone(control["proven_zero_edge"])
            self.assertIsNone(control["metrics"])
            self.assertIsNone(control["proof"]["nonzero_edge_count"])



    def test_persisted_config_and_cli_refuse_alternate_identity_or_counts(self) -> None:
        with self.assertRaises(ValueError):
            AcceptanceConfig(
                mode="persisted",
                source_backup=PINNED_SOURCE,
                dataset_id="alternate-dataset",
                dataset_version=POLYMARKET_HISTORICAL_DATASET_VERSION,
            )
        with self.assertRaises(ValueError):
            AcceptanceConfig(
                mode="persisted",
                source_backup=PINNED_SOURCE,
                dataset_id=POLYMARKET_HISTORICAL_DATASET_ID,
                dataset_version=POLYMARKET_HISTORICAL_DATASET_VERSION,
                expected_row_count=1,
            )
        with self.assertRaises(ValueError):
            main(
                [
                    "--mode",
                    "persisted",
                    "--source-backup",
                    str(PINNED_SOURCE),
                    "--dataset-id",
                    "alternate-dataset",
                    "--dataset-version",
                    POLYMARKET_HISTORICAL_DATASET_VERSION,
                ]
            )

    def test_persisted_live_path_symlink_and_hardlink_are_rejected(self) -> None:
        if not LIVE_SOURCE.exists():
            self.skipTest("live runtime database is not present")
        candidates: list[Path] = [LIVE_SOURCE]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            symlink = root / "live-symlink.sqlite"
            try:
                os.symlink(LIVE_SOURCE, symlink)
            except (OSError, NotImplementedError):
                pass
            else:
                candidates.append(symlink)
            hardlink = root / "live-hardlink.sqlite"
            try:
                os.link(LIVE_SOURCE, hardlink)
            except (OSError, NotImplementedError):
                pass
            else:
                candidates.append(hardlink)
            for candidate in candidates:
                with self.subTest(source=candidate.name):
                    report = run_persisted_acceptance(
                        candidate,
                        POLYMARKET_HISTORICAL_DATASET_ID,
                        POLYMARKET_HISTORICAL_DATASET_VERSION,
                        generated_at=COMPACT_TIMESTAMP,
                    )
                    self.assertFalse(report["validation"]["passed"])
                    self.assertTrue(report["security"]["source_live_database_rejected"])
                    self.assertIn("SOURCE_IS_LIVE_DATABASE", report["validation"]["reasons"])
                    self.assertEqual(report["chain"]["queue_status"], "NOT_RUN")

    def test_persisted_source_pre_post_hash_change_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "source.sqlite"
            calls: list[Path] = []

            def changing_hash(candidate: Path) -> str:
                digest = "sha256:" + hashlib.sha256(candidate.read_bytes()).hexdigest()
                if calls:
                    return digest + "-changed"
                calls.append(candidate)
                return digest

            with patch(
                "tools.polymarket_market_scope_acceptance._persisted_hash_file",
                side_effect=changing_hash,
            ):
                report = _compact_report(path)
            source = report["source"]
            self.assertEqual(report["validation"]["status"], "FAILED")
            self.assertIn("SOURCE_CHANGED_DURING_READ_TRANSACTION", report["validation"]["reasons"])
            self.assertIsNotNone(source["source_stat_pre"])
            self.assertIsNotNone(source["source_stat_post"])
            self.assertNotEqual(source["source_hash_pre"], source["source_hash_post"])

    def test_persisted_alias_conflict_and_missing_fields_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            alias_path = Path(directory) / "alias.sqlite"
            _build_compact_persisted_source(alias_path)
            _rewrite_compact_record(alias_path, yes_mid=0.6)
            alias_report = _compact_report(alias_path)
            self.assertFalse(alias_report["validation"]["passed"])
            self.assertIn("RECORD_ALIAS_CONFLICT:yes_mid", alias_report["validation"]["reasons"])

            provenance_path = Path(directory) / "provenance.sqlite"
            _build_compact_persisted_source(provenance_path)
            _rewrite_compact_aggregate_metadata(provenance_path, provider=_MISSING)
            provenance_report = _compact_report(provenance_path)
            self.assertFalse(provenance_report["validation"]["passed"])
            self.assertIn("DATASET_PROVENANCE_INVALID", provenance_report["validation"]["reasons"])

            field_path = Path(directory) / "field.sqlite"
            _build_compact_persisted_source(field_path)
            connection = sqlite3.connect(field_path)
            try:
                row = connection.execute(
                    "SELECT payload_json FROM datasets WHERE dataset_id=?",
                    (f"prediction:{COMPACT_MARKET_ID}",),
                ).fetchone()
                assert row is not None
                records = json.loads(row[0])
                records[0].pop("token_id", None)
                connection.execute(
                    "UPDATE datasets SET payload_json=? WHERE dataset_id=?",
                    (
                        json.dumps(records, sort_keys=True, separators=(",", ":")),
                        f"prediction:{COMPACT_MARKET_ID}",
                    ),
                )
                connection.commit()
            finally:
                connection.close()
            field_report = _compact_report(field_path)
            self.assertFalse(field_report["validation"]["passed"])
            self.assertIn("TOKEN_ID_INVALID", field_report["validation"]["reasons"])

    def test_persisted_missing_source_type_on_catalogs_fails_closed(self) -> None:
        mutations = {
            "aggregate": lambda path: _rewrite_compact_catalog_source_type(path),
            "constituent_catalog": lambda path: _rewrite_compact_catalog_source_type(
                path,
                dataset_id=f"prediction:{COMPACT_MARKET_ID}",
            ),
        }
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for boundary, mutate in mutations.items():
                with self.subTest(boundary=boundary):
                    path = root / f"missing-{boundary}.sqlite"
                    _build_compact_persisted_source(path)
                    mutate(path)
                    report = _compact_report(path)
                    self.assertFalse(report["validation"]["passed"])
                    self.assertIn("DATASET_PROVENANCE_INVALID", report["validation"]["reasons"])
                    self.assertEqual(report["export"]["status"], "NOT_RUN")

    def test_persisted_missing_record_source_type_binds_to_exact_attested_catalog(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "missing-record-source.sqlite"
            _build_compact_persisted_source(path, include_record_source_type=False)
            report = _compact_report(path)
        self.assertTrue(report["validation"]["passed"])
        provenance = report["validation"]["record_provenance"]
        self.assertEqual(provenance["explicit_source_type_rows"], 0)
        self.assertEqual(provenance["catalog_bound_source_type_rows"], 1)
        self.assertTrue(provenance["attestation_valid"])
        authority = report["source"]["constituent_evidence"]["sample"][0]["provenance_authority"]
        self.assertEqual(authority["source"], "constituent_catalog")
        self.assertEqual(authority["source_type"], "HISTORICAL")
        self.assertEqual(authority["attestation_status"], "CURRENT")
        self.assertEqual(authority["attestation_contamination_result"], "PASS")

    def test_persisted_missing_record_market_id_inherits_exact_attested_catalog_identity(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "missing-record-market-id.sqlite"
            _build_compact_persisted_source(path, include_record_market_id=False)
            report = _compact_report(path)

        self.assertTrue(report["validation"]["passed"])
        source_record = report["source"]["constituent_evidence"]["sample"][0]["record_sample"][0]
        exported_record = report["export"]["evaluated_record_sample"][0]
        self.assertEqual(source_record["market_id"], COMPACT_MARKET_ID)
        self.assertEqual(exported_record["market_id"], COMPACT_MARKET_ID)
        processor_result = report["chain"]["processor"]["results"][0]
        self.assertEqual(processor_result["reason_code"], "INSUFFICIENT_DATA")
        self.assertNotIn("resolved contract market_id is required", str(processor_result.get("reason", "")))

    def test_persisted_explicit_matching_market_id_is_retained_in_export_and_hash(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            report = _compact_report(Path(directory) / "explicit-record-market-id.sqlite")

        exported_record = report["export"]["evaluated_record_sample"][0]
        self.assertEqual(exported_record["market_id"], COMPACT_MARKET_ID)
        self.assertEqual(report["export"]["evaluated_records_hash"], _sha256_json([exported_record]))
        constituent_record = report["source"]["constituent_evidence"]["sample"][0]["record_sample"][0]
        self.assertEqual(constituent_record["market_id"], COMPACT_MARKET_ID)

    def test_persisted_conflicting_record_market_id_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "conflicting-record-market-id.sqlite"
            _build_compact_persisted_source(path)
            _rewrite_compact_record(path, market_id="different-market")
            report = _compact_report(path)

        self.assertFalse(report["validation"]["passed"])
        self.assertIn("MARKET_ID_INVALID", report["validation"]["reasons"])
        self.assertEqual(report["export"]["status"], "NOT_RUN")

    def test_persisted_missing_record_source_without_current_attestation_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "missing-record-attestation.sqlite"
            _build_compact_persisted_source(path, include_record_source_type=False)
            _compact_mutate(
                path,
                "UPDATE dataset_integrity_attestation SET status=?",
                ("STALE",),
            )
            report = _compact_report(path)
        self.assertFalse(report["validation"]["passed"])
        self.assertIn("ATTESTATION_INVALID", report["validation"]["reasons"])
        self.assertEqual(report["export"]["status"], "NOT_RUN")

    def test_persisted_forward_source_is_rejected_not_relabelled(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "forward.sqlite"
            _build_compact_persisted_source(path)
            _rewrite_compact_record(path, source_type="FORWARD_COLLECTED")
            report = _compact_report(path)
            self.assertFalse(report["validation"]["passed"])
            self.assertIn("FORWARD_CURRENT_RELABEL_REJECTED", report["validation"]["reasons"])
            self.assertTrue(report["security"]["forward_relabel_rejected"])
            self.assertEqual(report["export"]["status"], "NOT_RUN")

    def test_persisted_attestation_revalidation_and_isolated_export_are_faithful(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "valid.sqlite"
            report = _compact_report(path)
            validation = report["validation"]
            source = report["source"]
            self.assertEqual(validation["status"], "PASSED")
            self.assertTrue(validation["passed"])
            self.assertEqual(source["dataset_id"], COMPACT_DATASET_ID)
            self.assertEqual(source["dataset_version"], "compact-v1")
            self.assertEqual(source["constituents"], {"catalog_count": 1, "binding_count": 1, "row_count": 1})
            self.assertEqual(source["source_hash_pre"], source["source_hash_post"])
            self.assertEqual(
                validation["attestation"]["attestation_hash"],
                source["attestation"]["attestation_hash"],
            )
            self.assertEqual(validation["attestation"]["status"], "CURRENT")
            self.assertEqual(validation["attestation"]["contamination_result"], "PASS")
            self.assertEqual(report["export"]["status"], "PASSED")
            self.assertTrue(report["export"]["isolated"])
            self.assertTrue(report["export"]["temporary_store"])
            self.assertTrue(report["export"]["controls"]["disabled_or_absent"])
            self.assertIs(report["export"]["controls"]["credential_state_exported"], False)
            self.assertTrue(report["security"]["controls_disabled_or_absent"])
            self.assertFalse(report["security"]["credentials_used"])
            self.assertFalse(report["security"]["network_calls"])
            self.assertFalse(report["security"]["order_transport_called"])
            self.assertFalse(report["security"]["submit_order_called"])
            self.assertIsNotNone(report["export_hash"])
            self.assertIsNotNone(report["export"]["evaluated_records_hash"])
            self.assertEqual(
                report["export"]["core_integrity_verification"]["attestation_hash"],
                validation["attestation"]["attestation_hash"],
            )
            self.assertEqual(report["export"]["evaluated_records_hash"], report["export"]["evaluated_records_hash"])
            self.assertEqual(report["proposal"]["dataset_id"], COMPACT_DATASET_ID)
            self.assertEqual(report["proposal"]["dataset_version"], "compact-v1")

            stale_path = Path(directory) / "stale.sqlite"
            _build_compact_persisted_source(stale_path)
            _compact_mutate(
                stale_path,
                "UPDATE dataset_integrity_attestation SET status=?, attestation_hash=?",
                ("CURRENT", "sha256:stale"),
            )
            stale_report = _compact_report(stale_path)
            self.assertFalse(stale_report["validation"]["passed"])
            self.assertIn("ATTESTATION_INVALID", stale_report["validation"]["reasons"])

    def test_persisted_unknown_metadata_is_rejected_without_export_but_provenance_remains(self) -> None:
        sentinels = {
            "control_payload": "CONTROL-METADATA-SENTINEL",
            "order_payload": "ORDER-METADATA-SENTINEL",
            "wallet_payload": "WALLET-METADATA-SENTINEL",
            "balance_payload": "BALANCE-METADATA-SENTINEL",
            "credential_payload": "CREDENTIAL-METADATA-SENTINEL",
        }
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "metadata.sqlite"
            _build_compact_persisted_source(path)
            _rewrite_compact_aggregate_metadata(path, **sentinels)
            _rewrite_compact_aggregate_metadata(
                path,
                dataset_id=f"prediction:{COMPACT_MARKET_ID}",
                **sentinels,
            )
            report = _compact_report(path)

        self.assertEqual(report["validation"]["status"], "FAILED")
        self.assertFalse(report["validation"]["passed"])
        self.assertIn("ATTESTATION_INVALID", report["validation"]["reasons"])
        self.assertEqual(report["export"]["status"], "NOT_RUN")
        final_report_json = json.dumps(report, sort_keys=True, separators=(",", ":"))
        for sentinel in sentinels.values():
            self.assertNotIn(sentinel, final_report_json)

        metadata_sections = (
            report["source"]["catalog"]["metadata"],
            report["source"]["constituent_evidence"]["sample"][0]["catalog"]["metadata"],
            report["validation"]["catalog"]["metadata"],
        )
        for metadata in metadata_sections:
            self.assertEqual(metadata["source_type"], "HISTORICAL")
            self.assertEqual(metadata["provider"], "polymarket")
            self.assertEqual(metadata["instrument"], "POLYMARKET")


    def test_persisted_shared_report_freezes_momentum_and_excludes_zero_edge_control(self) -> None:
        records = _historical_records(count=4)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "momentum.sqlite"
            _build_compact_historical_source(path, records)
            report = _compact_report(path, expected_row_count=len(records))

        control = report["no_edge_control"]
        self.assertEqual(control["role"], "ZERO_EDGE_CONTROL")
        self.assertEqual(control["template"], "probability_mispricing")
        self.assertEqual(control["model_document"], {"field": "yes_mid"})
        self.assertTrue(control["selection_excluded"])
        self.assertTrue(control["proven_zero_edge"])
        self.assertEqual(control["metrics"]["nonzero_edge_count"], 0)
        self.assertGreater(control["metrics"]["model_evaluations"], 0)
        self.assertGreater(control["metrics"]["raw_observations"], 0)
        self.assertEqual(control["metrics"]["scored_predictions"], 0)

        assessment = report["assessment_candidate"]
        self.assertEqual(assessment["family"], "momentum")
        self.assertEqual(assessment["template"], "momentum")
        self.assertEqual(assessment["parameters"], {"lookback": 1, "threshold": 0.05})
        self.assertEqual(assessment["status"], "QUEUED")
        self.assertTrue(assessment["assessment_candidate_defined_before_current_inspection"])
        self.assertTrue(assessment["configuration_frozen_before_current_inspection"])
        self.assertTrue(assessment["configuration_frozen_hash"].startswith("sha256:"))
        self.assertEqual(
            set(assessment["causal_allowed_features"]),
            {"timestamp", "source_timestamp", "market_id", "token_id", "yes_mid", "price"},
        )
        self.assertEqual(
            assessment["costs"],
            {
                "initial_cash": 10_000.0,
                "allocation": 0.25,
                "fee_bps": 10.0,
                "slippage_bps": 5.0,
            },
        )
        self.assertFalse(assessment["processor_selected"])
        self.assertFalse(report["chain"]["processor_selected"])
        self.assertIsNone(report["chain"]["lifecycle_stage"])
        self.assertNotEqual(
            assessment["assessment_candidate_defined_before_current_inspection"],
            report["chain"]["processor_selected"],
        )
        filled = compute_dollar_limit_buy_feasibility(
            {"asks": [{"price": "0.50", "size": "10"}]},
            budget=100.0,
            slippage_bps=assessment["costs"]["slippage_bps"],
            fee_bps=assessment["costs"]["fee_bps"],
        )
        self.assertTrue(filled["feasible"])
        self.assertGreater(filled["quantity"], 0.0)
        self.assertGreater(filled["fee"], 0.0)
        self.assertGreater(filled["limit_price"], filled["best_ask"])

        history_metrics = report["historical_metrics"]
        audit = report["historical_support_audit"]
        self.assertEqual(audit["required_observations_per_market"], 2)
        self.assertEqual(audit["eligible_row_count"], len(records))
        self.assertEqual(audit["imported_market_count"], 1)
        self.assertEqual(audit["eligible_market_count"], 1)
        self.assertEqual(audit["ineligible_market_count"], 0)
        self.assertEqual(history_metrics["independent_raw_markets"], 1)
        self.assertEqual(history_metrics["independent_eligible_markets"], 1)
        self.assertTrue(audit["markets_hash"].startswith("sha256:"))
        self.assertEqual(len(audit["eligible_markets_sample"]), 1)
        self.assertEqual(audit["ineligible_markets_sample"], [])
        self.assertNotIn("markets", audit)
        self.assertGreater(history_metrics["raw_observations"], 0)
        self.assertGreater(history_metrics["signal_evaluation_count"], 0)
        self.assertGreaterEqual(history_metrics["signal_count"], 1)
        self.assertEqual(history_metrics["actionable_signal_count"], 0)
        self.assertEqual(history_metrics["model_evaluations"], 0)
        self.assertEqual(history_metrics["independent_scored_markets"], 0)
        self.assertEqual(history_metrics["scored_prediction_count"], 0)
        self.assertEqual(history_metrics["calibration_observation_count"], 0)
        self.assertEqual(history_metrics["fill_count"], 0)
        self.assertEqual(history_metrics["fills"], 0)
        self.assertEqual(history_metrics["closed_trade_count"], 0)
        self.assertEqual(report["qualification"]["status"], "REJECTED")
        self.assertEqual(report["qualification"]["blocker"], "HISTORICAL_MIN_TRADES_NOT_MET")
        self.assertFalse(report["qualification"]["historical_qualified"])
        self.assertFalse(report["qualification"]["current_evaluation_allowed"])
        chain = report["chain"]
        self.assertEqual(chain["reason_code"], "HISTORICAL_MIN_TRADES_NOT_MET")
        self.assertEqual(chain["exact_reason"], "HISTORICAL_MIN_TRADES_NOT_MET")
        self.assertEqual(chain["decision_reason"], "HISTORICAL_MIN_TRADES_NOT_MET")
        self.assertEqual(chain["reason"], "HISTORICAL_MIN_TRADES_NOT_MET")
        self.assertEqual(chain["primary_reason"], "HISTORICAL_MIN_TRADES_NOT_MET")
        self.assertEqual(
            chain["canonical_pipeline_reason"],
            "INSUFFICIENT_DATA",
        )
        self.assertNotEqual(chain["reason_code"], chain["canonical_pipeline_reason"])
        self.assertNotEqual(chain["exact_reason"], chain["canonical_pipeline_reason"])
        self.assertNotEqual(chain["decision_reason"], chain["canonical_pipeline_reason"])
        runtime = report["runtime_metric_assessment"]
        self.assertEqual(
            runtime["counts"],
            {
                "filled_trades": 0.0,
                "closed_trades": 0.0,
                "scored_predictions": 0.0,
                "calibration_observations": 0.0,
            },
        )
        self.assertEqual(runtime["decisive_blocker"], "HISTORICAL_MIN_TRADES_NOT_MET")
        paper_runtime = report["gate_assessment"]["PAPER_PROMOTABLE"]["evidence"]["runtime_metric_assessment"]
        self.assertEqual(paper_runtime["decisive_blocker"], "HISTORICAL_MIN_TRADES_NOT_MET")
        self.assertEqual(
            report["qualification"]["evidence"]["actionable_signal_count"],
            0.0,
        )
        self.assertEqual(
            report["qualification"]["evidence"]["closed_trade_count"],
            0.0,
        )
        self.assertEqual(
            report["source"]["attestation"]["execution_fidelity"],
            "PRICE_PROXY",
        )
        self.assertEqual(
            history_metrics["historical_execution_fidelity"],
            "PRICE_PROXY",
        )
        processor_result = chain["processor"]["results"][0]
        self.assertNotIn("data_quality", processor_result)
        encoded_report = json.dumps(report, sort_keys=True, separators=(",", ":"))
        self.assertNotIn("ORDER_BOOK_SIMULATED", encoded_report)
        self.assertTrue(report["qualification"]["same_candidate_required"])
        self.assertEqual(
            report["qualification"]["canonical_pipeline_reason"],
            report["chain"]["canonical_pipeline_reason"],
        )
        self.assertEqual(report["current_market_input_readiness"]["status"], "NOT_RUN")
        self.assertEqual(
            report["current_market_input_readiness"]["reason_code"],
            "HISTORICAL_QUALIFICATION_FAILED",
        )
        self.assertEqual(report["evaluator"]["status"], "SKIPPED")
        self.assertEqual(
            report["evaluator"]["reason"],
            "HISTORICAL_QUALIFICATION_FAILED",
        )
        self.assertEqual(
            report["evaluator"]["qualification_blocker"],
            "HISTORICAL_MIN_TRADES_NOT_MET",
        )
        self.assertEqual(report["actual_decision"]["status"], "NOT_RUN")
        self.assertEqual(
            report["actual_decision"]["reason_code"],
            "HISTORICAL_QUALIFICATION_FAILED",
        )
        self.assertEqual(report["execution_feasibility"]["status"], "NOT_RUN")
        self.assertEqual(
            report["execution_feasibility"]["reason_code"],
            "HISTORICAL_QUALIFICATION_FAILED",
        )

    def test_historical_metrics_count_raw_and_eligible_markets_from_lookback_audit(self) -> None:
        from tools.polymarket_market_scope_acceptance import (
            _persisted_historical_metrics,
            _persisted_historical_support_audit,
        )

        def rows(market_id: str, prices: tuple[float, ...]) -> list[dict[str, object]]:
            return [
                {
                    "market_id": market_id,
                    "token_id": f"yes-{market_id}",
                    "source_timestamp": f"2026-01-01T0{index}:00:00+00:00",
                    "timestamp": f"2026-01-01T0{index}:00:00+00:00",
                    "yes_mid": price,
                    "price": price,
                }
                for index, price in enumerate(prices)
            ]

        constant = rows("constant-market", (0.50, 0.50))
        moving = rows("moving-market", (0.40, 0.50))
        short = rows("short-market", (0.70,))
        constituents = [
            {"market_id": market_id, "records": market_rows}
            for market_id, market_rows in (
                ("constant-market", constant),
                ("moving-market", moving),
                ("short-market", short),
            )
        ]
        aggregate = [*constant, *moving, *short]
        audit = _persisted_historical_support_audit(
            constituents,
            aggregate,
            lookback=1,
        )
        metrics = _persisted_historical_metrics(
            aggregate,
            family="momentum",
            threshold=0.05,
            historical_support_audit=audit,
        )

        self.assertEqual(audit["imported_market_count"], 3)
        self.assertEqual(audit["imported_row_count"], 5)
        self.assertEqual(audit["eligible_market_count"], 2)
        self.assertEqual(audit["eligible_row_count"], 4)
        self.assertEqual(audit["ineligible_market_count"], 1)
        self.assertEqual(audit["ineligible_row_count"], 1)
        self.assertEqual(metrics["raw_observations"], 5)
        self.assertEqual(metrics["sample_count"], 5)
        self.assertEqual(metrics["independent_raw_markets"], 3)
        self.assertEqual(metrics["independent_eligible_markets"], 2)
        self.assertEqual(metrics["signal_count"], 1)
        self.assertEqual(metrics["actionable_signal_count"], 0)


    def test_paper_forward_stage_cannot_override_historical_rejection(self) -> None:
        qualification = _historical_qualification_assessment(
            {
                "actionable_signal_count": 1,
                "closed_trade_count": 1,
                "filled_trades": 0,
                "robustness": {
                    "robustness_passed": True,
                    "minimum_sample_check": {"trades": 0, "min_trades": 20},
                },
            },
            {
                "status": "pass",
                "criteria": {"min_trades": 20},
                "metrics": {"filled_trades": {"status": "pass"}},
            },
            {"status": "PASSED", "as_of_lifecycle_status": "AVAILABLE"},
            processor_selected=False,
            lifecycle_stage="PAPER_FORWARD",
            canonical_pipeline_reason="CANDIDATE_FORWARD_MARKET_UNRESOLVED",
        )
        self.assertEqual(qualification["lifecycle_stage"], "PAPER_FORWARD")
        self.assertFalse(qualification["processor_selected"])
        self.assertFalse(qualification["historical_qualified"])
        self.assertEqual(qualification["status"], "REJECTED")
        self.assertEqual(qualification["blocker"], "HISTORICAL_MIN_TRADES_NOT_MET")
        self.assertEqual(
            qualification["canonical_pipeline_reason"],
            "CANDIDATE_FORWARD_MARKET_UNRESOLVED",
        )
        self.assertEqual(
            qualification["canonical_pipeline_reason_source"],
            "queue/current-authority",
        )

    def test_persisted_history_projection_excludes_copied_terminal_and_market_state(self) -> None:
        records = _historical_records(count=3)
        records[1].update(
            {
                "settlement": "YES",
                "liquidity": 12_345.0,
                "volume": 98_765.0,
                "order_book": {"asks": [{"price": 0.51, "size": 100.0}]},
            }
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "causal-projection.sqlite"
            _build_compact_historical_source(path, records)
            report = _compact_report(path, expected_row_count=len(records))

        audit = report["historical_support_audit"]
        projection = audit["research_projection"]
        self.assertEqual(
            set(projection["included_features"]),
            {"timestamp", "source_timestamp", "market_id", "token_id", "yes_mid", "price"},
        )
        for field in ("settlement", "liquidity", "volume", "order_book"):
            self.assertIn(field, projection["excluded_fields"])
        self.assertEqual(audit["as_of_lifecycle_status"], "AS_OF_LIFECYCLE_UNAVAILABLE")
        self.assertEqual(audit["historical_as_of_fields_observed"], [])
        self.assertEqual(audit["future_label_exclusion"]["status"], "EXCLUDED")
        self.assertFalse(projection["historical_order_books_used"])
        self.assertFalse(projection["historical_liquidity_used"])
        self.assertFalse(projection["historical_volume_used"])
        self.assertFalse(projection["states_synthesized"])

        provenance = report["validation"]["record_provenance"]
        self.assertEqual(provenance["explicit_source_type_rows"], len(records))
        readiness = report["current_market_input_readiness"]
        self.assertEqual(readiness["status"], "SKIPPED")
        self.assertEqual(readiness["reason_code"], "HISTORICAL_QUALIFICATION_REQUIRED")
        self.assertFalse(readiness["historical_order_book_available"])
        execution = report["execution_feasibility"]
        self.assertEqual(execution["status"], "SKIPPED")
        self.assertEqual(execution["reason_code"], "HISTORICAL_QUALIFICATION_REQUIRED")
        self.assertFalse(execution["current_book_required"])
        self.assertFalse(execution["order_transport_called"])
        self.assertIsNone(report["dollar_limit_buy_feasibility"]["selected_market"])

    def test_persisted_history_requires_same_market_order_and_lookback(self) -> None:
        duplicate = _historical_records(count=4)
        duplicate[2]["source_timestamp"] = duplicate[1]["source_timestamp"]
        duplicate[2]["timestamp"] = duplicate[1]["timestamp"]
        out_of_order = _historical_records(count=4)
        out_of_order[1]["source_timestamp"], out_of_order[2]["source_timestamp"] = (
            out_of_order[2]["source_timestamp"],
            out_of_order[1]["source_timestamp"],
        )
        out_of_order[1]["timestamp"], out_of_order[2]["timestamp"] = (
            out_of_order[2]["timestamp"],
            out_of_order[1]["timestamp"],
        )

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name, records in (("duplicate", duplicate), ("out-of-order", out_of_order)):
                with self.subTest(name=name):
                    path = root / f"{name}.sqlite"
                    _build_compact_historical_source(path, records)
                    report = _compact_report(path, expected_row_count=len(records))
                    audit = report["historical_support_audit"]
                    if name == "duplicate":
                        market = audit["ineligible_markets_sample"][0]
                        self.assertFalse(market["strict_increasing_unique_source_timestamps"])
                        self.assertIn("SOURCE_TIMESTAMPS_NOT_STRICTLY_INCREASING", market["reasons"])
                        self.assertEqual(audit["eligible_market_count"], 0)
                        self.assertEqual(audit["ineligible_market_count"], 1)
                    else:
                        market = audit["eligible_markets_sample"][0]
                        self.assertTrue(market["strict_increasing_unique_source_timestamps"])
                        self.assertEqual(market["reasons"], [])
                        self.assertEqual(audit["eligible_market_count"], 1)
                        self.assertEqual(audit["ineligible_market_count"], 0)
                    self.assertTrue(audit["market_audit_bounded"])
                    self.assertNotIn("markets", audit)
                    if name == "duplicate":
                        self.assertEqual(report["qualification"]["status"], "REJECTED")
                    else:
                        self.assertNotEqual(report["qualification"]["status"], "QUALIFIED")

            insufficient_path = root / "lookback-insufficient.sqlite"
            one = _historical_records(count=1)
            _build_compact_historical_source(insufficient_path, one)
            insufficient = _compact_report(insufficient_path, expected_row_count=1)
            self.assertEqual(insufficient["historical_support_audit"]["eligible_row_count"], 0)
            self.assertFalse(insufficient["historical_support_audit"]["ineligible_markets_sample"][0]["lookback_sufficient"])
            self.assertEqual(insufficient["historical_metrics"]["signal_count"], 0)
            self.assertEqual(insufficient["historical_metrics"]["raw_observations"], 1)
            self.assertEqual(insufficient["historical_metrics"]["independent_raw_markets"], 1)
            self.assertEqual(insufficient["historical_metrics"]["independent_eligible_markets"], 0)

    def test_persisted_zero_observations_cannot_pass_calibration_or_profitability(self) -> None:
        assessment = _runtime_metric_assessment(
            {
                "validation": {
                    "validation": {
                        "independent_samples": 0,
                        "filled_trades": 0,
                        "closed_trade_count": 0,
                        "scored_predictions": 0,
                        "calibration_observations": 0,
                        "max_drawdown": 0.0,
                        "expectancy": 0.0,
                        "confidence_interval": {"lower": 0.0},
                        "regime_count": 0,
                    },
                    "validation_stability": 1.0,
                    "validation_calibration": 1.0,
                }
            },
            {
                "min_independent_samples": 30,
                "min_trades": 20,
                "max_drawdown": 0.20,
                "min_expectancy": 0.0,
                "min_confidence_lower_bound": 0.0,
                "min_stability": 0.60,
                "min_calibration": 0.80,
                "min_liquidity": 0.0,
                "min_forward_duration_seconds": 7.0 * 86400.0,
                "min_regimes": 3,
            },
            current_market_blocker=None,
        )
        metrics = assessment["metrics"]
        self.assertNotEqual(metrics["expectancy"]["status"], "pass")
        self.assertNotEqual(metrics["calibration"]["status"], "pass")
        self.assertNotEqual(assessment["status"], "pass")

    def test_persisted_gate_and_current_evaluation_statuses_are_distinct_and_safe(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            report = _compact_report(Path(directory) / "gates.sqlite")

        gates = report["gate_assessment"]
        self.assertEqual(set(gates), {"historical", "canary", "PAPER_PROMOTABLE"})
        self.assertEqual(gates["historical"]["status"], "REJECTED")
        self.assertNotEqual(gates["canary"]["status"], "PASS")
        self.assertNotEqual(gates["PAPER_PROMOTABLE"]["status"], "PASS")
        self.assertEqual(report["actual_decision"]["status"], "NOT_RUN")
        self.assertEqual(report["current_market_input_readiness"]["status"], "NOT_RUN")
        self.assertEqual(report["evaluator"]["status"], "SKIPPED")
        self.assertEqual(report["execution_feasibility"]["status"], "NOT_RUN")
        for section in (
            report["current_market_input_readiness"],
            report["actual_decision"],
            report["execution_feasibility"],
        ):
            self.assertEqual(section["reason_code"], "HISTORICAL_QUALIFICATION_FAILED")
        self.assertEqual(report["evaluator"]["reason"], "HISTORICAL_QUALIFICATION_FAILED")
        self.assertEqual(
            report["evaluator"]["qualification_blocker"],
            "HISTORICAL_MIN_TRADES_NOT_MET",
        )
        self.assertTrue(report["execution_feasibility"]["current_book_required"] is False)
        self.assertFalse(report["security"]["credentials_used"])
        self.assertFalse(report["security"]["authenticated_calls"])
        self.assertFalse(report["security"]["submit_order_called"])

    def test_persisted_default_criteria_costs_and_no_synthetic_model_are_observable(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            report = _compact_report(Path(directory) / "criteria.sqlite")
            self.assertFalse(report["security"]["synthetic_probability_model"])
            self.assertEqual(report["source"]["attestation"]["execution_fidelity"], "PRICE_PROXY")
            self.assertEqual(report["runtime_criteria"]["min_independent_samples"], 30)
            self.assertEqual(report["runtime_criteria"]["min_trades"], 20)
            methodology = report["proposal"]["experiment_plan"]["methodology"]
            self.assertEqual(methodology["initial_cash"], 10_000.0)
            self.assertEqual(methodology["fee_bps"], 10.0)
            self.assertEqual(methodology["slippage_bps"], 5.0)
            self.assertEqual(methodology["allocation"], 0.25)
            self.assertNotIn("model_document", report["proposal"]["experiment_plan"])
            self.assertEqual(report["chain"]["queue_status"], "REJECTED")
            self.assertEqual(report["acceptance_status"], "RESULT_RECORDED")
            processor_result = report["chain"]["processor"]["results"][0]
            self.assertFalse(processor_result["accepted"])
            self.assertEqual(processor_result["reason_code"], "INSUFFICIENT_DATA")
            self.assertEqual(processor_result["reason"], "at least three chronological observations are required")
            self.assertEqual(processor_result["paper_only"], True)

    def test_persisted_report_is_compact_and_samples_are_bounded(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            report = _compact_report(Path(directory) / "compact-artifact.sqlite")
        encoded = json.dumps(report, sort_keys=True, separators=(",", ":"))
        self.assertLess(len(encoded), 500_000)
        self.assertNotIn('"markets":', encoded)
        audit = report["historical_support_audit"]
        self.assertTrue(audit["market_audit_bounded"])
        self.assertEqual(audit["imported_market_count"], 1)
        self.assertTrue(audit["markets_hash"].startswith("sha256:"))
        self.assertLessEqual(len(audit["eligible_markets_sample"]), 3)
        self.assertLessEqual(len(audit["ineligible_markets_sample"]), 3)
        self.assertEqual(encoded.count('"historical_support_audit"'), 1)
        historical_evidence = report["gate_assessment"]["historical"]["evidence"]
        self.assertNotIn("historical_support_audit", historical_evidence)
        self.assertNotIn("historical_metrics", historical_evidence)
        self.assertEqual(
            historical_evidence["historical_support_audit_hash"],
            audit["markets_hash"],
        )
        self.assertEqual(
            historical_evidence["historical_market_counts"],
            {"imported": 1, "eligible": 0, "ineligible": 1},
        )
        self.assertEqual(
            historical_evidence["historical_metrics_hash"],
            _sha256_json(report["historical_metrics"]),
        )

        source = report["source"]
        self.assertEqual(source["constituents"]["catalog_count"], 1)
        self.assertEqual(source["constituents"]["row_count"], 1)
        self.assertNotIsInstance(source["attestation_row"].get("constituent_bindings"), list)
        self.assertLessEqual(len(source["constituent_evidence"]["sample"]), 3)
        self.assertNotIsInstance(source["catalog"]["metadata"].get("market_versions"), list)
        self.assertEqual(source["catalog"]["metadata"]["market_versions_count"], 1)
        export = report["export"]
        self.assertEqual(export["constituent_catalogs_count"], 1)
        self.assertLessEqual(len(export["constituent_catalog_sample"]), 3)
        self.assertEqual(export["evaluated_records_count"], 1)
        self.assertLessEqual(len(export["evaluated_record_sample"]), 2)
        self.assertNotIn("constituent_catalogs", export)
        chain = report["chain"]
        self.assertNotIn("experiment_plan", chain.get("assessment_candidate_payload", {}))
        self.assertLessEqual(len(chain["assessment_candidate_lifecycle_events"]), 100)
        for event in chain["assessment_candidate_lifecycle_events"]:
            self.assertNotIn("payload", event)
            self.assertIn("payload_hash", event)
            self.assertIn("payload_metrics", event)

    def test_runtime_metric_assessment_compares_validation_to_unmodified_defaults(self) -> None:
        criteria = {
            "min_independent_samples": 30,
            "min_trades": 20,
            "max_drawdown": 0.20,
            "min_expectancy": 0.0,
            "min_confidence_lower_bound": 0.0,
            "min_stability": 0.60,
            "min_calibration": 0.80,
            "min_liquidity": 0.0,
            "min_forward_duration_seconds": 7.0 * 86400.0,
            "min_regimes": 3,
            "min_order_attempts_for_execution_rejection": 5,
        }
        assessment = _runtime_metric_assessment(
            {
                "validation": {
                    "validation": {
                        "independent_samples": 434,
                        "filled_trades": 0,
                        "max_drawdown": 0.0,
                        "expectancy": 0.0,
                        "confidence_interval": {"lower": 0.0},
                        "regime_count": 1,
                    },
                    "validation_stability": 1.0,
                    "validation_calibration": 1.0,
                }
            },
            criteria,
            current_market_blocker="CANDIDATE_FORWARD_MARKET_UNRESOLVED",
        )
        metrics = assessment["metrics"]
        self.assertEqual(metrics["independent_samples"]["observed"], 434.0)
        self.assertEqual(metrics["independent_samples"]["status"], "pass")
        self.assertEqual(metrics["filled_trades"]["status"], "fail")
        self.assertEqual(metrics["drawdown"]["status"], "pass")
        self.assertNotEqual(metrics["expectancy"]["status"], "pass")
        self.assertEqual(metrics["confidence_interval_lower"]["status"], "not_reached")
        self.assertEqual(metrics["stability"]["status"], "pass")
        self.assertNotEqual(metrics["calibration"]["status"], "pass")
        self.assertEqual(metrics["liquidity"]["status"], "not_reached")
        self.assertEqual(metrics["regimes"]["status"], "fail")
        self.assertEqual(metrics["forward_duration_seconds"]["status"], "not_reached")
        self.assertEqual(metrics["forward_order_attempts"]["status"], "not_reached")
        self.assertEqual(
            assessment["decisive_current_market_blocker"]["reason_code"],
            "CANDIDATE_FORWARD_MARKET_UNRESOLVED",
        )
        self.assertEqual(assessment["status"], "fail")

    def test_release_plan_is_prepared_only_and_uses_consistent_explicit_database(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            report = _compact_report(Path(directory) / "release.sqlite")
        plan = report["release_plan"]
        self.assertTrue(plan["prepared_not_executed"])
        self.assertEqual(plan["release_branch"], "feature/polymarket-acceptance-real-candidate")
        self.assertIn("git rev-parse feature/polymarket-acceptance-real-candidate", plan["merge_gates"]["reviewed_commit_precondition"])
        self.assertIn("git ls-remote origin refs/heads/feature/polymarket-acceptance-real-candidate", plan["merge_gates"]["reviewed_commit_precondition"])
        self.assertEqual(
            plan["merge_gates"]["commands"],
            [
                "git status --short --branch",
                "git diff --check",
                "git fetch origin",
                "git switch main",
                "git pull --ff-only origin main",
                "git merge --ff-only feature/polymarket-acceptance-real-candidate",
                "git push origin main",
            ],
        )

        self.assertIn("python -m unittest tests.test_market_scope_acceptance_tool -v", plan["focused_unittest_command"])
        self.assertIn("python -m unittest discover", plan["full_unittest_command"])
        deployment = plan["deployment"]
        self.assertTrue(deployment["same_explicit_db_for_all_commands"])
        self.assertTrue(deployment["never_alter_controls"])
        self.assertFalse(deployment["commands_executed"])
        db_path = deployment["explicit_db"]
        self.assertIn(f'--db "{db_path}"', deployment["canary_status_before"])
        self.assertIn(f'--db "{db_path}"', deployment["canary_status_after_start"])
        for name in (
            "node_status_before",
            "stop",
            "node_status_after_stop",
            "start",
            "node_status_after_start",
        ):
            self.assertIn(f'-DbPath "{db_path}"', deployment[name])
        self.assertIn("canary-status", deployment["canary_status_before"])
        self.assertIn("canary-status", deployment["canary_status_after_start"])
        self.assertIn("restart_axiom_node.ps1", plan["warning"])
        self.assertIn("parameter mismatch", plan["warning"])
    def test_persisted_rejected_qualification_is_success_and_infrastructure_is_error(self) -> None:
        from axiom.research_bus import DurableResearchBus

        with tempfile.TemporaryDirectory() as directory:
            rejected_path = Path(directory) / "rejected.sqlite"
            rejected_output = Path(directory) / "rejected.json"
            with _compact_contract(rejected_path) as (dataset_id, dataset_version, _):
                result = main(
                    [
                        "--mode",
                        "persisted",
                        "--source-backup",
                        str(rejected_path),
                        "--dataset-id",
                        dataset_id,
                        "--dataset-version",
                        dataset_version,
                        "--output",
                        str(rejected_output),
                    ]
                )
            self.assertEqual(result, 0)
            rejected = json.loads(rejected_output.read_text(encoding="utf-8"))
            self.assertEqual(rejected["validation"]["status"], "PASSED")
            self.assertEqual(rejected["chain"]["queue_status"], "REJECTED")
            self.assertEqual(rejected["status_categories"]["queue"], "REJECTED")
            self.assertEqual(rejected["evaluator"]["status"], "SKIPPED")

            error_path = Path(directory) / "error.sqlite"
            error_output = Path(directory) / "error.json"
            with patch.object(DurableResearchBus, "__init__", side_effect=RuntimeError("bus unavailable")):
                with _compact_contract(error_path) as (dataset_id, dataset_version, _):
                    result = main(
                        [
                            "--mode",
                            "persisted",
                            "--source-backup",
                            str(error_path),
                            "--dataset-id",
                            dataset_id,
                            "--dataset-version",
                            dataset_version,
                            "--output",
                            str(error_output),
                        ]
                    )
            self.assertNotEqual(result, 0)
            error = json.loads(error_output.read_text(encoding="utf-8"))
            self.assertEqual(error["chain"]["queue_status"], "ERROR")
            self.assertEqual(error["status_categories"]["queue"], "ERROR")
            self.assertEqual(error["evaluator"]["status"], "SKIPPED")
            self.assertEqual(error["current_market_input_readiness"]["status"], "SKIPPED")
            self.assertEqual(
                error["current_market_input_readiness"]["reason_code"],
                "HISTORICAL_QUALIFICATION_REQUIRED",
            )
            self.assertTrue(error["chain"]["reasons"][0].startswith("PIPELINE_ERROR:"))

    def test_sanitized_persisted_no_book_report_preserves_null_and_redacts_token(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "sanitized.sqlite"
            report = _compact_report(path)
            self.assertIsNone(
                report["dollar_limit_buy_feasibility"]["selected_token_id"]
            )
            with patch(
                "tools.polymarket_market_scope_acceptance.compute_dollar_limit_buy_feasibility",
                return_value={"selected_token_id": "TOKEN-SENTINEL"},
            ):
                redacted_report = _compact_report(path)
        self.assertEqual(
            redacted_report["dollar_limit_buy_feasibility"]["selected_token_id"],
            "[REDACTED]",
        )

    def test_persisted_evaluator_is_gated_without_same_candidate_order(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            report = _compact_report(Path(directory) / "evaluator.sqlite")
            evaluator = report["evaluator"]
            self.assertEqual(evaluator["status"], "SKIPPED")
            self.assertEqual(evaluator["reason"], "HISTORICAL_QUALIFICATION_FAILED")
            self.assertEqual(evaluator["qualification_blocker"], "HISTORICAL_MIN_TRADES_NOT_MET")
            self.assertEqual(evaluator["current_market_resolution"], "NOT_RUN")
            self.assertEqual(evaluator["fresh_identity_and_books"], "NOT_RUN")
            self.assertIsNone(evaluator["decision"])
            self.assertEqual(report["release_readiness"]["status"], "NOT_READY")
            self.assertFalse(report["release_readiness"]["place_order_called"])

    def test_dollar_limit_buy_worst_case_price_quantity_minimum_fees_and_zero_quantity(self) -> None:
        no_book = compute_dollar_limit_buy_feasibility(None)
        self.assertEqual(no_book["selected_market"], None)
        self.assertEqual(no_book["selected_market_id"], None)
        self.assertEqual(no_book["selected_token_id"], None)
        self.assertEqual(
            no_book["selected_order"],
            {
                "side": "BUY",
                "order_type": "LIMIT",
                "budget": 1.0,
                "status": "not_reached",
                "submit_called": False,
            },
        )
        self.assertEqual(no_book["order_type"], "LIMIT")
        self.assertEqual(no_book["budget"], 1.0)
        self.assertFalse(no_book["feasible"])
        self.assertEqual(no_book["reason"], "NO_PERMITTED_PUBLIC_BOOK")
        self.assertFalse(no_book["submit_called"])
        self.assertEqual(no_book["fee"], "not_evaluated")
        for field in (
            "min_quantity",
            "min_notional",
            "fee_rate",
            "quantity_step",
            "quantity_rounding",
            "quantity_increment",
            "book_depth",
        ):
            self.assertIsNone(no_book[field])
        self.assertIsNone(no_book["quantity"])
        self.assertIsNone(no_book["notional"])
        self.assertIsNone(no_book["total"])
        self.assertEqual(
            no_book["checks"],
            {
                "min_quantity": "not_reached",
                "min_notional": "not_reached",
                "fee_rate": "not_reached",
                "quantity_rounding": "not_reached",
                "quantity_increment": "not_reached",
                "book_depth": "not_reached",
            },
        )

        feasible = compute_dollar_limit_buy_feasibility(
            {"asks": [{"price": "0.99", "size": "10"}]},
            budget=1.0,
            slippage_bps=100.0,
            quantity_step="0.01",
            min_quantity="1.00",
            min_notional="0.99",
            fee_bps=100.0,
        )
        self.assertTrue(feasible["feasible"])
        self.assertEqual(feasible["side"], "BUY")
        self.assertEqual(feasible["order_type"], "LIMIT")
        self.assertEqual(feasible["best_ask"], 0.99)
        self.assertEqual(feasible["limit_price"], 0.9999)
        self.assertEqual(feasible["quantity"], 1.0)
        self.assertEqual(feasible["notional"], 0.99)
        self.assertEqual(feasible["fee"], 0.0099)
        self.assertEqual(feasible["total"], 0.9999)
        self.assertFalse(feasible["submit_called"])

        minimum_quantity = compute_dollar_limit_buy_feasibility(
            {"asks": [["0.50", "10"]]},
            quantity_step="0.10",
            min_quantity="2.10",
        )
        self.assertFalse(minimum_quantity["feasible"])
        self.assertEqual(minimum_quantity["reason"], "VENUE_MINIMUM_QUANTITY")

        minimum_notional = compute_dollar_limit_buy_feasibility(
            {"asks": [["0.50", "10"]]},
            quantity_step="0.10",
            min_notional="1.01",
        )
        self.assertFalse(minimum_notional["feasible"])
        self.assertEqual(minimum_notional["reason"], "VENUE_MINIMUM_NOTIONAL")

        zero_quantity = compute_dollar_limit_buy_feasibility(
            {"asks": [["0.50", "0.001"]]},
            quantity_step="0.01",
        )
        self.assertEqual(zero_quantity["quantity"], 0.0)
        self.assertFalse(zero_quantity["feasible"])
        self.assertFalse(zero_quantity["submit_called"])

    def test_public_nonexistent_dataset_negative_section_remains_unchanged(self) -> None:
        report = run_acceptance(
            build_offline_fixture_adapter(),
            AcceptanceConfig(page_limit=1, max_pages=1, max_seconds=30, max_books=0, book_depth=1),
            generated_at="2026-01-01T00:00:00+00:00",
            include_queue_demo=False,
        )
        self.assertEqual(
            report["missing_dataset_public_queue_negative"],
            {
                "preserved": True,
                "executed": False,
                "note": "The existing public missing-dataset negative section is unchanged.",
            },
        )


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
