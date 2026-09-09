from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
import json
import tempfile
import unittest
from types import SimpleNamespace

from tools.polymarket_market_scope_acceptance import (
    AcceptanceConfig,
    OfflineDiscoveryPage,
    OfflineFixtureAdapter,
    build_offline_fixture_adapter,
    enqueue_legacy_successor,
    main,
    run_acceptance,
)


class MarketScopeAcceptanceToolTests(unittest.TestCase):
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
        self.assertEqual(result["processor_reason"], "unsupported_by_validation")
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


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
