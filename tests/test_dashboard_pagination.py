from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import re
import tempfile
import threading
import unittest
from urllib.error import HTTPError
from urllib.parse import quote, urlencode
from urllib.request import urlopen

from axiom.canary import CanaryService
from axiom.dashboard import DashboardData, DashboardServer, _dashboard_html, _jsonable
from axiom.domain import MarketType
from axiom.operator import CANARY_CONNECTIVITY_CONFIG_KEY
from axiom.ranker import CandidateCanaryRanker
from axiom.storage import AxiomStore


UTC = timezone.utc
T0 = datetime(2024, 1, 1, tzinfo=UTC)
COMMON_PAGE_KEYS = {"items", "page", "page_size", "total", "pages"}
DATASET_COUNT = 24
MARKET_COUNT = 23
CANDIDATE_COUNT = 23
QUEUE_COUNT = 23
PAPER_COUNT = 23
ACTIVITY_COUNT = DATASET_COUNT + 1 + 45 + 25
ACTIONABLE_SCAN_FIELDS = (
    "candidates_ranked",
    "candidates_signal_checked",
    "candidates_no_signal",
    "actionable_candidates_found",
    "selected_actionable_candidate",
    "selected_actionable_rank",
    "selected_actionable_score",
    "signal_scan_cursor",
    "signal_scan_ranking_run_id",
    "next_signal_scan_start_rank",
    "next_signal_scan_end_rank",
)




CANARY_ALLOWANCE_INSUFFICIENT_REASON = (
    "Current allowance is below the amount required for a $1 canary."
)
PERSISTED_CONNECTIVITY_SECRET_VALUES = (
    "API_KEY_SENTINEL",
    "MNEMONIC_SENTINEL",
    "SECRET_SENTINEL",
    "ADDRESS_SENTINEL",
    "RAW_SENTINEL",
    "SPENDER_SENTINEL",
)
WALLET_LIKE_CONNECTIVITY_VALUE = "0123456789abcdef0123456789abcdef01234567"


FORBIDDEN_CONNECTIVITY_VALUES = (
    "PRIVATE_KEY_SENTINEL",
    "API_SECRET_SENTINEL",
    "PASSPHRASE_SENTINEL",
    "KEYRING_VALUE_SENTINEL",
    "RAW_DIAGNOSTIC_SENTINEL",
)


def _connectivity_projection(
    *,
    ready: bool,
    status: str,
    checked_at: str = "2024-01-02T03:04:05+00:00",
    allowance_status: str = "SUFFICIENT",
) -> dict[str, object]:
    """Return the bounded, public connectivity fixture shared by API tests."""
    return {
        "ready": ready,
        "status": status,
        "checked_at": checked_at,
        "sdk": {
            "installed": True,
            "name": "polymarket-client",
            "version": "0.9.0",
            "status": "INSTALLED",
        },
        "credentials": {"status": "CONFIGURED"},
        "authentication": {"status": "PASS"},
        "account": {"status": "PASS", "wallet_type": "EOA"},
        "geoblock": {"status": "PASS", "country": "PH", "region": "NCR"},
        "balance": {"status": "PASS", "available_usd": "12.34"},
        "allowance": {"status": allowance_status},
        "market": {"status": "PASS"},
        "order_book": {"status": "PASS"},
        "failure_codes": [],
        "failure_reasons": [],
        "live_execution": False,
    }

class _BlockingOperatorControl:
    """Control stub that makes the legacy status path observably unavailable."""

    def __init__(self) -> None:
        self.status_started = threading.Event()
        self.release_status = threading.Event()
        self.status_calls = 0

    def status(self) -> dict[str, object]:
        self.status_calls += 1
        self.status_started.set()
        if not self.release_status.wait(timeout=5):
            raise RuntimeError("operator status intentionally timed out")
        raise RuntimeError("operator status intentionally failed")


class DashboardPaginationFixture(unittest.TestCase):
    """Small persisted fixture shared by endpoint tests.

    Every collection is large enough to cross a ten-row page boundary, while
    remaining tiny compared with the production scan bounds these APIs replace.
    """

    def setUp(self) -> None:
        self._temporary_directory = tempfile.TemporaryDirectory()
        # Register teardown before any fixture writes so a seed failure does
        # not leave an open SQLite handle or a locked temporary directory.
        self.addCleanup(self._temporary_directory.cleanup)
        database_path = Path(self._temporary_directory.name) / "dashboard.sqlite3"
        self.store = AxiomStore(str(database_path))
        self.addCleanup(self.store.close)
        self.canary_service = CanaryService(self.store)
        self._seed_datasets()
        self._seed_polymarket()
        self._seed_candidates()
        self._seed_queue()
        self._seed_paper()
        self.server = DashboardServer(port=0, data=DashboardData(store=self.store)).start()
        # unittest cleanups run last-in, first-out; close the HTTP server and
        # SQLite connection before removing the temporary database directory.
        self.addCleanup(self.server.stop)

    def _seed_datasets(self) -> None:
        for index in range(DATASET_COUNT):
            dataset_id = f"dataset-{index:02d}"
            source_type = "HISTORICAL" if index % 2 == 0 else "FORWARD_COLLECTED"
            market_type = MarketType.CRYPTO_SPOT.value if index % 3 else MarketType.PREDICTION.value
            timeframe = "1d" if index % 2 == 0 else "1h"
            quality = "HIGH" if index % 4 else "LOW"
            timestamp = T0 + timedelta(minutes=index // 2)
            missing_ranges = (
                [
                    {
                        "start": f"2024-02-{range_index + 1:02d}T00:00:00+00:00",
                        "end": f"2024-02-{range_index + 1:02d}T01:00:00+00:00",
                    }
                    for range_index in range(23)
                ]
                if index == 0
                else ()
            )
            self.store.save_dataset_catalog(
                dataset_id,
                "v1",
                provider="fixture-provider",
                instrument=f"instrument-{index:02d}",
                market_type=market_type,
                timeframe=timeframe,
                start_timestamp=T0,
                end_timestamp=T0 + timedelta(hours=1),
                row_count=index + 1,
                completeness=0.75 + (index % 4) / 16,
                missing_ranges=missing_ranges,
                quality=quality,
                source_type=source_type,
                snapshot_id=f"snapshot-{index:02d}",
                metadata={"fixture": True},
                created_at=timestamp,
                updated_at=timestamp,
            )

        # The detail route must decode the path segment and return the actual
        # persisted dataset, not materialize the entire catalog in the index.
        self.detail_dataset_id = "dataset/detail with/slash"
        self.store.save_dataset(
            self.detail_dataset_id,
            "v1",
            [{"value": "detail-only"}],
            metadata={"fixture": True, "detail": True},
        )
        self.store.save_dataset_catalog(
            self.detail_dataset_id,
            "v1",
            provider="fixture-provider",
            instrument="detail-instrument",
            market_type=MarketType.CRYPTO_SPOT.value,
            timeframe="detail",
            start_timestamp=T0,
            end_timestamp=T0,
            row_count=1,
            completeness=1.0,
            quality="DETAIL",
            source_type="FORWARD_COLLECTED",
            snapshot_id="detail-snapshot",
            metadata={"fixture": True, "detail": True},
            created_at=T0 + timedelta(minutes=100),
            updated_at=T0 + timedelta(minutes=100),
        )

    def _seed_polymarket(self) -> None:
        for index in range(MARKET_COUNT):
            market_id = f"market-{index:02d}"
            category = "weather" if index % 3 == 0 else "sports"
            timeframe = "1d" if index % 2 == 0 else "7d"
            settlement = "open" if index != 20 else "resolved_yes"
            quality = "ORDER_BOOK_SIMULATED" if index % 2 == 0 else "PRICE_PROXY"
            question = f"Will fixture event {index:02d} happen?"
            metadata = {
                "market_id": market_id,
                "question": question,
                "category": category,
                "timeframe": timeframe,
                "active": True,
                "closed": False,
            }
            snapshot = {
                "market_id": market_id,
                "question": question,
                "category": category,
                "timeframe": timeframe,
                "settlement": settlement,
                "yes_mid": 0.40 + index / 100,
                "liquidity": 1000 + index,
            }
            self.store.save_polymarket_market_metadata(
                market_id,
                metadata,
                observed_at=T0 + timedelta(minutes=index // 2),
            )
            self.store.save_polymarket_snapshot(
                f"market-snapshot-{index:02d}",
                market_id,
                T0 + timedelta(minutes=index // 2),
                T0 + timedelta(minutes=index // 2, seconds=1),
                {"snapshot": snapshot, "source_type": "FORWARD_COLLECTED"},
                quality=quality,
            )

    def _seed_candidates(self) -> None:
        for index in range(CANDIDATE_COUNT):
            candidate_id = f"candidate-{index:02d}"
            payload = {
                "strategy_id": f"strategy-{index:02d}",
                "experiment_family": "trend" if index % 2 == 0 else "mean_reversion",
                "market_type": MarketType.CRYPTO_SPOT.value,
                "generation": index % 3,
                "quality": "HIGH" if index % 2 == 0 else "LOW",
                "hypothesis": f"candidate hypothesis {index:02d}",
            }
            if index == 0:
                stage = "IDEA"
                self.store.save_candidate_lifecycle(
                    candidate_id,
                    stage,
                    payload,
                    reason="fixture seed",
                    timestamp=T0,
                )
                progression = (
                    "SCHEMA_VALIDATED",
                    "BACKTESTED",
                    "VALIDATED",
                    "ROBUSTNESS_CHECKED",
                    "FROZEN",
                    "PAPER_FORWARD",
                    "PAPER_PROMOTABLE",
                )
                for next_stage in progression:
                    payload = {**payload, "event_marker": next_stage}
                    self.store.save_candidate_lifecycle(
                        candidate_id,
                        next_stage,
                        payload,
                        from_stage=stage,
                        reason="fixture event pagination",
                        timestamp=T0,
                    )
                    stage = next_stage
                for marker in range(4):
                    payload = {**payload, "event_marker": f"repeat-{marker}"}
                    self.store.save_candidate_lifecycle(
                        candidate_id,
                        stage,
                        payload,
                        from_stage=stage,
                        reason="fixture event pagination",
                        timestamp=T0,
                    )
            elif index == 2:
                payload = {
                    **payload,
                    "strategy_hash": "fixture-strategy-hash-02",
                    "model_hash": "fixture-model-hash-02",
                    "config_hash": "fixture-config-hash-02",
                }
                payload["frozen_hash"] = hashlib.sha256(
                    "|".join(
                        payload[key] for key in ("strategy_hash", "model_hash", "config_hash")
                    ).encode()
                ).hexdigest()
                self.store.save_candidate_lifecycle(
                    candidate_id,
                    "IDEA",
                    payload,
                    reason="fixture seed",
                    timestamp=T0,
                )
                self.store.save_candidate_lifecycle(
                    candidate_id,
                    "FROZEN",
                    payload,
                    from_stage="IDEA",
                    reason="fixture seed",
                    timestamp=T0,
                )
            elif index % 2 == 0:
                self.store.save_candidate_lifecycle(
                    candidate_id,
                    "IDEA",
                    payload,
                    reason="fixture seed",
                    timestamp=T0,
                )
                self.store.save_candidate_lifecycle(
                    candidate_id,
                    "VALIDATED",
                    payload,
                    from_stage="IDEA",
                    reason="fixture seed",
                    timestamp=T0,
                )
            else:
                self.store.save_candidate_lifecycle(
                    candidate_id,
                    "IDEA",
                    payload,
                    reason="fixture seed",
                    timestamp=T0,
                )
        # Persist one canary-eligible candidate before any paper-forward or
        # promotion stage; no historical-gates display value is persisted.
        eligible_record = self.store.load_candidate_lifecycle("candidate-02")
        assert isinstance(eligible_record, dict)
        eligible_payload = eligible_record["payload"]
        self.store.connection.execute(
            "INSERT INTO canary_eligibility(candidate_id,eligible_at,frozen_hash,evidence_json) VALUES (?,?,?,?)",
            (
                "candidate-02",
                T0.isoformat(),
                eligible_payload["frozen_hash"],
                json.dumps(eligible_payload, sort_keys=True),
            ),
        )
        self.store.connection.commit()
    def _seed_ranked_selection(self, candidate_id: str = "dashboard-winner") -> dict[str, object]:
        """Persist one complete candidate so dashboard selection is snapshot-valid."""
        dataset_id = "dashboard-history"
        self.store.save_dataset(
            dataset_id,
            "v1",
            [{"timestamp": T0.isoformat(), "price": 0.5, "source_type": "HISTORICAL"}],
        )
        self.store.save_dataset_catalog(
            dataset_id,
            "v1",
            provider="fixture-provider",
            instrument="POLYMARKET",
            market_type="prediction",
            timeframe="event",
            start_timestamp=T0,
            end_timestamp=T0,
            row_count=1,
            completeness=1.0,
            quality="PRICE_PROXY",
            source_type="HISTORICAL",
            snapshot_id=f"{dataset_id}:v1",
            metadata={
                "provider": "fixture-provider",
                "source_type": "HISTORICAL",
                "research_quality": "PRICE_PROXY",
                "historical_order_book_available": False,
            },
        )
        payload: dict[str, object] = {
            "market_type": "prediction",
            "dataset_id": dataset_id,
            "dataset_version": "v1",
            "dataset_provenance": {
                "dataset_id": dataset_id,
                "dataset_version": "v1",
                "source_type": "HISTORICAL",
                "time_split": "train-validation-holdout",
            },
            "lineage": [candidate_id],
            "mutation_cluster": candidate_id,
            "experiment_family": "dashboard-regression",
            "schema_validated": True,
            "historical_backtest_passed": True,
            "validation_passed": True,
            "robustness_passed": True,
            "data_quality_passed": True,
            "frozen": True,
            "holdout_used": False,
            "strategy_hash": "dashboard-strategy-v1",
            "model_hash": "dashboard-model-v1",
            "config_hash": "dashboard-config-v1",
            "validation_expectancy": 0.40,
            "validation_confidence_lower_bound": 0.35,
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
        }
        payload["frozen_hash"] = hashlib.sha256(
            "|".join(
                str(payload[key])
                for key in ("strategy_hash", "model_hash", "config_hash")
            ).encode()
        ).hexdigest()
        self.store.save_candidate_lifecycle(
            candidate_id,
            "IDEA",
            payload,
            reason="dashboard regression",
            timestamp=T0,
        )
        self.store.save_candidate_lifecycle(
            candidate_id,
            "FROZEN",
            payload,
            from_stage="IDEA",
            reason="dashboard regression",
            timestamp=T0,
        )
        result = CandidateCanaryRanker(self.store, clock=lambda: T0).evaluate_and_select(T0)
        self.assertEqual(result["selected_candidate"], candidate_id)
        self.canary_service.publish_readiness_snapshot(reason="DASHBOARD_FIXTURE")
        return payload
    def _persist_actionable_scan(
        self,
        *,
        candidates_ranked: int,
        candidates_signal_checked: int,
        candidates_no_signal: int,
        actionable_candidates_found: int,
        selected_actionable_candidate: str | None,
        selected_actionable_rank: int | None,
        selected_actionable_score: float | None,
        signal_scan_cursor: int,
        signal_scan_ranking_run_id: str | None,
        next_signal_scan_start_rank: int | None,
        next_signal_scan_end_rank: int | None,
    ) -> None:
        """Persist a completed bounded scan without constructing a venue."""
        self.canary_service.record_autonomous_decision(
            next_decision="WAIT_FOR_FRESH_ACTIONABLE_SIGNAL",
            blocker=(
                None
                if actionable_candidates_found
                else "NO_ACTIONABLE_SIGNAL"
            ),
            worker_status="IDLE",
            timestamp=T0,
            candidates_evaluated=candidates_ranked,
            signals_generated=actionable_candidates_found,
            orders_attempted=0,
            candidates_ranked=candidates_ranked,
            candidates_signal_checked=candidates_signal_checked,
            candidates_no_signal=candidates_no_signal,
            actionable_candidates_found=actionable_candidates_found,
            selected_actionable_candidate=selected_actionable_candidate,
            selected_actionable_rank=selected_actionable_rank,
            selected_actionable_score=selected_actionable_score,
            signal_scan_cursor=signal_scan_cursor,
            signal_scan_ranking_run_id=signal_scan_ranking_run_id,
            next_signal_scan_start_rank=next_signal_scan_start_rank,
            next_signal_scan_end_rank=next_signal_scan_end_rank,
        )
        self.canary_service.publish_readiness_snapshot(
            reason="DASHBOARD_ACTIONABLE_SCAN"
        )


    def _seed_queue(self) -> None:
        for index in range(QUEUE_COUNT):
            payload = {"label": f"hypothesis-{index:02d}", "market": "crypto_spot"}
            if index == 0:
                payload.update(
                    {
                        "plan": {
                            "dataset_selector": {"dataset_id": "fixture-dataset", "dataset_version": "fixture-v1"},
                            "experiment_family": "fixture-family",
                        },
                    }
                )
            self.store.enqueue_research_item(
                "hypothesis",
                payload,
                source="fixture-hermes",
                author="fixture",
                item_id=f"queue-{index:02d}",
                dedupe_key=f"fixture-queue-{index:02d}",
                priority=100 if index == 0 else index % 3,
            )
        claim_time = datetime.now(UTC) + timedelta(seconds=1)
        claimed = self.store.claim_research_item("fixture-worker", now=claim_time)
        self.assertIsNotNone(claimed)
        self.store.complete_research_item(
            "queue-00",
            "ACCEPTED",
            result={"fixture": True},
            now=claim_time + timedelta(seconds=1),
            worker="fixture-worker",
        )


    def _seed_paper(self) -> None:
        for index in range(PAPER_COUNT):
            self.store.save_paper_state(
                f"paper-{index:02d}",
                {
                    "status": "OPEN" if index % 2 == 0 else "CLOSED",
                    "market_id": f"market-{index:02d}",
                    "portfolio": {
                        "equity": 1000.0 + index,
                        "initial_cash": 1000.0,
                        "positions": {},
                    },
                },
                timestamp=T0 + timedelta(minutes=index // 2),
            )

    def _request(self, path: str, **params: object) -> tuple[int, object, str]:
        assert self.server.url is not None
        query = urlencode({key: value for key, value in params.items() if value is not None})
        url = f"{self.server.url}/{path}"
        if query:
            url += "?" + query
        try:
            with urlopen(url, timeout=3) as response:
                body = response.read().decode("utf-8")
                return response.status, json.loads(body), body
        except HTTPError as error:
            body = error.read().decode("utf-8")
            try:
                payload: object = json.loads(body)
            except json.JSONDecodeError:
                payload = body
            return error.code, payload, body

    def _page(
        self,
        path: str,
        *,
        expected_page: int,
        expected_size: int,
        expected_total: int,
        **params: object,
    ) -> dict[str, object]:
        status, payload, _ = self._request(path, **params)
        self.assertEqual(status, 200)
        self.assertIsInstance(payload, dict)
        assert isinstance(payload, dict)
        self.assertTrue(COMMON_PAGE_KEYS <= payload.keys())
        self.assertEqual(payload["page"], expected_page)
        self.assertEqual(payload["page_size"], expected_size)
        self.assertEqual(payload["total"], expected_total)
        self.assertEqual(payload["pages"], (expected_total + expected_size - 1) // expected_size)
        self.assertIsInstance(payload["items"], list)
        self.assertLessEqual(len(payload["items"]), expected_size)
        return payload

    def _assert_bad_request(self, path: str, **params: object) -> None:
        status, payload, _ = self._request(path, **params)
        self.assertEqual(status, 400)
        self.assertIsInstance(payload, dict)
        assert isinstance(payload, dict)
        self.assertIn("error", payload)


class DashboardPaginationEndpointTests(DashboardPaginationFixture):
    def test_canary_snapshot_counts_and_current_selection_survive_store_reopen(self) -> None:
        self._seed_ranked_selection()
        # Keep one persisted raw eligibility row that no longer has an
        # authoritative lifecycle stage.  Raw and validated counts must not
        # collapse into the same readiness number.
        stale_record = self.store.load_candidate_lifecycle("candidate-02")
        assert isinstance(stale_record, dict)
        stale_payload = stale_record["payload"]
        self.store.save_candidate_lifecycle(
            "candidate-02",
            "REJECTED",
            stale_payload,
            from_stage="FROZEN",
            reason="dashboard stale fixture",
            timestamp=T0 + timedelta(minutes=1),
        )
        self.store.connection.execute(
            "INSERT OR REPLACE INTO canary_eligibility("
            "candidate_id,eligible_at,frozen_hash,evidence_json) VALUES (?,?,?,?)",
            (
                "candidate-02",
                T0.isoformat(),
                stale_payload["frozen_hash"],
                json.dumps(stale_payload, sort_keys=True),
            ),
        )
        self.store.connection.commit()
        self.store.connection.execute(
            "INSERT OR REPLACE INTO canary_rankings("
            "candidate_id,ranking_run_id,ranking_timestamp,rank,total_score,"
            "component_scores_json,evidence_versions_json,cluster_key,"
            "cluster_representative,selected,reason) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (
                "ranking-invalid",
                "ranking-raw-only",
                T0.isoformat(),
                99,
                0.01,
                "{}",
                "{}",
                "raw-only",
                0,
                0,
                "RAW_ONLY",
            ),
        )
        self.store.connection.commit()
        self.canary_service.publish_readiness_snapshot(reason="DASHBOARD_FIXTURE")

        status, payload, _ = self._request("api/v2/canary")
        self.assertEqual(status, 200)
        self.assertIsInstance(payload, dict)
        assert isinstance(payload, dict)
        canary = payload["canary"]
        autonomous = payload["autonomous_canary"]
        self.assertEqual(canary["selection_status"], "CURRENT")
        self.assertIs(canary["selection_valid"], True)
        self.assertEqual(canary["selected_candidate"], "dashboard-winner")
        self.assertEqual(canary["last_selected_candidate"], "dashboard-winner")
        self.assertEqual(canary["winner_id"], "dashboard-winner")
        self.assertIsInstance(canary["ranking_run_id"], str)
        self.assertEqual(canary["ranking_timestamp"], T0.isoformat())
        self.assertIsNone(canary["selection_invalidation_reason"])
        self.assertGreaterEqual(canary["eligibility_raw_count"], canary["eligible_count"])
        self.assertGreaterEqual(canary["rankable_raw_count"], canary["rankable_count"])
        for projection in (canary, autonomous):
            self.assertEqual(projection["selection_status"], "CURRENT")
            self.assertIs(projection["selection_valid"], True)
            self.assertEqual(projection["selected_candidate"], "dashboard-winner")
            self.assertEqual(projection["last_selected_candidate"], "dashboard-winner")
            self.assertEqual(projection["winner_id"], "dashboard-winner")
            self.assertIsInstance(projection["ranking_run_id"], str)
            self.assertEqual(projection["ranking_timestamp"], T0.isoformat())
            self.assertGreaterEqual(
                projection["eligibility_raw_count"],
                projection["eligible_count"],
            )
            self.assertGreaterEqual(
                projection["rankable_raw_count"],
                projection["rankable_count"],
            )

        reopened = AxiomStore(self.store.path)
        self.addCleanup(reopened.close)
        self.server.data.store = reopened
        status, reopened_payload, _ = self._request("api/v2/canary")
        self.assertEqual(status, 200)
        self.assertIsInstance(reopened_payload, dict)
        assert isinstance(reopened_payload, dict)
        reopened_canary = reopened_payload["canary"]
        self.assertEqual(reopened_canary["selection_status"], "CURRENT")
        self.assertIs(reopened_canary["selection_valid"], True)
        self.assertEqual(reopened_canary["selected_candidate"], "dashboard-winner")
        self.assertEqual(reopened_canary["last_selected_candidate"], "dashboard-winner")
        self.assertEqual(reopened_canary["ranking_run_id"], canary["ranking_run_id"])
        self.assertEqual(reopened_canary["ranking_timestamp"], T0.isoformat())

    def test_canary_stale_selection_keeps_history_without_executable_fallback(self) -> None:
        self._seed_ranked_selection("dashboard-stale")
        # A ranking run is historical evidence, not a current executable
        # selection once its ranking rows are no longer available.
        self.store.connection.execute("DELETE FROM canary_rankings")
        self.store.connection.commit()
        self.canary_service.mark_readiness_snapshot_stale(reason="REEVALUATION_REQUIRED")

        status, payload, body = self._request("api/v2/canary")
        self.assertEqual(status, 200)
        self.assertIsInstance(payload, dict)
        assert isinstance(payload, dict)
        canary = payload["canary"]
        autonomous = payload["autonomous_canary"]
        for projection in (canary, autonomous):
            self.assertEqual(projection["selection_status"], "STALE")
            self.assertIs(projection["selection_valid"], False)
            self.assertIsNone(projection["selected_candidate"])
            self.assertIsNone(projection["winner_id"])
            self.assertEqual(projection["last_selected_candidate"], "dashboard-stale")
            self.assertEqual(projection["eligible_count"], 1)
            self.assertEqual(projection["rankable_count"], 1)
            self.assertEqual(projection["selection_reason"], "SELECTED_WINNER")
            self.assertEqual(
                projection["selection_invalidation_reason"],
                "REEVALUATION_REQUIRED",
            )
        self.assertIn("dashboard-stale", body)
        selected_winner = canary.get("selected_winner")
        self.assertIsInstance(selected_winner, dict)
        assert isinstance(selected_winner, dict)
        self.assertEqual(selected_winner["selection_status"], "STALE")
        self.assertIs(selected_winner["selection_valid"], False)
        self.assertEqual(
            selected_winner["selection_invalidation_reason"],
            "REEVALUATION_REQUIRED",
        )
    def test_canary_endpoint_separates_research_winner_from_actionable_candidate(self) -> None:
        self._seed_ranked_selection("research-winner")
        status, before, _ = self._request("api/v2/canary")
        self.assertEqual(status, 200)
        self.assertIsInstance(before, dict)
        assert isinstance(before, dict)
        research = before["canary"]
        ranking_run_id = research["ranking_run_id"]
        research_score = research["winner_score"]
        self.assertEqual(research["winner_id"], "research-winner")
        self.assertEqual(research["winner_rank"], 1)
        self.assertIsInstance(research_score, (int, float))
        self.assertIsInstance(ranking_run_id, str)

        self._persist_actionable_scan(
            candidates_ranked=20,
            candidates_signal_checked=10,
            candidates_no_signal=9,
            actionable_candidates_found=1,
            selected_actionable_candidate="current-actionable",
            selected_actionable_rank=2,
            selected_actionable_score=0.73,
            signal_scan_cursor=0,
            signal_scan_ranking_run_id=ranking_run_id,
            next_signal_scan_start_rank=1,
            next_signal_scan_end_rank=10,
        )
        status, payload, _ = self._request("api/v2/canary")
        self.assertEqual(status, 200)
        self.assertIsInstance(payload, dict)
        assert isinstance(payload, dict)
        for projection in (payload["canary"], payload["autonomous_canary"]):
            self.assertEqual(projection["winner_id"], "research-winner")
            self.assertEqual(projection["winner_rank"], 1)
            self.assertEqual(projection["winner_score"], research_score)
            self.assertEqual(projection["selected_actionable_candidate"], "current-actionable")
            self.assertEqual(projection["selected_actionable_rank"], 2)
            self.assertEqual(projection["selected_actionable_score"], 0.73)
            self.assertEqual(projection["signal_scan_ranking_run_id"], ranking_run_id)
            self.assertEqual(
                {name: projection[name] for name in ACTIONABLE_SCAN_FIELDS},
                {
                    "candidates_ranked": 20,
                    "candidates_signal_checked": 10,
                    "candidates_no_signal": 9,
                    "actionable_candidates_found": 1,
                    "selected_actionable_candidate": "current-actionable",
                    "selected_actionable_rank": 2,
                    "selected_actionable_score": 0.73,
                    "signal_scan_cursor": 0,
                    "signal_scan_ranking_run_id": ranking_run_id,
                    "next_signal_scan_start_rank": 1,
                    "next_signal_scan_end_rank": 10,
                },
            )

    def test_canary_endpoint_reports_none_and_advances_after_no_signal_window(self) -> None:
        ranking = CandidateCanaryRanker(self.store, clock=lambda: T0).evaluate_and_select(T0)
        self.assertEqual(ranking["selection_status"], "NONE")
        ranking_run_id = ranking["ranking_run_id"]
        self._persist_actionable_scan(
            candidates_ranked=20,
            candidates_signal_checked=10,
            candidates_no_signal=10,
            actionable_candidates_found=0,
            selected_actionable_candidate=None,
            selected_actionable_rank=None,
            selected_actionable_score=None,
            signal_scan_cursor=10,
            signal_scan_ranking_run_id=ranking_run_id,
            next_signal_scan_start_rank=11,
            next_signal_scan_end_rank=20,
        )

        status, payload, body = self._request("api/v2/canary")
        self.assertEqual(status, 200)
        self.assertIsInstance(payload, dict)
        assert isinstance(payload, dict)
        for projection in (payload["canary"], payload["autonomous_canary"]):
            self.assertEqual(projection["selection_status"], "NONE")
            self.assertIsNone(projection["winner_id"])
            self.assertIsNone(projection["winner_rank"])
            self.assertIsNone(projection["winner_score"])
            self.assertIsNone(projection["selected_actionable_candidate"])
            self.assertIsNone(projection["selected_actionable_rank"])
            self.assertIsNone(projection["selected_actionable_score"])
            self.assertEqual(projection["candidates_ranked"], 20)
            self.assertEqual(projection["candidates_signal_checked"], 10)
            self.assertEqual(projection["candidates_no_signal"], 10)
            self.assertEqual(projection["actionable_candidates_found"], 0)
            self.assertEqual(projection["signal_scan_cursor"], 10)
            self.assertEqual(projection["signal_scan_ranking_run_id"], ranking_run_id)
            self.assertEqual(projection["next_signal_scan_start_rank"], 11)
            self.assertEqual(projection["next_signal_scan_end_rank"], 20)
        self.assertIn("NONE", body)

    def test_canary_endpoint_keeps_actionable_scan_unknown_before_completed_scan(self) -> None:
        status, payload, body = self._request("api/v2/canary")
        self.assertEqual(status, 200)
        self.assertIsInstance(payload, dict)
        assert isinstance(payload, dict)
        for projection in (payload["canary"], payload["autonomous_canary"]):
            self.assertEqual(projection["selection_status"], "UNKNOWN")
            for field in ACTIONABLE_SCAN_FIELDS:
                if field == "signal_scan_cursor":
                    self.assertEqual(projection[field], 0)
                else:
                    self.assertIsNone(
                        projection[field],
                        f"{field} must remain UNKNOWN/null without a completed scan",
                    )
        self.assertIn("UNKNOWN", body)


    def test_datasets_cover_page_navigation_filters_and_detail_path(self) -> None:
        first = self._page(
            "api/v2/datasets",
            page=1,
            page_size=10,
            sort="dataset_id",
            direction="asc",
            expected_page=1,
            expected_size=10,
            expected_total=DATASET_COUNT + 1,
        )
        self.assertEqual(
            [item["dataset_id"] for item in first["items"]],
            [f"dataset-{index:02d}" for index in range(10)],
        )

        middle = self._page(
            "api/v2/datasets",
            page=2,
            page_size=10,
            sort="dataset_id",
            direction="asc",
            expected_page=2,
            expected_size=10,
            expected_total=DATASET_COUNT + 1,
        )
        self.assertEqual(
            [item["dataset_id"] for item in middle["items"]],
            [f"dataset-{index:02d}" for index in range(10, 20)],
        )

        final = self._page(
            "api/v2/datasets",
            page=3,
            page_size=10,
            sort="dataset_id",
            direction="asc",
            expected_page=3,
            expected_size=10,
            expected_total=DATASET_COUNT + 1,
        )
        self.assertEqual(
            [item["dataset_id"] for item in final["items"]],
            [f"dataset-{index:02d}" for index in range(20, 24)] + [self.detail_dataset_id],
        )

        out_of_range = self._page(
            "api/v2/datasets",
            page=4,
            page_size=10,
            sort="dataset_id",
            direction="asc",
            expected_page=3,
            expected_size=10,
            expected_total=DATASET_COUNT + 1,
        )
        self.assertEqual(
            [item["dataset_id"] for item in out_of_range["items"]],
            [f"dataset-{index:02d}" for index in range(20, 24)] + [self.detail_dataset_id],
        )

        historical = self._page(
            "api/v2/datasets",
            page=2,
            page_size=10,
            source_type="HISTORICAL",
            timeframe="1d",
            sort="dataset_id",
            direction="asc",
            expected_page=2,
            expected_size=10,
            expected_total=12,
        )
        self.assertEqual([item["dataset_id"] for item in historical["items"]], ["dataset-20", "dataset-22"])
        self.assertTrue(all(item["source_type"] == "HISTORICAL" for item in historical["items"]))

        crypto = self._page(
            "api/v2/datasets",
            page=1,
            page_size=10,
            market="crypto_spot",
            sort="dataset_id",
            direction="asc",
            expected_page=1,
            expected_size=10,
            expected_total=17,
        )
        self.assertTrue(all(item["market_type"] == "crypto_spot" for item in crypto["items"]))

        high_quality = self._page(
            "api/v2/datasets",
            page=1,
            page_size=10,
            quality="HIGH",
            sort="dataset_id",
            direction="asc",
            expected_page=1,
            expected_size=10,
            expected_total=18,
        )
        self.assertTrue(all(item["quality"] == "HIGH" for item in high_quality["items"]))

        status, detail, _ = self._request(f"api/v2/datasets/{quote(self.detail_dataset_id, safe='')}")
        self.assertEqual(status, 200)
        self.assertIsInstance(detail, dict)
        assert isinstance(detail, dict)
        self.assertTrue(detail["available"])
        self.assertEqual(detail["dataset_id"], self.detail_dataset_id)
        self.assertEqual(detail["catalog"]["dataset_id"], self.detail_dataset_id)
        self.assertEqual(detail["catalog"]["dataset_version"], "v1")
        self.assertEqual(detail["catalog"]["row_count"], 1)

    def test_dataset_missing_ranges_are_paginated(self) -> None:
        path = f"api/v2/datasets/{quote('dataset-00', safe='')}/missing-ranges"
        first = self._page(
            path,
            page=1,
            page_size=10,
            sort="range_index",
            direction="asc",
            expected_page=1,
            expected_size=10,
            expected_total=23,
        )
        self.assertEqual([item["range_index"] for item in first["items"]], list(range(10)))
        self.assertEqual(first["items"][0]["dataset_id"], "dataset-00")
        self.assertEqual(first["items"][0]["dataset_version"], "v1")
        self.assertEqual(first["items"][0]["range"], first["items"][0]["missing_range"])
        self.assertEqual(first["items"][0]["range"]["start"], "2024-02-01T00:00:00+00:00")

        final = self._page(
            path,
            page=3,
            page_size=10,
            sort="range_index",
            direction="asc",
            expected_page=3,
            expected_size=10,
            expected_total=23,
        )
        self.assertEqual([item["range_index"] for item in final["items"]], [20, 21, 22])

        out_of_range = self._page(
            path,
            page=4,
            page_size=10,
            sort="range_index",
            direction="asc",
            expected_page=3,
            expected_size=10,
            expected_total=23,
        )
        self.assertEqual([item["range_index"] for item in out_of_range["items"]], [20, 21, 22])

    def test_candidate_lifecycle_events_are_paginated(self) -> None:
        path = f"api/v2/candidates/{quote('candidate-00', safe='')}/events"
        first = self._page(
            path,
            page=1,
            page_size=10,
            sort="event_id",
            direction="asc",
            expected_page=1,
            expected_size=10,
            expected_total=12,
        )
        self.assertEqual(len(first["items"]), 10)
        self.assertTrue(all(item["candidate_id"] == "candidate-00" for item in first["items"]))
        self.assertTrue({"event_id", "candidate_id", "to_stage", "reason", "payload", "created_at"} <= first["items"][0].keys())

        final = self._page(
            path,
            page=2,
            page_size=10,
            sort="event_id",
            direction="asc",
            expected_page=2,
            expected_size=10,
            expected_total=12,
        )
        self.assertEqual(len(final["items"]), 2)
        self.assertTrue(all(item["candidate_id"] == "candidate-00" for item in final["items"]))


    def test_candidate_pagination_filters_and_stable_tie_order(self) -> None:
        page = self._page(
            "api/v2/candidates",
            page=1,
            page_size=10,
            sort="updated_at",
            direction="asc",
            expected_page=1,
            expected_size=10,
            expected_total=CANDIDATE_COUNT,
        )
        self.assertEqual(
            [item["candidate_id"] for item in page["items"]],
            [f"candidate-{index:02d}" for index in range(10)],
        )
        for item in page["items"]:
            self.assertTrue({"candidate_id", "stage", "payload", "updated_at"} <= item.keys())

        validated = self._page(
            "api/v2/candidates",
            page=1,
            page_size=10,
            stage="VALIDATED",
            filter="candidate-1",
            sort="candidate_id",
            direction="asc",
            expected_page=1,
            expected_size=10,
            expected_total=5,
        )
        self.assertEqual(
            [item["candidate_id"] for item in validated["items"]],
            ["candidate-10", "candidate-12", "candidate-14", "candidate-16", "candidate-18"],
        )
        self.assertTrue(all(item["stage"] == "VALIDATED" for item in validated["items"]))

        descending = self._page(
            "api/v2/candidates",
            page=2,
            page_size=10,
            sort="candidate_id",
            direction="desc",
            expected_page=2,
            expected_size=10,
            expected_total=CANDIDATE_COUNT,
        )
        self.assertEqual(
            [item["candidate_id"] for item in descending["items"]],
            [f"candidate-{index:02d}" for index in range(12, 2, -1)],
        )
    def test_candidate_statuses_keep_canary_and_paper_lifecycle_distinct(self) -> None:
        # Replace the sparse fixture row with complete, bound prediction
        # qualification evidence while preserving its FROZEN lifecycle stage.
        qualified_dataset_id = "candidate-02-qualification"
        self.store.save_dataset(
            qualified_dataset_id,
            "v1",
            [{"timestamp": T0.isoformat(), "price": 0.5, "source_type": "HISTORICAL"}],
        )
        self.store.save_dataset_catalog(
            qualified_dataset_id,
            "v1",
            provider="fixture-provider",
            instrument="POLYMARKET",
            market_type="prediction",
            timeframe="event",
            start_timestamp=T0,
            end_timestamp=T0,
            row_count=1,
            completeness=1.0,
            quality="PRICE_PROXY",
            source_type="HISTORICAL",
            snapshot_id=f"{qualified_dataset_id}:v1",
            metadata={
                "provider": "fixture-provider",
                "source_type": "HISTORICAL",
                "research_quality": "PRICE_PROXY",
                "historical_order_book_available": False,
            },
        )
        existing = self.store.load_candidate_lifecycle("candidate-02")
        assert isinstance(existing, dict)
        existing_payload = existing["payload"]
        assert isinstance(existing_payload, dict)
        qualified_payload = {
            **existing_payload,
            "candidate_id": "candidate-02",
            "instrument": "POLYMARKET",
            "source_type": "HISTORICAL",
            "timeframe": "event",
            "market_type": "prediction",
            "dataset_id": qualified_dataset_id,
            "dataset_version": "v1",
            "dataset_provenance": {
                "dataset_id": qualified_dataset_id,
                "dataset_version": "v1",
                "source_type": "HISTORICAL",
                "time_split": "train-validation-holdout",
            },
            "lineage": ["candidate-02"],
            "mutation_cluster": "candidate-02",
            "schema_validated": True,
            "historical_backtest_passed": True,
            "validation_passed": True,
            "robustness_passed": True,
            "data_quality_passed": True,
            "data_quality": "PRICE_PROXY",
            "frozen": True,
            "holdout_used": False,
            "critical_error": None,
            "strategy_hash": "candidate-02-strategy-v1",
            "model_hash": "candidate-02-model-v1",
            "config_hash": "candidate-02-config-v1",
            "validation_expectancy": 0.40,
            "validation_confidence_lower_bound": 0.35,
            "validation_stability": 0.90,
            "validation_calibration": 0.90,
            "validation_sample_count": 100,
            "validation_trade_count": 50,
            "validation_execution_quality": 0.90,
            "minimum_sample_check": {
                "passed": True,
                "count": 100,
                "trades": 50,
                "checks": {"observations": True, "trades": True},
            },
        }
        qualified_payload["frozen_hash"] = hashlib.sha256(
            "|".join(
                str(qualified_payload[key])
                for key in ("strategy_hash", "model_hash", "config_hash")
            ).encode()
        ).hexdigest()
        self.store.save_candidate_lifecycle(
            "candidate-02",
            "FROZEN",
            qualified_payload,
            from_stage="FROZEN",
            reason="complete prediction qualification fixture",
            timestamp=T0,
        )
        self.canary_service.mark_eligible("candidate-02")
        self.canary_service.publish_readiness_snapshot(reason="DASHBOARD_FIXTURE")
        page = self._page(
            "api/v2/candidates",
            page=1,
            page_size=10,
            sort="candidate_id",
            direction="asc",
            expected_page=1,
            expected_size=10,
            expected_total=CANDIDATE_COUNT,
        )
        eligible_before_paper = next(item for item in page["items"] if item["candidate_id"] == "candidate-02")
        self.assertEqual(eligible_before_paper["stage"], "FROZEN")
        self.assertEqual(eligible_before_paper["historical_gates"], "NOT_PASSED")
        self.assertTrue(eligible_before_paper["canary_eligible"])
        self.assertEqual(eligible_before_paper["canary_status"], "ELIGIBLE")
        self.assertFalse(eligible_before_paper["paper_forward"])
        self.assertEqual(eligible_before_paper["paper_forward_status"], "NOT_STARTED")
        self.assertFalse(eligible_before_paper["paper_promotable"])
        self.assertEqual(eligible_before_paper["paper_promotable_status"], "NOT_YET")

        promoted = next(item for item in page["items"] if item["candidate_id"] == "candidate-00")
        self.assertTrue(promoted["paper_forward"])
        self.assertTrue(promoted["paper_promotable"])
        self.assertFalse(promoted["canary_eligible"])
        self.assertEqual(promoted["historical_gates"], "NOT_PASSED")

        status, detail, _ = self._request("api/v2/candidates/candidate-02")
        self.assertEqual(status, 200)
        assert isinstance(detail, dict)
        self.assertEqual(detail["historical_gates"], "NOT_PASSED")
        self.assertEqual(detail["canary_status"], "ELIGIBLE")
        self.assertEqual(detail["paper_forward_status"], "NOT_STARTED")
        self.assertEqual(detail["paper_promotable_status"], "NOT_YET")

        status, operator, _ = self._request("api/operator")
        self.assertEqual(status, 200)
        assert isinstance(operator, dict)
        self.assertEqual(operator["candidate_status"]["canary_eligible"], 1)
        self.assertEqual(operator["candidate_status"]["paper_forward"], 1)
        self.assertEqual(operator["candidate_status"]["paper_promotable"], 1)
        self.store.connection.execute(
            "UPDATE canary_eligibility SET frozen_hash=? WHERE candidate_id=?",
            ("tampered-frozen-hash", "candidate-02"),
        )
        self.store.connection.commit()
        stale_page = self._page(
            "api/v2/candidates",
            page=1,
            page_size=10,
            sort="candidate_id",
            direction="asc",
            expected_page=1,
            expected_size=10,
            expected_total=CANDIDATE_COUNT,
        )
        stale = next(item for item in stale_page["items"] if item["candidate_id"] == "candidate-02")
        self.assertTrue(stale["canary_eligible"])
        self.assertEqual(stale["canary_status"], "ELIGIBLE")
        status, operator, _ = self._request("api/operator")
        self.assertEqual(status, 200)
        assert isinstance(operator, dict)
        self.assertEqual(operator["candidate_status"]["canary_eligible"], 1)


    @staticmethod
    def _market_value(item: dict[str, object], key: str) -> object:
        if key in item:
            return item[key]
        payload = item.get("payload")
        if isinstance(payload, dict):
            if key in payload:
                return payload[key]
            for nested_key in ("snapshot", "metadata"):
                nested = payload.get(nested_key)
                if isinstance(nested, dict) and key in nested:
                    return nested[key]
        return None

    def test_polymarket_pagination_and_market_filters(self) -> None:
        page = self._page(
            "api/v2/polymarket",
            page=1,
            page_size=10,
            sort="market_id",
            direction="asc",
            expected_page=1,
            expected_size=10,
            expected_total=MARKET_COUNT,
        )
        self.assertEqual(
            [item["market_id"] for item in page["items"]],
            [f"market-{index:02d}" for index in range(10)],
        )
        for item in page["items"]:
            self.assertTrue({"market_id", "payload", "observed_at", "quality"} <= item.keys())

        weather_count = len(range(0, MARKET_COUNT, 3))
        weather = self._page(
            "api/v2/polymarket",
            page=1,
            page_size=10,
            category="weather",
            sort="market_id",
            direction="asc",
            expected_page=1,
            expected_size=10,
            expected_total=weather_count,
        )
        self.assertEqual(
            [item["market_id"] for item in weather["items"]],
            [f"market-{index:02d}" for index in range(0, MARKET_COUNT, 3)],
        )
        self.assertTrue(all(self._market_value(item, "category") == "weather" for item in weather["items"]))

        open_markets = self._page(
            "api/v2/polymarket",
            page=2,
            page_size=10,
            settlement="open",
            quality="ORDER_BOOK_SIMULATED",
            sort="market_id",
            direction="asc",
            expected_page=2,
            expected_size=10,
            expected_total=11,
        )
        self.assertEqual(
            [item["market_id"] for item in open_markets["items"]],
            ["market-22"],
        )
        self.assertTrue(all(self._market_value(item, "settlement") == "open" for item in open_markets["items"]))
        self.assertTrue(all(item["quality"] == "ORDER_BOOK_SIMULATED" for item in open_markets["items"]))
        self.assertTrue(all("quality_label" in item and "quality_context" in item for item in open_markets["items"]))
        self.assertEqual(open_markets["items"][0]["quality_label"], "CURRENT ORDER BOOK · SIMULATED EXECUTION")

    def test_activity_is_paginated_and_uses_deterministic_tie_breaking(self) -> None:
        page = self._page(
            "api/v2/activity",
            page=1,
            page_size=10,
            filter="dataset-",
            sort="timestamp",
            direction="asc",
            expected_page=1,
            expected_size=10,
            expected_total=DATASET_COUNT,
        )
        self.assertTrue(all(item["kind"] == "dataset" for item in page["items"]))
        self.assertEqual(page["items"][0]["details"]["source_type"], "HISTORICAL")
        self.assertIn("dataset-00", page["items"][0]["message"])
        self.assertIn("dataset-01", page["items"][1]["message"])

        final = self._page(
            "api/v2/activity",
            page=3,
            page_size=10,
            filter="dataset-",
            sort="timestamp",
            direction="asc",
            expected_page=3,
            expected_size=10,
            expected_total=DATASET_COUNT,
        )
        self.assertEqual([item["message"] for item in final["items"]], [
            "Dataset dataset-20 published (21 rows)",
            "Dataset dataset-21 published (22 rows)",
            "Dataset dataset-22 published (23 rows)",
            "Dataset dataset-23 published (24 rows)",
        ])

    def test_hermes_and_paper_records_are_page_bounded(self) -> None:
        hermes = self._page(
            "api/v2/hermes",
            page=1,
            page_size=25,
            sort="item_id",
            direction="asc",
            expected_page=1,
            expected_size=25,
            expected_total=QUEUE_COUNT,
        )
        self.assertEqual(len(hermes["items"]), QUEUE_COUNT)
        self.assertEqual(hermes["items"][0]["item_id"], "queue-00")
        self.assertTrue(all({"item_id", "item_type", "status", "payload"} <= item.keys() for item in hermes["items"]))

        accepted = self._page(
            "api/v2/hermes",
            page=1,
            page_size=10,
            status="ACCEPTED",
            sort="item_id",
            direction="asc",
            expected_page=1,
            expected_size=10,
            expected_total=1,
        )
        self.assertEqual(accepted["items"][0]["item_id"], "queue-00")
        self.assertEqual(accepted["items"][0]["status"], "ACCEPTED")
        status, detail, _ = self._request("api/v2/hermes/queue-00")
        self.assertEqual(status, 200)
        self.assertEqual(detail["dataset_id"], "fixture-dataset")
        self.assertEqual(detail["dataset_version"], "fixture-v1")
        self.assertEqual(detail["family"], "fixture-family")

        first_paper = self._page(
            "api/v2/paper",
            page=1,
            page_size=10,
            sort="experiment_id",
            direction="asc",
            expected_page=1,
            expected_size=10,
            expected_total=PAPER_COUNT,
        )
        self.assertEqual(
            [item["experiment_id"] for item in first_paper["items"]],
            [f"paper-{index:02d}" for index in range(10)],
        )
        self.assertTrue(all({"record_type", "experiment_id", "timestamp", "status", "payload"} <= item.keys() for item in first_paper["items"]))

        final_paper = self._page(
            "api/v2/paper",
            page=3,
            page_size=10,
            sort="experiment_id",
            direction="asc",
            expected_page=3,
            expected_size=10,
            expected_total=PAPER_COUNT,
        )
        self.assertEqual(
            [item["experiment_id"] for item in final_paper["items"]],
            [f"paper-{index:02d}" for index in range(20, PAPER_COUNT)],
        )

        open_paper = self._page(
            "api/v2/paper",
            page=2,
            page_size=10,
            status="OPEN",
            sort="experiment_id",
            direction="asc",
            expected_page=2,
            expected_size=10,
            expected_total=12,
        )
        self.assertEqual([item["experiment_id"] for item in open_paper["items"]], ["paper-20", "paper-22"])
        self.assertTrue(all(item["status"] == "OPEN" for item in open_paper["items"]))


    def test_every_ui_sort_column_has_a_supported_paged_endpoint(self) -> None:
        # These are the backend keys emitted by each table's sort buttons.
        # A click must not silently produce an empty page or a server error.
        sortable_columns = {
            "api/v2/datasets": (
                "dataset_id",
                "source_type",
                "market_type",
                "instrument",
                "timeframe",
                "quality",
                "row_count",
                "updated_at",
            ),
            "api/v2/activity": ("timestamp", "kind"),
            "api/v2/candidates": ("candidate_id", "stage", "updated_at"),
            "api/v2/polymarket": ("market_id", "category", "settlement", "quality"),
            "api/v2/hermes": ("item_id", "item_type", "status", "created_at"),
            "api/v2/paper": ("record_type", "experiment_id", "market_id", "status", "timestamp"),
        }
        totals = {
            "api/v2/datasets": DATASET_COUNT + 1,
            "api/v2/activity": ACTIVITY_COUNT,
            "api/v2/candidates": CANDIDATE_COUNT,
            "api/v2/polymarket": MARKET_COUNT,
            "api/v2/hermes": QUEUE_COUNT,
            "api/v2/paper": PAPER_COUNT,
        }
        for endpoint, columns in sortable_columns.items():
            for column in columns:
                page = self._page(
                    endpoint,
                    page=1,
                    page_size=10,
                    sort=column,
                    direction="asc",
                    expected_page=1,
                    expected_size=10,
                    expected_total=totals[endpoint],
                )
                self.assertLessEqual(len(page["items"]), 10)

    def test_all_allowed_page_sizes_return_bounded_numbered_pages(self) -> None:
        for size in (10, 25, 50, 100):
            page = self._page(
                "api/v2/datasets",
                page=1,
                page_size=size,
                sort="dataset_id",
                direction="asc",
                expected_page=1,
                expected_size=size,
                expected_total=DATASET_COUNT + 1,
            )
            self.assertLessEqual(len(page["items"]), size)

    def test_persisted_v2_endpoints_do_not_wait_for_operator_control_status(self) -> None:
        # Persist one complete FROZEN prediction qualification so the
        # endpoint count reflects validated evidence, not a raw PASS badge.
        qualified_dataset_id = "candidate-02-qualification"
        self.store.save_dataset(
            qualified_dataset_id,
            "v1",
            [{"timestamp": T0.isoformat(), "price": 0.5, "source_type": "HISTORICAL"}],
        )
        self.store.save_dataset_catalog(
            qualified_dataset_id,
            "v1",
            provider="fixture-provider",
            instrument="POLYMARKET",
            market_type="prediction",
            timeframe="event",
            start_timestamp=T0,
            end_timestamp=T0,
            row_count=1,
            completeness=1.0,
            quality="PRICE_PROXY",
            source_type="HISTORICAL",
            snapshot_id=f"{qualified_dataset_id}:v1",
            metadata={
                "provider": "fixture-provider",
                "source_type": "HISTORICAL",
                "research_quality": "PRICE_PROXY",
                "historical_order_book_available": False,
            },
        )
        existing = self.store.load_candidate_lifecycle("candidate-02")
        assert isinstance(existing, dict)
        existing_payload = existing["payload"]
        assert isinstance(existing_payload, dict)
        qualified_payload = {
            **existing_payload,
            "candidate_id": "candidate-02",
            "instrument": "POLYMARKET",
            "source_type": "HISTORICAL",
            "timeframe": "event",
            "market_type": "prediction",
            "dataset_id": qualified_dataset_id,
            "dataset_version": "v1",
            "dataset_provenance": {
                "dataset_id": qualified_dataset_id,
                "dataset_version": "v1",
                "source_type": "HISTORICAL",
                "time_split": "train-validation-holdout",
            },
            "lineage": ["candidate-02"],
            "mutation_cluster": "candidate-02",
            "schema_validated": True,
            "historical_backtest_passed": True,
            "validation_passed": True,
            "robustness_passed": True,
            "data_quality_passed": True,
            "data_quality": "PRICE_PROXY",
            "frozen": True,
            "holdout_used": False,
            "critical_error": None,
            "strategy_hash": "candidate-02-strategy-v1",
            "model_hash": "candidate-02-model-v1",
            "config_hash": "candidate-02-config-v1",
            "validation_expectancy": 0.40,
            "validation_confidence_lower_bound": 0.35,
            "validation_stability": 0.90,
            "validation_calibration": 0.90,
            "validation_sample_count": 100,
            "validation_trade_count": 50,
            "validation_execution_quality": 0.90,
            "minimum_sample_check": {
                "passed": True,
                "count": 100,
                "trades": 50,
                "checks": {"observations": True, "trades": True},
            },
        }
        qualified_payload["frozen_hash"] = hashlib.sha256(
            "|".join(
                str(qualified_payload[key])
                for key in ("strategy_hash", "model_hash", "config_hash")
            ).encode()
        ).hexdigest()
        self.store.save_candidate_lifecycle(
            "candidate-02",
            "FROZEN",
            qualified_payload,
            from_stage="FROZEN",
            reason="complete prediction qualification fixture",
            timestamp=T0,
        )
        self.canary_service.mark_eligible("candidate-02")
        self.canary_service.publish_readiness_snapshot(reason="DASHBOARD_FIXTURE")
        # Leave the candidate eligible but without a current ranking snapshot:
        # it must count as persisted eligibility and not as rankable evidence.
        persisted = self.store.dashboard_overview_summary(activity_limit=8)
        control = _BlockingOperatorControl()
        self.server.data.control = control
        operator_result: list[tuple[int, object, str]] = []
        operator_errors: list[BaseException] = []
        endpoint_threads: list[threading.Thread] = []

        def request_operator() -> None:
            try:
                operator_result.append(self._request("api/operator"))
            except BaseException as exc:  # pragma: no cover - only captures thread failures
                operator_errors.append(exc)

        def request_before_control_release(path: str) -> tuple[int, object, str]:
            result: list[tuple[int, object, str]] = []
            errors: list[BaseException] = []
            done = threading.Event()

            def request_endpoint() -> None:
                try:
                    result.append(self._request(path))
                except BaseException as exc:  # pragma: no cover - only captures thread failures
                    errors.append(exc)
                finally:
                    done.set()

            endpoint_thread = threading.Thread(target=request_endpoint, daemon=True)
            endpoint_threads.append(endpoint_thread)
            endpoint_thread.start()
            self.assertTrue(
                done.wait(timeout=2),
                f"{path} waited for the slow operator-control status path",
            )
            endpoint_thread.join(timeout=1)
            self.assertFalse(endpoint_thread.is_alive())
            self.assertFalse(errors)
            self.assertEqual(len(result), 1)
            return result[0]

        operator_thread = threading.Thread(target=request_operator, daemon=True)
        operator_thread.start()
        try:
            self.assertTrue(
                control.status_started.wait(timeout=1),
                "the control status request did not enter its bounded synchronization point",
            )
            status, overview, _ = request_before_control_release("api/v2/overview-summary")
            self.assertEqual(status, 200)
            self.assertIsInstance(overview, dict)
            assert isinstance(overview, dict)
            historical = persisted["catalog"]["historical"]
            self.assertEqual(overview["coverage"]["historical_count"], historical["datasets"])
            self.assertEqual(overview["latest_activity"], _jsonable(persisted["latest_activity"]))
            self.assertGreater(overview["research_cards"]["canary_eligible"], 0)

            status, canary, _ = request_before_control_release("api/v2/canary")
            self.assertEqual(status, 200)
            self.assertIsInstance(canary, dict)
            assert isinstance(canary, dict)
            canary_status = canary["canary"]
            self.assertEqual(canary_status["micro_live_canary"], "UNKNOWN")
            self.assertEqual(canary_status["eligible_count"], 1)
            self.assertEqual(canary_status["rankable_count"], 0)
            self.assertEqual(canary_status["execution_event_count"], 0)
            autonomous = canary["autonomous_canary"]
            self.assertEqual(autonomous["eligible_count"], 1)
            self.assertEqual(autonomous["rankable_count"], 0)
            self.assertIsNone(canary["canary_signal"])
            self.assertEqual(canary["research_cards"]["canary_eligible"], 1)
            self.assertEqual(canary["candidate_status"]["rankable"], 0)
            self.assertEqual(canary["real_execution_events"], 0)
            credentials = canary["credentials"]
            self.assertEqual(
                set(credentials),
                {"configured", "status", "secret_values_exposed"},
            )
            self.assertIsInstance(credentials["configured"], bool)
            self.assertIn(credentials["status"], {"CONFIGURED", "NOT CONFIGURED"})
            self.assertFalse(credentials["secret_values_exposed"])
        finally:
            control.release_status.set()
            for endpoint_thread in endpoint_threads:
                endpoint_thread.join(timeout=2)
            operator_thread.join(timeout=2)
        self.assertFalse(
            any(thread.is_alive() for thread in endpoint_threads),
            "persisted endpoint request thread was not released",
        )
        self.assertFalse(operator_thread.is_alive(), "slow control status thread was not released")
        self.assertFalse(operator_errors)
        self.assertEqual(len(operator_result), 1)
        self.assertEqual(control.status_calls, 1)
    def test_canary_endpoint_serves_bounded_connectivity_and_exact_block_reason(self) -> None:
        ready = _connectivity_projection(ready=True, status="READY")
        self.store.set_operator_config(CANARY_CONNECTIVITY_CONFIG_KEY, ready)
        status, payload, body = self._request("api/v2/canary")
        self.assertEqual(status, 200)
        self.assertIsInstance(payload, dict)
        assert isinstance(payload, dict)
        self.assertEqual(payload["connectivity"], ready)
        self.assertEqual(
            set(payload["connectivity"]),
            {
                "ready",
                "status",
                "checked_at",
                "sdk",
                "credentials",
                "authentication",
                "account",
                "geoblock",
                "balance",
                "allowance",
                "market",
                "order_book",
                "failure_codes",
                "failure_reasons",
                "live_execution",
            },
        )
        for value in ("0.9.0", "CONFIGURED", "PASS", "EOA", "PH", "NCR", "12.34"):
            self.assertIn(value, body)

        blocked = _connectivity_projection(
            ready=False,
            status="BLOCKED",
            allowance_status="INSUFFICIENT",
        )
        blocked["failure_codes"] = ["CANARY_ALLOWANCE_INSUFFICIENT"]
        blocked["failure_reasons"] = [
            {
                "code": "CANARY_ALLOWANCE_INSUFFICIENT",
                "reason": CANARY_ALLOWANCE_INSUFFICIENT_REASON,
            }
        ]
        self.store.set_operator_config(CANARY_CONNECTIVITY_CONFIG_KEY, blocked)
        status, payload, body = self._request("api/v2/canary")
        self.assertEqual(status, 200)
        self.assertIsInstance(payload, dict)
        assert isinstance(payload, dict)
        self.assertEqual(payload["connectivity"], blocked)
        self.assertEqual(payload["connectivity"]["allowance"]["status"], "INSUFFICIENT")
        self.assertEqual(
            payload["connectivity"]["failure_codes"],
            ["CANARY_ALLOWANCE_INSUFFICIENT"],
        )
        self.assertEqual(
            payload["connectivity"]["failure_reasons"],
            [
                {
                    "code": "CANARY_ALLOWANCE_INSUFFICIENT",
                    "reason": CANARY_ALLOWANCE_INSUFFICIENT_REASON,
                }
            ],
        )
        self.assertIn("CANARY_ALLOWANCE_INSUFFICIENT", body)
        self.assertIn(CANARY_ALLOWANCE_INSUFFICIENT_REASON, body)

        tampered_with_extra = {**ready, "unexpected": "EXTRA_SENTINEL"}
        malformed_ready = {**ready, "ready": False}
        tampered_balance = {
            **ready,
            "balance": {
                **ready["balance"],
                "available_usd": "1e999999999",
            },
        }
        secret_bearing = {
            **ready,
            "sdk": {
                **ready["sdk"],
                "version": PERSISTED_CONNECTIVITY_SECRET_VALUES[0],
            },
            "private_key": FORBIDDEN_CONNECTIVITY_VALUES[0],
        }
        opaque_failure = {
            **blocked,
            "failure_codes": ["OPAQUE_FAILURE_CODE"],
            "failure_reasons": [
                {
                    "code": "OPAQUE_FAILURE_CODE",
                    "reason": "OPAQUE_FAILURE_REASON",
                }
            ],
        }
        for label, persisted in (
            ("extra", tampered_with_extra),
            ("malformed", malformed_ready),
            ("extreme exponent balance", tampered_balance),
            ("secret-bearing", secret_bearing),
            ("opaque failure code", opaque_failure),
        ):
            self.store.set_operator_config(CANARY_CONNECTIVITY_CONFIG_KEY, persisted)
            status, payload, body = self._request("api/v2/canary")
            self.assertEqual(status, 200, label)
            self.assertIsInstance(payload, dict, label)
            assert isinstance(payload, dict)
            self.assertIsNone(payload["connectivity"], label)
            serialized = json.dumps(payload, sort_keys=True)
            for forbidden in (
                *FORBIDDEN_CONNECTIVITY_VALUES,
                *PERSISTED_CONNECTIVITY_SECRET_VALUES,
                WALLET_LIKE_CONNECTIVITY_VALUE,
                "EXTRA_SENTINEL",
                "OPAQUE_FAILURE_CODE",
                "OPAQUE_FAILURE_REASON",
                "1e999999999",
            ):
                self.assertNotIn(forbidden, serialized, label)
                self.assertNotIn(forbidden, body, label)

    def test_v2_canary_restores_connectivity_after_file_backed_store_reopen(self) -> None:
        expected = _connectivity_projection(
            ready=True,
            status="READY",
            checked_at="2024-01-03T12:34:56+00:00",
        )
        self.store.set_operator_config(CANARY_CONNECTIVITY_CONFIG_KEY, expected)
        reopened = AxiomStore(self.store.path)
        self.addCleanup(reopened.close)
        self.server.data.store = reopened

        status, payload, _ = self._request("api/v2/canary")
        self.assertEqual(status, 200)
        self.assertIsInstance(payload, dict)
        assert isinstance(payload, dict)
        self.assertEqual(payload["connectivity"], expected)
        self.assertIs(payload["connectivity"]["live_execution"], False)


class DashboardPaginationSurfaceTests(DashboardPaginationFixture):

    def test_overview_and_list_responses_do_not_embed_unbounded_records(self) -> None:
        status, overview, overview_body = self._request("api/overview")
        self.assertEqual(status, 200)
        self.assertIsInstance(overview, dict)
        assert isinstance(overview, dict)
        self.assertNotIn("items", overview)
        self.assertNotIn("dataset-00", overview_body)
        self.assertLess(len(overview_body), 5000)
        status, datasets, datasets_body = self._request("api/v2/datasets", page=1, page_size=10)
        self.assertEqual(status, 200)
        self.assertIsInstance(datasets, dict)
        assert isinstance(datasets, dict)
        self.assertLessEqual(len(datasets["items"]), 10)
        self.assertLess(len(datasets_body), 50000)
        self.assertNotIn('"records"', datasets_body)

    def test_lightweight_overview_endpoint_is_bounded(self) -> None:
        status, overview, overview_body = self._request("api/v2/overview-summary")
        self.assertEqual(status, 200)
        self.assertIsInstance(overview, dict)
        assert isinstance(overview, dict)
        self.assertLessEqual(len(overview["latest_activity"]), 8)
        self.assertNotIn("current_failures", overview_body)
        self.assertIn("collector_health", overview)
        funnel = overview.get("lifecycle_funnel")
        self.assertIsInstance(funnel, dict)
        assert isinstance(funnel, dict)
        self.assertTrue(funnel)
        self.assertGreater(sum(funnel.values()), 0)
        self.assertEqual(funnel["FROZEN"], 1)

    def test_final_overview_renderer_consumes_lifecycle_funnel(self) -> None:
        html = _dashboard_html()
        start = html.rfind("renderOverview = (data) =>")
        end = html.index("renderDatasets =", start)
        final_renderer = html[start:end]
        for marker in (
            "data.lifecycle_funnel",
            "funnel-row",
            "funnel-track",
            "funnel-bar",
            'empty("No candidate lifecycle"',
            "Hermes hypotheses appear after a durable queue item is processed.",
        ):
            self.assertIn(marker, final_renderer)
        self.assertRegex(final_renderer, r'\$\(\s*["\']funnel["\']\s*\)\.innerHTML\s*=')
        self.assertRegex(final_renderer, r"Object\.entries\(\s*funnel\s*\)")
        self.assertRegex(final_renderer, r"safe\(\s*k\s*\)")
        self.assertRegex(final_renderer, r"count\(\s*v\s*\)")

    def test_independent_operator_controls_preserve_both_response_orders(self) -> None:
        html = _dashboard_html()
        status, overview, _ = self._request("api/v2/overview-summary")
        self.assertEqual(status, 200)
        self.assertIsInstance(overview, dict)
        assert isinstance(overview, dict)
        self.assertNotIn("operator_controls", overview)
        overview_start = html.rfind("const _renderOverviewScheduling = renderOverview;")
        overview_end = html.index("renderDatasets =", overview_start)
        final_renderer = html[overview_start:overview_end]
        render_call = "renderOperatorControls(data)"
        self.assertIn(render_call, final_renderer)
        call_prefix = final_renderer[:final_renderer.index(render_call)]
        self.assertRegex(
            call_prefix,
            r'if\s*\(\s*Object\.prototype\.hasOwnProperty\.call\(\s*data\s*,\s*["\']operator_controls["\']\s*\)\s*\|\|\s*!operatorControlsRendered\s*\)\s*$',
            "v2 overview data must not clear an already-rendered control response",
        )

        controls_start = html.index("function renderOperatorControls(data)")
        controls_end = html.index("function saveState", controls_start)
        controls_renderer = html[controls_start:controls_end]
        self.assertRegex(
            controls_renderer,
            r"if\s*\(\s*!operatorControlsRendered\s*\)\s*\$\(\s*['\"]operator-controls['\"]\s*\)\.innerHTML\s*=",
            "an overview-first empty control render may show unavailable state but must not clear known controls",
        )
        self.assertIn("operatorControlsRendered=true", controls_renderer)

        fetch_start = html.index('const controls=await fetchWithTimeout("/api/operator"')
        fetch_end = html.index("} catch(error)", fetch_start)
        control_success = html[fetch_start:fetch_end]
        self.assertRegex(
            control_success,
            r"renderOperatorControls\(\{\s*operator_controls\s*:\s*controls\.operator_controls\s*\|\|\s*controls\s*\}\)[\s\S]*"
            r"lastGood\.controls\s*=\s*controls;[\s\S]*operator\s*=\s*controls;",
            "the independent control-fetch success path must render its own response",
        )

    def test_html_has_paginated_views_url_state_and_responsive_sticky_layout(self) -> None:
        html = _dashboard_html()
        for marker in (
            'id="view-datasets"',
            'id="view-activity"',
            'data-view="datasets"',
            'data-view="activity"',
            "URLSearchParams",
            "page_size",
            "Showing ${start}",
            "windowStart",
            "numbers.map",
            "Previous",
            "Next",
            "select.facet",
            "dataset.param",
            "datasets-market",
            "datasets-timeframe",
            "datasets-quality",
            "AbortController",
            "polymarket-quality",
            "fetch(",
            "canary_eligible",
            "historical_gates",
            "Historical gates",
            "Micro-live canary",
            "Paper forward status",
            "Paper promotable",
        ):
            self.assertIn(marker, html)
        self.assertRegex(html, r"history\.(?:replaceState|pushState)")
        self.assertRegex(html, r"body\s*\{[^}]*max-width|main\s*\{[^}]*max-width")
        self.assertRegex(html, r"overflow-x\s*:\s*auto")
        self.assertRegex(html, r"header\s*\{[^}]*position\s*:\s*sticky[^}]*top\s*:\s*0")
        self.assertIn("box-sizing: border-box", html)
        # Data is fetched after load; it is not rendered as a giant inline JS
        # literal in the initial HTML document.
        self.assertNotIn("dataset-00", html)
        self.assertNotIn("market-00", html)
    def test_canary_renderer_labels_research_and_actionable_scan_separately(self) -> None:
        html = _dashboard_html()
        start = html.index("function renderCanary(data)")
        end = html.index("function renderBtc", start)
        renderer = html[start:end]
        for label in (
            "Research winner",
            "Research rank",
            "Current actionable candidate",
            "Candidates ranked",
            "Signal checked this tick",
            "next scan window ranks",
            "Actionable",
            "Chosen actionable rank",
            "Chosen score",
            "NO ACTIONABLE SIGNAL",
        ):
            self.assertIn(label, renderer)
        for field in ACTIONABLE_SCAN_FIELDS:
            self.assertIn(field, renderer)
        self.assertIn("winner_id", renderer)
        self.assertIn("selected_actionable_candidate", renderer)
        self.assertIn("selected_actionable_rank", renderer)
        self.assertIn("selected_actionable_score", renderer)


    def test_load_page_initializes_generation_controller_and_refresh_timer(self) -> None:
        html = _dashboard_html()
        start = html.index("loadPage = async function(tab,force=false)")
        end = html.index("activate = function(tab,push=true)", start)
        load_page = html[start:end]
        compact_load_page = re.sub(r"\s+", "", load_page)

        initialization = (
            "constgeneration=++refreshGeneration,controller=newAbortController();"
            "activeController=controller;loadInFlight=true;refreshMessage(tab,\"\");"
            "slowRefreshTimer=setTimeout(()=>{if(generation===refreshGeneration)"
        )
        self.assertIn(initialization, compact_load_page)
        self.assertEqual(
            compact_load_page.count("constgeneration=++refreshGeneration,controller=newAbortController();"),
            1,
        )
        self.assertEqual(load_page.count("slowRefreshTimer=setTimeout("), 1)
        self.assertLess(
            load_page.index("const generation="),
            load_page.index("fetchWithTimeout"),
            "per-load state must be initialized before any request starts",
        )
        self.assertLess(
            load_page.index("slowRefreshTimer=setTimeout("),
            load_page.index("clearTimeout(slowRefreshTimer)"),
            "the slow-refresh timer must be installed before the finally cleanup",
        )
        self.assertIn("slowRefreshTimer=null;", load_page)

        activation_start = end
        activation_end = html.index("load = async function()", activation_start)
        activation = html[activation_start:activation_end]
        self.assertIn("if(activeController)activeController.abort()", activation)
        self.assertIn("refreshGeneration++;", activation)

        stale_guard = load_page.index("if(generation!==refreshGeneration)return;")
        self.assertLess(
            stale_guard,
            load_page.index("render(data)"),
            "a stale response must not render over a newer generation",
        )
        self.assertIn("render(lastGood[kind]);", load_page)
        self.assertIn("showing last successful content", load_page)
        self.assertIn("no cached dashboard snapshot available", load_page)


    def test_real_canary_actions_post_once_to_local_result_and_survive_refresh(self) -> None:
        html = _dashboard_html()
        start = html.index("function renderCanary(data)")
        end = html.index("function renderBtc", start)
        real_canary = html[start:end]
        canary_actions = (
            "canary.connectivity_check",
            "canary.enable_auto",
            "canary.disarm",
            "canary.kill",
        )
        for action in canary_actions:
            self.assertIn(
                f'controlButton("{action}"',
                real_canary,
                f"{action} must be rendered on REAL CANARY",
            )
        self.assertRegex(
            html,
            r"function isCanaryAction\(action\)[\s\S]{0,160}startsWith\([\"']canary\.[\"']\)",
        )
        self.assertRegex(
            html,
            r"function actionResultNode\(action\)[\s\S]{0,220}"
            r"[\"']canary-action-result[\"'][\s\S]{0,80}[\"']control-result[\"']",
            "all canary actions must route to the REAL CANARY result, while ordinary actions retain Overview routing",
        )
        self.assertNotIn("control-result", real_canary)

        control_post_start = html.index("async function controlPost")
        control_post_end = html.index("function renderOperatorControls", control_post_start)
        control_post = html[control_post_start:control_post_end]
        for forbidden in FORBIDDEN_CONNECTIVITY_VALUES:
            self.assertNotIn(forbidden, real_canary)
            self.assertNotIn(forbidden, control_post)
        self.assertEqual(control_post.count('fetch("/api/control"'), 1)
        self.assertIn("actionResultMessage(action", control_post)
        self.assertRegex(
            control_post,
            r"result\?\.result\?\.connectivity",
            "canary control responses must consume the fresh nested connectivity projection",
        )
        canary_update_start = control_post.index("if(isCanaryAction(action)&&connectivity)")
        canary_update_end = control_post.index("actionResultMessage", canary_update_start)
        canary_update = control_post[canary_update_start:canary_update_end]
        merge = re.search(
            r"if\s*\(\s*lastGood\.canary\s*&&\s*typeof\s+lastGood\.canary\s*===\s*['\"]object['\"]"
            r"\s*&&\s*!Array\.isArray\(\s*lastGood\.canary\s*\)\s*\)\s*"
            r"(?:\{\s*)?lastGood\.canary\s*=\s*\{\s*\.\.\.\s*lastGood\.canary\s*,\s*connectivity\s*\}",
            canary_update,
        )
        self.assertIsNotNone(
            merge,
            "fresh canary connectivity must immutably replace the persisted projection only when one exists",
        )
        assert merge is not None
        render = canary_update.index("renderCanaryConnectivity(connectivity)")
        load = control_post.index("await loadPage(state.tab,true)", canary_update_start)
        self.assertLess(
            merge.start(),
            render,
            "lastGood.canary must be updated before the immediate connectivity render",
        )
        self.assertLess(
            canary_update_start + render,
            load,
            "fresh canary connectivity must be retained before the follow-up load/fallback can run",
        )
        self.assertNotRegex(
            canary_update,
            r"lastGood\.canary\s*=\s*\{\s*connectivity\s*\}",
            "a missing lastGood.canary must not be seeded with a partial projection",
        )
        self.assertRegex(
            canary_update,
            r"lastGood\.canary\s*&&\s*typeof\s+lastGood\.canary\s*===\s*['\"]object['\"]",
            "the POST merge must be guarded against an absent last-good canary",
        )
        message = control_post.index("actionResultMessage(action")
        render_position = canary_update_start + render
        self.assertLess(
            render_position,
            message,
            "fresh canary connectivity must render before action feedback is written",
        )
        forced_load = control_post.index("await loadPage(state.tab,true)", message)
        self.assertLess(
            message,
            forced_load,
            "the action result must remain visible before the forced refresh starts",
        )
        post_feedback = control_post[message:forced_load]
        abort = re.search(
            r"if\s*\(\s*activeController\s*\)\s*activeController\.abort\(\)",
            post_feedback,
        )
        self.assertIsNotNone(
            abort,
            "an action must abort any pre-action refresh before forcing a new load",
        )
        generation = re.search(
            r"refreshGeneration\s*(?:\+\+|\+=\s*1)",
            post_feedback,
        )
        self.assertIsNotNone(
            generation,
            "an action must invalidate the pre-action refresh generation",
        )
        for bookkeeping in (
            r"activeController\s*=\s*null",
            r"loadInFlight\s*=\s*false",
            r"clearTimeout\(\s*slowRefreshTimer\s*\)",
            r"slowRefreshTimer\s*=\s*null",
            r"nextRefreshAt\s*=\s*0",
        ):
            self.assertRegex(
                post_feedback,
                bookkeeping,
                "action refresh bookkeeping must reset before the forced active-tab load",
            )

        click_start = html.index('document.addEventListener("click",async event=>')
        click_end = html.index("ensureActivityKind()", click_start)
        click_handler = html[click_start:click_end]
        self.assertEqual(click_handler.count("controlPost("), 1)
        self.assertEqual(click_handler.count('fetch("/api/control"'), 0)

        result_markup = re.search(r'<(?:div|p)[^>]*id=["\']canary-action-result["\']', html)
        self.assertIsNotNone(result_markup)
        assert result_markup is not None
        self.assertLess(result_markup.start(), start)
        self.assertNotRegex(
            real_canary,
            r'id=["\']canary-action-result["\']',
            "renderCanary must not recreate the result node during ordinary refresh",
        )
        self.assertNotRegex(
            real_canary,
            r'\$\(\s*["\']canary-action-result["\']\s*\)\.(?:innerHTML|textContent)\s*=',
            "renderCanary must not clear an action result",
        )

    def test_canary_endpoint_reports_connectivity_state_and_blocker(self) -> None:
        blocked = _connectivity_projection(ready=False, status="BLOCKED")
        blocked["failure_codes"] = ["CONNECTIVITY_CHECK_FAILED"]
        blocked["failure_reasons"] = [
            {
                "code": "CONNECTIVITY_CHECK_FAILED",
                "reason": "Connectivity check failed.",
            }
        ]
        self.store.set_operator_config(CANARY_CONNECTIVITY_CONFIG_KEY, blocked)
        status, payload, _ = self._request("api/v2/canary")
        self.assertEqual(status, 200)
        self.assertIsInstance(payload, dict)
        assert isinstance(payload, dict)
        self.assertFalse(payload["connectivity"]["ready"])
        self.assertEqual(
            payload["autonomous_canary"]["blocker"],
            "AUTONOMOUS_CONTROL_UNKNOWN",
        )
        self.assertEqual(payload["canary"]["control_state"], "UNKNOWN")

        ready = _connectivity_projection(ready=True, status="READY")
        self.store.set_operator_config(CANARY_CONNECTIVITY_CONFIG_KEY, ready)
        status, payload, _ = self._request("api/v2/canary")
        self.assertEqual(status, 200)
        self.assertIsInstance(payload, dict)
        assert isinstance(payload, dict)
        self.assertTrue(payload["connectivity"]["ready"])
        self.assertEqual(
            payload["autonomous_canary"]["blocker"],
            "AUTONOMOUS_CONTROL_UNKNOWN",
        )
        self.assertEqual(payload["canary"]["control_state"], "UNKNOWN")


    def test_dashboard_formats_utc_as_pht_without_mutating_api_timestamps(self) -> None:
        html = _dashboard_html()
        self.assertIn("Asia/Manila", html)
        self.assertIn("PHT", html)
        self.assertIn("hourCycle:\"h23\"", html)
        self.assertNotIn("new Date(v).toISOString()", html)

        midnight_utc = datetime(2024, 1, 1, 16, tzinfo=UTC)
        rollover = midnight_utc.astimezone(timezone(timedelta(hours=8)))
        self.assertEqual(
            rollover.strftime("%Y-%m-%d %H:%M:%S PHT"),
            "2024-01-02 00:00:00 PHT",
        )
        status, payload, _ = self._request(
            "api/v2/datasets",
            page=1,
            page_size=10,
            sort="dataset_id",
            direction="asc",
        )
        self.assertEqual(status, 200)
        assert isinstance(payload, dict)
        self.assertEqual(payload["items"][0]["updated_at"], T0.isoformat())

if __name__ == "__main__":
    unittest.main()
