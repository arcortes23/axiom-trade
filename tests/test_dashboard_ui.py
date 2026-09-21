from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pytest
from axiom.dashboard import DashboardData, DashboardServer
from axiom.storage import AxiomStore
from ui_fixture_server import FixtureServer, fixture_payload


def _get(url: str, *, headers: dict[str, str] | None = None) -> tuple[int, str, bytes]:
    request = Request(url, headers=headers or {})
    try:
        with urlopen(request, timeout=3) as response:
            return response.status, response.headers.get_content_type(), response.read()
    except HTTPError as error:
        return error.code, error.headers.get_content_type(), error.read()


def _post(url: str) -> tuple[int, str, bytes]:
    request = Request(url, data=b"{}", method="POST", headers={"Content-Type": "application/json"})
    try:
        with urlopen(request, timeout=3) as response:
            return response.status, response.headers.get_content_type(), response.read()
    except HTTPError as error:
        return error.code, error.headers.get_content_type(), error.read()


def _admin_post(url: str, payload: dict, *, headers: dict[str, str] | None = None) -> tuple[int, dict]:
    request_headers = {"Content-Type": "application/json"}
    request_headers.update(headers or {})
    request = Request(url, data=json.dumps(payload).encode("utf-8"), headers=request_headers, method="POST")
    try:
        with urlopen(request, timeout=3) as response:
            return response.status, json.loads(response.read().decode("utf-8"))
    except HTTPError as error:
        return error.code, json.loads(error.read().decode("utf-8"))
def _json(url: str) -> tuple[int, dict]:
    status, _content_type, body = _get(url, headers={"Accept": "application/json"})
    return status, json.loads(body.decode("utf-8"))


def test_fixture_payload_is_secret_free_and_uses_native_authority_envelopes() -> None:
    payload = fixture_payload("active_no_signal")
    encoded = json.dumps(payload, sort_keys=True).lower()
    assert "private_key" not in encoded
    assert "csrf" not in encoded
    assert payload["canary"]["production_live_trading"] is False
    assert payload["canary"]["execution_authorization"]["status"] == "ACTIVE"
    assert payload["canary"]["execution_authorization"]["mode"] == "EXPLORATORY_MICRO_CANARY"
    assert isinstance(payload["canary"]["risk_settings"]["remaining"], dict)


def test_fixture_uses_dashboard_server_routes_and_explicit_fixture_label() -> None:
    with FixtureServer("active_no_signal") as fixture:
        assert fixture.url is not None
        status, content_type, html = _get(fixture.url + "/")
        assert status == 200
        text = html.decode("utf-8")
        assert 'data-fixture-banner="true"' in text
        assert "FIXTURE ONLY" in text
        assert "PRIVATE_KEY" not in text

        status, operator = _json(fixture.url + "/api/operator")
        assert status == 200
        assert operator["fixture"] is True
        assert operator["fixture_scenario"] == "active_no_signal"
        assert operator["production_live_trading"] is False

        status, canary = _json(fixture.url + "/api/v2/canary?page=1&page_size=10")
        assert status == 200
        status, ui_state = _json(f"{fixture.url}/api/ui-state")
        assert status == 200
        assert ui_state["execution_authorization"]["status"] == "ACTIVE"
        status, ui_status = _json(f"{fixture.url}/api/ui-status")
        assert status == 200
        assert ui_status["schema"] == "ui-status.v1"
        assert ui_status["live_execution"] is False
        assert ui_status["status_scope"]["kind"] == "persisted_worker_state"
        assert ui_status["status_scope"]["details_complete"] is True
        assert ui_status["provenance"]["full_status_endpoint"] == "/api/status"
        assert ui_status["status_scope"]["scan_truncated"] is False
        assert ui_status["workers_returned"] == len(ui_status["workers"])
        assert ui_status["workers_returned"] <= 64
        assert ui_status["workers_considered"] == ui_status["workers_total"]
        assert "summary" not in ui_status
        assert "cycles" not in ui_status
        assert "queue" not in ui_status
        assert "normalized_workers" not in ui_status
        assert fixture.control.calls == []
        assert canary["execution_authorization"]["status"] == "ACTIVE"


def test_fixture_can_serve_token_free_legacy_baseline_for_before_captures() -> None:
    with TemporaryDirectory() as directory:
        path = Path(directory) / "legacy.html"
        path.write_text("<!doctype html><title>BEFORE FIXTURE</title><meta name=\"axiom-control-token\" content=\"\">", encoding="utf-8")
        with FixtureServer("prepared", legacy_html=path) as fixture:
            assert fixture.legacy_url is not None
            status, content_type, body = _get(fixture.legacy_url + "/")
            assert status == 200
            assert content_type == "text/html"
            assert b"BEFORE FIXTURE" in body
            assert b'data-fixture-banner="true"' in body
            assert b"FIXTURE ONLY" in body
            assert b"PRIVATE_KEY" not in body
            status, legacy_operator = _json(fixture.legacy_url + "/api/operator")
            assert status == 200
            assert legacy_operator["fixture"] is True
            status, legacy_canary = _json(fixture.legacy_url + "/api/v2/canary?page=1&page_size=10")
            assert status == 200
            assert legacy_canary["execution_authorization"]["status"] == legacy_operator["execution_authorization"]["status"]
            assert legacy_canary["execution_authorization"]["mode"] == legacy_operator["execution_authorization"]["mode"]
            assert legacy_canary["execution_authorization"]["active"] is None
            assert legacy_canary["operator_controls"]["armed"] is False
            assert legacy_operator["production_live_trading"] is False
            status, legacy_candidate = _json(fixture.legacy_url + "/api/v2/candidates/fixture-candidate-aurora")
            assert status == 200
            assert legacy_candidate["candidate_id"] == "fixture-candidate-aurora"
            status, _content_type, _body = _post(fixture.legacy_url + "/api/control")
            assert status == 405

def test_fixture_routes_keep_bounded_pagination_and_exact_detail_surfaces() -> None:
    with FixtureServer("prepared") as fixture:
        assert fixture.url is not None
        for endpoint in ("polymarket", "candidates", "datasets", "hermes", "crypto-research", "activity"):
            status, payload = _json(f"{fixture.url}/api/v2/{endpoint}?page=1&page_size=10")
            assert status == 200
            assert payload["page"] == 1
            assert payload["page_size"] == 10
            assert len(payload["items"]) <= 10

        status, candidate = _json(f"{fixture.url}/api/v2/candidates/fixture-candidate-aurora")
        assert status == 200
        assert candidate["candidate_id"] == "fixture-candidate-aurora"
        status, events = _json(f"{fixture.url}/api/v2/candidates/fixture-candidate-aurora/events?page=1&page_size=10")
        assert status == 200
        assert events["items"][0]["event_id"] == "fixture-event-aurora-1"

        status, dataset = _json(f"{fixture.url}/api/v2/datasets/fixture-dataset-aurora")
        assert status == 200
        assert dataset["dataset_id"] == "fixture-dataset-aurora"
        status, gaps = _json(f"{fixture.url}/api/v2/datasets/fixture-dataset-aurora/missing-ranges?page=1&page_size=10")
        assert status == 200
        assert gaps["items"][0]["range_index"] == 0
        gap_start = datetime.fromisoformat(gaps["items"][0]["range"]["start"])
        gap_end = datetime.fromisoformat(gaps["items"][0]["range"]["end"])
        observed_at = datetime.fromisoformat(dataset["updated_at"])
        assert gap_start.tzinfo is not None and gap_end.tzinfo is not None
        assert gap_start < gap_end <= observed_at

        status, market_record = _json(f"{fixture.url}/api/ui-record?kind=market&id=fixture-market-aurora")
        assert status == 200
        assert market_record["kind"] == "market"
        assert market_record["id"] == "fixture-market-aurora"
        assert market_record["record"]["market_id"] == "fixture-market-aurora"
        status, order_record = _json(f"{fixture.url}/api/ui-record?kind=order&id=fixture-position-request-aurora")
        assert status == 200
        assert order_record["kind"] == "order"
        assert order_record["id"] == "fixture-position-request-aurora"
        assert order_record["record"]["request_id"] == "fixture-position-request-aurora"
        assert order_record["record"].get("position_id") != order_record["id"]
        status, missing_record = _json(f"{fixture.url}/api/ui-record?kind=market&id=does-not-exist")
        assert status == 404
        assert missing_record["kind"] == "market"


def test_dataset_reads_stay_available_when_aggregate_health_is_unreadable() -> None:
    with AxiomStore(":memory:") as store:
        store.save_dataset_catalog(
            "dataset-aurora",
            "stored-v1",
            provider="fixture-provider",
            instrument="fixture-aurora",
            market_type="prediction",
            timeframe="1h",
            row_count=1000,
            completeness=0.8,
            missing_ranges=[{"start": "v1-start", "end": "v1-end"}],
            quality="HISTORICAL",
            source_type="HISTORICAL",
            snapshot_id="snapshot-v1",
            metadata={"category": "fixture"},
        )
        store.save_dataset_catalog(
            "dataset-aurora",
            "stored-v2",
            provider="fixture-provider",
            instrument="fixture-aurora",
            market_type="prediction",
            timeframe="1h",
            row_count=1842,
            completeness=0.9,
            missing_ranges=[{"start": "v2-start", "end": "v2-end"}],
            quality="PRICE_PROXY",
            source_type="FORWARD_COLLECTED",
            snapshot_id="snapshot-v2",
            metadata={"category": "fixture"},
        )

        def deny_global_health_tables(
            action: int,
            table: str | None,
            _column: str | None,
            _database: str | None,
            _source: str | None,
        ) -> int:
            if action == sqlite3.SQLITE_READ and table in {"bars", "snapshots", "datasets"}:
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        dashboard = DashboardData(store=store)
        store.connection.set_authorizer(deny_global_health_tables)
        try:
            detail = dashboard.v2_snapshot("datasets/dataset-aurora")
            assert detail["available"] is True
            assert detail["dataset_id"] == "dataset-aurora"
            assert detail["dataset_version"] == "stored-v2"
            assert detail["catalog"]["dataset_version"] == "stored-v2"
            assert detail["catalog"]["row_count"] == 1842
            assert detail["catalog"]["quality"] == "PRICE_PROXY"
            assert detail["catalog"]["source_type"] == "FORWARD_COLLECTED"
            assert detail["catalog"]["missing_range_count"] == 1
            assert detail["health"] is None

            gaps = dashboard.v2_snapshot(
                "datasets/dataset-aurora/missing-ranges",
                {"page": "1", "page_size": "10"},
            )
            assert gaps["total"] == 2
            assert [item["dataset_version"] for item in gaps["items"]] == ["stored-v1", "stored-v2"]
            assert gaps["range_payload_truncated"] is False

            unknown = dashboard.v2_snapshot(
                "datasets/does-not-exist/missing-ranges",
                {"page": "1", "page_size": "10"},
            )
            assert unknown["items"] == []
            assert unknown["total"] == 0
        finally:
            store.connection.set_authorizer(None)


def test_system_reads_bounded_storage_and_persisted_health_when_history_is_denied() -> None:
    with AxiomStore(":memory:") as store:
        heartbeat = datetime(2024, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
        store.save_worker_state(
            "health-monitor",
            "RUNNING",
            {
                "grade": "B",
                "grade_scope": "collector_health",
                "reason_code": "CURRENT_COLLECTION_FAILURES",
                "reasons": [{"code": "CURRENT_COLLECTION_FAILURES", "reason": "fixture"}],
                "source_type": "FORWARD_COLLECTED",
            },
            heartbeat_at=heartbeat,
        )
        persisted = store.list_worker_states_dashboard(worker_name="health-monitor", limit=1)[0]
        dashboard = DashboardData(store=store)

        def deny_history_and_writes(
            action: int,
            table: str | None,
            _column: str | None,
            _database: str | None,
            _source: str | None,
        ) -> int:
            denied_tables = {
                "bars",
                "snapshots",
                "datasets",
                "polymarket_snapshots",
                "polymarket_trades",
                "collection_errors",
            }
            denied_writes = {
                sqlite3.SQLITE_INSERT,
                sqlite3.SQLITE_UPDATE,
                sqlite3.SQLITE_DELETE,
            }
            if action == sqlite3.SQLITE_READ and table in denied_tables:
                return sqlite3.SQLITE_DENY
            if action in denied_writes:
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        store.connection.set_authorizer(deny_history_and_writes)
        try:
            system = dashboard.system()
        finally:
            store.connection.set_authorizer(None)

    assert system["storage"]["status"] == "READY"
    assert system["storage"]["available"] is True
    assert system["storage"]["scope"] == "database"
    assert system["storage"]["database_bytes"] > 0
    assert system["dataset_health"]["available"] is True
    assert system["dataset_health"]["grade"] == "B"
    assert system["dataset_health"]["status"] == "RUNNING"
    assert system["dataset_health"]["heartbeat_at"] == persisted["heartbeat_at"]
    assert system["dataset_health"]["updated_at"] == persisted["updated_at"]
    assert system["dataset_health"]["provenance"] == "persisted worker_state health-monitor row"


def test_ui_status_http_keeps_worker_health_when_history_reads_and_writes_are_denied() -> None:
    with TemporaryDirectory() as directory:
        store = AxiomStore(str(Path(directory) / "ui-status.sqlite"))
        now = datetime(2024, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
        store.save_worker_state(
            "health-monitor",
            "RUNNING",
            {
                "grade": "B",
                "grade_scope": "collector_health",
                "reason_code": "CURRENT_COLLECTION_FAILURES",
                "reasons": [{"code": "CURRENT_COLLECTION_FAILURES", "reason": "fixture"}],
                "source_type": "FORWARD_COLLECTED",
            },
            heartbeat_at=now,
        )
        store.save_worker_state("polymarket-collector", "RUNNING", {"stale_after_seconds": 300}, heartbeat_at=now)
        server = DashboardServer(
            port=0,
            data=DashboardData(store=store, clock=lambda: now),
        ).start()
        try:
            def deny_history_and_writes(
                action: int,
                table: str | None,
                _column: str | None,
                _database: str | None,
                _source: str | None,
            ) -> int:
                denied_tables = {
                    "bars",
                    "snapshots",
                    "datasets",
                    "dataset_catalog",
                    "polymarket_markets",
                    "polymarket_snapshots",
                    "polymarket_trades",
                    "collection_errors",
                    "collection_cycles",
                    "research_queue",
                    "research_queue_events",
                }
                if action == sqlite3.SQLITE_READ and table in denied_tables:
                    return sqlite3.SQLITE_DENY
                if action in {sqlite3.SQLITE_INSERT, sqlite3.SQLITE_UPDATE, sqlite3.SQLITE_DELETE}:
                    return sqlite3.SQLITE_DENY
                return sqlite3.SQLITE_OK

            store.connection.set_authorizer(deny_history_and_writes)
            assert server.url is not None
            status, payload = _json(server.url + "/api/ui-status")
        finally:
            store.connection.set_authorizer(None)
            server.stop()
            store.close()

    assert status == 200
    assert payload["schema"] == "ui-status.v1"
    assert payload["status"] == "degraded"
    assert payload["health_grade"] == "B"
    assert payload["degrading_worker"] == "health-monitor"
    assert payload["workers_total"] == 2
    assert payload["workers_returned"] == 2


def test_ui_status_http_keeps_native_stale_then_degraded_precedence() -> None:
    with TemporaryDirectory() as directory:
        store = AxiomStore(str(Path(directory) / "ui-status-precedence.sqlite"))
        now = datetime(2024, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
        store.save_worker_state(
            "health-monitor",
            "RUNNING",
            {
                "grade": "C",
                "reason_code": "CURRENT_COLLECTION_FAILURES",
                "reasons": [{"code": "CURRENT_COLLECTION_FAILURES", "reason": "fixture"}],
            },
            heartbeat_at=now,
        )
        store.save_worker_state(
            "polymarket-collector",
            "RUNNING",
            {"stale_after_seconds": 60},
            heartbeat_at=now.replace(minute=2),
        )
        server = DashboardServer(port=0, data=DashboardData(store=store, clock=lambda: now)).start()
        try:
            assert server.url is not None
            status, stale = _json(server.url + "/api/ui-status")
            assert status == 200
            assert stale["status"] == "stale"
            assert any(row["status"] == "stale" for row in stale["workers"])

            store.save_worker_state(
                "polymarket-collector",
                "RUNNING",
                {"stale_after_seconds": 60},
                heartbeat_at=now,
            )
            status, degraded = _json(server.url + "/api/ui-status")
            assert status == 200
            assert degraded["status"] == "degraded"
            assert degraded["health_grade"] == "C"

            store.save_worker_state(
                "health-monitor",
                "RUNNING",
                {"grade": "A", "reasons": []},
                heartbeat_at=now,
            )
            status, running = _json(server.url + "/api/ui-status")
            assert status == 200
            assert running["status"] == "running"
        finally:
            server.stop()
            store.close()


def test_ui_status_http_reports_truthful_worker_scope_when_degraded_row_is_omitted() -> None:
    with TemporaryDirectory() as directory:
        store = AxiomStore(str(Path(directory) / "ui-status-scope.sqlite"))
        now = datetime(2024, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
        store.save_worker_state(
            "worker-zzz-degraded",
            "DEGRADED",
            {"reason_code": "OUTSIDE_DETAIL", "degrading_reason": "fixture degraded worker"},
            heartbeat_at=now,
        )
        for index in range(64):
            store.save_worker_state(
                f"worker-{index:03d}",
                "RUNNING",
                {"stale_after_seconds": 300},
                heartbeat_at=now,
            )
        server = DashboardServer(port=0, data=DashboardData(store=store, clock=lambda: now)).start()
        try:
            assert server.url is not None
            status, payload = _json(server.url + "/api/ui-status")
        finally:
            server.stop()
            store.close()

    assert status == 200
    assert payload["status"] == "degraded"
    assert payload["workers_total"] == 65
    assert payload["workers_considered"] == 65
    assert payload["workers_returned"] == 64
    assert payload["workers_truncated"] is True
    assert payload["status_scope"]["details_complete"] is False
    assert payload["status_scope"]["scan_limit"] == 128
    assert payload["status_scope"]["scan_truncated"] is False
    assert payload["status_scope"]["details_limit"] == 64
    assert payload["degrading_worker"] == "worker-zzz-degraded"
    assert "worker-zzz-degraded" not in {row["worker_name"] for row in payload["workers"]}


def test_fixture_catalogs_prove_default_page_two_facets_sort_and_deep_identity() -> None:
    with FixtureServer("prepared") as fixture:
        assert fixture.url is not None
        for endpoint, identity in (
            ("polymarket", "market_id"),
            ("candidates", "candidate_id"),
            ("datasets", "dataset_id"),
            ("hermes", "job_id"),
            ("crypto-research", "symbol"),
            ("activity", "id"),
        ):
            status, first = _json(f"{fixture.url}/api/v2/{endpoint}")
            assert status == 200
            assert first["total"] == 31
            assert first["pages"] == 2
            assert len(first["items"]) == 25
            status, second = _json(f"{fixture.url}/api/v2/{endpoint}?page=2")
            assert status == 200
            assert second["page"] == 2
            assert len(second["items"]) == 6
            assert second["items"][0][identity] != first["items"][0][identity]
            assert len({row[identity] for row in first["items"] + second["items"]}) == 31

        status, filtered = _json(f"{fixture.url}/api/v2/polymarket?category=Sports")
        assert status == 200
        assert filtered["total"] > 0
        assert all(row["category"] == "Sports" for row in filtered["items"])
        status, empty = _json(f"{fixture.url}/api/v2/polymarket?category=NoSuchFixtureCategory")
        assert status == 200
        assert empty["total"] == 0
        assert empty["items"] == []

        status, descending = _json(f"{fixture.url}/api/v2/datasets?sort=dataset_id&direction=desc&page_size=10")
        assert status == 200
        status, ascending = _json(f"{fixture.url}/api/v2/datasets?sort=dataset_id&direction=asc&page_size=10")
        assert status == 200
        assert descending["items"][0]["dataset_id"] != ascending["items"][0]["dataset_id"]

        status, candidate = _json(f"{fixture.url}/api/v2/candidates/fixture-candidate-31")
        assert status == 200
        assert candidate["candidate_id"] == "fixture-candidate-31"


def test_dashboard_host_and_csrf_checks_remain_in_force_for_fixture_controls() -> None:
    with FixtureServer("network_failure") as fixture:
        assert fixture.url is not None
        status, body = _json(fixture.url + "/api/v2/overview-summary")
        assert status == 200
        assert body["error"] == "FIXTURE_NETWORK_FAILURE"

        request = Request(
            fixture.url + "/api/control",
            data=json.dumps({"action": "fixture.read_only_check"}).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with pytest.raises(HTTPError) as error:
            urlopen(request, timeout=3)
        assert error.value.code == 403

        fixture.set_scenario("prepared")
        token = fixture.control_token
        assert token
        request = Request(
            fixture.url + "/api/control",
            data=json.dumps({"action": "fixture.read_only_check"}).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "X-Axiom-Control-Token": token,
                "Origin": fixture.url,
            },
            method="POST",
        )
        with urlopen(request, timeout=3) as response:
            result = json.loads(response.read().decode("utf-8"))
        assert result["fixture"] is True
        assert fixture.control.calls[0]["action"] == "fixture.read_only_check"

        bad_host = Request(fixture.url + "/api/operator", headers={"Host": "evil.invalid"})
        with pytest.raises(HTTPError) as error:
            urlopen(bad_host, timeout=3)
        assert error.value.code == 403

        hostile_body = json.dumps({"action": "fixture.read_only_hostile_origin"}).encode("utf-8")
        for target in (fixture.url, fixture.upstream_url):
            assert target is not None
            for origin in ("https://evil.invalid", "null"):
                hostile = Request(
                    target + "/api/control",
                    data=hostile_body,
                    headers={
                        "Content-Type": "application/json",
                        "X-Axiom-Control-Token": token,
                        "Origin": origin,
                    },
                    method="POST",
                )
                with pytest.raises(HTTPError) as error:
                    urlopen(hostile, timeout=3)
                assert error.value.code == 403
def test_fixture_admin_exercises_exact_once_review_uncertain_resolution_and_ancillary_failure() -> None:
    with FixtureServer("prepared") as fixture:
        assert fixture.url is not None and fixture.admin_url is not None
        status, _ = _admin_post(fixture.admin_url + "/admin/behavior", {"delay_ms": 1, "fail_ancillary": True})
        assert status == 200
        status, body = _json(fixture.url + "/api/v2/candidates/fixture-candidate-aurora/events?page=1&page_size=10")
        assert status == 503
        assert body["error"] == "data unavailable"
        status, _ = _admin_post(fixture.admin_url + "/admin/behavior", {"fail_ancillary": False})
        assert status == 200
        token = fixture.control_token
        assert token
        request = Request(
            fixture.url + "/api/control",
            data=json.dumps({"action": "exploratory.live.review_confirm", "confirm": "CONFIRM EXPLORATORY LIVE", "payload": {}}).encode("utf-8"),
            headers={"Content-Type": "application/json", "X-Axiom-Control-Token": token, "Origin": fixture.url},
            method="POST",
        )
        with urlopen(request, timeout=3) as response:
            result = json.loads(response.read().decode("utf-8"))
        assert result["ok"] is True
        assert result["status"] == "CONFIRMED"
        action_id = result["action_id"]
        assert action_id.startswith("fixture-action-")
        status, ui_state = _json(fixture.url + "/api/ui-state")
        assert status == 200
        assert ui_state["execution_authorization"]["status"] == "ACTIVE"
        assert any(row.get("action_id") == action_id for row in ui_state["actions"])
        status, stats = _json(fixture.admin_url + "/admin/action-stats")
        assert status == 200
        assert stats["action_stats"]["completed"] == 1
        assert stats["action_stats"]["pending"] == 0
        status, _ = _admin_post(fixture.admin_url + "/admin/behavior", {"uncertain_next": True})
        assert status == 200
        request = Request(
            fixture.url + "/api/control",
            data=json.dumps({"action": "canary.enable_auto", "payload": {"action_id": "fixture-uncertain-1"}}).encode("utf-8"),
            headers={"Content-Type": "application/json", "X-Axiom-Control-Token": token, "Origin": fixture.url},
            method="POST",
        )
        with pytest.raises(HTTPError) as error:
            urlopen(request, timeout=3)
        assert error.value.code == 400
        status, _ = _admin_post(fixture.admin_url + "/admin/behavior", {"resolve_uncertain": True})
        assert status == 200


def test_fixture_canary_disarm_stops_new_work_without_rewriting_authorization_or_positions() -> None:
    with FixtureServer("active_no_signal") as fixture:
        assert fixture.url is not None
        token = fixture.control_token
        assert token
        request = Request(
            fixture.url + "/api/control",
            data=json.dumps({"action": "canary.disarm", "confirm": "DISARM", "payload": {}}).encode("utf-8"),
            headers={"Content-Type": "application/json", "X-Axiom-Control-Token": token, "Origin": fixture.url},
            method="POST",
        )
        with urlopen(request, timeout=3) as response:
            result = json.loads(response.read().decode("utf-8"))
        assert result["ok"] is True
        assert result["status"] == "COMPLETE"
        assert result["result"]["canary"]["orders_closed"] is False
        assert result["result"]["canary"]["orders_cancelled"] is False
        status, canary = _json(fixture.url + "/api/v2/canary")
        assert status == 200
        assert canary["control_state"] == "DISARMED"
        assert canary["display_state"] == "DISABLED"
        assert canary["micro_live_canary"] == "DISARMED"
        assert canary["operator_controls"]["armed"] is False
        assert canary["autonomous"]["enabled"] is False
        assert canary["execution_authorization"]["status"] == "ACTIVE"
        assert canary["execution"]["last_request_status"] == "STOPPED"
        assert canary["production_live_trading"] is False

def test_fixture_authorization_revoke_updates_permission_without_claiming_canary_disarm() -> None:
    with FixtureServer("active_no_signal") as fixture:
        assert fixture.url is not None
        token = fixture.control_token
        assert token
        request = Request(
            fixture.url + "/api/control",
            data=json.dumps({
                "action": "execution_authorization.revoke",
                "confirm": "REVOKE EXPLORATORY AUTHORIZATION",
                "payload": {
                    "action_id": "fixture-revoke-1",
                    "authorization_id": "fixture-execution-authorization",
                    "expected_generation": 3,
                    "reason": "fixture operator test",
                },
            }).encode("utf-8"),
            headers={"Content-Type": "application/json", "X-Axiom-Control-Token": token, "Origin": fixture.url},
            method="POST",
        )
        with urlopen(request, timeout=3) as response:
            result = json.loads(response.read().decode("utf-8"))
        assert result["ok"] is True
        assert result["status"] == "COMPLETE"
        assert result["result"]["execution_authorization"]["status"] == "REVOKED"
        status, canary = _json(fixture.url + "/api/v2/canary")
        assert status == 200
        assert canary["execution_authorization"]["status"] == "REVOKED"
        assert canary["execution_authorization"]["active"] is None
        assert canary["operator_controls"]["armed"] is True
        assert canary["production_live_trading"] is False


def test_scenarios_distinguish_unknown_missing_and_partial_without_recomputation() -> None:
    with FixtureServer("missing") as fixture:
        assert fixture.url is not None
        status, payload = _json(fixture.url + "/api/v2/canary")
        assert status == 200
        assert payload["risk_settings"]["remaining"] is None
        assert payload["readiness"]["status"] == "UNKNOWN"

        fixture.set_scenario("partial_fill")
        status, payload = _json(fixture.url + "/api/v2/canary")
        assert status == 200
        assert payload["execution"]["orders"][0]["status"] == "PARTIAL"
        assert payload["execution"]["fills"][0]["filled_quantity"] == "0.25"

        fixture.set_scenario("unknown_order")
        status, payload = _json(fixture.url + "/api/v2/canary")
        assert status == 200
        assert payload["execution"]["orders"][0]["status"] == "UNKNOWN"
        assert payload["execution"]["unknown_obligations"][0]["reason"] == "OUTCOME_UNCERTAIN"
        unknown = payload["execution"]["unknown_obligations"][0]
        assert unknown["record_kind"] == "submission"
        assert unknown["record_id"] == payload["execution"]["orders"][0]["attempt_id"]
        detail_status, detail = _json(f"{fixture.url}/api/ui-record?kind=submission&id={unknown['record_id']}")
        assert detail_status == 200
        assert detail["id"] == unknown["record_id"]


def test_fixture_authority_absent_armed_without_permission_and_stale_are_distinct() -> None:
    with FixtureServer("missingauth") as fixture:
        status, payload = _json(fixture.url + "/api/v2/canary")
        assert status == 200
        assert payload["execution_authorization"] is None
        fixture.set_scenario("armed_no_permission")
        status, payload = _json(fixture.url + "/api/v2/canary")
        assert status == 200
        assert payload["operator_controls"]["armed"] is True
        assert payload["execution_authorization"]["status"] == "DRAFT"
        fixture.set_scenario("stale")
        status, payload = _json(fixture.url + "/api/v2/canary")
        assert status == 200
        checked_at = datetime.fromisoformat(payload["readiness"]["checked_at"])
        age = (datetime.now(timezone.utc) - checked_at).total_seconds()
        assert 90 <= age <= 180
        status, ui_status = _json(f"{fixture.url}/api/ui-status")
        assert status == 200
        assert ui_status["schema"] == "ui-status.v1"
        assert ui_status["status"] == "stale"
        assert any(row["status"] == "stale" for row in ui_status["workers"])

def test_binance_fixture_is_parked_without_forbidden_probe_or_activation_controls() -> None:
    with FixtureServer("prepared") as fixture:
        assert fixture.url is not None
        status, payload = _json(fixture.url + "/api/v2/binance-canary")
        assert status == 200
        assert payload["fixture_banner"].startswith("FIXTURE")
        no_token_status, _content_type, _body = _post(f"{fixture.url}/api/binance/control")
        assert no_token_status == 403
        token_status, token_body = _json(f"{fixture.url}/api/control-token")
        assert token_status == 200
        assert token_body["token"] == fixture.control_token
        control_status, control_result = _admin_post(f"{fixture.url}/api/binance/control", {"action": "DISARM", "payload": {}}, headers={"X-Axiom-Control-Token": token_body["token"], "Origin": fixture.url})
        assert control_status == 200
        assert control_result["fixture"] is True
        assert control_result["action"] == "DISARM"
        assert fixture.control.calls == []
        assert fixture.binance_control.calls[-1]["action"] == "DISARM"
        assert payload["environment"] == "BINANCE_SPOT_TESTNET"
        assert payload["status"] == "PARKED"
        assert payload["transport"] == "DISABLED"
        encoded = json.dumps(payload).upper()
        assert "EXECUTION_PROBE" not in encoded
        assert "RECONCILE_PROBE" not in encoded
