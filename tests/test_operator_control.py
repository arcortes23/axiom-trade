from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import Mock, patch
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from types import SimpleNamespace

from axiom.canary import AUTONOMOUS_CANARY_LIMITS, CanaryService, CredentialStore
from axiom.dashboard import DashboardData, DashboardServer, _DashboardHandler
from axiom.operator import (
    BOOTSTRAP_JOB_NAME,
    CANARY_CONNECTIVITY_CONFIG_KEY,
    DEFAULT_HERMES_JOB_ID,
    OperatorControlPlane,
    _loopback_host,
)
from axiom.ranker import CandidateCanaryRanker
from axiom.storage import AxiomStore


CONNECTIVITY_SECRET_VALUES = (
    "PRIVATE-KEY-SENTINEL",
    "WALLET-ADDRESS-SENTINEL",
    "RAW-DIAGNOSTIC-SENTINEL",
    "SPENDER-IDENTIFIER-SENTINEL",
)


def _raw_connectivity_result(*, allowance_status: str = "OK") -> dict[str, object]:
    allowance: dict[str, object] = {
        "status": allowance_status,
        "available_base_units": "1000000" if allowance_status == "OK" else "1",
        "spender": CONNECTIVITY_SECRET_VALUES[3],
        "raw": CONNECTIVITY_SECRET_VALUES[2],
    }
    return {
        "ready": allowance_status == "OK",
        "credentials_configured": True,
        "message": "READY FOR CANARY CONNECTIVITY",
        "failures": [] if allowance_status == "OK" else ["CANARY_ALLOWANCE_INSUFFICIENT"],
        "diagnostics": {
            "sdk_version": "0.9.0",
            "credentials_configured": True,
            "authentication": {
                "status": "OK",
                "private_key": CONNECTIVITY_SECRET_VALUES[0],
            },
            "account": {
                "authenticated": True,
                "wallet_type": "EOA",
                "wallet_address": CONNECTIVITY_SECRET_VALUES[1],
            },
            "geoblock": {
                "blocked": False,
                "close_only": False,
                "country": "ZZ",
                "region": "T",
                "diagnostic": CONNECTIVITY_SECRET_VALUES[2],
            },
            "balance": {
                "status": "OK",
                "available_usd": "10",
                "private_key": CONNECTIVITY_SECRET_VALUES[0],
            },
            "allowance": allowance,
            "market": {"status": "SKIPPED", "raw": CONNECTIVITY_SECRET_VALUES[2]},
            "book": {"status": "SKIPPED", "raw": CONNECTIVITY_SECRET_VALUES[2]},
        },
        "live_execution": False,
        "connectivity_only": True,
    }


def _safe_connectivity_projection(
    *,
    ready: bool,
    allowance_status: str,
    checked_at: str = "2026-01-02T12:00:00+00:00",
) -> dict[str, object]:
    failure_codes = [] if ready else ["CANARY_ALLOWANCE_INSUFFICIENT"]
    failure_reasons = (
        []
        if ready
        else [
            {
                "code": "CANARY_ALLOWANCE_INSUFFICIENT",
                "reason": "Current allowance is below the amount required for a $1 canary.",
            }
        ]
    )
    return {
        "ready": ready,
        "status": "READY" if ready else "BLOCKED",
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
        "geoblock": {"status": "PASS", "country": "ZZ", "region": "T"},
        "balance": {"status": "PASS", "available_usd": "10"},
        "allowance": {"status": allowance_status},
        "market": {"status": "SKIPPED"},
        "order_book": {"status": "SKIPPED"},
        "failure_codes": failure_codes,
        "failure_reasons": failure_reasons,
        "live_execution": False,
    }


class ConnectivityVenueSentinel:
    """Read-only venue fake whose execution methods fail loudly if touched."""

    def __init__(self, *, allowance_status: str = "OK") -> None:
        self.allowance_status = allowance_status
        self.order_calls = 0
        self.connectivity_calls = 0
        self.approval_calls = 0

    @staticmethod
    def installed_sdk_version() -> str:
        return "0.9.0"

    def geoblock(self) -> dict[str, object]:
        return {
            "blocked": False,
            "close_only": False,
            "country": "ZZ",
            "region": "T",
            "private_key": CONNECTIVITY_SECRET_VALUES[0],
        }

    def connectivity_check(self) -> bool:
        self.connectivity_calls += 1
        return True

    def account(self) -> dict[str, object]:
        return {
            "authenticated": True,
            "wallet_type": "EOA",
            "wallet_address": CONNECTIVITY_SECRET_VALUES[1],
        }

    @staticmethod
    def balance() -> Decimal:
        return Decimal("10")

    def allowance(self) -> dict[str, object]:
        return {
            "status": self.allowance_status,
            "available_base_units": "1000000" if self.allowance_status == "OK" else "1",
            "spender": CONNECTIVITY_SECRET_VALUES[3],
        }


    def market_context(self, market_id: str, token_id: str) -> dict[str, object]:
        return {
            "accepting_orders": True,
            "min_order_size": "1",
            "tick_size": "0.01",
            "bids": [{"price": "0.49", "size": "100"}],
            "asks": [{"price": "0.50", "size": "100"}],
            "allowance": self.allowance(),
            "raw": CONNECTIVITY_SECRET_VALUES[2],
        }

    def submit_limit_order(self, **_: object) -> None:
        self.order_calls += 1
        raise AssertionError("connectivity check attempted to submit an order")

    def create_limit_order(self, **_: object) -> None:
        self.order_calls += 1
        raise AssertionError("connectivity check attempted to create an order")

    def post_order(self, *_: object, **__: object) -> None:
        self.order_calls += 1
        raise AssertionError("connectivity check attempted to post an order")

    def approve(self, **_: object) -> None:
        self.approval_calls += 1
        raise AssertionError("connectivity check attempted an approval")


class OperatorControlTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.db = str(Path(self.tempdir.name) / "operator.sqlite")
        self.store = AxiomStore(self.db)
        self.control = OperatorControlPlane(self.store)
        self.server = DashboardServer(
            port=0,
            data=DashboardData(store=self.store, control=self.control),
        ).start()
        self.addCleanup(self.store.close)
        self.addCleanup(self.server.stop)

    def test_connectivity_control_persists_one_safe_success_projection(self) -> None:
        expected = _safe_connectivity_projection(
            ready=True,
            allowance_status="AVAILABLE",
        )
        raw_result = _raw_connectivity_result(allowance_status="OK")
        raw_result["checked_at"] = expected["checked_at"]
        service = Mock()
        service.connectivity_check.return_value = raw_result
        credentials = Mock()
        credentials.configured.return_value = True
        venue = ConnectivityVenueSentinel()
        with patch("axiom.operator.CredentialStore", return_value=credentials), patch(
            "axiom.operator.PolymarketClobV2Venue",
            return_value=venue,
        ), patch("axiom.operator.CanaryService", return_value=service), patch(
            "axiom.operator.utc_now",
            return_value=datetime(2026, 1, 2, 12, tzinfo=timezone.utc),
        ):
            response = self.control.execute("canary.connectivity_check")

        self.assertFalse(response["live_execution"])
        self.assertTrue(response["ok"])
        connectivity = response["result"]["connectivity"]
        self.assertEqual(connectivity, expected)
        self.assertEqual(
            self.store.get_operator_config(CANARY_CONNECTIVITY_CONFIG_KEY),
            expected,
        )
        self.assertEqual(self.control.status()["canary"]["connectivity"], expected)
        self.assertEqual(self.control.status()["connectivity"], expected)
        service.submit.assert_not_called()
        service.submit_signal.assert_not_called()
        service.create_limit_order.assert_not_called()
        service.post_order.assert_not_called()
        service.approve.assert_not_called()
        service.connectivity_check.assert_called_once_with(
            venue=venue,
            allow_environment=False,
        )
        service.enable_autonomous_micro_live.assert_not_called()
        self.assertEqual(venue.order_calls, 0)
        self.assertEqual(venue.approval_calls, 0)

        dashboard = DashboardData(store=self.store, control=self.control).canary_data()
        self.assertEqual(dashboard["connectivity"], expected)
        encoded = json.dumps(
            {
                "response": response,
                "stored": self.store.get_operator_config(CANARY_CONNECTIVITY_CONFIG_KEY),
                "dashboard": dashboard,
            },
            default=str,
        )
        for secret in CONNECTIVITY_SECRET_VALUES:
            self.assertNotIn(secret, encoded)
        self.assertEqual(
            set(connectivity),
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
        self.assertFalse(connectivity["live_execution"])

    def test_connectivity_control_runs_real_service_authenticated_read_only(self) -> None:
        expected = _safe_connectivity_projection(
            ready=True,
            allowance_status="AVAILABLE",
        )
        credentials = Mock()
        credentials.configured.return_value = True
        venue = ConnectivityVenueSentinel()
        with patch("axiom.operator.CredentialStore", return_value=credentials), patch(
            "axiom.operator.PolymarketClobV2Venue",
            return_value=venue,
        ), patch(
            "axiom.operator.utc_now",
            return_value=datetime(2026, 1, 2, 12, tzinfo=timezone.utc),
        ):
            response = self.control.execute("canary.connectivity_check")

        self.assertTrue(response["ok"])
        self.assertFalse(response["live_execution"])
        connectivity = response["result"]["connectivity"]
        self.assertEqual(connectivity, expected)
        self.assertEqual(
            self.store.get_operator_config(CANARY_CONNECTIVITY_CONFIG_KEY),
            expected,
        )
        self.assertEqual(self.control.status()["connectivity"], expected)
        dashboard = DashboardData(store=self.store, control=self.control).canary_data()
        self.assertEqual(dashboard["connectivity"], expected)
        encoded = json.dumps(
            {
                "response": response,
                "stored": self.store.get_operator_config(CANARY_CONNECTIVITY_CONFIG_KEY),
                "dashboard": dashboard,
            },
            default=str,
        )
        for secret in CONNECTIVITY_SECRET_VALUES:
            self.assertNotIn(secret, encoded)

        self.assertEqual(credentials.configured.call_count, 2)
        self.assertEqual(venue.connectivity_calls, 1)
        self.assertEqual(venue.order_calls, 0)
        self.assertEqual(venue.approval_calls, 0)

    def test_connectivity_control_real_service_blocks_without_credentials(self) -> None:
        credentials = Mock()
        credentials.configured.return_value = False
        with patch("axiom.operator.CredentialStore", return_value=credentials), patch(
            "axiom.operator.PolymarketClobV2Venue"
        ) as venue_factory, patch(
            "axiom.operator.utc_now",
            return_value=datetime(2026, 1, 2, 12, tzinfo=timezone.utc),
        ):
            response = self.control.execute("canary.connectivity_check")

        self.assertTrue(response["ok"])
        self.assertFalse(response["live_execution"])
        connectivity = response["result"]["connectivity"]
        self.assertFalse(connectivity["ready"])
        self.assertEqual(connectivity["status"], "BLOCKED")
        self.assertEqual(connectivity["credentials"], {"status": "NOT CONFIGURED"})
        self.assertEqual(connectivity["authentication"], {"status": "SKIPPED"})
        self.assertEqual(connectivity["account"], {"status": "SKIPPED", "wallet_type": None})
        self.assertEqual(connectivity["balance"], {"status": "SKIPPED", "available_usd": None})
        self.assertEqual(connectivity["failure_codes"], ["CREDENTIALS_NOT_CONFIGURED"])
        self.assertEqual(
            connectivity["failure_reasons"],
            [
                {
                    "code": "CREDENTIALS_NOT_CONFIGURED",
                    "reason": "Polymarket credentials are not configured.",
                }
            ],
        )
        self.assertEqual(
            self.store.get_operator_config(CANARY_CONNECTIVITY_CONFIG_KEY),
            connectivity,
        )
        venue_factory.assert_not_called()
        credentials.configured.assert_called_with(allow_environment=False)


    def test_connectivity_blocked_allowance_has_exact_readable_failure(self) -> None:
        expected = _safe_connectivity_projection(
            ready=False,
            allowance_status="INSUFFICIENT",
        )
        raw_result = _raw_connectivity_result(allowance_status="INSUFFICIENT")
        raw_result["checked_at"] = expected["checked_at"]
        service = Mock()
        service.connectivity_check.return_value = raw_result
        credentials = Mock()
        credentials.configured.return_value = True
        venue = ConnectivityVenueSentinel(allowance_status="INSUFFICIENT")
        with patch("axiom.operator.CredentialStore", return_value=credentials), patch(
            "axiom.operator.PolymarketClobV2Venue",
            return_value=venue,
        ), patch("axiom.operator.CanaryService", return_value=service), patch(
            "axiom.operator.utc_now",
            return_value=datetime(2026, 1, 2, 12, tzinfo=timezone.utc),
        ):
            response = self.control.execute("canary.connectivity_check")

        self.assertTrue(response["ok"])
        self.assertFalse(response["live_execution"])
        connectivity = response["result"]["connectivity"]
        self.assertFalse(connectivity["ready"])
        self.assertEqual(connectivity["status"], "BLOCKED")
        self.assertEqual(connectivity["allowance"], {"status": "INSUFFICIENT"})
        self.assertEqual(connectivity["failure_codes"], ["CANARY_ALLOWANCE_INSUFFICIENT"])
        self.assertEqual(
            connectivity["failure_reasons"],
            [
                {
                    "code": "CANARY_ALLOWANCE_INSUFFICIENT",
                    "reason": "Current allowance is below the amount required for a $1 canary.",
                }
            ],
        )
        self.assertEqual(
            self.store.get_operator_config(CANARY_CONNECTIVITY_CONFIG_KEY),
            expected,
        )
        self.assertEqual(venue.order_calls, 0)
        self.assertEqual(venue.approval_calls, 0)
        service.connectivity_check.assert_called_once_with(
            venue=venue,
            allow_environment=False,
        )
        service.submit.assert_not_called()
        service.submit_signal.assert_not_called()
        service.create_limit_order.assert_not_called()
        service.post_order.assert_not_called()
        service.approve.assert_not_called()

    def test_connectivity_unknown_failure_is_collapsed_without_echoing_source(self) -> None:
        opaque_failure = "upstream exploded: PRIVATE-KEY-SENTINEL"
        raw_result = _raw_connectivity_result()
        raw_result["failures"] = [opaque_failure]
        credentials = Mock()
        credentials.configured.return_value = True
        service = Mock()
        service.connectivity_check.return_value = raw_result
        venue = ConnectivityVenueSentinel()
        with patch("axiom.operator.CredentialStore", return_value=credentials), patch(
            "axiom.operator.PolymarketClobV2Venue",
            return_value=venue,
        ), patch("axiom.operator.CanaryService", return_value=service), patch(
            "axiom.operator.utc_now",
            return_value=datetime(2026, 1, 2, 12, tzinfo=timezone.utc),
        ):
            response = self.control.execute("canary.connectivity_check")

        self.assertTrue(response["ok"])
        connectivity = response["result"]["connectivity"]
        self.assertEqual(connectivity["status"], "BLOCKED")
        self.assertEqual(connectivity["failure_codes"], ["CONNECTIVITY_CHECK_FAILED"])
        self.assertEqual(
            connectivity["failure_reasons"],
            [{"code": "CONNECTIVITY_CHECK_FAILED", "reason": "Connectivity check failed."}],
        )
        encoded = json.dumps(
            {
                "response": response,
                "stored": self.store.get_operator_config(CANARY_CONNECTIVITY_CONFIG_KEY),
                "actions": self.store.list_operator_actions(),
            },
            default=str,
        )
        self.assertNotIn(opaque_failure, encoded)
        self.assertNotIn("PRIVATE-KEY-SENTINEL", encoded)

    def test_connectivity_dual_failure_fields_include_legacy_unknown_without_echo(self) -> None:
        opaque_failure = "legacy failure: PRIVATE-KEY-SENTINEL"
        raw_result = _raw_connectivity_result()
        raw_result["failure_codes"] = []
        raw_result["failures"] = [opaque_failure]
        credentials = Mock()
        credentials.configured.return_value = True
        service = Mock()
        service.connectivity_check.return_value = raw_result
        venue = ConnectivityVenueSentinel()
        with patch("axiom.operator.CredentialStore", return_value=credentials), patch(
            "axiom.operator.PolymarketClobV2Venue",
            return_value=venue,
        ), patch("axiom.operator.CanaryService", return_value=service), patch(
            "axiom.operator.utc_now",
            return_value=datetime(2026, 1, 2, 12, tzinfo=timezone.utc),
        ):
            response = self.control.execute("canary.connectivity_check")

        self.assertTrue(response["ok"])
        connectivity = response["result"]["connectivity"]
        self.assertFalse(connectivity["ready"])
        self.assertEqual(connectivity["status"], "BLOCKED")
        self.assertEqual(connectivity["failure_codes"], ["CONNECTIVITY_CHECK_FAILED"])
        encoded = json.dumps(
            {
                "response": response,
                "stored": self.store.get_operator_config(CANARY_CONNECTIVITY_CONFIG_KEY),
                "actions": self.store.list_operator_actions(),
            },
            default=str,
        )
        self.assertNotIn(opaque_failure, encoded)
        self.assertNotIn("PRIVATE-KEY-SENTINEL", encoded)
    def test_connectivity_raw_ready_with_failed_account_is_blocked(self) -> None:
        raw_result = _raw_connectivity_result()
        raw_result["failure_codes"] = []
        raw_result["failures"] = []
        account = raw_result["diagnostics"]["account"]
        account["authenticated"] = False
        account["status"] = "FAIL"
        credentials = Mock()
        credentials.configured.return_value = True
        service = Mock()
        service.connectivity_check.return_value = raw_result
        venue = ConnectivityVenueSentinel()
        with patch("axiom.operator.CredentialStore", return_value=credentials), patch(
            "axiom.operator.PolymarketClobV2Venue",
            return_value=venue,
        ), patch("axiom.operator.CanaryService", return_value=service), patch(
            "axiom.operator.utc_now",
            return_value=datetime(2026, 1, 2, 12, tzinfo=timezone.utc),
        ):
            response = self.control.execute("canary.connectivity_check")

        self.assertTrue(response["ok"])
        connectivity = response["result"]["connectivity"]
        self.assertFalse(connectivity["ready"])
        self.assertEqual(connectivity["status"], "BLOCKED")
        self.assertEqual(connectivity["account"]["status"], "FAIL")
        self.assertEqual(connectivity["failure_codes"], ["ACCOUNT_CHECK_FAILED"])
        self.assertEqual(
            self.store.get_operator_config(CANARY_CONNECTIVITY_CONFIG_KEY),
            connectivity,
        )
        service.connectivity_check.assert_called_once_with(
            venue=venue,
            allow_environment=False,
        )
        service.enable_autonomous_micro_live.assert_not_called()
        self.assertEqual(venue.order_calls, 0)
        self.assertEqual(venue.approval_calls, 0)



    def test_connectivity_failure_codes_cap_reserves_unknown_fallback(self) -> None:
        opaque_failure = "legacy unknown: WALLET-ADDRESS-SENTINEL"
        known_codes = [
            "CREDENTIALS_NOT_CONFIGURED",
            "VENUE_REQUIRED",
            "OFFICIAL_POLYMARKET_SDK_NOT_INSTALLED",
            "UNSUPPORTED_POLYMARKET_SDK",
            "OFFICIAL_POLYMARKET_SDK_NOT_READONLY_COMPATIBLE",
            "GEOGRAPHICALLY_BLOCKED",
            "GEOBLOCK_CHECK_FAILED",
            "AUTHENTICATED_CONNECTIVITY_FAILED",
            "ACCOUNT_CHECK_FAILED",
            "BALANCE_CHECK_FAILED",
            "BALANCE_RESPONSE_INVALID",
            "INSUFFICIENT_BALANCE",
            "CANARY_ALLOWANCE_UNAVAILABLE",
            "CANARY_ALLOWANCE_INSUFFICIENT",
            "CANARY_SPENDER_UNAVAILABLE",
            "MARKET_CONNECTIVITY_FAILED",
            "CONNECTIVITY_CHECK_FAILED",
        ]
        raw_result = _raw_connectivity_result()
        raw_result["failure_codes"] = known_codes
        raw_result["failures"] = [opaque_failure]
        credentials = Mock()
        credentials.configured.return_value = True
        service = Mock()
        service.connectivity_check.return_value = raw_result
        venue = ConnectivityVenueSentinel()
        with patch("axiom.operator.CredentialStore", return_value=credentials), patch(
            "axiom.operator.PolymarketClobV2Venue",
            return_value=venue,
        ), patch("axiom.operator.CanaryService", return_value=service), patch(
            "axiom.operator.utc_now",
            return_value=datetime(2026, 1, 2, 12, tzinfo=timezone.utc),
        ):
            response = self.control.execute("canary.connectivity_check")

        self.assertTrue(response["ok"])
        connectivity = response["result"]["connectivity"]
        self.assertFalse(connectivity["ready"])
        self.assertEqual(connectivity["status"], "BLOCKED")
        failure_codes = connectivity["failure_codes"]
        self.assertEqual(len(failure_codes), 16)
        self.assertIn("CONNECTIVITY_CHECK_FAILED", failure_codes)
        self.assertNotIn(opaque_failure, failure_codes)
        encoded = json.dumps(
            {
                "response": response,
                "stored": self.store.get_operator_config(CANARY_CONNECTIVITY_CONFIG_KEY),
                "actions": self.store.list_operator_actions(),
            },
            default=str,
        )
        self.assertNotIn(opaque_failure, encoded)
        self.assertNotIn("WALLET-ADDRESS-SENTINEL", encoded)
    def test_concurrent_connectivity_checks_serialize_and_keep_newer_result(self) -> None:
        first_started = threading.Event()
        release_first = threading.Event()
        second_attempted = threading.Event()
        second_started = threading.Event()
        responses: dict[str, dict[str, object]] = {}

        first_raw = _raw_connectivity_result()
        second_raw = _raw_connectivity_result(allowance_status="INSUFFICIENT")
        first_service = Mock()
        second_service = Mock()

        def delayed_first_check(**_: object) -> dict[str, object]:
            first_started.set()
            if not release_first.wait(2):
                raise AssertionError("first connectivity check was not released")
            return first_raw

        def second_check(**_: object) -> dict[str, object]:
            second_started.set()
            return second_raw

        first_service.connectivity_check.side_effect = delayed_first_check
        second_service.connectivity_check.side_effect = second_check
        credentials = Mock()
        credentials.configured.return_value = True
        venue = ConnectivityVenueSentinel()

        def run(name: str) -> None:
            responses[name] = self.control.execute("canary.connectivity_check")

        with patch("axiom.operator.CredentialStore", return_value=credentials), patch(
            "axiom.operator.PolymarketClobV2Venue",
            return_value=venue,
        ), patch(
            "axiom.operator.CanaryService",
            side_effect=[first_service, second_service],
        ):
            first_thread = threading.Thread(target=run, args=("first",), daemon=True)
            second_thread = threading.Thread(
                target=lambda: (second_attempted.set(), run("second")),
                daemon=True,
            )
            try:
                first_thread.start()
                self.assertTrue(first_started.wait(1))
                second_thread.start()
                self.assertTrue(second_attempted.wait(1))
                self.assertFalse(second_started.wait(0.2))
            finally:
                release_first.set()
                first_thread.join(2)
                second_thread.join(2)

        self.assertFalse(first_thread.is_alive())
        self.assertFalse(second_thread.is_alive())
        self.assertTrue(second_started.is_set())
        self.assertTrue(responses["first"]["ok"])
        self.assertTrue(responses["second"]["ok"])
        self.assertEqual(
            self.store.get_operator_config(CANARY_CONNECTIVITY_CONFIG_KEY),
            responses["second"]["result"]["connectivity"],
        )
        self.assertEqual(
            responses["second"]["result"]["connectivity"]["failure_codes"],
            ["CANARY_ALLOWANCE_INSUFFICIENT"],
        )
        first_service.connectivity_check.assert_called_once_with(
            venue=venue,
            allow_environment=False,
        )
        second_service.connectivity_check.assert_called_once_with(
            venue=venue,
            allow_environment=False,
        )
        self.assertEqual(venue.order_calls, 0)
        self.assertEqual(venue.approval_calls, 0)



    def test_enable_waits_for_inflight_blocked_connectivity_check(self) -> None:
        connectivity_started = threading.Event()
        release_connectivity = threading.Event()
        enable_attempted = threading.Event()
        readiness_read = threading.Event()
        results: dict[str, dict[str, object]] = {}

        self.store.set_operator_config(
            CANARY_CONNECTIVITY_CONFIG_KEY,
            _safe_connectivity_projection(
                ready=True,
                allowance_status="AVAILABLE",
            ),
        )
        blocked_raw = _raw_connectivity_result(allowance_status="INSUFFICIENT")
        blocked_service = Mock()
        transition_service = Mock()

        def delayed_blocked_check(**_: object) -> dict[str, object]:
            connectivity_started.set()
            if not release_connectivity.wait(2):
                raise AssertionError("blocked connectivity check was not released")
            return blocked_raw

        blocked_service.connectivity_check.side_effect = delayed_blocked_check
        credentials = Mock()
        credentials.configured.return_value = True
        venue = ConnectivityVenueSentinel(allowance_status="INSUFFICIENT")
        original_get = self.store.get_operator_config

        def observed_get(key: str, default: object = None) -> object:
            if key == CANARY_CONNECTIVITY_CONFIG_KEY:
                readiness_read.set()
            return original_get(key, default)

        def run_connectivity() -> None:
            results["connectivity"] = self.control.execute("canary.connectivity_check")

        def run_enable() -> None:
            enable_attempted.set()
            results["enable"] = self.control.execute(
                "canary.enable_auto",
                confirm="ENABLE AUTO CANARY",
            )

        with patch.object(self.store, "get_operator_config", side_effect=observed_get), patch(
            "axiom.operator.CredentialStore",
            return_value=credentials,
        ), patch(
            "axiom.operator.PolymarketClobV2Venue",
            return_value=venue,
        ), patch(
            "axiom.operator.CanaryService",
            side_effect=[blocked_service, transition_service],
        ):
            connectivity_thread = threading.Thread(target=run_connectivity, daemon=True)
            enable_thread = threading.Thread(target=run_enable, daemon=True)
            try:
                connectivity_thread.start()
                self.assertTrue(connectivity_started.wait(1))
                enable_thread.start()
                self.assertTrue(enable_attempted.wait(1))
                self.assertFalse(readiness_read.wait(0.2))
            finally:
                release_connectivity.set()
                connectivity_thread.join(2)
                enable_thread.join(2)

        self.assertFalse(connectivity_thread.is_alive())
        self.assertFalse(enable_thread.is_alive())
        self.assertTrue(results["connectivity"]["ok"])
        self.assertFalse(results["enable"]["ok"])
        self.assertEqual(results["enable"]["reason"], "CANARY_ALLOWANCE_INSUFFICIENT")
        blocked_service.connectivity_check.assert_called_once_with(
            venue=venue,
            allow_environment=False,
        )
        transition_service.enable_autonomous_micro_live.assert_not_called()
        self.assertEqual(venue.order_calls, 0)
        self.assertEqual(venue.approval_calls, 0)

        persisted = self.store.get_operator_config(CANARY_CONNECTIVITY_CONFIG_KEY)
        self.assertEqual(persisted["status"], "BLOCKED")
        self.assertEqual(persisted["failure_codes"], ["CANARY_ALLOWANCE_INSUFFICIENT"])





    def test_connectivity_exponent_balance_is_null_and_failed_without_expansion(self) -> None:
        raw_result = _raw_connectivity_result()
        raw_result["failures"] = ["BALANCE_RESPONSE_INVALID"]
        raw_result["diagnostics"]["balance"]["status"] = "FAILED"
        raw_result["diagnostics"]["balance"]["available_usd"] = "1e2"
        credentials = Mock()
        credentials.configured.return_value = True
        service = Mock()
        service.connectivity_check.return_value = raw_result

        venue = ConnectivityVenueSentinel()
        with patch("axiom.operator.CredentialStore", return_value=credentials), patch(
            "axiom.operator.PolymarketClobV2Venue",
            return_value=venue,
        ), patch("axiom.operator.CanaryService", return_value=service), patch(
            "axiom.operator.utc_now",
            return_value=datetime(2026, 1, 2, 12, tzinfo=timezone.utc),
        ):
            response = self.control.execute("canary.connectivity_check")

        self.assertTrue(response["ok"])
        connectivity = response["result"]["connectivity"]
        self.assertEqual(connectivity["status"], "BLOCKED")
        self.assertEqual(connectivity["balance"], {"status": "FAIL", "available_usd": None})
        self.assertEqual(connectivity["failure_codes"], ["BALANCE_RESPONSE_INVALID"])

    def test_connectivity_exception_replaces_prior_ready_with_safe_block(self) -> None:
        prior = _safe_connectivity_projection(ready=True, allowance_status="AVAILABLE")
        self.store.set_operator_config(CANARY_CONNECTIVITY_CONFIG_KEY, prior)
        exception_secret = "VENUE-EXCEPTION-PRIVATE-KEY-SENTINEL"
        credentials = Mock()
        credentials.configured.return_value = True
        service = Mock()
        service.connectivity_check.side_effect = RuntimeError(exception_secret)
        venue = ConnectivityVenueSentinel()
        with patch("axiom.operator.CredentialStore", return_value=credentials), patch(
            "axiom.operator.PolymarketClobV2Venue",
            return_value=venue,
        ), patch("axiom.operator.CanaryService", return_value=service), patch(
            "axiom.operator.utc_now",
            return_value=datetime(2026, 1, 2, 12, tzinfo=timezone.utc),
        ):
            response = self.control.execute("canary.connectivity_check")

        self.assertTrue(response["ok"])
        self.assertEqual(response["action"], "canary.connectivity_check")
        connectivity = response["result"]["connectivity"]
        self.assertFalse(connectivity["ready"])
        self.assertEqual(connectivity["status"], "BLOCKED")
        self.assertEqual(connectivity["failure_codes"], ["CONNECTIVITY_CHECK_FAILED"])
        self.assertEqual(
            self.store.get_operator_config(CANARY_CONNECTIVITY_CONFIG_KEY),
            connectivity,
        )
        self.assertNotEqual(connectivity, prior)
        encoded = json.dumps(
            {
                "response": response,
                "stored": self.store.get_operator_config(CANARY_CONNECTIVITY_CONFIG_KEY),
                "actions": self.store.list_operator_actions(),
            },
            default=str,
        )
        self.assertNotIn(exception_secret, encoded)

    def test_connectivity_constructor_exception_replaces_prior_ready_with_safe_block(self) -> None:
        prior = _safe_connectivity_projection(ready=True, allowance_status="AVAILABLE")
        self.store.set_operator_config(CANARY_CONNECTIVITY_CONFIG_KEY, prior)
        exception_secret = "CONNECTIVITY-CONSTRUCTOR-PRIVATE-KEY-SENTINEL"
        credentials = Mock()
        credentials.configured.return_value = True
        with patch("axiom.operator.CredentialStore", return_value=credentials), patch(
            "axiom.operator.PolymarketClobV2Venue",
            side_effect=RuntimeError(exception_secret),
        ), patch("axiom.operator.utc_now", return_value=datetime(2026, 1, 2, 12, tzinfo=timezone.utc)):
            response = self.control.execute("canary.connectivity_check")

        self.assertTrue(response["ok"])
        connectivity = response["result"]["connectivity"]
        self.assertFalse(connectivity["ready"])
        self.assertEqual(connectivity["status"], "BLOCKED")
        self.assertEqual(connectivity["failure_codes"], ["CONNECTIVITY_CHECK_FAILED"])
        self.assertEqual(
            self.store.get_operator_config(CANARY_CONNECTIVITY_CONFIG_KEY),
            connectivity,
        )
        encoded = json.dumps(
            {
                "response": response,
                "stored": self.store.get_operator_config(CANARY_CONNECTIVITY_CONFIG_KEY),
                "actions": self.store.list_operator_actions(),
            },
            default=str,
        )
        self.assertNotIn(exception_secret, encoded)


    def test_operator_status_separates_unknown_external_hermes_from_internal_queue(self) -> None:
        before_changes = self.store.connection.total_changes
        status = self.control.status()
        self.assertEqual(self.store.connection.total_changes, before_changes)

        hermes = status["hermes"]
        external = hermes["external_hermes"]
        self.assertEqual(external["job_id"], DEFAULT_HERMES_JOB_ID)
        self.assertEqual(external["status"], "UNKNOWN")
        self.assertIn("evidence", external)
        self.assertNotEqual(external["status"], hermes["internal_queue"]["status"])
        self.assertEqual(hermes["internal_queue"]["status"], "ACTIVE")
        self.assertIn("trigger", hermes["internal_queue"])
        self.assertIsNone(hermes["internal_queue"]["last_cycle_at"])
        self.assertEqual(hermes["control_scope"], "INTERNAL_RESEARCH_QUEUE_PROCESSOR")

    def test_operator_status_retains_latest_canary_signal(self) -> None:
        latest_signal = {
            "signal_id": "signal-status-regression",
            "candidate_id": "candidate-status-regression",
            "status": "READY",
        }
        with patch.object(CanaryService, "latest_signal", return_value=latest_signal):
            status = self.control.status()
        self.assertEqual(status["canary"]["latest_signal"], latest_signal)


    def test_operator_exports_default_hermes_job_id_and_checks_loopback_hosts(self) -> None:
        exported: dict[str, object] = {}
        exec("from axiom.operator import *", exported)
        self.assertEqual(exported["DEFAULT_HERMES_JOB_ID"], DEFAULT_HERMES_JOB_ID)
        self.assertTrue(_loopback_host("localhost"))
        self.assertTrue(_loopback_host("127.0.0.1"))
        self.assertTrue(_loopback_host("::1"))
        self.assertFalse(_loopback_host("192.0.2.1"))


    def test_connectivity_projection_survives_fresh_file_backed_dashboard(self) -> None:
        restart_db = str(Path(self.tempdir.name) / "connectivity-restart.sqlite")
        expected = _safe_connectivity_projection(
            ready=False,
            allowance_status="INSUFFICIENT",
        )
        service = Mock()
        service.connectivity_check.return_value = expected
        credentials = Mock()
        credentials.configured.return_value = False
        with AxiomStore(restart_db) as original_store:
            original_control = OperatorControlPlane(original_store)
            with patch("axiom.operator.CredentialStore", return_value=credentials), patch(
                "axiom.operator.CanaryService",
                return_value=service,
            ), patch(
                "axiom.operator.utc_now",
                return_value=datetime(2026, 1, 2, 12, tzinfo=timezone.utc),
            ):
                response = original_control.execute("canary.connectivity_check")
            self.assertTrue(response["ok"])

        with AxiomStore(restart_db) as reopened_store:
            reopened_control = OperatorControlPlane(reopened_store)
            dashboard = DashboardData(
                store=reopened_store,
                control=reopened_control,
            ).canary_data()
            self.assertEqual(
                reopened_store.get_operator_config(CANARY_CONNECTIVITY_CONFIG_KEY),
                expected,
            )
            self.assertEqual(reopened_control.status()["connectivity"], expected)
            self.assertEqual(dashboard["connectivity"], expected)
            encoded = json.dumps(dashboard, default=str)
            for secret in CONNECTIVITY_SECRET_VALUES:
                self.assertNotIn(secret, encoded)

    def _post(self, body: dict, *, token: str | None = None) -> tuple[int, dict]:
        assert self.server.url is not None
        headers = {"Content-Type": "application/json"}
        if token is not None:
            headers["X-Axiom-Control-Token"] = token
        request = Request(
            self.server.url + "/api/control",
            data=json.dumps(body).encode(),
            headers=headers,
            method="POST",
        )
        with urlopen(request, timeout=3) as response:
            return response.status, json.loads(response.read())

    def test_localhost_control_requires_token_and_allowlist(self) -> None:
        assert self.server._server is not None
        assert self.server.url is not None
        body = {"action": "hermes.pause"}
        with self.assertRaises(HTTPError) as context:
            self._post(body)
        self.assertEqual(context.exception.code, 403)
        status, result = self._post(body, token=self.server._server.control_token)
        self.assertEqual(status, 200)
        self.assertTrue(result["ok"])
        self.assertEqual(self.store.get_scheduler_state("hermes-control")["status"], "PAUSED")
        self.assertFalse(_DashboardHandler._loopback_client(type("Request", (), {"client_address": ("192.0.2.1", 1)})()))

    def test_no_arbitrary_command_execution(self) -> None:
        launcher = Mock()
        control = OperatorControlPlane(self.store, node_launcher=launcher)
        result = control.execute("shell.exec", "python", confirm="")
        self.assertFalse(result["ok"])
        self.assertEqual(result["reason"], "ACTION_NOT_ALLOWED")
        launcher.assert_not_called()
        self.assertNotIn("command", json.dumps(result).lower())

    def test_duplicate_node_prevention(self) -> None:
        launcher = Mock()
        control = OperatorControlPlane(self.store, node_launcher=launcher)
        running = {"status": "RUNNING", "pid": 123, "worker_identity_valid": True}
        with patch.object(control, "_node_status", return_value=running):
            self.assertEqual(control.ensure_node(), running)
        launcher.assert_not_called()

    def test_fixed_hermes_adapter_operations_are_persisted(self) -> None:
        paused = self.control.execute("hermes.pause")
        self.assertTrue(paused["ok"])
        self.assertEqual(paused["control_scope"], "INTERNAL_RESEARCH_QUEUE_PROCESSOR")
        self.assertEqual(paused["result"]["hermes"]["control_scope"], "INTERNAL_RESEARCH_QUEUE_PROCESSOR")
        blocked = self.control.execute("hermes.run_now")
        self.assertFalse(blocked["ok"])
        self.assertEqual(blocked["reason"], "HERMES_PAUSED")
        self.assertEqual(blocked["control_scope"], "INTERNAL_RESEARCH_QUEUE_PROCESSOR")
        self.assertEqual(paused["result"]["hermes"]["internal_queue"]["status"], "PAUSED")
        self.assertEqual(paused["result"]["hermes"]["external_hermes"]["status"], "UNKNOWN")
        self.assertEqual(self.control.status()["hermes"]["status"], "PAUSED")

        resumed = self.control.execute("hermes.resume")
        self.assertTrue(resumed["ok"])
        self.assertEqual(resumed["control_scope"], "INTERNAL_RESEARCH_QUEUE_PROCESSOR")
        self.assertEqual(resumed["result"]["hermes"]["internal_queue"]["status"], "ACTIVE")
        self.assertEqual(resumed["result"]["hermes"]["external_hermes"]["status"], "UNKNOWN")
        self.assertEqual(self.control.status()["hermes"]["status"], "ACTIVE")

        processor = Mock()
        processor.process_pending.return_value = SimpleNamespace(claimed=1, completed=1, rejected=0, failed=0)
        with patch("axiom.operator.AutonomousResearchProcessor", return_value=processor):
            result = self.control.execute("hermes.run_now")
        self.assertTrue(result["ok"])
        self.assertEqual(result["control_scope"], "INTERNAL_RESEARCH_QUEUE_PROCESSOR")
        self.assertEqual(result["result"]["hermes"]["external_hermes"]["status"], "UNKNOWN")
        self.assertEqual(result["result"]["hermes"]["internal_queue"]["status"], "ACTIVE")
        processor.process_pending.assert_called_once_with(worker="operator-hermes")
        self.assertNotIn("argv", json.dumps(result).lower())


    def test_bootstrap_duplicate_and_resume_behavior(self) -> None:
        snapshot = SimpleNamespace(selected_symbols=("BTCUSDT",))
        started = threading.Event()
        release = threading.Event()

        def blocked_worker(**_: object) -> None:
            started.set()
            release.wait(2)

        with patch("axiom.operator.load_crypto_universe", return_value=snapshot), patch.object(
            self.control, "_bootstrap_worker", side_effect=blocked_worker
        ):
            first = self.control.start_bootstrap(resume=False)
            self.assertEqual(first["status"], "RUNNING")
            self.assertTrue(started.wait(1))
            with self.assertRaisesRegex(RuntimeError, "BOOTSTRAP_ALREADY_RUNNING"):
                self.control.start_bootstrap(resume=True)
            release.set()
            thread = self.control._bootstrap_threads[BOOTSTRAP_JOB_NAME]
            thread.join(2)
        self.store.set_operator_job(
            BOOTSTRAP_JOB_NAME,
            "FAILED",
            {"total_symbols": 1, "total_datasets": 4},
            pid=None,
            last_error="BOOTSTRAP_DATASET_FAILED",
            resumable=True,
        )
        with patch("axiom.operator.load_crypto_universe", return_value=snapshot), patch.object(
            self.control, "_bootstrap_worker", return_value=None
        ):
            resumed = self.control.start_bootstrap(resume=True)
            self.assertEqual(resumed["status"], "RUNNING")
            thread = self.control._bootstrap_threads[BOOTSTRAP_JOB_NAME]
            thread.join(2)

    def _seed_candidate(
        self,
        candidate_id: str = "candidate-1",
        *,
        store: AxiomStore | None = None,
        timestamp: datetime | None = None,
    ) -> None:
        target = store or self.store
        dataset_timestamp = datetime(2025, 1, 1, tzinfo=timezone.utc)
        target.save_dataset(
            "dataset-1",
            "dataset-v1",
            [{"timestamp": dataset_timestamp.isoformat(), "price": 0.5, "source_type": "HISTORICAL"}],
        )
        target.save_dataset_catalog(
            "dataset-1",
            "dataset-v1",
            provider="polymarket",
            instrument="POLYMARKET",
            market_type="prediction",
            timeframe="event",
            start_timestamp=dataset_timestamp,
            end_timestamp=dataset_timestamp,
            row_count=1,
            completeness=1.0,
            quality="PRICE_PROXY",
            source_type="HISTORICAL",
            snapshot_id="dataset-1:dataset-v1",
            metadata={
                "provider": "polymarket",
                "source_type": "HISTORICAL",
                "research_quality": "PRICE_PROXY",
                "historical_order_book_available": False,
            },
        )
        strategy_hash = "strategy-hash"
        model_hash = "model-hash"
        config_hash = "config-hash"
        frozen_hash = hashlib.sha256(f"{strategy_hash}|{model_hash}|{config_hash}".encode()).hexdigest()
        payload = {
            "candidate_id": candidate_id,
            "strategy_id": "strategy-1",
            "experiment_family": "test-family",
            "market_type": "prediction",
            "instrument": "MARKET-1",
            "dataset_id": "dataset-1",
            "dataset_version": "dataset-v1",
            "dataset_provenance": {
                "dataset_id": "dataset-1",
                "dataset_version": "dataset-v1",
                "source_type": "HISTORICAL",
                "time_split": "train-validation-holdout",
            },
            "source_type": "HISTORICAL",
            "timeframe": "event",
            "strategy_hash": strategy_hash,
            "model_hash": model_hash,
            "config_hash": config_hash,
            "frozen_hash": frozen_hash,
            "schema_validated": True,
            "historical_backtest_passed": True,
            "validation_passed": True,
            "robustness_passed": True,
            "data_quality_passed": True,
            "minimum_sample_check": {
                "passed": True,
                "count": 30,
                "trades": 0,
                "min_observations": 30,
                "min_trades": 0,
                "checks": {"observations": True, "trades": True},
            },
            "holdout_used": False,
            "frozen": True,
            "critical_error": None,
        }
        target.save_candidate_lifecycle(candidate_id, "IDEA", payload, timestamp=timestamp)
        target.save_candidate_lifecycle(candidate_id, "FROZEN", payload, from_stage="IDEA", timestamp=timestamp)
    def test_dashboard_restart_hydrates_persisted_overview_and_canary(self) -> None:
        restart_db = str(Path(self.tempdir.name) / "restart.sqlite")
        timestamp = datetime(2026, 1, 2, 12, tzinfo=timezone.utc)
        secrets = ("FAKE_PRIVATE_KEY", "FAKE_WALLET_ADDRESS", "FAKE_RELAYER_KEY")
        credentials = Mock()
        credentials.configured.return_value = True
        credentials.load.return_value = {
            "private_key": secrets[0],
            "wallet_address": secrets[1],
            "relayer_api_key": secrets[2],
        }
        credentials.safe_projection.return_value = {
            "configured": True,
            "status": "CONFIGURED",
            "secret_values_exposed": False,
        }

        with patch("axiom.storage._now_iso", return_value=timestamp.isoformat()):
            with AxiomStore(restart_db) as original_store:
                original_control = OperatorControlPlane(original_store)
                original_dashboard = DashboardData(store=original_store, control=original_control)
                self._seed_candidate("winner", store=original_store, timestamp=timestamp)
                winner = original_store.load_candidate_lifecycle("winner")
                self.assertIsInstance(winner, dict)
                winner_payload = dict(winner["payload"])

                def rankable_payload(candidate_id: str, score: float) -> dict[str, object]:
                    return {
                        **winner_payload,
                        "candidate_id": candidate_id,
                        "validation_expectancy": score,
                        "validation_confidence_lower_bound": score,
                        "validation_stability": 0.90,
                        "validation_calibration": 0.90,
                        "validation_sample_count": 100,
                        "validation_trade_count": 50,
                        "validation_execution_quality": 0.90,
                    }

                winner_payload = rankable_payload("winner", 0.42)
                original_store.save_candidate_lifecycle(
                    "winner",
                    "FROZEN",
                    winner_payload,
                    from_stage="FROZEN",
                    timestamp=timestamp,
                )
                runner_payload = rankable_payload("runner", 0.21)
                original_store.save_candidate_lifecycle("runner", "IDEA", runner_payload, timestamp=timestamp)
                original_store.save_candidate_lifecycle(
                    "runner",
                    "FROZEN",
                    runner_payload,
                    from_stage="IDEA",
                    timestamp=timestamp,
                )

                canary_service = CanaryService(original_store, clock=lambda: timestamp)
                ranking = CandidateCanaryRanker(
                    original_store,
                    service=canary_service,
                    clock=lambda: timestamp,
                ).evaluate_and_select(timestamp)
                self.assertEqual(ranking["selected_candidate"], "winner")
                selected = ranking["selected"]
                self.assertIsInstance(selected, dict)
                selected_score = selected["total_score"]
                self.assertIsInstance(selected_score, float)

                # Use the typed operator action to persist the disabled state and
                # its restart-safe autonomous decision/blocker.
                self.assertTrue(original_control.execute("canary.disarm", confirm="DISARM")["ok"])

                queue_item = original_store.enqueue_research_item(
                    "hypothesis",
                    {
                        "dataset_id": "dataset-1",
                        "dataset_version": "dataset-v1",
                        "family": "test-family",
                        "reason_code": "HERMES_FIXTURE_COMPLETE",
                    },
                    dedupe_key="hermes-restart-fixture",
                    source="hermes",
                    author="fixture",
                    item_id="hermes-restart-item",
                    available_at=timestamp,
                )
                self.assertEqual(queue_item["status"], "PENDING")
                claimed = original_store.claim_research_item("hermes-fixture", now=timestamp)
                self.assertIsNotNone(claimed)
                original_store.complete_research_item(
                    "hermes-restart-item",
                    "COMPLETED",
                    result={
                        "dataset_id": "dataset-1",
                        "dataset_version": "dataset-v1",
                        "family": "test-family",
                        "reason_code": "HERMES_FIXTURE_COMPLETE",
                    },
                    now=timestamp + timedelta(seconds=1),
                    worker="hermes-fixture",
                )

            # The context boundary above closes the original store before the
            # fresh objects below are created.
            with AxiomStore(restart_db) as reopened_store:
                reopened_control = OperatorControlPlane(reopened_store)
                reopened_dashboard = DashboardData(store=reopened_store, control=reopened_control)
                before_hydration_changes = reopened_store.connection.total_changes
                with patch("axiom.operator.CredentialStore", return_value=credentials), patch(
                    "axiom.canary.CredentialStore", return_value=credentials
                ):
                    overview = reopened_dashboard.overview_summary()
                    operator = reopened_dashboard.operator_data()
                    canary = reopened_dashboard.canary_data()
                self.assertEqual(reopened_store.connection.total_changes, before_hydration_changes)

        self.assertLessEqual(len(overview["latest_activity"]), 8)
        self.assertEqual(overview["coverage"]["historical_count"], 1)
        self.assertEqual(overview["coverage"]["historical_rows"], 1)
        self.assertEqual(overview["lifecycle_funnel"]["FROZEN"], 2)
        self.assertTrue(overview["latest_activity"])
        self.assertEqual(overview["research_cards"]["canary_eligible"], 2)
        self.assertEqual(
            {item["candidate_id"] for item in overview["latest_candidates"]},
            {"winner", "runner"},
        )
        overview_hermes = overview["hermes_latest_outcome"]
        self.assertIsInstance(overview_hermes, dict)
        self.assertEqual(overview_hermes["status"], "COMPLETED")
        self.assertEqual(overview_hermes["outcome_type"], "EXPERIMENT_COMPLETED")

        self.assertEqual(operator["research_cards"]["canary_eligible"], 2)
        self.assertEqual(operator["coverage"]["historical_count"], 1)
        self.assertEqual(operator["coverage"]["historical_rows"], 1)
        self.assertEqual(operator["lifecycle_funnel"]["FROZEN"], 2)
        latest_candidates = operator["latest_candidates"]
        self.assertLessEqual(len(latest_candidates), 10)
        self.assertEqual(
            {item["candidate_id"] for item in latest_candidates},
            {"winner", "runner"},
        )
        hermes_outcome = operator["hermes_latest_outcome"]
        self.assertIsInstance(hermes_outcome, dict)
        self.assertEqual(hermes_outcome["status"], "COMPLETED")
        self.assertEqual(hermes_outcome["outcome_type"], "EXPERIMENT_COMPLETED")
        self.assertEqual(hermes_outcome["reason_code"], "HERMES_FIXTURE_COMPLETE")
        self.assertEqual(hermes_outcome["dataset_id"], "dataset-1")
        self.assertEqual(hermes_outcome["dataset_version"], "dataset-v1")
        self.assertTrue(any(item["kind"] == "research" for item in operator["latest_activity"]))

        status = canary["canary"]
        self.assertEqual(status["micro_live_canary"], "DISARMED")
        self.assertEqual(status["display_state"], "DISABLED")
        self.assertEqual(status["risk_envelope"], dict(AUTONOMOUS_CANARY_LIMITS))
        self.assertEqual(status["risk_limits"], dict(AUTONOMOUS_CANARY_LIMITS))
        self.assertEqual(status["eligible_count"], 2)
        self.assertEqual(status["rankable_count"], 2)
        self.assertEqual(status["execution_event_count"], 0)
        self.assertEqual(status["real_execution_events"], 0)
        self.assertEqual(status["trades"], [])
        self.assertIsNone(canary["canary_signal"])
        self.assertEqual(canary["research_cards"]["canary_eligible"], 2)
        self.assertEqual(canary["candidate_status"]["rankable"], 2)
        self.assertEqual(canary["real_execution_events"], 0)

        autonomous = canary["autonomous_canary"]
        self.assertEqual(autonomous["selected_candidate"], "winner")
        self.assertEqual(autonomous["rank"], 1)
        self.assertEqual(autonomous["score"], selected_score)
        self.assertEqual(autonomous["selection_reason"], "SELECTED_WINNER")
        self.assertEqual(autonomous["eligible_count"], 2)
        self.assertEqual(autonomous["rankable_count"], 2)
        self.assertEqual(autonomous["historical_data_integrity"], "PASS")
        self.assertEqual(autonomous["historical_execution_fidelity"], "PRICE_PROXY · LIMITED")
        self.assertEqual(autonomous["current_execution_evidence"], "CURRENT_ORDER_BOOK_REQUIRED")
        self.assertEqual(autonomous["next_decision"], "ENABLE AUTO CANARY")
        self.assertEqual(autonomous["blocker"], "AUTONOMOUS_CANARY_DISABLED")

        for payload in (operator, canary):
            projected = payload["credentials"]
            self.assertEqual(
                set(projected),
                {"configured", "status", "secret_values_exposed"},
            )
            self.assertTrue(projected["configured"])
            self.assertEqual(projected["status"], "CONFIGURED")
            self.assertFalse(projected["secret_values_exposed"])
        encoded = json.dumps({"operator": operator, "canary": canary}, default=str)
        for secret in secrets:
            self.assertNotIn(secret, encoded)

    def test_candidate_provenance_is_explicit_and_complete(self) -> None:
        self._seed_candidate()
        data = DashboardData(store=self.store, control=self.control)
        row = data._candidate_row(self.store.load_candidate_lifecycle("candidate-1"))
        self.assertEqual(row["market"], "prediction")
        self.assertEqual(row["provenance"]["dataset_id"], "dataset-1")
        detail = data.strategy_detail("candidate-1")
        self.assertEqual(detail["provenance"]["dataset_version"], "dataset-v1")
        missing = data._candidate_row({"candidate_id": "missing", "payload": {}})
        self.assertEqual(missing["market"], "MISSING PROVENANCE")
        self.assertEqual(missing["provenance"]["status"], "MISSING PROVENANCE")

    def test_canary_eligibility_uses_backend_and_never_trades(self) -> None:
        self._seed_candidate()
        dry_run = CanaryService(self.store, initialize=False).validate_eligibility("candidate-1")
        self.assertTrue(dry_run["eligible"])
        service = Mock()
        service.validate_eligibility.return_value = {"candidate_id": "candidate-1", "eligible": True, "checks": []}
        with patch("axiom.operator.CanaryService", return_value=service):
            verified = self.control.execute("canary.eligibility.verify", "candidate-1")
            marked = self.control.execute(
                "canary.eligibility.mark", "candidate-1", confirm="MARK CANARY ELIGIBLE"
            )
        self.assertTrue(verified["ok"])
        self.assertTrue(marked["ok"])
        service.validate_eligibility.assert_called()
        service.mark_eligible.assert_called_once_with("candidate-1")
        service.arm.assert_not_called()
        service.submit.assert_not_called()

    def test_arm_requires_exact_confirmation_and_has_no_submit_path(self) -> None:
        credentials = Mock()
        credentials.configured.return_value = True
        service = Mock()
        service.arm.return_value = {"micro_live_canary": "ARMED", "limits": {"target_notional_usd": "1"}}
        with patch("axiom.operator.CredentialStore", return_value=credentials), patch(
            "axiom.operator.CanaryService", return_value=service
        ):
            denied = self.control.execute("canary.arm", "candidate-1", confirm="arm")
            armed = self.control.execute("canary.arm", "candidate-1", confirm="ARM")
        self.assertFalse(denied["ok"])
        self.assertEqual(denied["reason"], "EXACT_CONFIRMATION_REQUIRED")
        self.assertTrue(armed["ok"])
        service.arm.assert_called_once()
        service.submit.assert_not_called()
        self.assertNotIn("canary.submit", json.dumps(armed))

    def test_secret_isolation_and_audit_activity(self) -> None:
        credentials = Mock()
        credentials.configured.return_value = True
        credentials.load.return_value = {"private_key": "PRIVATE", "wallet_address": "WALLET"}
        with patch("axiom.operator.CredentialStore", return_value=credentials):
            status = self.control.status()
        encoded = json.dumps(status, default=str)
        self.assertNotIn("PRIVATE", encoded)
        self.assertNotIn("WALLET", encoded)
        self.assertFalse(status["credentials"]["secret_values_exposed"])
        self.control.execute("hermes.pause")
        self.control.execute("not.allowed")
        actions = self.store.list_operator_actions()
        self.assertGreaterEqual(len(actions), 2)
        activity = self.store.paginate_research_activity(kind="operator", page_size=25)
        self.assertGreaterEqual(activity["total"], 2)
    def test_manual_arm_target_is_distinct_from_persisted_autonomous_winner(self) -> None:
        timestamp = datetime.now(timezone.utc)
        self._seed_candidate("A", timestamp=timestamp)
        candidate_a = dict(self.store.load_candidate_lifecycle("A")["payload"])

        def rankable_payload(candidate_id: str, score: float) -> dict[str, object]:
            payload = {
                **candidate_a,
                "candidate_id": candidate_id,
                "mutation_cluster": f"cluster-{candidate_id}",
                "lineage": [candidate_id],
                "validation_expectancy": score,
                "validation_confidence_lower_bound": score,
                "validation_stability": 0.90,
                "validation_calibration": 0.90,
                "validation_sample_count": 100,
                "validation_trade_count": 50,
                "validation_execution_quality": 0.90,
            }
            return payload

        candidate_a = rankable_payload("A", 0.21)
        self.store.save_candidate_lifecycle(
            "A",
            "FROZEN",
            candidate_a,
            from_stage="FROZEN",
            timestamp=timestamp,
        )
        candidate_b = rankable_payload("B", 0.99)
        self.store.save_candidate_lifecycle(
            "B",
            "IDEA",
            candidate_b,
            timestamp=timestamp,
        )
        self.store.save_candidate_lifecycle(
            "B",
            "FROZEN",
            candidate_b,
            from_stage="IDEA",
            timestamp=timestamp,
        )
        credentials = Mock()
        credentials.configured.return_value = True
        service = CanaryService(
            self.store,
            credentials=credentials,
            clock=lambda: timestamp,
        )
        self.store.polymarket_health = lambda **_: {"grade": "A", "errors": 0}

        ranking = CandidateCanaryRanker(
            self.store,
            service=service,
            clock=lambda: timestamp,
        ).evaluate_and_select(timestamp)
        self.assertEqual(ranking["selected_candidate"], "B")
        self.assertEqual(ranking["winner_id"], "B")
        self.assertEqual(ranking["selection_status"], "CURRENT")
        self.assertTrue(ranking["selection_valid"])
        self.assertEqual(ranking["selected"]["candidate_id"], "B")

        class ArmVenue:
            @staticmethod
            def geoblock() -> dict[str, bool]:
                return {"blocked": False, "close_only": False}

        armed = service.arm("A", venue=ArmVenue(), credentials_configured=True)
        self.assertEqual(armed["candidate"], "A")
        self.assertEqual(armed["selected_candidate"], "B")
        self.assertEqual(armed["selection_status"], "CURRENT")
        self.assertTrue(armed["selection_valid"])
        self.assertEqual(armed["autonomous"]["selected_candidate"], "B")
        self.assertEqual(armed["autonomous"]["last_selected_candidate"], "B")
        self.assertIsInstance(armed["selected_winner"], dict)
        self.assertEqual(armed["selected_winner"]["candidate_id"], "B")
        self.assertEqual(armed["selected_winner"]["selected_candidate"], "B")
        self.assertEqual(armed["selected_winner"]["last_selected_candidate"], "B")

        check_a = service.check(candidate_id="A", venue=ArmVenue())
        check_b = service.check(candidate_id="B", venue=ArmVenue())
        self.assertNotIn("CANDIDATE_NOT_ARMED", check_a["failures"])
        self.assertIn("CANDIDATE_NOT_ARMED", check_b["failures"])

        with patch.object(
            CredentialStore,
            "safe_projection",
            return_value={
                "configured": False,
                "status": "NOT CONFIGURED",
                "secret_values_exposed": False,
            },
        ):
            dashboard = DashboardData(store=self.store, control=self.control).canary_data()
        self.assertEqual(dashboard["canary"]["candidate"], "A")
        self.assertEqual(dashboard["canary"]["selected_candidate"], "B")
        self.assertEqual(dashboard["canary"]["selection_status"], "CURRENT")
        self.assertTrue(dashboard["canary"]["selection_valid"])
        self.assertEqual(dashboard["autonomous_canary"]["selected_candidate"], "B")
        self.assertEqual(dashboard["autonomous_canary"]["last_selected_candidate"], "B")
        self.assertEqual(dashboard["canary"]["selected_winner"]["candidate_id"], "B")
        self.assertEqual(dashboard["canary"]["selected_winner"]["selected_candidate"], "B")
        self.assertEqual(
            dashboard["canary"]["selected_winner"]["last_selected_candidate"], "B"
        )


    def test_get_dashboard_is_side_effect_free_and_survives_control_failure(self) -> None:
        before_changes = self.store.connection.total_changes
        before_actions = len(self.store.list_operator_actions())
        response = self.store
        DashboardData(store=response, control=self.control).operator_data()
        self.assertEqual(self.store.connection.total_changes, before_changes)
        self.assertEqual(len(self.store.list_operator_actions()), before_actions)
        failing = Mock()
        failing.status.side_effect = RuntimeError("background failure")
        server = DashboardServer(port=0, data=DashboardData(store=self.store, control=failing)).start()
        self.addCleanup(server.stop)
        assert server.url is not None
        with self.assertRaises(HTTPError) as context:
            urlopen(server.url + "/api/operator", timeout=3)
        self.assertEqual(context.exception.code, 503)
        with urlopen(server.url + "/", timeout=3) as root:
            self.assertEqual(root.status, 200)


if __name__ == "__main__":
    unittest.main()
