from __future__ import annotations

import hashlib
import hmac
import json
import os
from pathlib import Path
import time
import tempfile
import unittest
from urllib.parse import parse_qs, urlsplit

from axiom.binance_spot import (
    BINANCE_SPOT_LIVE,
    BINANCE_SPOT_TESTNET,
    PAPER,
    BinanceCredentialRef,
    BinanceCredentialStore,
    BinanceRuntimeProfile,
    BinanceSpotConfigurationError,
    BinanceSpotCredentials,
    BinanceSpotTransportError,
    BinanceSpotEnvironment,
    BinanceSpotEnvironmentMismatch,
    BinanceSpotRESTClient,
)


class FakeKeyring:
    def __init__(self):
        self.values = {}

    def set_password(self, service, username, password):
        self.values[(service, username)] = password

    def get_password(self, service, username):
        return self.values.get((service, username))


class Response:
    def __init__(self, payload=None, status=200, headers=None):
        self.status = status
        self.headers = headers or {}
        self.payload = payload

    def read(self):
        return json.dumps(self.payload).encode()


def profile(tmp_path: Path) -> BinanceRuntimeProfile:
    runtime = tmp_path / "runtime-data"
    runtime.mkdir()
    return BinanceRuntimeProfile.development(tmp_path, runtime / "binance-dev.sqlite")


class BinanceSpotTests(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmpdir.cleanup)
        self.tmp_path = Path(self._tmpdir.name)

    def test_development_profile_is_immutable_and_preserves_lexical_db_path(self):
        p = profile(self.tmp_path)
        self.assertEqual(p.host, "127.0.0.1")
        self.assertEqual(p.port, 8081)
        expected_db = self.tmp_path / "runtime-data" / "binance-dev.sqlite"
        self.assertEqual(
            p.db_path,
            os.path.normcase(os.path.abspath(os.path.normpath(str(expected_db)))),
        )
        self.assertFalse(expected_db.exists())
        projection = p.projection()
        self.assertEqual(projection["feature_instance"], "binance-dev")
        self.assertEqual(projection["log_identity"], "binance-dev.log")
        self.assertEqual(projection["lock_identity"], "binance-dev.lock")
        self.assertEqual(projection["stop_identity"], "binance-dev.stop")
        self.assertEqual(projection["pid_identity"], "binance-dev.pid")
        self.assertEqual(projection["background_identity"], "binance-dev")
        with self.assertRaises((AttributeError, TypeError)):
            p.port = 8080


    def test_explicit_testnet_profile_has_distinct_resources(self):
        p = BinanceRuntimeProfile.testnet(self.tmp_path)
        self.assertEqual(p.environment, BinanceSpotEnvironment.BINANCE_SPOT_TESTNET)
        self.assertEqual(p.port, 8082)
        self.assertEqual(
            p.db_path,
            os.path.normcase(
                os.path.abspath(
                    os.path.normpath(
                        str(self.tmp_path / "runtime-data" / "binance-testnet.sqlite")
                    )
                )
            ),
        )
        self.assertEqual(p.feature_instance, "binance-testnet")
        self.assertEqual(p.runtime_identity, "binance-testnet")
        self.assertEqual(p.background_identity, "binance-testnet")
        self.assertEqual(p.log_identity, "binance-testnet.log")
        self.assertEqual(p.lock_identity, "binance-testnet.lock")
        self.assertEqual(p.stop_identity, "binance-testnet.stop")
        self.assertEqual(p.pid_identity, "binance-testnet.pid")

    def test_profile_rejects_shared_axiom_db_wrong_names_and_roots(self):
        runtime = self.tmp_path / "runtime-data"
        runtime.mkdir()
        axiom_db = runtime / "axiom.sqlite"
        wrong_root = self.tmp_path / "other-runtime-data"
        wrong_root.mkdir()

        invalid_profiles = (
            lambda: BinanceRuntimeProfile.paper(self.tmp_path, axiom_db),
            lambda: BinanceRuntimeProfile.testnet(self.tmp_path, axiom_db),
            lambda: BinanceRuntimeProfile.paper(self.tmp_path, runtime / "binance-testnet.sqlite"),
            lambda: BinanceRuntimeProfile.testnet(self.tmp_path, runtime / "binance-dev.sqlite"),
            lambda: BinanceRuntimeProfile.paper(self.tmp_path, wrong_root / "binance-dev.sqlite"),
            lambda: BinanceRuntimeProfile.testnet(self.tmp_path, wrong_root / "binance-testnet.sqlite"),
        )
        for build_profile in invalid_profiles:
            with self.assertRaises(BinanceSpotConfigurationError):
                build_profile()

    def test_profile_rejects_dedicated_db_symlink_to_axiom_db(self):
        runtime = self.tmp_path / "runtime-data"
        runtime.mkdir()
        axiom_db = runtime / "axiom.sqlite"
        axiom_db.touch()
        for db_name, build_profile in (
            (
                "binance-dev.sqlite",
                lambda path: BinanceRuntimeProfile.paper(self.tmp_path, path),
            ),
            (
                "binance-testnet.sqlite",
                lambda path: BinanceRuntimeProfile.testnet(self.tmp_path, path),
            ),
        ):
            dedicated_db = runtime / db_name
            try:
                dedicated_db.symlink_to(axiom_db)
            except (NotImplementedError, OSError) as exc:
                self.skipTest(f"filesystem symlinks unavailable: {exc}")
            try:
                with self.assertRaises(BinanceSpotConfigurationError):
                    build_profile(dedicated_db)
            finally:
                dedicated_db.unlink(missing_ok=True)
    def test_profile_rejects_dedicated_db_hardlink_to_axiom_db(self):
        runtime = self.tmp_path / "runtime-data"
        runtime.mkdir()
        axiom_db = runtime / "axiom.sqlite"
        axiom_db.touch()
        for db_name, build_profile in (
            (
                "binance-dev.sqlite",
                lambda path: BinanceRuntimeProfile.paper(self.tmp_path, path),
            ),
            (
                "binance-testnet.sqlite",
                lambda path: BinanceRuntimeProfile.testnet(self.tmp_path, path),
            ),
        ):
            dedicated_db = runtime / db_name
            try:
                os.link(axiom_db, dedicated_db)
            except (NotImplementedError, OSError) as exc:
                self.skipTest(f"filesystem hardlinks unavailable: {exc}")
            try:
                with self.assertRaises(BinanceSpotConfigurationError):
                    build_profile(dedicated_db)
            finally:
                dedicated_db.unlink(missing_ok=True)



    def test_profile_rejects_runtime_data_directory_symlink(self):
        real_runtime = self.tmp_path / "real-runtime-data"
        real_runtime.mkdir()
        (real_runtime / "binance-dev.sqlite").touch()
        runtime = self.tmp_path / "runtime-data"
        try:
            runtime.symlink_to(real_runtime, target_is_directory=True)
        except (NotImplementedError, OSError) as exc:
            self.skipTest(f"filesystem symlinks unavailable: {exc}")
        try:
            with self.assertRaises(BinanceSpotConfigurationError):
                BinanceRuntimeProfile.paper(self.tmp_path, runtime / "binance-dev.sqlite")
        finally:
            runtime.unlink(missing_ok=True)

    def test_profile_rejects_dot_and_dotdot_db_components(self):
        runtime = self.tmp_path / "runtime-data"
        runtime.mkdir()
        for db_path in (
            str(runtime) + os.sep + "." + os.sep + "binance-dev.sqlite",
            str(runtime) + os.sep + ".." + os.sep + "binance-dev.sqlite",
        ):
            with self.assertRaises(BinanceSpotConfigurationError):
                BinanceRuntimeProfile.paper(self.tmp_path, db_path)
    def test_profile_rejects_wrong_db_alias_port_host_and_transport(self):
        p = profile(self.tmp_path)
        with self.assertRaises(BinanceSpotConfigurationError):
            BinanceRuntimeProfile.development(
                self.tmp_path,
                self.tmp_path / "runtime-data" / "axiom.sqlite",
            )
        with self.assertRaises(BinanceSpotConfigurationError):
            BinanceRuntimeProfile.development(self.tmp_path, p.db_path, port=8080)
        with self.assertRaises(BinanceSpotConfigurationError):
            BinanceRuntimeProfile.development(self.tmp_path, p.db_path, host="localhost")
        with self.assertRaises(BinanceSpotConfigurationError):
            BinanceRuntimeProfile.development(self.tmp_path, p.db_path, transport="hermes")
        with self.assertRaises(BinanceSpotConfigurationError):
            BinanceRuntimeProfile.development(self.tmp_path, p.db_path, transport="polymarket")
        with self.assertRaises(BinanceSpotConfigurationError):
            BinanceRuntimeProfile.development(
                self.tmp_path,
                p.db_path,
                environment=BINANCE_SPOT_LIVE,
            )

    def test_credential_store_namespace_environment_isolation_and_safe_projection(self):
        keyring = FakeKeyring()
        testnet = BinanceCredentialRef("binance-testnet", BINANCE_SPOT_TESTNET)
        store = BinanceCredentialStore(ref=testnet, keyring_backend=keyring)
        store.configure("public-key", "private-secret")
        self.assertEqual(
            store.load(),
            BinanceSpotCredentials("public-key", "private-secret"),
        )
        self.assertEqual(store.namespace, "AXIOM-BINANCE-SPOT-TESTNET")
        self.assertIn(
            ("AXIOM-BINANCE-SPOT-TESTNET", "binance-testnet:BINANCE_SPOT_TESTNET:api_key"),
            keyring.values,
        )
        self.assertTrue(
            all("private-secret" not in str(value) for value in store.safe_projection().values())
        )
        self.assertTrue(
            all("public-key" not in str(value) for value in store.safe_projection().values())
        )
        with self.assertRaises(BinanceSpotEnvironmentMismatch):
            BinanceCredentialRef("binance-dev", BINANCE_SPOT_TESTNET)
        with self.assertRaises(BinanceSpotEnvironmentMismatch):
            BinanceCredentialRef(
                "binance-testnet",
                BINANCE_SPOT_TESTNET,
                namespace="AXIOM-BINANCE-SPOT",
            )
        with self.assertRaises(BinanceSpotConfigurationError):
            BinanceCredentialRef("binance-testnet", BINANCE_SPOT_LIVE)
        with self.assertRaises(BinanceSpotEnvironmentMismatch):
            store.load(BinanceCredentialRef("binance-dev", PAPER))
        with self.assertRaises(BinanceSpotConfigurationError):
            BinanceCredentialStore(
                ref=testnet,
                keyring_backend=keyring,
                allow_environment=True,
            )
    def test_credential_ref_stable_id_is_deterministic_separated_and_opaque(self):
        testnet = BinanceCredentialRef("binance-testnet", BINANCE_SPOT_TESTNET)
        stable_id = testnet.stable_id()
        self.assertEqual(
            stable_id,
            BinanceCredentialRef("binance-testnet", BINANCE_SPOT_TESTNET).stable_id(),
        )
        self.assertEqual(len(stable_id), 64)
        self.assertTrue(all(character in "0123456789abcdef" for character in stable_id))
        self.assertNotEqual(
            stable_id,
            BinanceCredentialRef("binance-dev", PAPER).stable_id(),
        )

        store = BinanceCredentialStore(ref=testnet, keyring_backend=FakeKeyring())
        store.configure("public-key", "private-secret")
        self.assertEqual(testnet.stable_id(), stable_id)
        projection = json.dumps(store.safe_projection(), sort_keys=True)
        self.assertNotIn("public-key", projection)
        self.assertNotIn("private-secret", projection)


    def test_public_testnet_methods_are_unsigned_and_fixed_origin(self):
        seen = []

        def opener(request, timeout):
            seen.append(request)
            return Response({"ok": True})

        client = BinanceSpotRESTClient(BINANCE_SPOT_TESTNET, opener=opener)
        client.time()
        client.exchange_info(symbol="BTCUSDT")
        client.ticker_price(symbol="BTCUSDT")
        client.depth(symbol="BTCUSDT", limit=5)
        client.klines(symbol="BTCUSDT", interval="1m", limit=2)
        self.assertEqual(len(seen), 5)
        for request in seen:
            self.assertTrue(
                request.full_url.startswith("https://testnet.binance.vision/api/v3/")
            )
            self.assertNotIn("signature=", request.full_url)
            self.assertNotIn("X-mbx-apikey", request.headers)
            self.assertIsNone(request.data)
        client.base_url = "https://api.binance.com"
        with self.assertRaises(BinanceSpotConfigurationError):
            client.time()
        with self.assertRaises(BinanceSpotConfigurationError):
            BinanceSpotRESTClient(
                BINANCE_SPOT_LIVE,
                {"api_key": "k", "api_secret": "s"},
                opener=opener,
            )

    def test_exact_signed_query_and_header(self):
        seen = []

        def opener(request, timeout):
            seen.append(request)
            return Response({"account": True})

        client = BinanceSpotRESTClient(
            BinanceSpotEnvironment.BINANCE_SPOT_TESTNET,
            BinanceSpotCredentials("public-key", "private-secret"),
            opener=opener,
            clock=lambda: 1_700_000_000,
        )
        result = client.account()
        self.assertEqual(result.status, "OK")
        request = seen[0]
        query = urlsplit(request.full_url).query
        unsigned = query.split("&signature=", 1)[0].encode()
        expected = hmac.new(b"private-secret", unsigned, hashlib.sha256).hexdigest()
        self.assertEqual(parse_qs(query)["signature"], [expected])
        self.assertEqual(request.headers["X-mbx-apikey"], "public-key")
        self.assertIsNone(request.data)
        self.assertTrue(
            request.full_url.startswith("https://testnet.binance.vision/api/v3/account?")
        )

    def test_deadline_is_transport_only_and_bounds_opener_timeout(self):
        seen = []
        timeouts = []

        def opener(request, timeout):
            seen.append(request)
            timeouts.append(timeout)
            return Response({"status": "NEW"})

        client = BinanceSpotRESTClient(
            BINANCE_SPOT_TESTNET,
            {"api_key": "public-key", "api_secret": "private-secret"},
            opener=opener,
            clock=lambda: 1_700_000_000,
        )
        deadline = time.monotonic() + 0.25
        client.place_order(
            symbol="BTCUSDT",
            side="BUY",
            quantity="1",
            price="100",
            deadline_monotonic=deadline,
        )
        request = seen[0]
        encoded = request.data.decode() if request.data else urlsplit(request.full_url).query
        self.assertNotIn("deadline_monotonic", encoded)
        self.assertLessEqual(timeouts[0], 0.25)

    def test_signed_timestamp_uses_bounded_server_offset_and_reserved_fields_fail_closed(self):
        seen = []

        def opener(request, timeout):
            seen.append(request)
            return Response({})

        client = BinanceSpotRESTClient(
            BINANCE_SPOT_TESTNET,
            {"api_key": "public-key", "api_secret": "private-secret"},
            opener=opener,
            clock=lambda: 1_700_000_000,
        )
        client.set_server_time_offset_ms(8_000)
        client.test_order(
            symbol="BTCUSDT",
            side="BUY",
            quantity="1",
            price="100",
        )
        body = parse_qs(seen[0].data.decode())
        self.assertEqual(body["timestamp"], ["1700000008000"])
        self.assertEqual(body["recvWindow"], ["5000"])
        with self.assertRaises(BinanceSpotConfigurationError):
            client.account(timestamp=1)
        with self.assertRaises(BinanceSpotConfigurationError):
            client.account(recvWindow=50_000)

    def test_signed_redirect_is_rejected_before_any_follow_up(self):
        calls = []

        def opener(request, timeout):
            calls.append(request)
            return Response({}, status=302, headers={"Location": "https://evil.example/account"})

        client = BinanceSpotRESTClient(
            BINANCE_SPOT_TESTNET,
            {"api_key": "public-key", "api_secret": "private-secret"},
            opener=opener,
        )
        with self.assertRaises(BinanceSpotTransportError):
            client.account()
        self.assertEqual(len(calls), 1)

    def test_query_and_cancel_never_sign_python_client_order_id_alias(self):
        seen = []

        def opener(request, timeout):
            seen.append(request)
            return Response({"status": "CANCELED"})

        client = BinanceSpotRESTClient(
            BINANCE_SPOT_TESTNET,
            {"api_key": "k", "api_secret": "s"},
            opener=opener,
            clock=lambda: 1,
        )
        client.query_order(
            symbol="BTCUSDT",
            order_id="42",
            client_order_id="python-only",
        )
        client.my_trades(
            symbol="BTCUSDT",
            order_id="42",
            client_order_id="python-only",
        )
        client.cancel_owned_order(
            symbol="BTCUSDT",
            order_id="42",
            client_order_id="python-only",
        )
        for request in seen:
            body = request.full_url
            if request.data:
                body += "?" + request.data.decode()
            self.assertNotIn("client_order_id", body)
            self.assertNotIn("new_client_order_id", body)
            self.assertNotIn("newClientOrderId", body)
        self.assertEqual(len(seen), 3)
        my_trades_query = urlsplit(seen[1].full_url).query
        self.assertEqual(parse_qs(my_trades_query)["orderId"], ["42"])
        self.assertNotIn("order_id", my_trades_query)


    def test_endpoint_allowlist_orders_and_validation_only_label(self):
        calls = []

        def opener(request, timeout):
            calls.append(request)
            return Response({})

        client = BinanceSpotRESTClient(
            BINANCE_SPOT_TESTNET,
            {"api_key": "k", "api_secret": "s"},
            opener=opener,
            clock=lambda: 1,
        )
        result = client.test_order(
            symbol="BTCUSDT",
            side="BUY",
            quantity="1",
            price="100",
            time_in_force="IOC",
        )
        self.assertTrue(result.validation_only)
        self.assertEqual(result.label, "VALIDATION_ONLY")
        self.assertIn("/api/v3/order/test", calls[0].full_url)
        with self.assertRaises(BinanceSpotConfigurationError):
            client.place_order(
                symbol="BTCUSDT",
                side="BUY",
                quantity="1",
                price="100",
                time_in_force="GTC",
            )

    def test_unknown_rate_limit_reject_and_paper(self):
        responses = [
            Response({"code": -1007, "msg": "timeout"}, status=400),
            Response({"code": -1}, status=429, headers={"Retry-After": "3"}),
            Response({"code": -2010, "msg": "reject"}, status=400),
        ]

        def opener(request, timeout):
            return responses.pop(0)

        client = BinanceSpotRESTClient(
            BINANCE_SPOT_TESTNET,
            {"api_key": "k", "api_secret": "s"},
            opener=opener,
            clock=lambda: 1,
        )
        self.assertEqual(client.account().status, "UNKNOWN")
        self.assertEqual(client.account().retry_after, "3")
        self.assertEqual(client.account().status, "REJECTED")
        paper = BinanceSpotRESTClient(
            BinanceSpotEnvironment.PAPER,
            BinanceSpotCredentials("k", "s"),
            opener=opener,
        )
        with self.assertRaises(BinanceSpotConfigurationError):
            paper.account()


if __name__ == "__main__":
    unittest.main()
