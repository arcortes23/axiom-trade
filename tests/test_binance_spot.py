from __future__ import annotations

import hashlib
import hmac
import json
import os
from pathlib import Path
import tempfile
import unittest
from urllib.parse import parse_qs, urlsplit


from axiom.binance_spot import (
    BINANCE_SPOT_LIVE,
    BINANCE_SPOT_TESTNET,
    BinanceCredentialRef,
    BinanceCredentialStore,
    BinanceRuntimeProfile,
    BinanceSpotConfigurationError,
    BinanceSpotCredentials,
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

    def test_development_profile_is_immutable_and_canonical(self):
        p = profile(self.tmp_path)
        self.assertEqual(p.host, "127.0.0.1")
        self.assertEqual(p.port, 8081)
        self.assertEqual(
            p.db_path,
            os.path.normcase(
                os.path.realpath(str(self.tmp_path / "runtime-data" / "binance-dev.sqlite"))
            ),
        )
        projection = p.projection()
        self.assertEqual(projection["feature_instance"], "binance-dev")
        self.assertEqual(projection["log_identity"], "binance-dev.log")
        self.assertEqual(projection["lock_identity"], "binance-dev.lock")
        self.assertEqual(projection["stop_identity"], "binance-dev.stop")
        self.assertEqual(projection["pid_identity"], "binance-dev.pid")
        self.assertEqual(projection["background_identity"], "binance-dev")
        with self.assertRaises((AttributeError, TypeError)):
            p.port = 8080

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
        testnet = BinanceCredentialRef("binance-dev", BINANCE_SPOT_TESTNET)
        store = BinanceCredentialStore(ref=testnet, keyring_backend=keyring)
        store.configure("public-key", "private-secret")
        self.assertEqual(
            store.load(),
            BinanceSpotCredentials("public-key", "private-secret"),
        )
        self.assertEqual(store.namespace, "AXIOM-BINANCE-SPOT")
        self.assertTrue(
            all("private-secret" not in str(value) for value in store.safe_projection().values())
        )
        self.assertTrue(
            all("public-key" not in str(value) for value in store.safe_projection().values())
        )
        live = BinanceCredentialRef("binance-dev", BINANCE_SPOT_LIVE)
        self.assertIsNone(BinanceCredentialStore(ref=live, keyring_backend=keyring).load())
        with self.assertRaises(BinanceSpotEnvironmentMismatch):
            store.load(live)
        with self.assertRaises(BinanceSpotConfigurationError):
            BinanceCredentialStore(
                ref=testnet,
                keyring_backend=keyring,
                allow_environment=True,
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
