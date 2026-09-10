from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from axiom.bootstrap import BTC_HISTORY_START
from axiom.operator import BOOTSTRAP_JOB_NAME, OperatorControlPlane
from axiom.storage import AxiomStore


class FakeBootstrapper:
    def __init__(self, *, provider: object, entered: threading.Event, release: threading.Event) -> None:
        self.provider = provider
        self.entered = entered
        self.release = release
        self.calls: list[tuple[object, datetime, bool, int]] = []

    def bootstrap_crypto_universe(
        self,
        snapshot: object,
        *,
        start: datetime,
        resume: bool,
        max_symbols: int,
    ) -> tuple[SimpleNamespace, ...]:
        self.calls.append((snapshot, start, resume, max_symbols))
        self.entered.set()
        self.release.wait(timeout=2)
        return (SimpleNamespace(status="COMPLETE"),)


class OperatorBootstrapRegressionTests(unittest.TestCase):
    def test_bootstrap_worker_resolves_imports_and_persists_result(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            db_path = str(Path(tempdir) / "operator.sqlite")
            snapshot = SimpleNamespace(selected_symbols=("BTCUSDT",))
            provider = object()
            entered = threading.Event()
            release = threading.Event()
            fake = FakeBootstrapper(provider=provider, entered=entered, release=release)
            with AxiomStore(db_path) as store:
                control = OperatorControlPlane(store)
                with patch("axiom.operator.load_crypto_universe", return_value=snapshot), patch(
                    "axiom.operator.BinanceAdapter", return_value=provider
                ), patch("axiom.operator.HistoricalBootstrapper", return_value=fake):
                    started = control.start_bootstrap(resume=False)
                    self.assertTrue(entered.wait(timeout=1))
                    self.assertEqual(started["status"], "RUNNING")
                    release.set()
                    deadline = time.monotonic() + 2.0
                    persisted = None
                    while time.monotonic() < deadline:
                        persisted = store.get_operator_job(BOOTSTRAP_JOB_NAME)
                        if persisted is not None and persisted["status"] != "RUNNING":
                            break
                        time.sleep(0.01)
                    self.assertIsNotNone(persisted)
                    assert persisted is not None
                    self.assertEqual(persisted["status"], "COMPLETE")

            with AxiomStore(db_path) as reopened:
                result = reopened.get_operator_job(BOOTSTRAP_JOB_NAME)

            self.assertIsNotNone(result)
            assert result is not None
            self.assertEqual(result["status"], "COMPLETE")
            self.assertIsNone(result["last_error"])
            self.assertFalse(result["resumable"])
            self.assertEqual(
                fake.calls,
                [(snapshot, BTC_HISTORY_START, False, 50)],
            )


if __name__ == "__main__":
    unittest.main()
