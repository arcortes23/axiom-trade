from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from axiom.binance_dev import BinanceDevelopmentRuntime
from axiom.binance_spot import BinanceRuntimeProfile
from axiom.storage import AxiomStore


class _Worker:
    def run(self, *, max_cycles=None):
        return []

    def stop(self):
        return None

    def status(self):
        return {"status": "IDLE"}


class _Server:
    def __init__(self):
        self.started = False
        self.stopped = False

    def start(self):
        self.started = True

    def stop(self):
        self.stopped = True


class BinanceResourceOwnershipRegressionTests(unittest.TestCase):
    @staticmethod
    def _runtime(root: Path) -> BinanceDevelopmentRuntime:
        profile = BinanceRuntimeProfile.paper(root)
        return BinanceDevelopmentRuntime(
            root,
            profile=profile,
            worker=_Worker(),
            dashboard_server=_Server(),
            # Keep this regression filesystem-only: runtime markers use the
            # temporary checkout while the required service state is in memory.
            store_factory=lambda _: AxiomStore(":memory:"),
        )

    def test_losing_paper_starter_cannot_clear_active_stop_request(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            owner = self._runtime(root)
            contender = self._runtime(root)
            try:
                owner.start(once=True)
                stop_request = json.loads(contender._owner_document())
                Path(owner.stop_path).write_text(
                    json.dumps(stop_request, sort_keys=True),
                    encoding="utf-8",
                )
                before = Path(owner.stop_path).read_bytes()

                with self.assertRaises(FileExistsError):
                    contender.start(once=True)

                self.assertEqual(Path(owner.stop_path).read_bytes(), before)
                self.assertTrue(contender._stop_marker_owned())
            finally:
                contender.stop()
                owner.stop()

    def test_release_preserves_replaced_lock_and_pid_markers(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime = self._runtime(root)
            try:
                runtime.start(once=True)
                replacement = json.dumps(
                    {
                        "pid": runtime.owner.pid,
                        "runtime_identity": runtime.runtime_identity,
                        "owner_token": "replacement-owner-token",
                    },
                    sort_keys=True,
                ).encode("utf-8")
                Path(runtime.lock_path).write_bytes(replacement)
                Path(runtime.pid_path).write_bytes(replacement)

                runtime.stop()

                self.assertEqual(Path(runtime.lock_path).read_bytes(), replacement)
                self.assertEqual(Path(runtime.pid_path).read_bytes(), replacement)
            finally:
                runtime.stop()

    def test_release_hands_pid_off_before_lock_to_peer(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            owner = self._runtime(root)
            peer = self._runtime(root)
            peer_acquired = False
            original_release = owner._release_owned_path

            def release_owned(path):
                nonlocal peer_acquired
                if path == owner.pid_path:
                    self.assertTrue(Path(owner.lock_path).exists())
                    with self.assertRaises(FileExistsError):
                        peer._acquire()
                result = original_release(path)
                if path == owner.pid_path:
                    self.assertFalse(Path(owner.pid_path).exists())
                    self.assertTrue(Path(owner.lock_path).exists())
                elif path == owner.lock_path:
                    self.assertFalse(Path(owner.lock_path).exists())
                    peer._acquire()
                    peer_acquired = True
                return result

            owner._release_owned_path = release_owned
            try:
                owner.start(once=True)
                owner.stop()
                self.assertTrue(peer_acquired)
                self.assertTrue(peer._owns_file(peer.pid_path))
                self.assertTrue(peer._owns_file(peer.lock_path))
            finally:
                peer.stop()
                owner.stop()

    def test_stop_marker_published_during_acquisition_survives_startup_cleanup(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime = self._runtime(root)
            original_acquire = runtime._acquire
            stop_request = runtime._owner_document().encode("utf-8")

            def acquire_and_publish_marker():
                original_acquire()
                Path(runtime.stop_path).write_bytes(stop_request)

            runtime._acquire = acquire_and_publish_marker
            try:
                runtime.start(once=True)
                self.assertEqual(Path(runtime.stop_path).read_bytes(), stop_request)
                self.assertTrue(runtime.stop_event.is_set())
            finally:
                runtime.stop()



if __name__ == "__main__":
    unittest.main()
