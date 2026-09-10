"""Minimal process-wide safety boundary for the release test suite.

The suite uses explicit fakes for credentials, SDKs, transports, and databases.
This guard keeps an accidentally copied flag or ambient workstation setting from
turning one of those tests into a real-resource operation.
"""
from __future__ import annotations

import ipaddress
import os
from pathlib import Path
import socket
import sqlite3
import sys
from typing import Any

import pytest


_RELEASE_ROOT = Path(__file__).resolve().parents[1]
_PROTECTED_ROOT = _RELEASE_ROOT.parent / "axiom"
_CREDENTIAL_ENV_NAMES = frozenset(
    {
        "POLYMARKET_PRIVATE_KEY",
        "POLYMARKET_WALLET_ADDRESS",
        "POLYMARKET_RELAYER_API_KEY",
        "POLYMARKET_RELAYER_API_KEY_ADDRESS",
        "POLYMARKET_CLOB_API_KEY",
        "POLYMARKET_CLOB_API_SECRET",
        "POLYMARKET_CLOB_API_PASSPHRASE",
        "POLYMARKET_API_KEY",
        "POLYMARKET_API_SECRET",
        "POLYMARKET_PASSPHRASE",
        "BINANCE_API_KEY",
        "BINANCE_API_SECRET",
        "BINANCE_ACCESS_KEY",
        "BINANCE_SECRET_KEY",
    }
)


def _loopback_host(value: object) -> bool:
    text = str(value or "").strip().strip("[]").lower()
    if text in {"localhost", "localhost.localdomain"}:
        return True
    try:
        return ipaddress.ip_address(text).is_loopback
    except ValueError:
        return False


def _guarded_address(address: object) -> None:
    if isinstance(address, tuple) and len(address) >= 2:
        host, port = address[0], address[1]
        try:
            port_number = int(port)
        except (TypeError, ValueError):
            port_number = None
        if port_number == 8080:
            raise PermissionError("release tests cannot connect to protected port 8080")
        if host is not None and not _loopback_host(host):
            raise PermissionError("release tests cannot connect to non-loopback network")
        return
    if isinstance(address, str) and address:
        # AF_UNIX paths are local IPC rather than network transport.
        return


class _GuardedSocket(socket.socket):
    def connect(self, address: object):
        _guarded_address(address)
        return super().connect(address)

    def connect_ex(self, address: object):
        _guarded_address(address)
        return super().connect_ex(address)

    def bind(self, address: object):
        _guarded_address(address)
        return super().bind(address)






def _guard_create_connection(original: Any):
    def create_connection(address: object, *args: object, **kwargs: object):
        _guarded_address(address)
        return original(address, *args, **kwargs)

    return create_connection


def _guard_getaddrinfo(original: Any):
    def getaddrinfo(host: object, port: object, *args: object, **kwargs: object):
        if port is not None:
            try:
                if int(port) == 8080:
                    raise PermissionError("release tests cannot resolve protected port 8080")
            except (TypeError, ValueError):
                pass
        if host is not None and not _loopback_host(host):
            raise PermissionError("release tests cannot resolve non-loopback network")
        return original(host, port, *args, **kwargs)

    return getaddrinfo


def _protected_database(path: object) -> bool:
    if isinstance(path, bytes):
        path = os.fsdecode(path)
    text = str(path or "").strip()
    if not text or text == ":memory:":
        return False
    if text.startswith("file:"):
        text = text[5:].split("?", 1)[0]
        if text.startswith("/") and len(text) > 2 and text[2] == ":":
            text = text[1:]
    try:
        candidate = Path(text).expanduser().resolve(strict=False)
        protected = _PROTECTED_ROOT.resolve(strict=False)
        return candidate == protected or protected in candidate.parents
    except (OSError, ValueError, RuntimeError):
        return False


def _guard_sqlite_connect(original: Any):
    def connect(database: object, *args: object, **kwargs: object):
        if _protected_database(database):
            raise PermissionError("release tests cannot open the protected live database")
        return original(database, *args, **kwargs)

    return connect

def _shield_cached_keyring() -> None:
    keyring = sys.modules.get("keyring")
    if keyring is None:
        return

    def no_password(*_: object, **__: object) -> None:
        return None

    def deny_persistence(*_: object, **__: object) -> None:
        raise RuntimeError("release tests use a null keyring unless a fake backend is injected")

    for name, implementation in (
        ("get_password", no_password),
        ("set_password", deny_persistence),
        ("delete_password", deny_persistence),
    ):
        if hasattr(keyring, name):
            setattr(keyring, name, implementation)



def _scrub_ambient_safety_state() -> None:
    os.environ["AXIOM_EXECUTION_PROFILE"] = "isolated"
    os.environ["PYTHON_KEYRING_BACKEND"] = "keyring.backends.null.Keyring"
    for name in list(os.environ):
        lowered = name.lower()
        if name in _CREDENTIAL_ENV_NAMES or (
            lowered.startswith(("polymarket_", "binance_"))
            and any(token in lowered for token in ("key", "secret", "token", "password", "credential"))
        ):
            os.environ.pop(name, None)
def _enforce_ambient_safety_state() -> None:
    _scrub_ambient_safety_state()
    _shield_cached_keyring()



_enforce_ambient_safety_state()
socket.socket = _GuardedSocket
socket.create_connection = _guard_create_connection(socket.create_connection)
socket.getaddrinfo = _guard_getaddrinfo(socket.getaddrinfo)
sqlite3.connect = _guard_sqlite_connect(sqlite3.connect)


@pytest.fixture(autouse=True)
def release_safety_guard(request: pytest.FixtureRequest) -> None:
    """Keep isolation and transport guards active for collection and each test."""
    _enforce_ambient_safety_state()
    request.addfinalizer(_enforce_ambient_safety_state)
