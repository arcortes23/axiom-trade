"""Isolated Binance Spot canary contracts.

This module deliberately does not use the read-only :mod:`axiom.data.binance`
adapter.  The client below is a small, explicit, authenticated Spot REST
transport with fixed environments and a narrow endpoint surface.  Credentials
are supplied by the caller (or loaded from the isolated keyring store); no
ambient environment variables are consulted.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import math
import os
import socket
import threading
import time
from collections.abc import Mapping, MutableMapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
import stat
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener


class BinanceSpotEnvironment(str, Enum):
    """Supported Spot execution environments.

    ``PAPER`` intentionally has no REST origin and cannot perform an
    authenticated request.  The remaining origins are constants rather than
    configurable values, preventing an accidental live/testnet swap.
    """

    PAPER = "PAPER"
    BINANCE_SPOT_TESTNET = "BINANCE_SPOT_TESTNET"
    BINANCE_SPOT_LIVE = "BINANCE_SPOT_LIVE"
    # Short aliases are useful to callers while preserving the explicit public
    # names and values above.  They are aliases, not additional environments.
    TESTNET = "BINANCE_SPOT_TESTNET"
    LIVE = "BINANCE_SPOT_LIVE"


PAPER = BinanceSpotEnvironment.PAPER.value
BINANCE_SPOT_TESTNET = BinanceSpotEnvironment.BINANCE_SPOT_TESTNET.value
BINANCE_SPOT_LIVE = BinanceSpotEnvironment.BINANCE_SPOT_LIVE.value
QUOTE_ASSET = "USDT"
SPOT_REST_ORIGINS: Mapping[BinanceSpotEnvironment, str | None] = {
    BinanceSpotEnvironment.PAPER: None,
    BinanceSpotEnvironment.BINANCE_SPOT_TESTNET: "https://testnet.binance.vision",
    BinanceSpotEnvironment.BINANCE_SPOT_LIVE: "https://api.binance.com",
}
_PROFILE_SPECS: Mapping[BinanceSpotEnvironment, Mapping[str, Any]] = {
    BinanceSpotEnvironment.PAPER: {
        "db_name": "binance-dev.sqlite",
        "port": 8081,
        "identity": "binance-dev",
    },
    BinanceSpotEnvironment.BINANCE_SPOT_TESTNET: {
        "db_name": "binance-testnet.sqlite",
        "port": 8082,
        "identity": "binance-testnet",
    },
}
BINANCE_API_BASELINE_COMMIT = "041bba2d8a0bb8d26f77b88a0e2761743233fcf7"
BINANCE_API_BASELINE_DATE = "2026-09-02"
BINANCE_TESTNET_API_BASELINE_DATE = "2026-09-04"
# Short names are retained for consumers that only need the recorded baseline.
API_BASELINE_COMMIT = BINANCE_API_BASELINE_COMMIT
API_BASELINE_DATE = BINANCE_API_BASELINE_DATE
TESTNET_API_BASELINE_DATE = BINANCE_TESTNET_API_BASELINE_DATE
BINANCE_SPOT_REQUIRED_PERMISSIONS = frozenset({"USER_DATA", "TRADE"})
BINANCE_SPOT_PROHIBITED_PERMISSIONS = frozenset({"WITHDRAWAL", "TRANSFER"})
_MAX_SERVER_TIME_OFFSET_MS = 120_000
_DEADLINE_REQUEST_CEILING_SECONDS = 1.0


class _RejectRedirectHandler(HTTPRedirectHandler):
    """Reject redirects before urllib can replay an authenticated request."""

    def redirect_request(self, req: Request, fp: Any, code: int, msg: str, headers: Any, newurl: str) -> Request | None:
        raise BinanceSpotTransportError("Binance Spot transport rejected redirect")


class BinanceSpotConfigurationError(ValueError):
    """A profile, environment, or credential boundary was violated."""


class BinanceSpotEnvironmentMismatch(BinanceSpotConfigurationError):
    """A credential/profile was used for a different environment."""


class BinanceSpotTransportError(RuntimeError):
    """Raised only for malformed local transport setup, never remote rejects."""


class BinanceSpotStatus(str, Enum):
    OK = "OK"
    REJECTED = "REJECTED"
    UNKNOWN = "UNKNOWN"
    RATE_LIMIT = "RATE_LIMIT"


def _path_text(value: str | os.PathLike[str]) -> str:
    raw = os.fspath(value)
    return os.fsdecode(raw) if isinstance(raw, bytes) else raw


def _lexical_path(value: str | os.PathLike[str]) -> str:
    """Return an absolute path without following filesystem aliases."""

    return os.path.normcase(os.path.abspath(os.path.normpath(_path_text(value))))


def _has_dot_component(value: str | os.PathLike[str]) -> bool:
    """Return whether a path contains an explicit ``.`` or ``..`` segment."""

    text = _path_text(value)
    separators = os.sep
    if os.path.altsep:
        separators += os.path.altsep
    components = [text]
    for separator in separators:
        components = [part for component in components for part in component.split(separator)]
    return any(component in {".", ".."} for component in components)


def _path_components(value: str | os.PathLike[str]) -> tuple[str, ...]:
    """Return absolute path components, retaining the filesystem root."""

    path = _lexical_path(value)
    drive, tail = os.path.splitdrive(path)
    if tail.startswith((os.sep, os.path.altsep or os.sep)):
        current = drive + os.sep
    else:
        current = drive
    components: list[str] = []
    for component in tail.replace(os.path.altsep or "\0", os.sep).split(os.sep):
        if not component:
            continue
        current = os.path.join(current, component)
        components.append(current)
    if not components and current:
        components.append(current)
    return tuple(components)


def _is_reparse_or_symlink(path: str) -> bool:
    try:
        info = os.lstat(path)
    except FileNotFoundError:
        return False
    except OSError as exc:
        raise BinanceSpotConfigurationError(
            "cannot inspect a Binance profile database path component"
        ) from exc
    reparse_point = bool(getattr(info, "st_file_attributes", 0) & 0x0400)
    return stat.S_ISLNK(info.st_mode) or reparse_point


def _reject_database_aliases(path: str) -> None:
    for component in _path_components(path):
        if _is_reparse_or_symlink(component):
            raise BinanceSpotConfigurationError(
                "Binance profile database paths cannot contain symlinks, junctions, or reparse points"
            )
    try:
        info = os.stat(path)
    except FileNotFoundError:
        return
    except OSError as exc:
        raise BinanceSpotConfigurationError(
            "cannot inspect a Binance profile database path"
        ) from exc
    if int(getattr(info, "st_nlink", 1)) > 1:
        raise BinanceSpotConfigurationError(
            "Binance profile database paths cannot be hard-linked or multiply-linked"
        )


def _canonical_path(value: str | os.PathLike[str]) -> str:
    """Return one comparison form for Windows paths and existing aliases."""

    # resolve follows junctions/symlinks; realpath covers platform aliases and
    # normcase makes the final comparison case-insensitive on Windows.
    resolved = Path(os.path.abspath(os.fspath(value))).resolve(strict=False)
    return os.path.normcase(os.path.realpath(os.fspath(resolved)))


def canonical_path(value: str | os.PathLike[str]) -> str:
    """Public path canonicalizer used by profile validation and callers."""

    return _canonical_path(value)

def canonical_json(value: Any) -> str:
    """Stable JSON representation used for identifiers and hashes."""

    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def credential_fingerprint(value: Any) -> str:
    """Return the authorization fingerprint without exposing credential values."""

    if isinstance(value, BinanceSpotCredentials):
        api_key, api_secret = value.api_key, value.api_secret
    elif isinstance(value, Mapping):
        api_key = value.get("api_key")
        api_secret = value.get("api_secret")
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)) and len(value) == 2:
        api_key, api_secret = value
    else:
        api_key = getattr(value, "api_key", None) if value is not None else None
        api_secret = getattr(value, "api_secret", None) if value is not None else None
    if not api_key or not api_secret:
        return canonical_sha256({"configured": False})
    return canonical_sha256({"api_key": str(api_key), "api_secret": str(api_secret)})


def _environment(value: BinanceSpotEnvironment | str) -> BinanceSpotEnvironment:
    if isinstance(value, BinanceSpotEnvironment):
        return value
    text = str(value).strip()
    try:
        return BinanceSpotEnvironment(text)
    except ValueError as exc:
        # Also accept a member name (e.g. ``"TESTNET"``) without accepting
        # arbitrary host/environment strings.
        try:
            return BinanceSpotEnvironment[text]
        except (KeyError, TypeError):
            raise BinanceSpotConfigurationError(f"unsupported Binance Spot environment: {value!r}") from exc


def validate_spot_venue_identity(
    venue: Any,
    environment: BinanceSpotEnvironment | str,
    *,
    credential_hash: str | None = None,
) -> None:
    """Enforce the concrete venue and credential execution boundary."""

    expected_environment = _environment(environment)
    if expected_environment is BinanceSpotEnvironment.BINANCE_SPOT_LIVE:
        raise BinanceSpotConfigurationError(
            "development execution refuses Binance LIVE authenticated transport"
        )
    if venue is None:
        raise BinanceSpotEnvironmentMismatch(
            "an exact concrete Binance Spot venue is required"
        )
    if expected_environment is BinanceSpotEnvironment.PAPER:
        # The import is intentionally local: binance_dev depends on this
        # module, while this check must still identify the one trusted class.
        from .binance_dev import PaperBinanceSpotVenue

        if type(venue) is not PaperBinanceSpotVenue:
            raise BinanceSpotEnvironmentMismatch(
                "PAPER requires the built-in offline PaperBinanceSpotVenue"
            )
        return
    if type(venue) is not BinanceSpotRESTClient:
        raise BinanceSpotEnvironmentMismatch(
            "TESTNET requires the exact BinanceSpotRESTClient"
        )
    if venue.environment is not expected_environment:
        raise BinanceSpotEnvironmentMismatch(
            "venue environment does not match execution profile"
        )
    expected_origin = SPOT_REST_ORIGINS[expected_environment]
    if venue.origin != expected_origin:
        raise BinanceSpotEnvironmentMismatch(
            "venue origin does not match execution profile"
        )
    if credential_hash is not None:
        if not hmac.compare_digest(credential_fingerprint(venue.credentials), credential_hash):
            raise BinanceSpotEnvironmentMismatch(
                "venue credentials do not match the authorization fingerprint"
            )

@dataclass(frozen=True, slots=True)
class BinanceRuntimeProfile:
    """Immutable PAPER or TESTNET runtime boundary for Binance Spot."""

    worktree_root: str
    db_path: str
    host: str = "127.0.0.1"
    port: int | None = None
    environment: BinanceSpotEnvironment = BinanceSpotEnvironment.PAPER
    feature_instance: str = field(init=False)
    runtime_identity: str = field(init=False)
    log_identity: str = field(init=False)
    lock_identity: str = field(init=False)
    stop_identity: str = field(init=False)
    pid_identity: str = field(init=False)
    background_identity: str = field(init=False)
    transport: str = field(init=False)

    def __post_init__(self) -> None:
        root = _lexical_path(self.worktree_root)
        env = _environment(self.environment)
        if env is BinanceSpotEnvironment.BINANCE_SPOT_LIVE:
            raise BinanceSpotConfigurationError("Binance LIVE is not a development profile")
        spec = _PROFILE_SPECS[env]
        raw_db = _path_text(self.db_path)
        if _has_dot_component(raw_db):
            raise BinanceSpotConfigurationError(
                f"{env.value} profile database path cannot contain '.' or '..' components"
            )
        db = _lexical_path(raw_db)
        expected = _lexical_path(os.path.join(root, "runtime-data", spec["db_name"]))
        if db != expected:
            raise BinanceSpotConfigurationError(
                f"{env.value} profile requires runtime-data/{spec['db_name']}"
            )
        _reject_database_aliases(expected)
        if _canonical_path(db) != _canonical_path(expected):
            raise BinanceSpotConfigurationError(
                f"{env.value} profile database path resolves outside its dedicated database"
            )
        if str(self.host) != "127.0.0.1":
            raise BinanceSpotConfigurationError(
                f"{env.value} profile must bind loopback 127.0.0.1"
            )
        if self.port is None:
            port = int(spec["port"])
        else:
            try:
                port = int(self.port)
            except (TypeError, ValueError) as exc:
                raise BinanceSpotConfigurationError(
                    f"{env.value} profile port must be {spec['port']}"
                ) from exc
        if port != spec["port"]:
            raise BinanceSpotConfigurationError(
                f"{env.value} profile must use port {spec['port']}"
            )
        identity = str(spec["identity"])
        object.__setattr__(self, "worktree_root", root)
        object.__setattr__(self, "db_path", db)
        object.__setattr__(self, "port", port)
        object.__setattr__(self, "environment", env)
        object.__setattr__(self, "feature_instance", identity)
        object.__setattr__(self, "runtime_identity", identity)
        object.__setattr__(self, "log_identity", f"{identity}.log")
        object.__setattr__(self, "lock_identity", f"{identity}.lock")
        object.__setattr__(self, "stop_identity", f"{identity}.stop")
        object.__setattr__(self, "pid_identity", f"{identity}.pid")
        object.__setattr__(self, "background_identity", identity)
        object.__setattr__(self, "transport", "binance_spot")

    @classmethod
    def development(
        cls,
        worktree_root: str | os.PathLike[str],
        db_path: str | os.PathLike[str] | None = None,
        host: str | None = None,
        port: int | None = None,
        *,
        environment: BinanceSpotEnvironment | str | None = None,
        transport: str = "binance_spot",
    ) -> "BinanceRuntimeProfile":
        """Compatibility constructor selecting one exact profile.

        Existing PAPER callers pass ``binance-dev.sqlite`` and TESTNET callers
        may pass ``binance-testnet.sqlite``.  The explicit ``paper`` and
        ``testnet`` constructors are preferred for new code.
        """

        normalized_transport = str(transport).strip().lower().replace("-", "_")
        if normalized_transport not in {"binance_spot", "binance"}:
            raise BinanceSpotConfigurationError(
                "Binance Spot profiles reject non-Binance transports"
            )
        if environment is None:
            if db_path is not None and os.path.basename(os.fspath(db_path)).lower() == "binance-testnet.sqlite":
                env = BinanceSpotEnvironment.BINANCE_SPOT_TESTNET
            else:
                env = BinanceSpotEnvironment.PAPER
        else:
            env = _environment(environment)
        spec = _PROFILE_SPECS.get(env)
        if spec is None:
            raise BinanceSpotConfigurationError("Binance LIVE is not a development profile")
        root = _lexical_path(worktree_root)
        selected_db = (
            os.fspath(db_path)
            if db_path is not None
            else os.path.join(root, "runtime-data", spec["db_name"])
        )
        selected_host = "127.0.0.1" if host is None else host
        selected_port = spec["port"] if port is None else port
        return cls(
            worktree_root=root,
            db_path=selected_db,
            host=selected_host,
            port=selected_port,
            environment=env,
        )

    @classmethod
    def paper(
        cls,
        worktree_root: str | os.PathLike[str],
        db_path: str | os.PathLike[str] | None = None,
        host: str = "127.0.0.1",
        port: int = 8081,
        *,
        transport: str = "binance_spot",
    ) -> "BinanceRuntimeProfile":
        return cls.development(
            worktree_root,
            db_path,
            host,
            port,
            environment=BinanceSpotEnvironment.PAPER,
            transport=transport,
        )

    @classmethod
    def testnet(
        cls,
        worktree_root: str | os.PathLike[str],
        db_path: str | os.PathLike[str] | None = None,
        host: str = "127.0.0.1",
        port: int = 8082,
        *,
        transport: str = "binance_spot",
    ) -> "BinanceRuntimeProfile":
        return cls.development(
            worktree_root,
            db_path,
            host,
            port,
            environment=BinanceSpotEnvironment.BINANCE_SPOT_TESTNET,
            transport=transport,
        )

    @classmethod
    def for_environment(
        cls,
        worktree_root: str | os.PathLike[str],
        environment: BinanceSpotEnvironment | str,
        db_path: str | os.PathLike[str] | None = None,
        host: str | None = None,
        port: int | None = None,
        *,
        transport: str = "binance_spot",
    ) -> "BinanceRuntimeProfile":
        return cls.development(
            worktree_root,
            db_path,
            host,
            port,
            environment=environment,
            transport=transport,
        )

    @property
    def runtime_data_dir(self) -> str:
        return _canonical_path(os.path.join(self.worktree_root, "runtime-data"))

    @property
    def log_path(self) -> str:
        return _canonical_path(os.path.join(self.runtime_data_dir, self.log_identity))

    @property
    def lock_path(self) -> str:
        return _canonical_path(os.path.join(self.runtime_data_dir, self.lock_identity))

    @property
    def stop_path(self) -> str:
        return _canonical_path(os.path.join(self.runtime_data_dir, self.stop_identity))

    @property
    def pid_path(self) -> str:
        return _canonical_path(os.path.join(self.runtime_data_dir, self.pid_identity))

    def projection(self) -> dict[str, Any]:
        """Return non-secret runtime identity metadata only."""

        return {
            "feature": "binance_spot",
            "feature_instance": self.feature_instance,
            "environment": self.environment.value,
            "runtime": self.runtime_identity,
            "runtime_identity": self.runtime_identity,
            "db_path": self.db_path,
            "host": self.host,
            "port": self.port,
            "log": self.log_identity,
            "log_identity": self.log_identity,
            "log_path": self.log_path,
            "lock": self.lock_identity,
            "lock_identity": self.lock_identity,
            "lock_path": self.lock_path,
            "stop": self.stop_identity,
            "stop_identity": self.stop_identity,
            "stop_path": self.stop_path,
            "pid": self.pid_identity,
            "pid_identity": self.pid_identity,
            "pid_path": self.pid_path,
            "background": self.background_identity,
            "background_identity": self.background_identity,
            "transport": self.transport,
            "permissions": permission_projection(),
        }

    safe_projection = projection


# A more descriptive alias used by early downstream prototypes.
BinanceSpotRuntimeProfile = BinanceRuntimeProfile


def permission_projection() -> dict[str, Any]:
    """Describe the allowed API permissions without exposing credentials."""

    return {
        "required": tuple(sorted(BINANCE_SPOT_REQUIRED_PERMISSIONS)),
        "prohibited": tuple(sorted(BINANCE_SPOT_PROHIBITED_PERMISSIONS)),
        "withdrawal": False,
        "transfer": False,
    }


@dataclass(frozen=True, slots=True)
class BinanceCredentialRef:
    """Environment-bound keyring identity; never contains credential values."""

    instance: str
    environment: BinanceSpotEnvironment | str
    namespace: str | None = None

    def __post_init__(self) -> None:
        instance = str(self.instance).strip()
        if not instance or any(
            ch not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.:-"
            for ch in instance
        ):
            raise BinanceSpotConfigurationError(
                "credential instance must be a safe non-empty identifier"
            )
        env = _environment(self.environment)
        expected = {
            BinanceSpotEnvironment.PAPER: ("binance-dev", "AXIOM-BINANCE-SPOT"),
            BinanceSpotEnvironment.BINANCE_SPOT_TESTNET: (
                "binance-testnet",
                "AXIOM-BINANCE-SPOT-TESTNET",
            ),
        }.get(env)
        if expected is None:
            raise BinanceSpotConfigurationError(
                "Binance LIVE credentials are prohibited"
            )
        expected_instance, expected_namespace = expected
        if instance != expected_instance:
            raise BinanceSpotEnvironmentMismatch(
                f"{env.value} credentials require instance {expected_instance!r}"
            )
        namespace = expected_namespace if self.namespace is None else str(self.namespace)
        if namespace != expected_namespace:
            raise BinanceSpotEnvironmentMismatch(
                f"{env.value} credentials require namespace {expected_namespace!r}"
            )
        object.__setattr__(self, "instance", instance)
        object.__setattr__(self, "environment", env)
        object.__setattr__(self, "namespace", namespace)

    @property
    def identity(self) -> str:
        return f"{self.instance}:{self.environment.value}"

    @property
    def service(self) -> str:
        return str(self.namespace)
    def stable_id(self) -> str:
        """Return an opaque, deterministic identity hash without credentials."""

        return canonical_sha256(
            {
                "environment": self.environment.value,
                "instance": self.instance,
                "namespace": self.namespace,
                "service": self.service,
            }
        )


    def projection(self) -> dict[str, str]:
        return {
            "namespace": str(self.namespace),
            "service": self.service,
            "instance": self.instance,
            "environment": self.environment.value,
            "identity": self.identity,
        }

class BinanceCredentialStore:
    """Isolated keyring store with no environment-variable fallback."""

    def __init__(
        self,
        ref: BinanceCredentialRef | None = None,
        *,
        instance: str | None = None,
        environment: BinanceSpotEnvironment | str | None = None,
        namespace: str | None = None,
        keyring_backend: Any | None = None,
        keyring: Any | None = None,
        allow_environment: bool = False,
    ) -> None:
        if allow_environment:
            raise BinanceSpotConfigurationError(
                "environment credential fallback is prohibited"
            )
        if ref is not None:
            if not isinstance(ref, BinanceCredentialRef):
                raise TypeError("ref must be BinanceCredentialRef")
            if instance is not None and str(instance) != ref.instance:
                raise BinanceSpotEnvironmentMismatch(
                    "credential reference instance mismatch"
                )
            if environment is not None and _environment(environment) is not ref.environment:
                raise BinanceSpotEnvironmentMismatch(
                    "credential reference environment mismatch"
                )
            if namespace is not None and str(namespace) != ref.namespace:
                raise BinanceSpotEnvironmentMismatch(
                    "credential reference namespace mismatch"
                )
            candidate = ref
        else:
            env = (
                BinanceSpotEnvironment.PAPER
                if environment is None
                else _environment(environment)
            )
            if instance is None:
                instance = {
                    BinanceSpotEnvironment.PAPER: "binance-dev",
                    BinanceSpotEnvironment.BINANCE_SPOT_TESTNET: "binance-testnet",
                }.get(env)
            if instance is None:
                raise BinanceSpotConfigurationError(
                    "Binance LIVE credentials are prohibited"
                )
            candidate = BinanceCredentialRef(
                instance=instance,
                environment=env,
                namespace=namespace,
            )
        self.ref = candidate
        self._keyring = keyring_backend if keyring_backend is not None else keyring

    @property
    def namespace(self) -> str:
        return self.ref.namespace

    @property
    def service(self) -> str:
        """The backend service is always derived from the validated ref."""

        return self.ref.service

    def _backend(self) -> Any:
        if self._keyring is not None:
            return self._keyring
        try:
            import keyring as backend
        except ImportError as exc:  # pragma: no cover - install-dependent
            raise BinanceSpotConfigurationError("OS keyring unavailable") from exc
        return backend

    def _username(self, field: str) -> str:
        return f"{self.ref.identity}:{field}"

    def configure(self, api_key: str, api_secret: str) -> BinanceCredentialRef:
        credentials = BinanceSpotCredentials(api_key, api_secret)
        backend = self._backend()
        backend.set_password(self.service, self._username("api_key"), credentials.api_key)
        backend.set_password(self.service, self._username("api_secret"), credentials.api_secret)
        return self.ref

    store = configure
    set = configure

    def load(self, ref: BinanceCredentialRef | None = None) -> BinanceSpotCredentials | None:
        if ref is not None and ref != self.ref:
            raise BinanceSpotEnvironmentMismatch(
                "credential reference environment/instance/namespace mismatch"
            )
        backend = self._backend()
        key = backend.get_password(self.service, self._username("api_key"))
        secret = backend.get_password(self.service, self._username("api_secret"))
        if not key and not secret:
            return None
        if not key or not secret:
            raise BinanceSpotConfigurationError("incomplete Binance Spot credentials")
        return BinanceSpotCredentials(str(key), str(secret))

    def configured(self, ref: BinanceCredentialRef | None = None) -> bool:
        return self.load(ref) is not None

    def safe_projection(self) -> dict[str, Any]:
        loaded = self.load()
        projection = self.ref.projection()
        projection.update({"configured": loaded is not None, "secret_values_exposed": False})
        return projection

    projection = safe_projection


@dataclass(frozen=True, slots=True, repr=False)
class BinanceSpotCredentials:
    """Explicit credentials held in memory only for signing requests."""

    api_key: str
    api_secret: str

    def __post_init__(self) -> None:
        if not isinstance(self.api_key, str) or not self.api_key:
            raise BinanceSpotConfigurationError("api_key must be non-empty")
        if not isinstance(self.api_secret, str) or not self.api_secret:
            raise BinanceSpotConfigurationError("api_secret must be non-empty")

    def __repr__(self) -> str:  # pragma: no cover - defensive secret hygiene
        return "BinanceSpotCredentials(api_key=<redacted>, api_secret=<redacted>)"

    def projection(self) -> dict[str, bool]:
        return {"configured": True, "api_key_configured": True, "api_secret_configured": True, "secret_values_exposed": False}




class BinanceSpotResult(Mapping[str, Any]):
    """Safe structured result for REST calls (never includes signing secrets)."""

    __slots__ = ("status", "payload", "http_status", "error_code", "retry_after", "endpoint", "validation_only", "label")

    def __init__(
        self,
        status: BinanceSpotStatus | str,
        payload: Any = None,
        *,
        http_status: int | None = None,
        error_code: int | None = None,
        retry_after: str | None = None,
        endpoint: str | None = None,
        validation_only: bool = False,
        label: str | None = None,
    ) -> None:
        self.status = BinanceSpotStatus(status).value if not isinstance(status, BinanceSpotStatus) else status.value
        self.payload = payload
        self.http_status = http_status
        self.error_code = error_code
        self.retry_after = retry_after
        self.endpoint = endpoint
        self.validation_only = bool(validation_only)
        self.label = label

    def __getitem__(self, key: str) -> Any:
        return self.to_dict()[key]

    def __iter__(self):
        return iter(self.to_dict())

    def __len__(self) -> int:
        return len(self.to_dict())

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "payload": self.payload,
            "http_status": self.http_status,
            "error_code": self.error_code,
            "retry_after": self.retry_after,
            "endpoint": self.endpoint,
            "validation_only": self.validation_only,
            "label": self.label,
        }

    def __repr__(self) -> str:
        return f"BinanceSpotResult(status={self.status!r}, endpoint={self.endpoint!r}, http_status={self.http_status!r})"


class BinanceSpotRESTClient:
    """Authenticated Spot REST client with an explicit endpoint allowlist."""

    _ALLOWED = frozenset(
        {
            ("GET", "/api/v3/account"),
            ("POST", "/api/v3/order"),
            ("POST", "/api/v3/order/test"),
            ("GET", "/api/v3/order"),
            ("GET", "/api/v3/openOrders"),
            ("GET", "/api/v3/allOrders"),
            ("GET", "/api/v3/myTrades"),
            ("GET", "/api/v3/rateLimit/order"),
            ("DELETE", "/api/v3/order"),
        }
    )
    # Public requests are deliberately separate from the signed allowlist.
    _PUBLIC_ALLOWED = frozenset(
        {
            ("GET", "/api/v3/time"),
            ("GET", "/api/v3/exchangeInfo"),
            ("GET", "/api/v3/ticker/24hr"),
            ("GET", "/api/v3/ticker/price"),
            ("GET", "/api/v3/ticker/bookTicker"),
            ("GET", "/api/v3/depth"),
            ("GET", "/api/v3/klines"),
        }
    )
    AMBIGUOUS_CODES = frozenset({-1000, -1006, -1007})

    def __init__(
        self,
        environment: BinanceSpotEnvironment | str | BinanceRuntimeProfile,
        credentials: BinanceSpotCredentials | Mapping[str, str] | Sequence[str] | None = None,
        *,
        profile: BinanceRuntimeProfile | None = None,
        opener: Callable[..., Any] | None = None,
        timeout: float = 10.0,
        clock: Callable[[], Any] | None = None,
        recv_window: int = 5000,
    ) -> None:
        if isinstance(environment, BinanceRuntimeProfile):
            if profile is not None and profile != environment:
                raise BinanceSpotEnvironmentMismatch("multiple runtime profiles supplied")
            profile = environment
            env = profile.environment
        else:
            env = _environment(environment)
        if profile is not None:
            if not isinstance(profile, BinanceRuntimeProfile):
                raise TypeError("profile must be BinanceRuntimeProfile")
            if env is not profile.environment:
                raise BinanceSpotEnvironmentMismatch("profile and client environment differ")
        if env is BinanceSpotEnvironment.BINANCE_SPOT_LIVE:
            raise BinanceSpotConfigurationError(
                "Binance LIVE/mainnet transport is prohibited"
            )
        if isinstance(credentials, BinanceSpotCredentials):
            explicit = credentials
        elif isinstance(credentials, Mapping):
            explicit = BinanceSpotCredentials(
                str(credentials.get("api_key", "")),
                str(credentials.get("api_secret", "")),
            )
        elif (
            isinstance(credentials, Sequence)
            and not isinstance(credentials, (str, bytes, bytearray))
            and len(credentials) == 2
        ):
            explicit = BinanceSpotCredentials(str(credentials[0]), str(credentials[1]))
        elif credentials is None:
            explicit = None
        else:
            raise TypeError(
                "credentials must be explicit BinanceSpotCredentials or api_key/api_secret mapping"
            )
        self.environment = env
        self.profile = profile
        self.credentials = explicit
        self.base_url = SPOT_REST_ORIGINS[env]
        timeout_value = float(timeout)
        if not math.isfinite(timeout_value) or timeout_value <= 0:
            raise ValueError("timeout must be finite and positive")
        recv_value = int(recv_window)
        if recv_value < 1 or recv_value > 5000:
            raise ValueError("recv_window must be between 1 and 5000")
        self.timeout = timeout_value
        self.recv_window = recv_value
        self._request_lock = threading.RLock()
        self._timeout_local = threading.local()
        self._server_time_offset_ms = 0
        self._opener = opener or build_opener(_RejectRedirectHandler())
        self._clock = clock or time.time

    @property
    def origin(self) -> str | None:
        return self.base_url

    def _effective_timeout(self) -> float:
        with self._request_lock:
            return float(getattr(self._timeout_local, "value", self.timeout))

    def set_server_time_offset_ms(self, offset_ms: int | float) -> None:
        """Set the bounded clock correction used by every signed request."""
        if isinstance(offset_ms, bool):
            raise ValueError("server time offset must be finite")
        try:
            value = int(offset_ms)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError("server time offset must be finite") from exc
        if abs(value) > _MAX_SERVER_TIME_OFFSET_MS:
            raise ValueError("server time offset is outside the supported bound")
        with self._request_lock:
            self._server_time_offset_ms = value

    @property
    def server_time_offset_ms(self) -> int:
        with self._request_lock:
            return self._server_time_offset_ms

    def _response_origin_check(self, response: Any, request_url: str) -> None:
        geturl = getattr(response, "geturl", None)
        response_url = geturl() if callable(geturl) else None
        if not response_url:
            return
        expected = urlsplit(request_url)
        actual = urlsplit(str(response_url))
        expected_origin = (expected.scheme.lower(), expected.hostname, expected.port)
        actual_origin = (actual.scheme.lower(), actual.hostname, actual.port)
        if actual_origin != expected_origin:
            raise BinanceSpotTransportError("Binance Spot transport rejected cross-origin response")

    def _request_timeout(self, deadline_monotonic: float | None) -> float:
        if deadline_monotonic is None:
            return self.timeout
        try:
            remaining = float(deadline_monotonic) - time.monotonic()
        except (TypeError, ValueError, OverflowError):
            remaining = -1.0
        if remaining <= 0:
            raise TimeoutError("AUTO_DEADLINE_EXPIRED")
        return min(self.timeout, remaining, _DEADLINE_REQUEST_CEILING_SECONDS)
    def _require_auth(self) -> BinanceSpotCredentials:
        if self.environment is not BinanceSpotEnvironment.BINANCE_SPOT_TESTNET:
            raise BinanceSpotConfigurationError(
                "authenticated Binance Spot calls are TESTNET-only"
            )
        if self.base_url != SPOT_REST_ORIGINS[BinanceSpotEnvironment.BINANCE_SPOT_TESTNET]:
            raise BinanceSpotConfigurationError(
                "Binance Spot authenticated origin is not fixed to TESTNET"
            )
        if self.credentials is None:
            raise BinanceSpotConfigurationError("explicit Binance Spot credentials are required")
        return self.credentials

    @staticmethod
    def _millis(clock: Callable[[], Any]) -> int:
        value = clock()
        if isinstance(value, datetime):
            if value.tzinfo is None:
                value = value.replace(tzinfo=timezone.utc)
            return int(value.timestamp() * 1000)
        return int(float(value) * 1000)

    @staticmethod
    def _encode(params: Mapping[str, Any]) -> bytes:
        pairs: list[tuple[str, str]] = []
        for key, value in params.items():
            if value is None:
                continue
            if isinstance(value, (list, tuple)):
                pairs.extend((str(key), str(item)) for item in value)
            else:
                pairs.append((str(key), str(value)))
        return urlencode(pairs, doseq=True).encode("utf-8")

    def _request(
        self,
        method: str,
        endpoint: str,
        params: MutableMapping[str, Any] | None = None,
        *,
        validation_only: bool = False,
        deadline_monotonic: float | None = None,
    ) -> BinanceSpotResult:
        request_timeout = self._request_timeout(deadline_monotonic)
        credentials = self._require_auth()
        method = method.upper()
        if (method, endpoint) not in self._ALLOWED:
            raise BinanceSpotConfigurationError(f"Spot endpoint is not allowlisted: {method} {endpoint}")
        values: dict[str, Any] = dict(params or {})
        if any(key in values for key in ("deadline_monotonic", "timestamp", "recvWindow")):
            raise BinanceSpotConfigurationError("deadline, timestamp, and recvWindow are client-owned")
        with self._request_lock:
            offset_ms = self._server_time_offset_ms
        values["timestamp"] = self._millis(self._clock) + offset_ms
        values["recvWindow"] = self.recv_window
        unsigned = self._encode(values)
        signature = hmac.new(credentials.api_secret.encode("utf-8"), unsigned, hashlib.sha256).hexdigest()
        signed = unsigned + b"&signature=" + signature.encode("ascii")
        if method == "GET":
            url = f"{self.base_url}{endpoint}?{signed.decode('ascii')}"
            data = None
        else:
            url = f"{self.base_url}{endpoint}"
            data = signed
        request = Request(
            url,
            data=data,
            headers={"X-MBX-APIKEY": credentials.api_key, "Content-Type": "application/x-www-form-urlencoded"},
            method=method,
        )
        try:
            raw = self._opener(request, timeout=request_timeout)
            self._response_origin_check(raw, url)
            status = int(getattr(raw, "status", getattr(raw, "code", 200)))
            if 300 <= status < 400:
                raise BinanceSpotTransportError("Binance Spot transport rejected redirect")
            headers = getattr(raw, "headers", {}) or {}
            body = raw.read() if hasattr(raw, "read") else raw
            payload = self._decode(body)
        except HTTPError as exc:
            status = int(getattr(exc, "code", 0) or 0)
            if 300 <= status < 400:
                raise BinanceSpotTransportError("Binance Spot transport rejected redirect") from exc
            headers = getattr(exc, "headers", {}) or {}
            body = exc.read() if hasattr(exc, "read") else b""
            payload = self._decode(body)
        except (TimeoutError, socket.timeout, URLError, OSError):
            return BinanceSpotResult(BinanceSpotStatus.UNKNOWN, endpoint=endpoint, validation_only=validation_only)
        if status in {418, 429}:
            return BinanceSpotResult(BinanceSpotStatus.RATE_LIMIT, payload, http_status=status, retry_after=self._header(headers, "Retry-After"), endpoint=endpoint, validation_only=validation_only)
        code = self._error_code(payload)
        if status >= 500 or code in self.AMBIGUOUS_CODES:
            return BinanceSpotResult(BinanceSpotStatus.UNKNOWN, payload, http_status=status, error_code=code, endpoint=endpoint, validation_only=validation_only)
        if status >= 400 or code is not None:
            return BinanceSpotResult(BinanceSpotStatus.REJECTED, payload, http_status=status, error_code=code, endpoint=endpoint, validation_only=validation_only)
        return BinanceSpotResult(BinanceSpotStatus.OK, payload, http_status=status, endpoint=endpoint, validation_only=validation_only, label="VALIDATION_ONLY" if validation_only else None)

    def request(
        self,
        method: str,
        endpoint: str,
        params: Mapping[str, Any] | None = None,
        *,
        validation_only: bool = False,
        deadline_monotonic: float | None = None,
    ) -> BinanceSpotResult:
        """Issue one explicitly allowlisted Spot request."""
        return self._request(
            method,
            endpoint,
            dict(params or {}),
            validation_only=validation_only,
            deadline_monotonic=deadline_monotonic,
        )

    def public_request(
        self,
        method: str,
        endpoint: str,
        params: Mapping[str, Any] | None = None,
        *,
        deadline_monotonic: float | None = None,
    ) -> BinanceSpotResult:
        """Issue one fixed-origin, unsigned TESTNET public request."""
        request_timeout = self._request_timeout(deadline_monotonic)
        if self.environment is not BinanceSpotEnvironment.BINANCE_SPOT_TESTNET:
            raise BinanceSpotConfigurationError("public Spot requests are TESTNET-only")
        if self.base_url != SPOT_REST_ORIGINS[BinanceSpotEnvironment.BINANCE_SPOT_TESTNET]:
            raise BinanceSpotConfigurationError(
                "Binance Spot public origin is not fixed to TESTNET"
            )
        method = str(method).upper()
        if (method, endpoint) not in self._PUBLIC_ALLOWED:
            raise BinanceSpotConfigurationError(
                f"Spot public endpoint is not allowlisted: {method} {endpoint}"
            )
        values = dict(params or {})
        if "deadline_monotonic" in values:
            raise BinanceSpotConfigurationError("deadline_monotonic is transport-only")
        encoded = self._encode(values).decode("ascii")
        url = f"{self.base_url}{endpoint}"
        if encoded:
            url = f"{url}?{encoded}"
        request = Request(
            url,
            headers={"Accept": "application/json"},
            method="GET",
        )
        try:
            raw = self._opener(request, timeout=request_timeout)
            self._response_origin_check(raw, url)
            status = int(getattr(raw, "status", getattr(raw, "code", 200)))
            if 300 <= status < 400:
                raise BinanceSpotTransportError("Binance Spot transport rejected redirect")
            headers = getattr(raw, "headers", {}) or {}
            body = raw.read() if hasattr(raw, "read") else raw
            payload = self._decode(body)
        except HTTPError as exc:
            status = int(getattr(exc, "code", 0) or 0)
            if 300 <= status < 400:
                raise BinanceSpotTransportError("Binance Spot transport rejected redirect") from exc
            headers = getattr(exc, "headers", {}) or {}
            body = exc.read() if hasattr(exc, "read") else b""
            payload = self._decode(body)
        except (TimeoutError, socket.timeout, URLError, OSError):
            return BinanceSpotResult(BinanceSpotStatus.UNKNOWN, endpoint=endpoint)
        if status in {418, 429}:
            return BinanceSpotResult(BinanceSpotStatus.RATE_LIMIT, payload, http_status=status, retry_after=self._header(headers, "Retry-After"), endpoint=endpoint)
        code = self._error_code(payload)
        if status >= 500 or code in self.AMBIGUOUS_CODES:
            return BinanceSpotResult(BinanceSpotStatus.UNKNOWN, payload, http_status=status, error_code=code, endpoint=endpoint)
        if status >= 400 or code is not None:
            return BinanceSpotResult(BinanceSpotStatus.REJECTED, payload, http_status=status, error_code=code, endpoint=endpoint)
        return BinanceSpotResult(BinanceSpotStatus.OK, payload, http_status=status, endpoint=endpoint)
    @staticmethod
    def _decode(body: Any) -> Any:
        if isinstance(body, bytes):
            body = body.decode("utf-8", errors="replace")
        if isinstance(body, str):
            if not body:
                return None
            try:
                return json.loads(body)
            except json.JSONDecodeError:
                return body
        return body

    @staticmethod
    def _error_code(payload: Any) -> int | None:
        if isinstance(payload, Mapping):
            value = payload.get("code")
            try:
                return int(value) if value is not None else None
            except (TypeError, ValueError):
                return None
        return None

    @staticmethod
    def _header(headers: Any, name: str) -> str | None:
        if hasattr(headers, "get"):
            value = headers.get(name)
            if value is None:
                value = headers.get(name.lower())
            return str(value) if value is not None else None
        return None

    @staticmethod
    def _symbol(symbol: str) -> str:
        normalized = str(symbol).replace("/", "").replace("-", "").strip().upper()
        if not normalized:
            raise ValueError("symbol must not be empty")
        return normalized
    def time(self, *, deadline_monotonic: float | None = None) -> BinanceSpotResult:
        return self.public_request("GET", "/api/v3/time", deadline_monotonic=deadline_monotonic)

    get_time = time
    server_time = time
    get_server_time = time
    public_time = time

    def exchange_info(
        self,
        *,
        symbol: str | None = None,
        symbols: Sequence[str] | None = None,
        deadline_monotonic: float | None = None,
        **params: Any,
    ) -> BinanceSpotResult:
        values = dict(params)
        if symbol is not None:
            values["symbol"] = self._symbol(symbol)
        if symbols is not None:
            values["symbols"] = json.dumps(
                [self._symbol(value) for value in symbols],
                separators=(",", ":"),
            )
        return self.public_request(
            "GET",
            "/api/v3/exchangeInfo",
            values,
            deadline_monotonic=deadline_monotonic,
        )

    get_exchange_info = exchange_info

    def ticker(
        self,
        *,
        symbol: str | None = None,
        deadline_monotonic: float | None = None,
        **params: Any,
    ) -> BinanceSpotResult:
        values = dict(params)
        if symbol is not None:
            values["symbol"] = self._symbol(symbol)
        return self.public_request(
            "GET",
            "/api/v3/ticker/24hr",
            values,
            deadline_monotonic=deadline_monotonic,
        )

    ticker_24hr = ticker
    ticker_stats = ticker

    def ticker_price(
        self,
        *,
        symbol: str | None = None,
        deadline_monotonic: float | None = None,
        **params: Any,
    ) -> BinanceSpotResult:
        values = dict(params)
        if symbol is not None:
            values["symbol"] = self._symbol(symbol)
        return self.public_request(
            "GET",
            "/api/v3/ticker/price",
            values,
            deadline_monotonic=deadline_monotonic,
        )

    price_ticker = ticker_price

    def ticker_book(
        self,
        *,
        symbol: str | None = None,
        deadline_monotonic: float | None = None,
        **params: Any,
    ) -> BinanceSpotResult:
        values = dict(params)
        if symbol is not None:
            values["symbol"] = self._symbol(symbol)
        return self.public_request(
            "GET",
            "/api/v3/ticker/bookTicker",
            values,
            deadline_monotonic=deadline_monotonic,
        )

    book_ticker = ticker_book

    def depth(
        self,
        *,
        symbol: str,
        limit: int | None = None,
        deadline_monotonic: float | None = None,
        **params: Any,
    ) -> BinanceSpotResult:
        values = dict(params)
        values["symbol"] = self._symbol(symbol)
        if limit is not None:
            values["limit"] = limit
        return self.public_request(
            "GET",
            "/api/v3/depth",
            values,
            deadline_monotonic=deadline_monotonic,
        )

    order_book = depth

    def klines(
        self,
        *,
        symbol: str,
        interval: str,
        start_time: int | None = None,
        end_time: int | None = None,
        limit: int | None = None,
        deadline_monotonic: float | None = None,
        **params: Any,
    ) -> BinanceSpotResult:
        values = dict(params)
        values.update({"symbol": self._symbol(symbol), "interval": str(interval)})
        if start_time is not None:
            values["startTime"] = start_time
        if end_time is not None:
            values["endTime"] = end_time
        if limit is not None:
            values["limit"] = limit
        return self.public_request(
            "GET",
            "/api/v3/klines",
            values,
            deadline_monotonic=deadline_monotonic,
        )

    candles = klines

    def account(self, *, deadline_monotonic: float | None = None, **params: Any) -> BinanceSpotResult:
        return self._request(
            "GET",
            "/api/v3/account",
            params,
            deadline_monotonic=deadline_monotonic,
        )

    get_account = account

    def place_order(
        self,
        *,
        symbol: str,
        side: str,
        quantity: str | int | float,
        price: str | int | float,
        time_in_force: str = "IOC",
        deadline_monotonic: float | None = None,
        **params: Any,
    ) -> BinanceSpotResult:
        tif = str(time_in_force).upper()
        if tif not in {"IOC", "FOK"}:
            raise BinanceSpotConfigurationError("only LIMIT IOC/FOK orders are permitted")
        side_value = str(side).upper()
        if side_value not in {"BUY", "SELL"}:
            raise ValueError("side must be BUY or SELL")
        values = {
            "symbol": self._symbol(symbol),
            "side": side_value,
            "type": "LIMIT",
            "timeInForce": tif,
            "quantity": quantity,
            "price": price,
            **params,
        }
        values["type"] = "LIMIT"
        values["timeInForce"] = tif
        return self._request(
            "POST",
            "/api/v3/order",
            values,
            deadline_monotonic=deadline_monotonic,
        )

    place_limit_order = place_order
    place_limit_ioc_order = place_order
    place_limit_fok_order = place_order

    def test_order(
        self,
        *,
        symbol: str,
        side: str,
        quantity: str | int | float,
        price: str | int | float,
        time_in_force: str = "IOC",
        deadline_monotonic: float | None = None,
        **params: Any,
    ) -> BinanceSpotResult:
        tif = str(time_in_force).upper()
        if tif not in {"IOC", "FOK"}:
            raise BinanceSpotConfigurationError("only LIMIT IOC/FOK test orders are permitted")
        side_value = str(side).upper()
        if side_value not in {"BUY", "SELL"}:
            raise ValueError("side must be BUY or SELL")
        values = {
            "symbol": self._symbol(symbol),
            "side": side_value,
            "type": "LIMIT",
            "timeInForce": tif,
            "quantity": quantity,
            "price": price,
            **params,
        }
        values["type"] = "LIMIT"
        values["timeInForce"] = tif
        return self._request(
            "POST",
            "/api/v3/order/test",
            values,
            validation_only=True,
            deadline_monotonic=deadline_monotonic,
        )

    place_test_order = test_order
    validation_test_order = test_order

    def query_order(
        self,
        *,
        symbol: str,
        order_id: str | int | None = None,
        orig_client_order_id: str | None = None,
        deadline_monotonic: float | None = None,
        **params: Any,
    ) -> BinanceSpotResult:
        if order_id is None and not orig_client_order_id:
            raise ValueError("order_id or orig_client_order_id is required")
        values = {
            key: value
            for key, value in params.items()
            if key not in {
                "order_id",
                "client_order_id",
                "new_client_order_id",
                "newClientOrderId",
            }
        }
        values["symbol"] = self._symbol(symbol)
        if order_id is not None:
            values["orderId"] = order_id
        if orig_client_order_id is not None:
            values["origClientOrderId"] = orig_client_order_id
        return self._request(
            "GET",
            "/api/v3/order",
            values,
            deadline_monotonic=deadline_monotonic,
        )

    get_order = query_order
    query = query_order

    def open_orders(
        self,
        *,
        symbol: str | None = None,
        deadline_monotonic: float | None = None,
        **params: Any,
    ) -> BinanceSpotResult:
        values = dict(params)
        if symbol is not None:
            values["symbol"] = self._symbol(symbol)
        return self._request(
            "GET",
            "/api/v3/openOrders",
            values,
            deadline_monotonic=deadline_monotonic,
        )

    get_open_orders = open_orders
    open = open_orders

    def all_orders(
        self,
        *,
        symbol: str,
        deadline_monotonic: float | None = None,
        **params: Any,
    ) -> BinanceSpotResult:
        values = {"symbol": self._symbol(symbol), **params}
        return self._request(
            "GET",
            "/api/v3/allOrders",
            values,
            deadline_monotonic=deadline_monotonic,
        )

    all = all_orders
    get_all_orders = all_orders

    def my_trades(
        self,
        *,
        symbol: str,
        order_id: str | int | None = None,
        deadline_monotonic: float | None = None,
        **params: Any,
    ) -> BinanceSpotResult:
        values = {
            key: value
            for key, value in params.items()
            if key not in {
                "order_id",
                "client_order_id",
                "new_client_order_id",
                "newClientOrderId",
                "orig_client_order_id",
            }
        }
        values["symbol"] = self._symbol(symbol)
        if order_id is not None:
            values["orderId"] = order_id
        return self._request(
            "GET",
            "/api/v3/myTrades",
            values,
            deadline_monotonic=deadline_monotonic,
        )

    trades = my_trades

    def cancel_owned_order(
        self,
        *,
        symbol: str,
        order_id: str | int | None = None,
        orig_client_order_id: str | None = None,
        deadline_monotonic: float | None = None,
        **params: Any,
    ) -> BinanceSpotResult:
        if order_id is None and not orig_client_order_id:
            raise ValueError("order_id or orig_client_order_id is required")
        values = {
            key: value
            for key, value in params.items()
            if key not in {"client_order_id", "new_client_order_id", "newClientOrderId"}
        }
        values["symbol"] = self._symbol(symbol)
        if order_id is not None:
            values["orderId"] = order_id
        if orig_client_order_id is not None:
            values["origClientOrderId"] = orig_client_order_id
        return self._request(
            "DELETE",
            "/api/v3/order",
            values,
            deadline_monotonic=deadline_monotonic,
        )

    cancel_owned = cancel_owned_order
    cancel_order = cancel_owned_order

    def rate_limit_order(
        self,
        *,
        deadline_monotonic: float | None = None,
    ) -> BinanceSpotResult:
        return self._request(
            "GET",
            "/api/v3/rateLimit/order",
            deadline_monotonic=deadline_monotonic,
        )

    order_rate_limit = rate_limit_order
    rate_limits = rate_limit_order


__all__ = [
    "PAPER",
    "BINANCE_SPOT_TESTNET",
    "BINANCE_SPOT_LIVE",
    "QUOTE_ASSET",
    "SPOT_REST_ORIGINS",
    "BINANCE_API_BASELINE_COMMIT",
    "BINANCE_API_BASELINE_DATE",
    "BINANCE_TESTNET_API_BASELINE_DATE",
    "API_BASELINE_COMMIT",
    "API_BASELINE_DATE",
    "TESTNET_API_BASELINE_DATE",
    "BINANCE_SPOT_REQUIRED_PERMISSIONS",
    "BINANCE_SPOT_PROHIBITED_PERMISSIONS",
    "BinanceSpotEnvironment",
    "BinanceSpotStatus",
    "BinanceSpotConfigurationError",
    "BinanceSpotEnvironmentMismatch",
    "BinanceSpotTransportError",
    "BinanceRuntimeProfile",
    "BinanceSpotRuntimeProfile",
    "BinanceCredentialRef",
    "BinanceSpotCredentials",
    "BinanceCredentialStore",
    "BinanceSpotResult",
    "BinanceSpotRESTClient",
    "canonical_path",
    "validate_spot_venue_identity",
    "canonical_json",
    "credential_fingerprint",
    "canonical_sha256",
    "permission_projection",
]
