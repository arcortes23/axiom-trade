"""Isolated, offline-safe Binance Spot development runtime.

The runtime in this module is intentionally narrower than the normal Axiom
supervisor.  It owns one fixed SQLite database, one loopback dashboard and one
Binance Spot worker.  PAPER is the only environment supported by the
development runtime; TESTNET has its own dedicated runtime below.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import math
import os
import inspect
import sqlite3
from pathlib import Path
import threading
import time
from typing import Any, Callable, Mapping
import uuid
from .binance_risk import DEFAULT_BINANCE_RISK_ENVELOPE
from .binance_auto import BinanceAutonomousWorker
from .binance_execution import BinanceExecutionService
from .binance_market import BoundedBinanceMarketCollector
from .binance_operator import BinanceCanaryControlPlane, BinanceOperatorError, BinanceTestnetControlPlane
from .binance_research import BinanceCryptoQualificationService
from .binance_spot import (
    BINANCE_SPOT_TESTNET,
    PAPER,
    BinanceCredentialRef,
    BinanceCredentialStore,
    BinanceRuntimeProfile,
    BinanceSpotEnvironment,
    BinanceSpotRESTClient,
    BinanceSpotResult,
    credential_fingerprint,
    validate_spot_venue_identity,
)
from .binance_testnet import BinanceTestnetGateService
from .crypto_universe import load_crypto_universe
from .data.binance import BinanceAdapter
from .dashboard import DashboardData, DashboardServer
from .storage import AxiomStore

UTC = timezone.utc
RUNTIME_IDENTITY = "binance-dev"
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8081
DEFAULT_INTERVAL_SECONDS = 60.0


def canonical_checkout_root(start: str | os.PathLike[str] | None = None) -> str:
    """Discover the canonical Axiom checkout containing ``pyproject.toml``.

    An explicitly supplied root is accepted when it is a temporary test
    checkout; discovery is strict only when no root is supplied.  ``resolve``
    and ``realpath`` collapse junction/symlink aliases before profile creation.
    """

    if start is not None:
        return os.path.normcase(os.path.realpath(os.path.abspath(os.fspath(start))))
    current = Path(__file__).resolve().parent
    for candidate in (current, *current.parents):
        if (candidate / "pyproject.toml").is_file() and (candidate / "axiom").is_dir():
            return os.path.normcase(os.path.realpath(str(candidate)))
    raise RuntimeError("unable to discover the Axiom checkout root")


def _decimal(value: Any, *, name: str = "value", default: Decimal | None = None) -> Decimal:
    if value is None or value == "":
        if default is not None:
            return default
        raise ValueError(f"{name} is required")
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a finite decimal") from exc
    if not result.is_finite():
        raise ValueError(f"{name} must be finite")
    return result


def _symbol(value: Any) -> str:
    result = str(value or "").replace("/", "").replace("-", "").replace("_", "").strip().upper()
    if not result:
        raise ValueError("symbol must not be empty")
    return result


def _environment(value: Any) -> BinanceSpotEnvironment:
    if isinstance(value, BinanceSpotEnvironment):
        return value
    text = str(value).strip()
    try:
        return BinanceSpotEnvironment(text)
    except ValueError:
        try:
            return BinanceSpotEnvironment[text]
        except (KeyError, TypeError) as exc:
            raise ValueError(f"unsupported Binance environment: {value!r}") from exc


class PaperBinanceSpotVenue:
    """Deterministic local Spot venue implementing the execution boundary.

    There is no transport in this venue.  LIMIT IOC/FOK orders consume a
    configured Decimal liquidity amount.  IOC fills up to available liquidity;
    FOK fills only when the complete quantity is available.  IDs and fills are
    monotonically deterministic for one venue instance and every operation is
    restricted to orders created by that instance.
    """

    environment = PAPER
    origin = None
    def __init__(
        self,
        *,
        initial_balances: Mapping[str, Any] | None = None,
        liquidity: Mapping[str, Any] | None = None,
        fill_ratio: Any = Decimal("1"),
        clock: Callable[[], Any] | None = None,
    ) -> None:
        self._lock = threading.RLock()
        self._clock = clock or (lambda: datetime.now(UTC))
        ratio = _decimal(fill_ratio, name="fill_ratio")
        if ratio < 0 or ratio > 1:
            raise ValueError("fill_ratio must be in [0, 1]")
        self.fill_ratio = ratio
        self.submissions: list[dict[str, Any]] = []
        self.cancellations: list[dict[str, Any]] = []
        self.trades: dict[str, list[dict[str, Any]]] = {}
        self.query_result: Any = None
        self.cancel_result: Any = None
        self.fail_query = False
        self.fail_trades = False
        self.raise_after_submit = False
        self.submit_started: Any = None
        self.submit_release: Any = None
        self.empty_fill_status = "EXPIRED"
        self._liquidity: dict[str, Decimal] = {
            _symbol(key): _decimal(value, name="liquidity")
            for key, value in (liquidity or {}).items()
        }
        self._balances: dict[str, Decimal] = {
            "USDT": Decimal("1000"),
            **{
                str(key).strip().upper(): _decimal(value, name="balance")
                for key, value in (initial_balances or {}).items()
            },
        }
        if any(value < 0 for value in self._balances.values()):
            raise ValueError("balances must be non-negative")
        self._orders: dict[str, dict[str, Any]] = {}
        self._orders_by_client: dict[str, str] = {}
        self.orders = self._orders
        self._fills: list[dict[str, Any]] = []
        self._order_number = 1_000_000
        self._trade_number = 2_000_000

    @staticmethod
    def _text(value: Decimal) -> str:
        return format(value, "f")

    def _now_millis(self) -> int:
        value = self._clock()
        if isinstance(value, datetime):
            moment = value if value.tzinfo else value.replace(tzinfo=UTC)
            return int(moment.astimezone(UTC).timestamp() * 1000)
        return int(float(value) * 1000)


    def _result(self, payload: Mapping[str, Any], *, status: str = "OK") -> BinanceSpotResult:
        return BinanceSpotResult(status, dict(payload), endpoint="paper://binance")

    def _owned(self, *, symbol: Any = None, order_id: Any = None, client_id: Any = None) -> dict[str, Any] | None:
        expected_symbol = _symbol(symbol) if symbol is not None else None
        if order_id is not None:
            row = self._orders.get(str(order_id))
            matches = [
                value
                for value in self._orders.values()
                if str(value.get("orderId", "")) == str(order_id)
            ]
            if matches:
                row = matches[-1]
            if row is not None and (expected_symbol is None or row.get("symbol", expected_symbol) == expected_symbol):
                return row
        if client_id:
            order_key = self._orders_by_client.get(str(client_id))
            row = self._orders.get(order_key) if order_key else None
            if row is None:
                row = self._orders.get(str(client_id))
            if row is not None and (expected_symbol is None or row.get("symbol", expected_symbol) == expected_symbol):
                return row
        return None
    def account(self, **_: Any) -> BinanceSpotResult:
        with self._lock:
            balances = [
                {"asset": asset, "free": self._text(amount), "locked": "0"}
                for asset, amount in sorted(self._balances.items())
            ]
            inventory = {
                asset + "USDT": self._text(amount)
                for asset, amount in self._balances.items()
                if asset != "USDT"
            }
            return self._result(
                {
                    "accountType": "SPOT",
                    "canTrade": True,
                    "balances": balances,
                    "quote_available": self._text(self._balances.get("USDT", Decimal("0"))),
                    "aggregate_exposure": "0",
                    "reserved_exposure": "0",
                    "owned_inventory": inventory,
                    "positions": sum(
                        1
                        for asset, amount in self._balances.items()
                        if asset != "USDT" and amount > 0
                    ),
                    "exit_submissions_today": sum(
                        1 for row in self._orders.values() if row.get("side") == "SELL"
                    ),
                    "epoch": "paper-1",
                }
            )

    get_account = account

    def test_order(self, *, symbol: str, side: str, quantity: Any, price: Any, time_in_force: str = "IOC", **_: Any) -> BinanceSpotResult:
        normalized = _symbol(symbol)
        side_value = str(side).upper()
        tif = str(time_in_force).upper()
        qty = _decimal(quantity, name="quantity")
        px = _decimal(price, name="price")
        if side_value not in {"BUY", "SELL"} or qty <= 0 or px <= 0 or tif not in {"IOC", "FOK"}:
            return self._result({"code": -1100, "msg": "invalid LIMIT IOC/FOK test order", "symbol": normalized}, status="REJECTED")
        return self._result({"symbol": normalized, "side": side_value, "type": "LIMIT", "timeInForce": tif, "status": "TEST_ORDER", "valid": True})

    order_test = test_order
    place_test_order = test_order

    def place_limit_order(
        self,
        *,
        symbol: str,
        side: str,
        quantity: Any,
        price: Any,
        time_in_force: str = "IOC",
        new_client_order_id: str | None = None,
        newClientOrderId: str | None = None,
        client_order_id: str | None = None,
        **_: Any,
    ) -> BinanceSpotResult:
        normalized = _symbol(symbol)
        side_value = str(side).upper()
        tif = str(time_in_force).upper()
        qty = _decimal(quantity, name="quantity")
        px = _decimal(price, name="price")
        client = str(new_client_order_id or newClientOrderId or client_order_id or "").strip()
        if not client:
            client = "PAPER-" + hashlib.sha256(
                f"{normalized}:{side_value}:{qty}:{px}:{self._order_number}".encode()
            ).hexdigest()[:20]
        self.submissions.append(
            {
                "symbol": normalized,
                "side": side_value,
                "quantity": self._text(qty),
                "price": self._text(px),
                "time_in_force": tif,
                "new_client_order_id": client,
            }
        )
        with self._lock:
            if side_value not in {"BUY", "SELL"} or qty <= 0 or px <= 0 or tif not in {"IOC", "FOK"}:
                return self._result({"code": -1100, "msg": "only positive LIMIT IOC/FOK orders are permitted"}, status="REJECTED")
            prior = self._owned(client_id=client)
            if prior is not None:
                return self._result(prior)
            self._order_number += 1
            order_id = str(self._order_number)
            base = normalized[:-4] if normalized.endswith("USDT") else normalized
            available = self._liquidity.get(normalized, qty)
            available = max(Decimal("0"), min(qty, available) * self.fill_ratio)
            if side_value == "BUY":
                available = min(available, self._balances.get("USDT", Decimal("0")) / px)
            else:
                available = min(available, self._balances.get(base, Decimal("0")))
            requested_fill = min(qty, max(Decimal("0"), available))
            if tif == "FOK" and requested_fill < qty:
                executed = Decimal("0")
                status = "EXPIRED"
            else:
                executed = requested_fill
                status = "FILLED" if executed >= qty else ("PARTIALLY_FILLED" if executed > 0 else self.empty_fill_status)
            if normalized in self._liquidity:
                self._liquidity[normalized] = max(Decimal("0"), self._liquidity[normalized] - executed)
            quote = executed * px
            fills: list[dict[str, Any]] = []
            if executed > 0:
                self._trade_number += 1
                trade = {
                    "tradeId": str(self._trade_number),
                    "orderId": order_id,
                    "symbol": normalized,
                    "side": side_value,
                    "price": self._text(px),
                    "qty": self._text(executed),
                    "quoteQty": self._text(quote),
                    "commission": "0",
                    "commissionAsset": "USDT",
                    "time": self._now_millis(),
                    "isBuyer": side_value == "BUY",
                }
                fills.append(trade)
                self._fills.append(dict(trade))
                base = normalized[:-4] if normalized.endswith("USDT") else normalized
                if side_value == "BUY":
                    self._balances["USDT"] = self._balances.get("USDT", Decimal("0")) - quote
                    self._balances[base] = self._balances.get(base, Decimal("0")) + executed
                else:
                    self._balances[base] = self._balances.get(base, Decimal("0")) - executed
                    self._balances["USDT"] = self._balances.get("USDT", Decimal("0")) + quote
            order = {
                "symbol": normalized,
                "orderId": order_id,
                "clientOrderId": client,
                "transactTime": self._now_millis(),
                "price": self._text(px),
                "origQty": self._text(qty),
                "executedQty": self._text(executed),
                "cummulativeQuoteQty": self._text(quote),
                "status": status,
                "timeInForce": tif,
                "type": "LIMIT",
                "side": side_value,
                "fills": fills,
            }
            self._orders[order_id] = order
            self._orders_by_client[client] = order_id
            return self._result(order)

    place_order = place_limit_order
    place_limit_ioc_order = place_limit_order
    place_limit_fok_order = place_limit_order

    def query_order(
        self,
        *,
        symbol: str,
        order_id: str | int | None = None,
        orig_client_order_id: str | None = None,
        client_order_id: str | None = None,
        **_: Any,
    ) -> BinanceSpotResult:
        if self.fail_query:
            raise ConnectionError("query disconnected")
        if self.query_result is not None:
            return self.query_result
        with self._lock:
            order = self._owned(symbol=symbol, order_id=order_id, client_id=orig_client_order_id or client_order_id)
            if order is None:
                return self._result({"code": -2013, "msg": "Order does not exist", "authoritative_missing": True}, status="REJECTED")
            return self._result(order)

    get_order = query_order
    query = query_order
    def my_trades(
        self,
        *,
        symbol: str,
        order_id: str | int | None = None,
        **_: Any,
    ) -> BinanceSpotResult:
        normalized = _symbol(symbol)
        if self.fail_trades:
            raise ConnectionError("myTrades disconnected")
        if order_id is not None and str(order_id) in self.trades:
            return self._result(
                {
                    "symbol": normalized,
                    "trades": [dict(row) for row in self.trades[str(order_id)]],
                }
            )
        with self._lock:
            rows = [
                dict(fill)
                for fill in self._fills
                if fill["symbol"] == normalized
                and (order_id is None or str(fill["orderId"]) == str(order_id))
            ]
            return self._result({"symbol": normalized, "trades": rows})

    def open_orders(self, *, symbol: str | None = None, **_: Any) -> BinanceSpotResult:
        normalized = _symbol(symbol) if symbol is not None else None
        with self._lock:
            rows = [dict(row) for row in self._orders.values() if row["status"] in {"NEW", "ACKNOWLEDGED", "PARTIALLY_FILLED"} and (normalized is None or row["symbol"] == normalized)]
            return self._result({"orders": rows})

    open = open_orders

    def cancel_owned_order(
        self,
        *,
        symbol: str,
        order_id: str | int | None = None,
        orig_client_order_id: str | None = None,
        client_order_id: str | None = None,
        **_: Any,
    ) -> BinanceSpotResult:
        self.cancellations.append(
            {
                "symbol": _symbol(symbol),
                "order_id": order_id,
                "orig_client_order_id": orig_client_order_id or client_order_id,
            }
        )
        if self.cancel_result is not None:
            return self.cancel_result
        with self._lock:
            order = self._owned(symbol=symbol, order_id=order_id, client_id=orig_client_order_id or client_order_id)
            if order is None:
                return self._result({"code": -2013, "msg": "Order does not exist", "authoritative_missing": True}, status="REJECTED")
            order["status"] = "CANCELED"
            order["transactTime"] = self._now_millis()
            return self._result(order)

    cancel_owned = cancel_owned_order
    cancel_order = cancel_owned_order
def _validate_runtime_venue(
    environment: BinanceSpotEnvironment,
    venue: Any,
    credentials: Any | None = None,
) -> None:
    """Apply the concrete venue and credential execution boundary."""
    try:
        validate_spot_venue_identity(
            venue,
            environment,
            credential_hash=credential_fingerprint(credentials)
            if environment is BinanceSpotEnvironment.BINANCE_SPOT_TESTNET
            else None,
        )
    except ValueError:
        raise
    except Exception as exc:
        raise ValueError("invalid Binance Spot venue identity") from exc


class _CurrentPersistedUniverseLoader:
    """Duck-typed current-universe loader bound to one development store."""

    def __init__(self, store: AxiomStore) -> None:
        self.store = store

    def load(self, **_: Any) -> Any:
        return load_crypto_universe(self.store)

    load_persisted = load
    snapshot = load


@dataclass(frozen=True)
class _RuntimeOwner:
    pid: int
    token: str


class BinanceDevelopmentRuntime:
    """Own the isolated PAPER development process and its resources."""

    def __init__(
        self,
        worktree_root: str | os.PathLike[str] | None = None,
        *,
        profile: BinanceRuntimeProfile | None = None,
        environment: BinanceSpotEnvironment | str | None = None,
        credentials: Any | None = None,
        venue: Any | None = None,
        provider: Any | None = None,
        collector: Any | None = None,
        worker: Any | None = None,
        dashboard_server: Any | None = None,
        clock: Callable[[], Any] | None = None,
        store_factory: Callable[[str], Any] | None = None,
        worker_factory: Callable[..., Any] | None = None,
        dashboard_server_factory: Callable[..., Any] | None = None,
    ) -> None:
        root = canonical_checkout_root(worktree_root)
        if profile is None:
            env = _environment(PAPER if environment is None else environment)
            if env is not BinanceSpotEnvironment.PAPER:
                raise ValueError("Binance development runtime requires PAPER")
            profile = BinanceRuntimeProfile.paper(root)
        else:
            if not isinstance(profile, BinanceRuntimeProfile):
                raise TypeError("profile must be BinanceRuntimeProfile")
            if profile.environment is not BinanceSpotEnvironment.PAPER:
                raise ValueError("Binance development runtime requires a PAPER profile")
            env = _environment(profile.environment if environment is None else environment)
            if env is not BinanceSpotEnvironment.PAPER:
                raise ValueError("Binance development runtime requires PAPER")
        if os.path.normcase(os.path.realpath(profile.worktree_root)) != root:
            raise ValueError("profile root does not match the development checkout")
        if profile.environment is not env:
            raise ValueError("profile/environment mismatch")
        if credentials is not None:
            raise ValueError("PAPER runtime does not accept credentials")
        resolved_venue = venue if venue is not None else PaperBinanceSpotVenue(clock=clock)
        _validate_runtime_venue(env, resolved_venue, None)

        self.profile = profile
        self.runtime_identity = self.profile.runtime_identity
        self.environment = env.value
        self.owner = _RuntimeOwner(os.getpid(), uuid.uuid4().hex)
        self.stop_event = threading.Event()
        self.clock = clock or (lambda: datetime.now(UTC))
        self._lock_fd: int | None = None
        self._owned_paths: set[str] = set()
        self._started = False
        self._closed = False
        self._worker_thread: threading.Thread | None = None
        self._worker_error: BaseException | None = None
        self.store = (store_factory or AxiomStore)(self.profile.db_path)
        self.provider = provider or BinanceAdapter(clock=self.clock)
        self.universe_loader = _CurrentPersistedUniverseLoader(self.store)
        snapshot = self.universe_loader.load()
        self.collector = collector
        if self.collector is None and snapshot is not None:
            self.collector = BoundedBinanceMarketCollector(self.provider, snapshot, max_workers=4, depth=20, clock=self.clock)
        self.paper_venue = resolved_venue
        self.venue = self.paper_venue
        self.qualification = BinanceCryptoQualificationService(self.store)
        self.execution = BinanceExecutionService(
            self.store,
            venue=self.venue,
            profile=self.profile,
            environment=self.environment,
            credentials=credentials,
            owner_id=self.runtime_identity,
        )
        self.worker = worker
        if self.worker is None:
            factory = worker_factory or BinanceAutonomousWorker
            self.worker = factory(
                self.store,
                self.execution,
                collector=self.collector,
                provider=None if self.collector is not None else self.provider,
                universe_loader=self.universe_loader.load,
                qualification=self.qualification,
                interval_seconds=DEFAULT_INTERVAL_SECONDS,
                stop_event=self.stop_event,
                worker_id=self.runtime_identity,
                profile=self.profile,
            )
        self.binance_canary = BinanceCanaryControlPlane(
            self.store,
            self.execution,
            qualification=self.qualification,
            worker=self.worker,
            profile=self.profile,
        )
        self.dashboard_data = DashboardData(store=self.store, binance_canary=self.binance_canary)
        self.server = dashboard_server
        if self.server is None:
            factory = dashboard_server_factory or DashboardServer
            self.server = factory(self.profile.host, self.profile.port, data=self.dashboard_data)


    @property
    def root(self) -> str:
        return self.profile.worktree_root

    @property
    def db_path(self) -> str:
        return self.profile.db_path

    @property
    def lock_path(self) -> str:
        return self.profile.lock_path

    @property
    def log_path(self) -> str:
        return self.profile.log_path

    @property
    def stop_path(self) -> str:
        return self.profile.stop_path

    @property
    def pid_path(self) -> str:
        return self.profile.pid_path

    def _owner_document(self) -> str:
        return json.dumps({"pid": self.owner.pid, "runtime_identity": self.runtime_identity, "owner_token": self.owner.token}, sort_keys=True, separators=(",", ":"))

    def _write_owned_file(self, path: str, content: str, *, exclusive: bool = False) -> None:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        if exclusive:
            fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            self._lock_fd = fd
            self._owned_paths.add(path)
            os.write(fd, content.encode("utf-8"))
            os.fsync(fd)
        else:
            # Track the path before writing so a short write or fsync-like
            # failure cannot leave a malformed PID marker behind.
            self._owned_paths.add(path)
            Path(path).write_text(content, encoding="utf-8")

    def _acquire(self) -> None:
        """Acquire lock/PID as one rollback-safe ownership transaction."""
        try:
            self._write_owned_file(self.lock_path, self._owner_document(), exclusive=True)
            self._write_owned_file(self.pid_path, self._owner_document())
            Path(self.log_path).parent.mkdir(parents=True, exist_ok=True)
            with open(self.log_path, "a", encoding="utf-8") as handle:
                handle.write(self._owner_document() + "\n")
        except BaseException:
            try:
                self._release_lock()
            finally:
                self._lock_fd = None
                self._started = False
            raise

    def _owns_file(self, path: str) -> bool:
        try:
            document = json.loads(Path(path).read_text(encoding="utf-8"))
        except (FileNotFoundError, OSError, ValueError, json.JSONDecodeError):
            return False
        try:
            pid = int(document.get("pid", -1))
        except (TypeError, ValueError):
            return False
        return (
            document.get("runtime_identity") == self.runtime_identity
            and document.get("owner_token") == self.owner.token
            and pid == self.owner.pid
        )

    def _release_owned_path(self, path: str) -> None:
        owned = path in self._owned_paths or self._owns_file(path)
        self._owned_paths.discard(path)
        if not owned:
            return
        try:
            Path(path).unlink()
        except FileNotFoundError:
            return
        except OSError:
            # A fault-injected Path.unlink must not strand the marker when
            # the lower-level unlink is still available.
            try:
                os.unlink(path)
            except (FileNotFoundError, OSError):
                pass

    def _release_lock(self) -> None:
        fd = self._lock_fd
        self._lock_fd = None
        if fd is not None:
            try:
                os.close(fd)
            except OSError:
                pass
        self._release_owned_path(self.lock_path)
        self._release_owned_path(self.pid_path)
    def _clear_stale_stop(self) -> None:
        try:
            document = json.loads(Path(self.stop_path).read_text(encoding="utf-8"))
        except (FileNotFoundError, OSError, ValueError, json.JSONDecodeError):
            return
        if document.get("runtime_identity") == self.runtime_identity:
            try:
                Path(self.stop_path).unlink()
            except FileNotFoundError:
                pass
            except OSError:
                # Preserve the cleanup failure for the transactional caller,
                # but make a best effort to remove the owned stale marker.
                try:
                    os.unlink(self.stop_path)
                except (FileNotFoundError, OSError):
                    pass
                raise

    def _run_worker(self, once: bool) -> None:
        try:
            runner = getattr(self.worker, "run", None)
            if callable(runner):
                runner(max_cycles=1 if once else None)
            elif once:
                cycle = getattr(self.worker, "cycle", None)
                if callable(cycle):
                    cycle()
        except BaseException as exc:
            self._worker_error = exc
            if not once:
                self.stop_event.set()

    def start(self, *, once: bool = False) -> "BinanceDevelopmentRuntime":
        if self._started:
            return self
        if self._closed:
            raise RuntimeError("runtime is closed")
        self._clear_stale_stop()
        self._acquire()
        try:
            self.stop_event.clear()
            starter = getattr(self.server, "start", None)
            if callable(starter):
                starter()
            self._started = True
            if once:
                self._run_worker(True)
            else:
                self._worker_thread = threading.Thread(target=self._run_worker, args=(False,), name=self.runtime_identity, daemon=True)
                self._worker_thread.start()
            return self
        except BaseException:
            self.stop()
            raise

    def _stop_marker_owned(self) -> bool:
        try:
            document = json.loads(Path(self.stop_path).read_text(encoding="utf-8"))
        except (FileNotFoundError, OSError, ValueError, json.JSONDecodeError):
            return False
        return document.get("runtime_identity") == self.runtime_identity

    def _write_stop_marker(self) -> bool:
        """Publish this runtime's stop marker without replacing another one."""
        path = Path(self.stop_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            return self._owns_file(self.stop_path)

        temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
        fd: int | None = None
        try:
            fd = os.open(str(temporary), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            payload = self._owner_document().encode("utf-8")
            os.write(fd, payload)
            os.fsync(fd)
            os.close(fd)
            fd = None
            try:
                # Linking a fully written temporary file publishes it atomically
                # and, unlike replace(), cannot overwrite a competing marker.
                os.link(str(temporary), str(path))
            except FileExistsError:
                return self._owns_file(self.stop_path)
            return True
        finally:
            if fd is not None:
                try:
                    os.close(fd)
                except OSError:
                    pass
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass

    def stop(self) -> dict[str, Any]:
        self.stop_event.set()
        try:
            self._write_stop_marker()
        except OSError:
            pass
        if not self._started and self._closed:
            return self.status()
        stopper = getattr(self.worker, "stop", None)
        if callable(stopper):
            try:
                stopper()
            except Exception:
                pass
        if self._worker_thread is not None and self._worker_thread is not threading.current_thread():
            self._worker_thread.join(timeout=2)
        server_stopper = getattr(self.server, "stop", None) or getattr(self.server, "close", None)
        if callable(server_stopper):
            try:
                server_stopper()
            except Exception:
                pass
        if not self._closed:
            closer = getattr(self.store, "close", None)
            if callable(closer):
                closer()
            self._closed = True
        self._release_lock()
        self._started = False
        return self.status()

    close = stop

    def serve_forever(self) -> None:
        if not self._started:
            self.start()
        while not self.stop_event.wait(0.25):
            if self._stop_marker_owned():
                break
        if self._stop_marker_owned():
            self.stop()

    def status(self) -> dict[str, Any]:
        worker_status: Any = {}
        status_method = getattr(self.worker, "status", None)
        if callable(status_method):
            try:
                worker_status = status_method()
            except Exception as exc:
                worker_status = {"status": "ERROR", "error": type(exc).__name__}
        url = getattr(self.server, "url", None)
        if callable(url):
            url = url()
        profile_projection = self.profile.projection()
        return {
            "status": "RUNNING" if self._started else ("STOPPED" if self._closed else "READY"),
            "runtime_identity": profile_projection["runtime_identity"],
            "url": url,
            "profile": profile_projection,
            "paths": {"db": self.db_path, "log": self.log_path, "lock": self.lock_path, "stop": self.stop_path, "pid": self.pid_path},
            "worker": worker_status,
            "paper_only": profile_projection["environment"] == BinanceSpotEnvironment.PAPER.value,
            "live_execution": False,
            "polymarket_transport": "DISABLED",
            "hermes": "DISABLED",
            "research_node": "DISABLED",
            "operator_control_plane": "DISABLED",
            "worker_error": None if self._worker_error is None else type(self._worker_error).__name__,
        }

def _testnet_credential_ref() -> BinanceCredentialRef:
    """Return the one keyring identity accepted by the TESTNET runtime."""

    return BinanceCredentialRef(
        instance="binance-testnet",
        environment=BINANCE_SPOT_TESTNET,
        namespace="AXIOM-BINANCE-SPOT-TESTNET",
    )


def _safe_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return {str(key): item for key, item in value.items()}
    if hasattr(value, "as_dict") and callable(value.as_dict):
        try:
            return _safe_mapping(value.as_dict())
        except Exception:
            return {}
    if hasattr(value, "to_dict") and callable(value.to_dict):
        try:
            return _safe_mapping(value.to_dict())
        except Exception:
            return {}
    return {}

_TESTNET_PUBLIC_ORIGIN = "https://testnet.binance.vision"


def _validate_testnet_provider(provider: Any) -> None:
    """Reject a market provider that can escape the TESTNET public origin.

    Every exposed endpoint identity is checked independently.  Looking at only
    ``origin`` (or accepting the first available alias) lets a contradictory
    ``base_url`` route requests to mainnet.
    """
    if provider is None:
        return
    for name in ("origin", "base_url", "endpoint"):
        try:
            value = getattr(provider, name, None)
        except BaseException as exc:
            raise ValueError("TESTNET provider identity unavailable") from exc
        if value is not None and str(value).rstrip("/") != _TESTNET_PUBLIC_ORIGIN:
            raise ValueError("TESTNET provider origin must be https://testnet.binance.vision")
    for source_name, source in (
        ("provider environment", getattr(provider, "environment", None)),
        ("provider profile", getattr(provider, "profile", None)),
    ):
        if isinstance(source, Mapping):
            value = source.get("environment")
        else:
            value = getattr(source, "environment", source)
        value = getattr(value, "value", value)
        if str(value).upper() not in {BINANCE_SPOT_TESTNET, "TESTNET"}:
            raise ValueError(f"TESTNET {source_name} mismatch")


def _deadline_expired(deadline_monotonic: float | None) -> bool:
    if deadline_monotonic is None:
        return False
    try:
        return time.monotonic() >= float(deadline_monotonic)
    except (TypeError, ValueError, OverflowError):
        return True


def _is_deadline_exception(exc: BaseException) -> bool:
    text = str(exc).upper()
    return bool(
        getattr(exc, "deadline_expired", False)
        or text == "AUTO_DEADLINE_EXPIRED"
        or "AUTO_DEADLINE_EXPIRED" in text
        or "DEADLINE_EXPIRED" in text
    )

def _call_with_optional_deadline(method: Any, *, deadline_monotonic: float | None = None, **kwargs: Any) -> Any:
    """Call a production/fake method while preserving the deadline contract."""
    if not callable(method):
        raise TypeError("required operational method is unavailable")
    try:
        signature = inspect.signature(method)
    except (TypeError, ValueError):
        signature = None
    values = dict(kwargs)
    if deadline_monotonic is not None:
        values["deadline_monotonic"] = deadline_monotonic
    if signature is not None and not any(
        parameter.kind is inspect.Parameter.VAR_KEYWORD
        for parameter in signature.parameters.values()
    ):
        values = {key: value for key, value in values.items() if key in signature.parameters}
    return method(**values)


def _component_environment_sources(component: Any) -> list[str]:
    values: list[str] = []
    if component is None:
        return values
    for source in (
        getattr(component, "environment", None),
        getattr(component, "profile", None),
    ):
        if source is None:
            continue
        if isinstance(source, Mapping):
            value = source.get("environment")
        else:
            value = getattr(source, "environment", source)
        value = getattr(value, "value", value)
        values.append(str(value).strip().upper())
    return values


def _store_identity(store: Any) -> str | None:
    """Return lexical path, connection path, and SQLite file identity.

    Store identity intentionally retains the caller's raw canonical path
    instead of resolving symlinks.  A symlink or hardlink therefore cannot
    masquerade as the runtime's dedicated database merely because it points
    at the same inode.
    """
    if store is None:
        return None
    connection = getattr(store, "connection", None)
    if not isinstance(connection, sqlite3.Connection):
        connection = getattr(store, "_conn", None)
    filename = ""
    if isinstance(connection, sqlite3.Connection):
        try:
            row = connection.execute("PRAGMA database_list").fetchone()
            filename = str(row[2] or "") if row is not None else ""
        except Exception:
            filename = ""
    raw_value = str(getattr(store, "path", "") or "")
    if raw_value in {":memory:", ""}:
        if raw_value == ":memory:":
            return f"memory:{id(connection)}"
        raw_value = filename
    if not raw_value or raw_value == ":memory:":
        return f"memory:{id(connection)}" if connection is not None else None
    try:
        raw_path = os.path.normcase(os.path.abspath(raw_value))
        connection_path = (
            os.path.normcase(os.path.abspath(filename))
            if filename and filename != ":memory:"
            else raw_path
        )
        stat_path = filename if filename and filename != ":memory:" else raw_value
        stat = os.stat(stat_path)
        return f"{raw_path}|{connection_path}|{int(stat.st_dev)}:{int(stat.st_ino)}"
    except (OSError, TypeError, ValueError):
        return None

def _profile_matches(actual: Any, expected: BinanceRuntimeProfile) -> bool:
    if actual is None:
        return False
    if isinstance(actual, BinanceRuntimeProfile):
        return actual == expected
    if isinstance(actual, Mapping):
        expected_projection = expected.projection()
        required = ("environment", "runtime_identity", "db_path", "worktree_root", "host", "port")
        for key in required:
            if key not in actual:
                return False
            actual_value = getattr(actual.get(key), "value", actual.get(key))
            expected_value = expected.worktree_root if key == "worktree_root" else getattr(expected_projection.get(key), "value", expected_projection.get(key))
            if key in {"db_path", "worktree_root"}:
                try:
                    actual_value = os.path.normcase(os.path.realpath(os.path.abspath(str(actual_value))))
                    expected_value = os.path.normcase(os.path.realpath(os.path.abspath(str(expected_value))))
                except (OSError, TypeError, ValueError):
                    pass
            if str(actual_value) != str(expected_value):
                return False
        return True
    return False


_TESTNET_UNSET = object()

def _validate_injected_testnet_component(
    component: Any,
    label: str,
    *,
    profile: BinanceRuntimeProfile | None = None,
    venue: Any | None = None,
    credential_ref: BinanceCredentialRef | None = None,
    credentials: Any | None = None,
    provider: Any | None = None,
    qualification: Any | None = None,
    expected_store: Any | None = None,
    expected_credential_store: Any | None = None,
    expected_execution: Any = _TESTNET_UNSET,
    expected_worker: Any = _TESTNET_UNSET,
    expected_gate: Any = _TESTNET_UNSET,
) -> None:
    """Validate direct and factory-produced TESTNET execution graph seams."""
    if component is None:
        return
    concrete = (
        (label == "execution" and type(component) is BinanceExecutionService)
        or (label == "worker" and type(component) is BinanceAutonomousWorker)
        or (label == "gate" and type(component) is BinanceTestnetGateService)
        or (label == "control" and type(component) is BinanceTestnetControlPlane)
    )
    if not concrete and getattr(component, "strict_testnet", None) is not True:
        raise ValueError(f"TESTNET {label} requires strict_testnet=True")
    environments = _component_environment_sources(component)
    if label == "control" and not environments:
        environments = _component_environment_sources(getattr(component, "gate", None))
    if label not in {"research", "qualification"} and (
        not environments or any(value not in {BINANCE_SPOT_TESTNET, "TESTNET"} for value in environments)
    ):
        raise ValueError(f"TESTNET {label} environment mismatch")
    if profile is not None and label not in {"research", "qualification"}:
        actual_profile = getattr(component, "profile", None)
        if actual_profile is None and label == "control":
            actual_profile = getattr(getattr(component, "gate", None), "profile", None)
        if not _profile_matches(actual_profile, profile):
            raise ValueError(f"TESTNET {label} profile mismatch")
    if label in {"execution", "worker", "control", "gate"} and expected_store is not None:
        actual_store = getattr(component, "store", None)
        if type(actual_store) is not AxiomStore:
            raise ValueError(f"TESTNET {label} store mismatch")
        if _store_identity(actual_store) != _store_identity(expected_store):
            raise ValueError(f"TESTNET {label} store mismatch")
    if label in {"research", "qualification"}:
        if type(component) is not BinanceCryptoQualificationService:
            raise ValueError("TESTNET research must use the canonical qualification service")
        actual_store = getattr(component, "store", None)
        if expected_store is not None and (
            type(actual_store) is not AxiomStore
            or _store_identity(actual_store) != _store_identity(expected_store)
        ):
            raise ValueError("TESTNET research store mismatch")
        return
    if label == "execution":
        policy = getattr(component, "entry_policy_hash", None)
        authorizer = getattr(component, "entry_binding_authorizer", None)
        if (
            policy != "BINANCE_TESTNET_CURRENT_QUALIFICATION_V1"
            or not callable(authorizer)
            or getattr(authorizer, "_axiom_testnet_runtime_authorizer", False) is not True
            or qualification is None
            or getattr(authorizer, "_axiom_testnet_qualification", None) is not qualification
        ):
            raise ValueError("TESTNET execution dynamic policy unavailable")
        if getattr(component, "risk_envelope", None) is not DEFAULT_BINANCE_RISK_ENVELOPE:
            raise ValueError("TESTNET execution risk envelope mismatch")
        if venue is not None:
            actual_venue = getattr(component, "venue", None)
            if actual_venue is not venue:
                raise ValueError("TESTNET execution venue mismatch")
        actual_venue = getattr(component, "venue", None)
        if type(actual_venue) is not BinanceSpotRESTClient:
            raise ValueError("TESTNET execution requires the exact REST venue")
        if actual_venue.environment.value != BINANCE_SPOT_TESTNET or actual_venue.origin != _TESTNET_PUBLIC_ORIGIN:
            raise ValueError("TESTNET execution venue identity mismatch")
        if credentials is not None and credential_fingerprint(actual_venue.credentials) != credential_fingerprint(credentials):
            raise ValueError("TESTNET execution venue credentials mismatch")
        if credential_ref is not None:
            if type(getattr(component, "credential_ref", None)) is not BinanceCredentialRef or getattr(component, "credential_ref") != credential_ref:
                raise ValueError("TESTNET execution credential ref mismatch")
        if expected_credential_store is not None and getattr(component, "credential_store", None) is not expected_credential_store:
            raise ValueError("TESTNET execution credential store mismatch")
    elif label == "worker":
        capability = getattr(component, "supports_persisted_strategy", None)
        if not callable(capability) or capability() is not True or not callable(getattr(component, "cycle", None)):
            raise ValueError("TESTNET worker dynamic capability unavailable")
        linked_execution = getattr(component, "execution", None)
        if linked_execution is None:
            raise ValueError("TESTNET worker execution link missing")
        if expected_execution is not None and linked_execution is not expected_execution:
            raise ValueError("TESTNET worker execution link mismatch")
        if qualification is not None and getattr(component, "qualification", None) is not qualification:
            raise ValueError("TESTNET worker qualification link mismatch")
        linked_provider = getattr(component, "_provider", None)
        collector = getattr(component, "_collector", None)
        if collector is not None:
            if not hasattr(collector, "provider") or provider is None or collector.provider is not provider:
                raise ValueError("TESTNET worker collector provider link mismatch")
        elif provider is not None and linked_provider is not provider:
            raise ValueError("TESTNET worker provider link mismatch")
    elif label == "gate":
        venue_sources = [
            getattr(component, "_venue", None),
            getattr(component, "venue", None),
        ]
        venues = [value for value in venue_sources if value is not None]
        if venues and any(value is not venues[0] for value in venues[1:]):
            raise ValueError("TESTNET gate venue identity conflict")
        actual_venue = venues[0] if venues else None
        if actual_venue is None:
            if credentials is not None or venue is not None:
                raise ValueError("TESTNET gate venue identity mismatch")
        elif type(actual_venue) is not BinanceSpotRESTClient:
            raise ValueError("TESTNET gate venue identity mismatch")
        elif actual_venue.environment.value != BINANCE_SPOT_TESTNET or actual_venue.origin != _TESTNET_PUBLIC_ORIGIN:
            raise ValueError("TESTNET gate venue identity mismatch")
        if credentials is None and (
            actual_venue is not None
            or getattr(component, "credentials", None) is not None
            or getattr(getattr(component, "_venue", None), "credentials", None) is not None
        ):
            raise ValueError("TESTNET gate credentials/venue require configured keyring")
        if credentials is not None and credential_fingerprint(actual_venue.credentials) != credential_fingerprint(credentials):
            raise ValueError("TESTNET gate venue credentials mismatch")
        if venue is not None and actual_venue is not venue:
            raise ValueError("TESTNET gate venue mismatch")
        store = getattr(component, "credential_store", None)
        if expected_credential_store is not None and store is not expected_credential_store:
            raise ValueError("TESTNET gate credential store mismatch")
        if credential_ref is not None and getattr(store, "ref", None) != credential_ref:
            raise ValueError("TESTNET gate credential ref mismatch")
        if expected_store is not None:
            actual_store = getattr(component, "store", None)
            if actual_store is None or _store_identity(actual_store) != _store_identity(expected_store):
                raise ValueError("TESTNET gate store mismatch")
        actual_credentials = getattr(component, "credentials", None)
        if actual_credentials is not None and credentials is not None and credential_fingerprint(actual_credentials) != credential_fingerprint(credentials):
            raise ValueError("TESTNET gate credentials mismatch")
    elif label == "control":
        linked_gate = getattr(component, "gate", None)
        if expected_gate is not _TESTNET_UNSET and linked_gate is not expected_gate:
            raise ValueError("TESTNET control gate link mismatch")
        if expected_execution is not _TESTNET_UNSET and getattr(component, "execution", None) is not expected_execution:
            raise ValueError("TESTNET control execution link mismatch")
        if expected_worker is not _TESTNET_UNSET and getattr(component, "worker", None) is not expected_worker:
            raise ValueError("TESTNET control worker link mismatch")
        if linked_gate is not None:
            _validate_injected_testnet_component(
                linked_gate,
                "gate",
                profile=profile,
                venue=venue,
                credential_ref=credential_ref,
                credentials=credentials,
                provider=provider,
                expected_store=expected_store,
                expected_credential_store=expected_credential_store,
            )
        if venue is not None and linked_gate is not None:
            gate_venue = getattr(linked_gate, "_venue", getattr(linked_gate, "venue", None))
            if gate_venue is not venue:
                raise ValueError("TESTNET control gate venue mismatch")

class BinanceTestnetRuntime(BinanceDevelopmentRuntime):
    """Dedicated TESTNET operator/runtime boundary.

    Unlike :class:`BinanceDevelopmentRuntime`, this class owns an isolated
    TESTNET execution graph.  Configured credentials construct the fixed
    TESTNET public provider, authenticated venue, qualification service,
    execution service, and autonomous worker; missing credentials construct
    none of those network/execution objects.
    """

    TESTNET_CONFIRMATION = "ENABLE BINANCE TESTNET AUTO CANARY"
    PROBE_CONFIRMATION = "RUN BINANCE TESTNET EXECUTION PROBE"
    MIN_AUTO_WINDOW_SECONDS = 30
    MAX_AUTO_WINDOW_SECONDS = 900

    def __init__(
        self,
        worktree_root: str | os.PathLike[str] | None = None,
        *,
        profile: BinanceRuntimeProfile | None = None,
        credential_store: Any | None = None,
        keyring_backend: Any | None = None,
        credentials: Any | None = None,
        venue: Any | None = None,
        gate: Any | None = None,
        execution: Any | None = None,
        worker: Any | None = None,
        strategy: Any | None = None,
        strategy_components: Mapping[str, Any] | None = None,
        clock: Callable[[], Any] | None = None,
        store: Any | None = None,
        store_factory: Callable[[str], Any] | None = None,
        gate_factory: Callable[..., Any] | None = None,
        execution_factory: Callable[..., Any] | None = None,
        worker_factory: Callable[..., Any] | None = None,
        control: Any | None = None,
        control_factory: Callable[..., Any] | None = None,
        dashboard_server: Any | None = None,
        dashboard_server_factory: Callable[..., Any] | None = None,
    ) -> None:
        root = canonical_checkout_root(worktree_root)
        if profile is None:
            profile = BinanceRuntimeProfile.testnet(root)
        elif not isinstance(profile, BinanceRuntimeProfile):
            raise TypeError("profile must be BinanceRuntimeProfile")
        if profile.environment is not BinanceSpotEnvironment.BINANCE_SPOT_TESTNET:
            raise ValueError("Binance testnet runtime requires the strict TESTNET profile")
        if os.path.normcase(os.path.realpath(profile.worktree_root)) != root:
            raise ValueError("profile root does not match the development checkout")
        if profile.host != DEFAULT_HOST or profile.port != 8082:
            raise ValueError("Binance testnet runtime must use 127.0.0.1:8082")

        expected_credential_ref = _testnet_credential_ref()
        # Validate the injected store identity before loading credentials or
        # constructing any SQLite stores, venues, or workers.
        if credential_store is None:
            credential_store = BinanceCredentialStore(
                ref=expected_credential_ref,
                keyring_backend=keyring_backend,
            )
        try:
            actual_credential_ref = credential_store.ref
        except BaseException as exc:
            raise ValueError("TESTNET runtime requires the exact credential ref") from exc
        if (
            type(actual_credential_ref) is not BinanceCredentialRef
            or actual_credential_ref != expected_credential_ref
        ):
            raise ValueError("TESTNET runtime requires the exact credential ref")
        if credentials is not None:
            raise ValueError("TESTNET credentials must come from the exact credential store")
        # Explicitly identified injected components are never allowed to
        # change this runtime's environment.  Concrete runtime classes are
        # trusted only after their complete dependency identity is checked;
        # non-concrete doubles must opt in with strict_testnet=True.

        self.profile = profile
        self.runtime_identity = profile.runtime_identity
        self.environment = profile.environment.value
        self.owner = _RuntimeOwner(os.getpid(), uuid.uuid4().hex)
        self.stop_event = threading.Event()
        self._lock_fd: int | None = None
        self._owned_paths: set[str] = set()
        self._started = False
        self._closed = False
        self._worker_thread: threading.Thread | None = None
        self._worker_error: BaseException | None = None
        self.clock = clock or (lambda: datetime.now(UTC))
        self._last_auto_evidence: dict[str, Any] | None = None
        self._last_status: dict[str, Any] | None = None
        self._owned_resources: list[Any] = []
        self._stopped_resources: set[int] = set()
        self._store_connections: set[int] = set()
        # A keyring backend is an injectable test seam, while production
        # construction always resolves the OS keyring through this ref.
        self.credential_store = credential_store
        loaded_credentials: Any | None = None
        try:
            loader = getattr(credential_store, "load", None)
            if callable(loader):
                loaded_credentials = loader()
        except Exception as exc:
            raise ValueError("TESTNET credential store load failed") from exc
        self.credentials = loaded_credentials
        self.credentials_configured = loaded_credentials is not None
        self._credential_fingerprint = credential_fingerprint(loaded_credentials)
        if not self.credentials_configured and venue is not None:
            raise ValueError("TESTNET venue requires configured credentials")
        self._credential_state_error: str | None = None

        def require_store(value: Any) -> Any:
            if type(value) is not AxiomStore:
                raise TypeError("TESTNET runtime stores require the canonical AxiomStore")
            connection = getattr(value, "connection", None)
            if not isinstance(connection, sqlite3.Connection):
                raise TypeError("TESTNET runtime stores must expose a SQLite connection")
            expected_path = os.path.normcase(os.path.abspath(profile.db_path))
            raw_path = str(getattr(value, "path", "") or "")
            if raw_path and raw_path not in {":memory:", ""}:
                raw_canonical = os.path.normcase(os.path.abspath(raw_path))
                if raw_canonical != expected_path:
                    raise ValueError("TESTNET runtime store path mismatch")
            actual_identity = _store_identity(value)
            try:
                stat = os.stat(expected_path)
                expected_identity = (
                    f"{expected_path}|{expected_path}|"
                    f"{int(stat.st_dev)}:{int(stat.st_ino)}"
                )
            except OSError as exc:
                raise ValueError("TESTNET runtime store identity unavailable") from exc
            if actual_identity != expected_identity:
                raise ValueError("TESTNET runtime store identity mismatch")
            connection_id = id(connection)
            if connection_id in self._store_connections:
                raise ValueError("TESTNET runtime stores require distinct SQLite connections")
            self._store_connections.add(connection_id)
            return value

        def create_store() -> Any:
            value = (store_factory or AxiomStore)(profile.db_path)
            self._track_resource(value)
            return require_store(value)
        try:
            if store is not None:
                self.store = require_store(store)
                self._track_resource(self.store)
            else:
                self.store = create_store()
            self.dashboard_store = self.store
            self.base_store = self.dashboard_store
            self.gate_store = create_store()
            self.qualification_store = create_store()
            self.execution_store = create_store()
            self.strategy_store = self.execution_store
            self.operator_store = create_store()
            self.audit_store = self.operator_store
        except BaseException:
            self._close_resources()
            self._release_lock()
            raise
        components = dict(strategy_components or {})
        self.provider: Any | None = None
        self.collector: Any | None = None
        self.universe_loader = _CurrentPersistedUniverseLoader(self.dashboard_store)
        self.venue: Any | None = None
        qualification_override = components.get("qualification")
        if qualification_override is not None:
            try:
                if type(qualification_override) is not BinanceCryptoQualificationService:
                    raise ValueError("TESTNET qualification override must be the canonical service")
                if _store_identity(getattr(qualification_override, "store", None)) != _store_identity(self.qualification_store):
                    raise ValueError("TESTNET qualification store mismatch")
            except BaseException:
                self._close_resources()
                raise
        self.execution = execution if self.credentials_configured else None
        self.qualification = qualification_override
        self.strategy = strategy if strategy is not None else getattr(worker, "strategy", None)
        self._strategy_configured = self.strategy is not None
        self._track_resource(getattr(self.execution, "store", None))
        self._track_resource(self.execution)
        self._track_resource(self.qualification)
        self._track_resource(self.strategy)

        try:
            if self.credentials_configured:
                if self.provider is None:
                    self.provider = BinanceAdapter(
                        base_url=_TESTNET_PUBLIC_ORIGIN,
                        timeout=float(components.get("provider_timeout", 5.0)),
                        clock=self.clock,
                    )
                    self.provider.environment = BINANCE_SPOT_TESTNET
                    self.provider.profile = profile
                resolved_venue = venue
                if resolved_venue is None:
                    resolved_venue = BinanceSpotRESTClient(
                        profile,
                        loaded_credentials,
                        clock=self.clock,
                        timeout=float(components.get("venue_timeout", 5.0)),
                        recv_window=5000,
                    )
                _validate_runtime_venue(
                    profile.environment,
                    resolved_venue,
                    loaded_credentials,
                )
                self.venue = self._track_resource(resolved_venue)
                snapshot = self.universe_loader.load()
                self.collector = components.get("collector")
                if self.collector is None and snapshot is not None:
                    self.collector = BoundedBinanceMarketCollector(
                        self.provider,
                        snapshot,
                        max_workers=4,
                        depth=20,
                        clock=self.clock,
                    )
                if self.collector is not None:
                    if not hasattr(self.collector, "provider") or self.collector.provider is not self.provider:
                        raise ValueError("TESTNET collector provider mismatch")
                self._track_resource(self.collector)
            if self.qualification is None:
                self.qualification = BinanceCryptoQualificationService(
                    self.qualification_store,
                    venue="BINANCE_SPOT",
                    adapter_version=type(self.provider).__name__ if self.provider is not None else "binance-testnet",
                    clock=self.clock,
                )
                self._track_resource(self.qualification)
            if (
                type(self.qualification) is not BinanceCryptoQualificationService
                or _store_identity(getattr(self.qualification, "store", None)) != _store_identity(self.qualification_store)
            ):
                raise ValueError("TESTNET qualification must use the canonical research store")

            if self.credentials_configured and self.execution is None:
                factory = execution_factory or BinanceExecutionService

                def entry_binding_authorizer(signal: Mapping[str, Any]) -> tuple[bool, str]:
                    """Authorize ENTRY against the direct current-selection row."""
                    current_method = getattr(self.qualification, "current_selection", None)
                    if not callable(current_method):
                        return False, "QUALIFICATION_SELECTION_UNAVAILABLE"
                    try:
                        # ``current_selection`` is the authoritative row, not
                        # the status wrapper returned by ``status()``.
                        selected = _safe_mapping(current_method())
                    except Exception:
                        return False, "QUALIFICATION_SELECTION_UNAVAILABLE"
                    if str(selected.get("selection_status", "")).upper() != "CURRENT":
                        return False, "QUALIFICATION_SELECTION_NOT_CURRENT"
                    if selected.get("selection_valid") is False:
                        return False, "QUALIFICATION_SELECTION_NOT_CURRENT"
                    if selected.get("qualified") is False:
                        return False, "QUALIFICATION_SELECTION_NOT_CURRENT"

                    candidate_id = str(selected.get("candidate_id") or "").strip()
                    try:
                        selected_symbol = _symbol(selected.get("symbol"))
                    except ValueError:
                        return False, "QUALIFICATION_SELECTION_NOT_CURRENT"
                    source = selected.get("binding")
                    if not candidate_id or not isinstance(source, Mapping):
                        return False, "SOURCE_BINDING_MISSING"
                    source_copy = dict(source)
                    try:
                        if (
                            str(source_copy.get("candidate_id") or "") != candidate_id
                            or _symbol(source_copy.get("symbol")) != selected_symbol
                        ):
                            return False, "SOURCE_BINDING_IDENTITY_MISMATCH"
                    except ValueError:
                        return False, "SOURCE_BINDING_IDENTITY_MISMATCH"
                    selected_source_hash = str(
                        selected.get("source_binding_hash")
                        or selected.get("binding_hash")
                        or source_copy.get("binding_hash")
                        or ""
                    )
                    try:
                        from .binance_research import project_testnet_execution_binding

                        successor, source_hash = project_testnet_execution_binding(source_copy)
                    except Exception:
                        return False, "SOURCE_BINDING_PROJECTION_INVALID"
                    if selected_source_hash and selected_source_hash != str(source_hash):
                        return False, "SOURCE_BINDING_HASH_MISMATCH"
                    successor_dict = (
                        successor.as_dict()
                        if hasattr(successor, "as_dict")
                        else _safe_mapping(successor)
                    )
                    expected_hash = str(getattr(successor, "binding_hash", "") or "")
                    if not expected_hash:
                        return False, "SUCCESSOR_BINDING_HASH_MISSING"
                    # The hash is part of the projected successor identity even
                    # though CryptoExecutionBinding.as_dict() omits it.
                    expected_binding = {**successor_dict, "binding_hash": expected_hash}

                    try:
                        signal_candidate = str(signal.get("candidate_id") or "")
                        signal_symbol = _symbol(signal.get("symbol"))
                    except ValueError:
                        return False, "QUALIFICATION_SELECTION_CHANGED"
                    if signal_candidate != candidate_id or signal_symbol != selected_symbol:
                        return False, "QUALIFICATION_SELECTION_CHANGED"
                    if str(signal.get("binding_hash") or "") != expected_hash:
                        return False, "SUCCESSOR_BINDING_MISMATCH"
                    if str(signal.get("source_binding_hash") or "") != str(source_hash):
                        return False, "SOURCE_BINDING_MISMATCH"

                    expected_qualification_hash = str(selected.get("qualification_hash") or "")
                    if not expected_qualification_hash or str(signal.get("qualification_hash") or "") != expected_qualification_hash:
                        return False, "QUALIFICATION_HASH_MISMATCH"
                    expected_immutable = selected.get("immutable_hashes")
                    expected_immutable = dict(expected_immutable) if isinstance(expected_immutable, Mapping) else {}
                    if signal.get("immutable_hashes") != expected_immutable:
                        return False, "IMMUTABLE_EVIDENCE_MISMATCH"
                    expected_strategy = selected.get("strategy_ref")
                    if not isinstance(expected_strategy, Mapping) or not expected_strategy:
                        expected_strategy = source_copy.get("strategy_ref")
                    if not isinstance(expected_strategy, Mapping) or not expected_strategy:
                        return False, "STRATEGY_REFERENCE_MISSING"
                    expected_strategy = dict(expected_strategy)
                    if signal.get("strategy_ref") != expected_strategy:
                        return False, "STRATEGY_REFERENCE_MISMATCH"
                    if signal.get("binding") != expected_binding:
                        return False, "SUCCESSOR_BINDING_MISMATCH"
                    if signal.get("source_binding") not in (None, source_copy):
                        return False, "SOURCE_BINDING_MISMATCH"

                    provenance = signal.get("provenance")
                    if not isinstance(provenance, Mapping):
                        return False, "ENTRY_PROVENANCE_REQUIRED"
                    required_provenance = {
                        "binding": expected_binding,
                        "source_binding_hash": str(source_hash),
                        "qualification_hash": expected_qualification_hash,
                        "immutable_hashes": expected_immutable,
                        "strategy_ref": expected_strategy,
                        "source_candidate_id": candidate_id,
                        "source_symbol": selected_symbol,
                    }
                    for key, expected in required_provenance.items():
                        if provenance.get(key) != expected:
                            return False, "ENTRY_PROVENANCE_" + key.upper() + "_MISMATCH"
                    lifecycle_stage = provenance.get("lifecycle_stage")
                    if lifecycle_stage not in (None, "", "FROZEN"):
                        return False, "ENTRY_PROVENANCE_LIFECYCLE_MISMATCH"
                    return True, "AUTHORIZED"
                entry_binding_authorizer._axiom_testnet_runtime_authorizer = True
                entry_binding_authorizer._axiom_testnet_qualification = self.qualification

                execution_kwargs: dict[str, Any] = {
                    "venue": self.venue,
                    "adapter": self.provider,
                    "profile": profile,
                    "environment": profile.environment,
                    "credentials": loaded_credentials,
                    "credential_store": credential_store,
                    "credential_ref": getattr(credential_store, "ref", _testnet_credential_ref()),
                    "owner_id": profile.runtime_identity,
                    "clock": self.clock,
                    # Testnet's authorizer and policy are runtime-owned.  A
                    # caller-supplied component map must not replace them.
                    "qualification": self.qualification,
                    "entry_binding_authorizer": entry_binding_authorizer,
                    "entry_policy_hash": "BINANCE_TESTNET_CURRENT_QUALIFICATION_V1",
                }
                if "binding" in components:
                    execution_kwargs["binding"] = components["binding"]
                self.execution = factory(self.execution_store, **execution_kwargs)
                self._track_resource(getattr(self.execution, "store", None))
                self._track_resource(self.execution)
            if self.execution is not None:
                _validate_injected_testnet_component(
                    self.execution,
                    "execution",
                    profile=profile,
                    venue=self.venue,
                    credential_ref=expected_credential_ref,
                    credentials=loaded_credentials,
                    provider=self.provider,
                    qualification=self.qualification,
                    expected_store=self.execution_store,
                    expected_credential_store=credential_store,
                    expected_execution=self.execution,
                )
                if type(self.execution) is BinanceExecutionService:
                    self.execution.strict_testnet = True
            self.worker = worker if self.credentials_configured else None
            if self.worker is not None:
                _validate_injected_testnet_component(
                    self.worker,
                    "worker",
                    profile=profile,
                    venue=self.venue,
                    credential_ref=expected_credential_ref,
                    credentials=loaded_credentials,
                    provider=self.provider,
                    qualification=self.qualification,
                    expected_store=self.execution_store,
                    expected_execution=self.execution,
                )
                if getattr(self.worker, "execution", None) is not self.execution:
                    raise ValueError("TESTNET worker execution link mismatch")
            self._track_resource(self.worker)
            if self.credentials_configured and self.worker is None:
                factory = worker_factory or BinanceAutonomousWorker
                worker_kwargs: dict[str, Any] = {
                    "collector": self.collector,
                    "provider": None if self.collector is not None else self.provider,
                    "universe": components.get("universe"),
                    "universe_loader": components.get("universe_loader") or self.universe_loader.load,
                    "qualification": self.qualification,
                    "strategy": self.strategy,
                    "interval_seconds": float(components.get("interval_seconds", DEFAULT_INTERVAL_SECONDS)),
                    "stop_event": self.stop_event,
                    "worker_id": profile.runtime_identity,
                    "profile": profile,
                    "clock": self.clock,
                }
                worker_kwargs.update(
                    {
                        key: value
                        for key, value in components.items()
                        if key not in {
                            "collector", "provider", "provider_timeout", "venue_timeout",
                            "universe", "universe_loader", "qualification", "interval_seconds",
                            "binding", "entry_binding_authorizer", "entry_policy_hash",
                            "clock", "profile", "stop_event", "worker_id", "strategy",
                            "execution",
                        }
                    }
                )
                self.worker = factory(self.execution_store, self.execution, **worker_kwargs)
                if self.worker is None:
                    raise ValueError("TESTNET worker factory returned no worker")
                _validate_injected_testnet_component(
                    self.worker,
                    "worker",
                    profile=profile,
                    venue=self.venue,
                    credential_ref=expected_credential_ref,
                    credentials=loaded_credentials,
                    provider=self.provider,
                    qualification=self.qualification,
                    expected_store=self.execution_store,
                    expected_execution=self.execution,
                )
                if getattr(self.worker, "execution", None) is not self.execution:
                    raise ValueError("TESTNET worker execution link mismatch")
                self._track_resource(self.worker)
        except BaseException:
            self._close_resources()
            self._release_lock()
            raise

        if gate is None:
            try:
                if gate_factory is None:
                    gate_factory = BinanceTestnetGateService
                gate = gate_factory(
                    self.gate_store,
                    profile=profile,
                    venue=self.venue,
                    credential_store=credential_store,
                    credentials=loaded_credentials,
                    clock=self.clock,
                )
            except BaseException:
                self._close_resources()
                self._release_lock()
                raise
        try:
            _validate_injected_testnet_component(
                gate,
                "gate",
                profile=profile,
                venue=self.venue,
                credential_ref=expected_credential_ref,
                credentials=loaded_credentials,
                expected_store=self.gate_store,
                expected_credential_store=credential_store,
                provider=self.provider,
                qualification=self.qualification,
            )
        except BaseException:
            self._close_resources()
            self._release_lock()
            raise
        self.gate = self._track_resource(gate)

        if control is None:
            try:
                if control_factory is None:
                    control_factory = BinanceTestnetControlPlane
                try:
                    control = control_factory(
                        self.gate,
                        execution=self.execution,
                        worker=self.worker,
                        clock=self.clock,
                        store=self.operator_store,
                    )
                except TypeError as first:
                    try:
                        control = control_factory(
                            self.gate,
                            execution=self.execution,
                            worker=self.worker,
                            clock=self.clock,
                        )
                    except TypeError:
                        raise first
            except BaseException:
                self._close_resources()
                self._release_lock()
                raise
        try:
            _validate_injected_testnet_component(
                control,
                "control",
                profile=profile,
                venue=self.venue,
                credential_ref=expected_credential_ref,
                credentials=loaded_credentials,
                provider=self.provider,
                qualification=self.qualification,
                expected_store=self.operator_store,
                expected_execution=self.execution,
                expected_worker=self.worker,
                expected_gate=self.gate,
            )
        except BaseException:
            self._close_resources()
            self._release_lock()
            raise
        self.control = self._track_resource(control)
        self.binance_testnet = control
        self.binance_canary = control
        try:
            self.dashboard_data = DashboardData(store=self.dashboard_store, binance_canary=control)
            self.server = dashboard_server
            if self.server is None:
                factory = dashboard_server_factory or DashboardServer
                self.server = factory(profile.host, profile.port, data=self.dashboard_data)
        except BaseException:
            self._close_resources()
            self._release_lock()
            raise
        self._track_resource(self.server)

    def _track_resource(self, resource: Any) -> Any:
        if resource is not None and all(existing is not resource for existing in self._owned_resources):
            self._owned_resources.append(resource)
        return resource

    def _close_resources(self) -> None:
        """Close constructed resources without allowing one failure to leak the rest."""
        resources = tuple(reversed(self._owned_resources))
        self._owned_resources.clear()
        for resource in resources:
            method = None
            for method_name in ("close", "shutdown", "release"):
                try:
                    candidate = getattr(resource, method_name, None)
                except BaseException:
                    continue
                if callable(candidate):
                    method = candidate
                    break
            if method is None and id(resource) not in self._stopped_resources:
                try:
                    candidate = getattr(resource, "stop", None)
                except BaseException:
                    candidate = None
                if callable(candidate):
                    method = candidate
            if method is not None:
                try:
                    method()
                except BaseException:
                    pass

    @property
    def root(self) -> str:
        return self.profile.worktree_root

    @property
    def db_path(self) -> str:
        return self.profile.db_path

    @property
    def lock_path(self) -> str:
        return self.profile.lock_path

    @property
    def log_path(self) -> str:
        return self.profile.log_path

    @property
    def stop_path(self) -> str:
        return self.profile.stop_path

    @property
    def pid_path(self) -> str:
        return self.profile.pid_path

    def _refresh_credentials(self, deadline_monotonic: float | None = None) -> bool:
        """Reload the exact keyring entry before every operational boundary."""
        loader = getattr(self.credential_store, "load", None)
        try:
            loaded = loader() if callable(loader) else None
            fingerprint = credential_fingerprint(loaded)
        except Exception:
            loaded = None
            fingerprint = credential_fingerprint(None)
            self._credential_state_error = "CREDENTIALS_UNAVAILABLE"
        if deadline_monotonic is not None and _deadline_expired(deadline_monotonic):
            self._credential_state_error = "AUTO_DEADLINE_EXPIRED"
            return False
        if loaded is None or fingerprint == credential_fingerprint(None):
            self.credentials = None
            self.credentials_configured = False
            if self._credential_state_error is None:
                self._credential_state_error = "CREDENTIALS_NOT_CONFIGURED"
            return False
        if fingerprint != self._credential_fingerprint:
            # Existing venue/execution objects retain the old secret.  Do not
            # re-use them after a keyring rotation; fail closed before calls.
            self.credentials = None
            self.credentials_configured = False
            self._credential_state_error = "CREDENTIALS_CHANGED"
            return False
        self.credentials = loaded
        self.credentials_configured = True
        self._credential_state_error = None
        return not (deadline_monotonic is not None and _deadline_expired(deadline_monotonic))

    def _credentials_projection(self) -> dict[str, Any]:
        projection = getattr(self.credential_store, "safe_projection", None)
        try:
            value = projection() if callable(projection) else None
        except Exception:
            value = None
        result = _safe_mapping(value)
        configured = bool(self.credentials_configured)
        result.update(
            {
                "environment": BINANCE_SPOT_TESTNET,
                "namespace": "AXIOM-BINANCE-SPOT-TESTNET",
                "instance": "binance-testnet",
                "configured": configured,
                "api_key_configured": configured,
                "api_secret_configured": configured,
                "secret_values_exposed": False,
            }
        )
        for key in ("api_key", "api_secret", "secret", "password", "token"):
            result.pop(key, None)
        return result

    def _gate_status(self, method_name: str, fallback_reason: str) -> dict[str, Any]:
        method = getattr(self.gate, method_name, None)
        if not callable(method):
            return {"status": "BLOCKED", "reason": fallback_reason, "environment": BINANCE_SPOT_TESTNET}
        try:
            value = method()
        except Exception as exc:
            return {"status": "BLOCKED", "reason": type(exc).__name__, "environment": BINANCE_SPOT_TESTNET}
        result = _safe_mapping(value)
        result.setdefault("status", "BLOCKED")
        result.setdefault("environment", BINANCE_SPOT_TESTNET)
        return result

    def _autonomous_projection(self, control_status: Mapping[str, Any]) -> dict[str, Any]:
        raw = control_status.get("autonomous")
        autonomous = _safe_mapping(raw)
        worker_status: dict[str, Any] = {}
        if self.worker is not None:
            status_method = getattr(self.worker, "status", None)
            if callable(status_method):
                try:
                    worker_status = _safe_mapping(status_method())
                except Exception as exc:
                    worker_status = {"status": "ERROR", "error": type(exc).__name__}
        autonomous_configured = bool(
            self.credentials_configured
            and self.execution is not None
            and self.worker is not None
        )
        if not autonomous_configured:
            autonomous["enabled"] = False
            autonomous["state"] = "BLOCKED"
            autonomous["blocked_reason"] = (
                self._credential_state_error or "AUTONOMOUS_COMPONENTS_NOT_CONFIGURED"
            )
        else:
            autonomous.setdefault("enabled", True)
            autonomous.setdefault("state", worker_status.get("status", "IDLE"))
            autonomous.setdefault("blocked_reason", None)
        autonomous.setdefault("selected_candidate", worker_status.get("selected_candidate"))
        autonomous.setdefault("current_signal", worker_status.get("current_signal"))
        autonomous.setdefault("no_trade_reason", worker_status.get("no_trade_reason"))
        autonomous.setdefault("risk_envelope", worker_status.get("risk_envelope"))
        autonomous.setdefault("bounded_window", None)
        return autonomous

    def status(self) -> dict[str, Any]:
        """Return a merged, secret-free projection without network calls."""

        if self._closed and self._last_status is not None:
            result = dict(self._last_status)
            result["status"] = "STOPPED"
            return result
        self._refresh_credentials()
        control_method = getattr(self.control, "status", None)
        try:
            control_status = _safe_mapping(control_method()) if callable(control_method) else {}
        except BaseException as exc:
            control_status = {"status": "BLOCKED", "reason": type(exc).__name__}
        connectivity = self._gate_status("connectivity_status", "NOT_CHECKED")
        validation = self._gate_status("validation_status", "NOT_CHECKED")
        probe = self._gate_status("probe_status", "NOT_STARTED")
        worker_status: dict[str, Any] = {}
        autonomous_configured = bool(
            self.credentials_configured
            and self.execution is not None
            and self.worker is not None
        )
        result: dict[str, Any] = {
            "title": "BINANCE SPOT TESTNET",
            "strict_testnet": True,
            "status": "RUNNING" if self._started else "READY",
            "runtime_identity": self.runtime_identity,
            "environment": BINANCE_SPOT_TESTNET,
            "profile": self.profile.projection(),
            "credentials": self._credentials_projection(),
            "connectivity": connectivity,
            "validation": validation,
            "probe": probe,
            "isolation": {
                "schema_namespace": "binance_testnet_*",
                "strategy_schema_namespace": "binance_execution_*/binance_auto_*",
                "strategy_ledgers_touched": False,
                "probe_tables_separate": True,
                "autonomous_activation": False,
                "polymarket_transport": "DISABLED",
            },
            "autonomous": self._autonomous_projection(control_status),
            "paths": {
                "db": self.db_path,
                "log": self.log_path,
                "lock": self.lock_path,
                "stop": self.stop_path,
                "pid": self.pid_path,
            },
            "worker": worker_status,
            "live_execution": False,
            "paper_only": False,
            "no_mainnet": True,
            "no_sapi": True,
            "operator_control_plane": "BINANCE_TESTNET",
            "worker_error": None if self._worker_error is None else type(self._worker_error).__name__,
        }
        # Control fields are useful to dashboard consumers, but never allowed
        # to replace the fixed environment/profile/gate truth projection.
        for key, value in control_status.items():
            if key not in result and key not in {"credentials", "connectivity", "validation", "probe", "environment", "profile"}:
                result[key] = value
        result["autonomous"] = self._autonomous_projection(control_status)
        if self._last_auto_evidence is not None:
            result["autonomous"]["bounded_window"] = dict(self._last_auto_evidence)
        self._last_status = result
        return result

    snapshot = status

    def list_actions(self, *, limit: int = 25) -> list[dict[str, Any]]:
        method = getattr(self.control, "list_actions", None)
        if not callable(method):
            return []
        try:
            value = method(limit=limit)
        except TypeError:
            value = method()
        return list(value) if isinstance(value, (list, tuple)) else []

    def action(self, action: str, payload: Mapping[str, Any] | None = None) -> dict[str, Any]:
        action_name = str(action or "").strip().upper()
        body = dict(payload or {}) if isinstance(payload, Mapping) else {}
        if action_name == "EXECUTION_PROBE" and body.get("confirmation") != self.PROBE_CONFIRMATION:
            return {"ok": False, "action": action_name, "reason": "EXACT_CONFIRMATION_REQUIRED"}
        if not self._refresh_credentials():
            return {
                "ok": False,
                "action": action_name,
                "reason": self._credential_state_error or "CREDENTIALS_NOT_CONFIGURED",
            }
        method = getattr(self.control, "action", None) or getattr(self.control, "execute", None)
        if not callable(method):
            return {"ok": False, "action": action_name, "reason": "CONTROL_UNAVAILABLE"}
        try:
            result = method(action_name, body)
        except TypeError:
            result = method(action_name, payload=body)
        output = _safe_mapping(result)
        reset_reason = str(output.get("reset_reason") or output.get("reason") or "")
        if reset_reason == "TESTNET_RESET_HISTORY_MISSING" or (
            output.get("reset_detected") is True and output.get("status") in {"UNKNOWN", "PAUSED", "RESET"}
        ):
            output.update(
                {
                    "status": "PAUSED",
                    "reason": "TESTNET_RESET_HISTORY_MISSING",
                    "reset_reason": "TESTNET_RESET_HISTORY_MISSING",
                    "reset_detected": True,
                    "control_path_paused": True,
                    "execution_paused": True,
                    "ok": False,
                    "action": action_name,
                }
            )
        return output

    def locked_action(
        self,
        action: str,
        payload: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Run one CLI gate/probe mutation while owning the TESTNET lock."""
        action_name = str(action or "").strip().upper()
        allowed = {
            "CONNECTIVITY_CHECK",
            "ORDER_VALIDATION_TEST",
            "EXECUTION_PROBE",
            "RECONCILE_PROBE",
        }
        if action_name not in allowed:
            raise ValueError("locked_action only supports TESTNET gate/probe actions")
        body = dict(payload or {}) if isinstance(payload, Mapping) else {}
        if action_name == "EXECUTION_PROBE" and body.get("confirmation") != self.PROBE_CONFIRMATION:
            return {"ok": False, "action": action_name, "reason": "EXACT_CONFIRMATION_REQUIRED"}
        self._acquire_testnet()
        try:
            if not self._refresh_credentials():
                return {
                    "ok": False,
                    "action": action_name,
                    "reason": self._credential_state_error or "CREDENTIALS_NOT_CONFIGURED",
                }
            return self.action(action_name, body)
        finally:
            self._release_lock()

    # Explicit alias for callers which prefer verb-first naming.
    action_locked = locked_action

    execute = action

    def _acquire_testnet(self) -> None:
        # Lock ownership is the first mutation.  A losing contender must not
        # clear a stop marker or touch its stop event.  Stale-stop cleanup is
        # part of this transaction and rolls the lock back on failure.
        self._acquire()
        try:
            self._clear_stale_stop()
            self.stop_event.clear()
        except BaseException:
            try:
                self._release_lock()
            finally:
                self._started = False
            raise

    def start(self) -> "BinanceTestnetRuntime":
        """Start only the dashboard lifecycle; autonomous work is explicit."""

        if self._closed:
            raise RuntimeError("runtime is closed")
        if self._started:
            return self
        self._acquire_testnet()
        try:
            starter = getattr(self.server, "start", None)
            if callable(starter):
                starter()
            self._started = True
            return self
        except BaseException:
            self.stop()
            raise

    def serve(self, *, once: bool = False) -> dict[str, Any]:
        """Serve the dashboard and always release runtime resources."""
        try:
            self.start()
            if once:
                return self.status()
            runner = getattr(self.server, "serve_forever", None)
            if callable(runner):
                runner()
            else:
                while not self.stop_event.wait(0.25):
                    if self._stop_marker_owned():
                        break
            return self.status()
        finally:
            # A normal return, server exception, or failed startup must not
            # leave the TESTNET lock/PID or server resources behind.
            self.stop()

    @staticmethod
    def _nested_action_failure(value: Any) -> bool:
        failure_statuses = {"FAILURE", "FAILED", "ERROR", "BLOCKED", "REJECTED", "DENIED"}
        if isinstance(value, Mapping):
            for key, nested in value.items():
                if key == "ok":
                    if nested is not True:
                        return True
                    continue
                if key == "status" and str(nested).upper() in failure_statuses:
                    return True
                if BinanceTestnetRuntime._nested_action_failure(nested):
                    return True
        elif isinstance(value, (list, tuple)):
            return any(BinanceTestnetRuntime._nested_action_failure(item) for item in value)
        return False

    @staticmethod
    def _reported_states(value: Any) -> list[str]:
        states: list[str] = []
        if isinstance(value, Mapping):
            for key, nested in value.items():
                if key in {"state", "control_state"} and nested not in (None, ""):
                    states.append(str(nested).upper())
                states.extend(BinanceTestnetRuntime._reported_states(nested))
        elif isinstance(value, (list, tuple)):
            for item in value:
                states.extend(BinanceTestnetRuntime._reported_states(item))
        return states

    def _control_action_raw(self, action: str, reason: str) -> dict[str, Any]:
        method = getattr(self.control, "action", None) or getattr(self.control, "execute", None)
        if not callable(method):
            return {"ok": False, "action": action, "reason": "CONTROL_UNAVAILABLE"}
        body = {"reason": reason}
        try:
            value = method(action, body)
        except TypeError:
            value = method(action, payload=body)
        result = _safe_mapping(value)
        result.setdefault("action", action)
        # Only an explicit boolean success is proof of an action.  Missing,
        # malformed, or status-only responses are failures.
        result["ok"] = result.get("ok") is True and not self._nested_action_failure(result)
        return result

    def _control_terminal_state(self) -> str | None:
        method = getattr(self.control, "status", None)
        if not callable(method):
            return None
        try:
            value = _safe_mapping(method())
        except Exception:
            return None
        failure_statuses = {"FAILURE", "FAILED", "ERROR", "BLOCKED", "REJECTED", "DENIED"}
        for section in (value, _safe_mapping(value.get("control")), _safe_mapping(value.get("autonomous"))):
            if str(section.get("status", "")).upper() in failure_statuses:
                return None
        candidates = [
            value.get("state"),
            value.get("control_state"),
            _safe_mapping(value.get("control")).get("state"),
            _safe_mapping(value.get("autonomous")).get("state"),
        ]
        states = {str(candidate).upper() for candidate in candidates if candidate not in (None, "")}
        if len(states) != 1:
            return None
        return next(iter(states))

    def _pause_after_auto(self, reason: str) -> dict[str, Any]:
        """Always attempt PAUSE then DISARM and verify the terminal state."""
        attempts: list[dict[str, Any]] = []
        for action in ("PAUSE", "DISARM"):
            try:
                outcome = self._control_action_raw(action, reason)
            except BaseException as exc:
                outcome = {"ok": False, "action": action, "reason": type(exc).__name__}
            attempts.append(outcome)
        terminal_state = self._control_terminal_state()
        pause_states = self._reported_states(attempts[0])
        disarm_states = self._reported_states(attempts[1])
        pause_ok = attempts[0].get("ok") is True and bool(pause_states) and all(state == "PAUSED" for state in pause_states)
        disarm_ok = attempts[1].get("ok") is True and bool(disarm_states) and all(state == "DISARMED" for state in disarm_states)
        terminal_verified = disarm_ok and terminal_state == "DISARMED"
        return {
            "ok": bool(pause_ok and disarm_ok and terminal_verified),
            "pause": attempts[0],
            "disarm": attempts[1],
            "attempts": attempts,
            "terminal_state": terminal_state,
            "terminal_verified": terminal_verified,
        }
    def _probe_auto_block_reason(self) -> str | None:
        """Reject autonomous arming while the separate probe remains unresolved."""
        method = getattr(self.gate, "probe_status", None)
        if not callable(method):
            return None
        try:
            projection = _safe_mapping(method())
        except BaseException:
            return "PROBE_STATE_UNAVAILABLE"
        risk = _safe_mapping(projection.get("risk"))
        reasons = [str(value).upper() for value in risk.get("reasons", ()) if value not in (None, "")]
        status = str(projection.get("status", "")).upper()
        if projection.get("reset_detected") or status in {"UNKNOWN", "DUST", "RESERVED", "SUBMITTING", "ACKNOWLEDGED", "PARTIALLY_FILLED"}:
            return "PROBE_UNRESOLVED"
        try:
            if _decimal(projection.get("owned_quantity", "0"), name="probe inventory") > 0:
                return "PROBE_OPEN_INVENTORY"
        except (TypeError, ValueError):
            return "PROBE_STATE_MALFORMED"
        for reason in reasons:
            if reason in {
                "PROBE_UNKNOWN",
                "PROBE_HELD",
                "PROBE_DUST",
                "PROBE_OPEN_INVENTORY",
                "PROBE_RESET",
                "FEE_VALUATION_UNAVAILABLE",
                "REALIZED_LOSS",
                "EQUITY_LOSS",
                "AUTONOMOUS_UNRESOLVED",
            }:
                return reason
        return None


    def auto(
        self,
        confirmation: str,
        window_seconds: int,
        symbol: str | None = None,
    ) -> dict[str, Any]:
        # Validate all caller-controlled values before acquiring the profile
        # lock or touching any shared state.
        if confirmation != self.TESTNET_CONFIRMATION:
            raise ValueError("exact TESTNET auto confirmation is required")
        if (
            not isinstance(window_seconds, int)
            or isinstance(window_seconds, bool)
            or not self.MIN_AUTO_WINDOW_SECONDS <= window_seconds <= self.MAX_AUTO_WINDOW_SECONDS
        ):
            raise ValueError("window_seconds must be a finite integer between 30 and 900")
        if symbol is not None and not isinstance(symbol, str):
            raise ValueError("symbol must be a string when provided")
        normalized_symbol = _symbol(symbol) if symbol is not None else None
        window = int(window_seconds)

        def utc_datetime(value: Any) -> datetime:
            if not isinstance(value, datetime):
                return datetime.now(UTC)
            if value.tzinfo is None:
                return value.replace(tzinfo=UTC)
            return value.astimezone(UTC)

        def clock_now() -> datetime:
            try:
                return utc_datetime(self.clock())
            except BaseException:
                return datetime.now(UTC)

        started_monotonic = time.monotonic()
        started_datetime = clock_now()
        deadline_monotonic = started_monotonic + float(window)
        deadline_datetime = started_datetime + timedelta(seconds=window)
        result: dict[str, Any] = {
            "ok": False,
            "action": "AUTO",
            "window_seconds": window,
            "requested_window_seconds": window,
            "symbol": normalized_symbol,
            "started_at": started_datetime.isoformat(),
            "deadline_at": deadline_datetime.isoformat(),
            "finished_at": None,
            "cycles_started": 0,
            "cycles_completed": 0,
            "supervised": True,
            "background": False,
            "worker_method": None,
            "worker": None,
            "paused": False,
            "lock": {"acquired": False, "released": False, "owned": False},
            "cleanup": None,
        }

        def finish_evidence() -> None:
            finished_monotonic = time.monotonic()
            elapsed = max(0.0, finished_monotonic - started_monotonic)
            overrun = max(0.0, elapsed - float(window))
            result["finished_at"] = clock_now().isoformat()
            result["elapsed_seconds"] = elapsed
            result["overrun_seconds"] = overrun
            result["overrun"] = bool(overrun > 0)
            if overrun > 0:
                result["ok"] = False
                result.setdefault("reason", "DEADLINE_OVERRUN")
            self._last_auto_evidence = {
                key: result.get(key)
                for key in (
                    "window_seconds", "requested_window_seconds", "symbol",
                    "started_at", "deadline_at", "finished_at",
                    "elapsed_seconds", "overrun_seconds", "overrun",
                    "cycles_started", "cycles_completed", "supervised",
                    "background", "worker_method", "reason", "lock", "cleanup",
                )
            }

        lock_owned = False
        authorization_attempted = False
        cleanup_reason = "AUTO_ENABLE_FAILED"

        # A missing/rotated keyring entry is a pre-lock failure.  It must not
        # invoke either a gate or a control mutation.
        if self.worker is None or self.execution is None or not self.credentials_configured:
            result["reason"] = self._credential_state_error or "AUTONOMOUS_NOT_CONFIGURED"
            finish_evidence()
            return result
        if not self._refresh_credentials(deadline_monotonic):
            result["reason"] = (
                "DEADLINE_ELAPSED"
                if _deadline_expired(deadline_monotonic)
                else self._credential_state_error or "CREDENTIALS_NOT_CONFIGURED"
            )
            finish_evidence()
            return result

        try:
            # Lock before live gate calls: those methods persist gate evidence.
            self._acquire_testnet()
            lock_owned = True
            result["lock"] = {"acquired": True, "released": False, "owned": True}
            self._started = True
            if _deadline_expired(deadline_monotonic):
                result["reason"] = "DEADLINE_ELAPSED"
                return result
            if not self._refresh_credentials(deadline_monotonic):
                result["reason"] = (
                    "DEADLINE_ELAPSED"
                    if _deadline_expired(deadline_monotonic)
                    else self._credential_state_error or "CREDENTIALS_CHANGED"
                )
                return result
            if _deadline_expired(deadline_monotonic):
                result["reason"] = "DEADLINE_ELAPSED"
                return result

            connectivity_method = getattr(self.gate, "check_connectivity", None)
            try:
                connectivity = _safe_mapping(
                    _call_with_optional_deadline(
                        connectivity_method,
                        deadline_monotonic=deadline_monotonic,
                    )
                )
            except BaseException as exc:
                if _is_deadline_exception(exc) or _deadline_expired(deadline_monotonic):
                    result["reason"] = "DEADLINE_ELAPSED"
                else:
                    result["reason"] = "CONNECTIVITY_" + type(exc).__name__.upper()
                return result
            result["connectivity"] = connectivity
            if _deadline_expired(deadline_monotonic):
                result["reason"] = "DEADLINE_ELAPSED"
                return result
            if str(connectivity.get("status", "")).upper() != "PASS":
                result["reason"] = "GATES_NOT_PASS"
                return result
            if not self._refresh_credentials(deadline_monotonic):
                result["reason"] = (
                    "DEADLINE_ELAPSED"
                    if _deadline_expired(deadline_monotonic)
                    else self._credential_state_error or "CREDENTIALS_CHANGED"
                )
                return result
            if _deadline_expired(deadline_monotonic):
                result["reason"] = "DEADLINE_ELAPSED"
                return result
            probe_block = self._probe_auto_block_reason()
            if probe_block is not None:
                result["reason"] = probe_block
                return result

            validation_method = getattr(self.gate, "validate_order", None)
            try:
                validation = _safe_mapping(
                    _call_with_optional_deadline(
                        validation_method,
                        deadline_monotonic=deadline_monotonic,
                        symbol=normalized_symbol,
                    )
                )
            except BaseException as exc:
                if _is_deadline_exception(exc) or _deadline_expired(deadline_monotonic):
                    result["reason"] = "DEADLINE_ELAPSED"
                else:
                    result["reason"] = "VALIDATION_" + type(exc).__name__.upper()
                return result
            result["validation"] = validation
            if _deadline_expired(deadline_monotonic):
                result["reason"] = "DEADLINE_ELAPSED"
                return result
            if not self._refresh_credentials(deadline_monotonic):
                result["reason"] = (
                    "DEADLINE_ELAPSED"
                    if _deadline_expired(deadline_monotonic)
                    else self._credential_state_error or "CREDENTIALS_CHANGED"
                )
                return result
            if _deadline_expired(deadline_monotonic):
                result["reason"] = "DEADLINE_ELAPSED"
                return result
            if str(validation.get("status", "")).upper() != "PASS":
                result["reason"] = "GATES_NOT_PASS"
                return result

            # Authorization is the first control mutation and remains inside
            # the profile lock after both live gates have passed.
            if _deadline_expired(deadline_monotonic):
                result["reason"] = "DEADLINE_ELAPSED"
                return result
            authorization_attempted = True
            authorizer = getattr(self.control, "authorize_bounded_auto", None)
            try:
                if not callable(authorizer):
                    raise BinanceOperatorError("BOUNDED_AUTO_REQUIRES_CLI")
                authorization = _safe_mapping(
                    _call_with_optional_deadline(
                        authorizer,
                        deadline_monotonic=deadline_monotonic,
                        confirmation=self.TESTNET_CONFIRMATION,
                        window_seconds=window,
                    )
                )
                enabled = {"ok": True, "action": "ENABLE", **authorization}
            except BinanceOperatorError as exc:
                enabled = {"ok": False, "action": "ENABLE", "reason": exc.reason}
            except BaseException as exc:
                if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                    raise
                enabled = {"ok": False, "action": "ENABLE", "reason": type(exc).__name__}
            result["enable"] = enabled
            if not enabled.get("ok", False):
                result["reason"] = "ENABLE_FAILED"
                return result
            cleanup_reason = "AUTO_COMPLETED"
            if _deadline_expired(deadline_monotonic):
                result["reason"] = "DEADLINE_ELAPSED"
                cleanup_reason = "AUTO_DEADLINE_ELAPSED"
                return result

            runner = getattr(self.worker, "run_once", None)
            runner_name = "run_once"
            if not callable(runner):
                runner = getattr(self.worker, "cycle", None)
                runner_name = "cycle"
            if not callable(runner):
                result["reason"] = "WORKER_RUN_ONCE_UNAVAILABLE"
                cleanup_reason = "AUTO_WORKER_ERROR"
                return result
            result["worker_method"] = runner_name
            result["cycles_started"] = 1
            try:
                result["worker"] = _call_with_optional_deadline(
                    runner,
                    deadline_monotonic=deadline_monotonic,
                    symbol=normalized_symbol,
                    now=started_datetime,
                )
            except BaseException as exc:
                if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                    raise
                cleanup_reason = "AUTO_DEADLINE_ELAPSED" if _is_deadline_exception(exc) else "AUTO_WORKER_ERROR"
                result["reason"] = "DEADLINE_ELAPSED" if _is_deadline_exception(exc) else type(exc).__name__
                return result
            if _deadline_expired(deadline_monotonic):
                result["reason"] = "DEADLINE_ELAPSED"
                cleanup_reason = "AUTO_DEADLINE_ELAPSED"
                return result
            result["cycles_completed"] = 1
            result["ok"] = True
            return result
        finally:
            if authorization_attempted and lock_owned:
                cleanup = self._pause_after_auto(cleanup_reason)
                result["cleanup"] = cleanup
                result["paused"] = bool(cleanup.get("ok"))
                if not cleanup.get("ok"):
                    result["ok"] = False
                    result["reason"] = "CLEANUP_FAILED"
            if lock_owned:
                release_error: str | None = None
                try:
                    self._release_lock()
                except BaseException as exc:
                    release_error = type(exc).__name__
                result["lock"] = {
                    "acquired": True,
                    "released": not Path(self.lock_path).exists(),
                    "owned": True,
                    "error": release_error,
                }
            self._started = False
            finish_evidence()


    def stop(self) -> dict[str, Any]:
        self.stop_event.set()
        owned_before_stop = bool(
            self._started
            or self._lock_fd is not None
            or self._owns_file(self.lock_path)
        )
        if owned_before_stop:
            try:
                self._write_stop_marker()
            except BaseException:
                pass
        stopper = getattr(self.worker, "stop", None) if self.worker is not None else None
        if callable(stopper):
            try:
                stopper()
                self._stopped_resources.add(id(self.worker))
            except BaseException:
                pass
        if self._worker_thread is not None and self._worker_thread is not threading.current_thread():
            try:
                self._worker_thread.join(timeout=2)
            except BaseException:
                pass
        server_stopper = getattr(self.server, "stop", None) or getattr(self.server, "close", None)
        if callable(server_stopper):
            try:
                server_stopper()
                self._stopped_resources.add(id(self.server))
            except BaseException:
                pass
        self._started = False
        if not self._closed:
            try:
                self._last_status = self.status()
            except BaseException:
                pass
            self._closed = True
            self._close_resources()
        try:
            self._release_lock()
        except BaseException:
            pass
        result = dict(self._last_status or {})
        result["status"] = "STOPPED"
        return result


__all__ = [
    "BinanceDevelopmentRuntime",
    "BinanceTestnetRuntime",
    "PaperBinanceSpotVenue",
    "canonical_checkout_root",
]
