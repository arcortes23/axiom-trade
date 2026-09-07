"""Isolated, offline-safe Binance Spot development runtime.

The runtime in this module is intentionally narrower than the normal Axiom
supervisor.  It owns one fixed SQLite database, one loopback dashboard and one
Binance Spot worker.  PAPER is the only CLI/default execution environment;
TESTNET is available only to embedders that inject both credentials and a
venue explicitly.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import os
from pathlib import Path
import threading
import time
from typing import Any, Callable, Mapping
import uuid

from .binance_auto import BinanceAutonomousWorker
from .binance_execution import BinanceExecutionService
from .binance_market import BoundedBinanceMarketCollector
from .binance_operator import BinanceCanaryControlPlane
from .binance_research import BinanceCryptoQualificationService
from .binance_spot import (
    BINANCE_SPOT_LIVE,
    BINANCE_SPOT_TESTNET,
    PAPER,
    BinanceRuntimeProfile,
    BinanceSpotEnvironment,
    BinanceSpotRESTClient,
    BinanceSpotResult,
    credential_fingerprint,
    validate_spot_venue_identity,
)
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
    """Own the isolated Binance development process and its resources."""

    def __init__(
        self,
        worktree_root: str | os.PathLike[str] | None = None,
        *,
        profile: BinanceRuntimeProfile | None = None,
        environment: BinanceSpotEnvironment | str = PAPER,
        credentials: Any | None = None,
        venue: Any | None = None,
        provider: Any | None = None,
        collector: Any | None = None,
        worker: Any | None = None,
        dashboard_server: Any | None = None,
        store_factory: Callable[[str], Any] | None = None,
        worker_factory: Callable[..., Any] | None = None,
        dashboard_server_factory: Callable[..., Any] | None = None,
    ) -> None:
        root = canonical_checkout_root(worktree_root)
        env = _environment(environment)
        if env is BinanceSpotEnvironment.BINANCE_SPOT_LIVE:
            raise ValueError("Binance development runtime cannot use LIVE")
        if env is BinanceSpotEnvironment.BINANCE_SPOT_TESTNET and credentials is None:
            raise ValueError("TESTNET requires explicit credentials")
        if env is BinanceSpotEnvironment.PAPER and credentials is not None:
            raise ValueError("PAPER runtime does not accept credentials")
        expected_db = os.path.join(root, "runtime-data", "binance-dev.sqlite")
        if profile is None:
            profile = BinanceRuntimeProfile.development(root, expected_db, DEFAULT_HOST, DEFAULT_PORT, environment=env)
        elif not isinstance(profile, BinanceRuntimeProfile):
            raise TypeError("profile must be BinanceRuntimeProfile")
        if os.path.normcase(os.path.realpath(profile.worktree_root)) != root:
            raise ValueError("profile root does not match the development checkout")
        if profile.environment is not env:
            raise ValueError("profile/environment mismatch")
        resolved_venue = venue
        if resolved_venue is None and env is BinanceSpotEnvironment.PAPER:
            resolved_venue = PaperBinanceSpotVenue()
        elif resolved_venue is None and env is BinanceSpotEnvironment.BINANCE_SPOT_TESTNET:
            resolved_venue = BinanceSpotRESTClient(env, credentials)
        _validate_runtime_venue(env, resolved_venue, credentials)

        self.profile = profile
        self.environment = env.value
        self.owner = _RuntimeOwner(os.getpid(), uuid.uuid4().hex)
        self.stop_event = threading.Event()
        self._lock_fd: int | None = None
        self._started = False
        self._closed = False
        self._worker_thread: threading.Thread | None = None
        self._worker_error: BaseException | None = None
        self.store = (store_factory or AxiomStore)(self.profile.db_path)
        self.provider = provider or BinanceAdapter()
        self.universe_loader = _CurrentPersistedUniverseLoader(self.store)
        snapshot = self.universe_loader.load()
        self.collector = collector
        if self.collector is None and snapshot is not None:
            self.collector = BoundedBinanceMarketCollector(self.provider, snapshot, max_workers=4, depth=20)
        self.paper_venue = resolved_venue
        self.venue = self.paper_venue
        self.qualification = BinanceCryptoQualificationService(self.store)
        self.execution = BinanceExecutionService(
            self.store,
            venue=self.venue,
            profile=self.profile,
            environment=self.environment,
            credentials=credentials,
            owner_id=RUNTIME_IDENTITY,
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
                worker_id=RUNTIME_IDENTITY,
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
            self.server = factory(DEFAULT_HOST, DEFAULT_PORT, data=self.dashboard_data)

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
        return json.dumps({"pid": self.owner.pid, "runtime_identity": RUNTIME_IDENTITY, "owner_token": self.owner.token}, sort_keys=True, separators=(",", ":"))

    def _write_owned_file(self, path: str, content: str, *, exclusive: bool = False) -> None:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        if exclusive:
            fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            self._lock_fd = fd
            os.write(fd, content.encode("utf-8"))
            os.fsync(fd)
        else:
            Path(path).write_text(content, encoding="utf-8")

    def _acquire(self) -> None:
        self._write_owned_file(self.lock_path, self._owner_document(), exclusive=True)
        try:
            self._write_owned_file(self.pid_path, self._owner_document())
            Path(self.log_path).parent.mkdir(parents=True, exist_ok=True)
            with open(self.log_path, "a", encoding="utf-8") as handle:
                handle.write(self._owner_document() + "\n")
        except BaseException:
            self._release_lock()
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
        return document.get("runtime_identity") == RUNTIME_IDENTITY and document.get("owner_token") == self.owner.token and pid == self.owner.pid

    def _release_lock(self) -> None:
        fd = self._lock_fd
        self._lock_fd = None
        if fd is not None:
            try:
                os.close(fd)
            except OSError:
                pass
        if self._owns_file(self.lock_path):
            try:
                Path(self.lock_path).unlink()
            except FileNotFoundError:
                pass
        if self._owns_file(self.pid_path):
            try:
                Path(self.pid_path).unlink()
            except FileNotFoundError:
                pass

    def _clear_stale_stop(self) -> None:
        try:
            document = json.loads(Path(self.stop_path).read_text(encoding="utf-8"))
        except (FileNotFoundError, OSError, ValueError, json.JSONDecodeError):
            return
        if document.get("runtime_identity") == RUNTIME_IDENTITY:
            try:
                Path(self.stop_path).unlink()
            except FileNotFoundError:
                pass

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
                self._worker_thread = threading.Thread(target=self._run_worker, args=(False,), name=RUNTIME_IDENTITY, daemon=True)
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
        return document.get("runtime_identity") == RUNTIME_IDENTITY

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
        return {
            "status": "RUNNING" if self._started else ("STOPPED" if self._closed else "READY"),
            "url": url,
            "profile": self.profile.projection(),
            "paths": {"db": self.db_path, "log": self.log_path, "lock": self.lock_path, "stop": self.stop_path, "pid": self.pid_path},
            "worker": worker_status,
            "paper_only": self.environment == PAPER,
            "live_execution": False,
            "polymarket_transport": "DISABLED",
            "hermes": "DISABLED",
            "research_node": "DISABLED",
            "operator_control_plane": "DISABLED",
            "worker_error": None if self._worker_error is None else type(self._worker_error).__name__,
        }


__all__ = [
    "BinanceDevelopmentRuntime",
    "PaperBinanceSpotVenue",
    "canonical_checkout_root",
]
