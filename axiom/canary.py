"""Fail-closed micro-live Polymarket canary controls.

This module is deliberately separate from paper execution. It never enables the
platform's production-live flag and only submits through an explicitly injected
CLOB V2 venue after every persisted safety gate passes.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_DOWN
import getpass
import hashlib
import json
import os
from typing import Any, Mapping, Protocol
from urllib.request import Request, urlopen

from .domain import ensure_utc, utc_now
from .storage import AxiomStore

PRODUCTION_LIVE_EXECUTION = False
DEFAULT_TARGET_NOTIONAL_USD = Decimal("1.00")
DEFAULT_MAX_EXPOSURE_USD = Decimal("5.00")
DEFAULT_DAILY_LOSS_USD = Decimal("2.00")
DEFAULT_MAX_OPEN_POSITIONS = 3
DEFAULT_MAX_ORDERS_PER_DAY = 5
DEFAULT_MAX_SLIPPAGE_BPS = 100
_SECRET_NAMES = ("private_key", "wallet_address", "relayer_api_key", "relayer_api_key_address")
_ENV_NAMES = {
    "private_key": "POLYMARKET_PRIVATE_KEY",
    "wallet_address": "POLYMARKET_WALLET_ADDRESS",
    "relayer_api_key": "POLYMARKET_RELAYER_API_KEY",
    "relayer_api_key_address": "POLYMARKET_RELAYER_API_KEY_ADDRESS",
}

class CanaryBlocked(RuntimeError):
    """A safe, credential-free canary rejection."""

class CanaryVenue(Protocol):
    def geoblock(self) -> Mapping[str, Any]: ...
    def connectivity_check(self) -> bool: ...
    def market_context(self, market_id: str, token_id: str) -> Mapping[str, Any]: ...
    def balance(self) -> Decimal: ...
    def submit_limit_order(self, *, token_id: str, side: str, price: Decimal, size: Decimal, client_order_id: str) -> Mapping[str, Any]: ...

@dataclass(frozen=True, slots=True)
class CanaryLimits:
    target_notional_usd: Decimal = DEFAULT_TARGET_NOTIONAL_USD
    max_exposure_usd: Decimal = DEFAULT_MAX_EXPOSURE_USD
    max_daily_loss_usd: Decimal = DEFAULT_DAILY_LOSS_USD
    max_open_positions: int = DEFAULT_MAX_OPEN_POSITIONS
    max_orders_per_day: int = DEFAULT_MAX_ORDERS_PER_DAY
    max_slippage_bps: int = DEFAULT_MAX_SLIPPAGE_BPS

    def __post_init__(self) -> None:
        for name in ("target_notional_usd", "max_exposure_usd", "max_daily_loss_usd"):
            if Decimal(getattr(self, name)) <= 0:
                raise ValueError(f"{name} must be positive")
        for name in ("max_open_positions", "max_orders_per_day", "max_slippage_bps"):
            if isinstance(getattr(self, name), bool) or int(getattr(self, name)) <= 0:
                raise ValueError(f"{name} must be a positive integer")

class CredentialStore:
    """OS-keyring first; environment variables are an explicit fallback only."""
    service = "AXIOM-POLYMARKET-CANARY"

    def configure(self, *, reader=getpass.getpass) -> None:
        try:
            import keyring
        except ImportError as exc:
            raise CanaryBlocked("OS_KEYRING_UNAVAILABLE") from exc
        values = {
            "private_key": reader("Dedicated canary signer private key: "),
            "wallet_address": reader("Dedicated Polymarket wallet address: "),
            "relayer_api_key": reader("Polymarket relayer API key: "),
            "relayer_api_key_address": reader("Polymarket relayer API key address: "),
        }
        if not all(values.values()):
            raise CanaryBlocked("CREDENTIAL_CONFIGURATION_INCOMPLETE")
        for name, value in values.items():
            keyring.set_password(self.service, name, value)

    def load(self, *, allow_environment: bool = False) -> dict[str, str]:
        values: dict[str, str] = {}
        try:
            import keyring
            for name in _SECRET_NAMES:
                value = keyring.get_password(self.service, name)
                if value:
                    values[name] = value
        except Exception:
            pass
        if allow_environment:
            for name, variable in _ENV_NAMES.items():
                if name not in values and os.environ.get(variable):
                    values[name] = os.environ[variable]
        return values if all(name in values for name in _SECRET_NAMES) else {}

    def configured(self, *, allow_environment: bool = False) -> bool:
        return bool(self.load(allow_environment=allow_environment))

class PolymarketClobV2Venue:
    """Official unified Python SDK integration; no deprecated V1 signing."""
    def __init__(self, credentials: Mapping[str, str], *, geoblock_url: str = "https://polymarket.com/api/geoblock") -> None:
        self._credentials = dict(credentials)
        self._geoblock_url = geoblock_url
        self._client: Any = None

    def _secure_client(self) -> Any:
        if self._client is None:
            try:
                from polymarket import RelayerApiKey, SecureClient
            except ImportError as exc:
                raise CanaryBlocked("OFFICIAL_POLYMARKET_SDK_NOT_INSTALLED") from exc
            self._client = SecureClient.create(
                private_key=self._credentials["private_key"],
                wallet=self._credentials["wallet_address"],
                api_key=RelayerApiKey(key=self._credentials["relayer_api_key"], address=self._credentials["relayer_api_key_address"]),
            )
        return self._client

    def geoblock(self) -> Mapping[str, Any]:
        request = Request(self._geoblock_url, headers={"Accept": "application/json", "User-Agent": "AXIOM-canary/1"})
        with urlopen(request, timeout=10) as response:
            payload = json.loads(response.read().decode("utf-8"))
        return {"blocked": bool(payload.get("blocked", True)), "country": payload.get("country"), "region": payload.get("region")}

    def connectivity_check(self) -> bool:
        client = self._secure_client()
        return bool(getattr(client, "wallet", None))

    def market_context(self, market_id: str, token_id: str) -> Mapping[str, Any]:
        client = self._secure_client()
        market = client.get_market(market_id=market_id)
        book = client.get_order_book(token_id=token_id)
        return {
            "accepting_orders": bool(getattr(market, "accepting_orders", False)),
            "min_order_size": str(book.min_order_size), "tick_size": str(book.tick_size),
            "bids": [{"price": str(x.price), "size": str(x.size)} for x in book.bids],
            "asks": [{"price": str(x.price), "size": str(x.size)} for x in book.asks],
            "fee_bps": str(getattr(market, "fee_bps", 0) or 0),
        }

    def balance(self) -> Decimal:
        client = self._secure_client()
        value = client.get_balance_allowance()
        return Decimal(str(getattr(value, "balance", 0)))

    def submit_limit_order(self, *, token_id: str, side: str, price: Decimal, size: Decimal, client_order_id: str) -> Mapping[str, Any]:
        response = self._secure_client().place_limit_order(token_id=token_id, side=side, price=str(price), size=str(size))
        return {"ok": bool(response.ok), "order_id": getattr(response, "order_id", None), "status": getattr(response, "status", None), "trade_ids": list(getattr(response, "trade_ids", ()) or ())}

class CanaryService:
    def __init__(self, store: AxiomStore, *, credentials: CredentialStore | None = None, clock=utc_now) -> None:
        self.store, self.credentials, self.clock = store, credentials or CredentialStore(), clock
        self._initialize()

    def _initialize(self) -> None:
        with self.store.connection:
            self.store.connection.executescript("""
            CREATE TABLE IF NOT EXISTS canary_control (
              singleton INTEGER PRIMARY KEY CHECK(singleton=1), state TEXT NOT NULL,
              candidate_id TEXT, venue TEXT, armed_at TEXT, expires_at TEXT,
              limits_json TEXT NOT NULL, integrity_hash TEXT NOT NULL, updated_at TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS canary_eligibility (
              candidate_id TEXT PRIMARY KEY, eligible_at TEXT NOT NULL,
              frozen_hash TEXT NOT NULL, evidence_json TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS canary_ledger (
              event_id TEXT PRIMARY KEY, signal_id TEXT NOT NULL UNIQUE, timestamp TEXT NOT NULL,
              candidate_id TEXT NOT NULL, venue TEXT NOT NULL, market_id TEXT NOT NULL, token_id TEXT NOT NULL,
              side TEXT NOT NULL, requested_notional TEXT NOT NULL, paper_expected_price TEXT NOT NULL,
              max_price TEXT NOT NULL, submitted_quantity TEXT, exchange_order_id TEXT,
              fill_quantity TEXT, actual_average_price TEXT, fees TEXT, status TEXT NOT NULL,
              latency_ms INTEGER, price_difference TEXT, fee_difference TEXT, slippage_difference TEXT,
              settlement TEXT, realized_pnl TEXT, evidence_json TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS canary_execution_events (
              execution_event_id TEXT PRIMARY KEY, canary_event_id TEXT NOT NULL,
              timestamp TEXT NOT NULL, exchange_order_id TEXT, status TEXT NOT NULL,
              fill_quantity TEXT, actual_average_price TEXT, fees TEXT,
              latency_ms INTEGER, evidence_json TEXT NOT NULL,
              FOREIGN KEY(canary_event_id) REFERENCES canary_ledger(event_id));
            CREATE INDEX IF NOT EXISTS idx_canary_execution_events_order
              ON canary_execution_events(canary_event_id, timestamp);
            CREATE INDEX IF NOT EXISTS idx_canary_ledger_time ON canary_ledger(timestamp, event_id);
            """)

    @staticmethod
    def _integrity(candidate: str, venue: str, expires: str, limits: Mapping[str, Any]) -> str:
        return hashlib.sha256(json.dumps({"candidate": candidate, "venue": venue, "expires": expires, "limits": limits}, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    def _limits_record(self, limits: CanaryLimits) -> dict[str, Any]:
        return {"target_notional_usd": str(limits.target_notional_usd), "max_exposure_usd": str(limits.max_exposure_usd), "max_daily_loss_usd": str(limits.max_daily_loss_usd), "max_open_positions": limits.max_open_positions, "max_orders_per_day": limits.max_orders_per_day, "max_slippage_bps": limits.max_slippage_bps}

    def mark_eligible(self, candidate_id: str) -> None:
        record = self.store.load_candidate_lifecycle(candidate_id)
        if not record or record.get("stage") not in {"FROZEN", "PAPER_FORWARD", "PAPER_PROMOTABLE"}:
            raise CanaryBlocked("CANDIDATE_RESEARCH_GATES_INCOMPLETE")
        payload = dict(record.get("payload") or {})
        schema_ok = bool(payload.get("schema_validated", payload.get("paper_only") is True))
        backtest_ok = bool(payload.get("historical_backtest_passed", payload.get("raw_observations", 0) or payload.get("minimum_sample_check")))
        validation_ok = bool(payload.get("validation_passed", payload.get("validation_expectancy") is not None))
        robustness_ok = bool(payload.get("robustness_passed"))
        hash_parts = [str(payload.get(key) or "") for key in ("strategy_hash", "model_hash", "config_hash")]
        frozen_hash = str(payload.get("frozen_hash") or (hashlib.sha256("|".join(hash_parts).encode()).hexdigest() if all(hash_parts) else ""))
        quality_ok = bool(payload.get("data_quality_passed", payload.get("validation_execution_quality") not in {None, "", "INVALID"}))
        if not all((schema_ok, backtest_ok, validation_ok, robustness_ok, bool(frozen_hash), quality_ok)) or payload.get("critical_error"):
            raise CanaryBlocked("CANDIDATE_RESEARCH_GATES_INCOMPLETE")
        evidence = {"schema_validated":schema_ok,"historical_backtest_passed":backtest_ok,"validation_passed":validation_ok,"robustness_passed":robustness_ok,"frozen_hash":frozen_hash,"data_quality_passed":quality_ok}
        with self.store.connection:
            self.store.connection.execute("INSERT INTO canary_eligibility(candidate_id,eligible_at,frozen_hash,evidence_json) VALUES(?,?,?,?) ON CONFLICT(candidate_id) DO UPDATE SET eligible_at=excluded.eligible_at,frozen_hash=excluded.frozen_hash,evidence_json=excluded.evidence_json", (candidate_id, ensure_utc(self.clock()).isoformat(), frozen_hash, json.dumps(evidence, sort_keys=True)))

    def arm(self, candidate_id: str, *, venue: CanaryVenue, target_notional_usd: Decimal = DEFAULT_TARGET_NOTIONAL_USD, expires_hours: Decimal = Decimal("24"), limits: CanaryLimits | None = None, credentials_configured: bool | None = None) -> Mapping[str, Any]:
        now = ensure_utc(self.clock()); limits = limits or CanaryLimits(target_notional_usd=Decimal(target_notional_usd))
        eligible = self.store.connection.execute("SELECT 1 FROM canary_eligibility WHERE candidate_id=?", (candidate_id,)).fetchone()
        if eligible is None: raise CanaryBlocked("CANDIDATE_NOT_CANARY_ELIGIBLE")
        health = self.store.polymarket_health(now=now)
        if str(health.get("grade", "F")).upper() not in {"A", "B"}: raise CanaryBlocked("COLLECTOR_DEGRADED")
        if credentials_configured is False or (credentials_configured is None and not self.credentials.configured()): raise CanaryBlocked("CREDENTIALS_NOT_CONFIGURED")
        geo = venue.geoblock()
        if geo.get("blocked") or geo.get("close_only"): raise CanaryBlocked("GEOGRAPHICALLY_BLOCKED")
        expires = now + timedelta(hours=float(expires_hours)); values=self._limits_record(limits); digest=self._integrity(candidate_id,"polymarket",expires.isoformat(),values)
        with self.store.connection:
            self.store.connection.execute("INSERT INTO canary_control VALUES(1,'ARMED',?,?,?,?,?,?,?) ON CONFLICT(singleton) DO UPDATE SET state=excluded.state,candidate_id=excluded.candidate_id,venue=excluded.venue,armed_at=excluded.armed_at,expires_at=excluded.expires_at,limits_json=excluded.limits_json,integrity_hash=excluded.integrity_hash,updated_at=excluded.updated_at", (candidate_id,"polymarket",now.isoformat(),expires.isoformat(),json.dumps(values,sort_keys=True),digest,now.isoformat()))
        return self.status()

    def disarm(self) -> None: self._set_state("DISARMED")
    def kill(self) -> None: self._set_state("KILLED")
    def _set_state(self, state: str) -> None:
        now=ensure_utc(self.clock()).isoformat()
        with self.store.connection:
            self.store.connection.execute("INSERT INTO canary_control(singleton,state,limits_json,integrity_hash,updated_at) VALUES(1,?,'{}','',?) ON CONFLICT(singleton) DO UPDATE SET state=excluded.state,updated_at=excluded.updated_at",(state,now))

    def status(self) -> dict[str, Any]:
        row=self.store.connection.execute("SELECT * FROM canary_control WHERE singleton=1").fetchone()
        now=ensure_utc(self.clock())
        if row is None:
            return {"production_live_trading":"DISABLED","micro_live_canary":"DISARMED","candidate":None,"venue":None,"expiry":None,"today_orders":0,"today_realized_pnl":0.0,"total_exposure":0.0,"open_positions":0,"daily_loss_budget_remaining":float(DEFAULT_DAILY_LOSS_USD),"limits":self._limits_record(CanaryLimits()),"live_execution":False,"trades":[]}
        data=dict(row); state=data["state"]
        if state=="ARMED":
            limits=json.loads(data.get("limits_json") or "{}")
            expected=self._integrity(str(data.get("candidate_id") or ""),str(data.get("venue") or ""),str(data.get("expires_at") or ""),limits)
            eligible=self.store.connection.execute("SELECT 1 FROM canary_eligibility WHERE candidate_id=?",(data.get("candidate_id"),)).fetchone()
            if not eligible or expected!=data.get("integrity_hash"): state="KILLED"
            elif not data.get("expires_at") or ensure_utc(datetime.fromisoformat(data["expires_at"]))<=now: state="DISARMED"
        start=now.replace(hour=0,minute=0,second=0,microsecond=0).isoformat()
        trades=[dict(x) for x in self.store.connection.execute("SELECT l.timestamp,l.candidate_id,l.market_id,l.side,l.requested_notional,l.paper_expected_price,e.actual_average_price,CASE WHEN e.actual_average_price IS NOT NULL THEN CAST(e.actual_average_price AS REAL)-CAST(l.paper_expected_price AS REAL) END price_difference,COALESCE(e.status,l.status) status,l.realized_pnl FROM canary_ledger l LEFT JOIN canary_execution_events e ON e.execution_event_id=(SELECT e2.execution_event_id FROM canary_execution_events e2 WHERE e2.canary_event_id=l.event_id ORDER BY e2.timestamp DESC,e2.execution_event_id DESC LIMIT 1) ORDER BY l.timestamp DESC LIMIT 100")]
        aggregates=self.store.connection.execute("SELECT COUNT(*) orders, COALESCE(SUM(CASE WHEN status IN ('RESERVED','OPEN','PARTIAL','SUBMITTED') THEN CAST(requested_notional AS REAL) ELSE 0 END),0) exposure, COUNT(DISTINCT CASE WHEN status IN ('RESERVED','OPEN','PARTIAL','SUBMITTED') THEN market_id END) positions, COALESCE(SUM(CASE WHEN timestamp>=? THEN CAST(realized_pnl AS REAL) ELSE 0 END),0) pnl FROM canary_ledger WHERE timestamp>=? OR status IN ('RESERVED','OPEN','PARTIAL','SUBMITTED')",(start,start)).fetchone()
        limits=json.loads(data.get("limits_json") or "{}")
        return {"production_live_trading":"DISABLED","micro_live_canary":state,"candidate":data.get("candidate_id"),"venue":data.get("venue"),"expiry":data.get("expires_at"),"today_orders":int(aggregates["orders"]),"today_realized_pnl":float(aggregates["pnl"]),"total_exposure":float(aggregates["exposure"]),"open_positions":int(aggregates["positions"]),"daily_loss_budget_remaining":max(0,float(limits.get("max_daily_loss_usd",2))+float(aggregates["pnl"])),"limits":limits,"trades":trades,"live_execution":False}

    def check(self, *, candidate_id: str | None, venue: CanaryVenue | None, market_id: str | None = None, token_id: str | None = None, allow_environment: bool = False) -> dict[str, Any]:
        failures=[]; status=self.status()
        if status["micro_live_canary"]!="ARMED": failures.append("CANARY_NOT_ARMED")
        if candidate_id and candidate_id!=status.get("candidate"): failures.append("CANDIDATE_NOT_ARMED")
        if not self.credentials.configured(allow_environment=allow_environment): failures.append("CREDENTIALS_NOT_CONFIGURED")
        try:
            if str(self.store.polymarket_health(now=ensure_utc(self.clock())).get("grade","F")).upper() not in {"A","B"}: failures.append("COLLECTOR_DEGRADED")
        except Exception: failures.append("COLLECTOR_DEGRADED")
        if venue is not None:
            try:
                geo=venue.geoblock()
                if geo.get("blocked") or geo.get("close_only"): failures.append("GEOGRAPHICALLY_BLOCKED")
                if not venue.connectivity_check(): failures.append("AUTHENTICATED_CONNECTIVITY_FAILED")
                if market_id and token_id:
                    context=venue.market_context(market_id,token_id)
                    if not context.get("accepting_orders"): failures.append("MARKET_NOT_ACCEPTING_ORDERS")
                    if Decimal(str(context.get("min_order_size",0)))*Decimal(str((context.get("asks") or [{}])[-1].get("price",1)))>Decimal(str(status.get("limits",{}).get("target_notional_usd",1))): failures.append("VENUE_MINIMUM_EXCEEDS_CANARY_TARGET")
                    if venue.balance()<Decimal(str(status.get("limits",{}).get("target_notional_usd",1))): failures.append("INSUFFICIENT_BALANCE")
            except Exception: failures.append("VENUE_CONNECTIVITY_FAILED")
        return {"ready":not failures,"message":"READY FOR MICRO LIVE CANARY" if not failures else "NOT READY FOR MICRO LIVE CANARY","failures":list(dict.fromkeys(failures)),"live_execution":False}

    def submit(self, *, signal_id: str, candidate_id: str, market_id: str, token_id: str, side: str, paper_expected_price: Decimal, venue: CanaryVenue) -> Mapping[str, Any]:
        now=ensure_utc(self.clock()); status=self.status(); limits=status.get("limits",{})
        def block(reason: str) -> None: raise CanaryBlocked(reason)
        if status["micro_live_canary"]!="ARMED": block("CANARY_NOT_ARMED")
        if candidate_id!=status.get("candidate"): block("CANDIDATE_NOT_ARMED")
        if self.store.connection.execute("SELECT 1 FROM canary_ledger WHERE signal_id=?",(signal_id,)).fetchone(): block("DUPLICATE_SIGNAL")
        if status["today_orders"]>=int(limits["max_orders_per_day"]): block("DAILY_ORDER_LIMIT")
        if status["open_positions"]>=int(limits["max_open_positions"]): block("OPEN_POSITION_LIMIT")
        if status["today_realized_pnl"]<=-float(limits["max_daily_loss_usd"]): block("DAILY_LOSS_LIMIT")
        target=Decimal(limits["target_notional_usd"])
        if Decimal(str(status["total_exposure"]))+target>Decimal(limits["max_exposure_usd"]): block("EXPOSURE_LIMIT")
        geo=venue.geoblock()
        if geo.get("blocked") or geo.get("close_only"): block("GEOGRAPHICALLY_BLOCKED")
        if not self.credentials.configured(): block("CREDENTIALS_NOT_CONFIGURED")
        context=venue.market_context(market_id,token_id)
        if not context.get("accepting_orders"): block("MARKET_NOT_ACCEPTING_ORDERS")
        asks=context.get("asks") or []
        if side.upper()!="BUY" or not asks: block("NO_EXECUTABLE_BOOK")
        best=Decimal(str(asks[-1]["price"])); max_price=(Decimal(paper_expected_price)*(Decimal(1)+Decimal(limits["max_slippage_bps"])/Decimal(10000)))
        tick=Decimal(str(context["tick_size"])); max_price=(max_price/tick).to_integral_value(rounding=ROUND_DOWN)*tick
        if best>max_price: block("SLIPPAGE_LIMIT")
        quantity=(target/best).quantize(Decimal("0.01"),rounding=ROUND_DOWN)
        if quantity<Decimal(str(context["min_order_size"])): block("VENUE_MINIMUM_EXCEEDS_CANARY_TARGET")
        notional=quantity*best
        if notional>target: block("CANARY_TARGET_EXCEEDED")
        event_id="canary-"+hashlib.sha256(signal_id.encode()).hexdigest()[:24]
        evidence={"bid":(context.get("bids") or [{}])[-1].get("price"),"ask":str(best),"depth":asks,"tick_size":str(tick),"min_order_size":str(context["min_order_size"]),"estimated_fees":str(notional*Decimal(str(context.get("fee_bps",0)))/Decimal(10000)),"geoblock":{"blocked":False,"country":geo.get("country"),"region":geo.get("region")}}
        with self.store.connection:
            self.store.connection.execute("INSERT INTO canary_ledger(event_id,signal_id,timestamp,candidate_id,venue,market_id,token_id,side,requested_notional,paper_expected_price,max_price,submitted_quantity,status,evidence_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",(event_id,signal_id,now.isoformat(),candidate_id,"polymarket",market_id,token_id,side.upper(),str(notional),str(paper_expected_price),str(max_price),str(quantity),"RESERVED",json.dumps(evidence,sort_keys=True)))
        submitted_at=ensure_utc(self.clock())
        response=venue.submit_limit_order(token_id=token_id,side=side.upper(),price=max_price,size=quantity,client_order_id=event_id)
        received_at=ensure_utc(self.clock()); execution_id=event_id+"-submitted"
        with self.store.connection:
            self.store.connection.execute("INSERT INTO canary_execution_events(execution_event_id,canary_event_id,timestamp,exchange_order_id,status,fill_quantity,actual_average_price,fees,latency_ms,evidence_json) VALUES(?,?,?,?,?,?,?,?,?,?)",(execution_id,event_id,received_at.isoformat(),response.get("order_id"),str(response.get("status") or ("SUBMITTED" if response.get("ok") else "REJECTED")).upper(),str(response.get("fill_quantity")) if response.get("fill_quantity") is not None else None,str(response.get("actual_average_price")) if response.get("actual_average_price") is not None else None,str(response.get("fees")) if response.get("fees") is not None else None,max(0,int((received_at-submitted_at).total_seconds()*1000)),json.dumps({"trade_ids":response.get("trade_ids",[])},sort_keys=True)))
        return {"event_id":event_id,"status":response.get("status"),"requested_notional":str(notional),"production_live_execution":False}

__all__=["CanaryBlocked","CanaryLimits","CanaryService","CanaryVenue","CredentialStore","PolymarketClobV2Venue","PRODUCTION_LIVE_EXECUTION"]
