"""Minimal offline dashboard HTTP server.

The server is read-only and dependency-free.  Every endpoint returns JSON and
``/`` serves a tiny HTML index that links to the same data, making the service
useful with no network providers or frontend build step.
"""
from __future__ import annotations

from dataclasses import asdict, is_dataclass
from datetime import date, datetime
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from types import MappingProxyType
from typing import Any, Mapping
from urllib.parse import urlparse

from .domain import ensure_utc, to_record


_ENDPOINTS = ("overview", "research", "crypto", "prediction", "evolution", "risk", "system", "dataset-health")


def _jsonable(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (tuple, list, set, frozenset)):
        return [_jsonable(v) for v in value]
    if isinstance(value, (datetime, date)):
        return ensure_utc(value).isoformat() if isinstance(value, datetime) else value.isoformat()
    if is_dataclass(value):
        try:
            return _jsonable(to_record(value))
        except (TypeError, ValueError):
            return _jsonable(asdict(value))
    if hasattr(value, "value") and not isinstance(value, (str, bytes)):
        return value.value
    return value


def _candidate_record(candidate: Any) -> dict[str, Any]:
    strategy = getattr(candidate, "strategy", None)
    strategy_record = strategy.to_dict() if callable(getattr(strategy, "to_dict", None)) else strategy
    return {
        "candidate_id": getattr(candidate, "candidate_id", ""),
        "generation": getattr(candidate, "generation", 0),
        "lineage": list(getattr(candidate, "lineage", ())),
        "score": getattr(candidate, "score", None),
        "train_score": getattr(candidate, "train_score", None),
        "validation_score": getattr(candidate, "validation_score", None),
        "holdout_score": getattr(candidate, "holdout_score", None),
        "rejected": getattr(candidate, "rejected", False),
        "strategy": strategy_record,
    }

class DashboardData:
    """Read-only data facade used by HTTP handlers and embedders."""

    def __init__(
        self,
        *,
        data: Mapping[str, Any] | None = None,
        tracker: Any | None = None,
        crypto_provider: Any | None = None,
        prediction_provider: Any | None = None,
        evolution: Any | None = None,
        risk: Any | None = None,
        store: Any | None = None,
    ) -> None:
        self._data = dict(data or {})
        self.tracker = tracker
        self.crypto_provider = crypto_provider
        self.prediction_provider = prediction_provider
        self.evolution = evolution
        self.risk = risk
        self.store = store

    def _configured(self, name: str) -> Any:
        if name in self._data:
            return self._data[name]
        return None

    def research(self) -> Any:
        configured = self._configured("research")
        if configured is not None:
            return configured
        if self.tracker is not None:
            if hasattr(self.tracker, "as_records"):
                return {"experiments": self.tracker.as_records()}
            records = getattr(self.tracker, "records", ())
            return {"experiments": [_jsonable(item) for item in records]}
        return {"experiments": []}

    def crypto(self) -> Any:
        configured = self._configured("crypto")
        if configured is not None:
            return configured
        provider = self.crypto_provider
        if provider is None:
            return {"available": False, "provider": None, "symbols": []}
        symbols = self._data.get("crypto_symbols", ())
        tickers = {}
        for symbol in symbols:
            try:
                ticker = provider.ticker(symbol)
            except Exception:
                ticker = None
            if ticker is not None:
                tickers[symbol] = ticker
        return {"available": True, "provider": provider.__class__.__name__, "tickers": tickers}

    def prediction(self) -> Any:
        configured = self._configured("prediction")
        if configured is not None:
            return configured
        provider = self.prediction_provider
        if provider is None:
            return {"available": False, "provider": None, "markets": []}
        try:
            markets = provider.markets(active=True)
        except Exception:
            markets = ()
        return {"available": True, "provider": provider.__class__.__name__, "markets": markets}

    def evolution_data(self) -> Any:
        configured = self._configured("evolution")
        if configured is not None:
            return configured
        source = self.evolution
        if source is None:
            return {"available": False, "candidates": []}
        if callable(source):
            return source()
        snapshot = getattr(source, "snapshot", None)
        if callable(snapshot):
            return snapshot()
        population = getattr(source, "population", None)
        if population is not None and hasattr(source, "generation"):
            return {
                "available": True,
                "generation": int(source.generation),
                "population": [_candidate_record(candidate) for candidate in population],
            }
        return source

    def risk_data(self) -> Any:
        configured = self._configured("risk")
        if configured is not None:
            return configured
        source = self.risk
        if source is None:
            return {"available": False, "live_execution": False}
        if callable(source):
            return source()
        snapshot = getattr(source, "snapshot", None)
        if callable(snapshot):
            return snapshot()
        status = getattr(source, "status", None)
        if callable(status):
            return status()
        return source

    def dataset_health(self) -> Any:
        configured = self._configured("dataset-health")
        if configured is not None:
            return configured
        if self.store is None:
            return {
                "grade": "F",
                "markets": 0,
                "snapshots": 0,
                "trades": 0,
                "collection_errors": 0,
                "stale_markets": [],
                "gaps": [],
                "live_execution": False,
            }
        health = getattr(self.store, "polymarket_health", None)
        if not callable(health):
            return {"grade": "F", "error": "store has no polymarket health method", "live_execution": False}
        return health()
    def system(self) -> dict[str, Any]:
        configured = self._configured("system")
        if configured is not None:
            result = dict(configured) if isinstance(configured, Mapping) else {"value": configured}
        else:
            result = {"service": "axiom-dashboard", "status": "ok", "offline": True, "live_execution": False}
        result["dataset_health"] = self.dataset_health()
        result["endpoints"] = list(_ENDPOINTS)
        return result

    def overview(self) -> dict[str, Any]:
        configured = self._configured("overview")
        if configured is not None:
            return dict(configured) if isinstance(configured, Mapping) else configured
        research = self.research()
        experiments = research.get("experiments", ()) if isinstance(research, Mapping) else ()
        return {
            "service": "axiom-dashboard",
            "status": "ok",
            "offline": True,
            "live_execution": False,
            "experiment_count": len(experiments) if hasattr(experiments, "__len__") else 0,
            "endpoints": list(_ENDPOINTS),
        }

    def snapshot(self, endpoint: str) -> Any:
        endpoint = endpoint.strip("/").lower()
        if endpoint == "overview":
            return self.overview()
        if endpoint == "research":
            return self.research()
        if endpoint == "crypto":
            return self.crypto()
        if endpoint == "prediction":
            return self.prediction()
        if endpoint == "evolution":
            return self.evolution_data()
        if endpoint == "risk":
            return self.risk_data()
        if endpoint == "system":
            return self.system()
        if endpoint == "dataset-health":
            return self.dataset_health()
        raise KeyError(endpoint)


def _dashboard_html() -> str:
    """Return the dependency-free read-only dashboard surface."""
    return """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Axiom Research Dashboard</title>
  <style>
    :root { color-scheme: dark; font-family: system-ui, sans-serif; background: #0f172a; color: #e2e8f0; }
    body { margin: 0; }
    header { padding: 24px 5vw; border-bottom: 1px solid #334155; background: #111827; }
    h1, h2, p { margin: 0; }
    h1 { font-size: 1.5rem; }
    .muted { color: #94a3b8; }
    main { max-width: 1400px; margin: 0 auto; padding: 24px 5vw 48px; }
    .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 16px; margin-bottom: 16px; }
    .panel { border: 1px solid #334155; border-radius: 10px; background: #1e293b; padding: 16px; overflow: auto; }
    .panel h2 { font-size: 1rem; margin-bottom: 12px; }
    .metric { font-size: 1.6rem; font-weight: 700; margin-top: 8px; }
    table { width: 100%; border-collapse: collapse; font-size: .9rem; }
    th, td { text-align: left; padding: 8px; border-bottom: 1px solid #334155; white-space: nowrap; }
    th { color: #94a3b8; font-weight: 500; }
    pre { margin: 0; max-height: 280px; overflow: auto; white-space: pre-wrap; word-break: break-word; color: #cbd5e1; }
    svg { width: 100%; height: 150px; background: #0f172a; border-radius: 6px; }
    .status { display: inline-block; padding: 4px 8px; border-radius: 999px; background: #14532d; color: #bbf7d0; font-size: .8rem; }
    .warning { background: #713f12; color: #fde68a; }
    a { color: #93c5fd; }
  </style>
</head>
<body>
  <header>
    <h1>Axiom Research Dashboard</h1>
    <p class="muted">Offline, read-only market research surface. Live execution is disabled.</p>
    <p id="status"><span class="status">Loading</span></p>
  </header>
  <main>
    <section class="grid">
      <article class="panel"><h2>Experiments</h2><div id="experiment-count" class="metric">—</div><p class="muted">tracked records</p></article>
      <article class="panel"><h2>Data health</h2><div id="data-health" class="metric">—</div><p class="muted">collector grade</p></article>
      <article class="panel"><h2>Risk state</h2><div id="risk-state" class="metric">—</div><p class="muted">pre-trade gate</p></article>
      <article class="panel"><h2>Execution</h2><div class="metric">Paper only</div><p class="muted">no broker credentials or order route</p></article>
    </section>
    <section class="grid">
      <article class="panel"><h2>Strategy evaluation curves</h2><div id="curve-plot" class="muted">No evaluation data.</div></article>
      <article class="panel"><h2>Evolution</h2><pre id="evolution">Loading…</pre></article>
    </section>
    <section class="grid">
      <article class="panel"><h2>Crypto BTC/USDT</h2><pre id="crypto">Loading…</pre></article>
      <article class="panel"><h2>Prediction markets</h2><div id="prediction">Loading…</div></article>
    </section>
    <section class="grid">
      <article class="panel"><h2>Experiment provenance</h2><div id="experiments">Loading…</div></article>
      <article class="panel"><h2>Dataset health detail</h2><pre id="dataset-health-detail">Loading…</pre></article>
      <article class="panel"><h2>Risk and system health</h2><pre id="risk">Loading…</pre><pre id="system">Loading…</pre></article>
    </section>
    <p class="muted">JSON endpoints: <span id="links"></span></p>
  </main>
  <script>
    const names = ["overview", "research", "crypto", "prediction", "evolution", "risk", "system", "dataset-health"];
    const $ = (id) => document.getElementById(id);
    const safe = (value) => String(value ?? "—").replace(/[&<>"']/g, (char) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[char]));
    const show = (id, value) => { $(id).textContent = JSON.stringify(value, null, 2); };
    const finite = (value) => typeof value === "number" && Number.isFinite(value);
    function curve(values) {
      const points = values.filter(finite);
      if (!points.length) return "<p class='muted'>No numeric evaluation curve.</p>";
      const low = Math.min(...points), high = Math.max(...points), span = high - low || 1;
      const coordinates = points.map((value, index) => `${(index / Math.max(1, points.length - 1)) * 380 + 10},${140 - ((value - low) / span) * 120}`).join(" ");
      return `<svg viewBox="0 0 400 150" role="img" aria-label="strategy evaluation curve"><polyline fill="none" stroke="#60a5fa" stroke-width="3" points="${coordinates}"></polyline></svg><p class="muted">low ${low.toFixed(4)} · high ${high.toFixed(4)}</p>`;
    }
    function renderResearch(data) {
      const experiments = Array.isArray(data?.experiments) ? data.experiments : [];
      $("experiment-count").textContent = String(experiments.length);
      const rows = experiments.slice(0, 20).map((item) => {
        const evaluation = item.evaluation || {};
        const metrics = item.metrics || {};
        const train = evaluation.train_score ?? metrics.train_total_return ?? item.train?.total_return;
        const validation = evaluation.validation_score ?? metrics.validation_total_return ?? item.validation?.total_return;
        const holdout = evaluation.holdout_score ?? metrics.holdout_total_return ?? item.holdout?.total_return;
        return `<tr><td>${safe(item.strategy_id || item.strategy || "—")}</td><td>${safe(train)}</td><td>${safe(validation)}</td><td>${safe(holdout)}</td><td>${safe(evaluation.fitness ?? item.fitness ?? metrics.fitness)}</td><td>${item.rejected ? "rejected" : "eligible"}</td></tr>`;
      }).join("");
      $("experiments").innerHTML = experiments.length
        ? `<table><thead><tr><th>Strategy</th><th>Train</th><th>Validation</th><th>Holdout</th><th>Fitness</th><th>Status</th></tr></thead><tbody>${rows}</tbody></table>`
        : "<p class='muted'>No experiments tracked.</p>";
      const first = experiments[0] || {};
      const evaluation = first.evaluation || {};
      const metrics = first.metrics || {};
      $("curve-plot").innerHTML = curve([
        evaluation.train_score ?? metrics.train_total_return ?? first.train?.total_return,
        evaluation.validation_score ?? metrics.validation_total_return ?? first.validation?.total_return,
        evaluation.holdout_score ?? metrics.holdout_total_return ?? first.holdout?.total_return,
      ]);
    }
    function renderPrediction(data) {
      const markets = Array.isArray(data?.markets) ? data.markets : [];
      if (!markets.length) { $("prediction").innerHTML = "<p class='muted'>No active market snapshots.</p>"; return; }
      $("prediction").innerHTML = `<table><thead><tr><th>Market</th><th>YES mid</th><th>Liquidity</th><th>Settlement</th></tr></thead><tbody>${markets.slice(0, 20).map((item) => `<tr><td>${safe(item.question || item.market_id)}</td><td>${safe(item.yes_mid)}</td><td>${safe(item.liquidity)}</td><td>${safe(item.settlement)}</td></tr>`).join("")}</tbody></table>`;
    }
    async function load() {
      try {
        const values = await Promise.all(names.map((name) => fetch(`/api/${name}`, { cache: "no-store" }).then(async (response) => { if (!response.ok) throw new Error(`${name} HTTP ${response.status}`); return response.json(); })));
        const data = Object.fromEntries(names.map((name, index) => [name, values[index]]));
        $("status").innerHTML = "<span class='status'>Ready · read-only</span>";
        $("experiment-count").textContent = String(data.overview?.experiment_count ?? ((data.research?.experiments || []).length || 0));
        $("data-health").textContent = String(data["dataset-health"]?.grade ?? "F");
        $("risk-state").textContent = data.risk?.allowed === false ? "blocked" : "allowed";
        renderResearch(data.research);
        renderPrediction(data.prediction);
        show("crypto", data.crypto);
        show("evolution", data.evolution);
        show("risk", data.risk);
        show("dataset-health-detail", data["dataset-health"]);
        show("system", data.system);
        $("links").innerHTML = names.map((name) => `<a href="/api/${name}">${name}</a>`).join(" · ");
      } catch (error) {
        $("status").innerHTML = `<span class="status warning">Unavailable · ${safe(error.message)}</span>`;
      }
    }
    load();
  </script>
</body>
</html>"""

class _DashboardHandler(BaseHTTPRequestHandler):
    server: "_BoundDashboardServer"

    def log_message(self, format: str, *args: Any) -> None:
        return

    def _send(self, status: int, payload: Any, content_type: str = "application/json; charset=utf-8") -> None:
        body = payload.encode("utf-8") if isinstance(payload, str) else json.dumps(_jsonable(payload), sort_keys=True, indent=2, allow_nan=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802 - stdlib handler API
        path = urlparse(self.path).path.strip("/")
        if path in {"", "index.html"}:
            self._send(200, _dashboard_html(), "text/html; charset=utf-8")
            return
        endpoint = path[4:] if path.startswith("api/") else path
        if endpoint in _ENDPOINTS:
            try:
                self._send(200, self.server.dashboard_data.snapshot(endpoint))
            except Exception as exc:
                self._send(503, {"error": "data unavailable", "detail": str(exc)})
            return
        self._send(404, {"error": "not found", "endpoints": ["/", *[f"/api/{name}" for name in _ENDPOINTS]]})


class _BoundDashboardServer(ThreadingHTTPServer):
    def __init__(self, address: tuple[str, int], data: DashboardData) -> None:
        super().__init__(address, _DashboardHandler)
        self.dashboard_data = data
        self.daemon_threads = True
        self.allow_reuse_address = True


class DashboardServer:
    """Threaded local dashboard server; ``start`` is non-blocking."""

    def __init__(self, host: str = "127.0.0.1", port: int = 0, *, data: DashboardData | None = None, **data_kwargs: Any) -> None:
        self.host = host
        self.port = int(port)
        self.data = data or DashboardData(**data_kwargs)
        self._server: _BoundDashboardServer | None = None
        self._thread: Thread | None = None

    @property
    def address(self) -> tuple[str, int] | None:
        return None if self._server is None else (str(self._server.server_address[0]), int(self._server.server_address[1]))

    @property
    def url(self) -> str | None:
        address = self.address
        return None if address is None else f"http://{address[0]}:{address[1]}"

    def start(self) -> "DashboardServer":
        if self._server is not None:
            return self
        self._server = _BoundDashboardServer((self.host, self.port), self.data)
        self._thread = Thread(target=self._server.serve_forever, name="axiom-dashboard", daemon=True)
        self._thread.start()
        return self

    def serve_forever(self) -> None:
        if self._server is None:
            self._server = _BoundDashboardServer((self.host, self.port), self.data)
        self._server.serve_forever()

    def stop(self) -> None:
        if self._server is None:
            return
        self._server.shutdown()
        self._server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=2)
        self._server = None
        self._thread = None

    close = stop

    def __enter__(self) -> "DashboardServer":
        return self.start()

    def __exit__(self, *_: Any) -> None:
        self.stop()


Dashboard = DashboardServer


def create_dashboard_server(host: str = "127.0.0.1", port: int = 0, **kwargs: Any) -> DashboardServer:
    return DashboardServer(host, port, **kwargs)


def serve_dashboard(host: str = "127.0.0.1", port: int = 8080, **kwargs: Any) -> None:
    server = DashboardServer(host, port, **kwargs)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.stop()


__all__ = ["DashboardData", "DashboardServer", "Dashboard", "create_dashboard_server", "serve_dashboard"]
