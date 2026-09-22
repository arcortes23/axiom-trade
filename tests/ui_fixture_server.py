"""Sanitized, loopback-only UI fixture service.

The fixture deliberately uses the production DashboardServer handler and its
CSRF/host checks. Only the data facade and fake control transport are synthetic;
no database, credentials, venue transport, process, or external network is used.
"""
from __future__ import annotations

from copy import deepcopy
from pathlib import Path
import argparse
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from urllib.parse import parse_qs, unquote, urlparse
from urllib.error import HTTPError as UrllibHTTPError, URLError
from urllib.request import Request as UrllibRequest, urlopen as urllib_urlopen
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

from typing import Any, Callable, Mapping
from axiom.canary_settings import CanarySettingsService, CanarySettingsValidationError
from axiom.dashboard import DashboardData, DashboardServer, _ENDPOINTS, _V2_ENDPOINTS, _pagination_error


FIXTURE_TIME = "2034-02-03T04:05:06+00:00"
SCENARIOS = (
    "prepared",
    "legacy_missingfield",
    "no_proposal",
    "positive_proposed_zero_active",
    "poll",
    "reload",
    "stale",
    "changed",
    "missingaccount",
    "missingauth",
    "adverse_required",
    "adverse_not_required",
    "armed_no_permission",
    "no_members",
    "unaffordable",
    "network_failure",
    "active_no_signal",
    "open_position",
    "partial_fill",
    "unknown_order",
    "expired",
    "revoked",
    "missing",
    "empty",
)
_FINAL_REVIEW_ACTIONS = frozenset({
    "exploratory.live.review_confirm",
    "exploratory_live.review_confirm",
    "canary.exploratory_live.review_confirm",
})


def _page(items: list[dict[str, Any]], *, page_size: int = 25, total: int | None = None) -> dict[str, Any]:
    total_count = len(items) if total is None else total
    pages = max(1, (total_count + page_size - 1) // page_size) if total_count else 0
    return {"items": items, "page": 1, "page_size": page_size, "total": total_count, "pages": pages}


def _record(kind: str, index: int = 1, **extra: Any) -> dict[str, Any]:
    return {
        "id": f"fixture-{kind}-{index}",
        "name": f"Fixture {kind.replace('_', ' ').title()} {index}",
        "status": "COMPLETE",
        "updated_at": FIXTURE_TIME,
        **extra,
    }


_TIMESTAMP_KEYS = {"timestamp", "updated_at", "created_at", "observed_at", "checked_at", "heartbeat_at", "expires_at", "next_schedule", "next_evaluation_at", "available_at", "readiness_snapshot_updated_at", "submitted_at", "filled_at", "opened_at", "marked_at", "last_tick_at", "acquired_at", "start", "end"}
def _retime(value: Any, stamp: str) -> Any:
    base = datetime.fromisoformat(FIXTURE_TIME)
    current = datetime.fromisoformat(stamp)
    if isinstance(value, dict):
        result: dict[str, Any] = {}
        for key, item in value.items():
            if key in _TIMESTAMP_KEYS and isinstance(item, str):
                try:
                    original = datetime.fromisoformat(item)
                    item = (current + (original - base)).isoformat()
                except ValueError:
                    item = stamp
            result[key] = _retime(item, stamp) if key not in _TIMESTAMP_KEYS else item
        return result
    if isinstance(value, list):
        return [_retime(item, stamp) for item in value]
    return value
def _catalog_variants(seed: dict[str, Any], *, kind: str, identity_key: str, count: int = 31) -> list[dict[str, Any]]:
    rows = [deepcopy(seed)]
    base = datetime.fromisoformat(FIXTURE_TIME)
    for index in range(2, count + 1):
        row = deepcopy(seed)
        identifier = f"fixture-{kind}-{index:02d}"
        row["id"] = identifier
        row[identity_key] = identifier
        row["name"] = f"{seed.get('name', kind.title())} {index:02d}"
        for key in _TIMESTAMP_KEYS:
            original = row.get(key)
            if isinstance(original, str):
                try:
                    offset = timedelta(minutes=index if key == "next_schedule" else -index)
                    row[key] = (base + offset).isoformat()
                except (TypeError, ValueError):
                    pass
        if kind == "market":
            row["question"] = f"Fixture market question {index:02d}"
            row["category"] = ("Politics", "Sports", "Weather")[index % 3]
            row["market_type"] = ("prediction", "crypto_spot")[index % 2]
            row["settlement"] = ("open", "resolved_yes", "resolved_no", "void")[index % 4]
            row["quality"] = ("PRICE_PROXY", "ORDER_BOOK_SIMULATED")[index % 2]
            row["source_type"] = "FORWARD_COLLECTED" if index % 2 else "HISTORICAL"
            row["source"] = row["source_type"]
            row["timeframe"] = ("1m", "1h", "1d", "live")[index % 4]
        elif kind == "candidate":
            row["candidate_id"] = identifier
            row["stage"] = ("IDEA", "SCHEMA_VALIDATED", "BACKTESTED", "VALIDATED", "ROBUSTNESS_CHECKED", "FROZEN", "PAPER_FORWARD", "PAPER_PROMOTABLE", "REJECTED")[index % 9]
            row["data_quality"] = ("PRICE_PROXY", "TIMESTAMPED_DEPTH", "CURRENT_ORDER_BOOK")[index % 3]
            row["family"] = f"fixture family {index:02d}"
            row["events"] = [{"event_id": f"{identifier}-event-1", "timestamp": row.get("updated_at", FIXTURE_TIME), "stage": row["stage"], "title": f"Fixture candidate event {index:02d}", "reason": "Bounded fixture evidence"}]
        elif kind == "dataset":
            row["dataset_id"] = identifier
            row["dataset_version"] = f"fixture-v{index}"
            row["source_type"] = "FORWARD_COLLECTED" if index % 2 else "HISTORICAL"
            row["source"] = row["source_type"]
            row["market_type"] = ("prediction", "crypto_spot")[index % 2]
            row["quality"] = ("OHLCV", "PRICE_PROXY", "ORDER_BOOK_SIMULATED")[index % 3]
            row["timeframe"] = ("1m", "1h", "1d", "live")[index % 4]
            row["missing_ranges"] = [{"start": (base - timedelta(hours=index + 1)).isoformat(), "end": (base - timedelta(hours=index)).isoformat(), "kind": "missing", "reason": "No fixture observation"}]
        elif kind == "crypto":
            row["symbol"] = f"AUR{index:02d}X"
            row["source_type"] = "FORWARD_COLLECTED" if index % 2 else "HISTORICAL"
            row["source"] = row["source_type"]
            row["market_type"] = "crypto_spot"
            row["instrument"] = row["symbol"]
            row["coverage"] = "CATALOG_ONLY" if index % 2 else "PARTIAL"
            row["quality"] = "REVIEW" if index % 3 else "FRESH"
        elif kind == "hermes":
            row["job_id"] = identifier
            row["item_id"] = identifier
            row["status"] = ("PENDING", "COMPLETE", "FAILED")[index % 3]
            row["title"] = f"Fixture queue item {index:02d}"
            row["human_reason"] = f"Fixture queue reason {index:02d}"
            row["reason"] = row["human_reason"]
        elif kind == "activity":
            row["id"] = identifier
            row["event_id"] = identifier
            row["kind"] = ("research", "trading", "system")[index % 3]
            row["source"] = "fixture"
            row["source_type"] = ("fixture_catalog", "fixture_canary", "fixture_system")[index % 3]
            row["status"] = ("INFO", "NOTICE", "WARNING")[index % 3]
            row["title"] = f"Fixture activity event {index:02d}"
            row["message"] = f"Fixture activity message {index:02d}"
            row["financial"] = index % 10 == 0
            row["details"] = {"source": "fixture", "record": identifier}
        rows.append(row)
    return rows
def _base_data(now: datetime | None = None) -> dict[str, Any]:
    stamp = (now or datetime.fromisoformat(FIXTURE_TIME)).astimezone(timezone.utc).isoformat()
    market = {
        "id": "fixture-market-aurora",
        "market_id": "fixture-market-aurora",
        "event_id": "fixture-event-aurora",
        "token_id": "fixture-token-aurora-yes",
        "question": "Will the Aurora index close above 61 by the fixture cutoff?",
        "name": "Aurora index close",
        "outcome": "YES / NO",
        "snapshot": {
            "yes_mid": "0.4100",
            "yes_ask": "0.4200",
            "yes_bid": "0.4000",
            "no_mid": "0.5900",
            "no_ask": "0.6000",
            "no_bid": "0.5800",
        },
        "midpoint": "0.4100",
        "buy_ask": "0.4200",
        "category": "Politics",
        "market_type": "prediction",
        "settlement": "open",
        "freshness": "FRESH",
        "quality": "PRICE_PROXY",
        "source_type": "FORWARD_COLLECTED",
        "source": "FORWARD_COLLECTED",
        "timeframe": "live",
        "strategy_use": "OBSERVATION_ONLY",
        "depth": "12.40",
        "min_affordable": "0.43",
        "observed_at": FIXTURE_TIME,
    }
    candidate = {
        "id": "fixture-candidate-aurora",
        "candidate_id": "fixture-candidate-aurora",
        "name": "Aurora threshold study",
        "family": "fixture momentum",
        "data_quality": "PRICE_PROXY",
        "stage": "PAPER_FORWARD",
        "payload": {
            "operational_setup": "One fresh observation above the reviewed threshold",
            "strategy_document": "Close on settlement or invalidated observation",
            "canonical_strategy": "FIXTURE_MOMENTUM_V1",
            "exit_policy": "Settlement or invalidation",
            "data_quality": "PRICE_PROXY",
        },
        "provenance": {"source": "FIXTURE_CATALOG", "captured_at": FIXTURE_TIME},
        "entry_setup": "One fresh observation above the reviewed threshold",
        "exit_setup": "Close on settlement or invalidated observation",
        "evidence_status": "ADVERSE_RETAINED",
        "paper_forward_status": "OBSERVED",
        "live_status": "NOT_SELECTED",
        "next_work": "Review the next bounded observation",
        "updated_at": FIXTURE_TIME,
    }
    candidate["events"] = [
        {
            "event_id": f"fixture-event-aurora-{index}",
            "timestamp": (datetime.fromisoformat(FIXTURE_TIME) - timedelta(hours=index)).isoformat(),
            "stage": ("IDEA", "SCHEMA_VALIDATED", "BACKTESTED", "VALIDATED", "ROBUSTNESS_CHECKED", "FROZEN", "PAPER_FORWARD", "PAPER_PROMOTABLE", "REJECTED")[index % 9],
            "title": f"Fixture evidence observation {index}",
            "reason": "Bounded fixture evidence remains visible",
        }
        for index in range(1, 19)
    ]
    dataset = {
        "id": "fixture-dataset-aurora",
        "dataset_id": "fixture-dataset-aurora",
        "name": "Aurora observations",
        "dataset_version": "fixture-v3",
        "source_type": "FORWARD_COLLECTED",
        "source": "FORWARD_COLLECTED",
        "market_type": "prediction",
        "instrument": "fixture-aurora",
        "timeframe": "1h",
        "coverage": "PARTIAL",
        "quality": "PRICE_PROXY",
        "updated_at": FIXTURE_TIME,
        "health": None,
    }
    dataset["missing_ranges"] = [
        {
            "start": (datetime.fromisoformat(FIXTURE_TIME) - timedelta(hours=index + 1)).isoformat(),
            "end": (datetime.fromisoformat(FIXTURE_TIME) - timedelta(hours=index)).isoformat(),
            "kind": "missing",
            "reason": "No fixture observation",
        }
        for index in range(11, 29)
    ]
    crypto = {
        "id": "fixture-crypto-aurora",
        "symbol": "AURX",
        "name": "Aurora synthetic asset",
        "universe_version": "fixture-universe-v2",
        "coverage": "CATALOG_ONLY",
        "source_type": "FORWARD_COLLECTED",
        "source": "FORWARD_COLLECTED",
        "market_type": "crypto_spot",
        "instrument": "AURX",
        "report_status": "PARKED",
        "quality": "REVIEW",
        "updated_at": FIXTURE_TIME,
    }
    hermes = {
        "id": "fixture-hermes-aurora",
        "job_id": "fixture-hermes-aurora",
        "item_id": "fixture-hermes-aurora",
        "title": "Aurora evidence refresh",
        "name": "Aurora evidence refresh",
        "human_reason": "Refresh the bounded fixture evidence before review",
        "available_at": "2034-02-03T04:15:06+00:00",
        "next_schedule": "2034-02-03T04:15:06+00:00",
        "status": "PENDING",
        "connection": "FIXTURE_CONNECTED",
        "last_error": None,
        "updated_at": FIXTURE_TIME,
    }
    order = {
        "id": "fixture-order-aurora",
        "attempt_id": "fixture-submission-aurora",
        "record_kind": "submission",
        "question": market["question"],
        "side": "BUY",
        "status": "RESTING",
        "stage": "ATTEMPTED",
        "quantity": "1",
        "price": "0.42",
        "timestamp": FIXTURE_TIME,
    }
    fill = {
        "fill_id": "fixture-fill-aurora",
        "record_kind": "risk-fill",
        "question": market["question"],
        "side": "BUY",
        "filled_quantity": "0.25",
        "fill_price": "0.42",
        "fee": "0.0001",
        "timestamp": FIXTURE_TIME,
    }
    position_request = {
        "request_id": "fixture-position-request-aurora",
        "record_kind": "order",
        "position_id": "fixture-position-aurora",
        "reservation_id": "fixture-reservation-aurora",
        "event_id": market["event_id"],
        "venue": "POLYMARKET",
        "market_id": market["market_id"],
        "token_id": market["token_id"],
        "side": "BUY",
        "requested_quantity": "1",
        "requested_price": "0.42",
        "filled_quantity": "0.25",
        "average_price": "0.42",
        "fees": "0.0001",
        "status": "PARTIAL",
        "expected_generation": 3,
        "config_id": "fixture-risk-active",
        "submitted_at": FIXTURE_TIME,
        "updated_at": FIXTURE_TIME,
        "last_error": None,
        "settlement_status": "PENDING",
    }
    position_fill = {
        "fill_id": "fixture-position-fill-aurora",
        "record_kind": "fill",
        "request_id": position_request["request_id"],
        "position_id": position_request["position_id"],
        "quantity": "0.25",
        "price": "0.42",
        "fee": "0.0001",
        "status": "SETTLED",
        "filled_at": FIXTURE_TIME,
    }
    reservation = {
        "reservation_id": "fixture-reservation-aurora",
        "record_kind": "reservation",
        "intent_id": position_request["request_id"],
        "request_id": position_request["request_id"],
        "position_id": position_request["position_id"],
        "venue": "POLYMARKET",
        "market_id": market["market_id"],
        "event_id": market["event_id"],
        "side": "BUY",
        "requested_cost": "0.105",
        "filled_cost": "0.105",
        "remaining_cost": "0.00",
        "fee_reserve": "0.0001",
        "quantity": "0.25",
        "reserved_quantity": "0.25",
        "filled_quantity": "0.25",
        "reserved_cost": "0.105",
        "status": "SETTLED",
        "created_at": FIXTURE_TIME,
        "submitted_at": FIXTURE_TIME,
        "reserved_at": FIXTURE_TIME,
        "updated_at": FIXTURE_TIME,
    }
    risk_fill = {
        "fill_id": "fixture-risk-fill-aurora",
        "record_kind": "risk-fill",
        "reservation_id": reservation["reservation_id"],
        "request_id": position_request["request_id"],
        "position_id": position_request["position_id"],
        "quantity": "0.25",
        "price": "0.42",
        "cost": "0.105",
        "fee": "0.0001",
        "status": "SETTLED",
        "filled_at": FIXTURE_TIME,
    }
    position = {
        "position_id": position_request["position_id"],
        "record_kind": "position",
        "reservation_id": reservation["reservation_id"],
        "event_id": market["event_id"],
        "venue": "POLYMARKET",
        "market_id": market["market_id"],
        "token_id": market["token_id"],
        "question": market["question"],
        "side": "BUY",
        "quantity": "0.25",
        "sold_quantity": "0",
        "cost_basis": "0.105",
        "fees": "0.0001",
        "gross_proceeds": "0",
        "exit_fees": "0",
        "realized_pnl": "0.0000",
        "pending_exit_quantity": "0",
        "marked_value": "0.105",
        "status": "OPEN",
        "opened_at": FIXTURE_TIME,
        "updated_at": FIXTURE_TIME,
    }
    mark = {
        "mark_id": "fixture-mark-aurora",
        "record_kind": "mark",
        "position_id": position["position_id"],
        "token_id": market["token_id"],
        "side": "SELL",
        "quantity": "0.25",
        "mark_price": "0.42",
        "cost_basis_usd": "0.105",
        "mark_fee": "0",
        "marked_value": "0.105",
        "status": "CURRENT",
        "observed_at": FIXTURE_TIME,
        "marked_at": FIXTURE_TIME,
        "source": "FIXTURE_SOURCE",
    }
    cashflow = {
        "flow_id": "fixture-cashflow-aurora",
        "record_kind": "cashflow",
        "position_id": position["position_id"],
        "market_id": market["market_id"],
        "kind": "EXTERNAL",
        "amount": "0.105",
        "occurred_at": FIXTURE_TIME,
    }
    activity = [
        {"id": "fixture-activity-1", "event_id": "fixture-activity-1", "timestamp": FIXTURE_TIME, "kind": "research", "source": "fixture", "source_type": "fixture_catalog", "status": "INFO", "title": "Fixture evidence retained", "message": "Fixture evidence retained", "details": {"source": "fixture"}},
        {"id": "fixture-activity-2", "event_id": "fixture-activity-2", "timestamp": FIXTURE_TIME, "kind": "research", "source": "fixture", "source_type": "fixture_catalog", "status": "INFO", "title": "Fixture evidence retained", "message": "Fixture evidence retained", "details": {"source": "fixture"}},
        {"id": "fixture-activity-3", "event_id": "fixture-activity-3", "timestamp": FIXTURE_TIME, "kind": "trading", "source": "fixture", "source_type": "fixture_canary", "status": "NOTICE", "title": "Fixture order observed", "message": "Fixture order observed", "financial": True, "details": {"order_id": order["id"]}},
    ]
    markets = _catalog_variants(market, kind="market", identity_key="market_id")
    candidates = _catalog_variants(candidate, kind="candidate", identity_key="candidate_id")
    datasets = _catalog_variants(dataset, kind="dataset", identity_key="dataset_id")
    crypto_catalog = _catalog_variants(crypto, kind="crypto", identity_key="symbol")
    hermes_jobs = _catalog_variants(hermes, kind="hermes", identity_key="job_id")
    activity_rows = _catalog_variants(activity[0], kind="activity", identity_key="id")
    activity_rows[:3] = [deepcopy(item) for item in activity]
    shadow_jobs = [_record("shadow", 1, job_id="fixture-shadow-aurora", status="PARKED", heartbeat_at=FIXTURE_TIME, venue="PAPER")]
    for index in range(2, 26):
        heartbeat_stamp = (datetime.fromisoformat(FIXTURE_TIME) - timedelta(minutes=index)).isoformat()
        next_evaluation_stamp = (datetime.fromisoformat(FIXTURE_TIME) + timedelta(minutes=index)).isoformat()
        shadow_jobs.append(_record("shadow", index, job_id=f"fixture-shadow-{index:02d}", status=("PARKED", "COMPLETE", "BLOCKED")[index % 3], heartbeat_at=heartbeat_stamp, next_evaluation_at=next_evaluation_stamp, venue="PAPER"))
    def _native_authority() -> dict[str, Any]:
        scope_draft = {
            "draft_id": "fixture-scope-draft-aurora",
            "draft_hash": "sha256:fixture-scope-draft",
            "scope_version": "fixture-scope-v1",
            "scope_hash": "sha256:fixture-scope",
            "status": "REVIEW_REQUIRED",
            "live_execution": False,
            "paper_only": True,
            "scope": {
                "schema_version": "fixture-market-scope-v1",
                "mode": "EXACT_MARKETS",
                "instrument": "POLYMARKET",
                "market_ids": ["fixture-market-aurora"],
                "categories": ["fixture"],
                "filters": {"market_id": "fixture-market-aurora"},
                "provenance": "FIXTURE_SOURCE",
                "regime_restrictions": {},
            },
            "category_restriction": {"mode": "ALLOWLIST", "categories": ["fixture"]},
            "supported_market_types": ["BINARY"],
            "exclusions": [],
        }
        setup_binding = {
            "candidate_id": "fixture-candidate-aurora",
            "draft_bound": True,
            "draft_hash": "sha256:fixture-setup-draft",
            "draft_id": "fixture-setup-draft-aurora",
            "market_bindings": ["fixture-market-aurora"],
            "operational_setup_hash": "sha256:fixture-operational-setup",
            "scope_hash": scope_draft["scope_hash"],
            "scope_version": scope_draft["scope_version"],
            "setup_hash": "sha256:fixture-setup",
            "setup_id": "fixture-setup-aurora",
            "setup_name": "Fresh observation threshold",
            "setup_version": "fixture-setup-v1",
            "strategy_hash": "sha256:fixture-strategy",
            "strategy_name": "Aurora threshold study",
            "strategy_version": "fixture-strategy-v1",
            "strategy_version_id": "fixture-strategy-v1",
            "name": "Fresh observation threshold",
            "entry_predicate": "One fresh observation above the reviewed threshold",
            "direction": {"allowed": ["BUY"], "selected": "BUY"},
            "sizing": {"mode": "FIXED_NOTIONAL", "target_notional_usd": "0.42"},
            "holding_semantics": "Until settlement or invalidation",
            "exit_semantics": "Close on settlement or invalidated observation",
            "lookback": {"forward_days": 7, "resolved_bets": 30},
            "operational_setup": {
                "name": "Fresh observation threshold",
                "setup_id": "fixture-setup-aurora",
                "setup_version": "fixture-setup-v1",
                "setup_hash": "sha256:fixture-setup",
                "operational_setup_hash": "sha256:fixture-operational-setup",
                "entry_predicate": "One fresh observation above the reviewed threshold",
                "direction": {"allowed": ["BUY"], "selected": "BUY"},
                "sizing": {"mode": "FIXED_NOTIONAL", "target_notional_usd": "0.42"},
                "holding_semantics": "Until settlement or invalidation",
                "exit_semantics": "Close on settlement or invalidated observation",
                "lookback": {"forward_days": 7, "resolved_bets": 30},
            },
            "strategy": {
                "name": "Aurora threshold study",
                "strategy_version_id": "fixture-strategy-v1",
                "version": "fixture-strategy-v1",
                "strategy_hash": "sha256:fixture-strategy",
                "family": "fixture momentum",
            },
            "strategy_document": {
                "name": "Aurora threshold study",
                "canonical_strategy": "FIXTURE_MOMENTUM_V1",
                "source": "FIXTURE_SOURCE",
            },
            "frozen_scope": deepcopy(scope_draft["scope"]),
            "scope_restrictions": {
                "category_restriction": deepcopy(scope_draft["category_restriction"]),
                "supported_market_types": list(scope_draft["supported_market_types"]),
            },
            "exclusions": deepcopy(scope_draft["exclusions"]),
        }
        proposal_member = {
            "allocation": "2.50",
            "allocation_active": False,
            "candidate_id": "fixture-candidate-aurora",
            "proposed_allocation": "2.50",
            "setup_binding": setup_binding,
            "setup_name": setup_binding["setup_name"],
            "status": "SELECTED",
            "strategy_name": setup_binding["strategy_name"],
            "strategy_version_id": "fixture-strategy-v1",
        }
        proposal_member_two = deepcopy(proposal_member)
        proposal_member_two.update({
            "allocation": "2.50",
            "candidate_id": "fixture-candidate-borealis",
            "proposed_allocation": "2.50",
            "setup_name": "Borealis threshold study",
            "strategy_name": "Borealis threshold study",
        })
        setup_binding_two = deepcopy(setup_binding)
        setup_binding_two.update({
            "candidate_id": "fixture-candidate-borealis",
            "setup_id": "fixture-setup-borealis",
            "setup_name": "Borealis threshold study",
            "name": "Borealis threshold study",
            "strategy_name": "Borealis threshold study",
        })
        proposal_member_two["setup_binding"] = setup_binding_two
        proposal = {
            "status": "REVIEW_REQUIRED",
            "selection_id": "fixture-selection-aurora",
            "selection_hash": "sha256:fixture-selection",
            "policy_id": "fixture-policy-rolling",
            "policy_version": "fixture-policy-v1",
            "policy_hash": "sha256:fixture-policy",
            "scope_draft_id": scope_draft["draft_id"],
            "scope_draft_version": scope_draft["scope_version"],
            "scope_draft_hash": scope_draft["draft_hash"],
            "proposed_allocation_total": "5.00",
            "proposed_allocation_risk_digest": "sha256:fixture-risk-digest",
            "members": [proposal_member, proposal_member_two],
            "selected_setups": [deepcopy(setup_binding), deepcopy(setup_binding_two)],
            "setup_bindings": [deepcopy(setup_binding), deepcopy(setup_binding_two)],
        }
        auth_draft = {
            "active_settings_generation": 2,
            "active_settings_hash": "sha256:fixture-risk-active",
            "adverse_evidence_ack": {"acknowledged": False, "required": False},
            "authorization_id": "fixture-execution-authorization",
            "duration_seconds": 86400,
            "exact_strategy_versions": ["fixture-strategy-v1"],
            "expires_at": None,
            "expiry_anchor": "FINAL_CONFIRMATION",
            "generation": 3,
            "lifetime_budget": {"max_notional_usd": "5.00"},
            "shared_allocation": "5.00",
            "live_execution": False,
            "mode": "EXPLORATORY_MICRO_CANARY",
            "paper_only": True,
            "purpose": "commission exploratory automation and measure actual net results; profitability unproven",
            "reviewed_selection_policy_hash": proposal["policy_hash"],
            "scope_hash": scope_draft["scope_hash"],
            "scope_version": scope_draft["scope_version"],
            "selection_hash": proposal["selection_hash"],
            "selection_id": proposal["selection_id"],
            "selection_policy_hash": proposal["policy_hash"],
            "status": "DRAFT",
            "stop_rules": {"halt_on_unknown_execution": True, "on_any_blocker": "STOP_AND_REVIEW"},
            "strategy_version_ids": ["fixture-strategy-v1"],
        }
        controller_lease = {
            "acquired_at": FIXTURE_TIME,
            "expires_at": "2034-02-03T04:20:27+00:00",
            "generation": 3,
            "owner_id": "fixture-controller",
            "status": "AVAILABLE",
            "updated_at": FIXTURE_TIME,
        }
        identity = {
            "database": "fixture://axiom-ui",
            "db_path": "fixture://axiom-ui",
            "instance_id": "fixture-instance",
            "pid": 0,
            "process_identity": "fixture-process",
            "revision": "fixture-revision",
            "service": "axiom.fixture.CanaryService",
            "service_identity": "axiom.fixture.CanaryService",
        }
        risk_limits = {
            "cumulative_buy_cap_usd": None,
            "equity_loss_entry_stop_usd": "0.25",
            "max_aggregate_exposure_usd": "0.73",
            "max_aggregate_open_cost_usd": "0.73",
            "max_all_in_buy_usd": "0.73",
            "max_daily_loss_usd": "0.25",
            "max_exposure_usd": "0.73",
            "max_fee_reserve_usd": "0.01",
            "max_gross_daily_buy_usd": "0.73",
            "max_open_positions": 1,
            "max_orders_per_day": 2,
            "max_positions": 1,
            "max_slippage_bps": 100,
            "max_submitted_orders_per_day": 2,
            "per_event_buy_cap_usd": None,
            "per_market_buy_cap_usd": None,
            "realized_loss_entry_stop_usd": "0.25",
            "target_notional_usd": "0.42",
        }
        risk_settings = {
            "active": {
                "config_hash": "sha256:fixture-risk-active",
                "config_id": "fixture-risk-active",
                "generation": 2,
                "settings": risk_limits,
                "values": risk_limits,
            },
            "config_hash": "sha256:fixture-risk-active",
            "config_id": "fixture-risk-active",
            "control_generation": 3,
            "cumulative_buy_cap_usd": None,
            "cumulative_buy_over_limit_reason": None,
            "effective_limits": risk_limits,
            "generation": 2,
            "remaining": {
                "aggregate_exposure_usd": "0.625",
                "aggregate_open_cost_usd": "0.625",
                "all_in_buy_usd": "0.625",
                "cumulative_buy_usd": None,
                "equity_loss_usd": "0.25",
                "exploratory_lifetime_orders": None,
                "exploratory_lifetime_usd": "0.73",
                "gross_daily_buy_usd": "0.625",
                "positions": 0,
                "realized_loss_usd": "0.25",
                "slippage_bps": 100,
                "submitted_orders": 1,
            },
            "remaining_cumulative_buy_usd": None,
            "status": "CURRENT",
            "usage": {
                "accounting_day_pht": "2034-02-03",
                "aggregate_exposure_usd": "0.105",
                "aggregate_open_cost_usd": "0.105",
                "all_in_buy_reserved_usd": "0.105",
                "buy_filled_usd": "0.105",
                "buy_pending_usd": "0.00",
                "buy_unknown_usd": "0.00",
                "cumulative_buy_usd": "0.105",
                "equity_loss_usd": "0.00",
                "equity_status": "CURRENT",
                "execution_authorization_id": None,
                "exploratory_lifetime_orders": 1,
                "exploratory_lifetime_used_usd": "0.105",
                "external_flow_usd": "0.105",
                "gross_daily_buy_usd": "0.105",
                "lifetime_buy_usd": "0.105",
                "lifetime_orders": 1,
                "submitted_orders": 1,
                "lineage": {"source": "FIXTURE_SOURCE"},
                "open_lot_slots": 0,
                "open_market_ids": ["fixture-market-aurora"],
                "open_positions": 1,
                "open_quantity_by_market": {"fixture-market-aurora": "0.25"},
                "pending_sell_quantity_by_market": {},
                "per_event_buy_usd": {"fixture-event-aurora": "0.105"},
                "per_market_buy_usd": {"fixture-market-aurora": "0.105"},
                "realized_loss_usd": "0.00",
                "risk_breaker": None,
                "rolling_global_budget_usd": "1.37",
                "rolling_global_reserved_usd": "0.00",
                "rolling_strategy_allocations": {"fixture-strategy-v1": "0.73"},
                "rolling_strategy_reserved_usd": {"fixture-strategy-v1": "0.00"},
                "aggregate_open_cost_usd": "0.105",
            },
        }
        choices = {
            "approved": False,
            "duration_seconds": auth_draft["duration_seconds"],
            "expires_at": auth_draft["expires_at"],
            "expiry_anchor": auth_draft["expiry_anchor"],
            "lifetime_budget": auth_draft["lifetime_budget"],
            "purpose": auth_draft["purpose"],
            "shared_allocation": "5.00",
            "status": "REVIEW_REQUIRED",
            "stop_rules": auth_draft["stop_rules"],
            "adverse_evidence_ack_required": False,
        }
        session_setup = {
            "supported": True,
            "required": False,
            "action": "exploratory.live.prepare",
            "confirmation": "PREPARE EXPLORATORY SESSION",
            "fixed_allocation": True,
            "shared_allocation": "5.00",
            "lifetime_budget": {"max_notional_usd": "5.00"},
            "expiry_anchor": "FINAL_CONFIRMATION",
            "duration_seconds": 86400,
            "proposal_only": True,
        }
        review = {
            "status": "REVIEW_REQUIRED",
            "proposal_status": "REVIEW_REQUIRED",
            "proposal": proposal,
            "scope": scope_draft["scope"],
            "scope_binding": {"draft_id": scope_draft["draft_id"], "draft_hash": scope_draft["draft_hash"], "scope_hash": scope_draft["scope_hash"], "scope_version": scope_draft["scope_version"]},
            "authorization": deepcopy(auth_draft),
            "choices": choices,
            "authorization_choices": choices,
            "authorization_bindings": {"selection_id": proposal["selection_id"], "selection_hash": proposal["selection_hash"], "policy_id": proposal["policy_id"], "policy_version": proposal["policy_version"], "policy_hash": proposal["policy_hash"], "scope_draft_id": scope_draft["draft_id"], "scope_draft_hash": scope_draft["draft_hash"], "scope_draft_version": scope_draft["scope_version"]},
            "session_setup": session_setup,
            "members": [proposal_member, proposal_member_two],
            "selected_setups": [setup_binding, setup_binding_two],
            "setup_bindings": [setup_binding, setup_binding_two],
            "entry_predicate": {"status": "ELIGIBLE", "expression": "fixture_entry_predicate_v1"},
            "direction": {"allowed": ["BUY"], "selected": "BUY"},
            "sizing": {"mode": "FIXED_NOTIONAL", "target_notional_usd": "0.42"},
            "exit": {"mode": "STOP_AND_REVIEW", "on_unknown_execution": True},
            "lookback": {"forward_days": 7, "resolved_bets": 30},
            "adverse_evidence": {"required": False, "acknowledged": False, "status": "NOT_REQUIRED", "items": []},
            "risk_limits": deepcopy(risk_limits),
            "limits": risk_limits,
            "limit_blockers": [],
            "affordability": {"status": "AFFORDABLE", "remaining_exploratory_lifetime_usd": "0.73"},
            "readiness": {"status": "CURRENT", "checks": ["scope", "setup", "risk", "worker"], "checked_at": FIXTURE_TIME},
            "no_member_reason": None,
            "shared_allocation": choices["shared_allocation"],
            "lifetime_budget": choices["lifetime_budget"],
            "expiry_anchor": choices["expiry_anchor"],
            "duration_seconds": choices["duration_seconds"],
            "expires_at": choices["expires_at"],
            "stop_rules": choices["stop_rules"],
            "paper_only": True,
            "live_execution": False,
        }
        execution_authorization = {
            "status": "REVOKED",
            "active": None,
            "expires_at": None,
            "controller_lease": controller_lease,
            "draft": auth_draft,
            "generation": auth_draft["generation"],
            "identity": identity,
            "live_execution": False,
            "mode": auth_draft["mode"],
            "paper_only": True,
            "rolling_exploratory_scope_draft": scope_draft,
        }
        operator_controls = {
            "armed": False,
            "armed_state": "DISARMED",
            "controller_lease": controller_lease,
            "execution_authorization": execution_authorization,
            "exploratory_live_review": review,
            "scope_draft": scope_draft,
            "rolling_exploratory_scope_draft": scope_draft,
            "risk_settings": risk_settings,
            "blockers": [],
            "paper_only": True,
            "live_execution": False,
        }
        canary = {
            "fixture": True,
            "fixture_banner": "FIXTURE · synthetic bounded projection",
            "production_live_trading": False,
            "paper_only": True,
            "micro_live_canary": "ACTIVE",
            "display_state": "DISABLED",
            "control_state": "DISARMED",
            "selection_status": "NO_SIGNAL",
            "readiness_snapshot_status": "CURRENT",
            "readiness_snapshot_stale": False,
            "readiness_snapshot_updated_at": FIXTURE_TIME,
            "execution_authorization": execution_authorization,
            "worker": {"last_tick_at": FIXTURE_TIME, "tick_count": 12, "next_decision": "NO_SIGNAL", "blocker": None},
            "autonomous": {"enabled": False, "control_state": "DISARMED", "next_decision": "NO_SIGNAL"},
            "operator_controls": operator_controls,
            "readiness": {"status": "READY", "checked_at": FIXTURE_TIME},
            "decision": {"status": "NO_SIGNAL", "reason_code": "NO_SIGNAL"},
            "execution": {
                "event_count": 1,
                "real_execution_events": 1,
                "today_orders": 1,
                "today_realized_pnl": "0.0000",
                "total_exposure": "0.105",
                "open_positions": 1,
                "last_request_status": "NO_SIGNAL",
                "orders": [order],
                "fills": [fill],
                "closed_round_trips": [],
                "resolution_payouts": [cashflow],
                "open_inventory": [position],
                "unknown_obligations": [],
                "canary_submission_attempts": [order],
                "canary_position_requests": [position_request],
                "canary_risk_reservations": [reservation],
                "canary_risk_fills": [risk_fill],
                "canary_position_fills": [position_fill],
                "canary_position_lots": [position],
                "position_marks": [mark],
                "canary_equity_marks": [mark],
                "canary_risk_cashflows": [cashflow],
            },
            "risk_settings": risk_settings,
            "items": [order],
        }
        return canary
    canary = _native_authority()
    payload = {
        "operator": {
            "fixture": True,
            "fixture_banner": "FIXTURE · synthetic data only",
            "production_live_trading": False,
            "paper_only": True,
            "worker": canary["worker"],
            "autonomous": canary["autonomous"],
            "operator_controls": canary["operator_controls"],
            "execution_authorization": canary["execution_authorization"],
            "risk_settings": canary["risk_settings"],
            "latest_action": "FIXTURE_READ_ONLY",
            "research": {"status": "LIVE"},
        },
        "overview-summary": {"fixture": True, "fixture_banner": "FIXTURE · synthetic bounded projection", "counts": {"markets": 31, "candidates": 31, "datasets": 31}, "latest_action": "FIXTURE_READ_ONLY"},
        "canary": canary,
        "execution_authorization": canary["execution_authorization"],
        "risk-settings": canary["risk_settings"],
        "paper": _page([_record("practice", 1, question=market["question"], stage="PAPER", status="OPEN", known_result="0.00", simulated=True)]),
        "rolling-portfolio": {"fixture": True, "status": "REVIEW", "active_member_count": 0, "shared_allocation": "1.37", "active_rows": [], "policy_review": {"active": {"state": "NONE", "member_count": 0}, "proposed": {"state": "PROPOSED", "member_count": 2, "decision": "REVIEW_REQUIRED"}, "proposals": [{"proposal_id": "fixture-proposal-1", "outcome": "PROPOSED", "reason": "Awaiting review"}]}, "cold_start_requirements": ["fixture evidence review"]},
        "polymarket": _page(markets),
        "candidates": _page(candidates),
        "datasets": _page(datasets),
        "activity": _page(activity_rows),
        "hermes": _page(hermes_jobs),
        "shadow": _page(shadow_jobs),
        "crypto-research": {**_page(crypto_catalog), "universe_version": "fixture-universe-v2", "symbol_count": len(crypto_catalog), "report_count": len(crypto_catalog), "reports": deepcopy(crypto_catalog[:3]), "strategy_reports": deepcopy(crypto_catalog[3:6]), "bootstrap_reports": deepcopy(crypto_catalog[6:9]), "catalogs": deepcopy(crypto_catalog[:9]), "universe_versions": [{"universe_version": "fixture-universe-v2", "status": "CURRENT", "source": "FIXTURE_SOURCE", "updated_at": FIXTURE_TIME}]},
        "binance-canary": {"fixture": True, "fixture_banner": "FIXTURE · Binance is parked", "available": True, "strict_testnet": True, "title": "BINANCE SPOT TESTNET", "environment": "BINANCE_SPOT_TESTNET", "status": "PARKED", "mode": "STRICT_TESTNET", "transport": "DISABLED", "credentials": {"status": "NOT READ"}, "profile": {"environment": "TESTNET", "strict_testnet": True}, "items": []},
    }
    return _retime(payload, stamp)


@dataclass
class FixtureControl:
    scenario: str
    calls: list[dict[str, Any]] = field(default_factory=list)
    action_history: list[dict[str, Any]] = field(default_factory=list)
    _pending: dict[str, dict[str, Any]] = field(default_factory=dict)
    delay_ms: int = 0
    drop_next_response: bool = False
    fail_ancillary: bool = False
    uncertain_next: bool = False
    projection: dict[str, Any] | None = field(default=None, repr=False)
    clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc)

    def _confirm_projection(self) -> None:
        if not isinstance(self.projection, dict):
            return
        canary = self.projection.get("canary")
        if not isinstance(canary, dict):
            return
        confirmed_now = self.clock().astimezone(timezone.utc)
        confirmed_expires = None
        authorization = canary.get("execution_authorization")
        if isinstance(authorization, dict):
            draft = authorization.get("draft")
            active = deepcopy(draft) if isinstance(draft, Mapping) else {}
            duration = int(active.get("duration_seconds") or 0)
            confirmed_expires = (confirmed_now + timedelta(seconds=duration)).isoformat() if duration > 0 else None
            active.update({"status": "ACTIVE", "active": True, "live_execution": True, "paper_only": False, "expires_at": confirmed_expires})
            authorization.update({"status": "ACTIVE", "expires_at": confirmed_expires, "active": active, "live_execution": True, "paper_only": False})
        canary.update({"display_state": "ENABLED", "control_state": "ARMED", "micro_live_canary": "ACTIVE", "live_execution": True, "paper_only": False})
        autonomous = canary.get("autonomous")
        if isinstance(autonomous, dict):
            autonomous.update({"enabled": True, "control_state": "ARMED", "next_decision": "NO_SIGNAL"})
        worker = canary.get("worker")
        if isinstance(worker, dict):
            worker.update({"last_tick_at": confirmed_now.isoformat(), "next_decision": "NO_SIGNAL", "blocker": None})
        controls = canary.get("operator_controls")
        if isinstance(controls, dict):
            controls.update({"armed": True, "armed_state": "ARMED", "live_execution": True, "paper_only": False, "execution_authorization": deepcopy(authorization)})
            review = controls.get("exploratory_live_review")
            if isinstance(review, dict):
                review.update({"status": "CONFIRMED", "proposal_status": "CONFIRMED", "permission": "ACTIVE", "armed": True, "blockers": [], "expires_at": confirmed_expires})
                choices = review.get("authorization_choices")
                if isinstance(choices, dict):
                    choices.update({"approved": True, "status": "APPROVED", "expires_at": confirmed_expires})
                review_choices = review.get("choices")
                if isinstance(review_choices, dict):
                    review_choices.update({"approved": True, "status": "APPROVED", "expires_at": confirmed_expires})
                review_auth = review.get("authorization")
                if isinstance(review_auth, dict):
                    review_auth.update({"status": "ACTIVE", "active": True, "live_execution": True, "paper_only": False, "expires_at": confirmed_expires})
                proposal = review.get("proposal")
                if isinstance(proposal, dict):
                    proposal["status"] = "CONFIRMED"
                for member in review.get("members", []):
                    if isinstance(member, dict):
                        member["allocation_active"] = True
                        member["status"] = "ACTIVE"
        mirror_flags = {
            "production_live_trading": bool(canary.get("production_live_trading", False)),
            "paper_only": bool(canary.get("paper_only", True)),
            "live_execution": bool(canary.get("live_execution", False)),
            "autonomous": deepcopy(canary.get("autonomous")),
        }
        operator = self.projection.get("operator")
        if isinstance(operator, dict):
            operator.update(mirror_flags)
            operator["execution_authorization"] = deepcopy(authorization)
            operator["operator_controls"] = deepcopy(controls)
            if isinstance(canary.get("risk_settings"), dict):
                operator["risk_settings"] = deepcopy(canary["risk_settings"])
        self.projection["execution_authorization"] = deepcopy(authorization)
        self.projection["operator_controls"] = deepcopy(controls)
        self.projection.update(mirror_flags)

    def _now_iso(self) -> str:
        return self.clock().astimezone(timezone.utc).isoformat()

    def _sync_projection(self) -> None:
        projection = self.projection if isinstance(self.projection, dict) else {}
        canary = projection.get("canary") if isinstance(projection.get("canary"), dict) else {}
        controls = canary.get("operator_controls")
        authorization = canary.get("execution_authorization")
        if isinstance(controls, dict):
            controls["execution_authorization"] = deepcopy(authorization)
            if isinstance(canary.get("risk_settings"), dict):
                controls["risk_settings"] = deepcopy(canary["risk_settings"])
        mirror_flags = {
            "production_live_trading": bool(canary.get("production_live_trading", False)),
            "paper_only": bool(canary.get("paper_only", True)),
            "live_execution": bool(canary.get("live_execution", False)),
            "autonomous": deepcopy(canary.get("autonomous")),
        }
        operator = projection.get("operator")
        if isinstance(operator, dict):
            operator.update(mirror_flags)
            operator["execution_authorization"] = deepcopy(authorization)
            operator["operator_controls"] = deepcopy(controls)
            if isinstance(canary.get("risk_settings"), dict):
                operator["risk_settings"] = deepcopy(canary["risk_settings"])
        projection["canary"] = canary
        projection["execution_authorization"] = deepcopy(authorization)
        projection["operator_controls"] = deepcopy(controls)
        projection.update(mirror_flags)
        if isinstance(canary.get("risk_settings"), dict):
            projection["risk-settings"] = deepcopy(canary["risk_settings"])

    def _review_projection(self, values: Mapping[str, Any] | None = None) -> dict[str, Any]:
        projection = self.projection if isinstance(self.projection, dict) else {}
        canary = projection.get("canary") if isinstance(projection.get("canary"), dict) else {}
        authorization = canary.get("execution_authorization")
        if isinstance(authorization, dict):
            authorization.update({"status": "DRAFT", "active": None, "live_execution": False, "paper_only": True})
            draft = authorization.get("draft")
            if isinstance(draft, dict):
                draft.update({"status": "DRAFT", "live_execution": False, "paper_only": True})
            controls = canary.get("operator_controls")
            review = controls.get("exploratory_live_review") if isinstance(controls, dict) else None
            if isinstance(values, Mapping) and "adverse_evidence_ack" in values:
                supplied = values.get("adverse_evidence_ack")
                acknowledged = supplied is True or (isinstance(supplied, Mapping) and supplied.get("acknowledged") is True)
                required = bool(
                    (review.get("adverse_evidence", {}).get("required") if isinstance(review, dict) and isinstance(review.get("adverse_evidence"), Mapping) else False)
                    or (draft.get("adverse_evidence_ack", {}).get("required") if isinstance(draft, dict) and isinstance(draft.get("adverse_evidence_ack"), Mapping) else False)
                )
                consent = {"acknowledged": acknowledged, "required": required}
                authorization["adverse_evidence_ack"] = deepcopy(consent)
                if isinstance(draft, dict):
                    draft["adverse_evidence_ack"] = deepcopy(consent)
                if isinstance(review, dict):
                    adverse = review.get("adverse_evidence")
                    if isinstance(adverse, dict):
                        adverse.update({"acknowledged": acknowledged, "status": "ACKNOWLEDGED" if acknowledged else ("REQUIRED" if required else "NOT_REQUIRED")})
                    review["adverse_evidence_ack"] = deepcopy(consent)
                    if acknowledged:
                        review["blockers"] = [item for item in review.get("blockers", []) if item != "ADVERSE_EVIDENCE_ACK_REQUIRED"]
                        review["adverse_evidence_acknowledged"] = True
        self._sync_projection()
        return deepcopy(authorization) if isinstance(authorization, dict) else {}
    def _prepare_projection(self) -> dict[str, Any]:
        projection = self.projection if isinstance(self.projection, dict) else {}
        canary = projection.get("canary") if isinstance(projection.get("canary"), dict) else {}
        controls = canary.get("operator_controls") if isinstance(canary.get("operator_controls"), dict) else {}
        review = controls.get("exploratory_live_review") if isinstance(controls.get("exploratory_live_review"), dict) else {}
        authorization = canary.get("execution_authorization") if isinstance(canary.get("execution_authorization"), dict) else {}
        draft = authorization.get("draft") if isinstance(authorization.get("draft"), dict) else {}
        proposal = review.get("proposal") if isinstance(review.get("proposal"), dict) else {}
        members = review.get("members") if isinstance(review.get("members"), list) else []
        if not members:
            members = [
                {"allocation": "2.50", "allocation_active": False, "candidate_id": "fixture-candidate-aurora", "proposed_allocation": "2.50", "status": "SELECTED", "strategy_name": "Aurora threshold study", "strategy_version_id": "fixture-strategy-v1"},
                {"allocation": "2.50", "allocation_active": False, "candidate_id": "fixture-candidate-borealis", "proposed_allocation": "2.50", "status": "SELECTED", "strategy_name": "Borealis threshold study", "strategy_version_id": "fixture-strategy-v1"},
            ]
        setup_bindings = review.get("setup_bindings") if isinstance(review.get("setup_bindings"), list) else []
        if not setup_bindings:
            setup_bindings = [{"candidate_id": member["candidate_id"], "setup_id": f"fixture-setup-{index}", "setup_name": member.get("strategy_name", "Fixture setup"), "strategy_version_id": member.get("strategy_version_id", "fixture-strategy-v1")} for index, member in enumerate(members, 1)]
        proposal.update({"status": "REVIEW_REQUIRED", "proposed_allocation_total": "5.00", "proposed_allocation_risk_digest": "sha256:fixture-risk-digest", "members": deepcopy(members), "selected_setups": deepcopy(setup_bindings), "setup_bindings": deepcopy(setup_bindings)})
        review.update({"status": "REVIEW_REQUIRED", "proposal_status": "REVIEW_REQUIRED", "proposal": proposal, "members": deepcopy(members), "selected_setups": deepcopy(setup_bindings), "setup_bindings": deepcopy(setup_bindings), "shared_allocation": "5.00", "blockers": []})
        choices = review.get("choices")
        if isinstance(choices, dict):
            choices["shared_allocation"] = "5.00"
        authorization_choices = review.get("authorization_choices")
        if isinstance(authorization_choices, dict):
            authorization_choices["shared_allocation"] = "5.00"
        draft["shared_allocation"] = "5.00"
        draft.setdefault("lifetime_budget", {"max_notional_usd": "5.00"})
        authorization["draft"] = draft
        authorization["status"] = "REVOKED"
        authorization["active"] = None
        setup = review.get("session_setup")
        if isinstance(setup, dict):
            setup["required"] = False
        self._sync_projection()
        return {"proposal": deepcopy(proposal), "members": deepcopy(members), "proposal_only": True, "authority_changed": False}

    def _refresh_connectivity(self) -> dict[str, Any]:
        projection = self.projection if isinstance(self.projection, dict) else {}
        canary = projection.get("canary") if isinstance(projection.get("canary"), dict) else {}
        stamp = self._now_iso()
        connectivity = {
            "status": "READY",
            "ready": True,
            "checked_at": stamp,
            "failure_codes": [],
            "source": "FIXTURE_READ_ONLY",
            "live_execution": False,
        }
        canary["connectivity"] = connectivity
        readiness = canary.get("readiness")
        if isinstance(readiness, dict):
            readiness.update({"status": "CURRENT", "checked_at": stamp})
        controls = canary.get("operator_controls")
        if isinstance(controls, dict):
            controls["connectivity"] = deepcopy(connectivity)
        operator = projection.get("operator")
        if isinstance(operator, dict):
            operator["connectivity"] = deepcopy(connectivity)
        self._sync_projection()
        return {"connectivity": deepcopy(connectivity), "readiness": deepcopy(readiness)}

    def _disarm_projection(self) -> dict[str, Any]:
        projection = self.projection if isinstance(self.projection, dict) else {}
        canary = projection.get("canary") if isinstance(projection.get("canary"), dict) else {}
        canary.update({"control_state": "DISARMED", "display_state": "DISABLED", "micro_live_canary": "DISARMED", "live_execution": False, "paper_only": True})
        autonomous = canary.get("autonomous")
        if isinstance(autonomous, dict):
            autonomous.update({"enabled": False, "control_state": "DISARMED", "next_decision": "STOPPED"})
        worker = canary.get("worker")
        if isinstance(worker, dict):
            worker.update({"next_decision": "STOPPED", "blocker": "DISARMED_BY_OPERATOR"})
        canary["decision"] = {"status": "STOPPED", "reason_code": "DISARMED_BY_OPERATOR"}
        execution = canary.get("execution")
        if isinstance(execution, dict):
            execution["last_request_status"] = "STOPPED"
        controls = canary.get("operator_controls")
        if isinstance(controls, dict):
            controls.update({"armed": False, "armed_state": "DISARMED", "live_execution": False, "paper_only": True})
        self._sync_projection()
        return {"armed": False, "armed_state": "DISARMED", "orders_closed": False, "orders_cancelled": False}
    def _revoke_projection(self, payload: Mapping[str, Any]) -> tuple[bool, str, dict[str, Any]]:
        projection = self.projection if isinstance(self.projection, dict) else {}
        canary = projection.get("canary") if isinstance(projection.get("canary"), dict) else {}
        authorization = canary.get("execution_authorization")
        if not isinstance(authorization, dict):
            return False, "EXECUTION_AUTHORIZATION_ACTIVE_REQUIRED", {}
        active = authorization.get("active")
        if not isinstance(active, Mapping):
            return False, "EXECUTION_AUTHORIZATION_ACTIVE_REQUIRED", {}
        active_id = str(active.get("authorization_id") or active.get("id") or "").strip()
        supplied_id = str(payload.get("authorization_id") or "").strip()
        if supplied_id and supplied_id != active_id:
            return False, "EXECUTION_AUTHORIZATION_BINDING_STALE", {}
        expected_generation = payload.get("expected_generation")
        if expected_generation not in (None, ""):
            try:
                if int(expected_generation) != int(active.get("generation") or 0):
                    return False, "EXECUTION_AUTHORIZATION_GENERATION_CHANGED", {}
            except (TypeError, ValueError, OverflowError):
                return False, "EXECUTION_AUTHORIZATION_GENERATION_REQUIRED", {}
        stamp = self._now_iso()
        authorization.update({"status": "REVOKED", "active": None, "live_execution": False, "paper_only": True, "revoked_at": stamp})
        draft = authorization.get("draft")
        if isinstance(draft, dict):
            draft.update({"status": "REVOKED", "live_execution": False, "paper_only": True, "revoked_at": stamp})
        controls = canary.get("operator_controls")
        if isinstance(controls, dict):
            controls["execution_authorization"] = deepcopy(authorization)
            review = controls.get("exploratory_live_review")
            if isinstance(review, dict):
                review.update({"status": "BLOCKED", "proposal_status": "REVOKED", "blockers": ["AUTHORIZATION_REVOKED"]})
                review_auth = review.get("authorization")
                if isinstance(review_auth, dict):
                    review_auth.update({"status": "REVOKED", "active": None, "live_execution": False, "paper_only": True, "revoked_at": stamp})
        self._sync_projection()
        return True, "FIXTURE_AUTHORIZATION_REVOKED", {"execution_authorization": deepcopy(authorization)}

    def _activate_risk_draft(self, payload: Mapping[str, Any]) -> tuple[bool, str, dict[str, Any]]:
        projection = self.projection if isinstance(self.projection, dict) else {}
        canary = projection.get("canary") if isinstance(projection.get("canary"), dict) else {}
        risk = canary.get("risk_settings") if isinstance(canary.get("risk_settings"), dict) else {}
        active = risk.get("active") if isinstance(risk.get("active"), dict) else {}
        draft = risk.get("draft") if isinstance(risk.get("draft"), dict) else None
        config_id = str(payload.get("config_id") or "")
        expected = payload.get("expected_generation")
        try:
            expected_generation = int(expected)
        except (TypeError, ValueError):
            expected_generation = -1
        if draft is None:
            return False, "RISK_SETTINGS_DRAFT_REQUIRED", {}
        if config_id != str(draft.get("config_id") or ""):
            return False, "RISK_SETTINGS_CONFIG_CONFLICT", {"expected_config_id": draft.get("config_id")}
        if expected_generation != int(active.get("generation") or 0):
            return False, "RISK_SETTINGS_GENERATION_CONFLICT", {"expected_generation": active.get("generation")}
        values = deepcopy(draft.get("values") if isinstance(draft.get("values"), dict) else {})
        activated = {
            "config_hash": draft.get("config_hash"),
            "config_id": draft.get("config_id"),
            "generation": draft.get("generation"),
            "settings": deepcopy(values),
            "values": deepcopy(values),
            "state": "ACTIVE",
            "status": "ACTIVE",
            "activated_at": self._now_iso(),
        }
        risk.update({
            "active": activated,
            "draft": None,
            "config_hash": activated["config_hash"],
            "config_id": activated["config_id"],
            "generation": activated["generation"],
            "effective_limits": deepcopy(values),
            "status": "CURRENT",
        })
        canary["risk_settings"] = risk
        self._sync_projection()
        return True, "FIXTURE_RISK_DRAFT_ACTIVATED", {"active": deepcopy(activated), "draft": None}

    def resolve_uncertain(self, *, outcome: str = "") -> dict[str, Any]:
        record = next((row for row in reversed(self.action_history) if row.get("status") in {"RUNNING", "UNCERTAIN"}), None)
        if record is None:
            return {"resolved": False, "status": "NOT_FOUND", "reason": "NO_UNCERTAIN_ACTION"}
        success = str(outcome or "").strip().upper() in {"COMPLETE", "SUCCESS", "SUCCEEDED", "RESOLVED"}
        result: dict[str, Any] = {"resolved": success, "action_id": record.get("action_id")}
        if success:
            name = str(record.get("action") or "")
            payload = record.get("payload") if isinstance(record.get("payload"), Mapping) else {}
            if name == "exploratory.live.prepare":
                if str(record.get("confirm") or "").strip() != "PREPARE EXPLORATORY SESSION" or payload:
                    success = False
                    result["reason"] = "FIXTURE_PREPARE_ACTION_INVALID"
                else:
                    result["result"] = self._prepare_projection()
            elif name == "execution_authorization.review":
                projection = self.projection if isinstance(self.projection, dict) else {}
                canary = projection.get("canary") if isinstance(projection.get("canary"), dict) else {}
                controls = canary.get("operator_controls")
                review = controls.get("exploratory_live_review") if isinstance(controls, Mapping) else None
                if isinstance(review, dict):
                    review.update({"status": "REVIEWED_ONLY", "reviewed_at": self._now_iso()})
                reviewed = self._review_projection(payload.get("values") if isinstance(payload.get("values"), Mapping) else None)
                result["result"] = {"permission": "REVIEWED_ONLY", "armed": False, "backend_invocation": False, "execution_authorization": reviewed}
            elif name in _FINAL_REVIEW_ACTIONS:
                if str(record.get("confirm") or "").strip() != "CONFIRM EXPLORATORY LIVE" or payload:
                    success = False
                    result["reason"] = "FIXTURE_FINAL_ACTION_INVALID"
                else:
                    self._confirm_projection()
                    result["result"] = {"armed": True, "permission": "ACTIVE", "backend_invocation": False}
            elif name == "canary.connectivity_check":
                result["result"] = self._refresh_connectivity()
            elif name in {"risk.settings.save_draft", "canary.settings.save_draft"}:
                try:
                    draft = self._save_risk_draft(payload.get("values") if isinstance(payload.get("values"), Mapping) else {})
                except CanarySettingsValidationError as exc:
                    success = False
                    result["reason"] = type(exc).__name__.upper()
                else:
                    result["result"] = {"draft": draft, "active_unchanged": True}
            elif name in {"risk.settings.activate_draft", "canary.settings.activate_draft"}:
                ok, reason, activation = self._activate_risk_draft(payload)
                if not ok:
                    success = False
                    result["reason"] = reason
                else:
                    result["result"] = activation
            elif name == "canary.disarm":
                result["result"] = self._disarm_projection()
            else:
                success = False
                result["reason"] = "FIXTURE_ACTION_UNSUPPORTED"
        if success:
            record.update({"ok": True, "status": "COMPLETE", "reason": "FIXTURE_UNCERTAIN_RESOLVED", "result": result.get("result", {})})
        else:
            record.update({"ok": False, "status": "FAILED", "reason": result.get("reason", "FIXTURE_UNCERTAIN_FAILED"), "result": {"resolved": False, "recorded_only": True}})
        record["completed_at"] = self._now_iso()
        result.update({"status": record["status"], "reason": record["reason"]})
        return result

    def _save_risk_draft(self, values: Mapping[str, Any]) -> dict[str, Any]:
        projection = self.projection if isinstance(self.projection, dict) else {}
        canary = projection.get("canary") if isinstance(projection.get("canary"), dict) else {}
        risk = canary.get("risk_settings") if isinstance(canary.get("risk_settings"), dict) else {}
        active = risk.get("active") if isinstance(risk.get("active"), dict) else {}
        active_values = active.get("values") if isinstance(active.get("values"), dict) else {}
        validator = object.__new__(CanarySettingsService)
        draft_values = validator._normalize(values, base=active_values)
        diff = {
            key: {"active": active_values.get(key), "draft": draft_values.get(key)}
            for key in draft_values
            if active_values.get(key) != draft_values.get(key)
        }
        draft = {
            "config_id": "fixture-risk-draft",
            "config_hash": "sha256:fixture-risk-draft",
            "generation": int(risk.get("generation") or active.get("generation") or 0) + 1,
            "state": "DRAFT",
            "status": "REVIEW_REQUIRED",
            "values": draft_values,
            "diff": diff,
            "review_required": True,
            "source": "FIXTURE_CONTROL",
            "created_at": self._now_iso(),
        }
        risk["draft"] = draft
        canary["risk_settings"] = risk
        self._sync_projection()
        return draft
    def execute(self, action: str, target: str = "", *, confirm: str = "", payload: Mapping[str, Any] | None = None) -> dict[str, Any]:
        name = str(action or "").strip()
        request = dict(payload) if isinstance(payload, Mapping) else {}
        action_id = str(request.get("action_id") or f"fixture-action-{len(self.calls) + 1}")
        if action_id in self._pending:
            return {"ok": False, "fixture": True, "action_id": action_id, "status": "PENDING", "reason": "DUPLICATE_PENDING"}
        prior = next((row for row in self.action_history if row.get("action_id") == action_id), None)
        if prior is not None:
            return {"ok": bool(prior.get("ok")), "fixture": True, "action_id": action_id, "status": prior.get("status"), "reason": prior.get("reason"), "result": deepcopy(prior.get("result") or {}), "history": list(self.action_history)}
        record = {"action_id": action_id, "action": name, "target": str(target or ""), "confirm": str(confirm or ""), "payload": deepcopy(request), "count": len(self.calls) + 1, "status": "PENDING", "started_at": self._now_iso()}
        self.calls.append(record)
        self._pending[action_id] = record
        try:
            if self.delay_ms > 0:
                import time
                time.sleep(self.delay_ms / 1000)
            uncertain = self.uncertain_next or self.scenario in {"network_failure", "unknown_order", "poll"}
            self.uncertain_next = False
            if uncertain:
                result = {"ok": False, "fixture": True, "action_id": action_id, "status": "RUNNING", "action_status": "RUNNING", "reason": "OUTCOME_UNCERTAIN", "result": {"resolution_required": True}}
            elif name in {"risk.settings.save_draft", "canary.settings.save_draft"}:
                values = request.get("values")
                if not isinstance(values, Mapping):
                    result = {"ok": False, "fixture": True, "action_id": action_id, "status": "FAILED", "reason": "RISK_SETTINGS_VALUES_REQUIRED", "result": {}}
                else:
                    try:
                        draft = self._save_risk_draft(values)
                    except CanarySettingsValidationError as exc:
                        reason = type(exc).__name__.upper()
                        result = {"ok": False, "fixture": True, "action_id": action_id, "status": "FAILED", "reason": reason, "result": {"ok": False, "reason": reason}}
                    else:
                        result = {"ok": True, "fixture": True, "action_id": action_id, "status": "COMPLETE", "reason": "FIXTURE_RISK_DRAFT_SAVED", "result": {"draft": draft, "active_unchanged": True}}
            elif name in {"risk.settings.activate_draft", "canary.settings.activate_draft"}:
                ok, reason, activation = self._activate_risk_draft(request)
                result = {"ok": ok, "fixture": True, "action_id": action_id, "status": "COMPLETE" if ok else "FAILED", "reason": reason, "result": activation}
            elif name == "exploratory.live.prepare":
                if str(confirm or "").strip() != "PREPARE EXPLORATORY SESSION" or request:
                    result = {"ok": False, "fixture": True, "action_id": action_id, "status": "FAILED", "reason": "FIXTURE_PREPARE_ACTION_INVALID", "result": {}}
                else:
                    prepared = self._prepare_projection()
                    result = {"ok": True, "fixture": True, "action_id": action_id, "status": "COMPLETE", "reason": "FIXTURE_EXPLORATORY_SESSION_PREPARED", "result": prepared}
            elif name == "execution_authorization.review":
                values = request.get("values")
                if not isinstance(values, Mapping):
                    result = {"ok": False, "fixture": True, "action_id": action_id, "status": "FAILED", "reason": "EXECUTION_AUTHORIZATION_VALUES_REQUIRED", "result": {}}
                else:
                    projection = self.projection if isinstance(self.projection, dict) else {}
                    canary = projection.get("canary") if isinstance(projection.get("canary"), dict) else {}
                    controls = canary.get("operator_controls")
                    review = controls.get("exploratory_live_review") if isinstance(controls, dict) else None
                    if isinstance(review, dict):
                        review.update({"status": "REVIEWED_ONLY", "reviewed_at": self._now_iso()})
                    reviewed = self._review_projection(values)
                    result = {"ok": True, "fixture": True, "action_id": action_id, "status": "COMPLETE", "reason": "FIXTURE_AUTHORIZATION_REVIEW_RECORDED", "result": {"permission": "REVIEWED_ONLY", "armed": False, "backend_invocation": False, "execution_authorization": reviewed}}
            elif name == "execution_authorization.activate":
                authorization = (self.projection or {}).get("canary", {}).get("execution_authorization", {})
                expected_id = str(authorization.get("authorization_id") or "") if isinstance(authorization, Mapping) else ""
                expected_generation = int(authorization.get("generation") or 0) if isinstance(authorization, Mapping) else 0
                try:
                    requested_generation = int(request.get("expected_generation"))
                except (TypeError, ValueError):
                    requested_generation = -1
                if str(request.get("authorization_id") or "") != expected_id or requested_generation != expected_generation:
                    result = {"ok": False, "fixture": True, "action_id": action_id, "status": "FAILED", "reason": "EXECUTION_AUTHORIZATION_GENERATION_CONFLICT", "result": {}}
                else:
                    self._confirm_projection()
                    result = {"ok": True, "fixture": True, "action_id": action_id, "status": "COMPLETE", "reason": "FIXTURE_AUTHORIZATION_ACTIVATED", "result": {"armed": True, "permission": "ACTIVE", "backend_invocation": False}}
            elif name == "execution_authorization.revoke":
                ok, reason, revoked = self._revoke_projection(request)
                result = {"ok": ok, "fixture": True, "action_id": action_id, "status": "COMPLETE" if ok else "FAILED", "reason": reason, "result": revoked}
            elif name == "canary.disarm":
                result = {"ok": True, "fixture": True, "action_id": action_id, "status": "COMPLETE", "reason": "FIXTURE_CANARY_DISARMED", "result": {"canary": self._disarm_projection()}}
            elif name in _FINAL_REVIEW_ACTIONS:
                if str(confirm or "").strip() != "CONFIRM EXPLORATORY LIVE" or request:
                    result = {"ok": False, "fixture": True, "action_id": action_id, "status": "FAILED", "reason": "FIXTURE_FINAL_ACTION_INVALID", "result": {}}
                else:
                    self._confirm_projection()
                    result = {"ok": True, "fixture": True, "action_id": action_id, "status": "CONFIRMED", "action_status": "COMPLETE", "reason": "FIXTURE_REVIEW_RECORDED", "result": {"armed": True, "permission": "ACTIVE", "backend_invocation": False}}
            elif name.startswith("fixture."):
                result = {"ok": True, "fixture": True, "action_id": action_id, "status": "RECORDED_ONLY", "reason": "FIXTURE_ACTION_RECORDED_ONLY", "result": {"recorded_only": True, "supported": False}}
            else:
                result = {"ok": False, "fixture": True, "action_id": action_id, "status": "UNSUPPORTED", "reason": "FIXTURE_ACTION_UNSUPPORTED", "result": {"recorded_only": True, "supported": False}}
            record.update({key: value for key, value in result.items() if key in {"ok", "status", "reason", "result", "action_status"}})
            if result.get("action_status"):
                record["status"] = result["action_status"]
            if record["status"] not in {"RUNNING", "PENDING"}:
                record["completed_at"] = self._now_iso()
            self.action_history.append(dict(record))
            if self.drop_next_response:
                self.drop_next_response = False
                raise ConnectionError("fixture response intentionally dropped")
            return {**result, "history": list(self.action_history)}
        finally:
            self._pending.pop(action_id, None)

    def status(self) -> dict[str, Any]:
        latest = self.action_history[-1] if self.action_history else {}
        projection = self.projection if isinstance(self.projection, dict) else {}
        canary = projection.get("canary") if isinstance(projection.get("canary"), dict) else {}
        authorization = canary.get("execution_authorization")
        controls = canary.get("operator_controls")
        review = controls.get("exploratory_live_review") if isinstance(controls, dict) else {}
        auth_status = str(authorization.get("status") or "UNKNOWN") if isinstance(authorization, Mapping) else "UNKNOWN"
        armed = bool(controls.get("armed")) if isinstance(controls, Mapping) else False
        review_status = str(review.get("status") or "READY") if isinstance(review, Mapping) else "READY"
        return {
            "fixture": True,
            "calls": len(self.calls),
            "action_stats": {"total": len(self.calls), "completed": len([row for row in self.action_history if row.get("status") not in {"RUNNING", "PENDING"}]), "pending": len(self._pending), "latest": latest},
            "state": "ARMED" if armed else "DISARMED",
            "armed": armed,
            "armed_state": "ARMED" if armed else "DISARMED",
            "execution_authorization": {"status": auth_status, "mode": "EXPLORATORY_MICRO_CANARY"},
            "exploratory_live_review": {"status": review_status, "permission": "ACTIVE" if auth_status == "ACTIVE" else "REVIEWED_ONLY", "armed": armed},
        }

class _LegacyHandler(BaseHTTPRequestHandler):
    server: "LegacyHtmlServer"


    def _send(self, status: int, payload: Any, content_type: str = "application/json") -> None:
        if content_type.startswith("application/json"):
            body = json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
        elif isinstance(payload, bytes):
            body = payload
        else:
            body = str(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", f"{content_type}; charset=utf-8" if "charset=" not in content_type else content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _read_request_allowed(self) -> bool:
        host = self.headers.get("Host", "").split(":", 1)[0].strip("[]").lower()
        return host in {"127.0.0.1", "localhost", "::1"}

    def do_GET(self) -> None:  # noqa: N802 - stdlib handler API
        if not self._read_request_allowed():
            self._send(403, {"error": "localhost fixture host required"})
            return
        parsed = urlparse(self.path)
        path = parsed.path.strip("/")
        if path in {"", "index.html"}:
            self._send(200, self.server.html_bytes, "text/html; charset=utf-8")
            return
        if path in {"api/control", "api/binance/control"}:
            self._send(405, {"error": "POST required"})
            return
        query = parse_qs(parsed.query, keep_blank_values=True)
        if path.startswith("api/v2/"):
            endpoint = path[len("api/v2/") :]
            allowed = endpoint.lower() in _V2_ENDPOINTS or endpoint.lower().startswith(("datasets/", "candidates/", "hermes/", "crypto-research/", "shadow/"))
            if not allowed:
                self._send(404, {"error": "not found"})
                return
            validation_error = _pagination_error(query)
            if validation_error:
                self._send(400, {"error": "invalid pagination", "detail": validation_error})
                return
            try:
                self._send(200, self.server.dashboard_data.v2_snapshot(endpoint, query))
            except ValueError as exc:
                self._send(400, {"error": "invalid request", "detail": str(exc)})
            except Exception as exc:
                self._send(503, {"error": "data unavailable", "detail": type(exc).__name__})
            return
        endpoint = path[4:] if path.startswith("api/") else path
        if endpoint.lower() == "ui-record":
            kind = _query_value(query, "kind").strip().lower()
            identifier = _query_value(query, "id").strip()
            if kind not in {"market", "order", "submission", "reservation", "fill", "risk-fill", "position", "mark", "cashflow"} or not identifier:
                self._send(400, {"error": "invalid ui record", "kind": kind, "id": identifier})
                return
            record = self.server.dashboard_data.ui_record_data(kind, identifier)
            if record is None:
                self._send(404, {"error": "record unavailable", "kind": kind, "id": identifier})
                return
            self._send(200, record)
            return
        dynamic_strategy = endpoint.lower().startswith("strategy/") and len(endpoint.split("/", 1)[1]) > 0
        if endpoint.lower() not in {"ui-state", "ui-status"} and endpoint not in _ENDPOINTS and not dynamic_strategy:
            self._send(404, {"error": "not found"})
            return
        try:
            if dynamic_strategy:
                endpoint = "strategy/" + unquote(endpoint.split("/", 1)[1])
            self._send(200, self.server.dashboard_data.snapshot(endpoint, query))
        except Exception as exc:
            self._send(503, {"error": "data unavailable", "detail": type(exc).__name__})

    def do_POST(self) -> None:  # noqa: N802 - fixture is intentionally read-only
        if not self._read_request_allowed():
            self._send(403, {"error": "localhost fixture host required"})
            return
        self._send(405, {"error": "fixture is read-only"})


class LegacyHtmlServer:
    """Serve the token-free BEFORE root with the same read-only fixture APIs."""

    def __init__(self, path: str | Path, *, data: DashboardData, host: str = "127.0.0.1", port: int = 0) -> None:
        if str(host).strip().lower() not in {"127.0.0.1", "localhost", "::1"}:
            raise ValueError("legacy fixture server must bind to a loopback address")
        self.path = Path(path)
        self.host = host
        self.port = port
        self.dashboard_data = data
        self._server: ThreadingHTTPServer | None = None
        self._thread: Thread | None = None
        self.html_bytes = b""

    @property
    def url(self) -> str | None:
        if self._server is None:
            return None
        address = self._server.server_address
        return f"http://{address[0]}:{address[1]}"

    def start(self) -> "LegacyHtmlServer":
        if self._server is not None:
            return self
        self.html_bytes = self.path.read_bytes()
        marker = b'<div class="fixture-banner" data-fixture-banner="true" role="status" aria-label="Fixture only" style="position:fixed;right:12px;bottom:12px;z-index:2147483647;pointer-events:none;padding:8px 10px;border:1px solid #d97706;border-radius:6px;background:#1f1305;color:#fed7aa;font:600 12px/1.3 system-ui,sans-serif;box-shadow:0 4px 14px rgba(0,0,0,.35)">FIXTURE ONLY - sanitized read-only fixture; no production authority</div>'
        if b"data-fixture-banner" not in self.html_bytes.lower():
            lowered = self.html_bytes.lower()
            if b"</body>" in lowered:
                index = lowered.rfind(b"</body>")
                self.html_bytes = self.html_bytes[:index] + marker + self.html_bytes[index:]
            else:
                self.html_bytes += marker
        self._server = ThreadingHTTPServer((self.host, self.port), _LegacyHandler)
        self._server.dashboard_data = self.dashboard_data
        self._server.html_bytes = self.html_bytes
        self._thread = Thread(target=self._server.serve_forever, name="axiom-fixture-legacy", daemon=True)
        self._thread.start()
        return self

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

    def __enter__(self) -> "LegacyHtmlServer":
        return self.start()

    def __exit__(self, *_args: Any) -> None:
        self.stop()

def _decorate_fixture_html(body: bytes) -> bytes:
    marker = b'<div class="fixture-banner" data-fixture-banner="true" role="status" aria-label="Fixture only" style="position:fixed;right:12px;bottom:12px;z-index:2147483647;pointer-events:none;padding:8px 10px;border:1px solid #d97706;border-radius:6px;background:#1f1305;color:#fed7aa;font:600 12px/1.3 system-ui,sans-serif;box-shadow:0 4px 14px rgba(0,0,0,.35)">FIXTURE ONLY - sanitized read-only fixture; no production authority</div>'
    lowered = body.lower()
    if b"data-fixture-banner" in lowered and b"position:fixed" in lowered:
        return body
    if b"</body>" in lowered:
        index = lowered.rfind(b"</body>")
        return body[:index] + marker + body[index:]
    return body + marker


class _FixtureRootHandler(BaseHTTPRequestHandler):
    server: "FixtureRootServer"

    def log_message(self, *_args: Any) -> None:
        return

    def _allowed(self) -> bool:
        host = self.headers.get("Host", "").split(":", 1)[0].strip("[]").lower()
        return host in {"127.0.0.1", "localhost", "::1"}

    def _origin_allowed(self) -> bool:
        origin = self.headers.get("Origin")
        return origin in (None, self.server.public_url)

    def _send(self, status: int, body: bytes, content_type: str = "application/json") -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _forward(self, method: str, body: bytes = b"") -> None:
        target = f"{self.server.upstream_url}{self.path}"
        headers = {}
        for key in ("Content-Type", "Accept", "X-Axiom-Control-Token"):
            value = self.headers.get(key)
            if value:
                headers[key] = value
        if self.headers.get("Origin") is not None:
            headers["Origin"] = self.server.upstream_url
        request = UrllibRequest(target, data=body if method == "POST" else None, headers=headers, method=method)
        try:
            with urllib_urlopen(request, timeout=5) as response:
                self._send(response.status, response.read(), response.headers.get("Content-Type", "application/json"))
        except UrllibHTTPError as error:
            self._send(error.code, error.read(), error.headers.get("Content-Type", "application/json"))
        except URLError:
            self._send(503, b'{"error":"fixture upstream unavailable"}')

    def do_GET(self) -> None:  # noqa: N802
        if not self._allowed():
            self._send(403, b'{"error":"localhost fixture host required"}')
            return
        if not self._origin_allowed():
            self._send(403, b'{"error":"fixture origin required"}')
            return
        path = urlparse(self.path).path.strip("/")
        if path == "api/control-token":
            token = str(getattr(self.server, "control_token", "") or "")
            self._send(200, json.dumps({"token": token}, sort_keys=True).encode("utf-8"))
            return
        if path in {"", "index.html"}:
            target = f"{self.server.upstream_url}{self.path}"
            try:
                with urllib_urlopen(target, timeout=5) as response:
                    self._send(response.status, _decorate_fixture_html(response.read()), response.headers.get("Content-Type", "text/html; charset=utf-8"))
            except (UrllibHTTPError, URLError) as error:
                body = error.read() if isinstance(error, UrllibHTTPError) else b'{"error":"fixture upstream unavailable"}'
                self._send(error.code if isinstance(error, UrllibHTTPError) else 503, body, "text/html; charset=utf-8")
            return
        self._forward("GET")

    def do_POST(self) -> None:  # noqa: N802
        if not self._allowed():
            self._send(403, b'{"error":"localhost fixture host required"}')
            return
        if not self._origin_allowed():
            self._send(403, b'{"error":"fixture origin required"}')
            return
        length = min(max(int(self.headers.get("Content-Length", "0") or 0), 0), 16_384)
        body = self.rfile.read(length)
        if urlparse(self.path).path.rstrip("/") == "/api/binance/control":
            expected_token = str(getattr(self.server, "control_token", "") or "")
            if not expected_token or self.headers.get("X-Axiom-Control-Token", "") != expected_token:
                self._send(403, b'{"error":"control token required"}')
                return
            control = getattr(getattr(self.server, "upstream_dashboard", None), "binance_canary", None)
            if control is None:
                self._send(503, b'{"error":"fixture Binance control unavailable"}')
                return
            try:
                request = json.loads(body.decode("utf-8") or "{}")
            except (UnicodeDecodeError, json.JSONDecodeError):
                self._send(400, b'{"error":"invalid JSON"}')
                return
            payload = request.get("payload") if isinstance(request, Mapping) else {}
            action = request.get("action") if isinstance(request, Mapping) else ""
            result = control.action(action, payload)
            self._send(200 if result.get("ok") else 400, json.dumps(result, sort_keys=True, default=str).encode("utf-8"))
            return
        self._forward("POST", body)


class FixtureRootServer:
    """Decorates only the fixture's current DashboardServer root."""

    def __init__(self, upstream: DashboardServer, *, port: int = 0) -> None:
        self.upstream = upstream
        self.port = port
        self._server: ThreadingHTTPServer | None = None
        self._thread: Thread | None = None

    @property
    def upstream_url(self) -> str:
        return str(self.upstream.url or "")

    @property
    def url(self) -> str | None:
        if self._server is None:
            return None
        host, port = self._server.server_address
        return f"http://{host}:{port}"

    @property
    def public_url(self) -> str:
        return str(self.url or "")

    def start(self) -> "FixtureRootServer":
        if self._server is None:
            self._server = ThreadingHTTPServer(("127.0.0.1", self.port), _FixtureRootHandler)
            self._server.upstream_url = self.upstream_url
            self._server.upstream_dashboard = self.upstream.data
            upstream_bound = getattr(self.upstream, "_server", None)
            self._server.control_token = str(getattr(upstream_bound, "control_token", "") or "")
            self._server.public_url = self.public_url
            self._thread = Thread(target=self._server.serve_forever, name="axiom-fixture-root", daemon=True)
            self._thread.start()
        return self

    def stop(self) -> None:
        if self._server is None:
            return
        self._server.shutdown()
        self._server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=2)
        self._server = None
        self._thread = None
def _query_value(params: Mapping[str, Any] | None, key: str, default: str = "") -> str:
    if not isinstance(params, Mapping):
        return default
    value = params.get(key, default)
    if isinstance(value, (list, tuple)):
        value = value[0] if value else default
    return str(value or default)
def _fixture_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, Mapping):
        rows = payload.get("items", [])
    else:
        rows = payload
    return [deepcopy(row) for row in rows] if isinstance(rows, list) else []

def _fixture_page(payload: Any, params: Mapping[str, Any] | None = None) -> dict[str, Any]:
    normalize = lambda value: " ".join(str(value or "").strip().lower().replace("_", " ").replace("-", " ").split())
    page = max(1, int(_query_value(params, "page", "1") or 1))
    page_size = max(1, int(_query_value(params, "page_size", "25") or 25))
    rows = _fixture_rows(payload)
    identifier_queries = {"market_id": "market_id", "candidate_id": "candidate_id", "dataset_id": "dataset_id", "symbol": "symbol", "job_id": "job_id", "item_id": "job_id"}
    for query_key, field in identifier_queries.items():
        expected = _query_value(params, query_key).strip()
        if expected:
            rows = [row for row in rows if str(row.get(field, "")) == expected]
    needle = _query_value(params, "filter").strip().lower()
    if needle:
        rows = [row for row in rows if needle in json.dumps(row, sort_keys=True, default=str).lower()]
    aliases = {"kind": "kind", "item_id": "job_id", "item_type": "status", "market": "market_type", "market_type": "market_type"}
    for query_key in ("category", "market", "market_type", "settlement", "quality", "source", "source_type", "timeframe", "stage", "status", "kind", "severity", "instrument"):
        expected = normalize(_query_value(params, query_key))
        if not expected:
            continue
        field = aliases.get(query_key, query_key)
        rows = [row for row in rows if normalize(row.get(field, "")) == expected]
    sort_key = _query_value(params, "sort").strip()
    if sort_key:
        field = aliases.get(sort_key, sort_key)
        rows.sort(key=lambda row: (normalize(row.get(field, "")), str(row.get("id", ""))), reverse=_query_value(params, "direction", "desc").lower() == "desc")
    total = len(rows)
    start = (page - 1) * page_size
    return _page(rows[start : start + page_size], page_size=page_size, total=total) | {"page": page}


def _fixture_detail(rows: list[dict[str, Any]], identifier: str) -> dict[str, Any]:
    for row in rows:
        if identifier in {str(row.get("id", "")), str(row.get("market_id", "")), str(row.get("candidate_id", "")), str(row.get("dataset_id", "")), str(row.get("job_id", "")), str(row.get("symbol", ""))}:
            return deepcopy(row)
    return {}


@dataclass
class FixtureBinanceControl:
    """Synthetic strict-Testnet control transport for the fixture only."""

    scenario: str = "prepared"
    calls: list[dict[str, Any]] = field(default_factory=list)
    strict_testnet: bool = True

    def action(self, action: str, payload: Mapping[str, Any] | None = None) -> dict[str, Any]:
        name = str(action or "").strip().upper()
        values = dict(payload) if isinstance(payload, Mapping) else {}
        self.calls.append({"action": name, "payload": deepcopy(values), "fixture": True})
        allowed = {"CONNECTIVITY_CHECK", "ORDER_VALIDATION_TEST", "PAUSE", "DISARM", "KILL"}
        if name not in allowed:
            return {"ok": False, "fixture": True, "action": name, "status": "REJECTED", "reason": "STRICT_TESTNET_BROWSER_ACTION_FORBIDDEN"}
        result = {
            "CONNECTIVITY_CHECK": {"status": "READY", "transport": "TESTNET_ONLY"},
            "ORDER_VALIDATION_TEST": {"status": "VALIDATED", "submission": "NOT_SUBMITTED", "payload_received": sorted(values)},
            "PAUSE": {"state": "PAUSED"},
            "DISARM": {"state": "DISARMED"},
            "KILL": {"state": "KILLED"},
        }[name]
        return {"ok": True, "fixture": True, "action": name, "status": "COMPLETE", "result": result}


class FixtureDashboardData(DashboardData):
    """Dashboard facade that exposes fixture DTOs through production routes."""

    def _fixture_delay(self) -> None:
        delay = getattr(self.control, "delay_ms", 0)
        if delay:
            import time
            time.sleep(max(0, min(int(delay), 10_000)) / 1000)

    def canary_data(
        self,
        *,
        risk_snapshot: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        del risk_snapshot
        self._fixture_delay()
        return deepcopy(self._data.get("canary") or {})
    def risk_settings_data(self) -> dict[str, Any]:
        self._fixture_delay()
        canary = self._data.get("canary")
        risk = canary.get("risk_settings") if isinstance(canary, Mapping) else None
        return deepcopy(risk) if isinstance(risk, Mapping) else {"status": "UNKNOWN", "live_execution": False}

    def v2_snapshot(self, endpoint: str, params: Mapping[str, Any] | None = None) -> dict[str, Any]:
        if getattr(self.control, "fail_ancillary", False) and any(token in endpoint.lower() for token in ("/events", "/missing-ranges", "shadow/")):
            raise RuntimeError("FIXTURE_ANCILLARY_FAILURE")
        self._fixture_delay()
        name = endpoint.strip("/")
        collections = {
            "polymarket": "polymarket",
            "candidates": "candidates",
            "datasets": "datasets",
            "hermes": "hermes",
            "shadow": "shadow",
            "crypto-research": "crypto-research",
            "crypto": "crypto-research",
            "activity": "activity",
            "paper": "paper",
        }
        if name == "rolling-portfolio":
            return deepcopy(self._data.get("rolling-portfolio") or {})
        if name == "binance-canary":
            return deepcopy(self._data.get("binance-canary") or {})
        if name in {"crypto-research", "crypto"}:
            payload = self._data.get("crypto-research") or {}
            page = _fixture_page(payload, params)
            if isinstance(payload, Mapping):
                for key in ("reports", "strategy_reports", "bootstrap_reports", "catalogs", "universe_versions", "universe_version", "symbol_count", "report_count"):
                    if key in payload:
                        page[key] = deepcopy(payload[key])
            return page
        if name in collections:
            return _fixture_page(self._data.get(collections[name]), params)
        if name.startswith("candidates/") and name.endswith("/events"):
            identifier = unquote(name.split("/", 1)[1].rsplit("/", 1)[0])
            detail = _fixture_detail(_fixture_rows(self._data.get("candidates")), identifier)
            return _fixture_page({"items": detail.get("events", [])}, params)
        if name.startswith("datasets/") and name.endswith("/missing-ranges"):
            identifier = unquote(name.split("/", 1)[1].rsplit("/", 1)[0])
            detail = _fixture_detail(_fixture_rows(self._data.get("datasets")), identifier)
            raw_ranges = detail.get("missing_ranges", []) if isinstance(detail, Mapping) else []
            items = [
                {
                    "dataset_id": detail.get("dataset_id"),
                    "dataset_version": detail.get("dataset_version"),
                    "range_index": index,
                    "range": deepcopy(item),
                    "missing_range": deepcopy(item),
                    "range_truncated": False,
                }
                for index, item in enumerate(raw_ranges if isinstance(raw_ranges, list) else [])
                if isinstance(item, Mapping)
            ]
            return _fixture_page({"items": items}, params)
        for prefix, key in (("candidates/", "candidates"), ("datasets/", "datasets"), ("hermes/", "hermes"), ("shadow/", "shadow"), ("crypto-research/", "crypto-research")):
            if name.startswith(prefix):
                identifier = unquote(name.split("/", 1)[1])
                return _fixture_detail(_fixture_rows(self._data.get(key)), identifier)
        return super().v2_snapshot(endpoint, params)

    def ui_record_data(self, kind: str, identifier: str) -> dict[str, Any] | None:
        allowed = {"market", "order", "submission", "reservation", "fill", "risk-fill", "position", "mark", "cashflow"}
        normalized_kind = str(kind or "").strip().lower()
        normalized_id = str(identifier or "").strip()
        if normalized_kind not in allowed or not normalized_id:
            return None
        execution = self._data.get("canary", {}).get("execution", {})
        sources: dict[str, list[dict[str, Any]]] = {
            "market": _fixture_rows(self._data.get("polymarket")),
            "order": _fixture_rows(execution.get("canary_position_requests", [])),
            "submission": _fixture_rows(execution.get("canary_submission_attempts", [])),
            "reservation": _fixture_rows(execution.get("canary_risk_reservations", [])),
            "fill": _fixture_rows(execution.get("canary_position_fills", [])),
            "risk-fill": _fixture_rows(execution.get("canary_risk_fills", [])),
            "position": _fixture_rows(execution.get("canary_position_lots", [])),
            "mark": _fixture_rows(execution.get("canary_equity_marks", execution.get("position_marks", []))),
            "cashflow": _fixture_rows(execution.get("canary_risk_cashflows", execution.get("resolution_payouts", []))),
        }
        id_fields = {
            "market": ("market_id", "id"),
            "order": ("request_id",),
            "submission": ("attempt_id", "id"),
            "reservation": ("reservation_id",),
            "fill": ("fill_id",),
            "risk-fill": ("fill_id",),
            "position": ("position_id",),
            "mark": ("mark_id",),
            "cashflow": ("flow_id",),
        }
        for row in sources[normalized_kind]:
            if any(str(row.get(field, "")) == normalized_id for field in id_fields[normalized_kind]):
                return {"kind": normalized_kind, "id": normalized_id, "record": deepcopy(row), "provenance": {"fixture": True, "source": normalized_kind}}
        return None

    def ui_status_data(self) -> dict[str, Any]:
        """Return the bounded synthetic worker-status projection used by Settings."""
        now = self.clock().astimezone(timezone.utc)
        scenario = str(self._data.get("operator", {}).get("fixture_scenario") or "")
        health_grade = "C" if scenario in {"network_failure", "stale"} else "A"
        health_reason_code = (
            "FIXTURE_STALE_WORKER" if scenario == "stale"
            else "FIXTURE_NETWORK_FAILURE" if scenario == "network_failure"
            else None
        )
        health_reasons = (
            [{"code": health_reason_code, "reason": "Fixture persisted worker evidence is degraded."}]
            if health_reason_code
            else []
        )
        worker_specs = [
            ("health-monitor", "RUNNING", health_grade, health_reason_code),
            ("polymarket-collector", "DEGRADED" if scenario == "network_failure" else "RUNNING", None, None),
            ("axiom-node", "RUNNING", None, None),
        ]
        workers: list[dict[str, Any]] = []
        for worker_name, raw_status, grade, reason_code in worker_specs:
            stale_after = 60.0 if scenario == "stale" and worker_name == "polymarket-collector" else 300.0
            age = 120.0 if scenario == "stale" and worker_name == "polymarket-collector" else 0.0
            heartbeat = (now - timedelta(seconds=age)).isoformat()
            status = str(raw_status).lower()
            if status in {"running", "degraded"} and age > stale_after:
                status = "stale"
            reason = (
                "Fixture persisted worker heartbeat exceeded its stale threshold."
                if status == "stale"
                else "Fixture synthetic network failure."
                if status == "degraded"
                else None
            )
            workers.append(
                {
                    "worker_name": worker_name,
                    "status": status,
                    "started_at": (now - timedelta(minutes=5)).isoformat(),
                    "heartbeat_at": heartbeat,
                    "updated_at": now.isoformat(),
                    "heartbeat_age_seconds": age,
                    "stale_after_seconds": stale_after,
                    "worker_alive": None,
                    "worker_identity_valid": None,
                    "worker_lock_owner_valid": None,
                    "liveness": "PERSISTED_ONLY",
                    "degrading_reason": reason,
                    "reason_code": reason_code,
                    "last_error": "FIXTURE_NETWORK_FAILURE" if status == "degraded" else None,
                    "grade": grade,
                    "reasons": deepcopy(health_reasons if worker_name == "health-monitor" else []),
                    "crypto_paper": None,
                    "payload_bytes": 256,
                    "payload_truncated": False,
                    "payload_projection_pending": False,
                    "projection_truncated": False,
                    "projection_truncated_fields": [],
                }
            )
        degrading_worker = "health-monitor" if health_grade not in {"A", "OK", "HEALTHY"} else (
            next((row["worker_name"] for row in workers if row["status"] in {"degraded", "stale"}), None)
        )
        return {
            "schema": "ui-status.v1",
            "status": "stale" if any(row["status"] == "stale" for row in workers) else (
                "degraded" if health_grade not in {"A", "OK", "HEALTHY"} or any(row["status"] == "degraded" for row in workers)
                else "running"
            ),
            "live_execution": False,
            "workers": workers,
            "workers_total": len(workers),
            "workers_returned": len(workers),
            "workers_considered": len(workers),
            "workers_truncated": False,
            "status_scope": {
                "kind": "persisted_worker_state",
                "scan_limit": 128,
                "details_limit": 64,
                "scan_truncated": False,
                "details_complete": True,
            },
            "provenance": {
                "source": "persisted worker_state dashboard projection",
                "full_status_endpoint": "/api/status",
                "process_identity_verified": False,
                "lock_owner_verified": False,
            },
            "health_grade": health_grade,
            "health_reason_code": health_reason_code,
            "health_reasons": health_reasons,
            "degrading_worker": degrading_worker,
            "degrading_reason": (
                "Fixture persisted worker evidence is degraded."
                if health_reason_code
                else next((row["degrading_reason"] for row in workers if row["degrading_reason"]), None)
            ),
            "historical_maturity_grade": None,
            "historical_error_count": 0,
            "health_window": {"start": None, "end": None, "seconds": None},
        }

    def ui_state_data(self) -> dict[str, Any]:
        canary = self.canary_data()
        execution = canary.get("execution") if isinstance(canary.get("execution"), Mapping) else {}

        def ledger_rows(rows: Any, kind: str, fields: tuple[str, ...]) -> list[dict[str, Any]]:
            result: list[dict[str, Any]] = []
            for raw in rows if isinstance(rows, list) else []:
                if not isinstance(raw, Mapping):
                    continue
                row = deepcopy(dict(raw))
                record_id = next((str(row[field]) for field in fields if row.get(field) not in (None, "")), "")
                if record_id:
                    row.setdefault("record_kind", kind)
                    row.setdefault("record_id", record_id)
                result.append(row)
            return result

        risk_fills = ledger_rows(execution.get("canary_risk_fills") or [], "risk-fill", ("record_id", "fill_id", "id"))
        position_fills = ledger_rows(execution.get("canary_position_fills") or [], "fill", ("record_id", "fill_id", "id"))
        actions = []
        control = self.control
        for entry in list(control.action_history if control is not None else []):
            result = {
                key: entry.get(key)
                for key in ("ok", "reason", "status", "action_id", "authorization_id", "generation")
                if entry.get(key) is not None
            }
            if isinstance(entry.get("result"), Mapping):
                result["result"] = deepcopy(entry["result"])
            actions.append({
                "action_id": entry.get("action_id"),
                "action": entry.get("action"),
                "target": entry.get("target"),
                "status": entry.get("status"),
                "started_at": entry.get("started_at"),
                "completed_at": entry.get("completed_at"),
                "pid": None,
                "reason": entry.get("reason"),
                "result": result,
            })
        actions.sort(key=lambda item: (str(item.get("started_at") or ""), str(item.get("action_id") or "")), reverse=True)
        actions = actions[:32]
        return {
            "schema": "ui-state.v1",
            "fixture": True,
            "fixture_banner": "FIXTURE · synthetic ui-state DTO",
            "execution_authorization": deepcopy(canary.get("execution_authorization")),
            "risk_settings": deepcopy(canary.get("risk_settings")),
            "operator_controls": deepcopy(canary.get("operator_controls")),
            "canary": canary,
            "actions": actions,
            "node": deepcopy(self._data.get("operator", {}).get("worker", {})),
            "ledger": {
                "orders": ledger_rows(list(execution.get("canary_submission_attempts") or []) + list(execution.get("canary_position_requests") or []), "order", ("record_id", "attempt_id", "request_id", "id")),
                "reservations": ledger_rows(execution.get("canary_risk_reservations") or [], "reservation", ("record_id", "reservation_id", "id")),
                "inventory": ledger_rows(execution.get("canary_position_lots") or [], "position", ("record_id", "position_id", "id")),
                "marks": ledger_rows(execution.get("position_marks") or [], "mark", ("record_id", "mark_id", "id")),
                "fills": risk_fills + position_fills,
                "payouts": ledger_rows(execution.get("resolution_payouts") or [], "cashflow", ("record_id", "flow_id", "id")),
                "round_trips": ledger_rows(execution.get("closed_round_trips") or [], "position", ("record_id", "position_id", "id")),
                "unknown_obligations": ledger_rows(execution.get("unknown_obligations") or [], "unknown", ("record_id", "id")),
            },
            "provenance": {
                "authorization": "FixtureDashboardData._base_data",
                "risk": "FixtureDashboardData._base_data",
                "current_review": "FixtureDashboardData._base_data",
                "actions": "FixtureControl.action_history",
                "ledger": "fixture canary ledger projection",
            },
        }

    def snapshot(self, endpoint: str, params: Mapping[str, Any] | None = None) -> Any:
        if endpoint.strip("/").lower() == "ui-status":
            self._fixture_delay()
            return self.ui_status_data()
        if endpoint.strip("/").lower() == "ui-state":
            self._fixture_delay()
            return self.ui_state_data()
        if endpoint.strip("/").lower() == "ui-record":
            self._fixture_delay()
            kind = _query_value(params, "kind")
            identifier = _query_value(params, "id")
            return self.ui_record_data(kind, identifier)
        return super().snapshot(endpoint, params)


class _FixtureAdminHandler(BaseHTTPRequestHandler):
    server: "FixtureAdminServer"

    def log_message(self, *_args: Any) -> None:
        return

    def _send(self, status: int, payload: Any) -> None:
        body = json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _allowed(self) -> bool:
        host = self.headers.get("Host", "").split(":", 1)[0].strip("[]").lower()
        return host in {"127.0.0.1", "localhost", "::1"}

    def do_GET(self) -> None:  # noqa: N802
        if not self._allowed():
            self._send(403, {"error": "localhost fixture host required"})
            return
        if urlparse(self.path).path.rstrip("/") in {"/admin/status", "/admin/action-stats"}:
            self._send(200, self.server.fixture.status())
            return
        self._send(404, {"error": "not found"})

    def do_POST(self) -> None:  # noqa: N802
        if not self._allowed():
            self._send(403, {"error": "localhost fixture host required"})
            return
        path = urlparse(self.path).path.rstrip("/")
        try:
            length = min(int(self.headers.get("Content-Length", "0")), 16_384)
            body = json.loads(self.rfile.read(max(0, length)) or b"{}")
        except (TypeError, ValueError, json.JSONDecodeError):
            self._send(400, {"error": "invalid JSON"})
            return
        if not isinstance(body, Mapping):
            self._send(400, {"error": "object required"})
            return
        fixture = self.server.fixture
        if path == "/admin/scenario":
            try:
                fixture.set_scenario(str(body.get("scenario") or "prepared"))
            except ValueError as exc:
                self._send(400, {"error": str(exc)})
                return
            self._send(200, {"ok": True, "scenario": fixture.scenario, "fixture": True})
            return
        if path == "/admin/behavior":
            control = fixture.control
            if "delay_ms" in body:
                control.delay_ms = max(0, min(int(body["delay_ms"]), 10_000))
            if "drop_next_response" in body:
                control.drop_next_response = bool(body["drop_next_response"])
            if "fail_ancillary" in body:
                control.fail_ancillary = bool(body["fail_ancillary"])
            if "uncertain_next" in body:
                control.uncertain_next = bool(body["uncertain_next"])
            resolution = None
            if body.get("resolve_uncertain"):
                control.uncertain_next = False
                outcome = body.get("uncertain_outcome", body.get("outcome", body.get("status", "")))
                resolution = control.resolve_uncertain(outcome=str(outcome or ""))
            self._send(200, {"ok": True, "fixture": True, "resolution": resolution, "status": fixture.status()})
            return
        self._send(404, {"error": "not found"})


class FixtureAdminServer:
    """Loopback-only fixture orchestration controls, never a production route."""

    def __init__(self, fixture: "FixtureServer", *, port: int = 0) -> None:
        self.fixture = fixture
        self.port = port
        self._server: ThreadingHTTPServer | None = None
        self._thread: Thread | None = None

    @property
    def url(self) -> str | None:
        if self._server is None:
            return None
        host, port = self._server.server_address
        return f"http://{host}:{port}"

    def start(self) -> "FixtureAdminServer":
        if self._server is None:
            self._server = ThreadingHTTPServer(("127.0.0.1", self.port), _FixtureAdminHandler)
            self._server.fixture = self.fixture
            self._thread = Thread(target=self._server.serve_forever, name="axiom-fixture-admin", daemon=True)
            self._thread.start()
        return self

    def stop(self) -> None:
        if self._server is None:
            return
        self._server.shutdown()
        self._server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=2)
        self._server = None
        self._thread = None


class FixtureServer:
    """A DashboardServer with selectable synthetic scenarios.

    Scenario selection is a fixture-service operation, not a production route.
    ``set_scenario`` replaces only the in-memory data facade and never touches
    a real store or venue.
    """

    def __init__(self, scenario: str = "prepared", *, port: int = 0, legacy_html: str | Path | None = None, clock: Callable[[], datetime] | None = None) -> None:
        self.scenario = "prepared"
        self.port = port
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.legacy_html_path = Path(legacy_html) if legacy_html else None
        self.legacy_server: LegacyHtmlServer | None = None
        self.admin_server: FixtureAdminServer | None = None
        self.control = FixtureControl("prepared")
        self.server: DashboardServer | None = None
        self.binance_control = FixtureBinanceControl("prepared")
        self.root_server: FixtureRootServer | None = None
        self.set_scenario(scenario)

    def legacy_html_text(self) -> str:
        """Read the token-free baseline for BEFORE captures only."""
        if self.legacy_html_path is None:
            raise ValueError("legacy_html path was not configured")
        return self.legacy_html_path.read_text(encoding="utf-8")


    @property
    def legacy_url(self) -> str | None:
        return self.legacy_server.url if self.legacy_server else None

    @property
    def admin_url(self) -> str | None:
        return self.admin_server.url if self.admin_server else None

    @property
    def url(self) -> str | None:
        if self.root_server is not None:
            return self.root_server.url
        return self.server.url if self.server else None

    @property
    def upstream_url(self) -> str | None:
        """Direct upstream URL, exposed for fixture-only security-boundary checks."""
        return self.server.url if self.server else None

    @property
    def control_token(self) -> str | None:
        bound = getattr(self.server, "_server", None) if self.server else None
        return getattr(bound, "control_token", None)

    def set_scenario(self, scenario: str) -> None:
        requested = str(scenario or "prepared").strip().lower()
        if requested not in SCENARIOS:
            raise ValueError(f"unknown fixture scenario: {requested}")
        self.scenario = requested
        self.control = FixtureControl(requested, clock=self.clock)
        self.binance_control = FixtureBinanceControl(requested)
        data = _base_data(self.clock())
        _apply_scenario(data, requested, self.clock())
        self.control.projection = data
        data["operator"]["fixture_scenario"] = requested
        data["canary"]["fixture_scenario"] = requested
        if self.server is not None:
            # Scenario changes are deliberately bounded and in-memory. Existing
            # HTTP/CSRF handling remains the DashboardServer's responsibility.
            self.server.data = FixtureDashboardData(data=data, control=self.control, binance_canary=self.binance_control, clock=self.clock)
            bound = getattr(self.server, "_server", None)
            if bound is not None:
                bound.dashboard_data = self.server.data
            root_bound = getattr(self.root_server, "_server", None) if self.root_server is not None else None
            if root_bound is not None:
                root_bound.upstream_dashboard = self.server.data
            if self.legacy_server is not None:
                self.legacy_server.dashboard_data = self.server.data
                legacy_bound = getattr(self.legacy_server, "_server", None)
                if legacy_bound is not None:
                    legacy_bound.dashboard_data = self.server.data
        else:
            self._data = data

    def status(self) -> dict[str, Any]:
        result = dict(self.control.status())
        result["scenario"] = self.scenario
        result["url"] = self.url
        result["upstream_url"] = self.upstream_url
        result["admin_url"] = self.admin_url
        result["legacy_url"] = self.legacy_url
        return result
    def start(self) -> "FixtureServer":
        if self.server is None:
            self.server = DashboardServer(port=self.port, data=FixtureDashboardData(data=self._data, control=self.control, binance_canary=self.binance_control, clock=self.clock)).start()
        if self.root_server is None:
            self.root_server = FixtureRootServer(self.server).start()
        if self.legacy_html_path is not None and self.legacy_server is None:
            self.legacy_server = LegacyHtmlServer(self.legacy_html_path, data=self.server.data).start()
        if self.admin_server is None:
            self.admin_server = FixtureAdminServer(self).start()
        return self
    def stop(self) -> None:
        if self.admin_server is not None:
            self.admin_server.stop()
            self.admin_server = None
        if self.legacy_server is not None:
            self.legacy_server.stop()
            self.legacy_server = None
        if self.root_server is not None:
            self.root_server.stop()
            self.root_server = None
        if self.server is not None:
            self.server.stop()
            self.server = None
    close = stop

    def __enter__(self) -> "FixtureServer":
        return self.start()

    def __exit__(self, *_: Any) -> None:
        self.stop()
def _apply_scenario(data: dict[str, Any], scenario: str, now: datetime | None = None) -> None:
    canary = data["canary"]
    authorization = canary["execution_authorization"]
    execution = canary["execution"]
    stamp = (now or datetime.fromisoformat(FIXTURE_TIME)).astimezone(timezone.utc)
    if scenario == "prepared":
        authorization.update({"status": "REVOKED", "active": None})
        draft = authorization.get("draft")
        if isinstance(draft, dict):
            draft["status"] = "DRAFT"
        canary["control_state"] = "DISARMED"
        canary["readiness"]["status"] = "CURRENT"
    elif scenario == "legacy_missingfield":
        review = canary["operator_controls"].get("exploratory_live_review")
        draft = authorization.get("draft")
        if isinstance(review, dict):
            review.pop("shared_allocation", None)
            choices = review.get("choices")
            if isinstance(choices, dict):
                choices.pop("shared_allocation", None)
            review_choices = review.get("authorization_choices")
            if isinstance(review_choices, dict):
                review_choices.pop("shared_allocation", None)
            setup = review.get("session_setup")
            if isinstance(setup, dict):
                setup["required"] = False
        authorization_view = review.get("authorization")
        if isinstance(authorization_view, dict):
            authorization_view.pop("shared_allocation", None)
        if isinstance(draft, dict):
            draft.pop("shared_allocation", None)
        canary["control_state"] = "DISARMED"
    elif scenario == "no_proposal":
        review = canary["operator_controls"].get("exploratory_live_review")
        draft = authorization.get("draft")
        if isinstance(review, dict):
            review["proposal"] = {"status": "UNAVAILABLE", "members": []}
            review.update({"status": "REVIEW_REQUIRED", "proposal_status": "UNAVAILABLE", "members": [], "selected_setups": [], "setup_bindings": [], "blockers": ["PROPOSAL_REQUIRED"]})
            review.pop("shared_allocation", None)
            choices = review.get("choices")
            if isinstance(choices, dict):
                choices.pop("shared_allocation", None)
            review_choices = review.get("authorization_choices")
            if isinstance(review_choices, dict):
                review_choices.pop("shared_allocation", None)
            setup = review.get("session_setup")
            if isinstance(setup, dict):
                setup["required"] = True
        authorization_view = review.get("authorization")
        if isinstance(authorization_view, dict):
            authorization_view.pop("shared_allocation", None)
        if isinstance(draft, dict):
            draft.pop("shared_allocation", None)
        canary["control_state"] = "DISARMED"
    elif scenario == "positive_proposed_zero_active":
        review = canary["operator_controls"].get("exploratory_live_review")
        draft = authorization.get("draft")
        if isinstance(review, dict):
            review.pop("shared_allocation", None)
            choices = review.get("choices")
            if isinstance(choices, dict):
                choices.pop("shared_allocation", None)
            review_choices = review.get("authorization_choices")
            if isinstance(review_choices, dict):
                review_choices.pop("shared_allocation", None)
        authorization_view = review.get("authorization")
        if isinstance(authorization_view, dict):
            authorization_view.pop("shared_allocation", None)
        if isinstance(draft, dict):
            draft.pop("shared_allocation", None)
        active = deepcopy(draft) if isinstance(draft, Mapping) else {}
        active.update({"status": "ACTIVE", "active": True, "allocation_active": True, "shared_allocation": "0.00", "proposed_allocation_total": "0.00"})
        authorization["active"] = active
        authorization["status"] = "REVOKED"
        canary["control_state"] = "DISARMED"
    elif scenario in {"poll", "reload"}:
        authorization.update({"status": "REVOKED", "active": None})
        draft = authorization.get("draft")
        if isinstance(draft, dict):
            draft["status"] = "DRAFT"
        canary["control_state"] = "DISARMED"
    elif scenario == "stale":
        canary["readiness"].update({"status": "STALE", "checked_at": (stamp - timedelta(seconds=120)).isoformat()})
        canary["readiness_snapshot_status"] = "STALE"
        canary["readiness_snapshot_stale"] = True
    elif scenario == "changed":
        authorization.update({"status": "DRAFT", "active": None})
        draft = authorization.get("draft")
        changed_duration = int(draft.get("duration_seconds") or 0) + 1 if isinstance(draft, dict) else 322
        if isinstance(draft, dict):
            draft.update({"status": "DRAFT", "purpose": "Fixture terms changed", "duration_seconds": changed_duration})
        review = canary["operator_controls"].get("exploratory_live_review")
        if isinstance(review, dict):
            review.update({"status": "TERMS_CHANGED", "proposal_status": "REVIEW_REQUIRED", "blockers": ["TERMS_CHANGED"], "readiness": {"status": "TERMS_CHANGED", "checks": ["terms"], "checked_at": stamp.isoformat()}, "duration_seconds": changed_duration})
            choices = review.get("choices")
            if isinstance(choices, dict):
                choices["duration_seconds"] = changed_duration
            authorization_view = review.get("authorization")
            if isinstance(authorization_view, dict):
                authorization_view.update({"status": "DRAFT", "purpose": "Fixture terms changed", "duration_seconds": changed_duration})
    elif scenario == "adverse_required":
        review = canary["operator_controls"].get("exploratory_live_review")
        if isinstance(review, dict):
            review["adverse_evidence"] = {"required": True, "acknowledged": False, "status": "REQUIRED", "items": ["FIXTURE_ADVERSE_EVIDENCE_REVIEW"]}
            review["blockers"] = ["ADVERSE_EVIDENCE_ACK_REQUIRED"]
            review["status"] = "REVIEW_REQUIRED"
            review["proposal_status"] = "REVIEW_REQUIRED"
            choices = review.get("choices")
            if isinstance(choices, dict):
                choices["adverse_evidence_ack_required"] = True
        draft = authorization.get("draft")
        if isinstance(draft, dict):
            draft["adverse_evidence_ack"] = {"acknowledged": False, "required": True}
    elif scenario == "adverse_not_required":
        review = canary["operator_controls"].get("exploratory_live_review")
        if isinstance(review, dict):
            review["adverse_evidence"] = {"required": False, "acknowledged": False, "status": "NOT_REQUIRED", "items": []}
            review["blockers"] = []
        draft = authorization.get("draft")
        if isinstance(draft, dict):
            draft["adverse_evidence_ack"] = {"acknowledged": False, "required": False}
    elif scenario == "missingaccount":
        canary["readiness"].update({"status": "ACCOUNT_DISCONNECTED", "account": "UNKNOWN"})
        review = canary["operator_controls"].get("exploratory_live_review")
        if isinstance(review, dict):
            review["readiness"] = {"status": "ACCOUNT_DISCONNECTED", "checks": ["account"], "checked_at": FIXTURE_TIME}
            review["blockers"] = ["ACCOUNT_DISCONNECTED"]
    elif scenario == "missingauth":
        canary["execution_authorization"] = None
        canary["operator_controls"]["execution_authorization"] = None
        review = canary["operator_controls"].get("exploratory_live_review")
        if isinstance(review, dict):
            review.update({"status": "UNAVAILABLE", "proposal_status": "UNAVAILABLE", "blockers": ["EXECUTION_AUTHORIZATION_REQUIRED"], "authorization": None, "authorization_choices": {}})
        data["operator"]["execution_authorization"] = None
        data["execution_authorization"] = None
        canary["control_state"] = "DISARMED"
    elif scenario == "armed_no_permission":
        authorization["status"] = "DRAFT"
        authorization["active"] = None
        controls = canary["operator_controls"]
        controls.update({"armed": True, "armed_state": "ARMED", "execution_authorization": deepcopy(authorization)})
        review = controls.get("exploratory_live_review")
        if isinstance(review, dict):
            review.update({"status": "BLOCKED", "proposal_status": "REVIEW_REQUIRED", "blockers": ["PERMISSION_REQUIRED"]})
        canary["control_state"] = "ARMED"
    elif scenario == "no_members":
        data["rolling-portfolio"].update({"active_member_count": 0, "active_rows": []})
        data["rolling-portfolio"]["policy_review"]["proposed"]["member_count"] = 0
        review = canary["operator_controls"].get("exploratory_live_review")
        if isinstance(review, dict):
            review.update({"status": "BLOCKED", "proposal_status": "NO_MEMBERS", "members": [], "selected_setups": [], "blockers": ["NO_MEMBERS"], "no_member_reason": {"code": "NO_MEMBERS", "selection_status": "EMPTY", "k": 0}})
            proposal = review.get("proposal")
            if isinstance(proposal, dict):
                proposal.update({"status": "NO_MEMBERS", "members": []})
    elif scenario == "unaffordable":
        canary["risk_settings"]["remaining"]["gross_daily_buy_usd"] = "0.00"
        canary["risk_settings"]["remaining"]["exploratory_lifetime_usd"] = "0.00"
        canary["decision"] = {"status": "UNAFFORDABLE", "reason_code": "UNAFFORDABLE"}
        review = canary["operator_controls"].get("exploratory_live_review")
        if isinstance(review, dict):
            review.update({"status": "BLOCKED", "proposal_status": "REVIEW_REQUIRED", "affordability": {"status": "UNAFFORDABLE", "remaining_exploratory_lifetime_usd": "0.00"}, "blockers": ["UNAFFORDABLE"]})
    elif scenario == "network_failure":
        data["overview-summary"]["error"] = "FIXTURE_NETWORK_FAILURE"
        canary["error"] = "FIXTURE_NETWORK_FAILURE"
    elif scenario == "active_no_signal":
        authorization["status"] = "ACTIVE"
        draft = authorization.get("draft")
        active = deepcopy(draft) if isinstance(draft, Mapping) else {}
        duration = int(active.get("duration_seconds") or 0)
        confirmed_expires = (stamp + timedelta(seconds=duration)).isoformat() if duration > 0 else None
        active.update({"status": "ACTIVE", "active": True, "live_execution": False, "paper_only": True, "expires_at": confirmed_expires})
        authorization.update({"status": "ACTIVE", "live_execution": False, "paper_only": True, "expires_at": confirmed_expires})
        authorization["active"] = active
        canary.update({"display_state": "ENABLED", "control_state": "ARMED", "micro_live_canary": "ACTIVE", "live_execution": False, "paper_only": True})
        canary["autonomous"].update({"enabled": True, "control_state": "ARMED", "next_decision": "NO_SIGNAL"})
        canary["operator_controls"].update({"armed": True, "armed_state": "ARMED", "live_execution": False, "paper_only": True, "execution_authorization": deepcopy(authorization)})

        review = canary["operator_controls"].get("exploratory_live_review")
        if isinstance(review, dict):
            review.update({"status": "CONFIRMED", "proposal_status": "CONFIRMED", "permission": "ACTIVE", "blockers": [], "expires_at": confirmed_expires})
            review_auth = review.get("authorization")
            if isinstance(review_auth, dict):
                review_auth.update({"status": "ACTIVE", "active": True, "live_execution": False, "paper_only": True, "expires_at": confirmed_expires})
            for choices_key in ("choices", "authorization_choices"):
                choices = review.get(choices_key)
                if isinstance(choices, dict):
                    choices.update({"approved": True, "status": "APPROVED", "expires_at": confirmed_expires})
        canary["decision"] = {"status": "NO_SIGNAL", "reason_code": "NO_SIGNAL"}
    elif scenario == "open_position":
        lot = {
            "position_id": "fixture-position-aurora",
            "record_kind": "position",
            "record_id": "fixture-position-aurora",
            "market_id": "fixture-market-aurora",
            "event_id": "fixture-event-aurora",
            "venue": "POLYMARKET",
            "token_id": "fixture-token-aurora-yes",
            "side": "BUY",
            "token": "YES",
            "quantity": "0.25",
            "sold_quantity": "0",
            "cost_basis": "0.105",
            "fees": "0.0001",
            "realized_pnl": "0.0000",
            "status": "OPEN",
            "opened_at": stamp.isoformat(),
            "updated_at": stamp.isoformat(),
        }
        execution["canary_position_lots"] = [lot]
        execution["position_marks"] = [{"mark_id": "fixture-mark-aurora", "record_kind": "mark", "record_id": "fixture-mark-aurora", "position_id": lot["position_id"], "market_id": lot["market_id"], "token_id": "fixture-token-aurora-yes", "side": "SELL", "quantity": "0.25", "mark_price": "0.44", "cost_basis_usd": "0.105", "mark_fee": "0", "marked_value": "0.11", "observed_at": stamp.isoformat(), "marked_at": stamp.isoformat(), "source": "FIXTURE_SOURCE"}]
        execution["canary_equity_marks"] = execution["position_marks"]
        execution["open_inventory"] = [lot]
        execution["open_positions"] = 1
    elif scenario == "partial_fill":
        execution["orders"][0].update({"status": "PARTIAL", "stage": "PARTIAL"})
        execution["fills"][0]["filled_quantity"] = "0.25"
    elif scenario == "unknown_order":
        execution["orders"][0].update({"status": "UNKNOWN", "stage": "ATTEMPTED"})
        unknown = deepcopy(execution["orders"][0])
        unknown.update({"record_kind": "submission", "record_id": unknown.get("attempt_id"), "status": "UNKNOWN", "reason": "OUTCOME_UNCERTAIN", "amount": "0.42", "created_at": stamp.isoformat()})
        execution["unknown_obligations"] = [unknown]
        authorization.update({"status": "EXPIRED", "active": None, "expires_at": (stamp - timedelta(seconds=30)).isoformat()})
        draft = authorization.get("draft")
        if isinstance(draft, dict):
            draft.update({"status": "EXPIRED", "expires_at": authorization["expires_at"]})
        review = canary["operator_controls"].get("exploratory_live_review")
        if isinstance(review, dict):
            review.update({"status": "BLOCKED", "proposal_status": "EXPIRED", "blockers": ["UNKNOWN_ORDER", "STOP_AND_REVIEW"]})
        canary["control_state"] = "DISARMED"
    elif scenario == "expired":
        authorization.update({"status": "EXPIRED", "active": None, "expires_at": (stamp - timedelta(seconds=30)).isoformat()})
        draft = authorization.get("draft")
        if isinstance(draft, dict):
            draft.update({"status": "EXPIRED", "expires_at": authorization["expires_at"]})
        review = canary["operator_controls"].get("exploratory_live_review")
        if isinstance(review, dict):
            review.update({"status": "BLOCKED", "proposal_status": "EXPIRED", "blockers": ["AUTHORIZATION_EXPIRED"]})
        canary["control_state"] = "DISARMED"
    elif scenario == "revoked":
        authorization.update({"status": "REVOKED", "active": None})
        draft = authorization.get("draft")
        if isinstance(draft, dict):
            draft["status"] = "REVOKED"
        review = canary["operator_controls"].get("exploratory_live_review")
        if isinstance(review, dict):
            review.update({"status": "BLOCKED", "proposal_status": "REVOKED", "blockers": ["AUTHORIZATION_REVOKED"]})
        canary["control_state"] = "DISARMED"
    elif scenario in {"missing", "empty"}:
        for key in ("polymarket", "candidates", "datasets", "activity", "hermes", "paper", "crypto-research"):
            payload = data[key]
            if isinstance(payload, dict):
                payload["items"] = []
                payload["total"] = 0
                payload["pages"] = 0
        data["overview-summary"]["counts"] = {"markets": 0, "candidates": 0, "datasets": 0}
        data["crypto-research"]["symbol_count"] = 0
        data["crypto-research"]["report_count"] = 0
        if scenario == "missing":
            canary["risk_settings"]["remaining"] = None
            canary["readiness"] = {"status": "UNKNOWN"}

    auth_value = deepcopy(canary.get("execution_authorization"))
    controls = canary.get("operator_controls")
    if isinstance(controls, dict):
        controls["execution_authorization"] = deepcopy(auth_value)
    operator = data.get("operator")
    if isinstance(operator, dict):
        operator["execution_authorization"] = deepcopy(auth_value)
        operator["operator_controls"] = deepcopy(controls)
    data["execution_authorization"] = deepcopy(auth_value)

def fixture_payload(scenario: str = "prepared") -> dict[str, Any]:
    """Return a fresh, secret-free payload for pure adapter tests."""
    if scenario not in SCENARIOS:
        raise ValueError(scenario)
    payload = _base_data()
    _apply_scenario(payload, scenario)
    return deepcopy(payload)

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Serve sanitized AXIOM UI fixtures on loopback.")
    parser.add_argument("--scenario", choices=SCENARIOS, default="prepared")
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--legacy-html", type=Path, help="Serve the token-free BEFORE baseline alongside the fixture.")
    args = parser.parse_args(argv)
    fixture = FixtureServer(args.scenario, port=args.port, legacy_html=args.legacy_html)
    fixture.start()
    print(f"FIXTURE URL={fixture.url} SCENARIO={fixture.scenario}", flush=True)
    if fixture.upstream_url:
        print(f"FIXTURE_UPSTREAM URL={fixture.upstream_url}", flush=True)
    if fixture.admin_url:
        print(f"FIXTURE_ADMIN URL={fixture.admin_url}", flush=True)
    if fixture.legacy_url:
        print(f"LEGACY_HTML URL={fixture.legacy_url}", flush=True)
    try:
        input()
    except (EOFError, KeyboardInterrupt):
        pass
    finally:
        fixture.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
__all__ = ["FIXTURE_TIME", "SCENARIOS", "FixtureControl", "FixtureBinanceControl", "FixtureDashboardData", "FixtureAdminServer", "FixtureRootServer", "FixtureServer", "LegacyHtmlServer", "fixture_payload", "main"]

