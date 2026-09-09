"""Typed, localhost-only operator controls for the paper research node.

This module deliberately has no shell or browser-provided command execution. The
control surface maps a small allowlist to existing in-process service APIs and
persists every requested action as a bounded audit record.
"""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
import ipaddress
import os
from pathlib import Path
import re
import subprocess
import sqlite3
import sys
import threading
import time
from typing import Any, Callable, Mapping

from .autonomous import AutonomousResearchProcessor
from .canary import (
    AUTONOMOUS_CANARY_LIMITS,
    CanaryBlocked,
    CanaryService,
    CredentialStore,
    PolymarketClobV2Venue,
)
from .bootstrap import BTC_HISTORY_START, HistoricalBootstrapper
from .crypto_universe import load_crypto_universe
from .data import BinanceAdapter
from .domain import utc_now
from .node import _pid_alive, _pid_matches_node
from .storage import AxiomStore


DEFAULT_HERMES_JOB_ID = "f1d27bf8c27a"
BOOTSTRAP_JOB_NAME = "crypto-universe-bootstrap"
HERMES_STATE_NAME = "hermes-control"
# The configured Hermes job is an external reference only.  The operator has
# no local verifier for that process, so scheduler state must never be exposed
# as the external job's status.
HERMES_CONTROL_SCOPE = "INTERNAL_RESEARCH_QUEUE_PROCESSOR"
HERMES_EXTERNAL_STATUS = "UNKNOWN"
HERMES_EXTERNAL_EVIDENCE = (
    "No local verifier is available for the external Hermes job; "
    "scheduler state is internal-only."
)
_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,255}$")
_SECRET_KEY = re.compile(r"(?:secret|private|password|token|api[_-]?key|credential|mnemonic)", re.I)
_ALLOWED_ACTIONS = frozenset(
    {
        "node.restart",
        "bootstrap.start",
        "bootstrap.resume",
        "hermes.pause",
        "hermes.resume",
        "hermes.run_now",
        "canary.connectivity_check",
        "canary.eligibility.verify",
        "canary.eligibility.mark",
        "canary.generate_signal",
        "canary.arm",
        "canary.enable_auto",
        "canary.disarm",
        "canary.kill",
    }
)
_CONFIRMATIONS = {
    "canary.eligibility.mark": "MARK CANARY ELIGIBLE",
    "canary.arm": "ARM",
    "canary.enable_auto": "ENABLE AUTO CANARY",
    "canary.disarm": "DISARM",
    "canary.kill": "KILL",
}
CANARY_CONNECTIVITY_CONFIG_KEY = "canary_connectivity_status"

_CONNECTIVITY_FAILURE_REASONS = {
    "CONNECTIVITY_CHECK_FAILED": "Connectivity check failed.",
    "CREDENTIALS_NOT_CONFIGURED": "Polymarket credentials are not configured.",
    "VENUE_REQUIRED": "A Polymarket venue is required for the connectivity check.",
    "OFFICIAL_POLYMARKET_SDK_NOT_INSTALLED": "The Polymarket SDK is not installed.",
    "UNSUPPORTED_POLYMARKET_SDK": "The installed Polymarket SDK version is unsupported.",
    "OFFICIAL_POLYMARKET_SDK_NOT_READONLY_COMPATIBLE": "The installed Polymarket SDK does not support safe read-only connectivity.",
    "GEOGRAPHICALLY_BLOCKED": "The current region is blocked from Polymarket access.",
    "GEOBLOCK_CHECK_FAILED": "The geographic access check failed.",
    "AUTHENTICATED_CONNECTIVITY_FAILED": "Authenticated connectivity failed.",
    "ACCOUNT_CHECK_FAILED": "The account check failed.",
    "BALANCE_CHECK_FAILED": "The balance check failed.",
    "BALANCE_RESPONSE_INVALID": "The balance response was invalid.",
    "INSUFFICIENT_BALANCE": "Available balance is below the $1 canary requirement.",
    "CANARY_ALLOWANCE_UNAVAILABLE": "Allowance information is unavailable.",
    "CANARY_ALLOWANCE_INSUFFICIENT": "Current allowance is below the amount required for a $1 canary.",
    "CANARY_SPENDER_UNAVAILABLE": "The canary allowance spender is unavailable.",
    "MARKET_CONNECTIVITY_FAILED": "Market connectivity failed.",
    "MARKET_NOT_ACCEPTING_ORDERS": "The market is not accepting orders.",
    "MARKET_OUTCOMES_UNAVAILABLE": "Market outcomes are unavailable.",
    "MARKET_OUTCOME_NOT_ALLOWED": "The requested market outcome is not allowed.",
    "MARKET_OUTCOME_ID_UNAVAILABLE": "The market outcome identifier is unavailable.",
    "VENUE_MINIMUM_EXCEEDS_CANARY_TARGET": "The venue minimum exceeds the $1 canary target.",
}
_CONNECTIVITY_FAILURE_CODES = frozenset(_CONNECTIVITY_FAILURE_REASONS)
_CONNECTIVITY_TEXT_LIMIT = 128
_CONNECTIVITY_MAX_FAILURES = 16
_CONNECTIVITY_SUPPORTED_SDK = "0.9"
_CONNECTIVITY_CODE = re.compile(r"^[A-Z0-9][A-Z0-9_.:-]{0,63}$")
_CONNECTIVITY_LABEL = re.compile(r"^[A-Za-z0-9][A-Za-z0-9 _./:-]{0,63}$")
_CONNECTIVITY_DECIMAL = re.compile(r"^(?:0|[1-9][0-9]*)(?:\.[0-9]+)?$")
_CONNECTIVITY_VERSION = re.compile(r"^[vV]?[0-9]{1,8}(?:\.[0-9]{1,8}){0,7}(?:[-+][0-9A-Za-z.-]{1,32})?$")
_CONNECTIVITY_ADDRESS = re.compile(r"0x[0-9a-fA-F]{16,}", re.I)
_CONNECTIVITY_HEX_ADDRESS = re.compile(r"(?<![A-Za-z0-9])[0-9a-fA-F]{32,}(?![A-Za-z0-9])")
_CONNECTIVITY_SENSITIVE_TEXT = re.compile(
    r"(?<![A-Za-z0-9])(?:address|api|credential|key|mnemonic|passphrase|private|raw|secret|spender|token)(?![A-Za-z0-9])",
    re.I,
)


def _connectivity_text(value: Any, *, pattern: re.Pattern[str] = _CONNECTIVITY_LABEL) -> str | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if (
        not text
        or len(text) > _CONNECTIVITY_TEXT_LIMIT
        or _CONNECTIVITY_ADDRESS.search(text)
        or _CONNECTIVITY_HEX_ADDRESS.search(text)
        or _CONNECTIVITY_SENSITIVE_TEXT.search(text)
        or pattern.fullmatch(text) is None
    ):
        return None
    return text


def _connectivity_version(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    version = value.strip()
    if (
        not version
        or len(version) > _CONNECTIVITY_TEXT_LIMIT
        or _CONNECTIVITY_SENSITIVE_TEXT.search(version)
        or _CONNECTIVITY_VERSION.fullmatch(version) is None
    ):
        return None
    return version


def _connectivity_decimal(value: Any) -> str | None:
    """Return only bounded, canonical, nonnegative fixed-point decimals."""
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int):
        if value < 0 or value.bit_length() > 430:
            return None
        text = str(value)
    elif isinstance(value, (str, Decimal, float)):
        try:
            text = str(value).strip()
        except Exception:
            return None
    else:
        return None
    if (
        not text
        or len(text) > _CONNECTIVITY_TEXT_LIMIT
        or _CONNECTIVITY_DECIMAL.fullmatch(text) is None
    ):
        return None
    try:
        decimal_value = Decimal(text)
    except (ArithmeticError, TypeError, ValueError):
        return None
    if not decimal_value.is_finite():
        return None
    formatted = format(decimal_value, "f")
    return formatted if len(formatted) <= _CONNECTIVITY_TEXT_LIMIT else None


def _connectivity_checked_at(value: Any) -> str | None:
    if isinstance(value, datetime):
        stamp = value
    elif isinstance(value, str):
        stamp = value.strip()
        if not stamp:
            return None
        try:
            stamp = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if stamp.tzinfo is None:
        return None
    return stamp.astimezone(timezone.utc).isoformat()


def _connectivity_failure_codes(raw: Mapping[str, Any], *, ready: bool) -> list[str]:
    """Return a bounded, deterministic set of canonical failure codes."""
    result: list[str] = []
    unknown = False
    generic_seen = False
    scan_limit = _CONNECTIVITY_MAX_FAILURES * 4
    for field in ("failure_codes", "failures"):
        if field not in raw:
            continue
        values = raw[field]
        if isinstance(values, str):
            values = (values,)
        elif not isinstance(values, (list, tuple, set, frozenset)):
            unknown = True
            continue
        try:
            value_count = len(values)
        except Exception:
            value_count = scan_limit + 1
        if value_count > scan_limit:
            unknown = True
        # Sets have no input order.  Keep their canonical output in the fixed
        # failure-code order below, while list/tuple inputs retain their order.
        unordered = isinstance(values, (set, frozenset))
        set_codes: set[str] = set()
        try:
            iterator = iter(values)
            for index, value in enumerate(iterator):
                if index >= scan_limit:
                    break
                if not isinstance(value, str):
                    unknown = True
                    continue
                if len(value) > _CONNECTIVITY_TEXT_LIMIT:
                    unknown = True
                    continue
                code = value.strip().upper()
                if code == "CONNECTIVITY_CHECK_FAILED":
                    generic_seen = True
                elif code in _CONNECTIVITY_FAILURE_CODES:
                    if unordered:
                        set_codes.add(code)
                    elif code not in result:
                        result.append(code)
                else:
                    unknown = True
        except Exception:
            unknown = True
        if unordered:
            for code in _CONNECTIVITY_FAILURE_REASONS:
                if code in set_codes and code not in result:
                    result.append(code)
    fallback_needed = generic_seen or unknown or (not ready and not result)
    if fallback_needed:
        result = result[: _CONNECTIVITY_MAX_FAILURES - 1]
        result.append("CONNECTIVITY_CHECK_FAILED")
    return result[:_CONNECTIVITY_MAX_FAILURES]


def _connectivity_sdk_supported(value: Any) -> bool:
    version = _connectivity_version(value)
    if version is None:
        return False
    parts = version.lstrip("vV").split(".")
    supported = _CONNECTIVITY_SUPPORTED_SDK.split(".")
    return len(parts) >= len(supported) and parts[: len(supported)] == supported


def _connectivity_status(value: Any, *, pass_values: frozenset[str], fail_values: frozenset[str]) -> str | None:
    if not isinstance(value, str):
        return None
    status = value.strip().upper()
    if status in pass_values:
        return "PASS"
    if status in fail_values:
        return "FAIL"
    if status == "SKIPPED":
        return "SKIPPED"
    return None


def _project_connectivity(value: Any, *, checked_at: Any = None) -> dict[str, Any]:
    """Project service diagnostics into the deliberately small operator schema."""
    raw = value if isinstance(value, Mapping) else {}
    diagnostics = raw.get("diagnostics") if isinstance(raw.get("diagnostics"), Mapping) else {}
    requested_ready = raw.get("ready") if isinstance(raw.get("ready"), bool) else False
    # Delay the not-ready fallback until projected diagnostics have supplied a
    # more useful canonical reason.
    failure_codes = _connectivity_failure_codes(raw, ready=True)

    def add_failure(code: str) -> None:
        if code not in _CONNECTIVITY_FAILURE_CODES or code in failure_codes:
            return
        if len(failure_codes) < _CONNECTIVITY_MAX_FAILURES:
            failure_codes.append(code)
    sdk_raw = raw.get("sdk") if isinstance(raw.get("sdk"), Mapping) else diagnostics
    sdk_version = _connectivity_version(
        sdk_raw.get("version") if "version" in sdk_raw else sdk_raw.get("sdk_version")
    )
    if isinstance(raw.get("sdk"), Mapping) and "installed" in sdk_raw:
        sdk_installed = sdk_raw.get("installed") is True
    else:
        sdk_installed = sdk_version is not None
    if isinstance(sdk_raw.get("status"), str):
        sdk_status = sdk_raw["status"].strip().upper()
        if sdk_status in {"NOT INSTALLED", "MISSING"}:
            sdk_installed = False

    credentials_raw = raw.get("credentials") if isinstance(raw.get("credentials"), Mapping) else {}
    credentials_configured = raw.get("credentials_configured")
    if not isinstance(credentials_configured, bool):
        credentials_configured = diagnostics.get("credentials_configured")
    if not isinstance(credentials_configured, bool):
        credentials_configured = credentials_raw.get("status") == "CONFIGURED"
    credentials_status = str(credentials_raw.get("status") or "").strip().upper()
    if credentials_status == "NOT CONFIGURED":
        credentials_configured = False
    elif credentials_status == "CONFIGURED" and not isinstance(raw.get("credentials_configured"), bool):
        credentials_configured = True

    def source(name: str, diagnostic_name: str | None = None) -> Mapping[str, Any]:
        candidate = raw.get(name)
        if isinstance(candidate, Mapping):
            return candidate
        candidate = diagnostics.get(diagnostic_name or name)
        return candidate if isinstance(candidate, Mapping) else {}

    def explicit_status(
        candidate: Mapping[str, Any],
        *,
        pass_values: frozenset[str],
        fail_values: frozenset[str],
    ) -> str | None:
        status = _connectivity_status(
            candidate.get("status"),
            pass_values=pass_values,
            fail_values=fail_values,
        )
        raw_status = str(candidate.get("status") or "").strip().upper()
        if status is None and raw_status and raw_status != "SKIPPED":
            status = "FAIL"
        return status

    authentication_raw = source("authentication")
    authentication_status = explicit_status(
        authentication_raw,
        pass_values=frozenset({"OK", "PASS", "PASSED", "SUCCESS"}),
        fail_values=frozenset({"FAILED", "FAIL", "ERROR"}),
    )
    if authentication_status is None and "AUTHENTICATED_CONNECTIVITY_FAILED" in failure_codes:
        authentication_status = "FAIL"
    if not authentication_status and not credentials_configured:
        authentication_status = "SKIPPED"
    authentication = {"status": authentication_status or "SKIPPED"}

    account_raw = source("account")
    account_status = explicit_status(
        account_raw,
        pass_values=frozenset({"OK", "PASS", "PASSED", "SUCCESS"}),
        fail_values=frozenset({"FAILED", "FAIL", "ERROR"}),
    )
    if isinstance(account_raw.get("authenticated"), bool):
        account_status = "PASS" if account_raw["authenticated"] else "FAIL"
    if account_status is None and "ACCOUNT_CHECK_FAILED" in failure_codes:
        account_status = "FAIL"
    account = {
        "status": account_status or "SKIPPED",
        "wallet_type": _connectivity_text(account_raw.get("wallet_type")),
    }

    geoblock_raw = source("geoblock")
    geoblock_status = explicit_status(
        geoblock_raw,
        pass_values=frozenset({"OK", "PASS", "PASSED", "SUCCESS"}),
        fail_values=frozenset({"FAILED", "FAIL", "ERROR"}),
    )
    if str(geoblock_raw.get("status") or "").strip().upper() == "BLOCKED":
        geoblock_status = "BLOCKED"
    if geoblock_raw.get("blocked") or geoblock_raw.get("close_only"):
        geoblock_status = "BLOCKED"
    elif geoblock_status is None and isinstance(geoblock_raw.get("blocked"), bool):
        geoblock_status = "PASS"
    if "GEOGRAPHICALLY_BLOCKED" in failure_codes:
        geoblock_status = "BLOCKED"
    elif "GEOBLOCK_CHECK_FAILED" in failure_codes and geoblock_status != "BLOCKED":
        geoblock_status = "FAIL"
    geoblock = {
        "status": geoblock_status or "SKIPPED",
        "country": _connectivity_text(geoblock_raw.get("country")),
        "region": _connectivity_text(geoblock_raw.get("region")),
    }

    balance_raw = source("balance")
    balance_status = explicit_status(
        balance_raw,
        pass_values=frozenset({"OK", "PASS", "PASSED", "SUCCESS"}),
        fail_values=frozenset({"FAILED", "FAIL", "ERROR"}),
    )
    available_usd = _connectivity_decimal(balance_raw.get("available_usd"))
    balance_available_status = str(balance_raw.get("status") or "").strip().upper()
    if "available_usd" in balance_raw and available_usd is None and balance_available_status != "SKIPPED":
        balance_status = "FAIL"
        add_failure("BALANCE_RESPONSE_INVALID")
    if balance_status == "PASS" and available_usd is None:
        balance_status = "FAIL"
        add_failure("BALANCE_RESPONSE_INVALID")
    if "BALANCE_CHECK_FAILED" in failure_codes or "BALANCE_RESPONSE_INVALID" in failure_codes:
        balance_status = "FAIL"
    balance = {"status": balance_status or "SKIPPED", "available_usd": available_usd}

    allowance_raw = source("allowance")
    allowance_value = str(allowance_raw.get("status") or "").strip().upper()
    if "CANARY_ALLOWANCE_INSUFFICIENT" in failure_codes or allowance_value == "INSUFFICIENT":
        allowance_status = "INSUFFICIENT"
    elif "CANARY_ALLOWANCE_UNAVAILABLE" in failure_codes or allowance_value in {"FAILED", "FAIL", "ERROR", "UNAVAILABLE"}:
        allowance_status = "UNAVAILABLE"
    elif allowance_value in {"SUFFICIENT"}:
        allowance_status = "SUFFICIENT"
    elif allowance_value in {"OK", "PASS", "PASSED", "SUCCESS", "AVAILABLE"}:
        allowance_status = "AVAILABLE"
    elif allowance_value and allowance_value != "SKIPPED":
        allowance_status = "UNAVAILABLE"
    else:
        allowance_status = "SKIPPED"
    allowance = {"status": allowance_status}

    market_raw = source("market")
    market_status = explicit_status(
        market_raw,
        pass_values=frozenset({"OK", "PASS", "PASSED", "SUCCESS"}),
        fail_values=frozenset({"FAILED", "FAIL", "ERROR"}),
    )
    if market_status is None and market_raw:
        market_status = (
            "FAIL"
            if any(code in failure_codes for code in {
                "MARKET_CONNECTIVITY_FAILED",
                "MARKET_NOT_ACCEPTING_ORDERS",
                "MARKET_OUTCOMES_UNAVAILABLE",
                "MARKET_OUTCOME_NOT_ALLOWED",
                "MARKET_OUTCOME_ID_UNAVAILABLE",
                "VENUE_MINIMUM_EXCEEDS_CANARY_TARGET",
            })
            else "PASS"
        )
    market = {"status": market_status or "SKIPPED"}

    order_book_raw = source("order_book", "book")
    order_book_status = explicit_status(
        order_book_raw,
        pass_values=frozenset({"OK", "PASS", "PASSED", "SUCCESS"}),
        fail_values=frozenset({"FAILED", "FAIL", "ERROR"}),
    )
    if order_book_status is None and order_book_raw:
        order_book_status = "FAIL" if "MARKET_CONNECTIVITY_FAILED" in failure_codes else "PASS"
    order_book = {"status": order_book_status or "SKIPPED"}

    enforce_diagnostics = requested_ready or not failure_codes
    if enforce_diagnostics:
        if not credentials_configured:
            add_failure("CREDENTIALS_NOT_CONFIGURED")
        else:
            if not sdk_installed:
                add_failure("OFFICIAL_POLYMARKET_SDK_NOT_INSTALLED")
            elif not _connectivity_sdk_supported(sdk_version):
                add_failure("UNSUPPORTED_POLYMARKET_SDK")
            if authentication["status"] != "PASS":
                add_failure("AUTHENTICATED_CONNECTIVITY_FAILED")
            if account["status"] != "PASS":
                add_failure("ACCOUNT_CHECK_FAILED")
            if geoblock["status"] == "BLOCKED":
                add_failure("GEOGRAPHICALLY_BLOCKED")
            elif geoblock["status"] != "PASS":
                add_failure("GEOBLOCK_CHECK_FAILED")
            if balance["status"] != "PASS":
                add_failure(
                    "BALANCE_RESPONSE_INVALID"
                    if balance["available_usd"] is None
                    else "BALANCE_CHECK_FAILED"
                )
            elif Decimal(balance["available_usd"]) < Decimal("1"):
                add_failure("INSUFFICIENT_BALANCE")
            if allowance["status"] == "INSUFFICIENT":
                add_failure("CANARY_ALLOWANCE_INSUFFICIENT")
            elif allowance["status"] not in {"AVAILABLE", "SUFFICIENT"}:
                add_failure("CANARY_ALLOWANCE_UNAVAILABLE")
    elif credentials_configured:
        if authentication["status"] == "FAIL":
            add_failure("AUTHENTICATED_CONNECTIVITY_FAILED")
        if account["status"] == "FAIL":
            add_failure("ACCOUNT_CHECK_FAILED")
        if geoblock["status"] == "BLOCKED":
            add_failure("GEOGRAPHICALLY_BLOCKED")
        elif geoblock["status"] == "FAIL":
            add_failure("GEOBLOCK_CHECK_FAILED")
        if balance["status"] == "FAIL":
            add_failure(
                "BALANCE_RESPONSE_INVALID"
                if balance["available_usd"] is None
                else "BALANCE_CHECK_FAILED"
            )
        elif balance["status"] == "PASS" and balance["available_usd"] is not None and Decimal(balance["available_usd"]) < Decimal("1"):
            add_failure("INSUFFICIENT_BALANCE")
        if allowance["status"] == "INSUFFICIENT":
            add_failure("CANARY_ALLOWANCE_INSUFFICIENT")
        elif allowance["status"] == "UNAVAILABLE":
            add_failure("CANARY_ALLOWANCE_UNAVAILABLE")
    if market["status"] == "FAIL" or order_book["status"] == "FAIL":
        add_failure("MARKET_CONNECTIVITY_FAILED")
    if not requested_ready and not failure_codes:
        add_failure("CONNECTIVITY_CHECK_FAILED")

    ready = bool(
        requested_ready
        and not failure_codes
        and credentials_configured
        and sdk_installed
        and _connectivity_sdk_supported(sdk_version)
        and authentication["status"] == "PASS"
        and account["status"] == "PASS"
        and geoblock["status"] == "PASS"
        and balance["status"] == "PASS"
        and balance["available_usd"] is not None
        and Decimal(balance["available_usd"]) >= Decimal("1")
        and allowance["status"] in {"AVAILABLE", "SUFFICIENT"}
        and market["status"] in {"PASS", "SKIPPED"}
        and order_book["status"] in {"PASS", "SKIPPED"}
    )
    stamp = _connectivity_checked_at(checked_at)
    if stamp is None:
        stamp = _connectivity_checked_at(raw.get("checked_at"))
    if stamp is None:
        stamp = _connectivity_checked_at(utc_now())
    failure_reasons = [
        {
            "code": code,
            "reason": _CONNECTIVITY_FAILURE_REASONS[code][:_CONNECTIVITY_TEXT_LIMIT],
        }
        for code in failure_codes
    ]
    return {
        "ready": ready,
        "status": "READY" if ready else "BLOCKED",
        "checked_at": stamp,
        "sdk": {
            "installed": bool(sdk_installed),
            "name": "polymarket-client",
            "version": sdk_version,
            "status": "INSTALLED" if sdk_installed else "NOT INSTALLED",
        },
        "credentials": {"status": "CONFIGURED" if credentials_configured else "NOT CONFIGURED"},
        "authentication": authentication,
        "account": account,
        "geoblock": geoblock,
        "balance": balance,
        "allowance": allowance,
        "market": market,
        "order_book": order_book,
        "failure_codes": failure_codes[:_CONNECTIVITY_MAX_FAILURES],
        "failure_reasons": failure_reasons[:_CONNECTIVITY_MAX_FAILURES],
        "live_execution": False,
    }


_CONNECTIVITY_PROJECTION_KEYS = frozenset(
    {
        "ready",
        "status",
        "checked_at",
        "sdk",
        "credentials",
        "authentication",
        "account",
        "geoblock",
        "balance",
        "allowance",
        "market",
        "order_book",
        "failure_codes",
        "failure_reasons",
        "live_execution",
    }
)
_CONNECTIVITY_PROJECTION_NESTED_KEYS = {
    "sdk": frozenset({"installed", "name", "version", "status"}),
    "credentials": frozenset({"status"}),
    "authentication": frozenset({"status"}),
    "account": frozenset({"status", "wallet_type"}),
    "geoblock": frozenset({"status", "country", "region"}),
    "balance": frozenset({"status", "available_usd"}),
    "allowance": frozenset({"status"}),
    "market": frozenset({"status"}),
    "order_book": frozenset({"status"}),
}


def _stored_connectivity_projection(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, Mapping) or set(value) != _CONNECTIVITY_PROJECTION_KEYS:
        return None
    checked_at = _connectivity_checked_at(value.get("checked_at"))
    if checked_at is None or value.get("checked_at") != checked_at:
        return None
    for name, keys in _CONNECTIVITY_PROJECTION_NESTED_KEYS.items():
        child = value.get(name)
        if not isinstance(child, Mapping) or set(child) != keys:
            return None
    if not isinstance(value.get("ready"), bool) or value.get("status") not in {"READY", "BLOCKED"}:
        return None
    projected = _project_connectivity(value, checked_at=checked_at)
    if projected != dict(value):
        return None
    if value["live_execution"] is not False:
        return None
    failure_codes = value["failure_codes"]
    if (
        not isinstance(failure_codes, list)
        or len(failure_codes) > _CONNECTIVITY_MAX_FAILURES
        or any(
            not isinstance(code, str) or code not in _CONNECTIVITY_FAILURE_CODES
            for code in failure_codes
        )
    ):
        return None
    if value["ready"]:
        sdk_version = value["sdk"].get("version")
        available_usd = _connectivity_decimal(value["balance"].get("available_usd"))
        if (
            value["status"] != "READY"
            or failure_codes
            or value["sdk"]["installed"] is not True
            or _connectivity_version(sdk_version) != sdk_version
            or not _connectivity_sdk_supported(sdk_version)
            or value["credentials"]["status"] != "CONFIGURED"
            or value["authentication"]["status"] != "PASS"
            or value["account"]["status"] != "PASS"
            or value["geoblock"]["status"] != "PASS"
            or value["balance"]["status"] != "PASS"
            or available_usd is None
            or Decimal(available_usd) < Decimal("1")
            or value["balance"]["available_usd"] != available_usd
            or value["allowance"]["status"] not in {"AVAILABLE", "SUFFICIENT"}
            or value["market"]["status"] not in {"PASS", "SKIPPED"}
            or value["order_book"]["status"] not in {"PASS", "SKIPPED"}
        ):
            return None
    elif value["status"] != "BLOCKED" or not failure_codes:
        return None
    return dict(value)


def _blocked_connectivity_projection(code: Any = None) -> dict[str, Any]:
    candidate = code.strip().upper() if isinstance(code, str) else ""
    safe_code = candidate if candidate in _CONNECTIVITY_FAILURE_CODES else "CONNECTIVITY_CHECK_FAILED"
    return _project_connectivity(
        {"ready": False, "failures": [safe_code], "live_execution": False},
        checked_at=utc_now(),
    )


class OperatorControlError(RuntimeError):
    """Safe, bounded error returned by an operator action."""

    def __init__(self, code: str, detail: str | None = None) -> None:
        self.code = str(code).strip() or "OPERATOR_ACTION_FAILED"
        super().__init__(detail or self.code)


def _safe_value(value: Any, *, depth: int = 0) -> Any:
    if depth >= 4:
        return "<truncated>"
    if isinstance(value, Mapping):
        return {
            str(key): _safe_value(child, depth=depth + 1)
            for key, child in list(value.items())[:32]
            if not _SECRET_KEY.search(str(key))
        }
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_safe_value(child, depth=depth + 1) for child in list(value)[:32]]
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value if not isinstance(value, str) or len(value) <= 1024 else value[:1021] + "..."
    return str(value)[:1024]
def _operator_value_missing(value: Any) -> bool:
    return value is None or (isinstance(value, str) and not value.strip())
def _operator_signal_projection(value: Any) -> dict[str, Any] | None:
    """Return a non-empty signal mapping, or the explicit missing value."""
    if not isinstance(value, Mapping) or not value:
        return None
    projected = dict(value)
    return projected or None


def _operator_canonical_blocker(*sources: Mapping[str, Any] | None) -> Any:
    """Return the first non-missing blocker from bounded report sections."""
    for source in sources:
        if not isinstance(source, Mapping):
            continue
        value = source.get("blocker")
        if not _operator_value_missing(value):
            return value
    return None
def _operator_control_state(*sources: Mapping[str, Any] | None) -> str:
    """Return the bounded control state represented by an authoritative report."""
    states: list[str] = []
    for source in sources:
        if not isinstance(source, Mapping):
            continue
        for key in ("state", "control_state", "micro_live_canary"):
            value = source.get(key)
            if _operator_value_missing(value):
                continue
            state = str(value).strip().upper()
            if state:
                states.append(state)
    if "KILLED" in states:
        return "KILLED"
    for state in states:
        if state in {"DISABLED", "DISARMED"}:
            return state
    for state in states:
        if state != "UNKNOWN":
            return state
    return "UNKNOWN"


def _operator_normalized_canary_blocker(
    blocker: Any,
    *control_sources: Mapping[str, Any] | None,
) -> Any:
    """Fill only absent blockers from the authoritative control state."""
    if not _operator_value_missing(blocker):
        return blocker
    control_state = _operator_control_state(*control_sources)
    if control_state in {"DISABLED", "DISARMED"}:
        return "AUTONOMOUS_CANARY_DISABLED"
    if control_state == "UNKNOWN":
        return "AUTONOMOUS_CONTROL_UNKNOWN"
    return blocker






def _operator_merge_persisted(
    persisted: Mapping[str, Any] | None,
    *sections: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Keep non-missing readiness singleton values while filling report gaps."""
    merged = dict(persisted) if isinstance(persisted, Mapping) else {}
    for section in sections:
        if not isinstance(section, Mapping):
            continue
        for key, value in section.items():
            if key not in merged or _operator_value_missing(merged[key]):
                merged[key] = value
    return merged


def _operator_patch_non_missing(
    target: Mapping[str, Any] | None,
    *sections: Mapping[str, Any] | None,
) -> dict[str, Any]:
    merged = dict(target) if isinstance(target, Mapping) else {}
    for section in sections:
        if not isinstance(section, Mapping):
            continue
        for key, value in section.items():
            if not _operator_value_missing(value):
                merged[key] = value
    return merged

_OPERATOR_PRESERVE_KEYS = (
    "eligibility_raw_count",
    "eligible_count",
    "rankable_raw_count",
    "rankable_count",
    "ranking_run_id",
    "ranking_timestamp",
    "selection_status",
    "selection_valid",
    "selection_invalidation_reason",
    "selection_reason",
    "selected_candidate",
    "last_selected_candidate",
    "readiness_snapshot_status",
    "readiness_snapshot_stale",
    "readiness_snapshot_reason",
    "readiness_snapshot_updated_at",
    "latest_signal",
    "rank",
    "score",
    "next_decision",
    "blocker",
    "winner_rank",
    "winner_score",
    "autonomous",
    "candidate_status",
)


def _operator_safe_mapping(value: Any) -> dict[str, Any]:
    projected = _safe_value(value)
    result = dict(projected) if isinstance(projected, Mapping) else {}
    if isinstance(value, Mapping):
        for key in _OPERATOR_PRESERVE_KEYS:
            if key in value:
                result[key] = _safe_value(value[key])
    return result





def _safe_identifier(value: Any, field: str) -> str:
    identifier = str(value or "").strip()
    if not identifier or _SAFE_IDENTIFIER.fullmatch(identifier) is None:
        raise OperatorControlError("INVALID_IDENTIFIER", f"invalid {field}")
    return identifier


def _loopback_host(value: str) -> bool:
    if str(value).strip().lower() == "localhost":
        return True
    try:
        return ipaddress.ip_address(str(value).strip()).is_loopback
    except ValueError:
        return False


def _operator_job_payload(job: Mapping[str, Any] | None) -> dict[str, Any]:
    if not isinstance(job, Mapping):
        return {
            "job_name": BOOTSTRAP_JOB_NAME,
            "status": "NOT_STARTED",
            "current_symbol": None,
            "current_timeframe": None,
            "completed_symbols": 0,
            "total_symbols": 0,
            "completed_datasets": 0,
            "total_datasets": 0,
            "started_at": None,
            "updated_at": None,
            "last_error": None,
            "resumable": False,
        }
    payload = job.get("payload") if isinstance(job.get("payload"), Mapping) else {}
    return {
        "job_name": BOOTSTRAP_JOB_NAME,
        "status": str(job.get("status") or "NOT_STARTED").upper(),
        "current_symbol": payload.get("current_symbol"),
        "current_timeframe": payload.get("current_timeframe"),
        "completed_symbols": int(payload.get("completed_symbols", 0) or 0),
        "total_symbols": int(payload.get("total_symbols", 0) or 0),
        "completed_datasets": int(payload.get("completed_datasets", 0) or 0),
        "total_datasets": int(payload.get("total_datasets", 0) or 0),
        "started_at": job.get("started_at"),
        "updated_at": job.get("updated_at"),
        "last_error": job.get("last_error"),
        "resumable": bool(job.get("resumable")),
    }


class HermesOperatorAdapter:
    """Fixed in-process Hermes adapter; it accepts no executable or argv."""
    def __init__(self, store: AxiomStore, job_id: str) -> None:
        self.store = store
        self.job_id = _safe_identifier(job_id, "Hermes job ID")

    def state(self) -> dict[str, Any]:
        raw = self.store.get_scheduler_state(HERMES_STATE_NAME) or {}
        state = dict(raw) if isinstance(raw, Mapping) else {}
        # This is the control state of Axiom's in-process queue processor.
        # It is deliberately kept separate from the external Hermes status,
        # which cannot be verified by this adapter.
        status = str(state.get("status") or "ACTIVE").upper()
        if status not in {"ACTIVE", "PAUSED"}:
            status = "ACTIVE"
        schedule = state.get("schedule") or "after_each_collection"
        last_cycle_at = state.get("last_cycle_at")
        if last_cycle_at is None:
            last_cycle_at = state.get("last_run_at")
        trigger = state.get("trigger") or schedule
        return {
            # Legacy scheduler fields remain available for the node and
            # existing callers; status is explicitly the local queue status.
            "job_id": self.job_id,
            "status": status,
            "schedule": schedule,
            "last_run_at": state.get("last_run_at"),
            "next_run_at": state.get("next_run_at"),
            "last_result": _safe_value(state.get("last_result")),
            "run_requested_at": state.get("run_requested_at"),
            "control_scope": HERMES_CONTROL_SCOPE,
            "external_hermes": {
                "job_id": self.job_id,
                "status": HERMES_EXTERNAL_STATUS,
                "evidence": HERMES_EXTERNAL_EVIDENCE,
            },
            "internal_queue": {
                "status": status,
                "trigger": trigger,
                "last_cycle_at": last_cycle_at,
            },
        }

    def set_status(self, status: str) -> dict[str, Any]:
        requested = str(status).upper()
        if requested not in {"ACTIVE", "PAUSED"}:
            raise OperatorControlError("INVALID_HERMES_STATUS")
        current = self.store.get_scheduler_state(HERMES_STATE_NAME) or {}
        body = dict(current) if isinstance(current, Mapping) else {}
        body.update({"status": requested, "job_id": self.job_id, "updated_at": utc_now().isoformat()})
        self.store.set_scheduler_state(HERMES_STATE_NAME, body)
        return self.state()

    def run_now(self) -> dict[str, Any]:
        current = self.state()
        if current["status"] == "PAUSED":
            raise OperatorControlError("HERMES_PAUSED", "resume Hermes before running it")
        processor = AutonomousResearchProcessor(self.store)
        cycle = processor.process_pending(worker="operator-hermes")
        now = utc_now().isoformat()
        result = {
            "claimed": int(getattr(cycle, "claimed", 0)),
            "completed": int(getattr(cycle, "completed", 0)),
            "rejected": int(getattr(cycle, "rejected", 0)),
            "failed": int(getattr(cycle, "failed", 0)),
        }
        self.store.set_scheduler_state(
            HERMES_STATE_NAME,
            {
                **(self.store.get_scheduler_state(HERMES_STATE_NAME) or {}),
                "status": "ACTIVE",
                "job_id": self.job_id,
                "last_run_at": now,
                "last_result": result,
                "run_requested_at": None,
                "updated_at": now,
            },
        )
        return {**self.state(), "last_result": result}


class OperatorControlPlane:
    """Authoritative action boundary used by the dashboard and launcher."""

    def __init__(
        self,
        store: AxiomStore,
        *,
        db_path: str | os.PathLike[str] | None = None,
        hermes_job_id: str | None = None,
        node_launcher: Callable[[list[str]], Any] | None = None,
    ) -> None:
        self.store = store
        raw_db = db_path if db_path is not None else getattr(store, "path", "")
        self.db_path = os.path.abspath(os.path.expanduser(str(raw_db))) if str(raw_db) not in {"", ":memory:"} else str(raw_db)
        self._node_launcher = node_launcher or self._spawn_node
        self._lock = threading.RLock()
        self._connectivity_lock = threading.RLock()
        self._bootstrap_threads: dict[str, threading.Thread] = {}
        # Windows identity checks can invoke CIM and are disproportionately
        # expensive on a dashboard refresh.  Keep a tiny, short-lived cache so
        # repeated status reads cannot fan out into an unbounded process scan.
        self._pid_identity_cache: dict[tuple[int, str], tuple[float, bool]] = {}
        self._pid_identity_cache_ttl = 1.0
        self._pid_identity_cache_limit = 32
        configured = self.store.get_operator_config("hermes_research_job_id", None)
        selected = hermes_job_id or configured or DEFAULT_HERMES_JOB_ID
        self.hermes_job_id = _safe_identifier(selected, "Hermes job ID")
        if hermes_job_id is not None:
            self.configure_hermes_job_id(hermes_job_id)

    def _cached_pid_matches_node(self, pid: int) -> bool:
        """Bound the Windows CIM/process identity work used by status reads."""
        try:
            pid_value = int(pid)
        except (TypeError, ValueError):
            return False
        if pid_value <= 0:
            return False
        now = time.monotonic()
        key = (pid_value, self.db_path)
        cached = self._pid_identity_cache.get(key)
        if cached is not None and now - cached[0] <= self._pid_identity_cache_ttl:
            return cached[1]
        result = bool(_pid_matches_node(pid_value, self.db_path))
        self._pid_identity_cache[key] = (now, result)
        if len(self._pid_identity_cache) > self._pid_identity_cache_limit:
            oldest = min(self._pid_identity_cache, key=self._pid_identity_cache.__getitem__)
            self._pid_identity_cache.pop(oldest, None)
        return result

    def configure_hermes_job_id(self, job_id: str) -> str:
        value = _safe_identifier(job_id, "Hermes job ID")
        self.store.set_operator_config("hermes_research_job_id", value)
        self.hermes_job_id = value
        return value

    def _node_lock_path(self) -> Path:
        return Path(str(self.db_path) + ".lock")

    def _node_status(self) -> dict[str, Any]:
        lock_path = self._node_lock_path()
        lock_pid = 0
        lock_text = ""
        try:
            lock_text = lock_path.read_text(encoding="ascii")
            lock_pid = int(lock_text.splitlines()[0].strip())
        except (FileNotFoundError, OSError, ValueError):
            pass
        workers = self.store.list_worker_states(limit=32)
        root = next((item for item in workers if str(item.get("worker_name")) == "axiom-node"), {})
        worker_payload = root.get("payload") if isinstance(root.get("payload"), Mapping) else {}
        persisted_pid = worker_payload.get("pid")
        try:
            pid = int(lock_pid or persisted_pid or 0)
        except (TypeError, ValueError):
            pid = 0
        identity_valid = bool(pid and self._cached_pid_matches_node(pid))
        alive = bool(pid and _pid_alive(pid))
        persisted_status = str(root.get("status") or "").lower()
        if alive and identity_valid:
            state = "RUNNING"
        elif persisted_status in {"running", "degraded"} and (pid or lock_path.exists()):
            state = "STALE"
        else:
            state = "STOPPED"
        return {
            "status": state,
            "pid": pid or None,
            "started_at": root.get("started_at"),
            "heartbeat_at": root.get("heartbeat_at"),
            "lock_path": str(lock_path),
            "lock_exists": lock_path.exists(),
            "worker_status": root.get("status"),
            "worker_alive": alive,
            "worker_identity_valid": identity_valid,
        }

    def _clear_stale_lock(self) -> None:
        path = self._node_lock_path()
        try:
            text = path.read_text(encoding="ascii")
            pid = int(text.splitlines()[0].strip())
        except (FileNotFoundError, OSError, ValueError):
            return
        if _pid_alive(pid) and self._cached_pid_matches_node(pid):
            raise OperatorControlError("NODE_ALREADY_RUNNING")
        try:
            if path.read_text(encoding="ascii") == text:
                path.unlink()
        except (FileNotFoundError, OSError):
            pass

    def _spawn_node(self, command: list[str]) -> subprocess.Popen[Any]:
        kwargs: dict[str, Any] = {
            "stdin": subprocess.DEVNULL,
            "stdout": subprocess.DEVNULL,
            "stderr": subprocess.DEVNULL,
            "shell": False,
        }
        if os.name == "nt":
            kwargs["creationflags"] = int(getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)) | int(getattr(subprocess, "DETACHED_PROCESS", 0))
        else:
            kwargs["start_new_session"] = True
        return subprocess.Popen(command, **kwargs)

    def ensure_node(self, *, wait_seconds: float = 5.0) -> dict[str, Any]:
        with self._lock:
            current = self._node_status()
            if current["status"] == "RUNNING":
                return current
            self._clear_stale_lock()
            command = [sys.executable, "-m", "axiom.cli", "node-run", "--db", self.db_path, "--cycles", "0"]
            try:
                process = self._node_launcher(command)
            except (OSError, RuntimeError) as exc:
                raise OperatorControlError("NODE_START_FAILED") from exc
            deadline = time.monotonic() + max(0.1, float(wait_seconds))
            while time.monotonic() < deadline:
                current = self._node_status()
                if current["status"] == "RUNNING":
                    return current
                if hasattr(process, "poll") and process.poll() is not None:
                    break
                time.sleep(0.1)
            raise OperatorControlError("NODE_START_TIMEOUT")

    def restart_node(self) -> dict[str, Any]:
        with self._lock:
            current = self._node_status()
            if current["status"] == "RUNNING":
                pid = int(current.get("pid") or 0)
                path = self._node_lock_path()
                try:
                    marker = path.read_text(encoding="ascii")
                except (FileNotFoundError, OSError) as exc:
                    raise OperatorControlError("NODE_STOP_UNSAFE") from exc
                if not marker or int(marker.splitlines()[0].strip()) != pid:
                    raise OperatorControlError("NODE_STOP_UNSAFE")
                stop_path = Path(str(self.db_path) + ".stop")
                try:
                    stop_path.write_text(marker, encoding="ascii")
                except OSError as exc:
                    raise OperatorControlError("NODE_STOP_REQUEST_FAILED") from exc
                deadline = time.monotonic() + 15.0
                while time.monotonic() < deadline:
                    if self._node_status()["status"] != "RUNNING":
                        break
                    time.sleep(0.1)
                if self._node_status()["status"] == "RUNNING":
                    raise OperatorControlError("NODE_STOP_TIMEOUT")
            return self.ensure_node()

    def _bootstrap_status(self) -> dict[str, Any]:
        """Project persisted bootstrap progress using bounded aggregate reads."""
        job = self.store.get_operator_job(BOOTSTRAP_JOB_NAME)
        result = _operator_job_payload(job)
        aggregate: Mapping[str, Any] = {}
        aggregate_method = getattr(self.store, "dashboard_overview_summary", None)
        if callable(aggregate_method):
            try:
                candidate = aggregate_method(activity_limit=1)
                if isinstance(candidate, Mapping):
                    aggregate = candidate
            except (AttributeError, TypeError, ValueError, OSError):
                aggregate = {}
        statuses = aggregate.get("bootstrap_statuses", {})
        statuses = statuses if isinstance(statuses, Mapping) else {}
        normalized_statuses = {
            str(key).upper(): max(0, int(value or 0))
            for key, value in statuses.items()
            if str(key).strip()
        }
        complete_count = sum(
            normalized_statuses.get(key, 0) for key in ("COMPLETE", "EMPTY")
        )
        running_row: Mapping[str, Any] | None = None
        states_method = getattr(self.store, "list_dataset_bootstrap_states", None)
        if callable(states_method):
            try:
                latest = states_method(limit=1)
                if isinstance(latest, (list, tuple)) and latest:
                    row = latest[0]
                    if isinstance(row, Mapping) and str(row.get("status", "")).upper() == "RUNNING":
                        running_row = row
            except (AttributeError, TypeError, ValueError, OSError):
                running_row = None
        total_datasets = max(
            int(result.get("total_datasets", 0) or 0),
            sum(normalized_statuses.values()),
        )
        total_symbols = int(result.get("total_symbols", 0) or 0)
        if not total_symbols and total_datasets:
            total_symbols = (total_datasets + 3) // 4
        result.update(
            {
                "current_symbol": (
                    running_row.get("instrument")
                    if running_row is not None
                    else result.get("current_symbol")
                ),
                "current_timeframe": (
                    running_row.get("timeframe")
                    if running_row is not None
                    else result.get("current_timeframe")
                ),
                "completed_datasets": complete_count,
                "total_datasets": total_datasets,
                "total_symbols": total_symbols,
            }
        )
        if (
            result["status"] == "RUNNING"
            and running_row is None
            and complete_count
            and total_datasets
            and complete_count >= total_datasets
        ):
            result["status"] = "COMPLETE"
        elif (
            result["status"] == "NOT_STARTED"
            and 0 < complete_count < total_datasets
        ):
            result["status"] = "PARTIAL"
            result["resumable"] = True
        return result

    def _bootstrap_worker(self, *, resume: bool, started_at: datetime, total_symbols: int) -> None:
        status = "COMPLETE"
        error: str | None = None
        try:
            snapshot = load_crypto_universe(self.store)
            if snapshot is None:
                raise OperatorControlError("CRYPTO_UNIVERSE_NOT_INITIALIZED")
            bootstrapper = HistoricalBootstrapper(self.store, crypto_provider=BinanceAdapter(timeout=10.0))
            reports = bootstrapper.bootstrap_crypto_universe(
                snapshot,
                start=BTC_HISTORY_START,
                resume=bool(resume),
                max_symbols=50,
            )
            failed = [item for item in reports if str(getattr(item, "status", "")).upper() in {"FAILED", "ERROR"}]
            if failed:
                status = "FAILED"
                error = "BOOTSTRAP_DATASET_FAILED"
        except Exception as exc:
            status = "FAILED"
            error = exc.code if isinstance(exc, OperatorControlError) else type(exc).__name__.upper()
        finally:
            progress = self._bootstrap_status()
            progress.update({"status": status, "total_symbols": total_symbols, "last_error": error, "resumable": status != "COMPLETE"})
            self.store.set_operator_job(
                BOOTSTRAP_JOB_NAME,
                status,
                {key: progress.get(key) for key in ("current_symbol", "current_timeframe", "completed_symbols", "total_symbols", "completed_datasets", "total_datasets")},
                pid=os.getpid(),
                started_at=started_at,
                last_error=error,
                resumable=status != "COMPLETE",
            )
            with self._lock:
                self._bootstrap_threads.pop(BOOTSTRAP_JOB_NAME, None)

    def start_bootstrap(self, *, resume: bool) -> dict[str, Any]:
        with self._lock:
            current = self._bootstrap_status()
            thread = self._bootstrap_threads.get(BOOTSTRAP_JOB_NAME)
            if current["status"] == "RUNNING" and thread is not None and thread.is_alive():
                raise OperatorControlError("BOOTSTRAP_ALREADY_RUNNING")
            if resume and not current["resumable"] and current["status"] not in {"FAILED", "STALE"}:
                raise OperatorControlError("BOOTSTRAP_NOT_RESUMABLE")
            try:
                snapshot = load_crypto_universe(self.store)
            except (AttributeError, TypeError, ValueError):
                snapshot = None
            if snapshot is None:
                raise OperatorControlError("CRYPTO_UNIVERSE_NOT_INITIALIZED")
            selected = list(snapshot.selected_symbols)
            now = utc_now()
            self.store.set_operator_job(
                BOOTSTRAP_JOB_NAME,
                "RUNNING",
                {
                    "current_symbol": current.get("current_symbol"),
                    "current_timeframe": current.get("current_timeframe"),
                    "completed_symbols": current.get("completed_symbols", 0),
                    "total_symbols": len(selected),
                    "completed_datasets": current.get("completed_datasets", 0),
                    "total_datasets": len(selected) * 4,
                    "resume": bool(resume),
                },
                pid=os.getpid(),
                started_at=now,
                resumable=True,
            )
            thread = threading.Thread(
                target=self._bootstrap_worker,
                kwargs={"resume": bool(resume), "started_at": now, "total_symbols": len(selected)},
                name="axiom-crypto-bootstrap",
                daemon=True,
            )
            self._bootstrap_threads[BOOTSTRAP_JOB_NAME] = thread
            thread.start()
            return self._bootstrap_status()
    def _hermes(self) -> HermesOperatorAdapter:
        configured = self.store.get_operator_config("hermes_research_job_id", self.hermes_job_id)
        selected = configured if isinstance(configured, str) and configured else self.hermes_job_id
        return HermesOperatorAdapter(self.store, selected)

    def status(self) -> dict[str, Any]:
        hermes = self._hermes()
        workers = self.store.list_worker_states(limit=32)
        worker_map = {str(item.get("worker_name")): item for item in workers if isinstance(item, Mapping)}
        scope_funnel_method = getattr(self.store, "market_scope_resolution_funnel", None)
        market_scope_funnel: dict[str, Any] = {}
        if callable(scope_funnel_method):
            try:
                persisted_funnel = scope_funnel_method(limit=1000)
            except TypeError:
                try:
                    persisted_funnel = scope_funnel_method()
                except (AttributeError, TypeError, ValueError, sqlite3.Error):
                    persisted_funnel = {}
            except (AttributeError, TypeError, ValueError, sqlite3.Error):
                persisted_funnel = {}
            if isinstance(persisted_funnel, Mapping):
                market_scope_funnel = _safe_value(persisted_funnel)
                if not isinstance(market_scope_funnel, dict):
                    market_scope_funnel = {}

        def worker(name: str) -> dict[str, Any]:
            row = worker_map.get(name, {})
            payload = row.get("payload") if isinstance(row.get("payload"), Mapping) else {}
            return {
                "status": str(row.get("status") or "NOT_STARTED").upper(),
                "pid": payload.get("pid"),
                "heartbeat_at": row.get("heartbeat_at"),
                "started_at": row.get("started_at"),
            }


        canary = CanaryService(self.store, initialize=False)
        report: Mapping[str, Any] = {}
        legacy_readiness: Mapping[str, Any] = {}
        try:
            readiness_method = getattr(canary, "status", None)
            if not callable(readiness_method):
                readiness_method = getattr(canary, "readiness_snapshot", None)
            if callable(readiness_method):
                raw_readiness = readiness_method()
                candidate_readiness = _operator_safe_mapping(raw_readiness)
                if isinstance(candidate_readiness, Mapping):
                    legacy_readiness = candidate_readiness
            report_method = getattr(canary, "status_report", None)
            if callable(report_method):
                raw_report = report_method()
                candidate_report = _safe_value(raw_report)
                if isinstance(raw_report, Mapping) and isinstance(candidate_report, Mapping):
                    candidate_report = dict(candidate_report)
                    for section_name in (
                        "latest_signal",
                        "control",
                        "authoritative_control",
                        "readiness",
                        "authoritative_readiness",
                        "worker",
                        "execution",
                    ):
                        if section_name in raw_report:
                            candidate_report[section_name] = _safe_value(
                                raw_report[section_name]
                            )
                if isinstance(candidate_report, Mapping):
                    report = dict(candidate_report)
            if not report:
                report = {"readiness": dict(legacy_readiness)}
        except Exception:
            report = {"readiness": dict(legacy_readiness)}
        control_report = report.get("control") or report.get("authoritative_control") or {}
        raw_readiness_report = report.get("readiness") or {}
        readiness_report = _operator_merge_persisted(
            legacy_readiness if isinstance(legacy_readiness, Mapping) else {},
            raw_readiness_report
            if isinstance(raw_readiness_report, Mapping)
            else None,
        )
        worker_report = report.get("worker") or {}
        execution_report = report.get("execution") or {}
        report = dict(report)
        report["readiness"] = dict(readiness_report)
        control_report = control_report if isinstance(control_report, Mapping) else {}
        readiness_report = readiness_report if isinstance(readiness_report, Mapping) else {}
        worker_report = worker_report if isinstance(worker_report, Mapping) else {}
        execution_report = execution_report if isinstance(execution_report, Mapping) else {}
        canary_status = dict(readiness_report)
        # Fill only absent legacy aliases from bounded report sections.  The
        # readiness singleton remains authoritative for non-missing display data.
        for section in (control_report, worker_report, execution_report):
            canary_status = _operator_merge_persisted(canary_status, section)
        latest_signal = None
        for source in (
            report,
            execution_report,
            worker_report,
            canary_status,
        ):
            for alias in ("latest_signal", "signal"):
                candidate = _operator_signal_projection(source.get(alias))
                if candidate is not None:
                    latest_signal = candidate
                    break
            if latest_signal is not None:
                break
        latest_signal_method = getattr(canary, "latest_signal", None)
        if callable(latest_signal_method):
            try:
                candidate_signal = _operator_signal_projection(
                    _safe_value(latest_signal_method())
                )
            except Exception:
                candidate_signal = None
            if candidate_signal is not None:
                latest_signal = candidate_signal
        canary_status["latest_signal"] = latest_signal
        autonomous_state = _operator_merge_persisted(
            readiness_report.get("autonomous")
            if isinstance(readiness_report.get("autonomous"), Mapping)
            else {},
            canary_status.get("autonomous")
            if isinstance(canary_status.get("autonomous"), Mapping)
            else None,
        )
        if isinstance(worker_report, Mapping):
            autonomous_state = _operator_patch_non_missing(
                autonomous_state,
                worker_report.get("autonomous"),
            )
            worker_fields = {
                key: worker_report.get(key)
                for key in (
                    "last_tick_at",
                    "last_tick_started_at",
                    "last_tick_completed_at",
                    "last_successful_tick",
                    "last_error_code",
                    "consecutive_failures",
                    "next_retry_at",
                    "candidates_evaluated",
                    "signals_generated",
                    "orders_attempted",
                    "next_decision",
                    "blocker",
                    "last_signal_id",
                    "worker_status",
                )
                if key in worker_report
            }
            autonomous_state = _operator_patch_non_missing(
                autonomous_state,
                worker_fields,
            )
        rank_value = canary_status.get("rank")
        if _operator_value_missing(rank_value):
            rank_value = canary_status.get("winner_rank")
        score_value = canary_status.get("score")
        if _operator_value_missing(score_value):
            score_value = canary_status.get("winner_score")
        autonomous_state = _operator_patch_non_missing(
            autonomous_state,
            {
                "rank": rank_value,
                "score": score_value,
                "selection_reason": canary_status.get("selection_reason"),
                "next_decision": canary_status.get("next_decision"),
                "blocker": canary_status.get("blocker"),
            },
        )
        blocker = _operator_normalized_canary_blocker(
            _operator_canonical_blocker(
                worker_report,
                report,
                control_report,
                execution_report,
                autonomous_state,
                canary_status,
                readiness_report,
            ),
            control_report,
            report.get("authoritative_control"),
            readiness_report,
            report,
            canary_status,
        )
        canary_status["blocker"] = blocker
        autonomous_state["blocker"] = blocker
        report["latest_signal"] = latest_signal
        report["readiness"] = {
            **readiness_report,
            "latest_signal": latest_signal,
            "blocker": blocker,
        }
        autonomous_state.setdefault(
            "enabled",
            str(canary_status.get("micro_live_canary") or "").upper()
            in {"AUTONOMOUS_MICRO_LIVE", "ENABLED"},
        )
        canary_status.setdefault(
            "risk_envelope",
            readiness_report.get("risk_envelope")
            or control_report.get("risk_envelope")
            or dict(AUTONOMOUS_CANARY_LIMITS),
        )
        canary_status.setdefault(
            "risk_limits",
            readiness_report.get("risk_limits")
            or control_report.get("risk_limits")
            or canary_status.get("risk_envelope", {}),
        )
        canary_status.setdefault("trades", execution_report.get("trades", []))
        canary_status.setdefault(
            "execution_event_count",
            execution_report.get("event_count", execution_report.get("real_execution_events", 0)),
        )
        canary_status.setdefault(
            "real_execution_events",
            execution_report.get("real_execution_events", execution_report.get("event_count", 0)),
        )
        worker_status = worker("autonomous-canary")
        latest_connectivity = _stored_connectivity_projection(
            self.store.get_operator_config(CANARY_CONNECTIVITY_CONFIG_KEY, None)
        )
        credential_type = CredentialStore
        try:
            projected_credentials = credential_type.cached_projection(
                allow_environment=False,
                persisted={
                    "canary": canary_status,
                    "status_report": report,
                    "readiness": readiness_report,
                    "connectivity": latest_connectivity,
                },
            )
        except BaseException:
            projected_credentials = None
        if not isinstance(projected_credentials, Mapping):
            try:
                credential_store = credential_type()
                projected_credentials = credential_store.cached_projection(
                    allow_environment=False,
                    persisted={
                        "canary": canary_status,
                        "status_report": report,
                        "readiness": readiness_report,
                        "connectivity": latest_connectivity,
                    },
                )
            except BaseException:
                projected_credentials = None
        credentials = (
            dict(projected_credentials)
            if isinstance(projected_credentials, Mapping)
            else {
                "configured": None,
                "status": "NOT CHECKED",
                "secret_values_exposed": False,
            }
        )
        credentials["secret_values_exposed"] = False
        return {
            "canary_status_report": dict(report),
            "connectivity": latest_connectivity,
            "node": self._node_status(),
            "bootstrap": self._bootstrap_status(),
            "hermes": hermes.state(),
            "collector": worker("polymarket-collector"),
            "paper": {**worker("paper-engine"), "read_only": True, "live_execution": False},
            "research": worker("research-engine"),
            "autonomous_canary_worker": worker_status,
            "market_scope_funnel": market_scope_funnel,
            "credentials": credentials,
            "canary": {
                "status": canary_status,
                "status_report": dict(report),
                "control": dict(control_report),
                "readiness": dict(readiness_report),
                "worker": dict(worker_report),
                "execution": dict(execution_report),
                "latest_signal": latest_signal,
                "autonomous": autonomous_state,
                "connectivity": latest_connectivity,
                "submit": "AUTONOMOUS_WORKER" if autonomous_state.get("enabled") else "DISABLED_UNTIL_OPERATOR_ENABLE",
            },
            "live_execution": False,
            "paper_only": True,
        }

    def _audit(self, action: str, target: str, *, success: bool, reason: str = "", result: Mapping[str, Any] | None = None) -> None:
        try:
            self.store.record_operator_action(
                action,
                target,
                success=success,
                reason=reason,
                result=_safe_value(result or {}),
            )
        except Exception:
            pass

    def execute(self, action: str, target: str = "", *, confirm: str = "") -> dict[str, Any]:
        action_value = str(action or "").strip()
        target_value = str(target or "").strip()
        try:
            if action_value not in _ALLOWED_ACTIONS:
                raise OperatorControlError("ACTION_NOT_ALLOWED")
            expected = _CONFIRMATIONS.get(action_value)
            if expected is not None and confirm != expected:
                raise OperatorControlError("EXACT_CONFIRMATION_REQUIRED")
            if action_value in {"canary.eligibility.verify", "canary.eligibility.mark", "canary.generate_signal", "canary.arm"}:
                target_value = _safe_identifier(target_value, "candidate ID")
            elif action_value == "canary.enable_auto" and target_value:
                raise OperatorControlError("AUTONOMOUS_CANARY_ACCEPTS_NO_TARGET")
            if action_value == "node.restart":
                result = {"node": self.restart_node()}
            elif action_value == "bootstrap.start":
                result = {"bootstrap": self.start_bootstrap(resume=False)}
            elif action_value == "bootstrap.resume":
                result = {"bootstrap": self.start_bootstrap(resume=True)}
            elif action_value == "hermes.pause":
                result = {"hermes": self._hermes().set_status("PAUSED")}
            elif action_value == "hermes.resume":
                result = {"hermes": self._hermes().set_status("ACTIVE")}
            elif action_value == "hermes.run_now":
                result = {"hermes": self._hermes().run_now()}
            elif action_value == "canary.connectivity_check":
                with self._connectivity_lock:
                    try:
                        credentials = CredentialStore()
                        configured = credentials.configured(allow_environment=False)
                        venue = PolymarketClobV2Venue(allow_environment=False) if configured else None
                        service = CanaryService(self.store, credentials=credentials, initialize=False)
                        raw_connectivity = service.connectivity_check(
                            venue=venue,
                            allow_environment=False,
                        )
                        connectivity = _project_connectivity(raw_connectivity, checked_at=utc_now())
                    except CanaryBlocked as exc:
                        connectivity = _blocked_connectivity_projection(str(exc))
                    except Exception:
                        connectivity = _blocked_connectivity_projection()
                    self.store.set_operator_config(CANARY_CONNECTIVITY_CONFIG_KEY, connectivity)
                    result = {"connectivity": connectivity}
            elif action_value == "canary.eligibility.verify":
                service = CanaryService(self.store, initialize=False)
                result = {"eligibility": service.validate_eligibility(target_value)}
            elif action_value == "canary.eligibility.mark":
                service = CanaryService(self.store, initialize=True)
                validation = service.validate_eligibility(target_value)
                if not validation.get("eligible"):
                    raise OperatorControlError(str(validation.get("reason_code") or "CANDIDATE_RESEARCH_GATES_INCOMPLETE"))
                service.mark_eligible(target_value)
                result = {"eligibility": validation, "marked": True}
            elif action_value == "canary.generate_signal":
                service = CanaryService(self.store, initialize=True)
                result = {"signal": service.generate_signal(target_value)}
            elif action_value == "canary.arm":
                credentials = CredentialStore()
                if not credentials.configured(allow_environment=False):
                    raise OperatorControlError("CREDENTIALS_NOT_CONFIGURED")
                service = CanaryService(self.store, credentials=credentials, initialize=True)
                venue = PolymarketClobV2Venue(allow_environment=False)
                result = {
                    "canary": service.arm(
                        target_value,
                        venue=venue,
                    )
                }
            elif action_value == "canary.enable_auto":
                with self._connectivity_lock:
                    connectivity = _stored_connectivity_projection(
                        self.store.get_operator_config(CANARY_CONNECTIVITY_CONFIG_KEY, None)
                    )
                    if connectivity is None:
                        raise OperatorControlError("CONNECTIVITY_CHECK_REQUIRED")
                    if not connectivity["ready"] or connectivity["status"] != "READY":
                        failure_codes = connectivity.get("failure_codes")
                        blocked_reason = (
                            failure_codes[0]
                            if isinstance(failure_codes, list) and failure_codes and isinstance(failure_codes[0], str)
                            else "CONNECTIVITY_BLOCKED"
                        )
                        raise OperatorControlError(blocked_reason)
                    service = CanaryService(self.store, initialize=True)
                    result = {
                        "canary": service.enable_autonomous_micro_live(),
                        "confirmation": "ENABLE AUTO CANARY",
                    }
            elif action_value == "canary.disarm":
                service = CanaryService(self.store, initialize=True)
                service.disarm()
                result = {"canary": service.status()}
            elif action_value == "canary.kill":
                service = CanaryService(self.store, initialize=True)
                service.kill()
                result = {"canary": service.status()}
            else:
                raise OperatorControlError("ACTION_NOT_ALLOWED")
            if action_value == "canary.connectivity_check":
                public = {"connectivity": connectivity}
            else:
                public = _safe_value(result)
            self._audit(action_value, target_value, success=True, result=public)
            response = {
                "ok": True,
                "action": action_value,
                "target": target_value,
                "result": public,
                "paper_only": True,
                "live_execution": False,
            }
            if action_value.startswith("hermes."):
                response["control_scope"] = HERMES_CONTROL_SCOPE
            return response
        except OperatorControlError as exc:
            reason = exc.code
        except CanaryBlocked as exc:
            reason = str(exc)[:160] or "CANARY_BLOCKED"
        except (OSError, RuntimeError, TypeError, ValueError) as exc:
            reason = type(exc).__name__.upper()
        except Exception as exc:
            reason = type(exc).__name__.upper()
        failure = {
            "ok": False,
            "action": action_value,
            "target": target_value,
            "reason": reason,
            "paper_only": True,
            "live_execution": False,
        }
        if action_value.startswith("hermes."):
            failure["control_scope"] = HERMES_CONTROL_SCOPE
        self._audit(
            action_value or "invalid",
            target_value,
            success=False,
            reason=reason,
            result={"ok": False, "reason": reason},
        )
        return failure


__all__ = [
    "DEFAULT_HERMES_JOB_ID",
    "BOOTSTRAP_JOB_NAME",
    "HERMES_CONTROL_SCOPE",
    "CANARY_CONNECTIVITY_CONFIG_KEY",
    "HermesOperatorAdapter",
    "OperatorControlError",
    "OperatorControlPlane",
]
